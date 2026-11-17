"""Verify normalized CDC, SCD-2 histories, journal balances, and restarts."""

import hashlib
import json
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

from .cdc import ENTITIES, normalize, run
from .runtime import spark_session
from .silver import arguments, table_path


def digest(frame):
    return hashlib.sha256(
        "\n".join(sorted(frame.toJSON().collect())).encode()
    ).hexdigest()


def verify(root, dataset):
    from pyspark.sql import Window, functions as F
    from pyspark.sql.types import DecimalType

    output = Path("/workspace/pipelines/output") / dataset
    bronze_proof = json.loads(
        (output / "verification.json").read_text(encoding="utf-8")
    )
    source_report = Path(bronze_proof["source_report"])
    canonical = json.loads(
        (source_report.parent / "canonical_transactions.json").read_text(
            encoding="utf-8"
        )
    )
    expected = {row["invoice_id"]: row for row in canonical}
    proof_path = output / "cdc_verification.json"
    prior = (
        json.loads(proof_path.read_text(encoding="utf-8"))
        if proof_path.exists()
        else None
    )
    spark = spark_session("FinRecon CDC/SCD-2 verification")
    try:
        first = run(spark, root, dataset)
        raw = spark.read.format("delta").load(
            str(Path(root) / "bronze" / dataset / "bronze_cdc_ledger")
        )
        normalized = normalize(raw).cache()
        assert normalized.where(F.size("failed_rules") > 0).count() == 0
        changes = normalized.where(~F.col("is_tombstone"))
        latest = changes.withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy("cdc_table", "entity_key").orderBy(
                    F.col("source_version").desc(), F.col("record_id").desc()
                ),
            ),
        ).where("_rank = 1")

        def read(table):
            return spark.read.format("delta").load(
                str(table_path(root, dataset, table))
            )

        tables = {spec["table"]: read(spec["table"]) for spec in ENTITIES.values()}
        tables["silver_cdc_events"] = read("silver_cdc_events")
        assert tables["silver_cdc_events"].count() == raw.count()
        assert (
            tables["silver_cdc_events"].select("record_id").distinct().count()
            == raw.count()
        )
        for entity, spec in ENTITIES.items():
            state = tables[spec["table"]]
            current = state.where("is_current") if entity != "ledger_entries" else state
            expected_state = latest.where(F.col("cdc_table") == entity).select(
                "entity_key",
                F.col("row_hash").alias("expected_hash"),
                F.col("source_version").alias("expected_version"),
            )
            comparison = current.select(
                "entity_key", "row_hash", "last_source_version"
            ).join(expected_state, "entity_key", "full")
            assert (
                comparison.where(
                    ~F.col("row_hash").eqNullSafe(F.col("expected_hash"))
                    | ~F.col("last_source_version").eqNullSafe(
                        F.col("expected_version")
                    )
                ).count()
                == 0
            )
            assert (
                current.groupBy("entity_key").count().where("count != 1").count() == 0
            )
            if entity != "ledger_entries":
                assert state.select("version_id").distinct().count() == state.count()
                ordered = state.withColumn(
                    "_next",
                    F.lead("eff_start_version").over(
                        Window.partitionBy("entity_key").orderBy("eff_start_version"),
                    ),
                )
                assert (
                    ordered.where(
                        ~F.col("eff_end_version").eqNullSafe(F.col("_next"))
                    ).count()
                    == 0
                )
                assert (
                    state.where(
                        "eff_end_ts < eff_start_ts OR last_source_version < eff_start_version"
                    ).count()
                    == 0
                )
                assert (
                    state.where(
                        "NOT is_current AND last_source_version >= eff_end_version"
                    ).count()
                    == 0
                )

        invoice_history = tables[ENTITIES["invoices"]["table"]].where(
            F.col("invoice_id").isin(list(expected))
        )
        invoice_rows = invoice_history.orderBy(
            "invoice_id", "eff_start_version"
        ).collect()
        assert len(invoice_rows) == len(expected) * 2
        versions = defaultdict(list)
        for row in invoice_rows:
            versions[row.invoice_id].append(row)
            assert row.total_amount_dec == Decimal(
                expected[row.invoice_id]["amount_dec"]
            )
            assert row.currency == expected[row.invoice_id]["currency"]
        assert all(
            [row.status for row in rows] == ["ISSUED", "PAID"]
            for rows in versions.values()
        )
        assert all(
            rows[-1].is_current and not rows[-1].is_deleted
            for rows in versions.values()
        )
        assert invoice_history.schema["total_amount_dec"].dataType == DecimalType(18, 4)
        journal = tables["silver_ledger_entries"].where(
            ~F.col("is_deleted") & F.col("invoice_id").isin(list(expected))
        )
        assert (
            journal.schema["debit_dec"].dataType
            == journal.schema["credit_dec"].dataType
            == DecimalType(18, 4)
        )
        entries = journal.collect()
        assert len(entries) == len(expected) * 2
        per_invoice = defaultdict(lambda: [Decimal("0.0000"), Decimal("0.0000")])
        for row in entries:
            per_invoice[row.invoice_id][0] += row.debit_dec
            per_invoice[row.invoice_id][1] += row.credit_dec
        assert all(
            debit == credit == Decimal(expected[key]["amount_dec"])
            for key, (debit, credit) in per_invoice.items()
        )
        totals = defaultdict(Decimal)
        for key, (debit, credit) in per_invoice.items():
            totals[expected[key]["currency"]] += debit

        before = {name: digest(frame) for name, frame in tables.items()}
        second = run(spark, root, dataset)
        after = {name: digest(read(name)) for name in tables}
        assert before == after
        assert (
            first["query_id"] == second["query_id"]
            and first["run_id"] != second["run_id"]
        )
        if prior:
            assert prior["query"]["query_id"] == first["query_id"]
            assert prior["table_digests"] == after
        report = {
            "dataset": dataset,
            "normalized_cdc_rows": raw.count(),
            "tombstones_preserved": normalized.where("is_tombstone").count(),
            "row_counts": {name: frame.count() for name, frame in tables.items()},
            "deleted_current_accounts": tables[ENTITIES["accounts"]["table"]]
            .where("is_current AND is_deleted")
            .count(),
            "deleted_current_invoices": tables[ENTITIES["invoices"]["table"]]
            .where("is_current AND is_deleted")
            .count(),
            "correlated_invoices": len(expected),
            "correlated_invoice_history_rows": len(invoice_rows),
            "correlated_active_journal_rows": len(entries),
            "financial_type": "DECIMAL(18,4)",
            "totals_by_currency": {
                currency: format(value, ".4f")
                for currency, value in sorted(totals.items())
            },
            "one_current_version_per_key": True,
            "version_intervals_nonoverlapping": True,
            "restart_changed_rows": 0,
            "previous_process_checkpoint_verified": prior is not None,
            "table_digests": after,
            "query": second,
        }
        proof_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
        normalized.unpersist()
        return report
    finally:
        spark.stop()


if __name__ == "__main__":
    options = arguments()
    verify(options.lakehouse_dir, options.dataset)
