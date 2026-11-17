"""Verify gateway/bank Silver and an explicit correction using fixture truth."""

import json
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from .contracts import prepare
from .replay import replay
from .runtime import spark_session
from .silver import arguments, run, table_path


def verify(root, dataset, as_of_date):
    from pyspark.sql import functions as F
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
    count = len(expected)
    poison, messy = count // 100, count // 50
    if any(
        date.fromisoformat(row["event_ts"][:10]) < as_of_date - timedelta(days=6)
        for row in canonical
    ):
        raise ValueError(
            "Fixture is outside the seven-day window; generate a fresh fixture or set an explicit --as-of-date for backfill"
        )
    proof_path = output / "silver_verification.json"
    previous = (
        json.loads(proof_path.read_text(encoding="utf-8"))
        if proof_path.exists()
        else None
    )
    spark = spark_session("FinRecon Silver verification")
    try:
        first = run(spark, root, dataset, as_of_date)

        def read(table):
            return spark.read.format("delta").load(
                str(table_path(root, dataset, table))
            )

        raw_gateway = spark.read.format("delta").load(
            str(Path(root) / "bronze" / dataset / "bronze_gateway_events")
        )
        rejected = prepare(raw_gateway, "gateway", as_of_date).where(
            F.size("failed_rules") > 0
        )
        rejected_ids = {row.record_id for row in rejected.select("record_id").collect()}
        quarantine = read("silver_quarantine").where(
            "source_table IN ('bronze_gateway_events', 'bronze_bank_settlements')"
        )
        quarantined = quarantine.collect()
        assert {row.record_id for row in quarantined} == rejected_ids
        bad_keys = {row.entity_key for row in quarantined}
        assert len(bad_keys) == poison
        gateway_before = read("silver_gateway_events").count()
        replayed_keys = {
            row.entity_key for row in quarantined if row.status == "REPLAYED"
        }
        assert gateway_before == count - poison + len(replayed_keys)
        bank = read("silver_bank_settlements")
        assert bank.count() == count
        assert isinstance(bank.schema["settled_amount_dec"].dataType, DecimalType)
        assert bank.schema["settled_amount_dec"].dataType == DecimalType(18, 4)
        assert (
            bank.where(
                F.col("gateway_ref").isNull() | (F.length("gateway_ref") == 0)
            ).count()
            == messy
        )

        corrections = []
        for row in quarantined:
            original = json.loads(row.raw_payload)
            clean = expected[original["invoice_id"]]
            corrected = {
                **original,
                "amount_dec": clean["amount_dec"],
                "currency": clean["currency"],
                "merchant_id": clean["merchant_id"],
                "event_ts": clean["event_ts"],
            }
            corrections.append(
                {"record_id": row.record_id, "corrected_payload": json.dumps(corrected)}
            )
        corrections_path = output / "demo_corrections.jsonl"
        # This is an explicit test-only repair from the fixture's known truth.
        # The normal Silver job never infers or edits a financial value.
        corrections_path.write_text(
            "\n".join(json.dumps(row) for row in corrections), encoding="utf-8"
        )
        replay_result = replay(spark, root, dataset, as_of_date, corrections_path)
        gateway = read("silver_gateway_events")
        assert gateway.schema["amount_dec"].dataType == DecimalType(18, 4)
        gateway_rows = gateway.collect()
        assert len(gateway_rows) == count
        assert len({row.idempotency_key for row in gateway_rows}) == count
        totals = defaultdict(Decimal)
        for row in gateway_rows:
            clean = expected[row.invoice_id]
            assert row.amount_dec == Decimal(clean["amount_dec"])
            assert (
                row.currency == clean["currency"]
                and row.merchant_id == clean["merchant_id"]
            )
            totals[row.currency] += row.amount_dec
        bank_totals = defaultdict(Decimal)
        for row in read("silver_bank_settlements").collect():
            identity = "INV-" + row.settlement_id.removeprefix("BANK-")
            clean = expected[identity]
            assert row.settled_amount_dec == Decimal(clean["amount_dec"])
            assert row.currency == clean["currency"]
            bank_totals[row.currency] += row.settled_amount_dec
        assert totals == bank_totals
        acknowledged = (
            read("silver_quarantine")
            .where(
                "source_table IN ('bronze_gateway_events', 'bronze_bank_settlements')"
            )
            .collect()
        )
        assert len(acknowledged) == len(rejected_ids)
        assert all(
            row.status == "REPLAYED" and row.corrected_payload for row in acknowledged
        )
        assert {row.record_id: row.raw_payload for row in acknowledged} == {
            row.record_id: row.raw_payload for row in quarantined
        }
        replayed_at = {
            row.record_id: row.replayed_at.isoformat() for row in acknowledged
        }
        assert replay(spark, root, dataset, as_of_date, corrections_path) == {
            "replayed": 0,
            "still_pending": 0,
        }

        second = run(spark, root, dataset, as_of_date)
        assert read("silver_gateway_events").count() == count
        assert read("silver_bank_settlements").count() == count
        assert read("silver_quarantine").where(
            "source_table IN ('bronze_gateway_events', 'bronze_bank_settlements')"
        ).count() == len(rejected_ids)
        for source in first:
            assert first[source]["query_id"] == second[source]["query_id"]
            assert first[source]["run_id"] != second[source]["run_id"]
            if previous:
                assert (
                    previous["queries"][source]["query_id"] == first[source]["query_id"]
                )
        if previous:
            assert previous["quarantine_replayed_at"] == replayed_at
        report = {
            "dataset": dataset,
            "as_of_date": as_of_date.isoformat(),
            "merge_window_start": (as_of_date - timedelta(days=6)).isoformat(),
            "valid_gateway_keys_before_demo_replay": count - poison,
            "gateway_rows_before_this_replay": gateway_before,
            "gateway_rows_after_demo_replay": count,
            "bank_rows": count,
            "quarantined_raw_records": len(rejected_ids),
            "quarantine_status": "REPLAYED",
            "replay_result_this_run": replay_result,
            "decimal_type": "DECIMAL(18,4)",
            "original_quarantine_payloads_preserved": True,
            "repeated_replay_added_rows": 0,
            "stream_restart_added_rows": 0,
            "previous_process_checkpoint_verified": previous is not None,
            "quarantine_replayed_at": replayed_at,
            "totals_by_currency": {
                currency: format(value, ".4f")
                for currency, value in sorted(totals.items())
            },
            "queries": second,
            "cdc_scd2_stage": "not part of this gateway/bank slice",
        }
        proof_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
        return report
    finally:
        spark.stop()


if __name__ == "__main__":
    options = arguments()
    verify(options.lakehouse_dir, options.dataset, options.as_of_date)
