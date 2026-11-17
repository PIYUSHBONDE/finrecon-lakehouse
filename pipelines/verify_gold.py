"""Verify three-way matches and durable exceptions on the correlated local fixture."""

import json
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

from .gold import run
from .gold_inputs import gold_path
from .runtime import spark_session
from .silver import arguments
from .verify_cdc import digest
from .exception_links import linked_banks
from .gold_inputs import lookup


def verify(root, dataset):
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    output = Path("/workspace/pipelines/output") / dataset
    bronze = json.loads((output / "verification.json").read_text(encoding="utf-8"))
    canonical = json.loads(
        (
            Path(bronze["source_report"]).parent / "canonical_transactions.json"
        ).read_text(encoding="utf-8")
    )
    expected = {row["invoice_id"]: row for row in canonical}
    count, messy = len(expected), len(expected) // 50
    proof_path = output / "gold_verification.json"
    prior = (
        json.loads(proof_path.read_text(encoding="utf-8"))
        if proof_path.exists()
        else None
    )
    spark = spark_session("FinRecon Gold verification")
    try:
        first = run(spark, root, dataset)

        def read(table):
            return spark.read.format("delta").load(str(gold_path(root, dataset, table)))

        result = read("gold_reconciliation")
        opened = read("state_unreconciled_open_items")
        ids = spark.createDataFrame([(key,) for key in expected], "lookup_key STRING")
        cache = gold_path(root, dataset, "state_reconciliation_inputs")
        references = (
            lookup(spark, cache, "gateway", ids)
            .selectExpr("gateway_ref AS lookup_key")
            .distinct()
        )
        applied_links = linked_banks(spark, cache, references).collect()
        assert len(applied_links) <= messy
        for bank in applied_links:
            clean = expected["INV-" + bank.key.removeprefix("BANK-")]
            assert clean["invoice_id"] in bank.remittance_memo
            assert (
                bank.amount_dec == Decimal(clean["amount_dec"])
                and bank.currency == clean["currency"]
                and bank.merchant_id == clean["merchant_id"]
            )
        expected_matches = count - messy + len(applied_links)
        for name in (
            "amount_dec",
            "gateway_amount_dec",
            "bank_amount_dec",
            "ledger_debit_dec",
            "ledger_credit_dec",
        ):
            assert result.schema[name].dataType == DecimalType(18, 4)
        invoices = result.where(F.col("invoice_id").isin(list(expected))).collect()
        assert len(invoices) == count
        assert Counter(row.status for row in invoices) == Counter(
            {"MATCHED": expected_matches, "OPEN": count - expected_matches}
        )
        totals = defaultdict(Decimal)
        for row in invoices:
            clean = expected[row.invoice_id]
            assert (
                row.amount_dec
                == row.gateway_amount_dec
                == row.ledger_debit_dec
                == row.ledger_credit_dec
                == Decimal(clean["amount_dec"])
            )
            assert (
                row.currency == clean["currency"]
                and row.merchant_id == clean["merchant_id"]
            )
            if row.status == "MATCHED":
                assert row.reasons == [] and row.bank_amount_dec == row.amount_dec
                assert (
                    row.gateway_count == row.bank_count == 1 and row.journal_count == 2
                )
                totals[row.currency] += row.amount_dec
            else:
                assert row.reasons == [
                    "MISSING_BANK",
                    "AMOUNT_MISMATCH",
                    "CURRENCY_MISMATCH",
                    "MERCHANT_MISMATCH",
                ]
        orphan_banks = result.where("item_type = 'BANK'").collect()
        assert len(orphan_banks) == messy - len(applied_links)
        assert all(
            row.reasons == ["MISSING_GATEWAY_REFERENCE"]
            and "inv#INV-" in row.remittance_memo
            for row in orphan_banks
        )
        assert (
            len(
                {
                    row.settlement_id
                    for row in result.where("status = 'MATCHED'")
                    .select("settlement_id")
                    .collect()
                }
            )
            == expected_matches
        )
        assert opened.count() == result.where("status = 'OPEN'").count()
        assert result.select("item_id").distinct().count() == result.count()
        tables = (
            "gold_reconciliation",
            "state_unreconciled_open_items",
            "state_reconciliation_inputs",
            "state_reconciliation_cursors",
        )
        before = {name: digest(read(name)) for name in tables}
        second = run(spark, root, dataset)
        after = {name: digest(read(name)) for name in tables}
        assert (
            second == {"source_changed": False, "evaluated_items": 0}
            and before == after
        )
        comparable = (
            prior is not None
            and prior.get("proof_version") == 2
            and prior["correlated_matched"] == expected_matches
        )
        if comparable:
            assert prior["table_digests"] == after
        report = {
            "dataset": dataset,
            "proof_version": 2,
            "correlated_invoices": len(invoices),
            "correlated_matched": expected_matches,
            "correlated_open_invoices": count - expected_matches,
            "validated_memo_links_applied": len(applied_links),
            "bank_memo_exceptions": len(orphan_banks),
            "total_reconciliation_rows": result.count(),
            "total_open_items": opened.count(),
            "matched_totals_by_currency": {
                key: format(value, ".4f") for key, value in sorted(totals.items())
            },
            "financial_type": "DECIMAL(18,4)",
            "restart_changed_rows": 0,
            "previous_process_state_verified": comparable,
            "first_run": first,
            "table_digests": after,
        }
        proof_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
        return report
    finally:
        spark.stop()


if __name__ == "__main__":
    options = arguments()
    verify(options.lakehouse_dir, options.dataset)
