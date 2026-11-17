"""Independent Decimal oracle and fresh-process proof for synthetic FX reporting."""

import argparse
import csv
import json
import re
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from pathlib import Path

from .fx import run
from .gold_inputs import gold_path
from .quality import atomic_json
from .silver import table_path
from .verify_cdc import digest


def verify(root, dataset, file):
    from pyspark.sql.types import DecimalType
    from .runtime import spark_session

    output = Path("/workspace/pipelines/output") / dataset
    source = json.loads((output / "verification.json").read_text(encoding="utf-8"))
    canonical = json.loads(
        (
            Path(source["source_report"]).parent / "canonical_transactions.json"
        ).read_text(encoding="utf-8")
    )
    expected = {row["invoice_id"]: row for row in canonical}
    with Path(file).open(newline="", encoding="utf-8") as stream:
        quotes = {
            (row["rate_date"], row["currency"]): Decimal(row["rate"])
            for row in csv.DictReader(stream)
        }
    path = output / "fx_verification.json"
    prior = json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
    spark = spark_session("FinRecon FX fixture verification")
    try:

        def gold(name):
            return spark.read.format("delta").load(str(gold_path(root, dataset, name)))

        protected = {
            name: digest(gold(name))
            for name in ("gold_reconciliation", "state_unreconciled_open_items")
        }
        before_bank = digest(
            spark.read.format("delta").load(
                str(table_path(root, dataset, "silver_bank_settlements"))
            )
        )
        first = run(spark, root, dataset, file)
        assert (
            first["passed"]
            and first["eligible_payments"] == len(expected)
            and first["rate_rows"] == len(quotes)
        )
        valued = gold("gold_fx_valuation")
        for name in ("amount_dec", "fx_rate_dec", "converted_amount_dec"):
            assert valued.schema[name].dataType == DecimalType(18, 4)
        rows = valued.collect()
        assert len(rows) == len({row.item_id for row in rows}) == len(expected)
        total = Decimal("0.0000")
        with localcontext() as context:
            context.prec = 50
            for row in rows:
                clean = expected[row.invoice_id]
                date = clean["event_ts"][:10]
                amount = Decimal(clean["amount_dec"])
                rate = quotes[(date, clean["currency"])]
                oracle = (amount * rate).quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_EVEN
                )
                assert (
                    row.valuation_status == "VALUED"
                    and row.business_date.isoformat() == date
                )
                assert (
                    row.amount_dec == amount
                    and row.fx_rate_dec == rate
                    and row.converted_amount_dec == oracle
                )
                assert row.reporting_currency == "INR" and "SYNTHETIC" in row.provenance
                total += oracle
        assert first["converted_total_dec"] == format(total, ".4f")
        before = digest(valued)
        second = run(spark, root, dataset, file)
        after = digest(gold("gold_fx_valuation"))
        assert before == after and second["passed"]
        assert protected == {name: digest(gold(name)) for name in protected}
        assert before_bank == digest(
            spark.read.format("delta").load(
                str(table_path(root, dataset, "silver_bank_settlements"))
            )
        )
        comparable = prior is not None and prior["book_id"] == first["book_id"]
        if comparable:
            assert prior["valuation_digest"] == after
        report = {
            "dataset": dataset,
            "book_id": first["book_id"],
            "synthetic_rates_only": True,
            "payments_valued": len(expected),
            "rate_rows": len(quotes),
            "reporting_currency": "INR",
            "converted_total_dec": format(total, ".4f"),
            "rounding": "HALF_EVEN_4",
            "financial_type": "DECIMAL(18,4)",
            "independent_decimal_oracle_passed": True,
            "reconciliation_and_bank_unchanged": True,
            "restart_changed_rows": 0,
            "previous_process_state_verified": comparable,
            "valuation_digest": after,
        }
        atomic_json(path, report)
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="portfolio_demo")
    parser.add_argument("--lakehouse-dir", type=Path, default=Path("/lakehouse"))
    parser.add_argument(
        "--rates-file",
        type=Path,
        default=Path("/workspace/data_generator/fixtures/fx_synthetic_20261111.csv"),
    )
    options = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", options.dataset):
        parser.error("Invalid dataset name")
    verify(options.lakehouse_dir, options.dataset, options.rates_file)
