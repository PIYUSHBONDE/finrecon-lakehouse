"""Fixture proof for memo validation, raw preservation, and extraction replay."""

import json
from pathlib import Path

from .exceptions import propose
from .gold import run
from .gold_inputs import gold_path
from .quality import atomic_json, evaluate
from .silver import arguments, table_path
from .verify_cdc import digest


def verify(root, dataset):
    from .runtime import spark_session
    from pyspark.sql import functions as F

    spark = spark_session("FinRecon exception verification")
    directory = Path("/workspace/pipelines/output") / dataset
    source = json.loads((directory / "verification.json").read_text(encoding="utf-8"))
    canonical = json.loads(
        (
            Path(source["source_report"]).parent / "canonical_transactions.json"
        ).read_text(encoding="utf-8")
    )
    expected = [row["invoice_id"] for row in canonical]
    messy = len(expected) // 50
    output = (
        Path("/workspace/pipelines/output") / dataset / "exception_verification.json"
    )
    prior = json.loads(output.read_text(encoding="utf-8")) if output.exists() else None
    try:
        run(spark, root, dataset)
        raw_path = str(table_path(root, dataset, "silver_bank_settlements"))
        before_raw = digest(spark.read.format("delta").load(raw_path))
        first = propose(spark, root, dataset)
        run(spark, root, dataset)
        tables = (
            "gold_reconciliation",
            "state_unreconciled_open_items",
            "state_exception_links",
            "exception_extraction_audit",
        )

        def read(name):
            return spark.read.format("delta").load(str(gold_path(root, dataset, name)))

        links = read("state_exception_links").collect()
        assert len(links) == messy
        assert all(
            row.proposal_hash and row.active and row.gateway_ref for row in links
        )
        audit = read("exception_extraction_audit")
        assert audit.count() == audit.select("proposal_id").distinct().count() == messy
        assert audit.where("decision = 'VALIDATED'").count() == messy
        decisions = read("gold_reconciliation")
        fixture = decisions.where(F.col("invoice_id").isin(expected))
        assert (
            fixture.count()
            == fixture.where("status = 'MATCHED'").count()
            == len(expected)
        )
        assert read("gold_reconciliation").where("item_type = 'BANK'").count() == 0
        opened = read("state_unreconciled_open_items")
        assert opened.where(F.col("invoice_id").isin(expected)).count() == 0
        historical_open = opened.count()
        assert historical_open == decisions.where("status = 'OPEN'").count()
        before = {name: digest(read(name)) for name in tables}
        assert propose(spark, root, dataset)["targeted_banks"] == 0
        assert run(spark, root, dataset)["source_changed"] is False
        after = {name: digest(read(name)) for name in tables}
        assert before == after
        assert before_raw == digest(spark.read.format("delta").load(raw_path))
        assert (
            spark.read.format("delta")
            .load(raw_path)
            .where("gateway_ref IS NULL OR gateway_ref = ''")
            .count()
            == messy
        )
        gate = evaluate(spark, root, dataset, "gold")
        assert gate["passed"]
        if prior:
            assert prior["table_digests"] == after
        report = {
            "dataset": dataset,
            "validated_links": messy,
            "matched_fixture_invoices": len(expected),
            "remaining_historical_open_items": historical_open,
            "raw_bank_unchanged": True,
            "replay_changed_rows": 0,
            "previous_process_state_verified": prior is not None,
            "method": "invoice_uuid_regex_v1",
            "first_extraction_this_run": first,
            "gold_quality_passed": True,
            "table_digests": after,
        }
        atomic_json(output, report)
        print(json.dumps(report, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    options = arguments()
    verify(options.lakehouse_dir, options.dataset)
