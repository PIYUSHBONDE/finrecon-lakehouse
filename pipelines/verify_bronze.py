"""Verify raw Bronze ingestion and query restarts against a correlated fixture."""

import argparse
import json
from collections import Counter
from pathlib import Path

from .bronze import TABLES, ingest, parse_args, paths
from .runtime import spark_session


def verify(source_report, dataset):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    source = json.loads(source_report.read_text(encoding="utf-8"))
    canonical = json.loads(
        (source_report.parent / "canonical_transactions.json").read_text(
            encoding="utf-8"
        )
    )
    canonical_map = {row["invoice_id"]: row for row in canonical}
    count = len(canonical_map)
    retries, poison, messy = count // 20, count // 100, count // 50
    args = parse_args(
        [
            "--gateway-topics",
            source["gateway_topic"],
            "--bank-dir",
            str(source_report.parent / "bank_settlements"),
            "--dataset",
            dataset,
        ]
    )
    output = Path("/workspace/pipelines/output") / dataset
    output.mkdir(parents=True, exist_ok=True)
    prior_path = output / "verification.json"
    prior = (
        json.loads(prior_path.read_text(encoding="utf-8"))
        if prior_path.exists()
        else None
    )
    spark = spark_session("FinRecon Bronze verification")
    try:
        first = ingest(spark, args)
        frames = {
            table: spark.read.format("delta").load(str(paths(args, table)[0]))
            for table in TABLES
        }
        counts = {table: frame.count() for table, frame in frames.items()}
        assert (
            counts[TABLES[0]] == count + retries and counts[TABLES[2]] == count
        ), counts
        gateway = frames[TABLES[0]]
        assert (
            gateway.select("topic", "partition", "offset").distinct().count()
            == count + retries
        )
        actual_gateway = [
            json.loads(row.raw_payload)
            for row in gateway.select("raw_payload").collect()
        ]
        id_counts = Counter(row["idempotency_key"] for row in actual_gateway)
        assert Counter(id_counts.values()) == {1: count - retries, 2: retries}
        originals = {row["invoice_id"]: row for row in actual_gateway}
        assert originals.keys() == canonical_map.keys()
        assert (
            sum(row["merchant_id"] == "MERCHANT_ENT_01" for row in originals.values())
            == count * 65 // 100
        )
        assert (
            sum(
                row["amount_dec"] != canonical_map[identity]["amount_dec"]
                or row["currency"] != canonical_map[identity]["currency"]
                for identity, row in originals.items()
            )
            == poison
        )
        for row in gateway.select("raw_value", "raw_payload").collect():
            assert bytes(row.raw_value).decode("utf-8") == row.raw_payload

        bank = frames[TABLES[2]]
        assert isinstance(bank.schema["settled_amount_dec"].dataType, StringType)
        assert bank.where(F.col("_corrupt_record").isNotNull()).count() == 0
        bank_rows = bank.collect()
        assert sum(not row.gateway_ref for row in bank_rows) == messy
        for row in bank_rows:
            identity = "INV-" + row.settlement_id.removeprefix("BANK-")
            clean = canonical_map[identity]
            assert row.settled_amount_dec == clean["amount_dec"]
            assert row.currency == clean["currency"]
            assert row.merchant_id == clean["merchant_id"]
            assert identity in row.remittance_memo

        cdc = (
            frames[TABLES[1]]
            .withColumn(
                "invoice_id",
                F.get_json_object("raw_payload", "$.payload.after.invoice_id"),
            )
            .where(F.col("invoice_id").isin(list(canonical_map)))
        )
        cdc_rows = [
            json.loads(row.raw_payload)["payload"]
            for row in cdc.select("raw_payload").collect()
        ]
        assert Counter((row["source"]["table"], row["op"]) for row in cdc_rows) == {
            ("invoices", "c"): count,
            ("invoices", "u"): count,
            ("ledger_entries", "c"): count * 2,
        }
        for row in cdc_rows:
            if row["source"]["table"] == "invoices":
                assert (
                    row["after"]["total_amount"]
                    == canonical_map[row["after"]["invoice_id"]]["amount_dec"]
                )
        for table in TABLES[:2]:
            frame = frames[table]
            assert (
                frame.select("topic", "partition", "offset").distinct().count()
                == counts[table]
            )

        second = ingest(spark, args)
        after_restart = {
            table: spark.read.format("delta").load(str(paths(args, table)[0])).count()
            for table in TABLES
        }
        assert after_restart == counts
        for table in TABLES:
            assert (
                first["tables"][table]["query_id"]
                == second["tables"][table]["query_id"]
            )
            assert first["tables"][table]["run_id"] != second["tables"][table]["run_id"]
            assert second["tables"][table]["input_rows_this_run"] == 0
            if prior:
                assert (
                    prior["queries"][table]["query_id"]
                    == first["tables"][table]["query_id"]
                )
                assert first["tables"][table]["input_rows_this_run"] == 0
        report = {
            "dataset": dataset,
            "source_report": str(source_report),
            "row_counts": counts,
            "correlated_cdc_events": len(cdc_rows),
            "gateway_originals": len(originals),
            "gateway_retries_retained": retries,
            "poison_pills_retained": poison,
            "messy_memos_retained": messy,
            "financial_strings_preserved": True,
            "kafka_offsets_unique": True,
            "query_restart_added_rows": 0,
            "previous_process_checkpoint_verified": prior is not None,
            "queries": second["tables"],
        }
        prior_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2), flush=True)
        return report
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-report", type=Path, required=True)
    parser.add_argument("--dataset", default="portfolio_demo")
    options = parser.parse_args()
    verify(options.source_report, options.dataset)
