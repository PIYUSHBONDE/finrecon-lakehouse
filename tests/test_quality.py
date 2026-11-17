"""Real Delta quality gates accept open exceptions and reject corrupted decisions."""

import importlib.util
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from pipelines.gold_inputs import gold_path
from pipelines.quality import evaluate


@unittest.skipUnless(
    importlib.util.find_spec("pyspark") and importlib.util.find_spec("delta"),
    "Run in Spark container",
)
class QualityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pipelines.runtime import spark_session

        cls.spark = spark_session("FinRecon quality tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_matched_corruption_and_missing_open_item_are_blocked(self):
        from delta.tables import DeltaTable

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            schema = "item_id STRING, item_type STRING, status STRING, reasons ARRAY<STRING>, gateway_count LONG, bank_count LONG, journal_count LONG, amount_dec DECIMAL(18,4), gateway_amount_dec DECIMAL(18,4), bank_amount_dec DECIMAL(18,4), ledger_debit_dec DECIMAL(18,4), ledger_credit_dec DECIMAL(18,4), settlement_id STRING, content_hash STRING"
            amount = Decimal("0.0001")
            rows = [
                (
                    "I:1",
                    "INVOICE",
                    "MATCHED",
                    [],
                    1,
                    1,
                    2,
                    amount,
                    amount,
                    amount,
                    amount,
                    amount,
                    "B1",
                    "hash1",
                ),
                (
                    "I:2",
                    "INVOICE",
                    "OPEN",
                    ["MISSING_BANK"],
                    1,
                    0,
                    2,
                    amount,
                    amount,
                    None,
                    amount,
                    amount,
                    None,
                    "hash2",
                ),
            ]
            frame = self.spark.createDataFrame(rows, schema)
            result_path = str(gold_path(root, "test", "gold_reconciliation"))
            open_path = str(gold_path(root, "test", "state_unreconciled_open_items"))
            frame.write.format("delta").save(result_path)
            frame.where("status = 'OPEN'").write.format("delta").save(open_path)
            result = evaluate(self.spark, root, "test", "gold")
            self.assertTrue(result["passed"], result)
            self.assertEqual(result["metrics"]["open_items"], 1)
            DeltaTable.forPath(self.spark, result_path).update(
                "status = 'MATCHED'",
                {"bank_amount_dec": "CAST(0.0002 AS DECIMAL(18,4))"},
            )
            DeltaTable.forPath(self.spark, open_path).delete()
            failed = evaluate(self.spark, root, "test", "gold")
            self.assertFalse(failed["passed"])
            names = {item["name"] for item in failed["checks"] if not item["passed"]}
            self.assertIn("matched_financial_invariants", names)
            self.assertIn("open_items_exactly_match_decisions", names)
