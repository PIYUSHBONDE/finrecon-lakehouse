"""Exact rounding, dated quotes, bounded broadcast, quarantine, and replay."""

import csv
import importlib.util
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path

from pipelines.fx import FIELDS, FxContractError, convert, load_book, run
from pipelines.gold_inputs import gold_path
from pipelines.silver import table_path

DAY = date(2026, 11, 11)


@unittest.skipUnless(
    importlib.util.find_spec("pyspark") and importlib.util.find_spec("delta"),
    "Run in Spark container",
)
class FxTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pipelines.runtime import spark_session

        cls.spark = spark_session("FinRecon FX tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def book(self, rows, name="rates.csv"):
        file = self.root / name
        with file.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(FIELDS)
            writer.writerows(rows)
        return file

    def quote(self, currency="USD", value="80.0000", day="2026-11-11"):
        return (
            day,
            currency,
            "INR",
            value,
            "TEST-" + currency,
            "SYNTHETIC_TEST_NOT_MARKET_DATA",
        )

    def test_rounding_exact_date_overflow_and_broadcast(self):
        from pyspark.sql.types import DecimalType

        rates = self.spark.createDataFrame(
            [
                (DAY, "GBP", "INR", Decimal("0.5000"), "q1", "synthetic"),
                (DAY, "INR", "INR", Decimal("1.0000"), "q2", "synthetic"),
                (DAY, "USD", "INR", Decimal("80.0000"), "q3", "synthetic"),
            ],
            "rate_date DATE, currency STRING, reporting_currency STRING, fx_rate_dec DECIMAL(18,4), rate_id STRING, provenance STRING",
        )
        payments = self.spark.createDataFrame(
            [
                ("tie_even_zero", DAY, "GBP", Decimal("0.0001")),
                ("tie_even_two", DAY, "GBP", Decimal("0.0003")),
                ("max_exact", DAY, "INR", Decimal("99999999999999.9999")),
                ("overflow", DAY, "USD", Decimal("99999999999999.9999")),
                ("missing_currency", DAY, "JPY", Decimal("1.0000")),
                ("no_prior_day_fallback", date(2026, 11, 12), "USD", Decimal("1.0000")),
                ("missing_date", None, "USD", Decimal("1.0000")),
            ],
            "item_id STRING, business_date DATE, currency STRING, amount_dec DECIMAL(18,4)",
        )
        valued = convert(payments, rates)
        actual = {row.item_id: row for row in valued.collect()}
        self.assertEqual(
            valued.schema["converted_amount_dec"].dataType, DecimalType(18, 4)
        )
        self.assertEqual(
            actual["tie_even_zero"].converted_amount_dec, Decimal("0.0000")
        )
        self.assertEqual(actual["tie_even_two"].converted_amount_dec, Decimal("0.0002"))
        self.assertEqual(
            actual["max_exact"].converted_amount_dec, Decimal("99999999999999.9999")
        )
        self.assertEqual(actual["overflow"].valuation_status, "AMOUNT_OVERFLOW")
        self.assertIsNone(actual["overflow"].converted_amount_dec)
        for key in ("missing_currency", "no_prior_day_fallback"):
            self.assertEqual(actual[key].valuation_status, "MISSING_RATE")
        self.assertEqual(
            actual["missing_date"].valuation_status, "MISSING_BUSINESS_DATE"
        )
        plan = valued._jdf.queryExecution().executedPlan().toString()
        self.assertIn("BroadcastHashJoin", plan)
        self.assertNotIn(" AS DOUBLE", plan.upper())

    def test_contract_quarantine_immutable_books_and_duplicate_pairs(self):
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        invalid = self.book(
            [
                self.quote(value=value)
                for value in ("-1.0000", "0", "0.12345", "100000000000000.0000")
            ]
            + [
                self.quote(currency="INVALID"),
                self.quote(currency="INR", value="2.0000"),
                self.quote(day="2026-02-30"),
                self.quote(),
            ],
            "bad.csv",
        )
        with self.assertRaisesRegex(FxContractError, "quarantined"):
            load_book(self.spark, self.root, "test", invalid)
        quarantine = str(table_path(self.root, "test", "silver_fx_rate_quarantine"))
        self.assertEqual(self.spark.read.format("delta").load(quarantine).count(), 7)
        with self.assertRaises(FxContractError):
            load_book(self.spark, self.root, "test", invalid)
        self.assertEqual(self.spark.read.format("delta").load(quarantine).count(), 7)
        self.assertFalse(
            DeltaTable.isDeltaTable(
                self.spark, str(table_path(self.root, "test", "silver_fx_rates"))
            )
        )
        duplicate = self.book(
            [self.quote(), self.quote(value="81.0000")], "duplicate.csv"
        )
        with self.assertRaisesRegex(FxContractError, "Duplicate"):
            load_book(self.spark, self.root, "test", duplicate)
        good = self.book([self.quote(), self.quote(currency="INR", value="1.0000")])
        rates, identity, count = load_book(self.spark, self.root, "test", good)
        self.assertEqual(count, 2)
        self.assertEqual(load_book(self.spark, self.root, "test", good)[1], identity)
        revision = self.book([self.quote(value="81.0000")], "revision.csv")
        _, new_id, _ = load_book(self.spark, self.root, "test", revision)
        self.assertNotEqual(identity, new_id)
        self.assertEqual(
            rates.where(F.col("currency") == "USD").first().fx_rate_dec,
            Decimal("80.0000"),
        )

    def test_replay_reopened_decision_and_missing_rate_block(self):
        from delta.tables import DeltaTable
        from pipelines.verify_cdc import digest

        book = self.book([self.quote(), self.quote(currency="INR", value="1.0000")])
        decisions = self.spark.createDataFrame(
            [("I1", "INV1", "G1", "USD", Decimal("0.0001"), "hash", "MATCHED")],
            "item_id STRING, invoice_id STRING, gateway_key STRING, currency STRING, amount_dec DECIMAL(18,4), content_hash STRING, status STRING",
        )
        decision_path = str(gold_path(self.root, "test", "gold_reconciliation"))
        decisions.write.format("delta").save(decision_path)
        gateways = self.spark.createDataFrame(
            [("G1", DAY)], "entity_key STRING, event_date DATE"
        )
        gateways.write.format("delta").save(
            str(table_path(self.root, "test", "silver_gateway_events"))
        )
        first = run(self.spark, self.root, "test", book)
        self.assertTrue(first["passed"])
        valuation_path = str(gold_path(self.root, "test", "gold_fx_valuation"))

        def read():
            return self.spark.read.format("delta").load(valuation_path)

        self.assertEqual(read().first().converted_amount_dec, Decimal("0.0080"))
        before = digest(read())
        self.assertTrue(run(self.spark, self.root, "test", book)["passed"])
        self.assertEqual(digest(read()), before)
        DeltaTable.forPath(self.spark, decision_path).update(set={"status": "'OPEN'"})
        self.assertTrue(run(self.spark, self.root, "test", book)["passed"])
        self.assertEqual(read().count(), 0)
        DeltaTable.forPath(self.spark, decision_path).update(
            set={"status": "'MATCHED'", "currency": "'EUR'"}
        )
        blocked = run(self.spark, self.root, "test", book)
        self.assertFalse(blocked["passed"])
        self.assertEqual(read().first().valuation_status, "MISSING_RATE")
        self.assertIsNone(read().first().converted_amount_dec)
        self.assertEqual(
            self.spark.read.format("delta").load(decision_path).first().status,
            "MATCHED",
        )
        # Each payment fits DECIMAL(18,4), while their reporting total does not.
        large = Decimal("99999999999999.9999")
        self.spark.createDataFrame(
            [
                ("I1", "INV1", "G1", "INR", large, "h1", "MATCHED"),
                ("I2", "INV2", "G2", "INR", large, "h2", "MATCHED"),
            ],
            decisions.schema,
        ).write.format("delta").mode("overwrite").save(decision_path)
        self.spark.createDataFrame(
            [("G1", DAY), ("G2", DAY)], gateways.schema
        ).write.format("delta").mode("overwrite").save(
            str(table_path(self.root, "test", "silver_gateway_events"))
        )
        overflow = run(self.spark, self.root, "test", book)
        self.assertFalse(overflow["passed"])
        self.assertFalse(overflow["aggregate_representable"])
        self.assertIsNone(overflow["converted_total_dec"])
        self.assertEqual(overflow["status_counts"], {"VALUED": 2})
