"""Exact reconciliation, delayed settlements, CDC invalidation, and retry proofs."""

import importlib.util
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from pipelines.gold import bank_results, invoice_results, run
from pipelines.gold_inputs import SCHEMA, TABLES, bucket, gold_path
from pipelines.silver import table_path


@unittest.skipUnless(
    importlib.util.find_spec("pyspark") and importlib.util.find_spec("delta"),
    "Run Gold tests in the Spark container",
)
class GoldTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pipelines.runtime import spark_session

        cls.spark = spark_session("FinRecon Gold tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def read(self, table):
        return self.spark.read.format("delta").load(
            str(gold_path(self.root, "test", table))
        )

    def test_exact_money_ambiguity_and_bucket_pruning(self):
        from pyspark.sql import functions as F
        from pyspark.sql.types import DecimalType

        rows = []
        for index in range(9):
            identity = f"I{index}"
            merchant = (
                "MERCHANT_ENT_01"  # Deliberately the same merchant for every invoice.
            )
            amount = Decimal("0.0001")
            shared = dict(
                invoice_id=identity,
                merchant_id=merchant,
                currency="USD",
                active=True,
                source_commit=1,
            )

            def add(kind, key, lookup_key, **values):
                rows.append(
                    {
                        **shared,
                        "kind": kind,
                        "key": key,
                        "lookup_key": lookup_key,
                        **values,
                    }
                )

            add(
                "invoice",
                identity,
                identity,
                account_id=f"A{index}",
                amount_dec=amount,
                source_status="PAID",
            )
            add("account", f"A{index}", f"A{index}", account_id=f"A{index}")
            add(
                "gateway",
                f"G{index}",
                identity,
                gateway_ref=f"R{index}",
                amount_dec=amount,
            )
            add("gateway_ref", f"G{index}", f"R{index}", gateway_ref=f"R{index}")
            add(
                "journal",
                f"D{index}",
                identity,
                account_id=f"A{index}",
                debit_dec=amount,
                credit_dec=Decimal("0"),
            )
            add(
                "journal",
                f"C{index}",
                identity,
                account_id=f"A{index}",
                debit_dec=Decimal("0"),
                credit_dec=amount,
            )
            add(
                "bank",
                f"B{index}",
                f"R{index}",
                gateway_ref=f"R{index}",
                amount_dec=amount,
                remittance_memo="memo",
            )
        # Each case isolates a material failure; no cross-currency conversion or
        # random winner is allowed to turn a mismatch into a match.
        for row in rows:
            if row["kind"] == "bank" and row["invoice_id"] == "I1":
                row["amount_dec"] = Decimal("0.0002")
            if row["kind"] == "bank" and row["invoice_id"] == "I2":
                row["currency"] = "EUR"
            if row["kind"] == "bank" and row["invoice_id"] == "I3":
                row["merchant_id"] = "WRONG"
            if row["key"] == "C4":
                row["credit_dec"] = Decimal("0.0002")
            if row["kind"] == "bank" and row["invoice_id"] == "I5":
                row["gateway_ref"] = ""
                row["lookup_key"] = "unreferenced:B5"
                row["remittance_memo"] = "inv#I5: do not guess a link here"
            if row["kind"] == "account" and row["invoice_id"] == "I6":
                row["active"] = False
            if row["invoice_id"] == "I8":
                if row.get("amount_dec") is not None:
                    row["amount_dec"] = Decimal("99999999999999.9999")
                if row.get("debit_dec", 0) > 0:
                    row["debit_dec"] = Decimal("99999999999999.9999")
                if row.get("credit_dec", 0) > 0:
                    row["credit_dec"] = Decimal("99999999999999.9999")
        rows.extend(
            [
                {**next(row for row in rows if row["key"] == "D8"), "key": "D8-extra"},
                {**next(row for row in rows if row["key"] == "C8"), "key": "C8-extra"},
            ]
        )
        rows.append(
            {**next(row for row in rows if row["key"] == "B7"), "key": "B7-duplicate"}
        )
        schema = ",".join(f"{name} {kind}" for name, kind in SCHEMA.items())
        frame = (
            self.spark.createDataFrame(rows, schema)
            .withColumn("bucket", bucket("lookup_key"))
            .withColumn("content_hash", F.sha2(F.to_json(F.struct(*SCHEMA)), 256))
        )
        cache = gold_path(self.root, "test", "state_reconciliation_inputs")
        frame.write.format("delta").partitionBy("kind", "bucket").save(str(cache))
        ids = self.spark.createDataFrame(
            [(f"I{i}",) for i in range(9)], "lookup_key STRING"
        )
        actual = invoice_results(self.spark, cache, ids)
        self.assertEqual(actual.schema["ledger_debit_dec"].dataType, DecimalType(18, 4))
        result = {row.invoice_id: row for row in actual.collect()}
        self.assertEqual(result["I0"].reasons, [])
        self.assertEqual(result["I0"].ledger_debit_dec, Decimal("0.0001"))
        for identity, reason in [
            ("I1", "AMOUNT_MISMATCH"),
            ("I2", "CURRENCY_MISMATCH"),
            ("I3", "MERCHANT_MISMATCH"),
            ("I4", "UNBALANCED_LEDGER"),
            ("I5", "MISSING_BANK"),
            ("I6", "MISSING_OR_INACTIVE_ACCOUNT"),
            ("I7", "AMBIGUOUS_BANK"),
            ("I8", "LEDGER_AMOUNT_OVERFLOW"),
        ]:
            self.assertIn(reason, result[identity].reasons)
        orphan = bank_results(
            self.spark,
            cache,
            self.spark.createDataFrame([("unreferenced:B5",)], "lookup_key STRING"),
        ).first()
        self.assertEqual(orphan.reasons, ["MISSING_GATEWAY_REFERENCE"])
        self.assertIn("inv#I5", orphan.remittance_memo)
        # Static hash-bucket and kind filters survive in the physical scan plan.
        from pipelines.gold_inputs import lookup

        lookup_frame = lookup(self.spark, cache, "invoice", ids.limit(1))
        lookup_frame.collect()
        plan = lookup_frame._jdf.queryExecution().executedPlan().toString()
        self.assertIn("PartitionFilters", plan)
        self.assertIn("bucket", plan)

    def fixture(self):
        definitions = {
            "gateway": (
                [("G1", "I1", "R1", "M1", "USD", Decimal("0.0001"), "g1")],
                "entity_key STRING, invoice_id STRING, gateway_ref STRING, merchant_id STRING, currency STRING, amount_dec DECIMAL(18,4), record_id STRING",
            ),
            "bank": (
                [],
                "entity_key STRING, gateway_ref STRING, merchant_id STRING, currency STRING, settled_amount_dec DECIMAL(18,4), remittance_memo STRING, record_id STRING",
            ),
            "invoice": (
                [
                    (
                        "I1",
                        "I1",
                        "A1",
                        Decimal("0.0001"),
                        "USD",
                        "PAID",
                        True,
                        False,
                        "i1",
                    )
                ],
                "entity_key STRING, invoice_id STRING, account_id STRING, total_amount_dec DECIMAL(18,4), currency STRING, status STRING, is_current BOOLEAN, is_deleted BOOLEAN, record_id STRING",
            ),
            "account": (
                [("A1", "A1", "M1", True, False, True, "a1")],
                "entity_key STRING, account_id STRING, merchant_id STRING, is_current BOOLEAN, is_deleted BOOLEAN, is_active BOOLEAN, record_id STRING",
            ),
            "journal": (
                [
                    ("D1", "I1", "A1", Decimal("0.0001"), Decimal("0"), False, "d1"),
                    ("C1", "I1", "A1", Decimal("0"), Decimal("0.0001"), False, "c1"),
                ],
                "entity_key STRING, invoice_id STRING, account_id STRING, debit_dec DECIMAL(18,4), credit_dec DECIMAL(18,4), is_deleted BOOLEAN, record_id STRING",
            ),
        }
        for kind, (rows, schema) in definitions.items():
            self.spark.createDataFrame(rows, schema).write.format("delta").save(
                str(table_path(self.root, "test", TABLES[kind]))
            )
        return definitions

    def test_delayed_bank_crash_retry_and_account_invalidation(self):
        from delta.tables import DeltaTable
        from pipelines.verify_cdc import digest

        definitions = self.fixture()
        run(self.spark, self.root, "test")
        opened = self.read("state_unreconciled_open_items").first()
        self.assertEqual(opened.item_id, "INVOICE:I1")
        self.assertIn("MISSING_BANK", opened.reasons)
        initial_first_seen = self.read("gold_reconciliation").first().first_seen_at
        self.assertEqual(
            run(self.spark, self.root, "test"),
            {"source_changed": False, "evaluated_items": 0},
        )
        self.assertEqual(
            self.read("state_unreconciled_open_items").first().first_seen_at,
            opened.first_seen_at,
        )
        bank = self.spark.createDataFrame(
            [("B1", "R1", "M1", "USD", Decimal("0.0001"), "late bank", "b1")],
            definitions["bank"][1],
        )
        bank.write.format("delta").mode("append").save(
            str(table_path(self.root, "test", TABLES["bank"]))
        )
        with patch(
            "pipelines.gold.commit_cursors",
            side_effect=RuntimeError("after results before cursor"),
        ):
            with self.assertRaisesRegex(RuntimeError, "before cursor"):
                run(self.spark, self.root, "test")
        reconciled = self.read("gold_reconciliation")
        self.assertEqual(reconciled.first().status, "MATCHED")
        before = digest(reconciled)
        self.assertEqual(self.read("state_unreconciled_open_items").count(), 0)
        run(self.spark, self.root, "test")
        self.assertEqual(digest(self.read("gold_reconciliation")), before)
        self.assertEqual(
            self.read("gold_reconciliation").first().first_seen_at, initial_first_seen
        )
        # A current account change invalidates a previously closed invoice through
        # the reverse index; it must not depend on the invoice already being open.
        account_path = str(table_path(self.root, "test", TABLES["account"]))
        DeltaTable.forPath(self.spark, account_path).update(set={"is_active": "false"})
        run(self.spark, self.root, "test")
        self.assertIn(
            "MISSING_OR_INACTIVE_ACCOUNT",
            self.read("state_unreconciled_open_items").first().reasons,
        )
        DeltaTable.forPath(self.spark, account_path).update(set={"is_active": "true"})
        journal_path = str(table_path(self.root, "test", TABLES["journal"]))
        DeltaTable.forPath(self.spark, journal_path).update(
            "entity_key = 'C1'", set={"is_deleted": "true"}
        )
        run(self.spark, self.root, "test")
        self.assertIn(
            "MISSING_JOURNAL_LEGS",
            self.read("state_unreconciled_open_items").first().reasons,
        )
        duplicate = self.spark.createDataFrame(
            [("G2", "I2", "R1", "M1", "USD", Decimal("0.0001"), "g2")],
            definitions["gateway"][1],
        )
        duplicate.write.format("delta").mode("append").save(
            str(table_path(self.root, "test", TABLES["gateway"]))
        )
        run(self.spark, self.root, "test")
        self.assertIn(
            "AMBIGUOUS_GATEWAY_REFERENCE",
            self.read("gold_reconciliation").where("invoice_id = 'I1'").first().reasons,
        )
        self.spark.sql(
            f"ALTER TABLE delta.`{account_path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = false)"
        )
        with self.assertRaisesRegex(ValueError, "CDF disabled"):
            run(self.spark, self.root, "test")

    def test_duplicate_source_key_fails_closed(self):
        from pipelines.gold_inputs import read_changes

        definitions = self.fixture()
        changes, current, sources = read_changes(self.spark, self.root, "test")
        self.assertEqual(len(sources), 6)
        self.assertGreater(changes.count(), 0)
        self.assertEqual(changes.count(), current.count())
        duplicate = self.spark.createDataFrame(
            [
                (
                    "G1",
                    "OTHER-INVOICE",
                    "OTHER-REF",
                    "M1",
                    "USD",
                    Decimal("1.0000"),
                    "other-record",
                )
            ],
            definitions["gateway"][1],
        )
        duplicate.write.format("delta").mode("append").save(
            str(table_path(self.root, "test", TABLES["gateway"]))
        )
        with self.assertRaisesRegex(ValueError, "Ambiguous current source key"):
            read_changes(self.spark, self.root, "test")

    def test_overlay_rechecks_bank_hash_and_unique_reference(self):
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        from pipelines.exception_links import linked_banks

        schema = ",".join(f"{name} {kind}" for name, kind in SCHEMA.items())
        common = dict(
            active=True,
            amount_dec=Decimal("0.0001"),
            currency="USD",
            merchant_id="M1",
            source_commit=1,
        )
        bank = dict(
            common,
            kind="bank",
            key="B1",
            lookup_key="unreferenced:B1",
            gateway_ref="",
            record_id="b1",
        )
        gateway = dict(
            common,
            kind="gateway_ref",
            key="G1",
            lookup_key="R1",
            gateway_ref="R1",
            invoice_id="I1",
            record_id="g1",
        )

        def frame(rows):
            return (
                self.spark.createDataFrame(rows, schema)
                .withColumn("bucket", bucket("lookup_key"))
                .withColumn("content_hash", F.sha2(F.to_json(F.struct(*SCHEMA)), 256))
            )

        bank_hash = frame([bank]).first().content_hash
        link = dict(
            common,
            kind="bank_link",
            key="B1",
            lookup_key="R1",
            gateway_ref="R1",
            invoice_id="I1",
            account_id="G1",
            record_id="b1",
            source_status=bank_hash,
        )
        cache = gold_path(self.root, "test", "state_reconciliation_inputs")
        frame([bank, gateway, link]).write.format("delta").partitionBy(
            "kind", "bucket"
        ).save(str(cache))
        references = self.spark.createDataFrame([("R1",)], "lookup_key STRING")
        self.assertEqual(linked_banks(self.spark, cache, references).count(), 1)
        DeltaTable.forPath(self.spark, str(cache)).update(
            "kind = 'bank'", set={"content_hash": "'changed-bank-evidence'"}
        )
        self.assertEqual(linked_banks(self.spark, cache, references).count(), 0)
        DeltaTable.forPath(self.spark, str(cache)).update(
            "kind = 'bank'", set={"content_hash": f"'{bank_hash}'"}
        )
        frame([{**gateway, "key": "G2", "invoice_id": "I2"}]).write.format(
            "delta"
        ).mode("append").save(str(cache))
        self.assertEqual(linked_banks(self.spark, cache, references).count(), 0)


if __name__ == "__main__":
    unittest.main()
