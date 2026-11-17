"""WAL ordering, atomic SCD history, deletes, and crash recovery proofs."""

import importlib.util
import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from pipelines.cdc import ENTITIES, normalize, process_batch
from pipelines.silver import table_path

BASE_TIME = 1794380000000


def event(table, key, lsn, op="u", ts=None, key_only=False, **changes):
    attributes = {
        "accounts": {
            "account_id": key,
            "merchant_id": "M1",
            "account_name": "Account",
            "tier": "STANDARD",
            "is_active": True,
        },
        "invoices": {
            "invoice_id": key,
            "account_id": "A1",
            "total_amount": "0.0001",
            "currency": "USD",
            "status": "ISSUED",
        },
        "ledger_entries": {
            "entry_id": key,
            "invoice_id": "I1",
            "account_id": "A1",
            "debit": "0.0001",
            "credit": "0.0000",
            "entry_ts": "2026-11-11T07:00:00Z",
        },
    }[table]
    attributes.update(changes)
    if key_only:
        attributes = {ENTITIES[table]["key"]: key}
    body = {
        "source": {
            "connector": "postgresql",
            "db": "finrecon",
            "schema": "public",
            "table": table,
            "lsn": lsn,
            "ts_ms": BASE_TIME if ts is None else ts,
        },
        "op": op,
        "before": attributes if op == "d" else None,
        "after": None if op == "d" else attributes,
        "transaction": None,
    }
    return table, key, json.dumps({"payload": body})


@unittest.skipUnless(
    importlib.util.find_spec("pyspark") and importlib.util.find_spec("delta"),
    "Run CDC tests in the Spark container",
)
class CdcTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pipelines.runtime import spark_session

        cls.spark = spark_session("FinRecon CDC tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.offset = 0

    def frame(self, events):
        rows = []
        for table, key, payload in events:
            wire_key = json.dumps({"payload": {ENTITIES[table]["key"]: key}})
            rows.append(
                (payload, f"finrecon_db.public.{table}", 0, self.offset, wire_key)
            )
            self.offset += 1
        return self.spark.createDataFrame(
            rows,
            "raw_payload STRING, topic STRING, partition INT, offset LONG, message_key STRING",
        )

    def read(self, table):
        return self.spark.read.format("delta").load(
            str(table_path(self.root, "test", table))
        )

    def apply(self, events):
        frame = self.frame(events)
        process_batch(self.spark, frame, self.root, "test")
        return frame

    def test_normalization_decimal_limits_and_tombstones(self):
        from pyspark.sql.types import DecimalType

        frame = normalize(
            self.frame(
                [
                    event("invoices", "MIN", 100, "r"),
                    event(
                        "invoices",
                        "MAX",
                        2**63 + 50,
                        total_amount="99999999999999.9999",
                    ),
                    event("invoices", "SCALE", 200, total_amount="1.00001"),
                    event("invoices", "OVER", 201, total_amount="100000000000000.0000"),
                    event("invoices", "CUR", 202, currency="BAD"),
                    event("accounts", "DEL", 300, "d", key_only=True),
                    ("accounts", "DEL", None),
                    ("accounts", "BROKEN", "{bad"),
                    event("accounts", "MISSING_PK", 1000, account_id=None),
                ]
            )
        )
        self.assertEqual(frame.schema["total_amount_dec"].dataType, DecimalType(18, 4))
        rows = frame.collect()
        self.assertEqual(sum(not row.failed_rules for row in rows), 4)
        self.assertEqual(
            next(row for row in rows if row.entity_key == "MAX").total_amount_dec,
            Decimal("99999999999999.9999"),
        )
        self.assertTrue(
            next(row for row in rows if row.raw_payload is None).is_tombstone
        )

    def test_every_change_same_millisecond_delete_reinsert_and_stale_guard(self):
        original = self.apply(
            [
                event("accounts", "A1", 100, "c"),
                event("accounts", "A1", 200, tier="ENTERPRISE"),
                event("accounts", "A1", 250, tier="ENTERPRISE"),
                event("accounts", "A1", 300),
            ]
        )
        table = "silver_dim_accounts_scd2"
        rows = self.read(table).orderBy("eff_start_version").collect()
        self.assertEqual(
            [row.tier for row in rows], ["STANDARD", "ENTERPRISE", "STANDARD"]
        )
        self.assertEqual(sum(row.is_current for row in rows), 1)
        self.assertEqual(rows[0].eff_end_version, rows[1].eff_start_version)
        self.assertEqual(rows[1].eff_end_version, rows[2].eff_start_version)
        self.assertTrue(all(row.eff_start_ts == rows[0].eff_start_ts for row in rows))
        self.apply(
            [event("accounts", "A1", 400, "d", key_only=True), ("accounts", "A1", None)]
        )
        self.assertTrue(self.read(table).where("is_current").first().is_deleted)
        self.apply([event("accounts", "A1", 450, "d")])
        self.assertEqual(self.read(table).count(), 4)
        self.apply(
            [event("accounts", "A1", 350, tier="ENTERPRISE", ts=BASE_TIME + 10000)]
        )
        self.assertTrue(self.read(table).where("is_current").first().is_deleted)
        self.apply([event("accounts", "A1", 500, "c", tier="ENTERPRISE")])
        self.apply(
            [event("accounts", "A1", 600, tier="ENTERPRISE", ts=BASE_TIME - 10000)]
        )
        self.apply([event("accounts", "A1", 550, ts=BASE_TIME + 20000)])
        current = self.read(table).where("is_current").first()
        self.assertEqual(current.tier, "ENTERPRISE")
        self.assertFalse(current.is_deleted)
        self.assertTrue(current.clock_regressed)
        self.assertEqual(current.last_source_ts_ms, BASE_TIME - 10000)
        self.assertEqual(self.read(table).count(), 5)
        process_batch(self.spark, original, self.root, "test")
        self.assertEqual(self.read(table).count(), 5)
        self.assertEqual(self.read("silver_quarantine").count(), 2)

    def test_atomic_union_merge_and_recovery_before_audit_acknowledgement(self):
        from delta.tables import DeltaTable

        self.apply([event("accounts", "A1", 100, "c")])
        target = table_path(self.root, "test", "silver_dim_accounts_scd2")
        old_version = (
            DeltaTable.forPath(self.spark, str(target)).history(1).first().version
        )
        batch = self.frame(
            [
                event("accounts", "A1", 200, tier="ENTERPRISE"),
                event("accounts", "A1", 300),
                event("invoices", "I1", 400, "c"),
                event("invoices", "I1", 500, status="PAID"),
                event("ledger_entries", "J1", 600, "c"),
                event(
                    "ledger_entries", "J2", 601, "c", debit="0.0000", credit="0.0001"
                ),
            ]
        )
        with patch(
            "pipelines.scd2.commit_scd2",
            side_effect=RuntimeError("before atomic commit"),
        ):
            with self.assertRaisesRegex(RuntimeError, "before atomic commit"):
                process_batch(self.spark, batch, self.root, "test")
        self.assertEqual(
            DeltaTable.forPath(self.spark, str(target)).history(1).first().version,
            old_version,
        )
        self.assertEqual(self.read("silver_cdc_events").count(), 1)
        with patch(
            "pipelines.cdc.merge_audit",
            side_effect=RuntimeError("lost audit acknowledgement"),
        ):
            with self.assertRaisesRegex(RuntimeError, "lost audit acknowledgement"):
                process_batch(self.spark, batch, self.root, "test")
        commit = DeltaTable.forPath(self.spark, str(target)).history(1).first()
        self.assertEqual(commit.operation, "MERGE")
        self.assertEqual(commit.operationMetrics["numTargetRowsUpdated"], "1")
        self.assertEqual(commit.operationMetrics["numTargetRowsInserted"], "2")
        self.assertEqual(self.read("silver_dim_accounts_scd2").count(), 3)
        self.assertEqual(self.read("silver_dim_invoices_scd2").count(), 2)
        self.assertEqual(self.read("silver_cdc_events").count(), 1)
        process_batch(self.spark, batch, self.root, "test")
        self.assertEqual(self.read("silver_cdc_events").count(), 7)
        self.assertEqual(
            DeltaTable.forPath(self.spark, str(target)).history(1).first().version,
            commit.version,
        )
        journal = self.read("silver_ledger_entries").collect()
        self.assertEqual(sum(row.debit_dec for row in journal), Decimal("0.0001"))
        self.assertEqual(sum(row.credit_dec for row in journal), Decimal("0.0001"))
        process_batch(self.spark, batch, self.root, "test")
        self.assertEqual(self.read("silver_cdc_events").count(), 7)
        self.apply(
            [
                event("ledger_entries", "J1", 700, "d", key_only=True),
                ("ledger_entries", "J1", None),
            ]
        )
        self.apply([event("ledger_entries", "J1", 650, debit="0.0002")])
        self.assertTrue(
            self.read("silver_ledger_entries")
            .where("entry_id = 'J1'")
            .first()
            .is_deleted
        )


if __name__ == "__main__":
    unittest.main()
