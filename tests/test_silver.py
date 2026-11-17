"""Silver financial contracts, late retries, conflicts, and replay crash proofs."""

import importlib.util
import json
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from pipelines.contracts import load_contract, prepare
from pipelines.replay import replay
from pipelines.silver import process_batch, table_path

DAY = date(2026, 11, 11)


def payment(key="K1", **changes):
    row = {
        "event_id": f"E-{key}",
        "idempotency_key": key,
        "gateway_ref": f"GW-{key}",
        "invoice_id": f"INV-{key}",
        "merchant_id": "MERCHANT_TEST",
        "amount_dec": "1.0000",
        "currency": "USD",
        "event_ts": "2026-11-11T08:00:00+00:00",
    }
    return {**row, **changes}


@unittest.skipUnless(
    importlib.util.find_spec("yaml"), "YAML parser is installed in the Spark image"
)
class ContractFileTests(unittest.TestCase):
    def test_repository_contracts_have_stable_fingerprints(self):
        for source in ("gateway", "bank"):
            rules, fingerprint = load_contract(source)
            self.assertTrue(rules)
            self.assertEqual(len(fingerprint), 64)
            self.assertEqual(load_contract(source)[1], fingerprint)

    def test_duplicate_rules_and_wrong_versions_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for document in (
                "version: 2\nsource: gateway\nrules: []\n",
                "version: 1\nsource: gateway\nrules:\n- {name: X, expression: 'true'}\n- {name: X, expression: 'false'}\n",
            ):
                (root / "gateway.yaml").write_text(document, encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_contract("gateway", root)


@unittest.skipUnless(
    importlib.util.find_spec("pyspark")
    and importlib.util.find_spec("delta")
    and importlib.util.find_spec("yaml"),
    "Run financial Spark tests in the local compute container",
)
class SilverSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pipelines.runtime import spark_session

        cls.spark = spark_session("FinRecon Silver tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def frame(self, rows, start=0):
        return self.spark.createDataFrame(
            [
                (
                    json.dumps(row) if isinstance(row, dict) else row,
                    "test_gateway",
                    0,
                    start + index,
                )
                for index, row in enumerate(rows)
            ],
            "raw_payload STRING, topic STRING, partition INT, offset LONG",
        )

    def read(self, name):
        return self.spark.read.format("delta").load(
            str(table_path(self.root, "test", name))
        )

    def process(self, rows, start=0):
        return process_batch(
            self.spark, self.frame(rows, start), "gateway", self.root, "test", DAY
        )

    def test_contract_matrix_never_rounds_invalid_amounts(self):
        from pyspark.sql.types import DecimalType

        cases = [
            payment("MIN", amount_dec="0.0001"),
            payment("MAX", amount_dec="99999999999999.9999"),
        ]
        cases += [
            payment(f"BAD{index}", amount_dec=amount)
            for index, amount in enumerate(
                ("-1.0000", "0.0000", "1.00001", "100000000000000.0000", "1e3"),
            )
        ]
        cases += [
            payment("CUR", currency="INVALID"),
            payment("MER", merchant_id=" "),
            payment("TS", event_ts="bad"),
            payment("OLD", event_ts="2026-11-04T08:00:00+00:00"),
            "{broken",
        ]
        frame = prepare(self.frame(cases), "gateway", DAY)
        self.assertEqual(frame.schema["amount_dec"].dataType, DecimalType(18, 4))
        rows = frame.select("entity_key", "amount_dec", "failed_rules").collect()
        valid = {row.entity_key: row.amount_dec for row in rows if not row.failed_rules}
        self.assertEqual(
            valid, {"MIN": Decimal("0.0001"), "MAX": Decimal("99999999999999.9999")}
        )
        self.assertEqual(sum(bool(row.failed_rules) for row in rows), len(cases) - 2)

    def test_duplicates_late_arrivals_conflicts_and_expired_window(self):
        row = payment()
        self.process([row, dict(row)])
        self.process([row, dict(row)])
        self.assertEqual(self.read("silver_gateway_events").count(), 1)
        self.process([row, payment("LATE", event_ts="2026-11-11T06:00:00+00:00")], 10)
        self.assertEqual(self.read("silver_gateway_events").count(), 2)
        self.process([payment(amount_dec="2.0000")], 20)
        self.process([payment("OLD", event_ts="2026-11-03T08:00:00+00:00")], 30)
        self.process(
            [payment("AMB", amount_dec="3.0000"), payment("AMB", amount_dec="4.0000")],
            40,
        )
        self.assertEqual(self.read("silver_gateway_events").count(), 2)
        stored = self.read("silver_gateway_events").where("entity_key = 'K1'").first()
        self.assertEqual(stored.amount_dec, Decimal("1.0000"))
        quarantine = self.read("silver_quarantine").collect()
        self.assertEqual(len(quarantine), 4)
        self.assertEqual(
            sum("FINANCIAL_CONFLICT" in row.failed_rules for row in quarantine), 3
        )
        self.assertTrue(
            any(
                "OUTSIDE_RECONCILIATION_WINDOW" in row.failed_rules
                for row in quarantine
            )
        )

    def test_replay_revalidates_rejects_stale_and_recovers_lost_ack(self):
        self.process(
            [payment("FIX", currency="INVALID"), payment("STALE", currency="INVALID")]
        )
        bank = {
            "settlement_id": "B1",
            "gateway_ref": "",
            "merchant_id": "MERCHANT_TEST",
            "settled_amount_dec": "0.0000",
            "currency": "USD",
            "remittance_memo": "INV-FIX LESS FEES",
            "batch_date": "2026-11-11",
        }
        bank_frame = self.spark.createDataFrame(
            [(json.dumps(bank), "test_bank.csv")],
            "raw_payload STRING, source_file STRING",
        )
        process_batch(self.spark, bank_frame, "bank", self.root, "test", DAY)
        self.process([payment("STALE", event_ts="2026-11-11T10:00:00+00:00")], 10)
        corrections = []
        for row in self.read("silver_quarantine").collect():
            corrected = (
                payment(row.entity_key)
                if row.source_table == "bronze_gateway_events"
                else {**bank, "settled_amount_dec": "1.0000"}
            )
            corrections.append(
                {"record_id": row.record_id, "corrected_payload": json.dumps(corrected)}
            )
        patch_file = self.root / "corrections.jsonl"
        patch_file.write_text(
            "\n".join(json.dumps(row) for row in corrections), encoding="utf-8"
        )
        fix = next(
            row
            for row in corrections
            if json.loads(row["corrected_payload"]).get("idempotency_key") == "FIX"
        )
        changed = {
            **json.loads(fix["corrected_payload"]),
            "idempotency_key": "OTHER",
            "event_ts": "2026-11-11T12:00:00+00:00",
        }
        changed_file = self.root / "changed_identity.jsonl"
        changed_file.write_text(
            json.dumps(
                {
                    "record_id": fix["record_id"],
                    "corrected_payload": json.dumps(changed),
                }
            ),
            encoding="utf-8",
        )
        self.assertEqual(
            replay(self.spark, self.root, "test", DAY, changed_file),
            {"replayed": 0, "still_pending": 1},
        )
        self.assertEqual(self.read("silver_gateway_events").count(), 1)
        self.assertIn(
            "REPLAY_IDENTITY_CHANGED",
            self.read("silver_quarantine")
            .where("entity_key = 'FIX'")
            .first()
            .last_replay_error,
        )
        with patch(
            "pipelines.replay.acknowledge_replay",
            side_effect=RuntimeError("lost acknowledgement"),
        ):
            with self.assertRaisesRegex(RuntimeError, "lost acknowledgement"):
                replay(self.spark, self.root, "test", DAY, patch_file)
        self.assertEqual(self.read("silver_gateway_events").count(), 2)
        result = replay(self.spark, self.root, "test", DAY, patch_file)
        self.assertEqual(result, {"replayed": 2, "still_pending": 1})
        self.assertEqual(self.read("silver_gateway_events").count(), 2)
        self.assertEqual(self.read("silver_bank_settlements").count(), 1)
        quarantine = self.read("silver_quarantine").collect()
        self.assertEqual(sum(row.status == "REPLAYED" for row in quarantine), 2)
        stale = next(row for row in quarantine if row.entity_key == "STALE")
        self.assertEqual(stale.status, "PENDING")
        self.assertIn("STALE_SOURCE", stale.last_replay_error)
        self.assertEqual(
            replay(self.spark, self.root, "test", DAY, patch_file),
            {"replayed": 0, "still_pending": 1},
        )


if __name__ == "__main__":
    unittest.main()
