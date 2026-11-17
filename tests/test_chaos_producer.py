"""Contract, replay, and delivery-failure proofs without a running Kafka broker."""

import csv
import json
import re
import sys
import tempfile
import unittest
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from data_generator.chaos_producer import (
    CURRENCIES,
    HOT_MERCHANT,
    JsonlSink,
    KafkaSink,
    Outbox,
    ScenarioGenerator,
    VirtualClock,
    parse_args,
    run,
)

START = datetime(2026, 11, 10, tzinfo=timezone.utc)


class MemorySink:
    def __init__(self):
        self.messages = []

    def send(self, topic, key, payload, emitted_at):
        self.messages.append((topic, key, payload, emitted_at))


class ChaosProducerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def args(self, count=1000):
        return parse_args(
            [
                "--dry-run",
                "--seed",
                "42",
                "--count",
                str(count),
                "--output-dir",
                str(self.root),
            ]
        )

    def outbox(self, args):
        box = Outbox(args.output_dir / "outbox.sqlite3")
        self.addCleanup(box.close)
        return box

    def test_complete_run_preserves_chaos_contracts_and_references(self):
        args = self.args(10000)
        sink = MemorySink()
        result = run(args, sink, self.outbox(args), VirtualClock(START))
        self.assertEqual(
            result,
            {
                "generated": 10000,
                "delivered": 10500,
                "retries": 500,
                "pending": 0,
                "pending_ledger": 0,
            },
        )
        by_key = defaultdict(list)
        arrivals = []
        for topic, key, payload, emitted_at in sink.messages:
            self.assertEqual(topic, "gateway_events")
            by_key[key].append((payload, emitted_at))
            arrivals.append(emitted_at)
        self.assertEqual(arrivals, sorted(arrivals))
        originals = [json.loads(rows[0][0]) for rows in by_key.values()]
        self.assertEqual(len(originals), 10000)
        self.assertEqual(
            sum(row["merchant_id"] == HOT_MERCHANT for row in originals), 6500
        )
        poison = [
            row
            for row in originals
            if (Decimal(row["amount_dec"]) < 0 or row["currency"] not in CURRENCIES)
        ]
        self.assertEqual(len(poison), 100)
        self.assertEqual(Counter(map(len, by_key.values())), {1: 9500, 2: 500})
        for rows in by_key.values():
            if len(rows) == 2:
                self.assertEqual(rows[0][0], rows[1][0])
                delay = rows[1][1] - rows[0][1]
                self.assertGreaterEqual(delay, timedelta(minutes=15))
                self.assertLessEqual(delay, timedelta(minutes=120))
        for row in originals:
            self.assertRegex(row["amount_dec"], r"^-?\d{1,14}\.\d{4}$")
            self.assertIsInstance(row["amount_dec"], str)
            self.assertEqual(
                datetime.fromisoformat(row["event_ts"]).utcoffset(), timedelta(0)
            )

        bank = []
        for path in args.landing_dir.glob("*.csv"):
            with path.open(newline="", encoding="utf-8") as stream:
                bank.extend(csv.DictReader(stream))
        self.assertEqual(len(bank), 10000)
        self.assertEqual(len({row["settlement_id"] for row in bank}), 10000)
        self.assertEqual(sum(not row["gateway_ref"] for row in bank), 200)
        by_reference = {row["gateway_ref"]: row for row in originals}
        by_invoice = {row["invoice_id"]: row for row in originals}
        for row in bank:
            invoice = re.search(r"INV-[0-9a-f-]{36}", row["remittance_memo"]).group()
            original = by_invoice[invoice]
            self.assertEqual(row["merchant_id"], original["merchant_id"])
            self.assertGreater(Decimal(row["settled_amount_dec"]), 0)
            self.assertIn(row["currency"], CURRENCIES)
            self.assertRegex(row["settled_amount_dec"], r"^\d{1,14}\.\d{4}$")
            if row["gateway_ref"]:
                self.assertEqual(by_reference[row["gateway_ref"]], original)
            else:
                self.assertIn("LESS FEES", row["remittance_memo"])
        self.assertFalse(list(args.landing_dir.glob("*.tmp")))

    def test_quotas_hold_across_seeds(self):
        for seed in range(10):
            messages, bank, _ = ScenarioGenerator(seed).block(
                100, START, timedelta(seconds=1), "test"
            )
            originals = [json.loads(row[3]) for row in messages if not row[4]]
            self.assertEqual(
                sum(row["merchant_id"] == HOT_MERCHANT for row in originals), 65
            )
            self.assertEqual(sum(row[4] for row in messages), 5)
            self.assertEqual(sum(not row["gateway_ref"] for row in bank), 2)
            self.assertEqual(
                sum(
                    Decimal(row["amount_dec"]) < 0 or row["currency"] not in CURRENCIES
                    for row in originals
                ),
                1,
            )

    def test_partial_block_and_zero_count(self):
        args = self.args(37)
        sink = MemorySink()
        box = self.outbox(args)
        result = run(args, sink, box, VirtualClock(START))
        self.assertEqual(result["generated"], 37)
        self.assertEqual(result["delivered"], 38)
        result = run(self.args(0), sink, box, VirtualClock(START))
        self.assertEqual(result["delivered"], 0)

    def test_failure_keeps_messages_and_restart_drains_without_new_rows(self):
        args = self.args(100)
        path = args.output_dir / "outbox.sqlite3"
        box = Outbox(path)
        sink = Mock()
        sink.send.side_effect = RuntimeError("broker unavailable")
        try:
            with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
                run(args, sink, box, VirtualClock(START))
            self.assertEqual(box.count(), 105)
        finally:
            box.close()
        reopened = self.outbox(args)
        sink = MemorySink()
        result = run(self.args(0), sink, reopened, VirtualClock(START))
        self.assertEqual(
            result,
            {
                "generated": 0,
                "delivered": 105,
                "retries": 5,
                "pending": 0,
                "pending_ledger": 0,
            },
        )
        self.assertEqual(len(list(args.landing_dir.glob("*.csv"))), 1)

    def test_retry_delay_starts_after_actual_original_delivery(self):
        args = self.args(0)
        box = self.outbox(args)
        messages, _, _ = ScenarioGenerator(1).block(
            100, START, timedelta(seconds=1), "test"
        )
        retry = next(row for row in messages if row[4])
        original = next(row for row in messages if not row[4] and row[2] == retry[2])
        box.add([original, retry])
        sink = MemorySink()
        run(args, sink, box, VirtualClock(START + timedelta(days=2)))
        self.assertEqual(len(sink.messages), 2)
        expected_delay = datetime.fromisoformat(retry[0]) - datetime.fromisoformat(
            original[0]
        )
        self.assertEqual(sink.messages[1][3] - sink.messages[0][3], expected_delay)

    def test_stop_keeps_pending_rows(self):
        args = self.args(100)
        box = self.outbox(args)
        sink = MemorySink()
        result = run(
            args, sink, box, VirtualClock(START), lambda: len(sink.messages) >= 1
        )
        self.assertEqual(result["delivered"], 1)
        self.assertEqual(result["pending"], 104)

    def test_jsonl_sink_records_arrival_separately_from_event_time(self):
        path = self.root / "events.jsonl"
        sink = JsonlSink(path)
        try:
            sink.send("test", "key", '{"amount_dec":"0.0001"}', START)
        finally:
            sink.close()
        result = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(result["payload"]["amount_dec"], "0.0001")
        self.assertEqual(datetime.fromisoformat(result["emitted_at"]), START)

    def test_kafka_acknowledgement_and_delivery_errors(self):
        producer = Mock()
        module = SimpleNamespace(Producer=Mock(return_value=producer))
        with patch.dict(sys.modules, {"confluent_kafka": module}):
            sink = KafkaSink("localhost:9092")
        self.assertTrue(module.Producer.call_args.args[0]["enable.idempotence"])

        def flush_success(timeout):
            producer.produce.call_args.kwargs["on_delivery"](None, Mock())
            return 0

        producer.flush.side_effect = flush_success
        sink.send("test", "key", "{}", START)
        self.assertEqual(producer.produce.call_args.kwargs["key"], b"key")

        def flush_failure(timeout):
            producer.produce.call_args.kwargs["on_delivery"]("delivery error", None)
            return 0

        producer.flush.side_effect = flush_failure
        with self.assertRaisesRegex(RuntimeError, "Kafka delivery failed"):
            sink.send("test", "key", "{}", START)
        producer.flush.side_effect = None
        producer.flush.return_value = 1
        with self.assertRaisesRegex(RuntimeError, "Kafka delivery failed"):
            sink.send("test", "key", "{}", START)

    def test_invalid_arguments(self):
        for arguments in (
            ["--count", "-1"],
            ["--rate", "0"],
            ["--rate", "1000001"],
            ["--topic", " "],
        ):
            with self.subTest(arguments=arguments), patch(
                "sys.stderr"
            ), self.assertRaises(SystemExit):
                parse_args(arguments)

    def test_live_and_dry_run_use_separate_outboxes(self):
        live = parse_args(["--output-dir", str(self.root)])
        dry = self.args()
        self.assertNotEqual(live.output_dir, dry.output_dir)


if __name__ == "__main__":
    unittest.main()
