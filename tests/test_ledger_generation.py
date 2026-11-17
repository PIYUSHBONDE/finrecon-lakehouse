"""Financial source correlation and durable ledger handoff proofs."""

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

from data_generator.chaos_producer import (
    Outbox,
    ScenarioGenerator,
    VirtualClock,
    parse_args,
    run,
)
from data_generator.ledger import posting_details

START = datetime(2026, 11, 10, tzinfo=timezone.utc)


class LedgerGenerationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.args = parse_args(
            [
                "--dry-run",
                "--count",
                "100",
                "--seed",
                "42",
                "--output-dir",
                str(self.root),
            ]
        )

    def outbox(self):
        box = Outbox(self.args.output_dir / "outbox.sqlite3")
        self.addCleanup(box.close)
        return box

    def block(self):
        return ScenarioGenerator(42).block(100, START, timedelta(seconds=1), "test")

    def test_canonical_ledger_matches_bank_even_when_gateway_is_poisoned(self):
        messages, bank, transactions = self.block()
        self.assertEqual(len(transactions), 100)
        originals = {
            json.loads(row[3])["invoice_id"]: json.loads(row[3])
            for row in messages
            if not row[4]
        }
        for settlement, transaction in zip(bank, transactions, strict=True):
            original = originals[transaction["invoice_id"]]
            self.assertEqual(
                transaction["amount_dec"], settlement["settled_amount_dec"]
            )
            self.assertEqual(transaction["currency"], settlement["currency"])
            self.assertEqual(transaction["merchant_id"], settlement["merchant_id"])
            self.assertEqual(transaction["event_ts"], original["event_ts"])
            self.assertGreater(Decimal(transaction["amount_dec"]), 0)
            self.assertIn(transaction["invoice_id"], settlement["remittance_memo"])
        self.assertEqual(
            sum(
                row["amount_dec"] != originals[row["invoice_id"]]["amount_dec"]
                or row["currency"] != originals[row["invoice_id"]]["currency"]
                for row in transactions
            ),
            1,
        )

    def test_posting_identities_are_stable_and_currency_specific(self):
        transaction = self.block()[2][0]
        details = posting_details(transaction)
        self.assertEqual(details, posting_details(dict(transaction)))
        amount, event_time, payment, clearing, debit, credit = details
        self.assertIsInstance(amount, Decimal)
        self.assertIsNotNone(event_time.tzinfo)
        self.assertNotEqual(payment, clearing)
        self.assertNotEqual(debit, credit)
        other_currency = dict(
            transaction, currency="EUR" if transaction["currency"] != "EUR" else "USD"
        )
        self.assertNotEqual(payment, posting_details(other_currency)[2])
        maximum = dict(transaction, amount_dec="99999999999999.9999")
        self.assertEqual(posting_details(maximum)[0], Decimal("99999999999999.9999"))

    def test_invalid_financial_values_never_reach_postgres(self):
        transaction = self.block()[2][0]
        for amount in (
            "0.0000",
            "-1.0000",
            "1.00001",
            "100000000000000.0000",
            "NaN",
            "Infinity",
            1.25,
        ):
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                posting_details(dict(transaction, amount_dec=amount))
        with self.assertRaises(ValueError):
            posting_details(dict(transaction, currency="INVALID"))
        with self.assertRaises(ValueError):
            posting_details(dict(transaction, event_ts="2026-11-10T00:00:00"))

    def test_outbox_enqueues_kafka_and_ledger_atomically(self):
        messages, _, transactions = self.block()
        box = self.outbox()
        box.add(messages, transactions)
        self.assertEqual(box.count(), 105)
        self.assertEqual(box.ledger_count(), 100)
        with self.assertRaises(sqlite3.IntegrityError):
            box.add(messages, transactions)
        self.assertEqual(box.count(), 105)
        self.assertEqual(box.ledger_count(), 100)

    def test_ledger_failure_blocks_kafka_and_survives_restart(self):
        box = Outbox(self.args.output_dir / "outbox.sqlite3")
        ledger, sink = Mock(), Mock()
        ledger.write.side_effect = RuntimeError("PostgreSQL unavailable")
        try:
            with self.assertRaisesRegex(RuntimeError, "PostgreSQL unavailable"):
                run(self.args, sink, box, VirtualClock(START), ledger=ledger)
            sink.send.assert_not_called()
            self.assertEqual(box.count(), 105)
            self.assertEqual(box.ledger_count(), 100)
            canonical = box.ledger_batch()
        finally:
            box.close()
        reopened = self.outbox()
        ledger = Mock()
        self.args.count = 0
        result = run(self.args, sink, reopened, VirtualClock(START), ledger=ledger)
        ledger.write.assert_called_once_with(canonical)
        self.assertEqual(sink.send.call_count, 105)
        self.assertEqual(result["pending_ledger"], 0)
        self.assertEqual(result["pending"], 0)

    def test_pending_ledger_cannot_be_skipped_on_resume(self):
        box = self.outbox()
        messages, _, transactions = self.block()
        box.add(messages, transactions)
        sink = Mock()
        with self.assertRaisesRegex(RuntimeError, "requires a PostgreSQL ledger store"):
            run(self.args, sink, box, VirtualClock(START))
        sink.send.assert_not_called()
        self.assertEqual(box.ledger_count(), 100)

    def test_gateway_retries_do_not_post_the_ledger_again(self):
        box = self.outbox()
        ledger, sink = Mock(), Mock()
        run(self.args, sink, box, VirtualClock(START), ledger=ledger)
        self.assertEqual(ledger.write.call_count, 1)
        self.assertEqual(len(ledger.write.call_args.args[0]), 100)
        self.assertEqual(sink.send.call_count, 105)


if __name__ == "__main__":
    unittest.main()
