"""Opt-in PostgreSQL proofs; set FINRECON_TEST_POSTGRES_DSN to run locally."""

import os
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

from data_generator.chaos_producer import ScenarioGenerator
from data_generator.ledger import PostgresLedger, posting_details


@unittest.skipUnless(
    os.environ.get("FINRECON_TEST_POSTGRES_DSN"),
    "PostgreSQL integration test is opt-in",
)
class PostgresLedgerTests(unittest.TestCase):
    def setUp(self):
        self.store = PostgresLedger(os.environ["FINRECON_TEST_POSTGRES_DSN"])
        self.addCleanup(self.store.close)
        self.merchant = f"TEST_{uuid4().hex[:12]}"
        _, _, self.transactions = ScenarioGenerator(42).block(
            3,
            datetime.now(timezone.utc),
            timedelta(seconds=1),
            "test",
        )
        for row in self.transactions:
            row["merchant_id"] = self.merchant
        self.addCleanup(self.cleanup_rows)

    def cleanup_rows(self):
        invoices = [row["invoice_id"] for row in self.transactions]
        with self.store.connection.transaction():
            self.store.connection.execute(
                "DELETE FROM public.ledger_entries WHERE invoice_id = ANY(%s)",
                (invoices,),
            )
            self.store.connection.execute(
                "DELETE FROM public.invoices WHERE invoice_id = ANY(%s)", (invoices,)
            )
            self.store.connection.execute(
                "DELETE FROM public.accounts WHERE merchant_id = %s", (self.merchant,)
            )

    def snapshot(self):
        invoices = [row["invoice_id"] for row in self.transactions]
        return self.store.connection.execute(
            "SELECT entry_id, invoice_id, account_id, debit, credit, entry_ts "
            "FROM public.ledger_entries WHERE invoice_id = ANY(%s) ORDER BY entry_id",
            (invoices,),
        ).fetchall()

    def test_balanced_postings_and_replay_preserve_rows_and_timestamps(self):
        self.store.write(self.transactions)
        before = self.snapshot()
        self.assertEqual(len(before), 6)
        for transaction in self.transactions:
            rows = [row for row in before if row[1] == transaction["invoice_id"]]
            amount = Decimal(transaction["amount_dec"])
            self.assertEqual(sum(row[3] for row in rows), amount)
            self.assertEqual(sum(row[4] for row in rows), amount)
            self.assertNotEqual(rows[0][2], rows[1][2])
        self.store.write(self.transactions)
        self.assertEqual(self.snapshot(), before)
        statuses = self.store.connection.execute(
            "SELECT status FROM public.invoices WHERE invoice_id = ANY(%s)",
            ([row["invoice_id"] for row in self.transactions],),
        ).fetchall()
        self.assertEqual(statuses, [("PAID",)] * 3)

    def test_conflicting_invoice_rolls_back_entire_batch(self):
        self.store.write(self.transactions[:1])
        before = self.snapshot()
        conflict = dict(self.transactions[0], amount_dec="0.0001")
        with self.assertRaisesRegex(ValueError, "Conflicting financial values"):
            self.store.write([self.transactions[1], conflict])
        self.assertEqual(self.snapshot(), before)
        self.assertIsNone(
            self.store.connection.execute(
                "SELECT invoice_id FROM public.invoices WHERE invoice_id = %s",
                (self.transactions[1]["invoice_id"],),
            ).fetchone()
        )

    def test_credit_insert_failure_leaves_no_partial_invoice_or_debit(self):
        transaction = self.transactions[0]
        credit_id = posting_details(transaction)[5]
        # Occupy this deterministic credit ID under another invoice to force an
        # actual database failure after the new invoice and debit were inserted.
        self.store.write(self.transactions[1:2])
        other = self.transactions[1]
        self.store.connection.execute(
            "INSERT INTO public.ledger_entries (entry_id, invoice_id, account_id, debit, credit) "
            "SELECT %s, invoice_id, account_id, 0.0000, 0.0000 FROM public.invoices WHERE invoice_id = %s",
            (credit_id, other["invoice_id"]),
        )
        import psycopg

        with self.assertRaises(psycopg.errors.UniqueViolation):
            self.store.write([transaction])
        self.assertIsNone(
            self.store.connection.execute(
                "SELECT invoice_id FROM public.invoices WHERE invoice_id = %s",
                (transaction["invoice_id"],),
            ).fetchone()
        )
        self.assertEqual(
            self.store.connection.execute(
                "SELECT COUNT(*) FROM public.ledger_entries WHERE invoice_id = %s",
                (transaction["invoice_id"],),
            ).fetchone()[0],
            0,
        )


if __name__ == "__main__":
    unittest.main()
