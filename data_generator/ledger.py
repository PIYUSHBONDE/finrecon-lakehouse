"""Atomic, idempotent postings for the existing PostgreSQL financial schema."""

import os
from datetime import datetime
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

DEFAULT_DSN = (
    "host=localhost port=15432 dbname=finrecon user=finrecon password=finrecon"
)
VALID_CURRENCIES = frozenset(("USD", "EUR", "GBP", "JPY", "INR"))


def posting_details(transaction):
    """Validate clean source values and derive stable account/journal identities."""
    if not isinstance(transaction["amount_dec"], str):
        raise ValueError("Ledger amount must be a decimal string")
    amount = Decimal(transaction["amount_dec"])
    if (
        not amount.is_finite()
        or not Decimal("0") < amount < Decimal("100000000000000")
        or amount.as_tuple().exponent < -4
    ):
        raise ValueError("Ledger amount must be positive and fit DECIMAL(18,4)")
    currency = transaction["currency"]
    if currency not in VALID_CURRENCIES:
        raise ValueError("Ledger currency is invalid")
    event_time = datetime.fromisoformat(transaction["event_ts"])
    if event_time.tzinfo is None:
        raise ValueError("Ledger event time must include a timezone")
    suffix = f"{transaction['merchant_id']}-{currency}"
    payment_account, clearing_account = f"ACC-{suffix}", f"CLR-{suffix}"
    if max(len(payment_account), len(clearing_account)) > 50:
        raise ValueError("Ledger account ID exceeds schema length")
    invoice = transaction["invoice_id"]
    debit_id = uuid5(NAMESPACE_URL, f"finrecon/{invoice}/debit")
    credit_id = uuid5(NAMESPACE_URL, f"finrecon/{invoice}/credit")
    return amount, event_time, payment_account, clearing_account, debit_id, credit_id


class PostgresLedger:
    def __init__(self, dsn=None):
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError(
                "Install PostgreSQL client: python -m pip install -r data_generator/requirements.txt"
            ) from exc
        self.connection = psycopg.connect(
            dsn
            if dsn is not None
            else os.environ.get("FINRECON_POSTGRES_DSN", DEFAULT_DSN),
            autocommit=True,
            connect_timeout=5,
            application_name="finrecon-chaos-producer",
        )

    def write(self, transactions):
        # The entire bounded batch commits together; exceptions roll back all legs.
        with self.connection.transaction(), self.connection.cursor() as cursor:
            for transaction in transactions:
                self._post(cursor, transaction)

    def _post(self, cursor, transaction):
        amount, event_time, account, clearing, debit_id, credit_id = posting_details(
            transaction
        )
        invoice = transaction["invoice_id"]
        merchant = transaction["merchant_id"]
        currency = transaction["currency"]
        tier = "ENTERPRISE" if merchant == "MERCHANT_ENT_01" else "STANDARD"
        cursor.executemany(
            """INSERT INTO public.accounts
               (account_id, merchant_id, account_name, tier, created_at, updated_at)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (account_id) DO NOTHING""",
            [
                (
                    account,
                    merchant,
                    f"Payment account {currency}",
                    tier,
                    event_time,
                    event_time,
                ),
                (
                    clearing,
                    merchant,
                    f"Settlement clearing {currency}",
                    tier,
                    event_time,
                    event_time,
                ),
            ],
        )
        cursor.execute(
            """INSERT INTO public.invoices
               (invoice_id, account_id, total_amount, currency, status, created_at)
               VALUES (%s, %s, %s, %s, 'ISSUED', %s)
               ON CONFLICT (invoice_id) DO NOTHING RETURNING invoice_id""",
            (invoice, account, amount, currency, event_time),
        )
        inserted = cursor.fetchone() is not None
        # Lock the invoice so concurrent replays serialize on its financial identity.
        cursor.execute(
            """SELECT account_id, total_amount, currency FROM public.invoices
               WHERE invoice_id = %s FOR UPDATE""",
            (invoice,),
        )
        if cursor.fetchone() != (account, amount, currency):
            raise ValueError(f"Conflicting financial values for invoice {invoice}")
        expected = {
            debit_id: (account, amount, Decimal("0.0000")),
            credit_id: (clearing, Decimal("0.0000"), amount),
        }
        if inserted:
            cursor.executemany(
                """INSERT INTO public.ledger_entries
                   (entry_id, invoice_id, account_id, debit, credit, entry_ts)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                [
                    (identity, invoice, *values, event_time)
                    for identity, values in expected.items()
                ],
            )
            cursor.execute(
                "UPDATE public.invoices SET status = 'PAID' WHERE invoice_id = %s",
                (invoice,),
            )
        cursor.execute(
            """SELECT entry_id, account_id, debit, credit FROM public.ledger_entries
               WHERE invoice_id = %s""",
            (invoice,),
        )
        actual = {row[0]: row[1:] for row in cursor.fetchall()}
        if actual != expected:
            raise ValueError(f"Incomplete or conflicting journal for invoice {invoice}")

    def close(self):
        self.connection.close()
