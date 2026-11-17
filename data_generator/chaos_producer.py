"""Generate correlated gateway, PostgreSQL ledger, and bank CSV sources.

SQLite keeps scheduled messages across shutdowns. Live retries wait 15-120 real
minutes. Dry runs use a virtual clock and write arrival envelopes as JSONL.
Live runs commit clean ledger postings before publishing their gateway events.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import signal
import sqlite3
import time
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4, uuid5

LOG = logging.getLogger(__name__)
HOT_MERCHANT = "MERCHANT_ENT_01"
CURRENCIES = ("USD", "EUR", "GBP", "JPY", "INR")
BANK_FIELDS = (
    "settlement_id",
    "gateway_ref",
    "merchant_id",
    "settled_amount_dec",
    "currency",
    "remittance_memo",
    "batch_date",
)


def timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def encode(payload: dict) -> str:
    return json.dumps(payload, separators=(",", ":"), allow_nan=False)


class ScenarioGenerator:
    """Use shuffled quotas per 100 originals; never retain transaction history."""

    def __init__(self, seed: int | None = None):
        self.rng = random.Random(seed)
        self.run_id = uuid4()
        self.counter = 0

    def other_merchant(self) -> str:
        # Integer Zipf weights keep the remaining merchants unevenly distributed.
        weights = [10000 // rank for rank in range(1, 101)]
        ticket = self.rng.randrange(sum(weights))
        for rank, weight in enumerate(weights, 1):
            ticket -= weight
            if ticket < 0:
                return f"MERCHANT_{rank:05d}"
        raise AssertionError("Unreachable merchant selection")

    def block(self, size: int, start: datetime, interval: timedelta, topic: str):
        if not 1 <= size <= 100:
            raise ValueError("Block size must be between 1 and 100")
        flags = {
            name: set(self.rng.sample(range(size), size * percent // 100))
            for name, percent in (
                ("hot", 65),
                ("retry", 5),
                ("poison", 1),
                ("messy", 2),
            )
        }
        messages, settlements, ledger_transactions = [], [], []
        for index in range(size):
            self.counter += 1
            identity = str(uuid5(self.run_id, str(self.counter)))
            invoice_id = f"INV-{identity}"
            merchant = HOT_MERCHANT if index in flags["hot"] else self.other_merchant()
            amount = format(
                Decimal(self.rng.randint(1, 50_000_000)) / Decimal(10000), ".4f"
            )
            currency = self.rng.choice(CURRENCIES)
            event_time = start + index * interval
            event = {
                "event_id": identity,
                "idempotency_key": f"PAY-{identity}",
                "gateway_ref": f"GW-{identity}",
                "invoice_id": invoice_id,
                "merchant_id": merchant,
                "amount_dec": amount,
                "currency": currency,
                "event_ts": timestamp(event_time),
            }
            # Capture canonical financial values before gateway-only poisoning.
            ledger_transactions.append(
                {
                    name: event[name]
                    for name in (
                        "invoice_id",
                        "merchant_id",
                        "amount_dec",
                        "currency",
                        "event_ts",
                    )
                }
            )
            messy = index in flags["messy"]
            settlements.append(
                {
                    "settlement_id": f"BANK-{identity}",
                    "gateway_ref": "" if messy else event["gateway_ref"],
                    "merchant_id": merchant,
                    "settled_amount_dec": amount,
                    "currency": currency,
                    "remittance_memo": (
                        f"ACH PMT / treasury advice: inv#{invoice_id} ; LESS FEES / thank you"
                        if messy
                        else f"Invoice {invoice_id}"
                    ),
                    "batch_date": (event_time + timedelta(days=1)).date().isoformat(),
                }
            )
            if index in flags["poison"]:
                if self.rng.randrange(2):
                    event["amount_dec"] = format(-Decimal(amount), ".4f")
                else:
                    event["currency"] = "INVALID"
            payload = encode(event)
            messages.append(
                (timestamp(event_time), topic, event["idempotency_key"], payload, 0)
            )
            if index in flags["retry"]:
                due = event_time + timedelta(minutes=self.rng.randint(15, 120))
                # Preserve the exact business payload, including event time and IDs.
                messages.append(
                    (timestamp(due), topic, event["idempotency_key"], payload, 1)
                )
        return messages, settlements, ledger_transactions


class Outbox:
    """Disk-backed, at-least-once delivery; delete only after sink acknowledgement."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS pending (
                id INTEGER PRIMARY KEY, due_at TEXT NOT NULL, topic TEXT NOT NULL,
                message_key TEXT NOT NULL, payload TEXT NOT NULL, is_retry INTEGER NOT NULL
            )
        """)
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS pending_due ON pending(due_at, id)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS pending_key ON pending(message_key, is_retry)"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS ledger_pending "
            "(invoice_id TEXT PRIMARY KEY, payload TEXT NOT NULL)"
        )
        self.connection.commit()

    def add(self, messages, ledger_transactions=()):
        with self.connection:
            self.connection.executemany(
                "INSERT INTO pending (due_at, topic, message_key, payload, is_retry) "
                "VALUES (?, ?, ?, ?, ?)",
                messages,
            )
            self.connection.executemany(
                "INSERT INTO ledger_pending (invoice_id, payload) VALUES (?, ?)",
                [(row["invoice_id"], encode(row)) for row in ledger_transactions],
            )

    def ledger_batch(self):
        return [
            json.loads(row[0])
            for row in self.connection.execute(
                "SELECT payload FROM ledger_pending ORDER BY rowid LIMIT 100"
            ).fetchall()
        ]

    def acknowledge_ledger(self, transactions):
        with self.connection:
            self.connection.executemany(
                "DELETE FROM ledger_pending WHERE invoice_id = ?",
                [(row["invoice_id"],) for row in transactions],
            )

    def ledger_count(self):
        return self.connection.execute(
            "SELECT COUNT(*) FROM ledger_pending"
        ).fetchone()[0]

    def first(self):
        return self.connection.execute(
            "SELECT id, due_at, topic, message_key, payload, is_retry "
            "FROM pending ORDER BY due_at, id LIMIT 1"
        ).fetchone()

    def acknowledge(self, row, emitted_at):
        with self.connection:
            if not row[5]:
                retry = self.connection.execute(
                    "SELECT id, due_at FROM pending WHERE message_key = ? AND is_retry = 1",
                    (row[3],),
                ).fetchone()
                if retry:
                    delay = datetime.fromisoformat(retry[1]) - datetime.fromisoformat(
                        row[1]
                    )
                    self.connection.execute(
                        "UPDATE pending SET due_at = ? WHERE id = ?",
                        (timestamp(emitted_at + delay), retry[0]),
                    )
            self.connection.execute("DELETE FROM pending WHERE id = ?", (row[0],))

    def count(self):
        return self.connection.execute("SELECT COUNT(*) FROM pending").fetchone()[0]

    def close(self):
        self.connection.close()


def write_settlements(directory: Path, run_id, block_number: int, rows):
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"settlements_{run_id}_{block_number:08d}.csv"
    temporary = target.with_suffix(".csv.tmp")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=BANK_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    # Auto Loader sees only complete CSVs, never partially written batches.
    temporary.replace(target)


class KafkaSink:
    def __init__(self, bootstrap_servers: str):
        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise RuntimeError(
                "Install Kafka client: python -m pip install -r data_generator/requirements.txt"
            ) from exc
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "delivery.timeout.ms": 30000,
                "queue.buffering.max.kbytes": 16384,
            }
        )

    def send(self, topic, key, payload, emitted_at):
        outcome = []
        self.producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=payload.encode("utf-8"),
            on_delivery=lambda error, message: outcome.append(error),
        )
        remaining = self.producer.flush(35)
        if remaining or not outcome or outcome[0] is not None:
            raise RuntimeError(
                f"Kafka delivery failed: {outcome or 'delivery timed out'}"
            )

    def close(self):
        self.producer.flush(5)


class JsonlSink:
    """Offline sink with explicit simulated arrival times for replay assertions."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = path.open("a", encoding="utf-8")

    def send(self, topic, key, payload, emitted_at):
        self.stream.write(
            encode(
                {
                    "topic": topic,
                    "key": key,
                    "emitted_at": timestamp(emitted_at),
                    "payload": json.loads(payload),
                }
            )
            + "\n"
        )
        self.stream.flush()

    def close(self):
        self.stream.close()


class Clock:
    def now(self):
        return datetime.now(timezone.utc)

    def wait_until(self, target):
        # Only wall-clock scheduling uses floating-point seconds; money never does.
        time.sleep(max(0, min(1, (target - self.now()).total_seconds())))


class VirtualClock(Clock):
    def __init__(self, start=None):
        self.current = start or datetime.now(timezone.utc)

    def now(self):
        return self.current

    def wait_until(self, target):
        self.current = max(self.current, target)


def run(args, sink, outbox, clock, stopped=lambda: False, ledger=None):
    generator = ScenarioGenerator(args.seed)
    interval = timedelta(microseconds=1_000_000 // args.rate)
    next_original = clock.now()
    generated = delivered = retries = block_number = 0
    while not stopped():
        ledger_transactions = outbox.ledger_batch()
        if ledger_transactions:
            if ledger is None:
                raise RuntimeError(
                    "Pending ledger work requires a PostgreSQL ledger store"
                )
            ledger.write(ledger_transactions)
            outbox.acknowledge_ledger(ledger_transactions)
            LOG.info("Ledger batch committed: invoices=%s", len(ledger_transactions))
            continue
        row = outbox.first()
        if generated < args.count and clock.now() >= next_original:
            size = min(100, args.count - generated)
            messages, settlements, ledger_transactions = generator.block(
                size, next_original, interval, args.topic
            )
            # Publish the independent bank source as atomic, bounded-size CSV batches.
            write_settlements(
                args.landing_dir, generator.run_id, block_number, settlements
            )
            outbox.add(messages, ledger_transactions if ledger is not None else ())
            generated += size
            block_number += 1
            next_original += size * interval
            continue
        if row is None and generated >= args.count:
            break
        due = datetime.fromisoformat(row[1]) if row else next_original
        if due > clock.now():
            target = min(due, next_original) if generated < args.count else due
            clock.wait_until(target)
            continue
        emitted_at = clock.now()
        sink.send(row[2], row[3], row[4], emitted_at)
        outbox.acknowledge(row, clock.now())
        delivered += 1
        retries += row[5]
        if delivered % 100 == 0:
            LOG.info(
                "generated=%s delivered=%s retries=%s", generated, delivered, retries
            )
    result = {
        "generated": generated,
        "delivered": delivered,
        "retries": retries,
        "pending": outbox.count(),
        "pending_ledger": outbox.ledger_count(),
    }
    LOG.info("Finished: %s", result)
    return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="gateway_events")
    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        help="New original events; 0 drains the existing outbox only",
    )
    parser.add_argument(
        "--rate", type=int, default=10, help="Target originals per second"
    )
    parser.add_argument("--seed", type=int)
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data_generator/output")
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write JSONL and advance a virtual clock; no Kafka or real waits",
    )
    args = parser.parse_args(argv)
    if args.count < 0 or not 1 <= args.rate <= 1_000_000:
        parser.error("--count must be >= 0; --rate must be between 1 and 1000000")
    if not args.topic.strip():
        parser.error("--topic must not be empty")
    mode = "dry_run" if args.dry_run else "live"
    args.output_dir = args.output_dir / mode
    args.landing_dir = args.output_dir / "bank_settlements"
    return args


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    LOG.info(
        "mode=%s originals=%s; live retries wait 15-120 minutes; Ctrl+C preserves pending rows",
        "dry-run" if args.dry_run else "live",
        args.count,
    )
    try:
        with ExitStack() as stack:
            outbox = Outbox(args.output_dir / "outbox.sqlite3")
            stack.callback(outbox.close)
            sink = (
                JsonlSink(args.output_dir / "gateway_events.jsonl")
                if args.dry_run
                else KafkaSink(args.bootstrap_servers)
            )
            stack.callback(sink.close)
            ledger = None
            if not args.dry_run:
                if __package__:
                    from .ledger import PostgresLedger
                else:
                    from ledger import PostgresLedger
                ledger = PostgresLedger()
                stack.callback(ledger.close)
            run(
                args,
                sink,
                outbox,
                VirtualClock() if args.dry_run else Clock(),
                lambda: stopping,
                ledger=ledger,
            )
    except Exception:
        LOG.exception("Producer failed; pending messages remain in the outbox")
        return 1
    return 130 if stopping else 0


if __name__ == "__main__":
    raise SystemExit(main())
