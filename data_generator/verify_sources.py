"""Verify correlated payments through PostgreSQL, CDC, Kafka, and bank CSVs.

Run: .venv/Scripts/python.exe -m data_generator.verify_sources
Kafka and PostgreSQL transport are real. Retry scheduling uses a virtual clock.
Synthetic source records and a verification report remain for inspection.
"""

import argparse
import csv
import json
import logging
import time
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition

from .chaos_producer import KafkaSink, Outbox, VirtualClock, parse_args, run
from .ledger import PostgresLedger


def verify(count=100):
    if count < 100 or count > 10000 or count % 100:
        raise ValueError(
            "Fixture count must be a multiple of 100 between 100 and 10000"
        )
    retries, poison, messy, hot = (
        count // 20,
        count // 100,
        count // 50,
        count * 65 // 100,
    )
    root = Path("data_generator/output") / f"correlated_{uuid4().hex[:8]}"
    topic = f"gateway_correlated_{uuid4().hex[:8]}"
    args = parse_args(
        [
            "--count",
            str(count),
            "--rate",
            "100",
            "--seed",
            "42",
            "--topic",
            topic,
            "--output-dir",
            str(root),
        ]
    )
    source_topics = ("finrecon_db.public.invoices", "finrecon_db.public.ledger_entries")
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": f"finrecon-verify-{uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    ledger = sink = box = None
    arrivals = []
    try:
        metadata = consumer.list_topics(timeout=10)
        offsets = {}
        for source_topic in source_topics:
            details = metadata.topics.get(source_topic)
            if details and not details.error:
                for number in details.partitions:
                    offsets[(source_topic, number)] = consumer.get_watermark_offsets(
                        TopicPartition(source_topic, number),
                        timeout=10,
                    )[1]
        ledger = PostgresLedger()
        sink = KafkaSink(args.bootstrap_servers)
        box = Outbox(args.output_dir / "outbox.sqlite3")
        clock = VirtualClock()

        class RecordingSink:
            def send(self, target_topic, key, payload, emitted_at):
                sink.send(target_topic, key, payload, emitted_at)
                arrivals.append((key, payload, emitted_at))

        # Model the crash window: PostgreSQL commits but SQLite never receives
        # acknowledgement. Resume must validate existing postings without adding any.
        with patch.object(
            box,
            "acknowledge_ledger",
            side_effect=RuntimeError("simulated lost acknowledgement"),
        ):
            try:
                run(args, RecordingSink(), box, clock, ledger=ledger)
            except RuntimeError as error:
                if str(error) != "simulated lost acknowledgement":
                    raise
            else:
                raise AssertionError("Failure injection did not execute")
        canonical = box.ledger_batch()
        invoice_ids = [row["invoice_id"] for row in canonical]
        assert len(invoice_ids) == 100 and box.ledger_count() == 100
        assert box.count() == 105 and not arrivals
        injected_ids = set(invoice_ids)

        def journals():
            return ledger.connection.execute(
                "SELECT entry_id, invoice_id, account_id, debit, credit, entry_ts "
                "FROM public.ledger_entries WHERE invoice_id = ANY(%s) ORDER BY entry_id",
                (invoice_ids,),
            ).fetchall()

        before_replay = journals()
        assert len(before_replay) == 200
        box.close()
        box = Outbox(args.output_dir / "outbox.sqlite3")
        args.count = count - 100
        write = ledger.write

        def record_write(transactions):
            write(transactions)
            canonical.extend(
                row for row in transactions if row["invoice_id"] not in injected_ids
            )

        with patch.object(ledger, "write", side_effect=record_write):
            result = run(args, RecordingSink(), box, clock, ledger=ledger)
        assert result == {
            "generated": count - 100,
            "delivered": count + retries,
            "retries": retries,
            "pending": 0,
            "pending_ledger": 0,
        }, result
        assert journals() == before_replay, "Replay changed journal rows or timestamps"
        invoice_ids = [row["invoice_id"] for row in canonical]
        assert len(invoice_ids) == len(set(invoice_ids)) == count
        before_replay = journals()
        assert len(before_replay) == count * 2

        invoices = ledger.connection.execute(
            "SELECT i.invoice_id, i.total_amount, i.currency, i.status, a.merchant_id "
            "FROM public.invoices i JOIN public.accounts a ON a.account_id = i.account_id "
            "WHERE i.invoice_id = ANY(%s)",
            (invoice_ids,),
        ).fetchall()
        invoice_map = {row[0]: row[1:] for row in invoices}
        canonical_map = {row["invoice_id"]: row for row in canonical}
        assert len(invoice_map) == count
        journal_map = defaultdict(list)
        for row in before_replay:
            journal_map[row[1]].append(row)
        for row in canonical:
            identity, amount = row["invoice_id"], Decimal(row["amount_dec"])
            assert invoice_map[identity] == (
                amount,
                row["currency"],
                "PAID",
                row["merchant_id"],
            )
            entries = journal_map[identity]
            assert len(entries) == 2 and entries[0][2] != entries[1][2]
            assert sum(entry[3] for entry in entries) == amount
            assert sum(entry[4] for entry in entries) == amount
        bank = []
        for path in args.landing_dir.glob("*.csv"):
            with path.open(newline="", encoding="utf-8") as stream:
                bank.extend(csv.DictReader(stream))
        assert (
            len(bank) == count and sum(not row["gateway_ref"] for row in bank) == messy
        )
        for row in bank:
            identity = "INV-" + row["settlement_id"].removeprefix("BANK-")
            clean = canonical_map[identity]
            assert row["settled_amount_dec"] == clean["amount_dec"]
            assert row["currency"] == clean["currency"]
            assert row["merchant_id"] == clean["merchant_id"]
            assert identity in row["remittance_memo"]
        invoice_totals, bank_totals, debit_totals, credit_totals = (
            defaultdict(Decimal) for _ in range(4)
        )
        for row in invoices:
            invoice_totals[row[2]] += row[1]
        for row in bank:
            bank_totals[row["currency"]] += Decimal(row["settled_amount_dec"])
        for row in before_replay:
            currency = canonical_map[row[1]]["currency"]
            debit_totals[currency] += row[3]
            credit_totals[currency] += row[4]
        assert invoice_totals == bank_totals == debit_totals == credit_totals

        metadata = consumer.list_topics(timeout=10)
        consumer.assign(
            [
                TopicPartition(target, number, offsets.get((target, number), 0))
                for target in (*source_topics, topic)
                for number in metadata.topics[target].partitions
            ]
        )
        expected_gateway = Counter((key, payload) for key, payload, arrival in arrivals)
        gateway, cdc = Counter(), []
        deadline = time.monotonic() + 45
        while (
            sum(gateway.values()) < count + retries or len(cdc) < count * 4
        ) and time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            if message.topic() == topic:
                gateway[(message.key().decode(), message.value().decode())] += 1
                continue
            envelope = json.loads(message.value())
            event = envelope.get("payload", envelope)
            if (event.get("after") or {}).get("invoice_id") in canonical_map:
                cdc.append(event)
        assert gateway == expected_gateway, f"Gateway consumed: {sum(gateway.values())}"
        counts = Counter((event["source"]["table"], event["op"]) for event in cdc)
        assert counts == {
            ("invoices", "c"): count,
            ("invoices", "u"): count,
            ("ledger_entries", "c"): count * 2,
        }, counts
        for event in cdc:
            clean = canonical_map[event["after"]["invoice_id"]]
            if event["source"]["table"] == "invoices":
                assert event["after"]["total_amount"] == clean["amount_dec"]
                if event["op"] == "u":
                    assert event["before"]["status"] == "ISSUED"
                    assert event["after"]["status"] == "PAID"
            else:
                assert isinstance(event["after"]["debit"], str)
                assert isinstance(event["after"]["credit"], str)
        by_key = defaultdict(list)
        for key, payload, arrival in arrivals:
            by_key[key].append((payload, arrival))
        assert Counter(len(rows) for rows in by_key.values()) == {
            1: count - retries,
            2: retries,
        }
        for rows in by_key.values():
            if len(rows) == 2:
                assert rows[0][0] == rows[1][0]
                assert 15 * 60 <= (rows[1][1] - rows[0][1]).total_seconds() <= 120 * 60
        originals = [json.loads(rows[0][0]) for rows in by_key.values()]
        assert sum(row["merchant_id"] == "MERCHANT_ENT_01" for row in originals) == hot
        assert (
            sum(
                row["amount_dec"] != canonical_map[row["invoice_id"]]["amount_dec"]
                or row["currency"] != canonical_map[row["invoice_id"]]["currency"]
                for row in originals
            )
            == poison
        )
        report = {
            "gateway_topic": topic,
            "gateway_messages_consumed": sum(gateway.values()),
            "correlated_invoices": len(invoices),
            "ledger_entries": len(before_replay),
            "bank_rows": len(bank),
            "messy_memos": messy,
            "hot_originals": hot,
            "poison_pills": poison,
            "exact_retries_verified": retries,
            "retry_delays_minutes": [15, 120],
            "cdc_events_consumed": len(cdc),
            "cdc_invoice_inserts": counts[("invoices", "c")],
            "cdc_invoice_updates": counts[("invoices", "u")],
            "cdc_journal_inserts": counts[("ledger_entries", "c")],
            "totals_by_currency": {
                currency: {
                    "invoice_amount": format(invoice_totals[currency], ".4f"),
                    "bank_amount": format(bank_totals[currency], ".4f"),
                    "ledger_debit": format(debit_totals[currency], ".4f"),
                    "ledger_credit": format(credit_totals[currency], ".4f"),
                }
                for currency in sorted(invoice_totals)
            },
            "lost_acknowledgement_replay_preserved_postings": True,
            "retry_clock": "virtual; PostgreSQL and Kafka transport real",
            "pending": 0,
            "pending_ledger": 0,
            "output_dir": str(args.output_dir),
        }
        (args.output_dir / "verification.json").write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        (args.output_dir / "canonical_transactions.json").write_text(
            json.dumps(canonical, indent=2), encoding="utf-8"
        )
        (args.output_dir / "cdc_events.json").write_text(
            json.dumps(cdc, indent=2), encoding="utf-8"
        )
        print(json.dumps(report, indent=2))
        return report
    finally:
        if box is not None:
            box.close()
        if sink is not None:
            sink.close()
        if ledger is not None:
            ledger.close()
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=100)
    verify(parser.parse_args().count)
