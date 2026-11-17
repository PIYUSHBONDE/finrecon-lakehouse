# FinRecon chaos producer

Run commands from the repository root with Python 3.11 or newer.

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r data_generator/requirements.txt
.venv/Scripts/python.exe data_generator/chaos_producer.py --count 1000 --rate 10 --seed 42
```

The default broker is `localhost:9092`, matching Docker Compose's host listener.
Use `--bootstrap-servers kafka:29092` when running inside the Compose network.
The producer sends JSON to `gateway_events`; override with `--topic`.
Create that topic before running if broker topic auto-creation is disabled.

Live runs also connect to PostgreSQL at `localhost:15432`, using database, user,
and password `finrecon`. Docker's host mapping is `15432:5432` because a separate
Windows PostgreSQL instance already uses port 5432. Debezium still connects to
`postgres:5432` inside Docker. Override the producer connection when needed:

```powershell
$env:FINRECON_POSTGRES_DSN = 'host=localhost port=15432 dbname=finrecon user=finrecon password=finrecon'
```

Inside the Compose network, set the DSN host to `postgres` and port to `5432`.

## Correlated ledger source

Each original payment now produces:

- One invoice with the same `invoice_id` as its gateway event and bank memo.
- A merchant/currency payment account and a separate clearing account, reused
  across that merchant's payments.
- Exactly two journal entries: payment debit and clearing credit, each equal to
  the clean invoice amount. Entry IDs are deterministic for the invoice and leg.
- An invoice transition from `ISSUED` to `PAID`, visible as CDC insert/update events.

The canonical amount and currency are captured before gateway poisoning. Invalid
gateway values therefore do not corrupt the ledger or bank source. Messy bank
rows still omit the direct reference and retain the invoice ID only in memo text.

A PostgreSQL transaction commits each batch of at most 100 invoices together
with both journal legs. Gateway publication waits for ledger commitment. Replays
validate existing invoices/postings and preserve their rows and timestamps.
Conflicting financial values or incomplete journals stop processing; they are
never silently overwritten. Application-level gateway retries do not post again.

The existing schema stores merchant on accounts and currency on invoices. Join
those tables when enriching journal rows. Reconcile one invoice amount or its
debit leg; summing both journal legs would double the payment. Compare balances
per currency until the FX conversion layer exists.

## Chaos guarantees

Every complete block of **100 original transactions** contains:

| Scenario | Count | Behavior |
| --- | ---: | --- |
| Merchant skew | 65 | `MERCHANT_ENT_01`; remaining merchants follow integer Zipf weights |
| Retry candidates | 5 | One byte-identical retry, scheduled 15–120 minutes after original acknowledgement |
| Poison pills | 1 | Negative `amount_dec` or `currency = "INVALID"` in the gateway payload |
| Messy bank memos | 2 | Blank `gateway_ref`; invoice ID appears only inside free text |

Flags overlap independently. Percentages use originals as the denominator; 1,000
originals produce 1,050 gateway messages and 1,000 bank rows. Duplicate selection
can slightly change the merchant/poison proportions among all emitted messages.
Final blocks smaller than 100 round each quota down. Use multiples of 100 for
exact percentages. Seed controls scenario placement and money, while IDs and
start time stay unique between invocations.

All amounts originate from integer ten-thousandths, use Python `Decimal`, and
serialize as fixed four-place strings. Read them into `DECIMAL(18,4)` downstream.
Only wall-clock sleeping uses floating-point seconds. Poison pills remain valid
JSON so contract validation can quarantine them without parser failures.

`event_id`, `idempotency_key`, `invoice_id`, `gateway_ref`, and `event_ts` remain
unchanged across retries. Kafka keys use `idempotency_key`, so a retry follows
the same partition as its original. Merchant skew remains available for the
downstream merchant join without concentrating ingestion into one Kafka key.

## Output and restart

Live output lives in `data_generator/output/live/`:

- `outbox.sqlite3`: durable scheduled originals/retries and pending canonical
  ledger work, committed together locally.
- `bank_settlements/*.csv`: atomic batches of at most 100 bank rows.

CSV rows contain `settlement_id`, `gateway_ref`, `merchant_id`,
`settled_amount_dec`, `currency`, `remittance_memo`, and `batch_date`.
Bank rows preserve the clean financial values before gateway poisoning.
Bank batch dates represent the next UTC day. Files land immediately as fixtures;
real delayed bank-file delivery is separate work.
Point Auto Loader at `*.csv` so it ignores `.csv.tmp` files.

After generating originals, the process **waits for all retries**, potentially
two hours. Ctrl+C stops new work and keeps pending rows. Drain without adding
transactions using the same output directory and mode:

```powershell
.venv/Scripts/python.exe data_generator/chaos_producer.py --count 0
```

Restart downtime or broker outages can make delivery later than 120 minutes.
Retries retain original event time even when delivered after recovery.
Run only one producer per output directory. Pending rows retain their topic;
resume against the same broker. A normal invocation adds `--count` new originals
alongside existing pending messages.

If PostgreSQL commits before the SQLite acknowledgement is saved, restart
replays the same canonical invoices without extra journal entries. A PostgreSQL
failure leaves both ledger and Kafka work pending. Outboxes generated by the
earlier gateway-only version cannot backfill a clean ledger for old events;
use a new output directory for a fresh three-source dataset.

Kafka sends require acknowledgement before deleting the outbox row. Delivery
errors exit nonzero and preserve unacknowledged messages. A crash between broker
acknowledgement and SQLite deletion can replay a message: delivery is at least
once. Kafka protocol idempotence protects client transport retries, while the
intentional application retries still reach Kafka. SQLite and CSV/Kafka are not
one distributed transaction; a crash between bank publication and outbox insert
can leave an unmatched bank batch, suitable for the open-item path.

The generator retains at most one block in Python memory. SQLite stores pending
work on disk. Kafka sends are acknowledged synchronously for simple recovery;
`--rate` is a target, not a throughput guarantee. This is a correctness fixture
generator, not a broker load benchmark.

## Offline verification

```powershell
python data_generator/chaos_producer.py --dry-run --count 1000 --seed 42
python -m unittest discover -s tests -v
```

Dry runs require no third-party packages. They advance a virtual clock and write
`output/dry_run/gateway_events.jsonl` with `topic`, `key`, `emitted_at`, and
`payload`. Live Kafka values contain only the payload. Dry-run and live outboxes
are separate. Repeated runs append JSONL and add uniquely named bank batches.
Dry runs do not connect to PostgreSQL or emit CDC; unit tests still validate the
canonical ledger values derived before gateway poisoning.

## Live three-source verification

With the Docker stack and connector running, run this from the repository root:

```powershell
.venv/Scripts/python.exe -m data_generator.verify_sources
```

The verifier creates an isolated gateway topic and dataset directory. It inserts
100 invoices, deliberately loses the ledger acknowledgement after PostgreSQL
commit, reopens SQLite, and resumes delivery. It checks:

- 105 consumed gateway messages, 100 correlated invoices, 200 journal entries,
  and 100 bank rows.
- 100 invoice inserts, 100 invoice updates, and 200 journal inserts consumed from
  the actual Debezium Kafka topics.
- Equal invoice, bank, debit, and credit totals independently for each currency.
- Unchanged journal IDs and timestamps after the lost-acknowledgement replay.
- Preserved merchant skew, gateway poisoning, and messy-memo quotas.

Verification on 2026-11-11 passed all checks above, including the simulated lost
acknowledgement after PostgreSQL commit. The interrupted prior run also resumed
its six retained Kafka messages and finished with no pending work. Reports are
under `output/correlated_b5a71edf/live/verification.json` and
`output/correlated_fd4fd7b1/live/interruption_recovery.json` respectively.

PostgreSQL and Kafka transport are real; a virtual clock accelerates gateway
retry scheduling. The verifier leaves labelled synthetic data and outputs under
`output/correlated_*/live/`: `verification.json`, `canonical_transactions.json`,
`cdc_events.json`, bank CSVs, and the drained outbox.

Three additional integration tests exercise real PostgreSQL replay, conflicting
amounts, and rollback when a credit insert fails. They are opt-in:

```powershell
$env:FINRECON_TEST_POSTGRES_DSN = 'host=localhost port=15432 dbname=finrecon user=finrecon password=finrecon'
.venv/Scripts/python.exe -m unittest discover -s tests -p test_postgres_ledger.py -v
```

These tests create uniquely labelled `TEST_*` merchants and clean up only their
own records. Default unit-test discovery skips database tests when the environment
variable is absent.

Wire format is deliberately JSON for the current Compose stack, which has no
Schema Registry. Avro decimal enforcement belongs to the later registry setup.
Live runs populate PostgreSQL but do not register Debezium. The connector JSON
matches Compose's `finrecon` database and local credentials, and uses the
Debezium 2.4 `topic.prefix` setting. See [CDC setup](../docker/debezium/README.md).

Live smoke verification on 2026-11-10 consumed 105 gateway messages for 100
originals, including five retries recovered after reopening the SQLite outbox.
It also checked 65 hot-merchant originals, one poison pill, and two messy memos
among 100 bank rows. Kafka transport was real; retry time advanced through a
virtual clock rather than waiting 15–120 real minutes.

Kafka client delivery semantics follow the [Confluent Python API documentation](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html).
Ledger transaction boundaries follow the [Psycopg transaction documentation](https://www.psycopg.org/psycopg3/docs/basic/transactions.html).
