# Gateway and bank Silver

This slice implements YAML contracts, quarantine, financial deduplication, and
explicit correction replay for gateway events and bank settlements. Everything
runs in the existing local Spark/Delta container with no cloud service.

CDC normalization, account/invoice SCD-2, and ledger state run in the separate
[CDC stage](CDC.md). The implemented [Gold stage](GOLD.md) consumes these outputs. This
gateway/bank job leaves raw Bronze CDC unchanged.

## Normal ingestion

```powershell
docker compose -f docker/docker-compose.yml --profile compute build bronze
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m pipelines.silver --dataset portfolio_demo
```

The Compose service is named `bronze` because it is the shared local compute
runner; its Python command selects which pipeline runs. Each job incrementally
reads Bronze Delta with `AvailableNow` and separate persistent Silver checkpoints.
Input batches are limited to 25 files or approximately 32 MB, and gateway/bank
streams run sequentially inside the existing memory limit.

Rules in `contracts/gateway.yaml` and `contracts/bank.yaml` compile into one
native Spark `failed_rules` array expression. SQL null results fail a rule.
Evaluated batches use memory-and-disk persistence for their valid/quarantine
branches. No Python UDF or Python row loop evaluates contracts.

Financial strings must match a plain decimal format with at most four decimal
places and must fit `DECIMAL(18,4)` with a strictly positive value. Overflow,
negative/zero values, scientific notation, and excessive scale are rejected.
The lexical gate rejects excessive scale even if Spark's decimal cast could
round it. No monetary float/double is created or used for calculations.

The bank contract allows a missing `gateway_ref` when a memo exists, preserving
the two messy-memo exceptions for later matching rather than quarantining them.

## Tables and identities

Paths are `/lakehouse/silver/<dataset>/`:

| Table | Key | Financial/date columns |
| --- | --- | --- |
| `silver_gateway_events` | `idempotency_key` / `entity_key` | `amount_dec DECIMAL(18,4)`, `event_ts`, `event_date` |
| `silver_bank_settlements` | `settlement_id` / `entity_key` | `settled_amount_dec DECIMAL(18,4)`, `batch_date`, `event_date` |
| `silver_quarantine` | `record_id` | Original payload, failed rules, contract hash, source timestamp, provenance, replay status |

Gateway quarantine IDs derive from source table and Kafka topic/partition/offset.
Bank quarantine IDs derive from source table, file, and raw payload; identical
bad rows within the same file therefore share one quarantine identity. Physical
rows remain available in Bronze. Reprocessing never resets `REPLAYED` to `PENDING`.

Both financial tables are partitioned by `event_date`. Within a batch, exact
business duplicates collapse to one row. Multiple financial variants of a key
are all quarantined. Across batches, bounded target lookup compares canonical
financial hashes: exact retries leave the existing row unchanged, conflicting
values are quarantined, and older source versions cannot replace newer ones.
Financial MERGEs insert only; they do not update existing postings.

The lookback includes the UTC job date and six prior dates. One future date is
allowed for the fixture's planned next-day settlement files. Target lookup and
MERGE predicates prune dates before this window. Older source events are kept
in quarantine, not blindly inserted. The guarantee assumes retries retain their
original business key and event date; this is not an all-history key registry.

This correctness path intentionally does not drop events at a stateful Spark
watermark. Such operators can discard legitimate late arrivals before MERGE
sees them. Incremental micro-batches plus the bounded MERGE retain valid late
payments within the seven-day window. A separate hot-path watermark optimization
would need a Bronze-backed catch-up path before it could replace this design.

## Explicit replay

Normal ingestion leaves invalid rows `PENDING`; it never guesses a corrected
amount or currency. Corrections are a JSONL file containing `record_id` and
`corrected_payload` (a JSON-encoded string) for each repaired record.

```powershell
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m pipelines.replay --dataset portfolio_demo --corrections /workspace/pipelines/output/portfolio_demo/manual_corrections.jsonl
```

Replay re-runs the same current contracts and window checks. A known business
key and known source timestamp cannot change; invalid/missing identity values
may be supplied explicitly. Stale corrections and financial conflicts remain
`PENDING` with `last_replay_error`. Successful insertions or identical existing
rows become `REPLAYED`, retaining both original and corrected payloads.

Silver insertion commits before quarantine acknowledgement. If the process
fails between them, another replay finds the identical financial row and
completes acknowledgement without a second insert. These are separate Delta
transactions, not a multi-table atomic transaction. Run only one ingestion or
replay writer for a dataset at a time.

The default window uses the current UTC date. `--as-of-date YYYY-MM-DD` supports
reproducible fixture audits; it does not rewrite source timestamps. A fixture
older than the window needs a deliberate historical run or a fresh fixture.
Changing YAML rules affects new batches and explicit replays; checkpoints do not
automatically revalidate historical valid rows.

## Verification and tests

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify_silver.ps1
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m unittest discover -s tests -p test_silver.py -v
```

The verifier uses the already verified 100-payment Bronze fixture. It checks
99 valid gateway keys, 100 bank rows, and quarantine of the poisoned original.
It then **explicitly repairs the demo poison from known fixture truth** and
exercises replay, producing 100 correct gateway rows. This test-only correction
file is `pipelines/output/<dataset>/demo_corrections.jsonl`; the normal ingestion
command does not perform that repair.

The verifier checks exact per-currency totals, decimal schemas, repeated replay,
query restarts, and a second fresh container using the same checkpoints. Results
are saved in `pipelines/output/<dataset>/silver_verification.json`. Use a quiet
fixture while checking restart row counts.

Tests cover strict decimal boundaries, malformed JSON, expired events, late
payments, exact retries, financial conflicts, ambiguous batches, stale replay,
changed replay identity, and failure between Silver insertion and quarantine
acknowledgement.

## Verified on 2026-11-11

All five Silver tests passed in the local Spark container. The existing
`portfolio_demo` fixture also passed verification in two fresh containers:

| Check | Result |
| --- | --- |
| Valid gateway keys before the explicit demo repair | 99 |
| Poisoned original quarantined with its raw payload | 1 |
| Bank rows, including two messy memos | 100 |
| Gateway rows after explicit correction replay | 100 |
| Extra financial rows after repeat replay/restart | 0 |
| Decimal schemas and per-currency totals | Passed |
| Fresh-process checkpoint and replay timestamp preservation | Passed |

Inspect `pipelines/output/portfolio_demo/silver_verification.json` and
`demo_corrections.jsonl`. The correction used only the synthetic fixture's known
truth; it is not an automatic production repair policy.

References: [Delta MERGE and foreachBatch idempotency](https://docs.delta.io/delta-update/),
[Delta streaming transactions](https://docs.delta.io/delta-streaming/).
