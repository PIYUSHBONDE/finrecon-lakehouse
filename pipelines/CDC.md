# CDC normalization and atomic SCD Type 2

This local stage reads `bronze_cdc_ledger` incrementally with `AvailableNow` and
the existing Spark/Delta runner. It processes Debezium JSON for PostgreSQL
`public.accounts`, `public.invoices`, and `public.ledger_entries`.

```powershell
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m pipelines.cdc --dataset portfolio_demo
```

## Outputs

All paths are under `/lakehouse/silver/<dataset>/`:

| Table | Purpose |
| --- | --- |
| `silver_cdc_events` | Normalized audit, retaining one row per Kafka source identity |
| `silver_dim_accounts_scd2` | Account business-state history |
| `silver_dim_invoices_scd2` | Invoice history, including `ISSUED` to `PAID` transitions |
| `silver_ledger_entries` | Latest journal state, including logical delete markers |
| `silver_quarantine` | Contract failures, conflicting versions, and unrepresented stale changes |

CDC rows retain raw payload, message key, Kafka coordinates, source timestamp,
WAL position, operation, contract fingerprint, and typed business values.
Invoices and journal money use `DECIMAL(18,4)`. Rules in `contracts/cdc.yaml`
reject bad precision, missing keys, key mismatches, unsupported operations,
incorrect source identity, and malformed ordering metadata. Deletes may contain
only a key; snapshots, creates, and updates require their business fields.

Audit rows include valid changes, quarantined inputs, and Kafka tombstones.
`failed_rules` in this audit is the normalization contract result. State-order
rejections are recorded in quarantine, so the audit alone is not an applied-state
table. Consumers use the dimension/current-journal tables for operational state.

## Authoritative ordering

Ordering uses PostgreSQL `source.lsn`, then transaction `total_order` when
available, otherwise Kafka offset. WAL positions are exact `DECIMAL(20,0)`
metadata, supporting the unsigned 64-bit range. `source_version` is a sortable
string containing a 20-digit zero-padded WAL position and 20-digit order value.
It is an ordering token, not a financial amount.

This deployment captures one PostgreSQL keyspace with stable topic partitioning.
Do not reuse these checkpoints for a different database, Kafka cluster, topic
mapping, or repartitioned source. Tied WAL positions use transaction order or
the existing per-key Kafka order; independently republished unordered feeds
require stable source ordering metadata before ingestion.

Connector processing time and ingestion time never decide which version wins.
Events sharing a database timestamp remain distinct when their WAL positions
differ. Repeating the same business state advances the source watermark without
creating an unnecessary SCD version. A state changing A to B and back to A
creates all three versions, even within one micro-batch.

## Atomic history and deletes

For each dimension, a single union-source Delta MERGE stages control rows to
expire the previous current version and insert rows for every new state segment.
There is no separate expire-then-append transaction or last-event-only collapse.
The transaction leaves exactly one current marker per known key.

Delete events close the active version and insert a current `is_deleted = true`
marker. This marker prevents a later-arriving old update from resurrecting the
entity. A genuinely newer create can reinsert the key. Kafka tombstones are
retained in the audit but do not act as another delete without a WAL version.
Deleted markers can have incomplete attributes when the before image is partial;
the historical active version and raw event remain available.

History intervals are half-open: `[eff_start_version, eff_end_version)`. Version
tokens are authoritative because multiple changes can share a millisecond.
Display timestamps are clamped monotonically when the source clock regresses;
the original `source_ts_ms`, last observed timestamp, and `clock_regressed` flag
preserve that fact. Equal display timestamps do not imply equal source versions.

Already represented historical events are harmless replays. An old event that
would rewrite applied history is quarantined instead of rolling current state
back. This stage does not automatically rebuild history around a newly discovered
stale change. Conflicting payloads at the same ordered version are also rejected.

## Recovery and boundaries

Each dimension MERGE is atomic. Accounts, invoices, journals, quarantine, and
audit are separate Delta transactions. The audit acknowledgement is written
only after state merges and quarantine succeed. If a process fails earlier,
retry finds represented SCD versions and the same journal state, then completes
the audit without duplicating history. Consumers should wait for the complete
CDC job to succeed before running reconciliation. Use one writer per dataset.

Raw CDC remains unchanged. Valid current records use `is_current AND NOT
is_deleted` for dimensions and `NOT is_deleted` for journals. An account's
business `is_active` flag is distinct from physical deletion.

Gateway/bank correction replay deliberately rejects CDC quarantine IDs. Repair
operational data at its source and capture a new WAL-ordered change; do not
invent ordering metadata in a gateway correction file. A specialized historical
CDC backfill/resolution workflow is outside this stage.

Kafka now uses a named data volume. Migration preserved the original anonymous
volume and verified broker identity, topic IDs, offsets, and file metadata.
A coordinated physical restore resumed the existing connector and Bronze
checkpoints in an isolated network. Preserve broker data and Delta checkpoints;
container recreation with an empty volume is not a recovery procedure.
See [the recovery drills and boundaries](../operations/RECOVERY.md).

## Verification

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify_cdc.ps1
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m unittest discover -s tests -p test_cdc.py -v
```

The verifier uses the existing correlated Bronze fixture. It checks the full
normalized audit, one current version per key, unique/non-overlapping history
versions, exact invoice amounts, both invoice states, and balanced journal legs
for 100 correlated payments. Table digests must remain identical after query
restart and a second fresh container. Use a quiet fixture for these assertions.
Results are written to `pipelines/output/<dataset>/cdc_verification.json`.

Verified locally on 2026-11-11: 1,026 normalized CDC records, including 52 Kafka
tombstones. The correlated fixture retained 100 invoices with 200 history rows
and 200 balanced active journal entries. All four output table digests remained
unchanged after query restart and a second fresh container. The report confirms
one current version per key and non-overlapping version intervals.

Tests inject failures before the atomic SCD commit and after state commits but
before audit acknowledgement. They also exercise same-millisecond updates,
no-op updates, delete/tombstone/reinsert sequences, late updates, clock regression,
unsigned WAL precision, malformed input, and decimal overflow/scale limits.

References: [Delta SCD-2 MERGE pattern](https://docs.delta.io/delta-update/),
[Debezium PostgreSQL event metadata](https://debezium.io/documentation/reference/3.0/connectors/postgresql.html).
