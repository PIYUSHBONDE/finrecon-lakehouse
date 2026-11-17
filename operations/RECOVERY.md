# Local Kafka recovery

The local broker uses a named Docker volume. The first adoption preserves the
original anonymous volume, an offline sparse archive, an independently restored
probe volume, and a rollback Compose override. These are not removed by the tool.
Never run `docker compose down -v`, volume pruning, or backup cleanup during a
recovery. All copies on this computer share its physical failure domain.

## Migration procedure

Stop producers and compute writers. Keep PostgreSQL running. Run from the project
root with Docker Desktop running and the existing Spark image built:

```powershell
.venv-ci/Scripts/python.exe -m operations.kafka_recovery prepare --directory pipelines/output/recovery/kafka_migration
```

Preparation stops Debezium before freezing Kafka. It records topic IDs, cluster
identity, and both offset boundaries. It archives the stopped data volume with
GNU tar sparse-file support, then restores and verifies every file's SHA-256,
size, owner, mode, and modification time. It boots a second restored broker with
`--network none`, no published ports, and no connection to production services.
The probe must recover exactly the recorded Kafka identities and offsets.

After successful preparation Kafka restarts on its original data. Debezium stays
stopped until cutover so the prepared copy cannot fall behind. Do not produce
events between preparation and cutover. On a preparation error the tool attempts
to restart both original services. Inspect `recovery.json` if recovery itself fails.

Only after preparation passes, add `kafka_data:/var/lib/kafka/data` to Kafka's
Compose volumes and declare `kafka_data` at the top-level volumes. Then run:

```powershell
.venv-ci/Scripts/python.exe -m operations.kafka_recovery cutover --directory pipelines/output/recovery/kafka_migration
```

Cutover refuses changed source offsets or a different target volume. It recreates
only Kafka, checks the actual named mount, rechecks identities and offsets, and
restarts Debezium. Both connector and task must report `RUNNING`.

The tool refuses an existing target during preparation. It never empties a volume
to make a retry pass. Investigate a partial preparation before choosing a new
target or manually reusing a verified copy. Do not run concurrent recoveries.

## Rollback and later recovery

The report directory contains `compose.original.yml`, `rollback.compose.json`,
`manifest.json`, and `data.tar`. Before cutover, rollback uses the original volume:

```powershell
docker stop finrecon_debezium
docker compose -f docker/docker-compose.yml -f pipelines/output/recovery/kafka_migration/rollback.compose.json up -d --no-deps --force-recreate kafka
docker start finrecon_debezium
```

This rollback restores the **pre-migration point**. Once new events are accepted on
the named volume, the old volume and archive are stale. Do not roll back without
reconciling writes made after that point. Preserve the current volume first.
The override file remains necessary until the main Compose mount is corrected.

`operations.volume_archive` also supports offline backup/restore of a mounted
volume. Run it in the pinned Spark image as root, with the repository mounted
read-only at `/workspace`, the stopped source read-only at `/data`, and an empty
report directory at `/backup`. Use `python3 -m operations.volume_archive backup`.
Restore with `restore`, mounting an **empty, unused** target read-write at `/data`
and the backup read-only. A restored archive cannot replace live data in place.

Kafka recovery alone is not a consistent full-stack recovery. PostgreSQL WAL and
replication slots, Debezium offsets, Delta tables/checkpoints, and immutable bank
files must represent a compatible point. A logical PostgreSQL dump alone does
not preserve the physical WAL/replication-slot state. The Kafka drill does not
claim complete disaster recovery, off-host backups, or multi-host resilience.

## Coordinated full-stack drill

After dataset readiness passes, stop producers and compute jobs. Run:

```powershell
.venv-ci/Scripts/python.exe -m operations.stack_recovery --dataset readiness_20261114 --directory pipelines/output/recovery/stack_drill
```

This pauses original Kafka and Debezium throughout the drill to fit the local
memory budget. PostgreSQL stops for the snapshot, then restarts with Connect
still stopped so its durable slot state can be compared. Cleanup restarts all
original services, including on failure. Plan several minutes of local downtime.
Do not launch a producer,
pipeline, cleanup task, or second recovery concurrently.

The tool archives PostgreSQL's **physical** data/WAL/slots, Kafka including Connect
offset topics, the complete Delta lakehouse including checkpoints/manifests, and
the Spark dependency cache. It copies and hashes the generator output directory,
including immutable landing files and durable outboxes. Each restored volume must
match its offline file/metadata manifest before any service starts.
It also archives other workspace bank directories referenced by source manifests.
Missing or external container landing paths stop the backup rather than silently
producing an incomplete recovery set.
A landing directory containing the backup destination is also rejected to prevent
a recursive copy. The guard was verified against all three actual archived source
bindings; their copied bank files matched the original hashes.

Restored PostgreSQL, Kafka, and Debezium run on a separate `--internal` Docker
network without published ports. Runtime inspection verifies network isolation
and cloned financial volumes. PostgreSQL totals and Kafka identities/offsets
must match the frozen point. Persisted replication slots must match the restarted
original while Connect stays stopped. Live slot acknowledgements can exceed the
last durable checkpoint; [PostgreSQL documents this behavior](https://www.postgresql.org/docs/16/logicaldecoding-explanation.html).
The first drill exposed that distinction and preserved its failed report; the
verifier checks exact durable slot state rather than requiring live-memory LSNs
to survive a restart unchanged. Existing connector configuration must restart from
Kafka's restored internal topics; the tool does not re-register or resnapshot it.
The drill's global debit/credit sums are restoration checksums, not financial
reporting totals across currencies. Business proofs and reconciliation totals
remain grouped by native currency.

The restored Bronze checkpoint must first ingest zero old rows. The drill then
writes a new balanced INR 1.0000 invoice **only into cloned PostgreSQL**. It verifies
incremental invoice `c`/`u` events with string decimals and exactly six new Bronze
CDC events: two accounts, two invoice states, and two journal legs. Gateway and
bank checkpoint counts must remain unchanged. The invoice must be absent from
original PostgreSQL. Original Delta tables are never mounted by the probe.

`stack_recovery.json` records phases, restoration proofs, connector status,
runtime isolation, probe events, and both Bronze progress reports. Clone services
and their private network are stopped/removed; restored named volumes, original
volumes, archives, source-file copies, and logs remain. Repeated runs require a
new report directory and create new clones; they never overwrite prior backups.

This is a single-host, coordinated recovery drill through resumed CDC and Bronze.
It does not claim off-host disaster recovery, automatic failover, a second full
Silver/Gold reconciliation on restored data, or protection against disk loss.
Archive directories contain financial records, outboxes, and infrastructure
metadata. Keep them local and access-controlled; they are excluded from Git.
