# Run and inspect the local portfolio

This is the zero-cloud-budget edition. PostgreSQL, Kafka, Debezium, Spark, and
Delta run on this computer. No cloud resource or paid model is activated.

## Start and check

Start Docker Desktop, then run these commands from the repository root:

```powershell
docker compose -f docker/docker-compose.yml up -d postgres kafka debezium
.venv-ci/Scripts/python.exe -m operations.status --dataset readiness_20261114
```

The status command reads live PostgreSQL/Kafka/connector health, verifies actual
named source volumes, and checks the completed readiness proof. It returns exit
code 1 if a check fails or the proof is older than 72 hours. Historical evidence
remains useful after that limit, but a fresh run is required for a fresh status.

The initial environment needs Python 3.11, Docker's Linux engine, the pinned
dependencies in `requirements-dev.txt` and `data_generator/requirements.txt`, and
the local compute image. Create `.venv` for source clients and `.venv-ci` for
operations/CI. Existing environments in this workspace are already prepared.

```powershell
docker compose -f docker/docker-compose.yml --profile compute build bronze
```

If the connector is absent on a new installation, follow
[the Debezium registration guide](../docker/debezium/README.md). Do not reset
Kafka topics, PostgreSQL, or replication slots to repair an existing installation.

## Create fresh, inspectable evidence

```powershell
.venv/Scripts/python.exe -m data_generator.verify_sources --count 1000
```

The command prints the new source report path. It tests real PostgreSQL and Kafka
transport, exact ledger replay after a lost acknowledgement, 65% merchant skew,
5% identical retries, 1% poison pills, and 2% missing-reference bank memos.
Retry delays use a virtual clock; this does not claim two hours of wall time.
Verification accepts complete blocks of 100, up to 10,000 originals. This is a
bounded correctness fixture, not a sustained-load benchmark.

Use the printed report and a **new dataset name**:

```powershell
.venv-ci/Scripts/python.exe -m operations.readiness --source-report data_generator/output/correlated_XXXXXXXX/live/verification.json --dataset my_fresh_demo
```

The verifier runs all six layer proofs, an eight-stage orchestration, and six
fresh-container replay proofs. It also verifies Gold after memo links settle.
Each action saves logs and timestamps. It fails immediately if any action fails.
`pipelines/output/my_fresh_demo/readiness_run/run.json` shows progress.
`pipelines/output/my_fresh_demo/readiness.json` exists only after final assertions.

Silver verification explicitly corrects synthetic poison pills using the fixture's
known canonical values. Normal Silver processing quarantines those rows and does
not invent corrections. Memo extraction uses a deterministic UUID pattern and
financial validation; no LLM or embeddings are running. FX verification uses
dated, explicitly labeled synthetic quotes and an independent Decimal oracle.

All new fixture payments must match after validated memo links. Unrelated ledger
history remains as open items. A dataset reading shared CDC topics legitimately
contains historical invoice state without corresponding gateway/bank fixtures.
The report records that count; it never deletes those rows to make results tidy.

Run only one producer/readiness/recovery operation at a time. The orchestrator
locks its dataset. Direct verification commands must also be serialized by the
operator. A failed readiness run preserves tables, checkpoints, quarantine, and
logs. Diagnose and rerun the failed layer explicitly; do not delete state to hide
a failure. The wrapper refuses to reuse an existing evidence directory.

## Verify code and preserve data

```powershell
powershell -ExecutionPolicy Bypass -File scripts/ci_checks.ps1 -Profile full -Python .venv-ci/Scripts/python.exe
```

CI uses separate lakehouse/cache volumes and mounts source files read-only.
It does not clear the demo dataset or launch PostgreSQL/Kafka. See
[CI details](../pipelines/CI.md) and [recovery instructions](RECOVERY.md).
Completed reports are local ignored artifacts; the repository contains the code
needed to reproduce them. Keep backups separately before cleaning Docker storage.

Test retention loss separately after normal readiness finishes:

```powershell
.venv/Scripts/python.exe -m operations.source_retention
```

This creates unique disposable Kafka topics and a separate dataset. It checkpoints
one row, publishes 100 more, then deletes unread records **only from its newly
created gateway probe topic**. Bronze must fail explicitly on lost offsets and
leave committed gateway Delta files unchanged. Existing business topics are not
modified. The expected failed Spark log and `retention_verification.json` remain
under the printed `retention_*` output directory. Do not change `failOnDataLoss`
to false or reset the failed checkpoint to make this negative test pass.

## What “ready” covers

Local correctness, quality gates, exact money, exception auditing, incremental
state, repeatable restarts, measured synthetic skew, and the documented recovery
drills are demonstrable. The project remains one host with one Kafka broker.
Synthetic compute measurements do not certify end-to-end high-volume capacity.

Airflow, Soda, LLM/embedding extraction, managed Databricks features, actual market
FX/provider policy, cross-currency settlement, multi-host resilience, and GitHub
publication/runner activation are separate work. Current local alternatives are
named explicitly. See [the full checklist](../PROJECT_STATUS.md).
