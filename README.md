# FinRecon Lakehouse

For reviewers: [architecture and design decisions](docs/ARCHITECTURE.md),
[ten-minute demo and interview questions](docs/PORTFOLIO.md), and
[recorded validation evidence](docs/evidence/local_validation.json).
See [GitHub publication preparation](docs/PUBLISHING.md) for the reviewed handover.

For the complete local demo, live status command, evidence, and recovery workflow,
start with [the local run guide](operations/READY.md).

A payment gateway, operational ledger, and bank settlement reconciliation
portfolio project. **Cloud budget: INR 0.** The runnable implementation uses
local Docker, open-source Spark, and Delta Lake. It requires no paid workspace,
trial credits, cloud subscription, or hosted LLM API.

## Budget-adjusted architecture

| Original platform component | Local implementation |
| --- | --- |
| Databricks Spark compute | Apache Spark in a bounded Linux Docker runner |
| Delta Lake tables | Delta tables in a persistent local Docker volume |
| Auto Loader bank ingestion | Spark Structured Streaming CSV source |
| Cloud object storage | Immutable local landing files and Docker volumes |
| Managed workflow deployment | Local CLI/Compose orchestrator; managed scheduling is future work |
| Paid AI extraction | Local deterministic memo extraction; model integration remains future work |

This preserves the financial and data-engineering goals while changing the
hosting platform. Databricks Auto Loader, Unity Catalog integration, managed
jobs, and cloud networking are not validated by local tests. Free Edition is an
optional showcase, not a dependency of the project.

## Current components

- PostgreSQL logical WAL, Kafka in KRaft mode, and Debezium Connect.
- Correlated gateway/ledger/bank generation with merchant skew, late duplicate
  retries, poison pills, messy memos, and replay-safe balanced ledger postings.
- Local append-only Bronze ingestion with checkpointed `AvailableNow` streams.
- Gateway/bank Silver contracts, quarantine, bounded financial deduplication,
  and explicit, revalidated correction replay.
- CDC normalization, atomic account/invoice SCD-2, and WAL-ordered journal state.
- Incremental Gold three-way reconciliation with durable unmatched items.
- One-command local orchestration, bounded retries, dataset locking, and quality gates.
- Local memo exception extraction, exact financial validation, and audited reference overlays.
- Dated INR FX reporting with immutable rate books, decimal rounding, and a broadcast join.
- Isolated skew benchmarks with exact totals, saved plans, and Spark task/memory evidence.
- Local CI checks, an explicit test inventory, and isolated compute regression suites.
- Source, ledger, Bronze, and Silver verification commands with inspectable reports.

LLM/embedding extraction, sustained end-to-end load testing, multi-host resilience,
and managed deployment remain outside the verified local scope.

## Start and inspect

```powershell
docker compose -f docker/docker-compose.yml up -d
```

PostgreSQL is on `localhost:15432`, Kafka on `localhost:9092`, and Connect on
`localhost:8083`. Register the connector as described in the [CDC guide](docker/debezium/README.md).

Generate and verify a complete 100-payment source fixture:

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r data_generator/requirements.txt
.venv/Scripts/python.exe -m data_generator.verify_sources
```

Then verify Bronze against the latest successful source fixture:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify_bronze.ps1
```

The script builds a local Spark image and checks ingestion in two fresh
containers with persistent checkpoints. It prints and saves the result under
`pipelines/output/portfolio_demo/verification.json`. A different fixture needs
a different `-Dataset` name; source manifests prevent unsafe checkpoint reuse.

See [source generator details](data_generator/README.md) and [Bronze details](pipelines/README.md).

Verify the gateway/bank Silver layer and its explicit demo replay:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify_silver.ps1
```

See [Silver contracts, quarantine, and replay](pipelines/SILVER.md) for normal
ingestion, manual correction semantics, seven-day scope, and verification evidence.

Verify CDC normalization, SCD-2 history, and current journal balances:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify_cdc.ps1
```

See [CDC ordering, atomic history, and recovery](pipelines/CDC.md) for delete,
snapshot, replay, and source-order semantics.

Verify Gold three-way reconciliation and persistent open items:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify_gold.ps1
```

See [Gold matching policy, incremental state, and recovery](pipelines/GOLD.md).

Run all stages and both quality gates with [the local operations guide](pipelines/OPERATIONS.md).
Each invocation saves stage logs and a structured run report. Airflow scheduling
and Soda integration remain future integrations.

See [memo exception extraction and replay](pipelines/EXCEPTIONS.md). The local
deterministic baseline can resolve the two fixture memo gaps after validation.

See [FX reporting and synthetic demo](pipelines/FX.md) for the optional eighth
pipeline stage. Demo quotes are synthetic, not market data.

Run [local skew experiments](pipelines/BENCHMARKS.md) separately from the pipeline.
They compare shuffle joins, AQE, salting, and invoice-key reconciliation controls.

Run [local CI](pipelines/CI.md) for repeatable lint, contract, unit, and isolated
Spark checks. The GitHub workflow is prepared for a dedicated local runner;
it has not been activated on GitHub. See the [completion checklist](PROJECT_STATUS.md)
for implemented behavior and remaining work.

## Honest evidence boundaries

Small fixtures demonstrate correctness and recovery, not 100-million-row scale.
The local runner uses one host. Future multi-worker containers can demonstrate
executor behavior, but do not prove resilience across independent machines.
Raw Bronze retains gateway retries and invalid values. Gateway/bank Silver
applies contracts and business idempotency. CDC/SCD-2 protects operational state
against stale updates. Gold applies exact three-way checks and carries open items
across runs. Isolated synthetic skew measurements are available; end-to-end
large-scale throughput and multi-host resilience remain unverified.
