# Ten-minute portfolio demo

Use the existing recorded fixture for a short walkthrough. A fresh end-to-end
verification or full CI run takes longer than ten minutes and belongs before
the presentation. Recorded results below are from November 14, 2026; they are
not a live measurement of the reviewer's machine.

## Opening pitch: 30 seconds

> I built a local three-source payment reconciliation lakehouse using Kafka,
> PostgreSQL CDC, Spark, and Delta. The project focuses on exact monetary values,
> replay-safe business effects, ordered history, and persistent exceptions. I
> verified 1,000 correlated payments end to end and tested a coordinated restore.
> The runtime uses existing hardware and no paid cloud or model API.

## Walkthrough: ten minutes

| Time | Show | Explain |
| --- | --- | --- |
| 0:00-1:00 | [Architecture diagram](ARCHITECTURE.md) | Three sources, raw evidence, financial state, and decisions |
| 1:00-2:00 | Recorded summary command below | 1,000 payments, 50 retries, ten poison pills, twenty memo gaps |
| 2:00-3:30 | Follow the illustrative payment in the architecture guide | Bank delay creates an open item; exact evidence closes it |
| 3:30-5:00 | [Silver](../pipelines/SILVER.md) and [CDC](../pipelines/CDC.md) | Quarantine, seven-day idempotency, WAL order, and atomic dimension merges |
| 5:00-6:00 | [Memo validation](../pipelines/EXCEPTIONS.md) | A parser proposes an identity; financial checks authorize the link |
| 6:00-7:00 | [Skew results](../pipelines/BENCHMARK_RESULTS.md) | AQE helped the measured large case; salting was not automatically better |
| 7:00-8:30 | [Recovery proof](../operations/RECOVERY.md) | Restored checkpoints, resumed CDC, and fail-closed retention loss |
| 8:30-10:00 | [Current gaps](../PROJECT_STATUS.md) and questions | Separate implemented behavior, measured scale, and migration work |

From PowerShell in the repository root, show the committed aggregate evidence:

```powershell
powershell -NoProfile -File scripts/show_portfolio.ps1
```

This command needs no Docker or Python and writes no data. It labels its output
as historical evidence. The evidence file contains aggregate results, relative
report references, and report hashes; bulk generated records and backups remain
excluded from the publication snapshot.

On the prepared development machine, check current services separately:

```powershell
.venv-ci/Scripts/python.exe -m operations.status --dataset readiness_20261114
```

A live health failure or proof older than 72 hours must be shown as a failure.
Do not replace the health result with historical evidence and call it live.
Generate a fresh fixture and dataset using [the run guide](../operations/READY.md)
when a fresh demonstration is needed. A new clone has no generated reports or
preloaded Docker data; the recorded-summary command still works there.

## Questions to rehearse

| Question | Precise answer |
| --- | --- |
| Why not deduplicate in Bronze? | Raw duplicate and invalid input remain evidence; Silver applies business identity. |
| Is the seven-day MERGE globally idempotent? | No. It protects the documented event-date window; older rows are quarantined. |
| Why no watermark deduplication yet? | Dropping late first arrivals would lose legitimate payments without a catch-up path. |
| Why use WAL order? | Source causality survives timestamp ties and processing delays. |
| Is the whole lakehouse one transaction? | No. Each table merge is atomic; serialized publication and replay handle partial progress. |
| Why are 311 open items still present? | Shared CDC includes historical invoices outside this 1,000-payment gateway/bank cohort. |
| Is the parser an LLM? | No. It extracts complete UUID tokens deterministically; ambiguous/unsupported memos remain open. |
| Did salting make the pipeline faster? | It spread the synthetic hot-key workload, but its observed median was slower than AQE. |
| Does one million mean pipeline capacity? | No. That is isolated synthetic compute; the end-to-end correctness fixture is 1,000 payments. |
| Does FX perform cross-currency settlement? | No. It supplies reporting valuation with an explicit book; demo quotes are synthetic. |
| Why did the first coordinated restore check fail? | It compared live slot acknowledgements with durable positions; the corrected verifier compares restarted durable state. |
| What remains before managed deployment? | Storage/checkpoint mapping, runtime configuration, identity/networking, platform integration, and observed deployment tests. |

## Resume/project description

Use these as project statements, not production-employment claims:

- Built a local Kafka/PostgreSQL CDC/Spark/Delta reconciliation lakehouse;
  verified 1,000 payments with exact `DECIMAL(18,4)` matching and 35 quality gates.
- Implemented ordered SCD-2 history, replay-safe ledger handling, quarantine,
  durable open items, and financially validated memo-reference overlays.
- Compared AQE and salting through one million synthetic payments and two
  million journal legs, retaining task metrics and exact financial oracles.
- Verified coordinated physical restore through resumed CDC and Bronze,
  plus fail-closed behavior after deliberate loss of disposable Kafka records.

Do not claim a Databricks deployment, LLM implementation, production adoption,
RPO/RTO guarantee, multi-host failover, market FX authority, or salary outcome.
