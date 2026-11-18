# FinRecon completion checklist

Status as of 2026-11-18. Measurements below retain their original dates. This distinguishes implemented local behavior from
remaining enterprise/platform work. No paid workspace or cloud subscription is
required for the current implementation.

| Area | Current implementation | Evidence / remaining boundary |
| --- | --- | --- |
| Source infrastructure | PostgreSQL logical WAL, Kafka KRaft with a named data volume, Debezium | [CDC setup](docker/debezium/README.md); migration preserved topic IDs, offsets, and broker identity |
| Chaos and correlation | 65% institutional merchant; retries, poison pills, memos, balanced ledger | [Generator](data_generator/README.md); live sources and virtual-clock retry proofs |
| Bronze | Append-only Kafka/CSV, raw strings, independent persistent checkpoints | [Bronze](pipelines/README.md); fresh-container restart proof |
| Silver contracts | YAML rules, quarantine, explicit correction replay | [Silver](pipelines/SILVER.md); no automatic financial-value repair |
| Financial idempotency | Seven-day bounded, partition-pruned Delta merge | Hot watermark deduplication is not implemented; behavior outside the rolling window is documented |
| CDC/SCD-2 | WAL ordering, atomic dimension merges, journal state, logical deletes | [CDC](pipelines/CDC.md); unrepresented stale history is quarantined |
| Gold reconciliation | Exact native-currency three-way checks and durable open items | [Gold](pipelines/GOLD.md); 1,000 matched fresh fixture payments after validated memo links |
| Memo exceptions | Local deterministic extraction, exact validation, immutable audit, stale-link invalidation | [Exceptions](pipelines/EXCEPTIONS.md); LLM/embeddings remain unimplemented |
| FX | Explicit dated books, INR reporting, decimal rounding/overflow gates, broadcast join | [FX](pipelines/FX.md); synthetic demo quotes, no cross-currency settlement matching |
| Orchestration and quality | Seven stages plus optional FX, retries, locks, 35 native quality checks | [Operations](pipelines/OPERATIONS.md); Airflow and Soda integrations remain future work |
| Skew/volume evidence | Synthetic compute through one million payments and two million journal legs | [Measured results](pipelines/BENCHMARK_RESULTS.md); not end-to-end pipeline throughput |
| CI | Pinned local tools, explicit test inventory, strict no-skip gates, isolated Spark workers | [CI](pipelines/CI.md); GitHub full run passed 64 tests; two later fast scanner regressions passed separately, making current selection 66 |
| Databricks | Portable Spark/Delta logic and documented local substitutes | Auto Loader, Unity Catalog, managed jobs, networking, and Asset Bundles are unvalidated |
| Recovery | Coordinated physical PostgreSQL/Kafka/Delta snapshots, sparse archives, retained rollback data | [Recovery](operations/RECOVERY.md); isolated restore resumed CDC and ingested exactly six new probe events |
| Retention failure | Disposable-topic record-loss injection, explicit Spark failure, committed Delta preservation | [Run guide](operations/READY.md); lost unread offsets failed closed without changing committed data |
| Local readiness | Six layer proofs, eight-stage run, six fresh-container replays | [Run guide](operations/READY.md); 1,000 payments, 35 quality checks, zero replay changes |

## Prioritized remaining work

1. **Broader scale and failure evidence:** measure the entire source-to-Gold path,
   delayed/retried events at higher volumes, and recovery
   across independent worker/host failures. Existing skew results cover isolated
   synthetic Spark workloads on one host only. The fresh 1,000-payment end-to-end
   run proves correctness and process replay, not sustained high-volume capacity.
2. **Business-model extensions:** define account currency, actual FX/provider
   authority, settlement conversion/fees, and gains/loss postings before adding
   cross-currency reconciliation. Current same-currency checks remain strict.
3. **Optional integrations:** LLM/embedding candidate extraction, Airflow, Soda,
   and a reviewed cloud migration/deployment path. These are not silently claimed
   by the local equivalents. Cloud deployment stays deferred under the zero
   cloud budget.
Repository publication completed on November 17. On November 18 the dedicated
Windows runner was registered and completed GitHub fast and full validation.
The fresh 1,000-payment demonstration and all replay checks also completed.
Run receipts and exact validation scopes are documented in [CI.md](pipelines/CI.md). No historical
commits are invented, and no project license has been assigned automatically.

## Inspectable demo evidence

The latest complete proof is
`pipelines/output/readiness_20261118/readiness.json`: 1,000 payments matched,
50 exact retries retained in Bronze, ten synthetic poison pills explicitly
corrected from fixture truth, and twenty financially validated memo links.
All six layer proofs passed again in fresh containers without changing state.
The eight-stage run passed all 35 quality checks:
`pipelines/output/readiness_20261118/runs/20261118T035544Z_dde62c54/run.json`.
This cohort retains 1,311 unrelated historical open items from shared CDC topics.
The November 14 fixture remains inspectable under `readiness_20261114`; its 311
historical open items predate the additional synthetic fixture now in shared CDC.

Kafka migration/restore proof is
`pipelines/output/recovery/kafka_20261114/recovery.json`.
Coordinated restore proof is
`pipelines/output/recovery/stack_20261114_retry/stack_recovery.json`.
The restored checkpoint read zero old rows, then exactly six new CDC rows from
a cloned INR 1.0000 transaction. Original PostgreSQL never received that invoice.
The first coordinated attempt's failed report remains alongside the passing
retry; the corrected check compares persisted slot positions, not live memory.

The retention-loss proof is
`pipelines/output/retention_9ca6c8e9cd40/retention_verification.json`.
Only dedicated disposable probe records were removed. Spark failed with exit
code 1 as expected, and committed gateway Delta files remained identical.

Final CI proof is
`pipelines/output/ci/20261114T052812Z_343c7af3/verification.json`:
45 Windows fast checks plus 19 compute checks, with zero selected skips.
Runtime inspection verified read-only source, isolated CI volumes, two CPUs, and
the 3 GiB container limit. Three optional source-mutating PostgreSQL unit tests
remain outside CI; the real 1,000-payment source verifier supplies live CDC proof.

The source-only review bundle is
`pipelines/output/release/finrecon-local-20261114.zip`. It contains code, docs,
an aggregate proof summary, and a SHA-256 file manifest. Generated financial
files, database archives, environments, and caches are excluded from the bundle.

Portfolio handover prepared on November 15:
[architecture](docs/ARCHITECTURE.md), [ten-minute demo](docs/PORTFOLIO.md),
[public aggregate evidence](docs/evidence/local_validation.json), and
[publication steps](docs/PUBLISHING.md). The refreshed review bundle is
`pipelines/output/release/finrecon-portfolio-20261115.zip`.
The separate local Git snapshot is
`pipelines/output/portfolio/20261115/repository`; its adjacent `review.json`
records staged contents and artifact hashes. This is preparation, not a GitHub
publication or a newly observed CI run.

### Earlier 100-payment fixture

Local reports under `pipelines/output/portfolio_demo/` cover Bronze, Silver,
CDC, Gold, memo extraction, and FX verification. They record exact amounts,
fresh-process restart comparisons, and preserved raw data. The eight-stage demo
report is `runs/20261112T043853Z_39db1599/run.json` under that directory.

The fixture has 100 payments. Its two messy bank memos now have validated links,
so all 100 match. The shared development dataset also retains 111 unmatched
historical test items; these are not additional payments in the 100-payment
fixture and are not silently dropped to make a dashboard look clean.

The local runnable portfolio has completed its current readiness proofs. This is not a claim of
production certification, complete adoption of the original managed stack, or
100-million-row/multi-host resilience.
