# Local orchestration and quality gates

Use the existing Spark image and Docker source services. The orchestrator runs
one Spark process at a time, preserving the 1 GB heap and 3 GB container limit.
It adds no package or always-on service.

```powershell
docker compose -f docker/docker-compose.yml up -d
powershell -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1 -Dataset local -GatewayTopics gateway_events -BankDirectory /workspace/data_generator/output/live/bank_settlements
```

Choose the actual gateway topic and existing bank landing directory. A dataset
is bound to its source configuration. Reuse its checkpoints for subsequent runs;
use a new dataset when changing sources. The wrapper does not generate payments
or apply correction files. Build the Spark image once using the Bronze guide.

For the verified portfolio fixture:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_pipeline.ps1 -Dataset portfolio_demo -GatewayTopics gateway_correlated_54548d16 -BankDirectory /workspace/data_generator/output/correlated_b5a71edf/live/bank_settlements
```

The sequence is Bronze, gateway/bank Silver, CDC/SCD-2, Silver quality, Gold,
memo exceptions with Gold refresh, then Gold quality. A failed stage stops downstream work. The default is two
attempts per stage, each in a fresh Spark process. An explicit quality rejection
(exit 2) stops immediately. Other nonzero exits get one retry. Successful earlier
stages are not rolled back. Restart reruns the sequence using existing ingestion
checkpoints, idempotent merges, and Gold cursors.

The dataset lock uses Linux `flock` on the persistent lakehouse volume. A second
orchestrator for the same dataset fails immediately; the OS releases the lock
when the owning process exits. Run the wrapper through Docker. Direct individual
stage CLIs do not acquire this lock and must not run concurrently. Pause source
writers while taking a reproducible fixture proof. The runner does not provide
a cross-table snapshot or distributed scheduler.

Supply `-FxRatesFile` to append the optional eighth stage for [FX reporting](FX.md).
Its CSV path must exist inside the runner. There is no automatic rate selection;
demo books contain explicitly synthetic quotes.

Verified the optional eight-stage wrapper run on 2026-11-12 with the synthetic
FX book: `pipelines/output/portfolio_demo/runs/20261112T043853Z_39db1599/run.json`.

## Reports and quality policy

Every invocation prints a unique report path under
`pipelines/output/<dataset>/runs/<UTC-time>_<id>/`. It records stage status,
attempt exit codes, timestamps, and a separate log per attempt. Run metadata is
atomically replaced after each transition. Quality checks also save structured
JSON reports in that directory. Only `SUCCEEDED` means all stages and both gates
passed. Historical successful runs do not certify a later failed run.

Rules live in `contracts/quality.yaml`; additional schema, freshness, and SCD
checks use native Spark expressions. Gates enforce:

- Financial columns remain `DECIMAL(18,4)`; gateway/bank amounts and currencies
  remain valid. Gateway, bank, and journal business keys are unique.
- Each dimension key has one current marker, unique version IDs, and contiguous,
  non-overlapping version intervals.
- Each Bronze source has an ingestion timestamp within the configured age and
  no more than five minutes into the future.
- Matched decisions have one gateway and bank, balanced exact amounts, journal
  legs, and no failed matching reasons. A settlement cannot match twice.
- Open items exactly match current open decisions by identity and content hash.

Pending quarantine and unresolved business exceptions are reported as metrics.
They do not automatically fail a job: missing bank data is expected in this
scenario. Historical fixture journals are not all valid payment examples; the
balance gate applies to decisions claiming `MATCHED`. No quality gate silently
repairs an amount, links a memo, or changes a business decision.

Freshness defaults to 72 hours of **Bronze ingestion age**, measured against the
UTC execution clock. This is a local demonstration threshold, not a promised
business SLA or Kafka consumer-lag measurement. An idle source can fail freshness.
Use `-MaxAgeHours` to set an explicit policy. `-AsOfDate yyyy-MM-dd` controls the
Silver seven-day business window; it does not change the quality gate's clock.

These baseline audit gates scan current Silver/Gold tables and dimension history.
Their cost grows with retained data; scale tuning for audit gates remains future
work. Gold's normal reconciliation still uses incremental evidence and open items.

A gate can reject after a stage has committed data. Consumers must check the
whole run report before using that run's output. This is fail-closed downstream
execution, not an atomic publication mechanism across all Delta tables. A hard
container kill may leave a `RUNNING` report; it is never treated as success.

## Verification and remaining integrations

```powershell
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m unittest discover -s tests -p test_orchestrate.py -v
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m unittest discover -s tests -p test_quality.py -v
```

Tests exercise successful retry, retry exhaustion, terminal quality failure,
downstream skipping, dataset lock exclusion/release, valid open exceptions,
corrupted matched amounts, and missing open-item state. All writes in the
quality test use temporary Delta tables.

Before adding the memo stage, verified locally on 2026-11-12 with `portfolio_demo`: all six stages succeeded,
including 19 Silver and 16 Gold quality checks. Four runner tests passed in
Linux, including locking, and the real Delta corruption test passed. The report
is `pipelines/output/portfolio_demo/runs/20261112T033558Z_561a423f/run.json`.

After adding memo extraction, all seven stages succeeded on 2026-11-12 with
100 matched fixture payments and all 35 quality checks passing. The integrated
report is `pipelines/output/portfolio_demo/runs/20261112T041842Z_a7ee3dd8/run.json`.

On 2026-11-14, the fresh `readiness_20261114` cohort completed all eight stages,
including synthetic FX reporting, with 1,000 matched payments and all 35 quality
checks passing. Report:
`pipelines/output/readiness_20261114/runs/20261114T050443Z_e4511c99/run.json`.
All six layer verifiers also passed in fresh containers without changing state.
See [local readiness and recovery](../operations/READY.md).

Airflow scheduling, Soda integration, GitHub runner activation, and alert delivery remain
future integrations. This slice supplies an on-demand local runner and native
Spark quality gates. It does not require paid resources or external accounts.
