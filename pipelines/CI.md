# Local CI and prepared GitHub Actions workflow

CI uses existing local hardware. No cloud deployment, GitHub-hosted runner,
artifact upload, or paid service is configured. Local execution works before a
GitHub repository or Actions runner is registered.

## Run the same checks locally

Use Python 3.11 and the pinned tools from `requirements-dev.txt`:

```powershell
python -m venv .venv-ci
.venv-ci/Scripts/python.exe -m pip install -r requirements-dev.txt
powershell -ExecutionPolicy Bypass -File scripts/ci_checks.ps1 -Profile fast -Python .venv-ci/Scripts/python.exe
```

`fast` runs Ruff lint/format checks, compilation, YAML contracts, CI/workflow
policy checks, test-inventory coverage, and 45 selected Windows unit tests. It
requires no running database, broker, or Spark. The Linux-only lock test is
covered by the compute profile; it is not silently skipped on Windows.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/ci_checks.ps1 -Profile full -Python .venv-ci/Scripts/python.exe
```

`full` runs fast checks, builds the existing Spark image, then runs 19 compute
tests in the isolated CI Compose project. Nine worker suites cover Bronze,
Silver, CDC/SCD-2, Gold, exceptions, FX, quality gates, the small skew correctness
case, and the Linux dataset lock. Each suite starts a fresh Python/JVM process.
All suites run sequentially to stay within the local resource budget. Full CI
can take tens of minutes; large performance benchmarks are separate commands.

Three opt-in PostgreSQL tests are deliberately outside CI. They write and clean
up source rows and require `FINRECON_TEST_POSTGRES_DSN`. Broker/CDC end-to-end
verifiers are separate live-service proofs. This CI run does not replace them.

The inventory currently accounts for 67 tests: 45 fast, 19 compute, and three
opt-in PostgreSQL. A new test class/method must belong to exactly one configured
group, preventing accidental omission when tests are added. An empty suite,
missing dependency, unittest error, failure, or skipped selected test fails CI.
Expected import skips never substitute for real Spark execution.

## Isolation and reports

CI uses `docker/docker-compose.ci.yml` and project name `finrecon-ci`:

- Repository files are mounted read-only at `/workspace`.
- Only the unique CI report directory is a writable host bind at `/reports`.
- Warehouse and Maven caches use `finrecon-ci_ci_lakehouse` and
  `finrecon-ci_ci_ivy`; production `bronze_data`, `pgdata`, and `spark_ivy`
  volumes are not mounted.
- There are no PostgreSQL, Kafka, or Debezium services in the CI Compose file.
- The compute container has two CPUs, a 1 GB JVM heap, and a 3 GiB memory limit.

The tests use temporary input/Delta directories. CI does not generate payments
in the operational database, consume real Kafka topics, or replay production
checkpoints. Run full CI sequentially on this PC; GitHub's repository concurrency
setting serializes workflow jobs, while direct local invocations are independent.

Each command prints `pipelines/output/ci/<UTC-time>_<id>/`. Inspect:

- `fast/ci.json`, static-check logs, and `fast/unit/tests.json`/`tests.log`.
- `spark/ci.json` for planned suites, started steps, exit codes, and test totals.
- `spark/suite_XX/tests.json` and `tests.log` for each worker's actual results.
- `spark/spark_XX.log` for Spark/JVM/dependency output and tracebacks.

Only a complete `SUCCEEDED` report certifies its profile. A hard interruption
may leave `RUNNING`; that is not a successful run. Reports are ignored by Git.
No automatic cleanup or recursive deletion of previous reports/volumes occurs.

First use may download free Maven artifacts into the isolated cache. Java must
reach Maven Central. A failed download can produce `JAVA_GATEWAY_EXITED` before
any test executes; inspect the earlier Ivy error rather than treating that
message as a financial-code failure. The 2026-11-13 initial run correctly failed
with zero compute tests when Maven connections were refused. Java and Python
access were verified on 2026-11-14 before rerunning. No production cache was
mounted or copied to bypass isolation, and no financial code was changed.

## GitHub runner operation

The repository was published on November 17, 2026. On November 18, the dedicated
Windows x64 runner `finrecon-piyush-windows` was registered for
`PIYUSHBONDE/finrecon-lakehouse` with custom label `finrecon`. The official runner
release was downloaded and verified against its published SHA-256 digest.

The workflow selects `[self-hosted, windows, x64, finrecon]`, runs fast checks on
trusted `main` pushes, and offers manual `fast`/`full` dispatch. It has no pull
request trigger. It uses a read-only token, disables persisted checkout
credentials, and pins the official checkout action to a verified full commit.
There are no deployment or upload steps.

The runner is installed under `pipelines/output/actions-runner`, with its own
`_work` checkout separate from this active workspace. Installation, credentials,
launcher logs, and generated reports remain ignored by Git. Registration tokens
were handled in memory; runner credentials are restricted to the Windows owner,
SYSTEM, and Administrators. No Windows service was installed.

Start or restart it from the development workspace as the Windows owner:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/start_ci_runner.ps1
```

The helper checks Python 3.11 and Docker availability, detects an already-running
listener, and launches the official runner wrapper in a hidden window with logs.
It does not modify global Git settings or register another runner. After a PC
restart or logout, start the runner again and keep Docker Desktop running.
The helper prints the launcher PID and log path; GitHub's runner/job status is
the authoritative online result. Stop the listener only after active jobs finish.

For a new machine or clone, register a separate repository runner before using
the helper; runner credentials are intentionally absent from the published code.
Use a current runner compatible with the pinned checkout action, Python 3.11,
and access to Docker's Linux engine. Review trusted code before pushing `main`.
There is no hosted-runner fallback; without a matching online runner jobs queue.

GitHub documents [runner label matching](https://docs.github.com/en/actions/how-tos/write-workflows/choose-where-workflows-run/choose-the-runner-for-a-job)
and [self-hosted runner operation](https://docs.github.com/en/actions/concepts/runners/self-hosted-runners).
The chosen configuration uses existing hardware and avoids hosted compute
usage; ongoing runner maintenance remains the owner's responsibility.
GitHub's [current billing documentation](https://docs.github.com/en/billing/concepts/product-billing/github-actions)
lists self-hosted runners as free compute usage. This workflow uses the existing
PC and local Docker storage; it configures no paid runner or artifact upload.

The initial push workflow completed successfully on November 18 on this runner:
[GitHub fast validation](https://github.com/PIYUSHBONDE/finrecon-lakehouse/actions/runs/44136367643).
Its actual runner report recorded 45 tests, no failures/errors, and no skips.
This is observed GitHub execution of the fast profile; the 19-test compute profile
is a separate manual `full` dispatch. Do not run a full dispatch alongside a
direct readiness/Spark verification on this memory-limited host.

## Verified local result

On 2026-11-14, final checks completed with pinned local tools: all 45 Windows
fast tests and 19 compute tests passed across nine fresh worker processes, with
no selected tests skipped. Runtime inspection verified read-only source files,
isolated warehouse/Ivy volumes, two CPUs, and the 3 GiB container limit.
The missing-Spark preflight also correctly failed instead of reporting success.

Combined proof:
`pipelines/output/ci/20261114T052812Z_343c7af3/verification.json`.
The same directory retains the original fast/compute reports, suite results,
logs, and live-container isolation inspection. These November 14 results are
historical local CI; the GitHub execution above is recorded separately.

The final fast profile was rerun after adding the recursive landing-copy guard;
its report is `fast_final/ci.json` in that directory. Compute code was unchanged,
and the completed full run's 19 Spark checks remain the compute proof. All three
actual archived landing bindings also passed the new guard and hash comparison.
