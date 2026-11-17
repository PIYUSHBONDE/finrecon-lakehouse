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

## GitHub activation is separate

`.github/workflows/ci.yml` is prepared but has not been run on GitHub. This
workspace currently has no Git repository metadata or remote. No Actions runner
was installed or registered as part of this work. Nothing was published during
local validation.

The workflow selects `[self-hosted, windows, x64, finrecon]`, runs fast checks on
trusted `main` pushes, and offers manual `fast`/`full` dispatch. It has no pull
request trigger. It uses a read-only token, disables persisted checkout
credentials, and pins the official checkout action to a verified full commit.
There are no deployment or upload steps.

Before activation, publish the repository deliberately and register a dedicated
Windows x64 runner with label `finrecon`. Use a current runner compatible with
the pinned checkout action, Python 3.11, and access to the Docker Linux engine.
Use a separate runner checkout/work directory, not this active development
workspace. Review code before running it on your machine. The workflow has no
fallback to a hosted runner; without a matching online runner it stays queued.

GitHub documents [runner label matching](https://docs.github.com/en/actions/how-tos/write-workflows/choose-where-workflows-run/choose-the-runner-for-a-job)
and [self-hosted runner operation](https://docs.github.com/en/actions/concepts/runners/self-hosted-runners).
The chosen configuration uses existing hardware and avoids hosted compute
usage; ongoing runner maintenance remains the owner's responsibility.

## Verified local result

On 2026-11-14, final checks completed with pinned local tools: all 45 Windows
fast tests and 19 compute tests passed across nine fresh worker processes, with
no selected tests skipped. Runtime inspection verified read-only source files,
isolated warehouse/Ivy volumes, two CPUs, and the 3 GiB container limit.
The missing-Spark preflight also correctly failed instead of reporting success.

Combined proof:
`pipelines/output/ci/20261114T052812Z_343c7af3/verification.json`.
The same directory retains the original fast/compute reports, suite results,
logs, and live-container isolation inspection. This is observed local CI;
GitHub registration and execution remain separate pending work.

The final fast profile was rerun after adding the recursive landing-copy guard;
its report is `fast_final/ci.json` in that directory. Compute code was unchanged,
and the completed full run's 19 Spark checks remain the compute proof. All three
actual archived landing bindings also passed the new guard and hash comparison.
