# Skew results — 2026-11-13

Measured locally with Spark 3.5.7, `local[2]`, a 1 GB JVM heap, and a 3 GiB
container limit. Inputs were synthetic and isolated from operational data.
The full assumptions and reproducible command are in [BENCHMARKS.md](BENCHMARKS.md).

Raw report: `pipelines/output/benchmarks/20261113T070436Z_ceffe63a/benchmark.json`.
The same directory contains final plans and a 26.9 MB Spark event log.

## Repeated shuffle-join workload

Three trials per strategy; median wall time in milliseconds. Each action
includes the join, currency aggregation, payload checksum, and Spark action
overhead. Input preparation is excluded. This is not a pure operator timer.

| Synthetic payments | Sort-merge baseline | AQE skew handling | Manual salting |
| ---: | ---: | ---: | ---: |
| 10,000 | 1,915 | 2,083 | 3,225 |
| 100,000 | 1,772 | 2,043 | 3,082 |
| 1,000,000 | 8,109 | 5,081 | 8,229 |

For one million payments, the raw trial ranges were 4,807–11,201 ms for baseline,
4,472–6,858 ms for AQE, and 5,909–13,237 ms for salting. These wide ranges and
three samples do not establish a general speedup or statistical significance.
AQE was faster by the observed million-row median. It was slower at the smaller
sizes. Salting did not improve median time in this run.

Automatic broadcast was deliberately disabled to study skewed shuffles. The
merchant dimension was unique and small; broadcasting would normally be a
candidate. This experiment does not recommend forcing that dimension to shuffle.

## Distribution and memory evidence

At one million payments, exactly 650,000 came from `MERCHANT_ENT_01`.

| Fixed 16-partition row-count probe | Largest partition |
| --- | ---: |
| Invoice key | 62,946 |
| Merchant key | 682,686 |
| Merchant plus deterministic hot-key salt | 185,894 |

Merchant concentration did not produce invoice-key concentration. Production
gateway/ledger reconciliation should keep its transaction grain. Salting spread
the merchant-key workload without proving a timing advantage.

AQE splitting appeared in all three final million-row AQE plans. Task metrics
also showed lower maximum shuffle-read record counts and execution-memory peaks:

| Million-row enrichment strategy | Maximum task shuffle-read records | Maximum task execution memory, bytes |
| --- | ---: | ---: |
| Baseline | 682,695 | 235,208,288 |
| AQE | 43,308 | 34,930,624 |
| Salted | 185,906 | 84,213,616 |

These are whole-action task metrics, not exclusively join-operator counters or
JVM RSS. Peak container memory was 2,552,627,200 bytes (about 2.38 GiB), under the
configured 3 GiB limit. No OOM task failures, cgroup OOM events, or OOM kills were
recorded. Disk-spill metrics were zero except for 210 bytes in one baseline
million-row trial. Normal shuffle/cache disk I/O is separate from spill metrics.

## Financial and correctness gates

All 36 measured actions passed their exact financial oracle checks. Enrichment
strategies preserved the same payload/reference checksum. Direct and salted
merchant totals matched independent integer/Decimal calculations. Invoice-key
controls reconciled 10,000, 100,000, and 1,000,000 payments against 20,000,
200,000, and 2,000,000 balanced journal legs respectively.

All four focused tests passed, covering metric attribution, OOM visibility,
input bounds, and real Spark financial equivalence. Final application event
logs completed and every measured action had task evidence.

The evidence supports these synthetic one-host workloads at the measured sizes.
It does not establish eight-stage pipeline throughput, Kafka/Delta scaling,
multi-machine recovery, or 100-million-row capacity. No production salting
change was made from these results.
