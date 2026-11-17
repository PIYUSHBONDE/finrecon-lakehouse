# Local skew benchmark

This is an isolated synthetic compute benchmark. It reads no operational tables
and changes no Bronze/Silver/Gold data, checkpoints, open items, or rate books.
It uses the existing Docker runner: `local[2]`, a 1 GB JVM driver heap, and a
3 GB container memory limit. It adds no service or package.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/benchmark_skew.ps1
```

Defaults: 10,000, 100,000, and 1,000,000 payments; three trials per join strategy.
Each size is a multiple of 100, with exactly 65% of payments assigned to
`MERCHANT_ENT_01`. Remaining merchants use deterministic integer Zipf weights.
Amounts use integer-derived `DECIMAL(18,4)` values across five currencies. A
192-byte, varying SHA-derived payload prevents a count-only query from pruning
most shuffled data. Each invoice ID is unique.

The CLI limits sizes to 1,000..5,000,000 rows and trials to 1..5. The upper bound
is an input safety cap, not a proven capacity rating. For a small check:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/benchmark_skew.ps1 -Rows 1000 -Trials 1
```

Measured findings: [2026-11-13 results](BENCHMARK_RESULTS.md).

## What the workloads prove

Merchant frequency and transaction-key skew are different. A dominant merchant
does not create a dominant invoice ID. Joining gateway and ledger on merchant
alone would change the financial grain and produce incorrect matches. The
benchmark therefore separates three workloads:

1. **Merchant enrichment:** one fact payment joins one unique merchant reference.
   Compare forced sort-merge baseline, AQE skew handling, and a salted shuffle
   join. Automatic broadcast is disabled only in this experiment to expose a
   skewed shuffle. The tiny reference table would normally be a broadcast
   candidate; these timings do not justify forcing a production shuffle.
2. **Merchant aggregation:** compare direct merchant/currency totals with salted
   partial totals followed by exact final totals. Partial sums widen as Spark
   decimals; final values must fit `DECIMAL(18,4)`.
3. **Invoice reconciliation control:** create two balanced journal legs per
   payment, aggregate by invoice ID, and join to the gateway. Check every invoice
   has two legs and equal gateway/debit/credit amounts. This preserves the
   transaction grain used by Gold.

Salting applies only to the hot merchant. A stable hash of invoice ID chooses
one of 16 salts; only the hot dimension row is replicated. It preserves one
enrichment row per payment. Separate row-count probes compare 16 invoice,
merchant, and merchant/salt hash partitions with AQE disabled for reproducibility.
They measure key distribution, not bytes or adaptive task sizes.

Every strategy must match an independent Python integer/Decimal oracle for
currency counts and sums. Merchant aggregate results also match the oracle.
Enrichment strategies must produce an identical payload/reference checksum.
Only bounded summaries reach the driver; full fact rows are never collected.

## Timing and optimizer settings

Inputs materialize with `DISK_ONLY` persistence; preparation and oracle creation
are outside timed cases. A 1,000-row workload warms all three strategies first.
Trial order rotates between strategies. Timings cover the join plus currency
aggregation/checksum action, including its Spark planning/action overhead, not
just one join operator. The report keeps every sample and an integer median.
OS caches, JIT, background services, and one-host scheduling still affect results.
Three trials do not establish statistical confidence or guarantee a speedup.

All experiments use 16 shuffle partitions. AQE uses an 8 MiB skew threshold,
factor 2, and 8 MiB advisory size, with coalescing disabled and forced skew
optimization enabled. The baseline and manually salted cases disable AQE.
Automatic broadcast thresholds are disabled for all three shuffle experiments.
Actual settings and final physical plans are saved. A plan marker records
whether AQE splitting was observed; enabling the option is not treated as proof
that it split a partition. See [Spark 3.5.7 performance tuning](https://spark.apache.org/docs/3.5.7/sql-performance-tuning.html).

## Evidence and limits

Each invocation prints a unique directory under `pipelines/output/benchmarks/`:

- `benchmark.json`: source sizes, exact checks, partition probes, samples,
  medians, actual Spark version/master, memory observations, and task metrics.
- `plans/*.txt`: final executed plans for every measured case.
- `events/*`: uncompressed Spark event logs retained after shutdown.

Task metrics include spill bytes, shuffle bytes, maximum task shuffle-read
records, executor time, and peak task execution memory. Event records are
attributed to the job group that first submitted their stage. Reused stages are
not charged again without new tasks. Metrics cover each whole measured action,
not exclusively the join operator. Peak task execution memory is not JVM RSS.
Heap values are post-action samples, not a measured heap peak. cgroup memory
limits, whole-container peak, and OOM counters are recorded when available.
See [Spark monitoring](https://spark.apache.org/docs/3.5.7/monitoring.html).

A successful benchmark requires completed Spark application logs, real task
evidence for every case, matching financial results, no OOM task failures, and
no increase in the container OOM or OOM-kill counters. A crash/incomplete `RUNNING` report
is not success. No assertion requires AQE or salting to be faster; an honest
negative result is retained.

This does not benchmark Kafka throughput, Delta ingestion/MERGE, checkpoint
recovery, cross-host failures, or the entire eight-stage pipeline at these row
counts. Measurements on one host do not establish multi-machine resilience or
100-million-row capacity. Review the saved results before changing production
tuning. Existing correctness fixtures remain separate evidence.

```powershell
docker compose -f docker/docker-compose.yml --profile compute run --rm --no-deps bronze python3 -m unittest discover -s tests -p test_benchmark_skew.py -v
```

Four focused tests cover evidence attribution, explicit OOM detection, workload
bounds, and real Spark financial equivalence with balanced journal legs.
