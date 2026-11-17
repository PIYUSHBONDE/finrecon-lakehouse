"""Bounded synthetic skew experiments with financial oracles and Spark event evidence."""

import argparse
from bisect import bisect_right
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from time import perf_counter_ns
from uuid import uuid4

from .quality import atomic_json

HOT = "MERCHANT_ENT_01"
CURRENCIES = ("USD", "EUR", "GBP", "JPY", "INR")
PARTITIONS = 16
SALTS = 16
MODES = ("shuffle_baseline", "shuffle_aqe", "shuffle_salted")
WEIGHTS = [10000 // rank for rank in range(1, 101)]
CUMULATIVE = []
for _weight in WEIGHTS:
    CUMULATIVE.append(_weight + (CUMULATIVE[-1] if CUMULATIVE else 0))


def configure(spark, adaptive):
    settings = {
        "spark.sql.shuffle.partitions": str(PARTITIONS),
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.enabled": str(adaptive).lower(),
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.forceOptimizeSkewedJoin": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "2",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": str(
            8 * 1024 * 1024
        ),
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": str(8 * 1024 * 1024),
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
    }
    for key, value in settings.items():
        spark.conf.set(key, value)
    return settings


def facts(spark, rows):
    from pyspark.sql import functions as F

    ticket = f"pmod(id * 104729 + 17, {CUMULATIVE[-1]})"
    cases = " ".join(
        f"WHEN {ticket} < {limit} THEN 'MERCHANT_{index:05d}'"
        for index, limit in enumerate(CUMULATIVE, 1)
    )
    merchant = F.when(F.pmod("id", F.lit(100)) < 65, F.lit(HOT)).otherwise(
        F.expr("CASE " + cases + " END")
    )
    return (
        spark.range(rows, numPartitions=PARTITIONS)
        .withColumn("invoice_id", F.concat(F.lit("INV-"), F.col("id")))
        .withColumn("merchant_id", merchant)
        .withColumn(
            "currency",
            F.element_at(
                F.array(*[F.lit(c) for c in CURRENCIES]),
                (F.pmod("id", F.lit(5)) + 1).cast("int"),
            ),
        )
        .withColumn("_units", F.pmod(F.col("id") * 7919 + 17, F.lit(999999)) + 1)
        .withColumn(
            "amount_dec",
            F.expr(
                "CAST(CAST(_units AS DECIMAL(18,4)) * CAST('0.0001' AS DECIMAL(18,4)) AS DECIMAL(18,4))"
            ),
        )
        .withColumn(
            "payload",
            F.concat(
                *[
                    F.sha2((F.col("id") * prime + 17).cast("string"), 256)
                    for prime in (7919, 104729, 15485863)
                ]
            ),
        )
        .select("id", "invoice_id", "merchant_id", "currency", "amount_dec", "payload")
    )


def dimension(spark):
    from pyspark.sql import functions as F

    cold = spark.range(1, 101).select(
        F.format_string("MERCHANT_%05d", F.col("id")).alias("merchant_id"),
        F.concat(F.lit("CATEGORY-"), F.pmod("id", F.lit(7))).alias("reference"),
    )
    return cold.unionByName(
        spark.createDataFrame(
            [(HOT, "INSTITUTIONAL")], "merchant_id STRING, reference STRING"
        )
    )


def salt(frame):
    from pyspark.sql import functions as F

    return frame.withColumn(
        "salt",
        F.when(
            F.col("merchant_id") == HOT, F.pmod(F.xxhash64("invoice_id"), F.lit(SALTS))
        ).otherwise(F.lit(0)),
    )


def enrichment(spark, frame, mode):
    from pyspark.sql import functions as F

    dim = dimension(spark)
    if mode == "shuffle_salted":
        dim = (
            dim.where(F.col("merchant_id") != HOT)
            .withColumn("salt", F.lit(0))
            .unionByName(
                dim.where(F.col("merchant_id") == HOT).crossJoin(
                    spark.range(SALTS).select(F.col("id").alias("salt"))
                )
            )
        )
        joined = (
            salt(frame).hint("merge").join(dim.hint("merge"), ["merchant_id", "salt"])
        )
    else:
        joined = frame.hint("merge").join(dim.hint("merge"), "merchant_id")
    # Depend on both payload and dimension attributes so the payload must cross
    # the join. A count-only benchmark could optimize most of that work away.
    return joined.withColumn(
        "_checksum",
        F.pmod(F.xxhash64("invoice_id", "payload", "reference"), F.lit(1000000007)),
    )


def currency_summary(frame):
    from pyspark.sql import functions as F

    return frame.groupBy("currency").agg(
        F.count("*").alias("rows"),
        F.expr("try_cast(sum(amount_dec) AS DECIMAL(18,4))").alias("amount_dec"),
        F.sum("_checksum").alias("checksum"),
    )


def merchant_summary(frame, salted):
    from pyspark.sql import functions as F

    if salted:
        partial = (
            salt(frame)
            .groupBy("merchant_id", "currency", "salt")
            .agg(
                F.sum("amount_dec").alias("partial_dec"),
                F.count("*").alias("partial_rows"),
            )
        )
        return partial.groupBy("merchant_id", "currency").agg(
            F.expr("try_cast(sum(partial_dec) AS DECIMAL(18,4))").alias("amount_dec"),
            F.sum("partial_rows").alias("rows"),
        )
    return frame.groupBy("merchant_id", "currency").agg(
        F.expr("try_cast(sum(amount_dec) AS DECIMAL(18,4))").alias("amount_dec"),
        F.count("*").alias("rows"),
    )


def oracle(rows):
    totals, merchants = {}, {}
    for identity in range(rows):
        currency = CURRENCIES[identity % 5]
        merchant = (
            HOT
            if identity % 100 < 65
            else f"MERCHANT_{bisect_right(CUMULATIVE, (identity * 104729 + 17) % CUMULATIVE[-1]) + 1:05d}"
        )
        units = (identity * 7919 + 17) % 999999 + 1
        for mapping, key in ((totals, currency), (merchants, (merchant, currency))):
            count, amount = mapping.get(key, (0, 0))
            mapping[key] = (count + 1, amount + units)

    def result(mapping, fields):
        return [
            {
                **dict(zip(fields, key if isinstance(key, tuple) else (key,))),
                "rows": value[0],
                "amount_dec": format(Decimal(value[1]).scaleb(-4), ".4f"),
            }
            for key, value in sorted(mapping.items())
        ]

    return result(totals, ("currency",)), result(merchants, ("merchant_id", "currency"))


def canonical(frame, fields):
    result = []
    for row in frame.collect():
        value = row.asDict()
        if value.get("amount_dec") is None:
            raise AssertionError("Aggregate exceeded DECIMAL(18,4)")
        value["amount_dec"] = format(value["amount_dec"], ".4f")
        result.append(value)
    return sorted(result, key=lambda row: tuple(row[field] for field in fields))


def checksum(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def partition_probe(frame, columns):
    from pyspark.sql import functions as F

    measured = (
        frame.repartition(PARTITIONS, *columns)
        .select(F.spark_partition_id().alias("partition"))
        .groupBy("partition")
        .count()
        .collect()
    )
    counts = {row.partition: row["count"] for row in measured}
    values = [counts.get(index, 0) for index in range(PARTITIONS)]
    return {
        "rows_per_partition": values,
        "max_partition_rows": max(values),
        "total_rows": sum(values),
    }


def invoice_control(frame):
    from pyspark.sql import functions as F

    zero = F.lit("0.0000").cast("decimal(18,4)")
    legs = frame.select("invoice_id", "amount_dec").withColumn(
        "leg", F.explode(F.array(F.lit("DEBIT"), F.lit("CREDIT")))
    )
    totals = legs.groupBy("invoice_id").agg(
        F.sum(
            F.when(F.col("leg") == "DEBIT", F.col("amount_dec")).otherwise(zero)
        ).alias("debit_dec"),
        F.sum(
            F.when(F.col("leg") == "CREDIT", F.col("amount_dec")).otherwise(zero)
        ).alias("credit_dec"),
        F.count("*").alias("legs"),
    )
    joined = frame.select("invoice_id", "amount_dec", "currency").join(
        totals, "invoice_id"
    )
    return joined.groupBy("currency").agg(
        F.count("*").alias("rows"),
        F.expr("try_cast(sum(amount_dec) AS DECIMAL(18,4))").alias("amount_dec"),
        F.sum(
            F.when(
                (F.col("debit_dec") == F.col("credit_dec"))
                & (F.col("debit_dec") == F.col("amount_dec"))
                & (F.col("legs") == 2),
                0,
            ).otherwise(1)
        ).alias("bad_rows"),
    )


def event_metrics(directory):
    groups, stage_groups = {}, {}
    ended, oom_failures = False, 0
    for path in sorted(Path(directory).iterdir()):
        if not path.is_file() or path.name.startswith("."):
            continue
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                event = json.loads(line)
                if event["Event"] == "SparkListenerApplicationEnd":
                    ended = True
                elif event["Event"] == "SparkListenerJobStart":
                    group = event.get("Properties", {}).get(
                        "spark.jobGroup.id", "unattributed"
                    )
                    for stage in event["Stage IDs"]:
                        stage_groups.setdefault(stage, group)
                elif event["Event"] == "SparkListenerTaskEnd":
                    group = stage_groups.get(event["Stage ID"], "unattributed")
                    metric = groups.setdefault(
                        group,
                        {
                            "tasks": 0,
                            "failed_attempts": 0,
                            "memory_spilled_bytes": 0,
                            "disk_spilled_bytes": 0,
                            "peak_task_execution_memory_bytes": 0,
                            "max_task_shuffle_read_records": 0,
                            "shuffle_read_bytes": 0,
                            "shuffle_write_bytes": 0,
                            "executor_run_ms": 0,
                        },
                    )
                    task = event.get("Task Metrics", {})
                    reason = event.get("Task End Reason", {})
                    metric["tasks"] += 1
                    metric["failed_attempts"] += reason.get("Reason") != "Success"
                    text = json.dumps(reason).lower()
                    oom_failures += (
                        "outofmemoryerror" in text or "out of memory" in text
                    )
                    metric["memory_spilled_bytes"] += task.get(
                        "Memory Bytes Spilled", 0
                    )
                    metric["disk_spilled_bytes"] += task.get("Disk Bytes Spilled", 0)
                    metric["peak_task_execution_memory_bytes"] = max(
                        metric["peak_task_execution_memory_bytes"],
                        task.get("Peak Execution Memory", 0),
                    )
                    read = task.get("Shuffle Read Metrics", {})
                    metric["max_task_shuffle_read_records"] = max(
                        metric["max_task_shuffle_read_records"],
                        read.get("Total Records Read", 0),
                    )
                    metric["shuffle_read_bytes"] += read.get(
                        "Local Bytes Read", 0
                    ) + read.get("Remote Bytes Read", 0)
                    metric["shuffle_write_bytes"] += task.get(
                        "Shuffle Write Metrics", {}
                    ).get("Shuffle Bytes Written", 0)
                    metric["executor_run_ms"] += task.get("Executor Run Time", 0)
    return {
        "application_ended": ended,
        "oom_task_failures": oom_failures,
        "groups": groups,
    }


def cgroup_memory():
    base = Path("/sys/fs/cgroup")
    result = {}
    for name in ("memory.max", "memory.peak"):
        path = base / name
        if path.exists():
            value = path.read_text().strip()
            result[name] = int(value) if value.isdigit() else value
    path = base / "memory.events"
    if path.exists():
        result["events"] = {
            key: int(value)
            for key, value in (line.split() for line in path.read_text().splitlines())
        }
    return result


def run_benchmark(output, sizes, trials=3):
    if (
        not sizes
        or len(set(sizes)) != len(sizes)
        or any(not 1000 <= size <= 5000000 or size % 100 for size in sizes)
        or not 1 <= trials <= 5
    ):
        raise ValueError(
            "Sizes must be distinct multiples of 100 in 1000..5000000; trials must be 1..5"
        )
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from .runtime import spark_session

    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    events, plans = output / "events", output / "plans"
    events.mkdir()
    plans.mkdir()
    report = {
        "status": "RUNNING",
        "synthetic_only": True,
        "sizes": sizes,
        "trials": trials,
        "shuffle_partitions": PARTITIONS,
        "salt_buckets": SALTS,
        "payload_bytes": 192,
        "master": "local[2]",
        "cgroup_before": cgroup_memory(),
        "profiles": [],
        "cases": [],
    }
    atomic_json(output / "benchmark.json", report)
    spark = spark_session(
        "FinRecon bounded skew benchmark",
        {
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": events.as_uri(),
            "spark.eventLog.compress": "false",
            "spark.eventLog.rolling.enabled": "false",
        },
    )
    report["spark_version"] = spark.version
    report["master"] = spark.sparkContext.master
    report["fixed_tuning"] = configure(spark, False)
    report["timing_scope"] = (
        "Join plus currency aggregation/checksum action; source preparation and Python assertions excluded"
    )

    def group(name):
        spark.sparkContext.setJobGroup(name, name, interruptOnCancel=True)

    def execute(frame, name, fields):
        group(name)
        start = perf_counter_ns()
        data = canonical(frame, fields)
        elapsed = (perf_counter_ns() - start) // 1000000
        plan = frame._jdf.queryExecution().executedPlan().toString()
        plan_path = plans / (name.replace("/", "_") + ".txt")
        plan_path.write_text(plan, encoding="utf-8")
        heap = spark._jvm.java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()
        record = {
            "group": name,
            "elapsed_ms": elapsed,
            "result_digest": checksum(data),
            "plan": str(plan_path),
            "aqe_skew_split_observed": "AQEShuffleRead skewed" in plan
            or "isSkew=true" in plan
            or "isSkewJoin=true" in plan,
            "driver_heap_after_bytes": heap.getUsed(),
            "driver_heap_max_bytes": heap.getMax(),
        }
        report["cases"].append(record)
        return data, record, plan

    try:
        configure(spark, False)
        group("warmup_prepare")
        warm = facts(spark, 1000).persist(StorageLevel.DISK_ONLY)
        warm.count()
        for mode in MODES:
            configure(spark, mode == "shuffle_aqe")
            group("warmup_" + mode)
            currency_summary(enrichment(spark, warm, mode)).collect()
        warm.unpersist(blocking=True)
        for size in sizes:
            configure(spark, False)
            group(f"n{size}/prepare")
            frame = facts(spark, size).persist(StorageLevel.DISK_ONLY)
            try:
                assert frame.count() == size
                assert (
                    frame.where(F.col("merchant_id") == HOT).count() == size * 65 // 100
                )
                expected, expected_merchants = oracle(size)
                group(f"n{size}/partition_probes")
                probes = {
                    "invoice": partition_probe(frame, ["invoice_id"]),
                    "merchant": partition_probe(frame, ["merchant_id"]),
                    "salted_merchant": partition_probe(
                        salt(frame), ["merchant_id", "salt"]
                    ),
                }
                assert all(value["total_rows"] == size for value in probes.values())
                assert (
                    probes["salted_merchant"]["max_partition_rows"]
                    < probes["merchant"]["max_partition_rows"]
                )
                profile = {
                    "rows": size,
                    "hot_merchant_rows": size * 65 // 100,
                    "partition_probes": probes,
                    "expected_currency_totals": expected,
                }
                report["profiles"].append(profile)
                reference_digest = None
                for trial in range(trials):
                    order = MODES[trial % len(MODES) :] + MODES[: trial % len(MODES)]
                    for mode in order:
                        configure(spark, mode == "shuffle_aqe")
                        result, record, plan = execute(
                            currency_summary(enrichment(spark, frame, mode)),
                            f"n{size}/t{trial}/{mode}",
                            ("currency",),
                        )
                        assert [
                            {
                                key: row[key]
                                for key in ("currency", "rows", "amount_dec")
                            }
                            for row in result
                        ] == expected
                        assert "SortMergeJoin" in plan
                        if reference_digest is None:
                            reference_digest = record["result_digest"]
                        assert record["result_digest"] == reference_digest
                        record.update(
                            workload="merchant_enrichment",
                            mode=mode,
                            rows=size,
                            trial=trial,
                            financial_oracle_passed=True,
                        )
                        atomic_json(output / "benchmark.json", report)
                configure(spark, False)
                for salted in (False, True):
                    result, record, _ = execute(
                        merchant_summary(frame, salted),
                        f"n{size}/merchant_aggregate_{'salted' if salted else 'baseline'}",
                        ("merchant_id", "currency"),
                    )
                    assert result == expected_merchants
                    record.update(
                        workload="merchant_aggregation",
                        mode="salted" if salted else "baseline",
                        rows=size,
                        financial_oracle_passed=True,
                    )
                configure(spark, True)
                result, record, _ = execute(
                    invoice_control(frame),
                    f"n{size}/invoice_reconciliation",
                    ("currency",),
                )
                assert all(row.pop("bad_rows") == 0 for row in result)
                assert result == expected
                record.update(
                    workload="invoice_reconciliation",
                    rows=size,
                    journal_legs=size * 2,
                    financial_oracle_passed=True,
                )
                atomic_json(output / "benchmark.json", report)
                print(
                    json.dumps(
                        {
                            "completed_rows": size,
                            "hot_rows": size * 65 // 100,
                            "financial_checks": "PASSED",
                        }
                    ),
                    flush=True,
                )
            finally:
                frame.unpersist(blocking=True)
    except BaseException as exc:
        report.update(status="FAILED", error=str(exc))
        raise
    finally:
        spark.stop()
        report["cgroup_after"] = cgroup_memory()
        evidence = event_metrics(events)
        report["event_evidence"] = evidence
        for case in report["cases"]:
            case["task_metrics"] = evidence["groups"].get(case["group"], {})
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
        atomic_json(output / "benchmark.json", report)
    assert evidence["application_ended"] and evidence["oom_task_failures"] == 0
    assert all(case["task_metrics"].get("tasks", 0) > 0 for case in report["cases"])
    for counter in ("oom", "oom_kill"):
        before = report["cgroup_before"].get("events", {}).get(counter, 0)
        after = report["cgroup_after"].get("events", {}).get(counter, 0)
        assert before == after
    report["timing_medians_ms"] = []
    for size in sizes:
        for mode in MODES:
            values = sorted(
                case["elapsed_ms"]
                for case in report["cases"]
                if case.get("workload") == "merchant_enrichment"
                and case["rows"] == size
                and case["mode"] == mode
            )
            middle = len(values) // 2
            report["timing_medians_ms"].append(
                {
                    "rows": size,
                    "mode": mode,
                    "samples_ms": values,
                    "median_ms": values[middle]
                    if len(values) % 2
                    else (values[middle - 1] + values[middle]) // 2,
                }
            )
    report["status"] = "SUCCEEDED"
    atomic_json(output / "benchmark.json", report)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, nargs="+", default=[10000, 100000, 1000000])
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--output", type=Path)
    options = parser.parse_args()
    output = options.output or Path("/workspace/pipelines/output/benchmarks") / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    )
    print(f"Benchmark report: {output / 'benchmark.json'}", flush=True)
    result = run_benchmark(output, options.rows, options.trials)
    print(
        json.dumps(
            {
                "status": result["status"],
                "timing_medians_ms": result["timing_medians_ms"],
            },
            indent=2,
        )
    )
