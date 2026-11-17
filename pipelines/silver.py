"""Incremental Silver contracts, quarantine, and bounded financial deduplication."""

import argparse
import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .contracts import METADATA, SOURCES, output_columns, prepare


def table_path(root, dataset, table):
    return Path(root) / "silver" / dataset / table


def ensure_delta(spark, frame, path, partition=None):
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, str(path)):
        writer = frame.limit(0).write.format("delta").mode("errorifexists")
        if partition:
            writer = writer.partitionBy(partition)
        writer.save(str(path))


def classify(spark, prepared, source, target, as_of_date):
    """Quarantine every variant of an ambiguous key instead of picking a winner."""
    from pyspark.sql import functions as F

    contract_bad = prepared.where(F.size("failed_rules") > 0)
    valid = prepared.where(F.size("failed_rules") == 0)
    variants = valid.groupBy("entity_key").agg(
        F.countDistinct("content_hash").alias("_variants")
    )
    valid = valid.join(variants, "entity_key")
    ambiguous = (
        valid.where(F.col("_variants") > 1)
        .drop("_variants")
        .withColumn(
            "failed_rules",
            F.array(F.lit("FINANCIAL_CONFLICT")),
        )
    )
    unique = valid.where(F.col("_variants") == 1).drop("_variants")
    cutoff = as_of_date - timedelta(days=6)
    existing = (
        spark.read.format("delta")
        .load(str(target))
        .where(F.col("event_date") >= F.lit(cutoff))
        .select(
            "entity_key",
            F.col("content_hash").alias("_existing_hash"),
            F.col("source_ts_ms").alias("_existing_ts"),
        )
    )
    compared = (
        unique.join(existing, "entity_key", "left")
        .withColumn(
            "failed_rules",
            F.when(
                F.col("_existing_ts") > F.col("source_ts_ms"),
                F.array(F.lit("STALE_SOURCE")),
            )
            .when(
                F.col("_existing_hash").isNotNull()
                & (F.col("_existing_hash") != F.col("content_hash")),
                F.array(F.lit("FINANCIAL_CONFLICT")),
            )
            .otherwise(F.col("failed_rules")),
        )
        .drop("_existing_hash", "_existing_ts")
    )
    return contract_bad.unionByName(ambiguous).unionByName(compared)


def quarantine_rows(frame):
    from pyspark.sql import functions as F

    return frame.select(
        "record_id",
        "source_table",
        "entity_key",
        "source_ts_ms",
        "raw_payload",
        "failed_rules",
        "contract_version",
        *METADATA,
        F.lit("PENDING").alias("status"),
        F.current_timestamp().alias("quarantined_at"),
        F.lit(None).cast("string").alias("corrected_payload"),
        F.lit(None).cast("string").alias("last_replay_error"),
        F.lit(None).cast("timestamp").alias("replayed_at"),
    ).dropDuplicates(["record_id"])


def merge_quarantine(spark, frame, path):
    from delta.tables import DeltaTable

    rows = quarantine_rows(frame)
    ensure_delta(spark, rows, path)
    DeltaTable.forPath(spark, str(path)).alias("target").merge(
        rows.alias("source"),
        "target.record_id = source.record_id",
    ).whenNotMatchedInsertAll().execute()


def merge_valid(spark, frame, source, target, as_of_date):
    from delta.tables import DeltaTable
    from pyspark.sql import Window, functions as F

    candidates = (
        frame.where(F.size("failed_rules") == 0)
        .withColumn(
            "_rank",
            F.row_number().over(Window.partitionBy("entity_key").orderBy("record_id")),
        )
        .where(F.col("_rank") == 1)
        .select(*output_columns(source))
    )
    cutoff = (as_of_date - timedelta(days=6)).isoformat()
    DeltaTable.forPath(spark, str(target)).alias("target").merge(
        candidates.alias("source"),
        f"target.entity_key = source.entity_key AND target.event_date >= DATE '{cutoff}'",
    ).whenNotMatchedInsertAll().execute()


def process_batch(spark, batch, source, root, dataset, as_of_date):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    if batch.isEmpty():
        return {"input_rows": 0, "quarantined_rows": 0}
    target = table_path(root, dataset, SOURCES[source]["silver"])
    quarantine = table_path(root, dataset, "silver_quarantine")
    evaluated = prepare(batch, source, as_of_date).persist(StorageLevel.MEMORY_AND_DISK)
    classified = None
    try:
        ensure_delta(
            spark, evaluated.select(*output_columns(source)), target, "event_date"
        )
        classified = classify(spark, evaluated, source, target, as_of_date).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        metrics = (
            classified.agg(
                F.count("*").alias("input_rows"),
                F.sum(F.when(F.size("failed_rules") > 0, 1).otherwise(0)).alias(
                    "quarantined_rows"
                ),
            )
            .first()
            .asDict()
        )
        if metrics["quarantined_rows"]:
            merge_quarantine(
                spark, classified.where(F.size("failed_rules") > 0), quarantine
            )
        else:
            ensure_delta(spark, quarantine_rows(classified.limit(0)), quarantine)
        if metrics["input_rows"] > metrics["quarantined_rows"]:
            merge_valid(spark, classified, source, target, as_of_date)
        print(json.dumps({"source": source, **metrics}), flush=True)
        return metrics
    finally:
        if classified is not None:
            classified.unpersist()
        evaluated.unpersist()


def run(spark, root, dataset, as_of_date):
    reports = {}
    for source, spec in SOURCES.items():
        bronze = Path(root) / "bronze" / dataset / spec["bronze"]
        checkpoint = Path(root) / "silver_checkpoints" / dataset / source
        stream = (
            spark.readStream.format("delta")
            .option("maxFilesPerTrigger", 25)
            .option("maxBytesPerTrigger", "32m")
            .load(str(bronze))
        )

        def write_batch(frame, batch_id, source_name=source):
            process_batch(spark, frame, source_name, root, dataset, as_of_date)

        query = (
            stream.writeStream.foreachBatch(write_batch)
            .option("checkpointLocation", str(checkpoint))
            .queryName(f"silver_{dataset}_{source}")
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        reports[source] = {"query_id": str(query.id), "run_id": str(query.runId)}
    return reports


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="portfolio_demo")
    parser.add_argument("--lakehouse-dir", type=Path, default=Path("/lakehouse"))
    parser.add_argument(
        "--as-of-date",
        type=date.fromisoformat,
        default=datetime.now(timezone.utc).date(),
    )
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", args.dataset):
        parser.error("Invalid dataset name")
    return args


if __name__ == "__main__":
    from .runtime import spark_session

    options = arguments()
    session = spark_session("FinRecon Silver")
    try:
        print(
            json.dumps(
                run(
                    session, options.lakehouse_dir, options.dataset, options.as_of_date
                ),
                indent=2,
            )
        )
    finally:
        session.stop()
