"""Explicit, revalidated corrections for pending gateway/bank quarantine rows."""

import json

from .contracts import METADATA, SOURCES, prepare
from .silver import classify, merge_valid, table_path


def acknowledge_replay(spark, quarantine_path, updates):
    from delta.tables import DeltaTable

    DeltaTable.forPath(spark, str(quarantine_path)).alias("target").merge(
        updates.alias("source"),
        "target.record_id = source.record_id",
    ).whenMatchedUpdate(
        condition="target.status = 'PENDING'",
        set={
            "status": "source.new_status",
            "corrected_payload": "source.corrected_payload",
            "last_replay_error": "source.error",
            "replayed_at": "CASE WHEN source.new_status = 'REPLAYED' THEN current_timestamp() ELSE NULL END",
        },
    ).execute()


def replay(spark, root, dataset, as_of_date, corrections_path):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    quarantine_path = table_path(root, dataset, "silver_quarantine")
    quarantine = spark.read.format("delta").load(str(quarantine_path))
    corrections = (
        spark.read.schema("record_id STRING, corrected_payload STRING")
        .option("mode", "FAILFAST")
        .json(str(corrections_path))
    ).cache()
    try:
        if (
            corrections.where(
                F.col("record_id").isNull() | F.col("corrected_payload").isNull()
            )
            .limit(1)
            .count()
        ):
            raise ValueError("Corrections require record_id and corrected_payload")
        if corrections.groupBy("record_id").count().where("count > 1").limit(1).count():
            raise ValueError("Only one correction per record_id is allowed")
        if (
            corrections.join(quarantine.select("record_id"), "record_id", "left_anti")
            .limit(1)
            .count()
        ):
            raise ValueError("Correction references an unknown quarantine record")
        pending = (
            quarantine.where("status = 'PENDING'")
            .drop("corrected_payload")
            .join(corrections, "record_id")
        )
        if (
            pending.where(
                ~F.col("source_table").isin(
                    [spec["bronze"] for spec in SOURCES.values()]
                )
            )
            .limit(1)
            .count()
        ):
            raise ValueError(
                "CDC quarantine requires a source-specific correction; gateway/bank replay cannot apply it"
            )
        report = {"replayed": 0, "still_pending": 0}
        for source, spec in SOURCES.items():
            selected = pending.where(F.col("source_table") == spec["bronze"])
            if selected.isEmpty():
                continue
            corrected = selected.select(
                "record_id", F.col("corrected_payload").alias("raw_payload"), *METADATA
            )
            identity = selected.select(
                "record_id",
                F.col("entity_key").alias("_original_key"),
                F.col("source_ts_ms").alias("_original_ts"),
            )
            evaluated = prepare(
                corrected, source, as_of_date, preserve_record_id=True
            ).join(identity, "record_id")
            identity_changed = (
                F.col("_original_key").isNotNull()
                & ~F.col("entity_key").eqNullSafe(F.col("_original_key"))
            ) | (
                F.col("_original_ts").isNotNull()
                & ~F.col("source_ts_ms").eqNullSafe(F.col("_original_ts"))
            )
            evaluated = evaluated.withColumn(
                "failed_rules",
                F.when(
                    identity_changed,
                    F.array_union(
                        "failed_rules", F.array(F.lit("REPLAY_IDENTITY_CHANGED"))
                    ),
                ).otherwise(F.col("failed_rules")),
            ).persist(StorageLevel.MEMORY_AND_DISK)
            classified = None
            try:
                target = table_path(root, dataset, spec["silver"])
                classified = classify(
                    spark, evaluated, source, target, as_of_date
                ).persist(StorageLevel.MEMORY_AND_DISK)
                updates = (
                    classified.select(
                        "record_id",
                        F.col("raw_payload").alias("corrected_payload"),
                        F.when(F.size("failed_rules") == 0, F.lit("REPLAYED"))
                        .otherwise(F.lit("PENDING"))
                        .alias("new_status"),
                        F.when(
                            F.size("failed_rules") > 0, F.concat_ws(",", "failed_rules")
                        ).alias("error"),
                    )
                    .dropDuplicates(["record_id"])
                    .cache()
                )
                try:
                    tally = {
                        row.new_status: row["count"]
                        for row in updates.groupBy("new_status").count().collect()
                    }
                    if tally.get("REPLAYED", 0):
                        merge_valid(spark, classified, source, target, as_of_date)
                    # If a crash occurs before this acknowledgement, replay finds
                    # the same hash in Silver and completes without another insert.
                    acknowledge_replay(spark, quarantine_path, updates)
                    report["replayed"] += tally.get("REPLAYED", 0)
                    report["still_pending"] += tally.get("PENDING", 0)
                finally:
                    updates.unpersist()
            finally:
                if classified is not None:
                    classified.unpersist()
                evaluated.unpersist()
        print(json.dumps(report), flush=True)
        return report
    finally:
        corrections.unpersist()


if __name__ == "__main__":
    import argparse
    import re
    from datetime import date, datetime, timezone
    from pathlib import Path
    from .runtime import spark_session

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="portfolio_demo")
    parser.add_argument("--lakehouse-dir", type=Path, default=Path("/lakehouse"))
    parser.add_argument(
        "--as-of-date",
        type=date.fromisoformat,
        default=datetime.now(timezone.utc).date(),
    )
    parser.add_argument("--corrections", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", args.dataset):
        parser.error("Invalid dataset name")
    session = spark_session("FinRecon quarantine replay")
    try:
        replay(
            session, args.lakehouse_dir, args.dataset, args.as_of_date, args.corrections
        )
    finally:
        session.stop()
