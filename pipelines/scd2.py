"""Atomic union-source SCD-2 and WAL-ordered operational journal state."""

from .silver import ensure_delta

SCD_METADATA = (
    "entity_key",
    "is_deleted",
    "row_hash",
    "record_id",
    "source_lsn",
    "source_order",
    "source_ts_ms",
    "version_id",
    "eff_start_version",
    "eff_end_version",
    "eff_start_ts",
    "eff_end_ts",
    "is_current",
    "last_source_version",
    "last_source_ts_ms",
    "last_effective_ts",
    "last_record_id",
    "clock_regressed",
)


def _ledger_projection(rows, spec):
    from pyspark.sql import functions as F

    return rows.select(
        "entity_key",
        "is_deleted",
        "row_hash",
        "record_id",
        "source_lsn",
        "source_order",
        "source_ts_ms",
        "source_version",
        F.col("source_version").alias("last_source_version"),
        F.col("source_ts_ms").alias("last_source_ts_ms"),
        F.timestamp_millis("source_ts_ms").alias("last_effective_ts"),
        *spec["attributes"],
    )


def _dimension_seed(rows, spec):
    from pyspark.sql import functions as F

    return rows.select(
        "entity_key",
        "is_deleted",
        "row_hash",
        "record_id",
        "source_lsn",
        "source_order",
        "source_ts_ms",
        *spec["attributes"],
        F.sha2(F.to_json(F.struct("entity_key", "source_version")), 256).alias(
            "version_id"
        ),
        F.col("source_version").alias("eff_start_version"),
        F.lit(None).cast("string").alias("eff_end_version"),
        F.timestamp_millis("source_ts_ms").alias("eff_start_ts"),
        F.lit(None).cast("timestamp").alias("eff_end_ts"),
        F.lit(True).alias("is_current"),
        F.col("source_version").alias("last_source_version"),
        F.col("source_ts_ms").alias("last_source_ts_ms"),
        F.timestamp_millis("source_ts_ms").alias("last_effective_ts"),
        F.col("record_id").alias("last_record_id"),
        F.lit(False).alias("clock_regressed"),
    )


def _eligible(spark, rows, target, dimension):
    from pyspark.sql import Window, functions as F

    original_columns = rows.columns
    counts = rows.groupBy("entity_key", "source_version").agg(
        F.countDistinct("row_hash").alias("_variants")
    )
    grouped = rows.join(counts, ["entity_key", "source_version"])
    ambiguous = (
        grouped.where("_variants > 1")
        .select(*original_columns)
        .withColumn(
            "failed_rules",
            F.array(F.lit("SOURCE_VERSION_CONFLICT")),
        )
    )
    unambiguous = (
        grouped.where("_variants = 1")
        .withColumn(
            "_pick",
            F.row_number().over(
                Window.partitionBy("entity_key", "source_version").orderBy("record_id"),
            ),
        )
        .where("_pick = 1")
        .select(*original_columns)
    )
    history = spark.read.format("delta").load(str(target))
    current = history.where("is_current") if dimension else history
    lookup = current.select(
        "entity_key",
        F.col("last_source_version").alias("_prior_version"),
        F.col("row_hash").alias("_prior_hash"),
        F.col("last_effective_ts").alias("_prior_time"),
    )
    compared = unambiguous.join(lookup, "entity_key", "left")
    old = compared.where(
        F.col("_prior_version").isNotNull()
        & (F.col("source_version") <= F.col("_prior_version"))
    )
    if dimension:
        known = (
            old.alias("s")
            .join(
                history.alias("h"),
                (F.col("s.entity_key") == F.col("h.entity_key"))
                & (F.col("s.row_hash") == F.col("h.row_hash"))
                & (F.col("s.source_version") >= F.col("h.eff_start_version"))
                & (
                    F.col("h.eff_end_version").isNull()
                    | (F.col("s.source_version") < F.col("h.eff_end_version"))
                ),
                "left_semi",
            )
            .select("record_id")
        )
        stale = old.join(known, "record_id", "left_anti")
    else:
        stale = old.where(F.col("row_hash") != F.col("_prior_hash"))
    rejected = stale.withColumn(
        "failed_rules",
        F.when(
            F.col("source_version") == F.col("_prior_version"),
            F.array(F.lit("SOURCE_VERSION_CONFLICT")),
        ).otherwise(F.array(F.lit("STALE_SOURCE"))),
    ).select(*original_columns)
    newer = compared.where(
        F.col("_prior_version").isNull()
        | (F.col("source_version") > F.col("_prior_version"))
    )
    return newer, ambiguous.unionByName(rejected)


def commit_scd2(spark, target, staged, columns):
    """One Delta transaction closes current rows and inserts every new version."""
    from delta.tables import DeltaTable

    metadata_updates = (
        "is_current",
        "eff_end_version",
        "eff_end_ts",
        "last_source_version",
        "last_source_ts_ms",
        "last_effective_ts",
        "last_record_id",
        "clock_regressed",
    )
    DeltaTable.forPath(spark, str(target)).alias("target").merge(
        staged.alias("source"),
        "target.entity_key = source.merge_key AND target.is_current = true",
    ).whenMatchedUpdate(
        set={name: f"source.{name}" for name in metadata_updates}
    ).whenNotMatchedInsert(
        condition="source._action = 'INSERT'",
        values={name: f"source.{name}" for name in columns},
    ).execute()


def apply_dimension(spark, rows, target, spec):
    from pyspark import StorageLevel
    from pyspark.sql import Window, functions as F

    seed = _dimension_seed(rows, spec)
    columns = seed.columns
    ensure_delta(spark, seed, target)
    eligible, rejected = _eligible(spark, rows, target, True)
    eligible = eligible.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        if eligible.isEmpty():
            return rejected
        order = Window.partitionBy("entity_key").orderBy("source_version")
        cumulative = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        incoming = eligible.withColumn(
            "_effective_ts",
            F.greatest(
                F.max(F.timestamp_millis("source_ts_ms")).over(cumulative),
                F.col("_prior_time"),
            ),
        )
        previous = F.coalesce(F.lag("row_hash").over(order), F.col("_prior_hash"))
        incoming = incoming.withColumn(
            "_changed", (~F.col("row_hash").eqNullSafe(previous)).cast("long")
        )
        incoming = incoming.withColumn("_segment", F.sum("_changed").over(cumulative))
        segment = Window.partitionBy("entity_key", "_segment")
        incoming = incoming.withColumn(
            "_last",
            F.max(
                F.struct(
                    F.col("source_version").alias("version"),
                    F.col("source_ts_ms").alias("ts_ms"),
                    F.col("_effective_ts").alias("effective_ts"),
                    F.col("record_id").alias("record_id"),
                )
            ).over(segment),
        )
        incoming = incoming.withColumn(
            "_clock",
            F.max(
                (F.timestamp_millis("source_ts_ms") < F.col("_effective_ts")).cast(
                    "int"
                ),
            ).over(segment),
        )
        segments = incoming.withColumn(
            "_first", F.row_number().over(segment.orderBy("source_version"))
        ).where("_first = 1")
        changes = segments.where("_segment > 0")
        changes = (
            changes.withColumn(
                "version_id",
                F.sha2(F.to_json(F.struct("entity_key", "source_version")), 256),
            )
            .withColumn("eff_start_version", F.col("source_version"))
            .withColumn("eff_end_version", F.lead("source_version").over(order))
            .withColumn("eff_start_ts", F.col("_effective_ts"))
            .withColumn("eff_end_ts", F.lead("_effective_ts").over(order))
            .withColumn("is_current", F.col("eff_end_version").isNull())
            .withColumn("last_source_version", F.col("_last.version"))
            .withColumn("last_source_ts_ms", F.col("_last.ts_ms"))
            .withColumn("last_effective_ts", F.col("_last.effective_ts"))
            .withColumn("last_record_id", F.col("_last.record_id"))
            .withColumn("clock_regressed", F.col("_clock") > 0)
        )
        first_change = changes.groupBy("entity_key").agg(
            F.min(
                F.struct(
                    F.col("eff_start_version").alias("version"),
                    F.col("eff_start_ts").alias("time"),
                )
            ).alias("_close")
        )
        prefix = segments.where("_segment = 0").select(
            "entity_key",
            F.col("_last").alias("_prefix"),
            F.col("_clock").alias("_prefix_clock"),
        )
        current = spark.read.format("delta").load(str(target)).where("is_current")
        controls = (
            current.join(eligible.select("entity_key").distinct(), "entity_key")
            .join(first_change, "entity_key", "left")
            .join(prefix, "entity_key", "left")
        )
        controls = (
            controls.withColumn("is_current", F.col("_close").isNull())
            .withColumn("eff_end_version", F.col("_close.version"))
            .withColumn("eff_end_ts", F.col("_close.time"))
            .withColumn(
                "last_source_version",
                F.coalesce("_prefix.version", "last_source_version"),
            )
            .withColumn(
                "last_source_ts_ms", F.coalesce("_prefix.ts_ms", "last_source_ts_ms")
            )
            .withColumn(
                "last_effective_ts",
                F.coalesce("_prefix.effective_ts", "last_effective_ts"),
            )
            .withColumn(
                "last_record_id", F.coalesce("_prefix.record_id", "last_record_id")
            )
            .withColumn(
                "clock_regressed",
                F.col("clock_regressed")
                | F.coalesce(F.col("_prefix_clock") > 0, F.lit(False)),
            )
            .select(*columns)
            .withColumn("merge_key", F.col("entity_key"))
            .withColumn("_action", F.lit("CONTROL"))
        )
        inserts = (
            changes.select(*columns)
            .withColumn("merge_key", F.lit(None).cast("string"))
            .withColumn("_action", F.lit("INSERT"))
        )
        commit_scd2(spark, target, controls.unionByName(inserts), columns)
        return rejected
    finally:
        eligible.unpersist()


def apply_ledger(spark, rows, target, spec):
    from delta.tables import DeltaTable
    from pyspark import StorageLevel
    from pyspark.sql import Window, functions as F

    seed = _ledger_projection(rows, spec)
    ensure_delta(spark, seed, target)
    latest = (
        rows.withColumn(
            "_latest",
            F.dense_rank().over(
                Window.partitionBy("entity_key").orderBy(
                    F.col("source_version").desc()
                ),
            ),
        )
        .where("_latest = 1")
        .drop("_latest")
    )
    eligible, rejected = _eligible(spark, latest, target, False)
    eligible = eligible.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        if not eligible.isEmpty():
            DeltaTable.forPath(spark, str(target)).alias("target").merge(
                _ledger_projection(eligible, spec).alias("source"),
                "target.entity_key = source.entity_key",
            ).whenMatchedUpdateAll(
                condition="source.last_source_version > target.last_source_version"
            ).whenNotMatchedInsertAll().execute()
        return rejected
    finally:
        eligible.unpersist()
