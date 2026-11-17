"""Append Kafka and bank CSV sources to local Delta using AvailableNow.

Bronze keeps duplicates and invalid financial values. Contract validation,
decimal casting, and business-key deduplication belong to Silver.
"""

import argparse
import json
import re
from pathlib import Path

from data_generator.chaos_producer import BANK_FIELDS

TABLES = ("bronze_gateway_events", "bronze_cdc_ledger", "bronze_bank_settlements")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--gateway-topics", default="gateway_events")
    parser.add_argument(
        "--cdc-topics",
        default="finrecon_db.public.accounts,finrecon_db.public.invoices,finrecon_db.public.ledger_entries",
    )
    parser.add_argument("--bank-dir", type=Path, required=True)
    parser.add_argument("--dataset", default="local")
    parser.add_argument("--lakehouse-dir", type=Path, default=Path("/lakehouse"))
    parser.add_argument("--max-offsets-per-trigger", type=int, default=10000)
    parser.add_argument("--max-files-per-trigger", type=int, default=25)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", args.dataset):
        parser.error(
            "--dataset must be a simple name of 1-64 letters, digits, underscores, or hyphens"
        )
    if args.max_offsets_per_trigger <= 0 or args.max_files_per_trigger <= 0:
        parser.error("Micro-batch limits must be positive")
    for name in ("gateway_topics", "cdc_topics"):
        topics = [topic.strip() for topic in getattr(args, name).split(",")]
        if not all(topics):
            parser.error("Topic lists must not contain empty names")
        setattr(args, name, ",".join(sorted(set(topics))))
    if not args.bank_dir.is_dir():
        parser.error("--bank-dir must be an existing landing directory")
    args.bank_dir = args.bank_dir.resolve()
    args.lakehouse_dir = args.lakehouse_dir.resolve()
    return args


def paths(args, table):
    return (
        args.lakehouse_dir / "bronze" / args.dataset / table,
        args.lakehouse_dir / "checkpoints" / args.dataset / table,
    )


def bind_sources(args):
    """Prevent unsafe reuse of checkpoints with different sources or sinks."""
    identity = {
        "version": 1,
        "bootstrap_servers": args.bootstrap_servers,
        "gateway_topics": args.gateway_topics,
        "cdc_topics": args.cdc_topics,
        "bank_dir": str(args.bank_dir),
        "lakehouse_dir": str(args.lakehouse_dir),
    }
    target = args.lakehouse_dir / "manifests" / f"{args.dataset}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with target.open("x", encoding="utf-8") as stream:
            json.dump(identity, stream, indent=2)
    except FileExistsError:
        if json.loads(target.read_text(encoding="utf-8")) != identity:
            raise ValueError(
                "Source configuration changed; choose a new --dataset for new checkpoints"
            )


def ingestion_metadata(frame, source_system):
    from pyspark.sql import functions as F

    return (
        frame.withColumn("source_system", F.lit(source_system))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingest_date", F.to_date("ingested_at"))
    )


def kafka_rows(spark, args, topics, source_system):
    from pyspark.sql import functions as F

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", topics)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("includeHeaders", "true")
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .load()
    )
    return ingestion_metadata(
        raw.select(
            F.col("key").alias("raw_key"),
            F.col("value").alias("raw_value"),
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("raw_payload"),
            "topic",
            "partition",
            "offset",
            "headers",
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("timestampType").alias("kafka_timestamp_type"),
        ),
        source_system,
    )


def bank_rows(spark, args):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType(
        [StructField(name, StringType(), True) for name in BANK_FIELDS]
        + [StructField("_corrupt_record", StringType(), True)]
    )
    raw = (
        spark.readStream.format("csv")
        .schema(schema)
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "true")
        .option("pathGlobFilter", "*.csv")
        .option("maxFilesPerTrigger", args.max_files_per_trigger)
        .load(str(args.bank_dir))
    )
    return ingestion_metadata(
        raw.withColumn("source_file", F.input_file_name()).withColumn(
            "raw_payload",
            F.coalesce(F.col("_corrupt_record"), F.to_json(F.struct(*BANK_FIELDS))),
        ),
        "bank",
    )


def append_available(frame, target, checkpoint, query_name):
    query = (
        frame.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("ingest_date")
        .option("checkpointLocation", str(checkpoint))
        .queryName(query_name)
        .trigger(availableNow=True)
        .start(str(target))
    )
    query.awaitTermination()
    return {
        "query_id": str(query.id),
        "run_id": str(query.runId),
        "input_rows_this_run": sum(
            item["numInputRows"] for item in query.recentProgress
        ),
        "progress_batches_retained": len(query.recentProgress),
    }


def ingest(spark, args):
    bind_sources(args)
    report = {"dataset": args.dataset, "tables": {}}
    readers = (
        (TABLES[0], lambda: kafka_rows(spark, args, args.gateway_topics, "gateway")),
        (TABLES[1], lambda: kafka_rows(spark, args, args.cdc_topics, "ledger_cdc")),
        (TABLES[2], lambda: bank_rows(spark, args)),
    )
    # Run one stream at a time to stay inside the local Docker memory budget.
    for table, reader in readers:
        target, checkpoint = paths(args, table)
        progress = append_available(
            reader(), target, checkpoint, f"{args.dataset}_{table}"
        )
        report["tables"][table] = {
            "path": str(target),
            "checkpoint": str(checkpoint),
            **progress,
        }
        print(json.dumps({"completed": table, **progress}), flush=True)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main():
    from .runtime import spark_session

    args = parse_args()
    bind_sources(args)
    spark = spark_session("FinRecon local Bronze")
    try:
        print(json.dumps(ingest(spark, args), indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
