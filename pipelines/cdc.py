"""Normalize Debezium CDC and apply ordered account/invoice/ledger state."""

import json
from pathlib import Path

from .contracts import METADATA, evaluate_rules, load_contract
from .silver import arguments, ensure_delta, merge_quarantine, table_path

ENTITIES = {
    "accounts": {
        "key": "account_id",
        "table": "silver_dim_accounts_scd2",
        "attributes": (
            "account_id",
            "merchant_id",
            "account_name",
            "tier",
            "is_active",
        ),
    },
    "invoices": {
        "key": "invoice_id",
        "table": "silver_dim_invoices_scd2",
        "attributes": (
            "invoice_id",
            "account_id",
            "total_amount_dec",
            "currency",
            "status",
        ),
    },
    "ledger_entries": {
        "key": "entry_id",
        "table": "silver_ledger_entries",
        "attributes": (
            "entry_id",
            "invoice_id",
            "account_id",
            "debit_dec",
            "credit_dec",
            "entry_ts",
        ),
    },
}
CDC_COLUMNS = (
    "record_id",
    "source_table",
    "cdc_table",
    "entity_key",
    "message_entity_key",
    "op",
    "is_deleted",
    "is_tombstone",
    "source_lsn",
    "source_order",
    "source_version",
    "source_ts_ms",
    "row_hash",
    "raw_payload",
    "message_key",
    *METADATA,
    "contract_version",
    "failed_rules",
    "account_id",
    "merchant_id",
    "account_name",
    "tier",
    "is_active",
    "invoice_id",
    "total_amount_dec",
    "currency",
    "status",
    "entry_id",
    "debit_dec",
    "credit_dec",
    "entry_ts",
)


def normalize(frame):
    from pyspark.sql import functions as F

    rules, fingerprint = load_contract("cdc")
    selected = [
        F.col(name).cast(kind).alias(name)
        if name in frame.columns
        else F.lit(None).cast(kind).alias(name)
        for name, kind in {**METADATA, "message_key": "string"}.items()
    ]
    data = frame.select("raw_payload", *selected)
    data = data.withColumn(
        "_body",
        F.coalesce(F.get_json_object("raw_payload", "$.payload"), F.col("raw_payload")),
    )
    data = data.withColumn("op", F.get_json_object("_body", "$.op"))
    data = data.withColumn("is_tombstone", F.col("raw_payload").isNull())
    data = data.withColumn("topic_table", F.regexp_extract("topic", r"[.]([^.]+)$", 1))
    data = data.withColumn(
        "cdc_table",
        F.coalesce(F.get_json_object("_body", "$.source.table"), F.col("topic_table")),
    )
    data = data.withColumn(
        "_row",
        F.when(F.col("op") == "d", F.get_json_object("_body", "$.before")).otherwise(
            F.get_json_object("_body", "$.after")
        ),
    )
    key_json = F.coalesce(
        F.get_json_object("message_key", "$.payload"), F.col("message_key")
    )
    row_key, wire_key = F.lit(None).cast("string"), F.lit(None).cast("string")
    for name, spec in ENTITIES.items():
        row_key = F.when(
            F.col("cdc_table") == name, F.get_json_object("_row", f"$.{spec['key']}")
        ).otherwise(row_key)
        wire_key = F.when(
            F.col("cdc_table") == name, F.get_json_object(key_json, f"$.{spec['key']}")
        ).otherwise(wire_key)
    data = data.withColumn("message_entity_key", wire_key).withColumn(
        "entity_key", F.coalesce(row_key, wire_key)
    )
    for name in (
        "account_id",
        "merchant_id",
        "account_name",
        "tier",
        "invoice_id",
        "currency",
        "status",
        "entry_id",
    ):
        data = data.withColumn(name, F.trim(F.get_json_object("_row", f"$.{name}")))
    for table, spec in ENTITIES.items():
        key = spec["key"]
        data = data.withColumn(
            key,
            F.when(
                (F.col("cdc_table") == table) & (F.col("op") == "d"),
                F.coalesce(F.col(key), F.col("entity_key")),
            ).otherwise(F.col(key)),
        )
    for name, path in {
        "active_text": "is_active",
        "total_amount_text": "total_amount",
        "debit_text": "debit",
        "credit_text": "credit",
    }.items():
        data = data.withColumn(name, F.get_json_object("_row", f"$.{path}"))
    for name, expression in {
        "is_active": "try_cast(active_text AS BOOLEAN)",
        "total_amount_dec": "try_cast(total_amount_text AS DECIMAL(18,4))",
        "debit_dec": "try_cast(debit_text AS DECIMAL(18,4))",
        "credit_dec": "try_cast(credit_text AS DECIMAL(18,4))",
    }.items():
        data = data.withColumn(name, F.expr(expression))
    data = data.withColumn("entry_ts_text", F.get_json_object("_row", "$.entry_ts"))
    data = data.withColumn("entry_ts", F.expr("try_cast(entry_ts_text AS TIMESTAMP)"))
    for name, path in {
        "lsn_text": "lsn",
        "source_ts_text": "ts_ms",
        "source_db": "db",
        "source_schema": "schema",
        "source_connector": "connector",
    }.items():
        data = data.withColumn(name, F.get_json_object("_body", f"$.source.{path}"))
    data = data.withColumn("source_lsn", F.expr("try_cast(lsn_text AS DECIMAL(20,0))"))
    data = data.withColumn("source_ts_ms", F.expr("try_cast(source_ts_text AS BIGINT)"))
    data = data.withColumn(
        "_tx_order", F.get_json_object("_body", "$.transaction.total_order")
    )
    data = data.withColumn(
        "source_order",
        F.coalesce(F.expr("try_cast(_tx_order AS BIGINT)"), F.col("offset")),
    )
    data = data.withColumn(
        "source_version",
        F.concat(
            F.lpad(F.col("source_lsn").cast("string"), 20, "0"),
            F.lit(":"),
            F.lpad(F.col("source_order").cast("string"), 20, "0"),
        ),
    )
    data = data.withColumn("is_deleted", F.coalesce(F.col("op") == "d", F.lit(False)))
    state_json = F.lit(None).cast("string")
    for name, spec in ENTITIES.items():
        state_json = F.when(
            F.col("cdc_table") == name,
            F.to_json(F.struct("is_deleted", *spec["attributes"])),
        ).otherwise(state_json)
    state_json = F.when(
        F.col("is_deleted"), F.to_json(F.struct("entity_key", "is_deleted"))
    ).otherwise(state_json)
    data = data.withColumn("row_hash", F.sha2(state_json, 256))
    data = data.withColumn("source_table", F.lit("bronze_cdc_ledger"))
    data = data.withColumn(
        "record_id",
        F.sha2(
            F.to_json(F.struct("source_table", "topic", "partition", "offset")), 256
        ),
    )
    data = data.withColumn("contract_version", F.lit(fingerprint))
    return evaluate_rules(data, rules).select(*CDC_COLUMNS)


def with_failure(frame, reason):
    from pyspark.sql import functions as F

    return frame.withColumn(
        "failed_rules", F.array_union("failed_rules", F.array(F.lit(reason)))
    )


def merge_audit(spark, frame, path):
    from delta.tables import DeltaTable

    ensure_delta(spark, frame, path)
    DeltaTable.forPath(spark, str(path)).alias("target").merge(
        frame.dropDuplicates(["record_id"]).alias("source"),
        "target.record_id = source.record_id",
    ).whenNotMatchedInsertAll().execute()


def process_batch(spark, batch, root, dataset):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from .scd2 import apply_dimension, apply_ledger

    if batch.isEmpty():
        return
    normalized = normalize(batch).persist(StorageLevel.MEMORY_AND_DISK)
    fresh = None
    try:
        audit_path = table_path(root, dataset, "silver_cdc_events")
        ensure_delta(spark, normalized, audit_path)
        processed = spark.read.format("delta").load(str(audit_path)).select("record_id")
        fresh = normalized.join(processed, "record_id", "left_anti").persist(
            StorageLevel.MEMORY_AND_DISK
        )
        if fresh.isEmpty():
            return
        input_count = fresh.count()
        bad = fresh.where(F.size("failed_rules") > 0)
        changes = fresh.where((F.size("failed_rules") == 0) & ~F.col("is_tombstone"))
        quarantine_path = table_path(root, dataset, "silver_quarantine")
        rejected = bad
        for entity, spec in ENTITIES.items():
            rows = changes.where(F.col("cdc_table") == entity)
            if rows.isEmpty():
                continue
            target = table_path(root, dataset, spec["table"])
            failures = (
                apply_ledger(spark, rows, target, spec)
                if entity == "ledger_entries"
                else apply_dimension(spark, rows, target, spec)
            )
            rejected = rejected.unionByName(failures)
        rejected = rejected.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            rejected_count = rejected.count()
            if rejected_count:
                merge_quarantine(spark, rejected, quarantine_path)
            # This final acknowledgement is deliberately after every state merge.
            # A failed earlier write cannot mark an unapplied event as processed.
            merge_audit(spark, fresh, audit_path)
            print(
                json.dumps(
                    {
                        "cdc_input_rows": input_count,
                        "quarantined_rows": rejected_count,
                    }
                ),
                flush=True,
            )
        finally:
            rejected.unpersist()
    finally:
        if fresh is not None:
            fresh.unpersist()
        normalized.unpersist()


def run(spark, root, dataset):
    source = Path(root) / "bronze" / dataset / "bronze_cdc_ledger"
    checkpoint = Path(root) / "silver_checkpoints" / dataset / "cdc"
    stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", 25)
        .option("maxBytesPerTrigger", "32m")
        .load(str(source))
    )
    query = (
        stream.writeStream.foreachBatch(
            lambda frame, batch_id: process_batch(spark, frame, root, dataset)
        )
        .option("checkpointLocation", str(checkpoint))
        .queryName(f"silver_{dataset}_cdc")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return {"query_id": str(query.id), "run_id": str(query.runId)}


if __name__ == "__main__":
    from .runtime import spark_session

    options = arguments()
    session = spark_session("FinRecon CDC normalization")
    try:
        print(
            json.dumps(run(session, options.lakehouse_dir, options.dataset), indent=2)
        )
    finally:
        session.stop()
