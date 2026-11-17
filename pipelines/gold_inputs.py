"""Incremental Silver snapshots and bucketed current evidence for reconciliation."""

import json
from pathlib import Path

from .silver import ensure_delta, table_path

TABLES = {
    "gateway": "silver_gateway_events",
    "bank": "silver_bank_settlements",
    "invoice": "silver_dim_invoices_scd2",
    "account": "silver_dim_accounts_scd2",
    "journal": "silver_ledger_entries",
}
BUCKETS = 32
SCHEMA = {
    "kind": "string",
    "key": "string",
    "lookup_key": "string",
    "invoice_id": "string",
    "account_id": "string",
    "gateway_ref": "string",
    "merchant_id": "string",
    "currency": "string",
    "amount_dec": "decimal(18,4)",
    "debit_dec": "decimal(18,4)",
    "credit_dec": "decimal(18,4)",
    "source_status": "string",
    "remittance_memo": "string",
    "active": "boolean",
    "record_id": "string",
    "source_commit": "long",
}


def gold_path(root, dataset, table):
    return Path(root) / "gold" / dataset / table


def bucket(column):
    from pyspark.sql import functions as F

    return F.pmod(F.xxhash64(column), F.lit(BUCKETS)).cast("int")


def bank_lookup():
    from pyspark.sql import functions as F

    return F.when(F.length("gateway_ref") > 0, F.col("gateway_ref")).otherwise(
        F.concat(F.lit("unreferenced:"), F.col("key"))
    )


def project(frame, kind):
    """Represent current evidence and two reverse indexes with identical schemas."""
    from pyspark.sql import functions as F

    values = {name: F.col(name) for name in SCHEMA if name in frame.columns}
    values.update(
        kind=F.lit(kind),
        key=F.col("entity_key"),
        source_commit=F.col("_commit_version"),
        active=F.lit(True),
    )
    if kind in ("invoice", "account", "journal"):
        values["active"] = ~F.col("is_deleted")
    if kind == "account":
        values["active"] = values["active"] & F.coalesce("is_active", F.lit(False))
    if "_change_type" in frame.columns:
        values["active"] = values["active"] & (F.col("_change_type") != "delete")
    if kind == "invoice":
        values["amount_dec"] = F.col("total_amount_dec")
        values["source_status"] = F.col("status")
    elif kind == "bank":
        values["amount_dec"] = F.col("settled_amount_dec")
    if kind == "bank_link":
        values["active"] = F.col("active") & (F.col("_change_type") != "delete")
    values["lookup_key"] = (
        (F.col("account_id") if kind == "account" else F.col("invoice_id"))
        if kind not in ("bank", "bank_link")
        else F.col("gateway_ref")
    )
    rows = frame.select(
        *[
            values.get(name, F.lit(None)).cast(data_type).alias(name)
            for name, data_type in SCHEMA.items()
        ]
    )
    if kind == "bank":
        rows = rows.withColumn("lookup_key", bank_lookup())
    if kind == "gateway":
        reference = rows.withColumn("kind", F.lit("gateway_ref")).withColumn(
            "lookup_key", F.col("gateway_ref")
        )
        rows = rows.unionByName(reference)
    elif kind in ("invoice", "journal"):
        reference = (
            rows.withColumn("kind", F.lit("account_ref"))
            .withColumn("key", F.concat(F.lit(kind + ":"), F.col("key")))
            .withColumn("lookup_key", F.col("account_id"))
        )
        rows = rows.unionByName(reference)
    return rows.withColumn("bucket", bucket("lookup_key")).withColumn(
        "content_hash", F.sha2(F.to_json(F.struct(*SCHEMA)), 256)
    )


def read_changes(spark, root, dataset):
    """Pin table versions; bootstrap once, then consume CDF with fail-closed gaps."""
    from delta.tables import DeltaTable
    from pyspark.sql import Window, functions as F

    cursor_path = gold_path(root, dataset, "state_reconciliation_cursors")
    previous = {}
    if DeltaTable.isDeltaTable(spark, str(cursor_path)):
        previous = json.loads(
            spark.read.format("delta").load(str(cursor_path)).first().sources
        )
    sources, changes, current = {}, [], []
    from .exception_links import ensure_links

    link_path = ensure_links(spark, root, dataset)
    for kind, name in {**TABLES, "bank_link": "state_exception_links"}.items():
        path = str(
            link_path if kind == "bank_link" else table_path(root, dataset, name)
        )
        table = DeltaTable.forPath(spark, path)
        detail = table.detail().first()
        prior = previous.get(kind)
        if prior and prior["id"] != detail.id:
            raise ValueError(f"Silver table replaced: {name}; use a new Gold dataset")
        if detail.properties.get("delta.enableChangeDataFeed") != "true":
            if prior:
                raise ValueError(f"CDF disabled for tracked table {name}")
            spark.sql(
                f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
            )
        version = table.history(1).first().version
        sources[kind] = {"id": detail.id, "version": version}
        if prior and version == prior["version"]:
            continue
        if prior:
            raw = (
                spark.read.format("delta")
                .option("readChangeFeed", "true")
                .option("startingVersion", prior["version"] + 1)
                .option("endingVersion", version)
                .load(path)
            )
        else:
            raw = (
                spark.read.format("delta")
                .option("versionAsOf", version)
                .load(path)
                .withColumn("_commit_version", F.lit(version).cast("long"))
                .withColumn("_change_type", F.lit("insert"))
            )
        if kind in ("invoice", "account"):
            raw = raw.where("is_current")
        changes.append(project(raw, kind))
        after = raw.where("_change_type != 'update_preimage'")
        if not (
            after.groupBy("entity_key", "_commit_version")
            .count()
            .where("count > 1")
            .isEmpty()
        ):
            raise ValueError(f"Ambiguous current source key in {name}")
        latest = after.withColumn(
            "_latest",
            F.row_number().over(
                Window.partitionBy("entity_key").orderBy(
                    F.col("_commit_version").desc()
                )
            ),
        ).where("_latest = 1")
        current.append(project(latest, kind))
    if not changes:
        return None, None, sources
    from functools import reduce

    return (
        reduce(lambda left, right: left.unionByName(right), changes),
        reduce(lambda left, right: left.unionByName(right), current),
        sources,
    )


def merge_inputs(spark, rows, changes, path):
    from delta.tables import DeltaTable

    ensure_delta(spark, rows, path, ["kind", "bucket"])
    buckets = [str(row.bucket) for row in changes.select("bucket").distinct().collect()]
    if not buckets:
        return
    DeltaTable.forPath(spark, str(path)).alias("t").merge(
        rows.alias("s"),
        "t.kind = s.kind AND t.key = s.key AND t.bucket IN (" + ",".join(buckets) + ")",
    ).whenMatchedUpdateAll(
        condition="s.source_commit >= t.source_commit AND s.content_hash != t.content_hash"
    ).whenNotMatchedInsertAll().execute()


def lookup(spark, path, kind, keys, active=True):
    """Read only touched hash buckets; never collect transaction keys on driver."""
    from pyspark.sql import functions as F

    keys = (
        keys.select(F.col(keys.columns[0]).alias("lookup_key"))
        .where("lookup_key IS NOT NULL")
        .distinct()
    )
    buckets = [
        row.bucket
        for row in keys.select(bucket("lookup_key").alias("bucket"))
        .distinct()
        .collect()
    ]
    rows = spark.read.format("delta").load(str(path)).where(F.col("kind") == kind)
    rows = rows.where(F.col("bucket").isin(buckets))
    if active:
        rows = rows.where("active")
    return rows.join(keys, "lookup_key", "left_semi")


def commit_cursors(spark, root, dataset, sources):
    from delta.tables import DeltaTable

    path = gold_path(root, dataset, "state_reconciliation_cursors")
    row = spark.createDataFrame(
        [("gold", json.dumps(sources, sort_keys=True))],
        "pipeline STRING, sources STRING",
    )
    ensure_delta(spark, row, path)
    DeltaTable.forPath(spark, str(path)).alias("t").merge(
        row.alias("s"), "t.pipeline = s.pipeline"
    ).whenMatchedUpdateAll(
        condition="t.sources != s.sources"
    ).whenNotMatchedInsertAll().execute()
