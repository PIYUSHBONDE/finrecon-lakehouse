"""Read-only, revalidated bank-reference overlays; raw Silver is never edited."""

from .gold_inputs import SCHEMA, gold_path, lookup
from .silver import ensure_delta


def ensure_links(spark, root, dataset):
    schema = ",".join(f"{name} {kind}" for name, kind in SCHEMA.items())
    seed = spark.createDataFrame(
        [], schema + ",entity_key STRING, proposal_hash STRING"
    )
    path = gold_path(root, dataset, "state_exception_links")
    ensure_delta(spark, seed, path)
    return path


def linked_banks(spark, cache, references):
    from pyspark.sql import functions as F
    from .gold import keys

    links = lookup(spark, cache, "bank_link", references)
    bank_keys = keys(links.select(F.concat(F.lit("unreferenced:"), F.col("key"))))
    banks = lookup(spark, cache, "bank", bank_keys)
    gateways = lookup(spark, cache, "gateway_ref", references)
    unique_references = (
        gateways.groupBy("gateway_ref").count().where("count = 1").select("gateway_ref")
    )
    links = links.join(unique_references, "gateway_ref", "left_semi")
    joined = (
        links.alias("l")
        .join(
            banks.alias("b"),
            (F.col("l.key") == F.col("b.key"))
            & (F.col("l.source_status") == F.col("b.content_hash"))
            & (F.col("l.record_id") == F.col("b.record_id"))
            & (F.col("l.amount_dec") == F.col("b.amount_dec"))
            & (F.col("l.currency") == F.col("b.currency"))
            & (F.col("l.merchant_id") == F.col("b.merchant_id")),
        )
        .join(
            gateways.alias("g"),
            (F.col("l.gateway_ref") == F.col("g.gateway_ref"))
            & (F.col("l.invoice_id") == F.col("g.invoice_id"))
            & (F.col("l.account_id") == F.col("g.key"))
            & (F.col("l.amount_dec") == F.col("g.amount_dec"))
            & (F.col("l.currency") == F.col("g.currency"))
            & (F.col("l.merchant_id") == F.col("g.merchant_id")),
        )
    )
    # account_id in link evidence binds the gateway business key. No source
    # account column is modified; real bank evidence has no account identity.
    return joined.select(
        *[
            F.col("l.gateway_ref").alias(name)
            if name in ("gateway_ref", "lookup_key")
            else F.col(f"b.{name}").alias(name)
            for name in banks.columns
        ]
    )


def banks_with_links(spark, cache, references):
    return lookup(spark, cache, "bank", references).unionByName(
        linked_banks(spark, cache, references)
    )
