"""Exact three-way reconciliation with incremental evidence and durable open items."""

import json

from .gold_inputs import (
    commit_cursors,
    gold_path,
    lookup,
    merge_inputs,
    read_changes,
)
from .silver import arguments, ensure_delta
from .exception_links import banks_with_links, linked_banks


def keys(*frames):
    from functools import reduce
    from pyspark.sql import functions as F

    return (
        reduce(
            lambda a, b: a.unionByName(b),
            [
                frame.select(F.col(frame.columns[0]).alias("lookup_key"))
                for frame in frames
            ],
        )
        .where("lookup_key IS NOT NULL AND length(lookup_key) > 0")
        .distinct()
    )


def reference_counts(spark, cache, references):
    from pyspark.sql import functions as F

    return (
        lookup(spark, cache, "gateway_ref", references)
        .groupBy("gateway_ref")
        .agg(F.count("*").alias("reference_count"))
    )


def invoice_results(
    spark, cache, invoice_ids, cut=lambda frame: frame, bank_reader=banks_with_links
):
    from pyspark.sql import functions as F

    invoices = lookup(spark, cache, "invoice", invoice_ids).select(
        "invoice_id",
        F.col("key").alias("invoice_key"),
        F.col("amount_dec").alias("invoice_amount_dec"),
        F.col("currency").alias("invoice_currency"),
        F.col("account_id").alias("invoice_account_id"),
        "source_status",
    )
    gateway_rows = lookup(spark, cache, "gateway", invoice_ids)
    gateways = gateway_rows.groupBy("invoice_id").agg(
        F.count("*").alias("gateway_count"),
        F.max(
            F.struct("key", "gateway_ref", "amount_dec", "currency", "merchant_id")
        ).alias("g"),
    )
    references = cut(keys(gateway_rows.select("gateway_ref")))
    counts = reference_counts(spark, cache, references)
    banks = (
        bank_reader(spark, cache, references)
        .groupBy("gateway_ref")
        .agg(
            F.count("*").alias("bank_count"),
            F.max(F.struct("key", "amount_dec", "currency", "merchant_id")).alias("b"),
        )
    )
    journals = lookup(spark, cache, "journal", invoice_ids)
    account_ids = cut(
        keys(invoices.select("invoice_account_id"), journals.select("account_id"))
    )
    accounts = lookup(spark, cache, "account", account_ids).select(
        "account_id", F.col("merchant_id").alias("account_merchant")
    )
    journal_totals = (
        journals.join(accounts, "account_id", "left")
        .groupBy("invoice_id")
        .agg(
            F.count("*").alias("journal_count"),
            F.sum("debit_dec").alias("_debit_wide"),
            F.sum("credit_dec").alias("_credit_wide"),
            F.sum(F.col("account_merchant").isNull().cast("int")).alias(
                "bad_journal_accounts"
            ),
            F.countDistinct("account_merchant").alias("journal_merchants"),
            F.max("account_merchant").alias("journal_merchant"),
            F.countDistinct(F.when(F.col("debit_dec") > 0, F.col("account_id"))).alias(
                "debit_accounts"
            ),
            F.max(F.when(F.col("debit_dec") > 0, F.col("account_id"))).alias(
                "debit_account"
            ),
        )
        .withColumn(
            "ledger_debit_dec", F.expr("try_cast(_debit_wide AS DECIMAL(18,4))")
        )
        .withColumn(
            "ledger_credit_dec", F.expr("try_cast(_credit_wide AS DECIMAL(18,4))")
        )
    )
    base = (
        invoice_ids.select(F.col("lookup_key").alias("invoice_id"))
        .join(invoices, "invoice_id", "left")
        .join(gateways, "invoice_id", "left")
        .join(journal_totals, "invoice_id", "left")
        .withColumn("gateway_ref", F.col("g.gateway_ref"))
        .join(counts, "gateway_ref", "left")
        .join(banks, "gateway_ref", "left")
        .join(accounts, F.col("invoice_account_id") == F.col("account_id"), "left")
        .fillna(0, ["gateway_count", "bank_count", "journal_count", "reference_count"])
    )
    checks = [
        (F.col("invoice_key").isNotNull(), "MISSING_INVOICE"),
        (F.col("source_status") == "PAID", "INVOICE_NOT_PAID"),
        (F.col("gateway_count") > 0, "MISSING_GATEWAY"),
        (F.col("gateway_count") <= 1, "AMBIGUOUS_GATEWAY"),
        (F.length("gateway_ref") > 0, "MISSING_GATEWAY_REFERENCE"),
        (F.col("reference_count") <= 1, "AMBIGUOUS_GATEWAY_REFERENCE"),
        (F.col("bank_count") > 0, "MISSING_BANK"),
        (F.col("bank_count") <= 1, "AMBIGUOUS_BANK"),
        (F.col("journal_count") >= 2, "MISSING_JOURNAL_LEGS"),
        (
            F.col("ledger_debit_dec").isNotNull()
            & F.col("ledger_credit_dec").isNotNull(),
            "LEDGER_AMOUNT_OVERFLOW",
        ),
        (F.col("ledger_debit_dec") == F.col("ledger_credit_dec"), "UNBALANCED_LEDGER"),
        (
            (F.col("invoice_amount_dec") == F.col("g.amount_dec"))
            & (F.col("g.amount_dec") == F.col("b.amount_dec"))
            & (F.col("invoice_amount_dec") == F.col("ledger_debit_dec")),
            "AMOUNT_MISMATCH",
        ),
        (
            (F.col("invoice_currency") == F.col("g.currency"))
            & (F.col("g.currency") == F.col("b.currency")),
            "CURRENCY_MISMATCH",
        ),
        (
            F.col("account_merchant").isNotNull()
            & (F.col("bad_journal_accounts") == 0),
            "MISSING_OR_INACTIVE_ACCOUNT",
        ),
        (
            (F.col("account_merchant") == F.col("g.merchant_id"))
            & (F.col("g.merchant_id") == F.col("b.merchant_id"))
            & (F.col("journal_merchants") == 1)
            & (F.col("journal_merchant") == F.col("account_merchant")),
            "MERCHANT_MISMATCH",
        ),
        (
            (F.col("debit_accounts") == 1)
            & (F.col("debit_account") == F.col("invoice_account_id")),
            "DEBIT_ACCOUNT_MISMATCH",
        ),
    ]
    return base.select(
        F.concat(F.lit("INVOICE:"), F.col("invoice_id")).alias("item_id"),
        F.lit("INVOICE").alias("item_type"),
        "invoice_id",
        F.col("g.key").alias("gateway_key"),
        F.col("b.key").alias("settlement_id"),
        "gateway_ref",
        F.coalesce("account_merchant", "g.merchant_id", "b.merchant_id").alias(
            "merchant_id"
        ),
        F.coalesce("invoice_currency", "g.currency", "b.currency").alias("currency"),
        F.coalesce("invoice_amount_dec", "g.amount_dec", "b.amount_dec").alias(
            "amount_dec"
        ),
        F.col("g.amount_dec").alias("gateway_amount_dec"),
        F.col("b.amount_dec").alias("bank_amount_dec"),
        "ledger_debit_dec",
        "ledger_credit_dec",
        "gateway_count",
        "bank_count",
        "journal_count",
        F.lit(None).cast("string").alias("remittance_memo"),
        F.array_compact(
            F.array(
                *[
                    F.when(~F.coalesce(condition, F.lit(False)), F.lit(reason))
                    for condition, reason in checks
                ]
            )
        ).alias("reasons"),
        F.lit(False).alias("_remove"),
    )


def bank_results(spark, cache, references):
    from pyspark.sql import functions as F

    rows = (
        lookup(spark, cache, "bank", references, active=False)
        .join(reference_counts(spark, cache, references), "gateway_ref", "left")
        .fillna(0, ["reference_count"])
    )
    applied = (
        linked_banks(spark, cache, references)
        .select("key")
        .distinct()
        .withColumn("_linked", F.lit(True))
    )
    rows = rows.join(applied, "key", "left").fillna(False, ["_linked"])
    return rows.select(
        F.concat(F.lit("BANK:"), F.col("key")).alias("item_id"),
        F.lit("BANK").alias("item_type"),
        F.lit(None).cast("string").alias("invoice_id"),
        F.lit(None).cast("string").alias("gateway_key"),
        F.col("key").alias("settlement_id"),
        "gateway_ref",
        "merchant_id",
        "currency",
        "amount_dec",
        F.lit(None).cast("decimal(18,4)").alias("gateway_amount_dec"),
        F.col("amount_dec").alias("bank_amount_dec"),
        F.lit(None).cast("decimal(18,4)").alias("ledger_debit_dec"),
        F.lit(None).cast("decimal(18,4)").alias("ledger_credit_dec"),
        F.col("reference_count").alias("gateway_count"),
        F.lit(1).cast("long").alias("bank_count"),
        F.lit(0).cast("long").alias("journal_count"),
        "remittance_memo",
        F.array(
            F.when(~F.col("active"), F.lit("SOURCE_DELETED"))
            .when(
                F.length("gateway_ref").isNull() | (F.length("gateway_ref") == 0),
                F.lit("MISSING_GATEWAY_REFERENCE"),
            )
            .when(F.col("reference_count") > 1, F.lit("AMBIGUOUS_GATEWAY_REFERENCE"))
            .otherwise(F.lit("MISSING_GATEWAY"))
        ).alias("reasons"),
        (F.col("active") & ((F.col("reference_count") == 1) | F.col("_linked"))).alias(
            "_remove"
        ),
    )


def commit_results(spark, rows, path, open_only=False):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    columns = [name for name in rows.columns if name != "_remove"]
    rows = rows.withColumn("first_seen_at", F.current_timestamp()).withColumn(
        "last_changed_at", F.current_timestamp()
    )
    ensure_delta(spark, rows.drop("_remove"), path, "item_type")
    remove = "s._remove" + (" OR s.status = 'MATCHED'" if open_only else "")
    updates = {name: f"s.{name}" for name in [*columns, "last_changed_at"]}
    DeltaTable.forPath(spark, str(path)).alias("t").merge(
        rows.alias("s"), "t.item_id = s.item_id AND t.item_type = s.item_type"
    ).whenMatchedDelete(condition=remove).whenMatchedUpdate(
        condition="s.content_hash != t.content_hash", set=updates
    ).whenNotMatchedInsert(
        condition=f"NOT ({remove})",
        values={
            name: f"s.{name}" for name in [*columns, "first_seen_at", "last_changed_at"]
        },
    ).execute()


def run(spark, root, dataset):
    from delta.tables import DeltaTable
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    cache = gold_path(root, dataset, "state_reconciliation_inputs")
    open_path = gold_path(root, dataset, "state_unreconciled_open_items")
    changes, current, sources = read_changes(spark, root, dataset)
    if changes is None:
        return {"source_changed": False, "evaluated_items": 0}
    checkpoints = []

    def cut(frame):
        # Bound repeated key-lookup plans within this run. Checkpoint contents
        # may spill to disk and are released after all Delta writes complete.
        materialized = frame.localCheckpoint(eager=True)
        checkpoints.append(materialized)
        return materialized

    changes = cut(changes)
    current = cut(current)
    results = None
    try:
        merge_inputs(spark, current, changes, cache)
        references = cut(
            keys(
                changes.where("kind IN ('gateway_ref', 'bank', 'bank_link')").select(
                    "lookup_key"
                ),
                changes.where("kind = 'bank_link'").select(
                    F.concat(F.lit("unreferenced:"), F.col("key"))
                ),
            )
        )
        account_keys = keys(changes.where("kind = 'account'").select("lookup_key"))
        invoice_ids = cut(
            keys(
                changes.select("invoice_id"),
                lookup(spark, cache, "gateway_ref", references).select("invoice_id"),
                lookup(spark, cache, "account_ref", account_keys).select("invoice_id"),
                spark.read.format("delta")
                .load(str(cache))
                .where("kind = 'bank_link'")
                .join(
                    changes.where("kind = 'bank'").select("key").distinct(),
                    "key",
                    "left_semi",
                )
                .select("invoice_id"),
            )
        )
        if DeltaTable.isDeltaTable(spark, str(open_path)):
            opened = spark.read.format("delta").load(str(open_path))
            invoice_ids = keys(
                invoice_ids, opened.where("item_type = 'INVOICE'").select("invoice_id")
            )
            open_banks = opened.where("item_type = 'BANK'").select(
                F.when(F.length("gateway_ref") > 0, F.col("gateway_ref"))
                .otherwise(F.concat(F.lit("unreferenced:"), F.col("settlement_id")))
                .alias("lookup_key")
            )
            references = keys(references, open_banks)
        # Includes all references of touched invoices, so an orphan bank is removed
        # when a delayed gateway finally supplies its invoice link.
        invoice_ids = cut(invoice_ids)
        references = cut(
            keys(
                references,
                lookup(spark, cache, "gateway", invoice_ids).select("gateway_ref"),
                spark.read.format("delta")
                .load(str(cache))
                .where("kind = 'bank_link'")
                .join(
                    invoice_ids.select(F.col("lookup_key").alias("invoice_id")),
                    "invoice_id",
                    "left_semi",
                )
                .select(F.concat(F.lit("unreferenced:"), F.col("key"))),
            )
        )
        results = invoice_results(spark, cache, invoice_ids, cut).unionByName(
            bank_results(spark, cache, references)
        )
        payload = [name for name in results.columns if name != "_remove"]
        results = results.withColumn(
            "status", F.when(F.size("reasons") == 0, "MATCHED").otherwise("OPEN")
        ).withColumn("content_hash", F.sha2(F.to_json(F.struct(*payload)), 256))
        results = results.persist(StorageLevel.MEMORY_AND_DISK)
        count = results.count()
        commit_results(spark, results, gold_path(root, dataset, "gold_reconciliation"))
        commit_results(spark, results, open_path, open_only=True)
        # Cursor acknowledgement comes last. Repeating an interrupted run safely
        # rebuilds the same evidence and results before advancing source cursors.
        commit_cursors(spark, root, dataset, sources)
        return {"source_changed": True, "evaluated_items": count}
    finally:
        if results is not None:
            results.unpersist()
        for frame in reversed(checkpoints):
            frame.unpersist()


if __name__ == "__main__":
    from .runtime import spark_session

    options = arguments()
    session = spark_session("FinRecon Gold reconciliation")
    try:
        print(
            json.dumps(run(session, options.lakehouse_dir, options.dataset), indent=2)
        )
    finally:
        session.stop()
