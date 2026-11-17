"""Dated INR reporting valuation using immutable books and decimal broadcast joins."""

import argparse
import hashlib
import json
import re
from pathlib import Path

from .contracts import evaluate_rules, load_contract
from .gold_inputs import gold_path
from .quality import atomic_json
from .silver import ensure_delta, table_path

FIELDS = (
    "rate_date",
    "currency",
    "reporting_currency",
    "rate",
    "rate_id",
    "provenance",
)
MAX_RATES = 10000
MAX_BYTES = 10 * 1024 * 1024
POLICY = "inr_gateway_date_half_even_4_v1"


class FxContractError(ValueError):
    pass


def load_book(spark, root, dataset, file):
    from delta.tables import DeltaTable
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    file = Path(file)
    if file.stat().st_size > MAX_BYTES:
        raise FxContractError("Rate book exceeds 10 MiB")
    payload = file.read_bytes()
    if len(payload) > MAX_BYTES:
        raise FxContractError("Rate book exceeds 10 MiB")
    book_id = hashlib.sha256(payload).hexdigest()
    snapshot = Path(root) / "fx_books" / dataset / f"{book_id}.csv"
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    if not snapshot.exists():
        temporary = snapshot.with_suffix(".tmp")
        temporary.write_bytes(payload)
        temporary.replace(snapshot)
    if hashlib.sha256(snapshot.read_bytes()).hexdigest() != book_id:
        raise FxContractError("Archived rate book hash mismatch")
    raw = (
        spark.read.schema(
            ",".join(f"{name} STRING" for name in (*FIELDS, "_corrupt_record"))
        )
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("multiLine", "true")
        .csv(str(snapshot))
    )
    rules, fingerprint = load_contract("fx")
    prepared = (
        raw.select(
            F.to_json(F.struct(*raw.columns)).alias("raw_payload"),
            F.trim("rate_date").alias("rate_date_text"),
            F.trim("rate").alias("rate_text"),
            *[
                F.trim(name).alias(name)
                for name in ("currency", "reporting_currency", "rate_id", "provenance")
            ],
            "_corrupt_record",
        )
        .withColumn("rate_date", F.expr("try_cast(rate_date_text AS DATE)"))
        .withColumn("fx_rate_dec", F.expr("try_cast(rate_text AS DECIMAL(18,4))"))
    )
    prepared = (
        evaluate_rules(prepared, rules)
        .withColumn("book_id", F.lit(book_id))
        .withColumn("contract_version", F.lit(fingerprint))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    try:
        count = prepared.limit(MAX_RATES + 1).count()
        if not 0 < count <= MAX_RATES:
            raise FxContractError("A rate book must contain 1-10000 rows")
        bad = prepared.where(F.size("failed_rules") > 0)
        if not bad.isEmpty():
            rows = (
                bad.withColumn(
                    "record_id",
                    F.sha2(F.to_json(F.struct("book_id", "raw_payload")), 256),
                )
                .select(
                    "record_id",
                    "book_id",
                    "raw_payload",
                    "failed_rules",
                    "contract_version",
                )
                .dropDuplicates(["record_id"])
            )
            path = table_path(root, dataset, "silver_fx_rate_quarantine")
            ensure_delta(spark, rows, path)
            DeltaTable.forPath(spark, str(path)).alias("t").merge(
                rows.alias("s"), "t.record_id = s.record_id"
            ).whenNotMatchedInsertAll().execute()
            raise FxContractError("Invalid rate rows quarantined; entire book rejected")
        duplicates = (
            prepared.groupBy("rate_date", "currency", "reporting_currency")
            .count()
            .where("count > 1")
        )
        if not duplicates.isEmpty():
            raise FxContractError("Duplicate dated currency pair; entire book rejected")
        rows = prepared.select(
            "book_id",
            "rate_date",
            "currency",
            "reporting_currency",
            "fx_rate_dec",
            "rate_id",
            "provenance",
            "contract_version",
        )
        path = table_path(root, dataset, "silver_fx_rates")
        ensure_delta(spark, rows, path, "book_id")
        DeltaTable.forPath(spark, str(path)).alias("t").merge(
            rows.alias("s"),
            "t.book_id = s.book_id AND t.rate_date = s.rate_date AND t.currency = s.currency AND t.reporting_currency = s.reporting_currency",
        ).whenNotMatchedInsertAll().execute()
        rates = (
            spark.read.format("delta")
            .load(str(path))
            .where(F.col("book_id") == book_id)
        )
        return rates, book_id, count
    finally:
        prepared.unpersist()


def convert(payments, rates):
    """One direct quote per exact UTC date. No inverse, prior-day, or float path."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    if payments.schema["amount_dec"].dataType != DecimalType(18, 4) or rates.schema[
        "fx_rate_dec"
    ].dataType != DecimalType(18, 4):
        raise FxContractError("Money and rates must use DECIMAL(18,4)")
    joined = payments.alias("p").join(
        F.broadcast(rates).alias("r"),
        (F.col("p.currency") == F.col("r.currency"))
        & (F.col("p.business_date") == F.col("r.rate_date"))
        & (F.col("r.reporting_currency") == F.lit("INR")),
        "left",
    )
    rows = joined.select(
        "p.*",
        F.col("r.reporting_currency").alias("_reporting_currency"),
        "r.fx_rate_dec",
        "r.rate_id",
        "r.provenance",
    ).withColumn(
        "converted_amount_dec",
        F.expr("try_cast(bround(amount_dec * fx_rate_dec, 4) AS DECIMAL(18,4))"),
    )
    return (
        rows.withColumn("reporting_currency", F.lit("INR"))
        .withColumn(
            "valuation_status",
            F.when(F.col("business_date").isNull(), "MISSING_BUSINESS_DATE")
            .when(
                F.col("amount_dec").isNull() | (F.col("amount_dec") <= 0),
                "INVALID_NATIVE_AMOUNT",
            )
            .when(F.col("fx_rate_dec").isNull(), "MISSING_RATE")
            .when(F.col("converted_amount_dec").isNull(), "AMOUNT_OVERFLOW")
            .otherwise("VALUED"),
        )
        .drop("_reporting_currency")
    )


def snapshot_rows(spark, path):
    from delta.tables import DeltaTable

    table = DeltaTable.forPath(spark, str(path))
    identity = table.detail().first().id
    version = table.history(1).first().version
    return spark.read.format("delta").option("versionAsOf", version).load(str(path)), {
        "id": identity,
        "version": version,
    }


def run(spark, root, dataset, rates_file):
    from delta.tables import DeltaTable
    from pyspark import StorageLevel
    from pyspark.sql import functions as F

    rates, book_id, rate_count = load_book(spark, root, dataset, rates_file)
    decisions, gold_source = snapshot_rows(
        spark, gold_path(root, dataset, "gold_reconciliation")
    )
    gateways, gateway_source = snapshot_rows(
        spark, table_path(root, dataset, "silver_gateway_events")
    )
    matched = decisions.where("status = 'MATCHED'").select(
        "item_id",
        "invoice_id",
        "gateway_key",
        "currency",
        "amount_dec",
        F.col("content_hash").alias("reconciliation_hash"),
    )
    if (
        not matched.groupBy("item_id")
        .count()
        .where("count > 1 OR item_id IS NULL")
        .isEmpty()
    ):
        raise FxContractError("Duplicate or missing matched decision key")
    metadata = gateways.select(
        F.col("entity_key").alias("gateway_key"),
        F.col("event_date").alias("business_date"),
    )
    metadata = metadata.join(
        matched.select("gateway_key").distinct(), "gateway_key", "left_semi"
    )
    if not metadata.groupBy("gateway_key").count().where("count > 1").isEmpty():
        raise FxContractError("Ambiguous gateway business date")
    payments = matched.join(metadata, "gateway_key", "left")
    rows = (
        convert(payments, rates)
        .withColumn("book_id", F.lit(book_id))
        .withColumn("valuation_policy", F.lit(POLICY))
    )
    rows = rows.withColumn(
        "content_hash", F.sha2(F.to_json(F.struct(*rows.columns)), 256)
    ).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        counts = {
            row.valuation_status: row["count"]
            for row in rows.groupBy("valuation_status").count().collect()
        }
        total = (
            rows.where("valuation_status = 'VALUED'")
            .agg(
                F.expr("try_cast(sum(converted_amount_dec) AS DECIMAL(18,4))").alias(
                    "total"
                )
            )
            .first()
            .total
        )
        eligible = sum(counts.values())
        aggregate_representable = counts.get("VALUED", 0) == 0 or total is not None
        passed = counts.get("VALUED", 0) == eligible and aggregate_representable
        path = gold_path(root, dataset, "gold_fx_valuation")
        ensure_delta(
            spark,
            rows.withColumn("valued_at", F.current_timestamp()),
            path,
            "business_date",
        )
        values = {name: f"s.{name}" for name in rows.columns}
        values["valued_at"] = "current_timestamp()"
        DeltaTable.forPath(spark, str(path)).alias("t").merge(
            rows.alias("s"), "t.item_id = s.item_id"
        ).whenMatchedUpdate(
            condition="t.content_hash != s.content_hash", set=values
        ).whenNotMatchedInsert(values=values).whenNotMatchedBySourceDelete().execute()
        return {
            "dataset": dataset,
            "passed": passed,
            "book_id": book_id,
            "rate_rows": rate_count,
            "valuation_policy": POLICY,
            "reporting_currency": "INR",
            "eligible_payments": eligible,
            "status_counts": counts,
            "converted_total_dec": (
                format(total, ".4f") if total is not None else "0.0000"
            )
            if passed
            else None,
            "aggregate_representable": aggregate_representable,
            "gold_source": gold_source,
            "gateway_source": gateway_source,
            "broadcast_requested": True,
            "contains_synthetic_rates": not rates.where(
                F.upper("provenance").contains("SYNTHETIC")
            ).isEmpty(),
        }
    finally:
        rows.unpersist()


def main():
    from .runtime import spark_session

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--lakehouse-dir", type=Path, default=Path("/lakehouse"))
    parser.add_argument("--rates-file", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", args.dataset):
        parser.error("Invalid dataset name")
    report = (
        args.report
        or Path("/workspace/pipelines/output") / args.dataset / "fx_run.json"
    )
    spark = spark_session("FinRecon dated FX valuation")
    try:
        try:
            result = run(spark, args.lakehouse_dir, args.dataset, args.rates_file)
        except FxContractError as exc:
            result = {"dataset": args.dataset, "passed": False, "error": str(exc)}
        atomic_json(report, result)
        print(json.dumps(result, indent=2))
        return 0 if result["passed"] else 2
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
