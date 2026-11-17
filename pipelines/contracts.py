"""Compile repository YAML contracts to native Spark expressions, without UDFs."""

import hashlib
import json
from datetime import timedelta
from pathlib import Path

CONTRACT_DIR = Path(__file__).resolve().parents[1] / "contracts"
SOURCES = {
    "gateway": {
        "bronze": "bronze_gateway_events",
        "silver": "silver_gateway_events",
        "key": "idempotency_key",
        "money": "amount_dec",
        "fields": (
            "event_id",
            "idempotency_key",
            "gateway_ref",
            "invoice_id",
            "merchant_id",
            "amount_dec",
            "currency",
            "event_ts",
        ),
        "business": (
            "event_id",
            "idempotency_key",
            "gateway_ref",
            "invoice_id",
            "amount_dec",
        ),
    },
    "bank": {
        "bronze": "bronze_bank_settlements",
        "silver": "silver_bank_settlements",
        "key": "settlement_id",
        "money": "settled_amount_dec",
        "fields": (
            "settlement_id",
            "gateway_ref",
            "merchant_id",
            "settled_amount_dec",
            "currency",
            "remittance_memo",
            "batch_date",
        ),
        "business": (
            "settlement_id",
            "gateway_ref",
            "settled_amount_dec",
            "remittance_memo",
            "batch_date",
        ),
    },
}
METADATA = {
    "topic": "string",
    "partition": "int",
    "offset": "long",
    "source_file": "string",
    "ingested_at": "timestamp",
}
COMMON_OUTPUT = (
    "entity_key",
    "record_id",
    "merchant_id",
    "currency",
    "event_ts",
    "event_date",
    "source_ts_ms",
    "content_hash",
    "raw_payload",
    *METADATA,
    "source_table",
    "contract_version",
)


def load_contract(source, directory=CONTRACT_DIR):
    import yaml

    if source not in SOURCES and source not in ("cdc", "fx"):
        raise ValueError(f"Unknown contract source: {source}")
    document = yaml.safe_load(
        (Path(directory) / f"{source}.yaml").read_text(encoding="utf-8")
    )
    if (
        not isinstance(document, dict)
        or document.get("version") != 1
        or document.get("source") != source
    ):
        raise ValueError("Contract must declare version 1 and the matching source")
    rules = document.get("rules")
    if not isinstance(rules, list) or not rules:
        raise ValueError("Contract requires at least one rule")
    names = set()
    for rule in rules:
        if (
            not isinstance(rule, dict)
            or not isinstance(rule.get("name"), str)
            or not isinstance(rule.get("expression"), str)
        ):
            raise ValueError("Each rule requires a name and SQL expression")
        if not rule["name"] or not rule["expression"] or rule["name"] in names:
            raise ValueError("Rule names must be unique and rules must not be empty")
        names.add(rule["name"])
    fingerprint = hashlib.sha256(
        json.dumps(document, sort_keys=True).encode()
    ).hexdigest()
    return rules, fingerprint


def output_columns(source):
    return [*COMMON_OUTPUT, *SOURCES[source]["business"]]


def evaluate_rules(frame, rules):
    from pyspark.sql import functions as F

    failures = [
        F.when(
            ~F.coalesce(F.expr(rule["expression"]), F.lit(False)), F.lit(rule["name"])
        )
        for rule in rules
    ]
    return frame.withColumn("failed_rules", F.array_compact(F.array(*failures)))


def prepare(
    frame, source, as_of_date, directory=CONTRACT_DIR, preserve_record_id=False
):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    spec = SOURCES[source]
    rules, fingerprint = load_contract(source, directory)
    schema = StructType(
        [StructField(name, StringType(), True) for name in spec["fields"]]
        + [StructField("_corrupt_record", StringType(), True)]
    )
    metadata = [
        F.col(name).cast(kind).alias(name)
        if name in frame.columns
        else F.lit(None).cast(kind).alias(name)
        for name, kind in METADATA.items()
    ]
    data = frame.select(
        "raw_payload",
        *metadata,
        (
            F.col("record_id") if preserve_record_id else F.lit(None).cast("string")
        ).alias("_original_record_id"),
        F.from_json("raw_payload", schema, {"mode": "PERMISSIVE"}).alias("_data"),
    )
    fields = []
    for name in spec["fields"]:
        alias = (
            "_amount_text"
            if name == spec["money"]
            else (
                "event_ts_text"
                if name == "event_ts"
                else "batch_date_text"
                if name == "batch_date"
                else name
            )
        )
        fields.append(F.trim(F.col(f"_data.{name}")).alias(alias))
    data = data.select(
        "raw_payload",
        "_original_record_id",
        *METADATA,
        *fields,
        (F.col("_data").isNotNull() & F.col("_data._corrupt_record").isNull()).alias(
            "json_valid"
        ),
    )
    data = data.withColumn(
        spec["money"], F.expr("try_cast(_amount_text AS DECIMAL(18,4))")
    )
    if source == "gateway":
        data = data.withColumn(
            "event_ts", F.expr("try_cast(event_ts_text AS TIMESTAMP)")
        )
    else:
        data = data.withColumn(
            "batch_date", F.expr("try_cast(batch_date_text AS DATE)")
        )
        data = data.withColumn("event_ts", F.col("batch_date").cast("timestamp"))
    data = (
        data.withColumn("event_date", F.to_date("event_ts"))
        .withColumn("source_ts_ms", F.unix_millis("event_ts"))
        .withColumn("entity_key", F.col(spec["key"]))
        .withColumn("source_table", F.lit(spec["bronze"]))
        .withColumn("contract_version", F.lit(fingerprint))
        .withColumn("as_of_date", F.lit(as_of_date).cast("date"))
        .withColumn("window_start", F.lit(as_of_date - timedelta(days=6)).cast("date"))
    )
    identity = F.when(
        F.col("topic").isNotNull(),
        F.to_json(
            F.struct(
                "source_table",
                "topic",
                "partition",
                "offset",
            )
        ),
    ).otherwise(F.to_json(F.struct("source_table", "source_file", "raw_payload")))
    data = data.withColumn(
        "record_id", F.coalesce("_original_record_id", F.sha2(identity, 256))
    )
    financial_fields = [
        "entity_key",
        "merchant_id",
        "currency",
        spec["money"],
        "event_ts",
        "gateway_ref",
    ]
    financial_fields += ["invoice_id"] if source == "gateway" else ["remittance_memo"]
    data = data.withColumn(
        "content_hash", F.sha2(F.to_json(F.struct(*financial_fields)), 256)
    )
    return evaluate_rules(data, rules)
