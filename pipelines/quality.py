"""Fail-closed local quality gates; unresolved business exceptions are metrics."""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .gold_inputs import TABLES, gold_path
from .silver import table_path


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")
    temporary.replace(path)


def evaluate(spark, root, dataset, layer, max_age_hours=72, now=None):
    import yaml
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType

    if layer not in ("silver", "gold") or max_age_hours <= 0:
        raise ValueError("Invalid quality gate options")
    now = now or datetime.now(timezone.utc)
    checks, metrics = [], {}

    def check(name, violations):
        checks.append(
            {"name": name, "violations": int(violations), "passed": violations == 0}
        )

    if layer == "silver":
        tables = {name: table_path(root, dataset, name) for name in TABLES.values()}
    else:
        tables = {
            name: gold_path(root, dataset, name)
            for name in ("gold_reconciliation", "state_unreconciled_open_items")
        }
    frames = {}
    for name, path in tables.items():
        frame = spark.read.format("delta").load(str(path))
        frame.createOrReplaceTempView(name)
        frames[name] = frame
        metrics[name + "_rows"] = frame.count()
        for field in frame.schema.fields:
            if field.name.endswith("_dec"):
                check(
                    name + "." + field.name + ".decimal18_4",
                    field.dataType != DecimalType(18, 4),
                )
    document = yaml.safe_load(
        (Path(__file__).resolve().parents[1] / "contracts/quality.yaml").read_text(
            encoding="utf-8"
        )
    )
    if document.get("version") != 1 or not document.get(layer):
        raise ValueError("Invalid quality contract")
    for rule in document[layer]:
        check(rule["name"], spark.sql(rule["sql"]).first().violations)
    if layer == "silver":
        for name in ("silver_dim_accounts_scd2", "silver_dim_invoices_scd2"):
            check(
                name + ".one_current",
                spark.sql(
                    f"SELECT count(*) AS n FROM (SELECT entity_key FROM {name} GROUP BY entity_key HAVING sum(CASE WHEN is_current THEN 1 ELSE 0 END) != 1 OR entity_key IS NULL)"
                )
                .first()
                .n,
            )
            check(
                name + ".version_intervals",
                spark.sql(
                    f"SELECT count(*) AS n FROM (SELECT *, lead(eff_start_version) OVER (PARTITION BY entity_key ORDER BY eff_start_version) AS next_version FROM {name}) WHERE NOT (eff_end_version <=> next_version) OR NOT (is_current <=> (eff_end_version IS NULL)) OR (eff_end_version IS NOT NULL AND eff_start_version >= eff_end_version)"
                )
                .first()
                .n,
            )
            check(
                name + ".unique_version",
                frames[name]
                .groupBy("version_id")
                .count()
                .where("count > 1 OR version_id IS NULL")
                .count(),
            )
        for name in (
            "bronze_gateway_events",
            "bronze_cdc_ledger",
            "bronze_bank_settlements",
        ):
            raw = spark.read.format("delta").load(
                str(Path(root) / "bronze" / dataset / name)
            )
            latest = raw.agg(F.max("ingested_at").alias("latest")).first().latest
            latest = latest.replace(tzinfo=timezone.utc) if latest else None
            metrics[name + "_latest_ingested_at"] = (
                latest.isoformat() if latest else None
            )
            check(
                name + ".freshness",
                latest is None
                or latest < now - timedelta(hours=max_age_hours)
                or latest > now + timedelta(minutes=5),
            )
        q = spark.read.format("delta").load(
            str(table_path(root, dataset, "silver_quarantine"))
        )
        metrics["quarantine_pending"] = q.where("status = 'PENDING'").count()
    else:
        metrics["matched_items"] = (
            frames["gold_reconciliation"].where("status = 'MATCHED'").count()
        )
        metrics["open_items"] = metrics["state_unreconciled_open_items_rows"]
    return {
        "dataset": dataset,
        "layer": layer,
        "checked_at": now.isoformat(),
        "passed": all(item["passed"] for item in checks),
        "checks": checks,
        "metrics": metrics,
    }


def main():
    from .runtime import spark_session

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--lakehouse-dir", type=Path, default=Path("/lakehouse"))
    parser.add_argument("--layer", choices=("silver", "gold"), required=True)
    parser.add_argument("--max-age-hours", type=int, default=72)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    session = spark_session("FinRecon quality gate")
    try:
        result = evaluate(
            session, args.lakehouse_dir, args.dataset, args.layer, args.max_age_hours
        )
    except Exception as exc:
        atomic_json(
            args.report, {"passed": False, "error": str(exc), "layer": args.layer}
        )
        raise
    else:
        atomic_json(args.report, result)
        print(json.dumps(result, indent=2))
        return 0 if result["passed"] else 2
    finally:
        session.stop()


if __name__ == "__main__":
    raise SystemExit(main())
