"""Deterministic memo extraction and exact financial validation, with no paid API."""

import argparse
import json
import re
from pathlib import Path

from .exception_links import banks_with_links, ensure_links
from .gold import invoice_results, keys
from .gold_inputs import SCHEMA, gold_path, lookup
from .quality import atomic_json
from .silver import ensure_delta

METHOD = "invoice_uuid_regex_v1"
TOKEN = r"(?<![A-Za-z0-9_-])(INV-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})(?![A-Za-z0-9_-])"


def extract(frame):
    from pyspark.sql import functions as F

    return frame.withColumn(
        "candidates",
        F.array_distinct(
            F.regexp_extract_all(
                F.coalesce("remittance_memo", F.lit("")), F.lit(TOKEN), 1
            )
        ),
    ).withColumn(
        "candidate_invoice_id",
        F.when(F.size("candidates") == 1, F.element_at("candidates", 1)),
    )


def propose(spark, root, dataset):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    cache = gold_path(root, dataset, "state_reconciliation_inputs")
    link_path = ensure_links(spark, root, dataset)
    opened = spark.read.format("delta").load(
        str(gold_path(root, dataset, "state_unreconciled_open_items"))
    )
    bank_keys = keys(
        opened.where("item_type = 'BANK'")
        .where(F.array_contains("reasons", "MISSING_GATEWAY_REFERENCE"))
        .select(F.concat(F.lit("unreferenced:"), F.col("settlement_id")))
    )
    checkpoints = []

    def cut(frame):
        value = frame.localCheckpoint(eager=True)
        checkpoints.append(value)
        return value

    try:
        banks = cut(extract(lookup(spark, cache, "bank", bank_keys)))
        if banks.isEmpty():
            return {
                "targeted_banks": 0,
                "validated_links": 0,
                "rejected": 0,
                "method": METHOD,
            }
        invoice_ids = cut(keys(banks.select("candidate_invoice_id")))
        gateways = lookup(spark, cache, "gateway", invoice_ids)
        gateways = gateways.groupBy("invoice_id").agg(
            F.count("*").alias("gateway_candidates"),
            F.max(F.struct("key", "gateway_ref")).alias("gateway"),
        )
        proposals = cut(
            banks.alias("b")
            .join(
                gateways.alias("g"),
                F.col("b.candidate_invoice_id") == F.col("g.invoice_id"),
                "left",
            )
            .select(
                "b.*",
                "gateway_candidates",
                F.col("g.gateway.key").alias("proposed_gateway_key"),
                F.col("g.gateway.gateway_ref").alias("proposed_ref"),
            )
        )
        hypothetical = proposals.where(
            "size(candidates) = 1 AND gateway_candidates = 1 AND length(proposed_ref) > 0"
        ).select(
            *[
                F.col("proposed_ref").alias(name)
                if name in ("gateway_ref", "lookup_key")
                else F.col(name)
                for name in lookup(spark, cache, "bank", bank_keys).columns
            ]
        )
        hypothetical = cut(hypothetical)

        def bank_reader(session, path, references):
            existing = banks_with_links(session, path, references).join(
                hypothetical.select("key"), "key", "left_anti"
            )
            candidates = hypothetical.join(references, "lookup_key", "left_semi")
            return existing.unionByName(candidates)

        decisions = cut(
            invoice_results(spark, cache, invoice_ids, cut, bank_reader).select(
                "invoice_id",
                F.col("settlement_id").alias("matched_bank"),
                F.col("reasons").alias("financial_reasons"),
            )
        )
        evaluated = proposals.join(
            decisions, proposals.candidate_invoice_id == decisions.invoice_id, "left"
        )
        evaluated = evaluated.withColumn(
            "rejection_reasons",
            F.when(F.size("candidates") == 0, F.array(F.lit("NO_INVOICE_TOKEN")))
            .when(F.size("candidates") > 1, F.array(F.lit("MULTIPLE_INVOICE_TOKENS")))
            .when(
                F.col("gateway_candidates").isNull()
                | (F.col("gateway_candidates") != 1),
                F.array(F.lit("MISSING_OR_AMBIGUOUS_GATEWAY")),
            )
            .when(F.size("financial_reasons") > 0, F.col("financial_reasons"))
            .when(
                F.col("matched_bank").isNull()
                | (F.col("matched_bank") != F.col("key")),
                F.array(F.lit("UNVERIFIED_BANK_LINK")),
            )
            .otherwise(F.array().cast("array<string>")),
        )
        evaluated = cut(
            evaluated.withColumn(
                "proposal_id",
                F.sha2(
                    F.to_json(
                        F.struct(
                            F.lit(METHOD).alias("method"),
                            "key",
                            "content_hash",
                            "candidates",
                            "proposed_gateway_key",
                            "proposed_ref",
                            "rejection_reasons",
                        )
                    ),
                    256,
                ),
            )
        )
        audit = evaluated.select(
            "proposal_id",
            F.col("key").alias("settlement_id"),
            "record_id",
            F.col("content_hash").alias("bank_evidence_hash"),
            "remittance_memo",
            "candidates",
            "candidate_invoice_id",
            "proposed_gateway_key",
            "proposed_ref",
            "rejection_reasons",
            F.lit(METHOD).alias("method"),
            F.when(F.size("rejection_reasons") == 0, "VALIDATED")
            .otherwise("REJECTED")
            .alias("decision"),
            F.current_timestamp().alias("evaluated_at"),
        )
        audit_path = gold_path(root, dataset, "exception_extraction_audit")
        ensure_delta(spark, audit, audit_path)
        # VALIDATED is a proposal decision, not an assertion that Gold committed.
        DeltaTable.forPath(spark, str(audit_path)).alias("t").merge(
            audit.alias("s"), "t.proposal_id = s.proposal_id"
        ).whenNotMatchedInsertAll().execute()
        accepted = evaluated.where("size(rejection_reasons) = 0")
        links = accepted.select(
            *[
                F.lit("bank_link").alias(name)
                if name == "kind"
                else F.col("key").alias(name)
                if name == "entity_key"
                else F.col("proposed_gateway_key").alias(name)
                if name == "account_id"
                else F.col("candidate_invoice_id").alias(name)
                if name == "invoice_id"
                else F.col("proposed_ref").alias(name)
                if name in ("gateway_ref", "lookup_key")
                else F.col("content_hash").alias(name)
                if name == "source_status"
                else F.col("proposal_id").alias(name)
                if name == "proposal_hash"
                else F.col(name)
                for name in [*SCHEMA, "entity_key", "proposal_hash"]
            ]
        )
        DeltaTable.forPath(spark, str(link_path)).alias("t").merge(
            links.alias("s"), "t.entity_key = s.entity_key"
        ).whenMatchedUpdateAll(
            condition="t.proposal_hash != s.proposal_hash"
        ).whenNotMatchedInsertAll().execute()
        total, valid = banks.count(), accepted.count()
        return {
            "targeted_banks": total,
            "validated_links": valid,
            "rejected": total - valid,
            "method": METHOD,
        }
    finally:
        for frame in reversed(checkpoints):
            frame.unpersist()


def main():
    from .gold import run
    from .runtime import spark_session

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--lakehouse-dir", type=Path, default=Path("/lakehouse"))
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", args.dataset):
        parser.error("Invalid dataset name")
    spark = spark_session("FinRecon memo exceptions")
    try:
        run(spark, args.lakehouse_dir, args.dataset)
        result = propose(spark, args.lakehouse_dir, args.dataset)
        result["gold"] = run(spark, args.lakehouse_dir, args.dataset)
        output = (
            args.report
            or Path("/workspace/pipelines/output")
            / args.dataset
            / "exception_extraction.json"
        )
        atomic_json(output, result)
        print(json.dumps(result, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
