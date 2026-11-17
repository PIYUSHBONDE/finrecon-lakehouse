"""Verify a fresh correlated fixture end to end, then replay in fresh containers.

This is a demo verifier: Silver's correction stage uses known synthetic fixture
truth. It never silently repairs production financial values. FX quotes generated
here are explicitly synthetic and must never be used as market data.
"""

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil

from pipelines.orchestrate import execute
from pipelines.quality import atomic_json

ROOT = Path(__file__).resolve().parents[1]
RATES = {
    "INR": "1.0000",
    "USD": "80.0000",
    "EUR": "90.0000",
    "GBP": "100.0000",
    "JPY": "0.5000",
}


def container_path(path):
    return "/workspace/" + Path(path).resolve().relative_to(ROOT).as_posix()


def synthetic_book(canonical, path):
    keys = sorted({(row["event_ts"][:10], row["currency"]) for row in canonical})
    with Path(path).open("x", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(
            (
                "rate_date",
                "currency",
                "reporting_currency",
                "rate",
                "rate_id",
                "provenance",
            )
        )
        for date, currency in keys:
            writer.writerow(
                (
                    date,
                    currency,
                    "INR",
                    RATES[currency],
                    f"SYNTH-{currency}-{date}",
                    "SYNTHETIC_TEST_NOT_MARKET_DATA",
                )
            )


def run(source_report, dataset, docker):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", dataset):
        raise ValueError("Invalid dataset name")
    source_report = Path(source_report).resolve()
    container_path(source_report)
    source = json.loads(source_report.read_text(encoding="utf-8"))
    canonical = json.loads(
        (source_report.parent / "canonical_transactions.json").read_text(
            encoding="utf-8"
        )
    )
    count = len(canonical)
    if (
        count < 100
        or count % 100
        or len({row["invoice_id"] for row in canonical}) != count
    ):
        raise ValueError(
            "Expected unique correlated fixtures in complete 100-row blocks"
        )
    if (
        source["correlated_invoices"] != count
        or not source["lost_acknowledgement_replay_preserved_postings"]
    ):
        raise ValueError("Source verification has not passed")
    output = ROOT / "pipelines/output" / dataset
    # Never reuse an unrelated fixture's verification directory or checkpoints.
    output.mkdir(parents=True, exist_ok=False)
    rates = output / "fx_synthetic.csv"
    synthetic_book(canonical, rates)
    compose = [
        docker,
        "compose",
        "-f",
        str(ROOT / "docker/docker-compose.yml"),
        "--profile",
        "compute",
        "run",
        "--rm",
        "--no-deps",
        "bronze",
        "python3",
        "-m",
    ]
    common = ["--dataset", dataset]

    def command(module, extra=()):
        return [*compose, "pipelines." + module, *common, *extra]

    commands = {
        "bronze": command(
            "verify_bronze", ["--source-report", container_path(source_report)]
        ),
        "silver": command("verify_silver"),
        "cdc": command("verify_cdc"),
        "gold": command("verify_gold"),
        "exceptions": command("verify_exceptions"),
        "fx": command("verify_fx", ["--rates-file", container_path(rates)]),
    }
    stages = [("first_" + name, arguments) for name, arguments in commands.items()]
    # Memo extraction changes Gold after its first proof. Establish that settled
    # state before demanding an identical fresh-process replay digest.
    stages.append(("settled_gold", commands["gold"]))
    stages.append(
        (
            "pipeline",
            command(
                "orchestrate",
                [
                    "--gateway-topics",
                    source["gateway_topic"],
                    "--bank-dir",
                    container_path(source_report.parent / "bank_settlements"),
                    "--fx-rates",
                    container_path(rates),
                ],
            ),
        )
    )
    stages.extend(("replay_" + name, arguments) for name, arguments in commands.items())
    started = datetime.now(timezone.utc)
    execute(stages, output / "readiness_run", attempts=1)
    proofs = {
        name: json.loads((output / file).read_text(encoding="utf-8"))
        for name, file in {
            "bronze": "verification.json",
            "silver": "silver_verification.json",
            "cdc": "cdc_verification.json",
            "gold": "gold_verification.json",
            "exceptions": "exception_verification.json",
            "fx": "fx_verification.json",
        }.items()
    }
    checks = {
        "bronze_fresh_process": proofs["bronze"][
            "previous_process_checkpoint_verified"
        ],
        "silver_fresh_process": proofs["silver"][
            "previous_process_checkpoint_verified"
        ],
        "cdc_fresh_process": proofs["cdc"]["previous_process_checkpoint_verified"],
        "gold_fresh_process": proofs["gold"]["previous_process_state_verified"],
        "exceptions_fresh_process": proofs["exceptions"][
            "previous_process_state_verified"
        ],
        "fx_fresh_process": proofs["fx"]["previous_process_state_verified"],
        "all_fixture_payments_matched": proofs["gold"]["correlated_matched"] == count,
        "no_fixture_open_invoices": proofs["gold"]["correlated_open_invoices"] == 0,
        "decimal_oracle": proofs["fx"]["independent_decimal_oracle_passed"],
    }
    report = {
        "passed": all(checks.values()),
        "dataset": dataset,
        "payments": count,
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
        "source_report": str(source_report),
        "historical_open_items_retained": proofs["exceptions"][
            "remaining_historical_open_items"
        ],
        "retry_clock": source["retry_clock"],
        "fx_quotes": "SYNTHETIC_TEST_NOT_MARKET_DATA",
        "scope": "single-host local end-to-end correctness and process restart; not capacity certification",
    }
    atomic_json(output / "readiness.json", report)
    if not report["passed"]:
        raise RuntimeError("Readiness assertions failed: " + json.dumps(checks))
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-report", type=Path, required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--docker", default=shutil.which("docker") or "docker")
    args = parser.parse_args()
    print(json.dumps(run(args.source_report, args.dataset, args.docker), indent=2))


if __name__ == "__main__":
    main()
