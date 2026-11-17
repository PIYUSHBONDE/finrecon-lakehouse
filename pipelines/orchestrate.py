"""Run local stages sequentially with bounded retries, locking, and durable reports."""

import argparse
import contextlib
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

from .bronze import parse_args
from .quality import atomic_json


@contextlib.contextmanager
def dataset_lock(path):
    """OS-released lock survives process crashes without a stale-lock override."""
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another orchestrator holds the dataset lock") from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def execute(stages, directory, attempts=2, runner=subprocess.run):
    """Retry failed jobs; deterministic quality failures (exit 2) stop immediately."""
    directory.mkdir(parents=True, exist_ok=False)
    report = {
        "status": "RUNNING",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "stages": [
            {"name": name, "status": "PENDING", "attempts": []} for name, _ in stages
        ],
    }
    path = directory / "run.json"
    atomic_json(path, report)
    try:
        for (name, command), stage in zip(stages, report["stages"]):
            for attempt in range(1, attempts + 1):
                log = directory / f"{name}.{attempt}.log"
                stage["status"] = "RUNNING"
                record = {
                    "attempt": attempt,
                    "log": str(log),
                    "started_at": datetime.now(timezone.utc).isoformat(),
                }
                stage["attempts"].append(record)
                atomic_json(path, report)
                print(f"Running {name}, attempt {attempt}/{attempts}", flush=True)
                with log.open("w", encoding="utf-8") as output:
                    result = runner(
                        command, stdout=output, stderr=subprocess.STDOUT, check=False
                    )
                record.update(
                    exit_code=result.returncode,
                    finished_at=datetime.now(timezone.utc).isoformat(),
                )
                atomic_json(path, report)
                if result.returncode == 0:
                    stage["status"] = "SUCCEEDED"
                    break
                if result.returncode == 2 or attempt == attempts:
                    stage["status"] = "FAILED"
                    raise RuntimeError(f"{name} failed; inspect {log}")
            atomic_json(path, report)
        report["status"] = "SUCCEEDED"
    except BaseException as exc:
        report["status"] = (
            "INTERRUPTED" if isinstance(exc, KeyboardInterrupt) else "FAILED"
        )
        report["error"] = str(exc)
        for stage in report["stages"]:
            if stage["status"] == "PENDING":
                stage["status"] = "SKIPPED"
            elif stage["status"] == "RUNNING":
                stage["status"] = "FAILED"
        raise
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        atomic_json(path, report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--as-of-date",
        type=date.fromisoformat,
        default=datetime.now(timezone.utc).date(),
    )
    parser.add_argument("--attempts", type=int, choices=range(1, 4), default=2)
    parser.add_argument("--max-age-hours", type=int, default=72)
    parser.add_argument("--fx-rates", type=Path)
    options, bronze_argv = parser.parse_known_args(argv)
    if options.max_age_hours <= 0:
        parser.error("--max-age-hours must be positive")
    if options.fx_rates is not None and not options.fx_rates.is_file():
        parser.error("--fx-rates must be an existing rate book CSV inside the runner")
    args = parse_args(bronze_argv)
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex[:8]
    )
    directory = Path("/workspace/pipelines/output") / args.dataset / "runs" / run_id
    common = ["--dataset", args.dataset, "--lakehouse-dir", str(args.lakehouse_dir)]

    def command(module, extra=()):
        return [sys.executable, "-m", "pipelines." + module, *common, *extra]

    stages = [
        (
            "bronze",
            command(
                "bronze",
                [
                    "--bootstrap-servers",
                    args.bootstrap_servers,
                    "--gateway-topics",
                    args.gateway_topics,
                    "--cdc-topics",
                    args.cdc_topics,
                    "--bank-dir",
                    str(args.bank_dir),
                    "--max-offsets-per-trigger",
                    str(args.max_offsets_per_trigger),
                    "--max-files-per-trigger",
                    str(args.max_files_per_trigger),
                ],
            ),
        ),
        ("silver", command("silver", ["--as-of-date", options.as_of_date.isoformat()])),
        ("cdc", command("cdc")),
        (
            "silver_quality",
            command(
                "quality",
                [
                    "--layer",
                    "silver",
                    "--max-age-hours",
                    str(options.max_age_hours),
                    "--report",
                    str(directory / "silver_quality.json"),
                ],
            ),
        ),
        ("gold", command("gold")),
        (
            "exceptions",
            command("exceptions", ["--report", str(directory / "exceptions.json")]),
        ),
        (
            "gold_quality",
            command(
                "quality",
                ["--layer", "gold", "--report", str(directory / "gold_quality.json")],
            ),
        ),
    ]
    if options.fx_rates is not None:
        stages.append(
            (
                "fx_valuation",
                command(
                    "fx",
                    [
                        "--rates-file",
                        str(options.fx_rates.resolve()),
                        "--report",
                        str(directory / "fx.json"),
                    ],
                ),
            )
        )
    print(f"Run report: {directory / 'run.json'}", flush=True)
    with dataset_lock(args.lakehouse_dir / "run_locks" / f"{args.dataset}.lock"):
        execute(stages, directory, options.attempts)


if __name__ == "__main__":
    main()
