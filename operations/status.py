"""Read-only service and evidence checks for the local portfolio demo."""

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import shutil

from .kafka_recovery import command, running_connector

ROOT = Path(__file__).resolve().parents[1]


def evidence(path, max_age_hours, now=None):
    now = now or datetime.now(timezone.utc)
    record = json.loads(Path(path).read_text(encoding="utf-8"))
    finished = datetime.fromisoformat(record["finished_at"])
    return {
        "passed": record.get("passed") is True
        and now - timedelta(hours=max_age_hours) <= finished <= now,
        "path": str(path),
        "finished_at": record["finished_at"],
        "payments": record.get("payments"),
    }


def inspect(dataset, docker, max_age_hours=72):
    if (
        not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", dataset)
        or max_age_hours <= 0
    ):
        raise ValueError("Invalid dataset or proof age")
    checks = {}

    def check(name, function):
        try:
            value = function()
            checks[name] = {"passed": True, "value": value}
        except (RuntimeError, OSError, ValueError, KeyError) as error:
            checks[name] = {"passed": False, "error": str(error)}

    def mounts():
        containers = json.loads(
            command(docker, "inspect", "finrecon_postgres", "finrecon_kafka")
        )
        result = {}
        for container, path, suffix in zip(
            containers,
            ("/var/lib/postgresql/data", "/var/lib/kafka/data"),
            ("_pgdata", "_kafka_data"),
        ):
            if not container["State"]["Running"]:
                raise RuntimeError(container["Name"] + " is stopped")
            volume = next(m for m in container["Mounts"] if m["Destination"] == path)
            project = container["Config"]["Labels"]["com.docker.compose.project"]
            if volume["Type"] != "volume" or volume["Name"] != project + suffix:
                raise RuntimeError("Unexpected data mount for " + container["Name"])
            result[container["Name"]] = volume["Name"]
        return result

    check("persistent_source_volumes", mounts)
    check(
        "postgres",
        lambda: command(
            docker,
            "exec",
            "finrecon_postgres",
            "pg_isready",
            "-U",
            "finrecon",
            "-d",
            "finrecon",
        ),
    )
    check(
        "kafka",
        lambda: command(
            docker,
            "exec",
            "finrecon_kafka",
            "kafka-topics",
            "--bootstrap-server",
            "127.0.0.1:29092",
            "--list",
        ).splitlines(),
    )
    check("debezium", running_connector)
    check(
        "readiness_evidence",
        lambda: evidence(
            ROOT / "pipelines/output" / dataset / "readiness.json", max_age_hours
        ),
    )
    proof = checks.get("readiness_evidence", {}).get("value", {})
    if proof and not proof["passed"]:
        checks["readiness_evidence"]["passed"] = False
    return {
        "passed": all(row["passed"] for row in checks.values()),
        "dataset": dataset,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "checks": checks,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--max-proof-age-hours", type=int, default=72)
    parser.add_argument("--docker", default=shutil.which("docker") or "docker")
    args = parser.parse_args()
    report = inspect(args.dataset, args.docker, args.max_proof_age_hours)
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
