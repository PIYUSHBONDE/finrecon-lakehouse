"""Preserve Kafka data while adopting a named volume, with an isolated restore drill.

Use prepare, then change only the Kafka data mount in Compose, then cutover.
No source volume, archive, or recovery-probe volume is deleted. A failed prepare
restarts original services. A failed cutover leaves a report and rollback override.
Stop local producers and compute jobs first. Never run two recoveries concurrently.
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import urllib.request
from uuid import uuid4

from pipelines.quality import atomic_json

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "docker/docker-compose.yml"
IMAGE = "finrecon-spark:3.5.7-delta3.3.2"
BROKER_IMAGE = "confluentinc/cp-kafka:7.5.0"


def command(*args):
    result = subprocess.run(args, capture_output=True, text=True, encoding="utf-8")
    if result.returncode:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {args!r}\n{result.stderr[-4000:]}"
        )
    return result.stdout.strip()


def connector_status():
    with urllib.request.urlopen(
        "http://localhost:8083/connectors/postgres-ledger-connector/status", timeout=5
    ) as response:
        return json.load(response)


def wait_for(check, seconds=120):
    deadline = time.monotonic() + seconds
    error = None
    while time.monotonic() < deadline:
        try:
            return check()
        except (RuntimeError, OSError, ValueError) as caught:
            error = caught
            time.sleep(2)
    raise RuntimeError(f"Readiness timeout: {error}")


def running_connector():
    status = connector_status()
    if (
        status["connector"]["state"] != "RUNNING"
        or not status["tasks"]
        or any(task["state"] != "RUNNING" for task in status["tasks"])
    ):
        raise RuntimeError(
            "Debezium connector or task is not running: " + json.dumps(status)
        )
    return status


def snapshot(docker, container):
    def cli(*args):
        return command(docker, "exec", container, *args)

    offsets = {}
    for boundary, flag in (("earliest", "-2"), ("latest", "-1")):
        raw = cli(
            "kafka-get-offsets", "--bootstrap-server", "127.0.0.1:29092", "--time", flag
        )
        offsets[boundary] = {}
        for line in raw.splitlines():
            match = re.fullmatch(r"([^:]+):(\d+):(\d+)", line)
            if not match:
                raise ValueError("Unexpected offset output: " + line)
            topic, partition, value = match.groups()
            offsets[boundary][f"{topic}:{partition}"] = int(value)
    described = cli(
        "kafka-topics", "--bootstrap-server", "127.0.0.1:29092", "--describe"
    )
    topics = dict(re.findall(r"Topic: (\S+)\s+TopicId: (\S+)", described))
    if not topics or not offsets["latest"]:
        raise ValueError("Broker returned no topics or offsets")
    meta = cli("cat", "/var/lib/kafka/data/meta.properties")
    cluster = re.search(r"^cluster.id=(.+)$", meta, re.MULTILINE).group(1)
    return {"cluster_id": cluster, "topics": topics, **offsets}


def preserved(before, after):
    if before != after:
        raise RuntimeError(
            "Kafka topic IDs, cluster identity, or offsets changed during recovery"
        )


def helper(docker, action, volume, directory):
    return command(
        docker,
        "run",
        "--rm",
        "--network",
        "none",
        "--user",
        "0",
        "--entrypoint",
        "python3",
        "--env",
        "PYTHONDONTWRITEBYTECODE=1",
        "--mount",
        f"type=bind,source={ROOT},target=/workspace,readonly",
        "--mount",
        f"type=bind,source={directory},target=/backup"
        + (",readonly" if action == "restore" else ""),
        "--mount",
        f"type=volume,source={volume},target=/data"
        + (",readonly" if action == "backup" else ""),
        "--workdir",
        "/workspace",
        IMAGE,
        "-m",
        "operations.volume_archive",
        action,
    )


def prepare(docker, directory):
    info = json.loads(command(docker, "inspect", "finrecon_kafka"))[0]
    if not info["State"]["Running"]:
        raise ValueError("Start original Kafka before preparing recovery")
    project = info["Config"]["Labels"]["com.docker.compose.project"]
    source = next(
        m["Name"] for m in info["Mounts"] if m["Destination"] == "/var/lib/kafka/data"
    )
    target = project + "_kafka_data"
    if source == target:
        raise ValueError(
            "Kafka already uses its named data volume; use the retained recovery report"
        )
    existing = command(
        docker, "volume", "ls", "--format", "{{.Name}}", "--filter", f"name=^{target}$"
    )
    if existing:
        raise ValueError(
            "Target already exists; inspect the prior recovery report before retrying"
        )
    active = command(
        docker,
        "ps",
        "--filter",
        f"label=com.docker.compose.project={project}",
        "--filter",
        "label=com.docker.compose.service=bronze",
        "--format",
        "{{.Names}}",
    )
    if active:
        raise ValueError("Stop compute writers first: " + active)
    original = COMPOSE.read_bytes()
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "compose.original.yml").write_bytes(original)
    state = {
        "passed": False,
        "phase": "preflight",
        "source_volume": source,
        "target_volume": target,
        "project": project,
    }
    report = directory / "recovery.json"
    rollback = {
        "services": {"kafka": {"volumes": ["kafka_original:/var/lib/kafka/data"]}},
        "volumes": {"kafka_original": {"external": True, "name": source}},
    }
    atomic_json(directory / "rollback.compose.json", rollback)
    atomic_json(report, state)
    probe = "finrecon_recovery_" + uuid4().hex[:10]
    stopped = False
    try:
        state["connector_before"] = running_connector()
        command(docker, "stop", "--time", "60", "finrecon_debezium")
        stopped = True
        state["before"] = snapshot(docker, "finrecon_kafka")
        command(docker, "stop", "--time", "60", "finrecon_kafka")
        state["phase"] = "offline_backup"
        atomic_json(report, state)
        state["backup"] = json.loads(helper(docker, "backup", source, directory))
        command(
            docker,
            "volume",
            "create",
            "--label",
            f"com.docker.compose.project={project}",
            "--label",
            "com.docker.compose.volume=kafka_data",
            "--label",
            f"finrecon.recovery.source={source}",
            target,
        )
        state["target_restore"] = json.loads(
            helper(docker, "restore", target, directory)
        )
        probe_volume = probe + "_data"
        command(
            docker,
            "volume",
            "create",
            "--label",
            "finrecon.recovery.probe=true",
            probe_volume,
        )
        state["probe_volume"] = probe_volume
        state["probe_restore"] = json.loads(
            helper(docker, "restore", probe_volume, directory)
        )
        environment = {
            "KAFKA_NODE_ID": "1",
            "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_LISTENERS": "PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093",
            "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://127.0.0.1:29092",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@127.0.0.1:9093",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_HEAP_OPTS": "-Xms512m -Xmx512m",
            "CLUSTER_ID": state["before"]["cluster_id"],
        }
        arguments = [
            docker,
            "run",
            "-d",
            "--rm",
            "--name",
            probe,
            "--network",
            "none",
            "--memory",
            "1536m",
            "--cpus",
            "1",
            "--mount",
            f"type=volume,source={probe_volume},target=/var/lib/kafka/data",
        ]
        for key, value in environment.items():
            arguments.extend(("--env", f"{key}={value}"))
        command(*arguments, BROKER_IMAGE)
        state["probe"] = wait_for(lambda: snapshot(docker, probe))
        preserved(state["before"], state["probe"])
        state["phase"] = "prepared"
        state["passed"] = True
    except BaseException as error:
        state.update(passed=False, error=repr(error))
        raise
    finally:
        try:
            if command(docker, "ps", "-q", "--filter", "name=^/" + probe + "$"):
                command(docker, "stop", "--time", "30", probe)
            if stopped:
                command(docker, "start", "finrecon_kafka")
                wait_for(lambda: snapshot(docker, "finrecon_kafka"))
                if state["phase"] != "prepared":
                    command(docker, "start", "finrecon_debezium")
                    state["connector_resumed"] = wait_for(running_connector)
                else:
                    state["connector_paused_for_cutover"] = True
        except BaseException as error:
            state.update(passed=False, resume_error=repr(error))
            if "error" not in state:
                raise
        finally:
            atomic_json(report, state)
    return state


def cutover(docker, directory):
    report = directory / "recovery.json"
    state = json.loads(report.read_text(encoding="utf-8"))
    if not state["passed"] or state["phase"] not in ("prepared", "complete"):
        raise ValueError("A verified preparation is required")
    if state["phase"] == "complete":
        mounts = json.loads(command(docker, "inspect", "finrecon_kafka"))[0]["Mounts"]
        if not any(
            m.get("Name") == state["target_volume"]
            and m["Destination"] == "/var/lib/kafka/data"
            for m in mounts
        ):
            raise RuntimeError("Completed migration's named mount has changed")
        running_connector()
        return state
    config = json.loads(
        command(docker, "compose", "-f", str(COMPOSE), "config", "--format", "json")
    )
    volume = next(
        v
        for v in config["services"]["kafka"]["volumes"]
        if v["target"] == "/var/lib/kafka/data"
    )
    if (
        volume["source"] != "kafka_data"
        or config["volumes"]["kafka_data"]["name"] != state["target_volume"]
    ):
        raise ValueError("Compose must explicitly select the prepared named volume")
    command(docker, "stop", "--time", "60", "finrecon_debezium")
    try:
        current = snapshot(docker, "finrecon_kafka")
        preserved(state["before"], current)
        state.update(phase="cutover", passed=False)
        atomic_json(report, state)
        command(
            docker,
            "compose",
            "-f",
            str(COMPOSE),
            "up",
            "-d",
            "--no-deps",
            "--force-recreate",
            "kafka",
        )
        state["after"] = wait_for(lambda: snapshot(docker, "finrecon_kafka"))
        preserved(state["before"], state["after"])
        mounts = json.loads(command(docker, "inspect", "finrecon_kafka"))[0]["Mounts"]
        state["named_mount_verified"] = any(
            m.get("Name") == state["target_volume"]
            and m["Destination"] == "/var/lib/kafka/data"
            for m in mounts
        )
        if not state["named_mount_verified"]:
            raise RuntimeError("Named volume is not mounted")
        command(docker, "volume", "inspect", state["source_volume"])
        state["source_volume_retained"] = True
    except BaseException as error:
        state.update(passed=False, error=repr(error))
        raise
    finally:
        try:
            command(docker, "start", "finrecon_debezium")
            state["connector_after"] = wait_for(running_connector)
            if "error" not in state:
                state.update(phase="complete", passed=True)
        except BaseException as error:
            state.update(passed=False, resume_error=repr(error))
            if "error" not in state:
                raise
        finally:
            atomic_json(report, state)
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "cutover"))
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--docker", default=shutil.which("docker") or "docker")
    args = parser.parse_args()
    directory = args.directory or ROOT / "pipelines/output/recovery" / datetime.now(
        timezone.utc
    ).strftime("%Y%m%dT%H%M%SZ")
    directory = directory.resolve()
    if not directory.is_relative_to(ROOT / "pipelines/output/recovery"):
        parser.error("Recovery reports must remain under pipelines/output/recovery")
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    state = globals()[args.action](args.docker, directory)
    print(
        json.dumps(
            {
                "passed": state["passed"],
                "phase": state["phase"],
                "report": str(directory / "recovery.json"),
            }
        )
    )


if __name__ == "__main__":
    main()
