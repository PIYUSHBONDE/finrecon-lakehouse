"""Prove Bronze fails closed when a dedicated probe topic loses unread offsets.

Creates two unique, disposable topics and an isolated dataset. Record deletion
targets only the newly created gateway probe. Existing topics/data are untouched.
The failed checkpoint, topic names, logs, and Delta evidence remain for inspection.
"""

import argparse
import csv
from datetime import datetime, timezone
import json
import re
import shutil
import subprocess
from uuid import uuid4

from data_generator.chaos_producer import BANK_FIELDS
from pipelines.quality import atomic_json
from .kafka_recovery import IMAGE, ROOT, command


def owned_probe(topic):
    if not re.fullmatch(r"finrecon_retention_[0-9a-f]{12}_gateway", topic):
        raise ValueError("Record deletion is restricted to a generated retention probe")


def run(docker):
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic

    suffix = uuid4().hex[:12]
    dataset = "retention_" + suffix
    gateway = "finrecon_retention_" + suffix + "_gateway"
    cdc = "finrecon_retention_" + suffix + "_cdc"
    owned_probe(gateway)
    directory = ROOT / "pipelines/output" / dataset
    directory.mkdir(parents=True, exist_ok=False)
    bank = directory / "bank"
    bank.mkdir()
    with (bank / "probe.csv").open("x", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(BANK_FIELDS)
        writer.writerow(
            (
                "BANK-PROBE",
                "REF-PROBE",
                "MERCHANT_PROBE",
                "1.0000",
                "INR",
                "retention probe",
                datetime.now(timezone.utc).date().isoformat(),
            )
        )
    config = json.loads(
        command(
            docker,
            "compose",
            "-f",
            str(ROOT / "docker/docker-compose.yml"),
            "--profile",
            "compute",
            "config",
            "--format",
            "json",
        )
    )
    lakehouse = config["volumes"]["bronze_data"]["name"]
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    # create_topics must succeed for both topics before this run owns them.
    for future in admin.create_topics(
        [
            NewTopic(topic, num_partitions=1, replication_factor=1)
            for topic in (gateway, cdc)
        ]
    ).values():
        future.result(timeout=30)
    producer = Producer({"bootstrap.servers": "localhost:9092", "acks": "all"})

    def send(topic, count):
        errors = []
        for index in range(count):
            producer.produce(
                topic,
                key=str(index),
                value=json.dumps({"probe": index}),
                on_delivery=lambda error, message: errors.append(error)
                if error
                else None,
            )
        if producer.flush(30) or errors:
            raise RuntimeError("Probe delivery failed: " + repr(errors))

    def bronze(label):
        args = [
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
            "pipelines.bronze",
            "--dataset",
            dataset,
            "--gateway-topics",
            gateway,
            "--cdc-topics",
            cdc,
            "--bank-dir",
            "/workspace/" + bank.relative_to(ROOT).as_posix(),
            "--report",
            "/workspace/"
            + (directory / (label + ".json")).relative_to(ROOT).as_posix(),
        ]
        with (directory / (label + ".log")).open("w", encoding="utf-8") as stream:
            return subprocess.run(
                args, stdout=stream, stderr=subprocess.STDOUT, check=False
            ).returncode

    def table_manifest():
        script = (
            "import json; from operations.volume_archive import inventory; print(json.dumps(inventory('/lakehouse/bronze/"
            + dataset
            + "/bronze_gateway_events')))"
        )
        return json.loads(
            command(
                docker,
                "run",
                "--rm",
                "--network",
                "none",
                "--entrypoint",
                "python3",
                "--env",
                "PYTHONDONTWRITEBYTECODE=1",
                "--mount",
                f"type=bind,source={ROOT},target=/workspace,readonly",
                "--mount",
                f"type=volume,source={lakehouse},target=/lakehouse,readonly",
                IMAGE,
                "-c",
                script,
            )
        )

    state = {
        "passed": False,
        "dataset": dataset,
        "gateway_topic": gateway,
        "cdc_topic": cdc,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    report = directory / "retention_verification.json"
    atomic_json(report, state)
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "retention_" + suffix,
            "enable.auto.commit": False,
        }
    )
    try:
        send(gateway, 1)
        send(cdc, 1)
        if bronze("initial") != 0:
            raise RuntimeError("Initial probe ingestion failed")
        initial = json.loads((directory / "initial.json").read_text(encoding="utf-8"))
        if any(row["input_rows_this_run"] != 1 for row in initial["tables"].values()):
            raise RuntimeError("Probe did not checkpoint one row per source")
        before = table_manifest()
        send(gateway, 100)
        partition = TopicPartition(gateway, 0)
        low, high = consumer.get_watermark_offsets(partition, timeout=10)
        if (low, high) != (0, 101):
            raise RuntimeError("Unexpected dedicated probe offsets")
        # No deletion API accepts a caller-provided topic. This exact, newly
        # created UUID topic is the only permitted target, never business topics.
        owned_probe(gateway)
        for future in admin.delete_records(
            [TopicPartition(gateway, 0, high)], operation_timeout=30, request_timeout=35
        ).values():
            future.result(timeout=40)
        low, high = consumer.get_watermark_offsets(partition, timeout=10)
        if (low, high) != (101, 101):
            raise RuntimeError("Probe deletion did not advance the source boundary")
        exit_code = bronze("lost_offsets")
        log = (directory / "lost_offsets.log").read_text(encoding="utf-8")
        lower = log.lower()
        if exit_code == 0 or not any(
            marker in lower
            for marker in ("data loss", "data_loss", "data may have been lost")
        ):
            raise RuntimeError("Expected an explicit Spark data-loss failure")
        after = table_manifest()
        if before != after:
            raise RuntimeError("Failed ingestion changed committed gateway Delta data")
        state.update(
            passed=True,
            checkpoint_next_offset=1,
            retained_source_start=low,
            expected_failure_exit_code=exit_code,
            committed_delta_unchanged=True,
            table_manifest=after,
        )
    except BaseException as error:
        state["error"] = repr(error)
        raise
    finally:
        consumer.close()
        state["finished_at"] = datetime.now(timezone.utc).isoformat()
        atomic_json(report, state)
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker", default=shutil.which("docker") or "docker")
    args = parser.parse_args()
    report = run(args.docker)
    print(
        json.dumps(
            {key: value for key, value in report.items() if key != "table_manifest"},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
