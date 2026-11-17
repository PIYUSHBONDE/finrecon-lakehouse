"""Coordinated offline snapshots and an isolated, writable full-stack restore drill.

Stop producers and all compute jobs first. Original Kafka/Connect pause for the drill;
PostgreSQL restarts after snapshotting to verify persisted replication-slot state.
restored services run on a private internal Docker network with no published ports.
All source data, archives, and restored volumes are retained. No in-place restore.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
from uuid import uuid4

from pipelines.quality import atomic_json
from .kafka_recovery import (
    IMAGE,
    ROOT,
    command,
    helper,
    preserved,
    running_connector,
    snapshot,
    wait_for,
)
from .volume_archive import sha256


def file_hashes(root):
    root = Path(root)
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError("Linked source files are unsupported: " + str(path))
        if path.is_file():
            result[path.relative_to(root).as_posix()] = sha256(path)
    return result


def landing_source(binding, recovery_directory):
    relative = PurePosixPath(binding).relative_to("/workspace")
    original = ROOT.joinpath(*relative.parts).resolve()
    if not original.is_relative_to(ROOT) or not original.is_dir():
        raise ValueError(
            "Landing directory is missing or outside the workspace: " + binding
        )
    if Path(recovery_directory).resolve().is_relative_to(original):
        raise ValueError(
            "Landing directory contains the recovery destination: " + binding
        )
    return relative, original


def pg(docker, container, sql):
    return command(
        docker,
        "exec",
        container,
        "psql",
        "-U",
        "finrecon",
        "-d",
        "finrecon",
        "-At",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    )


def pg_facts(docker, container):
    return json.loads(
        pg(
            docker,
            container,
            "SELECT json_build_object('accounts',(SELECT count(*) FROM accounts),'invoices',(SELECT count(*) FROM invoices),'journal',(SELECT count(*) FROM ledger_entries),'debit',(SELECT sum(debit)::text FROM ledger_entries),'credit',(SELECT sum(credit)::text FROM ledger_entries),'slots',(SELECT json_agg(s) FROM (SELECT slot_name,plugin,restart_lsn,confirmed_flush_lsn FROM pg_replication_slots ORDER BY slot_name) s));",
        )
    )


def verify_pg_restore(before, recovered, persisted_original):
    """Compare durable positions, not an unflushed live slot acknowledgement.

    PostgreSQL can restore a slot to an earlier checkpoint LSN. Both original
    and clone restart with Connect stopped; their persisted slot states must be
    identical. Every financial aggregate and slot identity must also survive.
    """

    def financial(facts):
        return {key: value for key, value in facts.items() if key != "slots"}

    if financial(before) != financial(recovered) or financial(before) != financial(
        persisted_original
    ):
        raise RuntimeError("Restored PostgreSQL financial totals differ")

    def identities(facts):
        return [(row["slot_name"], row["plugin"]) for row in facts["slots"]]

    if (
        identities(before) != identities(recovered)
        or recovered["slots"] != persisted_original["slots"]
    ):
        raise RuntimeError("Restored PostgreSQL persisted replication slots differ")


def clone_service(docker, info, name, network, alias, mounts):
    args = [
        docker,
        "run",
        "-d",
        "--rm",
        "--name",
        name,
        "--network",
        network,
        "--network-alias",
        alias,
        "--memory",
        "512m" if alias == "postgres" else "1g",
        "--cpus",
        "1",
    ]
    for value in info["Config"]["Env"]:
        args.extend(("--env", value))
    if alias == "kafka":
        args.extend(("--env", "KAFKA_HEAP_OPTS=-Xms256m -Xmx384m"))
    elif alias == "connect":
        args.extend(("--env", "HEAP_OPTS=-Xms256m -Xmx384m"))
    for volume, path in mounts:
        args.extend(("--mount", f"type=volume,source={volume},target={path}"))
    command(*args, info["Config"]["Image"], *info["Config"]["Cmd"])


def clone_connector(docker, network):
    script = "import json,urllib.request; print(urllib.request.urlopen('http://connect:8083/connectors/postgres-ledger-connector/status',timeout=5).read().decode())"
    status = json.loads(
        command(
            docker,
            "run",
            "--rm",
            "--network",
            network,
            "--entrypoint",
            "python3",
            IMAGE,
            "-c",
            script,
        )
    )
    if (
        status["connector"]["state"] != "RUNNING"
        or not status["tasks"]
        or any(row["state"] != "RUNNING" for row in status["tasks"])
    ):
        raise RuntimeError("Restored connector not running: " + json.dumps(status))
    return status


def verify_isolation(docker, names, network, clones):
    details = json.loads(command(docker, "network", "inspect", network))[0]
    if details["Internal"] is not True:
        raise RuntimeError("Restore network permits external routing")
    services = json.loads(command(docker, "inspect", *names.values()))
    mounts = {
        "postgres": ("/var/lib/postgresql/data", clones["pgdata"]),
        "kafka": ("/var/lib/kafka/data", clones["kafka_data"]),
    }
    result = {}
    for key, info in zip(names, services):
        if (
            set(info["NetworkSettings"]["Networks"]) != {network}
            or info["HostConfig"]["PortBindings"]
        ):
            raise RuntimeError(
                "Restored service has an unexpected network or host port"
            )
        if key in mounts:
            path, volume = mounts[key]
            if not any(
                row.get("Name") == volume and row["Destination"] == path
                for row in info["Mounts"]
            ):
                raise RuntimeError("Restored service uses an unexpected data volume")
        result[key] = {
            "container": info["Name"],
            "network": network,
            "published_ports": False,
            "volume": mounts[key][1] if key in mounts else None,
        }
    return result


def run(directory, dataset, docker):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", dataset):
        raise ValueError("Invalid dataset")
    proof = json.loads(
        (ROOT / "pipelines/output" / dataset / "readiness.json").read_text(
            encoding="utf-8"
        )
    )
    if not proof["passed"]:
        raise ValueError("Complete dataset readiness before recovery drill")
    containers = {
        service: json.loads(command(docker, "inspect", "finrecon_" + service))[0]
        for service in ("postgres", "kafka", "debezium")
    }
    if any(not info["State"]["Running"] for info in containers.values()):
        raise ValueError("Start all original services first")
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
    project = containers["kafka"]["Config"]["Labels"]["com.docker.compose.project"]
    if command(
        docker,
        "ps",
        "--filter",
        f"label=com.docker.compose.project={project}",
        "--filter",
        "label=com.docker.compose.service=bronze",
        "-q",
    ):
        raise ValueError("Stop compute jobs before taking snapshots")
    directory.mkdir(parents=True, exist_ok=False)
    report_path = directory / "stack_recovery.json"
    state = {
        "passed": False,
        "phase": "snapshot",
        "dataset": dataset,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "volumes": {},
    }
    atomic_json(report_path, state)
    stopped = False
    prefix = "finrecon_restore_" + uuid4().hex[:8]
    network = prefix + "_network"
    names = {service: prefix + "_" + service for service in containers}
    spark_name = prefix + "_spark"
    clones = {}
    try:
        running_connector()
        command(docker, "stop", "--time", "60", "finrecon_debezium")
        stopped = True
        state["kafka_before"] = snapshot(docker, "finrecon_kafka")
        state["pg_before"] = pg_facts(docker, "finrecon_postgres")
        command(docker, "stop", "--time", "60", "finrecon_kafka", "finrecon_postgres")
        for key, path in (
            ("pgdata", "/var/lib/postgresql/data"),
            ("kafka_data", "/var/lib/kafka/data"),
            ("bronze_data", "/lakehouse"),
            ("spark_ivy", "/opt/spark-ivy"),
        ):
            volume = config["volumes"][key]["name"]
            if key in ("pgdata", "kafka_data"):
                service = "postgres" if key == "pgdata" else "kafka"
                actual = next(
                    m["Name"]
                    for m in containers[service]["Mounts"]
                    if m["Destination"] == path
                )
                if actual != volume:
                    raise ValueError("Compose and actual source volume differ")
            target = directory / key
            target.mkdir()
            state["volumes"][key] = {
                "source": volume,
                "backup": json.loads(helper(docker, "backup", volume, target)),
            }
            atomic_json(report_path, state)
        landing = ROOT / "data_generator/output"
        before_files = file_hashes(landing)
        shutil.copytree(landing, directory / "source_files")
        if before_files != file_hashes(landing) or before_files != file_hashes(
            directory / "source_files"
        ):
            raise RuntimeError("Source files changed during snapshot")
        state["source_file_hashes"] = before_files
        manifest_script = "import pathlib,json; print(json.dumps([json.loads(p.read_text())['bank_dir'] for p in pathlib.Path('/lakehouse/manifests').glob('*.json')]))"
        bindings = json.loads(
            command(
                docker,
                "run",
                "--rm",
                "--network",
                "none",
                "--entrypoint",
                "python3",
                "--mount",
                f"type=volume,source={config['volumes']['bronze_data']['name']},target=/lakehouse,readonly",
                IMAGE,
                "-c",
                manifest_script,
            )
        )
        state["extra_landing"] = {}
        for binding in sorted(set(bindings)):
            relative, original = landing_source(binding, directory)
            if relative.is_relative_to("data_generator/output"):
                continue
            saved = directory / "extra_landing" / Path(*relative.parts)
            hashes = file_hashes(original)
            shutil.copytree(original, saved)
            if hashes != file_hashes(original) or hashes != file_hashes(saved):
                raise RuntimeError("Extra landing files changed during snapshot")
            state["extra_landing"][binding] = {
                "archive_subdir": saved.relative_to(directory).as_posix(),
                "hashes": hashes,
            }
        (directory / "docker-compose.yml").write_bytes(
            (ROOT / "docker/docker-compose.yml").read_bytes()
        )
        # Keep originals offline until clone shutdown. This bounds memory on the
        # small Docker engine and keeps the source recovery point unchanged.
        state["phase"] = "isolated_restore"
        atomic_json(report_path, state)
        for key in state["volumes"]:
            clone = prefix + "_" + key
            command(
                docker,
                "volume",
                "create",
                "--label",
                "finrecon.recovery.stack=true",
                clone,
            )
            clones[key] = clone
            state["volumes"][key]["clone"] = clone
            state["volumes"][key]["restore"] = json.loads(
                helper(docker, "restore", clone, directory / key)
            )
            atomic_json(report_path, state)
        command(
            docker,
            "network",
            "create",
            "--internal",
            "--label",
            "finrecon.recovery.stack=true",
            network,
        )
        state["network"] = network
        clone_service(
            docker,
            containers["postgres"],
            names["postgres"],
            network,
            "postgres",
            [(clones["pgdata"], "/var/lib/postgresql/data")],
        )
        clone_service(
            docker,
            containers["kafka"],
            names["kafka"],
            network,
            "kafka",
            [(clones["kafka_data"], "/var/lib/kafka/data")],
        )
        state["pg_restored"] = wait_for(lambda: pg_facts(docker, names["postgres"]))
        # The live acknowledgement can be ahead of the last durable checkpoint.
        # Restart the original with Connect still stopped to measure that exact
        # persisted state, then demand equality with the restored physical copy.
        command(docker, "start", "finrecon_postgres")
        state["pg_persisted_original"] = wait_for(
            lambda: pg_facts(docker, "finrecon_postgres")
        )
        verify_pg_restore(
            state["pg_before"], state["pg_restored"], state["pg_persisted_original"]
        )
        state["kafka_restored"] = wait_for(lambda: snapshot(docker, names["kafka"]))
        preserved(state["kafka_before"], state["kafka_restored"])
        clone_service(
            docker, containers["debezium"], names["debezium"], network, "connect", []
        )
        state["clone_connector"] = wait_for(lambda: clone_connector(docker, network))
        state["runtime_isolation"] = verify_isolation(docker, names, network, clones)
        state["phase"] = "restored_checkpoint_probe"
        atomic_json(report_path, state)
        source = json.loads(Path(proof["source_report"]).read_text(encoding="utf-8"))
        source_directory = Path(proof["source_report"]).parent
        bank_path = (
            "/workspace/"
            + (source_directory / "bank_settlements")
            .resolve()
            .relative_to(ROOT)
            .as_posix()
        )

        def bronze(label):
            args = [
                docker,
                "run",
                "--rm",
                "--name",
                spark_name,
                "--network",
                network,
                "--memory",
                "3g",
                "--cpus",
                "2",
                "--env",
                "SPARK_LOCAL_IP=127.0.0.1",
                "--env",
                "SPARK_IVY_DIR=/opt/spark-ivy",
                "--env",
                "PYTHONDONTWRITEBYTECODE=1",
            ]
            for mount in (
                f"type=bind,source={ROOT},target=/workspace,readonly",
                f"type=bind,source={directory / 'source_files'},target=/workspace/data_generator/output,readonly",
                f"type=bind,source={directory},target=/reports",
                f"type=volume,source={clones['bronze_data']},target=/lakehouse",
                f"type=volume,source={clones['spark_ivy']},target=/opt/spark-ivy",
            ):
                args.extend(("--mount", mount))
            for binding, row in state["extra_landing"].items():
                args.extend(
                    (
                        "--mount",
                        f"type=bind,source={directory / row['archive_subdir']},target={binding},readonly",
                    )
                )
            args.extend(
                (
                    IMAGE,
                    "python3",
                    "-m",
                    "pipelines.bronze",
                    "--dataset",
                    dataset,
                    "--gateway-topics",
                    source["gateway_topic"],
                    "--bank-dir",
                    bank_path,
                    "--report",
                    "/reports/" + label + ".json",
                )
            )
            with (directory / (label + ".log")).open("w", encoding="utf-8") as stream:
                result = subprocess.run(
                    args, stdout=stream, stderr=subprocess.STDOUT, timeout=240
                )
            if result.returncode:
                raise RuntimeError("Restored Bronze failed; inspect " + label + ".log")
            return json.loads(
                (directory / (label + ".json")).read_text(encoding="utf-8")
            )

        state["bronze_before_probe"] = bronze("bronze_before_probe")
        if any(
            row["input_rows_this_run"]
            for row in state["bronze_before_probe"]["tables"].values()
        ):
            raise RuntimeError("Restored checkpoints replayed old source rows")
        invoice = "INV-" + str(uuid4())
        account, clearing = "RECOVERY-A-" + uuid4().hex, "RECOVERY-B-" + uuid4().hex
        state["probe_invoice"] = invoice
        state["phase"] = "incremental_cdc_probe"
        atomic_json(report_path, state)
        pg(
            docker,
            names["postgres"],
            f"BEGIN; INSERT INTO accounts(account_id,merchant_id,account_name,tier) VALUES ('{account}','MERCHANT_RECOVERY','Recovery debit','STANDARD'),('{clearing}','MERCHANT_RECOVERY','Recovery clearing','STANDARD'); INSERT INTO invoices(invoice_id,account_id,total_amount,currency,status) VALUES ('{invoice}','{account}',1.0000,'INR','ISSUED'); INSERT INTO ledger_entries(invoice_id,account_id,debit,credit) VALUES ('{invoice}','{account}',1.0000,0.0000),('{invoice}','{clearing}',0.0000,1.0000); UPDATE invoices SET status='PAID' WHERE invoice_id='{invoice}'; COMMIT;",
        )
        topic = "finrecon_db.public.invoices"
        offset = state["kafka_before"]["latest"][topic + ":0"]
        raw = command(
            docker,
            "exec",
            names["kafka"],
            "kafka-console-consumer",
            "--bootstrap-server",
            "kafka:29092",
            "--topic",
            topic,
            "--partition",
            "0",
            "--offset",
            str(offset),
            "--max-messages",
            "2",
            "--timeout-ms",
            "45000",
        )
        events = [json.loads(line)["payload"] for line in raw.splitlines()]
        if (
            len(events) != 2
            or [row["op"] for row in events] != ["c", "u"]
            or any(
                row["after"]["invoice_id"] != invoice
                or row["after"]["total_amount"] != "1.0000"
                for row in events
            )
        ):
            raise RuntimeError(
                "Restored CDC did not emit the new exact-decimal invoice"
            )
        state["cdc_probe_events"] = events
        atomic_json(report_path, state)
        state["bronze_after_probe"] = bronze("bronze_after_probe")
        before, after = (
            state["bronze_before_probe"]["tables"],
            state["bronze_after_probe"]["tables"],
        )
        if {key: row["input_rows_this_run"] for key, row in after.items()} != {
            "bronze_gateway_events": 0,
            "bronze_cdc_ledger": 6,
            "bronze_bank_settlements": 0,
        } or any(before[key]["query_id"] != after[key]["query_id"] for key in after):
            raise RuntimeError(
                "Restored checkpoint did not ingest exactly six new CDC events"
            )
        command(docker, "start", "finrecon_postgres")
        wait_for(
            lambda: command(
                docker, "exec", "finrecon_postgres", "pg_isready", "-U", "finrecon"
            )
        )
        if (
            pg(
                docker,
                "finrecon_postgres",
                f"SELECT count(*) FROM invoices WHERE invoice_id='{invoice}';",
            )
            != "0"
        ):
            raise RuntimeError("Probe unexpectedly reached original PostgreSQL")
        state.update(passed=True, phase="complete", probe_absent_from_original=True)
    except BaseException as error:
        state.update(passed=False, error=repr(error))
        raise
    finally:
        try:
            for name in (
                spark_name,
                names["debezium"],
                names["kafka"],
                names["postgres"],
            ):
                if command(docker, "ps", "-q", "--filter", "name=^/" + name + "$"):
                    command(docker, "stop", "--time", "30", name)
            if command(
                docker, "network", "ls", "-q", "--filter", "name=^" + network + "$"
            ):
                command(docker, "network", "rm", network)
            if stopped:
                command(docker, "start", "finrecon_postgres", "finrecon_kafka")
                wait_for(lambda: snapshot(docker, "finrecon_kafka"))
                command(docker, "start", "finrecon_debezium")
            state["original_connector_after"] = wait_for(running_connector)
        except BaseException as error:
            state.update(passed=False, cleanup_error=repr(error))
            if "error" not in state:
                raise
        finally:
            state["finished_at"] = datetime.now(timezone.utc).isoformat()
            atomic_json(report_path, state)
    return state


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--docker", default=shutil.which("docker") or "docker")
    args = parser.parse_args()
    directory = args.directory.resolve()
    if not directory.is_relative_to(ROOT / "pipelines/output/recovery"):
        parser.error("Recovery artifacts must remain under pipelines/output/recovery")
    result = run(directory, args.dataset, args.docker)
    print(
        json.dumps(
            {
                "passed": result["passed"],
                "phase": result["phase"],
                "report": str(directory / "stack_recovery.json"),
            }
        )
    )


if __name__ == "__main__":
    main()
