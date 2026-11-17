"""Reject unsafe restores and require unchanged Kafka identities and offsets."""

import io
import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from operations.kafka_recovery import preserved
from operations.kafka_recovery import cutover
from operations.volume_archive import inventory, restore, sha256, validate_archive
from operations.readiness import container_path, synthetic_book
from operations.status import evidence
from operations.stack_recovery import (
    landing_source,
    verify_isolation,
    verify_pg_restore,
)
from operations.source_retention import owned_probe


class RecoveryTests(unittest.TestCase):
    def test_landing_copy_rejects_recursive_backup_and_workspace_escape(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / "bank").mkdir()
            backup = root / "reports/recovery"
            with patch("operations.stack_recovery.ROOT", root):
                relative, source = landing_source("/workspace/bank", backup)
                self.assertEqual(relative.as_posix(), "bank")
                self.assertEqual(source, root / "bank")
                with self.assertRaisesRegex(
                    ValueError, "contains the recovery destination"
                ):
                    landing_source("/workspace", backup)
                with self.assertRaisesRegex(ValueError, "outside the workspace"):
                    landing_source("/workspace/..", backup)

    def test_pg_restore_checks_persisted_slots_and_exact_financial_totals(self):
        before = {
            "debit": "1.0000",
            "credit": "1.0000",
            "slots": [
                {
                    "slot_name": "debezium",
                    "plugin": "pgoutput",
                    "confirmed_flush_lsn": "0/200",
                }
            ],
        }
        durable = {
            **before,
            "slots": [{**before["slots"][0], "confirmed_flush_lsn": "0/100"}],
        }
        verify_pg_restore(before, durable, durable)
        with self.assertRaisesRegex(RuntimeError, "financial totals"):
            verify_pg_restore(before, {**durable, "debit": "2.0000"}, durable)
        with self.assertRaisesRegex(RuntimeError, "persisted replication slots"):
            verify_pg_restore(before, durable, before)
        with self.assertRaisesRegex(RuntimeError, "persisted replication slots"):
            wrong = {
                **durable,
                "slots": [{**durable["slots"][0], "slot_name": "other"}],
            }
            verify_pg_restore(before, wrong, wrong)

    def test_retention_deletion_cannot_target_business_or_caller_topics(self):
        owned_probe("finrecon_retention_012345abcdef_gateway")
        for topic in (
            "gateway_events",
            "finrecon_db.public.invoices",
            "finrecon_retention_012345abcdef_cdc",
            "finrecon_retention_invalid_gateway",
        ):
            with self.assertRaises(ValueError):
                owned_probe(topic)

    def test_restore_isolation_rejects_external_networks_ports_and_source_mounts(self):
        names = {
            "postgres": "clone_pg",
            "kafka": "clone_kafka",
            "debezium": "clone_connect",
        }
        clones = {"pgdata": "clone_pgdata", "kafka_data": "clone_kafka_data"}

        def service(volume=None, path=None):
            return {
                "Name": "clone",
                "NetworkSettings": {"Networks": {"isolated": {}}},
                "HostConfig": {"PortBindings": {}},
                "Mounts": [{"Name": volume, "Destination": path}] if volume else [],
            }

        rows = [
            service("clone_pgdata", "/var/lib/postgresql/data"),
            service("clone_kafka_data", "/var/lib/kafka/data"),
            service(),
        ]

        def verify(network, containers):
            with patch(
                "operations.stack_recovery.command",
                side_effect=[json.dumps([network]), json.dumps(containers)],
            ):
                return verify_isolation("docker", names, "isolated", clones)

        self.assertEqual(len(verify({"Internal": True}, rows)), 3)
        with self.assertRaisesRegex(RuntimeError, "external routing"):
            verify({"Internal": False}, rows)
        rows[0]["HostConfig"]["PortBindings"] = {"5432/tcp": [{"HostPort": "5432"}]}
        with self.assertRaisesRegex(RuntimeError, "host port"):
            verify({"Internal": True}, rows)
        rows[0]["HostConfig"]["PortBindings"] = {}
        rows[0]["Mounts"][0]["Name"] = "original_pgdata"
        with self.assertRaisesRegex(RuntimeError, "unexpected data volume"):
            verify({"Internal": True}, rows)

    def test_restore_rejects_hash_mismatch_without_extracting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "target"
            target.mkdir()
            archive = self.make_archive(directory)
            (root / "manifest.json").write_text(
                json.dumps(
                    {"version": 1, "archive_sha256": "incorrect", "entries": {}}
                ),
                encoding="utf-8",
            )
            with patch("operations.volume_archive.subprocess.run") as extract:
                with self.assertRaisesRegex(ValueError, "hash or version"):
                    restore(target, root)
                extract.assert_not_called()
            self.assertEqual(list(target.iterdir()), [])
            self.assertTrue(archive.is_file())

    def test_failed_cutover_keeps_original_error_and_failed_report(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "recovery.json"
            path.write_text(
                json.dumps(
                    {
                        "passed": True,
                        "phase": "prepared",
                        "target_volume": "docker_kafka_data",
                        "before": {},
                    }
                ),
                encoding="utf-8",
            )
            config = {
                "services": {
                    "kafka": {
                        "volumes": [
                            {"target": "/var/lib/kafka/data", "source": "kafka_data"}
                        ]
                    }
                },
                "volumes": {"kafka_data": {"name": "docker_kafka_data"}},
            }
            with patch(
                "operations.kafka_recovery.command", return_value=json.dumps(config)
            ), patch(
                "operations.kafka_recovery.snapshot",
                side_effect=RuntimeError("original broker failure"),
            ), patch(
                "operations.kafka_recovery.wait_for",
                side_effect=RuntimeError("resume failure"),
            ):
                with self.assertRaisesRegex(RuntimeError, "original broker failure"):
                    cutover("docker", Path(directory))
            report = json.loads(path.read_text(encoding="utf-8"))
            self.assertFalse(report["passed"])
            self.assertIn("original broker failure", report["error"])
            self.assertIn("resume failure", report["resume_error"])

    def test_status_rejects_stale_future_or_failed_proof(self):
        now = datetime(2026, 11, 14, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "readiness.json"
            for age, passed, expected in (
                (1, True, True),
                (73, True, False),
                (-1, True, False),
                (1, False, False),
            ):
                path.write_text(
                    json.dumps(
                        {
                            "finished_at": (now - timedelta(hours=age)).isoformat(),
                            "passed": passed,
                        }
                    ),
                    encoding="utf-8",
                )
                self.assertEqual(evidence(path, 72, now)["passed"], expected)

    def test_synthetic_book_is_dated_explicit_and_refuses_overwrite(self):
        rows = [{"event_ts": "2026-11-14T00:00:00Z", "currency": "USD"}] * 2
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rates.csv"
            synthetic_book(rows, path)
            with path.open(newline="", encoding="utf-8") as stream:
                quotes = list(csv.DictReader(stream))
            self.assertEqual(len(quotes), 1)
            self.assertEqual(quotes[0]["rate_date"], "2026-11-14")
            self.assertEqual(quotes[0]["rate"], "80.0000")
            self.assertEqual(quotes[0]["provenance"], "SYNTHETIC_TEST_NOT_MARKET_DATA")
            with self.assertRaises(FileExistsError):
                synthetic_book(rows, path)

    def test_container_path_rejects_external_files(self):
        with self.assertRaises(ValueError):
            container_path(Path(__file__).resolve().anchor)

    def make_archive(self, directory, name="record", kind=tarfile.REGTYPE):
        archive = Path(directory) / "data.tar"
        with tarfile.open(archive, "w") as stream:
            member = tarfile.TarInfo(name)
            member.type = kind
            if kind == tarfile.REGTYPE:
                member.size = 3
                stream.addfile(member, io.BytesIO(b"abc"))
            else:
                member.linkname = "/outside"
                stream.addfile(member)
        return archive

    def test_archive_requires_exact_safe_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = self.make_archive(directory)
            validate_archive(archive, {"record": {"kind": "file", "size": 3}})
            with self.assertRaises(ValueError):
                validate_archive(archive, {"missing": {"kind": "file", "size": 3}})
            with self.assertRaises(ValueError):
                validate_archive(archive, {"record": {"kind": "file", "size": 4}})

    def test_parent_absolute_and_link_entries_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            for name in ("../record", "/record", "a\\record"):
                archive = self.make_archive(directory, name)
                with self.assertRaises(ValueError):
                    validate_archive(archive, {name: {"kind": "file", "size": 3}})
            for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                archive = self.make_archive(directory, kind=kind)
                with self.assertRaises(ValueError):
                    validate_archive(archive, {"record": {"kind": "file", "size": 0}})

    def test_restore_refuses_nonempty_target_before_reading_backup(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "record"
            target.write_bytes(b"preserve me")
            with self.assertRaisesRegex(ValueError, "empty target"):
                restore(directory, Path(directory) / "missing")
            self.assertEqual(target.read_bytes(), b"preserve me")

    def test_inventory_detects_content_change_even_with_same_size(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "record"
            target.write_bytes(b"abc")
            before = inventory(directory)
            self.assertEqual(before["record"]["sha256"], sha256(target))
            target.write_bytes(b"xyz")
            self.assertNotEqual(
                before["record"]["sha256"], inventory(directory)["record"]["sha256"]
            )

    def test_kafka_preservation_checks_topics_cluster_and_both_boundaries(self):
        before = {
            "cluster_id": "cluster",
            "topics": {"t": "id"},
            "earliest": {"t:0": 2},
            "latest": {"t:0": 10},
        }
        preserved(before, dict(before))
        for field in before:
            changed = dict(before)
            changed[field] = "changed"
            with self.assertRaises(RuntimeError):
                preserved(before, changed)
