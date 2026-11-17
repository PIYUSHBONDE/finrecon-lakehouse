"""Runner retry, stop, report, and process-lock acceptance checks."""

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from pipelines.orchestrate import dataset_lock, execute


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_transient_failure_retries_in_order(self):
        calls = []

        def runner(command, **kwargs):
            calls.append(command[0])
            return SimpleNamespace(returncode=1 if len(calls) == 1 else 0)

        result = execute(
            [("bronze", ["bronze"]), ("silver", ["silver"])],
            self.root / "run",
            runner=runner,
        )
        self.assertEqual(calls, ["bronze", "bronze", "silver"])
        self.assertEqual(result["status"], "SUCCEEDED")
        self.assertEqual(len(result["stages"][0]["attempts"]), 2)
        self.assertEqual(
            json.loads((self.root / "run/run.json").read_text())["status"], "SUCCEEDED"
        )

    def test_quality_failure_skips_downstream_without_retry(self):
        calls = []

        def runner(command, **kwargs):
            calls.append(command[0])
            return SimpleNamespace(returncode=2)

        with self.assertRaises(RuntimeError):
            execute(
                [("quality", ["quality"]), ("gold", ["gold"])],
                self.root / "run",
                runner=runner,
            )
        report = json.loads((self.root / "run/run.json").read_text())
        self.assertEqual(calls, ["quality"])
        self.assertEqual(
            [stage["status"] for stage in report["stages"]], ["FAILED", "SKIPPED"]
        )
        self.assertEqual(report["status"], "FAILED")

    def test_retry_exhaustion_is_recorded(self):
        with self.assertRaises(RuntimeError):
            execute(
                [("bronze", ["bronze"])],
                self.root / "run",
                runner=lambda *a, **k: SimpleNamespace(returncode=1),
            )
        report = json.loads((self.root / "run/run.json").read_text())
        self.assertEqual(len(report["stages"][0]["attempts"]), 2)
        self.assertEqual(report["status"], "FAILED")

    @unittest.skipUnless(__import__("os").name == "posix", "Container uses Linux flock")
    def test_lock_excludes_second_writer_and_releases(self):
        path = self.root / "dataset.lock"
        with dataset_lock(path):
            with self.assertRaisesRegex(RuntimeError, "dataset lock"):
                with dataset_lock(path):
                    self.fail("Second writer entered")
        with dataset_lock(path):
            pass
