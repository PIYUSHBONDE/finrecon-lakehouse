"""Prevent green CI reports when tests are skipped, empty, or failing."""

import json
from pathlib import Path
import tempfile
import unittest

from pipelines.ci import execute_suite, source_files


class CiReportingTests(unittest.TestCase):
    def test_source_collection_keeps_nested_code_and_excludes_generated_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("data_generator", "pipelines", "operations", "tests"):
                (root / name).mkdir()
            nested = root / "pipelines/package/source.py"
            nested.parent.mkdir()
            nested.write_text("value = 1\n", encoding="utf-8")
            for relative in (
                "pipelines/output/actions-runner/vendor.py",
                "data_generator/output/snapshot.py",
                "tests/__pycache__/stale.py",
            ):
                path = root / relative
                path.parent.mkdir(parents=True)
                path.write_text("invalid generated code (", encoding="utf-8")
            self.assertEqual(source_files(root), ["pipelines/package/source.py"])

    def test_source_collection_rejects_missing_directories_and_empty_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(RuntimeError, "Missing CI source directory"):
                source_files(root)
            for name in ("data_generator", "pipelines", "operations", "tests"):
                (root / name).mkdir()
            with self.assertRaisesRegex(RuntimeError, "no Python source files"):
                source_files(root)

    def test_empty_suite_is_a_failure(self):
        with tempfile.TemporaryDirectory() as root:
            result = execute_suite(unittest.TestSuite(), Path(root) / "tests")
        self.assertFalse(result["passed"])
        self.assertEqual(result["tests_run"], 0)

    def test_skipped_suite_is_a_failure(self):
        @unittest.skip("missing dependency")
        def skipped():
            pass

        with tempfile.TemporaryDirectory() as root:
            result = execute_suite(
                unittest.TestSuite([unittest.FunctionTestCase(skipped)]),
                Path(root) / "tests",
            )
        self.assertFalse(result["passed"])
        self.assertEqual(len(result["skipped"]), 1)

    def test_pass_and_failure_have_structured_evidence(self):
        def fails():
            raise AssertionError("injected CI failure")

        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "passed"
            passed = execute_suite(
                unittest.TestSuite([unittest.FunctionTestCase(lambda: None)]), output
            )
            self.assertTrue(passed["passed"])
            self.assertEqual(
                json.loads((output / "tests.json").read_text())["tests_run"], 1
            )
            failed = execute_suite(
                unittest.TestSuite([unittest.FunctionTestCase(fails)]),
                Path(root) / "failed",
            )
            self.assertFalse(failed["passed"])
            self.assertIn("injected CI failure", failed["failures"][0]["traceback"])
