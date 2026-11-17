"""Prevent green CI reports when tests are skipped, empty, or failing."""

import json
from pathlib import Path
import tempfile
import unittest

from pipelines.ci import execute_suite


class CiReportingTests(unittest.TestCase):
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
