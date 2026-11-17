"""Synthetic skew correctness and evidence-parser acceptance checks."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

from pipelines.benchmark_skew import event_metrics, oracle, run_benchmark


class EvidenceTests(unittest.TestCase):
    def test_event_metrics_attribute_spill_and_peak_to_job_group(self):
        events = [
            {
                "Event": "SparkListenerJobStart",
                "Stage IDs": [1],
                "Properties": {"spark.jobGroup.id": "case"},
            },
            {
                "Event": "SparkListenerTaskEnd",
                "Stage ID": 1,
                "Task End Reason": {"Reason": "Success"},
                "Task Metrics": {
                    "Memory Bytes Spilled": 1000,
                    "Disk Bytes Spilled": 200,
                    "Peak Execution Memory": 8000,
                    "Executor Run Time": 10,
                    "Shuffle Read Metrics": {
                        "Local Bytes Read": 300,
                        "Remote Bytes Read": 400,
                        "Total Records Read": 50,
                    },
                    "Shuffle Write Metrics": {"Shuffle Bytes Written": 900},
                },
            },
            {
                "Event": "SparkListenerTaskEnd",
                "Stage ID": 1,
                "Task End Reason": {"Reason": "Success"},
                "Task Metrics": {"Peak Execution Memory": 6000},
            },
            {"Event": "SparkListenerApplicationEnd"},
        ]
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "events"
            path.write_text(
                "\n".join(json.dumps(event) for event in events), encoding="utf-8"
            )
            evidence = event_metrics(root)
        self.assertTrue(evidence["application_ended"])
        self.assertEqual(evidence["oom_task_failures"], 0)
        metric = evidence["groups"]["case"]
        self.assertEqual(metric["tasks"], 2)
        self.assertEqual(metric["memory_spilled_bytes"], 1000)
        self.assertEqual(metric["disk_spilled_bytes"], 200)
        self.assertEqual(metric["peak_task_execution_memory_bytes"], 8000)
        self.assertEqual(metric["shuffle_read_bytes"], 700)
        self.assertEqual(metric["shuffle_write_bytes"], 900)
        self.assertEqual(metric["max_task_shuffle_read_records"], 50)

    def test_oom_attempt_is_not_hidden_by_completed_application(self):
        with tempfile.TemporaryDirectory() as root:
            Path(root, "events").write_text(
                json.dumps(
                    {
                        "Event": "SparkListenerTaskEnd",
                        "Stage ID": 7,
                        "Task End Reason": {
                            "Reason": "ExceptionFailure",
                            "Class Name": "java.lang.OutOfMemoryError",
                        },
                    }
                )
                + "\n"
                + json.dumps({"Event": "SparkListenerApplicationEnd"}),
                encoding="utf-8",
            )
            result = event_metrics(root)
        self.assertEqual(result["oom_task_failures"], 1)
        self.assertEqual(result["groups"]["unattributed"]["failed_attempts"], 1)

    def test_invalid_workload_bounds_fail_before_directory_creation(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "run"
            # Bounds are checked before Spark or any benchmark output starts.
            for sizes, trials in (
                ([999], 1),
                ([1001], 1),
                ([1000, 1000], 1),
                ([5000100], 1),
                ([1000], 0),
            ):
                with self.assertRaises(ValueError):
                    run_benchmark(output, sizes, trials)
            self.assertFalse(output.exists())
        currency, merchant = oracle(1000)
        self.assertEqual(sum(row["rows"] for row in currency), 1000)
        self.assertEqual(
            sum(
                row["rows"]
                for row in merchant
                if row["merchant_id"] == "MERCHANT_ENT_01"
            ),
            650,
        )


@unittest.skipUnless(
    importlib.util.find_spec("pyspark") and importlib.util.find_spec("delta"),
    "Run in Spark container",
)
class SkewIntegrationTests(unittest.TestCase):
    def test_all_strategies_preserve_financials_and_emit_real_task_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            result = run_benchmark(Path(root) / "run", [1000], 1)
            self.assertEqual(result["status"], "SUCCEEDED")
            self.assertEqual(len(result["cases"]), 6)
            self.assertTrue(
                all(case["financial_oracle_passed"] for case in result["cases"])
            )
            self.assertTrue(
                all(case["task_metrics"]["tasks"] > 0 for case in result["cases"])
            )
            self.assertEqual(result["event_evidence"]["oom_task_failures"], 0)
            probes = result["profiles"][0]["partition_probes"]
            self.assertGreaterEqual(probes["merchant"]["max_partition_rows"], 650)
            self.assertLess(
                probes["salted_merchant"]["max_partition_rows"],
                probes["merchant"]["max_partition_rows"],
            )
            control = [
                case
                for case in result["cases"]
                if case["workload"] == "invoice_reconciliation"
            ][0]
            self.assertEqual(control["journal_legs"], 2000)
            self.assertEqual(result["cgroup_after"].get("memory.max"), 3 * 1024**3)
