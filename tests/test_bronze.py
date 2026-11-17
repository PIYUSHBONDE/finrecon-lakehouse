"""Bronze source identity and Spark checkpoint/financial-string preservation."""

import csv
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from data_generator.chaos_producer import BANK_FIELDS
from pipelines.bronze import append_available, bank_rows, bind_sources, parse_args


class BronzeConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def args(self, *extra):
        return parse_args(
            [
                "--bank-dir",
                str(self.root),
                "--lakehouse-dir",
                str(self.root / "lake"),
                *extra,
            ]
        )

    def test_manifest_preserves_identity_and_allows_only_rate_changes(self):
        args = self.args()
        bind_sources(args)
        bind_sources(args)
        args.max_files_per_trigger = 1
        args.max_offsets_per_trigger = 1
        bind_sources(args)
        args.gateway_topics = "a_different_source"
        with self.assertRaisesRegex(ValueError, "choose a new --dataset"):
            bind_sources(args)

    def test_topic_order_and_duplicates_do_not_change_identity(self):
        args = self.args("--gateway-topics", "b,a,a")
        self.assertEqual(args.gateway_topics, "a,b")
        bind_sources(args)
        bind_sources(self.args("--gateway-topics", "a,b"))

    def test_bad_dataset_and_limits_are_rejected(self):
        for extra in (
            ("--dataset", "../outside"),
            ("--max-files-per-trigger", "0"),
            ("--max-offsets-per-trigger", "-1"),
            ("--gateway-topics", "a,,b"),
        ):
            with self.subTest(extra=extra), patch("sys.stderr"), self.assertRaises(
                SystemExit
            ):
                self.args(*extra)


@unittest.skipUnless(
    importlib.util.find_spec("pyspark") and importlib.util.find_spec("delta"),
    "Run Spark tests in the local Bronze container",
)
class BronzeSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pipelines.runtime import spark_session

        cls.spark = spark_session("FinRecon Bronze tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_csv_strings_quotes_and_checkpoint_resume(self):
        from pyspark.sql.types import StringType

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            landing = root / "landing"
            landing.mkdir()
            args = parse_args(
                ["--bank-dir", str(landing), "--lakehouse-dir", str(root)]
            )
            rows = [
                dict(
                    zip(
                        BANK_FIELDS,
                        [
                            "B1",
                            "GW1",
                            "M1",
                            "0.0001",
                            "USD",
                            'INV-1 says "paid", now\ncontinued',
                            "2026-11-11",
                        ],
                    )
                ),
                dict(
                    zip(
                        BANK_FIELDS,
                        [
                            "B2",
                            "",
                            "M2",
                            "-123.4567",
                            "INVALID",
                            "INV-2 LESS FEES",
                            "bad-date",
                        ],
                    )
                ),
            ]

            def write_file(name, values):
                with (landing / name).open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=BANK_FIELDS)
                    writer.writeheader()
                    writer.writerows(values)

            write_file("first.csv", rows)
            (landing / "partial.csv.tmp").write_text(
                "not a complete CSV", encoding="utf-8"
            )
            target, checkpoint = root / "delta", root / "checkpoint"
            first = append_available(
                bank_rows(self.spark, args), target, checkpoint, "csv_test"
            )
            frame = self.spark.read.format("delta").load(str(target))
            self.assertEqual(frame.count(), 2)
            self.assertIsInstance(
                frame.schema["settled_amount_dec"].dataType, StringType
            )
            actual = {row["settlement_id"]: row.asDict() for row in frame.collect()}
            self.assertEqual(actual["B1"]["settled_amount_dec"], "0.0001")
            self.assertEqual(
                actual["B1"]["remittance_memo"], rows[0]["remittance_memo"]
            )
            self.assertEqual(actual["B2"]["settled_amount_dec"], "-123.4567")
            self.assertEqual(actual["B2"]["currency"], "INVALID")
            self.assertTrue(
                all(row["_corrupt_record"] is None for row in actual.values())
            )
            self.assertTrue(
                all(row["source_file"].endswith("first.csv") for row in actual.values())
            )
            self.assertIn('"settled_amount_dec":"0.0001"', actual["B1"]["raw_payload"])
            second = append_available(
                bank_rows(self.spark, args), target, checkpoint, "csv_test"
            )
            self.assertEqual(
                self.spark.read.format("delta").load(str(target)).count(), 2
            )
            self.assertEqual(first["query_id"], second["query_id"])
            self.assertNotEqual(first["run_id"], second["run_id"])
            write_file("next.csv", [dict(rows[0], settlement_id="B3")])
            append_available(
                bank_rows(self.spark, args), target, checkpoint, "csv_test"
            )
            self.assertEqual(
                self.spark.read.format("delta").load(str(target)).count(), 3
            )


if __name__ == "__main__":
    unittest.main()
