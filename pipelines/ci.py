"""Reproducible local CI with explicit suites, strict skip handling, and reports."""

import argparse
import ast
from datetime import datetime, timezone
import importlib.util
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import unittest

from .quality import atomic_json

ROOT = Path(__file__).resolve().parents[1]
FAST = (
    "test_chaos_producer.ChaosProducerTests",
    "test_ledger_generation.LedgerGenerationTests",
    "test_bronze.BronzeConfigurationTests",
    "test_silver.ContractFileTests",
    "test_orchestrate.RunnerTests.test_transient_failure_retries_in_order",
    "test_orchestrate.RunnerTests.test_quality_failure_skips_downstream_without_retry",
    "test_orchestrate.RunnerTests.test_retry_exhaustion_is_recorded",
    "test_benchmark_skew.EvidenceTests",
    "test_ci.CiReportingTests",
    "test_recovery.RecoveryTests",
)
SPARK = (
    "test_bronze.BronzeSparkTests",
    "test_silver.SilverSparkTests",
    "test_cdc.CdcTests",
    "test_gold.GoldTests",
    "test_exceptions.ExceptionTests",
    "test_fx.FxTests",
    "test_quality.QualityTests",
    "test_benchmark_skew.SkewIntegrationTests",
    "test_orchestrate.RunnerTests.test_lock_excludes_second_writer_and_releases",
)


def source_files(root=ROOT):
    """Select Python sources without entering generated output or runner state."""
    root = Path(root)
    selected = []

    def fail(error):
        raise error

    for name in ("data_generator", "pipelines", "operations", "tests"):
        directory = root / name
        if not directory.is_dir():
            raise RuntimeError("Missing CI source directory: " + name)
        for current, directories, files in os.walk(directory, onerror=fail):
            directories[:] = sorted(
                child
                for child in directories
                if child not in {"output", "__pycache__", ".git", ".venv", ".venv-ci"}
            )
            selected.extend(
                (Path(current) / file).relative_to(root).as_posix()
                for file in sorted(files)
                if file.endswith(".py")
            )
    if not selected:
        raise RuntimeError("CI collected no Python source files")
    return sorted(selected)


def execute_suite(suite, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    with (directory / "tests.log").open("w", encoding="utf-8") as stream:
        result = unittest.TextTestRunner(stream=stream, verbosity=2).run(suite)
    record = {
        "passed": result.wasSuccessful() and result.testsRun > 0 and not result.skipped,
        "tests_run": result.testsRun,
        "failures": [
            {"test": str(test), "traceback": error} for test, error in result.failures
        ],
        "errors": [
            {"test": str(test), "traceback": error} for test, error in result.errors
        ],
        "skipped": [
            {"test": str(test), "reason": reason} for test, reason in result.skipped
        ],
    }
    atomic_json(directory / "tests.json", record)
    return record


def named_tests(names):
    sys.path.insert(0, str(ROOT / "tests"))
    return unittest.defaultTestLoader.loadTestsFromNames(names)


def require_modules(names):
    missing = [name for name in names if importlib.util.find_spec(name) is None]
    if missing:
        raise RuntimeError("Missing CI dependencies: " + ", ".join(missing))


def validate_contracts():
    import yaml
    from .contracts import load_contract

    for source in ("gateway", "bank", "cdc", "fx"):
        load_contract(source)
    quality = yaml.safe_load(
        (ROOT / "contracts/quality.yaml").read_text(encoding="utf-8")
    )
    if quality.get("version") != 1:
        raise ValueError("Invalid quality contract version")
    for layer in ("silver", "gold"):
        rules = quality.get(layer)
        if not isinstance(rules, list) or not rules:
            raise ValueError("Missing quality rules: " + layer)
        names = [rule["name"] for rule in rules]
        if len(names) != len(set(names)) or any(not rule.get("sql") for rule in rules):
            raise ValueError("Invalid quality rules: " + layer)


def validate_ci_configuration():
    import yaml

    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    )
    if set(workflow["on"]) != {"push", "workflow_dispatch"} or workflow["on"]["push"][
        "branches"
    ] != ["main"]:
        raise ValueError("CI must run only trusted main pushes or manual dispatch")
    if workflow.get("permissions") != {"contents": "read"}:
        raise ValueError("CI token must remain read-only")
    for job in workflow["jobs"].values():
        if job.get("runs-on") != ["self-hosted", "windows", "x64", "finrecon"]:
            raise ValueError("CI must target the dedicated local runner")
        for step in job["steps"]:
            if "uses" in step and not re.fullmatch(
                r"actions/checkout@[0-9a-f]{40}", step["uses"]
            ):
                raise ValueError("CI permits only the commit-pinned checkout action")
    compose = yaml.safe_load(
        (ROOT / "docker/docker-compose.ci.yml").read_text(encoding="utf-8")
    )
    if set(compose["services"]) != {"spark-tests"} or set(compose["volumes"]) != {
        "ci_lakehouse",
        "ci_ivy",
    }:
        raise ValueError("Unexpected services or volumes in isolated CI")
    service = compose["services"]["spark-tests"]
    if service["mem_limit"] != "3g" or service["cpus"] != 2:
        raise ValueError("CI resource limits changed")
    if service["volumes"][0] != {
        "type": "bind",
        "source": "..",
        "target": "/workspace",
        "read_only": True,
    }:
        raise ValueError("CI source mount must remain read-only")


def test_inventory():
    """New test classes must be assigned explicitly instead of silently omitted."""
    groups = {
        "fast": FAST,
        "spark": SPARK,
        "opt_in_postgres": ("test_postgres_ledger.PostgresLedgerTests",),
    }
    counts = {key: 0 for key in groups}
    for file in (ROOT / "tests").rglob("test_*.py"):
        module = ".".join(file.relative_to(ROOT / "tests").with_suffix("").parts)
        for item in ast.parse(file.read_text(encoding="utf-8")).body:
            if isinstance(item, ast.FunctionDef) and item.name.startswith("test_"):
                raise ValueError(
                    "Module-level test needs an explicit CI assignment: " + module
                )
            if not isinstance(item, ast.ClassDef):
                continue
            for method in item.body:
                if not isinstance(
                    method, ast.FunctionDef
                ) or not method.name.startswith("test_"):
                    continue
                name = f"{module}.{item.name}.{method.name}"
                assigned = [
                    key
                    for key, patterns in groups.items()
                    if any(
                        name == pattern or name.startswith(pattern + ".")
                        for pattern in patterns
                    )
                ]
                if len(assigned) != 1:
                    raise ValueError(
                        "Test must belong to exactly one CI group: " + name
                    )
                counts[assigned[0]] += 1
    return counts


def run_checks(profile, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    report = {
        "profile": profile,
        "status": "RUNNING",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "python": sys.version,
        "steps": [],
    }
    path = directory / "ci.json"
    atomic_json(path, report)

    def command(name, arguments, suite=None):
        log = directory / f"{name}.log"
        step = {"name": name, "status": "RUNNING", "log": str(log)}
        if suite:
            step["suite"] = suite
        report["steps"].append(step)
        atomic_json(path, report)
        print(f"CI: {name}" + (f" ({suite})" if suite else ""), flush=True)
        with log.open("w", encoding="utf-8") as stream:
            result = subprocess.run(
                arguments,
                cwd=ROOT,
                stdout=stream,
                stderr=subprocess.STDOUT,
                check=False,
            )
        step.update(
            exit_code=result.returncode,
            status="SUCCEEDED" if result.returncode == 0 else "FAILED",
        )
        atomic_json(path, report)
        if result.returncode != 0:
            raise RuntimeError(f"CI step failed: {name}; inspect {log}")

    try:
        if sys.version_info[:2] != (3, 11):
            raise RuntimeError("The verified CI toolchain requires Python 3.11")
        require_modules(["yaml"])
        if profile == "fast":
            require_modules(["ruff"])
            targets = source_files()
            command("lint", [sys.executable, "-m", "ruff", "check", *targets])
            command(
                "format", [sys.executable, "-m", "ruff", "format", "--check", *targets]
            )
            command("compile", [sys.executable, "-m", "compileall", "-q", *targets])
            validate_contracts()
            report["steps"].append({"name": "contracts", "status": "SUCCEEDED"})
            validate_ci_configuration()
            report["steps"].append({"name": "ci_configuration", "status": "SUCCEEDED"})
            report["test_inventory"] = test_inventory()
            names = list(FAST)
            if os.name == "posix":
                names.append(SPARK[-1])
            tests = execute_suite(named_tests(names), directory / "unit")
            report["tests_run"] = tests["tests_run"]
            report["steps"].append(
                {"name": "unit", "status": "SUCCEEDED" if tests["passed"] else "FAILED"}
            )
            if not tests["passed"]:
                raise RuntimeError("Unit tests failed, skipped, or collected no cases")
        elif profile == "spark":
            require_modules(["pyspark", "delta"])
            if os.name != "posix":
                raise RuntimeError("Spark CI must run in the Linux Docker runner")
            report["tests_run"] = 0
            report["planned_suites"] = list(SPARK)
            for index, name in enumerate(SPARK):
                worker = directory / f"suite_{index:02d}"
                command(
                    f"spark_{index:02d}",
                    [
                        sys.executable,
                        "-m",
                        "pipelines.ci",
                        "--worker",
                        name,
                        "--report-dir",
                        str(worker),
                    ],
                    suite=name,
                )
                result = json.loads((worker / "tests.json").read_text(encoding="utf-8"))
                if not result["passed"]:
                    raise RuntimeError(
                        "Spark suite did not execute successfully: " + name
                    )
                report["tests_run"] += result["tests_run"]
                atomic_json(path, report)
        else:
            raise ValueError("Unknown CI profile")
        report["status"] = "SUCCEEDED"
    except BaseException as exc:
        report.update(status="FAILED", error=str(exc))
        raise
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        atomic_json(path, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("fast", "spark"), default="fast")
    parser.add_argument("--worker", choices=SPARK, help=argparse.SUPPRESS)
    parser.add_argument("--report-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.worker:
        require_modules(["pyspark", "delta", "yaml"])
        result = execute_suite(named_tests([args.worker]), args.report_dir)
        print(json.dumps(result, indent=2))
        return 0 if result["passed"] else 1
    result = run_checks(args.profile, args.report_dir)
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "SUCCEEDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
