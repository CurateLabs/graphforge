#!/usr/bin/env python3
"""Prove representative public API contract mutations fail pytest-bdd."""

from __future__ import annotations

import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap

ROOT = Path(__file__).resolve().parents[2]
PYTHON = os.environ.get("PYTHON", sys.executable)

MUTATIONS = {
    "wrong_row_count": (
        """
        Feature: mutation
          Scenario: wrong row count
            Given a graph with a Person node named "Alice"
            When I execute "MATCH (p:Person) RETURN p.name AS name"
            Then the table has 2 rows
        """,
        "expected 2 rows, got 1",
    ),
    "missing_column": (
        """
        Feature: mutation
          Scenario: missing column
            Given a graph with a Person node named "Alice"
            When I execute "MATCH (p:Person) RETURN p.name AS name"
            Then the table has column "missing"
        """,
        "missing result column: missing",
    ),
    "wrong_value": (
        """
        Feature: mutation
          Scenario: wrong value
            Given a graph with a Person node named "Alice"
            When I execute "MATCH (p:Person) RETURN p.name AS name"
            Then the first row value for "name" is "Bob"
        """,
        "expected first name value 'Bob', got 'Alice'",
    ),
    "wrong_error_class": (
        """
        Feature: mutation
          Scenario: wrong error class
            Given an empty graph
            When I execute "NOT VALID CYPHER !!!"
            Then an ExecutionError is raised
        """,
        "expected ExecutionError, got ParseError",
    ),
    "not_implemented": (
        """
        Feature: mutation
          Scenario: missing behavior
            When I invoke deliberately missing behavior
        """,
        "NotImplementedError: mutation sentinel",
    ),
}


def run_mutation(name: str, feature: str, expected_failure: str) -> None:
    with tempfile.TemporaryDirectory(prefix=f"graphforge-api-bdd-{name}-") as directory:
        temp = Path(directory)
        feature_path = temp / "mutation.feature"
        feature_path.write_text(textwrap.dedent(feature).strip() + "\n")
        runner = temp / "test_mutation.py"
        extra = ""
        if name == "not_implemented":
            extra = textwrap.dedent(
                """
                from pytest_bdd import when

                @when("I invoke deliberately missing behavior")
                def deliberately_missing():
                    raise NotImplementedError("mutation sentinel")
                """
            )
        runner.write_text(
            "pytest_plugins = ['tests.features.steps.api_steps']\n"
            "from pytest_bdd import scenarios\n"
            f"scenarios({str(feature_path)!r})\n" + extra
        )
        result = subprocess.run(
            [str(PYTHON), "-m", "pytest", str(runner), "-q", "--tb=short"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        output = result.stdout + result.stderr
        if result.returncode == 0:
            raise AssertionError(f"{name} mutation unexpectedly passed:\n{output}")
        lowered = output.lower()
        if "xfailed" in lowered or "xpassed" in lowered:
            raise AssertionError(f"{name} mutation was reclassified instead of failing:\n{output}")
        failed = re.search(r"\b(\d+) failed\b", lowered)
        if (
            failed is None
            or failed.group(1) != "1"
            or "error collecting" in lowered
            or "error at setup" in lowered
            or re.search(r"\b\d+ errors?\b", lowered) is not None
        ):
            raise AssertionError(f"{name} did not fail as one executed scenario:\n{output}")
        if expected_failure not in output:
            raise AssertionError(
                f"{name} failed for the wrong reason; expected {expected_failure!r}:\n{output}"
            )


def main() -> int:
    if shutil.which(PYTHON) is None:
        print(f"Python runtime not found: {PYTHON}", file=sys.stderr)
        return 2
    for name, (feature, expected_failure) in MUTATIONS.items():
        run_mutation(name, feature, expected_failure)
    print(f"api-bdd-mutations: {len(MUTATIONS)} fail-closed mutations rejected")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
