#!/usr/bin/env python3
"""Keep committed Python lock validation ahead of potentially repairing commands."""

from __future__ import annotations

import copy
from pathlib import Path
import shlex
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[2]


def validate(workflow: dict) -> None:
    """Require an unconditional read-only check and locked PR environment syncs."""
    policy = workflow["jobs"]["policy"]
    assert "if" not in policy, "lock policy must run for every PR"
    assert "continue-on-error" not in policy, "lock failures must block the PR"
    steps = policy["steps"]
    checks = [index for index, step in enumerate(steps) if step.get("run") == "uv lock --check"]
    assert len(checks) == 1, "require one non-mutating lock check"
    check_index = checks[0]
    check = steps[check_index]
    assert "if" not in check, "lock validation cannot be conditional"
    assert "continue-on-error" not in check, "lock failures cannot be ignored"
    assert any(
        step.get("uses", "").startswith("astral-sh/setup-uv@") for step in steps[:check_index]
    ), "install uv before lock validation"
    for step in steps[:check_index]:
        assert "uv run" not in step.get("run", ""), "uv run can repair the lock before validation"
        assert "uv sync" not in step.get("run", ""), "sync must follow non-mutating validation"
    for job in workflow["jobs"].values():
        for step in job.get("steps", []):
            commands = step.get("run", "").replace("\\\n", " ")
            for line in commands.splitlines():
                if line.strip().startswith("uv sync "):
                    words = shlex.split(line, comments=True)
                    assert "--locked" in words, "PR dependency sync must reject lock drift"


class PythonLockPolicyTests(unittest.TestCase):
    """Mutations must fail closed instead of silently repairing a stale lock."""

    def setUp(self) -> None:
        self.workflow = yaml.load(
            (ROOT / ".github/workflows/test.yml").read_text(encoding="utf-8"),
            Loader=yaml.BaseLoader,
        )

    def test_live_workflow(self) -> None:
        validate(self.workflow)

    def test_missing_or_ignored_check(self) -> None:
        for mutation in ("remove", "conditional", "ignored", "mutating"):
            with self.subTest(mutation=mutation):
                workflow = copy.deepcopy(self.workflow)
                steps = workflow["jobs"]["policy"]["steps"]
                check = next(step for step in steps if step.get("run") == "uv lock --check")
                if mutation == "remove":
                    steps.remove(check)
                elif mutation == "conditional":
                    check["if"] = "false"
                elif mutation == "ignored":
                    check["continue-on-error"] = "true"
                else:
                    check["run"] = "uv lock"
                with self.assertRaises(AssertionError):
                    validate(workflow)

    def test_repair_before_validation(self) -> None:
        for command in ("uv run ruff check .", "uv sync --all-extras"):
            with self.subTest(command=command):
                workflow = copy.deepcopy(self.workflow)
                workflow["jobs"]["policy"]["steps"].insert(1, {"run": command})
                with self.assertRaises(AssertionError):
                    validate(workflow)

    def test_unlocked_sync(self) -> None:
        workflow = copy.deepcopy(self.workflow)
        workflow["jobs"]["python-quality"]["steps"].append({"run": "uv sync --all-extras"})
        with self.assertRaises(AssertionError):
            validate(workflow)


if __name__ == "__main__":
    unittest.main()
