#!/usr/bin/env python3
"""Unit tests for CI Gate ruleset enforcement verification (#721)."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile

SCRIPT = Path(__file__).with_name("verify-ci-gate-enforcement.py")
SPEC = importlib.util.spec_from_file_location("verify_ci_gate_enforcement", SCRIPT)
assert SPEC and SPEC.loader
vce = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(vce)

FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "development"
    / "bazel-migration-evidence"
    / "ci-gate-ruleset-19988544.json"
)


def good_ruleset() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def expect_fail(ruleset: dict, needle: str) -> None:
    try:
        vce.validate_ci_gate_ruleset(ruleset)
    except vce.EnforcementError as exc:
        assert needle in str(exc), f"expected {needle!r} in {exc}"
        return
    raise AssertionError(f"expected EnforcementError containing {needle!r}")


def test_fixture_passes() -> None:
    ruleset = good_ruleset()
    vce.validate_ci_gate_ruleset(ruleset)
    assert vce.required_status_contexts(ruleset) == ["CI Gate"]


def test_missing_ci_gate_fails_even_if_workflow_name_exists_elsewhere() -> None:
    """YAML job naming is not enforcement — empty required checks must fail."""
    ruleset = good_ruleset()
    ruleset["rules"] = [
        {"type": "deletion"},
        {"type": "non_fast_forward"},
        {
            "type": "required_status_checks",
            "parameters": {
                "strict_required_status_checks_policy": True,
                "required_status_checks": [],
            },
        },
    ]
    expect_fail(ruleset, "does not require 'CI Gate'")


def test_absent_status_check_rule_fails() -> None:
    ruleset = good_ruleset()
    ruleset["rules"] = [{"type": "deletion"}, {"type": "non_fast_forward"}]
    expect_fail(ruleset, "exactly one required_status_checks")


def test_second_aggregate_context_fails() -> None:
    ruleset = good_ruleset()
    for rule in ruleset["rules"]:
        if rule.get("type") == "required_status_checks":
            rule["parameters"]["required_status_checks"] = [
                {"context": "CI Gate"},
                {"context": "Binding RC"},
            ]
    expect_fail(ruleset, "exactly")


def test_wrong_ruleset_id_fails() -> None:
    ruleset = good_ruleset()
    ruleset["id"] = 1
    expect_fail(ruleset, "expected ruleset id")


def test_default_branch_condition_required() -> None:
    ruleset = good_ruleset()
    ruleset["conditions"]["ref_name"]["include"] = ["refs/heads/main"]
    expect_fail(ruleset, "~DEFAULT_BRANCH")


def test_preserved_deletion_and_non_fast_forward() -> None:
    ruleset = good_ruleset()
    ruleset["rules"] = [
        rule
        for rule in ruleset["rules"]
        if rule.get("type") != "deletion"
    ]
    expect_fail(ruleset, "must preserve")


def test_strict_policy_required() -> None:
    ruleset = good_ruleset()
    for rule in ruleset["rules"]:
        if rule.get("type") == "required_status_checks":
            rule["parameters"]["strict_required_status_checks_policy"] = False
    expect_fail(ruleset, "strict_required_status_checks_policy")


def test_required_checks_alias_accepted() -> None:
    ruleset = good_ruleset()
    for rule in ruleset["rules"]:
        if rule.get("type") == "required_status_checks":
            params = rule["parameters"]
            params["required_checks"] = params.pop("required_status_checks")
    vce.validate_ci_gate_ruleset(ruleset)


def test_cli_fixture_ok() -> None:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--fixture", str(FIXTURE)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["ok"] is True
    assert payload["required_contexts"] == ["CI Gate"]


def test_cli_fixture_detects_unenforced_payload() -> None:
    bad = copy.deepcopy(good_ruleset())
    bad["rules"] = [{"type": "deletion"}, {"type": "non_fast_forward"}]
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "bad-ruleset.json"
        path.write_text(json.dumps(bad), encoding="utf-8")
        completed = subprocess.run(
            [sys.executable, str(SCRIPT), "--fixture", str(path)],
            check=False,
            capture_output=True,
            text=True,
        )
    assert completed.returncode == 1
    assert "CI Gate enforcement check failed" in completed.stderr


def main() -> None:
    test_fixture_passes()
    test_missing_ci_gate_fails_even_if_workflow_name_exists_elsewhere()
    test_absent_status_check_rule_fails()
    test_second_aggregate_context_fails()
    test_wrong_ruleset_id_fails()
    test_default_branch_condition_required()
    test_preserved_deletion_and_non_fast_forward()
    test_strict_policy_required()
    test_required_checks_alias_accepted()
    test_cli_fixture_ok()
    test_cli_fixture_detects_unenforced_payload()
    print("verify-ci-gate-enforcement tests passed")


if __name__ == "__main__":
    main()
