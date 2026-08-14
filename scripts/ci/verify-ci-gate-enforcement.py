#!/usr/bin/env python3
"""Verify live GitHub policy requires exactly the ``CI Gate`` aggregate (#721).

Workflow job naming is not enforcement. This checker validates a repository
ruleset payload (fixture or live ``gh api``) and fails when ``CI Gate`` is
absent, duplicated alongside another aggregate context, or when the ruleset no
longer targets the default branch with the preserved deletion / non-fast-forward
rules.

Maintainer live check::

    python3 scripts/ci/verify-ci-gate-enforcement.py --check-live

Fixture / unit mode::

    python3 scripts/ci/verify-ci-gate-enforcement.py \\
      --fixture docs/development/bazel-migration-evidence/ci-gate-ruleset-19988544.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

REQUIRED_CONTEXT = "CI Gate"
EXPECTED_RULESET_ID = 19988544
EXPECTED_RULESET_NAME = "main"
DEFAULT_BRANCH_INCLUDE = "~DEFAULT_BRANCH"
PRESERVED_RULE_TYPES = frozenset({"deletion", "non_fast_forward"})
DEFAULT_OWNER = "CurateLabs"
DEFAULT_REPO = "graphforge"
DEFAULT_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "development"
    / "bazel-migration-evidence"
    / "ci-gate-ruleset-19988544.json"
)


class EnforcementError(Exception):
    """Ruleset payload does not enforce CI Gate as required."""


def _status_check_entries(rule: dict[str, Any]) -> list[dict[str, Any]]:
    params = rule.get("parameters") or {}
    # OpenAPI: parameters.required_status_checks[].context
    # Some older exports used parameters.required_checks[].context
    entries = params.get("required_status_checks")
    if entries is None:
        entries = params.get("required_checks")
    if entries is None:
        return []
    if not isinstance(entries, list):
        raise EnforcementError("required_status_checks parameters must be a list of check objects")
    return [entry for entry in entries if isinstance(entry, dict)]


def required_status_contexts(ruleset: dict[str, Any]) -> list[str]:
    """Return required status-check contexts from a ruleset payload."""
    contexts: list[str] = []
    for rule in ruleset.get("rules") or []:
        if not isinstance(rule, dict) or rule.get("type") != "required_status_checks":
            continue
        for entry in _status_check_entries(rule):
            context = entry.get("context")
            if isinstance(context, str) and context.strip():
                contexts.append(context.strip())
    return contexts


def validate_ci_gate_ruleset(
    ruleset: dict[str, Any],
    *,
    expected_id: int = EXPECTED_RULESET_ID,
) -> None:
    """Fail closed unless the ruleset requires exactly ``CI Gate`` on default."""
    if not isinstance(ruleset, dict):
        raise EnforcementError("ruleset payload must be a JSON object")

    ruleset_id = ruleset.get("id")
    if ruleset_id != expected_id:
        raise EnforcementError(f"expected ruleset id {expected_id}, got {ruleset_id!r}")

    if ruleset.get("enforcement") != "active":
        raise EnforcementError(
            f"ruleset {expected_id} must be actively enforced, got {ruleset.get('enforcement')!r}"
        )

    if ruleset.get("target") != "branch":
        raise EnforcementError(
            f"ruleset {expected_id} must target branches, got {ruleset.get('target')!r}"
        )

    if ruleset.get("name") != EXPECTED_RULESET_NAME:
        raise EnforcementError(
            f"ruleset {expected_id} name must remain {EXPECTED_RULESET_NAME!r}, "
            f"got {ruleset.get('name')!r}"
        )

    conditions = ruleset.get("conditions") or {}
    ref_name = conditions.get("ref_name") or {}
    include = ref_name.get("include") or []
    if DEFAULT_BRANCH_INCLUDE not in include:
        raise EnforcementError(
            f"ruleset {expected_id} must include {DEFAULT_BRANCH_INCLUDE!r} "
            f"(got include={include!r})"
        )

    rules = ruleset.get("rules")
    if not isinstance(rules, list) or not rules:
        raise EnforcementError(f"ruleset {expected_id} has no rules")

    rule_types = {rule.get("type") for rule in rules if isinstance(rule, dict)}
    missing_preserved = sorted(PRESERVED_RULE_TYPES - rule_types)
    if missing_preserved:
        raise EnforcementError(
            f"ruleset {expected_id} must preserve {sorted(PRESERVED_RULE_TYPES)}; "
            f"missing {missing_preserved}"
        )

    status_rules = [
        rule
        for rule in rules
        if isinstance(rule, dict) and rule.get("type") == "required_status_checks"
    ]
    if len(status_rules) != 1:
        raise EnforcementError(
            f"ruleset {expected_id} must define exactly one required_status_checks "
            f"rule, found {len(status_rules)}"
        )

    params = status_rules[0].get("parameters") or {}
    if params.get("strict_required_status_checks_policy") is not True:
        raise EnforcementError(
            "strict_required_status_checks_policy must be true "
            "(exact head SHA / up-to-date with base)"
        )

    contexts = required_status_contexts(ruleset)
    if REQUIRED_CONTEXT not in contexts:
        raise EnforcementError(
            f"ruleset {expected_id} does not require {REQUIRED_CONTEXT!r}; "
            f"contexts={contexts!r}. Workflow YAML naming alone is not enforcement."
        )
    if contexts != [REQUIRED_CONTEXT]:
        raise EnforcementError(
            f"ruleset {expected_id} must require exactly {[REQUIRED_CONTEXT]!r}, "
            f"got {contexts!r} (no second competing aggregate context)"
        )


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise EnforcementError(f"{path} must contain a JSON object")
    return payload


def fetch_live_ruleset(
    *,
    owner: str = DEFAULT_OWNER,
    repo: str = DEFAULT_REPO,
    ruleset_id: int = EXPECTED_RULESET_ID,
) -> dict[str, Any]:
    """Fetch the authoritative ruleset via ``gh api``."""
    endpoint = f"repos/{owner}/{repo}/rulesets/{ruleset_id}"
    try:
        raw = subprocess.check_output(
            ["gh", "api", endpoint],
            text=True,
            stderr=subprocess.STDOUT,
        )
    except FileNotFoundError as exc:
        raise EnforcementError("`gh` CLI is required for --check-live") from exc
    except subprocess.CalledProcessError as exc:
        raise EnforcementError(
            f"gh api {endpoint} failed (exit {exc.returncode}): {exc.output}"
        ) from exc
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise EnforcementError(f"gh api {endpoint} returned non-object JSON")
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--fixture",
        type=Path,
        nargs="?",
        const=DEFAULT_FIXTURE,
        help="Validate a checked-in ruleset JSON fixture (default path if flag alone)",
    )
    mode.add_argument(
        "--check-live",
        action="store_true",
        help="Fetch ruleset via gh api and validate live enforcement",
    )
    parser.add_argument("--owner", default=DEFAULT_OWNER)
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--ruleset-id", type=int, default=EXPECTED_RULESET_ID)
    args = parser.parse_args(argv)

    source = "n/a"
    try:
        if args.check_live:
            ruleset = fetch_live_ruleset(
                owner=args.owner, repo=args.repo, ruleset_id=args.ruleset_id
            )
            source = f"live:{args.owner}/{args.repo}/rulesets/{args.ruleset_id}"
        else:
            fixture = args.fixture if args.fixture is not None else DEFAULT_FIXTURE
            ruleset = load_json(fixture)
            source = str(fixture)
        validate_ci_gate_ruleset(ruleset, expected_id=args.ruleset_id)
    except (EnforcementError, OSError, json.JSONDecodeError) as exc:
        print(f"CI Gate enforcement check failed ({source}): {exc}", file=sys.stderr)
        return 1

    contexts = required_status_contexts(ruleset)
    print(
        json.dumps(
            {
                "ok": True,
                "source": source,
                "ruleset_id": ruleset.get("id"),
                "required_contexts": contexts,
                "enforcement": ruleset.get("enforcement"),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
