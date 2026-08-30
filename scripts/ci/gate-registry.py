#!/usr/bin/env python3
"""Validate and render GraphForge's authoritative gate registry (#1009)."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
REGISTRY = ROOT / "config" / "gate-registry.json"
CLASSES = {
    "required_pr_check",
    "scheduled_health_stress",
    "operator_qualification",
    "release_certification",
}
REQUIRED_FIELDS = {"id", "owner", "command", "args", "evidence_contract", "freshness", "sha_rule"}
FORBIDDEN_OPERATOR_WORKFLOW_MARKERS = (
    "FLY_API_TOKEN",
    "graphforge_bench.fly_tiny_qualification",
    "--execute",
    "--cleanup-only",
)


class RegistryError(ValueError):
    """The checked-in gate taxonomy is incomplete or contradictory."""


def load_registry(path: Path = REGISTRY) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RegistryError("gate registry is unavailable or malformed") from error
    if not isinstance(value, dict) or value.get("schema") != "graphforge-gate-registry/1":
        raise RegistryError("gate registry schema is invalid")
    return value


def _workflow_files(root: Path) -> set[str]:
    directory = root / ".github" / "workflows"
    return {
        path.relative_to(root).as_posix()
        for pattern in ("*.yml", "*.yaml")
        for path in directory.glob(pattern)
    }


def validate_registry(value: dict[str, Any], root: Path = ROOT) -> None:
    commands = value.get("commands")
    matrix_variants = value.get("matrix_variants")
    workflows = value.get("workflows")
    operators = value.get("operator_gates")
    if (
        not isinstance(commands, dict)
        or not isinstance(matrix_variants, dict)
        or not isinstance(workflows, list)
        or not isinstance(operators, list)
    ):
        raise RegistryError("registry collections are malformed")
    expected_variants = {
        "concurrency/stress": "scripts/ci/concurrency-stress-gate.py",
        "durability/certification": "scripts/ci/durability-certification-gate.py",
    }
    if matrix_variants != expected_variants:
        raise RegistryError(
            "matrix variants must be the registry-owned concurrency/durability pair"
        )
    for script in matrix_variants.values():
        if not (root / script).is_file():
            raise RegistryError(f"matrix variant references a missing script: {script}")
    for name, argv in commands.items():
        if not isinstance(name, str) or not name or not isinstance(argv, list) or not argv:
            raise RegistryError("command definitions must be non-empty argv arrays")
        if any(not isinstance(arg, str) or not arg for arg in argv):
            raise RegistryError(f"command {name} contains an invalid argument")
        if argv[0].startswith("python") and argv[1:2] != ["-m"]:
            script = root / argv[1]
            if not script.is_file():
                raise RegistryError(f"command {name} references a missing script")
        if "/" in argv[0] and not (root / argv[0]).is_file():
            raise RegistryError(f"command {name} references a missing executable")

    ids: set[str] = set()
    paths: set[str] = set()
    records = workflows + operators
    for record in records:
        if not isinstance(record, dict) or not record.keys() >= REQUIRED_FIELDS:
            raise RegistryError("gate record is missing required metadata")
        gate_id = record["id"]
        if not isinstance(gate_id, str) or gate_id in ids:
            raise RegistryError(f"duplicate or invalid gate id: {gate_id}")
        ids.add(gate_id)
        command = record["command"]
        if (
            command not in commands
            or not isinstance(record["args"], list)
            or any(not isinstance(arg, str) or not arg for arg in record["args"])
        ):
            raise RegistryError(f"{gate_id}: command reference is invalid")
        for field in ("owner", "evidence_contract", "freshness", "sha_rule"):
            if not isinstance(record[field], str) or not record[field]:
                raise RegistryError(f"{gate_id}: {field} is empty")
        gate_class = record.get("class")
        if gate_class is None:
            if record.get("role") != "supporting_automation" or record in operators:
                raise RegistryError(f"{gate_id}: non-gate role is invalid")
        elif gate_class not in CLASSES:
            raise RegistryError(f"{gate_id}: class is invalid")
        if gate_class == "required_pr_check":
            if record["sha_rule"] != "exact_head" or not record["evidence_contract"].startswith(
                "github-status/"
            ):
                raise RegistryError(f"{gate_id}: required PR checks must bind an exact-head status")
        if gate_class == "operator_qualification" and record.get("control_plane") not in {
            "pulumi_esc",
            "local_delegated",
        }:
            raise RegistryError(f"{gate_id}: operator authority is missing")
        if record.get("control_plane") == "pulumi_esc" and command != "operator":
            raise RegistryError(f"{gate_id}: costly qualification bypasses the Python operator")
        if gate_class == "release_certification" and record["sha_rule"] not in {
            "exact_commit",
            "exact_main",
            "version_identity",
        }:
            raise RegistryError(f"{gate_id}: release evidence identity is weak")
        if "path" in record:
            path = record["path"]
            if not isinstance(path, str) or path in paths:
                raise RegistryError(f"{gate_id}: workflow path is duplicate or invalid")
            paths.add(path)
            try:
                workflow = (root / path).read_text(encoding="utf-8")
            except OSError as error:
                raise RegistryError(f"{gate_id}: workflow file is unavailable: {path}") from error
            if record.get("control_plane") == "pulumi_esc":
                marker = next(
                    (item for item in FORBIDDEN_OPERATOR_WORKFLOW_MARKERS if item in workflow),
                    None,
                )
                if marker is not None:
                    raise RegistryError(f"{gate_id}: workflow bypasses ESC operator via {marker}")
                if f"gate-registry.py command {gate_id}" not in workflow:
                    raise RegistryError(f"{gate_id}: workflow must render its registry command")
            if gate_id in {"concurrency-stress", "durability-certification"}:
                if f"gate-registry.py run {gate_id}" not in workflow:
                    raise RegistryError(f"{gate_id}: workflow bypasses its registry command")

    actual = _workflow_files(root)
    if paths != actual:
        missing = sorted(actual - paths)
        stale = sorted(paths - actual)
        raise RegistryError(f"workflow inventory mismatch: missing={missing} stale={stale}")

    required = [item for item in workflows if item.get("class") == "required_pr_check"]
    if [(item["id"], item["evidence_contract"]) for item in required] != [
        ("test-suite", "github-status/CI Gate")
    ]:
        raise RegistryError("required PR checks drifted from repository ruleset 19988544")

    families = {
        item["id"]: (item["command"], tuple(item["args"]))
        for item in workflows
        if item["id"] in {"concurrency-stress", "durability-certification"}
    }
    if set(families) != {"concurrency-stress", "durability-certification"} or any(
        command != "matrix-gate" or "--variant" not in args for command, args in families.values()
    ):
        raise RegistryError("durability/concurrency variants must share matrix-gate")

    release_owners = {
        item["owner"]
        for item in workflows
        if item["id"] in {"clean-environment", "publish-track", "publish"}
    }
    if release_owners != {"release"}:
        raise RegistryError("publication verification must have one release owner")
    referenced_commands = {item["command"] for item in records}
    if referenced_commands != set(commands):
        raise RegistryError("command definitions must be referenced exactly by gate records")


def command_argv(value: dict[str, Any], gate_id: str) -> list[str]:
    records = value["workflows"] + value["operator_gates"]
    try:
        record = next(item for item in records if item["id"] == gate_id)
    except StopIteration as error:
        raise RegistryError(f"unknown gate: {gate_id}") from error
    rendered = [*value["commands"][record["command"]], *record["args"]]
    if rendered[:3] == ["python3", "-m", "graphforge_bench.qualification_operator"]:
        rendered[0] = sys.executable
    return rendered


def command_environment(argv: list[str]) -> dict[str, str] | None:
    """Make benchmark modules importable for registry-owned operator commands."""
    if argv[1:3] != ["-m", "graphforge_bench.qualification_operator"]:
        return None
    environment = os.environ.copy()
    harness = str(ROOT / "benchmarks" / "harness")
    inherited = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = harness if not inherited else harness + os.pathsep + inherited
    return environment


def _strip_separator(argv: list[str]) -> list[str]:
    return argv[1:] if argv[:1] == ["--"] else argv


def reject_owned_options(passthrough: list[str], owned: list[str]) -> None:
    """Prevent caller arguments from overriding registry-selected options."""
    reserved = {value for value in owned if value.startswith("--")}
    for value in passthrough:
        option = value.split("=", 1)[0]
        if option in reserved:
            raise RegistryError(f"caller may not override registry-owned option {option}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("validate", allow_abbrev=False)
    command = sub.add_parser("command", allow_abbrev=False)
    command.add_argument("gate")
    command.add_argument("--json", action="store_true")
    run = sub.add_parser("run", allow_abbrev=False)
    run.add_argument("gate")
    matrix = sub.add_parser("matrix", allow_abbrev=False)
    matrix.add_argument("--family", choices=("concurrency", "durability"), required=True)
    matrix.add_argument("--variant", choices=("stress", "certification"), required=True)
    args, passthrough = parser.parse_known_args(argv)
    try:
        registry = load_registry()
        validate_registry(registry)
        if args.action == "validate":
            print(f"gate registry valid: {len(registry['workflows'])} workflows")
            return 0
        if args.action == "matrix":
            expected = {"concurrency": "stress", "durability": "certification"}
            if expected[args.family] != args.variant:
                raise RegistryError("matrix family/variant pairing is invalid")
            script = registry["matrix_variants"][f"{args.family}/{args.variant}"]
            passthrough = _strip_separator(passthrough)
            reject_owned_options(passthrough, ["--family", "--variant"])
            return subprocess.run(
                [sys.executable, script, "run", *passthrough], cwd=ROOT, check=False
            ).returncode
        rendered = command_argv(registry, args.gate)
        if args.action == "command":
            print(json.dumps(rendered) if args.json else shlex.join(rendered))
            return 0
        passthrough = _strip_separator(passthrough)
        record = next(
            item
            for item in registry["workflows"] + registry["operator_gates"]
            if item["id"] == args.gate
        )
        reject_owned_options(passthrough, record["args"])
        return subprocess.run(
            [*rendered, *passthrough],
            cwd=ROOT,
            check=False,
            env=command_environment(rendered),
        ).returncode
    except RegistryError as error:
        print(f"gate registry invalid: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
