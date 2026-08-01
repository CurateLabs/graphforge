#!/usr/bin/env python3
"""Offline release-artifact rehearsal and sequential recovery proof."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
from typing import Any

import release_candidate_manifest as candidate_contract
import release_registry as registry

ARTIFACT_REPORT_SCHEMA = "graphforge-release-artifact-rehearsal-v1"
RECONCILIATION_SCHEMA = "graphforge-release-reconciliation-v1"
JOB_OUTCOMES = {
    "success",
    "failure",
    "cancelled",
    "timed_out",
    "skipped",
    "not_scheduled",
}
FORBIDDEN_TEXT = ("authorization", "cookie", "password", "secret", "token")


class RehearsalError(ValueError):
    """The release rehearsal could not prove a safe outcome."""


def _load(path: Path, *, context: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RehearsalError(f"cannot read {context} {path}: {error}") from error
    if not isinstance(value, dict):
        raise RehearsalError(f"{context} must be a JSON object")
    return value


def _safe_report(value: Any) -> None:
    serialized = json.dumps(value, sort_keys=True).lower()
    if any(word in serialized for word in FORBIDDEN_TEXT):
        raise RehearsalError("rehearsal output contains a credential-shaped field")


def _run(command: list[str], *, cwd: Path, env: dict[str, str] | None = None) -> str:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=180,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RehearsalError(f"offline consumer could not execute {command[0]}: {error}") from error
    if result.returncode != 0:
        stderr = result.stderr.strip().splitlines()
        detail = stderr[-1] if stderr else f"exit {result.returncode}"
        raise RehearsalError(f"offline consumer {command[0]} failed: {detail}")
    return result.stdout.strip()


def _compatible_wheel(artifacts: list[dict[str, Any]]) -> dict[str, Any]:
    wheels = [item for item in artifacts if item.get("class") == "python-wheel"]
    system = sys.platform
    machine = platform.machine().lower()
    if system.startswith("linux"):
        platform_tokens = ("manylinux", "musllinux", "-linux")
        arch_tokens = ("aarch64", "arm64") if machine in {"aarch64", "arm64"} else ("x86_64",)
    elif system == "darwin":
        platform_tokens = ("macosx", "-macos")
        arch_tokens = ("arm64", "universal2") if machine == "arm64" else ("x86_64", "universal2")
    elif system == "win32":
        platform_tokens = ("win_amd64",) if machine in {"amd64", "x86_64"} else ("win_arm64",)
        arch_tokens = platform_tokens
    else:
        raise RehearsalError(f"unsupported rehearsal platform: {system}/{machine}")
    matches = [
        item
        for item in wheels
        if any(token in item["filename"].lower() for token in platform_tokens)
        and any(token in item["filename"].lower() for token in arch_tokens)
    ]
    if len(matches) != 1:
        raise RehearsalError(
            f"candidate requires exactly one compatible wheel for {system}/{machine}; "
            f"found {len(matches)}"
        )
    return matches[0]


def _python_consumer(
    manifest: dict[str, Any], artifacts_dir: Path, consumer_root: Path
) -> dict[str, Any]:
    wheel = _compatible_wheel(manifest["artifacts"])
    environment = consumer_root / "python"
    _run([sys.executable, "-m", "venv", str(environment)], cwd=consumer_root)
    python = environment / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    _run(
        [
            str(python),
            "-m",
            "pip",
            "install",
            "--no-index",
            "--no-deps",
            str((artifacts_dir / wheel["path"]).resolve()),
        ],
        cwd=consumer_root,
    )
    output = _run(
        [
            str(python),
            "-c",
            "import graphforge; print(graphforge.__version__)",
        ],
        cwd=consumer_root,
    )
    if output != manifest["version"]:
        raise RehearsalError(
            f"clean Python consumer loaded version {output!r}, expected {manifest['version']}"
        )
    return {"artifact": wheel["path"], "imported_version": output, "status": "passed"}


def _node_consumer(
    manifest: dict[str, Any], artifacts_dir: Path, consumer_root: Path
) -> dict[str, Any]:
    npm_items = sorted(
        (item for item in manifest["artifacts"] if item.get("surface") == "npm"),
        key=lambda item: item["name"],
    )
    if len(npm_items) != len(candidate_contract.NPM_PACKAGES):
        raise RehearsalError("candidate npm inventory is incomplete")
    node_root = consumer_root / "node"
    node_root.mkdir()
    (node_root / "package.json").write_text(
        '{"name":"graphforge-release-rehearsal","private":true,"type":"module"}\n',
        encoding="utf-8",
    )
    environment = dict(os.environ)
    environment.update(
        {
            "npm_config_audit": "false",
            "npm_config_fund": "false",
            "npm_config_ignore_scripts": "true",
            "npm_config_offline": "true",
            "npm_config_update_notifier": "false",
        }
    )
    tarballs = [str((artifacts_dir / item["path"]).resolve()) for item in npm_items]
    _run(
        ["npm", "install", "--offline", "--ignore-scripts", "--no-audit", "--no-fund", *tarballs],
        cwd=node_root,
        env=environment,
    )
    node_version = _run(
        [
            "node",
            "--input-type=module",
            "--eval",
            "import { version } from '@curatelabs/graphforge'; process.stdout.write(version())",
        ],
        cwd=node_root,
        env=environment,
    )
    if node_version != manifest["version"]:
        raise RehearsalError(
            f"clean Node/native consumer loaded version {node_version!r}, "
            f"expected {manifest['version']}"
        )
    cli = node_root / "node_modules" / "@curatelabs" / "graphforge-cli" / "bin" / "graphforge.js"
    cli_output = _run(
        ["node", str(cli), "--json", "config", "validate"], cwd=node_root, env=environment
    )
    try:
        cli_payload = json.loads(cli_output)
    except json.JSONDecodeError as error:
        raise RehearsalError("clean CLI consumer did not emit JSON") from error
    if not isinstance(cli_payload, dict):
        raise RehearsalError("clean CLI consumer emitted an invalid result")
    skills = (
        node_root
        / "node_modules"
        / "@curatelabs"
        / "graphforge-agent-skills"
        / "bin"
        / "graphforge-agent-skills.js"
    )
    compatibility_text = _run(
        ["node", str(skills), "compatibility", "--json"], cwd=node_root, env=environment
    )
    try:
        compatibility = json.loads(compatibility_text)
    except json.JSONDecodeError as error:
        raise RehearsalError("clean agent-skills consumer did not emit JSON") from error
    if compatibility.get("graphforge_release") != manifest["version"]:
        raise RehearsalError("agent-skills compatibility release diverges from the root version")
    return {
        "artifacts": [item["path"] for item in npm_items],
        "agent_skills_release": compatibility["graphforge_release"],
        "cli_status": "passed",
        "loaded_version": node_version,
        "native_addon_status": "loaded_through_main_package",
        "status": "passed",
    }


def rehearse_artifacts(
    manifest_path: Path,
    artifacts_dir: Path,
    *,
    expected_sha: str,
    version: str,
    rehearsed_at: str,
) -> dict[str, Any]:
    try:
        moment = datetime.fromisoformat(rehearsed_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise RehearsalError("rehearsed_at must be an ISO-8601 timestamp") from error
    if moment.tzinfo is None:
        raise RehearsalError("rehearsed_at must include a timezone")
    manifest = candidate_contract.validate(
        manifest_path, artifacts_dir, expected_sha, version, as_of=moment
    )
    crate_nodes = [node for node in manifest["nodes"] if node["registry"] == "crates"]
    crate_edges = [edge for edge in manifest["dependencies"] if edge["from"].startswith("crates:")]
    with tempfile.TemporaryDirectory(prefix="graphforge-release-rehearsal-") as temporary:
        consumer_root = Path(temporary)
        python_result = _python_consumer(manifest, artifacts_dir, consumer_root)
        node_result = _node_consumer(manifest, artifacts_dir, consumer_root)
    report = {
        "schema": ARTIFACT_REPORT_SCHEMA,
        "candidate_sha": expected_sha,
        "version": version,
        "rehearsed_at": moment.astimezone(timezone.utc).isoformat(),
        "offline": True,
        "registry_writes": 0,
        "checks": {
            "candidate_completeness": {"nodes": len(manifest["nodes"]), "status": "passed"},
            "python_clean_consumer": python_result,
            "node_cli_skills_clean_consumer": node_result,
            "rust_packages": {
                "dependency_edges": len(crate_edges),
                "packages": [node["name"] for node in crate_nodes],
                "status": "passed",
            },
            "shared_version": {"root_version": version, "status": "passed"},
        },
        "status": "passed",
    }
    _safe_report(report)
    return report


def _observation_set(manifest: dict[str, Any], values: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema": registry.OBSERVATION_SET_SCHEMA,
        "candidate_sha": manifest["commit_sha"],
        "version": manifest["version"],
        "observations": values,
    }


def reconcile(
    manifest: dict[str, Any],
    observations: dict[str, Any],
    availability: dict[str, Any],
    *,
    reconciled_at: str,
    job_outcomes: dict[str, str] | None = None,
) -> dict[str, Any]:
    outcomes = job_outcomes or {}
    node_ids = {node["id"] for node in manifest.get("nodes", [])}
    if not set(outcomes).issubset(node_ids):
        raise RehearsalError("job outcome references a node outside the candidate")
    if any(outcome not in JOB_OUTCOMES for outcome in outcomes.values()):
        raise RehearsalError("job outcome is invalid")
    plan = registry.plan_recovery(manifest, observations, availability, planned_at=reconciled_at)
    decisions = {item["node_id"]: item for item in plan["decisions"]}
    nodes = [
        {
            "node_id": node_id,
            "registry": decisions[node_id]["registry"],
            "registry_state": decisions[node_id]["state"],
            "disposition": decisions[node_id]["disposition"],
            "job_outcome": outcomes.get(node_id, "not_scheduled"),
        }
        for node_id in sorted(node_ids)
    ]
    report = {
        "schema": RECONCILIATION_SCHEMA,
        "candidate_sha": manifest["commit_sha"],
        "version": manifest["version"],
        "reconciled_at": plan["planned_at"],
        "complete": all(node["registry_state"] == "verified" for node in nodes),
        "nodes": nodes,
        "next_actions": plan["actions"],
        "blockers": plan["blockers"],
        "summary": {**plan["summary"], "nodes": len(nodes)},
    }
    _safe_report(report)
    return report


def simulate_sequential(
    manifest: dict[str, Any],
    observations: dict[str, Any],
    availability: dict[str, Any],
    transitions: list[dict[str, Any]],
    *,
    simulated_at: str,
) -> dict[str, Any]:
    current = list(observations.get("observations", []))
    outcomes: dict[str, str] = {}
    events: list[dict[str, Any]] = []
    for index, transition in enumerate(transitions):
        node_id = transition.get("node_id")
        action_kind = transition.get("action_kind")
        outcome = transition.get("job_outcome")
        observation = transition.get("observation")
        if outcome not in JOB_OUTCOMES - {"not_scheduled"}:
            raise RehearsalError(f"transition {index} has an invalid job outcome")
        before = registry.plan_recovery(
            manifest,
            _observation_set(manifest, current),
            availability,
            planned_at=simulated_at,
        )
        eligible = {(action["node_id"], action["kind"]) for action in before["actions"]}
        if (node_id, action_kind) not in eligible:
            raise RehearsalError(
                f"transition {index} is not dependency-ready: {node_id}/{action_kind}"
            )
        if not isinstance(observation, dict) or observation.get("node_id") != node_id:
            raise RehearsalError(f"transition {index} lacks matching live registry evidence")
        current = [item for item in current if item.get("node_id") != node_id]
        current.append(observation)
        current.sort(key=lambda item: item["node_id"])
        # Validation of the new observation is deliberately delegated to the pure planner.
        registry.plan_recovery(
            manifest,
            _observation_set(manifest, current),
            availability,
            planned_at=simulated_at,
        )
        outcomes[node_id] = outcome
        events.append(
            {
                "sequence": index + 1,
                "node_id": node_id,
                "action_kind": action_kind,
                "job_outcome": outcome,
                "observed_state": observation["state"],
            }
        )
    final = reconcile(
        manifest,
        _observation_set(manifest, current),
        availability,
        reconciled_at=simulated_at,
        job_outcomes=outcomes,
    )
    final["events"] = events
    final["sequential"] = True
    _safe_report(final)
    return final


def _write_or_print(value: dict[str, Any], output: Path | None) -> None:
    text = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    artifacts = commands.add_parser("artifacts")
    artifacts.add_argument("--manifest", type=Path, required=True)
    artifacts.add_argument("--artifacts-dir", type=Path, required=True)
    artifacts.add_argument("--expected-sha", required=True)
    artifacts.add_argument("--version", required=True)
    artifacts.add_argument("--rehearsed-at", required=True)
    artifacts.add_argument("--out", type=Path)
    reconciliation = commands.add_parser("reconcile")
    reconciliation.add_argument("--manifest", type=Path, required=True)
    reconciliation.add_argument("--observations", type=Path, required=True)
    reconciliation.add_argument("--availability", type=Path, required=True)
    reconciliation.add_argument("--job-outcomes", type=Path)
    reconciliation.add_argument("--reconciled-at", required=True)
    reconciliation.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "artifacts":
            result = rehearse_artifacts(
                args.manifest,
                args.artifacts_dir,
                expected_sha=args.expected_sha,
                version=args.version,
                rehearsed_at=args.rehearsed_at,
            )
        else:
            manifest = _load(args.manifest, context="candidate manifest")
            observations = _load(args.observations, context="registry observations")
            availability = _load(args.availability, context="artifact availability")
            outcomes = _load(args.job_outcomes, context="job outcomes") if args.job_outcomes else {}
            result = reconcile(
                manifest,
                observations,
                availability,
                reconciled_at=args.reconciled_at,
                job_outcomes=outcomes,
            )
        _write_or_print(result, args.out)
        return 0
    except (RehearsalError, candidate_contract.CandidateError, registry.RegistryError) as error:
        print(f"release-rehearsal: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
