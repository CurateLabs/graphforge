#!/usr/bin/env python3
"""Validate a retained partition and authorize one release-graph action."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from typing import Any

import release_candidate_manifest as candidate
import release_registry as registry


class ActionError(ValueError):
    """A partition or requested action is not safe to execute."""


def _load(path: Path, context: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ActionError(f"cannot read {context} {path}: {error}") from error
    if not isinstance(value, dict):
        raise ActionError(f"{context} must be a JSON object")
    return value


def validate_partition(
    manifest: dict[str, Any],
    artifacts_dir: Path,
    group_id: str,
    *,
    expected_sha: str,
    version: str,
    checked_at: str,
) -> dict[str, Any]:
    if manifest.get("schema") != candidate.SCHEMA:
        raise ActionError("partition publication requires the v2 candidate manifest")
    if manifest.get("commit_sha") != expected_sha:
        raise ActionError("partition candidate SHA diverges")
    if manifest.get("version") != version or manifest.get("tag") != f"v{version}":
        raise ActionError("partition root version diverges")
    try:
        now = datetime.fromisoformat(checked_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise ActionError("checked_at must be an ISO-8601 timestamp") from error
    if now.tzinfo is None:
        raise ActionError("checked_at must include a timezone")
    groups = {
        group.get("id"): group
        for group in manifest.get("artifact_groups", [])
        if isinstance(group, dict)
    }
    if group_id not in candidate.GROUPS or set(groups) != set(candidate.GROUPS):
        raise ActionError("candidate artifact groups are incomplete")
    group = groups[group_id]
    try:
        expiry = datetime.fromisoformat(str(group.get("expires_at")).replace("Z", "+00:00"))
    except ValueError as error:
        raise ActionError(f"artifact partition {group_id} expiry is invalid") from error
    if now >= expiry:
        raise ActionError(f"artifact partition {group_id} has expired")
    expected_paths = group.get("artifact_paths")
    if not isinstance(expected_paths, list) or not expected_paths:
        raise ActionError(f"artifact partition {group_id} is empty")
    by_path = {
        item.get("path"): item
        for item in manifest.get("artifacts", [])
        if isinstance(item, dict) and item.get("group") == group_id
    }
    if set(by_path) != set(expected_paths):
        raise ActionError(f"artifact partition {group_id} inventory diverges")
    actual_paths = {
        path.relative_to(artifacts_dir).as_posix()
        for path in artifacts_dir.rglob("*")
        if path.is_file() and not path.name.startswith(".")
    }
    if actual_paths != set(expected_paths):
        raise ActionError(
            f"artifact partition {group_id} files diverge: "
            f"missing={sorted(set(expected_paths) - actual_paths)} "
            f"extra={sorted(actual_paths - set(expected_paths))}"
        )
    for relative in sorted(expected_paths):
        item = by_path[relative]
        path = artifacts_dir / relative
        if item.get("version") != version:
            raise ActionError(f"partition artifact version diverges: {relative}")
        if path.stat().st_size != item.get("bytes"):
            raise ActionError(f"partition artifact byte count diverges: {relative}")
        if candidate.sha256_file(path) != item.get("sha256"):
            raise ActionError(f"partition artifact checksum diverges: {relative}")
        integrities = candidate._integrities(path)
        if item.get("integrity") != integrities[0] or item.get("integrities") != integrities:
            raise ActionError(f"partition artifact integrity diverges: {relative}")
        artifact_class = item.get("class")
        if artifact_class in {"python-wheel", "python-sdist", "npm-tarball", "rust-crate"}:
            if item.get("archive") != candidate.inspect_archive(path, artifact_class, version):
                raise ActionError(f"partition archive completeness diverges: {relative}")
    return {
        "schema": "graphforge-release-partition-validation-v1",
        "candidate_sha": expected_sha,
        "version": version,
        "group": group_id,
        "checked_at": now.astimezone(timezone.utc).isoformat(),
        "artifact_paths": sorted(expected_paths),
        "status": "passed",
    }


def authorize(
    manifest: dict[str, Any],
    observations: dict[str, Any],
    availability: dict[str, Any],
    node_id: str,
    *,
    planned_at: str,
) -> dict[str, Any]:
    node = next((item for item in manifest.get("nodes", []) if item.get("id") == node_id), None)
    if node is None:
        raise ActionError(f"requested node is not in the candidate: {node_id}")
    plan = registry.plan_recovery(
        manifest,
        observations,
        availability,
        planned_at=planned_at,
        registries={node["registry"]},
    )
    decision = next((item for item in plan["decisions"] if item["node_id"] == node_id), None)
    if decision is None:
        raise ActionError(f"planner returned no decision for node {node_id}")
    action = next((item for item in plan["actions"] if item["node_id"] == node_id), None)
    if decision["disposition"] not in {"publish", "skip_verified"}:
        reason = decision.get("reason")
        detail = f"{decision['disposition']} ({decision['state']}"
        if isinstance(reason, str) and reason:
            detail += f": {reason}"
        detail += ")"
        raise ActionError(f"node {node_id} is not safe to publish: {detail}")
    return {
        "schema": "graphforge-release-action-authorization-v1",
        "candidate_sha": manifest["commit_sha"],
        "version": manifest["version"],
        "node_id": node_id,
        "registry": node["registry"],
        "disposition": decision["disposition"],
        "publish": decision["disposition"] == "publish",
        "artifact_paths": action.get("artifact_paths", []) if action else [],
        "credential_scope": action.get("credential_scope") if action else None,
    }


def accepted_receipt(manifest: dict[str, Any], node_id: str, *, accepted_at: str) -> dict[str, Any]:
    if node_id not in {node.get("id") for node in manifest.get("nodes", [])}:
        raise ActionError("accepted receipt node is outside the candidate")
    try:
        moment = datetime.fromisoformat(accepted_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise ActionError("accepted_at must be an ISO-8601 timestamp") from error
    if moment.tzinfo is None:
        raise ActionError("accepted_at must include a timezone")
    moment = moment.astimezone(timezone.utc)
    return {
        "schema": "graphforge-release-accepted-receipt-v1",
        "node_id": node_id,
        "version": manifest["version"],
        "candidate_sha": manifest["commit_sha"],
        "accepted_at": moment.isoformat(),
        "visibility_deadline": (moment + timedelta(minutes=15)).isoformat(),
        "observation_count": 0,
    }


def write_attempt(manifest: dict[str, Any], node_id: str, *, started_at: str) -> dict[str, Any]:
    if node_id not in {node.get("id") for node in manifest.get("nodes", [])}:
        raise ActionError("write attempt node is outside the candidate")
    try:
        moment = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    except ValueError as error:
        raise ActionError("started_at must be an ISO-8601 timestamp") from error
    if moment.tzinfo is None:
        raise ActionError("started_at must include a timezone")
    return {
        "schema": "graphforge-release-write-attempt-v1",
        "node_id": node_id,
        "version": manifest["version"],
        "candidate_sha": manifest["commit_sha"],
        "started_at": moment.astimezone(timezone.utc).isoformat(),
    }


def _write(value: dict[str, Any], output: Path | None) -> None:
    text = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    else:
        sys.stdout.write(text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    partition = commands.add_parser("validate-partition")
    partition.add_argument("--manifest", type=Path, required=True)
    partition.add_argument("--artifacts-dir", type=Path, required=True)
    partition.add_argument("--group", choices=candidate.GROUPS, required=True)
    partition.add_argument("--expected-sha", required=True)
    partition.add_argument("--version", required=True)
    partition.add_argument("--checked-at", required=True)
    partition.add_argument("--out", type=Path)
    action = commands.add_parser("authorize")
    action.add_argument("--manifest", type=Path, required=True)
    action.add_argument("--observations", type=Path, required=True)
    action.add_argument("--availability", type=Path, required=True)
    action.add_argument("--node", required=True)
    action.add_argument("--planned-at", required=True)
    action.add_argument("--out", type=Path)
    receipt = commands.add_parser("receipt")
    receipt.add_argument("--manifest", type=Path, required=True)
    receipt.add_argument("--node", required=True)
    receipt.add_argument("--accepted-at", required=True)
    receipt.add_argument("--out", type=Path)
    attempt = commands.add_parser("attempt")
    attempt.add_argument("--manifest", type=Path, required=True)
    attempt.add_argument("--node", required=True)
    attempt.add_argument("--started-at", required=True)
    attempt.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    try:
        manifest = _load(args.manifest, "candidate manifest")
        if args.command == "validate-partition":
            result = validate_partition(
                manifest,
                args.artifacts_dir,
                args.group,
                expected_sha=args.expected_sha,
                version=args.version,
                checked_at=args.checked_at,
            )
        elif args.command == "authorize":
            result = authorize(
                manifest,
                _load(args.observations, "registry observations"),
                _load(args.availability, "artifact availability"),
                args.node,
                planned_at=args.planned_at,
            )
        elif args.command == "receipt":
            result = accepted_receipt(manifest, args.node, accepted_at=args.accepted_at)
        else:
            result = write_attempt(manifest, args.node, started_at=args.started_at)
        _write(result, args.out)
        return 0
    except (ActionError, candidate.CandidateError, registry.RegistryError) as error:
        print(f"release-action: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
