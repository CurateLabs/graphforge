#!/usr/bin/env python3
"""Registry truth adapters and pure recovery planning for GraphForge releases."""

from __future__ import annotations

import argparse
import base64
import binascii
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import sys
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

from release_candidate_manifest import GROUPS, PUBLICATION_STATES
from release_candidate_manifest import SCHEMA as CANDIDATE_SCHEMA

OBSERVATION_SCHEMA = "graphforge-registry-observation-v1"
OBSERVATION_SET_SCHEMA = "graphforge-registry-observations-v1"
PLAN_SCHEMA = "graphforge-release-recovery-plan-v1"
MAX_VISIBILITY_SECONDS = 15 * 60
MAX_VISIBILITY_OBSERVATIONS = 4
MAX_OBSERVATION_AGE_SECONDS = 10 * 60
USER_AGENT = "GraphForge release observer (github.com/CurateLabs/graphforge)"
SHA_RE = re.compile(r"[0-9a-f]{40}")
FORBIDDEN_OUTPUT_KEYS = ("authorization", "cookie", "password", "secret", "token")


class RegistryError(ValueError):
    """Registry evidence or a recovery input is unsafe or malformed."""


def _parse_time(value: str, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as error:
        raise RegistryError(f"{field} must be an ISO-8601 timestamp") from error
    if parsed.tzinfo is None:
        raise RegistryError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _load_json(path: Path, *, context: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RegistryError(f"cannot read {context} {path}: {error}") from error
    if not isinstance(value, dict):
        raise RegistryError(f"{context} must be a JSON object")
    return value


def _manifest_indexes(
    manifest: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, str]]:
    if manifest.get("schema") != CANDIDATE_SCHEMA:
        raise RegistryError("recovery requires graphforge-release-candidate-v2")
    version = manifest.get("version")
    commit_sha = manifest.get("commit_sha")
    if not isinstance(version, str) or not version:
        raise RegistryError("candidate root version is missing")
    if not isinstance(commit_sha, str) or SHA_RE.fullmatch(commit_sha) is None:
        raise RegistryError("candidate commit SHA is invalid")
    nodes_raw = manifest.get("nodes")
    artifacts_raw = manifest.get("artifacts")
    groups_raw = manifest.get("artifact_groups")
    if not all(isinstance(value, list) for value in (nodes_raw, artifacts_raw, groups_raw)):
        raise RegistryError("candidate nodes, artifacts, and groups must be arrays")
    nodes: dict[str, dict[str, Any]] = {}
    for node in nodes_raw:
        if not isinstance(node, dict) or not isinstance(node.get("id"), str):
            raise RegistryError("candidate contains an invalid public node")
        if "version" in node:
            raise RegistryError("candidate public nodes may not override the root version")
        if node["id"] in nodes:
            raise RegistryError(f"candidate contains duplicate node {node['id']}")
        registry = node.get("registry")
        name = node.get("name")
        if registry not in {"pypi", "npm", "crates"} or not isinstance(name, str):
            raise RegistryError(f"candidate node {node['id']} has invalid registry identity")
        if node["id"] != f"{registry}:{name}":
            raise RegistryError(f"candidate node identity mismatch: {node['id']}")
        nodes[node["id"]] = node
    artifacts: dict[str, dict[str, Any]] = {}
    path_to_group: dict[str, str] = {}
    for artifact in artifacts_raw:
        if not isinstance(artifact, dict) or not isinstance(artifact.get("path"), str):
            raise RegistryError("candidate contains an invalid artifact")
        path = artifact["path"]
        if path in artifacts:
            raise RegistryError(f"candidate contains duplicate artifact {path}")
        if artifact.get("version") != version:
            raise RegistryError(f"candidate artifact version diverges: {path}")
        group = artifact.get("group")
        if group not in GROUPS:
            raise RegistryError(f"candidate artifact group is invalid: {path}")
        artifacts[path] = artifact
        path_to_group[path] = group
    for node_id, node in nodes.items():
        paths = node.get("artifact_paths")
        if not isinstance(paths, list) or not paths:
            raise RegistryError(f"candidate node {node_id} has no artifact paths")
        for path in paths:
            artifact = artifacts.get(path)
            if artifact is None:
                raise RegistryError(f"candidate node {node_id} references missing artifact {path}")
            if artifact.get("surface") != node["registry"] or artifact.get("name") != node["name"]:
                raise RegistryError(f"candidate node/artifact identity mismatch: {node_id}")
    return nodes, artifacts, path_to_group


def _node_expected(manifest: dict[str, Any], node_id: str) -> dict[str, Any]:
    nodes, artifacts, _ = _manifest_indexes(manifest)
    try:
        node = nodes[node_id]
    except KeyError as error:
        raise RegistryError(f"unknown candidate node: {node_id}") from error
    selected = [artifacts[path] for path in node["artifact_paths"]]
    return {
        "node": node,
        "artifacts": selected,
        "version": manifest["version"],
        "commit_sha": manifest["commit_sha"],
    }


def endpoint_for(registry: str, name: str, version: str) -> str:
    if registry == "pypi":
        return f"https://pypi.org/pypi/{urllib.parse.quote(name, safe='')}/{version}/json"
    if registry == "npm":
        encoded = urllib.parse.quote(name, safe="")
        return f"https://registry.npmjs.org/{encoded}/{version}"
    if registry == "crates":
        return f"https://crates.io/api/v1/crates/{urllib.parse.quote(name, safe='')}/{version}"
    raise RegistryError(f"unsupported registry: {registry}")


def _receipt(
    value: dict[str, Any] | None,
    *,
    node_id: str,
    version: str,
    candidate_sha: str,
) -> dict[str, Any] | None:
    if value is None:
        return None
    if (
        value.get("schema") != "graphforge-release-accepted-receipt-v1"
        or value.get("node_id") != node_id
        or value.get("version") != version
        or value.get("candidate_sha") != candidate_sha
    ):
        raise RegistryError("accepted-write receipt identity diverges from the candidate")
    accepted_at = _parse_time(value.get("accepted_at"), field="receipt.accepted_at")
    deadline = _parse_time(value.get("visibility_deadline"), field="receipt.visibility_deadline")
    if deadline <= accepted_at or deadline > accepted_at + timedelta(
        seconds=MAX_VISIBILITY_SECONDS
    ):
        raise RegistryError("accepted-write visibility deadline is not bounded")
    observations = value.get("observation_count", 0)
    if not isinstance(observations, int) or observations < 0:
        raise RegistryError("accepted-write observation_count is invalid")
    return {
        "accepted_at": accepted_at.isoformat(),
        "visibility_deadline": deadline.isoformat(),
        "observation_count": observations,
    }


def _pending_or_indeterminate(
    receipt: dict[str, Any] | None,
    observed_at: datetime,
    *,
    pending_reason: str,
) -> tuple[str, str, dict[str, Any]]:
    if receipt is None:
        return "indeterminate", pending_reason, {}
    deadline = _parse_time(receipt["visibility_deadline"], field="receipt.visibility_deadline")
    count = receipt["observation_count"] + 1
    evidence = {
        "accepted_at": receipt["accepted_at"],
        "visibility_deadline": receipt["visibility_deadline"],
        "observation_count": count,
        "max_observations": MAX_VISIBILITY_OBSERVATIONS,
    }
    if observed_at <= deadline and count < MAX_VISIBILITY_OBSERVATIONS:
        return "accepted_pending_visibility", pending_reason, evidence
    return "indeterminate", "visibility_bound_exhausted", evidence


def _pypi(
    expected: dict[str, Any], payload: dict[str, Any], receipt: dict[str, Any] | None, now: datetime
) -> tuple[str, str, dict[str, Any]]:
    info = payload.get("info")
    urls = payload.get("urls")
    if not isinstance(info, dict) or not isinstance(urls, list):
        return _pending_or_indeterminate(receipt, now, pending_reason="pypi_metadata_incomplete")
    name = info.get("name")
    version = info.get("version")
    if not isinstance(name, str) or not isinstance(version, str):
        return _pending_or_indeterminate(receipt, now, pending_reason="pypi_identity_pending")
    if name != expected["node"]["name"] or version != expected["version"]:
        return (
            "conflict",
            "pypi_identity_mismatch",
            {"observed_name": name, "observed_version": version},
        )
    license_value = info.get("license_expression") or info.get("license")
    if license_value != "Apache-2.0":
        if license_value in {None, ""}:
            return _pending_or_indeterminate(receipt, now, pending_reason="pypi_license_pending")
        return "conflict", "pypi_license_mismatch", {"observed_license": str(license_value)}
    actual: dict[str, str] = {}
    for item in urls:
        if not isinstance(item, dict):
            return _pending_or_indeterminate(
                receipt, now, pending_reason="pypi_file_metadata_invalid"
            )
        filename = item.get("filename")
        digest = (
            item.get("digests", {}).get("sha256") if isinstance(item.get("digests"), dict) else None
        )
        if not isinstance(filename, str) or not isinstance(digest, str):
            return _pending_or_indeterminate(
                receipt, now, pending_reason="pypi_file_metadata_invalid"
            )
        if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            return "indeterminate", "pypi_checksum_malformed", {"filename": filename}
        actual[filename] = digest
    wanted = {item["filename"]: item["sha256"] for item in expected["artifacts"]}
    if set(actual) != set(wanted):
        return (
            "conflict",
            "pypi_file_set_mismatch",
            {
                "missing_files": sorted(set(wanted) - set(actual)),
                "unexpected_files": sorted(set(actual) - set(wanted)),
            },
        )
    mismatches = sorted(name for name in wanted if actual[name] != wanted[name])
    if mismatches:
        return "conflict", "pypi_checksum_mismatch", {"mismatched_files": mismatches}
    return (
        "verified",
        "pypi_public_files_match",
        {
            "filenames": sorted(wanted),
            "sha256_verified": True,
            "license": "Apache-2.0",
        },
    )


def _npm(
    expected: dict[str, Any], payload: dict[str, Any], receipt: dict[str, Any] | None, now: datetime
) -> tuple[str, str, dict[str, Any]]:
    name = payload.get("name")
    version = payload.get("version")
    if not isinstance(name, str) or not isinstance(version, str):
        return _pending_or_indeterminate(receipt, now, pending_reason="npm_identity_pending")
    if name != expected["node"]["name"] or version != expected["version"]:
        return (
            "conflict",
            "npm_identity_mismatch",
            {"observed_name": name, "observed_version": version},
        )
    license_value = payload.get("license")
    if license_value != "Apache-2.0":
        if license_value in {None, ""}:
            return _pending_or_indeterminate(receipt, now, pending_reason="npm_license_pending")
        return "conflict", "npm_license_mismatch", {"observed_license": str(license_value)}
    dist = payload.get("dist")
    integrity = dist.get("integrity") if isinstance(dist, dict) else None
    if not isinstance(integrity, str) or not integrity.strip():
        return _pending_or_indeterminate(receipt, now, pending_reason="npm_integrity_pending")
    tokens = set(integrity.split())
    supported: set[str] = set()
    for token in tokens:
        algorithm, separator, encoded = token.partition("-")
        if not separator or algorithm not in {"sha256", "sha512"}:
            continue
        try:
            decoded = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError):
            return "indeterminate", "npm_integrity_malformed", {"algorithm": algorithm}
        if len(decoded) != {"sha256": 32, "sha512": 64}[algorithm]:
            return "indeterminate", "npm_integrity_malformed", {"algorithm": algorithm}
        supported.add(token)
    if not supported:
        return "indeterminate", "npm_integrity_unsupported", {}
    artifact = expected["artifacts"][0]
    wanted_integrities = set(artifact.get("integrities", []))
    if not supported.intersection(wanted_integrities):
        return (
            "conflict",
            "npm_integrity_mismatch",
            {"algorithms": sorted(token.split("-", 1)[0] for token in supported)},
        )
    package = (artifact.get("archive") or {}).get("package", {})
    wanted_dependencies = package.get("dependencies", {})
    actual_dependencies: dict[str, str] = {}
    for field in ("dependencies", "optionalDependencies"):
        value = payload.get(field)
        if isinstance(value, dict):
            actual_dependencies.update(
                {key: str(item) for key, item in value.items() if key.startswith("@curatelabs/")}
            )
    if actual_dependencies != wanted_dependencies:
        return (
            "conflict",
            "npm_dependency_mismatch",
            {
                "expected_dependencies": sorted(wanted_dependencies),
                "observed_dependencies": sorted(actual_dependencies),
            },
        )
    return (
        "verified",
        "npm_public_integrity_matches",
        {
            "integrity_algorithms": sorted(token.split("-", 1)[0] for token in supported),
            "license": "Apache-2.0",
            "dependency_names": sorted(wanted_dependencies),
        },
    )


def _crates(
    expected: dict[str, Any], payload: dict[str, Any], receipt: dict[str, Any] | None, now: datetime
) -> tuple[str, str, dict[str, Any]]:
    version_record = payload.get("version")
    if not isinstance(version_record, dict):
        return _pending_or_indeterminate(receipt, now, pending_reason="crates_version_pending")
    name = version_record.get("crate")
    version = version_record.get("num")
    if not isinstance(name, str) or not isinstance(version, str):
        return _pending_or_indeterminate(receipt, now, pending_reason="crates_identity_pending")
    if name != expected["node"]["name"] or version != expected["version"]:
        return (
            "conflict",
            "crates_identity_mismatch",
            {
                "observed_name": name,
                "observed_version": version,
            },
        )
    checksum = version_record.get("checksum")
    if not isinstance(checksum, str) or re.fullmatch(r"[0-9a-f]{64}", checksum) is None:
        return "indeterminate", "crates_checksum_malformed", {}
    if checksum != expected["artifacts"][0]["sha256"]:
        return "conflict", "crates_checksum_mismatch", {"sha256_present": isinstance(checksum, str)}
    if version_record.get("yanked") is not False:
        return "conflict", "crates_version_yanked", {"yanked": version_record.get("yanked")}
    license_value = version_record.get("license")
    if license_value != "Apache-2.0":
        if license_value in {None, ""}:
            return _pending_or_indeterminate(receipt, now, pending_reason="crates_license_pending")
        return "conflict", "crates_license_mismatch", {"observed_license": str(license_value)}
    owners = payload.get("owners")
    users = owners.get("users") if isinstance(owners, dict) else None
    if not isinstance(users, list):
        return _pending_or_indeterminate(receipt, now, pending_reason="crates_owners_pending")
    logins = sorted(
        str(user.get("login")) for user in users if isinstance(user, dict) and user.get("login")
    )
    if "DecisionNerd" not in logins:
        return "conflict", "crates_owner_mismatch", {"owner_logins": logins}
    return (
        "verified",
        "crates_public_checksum_matches",
        {
            "sha256_verified": True,
            "license": "Apache-2.0",
            "owner_logins": logins,
        },
    )


def observe(
    manifest: dict[str, Any],
    node_id: str,
    response: dict[str, Any],
    *,
    observed_at: str,
    accepted_receipt: dict[str, Any] | None = None,
) -> dict[str, Any]:
    expected = _node_expected(manifest, node_id)
    node = expected["node"]
    now = _parse_time(observed_at, field="observed_at")
    receipt = _receipt(
        accepted_receipt,
        node_id=node_id,
        version=expected["version"],
        candidate_sha=expected["commit_sha"],
    )
    if receipt is not None and now < _parse_time(
        receipt["accepted_at"], field="receipt.accepted_at"
    ):
        raise RegistryError("registry observation predates accepted-write evidence")
    status = response.get("status")
    if not isinstance(status, int):
        raise RegistryError("registry transport response requires integer status")
    payload = response.get("json")
    evidence: dict[str, Any] = {"http_status": status}
    if status == 404:
        if receipt is None:
            state, reason = "absent", f"{node['registry']}_authoritative_not_found"
        else:
            state, reason, pending = _pending_or_indeterminate(
                receipt, now, pending_reason=f"{node['registry']}_accepted_not_visible"
            )
            evidence.update(pending)
    elif status in {401, 403}:
        state, reason = "failed", f"{node['registry']}_authorization_failed"
    elif status in {0, 408, 425, 429} or status >= 500:
        state, reason = "indeterminate", f"{node['registry']}_transport_indeterminate"
        retry_after = response.get("retry_after_seconds")
        if isinstance(retry_after, int) and 0 <= retry_after <= MAX_VISIBILITY_SECONDS:
            evidence["retry_after_seconds"] = retry_after
    elif status != 200:
        state, reason = "failed", f"{node['registry']}_unexpected_status"
    elif not isinstance(payload, dict):
        state, reason = "indeterminate", f"{node['registry']}_malformed_response"
    else:
        adapter = {"pypi": _pypi, "npm": _npm, "crates": _crates}[node["registry"]]
        state, reason, adapter_evidence = adapter(expected, payload, receipt, now)
        evidence.update(adapter_evidence)
    observation = {
        "schema": OBSERVATION_SCHEMA,
        "candidate_sha": expected["commit_sha"],
        "version": expected["version"],
        "node_id": node_id,
        "registry": node["registry"],
        "name": node["name"],
        "state": state,
        "reason": reason,
        "observed_at": now.isoformat(),
        "endpoint": endpoint_for(node["registry"], node["name"], expected["version"]),
        "evidence": evidence,
    }
    _assert_safe_output(observation)
    return observation


def _assert_safe_output(value: Any, *, path: str = "root") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            lowered = str(key).lower()
            if any(forbidden in lowered for forbidden in FORBIDDEN_OUTPUT_KEYS):
                raise RegistryError(f"sensitive key is forbidden in output: {path}.{key}")
            _assert_safe_output(item, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_safe_output(item, path=f"{path}[{index}]")


def _validate_normalized_observation(value: dict[str, Any], node: dict[str, Any]) -> None:
    state = value["state"]
    reason = value["reason"]
    evidence = value.get("evidence")
    if not isinstance(evidence, dict) or not isinstance(evidence.get("http_status"), int):
        raise RegistryError(f"observation evidence is incomplete: {node['id']}")
    status = evidence["http_status"]
    registry_name = node["registry"]
    if state == "not_attempted":
        raise RegistryError("not_attempted is represented by an omitted observation")
    if state == "absent":
        if status != 404 or reason != f"{registry_name}_authoritative_not_found":
            raise RegistryError(f"absent observation lacks authoritative evidence: {node['id']}")
    elif state == "accepted_pending_visibility":
        required = {"accepted_at", "visibility_deadline", "observation_count", "max_observations"}
        if status not in {200, 404} or not required.issubset(evidence):
            raise RegistryError(f"pending observation lacks accepted-write evidence: {node['id']}")
    elif state == "verified":
        expected_reason = {
            "pypi": "pypi_public_files_match",
            "npm": "npm_public_integrity_matches",
            "crates": "crates_public_checksum_matches",
        }[registry_name]
        if status != 200 or reason != expected_reason:
            raise RegistryError(f"verified observation evidence is invalid: {node['id']}")
        if registry_name in {"pypi", "crates"} and evidence.get("sha256_verified") is not True:
            raise RegistryError(f"verified observation lacks checksum proof: {node['id']}")
        if registry_name == "npm" and not evidence.get("integrity_algorithms"):
            raise RegistryError(f"verified npm observation lacks integrity proof: {node['id']}")
    elif state == "conflict" and status != 200:
        raise RegistryError(f"conflict observation lacks existing public identity: {node['id']}")
    elif state == "failed" and status in {0, 200, 404, 408, 425, 429}:
        raise RegistryError(
            f"failed observation lacks deterministic failure evidence: {node['id']}"
        )


def _observation_map(
    manifest: dict[str, Any], observations: dict[str, Any], now: datetime
) -> dict[str, dict[str, Any]]:
    nodes, _, _ = _manifest_indexes(manifest)
    if observations.get("schema") != OBSERVATION_SET_SCHEMA:
        raise RegistryError("observation set schema is invalid")
    if observations.get("candidate_sha") != manifest["commit_sha"]:
        raise RegistryError("observation set candidate SHA diverges")
    if observations.get("version") != manifest["version"]:
        raise RegistryError("observation set version diverges")
    values = observations.get("observations")
    if not isinstance(values, list):
        raise RegistryError("observation set observations must be an array")
    result: dict[str, dict[str, Any]] = {}
    for value in values:
        if not isinstance(value, dict) or value.get("schema") != OBSERVATION_SCHEMA:
            raise RegistryError("observation set contains an invalid observation")
        node_id = value.get("node_id")
        if node_id not in nodes or node_id in result:
            raise RegistryError(f"observation node is missing or duplicated: {node_id}")
        if (
            value.get("candidate_sha") != manifest["commit_sha"]
            or value.get("version") != manifest["version"]
        ):
            raise RegistryError(f"observation identity diverges: {node_id}")
        if value.get("state") not in PUBLICATION_STATES:
            raise RegistryError(f"observation state is invalid: {node_id}")
        node = nodes[node_id]
        if (
            value.get("registry") != node["registry"]
            or value.get("name") != node["name"]
            or value.get("endpoint")
            != endpoint_for(node["registry"], node["name"], manifest["version"])
        ):
            raise RegistryError(f"observation registry identity diverges: {node_id}")
        reason = value.get("reason")
        if not isinstance(reason, str) or re.fullmatch(r"[a-z0-9_]+", reason) is None:
            raise RegistryError(f"observation reason is unsafe: {node_id}")
        _validate_normalized_observation(value, node)
        normalized = value
        observed_at = _parse_time(value.get("observed_at"), field=f"{node_id}.observed_at")
        if observed_at > now or now - observed_at > timedelta(seconds=MAX_OBSERVATION_AGE_SECONDS):
            normalized = {
                **value,
                "state": "indeterminate",
                "reason": "observation_stale",
                "evidence": {"max_age_seconds": MAX_OBSERVATION_AGE_SECONDS},
            }
        result[node_id] = normalized
    return result


def _group_availability(
    manifest: dict[str, Any], availability: dict[str, Any], now: datetime
) -> dict[str, bool]:
    groups = manifest.get("artifact_groups")
    if not isinstance(groups, list):
        raise RegistryError("candidate artifact groups are invalid")
    manifest_groups = {group.get("id"): group for group in groups if isinstance(group, dict)}
    if set(manifest_groups) != set(GROUPS):
        raise RegistryError("candidate artifact groups are incomplete")
    result: dict[str, bool] = {}
    for group_id in GROUPS:
        value = availability.get(group_id, False)
        available = (
            value
            if isinstance(value, bool)
            else value.get("available", False)
            if isinstance(value, dict)
            else False
        )
        expiry = _parse_time(
            manifest_groups[group_id].get("expires_at"),
            field=f"artifact_groups.{group_id}.expires_at",
        )
        result[group_id] = bool(available and now < expiry)
    return result


def plan_recovery(
    manifest: dict[str, Any],
    observations: dict[str, Any],
    availability: dict[str, Any],
    *,
    planned_at: str,
    registries: set[str] | None = None,
) -> dict[str, Any]:
    nodes, _, path_to_group = _manifest_indexes(manifest)
    now = _parse_time(planned_at, field="planned_at")
    observed = _observation_map(manifest, observations, now)
    available = _group_availability(manifest, availability, now)
    selected_registries = registries or {"pypi", "npm", "crates"}
    if not selected_registries.issubset({"pypi", "npm", "crates"}):
        raise RegistryError("recovery registry filter is invalid")
    dependency_map: dict[str, list[str]] = {node_id: [] for node_id in nodes}
    edges = manifest.get("dependencies")
    if not isinstance(edges, list):
        raise RegistryError("candidate dependencies are invalid")
    for edge in edges:
        if not isinstance(edge, dict):
            raise RegistryError("candidate dependency edge is invalid")
        node_id = edge.get("from")
        dependency = edge.get("requires")
        if node_id not in nodes or dependency not in nodes:
            raise RegistryError("candidate dependency edge references a missing node")
        dependency_map[node_id].append(dependency)

    actions: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    blockers: list[dict[str, Any]] = []
    for node_id in sorted(nodes):
        node = nodes[node_id]
        if node["registry"] not in selected_registries:
            continue
        observation = observed.get(
            node_id,
            {
                "state": "not_attempted",
                "reason": "fresh_registry_observation_required",
            },
        )
        state = observation["state"]
        decision: dict[str, Any] = {
            "node_id": node_id,
            "registry": node["registry"],
            "state": state,
            "reason": observation.get("reason"),
        }
        if state == "verified":
            decision["disposition"] = "skip_verified"
        elif state == "not_attempted":
            decision["disposition"] = "observe"
            actions.append({"node_id": node_id, "kind": "observe", "registry": node["registry"]})
        elif state == "accepted_pending_visibility":
            decision["disposition"] = "verify_visibility"
            actions.append(
                {"node_id": node_id, "kind": "verify_visibility", "registry": node["registry"]}
            )
        elif state == "absent":
            dependencies = sorted(dependency_map[node_id])
            dependency_states = {
                dependency: observed.get(dependency, {"state": "not_attempted"})["state"]
                for dependency in dependencies
            }
            if any(state != "verified" for state in dependency_states.values()):
                decision["disposition"] = "blocked_dependencies"
                decision["dependency_states"] = dependency_states
                blockers.append(
                    {
                        "node_id": node_id,
                        "reason": "dependencies_not_verified",
                        "dependency_states": dependency_states,
                    }
                )
            else:
                paths = nodes[node_id]["artifact_paths"]
                groups = sorted({path_to_group[path] for path in paths})
                unavailable = [group for group in groups if not available[group]]
                if unavailable:
                    decision["disposition"] = "blocked_artifacts"
                    blockers.append(
                        {
                            "node_id": node_id,
                            "reason": "artifact_group_unavailable_or_expired",
                            "artifact_groups": unavailable,
                        }
                    )
                else:
                    decision["disposition"] = "publish"
                    actions.append(
                        {
                            "node_id": node_id,
                            "kind": "publish",
                            "registry": node["registry"],
                            "artifact_groups": groups,
                            "artifact_paths": paths,
                            "credential_scope": {
                                "pypi": "pypi-oidc",
                                "npm": "npm",
                                "crates": "crates.io",
                            }[node["registry"]],
                        }
                    )
        else:
            decision["disposition"] = "blocked_registry_state"
            blockers.append({"node_id": node_id, "reason": f"registry_state_{state}"})
        decisions.append(decision)

    download_groups = sorted(
        {
            group
            for action in actions
            if action["kind"] == "publish"
            for group in action["artifact_groups"]
        }
    )
    plan = {
        "schema": PLAN_SCHEMA,
        "candidate_sha": manifest["commit_sha"],
        "version": manifest["version"],
        "planned_at": now.isoformat(),
        "registry_scope": sorted(selected_registries),
        "actions": actions,
        "decisions": decisions,
        "download_groups": download_groups,
        "blockers": blockers,
        "summary": {
            "publish": sum(action["kind"] == "publish" for action in actions),
            "observe": sum(action["kind"] == "observe" for action in actions),
            "verify_visibility": sum(action["kind"] == "verify_visibility" for action in actions),
            "blocked": len(blockers),
            "verified": sum(decision["state"] == "verified" for decision in decisions),
        },
    }
    _assert_safe_output(plan)
    return plan


def _transport(url: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.load(response)
            return {"status": response.status, "json": payload}
    except urllib.error.HTTPError as error:
        retry_after = error.headers.get("Retry-After") if error.headers is not None else None
        return {
            "status": error.code,
            **(
                {"retry_after_seconds": int(retry_after)}
                if retry_after is not None and retry_after.isdigit()
                else {}
            ),
        }
    except (OSError, json.JSONDecodeError):
        return {"status": 0}


def live_response(manifest: dict[str, Any], node_id: str) -> dict[str, Any]:
    expected = _node_expected(manifest, node_id)
    node = expected["node"]
    response = _transport(endpoint_for(node["registry"], node["name"], expected["version"]))
    if node["registry"] == "crates" and response.get("status") == 200:
        owners_url = (
            f"https://crates.io/api/v1/crates/{urllib.parse.quote(node['name'], safe='')}/owners"
        )
        owners = _transport(owners_url)
        if owners.get("status") == 200 and isinstance(response.get("json"), dict):
            response["json"]["owners"] = owners.get("json")
        elif isinstance(response.get("json"), dict):
            response["json"]["owners"] = None
    return response


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    observe_parser = commands.add_parser("observe")
    observe_parser.add_argument("--manifest", type=Path, required=True)
    observe_parser.add_argument("--node", required=True)
    source = observe_parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--response", type=Path)
    source.add_argument("--live", action="store_true")
    observe_parser.add_argument("--accepted-receipt", type=Path)
    observe_parser.add_argument("--observed-at", required=True)
    observe_parser.add_argument("--out", type=Path)

    observe_all_parser = commands.add_parser("observe-all")
    observe_all_parser.add_argument("--manifest", type=Path, required=True)
    observe_all_parser.add_argument(
        "--registry", action="append", choices=("pypi", "npm", "crates")
    )
    observe_all_parser.add_argument("--observed-at", required=True)
    observe_all_parser.add_argument("--receipts-dir", type=Path)
    observe_all_parser.add_argument("--attempts-dir", type=Path)
    observe_all_parser.add_argument("--out", type=Path)

    plan_parser = commands.add_parser("plan")
    plan_parser.add_argument("--manifest", type=Path, required=True)
    plan_parser.add_argument("--observations", type=Path, required=True)
    plan_parser.add_argument("--availability", type=Path, required=True)
    plan_parser.add_argument("--planned-at", required=True)
    plan_parser.add_argument("--registry", action="append", choices=("pypi", "npm", "crates"))
    plan_parser.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    try:
        manifest = _load_json(args.manifest, context="candidate manifest")
        if args.command == "observe":
            response = (
                live_response(manifest, args.node)
                if args.live
                else _load_json(args.response, context="registry response")
            )
            receipt = (
                _load_json(args.accepted_receipt, context="accepted-write receipt")
                if args.accepted_receipt
                else None
            )
            result = observe(
                manifest,
                args.node,
                response,
                observed_at=args.observed_at,
                accepted_receipt=receipt,
            )
        elif args.command == "observe-all":
            selected = set(args.registry or ("pypi", "npm", "crates"))
            nodes, _, _ = _manifest_indexes(manifest)
            receipts: dict[str, dict[str, Any]] = {}
            if args.receipts_dir:
                for path in sorted(args.receipts_dir.glob("*.json")):
                    value = _load_json(path, context="accepted-write receipt")
                    node_id = value.get("node_id")
                    if (
                        value.get("schema") != "graphforge-release-accepted-receipt-v1"
                        or node_id not in nodes
                        or value.get("candidate_sha") != manifest["commit_sha"]
                        or value.get("version") != manifest["version"]
                    ):
                        raise RegistryError(
                            f"accepted-write receipt identity diverges: {path.name}"
                        )
                    if node_id in receipts:
                        raise RegistryError(f"duplicate accepted-write receipt: {node_id}")
                    receipts[node_id] = value
            attempts: dict[str, dict[str, Any]] = {}
            if args.attempts_dir:
                for path in sorted(args.attempts_dir.glob("*.json")):
                    value = _load_json(path, context="write attempt")
                    node_id = value.get("node_id")
                    if (
                        value.get("schema") != "graphforge-release-write-attempt-v1"
                        or node_id not in nodes
                        or value.get("candidate_sha") != manifest["commit_sha"]
                        or value.get("version") != manifest["version"]
                    ):
                        raise RegistryError(f"write attempt identity diverges: {path.name}")
                    _parse_time(value.get("started_at"), field=f"{path.name}.started_at")
                    if node_id in attempts:
                        raise RegistryError(f"duplicate write attempt: {node_id}")
                    attempts[node_id] = value

            def observe_node(node_id: str) -> dict[str, Any]:
                result = observe(
                    manifest,
                    node_id,
                    live_response(manifest, node_id),
                    observed_at=args.observed_at,
                    accepted_receipt=receipts.get(node_id),
                )
                if result["state"] == "absent" and node_id in attempts and node_id not in receipts:
                    result = {
                        **result,
                        "state": "indeterminate",
                        "reason": "write_attempt_outcome_unknown",
                        "evidence": {
                            **result["evidence"],
                            "attempt_started_at": attempts[node_id]["started_at"],
                        },
                    }
                return result

            result = {
                "schema": OBSERVATION_SET_SCHEMA,
                "candidate_sha": manifest["commit_sha"],
                "version": manifest["version"],
                "observations": [
                    observe_node(node_id)
                    for node_id, node in sorted(nodes.items())
                    if node["registry"] in selected
                ],
            }
            _assert_safe_output(result)
        else:
            observations = _load_json(args.observations, context="registry observations")
            availability = _load_json(args.availability, context="artifact availability")
            result = plan_recovery(
                manifest,
                observations,
                availability,
                planned_at=args.planned_at,
                registries=set(args.registry) if args.registry else None,
            )
        output = json.dumps(result, indent=2, sort_keys=True) + "\n"
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(output, encoding="utf-8")
        else:
            sys.stdout.write(output)
        return 0
    except RegistryError as error:
        print(f"release-registry: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
