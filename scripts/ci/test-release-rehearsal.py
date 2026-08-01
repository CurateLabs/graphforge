#!/usr/bin/env python3
"""Deterministic tests for offline rehearsal and sequential reconciliation."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import tempfile

import release_candidate_manifest as candidate_contract
import release_registry as registry
import release_rehearsal as rehearsal


def _module(filename: str, name: str):
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


registry_fixture = _module("test-release-registry.py", "release_registry_fixture")
candidate_fixture = _module("test-release-candidate.py", "release_candidate_fixture")
NOW = registry_fixture.NOW


def _availability(value: bool = True) -> dict[str, bool]:
    return dict.fromkeys(candidate_contract.GROUPS, value)


def _set(manifest, values):
    return {
        "schema": registry.OBSERVATION_SET_SCHEMA,
        "candidate_sha": manifest["commit_sha"],
        "version": manifest["version"],
        "observations": sorted(values, key=lambda item: item["node_id"]),
    }


def _all(manifest, response):
    return _set(
        manifest,
        [registry_fixture.observed(manifest, node["id"], response) for node in manifest["nodes"]],
    )


def _replace(values, replacement):
    result = copy.deepcopy(values)
    result["observations"] = [
        replacement if item["node_id"] == replacement["node_id"] else item
        for item in result["observations"]
    ]
    return result


def _verified(manifest, node_id):
    return registry_fixture.observed(manifest, node_id)


def _absent(manifest, node_id):
    return registry_fixture.observed(manifest, node_id, {"status": 404})


def _sequential_happy_path(manifest):
    current = _all(manifest, {"status": 404})
    transitions = []
    while True:
        plan = registry_fixture.plan(manifest, current)
        publishes = [action for action in plan["actions"] if action["kind"] == "publish"]
        if not publishes:
            break
        action = publishes[0]
        observation = _verified(manifest, action["node_id"])
        transitions.append(
            {
                "node_id": action["node_id"],
                "action_kind": "publish",
                "job_outcome": "success",
                "observation": observation,
            }
        )
        current = _replace(current, observation)
    return transitions


def test_artifact_rehearsal() -> None:
    original_python = rehearsal._python_consumer

    def fake_python(manifest, _artifacts, _root):
        return {
            "artifact": "python/fixture.whl",
            "imported_version": manifest["version"],
            "status": "passed",
        }

    rehearsal._python_consumer = fake_python
    try:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path, artifacts, manifest = candidate_fixture.create_candidate(root)
            report = rehearsal.rehearse_artifacts(
                manifest_path,
                artifacts,
                expected_sha=candidate_fixture.SHA,
                version=candidate_fixture.VERSION,
                rehearsed_at=manifest["recorded_at"],
            )
            assert report["status"] == "passed"
            assert report["registry_writes"] == 0
            assert report["checks"]["candidate_completeness"]["nodes"] == 24
            assert report["checks"]["node_cli_skills_clean_consumer"]["loaded_version"] == (
                candidate_fixture.VERSION
            )
            assert len(report["checks"]["rust_packages"]["packages"]) == 15
            assert not any(word in json.dumps(report).lower() for word in rehearsal.FORBIDDEN_TEXT)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path, artifacts, manifest = candidate_fixture.create_candidate(root)
            main_name = "@curatelabs/graphforge"
            main_path = next(
                artifacts / item["path"]
                for item in manifest["artifacts"]
                if item.get("name") == main_name
            )
            members = candidate_fixture.npm_members(main_name)
            members["package/index.js"] = b"exports.notVersion = true;\n"
            candidate_fixture.write_tar(main_path, members)
            rebuilt = candidate_contract.build_manifest(
                version=candidate_fixture.VERSION,
                dist_dir=artifacts,
                commit_sha=candidate_fixture.SHA,
                recorded_at=manifest["recorded_at"],
                notes="missing runtime entrypoint fixture",
            )
            manifest_path.write_text(
                json.dumps(rebuilt, indent=2, sort_keys=True), encoding="utf-8"
            )
            try:
                rehearsal.rehearse_artifacts(
                    manifest_path,
                    artifacts,
                    expected_sha=candidate_fixture.SHA,
                    version=candidate_fixture.VERSION,
                    rehearsed_at=rebuilt["recorded_at"],
                )
            except rehearsal.RehearsalError as error:
                assert "offline consumer node failed" in str(error)
            else:
                raise AssertionError("missing Node runtime entrypoint passed rehearsal")
    finally:
        rehearsal._python_consumer = original_python


def test_sequential_reconciliation() -> None:
    manifest = registry_fixture.candidate()
    availability = _availability()
    absent = _all(manifest, {"status": 404})
    transitions = _sequential_happy_path(manifest)
    assert len(transitions) == 24
    report = rehearsal.simulate_sequential(
        manifest, absent, availability, transitions, simulated_at=NOW
    )
    assert report["complete"] is True
    assert report["summary"]["nodes"] == 24
    assert report["summary"]["verified"] == 24
    assert len(report["events"]) == 24
    assert all(event["sequence"] == index for index, event in enumerate(report["events"], 1))

    all_verified = registry_fixture.observation_set(manifest)
    pypi_absent = _replace(all_verified, _absent(manifest, "pypi:graphforge"))
    pypi_report = rehearsal.reconcile(
        manifest,
        pypi_absent,
        availability,
        reconciled_at=NOW,
        job_outcomes={"pypi:graphforge": "cancelled"},
    )
    assert [action["node_id"] for action in pypi_report["next_actions"]] == ["pypi:graphforge"]
    pypi_node = next(node for node in pypi_report["nodes"] if node["node_id"] == "pypi:graphforge")
    assert pypi_node["job_outcome"] == "cancelled"
    assert pypi_node["disposition"] == "publish"

    timed_out = rehearsal.reconcile(
        manifest,
        all_verified,
        availability,
        reconciled_at=NOW,
        job_outcomes={"pypi:graphforge": "timed_out"},
    )
    assert timed_out["complete"] is True
    assert timed_out["next_actions"] == []

    cli_absent = _replace(all_verified, _absent(manifest, "npm:@curatelabs/graphforge-cli"))
    skipped = rehearsal.reconcile(
        manifest,
        cli_absent,
        availability,
        reconciled_at=NOW,
        job_outcomes={"npm:@curatelabs/graphforge-cli": "skipped"},
    )
    assert skipped["next_actions"][0]["node_id"] == "npm:@curatelabs/graphforge-cli"
    assert skipped["next_actions"][0]["kind"] == "publish"

    receipt = {
        "schema": "graphforge-release-accepted-receipt-v1",
        "node_id": "npm:@curatelabs/graphforge",
        "version": manifest["version"],
        "candidate_sha": manifest["commit_sha"],
        "accepted_at": "2030-01-01T12:00:00+00:00",
        "visibility_deadline": "2030-01-01T12:10:00+00:00",
        "observation_count": 0,
    }
    pending_observation = registry_fixture.observed(
        manifest,
        "npm:@curatelabs/graphforge",
        {"status": 404},
        receipt,
    )
    pending = rehearsal.reconcile(
        manifest,
        _replace(all_verified, pending_observation),
        availability,
        reconciled_at=NOW,
        job_outcomes={"npm:@curatelabs/graphforge": "success"},
    )
    assert pending["next_actions"] == [
        {
            "node_id": "npm:@curatelabs/graphforge",
            "kind": "verify_visibility",
            "registry": "npm",
        }
    ]

    conflict_response = registry_fixture.response_for(manifest, "crates:graphforge-core")
    conflict_response["json"]["version"]["checksum"] = "f" * 64
    conflict_observation = registry_fixture.observed(
        manifest, "crates:graphforge-core", conflict_response
    )
    conflict = rehearsal.reconcile(
        manifest,
        _replace(all_verified, conflict_observation),
        availability,
        reconciled_at=NOW,
    )
    assert conflict["complete"] is False
    assert conflict["blockers"][0]["reason"] == "registry_state_conflict"

    indeterminate_observation = registry_fixture.observed(
        manifest, "pypi:graphforge", {"status": 429}
    )
    indeterminate = rehearsal.reconcile(
        manifest,
        _replace(all_verified, indeterminate_observation),
        availability,
        reconciled_at=NOW,
    )
    assert indeterminate["blockers"][0]["reason"] == "registry_state_indeterminate"

    expired_manifest = copy.deepcopy(manifest)
    for group in expired_manifest["artifact_groups"]:
        if group["id"] == "python":
            group["expires_at"] = "2030-01-01T12:01:00+00:00"
    expired = rehearsal.reconcile(expired_manifest, pypi_absent, availability, reconciled_at=NOW)
    assert expired["blockers"][0]["reason"] == "artifact_group_unavailable_or_expired"

    partial_npm = copy.deepcopy(all_verified)
    missing_native = "npm:@curatelabs/graphforge-linux-x64-gnu"
    partial_npm = _replace(partial_npm, _absent(manifest, missing_native))
    partial_npm = _replace(partial_npm, _absent(manifest, "npm:@curatelabs/graphforge"))
    partial = rehearsal.reconcile(manifest, partial_npm, availability, reconciled_at=NOW)
    assert [action["node_id"] for action in partial["next_actions"]] == [missing_native]

    partial_crates = copy.deepcopy(all_verified)
    partial_crates = _replace(partial_crates, _absent(manifest, "crates:graphforge-core"))
    partial_crates = _replace(partial_crates, _absent(manifest, "crates:graphforge-api"))
    crates = rehearsal.reconcile(manifest, partial_crates, availability, reconciled_at=NOW)
    assert [action["node_id"] for action in crates["next_actions"]] == ["crates:graphforge-core"]

    assert rehearsal.reconcile(
        manifest, pypi_absent, availability, reconciled_at=NOW
    ) == rehearsal.reconcile(manifest, pypi_absent, availability, reconciled_at=NOW)
    assert not any(word in json.dumps(report).lower() for word in rehearsal.FORBIDDEN_TEXT)


def main() -> None:
    test_artifact_rehearsal()
    test_sequential_reconciliation()
    print("release-rehearsal tests: ok")


if __name__ == "__main__":
    main()
