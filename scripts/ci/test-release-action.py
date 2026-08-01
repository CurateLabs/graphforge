#!/usr/bin/env python3
"""Mutation tests for partition validation and one-node authorization."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile

import release_action as action
import release_registry as registry


def _module(filename: str, name: str):
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


candidate_fixture = _module("test-release-candidate.py", "release_action_candidate_fixture")
registry_fixture = _module("test-release-registry.py", "release_action_registry_fixture")


def test_partition() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        _, artifacts, manifest = candidate_fixture.create_candidate(root / "source")
        partition = root / "partition"
        npm_paths = next(
            group["artifact_paths"] for group in manifest["artifact_groups"] if group["id"] == "npm"
        )
        for relative in npm_paths:
            destination = partition / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(artifacts / relative, destination)
        report = action.validate_partition(
            manifest,
            partition,
            "npm",
            expected_sha=candidate_fixture.SHA,
            version=candidate_fixture.VERSION,
            checked_at=manifest["recorded_at"],
        )
        assert report["status"] == "passed"
        assert len(report["artifact_paths"]) == 8
        target = partition / npm_paths[0]
        original = target.read_bytes()
        target.write_bytes(b"\x00" * len(original))
        try:
            action.validate_partition(
                manifest,
                partition,
                "npm",
                expected_sha=candidate_fixture.SHA,
                version=candidate_fixture.VERSION,
                checked_at=manifest["recorded_at"],
            )
        except action.ActionError as error:
            assert "checksum diverges" in str(error)
        else:
            raise AssertionError("same-size mutation passed validation")
        target.write_bytes(b"different")
        try:
            action.validate_partition(
                manifest,
                partition,
                "npm",
                expected_sha=candidate_fixture.SHA,
                version=candidate_fixture.VERSION,
                checked_at=manifest["recorded_at"],
            )
        except action.ActionError as error:
            assert "byte count diverges" in str(error)
        else:
            raise AssertionError("mutated partition passed validation")


def test_authorization() -> None:
    manifest = registry_fixture.candidate()
    verified = registry_fixture.observation_set(manifest)
    availability = dict.fromkeys(("python", "npm", "crates", "evidence"), True)
    skipped = action.authorize(
        manifest,
        verified,
        availability,
        "pypi:graphforge",
        planned_at=registry_fixture.NOW,
    )
    assert skipped["disposition"] == "skip_verified"
    assert skipped["publish"] is False

    absent = copy.deepcopy(verified)
    replacement = registry_fixture.observed(manifest, "pypi:graphforge", {"status": 404})
    registry_fixture.replace_observation(absent, replacement)
    authorized = action.authorize(
        manifest,
        absent,
        availability,
        "pypi:graphforge",
        planned_at=registry_fixture.NOW,
    )
    assert authorized["publish"] is True
    assert authorized["credential_scope"] == "pypi-oidc"

    blocked = copy.deepcopy(verified)
    registry_fixture.replace_observation(
        blocked,
        registry_fixture.observed(
            manifest, "npm:@curatelabs/graphforge-linux-x64-gnu", {"status": 404}
        ),
    )
    registry_fixture.replace_observation(
        blocked,
        registry_fixture.observed(manifest, "npm:@curatelabs/graphforge", {"status": 404}),
    )
    try:
        action.authorize(
            manifest,
            blocked,
            availability,
            "npm:@curatelabs/graphforge",
            planned_at=registry_fixture.NOW,
        )
    except action.ActionError as error:
        assert "blocked_dependencies" in str(error)
    else:
        raise AssertionError("dependency-blocked npm main was authorized")

    receipt = action.accepted_receipt(
        manifest, "pypi:graphforge", accepted_at="2030-01-01T12:00:00Z"
    )
    assert receipt == {
        "schema": "graphforge-release-accepted-receipt-v1",
        "node_id": "pypi:graphforge",
        "version": manifest["version"],
        "candidate_sha": manifest["commit_sha"],
        "accepted_at": "2030-01-01T12:00:00+00:00",
        "visibility_deadline": "2030-01-01T12:15:00+00:00",
        "observation_count": 0,
    }
    attempt = action.write_attempt(manifest, "pypi:graphforge", started_at="2030-01-01T11:59:00Z")
    assert attempt["schema"] == "graphforge-release-write-attempt-v1"
    assert attempt["candidate_sha"] == manifest["commit_sha"]


def test_observe_all() -> None:
    manifest = registry_fixture.candidate()
    original = registry.live_response

    def fixture_response(value, node_id):
        return registry_fixture.response_for(value, node_id)

    registry.live_response = fixture_response
    try:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_path = root / "manifest.json"
            output = root / "observations.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            assert (
                registry.main(
                    [
                        "observe-all",
                        "--manifest",
                        str(manifest_path),
                        "--registry",
                        "npm",
                        "--observed-at",
                        registry_fixture.NOW,
                        "--out",
                        str(output),
                    ]
                )
                == 0
            )
            observations = json.loads(output.read_text(encoding="utf-8"))
            assert len(observations["observations"]) == 8
            assert all(value["state"] == "verified" for value in observations["observations"])

            receipts = root / "receipts"
            receipts.mkdir()
            (receipts / "pypi.json").write_text(
                json.dumps(
                    action.accepted_receipt(
                        manifest,
                        "pypi:graphforge",
                        accepted_at="2030-01-01T12:00:00Z",
                    )
                ),
                encoding="utf-8",
            )

            def not_visible(_value, _node_id):
                return {"status": 404}

            registry.live_response = not_visible
            assert (
                registry.main(
                    [
                        "observe-all",
                        "--manifest",
                        str(manifest_path),
                        "--registry",
                        "pypi",
                        "--receipts-dir",
                        str(receipts),
                        "--observed-at",
                        registry_fixture.NOW,
                        "--out",
                        str(output),
                    ]
                )
                == 0
            )
            pending = json.loads(output.read_text(encoding="utf-8"))["observations"]
            assert pending[0]["state"] == "accepted_pending_visibility"

            receipts.joinpath("pypi.json").unlink()
            attempts = root / "attempts"
            attempts.mkdir()
            (attempts / "pypi.json").write_text(
                json.dumps(
                    action.write_attempt(
                        manifest,
                        "pypi:graphforge",
                        started_at="2030-01-01T11:59:00Z",
                    )
                ),
                encoding="utf-8",
            )
            assert (
                registry.main(
                    [
                        "observe-all",
                        "--manifest",
                        str(manifest_path),
                        "--registry",
                        "pypi",
                        "--attempts-dir",
                        str(attempts),
                        "--observed-at",
                        registry_fixture.NOW,
                        "--out",
                        str(output),
                    ]
                )
                == 0
            )
            unknown = json.loads(output.read_text(encoding="utf-8"))["observations"]
            assert unknown[0]["state"] == "indeterminate"
            assert unknown[0]["reason"] == "write_attempt_outcome_unknown"
            assert unknown[0]["evidence"]["attempt_started_at"] == ("2030-01-01T11:59:00+00:00")

            receipts.mkdir(exist_ok=True)
            bad_receipt = action.accepted_receipt(
                manifest,
                "pypi:graphforge",
                accepted_at="2030-01-01T12:00:00Z",
            )
            bad_receipt["candidate_sha"] = "f" * 40
            receipts.joinpath("pypi.json").write_text(json.dumps(bad_receipt), encoding="utf-8")
            assert (
                registry.main(
                    [
                        "observe-all",
                        "--manifest",
                        str(manifest_path),
                        "--registry",
                        "pypi",
                        "--receipts-dir",
                        str(receipts),
                        "--observed-at",
                        registry_fixture.NOW,
                        "--out",
                        str(output),
                    ]
                )
                == 1
            )
    finally:
        registry.live_response = original


def main() -> None:
    test_partition()
    test_authorization()
    test_observe_all()
    print("release-action tests: ok")


if __name__ == "__main__":
    main()
