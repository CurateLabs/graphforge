#!/usr/bin/env python3
"""Deterministic registry-observer and recovery-planner tests."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile

from release_candidate_manifest import CRATES, NATIVE_NPM_PACKAGES, NPM_PACKAGES
import release_registry as registry

VERSION = "0.5.1"
SHA = "a" * 40
NOW = "2030-01-01T12:02:00+00:00"


def artifact(path: str, group: str, surface: str, name: str, *, dependencies=None):
    data = name.encode()
    sha256 = hashlib.sha256(data).digest()
    sha512 = hashlib.sha512(data).digest()
    digest = sha256.hex()
    return {
        "path": path,
        "group": group,
        "surface": surface,
        "name": name,
        "version": VERSION,
        "filename": path.rsplit("/", 1)[-1],
        "sha256": digest,
        "integrity": "sha256-" + base64.b64encode(sha256).decode("ascii"),
        "integrities": [
            "sha256-" + base64.b64encode(sha256).decode("ascii"),
            "sha512-" + base64.b64encode(sha512).decode("ascii"),
        ],
        "archive": {
            "package": {
                "name": name,
                "version": VERSION,
                "dependencies": dependencies or {},
            }
        },
    }


def candidate() -> dict[str, object]:
    artifacts = []
    python_paths = []
    for filename in (
        f"graphforge-{VERSION}-linux.whl",
        f"graphforge-{VERSION}-macos.whl",
        f"graphforge-{VERSION}-windows.whl",
        f"graphforge-{VERSION}.tar.gz",
    ):
        path = f"python/{filename}"
        python_paths.append(path)
        artifacts.append(artifact(path, "python", "pypi", "graphforge"))
    nodes = [
        {
            "id": "pypi:graphforge",
            "registry": "pypi",
            "name": "graphforge",
            "artifact_paths": python_paths,
        }
    ]
    dependencies = []
    for name in NPM_PACKAGES:
        package_dependencies = {}
        if name == "@curatelabs/graphforge":
            package_dependencies = dict.fromkeys(NATIVE_NPM_PACKAGES, VERSION)
        elif name == "@curatelabs/graphforge-cli":
            package_dependencies = {"@curatelabs/graphforge": VERSION}
        path = f"npm/{name.removeprefix('@curatelabs/').replace('/', '-')}-{VERSION}.tgz"
        artifacts.append(artifact(path, "npm", "npm", name, dependencies=package_dependencies))
        node_id = f"npm:{name}"
        nodes.append({"id": node_id, "registry": "npm", "name": name, "artifact_paths": [path]})
        for dependency in package_dependencies:
            dependencies.append({"from": node_id, "requires": f"npm:{dependency}"})
    dependencies.append(
        {
            "from": "npm:@curatelabs/graphforge-agent-skills",
            "requires": "npm:@curatelabs/graphforge-cli",
        }
    )
    for name in CRATES:
        package_dependencies = {} if name == "graphforge-core" else {"graphforge-core": VERSION}
        path = f"crates/{name}-{VERSION}.crate"
        artifacts.append(
            artifact(path, "crates", "crates", name, dependencies=package_dependencies)
        )
        node_id = f"crates:{name}"
        nodes.append({"id": node_id, "registry": "crates", "name": name, "artifact_paths": [path]})
        for dependency in package_dependencies:
            dependencies.append({"from": node_id, "requires": f"crates:{dependency}"})
    return {
        "schema": "graphforge-release-candidate-v2",
        "version": VERSION,
        "tag": f"v{VERSION}",
        "commit_sha": SHA,
        "recorded_at": "2029-12-31T12:00:00+00:00",
        "nodes": sorted(nodes, key=lambda item: item["id"]),
        "dependencies": sorted(dependencies, key=lambda item: (item["from"], item["requires"])),
        "artifacts": sorted(artifacts, key=lambda item: item["path"]),
        "artifact_groups": [
            {
                "id": group,
                "expires_at": "2030-01-30T12:00:00+00:00",
                "artifact_paths": [item["path"] for item in artifacts if item["group"] == group],
            }
            for group in ("python", "npm", "crates", "evidence")
        ],
    }


def response_for(manifest: dict[str, object], node_id: str) -> dict[str, object]:
    expected = registry._node_expected(manifest, node_id)
    node = expected["node"]
    if node["registry"] == "pypi":
        return {
            "status": 200,
            "json": {
                "info": {"name": node["name"], "version": VERSION, "license": "Apache-2.0"},
                "urls": [
                    {"filename": item["filename"], "digests": {"sha256": item["sha256"]}}
                    for item in expected["artifacts"]
                ],
            },
        }
    if node["registry"] == "npm":
        package = expected["artifacts"][0]["archive"]["package"]
        payload = {
            "name": node["name"],
            "version": VERSION,
            "license": "Apache-2.0",
            "dist": {"integrity": expected["artifacts"][0]["integrities"][1]},
        }
        if package["dependencies"]:
            field = (
                "optionalDependencies"
                if node["name"] == "@curatelabs/graphforge"
                else "dependencies"
            )
            payload[field] = package["dependencies"]
        return {"status": 200, "json": payload}
    return {
        "status": 200,
        "json": {
            "version": {
                "crate": node["name"],
                "num": VERSION,
                "checksum": expected["artifacts"][0]["sha256"],
                "yanked": False,
                "license": "Apache-2.0",
            },
            "owners": {"users": [{"login": "DecisionNerd"}]},
        },
    }


def observed(manifest, node_id, response=None, receipt=None, at=NOW):
    return registry.observe(
        manifest,
        node_id,
        response or response_for(manifest, node_id),
        observed_at=at,
        accepted_receipt=receipt,
    )


def observation_set(manifest):
    return {
        "schema": registry.OBSERVATION_SET_SCHEMA,
        "candidate_sha": SHA,
        "version": VERSION,
        "observations": [observed(manifest, node["id"]) for node in manifest["nodes"]],
    }


def replace_observation(values, replacement):
    values["observations"] = [
        replacement if item["node_id"] == replacement["node_id"] else item
        for item in values["observations"]
    ]


def plan(manifest, observations, availability=None, registries=None, at=NOW):
    return registry.plan_recovery(
        manifest,
        observations,
        availability or dict.fromkeys(("python", "npm", "crates", "evidence"), True),
        planned_at=at,
        registries=registries,
    )


def assert_state(value, expected):
    assert value["state"] == expected, value


def main() -> None:
    manifest = candidate()
    all_verified = observation_set(manifest)
    result = plan(manifest, all_verified)
    assert result["actions"] == []
    assert result["download_groups"] == []
    assert result["summary"]["verified"] == 25

    for node_id in (
        "pypi:graphforge",
        "npm:@curatelabs/graphforge",
        "crates:graphforge-core",
    ):
        assert_state(observed(manifest, node_id), "verified")
        assert_state(observed(manifest, node_id, {"status": 404}), "absent")
        assert_state(observed(manifest, node_id, {"status": 403}), "failed")
        assert_state(
            observed(manifest, node_id, {"status": 429, "retry_after_seconds": 30}), "indeterminate"
        )
        assert_state(observed(manifest, node_id, {"status": 200, "json": []}), "indeterminate")

    pypi_conflict = response_for(manifest, "pypi:graphforge")
    pypi_conflict["json"]["urls"][0]["digests"]["sha256"] = "f" * 64
    assert_state(observed(manifest, "pypi:graphforge", pypi_conflict), "conflict")
    npm_conflict = response_for(manifest, "npm:@curatelabs/graphforge")
    npm_conflict["json"]["dist"]["integrity"] = "sha512-" + base64.b64encode(
        hashlib.sha512(b"conflict").digest()
    ).decode("ascii")
    assert_state(observed(manifest, "npm:@curatelabs/graphforge", npm_conflict), "conflict")
    npm_malformed = response_for(manifest, "npm:@curatelabs/graphforge")
    npm_malformed["json"]["dist"]["integrity"] = "sha512-not-base64"
    assert_state(observed(manifest, "npm:@curatelabs/graphforge", npm_malformed), "indeterminate")
    crates_conflict = response_for(manifest, "crates:graphforge-core")
    crates_conflict["json"]["version"]["checksum"] = "f" * 64
    assert_state(observed(manifest, "crates:graphforge-core", crates_conflict), "conflict")

    receipt = {
        "schema": "graphforge-release-accepted-receipt-v1",
        "node_id": "npm:@curatelabs/graphforge",
        "version": VERSION,
        "candidate_sha": SHA,
        "accepted_at": "2030-01-01T12:00:00+00:00",
        "visibility_deadline": "2030-01-01T12:10:00+00:00",
        "observation_count": 0,
        "authorization": "must-not-escape",
    }
    pending = observed(
        manifest,
        "npm:@curatelabs/graphforge",
        {"status": 404, "json": {"token": "must-not-escape"}},
        receipt,
    )
    assert_state(pending, "accepted_pending_visibility")
    assert "must-not-escape" not in json.dumps(pending)
    exhausted = observed(
        manifest,
        "npm:@curatelabs/graphforge",
        {"status": 404},
        {**receipt, "observation_count": registry.MAX_VISIBILITY_OBSERVATIONS - 1},
    )
    assert_state(exhausted, "indeterminate")

    observations = copy.deepcopy(all_verified)
    absent_pypi = observed(manifest, "pypi:graphforge", {"status": 404})
    replace_observation(observations, absent_pypi)
    result = plan(manifest, observations)
    assert [action["node_id"] for action in result["actions"]] == ["pypi:graphforge"]
    assert result["actions"][0]["kind"] == "publish"
    assert result["download_groups"] == ["python"]

    observations = copy.deepcopy(all_verified)
    replace_observation(observations, pending)
    result = plan(manifest, observations)
    assert result["actions"] == [
        {"node_id": "npm:@curatelabs/graphforge", "kind": "verify_visibility", "registry": "npm"}
    ]
    assert result["download_groups"] == []

    observations = copy.deepcopy(all_verified)
    for name in NATIVE_NPM_PACKAGES:
        replace_observation(observations, observed(manifest, f"npm:{name}", {"status": 404}))
    replace_observation(
        observations,
        observed(manifest, "npm:@curatelabs/graphforge", {"status": 404}),
    )
    result = plan(manifest, observations, registries={"npm"})
    publishes = [action["node_id"] for action in result["actions"] if action["kind"] == "publish"]
    assert publishes == sorted(f"npm:{name}" for name in NATIVE_NPM_PACKAGES)
    main_decision = next(
        item for item in result["decisions"] if item["node_id"] == "npm:@curatelabs/graphforge"
    )
    assert main_decision["disposition"] == "blocked_dependencies"

    unavailable = dict.fromkeys(("python", "npm", "crates", "evidence"), True)
    unavailable["npm"] = False
    result = plan(manifest, observations, unavailable, registries={"npm"})
    assert not any(action["kind"] == "publish" for action in result["actions"])
    assert result["download_groups"] == []

    observations = copy.deepcopy(all_verified)
    replace_observation(
        observations,
        observed(manifest, "crates:graphforge-core", {"status": 404}),
    )
    replace_observation(
        observations,
        observed(manifest, "crates:graphforge-api", {"status": 404}),
    )
    result = plan(manifest, observations, registries={"crates"})
    assert [action["node_id"] for action in result["actions"]] == ["crates:graphforge-core"]
    assert result["download_groups"] == ["crates"]

    observations = copy.deepcopy(all_verified)
    core_conflict = response_for(manifest, "crates:graphforge-core")
    core_conflict["json"]["version"]["checksum"] = "f" * 64
    replace_observation(
        observations,
        observed(manifest, "crates:graphforge-core", core_conflict),
    )
    replace_observation(
        observations,
        observed(manifest, "crates:graphforge-api", {"status": 404}),
    )
    result = plan(manifest, observations, registries={"crates"})
    api_blocker = next(
        item for item in result["blockers"] if item["node_id"] == "crates:graphforge-api"
    )
    assert api_blocker["dependency_states"] == {"crates:graphforge-core": "conflict"}
    assert not any(action["node_id"] == "crates:graphforge-api" for action in result["actions"])

    no_observations = {
        "schema": registry.OBSERVATION_SET_SCHEMA,
        "candidate_sha": SHA,
        "version": VERSION,
        "observations": [],
    }
    result = plan(manifest, no_observations, registries={"pypi"})
    assert result["actions"] == [
        {"node_id": "pypi:graphforge", "kind": "observe", "registry": "pypi"}
    ]
    assert result["download_groups"] == []

    job_history = copy.deepcopy(all_verified)
    job_history["github_actions_job_history"] = {"publish-npm": "failure"}
    assert plan(manifest, job_history) == plan(manifest, all_verified)

    divergent = copy.deepcopy(all_verified)
    divergent["observations"][0]["version"] = "0.5.2"
    try:
        plan(manifest, divergent)
    except registry.RegistryError as error:
        assert "identity diverges" in str(error)
    else:
        raise AssertionError("version-divergent observation was accepted")

    forged_absence = copy.deepcopy(all_verified)
    forged_absence["observations"][0].update(
        {"state": "absent", "reason": "crates_authoritative_not_found"}
    )
    try:
        plan(manifest, forged_absence)
    except registry.RegistryError as error:
        assert "authoritative evidence" in str(error)
    else:
        raise AssertionError("absence without a registry 404 was accepted")

    stale = copy.deepcopy(all_verified)
    stale["observations"][0]["observed_at"] = "2029-12-31T00:00:00+00:00"
    result = plan(manifest, stale)
    assert result["blockers"][0]["reason"] == "registry_state_indeterminate"

    expired = copy.deepcopy(manifest)
    for group in expired["artifact_groups"]:
        if group["id"] == "python":
            group["expires_at"] = "2030-01-01T12:01:00+00:00"
    observations = copy.deepcopy(all_verified)
    replace_observation(observations, absent_pypi)
    result = plan(expired, observations)
    assert result["actions"] == []
    assert result["blockers"][0]["reason"] == "artifact_group_unavailable_or_expired"

    crates_only = plan(manifest, all_verified, registries={"crates"})
    assert crates_only["download_groups"] == []
    assert all(item["registry"] == "crates" for item in crates_only["decisions"])
    assert not any(word in json.dumps(result).lower() for word in ("password", "secret", "token"))
    source = Path(registry.__file__).read_text(encoding="utf-8")
    assert "time.sleep" not in source
    assert "while " not in source

    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        manifest_path = root / "manifest.json"
        response_path = root / "response.json"
        observation_path = root / "observation.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        response_path.write_text(
            json.dumps(response_for(manifest, "pypi:graphforge")), encoding="utf-8"
        )
        assert (
            registry.main(
                [
                    "observe",
                    "--manifest",
                    str(manifest_path),
                    "--node",
                    "pypi:graphforge",
                    "--response",
                    str(response_path),
                    "--observed-at",
                    NOW,
                    "--out",
                    str(observation_path),
                ]
            )
            == 0
        )
        assert json.loads(observation_path.read_text(encoding="utf-8"))["state"] == "verified"
    print("release-registry tests: ok")


if __name__ == "__main__":
    main()
