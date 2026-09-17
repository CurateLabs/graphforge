#!/usr/bin/env python3
"""Deterministic registry-observer and recovery-planner tests."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile

from release_candidate_manifest import CRATES, NATIVE_NPM_PACKAGES, NPM_PACKAGES, python_spelling
import release_registry as registry

VERSION = "0.5.1"
PRERELEASE = "0.6.0-rc.1"
SHA = "a" * 40
NOW = "2030-01-01T12:02:00+00:00"
# One PyPI node, every npm package, and every crates.io crate (#1373).
NODE_COUNT = 1 + len(NPM_PACKAGES) + len(CRATES)


def artifact(path: str, group: str, surface: str, name: str, *, dependencies=None, version=VERSION):
    data = name.encode()
    sha256 = hashlib.sha256(data).digest()
    sha512 = hashlib.sha512(data).digest()
    digest = sha256.hex()
    return {
        "path": path,
        "group": group,
        "surface": surface,
        "name": name,
        "version": version,
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
                "version": version,
                "dependencies": dependencies or {},
            }
        },
    }


def candidate(version: str = VERSION) -> dict[str, object]:
    # One logical version; Python artifacts carry its PEP 440 spelling (ADR 0033).
    python_version = python_spelling(version)
    artifacts = []
    python_paths = []
    for filename in (
        f"graphforge-{python_version}-linux.whl",
        f"graphforge-{python_version}-macos.whl",
        f"graphforge-{python_version}-windows.whl",
        f"graphforge-{python_version}.tar.gz",
    ):
        path = f"python/{filename}"
        python_paths.append(path)
        artifacts.append(artifact(path, "python", "pypi", "graphforge", version=version))
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
            package_dependencies = dict.fromkeys(NATIVE_NPM_PACKAGES, version)
        elif name == "@curatelabs/graphforge-cli":
            package_dependencies = {"@curatelabs/graphforge": version}
        path = f"npm/{name.removeprefix('@curatelabs/').replace('/', '-')}-{version}.tgz"
        artifacts.append(
            artifact(path, "npm", "npm", name, dependencies=package_dependencies, version=version)
        )
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
        package_dependencies = {} if name == "graphforge-core" else {"graphforge-core": version}
        path = f"crates/{name}-{version}.crate"
        artifacts.append(
            artifact(
                path, "crates", "crates", name, dependencies=package_dependencies, version=version
            )
        )
        node_id = f"crates:{name}"
        nodes.append({"id": node_id, "registry": "crates", "name": name, "artifact_paths": [path]})
        for dependency in package_dependencies:
            dependencies.append({"from": node_id, "requires": f"crates:{dependency}"})
    return {
        "schema": "graphforge-release-candidate-v2",
        "version": version,
        "python_version": python_version,
        "tag": f"v{version}",
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
                "info": {
                    "name": node["name"],
                    "version": expected["registry_version"],
                    "license": "Apache-2.0",
                },
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
            "version": expected["registry_version"],
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
                "num": expected["registry_version"],
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
        "version": manifest["version"],
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
    assert result["summary"]["verified"] == NODE_COUNT

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

    prerelease_observers()
    print("release-registry tests: ok")


def prerelease_observers() -> None:
    """A prerelease is one version; PyPI alone answers in PEP 440 spelling."""
    manifest = candidate(PRERELEASE)
    assert manifest["version"] == "0.6.0-rc.1"
    assert manifest["python_version"] == "0.6.0rc1"

    pypi = registry._node_expected(manifest, "pypi:graphforge")
    assert pypi["version"] == "0.6.0-rc.1"
    assert pypi["registry_version"] == "0.6.0rc1"
    npm = registry._node_expected(manifest, "npm:@curatelabs/graphforge")
    assert npm["registry_version"] == "0.6.0-rc.1"
    crates = registry._node_expected(manifest, "crates:graphforge-core")
    assert crates["registry_version"] == "0.6.0-rc.1"

    observation = observed(manifest, "pypi:graphforge")
    assert observation["state"] == "verified", observation
    # The observation records the one root version; only the endpoint normalizes.
    assert observation["version"] == "0.6.0-rc.1"
    assert observation["endpoint"] == "https://pypi.org/pypi/graphforge/0.6.0rc1/json"

    # The cargo spelling from PyPI is a genuine conflict, not the expected answer.
    raw = response_for(manifest, "pypi:graphforge")
    raw["json"]["info"]["version"] = "0.6.0-rc.1"
    conflict = observed(manifest, "pypi:graphforge", raw)
    assert conflict["state"] == "conflict", conflict
    assert conflict["reason"] == "pypi_identity_mismatch"

    for node_id in ("npm:@curatelabs/graphforge", "crates:graphforge-core"):
        assert observed(manifest, node_id)["state"] == "verified"

    # A second, independently chosen Python version is refused outright.
    forged = copy.deepcopy(manifest)
    forged["python_version"] = "0.6.0"
    try:
        registry._node_expected(forged, "pypi:graphforge")
    except registry.RegistryError as error:
        assert "derived PEP 440 spelling" in str(error), error
    else:
        raise AssertionError("an overridden python_version was accepted")

    plan_result = plan(manifest, observation_set(manifest))
    assert plan_result["blockers"] == []
    assert plan_result["summary"]["verified"] == NODE_COUNT


if __name__ == "__main__":
    main()
