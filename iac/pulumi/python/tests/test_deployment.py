from __future__ import annotations

import json
import unittest
from copy import deepcopy
from pathlib import Path
from typing import Any

import pulumi
import pytest
from pulumi.runtime import MockCallArgs, MockResourceArgs, Mocks

from graphforge_pulumi import (
    DeploymentSpec,
    render_deployment_spec,
    render_deployment_spec_json,
)

FIXTURE = (
    Path(__file__).parents[4] / "docs" / "contracts" / "examples" / "graphforge-resolved-v1.json"
)
RESOLVED = json.loads(FIXTURE.read_text())
DEPLOYMENT_FIXTURE = (
    Path(__file__).parents[4]
    / "docs"
    / "contracts"
    / "examples"
    / "graphforge-deployment-spec-production-v1.json"
)
PRODUCTION_SHA = "c" * 64
PRODUCTION_LOCATOR = f"registry.example/graphforge/runtime@sha256:{PRODUCTION_SHA}"


def test_renderer_emits_frozen_provider_neutral_projection() -> None:
    spec = render_deployment_spec(RESOLVED, "production", PRODUCTION_LOCATOR)
    assert spec == {
        "contract": "graphforge-deployment-spec/1",
        "resolved_config_sha256": (
            "eb9bd7c49ae277c62892b028d2c93e8328c24dd3e3fd5ecbabb4f2c94637e9e7"
        ),
        "target_id": "production",
        "artifact": {
            "kind": "oci_image",
            "locator": PRODUCTION_LOCATOR,
            "sha256": PRODUCTION_SHA,
            "version": "0.5.1",
        },
        "topology": {
            "execution": "container",
            "kind": "service",
            "ownership": "external",
            "replicas": 2,
            "scheduling": "long_running",
        },
        "requirements": {
            "backup": {"checkpoints": True, "retention_count": 14},
            "health": {"timeout_seconds": 30},
            "network": {"exposure": "private", "port": 8443, "tls_required": True},
            "observability": {"logs": True, "metrics": True, "traces": False},
            "resources": {"cpu_millis": 1000, "memory_bytes": 2147483648},
            "storage": {
                "capacity_bytes": 10737418240,
                "kind": "volume",
                "persistent": True,
            },
            "write": {"mode": "queued_writer", "queue_capacity": 64},
        },
        "bindings": {"secret_ids": ["service-token"], "source_ids": ["example-data"]},
        "ownership": {
            "data": "external",
            "infrastructure": "caller_owned",
            "runtime": "caller_owned",
            "specification": "graphforge",
        },
        "infrastructure": {"mutation": "none", "status": "caller_owned"},
        "connectivity": {"status": "not_checked"},
        "readiness": {"status": "not_checked"},
        "capability_compatibility": {
            "requirements": [
                {"id": "graph", "version": 1},
                {"id": "workspace", "version": 1},
            ],
            "status": "requirements_declared",
        },
    }
    encoded = render_deployment_spec_json(RESOLVED, "production", PRODUCTION_LOCATOR)
    assert encoded == json.dumps(spec, separators=(",", ":"), sort_keys=True) + "\n"
    assert (
        render_deployment_spec_json(deepcopy(RESOLVED), "production", PRODUCTION_LOCATOR) == encoded
    )


def test_renderer_matches_the_shared_pretty_golden_as_an_object() -> None:
    expected = json.loads(DEPLOYMENT_FIXTURE.read_text())
    locator = expected["artifact"]["locator"]
    encoded = render_deployment_spec_json(RESOLVED, "production", locator)
    assert json.loads(encoded) == expected


@pytest.mark.parametrize("kind", ["python_wheel", "node_package", "native_binary"])
def test_non_oci_artifact_kinds_accept_only_credential_free_https(kind: str) -> None:
    resolved = deepcopy(RESOLVED)
    target = next(item for item in resolved["targets"] if item["id"] == "local")
    target["artifact"]["kind"] = kind
    locator = f"https://artifacts.example/graphforge/{kind}/0.5.0"
    spec = render_deployment_spec(resolved, "local", locator)
    assert spec["artifact"] == {
        "kind": kind,
        "locator": locator,
        "sha256": "b" * 64,
        "version": "0.5.0",
    }


@pytest.mark.parametrize(
    "locator",
    [
        f"localhost:5000/graphforge/runtime@sha256:{PRODUCTION_SHA}",
        f"127.0.0.1:5000/graphforge/runtime@sha256:{PRODUCTION_SHA}",
    ],
)
def test_oci_locator_allows_caller_owned_local_registries(locator: str) -> None:
    spec = render_deployment_spec(RESOLVED, "production", locator)
    assert spec["artifact"]["locator"] == locator


def test_renderer_preserves_every_configured_role_and_topology() -> None:
    locators = {
        "external-host": "https://artifacts.example/graphforge-host-0.5.1",
        "external-job": f"registry.example/graphforge/job@sha256:{'f' * 64}",
        "external-worker": f"registry.example/graphforge/worker@sha256:{'e' * 64}",
        "local": "https://artifacts.example/graphforge-0.5.0.whl",
        "local-service": "https://artifacts.example/graphforge-0.5.0",
        "production": PRODUCTION_LOCATOR,
    }
    for target_id, locator in locators.items():
        configured = next(item for item in RESOLVED["targets"] if item["id"] == target_id)
        topology = configured["topology"]
        spec = render_deployment_spec(RESOLVED, target_id, locator)
        assert spec["topology"] == {
            "execution": topology["execution"],
            "kind": configured["kind"],
            "ownership": configured["ownership"],
            "replicas": topology["replicas"],
            "scheduling": topology["scheduling"],
        }


@pytest.mark.parametrize(
    ("locator", "message"),
    [
        ("registry.example/graphforge/runtime:latest", "pinned by sha256 digest"),
        (
            f"registry.example/graphforge/runtime:0.5@sha256:{PRODUCTION_SHA}",
            "mutable tag",
        ),
        (f"registry.example/graphforge/runtime@sha256:{'d' * 64}", "does not match"),
        (
            f"registry.example/runtime?x=1@sha256:{PRODUCTION_SHA}",
            "without a mutable tag",
        ),
        ("/tmp/runtime", "local path"),
        ("file:///tmp/runtime", "local path"),
        ("registry.example/runtime digest", "whitespace or control"),
        ("x" * 2049, "bounded string"),
    ],
)
def test_oci_locator_rejections_are_bounded(locator: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        render_deployment_spec(RESOLVED, "production", locator)


@pytest.mark.parametrize(
    "locator",
    [
        "http://artifacts.example/runtime.whl",
        "https://user:token@artifacts.example/runtime.whl",
        "https://artifacts.example/runtime.whl?token=secret",
        "https://artifacts.example/runtime.whl#sha256",
        "../runtime.whl",
    ],
)
def test_non_oci_locator_rejects_mutable_or_sensitive_forms(locator: str) -> None:
    with pytest.raises(ValueError):
        render_deployment_spec(RESOLVED, "local", locator)


def test_projection_drift_is_bounded_to_selected_inputs() -> None:
    first = render_deployment_spec_json(RESOLVED, "production", PRODUCTION_LOCATOR)
    changed = deepcopy(RESOLVED)
    production = next(item for item in changed["targets"] if item["id"] == "production")
    production["topology"]["replicas"] = 3
    second = render_deployment_spec_json(changed, "production", PRODUCTION_LOCATOR)
    assert second != first
    assert json.loads(second)["topology"]["replicas"] == 3
    assert json.loads(second)["infrastructure"] == {
        "mutation": "none",
        "status": "caller_owned",
    }

    artifact = production["artifact"]
    artifact["version"] = "0.5.2"
    artifact["sha256"] = "d" * 64
    changed_locator = f"registry.example/graphforge/runtime@sha256:{'d' * 64}"
    third = render_deployment_spec_json(changed, "production", changed_locator)
    assert third != second
    assert json.loads(third)["artifact"]["version"] == "0.5.2"


def test_invalid_config_and_artifact_fail_through_the_shared_closed_contract() -> None:
    unknown = deepcopy(RESOLVED)
    unknown["provider"] = "kubernetes"
    with pytest.raises(ValueError, match="unknown field provider"):
        render_deployment_spec(unknown, "production", PRODUCTION_LOCATOR)

    invalid_artifact = deepcopy(RESOLVED)
    production = next(item for item in invalid_artifact["targets"] if item["id"] == "production")
    production["artifact"]["kind"] = "server_build"
    with pytest.raises(ValueError, match=r"target\.artifact\.kind has an unsupported value"):
        render_deployment_spec(invalid_artifact, "production", PRODUCTION_LOCATOR)

    inline_secret = deepcopy(RESOLVED)
    inline_secret["secrets"][0]["value"] = "SECRET_VALUE_SENTINEL"
    with pytest.raises(ValueError, match=r"secrets\[0\] contains unknown field value"):
        render_deployment_spec(inline_secret, "production", PRODUCTION_LOCATOR)


def test_secret_and_source_values_never_enter_projection() -> None:
    resolved = deepcopy(RESOLVED)
    sentinel = "GRAPHFORGE_SECRET_SENTINEL"
    resolved["sources"][0]["uri"] = f"https://example.invalid/{sentinel}.parquet"
    encoded = render_deployment_spec_json(resolved, "production", PRODUCTION_LOCATOR)
    assert sentinel not in encoded
    assert ".graphforge/state" not in encoded
    assert '"secret_ids":["service-token"]' in encoded
    assert '"source_ids":["example-data"]' in encoded


class RecordingMocks(Mocks):
    def __init__(self) -> None:
        self.resources: list[tuple[str, dict[str, Any]]] = []

    def new_resource(self, args: MockResourceArgs) -> tuple[str, dict[str, Any]]:
        self.resources.append((args.typ, args.inputs))
        return f"{args.name}-id", args.inputs

    def call(self, args: MockCallArgs) -> dict[str, Any]:
        raise AssertionError(f"unexpected provider call {args.token}")


class ComponentTests(unittest.TestCase):
    @pulumi.runtime.test
    def test_component_registers_only_state_projection(self) -> pulumi.Output[Any]:
        mocks = RecordingMocks()
        pulumi.runtime.set_mocks(mocks, project="graphforge", stack="deployment-spec")
        component = DeploymentSpec(
            "production",
            resolved_config=RESOLVED,
            target_id="production",
            artifact_locator=PRODUCTION_LOCATOR,
        )

        def check(spec: dict[str, Any], canonical_json: str) -> None:
            self.assertEqual(spec["infrastructure"]["status"], "caller_owned")
            self.assertEqual(spec["readiness"]["status"], "not_checked")
            self.assertEqual(json.loads(canonical_json), spec)
            self.assertEqual(
                mocks.resources,
                [("graphforge:deployment:DeploymentSpec", {})],
            )
            serialized = json.dumps(mocks.resources)
            self.assertNotIn(".graphforge/state", serialized)
            self.assertNotIn("service-token", serialized)

        return pulumi.Output.all(
            spec=component.spec,
            canonical_json=component.canonical_json,
            urn=component.urn,
        ).apply(lambda values: check(values["spec"], values["canonical_json"]))
