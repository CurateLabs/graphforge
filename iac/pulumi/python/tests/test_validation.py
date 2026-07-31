from __future__ import annotations

import hashlib
import json
import unittest
from copy import deepcopy
from pathlib import Path
from typing import Any

import pulumi
import pytest
from pulumi.runtime import MockCallArgs, MockResourceArgs, Mocks

from graphforge_pulumi import TargetValidation, validate_target

FIXTURE = (
    Path(__file__).parents[4] / "docs" / "contracts" / "examples" / "graphforge-resolved-v1.json"
)
RESOLVED = json.loads(FIXTURE.read_text())


def test_pure_validator_emits_frozen_receipt_deterministically() -> None:
    first = validate_target(RESOLVED, "production")
    second = validate_target(deepcopy(RESOLVED), "production")
    assert second == first
    canonical = json.dumps(
        RESOLVED, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode()
    assert first["resolved_config_sha256"] == hashlib.sha256(canonical).hexdigest()
    assert first["contract"] == "graphforge-infra-validation/1"
    assert first["static_validity"] == {"status": "valid"}
    assert first["connectivity"] == {"status": "not_checked"}
    assert first["readiness"] == {"status": "not_checked"}
    assert first["planned_infrastructure"]["mutation"] == "none"
    assert first["planned_infrastructure"]["kind"] == "service"
    assert first["planned_infrastructure"]["execution"] == "container"
    assert first["planned_infrastructure"]["replicas"] == 2
    assert first["capability_compatibility"]["requirements"] == [
        {"id": "graph", "version": 1},
        {"id": "workspace", "version": 1},
    ]


def test_pure_validator_covers_every_authority_and_target_topology() -> None:
    expected = [
        ("external-host", "external", "host", "host", "long_running", 2),
        ("external-job", "external", "job", "container", "on_demand", 1),
        ("external-worker", "external", "worker", "container", "long_running", 3),
        ("local", "embedded", "embedded", "process", "long_running", 1),
        ("local-service", "local", "service", "process", "long_running", 1),
        ("production", "external", "service", "container", "long_running", 2),
    ]
    for target_id, ownership, kind, execution, scheduling, replicas in expected:
        plan = validate_target(RESOLVED, target_id)["planned_infrastructure"]
        assert (
            plan["ownership"],
            plan["kind"],
            plan["execution"],
            plan["scheduling"],
            plan["replicas"],
        ) == (ownership, kind, execution, scheduling, replicas)


def test_validator_rejects_unknown_fields_that_could_carry_secret_values() -> None:
    poisoned = deepcopy(RESOLVED)
    poisoned["targets"][1]["credential"] = "_".join(("GRAPHFORGE_SECRET", "SENTINEL"))
    with pytest.raises(ValueError, match="unknown field credential"):
        validate_target(poisoned, "production")


def test_validator_rejects_inline_source_credentials() -> None:
    poisoned = deepcopy(RESOLVED)
    poisoned["sources"][0]["uri"] = "https://user:password@example.invalid/data.parquet"
    with pytest.raises(ValueError, match=r"sources\[0\]\.uri is invalid"):
        validate_target(poisoned, "production")


def test_validator_rejects_integers_above_the_portable_json_limit() -> None:
    poisoned = deepcopy(RESOLVED)
    production = next(target for target in poisoned["targets"] if target["id"] == "production")
    production["resources"]["memory_bytes"] = 9_007_199_254_740_992
    with pytest.raises(ValueError, match=r"target\.resources\.memory_bytes must be an integer"):
        validate_target(poisoned, "production")


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
    def test_component_registers_no_provider_resources_or_inputs(self) -> pulumi.Output[Any]:
        mocks = RecordingMocks()
        pulumi.runtime.set_mocks(mocks, project="graphforge", stack="static-validation")
        component = TargetValidation(
            "production",
            resolved_config=RESOLVED,
            target_id="production",
        )

        def check(receipt: dict[str, Any]) -> None:
            self.assertEqual(receipt["connectivity"], {"status": "not_checked"})
            self.assertEqual(receipt["readiness"], {"status": "not_checked"})
            self.assertEqual(
                mocks.resources,
                [("graphforge:static:TargetValidation", {})],
            )
            serialized = json.dumps(mocks.resources)
            self.assertNotIn("service-token", serialized)
            self.assertNotIn("SECRET_SENTINEL", serialized)

        return component.receipt.apply(check)
