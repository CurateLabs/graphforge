"""Static acceptance for the repository configuration contracts."""

from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

from jsonschema import Draft202012Validator
from referencing import Registry, Resource
import yaml

ROOT = Path(__file__).parents[2]
CONTRACTS = ROOT / "docs" / "contracts"


def _objects_are_closed(value: object, path: str = "$") -> None:
    if isinstance(value, dict):
        if value.get("type") == "object":
            additional = value.get("additionalProperties")
            assert additional is False or (
                isinstance(additional, dict) and set(additional) == {"$ref"}
            ), f"open object schema at {path}"
        for key, child in value.items():
            _objects_are_closed(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _objects_are_closed(child, f"{path}[{index}]")


def test_contract_schemas_are_versioned_and_closed() -> None:
    config = json.loads((CONTRACTS / "graphforge-project-config-v1.schema.json").read_text())
    resolved = json.loads((CONTRACTS / "graphforge-resolved-config-v1.schema.json").read_text())
    infra = json.loads((CONTRACTS / "graphforge-infra-validation-v1.schema.json").read_text())
    deployment = json.loads((CONTRACTS / "graphforge-deployment-spec-v1.schema.json").read_text())
    assert config["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert resolved["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert infra["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert deployment["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert config["properties"]["schema_version"] == {"const": 1}
    assert resolved["properties"]["contract"] == {"const": "graphforge-resolved-config/1"}
    assert infra["properties"]["contract"] == {"const": "graphforge-infra-validation/1"}
    assert deployment["properties"]["contract"] == {"const": "graphforge-deployment-spec/1"}
    Draft202012Validator.check_schema(config)
    Draft202012Validator.check_schema(resolved)
    Draft202012Validator.check_schema(infra)
    Draft202012Validator.check_schema(deployment)
    _objects_are_closed(config)
    _objects_are_closed(resolved)
    _objects_are_closed(infra)
    _objects_are_closed(deployment)


def test_examples_preserve_data_and_secret_boundaries() -> None:
    config = yaml.safe_load((CONTRACTS / "examples" / "graphforge-v1.yaml").read_text())
    resolved = json.loads((CONTRACTS / "examples" / "graphforge-resolved-v1.json").read_text())
    config_schema = json.loads((CONTRACTS / "graphforge-project-config-v1.schema.json").read_text())
    resolved_schema = json.loads(
        (CONTRACTS / "graphforge-resolved-config-v1.schema.json").read_text()
    )
    infra_schema = json.loads(
        (CONTRACTS / "graphforge-infra-validation-v1.schema.json").read_text()
    )
    infra = json.loads(
        (CONTRACTS / "examples" / "graphforge-infra-validation-production-v1.json").read_text()
    )
    deployment_schema = json.loads(
        (CONTRACTS / "graphforge-deployment-spec-v1.schema.json").read_text()
    )
    deployment = json.loads(
        (CONTRACTS / "examples" / "graphforge-deployment-spec-production-v1.json").read_text()
    )
    registry = Registry().with_resources(
        [
            (config_schema["$id"], Resource.from_contents(config_schema)),
            (resolved_schema["$id"], Resource.from_contents(resolved_schema)),
            (infra_schema["$id"], Resource.from_contents(infra_schema)),
            (deployment_schema["$id"], Resource.from_contents(deployment_schema)),
        ]
    )
    Draft202012Validator(config_schema, registry=registry).validate(config)
    Draft202012Validator(resolved_schema, registry=registry).validate(resolved)
    Draft202012Validator(infra_schema, registry=registry).validate(infra)
    Draft202012Validator(deployment_schema, registry=registry).validate(deployment)
    invalid_uri = deepcopy(config)
    invalid_uri["sources"][0]["uri"] = "https://user@example.invalid/data.parquet"
    assert not Draft202012Validator(config_schema, registry=registry).is_valid(invalid_uri)
    invalid_integer = deepcopy(resolved)
    invalid_integer["targets"][0]["storage"]["capacity_bytes"] = 9_007_199_254_740_992
    assert not Draft202012Validator(resolved_schema, registry=registry).is_valid(invalid_integer)
    assert config["schema_version"] == 1
    assert config["project"] == {
        "ontology": ".graphforge/ontology",
        "schemas": ".graphforge/schemas",
        "seeds": ".graphforge/seeds",
        "migrations": ".graphforge/migrations",
    }
    assert [resolved["project"][key] for key in ("state", "imports", "exports")] == [
        ".graphforge/state",
        ".graphforge/imports",
        ".graphforge/exports",
    ]
    assert [target["id"] for target in resolved["targets"]] == [
        "external-host",
        "external-job",
        "external-worker",
        "local",
        "local-service",
        "production",
    ]
    assert {(target["kind"], target["ownership"]) for target in resolved["targets"]} >= {
        ("embedded", "embedded"),
        ("service", "local"),
        ("service", "external"),
        ("worker", "external"),
        ("job", "external"),
        ("host", "external"),
    }
    assert resolved["secrets"] == [{"id": "service-token", "source": "secret_manager"}]
    serialized = json.dumps(resolved).lower().replace("service-token", "")
    assert "secret_value" not in serialized and "credential" not in serialized
    infra_serialized = json.dumps(infra)
    sentinel = "_".join(("GRAPHFORGE_SECRET", "SENTINEL_231"))
    assert sentinel not in infra_serialized
    assert infra["static_validity"] == {"status": "valid"}
    assert infra["planned_infrastructure"]["mutation"] == "none"
    assert infra["connectivity"] == {"status": "not_checked"}
    assert infra["readiness"] == {"status": "not_checked"}
    deployment_serialized = json.dumps(deployment)
    assert sentinel not in deployment_serialized
    assert deployment["infrastructure"] == {"mutation": "none", "status": "caller_owned"}
    assert deployment["connectivity"] == {"status": "not_checked"}
    assert deployment["readiness"] == {"status": "not_checked"}
    assert deployment["artifact"]["sha256"] in deployment["artifact"]["locator"]
    assert deployment["capability_compatibility"]["status"] == "requirements_declared"
    invalid_deployment = deepcopy(deployment)
    invalid_deployment["infrastructure"]["status"] = "graphforge_owned"
    assert not Draft202012Validator(deployment_schema, registry=registry).is_valid(
        invalid_deployment
    )
    assert infra["capability_compatibility"]["status"] == "requirements_declared"


def test_resolved_example_has_canonicalizable_ordered_content() -> None:
    source = (CONTRACTS / "examples" / "graphforge-resolved-v1.json").read_text()
    resolved = json.loads(source)
    canonical = (
        json.dumps(resolved, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    )
    assert source == canonical
    assert [item["id"] for item in resolved["sources"]] == sorted(
        item["id"] for item in resolved["sources"]
    )
    assert [item["id"] for item in resolved["targets"]] == sorted(
        item["id"] for item in resolved["targets"]
    )
