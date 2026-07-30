"""Static acceptance for the repository configuration contracts."""

from __future__ import annotations

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
            assert "additionalProperties" in value, f"open object schema at {path}"
        for key, child in value.items():
            _objects_are_closed(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _objects_are_closed(child, f"{path}[{index}]")


def test_contract_schemas_are_versioned_and_closed() -> None:
    config = json.loads((CONTRACTS / "graphforge-project-config-v1.schema.json").read_text())
    resolved = json.loads((CONTRACTS / "graphforge-resolved-config-v1.schema.json").read_text())
    assert config["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert resolved["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert config["properties"]["schema_version"] == {"const": 1}
    assert resolved["properties"]["contract"] == {"const": "graphforge-resolved-config/1"}
    Draft202012Validator.check_schema(config)
    Draft202012Validator.check_schema(resolved)
    _objects_are_closed(config)
    _objects_are_closed(resolved)


def test_examples_preserve_data_and_secret_boundaries() -> None:
    config = yaml.safe_load((CONTRACTS / "examples" / "graphforge-v1.yaml").read_text())
    resolved = json.loads((CONTRACTS / "examples" / "graphforge-resolved-v1.json").read_text())
    config_schema = json.loads((CONTRACTS / "graphforge-project-config-v1.schema.json").read_text())
    resolved_schema = json.loads(
        (CONTRACTS / "graphforge-resolved-config-v1.schema.json").read_text()
    )
    registry = Registry().with_resources(
        [
            (config_schema["$id"], Resource.from_contents(config_schema)),
            (resolved_schema["$id"], Resource.from_contents(resolved_schema)),
        ]
    )
    Draft202012Validator(config_schema, registry=registry).validate(config)
    Draft202012Validator(resolved_schema, registry=registry).validate(resolved)
    assert config["schema_version"] == 1
    assert set(config["project"].values()) == {
        ".graphforge/ontology",
        ".graphforge/schemas",
        ".graphforge/seeds",
        ".graphforge/migrations",
    }
    assert [resolved["project"][key] for key in ("state", "imports", "exports")] == [
        ".graphforge/state",
        ".graphforge/imports",
        ".graphforge/exports",
    ]
    assert [target["id"] for target in resolved["targets"]] == ["local", "production"]
    assert resolved["secrets"] == [{"id": "service-token", "source": "secret_manager"}]
    serialized = json.dumps(resolved).lower().replace("service-token", "")
    assert "secret_value" not in serialized and "credential" not in serialized


def test_resolved_example_has_canonicalizable_ordered_content() -> None:
    resolved = json.loads((CONTRACTS / "examples" / "graphforge-resolved-v1.json").read_text())
    canonical = (
        json.dumps(resolved, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    )
    assert canonical.endswith("\n") and json.loads(canonical) == resolved
    assert [item["id"] for item in resolved["sources"]] == sorted(
        item["id"] for item in resolved["sources"]
    )
    assert [item["id"] for item in resolved["targets"]] == sorted(
        item["id"] for item in resolved["targets"]
    )
