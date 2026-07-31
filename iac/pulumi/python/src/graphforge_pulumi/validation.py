"""Frozen `graphforge-infra-validation/1` projection and Pulumi component."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any

import pulumi

JsonObject = dict[str, Any]

_STABLE_ID = re.compile(r"^[a-z](?:[a-z0-9]|[-_](?=[a-z0-9])){0,63}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def _fail(message: str) -> None:
    raise ValueError(f"invalid graphforge-resolved-config/1: {message}")


def _object(value: Any, label: str) -> JsonObject:
    if not isinstance(value, Mapping):
        _fail(f"{label} must be an object")
    return dict(value)


def _array(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        _fail(f"{label} must be an array")
    return value


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str):
        _fail(f"{label} must be a string")
    return value


def _integer(value: Any, label: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1 or value > maximum:
        _fail(f"{label} must be an integer from 1 through {maximum}")
    return value


def _integer_from(value: Any, label: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum or value > maximum:
        _fail(f"{label} must be an integer from {minimum} through {maximum}")
    return value


def _boolean(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        _fail(f"{label} must be a boolean")
    return value


def _exact_keys(
    value: JsonObject,
    required: set[str],
    optional: set[str],
    label: str,
) -> None:
    missing = required - value.keys()
    if missing:
        _fail(f"{label} is missing {sorted(missing)[0]}")
    unknown = value.keys() - required - optional
    if unknown:
        _fail(f"{label} contains unknown field {sorted(unknown)[0]}")


def _one_of(value: Any, choices: set[str], label: str) -> str:
    candidate = _text(value, label)
    if candidate not in choices:
        _fail(f"{label} has an unsupported value")
    return candidate


def _stable_id(value: Any, label: str) -> str:
    candidate = _text(value, label)
    if _STABLE_ID.fullmatch(candidate) is None:
        _fail(f"{label} is not a stable ID")
    return candidate


def _artifact(value: Any) -> JsonObject:
    artifact = _object(value, "target.artifact")
    _exact_keys(artifact, {"kind", "version", "sha256"}, set(), "target.artifact")
    _one_of(
        artifact["kind"],
        {"python_wheel", "node_package", "native_binary", "oci_image"},
        "target.artifact.kind",
    )
    version = _text(artifact["version"], "target.artifact.version")
    if not 1 <= len(version) <= 128:
        _fail("target.artifact.version exceeds contract bounds")
    if _SHA256.fullmatch(_text(artifact["sha256"], "target.artifact.sha256")) is None:
        _fail("target.artifact.sha256 must be lowercase SHA-256")
    return artifact


def _capabilities(value: Any) -> list[JsonObject]:
    capabilities = _array(value, "target.capabilities")
    if len(capabilities) > 64:
        _fail("target.capabilities exceeds contract bounds")
    result: list[JsonObject] = []
    seen: set[str] = set()
    for index, item in enumerate(capabilities):
        label = f"target.capabilities[{index}]"
        requirement = _object(item, label)
        _exact_keys(requirement, {"id", "version"}, set(), label)
        capability_id = _stable_id(requirement["id"], f"{label}.id")
        if capability_id in seen:
            _fail("target capability IDs must be unique")
        seen.add(capability_id)
        _integer(requirement["version"], f"{label}.version", 65535)
        result.append(requirement)
    return result


def _resolved_target(value: Any, target_id: str) -> JsonObject:
    target = _object(value, f"target {target_id}")
    required = {
        "id",
        "kind",
        "ownership",
        "artifact",
        "topology",
        "capabilities",
        "write",
        "storage",
        "resources",
        "network",
        "health",
        "observability",
        "backup",
        "source_ids",
        "secret_ids",
    }
    _exact_keys(target, required, set(), f"target {target_id}")
    if _stable_id(target["id"], "target.id") != target_id:
        _fail("selected target ID does not match target.id")
    kind = _one_of(
        target["kind"],
        {"embedded", "service", "worker", "job", "host"},
        "target.kind",
    )
    ownership = _one_of(
        target["ownership"],
        {"embedded", "local", "external"},
        "target.ownership",
    )
    _artifact(target["artifact"])
    _capabilities(target["capabilities"])
    topology = _object(target["topology"], "target.topology")
    _exact_keys(topology, {"execution", "scheduling", "replicas"}, set(), "target.topology")
    execution = _one_of(
        topology["execution"], {"process", "container", "host"}, "target.topology.execution"
    )
    scheduling = _one_of(
        topology["scheduling"],
        {"long_running", "on_demand"},
        "target.topology.scheduling",
    )
    replicas = _integer(topology["replicas"], "target.topology.replicas", 1024)
    write = _object(target["write"], "target.write")
    _exact_keys(write, {"mode"}, {"queue_capacity", "max_rebase_attempts"}, "target.write")
    write_mode = _one_of(
        write["mode"],
        {"single_writer", "queued_writer", "optimistic_multi_writer"},
        "target.write.mode",
    )
    if "queue_capacity" in write:
        _integer(write["queue_capacity"], "target.write.queue_capacity", 65536)
    if "max_rebase_attempts" in write:
        _integer_from(write["max_rebase_attempts"], "target.write.max_rebase_attempts", 0, 64)
    if (
        write_mode == "single_writer"
        and ("queue_capacity" in write or "max_rebase_attempts" in write)
    ) or (write_mode == "queued_writer" and "queue_capacity" not in write):
        _fail("target.write settings do not match its mode")
    if write_mode == "optimistic_multi_writer" and "max_rebase_attempts" not in write:
        _fail("target.write settings do not match its mode")

    storage = _object(target["storage"], "target.storage")
    _exact_keys(
        storage,
        {"kind"},
        {"persistent", "class", "capacity_bytes"},
        "target.storage",
    )
    storage_kind = _one_of(storage["kind"], {"local", "volume", "object"}, "target.storage.kind")
    if "persistent" in storage:
        _boolean(storage["persistent"], "target.storage.persistent")
    if "class" in storage:
        storage_class = _text(storage["class"], "target.storage.class")
        if not 1 <= len(storage_class) <= 128:
            _fail("target.storage.class exceeds contract bounds")
    if "capacity_bytes" in storage:
        _integer(storage["capacity_bytes"], "target.storage.capacity_bytes", (1 << 53) - 1)

    resources = _object(target["resources"], "target.resources")
    _exact_keys(resources, set(), {"cpu_millis", "memory_bytes"}, "target.resources")
    for key in ("cpu_millis", "memory_bytes"):
        if key in resources:
            _integer(resources[key], f"target.resources.{key}", (1 << 53) - 1)

    network = _object(target["network"], "target.network")
    _exact_keys(network, set(), {"exposure", "port", "tls_required"}, "target.network")
    exposure = (
        _one_of(network["exposure"], {"none", "private", "public"}, "target.network.exposure")
        if "exposure" in network
        else None
    )
    if "port" in network:
        _integer(network["port"], "target.network.port", 65535)
    if "tls_required" in network:
        _boolean(network["tls_required"], "target.network.tls_required")

    health = _object(target["health"], "target.health")
    _exact_keys(health, {"timeout_seconds"}, set(), "target.health")
    _integer(health["timeout_seconds"], "target.health.timeout_seconds", 300)

    observability = _object(target["observability"], "target.observability")
    _exact_keys(observability, set(), {"logs", "metrics", "traces"}, "target.observability")
    for key in ("logs", "metrics", "traces"):
        if key in observability:
            _boolean(observability[key], f"target.observability.{key}")

    backup = _object(target["backup"], "target.backup")
    _exact_keys(backup, set(), {"checkpoints", "retention_count"}, "target.backup")
    if "checkpoints" in backup:
        _boolean(backup["checkpoints"], "target.backup.checkpoints")
    if "retention_count" in backup:
        _integer(backup["retention_count"], "target.backup.retention_count", 1024)
        if backup.get("checkpoints") is not True:
            _fail("backup retention requires checkpoint backups")
    for key in ("source_ids", "secret_ids"):
        values = _array(target[key], f"target.{key}")
        maximum = 256 if key == "source_ids" else 128
        if len(values) > maximum:
            _fail(f"target.{key} exceeds contract bounds")
        ids = [_stable_id(item, f"target.{key}[{index}]") for index, item in enumerate(values)]
        if len(set(ids)) != len(ids):
            _fail(f"target.{key} must contain unique IDs")
    if (kind == "embedded") != (ownership == "embedded"):
        _fail("embedded ownership is valid only for an embedded target")
    if kind == "embedded" and (
        execution != "process"
        or scheduling != "long_running"
        or replicas != 1
        or storage_kind != "local"
        or exposure not in (None, "none")
    ):
        _fail("embedded target requirements are invalid")
    if (kind == "host") != (execution == "host"):
        _fail("host target and host execution must be used together")
    if (kind == "job") != (scheduling == "on_demand"):
        _fail("job targets are on-demand and other targets are long-running")
    if kind == "service" and "port" not in network:
        _fail("service targets require a network port")
    if exposure == "public" and network.get("tls_required") is not True:
        _fail("public targets require TLS")
    return target


def _validate_resolved_config(config: JsonObject) -> list[JsonObject]:
    _exact_keys(
        config,
        {"contract", "project", "sources", "secrets", "targets"},
        set(),
        "resolved config",
    )
    if config["contract"] != "graphforge-resolved-config/1":
        _fail("unsupported contract")
    project = _object(config["project"], "project")
    project_keys = {
        "integration_root",
        "state",
        "imports",
        "exports",
        "ontology",
        "schemas",
        "seeds",
        "migrations",
    }
    _exact_keys(project, project_keys, set(), "project")
    fixed_paths = {
        "integration_root": ".graphforge",
        "state": ".graphforge/state",
        "imports": ".graphforge/imports",
        "exports": ".graphforge/exports",
    }
    for key in project_keys:
        path = _text(project[key], f"project.{key}")
        if not 1 <= len(path) <= 1024:
            _fail(f"project.{key} exceeds contract bounds")
        if (
            path.startswith("/")
            or "\\" in path
            or ".." in path.split("/")
            or any(ord(character) < 32 or ord(character) == 127 for character in path)
        ):
            _fail(f"project.{key} is not a safe relative path")
        if key in fixed_paths and path != fixed_paths[key]:
            _fail(f"project.{key} is not canonical")

    source_ids: set[str] = set()
    sources = _array(config["sources"], "sources")
    if len(sources) > 256:
        _fail("sources exceeds contract bounds")
    for index, item in enumerate(sources):
        source = _object(item, f"sources[{index}]")
        _exact_keys(source, {"id", "uri", "sha256"}, {"media_type"}, f"sources[{index}]")
        source_id = _stable_id(source["id"], f"sources[{index}].id")
        if source_id in source_ids:
            _fail("source IDs must be unique")
        source_ids.add(source_id)
        uri = _text(source["uri"], f"sources[{index}].uri")
        host = uri.split("://", maxsplit=1)[-1].split("/", maxsplit=1)[0]
        if not 1 <= len(uri) <= 2048 or ("://" in uri and "@" in host):
            _fail(f"sources[{index}].uri is invalid")
        if _SHA256.fullmatch(_text(source["sha256"], f"sources[{index}].sha256")) is None:
            _fail(f"sources[{index}].sha256 must be lowercase SHA-256")
        if "media_type" in source:
            media_type = _text(source["media_type"], f"sources[{index}].media_type")
            if not 1 <= len(media_type) <= 128:
                _fail(f"sources[{index}].media_type exceeds contract bounds")

    secret_ids: set[str] = set()
    secrets = _array(config["secrets"], "secrets")
    if len(secrets) > 128:
        _fail("secrets exceeds contract bounds")
    for index, item in enumerate(secrets):
        secret = _object(item, f"secrets[{index}]")
        _exact_keys(secret, {"id", "source"}, set(), f"secrets[{index}]")
        secret_id = _stable_id(secret["id"], f"secrets[{index}].id")
        if secret_id in secret_ids:
            _fail("secret IDs must be unique")
        secret_ids.add(secret_id)
        _one_of(
            secret["source"],
            {"environment", "pulumi", "terraform", "secret_manager"},
            f"secrets[{index}].source",
        )

    raw_targets = _array(config["targets"], "targets")
    if not 1 <= len(raw_targets) <= 64:
        _fail("targets exceeds contract bounds")
    target_ids: set[str] = set()
    targets: list[JsonObject] = []
    for index, item in enumerate(raw_targets):
        candidate = _object(item, f"targets[{index}]")
        target_id = _stable_id(candidate.get("id"), f"targets[{index}].id")
        if target_id in target_ids:
            _fail("target IDs must be unique")
        target_ids.add(target_id)
        target = _resolved_target(candidate, target_id)
        if any(source_id not in source_ids for source_id in target["source_ids"]):
            _fail("target references an unknown source")
        if any(secret_id not in secret_ids for secret_id in target["secret_ids"]):
            _fail("target references an unknown secret")
        targets.append(target)
    return targets


def _canonical_json(value: JsonObject) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def validate_target(resolved_config: Mapping[str, Any], target_id: str) -> JsonObject:
    """Deterministically reproduce the Rust-owned static validation receipt."""
    if _STABLE_ID.fullmatch(target_id) is None:
        _fail("target_id is not a stable ID")
    config = _object(resolved_config, "resolved config")
    targets = _validate_resolved_config(config)
    matches = [item for item in targets if item.get("id") == target_id]
    if len(matches) != 1:
        _fail(f"expected exactly one target named {target_id}")
    target = _resolved_target(matches[0], target_id)
    topology = _object(target["topology"], "target.topology")
    capabilities = _capabilities(target["capabilities"])

    return {
        "contract": "graphforge-infra-validation/1",
        "resolved_config_sha256": hashlib.sha256(_canonical_json(config)).hexdigest(),
        "target": target,
        "static_validity": {"status": "valid"},
        "planned_infrastructure": {
            "status": "validated",
            "mutation": "none",
            "ownership": target["ownership"],
            "kind": target["kind"],
            "execution": topology["execution"],
            "scheduling": topology["scheduling"],
            "replicas": topology["replicas"],
            "artifact": _object(target["artifact"], "target.artifact"),
        },
        "connectivity": {"status": "not_checked"},
        "readiness": {"status": "not_checked"},
        "capability_compatibility": {
            "status": "requirements_declared",
            "requirements": capabilities,
        },
    }


class TargetValidation(pulumi.ComponentResource):
    """State-safe, provider-free static validation component."""

    receipt: pulumi.Output[JsonObject]

    def __init__(
        self,
        resource_name: str,
        *,
        resolved_config: Mapping[str, Any],
        target_id: str,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        receipt = validate_target(resolved_config, target_id)
        # Deliberately do not register resolved config as a component input.
        super().__init__("graphforge:static:TargetValidation", resource_name, {}, opts)
        self.receipt = pulumi.Output.from_input(receipt)
        self.register_outputs({"receipt": self.receipt})
