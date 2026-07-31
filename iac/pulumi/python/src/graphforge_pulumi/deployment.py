"""Provider-neutral ``graphforge-deployment-spec/1`` projection."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

import pulumi

from .validation import JsonObject, validate_target

_CONTROL_OR_SPACE = re.compile(r"[\x00-\x20\x7f]")
_WINDOWS_PATH = re.compile(r"^[A-Za-z]:[\\/]")
_OCI_DIGEST = re.compile(r"^(?P<repository>[^@]+)@sha256:(?P<digest>[0-9a-f]{64})$")
_OCI_REPOSITORY = re.compile(
    r"^[a-z0-9]+(?:[.-][a-z0-9]+)*(?::[1-9][0-9]{0,4})?/"
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*$"
)


def _fail(message: str) -> None:
    raise ValueError(f"invalid graphforge-deployment-spec/1: {message}")


def _artifact_locator(value: Any, kind: str, expected_sha256: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 2048:
        _fail("artifact_locator must be a bounded string")
    if _CONTROL_OR_SPACE.search(value):
        _fail("artifact_locator contains whitespace or control characters")
    lowered = value.lower()
    if (
        value.startswith(("/", "./", "../", "~"))
        or _WINDOWS_PATH.match(value)
        or "\\" in value
        or lowered.startswith("file:")
    ):
        _fail("artifact_locator must not be a local path")

    if kind == "oci_image":
        match = _OCI_DIGEST.fullmatch(value)
        if match is None:
            _fail("OCI artifact_locator must be pinned by sha256 digest")
        repository = match.group("repository")
        if _OCI_REPOSITORY.fullmatch(repository) is None:
            _fail("OCI artifact_locator must be registry/repository without a mutable tag")
        if match.group("digest") != expected_sha256:
            _fail("OCI artifact_locator digest does not match target.artifact.sha256")
    else:
        try:
            parsed = urlsplit(value)
            # Force bounded validation of malformed or out-of-range ports.
            _ = parsed.port
        except ValueError:
            _fail("non-OCI artifact_locator must be an https URL")
        if parsed.scheme != "https" or parsed.hostname is None:
            _fail("non-OCI artifact_locator must be an https URL")
        if parsed.username is not None or parsed.password is not None:
            _fail("artifact_locator must not contain inline credentials")
        if parsed.query or parsed.fragment:
            _fail("artifact_locator must not contain a query or fragment")
    return value


def render_deployment_spec(
    resolved_config: Mapping[str, Any],
    target_id: str,
    artifact_locator: str,
) -> JsonObject:
    """Render one closed deployment projection without reading project data."""
    validation = validate_target(resolved_config, target_id)
    target = dict(validation["target"])
    artifact = dict(target["artifact"])
    locator = _artifact_locator(
        artifact_locator,
        str(artifact["kind"]),
        str(artifact["sha256"]),
    )
    return {
        "contract": "graphforge-deployment-spec/1",
        "resolved_config_sha256": validation["resolved_config_sha256"],
        "target_id": target_id,
        "artifact": {
            "kind": artifact["kind"],
            "version": artifact["version"],
            "sha256": artifact["sha256"],
            "locator": locator,
        },
        "topology": {
            "execution": target["topology"]["execution"],
            "kind": target["kind"],
            "ownership": target["ownership"],
            "replicas": target["topology"]["replicas"],
            "scheduling": target["topology"]["scheduling"],
        },
        "requirements": {
            "backup": target["backup"],
            "health": target["health"],
            "network": target["network"],
            "observability": target["observability"],
            "resources": target["resources"],
            "storage": target["storage"],
            "write": target["write"],
        },
        "bindings": {
            "secret_ids": target["secret_ids"],
            "source_ids": target["source_ids"],
        },
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
            "status": "requirements_declared",
            "requirements": target["capabilities"],
        },
    }


def render_deployment_spec_json(
    resolved_config: Mapping[str, Any],
    target_id: str,
    artifact_locator: str,
) -> str:
    """Return canonical UTF-8 JSON text with a single trailing newline."""
    spec = render_deployment_spec(resolved_config, target_id, artifact_locator)
    return (
        json.dumps(
            spec,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    )


class DeploymentSpec(pulumi.ComponentResource):
    """State-only deployment projection that materializes no provider resources."""

    spec: pulumi.Output[JsonObject]
    canonical_json: pulumi.Output[str]

    def __init__(
        self,
        resource_name: str,
        *,
        resolved_config: Mapping[str, Any],
        target_id: str,
        artifact_locator: str,
        opts: pulumi.ResourceOptions | None = None,
    ) -> None:
        spec = render_deployment_spec(resolved_config, target_id, artifact_locator)
        canonical_json = (
            json.dumps(
                spec,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        )
        # The complete resolved config is deliberately omitted from component
        # inputs and state. The closed projection contains bounded IDs only.
        super().__init__("graphforge:deployment:DeploymentSpec", resource_name, {}, opts)
        self.spec = pulumi.Output.from_input(spec)
        self.canonical_json = pulumi.Output.from_input(canonical_json)
        self.register_outputs({"spec": self.spec, "canonical_json": self.canonical_json})
