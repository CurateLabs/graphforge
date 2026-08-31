"""Credential-free recovery lease and expired-lease janitor core.

Import-only: no CLI, no ESC opening, and no ambient provider credentials.
Cleanup reconstructs an owner-confined transport through an injected factory.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Protocol

from jsonschema import Draft202012Validator

from graphforge_bench.progressive_provider_attempt import (
    AttemptError,
    SpendAuthorization,
    _timestamp,
)

LEASE_SCHEMA = "graphforge-progressive-provider-recovery-lease/1"
SCHEMA_ROOT = Path(__file__).resolve().parents[2] / "schemas"
APP = re.compile(r"^gf-progressive-[0-9a-f]{32}$")


class _TeardownTransport(Protocol):
    def teardown(self, resources: Mapping[str, str]) -> Mapping[str, Any]: ...


class TransportFactory(Protocol):
    def __call__(self, lease: RecoveryLease) -> _TeardownTransport: ...


@dataclass(frozen=True)
class RecoveryLease:
    """Durable, credential-free receipt for host-independent expired cleanup."""

    schema: str
    version: int
    organization: str
    app: str
    attempt_nonce: str
    commit: str
    authorization_sha256: str
    image_digest: str
    teardown_owner: str
    execution_deadline: datetime
    cleanup_deadline: datetime
    resource_limits: Mapping[str, int]
    acknowledged_at: datetime
    claim: str


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _validate_schema(value: Mapping[str, Any], failure: str) -> None:
    try:
        schema = json.loads(
            (SCHEMA_ROOT / "progressive-provider-recovery-lease.json").read_text(encoding="utf-8")
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AttemptError(failure, "recovery lease schema is unavailable") from error
    error = next(Draft202012Validator(schema).iter_errors(value), None)
    if error is not None:
        raise AttemptError(failure, "recovery lease document failed its schema")


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _document(lease: RecoveryLease) -> dict[str, Any]:
    return {
        "schema": lease.schema,
        "version": lease.version,
        "organization": lease.organization,
        "app": lease.app,
        "attempt_nonce": lease.attempt_nonce,
        "commit": lease.commit,
        "authorization_sha256": lease.authorization_sha256,
        "image_digest": lease.image_digest,
        "teardown_owner": lease.teardown_owner,
        "execution_deadline": _format_timestamp(lease.execution_deadline),
        "cleanup_deadline": _format_timestamp(lease.cleanup_deadline),
        "resource_limits": dict(lease.resource_limits),
        "acknowledged_at": _format_timestamp(lease.acknowledged_at),
        "claim": lease.claim,
    }


def build_recovery_lease(
    authorization: SpendAuthorization,
    *,
    execution_deadline: datetime,
    acknowledged_at: datetime,
) -> RecoveryLease:
    """Construct a schema-shaped lease bound to spend authority identity."""
    if execution_deadline.tzinfo is None or acknowledged_at.tzinfo is None:
        raise AttemptError("recovery_refused", "recovery lease clock is not timezone-aware")
    if APP.fullmatch(authorization.app) is None:
        raise AttemptError("recovery_refused", "recovery lease app identity is malformed")
    if authorization.app != f"gf-progressive-{authorization.attempt_nonce}":
        raise AttemptError("recovery_refused", "recovery lease app/nonce binding is inconsistent")
    if execution_deadline > authorization.expires_at:
        raise AttemptError(
            "recovery_refused", "execution deadline exceeds spend authorization expiry"
        )
    return RecoveryLease(
        schema=LEASE_SCHEMA,
        version=1,
        organization=authorization.organization,
        app=authorization.app,
        attempt_nonce=authorization.attempt_nonce,
        commit=authorization.commit,
        authorization_sha256=authorization.authorization_sha256,
        image_digest=authorization.image_digest,
        teardown_owner=authorization.teardown_owner,
        execution_deadline=execution_deadline.astimezone(timezone.utc),
        cleanup_deadline=authorization.expires_at.astimezone(timezone.utc),
        resource_limits=dict(authorization.resource_limits),
        acknowledged_at=acknowledged_at.astimezone(timezone.utc),
        claim="recovery_lease_only",
    )


def parse_recovery_lease(value: str | bytes | Mapping[str, Any]) -> RecoveryLease:
    if isinstance(value, (str, bytes)):
        try:
            decoded = json.loads(value)
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise AttemptError("recovery_refused", "recovery lease is malformed") from error
    else:
        decoded = value
    if not isinstance(decoded, Mapping):
        raise AttemptError("recovery_refused", "recovery lease is malformed")
    _validate_schema(decoded, "recovery_refused")
    if decoded.get("schema") != LEASE_SCHEMA or decoded.get("version") != 1:
        raise AttemptError("recovery_refused", "recovery lease schema is invalid")
    if decoded["app"] != f"gf-progressive-{decoded['attempt_nonce']}":
        raise AttemptError("recovery_refused", "recovery lease app/nonce binding is inconsistent")
    execution_deadline = _timestamp(decoded["execution_deadline"])
    cleanup_deadline = _timestamp(decoded["cleanup_deadline"])
    acknowledged_at = _timestamp(decoded["acknowledged_at"])
    if cleanup_deadline < execution_deadline:
        raise AttemptError("recovery_refused", "cleanup deadline precedes execution deadline")
    return RecoveryLease(
        schema=LEASE_SCHEMA,
        version=1,
        organization=decoded["organization"],
        app=decoded["app"],
        attempt_nonce=decoded["attempt_nonce"],
        commit=decoded["commit"],
        authorization_sha256=decoded["authorization_sha256"],
        image_digest=decoded["image_digest"],
        teardown_owner=decoded["teardown_owner"],
        execution_deadline=execution_deadline,
        cleanup_deadline=cleanup_deadline,
        resource_limits=dict(decoded["resource_limits"]),
        acknowledged_at=acknowledged_at,
        claim=decoded["claim"],
    )


def save_recovery_lease(path: Path, lease: RecoveryLease) -> None:
    document = _document(lease)
    _validate_schema(document, "recovery_refused")
    _atomic_json(path, document)


def load_recovery_lease(path: Path) -> RecoveryLease:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AttemptError(
            "recovery_refused", "recovery lease is unavailable or malformed"
        ) from error
    return parse_recovery_lease(value)


def ack_recovery_lease(path: Path, lease: RecoveryLease) -> RecoveryLease:
    """Persist then re-read so acknowledgment is durable before mutation."""
    save_recovery_lease(path, lease)
    loaded = load_recovery_lease(path)
    if _document(loaded) != _document(lease):
        raise AttemptError("recovery_refused", "recovery lease acknowledgment drifted")
    return loaded


def validate_lease_identity(lease: RecoveryLease, *, expected: RecoveryLease | None = None) -> None:
    if APP.fullmatch(lease.app) is None:
        raise AttemptError("recovery_refused", "recovery lease app identity is malformed")
    if lease.app != f"gf-progressive-{lease.attempt_nonce}":
        raise AttemptError("recovery_refused", "recovery lease app/nonce binding is inconsistent")
    if expected is None:
        return
    if (
        lease.organization != expected.organization
        or lease.app != expected.app
        or lease.attempt_nonce != expected.attempt_nonce
        or lease.commit != expected.commit
        or lease.authorization_sha256 != expected.authorization_sha256
        or lease.image_digest != expected.image_digest
        or lease.teardown_owner != expected.teardown_owner
    ):
        raise AttemptError("recovery_refused", "recovery lease identity mismatch")


def cleanup_expired_lease(
    lease: RecoveryLease,
    *,
    transport_factory: TransportFactory,
    clock: Callable[[], datetime] | None = None,
    expected: RecoveryLease | None = None,
) -> Mapping[str, Any]:
    """Teardown after cleanup_deadline using an injected owner-confined transport."""
    clock_fn = clock or (lambda: datetime.now(timezone.utc))
    now = clock_fn()
    if now.tzinfo is None:
        raise AttemptError("recovery_refused", "recovery lease clock is not timezone-aware")
    validate_lease_identity(lease, expected=expected)
    if now < lease.cleanup_deadline:
        raise AttemptError("recovery_refused", "recovery lease has not expired")
    transport = transport_factory(lease)
    observed = transport.teardown({"owner_app": lease.app})
    expected_keys = {"app_exists", "machines", "volumes", "secrets"}
    if not isinstance(observed, Mapping) or set(observed) != expected_keys:
        raise AttemptError("inventory_unavailable", "teardown inventory is malformed")
    return {
        "app_exists": bool(observed["app_exists"]),
        "machines": int(observed["machines"]),
        "volumes": int(observed["volumes"]),
        "secrets": int(observed["secrets"]),
    }
