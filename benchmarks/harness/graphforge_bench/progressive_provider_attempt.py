"""Offline whole-attempt control for the progressive provider ladder.

This module owns ordering, durable local state, evidence admission, and teardown
semantics.  Provider I/O is deliberately absent: callers must supply a
``ProviderTransport`` and may inject the no-spend planner and validation hooks.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Protocol

from jsonschema import Draft202012Validator

from graphforge_bench.progressive_provider_plan import (
    ProviderPlanError,
    completed_rungs,
    plan_provider_ladder,
)

AUTHORIZATION_SCHEMA = "graphforge-progressive-spend-authorization/1"
ATTEMPT_SCHEMA = "graphforge-progressive-provider-attempt-result/1"
LEDGER_SCHEMA = "graphforge-progressive-provider-attempt-ledger/1"
RESULT_SCHEMA = "graphforge-progressive-provider-run-result/1"
CANONICAL_RUNGS = (18, 19, 20, 22, 24, 25, 26)
PROVIDER_RUNGS = (20, 22, 24, 25, 26)
COMMIT = re.compile(r"^[0-9a-f]{40}$")
SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
SAFE_REGION = re.compile(r"^[a-z]{3}$")
IMAGE_DIGEST = re.compile(r"^registry\.fly\.io/[a-z0-9][a-z0-9._/-]*@sha256:[0-9a-f]{64}$")
ATTEMPT_ID = re.compile(r"^[0-9a-f]{32}$")
MACHINE_ID = re.compile(r"^[0-9a-f]{14}$")
VOLUME_ID = re.compile(r"^vol_[a-z0-9]+$")
SCHEMA_ROOT = Path(__file__).resolve().parents[2] / "schemas"


class AttemptError(RuntimeError):
    """A closed failure suitable for an attempt result or operator diagnostic."""

    def __init__(self, failure: str, message: str):
        super().__init__(message)
        self.failure = failure


@dataclass(frozen=True)
class SpendAuthorization:
    """Typed, bounded authority supplied by the protected control plane."""

    schema: str
    status: str
    provider: str
    commit: str
    admitted_plan_sha256: str
    image_digest: str
    organization: str
    region: str
    machine_class: str
    volume_gib: int
    rung: str
    maximum_scale: int
    attempt_nonce: str
    app: str
    issued_at: datetime
    expires_at: datetime
    teardown_owner: str
    maximum_machine_seconds: int
    resource_limits: Mapping[str, int]
    pricing: Mapping[str, Any]
    claim: str
    authorization_sha256: str


@dataclass(frozen=True)
class AttemptInvocation:
    """Local paths and immutable identity for one controller invocation."""

    root: Path
    evidence_dir: Path
    ledger_path: Path
    commit: str
    provider_capacity: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class AttemptRequest:
    """Operator-facing request kept separate from protected spend authority."""

    commit: str
    organization: str
    app: str
    region: str
    machine_class: str
    volume_gib: int
    image_digest: str
    maximum_scale: int
    spend_authorization: str | bytes | Mapping[str, Any] | None
    provider_capacity: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class ProvisionedAttempt:
    """Provider-observed immutable image identity plus opaque cleanup handles."""

    image_digest: str
    resources: Mapping[str, str] = field(default_factory=dict)


@dataclass
class AttemptLedger:
    """Durable recovery state; authorization and provider output are excluded."""

    schema: str = LEDGER_SCHEMA
    generation: int = 0
    attempt_id: str | None = None
    owner_app: str | None = None
    commit: str | None = None
    authorization_sha256: str | None = None
    admitted_plan_sha256: str | None = None
    authorized_maximum_scale: int | None = None
    authorized_image_digest: str | None = None
    expires_at: str | None = None
    evidence_dir: str | None = None
    phase: str = "new"
    image_digest: str | None = None
    current_rung: int | None = None
    completed_scales: list[int] = field(default_factory=list)
    first_failed_rung: int | None = None
    failure: str | None = None
    cleanup_failure: str | None = None
    resources: dict[str, str] = field(default_factory=dict)
    teardown_observed: dict[str, Any] | None = None
    teardown_checked_at: str | None = None


@dataclass(frozen=True)
class AttemptOutcome:
    schema: str
    status: str
    commit: str
    authorized_maximum_scale: int
    completed_scales: tuple[int, ...]
    first_failed_rung: int | None
    failure: str | None
    cleanup_failure: str | None
    authorization_sha256: str
    admitted_plan_sha256: str
    authorized_image_digest: str
    observed_image_digest: str | None
    teardown_status: str
    teardown_observed: Mapping[str, Any] | None
    teardown_checked_at: str | None


class ProviderTransport(Protocol):
    """The sole boundary at which a future live controller may touch a provider."""

    def provision(
        self,
        invocation: AttemptInvocation,
        authorization: SpendAuthorization,
        *,
        deadline: datetime,
    ) -> ProvisionedAttempt: ...

    def upload_plan(self, *, rung: int, plan_path: Path, deadline: datetime) -> None: ...

    def execute_rung(self, *, rung: int, image_digest: str, deadline: datetime) -> int: ...

    def retrieve_result(self, *, rung: int, destination: Path, deadline: datetime) -> None: ...

    def retrieve_success_artifacts(
        self,
        *,
        rung: int,
        names: Sequence[str],
        destination: Path,
        deadline: datetime,
    ) -> None: ...

    def teardown(self, resources: Mapping[str, str]) -> Mapping[str, Any]: ...


class Planner(Protocol):
    def __call__(
        self,
        *,
        root: Path,
        output_dir: Path,
        commit: str,
        maximum_scale: int,
        provider_capacity: Mapping[str, Any] | None,
        image_digest: str | None,
    ) -> Mapping[str, Any]: ...


ResultValidator = Callable[[Path, int], Mapping[str, Any]]
BundleValidator = Callable[[Path, int, Mapping[str, Any]], None]
PrefixReader = Callable[..., list[Mapping[str, Any]]]


def _integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise AttemptError("authorization_refused", "spend authorization expiry is invalid")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as error:
        raise AttemptError(
            "authorization_refused", "spend authorization expiry is invalid"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise AttemptError("authorization_refused", "spend authorization expiry is invalid")
    return parsed


def parse_spend_authorization(value: str | bytes | Mapping[str, Any]) -> SpendAuthorization:
    """Parse a closed authorization document without retaining its encoded form."""
    if isinstance(value, (str, bytes)):
        try:
            decoded = json.loads(value)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise AttemptError(
                "authorization_refused", "spend authorization is malformed"
            ) from error
    else:
        decoded = dict(value)
    expected = {
        "schema",
        "status",
        "provider",
        "commit",
        "admitted_plan_sha256",
        "image_digest",
        "organization",
        "region",
        "machine_class",
        "volume_gib",
        "rung",
        "maximum_scale",
        "attempt_nonce",
        "app",
        "issued_at",
        "expires_at",
        "teardown_owner",
        "maximum_machine_seconds",
        "resource_limits",
        "pricing",
        "claim",
    }
    if not isinstance(decoded, dict) or set(decoded) != expected:
        raise AttemptError("authorization_refused", "spend authorization shape is invalid")
    _validate_schema("progressive-spend-authorization.json", decoded, "authorization_refused")
    pricing = decoded.get("pricing")
    pricing_fields = {
        "currency",
        "machine_microusd_per_hour",
        "volume_microusd_per_gib_hour",
        "transfer_allowance_microusd",
        "estimated_total_microusd",
        "maximum_total_microusd",
    }
    if (
        decoded.get("schema") != AUTHORIZATION_SCHEMA
        or decoded.get("status") != "authorized"
        or decoded.get("provider") != "fly"
        or not isinstance(decoded.get("commit"), str)
        or COMMIT.fullmatch(decoded["commit"]) is None
        or not isinstance(decoded.get("admitted_plan_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", decoded["admitted_plan_sha256"]) is None
        or not isinstance(decoded.get("image_digest"), str)
        or IMAGE_DIGEST.fullmatch(decoded["image_digest"]) is None
        or not isinstance(decoded.get("app"), str)
        or SAFE_NAME.fullmatch(decoded["app"]) is None
        or not isinstance(decoded.get("organization"), str)
        or SAFE_NAME.fullmatch(decoded["organization"]) is None
        or not isinstance(decoded.get("region"), str)
        or SAFE_REGION.fullmatch(decoded["region"]) is None
        or not isinstance(decoded.get("machine_class"), str)
        or SAFE_NAME.fullmatch(decoded["machine_class"]) is None
        or not _integer(decoded.get("volume_gib"))
        or not 1 <= decoded["volume_gib"] <= 500
        or not _integer(decoded.get("maximum_scale"))
        or decoded["maximum_scale"] not in PROVIDER_RUNGS
        or decoded.get("rung") not in {f"S{scale}" for scale in PROVIDER_RUNGS}
        or int(str(decoded["rung"])[1:]) > decoded["maximum_scale"]
        or not isinstance(decoded.get("attempt_nonce"), str)
        or ATTEMPT_ID.fullmatch(decoded["attempt_nonce"]) is None
        or decoded.get("app") != f"gf-progressive-{decoded.get('attempt_nonce')}"
        or not isinstance(decoded.get("teardown_owner"), str)
        or SAFE_NAME.fullmatch(decoded["teardown_owner"]) is None
        or not _integer(decoded.get("maximum_machine_seconds"))
        or not 1 <= decoded["maximum_machine_seconds"] <= 18_000
        or decoded.get("resource_limits")
        != {"apps": 1, "volumes": 1, "machines": 1, "image_builds": 0}
        or not isinstance(pricing, dict)
        or set(pricing) != pricing_fields
        or pricing.get("currency") != "USD"
        or any(
            not _integer(pricing.get(name)) or not 0 <= pricing[name] <= 1_000_000_000_000
            for name in pricing_fields - {"currency"}
        )
        or pricing["estimated_total_microusd"] < 1
        or pricing["maximum_total_microusd"] < 1
        or pricing["estimated_total_microusd"] > pricing["maximum_total_microusd"]
        or decoded.get("claim") != "spend_authorization_only"
    ):
        raise AttemptError("authorization_refused", "spend authorization values are invalid")
    issued_at = _timestamp(decoded["issued_at"])
    expires_at = _timestamp(decoded["expires_at"])
    if expires_at <= issued_at or expires_at - issued_at > timedelta(hours=5):
        raise AttemptError("authorization_refused", "spend authorization lifetime is invalid")
    seconds = decoded["maximum_machine_seconds"]
    machine = (pricing["machine_microusd_per_hour"] * seconds + 3599) // 3600
    volume = (
        pricing["volume_microusd_per_gib_hour"] * decoded["volume_gib"] * seconds + 3599
    ) // 3600
    conservative_total = machine + volume + pricing["transfer_allowance_microusd"]
    if conservative_total > pricing["estimated_total_microusd"]:
        raise AttemptError("authorization_refused", "spend authorization ceiling is insufficient")
    canonical = json.dumps(decoded, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return SpendAuthorization(
        schema=decoded["schema"],
        status=decoded["status"],
        provider=decoded["provider"],
        commit=decoded["commit"],
        admitted_plan_sha256=decoded["admitted_plan_sha256"],
        image_digest=decoded["image_digest"],
        organization=decoded["organization"],
        region=decoded["region"],
        machine_class=decoded["machine_class"],
        volume_gib=decoded["volume_gib"],
        rung=decoded["rung"],
        maximum_scale=decoded["maximum_scale"],
        attempt_nonce=decoded["attempt_nonce"],
        app=decoded["app"],
        issued_at=issued_at,
        expires_at=expires_at,
        teardown_owner=decoded["teardown_owner"],
        maximum_machine_seconds=decoded["maximum_machine_seconds"],
        resource_limits=dict(decoded["resource_limits"]),
        pricing=dict(decoded["pricing"]),
        claim=decoded["claim"],
        authorization_sha256=hashlib.sha256(canonical).hexdigest(),
    )


def validate_authorization(
    invocation: AttemptInvocation,
    authorization: SpendAuthorization,
    *,
    now: datetime | None = None,
) -> None:
    observed_now = now or datetime.now(timezone.utc)
    if observed_now.tzinfo is None:
        raise AttemptError("authorization_refused", "authorization clock is not timezone-aware")
    if not COMMIT.fullmatch(invocation.commit) or invocation.commit != authorization.commit:
        raise AttemptError("authorization_refused", "authorization commit mismatch")
    if authorization.issued_at > observed_now or observed_now >= authorization.expires_at:
        raise AttemptError("authorization_refused", "spend authorization has expired")
    if (
        observed_now + timedelta(seconds=authorization.maximum_machine_seconds)
        > authorization.expires_at
    ):
        raise AttemptError(
            "authorization_refused", "spend authorization cannot cover its runtime ceiling"
        )


def _require_before_deadline(clock: Callable[[], datetime], deadline: datetime) -> None:
    observed = clock()
    if observed.tzinfo is None or observed >= deadline:
        raise AttemptError("authorization_refused", "attempt execution deadline has expired")


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


def _validate_schema(name: str, value: Mapping[str, Any], failure: str) -> None:
    try:
        schema = json.loads((SCHEMA_ROOT / name).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AttemptError(failure, "attempt schema is unavailable") from error
    error = next(Draft202012Validator(schema).iter_errors(value), None)
    if error is not None:
        raise AttemptError(failure, "attempt document failed its schema")


def save_ledger(path: Path, ledger: AttemptLedger) -> None:
    document = asdict(ledger)
    _validate_schema("progressive-provider-attempt-ledger.json", document, "recovery_refused")
    _atomic_json(path, document)


def load_ledger(path: Path) -> AttemptLedger:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AttemptError(
            "recovery_refused", "attempt ledger is unavailable or malformed"
        ) from error
    if isinstance(value, dict):
        _validate_schema("progressive-provider-attempt-ledger.json", value, "recovery_refused")
        if value.get("owner_app") != f"gf-progressive-{value.get('attempt_id')}":
            raise AttemptError("recovery_refused", "attempt ledger ownership is inconsistent")
    if not isinstance(value, dict) or value.pop("schema", None) != LEDGER_SCHEMA:
        raise AttemptError("recovery_refused", "attempt ledger schema is invalid")
    expected = {name for name in AttemptLedger.__dataclass_fields__ if name != "schema"}
    if set(value) != expected:
        raise AttemptError("recovery_refused", "attempt ledger shape is invalid")
    try:
        ledger = AttemptLedger(**value)
    except TypeError as error:
        raise AttemptError("recovery_refused", "attempt ledger is malformed") from error
    if (
        not isinstance(ledger.completed_scales, list)
        or any(
            not _integer(scale) or scale not in CANONICAL_RUNGS for scale in ledger.completed_scales
        )
        or not isinstance(ledger.resources, dict)
        or any(
            not isinstance(key, str) or not isinstance(item, str)
            for key, item in ledger.resources.items()
        )
    ):
        raise AttemptError("recovery_refused", "attempt ledger values are invalid")
    return ledger


def _prefix_scales(prefix: Sequence[Mapping[str, Any]]) -> list[int]:
    scales = [item.get("scale") for item in prefix]
    if any(not _integer(scale) for scale in scales):
        raise AttemptError("prerequisite_refused", "completed rung prefix is malformed")
    typed = [int(scale) for scale in scales]
    if typed != list(CANONICAL_RUNGS[: len(typed)]):
        raise AttemptError("prerequisite_refused", "completed rung prefix is not contiguous")
    if typed[:2] != [18, 19]:
        raise AttemptError("prerequisite_refused", "passed S18 and S19 evidence is required")
    return typed


def validate_result(path: Path, rung: int) -> Mapping[str, Any]:
    """Perform the result-first closed-shape check before retrieving large artifacts."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AttemptError(
            "retrieval_failed", "provider rung result is unavailable or malformed"
        ) from error
    if (
        not isinstance(value, dict)
        or value.get("schema") != RESULT_SCHEMA
        or value.get("rung") != f"S{rung}"
        or value.get("status") not in {"passed", "failed"}
        or (value.get("status") == "passed" and value.get("failure") is not None)
        or (value.get("status") == "failed" and not isinstance(value.get("failure"), str))
    ):
        raise AttemptError("evidence_invalid", "provider rung result identity is invalid")
    _validate_schema("progressive-provider-run-result.json", value, "evidence_invalid")
    return value


def validate_staged_bundle(_stage: Path, _rung: int, _result: Mapping[str, Any]) -> None:
    """Default hook; authoritative repository validation follows publication."""


def _publish(path: Path, destination: Path) -> None:
    if destination.exists() or destination.is_symlink():
        raise AttemptError("retrieval_failed", "attempt evidence already exists")
    try:
        os.link(path, destination)
        directory = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError as error:
        raise AttemptError("retrieval_failed", "attempt evidence publication failed") from error


def _artifact_names(rung: int) -> tuple[str, ...]:
    return tuple(f"s{rung}-{suffix}.json" for suffix in ("plan", "benchexec", "graphforge", "rung"))


def _rollback_rung(evidence_dir: Path, rung: int) -> None:
    for name in (*_artifact_names(rung), f"s{rung}-result.json"):
        (evidence_dir / name).unlink(missing_ok=True)
    directory = os.open(evidence_dir, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _try_rollback_rung(evidence_dir: Path, rung: int) -> str | None:
    try:
        _rollback_rung(evidence_dir, rung)
    except Exception:
        return "evidence_cleanup_failed"
    return None


def _require_fresh_rung(evidence_dir: Path, rung: int) -> None:
    if any(
        (evidence_dir / name).exists() or (evidence_dir / name).is_symlink()
        for name in (*_artifact_names(rung), f"s{rung}-result.json")
    ):
        raise AttemptError(
            "source_mismatch", "incomplete provider evidence requires cleanup-only recovery"
        )


def _admitted_plan(
    invocation: AttemptInvocation,
    authorization: SpendAuthorization,
    next_rung: int,
    planner: Planner,
) -> Mapping[str, Any]:
    try:
        plan = planner(
            root=invocation.root,
            output_dir=invocation.evidence_dir,
            commit=invocation.commit,
            maximum_scale=authorization.maximum_scale,
            provider_capacity=invocation.provider_capacity,
            image_digest=authorization.image_digest,
        )
    except (OSError, ProviderPlanError, ValueError) as error:
        raise AttemptError("progression_refused", "next provider rung was not admitted") from error
    if (
        not isinstance(plan, Mapping)
        or plan.get("status") != "admitted"
        or plan.get("execution_authorized") is not True
        or plan.get("execution_refusal") is not None
        or plan.get("next_rung") != f"S{next_rung}"
        or plan.get("image_digest") != authorization.image_digest
    ):
        raise AttemptError("progression_refused", "planner violated sequential authority")
    return plan


def _plan_digest(path: Path, plan: Mapping[str, Any]) -> str:
    _atomic_json(path, plan)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _cleanup_handles(ledger: AttemptLedger) -> dict[str, str]:
    handles = dict(ledger.resources)
    if ledger.owner_app is not None:
        handles["owner_app"] = ledger.owner_app
    return handles


def _teardown_observation(value: Mapping[str, Any]) -> dict[str, Any]:
    expected = {"app_exists", "machines", "volumes", "secrets"}
    if not isinstance(value, Mapping) or set(value) != expected:
        raise AttemptError("inventory_unavailable", "teardown inventory is malformed")
    if type(value["app_exists"]) is not bool or any(
        not _integer(value[name]) or value[name] < 0 for name in expected - {"app_exists"}
    ):
        raise AttemptError("inventory_unavailable", "teardown inventory is malformed")
    return dict(value)


def _outcome(ledger: AttemptLedger) -> AttemptOutcome:
    failure = ledger.failure
    if failure is None and ledger.cleanup_failure is not None:
        failure = "cleanup_failed"
    return AttemptOutcome(
        schema=ATTEMPT_SCHEMA,
        status="passed" if failure is None else "failed",
        commit=str(ledger.commit),
        authorized_maximum_scale=int(ledger.authorized_maximum_scale),
        completed_scales=tuple(ledger.completed_scales),
        first_failed_rung=ledger.first_failed_rung,
        failure=failure,
        cleanup_failure=ledger.cleanup_failure,
        authorization_sha256=str(ledger.authorization_sha256),
        admitted_plan_sha256=str(ledger.admitted_plan_sha256),
        authorized_image_digest=str(ledger.authorized_image_digest),
        observed_image_digest=ledger.image_digest,
        teardown_status="failed" if ledger.cleanup_failure else "empty",
        teardown_observed=ledger.teardown_observed,
        teardown_checked_at=ledger.teardown_checked_at,
    )


def execute(
    invocation: AttemptInvocation,
    authorization: SpendAuthorization,
    *,
    transport: ProviderTransport,
    planner: Planner = plan_provider_ladder,
    prefix_reader: PrefixReader = completed_rungs,
    result_validator: ResultValidator = validate_result,
    bundle_validator: BundleValidator = validate_staged_bundle,
    now: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
) -> AttemptOutcome:
    """Execute a bounded provider attempt, one admitted rung at a time."""
    clock_fn = clock or (lambda: datetime.now(timezone.utc))
    started_at = now or clock_fn()
    validate_authorization(invocation, authorization, now=started_at)
    deadline = started_at + timedelta(seconds=authorization.maximum_machine_seconds)
    if invocation.ledger_path.exists() or invocation.ledger_path.is_symlink():
        raise AttemptError(
            "recovery_refused", "existing attempt ledger requires cleanup-only recovery"
        )
    invocation.evidence_dir.mkdir(parents=True, exist_ok=True)
    try:
        prefix = prefix_reader(
            invocation.root,
            invocation.evidence_dir,
            commit=invocation.commit,
        )
    except (OSError, ProviderPlanError, ValueError) as error:
        raise AttemptError("prerequisite_refused", "completed rung evidence is invalid") from error
    scales = _prefix_scales(prefix)
    if any(scale > authorization.maximum_scale for scale in scales):
        raise AttemptError("authorization_refused", "completed evidence exceeds authorization")
    if scales == list(CANONICAL_RUNGS) or scales[-1] >= authorization.maximum_scale:
        raise AttemptError("progression_refused", "authorized provider ladder is already complete")

    next_rung = next(rung for rung in PROVIDER_RUNGS if rung not in scales)
    _require_fresh_rung(invocation.evidence_dir, next_rung)
    control_plan = invocation.ledger_path.with_name(f".{invocation.ledger_path.name}.plan.json")
    first_plan = _admitted_plan(invocation, authorization, next_rung, planner)
    first_plan_sha256 = _plan_digest(control_plan, first_plan)
    if authorization.rung != f"S{next_rung}":
        control_plan.unlink(missing_ok=True)
        raise AttemptError("authorization_refused", "authorization selects a different first rung")
    if authorization.admitted_plan_sha256 != first_plan_sha256:
        control_plan.unlink(missing_ok=True)
        raise AttemptError("authorization_refused", "admitted plan contradicts authorization")

    ledger = AttemptLedger(
        generation=1,
        attempt_id=authorization.attempt_nonce,
        owner_app=authorization.app,
        commit=invocation.commit,
        authorization_sha256=authorization.authorization_sha256,
        admitted_plan_sha256=authorization.admitted_plan_sha256,
        authorized_maximum_scale=authorization.maximum_scale,
        authorized_image_digest=authorization.image_digest,
        expires_at=authorization.expires_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        evidence_dir=str(invocation.evidence_dir.resolve()),
        phase="authorized",
        completed_scales=scales,
    )
    save_ledger(invocation.ledger_path, ledger)
    from graphforge_bench.progressive_recovery_lease import (
        ack_recovery_lease,
        build_recovery_lease,
    )

    lease_path = invocation.ledger_path.with_name(
        f"{invocation.ledger_path.stem}.recovery-lease.json"
    )
    if lease_path.exists() or lease_path.is_symlink():
        raise AttemptError(
            "recovery_refused", "existing recovery lease requires expired-lease cleanup"
        )
    ack_recovery_lease(
        lease_path,
        build_recovery_lease(
            authorization,
            execution_deadline=deadline,
            acknowledged_at=started_at,
        ),
    )
    provisioned: ProvisionedAttempt | None = None
    primary_error: AttemptError | None = None
    pending_plan: Mapping[str, Any] | None = first_plan
    try:
        _require_before_deadline(clock_fn, deadline)
        provisioned = transport.provision(invocation, authorization, deadline=deadline)
        if IMAGE_DIGEST.fullmatch(provisioned.image_digest) is None:
            raise AttemptError(
                "machine_identity_mismatch", "provider-observed image digest is invalid"
            )
        if provisioned.image_digest != authorization.image_digest:
            raise AttemptError(
                "machine_identity_mismatch",
                "provider-observed image digest contradicts authorization",
            )
        if (
            set(provisioned.resources) != {"machine_id", "volume_id"}
            or not isinstance(provisioned.resources.get("machine_id"), str)
            or MACHINE_ID.fullmatch(provisioned.resources["machine_id"]) is None
            or not isinstance(provisioned.resources.get("volume_id"), str)
            or VOLUME_ID.fullmatch(provisioned.resources["volume_id"]) is None
        ):
            raise AttemptError("provision_failed", "provider cleanup handles are malformed")
        ledger.image_digest = provisioned.image_digest
        ledger.resources = dict(provisioned.resources)
        ledger.phase = "provisioned"
        save_ledger(invocation.ledger_path, ledger)

        while ledger.completed_scales[-1] < authorization.maximum_scale:
            _require_before_deadline(clock_fn, deadline)
            next_rung = next(rung for rung in PROVIDER_RUNGS if rung not in ledger.completed_scales)
            _require_fresh_rung(invocation.evidence_dir, next_rung)
            ledger.current_rung = next_rung
            plan = pending_plan or _admitted_plan(invocation, authorization, next_rung, planner)
            pending_plan = None
            ledger.phase = "planned"
            save_ledger(invocation.ledger_path, ledger)
            _atomic_json(control_plan, plan)
            try:
                transport.upload_plan(rung=next_rung, plan_path=control_plan, deadline=deadline)
                _require_before_deadline(clock_fn, deadline)
            except AttemptError:
                raise
            except Exception as error:
                raise AttemptError("upload_failed", "provider plan upload failed") from error
            ledger.phase = "executing"
            save_ledger(invocation.ledger_path, ledger)
            try:
                execution_status = transport.execute_rung(
                    rung=next_rung,
                    image_digest=provisioned.image_digest,
                    deadline=deadline,
                )
                _require_before_deadline(clock_fn, deadline)
            except AttemptError:
                raise
            except Exception as error:
                raise AttemptError("rung_failed", "provider rung execution failed") from error

            with tempfile.TemporaryDirectory(
                prefix=f".s{next_rung}-attempt-", dir=invocation.evidence_dir
            ) as temporary:
                stage = Path(temporary)
                result_name = f"s{next_rung}-result.json"
                staged_result = stage / result_name
                try:
                    transport.retrieve_result(
                        rung=next_rung, destination=staged_result, deadline=deadline
                    )
                    _require_before_deadline(clock_fn, deadline)
                except AttemptError:
                    raise
                except Exception as error:
                    raise AttemptError(
                        "retrieval_failed", "provider result retrieval failed"
                    ) from error
                result = result_validator(staged_result, next_rung)
                identities = result.get("identities")
                if (
                    not isinstance(identities, Mapping)
                    or identities.get("commit") != invocation.commit
                    or identities.get("image_digest") != provisioned.image_digest
                ):
                    raise AttemptError(
                        "evidence_invalid", "provider rung result identity is invalid"
                    )
                if result["status"] == "failed":
                    _publish(staged_result, invocation.evidence_dir / result_name)
                    ledger.first_failed_rung = next_rung
                    ledger.failure = "rung_failed"
                    ledger.phase = "rung_failed"
                    save_ledger(invocation.ledger_path, ledger)
                    break
                if execution_status != 0:
                    raise AttemptError(
                        "rung_failed",
                        "successful rung result contradicts execution status",
                    )
                names = _artifact_names(next_rung)
                try:
                    transport.retrieve_success_artifacts(
                        rung=next_rung,
                        names=names,
                        destination=stage,
                        deadline=deadline,
                    )
                    _require_before_deadline(clock_fn, deadline)
                except AttemptError:
                    raise
                except Exception as error:
                    raise AttemptError(
                        "retrieval_failed", "provider artifact retrieval failed"
                    ) from error
                if any(not (stage / name).is_file() for name in names):
                    raise AttemptError("retrieval_failed", "provider rung artifact is unavailable")
                bundle_validator(stage, next_rung, result)
                try:
                    for name in (*names, result_name):
                        _publish(stage / name, invocation.evidence_dir / name)
                except AttemptError as error:
                    if _try_rollback_rung(invocation.evidence_dir, next_rung) is not None:
                        raise AttemptError(
                            "evidence_invalid", "partial evidence cleanup failed"
                        ) from error
                    raise
                _require_before_deadline(clock_fn, deadline)

            try:
                accepted = prefix_reader(
                    invocation.root,
                    invocation.evidence_dir,
                    commit=invocation.commit,
                )
            except (OSError, ProviderPlanError, ValueError) as error:
                _try_rollback_rung(invocation.evidence_dir, next_rung)
                raise AttemptError(
                    "evidence_invalid", "retrieved rung evidence was not accepted"
                ) from error
            accepted_scales = _prefix_scales(accepted)
            if accepted_scales != [*ledger.completed_scales, next_rung]:
                _try_rollback_rung(invocation.evidence_dir, next_rung)
                raise AttemptError("evidence_invalid", "accepted prefix did not advance once")
            ledger.completed_scales = accepted_scales
            ledger.current_rung = None
            ledger.phase = "rung_accepted"
            save_ledger(invocation.ledger_path, ledger)
        if ledger.failure is None:
            ledger.phase = "completed"
            save_ledger(invocation.ledger_path, ledger)
    except AttemptError as error:
        primary_error = error
        ledger.failure = ledger.failure or error.failure
        if error.failure in {
            "upload_failed",
            "rung_failed",
            "retrieval_failed",
            "evidence_invalid",
            "progression_refused",
        }:
            ledger.first_failed_rung = ledger.current_rung
        ledger.phase = "failed"
        save_ledger(invocation.ledger_path, ledger)
    except Exception as error:
        primary_error = AttemptError("provision_failed", "attempt boundary failed")
        ledger.failure = ledger.failure or primary_error.failure
        ledger.phase = "failed"
        save_ledger(invocation.ledger_path, ledger)
        primary_error.__cause__ = error
    finally:
        control_plan.unlink(missing_ok=True)
        if ledger.current_rung is not None and ledger.current_rung not in ledger.completed_scales:
            ledger.cleanup_failure = _try_rollback_rung(
                invocation.evidence_dir, ledger.current_rung
            )
        ledger.phase = "teardown"
        save_ledger(invocation.ledger_path, ledger)
        try:
            observed = transport.teardown(_cleanup_handles(ledger))
            ledger.teardown_observed = _teardown_observation(observed)
            ledger.teardown_checked_at = (
                clock_fn().astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            )
            if ledger.teardown_observed != {
                "app_exists": False,
                "machines": 0,
                "volumes": 0,
                "secrets": 0,
            }:
                raise AttemptError("inventory_not_empty", "teardown inventory is not empty")
        except AttemptError as error:
            ledger.cleanup_failure = ledger.cleanup_failure or error.failure
            ledger.phase = "cleanup_failed"
        except Exception:
            ledger.cleanup_failure = ledger.cleanup_failure or "teardown_failed"
            ledger.phase = "cleanup_failed"
        else:
            ledger.resources.clear()
            ledger.phase = "cleanup_failed" if ledger.cleanup_failure else "closed"
        save_ledger(invocation.ledger_path, ledger)

    # Expected rung failures are returned as typed outcomes; unexpected controller
    # failures are likewise preserved in the durable ledger without raw diagnostics.
    _ = primary_error
    return _outcome(ledger)


def _teardown_document(outcome: AttemptOutcome) -> dict[str, Any]:
    return {
        "schema": "graphforge-progressive-provider-teardown-inventory/1",
        "status": outcome.teardown_status,
        "failure": outcome.cleanup_failure,
        "commit": outcome.commit,
        "authorized_maximum_scale": outcome.authorized_maximum_scale,
        "completed_scales": list(outcome.completed_scales),
        "authorization_sha256": outcome.authorization_sha256,
        "admitted_plan_sha256": outcome.admitted_plan_sha256,
        "checked_at": outcome.teardown_checked_at,
        "observed": dict(outcome.teardown_observed) if outcome.teardown_observed else None,
        "claim": "control_plane_evidence_only",
    }


def _outcome_document(outcome: AttemptOutcome, teardown_inventory_sha256: str) -> dict[str, Any]:
    return {
        "schema": outcome.schema,
        "status": outcome.status,
        "commit": outcome.commit,
        "authorized_maximum_scale": outcome.authorized_maximum_scale,
        "completed_scales": list(outcome.completed_scales),
        "first_failed_rung": (
            f"S{outcome.first_failed_rung}" if outcome.first_failed_rung is not None else None
        ),
        "failure": outcome.failure,
        "cleanup_failure": outcome.cleanup_failure,
        "authorization_sha256": outcome.authorization_sha256,
        "admitted_plan_sha256": outcome.admitted_plan_sha256,
        "authorized_image_digest": outcome.authorized_image_digest,
        "observed_image_digest": outcome.observed_image_digest,
        "teardown_status": outcome.teardown_status,
        "teardown_inventory_sha256": teardown_inventory_sha256,
        "claim": "engineering_evidence_only",
    }


def _write_outcome(result_path: Path, outcome: AttemptOutcome) -> dict[str, Any]:
    inventory_path = result_path.with_name(f"{result_path.stem}-teardown-inventory.json")
    inventory = _teardown_document(outcome)
    _validate_schema("progressive-provider-teardown-inventory.json", inventory, "evidence_invalid")
    _atomic_json(inventory_path, inventory)
    inventory_sha256 = hashlib.sha256(inventory_path.read_bytes()).hexdigest()
    document = _outcome_document(outcome, inventory_sha256)
    _validate_schema("progressive-provider-attempt-result.json", document, "evidence_invalid")
    _atomic_json(result_path, document)
    return document


def execute_attempt(
    request: AttemptRequest,
    *,
    root: Path,
    output_dir: Path,
    ledger_path: Path,
    result_path: Path,
    boundary: ProviderTransport,
    planner: Planner = plan_provider_ladder,
    prefix_reader: PrefixReader = completed_rungs,
    result_validator: ResultValidator = validate_result,
    bundle_validator: BundleValidator = validate_staged_bundle,
    now: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Validate an operator request, run the core, and durably write its result."""
    inventory_path = result_path.with_name(f"{result_path.stem}-teardown-inventory.json")
    if any(path.exists() or path.is_symlink() for path in (result_path, inventory_path)):
        raise AttemptError("source_mismatch", "attempt result path already exists")
    if request.spend_authorization is None:
        raise AttemptError("authorization_refused", "spend authorization is required")
    authorization = parse_spend_authorization(request.spend_authorization)
    if (
        request.commit != authorization.commit
        or request.organization != authorization.organization
        or request.app != authorization.app
        or request.region != authorization.region
        or request.machine_class != authorization.machine_class
        or request.volume_gib != authorization.volume_gib
        or request.image_digest != authorization.image_digest
        or request.maximum_scale != authorization.maximum_scale
    ):
        raise AttemptError("authorization_refused", "request contradicts authorization")
    outcome = execute(
        AttemptInvocation(
            root=root,
            evidence_dir=output_dir,
            ledger_path=ledger_path,
            commit=request.commit,
            provider_capacity=request.provider_capacity,
        ),
        authorization,
        transport=boundary,
        planner=planner,
        prefix_reader=prefix_reader,
        result_validator=result_validator,
        bundle_validator=bundle_validator,
        now=now,
        clock=clock,
    )
    return _write_outcome(result_path, outcome)


def cleanup_only(
    ledger_path: Path,
    result_path: Path,
    *,
    transport: ProviderTransport,
) -> dict[str, Any]:
    """Retry teardown from durable state without executing or planning a rung."""
    ledger = load_ledger(ledger_path)
    evidence_dir = Path(str(ledger.evidence_dir))
    if (
        not evidence_dir.is_absolute()
        or evidence_dir.is_symlink()
        or not evidence_dir.is_dir()
        or evidence_dir.resolve() != evidence_dir
    ):
        raise AttemptError("recovery_refused", "attempt evidence directory is unsafe")
    ledger.cleanup_failure = None
    if ledger.current_rung is not None and ledger.current_rung not in ledger.completed_scales:
        ledger.cleanup_failure = _try_rollback_rung(evidence_dir, ledger.current_rung)
    try:
        observed = transport.teardown(_cleanup_handles(ledger))
        ledger.teardown_observed = _teardown_observation(observed)
        ledger.teardown_checked_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if ledger.teardown_observed != {
            "app_exists": False,
            "machines": 0,
            "volumes": 0,
            "secrets": 0,
        }:
            raise AttemptError("inventory_not_empty", "teardown inventory is not empty")
    except AttemptError as error:
        ledger.cleanup_failure = ledger.cleanup_failure or error.failure
        ledger.phase = "cleanup_failed"
    except Exception:
        ledger.cleanup_failure = ledger.cleanup_failure or "teardown_failed"
        ledger.phase = "cleanup_failed"
    else:
        ledger.resources.clear()
        ledger.phase = "cleanup_failed" if ledger.cleanup_failure else "closed"
    save_ledger(ledger_path, ledger)
    return _write_outcome(result_path, _outcome(ledger))
