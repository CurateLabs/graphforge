"""Static, fail-closed command planner for the disposable Fly adapter.

This module never calls Fly.  It turns a closed lifecycle invocation from the
qualification controller into argv vectors and owns only provider resource
lifecycle.  Benchmark policy remains in the checked-in controller/profile.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, fields
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import shlex
from typing import Any


class AdapterError(ValueError):
    """A sanitized refusal; messages never contain provider identifiers."""


OCI_DIGEST = re.compile(r"^registry\.fly\.io/[a-z0-9-]+@sha256:[0-9a-f]{64}$")
SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
SAFE_REGION = re.compile(r"^[a-z]{3}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")
MACHINE_CLASSES = frozenset(
    {
        "shared-cpu-1x",
        "shared-cpu-2x",
        "shared-cpu-4x",
        "shared-cpu-6x",
        "shared-cpu-8x",
        "performance-1x",
        "performance-2x",
        "performance-4x",
        "performance-6x",
        "performance-8x",
        "performance-10x",
        "performance-12x",
        "performance-14x",
        "performance-16x",
    }
)
MACHINE_ID = re.compile(r"^[0-9a-f]{14}$")
VOLUME_ID = re.compile(r"^vol_[a-z0-9]+$")
FAILURE_TYPES = frozenset(
    {
        "authorization_refused",
        "readiness_timeout",
        "build_failed",
        "provision_failed",
        "lifecycle_failed",
        "retrieval_failed",
        "teardown_failed",
        "inventory_not_empty",
        "qualification_failed",
    }
)


@dataclass(frozen=True)
class LifecycleInvocation:
    commit: str
    rung: int
    profile: str
    profile_sha256: str
    argv: tuple[str, ...]
    evidence_files: tuple[str, ...]


@dataclass(frozen=True)
class FlyAttempt:
    organization: str
    app: str
    region: str
    volume_name: str
    volume_gib: int
    machine_class: str
    image: str
    maximum_authorized_scale: int
    prerequisites: Mapping[int, str]
    lifecycle: LifecycleInvocation


@dataclass(frozen=True)
class Command:
    operation: str
    argv: tuple[str, ...]


@dataclass
class ResourceLedger:
    """Local ownership ledger. IDs are never copied into evidence/diagnostics."""

    schema: str = "graphforge-fly-resource-ledger/1"
    app_owned: bool = False
    volume_id: str | None = None
    machine_id: str | None = None
    image_digest: str | None = None
    secret_names: list[str] = field(default_factory=list)
    token_material_present: bool = False

    def save(self, path: Path) -> None:
        path.write_text(json.dumps(asdict(self), indent=2, sort_keys=True) + "\n", encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> ResourceLedger:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise AdapterError("resource ledger is malformed") from error
        _refuse(not isinstance(value, dict), "resource ledger is malformed")
        if value.pop("schema", None) != "graphforge-fly-resource-ledger/1":
            raise AdapterError("resource ledger schema is invalid")
        expected = {item.name for item in fields(cls)} - {"schema"}
        _refuse(set(value) != expected, "resource ledger fields are invalid")
        try:
            ledger = cls(**value)
        except TypeError as error:
            raise AdapterError("resource ledger is malformed") from error
        validate_ledger(ledger)
        return ledger


def _refuse(condition: bool, message: str) -> None:
    if condition:
        raise AdapterError(message)


def validate_ledger(ledger: ResourceLedger) -> None:
    _refuse(type(ledger.app_owned) is not bool, "resource ledger ownership is invalid")
    _refuse(
        type(ledger.token_material_present) is not bool, "resource ledger token state is invalid"
    )
    _refuse(
        ledger.machine_id is not None and not MACHINE_ID.fullmatch(ledger.machine_id),
        "resource ledger machine identifier is invalid",
    )
    _refuse(
        ledger.volume_id is not None and not VOLUME_ID.fullmatch(ledger.volume_id),
        "resource ledger volume identifier is invalid",
    )
    _refuse(
        ledger.image_digest is not None and not OCI_DIGEST.fullmatch(ledger.image_digest),
        "resource ledger image identity is invalid",
    )
    _refuse(
        not isinstance(ledger.secret_names, list)
        or any(
            not isinstance(name, str) or not SAFE_NAME.fullmatch(name)
            for name in ledger.secret_names
        ),
        "resource ledger secrets are invalid",
    )


def validate_attempt(attempt: FlyAttempt) -> None:
    lifecycle = attempt.lifecycle
    _refuse(set(attempt.prerequisites) != {955, 956, 957}, "prerequisite ledger is incomplete")
    _refuse(
        any(state != "merged" for state in attempt.prerequisites.values()),
        "prerequisite is not merged",
    )
    _refuse(not SAFE_NAME.fullmatch(attempt.app), "app name is invalid")
    _refuse(not SAFE_NAME.fullmatch(attempt.organization), "organization name is invalid")
    _refuse(not SAFE_NAME.fullmatch(attempt.volume_name), "volume name is invalid")
    _refuse(not SAFE_REGION.fullmatch(attempt.region), "region is invalid")
    _refuse(attempt.machine_class not in MACHINE_CLASSES, "measured machine class is invalid")
    _refuse(not OCI_DIGEST.fullmatch(attempt.image), "image must be an immutable Fly OCI digest")
    _refuse(
        not attempt.image.startswith(f"registry.fly.io/{attempt.app}@"),
        "image does not belong to the requested app",
    )
    _refuse(attempt.volume_gib < 1 or attempt.volume_gib > 500, "volume size is outside Fly bounds")
    _refuse(not COMMIT.fullmatch(lifecycle.commit), "lifecycle commit is invalid")
    _refuse(
        lifecycle.rung > attempt.maximum_authorized_scale,
        "requested rung lacks explicit authorization",
    )
    _refuse(lifecycle.rung not in (18, 19, 20, 22, 24, 25, 26), "requested rung is not canonical")
    expected = f"benchmarks/profiles/graph500/s{lifecycle.rung}-"
    _refuse(
        not lifecycle.profile.startswith(expected),
        "lifecycle profile does not match requested rung",
    )
    _refuse(
        not re.fullmatch(r"sha256:[0-9a-f]{64}", lifecycle.profile_sha256),
        "profile digest is invalid",
    )
    _refuse(
        not lifecycle.argv or any(not value or "\x00" in value for value in lifecycle.argv),
        "lifecycle argv is invalid",
    )
    expected_evidence = {
        f"s{lifecycle.rung}-plan.json",
        f"s{lifecycle.rung}-benchexec.json",
        f"s{lifecycle.rung}-graphforge.json",
        f"s{lifecycle.rung}-rung.json",
        f"s{lifecycle.rung}-result.json",
    }
    _refuse(
        set(lifecycle.evidence_files) != expected_evidence,
        "lifecycle evidence set is not canonical",
    )
    _refuse(
        any(PurePosixPath(name).name != name for name in lifecycle.evidence_files),
        "lifecycle requested a non-sanitized artifact",
    )


def verify_checked_in_profile(root: Path, lifecycle: LifecycleInvocation) -> None:
    profile = (root / lifecycle.profile).resolve()
    profiles = (root / "benchmarks" / "profiles" / "graph500").resolve()
    _refuse(
        profiles not in profile.parents or not profile.is_file(),
        "checked-in profile is unavailable",
    )
    actual = hashlib.sha256(profile.read_bytes()).hexdigest()
    _refuse(lifecycle.profile_sha256 != f"sha256:{actual}", "checked-in profile digest mismatch")


def remote_build_command(*, app: str, source: Path, dockerfile: Path, commit: str) -> Command:
    _refuse(not SAFE_NAME.fullmatch(app), "app name is invalid")
    _refuse(not COMMIT.fullmatch(commit), "build commit is invalid")
    return Command(
        "remote_build",
        (
            "flyctl",
            "deploy",
            str(source),
            "--app",
            app,
            "--dockerfile",
            str(dockerfile),
            "--remote-only",
            "--build-only",
            "--push",
            "--no-public-ips",
            "--image-label",
            commit,
            "--build-arg",
            f"GRAPHFORGE_COMMIT={commit}",
            "--yes",
        ),
    )


def provisioning_commands(attempt: FlyAttempt) -> tuple[Command, ...]:
    validate_attempt(attempt)
    lifecycle = attempt.lifecycle
    return (
        Command(
            "create_app",
            (
                "flyctl",
                "apps",
                "create",
                attempt.app,
                "--org",
                attempt.organization,
                "--json",
                "--yes",
            ),
        ),
        Command(
            "create_volume",
            (
                "flyctl",
                "volumes",
                "create",
                attempt.volume_name,
                "--app",
                attempt.app,
                "--region",
                attempt.region,
                "--size",
                str(attempt.volume_gib),
                "--count",
                "1",
                "--scheduled-snapshots=false",
                "--json",
                "--yes",
            ),
        ),
        Command(
            "create_machine",
            (
                "flyctl",
                "machine",
                "run",
                attempt.image,
                "infinity",
                "--app",
                attempt.app,
                "--region",
                attempt.region,
                "--vm-size",
                attempt.machine_class,
                "--volume",
                "{volume_id}:/work",
                "--entrypoint",
                "/bin/sleep",
                "--restart",
                "no",
                "--autostop",
                "off",
                "--autostart=false",
                "--rootfs-persist",
                "never",
                "--rm",
                "--skip-dns-registration",
                "--detach",
            ),
        ),
        Command(
            "execute_lifecycle",
            (
                "flyctl",
                "machine",
                "exec",
                "--app",
                attempt.app,
                "--timeout",
                "14400",
                "--json",
                "{machine_id}",
                shlex.join(lifecycle.argv),
            ),
        ),
    )


def retrieval_commands(attempt: FlyAttempt, destination: Path) -> tuple[Command, ...]:
    validate_attempt(attempt)
    return tuple(
        Command(
            "retrieve_evidence",
            (
                "flyctl",
                "ssh",
                "sftp",
                "get",
                f"/work/evidence/{name}",
                str(destination / name),
                "--app",
                attempt.app,
                "--machine",
                "{machine_id}",
                "--quiet",
            ),
        )
        for name in attempt.lifecycle.evidence_files
    )


def cleanup_commands(app: str, ledger: ResourceLedger) -> tuple[Command, ...]:
    """Return repeatable best-effort deletion commands for resources we own."""
    _refuse(not SAFE_NAME.fullmatch(app), "app name is invalid")
    validate_ledger(ledger)
    commands: list[Command] = []
    if ledger.machine_id:
        commands.append(
            Command(
                "destroy_machine",
                ("flyctl", "machine", "destroy", "--app", app, "--force", ledger.machine_id),
            )
        )
    if ledger.volume_id:
        commands.append(
            Command(
                "destroy_volume",
                ("flyctl", "volumes", "destroy", "--app", app, "--yes", ledger.volume_id),
            )
        )
    for name in sorted(set(ledger.secret_names)):
        commands.append(
            Command("unset_secret", ("flyctl", "secrets", "unset", name, "--app", app, "--yes"))
        )
    if ledger.app_owned:
        commands.append(Command("destroy_app", ("flyctl", "apps", "destroy", app, "--yes")))
    return tuple(commands)


def inventory_commands(app: str) -> tuple[Command, ...]:
    _refuse(not SAFE_NAME.fullmatch(app), "app name is invalid")
    return (
        Command("inventory_machines", ("flyctl", "machine", "list", "--app", app, "--json")),
        Command("inventory_volumes", ("flyctl", "volumes", "list", "--app", app, "--json")),
        Command("inventory_secrets", ("flyctl", "secrets", "list", "--app", app, "--json")),
    )


def verify_empty_inventory(*, machines: Any, volumes: Any, secrets: Any, app_exists: bool) -> None:
    for value in (machines, volumes, secrets):
        _refuse(not isinstance(value, list), "provider inventory is malformed")
        _refuse(bool(value), "provider inventory is not empty")
    _refuse(app_exists, "provider inventory is not empty")


def sanitized_failure(failure: str) -> dict[str, Any]:
    _refuse(failure not in FAILURE_TYPES, "failure type is invalid")
    return {
        "schema": "graphforge-fly-adapter-result/1",
        "status": "failed",
        "failure": failure,
    }


def pin_remote_image(app: str, digest: str) -> str:
    """Close remote builder output to the sole immutable runtime identity."""
    image = f"registry.fly.io/{app}@{digest}"
    _refuse(not OCI_DIGEST.fullmatch(image), "remote build did not return an immutable digest")
    return image


def accepted_rung_reclamation(
    *,
    accepted_rung: int,
    current_rung: int,
    running: bool,
    evidence_accepted: bool,
    lifecycle_argv: Sequence[str],
) -> Command:
    _refuse(
        running or accepted_rung >= current_rung or not evidence_accepted,
        "rung artifacts are not reclaimable",
    )
    _refuse(
        not lifecycle_argv or any(not value or "\x00" in value for value in lifecycle_argv),
        "reclamation argv is invalid",
    )
    return Command(
        "reclaim_accepted_rung",
        (
            "flyctl",
            "machine",
            "exec",
            "--app",
            "{app}",
            "--timeout",
            "600",
            "--json",
            "{machine_id}",
            shlex.join(lifecycle_argv),
        ),
    )


def verify_download(path: Path, expected_sha256: str) -> None:
    _refuse(
        not re.fullmatch(
            r"s(?:18|19|20|22|24|25|26)-(?:plan|benchexec|graphforge|rung|result)\.json", path.name
        ),
        "download is not an allowed evidence document",
    )
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise AdapterError("download is unavailable") from error
    actual = hashlib.sha256(payload).hexdigest()
    _refuse(expected_sha256 != f"sha256:{actual}", "download digest mismatch")
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AdapterError("download is not typed evidence") from error
    _refuse(not isinstance(value, dict) or "schema" not in value, "download is not typed evidence")
