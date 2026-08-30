"""Execute the disposable Fly environment qualification for issue #958.

This is deliberately distinct from the S18-S26 ladder.  It provisions only
the existing tiny filesystem smoke, retrieves its closed evidence document,
and tears every owned provider resource down on every terminal path.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
from typing import Any, Protocol
import urllib.error
import urllib.request

from graphforge_bench.fly_adapter import (
    AdapterError,
    ResourceLedger,
    pin_remote_image,
    remote_build_command,
    sanitized_failure,
)

ROOT = Path(__file__).resolve().parents[3]
DOCKERFILE = ROOT / "containers" / "fly-filesystem-qualification" / "Dockerfile"
FLY_BUILD_CONFIG = ROOT / "containers" / "fly-filesystem-qualification" / "fly.build.toml"
VALIDATOR = ROOT / "scripts" / "ci" / "validate-fly-filesystem-qualification.py"
COMMIT = re.compile(r"^[0-9a-f]{40}$")
SAFE_NAME = re.compile(r"^[a-z][a-z0-9-]{2,62}$")
SAFE_VOLUME = re.compile(r"^[a-z][a-z0-9_]{0,29}$")
SAFE_REGION = re.compile(r"^[a-z]{3}$")
MACHINE_ID = re.compile(r"^[0-9a-f]{14}$")
VOLUME_ID = re.compile(r"^vol_[a-z0-9]+$")
SMOKE_EVIDENCE = "/work/fly-qualification-evidence.json"
SMOKE_ACK = "touch /work/controller-ack"
BUILD_TIMEOUT_SECONDS = 1_800
CREATE_TIMEOUT_SECONDS = 180
RETRIEVAL_TIMEOUT_SECONDS = 1_260
SFTP_TIMEOUT_SECONDS = 120
APP_READINESS_TIMEOUT_SECONDS = 60
APP_READINESS_PROBE_TIMEOUT_SECONDS = 5
APP_READINESS_INITIAL_BACKOFF_SECONDS = 0.25
APP_READINESS_MAX_BACKOFF_SECONDS = 2.0
TEARDOWN_POLL_ATTEMPTS = 6
TEARDOWN_POLL_INTERVAL_SECONDS = 2


class QualificationError(RuntimeError):
    """A typed, sanitized terminal failure."""

    def __init__(self, failure: str, message: str):
        super().__init__(message)
        self.failure = failure


class Transport(Protocol):
    """Injectable command boundary used by deterministic offline tests."""

    def run(
        self, argv: tuple[str, ...], *, timeout: int, check: bool = True
    ) -> subprocess.CompletedProcess[str]: ...

    def json(self, argv: tuple[str, ...], *, timeout: int) -> Any: ...

    def resolve_image(self, app: str, tag: str, *, timeout: int) -> str: ...

    def machine_state(self, app: str, machine_id: str, *, timeout: int) -> Any: ...


class FlyctlTransport:
    """Run flyctl without ever copying stdout or stderr into evidence."""

    def run(
        self, argv: tuple[str, ...], *, timeout: int, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            argv,
            cwd=ROOT,
            check=check,
            text=True,
            capture_output=True,
            timeout=timeout,
        )

    def json(self, argv: tuple[str, ...], *, timeout: int) -> Any:
        try:
            return json.loads(self.run(argv, timeout=timeout).stdout)
        except json.JSONDecodeError as error:
            raise QualificationError("provision_failed", "provider JSON is malformed") from error

    def resolve_image(self, app: str, tag: str, *, timeout: int) -> str:
        """Resolve a pushed tag through the registry without writing credentials."""
        token = self.run(("flyctl", "auth", "token"), timeout=30).stdout.strip()
        if not token:
            raise QualificationError("build_failed", "registry authentication is unavailable")
        request = urllib.request.Request(
            f"https://registry.fly.io/v2/{app}/manifests/{tag}",
            headers={
                "Accept": ", ".join(
                    (
                        "application/vnd.oci.image.index.v1+json",
                        "application/vnd.oci.image.manifest.v1+json",
                        "application/vnd.docker.distribution.manifest.list.v2+json",
                        "application/vnd.docker.distribution.manifest.v2+json",
                    )
                ),
                "Authorization": f"Bearer {token}",
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                digest = response.headers.get("Docker-Content-Digest")
        except (urllib.error.HTTPError, urllib.error.URLError):
            raise QualificationError("build_failed", "immutable image lookup failed") from None
        finally:
            token = ""
        if not isinstance(digest, str):
            raise QualificationError("build_failed", "immutable image digest is unavailable")
        return pin_remote_image(app, digest)

    def machine_state(self, app: str, machine_id: str, *, timeout: int) -> Any:
        """Read the authoritative Machine configuration without logging a token."""
        token = self.run(("flyctl", "auth", "token"), timeout=30).stdout.strip()
        if not token:
            raise QualificationError("provision_failed", "Machine authentication is unavailable")
        request = urllib.request.Request(
            f"https://api.machines.dev/v1/apps/{app}/machines/{machine_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError):
            raise QualificationError("provision_failed", "Machine state lookup failed") from None
        finally:
            token = ""


@dataclass(frozen=True)
class TinyQualificationInvocation:
    """Closed provider inputs for the non-ladder filesystem smoke."""

    commit: str
    organization: str
    app: str
    region: str
    volume_name: str
    machine_name: str
    prerequisites: Mapping[int, str]
    machine_class: str = "performance-1x"
    volume_gib: int = 10


@dataclass(frozen=True)
class LiveMachineSize:
    """Current provider shape independently observed during admission."""

    name: str
    cpus: int
    memory_mb: int
    baseline_apps: frozenset[str]


def validate_invocation(invocation: TinyQualificationInvocation) -> None:
    """Validate static inputs without treating the smoke as an S18 rung."""
    if not COMMIT.fullmatch(invocation.commit):
        raise AdapterError("commit is invalid")
    if set(invocation.prerequisites) != {955, 956, 957} or any(
        state != "merged" for state in invocation.prerequisites.values()
    ):
        raise AdapterError("prerequisite ledger is not fully merged")
    for value in (invocation.organization, invocation.app, invocation.machine_name):
        if not SAFE_NAME.fullmatch(value):
            raise AdapterError("provider name is invalid")
    if not SAFE_VOLUME.fullmatch(invocation.volume_name):
        raise AdapterError("volume name is invalid")
    if not SAFE_REGION.fullmatch(invocation.region):
        raise AdapterError("fixed region is invalid")
    if invocation.machine_class != "performance-1x":
        raise AdapterError("tiny qualification must use the smallest performance preset")
    if not 1 <= invocation.volume_gib <= 20:
        raise AdapterError("tiny qualification volume is outside its small bound")


def check_source(root: Path, commit: str) -> None:
    """Bind the remote build context to one clean exact commit."""
    head = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=root,
        check=True,
        text=True,
        capture_output=True,
        timeout=30,
    ).stdout.strip()
    dirty = subprocess.run(
        ("git", "status", "--porcelain"),
        cwd=root,
        check=True,
        text=True,
        capture_output=True,
        timeout=30,
    ).stdout
    if head != commit or dirty:
        raise QualificationError("authorization_refused", "source is not the clean exact commit")


def _list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise QualificationError("provision_failed", f"{label} inventory is malformed")
    return value


def verify_live_capacity(
    transport: Transport, invocation: TinyQualificationInvocation
) -> LiveMachineSize:
    """Require a current fixed region and the smallest live performance preset."""
    apps = _list(
        transport.json(("flyctl", "apps", "list", "--json"), timeout=CREATE_TIMEOUT_SECONDS),
        "app",
    )
    baseline_apps = frozenset(
        item.get("Name") or item.get("name") for item in apps if isinstance(item, dict)
    )
    if len(baseline_apps) != len(apps) or any(
        not isinstance(name, str) or not SAFE_NAME.fullmatch(name) for name in baseline_apps
    ):
        raise QualificationError("provision_failed", "app inventory is malformed")
    if any(
        item.get("Name") == invocation.app or item.get("name") == invocation.app for item in apps
    ):
        raise QualificationError("authorization_refused", "app name is not empty")

    regions = _list(
        transport.json(("flyctl", "platform", "regions", "--json"), timeout=CREATE_TIMEOUT_SECONDS),
        "region",
    )
    region = next(
        (
            item
            for item in regions
            if isinstance(item, dict) and item.get("code") == invocation.region
        ),
        None,
    )
    if (
        region is None
        or region.get("deprecated") is not False
        or not isinstance(region.get("capacity"), int)
        or region["capacity"] < 1
    ):
        raise QualificationError("authorization_refused", "fixed region is not currently admitted")

    sizes = transport.json(
        ("flyctl", "platform", "vm-sizes", "--json"), timeout=CREATE_TIMEOUT_SECONDS
    )
    if not isinstance(sizes, dict):
        raise QualificationError("provision_failed", "Machine size inventory is malformed")
    performance = [
        (name, value)
        for name, value in sizes.items()
        if isinstance(value, dict)
        and value.get("cpu_kind") == "performance"
        and isinstance(value.get("cpus"), int)
        and isinstance(value.get("memory_mb"), int)
    ]
    if not performance:
        raise QualificationError("authorization_refused", "no performance preset is available")
    smallest = min(performance, key=lambda item: (item[1]["memory_mb"], item[1]["cpus"], item[0]))
    if smallest[0] != invocation.machine_class:
        raise QualificationError(
            "authorization_refused", "requested Machine is not the smallest performance preset"
        )
    return LiveMachineSize(
        name=smallest[0],
        cpus=smallest[1]["cpus"],
        memory_mb=smallest[1]["memory_mb"],
        baseline_apps=baseline_apps,
    )


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    """Persist a closed artifact without exposing a partially written document."""
    path.parent.mkdir(parents=False, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        Path(temporary).replace(path)
    finally:
        with suppress(FileNotFoundError):
            Path(temporary).unlink()


def _save_ledger(path: Path, ledger: ResourceLedger) -> None:
    _atomic_json(
        path,
        {
            "schema": ledger.schema,
            "app_owned": ledger.app_owned,
            "volume_id": ledger.volume_id,
            "machine_id": ledger.machine_id,
            "image_digest": ledger.image_digest,
            "secret_names": ledger.secret_names,
            "token_material_present": ledger.token_material_present,
        },
    )


def _single_volume_id(value: Any) -> str:
    if isinstance(value, list) and len(value) == 1:
        value = value[0]
    volume_id = value.get("id") if isinstance(value, dict) else None
    if not isinstance(volume_id, str) or not VOLUME_ID.fullmatch(volume_id):
        raise QualificationError("provision_failed", "created volume identity is malformed")
    return volume_id


def _machine_id_for_name(value: Any, name: str) -> str:
    machines = _list(value, "Machine")
    matches = [item for item in machines if isinstance(item, dict) and item.get("name") == name]
    if len(matches) != 1:
        raise QualificationError("provision_failed", "created Machine identity is unavailable")
    machine_id = matches[0].get("id")
    if not isinstance(machine_id, str) or not MACHINE_ID.fullmatch(machine_id):
        raise QualificationError("provision_failed", "created Machine identity is malformed")
    return machine_id


def wait_for_app_readiness(
    transport: Transport,
    invocation: TinyQualificationInvocation,
    *,
    timeout_seconds: float = APP_READINESS_TIMEOUT_SECONDS,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> None:
    """Wait until the new app is usable through Fly's Machines authority."""
    clock = clock or time.monotonic
    sleeper = sleeper or time.sleep
    deadline = clock() + timeout_seconds
    backoff = APP_READINESS_INITIAL_BACKOFF_SECONDS
    command = ("flyctl", "machine", "list", "--app", invocation.app, "--json")
    while True:
        remaining = deadline - clock()
        if remaining <= 0:
            raise QualificationError(
                "readiness_timeout", "created app did not become ready for remote build"
            )
        if remaining < 1:
            raise QualificationError(
                "readiness_timeout", "created app did not become ready for remote build"
            )
        try:
            machines = _list(
                transport.json(
                    command,
                    timeout=min(APP_READINESS_PROBE_TIMEOUT_SECONDS, int(remaining)),
                ),
                "Machine",
            )
        except (subprocess.SubprocessError, OSError):
            machines = None
        if machines is not None:
            if clock() >= deadline:
                raise QualificationError(
                    "readiness_timeout", "created app did not become ready for remote build"
                )
            if machines:
                raise QualificationError(
                    "provision_failed", "new app is not empty at readiness admission"
                )
            return
        sleep_for = min(backoff, max(0.0, deadline - clock()))
        if sleep_for <= 0:
            raise QualificationError(
                "readiness_timeout", "created app did not become ready for remote build"
            )
        sleeper(sleep_for)
        backoff = min(backoff * 2, APP_READINESS_MAX_BACKOFF_SECONDS)


def verify_machine_state(
    value: Any,
    invocation: TinyQualificationInvocation,
    live_size: LiveMachineSize,
    *,
    image: str,
    volume_id: str,
) -> None:
    """Verify Fly applied the disposable private Machine contract."""
    if not isinstance(value, dict):
        raise QualificationError("provision_failed", "Machine state is malformed")
    config = value.get("config")
    image_ref = value.get("image_ref")
    if not isinstance(config, dict) or not isinstance(image_ref, dict):
        raise QualificationError("provision_failed", "Machine state is incomplete")
    guest = config.get("guest")
    mounts = config.get("mounts")
    restart = config.get("restart")
    if (
        value.get("region") != invocation.region
        or config.get("auto_destroy") is not True
        or not isinstance(restart, dict)
        or restart.get("policy") != "no"
        or config.get("services") not in (None, [])
        or not isinstance(guest, dict)
        or guest.get("cpu_kind") != "performance"
        or guest.get("cpus") != live_size.cpus
        or guest.get("memory_mb") != live_size.memory_mb
        or not isinstance(mounts, list)
        or len(mounts) != 1
        or not isinstance(mounts[0], dict)
        or mounts[0].get("path") != "/work"
        or mounts[0].get("volume") != volume_id
        or image_ref.get("digest") != image.rsplit("@", 1)[1]
    ):
        raise QualificationError("provision_failed", "Machine state differs from admitted plan")


def _machine_command(
    invocation: TinyQualificationInvocation, *, image: str, volume_id: str
) -> tuple[str, ...]:
    return (
        "flyctl",
        "machine",
        "run",
        image,
        "--app",
        invocation.app,
        "--name",
        invocation.machine_name,
        "--region",
        invocation.region,
        "--vm-size",
        invocation.machine_class,
        "--volume",
        f"{volume_id}:/work",
        "--env",
        f"GF_FLY_QUALIFICATION_GIT_SHA={invocation.commit}",
        "--env",
        f"GF_FLY_QUALIFICATION_IMAGE_DIGEST={image.rsplit('@', 1)[1]}",
        "--env",
        f"GF_FLY_QUALIFICATION_REGION={invocation.region}",
        "--restart",
        "no",
        "--autostop",
        "off",
        "--autostart=false",
        "--rootfs-persist",
        "never",
        "--skip-dns-registration",
        "--rm",
        "--detach",
    )


def _validate_evidence(path: Path, invocation: TinyQualificationInvocation, image: str) -> None:
    spec = importlib.util.spec_from_file_location("fly_evidence_validator", VALIDATOR)
    if spec is None or spec.loader is None:
        raise QualificationError("qualification_failed", "evidence validator is unavailable")
    validator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(validator)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        validator.validate(
            value,
            sha=invocation.commit,
            digest=image.rsplit("@", 1)[1],
            region=invocation.region,
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise QualificationError(
            "qualification_failed", "qualification evidence is invalid"
        ) from error
    if value.get("result") != "qualified" or value.get("full_run_authorized") is not False:
        raise QualificationError("qualification_failed", "tiny environment was not qualified")


def _retrieve_evidence(
    transport: Transport,
    invocation: TinyQualificationInvocation,
    machine_id: str,
    image: str,
    destination: Path,
) -> None:
    deadline = time.monotonic() + RETRIEVAL_TIMEOUT_SECONDS
    with tempfile.TemporaryDirectory(prefix="graphforge-fly-tiny-") as directory:
        local = Path(directory) / "evidence.json"
        while time.monotonic() < deadline:
            result = transport.run(
                (
                    "flyctl",
                    "ssh",
                    "sftp",
                    "get",
                    SMOKE_EVIDENCE,
                    str(local),
                    "--app",
                    invocation.app,
                    "--machine",
                    machine_id,
                    "--quiet",
                ),
                timeout=SFTP_TIMEOUT_SECONDS,
                check=False,
            )
            if result.returncode == 0 and local.is_file():
                break
            time.sleep(2)
        else:
            raise QualificationError(
                "retrieval_failed", "qualification evidence retrieval timed out"
            )
        _validate_evidence(local, invocation, image)
        destination.write_bytes(local.read_bytes())


def _cleanup(
    transport: Transport,
    invocation: TinyQualificationInvocation,
    ledger: ResourceLedger,
    ledger_path: Path,
    baseline_apps: frozenset[str],
) -> None:
    """Best-effort child-first cleanup followed by independent empty inventory."""
    failures = False

    def best_effort(argv: tuple[str, ...]) -> None:
        nonlocal failures
        try:
            transport.run(argv, timeout=CREATE_TIMEOUT_SECONDS, check=False)
        except (subprocess.SubprocessError, OSError):
            failures = True

    try:
        current_apps = _list(
            transport.json(("flyctl", "apps", "list", "--json"), timeout=CREATE_TIMEOUT_SECONDS),
            "app",
        )
        current_names = {
            item.get("Name") or item.get("name") for item in current_apps if isinstance(item, dict)
        }
    except (QualificationError, subprocess.SubprocessError, OSError):
        current_names = set()
        failures = True
    app_exists = invocation.app in current_names

    if app_exists:
        # The app was proven absent before creation, so every child in this
        # owned app belongs to this attempt even if a crash preceded ID capture.
        try:
            machines = _list(
                transport.json(
                    ("flyctl", "machine", "list", "--app", invocation.app, "--json"),
                    timeout=CREATE_TIMEOUT_SECONDS,
                ),
                "Machine",
            )
            volumes = _list(
                transport.json(
                    ("flyctl", "volumes", "list", "--app", invocation.app, "--json"),
                    timeout=CREATE_TIMEOUT_SECONDS,
                ),
                "volume",
            )
            secrets = _list(
                transport.json(
                    ("flyctl", "secrets", "list", "--app", invocation.app, "--json"),
                    timeout=CREATE_TIMEOUT_SECONDS,
                ),
                "secret",
            )
        except (QualificationError, subprocess.SubprocessError, OSError):
            machines, volumes, secrets = [], [], []
            failures = True

        machine_ids = {ledger.machine_id} if ledger.machine_id else set()
        for item in machines:
            machine_id = item.get("id") if isinstance(item, dict) else None
            if isinstance(machine_id, str) and MACHINE_ID.fullmatch(machine_id):
                machine_ids.add(machine_id)
            else:
                failures = True
        volume_ids = {ledger.volume_id} if ledger.volume_id else set()
        for item in volumes:
            volume_id = item.get("id") if isinstance(item, dict) else None
            if isinstance(volume_id, str) and VOLUME_ID.fullmatch(volume_id):
                volume_ids.add(volume_id)
            else:
                failures = True
        secret_names = set(ledger.secret_names)
        for item in secrets:
            secret = (item.get("Name") or item.get("name")) if isinstance(item, dict) else None
            if isinstance(secret, str) and SAFE_NAME.fullmatch(secret):
                secret_names.add(secret)
            else:
                failures = True

        for machine_id in sorted(machine_ids):
            best_effort(
                (
                    "flyctl",
                    "machine",
                    "destroy",
                    "--app",
                    invocation.app,
                    "--force",
                    machine_id,
                ),
            )
        for volume_id in sorted(volume_ids):
            best_effort(
                (
                    "flyctl",
                    "volumes",
                    "destroy",
                    "--app",
                    invocation.app,
                    "--yes",
                    volume_id,
                ),
            )
        for secret in sorted(secret_names):
            best_effort(("flyctl", "secrets", "unset", secret, "--app", invocation.app, "--yes"))

        # A successful destroy request may leave a Machine briefly in the
        # provider's destroying state. Poll only this bounded convergence
        # window; the last inventory snapshot remains authoritative.
        children_empty = False
        for attempt in range(TEARDOWN_POLL_ATTEMPTS):
            try:
                machines = _list(
                    transport.json(
                        ("flyctl", "machine", "list", "--app", invocation.app, "--json"),
                        timeout=CREATE_TIMEOUT_SECONDS,
                    ),
                    "Machine",
                )
                volumes = _list(
                    transport.json(
                        ("flyctl", "volumes", "list", "--app", invocation.app, "--json"),
                        timeout=CREATE_TIMEOUT_SECONDS,
                    ),
                    "volume",
                )
                secrets = _list(
                    transport.json(
                        ("flyctl", "secrets", "list", "--app", invocation.app, "--json"),
                        timeout=CREATE_TIMEOUT_SECONDS,
                    ),
                    "secret",
                )
                children_empty = not (machines or volumes or secrets)
            except (QualificationError, subprocess.SubprocessError, OSError):
                children_empty = False
            if children_empty:
                break
            if attempt + 1 < TEARDOWN_POLL_ATTEMPTS:
                # Reissue idempotent child deletes from the independently
                # observed inventory. In particular, a volume delete can be
                # rejected while its Machine is still transitioning away.
                for item in machines:
                    machine_id = item.get("id") if isinstance(item, dict) else None
                    if isinstance(machine_id, str) and MACHINE_ID.fullmatch(machine_id):
                        best_effort(
                            (
                                "flyctl",
                                "machine",
                                "destroy",
                                "--app",
                                invocation.app,
                                "--force",
                                machine_id,
                            )
                        )
                    else:
                        failures = True
                for item in volumes:
                    volume_id = item.get("id") if isinstance(item, dict) else None
                    if isinstance(volume_id, str) and VOLUME_ID.fullmatch(volume_id):
                        best_effort(
                            (
                                "flyctl",
                                "volumes",
                                "destroy",
                                "--app",
                                invocation.app,
                                "--yes",
                                volume_id,
                            )
                        )
                    else:
                        failures = True
                for item in secrets:
                    secret = (
                        (item.get("Name") or item.get("name")) if isinstance(item, dict) else None
                    )
                    if isinstance(secret, str) and SAFE_NAME.fullmatch(secret):
                        best_effort(
                            (
                                "flyctl",
                                "secrets",
                                "unset",
                                secret,
                                "--app",
                                invocation.app,
                                "--yes",
                            )
                        )
                    else:
                        failures = True
                time.sleep(TEARDOWN_POLL_INTERVAL_SECONDS)
        failures |= not children_empty

        if app_exists:
            best_effort(("flyctl", "apps", "destroy", invocation.app, "--yes"))

    try:
        apps = _list(
            transport.json(("flyctl", "apps", "list", "--json"), timeout=CREATE_TIMEOUT_SECONDS),
            "app",
        )
    except (QualificationError, subprocess.SubprocessError, OSError):
        apps = []
        failures = True
    else:
        final_names = {
            item.get("Name") or item.get("name") for item in apps if isinstance(item, dict)
        }
        if final_names != set(baseline_apps):
            failures = True
    if failures:
        _save_ledger(ledger_path, ledger)
        raise QualificationError("teardown_failed", "provider teardown was not independently empty")

    ledger.app_owned = False
    ledger.volume_id = None
    ledger.machine_id = None
    ledger.image_digest = None
    ledger.secret_names.clear()
    ledger.token_material_present = False
    _save_ledger(ledger_path, ledger)


def execute(
    invocation: TinyQualificationInvocation,
    *,
    transport: Transport,
    root: Path,
    ledger_path: Path,
    evidence_out: Path,
    dry_run: bool,
) -> dict[str, Any]:
    """Run the bounded one-app qualification and return sanitized status."""
    validate_invocation(invocation)
    check_source(root, invocation.commit)
    live_size = verify_live_capacity(transport, invocation)
    if dry_run:
        return {
            "schema": "graphforge-fly-tiny-plan/1",
            "status": "admitted",
            "machine_class": invocation.machine_class,
            "volume_gib": invocation.volume_gib,
            "full_run_authorized": False,
        }

    ledger = ResourceLedger()
    _save_ledger(ledger_path, ledger)
    failure: QualificationError | None = None
    failure_kind = "provision_failed"
    try:
        transport.run(
            (
                "flyctl",
                "apps",
                "create",
                invocation.app,
                "--org",
                invocation.organization,
                "--json",
                "--yes",
            ),
            timeout=CREATE_TIMEOUT_SECONDS,
        )
        ledger.app_owned = True
        _save_ledger(ledger_path, ledger)

        wait_for_app_readiness(
            transport,
            invocation,
            timeout_seconds=APP_READINESS_TIMEOUT_SECONDS,
        )

        failure_kind = "build_failed"
        build = remote_build_command(
            app=invocation.app,
            source=root,
            config=FLY_BUILD_CONFIG,
            dockerfile=DOCKERFILE,
            commit=invocation.commit,
        )
        transport.run(build.argv, timeout=BUILD_TIMEOUT_SECONDS)
        image = transport.resolve_image(
            invocation.app, invocation.commit, timeout=CREATE_TIMEOUT_SECONDS
        )
        ledger.image_digest = image
        _save_ledger(ledger_path, ledger)

        failure_kind = "provision_failed"
        volume = transport.json(
            (
                "flyctl",
                "volumes",
                "create",
                invocation.volume_name,
                "--app",
                invocation.app,
                "--region",
                invocation.region,
                "--size",
                str(invocation.volume_gib),
                "--count",
                "1",
                "--scheduled-snapshots=false",
                "--json",
                "--yes",
            ),
            timeout=CREATE_TIMEOUT_SECONDS,
        )
        ledger.volume_id = _single_volume_id(volume)
        _save_ledger(ledger_path, ledger)

        transport.run(
            _machine_command(invocation, image=image, volume_id=ledger.volume_id),
            timeout=CREATE_TIMEOUT_SECONDS,
        )
        ledger.machine_id = _machine_id_for_name(
            transport.json(
                ("flyctl", "machine", "list", "--app", invocation.app, "--json"),
                timeout=CREATE_TIMEOUT_SECONDS,
            ),
            invocation.machine_name,
        )
        _save_ledger(ledger_path, ledger)
        verify_machine_state(
            transport.machine_state(
                invocation.app, ledger.machine_id, timeout=CREATE_TIMEOUT_SECONDS
            ),
            invocation,
            live_size,
            image=image,
            volume_id=ledger.volume_id,
        )

        failure_kind = "retrieval_failed"
        _retrieve_evidence(transport, invocation, ledger.machine_id, image, evidence_out)
        failure_kind = "qualification_failed"
        transport.run(
            (
                "flyctl",
                "machine",
                "exec",
                ledger.machine_id,
                "--app",
                invocation.app,
                "--timeout",
                "30",
                SMOKE_ACK,
            ),
            timeout=60,
        )
    except QualificationError as error:
        failure = error
    except (AdapterError, subprocess.SubprocessError, OSError, ValueError) as error:
        failure = QualificationError(failure_kind, "provider operation failed")
        failure.__cause__ = error
    finally:
        try:
            _cleanup(transport, invocation, ledger, ledger_path, live_size.baseline_apps)
        except QualificationError as cleanup_error:
            failure = cleanup_error

    if failure is not None:
        return sanitized_failure(failure.failure)
    return {
        "schema": "graphforge-fly-adapter-result/1",
        "status": "passed",
        "failure": None,
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--expected-sha", required=True)
    result.add_argument("--org", required=True)
    result.add_argument("--app", required=True)
    result.add_argument("--region", required=True)
    result.add_argument("--volume-name", required=True)
    result.add_argument("--machine-name", required=True)
    result.add_argument("--prerequisite-955", choices=("merged",), required=True)
    result.add_argument("--prerequisite-956", choices=("merged",), required=True)
    result.add_argument("--prerequisite-957", choices=("merged",), required=True)
    result.add_argument("--machine-class", default="performance-1x")
    result.add_argument("--volume-gib", type=int, default=10)
    result.add_argument("--ledger", type=Path, required=True)
    result.add_argument("--evidence-out", type=Path, required=True)
    result.add_argument("--result-out", type=Path, required=True)
    result.add_argument("--execute", action="store_true")
    result.add_argument("--confirm-disposable", action="store_true")
    return result


def main() -> int:
    args = parser().parse_args()
    if args.execute and not args.confirm_disposable:
        result = sanitized_failure("authorization_refused")
        _atomic_json(args.result_out, result)
        return 1
    invocation = TinyQualificationInvocation(
        commit=args.expected_sha,
        organization=args.org,
        app=args.app,
        region=args.region,
        volume_name=args.volume_name,
        machine_name=args.machine_name,
        prerequisites={
            955: args.prerequisite_955,
            956: args.prerequisite_956,
            957: args.prerequisite_957,
        },
        machine_class=args.machine_class,
        volume_gib=args.volume_gib,
    )
    try:
        result = execute(
            invocation,
            transport=FlyctlTransport(),
            root=ROOT,
            ledger_path=args.ledger,
            evidence_out=args.evidence_out,
            dry_run=not args.execute,
        )
    except (AdapterError, QualificationError, subprocess.SubprocessError, OSError):
        result = sanitized_failure("authorization_refused")
    _atomic_json(args.result_out, result)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] in {"admitted", "passed"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
