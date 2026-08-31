"""Import-only Fly transport for progressive provider attempts.

All provider I/O is delegated to an injected, shell-free boundary.  This
module deliberately has no CLI and cannot open Pulumi ESC or start a live
qualification by itself.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import re
import shlex
import subprocess
import time
from typing import Any, Protocol
import urllib.error
import urllib.request

from graphforge_bench.progressive_provider_attempt import (
    AttemptError,
    AttemptInvocation,
    ProvisionedAttempt,
    SpendAuthorization,
)

PROVIDER_RUNGS = (20, 22, 24, 25, 26)
IMAGE = re.compile(r"^registry\.fly\.io/[a-z0-9][a-z0-9._/-]*@sha256:[0-9a-f]{64}$")
OBSERVED_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
APP = re.compile(r"^gf-progressive-[0-9a-f]{32}$")
MACHINE_ID = re.compile(r"^[0-9a-f]{14}$")
VOLUME_ID = re.compile(r"^vol_[a-z0-9]+$")
CREATE_TIMEOUT_SECONDS = 300
TRANSFER_TIMEOUT_SECONDS = 300
TEARDOWN_TIMEOUT_SECONDS = 300
TEARDOWN_POLL_ATTEMPTS = 6
TEARDOWN_POLL_INTERVAL_SECONDS = 1
READINESS_CAP_SECONDS = 300
READINESS_PROBE_TIMEOUT_SECONDS = 5
READINESS_INITIAL_BACKOFF_SECONDS = 0.25
READINESS_MAX_BACKOFF_SECONDS = 2.0
# Documented Fly Machine states still converging toward admission.
NONTERMINAL_MACHINE_STATES = frozenset({"created", "starting", "replacing"})
# Documented Fly Machine states that can never converge to a healthy worker.
TERMINAL_MACHINE_STATES = frozenset(
    {"stopped", "stopping", "destroyed", "destroying", "suspended", "suspending"}
)
REMOTE_OUTPUT_DIR = "/work/evidence"
API_ROOT = "https://api.machines.dev"
PROVIDER_ENVIRONMENT = frozenset(
    {"FLY_API_TOKEN", "HOME", "LANG", "LC_ALL", "PATH", "XDG_CONFIG_HOME"}
)
ALLOWED_FLYCTL_COMMANDS = frozenset(
    {
        ("apps", "create"),
        ("apps", "destroy"),
        ("apps", "list"),
        ("machine", "destroy"),
        ("machine", "exec"),
        ("machine", "list"),
        ("machine", "run"),
        ("secrets", "list"),
        ("secrets", "unset"),
        ("sftp", "get"),
        ("sftp", "put"),
        ("volumes", "create"),
        ("volumes", "destroy"),
        ("volumes", "list"),
    }
)
FORBIDDEN_FLYCTL_ARGUMENTS = frozenset(
    {
        "--access-token",
        "--build-depot",
        "--build-nixpacks",
        "--config",
        "--debug",
        "--dockerfile",
        "--verbose",
        "-c",
        "-t",
    }
)
SECRET_NAME = re.compile(r"^[A-Z_][A-Z0-9_]{0,127}$")
MACHINE_MEMORY_MB = {
    **{f"shared-cpu-{cpus}x": cpus * 256 for cpus in (1, 2, 4, 6, 8)},
    **{f"performance-{cpus}x": cpus * 2048 for cpus in range(1, 17) if cpus % 2 == 0 or cpus == 1},
}


class FlyTransportError(RuntimeError):
    """A closed provider failure that contains no provider output."""


class FlyBoundary(Protocol):
    """Injectable boundary for argv execution and authoritative Machine state."""

    def run(
        self, argv: tuple[str, ...], *, timeout: int, check: bool = True
    ) -> subprocess.CompletedProcess[str]: ...

    def json(self, argv: tuple[str, ...], *, timeout: int) -> Any: ...

    def api_json(self, path: str, *, timeout: int) -> Any: ...

    def machine_state(self, app: str, machine_id: str, *, timeout: int) -> Any: ...


class FlyctlMachineBoundary:
    """Concrete shell-free flyctl and Machines API boundary.

    ``environment`` is expected to be the minimal mapping returned by the ESC
    capsule.  It is deliberately retained as a mapping rather than copied so
    closing the capsule also clears the token visible to this boundary.
    """

    def __init__(
        self,
        environment: Mapping[str, str],
        owner_app: str,
        *,
        cwd: Path | None = None,
        urlopen: Callable[..., Any] | None = None,
    ) -> None:
        if set(environment) != PROVIDER_ENVIRONMENT:
            raise FlyTransportError("provider environment is not isolated")
        if APP.fullmatch(owner_app) is None:
            raise FlyTransportError("provider app ownership is malformed")
        self._environment = environment
        self._owner_app = owner_app
        self._cwd = cwd or Path.cwd()
        self._urlopen = urlopen or urllib.request.urlopen

    def _env(self) -> dict[str, str]:
        values = {name: self._environment[name] for name in PROVIDER_ENVIRONMENT}
        if not values["FLY_API_TOKEN"]:
            raise FlyTransportError("provider credential is unavailable")
        return values

    def run(
        self, argv: tuple[str, ...], *, timeout: int, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        if (
            len(argv) < 3
            or argv[0] != "flyctl"
            or argv[1:3] not in ALLOWED_FLYCTL_COMMANDS
            or self._has_forbidden_argument(argv[3:])
            or not self._targets_owner(argv)
        ):
            raise FlyTransportError("provider command is not allowed")
        try:
            completed = subprocess.run(
                argv,
                cwd=self._cwd,
                env=self._env(),
                shell=False,
                check=False,
                text=True,
                capture_output=True,
                timeout=timeout,
            )
        except (OSError, subprocess.SubprocessError):
            raise FlyTransportError("provider command failed") from None
        if check and completed.returncode != 0:
            raise FlyTransportError("provider command failed")
        return completed

    @staticmethod
    def _has_forbidden_argument(arguments: Sequence[str]) -> bool:
        forbidden_long = {item for item in FORBIDDEN_FLYCTL_ARGUMENTS if item.startswith("--")}
        forbidden_short = {
            item for item in FORBIDDEN_FLYCTL_ARGUMENTS if item.startswith("-") and len(item) == 2
        }
        return any(
            argument in FORBIDDEN_FLYCTL_ARGUMENTS
            or any(argument.startswith(f"{flag}=") for flag in forbidden_long)
            or any(argument.startswith(flag) for flag in forbidden_short)
            for argument in arguments
        )

    def _targets_owner(self, argv: tuple[str, ...]) -> bool:
        command = argv[1:3]
        arguments = argv[3:]
        if command == ("apps", "list"):
            return arguments == ("--json",)
        if command in {("apps", "create"), ("apps", "destroy")}:
            if not arguments or arguments[0] != self._owner_app:
                return False
        elif "--app" not in arguments and "-a" not in arguments:
            return False
        for index, argument in enumerate(arguments):
            if argument in {"--app", "-a"}:
                if index + 1 >= len(arguments) or arguments[index + 1] != self._owner_app:
                    return False
            elif argument.startswith("--app=") or (argument.startswith("-a") and argument != "-a"):
                return False
        return True

    def json(self, argv: tuple[str, ...], *, timeout: int) -> Any:
        try:
            return json.loads(self.run(argv, timeout=timeout).stdout)
        except (json.JSONDecodeError, UnicodeError):
            raise FlyTransportError("provider JSON is malformed") from None

    def api_json(self, path: str, *, timeout: int) -> Any:
        allowed_paths = {
            f"/v1/apps/{self._owner_app}",
        }
        machine_path = re.fullmatch(
            rf"/v1/apps/{re.escape(self._owner_app)}/machines/([0-9a-f]{{14}})", path
        )
        volume_path = re.fullmatch(
            rf"/v1/apps/{re.escape(self._owner_app)}/volumes/(vol_[a-z0-9]+)", path
        )
        if path not in allowed_paths and machine_path is None and volume_path is None:
            raise FlyTransportError("provider API path is not allowed")
        request = urllib.request.Request(
            API_ROOT + path,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._env()['FLY_API_TOKEN']}",
            },
        )
        try:
            with self._urlopen(request, timeout=timeout) as response:
                payload = response.read(1_048_577)
            if len(payload) > 1_048_576:
                raise FlyTransportError("provider JSON is malformed")
            return json.loads(payload)
        except FlyTransportError:
            raise
        except (OSError, urllib.error.URLError, json.JSONDecodeError, UnicodeError):
            raise FlyTransportError("provider API request failed") from None

    def machine_state(self, app: str, machine_id: str, *, timeout: int) -> Any:
        if APP.fullmatch(app) is None or MACHINE_ID.fullmatch(machine_id) is None:
            raise FlyTransportError("provider Machine identity is malformed")
        return self.api_json(f"/v1/apps/{app}/machines/{machine_id}", timeout=timeout)


def _remaining_seconds(
    deadline: datetime,
    clock: Callable[[], datetime],
    *,
    maximum: int,
) -> int:
    now = clock()
    if deadline.tzinfo is None or now.tzinfo is None:
        raise FlyTransportError("provider deadline is not timezone-aware")
    remaining = (deadline.astimezone(timezone.utc) - now.astimezone(timezone.utc)).total_seconds()
    if remaining <= 0:
        raise FlyTransportError("provider deadline expired")
    whole_seconds = int(remaining)
    if whole_seconds < 1:
        raise FlyTransportError("provider deadline expired")
    return min(maximum, whole_seconds)


def _list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise FlyTransportError(f"provider {label} inventory is malformed")
    return value


def _app_names(value: Any) -> set[str]:
    apps = _list(value, "app")
    names: set[str] = set()
    for item in apps:
        name = item.get("Name") or item.get("name") if isinstance(item, Mapping) else None
        if not isinstance(name, str):
            raise FlyTransportError("provider app inventory is malformed")
        names.add(name)
    if len(names) != len(apps):
        raise FlyTransportError("provider app inventory is malformed")
    return names


def _single_volume_id(value: Any) -> str:
    if isinstance(value, list) and len(value) == 1:
        value = value[0]
    volume_id = value.get("id") if isinstance(value, Mapping) else None
    if not isinstance(volume_id, str) or VOLUME_ID.fullmatch(volume_id) is None:
        raise FlyTransportError("created volume identity is malformed")
    return volume_id


def _machine_id(value: Any, name: str) -> str:
    machines = _list(value, "Machine")
    if len(machines) != 1 or not isinstance(machines[0], Mapping):
        raise FlyTransportError("provider Machine inventory is unexpected")
    matches = [item for item in machines if isinstance(item, Mapping) and item.get("name") == name]
    if len(matches) != 1:
        raise FlyTransportError("created Machine identity is unavailable")
    machine_id = matches[0].get("id")
    if not isinstance(machine_id, str) or MACHINE_ID.fullmatch(machine_id) is None:
        raise FlyTransportError("created Machine identity is malformed")
    return machine_id


def _volume_items(value: Any) -> list[Mapping[str, Any]]:
    volumes = _list(value, "volume")
    if any(not isinstance(item, Mapping) for item in volumes):
        raise FlyTransportError("provider volume inventory is malformed")
    return volumes


def _secret_names(value: Any) -> list[str]:
    secrets = _list(value, "secret")
    names: list[str] = []
    for item in secrets:
        name = item.get("Name") or item.get("name") if isinstance(item, Mapping) else None
        if not isinstance(name, str) or SECRET_NAME.fullmatch(name) is None:
            raise FlyTransportError("provider secret inventory is malformed")
        names.append(name)
    if len(set(names)) != len(names):
        raise FlyTransportError("provider secret inventory is malformed")
    return names


def _app_identity(value: Any, authorization: SpendAuthorization) -> None:
    organization = value.get("organization") if isinstance(value, Mapping) else None
    if (
        not isinstance(organization, Mapping)
        or value.get("name") != authorization.app
        or organization.get("slug") != authorization.organization
    ):
        raise FlyTransportError("provider app identity differs from authorization")


def _volume_identity(
    value: Any,
    authorization: SpendAuthorization,
    *,
    volume_id: str,
    machine_id: str,
) -> None:
    if not isinstance(value, Mapping):
        raise FlyTransportError("provider volume state is malformed")
    size_gb = value.get("size_gb", value.get("size_gb_total"))
    if (
        value.get("id") != volume_id
        or value.get("name") != f"{authorization.app}-data"
        or value.get("region") != authorization.region
        or size_gb != authorization.volume_gib
        or value.get("auto_backup_enabled") is not False
        or value.get("attached_machine_id") != machine_id
    ):
        raise FlyTransportError("provider volume state differs from authorization")


def _observed_image(
    value: Any,
    authorization: SpendAuthorization,
    *,
    volume_id: str,
    machine_id: str,
) -> str:
    if not isinstance(value, Mapping):
        raise FlyTransportError("Machine state is malformed")
    config = value.get("config")
    image_ref = value.get("image_ref")
    if not isinstance(config, Mapping) or not isinstance(image_ref, Mapping):
        raise FlyTransportError("Machine state is incomplete")
    guest = config.get("guest")
    mounts = config.get("mounts")
    restart = config.get("restart")
    expected_kind, cpus_text = authorization.machine_class.rsplit("-", 1)
    cpu_kind = "shared" if expected_kind == "shared-cpu" else "performance"
    digest = image_ref.get("digest")
    repository = authorization.image_digest.removeprefix("registry.fly.io/").rsplit("@", 1)[0]
    metadata = config.get("metadata")
    init = config.get("init")
    expected_memory = MACHINE_MEMORY_MB.get(authorization.machine_class)
    lifetime = str(authorization.maximum_machine_seconds)
    if (
        value.get("id") != machine_id
        or value.get("name") != f"{authorization.app}-worker"
        or value.get("state") != "started"
        or value.get("region") != authorization.region
        or not isinstance(value.get("private_ip"), str)
        or not value["private_ip"].startswith("fdaa:")
        or config.get("image") != authorization.image_digest
        or config.get("auto_destroy") is not True
        or not isinstance(restart, Mapping)
        or restart.get("policy") != "no"
        or config.get("services") not in (None, [])
        or not isinstance(init, Mapping)
        or init.get("entrypoint") not in (["/bin/sleep"], "/bin/sleep")
        or init.get("cmd") not in ([lifetime], lifetime)
        or not isinstance(guest, Mapping)
        or guest.get("cpu_kind") != cpu_kind
        or guest.get("cpus") != int(cpus_text.removesuffix("x"))
        or guest.get("memory_mb") != expected_memory
        or not isinstance(mounts, list)
        or len(mounts) != 1
        or not isinstance(mounts[0], Mapping)
        or mounts[0].get("path") != "/work"
        or mounts[0].get("volume") != volume_id
        or not isinstance(digest, str)
        or OBSERVED_DIGEST.fullmatch(digest) is None
        or image_ref.get("registry") != "registry.fly.io"
        or image_ref.get("repository") != repository
        or not isinstance(metadata, Mapping)
        or metadata.get("graphforge_attempt_nonce") != authorization.attempt_nonce
        or metadata.get("graphforge_commit") != authorization.commit
        or metadata.get("graphforge_owner") != authorization.teardown_owner
        or metadata.get("graphforge_machine_class") != authorization.machine_class
    ):
        raise FlyTransportError("Machine state differs from authorized resources")
    return f"registry.fly.io/{repository}@{digest}"


def _readiness_machine(value: Any, *, machine_name: str) -> tuple[str, str] | None:
    """Classify Machine inventory during readiness convergence.

    Returns ``None`` when the expected Machine is still absent. Returns
    ``(machine_id, state)`` when exactly one expected Machine is present in a
    documented nonterminal or ``started`` state. Raises on malformed, extra,
    wrong-name, unknown, or terminal inventory.
    """
    machines = _list(value, "Machine")
    if not machines:
        return None
    if len(machines) != 1 or not isinstance(machines[0], Mapping):
        raise FlyTransportError("provider Machine inventory is unexpected")
    machine = machines[0]
    if machine.get("name") != machine_name:
        raise FlyTransportError("created Machine identity is unavailable")
    machine_id = machine.get("id")
    state = machine.get("state")
    if not isinstance(machine_id, str) or MACHINE_ID.fullmatch(machine_id) is None:
        raise FlyTransportError("created Machine identity is malformed")
    if not isinstance(state, str):
        raise FlyTransportError("provider Machine state is malformed")
    if state in TERMINAL_MACHINE_STATES:
        raise FlyTransportError("provider Machine entered a terminal state")
    if state != "started" and state not in NONTERMINAL_MACHINE_STATES:
        raise FlyTransportError("provider Machine state is unexpected")
    return machine_id, state


def wait_for_machine_readiness(
    boundary: FlyBoundary,
    authorization: SpendAuthorization,
    *,
    machine_name: str,
    deadline: datetime,
    clock: Callable[[], datetime],
    sleeper: Callable[[float], None],
    admit: Callable[[str], str],
) -> tuple[str, str]:
    """Poll until the authorized Machine admits under full identity validation.

    Probe failures may be retried. Malformed, extra, wrong-name, terminal, or
    identity-drift inventory fails immediately. Mutation is never retried.
    """
    readiness_deadline = min(deadline, clock() + timedelta(seconds=READINESS_CAP_SECONDS))
    backoff = READINESS_INITIAL_BACKOFF_SECONDS
    while True:
        remaining = (readiness_deadline - clock()).total_seconds()
        if remaining <= 0 or remaining < 1:
            raise AttemptError("readiness_timeout", "created Machine did not become ready")
        probe_timeout = min(READINESS_PROBE_TIMEOUT_SECONDS, int(remaining))
        try:
            machines = boundary.json(
                ("flyctl", "machine", "list", "--app", authorization.app, "--json"),
                timeout=probe_timeout,
            )
        except (OSError, subprocess.SubprocessError, FlyTransportError):
            machines = None
        else:
            observed = _readiness_machine(machines, machine_name=machine_name)
            if observed is not None:
                machine_id, state = observed
                if state == "started":
                    return machine_id, admit(machine_id)
        sleep_for = min(backoff, max(0.0, (readiness_deadline - clock()).total_seconds()))
        if sleep_for <= 0:
            raise AttemptError("readiness_timeout", "created Machine did not become ready")
        sleeper(sleep_for)
        backoff = min(backoff * 2, READINESS_MAX_BACKOFF_SECONDS)


class FlyProviderTransport:
    """One-app, one-volume, one-Machine implementation of ProviderTransport."""

    def __init__(
        self,
        boundary: FlyBoundary,
        *,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._boundary = boundary
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sleeper = sleeper or time.sleep
        self._app: str | None = None
        self._machine_id: str | None = None
        self._volume_id: str | None = None
        self._authorization: SpendAuthorization | None = None
        self._uploaded_rung: int | None = None
        self._executed_rung: int | None = None
        self._retrieved_results: set[int] = set()

    def _timeout(self, deadline: datetime, maximum: int) -> int:
        return _remaining_seconds(deadline, self._clock, maximum=maximum)

    def _owned_machine(self) -> tuple[str, str]:
        if self._app is None or self._machine_id is None:
            raise FlyTransportError("provider Machine ownership is unavailable")
        return self._app, self._machine_id

    def _validate_owned_state(self, deadline: datetime) -> str:
        app, machine_id = self._owned_machine()
        authorization = self._authorization
        volume_id = self._volume_id
        if authorization is None or volume_id is None:
            raise FlyTransportError("provider ownership is unavailable")
        _app_identity(
            self._boundary.api_json(
                f"/v1/apps/{app}", timeout=self._timeout(deadline, CREATE_TIMEOUT_SECONDS)
            ),
            authorization,
        )
        machines = self._boundary.json(
            ("flyctl", "machine", "list", "--app", app, "--json"),
            timeout=self._timeout(deadline, CREATE_TIMEOUT_SECONDS),
        )
        if _machine_id(machines, f"{app}-worker") != machine_id:
            raise FlyTransportError("provider Machine inventory changed")
        volumes = _volume_items(
            self._boundary.json(
                ("flyctl", "volumes", "list", "--app", app, "--json"),
                timeout=self._timeout(deadline, CREATE_TIMEOUT_SECONDS),
            )
        )
        if len(volumes) != 1 or volumes[0].get("id") != volume_id:
            raise FlyTransportError("provider volume inventory is unexpected")
        if _secret_names(
            self._boundary.json(
                ("flyctl", "secrets", "list", "--app", app, "--json"),
                timeout=self._timeout(deadline, CREATE_TIMEOUT_SECONDS),
            )
        ):
            raise FlyTransportError("provider secret inventory is unexpected")
        state = self._boundary.machine_state(
            app, machine_id, timeout=self._timeout(deadline, CREATE_TIMEOUT_SECONDS)
        )
        observed = _observed_image(state, authorization, volume_id=volume_id, machine_id=machine_id)
        _volume_identity(
            self._boundary.api_json(
                f"/v1/apps/{app}/volumes/{volume_id}",
                timeout=self._timeout(deadline, CREATE_TIMEOUT_SECONDS),
            ),
            authorization,
            volume_id=volume_id,
            machine_id=machine_id,
        )
        return observed

    def provision(
        self,
        invocation: AttemptInvocation,
        authorization: SpendAuthorization,
        *,
        deadline: datetime,
    ) -> ProvisionedAttempt:
        if (
            APP.fullmatch(authorization.app) is None
            or IMAGE.fullmatch(authorization.image_digest) is None
            or authorization.resource_limits
            != {"apps": 1, "volumes": 1, "machines": 1, "image_builds": 0}
        ):
            raise FlyTransportError("provider authorization is incompatible")
        self._authorization = authorization
        if authorization.app in _app_names(
            self._boundary.json(
                ("flyctl", "apps", "list", "--json"),
                timeout=self._timeout(deadline, CREATE_TIMEOUT_SECONDS),
            )
        ):
            raise FlyTransportError("authorized provider app already exists")

        self._app = authorization.app
        self._boundary.run(
            (
                "flyctl",
                "apps",
                "create",
                authorization.app,
                "--org",
                authorization.organization,
                "--json",
                "--yes",
            ),
            timeout=self._timeout(deadline, CREATE_TIMEOUT_SECONDS),
        )
        volume_name = f"{authorization.app}-data"
        volume_id = _single_volume_id(
            self._boundary.json(
                (
                    "flyctl",
                    "volumes",
                    "create",
                    volume_name,
                    "--app",
                    authorization.app,
                    "--region",
                    authorization.region,
                    "--size",
                    str(authorization.volume_gib),
                    "--count",
                    "1",
                    "--scheduled-snapshots=false",
                    "--json",
                    "--yes",
                ),
                timeout=self._timeout(deadline, CREATE_TIMEOUT_SECONDS),
            )
        )
        self._volume_id = volume_id
        machine_name = f"{authorization.app}-worker"
        lifetime = str(authorization.maximum_machine_seconds)
        self._boundary.run(
            (
                "flyctl",
                "machine",
                "run",
                authorization.image_digest,
                lifetime,
                "--app",
                authorization.app,
                "--name",
                machine_name,
                "--metadata",
                f"graphforge_attempt_nonce={authorization.attempt_nonce}",
                "--metadata",
                f"graphforge_commit={invocation.commit}",
                "--metadata",
                f"graphforge_owner={authorization.teardown_owner}",
                "--metadata",
                f"graphforge_machine_class={authorization.machine_class}",
                "--region",
                authorization.region,
                "--vm-size",
                authorization.machine_class,
                "--volume",
                f"{volume_id}:/work",
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
            timeout=self._timeout(deadline, CREATE_TIMEOUT_SECONDS),
        )

        def admit(machine_id: str) -> str:
            self._machine_id = machine_id
            return self._validate_owned_state(deadline)

        self._machine_id, observed = wait_for_machine_readiness(
            self._boundary,
            authorization,
            machine_name=machine_name,
            deadline=deadline,
            clock=self._clock,
            sleeper=self._sleeper,
            admit=admit,
        )
        return ProvisionedAttempt(
            image_digest=observed,
            resources={"machine_id": self._machine_id, "volume_id": volume_id},
        )

    def upload_plan(self, *, rung: int, plan_path: Path, deadline: datetime) -> None:
        app, machine_id = self._owned_machine()
        if rung not in PROVIDER_RUNGS or not plan_path.is_file():
            raise FlyTransportError("admitted provider plan is unavailable")
        remote = f"/work/s{rung}-admitted-plan.json"
        self._boundary.run(
            (
                "flyctl",
                "sftp",
                "put",
                str(plan_path),
                remote,
                "--app",
                app,
                "--machine",
                machine_id,
                "--mode",
                "0444",
                "--quiet",
            ),
            timeout=self._timeout(deadline, TRANSFER_TIMEOUT_SECONDS),
        )
        self._uploaded_rung = rung

    def execute_rung(self, *, rung: int, image_digest: str, deadline: datetime) -> int:
        app, machine_id = self._owned_machine()
        if (
            self._uploaded_rung != rung
            or IMAGE.fullmatch(image_digest) is None
            or self._validate_owned_state(deadline) != image_digest
        ):
            raise FlyTransportError("provider rung lacks an uploaded immutable plan")
        remote = f"/work/s{rung}-admitted-plan.json"
        command = shlex.join(
            (
                "/usr/local/bin/run-progressive-qualification",
                "--admitted-plan",
                remote,
                "--output-dir",
                REMOTE_OUTPUT_DIR,
                "--image-digest",
                image_digest,
            )
        )
        completed = self._boundary.run(
            (
                "flyctl",
                "machine",
                "exec",
                machine_id,
                "--app",
                app,
                "--timeout",
                str(self._timeout(deadline, 18_000)),
                "--json",
                command,
            ),
            timeout=self._timeout(deadline, 18_000),
            check=False,
        )
        self._executed_rung = rung
        return completed.returncode

    def retrieve_result(self, *, rung: int, destination: Path, deadline: datetime) -> None:
        if self._executed_rung != rung or destination.name != f"s{rung}-result.json":
            raise FlyTransportError("provider result retrieval is not canonical")
        self._retrieve(
            rung=rung,
            name=destination.name,
            destination=destination,
            deadline=deadline,
        )
        self._retrieved_results.add(rung)

    def retrieve_success_artifacts(
        self,
        *,
        rung: int,
        names: Sequence[str],
        destination: Path,
        deadline: datetime,
    ) -> None:
        expected = tuple(
            f"s{rung}-{suffix}.json" for suffix in ("plan", "benchexec", "graphforge", "rung")
        )
        if (
            rung not in self._retrieved_results
            or tuple(names) != expected
            or not destination.is_dir()
        ):
            raise FlyTransportError("provider artifact retrieval is not canonical")
        for name in expected:
            self._retrieve(
                rung=rung,
                name=name,
                destination=destination / name,
                deadline=deadline,
            )

    def _retrieve(
        self,
        *,
        rung: int,
        name: str,
        destination: Path,
        deadline: datetime,
    ) -> None:
        app, machine_id = self._owned_machine()
        allowed = {
            f"s{rung}-{suffix}.json"
            for suffix in ("plan", "benchexec", "graphforge", "rung", "result")
        }
        if name not in allowed:
            raise FlyTransportError("provider evidence path is not allowed")
        self._boundary.run(
            (
                "flyctl",
                "sftp",
                "get",
                f"{REMOTE_OUTPUT_DIR}/{name}",
                str(destination),
                "--app",
                app,
                "--machine",
                machine_id,
                "--quiet",
            ),
            timeout=self._timeout(deadline, TRANSFER_TIMEOUT_SECONDS),
        )

    def teardown(self, resources: Mapping[str, str]) -> Mapping[str, Any]:
        app = resources.get("owner_app")
        if not isinstance(app, str) or APP.fullmatch(app) is None:
            raise FlyTransportError("provider teardown ownership is unavailable")
        teardown_deadline = self._clock() + timedelta(seconds=TEARDOWN_TIMEOUT_SECONDS)
        known_machine = resources.get("machine_id")
        known_volume = resources.get("volume_id")
        inventory_failure = False
        unexpected_inventory = False

        def best_effort(argv: tuple[str, ...]) -> None:
            with suppress(Exception):
                self._boundary.run(
                    argv,
                    timeout=self._timeout(teardown_deadline, TEARDOWN_TIMEOUT_SECONDS),
                    check=False,
                )

        def app_exists() -> bool:
            return app in _app_names(
                self._boundary.json(
                    ("flyctl", "apps", "list", "--json"),
                    timeout=self._timeout(teardown_deadline, TEARDOWN_TIMEOUT_SECONDS),
                )
            )

        def inventory() -> tuple[list[str], list[str], list[str]]:
            machines = _list(
                self._boundary.json(
                    ("flyctl", "machine", "list", "--app", app, "--json"),
                    timeout=self._timeout(teardown_deadline, TEARDOWN_TIMEOUT_SECONDS),
                ),
                "Machine",
            )
            machine_ids = [
                item.get("id") if isinstance(item, Mapping) else None for item in machines
            ]
            if any(
                not isinstance(item, str) or MACHINE_ID.fullmatch(item) is None
                for item in machine_ids
            ) or len(set(machine_ids)) != len(machine_ids):
                raise FlyTransportError("provider Machine inventory is malformed")
            volumes = _volume_items(
                self._boundary.json(
                    ("flyctl", "volumes", "list", "--app", app, "--json"),
                    timeout=self._timeout(teardown_deadline, TEARDOWN_TIMEOUT_SECONDS),
                )
            )
            volume_ids = [item.get("id") for item in volumes]
            if any(
                not isinstance(item, str) or VOLUME_ID.fullmatch(item) is None
                for item in volume_ids
            ) or len(set(volume_ids)) != len(volume_ids):
                raise FlyTransportError("provider volume inventory is malformed")
            secrets = _secret_names(
                self._boundary.json(
                    ("flyctl", "secrets", "list", "--app", app, "--json"),
                    timeout=self._timeout(teardown_deadline, TEARDOWN_TIMEOUT_SECONDS),
                )
            )
            return machine_ids, volume_ids, secrets

        machine_ids: set[str] = set()
        volume_ids: set[str] = set()
        secrets: list[str] = []
        if isinstance(known_machine, str) and MACHINE_ID.fullmatch(known_machine):
            machine_ids.add(known_machine)
        if isinstance(known_volume, str) and VOLUME_ID.fullmatch(known_volume):
            volume_ids.add(known_volume)
        try:
            exists = app_exists()
            if exists:
                observed_machines, observed_volumes, secrets = inventory()
                machine_ids.update(observed_machines)
                volume_ids.update(observed_volumes)
                unexpected_inventory = (
                    len(observed_machines) > 1 or len(observed_volumes) > 1 or bool(secrets)
                )
        except Exception:
            exists = True
            inventory_failure = True

        if exists:
            for machine_id in sorted(machine_ids):
                best_effort(("flyctl", "machine", "destroy", "--app", app, "--force", machine_id))
            for volume_id in sorted(volume_ids):
                best_effort(("flyctl", "volumes", "destroy", "--app", app, "--yes", volume_id))
            if secrets:
                best_effort(("flyctl", "secrets", "unset", *secrets, "--app", app, "--yes"))
            best_effort(("flyctl", "apps", "destroy", app, "--yes"))

        last = {"app_exists": False, "machines": 0, "volumes": 0, "secrets": 0}
        try:
            for attempt in range(TEARDOWN_POLL_ATTEMPTS):
                if not app_exists():
                    if inventory_failure or unexpected_inventory:
                        raise AttemptError(
                            "inventory_unavailable", "provider teardown inventory was anomalous"
                        )
                    return last
                remaining_machines, remaining_volumes, remaining_secrets = inventory()
                last = {
                    "app_exists": True,
                    "machines": len(remaining_machines),
                    "volumes": len(remaining_volumes),
                    "secrets": len(remaining_secrets),
                }
                if attempt + 1 < TEARDOWN_POLL_ATTEMPTS:
                    self._sleeper(TEARDOWN_POLL_INTERVAL_SECONDS)
        except AttemptError:
            raise
        except Exception as error:
            raise AttemptError(
                "inventory_unavailable", "provider teardown inventory is unavailable"
            ) from error
        return last
