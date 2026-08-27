#!/usr/bin/env python3
"""Run one disposable, private Fly S20 certification Machine.

Dry-run is the default.  This controller owns infrastructure only; the pinned
image owns the GraphForge lifecycle and writes the sanitized evidence artifact.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from contextlib import suppress
from datetime import datetime, timezone
from decimal import ROUND_CEILING, Decimal, InvalidOperation
import fcntl
import hashlib
from html.parser import HTMLParser
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from typing import Any
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "scripts/ci/validate-fly-g500-s20.py"
ATTESTATION_TOOL = ROOT / "scripts/ci/fly-s20-source-attestation.py"
SHA = re.compile(r"^[0-9a-f]{40}$")
LOCAL_IMAGE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/:@-]{0,255}$")
SAFE_NAME = re.compile(r"^[a-z][a-z0-9-]{2,62}$")
SAFE_VOLUME = re.compile(r"^[a-z][a-z0-9_]{0,29}$")
SAFE_REGION = re.compile(r"^[a-z0-9-]{2,20}$")
CPUS = 2
MEMORY_MB = 4096
MAX_COST_USD = 10.0
PRICING_SOURCE = "https://fly.io/docs/about/pricing/"
# Largest currently published regional performance-2x/4GB price (2026-08-27).
VOLUME_USD_PER_GB_MONTH = 0.15
COMPUTE_PRICE_CEILING_USD_PER_SECOND = 0.00003864
RUN_SECONDS = 14_400
RESULT_HANDOFF_SECONDS = 300
PROVISIONING_SECONDS = 300
MONITOR_INTERVAL_SECONDS = 30
MONITOR_TRANSFER_TIMEOUT_SECONDS = 20
REGISTRY_TRANSFER_TIMEOUT_SECONDS = 1_800
CONTROL_PLANE_TIMEOUT_SECONDS = 120
LOCAL_METADATA_TIMEOUT_SECONDS = 120
CLEANUP_RESERVE_SECONDS = 600
VOLUME_BILLING_HOURS = 5
EVIDENCE_PATH = "/work/s20-evidence.json"
RESULT_PATH = "/work/container-result.json"
JOURNAL_PATH = "/work/s20-journal.json"
ACTIVE_PHASE_PATH = "/work/s20-active-phase.json"
CLEANUP_ATTEMPTS = 60
CLEANUP_POLL_SECONDS = 2.0
AUTH_TOKEN_TTL_SECONDS = 300.0
RUN_TOKEN_LIFETIME_SECONDS = 6 * 60 * 60
TOKEN_SETUP_RESERVE_SECONDS = 60 * 60
PHASES = {
    "generate",
    "ingest",
    "source_reopen",
    "source_query_1hop",
    "source_query_2hop",
    "export",
    "verify",
    "import",
    "imported_reopen",
    "imported_query_1hop",
    "imported_query_2hop",
    "finalize",
    "runner",
}


class ControllerError(RuntimeError):
    pass


class OwnedAppCreationError(ControllerError):
    """Creation failed ambiguously, but the exact target app is now owned."""


class ProviderRequestError(ControllerError):
    def __init__(self, code: str, details: dict[str, Any]):
        super().__init__(code)
        self.code = code
        self.details = details


def validate_provider_request_diagnostic(value: dict[str, Any]) -> None:
    required = {
        "operation",
        "outcome",
        "http_class",
        "http_status",
        "elapsed_seconds",
        "body_prefix_sha256",
        "body_truncated",
        "request_id_sha256",
        "request_attempts",
        "read_reloaded_once",
    }
    if set(value) != required:
        raise ControllerError("provider request diagnostic is not closed")
    if value["operation"] not in {
        "machine_create",
        "machine_create_reconcile",
        "machine_fresh_get",
        "machine_runtime_get",
        "machine_absence_get",
        "volume_absence_get",
    }:
        raise ControllerError("provider request diagnostic operation is invalid")
    if value["outcome"] not in {"http_error", "network_error"}:
        raise ControllerError("provider request diagnostic outcome is invalid")
    if value["http_class"] not in {
        "authentication_invalid",
        "permission_denied",
        "not_found",
        "request_timeout",
        "conflict",
        "rate_limited",
        "transient_server",
        "provider_rejected",
        "network_error",
        "malformed_response",
    }:
        raise ControllerError("provider request diagnostic class is invalid")
    for key in ("body_prefix_sha256", "request_id_sha256"):
        if value[key] is not None and not re.fullmatch(r"sha256:[0-9a-f]{64}", value[key]):
            raise ControllerError("provider request diagnostic hash is invalid")
    if not isinstance(value["body_truncated"], bool) or not isinstance(
        value["read_reloaded_once"], bool
    ):
        raise ControllerError("provider request diagnostic boolean is invalid")
    if value["request_attempts"] not in {1, 2}:
        raise ControllerError("provider request diagnostic attempts are invalid")
    if type(value["request_attempts"]) is not int:
        raise ControllerError("provider request diagnostic attempts type is invalid")
    if type(value["elapsed_seconds"]) is not int or value["elapsed_seconds"] < 0:
        raise ControllerError("provider request diagnostic elapsed time is invalid")
    status = value["http_status"]
    if status is not None and (type(status) is not int or not 100 <= status <= 599):
        raise ControllerError("provider request diagnostic HTTP status is invalid")
    network = value["outcome"] == "network_error"
    if network != (value["http_class"] == "network_error" and status is None):
        raise ControllerError("provider request diagnostic network fields disagree")
    safe_read = value["operation"] != "machine_create"
    expected_reload = safe_read and value["request_attempts"] == 2
    if value["read_reloaded_once"] is not expected_reload:
        raise ControllerError("provider request diagnostic retry fields disagree")


class RunCredential:
    def __init__(self, token_id: str, name: str, secret: str, expires_at_monotonic: float):
        self.token_id = token_id
        self.name = name
        self.secret = secret
        self.expires_at_monotonic = expires_at_monotonic


def parse_org_token_list(output: str) -> dict[str, dict[str, str]]:
    """Parse flyctl's bounded human table, rejecting duplicate identities."""
    if "\x1b" in output or len(output) > 1_000_000:
        raise ControllerError("Fly org token list has unsafe formatting")
    lines = output.splitlines()
    header_index = next(
        (index for index, line in enumerate(lines) if "ID" in line and "EXPIRES AT" in line),
        None,
    )
    if header_index is None:
        raise ControllerError("Fly org token list has no recognized header")
    header = lines[header_index]
    rows: dict[str, dict[str, str]] = {}
    unicode_table = "│" in header
    columns = [
        header.index(label) for label in ("ID", "NAME", "CREATED BY", "EXPIRES AT", "REVOKED AT")
    ]
    if columns != sorted(columns) or len(set(columns)) != 5:
        raise ControllerError("Fly org token list header is ambiguous")
    for line in lines[header_index + 1 :]:
        if not line.strip():
            continue
        if unicode_table:
            fields = [field.strip() for field in line.split("│")]
        else:
            padded = line + " " * max(0, len(header) - len(line))
            fields = [padded[columns[index] : columns[index + 1]].strip() for index in range(4)] + [
                padded[columns[4] :].strip()
            ]
        if len(fields) != 5:
            raise ControllerError("Fly org token list row is malformed")
        token_id, name, created_by, expires_at, revoked_at = fields
        if not token_id or not name or token_id in rows:
            raise ControllerError("Fly org token list is ambiguous")
        rows[token_id] = {
            "name": name,
            "created_by": created_by,
            "expires_at": expires_at,
            "revoked_at": revoked_at,
        }
    return rows


def list_org_tokens(bootstrap: Flyctl, org: str) -> dict[str, dict[str, str]]:
    result = bootstrap.run(["tokens", "list", "--scope", "org", "--org", org])
    return parse_org_token_list(result.stdout)


def list_org_tokens_bounded(
    bootstrap: Flyctl, org: str, attempts: int = 3
) -> dict[str, dict[str, str]]:
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            return list_org_tokens(bootstrap, org)
        except (ControllerError, subprocess.SubprocessError) as error:  # noqa: PERF203
            last_error = error
    raise ControllerError("Fly org token inventory is unavailable") from last_error


def token_is_active(row: dict[str, str]) -> bool:
    marker = row["revoked_at"].strip()
    if marker in {"", "-"}:
        return True
    parse_token_expiry(marker)
    return False


def parse_token_expiry(value: str) -> float:
    normalized = value.strip().replace("Z", "+00:00")
    go_time = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?:\.(\d+))? \+0000 UTC",
        normalized,
    )
    if go_time:
        fraction = (go_time.group(2) or "")[:6]
        normalized = go_time.group(1) + (f".{fraction}" if fraction else "") + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        raise ControllerError("Fly org token expiry is not machine-verifiable") from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _registry_stderr_class(stderr: str) -> str:
    lowered = stderr.lower()
    for patterns, classification in (
        (("unauthorized", "authentication required"), "authentication_rejected"),
        (("denied", "forbidden"), "permission_rejected"),
        (("timeout", "timed out", "deadline exceeded"), "transport_timeout"),
        (("connection reset", "unexpected eof", "network"), "transport_failure"),
        (("blob upload", "manifest invalid"), "content_rejected"),
    ):
        if any(pattern in lowered for pattern in patterns):
            return classification
    return "unclassified_failure"


def _registry_failure_details(
    error: subprocess.CalledProcessError | subprocess.TimeoutExpired,
    elapsed_seconds: float,
) -> dict[str, Any]:
    stderr_value = error.stderr
    stdout_value = getattr(error, "stdout", None) or getattr(error, "output", None)
    if isinstance(stderr_value, bytes):
        stderr_value = stderr_value.decode("utf-8", errors="replace")
    stderr = stderr_value if isinstance(stderr_value, str) else ""
    if isinstance(stdout_value, bytes):
        stdout_value = stdout_value.decode("utf-8", errors="replace")
    stdout = stdout_value if isinstance(stdout_value, str) else ""
    captured = "\n".join((stdout[-16_384:], stderr[-16_384:]))
    timed_out = isinstance(error, subprocess.TimeoutExpired)
    return {
        "operation": "docker_push",
        "outcome": "timeout" if timed_out else "nonzero_exit",
        "exit_code": None if timed_out else int(error.returncode),
        "elapsed_seconds": min(
            REGISTRY_TRANSFER_TIMEOUT_SECONDS, max(0, math.ceil(elapsed_seconds))
        ),
        "timeout_seconds": REGISTRY_TRANSFER_TIMEOUT_SECONDS,
        "stderr_class": _registry_stderr_class(captured),
        "stdout_sha256": "sha256:" + hashlib.sha256(stdout.encode()).hexdigest(),
        "stderr_sha256": "sha256:" + hashlib.sha256(stderr.encode()).hexdigest(),
    }


class Flyctl:
    def __init__(self, credential: RunCredential | None = None):
        self.credential = credential

    def run(
        self,
        args: Sequence[str],
        *,
        check: bool = True,
        timeout: float = CONTROL_PLANE_TIMEOUT_SECONDS,
    ) -> subprocess.CompletedProcess[str]:
        environment = None
        if self.credential is not None:
            environment = {**os.environ, "FLY_API_TOKEN": self.auth_token()}
        return subprocess.run(
            ["flyctl", *args],
            cwd=ROOT,
            env=environment,
            check=check,
            text=True,
            capture_output=True,
            timeout=timeout,
        )

    def json(self, args: Sequence[str], *, timeout: float = 120) -> Any:
        return json.loads(self.run([*args, "--json"], timeout=timeout).stdout)

    def auth_token(self, *, deadline: float | None = None, force_refresh: bool = False) -> str:
        if self.credential is not None:
            if time.monotonic() >= self.credential.expires_at_monotonic:
                raise ControllerError("run-scoped Fly credential expired")
            return self.credential.secret
        cached = getattr(self, "_cached_auth_token", None)
        fetched_at = getattr(self, "_cached_auth_token_at", 0.0)
        if (
            not force_refresh
            and isinstance(cached, str)
            and cached
            and time.monotonic() - fetched_at < AUTH_TOKEN_TTL_SECONDS
        ):
            return cached
        timeout = _cleanup_timeout(deadline) if deadline is not None else 120
        token = self.run(["auth", "token", "--quiet"], timeout=timeout).stdout.strip()
        if not token:
            raise ControllerError("Fly authentication token is unavailable")
        self._cached_auth_token = token
        self._cached_auth_token_at = time.monotonic()
        return token

    def subprocess_environment(self, base: dict[str, str]) -> dict[str, str]:
        if self.credential is None:
            return base
        return {**base, "FLY_API_TOKEN": self.auth_token()}

    def api_json(
        self,
        method: str,
        path: str,
        *,
        data: dict[str, Any] | None = None,
        timeout: float = 30,
        deadline: float | None = None,
        absent_ok: bool = False,
        operation: str,
    ) -> Any:
        """Call the Machines API with method-safe, closed failure handling."""
        if operation not in {
            "machine_create",
            "machine_create_reconcile",
            "machine_fresh_get",
            "machine_runtime_get",
            "machine_absence_get",
            "volume_absence_get",
        }:
            raise ControllerError("unsupported Fly API operation")
        started = time.monotonic()
        safe_read = method == "GET"
        max_attempts = 2 if safe_read else 1
        for attempt in range(max_attempts):
            token = self.auth_token(
                deadline=deadline,
                force_refresh=attempt == 1 and self.credential is None,
            )
            headers = {"Authorization": f"Bearer {token}"}
            encoded = None
            if data is not None:
                headers["Content-Type"] = "application/json"
                encoded = json.dumps(data).encode()
            request = urllib.request.Request(
                f"https://api.machines.dev{path}",
                data=encoded,
                headers=headers,
                method=method,
            )
            request_timeout = _cleanup_timeout(deadline) if deadline is not None else timeout
            try:
                with urllib.request.urlopen(request, timeout=request_timeout) as response:
                    body = response.read()
                    if not body:
                        return None
                    try:
                        return json.loads(body)
                    except json.JSONDecodeError:
                        if safe_read and attempt == 0:
                            continue
                        prefix = body[:16_384]
                        raise ProviderRequestError(
                            f"fly_api_{operation}_malformed_response",
                            {
                                "operation": operation,
                                "outcome": "http_error",
                                "http_class": "malformed_response",
                                "http_status": 200,
                                "elapsed_seconds": max(0, math.ceil(time.monotonic() - started)),
                                "body_prefix_sha256": "sha256:"
                                + hashlib.sha256(prefix).hexdigest(),
                                "body_truncated": len(body) > len(prefix),
                                "request_id_sha256": None,
                                "request_attempts": attempt + 1,
                                "read_reloaded_once": bool(safe_read and attempt == 1),
                            },
                        ) from None
            except urllib.error.HTTPError as error:
                if error.code == 401 and safe_read and self.credential is None and attempt == 0:
                    continue
                if error.code == 404 and absent_ok:
                    return None
                body = error.read(16_385)
                prefix = body[:16_384]
                retryable = error.code in {408, 429} or 500 <= error.code <= 599
                # A safe read may be repeated once. Mutations are never replayed:
                # their caller must reconcile the exact resource identity.
                if safe_read and attempt == 0 and (error.code == 403 or retryable):
                    continue
                http_class = {
                    401: "authentication_invalid",
                    403: "permission_denied",
                    404: "not_found",
                    408: "request_timeout",
                    409: "conflict",
                    429: "rate_limited",
                }.get(
                    error.code,
                    "transient_server" if 500 <= error.code <= 599 else "provider_rejected",
                )
                header_value = ""
                for header in ("Fly-Request-Id", "X-Request-Id", "Request-Id"):
                    candidate = error.headers.get(header) if error.headers else None
                    if candidate:
                        header_value = candidate[:1_024]
                        break
                raise ProviderRequestError(
                    f"fly_api_{operation}_{http_class}",
                    {
                        "operation": operation,
                        "outcome": "http_error",
                        "http_class": http_class,
                        "http_status": error.code,
                        "elapsed_seconds": max(0, math.ceil(time.monotonic() - started)),
                        "body_prefix_sha256": "sha256:" + hashlib.sha256(prefix).hexdigest(),
                        "body_truncated": len(body) > len(prefix),
                        "request_id_sha256": (
                            "sha256:" + hashlib.sha256(header_value.encode()).hexdigest()
                            if header_value
                            else None
                        ),
                        "request_attempts": attempt + 1,
                        "read_reloaded_once": bool(safe_read and attempt == 1),
                    },
                ) from None
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                if safe_read and attempt == 0:
                    continue
                reason = type(error).__name__
                raise ProviderRequestError(
                    f"fly_api_{operation}_network_error",
                    {
                        "operation": operation,
                        "outcome": "network_error",
                        "http_class": "network_error",
                        "http_status": None,
                        "elapsed_seconds": max(0, math.ceil(time.monotonic() - started)),
                        "body_prefix_sha256": "sha256:"
                        + hashlib.sha256(reason.encode()).hexdigest(),
                        "body_truncated": False,
                        "request_id_sha256": None,
                        "request_attempts": attempt + 1,
                        "read_reloaded_once": bool(safe_read and attempt == 1),
                    },
                ) from None
        raise ControllerError("Fly API request exhausted")

    def resource_absent(self, kind: str, app: str, resource_id: str, *, deadline: float) -> bool:
        """Return true only for an authenticated provider 404."""
        if kind not in {"machines", "volumes"}:
            raise ControllerError("unsupported provider absence probe")
        return (
            self.api_json(
                "GET",
                f"/v1/apps/{app}/{kind}/{resource_id}",
                deadline=deadline,
                absent_ok=True,
                operation=f"{kind[:-1]}_absence_get",
            )
            is None
        )

    def machine_runtime(
        self, app: str, machine_id: str, *, deadline: float | None = None
    ) -> dict[str, Any]:
        """Read Machine runtime state from the stable authenticated provider API."""
        value = self.api_json(
            "GET",
            f"/v1/apps/{app}/machines/{machine_id}",
            deadline=deadline,
            absent_ok=True,
            operation="machine_runtime_get",
        )
        if value is None:
            return {"state": "destroyed", "oom": False}
        return normalize_machine_runtime(value)


def run_scoped_flyctl(credential: RunCredential) -> Flyctl:
    return Flyctl(credential)


def validate_inputs(args: argparse.Namespace) -> None:
    if not LOCAL_IMAGE.fullmatch(args.image):
        raise ControllerError("--image must be one safe local OCI image reference")
    if not SHA.fullmatch(args.expected_sha):
        raise ControllerError("--expected-sha must be exact lowercase 40-hex")
    if not SAFE_REGION.fullmatch(args.region):
        raise ControllerError("invalid fixed region")
    if not SAFE_NAME.fullmatch(args.app_name) or not SAFE_NAME.fullmatch(args.machine_name):
        raise ControllerError("app and Machine names must be safe lowercase names")
    if not SAFE_VOLUME.fullmatch(args.volume_name):
        raise ControllerError("invalid Fly volume name")
    if not 1 <= args.volume_size_gb <= 500:
        raise ControllerError("volume must be in 1..500 GB")
    if not 60 <= args.timeout_s <= 14_400:
        raise ControllerError("timeout must be in 60..14400 seconds")
    for path, label in (
        (args.evidence_out, "evidence"),
        (args.diagnostic_out, "diagnostic"),
        (args.ledger, "ledger"),
    ):
        if not path.parent.is_dir():
            raise ControllerError(f"{label} output parent directory does not exist")
    if args.execute and not args.confirm_disposable:
        raise ControllerError("--execute requires --confirm-disposable")


def check_source(expected_sha: str) -> None:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        timeout=30,
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        timeout=30,
    ).stdout
    if head != expected_sha or dirty:
        raise ControllerError("exact expected SHA must be checked out in a clean tree")


def price_reservation(
    volume_size_gb: int,
    compute_rate: Decimal = Decimal(str(COMPUTE_PRICE_CEILING_USD_PER_SECOND)),
    volume_rate: Decimal = Decimal(str(VOLUME_USD_PER_GB_MONTH)),
) -> dict[str, Any]:
    compute = Decimal(RUN_SECONDS + CLEANUP_RESERVE_SECONDS) * compute_rate
    volume = Decimal(volume_size_gb) * volume_rate / Decimal(30 * 24) * VOLUME_BILLING_HOURS
    total_cents = ((compute + volume) * 100).quantize(Decimal("1"), rounding=ROUND_CEILING)
    return {
        "pricing_source": PRICING_SOURCE,
        "compute_rate_usd_per_second": float(compute_rate),
        "volume_rate_usd_per_gb_month": float(volume_rate),
        "runtime_seconds": RUN_SECONDS,
        "cleanup_reserve_seconds": CLEANUP_RESERVE_SECONDS,
        "volume_billing_hours": VOLUME_BILLING_HOURS,
        "volume_size_gb": volume_size_gb,
        "reserved_usd": float(total_cents / 100),
    }


class _PricingTables(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.table_id: str | None = None
        self.rows: dict[str, list[list[str]]] = {}
        self.row: list[str] | None = None
        self.cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if (
            tag == "div"
            and (identifier := attributes.get("id"))
            and identifier.startswith("started-machines-pricing-matrix-")
        ):
            self.table_id = identifier
            self.rows.setdefault(identifier, [])
        elif self.table_id and tag == "tr":
            self.row = []
        elif self.row is not None and tag in {"td", "th"}:
            self.cell = []

    def handle_data(self, data: str) -> None:
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self.cell is not None and self.row is not None:
            self.row.append(" ".join("".join(self.cell).split()))
            self.cell = None
        elif tag == "tr" and self.row is not None and self.table_id:
            self.rows[self.table_id].append(self.row)
            self.row = None
        elif tag == "div" and self.table_id:
            self.table_id = None


def parse_current_pricing(html: str, region: str) -> tuple[Decimal, Decimal]:
    """Select exactly one official fixed-region performance-2x/4GB price row."""
    parser = _PricingTables()
    parser.feed(html)
    table = parser.rows.get(f"started-machines-pricing-matrix-{region}")
    if table is None:
        raise ControllerError("official Fly pricing has no table for the fixed region")
    preset = None
    matches: list[Decimal] = []
    for cells in table:
        if cells and re.fullmatch(r"performance-\d+x", cells[0]):
            preset = cells[0]
        if preset != "performance-2x" or "4GB" not in cells or "2 performance" not in cells:
            continue
        prices = [
            Decimal(value) for value in re.findall(r"\$([0-9]+(?:\.[0-9]+)?)", " ".join(cells))
        ]
        if len(prices) != 3:
            raise ControllerError("official Fly pricing row has ambiguous price columns")
        matches.append(prices[0])
    if len(matches) != 1:
        raise ControllerError("official Fly pricing does not contain one applicable compute row")
    volume_sections = re.findall(r"Fly Volumes(?:(?!Volume billing).)*Volume billing", html, re.S)
    volume_matches = {
        match
        for section in volume_sections
        for match in re.findall(r"\$([0-9]+(?:\.[0-9]+)?)/GB\s+per\s+month", section)
    }
    if len(volume_matches) != 1:
        raise ControllerError("official Fly pricing has ambiguous volume rates")
    compute = matches[0]
    volume = Decimal(next(iter(volume_matches)))
    if compute > Decimal(str(COMPUTE_PRICE_CEILING_USD_PER_SECOND)):
        raise ControllerError("applicable Fly compute price exceeds the authorized ceiling")
    return compute, volume


def fetch_current_pricing(region: str) -> tuple[Decimal, Decimal]:
    request = urllib.request.Request(
        PRICING_SOURCE, headers={"User-Agent": "graphforge-fly-s20-controller/1"}
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            if response.geturl() != PRICING_SOURCE:
                raise ControllerError("official Fly pricing request redirected")
            return parse_current_pricing(response.read().decode("utf-8"), region)
    except urllib.error.URLError:
        raise ControllerError("official Fly pricing is unavailable") from None


def expected_source_snapshot() -> str:
    spec = importlib.util.spec_from_file_location("source_attestation", ATTESTATION_TOOL)
    if not spec or not spec.loader:
        raise ControllerError("cannot load source attestation tool")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.snapshot_sha256(ROOT)


def inspect_image(
    image: str,
    expected_sha: str,
    digest: str | None = None,
    environment: dict[str, str] | None = None,
) -> str:
    """Authenticate a local image, optionally pulling an immutable registry digest."""
    if digest is not None:
        subprocess.run(
            ["docker", "pull", "--platform", "linux/amd64", image],
            env=environment,
            check=True,
            text=True,
            capture_output=True,
            timeout=REGISTRY_TRANSFER_TIMEOUT_SECONDS,
        )
    result = subprocess.run(
        ["docker", "image", "inspect", image],
        env=environment,
        check=True,
        text=True,
        capture_output=True,
        timeout=LOCAL_METADATA_TIMEOUT_SECONDS,
    )
    inspected = json.loads(result.stdout)
    if not isinstance(inspected, list) or len(inspected) != 1:
        raise ControllerError("OCI inspection did not return exactly one image")
    image_data = inspected[0]
    if image_data.get("Os") != "linux" or image_data.get("Architecture") != "amd64":
        raise ControllerError("pulled OCI image is not the required linux/amd64 runtime")
    repo_digests = image_data.get("RepoDigests")
    labels = image_data.get("Config", {}).get("Labels")
    if digest is not None:
        requested_repo = image.rsplit("@", 1)[0]
        if not isinstance(repo_digests, list) or f"{requested_repo}@{digest}" not in repo_digests:
            raise ControllerError(
                "pulled OCI image does not authenticate the requested repo digest"
            )
    if (
        not isinstance(labels, dict)
        or labels.get("org.opencontainers.image.revision") != expected_sha
    ):
        raise ControllerError("OCI revision label does not equal the exact expected SHA")
    if labels.get("dev.graphforge.fly-s20") != "graphforge-fly-s20-runtime/1":
        raise ControllerError("OCI runtime schema label is missing or unsupported")
    created = subprocess.run(
        ["docker", "create", "--platform", "linux/amd64", image],
        env=environment,
        check=True,
        text=True,
        capture_output=True,
        timeout=LOCAL_METADATA_TIMEOUT_SECONDS,
    ).stdout.strip()
    try:
        with tempfile.TemporaryDirectory(prefix="graphforge-image-provenance-") as directory:
            target = Path(directory) / "build-provenance.json"
            subprocess.run(
                [
                    "docker",
                    "cp",
                    f"{created}:/usr/local/share/graphforge/build-provenance.json",
                    str(target),
                ],
                env=environment,
                check=True,
                text=True,
                capture_output=True,
                timeout=LOCAL_METADATA_TIMEOUT_SECONDS,
            )
            provenance = json.loads(target.read_text(encoding="utf-8"))
    finally:
        subprocess.run(
            ["docker", "rm", created],
            env=environment,
            check=False,
            text=True,
            capture_output=True,
            timeout=LOCAL_METADATA_TIMEOUT_SECONDS,
        )
    expected_snapshot = expected_source_snapshot()
    if provenance != {
        "schema": "graphforge-fly-s20-build-provenance/1",
        "source_sha": expected_sha,
        "source_snapshot_sha256": expected_snapshot,
    }:
        raise ControllerError("embedded build provenance differs from the exact source snapshot")
    return expected_snapshot


def inspect_local_image(image: str, expected_sha: str) -> tuple[str, str]:
    """Resolve a mutable caller reference once, then authenticate its immutable image ID."""
    result = subprocess.run(
        ["docker", "image", "inspect", image],
        check=True,
        text=True,
        capture_output=True,
        timeout=LOCAL_METADATA_TIMEOUT_SECONDS,
    )
    inspected = json.loads(result.stdout)
    if not isinstance(inspected, list) or len(inspected) != 1:
        raise ControllerError("local OCI inspection did not return exactly one image")
    image_id = inspected[0].get("Id")
    if not isinstance(image_id, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
        raise ControllerError("local OCI image has no immutable image ID")
    return image_id, inspect_image(image_id, expected_sha)


def create_owned_app(args: argparse.Namespace, fly: Flyctl) -> None:
    """Create the pre-absent app or reconcile an ambiguous create result."""
    try:
        fly.run(["apps", "create", args.app_name, "--org", args.org])
        return
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        apps = fly.json(["apps", "list"])
        matches = [
            item
            for item in apps
            if item.get("Name") == args.app_name or item.get("name") == args.app_name
        ]
        if len(matches) != 1:
            raise ControllerError(
                "app creation failed without one reconcilable owned app"
            ) from None
        item = matches[0]
        organization = item.get("Organization", item.get("organization", {}))
        slug = (
            organization.get("Slug", organization.get("slug"))
            if isinstance(organization, dict)
            else None
        )
        slug = slug or item.get("organization_slug") or item.get("org_slug")
        if slug != args.org:
            raise ControllerError(
                "ambiguous app creation did not reconcile to the target org"
            ) from None
        raise OwnedAppCreationError(
            "app creation failed after the exact owned app became observable"
        ) from error


def create_owned_volume(args: argparse.Namespace, fly: Flyctl) -> dict[str, Any]:
    command = [
        "volumes",
        "create",
        args.volume_name,
        "--app",
        args.app_name,
        "--region",
        args.region,
        "--size",
        str(args.volume_size_gb),
        "--scheduled-snapshots=false",
        "--yes",
    ]
    try:
        value = fly.json(command)
    except (
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        json.JSONDecodeError,
    ) as error:
        listed = fly.json(["volumes", "list", "--app", args.app_name])
        matches = (
            [
                item
                for item in listed
                if isinstance(item, dict)
                and item.get("name", item.get("Name")) == args.volume_name
                and item.get("region", item.get("Region")) == args.region
                and int(item.get("size_gb", item.get("SizeGB", -1))) == args.volume_size_gb
            ]
            if isinstance(listed, list)
            else []
        )
        if len(matches) != 1:
            raise ControllerError(
                "ambiguous volume creation did not reconcile to one exact identity"
            ) from error
        value = matches[0]
    if not isinstance(value, dict):
        raise ControllerError("Fly volume create response is malformed")
    return value


def mint_run_credential(args: argparse.Namespace, bootstrap: Flyctl) -> RunCredential:
    """Mint and uniquely resolve one six-hour org token without persisting its secret."""
    name = f"gf-s20-{args.app_name[-16:]}"
    before = list_org_tokens_bounded(bootstrap, args.org)
    minted_at = time.time()
    result: subprocess.CompletedProcess[str] | None = None
    creation_error: Exception | None = None
    try:
        result = bootstrap.run(
            [
                "tokens",
                "create",
                "org",
                "--org",
                args.org,
                "--expiry",
                "6h",
                "--name",
                name,
                "--json",
            ]
        )
    except (subprocess.SubprocessError, ControllerError) as error:
        creation_error = error
    try:
        after = list_org_tokens_bounded(bootstrap, args.org)
    except ControllerError:
        raise ControllerError(
            "Fly run credential cleanup is unproven; provider expiry remains the safety bound"
        ) from None
    discovered = [
        (token_id, row)
        for token_id, row in after.items()
        if token_id not in before and row["name"] == name and token_is_active(row)
    ]
    candidates = []
    expiry_error: Exception | None = None
    for token_id, row in discovered:
        try:
            expiry = parse_token_expiry(row["expires_at"])
        except ControllerError as error:
            expiry_error = error
            continue
        if (
            minted_at + RUN_TOKEN_LIFETIME_SECONDS - 300
            <= expiry
            <= minted_at + RUN_TOKEN_LIFETIME_SECONDS + 300
        ):
            candidates.append((token_id, expiry))
    try:
        if creation_error is not None:
            raise ControllerError("Fly run credential creation failed") from creation_error
        if expiry_error is not None:
            raise expiry_error
        if result is None:
            raise ControllerError("Fly run credential response is unavailable")
        value = json.loads(result.stdout)
        if isinstance(value, str):
            secret = value
        elif isinstance(value, dict) and not set(value) - {"token"}:
            secret = value.get("token")
        else:
            raise ControllerError("Fly run credential response has unknown fields")
        if (
            not isinstance(secret, str)
            or not 20 <= len(secret) <= 4_096
            or "\n" in secret
            or "\r" in secret
        ):
            raise ControllerError("Fly run credential secret is malformed")
        if len(candidates) != 1:
            raise ControllerError("Fly run credential identity could not be uniquely resolved")
    except Exception:
        cleanup_failures = []
        for token_id, _row in discovered:
            try:
                revoke_token_id(bootstrap, token_id, args.org)
            except Exception as cleanup_error:  # noqa: PERF203
                cleanup_failures.append(str(cleanup_error))
        if cleanup_failures:
            raise ControllerError(
                "Fly run credential mint failed and token cleanup was unproven"
            ) from None
        raise
    token_id, provider_expiry = candidates[0]
    remaining = max(0.0, provider_expiry - time.time() - 60.0)
    return RunCredential(
        token_id=token_id,
        name=name,
        secret=secret,
        expires_at_monotonic=time.monotonic() + remaining,
    )


def revoke_token_id(bootstrap: Flyctl, token_id: str, org: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{7,159}", token_id):
        raise ControllerError("Fly run credential ID is unsafe")
    with suppress(ControllerError, subprocess.SubprocessError):
        bootstrap.run(["tokens", "revoke", token_id], check=False)
    deadline = time.monotonic() + 30.0
    while True:
        row = list_org_tokens_bounded(bootstrap, org).get(token_id)
        if row is None or not token_is_active(row):
            return
        if time.monotonic() >= deadline:
            raise ControllerError("Fly run credential revocation absence was not proven")
        time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))


def revoke_run_credential(bootstrap: Flyctl, credential: RunCredential, org: str) -> None:
    revoke_token_id(bootstrap, credential.token_id, org)


def admit_run_credential(credential: RunCredential, timeout_seconds: int) -> None:
    required = TOKEN_SETUP_RESERVE_SECONDS + timeout_seconds + CLEANUP_RESERVE_SECONDS
    if credential.expires_at_monotonic <= time.monotonic() + required:
        raise ControllerError("Fly run credential expires before teardown deadline")


def publish_to_fly_registry(
    args: argparse.Namespace, local_image_id: str, fly: Flyctl
) -> tuple[str, str, str]:
    """Publish the authenticated local image into the owned app registry."""
    repository = f"registry.fly.io/{args.app_name}"
    tag = f"{repository}:{args.expected_sha}"
    with tempfile.TemporaryDirectory(prefix="graphforge-fly-docker-config-") as config:
        base_environment = {**os.environ, "DOCKER_CONFIG": config}
        environment = (
            fly.subprocess_environment(base_environment)
            if hasattr(fly, "subprocess_environment")
            else base_environment
        )
        subprocess.run(
            ["flyctl", "auth", "docker"],
            cwd=ROOT,
            env=environment,
            check=True,
            text=True,
            capture_output=True,
            timeout=CONTROL_PLANE_TIMEOUT_SECONDS,
        )
        subprocess.run(
            ["docker", "tag", local_image_id, tag],
            env=environment,
            check=True,
            text=True,
            capture_output=True,
            timeout=120,
        )
        push_started = time.monotonic()
        try:
            pushed = subprocess.run(
                ["docker", "push", tag],
                env=environment,
                check=True,
                text=True,
                capture_output=True,
                timeout=REGISTRY_TRANSFER_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as error:
            persist_controller_failure(
                args,
                "registry_push_timeout",
                command_failure=_registry_failure_details(error, time.monotonic() - push_started),
            )
            raise ControllerError("Fly registry push exceeded its bounded timeout") from None
        except subprocess.CalledProcessError as error:
            persist_controller_failure(
                args,
                "registry_push_failed",
                command_failure=_registry_failure_details(error, time.monotonic() - push_started),
            )
            raise ControllerError("Fly registry push failed") from None
        matches = set(re.findall(r"digest:\s*(sha256:[0-9a-f]{64})", pushed.stdout + pushed.stderr))
        if len(matches) != 1:
            raise ControllerError("Fly registry push did not return one immutable digest")
        digest = next(iter(matches))
        image = f"{repository}@{digest}"
        subprocess.run(
            ["docker", "manifest", "inspect", image],
            env=environment,
            check=True,
            text=True,
            capture_output=True,
            timeout=LOCAL_METADATA_TIMEOUT_SECONDS,
        )
        snapshot = inspect_image(image, args.expected_sha, digest, environment)
        return image, digest, snapshot


def reserve_budget(path: Path, run_id: str, reservation: dict[str, Any]) -> None:
    """Append one hash-chained reservation with a separately durable anchor."""
    lock_path = path.with_suffix(path.suffix + ".lock")
    anchor_path = path.with_suffix(path.suffix + ".anchor")
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if path.exists() != anchor_path.exists():
            raise ControllerError("cost ledger or durable anchor is missing")
        state = {"schema": "graphforge-fly-cost-ledger/2", "limit_usd": MAX_COST_USD, "runs": []}
        anchor = {
            "schema": "graphforge-fly-cost-ledger-anchor/1",
            "head_sha256": "sha256:" + "0" * 64,
            "records": 0,
            "reserved_cents": 0,
        }
        if path.exists():
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
                anchor = json.loads(anchor_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                raise ControllerError("cost ledger history is unreadable") from None
        if (
            state.get("schema") != "graphforge-fly-cost-ledger/2"
            or state.get("limit_usd") != MAX_COST_USD
        ):
            raise ControllerError("invalid cost ledger")
        runs = state.get("runs")
        if not isinstance(runs, list):
            raise ControllerError("invalid cost ledger runs")
        head = "sha256:" + "0" * 64
        used_cents = 0
        for record in runs:
            if not isinstance(record, dict) or set(record) != {
                "run_id",
                "previous_sha256",
                "record_sha256",
                "reservation",
            }:
                raise ControllerError("cost ledger has malformed chained record")
            if record["previous_sha256"] != head or not SAFE_NAME.fullmatch(record["run_id"]):
                raise ControllerError("cost ledger chain is invalid")
            canonical = json.dumps(
                {
                    "run_id": record["run_id"],
                    "previous_sha256": record["previous_sha256"],
                    "reservation": record["reservation"],
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            computed = "sha256:" + hashlib.sha256(canonical).hexdigest()
            if record["record_sha256"] != computed:
                raise ControllerError("cost ledger record authentication failed")
            head = computed
            used_cents += int(_validated_reservation(record["reservation"]) * 100)
        expected_anchor = {
            "schema": "graphforge-fly-cost-ledger-anchor/1",
            "head_sha256": head,
            "records": len(runs),
            "reserved_cents": used_cents,
        }
        if anchor != expected_anchor:
            raise ControllerError(
                "cost ledger history regressed or differs from its durable anchor"
            )
        if any(run.get("run_id") == run_id for run in runs):
            raise ControllerError("run id is already reserved")
        amount = _validated_reservation(reservation)
        amount_cents = int(amount * 100)
        if used_cents + amount_cents > int(MAX_COST_USD * 100):
            raise ControllerError("durable cost reservations would exceed $10")
        payload = {"run_id": run_id, "previous_sha256": head, "reservation": reservation}
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        new_head = "sha256:" + hashlib.sha256(canonical).hexdigest()
        runs.append({**payload, "record_sha256": new_head})
        new_anchor = {
            "schema": "graphforge-fly-cost-ledger-anchor/1",
            "head_sha256": new_head,
            "records": len(runs),
            "reserved_cents": used_cents + amount_cents,
        }
        _atomic_durable_json(path, state)
        _atomic_durable_json(anchor_path, new_anchor)


def _atomic_durable_json(path: Path, value: Any) -> None:
    with tempfile.NamedTemporaryFile(
        "w", dir=path.parent, delete=False, encoding="utf-8"
    ) as handle:
        json.dump(value, handle, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _decimal(value: Any, label: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise ControllerError(f"cost ledger has malformed {label}")
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise ControllerError(f"cost ledger has malformed {label}") from None
    if not result.is_finite() or result < 0:
        raise ControllerError(f"cost ledger has invalid {label}")
    return result


def _validated_reservation(value: Any) -> Decimal:
    """Validate a closed reservation and recompute its authoritative cents."""
    fields = {
        "pricing_source",
        "compute_rate_usd_per_second",
        "volume_rate_usd_per_gb_month",
        "runtime_seconds",
        "cleanup_reserve_seconds",
        "volume_billing_hours",
        "volume_size_gb",
        "reserved_usd",
    }
    if (
        not isinstance(value, dict)
        or set(value) - (fields | {"run_id"})
        or not fields <= set(value)
    ):
        raise ControllerError("cost ledger has malformed reservation")
    if value["pricing_source"] != PRICING_SOURCE:
        raise ControllerError("cost ledger has invalid pricing source")
    if (
        value["runtime_seconds"] != RUN_SECONDS
        or value["cleanup_reserve_seconds"] != CLEANUP_RESERVE_SECONDS
        or value["volume_billing_hours"] != VOLUME_BILLING_HOURS
        or isinstance(value["volume_size_gb"], bool)
        or not isinstance(value["volume_size_gb"], int)
        or not 1 <= value["volume_size_gb"] <= 500
    ):
        raise ControllerError("cost ledger has invalid reservation envelope")
    compute = _decimal(value["compute_rate_usd_per_second"], "compute rate")
    volume = _decimal(value["volume_rate_usd_per_gb_month"], "volume rate")
    observed = _decimal(value["reserved_usd"], "reserved amount")
    expected = Decimal(
        str(price_reservation(value["volume_size_gb"], compute, volume)["reserved_usd"])
    )
    if observed != expected:
        raise ControllerError("cost ledger reserved amount does not match recomputed cents")
    return expected


def machine_payload(
    args: argparse.Namespace,
    volume_id: str,
    image: str,
    digest: str,
    source_snapshot: str,
) -> dict[str, Any]:
    return {
        "name": args.machine_name,
        "region": args.region,
        "skip_launch": False,
        "skip_service_registration": True,
        "config": {
            "image": image,
            "auto_destroy": True,
            "restart": {"policy": "no"},
            "guest": {"cpu_kind": "performance", "cpus": CPUS, "memory_mb": MEMORY_MB},
            "mounts": [{"volume": volume_id, "path": "/work"}],
            "services": [],
            "env": {
                "GF_G500_CERTIFICATION_SCALE": "20",
                "GF_G500_S20_EXPECTED_SHA": args.expected_sha,
                "GF_G500_S20_IMAGE_DIGEST": digest,
                "GF_G500_S20_SOURCE_SNAPSHOT_SHA256": source_snapshot,
                "GF_G500_S20_REGION": args.region,
                "GF_G500_S20_VOLUME_GB": str(args.volume_size_gb),
                "GF_G500_S20_EVIDENCE_OUT": EVIDENCE_PATH,
                "GF_G500_S20_RESULT_OUT": RESULT_PATH,
                "GF_G500_S20_TIMEOUT_SECONDS": str(
                    RUN_SECONDS - RESULT_HANDOFF_SECONDS - PROVISIONING_SECONDS
                ),
                "TMPDIR": "/work/tmp",
            },
        },
    }


def assert_volume(volume: Any, args: argparse.Namespace) -> str:
    if not isinstance(volume, dict) or not isinstance(volume.get("id"), str):
        raise ControllerError("Fly did not return an observed volume identity")
    size = volume.get("size_gb", volume.get("size"))
    if volume.get("region") != args.region or size != args.volume_size_gb:
        raise ControllerError("observed volume region/size differs from the pinned plan")
    return volume["id"]


def assert_machine(
    machine: dict[str, Any], args: argparse.Namespace, digest: str, volume_id: str
) -> None:
    checks = machine_response_checks(machine, args, digest, volume_id)
    if not checks["identity_match"]:
        raise ControllerError("observed Machine region/image differs from the pinned plan")
    if not checks["guest_match"]:
        raise ControllerError("observed Machine resources differ from 2 CPU/4096 MB")
    if not checks["disposable_match"]:
        raise ControllerError("observed Machine is not disposable")
    if not checks["private_match"]:
        raise ControllerError("observed Machine exposes a service")
    if not checks["mount_match"]:
        raise ControllerError("observed Machine does not have the one /work volume")


def machine_response_checks(
    machine: dict[str, Any], args: argparse.Namespace, digest: str, volume_id: str
) -> dict[str, bool]:
    """Project the create response into a closed, identifier-free assertion record."""
    config = machine.get("config", {})
    guest = config.get("guest", {})
    image_ref = machine.get("image_ref", {})
    mounts = config.get("mounts", [])
    return {
        "identity_match": machine.get("region") == args.region
        and image_ref.get("registry") == "registry.fly.io"
        and image_ref.get("repository") == args.app_name
        and image_ref.get("digest") == digest,
        "guest_match": isinstance(guest, dict)
        and guest.get("cpu_kind") == "performance"
        and guest.get("cpus") == CPUS
        and guest.get("memory_mb") == MEMORY_MB,
        "disposable_match": config.get("auto_destroy") is True
        and config.get("restart", {}).get("policy") == "no",
        "private_match": config.get("services") in (None, []),
        "mount_match": len(mounts) == 1
        and mounts[0].get("path") == "/work"
        and mounts[0].get("volume") == volume_id,
    }


def machine_assertion_code(checks: dict[str, bool]) -> str:
    for key, code in (
        ("identity_match", "machine_image_identity_mismatch"),
        ("guest_match", "machine_resource_mismatch"),
        ("disposable_match", "machine_disposable_policy_mismatch"),
        ("private_match", "machine_public_service_mismatch"),
        ("mount_match", "machine_volume_mount_mismatch"),
    ):
        if not checks[key]:
            return code
    return "machine_post_create_assertion_failed"


def create_machine(
    args: argparse.Namespace,
    fly: Flyctl,
    volume_id: str,
    image: str,
    digest: str,
    source_snapshot: str,
    *,
    deadline: float,
) -> dict[str, Any]:
    api = (
        fly.api_json
        if hasattr(fly, "api_json")
        else lambda *a, **kw: Flyctl.api_json(fly, *a, **kw)
    )
    try:
        value = api(
            "POST",
            f"/v1/apps/{args.app_name}/machines",
            data=machine_payload(args, volume_id, image, digest, source_snapshot),
            deadline=deadline,
            operation="machine_create",
        )
    except ProviderRequestError as error:
        if error.details.get("http_class") not in {
            "request_timeout",
            "transient_server",
            "network_error",
            "rate_limited",
            "conflict",
            "malformed_response",
        }:
            raise
        try:
            observed = api(
                "GET",
                f"/v1/apps/{args.app_name}/machines",
                deadline=deadline,
                operation="machine_create_reconcile",
            )
        except Exception:
            raise error from None
        matches = (
            [
                machine
                for machine in observed
                if isinstance(machine, dict) and machine.get("name") == args.machine_name
            ]
            if isinstance(observed, list)
            else []
        )
        if len(matches) != 1:
            raise error
        value = matches[0]
    if not isinstance(value, dict):
        raise ControllerError("Fly Machine create response is malformed")
    return value


def get_machine(
    args: argparse.Namespace,
    fly: Flyctl,
    machine_id: str,
    *,
    deadline: float,
) -> dict[str, Any]:
    """Fetch fresh provider state; never certify invariants from the POST echo."""
    api = (
        fly.api_json
        if hasattr(fly, "api_json")
        else lambda *a, **kw: Flyctl.api_json(fly, *a, **kw)
    )
    value = api(
        "GET",
        f"/v1/apps/{args.app_name}/machines/{machine_id}",
        deadline=deadline,
        operation="machine_fresh_get",
    )
    if not isinstance(value, dict) or value.get("id") != machine_id:
        raise ControllerError("Fly Machine GET did not return the observed Machine")
    return value


def cleanup_owned(
    fly: Flyctl, app: str, machine_id: str | None, volume_id: str | None, app_created: bool
) -> None:
    """Destroy only identifiers observed from resources created in this invocation."""
    deadline = time.monotonic() + CLEANUP_RESERVE_SECONDS
    attempt_failures = []
    if isinstance(fly, Flyctl):
        try:
            fly.auth_token(deadline=deadline, force_refresh=True)
        except (ControllerError, subprocess.SubprocessError):
            attempt_failures.append("teardown credential refresh failed")
    resources = []
    if machine_id:
        resources.append(
            (
                "Machine",
                ["machine", "destroy", machine_id, "--app", app, "--force"],
                ("machines", machine_id),
            )
        )
    if volume_id:
        resources.append(
            (
                "volume",
                ["volumes", "destroy", volume_id, "--app", app, "--yes"],
                ("volumes", volume_id),
            )
        )
    if app_created:
        resources.append(("app", ["apps", "destroy", app, "--yes"], None))
    for resource_index, (label, destroy, probe) in enumerate(resources):
        errors = []
        # Divide the remaining teardown window so one stuck child cannot starve
        # later resources; keep the last minute for the aggregate final proof.
        teardown_end = deadline - 60
        remaining_resources = len(resources) - resource_index
        slice_end = time.monotonic() + max(
            0, (teardown_end - time.monotonic()) / remaining_resources
        )
        for attempt in range(CLEANUP_ATTEMPTS):
            if attempt == 0:
                try:
                    result = fly.run(
                        destroy,
                        check=False,
                        timeout=min(5.0, _cleanup_timeout(slice_end)),
                    )
                    if result.returncode not in (0, 1):
                        errors.append(f"destroy rc={result.returncode}")
                except (subprocess.SubprocessError, ControllerError) as error:
                    errors.append(type(error).__name__)
            absent = False
            try:
                if probe is not None:
                    absent = fly.resource_absent(probe[0], app, probe[1], deadline=slice_end)
                else:
                    apps = fly.json(["apps", "list"], timeout=_cleanup_timeout(slice_end))
                    absent = not any(
                        item.get("Name") == app or item.get("name") == app for item in apps
                    )
            except (
                subprocess.SubprocessError,
                json.JSONDecodeError,
                ControllerError,
            ) as error:
                errors.append(type(error).__name__)
            if absent:
                break
            if attempt > 0:
                try:
                    result = fly.run(
                        destroy,
                        check=False,
                        timeout=min(5.0, _cleanup_timeout(slice_end)),
                    )
                    if result.returncode not in (0, 1):
                        errors.append(f"destroy rc={result.returncode}")
                except (subprocess.SubprocessError, ControllerError) as error:
                    errors.append(type(error).__name__)
            if attempt + 1 < CLEANUP_ATTEMPTS and time.monotonic() < slice_end:
                time.sleep(min(CLEANUP_POLL_SECONDS, max(0, slice_end - time.monotonic())))
            else:
                detail = f" ({'; '.join(errors)})" if errors else ""
                attempt_failures.append(
                    f"owned {label} was not absent after bounded cleanup{detail}"
                )
                break
        else:
            detail = f" ({'; '.join(errors)})" if errors else ""
            attempt_failures.append(f"owned {label} was not absent after bounded cleanup{detail}")
    try:
        verify_absent(fly, app, machine_id, volume_id, app_created, deadline=deadline)
    except (ControllerError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        detail = "; ".join([*attempt_failures, str(error)])
        raise ControllerError(detail) from error


def verify_absent(
    fly: Flyctl,
    app: str,
    machine_id: str | None,
    volume_id: str | None,
    app_created: bool,
    *,
    deadline: float | None = None,
) -> None:
    deadline = deadline if deadline is not None else time.monotonic() + 60
    if isinstance(fly, Flyctl):
        fly.auth_token(deadline=deadline, force_refresh=True)
    failures = []
    if machine_id:
        try:
            if not fly.resource_absent("machines", app, machine_id, deadline=deadline):
                failures.append("owned Machine remains after cleanup")
        except (subprocess.SubprocessError, ControllerError) as error:
            failures.append(f"Machine absence probe failed: {type(error).__name__}")
    if volume_id:
        try:
            if not fly.resource_absent("volumes", app, volume_id, deadline=deadline):
                failures.append("owned volume remains after cleanup")
        except (subprocess.SubprocessError, ControllerError) as error:
            failures.append(f"volume absence probe failed: {type(error).__name__}")
    if app_created:
        try:
            apps = fly.json(["apps", "list"], timeout=_cleanup_timeout(deadline))
            if any(item.get("Name") == app or item.get("name") == app for item in apps):
                failures.append("owned app remains after cleanup")
        except (subprocess.SubprocessError, json.JSONDecodeError) as error:
            failures.append(f"app absence probe failed: {type(error).__name__}")
    if failures:
        raise ControllerError("; ".join(failures))


def _cleanup_timeout(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ControllerError("cleanup deadline exhausted")
    return min(30.0, remaining)


def _bounded_timeout(deadline: float, maximum: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ControllerError("operation deadline exhausted")
    return min(maximum, remaining)


def observed_phase(path: Path) -> str:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "runner"
    candidates = [value.get("phase")] if isinstance(value, dict) else []
    if isinstance(value, list) and value:
        candidates.append(value[-1].get("phase") if isinstance(value[-1], dict) else None)
    return next((phase for phase in candidates if phase in PHASES), "runner")


def observed_status(path: Path) -> dict[str, str]:
    """Read only the closed durable monitor envelope, retaining no provider data."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"status": "running", "phase": "runner"}
    if not isinstance(value, dict) or set(value) - {"schema", "status", "phase", "code"}:
        raise ControllerError("runtime status envelope has unknown fields")
    if value.get("schema") != "graphforge-fly-s20-status/1":
        raise ControllerError("runtime status envelope has invalid schema")
    status = value.get("status")
    phase = value.get("phase")
    if status not in {"running", "success", "failure"} or phase not in PHASES:
        raise ControllerError("runtime status envelope has invalid state")
    result = {"status": status, "phase": phase}
    if status == "failure":
        code = value.get("code")
        if not isinstance(code, str) or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,80}", code):
            raise ControllerError("runtime status envelope has invalid failure code")
        result["code"] = code
    elif "code" in value:
        raise ControllerError("runtime status envelope has unexpected code")
    return result


def terminal_machine_diagnostic(machine: Any, phase: str) -> dict[str, str] | None:
    if (
        not isinstance(machine, dict)
        or set(machine) != {"state", "oom"}
        or not isinstance(machine.get("oom"), bool)
    ):
        raise ControllerError("normalized Machine runtime response is malformed")
    state = machine.get("state")
    if state not in {"stopped", "destroyed"}:
        return None
    code = "machine_oom" if machine["oom"] else "machine_exit"
    return {
        "schema": "graphforge-fly-g500-s20-diagnostic/1",
        "status": "failure",
        "phase": phase,
        "code": code,
    }


def normalize_machine_runtime(machine: Any) -> dict[str, Any]:
    """Reduce an extensible provider response to a closed state/OOM projection."""
    if not isinstance(machine, dict):
        raise ControllerError("Fly Machine runtime response is malformed")
    state = machine.get("state")
    if state not in {
        "created",
        "starting",
        "started",
        "stopping",
        "stopped",
        "suspended",
        "destroying",
        "destroyed",
        "replacing",
    }:
        raise ControllerError("Fly Machine runtime state is unrecognized")
    oom = False
    events = machine.get("events", [])
    if isinstance(events, list):
        for event in events:
            if not isinstance(event, dict):
                continue
            request = event.get("request")
            exit_event = request.get("exit_event") if isinstance(request, dict) else None
            monitor_event = (
                request.get("monitor_event", request.get("MonitorEvent"))
                if isinstance(request, dict)
                else None
            )
            monitor_exit = (
                monitor_event.get("exit_event") if isinstance(monitor_event, dict) else None
            )
            if (isinstance(exit_event, dict) and exit_event.get("oom_killed") is True) or (
                isinstance(monitor_exit, dict) and monitor_exit.get("oom_killed") is True
            ):
                oom = True
    return {"state": state, "oom": oom}


def validate_container_result(value: Any) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) - {"status", "code", "phase"}:
        raise ControllerError("container result has unknown fields")
    if value.get("status") not in {"success", "failure"}:
        raise ControllerError("container result has invalid status")
    diagnostic = {"schema": "graphforge-fly-g500-s20-diagnostic/1", "status": value["status"]}
    if value["status"] == "failure":
        for field in ("code", "phase"):
            item = value.get(field)
            if not isinstance(item, str) or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,80}", item):
                raise ControllerError(f"container failure has invalid {field}")
            diagnostic[field] = item
    return diagnostic


def fetch(
    fly: Flyctl,
    args: argparse.Namespace,
    machine_id: str,
    remote: str,
    local: Path,
    *,
    deadline: float | None = None,
) -> subprocess.CompletedProcess[str]:
    timeout = MONITOR_TRANSFER_TIMEOUT_SECONDS
    if deadline is not None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ControllerError("runtime monitoring deadline exhausted")
        timeout = min(timeout, remaining)
    return fly.run(
        [
            "ssh",
            "sftp",
            "get",
            remote,
            str(local),
            "--app",
            args.app_name,
            "--machine",
            machine_id,
        ],
        check=False,
        timeout=timeout,
    )


def persist_controller_failure(
    args: argparse.Namespace,
    code: str,
    *,
    observed: dict[str, Any] | None = None,
    command_failure: dict[str, Any] | None = None,
    provider_request: dict[str, Any] | None = None,
) -> None:
    """Persist one sanitized controller-side first failure without overwriting it."""
    if args.diagnostic_out.exists():
        return
    diagnostic: dict[str, Any] = {
        "schema": "graphforge-fly-g500-s20-diagnostic/1",
        "status": "failure",
        "phase": "runner",
        "code": code,
    }
    if observed is not None:
        diagnostic["observed_machine"] = observed
    if command_failure is not None:
        diagnostic["command_failure"] = command_failure
    if provider_request is not None:
        validate_provider_request_diagnostic(provider_request)
        diagnostic["provider_request"] = provider_request
    args.diagnostic_out.write_text(json.dumps(diagnostic, indent=2, sort_keys=True) + "\n")


def acknowledge_result(
    fly: Flyctl, args: argparse.Namespace, machine_id: str, deadline: float
) -> bool:
    """Best-effort handoff acknowledgement; validated evidence remains authoritative."""
    remaining = deadline - time.monotonic()
    if remaining < 1.0:
        return False
    try:
        result = fly.run(
            [
                "machine",
                "exec",
                machine_id,
                "--app",
                args.app_name,
                "touch /work/controller-ack",
            ],
            check=False,
            timeout=min(30.0, remaining),
        )
        return result.returncode == 0
    except (ControllerError, subprocess.SubprocessError):
        return False


def execute(
    args: argparse.Namespace,
    fly: Flyctl,
    local_image_id: str | None = None,
    local_snapshot: str | None = None,
    reservation: dict[str, Any] | None = None,
) -> None:
    bootstrap = fly
    run_fly: Flyctl | None = None
    credential: RunCredential | None = None
    app_created = False
    machine_id = volume_id = None
    primary_error: BaseException | None = None
    try:
        if local_image_id is None or local_snapshot is None:
            local_image_id, local_snapshot = inspect_local_image(args.image, args.expected_sha)
        apps = bootstrap.json(["apps", "list"])
        if any(
            item.get("Name") == args.app_name or item.get("name") == args.app_name for item in apps
        ):
            raise ControllerError("refusing to reuse an existing app")
        if reservation is None:
            compute_rate, volume_rate = fetch_current_pricing(args.region)
            reservation = price_reservation(args.volume_size_gb, compute_rate, volume_rate)
        reserve_budget(
            args.ledger,
            args.app_name,
            reservation,
        )
        credential = mint_run_credential(args, bootstrap)
        admit_run_credential(credential, args.timeout_s)
        run_fly = run_scoped_flyctl(credential)
        try:
            create_owned_app(args, run_fly)
            app_created = True
        except OwnedAppCreationError:
            app_created = True
            raise
        image, digest, source_snapshot = publish_to_fly_registry(args, local_image_id, run_fly)
        if source_snapshot != local_snapshot:
            raise ControllerError("Fly registry image source snapshot changed after publication")
        volume = create_owned_volume(args, run_fly)
        volume_id = assert_volume(volume, args)
        deadline = time.monotonic() + args.timeout_s
        created_machine = create_machine(
            args,
            run_fly,
            volume_id,
            image,
            digest,
            source_snapshot,
            deadline=deadline,
        )
        machine_id = created_machine["id"]
        machine = get_machine(args, run_fly, machine_id, deadline=deadline)
        checks = machine_response_checks(machine, args, digest, volume_id)
        try:
            assert_machine(machine, args, digest, volume_id)
        except ControllerError:
            persist_controller_failure(
                args,
                machine_assertion_code(checks),
                observed=checks,
            )
            raise
        with tempfile.TemporaryDirectory(prefix="graphforge-s20-") as directory:
            local = Path(directory) / "evidence.json"
            result_file = Path(directory) / "container-result.json"
            active_file = Path(directory) / "active-phase.json"
            journal_file = Path(directory) / "journal.json"
            last_phase = None
            while time.monotonic() < deadline:
                fetch(
                    run_fly,
                    args,
                    machine_id,
                    ACTIVE_PHASE_PATH,
                    active_file,
                    deadline=deadline,
                )
                status = observed_status(active_file)
                phase = status["phase"]
                if phase != last_phase:
                    fetch(
                        run_fly,
                        args,
                        machine_id,
                        JOURNAL_PATH,
                        journal_file,
                        deadline=deadline,
                    )
                    last_phase = phase
                if status["status"] != "running":
                    fetch(
                        run_fly,
                        args,
                        machine_id,
                        RESULT_PATH,
                        result_file,
                        deadline=deadline,
                    )
                    if result_file.is_file():
                        diagnostic = validate_container_result(json.loads(result_file.read_text()))
                        if diagnostic["status"] == "failure":
                            args.diagnostic_out.write_text(
                                json.dumps(diagnostic, indent=2, sort_keys=True) + "\n"
                            )
                            raise ControllerError(
                                f"container failed in {diagnostic['phase']} with "
                                f"{diagnostic['code']}"
                            )
                    if status["status"] == "success":
                        evidence_result = fetch(
                            run_fly,
                            args,
                            machine_id,
                            EVIDENCE_PATH,
                            local,
                            deadline=deadline,
                        )
                        if evidence_result.returncode == 0 and local.is_file():
                            break
                terminal = terminal_machine_diagnostic(
                    run_fly.machine_runtime(args.app_name, machine_id, deadline=deadline),
                    phase,
                )
                if terminal:
                    args.diagnostic_out.write_text(
                        json.dumps(terminal, indent=2, sort_keys=True) + "\n"
                    )
                    raise ControllerError(
                        f"Machine terminated in {terminal['phase']} with {terminal['code']}"
                    )
                time.sleep(min(MONITOR_INTERVAL_SECONDS, max(0, deadline - time.monotonic())))
            else:
                raise ControllerError("timed out retrieving S20 evidence")
            spec = importlib.util.spec_from_file_location("s20_validator", VALIDATOR)
            if not spec or not spec.loader:
                raise ControllerError("cannot load evidence validator")
            validator = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(validator)
            evidence = json.loads(local.read_text(encoding="utf-8"))
            validator.validate(
                evidence,
                args.expected_sha,
                digest,
                args.region,
                source_snapshot,
            )
            args.evidence_out.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
            acknowledge_result(run_fly, args, machine_id, deadline)
    except BaseException as error:
        primary_error = error
        if isinstance(error, ProviderRequestError):
            persist_controller_failure(args, error.code, provider_request=error.details)
        else:
            persist_controller_failure(args, f"controller_{type(error).__name__.lower()}")
    cleanup_errors: list[Exception] = []
    try:
        cleanup_owned(
            run_fly or bootstrap,
            args.app_name,
            machine_id,
            volume_id,
            app_created,
        )
    except Exception as error:
        cleanup_errors.append(error)
        if run_fly is not None:
            try:
                cleanup_owned(
                    bootstrap,
                    args.app_name,
                    machine_id,
                    volume_id,
                    app_created,
                )
            except Exception as bootstrap_cleanup_error:
                cleanup_errors.append(bootstrap_cleanup_error)
    if credential is not None:
        try:
            revoke_run_credential(bootstrap, credential, args.org)
        except Exception as error:
            cleanup_errors.append(error)
        finally:
            run_fly = None
            credential = None
    if primary_error is not None:
        for cleanup_error in cleanup_errors:
            primary_error.add_note(f"cleanup also failed: {cleanup_error}")
        raise primary_error
    if cleanup_errors:
        raise ControllerError(
            "cleanup failed: " + "; ".join(str(error) for error in cleanup_errors)
        )


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--expected-sha", required=True)
    value.add_argument("--image", required=True)
    value.add_argument("--region", required=True)
    value.add_argument("--org", required=True)
    value.add_argument("--app-name", required=True)
    value.add_argument("--volume-name", required=True)
    value.add_argument("--machine-name", required=True)
    value.add_argument("--volume-size-gb", type=int, default=500)
    value.add_argument("--timeout-s", type=int, default=14_400)
    value.add_argument("--ledger", type=Path, required=True)
    value.add_argument("--evidence-out", type=Path, required=True)
    value.add_argument("--diagnostic-out", type=Path, required=True)
    value.add_argument("--execute", action="store_true")
    value.add_argument("--confirm-disposable", action="store_true")
    return value


def main() -> int:
    args = parser().parse_args()
    try:
        validate_inputs(args)
        check_source(args.expected_sha)
        local_image_id, source_snapshot = inspect_local_image(args.image, args.expected_sha)
        compute_rate, volume_rate = fetch_current_pricing(args.region)
        reservation = price_reservation(args.volume_size_gb, compute_rate, volume_rate)
        plan = {
            "scale": 20,
            "source_sha": args.expected_sha,
            "cpus": CPUS,
            "memory_mb": MEMORY_MB,
            "volume_size_gb": args.volume_size_gb,
            "region": args.region,
            "local_image_id": local_image_id,
            "maximum_total_cost_usd": MAX_COST_USD,
            "reservation": reservation,
        }
        print(json.dumps(plan, sort_keys=True))
        if args.execute:
            execute(
                args,
                Flyctl(),
                local_image_id,
                source_snapshot,
                reservation,
            )
        return 0
    except KeyboardInterrupt as error:
        print("error: interrupted after cleanup", file=sys.stderr)
        for note in getattr(error, "__notes__", ()):
            print(f"error: {note}", file=sys.stderr)
        return 130
    except (ControllerError, subprocess.SubprocessError, json.JSONDecodeError, KeyError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
