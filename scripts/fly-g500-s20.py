#!/usr/bin/env python3
"""Run one disposable, private Fly S20 certification Machine.

Dry-run is the default.  This controller owns infrastructure only; the pinned
image owns the GraphForge lifecycle and writes the sanitized evidence artifact.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from decimal import ROUND_CEILING, Decimal, InvalidOperation
import fcntl
import hashlib
from html.parser import HTMLParser
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
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
CLEANUP_RESERVE_SECONDS = 600
VOLUME_BILLING_HOURS = 5
EVIDENCE_PATH = "/work/s20-evidence.json"
RESULT_PATH = "/work/container-result.json"
JOURNAL_PATH = "/work/s20-journal.json"
ACTIVE_PHASE_PATH = "/work/s20-active-phase.json"
CLEANUP_ATTEMPTS = 60
CLEANUP_POLL_SECONDS = 2.0
AUTH_TOKEN_TTL_SECONDS = 300.0
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


class Flyctl:
    def run(
        self, args: Sequence[str], *, check: bool = True, timeout: float = 120
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["flyctl", *args],
            cwd=ROOT,
            check=check,
            text=True,
            capture_output=True,
            timeout=timeout,
        )

    def json(self, args: Sequence[str], *, timeout: float = 120) -> Any:
        return json.loads(self.run([*args, "--json"], timeout=timeout).stdout)

    def auth_token(
        self, *, deadline: float | None = None, force_refresh: bool = False
    ) -> str:
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

    def api_json(
        self,
        method: str,
        path: str,
        *,
        data: dict[str, Any] | None = None,
        timeout: float = 30,
        deadline: float | None = None,
        absent_ok: bool = False,
    ) -> Any:
        """Call the Machines API, refreshing once on authentication expiry."""
        for attempt in range(2):
            token = self.auth_token(deadline=deadline, force_refresh=attempt == 1)
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
                    return json.loads(body) if body else None
            except urllib.error.HTTPError as error:
                if error.code == 401 and attempt == 0:
                    continue
                if error.code == 404 and absent_ok:
                    return None
                raise ControllerError(f"Fly API returned HTTP {error.code}") from None
            except (urllib.error.URLError, TimeoutError):
                raise ControllerError("Fly API connection failed") from None
        raise ControllerError("Fly API authentication refresh failed")

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
            )
            is None
        )

    def machine_runtime(self, app: str, machine_id: str) -> dict[str, Any]:
        """Read Machine runtime state from the stable authenticated provider API."""
        value = self.api_json(
            "GET", f"/v1/apps/{app}/machines/{machine_id}", absent_ok=True
        )
        if value is None:
            return {"state": "destroyed", "oom": False}
        return normalize_machine_runtime(value)


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
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
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
            timeout=900,
        )
    result = subprocess.run(
        ["docker", "image", "inspect", image],
        env=environment,
        check=True,
        text=True,
        capture_output=True,
        timeout=120,
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
        timeout=120,
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
                timeout=120,
            )
            provenance = json.loads(target.read_text(encoding="utf-8"))
    finally:
        subprocess.run(
            ["docker", "rm", created],
            env=environment,
            check=False,
            text=True,
            capture_output=True,
            timeout=120,
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
        timeout=120,
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


def publish_to_fly_registry(
    args: argparse.Namespace, local_image_id: str, _fly: Flyctl
) -> tuple[str, str, str]:
    """Publish the authenticated local image into the owned app registry."""
    repository = f"registry.fly.io/{args.app_name}"
    tag = f"{repository}:{args.expected_sha}"
    with tempfile.TemporaryDirectory(prefix="graphforge-fly-docker-config-") as config:
        environment = {**os.environ, "DOCKER_CONFIG": config}
        subprocess.run(
            ["flyctl", "auth", "docker"],
            cwd=ROOT,
            env=environment,
            check=True,
            text=True,
            capture_output=True,
            timeout=120,
        )
        subprocess.run(
            ["docker", "tag", local_image_id, tag],
            env=environment,
            check=True,
            text=True,
            capture_output=True,
            timeout=120,
        )
        pushed = subprocess.run(
            ["docker", "push", tag],
            env=environment,
            check=True,
            text=True,
            capture_output=True,
            timeout=1_800,
        )
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
            timeout=120,
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
                "GF_G500_S20_TIMEOUT_SECONDS": str(RUN_SECONDS - RESULT_HANDOFF_SECONDS),
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
) -> dict[str, Any]:
    token = fly.auth_token()
    if not token:
        raise ControllerError("Fly authentication token is unavailable")
    request = urllib.request.Request(
        f"https://api.machines.dev/v1/apps/{args.app_name}/machines",
        data=json.dumps(machine_payload(args, volume_id, image, digest, source_snapshot)).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise ControllerError(
            f"Fly Machines API rejected creation with HTTP {error.code}"
        ) from None
    except urllib.error.URLError:
        raise ControllerError("Fly Machines API connection failed") from None


def get_machine(args: argparse.Namespace, fly: Flyctl, machine_id: str) -> dict[str, Any]:
    """Fetch fresh provider state; never certify invariants from the POST echo."""
    token = (
        fly.auth_token()
        if hasattr(fly, "auth_token")
        else fly.run(["auth", "token"]).stdout.strip()
    )
    if not token:
        raise ControllerError("Fly authentication token is unavailable")
    request = urllib.request.Request(
        f"https://api.machines.dev/v1/apps/{args.app_name}/machines/{machine_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            value = json.load(response)
    except urllib.error.HTTPError as error:
        raise ControllerError(f"Fly Machine GET returned HTTP {error.code}") from None
    except urllib.error.URLError:
        raise ControllerError("Fly Machine GET connection failed") from None
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


def observed_phase(path: Path) -> str:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "runner"
    candidates = [value.get("phase")] if isinstance(value, dict) else []
    if isinstance(value, list) and value:
        candidates.append(value[-1].get("phase") if isinstance(value[-1], dict) else None)
    return next((phase for phase in candidates if phase in PHASES), "runner")


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
            if (
                isinstance(exit_event, dict) and exit_event.get("oom_killed") is True
            ) or (
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
    fly: Flyctl, args: argparse.Namespace, machine_id: str, remote: str, local: Path
) -> subprocess.CompletedProcess[str]:
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
    )


def persist_controller_failure(
    args: argparse.Namespace,
    code: str,
    *,
    observed: dict[str, Any] | None = None,
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
    args.diagnostic_out.write_text(json.dumps(diagnostic, indent=2, sort_keys=True) + "\n")


def execute(
    args: argparse.Namespace,
    fly: Flyctl,
    local_image_id: str | None = None,
    local_snapshot: str | None = None,
) -> None:
    app_created = False
    machine_id = volume_id = None
    primary_error: Exception | None = None
    try:
        if local_image_id is None or local_snapshot is None:
            local_image_id, local_snapshot = inspect_local_image(args.image, args.expected_sha)
        apps = fly.json(["apps", "list"])
        if any(
            item.get("Name") == args.app_name or item.get("name") == args.app_name for item in apps
        ):
            raise ControllerError("refusing to reuse an existing app")
        compute_rate, volume_rate = fetch_current_pricing(args.region)
        reserve_budget(
            args.ledger,
            args.app_name,
            price_reservation(args.volume_size_gb, compute_rate, volume_rate),
        )
        try:
            create_owned_app(args, fly)
            app_created = True
        except OwnedAppCreationError:
            app_created = True
            raise
        image, digest, source_snapshot = publish_to_fly_registry(args, local_image_id, fly)
        if source_snapshot != local_snapshot:
            raise ControllerError("Fly registry image source snapshot changed after publication")
        volume = fly.json(
            [
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
        )
        volume_id = assert_volume(volume, args)
        created_machine = create_machine(args, fly, volume_id, image, digest, source_snapshot)
        machine_id = created_machine["id"]
        machine = get_machine(args, fly, machine_id)
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
        deadline = time.monotonic() + args.timeout_s
        with tempfile.TemporaryDirectory(prefix="graphforge-s20-") as directory:
            local = Path(directory) / "evidence.json"
            result_file = Path(directory) / "container-result.json"
            active_file = Path(directory) / "active-phase.json"
            journal_file = Path(directory) / "journal.json"
            while time.monotonic() < deadline:
                result = fetch(fly, args, machine_id, RESULT_PATH, result_file)
                if result.returncode == 0 and result_file.is_file():
                    diagnostic = validate_container_result(json.loads(result_file.read_text()))
                    if diagnostic["status"] == "failure":
                        args.diagnostic_out.write_text(
                            json.dumps(diagnostic, indent=2, sort_keys=True) + "\n"
                        )
                        raise ControllerError(
                            f"container failed in {diagnostic['phase']} with {diagnostic['code']}"
                        )
                    evidence_result = fetch(fly, args, machine_id, EVIDENCE_PATH, local)
                    if evidence_result.returncode == 0 and local.is_file():
                        break
                # The shell result can be absent when the kernel or provider terminated
                # the process. Retrieve the last durable progress markers before asking
                # Fly for the terminal state so OOM and abrupt exits surface promptly.
                fetch(fly, args, machine_id, ACTIVE_PHASE_PATH, active_file)
                fetch(fly, args, machine_id, JOURNAL_PATH, journal_file)
                phase = observed_phase(active_file)
                if phase == "runner":
                    phase = observed_phase(journal_file)
                terminal = terminal_machine_diagnostic(
                    fly.machine_runtime(args.app_name, machine_id), phase
                )
                if terminal:
                    args.diagnostic_out.write_text(
                        json.dumps(terminal, indent=2, sort_keys=True) + "\n"
                    )
                    raise ControllerError(
                        f"Machine terminated in {terminal['phase']} with {terminal['code']}"
                    )
                time.sleep(2)
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
            fly.run(
                [
                    "machine",
                    "exec",
                    machine_id,
                    "--app",
                    args.app_name,
                    "touch /work/controller-ack",
                ],
                check=False,
            )
    except Exception as error:
        primary_error = error
        persist_controller_failure(args, f"controller_{type(error).__name__.lower()}")
    cleanup_error: Exception | None = None
    try:
        cleanup_owned(fly, args.app_name, machine_id, volume_id, app_created)
    except Exception as error:
        cleanup_error = error
    if primary_error is not None:
        if cleanup_error is not None:
            primary_error.add_note(f"cleanup also failed: {cleanup_error}")
        raise primary_error
    if cleanup_error is not None:
        raise cleanup_error


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
        plan = {
            "scale": 20,
            "source_sha": args.expected_sha,
            "cpus": CPUS,
            "memory_mb": MEMORY_MB,
            "volume_size_gb": args.volume_size_gb,
            "region": args.region,
            "local_image_id": local_image_id,
            "maximum_total_cost_usd": MAX_COST_USD,
            "reservation": price_reservation(args.volume_size_gb),
        }
        print(json.dumps(plan, sort_keys=True))
        if args.execute:
            execute(args, Flyctl(), local_image_id, source_snapshot)
        return 0
    except (ControllerError, subprocess.SubprocessError, json.JSONDecodeError, KeyError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    import sys

    raise SystemExit(main())
