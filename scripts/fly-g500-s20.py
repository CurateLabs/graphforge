#!/usr/bin/env python3
"""Plan or run one qualification-sized disposable Fly S20 Machine."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import subprocess
import tempfile
import time
from typing import Any
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
PRICING_URL = "https://fly.io/docs/about/pricing/"
SHA = re.compile(r"^[0-9a-f]{40}$")
CHILD_IMAGE = re.compile(r"^[^\s@]+@(?P<digest>sha256:[0-9a-f]{64})$")
SAFE_NAME = re.compile(r"^[a-z][a-z0-9-]{2,62}$")
SAFE_VOLUME = re.compile(r"^[a-z][a-z0-9_]{0,29}$")
PHASES = [
    "preflight",
    "generate",
    "ingest",
    "csr",
    "source_reopen",
    "source_query_1hop",
    "source_query_2hop",
    "export",
    "verify",
    "import",
    "imported_reopen",
    "imported_query_1hop",
    "imported_query_2hop",
    "drill_corruption",
    "drill_cancellation",
    "drill_resource_limit",
    "drill_interrupted_finalization",
]
# Phase ceilings are operational stop conditions, not performance pass criteria.
# They include measured Fly S20 headroom and observed shared-host I/O variance
# while ensuring a bad workload or plan cannot consume the entire four-hour
# certification envelope without a useful typed diagnosis. The outer hard TTL
# remains authoritative.
PHASE_TIMEOUT_S = {
    "preflight": 15 * 60,
    "generate": 15 * 60,
    "ingest": 90 * 60,
    "csr": 20 * 60,
    "source_reopen": 15 * 60,
    "source_query_1hop": 15 * 60,
    "source_query_2hop": 15 * 60,
    "export": 45 * 60,
    "verify": 30 * 60,
    "import": 90 * 60,
    "imported_reopen": 15 * 60,
    "imported_query_1hop": 15 * 60,
    "imported_query_2hop": 15 * 60,
    "drill_corruption": 15 * 60,
    "drill_cancellation": 15 * 60,
    "drill_resource_limit": 15 * 60,
    "drill_interrupted_finalization": 15 * 60,
}
POLL_INTERVAL_S = 15
HEARTBEAT_INTERVAL_S = 60
HARD_TTL_S = 4 * 3600
CLEANUP_TTL_S = 10 * 60
MAX_MEMORY_MB = 128 * 1024
MAX_VOLUME_GB = 500
MIN_MEMORY_HEADROOM_BYTES = 512 * 1024 * 1024
MEMORY_HEADROOM_RATIO = 1.25
VOLUME_HEADROOM_RATIO = 1.25
MAX_DIAGNOSTIC_EVENTS = 20
SENSITIVE_NORMALIZED_KEYS = {
    "authorization",
    "credential",
    "credentials",
    "password",
    "passwordhint",
    "secret",
    "token",
    "apitoken",
    "accesstoken",
    "flyapitoken",
    "clientsecret",
    "cookie",
    "setcookie",
}
BEARER_VALUE = re.compile(r"(?:^|\s)bearer\s+\S+", re.IGNORECASE)
REQUIRED_IMAGE_CONTRACT = "graphforge-s20-runtime/2"
REQUIRED_MEASUREMENT_CONTRACT = "graphforge-s20-phase-measurement/1"
REQUIRED_CONSTRUCTION_CONTRACT = "graphforge-storage-construction-session/1"
DIAGNOSTIC_EVENT_KEYS = {
    "created_at",
    "exit_code",
    "oom_killed",
    "requested_stop",
    "signal",
    "source",
    "status",
    "timestamp",
    "type",
    "updated_at",
}


class ControllerError(RuntimeError):
    pass


def emit_progress(event: str, **fields: Any) -> None:
    """Emit one sanitized, machine-readable operator update."""
    print(json.dumps({"event": event, **fields}, sort_keys=True), flush=True)


def sensitive_key(key: Any) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", str(key).casefold())
    return (
        normalized in SENSITIVE_NORMALIZED_KEYS
        or normalized.endswith("token")
        or normalized.endswith("secret")
        or normalized.startswith("authorization")
        or normalized.startswith("cookie")
        or normalized.endswith("cookie")
    )


def sanitize_artifact(value: Any, *, depth: int = 0) -> Any:
    """Bound diagnostic artifacts and redact credential-shaped fields."""
    if depth > 12:
        return "<maximum-depth>"
    if isinstance(value, dict):
        return {
            str(key): "<redacted>"
            if sensitive_key(key)
            else sanitize_artifact(item, depth=depth + 1)
            for key, item in list(value.items())[:1000]
        }
    if isinstance(value, list):
        return [sanitize_artifact(item, depth=depth + 1) for item in value[:1000]]
    if isinstance(value, str):
        return "<redacted>" if BEARER_VALUE.search(value) else value[:4096]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return "<unsupported-value>"


def write_sanitized_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(sanitize_artifact(value), indent=2, sort_keys=True) + "\n")


def journal_progress(journal: Any) -> tuple[int, str | None]:
    """Validate an atomic journal snapshot and identify the active phase."""
    if not isinstance(journal, list) or len(journal) > len(PHASES):
        raise ControllerError("journal_invalid invalid phase collection")
    for index, phase in enumerate(journal):
        if not isinstance(phase, dict) or phase.get("id") != PHASES[index]:
            raise ControllerError("journal_invalid phases are not the required ordered prefix")
        status = phase.get("status")
        if status == "fail":
            code = phase.get("failure_code") or "operation_failed"
            raise ControllerError(f"phase_failed phase={PHASES[index]} failure_code={code}")
        if status != "pass":
            raise ControllerError("journal_invalid completed phase has unknown status")
    active = PHASES[len(journal)] if len(journal) < len(PHASES) else None
    return len(journal), active


class Flyctl:
    def run(
        self, args: Sequence[str], *, check: bool = True, timeout: float = 120
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["flyctl", *args],
            cwd=ROOT,
            check=check,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def json(self, args: Sequence[str], *, timeout: float = 120) -> Any:
        return json.loads(self.run([*args, "--json"], timeout=timeout).stdout)


def fetch_pricing() -> str:
    request = urllib.request.Request(
        PRICING_URL, headers={"User-Agent": "graphforge-s20-controller/1"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.geturl() != PRICING_URL:
            raise ControllerError("official pricing request redirected")
        return response.read().decode("utf-8")


def parse_live_rates(
    html: str, region: str, machine: str, cpus: int, memory_mb: int
) -> dict[str, float]:
    matrix = re.search(
        rf'id="started-machines-pricing-matrix-{re.escape(region)}".*?</table>',
        html,
        re.DOTALL,
    )
    if not matrix:
        raise ControllerError(f"official pricing has no region {region}")
    if memory_mb % 1024:
        raise ControllerError("selected Machine memory must be whole GiB for price verification")
    row = re.search(
        rf"{re.escape(machine)}.*?{cpus} performance.*?{memory_mb // 1024}GB.*?"
        r"\$(?P<second>[0-9.]+).*?\$(?P<hour>[0-9.]+)",
        matrix.group(),
        re.DOTALL,
    )
    volume = re.search(r"\$(?P<rate>[0-9.]+)/GB per month of provisioned capacity", html)
    if not row or not volume:
        raise ControllerError("official pricing format did not contain required live rates")
    per_second = float(row.group("second"))
    per_hour = float(row.group("hour"))
    if abs(per_second * 3600 - per_hour) > 0.001:
        raise ControllerError("official per-second/hour compute rates disagree")
    return {"compute_per_hour_usd": per_hour, "volume_gb_month_usd": float(volume.group("rate"))}


def cost_plan(
    rates: dict[str, float],
    ceiling: float,
    reserve: float,
    volume_gb: int,
    qualification_reserved_usd: float = 0.0,
) -> dict[str, float]:
    if not math.isfinite(qualification_reserved_usd) or qualification_reserved_usd < 0:
        raise ControllerError("qualification reservation must be finite and nonnegative")
    billed_hours = (HARD_TTL_S + CLEANUP_TTL_S) / 3600
    compute = rates["compute_per_hour_usd"] * billed_hours
    volume = rates["volume_gb_month_usd"] * volume_gb * billed_hours / (30 * 24)
    projected = qualification_reserved_usd + compute + volume + reserve
    if projected > ceiling:
        raise ControllerError(f"projected maximum ${projected:.4f} exceeds ${ceiling:.2f} ceiling")
    return {
        "compute_usd": compute,
        "volume_usd": volume,
        "qualification_reserved_usd": qualification_reserved_usd,
        "unpriced_reserve_usd": reserve,
        "projected_max_usd": projected,
        "ceiling_usd": ceiling,
    }


def load_qualification(path: Path, digest: str, region: str) -> dict[str, Any]:
    """Validate lower-rung observations and derive the smallest safe resources."""
    value = json.loads(path.read_text())
    if value.get("schema") != "graphforge-fly-s20-qualification/1":
        raise ControllerError("qualification evidence has an unexpected schema")
    if value.get("region") != region or value.get("image_digest") != digest:
        raise ControllerError("qualification region/image differs from the planned run")
    volume = value.get("volume")
    if not isinstance(volume, dict) or set(volume) != {
        "provider",
        "class",
        "mount_path",
        "size_gb",
    }:
        raise ControllerError("qualification lacks its exact volume binding")
    if (
        volume.get("provider") != "fly.io"
        or volume.get("class") != "attached-volume"
        or volume.get("mount_path") != "/work"
        or type(volume.get("size_gb")) is not int
        or not 1 <= volume["size_gb"] <= MAX_VOLUME_GB
    ):
        raise ControllerError("qualification volume binding is unsupported")
    cost = value.get("cost_admission")
    required_cost_keys = {
        "authority",
        "ceiling_usd",
        "reserve_usd",
        "reserved_max_usd",
        "reported_cost_usd",
        "candidate_rate_snapshot",
        "attempts",
    }
    if not isinstance(cost, dict) or set(cost) != required_cost_keys:
        raise ControllerError("qualification lacks controller-owned cost admission")
    reserved_cost = cost.get("reserved_max_usd")
    reported_cost = cost.get("reported_cost_usd")
    reserve_cost = cost.get("reserve_usd")
    if (
        cost.get("authority") != "controller-reserved-exposure/1"
        or cost.get("ceiling_usd") != 10.0
        or type(reserve_cost) not in (int, float)
        or reserve_cost < 1.0
        or type(reserved_cost) not in (int, float)
        or not math.isfinite(reserved_cost)
        or not 0 < reserved_cost <= 10.0 - reserve_cost
        or type(reported_cost) not in (int, float)
        or not math.isfinite(reported_cost)
        or not 0 <= reported_cost <= reserved_cost
    ):
        raise ControllerError("qualification cost admission is invalid")
    snapshots = cost.get("candidate_rate_snapshot")
    attempts = cost.get("attempts")
    if not isinstance(snapshots, list) or not snapshots or not isinstance(attempts, list):
        raise ControllerError("qualification cost admission lacks rates or attempts")
    rates: dict[str, float] = {}
    for snapshot in snapshots:
        if not isinstance(snapshot, dict) or set(snapshot) != {
            "machine",
            "max_usd_per_observation",
        }:
            raise ControllerError("qualification cost rate snapshot is malformed")
        machine_name = snapshot.get("machine")
        maximum = snapshot.get("max_usd_per_observation")
        if (
            not isinstance(machine_name, str)
            or machine_name in rates
            or type(maximum) not in (int, float)
            or not math.isfinite(maximum)
            or maximum <= 0
        ):
            raise ControllerError("qualification cost rate snapshot is invalid")
        rates[machine_name] = float(maximum)
    summed_reserved = 0.0
    summed_reported = 0.0
    for attempt in attempts:
        if not isinstance(attempt, dict) or set(attempt) != {
            "machine",
            "scale",
            "reserved_max_usd",
            "reported_cost_usd",
            "reserved_at",
            "completed_at",
            "result",
        }:
            raise ControllerError("qualification cost attempt is malformed")
        maximum = attempt.get("reserved_max_usd")
        observed = attempt.get("reported_cost_usd")
        machine_name = attempt.get("machine")
        try:
            started = datetime.fromisoformat(attempt["reserved_at"])
            completed = datetime.fromisoformat(attempt["completed_at"])
        except (KeyError, TypeError, ValueError):
            raise ControllerError("qualification cost attempt timestamp is invalid") from None
        if (
            started.tzinfo is None
            or completed.tzinfo is None
            or completed < started
            or machine_name not in rates
            or maximum != rates[machine_name]
            or type(observed) not in (int, float)
            or not math.isfinite(observed)
            or not 0 <= observed <= maximum
            or attempt.get("scale") not in (18, 19)
            or attempt.get("result") not in ("pass", "capacity_exceeded")
        ):
            raise ControllerError("qualification cost attempt is invalid")
        summed_reserved += maximum
        summed_reported += observed
    if not math.isclose(summed_reserved, reserved_cost) or not math.isclose(
        summed_reported, reported_cost
    ):
        raise ControllerError("qualification cost totals do not match attempts")
    rungs = value.get("rungs")
    candidates = value.get("machine_candidates")
    if not isinstance(rungs, list) or len(rungs) < 2 or not isinstance(candidates, list):
        raise ControllerError("qualification requires at least two rungs and Machine candidates")
    scales: list[int] = []
    physical_peaks: list[int] = []
    phase_peaks: dict[str, list[int]] = {}
    projected_disk = 0
    common_budgets: dict[str, Any] | None = None
    qualified_runtime: dict[str, Any] | None = None
    construction_observations: list[dict[str, int]] = []
    for rung in rungs:
        if not isinstance(rung, dict) or rung.get("result") != "pass":
            raise ControllerError("qualification contains a non-pass rung")
        scale = rung.get("scale")
        phases = rung.get("phases")
        if not isinstance(scale, int) or not isinstance(phases, list) or not phases:
            raise ControllerError("qualification rung is incomplete")
        scales.append(scale)
        budgets = rung.get("budgets")
        runtime = rung.get("runtime")
        construction = rung.get("construction")
        if not isinstance(budgets, dict) or not budgets or any(
            not isinstance(value, int) or value <= 0 for value in budgets.values()
        ):
            raise ControllerError("qualification lacks positive identical operator budgets")
        if common_budgets is None:
            common_budgets = budgets
        elif budgets != common_budgets:
            raise ControllerError("qualification rungs used different operator budgets")
        if not isinstance(runtime, dict) or not all(
            isinstance(runtime.get(key), (str, int))
            for key in ("machine", "cpus", "memory_mb")
        ):
            raise ControllerError("qualification lacks its observed Machine runtime")
        if qualified_runtime is None:
            qualified_runtime = runtime
        elif runtime != qualified_runtime:
            raise ControllerError("qualification rungs used different Machine resources")
        if construction != {
            "contract": REQUIRED_CONSTRUCTION_CONTRACT,
            "source_current_transitions": 1,
            "import_current_transitions": 1,
        }:
            raise ControllerError("qualification did not use one storage construction session")
        physical_peak = rung.get("physical_volume_peak_bytes")
        projection = rung.get("s20_projected_physical_peak_bytes")
        if not isinstance(physical_peak, int) or physical_peak <= 0:
            raise ControllerError("qualification lacks physical volume peak")
        if not isinstance(projection, int) or projection < physical_peak:
            raise ControllerError("qualification lacks a defensible S20 disk projection")
        projected_disk = max(projected_disk, projection)
        physical_peaks.append(physical_peak)
        if [phase.get("id") for phase in phases if isinstance(phase, dict)] != PHASES:
            raise ControllerError("qualification must measure the exact lifecycle phase set")
        for phase in phases:
            if not isinstance(phase, dict) or not isinstance(phase.get("id"), str):
                raise ControllerError("qualification phase is malformed")
            memory = phase.get("memory")
            io = phase.get("io")
            if not isinstance(memory, dict) or not all(
                isinstance(memory.get(key), int) and memory[key] >= 0
                for key in (
                    "cgroup_current_before_bytes",
                    "cgroup_peak_bytes",
                    "cgroup_current_after_bytes",
                    "smaps_rss_bytes",
                    "smaps_anon_bytes",
                    "smaps_file_bytes",
                )
            ):
                raise ControllerError("qualification lacks phase-local cgroup/smaps evidence")
            if memory.get("peak_authority") != "sampled_cgroup_memory.current/250ms":
                raise ControllerError("qualification used an unsupported phase-memory authority")
            if not isinstance(io, dict) or not all(
                isinstance(io.get(key), int) and io[key] >= 0
                for key in (
                    "read_bytes",
                    "write_bytes",
                    "read_syscalls",
                    "write_syscalls",
                    "blocks",
                    "batches",
                    "shards",
                    "topology_rows",
                )
            ):
                raise ControllerError("qualification lacks block/batch/shard I/O evidence")
            if sum(io[key] for key in ("read_bytes", "write_bytes", "read_syscalls", "write_syscalls")) <= 0:
                raise ControllerError("qualification contains an unobserved I/O phase")
            elapsed_ms = phase.get("elapsed_ms")
            if not isinstance(elapsed_ms, int) or elapsed_ms <= 0:
                raise ControllerError("qualification phase lacks positive elapsed time")
            peak = memory["cgroup_peak_bytes"]
            if peak < max(
                memory["cgroup_current_before_bytes"],
                memory["cgroup_current_after_bytes"],
                memory["smaps_rss_bytes"],
            ) or memory["smaps_anon_bytes"] + memory["smaps_file_bytes"] > memory["smaps_rss_bytes"]:
                raise ControllerError("qualification memory counters are internally inconsistent")
            phase_peaks.setdefault(phase["id"], []).append(memory["cgroup_peak_bytes"])
            if phase["id"] in ("ingest", "import"):
                if any(io[key] <= 0 for key in ("blocks", "batches", "shards", "topology_rows")):
                    raise ControllerError("qualification construction I/O must be observed and nonzero")
                if io["read_syscalls"] + io["write_syscalls"] >= io["topology_rows"]:
                    raise ControllerError("qualification construction performs per-row I/O")
                if io["read_syscalls"] + io["write_syscalls"] <= 0:
                    raise ControllerError("qualification construction syscall counters are unobserved")
                construction_observations.append(
                    {
                        key: io[key]
                        for key in (
                            "read_syscalls",
                            "write_syscalls",
                            "blocks",
                            "batches",
                            "shards",
                            "topology_rows",
                        )
                    }
                )
    if scales != sorted(set(scales)):
        raise ControllerError("qualification rungs must be unique and increasing")
    if scales[-2:] != [18, 19]:
        raise ControllerError("qualification requires adjacent S18 and S19 observations")
    if physical_peaks != sorted(physical_peaks):
        raise ControllerError("qualification physical peaks regress across rungs")
    # Two construction phases per rung, in exact lifecycle order. Raw counters
    # must grow with rows at a bounded density, rejecting zero/fabricated and
    # pair-at-a-time I/O before any paid resource is created.
    adjacent_observations = construction_observations[-4:]
    for phase_offset in range(2):
        earlier = adjacent_observations[phase_offset]
        later = adjacent_observations[phase_offset + 2]
        if later["topology_rows"] <= earlier["topology_rows"]:
            raise ControllerError("qualification construction rows did not grow across rungs")
        row_growth = later["topology_rows"] / earlier["topology_rows"]
        sys_growth = (
            later["read_syscalls"] + later["write_syscalls"]
        ) / (earlier["read_syscalls"] + earlier["write_syscalls"])
        if sys_growth > row_growth * 1.25:
            raise ControllerError("qualification syscall growth exceeds topology growth")
        for key in ("blocks", "batches", "shards"):
            density_ratio = (later[key] / later["topology_rows"]) / (
                earlier[key] / earlier["topology_rows"]
            )
            if not 0.5 <= density_ratio <= 2.0:
                raise ControllerError(
                    f"qualification {key} do not scale linearly with topology rows"
                )
    linear_edge_projection = physical_peaks[-1] * (1 << max(0, 20 - scales[-1]))
    if projected_disk < linear_edge_projection:
        raise ControllerError("S20 disk projection is below the observed linear edge bound")
    plateau_ratio = value.get("max_phase_rss_growth_ratio", 1.20)
    if not isinstance(plateau_ratio, (int, float)) or not 1.0 <= plateau_ratio <= 1.5:
        raise ControllerError("qualification RSS plateau bound is invalid")
    for phase, peaks in phase_peaks.items():
        if len(peaks) != len(rungs):
            raise ControllerError(f"qualification phase {phase} is absent from a rung")
        if peaks[-1] > max(peaks[:-1]) * plateau_ratio:
            raise ControllerError(f"qualification RSS does not plateau for phase {phase}")
    peak_rss = max(max(peaks) for peaks in phase_peaks.values())
    required_memory = max(
        peak_rss + MIN_MEMORY_HEADROOM_BYTES,
        int(peak_rss * MEMORY_HEADROOM_RATIO),
    )
    valid_candidates = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        name = candidate.get("name")
        cpus = candidate.get("cpus")
        memory_mb = candidate.get("memory_mb")
        if (
            isinstance(name, str)
            and SAFE_NAME.fullmatch(name)
            and isinstance(cpus, int)
            and cpus > 0
            and isinstance(memory_mb, int)
            and 0 < memory_mb <= MAX_MEMORY_MB
        ):
            valid_candidates.append((memory_mb, cpus, name))
    valid_candidates.sort()
    selected = next(
        (
            candidate
            for candidate in valid_candidates
            if candidate[0] * 1024 * 1024 >= required_memory
        ),
        None,
    )
    if selected is None:
        raise ControllerError("qualification requires more than the 128 GiB certification ceiling")
    required_volume_gb = (
        int(projected_disk * VOLUME_HEADROOM_RATIO) + (1 << 30) - 1
    ) // (1 << 30)
    if not 1 <= required_volume_gb <= MAX_VOLUME_GB:
        raise ControllerError("qualification exceeds Fly's 500 GB volume envelope")
    volume_gb = volume["size_gb"]
    if volume_gb < required_volume_gb:
        raise ControllerError("qualification volume lacks projected S20 headroom")
    memory_mb, cpus, machine = selected
    if qualified_runtime != {"machine": machine, "cpus": cpus, "memory_mb": memory_mb}:
        raise ControllerError("selected Machine was not the Machine measured by qualification")
    if [
        (attempt.get("machine"), attempt.get("scale"), attempt.get("result"))
        for attempt in attempts[-2:]
    ] != [(machine, 18, "pass"), (machine, 19, "pass")]:
        raise ControllerError("qualification cost attempts do not bind the successful rung pair")
    return {
        "region": region,
        "image_digest": digest,
        "machine": machine,
        "cpus": cpus,
        "memory_mb": memory_mb,
        "volume_gb": volume_gb,
        "qualified_peak_rss_bytes": peak_rss,
        "projected_physical_peak_bytes": projected_disk,
        "rung_scales": scales,
        "max_phase_rss_growth_ratio": float(plateau_ratio),
        "construction_io_gate": "pass",
        "qualification_reserved_cost_usd": float(reserved_cost),
        "qualification_reported_cost_usd": float(reported_cost),
    }


def validate_args(args: argparse.Namespace) -> str:
    image = CHILD_IMAGE.fullmatch(args.image)
    if not image:
        raise ControllerError("--image must pin one immutable platform child @sha256 digest")
    if not SHA.fullmatch(args.expected_sha):
        raise ControllerError("--expected-sha must be exact lowercase 40-hex")
    if args.region != "dfw":
        raise ControllerError("S20 comparison region is fixed to dfw")
    if any(not SAFE_NAME.fullmatch(value) for value in (args.app_name, args.machine_name)):
        raise ControllerError("unsafe app or Machine name")
    if not SAFE_VOLUME.fullmatch(args.volume_name):
        raise ControllerError("unsafe volume name")
    if args.ceiling_usd != 10.0 or args.unpriced_reserve_usd < 1.0:
        raise ControllerError("controller requires the approved $10 ceiling and >=$1 reserve")
    if args.execute and (
        not args.confirm_disposable
        or args.pricing_html
        or args.manifest_json
        or args.image_contract_json
    ):
        raise ControllerError("execution requires confirmation and live official pricing")
    if (
        not args.evidence_out.parent.is_dir()
        or not args.journal_out.parent.is_dir()
        or not args.diagnostic_out.parent.is_dir()
    ):
        raise ControllerError("local output parents must already exist")
    return image.group("digest")


def check_source(expected_sha: str) -> None:
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if head != expected_sha or dirty:
        raise ControllerError("execution requires the exact clean checked-out source SHA")


def assert_platform_child(
    image: str,
    expected_sha: str,
    manifest_json: str | None = None,
    image_contract_json: str | None = None,
) -> None:
    if manifest_json is None:
        result = subprocess.run(
            ["docker", "buildx", "imagetools", "inspect", "--raw", image],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
        manifest_json = result.stdout
    manifest = json.loads(manifest_json)
    if "manifests" in manifest:
        raise ControllerError("image digest identifies an OCI index, not a platform child")
    media_type = manifest.get("mediaType", "")
    if "manifest" not in media_type:
        raise ControllerError("image digest did not resolve to an OCI/Docker child manifest")
    if image_contract_json is None:
        result = subprocess.run(
            [
                "docker",
                "buildx",
                "imagetools",
                "inspect",
                "--format",
                "{{json .Image}}",
                image,
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=120,
        )
        image_contract_json = result.stdout
    config = json.loads(image_contract_json)
    labels = config.get("config", {}).get("Labels", {})
    if config.get("architecture") != "amd64" or config.get("os") != "linux":
        raise ControllerError("image child is not linux/amd64")
    expected = {
        "org.opencontainers.image.revision": expected_sha,
        "dev.graphforge.s20.runtime": REQUIRED_IMAGE_CONTRACT,
        "dev.graphforge.s20.measurement": REQUIRED_MEASUREMENT_CONTRACT,
        "dev.graphforge.s20.construction": REQUIRED_CONSTRUCTION_CONTRACT,
    }
    if not isinstance(labels, dict) or any(labels.get(key) != value for key, value in expected.items()):
        raise ControllerError("image lacks the exact S20 measurement/construction contract")


def machine_payload(
    args: argparse.Namespace, volume_id: str, resources: dict[str, Any]
) -> dict[str, Any]:
    return {
        "name": args.machine_name,
        "region": args.region,
        "skip_launch": False,
        "skip_service_registration": True,
        "config": {
            "image": args.image,
            "auto_destroy": True,
            "restart": {"policy": "no"},
            "guest": {
                "cpu_kind": "performance",
                "cpus": resources["cpus"],
                "memory_mb": resources["memory_mb"],
            },
            "mounts": [{"volume": volume_id, "path": "/work"}],
            "services": [],
            "env": {
                "GF_G500_S20_EXPECTED_SHA": args.expected_sha,
                "GF_G500_S20_REGION": args.region,
                "GF_G500_S20_IMAGE_DIGEST": resources["image_digest"],
                "GF_G500_S20_MACHINE": resources["machine"],
                "GF_G500_S20_CPUS": str(resources["cpus"]),
                "GF_G500_S20_MEMORY_MB": str(resources["memory_mb"]),
                "GF_G500_S20_VOLUME_GB": str(resources["volume_gb"]),
                "GF_G500_S20_PUBLIC_SERVICES": "0",
                "GF_G500_S20_RESTART": "no",
            },
        },
    }


def create_machine(
    args: argparse.Namespace,
    fly: Flyctl,
    volume_id: str,
    resources: dict[str, Any],
) -> dict[str, Any]:
    token = fly.run(["auth", "token"]).stdout.strip()
    if not token:
        raise ControllerError("Fly authentication token is unavailable")
    request = urllib.request.Request(
        f"https://api.machines.dev/v1/apps/{args.app_name}/machines",
        data=json.dumps(machine_payload(args, volume_id, resources)).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.load(response)
    except (urllib.error.HTTPError, urllib.error.URLError):
        raise ControllerError("Fly Machines API rejected creation") from None


def assert_machine(
    machine: dict[str, Any],
    args: argparse.Namespace,
    digest: str,
    resources: dict[str, Any],
) -> None:
    config = machine.get("config", {})
    guest = config.get("guest", {})
    if machine.get("region") != args.region or machine.get("image_ref", {}).get("digest") != digest:
        raise ControllerError("observed region or child image digest differs from plan")
    if config.get("auto_destroy") is not True or config.get("restart") != {"policy": "no"}:
        raise ControllerError("observed Machine is not disposable")
    if config.get("services") not in (None, []) or guest != {
        "cpu_kind": "performance",
        "cpus": resources["cpus"],
        "memory_mb": resources["memory_mb"],
    }:
        raise ControllerError("observed Machine resources/services differ from plan")
    mounts = config.get("mounts", [])
    if len(mounts) != 1 or mounts[0].get("path") != "/work":
        raise ControllerError("observed work-root volume differs from plan")


def validate_phase_measurement(phase: dict[str, Any]) -> None:
    memory = phase.get("memory")
    io = phase.get("io")
    filesystem = phase.get("filesystem")
    if not isinstance(memory, dict) or not all(
        isinstance(memory.get(key), int) and memory[key] >= 0
        for key in (
            "cgroup_current_before_bytes",
            "cgroup_peak_bytes",
            "cgroup_current_after_bytes",
            "smaps_rss_before_bytes",
            "smaps_rss_after_bytes",
            "smaps_anon_before_bytes",
            "smaps_anon_after_bytes",
            "smaps_file_before_bytes",
            "smaps_file_after_bytes",
        )
    ):
        raise ControllerError("S20 phase lacks boundary cgroup/smaps memory evidence")
    if memory.get("peak_authority") != "sampled_cgroup_memory.current/250ms":
        raise ControllerError("S20 phase lacks sampled cgroup phase-peak authority")
    if not isinstance(io, dict) or not all(
        isinstance(io.get(key), int) and io[key] >= 0
        for key in (
            "proc_read_bytes",
            "proc_write_bytes",
            "proc_read_syscalls",
            "proc_write_syscalls",
            "storage_sequential_bytes",
            "storage_blocks",
            "arrow_batches",
            "max_arrow_batch_rows",
            "shards",
            "row_groups",
            "random_seeks",
            "fsyncs",
            "topology_rows",
        )
    ):
        raise ControllerError("S20 phase lacks process and storage I/O evidence")
    if not isinstance(filesystem, dict) or not all(
        isinstance(filesystem.get(key), int) and filesystem[key] >= 0
        for key in (
            "total_bytes",
            "free_before_bytes",
            "free_after_bytes",
            "available_before_bytes",
            "available_after_bytes",
            "allocated_before_bytes",
            "allocated_after_bytes",
        )
    ):
        raise ControllerError("S20 phase lacks statvfs/allocated-volume boundaries")
    memory_limit = phase.get("memory_limit_bytes")
    if (
        not isinstance(memory_limit, int)
        or memory_limit <= 0
        or memory["cgroup_peak_bytes"] > memory_limit
        or memory["cgroup_peak_bytes"]
        < max(memory["cgroup_current_before_bytes"], memory["cgroup_current_after_bytes"])
        or memory["smaps_anon_before_bytes"] + memory["smaps_file_before_bytes"]
        > memory["smaps_rss_before_bytes"]
        or memory["smaps_anon_after_bytes"] + memory["smaps_file_after_bytes"]
        > memory["smaps_rss_after_bytes"]
    ):
        raise ControllerError("S20 phase memory evidence is inconsistent")
    if (
        filesystem["total_bytes"] <= 0
        or max(
            filesystem["free_before_bytes"],
            filesystem["free_after_bytes"],
            filesystem["available_before_bytes"],
            filesystem["available_after_bytes"],
        )
        > filesystem["total_bytes"]
        or io["max_arrow_batch_rows"] > 65_536
        or io["random_seeks"] > io["storage_blocks"]
    ):
        raise ControllerError("S20 phase filesystem/I/O evidence is inconsistent")


def validate_evidence(
    evidence: dict[str, Any],
    journal: list[dict[str, Any]],
    sha: str,
    resources: dict[str, Any] | None = None,
) -> None:
    if evidence.get("schema") != "graphforge-s20-integrated-lifecycle-evidence/1":
        raise ControllerError("unexpected S20 evidence schema")
    if evidence.get("git_sha") != sha or evidence.get("result") != "pass":
        raise ControllerError("S20 evidence SHA/result mismatch")
    if evidence.get("measurement_contract") != REQUIRED_MEASUREMENT_CONTRACT or evidence.get(
        "construction_contract"
    ) != REQUIRED_CONSTRUCTION_CONTRACT:
        raise ControllerError("S20 evidence lacks the executable measurement/construction contract")
    if resources is not None and evidence.get("resource_gates") != {
        "rss_plateau": "pass",
        "disk_headroom": "pass",
        "construction_io": "pass",
    }:
        raise ControllerError("S20 evidence lacks the computed qualification resource gates")
    lifecycle = evidence.get("lifecycle", {})
    observed = [phase.get("id") for phase in lifecycle.get("phases", [])]
    if observed != PHASES or [phase.get("id") for phase in journal] != PHASES:
        raise ControllerError("S20 evidence does not contain the exact 17 phases")
    if any(phase.get("status") != "pass" for phase in journal) or any(
        phase.get("status") != "pass" for phase in lifecycle.get("phases", [])
    ):
        raise ControllerError("S20 evidence or journal contains a non-pass phase")
    if resources is not None:
        run = evidence.get("run_environment")
        expected = {
            "region": resources["region"],
            "image_digest": resources["image_digest"],
            "machine": resources["machine"],
            "cpus": resources["cpus"],
            "memory_mb": resources["memory_mb"],
            "volume_gb": resources["volume_gb"],
            "public_services": 0,
            "restart": "no",
        }
        if not isinstance(run, dict) or any(
            run.get(key) != value for key, value in expected.items()
        ):
            raise ControllerError("S20 evidence does not bind the observed disposable resources")
        if lifecycle.get("current_transitions") != {"source": 1, "clean_import": 1}:
            raise ControllerError("S20 lifecycle must publish exactly once per constructed project")
        for phase in lifecycle.get("phases", []):
            validate_phase_measurement(phase)
            if phase["memory_limit_bytes"] != resources["memory_mb"] * 1024 * 1024:
                raise ControllerError("S20 cgroup memory limit differs from selected Machine")
        phases = {phase["id"]: phase for phase in lifecycle["phases"]}
        for phase_id in ("ingest", "import"):
            io = phases[phase_id]["io"]
            if (
                io["storage_blocks"] <= 0
                or io["arrow_batches"] <= 0
                or io["topology_rows"] <= 0
                or io["proc_read_syscalls"] + io["proc_write_syscalls"] >= io["topology_rows"]
            ):
                raise ControllerError("construction I/O scales with rows instead of blocks/batches")
        volume_bytes = resources["volume_gb"] * (1 << 30)
        if any(
            phase["filesystem"]["total_bytes"] > volume_bytes
            or phase["filesystem"]["total_bytes"] < int(volume_bytes * 0.90)
            or phase["filesystem"]["available_after_bytes"]
            < phase["filesystem"]["total_bytes"] // 5
            for phase in lifecycle["phases"]
        ):
            raise ControllerError("S20 observed volume or disk headroom differs from qualification")
    for left, right in (
        ("source_edges", "imported_edges"),
        ("source_project_fingerprint", "imported_project_fingerprint"),
        ("source_authority_fingerprint", "imported_authority_fingerprint"),
    ):
        if lifecycle.get(left) != lifecycle.get(right):
            raise ControllerError(f"S20 lifecycle mismatch: {left}/{right}")


def preserve_and_validate_evidence(
    evidence: dict[str, Any],
    journal: list[dict[str, Any]],
    sha: str,
    evidence_out: Path,
    journal_out: Path,
    resources: dict[str, Any] | None = None,
) -> None:
    """Persist failure evidence before applying the success-only contract."""
    write_sanitized_json(evidence_out, evidence)
    write_sanitized_json(journal_out, journal)
    validate_evidence(evidence, journal, sha, resources)


def destroy_and_verify(
    fly: Flyctl,
    app: str,
    machine_id: str | None,
    volume_id: str | None,
    machine_name: str | None = None,
    volume_name: str | None = None,
) -> None:
    """Discover ambiguous creates, tear down independently, and prove absence."""
    deadline = time.monotonic() + CLEANUP_TTL_S
    failures: list[str] = []
    machine_ids = {machine_id} if machine_id else set()
    volume_ids = {volume_id} if volume_id else set()

    def attempt(kind: str, arguments: list[str]) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            failures.append(f"{kind}:deadline")
            return
        try:
            fly.run(arguments, check=False, timeout=min(30, remaining))
        except (OSError, subprocess.SubprocessError) as error:
            failures.append(f"{kind}:{type(error).__name__}")

    while time.monotonic() < deadline:
        app_absent = False
        try:
            query_timeout = min(15, max(0.1, deadline - time.monotonic()))
            apps = fly.json(["apps", "list"], timeout=query_timeout)
            app_absent = not any(item.get("Name") == app or item.get("name") == app for item in apps)
            if not app_absent:
                machines = fly.json(
                    ["machines", "list", "--app", app],
                    timeout=min(15, max(0.1, deadline - time.monotonic())),
                )
                volumes = fly.json(
                    ["volumes", "list", "--app", app],
                    timeout=min(15, max(0.1, deadline - time.monotonic())),
                )
                for item in machines:
                    if isinstance(item, dict) and (
                        item.get("id") in machine_ids
                        or (
                            machine_name is not None
                            and (item.get("name") == machine_name or item.get("Name") == machine_name)
                        )
                    ):
                        if isinstance(item.get("id"), str):
                            machine_ids.add(item["id"])
                for item in volumes:
                    if isinstance(item, dict) and (
                        item.get("id") in volume_ids
                        or (
                            volume_name is not None
                            and (item.get("name") == volume_name or item.get("Name") == volume_name)
                        )
                    ):
                        if isinstance(item.get("id"), str):
                            volume_ids.add(item["id"])
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
            pass
        if app_absent:
            return
        for discovered_machine in sorted(machine_ids):
            attempt(
                "machine",
                ["machine", "destroy", discovered_machine, "--app", app, "--force"],
            )
        for discovered_volume in sorted(volume_ids):
            attempt(
                "volume",
                ["volumes", "destroy", discovered_volume, "--app", app, "--yes"],
            )
        attempt("app", ["apps", "destroy", app, "--yes"])
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(2, remaining))
    raise ControllerError(
        "cleanup deadline left unresolved "
        f"app={app} machines={','.join(sorted(machine_ids)) or 'none'} "
        f"volumes={','.join(sorted(volume_ids)) or 'none'} "
        f"attempt_failures={','.join(failures) or 'none'}"
    )


def retrieve(fly: Flyctl, app: str, machine: str, remote: str, local: Path) -> bool:
    result = fly.run(
        ["ssh", "sftp", "get", remote, str(local), "--app", app, "--machine", machine],
        check=False,
    )
    return result.returncode == 0 and local.is_file()


def machine_diagnostic(fly: Flyctl, app: str, machine: str) -> dict[str, Any]:
    """Return a small allowlisted status record; never retain raw Machine logs."""
    result = fly.run(["machine", "status", machine, "--app", app, "--json"], check=False)
    if result.returncode != 0:
        return {"available": False, "status_returncode": result.returncode}
    try:
        status = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"available": False, "status_returncode": 0, "status_json": "invalid"}
    events = status.get("events", []) if isinstance(status, dict) else []
    safe_events = []
    for event in events[-MAX_DIAGNOSTIC_EVENTS:] if isinstance(events, list) else []:
        if isinstance(event, dict):
            safe_events.append({key: event[key] for key in DIAGNOSTIC_EVENT_KEYS if key in event})
    return {
        "available": True,
        "state": status.get("state") if isinstance(status, dict) else None,
        "region": status.get("region") if isinstance(status, dict) else None,
        "events": safe_events,
    }


def execute(args: argparse.Namespace, fly: Flyctl, digest: str, resources: dict[str, Any]) -> None:
    app_created = False
    machine_id = volume_id = None
    deadline = time.monotonic() + HARD_TTL_S
    try:
        apps = fly.json(["apps", "list"])
        app_exists = any(
            item.get("Name") == args.app_name or item.get("name") == args.app_name for item in apps
        )
        if app_exists:
            if fly.json(["machines", "list", "--app", args.app_name]) or fly.json(
                ["volumes", "list", "--app", args.app_name]
            ):
                raise ControllerError("refusing to reuse a non-empty image-staging app")
        else:
            app_created = True
            fly.run(["apps", "create", args.app_name, "--org", args.org])
        app_created = True
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
                str(resources["volume_gb"]),
                "--scheduled-snapshots=false",
                "--yes",
            ]
        )
        volume_id = volume["id"]
        if volume.get("size_gb") != resources["volume_gb"] or volume.get("region") not in (
            None,
            args.region,
        ):
            raise ControllerError("observed volume size or region differs from qualification")
        machine = create_machine(args, fly, volume_id, resources)
        machine_id = machine["id"]
        assert_machine(machine, args, digest, resources)
        with tempfile.TemporaryDirectory(prefix="graphforge-fly-s20-") as directory:
            journal_path = Path(directory) / "journal.json"
            evidence_path = Path(directory) / "evidence.json"
            completed = 0
            active_phase = PHASES[0]
            phase_started = time.monotonic()
            next_heartbeat = phase_started
            while time.monotonic() < deadline:
                now = time.monotonic()
                if retrieve(fly, args.app_name, machine_id, "/work/s20-journal.json", journal_path):
                    journal = json.loads(journal_path.read_text())
                    try:
                        observed_completed, observed_active = journal_progress(journal)
                    except ControllerError as error:
                        if str(error).startswith("phase_failed "):
                            write_sanitized_json(args.journal_out, journal)
                        raise
                    if observed_completed < completed:
                        raise ControllerError("journal_invalid completed phase count regressed")
                    if observed_completed > completed:
                        for phase in journal[completed:observed_completed]:
                            emit_progress(
                                "phase_complete",
                                phase=phase["id"],
                                elapsed_ms=phase.get("elapsed_ms"),
                                rss_peak_bytes=phase.get("rss_peak_bytes"),
                                disk_peak_bytes=phase.get("disk_peak_bytes"),
                            )
                        completed = observed_completed
                        active_phase = observed_active
                        phase_started = now
                        next_heartbeat = now
                        if active_phase is not None:
                            emit_progress("phase_start", phase=active_phase)
                    # Preserve the last valid incomplete journal even when a
                    # controller deadline stops and destroys the Machine.
                    write_sanitized_json(args.journal_out, journal)
                if retrieve(
                    fly, args.app_name, machine_id, "/work/s20-evidence.json", evidence_path
                ):
                    break
                if active_phase is not None:
                    phase_elapsed = now - phase_started
                    if phase_elapsed >= PHASE_TIMEOUT_S[active_phase]:
                        raise ControllerError(
                            f"phase_timeout phase={active_phase} "
                            f"elapsed_s={int(phase_elapsed)} "
                            f"limit_s={PHASE_TIMEOUT_S[active_phase]}"
                        )
                    if now >= next_heartbeat:
                        emit_progress(
                            "phase_heartbeat",
                            phase=active_phase,
                            elapsed_s=int(phase_elapsed),
                            limit_s=PHASE_TIMEOUT_S[active_phase],
                            completed_phases=completed,
                        )
                        next_heartbeat = now + HEARTBEAT_INTERVAL_S
                time.sleep(POLL_INTERVAL_S)
            else:
                raise ControllerError("run_timeout 4h hard deadline reached before S20 evidence")
            evidence = json.loads(evidence_path.read_text())
            journal = json.loads(journal_path.read_text())
            preserve_and_validate_evidence(
                evidence,
                journal,
                args.expected_sha,
                args.evidence_out,
                args.journal_out,
                resources,
            )
            fly.run(
                [
                    "machine",
                    "exec",
                    machine_id,
                    "--app",
                    args.app_name,
                    "touch /work/controller-ack",
                ]
            )
    except (ControllerError, OSError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        diagnostic: dict[str, Any] = {
            "schema": "graphforge-s20-controller-diagnostic/1",
            "result": "fail",
            "controller_error": str(error),
            "git_sha": args.expected_sha,
        }
        if machine_id:
            try:
                diagnostic["machine"] = machine_diagnostic(fly, args.app_name, machine_id)
            except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
                diagnostic["machine"] = {"available": False, "status_error": "query_failed"}
        write_sanitized_json(args.diagnostic_out, diagnostic)
        raise
    finally:
        if app_created:
            destroy_and_verify(
                fly,
                args.app_name,
                machine_id,
                volume_id,
                args.machine_name,
                args.volume_name,
            )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--expected-sha", required=True)
    result.add_argument("--image", required=True)
    result.add_argument("--region", default="dfw")
    result.add_argument("--org", required=True)
    result.add_argument("--app-name", required=True)
    result.add_argument("--machine-name", required=True)
    result.add_argument("--volume-name", required=True)
    result.add_argument("--ceiling-usd", type=float, default=10.0)
    result.add_argument("--unpriced-reserve-usd", type=float, default=1.0)
    result.add_argument("--pricing-html", type=Path, help="dry-run test fixture only")
    result.add_argument("--manifest-json", type=Path, help="dry-run manifest fixture only")
    result.add_argument("--image-contract-json", type=Path, help="dry-run image config fixture only")
    result.add_argument("--qualification-evidence", type=Path, required=True)
    result.add_argument("--evidence-out", type=Path, default=Path("s20-evidence.json"))
    result.add_argument("--journal-out", type=Path, default=Path("s20-journal.json"))
    result.add_argument("--diagnostic-out", type=Path, default=Path("s20-diagnostic.json"))
    result.add_argument("--execute", action="store_true")
    result.add_argument("--confirm-disposable", action="store_true")
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        digest = validate_args(args)
        if args.execute:
            check_source(args.expected_sha)
        assert_platform_child(
            args.image,
            args.expected_sha,
            args.manifest_json.read_text() if args.manifest_json else None,
            args.image_contract_json.read_text() if args.image_contract_json else None,
        )
        html = args.pricing_html.read_text() if args.pricing_html else fetch_pricing()
        resources = load_qualification(args.qualification_evidence, digest, args.region)
        resources.update({"region": args.region, "image_digest": digest})
        rates = parse_live_rates(
            html,
            args.region,
            resources["machine"],
            resources["cpus"],
            resources["memory_mb"],
        )
        costs = cost_plan(
            rates,
            args.ceiling_usd,
            args.unpriced_reserve_usd,
            resources["volume_gb"],
            resources["qualification_reserved_cost_usd"],
        )
        plan = {
            "mode": "execute" if args.execute else "dry-run",
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "pricing_source": PRICING_URL,
            "rates": rates,
            "cost": costs,
            "git_sha": args.expected_sha,
            "image_digest": digest,
            "region": args.region,
            "qualification": resources,
            "machine": {
                "name": resources["machine"],
                "cpu_kind": "performance",
                "cpus": resources["cpus"],
                "memory_mb": resources["memory_mb"],
            },
            "volume_gb": resources["volume_gb"],
            "public_services": 0,
            "restart": "no",
            "auto_destroy": True,
            "hard_ttl_s": HARD_TTL_S,
            "phase_timeout_s": PHASE_TIMEOUT_S,
            "heartbeat_interval_s": HEARTBEAT_INTERVAL_S,
        }
        print(json.dumps(plan, indent=2, sort_keys=True))
        if args.execute:
            execute(args, Flyctl(), digest, resources)
    except (ControllerError, OSError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        print(f"Fly S20 controller refused: {error}", file=__import__("sys").stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
