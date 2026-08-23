#!/usr/bin/env python3
"""Plan or run one disposable Fly 4 GiB S20 full-lifecycle Machine."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import datetime, timezone
import json
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
HARD_TTL_S = 4 * 3600 + 30 * 60
VOLUME_GB = 50
MEMORY_MB = 4096
CPUS = 2


class ControllerError(RuntimeError):
    pass


class Flyctl:
    def run(self, args: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["flyctl", *args],
            cwd=ROOT,
            check=check,
            capture_output=True,
            text=True,
            timeout=120,
        )

    def json(self, args: Sequence[str]) -> Any:
        return json.loads(self.run([*args, "--json"]).stdout)


def fetch_pricing() -> str:
    request = urllib.request.Request(
        PRICING_URL, headers={"User-Agent": "graphforge-s20-controller/1"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.geturl() != PRICING_URL:
            raise ControllerError("official pricing request redirected")
        return response.read().decode("utf-8")


def parse_live_rates(html: str, region: str) -> dict[str, float]:
    matrix = re.search(
        rf'id="started-machines-pricing-matrix-{re.escape(region)}".*?</table>',
        html,
        re.DOTALL,
    )
    if not matrix:
        raise ControllerError(f"official pricing has no region {region}")
    row = re.search(
        r"performance-2x.*?2 performance.*?4GB.*?"
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


def cost_plan(rates: dict[str, float], ceiling: float, reserve: float) -> dict[str, float]:
    compute = rates["compute_per_hour_usd"] * HARD_TTL_S / 3600
    # Volume billing is hourly; conservatively charge a full five hours.
    volume = rates["volume_gb_month_usd"] * VOLUME_GB * 5 / (30 * 24)
    projected = compute + volume + reserve
    if projected > ceiling:
        raise ControllerError(f"projected maximum ${projected:.4f} exceeds ${ceiling:.2f} ceiling")
    return {
        "compute_usd": compute,
        "volume_usd": volume,
        "unpriced_reserve_usd": reserve,
        "projected_max_usd": projected,
        "ceiling_usd": ceiling,
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
    if args.execute and (not args.confirm_disposable or args.pricing_html or args.manifest_json):
        raise ControllerError("execution requires confirmation and live official pricing")
    if not args.evidence_out.parent.is_dir() or not args.journal_out.parent.is_dir():
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


def assert_platform_child(image: str, manifest_json: str | None = None) -> None:
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


def machine_payload(args: argparse.Namespace, volume_id: str) -> dict[str, Any]:
    return {
        "name": args.machine_name,
        "region": args.region,
        "skip_launch": False,
        "skip_service_registration": True,
        "config": {
            "image": args.image,
            "auto_destroy": True,
            "restart": {"policy": "no"},
            "guest": {"cpu_kind": "performance", "cpus": CPUS, "memory_mb": MEMORY_MB},
            "mounts": [{"volume": volume_id, "path": "/work"}],
            "services": [],
            "env": {"GF_G500_S20_EXPECTED_SHA": args.expected_sha},
        },
    }


def create_machine(args: argparse.Namespace, fly: Flyctl, volume_id: str) -> dict[str, Any]:
    token = fly.run(["auth", "token"]).stdout.strip()
    if not token:
        raise ControllerError("Fly authentication token is unavailable")
    request = urllib.request.Request(
        f"https://api.machines.dev/v1/apps/{args.app_name}/machines",
        data=json.dumps(machine_payload(args, volume_id)).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.load(response)
    except (urllib.error.HTTPError, urllib.error.URLError):
        raise ControllerError("Fly Machines API rejected creation") from None


def assert_machine(machine: dict[str, Any], args: argparse.Namespace, digest: str) -> None:
    config = machine.get("config", {})
    guest = config.get("guest", {})
    if machine.get("region") != args.region or machine.get("image_ref", {}).get("digest") != digest:
        raise ControllerError("observed region or child image digest differs from plan")
    if config.get("auto_destroy") is not True or config.get("restart") != {"policy": "no"}:
        raise ControllerError("observed Machine is not disposable")
    if config.get("services") not in (None, []) or guest != {
        "cpu_kind": "performance",
        "cpus": CPUS,
        "memory_mb": MEMORY_MB,
    }:
        raise ControllerError("observed Machine resources/services differ from plan")
    mounts = config.get("mounts", [])
    if len(mounts) != 1 or mounts[0].get("path") != "/work":
        raise ControllerError("observed work-root volume differs from plan")


def validate_evidence(evidence: dict[str, Any], journal: list[dict[str, Any]], sha: str) -> None:
    if evidence.get("schema") != "graphforge-s20-integrated-lifecycle-evidence/1":
        raise ControllerError("unexpected S20 evidence schema")
    if evidence.get("git_sha") != sha or evidence.get("result") != "pass":
        raise ControllerError("S20 evidence SHA/result mismatch")
    lifecycle = evidence.get("lifecycle", {})
    observed = [phase.get("id") for phase in lifecycle.get("phases", [])]
    if observed != PHASES or [phase.get("id") for phase in journal] != PHASES:
        raise ControllerError("S20 evidence does not contain the exact 17 phases")
    if any(phase.get("status") != "pass" for phase in journal) or any(
        phase.get("status") != "pass" for phase in lifecycle.get("phases", [])
    ):
        raise ControllerError("S20 evidence or journal contains a non-pass phase")
    for left, right in (
        ("source_edges", "imported_edges"),
        ("source_project_fingerprint", "imported_project_fingerprint"),
        ("source_authority_fingerprint", "imported_authority_fingerprint"),
    ):
        if lifecycle.get(left) != lifecycle.get(right):
            raise ControllerError(f"S20 lifecycle mismatch: {left}/{right}")


def destroy_and_verify(
    fly: Flyctl, app: str, machine_id: str | None, volume_id: str | None
) -> None:
    if machine_id:
        fly.run(["machine", "destroy", machine_id, "--app", app, "--force"], check=False)
    if volume_id:
        fly.run(["volumes", "destroy", volume_id, "--app", app, "--yes"], check=False)
    for _ in range(10):
        machines = fly.json(["machines", "list", "--app", app])
        volumes = fly.json(["volumes", "list", "--app", app])
        machine_absent = not machine_id or not any(
            item.get("id") == machine_id for item in machines
        )
        volume_absent = not volume_id or not any(item.get("id") == volume_id for item in volumes)
        if machine_absent and volume_absent:
            break
        time.sleep(2)
    else:
        raise ControllerError("cleanup verification found a Machine or volume still present")
    fly.run(["apps", "destroy", app, "--yes"], check=False)
    for _ in range(10):
        apps = fly.json(["apps", "list"])
        if not any(item.get("Name") == app or item.get("name") == app for item in apps):
            return
        time.sleep(2)
    raise ControllerError("cleanup verification found the disposable app still present")


def retrieve(fly: Flyctl, app: str, machine: str, remote: str, local: Path) -> bool:
    result = fly.run(
        ["ssh", "sftp", "get", remote, str(local), "--app", app, "--machine", machine],
        check=False,
    )
    return result.returncode == 0 and local.is_file()


def execute(args: argparse.Namespace, fly: Flyctl, digest: str) -> None:
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
                str(VOLUME_GB),
                "--scheduled-snapshots=false",
                "--yes",
            ]
        )
        volume_id = volume["id"]
        machine = create_machine(args, fly, volume_id)
        machine_id = machine["id"]
        assert_machine(machine, args, digest)
        with tempfile.TemporaryDirectory(prefix="graphforge-fly-s20-") as directory:
            journal_path = Path(directory) / "journal.json"
            evidence_path = Path(directory) / "evidence.json"
            while time.monotonic() < deadline:
                retrieve(fly, args.app_name, machine_id, "/work/s20-journal.json", journal_path)
                if retrieve(
                    fly, args.app_name, machine_id, "/work/s20-evidence.json", evidence_path
                ):
                    break
                time.sleep(5)
            else:
                raise ControllerError("4h30 hard deadline reached before S20 evidence")
            evidence = json.loads(evidence_path.read_text())
            journal = json.loads(journal_path.read_text())
            validate_evidence(evidence, journal, args.expected_sha)
            args.evidence_out.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
            args.journal_out.write_text(json.dumps(journal, indent=2, sort_keys=True) + "\n")
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
    finally:
        if app_created:
            destroy_and_verify(fly, args.app_name, machine_id, volume_id)


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
    result.add_argument("--evidence-out", type=Path, default=Path("s20-evidence.json"))
    result.add_argument("--journal-out", type=Path, default=Path("s20-journal.json"))
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
            args.manifest_json.read_text() if args.manifest_json else None,
        )
        html = args.pricing_html.read_text() if args.pricing_html else fetch_pricing()
        rates = parse_live_rates(html, args.region)
        costs = cost_plan(rates, args.ceiling_usd, args.unpriced_reserve_usd)
        plan = {
            "mode": "execute" if args.execute else "dry-run",
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "pricing_source": PRICING_URL,
            "rates": rates,
            "cost": costs,
            "git_sha": args.expected_sha,
            "image_digest": digest,
            "region": args.region,
            "machine": {"cpu_kind": "performance", "cpus": CPUS, "memory_mb": MEMORY_MB},
            "volume_gb": VOLUME_GB,
            "public_services": 0,
            "restart": "no",
            "auto_destroy": True,
            "hard_ttl_s": HARD_TTL_S,
        }
        print(json.dumps(plan, indent=2, sort_keys=True))
        if args.execute:
            execute(args, Flyctl(), digest)
    except (ControllerError, OSError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        print(f"Fly S20 controller refused: {error}", file=__import__("sys").stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
