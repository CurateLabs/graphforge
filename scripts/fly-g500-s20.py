#!/usr/bin/env python3
"""Run one disposable, private Fly S20 certification Machine.

Dry-run is the default.  This controller owns infrastructure only; the pinned
image owns the GraphForge lifecycle and writes the sanitized evidence artifact.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import importlib.util
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
VALIDATOR = ROOT / "scripts/ci/validate-fly-g500-s20.py"
SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST = re.compile(r"^[^\s@]+@(?P<digest>sha256:[0-9a-f]{64})$")
SAFE_NAME = re.compile(r"^[a-z][a-z0-9-]{2,62}$")
SAFE_VOLUME = re.compile(r"^[a-z][a-z0-9_]{0,29}$")
SAFE_REGION = re.compile(r"^[a-z0-9-]{2,20}$")
CPUS = 2
MEMORY_MB = 4096
MAX_COST_USD = 10.0
PRICING_SOURCE = "https://fly.io/docs/about/pricing/"
# Largest currently published regional performance-2x/4GB price (2026-08-27).
COMPUTE_USD_PER_SECOND = 0.00003864
VOLUME_USD_PER_GB_MONTH = 0.15
RUN_SECONDS = 14_400
CLEANUP_RESERVE_SECONDS = 600
VOLUME_BILLING_HOURS = 5
EVIDENCE_PATH = "/work/s20-evidence.json"
RESULT_PATH = "/work/container-result.json"


class ControllerError(RuntimeError):
    pass


class Flyctl:
    def run(self, args: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["flyctl", *args],
            cwd=ROOT,
            check=check,
            text=True,
            capture_output=True,
            timeout=120,
        )

    def json(self, args: Sequence[str]) -> Any:
        return json.loads(self.run([*args, "--json"]).stdout)


def validate_inputs(args: argparse.Namespace) -> str:
    image = DIGEST.fullmatch(args.image)
    if not image:
        raise ControllerError("--image must be an immutable OCI @sha256 digest reference")
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
    return image.group("digest")


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


def price_reservation(volume_size_gb: int) -> dict[str, Any]:
    compute = (RUN_SECONDS + CLEANUP_RESERVE_SECONDS) * COMPUTE_USD_PER_SECOND
    volume = volume_size_gb * VOLUME_USD_PER_GB_MONTH / (30 * 24) * VOLUME_BILLING_HOURS
    total_cents = int((compute + volume) * 100 + 0.999999999)
    return {
        "pricing_source": PRICING_SOURCE,
        "compute_rate_usd_per_second": COMPUTE_USD_PER_SECOND,
        "volume_rate_usd_per_gb_month": VOLUME_USD_PER_GB_MONTH,
        "runtime_seconds": RUN_SECONDS,
        "cleanup_reserve_seconds": CLEANUP_RESERVE_SECONDS,
        "volume_billing_hours": VOLUME_BILLING_HOURS,
        "volume_size_gb": volume_size_gb,
        "reserved_usd": total_cents / 100,
    }


def reserve_budget(path: Path, run_id: str, reservation: dict[str, Any]) -> None:
    """Durably reserve before creation; reservations survive failed attempts."""
    state = {"schema": "graphforge-fly-cost-ledger/1", "limit_usd": MAX_COST_USD, "runs": []}
    if path.exists():
        state = json.loads(path.read_text(encoding="utf-8"))
    if (
        state.get("schema") != "graphforge-fly-cost-ledger/1"
        or state.get("limit_usd") != MAX_COST_USD
    ):
        raise ControllerError("invalid cost ledger")
    runs = state.get("runs")
    if not isinstance(runs, list):
        raise ControllerError("invalid cost ledger runs")
    if any(run.get("run_id") == run_id for run in runs):
        raise ControllerError("run id is already reserved")
    used = sum(float(run.get("reserved_usd", MAX_COST_USD + 1)) for run in runs)
    amount = float(reservation["reserved_usd"])
    if used + amount > MAX_COST_USD:
        raise ControllerError("durable cost reservations would exceed $10")
    runs.append({"run_id": run_id, **reservation})
    with tempfile.NamedTemporaryFile(
        "w", dir=path.parent, delete=False, encoding="utf-8"
    ) as handle:
        json.dump(state, handle, sort_keys=True)
        handle.write("\n")
        temporary = Path(handle.name)
    temporary.replace(path)


def machine_payload(args: argparse.Namespace, volume_id: str, digest: str) -> dict[str, Any]:
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
            "env": {
                "GF_G500_CERTIFICATION_SCALE": "20",
                "GF_G500_S20_EXPECTED_SHA": args.expected_sha,
                "GF_G500_S20_IMAGE_DIGEST": digest,
                "GF_G500_S20_REGION": args.region,
                "GF_G500_S20_VOLUME_GB": str(args.volume_size_gb),
                "GF_G500_S20_EVIDENCE_OUT": EVIDENCE_PATH,
                "GF_G500_S20_RESULT_OUT": RESULT_PATH,
                "GF_G500_S20_TIMEOUT_SECONDS": str(RUN_SECONDS),
                "TMPDIR": "/work/tmp",
            },
        },
    }


def assert_machine(machine: dict[str, Any], args: argparse.Namespace, digest: str) -> None:
    config = machine.get("config", {})
    guest = config.get("guest", {})
    if machine.get("region") != args.region or machine.get("image_ref", {}).get("digest") != digest:
        raise ControllerError("observed Machine region/image differs from the pinned plan")
    if guest != {"cpu_kind": "performance", "cpus": CPUS, "memory_mb": MEMORY_MB}:
        raise ControllerError("observed Machine resources differ from 2 CPU/4096 MB")
    if config.get("auto_destroy") is not True or config.get("restart", {}).get("policy") != "no":
        raise ControllerError("observed Machine is not disposable")
    if config.get("services") not in (None, []):
        raise ControllerError("observed Machine exposes a service")
    mounts = config.get("mounts", [])
    if len(mounts) != 1 or mounts[0].get("path") != "/work":
        raise ControllerError("observed Machine does not have the one /work volume")


def create_machine(
    args: argparse.Namespace, fly: Flyctl, volume_id: str, digest: str
) -> dict[str, Any]:
    token = fly.run(["auth", "token"]).stdout.strip()
    if not token:
        raise ControllerError("Fly authentication token is unavailable")
    request = urllib.request.Request(
        f"https://api.machines.dev/v1/apps/{args.app_name}/machines",
        data=json.dumps(machine_payload(args, volume_id, digest)).encode(),
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


def cleanup_owned(
    fly: Flyctl, app: str, machine_id: str | None, volume_id: str | None, app_created: bool
) -> None:
    """Destroy only identifiers observed from resources created in this invocation."""
    operations = []
    if machine_id:
        operations.append(["machine", "destroy", machine_id, "--app", app, "--force"])
    if volume_id:
        operations.append(["volumes", "destroy", volume_id, "--app", app, "--yes"])
    if app_created:
        operations.append(["apps", "destroy", app, "--yes"])
    for operation in operations:
        try:
            fly.run(operation, check=False)
        except subprocess.SubprocessError:  # noqa: PERF203 - attempt every cleanup operation
            continue


def verify_absent(
    fly: Flyctl, app: str, machine_id: str | None, volume_id: str | None, app_created: bool
) -> None:
    if (
        machine_id
        and fly.run(["machine", "status", machine_id, "--app", app], check=False).returncode == 0
    ):
        raise ControllerError("owned Machine remains after cleanup")
    if (
        volume_id
        and fly.run(["volumes", "show", volume_id, "--app", app], check=False).returncode == 0
    ):
        raise ControllerError("owned volume remains after cleanup")
    if app_created:
        apps = fly.json(["apps", "list"])
        if any(item.get("Name") == app or item.get("name") == app for item in apps):
            raise ControllerError("owned app remains after cleanup")


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


def execute(args: argparse.Namespace, fly: Flyctl, digest: str) -> None:
    app_created = False
    machine_id = volume_id = None
    try:
        apps = fly.json(["apps", "list"])
        if any(
            item.get("Name") == args.app_name or item.get("name") == args.app_name for item in apps
        ):
            raise ControllerError("refusing to reuse an existing app")
        reserve_budget(args.ledger, args.app_name, price_reservation(args.volume_size_gb))
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
                str(args.volume_size_gb),
                "--scheduled-snapshots=false",
                "--yes",
            ]
        )
        volume_id = volume["id"]
        machine = create_machine(args, fly, volume_id, digest)
        machine_id = machine["id"]
        assert_machine(machine, args, digest)
        deadline = time.monotonic() + args.timeout_s
        with tempfile.TemporaryDirectory(prefix="graphforge-s20-") as directory:
            local = Path(directory) / "evidence.json"
            result_file = Path(directory) / "container-result.json"
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
                time.sleep(2)
            else:
                raise ControllerError("timed out retrieving S20 evidence")
            spec = importlib.util.spec_from_file_location("s20_validator", VALIDATOR)
            if not spec or not spec.loader:
                raise ControllerError("cannot load evidence validator")
            validator = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(validator)
            evidence = json.loads(local.read_text(encoding="utf-8"))
            validator.validate(evidence, args.expected_sha, digest, args.region)
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
    finally:
        cleanup_owned(fly, args.app_name, machine_id, volume_id, app_created)
        verify_absent(fly, args.app_name, machine_id, volume_id, app_created)


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
        digest = validate_inputs(args)
        check_source(args.expected_sha)
        plan = {
            "scale": 20,
            "cpus": CPUS,
            "memory_mb": MEMORY_MB,
            "volume_size_gb": args.volume_size_gb,
            "region": args.region,
            "image_digest": digest,
            "maximum_total_cost_usd": MAX_COST_USD,
            "reservation": price_reservation(args.volume_size_gb),
        }
        print(json.dumps(plan, sort_keys=True))
        if args.execute:
            execute(args, Flyctl(), digest)
        return 0
    except (ControllerError, subprocess.SubprocessError, json.JSONDecodeError, KeyError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    import sys

    raise SystemExit(main())
