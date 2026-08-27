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
    if not 0 < args.reserved_cost_usd <= MAX_COST_USD:
        raise ControllerError("reserved cost must be positive and no more than $10")
    for path, label in ((args.evidence_out, "evidence"), (args.ledger, "ledger")):
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


def reserve_budget(path: Path, run_id: str, amount: float) -> None:
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
    if used + amount > MAX_COST_USD:
        raise ControllerError("durable cost reservations would exceed $10")
    runs.append({"run_id": run_id, "reserved_usd": amount})
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
                "GF_G500_CERTIFICATION_GIT_SHA": args.expected_sha,
                "GF_G500_CERTIFICATION_IMAGE_DIGEST": digest,
                "GF_G500_CERTIFICATION_REGION": args.region,
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


def execute(args: argparse.Namespace, fly: Flyctl, digest: str) -> None:
    app_created = False
    machine_id = volume_id = None
    try:
        apps = fly.json(["apps", "list"])
        if any(
            item.get("Name") == args.app_name or item.get("name") == args.app_name for item in apps
        ):
            raise ControllerError("refusing to reuse an existing app")
        reserve_budget(args.ledger, args.app_name, args.reserved_cost_usd)
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
            while time.monotonic() < deadline:
                result = fly.run(
                    [
                        "ssh",
                        "sftp",
                        "get",
                        "/work/evidence/g500-s20-evidence.json",
                        str(local),
                        "--app",
                        args.app_name,
                        "--machine",
                        machine_id,
                    ],
                    check=False,
                )
                if result.returncode == 0 and local.is_file():
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
    finally:
        cleanup_owned(fly, args.app_name, machine_id, volume_id, app_created)


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
    value.add_argument("--reserved-cost-usd", type=float, required=True)
    value.add_argument("--ledger", type=Path, required=True)
    value.add_argument("--evidence-out", type=Path, required=True)
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
