#!/usr/bin/env python3
"""Create one disposable, private Fly Machine for issue #882 qualification."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import importlib.util
import json
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
VALIDATOR_PATH = ROOT / "scripts/ci/validate-fly-filesystem-qualification.py"
SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST_REF = re.compile(r"^[^\s@]+@(?P<digest>sha256:[0-9a-f]{64})$")
SAFE_APP_MACHINE_NAME = re.compile(r"^[a-z][a-z0-9-]{2,62}$")
SAFE_VOLUME_NAME = re.compile(r"^[a-z][a-z0-9_]{0,29}$")
SAFE_REGION = re.compile(r"^[a-z0-9-]{2,20}$")


class QualificationError(RuntimeError):
    pass


class Flyctl:
    """Small injectable flyctl transport; stdout is never copied to evidence."""

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


def check_source(expected_sha: str) -> None:
    if not SHA.fullmatch(expected_sha):
        raise QualificationError("--expected-sha must be exact lowercase 40-hex")
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain"], cwd=ROOT, check=True, text=True, stdout=subprocess.PIPE
    ).stdout
    if head != expected_sha:
        raise QualificationError("expected SHA is not the checked-out HEAD")
    if dirty:
        raise QualificationError("source tree is not clean")


def machine_create_payload(args: argparse.Namespace, volume_id: str, digest: str) -> dict[str, Any]:
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
                "cpus": args.cpus,
                "memory_mb": args.memory_mb,
            },
            "mounts": [{"volume": volume_id, "path": "/work"}],
            "services": [],
            "env": {
                "GF_FLY_QUALIFICATION_GIT_SHA": args.expected_sha,
                "GF_FLY_QUALIFICATION_IMAGE_DIGEST": digest,
                "GF_FLY_QUALIFICATION_REGION": args.region,
            },
        },
    }


def create_machine(args: argparse.Namespace, fly: Flyctl, volume_id: str, digest: str) -> Any:
    token = fly.run(["auth", "token"]).stdout.strip()
    if not token:
        raise QualificationError("Fly authentication token is unavailable")
    request = urllib.request.Request(
        f"https://api.machines.dev/v1/apps/{args.app_name}/machines",
        data=json.dumps(machine_create_payload(args, volume_id, digest)).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise QualificationError(
            f"Fly Machines API rejected creation with HTTP {error.code}"
        ) from None
    except urllib.error.URLError:
        raise QualificationError("Fly Machines API connection failed") from None


def volume_create_args(args: argparse.Namespace) -> list[str]:
    return [
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


def validate_inputs(args: argparse.Namespace) -> str:
    match = DIGEST_REF.fullmatch(args.image)
    if not match:
        raise QualificationError("--image must be an immutable OCI @sha256 digest reference")
    if not SHA.fullmatch(args.expected_sha):
        raise QualificationError("--expected-sha must be exact lowercase 40-hex")
    if not SAFE_REGION.fullmatch(args.region):
        raise QualificationError("invalid fixed region")
    for value in (args.app_name, args.machine_name):
        if not SAFE_APP_MACHINE_NAME.fullmatch(value):
            raise QualificationError("app and Machine names must be safe lowercase names")
    if not SAFE_VOLUME_NAME.fullmatch(args.volume_name):
        raise QualificationError(
            "volume name must be 1..30 lowercase alphanumeric/underscore characters"
        )
    if not 1 <= args.cpus <= 16:
        raise QualificationError("qualification CPU count must be in 1..16")
    if not 1024 <= args.memory_mb <= 131072:
        raise QualificationError("qualification memory must be in 1024..131072 MiB")
    if not 1 <= args.volume_size_gb <= 20:
        raise QualificationError("qualification volume must be small (1..20 GiB)")
    if not 60 <= args.retrieve_timeout_s <= 1800:
        raise QualificationError("evidence retrieval timeout must be in 60..1800 seconds")
    if not args.evidence_out.parent.is_dir():
        raise QualificationError("evidence output parent directory does not exist")
    if args.execute and not args.confirm_disposable:
        raise QualificationError("--execute requires --confirm-disposable")
    return match.group("digest")


def assert_machine_config(machine: dict[str, Any], args: argparse.Namespace, digest: str) -> None:
    config = machine.get("config", {})
    guest = config.get("guest", {})
    image = machine.get("image_ref", {})
    mounts = config.get("mounts", [])
    if machine.get("region") != args.region or image.get("digest") != digest:
        raise QualificationError("observed Machine region/image differs from the pinned plan")
    if config.get("auto_destroy") is not True or config.get("restart", {}).get("policy") != "no":
        raise QualificationError("Machine is not disposable")
    if config.get("services") not in (None, []):
        raise QualificationError("Machine unexpectedly exposes a service")
    if (
        guest.get("cpu_kind") != "performance"
        or guest.get("cpus") != args.cpus
        or guest.get("memory_mb") != args.memory_mb
    ):
        raise QualificationError("observed Machine resources differ from explicit plan")
    if len(mounts) != 1 or mounts[0].get("path") != "/work":
        raise QualificationError(
            "Machine must have exactly one volume mounted at process work root"
        )


def cleanup(fly: Flyctl, app: str, machine_id: str | None, volume_id: str | None) -> None:
    # Child-before-parent cleanup is idempotent; already-absent resources are success.
    operations = []
    if machine_id:
        operations.append(["machine", "destroy", machine_id, "--app", app, "--force"])
    if volume_id:
        operations.append(["volumes", "destroy", volume_id, "--app", app, "--yes"])
    operations.append(["apps", "destroy", app, "--yes"])
    for operation in operations:
        try:
            fly.run(operation, check=False)
        except subprocess.SubprocessError:  # noqa: PERF203 - every cleanup must run
            continue


def execute(args: argparse.Namespace, fly: Flyctl, digest: str) -> None:
    app_created = False
    machine_id = None
    volume_id = None
    try:
        apps = fly.json(["apps", "list"])
        if any(
            item.get("Name") == args.app_name or item.get("name") == args.app_name for item in apps
        ):
            raise QualificationError("refusing to reuse a non-empty app name")
        app_created = True
        fly.run(["apps", "create", args.app_name, "--org", args.org])
        volume = fly.json(volume_create_args(args))
        volume_id = volume["id"]
        machine = create_machine(args, fly, volume_id, digest)
        machine_id = machine["id"]
        assert_machine_config(machine, args, digest)

        with tempfile.TemporaryDirectory(prefix="graphforge-fly-evidence-") as directory:
            local = Path(directory) / "evidence.json"
            deadline = time.monotonic() + args.retrieve_timeout_s
            while time.monotonic() < deadline:
                try:
                    result = fly.run(
                        [
                            "ssh",
                            "sftp",
                            "get",
                            "/work/fly-qualification-evidence.json",
                            str(local),
                            "--app",
                            args.app_name,
                            "--machine",
                            machine_id,
                        ],
                        check=False,
                    )
                except subprocess.TimeoutExpired:
                    continue
                if result.returncode == 0 and local.is_file():
                    break
                time.sleep(2)
            else:
                raise QualificationError("timed out retrieving qualification evidence")

            spec = importlib.util.spec_from_file_location("fly_evidence_validator", VALIDATOR_PATH)
            if spec is None or spec.loader is None:
                raise QualificationError("cannot load committed evidence validator")
            validator = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(validator)
            evidence = json.loads(local.read_text(encoding="utf-8"))
            validator.validate(evidence, sha=args.expected_sha, digest=digest, region=args.region)
            args.evidence_out.write_text(
                json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
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
            if evidence["result"] != "qualified":
                raise QualificationError(
                    "filesystem admission was not qualified; full run remains blocked"
                )
    finally:
        if app_created:
            cleanup(fly, args.app_name, machine_id, volume_id)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--expected-sha", required=True)
    result.add_argument("--image", required=True)
    result.add_argument("--region", required=True)
    result.add_argument("--org", required=True)
    result.add_argument("--app-name", required=True)
    result.add_argument("--volume-name", required=True)
    result.add_argument("--machine-name", required=True)
    result.add_argument("--cpus", type=int, default=2)
    result.add_argument("--memory-mb", type=int, default=4096)
    result.add_argument("--volume-size-gb", type=int, default=10)
    result.add_argument("--retrieve-timeout-s", type=int, default=1200)
    result.add_argument(
        "--evidence-out", type=Path, default=Path("fly-qualification-evidence.json")
    )
    result.add_argument("--execute", action="store_true")
    result.add_argument("--confirm-disposable", action="store_true")
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        digest = validate_inputs(args)
        check_source(args.expected_sha)
        if not args.execute:
            print(
                json.dumps(
                    {
                        "mode": "dry-run",
                        "git_sha": args.expected_sha,
                        "image_digest": digest,
                        "region": args.region,
                        "cpu_kind": "performance",
                        "cpus": args.cpus,
                        "memory_mb": args.memory_mb,
                        "volume_size_gb": args.volume_size_gb,
                        "mount_role": "process_work_root",
                        "public_services": 0,
                        "restart": "no",
                        "auto_destroy": True,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        execute(args, Flyctl(), digest)
    except (
        QualificationError,
        subprocess.SubprocessError,
        json.JSONDecodeError,
        KeyError,
        ValueError,
    ) as error:
        print(f"fly filesystem qualification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
