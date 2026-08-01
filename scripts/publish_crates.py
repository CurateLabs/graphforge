#!/usr/bin/env python3
"""Publish the complete GraphForge crates.io surface in dependency order.

The command is resumable without permitting different bytes under an existing
version. Before each registry write it packages the crate locally and computes
the archive checksum. If that exact version already exists, publication only
continues when crates.io reports the same checksum.

Requires ``CARGO_REGISTRY_TOKEN`` in the environment. The maintained release
credential is projected from Pulumi ESC into the GitHub Actions secret used by
``publish.yaml``.

The token is normalized before ``cargo publish``: leading/trailing whitespace
and CR/LF are stripped. The value is never logged.

crates.io new-crate rate limits (HTTP 429) are handled durably: the publisher
parses ``Retry-After`` / ``try again after …`` from cargo's error output, sleeps
until that time (plus a small buffer), and retries the same crate publish.
Total wait is capped so a full remaining surface (~10 new crates at ~10 minutes)
can finish in one job without hiding non-429 failures.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
PLAN_SCRIPT = ROOT / "scripts" / "ci" / "crate-publish-plan.py"
CRATES_API = "https://crates.io/api/v1/crates"
USER_AGENT = "GraphForge crates.io publisher (github.com/CurateLabs/graphforge)"
# New-crate limit is 1 / 10 minutes after the burst; leave headroom for ~10 crates.
RATE_LIMIT_BUFFER_SECONDS = 15
MAX_SINGLE_RATE_LIMIT_WAIT_SECONDS = 20 * 60
MAX_TOTAL_RATE_LIMIT_WAIT_SECONDS = 2 * 60 * 60
_TRY_AGAIN_AFTER = re.compile(
    r"try again after ([A-Za-z]{3}, \d{2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2} GMT)",
    re.IGNORECASE,
)
_RETRY_AFTER_HEADER = re.compile(r"(?im)^retry-after:\s*(\d+)\s*$")
_VERSION_MATCH = re.search(
    r'(?ms)^\[workspace\.package\].*?^version\s*=\s*"([^"]+)"',
    (ROOT / "Cargo.toml").read_text(encoding="utf-8"),
)
if _VERSION_MATCH is None:
    raise RuntimeError("Cargo.toml lacks [workspace.package] version")
VERSION = _VERSION_MATCH.group(1)


def normalize_registry_token(raw: str) -> str:
    """Return a cargo-safe registry token, or raise ValueError without echoing it.

    Cargo rejects tokens with non-printable / non-ISO-8859-1 characters. Secret
    projection and pasted GitHub secrets commonly introduce a trailing newline.
    """
    token = raw.strip()
    if not token:
        raise ValueError("CARGO_REGISTRY_TOKEN is empty after trim")
    # Printable ISO-8859-1 only: 0x20-0x7E and 0xA0-0xFF (not C0/C1/DEL).
    if any(not (0x20 <= ord(ch) <= 0x7E or 0xA0 <= ord(ch) <= 0xFF) for ch in token):
        raise ValueError("CARGO_REGISTRY_TOKEN contains non-printable or non-ISO-8859-1 characters")
    return token


def load_plan_module():
    spec = importlib.util.spec_from_file_location("crate_publish_plan", PLAN_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {PLAN_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(command: list[str]) -> None:
    print("+ " + " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def _is_rate_limit_output(output: str) -> bool:
    lowered = output.lower()
    return "status 429" in lowered or "too many requests" in lowered


def parse_rate_limit_retry_wait(
    output: str,
    *,
    now: datetime | None = None,
) -> float | None:
    """Return seconds to sleep for a crates.io 429, or None if not rate-limited.

    Prefers the ``try again after <HTTP-date>`` timestamp in the crates.io body,
    then a ``Retry-After: <seconds>`` header line if cargo surfaces one. Never
    inspects or returns credential material.
    """
    if not _is_rate_limit_output(output):
        return None

    match = _TRY_AGAIN_AFTER.search(output)
    if match is not None:
        when = parsedate_to_datetime(match.group(1))
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        current = now if now is not None else datetime.now(timezone.utc)
        delay = (when - current).total_seconds() + RATE_LIMIT_BUFFER_SECONDS
        return max(delay, float(RATE_LIMIT_BUFFER_SECONDS))

    header = _RETRY_AFTER_HEADER.search(output)
    if header is not None:
        return float(int(header.group(1))) + RATE_LIMIT_BUFFER_SECONDS

    # Recognized 429 without a parseable wait hint: fail closed (no blind backoff).
    return None


def _default_cargo_publish_run(command: list[str]) -> subprocess.CompletedProcess[str]:
    print("+ " + " ".join(command), flush=True)
    return subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
    )


def _emit_process_output(result: subprocess.CompletedProcess[str]) -> None:
    for stream in (result.stdout, result.stderr):
        if not stream:
            continue
        print(stream, end="" if stream.endswith("\n") else "\n", flush=True)


def cargo_publish(
    name: str,
    *,
    sleep: Callable[[float], None] = time.sleep,
    run_publish: Callable[[list[str]], subprocess.CompletedProcess[str]] | None = None,
    now: Callable[[], datetime] | None = None,
) -> None:
    """Run ``cargo publish`` for one crate, sleeping through bounded 429 waits.

    Uses ``--no-verify`` to match certified Binding RC packaging. Some crates
    (notably ``graphforge-cli``) embed workspace paths such as ``project-skills``
    that are outside the packaged tarball; verify would fail while the certified
    checksum gate still requires those exact bytes.
    """
    command = ["cargo", "publish", "-p", name, "--locked", "--no-verify"]
    runner = run_publish or _default_cargo_publish_run
    clock = now or (lambda: datetime.now(timezone.utc))
    waited = 0.0
    while True:
        result = runner(command)
        if result.returncode == 0:
            _emit_process_output(result)
            return

        combined = f"{result.stdout or ''}{result.stderr or ''}"
        wait = parse_rate_limit_retry_wait(combined, now=clock())
        if wait is None:
            _emit_process_output(result)
            raise subprocess.CalledProcessError(
                result.returncode,
                command,
                output=result.stdout,
                stderr=result.stderr,
            )

        if wait > MAX_SINGLE_RATE_LIMIT_WAIT_SECONDS:
            raise RuntimeError(
                f"crates.io rate-limit wait for {name} is {wait:.0f}s; "
                f"refusing waits above {MAX_SINGLE_RATE_LIMIT_WAIT_SECONDS}s"
            )
        if waited + wait > MAX_TOTAL_RATE_LIMIT_WAIT_SECONDS:
            raise RuntimeError(
                f"crates.io rate-limit wait budget exhausted for {name}: "
                f"already waited {waited:.0f}s, next wait {wait:.0f}s, "
                f"cap {MAX_TOTAL_RATE_LIMIT_WAIT_SECONDS}s "
                f"(~10 new crates at ~10 minutes each)"
            )

        print(
            f"{name}: crates.io 429 rate limit; sleeping {wait:.0f}s before retry "
            f"(waited {waited:.0f}s / {MAX_TOTAL_RATE_LIMIT_WAIT_SECONDS}s budget)",
            flush=True,
        )
        sleep(wait)
        waited += wait


def registry_json(path: str) -> dict[str, Any] | None:
    request = urllib.request.Request(
        f"{CRATES_API}/{path}",
        headers={"User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise


def version_record(name: str) -> dict[str, Any] | None:
    payload = registry_json(f"{name}/{VERSION}")
    if payload is None:
        return None
    return payload.get("version")


def owner_logins(name: str) -> set[str]:
    payload = registry_json(f"{name}/owners")
    if payload is None:
        return set()
    return {owner["login"] for owner in payload.get("users", [])}


def package_checksum(name: str, expected_checksum: str | None = None) -> str:
    run(
        [
            "cargo",
            "package",
            "-p",
            name,
            "--locked",
            "--no-verify",
        ]
    )
    configured_target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    target_dir = configured_target if configured_target.is_absolute() else ROOT / configured_target
    archive = target_dir / "package" / f"{name}-{VERSION}.crate"
    if not archive.is_file():
        raise RuntimeError(f"cargo package did not create {archive}")
    checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
    if expected_checksum is not None and checksum != expected_checksum:
        raise RuntimeError(
            f"{name} {VERSION} packaged checksum {checksum} differs from "
            f"the certified release record {expected_checksum}"
        )
    return checksum


def publish_one(
    name: str,
    *,
    expected_checksum: str | None = None,
) -> str:
    checksum = package_checksum(name, expected_checksum)
    existing = version_record(name)
    if existing is not None:
        registry_checksum = existing.get("checksum")
        if registry_checksum != checksum:
            raise RuntimeError(
                f"refusing to resume {name} {VERSION}: existing checksum "
                f"{registry_checksum} differs from local {checksum}"
            )
        owners = owner_logins(name)
        if "DecisionNerd" not in owners:
            raise RuntimeError(
                f"{name} {VERSION} is indexed but DecisionNerd is not an owner: {sorted(owners)}"
            )
        outcome = "already published; checksum and owner match"
    else:
        cargo_publish(name)
        outcome = "accepted; public checksum and owner verification required"

    print(f"{name} {VERSION}: {outcome}")
    return outcome


def publish_authorized(name: str, expected_checksum: str) -> str:
    """Execute one planner-authorized absent-node write without reclassification."""
    package_checksum(name, expected_checksum)
    cargo_publish(name)
    outcome = "accepted; public checksum and owner verification required"
    print(f"{name} {VERSION}: {outcome}")
    return outcome


def release_record_checksums(record_path: Path, artifacts_dir: Path) -> dict[str, str]:
    try:
        record = json.loads(record_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"cannot read release record {record_path}: {error}") from error
    if record.get("schema") not in {
        "graphforge-release-record-v1",
        "graphforge-release-candidate-v2",
    }:
        raise RuntimeError("unexpected release record schema")
    if record.get("version") != VERSION or record.get("tag") != f"v{VERSION}":
        raise RuntimeError("release record version/tag does not match the Cargo version")
    if (
        record.get("commit_sha")
        != subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        ).stdout.strip()
    ):
        raise RuntimeError("release record commit does not match the checked-out commit")

    checksums: dict[str, str] = {}
    artifacts_root = artifacts_dir.resolve()
    for item in record.get("artifacts", []):
        if item.get("surface") != "crates":
            continue
        name = item.get("name")
        relative = item.get("path")
        checksum = item.get("sha256")
        if not all(isinstance(value, str) for value in (name, relative, checksum)):
            raise RuntimeError("release record contains an invalid crates.io artifact")
        if item.get("version") != VERSION:
            raise RuntimeError(f"release record version mismatch for crate {name}")
        if name in checksums:
            raise RuntimeError(f"release record contains duplicate crate {name}")
        archive = (artifacts_dir / relative).resolve()
        if not archive.is_relative_to(artifacts_root):
            raise RuntimeError(f"certified crate archive escapes artifact root: {relative}")
        if not archive.is_file():
            raise RuntimeError(f"certified crate archive is missing: {relative}")
        actual = hashlib.sha256(archive.read_bytes()).hexdigest()
        if actual != checksum:
            raise RuntimeError(f"certified crate archive checksum mismatch: {relative}")
        checksums[name] = checksum
    return checksums


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--crate", required=True, help="One planner-authorized crate name")
    parser.add_argument(
        "--release-record",
        type=Path,
        help="Certified release record or candidate-manifest JSON",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        help="Root containing the certified artifact paths",
    )
    args = parser.parse_args(argv)

    if (args.release_record is None) != (args.artifacts_dir is None):
        print("--release-record and --artifacts-dir must be provided together", file=sys.stderr)
        return 2

    try:
        os.environ["CARGO_REGISTRY_TOKEN"] = normalize_registry_token(
            os.environ.get("CARGO_REGISTRY_TOKEN", "")
        )
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2

    plan = load_plan_module()
    crates = plan.load_workspace()
    order = plan.topological_publish_order(crates)
    if len(order) != 15:
        print(f"refusing unexpected publish-set size: {len(order)}", file=sys.stderr)
        return 2

    expected_checksums: dict[str, str] = {}
    if args.release_record is not None and args.artifacts_dir is not None:
        expected_checksums = release_record_checksums(args.release_record, args.artifacts_dir)
        if set(expected_checksums) != set(order):
            print(
                "release record crates do not match the complete publication plan",
                file=sys.stderr,
            )
            return 2

    check = subprocess.run(
        [sys.executable, str(PLAN_SCRIPT), "check"],
        cwd=ROOT,
        check=False,
    )
    if check.returncode != 0:
        return check.returncode

    if args.crate not in order:
        print(f"requested crate is outside the publication plan: {args.crate}", file=sys.stderr)
        return 2
    expected = expected_checksums.get(args.crate)
    if expected is None:
        print(f"candidate checksum is missing for {args.crate}", file=sys.stderr)
        return 2
    publish_authorized(args.crate, expected)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
