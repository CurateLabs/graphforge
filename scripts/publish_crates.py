#!/usr/bin/env python3
"""Publish every GraphForge crate to crates.io in dependency order.

Re-running is safe: a crate whose workspace version is already on crates.io is
skipped. ``--dry-run`` packages every crate in the same order with the same
flags and uploads nothing.

In the release workflow each ``cargo publish`` gets a fresh short-lived token
through crates.io Trusted Publishing (``CRATES_IO_TRUSTED_PUBLISHING=true``).
Trusted Publishing cannot create a crate, so a crate name that has never been
published uses ``CARGO_REGISTRY_TOKEN_NEW_CRATES`` when it is set. Outside the
workflow, set ``CARGO_REGISTRY_TOKEN``. Tokens are never logged.

crates.io rate limits (HTTP 429; new crates are limited to one per ten minutes
after a burst) are handled by sleeping until the time the registry names and
retrying the same crate. The total wait is capped so a non-429 failure is
never hidden.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
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
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
PLAN_SCRIPT = ROOT / "scripts" / "ci" / "crate-publish-plan.py"
CRATES_API = "https://crates.io/api/v1/crates"
TRUSTED_PUBLISHING_TOKENS_API = "https://crates.io/api/v1/trusted_publishing/tokens"
TRUSTED_PUBLISHING_ENV = "CRATES_IO_TRUSTED_PUBLISHING"
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


def trusted_publishing_enabled() -> bool:
    """Return whether this process should obtain per-attempt OIDC tokens."""
    return os.environ.get(TRUSTED_PUBLISHING_ENV) == "true"


def request_trusted_publishing_token() -> str:
    """Exchange this GitHub Actions job's OIDC identity for a crates.io token."""
    request_url = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL")
    request_token = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
    if not request_url or not request_token:
        raise RuntimeError("Trusted Publishing requires GitHub Actions id-token: write permission")

    separator = "&" if "?" in request_url else "?"
    oidc_request = urllib.request.Request(
        f"{request_url}{separator}{urllib.parse.urlencode({'audience': 'crates.io'})}",
        headers={"Authorization": f"Bearer {request_token}", "Accept": "application/json"},
    )
    with urllib.request.urlopen(oidc_request, timeout=30) as response:
        oidc_payload = json.load(response)
    jwt = oidc_payload.get("value")
    if not isinstance(jwt, str) or not jwt:
        raise RuntimeError("GitHub Actions did not return an OIDC token")

    exchange_request = urllib.request.Request(
        TRUSTED_PUBLISHING_TOKENS_API,
        data=json.dumps({"jwt": jwt}).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
        method="POST",
    )
    with urllib.request.urlopen(exchange_request, timeout=30) as response:
        exchange_payload = json.load(response)
    token = exchange_payload.get("token")
    if not isinstance(token, str) or not token:
        raise RuntimeError("crates.io did not return a Trusted Publishing token")
    return normalize_registry_token(token)


def revoke_trusted_publishing_token(token: str) -> None:
    """Revoke a per-attempt token after Cargo no longer needs it."""
    request = urllib.request.Request(
        TRUSTED_PUBLISHING_TOKENS_API,
        headers={"Authorization": f"Bearer {token}", "User-Agent": USER_AGENT},
        method="DELETE",
    )
    with urllib.request.urlopen(request, timeout=30):
        pass


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
    trusted: bool | None = None,
    sleep: Callable[[float], None] = time.sleep,
    run_publish: Callable[[list[str]], subprocess.CompletedProcess[str]] | None = None,
    now: Callable[[], datetime] | None = None,
) -> None:
    """Run ``cargo publish`` for one crate, sleeping through bounded 429 waits.

    Uses ``--no-verify``: some crates (notably ``graphforge-cli``) embed
    workspace paths such as ``project-skills`` that are outside the packaged
    tarball, so a verify build from the tarball alone would fail.
    """
    command = ["cargo", "publish", "-p", name, "--locked", "--no-verify"]
    runner = run_publish or _default_cargo_publish_run
    clock = now or (lambda: datetime.now(timezone.utc))
    waited = 0.0
    while True:
        use_trusted = trusted_publishing_enabled() if trusted is None else trusted
        trusted_token = request_trusted_publishing_token() if use_trusted else None
        if trusted_token is not None:
            os.environ["CARGO_REGISTRY_TOKEN"] = trusted_token
        try:
            result = runner(command)
        finally:
            if trusted_token is not None:
                try:
                    revoke_trusted_publishing_token(trusted_token)
                except (OSError, urllib.error.URLError) as error:
                    print(
                        f"{name}: warning: Trusted Publishing token revocation failed "
                        f"({type(error).__name__}); it will expire normally",
                        file=sys.stderr,
                    )
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


def crate_exists(name: str) -> bool:
    return registry_json(name) is not None


def package(order: list[str]) -> None:
    """Package every crate without uploading: the dry run of a release."""
    command = ["cargo", "package", "--locked", "--no-verify", "--allow-dirty"]
    for name in order:
        command += ["-p", name]
    run(command)


def publish(order: list[str]) -> None:
    new_crate_token = os.environ.get("CARGO_REGISTRY_TOKEN_NEW_CRATES", "").strip()
    for name in order:
        if version_record(name) is not None:
            print(f"{name} {VERSION}: already published, skipping", flush=True)
            continue
        if trusted_publishing_enabled() and new_crate_token and not crate_exists(name):
            # Trusted Publishing cannot create a crate; use the scoped token once.
            os.environ["CARGO_REGISTRY_TOKEN"] = normalize_registry_token(new_crate_token)
            try:
                cargo_publish(name, trusted=False)
            finally:
                del os.environ["CARGO_REGISTRY_TOKEN"]
        else:
            cargo_publish(name)
        print(f"{name} {VERSION}: published", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Package every crate in publish order and upload nothing",
    )
    args = parser.parse_args(argv)

    check = subprocess.run([sys.executable, str(PLAN_SCRIPT), "check"], cwd=ROOT, check=False)
    if check.returncode != 0:
        return check.returncode
    plan = load_plan_module()
    order = plan.topological_publish_order(plan.load_workspace())

    if args.dry_run:
        package(order)
        return 0

    if not trusted_publishing_enabled():
        try:
            os.environ["CARGO_REGISTRY_TOKEN"] = normalize_registry_token(
                os.environ.get("CARGO_REGISTRY_TOKEN", "")
            )
        except ValueError as error:
            print(str(error), file=sys.stderr)
            return 2
    publish(order)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
