"""Pulumi ESC control plane for provider-backed GraphForge qualifications.

The canonical ``run`` command opens an ESC environment and invokes the existing
controller inside Pulumi's secret-filtered process. It does not claim that a
caller-controlled marker can prove ESC ancestry. The progressive ladder remains
fail-closed until a dedicated scale executor exists; its no-spend plan is
available through ``plan-progressive``.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from pathlib import Path
import re
import subprocess
import sys

ESC_ENVIRONMENT = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]*(?:/[A-Za-z0-9][A-Za-z0-9_.-]*){0,2}(?:@[A-Za-z0-9_.-]+)?$"
)
COMMIT = re.compile(r"^[0-9a-f]{40}$")
LIVE_GATES = {"fly-tiny", "fly-tiny-recovery"}
ALL_GATES = (*sorted(LIVE_GATES), "progressive-ladder")
ROOT = Path(__file__).resolve().parents[3]


class OperatorRefusalError(ValueError):
    """Execution lacks a closed authority or implementation boundary."""


def _forwarded(argv: Sequence[str]) -> list[str]:
    values = list(argv)
    return values[1:] if values[:1] == ["--"] else values


def _require_flag(argv: Sequence[str], flag: str) -> None:
    if flag not in argv:
        raise OperatorRefusalError(f"{flag} is required")


def _single_value(argv: Sequence[str], flag: str) -> str:
    positions = [index for index, value in enumerate(argv) if value == flag]
    if len(positions) != 1 or positions[0] + 1 >= len(argv):
        raise OperatorRefusalError(f"{flag} must occur once with a value")
    value = argv[positions[0] + 1]
    if value.startswith("-"):
        raise OperatorRefusalError(f"{flag} must occur once with a value")
    return value


def validate_live_request(gate: str, argv: Sequence[str]) -> None:
    if gate == "progressive-ladder":
        raise OperatorRefusalError(
            "progressive-ladder execution is unavailable until the dedicated provider "
            "image and BenchExec scale executor are implemented"
        )
    if gate not in LIVE_GATES:
        raise OperatorRefusalError("qualification gate is unknown")
    commit = _single_value(argv, "--expected-sha")
    if COMMIT.fullmatch(commit) is None:
        raise OperatorRefusalError("--expected-sha must be a lowercase full Git object ID")
    _require_flag(argv, "--confirm-disposable")
    _require_flag(argv, "--execute" if gate == "fly-tiny" else "--cleanup-only")


def esc_command(environment: str, gate: str, argv: Sequence[str]) -> tuple[str, ...]:
    """Build one shell-free, secret-filtered ESC invocation."""
    if ESC_ENVIRONMENT.fullmatch(environment) is None:
        raise OperatorRefusalError("Pulumi ESC environment name is invalid")
    forwarded = _forwarded(argv)
    validate_live_request(gate, forwarded)
    return (
        "pulumi",
        "env",
        "run",
        environment,
        "--",
        sys.executable,
        "-m",
        "graphforge_bench.fly_tiny_qualification",
        *forwarded,
    )


def attest_current_main(
    commit: str,
    *,
    root: Path = ROOT,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> None:
    """Bind provider authority to a clean checkout of current origin/main."""
    try:
        head = runner(
            ("git", "rev-parse", "HEAD"),
            cwd=root,
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()
        status = runner(
            ("git", "status", "--porcelain"),
            cwd=root,
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()
        runner(
            ("git", "fetch", "--no-tags", "--depth=1", "origin", "main"),
            cwd=root,
            check=True,
        )
        fetched = runner(
            ("git", "rev-parse", "FETCH_HEAD"),
            cwd=root,
            check=True,
            text=True,
            capture_output=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise OperatorRefusalError("unable to attest current origin/main") from error
    if head != commit or status or fetched != commit:
        raise OperatorRefusalError(
            "--expected-sha must identify a clean checkout of current origin/main"
        )


def run_under_esc(
    environment: str,
    gate: str,
    argv: Sequence[str],
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    attestor: Callable[[str], None] = attest_current_main,
) -> int:
    command = esc_command(environment, gate, argv)
    commit = _single_value(_forwarded(argv), "--expected-sha")
    attestor(commit)
    completed = runner(command, check=False)
    return completed.returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    sub = parser.add_subparsers(dest="action", required=True)
    run = sub.add_parser(
        "run", help="Execute a live qualification under Pulumi ESC", allow_abbrev=False
    )
    run.add_argument("--environment", required=True)
    run.add_argument("--gate", choices=ALL_GATES, required=True)
    plan = sub.add_parser(
        "plan-progressive", help="Write a no-spend next-rung plan", allow_abbrev=False
    )
    plan.add_argument("arguments", nargs=argparse.REMAINDER)
    args, remainder = parser.parse_known_args(argv)
    try:
        if args.action == "run":
            return run_under_esc(args.environment, args.gate, remainder)
        from graphforge_bench import progressive_provider_plan

        return progressive_provider_plan.main(_forwarded(args.arguments))
    except OperatorRefusalError as error:
        print(f"qualification refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
