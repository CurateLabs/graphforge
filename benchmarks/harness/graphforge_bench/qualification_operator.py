"""Pulumi ESC control plane for provider-backed GraphForge qualifications.

The public ``run`` command is the only provider execution entry point. It opens
an ESC environment and then invokes the private ``execute`` command with secret
filtering owned by Pulumi. The progressive ladder remains fail-closed until a
dedicated scale executor exists; its no-spend plan is available through
``plan-progressive``.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
import os
import re
import subprocess
import sys

from graphforge_bench import fly_tiny_qualification

ESC_CONTEXT = "pulumi-esc-v1"
ESC_ENVIRONMENT = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]*(?:/[A-Za-z0-9][A-Za-z0-9_.-]*){0,2}(?:@[A-Za-z0-9_.-]+)?$"
)
COMMIT = re.compile(r"^[0-9a-f]{40}$")
LIVE_GATES = {"fly-tiny", "fly-tiny-recovery"}
ALL_GATES = (*sorted(LIVE_GATES), "progressive-ladder")


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
        "env",
        f"GRAPHFORGE_OPERATOR_CONTEXT={ESC_CONTEXT}",
        sys.executable,
        "-m",
        "graphforge_bench.qualification_operator",
        "execute",
        "--gate",
        gate,
        "--",
        *forwarded,
    )


def run_under_esc(
    environment: str,
    gate: str,
    argv: Sequence[str],
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> int:
    command = esc_command(environment, gate, argv)
    completed = runner(command, check=False)
    return completed.returncode


def execute_inner(gate: str, argv: Sequence[str]) -> int:
    """Dispatch only after ``pulumi env run`` established the process context."""
    if os.environ.get("GRAPHFORGE_OPERATOR_CONTEXT") != ESC_CONTEXT:
        raise OperatorRefusalError("provider execution must enter through pulumi env run")
    forwarded = _forwarded(argv)
    validate_live_request(gate, forwarded)
    return fly_tiny_qualification.main(forwarded)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    run = sub.add_parser("run", help="Execute a live qualification under Pulumi ESC")
    run.add_argument("--environment", required=True)
    run.add_argument("--gate", choices=ALL_GATES, required=True)
    execute = sub.add_parser("execute", help=argparse.SUPPRESS)
    execute.add_argument("--gate", choices=ALL_GATES, required=True)
    plan = sub.add_parser("plan-progressive", help="Write a no-spend next-rung plan")
    plan.add_argument("arguments", nargs=argparse.REMAINDER)
    args, remainder = parser.parse_known_args(argv)
    try:
        if args.action == "run":
            return run_under_esc(args.environment, args.gate, remainder)
        if args.action == "execute":
            return execute_inner(args.gate, remainder)
        from graphforge_bench import progressive_provider_plan

        return progressive_provider_plan.main(_forwarded(args.arguments))
    except OperatorRefusalError as error:
        print(f"qualification refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
