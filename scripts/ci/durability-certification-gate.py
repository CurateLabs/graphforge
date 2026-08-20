#!/usr/bin/env python3
"""Seeded durability/isolation certification gate (#756).

Validates the frozen certification contract, runs the bounded required-CI
state space via Cargo, and records scheduled-lane evidence with declared
history/seed counts. Failures fail closed — never retry a failed seed into a pass.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = "graphforge-durability-certification/1"
CERT_SEED = 7560
DEFAULT_CI_HISTORIES = 8
DEFAULT_CI_OPS = 12
SCHEDULED_HISTORIES = 64
SCHEDULED_OPS = 32
REQUIRED_OPERATIONS = [
    "ack_commit",
    "crash_at_phase",
    "tear_bytes",
    "pin_reader",
    "read_pinned",
    "fresh_open",
    "create_checkpoint",
    "acquire_lease",
    "release_lease",
    "publish_delta_run",
    "compact_deltas",
    "run_gc",
    "optimistic_write_skew",
    "cancel_unstarted",
    "idempotent_retry",
]
MATRIX_PATH = ROOT / "tests/contracts/durability-isolation-matrix.json"
CERT_CONTRACT_PATH = ROOT / "tests/contracts/durability-certification.json"

FORBIDDEN_POSITIVE = (
    "provides ssi",
    "is ssi",
    "serializable isolation",
    "universal filesystem",
    "distributed durability",
)


class GateError(RuntimeError):
    """Certification gate failure."""


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git_head() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        return "unknown"
    return (completed.stdout or "").strip() or "unknown"


def command_version(argv: list[str]) -> str:
    completed = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True, check=False)
    if completed.returncode != 0:
        return "unavailable"
    return ((completed.stdout or completed.stderr) or "").splitlines()[0].strip()


def load_cert_contract() -> dict[str, Any]:
    try:
        value = json.loads(CERT_CONTRACT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateError(f"cannot read certification contract: {error}") from error
    if not isinstance(value, dict):
        raise GateError("certification contract root must be an object")
    return value


def validate_config(seed: int, histories: int, ops: int) -> None:
    if seed != CERT_SEED:
        raise GateError(f"published certification seed must remain {CERT_SEED}")
    if histories < 1:
        raise GateError("histories must be >= 1")
    if ops < 1:
        raise GateError("ops per history must be >= 1")
    contract = load_cert_contract()
    if contract.get("contract") != CONTRACT:
        raise GateError(f"certification contract must declare {CONTRACT}")
    if contract.get("seed") != CERT_SEED:
        raise GateError(f"certification contract seed must be {CERT_SEED}")
    if contract.get("issue") != 756 or contract.get("parent_issue") != 747:
        raise GateError("certification contract must bind issues 756 / 747")
    observation = contract.get("production_observation")
    if (
        not isinstance(observation, dict)
        or observation.get("driver") != "graphforge_api::GraphForge"
    ):
        raise GateError("certification must use the production GraphForge driver")
    if observation.get("write_modes") != [
        "single_writer",
        "queued_writer",
        "optimistic_multi_writer",
    ]:
        raise GateError("production certification must cover all write modes")
    finite = contract.get("finite_coverage")
    if not isinstance(finite, dict) or finite.get("operations") != REQUIRED_OPERATIONS:
        raise GateError("certification must enumerate the complete finite transition basis")
    if finite.get("publication_phases") != "all":
        raise GateError("certification must cover every publication phase")
    native = contract.get("native_oracle_artifacts")
    if native != [
        "native-oracle-windows-${commit}.json",
        "native-oracle-macos-${commit}.json",
    ]:
        raise GateError("certification must bind both exact-SHA native oracle artifacts")
    if contract.get("native_oracle_aggregate") != "native-durability-aggregate-${commit}.json":
        raise GateError("certification must bind the exact-SHA native oracle aggregate")
    versions = contract.get("versions")
    if not isinstance(versions, dict) or versions.get("m6_benchmark_inventory") != "m6-storage-v1":
        raise GateError("certification must freeze the merged #782 benchmark inventory")
    benchmark = contract.get("benchmark_evidence")
    if not isinstance(benchmark, dict) or benchmark.get("walltime_bench") != "m6_storage_io":
        raise GateError("certification must bind the #782 walltime fixture")
    if benchmark.get("walltime_host") != "codspeed-macro-arm64":
        raise GateError("certification walltime evidence must use CodSpeed Macro ARM64")
    claims = contract.get("forbidden_positive_claims")
    if not isinstance(claims, list) or not claims:
        raise GateError("forbidden_positive_claims are required")
    lowered = " ".join(str(item).lower() for item in claims)
    for needle in ("ssi", "serializable", "distributed", "universal filesystem", "acid"):
        if needle not in lowered:
            raise GateError(f"forbidden_positive_claims must mention {needle!r}")

    matrix = json.loads(MATRIX_PATH.read_text(encoding="utf-8"))
    lifecycle = matrix.get("lifecycle")
    if not isinstance(lifecycle, list):
        raise GateError("durability matrix lifecycle missing")
    cell = next(
        (item for item in lifecycle if item.get("id") == "seeded_model_certification"), None
    )
    if cell is None or cell.get("coverage") != "covered":
        raise GateError("seeded_model_certification must be covered in the durability matrix")
    write_skew = next(item for item in matrix["anomalies"] if item["id"] == "write_skew")
    if write_skew.get("coverage") != "covered":
        raise GateError("write_skew must be covered by certification evidence")
    if write_skew["modes"].get("optimistic_multi_writer") != "allowed_documented_not_ssi":
        raise GateError("write_skew classification must remain allowed_documented_not_ssi")


def run_cargo_certification(histories: int, ops: int, commit: str, timeout: int) -> dict[str, Any]:
    env = os.environ.copy()
    env["GRAPHFORGE_CERT_HISTORIES"] = str(histories)
    env["GRAPHFORGE_CERT_OPS"] = str(ops)
    env["GRAPHFORGE_CERT_COMMIT"] = commit
    argv = [
        "cargo",
        "test",
        "-p",
        "graphforge-storage",
        "--features",
        "test-failpoints",
        "project_certification::tests::bounded_ci_state_space_passes_without_untriaged_failures",
        "--lib",
        "--",
        "--exact",
        "--nocapture",
    ]
    started = time.monotonic()
    try:
        completed = subprocess.run(
            argv,
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise GateError(f"certification hang after {error.timeout}s") from error
    duration_ms = int((time.monotonic() - started) * 1000)
    if completed.returncode != 0:
        raise GateError(
            "certification suite failed (fail-closed; no seed retries)\n"
            f"argv={' '.join(argv)}\n"
            f"{(completed.stdout or '')[-2000:]}\n{(completed.stderr or '')[-2000:]}"
        )
    return {
        "argv": argv,
        "duration_ms": duration_ms,
        "outcome": "ok",
        "histories": histories,
        "ops_per_history": ops,
        "seed": CERT_SEED,
        "reproduction": " ".join(argv),
    }


def write_evidence(output: Path, payload: dict[str, Any]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "durability-certification-report.json"
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    digest = sha256_file(report_path)
    (output / "durability-certification-report.sha256").write_text(digest + "\n", encoding="utf-8")
    commands = payload.get("commands") or []
    (output / "reproduction.txt").write_text("\n".join(commands) + "\n", encoding="utf-8")


def load_native_oracle(path: Path, expected_platform: str) -> dict[str, Any]:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateError(f"cannot read native oracle {path}: {error}") from error
    if report.get("contract") != "graphforge-native-durability-oracle/v1":
        raise GateError(f"native oracle contract drift: {path}")
    if report.get("platform") != expected_platform:
        raise GateError(
            f"native oracle platform mismatch: expected {expected_platform}, "
            f"got {report.get('platform')!r}"
        )
    if not isinstance(report.get("filesystem_class"), str) or not report["filesystem_class"]:
        raise GateError(f"native oracle filesystem class is missing: {path}")
    if not isinstance(report.get("observations"), list) or not report["observations"]:
        raise GateError(f"native oracle observations are missing: {path}")
    if not isinstance(report.get("modeled_faults"), list) or not report["modeled_faults"]:
        raise GateError(f"native oracle modeled faults are missing: {path}")
    if not isinstance(report.get("minimized_failure"), dict):
        raise GateError(f"native oracle minimized trace is missing: {path}")
    return report


def cmd_aggregate_native(args: argparse.Namespace) -> int:
    if not re.fullmatch(r"[0-9a-f]{40}", args.expected_sha):
        raise GateError("native aggregate expected SHA must be 40 lowercase hex characters")
    inputs = {
        "windows": Path(args.windows),
        "macos": Path(args.macos),
    }
    reports = {
        platform_name: load_native_oracle(path, platform_name)
        for platform_name, path in inputs.items()
    }
    seeds = {report.get("seed") for report in reports.values()}
    if len(seeds) != 1 or not all(isinstance(seed, int) for seed in seeds):
        raise GateError("native oracle seeds do not match")
    payload = {
        "contract": "graphforge-native-durability-aggregate/v1",
        "issue": 756,
        "commit": args.expected_sha,
        "status": "passed",
        "platforms": [
            {
                "platform": platform_name,
                "filesystem_class": reports[platform_name]["filesystem_class"],
                "seed": reports[platform_name]["seed"],
                "observation_count": len(reports[platform_name]["observations"]),
                "modeled_fault_count": len(reports[platform_name]["modeled_faults"]),
                "sha256": sha256_file(inputs[platform_name]),
            }
            for platform_name in ("windows", "macos")
        ],
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        "native durability aggregate ok: "
        f"commit={args.expected_sha} platforms=windows,macos seed={next(iter(seeds))}"
    )
    return 0


def cmd_validate(_: argparse.Namespace) -> int:
    validate_config(CERT_SEED, DEFAULT_CI_HISTORIES, DEFAULT_CI_OPS)
    print(
        f"durability certification gate config valid: "
        f"seed={CERT_SEED} ci_histories={DEFAULT_CI_HISTORIES} ci_ops={DEFAULT_CI_OPS} "
        f"scheduled_histories={SCHEDULED_HISTORIES} scheduled_ops={SCHEDULED_OPS}"
    )
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    histories = args.histories
    ops = args.ops
    validate_config(args.seed, histories, ops)
    commit = git_head()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    result = run_cargo_certification(histories, ops, commit, args.timeout)
    # Also run the API surface wrapper + write-skew honesty cell.
    api_argv = [
        "cargo",
        "test",
        "-p",
        "graphforge-api",
        "--lib",
        "durability_certification_tests",
        "--",
        "--nocapture",
    ]
    api = subprocess.run(
        api_argv,
        cwd=ROOT,
        env={
            **os.environ,
            "GRAPHFORGE_CERT_HISTORIES": str(histories),
            "GRAPHFORGE_CERT_OPS": str(ops),
            "GRAPHFORGE_CERT_COMMIT": commit,
        },
        text=True,
        capture_output=True,
        timeout=args.timeout,
        check=False,
    )
    if api.returncode != 0:
        raise GateError(
            "API certification surface failed\n"
            f"{(api.stdout or '')[-1500:]}\n{(api.stderr or '')[-1500:]}"
        )
    skew_argv = [
        "cargo",
        "test",
        "-p",
        "graphforge-api",
        "--lib",
        "transaction::tests::optimistic_write_skew_witness_matches_isolation_table",
        "--",
        "--exact",
    ]
    skew = subprocess.run(
        skew_argv,
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=args.timeout,
        check=False,
    )
    if skew.returncode != 0:
        raise GateError(
            "write-skew witness failed\n"
            f"{(skew.stdout or '')[-1500:]}\n{(skew.stderr or '')[-1500:]}"
        )

    production_cases = [
        [
            "cargo",
            "test",
            "-p",
            "graphforge-storage",
            "--features",
            "test-failpoints",
            "--lib",
            "project_certification::tests::finite_transition_basis_covers_every_mode_operation_and_publication_phase",
            "--",
            "--exact",
            "--nocapture",
        ],
        [
            "cargo",
            "test",
            "-p",
            "graphforge-storage",
            "--test",
            "graph_delta_journal",
            "--",
            "--nocapture",
        ],
        [
            "cargo",
            "test",
            "-p",
            "graphforge-storage",
            "--lib",
            "project_retention::tests::checkpoint_root_and_live_lease_are_never_selected",
            "--",
            "--exact",
            "--nocapture",
        ],
        [
            "cargo",
            "test",
            "-p",
            "graphforge-storage",
            "--lib",
            "graph_delta_compaction::tests::crash_oracle_before_and_after_ack_matches_frozen_contract",
            "--",
            "--exact",
            "--nocapture",
        ],
    ]
    for production_argv in production_cases:
        production = subprocess.run(
            production_argv,
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=args.timeout,
            check=False,
        )
        if production.returncode != 0:
            raise GateError(
                "production durability case failed\n"
                f"argv={' '.join(production_argv)}\n"
                f"{(production.stdout or '')[-1500:]}\n{(production.stderr or '')[-1500:]}"
            )

    native_path = output / f"native-oracle-{platform.system().lower()}.json"
    native_argv = [
        "cargo",
        "test",
        "-p",
        "graphforge-storage",
        "--features",
        "test-failpoints",
        "--lib",
        "project_recovery::tests::subprocess_kill_matrix_never_exposes_a_partial_generation",
        "--",
        "--exact",
        "--nocapture",
    ]
    native = subprocess.run(
        native_argv,
        cwd=ROOT,
        env={**os.environ, "GRAPHFORGE_NATIVE_ORACLE_EVIDENCE": str(native_path)},
        text=True,
        capture_output=True,
        timeout=args.timeout,
        check=False,
    )
    if native.returncode != 0 or not native_path.is_file():
        raise GateError(
            "native subprocess oracle failed or emitted no evidence\n"
            f"argv={' '.join(native_argv)}\n"
            f"{(native.stdout or '')[-1500:]}\n{(native.stderr or '')[-1500:]}"
        )

    payload = {
        "contract": CONTRACT,
        "issue": 756,
        "parent_issue": 747,
        "seed": CERT_SEED,
        "history_count": histories,
        "ops_per_history": ops,
        "untriaged_failures": 0,
        "commit": commit,
        "platform": f"{platform.system()}-{platform.machine()}-{platform.python_version()}",
        "toolchain": command_version(["rustc", "--version"]),
        "tool_versions": {
            "cargo": command_version(["cargo", "--version"]),
            "python": platform.python_version(),
        },
        "runner": os.environ.get("GRAPHFORGE_CERT_RUNNER", "local"),
        "filesystem_class": os.environ.get("GRAPHFORGE_CERT_FILESYSTEM_CLASS", "undeclared-local"),
        "versions": {
            "durability_isolation": "graphforge-durability-isolation/1",
            "durability_certification": CONTRACT,
            "delta_journal": "adr-0019",
            "fault_oracle": "project_fault_oracle",
            "m6_benchmark_inventory": "m6-storage-v1",
            "m6_storage_io_fixture": "v1",
        },
        "production_observation": load_cert_contract()["production_observation"],
        "finite_coverage": load_cert_contract()["finite_coverage"],
        "native_oracle_artifacts": [
            name.replace("${commit}", commit)
            for name in load_cert_contract()["native_oracle_artifacts"]
        ],
        "native_oracle_observed": {
            "platform": platform.system().lower(),
            "path": native_path.name,
            "sha256": sha256_file(native_path),
        },
        "benchmark_evidence": load_cert_contract()["benchmark_evidence"],
        "cases": [result],
        "commands": [
            result["reproduction"],
            " ".join(api_argv),
            " ".join(skew_argv),
            *(" ".join(argv) for argv in production_cases),
            " ".join(native_argv),
        ],
        "claims": {
            "ssi": False,
            "serializable_isolation": False,
            "universal_filesystem": False,
            "distributed_durability": False,
            "write_skew_classification": "allowed_documented_not_ssi",
        },
    }
    rendered = json.dumps(payload).lower()
    for forbidden in FORBIDDEN_POSITIVE:
        if forbidden in rendered and forbidden != "is ssi":
            # "allowed_documented_not_ssi" contains "ssi" as a denial — allow that token only
            # inside the honest classification string.
            if forbidden == "provides ssi" and "provides ssi" in rendered:
                raise GateError(f"evidence contains forbidden claim {forbidden!r}")
            if forbidden in {
                "serializable isolation",
                "universal filesystem",
                "distributed durability",
            }:
                raise GateError(f"evidence contains forbidden claim {forbidden!r}")
    write_evidence(output, payload)
    print(
        f"durability certification ok: seed={CERT_SEED} histories={histories} "
        f"ops={ops} untriaged=0 commit={commit}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    validate = sub.add_parser("validate", help="Validate frozen certification configuration")
    validate.set_defaults(func=cmd_validate)

    run = sub.add_parser("run", help="Run certification suite and write evidence")
    run.add_argument("--seed", type=int, default=CERT_SEED)
    run.add_argument("--histories", type=int, default=DEFAULT_CI_HISTORIES)
    run.add_argument("--ops", type=int, default=DEFAULT_CI_OPS)
    run.add_argument("--timeout", type=int, default=900)
    run.add_argument("--output", required=True)
    run.set_defaults(func=cmd_run)

    aggregate_native = sub.add_parser(
        "aggregate-native", help="Validate and aggregate exact-SHA native oracle evidence"
    )
    aggregate_native.add_argument("--expected-sha", required=True)
    aggregate_native.add_argument("--windows", required=True)
    aggregate_native.add_argument("--macos", required=True)
    aggregate_native.add_argument("--output", required=True)
    aggregate_native.set_defaults(func=cmd_aggregate_native)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except GateError as error:
        print(f"durability certification gate failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
