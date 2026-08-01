#!/usr/bin/env python3
"""Scheduled/manual bounded-resource concurrency stress evidence (#2417)."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SEED = 2417
DEFAULT_ITERATIONS = 24
DEFAULT_TIMEOUT_SECONDS = 900
RSS_GROWTH_BOUND_BYTES = 512 * 1024 * 1024
FD_GROWTH_BOUND = 256


class GateError(RuntimeError):
    """Concurrency stress gate failure."""


def _rss_scale() -> int:
    return 1 if sys.platform == "darwin" else 1024


def peak_rss_bytes(who: int = resource.RUSAGE_CHILDREN) -> int:
    """Peak RSS in bytes for reaped child workloads (not this gate process)."""
    usage = resource.getrusage(who)
    return int(usage.ru_maxrss) * _rss_scale()


def open_fd_count() -> int | None:
    """Open FD count for this gate process (gate-side self-check, not child FD accounting)."""
    fd_dir = Path("/proc/self/fd")
    if fd_dir.is_dir():
        return len(list(fd_dir.iterdir()))
    return None


def inventory(root: Path, name: str) -> list[str]:
    path = root / name
    if not path.is_dir():
        return []
    return sorted(entry.name for entry in path.iterdir())


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


def run_rust_case(case: str, env: dict[str, str], timeout: int) -> dict[str, Any]:
    argv = [
        "cargo",
        "test",
        "-p",
        "graphforge-api",
        "--lib",
        case,
        "--",
        "--exact",
    ]
    started = time.monotonic()
    before_rss = peak_rss_bytes()
    before_fd = open_fd_count()
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
        raise GateError(f"{case}: hang detected after {error.timeout}s") from error
    duration_ms = int((time.monotonic() - started) * 1000)
    if completed.returncode != 0:
        raise GateError(
            f"{case}: stress case failed exit={completed.returncode}\n"
            f"{(completed.stdout or '')[-1500:]}\n{(completed.stderr or '')[-1500:]}"
        )
    after_rss = peak_rss_bytes()
    after_fd = open_fd_count()
    return {
        "case": case,
        "argv": argv,
        "duration_ms": duration_ms,
        "peak_rss_bytes": max(before_rss, after_rss),
        "open_fds_before": before_fd,
        "open_fds_after": after_fd,
        "outcome": "ok",
        "reproduction": " ".join(argv),
    }


def mixed_python_workload(
    seed: int, iterations: int, work: Path, timeout: int = DEFAULT_TIMEOUT_SECONDS
) -> dict[str, Any]:
    script = f"""
import graphforge as g
from pathlib import Path

seed = {seed}
iterations = {iterations}
root = Path({str(work)!r}) / f"py-stress-{{seed}}"
root.mkdir(parents=True, exist_ok=True)
project = root / "project"
project.mkdir()
forge = g.GraphForge(str(project))
forge.execute("CREATE (:Person {{name:'Alpha'}})")
for index in range(iterations):
    token = g.CancellationToken()
    token.cancel()
    try:
        forge.list_checkpoints(cancellation=token)
        raise AssertionError("expected GF_CANCELLED from cancelled list_checkpoints")
    except g.GraphForgeError as error:
        assert error.code == "GF_CANCELLED", error.code
    peer = forge.list_checkpoints()
    assert peer.num_rows == 0, peer.num_rows
    forge.execute(
        "CREATE (:Person {{name:$name}})",
        {{"name": f"P{{seed}}-{{index}}"}},
    )
    names = forge.execute(
        "MATCH (n:Person) RETURN n.name AS name ORDER BY name"
    ).column("name").to_pylist()
    assert names[0] == "Alpha"
forge.close()
reopened = g.GraphForge(str(project))
assert reopened.execute("MATCH (n:Person) RETURN n").num_rows == iterations + 1
reopened.close()
print("python-stress-ok")
"""
    started = time.monotonic()
    before_rss = peak_rss_bytes()
    before_fd = open_fd_count()
    try:
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=ROOT,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise GateError(f"python-mixed-workload: hang detected after {error.timeout}s") from error
    duration_ms = int((time.monotonic() - started) * 1000)
    if completed.returncode != 0 or "python-stress-ok" not in (completed.stdout or ""):
        raise GateError(
            "python mixed stress failed\n"
            f"{(completed.stdout or '')[-1500:]}\n{(completed.stderr or '')[-1500:]}"
        )
    project = work / f"py-stress-{seed}" / "project"
    leftover_locks = inventory(project, ".")
    live_locks = [name for name in leftover_locks if name.endswith(".lock")]
    if live_locks:
        raise GateError(f"python stress leaked locks: {live_locks}")
    transactions = project / "transactions"
    for journal in sorted(transactions.glob("*.json")) if transactions.is_dir() else []:
        try:
            payload = json.loads(journal.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise GateError(f"unreadable journal path={journal}: {error}") from error
        phase = payload.get("phase")
        if phase not in {"COMMITTED", "ABORTED"}:
            raise GateError(f"unexpected journal phase={phase!r} path={journal}")
    return {
        "case": "python-mixed-workload",
        "seed": seed,
        "iterations": iterations,
        "duration_ms": duration_ms,
        "peak_rss_bytes": max(before_rss, peak_rss_bytes()),
        "open_fds_before": before_fd,
        "open_fds_after": open_fd_count(),
        "outcome": "ok",
        "reproduction": (
            f"GF_STRESS_SEED={seed} GF_STRESS_ITERATIONS={iterations} "
            f"python3 scripts/ci/concurrency-stress-gate.py run --seed {seed} "
            f"--iterations {iterations} --timeout-seconds {timeout} "
            f"--output /tmp/gf-concurrency-stress"
        ),
    }


def validate_config(seed: int, iterations: int, timeout: int) -> None:
    if seed != DEFAULT_SEED:
        raise GateError(f"published stress seed must remain {DEFAULT_SEED}")
    if iterations <= 0 or iterations > 10_000:
        raise GateError("iterations must be in 1..10000")
    if timeout <= 0:
        raise GateError("timeout must be positive")


def write_stress_report(
    output: Path,
    *,
    seed: int,
    iterations: int,
    timeout: int,
    baseline_rss: int,
    baseline_fd: int | None,
    results: list[dict[str, Any]],
    started: float,
    failure: str | None,
) -> None:
    report = {
        "gate": "Concurrency Stress",
        "schema_version": 1,
        "commit": git_head(),
        "platform": platform.platform(),
        "toolchain": {
            "python": sys.version.split()[0],
            "rustc": subprocess.run(
                ["rustc", "--version"],
                text=True,
                capture_output=True,
                check=False,
            ).stdout.strip(),
        },
        "seed": seed,
        "iterations": iterations,
        "timeout_seconds": timeout,
        "bounds": {
            "rss_growth_bytes": RSS_GROWTH_BOUND_BYTES,
            "fd_growth": FD_GROWTH_BOUND,
        },
        "baseline": {"peak_rss_bytes": baseline_rss, "open_fds": baseline_fd},
        "cases": results,
        "duration_ms": int((time.monotonic() - started) * 1000),
        "summary": {
            "cases": len(results),
            "passed": failure is None,
            "correctness_gate": True,
            "performance_observation": False,
        },
        "failure": failure,
        "notes": (
            "This lane is bounded-resource correctness evidence. Throughput "
            "or latency numbers are non-blocking observations and must not "
            "greenwash a failed required short concurrency matrix. "
            "peak_rss_bytes uses RUSAGE_CHILDREN (max over reaped workloads). "
            "open_fds_* is a gate-process self-check, not child FD accounting."
        ),
    }
    (output / "concurrency-stress-report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    repro_lines = [item["reproduction"] for item in results if item.get("reproduction")]
    if failure:
        repro_lines.insert(0, f"# failure: {failure}")
    repro_lines.append(
        f"python3 scripts/ci/concurrency-stress-gate.py run --seed {seed} "
        f"--iterations {iterations} --timeout-seconds {timeout} --output {output}"
    )
    (output / "reproduction.txt").write_text("\n".join(repro_lines) + "\n", encoding="utf-8")


def run_stress(output: Path, seed: int, iterations: int, timeout: int) -> int:
    validate_config(seed, iterations, timeout)
    output.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="gf-concurrency-stress-", dir=str(output)))
    env = os.environ.copy()
    env.setdefault("CARGO_TERM_COLOR", "never")
    env["TMPDIR"] = str(work)
    env["TEMP"] = str(work)
    env["TMP"] = str(work)
    started = time.monotonic()
    baseline_rss = peak_rss_bytes()
    baseline_fd = open_fd_count()
    cases = [
        "same_process_concurrency_tests::independent_instances_and_one_instance_reads_are_deterministic",
        "stream_cancellation_isolation_tests::cooperative_token_cancellation_does_not_cancel_concurrent_peer",
        "shared_directory_semantics_tests::competing_writer_fails_before_its_staging_or_publication",
        "multi_process_publication_tests::published_child_is_visible_only_to_fresh_current_reader",
        "composite_recovery_tests::composite_kill_reopen_matrix_never_exposes_mixed_state",
    ]
    results: list[dict[str, Any]] = []
    failure: str | None = None
    try:
        for case in cases:
            results.append(run_rust_case(case, env, timeout))
        results.append(mixed_python_workload(seed, iterations, work, timeout))
        peak_rss = max(item["peak_rss_bytes"] for item in results)
        if peak_rss - baseline_rss > RSS_GROWTH_BOUND_BYTES:
            raise GateError(f"RSS growth exceeded bound baseline={baseline_rss} peak={peak_rss}")
        final_fd = open_fd_count()
        if (
            baseline_fd is not None
            and final_fd is not None
            and final_fd - baseline_fd > FD_GROWTH_BOUND
        ):
            raise GateError(
                f"file-descriptor growth exceeded bound baseline={baseline_fd} final={final_fd}"
            )
        write_stress_report(
            output,
            seed=seed,
            iterations=iterations,
            timeout=timeout,
            baseline_rss=baseline_rss,
            baseline_fd=baseline_fd,
            results=results,
            started=started,
            failure=None,
        )
        print(
            "concurrency stress gate passed: "
            f"{len(results)} cases seed={seed} duration_ms="
            f"{int((time.monotonic() - started) * 1000)}"
        )
        return 0
    except GateError as error:
        failure = str(error)
        write_stress_report(
            output,
            seed=seed,
            iterations=iterations,
            timeout=timeout,
            baseline_rss=baseline_rss,
            baseline_fd=baseline_fd,
            results=results,
            started=started,
            failure=failure,
        )
        raise
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("validate")
    run = subparsers.add_parser("run")
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--seed", type=int, default=DEFAULT_SEED)
    run.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    run.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    args = parser.parse_args()
    try:
        if args.command == "run":
            return run_stress(args.output, args.seed, args.iterations, args.timeout_seconds)
        validate_config(DEFAULT_SEED, DEFAULT_ITERATIONS, DEFAULT_TIMEOUT_SECONDS)
    except GateError as error:
        print(f"concurrency stress gate failed: {error}", file=sys.stderr)
        return 1
    print(
        f"concurrency stress gate config valid: seed={DEFAULT_SEED} iterations={DEFAULT_ITERATIONS}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
