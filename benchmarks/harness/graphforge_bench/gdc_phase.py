"""Run one GDC scorecard rung phase inside BenchExec.

``python -m graphforge_bench.gdc_phase run TASK.json`` executes one phase task
(``graphforge-gdc-phase-task/1``) and prints exactly one
``graphforge-gdc-phase/1`` telemetry line on standard output:

- ``convert``: ``graphforge-benchmark-gdc-scorecard convert``.
- ``load``: ``gf import-session begin``, ``register-parquet`` for every
  converted node file then every edge file, ``validate`` and ``commit``: the
  ordinary public load path, one process per call.
- ``query``: ``gf storage-attribution`` for the published project size, then
  ``graphforge-benchmark-gdc-scorecard query``, which reopens the project,
  reconciles its counts and times every query with the driver clock.

BenchExec owns the phase's wall time, CPU time and process-tree resources. The
telemetry adds the largest single-process peak RSS among the phase's children
(``getrusage(RUSAGE_CHILDREN).ru_maxrss``, the kernel's per-process high-water
mark), the product receipts, and a typed failure cause. Each step's duration
is diagnostic only. The first failing step stops the phase and the process
exits 1; the query driver's own exit 3 (evidence written, some samples failed)
is reported as ``query_failed`` with the evidence kept.

This module runs inside the BenchExec container and imports only the
standard library.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
from pathlib import Path
import resource
import subprocess
import sys
import time
from typing import Any

TASK_SCHEMA = "graphforge-gdc-phase-task/1"
TELEMETRY_SCHEMA = "graphforge-gdc-phase/1"
PHASES = ("convert", "load", "query")
ERROR_TAIL_BYTES = 2048


class PhaseError(Exception):
    """A step failed; `cause` is the typed rung failure cause."""

    def __init__(self, cause: str, step: str, exit_code: int | None, detail: str) -> None:
        super().__init__(cause)
        self.cause = cause
        self.step = step
        self.exit_code = exit_code
        self.detail = detail


def _tail(text: str) -> str:
    encoded = text.strip().encode("utf-8", errors="replace")
    if len(encoded) <= ERROR_TAIL_BYTES:
        return encoded.decode("utf-8", errors="replace")
    return "..." + encoded[-ERROR_TAIL_BYTES:].decode("utf-8", errors="replace")


def _reported_cause(stderr: str) -> str | None:
    """The `{"error": {"cause": ...}}` line a GDC scorecard command prints."""
    for line in reversed(stderr.strip().splitlines()):
        try:
            document = json.loads(line)
        except json.JSONDecodeError:
            continue
        error = document.get("error") if isinstance(document, Mapping) else None
        cause = error.get("cause") if isinstance(error, Mapping) else None
        if isinstance(cause, str) and cause:
            return cause
    return None


class Phase:
    """Runs steps in order and records each one's exit and duration."""

    def __init__(self) -> None:
        self.steps: list[dict[str, Any]] = []

    def step(
        self, name: str, argv: Sequence[str], *, failure: str
    ) -> subprocess.CompletedProcess[str]:
        started = time.monotonic()
        completed = subprocess.run(
            list(argv), check=False, capture_output=True, text=True, stdin=subprocess.DEVNULL
        )
        self.steps.append(
            {
                "step": name,
                "exit_code": completed.returncode,
                "duration_ms": round((time.monotonic() - started) * 1000),
            }
        )
        if completed.returncode != 0:
            raise PhaseError(
                failure,
                name,
                completed.returncode,
                _tail(completed.stderr or completed.stdout),
            )
        return completed


def _json_stdout(completed: subprocess.CompletedProcess[str], step: str) -> dict[str, Any]:
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise PhaseError("receipt_malformed", step, 0, _tail(completed.stdout)) from error
    if not isinstance(value, dict):
        raise PhaseError("receipt_malformed", step, 0, _tail(completed.stdout))
    return value


def _convert(task: Mapping[str, Any], phase: Phase) -> dict[str, Any]:
    argv = [
        task["converter"],
        "convert",
        "--mapping",
        task["mapping"],
        "--input-root",
        task["input_root"],
        "--output-dir",
        task["output_dir"],
    ]
    try:
        phase.step("convert", argv, failure="convert_failed")
    except PhaseError as error:
        error.detail = f"{_reported_cause(error.detail) or 'unknown'}: {error.detail}"
        raise
    return {"conversion_manifest": str(Path(task["output_dir"]) / "conversion-manifest.json")}


def _load(task: Mapping[str, Any], phase: Phase) -> dict[str, Any]:
    gf = [task["gf"], "--json", "--project", task["project"], "import-session"]
    begun = phase.step(
        "begin",
        [*gf, "begin", "--operation-uuid", task["operation_uuid"]],
        failure="load_failed",
    )
    session = _json_stdout(begun, "begin").get("session_uuid")
    if not isinstance(session, str) or not session:
        raise PhaseError("receipt_malformed", "begin", 0, "begin returned no session_uuid")
    converted = Path(task["converted"])
    registered = 0
    for kind in ("nodes", "edges"):
        for path in sorted((converted / kind).glob("*.parquet")):
            phase.step(
                f"register-{kind}",
                [
                    *gf,
                    "register-parquet",
                    "--session-uuid",
                    session,
                    "--path",
                    str(path),
                    "--kind",
                    kind,
                ],
                failure="load_failed",
            )
            registered += 1
    if registered == 0:
        raise PhaseError("load_failed", "register", None, "no converted Parquet files")
    phase.step("validate", [*gf, "validate", "--session-uuid", session], failure="load_failed")
    committed = phase.step(
        "commit", [*gf, "commit", "--session-uuid", session], failure="load_failed"
    )
    receipt = _json_stdout(committed, "commit")
    if receipt.get("outcome") != "committed":
        raise PhaseError("load_failed", "commit", 0, f"outcome {receipt.get('outcome')!r}")
    return {"commit": receipt, "registered_files": registered}


def _query(task: Mapping[str, Any], phase: Phase) -> dict[str, Any]:
    attributed = phase.step(
        "storage-attribution",
        [task["gf"], "--json", "--project", task["project"], "storage-attribution"],
        failure="storage_attribution_failed",
    )
    storage = _json_stdout(attributed, "storage-attribution")
    argv = [
        task["driver"],
        "query",
        "--project",
        task["project"],
        "--workload",
        task["workload"],
        "--expected-counts",
        task["expected_counts"],
        "--output",
        task["evidence"],
    ]
    if task.get("results_dir") is not None:
        argv += ["--results-dir", task["results_dir"]]
    try:
        phase.step("query", argv, failure="query_driver_failed")
    except PhaseError as error:
        # Exit 3 writes evidence with failed samples; exit 2 refuses with a
        # typed cause (for example count_mismatch) and writes none.
        reported = _reported_cause(error.detail)
        if error.exit_code == 3:
            error.cause = "query_failed"
        elif error.exit_code == 2 and reported is not None:
            error.cause = reported
        raise
    return {"storage_attribution": storage, "evidence": task["evidence"]}


ACTIONS = {"convert": _convert, "load": _load, "query": _query}


def peak_child_rss_bytes() -> int:
    """Largest single-process resident high-water mark among waited children."""
    # Linux reports ru_maxrss in KiB.
    return int(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss) * 1024


def run_task(task: Mapping[str, Any]) -> dict[str, Any]:
    if task.get("schema") != TASK_SCHEMA or task.get("phase") not in PHASES:
        raise ValueError(f"phase task must be {TASK_SCHEMA} naming one of {PHASES}")
    name = str(task["phase"])
    phase = Phase()
    started = time.monotonic()
    failure: dict[str, Any] | None = None
    receipts: dict[str, Any] = {}
    try:
        receipts = ACTIONS[name](task, phase)
    except PhaseError as error:
        failure = {
            "cause": error.cause,
            "step": error.step,
            "exit_code": error.exit_code,
            "detail": error.detail,
        }
    except OSError as error:
        failure = {
            "cause": f"{name}_failed",
            "step": "spawn",
            "exit_code": None,
            "detail": str(error),
        }
    duration_ms = round((time.monotonic() - started) * 1000)
    peak = peak_child_rss_bytes()
    return {
        "schema": TELEMETRY_SCHEMA,
        "phase": name,
        "status": "failed" if failure else "passed",
        "failure": failure,
        "phases": [{"phase": name, "duration_ms": duration_ms, "peak_rss_bytes": peak}],
        "steps": phase.steps,
        "peak_rss_bytes": peak,
        "receipts": receipts,
    }


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 2 or arguments[0] != "run":
        print("usage: python -m graphforge_bench.gdc_phase run TASK.json", file=sys.stderr)
        return 2
    task = json.loads(Path(arguments[1]).read_text(encoding="utf-8"))
    telemetry = run_task(task)
    print(json.dumps(telemetry, sort_keys=True), flush=True)
    return 0 if telemetry["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
