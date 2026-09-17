"""Read-only runtime diagnosis from authenticated ordinary S20/S22 receipts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from graphforge_bench.native_rung import read_native_rung
from graphforge_bench.progressive_qualification import METRICS, Profile, _project, project


def summarize(documents: dict[str, Any]) -> dict[str, Any]:
    """Keep lifecycle, public-call, and I/O scopes separate."""
    phases = documents["graphforge"]["phases"]
    ingest = next(phase for phase in phases if phase["phase"] == "ingest")
    calls = {}
    for receipt in ingest.get("receipts", []):
        for operation, timing in receipt.get("operation_timings", {}).items():
            total = calls.setdefault(operation, dict.fromkeys(("calls", "errors", "elapsed_ns"), 0))
            for key, value in timing.items():
                total[key] += value
    if not calls:
        raise ValueError("runtime diagnosis requires operation timing receipts")
    call_ns = sum(timing["elapsed_ns"] for timing in calls.values())
    return {
        "identities": documents["result"]["identities"],
        "whole_lifecycle_benchexec": documents["benchexec"]["authority"],
        "phase_wall_ms": {phase["phase"]: phase["duration_ms"] for phase in phases},
        "process_peak_rss_bytes": documents["rung"]["metrics"]["peak_rss_bytes"],
        "construction_calls": calls,
        "ingest_outside_construction_calls_ns": ingest["duration_ms"] * 1_000_000 - call_ns,
        "lifecycle_outside_phase_wall_seconds": documents["benchexec"]["authority"]["wall_seconds"]
        - sum(phase["duration_ms"] for phase in phases) / 1000,
        "construction_application_io": documents["rung"]["storage_attribution"]["construction"][
            "application_io"
        ],
        # Per-phase I/O for every other lifecycle phase, same shape (#1389).
        # Absent for evidence recorded before that instrumentation existed, so
        # reports derived from historical bundles stay byte-comparable.
        **(
            {"lifecycle_application_io": lifecycle_io}
            if (
                lifecycle_io := documents["rung"]["storage_attribution"].get(
                    "lifecycle_application_io"
                )
            )
            else {}
        ),
        "metrics": documents["rung"]["metrics"],
    }


def analyze(root: Path, source: Path) -> dict[str, Any]:
    documents = [read_native_rung(root, source, scale) for scale in (20, 22)]
    # Scale-specific profile/projection identities differ; executables, workload
    # generator, host policy and measurement tools must be the same.
    varying = {"profile_id", "profile_sha256", "admitted_projection_sha256"}
    identities = [
        {key: value for key, value in item["result"]["identities"].items() if key not in varying}
        for item in documents
    ]
    if identities[0] != identities[1]:
        raise ValueError(
            "runtime sources have different executable, source, host or tool identities"
        )
    low, high = [item["rung"] for item in documents]
    capacity = documents[1]["plan"]["work_root_capacity"]
    admission = project(
        Profile("graph500-s24-provider", 24, "provider", (20, 22)),
        [low, high],
        native_capacity={key: capacity[key] for key in ("free_bytes", "reserved_headroom_bytes")},
    )
    return {
        "issue": 1279,
        "claim": "historical_runtime_diagnosis_only",
        "measurement_notes": [
            "Public seal includes shaping and canonical encoding; "
            "it is not isolated authentication.",
            "Construction-call wall times are nested inside ingest, "
            "which is inside lifecycle wall.",
            "Remainders include uninstrumented work and timing precision; they are not CPU costs.",
            "BenchExec peak memory includes cgroup/page cache; process RSS is reported separately.",
            "Logical I/O and physical I/O are different observations and must not be added.",
            "Capacity is the historical S22 plan sample, not current host free space.",
            "S25/S26 extrapolations are diagnostic, not adjacent-rung admission.",
        ],
        "rungs": {f"S{scale}": summarize(item) for scale, item in zip((20, 22), documents)},
        "diagnostic_extrapolations": {
            f"S{scale}": {
                metric: _project(low, high, 16 * (1 << scale), metric)
                for metric in METRICS
                if metric != "peak_rss_bytes"
            }
            for scale in (24, 25, 26)
        },
        "historical_s24_admission": admission,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    print(json.dumps(analyze(root, args.evidence), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
