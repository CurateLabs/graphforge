"""Reproduce #1282 ingestion attribution without running a workload or admission."""

from __future__ import annotations

import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
from typing import Any

from graphforge_bench.lifecycle_runtime import summarize
from graphforge_bench.native_rung import read_native_rung

SCALES = (18, 19, 20, 22)
COUNTERS = (
    "rows_read",
    "rows_written",
    "fixed_read_bytes",
    "fixed_written_bytes",
    "parquet_read_bytes",
    "parquet_written_bytes",
    "sync_calls",
    "inclusive_wall_ns",
)


def analyze(root: Path, evidence: Path) -> dict[str, Any]:
    documents = [read_native_rung(root, evidence, scale) for scale in SCALES]
    varying = {"profile_id", "profile_sha256", "admitted_projection_sha256"}
    identities = [
        {k: v for k, v in doc["result"]["identities"].items() if k not in varying}
        for doc in documents
    ]
    if any(identity != identities[0] for identity in identities[1:]):
        raise ValueError("prefix contains different source, executable, generator, host or tools")
    rungs = {}
    for scale, doc in zip(SCALES, documents, strict=True):
        item = summarize(doc)
        ingest = item["phase_wall_ms"]["ingest"] / 1000
        wall = item["whole_lifecycle_benchexec"]["wall_seconds"]
        if wall <= 0 or not 0 <= ingest <= wall:
            raise ValueError("invalid ingestion/lifecycle duration")
        item["ingestion_fraction"] = ingest / wall
        item["construction_call_wall_ns"] = sum(
            call["elapsed_ns"] for call in item["construction_calls"].values()
        )
        rungs[f"S{scale}"] = item
    return {
        "issue": 1282,
        "claim": "validated_completed_prefix_observations_not_isolated_speedup",
        "rungs": rungs,
        "artifacts": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(evidence.glob("s*.json"))
            if any(path.name.startswith(f"s{scale}-") for scale in SCALES)
        },
        "scope_notes": [
            "Public construction calls are disjoint and nested inside ingestion and lifecycle.",
            "Seal includes shaping and encoding; resume excludes recovery during facade open.",
            "Residual time includes input preparation, checkpoints and measurement precision.",
            "Logical I/O, physical I/O, CPU, sync latency, process RSS and cgroup memory differ.",
            "One accepted observation per scale does not isolate repairs or estimate variance.",
        ],
    }


def merge_summary(lines: list[str]) -> dict[str, Any]:
    """Aggregate non-overlapping merge calls from one diagnostic command only."""
    families: dict[str, Any] = {}
    for line in lines:
        if not line.startswith("INGEST_DIAGNOSTIC "):
            continue
        event = json.loads(line.removeprefix("INGEST_DIAGNOSTIC "))
        if event["event"] == "scope":
            continue
        family = event["family"]
        fixed = {
            "merge-identities",
            "merge-node-details",
            "merge-edge-details",
            "merge-endpoints",
            "merge-resolved",
        }
        row_family = any(
            family.startswith(prefix) and family.removeprefix(prefix).isdigit()
            for prefix in ("node-rows-", "edge-rows-")
        )
        if family not in fixed and not row_family:
            raise ValueError("unknown or unsanitized merge family")
        entry = families.setdefault(
            family, {"groups": 0, "max_scheduler_level": 0, **dict.fromkeys(COUNTERS, 0)}
        )
        if event["event"] == "inputs":
            if "input_runs" in entry or type(event["runs"]) is not int or event["runs"] < 1:
                raise ValueError("duplicate or invalid family inputs")
            entry["input_runs"] = event["runs"]
        elif event["event"] == "group":
            if event["success"] is not True:
                raise ValueError("incomplete merge diagnostic")
            for key in (*COUNTERS, "level", "inputs"):
                if type(event[key]) is not int or event[key] < 0:
                    raise ValueError("missing, decreasing or invalid merge counter")
            if not 1 <= event["inputs"] <= 32 or event["level"] < 1:
                raise ValueError("diagnostic differs from production fan-in")
            entry["groups"] += 1
            entry["max_scheduler_level"] = max(entry["max_scheduler_level"], event["level"])
            for key in COUNTERS:
                entry[key] += event[key]
        else:
            raise ValueError("unknown diagnostic event")
    if not families or any("input_runs" not in entry for entry in families.values()):
        raise ValueError("missing completed family input census")
    if any(entry["input_runs"] > 1 and entry["groups"] == 0 for entry in families.values()):
        raise ValueError("missing merge groups for multi-run family")
    if any(entry["rows_read"] != entry["rows_written"] for entry in families.values()):
        raise ValueError("merge output does not conserve rows")
    return families


def expected_commands(
    selection: list[dict[str, Any]], profile: dict[str, Any], boundary: bool
) -> list[str]:
    labels = []
    for case in selection:
        tag = f"{case['name']}-r{case['repetition']}"
        if boundary:
            labels.append(f"{tag}-ingest")
        for phase in profile["phases"]:
            if boundary and phase["phase"] in {"admission", "generate", "ingest"}:
                continue
            commands = phase["action"].get("commands", [phase["action"].get("args")])
            labels.extend(f"{tag}-{phase['phase']}-{index}" for index in range(len(commands)))
    return labels


def validate_boundary_families(
    nodes: int, edges: int, families: dict[str, Any], batch_rows: int = 1
) -> None:
    # Hand-derived production fan-in-32 work for one-record input runs. Detail
    # labels are exactly four UTF-8 bytes; compact wire widths are 16+1+4 and 48+1+4.
    if nodes % batch_rows or edges % batch_rows:
        raise ValueError("fixture requires full fixed-size batches")
    nodes //= batch_rows
    edges //= batch_rows
    work = {
        1: 0,
        2: 2,
        4: 4,
        17: 17,
        34: 68,
        68: 136,
        128: 256,
        16: 16,
        31: 31,
        32: 32,
        33: 65,
        62: 124,
        64: 128,
        66: 132,
        256: 512,
        272: 544,
        512: 1024,
        1023: 2046,
        1024: 2048,
        1025: 3073,
        1088: 3264,
        2046: 6138,
        2048: 6144,
        2050: 6148,
    }
    expected = {
        "merge-identities": (nodes + edges, work[nodes + edges], 26),
        "merge-node-details": (nodes, work[nodes], 21),
        "merge-edge-details": (edges, work[edges], 53),
        "merge-endpoints": (edges, 2 * work[edges], 33),
        "merge-resolved": (2 * edges, work[2 * edges], 25),
    }
    for family, (runs, run_rows, width) in expected.items():
        rows = run_rows * batch_rows
        entry = families[family]
        if (
            entry["input_runs"],
            entry["rows_read"],
            entry["rows_written"],
            entry["fixed_read_bytes"],
            entry["fixed_written_bytes"],
        ) != (runs, rows, rows, rows * width, rows * width):
            raise ValueError(f"fixed family work oracle mismatch: {family}")
    for kind, count in (("node-rows-", nodes), ("edge-rows-", edges)):
        entries = [entry for family, entry in families.items() if family.startswith(kind)]
        rows = (work[count] + (count if count in {1, 32, 1024} else 0)) * batch_rows
        if len(entries) != 1 or any(
            (entry["input_runs"], entry["rows_read"], entry["rows_written"]) != (count, rows, rows)
            for entry in entries
        ):
            raise ValueError(f"Parquet family work oracle mismatch: {kind}")


def interval_union_ns(intervals: list[tuple[int, int]]) -> int:
    """Measure covered wall time once, including nested/overlapping children."""
    total = 0
    end = 0
    for start, stop in sorted(intervals):
        if not 0 <= start <= stop:
            raise ValueError("invalid diagnostic interval")
        total += max(0, stop - max(start, end))
        end = max(end, stop)
    return total


def scope_summary(lines: list[str]) -> dict[str, Any]:
    events = [
        json.loads(line.removeprefix("INGEST_DIAGNOSTIC "))
        for line in lines
        if line.startswith("INGEST_DIAGNOSTIC ")
    ]
    scopes = [event for event in events if event["event"] == "scope"]
    groups = [event for event in events if event["event"] == "group"]
    result = {}
    for scope in scopes:
        start, end = scope["start_ns"], scope["end_ns"]
        children = [
            (child["start_ns"], child["end_ns"])
            for child in scopes + groups
            if child is not scope and start <= child["start_ns"] <= child["end_ns"] <= end
        ]
        inclusive = interval_union_ns([(start, end)])
        entry = result.setdefault(
            scope["scope"], {"calls": 0, "inclusive_wall_ns": 0, "outside_observed_children_ns": 0}
        )
        entry["calls"] += 1
        entry["inclusive_wall_ns"] += inclusive
        entry["outside_observed_children_ns"] += inclusive - interval_union_ns(children)
    return result


def sync_summary(lines: list[str]) -> dict[str, Any]:
    """Parse timestamped sync-only strace, including interleaved unfinished calls."""
    pending = {}
    intervals = []
    latencies = []
    operations: dict[str, int] = {}
    failed = 0
    for line in lines:
        header = re.match(r"\s*(\d+)\s+(\d+\.\d+)\s+(.*)", line)
        if not header:
            if "fsync" in line or "fdatasync" in line:
                raise ValueError("invalid sync trace header")
            continue
        pid, timestamp, body = header.groups()
        start = int(Decimal(timestamp) * 1_000_000_000)
        beginning = re.match(r"(fsync|fdatasync)\(", body)
        resumed = re.match(r"<\.\.\. (fsync|fdatasync) resumed>", body)
        if beginning:
            operation = beginning[1]
            if pid in pending:
                raise ValueError("unfinished sync overwritten")
            if "<unfinished ...>" in body:
                pending[pid] = (operation, start)
                continue
        elif resumed:
            if pid not in pending:
                raise ValueError("resumed sync has no beginning")
            operation, start = pending.pop(pid)
            if resumed[1] != operation:
                raise ValueError("resumed sync operation mismatch")
        else:
            continue
        complete = re.search(r"=\s+(-?\d+).*<(\d+\.\d+)>$", body)
        if not complete:
            raise ValueError("sync call lacks result or latency")
        latency = int(Decimal(complete[2]) * 1_000_000_000)
        failed += int(complete[1] != "0")
        operations[operation] = operations.get(operation, 0) + 1
        latencies.append(latency)
        intervals.append((start, start + latency))
    if pending or not latencies:
        raise ValueError("incomplete or empty sync trace")
    return {
        "calls": len(latencies),
        "operations": operations,
        "failed_calls": failed,
        "summed_thread_latency_ns": sum(latencies),
        "elapsed_interval_union_ns": interval_union_ns(intervals),
        "maximum_call_latency_ns": max(latencies),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True, type=Path)
    args = parser.parse_args()
    print(
        json.dumps(
            analyze(Path(__file__).resolve().parents[2], args.evidence), indent=2, sort_keys=True
        )
    )


if __name__ == "__main__":
    main()
