"""Validate and summarize a private #1282 measurement directory as sanitized JSON."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import statistics

from graphforge_bench.ingestion_attribution import (
    expected_commands,
    merge_summary,
    scope_summary,
    validate_boundary_families,
)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def summarize(directory):
    summary = json.loads((directory / "summary.json").read_text())
    selected = {f"{case['name']}-r{case['repetition']}" for case in summary["selection"]}
    completed = {case["case"] for case in summary.get("completed_cases", [])}
    if summary.get("status") != "passed" or completed != selected:
        raise ValueError("campaign is unfinished or does not match its preselection")
    if digest(directory / "selection.json") != summary["selection_sha256"]:
        raise ValueError("preselection digest mismatch")
    profile_path = Path(__file__).resolve().parents[1] / "profiles/graph500/s18-local.json"
    if digest(profile_path) != summary["profile_sha256"]:
        raise ValueError("profile identity mismatch")
    selection = json.loads((directory / "selection.json").read_text())
    expected = expected_commands(
        summary["selection"], json.loads(profile_path.read_text()), summary["suite"] == "boundary"
    )
    actual = [observation["label"] for observation in summary["observations"]]
    if selection != {"cases": summary["selection"], "commands": expected} or actual != expected:
        raise ValueError("missing, duplicate or reordered planned commands")
    if digest(directory / "source-manifest.json") != summary["source_manifest_sha256"]:
        raise ValueError("source manifest digest mismatch")
    result = {
        key: value
        for key, value in summary.items()
        if key not in {"observations", "host_activity", "rustc"}
    }
    result["rustc"] = summary["rustc"]
    result["summary_sha256"] = digest(directory / "summary.json")
    result["commands"] = []
    cases = {}
    for observation in summary["observations"]:
        label = observation["label"]
        if observation["exit_code"] != 0 or observation["failure"]:
            raise ValueError("failed command in successful campaign")
        if observation["host_activity_before"] or any(observation.get("host_activity", [])):
            raise ValueError("concurrent campaign invalidates controlled observation")
        for suffix in ("stdout", "stderr", "proc.json", "command.json"):
            key = "command_sha256" if suffix == "command.json" else suffix + "_sha256"
            if observation[key] != digest(directory / f"{label}.{suffix}"):
                raise ValueError(f"raw artifact digest mismatch: {label} {suffix}")
        for suffix in ("perf.data", "strace"):
            if suffix + "_sha256" in observation:
                if observation[suffix + "_sha256"] != digest(directory / f"{label}.{suffix}"):
                    raise ValueError("profile artifact digest mismatch")
        command = {
            k: v
            for k, v in observation.items()
            if k not in {"host_activity_before", "host_activity"}
        }
        stdout = (directory / f"{label}.stdout").read_text()
        if stdout.startswith("{"):
            receipt = json.loads(stdout)
            if "operation_timings" in receipt:
                command["construction_call_timings"] = receipt["operation_timings"]
            if receipt.get("outcome") == "committed" and "construction" in receipt:
                construction = receipt["construction"]
                command["construction_io"] = construction["application_io"]
                command["construction_peak_bytes"] = construction["transient_peak_allocated_bytes"]
        for line in stdout.splitlines():
            if "INGEST_BOUNDARY " in line:
                if "boundary" in command:
                    raise ValueError("duplicate boundary receipt")
                fixture = json.loads(line.partition("INGEST_BOUNDARY ")[2])
                command["boundary"] = {
                    key: fixture[key] for key in ("nodes", "edges", "append_ns", "seal_publish_ns")
                }
                command["boundary"]["work_counters"] = {
                    k: v
                    for k, v in fixture["evidence"].items()
                    if isinstance(v, int)
                    and (k.startswith(("merge_", "parquet_")) or k == "input_rows")
                }
        lines = (directory / f"{label}.stderr").read_text().splitlines()
        if any(line.startswith("INGEST_DIAGNOSTIC ") for line in lines):
            command["scopes"] = scope_summary(lines)
            if any('"event":"inputs"' in line for line in lines):
                command["merge_families"] = merge_summary(lines)
        boundary_ingest = summary["suite"] == "boundary" and observation["phase"] == "ingest"
        if boundary_ingest and "boundary" not in command:
            raise ValueError("missing boundary ingestion timing receipt")
        requires_merge = (summary["suite"] == "diagnostic" and label.endswith("-ingest-3")) or (
            summary["instrumented"] and boundary_ingest
        )
        if requires_merge:
            families = command.get("merge_families", {})
            required = {
                "merge-identities",
                "merge-node-details",
                "merge-edge-details",
                "merge-endpoints",
                "merge-resolved",
            }
            if (
                not required <= families.keys()
                or not any(k.startswith("node-rows-") for k in families)
                or not any(k.startswith("edge-rows-") for k in families)
            ):
                raise ValueError("missing required merge family diagnostic")
            if boundary_ingest:
                validate_boundary_families(
                    command["boundary"]["nodes"], command["boundary"]["edges"], families
                )
        result["commands"].append(command)
        tag = f"{observation['case']}-r{observation['repetition']}"
        case = cases.setdefault(
            tag,
            {
                "case": observation["case"],
                "repetition": observation["repetition"],
                "whole_command_wall_seconds": 0,
                "ingestion_wall_seconds": 0,
                "ingestion_command_cpu_seconds": 0,
                "ingestion_input_bytes": 0,
                "ingestion_output_bytes": 0,
                "peak_process_bytes": 0,
            },
        )
        case["whole_command_wall_seconds"] += observation["wall_seconds"]
        case["peak_process_bytes"] = max(
            case["peak_process_bytes"], observation["sampled_process_peak_bytes"]
        )
        if observation["phase"] == "ingest":
            case["ingestion_wall_seconds"] += (
                (command["boundary"]["append_ns"] + command["boundary"]["seal_publish_ns"]) / 1e9
                if "boundary" in command
                else observation["wall_seconds"]
            )
            case["ingestion_command_cpu_seconds"] += (
                observation["user_seconds"] + observation["system_seconds"]
            )
            case["ingestion_input_bytes"] += observation["wait4_input_bytes"]
            case["ingestion_output_bytes"] += observation["wait4_output_bytes"]
    result["cases"] = cases
    result["baseline_envelopes"] = {}
    for name in sorted({case["case"] for case in cases.values()}):
        repeats = [case for case in cases.values() if case["case"] == name]
        envelope = {}
        for key in (
            "ingestion_wall_seconds",
            "ingestion_command_cpu_seconds",
            "peak_process_bytes",
            "ingestion_input_bytes",
            "ingestion_output_bytes",
        ):
            values = [case[key] for case in repeats]
            envelope[key] = {
                "min": min(values),
                "median": statistics.median(values),
                "max": max(values),
            }
        wall = envelope["ingestion_wall_seconds"]
        envelope["minimum_useful_wall_saving_seconds"] = max(0.01, wall["max"] - wall["min"])
        envelope["measurement_resolution_seconds"] = 0.01
        result["baseline_envelopes"][name] = envelope
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.directory), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
