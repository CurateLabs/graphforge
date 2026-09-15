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
    sync_summary,
    validate_boundary_families,
)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def summarize(directory, *, retain_completed_roots=False):
    summary = json.loads((directory / "summary.json").read_text())
    if summary.get("status") != "passed":
        raise ValueError("campaign is unfinished")
    expected_completions = [
        {
            "case": f"{case['name']}-r{case['repetition']}",
            "nodes": case["nodes"],
            "edges": case["edges"],
            "independent_oracles": 8,
            "full_lifecycle": True,
        }
        for case in summary["selection"]
    ]
    completed = summary.get("completed_cases", [])
    if completed != expected_completions or any(
        case["full_lifecycle"] is not True for case in completed
    ):
        raise ValueError("incomplete or mismatched lifecycle/oracle completion evidence")
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
    if retain_completed_roots:
        result["issue"] = 1286
        result["row_root_policy"] = "retain_completed_intermediate"
    source_manifest = json.loads((directory / "source-manifest.json").read_text())
    native_inputs = {
        path: sha
        for path, sha in source_manifest.items()
        if path.endswith((".rs", ".toml", ".lock"))
    }
    result["native_source_inputs_sha256"] = hashlib.sha256(
        json.dumps(native_inputs, sort_keys=True).encode()
    ).hexdigest()
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
        required_profile = {"perf": "perf.data", "sync": "strace"}.get(summary["suite"])
        if (
            required_profile
            and observation["phase"] == "ingest"
            and required_profile + "_sha256" not in observation
        ):
            raise ValueError("missing required profile artifact")
        for suffix in ("perf.data", "strace"):
            if suffix + "_sha256" in observation:
                if observation[suffix + "_sha256"] != digest(directory / f"{label}.{suffix}"):
                    raise ValueError("profile artifact digest mismatch")
        command = {
            k: v
            for k, v in observation.items()
            if k not in {"host_activity_before", "host_activity"}
        }
        if "strace_sha256" in observation:
            command["sync_latency"] = sync_summary(
                (directory / f"{label}.strace").read_text().splitlines()
            )
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
                command["construction_peak_bytes"] = fixture["evidence"][
                    "storage_transient_peak_total_allocated_bytes"
                ]
        lines = (directory / f"{label}.stderr").read_text().splitlines()
        if any(line.startswith("INGEST_DIAGNOSTIC ") for line in lines):
            command["scopes"] = scope_summary(lines)
            if any('"event":"inputs"' in line for line in lines):
                command["merge_families"] = merge_summary(lines)
        boundary_ingest = summary["suite"] == "boundary" and observation["phase"] == "ingest"
        if boundary_ingest and "boundary" not in command:
            raise ValueError("missing boundary ingestion timing receipt")
        requires_merge = (
            (summary["suite"] == "diagnostic" or summary.get("custom_counters_enabled"))
            and label.endswith("-ingest-3")
        ) or (summary["instrumented"] and boundary_ingest)
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
                    command["boundary"]["nodes"],
                    command["boundary"]["edges"],
                    families,
                    retain_completed_roots=retain_completed_roots,
                )
            else:
                selected_case = next(
                    case
                    for case in summary["selection"]
                    if case["name"] == observation["case"]
                    and case["repetition"] == observation["repetition"]
                )
                validate_boundary_families(
                    selected_case["nodes"],
                    selected_case["edges"],
                    families,
                    65536,
                    retain_completed_roots=retain_completed_roots,
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
                "peak_construction_staging_allocated_bytes": 0,
                "construction_calls_seconds": {},
            },
        )
        case["whole_command_wall_seconds"] += observation["wall_seconds"]
        case["peak_process_bytes"] = max(
            case["peak_process_bytes"], observation["sampled_process_peak_bytes"]
        )
        case["peak_construction_staging_allocated_bytes"] = max(
            case["peak_construction_staging_allocated_bytes"],
            command.get("construction_peak_bytes", 0),
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
            for name, call in command.get("construction_call_timings", {}).items():
                calls = case["construction_calls_seconds"]
                calls[name] = calls.get(name, 0) + call["elapsed_ns"] / 1e9
    for case in cases.values():
        if case["construction_calls_seconds"]:
            residual = case["ingestion_wall_seconds"] - sum(
                case["construction_calls_seconds"].values()
            )
            if residual < 0:
                raise ValueError("construction durations exceed enclosing ingestion commands")
            case["ingestion_outside_construction_calls_seconds"] = residual
    result["cases"] = cases
    result["baseline_envelopes"] = {}
    for name in sorted({case["case"] for case in cases.values()}):
        repeats = [case for case in cases.values() if case["case"] == name]
        envelope = {}
        for key in (
            "ingestion_wall_seconds",
            "ingestion_command_cpu_seconds",
            "peak_process_bytes",
            "peak_construction_staging_allocated_bytes",
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
