"""Keep complete ingest, inclusive regions, useful work and matched speedup distinct."""

from __future__ import annotations

from typing import Any


def summarize_regions(receipts: list[dict[str, Any]]) -> dict[str, Any]:
    """Reconcile only disjoint command roots against the complete workflow."""
    boundaries = [r for r in receipts if r.get("contract") == "graphforge-workflow-timing/1"]
    commands = [r["region_diagnostics"] for r in receipts if "region_diagnostics" in r]
    if not commands and not boundaries:
        return {}
    if any(not command.get("complete") for command in commands):
        raise ValueError("incomplete region capture")
    roots = [command["regions"]["import_command"]["inclusive"] for command in commands]
    command_wall = sum(root["wall_ns"] for root in roots)
    command_cpu = _sum_available([root["process_cpu_ns"] for root in roots])
    stages = []
    for index, command in enumerate(commands):
        for path, row in command["regions"].items():
            measured = row["inclusive"]
            wall = measured["wall_ns"]
            cpu = measured["process_cpu_ns"]
            stages.append(
                {
                    "command_index": index,
                    "path": path,
                    **row,
                    "process_effective_cores": cpu / wall if cpu is not None and wall else None,
                    "calling_thread_on_cpu_fraction": (
                        measured["thread_running_ns"] / wall
                        if measured["thread_running_ns"] is not None and wall
                        else None
                    ),
                    "useful_work_per_second": {
                        unit: count * 1_000_000_000 / wall if wall else None
                        for unit, count in row["work"].items()
                    },
                    "matched_worker_speedup": None,
                }
            )
    result = {
        "command_wall_ns": command_wall,
        "command_process_cpu_ns": command_cpu,
        "stages_inclusive_do_not_sum": stages,
    }
    if len(boundaries) > 1:
        raise ValueError("multiple workflow boundaries")
    if boundaries:
        boundary = boundaries[0]
        cpu = _sum_available([boundary["runner_cpu_ns"], boundary["children_cpu_ns"]])
        wall = boundary["wall_ns"]
        result["complete_ingest"] = {
            **boundary,
            "cpu_ns": cpu,
            "effective_cores": cpu / wall if cpu is not None and wall else None,
            "outside_command_wall_ns": wall - command_wall,
            # Signed residual preserves sampling/10 ms CPU quantization uncertainty.
            "outside_command_cpu_ns": (
                cpu - command_cpu if cpu is not None and command_cpu is not None else None
            ),
        }
    return {"region_attribution": result}


def _sum_available(values: list[int | None]) -> int | None:
    return (
        sum(value for value in values if value is not None)
        if all(value is not None for value in values)
        else None
    )


def matched_worker_speedup(baseline: dict[str, Any], candidate: dict[str, Any]) -> float:
    """Compare equal work at one vs N workers, never N independent processes."""
    identities = ("build", "input", "host", "cache", "resource_policy", "scope", "unit")
    if any(
        not isinstance(baseline.get(key), str)
        or not baseline[key].strip()
        or baseline[key] != candidate.get(key)
        for key in identities
    ):
        raise ValueError("worker comparison requires matching nonempty provenance")
    for observation in (baseline, candidate):
        if any(
            type(observation.get(key)) is not int or observation[key] <= 0
            for key in ("workers", "processes", "wall_ns", "work")
        ):
            raise ValueError("worker comparison requires positive integer work, time and counts")
    if baseline["work"] != candidate["work"]:
        raise ValueError("worker comparison requires matching useful work")
    if baseline.get("workers") != 1 or candidate.get("workers", 0) <= 1:
        raise ValueError("worker comparison requires one-worker baseline and N-worker candidate")
    if baseline.get("processes") != 1 or candidate.get("processes") != 1:
        raise ValueError("multi-process throughput is not worker speedup")
    if baseline.get("wall_ns", 0) <= 0 or candidate.get("wall_ns", 0) <= 0:
        raise ValueError("worker comparison requires measured positive wall times")
    return baseline["wall_ns"] / candidate["wall_ns"]


def compare_region_receipts(
    baseline: dict[str, Any], candidate: dict[str, Any], scope: str, unit: str
) -> dict[str, Any]:
    """Read work/time from receipts and require explicit experiment provenance."""
    observations = []
    for document in (baseline, candidate):
        snapshot = document["receipt"]["region_diagnostics"]
        if not snapshot["complete"]:
            raise ValueError("cannot compare an incomplete capture")
        row = snapshot["regions"][scope]
        observations.append(
            {
                **document["provenance"],
                "scope": scope,
                "unit": unit,
                "work": row["work"][unit],
                "wall_ns": row["inclusive"]["wall_ns"],
                "cpu_ns": row["inclusive"]["process_cpu_ns"],
            }
        )
    return {
        "matched_worker_speedup": matched_worker_speedup(*observations),
        "process_effective_cores": [
            item["cpu_ns"] / item["wall_ns"] if item["cpu_ns"] is not None else None
            for item in observations
        ],
        "useful_work": observations[0]["work"],
        "unit": unit,
        "scope": scope,
    }


def main() -> None:
    """Compare two provenance-bearing stock receipts at a selected stage."""
    import argparse
    import json
    from pathlib import Path

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--unit", required=True, choices=("nodes", "edges", "rows", "bytes"))
    args = parser.parse_args()
    print(
        json.dumps(
            compare_region_receipts(
                json.loads(args.baseline.read_text()),
                json.loads(args.candidate.read_text()),
                args.scope,
                args.unit,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
