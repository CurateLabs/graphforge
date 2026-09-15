"""Validate #1286 observations and freeze a baseline-derived decision before comparison.

Use ``report`` for ordinary or diagnostic directories, ``freeze`` after the
baseline completes, then ``compare`` after the preselected candidate completes.
Historical #1282 reports keep their original merge-work expectations.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from report_ingestion_1282 import digest, summarize

HISTORICAL_RANGE = {"s16": 0.122031175, "s17": 0.406388363, "s18": 0.621841171}
COMPARABLE = (
    "generator_sha256",
    "profile_sha256",
    "cargo_lock_sha256",
    "rustc",
    "python",
    "pyarrow",
    "kernel_release",
    "cpu_affinity",
    "reserve_bytes",
    "process_rss_limit_bytes",
    "timeout_seconds",
    "cgroup_memory_ceiling_bytes",
    "cache_policy",
    "selection",
)


def ordinary(report):
    expected = [
        {"name": f"s{s}", "scale": s, "nodes": 1 << s, "edges": 16 << s, "repetition": r}
        for r in range(3)
        for s in (16, 17, 18)
    ]
    if report["suite"] != "scaling" or report["instrumented"] or report["selection"] != expected:
        raise ValueError("comparison requires all nine preselected ordinary lifecycles")


def freeze(baseline):
    ordinary(baseline)
    thresholds = {}
    for case, historical in HISTORICAL_RANGE.items():
        envelope = baseline["baseline_envelopes"][case]
        wall = envelope["ingestion_wall_seconds"]
        thresholds[case] = {
            "minimum_saving_seconds": max(0.01, historical, wall["max"] - wall["min"]),
            "baseline_envelope": envelope,
        }
    return {
        "issue": 1286,
        "frozen_unix": time.time(),
        "baseline_summary_sha256": baseline["summary_sha256"],
        "thresholds": thresholds,
        "primary_case": "s17",
        "policy": (
            "S17 median saving must strictly exceed frozen threshold; "
            "review all resources and controls"
        ),
    }


def compare(baseline, candidate, frozen):
    ordinary(candidate)
    expected = freeze(baseline)
    expected["frozen_unix"] = frozen["frozen_unix"]
    if frozen != expected:
        raise ValueError("thresholds differ from validated baseline")
    baseline_end = max(c["started_unix"] + c["wall_seconds"] for c in baseline["commands"])
    candidate_start = min(c["started_unix"] for c in candidate["commands"])
    if not baseline_end <= frozen["frozen_unix"] < candidate_start:
        raise ValueError("thresholds were not frozen between baseline and candidate")
    if any(baseline[key] != candidate[key] for key in COMPARABLE):
        raise ValueError("baseline/candidate fixture, toolchain or host profile mismatch")
    if baseline["gf_sha256"] == candidate["gf_sha256"]:
        raise ValueError("candidate executable is unchanged")
    cases = {}
    for name, threshold in frozen["thresholds"].items():
        before = baseline["baseline_envelopes"][name]
        after = candidate["baseline_envelopes"][name]
        saving = (
            before["ingestion_wall_seconds"]["median"] - after["ingestion_wall_seconds"]["median"]
        )
        cases[name] = {
            "median_ingestion_saving_seconds": saving,
            "minimum_saving_seconds": threshold["minimum_saving_seconds"],
            "exceeds_frozen_threshold": saving > threshold["minimum_saving_seconds"],
            "baseline": before,
            "candidate": after,
        }
    return {
        "issue": 1286,
        "baseline_summary_sha256": baseline["summary_sha256"],
        "candidate_summary_sha256": candidate["summary_sha256"],
        "cases": cases,
        "s17_benefit_threshold_passed": cases["s17"]["exceeds_frozen_threshold"],
        "limitation": (
            "Empirical ranges, not confidence intervals. "
            "Correctness and resource review remain required."
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    report = commands.add_parser("report")
    report.add_argument("directory", type=Path)
    report.add_argument("--candidate", action="store_true")
    baseline = commands.add_parser("freeze")
    baseline.add_argument("baseline", type=Path)
    comparison = commands.add_parser("compare")
    comparison.add_argument("baseline", type=Path)
    comparison.add_argument("candidate", type=Path)
    comparison.add_argument("thresholds", type=Path)
    args = parser.parse_args()
    if args.command == "report":
        result = summarize(args.directory, retain_completed_roots=args.candidate)
    elif args.command == "freeze":
        result = freeze(summarize(args.baseline))
    else:
        result = compare(
            summarize(args.baseline),
            summarize(args.candidate, retain_completed_roots=True),
            json.loads(args.thresholds.read_text()),
        )
        result["frozen_thresholds_sha256"] = digest(args.thresholds)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
