#!/usr/bin/env python3
"""Fail closed when the versioned M6 benchmark inventory changes."""

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "codspeed.yml"
EXPECTED = {
    "m6_storage.rs": {
        "gfdr_encode",
        "gfdr_decode_verify",
        "replay_merge_fingerprint",
        "manifest_reachability",
        "transaction_classification",
        "transaction_stage_and_classify",
    },
    "m6_storage_io.rs": {
        "durable_open",
        "durable_commit",
        "recovery_scan",
        "reachability_scan",
        "garbage_collection",
        "spill_compaction",
    },
}

missing = []
for filename, names in EXPECTED.items():
    source = (ROOT / "crates/graphforge-storage/benches" / filename).read_text()
    missing.extend(
        f"{filename}:{name}"
        for name in sorted(names)
        if re.search(
            rf"(?m)^\s*#\[divan::bench[^\n]*\]\s*\n\s*fn\s+{re.escape(name)}\s*\(",
            source,
        )
        is None
    )
if missing:
    print("missing M6 benchmarks: " + ", ".join(missing), file=sys.stderr)
    raise SystemExit(1)

walltime_source = (ROOT / "crates/graphforge-storage/benches/m6_storage_io.rs").read_text(
    encoding="utf-8"
)
for name in ("durable_commit", "spill_compaction"):
    function = re.search(
        rf"(?ms)^fn\s+{re.escape(name)}\s*\([^)]*\)\s*\{{(.*?)(?=^#\[divan::bench|\Z)",
        walltime_source,
    )
    if function is None:
        raise SystemExit(f"cannot inspect TempDir-backed benchmark {name}")
    body = function.group(1)
    if ".bench_local_refs(" not in body or ".bench_local_values(" in body:
        raise SystemExit(
            f"{name} must keep TempDir teardown outside the timed region with bench_local_refs"
        )

workflow = WORKFLOW.read_text(encoding="utf-8")
walltime_job = workflow.split("  m6-walltime:\n", 1)
if len(walltime_job) != 2:
    raise SystemExit("CodSpeed workflow is missing the m6-walltime job")
walltime_job = walltime_job[1].split("\n  m6-memory-fallback:", 1)[0]
if "runs-on: codspeed-macro" not in walltime_job:
    raise SystemExit("m6-walltime must run on the CodSpeed Macro Runner")
if "mode: walltime" not in walltime_job:
    raise SystemExit("m6-walltime must use the walltime instrument")

simulation_job = workflow.split("  benchmarks:\n", 1)
if len(simulation_job) != 2:
    raise SystemExit("CodSpeed workflow is missing the simulation benchmark job")
simulation_job = simulation_job[1].split("\n  m6-walltime:", 1)[0]
if "runs-on: codspeed-macro" in simulation_job:
    raise SystemExit("CPU simulation must remain separate from the Macro Runner")
if "mode: simulation" not in simulation_job:
    raise SystemExit("CPU benchmark job must use the simulation instrument")

print(f"M6 benchmark inventory v1: {sum(map(len, EXPECTED.values()))} names verified")
