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
        # Continuous bulk-ingest throughput (#1387 workstream 6). Pinned here so
        # it cannot be silently deleted: every storage gain the redesign wins is
        # unprotected the moment this benchmark stops running.
        "ingest_throughput",
        "ingest_identifier_density",
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
for name in (
    "durable_commit",
    "spill_compaction",
    "ingest_throughput",
    "ingest_identifier_density",
):
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

# Ingest does real durability work, so it belongs to the walltime instrument on
# the isolated runner, and its floor gate must actually run. CodSpeed only
# compares a run against the previous one; without this step a slow drift that
# never regresses in a single step passes indefinitely.
if 'GF_INGEST_FLOOR_GATE: "1"' not in walltime_job:
    raise SystemExit("m6-walltime must run the ingest floor gate")

INGEST_GATES = (
    "INGEST_FLOOR_EDGES_PER_SECOND",
    "INGEST_CEILING_BYTES_READ_PER_EDGE",
    "INGEST_CEILING_CPU_MICROS_PER_EDGE",
    "INGEST_MAX_READ_DEGRADATION_RATIO",
)
missing_gates = [
    gate
    for gate in INGEST_GATES
    if re.search(rf"(?m)^const\s+{gate}\s*:\s*f64\s*=", walltime_source) is None
]
if missing_gates:
    raise SystemExit("ingest floor gate is missing limits: " + ", ".join(missing_gates))

# Two or more sizes, and the ratio between them, is the point of the benchmark:
# a single size would report a flat healthy number while throughput degraded
# underneath it.
sweep = re.search(r"(?m)^const\s+INGEST_SWEEP_EDGES\s*:\s*\[u64;\s*(\d+)\]", walltime_source)
if sweep is None or int(sweep.group(1)) < 2:
    raise SystemExit("ingest_throughput must sweep at least two dataset sizes")

print(f"M6 benchmark inventory v2: {sum(map(len, EXPECTED.values()))} names verified")
