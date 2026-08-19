#!/usr/bin/env python3
"""Fail closed when the versioned M6 benchmark inventory changes."""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
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
    missing.extend(f"{filename}:{name}" for name in sorted(names) if f"fn {name}(" not in source)
if missing:
    print("missing M6 benchmarks: " + ", ".join(missing), file=sys.stderr)
    raise SystemExit(1)
print(f"M6 benchmark inventory v1: {sum(map(len, EXPECTED.values()))} names verified")
