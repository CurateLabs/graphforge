#!/usr/bin/env python3
"""Compare disjoint persistence leaves, separating journal and manifest barriers."""

import argparse
import json
from pathlib import Path
import subprocess

from measurement_contract import (
    RECEIPTS,
    baseline_source_publication,
    require_external_output,
    validate_qualification,
)

parser = argparse.ArgumentParser()
parser.add_argument("--pair", required=True, type=int)
parser.add_argument("--repository", required=True, type=Path)
parser.add_argument("--evidence-root", type=Path, required=True)
args = parser.parse_args()
base = args.evidence_root.resolve() / f"pair-{args.pair}"
require_external_output(base, args.repository, Path(__file__).resolve().parent)
identities = [
    json.loads((base / lane / "identity.json").read_text()) for lane in ("baseline", "candidate")
]
for lane, identity in zip(("baseline", "candidate"), identities):
    assert identity["lane"] == lane and identity["pair"] == args.pair, (
        "mismatched lane/pair identity"
    )
for key in (
    "host",
    "kernel",
    "logical_cpus",
    "cpu_affinity",
    "observer",
    "batch_rows",
    "inputs",
    "build_settings",
    "ambient_resources",
    "method_sha256",
):
    assert identities[0][key] == identities[1][key], (key, "unmatched provenance")
qualifications = [
    validate_qualification(base / lane, identity)
    for lane, identity in zip(("baseline", "candidate"), identities)
]
changes = subprocess.check_output(
    [
        "git",
        "diff",
        "--name-only",
        identities[0]["source_sha"],
        identities[1]["source_sha"],
        "--",
        "crates",
        "Cargo.toml",
        "Cargo.lock",
        "benchmarks",
    ],
    cwd=args.repository,
    text=True,
).splitlines()
allowed = {
    "crates/graphforge-api/src/import_session.rs",
    "crates/graphforge-api/src/import_session/journal.rs",
    "crates/graphforge-api/src/import_session/journal/tests.rs",
}
assert set(changes) <= allowed and changes, ("non-journal runtime delta", changes)
baseline_namespace = baseline_source_publication(
    subprocess.check_output(
        [
            "git",
            "show",
            f"{identities[0]['source_sha']}:crates/graphforge-api/src/import_session.rs",
        ],
        cwd=args.repository,
        text=True,
    )
)
fields = (
    "wall_ns",
    "process_cpu_ns",
    "fsync_calls",
    "fsync_elapsed_ns",
    "written_bytes",
    "hashed_bytes",
    "hash_elapsed_ns",
)
categories = {
    "manifest_persistence": "manifest_checkpoint",
    "journal_append": "journal_append",
    "journal_sync": "journal_sync",
    "journal_namespace_publication": "journal_namespace",
    "source_publication": "source_publication",
}


def totals(selected):
    # Empty journal subsets are exact zeros in the baseline. A missing required
    # manifest region or unavailable metric never becomes an invented zero.
    for row in selected:
        assert all(row[field] is not None for field in fields), (
            "unavailable persistence metric",
            row,
        )
    return {field: sum(row[field] for row in selected) for field in fields}


def combine(costs):
    return {
        field: None
        if any(cost[field] is None for cost in costs)
        else sum(cost[field] for cost in costs)
        for field in fields
    }


def delta(before, after):
    return {
        field: None
        if before[field] is None or after[field] is None
        else after[field] - before[field]
        for field in fields
    }


rows = []
for lane in ("baseline", "candidate"):
    lane_root = base / lane
    persistence = []
    commands = []
    receipts = sorted((lane_root / "run").glob("receipt-*.json"))
    assert tuple(path.name for path in receipts) == RECEIPTS, "incomplete five-command workflow"
    for path in receipts:
        snapshot = json.loads(path.read_text())["region_diagnostics"]
        assert snapshot["complete"] and snapshot["contract"] == "graphforge-region-diagnostics/2"
        regions = snapshot["regions"]
        selected = []
        for name, row in regions.items():
            category = categories.get(name.split("/")[-1])
            if category is None:
                continue
            assert not any(child.startswith(name + "/") for child in regions), (
                "non-leaf persistence region",
                name,
            )
            selected.append(
                {
                    "command": path.name,
                    "region": name,
                    "category": category,
                    "calls": row["calls"],
                    **{field: row["inclusive"][field] for field in fields},
                }
            )
        assert any(row["category"] == "manifest_checkpoint" for row in selected), (
            "missing manifest region",
            path,
        )
        if lane == "baseline":
            assert all(row["category"] == "manifest_checkpoint" for row in selected), (
                "baseline already contains journal change"
            )
        persistence.extend(selected)
        registration = path.name in RECEIPTS[1:3]
        source_rows = [row for row in selected if row["category"] == "source_publication"]
        if lane == "candidate":
            assert len(source_rows) == (1 if registration else 0), (
                "source-publication scope missing"
            )
            if registration:
                assert source_rows[0]["calls"] == 1, "unexpected source-publication call count"
        command_categories = {
            kind: totals([row for row in selected if row["category"] == kind])
            for kind in categories.values()
        }
        if lane == "baseline" and registration:
            # No namespace barrier exists at this reviewed baseline boundary.
            # Rename wall/CPU/writes/hash cost was not observed as a leaf.
            command_categories["source_publication"] = {
                field: 0 if field in baseline_namespace["known_zero_fields"] else None
                for field in fields
            }
        commands.append(
            {
                "command": path.name,
                "persistence": combine(list(command_categories.values())),
                "observed_leaf_totals": totals(selected),
                "by_category": command_categories,
            }
        )
    by_category = {
        kind: combine([command["by_category"][kind] for command in commands])
        for kind in categories.values()
    }
    if lane == "candidate":
        assert all(
            any(row["category"] == kind for row in persistence)
            for kind in ("journal_append", "journal_sync", "journal_namespace")
        ), "candidate journal scopes missing"
    rows.append(
        {
            "lane": lane,
            "identity": json.loads((lane_root / "identity.json").read_text()),
            "qualification": qualifications[len(rows)],
            "commands": commands,
            "persistence_regions": persistence,
            "persistence_totals": combine(list(by_category.values())),
            "observed_leaf_totals": totals(persistence),
            "by_category": by_category,
            "manifest_checkpoint_calls": sum(
                row["calls"] for row in persistence if row["category"] == "manifest_checkpoint"
            ),
        }
    )
comparison = {
    "pair": args.pair,
    "runtime_diff_paths": changes,
    "lanes": rows,
    "baseline_source_publication_semantics": baseline_namespace,
    "persistence_delta_candidate_minus_baseline": delta(
        rows[0]["persistence_totals"], rows[1]["persistence_totals"]
    ),
    "persistence_non_comparable_fields": [
        field for field in fields if rows[0]["persistence_totals"][field] is None
    ],
    "manifest_checkpoint_delta_candidate_minus_baseline": delta(
        rows[0]["by_category"]["manifest_checkpoint"], rows[1]["by_category"]["manifest_checkpoint"]
    ),
}
(base / "comparison.json").write_text(json.dumps(comparison, indent=2) + "\n")
print(
    json.dumps(
        {
            "pair": args.pair,
            "persistence_totals": [row["persistence_totals"] for row in rows],
            "manifest_checkpoint_totals": [
                row["by_category"]["manifest_checkpoint"] for row in rows
            ],
            "delta": comparison["persistence_delta_candidate_minus_baseline"],
        },
        indent=2,
    )
)
