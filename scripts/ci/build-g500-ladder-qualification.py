#!/usr/bin/env python3
"""Build sanitized #951 qualification evidence from adjacent certifications."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


CATEGORIES = (
    ("canonical_node_topology", "topology_nodes", "storage_owned_snapshot"),
    ("canonical_edge_topology", "topology_edges", "storage_owned_snapshot"),
    ("properties", "properties", "storage_owned_snapshot"),
    ("uuid_surrogate_indexes", "uuid_and_surrogates", "storage_owned_snapshot"),
    ("adjacency_csr", "adjacency", "storage_owned_snapshot"),
    ("catalog_manifests", "catalog_and_manifests", "storage_owned_snapshot"),
)


def artifact(category: str, totals: dict, source: str, peak: int | None = None) -> dict:
    allocated = totals.get("allocated_bytes", 0)
    return {
        "category": category,
        "logical_bytes": totals.get("logical_bytes", totals.get("physical_logical_bytes", 0)),
        "allocated_bytes": allocated,
        "current_retained_bytes": allocated,
        "transient_peak_allocated_bytes": allocated if peak is None else peak,
        "logical_references": totals.get("logical_references", 0),
        "physical_objects": totals.get("physical_objects", 0),
        "source": source,
    }


def rung(cert: dict) -> dict:
    if cert.get("envelope", {}).get("peak_disk_source") != "storage_owned_active_identity_union":
        raise ValueError("certification peak disk is not a storage-owned active identity union")
    storage = cert["storage_attribution"]
    source = storage["source"]
    rows = [artifact(name, source["categories"][key], owner) for name, key, owner in CATEGORIES]
    construction = storage["construction"]
    staging = construction.get("storage_current", {}).get("construction_staging", {})
    rows.append(artifact("construction_staging_spill", staging, "construction_receipts", construction.get("storage_transient_peak_total_allocated_bytes", 0)))
    rows.append(artifact("portable_package", storage["portable_package"], "exact_descriptor"))
    rows.append(artifact("clean_imported_project", storage["clean_import"], "clean_import_snapshot"))
    phase_map = storage["application_io_phases"]["phases"]
    phases = []
    for name, values in phase_map.items():
        applicable = any(values[field] != 0 for field in ("read_bytes", "write_bytes", "read_calls", "write_calls", "object_count", "block_count", "fsync_calls"))
        if name != "recovery_reauthentication" and not applicable:
            raise ValueError(f"required lifecycle phase has no source-owned observation: {name}")
        phases.append({"phase": name, "applicable": applicable, **values})
    totals = {
        "logical_bytes": sum(row["logical_bytes"] for row in rows),
        "allocated_bytes": sum(row["allocated_bytes"] for row in rows),
        # This is the native-identity union across simultaneously retained
        # owners. Category rows are local ownership views and can alias the
        # same CAS object, so summing them would double count.
        "current_retained_bytes": storage["workspace_current_allocated_bytes"],
        "transient_peak_allocated_bytes": cert["envelope"]["peak_disk_bytes"],
    }
    for field in ("read_bytes", "write_bytes", "read_calls", "write_calls", "object_count", "block_count", "fsync_calls"):
        totals[f"phase_{field}"] = sum(phase[field] for phase in phases)
    nodes, edges = cert["counts"]["source_nodes"], cert["counts"]["source_edges"]
    by_name = {row["category"]: row for row in rows}
    return {"id": f"S{cert['run']['scale']}", "scale": cert["run"]["scale"], "live_nodes": nodes, "live_edges": edges, "artifacts": rows, "phases": phases, "totals": totals, "ratios": {
        "canonical_node_bytes_per_live_node": {"numerator_bytes": by_name["canonical_node_topology"]["logical_bytes"], "denominator_count": nodes},
        "canonical_edge_bytes_per_live_edge": {"numerator_bytes": by_name["canonical_edge_topology"]["logical_bytes"], "denominator_count": edges},
        "authoritative_project_bytes_per_live_edge": {"numerator_bytes": source["allocated_bytes"], "denominator_count": edges},
        "full_lifecycle_peak_bytes_per_live_edge": {"numerator_bytes": totals["transient_peak_allocated_bytes"], "denominator_count": edges},
    }}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("low", type=Path)
    parser.add_argument("high", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--volume-bytes", type=int, required=True)
    parser.add_argument("--reserved-headroom-bytes", type=int, required=True)
    args = parser.parse_args()
    rungs = [rung(json.loads(path.read_text())) for path in (args.low, args.high)]
    low, high = rungs
    delta_bytes = high["totals"]["transient_peak_allocated_bytes"] - low["totals"]["transient_peak_allocated_bytes"]
    delta_edges = high["live_edges"] - low["live_edges"]
    ratio_num, ratio_den = high["totals"]["transient_peak_allocated_bytes"], high["live_edges"]
    if delta_bytes > 0 and delta_bytes * ratio_den > ratio_num * delta_edges:
        ratio_num, ratio_den = delta_bytes, delta_edges
    target_edges = 1 << 30
    peak = (ratio_num * target_edges + ratio_den - 1) // ratio_den
    target_nodes = 1 << 26
    by_category = {row["category"]: row for row in high["artifacts"]}
    canonical_nodes = (by_category["canonical_node_topology"]["current_retained_bytes"] * target_nodes + high["live_nodes"] - 1) // high["live_nodes"]
    canonical_edges = (by_category["canonical_edge_topology"]["current_retained_bytes"] * target_edges + high["live_edges"] - 1) // high["live_edges"]
    headroom = max(0, args.volume_bytes - peak)
    decision = "admit" if peak <= args.volume_bytes and headroom >= args.reserved_headroom_bytes else "refuse"
    value = {"schema": "graphforge-g500-ladder-qualification/3", "rungs": rungs, "projection": {"target": "S26", "source_rungs": [low["id"], high["id"]], "rate": {"numerator_bytes": ratio_num, "denominator_count": ratio_den}, "projected_canonical_node_bytes": canonical_nodes, "projected_canonical_edge_bytes": canonical_edges, "projected_lifecycle_peak_bytes": peak, "volume_bytes": args.volume_bytes, "reserved_headroom_bytes": args.reserved_headroom_bytes, "headroom_bytes": headroom, "decision": decision}}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
