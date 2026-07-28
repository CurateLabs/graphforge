"""Representative native-Python correction and reopen replay."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import uuid

import graphforge
import graphforge._graphforge_rs as native


def names(forge: graphforge.GraphForge) -> list[str]:
    return (
        forge.execute("MATCH (o:Organization) RETURN o.name AS name ORDER BY name")
        .column("name")
        .to_pylist()
    )


def organization_ids(forge: graphforge.GraphForge) -> dict[str, str]:
    table = forge.execute(
        "MATCH (o:Organization) RETURN o.node_uuid AS node_uuid, o.name AS name ORDER BY name"
    )
    return {
        name: str(uuid.UUID(bytes=node_uuid))
        for node_uuid, name in zip(
            table.column("node_uuid").to_pylist(),
            table.column("name").to_pylist(),
            strict=True,
        )
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    args = parser.parse_args()

    forge = graphforge.GraphForge(str(args.project))
    aster = forge.add_node("Organization", name="Aster Labs", risk_score=0.4)
    duplicate = forge.add_node("Organization", name="Aster Laboratory", risk_score=0.4)
    created_ids = {"Aster Labs": aster.uuid, "Aster Laboratory": duplicate.uuid}
    before = names(forge)
    before_ids = organization_ids(forge)
    forge.execute("MATCH (o:Organization {name:'Aster Laboratory'}) DETACH DELETE o")
    after = names(forge)
    after_ids = organization_ids(forge)
    # A repeated compensation is a deterministic no-op.
    repeated = forge.execute("MATCH (o:Organization {name:'Aster Laboratory'}) DETACH DELETE o")
    after_repeated = names(forge)
    after_repeated_ids = organization_ids(forge)
    del forge

    reopened = graphforge.GraphForge(str(args.project))
    reopened_names = names(reopened)
    reopened_ids = organization_ids(reopened)
    if before != ["Aster Laboratory", "Aster Labs"]:
        raise AssertionError(f"unexpected pre-correction rows: {before}")
    if after != ["Aster Labs"] or reopened_names != after:
        raise AssertionError("representative correction did not survive reopen")
    if before_ids != created_ids or reopened_ids != {"Aster Labs": created_ids["Aster Labs"]}:
        raise AssertionError("stable organization UUIDs did not survive correction and reopen")
    summary_columns = [
        "nodes_created",
        "edges_created",
        "nodes_deleted",
        "edges_deleted",
        "properties_set",
        "properties_removed",
    ]
    summary = {name: repeated.column(name).to_pylist() for name in summary_columns}
    if repeated.num_rows != 1 or summary != {name: [0] for name in summary_columns}:
        raise AssertionError(f"unexpected repeated-compensation summary: {summary}")
    if after_repeated != after or after_repeated_ids != after_ids:
        raise AssertionError("repeated compensation changed the stable identity view")

    extension = Path(native.__file__).resolve()
    evidence = {
        "schema_version": 1,
        "scenario_id": "correction-churn",
        "binding": "python",
        "commit_sha": args.commit_sha,
        "before": before,
        "after": after,
        "stable_node_uuids": reopened_ids,
        "repeated_compensation": "idempotent-no-op",
        "repeated_compensation_summary": summary,
        "reopen_equal": reopened_names == after
        and reopened_ids["Aster Labs"] == created_ids["Aster Labs"],
        "package_version": importlib.metadata.version("graphforge"),
        "native_version": graphforge.version(),
        "native_module_path": str(extension),
        "native_module_sha256": hashlib.sha256(extension.read_bytes()).hexdigest(),
    }
    args.evidence.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
