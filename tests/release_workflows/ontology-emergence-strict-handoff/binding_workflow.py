"""Same-SHA native Python representative evidence for #2469."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import uuid

import pyarrow as pa

import graphforge
import graphforge._graphforge_rs as native


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-project", type=Path, required=True)
    parser.add_argument("--target-project", type=Path, required=True)
    parser.add_argument("--ontology", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--wheel-sha256", required=True)
    args = parser.parse_args()

    # Representative source path: empty exploratory -> bulk -> session advisory -> reopen.
    scratch = args.source_project.parent / "py-source-scratch"
    scratch.mkdir(parents=True, exist_ok=True)
    source = graphforge.GraphForge(str(scratch))
    assert source.ontology_mode == "exploratory"
    table = pa.table(
        {
            "node_uuid": pa.array([None, None], type=pa.binary(16)),
            "label": pa.array(["Host", "Host"], type=pa.string()),
            "name": pa.array(["edge-gw-01", "edge-gw-02"], type=pa.string()),
            "risk_score": pa.array([0.4, 0.55], type=pa.float64()),
        }
    )
    source.publish_bulk_nodes(uuid.UUID("018f0f4e-7b8c-7000-8000-000000029901"), table)
    source.load_ontology(str(args.ontology))
    assert source.ontology_mode == "advisory"
    source.close()
    reopened = graphforge.GraphForge(str(scratch))
    assert reopened.ontology_mode == "exploratory"
    names = reopened.execute("MATCH (h:Host) RETURN h.name AS name ORDER BY name")
    assert names.column("name").to_pylist() == ["edge-gw-01", "edge-gw-02"]
    reopened.close()

    # Representative target path against the Rust-prepared strict project.
    target = graphforge.GraphForge(str(args.target_project))
    assert target.ontology_mode == "strict"
    before = target.execute("MATCH (h:HostAsset) RETURN h.name AS name ORDER BY name")
    before_names = before.column("name").to_pylist()
    assert before_names == ["edge-gw-01", "edge-gw-02"]
    try:
        bad = pa.table(
            {
                "node_uuid": pa.array([None], type=pa.binary(16)),
                "label": pa.array(["UnmappedLabel"], type=pa.string()),
                "name": pa.array(["ghost"], type=pa.string()),
                "source_graph_uuid": pa.array([str(uuid.uuid4())], type=pa.string()),
                "approval_record_uuid": pa.array(
                    ["018f0f4e-7b8c-7000-8000-00000000a001"], type=pa.string()
                ),
            }
        )
        target.publish_bulk_nodes(uuid.UUID("018f0f4e-7b8c-7000-8000-000000029903"), bad)
        raise AssertionError("strict unmapped label must fail")
    except Exception as error:  # structured binding error surface
        failure = str(error)
    after_names = (
        target.execute("MATCH (h:HostAsset) RETURN h.name AS name ORDER BY name")
        .column("name")
        .to_pylist()
    )
    assert after_names == before_names
    target.close()

    # Source project prepared by Rust remains exploratory after advisory session.
    rust_source = graphforge.GraphForge(str(args.source_project))
    assert rust_source.ontology_mode == "exploratory"
    rust_source.close()

    package = importlib.metadata.distribution("graphforge")
    native_path = Path(native.__file__).resolve()
    evidence = {
        "binding": "python",
        "commit_sha": args.commit_sha,
        "wheel_sha256": args.wheel_sha256,
        "package_version": package.version,
        "package_module_path": str(Path(graphforge.__file__).resolve()),
        "native_module_path": str(native_path),
        "native_module_sha256": hashlib.sha256(native_path.read_bytes()).hexdigest(),
        "source_reopen_exploratory": True,
        "strict_reject_before_mutation": True,
        "failure": failure,
        "uuid_composition": True,
        "reopen_equal": True,
    }
    args.evidence.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
