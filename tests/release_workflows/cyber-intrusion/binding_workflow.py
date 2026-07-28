"""Same-SHA native Python replay for the synthetic cyber workflow."""

from __future__ import annotations

import argparse
import gc
import hashlib
import importlib.metadata
import json
from pathlib import Path

import graphforge
import graphforge._graphforge_rs as native


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--ontology", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()

    args.project.mkdir(parents=True)
    forge = graphforge.GraphForge(str(args.project))
    forge.load_ontology(str(args.ontology))
    hosts = [
        forge.add_node(
            "Host",
            name=f"PY-HOST-{index}",
            hostname=f"py-host-{index}.example",
            criticality=5 - index,
            exposed=index == 1,
        )
        for index in range(1, 5)
    ]
    forge.add_edge(hosts[0], "COMMUNICATED_WITH", hosts[1])
    forge.add_edge(hosts[1], "COMMUNICATED_WITH", hosts[2])
    forge.add_edge(hosts[2], "COMMUNICATED_WITH", hosts[3])
    forge.add_edge(hosts[0], "COMMUNICATED_WITH", hosts[2])
    alerts = [
        forge.add_node(
            "Alert",
            name="PY-ALERT-01",
            summary="encoded powershell launch",
            severity=9,
        ),
        forge.add_node(
            "Alert",
            name="PY-ALERT-02",
            summary="powershell lateral movement",
            severity=8,
        ),
        forge.add_node(
            "Alert",
            name="PY-ALERT-03",
            summary="routine backup verification",
            severity=2,
        ),
    ]
    forge.publish_caller_embeddings(
        "alert-semantic-v1",
        [
            {"node": alerts[0], "vector": [1.0, 0.0, 0.0]},
            {"node": alerts[1], "vector": [0.9, 0.2, 0.1]},
            {"node": alerts[2], "vector": [0.0, 0.0, 1.0]},
        ],
        dimensions=3,
        source_projection={"label": "Alert", "property": "summary"},
        contract_version="cyber-intrusion-v1",
    )
    hybrid = forge.find(
        "encoded powershell",
        label="Alert",
        vector=[1.0, 0.0, 0.0],
        space="alert-semantic-v1",
        limit=3,
    )
    assert hybrid.column("matched_on").to_pylist()[0] == "text+vector"
    rank = forge.rank("Host", by="degree", via="COMMUNICATED_WITH")
    cluster = forge.cluster("Host", by="components", via="COMMUNICATED_WITH")
    path = forge.paths(hosts[0], hosts[3], by="bfs", via="COMMUNICATED_WITH", directed=True)
    similar = forge.similar("Host", by="node_similarity", k=3, via="COMMUNICATED_WITH")
    assert (
        rank.num_rows == 4
        and cluster.num_rows == 4
        and path.num_rows == 1
        and similar.num_rows == 2
    )

    try:
        forge.find()
    except graphforge.GraphForgeError as error:
        invalid_code = error.code
    else:
        raise AssertionError("invalid find unexpectedly succeeded")
    assert invalid_code == "GF_VALIDATION"

    host_uuids = [str(host.uuid) for host in hosts]
    del forge
    gc.collect()
    reopened = graphforge.GraphForge(str(args.project))
    reopened.load_ontology(str(args.ontology))
    reopened_rank = reopened.rank("Host", by="degree", via="COMMUNICATED_WITH")
    assert reopened_rank.equals(rank)
    assert reopened.find(
        "encoded powershell",
        label="Alert",
        vector=[1.0, 0.0, 0.0],
        space="alert-semantic-v1",
        limit=3,
    ).equals(hybrid)

    module_path = Path(native.__file__).resolve()
    evidence = {
        "schema_version": 1,
        "scenario_id": "cyber-intrusion",
        "binding": "python",
        "host_uuids": host_uuids,
        "operation_rows": {
            "hybrid": hybrid.num_rows,
            "rank": rank.num_rows,
            "cluster": cluster.num_rows,
            "paths": path.num_rows,
            "similar": similar.num_rows,
        },
        "invalid_error": invalid_code,
        "reopen_equal": True,
        "package_version": importlib.metadata.version("graphforge"),
        "native_version": graphforge.version(),
        "native_module_path": str(module_path),
        "native_module_sha256": hashlib.sha256(module_path.read_bytes()).hexdigest(),
    }
    args.evidence.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
