"""Same-SHA native Python evidence for derived-state freshness."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import uuid

import graphforge
import graphforge._graphforge_rs as native


def states(receipts: list[dict[str, object]]) -> list[str]:
    return [str(receipt["state"]) for receipt in receipts]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    args = parser.parse_args()

    forge = graphforge.GraphForge(str(args.project))
    alice = forge.add_node("Person", name="Alice", summary="Graph systems")
    bob = forge.add_node("Person", name="Bob", summary="Native bindings")
    edge = forge.add_edge(alice, "KNOWS", bob)

    text_current = forge.index("Person", properties=["name"], rebuild=True)
    adjacency_current = forge.index_adjacency()
    embedding_v1 = forge.publish_caller_embeddings(
        "semantic",
        [
            {"node": alice, "vector": [1.0, 0.0]},
            {"node": bob, "vector": [0.0, 1.0]},
        ],
        dimensions=2,
        contract_version="derived-state-v1",
        source_projection={"label": "Person", "recipe": "v1"},
    )
    embedding_initial = forge.inspect_embedding_space_freshness("semantic")

    carol = forge.add_node("Person", name="Carol", summary="Fresh state")
    text_stale = forge.inspect_text_index("Person", properties=["name"])
    forge.execute(
        "MATCH ()-[r:KNOWS]->() WHERE r.edge_uuid = $edge_uuid DELETE r",
        {"edge_uuid": uuid.UUID(edge.uuid)},
    )
    adjacency_stale = forge.inspect_adjacency()
    text_rebuilt = forge.index("Person", properties=["name"], rebuild=True)
    adjacency_rebuilt = forge.rebuild_adjacency()

    embedding_v2 = forge.publish_caller_embeddings(
        "semantic",
        [
            {"node": alice, "vector": [0.0, 1.0]},
            {"node": bob, "vector": [1.0, 0.0]},
            {"node": carol, "vector": [1.0, 0.0]},
        ],
        dimensions=2,
        contract_version="derived-state-v2",
        source_projection={"label": "Person", "recipe": "v2"},
        replace=True,
    )
    embedding_replaced = forge.inspect_embedding_space_freshness("semantic")

    assert states([text_current, text_stale, text_rebuilt]) == [
        "current",
        "stale",
        "current",
    ]
    assert states([adjacency_current, adjacency_stale, adjacency_rebuilt]) == [
        "current",
        "stale",
        "current",
    ]
    assert embedding_initial["state"] == embedding_replaced["state"] == "fresh"
    assert embedding_initial["compatibility_id"] == embedding_v1
    assert embedding_replaced["compatibility_id"] == embedding_v2
    assert embedding_initial["generation_id"] != embedding_replaced["generation_id"]

    authority = {
        "text": forge.inspect_text_index("Person", properties=["name"]),
        "adjacency": forge.inspect_adjacency(),
        "embedding": forge.inspect_embedding_space_freshness("semantic"),
    }
    forge.close()
    reopened = graphforge.GraphForge(str(args.project))
    reopened_authority = {
        "text": reopened.inspect_text_index("Person", properties=["name"]),
        "adjacency": reopened.inspect_adjacency(),
        "embedding": reopened.inspect_embedding_space_freshness("semantic"),
    }
    reopened.close()
    assert reopened_authority == authority

    module_path = Path(native.__file__).resolve()
    evidence = {
        "schema_version": 1,
        "scenario_id": "derived-state-freshness",
        "binding": "python",
        "commit_sha": args.commit_sha,
        "text_states": states([text_current, text_stale, text_rebuilt]),
        "adjacency_states": states([adjacency_current, adjacency_stale, adjacency_rebuilt]),
        "compatibility_ids": [embedding_v1, embedding_v2],
        "generation_ids": [
            embedding_initial["generation_id"],
            embedding_replaced["generation_id"],
        ],
        "embedding_states": [embedding_initial["state"], embedding_replaced["state"]],
        "reopen_equal": reopened_authority == authority,
        "package_version": importlib.metadata.version("graphforge"),
        "native_version": graphforge.version(),
        "native_module_path": str(module_path),
        "native_module_sha256": hashlib.sha256(module_path.read_bytes()).hexdigest(),
    }
    args.evidence.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
