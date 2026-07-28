"""Representative Python replay of the #2465 structural workflow."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path

import graphforge
from graphforge import GraphForge, _graphforge_rs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--wheel-sha256", required=True)
    args = parser.parse_args()
    args.project.mkdir(parents=True)

    forge = GraphForge(str(args.project))
    handles = {
        name: forge.add_node("Actor", name=name, summary=summary)
        for name, summary in [
            ("Ada", "regional organizer"),
            ("Ben", "operations liaison"),
            ("Cy", "event coordinator"),
            ("Dana", "external contact"),
            ("Eli", "independent observer"),
        ]
    }
    for source, target in [("Ada", "Ben"), ("Ben", "Cy"), ("Dana", "Eli")]:
        forge.add_edge(handles[source], "COMMUNICATED", handles[target])

    scope = forge.execute(
        "MATCH (a:Actor) RETURN a.node_uuid AS node_uuid, a.name AS name ORDER BY name"
    )
    found = forge.find("organizer", label="Actor", limit=5)
    rank = forge.rank("Actor", by="degree", via="COMMUNICATED", directed=False)
    clusters = forge.cluster("Actor", by="components", via="COMMUNICATED", directed=False)
    path = forge.paths(
        handles["Ada"],
        handles["Cy"],
        by="bfs",
        via="COMMUNICATED",
        directed=False,
    )
    ordered_names = scope.column("name").to_pylist()
    rank_uuids = sorted(value.hex() for value in rank.column("node_uuid").to_pylist())
    cluster_uuids = sorted(value.hex() for value in clusters.column("node_uuid").to_pylist())
    assert ordered_names == ["Ada", "Ben", "Cy", "Dana", "Eli"]
    assert found.column("name").to_pylist() == ["Ada"]
    assert rank_uuids == cluster_uuids
    assert path.num_rows == 1
    forge.close()

    reopened = GraphForge(str(args.project))
    reopened_scope = reopened.execute(
        "MATCH (a:Actor) RETURN a.node_uuid AS node_uuid, a.name AS name ORDER BY name"
    )
    reopened_rank = reopened.rank("Actor", by="degree", via="COMMUNICATED", directed=False)
    assert reopened_scope.equals(scope)
    assert reopened_rank.equals(rank)
    reopened.close()

    args.evidence.write_text(
        json.dumps(
            {
                "binding": "python",
                "commit_sha": args.commit_sha,
                "package_version": importlib.metadata.version("graphforge"),
                "package_module_path": str(Path(graphforge.__file__).resolve()),
                "native_module_path": str(Path(_graphforge_rs.__file__).resolve()),
                "native_module_sha256": hashlib.sha256(
                    Path(_graphforge_rs.__file__).read_bytes()
                ).hexdigest(),
                "wheel_sha256": args.wheel_sha256,
                "ordered_scope": ordered_names,
                "stable_node_uuids": rank_uuids,
                "uuid_composition": True,
                "path_rows": path.num_rows,
                "reopen_equal": True,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
