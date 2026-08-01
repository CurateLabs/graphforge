"""Shared projection contract bridge for #299.

Prefers #298 modules under ``examples/visualization/shared`` and
``examples/visualization/dataset`` when present. Until that PR merges, a
provisional loader builds the same ``karate-member-friend-v1`` projection
shape from fixtures / on-demand downloads.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable
from urllib.request import urlretrieve

STRESS_ROOT = Path(__file__).resolve().parents[1]
VIS_ROOT = STRESS_ROOT.parent
SHARED_DIR = VIS_ROOT / "shared"
DATASET_DIR = VIS_ROOT / "dataset"
FIXTURES = STRESS_ROOT / "fixtures"
CACHE_DIR = STRESS_ROOT / ".cache"

FACEBOOK_URL = "https://snap.stanford.edu/data/facebook_combined.txt.gz"

# Matches examples/visualization/shared/contract.json from #298.
PROJECTION_CONTRACT = {
    "projection_id": "karate-member-friend-v1",
    "node_label": "Member",
    "edge_type": "FRIEND",
    "directed": False,
    "layout_seed": 42,
    "node_fields": ["id", "label", "club_id"],
    "edge_fields": ["source", "target"],
}


@dataclass(frozen=True)
class ProjectionNode:
    id: str
    label: str
    club_id: int
    group: str = "ungrouped"


@dataclass(frozen=True)
class ProjectionEdge:
    source: str
    target: str
    type: str = "FRIEND"
    directed: bool = False


@dataclass
class GraphProjection:
    nodes: list[ProjectionNode]
    edges: list[ProjectionEdge]
    dataset_id: str
    dataset_checksum: str
    directed: bool
    layout_seed: int
    projection_id: str = PROJECTION_CONTRACT["projection_id"]
    provenance: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "projection_id": self.projection_id,
            "dataset_id": self.dataset_id,
            "dataset_checksum": self.dataset_checksum,
            "directed": self.directed,
            "layout_seed": self.layout_seed,
            "provenance": self.provenance,
            "nodes": [
                {
                    "id": n.id,
                    "label": n.label,
                    "club_id": n.club_id,
                    "group": n.group,
                }
                for n in self.nodes
            ],
            "edges": [
                {
                    "source": e.source,
                    "target": e.target,
                    "type": e.type,
                    "directed": e.directed,
                }
                for e in self.edges
            ],
        }


def _load_module(name: str, path: Path) -> Any | None:
    if not path.is_file():
        return None
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    # Ensure sibling imports (dataset.*) resolve when loading shared.projection.
    if str(VIS_ROOT) not in sys.path:
        sys.path.insert(0, str(VIS_ROOT))
    spec.loader.exec_module(module)
    return module


def _try_load_298_projection() -> Any | None:
    return _load_module("graphforge_viz_shared_projection", SHARED_DIR / "projection.py")


def _parse_edge_list(path: Path) -> list[tuple[str, str]]:
    edges: list[tuple[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.replace(",", "\t").split()
        if len(parts) < 2:
            continue
        u, v = parts[0], parts[1]
        if u == v:
            continue
        a, b = (u, v) if _id_key(u) <= _id_key(v) else (v, u)
        edges.append((a, b))
    return sorted(set(edges), key=lambda e: (_id_key(e[0]), _id_key(e[1])))


def _id_key(value: str) -> tuple[int, str]:
    try:
        return (0, f"{int(value):020d}")
    except ValueError:
        return (1, value)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_gml_undirected_edges(gml_text: str) -> list[tuple[str, str]]:
    edges: set[tuple[str, str]] = set()
    for match in re.finditer(
        r"edge\s*\[\s*source\s+(\d+)\s+target\s+(\d+)",
        gml_text,
    ):
        left, right = match.group(1), match.group(2)
        if left == right:
            continue
        edge = (left, right) if int(left) < int(right) else (right, left)
        edges.add(edge)
    return sorted(edges, key=lambda e: (int(e[0]), int(e[1])))


def ensure_facebook_edge_list() -> Path:
    import gzip

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    gz_path = CACHE_DIR / "facebook_combined.txt.gz"
    txt_path = CACHE_DIR / "facebook_combined.txt"
    if txt_path.is_file() and txt_path.stat().st_size > 0:
        return txt_path
    if not gz_path.is_file():
        urlretrieve(FACEBOOK_URL, gz_path)  # noqa: S310 — fixed SNAP URL
    with gzip.open(gz_path, "rb") as src, txt_path.open("wb") as dst:
        dst.write(src.read())
    return txt_path


def load_edge_list_dataset(name: str) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    if name == "karate":
        # Prefer #298 fetch + GML when available.
        fetch_mod = _load_module("graphforge_viz_dataset_fetch", DATASET_DIR / "fetch.py")
        if fetch_mod is not None:
            extract_dir = fetch_mod.fetch_dataset()
            gml_path = extract_dir / "karate.gml"
            edges = parse_gml_undirected_edges(gml_path.read_text(encoding="utf-8"))
            manifest = fetch_mod.load_manifest()
            provenance = {
                "dataset": manifest["id"],
                "license": manifest["license"],
                "source_url": manifest["source_url"],
                "citation": manifest["citation"],
                "checksum_sha256": manifest["archive"]["sha256"],
                "node_count": manifest["graph"]["node_count"],
                "edge_count": len(edges),
                "projection_id": PROJECTION_CONTRACT["projection_id"],
                "via": "examples/visualization/dataset/fetch.py",
            }
            return edges, provenance

        path = FIXTURES / "karate_edges.txt"
        nodes_meta = json.loads((FIXTURES / "karate_nodes.json").read_text(encoding="utf-8"))
        edges = _parse_edge_list(path)
        provenance = {
            "dataset": "zachary-karate-club",
            "license": nodes_meta.get(
                "license",
                "Public research redistribution with required citation of Zachary (1977)",
            ),
            "citation": "W. W. Zachary, An information flow model for conflict and fission in small groups, Journal of Anthropological Research 33, 452-473 (1977).",
            "fixture_path": str(path.relative_to(STRESS_ROOT)),
            "checksum_sha256": _sha256_file(path),
            "node_count": len(nodes_meta["nodes"]),
            "edge_count": len(edges),
            "projection_id": nodes_meta.get(
                "projection_id", PROJECTION_CONTRACT["projection_id"]
            ),
            "via": "stress/fixtures provisional (#298 shared not present)",
        }
        return edges, provenance

    if name == "facebook":
        path = ensure_facebook_edge_list()
        edges = _parse_edge_list(path)
        node_ids = {u for e in edges for u in e}
        provenance = {
            "dataset": "snap-ego-facebook-combined",
            "license": "SNAP datasets are freely available for research; see https://snap.stanford.edu/data/",
            "source_url": FACEBOOK_URL,
            "retrieval": "downloaded on demand into examples/visualization/stress/.cache/",
            "checksum_sha256": _sha256_file(path),
            "node_count": len(node_ids),
            "edge_count": len(edges),
            "projection_id": PROJECTION_CONTRACT["projection_id"],
            "note": (
                "Ladder steps >34 nodes use SNAP facebook_combined with the same "
                "karate-member-friend-v1 field shape (Member/FRIEND/layout_seed=42)."
            ),
            "via": "stress facebook extension",
        }
        return edges, provenance
    raise ValueError(f"unknown dataset {name!r}")


def sample_subgraph(
    edges: list[tuple[str, str]],
    target_nodes: int,
    seed: int,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Deterministic BFS growth from the lowest numeric/lexicographic node id."""
    from collections import defaultdict, deque

    adjacency: dict[str, list[str]] = defaultdict(list)
    for u, v in edges:
        adjacency[u].append(v)
        adjacency[v].append(u)
    for node, neighbors in adjacency.items():
        adjacency[node] = sorted(neighbors, key=_id_key)

    if not adjacency:
        return [], []

    start = sorted(adjacency, key=_id_key)[0]
    order_key = seed
    visited: list[str] = []
    seen: set[str] = set()
    queue: deque[str] = deque([start])
    seen.add(start)
    while queue and len(visited) < target_nodes:
        node = queue.popleft()
        visited.append(node)
        neighbors = adjacency[node]
        if neighbors:
            rot = order_key % len(neighbors)
            rotated = neighbors[rot:] + neighbors[:rot]
        else:
            rotated = []
        for nxt in rotated:
            if nxt not in seen:
                seen.add(nxt)
                queue.append(nxt)
        order_key += 1

    selected = set(visited)
    sub_edges = [(u, v) for u, v in edges if u in selected and v in selected]
    return visited, sub_edges


def projection_from_edges(
    edges: list[tuple[str, str]],
    *,
    dataset_id: str,
    checksum: str,
    provenance: dict[str, Any] | None = None,
) -> GraphProjection:
    node_ids = sorted({n for e in edges for n in e}, key=_id_key)
    nodes = [
        ProjectionNode(
            id=nid,
            label=f"M{nid}",
            club_id=int(nid) if nid.isdigit() else abs(hash(nid)) % (10**9),
            group="ungrouped",
        )
        for nid in node_ids
    ]
    proj_edges = [
        ProjectionEdge(
            source=u,
            target=v,
            type=PROJECTION_CONTRACT["edge_type"],
            directed=PROJECTION_CONTRACT["directed"],
        )
        for u, v in edges
    ]
    return GraphProjection(
        nodes=nodes,
        edges=proj_edges,
        dataset_id=dataset_id,
        dataset_checksum=checksum,
        directed=PROJECTION_CONTRACT["directed"],
        layout_seed=PROJECTION_CONTRACT["layout_seed"],
        provenance=provenance or {},
    )


def projection_from_298_dict(payload: dict[str, Any], *, elapsed: float) -> GraphProjection:
    nodes = [
        ProjectionNode(
            id=str(row["id"]),
            label=str(row["label"]),
            club_id=int(row.get("club_id", row["id"])),
        )
        for row in payload["nodes"]
    ]
    edges = [
        ProjectionEdge(
            source=str(row["source"]),
            target=str(row["target"]),
            type=PROJECTION_CONTRACT["edge_type"],
            directed=bool(payload.get("directed", False)),
        )
        for row in payload["edges"]
    ]
    checksum = hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()
    return GraphProjection(
        nodes=nodes,
        edges=edges,
        dataset_id="zachary-karate-club",
        dataset_checksum=checksum,
        directed=bool(payload.get("directed", False)),
        layout_seed=int(payload.get("layout_seed", PROJECTION_CONTRACT["layout_seed"])),
        projection_id=str(
            payload.get("projection_id", PROJECTION_CONTRACT["projection_id"])
        ),
        provenance={
            "via": "examples/visualization/shared/projection.py",
            "graphforge_projection_seconds_hint": elapsed,
        },
    )


def load_projection_via_graphforge(
    edges: Iterable[tuple[str, str]],
) -> tuple[GraphProjection, float]:
    """Build the shared projection through GraphForge's public Python API."""
    import graphforge
    from graphforge import GraphForge

    edge_list = list(edges)
    started = time.perf_counter()
    forge = GraphForge()
    handles: dict[str, Any] = {}
    for nid in sorted({n for e in edge_list for n in e}, key=_id_key):
        club_id = int(nid) if nid.isdigit() else abs(hash(nid)) % (10**9)
        handles[nid] = forge.add_node(
            PROJECTION_CONTRACT["node_label"],
            club_id=club_id,
            label=f"M{nid}",
        )
    for u, v in edge_list:
        forge.add_edge(handles[u], PROJECTION_CONTRACT["edge_type"], handles[v])
    node_table = forge.execute(
        "MATCH (n:Member) RETURN n.club_id AS club_id, n.label AS label ORDER BY club_id"
    )
    edge_table = forge.execute(
        "MATCH (a:Member)-[:FRIEND]-(b:Member) "
        "WHERE a.club_id < b.club_id "
        "RETURN a.club_id AS source, b.club_id AS target "
        "ORDER BY source, target"
    )
    forge.close()
    elapsed = time.perf_counter() - started

    nodes = [
        ProjectionNode(
            id=str(row["club_id"]),
            label=str(row["label"]),
            club_id=int(row["club_id"]),
        )
        for row in _arrow_rows(node_table)
    ]
    proj_edges = [
        ProjectionEdge(
            source=str(row["source"]),
            target=str(row["target"]),
            type=PROJECTION_CONTRACT["edge_type"],
            directed=False,
        )
        for row in _arrow_rows(edge_table)
    ]
    checksum = hashlib.sha256(
        json.dumps(
            {
                "nodes": [n.club_id for n in nodes],
                "edges": [(e.source, e.target) for e in proj_edges],
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    projection = GraphProjection(
        nodes=nodes,
        edges=proj_edges,
        dataset_id="graphforge-public-api-projection",
        dataset_checksum=checksum,
        directed=False,
        layout_seed=PROJECTION_CONTRACT["layout_seed"],
        provenance={
            "api": "graphforge.GraphForge.add_node/add_edge/execute",
            "graphforge_version": getattr(graphforge, "__version__", "unknown"),
            "projection_id": PROJECTION_CONTRACT["projection_id"],
        },
    )
    return projection, elapsed


def _arrow_rows(table: Any) -> list[dict[str, Any]]:
    if hasattr(table, "to_pylist"):
        rows = table.to_pylist()
        if rows and isinstance(rows[0], dict):
            return rows
    if hasattr(table, "to_pydict"):
        data = table.to_pydict()
        n = len(next(iter(data.values()))) if data else 0
        return [{k: data[k][i] for k in data} for i in range(n)]
    names = list(table.schema.names) if hasattr(table, "schema") else list(
        getattr(table, "column_names", [])
    )
    data = {name: table.column(name).to_pylist() for name in names}
    return [{k: data[k][i] for k in data} for i in range(table.num_rows)]


def resolve_base_dataset(max_nodes: int) -> tuple[list[tuple[str, str]], dict[str, Any]]:
    if max_nodes <= 34:
        return load_edge_list_dataset("karate")
    return load_edge_list_dataset("facebook")


def build_step_projection(
    target_nodes: int,
    seed: int,
    *,
    use_graphforge: bool = True,
) -> tuple[GraphProjection, float]:
    """Return projection for a ladder step and GraphForge projection seconds."""
    shared = _try_load_298_projection()
    # Full #298 projection only covers karate (34). Use it when the step fits
    # and GraphForge is requested; still apply deterministic subgraph sampling.
    if (
        shared is not None
        and use_graphforge
        and target_nodes <= 34
        and hasattr(shared, "project")
    ):
        started = time.perf_counter()
        try:
            full = shared.project()
            elapsed = time.perf_counter() - started
            edges = [
                (str(e["source"]), str(e["target"]))
                if int(e["source"]) < int(e["target"])
                else (str(e["target"]), str(e["source"]))
                for e in full["edges"]
            ]
            node_ids, sub_edges = sample_subgraph(edges, min(target_nodes, 34), seed)
            selected = set(node_ids)
            nodes = [
                ProjectionNode(
                    id=str(n["id"]),
                    label=str(n["label"]),
                    club_id=int(n.get("club_id", n["id"])),
                )
                for n in full["nodes"]
                if str(n["id"]) in selected
            ]
            nodes.sort(key=lambda n: n.club_id)
            proj_edges = [
                ProjectionEdge(source=u, target=v)
                for u, v in sub_edges
            ]
            checksum = hashlib.sha256(
                json.dumps(
                    {"nodes": [n.club_id for n in nodes], "edges": sub_edges},
                    sort_keys=True,
                ).encode()
            ).hexdigest()
            return (
                GraphProjection(
                    nodes=nodes,
                    edges=proj_edges,
                    dataset_id="zachary-karate-club",
                    dataset_checksum=checksum,
                    directed=bool(full.get("directed", False)),
                    layout_seed=int(
                        full.get("layout_seed", PROJECTION_CONTRACT["layout_seed"])
                    ),
                    projection_id=str(
                        full.get(
                            "projection_id", PROJECTION_CONTRACT["projection_id"]
                        )
                    ),
                    provenance={
                        "via": "examples/visualization/shared/projection.project",
                    },
                ),
                elapsed,
            )
        except ImportError:
            pass

    edges, provenance = resolve_base_dataset(target_nodes)
    available = len({n for e in edges for n in e})
    target = min(target_nodes, available)
    _node_ids, sub_edges = sample_subgraph(edges, target, seed)
    if use_graphforge:
        try:
            projection, elapsed = load_projection_via_graphforge(sub_edges)
            projection.provenance = {**provenance, **projection.provenance}
            projection.dataset_id = str(provenance.get("dataset", projection.dataset_id))
            projection.dataset_checksum = str(
                provenance.get("checksum_sha256", projection.dataset_checksum)
            )
            return projection, elapsed
        except ImportError:
            pass

    projection = projection_from_edges(
        sub_edges,
        dataset_id=str(provenance.get("dataset", "unknown")),
        checksum=str(provenance.get("checksum_sha256", "")),
        provenance={**provenance, "graphforge": "bypassed-import-error-or-unit-test"},
    )
    return projection, 0.0
