"""Graphalytics on the GDC scorecard rung runner (#952, #1903).

A Graphalytics archive ships the graph (``<graph>.v``, ``<graph>.e``), its
``<graph>.properties`` and one reference output per algorithm
(``<graph>-BFS``, ``-WCC``, ``-LCC``, ``-SSSP``, ``-PR``, ``-CDLP``): one line
per vertex, ``<vertex id> <value>``.

Before the rung converts anything, :func:`check_archive` holds the archive's
``.properties`` to the committed ladder: the vertex and edge counts, the
direction, the algorithm list (every listed algorithm is either run or
refused, and nothing else is), and each BFS and SSSP source vertex, which the
workload names by the UUID the converter derives for it, so the driver
dispatches by ``NodeSelector::Uuid``.

After the query phase, :func:`archive_reference` converts the archive's
reference outputs into the driver's written-result cell form (Arrow display
text) and states the Graphalytics matching rule per algorithm:

- BFS, ``exact``: rows ``(target_uuid, cost)`` keyed by target. ``paths``
  returns only reached vertices, so the reference's unreachable vertices
  (depth ``9223372036854775807``) are matched by absence and every other
  vertex by its exact depth.
- SSSP, ``epsilon`` 1e-4: rows ``(target_uuid, cost)``; ``infinity`` is
  matched by absence the same way.
- WCC, ``equivalence``: rows ``(id, community_id)``, the same partition up to
  relabelling.
- LCC, ``epsilon`` 1e-4: rows ``(id, score)``.

Each reference file must list every vertex of the graph exactly once, so
matching by absence cannot hide a vertex the reference leaves out.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
from typing import Any

from graphforge_bench.gdc_rung_inputs import (
    REFERENCE_SCHEMA,
    LadderSpec,
    RungInputError,
    read_json,
)

ARCHIVE_OUTPUTS = "graphalytics"
BFS_UNREACHABLE = 9_223_372_036_854_775_807
EPSILON = 1e-4
RUNS = 3
OUTPUT_SUFFIX = {
    "bfs": "BFS",
    "cdlp": "CDLP",
    "lcc": "LCC",
    "pr": "PR",
    "sssp": "SSSP",
    "wcc": "WCC",
}
NODE_NAMESPACE = b"graphforge.gdc.node.v1\0"


def node_uuid(label: str, vertex: int) -> str:
    """The converter's node identity (``identity.rs``) as Arrow displays it: 32 hex digits."""
    digest = bytearray(
        hashlib.sha256(
            NODE_NAMESPACE + label.encode("utf-8") + b"\0" + vertex.to_bytes(8, "big", signed=True)
        ).digest()[:16]
    )
    digest[6] = (digest[6] & 0x0F) | 0x70
    digest[8] = (digest[8] & 0x3F) | 0x80
    return digest.hex()


def canonical_uuid(hex_digits: str) -> str:
    """The hyphenated form ``NodeSelector::uuid`` parses."""
    h = hex_digits
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


def float64_text(value: int) -> str:
    """Arrow's display text of an integral Float64 such as a BFS depth (``3`` → ``3.0``)."""
    return f"{value}.0"


def parse_properties(text: str) -> dict[str, str]:
    """The ``key = value`` lines of a Graphalytics ``.properties`` file."""
    values: dict[str, str] = {}
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith(("#", "!")):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key.strip():
            raise RungInputError("archive_properties_invalid", f"line {number}: {raw!r}")
        key = key.strip()
        if key in values:
            raise RungInputError("archive_properties_invalid", f"line {number}: {key} repeats")
        values[key] = value.strip()
    return values


@dataclass(frozen=True)
class GraphProperties:
    """What one archive's ``.properties`` declares about its graph."""

    name: str
    vertex_file: str
    edge_file: str
    vertices: int
    edges: int
    directed: bool
    weighted: bool
    algorithms: tuple[str, ...]
    bfs_source: int | None
    sssp_source: int | None
    pr_damping: float | None = None
    pr_iterations: int | None = None
    cdlp_iterations: int | None = None


def _integer(values: Mapping[str, str], key: str) -> int:
    try:
        return int(values[key])
    except (KeyError, ValueError) as error:
        raise RungInputError("archive_properties_invalid", f"{key}: {error}") from error


def _iterations(values: Mapping[str, str], key: str) -> int:
    iterations = _integer(values, key)
    if not 0 <= iterations <= 2**32 - 1:
        raise RungInputError("archive_properties_invalid", f"{key} must fit UInt32")
    return iterations


def _damping(values: Mapping[str, str]) -> float:
    key = "pr.damping-factor"
    try:
        damping = float(values[key])
    except (KeyError, ValueError) as error:
        raise RungInputError("archive_properties_invalid", f"{key}: {error}") from error
    if not math.isfinite(damping) or not 0.0 <= damping <= 1.0:
        raise RungInputError("archive_properties_invalid", f"{key} must be finite and in [0, 1]")
    return damping


def read_properties(input_root: Path) -> GraphProperties:
    """The single ``<graph>.properties`` in an extracted Graphalytics archive."""
    found = sorted(input_root.glob("*.properties"))
    if len(found) != 1:
        raise RungInputError(
            "archive_properties_invalid", f"{input_root} holds {len(found)} .properties files"
        )
    path = found[0]
    name = path.name.removesuffix(".properties")
    values = parse_properties(path.read_text(encoding="utf-8"))
    prefix = f"graph.{name}."
    get = {
        key.removeprefix(prefix): value for key, value in values.items() if key.startswith(prefix)
    }
    directed = get.get("directed")
    if directed not in ("true", "false"):
        raise RungInputError("archive_properties_invalid", f"{prefix}directed is {directed!r}")
    algorithms = tuple(
        item.strip() for item in get.get("algorithms", "").split(",") if item.strip()
    )
    unknown = set(algorithms) - set(OUTPUT_SUFFIX)
    if not algorithms or unknown or len(set(algorithms)) != len(algorithms):
        raise RungInputError("archive_properties_invalid", f"{prefix}algorithms: {algorithms}")
    names = [
        item.strip() for item in get.get("edge-properties.names", "").split(",") if item.strip()
    ]
    return GraphProperties(
        name=name,
        vertex_file=get.get("vertex-file", ""),
        edge_file=get.get("edge-file", ""),
        vertices=_integer(get, "meta.vertices"),
        edges=_integer(get, "meta.edges"),
        directed=directed == "true",
        weighted="weight" in names,
        algorithms=algorithms,
        bfs_source=_integer(get, "bfs.source-vertex") if "bfs" in algorithms else None,
        sssp_source=_integer(get, "sssp.source-vertex") if "sssp" in algorithms else None,
        pr_damping=_damping(get) if "pr" in algorithms else None,
        pr_iterations=_iterations(get, "pr.num-iterations") if "pr" in algorithms else None,
        cdlp_iterations=_iterations(get, "cdlp.max-iterations") if "cdlp" in algorithms else None,
    )


def _ladder_entry(spec: LadderSpec, rung_id: str) -> Mapping[str, Any]:
    for entry in spec.counts_ladder()["datasets"]:
        if entry["id"] == rung_id:
            return entry
    raise RungInputError("invalid_rung_spec", f"count ladder has no rung {rung_id!r}")


def _tables(spec: LadderSpec, entry: Mapping[str, Any]) -> Mapping[str, Any]:
    mapping = read_json(spec.resolve(entry["load_mapping"]))
    return {"node": mapping["node_tables"][0], "edge": mapping["edge_tables"][0]}


@dataclass(frozen=True)
class Expectation:
    """How one algorithm must be dispatched on this graph."""

    operation: Mapping[str, Any]
    source: int | None
    columns: list[str]


def expectation(algorithm: str, graph: GraphProperties, label: str) -> Expectation:
    """The analyst-verb dispatch and answer columns of one supported algorithm."""
    if algorithm == "bfs":
        operation = {"kind": "paths", "by": "bfs", "directed": graph.directed}
        return Expectation(operation, graph.bfs_source, ["target_uuid", "cost"])
    if algorithm == "sssp":
        operation = {
            "kind": "paths",
            "by": "dijkstra",
            "directed": graph.directed,
            "weight": "weight",
        }
        return Expectation(operation, graph.sssp_source, ["target_uuid", "cost"])
    if algorithm == "wcc":
        operation = {"kind": "cluster", "label": label, "by": "components", "directed": False}
        return Expectation(operation, None, ["id", "community_id"])
    if algorithm == "pr":
        operation = {
            "kind": "rank",
            "label": label,
            "by": "pagerank",
            "directed": graph.directed,
            "pagerank": {"damping": graph.pr_damping, "iterations": graph.pr_iterations},
        }
        return Expectation(operation, None, ["id", "score"])
    if algorithm == "cdlp":
        operation = {
            "kind": "cluster",
            "label": label,
            "by": "label_propagation",
            "directed": graph.directed,
            "synchronous_label_propagation": {
                "iterations": graph.cdlp_iterations,
                "initial_label_property": "id",
            },
        }
        return Expectation(operation, None, ["id", "community_id"])
    if algorithm == "lcc":
        operation = {
            "kind": "rank",
            "label": label,
            "by": "clustering_coefficient",
            "directed": graph.directed,
            "clustering_normalization": "neighbor_edges",
        }
        return Expectation(operation, None, ["id", "score"])
    raise RungInputError("archive_properties_mismatch", f"{algorithm} has no supported mapping")


def check_archive(spec: LadderSpec, rung: Mapping[str, Any], input_root: Path) -> GraphProperties:
    """Hold the archive's ``.properties`` to the committed ladder and workload.

    Raises ``archive_properties_mismatch`` for any disagreement, before the
    rung converts or loads anything.
    """
    graph = read_properties(input_root)
    entry = _ladder_entry(spec, rung["id"])
    tables = _tables(spec, entry)
    declared = {
        "vertices": (graph.vertices, entry["vertices"]),
        "edges": (graph.edges, entry["edges"]),
        "directed": (graph.directed, entry["directed"]),
        "weighted": (graph.weighted, entry["weighted"]),
        "algorithms": (sorted(graph.algorithms), sorted(entry["algorithms"])),
        "bfs source": (graph.bfs_source, entry["bfs_source_vertex"]),
        "sssp source": (graph.sssp_source, entry["sssp_source_vertex"]),
        "pr damping": (graph.pr_damping, entry.get("pr_damping")),
        "pr iterations": (graph.pr_iterations, entry.get("pr_iterations")),
        "cdlp iterations": (graph.cdlp_iterations, entry.get("cdlp_iterations")),
        "vertex file": ([graph.vertex_file], tables["node"]["files"]),
        "edge file": ([graph.edge_file], tables["edge"]["files"]),
    }
    differ = [
        f"{name}: archive {got!r}, ladder {want!r}"
        for name, (got, want) in declared.items()
        if got != want
    ]
    if differ:
        raise RungInputError("archive_properties_mismatch", "; ".join(differ))
    check_workload(spec, rung, graph)
    return graph


def ladder_graph(spec: LadderSpec, rung_id: str) -> GraphProperties:
    """The graph facts the committed ladder records for a rung (no archive needed)."""
    entry = _ladder_entry(spec, rung_id)
    tables = _tables(spec, entry)
    return GraphProperties(
        name=rung_id,
        vertex_file=tables["node"]["files"][0],
        edge_file=tables["edge"]["files"][0],
        vertices=int(entry["vertices"]),
        edges=int(entry["edges"]),
        directed=bool(entry["directed"]),
        weighted=bool(entry["weighted"]),
        algorithms=tuple(entry["algorithms"]),
        bfs_source=entry["bfs_source_vertex"],
        sssp_source=entry["sssp_source_vertex"],
        pr_damping=entry.get("pr_damping"),
        pr_iterations=entry.get("pr_iterations"),
        cdlp_iterations=entry.get("cdlp_iterations"),
    )


def check_workload(spec: LadderSpec, rung: Mapping[str, Any], graph: GraphProperties) -> None:
    """Every listed algorithm is run or refused, and each run is dispatched as the graph needs."""
    label = _tables(spec, _ladder_entry(spec, rung["id"]))["node"]["label"]
    workload = read_json(spec.resolve(rung["workload"]))
    variants = {variant["id"]: variant for variant in workload["variants"]}
    refused = {item["query_id"] for item in rung["refused"]}
    if set(variants) | refused != set(graph.algorithms):
        raise RungInputError(
            "archive_properties_mismatch",
            f"runs {sorted(variants)} and refuses {sorted(refused)}; the archive lists "
            f"{sorted(graph.algorithms)}",
        )
    for algorithm, variant in variants.items():
        _check_variant(algorithm, variant, expectation(algorithm, graph, label), label)


def _check_variant(
    algorithm: str, variant: Mapping[str, Any], expected: Expectation, label: str
) -> None:
    def fail(detail: str) -> None:
        raise RungInputError("archive_properties_mismatch", f"{algorithm}: {detail}")

    operation = dict(variant["operation"])
    source = operation.pop("source", None)
    if operation != expected.operation:
        fail(f"dispatches {operation}, the archive needs {expected.operation}")
    if variant.get("columns") != expected.columns:
        fail(f"keeps {variant.get('columns')}, the answer is {expected.columns}")
    bindings = variant["bindings"]
    if len(bindings) != RUNS or len({json_key(b.get("params", {})) for b in bindings}) != 1:
        fail(f"Tp is the mean of {RUNS} identical runs; the workload has {len(bindings)}")
    if expected.source is None:
        if source is not None or bindings[0].get("params"):
            fail("takes no source")
        return
    if source != {"uuid_param": "source"}:
        fail(f"selects its source by {source}, not by UUID")
    want = canonical_uuid(node_uuid(label, expected.source))
    got = bindings[0]["params"].get("source")
    if got != {"type": "Str", "value": want}:
        fail(f"source {got} is not vertex {expected.source} ({want})")


def json_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def parse_vertex_values(path: Path, vertices: int) -> list[tuple[int, str]]:
    """``<vertex> <value>`` lines; every vertex of the graph exactly once."""
    rows: list[tuple[int, str]] = []
    seen: set[int] = set()
    with path.open(encoding="utf-8") as stream:
        for number, raw in enumerate(stream, start=1):
            fields = raw.split()
            if not fields:
                continue
            if len(fields) != 2:
                raise RungInputError("reference_invalid", f"{path.name}:{number}: {raw.strip()!r}")
            try:
                vertex = int(fields[0])
                float(fields[1])
            except ValueError as error:
                raise RungInputError(
                    "reference_invalid", f"{path.name}:{number}: {error}"
                ) from error
            if vertex in seen:
                raise RungInputError("reference_invalid", f"{path.name}: vertex {vertex} repeats")
            seen.add(vertex)
            rows.append((vertex, fields[1]))
    if len(rows) != vertices:
        raise RungInputError(
            "reference_invalid", f"{path.name} lists {len(rows)} vertices, the graph has {vertices}"
        )
    return rows


def _reachable(token: str, *, integral: bool) -> bool:
    if integral:
        return int(token) != BFS_UNREACHABLE
    return float(token) != float("inf")


def convert_reference(
    algorithm: str, rows: Sequence[tuple[int, str]], label: str
) -> tuple[dict[str, Any], list[str], list[list[str]]]:
    """One algorithm's reference rows in the driver's cell form, with its matching rule."""
    if algorithm == "bfs":
        cells = [
            [node_uuid(label, vertex), float64_text(int(token))]
            for vertex, token in rows
            if _reachable(token, integral=True)
        ]
        return {"matching": "exact", "key": ["target_uuid"]}, ["target_uuid", "cost"], cells
    if algorithm == "sssp":
        cells = [
            [node_uuid(label, vertex), token]
            for vertex, token in rows
            if _reachable(token, integral=False)
        ]
        rule = {"matching": "epsilon", "epsilon": EPSILON, "key": ["target_uuid"]}
        return rule, ["target_uuid", "cost"], cells
    if algorithm == "wcc":
        cells = [[str(vertex), str(int(token))] for vertex, token in rows]
        rule = {"matching": "equivalence", "key": ["id"], "label": "community_id"}
        return rule, ["id", "community_id"], cells
    if algorithm == "cdlp":
        cells = [[str(vertex), str(int(token))] for vertex, token in rows]
        return {"matching": "exact", "key": ["id"]}, ["id", "community_id"], cells
    if algorithm in ("pr", "lcc"):
        cells = [[str(vertex), token] for vertex, token in rows]
        return {"matching": "epsilon", "epsilon": EPSILON, "key": ["id"]}, ["id", "score"], cells
    raise RungInputError("invalid_rung_spec", f"no reference conversion for {algorithm}")


def archive_reference(
    spec: LadderSpec, rung: Mapping[str, Any], input_root: Path
) -> tuple[dict[str, Any], str]:
    """The rung's reference, derived from the archive, and the SHA-256 naming its inputs.

    The digest covers the ``.properties`` file and every reference output
    used, by name and content, so the correctness record names exactly the
    bytes it was checked against.
    """
    graph = check_archive(spec, rung, input_root)
    label = _tables(spec, _ladder_entry(spec, rung["id"]))["node"]["label"]
    workload = read_json(spec.resolve(rung["workload"]))
    queries: dict[str, Any] = {}
    used = [input_root / f"{graph.name}.properties"]
    for variant in workload["variants"]:
        algorithm = variant["id"]
        path = input_root / f"{graph.name}-{OUTPUT_SUFFIX[algorithm]}"
        if not path.is_file():
            raise RungInputError("reference_invalid", f"the archive has no {path.name}")
        used.append(path)
        rule, columns, cells = convert_reference(
            algorithm, parse_vertex_values(path, graph.vertices), label
        )
        expected = {"columns": columns, "rows": cells}
        queries[algorithm] = {
            **rule,
            "bindings": {binding["id"]: expected for binding in variant["bindings"]},
        }
    digest = hashlib.sha256()
    for path in sorted(used):
        digest.update(
            path.name.encode("utf-8") + b"\0" + _file_sha256(path).encode("ascii") + b"\n"
        )
    reference = {
        "schema": REFERENCE_SCHEMA,
        "suite_id": spec.suite_id,
        "rung_id": rung["id"],
        "source": f"the {graph.name} archive's LDBC Graphalytics reference outputs",
        "queries": queries,
    }
    return reference, digest.hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()
