"""Independent FinBench Transaction read results over the committed query fixture.

This module never runs GraphForge. It reads the fixture graph and parameter
bindings (``fixtures/gdc/finbench-transaction-queries``) and evaluates each
runnable read procedurally, from the LDBC FinBench Transaction specification,
so the Cypher the Rust runner executes is checked against an answer derived
another way. ``expected.json`` in the fixture is this module's output; the
benchmark unittest regenerates it and fails if the committed file drifts.

Semantics shared with the Rust query definitions:

* Time windows are open: ``startTime < timestamp < endTime``.
* Amount thresholds are strict: ``amount > threshold``.
* Calculated floats are rounded half-up to 3 decimal places.
* Truncation (``truncationLimit``, ``TIMESTAMP_DESCENDING``): when a step
  expands from a vertex, only the ``truncationLimit`` newest edges of the
  expanded type and direction at that vertex are traversed. Truncation applies
  to the raw adjacency before the window and amount filters. Ties on
  ``timestamp`` are broken by the far endpoint's id, ascending; edges that
  share both are kept or dropped together.

Usage::

    python -m graphforge_bench.gdc_finbench_transaction_reference FIXTURE_DIR [--write]
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
import json
import math
from pathlib import Path
import sys
from typing import Any

EXPECTED_SCHEMA = "graphforge-gdc-finbench-query-expected/1"
TRUNCATION_ORDER = "TIMESTAMP_DESCENDING"


@dataclass(frozen=True)
class Edge:
    """One fixture edge; ``index`` keeps parallel edges distinct."""

    kind: str
    index: int
    src: int
    dst: int
    timestamp: int
    amount: float


class Graph:
    def __init__(self, document: dict[str, Any]) -> None:
        self.nodes: dict[int, dict[str, Any]] = {}
        self.labels: dict[int, str] = {}
        for label, table in document["nodes"].items():
            for row in table["rows"]:
                props = dict(zip(table["columns"], row, strict=True))
                node_id = props["id"]
                if node_id in self.nodes:
                    raise ValueError(f"duplicate node id {node_id}")
                self.nodes[node_id] = props
                self.labels[node_id] = label
        self.out: dict[str, dict[int, list[Edge]]] = defaultdict(lambda: defaultdict(list))
        self.into: dict[str, dict[int, list[Edge]]] = defaultdict(lambda: defaultdict(list))
        for kind, table in document["edges"].items():
            for index, row in enumerate(table["rows"]):
                props = dict(zip(table["columns"], row, strict=True))
                edge = Edge(
                    kind=kind,
                    index=index,
                    src=props["from"],
                    dst=props["to"],
                    timestamp=props.get("timestamp", 0),
                    amount=float(props.get("amount", 0.0)),
                )
                self.out[kind][edge.src].append(edge)
                self.into[kind][edge.dst].append(edge)

    def label(self, node_id: int) -> str | None:
        return self.labels.get(node_id)

    def prop(self, node_id: int, name: str) -> Any:
        return self.nodes[node_id][name]


def round3(value: float) -> float:
    """Half-up rounding to 3 decimals, matching Cypher ``round(x * 1000) / 1000``."""
    return math.floor(value * 1000 + 0.5) / 1000


def fmt_float(value: float) -> str:
    return f"{round3(value):.3f}"


def fmt_bool(value: bool) -> str:
    return "true" if value else "false"


def admitted(edges: Iterable[Edge], limit: int, far: Callable[[Edge], int]) -> list[Edge]:
    """Edges a truncated step traverses.

    The step keeps the ``limit`` newest edges, ties broken by far-endpoint id
    ascending; edges sharing both timestamp and far endpoint are kept or
    dropped together.
    """
    edges = list(edges)
    ordered = sorted(edges, key=lambda edge: (-edge.timestamp, far(edge)))
    kept = {(far(edge), edge.timestamp) for edge in ordered[:limit]}
    return [edge for edge in edges if (far(edge), edge.timestamp) in kept]


def in_window(edge: Edge, binding: dict[str, Any]) -> bool:
    return binding["startTime"] < edge.timestamp < binding["endTime"]


def _check_order(binding: dict[str, Any]) -> int:
    if binding["truncationOrder"] != TRUNCATION_ORDER:
        raise ValueError(f"unsupported truncationOrder {binding['truncationOrder']}")
    return int(binding["truncationLimit"])


def tcr1(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    limit = _check_order(b)
    found: set[tuple[int, int, int, str]] = set()

    def walk(vertex: int, last_ts: float, depth: int) -> None:
        for edge in admitted(graph.out["transfer"][vertex], limit, lambda e: e.dst):
            if not in_window(edge, b) or edge.timestamp <= last_ts:
                continue
            distance = depth + 1
            for sign in graph.into["signIn"][edge.dst]:
                if graph.prop(sign.src, "isBlocked") and in_window(sign, b):
                    found.add((edge.dst, distance, sign.src, graph.prop(sign.src, "type")))
            if distance < 3:
                walk(edge.dst, edge.timestamp, distance)

    walk(b["id"], -math.inf, 0)
    rows = sorted(found, key=lambda row: (row[1], row[0], row[2]))
    return [[str(o), str(d), str(m), t] for o, d, m, t in rows]


def tcr2(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    limit = _check_order(b)
    others: set[int] = set()

    def walk_upstream(vertex: int, next_ts: float, depth: int) -> None:
        # Walking against the transfer direction; timestamps must ascend from
        # the upstream account to the owned account, so each earlier hop is older.
        for edge in admitted(graph.into["transfer"][vertex], limit, lambda e: e.src):
            if not in_window(edge, b) or edge.timestamp >= next_ts:
                continue
            others.add(edge.src)
            if depth + 1 < 3:
                walk_upstream(edge.src, edge.timestamp, depth + 1)

    for own in graph.out["own"][b["id"]]:
        walk_upstream(own.dst, math.inf, 0)
    rows = []
    for other in others:
        loans = {deposit.src for deposit in graph.into["deposit"][other] if in_window(deposit, b)}
        if not loans:
            continue
        amount = sum(graph.prop(loan, "loanAmount") for loan in loans)
        balance = sum(graph.prop(loan, "balance") for loan in loans)
        rows.append((other, round3(amount), round3(balance)))
    rows.sort(key=lambda row: (-row[1], row[0]))
    return [[str(o), fmt_float(a), fmt_float(bal)] for o, a, bal in rows]


def tcr3(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    source, target = b["id1"], b["id2"]
    distance: dict[int, int] = {}
    frontier, depth = [source], 0
    while frontier:
        depth += 1
        following = []
        for vertex in frontier:
            for edge in graph.out["transfer"][vertex]:
                if in_window(edge, b) and edge.dst not in distance:
                    distance[edge.dst] = depth
                    following.append(edge.dst)
        frontier = following
    return [[str(distance.get(target, -1))]]


def tcr4(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    src, dst = b["id1"], b["id2"]
    if not any(e.dst == dst and in_window(e, b) for e in graph.out["transfer"][src]):
        return []
    rows = []
    others = {e.dst for e in graph.out["transfer"][dst] if in_window(e, b)}
    for other in others:
        edge2 = [e for e in graph.out["transfer"][other] if e.dst == src and in_window(e, b)]
        edge3 = [e for e in graph.out["transfer"][dst] if e.dst == other and in_window(e, b)]
        if not edge2 or not edge3:
            continue
        rows.append(
            (
                other,
                len(edge2),
                round3(sum(e.amount for e in edge2)),
                round3(max(e.amount for e in edge2)),
                len(edge3),
                round3(sum(e.amount for e in edge3)),
                round3(max(e.amount for e in edge3)),
            )
        )
    rows.sort(key=lambda row: (-row[2], -row[5], row[0]))
    return [
        [str(o), str(n2), fmt_float(s2), fmt_float(m2), str(n3), fmt_float(s3), fmt_float(m3)]
        for o, n2, s2, m2, n3, s3, m3 in rows
    ]


def tcr5(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    limit = _check_order(b)
    paths: set[tuple[int, ...]] = set()

    def walk(path: tuple[int, ...], last_ts: float) -> None:
        for edge in admitted(graph.out["transfer"][path[-1]], limit, lambda e: e.dst):
            if not in_window(edge, b) or edge.timestamp <= last_ts or edge.dst in path:
                continue
            extended = (*path, edge.dst)
            paths.add(extended)
            if len(extended) < 4:
                walk(extended, edge.timestamp)

    for own in graph.out["own"][b["id"]]:
        walk((own.dst,), -math.inf)
    ordered = sorted(paths, key=lambda path: (-len(path), path))
    return [["[" + ", ".join(str(node) for node in path) + "]"] for path in ordered]


def tcr6(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    limit = _check_order(b)
    card = b["id"]
    if graph.label(card) != "Account" or not str(graph.prop(card, "type")).endswith("card"):
        return []
    withdrawn: dict[int, float] = defaultdict(float)
    for edge in admitted(graph.into["withdraw"][card], limit, lambda e: e.src):
        if in_window(edge, b) and edge.amount > b["threshold2"]:
            withdrawn[edge.src] += edge.amount
    rows = []
    for mid, edge2_sum in withdrawn.items():
        edge1 = [
            e
            for e in admitted(graph.into["transfer"][mid], limit, lambda e: e.src)
            if in_window(e, b) and e.amount > b["threshold1"]
        ]
        if len(edge1) > 3:
            rows.append((mid, round3(sum(e.amount for e in edge1)), round3(edge2_sum)))
    rows.sort(key=lambda row: (-row[2], row[0]))
    return [[str(m), fmt_float(s1), fmt_float(s2)] for m, s1, s2 in rows]


def tcr7(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    limit = _check_order(b)
    mid = b["id"]
    incoming = [
        e
        for e in admitted(graph.into["transfer"][mid], limit, lambda e: e.src)
        if in_window(e, b) and e.amount > b["threshold"]
    ]
    outgoing = [
        e
        for e in admitted(graph.out["transfer"][mid], limit, lambda e: e.dst)
        if in_window(e, b) and e.amount > b["threshold"]
    ]
    ratio = (
        -1.0 if not outgoing else sum(e.amount for e in incoming) / sum(e.amount for e in outgoing)
    )
    return [
        [
            str(len({e.src for e in incoming})),
            str(len({e.dst for e in outgoing})),
            fmt_float(ratio),
        ]
    ]


def tcr8(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    limit = _check_order(b)
    loan = b["id"]

    def upstream(account: int) -> float:
        return sum(e.amount for e in graph.into["transfer"][account] if in_window(e, b))

    def qualifying(account: int) -> list[Edge]:
        candidates = graph.out["transfer"][account] + graph.out["withdraw"][account]
        floor = b["threshold"] * upstream(account)
        return [
            e
            for e in admitted(candidates, limit, lambda e: e.dst)
            if in_window(e, b) and e.amount > floor
        ]

    distance: dict[int, int] = {}
    last_edges: dict[int, set[tuple[int, int, float]]] = defaultdict(set)

    def walk(account: int, used: frozenset[Edge], hops: int) -> None:
        for edge in qualifying(account):
            if edge in used:
                continue
            reached = hops + 1
            distance[edge.dst] = min(distance.get(edge.dst, reached + 1), reached + 1)
            last_edges[edge.dst].add((edge.src, edge.timestamp, edge.amount))
            if reached < 3:
                walk(edge.dst, used | {edge}, reached)

    for deposit in graph.out["deposit"][loan]:
        if in_window(deposit, b):
            walk(deposit.dst, frozenset(), 0)
    loan_amount = graph.prop(loan, "loanAmount")
    rows = [
        (dst, round3(sum(amount for _, _, amount in last_edges[dst]) / loan_amount), distance[dst])
        for dst in distance
    ]
    rows.sort(key=lambda row: (-row[2], -row[1], row[0]))
    return [[str(d), fmt_float(r), str(h)] for d, r, h in rows]


def tcr9(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    limit = _check_order(b)
    account = b["id"]

    def kept(edges: Iterable[Edge]) -> list[Edge]:
        return [e for e in edges if in_window(e, b) and e.amount > b["threshold"]]

    edge1 = kept(graph.into["deposit"][account])
    edge2 = kept(graph.out["repay"][account])
    edge3 = kept(admitted(graph.into["transfer"][account], limit, lambda e: e.src))
    edge4 = kept(admitted(graph.out["transfer"][account], limit, lambda e: e.dst))
    sum1, sum2, sum3, sum4 = (
        sum(e.amount for e in edges) for edges in (edge1, edge2, edge3, edge4)
    )
    ratio_repay = -1.0 if not edge2 else sum1 / sum2
    ratio_deposit = -1.0 if not edge4 else sum1 / sum4
    ratio_transfer = -1.0 if not edge4 else sum3 / sum4
    return [[fmt_float(ratio_repay), fmt_float(ratio_deposit), fmt_float(ratio_transfer)]]


def tcr10(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    def companies(person: int) -> set[int]:
        return {
            e.dst
            for e in graph.out["invest"][person]
            if in_window(e, b) and graph.label(e.dst) == "Company"
        }

    left, right = companies(b["pid1"]), companies(b["pid2"])
    union = left | right
    similarity = 0.0 if not union else len(left & right) / len(union)
    return [[fmt_float(similarity)]]


def tcr11(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    limit = _check_order(b)
    reached: set[int] = set()
    frontier = [b["id"]]
    expanded: set[int] = set()
    while frontier:
        following = []
        for person in frontier:
            if person in expanded:
                continue
            expanded.add(person)
            for edge in admitted(graph.out["guarantee"][person], limit, lambda e: e.dst):
                if in_window(edge, b) and graph.label(edge.dst) == "Person":
                    reached.add(edge.dst)
                    following.append(edge.dst)
        frontier = following
    loans = {e.dst for person in reached for e in graph.out["apply"][person]}
    total = sum(graph.prop(loan, "loanAmount") for loan in loans)
    return [[fmt_float(total), str(len(loans))]]


def tcr12(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    limit = _check_order(b)
    totals: dict[int, float] = defaultdict(float)
    for own in graph.out["own"][b["id"]]:
        for edge in admitted(graph.out["transfer"][own.dst], limit, lambda e: e.dst):
            if not in_window(edge, b):
                continue
            owners = graph.into["own"][edge.dst]
            if any(graph.label(owner.src) == "Company" for owner in owners):
                totals[edge.dst] += edge.amount
    rows = sorted(
        ((acc, round3(total)) for acc, total in totals.items()), key=lambda r: (-r[1], r[0])
    )
    return [[str(acc), fmt_float(total)] for acc, total in rows]


def tsr1(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    account = b["id"]
    if graph.label(account) != "Account":
        return []
    return [
        [
            str(graph.prop(account, "createTime")),
            fmt_bool(graph.prop(account, "isBlocked")),
            graph.prop(account, "type"),
        ]
    ]


def tsr2(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    account = b["id"]
    outs = [e for e in graph.out["transfer"][account] if in_window(e, b)]
    ins = [e for e in graph.into["transfer"][account] if in_window(e, b)]

    def summary(edges: list[Edge]) -> list[str]:
        return [
            fmt_float(sum(e.amount for e in edges)),
            fmt_float(max((e.amount for e in edges), default=-1.0)),
            str(len(edges)),
        ]

    return [summary(outs) + summary(ins)]


def tsr3(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    ins = graph.into["transfer"][b["id"]]
    blocked = [
        e
        for e in ins
        if graph.prop(e.src, "isBlocked") and in_window(e, b) and e.amount > b["threshold"]
    ]
    ratio = -1.0 if not ins else len(blocked) / len(ins)
    return [[fmt_float(ratio)]]


def _grouped(edges: list[Edge], key: Callable[[Edge], int]) -> list[list[str]]:
    groups: dict[int, list[Edge]] = defaultdict(list)
    for edge in edges:
        groups[key(edge)].append(edge)
    rows = sorted(
        (
            (node, len(group), round3(sum(e.amount for e in group)))
            for node, group in groups.items()
        ),
        key=lambda row: (-row[2], row[0]),
    )
    return [[str(node), str(count), fmt_float(total)] for node, count, total in rows]


def tsr4(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    edges = [
        e for e in graph.out["transfer"][b["id"]] if in_window(e, b) and e.amount > b["threshold"]
    ]
    return _grouped(edges, lambda e: e.dst)


def tsr5(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    edges = [
        e for e in graph.into["transfer"][b["id"]] if in_window(e, b) and e.amount > b["threshold"]
    ]
    return _grouped(edges, lambda e: e.src)


def tsr6(graph: Graph, b: dict[str, Any]) -> list[list[str]]:
    account = b["id"]
    dsts = {
        out.dst
        for edge in graph.into["transfer"][account]
        if in_window(edge, b)
        for out in graph.out["transfer"][edge.src]
        if in_window(out, b) and out.dst != account and graph.prop(out.dst, "isBlocked")
    }
    return [[str(dst)] for dst in sorted(dsts)]


EVALUATORS: dict[str, Callable[[Graph, dict[str, Any]], list[list[str]]]] = {
    "TCR1": tcr1,
    "TCR2": tcr2,
    "TCR3": tcr3,
    "TCR4": tcr4,
    "TCR5": tcr5,
    "TCR6": tcr6,
    "TCR7": tcr7,
    "TCR8": tcr8,
    "TCR9": tcr9,
    "TCR10": tcr10,
    "TCR11": tcr11,
    "TCR12": tcr12,
    "TSR1": tsr1,
    "TSR2": tsr2,
    "TSR3": tsr3,
    "TSR4": tsr4,
    "TSR5": tsr5,
    "TSR6": tsr6,
}


def derive_expected(fixture: Path) -> dict[str, Any]:
    """Evaluate every bound read over the fixture graph, without GraphForge."""
    graph_document = json.loads((fixture / "graph.json").read_text(encoding="utf-8"))
    parameters = json.loads((fixture / "parameters.json").read_text(encoding="utf-8"))
    if parameters["dataset_id"] != graph_document["dataset_id"]:
        raise ValueError("parameters and graph name different datasets")
    graph = Graph(graph_document)
    results: dict[str, list[dict[str, Any]]] = {}
    for operation, bindings in parameters["bindings"].items():
        evaluate = EVALUATORS[operation]
        results[operation] = [
            {"binding": binding, "rows": evaluate(graph, binding)} for binding in bindings
        ]
    return {
        "schema": EXPECTED_SCHEMA,
        "dataset_id": graph_document["dataset_id"],
        "derivation": "graphforge_bench.gdc_finbench_transaction_reference",
        "results": results,
    }


def render(expected: dict[str, Any]) -> str:
    return json.dumps(expected, indent=2) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("fixture", type=Path)
    parser.add_argument("--write", action="store_true", help="rewrite expected.json")
    args = parser.parse_args(argv)
    rendered = render(derive_expected(args.fixture))
    target = args.fixture / "expected.json"
    if args.write:
        target.write_text(rendered, encoding="utf-8")
        return 0
    if not target.is_file() or target.read_text(encoding="utf-8") != rendered:
        print(f"{target} is stale; rerun with --write", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
