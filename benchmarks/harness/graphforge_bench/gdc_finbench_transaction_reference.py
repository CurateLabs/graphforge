"""Independent FinBench Transaction read results, from the specification.

This module never runs GraphForge. It evaluates each runnable read
procedurally, from the LDBC FinBench Transaction specification, so the Cypher
the Rust runner executes is checked against an answer derived another way. It
reads two kinds of input:

* the committed query fixture (``fixtures/gdc/finbench-transaction-queries``):
  ``expected.json`` in the fixture is this module's output, and the benchmark
  unittest regenerates it and fails if the committed file drifts;
* a published LDBC FinBench scale factor: the ``snapshot/`` CSV directory and
  the ``complex_<n>_param.csv`` read parameters. The output is the rung's
  ``graphforge-gdc-rung-reference/1`` document, cells in the query driver's
  form (Arrow display text), for the scorecard's correctness check. It is a
  spec-derived reference, not an LDBC implementation.

Semantics shared with the Rust query definitions:

* Time windows are open: ``startTime < timestamp < endTime``.
* Amount thresholds are strict: ``amount > threshold``.
* Calculated floats are rounded half-up to 3 decimal places, after an exactly
  rounded sum (``math.fsum``), so no answer depends on summation order.
* Truncation (``truncationLimit``, ``TIMESTAMP_DESCENDING``): when a step
  expands from a vertex, only the ``truncationLimit`` newest edges of the
  expanded type and direction at that vertex are traversed. Truncation applies
  to the raw adjacency before the window and amount filters. Ties on
  ``timestamp`` are broken by the far endpoint's id, ascending; edges that
  share both are kept or dropped together.
* Vertices are identified by (label, id): LDBC ids repeat across labels, and
  every read names the label of each vertex it matches, as the Cypher does.

Usage::

    python -m graphforge_bench.gdc_finbench_transaction_reference fixture FIXTURE_DIR [--write]
    python -m graphforge_bench.gdc_finbench_transaction_reference ldbc \\
        --snapshot SNAPSHOT_DIR --params PARAMS_DIR --rung sf1 --output FILE
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
import json
import math
import os
from pathlib import Path
import sys
from typing import Any

EXPECTED_SCHEMA = "graphforge-gdc-finbench-query-expected/1"
REFERENCE_SCHEMA = "graphforge-gdc-rung-reference/1"
SUITE_ID = "finbench-transaction"
TRUNCATION_ORDER = "TIMESTAMP_DESCENDING"
DERIVATION = "graphforge_bench.gdc_finbench_transaction_reference"
REFERENCE_SOURCE = (
    "spec-derived reference (graphforge_bench.gdc_finbench_transaction_reference, "
    "LDBC FinBench Transaction v0.1.0); not an LDBC implementation"
)

Row = tuple[Any, ...]

# Result columns per read, as the Rust query catalog (`queries.rs`) names them.
# Kinds: int (Int64), float3 (Float64 rounded to 3 decimals), bool, text, int_list.
COLUMNS: dict[str, tuple[tuple[str, str], ...]] = {
    "TCR1": (
        ("otherId", "int"),
        ("accountDistance", "int"),
        ("mediumId", "int"),
        ("mediumType", "text"),
    ),
    "TCR2": (("otherId", "int"), ("sumLoanAmount", "float3"), ("sumLoanBalance", "float3")),
    "TCR3": (("shortestPathLength", "int"),),
    "TCR4": (
        ("otherId", "int"),
        ("numEdge2", "int"),
        ("sumEdge2Amount", "float3"),
        ("maxEdge2Amount", "float3"),
        ("numEdge3", "int"),
        ("sumEdge3Amount", "float3"),
        ("maxEdge3Amount", "float3"),
    ),
    "TCR5": (("path", "int_list"),),
    "TCR6": (("midId", "int"), ("sumEdge1Amount", "float3"), ("sumEdge2Amount", "float3")),
    "TCR7": (("numSrc", "int"), ("numDst", "int"), ("inOutRatio", "float3")),
    "TCR8": (("dstId", "int"), ("ratio", "float3"), ("minDistanceFromLoan", "int")),
    "TCR9": (("ratioRepay", "float3"), ("ratioDeposit", "float3"), ("ratioTransfer", "float3")),
    "TCR10": (("jaccardSimilarity", "float3"),),
    "TCR11": (("sumLoanAmount", "float3"), ("numLoans", "int")),
    "TCR12": (("compAccountId", "int"), ("sumEdge2Amount", "float3")),
    "TSR1": (("createTime", "int"), ("isBlocked", "bool"), ("type", "text")),
    "TSR2": (
        ("sumEdge1Amount", "float3"),
        ("maxEdge1Amount", "float3"),
        ("numEdge1", "int"),
        ("sumEdge2Amount", "float3"),
        ("maxEdge2Amount", "float3"),
        ("numEdge2", "int"),
    ),
    "TSR3": (("blockRatio", "float3"),),
    "TSR4": (("dstId", "int"), ("numEdges", "int"), ("sumAmount", "float3")),
    "TSR5": (("srcId", "int"), ("numEdges", "int"), ("sumAmount", "float3")),
    "TSR6": (("dstId", "int"),),
}

_LIMITED = ("startTime", "endTime", "truncationLimit", "truncationOrder")
_WINDOW = ("startTime", "endTime")
# Column order of the published `complex_<n>_param.csv` files, which carry no
# header (the LDBC FinBench driver's parameter order; names as in `queries.rs`).
LDBC_PARAMETERS: dict[str, tuple[str, ...]] = {
    "TCR1": ("id", *_LIMITED),
    "TCR2": ("id", *_LIMITED),
    "TCR3": ("id1", "id2", *_WINDOW),
    "TCR4": ("id1", "id2", *_WINDOW),
    "TCR5": ("id", *_LIMITED),
    "TCR6": ("id", "threshold1", "threshold2", *_LIMITED),
    "TCR7": ("id", "threshold", *_LIMITED),
    "TCR8": ("id", "threshold", *_LIMITED),
    "TCR9": ("id", "threshold", *_LIMITED),
    "TCR10": ("pid1", "pid2", *_WINDOW),
    "TCR11": ("id", *_LIMITED),
    "TCR12": ("id", *_LIMITED),
}
_FLOAT_PARAMETERS = frozenset({"threshold", "threshold1", "threshold2"})
_TEXT_PARAMETERS = frozenset({"truncationOrder"})
# The published files start with a literal `...` line in place of a header
# (all but complex_3); it is not a binding.
_PARAMETER_PLACEHOLDER = "..."


@dataclass(frozen=True, slots=True)
class Edge:
    """One edge; ``index`` keeps parallel edges of one source table distinct."""

    kind: str
    index: int
    src_label: str
    src: int
    dst_label: str
    dst: int
    timestamp: int
    amount: float


_NO_EDGES: tuple[Edge, ...] = ()


class Graph:
    """Label-qualified vertices with typed, directed adjacency."""

    def __init__(self) -> None:
        self.nodes: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
        self._out: dict[tuple[str, str], dict[int, list[Edge]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._in: dict[tuple[str, str], dict[int, list[Edge]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._admitted: dict[tuple[Any, ...], list[Edge]] = {}

    def add_node(self, label: str, node_id: int, props: dict[str, Any]) -> None:
        table = self.nodes[label]
        if node_id in table:
            raise ValueError(f"duplicate {label} id {node_id}")
        table[node_id] = props

    def add_edge(self, edge: Edge) -> None:
        self._out[(edge.kind, edge.src_label)][edge.src].append(edge)
        self._in[(edge.kind, edge.dst_label)][edge.dst].append(edge)

    @classmethod
    def from_fixture(cls, document: dict[str, Any]) -> Graph:
        graph = cls()
        labels: dict[int, str] = {}
        for label, table in document["nodes"].items():
            for row in table["rows"]:
                props = dict(zip(table["columns"], row, strict=True))
                node_id = props["id"]
                if node_id in labels:
                    raise ValueError(f"duplicate node id {node_id}")
                labels[node_id] = label
                graph.add_node(label, node_id, props)
        for kind, table in document["edges"].items():
            for index, row in enumerate(table["rows"]):
                props = dict(zip(table["columns"], row, strict=True))
                graph.add_edge(
                    Edge(
                        kind=kind,
                        index=index,
                        src_label=labels[props["from"]],
                        src=props["from"],
                        dst_label=labels[props["to"]],
                        dst=props["to"],
                        timestamp=props.get("timestamp", 0),
                        amount=float(props.get("amount", 0.0)),
                    )
                )
        return graph

    def has(self, label: str, node_id: int) -> bool:
        return node_id in self.nodes.get(label, {})

    def prop(self, label: str, node_id: int, name: str) -> Any:
        return self.nodes[label][node_id][name]

    def out(self, kind: str, label: str, node_id: int) -> Sequence[Edge]:
        table = self._out.get((kind, label))
        return table.get(node_id, _NO_EDGES) if table is not None else _NO_EDGES

    def into(self, kind: str, label: str, node_id: int) -> Sequence[Edge]:
        table = self._in.get((kind, label))
        return table.get(node_id, _NO_EDGES) if table is not None else _NO_EDGES

    def admitted_out(
        self, kinds: tuple[str, ...], label: str, node_id: int, limit: int
    ) -> list[Edge]:
        """The edges a truncated outgoing step over ``kinds`` traverses (cached)."""
        key = ("out", kinds, label, node_id, limit)
        cached = self._admitted.get(key)
        if cached is None:
            edges = [e for kind in kinds for e in self.out(kind, label, node_id)]
            cached = self._admitted[key] = admitted(edges, limit, lambda e: e.dst)
        return cached

    def admitted_in(self, kind: str, label: str, node_id: int, limit: int) -> list[Edge]:
        """The edges a truncated incoming step traverses (cached)."""
        key = ("in", kind, label, node_id, limit)
        cached = self._admitted.get(key)
        if cached is None:
            edges = self.into(kind, label, node_id)
            cached = self._admitted[key] = admitted(edges, limit, lambda e: e.src)
        return cached


def round3(value: float) -> float:
    """Half-up rounding to 3 decimals, matching Cypher ``round(x * 1000) / 1000``."""
    return math.floor(value * 1000 + 0.5) / 1000


def total(values: Iterable[float]) -> float:
    """The exactly rounded sum, independent of the order of ``values``."""
    return math.fsum(values)


def admitted(edges: Iterable[Edge], limit: int, far: Callable[[Edge], int]) -> list[Edge]:
    """Edges a truncated step traverses.

    The step keeps the ``limit`` newest edges, ties broken by far-endpoint id
    ascending; edges sharing both timestamp and far endpoint are kept or
    dropped together.
    """
    edges = list(edges)
    if len(edges) <= limit:
        return edges
    ordered = sorted(edges, key=lambda edge: (-edge.timestamp, far(edge)))
    kept = {(far(edge), edge.timestamp) for edge in ordered[:limit]}
    return [edge for edge in edges if (far(edge), edge.timestamp) in kept]


def in_window(edge: Edge, binding: dict[str, Any]) -> bool:
    return binding["startTime"] < edge.timestamp < binding["endTime"]


def _check_order(binding: dict[str, Any]) -> int:
    if binding["truncationOrder"] != TRUNCATION_ORDER:
        raise ValueError(f"unsupported truncationOrder {binding['truncationOrder']}")
    return int(binding["truncationLimit"])


ACCOUNT, COMPANY, LOAN, MEDIUM, PERSON = "Account", "Company", "Loan", "Medium", "Person"
TRANSFER = ("transfer",)


def tcr1(graph: Graph, b: dict[str, Any]) -> list[Row]:
    limit = _check_order(b)
    found: set[tuple[int, int, int, str]] = set()

    def walk(vertex: int, last_ts: float, depth: int) -> None:
        for edge in graph.admitted_out(TRANSFER, ACCOUNT, vertex, limit):
            if edge.dst_label != ACCOUNT or not in_window(edge, b) or edge.timestamp <= last_ts:
                continue
            distance = depth + 1
            for sign in graph.into("signIn", ACCOUNT, edge.dst):
                if (
                    sign.src_label == MEDIUM
                    and graph.prop(MEDIUM, sign.src, "isBlocked")
                    and in_window(sign, b)
                ):
                    found.add((edge.dst, distance, sign.src, graph.prop(MEDIUM, sign.src, "type")))
            if distance < 3:
                walk(edge.dst, edge.timestamp, distance)

    if graph.has(ACCOUNT, b["id"]):
        walk(b["id"], -math.inf, 0)
    return sorted(found, key=lambda row: (row[1], row[0], row[2]))


def _owned_accounts(graph: Graph, person: int) -> list[int]:
    return [e.dst for e in graph.out("own", PERSON, person) if e.dst_label == ACCOUNT]


def tcr2(graph: Graph, b: dict[str, Any]) -> list[Row]:
    limit = _check_order(b)
    others: set[int] = set()

    def walk_upstream(vertex: int, next_ts: float, depth: int) -> None:
        # Walking against the transfer direction; timestamps must ascend from
        # the upstream account to the owned account, so each earlier hop is older.
        for edge in graph.admitted_in("transfer", ACCOUNT, vertex, limit):
            if edge.src_label != ACCOUNT or not in_window(edge, b) or edge.timestamp >= next_ts:
                continue
            others.add(edge.src)
            if depth + 1 < 3:
                walk_upstream(edge.src, edge.timestamp, depth + 1)

    for account in _owned_accounts(graph, b["id"]):
        walk_upstream(account, math.inf, 0)
    rows = []
    for other in others:
        loans = {
            deposit.src
            for deposit in graph.into("deposit", ACCOUNT, other)
            if deposit.src_label == LOAN and in_window(deposit, b)
        }
        if not loans:
            continue
        amount = total(graph.prop(LOAN, loan, "loanAmount") for loan in loans)
        balance = total(graph.prop(LOAN, loan, "balance") for loan in loans)
        rows.append((other, round3(amount), round3(balance)))
    rows.sort(key=lambda row: (-row[1], row[0]))
    return rows


def _window_transfers(graph: Graph, b: dict[str, Any], direction: str, vertex: int) -> list[int]:
    """Far accounts of the in-window transfers at ``vertex`` (``out`` or ``in``)."""
    if direction == "out":
        return [
            e.dst
            for e in graph.out("transfer", ACCOUNT, vertex)
            if e.dst_label == ACCOUNT and in_window(e, b)
        ]
    return [
        e.src
        for e in graph.into("transfer", ACCOUNT, vertex)
        if e.src_label == ACCOUNT and in_window(e, b)
    ]


def shortest_transfer_path_forward(graph: Graph, b: dict[str, Any]) -> int:
    """Breadth-first from ``id1``: the first level that reaches ``id2``, or -1.

    The source is not pre-marked, so when ``id1 == id2`` a cycle back to it counts.
    """
    source, target = b["id1"], b["id2"]
    seen: set[int] = set()
    frontier, depth = [source], 0
    while frontier:
        depth += 1
        following = []
        for vertex in frontier:
            for far in _window_transfers(graph, b, "out", vertex):
                if far in seen:
                    continue
                if far == target:
                    return depth
                seen.add(far)
                following.append(far)
        frontier = following
    return -1


def shortest_transfer_path(graph: Graph, b: dict[str, Any]) -> int:
    """The same length as ``shortest_transfer_path_forward``, searched from both ends.

    Each round expands one whole level of the side with the smaller frontier.
    The first round that discovers a vertex the other side has reached yields
    the shortest length: the minimum, over those vertices, of the two distances.
    """
    source, target = b["id1"], b["id2"]
    if source == target:
        return shortest_transfer_path_forward(graph, b)
    distance = {"out": {source: 0}, "in": {target: 0}}
    frontier = {"out": [source], "in": [target]}
    while frontier["out"] and frontier["in"]:
        side = "out" if len(frontier["out"]) <= len(frontier["in"]) else "in"
        other = "in" if side == "out" else "out"
        mine, theirs = distance[side], distance[other]
        following: list[int] = []
        best = -1
        for vertex in frontier[side]:
            depth = mine[vertex] + 1
            for far in _window_transfers(graph, b, side, vertex):
                if far in mine:
                    continue
                mine[far] = depth
                following.append(far)
                if far in theirs and (best < 0 or depth + theirs[far] < best):
                    best = depth + theirs[far]
        if best >= 0:
            return best
        frontier[side] = following
    return -1


def tcr3(graph: Graph, b: dict[str, Any]) -> list[Row]:
    # An aggregate without a grouping key: one row, -1 when there is no path.
    if not (graph.has(ACCOUNT, b["id1"]) and graph.has(ACCOUNT, b["id2"])):
        return [(-1,)]
    return [(shortest_transfer_path(graph, b),)]


def tcr4(graph: Graph, b: dict[str, Any]) -> list[Row]:
    src, dst = b["id1"], b["id2"]

    def transfers(frm: int, to: int) -> list[Edge]:
        return [
            e
            for e in graph.out("transfer", ACCOUNT, frm)
            if e.dst == to and e.dst_label == ACCOUNT and in_window(e, b)
        ]

    if not (graph.has(ACCOUNT, src) and graph.has(ACCOUNT, dst)) or not transfers(src, dst):
        return []
    rows = []
    others = {
        e.dst
        for e in graph.out("transfer", ACCOUNT, dst)
        if e.dst_label == ACCOUNT and in_window(e, b)
    }
    for other in others:
        edge2 = transfers(other, src)
        edge3 = transfers(dst, other)
        if not edge2 or not edge3:
            continue
        rows.append(
            (
                other,
                len(edge2),
                round3(total(e.amount for e in edge2)),
                round3(max(e.amount for e in edge2)),
                len(edge3),
                round3(total(e.amount for e in edge3)),
                round3(max(e.amount for e in edge3)),
            )
        )
    rows.sort(key=lambda row: (-row[2], -row[5], row[0]))
    return rows


def tcr5(graph: Graph, b: dict[str, Any]) -> list[Row]:
    limit = _check_order(b)
    paths: set[tuple[int, ...]] = set()

    def walk(path: tuple[int, ...], last_ts: float) -> None:
        for edge in graph.admitted_out(TRANSFER, ACCOUNT, path[-1], limit):
            if (
                edge.dst_label != ACCOUNT
                or not in_window(edge, b)
                or edge.timestamp <= last_ts
                or edge.dst in path
            ):
                continue
            extended = (*path, edge.dst)
            paths.add(extended)
            if len(extended) < 4:
                walk(extended, edge.timestamp)

    for account in _owned_accounts(graph, b["id"]):
        walk((account,), -math.inf)
    ordered = sorted(paths, key=lambda path: (-len(path), path))
    return [(path,) for path in ordered]


def tcr6(graph: Graph, b: dict[str, Any]) -> list[Row]:
    limit = _check_order(b)
    card = b["id"]
    if not graph.has(ACCOUNT, card) or not str(graph.prop(ACCOUNT, card, "type")).endswith("card"):
        return []
    withdrawn: dict[int, list[float]] = defaultdict(list)
    for edge in graph.admitted_in("withdraw", ACCOUNT, card, limit):
        if edge.src_label == ACCOUNT and in_window(edge, b) and edge.amount > b["threshold2"]:
            withdrawn[edge.src].append(edge.amount)
    rows = []
    for mid, edge2 in withdrawn.items():
        edge1 = [
            e
            for e in graph.admitted_in("transfer", ACCOUNT, mid, limit)
            if e.src_label == ACCOUNT and in_window(e, b) and e.amount > b["threshold1"]
        ]
        if len(edge1) > 3:
            rows.append((mid, round3(total(e.amount for e in edge1)), round3(total(edge2))))
    rows.sort(key=lambda row: (-row[2], row[0]))
    return rows


def tcr7(graph: Graph, b: dict[str, Any]) -> list[Row]:
    limit = _check_order(b)
    mid = b["id"]
    if not graph.has(ACCOUNT, mid):
        return []
    incoming = [
        e
        for e in graph.admitted_in("transfer", ACCOUNT, mid, limit)
        if e.src_label == ACCOUNT and in_window(e, b) and e.amount > b["threshold"]
    ]
    outgoing = [
        e
        for e in graph.admitted_out(TRANSFER, ACCOUNT, mid, limit)
        if e.dst_label == ACCOUNT and in_window(e, b) and e.amount > b["threshold"]
    ]
    ratio = (
        -1.0
        if not outgoing
        else round3(total(e.amount for e in incoming) / total(e.amount for e in outgoing))
    )
    return [(len({e.src for e in incoming}), len({e.dst for e in outgoing}), ratio)]


def tcr8(graph: Graph, b: dict[str, Any]) -> list[Row]:
    limit = _check_order(b)
    loan = b["id"]
    if not graph.has(LOAN, loan):
        return []
    qualifying_cache: dict[int, list[Edge]] = {}

    def upstream(account: int) -> float:
        return total(
            e.amount
            for e in graph.into("transfer", ACCOUNT, account)
            if e.src_label == ACCOUNT and in_window(e, b)
        )

    def qualifying(account: int) -> list[Edge]:
        cached = qualifying_cache.get(account)
        if cached is None:
            floor = b["threshold"] * upstream(account)
            cached = qualifying_cache[account] = [
                e
                for e in graph.admitted_out(("transfer", "withdraw"), ACCOUNT, account, limit)
                if e.dst_label == ACCOUNT and in_window(e, b) and e.amount > floor
            ]
        return cached

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

    for deposit in graph.out("deposit", LOAN, loan):
        if deposit.dst_label == ACCOUNT and in_window(deposit, b):
            walk(deposit.dst, frozenset(), 0)
    loan_amount = graph.prop(LOAN, loan, "loanAmount")
    rows = [
        (dst, round3(total(amount for _, _, amount in last_edges[dst]) / loan_amount), hops)
        for dst, hops in distance.items()
    ]
    rows.sort(key=lambda row: (-row[2], -row[1], row[0]))
    return rows


def tcr9(graph: Graph, b: dict[str, Any]) -> list[Row]:
    limit = _check_order(b)
    account = b["id"]
    if not graph.has(ACCOUNT, account):
        return []

    def kept(edges: Iterable[Edge], label: str, far: Callable[[Edge], str]) -> list[Edge]:
        return [
            e for e in edges if far(e) == label and in_window(e, b) and e.amount > b["threshold"]
        ]

    edge1 = kept(graph.into("deposit", ACCOUNT, account), LOAN, lambda e: e.src_label)
    edge2 = kept(graph.out("repay", ACCOUNT, account), LOAN, lambda e: e.dst_label)
    edge3 = kept(
        graph.admitted_in("transfer", ACCOUNT, account, limit), ACCOUNT, lambda e: e.src_label
    )
    edge4 = kept(
        graph.admitted_out(TRANSFER, ACCOUNT, account, limit), ACCOUNT, lambda e: e.dst_label
    )
    sum1, sum2, sum3, sum4 = (
        total(e.amount for e in edges) for edges in (edge1, edge2, edge3, edge4)
    )
    ratio_repay = -1.0 if not edge2 else round3(sum1 / sum2)
    ratio_deposit = -1.0 if not edge4 else round3(sum1 / sum4)
    ratio_transfer = -1.0 if not edge4 else round3(sum3 / sum4)
    return [(ratio_repay, ratio_deposit, ratio_transfer)]


def tcr10(graph: Graph, b: dict[str, Any]) -> list[Row]:
    if not (graph.has(PERSON, b["pid1"]) and graph.has(PERSON, b["pid2"])):
        return []

    def companies(person: int) -> set[int]:
        return {
            e.dst
            for e in graph.out("invest", PERSON, person)
            if e.dst_label == COMPANY and in_window(e, b)
        }

    left, right = companies(b["pid1"]), companies(b["pid2"])
    union = left | right
    similarity = 0.0 if not union else round3(len(left & right) / len(union))
    return [(similarity,)]


def tcr11(graph: Graph, b: dict[str, Any]) -> list[Row]:
    # Aggregates without a grouping key: one row even when nothing is reached.
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
            for edge in graph.admitted_out(("guarantee",), PERSON, person, limit):
                if edge.dst_label == PERSON and in_window(edge, b):
                    reached.add(edge.dst)
                    following.append(edge.dst)
        frontier = following
    loans = {
        e.dst
        for person in reached
        for e in graph.out("apply", PERSON, person)
        if e.dst_label == LOAN
    }
    amount = total(graph.prop(LOAN, loan, "loanAmount") for loan in loans)
    return [(round3(amount), len(loans))]


def tcr12(graph: Graph, b: dict[str, Any]) -> list[Row]:
    limit = _check_order(b)
    amounts: dict[int, list[float]] = defaultdict(list)
    for account in _owned_accounts(graph, b["id"]):
        for edge in graph.admitted_out(TRANSFER, ACCOUNT, account, limit):
            if edge.dst_label != ACCOUNT or not in_window(edge, b):
                continue
            owners = graph.into("own", ACCOUNT, edge.dst)
            if any(owner.src_label == COMPANY for owner in owners):
                amounts[edge.dst].append(edge.amount)
    rows = [(acc, round3(total(values))) for acc, values in amounts.items()]
    rows.sort(key=lambda r: (-r[1], r[0]))
    return rows


def tsr1(graph: Graph, b: dict[str, Any]) -> list[Row]:
    account = b["id"]
    if not graph.has(ACCOUNT, account):
        return []
    return [
        (
            graph.prop(ACCOUNT, account, "createTime"),
            bool(graph.prop(ACCOUNT, account, "isBlocked")),
            graph.prop(ACCOUNT, account, "type"),
        )
    ]


def tsr2(graph: Graph, b: dict[str, Any]) -> list[Row]:
    account = b["id"]
    if not graph.has(ACCOUNT, account):
        return []
    outs = [e for e in graph.out("transfer", ACCOUNT, account) if in_window(e, b)]
    ins = [e for e in graph.into("transfer", ACCOUNT, account) if in_window(e, b)]

    def summary(edges: list[Edge]) -> tuple[float, float, int]:
        return (
            round3(total(e.amount for e in edges)),
            round3(max((e.amount for e in edges), default=-1.0)),
            len(edges),
        )

    return [summary(outs) + summary(ins)]


def tsr3(graph: Graph, b: dict[str, Any]) -> list[Row]:
    if not graph.has(ACCOUNT, b["id"]):
        return []
    ins = graph.into("transfer", ACCOUNT, b["id"])
    blocked = [
        e
        for e in ins
        if graph.prop(ACCOUNT, e.src, "isBlocked") and in_window(e, b) and e.amount > b["threshold"]
    ]
    ratio = -1.0 if not ins else round3(len(blocked) / len(ins))
    return [(ratio,)]


def _grouped(edges: list[Edge], key: Callable[[Edge], int]) -> list[Row]:
    groups: dict[int, list[Edge]] = defaultdict(list)
    for edge in edges:
        groups[key(edge)].append(edge)
    rows = sorted(
        (
            (node, len(group), round3(total(e.amount for e in group)))
            for node, group in groups.items()
        ),
        key=lambda row: (-row[2], row[0]),
    )
    return rows


def tsr4(graph: Graph, b: dict[str, Any]) -> list[Row]:
    edges = [
        e
        for e in graph.out("transfer", ACCOUNT, b["id"])
        if in_window(e, b) and e.amount > b["threshold"]
    ]
    return _grouped(edges, lambda e: e.dst)


def tsr5(graph: Graph, b: dict[str, Any]) -> list[Row]:
    edges = [
        e
        for e in graph.into("transfer", ACCOUNT, b["id"])
        if in_window(e, b) and e.amount > b["threshold"]
    ]
    return _grouped(edges, lambda e: e.src)


def tsr6(graph: Graph, b: dict[str, Any]) -> list[Row]:
    account = b["id"]
    dsts = {
        out.dst
        for edge in graph.into("transfer", ACCOUNT, account)
        if in_window(edge, b)
        for out in graph.out("transfer", ACCOUNT, edge.src)
        if in_window(out, b) and out.dst != account and graph.prop(ACCOUNT, out.dst, "isBlocked")
    }
    return [(dst,) for dst in sorted(dsts)]


EVALUATORS: dict[str, Callable[[Graph, dict[str, Any]], list[Row]]] = {
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


def arrow_float(value: float) -> str:
    """Arrow's display text for a Float64: ``ryu`` shortest round-trip layout.

    arrow-cast formats Float64 cells with ``ryu::Buffer::format``. The digits
    are the shortest that round-trip, which ``repr`` also yields; the layout
    follows ryu: plain decimal while the decimal exponent is in (-5, 16],
    otherwise ``<d>[.<ddd>]e<exp>``.
    """
    value = float(value)
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    sign = "-" if math.copysign(1.0, value) < 0 else ""
    if value == 0:
        return f"{sign}0.0"
    _, digit_tuple, decimal_exponent = Decimal(repr(abs(value))).as_tuple()
    digits = "".join(map(str, digit_tuple))
    exponent = int(decimal_exponent)
    stripped = digits.rstrip("0")
    exponent += len(digits) - len(stripped)
    digits = stripped
    length = len(digits)
    point = length + exponent  # 10^(point-1) <= value < 10^point
    if exponent >= 0 and point <= 16:
        text = digits + "0" * exponent + ".0"
    elif 0 < point <= 16:
        text = digits[:point] + "." + digits[point:]
    elif -5 < point <= 0:
        text = "0." + "0" * -point + digits
    elif length == 1:
        text = f"{digits}e{point - 1}"
    else:
        text = f"{digits[0]}.{digits[1:]}e{point - 1}"
    return sign + text


def _fixture_cell(kind: str, value: Any) -> str:
    if kind == "float3":
        return f"{round3(value):.3f}"
    return _common_cell(kind, value)


def _arrow_cell(kind: str, value: Any) -> str:
    if kind == "float3":
        return arrow_float(round3(value))
    return _common_cell(kind, value)


def _common_cell(kind: str, value: Any) -> str:
    if kind == "int":
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"int cell holds {value!r}")
        return str(value)
    if kind == "bool":
        return "true" if value else "false"
    if kind == "text":
        return str(value)
    if kind == "int_list":
        return "[" + ", ".join(str(node) for node in value) + "]"
    raise ValueError(f"unknown column kind {kind}")


def render_rows(
    operation: str, rows: list[Row], cell: Callable[[str, Any], str]
) -> list[list[str]]:
    kinds = [kind for _, kind in COLUMNS[operation]]
    rendered = []
    for row in rows:
        if len(row) != len(kinds):
            raise ValueError(f"{operation} row {row!r} does not have {len(kinds)} columns")
        rendered.append([cell(kind, value) for kind, value in zip(kinds, row, strict=True)])
    return rendered


def derive_expected(fixture: Path) -> dict[str, Any]:
    """Evaluate every bound read over the fixture graph, without GraphForge."""
    graph_document = json.loads((fixture / "graph.json").read_text(encoding="utf-8"))
    parameters = json.loads((fixture / "parameters.json").read_text(encoding="utf-8"))
    if parameters["dataset_id"] != graph_document["dataset_id"]:
        raise ValueError("parameters and graph name different datasets")
    graph = Graph.from_fixture(graph_document)
    results: dict[str, list[dict[str, Any]]] = {}
    for operation, bindings in parameters["bindings"].items():
        evaluate = EVALUATORS[operation]
        results[operation] = [
            {
                "binding": binding,
                "rows": render_rows(operation, evaluate(graph, binding), _fixture_cell),
            }
            for binding in bindings
        ]
    return {
        "schema": EXPECTED_SCHEMA,
        "dataset_id": graph_document["dataset_id"],
        "derivation": DERIVATION,
        "results": results,
    }


def render(expected: dict[str, Any]) -> str:
    return json.dumps(expected, indent=2) + "\n"


# --- Published LDBC FinBench scale factors -------------------------------------

# Edge files: (file stem, kind, source label, source column, target label,
# target column, amount column or None). Kinds match the load mapping's
# relationship types (`profiles/gdc/finbench-transaction-load-mapping.json`).
LDBC_EDGE_FILES: tuple[tuple[str, str, str, str, str, str, str | None], ...] = (
    ("AccountTransferAccount", "transfer", ACCOUNT, "fromId", ACCOUNT, "toId", "amount"),
    ("AccountWithdrawAccount", "withdraw", ACCOUNT, "fromId", ACCOUNT, "toId", "amount"),
    ("AccountRepayLoan", "repay", ACCOUNT, "accountId", LOAN, "loanId", "amount"),
    ("LoanDepositAccount", "deposit", LOAN, "loanId", ACCOUNT, "accountId", "amount"),
    ("MediumSignInAccount", "signIn", MEDIUM, "mediumId", ACCOUNT, "accountId", None),
    ("PersonOwnAccount", "own", PERSON, "personId", ACCOUNT, "accountId", None),
    ("CompanyOwnAccount", "own", COMPANY, "companyId", ACCOUNT, "accountId", None),
    ("PersonApplyLoan", "apply", PERSON, "personId", LOAN, "loanId", None),
    ("CompanyApplyLoan", "apply", COMPANY, "companyId", LOAN, "loanId", None),
    ("PersonInvestCompany", "invest", PERSON, "investorId", COMPANY, "companyId", None),
    ("CompanyInvestCompany", "invest", COMPANY, "investorId", COMPANY, "companyId", None),
    ("PersonGuaranteePerson", "guarantee", PERSON, "fromId", PERSON, "toId", None),
    ("CompanyGuaranteeCompany", "guarantee", COMPANY, "fromId", COMPANY, "toId", None),
)


def ldbc_bool(text: str) -> bool:
    if text == "true":
        return True
    if text == "false":
        return False
    raise ValueError(f"not a boolean: {text!r}")


_EPOCH = datetime(1970, 1, 1)
_MILLISECOND = timedelta(milliseconds=1)


def ldbc_epoch_millis(text: str) -> int:
    """``YYYY-MM-DD HH:MM:SS[.f{1,3}]``, naive UTC, as epoch milliseconds.

    The published CSV writes millisecond timestamps with trailing fraction
    zeros dropped; read parameters are epoch milliseconds.
    """
    fraction = text[20:]
    if (
        len(text) < 19
        or text[10] != " "
        or (len(text) > 19 and (text[19] != "." or not 1 <= len(fraction) <= 3))
        or (fraction and not fraction.isdigit())
    ):
        raise ValueError(f"not an LDBC datetime: {text!r}")
    moment = datetime.fromisoformat(text)
    if moment.tzinfo is not None:
        raise ValueError(f"not a naive LDBC datetime: {text!r}")
    return (moment - _EPOCH) // _MILLISECOND


# Vertex files: label -> (file stem, id column, {property: (column, parser)}).
_NODE_FILES: dict[str, tuple[str, str, dict[str, tuple[str, Callable[[str], Any]]]]] = {
    ACCOUNT: (
        "Account",
        "accountId",
        {
            "createTime": ("createTime", ldbc_epoch_millis),
            "isBlocked": ("isBlocked", ldbc_bool),
            "type": ("accoutType", str),
        },
    ),
    COMPANY: ("Company", "companyId", {"isBlocked": ("isBlocked", ldbc_bool)}),
    LOAN: (
        "Loan",
        "loanId",
        {"loanAmount": ("loanAmount", float), "balance": ("balance", float)},
    ),
    MEDIUM: (
        "Medium",
        "mediumId",
        {"type": ("mediumType", str), "isBlocked": ("isBlocked", ldbc_bool)},
    ),
    PERSON: ("Person", "personId", {"isBlocked": ("isBlocked", ldbc_bool)}),
}


def _csv_rows(path: Path, columns: Sequence[str]) -> Iterable[list[str]]:
    """The named leading-or-not columns of a pipe-delimited LDBC CSV file.

    Splits only up to the last needed column, so free text after it (transfer
    comments) never shifts a field.
    """
    with path.open(encoding="utf-8", newline="") as handle:
        header = handle.readline().rstrip("\r\n").split("|")
        try:
            indices = [header.index(column) for column in columns]
        except ValueError as error:
            raise ValueError(f"{path.name}: {error}") from error
        splits = max(indices) + 1
        for number, line in enumerate(handle, start=2):
            fields = line.rstrip("\r\n").split("|", splits)
            if len(fields) < splits:
                raise ValueError(f"{path.name}:{number}: expected at least {splits} fields")
            yield [fields[index] for index in indices]


def load_ldbc_snapshot(snapshot: Path) -> Graph:
    """Read an LDBC FinBench ``snapshot/`` directory; every edge endpoint must exist."""
    graph = Graph()
    for label, (stem, id_column, properties) in _NODE_FILES.items():
        names = list(properties)
        columns = [id_column, *(properties[name][0] for name in names)]
        parsers = [properties[name][1] for name in names]
        for fields in _csv_rows(snapshot / f"{stem}.csv", columns):
            values = {
                name: parse(text)
                for name, parse, text in zip(names, parsers, fields[1:], strict=True)
            }
            graph.add_node(label, int(fields[0]), values)
    for stem, kind, src_label, src_column, dst_label, dst_column, amount in LDBC_EDGE_FILES:
        columns = [src_column, dst_column, "createTime"] + ([amount] if amount else [])
        for index, fields in enumerate(_csv_rows(snapshot / f"{stem}.csv", columns)):
            src, dst = int(fields[0]), int(fields[1])
            if not graph.has(src_label, src) or not graph.has(dst_label, dst):
                raise ValueError(f"{stem}.csv row {index + 2}: dangling endpoint {src}->{dst}")
            graph.add_edge(
                Edge(
                    kind=kind,
                    index=index,
                    src_label=src_label,
                    src=src,
                    dst_label=dst_label,
                    dst=dst,
                    timestamp=ldbc_epoch_millis(fields[2]),
                    amount=float(fields[3]) if amount else 0.0,
                )
            )
    return graph


def _parameter_value(name: str, text: str) -> Any:
    if name in _TEXT_PARAMETERS:
        return text
    if name in _FLOAT_PARAMETERS:
        return float(text)
    return int(text)


def read_ldbc_parameters(params: Path) -> dict[str, list[tuple[str, dict[str, Any]]]]:
    """Every published read binding, keyed ``line-<n>`` by its line in the file.

    The binding ids are the ones a scorecard workload must use for the
    reference to apply.
    """
    bindings: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    for operation, names in LDBC_PARAMETERS.items():
        path = params / f"complex_{operation.removeprefix('TCR')}_param.csv"
        entries = []
        with path.open(encoding="utf-8", newline="") as handle:
            for number, line in enumerate(handle, start=1):
                text = line.rstrip("\r\n")
                if number == 1 and text == _PARAMETER_PLACEHOLDER:
                    continue
                if not text:
                    raise ValueError(f"{path.name}:{number}: empty line")
                fields = text.split("|")
                if len(fields) != len(names):
                    raise ValueError(f"{path.name}:{number}: expected {len(names)} fields")
                binding = {
                    name: _parameter_value(name, field)
                    for name, field in zip(names, fields, strict=True)
                }
                entries.append((f"line-{number}", binding))
        if not entries:
            raise ValueError(f"{path.name} has no bindings")
        bindings[operation] = entries
    return bindings


def derive_ldbc_reference(snapshot: Path, params: Path, rung_id: str) -> dict[str, Any]:
    """The rung reference for one published scale factor, in driver cell form."""
    bindings = read_ldbc_parameters(params)
    graph = load_ldbc_snapshot(snapshot)
    queries: dict[str, Any] = {}
    for operation, entries in bindings.items():
        evaluate = EVALUATORS[operation]
        columns = [name for name, _ in COLUMNS[operation]]
        queries[operation] = {
            "matching": "exact",
            "bindings": {
                binding_id: {
                    "columns": columns,
                    "rows": render_rows(operation, evaluate(graph, binding), _arrow_cell),
                }
                for binding_id, binding in entries
            },
        }
    return {
        "schema": REFERENCE_SCHEMA,
        "suite_id": SUITE_ID,
        "rung_id": rung_id,
        "source": REFERENCE_SOURCE,
        "queries": queries,
    }


def _write_new(path: Path, text: str) -> None:
    """Write ``path`` atomically; never overwrite an existing reference."""
    if path.exists():
        raise FileExistsError(f"{path} exists; references are never overwritten")
    partial = path.with_name(path.name + ".partial")
    with partial.open("x", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.link(partial, path)
    partial.unlink()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    fixture = commands.add_parser("fixture", help="check or rewrite the fixture's expected.json")
    fixture.add_argument("fixture", type=Path)
    fixture.add_argument("--write", action="store_true", help="rewrite expected.json")
    ldbc = commands.add_parser("ldbc", help="derive a published scale factor's rung reference")
    ldbc.add_argument("--snapshot", type=Path, required=True, help="the LDBC snapshot/ directory")
    ldbc.add_argument("--params", type=Path, required=True, help="the read parameter directory")
    ldbc.add_argument("--rung", required=True, help="the rung id, for example sf1")
    ldbc.add_argument("--output", type=Path, required=True, help="a new reference JSON file")
    args = parser.parse_args(argv)
    if args.command == "ldbc":
        reference = derive_ldbc_reference(args.snapshot, args.params, args.rung)
        _write_new(args.output, json.dumps(reference, indent=1) + "\n")
        return 0
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
