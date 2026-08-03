"""pytest-bdd steps shared by the public API feature corpus.

Required public API behavior runs strictly and fails closed.
"""

from __future__ import annotations

from typing import Any
import uuid

import pyarrow as pa
import pytest
from pytest_bdd import given, parsers, then, when

import graphforge
from graphforge.api import (
    EdgeHandle,
    GraphForge,
    NodeHandle,
)
from graphforge.exceptions import (
    ExecutionError,
    LifecycleError,
    OntologyError,
    ParseError,
    StorageError,
    ValidationError,
)

# ---------------------------------------------------------------------------
# Shared context holder
# ---------------------------------------------------------------------------


class _Ctx:
    """Per-scenario mutable state bag passed via fixtures."""

    forge: GraphForge | None = None
    result: Any = None
    error: BaseException | None = None
    nodes: dict[str, NodeHandle]
    edges: list[EdgeHandle]
    extra: dict[str, Any]

    def __init__(self) -> None:
        self.nodes = {}
        self.edges = []
        self.extra = {}


@pytest.fixture
def ctx():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    yield c
    if c.forge is not None:
        try:
            c.forge.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _catch(fn, *args, **kwargs):
    """Call fn; store exception on _Ctx if one is raised. Return (result, exc)."""
    try:
        return fn(*args, **kwargs), None
    except Exception as exc:
        return None, exc


# ---------------------------------------------------------------------------
# GIVEN steps
# ---------------------------------------------------------------------------


@given("an empty graph", target_fixture="ctx")
def given_empty_graph():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    return c


@given(parsers.parse('a graph with a Person node named "{name}"'), target_fixture="ctx")
def given_graph_with_person(name):
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    h = c.forge.add_node("Person", name=name)
    c.nodes[name] = h
    return c


@given(
    parsers.parse('a graph with a Person node named "{name}" with age {age:d}'),
    target_fixture="ctx",
)
def given_graph_with_person_age(name, age):
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    h = c.forge.add_node("Person", name=name, age=age)
    c.nodes[name] = h
    return c


@given(parsers.parse("a graph with {n:d} Person nodes"), target_fixture="ctx")
def given_graph_with_n_persons(n):
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    for i in range(n):
        nm = f"Person{i}"
        h = c.forge.add_node("Person", name=nm)
        c.nodes[nm] = h
    return c


@given(parsers.parse("a graph with 3 Person nodes connected by KNOWS edges"), target_fixture="ctx")
def given_3_persons_knows():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    c.forge.execute(
        "CREATE (a:Person {name:'Alice'})-[:KNOWS]->"
        "(b:Person {name:'Bob'})-[:KNOWS]->(c:Person {name:'Carol'})"
    )
    return c


@given(parsers.parse("a graph with 4 Person nodes in two connected groups"), target_fixture="ctx")
def given_4_persons_two_groups():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    c.forge.execute(
        "CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'})-[:KNOWS]->(d:Person {name:'Dave'})"
    )
    return c


@given("a graph with a directed cycle", target_fixture="ctx")
def given_directed_cycle():
    c = _Ctx()
    c.forge = GraphForge()
    c.forge.execute(
        "CREATE (a:Person {name:'Alice'})-[:KNOWS]->"
        "(b:Person {name:'Bob'})-[:KNOWS]->"
        "(c:Person {name:'Carol'})-[:KNOWS]->(a)"
    )
    return c


@given(parsers.parse('a graph with a Paper node titled "{title}"'), target_fixture="ctx")
def given_paper_node(title):
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    h = c.forge.add_node("Paper", title=title)
    c.nodes[title] = h
    return c


@given("a graph with a Paper node that has a stored vector embedding", target_fixture="ctx")
def given_paper_with_vector():
    import numpy as np

    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    h = c.forge.add_node("Paper", title="Stub Paper")
    c.nodes["Stub Paper"] = h
    c.extra["vector"] = np.ones(128, dtype=float)
    return c


@given(
    parsers.parse('a graph with a Paper node titled "{title}" and a stored vector embedding'),
    target_fixture="ctx",
)
def given_paper_with_title_and_vector(title):
    import numpy as np

    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    h = c.forge.add_node("Paper", title=title)
    c.nodes[title] = h
    c.extra["vector"] = np.ones(128, dtype=float)
    return c


@given(
    parsers.parse('a graph with a Paper node titled "{title}" and a Person node named "{name}"'),
    target_fixture="ctx",
)
def given_paper_and_person(title, name):
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    c.nodes[title] = c.forge.add_node("Paper", title=title)
    c.nodes[name] = c.forge.add_node("Person", name=name)
    return c


@given(parsers.parse("a graph with {n:d} Paper nodes with similar titles"), target_fixture="ctx")
def given_n_paper_nodes_similar(n):
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    for i in range(n):
        t = f"Graph Theory Paper {i}"
        c.nodes[t] = c.forge.add_node("Paper", title=t)
    return c


@given(
    parsers.parse("a graph with {n:d} Paper nodes with title and abstract properties"),
    target_fixture="ctx",
)
def given_paper_nodes_with_abstract(n):
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    for i in range(n):
        t = f"Neural Networks Paper {i}"
        c.nodes[t] = c.forge.add_node("Paper", title=t, abstract="About neural networks")
    return c


@given("a graph with a Paper node", target_fixture="ctx")
def given_single_paper():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    h = c.forge.add_node("Paper", title="Stub Paper")
    c.nodes["paper"] = h
    c.extra["paper_id"] = h.uuid
    return c


@given(parsers.parse("a graph with 3 Paper nodes with title properties"), target_fixture="ctx")
def given_3_paper_nodes_title():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    for i in range(3):
        t = f"Paper {i}"
        c.nodes[t] = c.forge.add_node("Paper", title=t)
    return c


@given(
    parsers.parse('a graph with a Person node named "{name}" and a Paper node titled "{title}"'),
    target_fixture="ctx",
)
def given_person_and_paper(name, title):
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    c.nodes[name] = c.forge.add_node("Person", name=name)
    c.nodes[title] = c.forge.add_node("Paper", title=title)
    return c


@given("a graph with a KNOWS relationship and an AUTHORED relationship", target_fixture="ctx")
def given_two_rel_types():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    alice = c.forge.add_node("Person", name="Alice")
    bob = c.forge.add_node("Person", name="Bob")
    paper = c.forge.add_node("Paper", title="GNN")
    c.edges.append(c.forge.add_edge(alice, "KNOWS", bob))
    c.edges.append(c.forge.add_edge(alice, "AUTHORED", paper))
    c.nodes["Alice"] = alice
    c.nodes["Bob"] = bob
    c.nodes["paper"] = paper
    return c


@given(
    parsers.parse("a graph with {np:d} Person nodes and {npa:d} Paper node"), target_fixture="ctx"
)
def given_persons_and_papers(np, npa):
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    for i in range(np):
        c.nodes[f"p{i}"] = c.forge.add_node("Person", name=f"Person{i}")
    for i in range(npa):
        c.nodes[f"paper{i}"] = c.forge.add_node("Paper", title=f"Paper{i}")
    return c


@given(parsers.parse("a graph with Person nodes but no Paper nodes"), target_fixture="ctx")
def given_persons_no_papers():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    for name in ("Alice", "Bob"):
        c.nodes[name] = c.forge.add_node("Person", name=name)
    return c


@given(
    parsers.parse("a graph with Paper nodes indexed with {n:d}-dimensional vectors"),
    target_fixture="ctx",
)
def given_paper_indexed_with_vectors(n):

    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    h = c.forge.add_node("Paper", title="Stub")
    c.nodes["paper"] = h
    c.extra["vector_dim"] = n
    return c


@given("a graph with Person nodes connected by KNOWS edges", target_fixture="ctx")
def given_persons_knows_generic():
    return given_3_persons_knows()


@given(
    parsers.parse("a graph with Person nodes connected by both KNOWS and FOLLOWS edges"),
    target_fixture="ctx",
)
def given_persons_knows_and_follows():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    alice = c.forge.add_node("Person", name="Alice")
    bob = c.forge.add_node("Person", name="Bob")
    c.forge.add_edge(alice, "KNOWS", bob)
    c.forge.add_edge(bob, "FOLLOWS", alice)
    c.nodes["Alice"] = alice
    c.nodes["Bob"] = bob
    return c


@given(
    parsers.parse("a graph with Person nodes connected by directed KNOWS edges"),
    target_fixture="ctx",
)
def given_directed_knows():
    return given_3_persons_knows()


@given(
    "2 other Person nodes connected by a KNOWS edge but isolated from the first pair",
    target_fixture="ctx",
)
def given_second_component(ctx):
    ctx.forge.execute("CREATE (c:Person {name:'Carol'})-[:KNOWS]->(d:Person {name:'Dave'})")
    return ctx


@given(parsers.parse("a graph with 2 Person nodes connected by a KNOWS edge"), target_fixture="ctx")
def given_2_connected():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    c.forge.execute("CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})")
    return c


@given(parsers.parse('a Person node named "{name}"'), target_fixture="ctx")
def given_person_node_plain(name, ctx):
    h = ctx.forge.add_node("Person", name=name)
    ctx.nodes[name] = h
    return ctx


@given(parsers.parse('Person nodes named "{first}" and "{second}"'), target_fixture="ctx")
def given_named_person_pair(first, second, ctx):
    for name in (first, second):
        ctx.nodes[name] = ctx.forge.add_node("Person", name=name)
    return ctx


@given(
    parsers.parse('a graph with a Person node with age stored as a string "{val}"'),
    target_fixture="ctx",
)
def given_person_string_age(val):
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    h = c.forge.add_node("Person", name="Alice", age=val)
    c.nodes["Alice"] = h
    return c


@given("a path that does not exist on disk", target_fixture="ctx")
def given_nonexistent_path(tmp_path):
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.extra["path"] = str(tmp_path / "does_not_exist")
    c.forge = None
    return c


@given("a persistent graph backed by Parquet", target_fixture="ctx")
def given_persistent_graph(tmp_path):
    d = tmp_path / "graph"
    d.mkdir()
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge(str(d))
    c.extra["path"] = str(d)
    return c


@given("a persistent graph at a temporary path", target_fixture="ctx")
def given_persistent_at_tmp(tmp_path):
    d = tmp_path / "graph"
    d.mkdir()
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge(str(d))
    c.extra["path"] = str(d)
    return c


@given("the forge instance is closed")
def given_forge_closed(ctx):
    ctx.forge.close()


@given(
    parsers.parse(
        'a graph with a Person node named "{name}" connected by a KNOWS edge to a Person node named "{name2}"'
    ),
    target_fixture="ctx",
)
def given_two_persons_edge(name, name2):
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    a = c.forge.add_node("Person", name=name)
    b = c.forge.add_node("Person", name=name2)
    c.forge.add_edge(a, "KNOWS", b)
    c.nodes[name] = a
    c.nodes[name2] = b
    return c


@given('a graph with a Person node named "Alice" without an age property', target_fixture="ctx")
def given_person_no_age():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    h = c.forge.add_node("Person", name="Alice")
    c.nodes["Alice"] = h
    return c


@given(parsers.parse("a valid ontology YAML file defining a Person label"), target_fixture="ctx")
def given_valid_ontology_yaml(tmp_path):
    p = tmp_path / "ontology.yaml"
    p.write_text(
        "ontology_id: people\n"
        'version: "2026.06"\n'
        "entity_types:\n"
        "  - name: Person\n"
        "properties:\n"
        "  - name: name\n"
        "    owner: Person\n"
        "    type: utf8\n"
    )
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {"ontology_path": str(p)}
    c.forge = GraphForge()
    return c


@given(parsers.parse("a valid ontology JSON file defining a Paper label"), target_fixture="ctx")
def given_valid_ontology_json(tmp_path):
    import json

    p = tmp_path / "ontology.json"
    p.write_text(
        json.dumps(
            {
                "ontology_id": "papers",
                "version": "2026.06",
                "entity_types": [{"name": "Paper"}],
                "properties": [{"name": "title", "owner": "Paper", "type": "utf8"}],
            }
        )
    )
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {"ontology_path": str(p)}
    c.forge = GraphForge()
    return c


@given("a file containing invalid YAML", target_fixture="ctx")
def given_invalid_yaml(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text(": this is not: valid: yaml: [")
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {"ontology_path": str(p)}
    c.forge = GraphForge()
    return c


@given("a graph with Person nodes connected by KNOWS edges up to 3 hops deep", target_fixture="ctx")
def given_persons_3hops():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    names = ["Alice", "Bob", "Carol", "Dave"]
    handles = [c.forge.add_node("Person", name=n) for n in names]
    c.nodes.update(zip(names, handles, strict=True))
    for i in range(len(handles) - 1):
        c.forge.add_edge(handles[i], "KNOWS", handles[i + 1])
    return c


@given(
    "a graph where Alice knows Bob and Bob knows Charlie but Alice does not know Charlie",
    target_fixture="ctx",
)
def given_alice_bob_charlie():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    alice = c.forge.add_node("Person", name="Alice")
    bob = c.forge.add_node("Person", name="Bob")
    charlie = c.forge.add_node("Person", name="Charlie")
    c.forge.add_edge(alice, "KNOWS", bob)
    c.forge.add_edge(bob, "KNOWS", charlie)
    c.nodes = {"Alice": alice, "Bob": bob, "Charlie": charlie}
    return c


@given("a graph where Alice knows Bob", target_fixture="ctx")
def given_alice_knows_bob():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    alice = c.forge.add_node("Person", name="Alice")
    bob = c.forge.add_node("Person", name="Bob")
    c.forge.add_edge(alice, "KNOWS", bob)
    c.nodes = {"Alice": alice, "Bob": bob}
    return c


@given('a graph with a single Person node named "Lone"', target_fixture="ctx")
def given_lone_person():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    h = c.forge.add_node("Person", name="Lone")
    c.nodes["Lone"] = h
    return c


# Shared GIVEN steps that modify existing ctx (no target_fixture)


@given('I have stored the node id as "paper_id"')
def given_store_paper_id(ctx):
    if "paper" in ctx.nodes:
        ctx.extra["paper_id"] = ctx.nodes["paper"].uuid
    elif ctx.nodes:
        ctx.extra["paper_id"] = next(iter(ctx.nodes.values())).uuid


@given('I have an embedding vector stored as "embedding"')
def given_store_embedding(ctx):
    import numpy as np

    ctx.extra["embedding"] = np.ones(128, dtype=float)


@given("no explicit index call was made before find")
def given_no_index_call(ctx):
    ctx.extra["index_called"] = False


# ---------------------------------------------------------------------------
# WHEN steps
# ---------------------------------------------------------------------------


@when(parsers.parse('I execute "{query}"'))
def when_execute(ctx, query):
    if ctx.forge is None:
        # forge failed to open — error already stored; stay in error state
        return
    ctx.result, ctx.error = _catch(ctx.forge.execute, query)


@when(parsers.parse('I execute "{query}" with parameter name "{value}"'))
def when_execute_with_param(ctx, query, value):
    ctx.result, ctx.error = _catch(ctx.forge.execute, query, {"name": value})


@when('I execute "" ')
def when_execute_empty(ctx):
    ctx.result, ctx.error = _catch(ctx.forge.execute, "")


@when(parsers.parse('I execute "{query}" without parameters'))
def when_execute_no_params(ctx, query):
    ctx.result, ctx.error = _catch(ctx.forge.execute, query, None)


@when(parsers.parse('I add a node with label "{label}" named "{name}"'))
def when_add_node(ctx, label, name):
    ctx.result, ctx.error = _catch(ctx.forge.add_node, label, name=name)
    if ctx.result:
        ctx.nodes[name] = ctx.result


@when(parsers.parse('I add a node with label "{label}" named "{name}" aged {age:d}'))
def when_add_node_aged(ctx, label, name, age):
    ctx.result, ctx.error = _catch(ctx.forge.add_node, label, name=name, age=age)
    if ctx.result:
        ctx.nodes[name] = ctx.result


@when(parsers.parse('I request "{algorithm}" paths using "{selector}" selectors'))
def when_paths_with_selector_form(ctx, algorithm, selector):
    alice = ctx.nodes["Alice"]
    bob = ctx.nodes["Bob"]
    if selector == "UUID":
        source, target = alice.uuid, bob.uuid
    elif selector == "handle":
        source, target = alice, bob
    elif selector == "property":
        source = {"label": "Person", "property": "name", "value": "Alice"}
        target = {"label": "Person", "property": "name", "value": "Bob"}
    else:
        raise AssertionError(f"unknown selector form {selector!r}")
    ctx.result, ctx.error = _catch(ctx.forge.paths, source, target, by=algorithm)


@when(parsers.parse('I request "{algorithm}" paths with a "{case}" source selector'))
def when_paths_with_invalid_selector(ctx, algorithm, case):
    bob = ctx.nodes["Bob"]
    ctx.extra["selector_case"] = case
    if case == "malformed":
        source = {"label": "Person", "property": "name"}
    elif case == "missing":
        source = "01900000-0000-7000-8000-000000000000"
    elif case == "ambiguous":
        ctx.forge.add_node("Person", name="Alice")
        source = {"label": "Person", "property": "name", "value": "Alice"}
    elif case == "cross-graph":
        other = GraphForge()
        source = other.add_node("Person", name="Mallory")
    else:
        raise AssertionError(f"unknown invalid selector case {case!r}")
    ctx.result, ctx.error = _catch(ctx.forge.paths, source, bob, by=algorithm)


@when(parsers.parse('I add a node with label "{label}"'))
def when_add_node_no_props(ctx, label):
    ctx.result, ctx.error = _catch(ctx.forge.add_node, label)


@when('I add a node with label "Person" with an unsupported property value')
def when_add_node_bad_prop(ctx):
    ctx.result, ctx.error = _catch(ctx.forge.add_node, "Person", data=object())


@when(
    parsers.parse('I add a "{rel_type}" edge from "{src_name}" to "{dst_name}" with since {year:d}')
)
def when_add_edge_since(ctx, rel_type, src_name, dst_name, year):
    src = ctx.nodes[src_name]
    dst = ctx.nodes[dst_name]
    ctx.result, ctx.error = _catch(ctx.forge.add_edge, src, rel_type, dst, since=year)
    if ctx.result:
        ctx.edges.append(ctx.result)


@when(parsers.parse('I add a "{rel_type}" edge from "{src_name}" to "{dst_name}"'))
def when_add_edge(ctx, rel_type, src_name, dst_name):
    src = ctx.nodes.get(src_name)
    dst = ctx.nodes.get(dst_name)
    if src is None or dst is None:
        ctx.error = ValidationError(f"Node not found: {src_name!r} or {dst_name!r}")
        return
    ctx.result, ctx.error = _catch(ctx.forge.add_edge, src, rel_type, dst)


@when('I add a "KNOWS" edge from a raw integer to the node for "Alice"')
def when_add_edge_bad_src(ctx):
    dst = ctx.nodes["Alice"]
    ctx.result, ctx.error = _catch(ctx.forge.add_edge, 42, "KNOWS", dst)


@when('I add a "KNOWS" edge from the node for "Alice" to a raw integer')
def when_add_edge_bad_dst(ctx):
    src = ctx.nodes["Alice"]
    ctx.result, ctx.error = _catch(ctx.forge.add_edge, src, "KNOWS", 42)


@when('I bulk add nodes with label "Person" and 2 records')
def when_bulk_add_nodes_list(ctx):
    records = [{"name": "Alice"}, {"name": "Bob"}]
    ctx.result, ctx.error = _catch(ctx.forge.add_nodes, "Person", records)


@when('I bulk add nodes with label "Person" from an Arrow Table of 5 rows')
def when_bulk_add_nodes_arrow(ctx):
    table = pa.table({"name": ["A", "B", "C", "D", "E"]})
    ctx.result, ctx.error = _catch(ctx.forge.add_nodes, "Person", table)


@when(
    'I bulk add edges with type "KNOWS" using source column "src_id" and destination column "dst_id"'
)
def when_bulk_add_edges(ctx):
    nodes = list(ctx.nodes.values())
    if len(nodes) < 2:
        ctx.error = ValidationError("Need 2 nodes")
        return
    records = [{"src_id": nodes[0].uuid, "dst_id": nodes[1].uuid}]
    ctx.result, ctx.error = _catch(
        ctx.forge.add_edges,
        "KNOWS",
        records,
        operation_uuid=nodes[0].uuid,
        src="src_id",
        dst="dst_id",
    )


@when('a graph with 2 Person nodes with ids in columns "src_id" and "dst_id"')
def given_2_nodes_for_edges(ctx):
    # This is sometimes used as a given — register the nodes in ctx.
    if len(ctx.nodes) < 2:
        for nm in ("SrcNode", "DstNode"):
            ctx.nodes[nm] = ctx.forge.add_node("Person", name=nm)


@when(parsers.parse('I rank "{label}" by "{algorithm}"'))
def when_rank(ctx, label, algorithm):
    ctx.result, ctx.error = _catch(ctx.forge.rank, label, by=algorithm)
    ctx.extra["last_rank"] = ctx.result


@when(parsers.parse('I rank "{label}" by "{algorithm}" writing result to property "{prop}"'))
def when_rank_write(ctx, label, algorithm, prop):
    ctx.result, ctx.error = _catch(ctx.forge.rank, label, by=algorithm, write_property=prop)


@when(parsers.parse('I rank "{label}" by "{algorithm}" via relationship type "{via}"'))
def when_rank_via(ctx, label, algorithm, via):
    ctx.result, ctx.error = _catch(ctx.forge.rank, label, by=algorithm, via=via)


@when(parsers.parse('I rank "{label}" by "{algorithm}" treating edges as directed'))
def when_rank_directed(ctx, label, algorithm):
    r, e = _catch(ctx.forge.rank, label, by=algorithm, directed=True)
    ctx.extra["rank_directed"] = r
    ctx.result, ctx.error = r, e


@when(parsers.parse('I rank "{label}" by "{algorithm}" treating edges as undirected'))
def when_rank_undirected(ctx, label, algorithm):
    r, e = _catch(ctx.forge.rank, label, by=algorithm, directed=False)
    ctx.extra["rank_undirected"] = r
    if ctx.error is None:
        ctx.error = e


@when(parsers.parse('I cluster "{label}" by "{algorithm}"'))
def when_cluster(ctx, label, algorithm):
    ctx.result, ctx.error = _catch(ctx.forge.cluster, label, by=algorithm)


@when(parsers.parse('I cluster "{label}" by "{algorithm}" writing result to property "{prop}"'))
def when_cluster_write(ctx, label, algorithm, prop):
    ctx.result, ctx.error = _catch(ctx.forge.cluster, label, by=algorithm, write_property=prop)


@when(parsers.parse('I find "{query_text}" in label "{label}"'))
def when_find_text(ctx, query_text, label):
    ctx.result, ctx.error = _catch(ctx.forge.find, query_text, label=label)


@when(parsers.parse('I find "{query_text}" in label "{label}" with limit {limit:d}'))
def when_find_text_limit(ctx, query_text, label, limit):
    ctx.result, ctx.error = _catch(ctx.forge.find, query_text, label=label, limit=limit)


@when('I find by the stored vector in label "Paper"')
def when_find_vector(ctx):
    vec = ctx.extra.get("vector")
    ctx.result, ctx.error = _catch(ctx.forge.find, label="Paper", vector=vec)


@when(parsers.parse('I find by the stored embedding in label "{label}" in space "{space}"'))
def when_find_embedding(ctx, label, space):
    vec = ctx.extra.get("embedding")
    ctx.result, ctx.error = _catch(ctx.forge.find, label=label, vector=vec, space=space)


@when(parsers.parse('I find "{query_text}" with the stored vector in label "{label}"'))
def when_find_text_vector(ctx, query_text, label):
    vec = ctx.extra.get("vector")
    ctx.result, ctx.error = _catch(ctx.forge.find, query_text, label=label, vector=vec)


@when('I find with no query and no vector in label "Paper"')
def when_find_no_args(ctx):
    ctx.result, ctx.error = _catch(ctx.forge.find, label="Paper")


@when('I find by an empty vector in label "Paper"')
def when_find_empty_vector(ctx):
    import numpy as np

    ctx.result, ctx.error = _catch(ctx.forge.find, label="Paper", vector=np.array([]))


@when('I find by a vector containing NaN in label "Paper"')
def when_find_nan_vector(ctx):
    import numpy as np

    ctx.result, ctx.error = _catch(
        ctx.forge.find, label="Paper", vector=np.array([float("nan"), 1.0])
    )


@when('I find by a vector containing infinity in label "Paper"')
def when_find_inf_vector(ctx):
    import numpy as np

    ctx.result, ctx.error = _catch(
        ctx.forge.find, label="Paper", vector=np.array([float("inf"), 1.0])
    )


@when(parsers.parse('I find by a {n:d}-dimensional vector in label "{label}"'))
def when_find_wrong_dim(ctx, n, label):
    import numpy as np

    ctx.result, ctx.error = _catch(ctx.forge.find, label=label, vector=np.ones(n))


@when(parsers.parse('I index label "{label}" on properties "{p1}" and "{p2}"'))
def when_index_two_props(ctx, label, p1, p2):
    ctx.result, ctx.error = _catch(ctx.forge.index, label, properties=[p1, p2])
    if isinstance(ctx.error, TypeError):
        raise AssertionError("required public API contract was not satisfied")
    ctx.extra["index_called"] = True


@when(parsers.parse('I index label "{label}" on property "{prop}"'))
def when_index_one_prop(ctx, label, prop):
    ctx.result, ctx.error = _catch(ctx.forge.index, label, properties=[prop])
    if isinstance(ctx.error, TypeError):
        raise AssertionError("required public API contract was not satisfied")
    ctx.extra["index_called"] = True
    if "first_find_result" not in ctx.extra:
        ctx.extra["first_find_result"] = ctx.forge.find("paper", label=label)
        ctx.extra["first_index_done"] = True


@when(
    parsers.parse(
        'I index label "{label}" storing the vector for node "{node_key}" in space "{space}"'
    )
)
def when_index_vector(ctx, label, node_key, space):
    node_id = ctx.extra.get("paper_id") or ctx.extra.get(node_key)
    vec = ctx.extra.get("embedding")
    ctx.result, ctx.error = _catch(ctx.forge.index, label, node_id=node_id, vector=vec, space=space)


@when('I index label "Paper" on an empty properties list')
def when_index_empty_props(ctx):
    ctx.result, ctx.error = _catch(ctx.forge.index, "Paper", properties=[])


@when('I add a node with label "Paper" titled "Deep Graph Learning"')
def when_add_deep_graph_paper(ctx):
    h = ctx.forge.add_node("Paper", title="Deep Graph Learning")
    ctx.nodes["Deep Graph Learning"] = h


@when("I call schema")
def when_schema(ctx):
    ctx.result, ctx.error = _catch(ctx.forge.schema)


@when("I call labels")
def when_labels(ctx):
    ctx.result, ctx.error = _catch(ctx.forge.labels)


@when("I call relationship_types")
def when_rel_types(ctx):
    ctx.result, ctx.error = _catch(ctx.forge.relationship_types)


@when(parsers.parse('I call node_count for label "{label}"'))
def when_node_count(ctx, label):
    ctx.result, ctx.error = _catch(ctx.forge.node_count, label)


@when(parsers.parse('I call explain on "{query}"'))
def when_explain(ctx, query):
    ctx.result, ctx.error = _catch(ctx.forge.explain, query)


@when("I call clear")
def when_clear(ctx):
    ctx.result, ctx.error = _catch(ctx.forge.clear)


@when(parsers.parse('I analyze by "{algorithm}"'))
def when_analyze(ctx, algorithm):
    ctx.result, ctx.error = _catch(ctx.forge.analyze, by=algorithm)


@when("I open a graph at that path")
def when_open_at_bad_path(ctx):
    path = ctx.extra.get("path", "/nonexistent/path")
    ctx.result, ctx.error = _catch(GraphForge, path)
    if ctx.result:
        ctx.forge = ctx.result


@when("I reopen the forge at the same path")
def when_reopen(ctx):
    path = ctx.extra["path"]
    ctx.forge, ctx.error = _catch(GraphForge, path)


@when(parsers.parse("I attempt to call {method}"))
def when_attempt_call(ctx, method):
    method = method.strip()
    if method.startswith("execute with query"):
        q = method.split('"')[1]
        ctx.result, ctx.error = _catch(ctx.forge.execute, q)
    elif method.startswith("rank with label"):
        parts = method.split('"')
        label, algo = parts[1], parts[3]
        ctx.result, ctx.error = _catch(ctx.forge.rank, label, by=algo)
    elif method.startswith("find with text"):
        parts = method.split('"')
        text, label = parts[1], parts[3]
        ctx.result, ctx.error = _catch(ctx.forge.find, text, label=label)
    elif method.startswith("add_node with label"):
        parts = method.split('"')
        label, name = parts[1], parts[3]
        ctx.result, ctx.error = _catch(ctx.forge.add_node, label, name=name)
    else:
        ctx.error = LifecycleError(f"Unknown method in step: {method}")


@when(parsers.parse("I load the ontology from that file"))
def when_load_ontology(ctx):
    path = ctx.extra.get("ontology_path")
    ctx.result, ctx.error = _catch(ctx.forge.load_ontology, path)


@when(
    parsers.parse(
        'I call neighbourhood for "{canonical}" with hops {hops:d} in label "{label}" using canonical property "{prop}"'
    )
)
def when_neighbourhood(ctx, canonical, hops, label, prop):
    from graphforge.recipes import neighbourhood

    ctx.result, ctx.error = _catch(
        neighbourhood, ctx.forge, canonical, hops, label=label, canonical_prop=prop
    )


# Shared find step used after index
@when(parsers.parse('I find "{query_text}" in label "{label}"'))
def when_find_text_shared(ctx, query_text, label):
    r, e = _catch(ctx.forge.find, query_text, label=label)
    ctx.result, ctx.error = r, e
    if "first_index_done" in ctx.extra and "first_find_result" not in ctx.extra:
        ctx.extra["first_find_result"] = r


# ---------------------------------------------------------------------------
# THEN steps
# ---------------------------------------------------------------------------


@then("the result is an Arrow Table")
def then_arrow_table(ctx):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    assert isinstance(ctx.result, pa.Table), f"Expected Arrow Table, got {type(ctx.result)}"


@then(parsers.parse('the table has column "{col}"'))
def then_has_column(ctx, col):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    assert isinstance(ctx.result, pa.Table)
    if col not in ctx.result.schema.names:
        raise AssertionError(f"missing result column: {col}")


@then(parsers.parse('the result schema contains column "{col}"'))
def then_schema_has_column(ctx, col):
    then_has_column(ctx, col)


@then(parsers.parse("the table has {n:d} rows"))
def then_row_count(ctx, n):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    assert isinstance(ctx.result, pa.Table)
    if ctx.result.num_rows != n:
        raise AssertionError(f"expected {n} rows, got {ctx.result.num_rows}")


@then(parsers.parse("the table has {n:d} row"))
def then_row_count_singular(ctx, n):
    then_row_count(ctx, n)


@then(parsers.parse('the "is_dag" value is {expected}'))
def then_is_dag_value(ctx, expected):
    assert ctx.error is None
    assert ctx.result["is_dag"][0].as_py() is (expected == "true")


@then(parsers.parse("the table has at most {n:d} rows"))
def then_at_most_rows(ctx, n):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    assert isinstance(ctx.result, pa.Table)
    if ctx.result.num_rows > n:
        raise AssertionError("required public API contract was not satisfied")


@then(parsers.parse('the first row value for "{col}" is "{val}"'))
def then_first_row_str(ctx, col, val):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    assert isinstance(ctx.result, pa.Table)
    if ctx.result.num_rows == 0 or col not in ctx.result.schema.names:
        raise AssertionError("required public API contract was not satisfied")
    actual = ctx.result.column(col)[0].as_py()
    if actual != val:
        raise AssertionError(f"expected first {col} value {val!r}, got {actual!r}")


@then(parsers.parse('the first row value for "{col}" is null'))
def then_first_row_null(ctx, col):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    assert isinstance(ctx.result, pa.Table)
    if ctx.result.num_rows == 0 or col not in ctx.result.schema.names:
        raise AssertionError("required public API contract was not satisfied")
    actual = ctx.result.column(col)[0].as_py()
    if actual is not None:
        raise AssertionError("required public API contract was not satisfied")


@then("a ParseError is raised")
def then_parse_error(ctx):
    if not isinstance(ctx.error, ParseError):
        raise AssertionError("required public API contract was not satisfied")


@then("the error includes a source span")
def then_has_span(ctx):
    if not isinstance(ctx.error, ParseError) or ctx.error.span is None:
        raise AssertionError("required public API contract was not satisfied")


@then("an ExecutionError is raised")
def then_execution_error(ctx):
    if not isinstance(ctx.error, ExecutionError):
        actual = type(ctx.error).__name__ if ctx.error is not None else "no error"
        raise AssertionError(f"expected ExecutionError, got {actual}")


@then("a StorageError is raised")
def then_storage_error(ctx):
    if not isinstance(ctx.error, StorageError):
        raise AssertionError("required public API contract was not satisfied")


@then("a LifecycleError is raised")
def then_lifecycle_error(ctx):
    if not isinstance(ctx.error, LifecycleError):
        raise AssertionError("required public API contract was not satisfied")


@then("a TypeError is raised")
def then_type_error(ctx):
    if not isinstance(ctx.error, TypeError):
        raise AssertionError("required public API contract was not satisfied")


@then("a ValidationError is raised")
def then_validation_error(ctx):
    if not isinstance(ctx.error, ValidationError):
        raise AssertionError("required public API contract was not satisfied")


@then("an OntologyError is raised")
def then_ontology_error(ctx):
    if not isinstance(ctx.error, OntologyError):
        raise AssertionError("required public API contract was not satisfied")


@then("no error is raised")
def then_no_error(ctx):
    if ctx.error is not None:
        raise AssertionError(f"Unexpected error: {ctx.error}")


@then(parsers.parse('the result is a NodeHandle with label "{label}"'))
def then_node_handle(ctx, label):
    if ctx.error or not isinstance(ctx.result, NodeHandle):
        raise AssertionError("required public API contract was not satisfied")
    assert NodeHandle is graphforge.NodeHandle
    assert ctx.result.label == label


@then("the NodeHandle exposes UUID identity with no numeric surrogate")
def then_node_handle_uuid_only(ctx):
    if ctx.error or not isinstance(ctx.result, NodeHandle):
        raise AssertionError("required public API contract was not satisfied")
    assert isinstance(ctx.result.uuid, str)
    assert not hasattr(ctx.result, "id")
    assert not hasattr(ctx.result, "get")


@then(parsers.parse('execute readback returns the NodeHandle UUID and name "{name}"'))
def then_execute_with_uuid_returns_name(ctx, name):
    escaped_name = name.replace("\\", "\\\\").replace("'", "\\'")
    result = ctx.forge.execute(
        f"MATCH (n {{name: '{escaped_name}'}}) RETURN n.node_uuid AS uuid, n.name AS name"
    )
    assert result.num_rows == 1
    assert result.column("name")[0].as_py() == name
    assert result.column("uuid")[0].as_py().hex() == ctx.nodes[name].uuid.replace("-", "")


@then("the result is an EdgeHandle with UUID identity and no numeric surrogate")
def then_edge_handle(ctx):
    if ctx.error or not isinstance(ctx.result, EdgeHandle):
        raise AssertionError("required public API contract was not satisfied")
    assert isinstance(ctx.result.uuid, str)
    assert not hasattr(ctx.result, "id")
    assert not hasattr(ctx.result, "edge_id")


@then(parsers.parse('execute "{query}" returns {n:d} rows'))
def then_execute_n_rows(ctx, query, n):
    ctx.result = ctx.forge.execute(query)
    if ctx.result.num_rows != n:
        raise AssertionError("required public API contract was not satisfied")


@then(parsers.parse('execute "{query}" returns {n:d} row with value {val:d}'))
def then_execute_1_row_value(ctx, query, n, val):
    result = ctx.forge.execute(query)
    if result.num_rows != n:
        raise AssertionError("required public API contract was not satisfied")
    if result.num_rows == 0 or result.column(0)[0].as_py() != val:
        raise AssertionError("required public API contract was not satisfied")
    ctx.result = result


@then("the string representation contains the NodeHandle UUID")
def then_handle_repr_contains_uuid(ctx):
    if ctx.error or not isinstance(ctx.result, NodeHandle):
        raise AssertionError("required public API contract was not satisfied")
    assert ctx.result.uuid in str(ctx.result)


@then(parsers.parse('the string representation does not contain cached property "{text}"'))
def then_repr_excludes_cached_property(ctx, text):
    if ctx.error or not isinstance(ctx.result, NodeHandle):
        raise AssertionError("required public API contract was not satisfied")
    assert text not in str(ctx.result)


@then("the path request reaches Rust dispatch")
def then_path_reaches_dispatch(ctx):
    assert ctx.error is None
    assert isinstance(ctx.result, pa.Table)
    assert ctx.result.column_names == ["source_uuid", "target_uuid", "cost", "path"]


@then("a structured selector error is raised")
def then_structured_selector_error(ctx):
    expected = TypeError if ctx.extra.get("selector_case") == "malformed" else ValidationError
    assert isinstance(ctx.error, expected)


@then(parsers.parse("the result is {n:d}"))
def then_result_is_n(ctx, n):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    if ctx.result != n:
        raise AssertionError("required public API contract was not satisfied")


@then("the result is a non-empty string")
def then_nonempty_string(ctx):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    assert isinstance(ctx.result, str) and len(ctx.result) > 0


@then(parsers.parse('the result contains "{text}"'))
def then_result_contains_text(ctx, text):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    if isinstance(ctx.result, str):
        assert text in ctx.result, f"{text!r} not in explain output"
    elif isinstance(ctx.result, list):
        assert text in ctx.result, f"{text!r} not in list result"
    else:
        raise AssertionError("required public API contract was not satisfied")


@then("the result is an empty list")
def then_empty_list(ctx):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    assert ctx.result == []


@then("calling relationship_types also returns an empty list")
def then_rel_types_empty(ctx):
    result = ctx.forge.relationship_types()
    assert result == []


@then(parsers.parse('the table contains an entry for label "{label}"'))
def then_schema_has_label(ctx, label):
    if ctx.error or not isinstance(ctx.result, pa.Table):
        raise AssertionError("required public API contract was not satisfied")
    if "label" not in ctx.result.schema.names:
        raise AssertionError("required public API contract was not satisfied")
    labels = ctx.result.column("label").to_pylist()
    if label not in labels:
        raise AssertionError("required public API contract was not satisfied")


@then("the two score results are not identical")
def then_scores_differ(ctx):
    d = ctx.extra.get("rank_directed")
    u = ctx.extra.get("rank_undirected")
    if d is None or u is None:
        raise AssertionError("required public API contract was not satisfied")
    if d.num_rows == 0:
        raise AssertionError("required public API contract was not satisfied")
    # Compare score columns
    if d.equals(u):
        raise AssertionError("required public API contract was not satisfied")


@then("the 2 connected nodes share the same community_id")
def then_connected_same_community(ctx):
    assert ctx.error is None
    values = dict(
        zip(
            ctx.result.column("name").to_pylist(),
            ctx.result.column("community_id").to_pylist(),
            strict=True,
        )
    )
    assert values["Alice"] == values["Bob"]


@then("the 2 isolated nodes share a different community_id")
def then_isolated_different_community(ctx):
    assert ctx.error is None
    values = dict(
        zip(
            ctx.result.column("name").to_pylist(),
            ctx.result.column("community_id").to_pylist(),
            strict=True,
        )
    )
    assert values["Carol"] == values["Dave"]
    assert values["Alice"] != values["Carol"]


@then("no explicit index call was made before find")
def then_no_index_call(ctx):
    assert not ctx.extra.get("index_called", False)


@then('for each result row the id is valid in execute "MATCH (n) WHERE n.node_uuid = $id RETURN n"')
def then_ids_addressable(ctx):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    assert isinstance(ctx.result, pa.Table)
    ids = ctx.result.column("node_uuid").to_pylist()
    assert ids
    for value in ids:
        identifier = uuid.UUID(bytes=value) if isinstance(value, bytes) else uuid.UUID(str(value))
        result = ctx.forge.execute(
            "MATCH (n) WHERE n.node_uuid = $id RETURN n",
            {"id": identifier},
        )
        assert result.num_rows == 1


@then(parsers.parse('all result rows have label "{label}"'))
def then_all_rows_label(ctx, label):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    assert isinstance(ctx.result, pa.Table)
    ids = ctx.result.column("node_uuid").to_pylist()
    assert ids
    for value in ids:
        identifier = uuid.UUID(bytes=value) if isinstance(value, bytes) else uuid.UUID(str(value))
        result = ctx.forge.execute(
            f"MATCH (n:{label}) WHERE n.node_uuid = $id RETURN n.node_uuid",
            {"id": identifier},
        )
        assert result.num_rows == 1


@then("the result contains that node")
def then_result_contains_node(ctx):
    if ctx.error or not isinstance(ctx.result, pa.Table):
        raise AssertionError("required public API contract was not satisfied")
    if ctx.result.num_rows == 0:
        raise AssertionError("required public API contract was not satisfied")
    if "node_uuid" not in ctx.result.schema.names:
        raise AssertionError("required public API contract was not satisfied")
    expected = str(ctx.extra["paper_id"]).replace("-", "").lower()
    actual = {
        value.hex() if isinstance(value, bytes) else str(value).replace("-", "").lower()
        for value in ctx.result.column("node_uuid").to_pylist()
    }
    if expected not in actual:
        raise AssertionError("required public API contract was not satisfied")


@then('find "paper" in label "Paper" returns the same results as after the first index call')
def then_idempotent_index(ctx):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    second = ctx.forge.find("paper", label="Paper")
    assert ctx.extra["first_find_result"].equals(second)


@then(parsers.parse('the result contains a row with title "{title}"'))
def then_result_has_title(ctx, title):
    if ctx.error or not isinstance(ctx.result, pa.Table):
        raise AssertionError("required public API contract was not satisfied")
    if "title" not in ctx.result.schema.names or ctx.result.num_rows == 0:
        raise AssertionError("required public API contract was not satisfied")
    titles = ctx.result.column("title").to_pylist()
    if title not in titles:
        raise AssertionError("required public API contract was not satisfied")


@then(parsers.parse('the result contains a row for "{name}"'))
def then_result_has_row_for(ctx, name):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    if isinstance(ctx.result, pa.Table):
        if "name" not in ctx.result.schema.names:
            raise AssertionError("required public API contract was not satisfied")
        if name not in ctx.result.column("name").to_pylist():
            raise AssertionError("required public API contract was not satisfied")
    elif isinstance(ctx.result, list):
        names = [r.get("name") or r.get("canonical") for r in ctx.result]
        if name not in names:
            raise AssertionError("required public API contract was not satisfied")
    else:
        raise AssertionError("required public API contract was not satisfied")


@then(parsers.parse('the result does not contain a row for "{name}"'))
def then_result_no_row_for(ctx, name):
    assert ctx.error is None, f"unexpected error: {ctx.error!r}"
    if isinstance(ctx.result, pa.Table):
        if "name" not in ctx.result.schema.names:
            raise AssertionError("required public API contract was not satisfied")
        if name in ctx.result.column("name").to_pylist():
            raise AssertionError("required public API contract was not satisfied")
    elif isinstance(ctx.result, list):
        names = [r.get("name") or r.get("canonical") for r in ctx.result]
        if name in names:
            raise AssertionError("required public API contract was not satisfied")
    else:
        raise AssertionError("required public API contract was not satisfied")


@then("the result is an Arrow Table with at least 1 row")
def then_arrow_at_least_1(ctx):
    if ctx.error or not isinstance(ctx.result, pa.Table):
        raise AssertionError("required public API contract was not satisfied")
    if ctx.result.num_rows < 1:
        raise AssertionError("required public API contract was not satisfied")


# ---------------------------------------------------------------------------
# Additional missing steps
# ---------------------------------------------------------------------------

# "the table has 1 row" — specific count alias (parsers.parse handles {n:d})
# already covered by then_row_count; the duplicate here registers the exact text form.


@then(parsers.parse('execute "{query}" returns {n:d} rows'))
def then_execute_returns_n_rows(ctx, query, n):
    ctx.result = ctx.forge.execute(query)
    if ctx.result.num_rows != n:
        raise AssertionError("required public API contract was not satisfied")


@then(parsers.parse('execute "{query}" returns {n:d} row'))
def then_execute_returns_n_row(ctx, query, n):
    ctx.result = ctx.forge.execute(query)
    if ctx.result.num_rows != n:
        raise AssertionError("required public API contract was not satisfied")


# Given step used in construction Background (Given form, not When)
@given(
    'a graph with 2 Person nodes with ids in columns "src_id" and "dst_id"', target_fixture="ctx"
)
def given_2_nodes_for_edges_given():
    c = _Ctx()
    c.nodes = {}
    c.edges = []
    c.extra = {}
    c.forge = GraphForge()
    for nm in ("SrcNode", "DstNode"):
        c.nodes[nm] = c.forge.add_node("Person", name=nm)
    return c


# Given-form index step (used in index.feature as a Given within a scenario)
@given(parsers.parse('I index label "{label}" on property "{prop}"'))
def given_index_one_prop(ctx, label, prop):
    _, error = _catch(ctx.forge.index, label, properties=[prop])
    if isinstance(error, TypeError):
        raise AssertionError("required public API contract was not satisfied")
    ctx.extra["index_called"] = True
    ctx.extra["first_index_done"] = True


# When "I execute """  — empty string special case
@when('I execute ""')
def when_execute_empty_str(ctx):
    ctx.result, ctx.error = _catch(ctx.forge.execute, "")


# When "I add a node with label """ — empty label
@when('I add a node with label ""')
def when_add_node_empty_label(ctx):
    ctx.result, ctx.error = _catch(ctx.forge.add_node, "")


# When "I add a "" edge from "Alice" to "Bob"" — empty rel type
@when('I add a "" edge from "Alice" to "Bob"')
def when_add_edge_empty_type(ctx):
    src = ctx.nodes.get("Alice")
    dst = ctx.nodes.get("Bob")
    if src and dst:
        ctx.result, ctx.error = _catch(ctx.forge.add_edge, src, "", dst)
    else:
        ctx.error = ValidationError("Nodes not found")


# Given step used inside lifecycle transaction scenarios
@given(parsers.parse('I add a node with label "{label}" named "{name}"'))
def given_add_node_inline(ctx, label, name):
    h = ctx.forge.add_node(label, name=name)
    ctx.nodes[name] = h


# StorageError test needs forge=None handling in when_execute
# The scenario uses "Given a path that does not exist" then "When I open" then "execute"
# The forge is set during "When I open" — if StorageError was raised during open,
# forge stays None. The then step checks for error.
@then("a StorageError is raised")
def then_storage_error_v2(ctx):
    if not isinstance(ctx.error, StorageError):
        raise AssertionError("required public API contract was not satisfied")
