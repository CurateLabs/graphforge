"""Clean-venv smoke test for the native ``graphforge`` wheel.

Invoked directly (not under pytest) by the CI ``native-build`` job after
installing the built wheel into a fresh venv, so ``import graphforge`` exercises
the installed native package. Covers construction (#585), the exception
hierarchy (#588), the query/explain surface (#586), load_ontology (#589), and
execute_polars (#590).
"""

import math
from pathlib import Path
import tempfile
import uuid

import pyarrow as pa

import graphforge as g

_SUBCLASSES = [
    "ParseError",
    "PlanError",
    "ExecutionError",
    "StorageError",
    "LifecycleError",
    "ValidationError",
    "OntologyError",
]


def check_construction() -> None:
    # #585 — version + construction + accessors
    assert isinstance(g.__version__, str) and g.__version__, "missing __version__"
    forge = g.GraphForge()
    assert repr(forge) == "GraphForge(in-memory)", repr(forge)
    assert forge.ontology_mode == "exploratory", forge.ontology_mode
    assert forge.path is None, forge.path


def check_exception_hierarchy() -> None:
    # #588 — every fault-domain class exists and derives from the base.
    for name in _SUBCLASSES:
        cls = getattr(g, name)
        assert issubclass(cls, g.GraphForgeError), name

    # A real fault (missing project dir) maps to StorageError.
    try:
        g.GraphForge("/no/such/dir/graphforge-smoke")
    except g.StorageError as exc:
        assert isinstance(exc, g.GraphForgeError)
        assert str(exc)
    else:
        raise SystemExit("expected StorageError for a missing path")

    # Pre-v1 roots fail with the frozen format code before mutation.
    with tempfile.TemporaryDirectory() as directory:
        legacy = Path(directory, "topology", "nodes.parquet")
        legacy.parent.mkdir()
        legacy.write_bytes(b"legacy")
        try:
            g.GraphForge(directory)
        except g.StorageError as exc:
            assert exc.code == "GF_UNSUPPORTED_PROJECT_FORMAT", exc.code
            assert str(exc) == "project root does not contain the supported FORMAT marker"
            assert legacy.read_bytes() == b"legacy"
            assert not Path(directory, "FORMAT").exists()
        else:
            raise SystemExit("expected unsupported-format error for pre-v1 root")


def check_execute() -> None:
    # #586 — execute returns a real pyarrow.Table carrying the result metadata.
    forge = g.GraphForge()
    forge.execute("CREATE (:Person {name: 'Alice', age: 30})")
    forge.execute("CREATE (:Person {name: 'Bob', age: 25})")
    table = forge.execute("MATCH (p:Person) RETURN p.name AS name, p.age AS age")
    assert isinstance(table, pa.Table), type(table)
    assert table.num_rows == 2, table.num_rows
    names = set(table.column("name").to_pylist())
    assert names == {"Alice", "Bob"}, names
    meta = table.schema.metadata or {}
    assert b"graphforge.query_id" in meta, list(meta)

    # Parameters bind $placeholders.
    filtered = forge.execute(
        "MATCH (p:Person) WHERE p.age > $min RETURN p.name AS name", {"min": 28}
    )
    assert filtered.column("name").to_pylist() == ["Alice"], filtered.to_pydict()

    # Zero-row result keeps a valid schema (node_uuid is always a topology col):
    # the projected column survives and the graphforge.* metadata is present.
    empty = forge.execute("MATCH (n:Nope) RETURN n.node_uuid AS id")
    assert isinstance(empty, pa.Table) and empty.num_rows == 0
    assert empty.column_names == ["id"], empty.column_names
    assert b"graphforge.query_id" in (empty.schema.metadata or {})


def check_typed_uuid_parameters() -> None:
    forge = g.GraphForge()
    alice = forge.add_node("Person", name="Alice")
    bob = forge.add_node("Person", name="Bob")
    carol = forge.add_node("Person", name="Carol")
    edge = forge.add_edge(alice, "KNOWS", bob)
    alice_uuid = uuid.UUID(alice.uuid)
    edge_uuid = uuid.UUID(edge.uuid)

    nodes = forge.execute(
        "MATCH (n:Person) WHERE n.node_uuid = $id RETURN n.node_uuid AS node_uuid, n.name AS name",
        {"id": alice_uuid},
    )
    assert nodes.schema == pa.schema(
        [pa.field("node_uuid", pa.binary(16), nullable=False), pa.field("name", pa.string())],
        metadata=nodes.schema.metadata,
    )
    assert nodes.column("node_uuid").to_pylist() == [alice_uuid.bytes]
    assert nodes.column("name").to_pylist() == ["Alice"]

    def assert_query_metadata(metadata: dict[bytes, bytes] | None) -> None:
        assert metadata is not None
        assert set(metadata) == {
            b"graphforge.ir_version",
            b"graphforge.ontology_mode",
            b"graphforge.query_id",
        }
        assert metadata[b"graphforge.ir_version"] == b"0.3.0"
        assert metadata[b"graphforge.ontology_mode"] == b"exploratory"
        query_id = uuid.UUID(metadata[b"graphforge.query_id"].decode())
        assert query_id.version == 7
        assert str(query_id) == metadata[b"graphforge.query_id"].decode()

    assert_query_metadata(nodes.schema.metadata)

    ordered = forge.execute("MATCH (n:Person) RETURN n.node_uuid AS node_uuid ORDER BY node_uuid")
    assert ordered.schema == pa.schema(
        [pa.field("node_uuid", pa.binary(16), nullable=False)],
        metadata=ordered.schema.metadata,
    )
    assert ordered.column("node_uuid").to_pylist() == sorted(
        [uuid.UUID(handle.uuid).bytes for handle in (alice, bob, carol)]
    )
    assert_query_metadata(ordered.schema.metadata)

    edges = forge.execute(
        "MATCH ()-[r:KNOWS]->() WHERE r.edge_uuid = $id RETURN r.edge_uuid AS edge_uuid",
        {"id": edge_uuid},
    )
    assert edges.schema.field("edge_uuid").type == pa.binary(16)
    assert edges.column("edge_uuid").to_pylist() == [edge_uuid.bytes]

    assert (
        forge.execute(
            "MATCH (n:Person) WHERE n.node_uuid = $id RETURN n.node_uuid AS node_uuid",
            {"id": alice.uuid},
        ).num_rows
        == 0
    )
    try:
        forge.execute(
            "MATCH (n:Person) WHERE n.name = $id RETURN n.name",
            {"id": alice_uuid},
        )
    except g.ValidationError as exc:
        assert exc.code == "GF_VALIDATION", exc.code
        assert str(exc) == (
            "typed UUID parameter `$id` is only supported as a direct node_uuid or "
            "edge_uuid identity equality predicate"
        ), str(exc)
    else:
        raise SystemExit("expected incompatible typed UUID predicate validation failure")
    assert forge.execute("MATCH (n) RETURN n.node_uuid").num_rows == 3

    forge.execute("CREATE (:Token {value:$value})", {"value": alice.uuid})
    forge.execute("MATCH (n:Token) SET n.copy = $value", {"value": alice.uuid})
    writable = forge.execute(
        "MATCH (n:Token) WHERE n.value = $value AND n.copy = $value RETURN n.value",
        {"value": alice.uuid},
    )
    assert writable.column(0).to_pylist() == [alice.uuid]

    before = forge.execute("MATCH (n) RETURN n.node_uuid").num_rows
    for value in (alice_uuid, ["safe", alice_uuid], {"nested": alice_uuid}):
        try:
            forge.execute("CREATE (:Rejected {value:$value})", {"value": value})
        except g.ValidationError as exc:
            assert exc.code == "GF_VALIDATION", exc.code
            assert str(exc) == (
                "typed UUID parameter `$value` is only supported as a direct node_uuid or "
                "edge_uuid identity equality predicate"
            ), str(exc)
        else:
            raise SystemExit("expected typed UUID property validation failure")
        assert forge.execute("MATCH (n) RETURN n.node_uuid").num_rows == before

    for value in (alice_uuid, [alice_uuid], {"nested": alice_uuid}):
        try:
            forge.execute("MATCH (n:Token) SET n.value = $value", {"value": value})
        except g.ValidationError as exc:
            assert exc.code == "GF_VALIDATION", exc.code
            assert str(exc) == (
                "typed UUID parameter `$value` is only supported as a direct node_uuid or "
                "edge_uuid identity equality predicate"
            ), str(exc)
        else:
            raise SystemExit("expected typed UUID SET validation failure")
        unchanged = forge.execute("MATCH (n:Token) RETURN n.value AS value")
        assert unchanged.column("value").to_pylist() == [alice.uuid]


def check_add_node() -> None:
    # #1298 — construction delegates to Rust and exposes UUID identity only.
    from graphforge._graphforge_rs import NodeHandle

    forge = g.GraphForge()
    handle = forge.add_node("Person", name="Alice", score=7)
    assert isinstance(handle, NodeHandle), type(handle)
    assert handle.label == "Person", handle.label
    assert uuid.UUID(handle.uuid).version == 7, handle.uuid
    assert not hasattr(handle, "id"), "internal node id must not escape"

    table = forge.execute(
        "MATCH (n:Person) RETURN n.node_uuid AS uuid, n.name AS name, n.score AS score"
    )
    assert table.num_rows == 1, table
    assert str(uuid.UUID(bytes=table.column("uuid")[0].as_py())) == handle.uuid
    assert table.column("name").to_pylist() == ["Alice"]
    assert table.column("score").to_pylist() == [7]

    try:
        forge.add_node("Person", unsupported=lambda: None)
    except TypeError:
        pass
    else:
        raise SystemExit("expected TypeError for unsupported node property")
    assert forge.execute("MATCH (n:Person) RETURN n").num_rows == 1


def check_parse_error_span() -> None:
    # #586/#588 — a syntax error surfaces as ParseError with a `span`.
    forge = g.GraphForge()
    try:
        forge.execute("MATCH (n) RETURN n WHERE")
    except g.ParseError as exc:
        assert isinstance(exc, g.GraphForgeError)
        assert hasattr(exc, "span"), "ParseError must carry a span"
        assert isinstance(exc.span, tuple) and len(exc.span) == 2, exc.span
    else:
        raise SystemExit("expected ParseError for invalid Cypher")


def check_explain() -> None:
    # #586 — explain renders the compiler pipeline; GraphIR names the operators.
    forge = g.GraphForge()
    plan = forge.explain("MATCH (n:Person) RETURN n.node_uuid AS id")
    assert "NodeScan" in plan, plan


def check_clear() -> None:
    # #1259 — clear() supports reset-isolated reuse for in-memory fixtures.
    forge = g.GraphForge()
    forge.execute("CREATE (:Person {name: 'Alice'})")
    forge.clear()
    assert forge.execute("MATCH (n) RETURN n").num_rows == 0
    forge.execute("CREATE (:Book {title: 'Graph Databases'})")
    assert forge.execute("MATCH (b:Book) RETURN b.title").num_rows == 1

    # Persistent projects reject destructive reset and preserve their data.
    with tempfile.TemporaryDirectory() as directory:
        project = Path(directory) / "persistent"
        project.mkdir()
        persistent = g.GraphForge(str(project))
        persistent.execute("CREATE (:Person {name: 'Alice'})")
        try:
            persistent.clear()
        except g.StorageError:
            pass
        else:
            raise SystemExit("expected StorageError from persistent clear()")
        assert persistent.execute("MATCH (n:Person) RETURN n.name").num_rows == 1


def _expect_validation_error(call) -> None:
    try:
        call()
    except g.ValidationError:
        return
    raise SystemExit("expected Rust selector validation failure")


def _expect_validation_message(message: str, call) -> None:
    try:
        call()
    except g.ValidationError as error:
        assert str(error) == message
        return
    raise SystemExit(f"expected ValidationError: {message}")


def check_inspection_surface() -> None:
    # #333 — inspection is functional and generic transaction stubs are absent.
    forge = g.GraphForge()
    forge.execute("CREATE (a:Person:Author {name: 'A'})-[:KNOWS]->(:Paper {title: 'Work'})")
    assert forge.labels() == ["Author", "Paper", "Person"]
    assert forge.relationship_types() == ["KNOWS"]
    assert forge.node_count() == 2
    assert forge.node_count("Person") == 1
    assert forge.node_count("Missing') MATCH (n) RETURN n //") == 0
    schema = forge.schema()
    assert schema.schema == pa.schema(
        [
            pa.field("label", pa.string(), nullable=True),
            pa.field("node_count", pa.uint64(), nullable=True),
            pa.field("rel_type", pa.string(), nullable=True),
            pa.field("rel_count", pa.uint64(), nullable=True),
        ]
    )
    assert schema.to_pydict() == {
        "label": ["Author", "Paper", "Person", None],
        "node_count": [1, 1, 1, None],
        "rel_type": [None, None, None, "KNOWS"],
        "rel_count": [None, None, None, 1],
    }
    for name in ("begin", "commit", "rollback"):
        assert not hasattr(forge, name), name


def check_find() -> None:
    # #2308 — find delegates to Rust and converts the canonical Arrow batch.
    forge = g.GraphForge()
    forge.execute("CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'})")
    table = forge.find("alice", label="Person")
    assert isinstance(table, pa.Table), type(table)
    assert table.column_names == ["node_uuid", "name", "score", "matched_on"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.column("name").to_pylist() == ["Alice"]
    assert table.column("matched_on").to_pylist() == ["text"]


def check_degree_rank() -> None:
    with tempfile.TemporaryDirectory() as directory:
        forge = g.GraphForge(directory)
        forge.execute(
            "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
            "(c:Person {name:'Carol'}), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b)"
        )
        table = forge.rank("Person", by="degree")
        assert table.schema.field("node_uuid").type == pa.binary(16)
        assert table.schema.field("score").type == pa.float64()
        assert "node_id" not in table.column_names
        assert table.schema.metadata[b"graphforge.algorithm"] == b"degree"
        assert table.column("name").to_pylist() == ["Alice", "Bob", "Carol"]
        assert table.column("score").to_pylist() == [1.0, 0.0, 0.0]
        assert (
            forge.execute(
                "MATCH (n:Person) WHERE n.degree_score IS NOT NULL RETURN n.degree_score"
            ).num_rows
            == 0
        )
        written = forge.rank("Person", by="degree", write_property="degree_score")
        assert written.equals(table)
        forge.close()

        reopened = g.GraphForge(directory)
        persisted = reopened.execute(
            "MATCH (n:Person) RETURN n.degree_score AS score ORDER BY n.name"
        )
        assert persisted.column("score").to_pylist() == [1.0, 0.0, 0.0]
        _expect_validation_error(lambda: reopened.rank("Person", by="not_a_rank"))
        reopened.close()


def check_pagerank() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (a)-[:KNOWS]->(b), "
        "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(a), (a)-[:OTHER]->(c)"
    )
    table = forge.rank("Person", by="pagerank", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"pagerank"
    assert table.equals(forge.rank("Person", by="pagerank", via="KNOWS"))
    assert abs(sum(table.column("score").to_pylist()) - 1.0) < 1e-9
    assert not table.equals(forge.rank("Person", by="pagerank", directed=False))
    written = forge.rank("Person", by="pagerank", write_property="page_rank")
    assert written.num_rows == 3
    assert (
        forge.execute("MATCH (n:Person) WHERE n.page_rank IS NOT NULL RETURN n.node_uuid").num_rows
        == 3
    )
    assert g.GraphForge().rank("Person", by="pagerank").num_rows == 0


def check_betweenness() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), "
        "(a)-[:KNOWS]->(d), (d)-[:KNOWS]->(c), (b)-[:KNOWS]->(b), "
        "(a)-[:OTHER]->(c)"
    )
    table = forge.rank("Person", by="betweenness", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"betweenness"
    assert table.column("name").to_pylist() == ["Alice", "Bob", "Carol", "Dan"]
    assert table.column("score").to_pylist() == [0.0, 1.0 / 9.0, 0.0, 1.0 / 18.0]
    assert table.equals(forge.rank("Person", by="betweenness", via="KNOWS"))
    assert not table.equals(forge.rank("Person", by="betweenness", directed=False))
    assert not table.equals(forge.rank("Person", by="betweenness"))
    assert forge.rank("Person", by="betweenness", write_property="between").num_rows == 4
    assert (
        forge.execute("MATCH (n:Person) WHERE n.between IS NOT NULL RETURN n.node_uuid").num_rows
        == 4
    )
    assert g.GraphForge().rank("Person", by="betweenness").num_rows == 0


def check_closeness() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), "
        "(b)-[:KNOWS]->(b), (a)-[:OTHER]->(c)"
    )
    table = forge.rank("Person", by="closeness", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"closeness"
    assert table.column("name").to_pylist() == ["Alice", "Bob", "Carol", "Dan"]
    assert table.column("score").to_pylist() == [4.0 / 9.0, 1.0 / 3.0, 0.0, 0.0]
    assert table.equals(forge.rank("Person", by="closeness", via="KNOWS"))
    assert forge.rank("Person", by="closeness", via="KNOWS", directed=False).column(
        "score"
    ).to_pylist() == [
        4.0 / 9.0,
        2.0 / 3.0,
        4.0 / 9.0,
        0.0,
    ]
    assert not table.equals(forge.rank("Person", by="closeness"))
    assert forge.rank("Person", by="closeness", write_property="close_score").num_rows == 4
    assert (
        forge.execute(
            "MATCH (n:Person) WHERE n.close_score IS NOT NULL RETURN n.node_uuid"
        ).num_rows
        == 4
    )
    assert g.GraphForge().rank("Person", by="closeness").num_rows == 0


def check_harmonic_closeness() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), "
        "(b)-[:KNOWS]->(b), (a)-[:OTHER]->(c)"
    )
    table = forge.rank("Person", by="harmonic_closeness", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"harmonic_closeness"
    assert table.column("name").to_pylist() == ["Alice", "Bob", "Carol", "Dan"]
    assert table.column("score").to_pylist() == [0.5, 1.0 / 3.0, 0.0, 0.0]
    assert table.equals(forge.rank("Person", by="harmonic_closeness", via="KNOWS"))
    assert forge.rank("Person", by="harmonic_closeness", via="KNOWS", directed=False).column(
        "score"
    ).to_pylist() == [0.5, 2.0 / 3.0, 0.5, 0.0]
    assert not table.equals(forge.rank("Person", by="harmonic_closeness"))
    assert forge.rank("Person", by="harmonic_closeness", write_property="harmonic").num_rows == 4
    assert (
        forge.execute("MATCH (n:Person) WHERE n.harmonic IS NOT NULL RETURN n.node_uuid").num_rows
        == 4
    )
    assert g.GraphForge().rank("Person", by="harmonic_closeness").num_rows == 0


def check_eigenvector() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(b), "
        "(a)-[:OTHER]->(c)"
    )
    table = forge.rank("Person", by="eigenvector", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"eigenvector"
    assert table.column("name").to_pylist() == ["Alice", "Bob", "Carol", "Dan"]
    ratio = 3.0 * 2.0**20 - 2.0
    norm = (ratio * ratio + 3.0) ** 0.5
    expected = [1.0 / norm, ratio / norm, 1.0 / norm, 1.0 / norm]
    assert all(
        abs(actual - wanted) <= 1.0e-15
        for actual, wanted in zip(table.column("score").to_pylist(), expected, strict=True)
    )
    assert table.equals(forge.rank("Person", by="eigenvector", via="KNOWS"))
    undirected = (
        forge.rank("Person", by="eigenvector", via="KNOWS", directed=False)
        .column("score")
        .to_pylist()
    )
    phi = (1.0 + 5.0**0.5) / 2.0
    principal_norm = (1.0 + phi * phi) ** 0.5
    assert abs(undirected[0] - 1.0 / principal_norm) <= 1.0e-7
    assert abs(undirected[1] - phi / principal_norm) <= 1.0e-7
    assert (
        forge.rank("Person", by="eigenvector").column("score")[2].as_py()
        > table.column("score")[0].as_py()
    )
    assert forge.rank("Person", by="eigenvector", write_property="eigen_score").num_rows == 4
    assert (
        forge.execute(
            "MATCH (n:Person) WHERE n.eigen_score IS NOT NULL RETURN n.node_uuid"
        ).num_rows
        == 4
    )
    assert g.GraphForge().rank("Person", by="eigenvector").num_rows == 0


def check_article_rank() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(a)-[:KNOWS]->(b), (a)-[:OTHER]->(c), "
        "(a)-[:OTHER]->(c), (c)-[:OTHER]->(c)"
    )
    table = forge.rank("Person", by="article_rank", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"article_rank"
    assert table.column("name").to_pylist() == ["Alice", "Bob", "Carol", "Dan"]
    assert all(
        abs(actual - expected) <= 1.0e-15
        for actual, expected in zip(
            table.column("score").to_pylist(), [0.15, 0.252, 0.15, 0.15], strict=True
        )
    )
    assert table.equals(forge.rank("Person", by="article_rank", via="KNOWS"))
    assert (
        forge.rank("Person", by="article_rank", via="KNOWS", directed=False)
        .column("score")
        .to_pylist()
        != table.column("score").to_pylist()
    )
    assert (
        forge.rank("Person", by="article_rank").column("score")[2].as_py()
        > table.column("score")[1].as_py()
    )
    assert forge.rank("Person", by="article_rank", write_property="article_score").num_rows == 4
    assert (
        forge.execute(
            "MATCH (n:Person) WHERE n.article_score IS NOT NULL RETURN n.node_uuid"
        ).num_rows
        == 4
    )
    assert g.GraphForge().rank("Person", by="article_rank").num_rows == 0


def check_hits_hub() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), "
        "(a)-[:OTHER]->(c), (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)"
    )
    table = forge.rank("Person", by="hits_hub", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"hits_hub"
    assert table.column("name").to_pylist() == ["Alice", "Bob", "Carol", "Dan"]
    expected = [1.0 / 2.0**0.5, 1.0 / 2.0**0.5, 0.0, 0.0]
    assert all(
        abs(actual - wanted) <= 1.0e-15
        for actual, wanted in zip(table.column("score").to_pylist(), expected, strict=True)
    )
    assert table.equals(forge.rank("Person", by="hits_hub", via="KNOWS"))
    undirected = (
        forge.rank("Person", by="hits_hub", via="KNOWS", directed=False).column("score").to_pylist()
    )
    assert all(abs(score - 1.0 / 3.0**0.5) <= 1.0e-12 for score in undirected[:3])
    assert forge.rank("Person", by="hits_hub").column("score")[2].as_py() > 0.0
    assert forge.rank("Person", by="hits_hub", write_property="hub_score").num_rows == 4
    assert (
        forge.execute("MATCH (n:Person) WHERE n.hub_score IS NOT NULL RETURN n.node_uuid").num_rows
        == 4
    )
    assert g.GraphForge().rank("Person", by="hits_hub").num_rows == 0


def check_hits_authority() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), "
        "(a)-[:OTHER]->(c), (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)"
    )
    table = forge.rank("Person", by="hits_authority", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"hits_authority"
    assert table.column("name").to_pylist() == ["Alice", "Bob", "Carol", "Dan"]
    expected = [0.0, 1.0 / 2.0**0.5, 1.0 / 2.0**0.5, 0.0]
    scores = table.column("score").to_pylist()
    assert all(
        abs(actual - wanted) <= 1.0e-15 for actual, wanted in zip(scores, expected, strict=True)
    )
    assert table.equals(forge.rank("Person", by="hits_authority", via="KNOWS"))
    undirected = (
        forge.rank("Person", by="hits_authority", via="KNOWS", directed=False)
        .column("score")
        .to_pylist()
    )
    root_six = 6.0**0.5
    expected_undirected = [1.0 / root_six, 2.0 / root_six, 1.0 / root_six, 0.0]
    assert all(
        abs(actual - wanted) <= 1.0e-12
        for actual, wanted in zip(undirected, expected_undirected, strict=True)
    )
    assert forge.rank("Person", by="hits_authority").column("score").to_pylist() != scores
    assert forge.rank("Person", by="hits_authority", write_property="authority_score").num_rows == 4
    assert (
        forge.execute(
            "MATCH (n:Person) WHERE n.authority_score IS NOT NULL RETURN n.node_uuid"
        ).num_rows
        == 4
    )
    assert g.GraphForge().rank("Person", by="hits_authority").num_rows == 0


def check_celf() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), "
        "(a)-[:OTHER]->(c), (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)"
    )
    table = forge.rank("Person", by="celf", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"celf"
    assert table.column("name").to_pylist() == ["Alice", "Bob", "Carol", "Dan"]
    scores = table.column("score").to_pylist()
    assert all(score >= 0.0 for score in scores)
    assert abs(sum(scores) - 4.0) <= 1.0e-12
    assert abs(scores[3] - 1.0) <= 1.0e-12
    assert table.equals(forge.rank("Person", by="celf", via="KNOWS"))
    undirected = (
        forge.rank("Person", by="celf", via="KNOWS", directed=False).column("score").to_pylist()
    )
    assert abs(sum(undirected) - 4.0) <= 1.0e-12 and undirected != scores
    all_edges = forge.rank("Person", by="celf").column("score").to_pylist()
    assert abs(sum(all_edges) - 4.0) <= 1.0e-12 and all_edges != scores
    assert forge.rank("Person", by="celf", write_property="celf_score").num_rows == 4
    assert (
        forge.execute("MATCH (n:Person) WHERE n.celf_score IS NOT NULL RETURN n.node_uuid").num_rows
        == 4
    )
    assert g.GraphForge().rank("Person", by="celf").num_rows == 0


def check_clustering_coefficient() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}), (a)-[:KNOWS]->(b), "
        "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), "
        "(c)-[:KNOWS]->(c), (d)-[:KNOWS]->(e), (a)-[:OTHER]->(d)"
    )
    table = forge.rank("Person", by="clustering_coefficient", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"clustering_coefficient"
    assert table.column("name").to_pylist() == ["Alice", "Bob", "Carol", "Dan", "Eve"]
    scores = table.column("score").to_pylist()
    assert scores == [0.5, 0.5, 0.5, 0.0, 0.0]
    assert table.equals(forge.rank("Person", by="local_clustering_coefficient", via="KNOWS"))
    assert forge.rank("Person", by="clustering_coefficient", via="KNOWS", directed=False).column(
        "score"
    ).to_pylist() == [1.0, 1.0, 1.0, 0.0, 0.0]
    assert forge.rank("Person", by="clustering_coefficient").column("score").to_pylist() != scores
    assert (
        forge.rank("Person", by="clustering_coefficient", write_property="clustering").num_rows == 5
    )
    assert (
        forge.execute("MATCH (n:Person) WHERE n.clustering IS NOT NULL RETURN n.node_uuid").num_rows
        == 5
    )
    assert g.GraphForge().rank("Person", by="clustering_coefficient").num_rows == 0


def check_triangles() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}), (f:Person {name:'Finn'}), "
        "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), "
        "(b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), "
        "(d)-[:KNOWS]->(a), (c)-[:KNOWS]->(c), (e)-[:KNOWS]->(f), "
        "(b)-[:OTHER]->(d)"
    )
    table = forge.rank("Person", by="triangles", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"triangles"
    assert table.column("name").to_pylist() == ["Alice", "Bob", "Carol", "Dan", "Eve", "Finn"]
    scores = table.column("score").to_pylist()
    assert scores == [2.0, 1.0, 2.0, 1.0, 0.0, 0.0]
    assert table.equals(forge.rank("Person", by="triangles", via="KNOWS"))
    assert (
        forge.rank("Person", by="triangles", via="KNOWS", directed=False)
        .column("score")
        .to_pylist()
        == scores
    )
    assert forge.rank("Person", by="triangles").column("score").to_pylist() == [
        3.0,
        3.0,
        3.0,
        3.0,
        0.0,
        0.0,
    ]
    assert forge.rank("Person", by="triangles", write_property="triangle_count").num_rows == 6
    assert (
        forge.execute(
            "MATCH (n:Person) WHERE n.triangle_count IS NOT NULL RETURN n.node_uuid"
        ).num_rows
        == 6
    )
    assert g.GraphForge().rank("Person", by="triangles").num_rows == 0


def check_k_core() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), "
        "(c:Person {name:'C'}), (d:Person {name:'D'}), "
        "(e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(g:Person {name:'G'}), (h:Person {name:'H'}), "
        "(i:Person {name:'I'}), (j:Person {name:'J'}), "
        "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), "
        "(a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), (b)-[:KNOWS]->(c), "
        "(b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), (c)-[:KNOWS]->(c), "
        "(a)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), "
        "(h)-[:KNOWS]->(i), (i)-[:KNOWS]->(j), (j)-[:KNOWS]->(h), "
        "(f)-[:OTHER]->(a)"
    )
    table = forge.rank("Person", by="k_core", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"k_core"
    assert table.column("name").to_pylist() == list("ABCDEFGHIJ")
    scores = table.column("score").to_pylist()
    assert scores == [3.0, 3.0, 3.0, 3.0, 1.0, 1.0, 0.0, 2.0, 2.0, 2.0]
    assert table.equals(forge.rank("Person", by="k_core", via="KNOWS"))
    assert (
        forge.rank("Person", by="k_core", via="KNOWS", directed=False).column("score").to_pylist()
        == scores
    )
    assert forge.rank("Person", by="k_core").column("score").to_pylist() == [
        3.0,
        3.0,
        3.0,
        3.0,
        2.0,
        2.0,
        0.0,
        2.0,
        2.0,
        2.0,
    ]
    assert forge.rank("Person", by="k_core", write_property="core").num_rows == 10
    assert (
        forge.execute("MATCH (n:Person) WHERE n.core IS NOT NULL RETURN n.node_uuid").num_rows == 10
    )
    assert g.GraphForge().rank("Person", by="k_core").num_rows == 0


def check_preferential_attachment() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), "
        "(c:Person {name:'C'}), (d:Person {name:'D'}), "
        "(e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), "
        "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), "
        "(d)-[:KNOWS]->(c), (e)-[:OTHER]->(f)"
    )
    table = forge.rank("Person", by="preferential_attachment", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"preferential_attachment"
    assert table.column("name").to_pylist() == list("ABCDEF")
    assert table.column("score").to_pylist() == [2.0, 3.0, 2.0, 3.0, 0.0, 0.0]
    assert table.equals(forge.rank("Person", by="preferential_attachment", via="KNOWS"))
    assert forge.rank("Person", by="preferential_attachment", via="KNOWS", directed=False).column(
        "score"
    ).to_pylist() == [2.0, 2.0, 0.0, 4.0, 0.0, 0.0]
    assert forge.rank("Person", by="preferential_attachment").column("score").to_pylist() == [
        4.0,
        4.0,
        3.0,
        4.0,
        5.0,
        0.0,
    ]
    assert (
        forge.rank(
            "Person", by="preferential_attachment", via="KNOWS", write_property="pa"
        ).num_rows
        == 6
    )
    persisted = forge.execute(
        "MATCH (n:Person) WHERE n.pa IS NOT NULL RETURN n.name AS name, n.pa AS pa ORDER BY name"
    )
    assert persisted.column("name").to_pylist() == list("ABCDEF")
    assert persisted.column("pa").to_pylist() == [2.0, 3.0, 2.0, 3.0, 0.0, 0.0]
    assert g.GraphForge().rank("Person", by="preferential_attachment").num_rows == 0


def check_adamic_adar() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), "
        "(c:Person {name:'C'}), (d:Person {name:'D'}), "
        "(e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), "
        "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), "
        "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), "
        "(a)-[:OTHER]->(f), (b)-[:OTHER]->(f)"
    )
    table = forge.rank("Person", by="adamic_adar", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"adamic_adar"
    assert table.column("name").to_pylist() == list("ABCDEF")
    inverse_log_two = 1.0 / math.log(2.0)
    expected = [
        2 * inverse_log_two,
        2 * inverse_log_two,
        inverse_log_two,
        inverse_log_two,
        0.0,
        0.0,
    ]
    scores = table.column("score").to_pylist()
    assert all(
        abs(actual - wanted) <= 1.0e-12 for actual, wanted in zip(scores, expected, strict=True)
    )
    assert table.equals(forge.rank("Person", by="adamic_adar", via="KNOWS"))
    assert not table.equals(forge.rank("Person", by="adamic_adar", via="KNOWS", directed=False))
    assert not table.equals(forge.rank("Person", by="adamic_adar"))
    assert (
        forge.rank("Person", by="adamic_adar", via="KNOWS", write_property="adamic").num_rows == 6
    )
    persisted = forge.execute(
        "MATCH (n:Person) WHERE n.adamic IS NOT NULL "
        "RETURN n.name AS name, n.adamic AS score ORDER BY name"
    )
    assert persisted.column("name").to_pylist() == list("ABCDEF")
    assert all(
        abs(actual - wanted) <= 1.0e-12
        for actual, wanted in zip(persisted.column("score").to_pylist(), expected, strict=True)
    )
    assert g.GraphForge().rank("Person", by="adamic_adar").num_rows == 0


def check_common_neighbors() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), "
        "(c:Person {name:'C'}), (d:Person {name:'D'}), "
        "(e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), "
        "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), "
        "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), "
        "(a)-[:OTHER]->(f), (b)-[:OTHER]->(f)"
    )
    table = forge.rank("Person", by="common_neighbors", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"common_neighbors"
    assert table.column("name").to_pylist() == list("ABCDEF")
    assert table.column("score").to_pylist() == [2.0, 2.0, 1.0, 1.0, 0.0, 0.0]
    assert table.equals(forge.rank("Person", by="common_neighbors", via="KNOWS"))
    assert forge.rank("Person", by="common_neighbors", via="KNOWS", directed=False).column(
        "score"
    ).to_pylist() == [4.0, 4.0, 3.0, 3.0, 4.0, 0.0]
    assert forge.rank("Person", by="common_neighbors").column("score").to_pylist() == [
        3.0,
        3.0,
        1.0,
        1.0,
        0.0,
        0.0,
    ]
    assert (
        forge.execute("MATCH (n:Person) WHERE n.common IS NOT NULL RETURN n.node_uuid").num_rows
        == 0
    )
    assert (
        forge.rank("Person", by="common_neighbors", via="KNOWS", write_property="common").num_rows
        == 6
    )
    persisted = forge.execute(
        "MATCH (n:Person) WHERE n.common IS NOT NULL "
        "RETURN n.name AS name, n.common AS score ORDER BY name"
    )
    assert persisted.column("score").to_pylist() == [2.0, 2.0, 1.0, 1.0, 0.0, 0.0]
    assert g.GraphForge().rank("Person", by="common_neighbors").num_rows == 0


def check_resource_allocation() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), "
        "(c:Person {name:'C'}), (d:Person {name:'D'}), "
        "(e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), "
        "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), "
        "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), "
        "(a)-[:OTHER]->(f), (b)-[:OTHER]->(f)"
    )
    table = forge.rank("Person", by="resource_allocation", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"resource_allocation"
    assert table.column("name").to_pylist() == list("ABCDEF")
    assert table.column("score").to_pylist() == [1.0, 1.0, 0.5, 0.5, 0.0, 0.0]
    assert table.equals(forge.rank("Person", by="resource_allocation", via="KNOWS"))
    expected_undirected = [4 / 3, 4 / 3, 1.5, 1.5, 4 / 3, 0.0]
    actual_undirected = (
        forge.rank("Person", by="resource_allocation", via="KNOWS", directed=False)
        .column("score")
        .to_pylist()
    )
    assert all(
        abs(actual - expected) <= 1.0e-12
        for actual, expected in zip(actual_undirected, expected_undirected, strict=True)
    )
    assert forge.rank("Person", by="resource_allocation").column("score").to_pylist() == [
        1.5,
        1.5,
        0.5,
        0.5,
        0.0,
        0.0,
    ]
    assert (
        forge.execute("MATCH (n:Person) WHERE n.resource IS NOT NULL RETURN n.node_uuid").num_rows
        == 0
    )
    assert (
        forge.rank(
            "Person", by="resource_allocation", via="KNOWS", write_property="resource"
        ).num_rows
        == 6
    )
    persisted = forge.execute(
        "MATCH (n:Person) WHERE n.resource IS NOT NULL "
        "RETURN n.name AS name, n.resource AS score ORDER BY name"
    )
    assert persisted.column("score").to_pylist() == [1.0, 1.0, 0.5, 0.5, 0.0, 0.0]
    assert g.GraphForge().rank("Person", by="resource_allocation").num_rows == 0


def check_total_neighbors() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), "
        "(c:Person {name:'C'}), (d:Person {name:'D'}), "
        "(e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), "
        "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), "
        "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), "
        "(a)-[:OTHER]->(f), (b)-[:OTHER]->(f)"
    )
    table = forge.rank("Person", by="total_neighbors", via="KNOWS")
    assert table.column_names == ["node_uuid", "score", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("score").type == pa.float64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"total_neighbors"
    assert table.column("name").to_pylist() == list("ABCDEF")
    assert table.column("score").to_pylist() == [6.0, 6.0, 8.0, 9.0, 7.0, 7.0]
    assert table.equals(forge.rank("Person", by="total_neighbors", via="KNOWS"))
    assert forge.rank("Person", by="total_neighbors", via="KNOWS", directed=False).column(
        "score"
    ).to_pylist() == [6.0, 6.0, 6.0, 6.0, 6.0, 12.0]
    assert forge.rank("Person", by="total_neighbors").column("score").to_pylist() == [
        6.0,
        6.0,
        9.0,
        11.0,
        9.0,
        9.0,
    ]
    assert (
        forge.execute("MATCH (n:Person) WHERE n.total IS NOT NULL RETURN n.node_uuid").num_rows == 0
    )
    assert (
        forge.rank("Person", by="total_neighbors", via="KNOWS", write_property="total").num_rows
        == 6
    )
    persisted = forge.execute(
        "MATCH (n:Person) WHERE n.total IS NOT NULL "
        "RETURN n.name AS name, n.total AS score ORDER BY name"
    )
    assert persisted.column("score").to_pylist() == [6.0, 6.0, 8.0, 9.0, 7.0, 7.0]
    assert g.GraphForge().rank("Person", by="total_neighbors").num_rows == 0


def check_components_cluster() -> None:
    with tempfile.TemporaryDirectory() as directory:
        forge = g.GraphForge(directory)
        forge.execute(
            "CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'}), "
            "(c:Person {name:'Carol'})"
        )
        table = forge.cluster("Person", by="components")
        assert table.schema.field("node_uuid").type == pa.binary(16)
        assert table.schema.field("community_id").type == pa.int64()
        assert "node_id" not in table.column_names
        assert table.schema.metadata[b"graphforge.algorithm"] == b"components"
        assert table.column("name").to_pylist() == ["Alice", "Bob", "Carol"]
        assert table.column("community_id").to_pylist() == [0, 0, 1]
        assert (
            forge.execute(
                "MATCH (n:Person) WHERE n.component IS NOT NULL RETURN n.component"
            ).num_rows
            == 0
        )
        forge.execute("MATCH (n:Person {name:'Alice'}) SET n.atomic_component = 'old'")
        try:
            forge.cluster("Person", by="components", write_property="atomic_component")
        except g.ValidationError as exc:
            assert "collides with existing Utf8 data" in str(exc)
        else:
            raise SystemExit("expected atomic component collision")
        assert forge.execute(
            "MATCH (n:Person) WHERE n.atomic_component IS NOT NULL "
            "RETURN n.atomic_component AS value"
        ).column("value").to_pylist() == ["old"]
        written = forge.cluster("Person", by="components", write_property="component")
        assert written.column("node_uuid").equals(table.column("node_uuid"))
        assert written.column("community_id").equals(table.column("community_id"))
        forge.close()

        reopened = g.GraphForge(directory)
        persisted = reopened.execute(
            "MATCH (n:Person) RETURN n.component AS component ORDER BY n.name"
        )
        assert persisted.column("component").to_pylist() == [0, 0, 1]

        def expect_cluster_validation(target, by, vector_property, message) -> None:
            try:
                target.cluster("Person", by=by, vector_property=vector_property)
            except g.ValidationError as exc:
                assert str(exc) == message
            else:
                raise SystemExit(f"expected Rust cluster validation for {by}")

        cases = [
            (reopened, "hdbscan", None, "cluster.hdbscan requires vector_property"),
            (reopened, "hdbscan", " ", 'invalid cluster vector property " "'),
            (
                reopened,
                "components",
                "features",
                "cluster.components does not accept vector_property",
            ),
        ]
        for case in cases:
            expect_cluster_validation(*case)
        reopened.close()


def check_strongly_connected_cluster() -> None:
    assert g.GraphForge().cluster("Person", by="strongly_connected").num_rows == 0
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), "
        "(c:Person {name:'c'}), (d:Person {name:'d'}), "
        "(e:Person {name:'e'}), (f:Person {name:'f'}), (g:Person {name:'g'}), "
        "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(b), "
        "(b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), "
        "(d)-[:KNOWS]->(e), (e)-[:KNOWS]->(d), (e)-[:KNOWS]->(f), "
        "(f)-[:OTHER]->(a)"
    )
    table = forge.cluster("Person", by="strongly_connected", via="KNOWS", directed=True)
    assert table.column_names == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert not table.schema.field("node_uuid").nullable
    assert not table.schema.field("community_id").nullable
    assert table.schema.metadata[b"graphforge.algorithm"] == b"strongly_connected"
    assert table.column("name").to_pylist() == list("abcdefg")
    assert table.column("community_id").to_pylist() == [0, 0, 0, 1, 1, 2, 3]
    expected = forge.execute("MATCH (n:Person) RETURN n.node_uuid AS node_uuid ORDER BY n.name")
    assert table.column("node_uuid").equals(expected.column("node_uuid"))
    assert forge.cluster("Person", by="strongly_connected", via="KNOWS", directed=False).column(
        "community_id"
    ).to_pylist() == [0, 0, 0, 0, 0, 0, 1]
    assert forge.cluster("Person", by="strongly_connected", directed=True).column(
        "community_id"
    ).to_pylist() == [0, 0, 0, 0, 0, 0, 1]
    assert (
        forge.execute("MATCH (n:Person) WHERE n.scc IS NOT NULL RETURN n.node_uuid").num_rows == 0
    )

    forge.execute("MATCH (n:Person {name:'a'}) SET n.atomic_scc = 'old'")

    def expect_validation(message, **kwargs) -> None:
        try:
            forge.cluster("Person", by="strongly_connected", **kwargs)
        except g.ValidationError as exc:
            assert message in str(exc)
        else:
            raise SystemExit("expected structured strongly_connected validation")

    expect_validation(
        'write_property "atomic_scc" collides with existing Utf8 data',
        via="KNOWS",
        directed=True,
        write_property="atomic_scc",
    )
    expect_validation(
        "cluster.strongly_connected does not accept vector_property",
        vector_property="features",
    )
    unchanged = forge.execute(
        "MATCH (n:Person) WHERE n.atomic_scc IS NOT NULL RETURN n.atomic_scc AS value"
    )
    assert unchanged.column("value").to_pylist() == ["old"]
    written = forge.cluster(
        "Person", by="strongly_connected", via="KNOWS", directed=True, write_property="scc"
    )
    assert written.column("community_id").to_pylist() == [0, 0, 0, 1, 1, 2, 3]
    persisted = forge.execute("MATCH (n:Person) RETURN n.scc AS scc ORDER BY n.name")
    assert persisted.column("scc").to_pylist() == [0, 0, 0, 1, 1, 2, 3]


def check_biconnected_cluster() -> None:
    assert g.GraphForge().cluster("Person", by="biconnected").num_rows == 0
    edgeless = g.GraphForge()
    edgeless.execute("CREATE (:Person {name:'a'}), (:Person {name:'b'})")
    assert edgeless.cluster("Person", by="biconnected").column("community_id").to_pylist() == [0, 1]

    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), "
        "(c:Person {name:'c'}), (d:Person {name:'d'}), "
        "(e:Person {name:'e'}), (f:Person {name:'f'}), (g:Person {name:'g'}), "
        "(a)-[:KNOWS {weight:99}]->(b), (a)-[:KNOWS]->(b), "
        "(b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), "
        "(d)-[:KNOWS]->(e), (e)-[:KNOWS]->(c), (e)-[:KNOWS]->(f), "
        "(f)-[:KNOWS]->(f), (g)-[:OTHER]->(a)"
    )
    table = forge.cluster("Person", by="biconnected", via="KNOWS", directed=True)
    assert table.column_names == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert not table.schema.field("node_uuid").nullable
    assert not table.schema.field("community_id").nullable
    assert table.schema.metadata[b"graphforge.algorithm"] == b"biconnected"
    assert table.column("name").to_pylist() == list("abcdefg")
    assert table.column("community_id").to_pylist() == [0, 0, 0, 1, 1, 2, 3]
    expected = forge.execute("MATCH (n:Person) RETURN n.node_uuid AS node_uuid ORDER BY n.name")
    assert table.column("node_uuid").equals(expected.column("node_uuid"))
    assert (
        forge.cluster("Person", by="biconnected", via="KNOWS", directed=False)
        .column("community_id")
        .to_pylist()
        == table.column("community_id").to_pylist()
    )
    assert forge.cluster("Person", by="biconnected", via="OTHER").column(
        "community_id"
    ).to_pylist() == [0, 1, 2, 3, 4, 5, 0]
    assert forge.execute("MATCH (n:Person) WHERE n.block IS NOT NULL RETURN n").num_rows == 0

    forge.execute("MATCH (n:Person {name:'a'}) SET n.atomic_block = 'old'")

    def expect_validation(message, **kwargs) -> None:
        try:
            forge.cluster("Person", by="biconnected", **kwargs)
        except g.ValidationError as exc:
            assert message in str(exc)
        else:
            raise SystemExit("expected structured biconnected validation")

    expect_validation(
        'write_property "atomic_block" collides with existing Utf8 data',
        via="KNOWS",
        directed=True,
        write_property="atomic_block",
    )
    expect_validation(
        "cluster.biconnected does not accept vector_property",
        vector_property="features",
    )
    unchanged = forge.execute(
        "MATCH (n:Person) WHERE n.atomic_block IS NOT NULL RETURN n.atomic_block AS value"
    )
    assert unchanged.column("value").to_pylist() == ["old"]
    written = forge.cluster(
        "Person", by="biconnected", via="KNOWS", directed=True, write_property="block"
    )
    assert written.column("community_id").to_pylist() == [0, 0, 0, 1, 1, 2, 3]
    persisted = forge.execute("MATCH (n:Person) RETURN n.block AS block ORDER BY n.name")
    assert persisted.column("block").to_pylist() == [0, 0, 0, 1, 1, 2, 3]


def check_k_core_decomposition_cluster() -> None:
    assert g.GraphForge().cluster("Person", by="k_core_decomposition").num_rows == 0
    edgeless = g.GraphForge()
    edgeless.execute("CREATE (:Person {name:'a'}), (:Person {name:'b'})")
    assert edgeless.cluster("Person", by="k_core_decomposition").column(
        "community_id"
    ).to_pylist() == [0, 0]

    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), "
        "(c:Person {name:'c'}), (d:Person {name:'d'}), "
        "(e:Person {name:'e'}), (f:Person {name:'f'}), "
        "(g:Person {name:'g'}), (h:Person {name:'h'}), "
        "(i:Person {name:'i'}), (j:Person {name:'j'}), "
        "(a)-[:KNOWS {weight:99}]->(b), (a)-[:KNOWS]->(b), "
        "(b)-[:KNOWS]->(a), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), "
        "(b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), "
        "(c)-[:KNOWS]->(c), (a)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), "
        "(h)-[:KNOWS]->(i), (i)-[:KNOWS]->(j), (j)-[:KNOWS]->(h), "
        "(f)-[:OTHER]->(a)"
    )
    table = forge.cluster("Person", by="k_core_decomposition", via="KNOWS", directed=True)
    assert table.column_names == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert not table.schema.field("node_uuid").nullable
    assert not table.schema.field("community_id").nullable
    assert table.schema.metadata[b"graphforge.algorithm"] == b"k_core_decomposition"
    assert table.column("name").to_pylist() == list("abcdefghij")
    expected_cores = [3, 3, 3, 3, 1, 1, 0, 2, 2, 2]
    assert table.column("community_id").to_pylist() == expected_cores
    identities = forge.execute("MATCH (n:Person) RETURN n.node_uuid AS uuid ORDER BY n.name")
    assert table.column("node_uuid").equals(identities.column("uuid"))
    assert (
        forge.cluster("Person", by="k_core_decomposition", via="KNOWS", directed=False)
        .column("community_id")
        .to_pylist()
        == expected_cores
    )
    assert forge.cluster("Person", by="k_core_decomposition", via="OTHER").column(
        "community_id"
    ).to_pylist() == [1, 0, 0, 0, 0, 1, 0, 0, 0, 0]
    assert forge.execute("MATCH (n:Person) WHERE n.core IS NOT NULL RETURN n").num_rows == 0

    forge.execute("MATCH (n:Person {name:'a'}) SET n.atomic_core = 'old'")
    _expect_validation_error(
        lambda: forge.cluster(
            "Person",
            by="k_core_decomposition",
            via="KNOWS",
            write_property="atomic_core",
        )
    )
    _expect_validation_error(
        lambda: forge.cluster("Person", by="k_core_decomposition", vector_property="features")
    )
    unchanged = forge.execute(
        "MATCH (n:Person) WHERE n.atomic_core IS NOT NULL RETURN n.atomic_core AS value"
    )
    assert unchanged.column("value").to_pylist() == ["old"]
    written = forge.cluster("Person", by="k_core_decomposition", via="KNOWS", write_property="core")
    assert written.column("community_id").to_pylist() == expected_cores
    persisted = forge.execute("MATCH (n:Person) RETURN n.core AS core ORDER BY n.name")
    assert persisted.column("core").to_pylist() == expected_cores


def check_louvain_cluster() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), "
        "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), "
        "(b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), "
        "(a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), "
        "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), (f)-[:KNOWS]->(f), "
        "(a)-[:OTHER]->(g)"
    )
    table = forge.cluster("Person", by="louvain", via="KNOWS", directed=True)
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert "node_id" not in table.column_names
    assert table.schema.metadata[b"graphforge.algorithm"] == b"louvain"
    assert table.column("name").to_pylist() == list("ABCDEFG")
    assert table.column("community_id").to_pylist() == [0, 0, 0, 1, 1, 1, 2]
    assert table.equals(forge.cluster("Person", by="louvain", via="KNOWS", directed=False))
    assert forge.cluster("Person", by="louvain").column("community_id").to_pylist() == [
        0,
        0,
        0,
        1,
        1,
        1,
        0,
    ]
    assert (
        forge.execute("MATCH (n:Person) WHERE n.group_id IS NOT NULL RETURN n.node_uuid").num_rows
        == 0
    )
    written = forge.cluster("Person", by="louvain", via="KNOWS", write_property="group_id")
    assert written.column("community_id").to_pylist() == [0, 0, 0, 1, 1, 1, 2]
    persisted = forge.execute("MATCH (n:Person) RETURN n.group_id AS id ORDER BY id, n.name")
    assert persisted.column("id").to_pylist() == [0, 0, 0, 1, 1, 1, 2]
    assert g.GraphForge().cluster("Person", by="louvain").num_rows == 0


def check_leiden_cluster() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), "
        "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(g:Person {name:'G'}), (h:Person {name:'H'}), (a)-[:KNOWS]->(e), "
        "(a)-[:KNOWS]->(e), (e)-[:KNOWS]->(a), (a)-[:KNOWS]->(g), "
        "(b)-[:KNOWS]->(c), (b)-[:KNOWS]->(f), (b)-[:KNOWS]->(g), "
        "(c)-[:KNOWS]->(g), (d)-[:KNOWS]->(g), (e)-[:KNOWS]->(g), "
        "(f)-[:KNOWS]->(g), (a)-[:KNOWS]->(a), (a)-[:OTHER]->(h)"
    )
    table = forge.cluster("Person", by="leiden", via="KNOWS", directed=True)
    assert table.column_names == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"leiden"
    assert table.column("name").to_pylist() == list("ABCDEFGH")
    expected = [0, 1, 1, 0, 0, 1, 0, 2]
    assert table.column("community_id").to_pylist() == expected
    assert table.equals(forge.cluster("Person", by="leiden", via="KNOWS", directed=False))
    assert (
        forge.execute("MATCH (n:Person) WHERE n.group_id IS NOT NULL RETURN n.node_uuid").num_rows
        == 0
    )
    written = forge.cluster("Person", by="leiden", via="KNOWS", write_property="group_id")
    assert written.column("community_id").to_pylist() == expected
    persisted = forge.execute("MATCH (n:Person) RETURN n.group_id AS id ORDER BY n.name")
    assert persisted.column("id").to_pylist() == expected
    assert g.GraphForge().cluster("Person", by="leiden").num_rows == 0


def check_label_propagation_cluster() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), "
        "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), "
        "(b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), "
        "(a)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), "
        "(f)-[:KNOWS]->(d), (f)-[:KNOWS]->(f), (c)-[:OTHER]->(d)"
    )
    table = forge.cluster("Person", by="label_propagation", via="KNOWS", directed=True)
    assert table.column_names == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"label_propagation"
    assert table.column("name").to_pylist() == list("ABCDEFG")
    expected = [0, 0, 0, 1, 1, 1, 2]
    assert table.column("community_id").to_pylist() == expected
    assert table.equals(
        forge.cluster("Person", by="label_propagation", via="KNOWS", directed=False)
    )
    assert (
        forge.execute("MATCH (n:Person) WHERE n.group_id IS NOT NULL RETURN n.node_uuid").num_rows
        == 0
    )
    written = forge.cluster(
        "Person", by="label_propagation", via="KNOWS", write_property="group_id"
    )
    assert written.column("community_id").to_pylist() == expected
    persisted = forge.execute("MATCH (n:Person) RETURN n.group_id AS id ORDER BY n.name")
    assert persisted.column("id").to_pylist() == expected
    assert g.GraphForge().cluster("Person", by="label_propagation").num_rows == 0


def check_speaker_listener_cluster() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), "
        "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), "
        "(b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), "
        "(a)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), "
        "(f)-[:KNOWS]->(d), (f)-[:KNOWS]->(f), (c)-[:OTHER]->(d)"
    )
    table = forge.cluster("Person", by="speaker_listener", via="KNOWS", directed=True)
    assert [field.name for field in table.schema] == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"speaker_listener"
    assert table.column("name").to_pylist() == list("ABCDEFG")
    expected = [0, 0, 0, 1, 1, 1, 2]
    assert table.column("community_id").to_pylist() == expected
    assert table.equals(forge.cluster("Person", by="speaker_listener", via="KNOWS", directed=False))
    assert (
        forge.execute("MATCH (n:Person) WHERE n.slpa_group IS NOT NULL RETURN n.node_uuid").num_rows
        == 0
    )
    written = forge.cluster(
        "Person", by="speaker_listener", via="KNOWS", write_property="slpa_group"
    )
    assert written.column("community_id").to_pylist() == expected
    persisted = forge.execute("MATCH (n:Person) RETURN n.slpa_group AS id ORDER BY n.name")
    assert persisted.column("id").to_pylist() == expected
    assert g.GraphForge().cluster("Person", by="speaker_listener").num_rows == 0


def check_girvan_newman_cluster() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), "
        "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), "
        "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), "
        "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)"
    )
    table = forge.cluster("Person", by="girvan_newman", via="KNOWS", directed=True)
    assert [field.name for field in table.schema] == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"girvan_newman"
    assert table.column("name").to_pylist() == list("ABCDEFG")
    assert table.column("community_id").to_pylist() == [0, 0, 0, 1, 1, 1, 2]


def check_modularity_optimization_cluster() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), "
        "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), "
        "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), "
        "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)"
    )
    table = forge.cluster("Person", by="modularity_optimization", via="KNOWS", directed=True)
    assert [field.name for field in table.schema] == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"modularity_optimization"
    assert table.column("name").to_pylist() == list("ABCDEFG")
    assert table.column("community_id").to_pylist() == [0, 0, 0, 1, 1, 1, 2]


def check_fastgreedy_cluster() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), "
        "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), "
        "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), "
        "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)"
    )
    table = forge.cluster("Person", by="fastgreedy", via="KNOWS", directed=True)
    assert [field.name for field in table.schema] == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"fastgreedy"
    assert table.column("name").to_pylist() == list("ABCDEFG")
    assert table.column("community_id").to_pylist() == [0, 0, 0, 1, 1, 1, 2]


def check_infomap_cluster() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), "
        "(d:Person {name:'D'}), (e:Person {name:'E'}), (a)-[:KNOWS]->(b), "
        "(b)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(c)"
    )
    table = forge.cluster("Person", by="infomap", via="KNOWS", directed=True)
    assert [field.name for field in table.schema] == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"infomap"
    assert table.column("name").to_pylist() == list("ABCDE")
    assert table.column("community_id").to_pylist() == [0, 0, 1, 1, 2]


def check_leading_eigenvector_cluster() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), "
        "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), "
        "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), "
        "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)"
    )
    table = forge.cluster("Person", by="leading_eigenvector", via="KNOWS", directed=True)
    assert [field.name for field in table.schema] == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"leading_eigenvector"
    assert table.column("name").to_pylist() == list("ABCDEFG")
    assert table.column("community_id").to_pylist() == [0, 0, 0, 1, 1, 1, 2]


def check_walktrap_cluster() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), "
        "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), "
        "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), "
        "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)"
    )
    table = forge.cluster("Person", by="walktrap", via="KNOWS", directed=True)
    assert [field.name for field in table.schema] == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"walktrap"
    assert table.column("name").to_pylist() == list("ABCDEFG")
    assert table.column("community_id").to_pylist() == [0, 0, 0, 1, 1, 1, 2]


def check_spinglass_cluster() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), "
        "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), "
        "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), "
        "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)"
    )
    table = forge.cluster("Person", by="spinglass", via="KNOWS", directed=True)
    assert [field.name for field in table.schema] == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"spinglass"
    assert table.column("name").to_pylist() == list("ABCDEFG")
    assert table.column("community_id").to_pylist() == [0, 0, 0, 1, 1, 1, 2]


def check_hdbscan_cluster() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (:Point {name:'a0', features:[0.0]}), (:Point {name:'a1', features:[0.1]}), "
        "(:Point {name:'a2', features:[0.2]}), (:Point {name:'a3', features:[0.3]}), "
        "(:Point {name:'a4', features:[0.4]}), (:Point {name:'b0', features:[10.0]}), "
        "(:Point {name:'b1', features:[10.1]}), (:Point {name:'b2', features:[10.2]}), "
        "(:Point {name:'b3', features:[10.3]}), (:Point {name:'b4', features:[10.4]}), "
        "(:Point {name:'noise', features:[100.0]})"
    )
    table = forge.cluster("Point", by="hdbscan", directed=True, vector_property="features")
    assert [field.name for field in table.schema] == [
        "node_uuid",
        "community_id",
        "features",
        "name",
    ]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"hdbscan"
    assert table.column("name").to_pylist() == [
        "a0",
        "a1",
        "a2",
        "a3",
        "a4",
        "b0",
        "b1",
        "b2",
        "b3",
        "b4",
        "noise",
    ]
    assert table.column("community_id").to_pylist() == [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, -1]
    assert table.equals(
        forge.cluster("Point", by="hdbscan", directed=False, vector_property="features")
    )


def check_kmeans_cluster() -> None:
    forge = g.GraphForge()
    values = [group * 10.0 + offset * 0.25 for group in range(10) for offset in range(2)]
    nodes = ",".join(
        f"(:Point {{name:'p{point:02}', features:[{value:.2f}]}})"
        for point, value in enumerate(values)
    )
    forge.execute(f"CREATE {nodes}")
    table = forge.cluster("Point", by="k_means", directed=True, vector_property="features")
    assert [field.name for field in table.schema] == [
        "node_uuid",
        "community_id",
        "features",
        "name",
    ]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert table.schema.metadata[b"graphforge.algorithm"] == b"k_means"
    assert "node_id" not in table.column_names
    assert len(set(table.column("node_uuid").to_pylist())) == 20
    assert table.column("name").to_pylist() == [f"p{point:02}" for point in range(20)]
    assert table.column("features").to_pylist() == [[value] for value in values]
    expected = [group for group in range(10) for _ in range(2)]
    assert table.column("community_id").to_pylist() == expected
    assert table.equals(
        forge.cluster("Point", by="k_means", directed=False, vector_property="features")
    )
    assert forge.execute("MATCH (p:Point) WHERE p.community IS NOT NULL RETURN p").num_rows == 0

    forge.execute("MATCH (p:Point {name:'p00'}) SET p.atomic_group = 'old'")
    try:
        forge.cluster(
            "Point",
            by="k_means",
            vector_property="features",
            write_property="atomic_group",
        )
    except g.ValidationError:
        pass
    else:
        raise SystemExit("expected atomic K-means write rejection")
    unchanged = forge.execute(
        "MATCH (p:Point) WHERE p.atomic_group IS NOT NULL RETURN p.atomic_group AS value"
    )
    assert unchanged.column("value").to_pylist() == ["old"]
    forge.cluster(
        "Point",
        by="k_means",
        vector_property="features",
        write_property="community",
    )
    written = forge.execute("MATCH (p:Point) RETURN p.community AS value ORDER BY p.name")
    assert written.column("value").to_pylist() == expected

    def expect_validation(**kwargs) -> None:
        try:
            forge.cluster("Point", by="k_means", **kwargs)
        except g.ValidationError:
            pass
        else:
            raise SystemExit("expected structured K-means validation error")

    expect_validation()
    expect_validation(vector_property="features", via="KNOWS")
    small = g.GraphForge()
    small.execute("CREATE (:Point {features:[0.0]}), (:Point {features:[1.0]})")
    try:
        small.cluster("Point", by="k_means", vector_property="features")
    except g.ExecutionError:
        pass
    else:
        raise SystemExit("expected structured small K-means failure")


def check_approximate_max_cut_cluster() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), "
        "(c:Person {name:'c'}), (d:Person {name:'d'}), "
        "(e:Person {name:'e'}), (a)-[:KNOWS]->(b), "
        "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(b), "
        "(b)-[:KNOWS]->(c), (c)-[:KNOWS]->(d), "
        "(d)-[:KNOWS]->(a), (a)-[:OTHER]->(e)"
    )

    def cluster(directed, write_property=None):
        return forge.cluster(
            "Person",
            by="approximate_max_k_cut",
            via="KNOWS",
            directed=directed,
            write_property=write_property,
        )

    table = cluster(True)
    assert table.column_names == ["node_uuid", "community_id", "name"]
    assert table.schema.field("node_uuid").type == pa.binary(16)
    assert table.schema.field("community_id").type == pa.int64()
    assert not table.schema.field("node_uuid").nullable
    assert not table.schema.field("community_id").nullable
    assert table.schema.metadata[b"graphforge.algorithm"] == b"approximate_max_k_cut"
    assert "node_id" not in table.column_names
    assert table.column("name").to_pylist() == list("abcde")
    assert table.column("community_id").to_pylist() == [0, 1, 0, 1, 0]
    uuids = table.column("node_uuid").to_pylist()
    assert len(set(uuids)) == 5
    expected = forge.execute("MATCH (p:Person) RETURN p.node_uuid AS node_uuid ORDER BY p.name")
    assert uuids == expected.column("node_uuid").to_pylist()
    assert table.equals(cluster(False))
    assert forge.cluster("Person", by="approximate_max_k_cut").column(
        "community_id"
    ).to_pylist() == [0, 1, 0, 1, 1]
    assert forge.execute("MATCH (p:Person) WHERE p.cut IS NOT NULL RETURN p").num_rows == 0

    forge.execute("MATCH (p:Person {name:'a'}) SET p.atomic_cut = 'old'")
    try:
        cluster(False, "atomic_cut")
    except g.ValidationError:
        pass
    else:
        raise SystemExit("expected atomic approximate max-cut write rejection")
    unchanged = forge.execute(
        "MATCH (p:Person) WHERE p.atomic_cut IS NOT NULL RETURN p.atomic_cut AS value"
    )
    assert unchanged.column("value").to_pylist() == ["old"]
    cluster(False, "cut")
    written = forge.execute("MATCH (p:Person) RETURN p.cut AS value ORDER BY p.name")
    assert written.column("value").to_pylist() == [0, 1, 0, 1, 0]

    try:
        forge.cluster("Person", by="approximate_max_k_cut", vector_property="features")
    except g.ValidationError:
        pass
    else:
        raise SystemExit("expected structured approximate max-cut validation error")

    oversized = g.GraphForge()
    oversized.execute("CREATE " + ",".join("(:Oversized)" for _ in range(4097)))
    try:
        oversized.cluster("Oversized", by="approximate_max_k_cut")
    except g.ExecutionError as exc:
        assert str(exc) == "algorithm node limit exceeded: observed 4097, limit 4096"
    else:
        raise SystemExit("expected structured approximate max-cut resource error")


def check_node_similarity() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), (e:Person {name:'Eve'}), "
        "(a)-[:KNOWS]->(d), (a)-[:KNOWS]->(e), (a)-[:KNOWS]->(e), "
        "(b)-[:KNOWS]->(d), (b)-[:KNOWS]->(e), (c)-[:KNOWS]->(d), "
        "(a)-[:OTHER]->(d), (c)-[:OTHER]->(d)"
    )
    table = forge.similar("Person", by="node_similarity", k=2, via="KNOWS")
    assert table.schema.field("node1_uuid").type == pa.binary(16)
    assert table.schema.field("node2_uuid").type == pa.binary(16)
    assert table.schema.field("similarity").type == pa.float64()
    assert "node1_id" not in table.column_names
    assert table.schema.metadata[b"graphforge.algorithm"] == b"node_similarity"
    identities = (
        forge.execute("MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name")
        .column("uuid")
        .to_pylist()
    )
    expected = [(0, 1), (0, 2), (1, 0), (1, 2), (2, 0), (2, 1)]
    assert list(
        zip(table.column("node1_uuid").to_pylist(), table.column("node2_uuid").to_pylist())
    ) == [(identities[left], identities[right]) for left, right in expected]
    assert table.column("similarity").to_pylist() == [1.0, 0.5, 1.0, 0.5, 0.5, 0.5]
    assert forge.similar("Person", by="node_similarity", k=1, via="KNOWS").num_rows == 3
    assert forge.similar("Person", by="node_similarity", via="OTHER").num_rows == 2
    assert g.GraphForge().similar("Person", by="node_similarity").num_rows == 0
    _expect_validation_error(lambda: forge.similar("Person", by="node_similarity", k=0))
    _expect_validation_error(
        lambda: forge.similar("Person", by="node_similarity", vector_property="v")
    )
    _expect_validation_error(lambda: forge.similar("Person", by="knn"))


def check_filtered_node_similarity() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), (e:Person {name:'Eve'}), "
        "(a)-[:KNOWS]->(a), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), "
        "(a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), (b)-[:KNOWS]->(a), "
        "(b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), "
        "(d)-[:KNOWS]->(c)"
    )
    run = lambda: forge.similar(  # noqa: E731
        "Person", by="filtered_node_similarity", k=2, via="KNOWS"
    )
    table = run()
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("node1_uuid", pa.binary(16), False),
        ("node2_uuid", pa.binary(16), False),
        ("similarity", pa.float64(), False),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"filtered_node_similarity"
    assert table.schema.metadata[b"graphforge.verb"] == b"similar"
    identities = (
        forge.execute("MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name")
        .column("uuid")
        .to_pylist()
    )
    pairs = list(
        zip(table.column("node1_uuid").to_pylist(), table.column("node2_uuid").to_pylist())
    )
    expected = [(0, 1), (0, 2), (1, 0), (1, 2)]
    assert pairs == [(identities[left], identities[right]) for left, right in expected]
    expected_scores = [0.75, 0.25, 0.75, 1.0 / 3.0]
    assert all(
        abs(actual - expected) < 1e-12
        for actual, expected in zip(table.column("similarity").to_pylist(), expected_scores)
    )
    assert table.equals(run())
    assert forge.similar("Person", by="filtered_node_similarity", via="KNOWS").num_rows == 6
    assert forge.similar("Person", by="filtered_node_similarity", k=2).num_rows == 4
    assert forge.similar("Person", by="filtered_node_similarity", k=2, via="MISSING").num_rows == 0

    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (c:Person {name:'Carol'}) CREATE (c)-[:KNOWS]->(a)"
    )
    assert run().num_rows == 5
    assert g.GraphForge().similar("Person", by="filtered_node_similarity").num_rows == 0
    for kwargs in [
        {"by": "filtered_node_similarity", "k": 0},
        {"by": "filtered_node_similarity", "via": " "},
        {"by": "filtered_node_similarity", "vector_property": "embedding"},
    ]:
        _expect_validation_error(lambda kwargs=kwargs: forge.similar("Person", **kwargs))


def check_knn() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (:Person {name:'a', embedding:[1.0, 0.0]}), "
        "(:Person {name:'b', embedding:[1.0, 0.0]}), "
        "(:Person {name:'c', embedding:[1.0, 1.0]}), "
        "(:Person {name:'d', embedding:[0.0, 1.0]}), "
        "(:Person {name:'e', embedding:[-1.0, 0.0]})"
    )
    table = forge.similar("Person", by="knn", k=2, vector_property="embedding")
    assert [field.name for field in table.schema] == [
        "node1_uuid",
        "node2_uuid",
        "similarity",
    ]
    assert [field.type for field in table.schema] == [pa.binary(16), pa.binary(16), pa.float64()]
    assert all(not field.nullable for field in table.schema)
    assert table.schema.metadata[b"graphforge.algorithm"] == b"knn"
    assert table.schema.metadata[b"graphforge.verb"] == b"similar"
    assert table.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
    identities = (
        forge.execute("MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name")
        .column("uuid")
        .to_pylist()
    )
    expected = [(0, 1), (0, 2), (1, 0), (1, 2), (2, 0), (2, 1), (3, 2), (3, 0), (4, 3)]
    pairs = list(
        zip(table.column("node1_uuid").to_pylist(), table.column("node2_uuid").to_pylist())
    )
    assert pairs == [(identities[left], identities[right]) for left, right in expected]
    scores = table.column("similarity").to_pylist()
    expected_scores = [1.0, 2**-0.5, 1.0, 2**-0.5, 2**-0.5, 2**-0.5, 2**-0.5, 0.0, 0.0]
    assert all(abs(actual - expected) < 1e-12 for actual, expected in zip(scores, expected_scores))
    assert forge.similar("Person", by="knn", vector_property="embedding").num_rows == 14

    forge.execute(
        "MATCH (a:Person {name:'a'}), (b:Person {name:'b'}) "
        "CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a)"
    )
    assert table.equals(forge.similar("Person", by="knn", k=2, vector_property="embedding"))
    assert g.GraphForge().similar("Person", by="knn", vector_property="embedding").num_rows == 0
    _expect_validation_error(lambda: forge.similar("Person", by="knn"))
    _expect_validation_error(
        lambda: forge.similar("Person", by="knn", vector_property="embedding", via="KNOWS")
    )
    zero = g.GraphForge()
    zero.execute("CREATE (:Person {embedding:[0.0, 0.0]})")
    _expect_validation_error(lambda: zero.similar("Person", by="knn", vector_property="embedding"))
    ragged = g.GraphForge()
    ragged.execute("CREATE (:Person {embedding:[1.0]}), (:Person {embedding:[1.0, 2.0]})")
    _expect_validation_error(
        lambda: ragged.similar("Person", by="knn", vector_property="embedding")
    )


def check_filtered_knn() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (:Person {name:'a', embedding:[1.0, 0.0]}), "
        "(:Person {name:'b', embedding:[1.0, 0.0]}), "
        "(:Person {name:'c', embedding:[1.0, 1.0]}), "
        "(:Person {name:'d', embedding:[0.0, 1.0]}), "
        "(:Person {name:'e', embedding:[-1.0, 0.0]})"
    )
    forge.execute(
        "MATCH (a:Person {name:'a'}), (b:Person {name:'b'}), "
        "(c:Person {name:'c'}), (d:Person {name:'d'}), (e:Person {name:'e'}) "
        "CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), "
        "(a)-[:KNOWS]->(a), (a)-[:OTHER]->(e), (b)-[:OTHER]->(a), "
        "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(b), (d)-[:KNOWS]->(c), "
        "(d)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(d)"
    )
    run = lambda: forge.similar(  # noqa: E731
        "Person", by="filtered_knn", k=2, vector_property="embedding", via="KNOWS"
    )
    table = run()
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("node1_uuid", pa.binary(16), False),
        ("node2_uuid", pa.binary(16), False),
        ("similarity", pa.float64(), False),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"filtered_knn"
    assert table.schema.metadata[b"graphforge.verb"] == b"similar"
    assert table.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
    identities = (
        forge.execute("MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name")
        .column("uuid")
        .to_pylist()
    )
    pairs = list(
        zip(table.column("node1_uuid").to_pylist(), table.column("node2_uuid").to_pylist())
    )
    expected = [(0, 1), (0, 2), (2, 0), (2, 1), (3, 2), (3, 0), (4, 3)]
    assert pairs == [(identities[left], identities[right]) for left, right in expected]
    expected_scores = [1.0, 2**-0.5, 2**-0.5, 2**-0.5, 2**-0.5, 0.0, 0.0]
    assert all(
        abs(actual - expected) < 1e-12
        for actual, expected in zip(table.column("similarity").to_pylist(), expected_scores)
    )
    assert table.equals(run())
    assert (
        forge.similar(
            "Person", by="filtered_knn", vector_property="embedding", via="KNOWS"
        ).num_rows
        == 8
    )
    assert (
        forge.similar("Person", by="filtered_knn", k=2, vector_property="embedding").num_rows == 8
    )
    assert (
        forge.similar(
            "Person", by="filtered_knn", k=2, vector_property="embedding", via="MISSING"
        ).num_rows
        == 0
    )

    forge.execute("MATCH (a:Person {name:'a'}), (b:Person {name:'b'}) CREATE (b)-[:KNOWS]->(a)")
    assert run().num_rows == 8
    assert (
        g.GraphForge().similar("Person", by="filtered_knn", vector_property="embedding").num_rows
        == 0
    )
    for kwargs in [
        {"by": "filtered_knn"},
        {"by": "filtered_knn", "k": 0, "vector_property": "embedding"},
        {"by": "filtered_knn", "vector_property": "embedding", "via": " "},
    ]:
        _expect_validation_error(lambda kwargs=kwargs: forge.similar("Person", **kwargs))
    for cypher in [
        "CREATE (:Person {embedding:[0.0, 0.0]})",
        "CREATE (:Person {embedding:[1.0]}), (:Person {embedding:[1.0, 2.0]})",
    ]:
        invalid = g.GraphForge()
        invalid.execute(cypher)
        _expect_validation_error(
            lambda invalid=invalid: invalid.similar(
                "Person", by="filtered_knn", vector_property="embedding"
            )
        )


def check_cosine() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (:Person {name:'a', embedding:[1.0, 0.0]}), "
        "(:Person {name:'b', embedding:[0.0, 1.0]}), "
        "(:Person {name:'c', embedding:[-1.0, 0.0]}), "
        "(:Person {name:'d', embedding:[-1.0, -1.0]})"
    )
    run = lambda: forge.similar(  # noqa: E731
        "Person", by="cosine", k=3, vector_property="embedding"
    )
    table = run()
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("node1_uuid", pa.binary(16), False),
        ("node2_uuid", pa.binary(16), False),
        ("similarity", pa.float64(), False),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"cosine"
    assert table.schema.metadata[b"graphforge.verb"] == b"similar"
    identities = (
        forge.execute("MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name")
        .column("uuid")
        .to_pylist()
    )
    expected = [
        (0, 1),
        (0, 3),
        (0, 2),
        (1, 0),
        (1, 2),
        (1, 3),
        (2, 3),
        (2, 1),
        (2, 0),
        (3, 2),
        (3, 0),
        (3, 1),
    ]
    pairs = list(
        zip(table.column("node1_uuid").to_pylist(), table.column("node2_uuid").to_pylist())
    )
    assert pairs == [(identities[left], identities[right]) for left, right in expected]
    root_half = 2**-0.5
    expected_scores = [
        0.0,
        -root_half,
        -1.0,
        0.0,
        0.0,
        -root_half,
        root_half,
        0.0,
        -1.0,
        root_half,
        -root_half,
        -root_half,
    ]
    assert all(
        abs(actual - expected_score) < 1e-12
        for actual, expected_score in zip(table.column("similarity").to_pylist(), expected_scores)
    )
    assert table.equals(run())
    assert forge.similar("Person", by="cosine", vector_property="embedding").num_rows == 12
    assert forge.similar("Person", by="cosine", k=2, vector_property="embedding").num_rows == 8
    forge.execute("MATCH (a:Person {name:'a'}), (b:Person {name:'b'}) CREATE (a)-[:KNOWS]->(b)")
    assert table.equals(run())
    assert g.GraphForge().similar("Person", by="cosine", vector_property="embedding").num_rows == 0

    invalid_calls = [
        lambda: forge.similar("Person", by="cosine"),
        lambda: forge.similar("Person", by="cosine", k=0, vector_property="embedding"),
        lambda: forge.similar("Person", by="cosine", vector_property=" embedding"),
        lambda: forge.similar("Person", by="cosine", vector_property="embedding", via="KNOWS"),
    ]
    for cypher in [
        "CREATE (:Person {name:'missing'})",
        "CREATE (:Person {embedding:[0.0, 0.0]})",
        "CREATE (:Person {embedding:[1.0]}), (:Person {embedding:[1.0, 2.0]})",
    ]:
        invalid = g.GraphForge()
        invalid.execute(cypher)
        invalid_calls.append(
            lambda invalid=invalid: invalid.similar(
                "Person", by="cosine", vector_property="embedding"
            )
        )
    non_finite = g.GraphForge()
    non_finite.add_node("Person", embedding=[float("nan")])
    invalid_calls.append(
        lambda: non_finite.similar("Person", by="cosine", vector_property="embedding")
    )
    for call in invalid_calls:
        _expect_validation_error(call)


def check_bfs_paths() -> None:
    # #1351 — Python only coerces selectors/options and converts native Arrow.
    forge = g.GraphForge()
    handles = {
        name: forge.add_node("Person", name=name)
        for name in ("Alice", "Bob", "Carol", "Dan", "Eve")
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), (e:Person {name:'Eve'}) "
        "CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), "
        "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), "
        "(d)-[:OTHER]->(e)"
    )
    identity_table = forge.execute(
        "MATCH (p:Person) RETURN p.name AS name, p.node_uuid AS uuid ORDER BY p.name"
    )
    identities = dict(
        zip(identity_table.column("name").to_pylist(), identity_table.column("uuid").to_pylist())
    )

    table = forge.paths(handles["Alice"], by="bfs", via="KNOWS")
    assert table.column_names == ["source_uuid", "target_uuid", "cost", "path"]
    assert table.schema.field("source_uuid").type == pa.binary(16)
    assert table.schema.field("target_uuid").type == pa.binary(16)
    assert table.schema.field("cost").type == pa.float64()
    path_type = table.schema.field("path").type
    assert pa.types.is_list(path_type)
    assert path_type.value_type == pa.binary(16)
    assert not path_type.value_field.nullable
    assert table.schema.metadata[b"graphforge.algorithm"] == b"bfs"
    assert all(not name.endswith("_id") for name in table.column_names)
    assert table.column("source_uuid").to_pylist() == [identities["Alice"]] * 4
    assert table.column("target_uuid").to_pylist() == [
        identities[name] for name in ("Alice", "Bob", "Carol", "Dan")
    ]
    assert table.column("cost").to_pylist() == [0.0, 1.0, 1.0, 2.0]
    assert table.column("path").to_pylist()[3] == [
        identities[name] for name in ("Alice", "Bob", "Dan")
    ]
    assert table.equals(forge.paths(handles["Alice"].uuid, by="bfs", via="KNOWS"))

    dan_selector = {"label": "Person", "property": "name", "value": "Dan"}
    targeted = forge.paths(handles["Alice"], dan_selector, by="bfs", via="KNOWS")
    assert targeted.num_rows == 1
    assert targeted.column("path").to_pylist()[0] == table.column("path").to_pylist()[3]
    reverse = forge.paths(handles["Dan"], handles["Alice"], by="bfs", via="KNOWS", directed=False)
    assert reverse.column("path").to_pylist()[0] == [
        identities[name] for name in ("Dan", "Bob", "Alice")
    ]
    assert forge.paths(handles["Dan"], handles["Eve"], by="bfs", via="OTHER").num_rows == 1
    assert forge.paths(handles["Alice"], handles["Eve"], by="bfs", via="KNOWS").num_rows == 0

    _expect_validation_error(lambda: forge.paths(handles["Alice"], by="bfs", k=2))
    _expect_validation_error(lambda: forge.paths(handles["Alice"], by="bfs", weight="distance"))
    _expect_validation_error(lambda: forge.paths(handles["Alice"], by="astar"))

    other = g.GraphForge()
    foreign = other.add_node("Person", name="Foreign")
    for selector in ("not-a-uuid", str(uuid.uuid4()), foreign):
        _expect_validation_error(lambda selector=selector: forge.paths(selector, by="bfs"))

    forge.add_node("Person", name="Alice")
    _expect_validation_error(
        lambda: forge.paths({"label": "Person", "property": "name", "value": "Alice"}, by="bfs")
    )

    try:
        forge.paths({"label": "Person", "value": "Alice"}, by="bfs")
    except TypeError:
        pass
    else:
        raise SystemExit("expected TypeError for malformed property selector")

    closed = g.GraphForge()
    closed_handle = closed.add_node("Person", name="Closed")
    closed.close()
    try:
        closed.paths(closed_handle, by="bfs")
    except g.LifecycleError:
        pass
    else:
        raise SystemExit("expected LifecycleError for paths after close")


def check_dfs_paths() -> None:
    # #1892 — Python only coerces selectors/options and converts native Arrow.
    forge = g.GraphForge()
    handles = {
        name: forge.add_node("Person", name=name)
        for name in ("Alice", "Bob", "Carol", "Dan", "Eve", "Isolate")
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}) "
        "CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), "
        "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), "
        "(a)-[:OTHER]->(e)"
    )
    identity_table = forge.execute(
        "MATCH (p:Person) RETURN p.name AS name, p.node_uuid AS uuid ORDER BY p.name"
    )
    identities = dict(
        zip(identity_table.column("name").to_pylist(), identity_table.column("uuid").to_pylist())
    )

    table = forge.paths(handles["Alice"], by="dfs", via="KNOWS")
    assert table.column_names == ["node_uuid", "depth", "order"]
    assert [field.type for field in table.schema] == [pa.binary(16), pa.uint64(), pa.uint64()]
    assert all(not field.nullable for field in table.schema)
    assert table.schema.metadata == {
        b"graphforge.algorithm": b"dfs",
        b"graphforge.algorithm_schema_version": b"1",
        b"graphforge.verb": b"paths",
    }
    assert table.column("node_uuid").to_pylist() == [
        identities[name] for name in ("Alice", "Bob", "Dan", "Carol")
    ]
    assert table.column("depth").to_pylist() == [0, 1, 2, 1]
    assert table.column("order").to_pylist() == [0, 1, 2, 3]
    assert table.equals(forge.paths(handles["Alice"].uuid, by="dfs", via="KNOWS"))

    directed = forge.paths(handles["Dan"], by="dfs", via="KNOWS")
    assert directed.column("node_uuid").to_pylist() == [identities["Dan"]]
    undirected = forge.paths(handles["Dan"], by="dfs", via="KNOWS", directed=False)
    assert set(undirected.column("node_uuid").to_pylist()) == {
        identities[name] for name in ("Alice", "Bob", "Carol", "Dan")
    }
    other = forge.paths(handles["Alice"], by="dfs", via="OTHER")
    assert other.column("node_uuid").to_pylist() == [identities["Alice"], identities["Eve"]]
    assert forge.paths(handles["Isolate"], by="dfs").column("depth").to_pylist() == [0]
    assert forge.paths(handles["Alice"], by="dfs", via="MISSING").num_rows == 1

    invalid_calls = [
        lambda: forge.paths(handles["Alice"], handles["Dan"], by="dfs"),
        lambda: forge.paths(handles["Alice"], by="dfs", k=2),
        lambda: forge.paths(handles["Alice"], by="dfs", weight="distance"),
        lambda: forge.paths(handles["Alice"], by="dfs", heuristic="estimate"),
        lambda: forge.paths(handles["Alice"], by="dfs", via=" "),
        lambda: forge.paths(str(uuid.uuid4()), by="dfs"),
        lambda: g.GraphForge().paths(str(uuid.uuid4()), by="dfs"),
    ]
    for call in invalid_calls:
        _expect_validation_error(call)

    closed = g.GraphForge()
    closed_handle = closed.add_node("Person", name="Closed")
    closed.close()
    try:
        closed.paths(closed_handle, by="dfs")
    except g.LifecycleError:
        pass
    else:
        raise SystemExit("expected LifecycleError for DFS after close")


def check_dijkstra_paths() -> None:
    # #1666 — PyO3 delegates weighted execution and only converts native Arrow.
    forge = g.GraphForge()
    handles = {
        name: forge.add_node("Person", name=name)
        for name in ("Alice", "Bob", "Carol", "Dan", "Eve")
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), (e:Person {name:'Eve'}) "
        "CREATE (a)-[:ROAD {cost:1.0, bad:'x', negative:-1.0}]->(c), "
        "(a)-[:ROAD {cost:1.0, bad:'x', negative:-1.0}]->(b), "
        "(b)-[:ROAD {cost:2.0, bad:'x', negative:-1.0}]->(d), "
        "(c)-[:ROAD {cost:2.0, bad:'x', negative:-1.0}]->(d), "
        "(a)-[:ROAD {cost:9.0, bad:'x', negative:-1.0}]->(d), "
        "(d)-[:OTHER {cost:0.5}]->(e)"
    )
    identity_table = forge.execute(
        "MATCH (p:Person) RETURN p.name AS name, p.node_uuid AS uuid ORDER BY p.name"
    )
    identities = dict(
        zip(identity_table.column("name").to_pylist(), identity_table.column("uuid").to_pylist())
    )
    weighted = forge.paths(handles["Alice"], by="dijkstra", via="ROAD", weight="cost")
    assert weighted.column_names == ["source_uuid", "target_uuid", "cost", "path"]
    assert weighted.schema.metadata[b"graphforge.algorithm"] == b"dijkstra"
    assert all(not field.nullable for field in weighted.schema)
    assert weighted.column("target_uuid").to_pylist() == [
        identities[name] for name in ("Alice", "Bob", "Carol", "Dan")
    ]
    assert weighted.column("cost").to_pylist() == [0.0, 1.0, 1.0, 3.0]
    assert weighted.column("path").to_pylist()[3] == [
        identities[name] for name in ("Alice", "Bob", "Dan")
    ]
    assert weighted.equals(
        forge.paths(handles["Alice"].uuid, by="dijkstra", via="ROAD", weight="cost")
    )
    targeted = forge.paths(
        handles["Alice"], handles["Dan"], by="dijkstra", via="ROAD", weight="cost"
    )
    assert targeted.column("path").to_pylist()[0] == weighted.column("path").to_pylist()[3]
    assert forge.paths(handles["Dan"], handles["Alice"], by="dijkstra", via="ROAD").num_rows == 0
    assert (
        forge.paths(
            handles["Dan"], handles["Alice"], by="dijkstra", via="ROAD", directed=False
        ).num_rows
        == 1
    )
    unit = forge.paths(handles["Alice"], handles["Dan"], by="dijkstra", via="ROAD")
    assert unit.column("cost").to_pylist() == [1.0]
    all_pairs = forge.paths(handles["Eve"], by="dijkstra_all_pairs", weight="cost")
    assert all_pairs.column_names == ["source_uuid", "target_uuid", "cost", "path"]
    assert all_pairs.schema.metadata[b"graphforge.algorithm"] == b"dijkstra_all_pairs"
    assert all(not field.nullable for field in all_pairs.schema)
    assert all_pairs.column("source_uuid").to_pylist() == [
        identities[name]
        for name in ("Alice", "Alice", "Alice", "Alice", "Bob", "Bob", "Carol", "Carol", "Dan")
    ]
    assert all_pairs.column("target_uuid").to_pylist() == [
        identities[name]
        for name in ("Bob", "Carol", "Dan", "Eve", "Dan", "Eve", "Dan", "Eve", "Eve")
    ]
    assert all_pairs.column("cost").to_pylist() == [1.0, 1.0, 3.0, 3.5, 2.0, 2.5, 2.0, 2.5, 0.5]
    assert all_pairs.column("path").to_pylist()[2] == [
        identities[name] for name in ("Alice", "Bob", "Dan")
    ]
    assert all_pairs.equals(
        forge.paths(handles["Alice"].uuid, by="dijkstra_all_pairs", weight="cost")
    )
    assert (
        forge.paths(handles["Alice"], by="dijkstra_all_pairs", via="ROAD", weight="cost").num_rows
        == 5
    )
    assert forge.paths(handles["Alice"], by="dijkstra_all_pairs", directed=False).num_rows == 20
    assert (
        forge.paths(handles["Alice"], by="dijkstra_all_pairs", via="ROAD")
        .column("cost")
        .to_pylist()
        == [1.0] * 5
    )
    isolated = g.GraphForge()
    isolated_source = isolated.add_node("Person", name="Solo")
    empty_pairs = isolated.paths(isolated_source, by="dijkstra_all_pairs")
    assert empty_pairs.num_rows == 0
    assert empty_pairs.schema == all_pairs.schema
    invalid = [
        {"k": 2},
        {"via": " "},
        {"via": "ROAD", "weight": " "},
        {"via": "ROAD", "weight": "missing"},
        {"via": "ROAD", "weight": "bad"},
        {"via": "ROAD", "weight": "negative"},
    ]

    def expect_structured(options: dict[str, object]) -> None:
        try:
            forge.paths(handles["Alice"], handles["Dan"], by="dijkstra", **options)
        except (g.ValidationError, g.ExecutionError):
            return
        raise SystemExit("expected structured Dijkstra validation failure")

    for options in invalid:
        expect_structured(options)
        try:
            forge.paths(handles["Alice"], by="dijkstra_all_pairs", **options)
        except (g.ValidationError, g.ExecutionError):
            pass
        else:
            raise SystemExit("expected structured all-pairs Dijkstra failure")
    _expect_validation_error(
        lambda: forge.paths(handles["Alice"], handles["Dan"], by="dijkstra_all_pairs")
    )
    _expect_validation_error(lambda: forge.paths(str(uuid.uuid4()), by="dijkstra_all_pairs"))
    _expect_validation_error(lambda: forge.paths(str(uuid.uuid4()), by="dijkstra"))


def check_astar_paths() -> None:
    # #1684 — PyO3 forwards the heuristic selector to Rust without local execution.
    forge = g.GraphForge()
    estimates = {"Alice": 3.0, "Bob": 2.0, "Carol": 2.0, "Dan": 0.0, "Eve": 8.0}
    handles = {
        name: forge.add_node("Person", name=name, heuristic=estimate)
        for name, estimate in estimates.items()
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), (e:Person {name:'Eve'}) "
        "CREATE (a)-[:ROAD {cost:1.0}]->(c), (a)-[:ROAD {cost:1.0}]->(b), "
        "(b)-[:ROAD {cost:2.0}]->(d), (c)-[:ROAD {cost:2.0}]->(d), "
        "(a)-[:ROAD {cost:9.0}]->(d), (a)-[:UNIT]->(b), (b)-[:UNIT]->(e)"
    )
    identities = dict(
        zip(
            ("Alice", "Bob", "Carol", "Dan", "Eve"),
            forge.execute("MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name")
            .column("uuid")
            .to_pylist(),
            strict=True,
        )
    )
    table = forge.paths(
        handles["Alice"],
        handles["Dan"],
        by="astar",
        via="ROAD",
        weight="cost",
        heuristic="heuristic",
    )
    assert table.column_names == ["source_uuid", "target_uuid", "cost", "path"]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"astar"
    assert table.schema.field("source_uuid").type == pa.binary(16)
    assert table.schema.field("path").type.value_type == pa.binary(16)
    assert table.column("cost").to_pylist() == [3.0]
    assert table.column("path").to_pylist()[0] == [
        identities[name] for name in ("Alice", "Bob", "Dan")
    ]
    repeated = forge.paths(
        handles["Alice"].uuid,
        {"label": "Person", "property": "name", "value": "Dan"},
        by="astar",
        via="ROAD",
        weight="cost",
        heuristic="heuristic",
    )
    assert repeated.equals(table)
    zero = forge.paths(handles["Alice"], handles["Dan"], by="astar", via="ROAD", weight="cost")
    assert zero.column("path").to_pylist() == table.column("path").to_pylist()
    unit = forge.paths(handles["Alice"], handles["Eve"], by="astar", via="UNIT")
    assert unit.column("cost").to_pylist() == [2.0]
    assert unit.column("path").to_pylist()[0] == [
        identities[name] for name in ("Alice", "Bob", "Eve")
    ]
    assert (
        forge.paths(
            handles["Dan"], handles["Alice"], by="astar", via="ROAD", weight="cost"
        ).num_rows
        == 0
    )
    assert (
        forge.paths(
            handles["Dan"],
            handles["Alice"],
            by="astar",
            via="ROAD",
            directed=False,
            weight="cost",
        ).num_rows
        == 1
    )
    singleton = forge.paths(handles["Dan"], handles["Dan"], by="astar", heuristic="heuristic")
    assert singleton.column("cost").to_pylist() == [0.0]

    invalid_calls = [
        lambda: forge.paths(handles["Alice"], by="astar"),
        lambda: forge.paths(handles["Alice"], handles["Dan"], by="astar", k=2),
        lambda: forge.paths(handles["Alice"], handles["Dan"], by="astar", heuristic=" "),
        lambda: forge.paths(handles["Alice"], handles["Dan"], by="astar", heuristic="missing"),
        lambda: forge.paths(
            handles["Alice"], handles["Dan"], by="astar", via="ROAD", weight="missing"
        ),
    ]
    invalid_target = g.GraphForge()
    bad_source = invalid_target.add_node("Person", heuristic=1.0)
    bad_target = invalid_target.add_node("Person", heuristic=1.0)
    try:
        invalid_target.paths(bad_source, bad_target, by="astar", heuristic="heuristic")
    except g.ExecutionError:
        pass
    else:
        raise SystemExit("expected Rust target-heuristic execution failure")
    for call in invalid_calls:
        _expect_validation_error(call)

    for value in (None, "near", float("nan")):
        invalid_heuristic = g.GraphForge()
        source = invalid_heuristic.add_node("Person", heuristic=value)
        target = invalid_heuristic.add_node("Person", heuristic=0.0)
        _expect_validation_error(
            lambda source=source, target=target, graph=invalid_heuristic: graph.paths(
                source, target, by="astar", heuristic="heuristic"
            )
        )
    negative_heuristic = g.GraphForge()
    source = negative_heuristic.add_node("Person", heuristic=-1.0)
    target = negative_heuristic.add_node("Person", heuristic=0.0)
    try:
        negative_heuristic.paths(source, target, by="astar", heuristic="heuristic")
    except g.ExecutionError:
        pass
    else:
        raise SystemExit("expected Rust negative-heuristic execution failure")

    for literal, error_type in (
        ("null", g.ValidationError),
        ("'heavy'", g.ValidationError),
        ("-1.0", g.ExecutionError),
        ("1e308 * 2.0", g.ValidationError),
    ):
        invalid_weight = g.GraphForge()
        source = invalid_weight.add_node("Person", name="source")
        target = invalid_weight.add_node("Person", name="target")
        invalid_weight.execute(
            "MATCH (s:Person {name:'source'}), (t:Person {name:'target'}) "
            f"CREATE (s)-[:ROAD {{cost:{literal}}}]->(t)"
        )
        try:
            invalid_weight.paths(source, target, by="astar", via="ROAD", weight="cost")
        except error_type:
            pass
        else:
            raise SystemExit(f"expected Rust invalid-weight failure for {literal}")


def check_bellman_ford_paths() -> None:
    # #1692 — both native bindings delegate negative-weight paths to Rust.
    def expect_validation_error(call, message: str) -> None:
        try:
            call()
        except g.ValidationError as error:
            assert str(error) == message
        else:
            raise SystemExit(f"expected ValidationError: {message}")

    forge = g.GraphForge()
    handles = {
        name: forge.add_node("Person", name=name)
        for name in ("Alice", "Bob", "Carol", "Dan", "Eve")
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), (e:Person {name:'Eve'}) "
        "CREATE (a)-[:ROAD {cost:5.0}]->(c), (a)-[:ROAD {cost:4.0}]->(b), "
        "(b)-[:ROAD {cost:-2.0}]->(c), (b)-[:ROAD {cost:6.0}]->(d), "
        "(c)-[:ROAD {cost:3.0}]->(d), (d)-[:ROAD {cost:-1.0}]->(e), "
        "(a)-[:UNIT]->(b), (b)-[:UNIT]->(e), (d)-[:BACK]->(a)"
    )
    identities = dict(
        zip(
            ("Alice", "Bob", "Carol", "Dan", "Eve"),
            forge.execute("MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name")
            .column("uuid")
            .to_pylist(),
            strict=True,
        )
    )
    table = forge.paths(handles["Alice"], by="bellman_ford", via="ROAD", weight="cost")
    assert table.column_names == ["source_uuid", "target_uuid", "cost", "path"]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"bellman_ford"
    assert table.schema.field("source_uuid").type == pa.binary(16)
    assert table.schema.field("path").type.value_type == pa.binary(16)
    assert table.column("source_uuid").to_pylist() == [identities["Alice"]] * 5
    assert table.column("target_uuid").to_pylist() == [
        identities[name] for name in ("Alice", "Bob", "Carol", "Dan", "Eve")
    ]
    assert table.column("cost").to_pylist() == [0.0, 4.0, 2.0, 5.0, 4.0]
    assert table.column("path").to_pylist()[4] == [
        identities[name] for name in ("Alice", "Bob", "Carol", "Dan", "Eve")
    ]
    repeated = forge.paths(
        handles["Alice"].uuid,
        {"label": "Person", "property": "name", "value": "Eve"},
        by="bellman_ford",
        via="ROAD",
        weight="cost",
    )
    assert repeated.column("path").to_pylist()[0] == table.column("path").to_pylist()[4]
    unit = forge.paths(handles["Alice"], handles["Eve"], by="bellman_ford", via="UNIT")
    assert unit.column("cost").to_pylist() == [2.0]
    assert (
        forge.paths(handles["Alice"], handles["Dan"], by="bellman_ford", via="BACK").num_rows == 0
    )
    assert (
        forge.paths(
            handles["Alice"],
            handles["Dan"],
            by="bellman_ford",
            via="BACK",
            directed=False,
        ).num_rows
        == 1
    )
    singleton = forge.paths(
        handles["Alice"], handles["Alice"], by="bellman_ford", via="ROAD", weight="cost"
    )
    assert singleton.column("cost").to_pylist() == [0.0]

    tie = g.GraphForge()
    tie_handles = {
        name: tie.add_node("Person", name=name)
        for name in ("source", "alpha", "beta", "target", "isolated")
    }
    tie.execute(
        "MATCH (s:Person {name:'source'}), (a:Person {name:'alpha'}), "
        "(b:Person {name:'beta'}), (t:Person {name:'target'}) "
        "CREATE (s)-[:ROAD {cost:1.0}]->(a), (a)-[:ROAD {cost:1.0}]->(t), "
        "(s)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:1.0}]->(t)"
    )
    tie_ids = dict(
        zip(
            ("alpha", "beta", "isolated", "source", "target"),
            tie.execute("MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name")
            .column("uuid")
            .to_pylist(),
            strict=True,
        )
    )
    tied = tie.paths(tie_handles["source"], by="bellman_ford", via="ROAD", weight="cost")
    assert tied.column("target_uuid").to_pylist() == sorted(
        [tie_ids[name] for name in ("source", "alpha", "beta", "target")]
    )
    assert tie_ids["isolated"] not in tied.column("target_uuid").to_pylist()
    target_row = tied.column("target_uuid").to_pylist().index(tie_ids["target"])
    assert tied.column("path").to_pylist()[target_row] == [
        tie_ids["source"],
        tie_ids["alpha"],
        tie_ids["target"],
    ]

    cycle = g.GraphForge()
    source = cycle.add_node("Person", name="source")
    target = cycle.add_node("Person", name="target")
    cycle.execute(
        "MATCH (s:Person {name:'source'}), (t:Person {name:'target'}) "
        "CREATE (s)-[:ROAD {cost:-1.0}]->(t), (t)-[:ROAD {cost:0.0}]->(s)"
    )
    try:
        cycle.paths(source, target, by="bellman_ford", via="ROAD", weight="cost")
    except g.ExecutionError as error:
        assert (
            str(error) == "Rust algorithm execution failed: bellman_ford found a negative cycle "
            "reachable from the source"
        )
    else:
        raise SystemExit("expected Rust negative-cycle execution failure")

    invalid_calls = [
        (
            lambda: forge.paths(handles["Alice"], by="bellman_ford", k=2),
            "bellman_ford k must be 1",
        ),
        (
            lambda: forge.paths(handles["Alice"], by="bellman_ford", heuristic="heuristic"),
            "bellman_ford does not accept a heuristic property",
        ),
        (
            lambda: forge.paths(handles["Alice"], by="bellman_ford", via=" "),
            'invalid paths relationship selector " "',
        ),
        (
            lambda: forge.paths(handles["Alice"], by="bellman_ford", weight=" "),
            'invalid paths weight property " "',
        ),
        (
            lambda: forge.paths(handles["Alice"], by="bellman_ford", weight="missing"),
            'edge weight property "missing" does not exist',
        ),
    ]
    for call, message in invalid_calls:
        expect_validation_error(call, message)
    for literal, message in (
        ("null", None),
        ("'heavy'", 'edge weight property "cost" must be numeric'),
        ("1e308 * 2.0", None),
    ):
        invalid_weight = g.GraphForge()
        source = invalid_weight.add_node("Person", name="source")
        target = invalid_weight.add_node("Person", name="target")
        invalid_weight.execute(
            "MATCH (s:Person {name:'source'}), (t:Person {name:'target'}) "
            f"CREATE (s)-[:ROAD {{cost:{literal}}}]->(t)"
        )
        expected_message = message
        if expected_message is None:
            edge_uuid = (
                invalid_weight.execute("MATCH ()-[r:ROAD]->() RETURN r.edge_uuid AS uuid")
                .column("uuid")[0]
                .as_py()
            )
            expected_message = (
                "edge weight is missing, NULL, NaN, or infinite for edge "
                f"{uuid.UUID(bytes=edge_uuid)}"
            )
        expect_validation_error(
            lambda graph=invalid_weight, start=source, end=target: graph.paths(
                start, end, by="bellman_ford", via="ROAD", weight="cost"
            ),
            expected_message,
        )


def check_delta_stepping_paths() -> None:
    # #1707 — the native wheel only adapts arguments and Rust Arrow results.
    def expect_validation_error(call, message: str) -> None:
        try:
            call()
        except g.ValidationError as error:
            assert str(error) == message
        else:
            raise SystemExit(f"expected ValidationError: {message}")

    forge = g.GraphForge()
    handles = {
        name: forge.add_node("Person", name=name)
        for name in ("Alice", "Bob", "Carol", "Dan", "Eve")
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}) "
        "CREATE (a)-[:ROAD {cost:1.0}]->(c), (a)-[:ROAD {cost:0.5}]->(b), "
        "(b)-[:ROAD {cost:0.5}]->(c), (a)-[:ROAD {cost:5.0}]->(d), "
        "(c)-[:ROAD {cost:2.0}]->(d), (a)-[:UNIT]->(b), "
        "(b)-[:UNIT]->(d), (d)-[:BACK]->(a)"
    )
    identities = dict(
        zip(
            ("Alice", "Bob", "Carol", "Dan", "Eve"),
            forge.execute("MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name")
            .column("uuid")
            .to_pylist(),
            strict=True,
        )
    )
    table = forge.paths(handles["Alice"], by="delta_stepping", via="ROAD", weight="cost")
    assert table.column_names == ["source_uuid", "target_uuid", "cost", "path"]
    assert all(not field.nullable for field in table.schema)
    assert table.schema.metadata[b"graphforge.algorithm"] == b"delta_stepping"
    assert table.schema.field("source_uuid").type == pa.binary(16)
    assert table.schema.field("target_uuid").type == pa.binary(16)
    assert table.schema.field("path").type.value_type == pa.binary(16)
    assert table.column("source_uuid").to_pylist() == [identities["Alice"]] * 4
    assert table.column("target_uuid").to_pylist() == [
        identities[name] for name in ("Alice", "Bob", "Carol", "Dan")
    ]
    assert table.column("cost").to_pylist() == [0.0, 0.5, 1.0, 3.0]
    assert table.column("path").to_pylist()[2] == [
        identities[name] for name in ("Alice", "Bob", "Carol")
    ]
    assert table.column("path").to_pylist()[3] == [
        identities[name] for name in ("Alice", "Bob", "Carol", "Dan")
    ]
    assert identities["Eve"] not in table.column("target_uuid").to_pylist()

    targeted = forge.paths(
        handles["Alice"].uuid,
        {"label": "Person", "property": "name", "value": "Dan"},
        by="delta_stepping",
        via="ROAD",
        weight="cost",
    )
    assert targeted.column("cost").to_pylist() == [3.0]
    assert targeted.column("path").to_pylist()[0] == table.column("path").to_pylist()[3]
    unit = forge.paths(handles["Alice"], handles["Dan"], by="delta_stepping", via="UNIT")
    assert unit.column("cost").to_pylist() == [2.0]
    assert unit.column("path").to_pylist()[0] == [
        identities[name] for name in ("Alice", "Bob", "Dan")
    ]
    assert (
        forge.paths(handles["Alice"], handles["Eve"], by="delta_stepping", via="ROAD").num_rows == 0
    )
    assert (
        forge.paths(handles["Alice"], handles["Dan"], by="delta_stepping", via="BACK").num_rows == 0
    )
    assert (
        forge.paths(
            handles["Alice"],
            handles["Dan"],
            by="delta_stepping",
            via="BACK",
            directed=False,
        ).num_rows
        == 1
    )
    singleton = forge.paths(
        handles["Alice"],
        handles["Alice"],
        by="delta_stepping",
        via="ROAD",
        weight="cost",
    )
    assert singleton.column("cost").to_pylist() == [0.0]
    assert singleton.column("path").to_pylist() == [[identities["Alice"]]]

    tie = g.GraphForge()
    tie_handles = {
        name: tie.add_node("Person", name=name)
        for name in ("source", "alpha", "beta", "target", "isolated")
    }
    tie.execute(
        "MATCH (s:Person {name:'source'}), (a:Person {name:'alpha'}), "
        "(b:Person {name:'beta'}), (t:Person {name:'target'}) "
        "CREATE (s)-[:ROAD {cost:1.0}]->(a), (a)-[:ROAD {cost:1.0}]->(t), "
        "(s)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:1.0}]->(t)"
    )
    tie_ids = dict(
        zip(
            ("alpha", "beta", "isolated", "source", "target"),
            tie.execute("MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name")
            .column("uuid")
            .to_pylist(),
            strict=True,
        )
    )
    tied = tie.paths(tie_handles["source"], by="delta_stepping", via="ROAD", weight="cost")
    assert tied.column("target_uuid").to_pylist() == sorted(
        [tie_ids[name] for name in ("source", "alpha", "beta", "target")]
    )
    assert tie_ids["isolated"] not in tied.column("target_uuid").to_pylist()
    target_row = tied.column("target_uuid").to_pylist().index(tie_ids["target"])
    assert tied.column("path").to_pylist()[target_row] == [
        tie_ids["source"],
        tie_ids["alpha"],
        tie_ids["target"],
    ]

    invalid_calls = [
        (
            lambda: forge.paths(handles["Alice"], by="delta_stepping", k=2),
            "delta_stepping k must be 1",
        ),
        (
            lambda: forge.paths(handles["Alice"], by="delta_stepping", heuristic="heuristic"),
            "delta_stepping does not accept a heuristic property",
        ),
        (
            lambda: forge.paths(handles["Alice"], by="delta_stepping", via=" "),
            'invalid paths relationship selector " "',
        ),
        (
            lambda: forge.paths(handles["Alice"], by="delta_stepping", weight=" "),
            'invalid paths weight property " "',
        ),
        (
            lambda: forge.paths(handles["Alice"], by="delta_stepping", weight="missing"),
            'edge weight property "missing" does not exist',
        ),
    ]
    for call, message in invalid_calls:
        expect_validation_error(call, message)

    for literal, error_type, message in (
        ("null", g.ValidationError, None),
        ("'heavy'", g.ValidationError, 'edge weight property "cost" must be numeric'),
        ("1e308 * 2.0", g.ValidationError, None),
        (
            "-1.0",
            g.ExecutionError,
            "Rust algorithm execution failed: delta_stepping requires finite non-negative "
            "edge weights",
        ),
    ):
        invalid_weight = g.GraphForge()
        source = invalid_weight.add_node("Person", name="source")
        target = invalid_weight.add_node("Person", name="target")
        invalid_weight.execute(
            "MATCH (s:Person {name:'source'}), (t:Person {name:'target'}) "
            f"CREATE (s)-[:ROAD {{cost:{literal}}}]->(t)"
        )
        expected_message = message
        if expected_message is None:
            edge_uuid = (
                invalid_weight.execute("MATCH ()-[r:ROAD]->() RETURN r.edge_uuid AS uuid")
                .column("uuid")[0]
                .as_py()
            )
            expected_message = (
                "edge weight is missing, NULL, NaN, or infinite for edge "
                f"{uuid.UUID(bytes=edge_uuid)}"
            )
        try:
            invalid_weight.paths(source, target, by="delta_stepping", via="ROAD", weight="cost")
        except error_type as error:
            assert str(error) == expected_message
        else:
            raise SystemExit(f"expected {error_type.__name__}: {expected_message}")


def check_yens_paths() -> None:
    # #1715 — PyO3 forwards selectors/options and only converts Rust Arrow output.
    def expect_error(error_type, message: str, call) -> None:
        try:
            call()
        except error_type as error:
            assert str(error) == message
        else:
            raise SystemExit(f"expected {error_type.__name__}: {message}")

    forge = g.GraphForge()
    handles = {
        name: forge.add_node("Person", name=name)
        for name in ("Alice", "Bob", "Carol", "Dan", "Eve")
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}) "
        "CREATE (a)-[:ROAD {cost:4.0}]->(b), "
        "(a)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:2.0}]->(d), "
        "(a)-[:ROAD {cost:1.0}]->(c), (c)-[:ROAD {cost:2.0}]->(d), "
        "(b)-[:ROAD {cost:0.5}]->(c), (a)-[:ROAD {cost:4.0}]->(d), "
        "(a)-[:ROAD {cost:0.0}]->(a), (c)-[:ROAD {cost:0.0}]->(a), "
        "(a)-[:UNIT]->(d), (a)-[:UNIT]->(b), (b)-[:UNIT]->(d)"
    )
    identities = {
        name: uuid.UUID(handles[name].uuid).bytes
        for name in ("Alice", "Bob", "Carol", "Dan", "Eve")
    }
    options = {
        "by": "yens",
        "via": "ROAD",
        "directed": True,
        "k": 10,
        "weight": "cost",
    }
    table = forge.paths(handles["Alice"], handles["Dan"], **options)

    assert table.column_names == ["source_uuid", "target_uuid", "rank", "cost", "path"]
    assert all(not field.nullable for field in table.schema)
    assert table.schema.field("source_uuid").type == pa.binary(16)
    assert table.schema.field("target_uuid").type == pa.binary(16)
    assert table.schema.field("rank").type == pa.uint64()
    assert table.schema.field("cost").type == pa.float64()
    path_type = table.schema.field("path").type
    assert pa.types.is_list(path_type)
    assert path_type.value_type == pa.binary(16)
    assert not path_type.value_field.nullable
    assert table.schema.metadata[b"graphforge.algorithm"] == b"yens"
    assert table.schema.metadata[b"graphforge.verb"] == b"paths"
    assert all(not name.endswith("_id") for name in table.column_names)
    assert table.column("rank").to_pylist() == [1, 2, 3, 4]
    assert table.column("cost").to_pylist() == [3.0, 3.0, 3.5, 4.0]
    assert table.column("source_uuid").to_pylist() == [identities["Alice"]] * 4
    assert table.column("target_uuid").to_pylist() == [identities["Dan"]] * 4
    assert table.column("path").to_pylist() == [
        [identities[name] for name in ("Alice", "Bob", "Dan")],
        [identities[name] for name in ("Alice", "Carol", "Dan")],
        [identities[name] for name in ("Alice", "Bob", "Carol", "Dan")],
        [identities[name] for name in ("Alice", "Dan")],
    ]
    assert table.equals(forge.paths(handles["Alice"], handles["Dan"], **options))
    assert table.equals(
        forge.paths(
            handles["Alice"].uuid,
            {"label": "Person", "property": "name", "value": "Dan"},
            **options,
        )
    )

    unit = forge.paths(
        handles["Alice"],
        handles["Dan"],
        by="yens",
        via="UNIT",
        k=2,
    )
    assert unit.column("cost").to_pylist() == [1.0, 2.0]
    assert (
        forge.paths(
            handles["Dan"],
            handles["Alice"],
            by="yens",
            via="ROAD",
            directed=True,
            k=2,
            weight="cost",
        ).num_rows
        == 0
    )
    assert (
        forge.paths(
            handles["Dan"],
            handles["Alice"],
            by="yens",
            via="ROAD",
            directed=False,
            k=2,
            weight="cost",
        ).num_rows
        > 0
    )
    assert (
        forge.paths(
            handles["Alice"],
            handles["Eve"],
            by="yens",
            via="ROAD",
            k=2,
            weight="cost",
        ).num_rows
        == 0
    )
    singleton = forge.paths(
        handles["Alice"],
        handles["Alice"],
        by="yens",
        via="ROAD",
        k=4,
        weight="cost",
    )
    assert singleton.column("rank").to_pylist() == [1]
    assert singleton.column("cost").to_pylist() == [0.0]
    assert singleton.column("path").to_pylist() == [[identities["Alice"]]]

    for call, message in (
        (
            lambda: forge.paths(handles["Alice"], by="yens", k=2),
            "yens requires a target selector",
        ),
        (
            lambda: forge.paths(handles["Alice"], handles["Dan"], by="yens", k=0),
            "yens k must be at least 1",
        ),
        (
            lambda: forge.paths(
                handles["Alice"],
                handles["Dan"],
                by="yens",
                k=2,
                heuristic="estimate",
            ),
            "yens does not accept a heuristic property",
        ),
        (
            lambda: forge.paths(handles["Alice"], handles["Dan"], by="yens", via=" ", k=2),
            'invalid paths relationship selector " "',
        ),
        (
            lambda: forge.paths(
                handles["Alice"],
                handles["Dan"],
                by="yens",
                via="ROAD",
                k=2,
                weight=" ",
            ),
            'invalid paths weight property " "',
        ),
        (
            lambda: forge.paths(
                handles["Alice"],
                handles["Dan"],
                by="yens",
                via="ROAD",
                k=2,
                weight="missing",
            ),
            'edge weight property "missing" does not exist',
        ),
    ):
        expect_error(g.ValidationError, message, call)

    for literal, error_type, message in (
        ("null", g.ValidationError, None),
        ("'heavy'", g.ValidationError, 'edge weight property "cost" must be numeric'),
        ("1e308 * 2.0", g.ValidationError, None),
        (
            "-1.0",
            g.ExecutionError,
            "Rust algorithm execution failed: yens requires finite non-negative edge weights",
        ),
    ):
        invalid = g.GraphForge()
        source = invalid.add_node("Person", name="source")
        target = invalid.add_node("Person", name="target")
        invalid.execute(
            "MATCH (s:Person {name:'source'}), (t:Person {name:'target'}) "
            f"CREATE (s)-[:ROAD {{cost:{literal}}}]->(t)"
        )
        expected_message = message
        if expected_message is None:
            edge_uuid = (
                invalid.execute("MATCH ()-[r:ROAD]->() RETURN r.edge_uuid AS uuid")
                .column("uuid")[0]
                .as_py()
            )
            expected_message = (
                "edge weight is missing, NULL, NaN, or infinite for edge "
                f"{uuid.UUID(bytes=edge_uuid)}"
            )
        expect_error(
            error_type,
            expected_message,
            lambda graph=invalid, start=source, end=target: graph.paths(
                start,
                end,
                by="yens",
                via="ROAD",
                k=2,
                weight="cost",
            ),
        )


def check_floyd_warshall_paths() -> None:
    # #1702 — the native wheel is a thin adapter over Rust all-pairs execution.
    forge = g.GraphForge()
    handles = {
        name: forge.add_node("Person", name=name)
        for name in ("Alice", "Bob", "Carol", "Dan", "Eve")
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}) "
        "CREATE (a)-[:ROAD {cost:4.0}]->(b), (a)-[:ROAD {cost:5.0}]->(c), "
        "(b)-[:ROAD {cost:-2.0}]->(c), (c)-[:ROAD {cost:3.0}]->(d), "
        "(a)-[:UNIT]->(d)"
    )
    uuids = dict(
        zip(
            ("Alice", "Bob", "Carol", "Dan", "Eve"),
            forge.execute("MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name")
            .column("uuid")
            .to_pylist(),
        )
    )
    table = forge.paths(handles["Eve"], by="floyd_warshall", via="ROAD", weight="cost")
    assert table.column_names == ["source_uuid", "target_uuid", "cost", "path"]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"floyd_warshall"
    assert all(not field.nullable for field in table.schema)
    assert table.column("source_uuid").to_pylist() == [
        uuids[name] for name in ("Alice", "Alice", "Alice", "Bob", "Bob", "Carol")
    ]
    assert table.column("target_uuid").to_pylist() == [
        uuids[name] for name in ("Bob", "Carol", "Dan", "Carol", "Dan", "Dan")
    ]
    assert table.column("cost").to_pylist() == [4.0, 2.0, 5.0, -2.0, 1.0, 3.0]
    assert table.column("path").to_pylist()[2] == [
        uuids[name] for name in ("Alice", "Bob", "Carol", "Dan")
    ]
    assert uuids["Eve"] not in table.column("source_uuid").to_pylist()
    assert table.equals(
        forge.paths(
            handles["Alice"].uuid,
            by="floyd_warshall",
            via="ROAD",
            weight="cost",
        )
    )
    assert forge.paths(handles["Alice"], by="floyd_warshall", via="UNIT").column(
        "cost"
    ).to_pylist() == [1.0]

    def expect_validation_error(call, message: str) -> None:
        try:
            call()
        except g.ValidationError as error:
            assert str(error) == message
        else:
            raise SystemExit(f"expected Floyd-Warshall error: {message}")

    invalid_calls = [
        (
            lambda: forge.paths(handles["Alice"], handles["Dan"], by="floyd_warshall"),
            "floyd_warshall does not accept a target selector",
        ),
        (
            lambda: forge.paths(handles["Alice"], by="floyd_warshall", k=2),
            "floyd_warshall k must be 1",
        ),
        (
            lambda: forge.paths(handles["Alice"], by="floyd_warshall", heuristic="estimate"),
            "floyd_warshall does not accept a heuristic property",
        ),
    ]
    for call, message in invalid_calls:
        expect_validation_error(call, message)
    _expect_validation_error(
        lambda: forge.paths(
            handles["Alice"],
            by="floyd_warshall",
            via="ROAD",
            weight="missing",
        )
    )

    cycle = g.GraphForge()
    source = cycle.add_node("Person", name="source")
    cycle.add_node("Person", name="target")
    cycle.execute(
        "MATCH (s:Person {name:'source'}), (t:Person {name:'target'}) "
        "CREATE (s)-[:ROAD {cost:-2.0}]->(t), (t)-[:ROAD {cost:1.0}]->(s)"
    )
    try:
        cycle.paths(source, by="floyd_warshall", via="ROAD", weight="cost")
    except g.ExecutionError as error:
        assert str(error) == (
            "Rust algorithm execution failed: floyd_warshall found a negative "
            "cycle in the selected graph"
        )
    else:
        raise SystemExit("expected Floyd-Warshall negative-cycle failure")


def check_transitive_closure_paths() -> None:
    # #1739 — PyO3 forwards selectors/options and converts the Rust PAIR batch.
    forge = g.GraphForge()
    handles = {
        name: forge.add_node("Person", name=name)
        for name in ("Alice", "Bob", "Carol", "Dan", "Eve")
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}) "
        "CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), "
        "(b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), "
        "(c)-[:KNOWS]->(c), (d)-[:OTHER]->(e)"
    )
    identity_table = forge.execute(
        "MATCH (p:Person) RETURN p.name AS name, p.node_uuid AS uuid ORDER BY p.name"
    )
    uuids = dict(
        zip(
            identity_table.column("name").to_pylist(),
            identity_table.column("uuid").to_pylist(),
        )
    )
    source = {"label": "Person", "property": "name", "value": "Eve"}

    table = forge.paths(source, by="transitive_closure", via="KNOWS")
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("source_uuid", pa.binary(16), False),
        ("target_uuid", pa.binary(16), False),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"transitive_closure"
    assert table.schema.metadata[b"graphforge.verb"] == b"paths"
    expected = sorted(
        (uuids[source_name], uuids[target_name])
        for source_name, target_name in (
            ("Alice", "Alice"),
            ("Alice", "Bob"),
            ("Alice", "Carol"),
            ("Bob", "Alice"),
            ("Bob", "Bob"),
            ("Bob", "Carol"),
            ("Carol", "Carol"),
        )
    )
    actual = list(
        zip(
            table.column("source_uuid").to_pylist(),
            table.column("target_uuid").to_pylist(),
        )
    )
    assert actual == expected
    assert table.equals(forge.paths(handles["Eve"].uuid, by="transitive_closure", via="KNOWS"))

    undirected = forge.paths(
        handles["Alice"],
        by="transitive_closure",
        via="KNOWS",
        directed=False,
    )
    connected = [uuids[name] for name in ("Alice", "Bob", "Carol")]
    assert list(
        zip(
            undirected.column("source_uuid").to_pylist(),
            undirected.column("target_uuid").to_pylist(),
        )
    ) == sorted(
        (source_uuid, target_uuid) for source_uuid in connected for target_uuid in connected
    )

    other = forge.paths(handles["Alice"], by="transitive_closure", via="OTHER")
    assert list(
        zip(
            other.column("source_uuid").to_pylist(),
            other.column("target_uuid").to_pylist(),
        )
    ) == [(uuids["Dan"], uuids["Eve"])]

    isolated = g.GraphForge()
    isolated_source = isolated.add_node("Person", name="Only")
    assert isolated.paths(isolated_source, by="transitive_closure").num_rows == 0

    invalid_calls = [
        lambda: forge.paths(handles["Alice"], handles["Bob"], by="transitive_closure"),
        lambda: forge.paths(handles["Alice"], by="transitive_closure", k=2),
        lambda: forge.paths(handles["Alice"], by="transitive_closure", weight="cost"),
        lambda: forge.paths(handles["Alice"], by="transitive_closure", heuristic="estimate"),
        lambda: forge.paths(handles["Alice"], by="transitive_closure", via=" "),
    ]
    for call in invalid_calls:
        _expect_validation_error(call)


def check_random_walk_paths() -> None:
    forge = g.GraphForge()
    alice = forge.add_node("Person", name="Alice")
    bob = forge.add_node("Person", name="Bob")
    carol = forge.add_node("Person", name="Carol")
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}) "
        "CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)"
    )

    options = {
        "by": "random_walk",
        "via": "KNOWS",
        "k": 2,
        "walk_length": 2,
        "seed": 42,
    }
    table = forge.paths(alice, **options)
    assert table.equals(forge.paths(alice.uuid, **options))
    assert table.schema == pa.schema(
        [
            pa.field("start_uuid", pa.binary(16), nullable=False),
            pa.field(
                "walk",
                pa.list_(pa.field("item", pa.binary(16), nullable=False)),
                nullable=False,
            ),
        ],
        metadata=table.schema.metadata,
    )
    assert table.schema.metadata[b"graphforge.algorithm"] == b"random_walk"
    assert table["start_uuid"].to_pylist() == [uuid.UUID(alice.uuid).bytes] * 2
    expected_walk = [uuid.UUID(handle.uuid).bytes for handle in (alice, bob, carol)]
    assert table["walk"].to_pylist() == [expected_walk] * 2

    invalid_calls = [
        lambda: forge.paths(alice, bob, **options),
        lambda: forge.paths(alice, **{**options, "k": 0}),
        lambda: forge.paths(alice, by="bfs", walk_length=2),
    ]
    for call in invalid_calls:
        _expect_validation_error(call)


def check_maximum_flow_paths() -> None:
    def expect_execution_error(call, message: str) -> None:
        try:
            call()
        except g.ExecutionError:
            return
        raise SystemExit(message)

    forge = g.GraphForge()
    handles = {name: forge.add_node("Person", name=name) for name in ("Source", "A", "B", "Sink")}
    forge.execute(
        "MATCH (s:Person {name:'Source'}), (a:Person {name:'A'}), "
        "(b:Person {name:'B'}), (t:Person {name:'Sink'}) "
        "CREATE (s)-[:PIPE {capacity:3.0}]->(a), "
        "(s)-[:PIPE {capacity:2.0}]->(b), "
        "(a)-[:PIPE {capacity:1.0}]->(b), "
        "(a)-[:PIPE {capacity:2.0}]->(t), "
        "(b)-[:PIPE {capacity:3.0}]->(t), "
        "(a)-[:PIPE {capacity:7.0}]->(a), "
        "(b)-[:PIPE {capacity:0.0}]->(a), "
        "(s)-[:OTHER {capacity:100.0}]->(t)"
    )
    options = {
        "via": "PIPE",
        "directed": True,
        "weight": "capacity",
    }
    scalar = forge.paths(
        handles["Source"],
        handles["Sink"],
        by="max_flow",
        **options,
    )
    edges = forge.paths(
        handles["Source"],
        handles["Sink"],
        by="max_flow_edges",
        **options,
    )

    assert scalar.schema == pa.schema(
        [
            pa.field("source_uuid", pa.binary(16), nullable=False),
            pa.field("sink_uuid", pa.binary(16), nullable=False),
            pa.field("flow", pa.float64(), nullable=False),
        ],
        metadata=scalar.schema.metadata,
    )
    assert edges.schema == pa.schema(
        [
            pa.field("edge_uuid", pa.binary(16), nullable=False),
            pa.field("source_uuid", pa.binary(16), nullable=False),
            pa.field("target_uuid", pa.binary(16), nullable=False),
            pa.field("flow", pa.float64(), nullable=False),
        ],
        metadata=edges.schema.metadata,
    )
    for table, algorithm in (
        (scalar, b"max_flow"),
        (edges, b"max_flow_edges"),
    ):
        assert table.schema.metadata[b"graphforge.algorithm"] == algorithm
        assert table.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
        assert table.schema.metadata[b"graphforge.verb"] == b"paths"

    uuids = {name: uuid.UUID(handle.uuid).bytes for name, handle in handles.items()}
    assert scalar.to_pydict() == {
        "source_uuid": [uuids["Source"]],
        "sink_uuid": [uuids["Sink"]],
        "flow": [5.0],
    }
    edge_uuids = edges["edge_uuid"].to_pylist()
    assert len(edge_uuids) == len(set(edge_uuids)) == 7
    assert edge_uuids == sorted(edge_uuids)
    assignments = {
        (source, target): flow
        for source, target, flow in zip(
            edges["source_uuid"].to_pylist(),
            edges["target_uuid"].to_pylist(),
            edges["flow"].to_pylist(),
        )
    }
    assert assignments == {
        (uuids["Source"], uuids["A"]): 3.0,
        (uuids["Source"], uuids["B"]): 2.0,
        (uuids["A"], uuids["B"]): 1.0,
        (uuids["A"], uuids["Sink"]): 2.0,
        (uuids["B"], uuids["Sink"]): 3.0,
        (uuids["A"], uuids["A"]): 0.0,
        (uuids["B"], uuids["A"]): 0.0,
    }
    assert edges.equals(
        forge.paths(
            handles["Source"].uuid,
            handles["Sink"].uuid,
            by="max_flow_edges",
            **options,
        )
    )
    assert (
        sum(flow for (source, _target), flow in assignments.items() if source == uuids["Source"])
        == scalar["flow"][0].as_py()
    )
    assert (
        sum(flow for (_source, target), flow in assignments.items() if target == uuids["Sink"])
        == scalar["flow"][0].as_py()
    )

    for algorithm in ("max_flow", "max_flow_edges"):
        _expect_validation_error(
            lambda algorithm=algorithm: forge.paths(
                handles["Source"],
                by=algorithm,
                **options,
            )
        )
        expect_execution_error(
            lambda algorithm=algorithm: forge.paths(
                handles["Source"],
                handles["Source"],
                by=algorithm,
                **options,
            ),
            f"expected ExecutionError from {algorithm} identical endpoints",
        )

    invalid = g.GraphForge()
    invalid_source = invalid.add_node("Person", name="Source")
    invalid_sink = invalid.add_node("Person", name="Sink")
    invalid.execute(
        "MATCH (s:Person {name:'Source'}), (t:Person {name:'Sink'}) "
        "CREATE (s)-[:PIPE {capacity:-1.0}]->(t)"
    )
    for algorithm in ("max_flow", "max_flow_edges"):
        expect_execution_error(
            lambda algorithm=algorithm: invalid.paths(
                invalid_source,
                invalid_sink,
                by=algorithm,
                **options,
            ),
            f"expected ExecutionError from {algorithm} negative capacity",
        )


def check_minimum_cost_maximum_flow_paths() -> None:
    forge = g.GraphForge()
    handles = {name: forge.add_node("Person", name=name) for name in ("Source", "A", "Sink")}
    forge.execute(
        "MATCH (s:Person {name:'Source'}), (a:Person {name:'A'}), "
        "(t:Person {name:'Sink'}) "
        "CREATE (s)-[:PIPE {capacity:2.0, cost:-1.0}]->(a), "
        "(a)-[:PIPE {capacity:2.0, cost:3.0}]->(t), "
        "(s)-[:PIPE {capacity:1.0, cost:5.0}]->(t), "
        "(a)-[:PIPE {capacity:9.0, cost:-8.0}]->(a), "
        "(s)-[:OTHER {capacity:100.0, cost:-100.0}]->(t)"
    )
    options = {
        "via": "PIPE",
        "directed": True,
        "capacity_property": "capacity",
        "cost_property": "cost",
    }
    scalar = forge.paths(handles["Source"], handles["Sink"], by="min_cost_max_flow", **options)
    edges = forge.paths(
        handles["Source"],
        handles["Sink"],
        by="min_cost_max_flow_edges",
        **options,
    )

    assert scalar.schema == pa.schema(
        [
            pa.field("source_uuid", pa.binary(16), nullable=False),
            pa.field("sink_uuid", pa.binary(16), nullable=False),
            pa.field("flow", pa.float64(), nullable=False),
            pa.field("cost", pa.float64(), nullable=False),
        ],
        metadata=scalar.schema.metadata,
    )
    assert edges.schema == pa.schema(
        [
            pa.field("edge_uuid", pa.binary(16), nullable=False),
            pa.field("source_uuid", pa.binary(16), nullable=False),
            pa.field("target_uuid", pa.binary(16), nullable=False),
            pa.field("flow", pa.float64(), nullable=False),
            pa.field("unit_cost", pa.float64(), nullable=False),
            pa.field("flow_cost", pa.float64(), nullable=False),
        ],
        metadata=edges.schema.metadata,
    )
    for table, algorithm in (
        (scalar, b"min_cost_max_flow"),
        (edges, b"min_cost_max_flow_edges"),
    ):
        assert table.schema.metadata == {
            b"graphforge.algorithm": algorithm,
            b"graphforge.algorithm_schema_version": b"1",
            b"graphforge.verb": b"paths",
        }
        assert all(field.nullable is False for field in table.schema)

    uuids = {name: uuid.UUID(handle.uuid).bytes for name, handle in handles.items()}
    assert scalar.to_pydict() == {
        "source_uuid": [uuids["Source"]],
        "sink_uuid": [uuids["Sink"]],
        "flow": [3.0],
        "cost": [9.0],
    }
    edge_uuids = edges["edge_uuid"].to_pylist()
    assert len(edge_uuids) == len(set(edge_uuids)) == 4
    assert edge_uuids == sorted(edge_uuids)
    assert all(isinstance(value, bytes) and len(value) == 16 for value in edge_uuids)
    assert all(
        isinstance(value, bytes) and len(value) == 16
        for name in ("source_uuid", "target_uuid")
        for value in edges[name].to_pylist()
    )
    rows = edges.to_pylist()
    assert sum(row["flow_cost"] for row in rows) == scalar["cost"][0].as_py()
    assert all(row["flow_cost"] == row["flow"] * row["unit_cost"] for row in rows)
    assert (
        sum(row["flow"] for row in rows if row["source_uuid"] == uuids["Source"])
        == scalar["flow"][0].as_py()
    )
    assert (
        sum(row["flow"] for row in rows if row["target_uuid"] == uuids["Sink"])
        == scalar["flow"][0].as_py()
    )
    assert edges.equals(
        forge.paths(
            handles["Source"].uuid,
            handles["Sink"].uuid,
            by="min_cost_max_flow_edges",
            **options,
        )
    )

    unit_capacity = forge.paths(
        handles["Source"],
        handles["Sink"],
        by="min_cost_max_flow",
        via="PIPE",
        directed=True,
        cost_property="cost",
    )
    assert unit_capacity["flow"].to_pylist() == [2.0]

    for algorithm in ("min_cost_max_flow", "min_cost_max_flow_edges"):
        _expect_validation_error(
            lambda algorithm=algorithm: forge.paths(
                handles["Source"],
                handles["Sink"],
                by=algorithm,
                via="PIPE",
                directed=True,
            )
        )
        _expect_validation_error(
            lambda algorithm=algorithm: forge.paths(
                "not-a-uuid",
                handles["Sink"],
                by=algorithm,
                **options,
            )
        )
        try:
            forge.paths(
                handles["Source"],
                handles["Source"],
                by=algorithm,
                **options,
            )
        except g.ExecutionError as exc:
            assert "distinct endpoints" in str(exc)
        else:
            raise SystemExit(f"expected ExecutionError from {algorithm} equal endpoints")


def check_minimum_cut_paths() -> None:
    forge = g.GraphForge()
    nodes = {
        name: forge.add_node("Person", name=name)
        for name in ("Source", "A", "B", "Sink", "Unreachable")
    }
    forge.execute(
        "MATCH (s:Person {name:'Source'}), (a:Person {name:'A'}), "
        "(b:Person {name:'B'}), (t:Person {name:'Sink'}) "
        "CREATE (s)-[:PIPE {capacity:3.0}]->(a), "
        "(s)-[:PIPE {capacity:2.0}]->(b), "
        "(a)-[:PIPE {capacity:1.0}]->(b), "
        "(a)-[:PIPE {capacity:2.0}]->(t), "
        "(b)-[:PIPE {capacity:4.0}]->(t), "
        "(a)-[:PIPE {capacity:7.0}]->(a), "
        "(b)-[:PIPE {capacity:0.0}]->(a), "
        "(s)-[:OTHER {capacity:100.0}]->(t)"
    )
    options = {"via": "PIPE", "directed": True, "weight": "capacity"}

    def run(by: str, target=nodes["Sink"], **extra):
        return forge.paths(nodes["Source"], target, by=by, **(options | extra))

    scalar = run("min_cut")
    edges = run("min_cut_edges")
    expected_schemas = {
        "min_cut": [
            pa.field("source_uuid", pa.binary(16), nullable=False),
            pa.field("sink_uuid", pa.binary(16), nullable=False),
            pa.field("cut_value", pa.float64(), nullable=False),
        ],
        "min_cut_edges": [
            pa.field("edge_uuid", pa.binary(16), nullable=False),
            pa.field("source_uuid", pa.binary(16), nullable=False),
            pa.field("target_uuid", pa.binary(16), nullable=False),
            pa.field("capacity", pa.float64(), nullable=False),
        ],
    }
    for table, algorithm in ((scalar, "min_cut"), (edges, "min_cut_edges")):
        assert list(table.schema) == expected_schemas[algorithm]
        assert table.schema.metadata == {
            b"graphforge.algorithm": algorithm.encode(),
            b"graphforge.algorithm_schema_version": b"1",
            b"graphforge.verb": b"paths",
        }
        assert all(column.null_count == 0 for column in table.columns)
        assert not {
            "node_id",
            "edge_id",
            "provenance_id",
            "confidence",
            "assertion_uuid",
            "belief_status",
            "valid_time",
        }.intersection(table.column_names)

    uuids = {name: uuid.UUID(node.uuid).bytes for name, node in nodes.items()}
    assert scalar.to_pydict() == {
        "source_uuid": [uuids["Source"]],
        "sink_uuid": [uuids["Sink"]],
        "cut_value": [5.0],
    }
    edge_rows = edges.to_pylist()
    assert [row["edge_uuid"] for row in edge_rows] == sorted(row["edge_uuid"] for row in edge_rows)
    assert {(row["source_uuid"], row["target_uuid"], row["capacity"]) for row in edge_rows} == {
        (uuids["Source"], uuids["A"], 3.0),
        (uuids["Source"], uuids["B"], 2.0),
    }
    assert sum(row["capacity"] for row in edge_rows) == scalar["cut_value"][0].as_py()
    assert scalar.equals(run("min_cut"))
    assert edges.equals(run("min_cut_edges"))
    assert run("min_cut", weight=None)["cut_value"].to_pylist() == [2.0]
    assert run("min_cut", nodes["Unreachable"])["cut_value"].to_pylist() == [0.0]
    assert run("min_cut_edges", nodes["Unreachable"]).num_rows == 0

    undirected = g.GraphForge()
    left = undirected.add_node("Person", name="Left")
    undirected.add_node("Person", name="Middle")
    right = undirected.add_node("Person", name="Right")
    undirected.execute(
        "MATCH (l:Person {name:'Left'}), (m:Person {name:'Middle'}), "
        "(r:Person {name:'Right'}) "
        "CREATE (l)-[:PIPE {capacity:2.0}]->(m), "
        "(m)-[:PIPE {capacity:2.0}]->(r)"
    )
    reverse = undirected.paths(
        right,
        left,
        by="min_cut_edges",
        via="PIPE",
        directed=False,
        weight="capacity",
    )
    assert reverse.to_pylist()[0]["source_uuid"] == uuid.UUID(left.uuid).bytes

    _expect_validation_error(lambda: run("min_cut", None))
    try:
        run("min_cut_edges", nodes["Source"])
    except g.ExecutionError:
        pass
    else:
        raise SystemExit("expected identical-endpoint minimum-cut ExecutionError")
    _expect_validation_error(
        lambda: g.GraphForge().paths(nodes["Source"], nodes["Sink"], by="min_cut")
    )


def check_is_dag() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (x:Animal {name:'Fox'}), "
        "(y:Animal {name:'Wolf'}), (a)-[:KNOWS]->(b), "
        "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), "
        "(x)-[:OTHER]->(y), (y)-[:OTHER]->(x)"
    )

    table = forge.analyze(by="is_dag")
    assert table.schema.names == ["is_dag"]
    assert table.schema.field("is_dag").type == pa.bool_()
    assert not table.schema.field("is_dag").nullable
    assert table.schema.metadata[b"graphforge.algorithm"] == b"is_dag"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert table.num_rows == 1
    assert table["is_dag"][0].as_py() is False
    assert table.equals(forge.analyze(by="is_dag"))
    assert forge.analyze("Person", by="is_dag")["is_dag"][0].as_py() is True
    assert forge.analyze(by="is_dag", via="KNOWS")["is_dag"][0].as_py() is True
    assert forge.analyze("Person", by="is_dag", directed=False)["is_dag"][0].as_py() is False

    empty = g.GraphForge()
    assert empty.analyze(by="is_dag")["is_dag"][0].as_py() is True
    _expect_validation_error(lambda: empty.analyze("", by="is_dag"))
    _expect_validation_error(lambda: empty.analyze(by="is_dag", via=" "))


def check_topological_sort() -> None:
    forge = g.GraphForge()
    people = sorted(
        [(name, forge.add_node("Person", name=name)) for name in ["Alice", "Bob", "Carol", "Dan"]],
        key=lambda item: uuid.UUID(item[1].uuid).bytes,
    )
    forge.add_node("Animal", name="Fox")
    forge.add_node("Animal", name="Wolf")
    forge.execute(
        f"MATCH (a:Person {{name:'{people[0][0]}'}}), "
        f"(b:Person {{name:'{people[1][0]}'}}), "
        f"(c:Person {{name:'{people[2][0]}'}}), "
        "(f:Animal {name:'Fox'}), (w:Animal {name:'Wolf'}) "
        "CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), "
        "(b)-[:KNOWS]->(c), (f)-[:OTHER]->(w), (w)-[:OTHER]->(f)"
    )

    table = forge.analyze("Person", by="topological_sort")
    assert table.schema.names == ["node_uuid", "order"]
    assert [field.type for field in table.schema] == [pa.binary(16), pa.uint64()]
    assert [field.nullable for field in table.schema] == [False, False]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"topological_sort"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert "node_id" not in table.column_names
    assert table["node_uuid"].to_pylist() == [uuid.UUID(handle.uuid).bytes for _, handle in people]
    assert table["order"].to_pylist() == [0, 1, 2, 3]
    assert table.equals(forge.analyze("Person", by="topological_sort"))

    via = forge.analyze(by="topological_sort", via="KNOWS")
    assert via.num_rows == 6
    assert via["order"].to_pylist() == list(range(6))
    try:
        forge.analyze(by="topological_sort")
    except g.ExecutionError as error:
        assert str(error) == ("Rust algorithm execution failed: selected graph contains a cycle")
    else:
        raise SystemExit("expected topological_sort cycle failure")

    empty = g.GraphForge()
    empty_table = empty.analyze(by="topological_sort")
    assert empty_table.num_rows == 0
    assert empty_table.schema == empty.analyze("Missing", by="topological_sort").schema

    invalid_calls = [
        (
            lambda: empty.analyze(by="topological_sort", directed=False),
            "topological_sort requires directed=true",
        ),
        (
            lambda: empty.analyze(by="topological_sort", weight="cost"),
            "topological_sort does not accept an edge weight property",
        ),
        (
            lambda: empty.analyze("", by="topological_sort"),
            'invalid analyze label ""',
        ),
        (
            lambda: empty.analyze(by="topological_sort", via=" "),
            'invalid analyze relationship selector " "',
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_articulation_points() -> None:
    forge = g.GraphForge()
    nodes = {
        name: forge.add_node("Person", name=name)
        for name in ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus", "Hal"]
    }
    forge.add_node("Animal", name="Wolf")
    forge.add_node("Animal", name="Yak")
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}), (f:Person {name:'Fox'}), "
        "(g:Person {name:'Gus'}), (w:Animal {name:'Wolf'}), "
        "(y:Animal {name:'Yak'}) "
        "CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), "
        "(b)-[:ROAD]->(d), (d)-[:ROAD]->(b), (d)-[:ROAD]->(e), "
        "(d)-[:ROAD]->(d), (f)-[:ROAD]->(g), (a)-[:OTHER]->(e), "
        "(w)-[:ROAD]->(y)"
    )

    options = {
        "by": "articulation_points",
        "via": "ROAD",
        "directed": False,
    }
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("node_uuid", pa.binary(16), False)
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"articulation_points"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert "node_id" not in table.column_names
    expected = sorted(uuid.UUID(nodes[name].uuid).bytes for name in ["Bob", "Dan"])
    assert table["node_uuid"].to_pylist() == expected
    assert table["node_uuid"].null_count == 0
    assert table.equals(forge.analyze("Person", **options))

    no_result = forge.analyze(
        "Person",
        by="articulation_points",
        via="OTHER",
        directed=False,
    )
    missing = forge.analyze(
        "Missing",
        by="articulation_points",
        via="ROAD",
        directed=False,
    )
    empty = g.GraphForge().analyze(by="articulation_points", directed=False)
    assert no_result.num_rows == 0
    assert missing.num_rows == 0
    assert empty.num_rows == 0
    assert empty.schema == table.schema

    invalid_calls = [
        (
            lambda: forge.analyze(by="articulation_points"),
            "articulation_points requires directed=false",
        ),
        (
            lambda: forge.analyze(
                by="articulation_points",
                directed=False,
                weight="cost",
            ),
            "articulation_points does not accept an edge weight property",
        ),
        (
            lambda: forge.analyze(
                by="articulation_points",
                via=" ",
                directed=False,
            ),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="articulation_points", directed=False),
            'invalid analyze label ""',
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_bridges() -> None:
    forge = g.GraphForge()
    nodes = {
        name: forge.add_node("Person", name=name)
        for name in ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus", "Hal"]
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}), (f:Person {name:'Fox'}), "
        "(g:Person {name:'Gus'}) "
        "CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), "
        "(b)-[:ROAD]->(d), (d)-[:ROAD]->(b), (d)-[:ROAD]->(e), "
        "(d)-[:ROAD]->(d), (f)-[:ROAD]->(g), (a)-[:OTHER]->(e)"
    )

    options = {"by": "bridges", "via": "ROAD", "directed": False}
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("edge_uuid", pa.binary(16), False),
        ("source_uuid", pa.binary(16), False),
        ("target_uuid", pa.binary(16), False),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"bridges"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert table["edge_uuid"].null_count == 0
    assert table["source_uuid"].null_count == 0
    assert table["target_uuid"].null_count == 0
    assert table.equals(forge.analyze("Person", **options))
    actual = list(
        zip(
            table["source_uuid"].to_pylist(),
            table["target_uuid"].to_pylist(),
            table["edge_uuid"].to_pylist(),
            strict=True,
        )
    )
    expected_pairs = {
        tuple(sorted((uuid.UUID(nodes[left].uuid).bytes, uuid.UUID(nodes[right].uuid).bytes)))
        for left, right in [("Dan", "Eve"), ("Fox", "Gus")]
    }
    assert len(actual) == len(expected_pairs)
    assert {(source, target) for source, target, _ in actual} == expected_pairs
    assert actual == sorted(actual, key=lambda row: (row[0], row[1], row[2]))

    missing = forge.analyze("Missing", **options)
    empty = g.GraphForge().analyze(by="bridges", directed=False)
    assert missing.num_rows == 0
    assert empty.num_rows == 0
    assert missing.schema == table.schema
    assert empty.schema == table.schema
    invalid_calls = [
        (
            lambda: forge.analyze(by="bridges"),
            "bridges requires directed=false",
        ),
        (
            lambda: forge.analyze(by="bridges", directed=False, weight="cost"),
            "bridges does not accept an edge weight property",
        ),
        (
            lambda: forge.analyze(by="bridges", via=" ", directed=False),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="bridges", directed=False),
            'invalid analyze label ""',
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_minimum_spanning_tree() -> None:
    forge = g.GraphForge()
    nodes = {
        name: forge.add_node("Person", name=name)
        for name in ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox"]
    }
    forge.add_node("Animal", name="Wolf")
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}), (f:Person {name:'Fox'}) "
        "CREATE (a)-[:ROAD {cost:4.0}]->(b), "
        "(a)-[:ROAD {cost:3.0}]->(c), (b)-[:ROAD {cost:1.0}]->(c), "
        "(b)-[:ROAD {cost:2.0}]->(d), (c)-[:ROAD {cost:4.0}]->(d), "
        "(e)-[:ROAD {cost:-2.0}]->(f), (e)-[:ROAD {cost:3.0}]->(f), "
        "(d)-[:ROAD {cost:-10.0}]->(d), "
        "(a)-[:OTHER {cost:-100.0}]->(d)"
    )

    options = {
        "by": "minimum_spanning_tree",
        "via": "ROAD",
        "directed": False,
        "weight": "cost",
    }
    table = forge.analyze("Person", **options)
    assert table.schema.names == [
        "edge_uuid",
        "source_uuid",
        "target_uuid",
        "weight",
    ]
    assert [field.type for field in table.schema] == [
        pa.binary(16),
        pa.binary(16),
        pa.binary(16),
        pa.float64(),
    ]
    assert [field.nullable for field in table.schema] == [False, False, False, True]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"minimum_spanning_tree"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert table["weight"].to_pylist() == [-2.0, 1.0, 2.0, 3.0]
    assert table["weight"].null_count == 0
    expected_pairs = [
        ("Eve", "Fox"),
        ("Bob", "Carol"),
        ("Bob", "Dan"),
        ("Alice", "Carol"),
    ]
    expected_pairs = [
        sorted([uuid.UUID(nodes[left].uuid).bytes, uuid.UUID(nodes[right].uuid).bytes])
        for left, right in expected_pairs
    ]
    actual_pairs = [
        [table["source_uuid"][row].as_py(), table["target_uuid"][row].as_py()]
        for row in range(table.num_rows)
    ]
    assert actual_pairs == expected_pairs
    assert all(source < target for source, target in actual_pairs)
    assert len(set(table["edge_uuid"].to_pylist())) == table.num_rows
    assert table.equals(forge.analyze("Person", **options))
    assert forge.analyze("Missing", **options).num_rows == 0

    tied = g.GraphForge()
    tied_nodes = sorted(
        [tied.add_node("Person", name=name) for name in ["Alice", "Bob", "Carol"]],
        key=lambda handle: uuid.UUID(handle.uuid).bytes,
    )
    tied.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}) "
        "CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), "
        "(a)-[:ROAD]->(c), (b)-[:ROAD]->(c)"
    )
    unit = tied.analyze(by="minimum_spanning_tree", via="ROAD", directed=False)
    assert unit["weight"].to_pylist() == [1.0, 1.0]
    assert [
        [unit["source_uuid"][row].as_py(), unit["target_uuid"][row].as_py()]
        for row in range(unit.num_rows)
    ] == [
        [uuid.UUID(tied_nodes[0].uuid).bytes, uuid.UUID(tied_nodes[target].uuid).bytes]
        for target in [1, 2]
    ]
    empty = g.GraphForge().analyze(by="minimum_spanning_tree", directed=False)
    assert empty.num_rows == 0
    assert empty.schema == unit.schema

    invalid = g.GraphForge()
    invalid.execute(
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(a)-[:ROAD {null_cost:null, text_cost:'heavy', "
        "infinite_cost:1e308 * 2.0}]->(b)"
    )

    def expect_message(message: str, call) -> None:
        try:
            call()
        except g.ValidationError as error:
            assert str(error) == message
        else:
            raise SystemExit(f"expected ValidationError: {message}")

    expect_message(
        "minimum_spanning_tree requires directed=false",
        lambda: invalid.analyze(by="minimum_spanning_tree"),
    )
    expect_message(
        'invalid analyze relationship selector " "',
        lambda: invalid.analyze(by="minimum_spanning_tree", via=" ", directed=False),
    )
    expect_message(
        'invalid analyze weight property " "',
        lambda: invalid.analyze(by="minimum_spanning_tree", via="ROAD", directed=False, weight=" "),
    )
    expect_message(
        'edge weight property "missing" does not exist',
        lambda: invalid.analyze(
            by="minimum_spanning_tree",
            via="ROAD",
            directed=False,
            weight="missing",
        ),
    )
    expect_message(
        'edge weight property "text_cost" must be numeric',
        lambda: invalid.analyze(
            by="minimum_spanning_tree",
            via="ROAD",
            directed=False,
            weight="text_cost",
        ),
    )

    def expect_strict_weight_error(property_name: str) -> None:
        try:
            invalid.analyze(
                by="minimum_spanning_tree",
                via="ROAD",
                directed=False,
                weight=property_name,
            )
        except g.ValidationError as error:
            prefix = "edge weight is missing, NULL, NaN, or infinite for edge "
            assert str(error).startswith(prefix)
            assert len(str(error).removeprefix(prefix)) == 36
        else:
            raise SystemExit(f"expected strict {property_name} ValidationError")

    for property_name in ["null_cost", "infinite_cost"]:
        expect_strict_weight_error(property_name)
    expect_message(
        "is_dag does not accept an edge weight property",
        lambda: invalid.analyze(by="is_dag", weight="cost"),
    )


def check_minimum_k_spanning_tree() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (a:Node), (b:Node), (c:Node), "
        "(a)-[:LINK {weight:1}]->(b), "
        "(a)-[:LINK {weight:1}]->(c), "
        "(b)-[:LINK {weight:2}]->(c)"
    )
    options = {
        "by": "minimum_k_spanning_tree",
        "via": "LINK",
        "directed": False,
        "weight": "weight",
    }

    default = forge.analyze("Node", **options)
    assert [(field.name, field.type, field.nullable) for field in default.schema] == [
        ("tree_id", pa.uint64(), False),
        ("edge_uuid", pa.binary(16), False),
        ("source_uuid", pa.binary(16), False),
        ("target_uuid", pa.binary(16), False),
        ("weight", pa.float64(), False),
    ]
    assert default.schema.metadata[b"graphforge.algorithm"] == b"minimum_k_spanning_tree"
    assert default.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert default.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
    assert default["tree_id"].to_pylist() == [0, 0]
    assert all(column.null_count == 0 for column in default.columns)
    assert "edge_id" not in default.schema.names

    explicit = forge.analyze("Node", **options, k=2)
    assert explicit["tree_id"].to_pylist() == [0, 0, 1, 1]
    assert explicit["weight"].to_pylist() == [1.0, 1.0, 1.0, 2.0]
    assert explicit.equals(forge.analyze("Node", **options, k=2))
    assert explicit.slice(0, default.num_rows).equals(default)

    try:
        forge.analyze("Node", **options, k=0)
    except g.ValidationError as error:
        assert str(error) == "minimum_k_spanning_tree requires k greater than zero"
    else:
        raise SystemExit("expected minimum-k k=0 ValidationError")


def check_maximum_spanning_tree() -> None:
    forge = g.GraphForge()
    nodes = {
        name: forge.add_node("Person", name=name)
        for name in ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus"]
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}), (f:Person {name:'Fox'}) "
        "CREATE (a)-[:ROAD {cost:4.0}]->(b), "
        "(a)-[:ROAD {cost:9.0}]->(b), (b)-[:ROAD {cost:8.0}]->(a), "
        "(a)-[:ROAD {cost:7.0}]->(c), (b)-[:ROAD {cost:6.0}]->(c), "
        "(b)-[:ROAD {cost:-3.0}]->(d), (c)-[:ROAD {cost:-1.0}]->(d), "
        "(e)-[:ROAD {cost:-5.0}]->(f), (e)-[:ROAD {cost:-2.0}]->(f), "
        "(d)-[:ROAD {cost:1e308}]->(d), "
        "(a)-[:OTHER {cost:100.0}]->(d)"
    )
    options = {
        "by": "maximum_spanning_tree",
        "via": "ROAD",
        "directed": False,
        "weight": "cost",
    }
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("edge_uuid", pa.binary(16), False),
        ("source_uuid", pa.binary(16), False),
        ("target_uuid", pa.binary(16), False),
        ("weight", pa.float64(), True),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"maximum_spanning_tree"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert table.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
    assert table["edge_uuid"].null_count == 0
    assert table["source_uuid"].null_count == 0
    assert table["target_uuid"].null_count == 0
    assert table["weight"].to_pylist() == [9.0, 7.0, -1.0, -2.0]
    assert table["weight"].null_count == 0
    expected_pairs = [
        ("Alice", "Bob"),
        ("Alice", "Carol"),
        ("Carol", "Dan"),
        ("Eve", "Fox"),
    ]
    expected_pairs = [
        sorted([uuid.UUID(nodes[left].uuid).bytes, uuid.UUID(nodes[right].uuid).bytes])
        for left, right in expected_pairs
    ]
    actual_pairs = [
        [table["source_uuid"][row].as_py(), table["target_uuid"][row].as_py()]
        for row in range(table.num_rows)
    ]
    assert actual_pairs == expected_pairs
    assert all(source < target for source, target in actual_pairs)
    assert uuid.UUID(nodes["Gus"].uuid).bytes not in {
        endpoint for pair in actual_pairs for endpoint in pair
    }
    assert table.equals(forge.analyze("Person", **options))

    missing = forge.analyze("Missing", **options)
    empty = g.GraphForge().analyze(by="maximum_spanning_tree", directed=False)
    for result in [missing, empty]:
        assert result.num_rows == 0
        assert list(result.schema) == list(table.schema)
        assert result.schema.metadata == table.schema.metadata
    invalid_calls = [
        (
            lambda: forge.analyze(by="maximum_spanning_tree"),
            "maximum_spanning_tree requires directed=false",
        ),
        (
            lambda: forge.analyze(by="maximum_spanning_tree", via=" ", directed=False),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze(
                by="maximum_spanning_tree",
                via="ROAD",
                directed=False,
                weight=" ",
            ),
            'invalid analyze weight property " "',
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_triangle_count() -> None:
    forge = g.GraphForge()
    for name in ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus"]:
        forge.add_node("Person", name=name)
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}), (f:Person {name:'Fox'}) "
        "CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), "
        "(b)-[:ROAD]->(c), (c)-[:ROAD]->(a), (a)-[:ROAD]->(a), "
        "(d)-[:ROAD]->(e), (e)-[:ROAD]->(f), (f)-[:ROAD]->(d), "
        "(a)-[:OTHER]->(d)"
    )
    options = {"by": "triangle_count", "via": "ROAD", "directed": False}
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("triangle_count", pa.uint64(), False),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"triangle_count"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert table.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
    assert table.num_rows == 1
    assert table["triangle_count"].null_count == 0
    assert table["triangle_count"].to_pylist() == [2]
    assert table.equals(forge.analyze("Person", **options))

    missing = forge.analyze("Missing", **options)
    empty = g.GraphForge().analyze(by="triangle_count", directed=False)
    other = forge.analyze("Person", by="triangle_count", via="OTHER", directed=False)
    for result in [missing, empty, other]:
        assert result.num_rows == 1
        assert result["triangle_count"].to_pylist() == [0]
        assert result["triangle_count"].null_count == 0
        assert list(result.schema) == list(table.schema)
        assert result.schema.metadata == table.schema.metadata

    invalid_calls = [
        (
            lambda: forge.analyze(by="triangle_count"),
            "triangle_count requires directed=false",
        ),
        (
            lambda: forge.analyze(by="triangle_count", via=" ", directed=False),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="triangle_count", directed=False),
            'invalid analyze label ""',
        ),
        (
            lambda: forge.analyze(by="triangle_count", directed=False, weight="cost"),
            "triangle_count does not accept an edge weight property",
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_transitivity() -> None:
    forge = g.GraphForge()
    for label, name in [
        ("Person", "Alice"),
        ("Person", "Bob"),
        ("Person", "Carol"),
        ("Person", "Dan"),
        ("Person", "Eve"),
        ("Animal", "Fox"),
    ]:
        forge.add_node(label, name=name)
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}), (f:Animal {name:'Fox'}) "
        "CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), "
        "(b)-[:ROAD]->(c), (c)-[:ROAD]->(a), "
        "(b)-[:ROAD]->(d), (c)-[:ROAD]->(d), "
        "(d)-[:ROAD]->(d), (a)-[:OTHER]->(e), "
        "(e)-[:OTHER]->(c), (f)-[:ROAD]->(a)"
    )

    options = {"by": "transitivity", "via": "ROAD", "directed": False}
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("transitivity", pa.float64(), False),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"transitivity"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert table.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
    assert table.num_rows == 1
    assert table["transitivity"].null_count == 0
    assert table["transitivity"].to_pylist() == [0.75]
    assert table.equals(forge.analyze("Person", **options))

    zero_results = [
        forge.analyze("Person", by="transitivity", via="OTHER", directed=False),
        forge.analyze("Missing", **options),
        g.GraphForge().analyze(by="transitivity", directed=False),
    ]
    for result in zero_results:
        assert result.num_rows == 1
        assert result["transitivity"].null_count == 0
        assert result["transitivity"].to_pylist() == [0.0]
        assert list(result.schema) == list(table.schema)
        assert result.schema.metadata == table.schema.metadata

    invalid_calls = [
        (
            lambda: forge.analyze(by="transitivity"),
            "transitivity requires directed=false",
        ),
        (
            lambda: forge.analyze(by="transitivity", via=" ", directed=False),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="transitivity", directed=False),
            'invalid analyze label ""',
        ),
        (
            lambda: forge.analyze(
                by="transitivity",
                directed=False,
                weight="cost",
            ),
            "transitivity does not accept an edge weight property",
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_conductance() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE "
        "(a:Person {side:'alpha'}), "
        "(b:Person {side:'alpha'}), "
        "(c:Person {side:'beta'}), "
        "(d:Person {side:'beta'}), "
        "(a)-[:LINK {weight:2}]->(c), "
        "(a)-[:LINK {weight:1}]->(c), "
        "(b)-[:LINK {weight:1}]->(c), "
        "(a)-[:LINK {weight:3}]->(b), "
        "(d)-[:LINK {weight:4}]->(d)"
    )
    options = {
        "by": "conductance",
        "directed": False,
        "weight": "weight",
        "partition_property": "side",
    }
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("partition_id", pa.string(), False),
        ("conductance", pa.float64(), False),
    ]
    assert table.schema.metadata == {
        b"graphforge.algorithm": b"conductance",
        b"graphforge.verb": b"analyze",
        b"graphforge.algorithm_schema_version": b"1",
    }
    assert table.to_pydict() == {
        "partition_id": ["alpha", "beta"],
        "conductance": [0.4, 0.4],
    }
    assert table.equals(forge.analyze("Person", **options))

    _expect_validation_message(
        "conductance requires directed=false",
        lambda: forge.analyze(
            "Person",
            by="conductance",
            partition_property="side",
        ),
    )
    _expect_validation_message(
        "conductance requires a non-empty partition_property",
        lambda: forge.analyze("Person", by="conductance", directed=False),
    )

    for properties, expected in [
        ("", "missing a partition value"),
        ("side:1.5", "unsupported partition type"),
    ]:
        node = "(b:Person)" if not properties else f"(b:Person {{{properties}}})"
        invalid = g.GraphForge()
        invalid.execute(f"CREATE (a:Person {{side:'alpha'}}), {node}, (a)-[:LINK]->(b)")
        try:
            invalid.analyze(
                "Person",
                by="conductance",
                directed=False,
                partition_property="side",
            )
        except g.ValidationError as error:
            assert expected in str(error), error
        else:
            raise SystemExit(f"expected conductance ValidationError: {expected}")

    zero_volume = g.GraphForge()
    zero_volume.execute("CREATE (a:Person {side:'alpha'}), (b:Person {side:'beta'})")
    try:
        zero_volume.analyze(
            "Person",
            by="conductance",
            directed=False,
            partition_property="side",
        )
    except g.ExecutionError as error:
        assert "conductance is undefined for partition alpha" in str(error), error
        assert "denominator volume is zero" in str(error), error
    else:
        raise SystemExit("expected undefined-conductance ExecutionError")


def _modularity_fixture() -> g.GraphForge:
    forge = g.GraphForge()
    forge.execute(
        "CREATE "
        "(a:Person {side:'alpha', bucket:1}), "
        "(b:Person {side:'alpha', bucket:1}), "
        "(c:Person {side:'beta', bucket:2}), "
        "(d:Person {side:'beta', bucket:2}), "
        "(a)-[:LINK {weight:2}]->(b), "
        "(a)-[:LINK {weight:1}]->(b), "
        "(c)-[:LINK {weight:2}]->(d), "
        "(b)-[:LINK {weight:1}]->(c), "
        "(a)-[:LINK {weight:3}]->(a)"
    )
    return forge


def check_modularity_result() -> None:
    forge = _modularity_fixture()
    options = {
        "by": "modularity",
        "directed": False,
        "weight": "weight",
        "partition_property": "side",
    }
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("modularity", pa.float64(), False),
    ]
    assert table.schema.metadata == {
        b"graphforge.algorithm": b"modularity",
        b"graphforge.verb": b"analyze",
        b"graphforge.algorithm_schema_version": b"1",
    }
    assert table.num_rows == 1
    assert table["modularity"].null_count == 0
    expected = 6.0 / 9.0 - (13.0 / 18.0) ** 2 + 2.0 / 9.0 - (5.0 / 18.0) ** 2
    assert table["modularity"][0].as_py() == expected
    assert table.equals(forge.analyze("Person", **options))

    integer = forge.analyze(
        "Person",
        by="modularity",
        directed=False,
        weight="weight",
        partition_property="bucket",
    )
    assert table.equals(integer)


def check_modularity_unit_weight_and_knowledge_boundary() -> None:
    forge = _modularity_fixture()
    table = forge.analyze(
        "Person",
        by="modularity",
        directed=False,
        partition_property="side",
    )
    expected = 3.0 / 5.0 - (7.0 / 10.0) ** 2 + 1.0 / 5.0 - (3.0 / 10.0) ** 2
    assert table["modularity"].to_pylist() == [expected]
    forbidden = {
        "confidence",
        "provenance_id",
        "evidence_uuid",
        "assertion_uuid",
        "belief_status",
        "hypothesis_uuid",
        "valid_time",
        "as_of",
    }
    assert forbidden.isdisjoint(table.column_names)


def check_modularity_errors() -> None:
    forge = _modularity_fixture()
    _expect_validation_message(
        "modularity requires directed=false",
        lambda: forge.analyze(
            "Person",
            by="modularity",
            partition_property="side",
        ),
    )

    incomplete = g.GraphForge()
    incomplete.execute("CREATE (a:Person {side:'alpha'}), (b:Person), (a)-[:LINK]->(b)")
    try:
        incomplete.analyze(
            "Person",
            by="modularity",
            directed=False,
            partition_property="side",
        )
    except g.ValidationError as error:
        assert "missing a partition value" in str(error), error
    else:
        raise SystemExit("expected incomplete-partition modularity ValidationError")


def check_max_weight_matching() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE "
        "(a:Node), (b:Node), (c:Node), (d:Node), "
        "(e:Node), (f:Node), (g:Node), "
        "(a)-[:PAIR {tag:'ab0', weight:10}]->(b), "
        "(a)-[:PAIR {tag:'ab1', weight:10}]->(b), "
        "(b)-[:PAIR {tag:'bc', weight:7}]->(c), "
        "(c)-[:PAIR {tag:'ca', weight:6}]->(a), "
        "(d)-[:PAIR {tag:'de', weight:5}]->(e), "
        "(f)-[:PAIR {tag:'fg', weight:-2}]->(g), "
        "(a)-[:PAIR {tag:'loop', weight:100}]->(a)"
    )
    options = {
        "by": "max_weight_matching",
        "via": "PAIR",
        "directed": False,
        "weight": "weight",
    }
    table = forge.analyze("Node", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("edge_uuid", pa.binary(16), False),
        ("source_uuid", pa.binary(16), False),
        ("target_uuid", pa.binary(16), False),
        ("weight", pa.float64(), True),
    ]
    assert table.schema.metadata == {
        b"graphforge.algorithm": b"max_weight_matching",
        b"graphforge.verb": b"analyze",
        b"graphforge.algorithm_schema_version": b"1",
    }
    assert all(table[name].null_count == 0 for name in table.column_names)

    topology = forge.execute(
        "MATCH (a)-[r:PAIR]->(b) "
        "RETURN r.tag AS tag, r.edge_uuid AS edge_uuid, "
        "a.node_uuid AS source_uuid, b.node_uuid AS target_uuid"
    )
    edges = {}
    for tag, edge, source, target in zip(
        topology["tag"].to_pylist(),
        topology["edge_uuid"].to_pylist(),
        topology["source_uuid"].to_pylist(),
        topology["target_uuid"].to_pylist(),
        strict=True,
    ):
        edges[tag] = (edge, *sorted([source, target]))
    parallel = min(edges["ab0"], edges["ab1"])
    expected = sorted(
        [
            (*parallel, 10.0),
            (*edges["de"], 5.0),
        ],
        key=lambda row: (row[1], row[2], row[0]),
    )
    rows = list(
        zip(
            table["edge_uuid"].to_pylist(),
            table["source_uuid"].to_pylist(),
            table["target_uuid"].to_pylist(),
            table["weight"].to_pylist(),
            strict=True,
        )
    )
    assert rows == expected
    assert table.equals(forge.analyze("Node", **options))

    unit = forge.analyze("Node", by="max_weight_matching", via="PAIR", directed=False)
    assert unit.num_rows == 3
    assert unit["weight"].to_pylist() == [1.0, 1.0, 1.0]
    _expect_validation_message(
        "max_weight_matching requires directed=false",
        lambda: forge.analyze("Node", by="max_weight_matching", via="PAIR"),
    )

    invalid = g.GraphForge()
    invalid.execute("CREATE (a:Node), (b:Node), (a)-[:PAIR {weight:1e308 * 2.0}]->(b)")
    try:
        invalid.analyze(
            "Node",
            by="max_weight_matching",
            via="PAIR",
            directed=False,
            weight="weight",
        )
    except g.ValidationError as error:
        assert str(error).startswith("edge weight is missing, NULL, NaN, or infinite for edge ")
    else:
        raise SystemExit("expected nonfinite max_weight_matching ValidationError")


def _max_cardinality_fixture() -> g.GraphForge:
    forge = g.GraphForge()
    forge.execute(
        "CREATE "
        "(a:Node), (b:Node), (c:Node), (d:Node), "
        "(e:Node), (f:Node), (g:Node), (h:Node), "
        "(a)-[:PAIR {tag:'ab0'}]->(b), "
        "(a)-[:PAIR {tag:'ab1'}]->(b), "
        "(b)-[:PAIR {tag:'bc'}]->(c), "
        "(c)-[:PAIR {tag:'ca'}]->(a), "
        "(b)-[:PAIR {tag:'bd'}]->(d), "
        "(c)-[:PAIR {tag:'ce'}]->(e), "
        "(f)-[:PAIR {tag:'fg'}]->(g), "
        "(h)-[:PAIR {tag:'loop'}]->(h)"
    )
    return forge


def _max_cardinality_rows(forge: g.GraphForge) -> list[tuple[bytes, bytes, bytes]]:
    topology = forge.execute(
        "MATCH (a)-[r:PAIR]->(b) "
        "RETURN r.edge_uuid AS edge_uuid, "
        "a.node_uuid AS source_uuid, b.node_uuid AS target_uuid"
    )
    edges = sorted(
        (edge, *sorted([source, target]))
        for edge, source, target in zip(
            topology["edge_uuid"].to_pylist(),
            topology["source_uuid"].to_pylist(),
            topology["target_uuid"].to_pylist(),
            strict=True,
        )
    )
    best: list[tuple[bytes, bytes, bytes]] = []
    for mask in range(1 << len(edges)):
        used: set[bytes] = set()
        candidate: list[tuple[bytes, bytes, bytes]] = []
        for position, edge in enumerate(edges):
            if not mask & (1 << position):
                continue
            _, source, target = edge
            if source == target or source in used or target in used:
                break
            used.update([source, target])
            candidate.append(edge)
        else:
            if len(candidate) > len(best) or (
                len(candidate) == len(best)
                and [row[0] for row in candidate] < [row[0] for row in best]
            ):
                best = candidate
    return best


def check_max_cardinality_matching_result() -> None:
    forge = _max_cardinality_fixture()
    options = {
        "by": "max_cardinality_matching",
        "via": "PAIR",
        "directed": False,
    }
    table = forge.analyze("Node", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("edge_uuid", pa.binary(16), False),
        ("source_uuid", pa.binary(16), False),
        ("target_uuid", pa.binary(16), False),
    ]
    assert table.schema.metadata == {
        b"graphforge.algorithm": b"max_cardinality_matching",
        b"graphforge.verb": b"analyze",
        b"graphforge.algorithm_schema_version": b"1",
    }
    assert all(table[name].null_count == 0 for name in table.column_names)
    forbidden = {
        "confidence",
        "provenance_id",
        "evidence_uuid",
        "assertion_uuid",
        "belief_status",
        "hypothesis_uuid",
        "valid_time",
        "as_of",
    }
    assert forbidden.isdisjoint(table.column_names)

    rows = list(
        zip(
            table["edge_uuid"].to_pylist(),
            table["source_uuid"].to_pylist(),
            table["target_uuid"].to_pylist(),
            strict=True,
        )
    )
    expected = _max_cardinality_rows(forge)
    assert len(expected) == 3
    assert rows == expected
    assert all(source < target for _, source, target in rows)
    assert [edge for edge, _, _ in rows] == sorted(edge for edge, _, _ in rows)
    assert table.equals(forge.analyze("Node", **options))


def check_max_cardinality_matching_empty() -> None:
    forge = g.GraphForge()
    forge.execute("CREATE (:Node), (:Node)")
    table = forge.analyze("Node", by="max_cardinality_matching", directed=False)
    assert table.num_rows == 0
    assert table.column_names == ["edge_uuid", "source_uuid", "target_uuid"]


def check_max_cardinality_matching_errors() -> None:
    forge = _max_cardinality_fixture()
    _expect_validation_message(
        "max_cardinality_matching requires directed=false",
        lambda: forge.analyze("Node", by="max_cardinality_matching", via="PAIR"),
    )
    _expect_validation_message(
        "max_cardinality_matching does not accept an edge weight property",
        lambda: forge.analyze(
            "Node",
            by="max_cardinality_matching",
            via="PAIR",
            directed=False,
            weight="weight",
        ),
    )


def check_max_bipartite_matching() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE "
        "(l1:Person {name:'l1', side:'a'}), "
        "(l2:Person {name:'l2', side:'a'}), "
        "(l3:Person {name:'l3', side:'a'}), "
        "(r1:Person {name:'r1', side:'z'}), "
        "(r2:Person {name:'r2', side:'z'}), "
        "(r3:Person {name:'r3', side:'z'}), "
        "(isolate:Person {name:'isolate', side:'a'}), "
        "(l1)-[:BIPARTITE]->(r1), "
        "(l1)-[:BIPARTITE]->(r2), "
        "(l1)-[:BIPARTITE]->(r2), "
        "(l2)-[:BIPARTITE]->(r1), "
        "(l3)-[:BIPARTITE]->(r2), "
        "(l3)-[:BIPARTITE]->(r3)"
    )
    options = {
        "by": "max_bipartite_matching",
        "via": "BIPARTITE",
        "directed": False,
        "partition_property": "side",
    }
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("edge_uuid", pa.binary(16), False),
        ("source_uuid", pa.binary(16), False),
        ("target_uuid", pa.binary(16), False),
    ]
    assert table.schema.metadata == {
        b"graphforge.algorithm": b"max_bipartite_matching",
        b"graphforge.verb": b"analyze",
        b"graphforge.algorithm_schema_version": b"1",
    }
    rows = list(
        zip(
            table["edge_uuid"].to_pylist(),
            table["source_uuid"].to_pylist(),
            table["target_uuid"].to_pylist(),
            strict=True,
        )
    )
    assert len(rows) == 3
    assert rows == sorted(rows, key=lambda row: (row[1], row[2], row[0]))
    assert table.equals(forge.analyze("Person", **options))

    node_table = forge.execute("MATCH (n:Person) RETURN n.node_uuid AS uuid, n.name AS name")
    names = dict(
        zip(
            node_table["uuid"].to_pylist(),
            node_table["name"].to_pylist(),
            strict=True,
        )
    )
    topology = forge.execute(
        "MATCH (a:Person)-[r:BIPARTITE]->(b:Person) "
        "RETURN r.edge_uuid AS edge_uuid, a.node_uuid AS source_uuid, "
        "b.node_uuid AS target_uuid"
    )
    topology_rows = list(
        zip(
            topology["edge_uuid"].to_pylist(),
            topology["source_uuid"].to_pylist(),
            topology["target_uuid"].to_pylist(),
            strict=True,
        )
    )
    assert len({source for _, source, _ in rows}) == 3
    assert len({target for _, _, target in rows}) == 3
    for edge, source, target in rows:
        assert names[source].startswith("l")
        assert names[target].startswith("r")
        assert names[source] != "isolate" and names[target] != "isolate"
        assert edge == min(
            candidate
            for candidate, candidate_source, candidate_target in topology_rows
            if candidate_source == source and candidate_target == target
        )

    inferred = g.GraphForge()
    inferred.execute(
        "CREATE "
        "(a:Person {name:'a'}), (b:Person {name:'b'}), "
        "(c:Person {name:'c'}), (d:Person {name:'d'}), "
        "(isolate:Person {name:'isolate'}), "
        "(b)-[:BIPARTITE]->(a), (d)-[:BIPARTITE]->(c)"
    )
    inferred_options = {
        "by": "max_bipartite_matching",
        "via": "BIPARTITE",
        "directed": False,
    }
    inferred_table = inferred.analyze("Person", **inferred_options)
    assert inferred_table.num_rows == 2
    assert inferred_table.equals(inferred.analyze("Person", **inferred_options))
    for source, target in zip(
        inferred_table["source_uuid"].to_pylist(),
        inferred_table["target_uuid"].to_pylist(),
        strict=True,
    ):
        assert source < target

    _expect_validation_message(
        "max_bipartite_matching requires directed=false",
        lambda: forge.analyze(
            "Person",
            by="max_bipartite_matching",
            via="BIPARTITE",
            partition_property="side",
        ),
    )
    _expect_validation_message(
        "max_bipartite_matching does not accept an edge weight property",
        lambda: forge.analyze("Person", weight="weight", **options),
    )

    for query, partition_property, expected in [
        (
            "CREATE (a:Person {side:'x'}), (b:Person {side:'x'}), (a)-[:BIPARTITE]->(b)",
            "side",
            "edge-bearing projection must contain exactly two partitions",
        ),
        (
            "CREATE (a:Person {side:'x'}), (b:Person), (a)-[:BIPARTITE]->(b)",
            "side",
            "missing a partition value",
        ),
        (
            "CREATE (a:Person {side:'x'}), (b:Person {side:null}), (a)-[:BIPARTITE]->(b)",
            "side",
            "missing a partition value",
        ),
        (
            "CREATE (a:Person), (b:Person), (c:Person), "
            "(a)-[:BIPARTITE]->(b), (b)-[:BIPARTITE]->(c), "
            "(c)-[:BIPARTITE]->(a)",
            None,
            "selected graph is not bipartite: odd cycle",
        ),
    ]:
        invalid = g.GraphForge()
        invalid.execute(query)
        try:
            invalid.analyze(
                "Person",
                by="max_bipartite_matching",
                via="BIPARTITE",
                directed=False,
                partition_property=partition_property,
            )
        except (g.ValidationError, g.ExecutionError) as error:
            assert expected in str(error), error
        else:
            raise SystemExit(f"expected max_bipartite_matching error: {expected}")


def check_is_planar() -> None:
    forge = g.GraphForge()
    for label, name in [
        ("Person", "A"),
        ("Person", "B"),
        ("Person", "C"),
        ("Person", "D"),
        ("Person", "E"),
        ("Person", "F"),
        ("Animal", "Fox"),
    ]:
        forge.add_node(label, name=name)
    forge.execute(
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), "
        "(c:Person {name:'C'}), (d:Person {name:'D'}), "
        "(e:Person {name:'E'}), (f:Person {name:'F'}), "
        "(fox:Animal {name:'Fox'}) "
        "CREATE (a)-[:ROAD]->(d), (a)-[:ROAD]->(e), (a)-[:ROAD]->(f), "
        "(b)-[:ROAD]->(d), (b)-[:ROAD]->(e), (b)-[:ROAD]->(f), "
        "(c)-[:ROAD]->(d), (c)-[:ROAD]->(e), (c)-[:ROAD]->(f), "
        "(a)-[:ROAD]->(d), (d)-[:ROAD]->(a), (a)-[:ROAD]->(a), "
        "(a)-[:OTHER]->(b), (fox)-[:ROAD]->(a)"
    )

    options = {"by": "is_planar", "via": "ROAD", "directed": False}
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("is_planar", pa.bool_(), False),
    ]
    assert table.schema.metadata == {
        b"graphforge.algorithm": b"is_planar",
        b"graphforge.algorithm_schema_version": b"1",
        b"graphforge.verb": b"analyze",
    }
    assert table.num_rows == 1
    assert table["is_planar"].null_count == 0
    assert table["is_planar"].to_pylist() == [False]
    assert table.equals(forge.analyze("Person", **options))

    for result in [
        forge.analyze("Person", by="is_planar", via="OTHER", directed=False),
        forge.analyze("Missing", **options),
        g.GraphForge().analyze(by="is_planar", directed=False),
    ]:
        assert result["is_planar"].to_pylist() == [True]
        assert list(result.schema) == list(table.schema)
        assert result.schema.metadata == table.schema.metadata

    forest = g.GraphForge()
    forest.execute(
        "CREATE (:Person {name:'A'})-[:ROAD]->(:Person {name:'B'}), "
        "(:Person {name:'C'}), (:Person {name:'D'})-[:ROAD]->(:Person {name:'E'})"
    )
    assert forest.analyze("Person", by="is_planar", via="ROAD", directed=False)[
        "is_planar"
    ].to_pylist() == [True]

    invalid_calls = [
        (
            lambda: forge.analyze(by="is_planar"),
            "is_planar requires directed=false",
        ),
        (
            lambda: forge.analyze(by="is_planar", via=" ", directed=False),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="is_planar", directed=False),
            'invalid analyze label ""',
        ),
        (
            lambda: forge.analyze(by="is_planar", directed=False, weight="cost"),
            "is_planar does not accept an edge weight property",
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_triad_census() -> None:
    forge = g.GraphForge()
    for name in ["Alice", "Bob", "Carol", "Isolate"]:
        forge.add_node("Person", name=name)
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}) "
        "CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), "
        "(a)-[:ROAD]->(a), (a)-[:ROAD]->(b), (a)-[:OTHER]->(c)"
    )
    options = {"by": "triad_census", "via": "ROAD", "directed": True}
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("triad_type", pa.string(), False),
        ("count", pa.uint64(), False),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"triad_census"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert table.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
    assert table["triad_type"].to_pylist() == [
        "003",
        "012",
        "102",
        "021D",
        "021U",
        "021C",
        "111D",
        "111U",
        "030T",
        "030C",
        "201",
        "120D",
        "120U",
        "120C",
        "210",
        "300",
    ]
    assert table["count"].to_pylist() == [0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0]
    assert table.equals(forge.analyze("Person", **options))

    for result in [
        forge.analyze("Missing", **options),
        g.GraphForge().analyze(by="triad_census", directed=True),
    ]:
        assert result.num_rows == 16
        assert sum(result["count"].to_pylist()) == 0
        assert list(result.schema) == list(table.schema)
        assert result.schema.metadata == table.schema.metadata

    invalid_calls = [
        (
            lambda: forge.analyze(by="triad_census", directed=False),
            "triad_census requires directed=true",
        ),
        (
            lambda: forge.analyze(by="triad_census", via=" ", directed=True),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="triad_census", directed=True),
            'invalid analyze label ""',
        ),
        (
            lambda: forge.analyze(by="triad_census", directed=True, weight="cost"),
            "triad_census does not accept an edge weight property",
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_dyad_census() -> None:
    forge = g.GraphForge()
    for name in ["Alice", "Bob", "Carol", "Dan", "Isolate"]:
        forge.add_node("Person", name=name)
    for name in ["Fox", "Owl"]:
        forge.add_node("Animal", name=name)
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}) "
        "CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), "
        "(a)-[:ROAD]->(b), (a)-[:ROAD]->(c), (d)-[:ROAD]->(c), "
        "(a)-[:ROAD]->(a), (c)-[:OTHER]->(a)"
    )
    options = {"by": "dyad_census", "via": "ROAD", "directed": True}
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("dyad_type", pa.string(), False),
        ("count", pa.uint64(), False),
    ]
    assert table.schema.metadata == {
        b"graphforge.algorithm": b"dyad_census",
        b"graphforge.algorithm_schema_version": b"1",
        b"graphforge.verb": b"analyze",
    }
    assert table["dyad_type"].to_pylist() == ["mutual", "asymmetric", "null"]
    assert table["count"].to_pylist() == [1, 2, 7]
    assert table.equals(forge.analyze("Person", **options))

    all_relationships = forge.analyze("Person", by="dyad_census", directed=True)
    assert all_relationships["count"].to_pylist() == [2, 1, 7]
    assert list(all_relationships.schema) == list(table.schema)
    assert all_relationships.schema.metadata == table.schema.metadata

    for result, counts in [
        (forge.analyze("Missing", **options), [0, 0, 0]),
        (forge.analyze("Animal", **options), [0, 0, 1]),
        (g.GraphForge().analyze(by="dyad_census", directed=True), [0, 0, 0]),
    ]:
        assert result.num_rows == 3
        assert result["dyad_type"].to_pylist() == ["mutual", "asymmetric", "null"]
        assert result["count"].to_pylist() == counts
        assert list(result.schema) == list(table.schema)
        assert result.schema.metadata == table.schema.metadata

    singleton = g.GraphForge()
    singleton.add_node("Person", name="Solo")
    assert singleton.analyze("Person", by="dyad_census", directed=True)["count"].to_pylist() == [
        0,
        0,
        0,
    ]

    invalid_calls = [
        (
            lambda: forge.analyze(by="dyad_census", directed=False),
            "dyad_census requires directed=true",
        ),
        (
            lambda: forge.analyze(by="dyad_census", via=" ", directed=True),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="dyad_census", directed=True),
            'invalid analyze label ""',
        ),
        (
            lambda: forge.analyze(by="dyad_census", directed=True, weight="cost"),
            "dyad_census does not accept an edge weight property",
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_node_coloring() -> None:
    # The bindings only select and decode the Rust-owned algorithm (#772).
    forge = g.GraphForge()
    people = sorted(
        [
            (name, forge.add_node("Person", name=name))
            for name in ["Alice", "Bob", "Carol", "Dan", "Eve"]
        ],
        key=lambda item: uuid.UUID(item[1].uuid).bytes,
    )
    forge.add_node("Animal", name="Fox")
    forge.execute(
        f"MATCH (a:Person {{name:'{people[0][0]}'}}), "
        f"(b:Person {{name:'{people[1][0]}'}}), "
        f"(c:Person {{name:'{people[2][0]}'}}), "
        f"(d:Person {{name:'{people[3][0]}'}}), "
        f"(e:Person {{name:'{people[4][0]}'}}), "
        "(f:Animal {name:'Fox'}) "
        "CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(c), "
        "(b)-[:ROAD]->(c), (c)-[:ROAD]->(d), "
        "(a)-[:ROAD]->(b), (b)-[:ROAD]->(a), "
        "(d)-[:OTHER]->(e), (f)-[:ROAD]->(a)"
    )

    options = {"by": "node_coloring", "via": "ROAD", "directed": False}
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("node_uuid", pa.binary(16), False),
        ("color", pa.uint64(), False),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"node_coloring"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert table.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
    assert table["node_uuid"].null_count == 0
    assert table["color"].null_count == 0
    assert table["node_uuid"].to_pylist() == [uuid.UUID(handle.uuid).bytes for _, handle in people]
    assert table["color"].to_pylist() == [0, 1, 2, 0, 0]
    assert table.equals(forge.analyze("Person", **options))

    colors = dict(zip(table["node_uuid"].to_pylist(), table["color"].to_pylist()))
    for left, right in [(0, 1), (0, 2), (1, 2), (2, 3)]:
        assert (
            colors[uuid.UUID(people[left][1].uuid).bytes]
            != colors[uuid.UUID(people[right][1].uuid).bytes]
        )

    missing = forge.analyze("Missing", **options)
    empty = g.GraphForge().analyze(by="node_coloring", directed=False)
    for result in [missing, empty]:
        assert result.num_rows == 0
        assert list(result.schema) == list(table.schema)
        assert result.schema.metadata == table.schema.metadata
        assert result["node_uuid"].null_count == 0
        assert result["color"].null_count == 0

    loop = g.GraphForge()
    loop.add_node("Person", name="Loop")
    loop.execute("MATCH (n:Person) CREATE (n)-[:ROAD]->(n)")
    try:
        loop.analyze("Person", by="node_coloring", via="ROAD", directed=False)
    except g.ExecutionError as exc:
        assert (
            str(exc) == "Rust algorithm execution failed: node_coloring cannot color a graph "
            "containing a self-loop"
        )
    else:
        raise SystemExit("expected node_coloring to reject a self-loop")

    invalid_calls = [
        (
            lambda: forge.analyze(by="node_coloring"),
            "node_coloring requires directed=false",
        ),
        (
            lambda: forge.analyze(
                by="node_coloring",
                directed=False,
                weight="cost",
            ),
            "node_coloring does not accept an edge weight property",
        ),
        (
            lambda: forge.analyze(by="node_coloring", via=" ", directed=False),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="node_coloring", directed=False),
            'invalid analyze label ""',
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_k1_coloring() -> None:
    # The binding selects and decodes the Rust-owned greedy algorithm.
    forge = g.GraphForge()
    people = sorted(
        [
            (name, forge.add_node("Person", name=name))
            for name in ["A", "B", "C", "D", "E", "F", "Isolate"]
        ],
        key=lambda item: uuid.UUID(item[1].uuid).bytes,
    )
    for left in [0, 2, 4]:
        for right in [1, 3, 5]:
            if left // 2 == right // 2:
                continue
            forge.execute(
                f"MATCH (a:Person {{name:'{people[left][0]}'}}), "
                f"(b:Person {{name:'{people[right][0]}'}}) "
                "CREATE (a)-[:ROAD]->(b)"
            )
    forge.execute(
        f"MATCH (a:Person {{name:'{people[0][0]}'}}), "
        f"(b:Person {{name:'{people[3][0]}'}}) "
        "CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(a)"
    )

    options = {"by": "k1_coloring", "via": "ROAD", "directed": False}
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("node_uuid", pa.binary(16), False),
        ("color", pa.uint64(), False),
    ]
    assert table.schema.metadata == {
        b"graphforge.algorithm": b"k1_coloring",
        b"graphforge.algorithm_schema_version": b"1",
        b"graphforge.verb": b"analyze",
    }
    assert table["node_uuid"].null_count == 0
    assert table["color"].null_count == 0
    assert table["node_uuid"].to_pylist() == [uuid.UUID(handle.uuid).bytes for _, handle in people]
    assert table["color"].to_pylist() == [0, 0, 1, 1, 2, 2, 0]
    assert table.equals(forge.analyze("Person", **options))
    assert forge.analyze("Person", by="chromatic_number", via="ROAD", directed=False)[
        "chromatic_number"
    ].to_pylist() == [2]

    loop = g.GraphForge()
    loop.add_node("Person", name="Loop")
    loop.execute("MATCH (n:Person) CREATE (n)-[:ROAD]->(n)")
    try:
        loop.analyze("Person", by="k1_coloring", via="ROAD", directed=False)
    except g.ExecutionError as exc:
        assert (
            str(exc) == "Rust algorithm execution failed: k1_coloring cannot color a graph "
            "containing a self-loop"
        )
    else:
        raise SystemExit("expected k1_coloring to reject a self-loop")

    for call, message in [
        (
            lambda: forge.analyze(by="k1_coloring"),
            "k1_coloring requires directed=false",
        ),
        (
            lambda: forge.analyze(
                by="k1_coloring",
                directed=False,
                weight="cost",
            ),
            "k1_coloring does not accept an edge weight property",
        ),
    ]:
        _expect_validation_message(message, call)


def check_chromatic_number() -> None:
    # The bindings only select and decode the Rust-owned algorithm (#772).
    forge = g.GraphForge()
    for name in ["Alice", "Bob", "Carol", "Dan", "Eve"]:
        forge.add_node("Person", name=name)
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}) "
        "CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), "
        "(c)-[:ROAD]->(a), (a)-[:ROAD]->(b), "
        "(b)-[:ROAD]->(a), (d)-[:OTHER]->(e)"
    )

    options = {"by": "chromatic_number", "via": "ROAD", "directed": False}
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("chromatic_number", pa.uint64(), False),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"chromatic_number"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert table.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
    assert table.num_rows == 1
    assert table["chromatic_number"].null_count == 0
    assert table["chromatic_number"].to_pylist() == [3]
    assert table.equals(forge.analyze("Person", **options))

    scalar_results = [
        (forge.analyze("Person", **(options | {"via": "MISSING"})), 1),
        (forge.analyze("Missing", **options), 0),
        (g.GraphForge().analyze(by="chromatic_number", directed=False), 0),
    ]
    for result, expected in scalar_results:
        assert result.num_rows == 1
        assert list(result.schema) == list(table.schema)
        assert result.schema.metadata == table.schema.metadata
        assert result["chromatic_number"].null_count == 0
        assert result["chromatic_number"].to_pylist() == [expected]

    loop = g.GraphForge()
    loop.add_node("Person", name="Loop")
    loop.execute("MATCH (n:Person) CREATE (n)-[:ROAD]->(n)")
    try:
        loop.analyze("Person", by="chromatic_number", via="ROAD", directed=False)
    except g.ExecutionError as exc:
        assert (
            str(exc) == "Rust algorithm execution failed: chromatic_number is undefined for a "
            "graph containing a self-loop"
        )
    else:
        raise SystemExit("expected chromatic_number to reject a self-loop")

    invalid_calls = [
        (
            lambda: forge.analyze(by="chromatic_number"),
            "chromatic_number requires directed=false",
        ),
        (
            lambda: forge.analyze(
                by="chromatic_number",
                directed=False,
                weight="cost",
            ),
            "chromatic_number does not accept an edge weight property",
        ),
        (
            lambda: forge.analyze(by="chromatic_number", via=" ", directed=False),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="chromatic_number", directed=False),
            'invalid analyze label ""',
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_count_automorphisms() -> None:
    # #2134 — fresh-wheel acceptance for the Rust-owned exact multigraph count.
    forge = g.GraphForge()
    for name in ["A", "B", "C", "D"]:
        forge.add_node(
            "Person",
            name=name,
            payload=f"unique-property-{name}",
            confidence=0.1 * (ord(name) - ord("A") + 1),
        )
    forge.execute(
        "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), "
        "(c:Person {name:'C'}), (d:Person {name:'D'}) "
        "CREATE (a)-[:ROAD]->(a), (b)-[:ROAD]->(b), "
        "(a)-[:ROAD]->(b), (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), "
        "(c)-[:ROAD]->(d), (d)-[:ROAD]->(c)"
    )

    def run(graph: g.GraphForge, *, directed: bool, label="Person", via="ROAD"):
        return graph.analyze(
            label,
            by="count_automorphisms",
            via=via,
            directed=directed,
        )

    directed = run(forge, directed=True)
    undirected = run(forge, directed=False)
    expected_schema = [("count", pa.uint64(), False)]
    expected_metadata = {
        b"graphforge.algorithm": b"count_automorphisms",
        b"graphforge.algorithm_schema_version": b"1",
        b"graphforge.verb": b"analyze",
    }
    forbidden = {
        "node_uuid",
        "provenance",
        "confidence",
        "assertion",
        "evidence",
        "belief",
        "hypothesis",
        "valid_time",
        "algorithm_run_uuid",
        "run_uuid",
    }
    for table, expected in [(directed, 2), (undirected, 4)]:
        assert [(field.name, field.type, field.nullable) for field in table.schema] == (
            expected_schema
        )
        assert table.schema.metadata == expected_metadata
        assert table.num_rows == 1
        assert table["count"].null_count == 0
        assert table["count"].to_pylist() == [expected]
        assert forbidden.isdisjoint(table.column_names)
    assert directed.equals(run(forge, directed=True))
    assert undirected.equals(run(forge, directed=False))

    empty = g.GraphForge()
    singleton = g.GraphForge()
    singleton.add_node("Person", name="only", evidence="ignored")
    for table in [
        run(empty, directed=False, label=None, via=None),
        run(singleton, directed=False, via=None),
    ]:
        assert [(field.name, field.type, field.nullable) for field in table.schema] == (
            expected_schema
        )
        assert table.schema.metadata == expected_metadata
        assert table["count"].to_pylist() == [1]

    _expect_validation_message(
        'invalid analyze relationship selector " "',
        lambda: forge.analyze(
            "Person",
            by="count_automorphisms",
            via=" ",
            directed=True,
        ),
    )


def check_edge_coloring() -> None:
    # The bindings only select and decode the Rust-owned algorithm (#772).
    forge = g.GraphForge()
    for label, name in [
        ("Person", "Alice"),
        ("Person", "Bob"),
        ("Person", "Carol"),
        ("Person", "Dan"),
        ("Person", "Eve"),
        ("Animal", "Fox"),
    ]:
        forge.add_node(label, name=name)
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}), (f:Animal {name:'Fox'}) "
        "CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), "
        "(b)-[:ROAD]->(a), (c)-[:ROAD]->(d), "
        "(d)-[:OTHER]->(e), (f)-[:ROAD]->(a)"
    )

    options = {"by": "edge_coloring", "via": "ROAD", "directed": False}
    table = forge.analyze("Person", **options)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("edge_uuid", pa.binary(16), False),
        ("color", pa.uint64(), False),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"edge_coloring"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert table.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
    assert table["edge_uuid"].null_count == 0
    assert table["color"].null_count == 0
    assert table.column_names == ["edge_uuid", "color"]

    edge_ids = table["edge_uuid"].to_pylist()
    assert edge_ids == sorted(edge_ids)
    assert len(edge_ids) == 4
    assert table.equals(forge.analyze("Person", **options))

    projected = forge.execute(
        "MATCH (s:Person)-[r:ROAD]->(t:Person) "
        "RETURN r.edge_uuid AS edge_uuid, "
        "s.node_uuid AS source_uuid, t.node_uuid AS target_uuid"
    )
    endpoints = {
        row["edge_uuid"]: (row["source_uuid"], row["target_uuid"]) for row in projected.to_pylist()
    }
    assert set(edge_ids) == set(endpoints)
    colors = dict(zip(edge_ids, table["color"].to_pylist()))
    for left_index, left in enumerate(edge_ids):
        for right in edge_ids[left_index + 1 :]:
            if set(endpoints[left]) & set(endpoints[right]):
                assert colors[left] != colors[right]

    missing = forge.analyze("Missing", **options)
    no_relationships = forge.analyze("Person", **(options | {"via": "MISSING"}))
    empty = g.GraphForge().analyze(by="edge_coloring", directed=False)
    for result in [missing, no_relationships, empty]:
        assert result.num_rows == 0
        assert list(result.schema) == list(table.schema)
        assert result.schema.metadata == table.schema.metadata
        assert result["edge_uuid"].null_count == 0
        assert result["color"].null_count == 0

    loop = g.GraphForge()
    loop.add_node("Person", name="Loop")
    loop.execute("MATCH (n:Person) CREATE (n)-[:ROAD]->(n)")
    try:
        loop.analyze("Person", by="edge_coloring", via="ROAD", directed=False)
    except g.ExecutionError as exc:
        assert (
            str(exc) == "Rust algorithm execution failed: edge_coloring cannot color a graph "
            "containing a self-loop"
        )
    else:
        raise SystemExit("expected edge_coloring to reject a self-loop")

    invalid_calls = [
        (
            lambda: forge.analyze(by="edge_coloring"),
            "edge_coloring requires directed=false",
        ),
        (
            lambda: forge.analyze(
                by="edge_coloring",
                directed=False,
                weight="cost",
            ),
            "edge_coloring does not accept an edge weight property",
        ),
        (
            lambda: forge.analyze(by="edge_coloring", via=" ", directed=False),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="edge_coloring", directed=False),
            'invalid analyze label ""',
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_find_cycles() -> None:
    forge = g.GraphForge()
    nodes = {
        name: forge.add_node("Person", name=name)
        for name in ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus"]
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}), (f:Person {name:'Fox'}) "
        "CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), "
        "(b)-[:ROAD]->(c), (c)-[:ROAD]->(a), "
        "(b)-[:ROAD]->(d), (d)-[:ROAD]->(b), (d)-[:ROAD]->(d), "
        "(e)-[:ROAD]->(f), (a)-[:OTHER]->(d), (d)-[:OTHER]->(a)"
    )

    def canonical(names: list[str], directed: bool) -> list[bytes]:
        values = [uuid.UUID(nodes[name].uuid).bytes for name in names]
        rotations = [values[offset:] + values[:offset] for offset in range(len(values))]
        if not directed and len(values) > 1:
            reversed_values = list(reversed(values))
            rotations += [
                reversed_values[offset:] + reversed_values[:offset]
                for offset in range(len(reversed_values))
            ]
        return min(rotations)

    def rows(table: pa.Table) -> list[list[bytes]]:
        cycles = table["cycle"]
        assert cycles.null_count == 0
        result = cycles.to_pylist()
        assert all(
            cycle is not None and all(item is not None for item in cycle) for cycle in result
        )
        assert all(len(cycle) == 1 or cycle[0] != cycle[-1] for cycle in result)
        return result

    directed_options = {"by": "find_cycles", "via": "ROAD"}
    directed = forge.analyze("Person", **directed_options)
    item = pa.field("item", pa.binary(16), nullable=False)
    assert [(field.name, field.type, field.nullable) for field in directed.schema] == [
        ("cycle", pa.list_(item), False)
    ]
    assert directed.schema.metadata[b"graphforge.algorithm"] == b"find_cycles"
    assert directed.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert directed.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
    expected_directed = sorted(
        [
            canonical(["Alice", "Bob", "Carol"], True),
            canonical(["Bob", "Dan"], True),
            canonical(["Dan"], True),
        ]
    )
    assert rows(directed) == expected_directed
    assert rows(forge.analyze("Person", **directed_options)) == expected_directed
    assert uuid.UUID(nodes["Gus"].uuid).bytes not in {
        item for cycle in expected_directed for item in cycle
    }

    undirected = forge.analyze(
        "Person",
        by="find_cycles",
        via="ROAD",
        directed=False,
    )
    assert rows(undirected) == sorted(
        [
            canonical(["Alice", "Bob", "Carol"], False),
            canonical(["Dan"], False),
        ]
    )

    missing = forge.analyze("Missing", **directed_options)
    empty = g.GraphForge().analyze(by="find_cycles")
    for result in [missing, empty]:
        assert result.num_rows == 0
        assert list(result.schema) == list(directed.schema)
        assert result.schema.metadata == directed.schema.metadata
        assert rows(result) == []

    invalid_calls = [
        (
            lambda: forge.analyze(by="find_cycles", weight="cost"),
            "find_cycles does not accept an edge weight property",
        ),
        (
            lambda: forge.analyze(by="find_cycles", via=" "),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="find_cycles"),
            'invalid analyze label ""',
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_dag_longest_path() -> None:
    # The bindings only select and decode the Rust-owned algorithm (#772).
    forge = g.GraphForge()
    nodes = {
        name: forge.add_node("Person", name=name)
        for name in ["Alice", "Bob", "Carol", "Dan", "Eve"]
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}) "
        "CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), "
        "(b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), (a)-[:OTHER]->(e)"
    )

    def run() -> pa.Table:
        return forge.analyze(
            "Person",
            by="dag_longest_path",
            via="KNOWS",
            directed=True,
        )

    table = run()
    item = pa.field("item", pa.binary(16), nullable=False)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("cost", pa.float64(), False),
        ("path", pa.list_(item), False),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"dag_longest_path"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert table.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
    assert table["cost"].null_count == 0
    assert table["path"].null_count == 0
    assert table["cost"].to_pylist() == [2.0]
    expected_middle = min(nodes["Bob"].uuid, nodes["Carol"].uuid)
    expected_path = [
        uuid.UUID(nodes["Alice"].uuid).bytes,
        uuid.UUID(expected_middle).bytes,
        uuid.UUID(nodes["Dan"].uuid).bytes,
    ]
    assert table["path"].to_pylist() == [expected_path]
    assert run().equals(table, check_metadata=True)

    for result in [
        forge.analyze("Missing", by="dag_longest_path", directed=True),
        g.GraphForge().analyze(by="dag_longest_path", directed=True),
    ]:
        assert result.schema.equals(table.schema, check_metadata=True)
        assert result.num_rows == 1
        assert result["cost"].null_count == 0
        assert result["path"].null_count == 0
        assert result["cost"].to_pylist() == [0.0]
        assert result["path"].to_pylist() == [[]]

    cyclic = g.GraphForge()
    cyclic.execute("CREATE (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(a)")
    try:
        cyclic.analyze(by="dag_longest_path", directed=True)
    except g.ExecutionError as error:
        assert (
            str(error) == "Rust algorithm execution failed: "
            "dag_longest_path requires a directed acyclic graph"
        )
    else:
        raise SystemExit("expected structured dag_longest_path cycle error")

    for call, message in [
        (
            lambda: forge.analyze(by="dag_longest_path", directed=False),
            "dag_longest_path requires directed=true",
        ),
        (
            lambda: forge.analyze(by="dag_longest_path", directed=True, weight="cost"),
            "dag_longest_path does not accept an edge weight property",
        ),
        (
            lambda: forge.analyze(by="dag_longest_path", directed=True, via=" "),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="dag_longest_path", directed=True),
            'invalid analyze label ""',
        ),
    ]:
        _expect_validation_message(message, call)


def check_weighted_dag_longest_path() -> None:
    # The bindings only select and decode the Rust-owned algorithm (#772).
    forge = g.GraphForge()
    nodes = {
        name: forge.add_node("Person", name=name)
        for name in ["Alice", "Bob", "Carol", "Dan", "Eve"]
    }
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), "
        "(e:Person {name:'Eve'}) "
        "CREATE (a)-[:ROAD {cost:2.0}]->(b), "
        "(b)-[:ROAD {cost:3.0}]->(d), "
        "(a)-[:ROAD {cost:2.0}]->(c), "
        "(c)-[:ROAD {cost:3.0}]->(d), "
        "(e)-[:ROAD {cost:-8.0}]->(d), "
        "(a)-[:OTHER {cost:100.0}]->(d)"
    )

    def run() -> pa.Table:
        return forge.analyze(
            "Person",
            by="dag_longest_path_weighted",
            via="ROAD",
            directed=True,
            weight="cost",
        )

    table = run()
    item = pa.field("item", pa.binary(16), nullable=False)
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("cost", pa.float64(), False),
        ("path", pa.list_(item), False),
    ]
    assert table.schema.metadata[b"graphforge.algorithm"] == b"dag_longest_path_weighted"
    assert table.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert table.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"
    assert table["cost"].null_count == 0
    assert table["path"].null_count == 0
    assert table["cost"].to_pylist() == [5.0]
    expected_middle = min(nodes["Bob"].uuid, nodes["Carol"].uuid)
    assert table["path"].to_pylist() == [
        [
            uuid.UUID(nodes["Alice"].uuid).bytes,
            uuid.UUID(expected_middle).bytes,
            uuid.UUID(nodes["Dan"].uuid).bytes,
        ]
    ]
    assert run().equals(table, check_metadata=True)

    for result in [
        forge.analyze(
            "Missing",
            by="dag_longest_path_weighted",
            directed=True,
            weight="cost",
        ),
        g.GraphForge().analyze(
            by="dag_longest_path_weighted",
            directed=True,
            weight="cost",
        ),
    ]:
        assert result.schema.equals(table.schema, check_metadata=True)
        assert result.num_rows == 1
        assert result["cost"].null_count == 0
        assert result["path"].null_count == 0
        assert result["cost"].to_pylist() == [0.0]
        assert result["path"].to_pylist() == [[]]

    cyclic = g.GraphForge()
    cyclic.execute("CREATE (a:Person)-[:ROAD {cost:1.0}]->(b:Person)-[:ROAD {cost:1.0}]->(a)")
    try:
        cyclic.analyze(
            by="dag_longest_path_weighted",
            directed=True,
            weight="cost",
        )
    except g.ExecutionError as error:
        assert (
            str(error) == "Rust algorithm execution failed: "
            "dag_longest_path_weighted requires a directed acyclic graph"
        )
    else:
        raise SystemExit("expected structured weighted DAG cycle error")

    invalid_weight = g.GraphForge()
    invalid_weight.execute("CREATE (:Person)-[:ROAD {cost:'heavy'}]->(:Person)")
    for call, message in [
        (
            lambda: forge.analyze(
                by="dag_longest_path_weighted",
                directed=False,
                weight="cost",
            ),
            "dag_longest_path_weighted requires directed=true",
        ),
        (
            lambda: forge.analyze(
                by="dag_longest_path_weighted",
                directed=True,
            ),
            "dag_longest_path_weighted requires an edge weight property",
        ),
        (
            lambda: forge.analyze(
                by="dag_longest_path_weighted",
                directed=True,
                weight=" ",
            ),
            'invalid analyze weight property " "',
        ),
        (
            lambda: forge.analyze(
                by="dag_longest_path_weighted",
                via=" ",
                directed=True,
                weight="cost",
            ),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze(
                "",
                by="dag_longest_path_weighted",
                directed=True,
                weight="cost",
            ),
            'invalid analyze label ""',
        ),
        (
            lambda: forge.analyze(
                by="dag_longest_path_weighted",
                directed=True,
                weight="missing",
            ),
            'edge weight property "missing" does not exist',
        ),
        (
            lambda: invalid_weight.analyze(
                by="dag_longest_path_weighted",
                directed=True,
                weight="cost",
            ),
            'edge weight property "cost" must be numeric',
        ),
    ]:
        _expect_validation_message(message, call)


def check_has_euler_circuit() -> None:
    # The binding supplies selectors and decodes Arrow; Rust owns the predicate (#772).
    forge = g.GraphForge()
    for name in ["Alice", "Bob", "Carol", "Isolate"]:
        forge.add_node("Person", name=name)
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}) "
        "CREATE (a)-[:CYCLE]->(b), (b)-[:CYCLE]->(c), (c)-[:CYCLE]->(a), "
        "(a)-[:PATH]->(b), (b)-[:PATH]->(c), "
        "(a)-[:LOOP]->(a), "
        "(a)-[:PARALLEL]->(b), (a)-[:PARALLEL]->(b), "
        "(a)-[:RECIPROCAL]->(b), (b)-[:RECIPROCAL]->(a)"
    )

    def result(via: str | None = None, *, directed: bool) -> pa.Table:
        return forge.analyze(
            "Person",
            by="has_euler_circuit",
            via=via,
            directed=directed,
        )

    reference = result("CYCLE", directed=False)
    assert [(field.name, field.type, field.nullable) for field in reference.schema] == [
        ("has_euler_circuit", pa.bool_(), False),
    ]
    assert reference.schema.metadata[b"graphforge.algorithm"] == b"has_euler_circuit"
    assert reference.schema.metadata[b"graphforge.verb"] == b"analyze"
    assert reference.schema.metadata[b"graphforge.algorithm_schema_version"] == b"1"

    cases = [
        (result("CYCLE", directed=False), True),
        (result("CYCLE", directed=True), True),
        (result("PATH", directed=False), False),
        (result("PATH", directed=True), False),
        (result("LOOP", directed=False), True),
        (result("LOOP", directed=True), True),
        (result("PARALLEL", directed=False), True),
        (result("PARALLEL", directed=True), False),
        (result("RECIPROCAL", directed=False), True),
        (result("RECIPROCAL", directed=True), True),
        (result("MISSING", directed=False), True),
        (
            g.GraphForge().analyze(
                by="has_euler_circuit",
                directed=True,
            ),
            True,
        ),
    ]
    for table, expected in cases:
        assert table.num_rows == 1
        assert table["has_euler_circuit"].null_count == 0
        assert table["has_euler_circuit"].to_pylist() == [expected]
        assert list(table.schema) == list(reference.schema)
        assert table.schema.metadata == reference.schema.metadata

    invalid_calls = [
        (
            lambda: forge.analyze(
                by="has_euler_circuit",
                via=" ",
                directed=False,
            ),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="has_euler_circuit", directed=False),
            'invalid analyze label ""',
        ),
        (
            lambda: forge.analyze(
                by="has_euler_circuit",
                directed=False,
                weight="cost",
            ),
            "has_euler_circuit does not accept an edge weight property",
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def check_has_euler_path() -> None:
    # The binding supplies selectors and decodes Arrow; Rust owns the predicate (#772).
    forge = g.GraphForge()
    for name in ["Alice", "Bob", "Carol", "Dan", "Isolate"]:
        forge.add_node("Person", name=name)
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}) "
        "CREATE (a)-[:PATH]->(b), (b)-[:PATH]->(c), "
        "(a)-[:STAR]->(b), (a)-[:STAR]->(c), (a)-[:STAR]->(d), "
        "(a)-[:LOOP]->(a), "
        "(a)-[:PARALLEL]->(b), (a)-[:PARALLEL]->(b), "
        "(a)-[:RECIPROCAL]->(b), (b)-[:RECIPROCAL]->(a), "
        "(a)-[:DISCONNECTED]->(b), (c)-[:DISCONNECTED]->(d)"
    )

    def result(via: str | None = None, *, directed: bool) -> pa.Table:
        return forge.analyze(
            "Person",
            by="has_euler_path",
            via=via,
            directed=directed,
        )

    reference = result("PATH", directed=False)
    assert [(field.name, field.type, field.nullable) for field in reference.schema] == [
        ("has_euler_path", pa.bool_(), False),
    ]
    assert reference.schema.metadata == {
        b"graphforge.algorithm": b"has_euler_path",
        b"graphforge.algorithm_schema_version": b"1",
        b"graphforge.verb": b"analyze",
    }

    cases = [
        (reference, True),
        (result("PATH", directed=True), True),
        (result("STAR", directed=False), False),
        (result("STAR", directed=True), False),
        (result("DISCONNECTED", directed=False), False),
        (result("DISCONNECTED", directed=True), False),
        (result("LOOP", directed=False), True),
        (result("LOOP", directed=True), True),
        (result("PARALLEL", directed=False), True),
        (result("PARALLEL", directed=True), False),
        (result("RECIPROCAL", directed=False), True),
        (result("RECIPROCAL", directed=True), True),
        (result("MISSING", directed=False), True),
        (
            g.GraphForge().analyze(
                by="has_euler_path",
                directed=True,
            ),
            True,
        ),
    ]
    for table, expected in cases:
        assert table.num_rows == 1
        assert table["has_euler_path"].null_count == 0
        assert table["has_euler_path"].to_pylist() == [expected]
        assert list(table.schema) == list(reference.schema)
        assert table.schema.metadata == reference.schema.metadata

    invalid_calls = [
        (
            lambda: forge.analyze(
                by="has_euler_path",
                via=" ",
                directed=False,
            ),
            'invalid analyze relationship selector " "',
        ),
        (
            lambda: forge.analyze("", by="has_euler_path", directed=False),
            'invalid analyze label ""',
        ),
        (
            lambda: forge.analyze(
                by="has_euler_path",
                directed=False,
                weight="cost",
            ),
            "has_euler_path does not accept an edge weight property",
        ),
    ]
    for call, message in invalid_calls:
        _expect_validation_message(message, call)


def _assert_euler_result(
    forge: g.GraphForge,
    *,
    by: str,
    via: str | None,
    directed: bool,
) -> tuple[list[bytes], list[bytes]]:
    """Assert the canonical native Euler result and its graph-edge coherence."""
    table = forge.analyze("Person", by=by, via=via, directed=directed)
    assert table.equals(forge.analyze("Person", by=by, via=via, directed=directed))
    assert table.num_rows == 1
    assert [(field.name, field.type, field.nullable) for field in table.schema] == [
        ("node_path", pa.list_(pa.field("item", pa.binary(16), nullable=False)), False),
        ("edge_path", pa.list_(pa.field("item", pa.binary(16), nullable=False)), False),
    ]
    assert table.schema.metadata == {
        b"graphforge.algorithm": by.encode(),
        b"graphforge.algorithm_schema_version": b"1",
        b"graphforge.verb": b"analyze",
    }
    node_path = table["node_path"][0].as_py()
    edge_path = table["edge_path"][0].as_py()
    assert len(node_path) == len(edge_path) + 1
    assert all(isinstance(value, bytes) and len(value) == 16 for value in node_path)
    assert all(isinstance(value, bytes) and len(value) == 16 for value in edge_path)
    assert len(edge_path) == len(set(edge_path))

    relationships = forge.execute(
        f"MATCH (s)-[e:{via}]->(t) "
        "RETURN e.edge_uuid AS edge_uuid, s.node_uuid AS source_uuid, "
        "t.node_uuid AS target_uuid"
    ).to_pylist()
    remaining = {
        row["edge_uuid"]: (row["source_uuid"], row["target_uuid"]) for row in relationships
    }
    assert set(edge_path) == set(remaining)
    for edge, source, target in zip(edge_path, node_path, node_path[1:], strict=False):
        edge_source, edge_target = remaining.pop(edge)
        assert (source, target) == (edge_source, edge_target) or (
            not directed and (source, target) == (edge_target, edge_source)
        )
    assert not remaining
    return node_path, edge_path


def check_euler_circuit() -> None:
    # The adapter only selects the value and decodes Arrow; Rust owns construction.
    forge = g.GraphForge()
    alice = forge.add_node("Person", name="Alice")
    bob = forge.add_node("Person", name="Bob")
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}) "
        "CREATE (a)-[:UNDIRECTED]->(b), (a)-[:UNDIRECTED]->(b), "
        "(a)-[:UNDIRECTED]->(a), (a)-[:DIRECTED]->(b), "
        "(b)-[:DIRECTED]->(a), (a)-[:DIRECTED]->(a)"
    )
    identities = {uuid.UUID(alice.uuid).bytes, uuid.UUID(bob.uuid).bytes}
    for via, directed in [("UNDIRECTED", False), ("DIRECTED", True)]:
        node_path, edge_path = _assert_euler_result(
            forge, by="euler_circuit", via=via, directed=directed
        )
        assert set(node_path) == identities
        assert node_path[0] == node_path[-1] == min(identities)
        assert len(edge_path) == 3  # loop and parallel/reciprocal edge UUIDs survive.

    empty = g.GraphForge().analyze(by="euler_circuit", directed=False)
    assert empty.num_rows == 0
    singleton = g.GraphForge()
    isolated = singleton.add_node("Person", name="Isolated")
    singleton_result = singleton.analyze("Person", by="euler_circuit", directed=False)
    assert singleton_result["node_path"][0].as_py() == [uuid.UUID(isolated.uuid).bytes]
    assert singleton_result["edge_path"][0].as_py() == []

    undefined = g.GraphForge()
    undefined.execute("CREATE (:Person)-[:TRAIL]->(:Person)")
    try:
        undefined.analyze("Person", by="euler_circuit", via="TRAIL", directed=False)
    except g.ExecutionError as error:
        assert str(error) == "Euler circuit is undefined for the selected graph"
    else:
        raise SystemExit("expected undefined Euler circuit ExecutionError")


def check_euler_path() -> None:
    forge = g.GraphForge()
    handles = [forge.add_node("Person", name=name) for name in ["Alice", "Bob", "Carol"]]
    forge.execute(
        "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
        "(c:Person {name:'Carol'}) "
        "CREATE (a)-[:TRAIL]->(b), (b)-[:TRAIL]->(b), (b)-[:TRAIL]->(c)"
    )
    identities = [uuid.UUID(handle.uuid).bytes for handle in handles]
    directed_nodes, directed_edges = _assert_euler_result(
        forge, by="euler_path", via="TRAIL", directed=True
    )
    assert directed_nodes == [identities[0], identities[1], identities[1], identities[2]]
    assert len(directed_edges) == 3

    undirected_nodes, undirected_edges = _assert_euler_result(
        forge, by="euler_path", via="TRAIL", directed=False
    )
    assert undirected_nodes[0] == min(identities[0], identities[2])
    assert undirected_nodes[-1] == max(identities[0], identities[2])
    assert len(undirected_edges) == 3

    empty = g.GraphForge().analyze(by="euler_path", directed=False)
    assert empty.num_rows == 0
    singleton = g.GraphForge()
    isolated = singleton.add_node("Person", name="Isolated")
    singleton_result = singleton.analyze("Person", by="euler_path", directed=False)
    assert singleton_result["node_path"][0].as_py() == [uuid.UUID(isolated.uuid).bytes]
    assert singleton_result["edge_path"][0].as_py() == []

    undefined = g.GraphForge()
    undefined.execute(
        "CREATE (a:Person), (b:Person), (c:Person), (d:Person) "
        "CREATE (a)-[:TRAIL]->(b), (a)-[:TRAIL]->(c), (a)-[:TRAIL]->(d)"
    )
    try:
        undefined.analyze("Person", by="euler_path", via="TRAIL", directed=False)
    except g.ExecutionError as error:
        assert str(error) == "Euler path is undefined for the selected graph"
    else:
        raise SystemExit("expected undefined Euler path ExecutionError")


def check_lifecycle() -> None:
    # #586 — operations after close() raise LifecycleError.
    forge = g.GraphForge()
    forge.close()
    forge.close()  # idempotent
    try:
        forge.execute("MATCH (n) RETURN n.node_uuid AS id")
    except g.LifecycleError as exc:
        assert isinstance(exc, g.GraphForgeError)
    else:
        raise SystemExit("expected LifecycleError after close()")


def check_load_ontology() -> None:
    # #589 — load_ontology() accepts a path; the declared label is queryable.
    yaml = (
        "ontology_id: people\n"
        'version: "2026.06"\n'
        "entity_types:\n"
        "  - name: Person\n"
        "properties:\n"
        "  - name: name\n"
        "    owner: Person\n"
        "    type: utf8\n"
    )
    forge = g.GraphForge()
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "ontology.yaml"
        path.write_text(yaml)
        forge.load_ontology(str(path))  # no error on a valid ontology
    # The declared label is queryable (empty graph → zero rows, valid schema).
    table = forge.execute("MATCH (p:Person) RETURN p.node_uuid AS id")
    assert isinstance(table, pa.Table) and table.num_rows == 0, table
    assert table.column_names == ["id"], table.column_names


def check_execute_polars() -> None:
    # #590 — execute_polars() returns a polars.DataFrame, or raises ImportError
    # with guidance when the optional 'polars' dependency is absent.
    forge = g.GraphForge()
    forge.execute("CREATE (:Person {name: 'A'})")
    try:
        import polars  # noqa: F401
    except ImportError:
        try:
            forge.execute_polars("MATCH (p:Person) RETURN p.name AS name")
        except ImportError as exc:
            # Must be our guidance message, not an unrelated import regression.
            assert "polars" in str(exc), exc
            return
        raise SystemExit("expected ImportError from execute_polars() without polars") from None
    df = forge.execute_polars("MATCH (p:Person) RETURN p.name AS name")
    assert df["name"][0] == "A", df


def check_execute_stream() -> None:
    # #587 — execute_stream returns a lazy, genuine pyarrow.RecordBatchReader.
    import gc

    forge = g.GraphForge()
    forge.execute("CREATE (:Person {name: 'Alice'})")
    forge.execute("CREATE (:Person {name: 'Bob'})")

    reader = forge.execute_stream("MATCH (p:Person) RETURN p.node_uuid AS id")
    assert isinstance(reader, pa.RecordBatchReader), type(reader)
    # Schema is available before iterating; UUID identity is FixedSizeBinary(16).
    assert reader.schema.field("id").type == pa.binary(16), reader.schema
    assert b"graphforge.query_id" in (reader.schema.metadata or {})

    batches = list(reader)  # lazy iteration yields RecordBatches
    assert batches and all(isinstance(b, pa.RecordBatch) for b in batches)
    assert sum(b.num_rows for b in batches) == 2

    # Writes are rejected on the streaming path.
    try:
        forge.execute_stream("CREATE (:Person {name: 'X'})")
    except g.ValidationError as exc:
        assert isinstance(exc, g.GraphForgeError)
    else:
        raise SystemExit("expected ValidationError for a streamed write")

    # The reader keeps the runtime alive after the parent GraphForge is dropped.
    forge2 = g.GraphForge()
    forge2.execute("CREATE (:Person {name: 'Carol'})")
    reader2 = forge2.execute_stream("MATCH (p:Person) RETURN p.node_uuid AS id")
    held_schema = reader2.schema
    del forge2
    gc.collect()
    assert sum(b.num_rows for b in reader2) == 1  # must not hang or error
    assert held_schema.field("id").type == pa.binary(16)


def check_assertions() -> None:
    forge = g.GraphForge()
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000301",
        capability_id="provenance",
        capability_version=1,
    )
    node = forge.add_node("Person", name="Ada")
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000302",
        capability_id="knowledge",
        capability_version=1,
    )
    request = {
        "operation_uuid": "018f0f4e-7b8c-7000-8000-000000000303",
        "assertion_uuid": "018f0f4e-7b8c-7000-8000-000000000304",
        "claim": "e\u0301 is not normalized to é",
        "graph_refs": [
            {
                "graph_uuid": node.uuid,
                "graph_kind": "node",
                "role": "subject",
                "ordinal": 0,
            }
        ],
    }
    created = forge.create_assertion(**request)
    replayed = forge.create_assertion(**request)
    assert created.equals(replayed)
    assert created.column_names == [
        "assertion_uuid",
        "claim",
        "provenance_uuid",
        "recorded_at",
        "contract_version",
    ]
    assert created.column("claim").to_pylist() == ["e\u0301 is not normalized to é"]
    assert forge.assertion(request["assertion_uuid"]).equals(created)
    assert forge.list_assertions(graph_uuid=node.uuid).equals(created)
    refs = forge.assertion_graph_refs(request["assertion_uuid"])
    assert refs.column("graph_uuid").to_pylist() == [bytes.fromhex(node.uuid.replace("-", ""))]
    assert refs.column("role").to_pylist() == ["subject"]


def check_reasoning() -> None:
    forge = g.GraphForge()
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000341",
        capability_id="provenance",
        capability_version=1,
    )
    node = forge.add_node("Person", name="Ada")
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000342",
        capability_id="knowledge",
        capability_version=1,
    )
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000343",
        capability_id="epistemic",
        capability_version=1,
    )
    assertion_uuid = "018f0f4e-7b8c-7000-8000-000000000344"
    assertion = forge.create_assertion(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000345",
        assertion_uuid=assertion_uuid,
        claim="reasoning target",
        graph_refs=[
            {
                "graph_uuid": node.uuid,
                "graph_kind": "node",
                "role": "subject",
                "ordinal": 0,
            }
        ],
    )
    provenance_uuid = str(uuid.UUID(bytes=assertion.column("provenance_uuid").to_pylist()[0]))
    request = {
        "operation_uuid": "018f0f4e-7b8c-7000-8000-000000000346",
        "reasoning_uuid": "018f0f4e-7b8c-7000-8000-000000000347",
        "assertion_uuid": assertion_uuid,
        "kind": "logical_inference",
        "content_format": "text/plain",
        "content": b"exact reasoning",
        "provenance_uuid": provenance_uuid,
    }
    created = forge.record_reasoning(**request)
    assert forge.record_reasoning(**request).equals(created)
    assert created.column_names == [
        "reasoning_uuid",
        "assertion_uuid",
        "kind",
        "content_format",
        "content",
        "supersedes_reasoning_uuid",
        "provenance_uuid",
        "recorded_at",
        "contract_version",
    ]
    assert created.column("content").to_pylist() == [b"exact reasoning"]
    assert forge.reasoning(request["reasoning_uuid"]).equals(created)
    assert forge.list_reasoning(assertion_uuid=assertion_uuid).equals(created)


def check_assertion_status() -> None:
    forge = g.GraphForge()
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000351",
        capability_id="provenance",
        capability_version=1,
    )
    node = forge.add_node("Person", name="Ada")
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000352",
        capability_id="knowledge",
        capability_version=1,
    )
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000353",
        capability_id="epistemic",
        capability_version=1,
    )
    assertion_uuid = "018f0f4e-7b8c-7000-8000-000000000354"
    status = forge.create_assertion_with_status(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000355",
        assertion_uuid=assertion_uuid,
        claim="explicit status target",
        graph_refs=[
            {
                "graph_uuid": node.uuid,
                "graph_kind": "node",
                "role": "subject",
                "ordinal": 0,
            }
        ],
        status_event_uuid="018f0f4e-7b8c-7000-8000-000000000356",
        status="hypothesis",
    )
    assert status.column_names == [
        "status_event_uuid",
        "assertion_uuid",
        "status",
        "confidence_uuid",
        "reasoning_uuid",
        "provenance_uuid",
        "recorded_at",
        "contract_version",
    ]
    assert status.column("status").to_pylist() == ["hypothesis"]
    assert forge.assertion_status(assertion_uuid).equals(status)
    assert forge.list_assertion_status(assertion_uuid=assertion_uuid).equals(status)
    provenance_uuid = str(uuid.UUID(bytes=status.column("provenance_uuid").to_pylist()[0]))
    updated = forge.record_assertion_status(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000357",
        status_event_uuid="018f0f4e-7b8c-7000-8000-000000000358",
        assertion_uuid=assertion_uuid,
        status="supported",
        provenance_uuid=provenance_uuid,
    )
    assert updated.column("status").to_pylist() == ["supported"]
    assert forge.list_assertion_status(assertion_uuid=assertion_uuid).num_rows == 2


def check_assertion_supersessions() -> None:
    forge = g.GraphForge()
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000361",
        capability_id="provenance",
        capability_version=1,
    )
    node = forge.add_node("Person", name="Ada")
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000362",
        capability_id="knowledge",
        capability_version=1,
    )
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000363",
        capability_id="epistemic",
        capability_version=1,
    )
    prior = "018f0f4e-7b8c-7000-8000-000000000364"
    replacement = "018f0f4e-7b8c-7000-8000-000000000365"
    prior_row = forge.create_assertion(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000366",
        assertion_uuid=prior,
        claim="prior claim",
        graph_refs=[
            {"graph_uuid": node.uuid, "graph_kind": "node", "role": "subject", "ordinal": 0}
        ],
    )
    forge.create_assertion(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000367",
        assertion_uuid=replacement,
        claim="replacement claim",
        graph_refs=[
            {"graph_uuid": node.uuid, "graph_kind": "node", "role": "subject", "ordinal": 0}
        ],
    )
    provenance = str(uuid.UUID(bytes=prior_row.column("provenance_uuid").to_pylist()[0]))
    reasoning = "018f0f4e-7b8c-7000-8000-000000000368"
    forge.record_reasoning(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000369",
        reasoning_uuid=reasoning,
        assertion_uuid=prior,
        kind="decision_rationale",
        content_format="text/plain",
        content=b"replacement rationale",
        provenance_uuid=provenance,
    )
    request = {
        "operation_uuid": "018f0f4e-7b8c-7000-8000-000000000370",
        "supersession_uuid": "018f0f4e-7b8c-7000-8000-000000000371",
        "prior_assertion_uuid": prior,
        "replacement_assertion_uuid": replacement,
        "status_event_uuid": "018f0f4e-7b8c-7000-8000-000000000372",
        "reasoning_uuid": reasoning,
        "provenance_uuid": provenance,
    }
    created = forge.supersede_assertion(**request)
    assert forge.supersede_assertion(**request).equals(created)
    assert created.column_names == [
        "supersession_uuid",
        "prior_assertion_uuid",
        "replacement_assertion_uuid",
        "status_event_uuid",
        "reasoning_uuid",
        "provenance_uuid",
        "recorded_at",
        "contract_version",
    ]
    assert forge.list_assertion_supersessions(prior_assertion_uuid=prior).equals(created)
    status = forge.assertion_status(prior)
    assert status.column("status").to_pylist() == ["superseded"]
    assert (
        str(uuid.UUID(bytes=status.column("status_event_uuid").to_pylist()[0]))
        == request["status_event_uuid"]
    )


def check_hypothesis_selection() -> None:
    forge = g.GraphForge()
    for operation, capability in [
        ("018f0f4e-7b8c-7000-8000-000000000381", "provenance"),
        ("018f0f4e-7b8c-7000-8000-000000000382", "knowledge"),
        ("018f0f4e-7b8c-7000-8000-000000000383", "epistemic"),
    ]:
        forge.enable_capability(
            operation_uuid=operation, capability_id=capability, capability_version=1
        )
    node = forge.add_node("Person", name="Ada")
    assertion_uuid = "018f0f4e-7b8c-7000-8000-000000000384"
    assertion = forge.create_assertion(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000385",
        assertion_uuid=assertion_uuid,
        claim="explicit hypothesis",
        graph_refs=[
            {"graph_uuid": node.uuid, "graph_kind": "node", "role": "subject", "ordinal": 0}
        ],
    )
    provenance = str(uuid.UUID(bytes=assertion.column("provenance_uuid").to_pylist()[0]))
    reasoning = "018f0f4e-7b8c-7000-8000-000000000386"
    forge.record_reasoning(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000387",
        reasoning_uuid=reasoning,
        assertion_uuid=assertion_uuid,
        kind="decision_rationale",
        content_format="text/plain",
        content=b"explicit selection",
        provenance_uuid=provenance,
    )
    group = "018f0f4e-7b8c-7000-8000-000000000388"
    forge.create_hypothesis_group(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000389",
        group_uuid=group,
        question_key="binding.selection.v1",
        provenance_uuid=provenance,
    )
    forge.record_hypothesis_membership(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000390",
        membership_event_uuid="018f0f4e-7b8c-7000-8000-000000000391",
        group_uuid=group,
        assertion_uuid=assertion_uuid,
        action="added",
        reasoning_uuid=reasoning,
        provenance_uuid=provenance,
    )
    assert forge.hypothesis_selection(group).num_rows == 0
    selected = forge.record_hypothesis_selection(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000392",
        selection_event_uuid="018f0f4e-7b8c-7000-8000-000000000393",
        group_uuid=group,
        selected_assertion_uuid=assertion_uuid,
        reasoning_uuid=reasoning,
        provenance_uuid=provenance,
    )
    assert forge.hypothesis_selection(group).equals(selected)
    assert forge.hypothesis_members(group).num_rows == 1
    assert forge.list_hypothesis_groups(question_key="binding.selection.v1").num_rows == 1
    assert forge.list_hypothesis_membership(group_uuid=group).num_rows == 1
    assert forge.list_hypothesis_selection(group_uuid=group).equals(selected)
    snapshot = forge.epistemic_snapshot(transaction_cutoff=2**63 - 1)
    assert snapshot.num_rows == 2
    assert snapshot.column("entity_kind").to_pylist() == [
        "assertion",
        "hypothesis_group",
    ]
    assert snapshot.schema.metadata[b"graphforge.snapshot_policy"] == (
        b"graphforge-epistemic-snapshot/1"
    )
    try:
        forge.record_hypothesis_membership(
            operation_uuid="018f0f4e-7b8c-7000-8000-000000000394",
            membership_event_uuid="018f0f4e-7b8c-7000-8000-000000000395",
            group_uuid=group,
            assertion_uuid=assertion_uuid,
            action="invalid",
            reasoning_uuid=reasoning,
            provenance_uuid=provenance,
        )
    except g.ValidationError:
        pass
    else:
        raise SystemExit("invalid hypothesis membership action must raise ValidationError")


def check_assertion_validity() -> None:
    forge = g.GraphForge()
    for operation, capability in [
        ("018f0f4e-7b8c-7000-8000-000000000401", "provenance"),
        ("018f0f4e-7b8c-7000-8000-000000000402", "knowledge"),
        ("018f0f4e-7b8c-7000-8000-000000000408", "epistemic"),
    ]:
        forge.enable_capability(
            operation_uuid=operation, capability_id=capability, capability_version=1
        )
    node = forge.add_node("ValiditySubject", name="Ada")
    assertion_uuid = "018f0f4e-7b8c-7000-8000-000000000404"
    assertion = forge.create_assertion(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000405",
        assertion_uuid=assertion_uuid,
        claim="bounded validity",
        graph_refs=[
            {
                "graph_uuid": node.uuid,
                "graph_kind": "node",
                "role": "subject",
                "ordinal": 0,
            }
        ],
    )
    provenance_uuid = str(uuid.UUID(bytes=assertion.column("provenance_uuid").to_pylist()[0]))
    request = {
        "operation_uuid": "018f0f4e-7b8c-7000-8000-000000000406",
        "validity_event_uuid": "018f0f4e-7b8c-7000-8000-000000000407",
        "assertion_uuid": assertion_uuid,
        "provenance_uuid": provenance_uuid,
        "valid_from": 10,
        "valid_to": 20,
    }
    try:
        forge.record_assertion_validity(**request)
    except g.StorageError as error:
        assert error.code == "GF_CAPABILITY_DISABLED"
    else:
        raise SystemExit("validity writes must require valid_time capability")
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000403",
        capability_id="valid_time",
        capability_version=1,
    )
    event = forge.record_assertion_validity(**request)
    assert forge.record_assertion_validity(**request).equals(event)
    assert forge.list_assertion_validity(assertion_uuid=assertion_uuid).equals(event)
    valid = forge.apply_valid_time(transaction_cutoff=2**63 - 1, valid_time=10)
    assert valid.column("interpretation").to_pylist() == ["interpreted"]
    assert valid.column("is_valid").to_pylist() == [True]
    invalid = forge.apply_valid_time(transaction_cutoff=2**63 - 1, valid_time=20)
    assert invalid.column("is_valid").to_pylist() == [False]


def check_resolved_belief_projection() -> None:
    forge = g.GraphForge()
    for operation, capability in [
        ("018f0f4e-7b8c-7000-8000-000000000421", "provenance"),
        ("018f0f4e-7b8c-7000-8000-000000000422", "knowledge"),
        ("018f0f4e-7b8c-7000-8000-000000000423", "epistemic"),
    ]:
        forge.enable_capability(
            operation_uuid=operation, capability_id=capability, capability_version=1
        )
    node = forge.add_node("Person", name="Ada")
    assertion_uuid = "018f0f4e-7b8c-7000-8000-000000000424"
    assertion = forge.create_assertion(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000425",
        assertion_uuid=assertion_uuid,
        claim="Ada is eligible",
        graph_refs=[
            {
                "graph_uuid": node.uuid,
                "graph_kind": "node",
                "role": "subject",
                "ordinal": 0,
            }
        ],
    )
    provenance_uuid = str(uuid.UUID(bytes=assertion.column("provenance_uuid").to_pylist()[0]))
    forge.record_assertion_status(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000426",
        status_event_uuid="018f0f4e-7b8c-7000-8000-000000000427",
        assertion_uuid=assertion_uuid,
        status="supported",
        provenance_uuid=provenance_uuid,
    )
    projection = forge.resolve_belief_projection(
        transaction_cutoff=2**63 - 1,
        included_statuses=["supported"],
        statusless="reject",
        supersession_branches="reject",
        hypotheses="require_selected",
    )
    assert projection.policy_bytes
    assert len(projection.snapshot_fingerprint) == 64
    assert projection.valid_time_fingerprint is None
    assert projection.source_record_uuids
    descriptor = projection.prepare_rank_invocation("Person", by="degree")
    request = {
        "projection": projection,
        "operation_uuid": "018f0f4e-7b8c-7000-8000-000000000428",
        "run_uuid": "018f0f4e-7b8c-7000-8000-000000000429",
        "attachment_uuid": "018f0f4e-7b8c-7000-8000-000000000430",
        "descriptor": descriptor,
    }
    resolved = forge.invoke_resolved_recorded(**request)
    assert resolved.result.num_rows == 1
    assert resolved.attachment_state == "attached"
    assert resolved.attachment_uuid == request["attachment_uuid"]
    assert resolved.attachment_error_code is None
    assert resolved.attachment.num_rows == 1
    replayed = forge.attach_resolved_run(
        projection=projection,
        operation_uuid=request["operation_uuid"],
        attachment_uuid=request["attachment_uuid"],
        run_uuid=request["run_uuid"],
        descriptor=descriptor,
    )
    assert replayed.equals(resolved.attachment)


def check_confidence_assessments() -> None:
    forge = g.GraphForge()
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000311",
        capability_id="provenance",
        capability_version=1,
    )
    node = forge.add_node("Person", name="Ada")
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000312",
        capability_id="knowledge",
        capability_version=1,
    )
    assertion_uuid = "018f0f4e-7b8c-7000-8000-000000000313"
    forge.create_assertion(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000314",
        assertion_uuid=assertion_uuid,
        claim="confidence target",
        graph_refs=[
            {
                "graph_uuid": node.uuid,
                "graph_kind": "node",
                "role": "subject",
                "ordinal": 0,
            }
        ],
    )
    confidence_uuid = "018f0f4e-7b8c-7000-8000-000000000315"
    request = {
        "operation_uuid": "018f0f4e-7b8c-7000-8000-000000000316",
        "confidence_uuid": confidence_uuid,
        "assertion_uuid": assertion_uuid,
        "policy": "explicit",
        "value": 0.75,
    }
    created = forge.assess_confidence(**request)
    assert forge.assess_confidence(**request).equals(created)
    assert created.column_names == [
        "confidence_uuid",
        "assertion_uuid",
        "policy",
        "policy_version",
        "value",
        "provenance_uuid",
        "recorded_at",
        "contract_version",
    ]
    assert created.column("value").to_pylist() == [0.75]
    assert forge.confidence_assessment(confidence_uuid).equals(created)
    assert forge.list_confidence_assessments(assertion_uuid=assertion_uuid).equals(created)
    assert forge.confidence_inputs(confidence_uuid).num_rows == 0


def check_evidence_links() -> None:
    forge = g.GraphForge()
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000321",
        capability_id="provenance",
        capability_version=1,
    )
    node = forge.add_node("Person", name="Ada")
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000322",
        capability_id="knowledge",
        capability_version=1,
    )
    assertion_uuid = "018f0f4e-7b8c-7000-8000-000000000323"
    evidence_uuid = "018f0f4e-7b8c-7000-8000-000000000324"
    created_assertion = forge.create_assertion_with_evidence(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000325",
        assertion_uuid=assertion_uuid,
        claim="evidence target",
        graph_refs=[
            {
                "graph_uuid": node.uuid,
                "graph_kind": "node",
                "role": "subject",
                "ordinal": 0,
            }
        ],
        evidence=[
            {
                "evidence_uuid": evidence_uuid,
                "source_uuid": node.uuid,
                "source_kind": "graph_node",
                "role": "supports",
                "weight": 0.8,
            }
        ],
    )
    assert created_assertion.column("assertion_uuid").to_pylist() == [
        bytes.fromhex(assertion_uuid.replace("-", ""))
    ]
    evidence = forge.evidence_link(evidence_uuid)
    assert evidence.column("role").to_pylist() == ["supports"]
    assert evidence.column("weight").to_pylist() == [0.8]
    assert forge.list_evidence_links(assertion_uuid=assertion_uuid).equals(evidence)


def check_algorithm_runs() -> None:
    forge = g.GraphForge()
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000331",
        capability_id="provenance",
        capability_version=1,
    )
    forge.add_node("Person", name="Ada")
    forge.enable_capability(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000332",
        capability_id="knowledge",
        capability_version=1,
    )
    descriptor = forge.prepare_rank_invocation("Person", by="degree")
    run_uuid = "018f0f4e-7b8c-7000-8000-000000000333"
    recorded = forge.invoke_recorded(
        operation_uuid="018f0f4e-7b8c-7000-8000-000000000334",
        run_uuid=run_uuid,
        descriptor=descriptor,
    )
    assert recorded.run_uuid == run_uuid
    assert recorded.result.num_rows == 1
    assert forge.algorithm_run(run_uuid).column("algorithm").to_pylist() == ["rank.degree"]
    assert forge.algorithm_run_events(run_uuid).column("state").to_pylist() == [
        "started",
        "completed",
    ]
    assert forge.list_algorithm_runs(algorithm="rank.degree").num_rows == 1
    conflicting = forge.prepare_rank_invocation("Person", by="pagerank")
    try:
        forge.invoke_recorded(
            operation_uuid="018f0f4e-7b8c-7000-8000-000000000335",
            run_uuid=run_uuid,
            descriptor=conflicting,
        )
    except g.GraphForgeError as exc:
        assert exc.code == "GF_IDEMPOTENCY_CONFLICT", exc.code
    else:
        raise SystemExit("expected recorded algorithm descriptor conflict")
    assert forge.algorithm_run_events(run_uuid).num_rows == 2


def main() -> None:
    check_construction()
    check_exception_hierarchy()
    check_execute()
    check_typed_uuid_parameters()
    check_add_node()
    check_bfs_paths()
    check_dfs_paths()
    check_dijkstra_paths()
    check_astar_paths()
    check_bellman_ford_paths()
    check_delta_stepping_paths()
    check_yens_paths()
    check_floyd_warshall_paths()
    check_transitive_closure_paths()
    check_random_walk_paths()
    check_maximum_flow_paths()
    check_minimum_cost_maximum_flow_paths()
    check_minimum_cut_paths()
    check_is_dag()
    check_topological_sort()
    check_articulation_points()
    check_bridges()
    check_minimum_spanning_tree()
    check_minimum_k_spanning_tree()
    check_maximum_spanning_tree()
    check_triangle_count()
    check_transitivity()
    check_conductance()
    check_modularity_result()
    check_modularity_unit_weight_and_knowledge_boundary()
    check_modularity_errors()
    check_max_weight_matching()
    check_max_cardinality_matching_result()
    check_max_cardinality_matching_empty()
    check_max_cardinality_matching_errors()
    check_max_bipartite_matching()
    check_is_planar()
    check_triad_census()
    check_dyad_census()
    check_node_coloring()
    check_k1_coloring()
    check_chromatic_number()
    check_count_automorphisms()
    check_edge_coloring()
    check_find_cycles()
    check_has_euler_circuit()
    check_has_euler_path()
    check_euler_circuit()
    check_euler_path()
    check_dag_longest_path()
    check_weighted_dag_longest_path()
    check_parse_error_span()
    check_explain()
    check_clear()
    check_load_ontology()
    check_execute_polars()
    check_execute_stream()
    check_assertions()
    check_reasoning()
    check_assertion_status()
    check_assertion_supersessions()
    check_hypothesis_selection()
    check_assertion_validity()
    check_resolved_belief_projection()
    check_confidence_assessments()
    check_evidence_links()
    check_algorithm_runs()
    check_degree_rank()
    check_pagerank()
    check_betweenness()
    check_closeness()
    check_harmonic_closeness()
    check_eigenvector()
    check_article_rank()
    check_hits_hub()
    check_hits_authority()
    check_celf()
    check_clustering_coefficient()
    check_triangles()
    check_k_core()
    check_preferential_attachment()
    check_adamic_adar()
    check_common_neighbors()
    check_resource_allocation()
    check_total_neighbors()
    check_components_cluster()
    check_strongly_connected_cluster()
    check_biconnected_cluster()
    check_k_core_decomposition_cluster()
    check_louvain_cluster()
    check_leiden_cluster()
    check_label_propagation_cluster()
    check_speaker_listener_cluster()
    check_girvan_newman_cluster()
    check_modularity_optimization_cluster()
    check_fastgreedy_cluster()
    check_infomap_cluster()
    check_leading_eigenvector_cluster()
    check_walktrap_cluster()
    check_spinglass_cluster()
    check_hdbscan_cluster()
    check_kmeans_cluster()
    check_approximate_max_cut_cluster()
    check_node_similarity()
    check_filtered_node_similarity()
    check_knn()
    check_filtered_knn()
    check_cosine()
    check_find()
    check_inspection_surface()
    check_lifecycle()
    print(f"native smoke OK: graphforge {g.__version__}")


if __name__ == "__main__":
    main()
