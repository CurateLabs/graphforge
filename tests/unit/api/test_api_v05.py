"""GraphForge v0.5 public-API tests against the native (PyO3) binding.

The 0.4.x pure-Python stub (``src/graphforge``) was retired in the cutover; these
tests exercise the native ``graphforge`` engine. Implemented query, search,
analyst, lifecycle, construction, and graph-inspection paths return their real
native results. Tests assert the full intended UX, with genuine gaps kept as
explicit expectations.
"""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

import graphforge as g
from graphforge import GraphForge
from graphforge._graphforge_rs import EdgeHandle as NativeEdgeHandle
from graphforge._graphforge_rs import NodeHandle as NativeNodeHandle


class TestConstruction:
    def test_in_memory(self) -> None:
        forge = GraphForge()
        assert forge.path is None
        assert forge.ontology_mode == "exploratory"

    def test_in_memory_repr(self) -> None:
        assert repr(GraphForge()) == "GraphForge(in-memory)"

    def test_persistent(self, tmp_path) -> None:
        d = tmp_path / "proj"
        d.mkdir()
        forge = GraphForge(str(d))
        assert forge.path == str(d)

    def test_missing_path_raises_storage(self, tmp_path) -> None:
        # A guaranteed-missing path under tmp_path keeps this portable (a bare
        # POSIX "/no/such/dir" is interpreted against the current drive on Windows).
        missing = tmp_path / "does_not_exist" / "graph"
        with pytest.raises(g.StorageError):
            GraphForge(str(missing))

    def test_native_add_node_returns_uuid_handle(self) -> None:
        forge = GraphForge()
        handle = forge.add_node("Person", name="Alice")
        assert isinstance(handle, NativeNodeHandle)
        assert uuid.UUID(handle.uuid).version == 7
        assert not hasattr(handle, "id")
        assert forge.execute("MATCH (n:Person) RETURN n.name").num_rows == 1

    def test_native_add_edge_returns_uuid_handle_and_persists(self, tmp_path) -> None:
        project = tmp_path / "edge-project"
        project.mkdir()
        forge = GraphForge(str(project))
        alice = forge.add_node("Person", name="Alice")
        bob = forge.add_node("Person", name="Bob")

        edge = forge.add_edge(alice, "KNOWS", bob, since=2026)

        assert isinstance(edge, NativeEdgeHandle)
        assert uuid.UUID(edge.uuid).version == 7
        assert edge.rel_type == "KNOWS"
        del forge
        reopened = GraphForge(str(project))
        rows = reopened.execute(
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.name AS source, r.since AS since, b.name AS target"
        )
        assert rows.to_pylist() == [{"source": "Alice", "since": 2026, "target": "Bob"}]


class TestProjectCapabilities:
    def test_inspect_enable_and_retry_use_arrow_contract(self) -> None:
        forge = GraphForge()
        initial = forge.project_capabilities()
        assert initial.column("capability_id").to_pylist() == ["graph", "workspace"]
        assert initial.schema.metadata == {
            b"graphforge.contract.id": b"project_capability",
            b"graphforge.contract.version": b"1",
        }

        operation_uuid = str(uuid.uuid4())
        enabled = forge.enable_capability(
            operation_uuid=operation_uuid,
            capability_id="knowledge",
            capability_version=1,
        )
        replayed = forge.enable_capability(
            operation_uuid=operation_uuid,
            capability_id="knowledge",
            capability_version=1,
        )
        assert enabled.column("capability_id").to_pylist() == [
            "graph",
            "knowledge",
            "workspace",
        ]
        assert (
            replayed.column("generation_uuid").to_pylist()
            == enabled.column("generation_uuid").to_pylist()
        )
        assert (
            replayed.column("capability_id").to_pylist()
            == enabled.column("capability_id").to_pylist()
        )

    def test_unsupported_version_preserves_structured_code(self) -> None:
        with pytest.raises(g.StorageError) as excinfo:
            GraphForge().enable_capability(
                operation_uuid=str(uuid.uuid4()),
                capability_id="knowledge",
                capability_version=2,
            )
        assert excinfo.value.code == "GF_UNSUPPORTED_CAPABILITY_VERSION"

    def test_provenance_reads_and_cancellation_are_native_arrow(self) -> None:
        forge = GraphForge()
        forge.enable_capability(
            operation_uuid=str(uuid.uuid4()),
            capability_id="provenance",
            capability_version=1,
        )
        node = forge.add_node("Person", name="Ada")

        history = forge.list_provenance_history(subject_uuid=node.uuid)
        assert history.num_rows == 1
        assert history.column("event_kind").to_pylist() == ["create_node"]
        event_uuid = str(uuid.UUID(bytes=history.column("provenance_uuid")[0].as_py()))
        event = forge.provenance_event(event_uuid)
        assert event.num_rows == 1
        assert event.schema == history.select(event.schema.names).schema

        cancellation = g.CancellationToken()
        cancellation.cancel()
        assert cancellation.is_cancelled
        with pytest.raises(g.GraphForgeError) as excinfo:
            forge.list_provenance_history(cancellation=cancellation)
        assert excinfo.value.code == "GF_CANCELLED"


class TestExecute:
    def test_create_then_match(self) -> None:
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice', age: 30})")
        forge.execute("CREATE (:Person {name: 'Bob', age: 25})")
        table = forge.execute("MATCH (p:Person) RETURN p.name AS name, p.age AS age")
        assert isinstance(table, pa.Table)
        assert table.num_rows == 2
        assert set(table.column("name").to_pylist()) == {"Alice", "Bob"}

    def test_params(self) -> None:
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice', age: 30})")
        forge.execute("CREATE (:Person {name: 'Bob', age: 25})")
        table = forge.execute(
            "MATCH (p:Person) WHERE p.age > $min RETURN p.name AS name", {"min": 28}
        )
        assert table.column("name").to_pylist() == ["Alice"]

    def test_returns_arrow_table(self) -> None:
        result = GraphForge().execute("MATCH (n) RETURN n.node_uuid AS id")
        assert isinstance(result, pa.Table)

    def test_empty_result_keeps_schema(self) -> None:
        empty = GraphForge().execute("MATCH (n:Nope) RETURN n.node_uuid AS id")
        assert isinstance(empty, pa.Table)
        assert empty.num_rows == 0
        assert empty.column_names == ["id"]

    def test_result_carries_query_metadata(self) -> None:
        table = GraphForge().execute("MATCH (n) RETURN n.node_uuid AS id")
        meta = table.schema.metadata or {}
        assert b"graphforge.query_id" in meta
        assert b"graphforge.ir_version" in meta

    def test_parse_error_has_span(self) -> None:
        with pytest.raises(g.ParseError) as excinfo:
            GraphForge().execute("MATCH (n) RETURN n WHERE")
        err = excinfo.value
        assert isinstance(err, g.GraphForgeError)
        assert isinstance(err.span, tuple) and len(err.span) == 2

    def test_bind_error_has_span(self) -> None:
        # A binder failure (undeclared variable) surfaces as ParseError with a
        # (offset, length) span pinpointing the token (#353 / #606).
        with pytest.raises(g.ParseError) as excinfo:
            GraphForge().execute("RETURN missingVar")
        err = excinfo.value
        assert isinstance(err, g.GraphForgeError)
        assert err.code == "GF_PARSE"
        assert isinstance(err.span, tuple) and len(err.span) == 2
        offset, length = err.span
        assert "RETURN missingVar"[offset : offset + length] == "missingVar"

    def test_empty_query_raises_validation(self) -> None:
        with pytest.raises(g.ValidationError):
            GraphForge().execute("   ")


class TestExecutePolars:
    def test_returns_dataframe(self) -> None:
        pl = pytest.importorskip("polars")
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Bob'})")
        df = forge.execute_polars("MATCH (p:Person) RETURN p.name AS name")
        assert isinstance(df, pl.DataFrame)
        assert df["name"][0] == "Bob"

    def test_params_bind(self) -> None:
        pytest.importorskip("polars")
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice', age: 30})")
        forge.execute("CREATE (:Person {name: 'Bob', age: 25})")
        df = forge.execute_polars(
            "MATCH (p:Person) WHERE p.age > $min RETURN p.name AS name", {"min": 28}
        )
        assert df["name"].to_list() == ["Alice"]

    def test_closed_instance_raises_lifecycle(self) -> None:
        forge = GraphForge()
        forge.close()
        with pytest.raises(g.LifecycleError):
            forge.execute_polars("MATCH (n) RETURN n.node_uuid AS id")


class TestRank:
    def test_degree_uses_native_uuid_only_arrow_contract(self) -> None:
        forge = GraphForge()
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
        with pytest.raises(
            g.ValidationError,
            match=r"unknown rank algorithm `not_a_rank`",
        ):
            forge.rank("Person", by="not_a_rank")


class TestCluster:
    def test_components_uses_native_uuid_only_arrow_contract(self) -> None:
        forge = GraphForge()
        forge.execute(
            "CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'}), "
            "(c:Person {name:'Carol'})"
        )

        table = forge.cluster("Person", by="components", write_property="component")

        assert table.schema.field("node_uuid").type == pa.binary(16)
        assert table.schema.field("community_id").type == pa.int64()
        assert "node_id" not in table.column_names
        assert table.schema.metadata[b"graphforge.algorithm"] == b"components"
        assert table.column("name").to_pylist() == ["Alice", "Bob", "Carol"]
        assert table.column("community_id").to_pylist() == [0, 0, 1]
        readback = forge.execute("MATCH (p:Person) RETURN p.component AS component ORDER BY p.name")
        assert readback.column("component").to_pylist() == [0, 0, 1]
        with pytest.raises(g.ValidationError, match=r"cluster\.hdbscan"):
            forge.cluster("Person", by="hdbscan")


class TestSimilar:
    def test_node_similarity_uses_native_uuid_only_arrow_contract(self) -> None:
        forge = GraphForge()
        forge.execute(
            "CREATE (a:Person), (b:Person), (c:Person), (d:Person), (e:Person), "
            "(a)-[:KNOWS]->(d), (a)-[:KNOWS]->(e), "
            "(b)-[:KNOWS]->(d), (b)-[:KNOWS]->(e), (c)-[:KNOWS]->(d)"
        )

        table = forge.similar("Person", by="node_similarity", k=1, via="KNOWS")

        assert table.schema.field("node1_uuid").type == pa.binary(16)
        assert table.schema.field("node2_uuid").type == pa.binary(16)
        assert table.schema.field("similarity").type == pa.float64()
        assert "node1_id" not in table.column_names
        assert table.schema.metadata[b"graphforge.algorithm"] == b"node_similarity"
        assert table.column("similarity").to_pylist() == [1.0, 1.0, 0.5]
        with pytest.raises(g.ValidationError, match="similar k must be positive"):
            forge.similar("Person", by="node_similarity", k=0)
        with pytest.raises(g.ValidationError, match="similar.knn"):
            forge.similar("Person", by="knn")


class TestPaths:
    def test_bfs_uses_native_uuid_only_arrow_contract(self) -> None:
        forge = GraphForge()
        alice = forge.add_node("Person", name="Alice")
        forge.add_node("Person", name="Bob")
        dan = forge.add_node("Person", name="Dan")
        forge.execute(
            "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), "
            "(d:Person {name:'Dan'}) CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(d)"
        )

        table = forge.paths(alice, dan, by="bfs", via="KNOWS")

        assert table.column_names == ["source_uuid", "target_uuid", "cost", "path"]
        assert table.schema.field("source_uuid").type == pa.binary(16)
        assert table.schema.field("target_uuid").type == pa.binary(16)
        assert table.schema.field("cost").type == pa.float64()
        path_type = table.schema.field("path").type
        assert pa.types.is_list(path_type)
        assert path_type.value_type == pa.binary(16)
        assert not path_type.value_field.nullable
        assert table.schema.metadata[b"graphforge.algorithm"] == b"bfs"
        assert table.column("cost").to_pylist() == [2.0]
        assert len(table.column("path").to_pylist()[0]) == 3
        with pytest.raises(g.ValidationError, match="bfs k must be 1"):
            forge.paths(alice, by="bfs", k=2)
        with pytest.raises(g.ValidationError, match="astar requires a target selector"):
            forge.paths(alice, by="astar")


class TestExecuteStream:
    def test_returns_record_batch_reader(self) -> None:
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice'})")
        reader = forge.execute_stream("MATCH (p:Person) RETURN p.node_uuid AS id")
        assert isinstance(reader, pa.RecordBatchReader)

    def test_schema_available_before_iteration(self) -> None:
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice'})")
        reader = forge.execute_stream("MATCH (p:Person) RETURN p.node_uuid AS id")
        assert reader.schema.field("id").type == pa.binary(16)
        assert b"graphforge.query_id" in (reader.schema.metadata or {})

    def test_lazy_iteration_yields_batches(self) -> None:
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice'})")
        forge.execute("CREATE (:Person {name: 'Bob'})")
        reader = forge.execute_stream("MATCH (p:Person) RETURN p.node_uuid AS id")
        batches = list(reader)
        assert batches
        assert sum(b.num_rows for b in batches) == 2

    def test_read_all_builds_table(self) -> None:
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice'})")
        reader = forge.execute_stream("MATCH (p:Person) RETURN p.name AS name")
        table = reader.read_all()  # native RecordBatchReader method
        assert table.column("name").to_pylist() == ["Alice"]

    def test_params_bind(self) -> None:
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice', age: 30})")
        forge.execute("CREATE (:Person {name: 'Bob', age: 25})")
        reader = forge.execute_stream(
            "MATCH (p:Person) WHERE p.age > $min RETURN p.name AS name", {"min": 28}
        )
        names = [n for b in reader for n in b.column("name").to_pylist()]
        assert names == ["Alice"]

    def test_rejects_writes(self) -> None:
        with pytest.raises(g.ValidationError):
            GraphForge().execute_stream("CREATE (:Person)")

    def test_closed_instance_raises_lifecycle(self) -> None:
        forge = GraphForge()
        forge.close()
        with pytest.raises(g.LifecycleError):
            forge.execute_stream("MATCH (n) RETURN n.node_uuid AS id")

    def test_reader_outlives_parent(self) -> None:
        import gc

        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice'})")
        reader = forge.execute_stream("MATCH (p:Person) RETURN p.node_uuid AS id")
        del forge
        gc.collect()
        # The RuntimeGuard keeps the runtime alive — consuming must not hang/error.
        assert sum(b.num_rows for b in reader) == 1

    def test_reader_survives_explicit_close(self) -> None:
        # close() only flips the lifecycle flag; an already-issued reader keeps
        # the runtime alive (via RuntimeGuard) and remains consumable.
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice'})")
        reader = forge.execute_stream("MATCH (p:Person) RETURN p.node_uuid AS id")
        forge.close()
        assert sum(b.num_rows for b in reader) == 1

    def test_empty_result_keeps_schema(self) -> None:
        reader = GraphForge().execute_stream("MATCH (n:Nope) RETURN n.node_uuid AS id")
        assert isinstance(reader, pa.RecordBatchReader)
        assert reader.schema.field("id").type == pa.binary(16)
        assert b"graphforge.query_id" in (reader.schema.metadata or {})
        assert sum(b.num_rows for b in reader) == 0

    def test_schema_omits_surrogate_columns(self) -> None:
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice'})")
        reader = forge.execute_stream("MATCH (p:Person) RETURN p.name AS name")
        assert "node_id" not in reader.schema.names
        assert "edge_id" not in reader.schema.names

    def test_incremental_pull(self) -> None:
        # Prove the pull-based (lazy) contract: batches arrive via the reader's
        # one-at-a-time API, not as a single pre-materialised block.
        forge = GraphForge()
        for _ in range(3):
            forge.execute("CREATE (:Person)")
        reader = forge.execute_stream("MATCH (p:Person) RETURN p.node_uuid AS id")
        rows = 0
        while True:
            batch = reader.read_next_batch()  # raises StopIteration when drained
            rows += batch.num_rows
            if rows >= 3:
                break
        assert rows == 3

    def test_missing_parameter_raises(self) -> None:
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice', age: 30})")
        with pytest.raises((g.PlanError, g.ValidationError, g.ExecutionError)):
            forge.execute_stream("MATCH (p:Person) WHERE p.age > $missing RETURN p.name AS n")


class TestExplain:
    def test_contains_pipeline_stages(self) -> None:
        plan = GraphForge().explain("MATCH (n:Person) RETURN n.node_uuid AS id")
        for marker in ("AST", "GraphIR", "LogicalPlan", "PhysicalPlan"):
            assert marker in plan, plan
        assert "NodeScan" in plan


_MINIMAL_ONTOLOGY = """\
ontology_id: binding_surface_test
version: "v1"
entity_types:
  - name: Person
    abstract: false
relation_types: []
properties:
  - owner: Person
    name: name
    type: utf8
    nullable: false
constraints: []
migrations: []
"""


class TestLoadOntology:
    def test_applies_and_promotes_mode(self, tmp_path) -> None:
        onto = tmp_path / "onto.yaml"
        onto.write_text(_MINIMAL_ONTOLOGY)
        forge = GraphForge()
        assert forge.ontology_mode == "exploratory"
        forge.load_ontology(str(onto))
        assert forge.ontology_mode == "advisory"
        assert forge.execute("MATCH (n:Person) RETURN n.node_uuid AS id").num_rows == 0

    def test_missing_file_raises_ontology(self) -> None:
        with pytest.raises(g.OntologyError):
            GraphForge().load_ontology("/no/such/ontology.yaml")


class TestLifecycle:
    def test_execute_after_close_raises(self) -> None:
        forge = GraphForge()
        forge.close()
        with pytest.raises(g.LifecycleError):
            forge.execute("MATCH (n) RETURN n.node_uuid AS id")

    def test_close_is_idempotent(self) -> None:
        forge = GraphForge()
        forge.close()

    def test_clear_resets_and_reuses_in_memory_instance(self) -> None:
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice'})")
        forge.clear()
        assert forge.execute("MATCH (n) RETURN n").num_rows == 0
        forge.execute("CREATE (:Book {title: 'Graph Databases'})")
        assert forge.execute("MATCH (b:Book) RETURN b.title").num_rows == 1

    def test_clear_rejects_persistent_instance_without_data_loss(self, tmp_path) -> None:
        project = tmp_path / "persistent"
        project.mkdir()
        forge = GraphForge(str(project))
        forge.execute("CREATE (:Person {name: 'Alice'})")
        with pytest.raises(g.StorageError):
            forge.clear()
        assert forge.execute("MATCH (n:Person) RETURN n.name").num_rows == 1
        forge.close()


class TestIndexSurface:
    def test_incomplete_typed_request_raises_validation(self) -> None:
        with pytest.raises(g.ValidationError, match="requires text fields"):
            GraphForge().index("text")


class TestFind:
    def test_text_returns_canonical_arrow_table(self) -> None:
        forge = GraphForge()
        forge.execute("CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'})")

        table = forge.find("alice", label="Person")

        assert isinstance(table, pa.Table)
        assert table.column_names == ["node_uuid", "name", "score", "matched_on"]
        assert table.schema.field("node_uuid").type == pa.binary(16)
        assert table.schema.field("score").type == pa.float64()
        assert table.column("name").to_pylist() == ["Alice"]
        assert table.column("matched_on").to_pylist() == ["text"]


class TestInspectionSurface:
    def test_empty_graph_is_exact(self) -> None:
        forge = GraphForge()
        assert forge.labels() == []
        assert forge.relationship_types() == []
        assert forge.node_count() == 0
        assert forge.node_count("Missing") == 0
        table = forge.schema()
        assert table.column_names == ["label", "node_count", "rel_type", "rel_count"]
        assert table.schema == pa.schema(
            [
                pa.field("label", pa.string(), nullable=True),
                pa.field("node_count", pa.uint64(), nullable=True),
                pa.field("rel_type", pa.string(), nullable=True),
                pa.field("rel_count", pa.uint64(), nullable=True),
            ]
        )
        assert table.num_rows == 0

    def test_multi_label_counts_and_schema_are_deterministic(self) -> None:
        forge = GraphForge()
        forge.execute(
            "CREATE (a:Person:Author), (b:Person), (p:Paper), "
            "(a)-[:AUTHORED]->(p), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a)"
        )

        assert forge.labels() == ["Author", "Paper", "Person"]
        assert forge.relationship_types() == ["AUTHORED", "KNOWS"]
        assert forge.node_count() == 3
        assert forge.node_count("Person") == 2
        assert forge.node_count("Person') MATCH (n) RETURN n //") == 0
        table = forge.schema()
        assert table.schema == pa.schema(
            [
                pa.field("label", pa.string(), nullable=True),
                pa.field("node_count", pa.uint64(), nullable=True),
                pa.field("rel_type", pa.string(), nullable=True),
                pa.field("rel_count", pa.uint64(), nullable=True),
            ]
        )
        assert table.to_pydict() == {
            "label": ["Author", "Paper", "Person", None, None],
            "node_count": [1, 1, 2, None, None],
            "rel_type": [None, None, None, "AUTHORED", "KNOWS"],
            "rel_count": [None, None, None, 1, 2],
        }

    def test_generic_transaction_methods_are_absent(self) -> None:
        forge = GraphForge()
        for name in ("begin", "commit", "rollback"):
            assert not hasattr(forge, name)

    @pytest.mark.parametrize(
        "call",
        [
            lambda forge: forge.schema(),
            lambda forge: forge.labels(),
            lambda forge: forge.relationship_types(),
            lambda forge: forge.node_count(),
        ],
    )
    def test_closed_instance_raises_lifecycle(self, call) -> None:
        forge = GraphForge()
        forge.close()
        with pytest.raises(g.LifecycleError):
            call(forge)
