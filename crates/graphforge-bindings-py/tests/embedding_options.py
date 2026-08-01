"""Fresh-wheel acceptance for thin Python embedding option construction."""

import math

import pyarrow as pa

import graphforge as g


def _expect(error_type: type[Exception], text: str, call) -> None:
    try:
        call()
    except error_type as error:
        assert text in str(error), str(error)
    else:
        raise AssertionError(f"expected {error_type.__name__} containing {text!r}")


def _options(forge: g.GraphForge, by: str, **overrides):
    defaults = {
        "graphsage": {
            "embedding_options": {"feature_properties": ["age"]},
        }
    }
    options = defaults.get(by, {}) | overrides
    return forge.analyze("Person", by=by, **options)


def check_embedding_option_construction() -> None:
    forge = g.GraphForge()

    node2vec = _options(forge, "node2vec")
    assert node2vec.num_rows == 0
    assert node2vec.column_names == ["node_uuid", "embedding"]
    assert node2vec.schema.metadata[b"graphforge.algorithm"] == b"node2vec"

    fastrp = _options(forge, "fast_random_projection")
    assert fastrp.num_rows == 0
    assert fastrp.schema.metadata[b"graphforge.algorithm"] == b"fast_random_projection"
    hashgnn = _options(forge, "hashgnn")
    assert isinstance(hashgnn, pa.Table)
    assert hashgnn.num_rows == 0
    assert hashgnn.schema.metadata[b"graphforge.algorithm"] == b"hashgnn"
    graphsage = _options(forge, "graphsage")
    assert graphsage.num_rows == 0
    assert graphsage.schema.metadata[b"graphforge.algorithm"] == b"graphsage"
    assert _options(forge, "node2vec", via="KNOWS", weight="strength").num_rows == 0

    explicit = {
        "node2vec": {
            "dimensions": 64,
            "walk_length": 12,
            "walks_per_node": 4,
            "p": 0.5,
            "q": 2.0,
            "window_size": 3,
            "negative_samples": 2,
            "epochs": 2,
            "learning_rate": 0.01,
            "seed": 7,
        },
        "graphsage": {
            "dimensions": 96,
            "hidden_dimensions": 48,
            "layers": 1,
            "sample_sizes": [8],
            "aggregator": "mean",
            "epochs": 2,
            "negative_samples": 4,
            "learning_rate": 0.001,
            "feature_properties": ["age", "score"],
            "seed": 8,
        },
        "fast_random_projection": {
            "dimensions": 80,
            "iteration_weights": [0.0, 0.5, 1.0],
            "normalization_strength": 0.0,
            "feature_weight": 0.25,
            "feature_properties": ["age"],
            "seed": 9,
        },
        "hashgnn": {
            "dimensions": 512,
            "iterations": 4,
            "embedding_density": 0.5,
            "heterogeneous": True,
            "node_type_property": "node_kind",
            "relationship_type_property": "edge_kind",
            "seed": 10,
        },
    }
    for algorithm, embedding_options in explicit.items():
        if algorithm in ("node2vec", "fast_random_projection", "hashgnn"):
            result = _options(forge, algorithm, embedding_options=embedding_options)
            assert result.num_rows == 0
            expected_dimensions = {
                "node2vec": b"64",
                "fast_random_projection": b"80",
                "hashgnn": b"512",
            }[algorithm]
            assert result.schema.metadata[b"graphforge.dimensions"] == expected_dimensions
            continue
        if algorithm == "graphsage":
            result = _options(forge, algorithm, embedding_options=embedding_options)
            assert result.num_rows == 0
            assert result.schema.metadata[b"graphforge.dimensions"] == b"96"
            continue
    invalid = [
        ("node2vec", {}, {"dimensions": 0}, "embedding dimensions"),
        ("node2vec", {}, {"learning_rate": 0.0}, "finite and positive"),
        (
            "graphsage",
            {"directed": False},
            {"feature_properties": []},
            "non-empty ordered list",
        ),
        (
            "fast_random_projection",
            {},
            {"feature_properties": ["confidence", "confidence"]},
            "cannot contain duplicate names",
        ),
        (
            "hashgnn",
            {},
            {"node_type_property": "kind"},
            "homogeneous hashgnn",
        ),
        ("hashgnn", {}, {"seed": -1}, "unsigned 64-bit"),
    ]
    for algorithm, invocation, embedding_options, message in invalid:
        _expect(
            g.ValidationError,
            message,
            lambda by=algorithm, call_options=invocation, options=embedding_options: _options(
                forge, by, embedding_options=options, **call_options
            ),
        )

    _expect(
        g.ValidationError,
        "unknown node2vec option",
        lambda: _options(forge, "node2vec", embedding_options={"provenance": "source"}),
    )
    _expect(
        g.ValidationError,
        "knowledge-layer field",
        lambda: _options(forge, "node2vec", via="evidence"),
    )
    _expect(
        g.ValidationError,
        "does not accept embedding_options",
        lambda: forge.analyze(by="is_dag", embedding_options={}),
    )
    _expect(
        g.ValidationError,
        "requires directed=false",
        lambda: _options(forge, "graphsage", directed=True),
    )
    forge.close()


def check_non_empty_node2vec_execution() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'}), (:Person {name:'Carol'})"
    )
    options = {
        "dimensions": 4,
        "walk_length": 3,
        "walks_per_node": 2,
        "window_size": 1,
        "negative_samples": 1,
        "epochs": 1,
        "seed": 7,
    }
    result = forge.analyze(
        "Person",
        by="node2vec",
        via="KNOWS",
        embedding_options=options,
    )
    assert result.equals(
        forge.analyze(
            "Person",
            by="node2vec",
            via="KNOWS",
            embedding_options=options,
        )
    )
    assert result.num_rows == 3
    assert result.schema.names == ["node_uuid", "embedding"]
    assert all(not field.nullable for field in result.schema)
    assert str(result.schema.field("embedding").type) == "fixed_size_list<item: float not null>[4]"
    assert result.schema.metadata == {
        b"graphforge.algorithm": b"node2vec",
        b"graphforge.verb": b"analyze",
        b"graphforge.algorithm_version": b"node2vec-v1",
        b"graphforge.algorithm_schema_version": b"1",
        b"graphforge.dimensions": b"4",
        b"graphforge.seed": b"7",
        b"graphforge.rng_version": b"splitmix64-v1",
        b"graphforge.rng_derivation": b"graphforge-embedding-substream-v1",
    }
    node_uuids = result.column("node_uuid").to_pylist()
    assert node_uuids == sorted(node_uuids)
    vectors = result.column("embedding").to_pylist()
    assert all(len(vector) == 4 for vector in vectors)
    assert all(math.isfinite(value) for vector in vectors for value in vector)
    forge.close()


def check_non_empty_graphsage_execution() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (:Person {name:'Alice', score:1.0, features:[1.0,0.0]})"
        "-[:KNOWS]->(:Person {name:'Bob', score:2.0, features:[0.0,1.0]}), "
        "(:Person {name:'Carol', score:3.0, features:[0.5,0.5]})"
    )
    options = {
        "dimensions": 2,
        "hidden_dimensions": 2,
        "layers": 1,
        "sample_sizes": [1],
        "epochs": 1,
        "negative_samples": 1,
        "learning_rate": 0.001,
        "feature_properties": ["score", "features"],
        "seed": 13,
    }
    result = forge.analyze(
        "Person",
        by="graphsage",
        via="KNOWS",
        directed=False,
        embedding_options=options,
    )
    repeated = forge.analyze(
        "Person",
        by="graphsage",
        via="KNOWS",
        directed=False,
        embedding_options=options,
    )
    assert result.equals(repeated)
    assert result.num_rows == 3
    assert result.schema.names == ["node_uuid", "embedding"]
    assert all(not field.nullable for field in result.schema)
    assert str(result.schema.field("embedding").type) == (
        "fixed_size_list<item: float not null>[2]"
    )
    assert result.schema.metadata == {
        b"graphforge.algorithm": b"graphsage",
        b"graphforge.verb": b"analyze",
        b"graphforge.algorithm_version": b"graphsage-unsupervised-v1",
        b"graphforge.algorithm_schema_version": b"1",
        b"graphforge.dimensions": b"2",
        b"graphforge.seed": b"13",
        b"graphforge.rng_version": b"splitmix64-v1",
        b"graphforge.rng_derivation": b"graphforge-embedding-substream-v1",
    }
    node_uuids = result.column("node_uuid").to_pylist()
    assert node_uuids == sorted(node_uuids)
    vectors = result.column("embedding").to_pylist()
    assert all(len(vector) == 2 for vector in vectors)
    assert all(math.isfinite(value) for vector in vectors for value in vector)
    forge.close()


def check_non_empty_fastrp_execution() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (:Person {name:'Alice', score:1.0})"
        "-[:KNOWS {strength:2.0}]->(:Person {name:'Bob', score:2.0}), "
        "(:Person {name:'Carol', score:3.0})"
    )
    options = {
        "dimensions": 4,
        "iteration_weights": [1.0, 1.0],
        "feature_weight": 1.0,
        "feature_properties": ["score"],
        "seed": 11,
    }
    result = forge.analyze(
        "Person",
        by="fast_random_projection",
        via="KNOWS",
        weight="strength",
        embedding_options=options,
    )
    repeated = forge.analyze(
        "Person",
        by="fast_random_projection",
        via="KNOWS",
        weight="strength",
        embedding_options=options,
    )
    assert result.equals(repeated)
    assert result.num_rows == 3
    assert result.schema.names == ["node_uuid", "embedding"]
    assert all(not field.nullable for field in result.schema)
    assert str(result.schema.field("embedding").type) == "fixed_size_list<item: float not null>[4]"
    assert result.schema.metadata == {
        b"graphforge.algorithm": b"fast_random_projection",
        b"graphforge.verb": b"analyze",
        b"graphforge.algorithm_version": b"fastrp-v1",
        b"graphforge.algorithm_schema_version": b"1",
        b"graphforge.dimensions": b"4",
        b"graphforge.seed": b"11",
        b"graphforge.rng_version": b"splitmix64-v1",
        b"graphforge.rng_derivation": b"graphforge-embedding-substream-v1",
    }
    node_uuids = result.column("node_uuid").to_pylist()
    assert node_uuids == sorted(node_uuids)
    vectors = result.column("embedding").to_pylist()
    assert all(len(vector) == 4 for vector in vectors)
    assert all(math.isfinite(value) for vector in vectors for value in vector)
    forge.close()


def check_non_empty_hashgnn_execution() -> None:
    forge = g.GraphForge()
    forge.execute(
        "CREATE (:Person {name:'Alice', kind:'human'})"
        "-[:KNOWS {kind:'friend'}]->"
        "(:Person {name:'Bob', kind:'bot'})"
        "-[:KNOWS {kind:'colleague'}]->"
        "(:Person {name:'Carol', kind:'human'})"
    )
    options = {
        "dimensions": 8,
        "iterations": 2,
        "embedding_density": 0.25,
        "heterogeneous": True,
        "node_type_property": "kind",
        "relationship_type_property": "kind",
        "seed": 19,
    }
    result = forge.analyze(
        "Person",
        by="hashgnn",
        via="KNOWS",
        directed=True,
        embedding_options=options,
    )
    repeated = forge.analyze(
        "Person",
        by="hashgnn",
        via="KNOWS",
        directed=True,
        embedding_options=options,
    )
    assert isinstance(result, pa.Table)
    assert result.equals(repeated)
    for selector in ("node_type_property", "relationship_type_property"):
        _expect(
            g.ValidationError,
            "missing HashGNN type property",
            lambda selector=selector: forge.analyze(
                "Person",
                by="hashgnn",
                via="KNOWS",
                directed=True,
                embedding_options=options | {selector: "missing"},
            ),
        )
    assert result.num_rows == 3
    assert result.schema.names == ["node_uuid", "embedding"]
    assert all(not field.nullable for field in result.schema)
    assert str(result.schema.field("embedding").type) == "fixed_size_list<item: float not null>[8]"
    assert result.schema.metadata == {
        b"graphforge.algorithm": b"hashgnn",
        b"graphforge.verb": b"analyze",
        b"graphforge.algorithm_version": b"hashgnn-v1",
        b"graphforge.algorithm_schema_version": b"1",
        b"graphforge.dimensions": b"8",
        b"graphforge.seed": b"19",
        b"graphforge.rng_version": b"splitmix64-v1",
        b"graphforge.rng_derivation": b"graphforge-embedding-substream-v1",
    }
    node_uuids = result.column("node_uuid").to_pylist()
    assert node_uuids == sorted(node_uuids)
    vectors = result.column("embedding").to_pylist()
    assert all(len(vector) == 8 for vector in vectors)
    assert all(value in (0.0, 1.0) for vector in vectors for value in vector)
    forge.close()


if __name__ == "__main__":
    check_embedding_option_construction()
    check_non_empty_node2vec_execution()
    check_non_empty_graphsage_execution()
    check_non_empty_fastrp_execution()
    check_non_empty_hashgnn_execution()
