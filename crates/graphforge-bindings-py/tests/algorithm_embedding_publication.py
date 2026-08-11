"""Fresh-wheel acceptance for canonical algorithm embedding publication."""

import tempfile
import uuid

import pyarrow as pa

import graphforge as g


def _expect(text: str, call) -> None:
    try:
        call()
    except g.GraphForgeError as error:
        assert text in str(error), str(error)
    else:
        raise AssertionError(f"expected GraphForgeError containing {text!r}")


def _schema(
    algorithm: str = "node2vec",
    *,
    algorithm_version: str = "node2vec-v1",
    dimensions: int = 2,
    seed: int = 0,
) -> pa.Schema:
    return pa.schema(
        [
            pa.field("node_uuid", pa.binary(16), nullable=False),
            pa.field(
                "embedding",
                pa.list_(
                    pa.field("item", pa.float32(), nullable=False),
                    list_size=dimensions,
                ),
                nullable=False,
            ),
        ],
        metadata={
            "graphforge.algorithm": algorithm,
            "graphforge.verb": "analyze",
            "graphforge.algorithm_version": algorithm_version,
            "graphforge.algorithm_schema_version": "1",
            "graphforge.dimensions": str(dimensions),
            "graphforge.seed": str(seed),
            "graphforge.rng_version": "splitmix64-v1",
            "graphforge.rng_derivation": "graphforge-embedding-substream-v1",
        },
    )


def _batch(schema: pa.Schema, rows: list[tuple[str, list[float]]]) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [
            pa.array([uuid.UUID(node).bytes for node, _ in rows], type=pa.binary(16)),
            pa.array(
                [vector for _, vector in rows],
                type=schema.field("embedding").type,
            ),
        ],
        schema=schema,
    )


def _publish(forge: g.GraphForge, name: str, result: pa.Table, **options) -> str:
    return forge.publish_algorithm_embeddings(
        name,
        result,
        algorithm=options.pop("algorithm", "node2vec"),
        algorithm_version=options.pop("algorithm_version", "node2vec-v1"),
        dimensions=options.pop("dimensions", 2),
        hyperparameters=options.pop("hyperparameters", {"walks": 8, "nested": [True]}),
        input_recipe=options.pop("input_recipe", {"recipe": "algorithm_nodes_v1"}),
        source_projection=options.pop(
            "source_projection", {"label": "Person", "recipe": "all_people_v1"}
        ),
        **options,
    )


def check_algorithm_embedding_publication() -> None:
    with tempfile.TemporaryDirectory() as project:
        forge = g.GraphForge(project)
        alice = forge.add_node("Person", name="Alice")
        bob = forge.add_node("Person", name="Bob")
        schema = _schema()
        result = pa.Table.from_batches(
            [
                _batch(schema, [(alice.uuid, [1.0, 0.0])]),
                _batch(schema, [(bob.uuid, [0.0, 1.0])]),
            ],
            schema=schema,
        )

        identity = _publish(forge, "structural", result)
        assert len(identity) == 64
        assert _publish(forge, "structural", result) == identity
        space = forge.embedding_space("structural")
        assert space["producer"] == {
            "kind": "algorithm",
            "algorithm": "node2vec",
            "algorithm_version": "node2vec-v1",
        }
        found = forge.find(vector=[1.0, 0.0], label="Person", space="structural", limit=2)
        assert [uuid.UUID(bytes=value).hex for value in found["node_uuid"].to_pylist()] == [
            uuid.UUID(alice.uuid).hex,
            uuid.UUID(bob.uuid).hex,
        ]
        assert not {
            "confidence",
            "provenance_id",
            "assertion_uuid",
            "belief_status",
            "valid_time",
        }.intersection(found.column_names)

        normalized = _publish(
            forge,
            "normalized",
            pa.Table.from_batches([_batch(schema, [(alice.uuid, [3.0, 4.0])])], schema=schema),
            normalization="l2",
        )
        assert normalized != identity

        _expect(
            "non-canonical algorithm metadata",
            lambda: _publish(forge, "structural", result, algorithm_version="node2vec-v2"),
        )
        replacement_schema = _schema(algorithm_version="node2vec-v2")
        replacement_result = pa.Table.from_batches(
            [
                _batch(
                    replacement_schema,
                    [
                        (alice.uuid, [1.0, 0.0]),
                        (bob.uuid, [0.0, 1.0]),
                    ],
                )
            ],
            schema=replacement_schema,
        )
        replaced = _publish(
            forge,
            "structural",
            replacement_result,
            algorithm_version="node2vec-v2",
            replace=True,
        )
        assert replaced != identity

        _expect(
            "requires an embedding analysis algorithm",
            lambda: _publish(
                forge,
                "unsupported",
                pa.Table.from_batches(
                    [
                        _batch(
                            _schema("is_dag", algorithm_version="not-an-embedding-v1"),
                            [(alice.uuid, [1.0, 0.0])],
                        )
                    ],
                    schema=_schema("is_dag", algorithm_version="not-an-embedding-v1"),
                ),
                algorithm="is_dag",
                algorithm_version="not-an-embedding-v1",
            ),
        )
        _expect(
            "duplicate",
            lambda: _publish(
                forge,
                "duplicate",
                pa.Table.from_batches(
                    [
                        _batch(
                            schema,
                            [
                                (alice.uuid, [1.0, 0.0]),
                                (alice.uuid, [0.0, 1.0]),
                            ],
                        )
                    ],
                    schema=schema,
                ),
            ),
        )
        variable_schema = pa.schema(
            [
                pa.field("node_uuid", pa.binary(16), nullable=False),
                pa.field(
                    "embedding",
                    pa.list_(pa.field("item", pa.float32(), nullable=False)),
                    nullable=False,
                ),
            ],
            metadata=schema.metadata,
        )
        _expect(
            "exact node_uuid and embedding fields",
            lambda: _publish(
                forge,
                "variable-list",
                pa.Table.from_arrays(
                    [
                        pa.array([uuid.UUID(alice.uuid).bytes], type=pa.binary(16)),
                        pa.array([[1.0, 0.0]], type=variable_schema.field("embedding").type),
                    ],
                    schema=variable_schema,
                ),
            ),
        )
        missing_metadata = dict(schema.metadata)
        del missing_metadata[b"graphforge.rng_derivation"]
        _expect(
            "non-canonical algorithm metadata",
            lambda: _publish(
                forge,
                "missing-metadata",
                result.replace_schema_metadata(missing_metadata),
            ),
        )
        extra_metadata = dict(schema.metadata)
        extra_metadata[b"graphforge.extra"] = b"forbidden"
        _expect(
            "non-canonical algorithm metadata",
            lambda: _publish(
                forge,
                "extra-metadata",
                result.replace_schema_metadata(extra_metadata),
            ),
        )
        _expect(
            "non-zero",
            lambda: _publish(
                forge,
                "zero",
                pa.Table.from_batches([_batch(schema, [(alice.uuid, [0.0, 0.0])])], schema=schema),
            ),
        )
        _expect(
            "input recipe",
            lambda: _publish(forge, "empty-recipe", result, input_recipe={}),
        )
        _expect(
            "unknown algorithm embedding normalization",
            lambda: _publish(forge, "normalization", result, normalization="unitish"),
        )

        empty = _publish(
            forge,
            "empty",
            pa.Table.from_batches(
                [_batch(_schema(dimensions=3), [])],
                schema=_schema(dimensions=3),
            ),
            dimensions=3,
            source_projection={"label": "Nobody"},
        )
        assert len(empty) == 64
        forge.close()

        reopened = g.GraphForge(project)
        persisted = reopened.find(vector=[0.0, 1.0], label="Person", space="structural", limit=2)
        assert uuid.UUID(bytes=persisted["node_uuid"][0].as_py()).hex == uuid.UUID(bob.uuid).hex
        reopened.close()


if __name__ == "__main__":
    check_algorithm_embedding_publication()
