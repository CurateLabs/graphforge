"""Real native Slice parity: boundary, frozen membership, revision and cancellation."""

import unittest
import uuid

import pyarrow as pa

import graphforge


def identity() -> str:
    return str(uuid.uuid4())


def encode(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


class SliceTests(unittest.TestCase):
    def test_native_slice_freeze_boundary_and_cancellation(self) -> None:
        graph = graphforge.GraphForge()
        graph.execute(
            "CREATE (a:Story {name: 'First'}), (b:Story {name: 'Second'}), "
            "(c:Character {name: 'Shared'}), (a)-[:FEATURES]->(c), (b)-[:FEATURES]->(c)"
        )
        request = {
            "request_uuid": identity(),
            "source": {"kind": "current"},
            "selector": {
                "kind": "filter",
                "label": "Character",
                "property": "name",
                "equals": "Shared",
            },
        }
        self.assertEqual(graph.preview_slice(request).num_rows, 1)
        self.assertEqual(graph.preview_slice(request, "boundary").num_rows, 4)
        version = identity()
        prepared = graph.prepare_research_version(
            {
                "operation_uuid": identity(),
                "version_uuid": version,
                "context_uuid": identity(),
                "label": None,
                "description": None,
                "created_at": 1,
                "required_versions": [],
            }
        )
        graph.commit_research_version_operation(prepared)
        request["source"] = {"kind": "version", "version_uuid": version}
        frozen = graph.freeze_slice(request)
        capsule = encode(frozen)
        graph.execute("CREATE (:Character {name: 'Later'})")
        self.assertEqual(graph.inspect_frozen_slice(capsule).num_rows, 1)
        old = graph.inspect_frozen_slice(capsule).column("object_uuid")[0].as_py()
        revised = graph.revise_frozen_slice(
            capsule,
            {"request_uuid": identity(), "exclude": {"nodes": [old]}, "source_version": None},
        )
        self.assertEqual(graph.inspect_frozen_slice(encode(revised)).num_rows, 0)
        token = graphforge.CancellationToken()
        token.cancel()
        with self.assertRaises(Exception) as cancelled:
            graph.preview_slice(request, cancellation=token)
        self.assertEqual(getattr(cancelled.exception, "code", None), "GF_CANCELLED")
        with self.assertRaises(Exception) as invalid:
            graph.freeze_slice({"private_sentinel": "private-secret-value"})
        self.assertNotIn("private_sentinel", str(invalid.exception))
        self.assertNotIn("private-secret-value", str(invalid.exception))
        graph.close()


def main() -> None:
    """Execute native Slice acceptance."""
    unittest.main()


if __name__ == "__main__":
    main()
