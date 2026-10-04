"""Saved definitions remain native metadata; execution yields ordinary Arrow results."""

from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

from graphforge import CancellationToken, GraphForge, GraphForgeError


def definition() -> dict:
    return {
        "query_uuid": str(uuid4()),
        "name": "Items above threshold",
        "description": "A reusable aggregate",
        "query": "MATCH (n:Item) WHERE n.score >= $minimum RETURN count(n) AS total",
        "parameters": {"minimum": "integer"},
    }


def check_saved_queries() -> None:
    check = unittest.TestCase()
    with tempfile.TemporaryDirectory() as directory:
        root = str(Path(directory) / "project")
        graph = GraphForge(root)
        graph.execute("CREATE (:Item {score:1}), (:Item {score:3})")
        saved = definition()
        query_uuid = saved["query_uuid"]
        assert graph.create_saved_query(saved) == saved
        assert graph.saved_queries() == [saved]
        assert graph.saved_query(query_uuid) == saved
        assert graph.execute_saved_query(query_uuid, {"minimum": 2}).to_pylist() == [{"total": 1}]
        for params in [None, {"minimum": "2"}, {"minimum": 2, "extra": 1}]:
            with check.assertRaises(GraphForgeError):
                graph.execute_saved_query(query_uuid, params)
        with check.assertRaises(GraphForgeError):
            graph.create_saved_query(saved)
        with check.assertRaises(GraphForgeError):
            graph.update_saved_query(dict(saved, query_uuid=str(uuid4())))
        with check.assertRaises(GraphForgeError):
            graph.create_saved_query(
                dict(
                    saved,
                    query_uuid=str(uuid4()),
                    name="Mutation",
                    query="CREATE (:Item)",
                    parameters={},
                )
            )
        version_uuid = str(uuid4())
        prepared = graph.prepare_research_version(
            {
                "operation_uuid": str(uuid4()),
                "version_uuid": version_uuid,
                "context_uuid": str(uuid4()),
                "created_at": 1,
                "required_versions": [],
            }
        )
        graph.commit_research_version_operation(prepared)
        historical = {"kind": "version", "version_uuid": version_uuid}
        updated = dict(saved, name="Revised threshold")
        assert graph.update_saved_query(updated) == updated
        graph.execute("CREATE (:Item {score:4})")
        graph.close()
        graph = GraphForge(root)
        assert graph.saved_query(query_uuid) == updated
        assert graph.saved_query(query_uuid, source=historical) == saved
        assert graph.saved_queries(source=historical) == [saved]
        assert graph.execute_saved_query(query_uuid, {"minimum": 2}).to_pylist() == [{"total": 2}]
        assert graph.execute_saved_query(
            query_uuid, {"minimum": 2}, source=historical
        ).to_pylist() == [{"total": 1}]
        token = CancellationToken()
        token.cancel()
        with check.assertRaises(GraphForgeError):
            graph.execute_saved_query(query_uuid, {"minimum": 2}, cancellation=token)
        uuid_query = dict(
            saved,
            query_uuid=str(uuid4()),
            name="UUID parameter",
            query="RETURN $identity AS identity",
            parameters={"identity": "uuid"},
        )
        graph.create_saved_query(uuid_query)
        value = uuid4()
        result = graph.execute_saved_query(uuid_query["query_uuid"], {"identity": value})
        assert result.num_rows == 1
        assert result.column(0)[0].as_py() in [value.bytes, str(value)]
        graph.delete_saved_query(query_uuid)
        with check.assertRaises(GraphForgeError):
            graph.saved_query(query_uuid)
        assert graph.saved_query(query_uuid, source=historical) == saved
        graph.close()


if __name__ == "__main__":
    check_saved_queries()
