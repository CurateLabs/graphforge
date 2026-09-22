"""Native semantic comparison Arrow and cancellation parity."""

import unittest
from uuid import uuid4

import graphforge


class ResearchComparisonTests(unittest.TestCase):
    def test_native_local_upstream_and_stale_continuation(self):
        graph = graphforge.GraphForge()

        def identity():
            return str(uuid4())

        def generation():
            return graph.research_project_summary()["identity"]["generation_uuid"]

        graph.execute("CREATE (:Item {x:0,y:0})")
        branch = identity()
        graph.create_research_branch(
            {
                "operation_uuid": identity(),
                "expected_generation_uuid": generation(),
                "branch_uuid": branch,
                "version_uuid": identity(),
                "source": {
                    "kind": "current",
                    "origin_version_uuid": identity(),
                    "context_uuid": identity(),
                },
                "creator_uuid": identity(),
                "created_at": 1,
                "label": "Comparison",
            }
        )
        graph.execute_research_branch(
            {
                "operation_uuid": identity(),
                "expected_generation_uuid": generation(),
                "branch_uuid": branch,
                "version_uuid": identity(),
                "created_at": 2,
                "query": "MATCH (n:Item) SET n.x = 1",
            }
        )
        graph.execute("MATCH (n:Item) SET n.y = 2")
        request = {
            "left": {"kind": "branch", "branch_uuid": branch},
            "right": {"kind": "project"},
            "detail": "changes",
            "max_fields": 40000,
            "max_bytes": 67108864,
            "page_size": 1000,
        }
        before = generation()
        result = graph.compare_research(request)
        self.assertEqual(
            dict(zip(result["field"].to_pylist(), result["change"].to_pylist())),
            {"property:x": "local", "property:y": "upstream"},
        )
        self.assertEqual(generation(), before)
        request["page_size"] = 1
        page = graph.compare_research(request)
        request["after"] = page.schema.metadata[b"graphforge.comparison.next_cursor"].decode()
        graph.execute("MATCH (n:Item) SET n.y = 3")
        with self.assertRaises(Exception) as error:
            graph.compare_research(request)
        self.assertEqual(error.exception.code, "GF_PAGE_SNAPSHOT_GONE")
        request.pop("after")
        token = graphforge.CancellationToken()
        token.cancel()
        with self.assertRaises(Exception) as error:
            graph.compare_research(request, cancellation=token)
        self.assertEqual(error.exception.code, "GF_CANCELLED")
        graph.close()


def main():
    result = unittest.TextTestRunner().run(
        unittest.defaultTestLoader.loadTestsFromTestCase(ResearchComparisonTests)
    )
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
