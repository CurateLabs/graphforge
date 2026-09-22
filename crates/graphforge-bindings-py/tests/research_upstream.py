"""Native upstream Arrow review, selective baseline publication and receipt parity."""

import unittest
from uuid import UUID, uuid4

import graphforge


class ResearchUpstreamTests(unittest.TestCase):
    def test_selected_baseline_and_permanent_receipt(self):
        graph = graphforge.GraphForge()
        self.addCleanup(graph.close)

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
                "label": "Upstream review",
            }
        )
        graph.execute("MATCH (n:Item) SET n.x=1,n.y=2")
        preview_request = {"branch_uuid": branch, "scope": {"kind": "branch"}}
        before = generation()
        preview = graph.preview_research_upstream(preview_request)
        self.assertEqual(generation(), before)
        row = next(row for row in preview.to_pylist() if row["field"] == "property:x")
        update = {
            "operation_uuid": identity(),
            "expected_generation_uuid": before,
            "version_uuid": identity(),
            "preview": preview_request,
            "preview_sha256": list(
                bytes.fromhex(
                    preview.schema.metadata[b"graphforge.upstream.preview_sha256"].decode()
                )
            ),
            "selection": {
                "kind": "selected",
                "decisions": [
                    {
                        "unit": {
                            "object_kind": row["object_kind"],
                            "object_uuid": str(UUID(bytes=row["object_uuid"])),
                            "field": row["field"],
                        },
                        "resolution": {"kind": "keep_local"},
                    }
                ],
            },
            "acknowledge_evidence": [],
            "actor_uuid": identity(),
            "created_at": 2,
            "explanation": "Retain reviewed local interpretation",
        }
        receipt = graph.update_research_branch(update)
        after = graph.preview_research_upstream(preview_request)
        self.assertEqual(
            {
                row["field"]: row["change"]
                for row in after.to_pylist()
                if row["field"].startswith("property:")
            },
            {"property:x": "local", "property:y": "upstream"},
        )
        self.assertEqual(graph.update_research_branch(update), receipt)
        history = graph.research_upstream_history({"branch_uuid": branch, "page_size": 10})
        self.assertEqual(history.num_rows, 1)
        self.assertEqual(history["operation_uuid"][0].as_py(), update["operation_uuid"])
        with self.assertRaises(Exception) as error:
            graph.update_research_branch({**update, "explanation": "changed request"})
        self.assertEqual(error.exception.code, "GF_IDEMPOTENCY_CONFLICT")
        token = graphforge.CancellationToken()
        token.cancel()
        with self.assertRaises(Exception) as error:
            graph.preview_research_upstream(preview_request, cancellation=token)
        self.assertEqual(error.exception.code, "GF_CANCELLED")


def main():
    result = unittest.TextTestRunner().run(
        unittest.defaultTestLoader.loadTestsFromTestCase(ResearchUpstreamTests)
    )
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
