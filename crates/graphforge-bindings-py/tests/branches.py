"""Native Branch isolation, durable replay and Arrow parity."""

from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

import graphforge


def identity():
    return str(uuid4())


class BranchTests(unittest.TestCase):
    def test_independent_branch_restore_replay_and_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            root = str(Path(directory) / "project")
            graph = graphforge.GraphForge(root)
            graph.execute("CREATE (:Character {name: 'Original'})")

            def generation():
                return graph.research_project_summary()["identity"]["generation_uuid"]

            branch, base = identity(), identity()
            create = {
                "operation_uuid": identity(),
                "expected_generation_uuid": generation(),
                "branch_uuid": branch,
                "version_uuid": base,
                "source": {
                    "kind": "current",
                    "origin_version_uuid": identity(),
                    "context_uuid": identity(),
                },
                "creator_uuid": identity(),
                "created_at": 1,
                "label": "Story",
            }
            receipt = graph.create_research_branch(create)
            self.assertEqual(graph.create_research_branch(create), receipt)
            selection = graph.research_branch_selection(branch)
            self.assertEqual(selection.num_rows, 1)
            edit = {
                "operation_uuid": identity(),
                "expected_generation_uuid": generation(),
                "branch_uuid": branch,
                "version_uuid": identity(),
                "created_at": 2,
                "query": "MATCH (n:Character) SET n.name = 'Local'",
            }
            graph.execute_research_branch(edit)
            self.assertEqual(
                graph.query_research_branch(branch, "MATCH (n) RETURN n.name AS name")["name"][
                    0
                ].as_py(),
                "Local",
            )
            self.assertEqual(
                graph.execute("MATCH (n) RETURN n.name AS name")["name"][0].as_py(), "Original"
            )
            self.assertTrue(selection.equals(graph.research_branch_selection(branch)))
            self.assertIn("local", graph.research_branch_fields(branch)["status"].to_pylist())
            token = graphforge.CancellationToken()
            token.cancel()
            restore = {
                "operation_uuid": identity(),
                "expected_generation_uuid": generation(),
                "branch_uuid": branch,
                "source_version_uuid": base,
                "version_uuid": identity(),
                "created_at": 3,
            }
            with self.assertRaises(Exception) as error:
                graph.restore_research_branch(restore, cancellation=token)
            self.assertEqual(error.exception.code, "GF_CANCELLED")
            restored = graph.restore_research_branch(restore)
            graph.close()
            graph = graphforge.GraphForge(root)
            self.assertEqual(graph.restore_research_branch(restore), restored)
            self.assertEqual(graph.research_branch(branch)["version_uuid"], restore["version_uuid"])
            self.assertEqual(
                graph.query_research_branch(branch, "MATCH (n) RETURN n.name AS name")["name"][
                    0
                ].as_py(),
                "Original",
            )
            self.assertEqual(graph.research_branch_references(branch).num_rows, 0)
            graph.close()


def main():
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(BranchTests)
    result = unittest.TextTestRunner().run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
