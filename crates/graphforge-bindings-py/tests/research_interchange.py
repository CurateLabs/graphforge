"""Native citation, research transport and explicit independent Fork parity."""

from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

import graphforge


def identity():
    return str(uuid4())


class ResearchInterchangeTests(unittest.TestCase):
    def test_native_reference_export_import_fork_and_cancellation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            graph = graphforge.GraphForge()
            self.addCleanup(graph.close)
            graph.execute("CREATE (:Item {score:7})")
            branch, version = identity(), identity()
            graph.create_research_branch(
                {
                    "operation_uuid": identity(),
                    "expected_generation_uuid": graph.research_project_summary()["identity"][
                        "generation_uuid"
                    ],
                    "branch_uuid": branch,
                    "version_uuid": version,
                    "source": {
                        "kind": "current",
                        "origin_version_uuid": identity(),
                        "context_uuid": identity(),
                    },
                    "creator_uuid": identity(),
                    "created_at": 1,
                    "label": "Portable research",
                }
            )
            target = {"kind": "version", "version_uuid": version}
            reference = graph.research_reference(target)
            live = graph.research_reference({"kind": "branch", "branch_uuid": branch})
            self.assertEqual(reference["version"], live["version"])
            self.assertEqual(reference["genealogy"][0]["branch_uuid"], branch)
            package = root / "package"
            export = {
                "version_uuid": version,
                "output": str(package),
                "bundled": False,
                "projection": None,
            }
            receipt = graph.export_research(export)
            verified = graphforge.GraphForge.verify_portable_v2(str(package), mode="full")
            self.assertEqual(verified["package_digest"], receipt["package_digest"])
            imported_root = root / "imported"
            graphforge.GraphForge.import_portable_v2(
                str(imported_root), input=str(package), operation_id=identity()
            )
            imported = graphforge.GraphForge(str(imported_root))
            try:
                after = imported.research_reference(target)
                self.assertEqual(after["version"], reference["version"])
                self.assertEqual(after["genealogy"], reference["genealogy"])
                self.assertEqual(
                    imported.execute("MATCH(n:Item) RETURN n.score").column(0)[0].as_py(), 7
                )
                with self.assertRaises(Exception) as unavailable:
                    imported.research_reference({"kind": "branch", "branch_uuid": branch})
                self.assertEqual(unavailable.exception.code, "GF_RESULT_NOT_RETAINED")
            finally:
                imported.close()
            metadata = graph.research_project_metadata()
            metadata["title"] = "Independent research"
            metadata["access"]["visibility"] = "private"
            metadata["access"]["access_policy"] = "Independent local review"
            fork = {
                "operation_uuid": identity(),
                "project_uuid": identity(),
                "version_uuid": version,
                "projection": None,
                "target": str(root / "fork"),
                "actor_uuid": identity(),
                "governance": "Independent review",
                "adopt_selected_ontology": True,
                "metadata": metadata,
            }
            first = graph.fork_research(fork)
            replay = graph.fork_research(fork)
            self.assertTrue(replay["idempotent_replay"])
            self.assertEqual(first["generation_uuid"], replay["generation_uuid"])
            destination = graphforge.GraphForge(fork["target"])
            try:
                self.assertEqual(destination.research_project_metadata(), metadata)
                citation = destination.research_reference(target)
                self.assertEqual(citation["project_uuid"], fork["project_uuid"])
                self.assertNotEqual(citation["project_uuid"], reference["project_uuid"])
                self.assertEqual(citation["version"], reference["version"])
                with self.assertRaises(Exception) as conflict:
                    graph.fork_research(dict(fork, governance="Changed policy"))
                self.assertEqual(conflict.exception.code, "GF_IDEMPOTENCY_CONFLICT")
            finally:
                destination.close()
            generation = graph.research_project_summary()["identity"]["generation_uuid"]
            token = graphforge.CancellationToken()
            token.cancel()
            cancelled_export = dict(export, output=str(root / "cancelled-export"))
            cancelled_fork = dict(
                fork, operation_uuid=identity(), target=str(root / "cancelled-fork")
            )
            for method, request in [
                (graph.research_reference, target),
                (graph.export_research, cancelled_export),
                (graph.fork_research, cancelled_fork),
            ]:
                with self.assertRaises(Exception) as cancelled:
                    method(request, cancellation=token)
                self.assertEqual(cancelled.exception.code, "GF_CANCELLED")
            self.assertFalse(Path(cancelled_export["output"]).exists())
            self.assertFalse(Path(cancelled_fork["target"]).exists())
            self.assertEqual(
                graph.research_project_summary()["identity"]["generation_uuid"], generation
            )

    def test_private_request_diagnostics_are_bounded(self):
        graph = graphforge.GraphForge()
        self.addCleanup(graph.close)
        sentinel = "PRIVATE_INTERCHANGE_SENTINEL"
        for method in [graph.research_reference, graph.export_research, graph.fork_research]:
            with self.assertRaises(Exception) as failure:
                method({sentinel: sentinel})
            self.assertNotIn(sentinel, str(failure.exception))
            self.assertLess(len(str(failure.exception)), 256)


def main():
    result = unittest.TextTestRunner().run(
        unittest.defaultTestLoader.loadTestsFromTestCase(ResearchInterchangeTests)
    )
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
