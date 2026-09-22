"""Real native contextual claims, authority and durable Branch isolation."""

from pathlib import Path
import tempfile
import unittest
from uuid import UUID, uuid4

import graphforge


def identity():
    value = list(str(uuid4()))
    value[14] = "7"
    return "".join(value)


class ResearchClaimsTests(unittest.TestCase):
    def test_contextual_claims_survive_reopen_without_changing_raw_graph(self):
        with tempfile.TemporaryDirectory() as directory:
            root = str(Path(directory) / "project")
            graph = graphforge.GraphForge(root)
            graph.execute("CREATE (:Person {name:'Ada'})")
            for capability in ["provenance", "knowledge", "epistemic"]:
                graph.enable_capability(
                    operation_uuid=identity(), capability_id=capability, capability_version=1
                )
            node = str(
                UUID(bytes=graph.execute("MATCH (n) RETURN n.node_uuid AS id")["id"][0].as_py())
            )

            def generation():
                return graph.research_project_summary()["identity"]["generation_uuid"]

            def claim(text):
                request = {
                    "operation_uuid": identity(),
                    "expected_generation_uuid": generation(),
                    "assertion_uuid": identity(),
                    "claim": text,
                    "graph_refs": [
                        {"graph_uuid": node, "graph_kind": "node", "role": "subject", "ordinal": 0}
                    ],
                    "category": "interpretation",
                    "creator_uuid": identity(),
                    "run_uuid": None,
                    "created_at": 1,
                }
                result = graph.create_research_claim(request)
                self.assertEqual(result.num_rows, 1)
                self.assertTrue(result.equals(graph.create_research_claim(request)))
                return request["assertion_uuid"]

            first, second = claim("Original interpretation"), claim("Alternative interpretation")
            provenance = str(UUID(bytes=graph.assertion(first)["provenance_uuid"][0].as_py()))
            graph.relate_research_claims(
                {
                    "operation_uuid": identity(),
                    "expected_generation_uuid": generation(),
                    "relation": {
                        "relation_uuid": identity(),
                        "source_assertion_uuid": second,
                        "target_assertion_uuid": first,
                        "kind": "alternative_to",
                        "creator_uuid": identity(),
                        "provenance_uuid": provenance,
                        "recorded_at": 2,
                    },
                }
            )
            project = {"kind": "project"}
            authority = {"context": project, "community_uuid": None}
            view = dict(**authority, include_suppressed=False)
            self.assertEqual(
                graph.inspect_research_claims(view)["canonical"].to_pylist(), [False, False]
            )
            decisions = dict(
                operation_uuid=identity(),
                expected_generation_uuid=generation(),
                **authority,
                creator_uuid=identity(),
                recorded_at=3,
                decisions=[
                    {
                        "decision_uuid": identity(),
                        "subject_kind": "assertion",
                        "subject_uuid": first,
                        "kind": "promote",
                        "source_version_uuid": None,
                    }
                ],
            )
            graph.record_research_decisions(decisions)
            self.assertEqual(graph.research_canonical_choices(authority).num_rows, 1)
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
                    "created_at": 4,
                    "label": "Alternative",
                }
            )
            branch_context = {"kind": "branch", "branch_uuid": branch}
            change = {
                "operation_uuid": identity(),
                "expected_generation_uuid": generation(),
                "branch_uuid": branch,
                "version_uuid": identity(),
                "creator_uuid": identity(),
                "created_at": 5,
                "change": {
                    "kind": "suppress",
                    "suppression_uuid": identity(),
                    "assertion_uuid": first,
                    "provenance_uuid": provenance,
                },
            }
            token = graphforge.CancellationToken()
            token.cancel()
            with self.assertRaises(Exception) as error:
                graph.change_research_branch_claim(change, cancellation=token)
            self.assertEqual(error.exception.code, "GF_CANCELLED")
            receipt = graph.change_research_branch_claim(change)
            self.assertEqual(
                graph.inspect_research_claims(
                    {"context": branch_context, "community_uuid": None, "include_suppressed": False}
                ).num_rows,
                1,
            )
            self.assertEqual(graph.inspect_research_claims(view).num_rows, 2)
            self.assertEqual(
                graph.research_claim_history(
                    {"context": branch_context, "family": "suppressions", "assertion_uuid": first}
                ).num_rows,
                1,
            )
            self.assertEqual(
                graph.research_claim_history(
                    {"context": project, "family": "relations", "assertion_uuid": first}
                ).num_rows,
                1,
            )
            self.assertEqual(graph.execute("MATCH (n) RETURN count(n) AS n")["n"][0].as_py(), 1)
            graph.close()
            graph = graphforge.GraphForge(root)
            self.assertEqual(graph.change_research_branch_claim(change), receipt)
            self.assertEqual(graph.research_decision_history(authority).num_rows, 1)
            self.assertEqual(
                graph.research_canonical_choices(
                    {"context": branch_context, "community_uuid": None}
                ).num_rows,
                0,
            )
            graph.close()


def main():
    result = unittest.TextTestRunner().run(
        unittest.defaultTestLoader.loadTestsFromTestCase(ResearchClaimsTests)
    )
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
