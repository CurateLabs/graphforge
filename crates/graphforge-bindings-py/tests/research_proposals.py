"""Real native selected proposal, atomic review, history and release parity."""

import unittest
from uuid import UUID, uuid4

import pyarrow as pa

import graphforge


class ResearchProposalTests(unittest.TestCase):
    def test_native_selective_proposal_review_and_replay(self):
        graph = graphforge.GraphForge()

        def identity():
            return str(uuid4())

        def generation():
            return graph.research_project_summary()["identity"]["generation_uuid"]

        graph.execute("CREATE (:Character {score:0, private_note:'private'})")
        node = str(
            UUID(
                bytes=graph.execute("MATCH (n:Character) RETURN n.node_uuid AS id")["id"][0].as_py()
            )
        )
        branch, version, proposal = identity(), identity(), identity()
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
                "label": "Story",
            }
        )
        graph.execute_research_branch(
            {
                "operation_uuid": identity(),
                "expected_generation_uuid": generation(),
                "branch_uuid": branch,
                "version_uuid": version,
                "created_at": 2,
                "query": "MATCH (n:Character) SET n.score=1",
            }
        )
        frozen = graph.freeze_slice(
            {
                "request_uuid": identity(),
                "source": {"kind": "version", "version_uuid": version},
                "selector": {"kind": "direct", "members": {"nodes": [node]}},
            }
        )
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, frozen.schema) as writer:
            writer.write_table(frozen)
        submit = {
            "operation_uuid": identity(),
            "expected_generation_uuid": generation(),
            "proposal_uuid": proposal,
            "source_branch_uuid": branch,
            "source_version_uuid": version,
            "frozen_ipc": list(sink.getvalue().to_pybytes()),
            "fields": [{"object_kind": "node", "object_uuid": node, "field": "property:score"}],
            "actor_uuid": identity(),
            "created_at": 3,
            "motivation": "Selected score",
            "policy": "",
        }
        graph.submit_research_proposal(submit)
        before = generation()
        preview = graph.preview_research_proposal({"proposal_uuid": proposal}).to_pylist()[0]
        self.assertEqual(generation(), before)
        review = {
            "operation_uuid": identity(),
            "expected_generation_uuid": preview["generation_uuid"],
            "proposal_uuid": proposal,
            "preview_sha256": list(bytes.fromhex(preview["preview_sha256"])),
            "decisions": {preview["item_uuid"]: "accept"},
            "resolve_conflicts": [],
            "acknowledge_evidence": [],
            "promotions": [],
            "community_uuid": None,
            "actor_uuid": identity(),
            "created_at": 4,
            "explanation": "Accept selected score",
            "policy": "",
        }
        receipt = graph.review_research_proposal(review)
        self.assertEqual(graph.review_research_proposal(review), receipt)
        self.assertEqual(
            graph.execute(
                "MATCH (n:Character) RETURN n.score AS score, n.private_note AS note"
            ).to_pylist(),
            [{"score": 1, "note": "private"}],
        )
        history = graph.research_proposal_history(
            {"proposal_uuid": proposal, "detail": "accepted", "page_size": 100}
        )
        self.assertEqual(history.num_rows, 1)
        graph.release_research_proposal(
            {
                "operation_uuid": identity(),
                "expected_generation_uuid": generation(),
                "proposal_uuid": proposal,
            }
        )
        self.assertEqual(graph.review_research_proposal(review), receipt)
        changed = dict(review, explanation="changed")
        with self.assertRaises(Exception) as error:
            graph.review_research_proposal(changed)
        self.assertEqual(error.exception.code, "GF_IDEMPOTENCY_CONFLICT")
        graph.close()


def main():
    result = unittest.TextTestRunner().run(
        unittest.defaultTestLoader.loadTestsFromTestCase(ResearchProposalTests)
    )
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
