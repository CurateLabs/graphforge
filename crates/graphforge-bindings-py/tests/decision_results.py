"""Real Python facade coverage for provider-neutral decision validation."""

import unittest
import uuid

import graphforge


def request(value: str = "research") -> dict:
    question = str(uuid.uuid4())
    item = str(uuid.uuid4())
    return {
        "input": {
            "generation_uuid": str(uuid.uuid4()),
            "version_uuid": None,
            "projection_sha256": [7] * 32,
            "selection_sha256": [9] * 32,
            "selected_item_uuids": [item],
        },
        "producer": {"name": "offline fixture"},
        "questions": [
            {
                "question_uuid": question,
                "text": "Where should this item go?",
                "item_uuids": [item],
                "kind": {
                    "kind": "choice",
                    "allowed_choices": ["research", "human_review"],
                },
            }
        ],
        "results": [
            {
                "question_uuid": question,
                "item_uuid": item,
                "status": "answered",
                "value": {"kind": "choice", "value": value},
            }
        ],
    }


class DecisionResultsTests(unittest.TestCase):
    def test_validates_external_values_and_returns_arrow(self) -> None:
        graph = graphforge.GraphForge()
        try:
            batch = request()
            before = graph.research_project_summary()["identity"]["generation_uuid"]
            table = graph.validate_decision_batch(batch)
            after = graph.research_project_summary()["identity"]["generation_uuid"]
            self.assertEqual(table.num_rows, 1)
            self.assertEqual(before, after)
            self.assertEqual(
                str(uuid.UUID(bytes=table["generation_uuid"][0].as_py())),
                batch["input"]["generation_uuid"],
            )
            self.assertEqual(table["question_kind"][0].as_py(), "choice")
            self.assertEqual(table["choice_value"][0].as_py(), "research")
            self.assertEqual(
                str(uuid.UUID(bytes=table["item_uuid"][0].as_py())),
                batch["input"]["selected_item_uuids"][0],
            )
        finally:
            graph.close()

    def test_rejects_values_outside_callers_choice_set(self) -> None:
        graph = graphforge.GraphForge()
        try:
            with self.assertRaises(graphforge.ValidationError):
                graph.validate_decision_batch(request("write"))
        finally:
            graph.close()

    def test_accepts_yes_no_values_with_nested_probabilities(self) -> None:
        graph = graphforge.GraphForge()
        try:
            batch = request()
            batch["questions"][0]["item_uuids"] = []
            batch["questions"][0]["kind"] = {"kind": "yes_no_probability"}
            batch["results"][0]["item_uuid"] = None
            batch["results"][0]["value"] = {
                "kind": "yes_no_probability",
                "value": {"yes_probability": 0.82, "no_probability": 0.18},
            }
            table = graph.validate_decision_batch(batch)
            self.assertEqual(table["yes_probability"][0].as_py(), 0.82)
            self.assertEqual(table["no_probability"][0].as_py(), 0.18)
        finally:
            graph.close()


def main() -> None:
    unittest.main()


if __name__ == "__main__":
    main()
