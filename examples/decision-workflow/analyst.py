"""Offline analyst routing and ranking using the real GraphForge Python binding."""

import hashlib
import json
import uuid

import graphforge


def uuid_text(value: bytes) -> str:
    return str(uuid.UUID(bytes=value))


def digest(value: bytes) -> list[int]:
    return list(hashlib.sha256(value).digest())


def main() -> None:
    graph = graphforge.GraphForge()
    try:
        graph.execute(
            "CREATE (:Candidate {title: 'Mystery', evidence: 'A locked room'}), "
            "(:Candidate {title: 'Voyage', evidence: 'A sea crossing'})"
        )
        rows = graph.execute(
            "MATCH (c:Candidate) RETURN c.node_uuid AS item_uuid, c.title AS title, "
            "c.evidence AS evidence ORDER BY c.node_uuid LIMIT 20"
        ).to_pylist()
        items = [uuid_text(row["item_uuid"]) for row in rows]
        projection = [
            {"item_uuid": item, "title": row["title"], "evidence": row["evidence"]}
            for item, row in zip(items, rows, strict=True)
        ]
        selected_bytes = json.dumps(items, separators=(",", ":")).encode()
        projection_bytes = json.dumps(projection, sort_keys=True, separators=(",", ":")).encode()
        route_question, rank_question = str(uuid.uuid4()), str(uuid.uuid4())
        producer = [
            {
                "question_uuid": route_question,
                "item_uuid": items[0],
                "status": "answered",
                "value": {"kind": "choice", "value": "research"},
            },
            {
                "question_uuid": route_question,
                "item_uuid": items[1],
                "status": "uncertain",
                "value": {"kind": "choice", "value": "human_review"},
                "confidence": {
                    "value": 0.55,
                    "minimum": 0.0,
                    "maximum": 1.0,
                    "domain": "unit_interval",
                    "meaning": "fixture estimate of routing correctness",
                },
            },
            {
                "question_uuid": rank_question,
                "item_uuid": items[0],
                "status": "answered",
                "value": {"kind": "rubric_score", "value": "high"},
            },
            {
                "question_uuid": rank_question,
                "item_uuid": items[1],
                "status": "answered",
                "value": {"kind": "rubric_score", "value": "medium"},
            },
        ]
        batch: graphforge.DecisionBatchV1 = {
            "input": {
                "generation_uuid": graph.research_project_summary()["identity"]["generation_uuid"],
                "version_uuid": None,
                "projection_sha256": digest(projection_bytes),
                "selection_sha256": digest(selected_bytes),
                "selected_item_uuids": items,
            },
            "producer": {"name": "offline analyst fixture", "model": "fixture-v1"},
            "questions": [
                {
                    "question_uuid": route_question,
                    "text": "Where should this candidate go?",
                    "item_uuids": items,
                    "kind": {
                        "kind": "choice",
                        "allowed_choices": ["research", "human_review"],
                    },
                },
                {
                    "question_uuid": rank_question,
                    "text": "How relevant is this candidate?",
                    "item_uuids": items,
                    "kind": {
                        "kind": "rubric_score",
                        "ordered_levels": ["low", "medium", "high"],
                    },
                },
            ],
            "results": producer,
        }
        table = graph.validate_decision_batch(batch)
        title_by_item = {
            uuid.UUID(item).bytes: value["title"] for item, value in zip(items, rows, strict=True)
        }
        for result in table.to_pylist():
            print(
                title_by_item[result["item_uuid"]],
                result["question_text"],
                result["status"],
                result["choice_value"] or result["rubric_score"],
                result["confidence_meaning"],
            )

        # Explicit caller policy stages only the clear route. Preserve this exact
        # request and operation UUID for retry; MERGE is idempotent on that key.
        clear_route = next(
            row
            for row in table.to_pylist()
            if row["question_uuid"] == uuid.UUID(route_question).bytes
            and row["item_uuid"] == uuid.UUID(items[0]).bytes
        )
        prepared = {
            "operation_uuid": str(uuid.uuid4()),
            "expected_generation_uuid": batch["input"]["generation_uuid"],
            "item_uuid": items[0],
            "destination": "research",
        }
        receipt_query = (
            "MATCH (r:DecisionRoute {operation_uuid: $operation_uuid}) "
            "RETURN r.item_uuid AS item_uuid, r.destination AS destination"
        )
        existing = graph.execute(receipt_query, prepared).to_pylist()
        current = graph.research_project_summary()["identity"]["generation_uuid"]
        if existing:
            receipt = existing[0]
            if (
                receipt["item_uuid"] != prepared["item_uuid"]
                or receipt["destination"] != prepared["destination"]
            ):
                raise ValueError("operation UUID already has a different action receipt")
            print("exact action retry:", receipt)
        elif (
            current == prepared["expected_generation_uuid"] and clear_route["status"] == "answered"
        ):
            graph.execute(
                "MERGE (r:DecisionRoute {operation_uuid: $operation_uuid}) "
                "SET r.item_uuid = $item_uuid, r.destination = $destination",
                prepared,
            )
            print("routed clear candidate", items[0])
        else:
            print("state changed or result uncertain; request human review")
    finally:
        graph.close()


if __name__ == "__main__":
    main()
