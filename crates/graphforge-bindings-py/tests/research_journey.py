"""Real two-story research journey through the thin Python surface."""

from itertools import count
import json
from pathlib import Path
import tempfile
import unittest
from uuid import UUID

import pyarrow as pa

import graphforge

CORPUS = json.loads(
    (
        Path(__file__).resolve().parents[3] / "tests/fixtures/analyst-journey-v1/corpus.json"
    ).read_text()
)


COUNTER = count(1)


def identity():
    return f"018f0f4e-7b8c-7000-8000-{next(COUNTER):012x}"


def uuid_text(value):
    return value if isinstance(value, str) else str(UUID(bytes=bytes(value)))


def encode(table):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return list(sink.getvalue().to_pybytes())


class Journey:
    def __init__(self, root):
        self.root = root
        self.graph = graphforge.GraphForge(str(root / "project"))
        self.actor = identity()
        self.graph.execute(CORPUS["initial_graph"])
        self.ids = {
            row["name"]: uuid_text(row["id"])
            for row in self.graph.execute(
                "MATCH(n) RETURN n.name AS name,n.node_uuid AS id"
            ).to_pylist()
        }
        metadata = self.graph.research_project_metadata()
        metadata["title"] = CORPUS["title"]
        self.graph.update_research_metadata(metadata, operation_uuid=identity())
        before = self.current()
        discovered = graphforge.GraphForge.discover_research_projects(
            project_roots=[str(root / "project")]
        )
        assert discovered["title"].to_pylist() == [CORPUS["title"]]
        assert self.current() == before

    def current(self):
        return self.graph.research_project_summary()["identity"]["generation_uuid"]

    def evidence(self):
        for capability in ["provenance", "knowledge", "epistemic"]:
            self.graph.enable_capability(
                operation_uuid=identity(), capability_id=capability, capability_version=1
            )
        self.source, self.scan, self.ocr, self.claim = [identity() for _ in range(4)]
        self.graph.register_source(
            operation_uuid=identity(),
            source_uuid=self.source,
            label=CORPUS["source_label"],
            source_kind="manuscript",
        )
        for artifact, kind, payload, inputs in [
            (self.scan, "raw_scan", CORPUS["scan_bytes"], []),
            (
                self.ocr,
                "ocr_text",
                CORPUS["ocr_text"],
                [{"input_uuid": self.scan, "input_kind": "artifact"}],
            ),
        ]:
            self.graph.register_artifact(
                operation_uuid=identity(),
                artifact_uuid=artifact,
                source_uuid=self.source,
                artifact_kind=kind,
                media_type="text/plain",
                payload={"kind": "local_bytes", "bytes": payload.encode()},
                derivation_inputs=inputs,
            )
        self.graph.set_preferred_artifact(
            operation_uuid=identity(),
            preference_event_uuid=identity(),
            source_uuid=self.source,
            artifact_uuid=self.ocr,
            reason="Selected transcription",
        )
        self.graph.create_research_claim(
            {
                "operation_uuid": identity(),
                "expected_generation_uuid": self.current(),
                "assertion_uuid": self.claim,
                "claim": CORPUS["claim"],
                "graph_refs": [
                    {
                        "graph_uuid": self.ids["Ada"],
                        "graph_kind": "node",
                        "role": "subject",
                        "ordinal": 0,
                    }
                ],
                "category": "analyst_assertion",
                "creator_uuid": self.actor,
                "run_uuid": None,
                "created_at": 1,
            }
        )
        self.graph.attach_evidence(
            operation_uuid=identity(),
            evidence_uuid=identity(),
            assertion_uuid=self.claim,
            source_uuid=self.ocr,
            source_kind="artifact",
            role="supports",
        )
        lineage = self.graph.research_lineage(
            subject_uuid=self.ocr, subject_kind="artifact", direction="backward", max_depth=4
        )
        assert self.scan in [uuid_text(v) for v in lineage["input_uuid"].to_pylist()]

    def capture(self):
        version = identity()
        prepared = self.graph.prepare_research_version(
            {
                "operation_uuid": identity(),
                "version_uuid": version,
                "context_uuid": identity(),
                "label": None,
                "description": None,
                "created_at": 2,
                "required_versions": [],
            }
        )
        self.graph.commit_research_version_operation(prepared)
        return version

    def branches(self):
        source = self.capture()
        self.a, self.b = identity(), identity()
        for branch, story, outside in [
            (self.a, "Mystery", "Voyage"),
            (self.b, "Voyage", "Mystery"),
        ]:
            request = {
                "request_uuid": identity(),
                "source": {"kind": "version", "version_uuid": source},
                "selector": {
                    "kind": "traverse",
                    "seeds": [self.ids[story]],
                    "direction": "both",
                    "max_depth": 1,
                    "relationship_types": [],
                },
                "include": {"assertions": [self.claim]},
            }
            before = self.current()
            included = self.graph.preview_slice(request).to_pylist()
            assert {row["object_uuid"] for row in included if row["object_kind"] == "node"} == {
                self.ids[story],
                self.ids["Ada"],
            }
            boundary = self.graph.preview_slice(request, "boundary").to_pylist()
            assert self.ids[outside] in [row["object_uuid"] for row in boundary]
            explanations = self.graph.preview_slice(request, "explanations")
            assert explanations.num_rows == len(included)
            why = next(r for r in explanations.to_pylist() if r["object_uuid"] == self.ids["Ada"])
            assert (why["reason"], why["root_uuid"], why["predecessor_uuid"], why["depth"]) == (
                "traversal",
                self.ids[story],
                self.ids[story],
                1,
            )
            assert why["via_edge_uuid"]
            dependencies = self.graph.preview_slice(request, "dependencies").to_pylist()
            assert {self.source, self.scan, self.ocr} <= {
                row["object_uuid"] for row in dependencies
            }
            frozen = encode(self.graph.freeze_slice(request))
            assert self.current() == before
            self.graph.create_research_branch(
                {
                    "operation_uuid": identity(),
                    "expected_generation_uuid": before,
                    "branch_uuid": branch,
                    "version_uuid": identity(),
                    "source": {"kind": "slice", "frozen_ipc": frozen},
                    "creator_uuid": self.actor,
                    "created_at": 3,
                    "label": story,
                }
            )
        analysis = self.graph.query_research_branch(
            self.a,
            "MATCH(s:Story)-[:FEATURES]->(c:Character) RETURN s.name AS story,c.name AS character",
        )
        assert analysis.to_pylist() == [{"story": "Mystery", "character": "Ada"}]

    def edit(self, branch, query):
        version = identity()
        self.graph.execute_research_branch(
            {
                "operation_uuid": identity(),
                "expected_generation_uuid": self.current(),
                "branch_uuid": branch,
                "version_uuid": version,
                "created_at": 4,
                "query": query,
            }
        )
        return version

    def upstream(self):
        self.edit(self.a, CORPUS["local_edit"])
        assert self.graph.execute("MATCH(n:Character) RETURN n.score").column(0)[0].as_py() == 0
        self.graph.execute(CORPUS["upstream_edit"])
        request = {"branch_uuid": self.a, "scope": {"kind": "branch"}}
        before = self.current()
        preview = self.graph.preview_research_upstream(request)
        assert self.current() == before
        row = next(row for row in preview.to_pylist() if row["field"] == "property:x")
        version = identity()
        self.graph.update_research_branch(
            {
                "operation_uuid": identity(),
                "expected_generation_uuid": before,
                "version_uuid": version,
                "preview": request,
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
                                "object_uuid": uuid_text(row["object_uuid"]),
                                "field": row["field"],
                            },
                            "resolution": {"kind": "adopt_upstream"},
                        }
                    ],
                },
                "acknowledge_evidence": [],
                "actor_uuid": self.actor,
                "created_at": 5,
                "explanation": "Only reviewed x",
            }
        )
        assert self.graph.query_research_branch(
            self.a,
            "MATCH(n:Character) RETURN n.score AS score,n.ending AS ending,n.x AS x,n.y AS y",
        ).to_pylist() == [CORPUS["expected_branch_after_upstream"]]
        return version

    def node_capsule(self, version):
        return encode(
            self.graph.freeze_slice(
                {
                    "request_uuid": identity(),
                    "source": {"kind": "version", "version_uuid": version},
                    "selector": {"kind": "direct", "members": {"nodes": [self.ids["Ada"]]}},
                }
            )
        )

    def proposal(self, version, fields):
        proposal = identity()
        self.graph.submit_research_proposal(
            {
                "operation_uuid": identity(),
                "expected_generation_uuid": self.current(),
                "proposal_uuid": proposal,
                "source_branch_uuid": self.a,
                "source_version_uuid": version,
                "frozen_ipc": self.node_capsule(version),
                "fields": [
                    {"object_kind": "node", "object_uuid": self.ids["Ada"], "field": field}
                    for field in fields
                ],
                "actor_uuid": self.actor,
                "created_at": 6,
                "motivation": "Selected public observations",
                "policy": "",
            }
        )
        before = self.current()
        rows = self.graph.preview_research_proposal({"proposal_uuid": proposal}).to_pylist()
        assert self.current() == before
        assert {r["field"] for r in rows} == set(fields)
        assert "PRIVATE_LOCAL" not in repr(rows) and "private_note" not in repr(rows)
        return {
            "operation_uuid": identity(),
            "expected_generation_uuid": before,
            "proposal_uuid": proposal,
            "preview_sha256": list(bytes.fromhex(rows[0]["preview_sha256"])),
            "decisions": {
                r["item_uuid"]: "defer" if r["field"] == "property:ending" else "accept"
                for r in rows
            },
            "resolve_conflicts": [],
            "acknowledge_evidence": [],
            "promotions": [],
            "community_uuid": None,
            "actor_uuid": self.actor,
            "created_at": 7,
            "explanation": "Accept score, defer ending",
            "policy": "",
        }

    def acceptance(self, version):
        comparison = {
            "left": {"kind": "branch", "branch_uuid": self.a},
            "right": {"kind": "project"},
            "left_authority": None,
            "right_authority": None,
            "detail": "changes",
            "accepted": [],
            "max_fields": 40000,
            "max_bytes": 67108864,
            "page_size": 1000,
            "after": None,
        }
        before = self.current()
        changes = self.graph.compare_research(comparison)
        assert {"property:score", "property:ending"} <= set(changes["field"].to_pylist())
        assert self.current() == before
        review = self.proposal(version, CORPUS["selected_fields"])
        receipt = self.graph.review_research_proposal(review)
        assert self.graph.review_research_proposal(review) == receipt
        assert self.graph.execute(
            "MATCH(n:Character) RETURN n.score AS score,n.ending AS ending,"
            "n.private_note AS private_note"
        ).to_pylist() == [CORPUS["expected_parent_after_acceptance"]]
        assert (
            self.graph.research_canonical_choices(
                {"context": {"kind": "project"}, "community_uuid": None}
            ).num_rows
            == 0
        )
        assert (
            self.graph.research_proposal_history(
                {"proposal_uuid": review["proposal_uuid"], "detail": "accepted", "page_size": 100}
            ).num_rows
            == 1
        )
        return review, receipt

    def restore(self, version, review, receipt):
        target = {"kind": "version", "version_uuid": version}
        citation = self.graph.research_reference(target)
        continued = self.edit(self.a, "MATCH(n:Character) SET n.score=2")
        assert (
            self.graph.research_reference({"kind": "branch", "branch_uuid": self.a})["version"][
                "version_uuid"
            ]
            == continued
        )
        assert self.graph.research_reference(target)["version"] == citation["version"]
        assert (
            self.graph.query_research_version(version, "MATCH(n:Character) RETURN n.score")
            .column(0)[0]
            .as_py()
            == 1
        )
        self.edit(self.b, "MATCH(n:Character) SET n.score=73")
        self.graph.execute("MATCH(n:Character) SET n.score=99")
        restored = identity()
        self.graph.restore_research_branch(
            {
                "operation_uuid": identity(),
                "expected_generation_uuid": self.current(),
                "branch_uuid": self.a,
                "source_version_uuid": version,
                "version_uuid": restored,
                "created_at": 8,
            }
        )
        assert self.graph.review_research_proposal(review) == receipt
        again = self.graph.review_research_proposal(self.proposal(restored, ["property:score"]))
        assert again["version_uuid"] is None
        for branch, expected in [(self.a, 1), (self.b, 73)]:
            assert (
                self.graph.query_research_branch(branch, "MATCH(n:Character) RETURN n.score")
                .column(0)[0]
                .as_py()
                == expected
            )
        assert self.graph.execute("MATCH(n:Character) RETURN n.score").column(0)[0].as_py() == 99
        return restored

    def roundtrip(self, version, projection=None, name="complete"):
        package, target = self.root / name, self.root / (name + "-import")
        exported = self.graph.export_research(
            {
                "version_uuid": version,
                "output": str(package),
                "bundled": False,
                "projection": projection,
            }
        )
        verified = graphforge.GraphForge.verify_portable_v2(str(package), mode="full")
        assert verified["package_digest"] == exported["package_digest"]
        request = {"input": str(package), "operation_id": identity()}
        first = graphforge.GraphForge.import_portable_v2(str(target), **request)
        replay = graphforge.GraphForge.import_portable_v2(str(target), **request)
        assert replay["idempotent_replay"] and first["generation_uuid"] == replay["generation_uuid"]
        return graphforge.GraphForge(str(target))

    def interchange(self, version):
        citation = self.graph.research_reference({"kind": "version", "version_uuid": version})
        complete = self.roundtrip(version)
        try:
            assert complete.research_version(version) == citation["version"]
            assert (
                complete.research_version_artifact_payload(version, self.ocr).column(0)[0].as_py()
                == CORPUS["ocr_text"].encode()
            )
            assert complete.research_version_ontology(
                version
            ) == self.graph.research_version_ontology(version)
        finally:
            complete.close()
        projected = identity()
        projection = {
            "version_uuid": projected,
            "frozen_ipc": self.node_capsule(version),
            "fields": [
                {"object_kind": "node", "object_uuid": self.ids["Ada"], "field": field}
                for field in ["$object", "$labels", "property:score"]
            ],
            "created_at": 9,
        }
        selected = self.roundtrip(version, projection, "selected")
        try:
            ref = selected.research_reference({"kind": "version", "version_uuid": projected})
            assert ref["version"]["content"]["source_version"] == version
            result = selected.query_research_version(
                projected,
                "MATCH(n:Character) RETURN n.score AS score,n.private_note AS private_note",
            )
            assert result.to_pylist() == [{"score": 1, "private_note": None}]
        finally:
            selected.close()
        metadata = self.graph.research_project_metadata()
        metadata["title"] = CORPUS["fork_title"]
        metadata["access"]["access_policy"] = "Independent local governance"
        request = {
            "operation_uuid": identity(),
            "project_uuid": identity(),
            "version_uuid": version,
            "projection": None,
            "target": str(self.root / "fork"),
            "actor_uuid": self.actor,
            "governance": "Independent research",
            "adopt_selected_ontology": True,
            "metadata": metadata,
        }
        first = self.graph.fork_research(request)
        replay = self.graph.fork_research(request)
        assert replay["idempotent_replay"] and replay["generation_uuid"] == first["generation_uuid"]
        fork = graphforge.GraphForge(request["target"])
        try:
            ref = fork.research_reference({"kind": "version", "version_uuid": version})
            assert ref["project_uuid"] == request["project_uuid"] != citation["project_uuid"]
            assert ref["origin_project_uuid"] == citation["origin_project_uuid"]
            assert fork.research_project_metadata() == metadata
        finally:
            fork.close()


class ResearchJourneyTests(unittest.TestCase):
    def test_native_two_story_journey(self):
        with tempfile.TemporaryDirectory() as root:
            journey = Journey(Path(root))
            try:
                journey.evidence()
                journey.branches()
                version = journey.upstream()
                review, receipt = journey.acceptance(version)
                restored = journey.restore(version, review, receipt)
                journey.interchange(restored)
                # Run native compaction and cleanup before reopening the durable Project.
                journey.graph.commit_research_version_operation(
                    {
                        "operation_uuid": identity(),
                        "expected_generation_uuid": journey.current(),
                        "mutation": {"operation": "compact", "versions": [restored]},
                    }
                )
                cleanup = journey.graph.execute_project_cleanup(retained_ancestors=0)
                self.assertFalse(cleanup["dry_run"])
                self.assertEqual(cleanup["graph_object_sweep"]["disposition"], "completed")
                self.assertGreater(cleanup["removed"], 0)
                before = journey.current()
                token = graphforge.CancellationToken()
                token.cancel()
                with self.assertRaises(Exception) as error:
                    journey.graph.research_reference(
                        {"kind": "version", "version_uuid": restored}, cancellation=token
                    )
                self.assertEqual(error.exception.code, "GF_CANCELLED")
                self.assertEqual(journey.current(), before)
                journey.graph.close()
                journey.graph = graphforge.GraphForge(str(Path(root) / "project"))
                self.assertEqual(journey.graph.review_research_proposal(review), receipt)
                self.assertEqual(
                    journey.graph.research_version_artifact_payload(restored, journey.ocr)
                    .column(0)[0]
                    .as_py(),
                    CORPUS["ocr_text"].encode(),
                )
                for branch, expected in [(journey.a, 1), (journey.b, 73)]:
                    self.assertEqual(
                        journey.graph.query_research_branch(
                            branch, "MATCH(n:Character) RETURN n.score"
                        )
                        .column(0)[0]
                        .as_py(),
                        expected,
                    )
                self.assertEqual(
                    journey.graph.execute("MATCH(n:Character) RETURN n.score").column(0)[0].as_py(),
                    99,
                )
            finally:
                journey.graph.close()


def main():
    unittest.main()


if __name__ == "__main__":
    main()
