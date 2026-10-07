"""Load a committed fixture through `gf import-session`, then drive its queries.

`graphforge-benchmark-gdc-scorecard query` reopens the durable project,
reconciles per-label and per-type counts, and times each variant with the
declared driver clock. Binaries are built on demand or taken from
GRAPHFORGE_GDC_SCORECARD_BIN / GRAPHFORGE_GF_BIN, as in the load test.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from graphforge_bench.gdc_contracts import workspace_root
from graphforge_bench.gdc_measurement_policy import (
    GdcMeasurementBoundaryError,
    assert_query_latency_authority,
)

from tests.test_gdc_scorecard_load import FIXTURE, Gf, converter_binary, gf_binary

QUERY_FIXTURE = workspace_root() / "fixtures" / "gdc" / "query-fixture"


class ScorecardQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.driver = converter_binary()
        gf = gf_binary()
        scratch = tempfile.TemporaryDirectory()
        cls.addClassCleanup(scratch.cleanup)
        cls.scratch = Path(scratch.name)
        converted = cls.scratch / "converted"
        completed = subprocess.run(
            [
                str(cls.driver),
                "convert",
                "--mapping",
                str(FIXTURE / "mapping.json"),
                "--input-root",
                str(FIXTURE),
                "--output-dir",
                str(converted),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, completed.stderr
        cls.project = cls.scratch / "project"
        committed = Gf(gf, cls.project).load(converted)
        assert committed["outcome"] == "committed", committed

    def query(self, expected: Path, output: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                str(self.driver),
                "query",
                "--project",
                str(self.project),
                "--workload",
                str(QUERY_FIXTURE / "workload.json"),
                "--expected-counts",
                str(expected),
                "--output",
                str(output),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_imported_project_reconciles_and_every_binding_is_measured(self) -> None:
        output = self.scratch / "evidence.json"
        completed = self.query(QUERY_FIXTURE / "expected-counts.json", output)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        evidence = json.loads(output.read_text(encoding="utf-8"))
        assert_query_latency_authority(evidence)

        reconciliation = evidence["reconciliation"]
        self.assertEqual(reconciliation["nodes"], {"expected": 11, "observed": 11})
        self.assertEqual(reconciliation["edges"], {"expected": 7, "observed": 7})
        self.assertEqual(
            {label: pair["observed"] for label, pair in reconciliation["labels"].items()},
            {"Organisation": 1, "Person": 3, "Place": 2, "Vertex": 5},
        )
        self.assertEqual(
            {name: pair["observed"] for name, pair in reconciliation["types"].items()},
            {"KNOWS": 3, "LINK": 4},
        )
        for name, path in (
            ("workload_sha256", QUERY_FIXTURE / "workload.json"),
            ("expected_counts_sha256", QUERY_FIXTURE / "expected-counts.json"),
        ):
            self.assertEqual(
                evidence["inputs"][name], hashlib.sha256(path.read_bytes()).hexdigest()
            )
        self.assertEqual(
            evidence["driver"]["executable_sha256"],
            hashlib.sha256(self.driver.read_bytes()).hexdigest(),
        )

        variants = {variant["query_id"]: variant for variant in evidence["variants"]}
        self.assertEqual(list(variants), ["friends-of-person", "vertex-components", "vertex-bfs"])
        rows = {
            query_id: [(sample["binding_id"], sample["rows"]) for sample in variant["samples"]]
            for query_id, variant in variants.items()
        }
        # Alice knows Bob and Carol, Bob knows Carol, Carol knows nobody.
        self.assertEqual(
            rows["friends-of-person"], [("person-1", 2), ("person-2", 1), ("person-3", 0)]
        )
        self.assertEqual(rows["vertex-components"], [("all", 5)])
        self.assertEqual(
            variants["vertex-components"]["interface"], "graphforge_api::GraphForge::cluster"
        )
        self.assertEqual(
            variants["vertex-bfs"]["warmup"], {"binding_id": "from-1", "excluded": True}
        )
        # From 1, LINK reaches 2 and 3; vertex 4 has no LINK edges.
        bfs = dict(rows["vertex-bfs"])
        self.assertGreater(bfs["from-1"], bfs["from-4"])

        # A second run over the reopened project gives the same answers.
        again = self.scratch / "again.json"
        self.assertEqual(self.query(QUERY_FIXTURE / "expected-counts.json", again).returncode, 0)
        digests = lambda document: [  # noqa: E731
            sample["result_sha256"] for v in document["variants"] for sample in v["samples"]
        ]
        self.assertEqual(digests(evidence), digests(json.loads(again.read_text(encoding="utf-8"))))

        # The policy refuses this real document once any latency is not the driver's.
        evidence["variants"][0]["summary"]["p95_ns"] += 1
        with self.assertRaises(GdcMeasurementBoundaryError) as error:
            assert_query_latency_authority(evidence)
        self.assertEqual(error.exception.cause, "latency_not_from_samples")

    def test_deliberate_count_mismatch_fails_typed_and_writes_no_evidence(self) -> None:
        expected = json.loads((QUERY_FIXTURE / "expected-counts.json").read_text(encoding="utf-8"))
        expected["edges"] = 8
        expected["types"]["KNOWS"] = 4
        path = self.scratch / "wrong-counts.json"
        path.write_text(json.dumps(expected), encoding="utf-8")
        output = self.scratch / "mismatch.json"
        completed = self.query(path, output)
        self.assertEqual(completed.returncode, 2, completed.stderr)
        error = json.loads(completed.stderr)["error"]
        self.assertEqual(error["cause"], "count_mismatch")
        self.assertEqual(
            error["message"], "edges: expected 8, read 7; type KNOWS: expected 4, read 3"
        )
        self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
