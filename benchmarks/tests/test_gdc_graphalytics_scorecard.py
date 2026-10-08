"""The Graphalytics scorecard through the GDC rung runner (#1903).

``fixtures/gdc/graphalytics-rung-fixture`` holds three tiny archives in the
Graphalytics format (``.v``, ``.e``, ``.properties`` and hand-derived
reference outputs). The real converter, ``gf`` and query driver run every
phase; only BenchExec is replaced (``FakeBenchExec``, as in ``test_gdc_rung``).
ga-directed and ga-undirected pass; ga-wrong's BFS reference is wrong for one
vertex, so the ladder stops there with ``reference_mismatch``.

The unit tests pin the pieces the end-to-end run relies on: ``.properties``
parsing and the archive check, reference parsing and conversion, the
Graphalytics matching rules and Tp as the mean of three runs.
"""

from __future__ import annotations

import contextlib
import io
import json
import math
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any
import unittest
from unittest.mock import patch

from graphforge_bench import gdc_graphalytics_scorecard as graphalytics
from graphforge_bench import gdc_rung
from graphforge_bench.gdc_contracts import workspace_root
from graphforge_bench.gdc_rung_inputs import (
    LadderSpec,
    RungInputError,
    load_ladder_spec,
    matches,
    within_epsilon,
)
from graphforge_bench.gdc_scorecard_card import (
    GRAPHALYTICS_RUNS,
    MAKESPAN_NOT_MEASURED,
    CardError,
    _graphalytics,
    render_card,
    write_card,
)

from tests.test_gdc_rung import QUIET, FakeBenchExec
from tests.test_gdc_scorecard_load import converter_binary, gf_binary

ROOT = workspace_root()
FIXTURE = ROOT / "fixtures" / "gdc" / "graphalytics-rung-fixture"
LADDER = FIXTURE / "ladder-spec.json"
REAL_LADDER = ROOT / "profiles" / "gdc" / "graphalytics-scorecard-ladder-spec.json"
COMMIT = "f013587f0123456789abcdef0123456789abcdef"


def serve_fixture_archives(url: str) -> contextlib.AbstractContextManager[io.BytesIO]:
    name = url.rsplit("/", 1)[1]
    return contextlib.closing(io.BytesIO((FIXTURE / "archives" / name).read_bytes()))


class GraphalyticsLadderEndToEndTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.executables = gdc_rung.GdcExecutables(
            gf=gf_binary(), driver=converter_binary(), benchexec_python=Path(sys.executable)
        )

    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="gdc-graphalytics-rung-")
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        self.work = self.scratch / "work"
        self.work.mkdir()
        self.output = self.scratch / "evidence"
        swap = patch.object(
            gdc_rung, "_host_swap_counters", return_value={"pswpin": 0, "pswpout": 0}
        )
        swap.start()
        self.addCleanup(swap.stop)

    def evidence(self, name: str) -> Any:
        return json.loads((self.output / name).read_text(encoding="utf-8"))

    def test_the_fixture_ladder_checks_every_algorithm_and_stops_at_a_wrong_answer(self) -> None:
        ladder = gdc_rung.prepare(
            root=ROOT,
            ladder_path=LADDER,
            output_dir=self.output,
            work_root=self.work,
            cache_root=self.scratch / "cache",
            executables=self.executables,
            commit=COMMIT,
            host_label="CI-RUNNER",
            storage_medium="SSD",
            opener=serve_fixture_archives,
            benchexec=FakeBenchExec(),
        )
        results = gdc_rung.climb(
            ladder,
            reserved_headroom_bytes=0,
            quiet_host_wait_seconds=0,
            quiet_host=lambda _wait: QUIET,
        )
        self.assertEqual(
            [(r["rung_id"], r["status"]) for r in results],
            [("ga-directed", "passed"), ("ga-undirected", "passed"), ("ga-wrong", "failed")],
            [r["failures"] for r in results],
        )
        for result in results:
            self.assertEqual(result["inventory"], {"empty": True, "entries": []})

        # Every run of every supported algorithm is checked against the archive.
        directed = self.evidence("graphalytics-ga-directed-correctness.json")
        self.assertEqual(
            (directed["status"], directed["checked"], directed["matched"]), ("passed", 6, 6)
        )
        self.assertEqual(
            {q: (t["matching"], t["matched"]) for q, t in directed["queries"].items()},
            {"bfs": ("exact", 3), "wcc": ("equivalence", 3)},
        )
        undirected = self.evidence("graphalytics-ga-undirected-correctness.json")
        self.assertEqual(
            (undirected["status"], undirected["checked"], undirected["matched"]),
            ("passed", 12, 12),
        )
        self.assertEqual(
            {q: t["matching"] for q, t in undirected["queries"].items()},
            {"bfs": "exact", "wcc": "equivalence", "lcc": "epsilon", "sssp": "epsilon"},
        )
        self.assertRegex(undirected["reference_sha256"], r"^[0-9a-f]{64}$")

        # The driver dispatched each source by UUID and kept only the answer columns.
        evidence = self.evidence("graphalytics-ga-undirected-query-evidence.json")
        by_id = {variant["query_id"]: variant for variant in evidence["variants"]}
        self.assertEqual(by_id["bfs"]["columns"], ["target_uuid", "cost"])
        self.assertEqual(by_id["lcc"]["columns"], ["id", "score"])
        self.assertEqual(
            [s["rows"] for s in by_id["bfs"]["samples"]], [5, 5, 5]
        )  # vertex 60 is unreachable

        # ga-wrong: the wrong BFS depth fails every BFS run, typed.
        wrong = results[2]
        self.assertEqual(
            [(f["phase"], f["cause"]) for f in wrong["failures"]],
            [("check", "reference_mismatch")] * 3,
        )
        self.assertEqual(
            sorted(f["detail"].split(":")[0] for f in wrong["failures"]),
            ["bfs/run-1", "bfs/run-2", "bfs/run-3"],
        )

        # The card headlines ga-undirected with Tl, Tp, EVPS and the makespan label.
        paths = write_card(ROOT, ladder.spec, self.output)
        card = json.loads(paths["json"].read_text(encoding="utf-8"))
        text = paths["text"].read_text(encoding="utf-8")
        self.assertEqual(render_card(card), text)
        self.assertEqual(card["dataset"]["rung_id"], "ga-undirected")
        algorithms = {a["algorithm"]: a for a in card["graphalytics"]["algorithms"]}
        self.assertEqual(sorted(algorithms), ["bfs", "lcc", "sssp", "wcc"])
        for variant in evidence["variants"]:
            latencies = [s["latency_ns"] for s in variant["samples"]]
            self.assertEqual(len(latencies), GRAPHALYTICS_RUNS)
            tp = sum(latencies) / 3 / 1e9
            self.assertAlmostEqual(algorithms[variant["query_id"]]["tp_seconds"], tp)
            self.assertAlmostEqual(algorithms[variant["query_id"]]["evps"], (6 + 6) / tp)
        self.assertIn(
            "Coverage:     4/6 algorithms; refused: pr (fixed_iteration_pagerank_not_exposed); "
            "cdlp (synchronous_cdlp_not_exposed)\n",
            text,
        )
        self.assertIn(f"Makespan:     not measured ({MAKESPAN_NOT_MEASURED})\n", text)
        self.assertIn(
            "Correctness:  12/12 supported results match the ga-undirected archive's LDBC "
            "Graphalytics reference outputs\n",
            text,
        )
        self.assertIn(
            "Next rung:    fixture wrong reference — typed failure (reference_mismatch)\n", text
        )
        self.assertIn("6 nodes / 6 edges loaded (LDBC published: 6 / 6; exact match)", text)


def fixture_spec() -> LadderSpec:
    return load_ladder_spec(ROOT, LADDER)


def copied_archive(test: unittest.TestCase, name: str) -> Path:
    scratch = tempfile.TemporaryDirectory(prefix="gdc-graphalytics-archive-")
    test.addCleanup(scratch.cleanup)
    target = Path(scratch.name) / name
    shutil.copytree(FIXTURE / "source" / name, target)
    return target


def edit(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    assert old in text, (path, old)
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


class ArchiveCheckTests(unittest.TestCase):
    def test_properties_parse_into_the_graph_facts(self) -> None:
        graph = graphalytics.read_properties(FIXTURE / "source" / "ga-undirected")
        self.assertEqual(
            graph,
            graphalytics.GraphProperties(
                name="ga-undirected",
                vertex_file="ga-undirected.v",
                edge_file="ga-undirected.e",
                vertices=6,
                edges=6,
                directed=False,
                weighted=True,
                algorithms=("bfs", "cdlp", "lcc", "pr", "sssp", "wcc"),
                bfs_source=10,
                sssp_source=10,
            ),
        )
        with self.assertRaises(RungInputError) as error:
            graphalytics.parse_properties("graph.x.directed true\n")
        self.assertEqual(error.exception.cause, "archive_properties_invalid")

    def test_the_archive_must_agree_with_the_ladder_and_workload(self) -> None:
        spec = fixture_spec()
        rung = spec.rung("ga-undirected")
        archive = copied_archive(self, "ga-undirected")
        graphalytics.check_archive(spec, rung, archive)
        properties = archive / "ga-undirected.properties"
        for old, new in (
            ("meta.vertices = 6", "meta.vertices = 7"),
            ("directed = false", "directed = true"),
            ("bfs.source-vertex = 10", "bfs.source-vertex = 20"),
            ("sssp.source-vertex = 10", "sssp.source-vertex = 20"),
            ("algorithms = bfs, cdlp, lcc, pr, sssp, wcc", "algorithms = bfs, cdlp, lcc, pr, wcc"),
        ):
            original = properties.read_text(encoding="utf-8")
            edit(properties, old, new)
            with self.assertRaises(RungInputError, msg=new) as error:
                graphalytics.check_archive(spec, rung, archive)
            self.assertEqual(error.exception.cause, "archive_properties_mismatch", new)
            properties.write_text(original, encoding="utf-8")

    def test_a_workload_that_does_not_dispatch_as_the_graph_needs_is_refused(self) -> None:
        spec = fixture_spec()
        graph = graphalytics.ladder_graph(spec, "ga-undirected")
        workload = json.loads((FIXTURE / "queries" / "undirected-workload.json").read_text())

        def refused(change: Any) -> str:
            broken = json.loads(json.dumps(workload))
            change(broken["variants"])
            with tempfile.NamedTemporaryFile("w", suffix=".json", dir=FIXTURE, delete=False) as f:
                json.dump(broken, f)
            self.addCleanup(Path(f.name).unlink)
            rung = {**spec.rung("ga-undirected"), "workload": Path(f.name).name}
            with self.assertRaises(RungInputError) as error:
                graphalytics.check_workload(spec, rung, graph)
            return error.exception.cause

        graphalytics.check_workload(spec, spec.rung("ga-undirected"), graph)
        cases = {
            "two runs": lambda v: v[0]["bindings"].pop(),
            "directed BFS on an undirected graph": lambda v: v[0]["operation"].update(
                directed=True
            ),
            "another source": lambda v: [
                b["params"]["source"].update(
                    value=graphalytics.canonical_uuid(graphalytics.node_uuid("Vertex", 20))
                )
                for b in v[0]["bindings"]
            ],
            "source by property": lambda v: v[0]["operation"].update(
                source={"label": "Vertex", "property": "id", "param": "source"}
            ),
            "path column kept": lambda v: v[0].update(columns=["target_uuid", "cost", "path"]),
            "an algorithm dropped": lambda v: v.pop(),
        }
        for name, change in cases.items():
            self.assertEqual(refused(change), "archive_properties_mismatch", name)


class ReferenceTests(unittest.TestCase):
    def test_node_uuid_reproduces_the_converters_identity(self) -> None:
        # identity.rs: SHA-256 prefix of ("graphforge.gdc.node.v1\0", label, \0, id BE)
        # with UUIDv7 version and variant bits; checked end to end by the BFS key match.
        uuid = graphalytics.node_uuid("Vertex", 1)
        self.assertRegex(uuid, r"^[0-9a-f]{12}7[0-9a-f]{3}[89ab][0-9a-f]{15}$")
        self.assertNotEqual(uuid, graphalytics.node_uuid("Vertex", 2))
        self.assertNotEqual(uuid, graphalytics.node_uuid("Other", 1))

    def test_reference_files_must_list_every_vertex_once(self) -> None:
        archive = copied_archive(self, "ga-directed")
        path = archive / "ga-directed-BFS"
        rows = graphalytics.parse_vertex_values(path, 9)
        self.assertEqual(rows[0], (1, "0"))
        self.assertEqual(rows[-1], (9, "9223372036854775807"))
        for content, vertices in (
            (path.read_text() + "9 1\n", 10),  # a repeated vertex
            (path.read_text(), 10),  # a missing vertex
            (path.read_text() + "10 1 2\n", 10),  # a malformed line
            (path.read_text().replace("2 1", "2 one"), 9),  # a non-numeric value
        ):
            path.write_text(content)
            with self.assertRaises(RungInputError) as error:
                graphalytics.parse_vertex_values(path, vertices)
            self.assertEqual(error.exception.cause, "reference_invalid")

    def test_conversion_matches_unreachable_vertices_by_absence(self) -> None:
        rows = [(1, "0"), (2, "2"), (3, str(graphalytics.BFS_UNREACHABLE))]
        rule, columns, cells = graphalytics.convert_reference("bfs", rows, "Vertex")
        self.assertEqual(rule, {"matching": "exact", "key": ["target_uuid"]})
        self.assertEqual(columns, ["target_uuid", "cost"])
        self.assertEqual(
            cells,
            [
                [graphalytics.node_uuid("Vertex", 1), "0.0"],
                [graphalytics.node_uuid("Vertex", 2), "2.0"],
            ],
        )
        rule, _, cells = graphalytics.convert_reference(
            "sssp", [(1, "0.000000000000000e+00"), (2, "infinity")], "Vertex"
        )
        self.assertEqual(rule["matching"], "epsilon")
        self.assertEqual(cells, [[graphalytics.node_uuid("Vertex", 1), "0.000000000000000e+00"]])
        rule, columns, cells = graphalytics.convert_reference("wcc", [(4, "1"), (5, "1")], "V")
        self.assertEqual(
            (rule, columns, cells),
            (
                {"matching": "equivalence", "key": ["id"], "label": "community_id"},
                ["id", "community_id"],
                [["4", "1"], ["5", "1"]],
            ),
        )
        rule, columns, cells = graphalytics.convert_reference("lcc", [(4, "1.5e-01")], "V")
        self.assertEqual(
            (rule, columns, cells),
            (
                {"matching": "epsilon", "epsilon": 1e-4, "key": ["id"]},
                ["id", "score"],
                [["4", "1.5e-01"]],
            ),
        )

    def test_the_archive_reference_covers_every_run_of_every_supported_algorithm(self) -> None:
        spec = fixture_spec()
        reference, digest = graphalytics.archive_reference(
            spec, spec.rung("ga-undirected"), FIXTURE / "source" / "ga-undirected"
        )
        self.assertEqual(sorted(reference["queries"]), ["bfs", "lcc", "sssp", "wcc"])
        for query in reference["queries"].values():
            self.assertEqual(sorted(query["bindings"]), ["run-1", "run-2", "run-3"])
        bfs = reference["queries"]["bfs"]["bindings"]["run-1"]
        self.assertEqual(len(bfs["rows"]), 5)  # 60 is unreachable
        self.assertEqual(len(reference["queries"]["wcc"]["bindings"]["run-1"]["rows"]), 6)
        self.assertRegex(digest, r"^[0-9a-f]{64}$")


class GraphalyticsMatchingTests(unittest.TestCase):
    @staticmethod
    def result(columns: list[str], rows: list[list[Any]]) -> dict[str, Any]:
        return {
            "columns": [{"name": name, "type": "Utf8"} for name in columns],
            "rows": rows,
            "ordered": False,
        }

    def test_bfs_depths_match_exactly_by_target(self) -> None:
        rule = {"matching": "exact", "key": ["target_uuid"]}
        reference = {"columns": ["target_uuid", "cost"], "rows": [["a", "0.0"], ["b", "1.0"]]}
        self.assertTrue(
            matches(
                rule, self.result(["target_uuid", "cost"], [["b", "1.0"], ["a", "0.0"]]), reference
            )
        )
        for rows in (
            [["a", "0.0"], ["b", "2.0"]],  # a wrong depth
            [["a", "0.0"]],  # a reached vertex missing
            [["a", "0.0"], ["b", "1.0"], ["c", "3.0"]],  # an unreachable vertex reached
            [["a", "0.0"], ["b", "1.0"], ["b", "1.0"]],  # a vertex twice
        ):
            self.assertFalse(
                matches(rule, self.result(["target_uuid", "cost"], rows), reference), rows
            )

    def test_epsilon_is_relative_to_the_reference_value(self) -> None:
        self.assertTrue(within_epsilon(1.0001, 1.0, 1e-4))
        self.assertTrue(within_epsilon(0.0, 0.0, 1e-4))
        self.assertFalse(within_epsilon(1e-12, 0.0, 1e-4))
        # A symmetric tolerance (relative to the larger value) would accept this.
        self.assertFalse(within_epsilon(1.00010001, 1.0, 1e-4))
        self.assertTrue(math.isclose(1.00010001, 1.0, rel_tol=1e-4))
        self.assertTrue(within_epsilon(math.inf, math.inf, 1e-4))
        self.assertFalse(within_epsilon(1e300, math.inf, 1e-4))
        self.assertFalse(within_epsilon(math.nan, 1.0, 1e-4))
        rule = {"matching": "epsilon", "epsilon": 1e-4, "key": ["id"]}
        reference = {"columns": ["id", "score"], "rows": [["1", "0.5"], ["2", "0"]]}
        self.assertTrue(
            matches(rule, self.result(["id", "score"], [["1", "0.50004"], ["2", "0.0"]]), reference)
        )
        self.assertFalse(
            matches(rule, self.result(["id", "score"], [["1", "0.5"], ["2", "1e-9"]]), reference)
        )

    def test_wcc_matches_under_equivalence_only(self) -> None:
        rule = {"matching": "equivalence", "key": ["id"], "label": "community_id"}
        reference = {
            "columns": ["id", "community_id"],
            "rows": [["1", "1"], ["2", "1"], ["3", "3"]],
        }
        relabelled = self.result(["id", "community_id"], [["1", "70"], ["2", "70"], ["3", "4"]])
        self.assertTrue(matches(rule, relabelled, reference))
        merged = self.result(["id", "community_id"], [["1", "0"], ["2", "0"], ["3", "0"]])
        self.assertFalse(matches(rule, merged, reference))
        split = self.result(["id", "community_id"], [["1", "0"], ["2", "5"], ["3", "3"]])
        self.assertFalse(matches(rule, split, reference))


def evidence_with(latencies: list[int | None]) -> dict[str, Any]:
    samples = [
        {"binding_id": f"run-{i}", "status": "measured", "latency_ns": latency}
        if latency is not None
        else {"binding_id": f"run-{i}", "status": "failed"}
        for i, latency in enumerate(latencies, start=1)
    ]
    return {"variants": [{"query_id": "bfs", "samples": samples}]}


class TpTests(unittest.TestCase):
    def test_tp_is_the_mean_of_three_runs_and_evps_divides_by_it(self) -> None:
        graphalytics_card = _graphalytics(
            evidence_with([1_000_000_000, 2_000_000_000, 6_000_000_000]), 7.5, 90, 10
        )
        self.assertEqual(graphalytics_card["tl_seconds"], 7.5)
        self.assertEqual(graphalytics_card["makespan_not_measured"], MAKESPAN_NOT_MEASURED)
        (bfs,) = graphalytics_card["algorithms"]
        self.assertEqual(bfs["tp_seconds"], 3.0)
        self.assertEqual(bfs["runs"], 3)
        self.assertEqual(bfs["evps"], 100 / 3.0)
        self.assertIsNone(bfs["makespan_seconds"])

    def test_tp_needs_exactly_three_measured_runs(self) -> None:
        for latencies in (
            [1, 2],
            [1, 2, 3, 4],
            [1, 2, None],
        ):
            with self.assertRaises(CardError, msg=latencies) as error:
                _graphalytics(evidence_with(latencies), 1.0, 1, 1)
            self.assertEqual(error.exception.cause, "graphalytics_runs_invalid")


class RealLadderTests(unittest.TestCase):
    """The committed Graphalytics ladder agrees with its count ladder and workloads."""

    def test_the_real_ladder_climbs_the_four_archives_in_order(self) -> None:
        spec = load_ladder_spec(ROOT, REAL_LADDER)
        self.assertEqual(
            [rung["id"] for rung in spec.document["rungs"]],
            ["wiki-Talk", "cit-Patents", "datagen-7_5-fb", "graph500-22"],
        )
        self.assertEqual(spec.document["metric_shape"], "graphalytics")
        for rung in spec.document["rungs"]:
            graph = graphalytics.ladder_graph(spec, rung["id"])
            graphalytics.check_workload(spec, rung, graph)
            self.assertEqual(rung["reference"], {"archive_outputs": "graphalytics"})
            refused = {item["query_id"]: item["cause"] for item in rung["refused"]}
            self.assertEqual(refused.pop("pr"), "fixed_iteration_pagerank_not_exposed")
            self.assertEqual(refused.pop("cdlp"), "synchronous_cdlp_not_exposed")
            if graph.directed:
                self.assertEqual(refused.pop("lcc"), "directed_lcc_semantics_not_exposed")
            self.assertEqual(refused, {})


if __name__ == "__main__":
    unittest.main()
