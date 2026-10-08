"""Drive a tiny GDC scorecard ladder end to end, then check its card.

The committed rung fixture (``fixtures/gdc/rung-fixture``) has three rungs in
the SNB Interactive v1 CSV shape: sf0 passes, sf1 fails typed (one query fails
at runtime and one reference answer is wrong), and sf2 is never attempted. The
real converter, ``gf`` and query driver run every phase. Only BenchExec is
replaced: CI runners cannot delegate cgroups, so ``FakeBenchExec`` runs the
tool-info module's own command line as a child process and writes BenchExec's
result XML and log layout with that child's measured wall and CPU time.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from typing import Any
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from graphforge_bench import gdc_rung
from graphforge_bench.gdc_contracts import workspace_root
from graphforge_bench.gdc_measurement_policy import (
    CARD_METRIC_SOURCES,
    GdcMeasurementBoundaryError,
)
from graphforge_bench.gdc_rung_inputs import (
    RungInputError,
    check_reference,
    expected_counts,
    load_ladder_spec,
    matches,
    read_results,
    result_digest,
)
from graphforge_bench.gdc_scorecard_card import (
    MAKESPAN_NOT_MEASURED,
    CardError,
    render_card,
    write_card,
)
from graphforge_bench.progressive_host_run import (
    HostRunError,
    reclaim_rung_workspace,
    reclaim_workspace,
)
from graphforge_bench.tools.graphforge_gdc_phase import EXECUTABLE, Tool
from jsonschema import Draft202012Validator

from tests.test_gdc_scorecard_load import converter_binary, gf_binary

ROOT = workspace_root()
FIXTURE = ROOT / "fixtures" / "gdc" / "rung-fixture"
LADDER = FIXTURE / "ladder-spec.json"
COMMIT = "f013587f0123456789abcdef0123456789abcdef"
QUIET = {"window_seconds": 60, "waited_seconds": 0, "mean_busy_cores": 0.1, "peak_busy_cores": 0.4}
CARD_LABELS = (
    "Hardware",
    "Graph",
    "Load time",
    "On disk",
    "Coverage",
    "Throughput",
    "Latency",
    "Peak RAM",
    "Correctness",
    "Next rung",
)


def serve_fixture_archives(url: str) -> contextlib.AbstractContextManager[io.BytesIO]:
    """The dataset cache's opener, serving the committed archives with no network."""
    name = url.rsplit("/", 1)[1]
    return contextlib.closing(io.BytesIO((FIXTURE / "archives" / name).read_bytes()))


class FakeBenchExec:
    """Runs a staged phase the way BenchExec would, writing BenchExec's output layout."""

    def __init__(self, *, termination: str | None = None) -> None:
        self.termination = termination
        self.stages: list[Path] = []

    def __call__(self, stage: Path, executables: Any, identities: Any, work_root: Path) -> int:
        self.stages.append(stage)
        raw = stage / "raw"
        raw.mkdir()
        task = SimpleNamespace(input_files_or_identifier=[str(stage / "phase.json")])
        argv = Tool().cmdline(str(stage / "bin" / EXECUTABLE), [], task, {})
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        started = time.monotonic()
        completed = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            check=False,
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(work_root)},
        )
        wall = time.monotonic() - started
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        cpu = (after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime)
        logs = raw / f"benchmark.{gdc_rung.DEFINITION}.logfiles"
        logs.mkdir()
        (logs / "phase.json.log").write_text(
            " ".join(argv) + "\n\n" + "-" * 80 + "\n\n" + completed.stdout, encoding="utf-8"
        )
        status = "DONE" if completed.returncode == 0 else "ERROR"
        columns = {
            "status": status if self.termination is None else "TIMEOUT",
            "walltime": f"{wall:.6f}s",
            "cputime": f"{cpu:.6f}s",
            "memory": f"{after.ru_maxrss * 1024}B",
            "blkio-read": "0B",
            "blkio-write": "0B",
            "pressure-cpu-some": "0s",
            "pressure-io-some": "0s",
            "pressure-memory-some": "0s",
            "returnvalue": str(completed.returncode),
        }
        if self.termination is not None:
            columns["terminationreason"] = self.termination
        result = ET.Element("result")
        run = ET.SubElement(result, "run", name="phase.json")
        for title, value in columns.items():
            ET.SubElement(run, "column", title=title, value=value)
        ET.ElementTree(result).write(raw / f"benchmark.results.{gdc_rung.DEFINITION}.xml")
        return 0


def digest_of(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


class Scratch(unittest.TestCase):
    def setUp(self) -> None:
        scratch = tempfile.TemporaryDirectory(prefix="gdc-rung-")
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        self.work = self.scratch / "work"
        self.work.mkdir()
        self.output = self.scratch / "evidence"
        self.cache = self.scratch / "cache"

    def ladder(
        self,
        executables: gdc_rung.GdcExecutables,
        benchexec: Any,
        *,
        opener: Any = serve_fixture_archives,
    ) -> gdc_rung.Ladder:
        return gdc_rung.prepare(
            root=ROOT,
            ladder_path=LADDER,
            output_dir=self.output,
            work_root=self.work,
            cache_root=self.cache,
            executables=executables,
            commit=COMMIT,
            host_label="CI-RUNNER",
            storage_medium="SSD",
            opener=opener,
            benchexec=benchexec,
        )


class TinyLadderEndToEndTests(Scratch):
    @classmethod
    def setUpClass(cls) -> None:
        cls.executables = gdc_rung.GdcExecutables(
            gf=gf_binary(), driver=converter_binary(), benchexec_python=Path(sys.executable)
        )

    def setUp(self) -> None:
        super().setUp()
        # Host swap is a host-wide interference check (PhaseClassificationTests);
        # a shared CI or developer host must not decide these fixture outcomes.
        swap = patch.object(
            gdc_rung, "_host_swap_counters", return_value={"pswpin": 0, "pswpout": 0}
        )
        swap.start()
        self.addCleanup(swap.stop)

    def test_tiny_ladder_stops_at_the_first_typed_failure_and_renders_its_card(self) -> None:
        benchexec = FakeBenchExec()
        ladder = self.ladder(self.executables, benchexec)
        results = gdc_rung.climb(
            ladder,
            reserved_headroom_bytes=0,
            quiet_host_wait_seconds=0,
            quiet_host=lambda _wait: QUIET,
        )

        # First-failure stop: sf0 passes, sf1 fails typed, sf2 never starts.
        self.assertEqual([r["status"] for r in results], ["passed", "failed"])
        self.assertTrue(gdc_rung.ladder_finished(ladder.spec, results))
        self.assertEqual(len(benchexec.stages), 6)
        self.assertEqual(sorted(self.output.glob("snb-interactive-sf2-*")), [])

        passed, failed = results
        # Three BenchExec documents per rung, each the schema-valid authority.
        validator = Draft202012Validator(
            json.loads((ROOT / "schemas/benchexec-run-evidence.json").read_text())
        )
        for phase in gdc_rung.PHASES:
            name = passed["phases"][phase]["benchexec_document"]
            self.assertEqual(name, f"snb-interactive-sf0-{phase}-benchexec.json")
            document = json.loads((self.output / name).read_text())
            self.assertEqual(list(validator.iter_errors(document)), [])
            self.assertEqual(document["outcome"], "passed")
            self.assertEqual(document["graphforge"]["phase"], phase)
            self.assertGreater(document["graphforge"]["peak_rss_bytes"], 0)
        for name, digest in passed["documents"].items():
            self.assertEqual(digest_of(self.output / name), digest, name)

        # Teardown: every rung leaves an empty work-root inventory.
        for result in results:
            self.assertEqual(result["inventory"], {"empty": True, "entries": []})
            inventory = json.loads(
                (self.output / f"snb-interactive-{result['rung_id']}-inventory.json").read_text()
            )
            self.assertTrue(inventory["empty"])
        self.assertEqual(sorted(p.name for p in (self.work / "workspace").iterdir()), [])

        # Expected counts: listed archive records; the label split of the
        # column-labelled place table is the converter's, summing to the ladder.
        expected = json.loads(
            (self.output / "snb-interactive-sf0-expected-counts.json").read_text()
        )
        self.assertEqual(
            (expected["nodes"], expected["edges"], expected["labels"], expected["types"]),
            (7, 6, {"City": 2, "Country": 1, "Person": 4}, {"IS_LOCATED_IN": 4, "KNOWS": 2}),
        )
        evidence = json.loads((self.output / "snb-interactive-sf0-query-evidence.json").read_text())
        self.assertEqual(evidence["reconciliation"]["status"], "reconciled")
        correctness = json.loads((self.output / "snb-interactive-sf0-correctness.json").read_text())
        self.assertEqual(
            (correctness["status"], correctness["checked"], correctness["matched"]),
            ("passed", 7, 7),
        )
        self.assertEqual(correctness["queries"]["person-components"]["matching"], "equivalence")

        # sf1: the runtime failure and the wrong answer are both recorded,
        # because every query still ran.
        self.assertEqual(failed["failure"]["cause"], "query_failed")
        self.assertEqual(
            [(f["phase"], f["cause"]) for f in failed["failures"]],
            [("query", "query_failed"), ("check", "reference_mismatch")],
        )
        self.assertIn("people-per-city/all", failed["failures"][1]["detail"])
        # The rung says what failed, not only that something did: the driver's own
        # exit message counts failures and names a workspace the teardown deletes.
        self.assertEqual(
            failed["failures"][0]["detail"],
            "unparsable (only): GF_PARSE: parse error at 21..27: expected RParen, found Return",
        )
        failed_evidence = json.loads(
            (self.output / "snb-interactive-sf1-query-evidence.json").read_text()
        )
        self.assertEqual(
            [v["status"] for v in failed_evidence["variants"]], ["measured", "failed", "measured"]
        )
        failed_correctness = json.loads(
            (self.output / "snb-interactive-sf1-correctness.json").read_text()
        )
        self.assertEqual((failed_correctness["checked"], failed_correctness["matched"]), (3, 2))
        self.assertTrue((self.output / "snb-interactive-sf1-query-benchexec-raw").is_dir())

        # The card headlines sf0 and names what stopped sf1.
        paths = write_card(ROOT, ladder.spec, self.output)
        text = paths["text"].read_text(encoding="utf-8")
        card = json.loads(paths["json"].read_text(encoding="utf-8"))
        self.assertEqual(render_card(card), text)
        lines = text.splitlines()
        self.assertRegex(
            lines[0],
            r"^GraphForge \S+ \(f013587f0123\) — GDC snb-interactive, fixture SF0, unaudited$",
        )
        self.assertEqual(
            lines[1],
            "These are not LDBC Benchmark Results. Read-only fixture queries over the "
            "bulk-load snapshot; the update stream is refused.",
        )
        self.assertEqual([line.split(":")[0] for line in lines[2:12]], list(CARD_LABELS))
        for line in lines[2:12]:
            self.assertRegex(line, r"^[A-Z][A-Za-z ]+: +\S")
            self.assertEqual(line.index(line.split(":", 1)[1].strip()), 14, line)
        by_label = {line.split(":", 1)[0]: line.split(":", 1)[1].strip() for line in lines[2:12]}
        self.assertEqual(
            by_label["Graph"],
            "7 nodes / 6 edges loaded (LDBC published: 8 / 6 whole network, snapshot exact match)",
        )
        self.assertEqual(
            by_label["Coverage"],
            "4/5 queries; refused: IU1 (interactive_update_stream_not_exposed)",
        )
        self.assertEqual(
            by_label["Correctness"],
            "7/7 supported results match the rung fixture's hand-derived answers",
        )
        self.assertEqual(by_label["Next rung"], "fixture SF1 — typed failure (query_failed)")
        self.assertIn("CC-BY 4.0", lines[-1])
        variances = lines[lines.index("Variances:") + 1 : -1]
        self.assertEqual(
            [line.split(" (")[0] for line in variances],
            ["  - scope", "  - rewrite", "  - discrepancy"],
        )
        self.assertIn("(person): Fixture: one person arrives", variances[2])
        load = json.loads((self.output / "snb-interactive-sf0-load-benchexec.json").read_text())
        self.assertEqual(card["load"]["wall_seconds"], load["authority"]["wall_seconds"])
        self.assertEqual(card["peak_rss"]["load_bytes"], load["graphforge"]["peak_rss_bytes"])
        self.assertGreater(card["on_disk_bytes"], 0)
        self.assertEqual(card["latency"]["samples"], 7)

        # Resuming a finished ladder launches nothing and changes nothing.
        again = gdc_rung.climb(
            ladder,
            reserved_headroom_bytes=0,
            quiet_host_wait_seconds=0,
            quiet_host=lambda _wait: self.fail("a finished ladder launches nothing"),
        )
        self.assertEqual(again, results)
        self.assertEqual(len(benchexec.stages), 6)
        with self.assertRaises(FileExistsError):
            write_card(ROOT, ladder.spec, self.output)

    def test_a_swap_rise_fails_the_phase_naming_and_retaining_the_counters(self) -> None:
        readings = iter(
            [{"pswpin": 10, "pswpout": 20}, {"pswpin": 12, "pswpout": 20}]  # before, after
        )
        ladder = self.ladder(self.executables, FakeBenchExec())
        with patch.object(gdc_rung, "_host_swap_counters", side_effect=lambda: next(readings)):
            results = gdc_rung.climb(
                ladder,
                reserved_headroom_bytes=0,
                quiet_host_wait_seconds=0,
                quiet_host=lambda _wait: QUIET,
            )
        self.assertEqual(
            results[0]["failure"],
            {
                "phase": "convert",
                "cause": "host_swapped",
                "detail": "host swap counters rose during the phase: pswpin +2 (10 -> 12); "
                "see host-swap.json",
            },
        )
        retained = self.output / "snb-interactive-sf0-convert-benchexec-raw" / "host-swap.json"
        self.assertEqual(
            json.loads(retained.read_text()),
            {"before": {"pswpin": 10, "pswpout": 20}, "after": {"pswpin": 12, "pswpout": 20}},
        )

    def test_a_phase_stopped_at_the_wall_fails_typed_and_still_tears_down(self) -> None:
        ladder = self.ladder(self.executables, FakeBenchExec(termination="walltime"))
        results = gdc_rung.climb(
            ladder,
            reserved_headroom_bytes=0,
            quiet_host_wait_seconds=0,
            quiet_host=lambda _wait: QUIET,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(
            (results[0]["status"], results[0]["failure"]["phase"], results[0]["failure"]["cause"]),
            ("failed", "convert", "rung_wall_exceeded"),
        )
        self.assertTrue(results[0]["inventory"]["empty"])
        with self.assertRaises(CardError) as error:
            write_card(ROOT, ladder.spec, self.output)
        self.assertEqual(error.exception.cause, "no_passing_rung")


class LadderControlTests(Scratch):
    """Admission, launch and teardown paths that need no real binaries."""

    def setUp(self) -> None:
        super().setUp()
        tools = self.scratch / "tools"
        tools.mkdir()
        for name in ("gf", "driver"):
            path = tools / name
            path.write_text("#!/bin/sh\necho graphforge 0.0.0-test\n", encoding="utf-8")
            path.chmod(0o755)
        self.executables = gdc_rung.GdcExecutables(
            gf=tools / "gf", driver=tools / "driver", benchexec_python=Path(sys.executable)
        )

    def test_a_refused_admission_records_not_admitted_and_stops(self) -> None:
        benchexec = FakeBenchExec()
        ladder = self.ladder(self.executables, benchexec)
        results = gdc_rung.climb(
            ladder,
            reserved_headroom_bytes=10**18,
            quiet_host_wait_seconds=0,
            quiet_host=lambda _wait: QUIET,
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "not_admitted")
        self.assertEqual(results[0]["failure"]["cause"], "work_root_capacity_refused")
        self.assertEqual(benchexec.stages, [])
        self.assertTrue(gdc_rung.ladder_finished(ladder.spec, results))

    def test_a_busy_host_launches_nothing_and_records_nothing(self) -> None:
        ladder = self.ladder(self.executables, FakeBenchExec())

        def busy(_wait: int) -> dict[str, Any]:
            raise HostRunError("host_not_quiet: busy processes ['cargo'], peak busy cores 9")

        with self.assertRaises(gdc_rung.LadderError) as error:
            gdc_rung.climb(
                ladder, reserved_headroom_bytes=0, quiet_host_wait_seconds=0, quiet_host=busy
            )
        self.assertEqual(error.exception.cause, "host_not_quiet")
        self.assertEqual(list(self.output.iterdir()), [])

    def test_debris_in_the_work_root_fails_teardown(self) -> None:
        def wrong_bytes(_url: str) -> contextlib.AbstractContextManager[io.BytesIO]:
            return contextlib.closing(io.BytesIO(b"not the pinned archive"))

        ladder = self.ladder(self.executables, FakeBenchExec(), opener=wrong_bytes)
        (self.work / "tmp").mkdir()
        (self.work / "tmp" / "leftover").write_bytes(b"x")
        results = gdc_rung.climb(
            ladder,
            reserved_headroom_bytes=0,
            quiet_host_wait_seconds=0,
            quiet_host=lambda _wait: QUIET,
        )
        self.assertEqual(
            [(f["phase"], f["cause"]) for f in results[0]["failures"]],
            [("acquisition", "checksum_mismatch"), ("teardown", "teardown_incomplete")],
        )
        self.assertEqual(results[0]["inventory"], {"empty": False, "entries": ["tmp/leftover"]})

    def test_an_interrupted_attempt_is_not_overwritten(self) -> None:
        ladder = self.ladder(self.executables, FakeBenchExec())
        self.output.mkdir(exist_ok=True)
        (self.output / "snb-interactive-sf0-convert-benchexec.json").write_text("{}")
        with self.assertRaises(gdc_rung.LadderError) as error:
            gdc_rung.climb(
                ladder,
                reserved_headroom_bytes=0,
                quiet_host_wait_seconds=0,
                quiet_host=lambda _wait: QUIET,
            )
        self.assertEqual(error.exception.cause, "existing_attempt_requires_inspection")

    def test_evidence_inside_the_work_root_is_refused(self) -> None:
        with self.assertRaises(gdc_rung.LadderError) as error:
            gdc_rung.prepare(
                root=ROOT,
                ladder_path=LADDER,
                output_dir=self.work / "evidence",
                work_root=self.work,
                cache_root=self.cache,
                executables=self.executables,
                commit=COMMIT,
                host_label="CI-RUNNER",
                storage_medium="SSD",
            )
        self.assertEqual(error.exception.cause, "output_dir_inside_work_root")


PASSED_RUN = {"timed_out": False, "termination_reason": None}
PASSED_TELEMETRY = {"failure": None, "peak_rss_bytes": 1024}


class PhaseClassificationTests(unittest.TestCase):
    def classify(
        self,
        measured: Any = PASSED_RUN,
        telemetry: Any = PASSED_TELEMETRY,
        document: Any = None,
        swapped: bool = False,
    ) -> Any:
        return gdc_rung.classify_phase(
            measured, telemetry, document or {"outcome": "passed"}, swapped=swapped
        )

    def test_each_limit_and_failure_has_its_typed_cause(self) -> None:
        self.assertIsNone(self.classify())
        cause = lambda **kwargs: self.classify(**kwargs)[0]  # noqa: E731
        self.assertEqual(cause(measured=None), "benchexec_failed")
        self.assertEqual(
            cause(measured={"timed_out": True, "termination_reason": "walltime"}),
            "rung_wall_exceeded",
        )
        self.assertEqual(
            cause(measured={"timed_out": False, "termination_reason": "memory"}),
            "memory_limit_exceeded",
        )
        self.assertEqual(cause(telemetry=None), "phase_telemetry_missing")
        self.assertEqual(
            cause(
                telemetry={"failure": {"cause": "count_mismatch", "step": "query", "detail": ""}}
            ),
            "count_mismatch",
        )
        # The 4 GiB envelope is the process peak, not BenchExec's cgroup ceiling.
        self.assertEqual(
            cause(telemetry={"failure": None, "peak_rss_bytes": 4 * 1024**3 + 1}),
            "memory_limit_exceeded",
        )
        self.assertIsNone(self.classify(telemetry={"failure": None, "peak_rss_bytes": 4 * 1024**3}))
        self.assertEqual(cause(swapped=True), "host_swapped")
        self.assertEqual(cause(document={"outcome": "exit"}), "benchexec_failed")


class FailedSampleDescriptionTests(unittest.TestCase):
    def test_each_distinct_error_lists_its_bindings_and_stays_bounded(self) -> None:
        def failed(binding: str, code: str, error: str) -> dict[str, Any]:
            return {"binding_id": binding, "status": "failed", "error_code": code, "error": error}

        evidence = {
            "variants": [
                {"query_id": "bfs", "samples": [failed("run-1", "GF_VALIDATION", "too big")] * 1},
                {
                    "query_id": "lcc",
                    "samples": [
                        failed("run-1", "GF_EXECUTION", "iteration limit"),
                        {"binding_id": "run-2", "status": "measured"},
                        failed("run-3", "GF_EXECUTION", "iteration limit"),
                    ],
                },
            ]
        }
        self.assertEqual(
            gdc_rung.describe_failed_samples(evidence),
            "bfs (run-1): GF_VALIDATION: too big; "
            "lcc (run-1, run-3): GF_EXECUTION: iteration limit",
        )
        evidence["variants"][0]["samples"][0]["error"] = "x" * 5000
        self.assertEqual(len(gdc_rung.describe_failed_samples(evidence)), 2048)
        self.assertEqual(gdc_rung.describe_failed_samples({"variants": []}), "")


class HostSwapDetailTests(unittest.TestCase):
    """A `host_swapped` phase names the counters that moved."""

    def test_the_detail_names_the_counters_that_rose(self) -> None:
        before = {"pswpin": 503592, "pswpout": 830053}
        self.assertEqual(
            gdc_rung.swap_detail(before, {"pswpin": 503594, "pswpout": 830053}),
            "host swap counters rose during the phase: pswpin +2 (503592 -> 503594); "
            "see host-swap.json",
        )
        self.assertEqual(
            gdc_rung.swap_detail(before, {"pswpin": 503592, "pswpout": 830060}),
            "host swap counters rose during the phase: pswpout +7 (830053 -> 830060); "
            "see host-swap.json",
        )


class ExpectedCountsTests(unittest.TestCase):
    def spec_for(self, suite: str, rung_id: str, ladder: str) -> Any:
        document = {
            "schema": "graphforge-gdc-scorecard-ladder-spec/1",
            "suite_id": suite,
            "description": "test",
            "profile_root": ".",
            "identity_profile": "scorecard",
            "counts_ladder": ladder,
            "metric_shape": "graphalytics" if suite == "graphalytics" else "queries",
            "variance_line": "test",
            "attribution": "CC-BY 4.0",
            "variances": [],
            "rungs": [
                {
                    "id": rung_id,
                    "label": rung_id,
                    "dataset_id": rung_id,
                    "input_subdir": None,
                    "workload": "fixtures/gdc/query-fixture/workload.json",
                    "refused": [],
                    "reference": None,
                    "reference_note": "test",
                    "variances": [],
                }
            ],
        }
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(document, handle)
        self.addCleanup(os.unlink, handle.name)
        return load_ladder_spec(ROOT, Path(handle.name))

    def test_graphalytics_reconciles_to_listed_edges_and_carries_the_discrepancy(self) -> None:
        spec = self.spec_for(
            "graphalytics", "cit-Patents", "profiles/gdc/graphalytics-scorecard-ladder.json"
        )
        counts = expected_counts(spec, "cit-Patents", None)
        self.assertEqual(
            (counts.expected["nodes"], counts.expected["edges"], counts.expected["types"]),
            (3774768, 16518947, {"EDGE": 16518947}),
        )
        self.assertEqual(counts.published, {"nodes": 3774768, "edges": 16518948})
        self.assertEqual([d["subject"] for d in counts.discrepancies], ["cit-Patents"])

    def test_every_real_ldbc_csv_rung_derives_counts_that_sum_to_its_tables(self) -> None:
        for suite in ("snb-bi", "snb-interactive", "finbench-transaction"):
            ladder_path = f"profiles/gdc/{suite}-scorecard-ladder.json"
            ladder = json.loads((ROOT / ladder_path).read_text())
            mapping = json.loads((ROOT / ladder["load_mapping"]).read_text())
            labelled = {t["id"]: t for t in mapping["node_tables"] if t.get("label_column")}
            for rung in ladder["rungs"]:
                spec = self.spec_for(suite, rung["id"], ladder_path)
                listed = {t["table"]: t["listed"] for t in rung["tables"]}
                # A converter manifest that puts every row of a column-labelled
                # table under its first mapped label.
                manifest = {
                    "outputs": [
                        {
                            "table": table,
                            "kind": "nodes",
                            "label": node["label"],
                            "rows": listed[table],
                            "labels": {next(iter(node["label_values"].values())): listed[table]},
                        }
                        for table, node in labelled.items()
                    ]
                }
                counts = expected_counts(spec, rung["id"], manifest)
                nodes = sum(t["listed"] for t in rung["tables"] if t["kind"] == "nodes")
                edges = sum(t["listed"] for t in rung["tables"] if t["kind"] == "edges")
                self.assertEqual(
                    (counts.expected["nodes"], counts.expected["edges"]), (nodes, edges)
                )
                self.assertEqual(sum(counts.expected["types"].values()), edges)
                snapshot = rung.get("published_snapshot_totals")
                if snapshot is not None and "snapshot_totals_discrepancy" not in rung:
                    self.assertEqual(counts.expected["nodes"], snapshot["nodes"])
                broken = json.loads(json.dumps(manifest))
                if broken["outputs"]:
                    first = broken["outputs"][0]
                    label = next(iter(first["labels"]))
                    first["labels"][label] -= 1
                    with self.assertRaises(RungInputError) as error:
                        expected_counts(spec, rung["id"], broken)
                    self.assertEqual(error.exception.cause, "count_mismatch")


class MatchingTests(unittest.TestCase):
    @staticmethod
    def result(columns: list[str], rows: list[list[Any]], ordered: bool = False) -> dict[str, Any]:
        return {
            "columns": [{"name": name, "type": "Utf8"} for name in columns],
            "rows": rows,
            "ordered": ordered,
        }

    def test_exact_respects_order_only_when_the_variant_is_ordered(self) -> None:
        rule = {"matching": "exact"}
        reference = {"columns": ["a"], "rows": [["1"], ["2"]]}
        self.assertTrue(matches(rule, self.result(["a"], [["2"], ["1"]]), reference))
        self.assertFalse(matches(rule, self.result(["a"], [["2"], ["1"]], ordered=True), reference))
        self.assertFalse(matches(rule, self.result(["b"], [["1"], ["2"]]), reference))
        self.assertFalse(matches(rule, self.result(["a"], [["1"], ["3"]]), reference))

    def test_epsilon_is_relative_and_pairs_rows_by_key(self) -> None:
        rule = {"matching": "epsilon", "epsilon": 1e-4, "key": ["id"]}
        reference = {"columns": ["id", "rank"], "rows": [["1", "0.5"], ["2", "Infinity"]]}
        close = self.result(
            ["node_uuid", "id", "rank"], [["u2", "2", "inf"], ["u1", "1", "0.50004"]]
        )
        self.assertTrue(matches(rule, close, reference))
        far = self.result(["id", "rank"], [["1", "0.5001"], ["2", "inf"]])
        self.assertFalse(matches(rule, far, reference))
        missing = self.result(["id", "rank"], [["1", "0.5"]])
        self.assertFalse(matches(rule, missing, reference))

    def test_equivalence_accepts_relabelled_partitions_only(self) -> None:
        rule = {"matching": "equivalence", "key": ["id"], "label": "component"}
        reference = {"columns": ["id", "component"], "rows": [["1", "7"], ["2", "7"], ["3", "9"]]}
        relabelled = self.result(
            ["id", "component", "name"], [["1", "0", "a"], ["2", "0", "b"], ["3", "1", "c"]]
        )
        self.assertTrue(matches(rule, relabelled, reference))
        merged = self.result(["id", "component"], [["1", "0"], ["2", "0"], ["3", "0"]])
        self.assertFalse(matches(rule, merged, reference))
        split = self.result(["id", "component"], [["1", "0"], ["2", "1"], ["3", "2"]])
        self.assertFalse(matches(rule, split, reference))

    def test_a_written_result_must_reproduce_the_measured_digest(self) -> None:
        columns = [{"name": "id", "type": "Int64"}]
        rows = [["1"], ["2"]]
        digest = result_digest(columns, rows, ordered=False)
        evidence = {
            "variants": [
                {
                    "query_id": "q",
                    "samples": [{"binding_id": "b", "status": "measured", "result_sha256": digest}],
                }
            ]
        }
        written = {
            "query_id": "q",
            "binding_id": "b",
            "ordered": False,
            "result_sha256": digest,
            "columns": columns,
            "rows": rows,
        }
        reference = {
            "source": "test",
            "queries": {
                "q": {"matching": "exact", "bindings": {"b": {"columns": ["id"], "rows": rows}}}
            },
        }
        passed = check_reference(
            reference=reference,
            reference_sha256=None,
            evidence=evidence,
            results={("q", "b"): written},
        )
        self.assertEqual((passed["status"], passed["checked"], passed["matched"]), ("passed", 1, 1))
        tampered = {**written, "rows": [["1"], ["3"]]}
        failed = check_reference(
            reference={
                **reference,
                "queries": {
                    "q": {
                        "matching": "exact",
                        "bindings": {"b": {"columns": ["id"], "rows": [["1"], ["3"]]}},
                    }
                },
            },
            reference_sha256=None,
            evidence=evidence,
            results={("q", "b"): tampered},
        )
        self.assertEqual([m["cause"] for m in failed["mismatches"]], ["result_digest_mismatch"])
        self.assertEqual(failed["status"], "failed")
        unmatched = check_reference(
            reference={**reference, "queries": {"other": reference["queries"]["q"]}},
            reference_sha256=None,
            evidence=evidence,
            results={("q", "b"): written},
        )
        self.assertEqual([m["cause"] for m in unmatched["mismatches"]], ["reference_unmatched"])

    def test_a_failed_referenced_sample_reports_its_error_text(self) -> None:
        evidence = {
            "variants": [
                {
                    "query_id": "q",
                    "samples": [
                        {
                            "binding_id": "b",
                            "status": "failed",
                            "cause": "query_failed",
                            "error_code": "GF_VALIDATION",
                            "error": "validation error: node selector topology scan exceeds row limit",
                        }
                    ],
                }
            ]
        }
        reference = {
            "source": "test",
            "queries": {
                "q": {"matching": "exact", "bindings": {"b": {"columns": ["id"], "rows": []}}}
            },
        }
        failed = check_reference(
            reference=reference, reference_sha256=None, evidence=evidence, results={}
        )
        self.assertEqual(
            failed["mismatches"],
            [
                {
                    "query_id": "q",
                    "binding_id": "b",
                    "cause": "query_failed",
                    "detail": "GF_VALIDATION: validation error: node selector topology scan "
                    "exceeds row limit",
                }
            ],
        )

    def test_results_written_twice_are_refused(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            document = {
                "schema": "graphforge-gdc-query-result/1",
                "query_id": "q",
                "binding_id": "b",
            }
            for name in ("00000000.json", "00000001.json"):
                (directory / name).write_text(json.dumps(document))
            with self.assertRaises(RungInputError):
                read_results(directory)


class ReclaimTests(unittest.TestCase):
    def test_reclaim_is_keyed_by_path_and_keeps_the_graph500_ladder(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            work = Path(raw)
            for name in ("s18", "gdc-snb-bi-sf1", "keep"):
                (work / "workspace" / name).mkdir(parents=True)
                (work / "workspace" / name / "data").write_bytes(b"x")
            reclaim_rung_workspace(work, 18)
            reclaim_workspace(work, "gdc-snb-bi-sf1")
            self.assertEqual(sorted(p.name for p in (work / "workspace").iterdir()), ["keep"])
            for name in ("..", ".", "", "a/b", "../keep"):
                with self.assertRaises(HostRunError) as error:
                    reclaim_workspace(work, name)
                self.assertEqual(str(error.exception), "workspace_name_invalid")
            (work / "workspace" / "linked").symlink_to(work / "workspace" / "keep")
            with self.assertRaises(HostRunError):
                reclaim_workspace(work, "linked")
            self.assertTrue((work / "workspace" / "keep" / "data").is_file())


class CardRenderingTests(unittest.TestCase):
    def card(self, **changes: Any) -> dict[str, Any]:
        card: dict[str, Any] = {
            "schema": "graphforge-gdc-scorecard-card/1",
            "suite_id": "graphalytics",
            "certification": False,
            "graphforge": {"version": "0.6.0", "commit": COMMIT},
            "dataset": {"rung_id": "cit-Patents", "label": "cit-Patents"},
            "fair_use_label": "These are not LDBC Benchmark Results.",
            "variance_line": "Analyst verbs stand in for the reference algorithms.",
            "hardware": {
                "label": "OVHC-AGENCY",
                "cores": 16,
                "memory_bytes": 125 * 1024**3 + 7,
                "storage_medium": "NVMe",
                "filesystem": "ext4",
                "os": "Ubuntu 26.04",
            },
            "graph": {
                "nodes": 3774768,
                "edges": 16518947,
                "published": {"nodes": 3774768, "edges": 16518948},
                "published_snapshot": None,
            },
            "load": {"wall_seconds": 61.25, "conversion_wall_seconds": 20.0},
            "on_disk_bytes": 3 * 1024**3,
            "coverage": {
                "supported": 4,
                "total": 6,
                "refused": [
                    {"query_id": "pr", "cause": "fixed_iteration_pagerank_not_exposed"},
                    {"query_id": "cdlp", "cause": "synchronous_cdlp_not_exposed"},
                ],
            },
            "throughput": None,
            "latency": None,
            "graphalytics": {
                "tl_seconds": 61.25,
                "makespan_not_measured": MAKESPAN_NOT_MEASURED,
                "algorithms": [
                    {
                        "algorithm": "bfs",
                        "tp_seconds": 2.0,
                        "runs": 3,
                        "evps": 10146857.5,
                        "makespan_seconds": None,
                    }
                ],
            },
            "peak_rss": {"load_bytes": 2 * 1024**3, "query_bytes": 3 * 1024**3},
            "correctness": {
                "status": "passed",
                "matched": 4,
                "checked": 4,
                "reference_source": "the archive's reference outputs",
                "reference_note": None,
            },
            "next_rung": {
                "label": "datagen-7_5-fb",
                "outcome": "not_admitted",
                "cause": "work_root_capacity_refused",
            },
            "variances": [
                {"kind": "discrepancy", "subject": "cit-Patents", "text": "One self-loop removed."}
            ],
            "attribution": (
                "LDBC Graphalytics (https://ldbcouncil.org/benchmarks/graphalytics/), CC-BY 4.0."
            ),
            "metric_sources": dict(CARD_METRIC_SOURCES),
            "evidence": {"graphalytics-cit-Patents-result.json": "0" * 64},
        }
        card.update(changes)
        return card

    def test_graphalytics_reports_tl_tp_makespan_and_evps_instead_of_latency(self) -> None:
        validator = Draft202012Validator(
            json.loads((ROOT / "schemas/gdc-scorecard-card.json").read_text())
        )
        card = self.card()
        self.assertEqual(list(validator.iter_errors(card)), [])
        text = render_card(card)
        self.assertIn(
            "Hardware:     OVHC-AGENCY — 16 cores, 125 GiB, NVMe ext4, Ubuntu 26.04\n", text
        )
        self.assertIn(
            "Graph:        3,774,768 nodes / 16,518,947 edges loaded (LDBC published: 3,774,768 / "
            "16,518,948; reconciled to the pinned archive's records, see variances)\n",
            text,
        )
        self.assertIn("Tp:           bfs 2.0 s (mean of 3 driver-clock runs)\n", text)
        self.assertIn(f"Makespan:     not measured ({MAKESPAN_NOT_MEASURED})\n", text)
        self.assertIn("EVPS:         bfs 1.01e+07\n", text)
        self.assertNotIn("Latency:", text)
        self.assertIn(
            "Coverage:     4/6 algorithms; refused: pr (fixed_iteration_pagerank_not_exposed); "
            "cdlp (synchronous_cdlp_not_exposed)\n",
            text,
        )
        self.assertIn(
            "Next rung:    datagen-7_5-fb — not admitted (work_root_capacity_refused)\n", text
        )
        self.assertIn("  - discrepancy (cit-Patents): One self-loop removed.\n", text)
        # The schema keeps Graphalytics' slot and refuses latency on its card.
        latency = {"p50_ns": 1, "p95_ns": 2, "samples": 1}
        self.assertNotEqual(list(validator.iter_errors(self.card(latency=latency))), [])

    def test_a_card_claiming_another_authority_or_no_label_is_refused(self) -> None:
        sources = {**CARD_METRIC_SOURCES, "latency": "benchexec"}
        with self.assertRaises(GdcMeasurementBoundaryError) as error:
            render_card(self.card(metric_sources=sources))
        self.assertEqual(error.exception.cause, "misattributed_authority")
        with self.assertRaises(CardError):
            render_card(self.card(fair_use_label="LDBC results"))


if __name__ == "__main__":
    unittest.main()
