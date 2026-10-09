"""The FinBench Transaction scorecard through the GDC rung runner (#1909).

The tiny rung fixture (``fixtures/gdc/finbench-scorecard-fixture``) is the
FinBench query fixture's graph and bindings in the shape LDBC publishes them:
``snapshot/`` CSV, ``complex_<n>_param.csv`` and archives. The real converter,
``gf``, the query driver and the committed production load mapping and Cypher
run every phase; only BenchExec is replaced (``tests.test_gdc_rung``). The
reference is derived from the fixture's CSV by the spec-derived module, which
never runs GraphForge, and pinned by SHA-256 in the dataset cache like the SF1
reference, so a mutated reference, workload or mapping must change an outcome.
"""

from __future__ import annotations

import copy
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any
import unittest
from unittest.mock import patch

from graphforge_bench import gdc_finbench_transaction_scorecard as scorecard
from graphforge_bench import gdc_rung
from graphforge_bench.gdc_dataset_cache import extract_archive
from graphforge_bench.gdc_finbench_transaction_reference import (
    derive_ldbc_reference,
    read_ldbc_parameters,
)
from graphforge_bench.gdc_rung_inputs import load_ladder_spec, sha256_file
from graphforge_bench.gdc_scorecard_card import build_card, render_card, write_card

from tests import finbench_scorecard_fixture as fixture
from tests.test_gdc_finbench_transaction import _ensure_runner_built
from tests.test_gdc_rung import COMMIT, QUIET, FakeBenchExec
from tests.test_gdc_scorecard_load import converter_binary, gf_binary

ROOT = fixture.ROOT
PRODUCTION = ROOT / "profiles" / "gdc"
QUERIES = PRODUCTION / "finbench-transaction-scorecard-queries.json"
MAPPING = PRODUCTION / "finbench-transaction-load-mapping.json"
LADDER_SPEC = PRODUCTION / "finbench-transaction-scorecard-ladder-spec.json"
SCRATCH_PARENT = ROOT / "fixtures" / "gdc"
WRITE_CAUSE = "finbench_transaction_write_semantics_not_exposed"


def serve_archives(url: str) -> Any:
    name = url.rsplit("/", 1)[1]
    return io.BytesIO((fixture.FIXTURE / "archives" / name).read_bytes())


class _Opened(io.BytesIO):
    def __enter__(self) -> _Opened:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def opener(url: str) -> _Opened:
    return _Opened(serve_archives(url).getvalue())


def tree(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


class FixtureTests(unittest.TestCase):
    def test_committed_source_is_the_rendered_query_fixture(self) -> None:
        self.assertEqual(tree(fixture.FIXTURE / "source"), fixture.render())

    def test_committed_archives_hold_exactly_the_source(self) -> None:
        with tempfile.TemporaryDirectory(prefix="finbench-archives-") as scratch:
            for name, folder in (
                ("finbench-fixture-sf1.tar.zst", "sf1"),
                ("finbench-fixture-read-params.zip", fixture.PARAMETER_DIR),
            ):
                target = extract_archive(
                    fixture.FIXTURE / "archives" / name, Path(scratch) / folder
                )
                expected = {
                    path: text
                    for path, text in tree(fixture.FIXTURE / "source").items()
                    if path.startswith(f"{folder}/")
                }
                self.assertEqual(tree(target), expected)

    def test_the_fixture_identity_pins_its_archives(self) -> None:
        identity = json.loads(
            (fixture.FIXTURE / "profiles" / "gdc" / "finbench-fixture-identity.json").read_text()
        )
        pins = {
            item["source"].rsplit("/", 1)[1]: item["checksum_sha256"]
            for item in identity["datasets"]
        }
        for name, digest in pins.items():
            self.assertEqual(sha256_file(fixture.FIXTURE / "archives" / name), digest, name)


class WorkloadTests(unittest.TestCase):
    def setUp(self) -> None:
        self.queries = scorecard.read_queries(QUERIES)
        self.params = fixture.FIXTURE / "source" / fixture.PARAMETER_DIR

    def test_binding_ids_are_the_reference_modules_line_ids(self) -> None:
        workload = scorecard.build_workload(self.queries, self.params)
        published = read_ldbc_parameters(self.params)
        self.assertEqual([v["id"] for v in workload["variants"]], list(published))
        for variant in workload["variants"]:
            self.assertEqual(
                [binding["id"] for binding in variant["bindings"]],
                [binding_id for binding_id, _ in published[variant["id"]]],
            )
        # complex_3 has no placeholder line, so its first binding is line-1.
        by_id = {v["id"]: v for v in workload["variants"]}
        self.assertEqual(by_id["TCR3"]["bindings"][0]["id"], "line-1")
        self.assertEqual(by_id["TCR1"]["bindings"][0]["id"], "line-2")

    def test_parameters_keep_their_types_and_drop_the_truncation_order(self) -> None:
        workload = scorecard.build_workload(self.queries, self.params)
        tcr6 = next(v for v in workload["variants"] if v["id"] == "TCR6")
        params = tcr6["bindings"][0]["params"]
        self.assertEqual(
            sorted(params),
            ["endTime", "id", "startTime", "threshold1", "threshold2", "truncationLimit"],
        )
        self.assertEqual(params["threshold1"], {"type": "Float", "value": 100.0})
        self.assertEqual(params["startTime"], {"type": "Int", "value": 1000})

    def test_a_truncation_order_the_cypher_does_not_implement_is_refused(self) -> None:
        with tempfile.TemporaryDirectory(prefix="finbench-params-") as scratch:
            shutil.copytree(self.params, scratch, dirs_exist_ok=True)
            path = Path(scratch) / "complex_1_param.csv"
            path.write_text(
                path.read_text().replace("TIMESTAMP_DESCENDING", "TIMESTAMP_ASCENDING", 1)
            )
            with self.assertRaises(scorecard.ScorecardInputError) as raised:
                scorecard.build_workload(self.queries, Path(scratch))
        self.assertEqual(raised.exception.cause, "truncation_order_not_supported")

    def test_the_committed_load_mapping_stores_what_the_cypher_reads(self) -> None:
        mapping = json.loads(MAPPING.read_text())
        self.assertEqual(scorecard.mapping_drift(self.queries, mapping), [])
        # The Cypher reads these; a rename or a type change is drift.
        self.assertIn("timestamp", scorecard.cypher_properties(self.queries))

    def test_mapping_drift_is_detected_for_the_name_and_the_type(self) -> None:
        mapping = json.loads(MAPPING.read_text())
        renamed = copy.deepcopy(mapping)
        for prop in renamed["edge_tables"][1]["properties"]:
            if prop.get("name") == "timestamp":
                del prop["name"]  # the column's own name, createTime
        retyped = copy.deepcopy(mapping)
        for table in retyped["edge_tables"]:
            for prop in table["properties"]:
                if prop.get("name") == "timestamp":
                    prop.update(type="datetime")
        for case, drifted in (("renamed", renamed), ("retyped", retyped)):
            drift = scorecard.mapping_drift(self.queries, drifted)
            self.assertTrue(drift, case)
            self.assertIn("timestamp", drift[0], case)


class CatalogTests(unittest.TestCase):
    """The committed Cypher is the Rust catalog's (the runner is built from this tree)."""

    @classmethod
    def setUpClass(cls) -> None:
        import os

        from graphforge_bench.gdc_finbench_transaction import list_query_catalog

        os.environ["GRAPHFORGE_GDC_FINBENCH_TRANSACTION_BIN"] = str(_ensure_runner_built(ROOT))
        cls.catalog = list_query_catalog()

    def test_committed_queries_are_the_catalogs(self) -> None:
        committed = json.loads(QUERIES.read_text())
        self.assertEqual(scorecard.catalog_drift(committed, self.catalog), [])
        self.assertEqual(committed, scorecard.queries_from_catalog(self.catalog))

    def test_a_changed_query_is_drift(self) -> None:
        committed = json.loads(QUERIES.read_text())
        committed["queries"][0]["cypher"] += " "
        self.assertEqual(scorecard.catalog_drift(committed, self.catalog), ["TCR1 cypher differs"])


class Scratch(unittest.TestCase):
    """A profile tree in the repository (profile_root is repository-relative) and a cache."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.executables = gdc_rung.GdcExecutables(
            gf=gf_binary(), driver=converter_binary(), benchexec_python=Path(sys.executable)
        )

    def setUp(self) -> None:
        swap = patch.object(
            gdc_rung, "_host_swap_counters", return_value={"pswpin": 0, "pswpout": 0}
        )
        swap.start()
        self.addCleanup(swap.stop)
        scratch = tempfile.TemporaryDirectory(prefix="finbench-rung-")
        self.addCleanup(scratch.cleanup)
        self.scratch = Path(scratch.name)
        self.work = self.scratch / "work"
        self.work.mkdir()
        self.cache = self.scratch / "cache"
        self.output = self.scratch / "evidence"
        self.profile_root = Path(tempfile.mkdtemp(prefix="scratch-finbench-", dir=SCRATCH_PARENT))
        self.addCleanup(shutil.rmtree, self.profile_root, ignore_errors=True)
        for name in ("profiles", "suites"):
            shutil.copytree(fixture.FIXTURE / name, self.profile_root / name)
        profiles = self.profile_root / "profiles" / "gdc"
        # The production mapping and Cypher, never copies that can drift.
        shutil.copy(MAPPING, profiles / MAPPING.name)
        shutil.copy(QUERIES, profiles / QUERIES.name)
        self.reference = self.derive_reference()

    def derive_reference(self) -> dict[str, Any]:
        source = fixture.FIXTURE / "source"
        return derive_ldbc_reference(
            source / "sf1" / "snapshot", source / fixture.PARAMETER_DIR, "sf1"
        )

    def pin_reference(self, reference: dict[str, Any]) -> dict[str, str]:
        path = self.cache / "finbench-reference" / "sf1-reference.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(reference, indent=1) + "\n", encoding="utf-8")
        return {"cache_path": "finbench-reference/sf1-reference.json", "sha256": sha256_file(path)}

    def rung(
        self, rung_id: str, pin: dict[str, str] | None, *, dataset: str = "finbench-fixture-sf1"
    ) -> dict[str, Any]:
        rung: dict[str, Any] = {
            "id": rung_id,
            "label": rung_id.upper(),
            "dataset_id": dataset,
            "input_subdir": None,
            "workload": {
                "builder": "finbench-transaction",
                "queries": f"profiles/gdc/{QUERIES.name}",
                "parameters": {
                    "dataset_id": "finbench-fixture-read-params",
                    "path": fixture.PARAMETER_DIR,
                },
            },
            "refused": [{"query_id": "TSR1", "cause": "no_published_parameters"}],
            "reference": pin,
            "variances": [],
        }
        if pin is None:
            rung["reference_note"] = "the fixture derives an SF1 reference only"
        return rung

    def ladder(self, rungs: list[dict[str, Any]], *, drifted: bool = False) -> gdc_rung.Ladder:
        production = json.loads(LADDER_SPEC.read_text())
        spec = {
            **production,
            "profile_root": self.profile_root.relative_to(ROOT).as_posix(),
            "counts_ladder": "profiles/gdc/finbench-fixture-scorecard-ladder.json",
            "rungs": rungs,
        }
        path = self.scratch / "ladder-spec.json"
        path.write_text(json.dumps(spec, indent=2), encoding="utf-8")
        return gdc_rung.prepare(
            root=ROOT,
            ladder_path=path,
            output_dir=self.output,
            work_root=self.work,
            cache_root=self.cache,
            executables=self.executables,
            commit=COMMIT,
            host_label="CI-RUNNER",
            storage_medium="SSD",
            opener=opener,
            benchexec=FakeBenchExec(),
        )

    def climb(self, ladder: gdc_rung.Ladder, **kwargs: Any) -> list[dict[str, Any]]:
        return gdc_rung.climb(
            ladder,
            reserved_headroom_bytes=0,
            quiet_host_wait_seconds=0,
            quiet_host=lambda _wait: QUIET,
            **kwargs,
        )

    def correctness(self, rung_id: str = "sf1") -> dict[str, Any]:
        path = self.output / f"finbench-transaction-{rung_id}-correctness.json"
        return json.loads(path.read_text())


class TinyLadderTests(Scratch):
    def test_the_ladder_passes_sf1_records_the_unpinned_rung_and_continues(self) -> None:
        pin = self.pin_reference(self.reference)
        ladder = self.ladder(
            [
                self.rung("sf1", pin),
                {"id": "sf2", "label": "SF2", "not_pinned": "the fixture pins SF1 and SF3 only"},
                self.rung("sf3", None, dataset="finbench-fixture-sf3"),
            ]
        )
        results = self.climb(ladder)

        self.assertEqual([r["status"] for r in results], ["passed", "not_admitted", "passed"])
        self.assertEqual(results[1]["failure"]["cause"], "rung_not_pinned")
        correctness = self.correctness("sf1")
        bindings = sum(len(rule["bindings"]) for rule in self.reference["queries"].values())
        self.assertEqual(correctness["status"], "passed")
        self.assertEqual((correctness["checked"], correctness["matched"]), (bindings, bindings))
        self.assertEqual(correctness["unchecked"], 0)
        self.assertEqual(correctness["reference_sha256"], pin["sha256"])
        self.assertEqual(set(correctness["queries"]), {f"TCR{n}" for n in range(1, 13)})
        self.assertTrue(
            all(
                tally["checked"] == tally["matched"] > 0
                for tally in correctness["queries"].values()
            )
        )
        # The higher rung runs the same queries but is not reference-checked.
        self.assertEqual(self.correctness("sf3")["status"], "not_reference_checked")

        text = write_card(ROOT, ladder.spec, self.output)["text"].read_text()
        self.assertIn("SF3", text.splitlines()[0])
        self.assertIn("not reference-checked", text)
        self.assertIn("SF2 not pinned", text)
        self.assertIn("12/13 queries; refused: TSR1 (no_published_parameters)", text)

    def test_the_card_of_a_reference_checked_rung_states_what_it_checked_against(self) -> None:
        pin = self.pin_reference(self.reference)
        ladder = self.ladder([self.rung("sf1", pin)])
        results = self.climb(ladder)
        self.assertEqual([r["status"] for r in results], ["passed"])
        card = build_card(ladder.spec, self.output)
        bindings = sum(len(rule["bindings"]) for rule in self.reference["queries"].values())
        self.assertEqual(card["correctness"]["matched"], bindings)
        self.assertEqual(card["coverage"]["supported"], 12)
        self.assertEqual(card["coverage"]["total"], 13)  # TSR1 is refused in this spec
        text = render_card(card)
        self.assertIn("spec-derived reference", text)
        self.assertIn(f"{bindings}/{bindings} supported results match", text)
        self.assertIn("12/13 queries; refused: TSR1 (no_published_parameters)", text)
        self.assertIn("unaudited", text.splitlines()[0])
        self.assertIn("These are not LDBC Benchmark Results.", text)

    def failed(self, results: list[dict[str, Any]]) -> dict[str, Any]:
        self.assertEqual([r["status"] for r in results], ["failed"])
        return results[0]


class ReferenceMutationTests(Scratch):
    """The reference check fails when the answer, the binding set or the windows change."""

    def run_sf1(self, reference: dict[str, Any]) -> dict[str, Any]:
        ladder = self.ladder([self.rung("sf1", self.pin_reference(reference))])
        results = self.climb(ladder)
        self.assertEqual([r["status"] for r in results], ["failed"])
        return results[0]

    def causes(self, result: dict[str, Any]) -> list[tuple[str, str]]:
        return [(f["cause"], f["detail"].split(":")[0]) for f in result["failures"]]

    def test_a_wrong_cell_is_exactly_one_reference_mismatch(self) -> None:
        wrong = copy.deepcopy(self.reference)
        binding_id, expected = next(
            (binding_id, expected)
            for binding_id, expected in wrong["queries"]["TCR2"]["bindings"].items()
            if expected["rows"]
        )
        expected["rows"][0][-1] = "-1.000"
        result = self.run_sf1(wrong)
        self.assertEqual(self.causes(result), [("reference_mismatch", f"TCR2/{binding_id}")])
        correctness = self.correctness()
        self.assertEqual(correctness["status"], "failed")
        self.assertEqual(correctness["checked"] - correctness["matched"], 1)

    def test_a_binding_the_workload_never_ran_is_unmatched(self) -> None:
        extra = copy.deepcopy(self.reference)
        bindings = extra["queries"]["TCR1"]["bindings"]
        bindings["line-999"] = copy.deepcopy(next(iter(bindings.values())))
        result = self.run_sf1(extra)
        self.assertEqual(self.causes(result), [("reference_unmatched", "TCR1/line-999")])

    def test_a_binding_the_reference_lacks_is_a_mismatch_not_unchecked(self) -> None:
        short = copy.deepcopy(self.reference)
        binding_id = next(iter(short["queries"]["TCR7"]["bindings"]))
        del short["queries"]["TCR7"]["bindings"][binding_id]
        result = self.run_sf1(short)
        self.assertEqual(self.causes(result), [("reference_binding_missing", f"TCR7/{binding_id}")])

    def test_a_query_the_reference_lacks_is_a_mismatch_for_each_binding(self) -> None:
        short = copy.deepcopy(self.reference)
        dropped = len(short["queries"].pop("TCR12")["bindings"])
        result = self.run_sf1(short)
        self.assertEqual({cause for cause, _ in self.causes(result)}, {"reference_binding_missing"})
        self.assertEqual(len(result["failures"]), dropped)

    def test_a_reference_other_than_the_pinned_bytes_is_refused(self) -> None:
        pin = self.pin_reference(self.reference)
        path = self.cache / pin["cache_path"]
        path.write_text(path.read_text().replace("TCR1", "TCR1 ", 1))
        ladder = self.ladder([self.rung("sf1", pin)])
        result = self.climb(ladder)[0]
        self.assertEqual(self.causes(result)[0][0], "reference_digest_mismatch")

    def test_a_reference_missing_from_the_cache_is_refused(self) -> None:
        pin = self.pin_reference(self.reference)
        (self.cache / pin["cache_path"]).unlink()
        result = self.climb(self.ladder([self.rung("sf1", pin)]))[0]
        self.assertEqual(self.causes(result)[0][0], "reference_missing")

    def drifted_mapping(self, *, retype: bool) -> None:
        path = self.profile_root / "profiles" / "gdc" / MAPPING.name
        mapping = json.loads(path.read_text())
        for table in mapping["edge_tables"]:
            for prop in table["properties"]:
                if prop.get("name") == "timestamp":
                    if retype:
                        prop["type"] = "datetime"  # the name stays; the value is a datetime
                    else:
                        del prop["name"]  # the type stays; the name is createTime
        path.write_text(json.dumps(mapping, indent=2), encoding="utf-8")

    def assert_mapping_drift_fails_before_any_query(self) -> None:
        ladder = self.ladder([self.rung("sf1", self.pin_reference(self.reference))])
        result = self.climb(ladder)[0]
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["failure"]["cause"], "mapping_drift")
        self.assertIn("timestamp", result["failure"]["detail"])
        self.assertEqual(sorted(result["phases"]), [])  # not even the conversion ran

    def test_a_timestamp_retyped_in_the_mapping_fails_the_rung_before_any_query(self) -> None:
        self.drifted_mapping(retype=True)
        self.assert_mapping_drift_fails_before_any_query()

    def test_a_timestamp_renamed_in_the_mapping_fails_the_rung_before_any_query(self) -> None:
        self.drifted_mapping(retype=False)
        self.assert_mapping_drift_fails_before_any_query()

    def test_a_window_start_that_became_inclusive_fails_against_the_reference(self) -> None:
        """The bounds are open (`startTime < ts`): an inclusive start selects other edges."""
        path = self.profile_root / "profiles" / "gdc" / QUERIES.name
        queries = json.loads(path.read_text())
        changed = 0
        for query in queries["queries"]:
            changed += query["cypher"].count("$startTime < ")
            query["cypher"] = query["cypher"].replace("$startTime < ", "$startTime <= ")
        self.assertGreater(changed, 0)
        path.write_text(json.dumps(queries, indent=2), encoding="utf-8")
        result = self.run_sf1(self.reference)
        self.assertEqual({cause for cause, _ in self.causes(result)}, {"reference_mismatch"})
        correctness = self.correctness()
        missed = correctness["checked"] - correctness["matched"]
        self.assertGreater(missed, 0)
        self.assertLess(missed, correctness["checked"])  # only windows with an edge on the bound


class LadderSpecTests(unittest.TestCase):
    def test_the_production_spec_loads_and_declares_the_unpinned_rung(self) -> None:
        spec = load_ladder_spec(ROOT, LADDER_SPEC)
        rungs = {rung["id"]: rung for rung in spec.document["rungs"]}
        self.assertEqual(list(rungs), ["sf1", "sf3", "sf10"])
        self.assertIn("not_pinned", rungs["sf3"])
        sf1 = rungs["sf1"]
        self.assertEqual(
            sf1["reference"],
            {
                "cache_path": "finbench-reference/sf1-reference.json",
                "sha256": "9c73fb5baa71136696fc598f42852505595b1f2b9ea25ae8ae8e5644ec0f822a",
            },
        )
        self.assertIsNone(rungs["sf10"]["reference"])
        refused = {item["query_id"]: item["cause"] for item in sf1["refused"]}
        self.assertEqual(
            {name for name, cause in refused.items() if cause == "no_published_parameters"},
            {f"TSR{n}" for n in range(1, 7)},
        )
        self.assertEqual(len(refused), 28)
        self.assertEqual(set(refused.values()) - {"no_published_parameters"}, {WRITE_CAUSE})
        readings = " ".join(f"{v['subject']} {v['text']}" for v in spec.document["variances"])
        for stated in ("spec-derived", "TCR6", "TCR11", "truncation", "GPStore"):
            self.assertIn(stated, readings)

    def test_the_pinned_parameter_datasets_are_in_the_scorecard_identity(self) -> None:
        spec = load_ladder_spec(ROOT, LADDER_SPEC)
        identity = json.loads(
            (PRODUCTION / "finbench-transaction-scorecard-identity.json").read_text()
        )
        datasets = {item["id"]: item for item in identity["datasets"]}
        for rung in spec.document["rungs"]:
            if "not_pinned" in rung:
                continue
            self.assertEqual(datasets[rung["dataset_id"]]["role"], "dataset")
            self.assertEqual(
                datasets[rung["workload"]["parameters"]["dataset_id"]]["role"], "parameter"
            )


if __name__ == "__main__":
    unittest.main()
