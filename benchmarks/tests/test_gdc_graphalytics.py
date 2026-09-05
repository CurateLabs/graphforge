from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from graphforge_bench.gdc_contracts import (
    GdcContractError,
    list_gdc_suites,
    load_pinned_identity,
    validate_acquisition,
    workspace_root,
)
from graphforge_bench.gdc_graphalytics import (
    ALGORITHMS,
    EVIDENCE_SCHEMA,
    GraphalyticsSuiteError,
    assert_identity_profiles_are_separated,
    assert_separate_from_graph500,
    identity_path,
    list_algorithm_rules,
    load_ladder,
    map_job_file,
    ordered_dataset_ids,
    run_tiny_live_suite,
    run_tiny_suite,
)
from jsonschema import Draft202012Validator


def _ensure_runner_built(root: Path) -> Path:
    binary = root / "target" / "debug" / "graphforge-benchmark-gdc-graphalytics"
    if binary.is_file():
        return binary
    target_dir = root / "target"
    completed = subprocess.run(
        [
            "cargo",
            "build",
            "--locked",
            "--manifest-path",
            str(root / "Cargo.toml"),
            "-p",
            "graphforge-benchmark-gdc-graphalytics",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "CARGO_TARGET_DIR": str(target_dir)},
    )
    if completed.returncode != 0:
        raise AssertionError(
            f"failed to build graphalytics runner\n{completed.stdout}\n{completed.stderr}"
        )
    assert binary.is_file(), binary
    return binary


class GdcGraphalyticsSuiteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = workspace_root()
        cls.binary = _ensure_runner_built(cls.root)
        os.environ["GRAPHFORGE_GDC_GRAPHALYTICS_BIN"] = str(cls.binary)

    def test_suite_declaration_uses_graphalytics_runner(self) -> None:
        suites = {suite["suite_id"]: suite for suite in list_gdc_suites()}
        suite = suites["graphalytics"]
        self.assertEqual(suite["runner"], "gdc-graphalytics")
        self.assertEqual(suite["disposition"], "executable")
        self.assertEqual(suite["datasets"][0], "ga-tiny")
        self.assertEqual(suite["pinned_identity"], "profiles/gdc/graphalytics-live-identity.json")
        self.assertEqual(
            suite["identity_profiles"],
            {
                "static": "profiles/gdc/graphalytics-static-identity.json",
                "live": "profiles/gdc/graphalytics-live-identity.json",
            },
        )
        self.assertNotIn("graph500", json.dumps(suite))
        assert_identity_profiles_are_separated()

    def test_ordered_ladder_begins_with_bounded_fixture(self) -> None:
        self.assertEqual(load_ladder()["datasets"][0]["id"], "ga-tiny")
        self.assertEqual(ordered_dataset_ids()[0], "ga-tiny")
        self.assertIn("wiki-Talk", ordered_dataset_ids())
        assert_separate_from_graph500()

    def test_all_six_algorithms_declare_mapping_and_tolerance_rules(self) -> None:
        rules = list_algorithm_rules()
        self.assertEqual(set(rules), set(ALGORITHMS))
        self.assertEqual(rules["bfs"]["validation"], "exact")
        self.assertEqual(rules["cdlp"]["validation"], "exact")
        self.assertEqual(rules["wcc"]["validation"], "equivalence")
        self.assertEqual(rules["pr"]["validation"], "epsilon")
        self.assertEqual(rules["lcc"]["validation"], "epsilon")
        self.assertEqual(rules["sssp"]["validation"], "epsilon")
        for algorithm in ALGORITHMS:
            self.assertTrue(rules[algorithm]["determinism"])

    def test_compatible_fixture_passes_or_fails_closed_per_algorithm(self) -> None:
        evidence = run_tiny_suite(fixture_name="compatible")
        self.assertEqual(evidence["schema"], EVIDENCE_SCHEMA)
        self.assertEqual(evidence["suite_id"], "graphalytics")
        self.assertEqual(evidence["dataset_id"], "ga-tiny")
        self.assertFalse(evidence["certification"])
        self.assertEqual(evidence["execution_mode"], "static_replay")
        self.assertEqual(evidence["status"], "passed")
        self.assertIn("spec", evidence["identities"])
        self.assertIn("driver", evidence["identities"])
        by_key = {item["workload_key"]: item for item in evidence["algorithms"]}
        self.assertEqual(set(by_key), set(ALGORITHMS))
        for key in ("bfs", "wcc", "sssp"):
            self.assertEqual(by_key[key]["status"], "passed", key)
            self.assertIsNotNone(by_key[key].get("public_api"))
        for key in ("pr", "cdlp", "lcc"):
            self.assertEqual(by_key[key]["status"], "semantic_incompatibility", key)
            self.assertIsNotNone(by_key[key].get("cause"))
        Draft202012Validator(
            json.loads(
                (self.root / "schemas" / "gdc-graphalytics-evidence.json").read_text(
                    encoding="utf-8"
                )
            )
        ).validate(evidence)

    def test_unsupported_semantics_fail_visibly(self) -> None:
        pr_job = (
            self.root / "fixtures" / "gdc" / "graphalytics-tiny" / "compatible" / "jobs" / "pr.json"
        )
        with self.assertRaises(GraphalyticsSuiteError) as raised:
            map_job_file(pr_job)
        self.assertEqual(raised.exception.cause, "semantic_incompatibility")
        self.assertIn("fixed_iteration_pagerank_not_exposed", str(raised.exception))

        cdlp_job = pr_job.with_name("cdlp.json")
        with self.assertRaises(GraphalyticsSuiteError) as raised_cdlp:
            map_job_file(cdlp_job)
        self.assertEqual(raised_cdlp.exception.cause, "semantic_incompatibility")
        self.assertIn("synchronous_cdlp_not_exposed", str(raised_cdlp.exception))

        lcc_job = pr_job.with_name("lcc.json")
        with self.assertRaises(GraphalyticsSuiteError) as raised_lcc:
            map_job_file(lcc_job)
        self.assertEqual(raised_lcc.exception.cause, "semantic_incompatibility")
        self.assertIn("directed_lcc_semantics_not_exposed", str(raised_lcc.exception))

    def test_live_public_api_executes_tiny_fixture_and_fails_closed(self) -> None:
        evidence = run_tiny_live_suite()
        self.assertFalse(evidence["certification"])
        self.assertEqual(evidence["execution_mode"], "live_public_api")
        self.assertEqual(evidence["status"], "passed")
        self.assertEqual(evidence["identities"]["spec"]["release"], "v1.0.5")
        self.assertEqual(
            evidence["identities"]["spec"]["commit"],
            "5cf6ae65d26c809f2e3e0dac4716f153c71dc639",
        )
        self.assertIsNone(evidence["identities"]["generator"]["commit"])
        self.assertIsNone(evidence["identities"]["driver"]["commit"])
        self.assertEqual(evidence["identities"]["datasets"][0]["id"], "ga-tiny")
        by_key = {item["workload_key"]: item for item in evidence["algorithms"]}
        for key in ("bfs", "wcc", "sssp"):
            self.assertEqual(by_key[key]["status"], "passed", key)
            self.assertIsNotNone(by_key[key]["public_api"], key)
        for key in ("pr", "cdlp", "lcc"):
            self.assertEqual(by_key[key]["status"], "semantic_incompatibility", key)
        Draft202012Validator(
            json.loads(
                (self.root / "schemas" / "gdc-graphalytics-evidence.json").read_text(
                    encoding="utf-8"
                )
            )
        ).validate(evidence)

    def test_reference_mismatch_is_visible(self) -> None:
        evidence = run_tiny_suite(fixture_name="reference-mismatch")
        by_key = {item["workload_key"]: item for item in evidence["algorithms"]}
        self.assertEqual(by_key["bfs"]["status"], "failed")
        self.assertIn("reference_mismatch", by_key["bfs"]["cause"])
        self.assertEqual(evidence["status"], "failed")

    def test_live_inputs_are_bound_to_pinned_context(self) -> None:
        from graphforge_bench.gdc_graphalytics import _validated_provenance

        source = self.root / "fixtures" / "gdc" / "graphalytics-tiny" / "compatible"
        for mutation, cause in (
            ("unchanged", None),
            ("reference", "live_asset_checksum_mismatch"),
            ("edges", "live_asset_checksum_mismatch"),
            ("identity", "live_identity_mismatch"),
            ("job", "live_job_context_mismatch"),
            ("duplicates", "each of the six algorithms exactly once"),
        ):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as tmp:
                fixture = Path(tmp) / "fixture"
                shutil.copytree(source, fixture)
                identities = Path(tmp) / "identities.json"
                provenance = _validated_provenance(self.root, source)
                if mutation == "identity":
                    provenance["spec"]["release"] = "forged"
                identities.write_text(json.dumps(provenance), encoding="utf-8")
                if mutation == "reference":
                    (fixture / "references/ga-tiny-bfs.ref").write_text("1 99\n")
                elif mutation == "edges":
                    (fixture / "ga-tiny.edges").write_text("1 2 99\n")
                elif mutation == "job":
                    path = fixture / "jobs/bfs.json"
                    job = json.loads(path.read_text())
                    job["source_vertex"] = 2
                    path.write_text(json.dumps(job))
                elif mutation == "duplicates":
                    bfs = (fixture / "jobs/bfs.json").read_text()
                    for path in (fixture / "jobs").glob("*.json"):
                        path.write_text(bfs)
                evidence = Path(tmp) / "evidence.json"
                completed = subprocess.run(
                    [
                        str(self.binary),
                        "run-live",
                        str(fixture / "ga-tiny.edges"),
                        str(fixture / "jobs"),
                        str(fixture / "references"),
                        str(identities),
                        str(evidence),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                if cause is None:
                    self.assertEqual(completed.returncode, 0, completed.stderr)
                    self.assertEqual(len(json.loads(evidence.read_text())["algorithms"]), 6)
                else:
                    self.assertNotEqual(completed.returncode, 0)
                    self.assertIn(cause, completed.stderr)
                    self.assertFalse(evidence.exists())

    def test_evidence_requires_all_six_distinct_outcomes(self) -> None:
        from graphforge_bench.gdc_graphalytics import _validate_evidence_envelope

        evidence = {
            "schema": "graphforge-gdc-graphalytics-evidence/1",
            "execution_mode": "live_public_api",
            "certification": False,
        }
        for outcomes in ([], [{"workload_key": "bfs"}] * 6):
            with self.subTest(outcomes=outcomes), self.assertRaises(GraphalyticsSuiteError):
                _validate_evidence_envelope({**evidence, "algorithms": outcomes}, "live_public_api")

    def test_static_output_directory_cannot_be_supplied_to_live_command(self) -> None:
        fixture = self.root / "fixtures" / "gdc" / "graphalytics-tiny" / "compatible"
        completed = subprocess.run(
            [
                str(self.binary),
                "run-live",
                str(fixture / "ga-tiny.edges"),
                str(fixture / "jobs"),
                str(fixture / "references"),
                str(fixture / "system-outputs"),
                str(fixture / "acquisition.json"),
                str(fixture / "evidence.json"),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(completed.returncode, 2)
        self.assertIn("usage: run-live", completed.stderr)

    def test_ga_tiny_identity_checksum_mismatch_fails_before_execution(self) -> None:
        fixture = self.root / "fixtures" / "gdc" / "graphalytics-tiny" / "compatible"
        pin = load_pinned_identity(identity_path("live", self.root))
        acquisition = json.loads((fixture / "acquisition.json").read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as tmp:
            copy = Path(tmp)
            shutil.copytree(fixture, copy / "fixture")
            copied_fixture = copy / "fixture"
            (copied_fixture / "ga-tiny.edges").write_text("1 2\n", encoding="utf-8")
            with self.assertRaises(GdcContractError) as raised:
                validate_acquisition(pin, acquisition, copied_fixture)
        self.assertEqual(raised.exception.cause, "checksum_mismatch")

    def test_ga_tiny_pr_reference_is_official_two_iteration_vector(self) -> None:
        expected = _official_graphalytics_pr_vector()
        formatted = _format_graphalytics_float_reference(expected)
        digest = hashlib.sha256(formatted.encode("utf-8")).hexdigest()
        self.assertNotEqual(
            formatted,
            "1 0.25\n2 0.25\n3 0.25\n4 0.25\n",
            "PR_0=1/|V| must not be stored as the 2-iteration official vector",
        )
        pin = load_pinned_identity(identity_path("live", self.root))
        pinned_pr = next(item for item in pin["references"] if item["workload_key"] == "pr")
        self.assertEqual(pinned_pr["checksum_sha256"], digest)
        for fixture_name in ("compatible", "reference-mismatch", "semantic-incompat"):
            fixture = self.root / "fixtures" / "gdc" / "graphalytics-tiny" / fixture_name
            path = fixture / "references" / "ga-tiny-pr.ref"
            text = path.read_text(encoding="utf-8")
            self.assertEqual(text, formatted, fixture_name)
            self.assertNotIn("0.25", text, fixture_name)
            acquisition = json.loads((fixture / "acquisition.json").read_text(encoding="utf-8"))
            acquired_pr = next(
                item for item in acquisition["references"] if item["workload_key"] == "pr"
            )
            self.assertEqual(acquired_pr["checksum_sha256"], digest, fixture_name)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), digest)

    def test_ga_tiny_cdlp_reference_remains_truthful(self) -> None:
        expected = _official_graphalytics_cdlp_labels()
        formatted = "".join(f"{vertex} {label}\n" for vertex, label in expected)
        for fixture_name in ("compatible", "reference-mismatch", "semantic-incompat"):
            path = (
                self.root
                / "fixtures"
                / "gdc"
                / "graphalytics-tiny"
                / fixture_name
                / "references"
                / "ga-tiny-cdlp.ref"
            )
            self.assertEqual(path.read_text(encoding="utf-8"), formatted, fixture_name)

    def test_index_and_readme_point_at_graphalytics_suite(self) -> None:
        index = (self.root / "gdc-suite-index.md").read_text(encoding="utf-8")
        self.assertIn("`graphalytics`", index)
        self.assertIn("gdc-graphalytics", index)
        self.assertIn("edges-only", index)
        readme = (self.root / "README.md").read_text(encoding="utf-8")
        self.assertIn("gdc_graphalytics", readme)
        self.assertIn("edges-only", readme)


def _ga_tiny_directed_edges() -> tuple[list[int], list[tuple[int, int]]]:
    # Matches the committed edges-only fixture; isolated vertices are out of scope.
    return [1, 2, 3, 4], [(1, 2), (1, 3), (2, 3), (3, 4)]


def _official_graphalytics_pr_vector(
    *, damping: float = 0.85, max_iterations: int = 2
) -> dict[int, float]:
    """Graphalytics v1.0.5 PageRank from definition.tex (IEEE-754 binary64)."""
    vertices, edges = _ga_tiny_directed_edges()
    n = float(len(vertices))
    outdeg = dict.fromkeys(vertices, 0)
    incoming = {vertex: [] for vertex in vertices}
    for source, target in edges:
        outdeg[source] += 1
        incoming[target].append(source)
    sinks = [vertex for vertex in vertices if outdeg[vertex] == 0]
    ranks = dict.fromkeys(vertices, 1.0 / n)
    for _ in range(max_iterations):
        teleport = (1.0 - damping) / n
        redistributed = (damping / n) * sum(ranks[vertex] for vertex in sinks)
        nxt = {}
        for vertex in vertices:
            importance = 0.0
            for source in incoming[vertex]:
                if outdeg[source] == 0:
                    continue
                importance += ranks[source] / outdeg[source]
            nxt[vertex] = teleport + damping * importance + redistributed
        ranks = nxt
    return ranks


def _format_graphalytics_float_reference(values: dict[int, float]) -> str:
    # Official validation files use scientific notation with 15 significant digits.
    return "".join(f"{vertex} {format(values[vertex], '.15e')}\n" for vertex in sorted(values))


def _official_graphalytics_cdlp_labels(*, max_iterations: int = 2) -> list[tuple[int, int]]:
    """Graphalytics v1.0.5 synchronous CDLP with min-label ties."""
    vertices, edges = _ga_tiny_directed_edges()
    incoming = {vertex: [] for vertex in vertices}
    outgoing = {vertex: [] for vertex in vertices}
    for source, target in edges:
        incoming[target].append(source)
        outgoing[source].append(target)
    labels = {vertex: vertex for vertex in vertices}
    for _ in range(max_iterations):
        nxt = {}
        for vertex in vertices:
            counts: dict[int, int] = {}
            for neighbor in incoming[vertex]:
                counts[labels[neighbor]] = counts.get(labels[neighbor], 0) + 1
            for neighbor in outgoing[vertex]:
                counts[labels[neighbor]] = counts.get(labels[neighbor], 0) + 1
            if not counts:
                nxt[vertex] = labels[vertex]
                continue
            max_freq = max(counts.values())
            nxt[vertex] = min(label for label, freq in counts.items() if freq == max_freq)
        labels = nxt
    return [(vertex, labels[vertex]) for vertex in vertices]


if __name__ == "__main__":
    unittest.main()
