"""Direct tests for the provenance-gated TCK performance consumer (#1654).

Every run directory, BenchExec record and Divan raw result built here is a
FIXTURE: synthetic input shaped like real evidence, never measured. BenchExec
records still go through the real `adapt_run_result`/`normalize_run` boundary.
The parity expectations in `fixtures/tck_perf_parity_origin_main.json` were
produced by the pre-#1654 Rust `tests/bdd/timing.rs::build_report` at
origin/main bf28be798 (see that file's `_source`).
"""

from __future__ import annotations

import copy
from decimal import Decimal
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from graphforge_bench import tck_perf
from graphforge_bench.benchexec_authority import Limits, adapt_run_result, normalize_run
from graphforge_bench.tck_perf import (
    PROVENANCE_KEYS,
    BaselineTimings,
    Policy,
    Sample,
    TckPerfError,
)

ROOT = Path(__file__).resolve().parents[2]
PARITY = json.loads(
    (Path(__file__).parent / "fixtures/tck_perf_parity_origin_main.json").read_text()
)

DELETE5 = "Delete5 - Delete clause interoperation with other clauses:34:[1] Delete node from a list"
CREATE3 = "Create3 - Interoperation with other clauses:49:[2] WITH-CREATE"


class _ExitCode:
    """FIXTURE stand-in for BenchExec's ProcessExitCode."""

    def __init__(self, value: int) -> None:
        self.value = value
        self.signal = None


def fixture_provenance(**changes: object) -> dict:
    """FIXTURE provenance with every compared key populated."""
    document = {
        "schema": tck_perf.PROVENANCE_SCHEMA,
        "host": {
            "cpu_model": "FIXTURE CPU",
            "logical_cpus": 16,
            "memory_bytes": 134_949_265_408,
            "runner_label": "local",
        },
        "build": {
            "rustc": "rustc 1.96.0 (fixture)\nhost: x86_64-unknown-linux-gnu",
            "profile": "release",
            "target": "x86_64-unknown-linux-gnu",
            "features": [],
        },
        "workload": {
            "fixture_profile": "pooled-isolated-serial-v1",
            "concurrency": 1,
            "corpus_digest": "a" * 64,
            "suite_selection": {"api_bdd": True, "tck": "whole-corpus"},
            "scenario_order": {"whole_tck": "cucumber-file-order", "divan": "divan-name-sorted"},
            "temp_root_filesystem": "ext4",
            "tool_versions": {"benchexec": "3.35", "codspeed-divan-compat": "5.0.1"},
            "sample_counts": {"divan_samples_per_scenario": 10, "benchexec_runs": 1},
        },
        "fault_injection": None,
    }
    for dotted, value in changes.items():
        section, _, name = dotted.partition("__")
        if name:
            document[section][name] = value
        else:
            document[section] = value
    return document


def mutated(value: object) -> object:
    """A different value of the same shape, for one provenance key."""
    if isinstance(value, bool):
        return not value
    if isinstance(value, int):
        return value + 1
    if isinstance(value, str):
        return f"{value}-other"
    if isinstance(value, list):
        return [*value, "extra-feature"]
    if isinstance(value, dict):
        changed = dict(value)
        first = sorted(changed)[0]
        changed[first] = mutated(changed[first])
        return changed
    raise AssertionError(f"no mutation for {value!r}")


def write_run(
    directory: Path,
    scenarios: dict[str, float],
    *,
    wall_seconds: float,
    provenance: dict | None = None,
    rounds: int = 10,
    complete: bool = True,
) -> Path:
    """Write a FIXTURE run directory: run.json, benchexec.json and Divan raw results.

    `scenarios` maps scenario key to its Divan median in milliseconds.
    """
    directory.mkdir(parents=True, exist_ok=True)
    raw = {
        "walltime": wall_seconds,
        "cputime": wall_seconds,
        "memory": 1024,
        "blkio-read": 0,
        "blkio-write": 0,
        # RunExecutor reports PSI totals as Decimal (BenchExec 3.35).
        "pressure-cpu-some": Decimal("0.000007"),
        "pressure-io-some": Decimal("0"),
        "pressure-memory-some": Decimal("0"),
        "exitcode": _ExitCode(0),
    }
    benchexec = normalize_run(
        benchexec=adapt_run_result(raw, correctness=True),
        graphforge={
            "status": "passed",
            "source": "FIXTURE",
            "phases": [{"phase": "tck_scenarios", "duration_ms": round(wall_seconds * 1000)}],
        },
        limits=Limits(7200.0, 7200.0, 1 << 30, (0,)),
    )
    (directory / "benchexec.json").write_text(json.dumps(benchexec), encoding="utf-8")
    divan = directory / "divan"
    divan.mkdir(exist_ok=True)
    for index, (key, median_ms) in enumerate(sorted(scenarios.items())):
        result = {
            "name": f"scenario[{key}]",
            "uri": "FIXTURE",
            "config": {},
            "stats": {
                "min_ns": median_ms * 1e6,
                "max_ns": median_ms * 1e6,
                "mean_ns": median_ms * 1e6,
                "median_ns": median_ms * 1e6,
                "rounds": rounds,
                "iter_per_round": 1,
            },
        }
        (divan / f"{index:05d}.json").write_text(json.dumps(result), encoding="utf-8")
    run = {
        "schema": tck_perf.RUN_SCHEMA,
        "provenance": provenance or fixture_provenance(),
        "complete": complete,
        "whole_tck": {
            "benchexec": "benchexec.json",
            "benchexec_sha256": tck_perf._sha256_file(directory / "benchexec.json"),
            "binary_sha256": "b" * 64,
            "tck_total": len(scenarios),
            "tck_passing": len(scenarios),
            "tck_regressed": 0,
            "tck_scenario_keys_sha256": tck_perf.key_set_digest(list(scenarios)),
        },
        "divan": {"mode": "bench", "raw_results": "divan", "binary_sha256": "c" * 64},
    }
    (directory / "run.json").write_text(json.dumps(run), encoding="utf-8")
    return directory


def edit_run(directory: Path, edit) -> None:
    document = json.loads((directory / "run.json").read_text())
    edit(document)
    (directory / "run.json").write_text(json.dumps(document), encoding="utf-8")


# FIXTURE timings: a clean run, one with Delete5 slowed, and one slowed overall.
CLEAN = {DELETE5: 41.886, CREATE3: 17.587, "Match1 - Match nodes:10:[1] Match all": 5.0}
CLEAN_WALL = 60.0


class _TempCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.dir = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def baseline(self, provenance: dict | None = None) -> tck_perf.Baseline:
        run = tck_perf.load_run(
            write_run(self.dir / "base-run", CLEAN, wall_seconds=CLEAN_WALL, provenance=provenance)
        )
        path = self.dir / "store/baseline.json"
        tck_perf.capture_baseline(run, path, ROOT)
        return tck_perf.load_baseline(path, "explicit")

    def slow_run(self, name: str = "slow-run", **kwargs) -> tck_perf.Run:
        """A FIXTURE run that warns on a scenario and the aggregate if compared."""
        slowed = dict(CLEAN, **{DELETE5: 1242.413})
        return tck_perf.load_run(write_run(self.dir / name, slowed, wall_seconds=141.0, **kwargs))


def _samples(case: dict) -> list[Sample]:
    return [
        Sample(key, feature, us, outcome == "passed")
        for key, feature, outcome, us in case["records"]
    ]


# The same inputs the origin/main probe used (see the parity fixture `_cases`).
PARITY_INPUTS = PARITY["_cases"]


class ParityWithPreviousRustThresholds(unittest.TestCase):
    """Matching provenance: identical findings and messages to origin/main."""

    def assert_case(self, name: str) -> None:
        case = PARITY_INPUTS[name]
        expected = PARITY[name]
        policy = Policy(**case["policy"])
        baseline = BaselineTimings(case["baseline_total"], case["baseline"], case["features"])
        samples = _samples(case)
        comparison = tck_perf.compare(
            samples,
            sum(sample.elapsed_us for sample in samples),
            baseline,
            policy,
            partial=case["partial"],
        )
        self.assertEqual(comparison.baseline_status, expected["baseline_status"])
        self.assertEqual(comparison.findings, expected["findings"])
        self.assertEqual(comparison.unbaselined, expected["unbaselined"])
        self.assertEqual(comparison.missing, expected["missing"])
        self.assertEqual(
            tck_perf.annotation_messages(comparison.findings, case["annotation_cap"]),
            expected["annotations"],
        )

    def test_only_tck_baseline_regressions_create_findings(self):
        self.assert_case("A")

    def test_aggregate_degradation_reports_largest_feature_contributors(self):
        self.assert_case("B")

    def test_partial_runs_never_compare_with_the_full_baseline(self):
        self.assert_case("C")

    def test_threshold_boundaries_are_strict_and_annotations_are_capped(self):
        self.assert_case("D")
        self.assert_case("E")

    def test_recorded_observations_under_the_committed_policy(self):
        self.assert_case("F")

    def test_absolute_threshold_is_strict_and_wins_over_relative(self):
        self.assert_case("G")

    def test_non_passing_scenarios_never_warn(self):
        self.assert_case("H")

    def test_invalid_policy_is_blocking(self):
        with self.assertRaisesRegex(TckPerfError, "invalid TCK timing policy"):
            tck_perf.compare([], 0, None, Policy(max_warning_annotations=0))

    def test_committed_schema_2_policy_values_are_the_default_policy(self):
        legacy = json.loads((ROOT / "tests/tck/performance_policy.json").read_text())
        default = Policy()
        for name in (
            "per_scenario_multiplier",
            "per_scenario_min_delta_ms",
            "aggregate_multiplier",
            "aggregate_min_delta_ms",
            "absolute_slow_ms",
            "max_warning_annotations",
        ):
            self.assertEqual(getattr(default, name), legacy[name], name)


class ConsumerParity(_TempCase):
    def test_matching_provenance_reports_the_previous_messages_through_check(self):
        result = tck_perf.check(self.slow_run(), self.baseline())
        self.assertEqual(result.report["baseline_status"], "compared")
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(
            [finding["message"] for finding in result.report["findings"]],
            [
                "openCypher TCK total: 141000.000 ms (baseline 60000.000 ms, "
                "warning threshold 75000.000 ms)",
                f"{DELETE5}: 1242.413 ms (baseline 41.886 ms, warning threshold 291.886 ms)",
            ],
        )
        self.assertEqual(
            result.lines,
            [f"TCK PERF WARNING: {finding['message']}" for finding in result.report["findings"]],
        )


def _mismatch_test(key: str):
    def test(self: _TempCase) -> None:
        base = fixture_provenance()
        current = copy.deepcopy(base)
        section, name = key.split(".", 1)
        current[section][name] = mutated(base[section][name])
        result = tck_perf.check(self.slow_run(provenance=current), self.baseline(base))
        report = result.report
        self.assertEqual(report["baseline_status"], "incompatible")
        self.assertEqual(report["mismatched_fields"], [key])
        self.assertTrue(report["skip_reason"].startswith(f"provenance mismatch: {key} (baseline "))
        self.assertEqual(report["findings"], [])
        self.assertFalse(any("TCK PERF WARNING" in line for line in result.lines))
        self.assertEqual(result.exit_code, 0)
        strict = tck_perf.check(
            self.slow_run("strict", provenance=current),
            self.baseline(base),
            require_compatible=True,
        )
        self.assertEqual(strict.exit_code, 3)
        self.assertEqual(strict.report["findings"], [])

    test.__name__ = f"test_mismatch_on_{key.replace('.', '_')}_skips_without_findings"
    return test


class ProvenanceGate(_TempCase):
    def test_every_documented_key_is_compared(self):
        self.assertEqual(len(PROVENANCE_KEYS), 16)
        self.assertEqual(
            {key.split(".")[0] for key in PROVENANCE_KEYS}, {"host", "build", "workload"}
        )

    def test_several_mismatches_are_all_named(self):
        current = fixture_provenance(build__profile="test", host__runner_label="ci")
        result = tck_perf.check(self.slow_run(provenance=current), self.baseline())
        self.assertEqual(result.report["mismatched_fields"], ["host.runner_label", "build.profile"])
        self.assertIn(
            'build.profile (baseline "release", current "test")', result.report["skip_reason"]
        )

    def test_fault_injection_is_recorded_but_not_a_compatibility_key(self):
        fault = {"scenario": DELETE5, "delay_ms": 1200}
        run = self.slow_run(provenance=fixture_provenance(fault_injection=fault))
        result = tck_perf.check(run, self.baseline())
        self.assertEqual(result.report["baseline_status"], "compared")
        self.assertEqual(result.report["fault_injection"], fault)
        self.assertTrue(result.lines[0].startswith("TCK PERF NOTICE: this run is fault-injected"))

    def test_missing_baseline_is_unbaselined_and_fails_only_when_compatibility_is_required(self):
        result = tck_perf.check(self.slow_run(), None)
        self.assertEqual((result.report["baseline_status"], result.exit_code), ("unbaselined", 0))
        self.assertEqual(result.report["findings"], [])
        self.assertEqual(
            tck_perf.check(self.slow_run("b"), None, require_compatible=True).exit_code, 3
        )


for _key in PROVENANCE_KEYS:
    _test = _mismatch_test(_key)
    setattr(ProvenanceGate, _test.__name__, _test)


class Rejections(_TempCase):
    def assert_rejected(self, pattern: str, loader) -> None:
        with self.assertRaisesRegex(TckPerfError, pattern):
            loader()

    def test_missing_run_evidence(self):
        self.assert_rejected("TCK perf run is missing", lambda: tck_perf.load_run(self.dir))

    def test_malformed_run_evidence(self):
        (self.dir / "run.json").write_text("{not json")
        self.assert_rejected("TCK perf run is malformed", lambda: tck_perf.load_run(self.dir))

    def test_schema_invalid_run_evidence(self):
        run = write_run(self.dir / "r", CLEAN, wall_seconds=1.0)
        edit_run(run, lambda doc: doc.update(schema="graphforge-tck-perf-run/0"))
        self.assert_rejected("must use schema", lambda: tck_perf.load_run(run))

    def test_schema_invalid_provenance(self):
        run = write_run(self.dir / "r", CLEAN, wall_seconds=1.0)
        edit_run(run, lambda doc: doc["provenance"]["build"].pop("profile"))
        self.assert_rejected("provenance is missing build.profile", lambda: tck_perf.load_run(run))

    def test_malformed_benchexec_evidence(self):
        run = write_run(self.dir / "r", CLEAN, wall_seconds=1.0)
        (run / "benchexec.json").write_text("{}")
        self.assert_rejected("does not match its recorded sha256", lambda: tck_perf.load_run(run))

    def test_failed_benchexec_outcome(self):
        run = write_run(self.dir / "r", CLEAN, wall_seconds=1.0)
        document = json.loads((run / "benchexec.json").read_text())
        document["outcome"] = "timeout"
        (run / "benchexec.json").write_text(json.dumps(document))
        edit_run(
            run,
            lambda doc: doc["whole_tck"].update(
                benchexec_sha256=tck_perf._sha256_file(run / "benchexec.json")
            ),
        )
        self.assert_rejected("BenchExec run did not pass", lambda: tck_perf.load_run(run))

    def test_whole_tck_regression(self):
        run = write_run(self.dir / "r", CLEAN, wall_seconds=1.0)
        edit_run(run, lambda doc: doc["whole_tck"].update(tck_regressed=1))
        self.assert_rejected("correctness failed", lambda: tck_perf.load_run(run))

    def test_partial_run(self):
        run = write_run(self.dir / "r", CLEAN, wall_seconds=1.0, complete=False)
        self.assert_rejected("partial run", lambda: tck_perf.load_run(run))

    def test_partial_divan_samples(self):
        run = write_run(self.dir / "r", CLEAN, wall_seconds=1.0, rounds=3)
        self.assert_rejected("partial Divan result", lambda: tck_perf.load_run(run))

    def test_test_mode_divan_with_no_raw_results(self):
        run = write_run(self.dir / "r", CLEAN, wall_seconds=1.0)
        for path in (run / "divan").iterdir():
            path.unlink()
        self.assert_rejected("Divan test mode writes none", lambda: tck_perf.load_run(run))

    def test_test_mode_divan_declared(self):
        run = write_run(self.dir / "r", CLEAN, wall_seconds=1.0)
        edit_run(run, lambda doc: doc["divan"].update(mode="test"))
        self.assert_rejected(
            "test mode is not performance evidence", lambda: tck_perf.load_run(run)
        )

    def test_scenario_set_mismatch_between_divan_and_the_whole_run(self):
        run = write_run(self.dir / "r", CLEAN, wall_seconds=1.0)
        edit_run(
            run,
            lambda doc: doc["whole_tck"].update(
                tck_scenario_keys_sha256=tck_perf.key_set_digest([*CLEAN, "Other:1:x"])
            ),
        )
        self.assert_rejected("scenario-set mismatch", lambda: tck_perf.load_run(run))

    def test_scenario_set_mismatch_against_a_matched_baseline(self):
        baseline = self.baseline()
        fewer = dict(CLEAN)
        fewer.pop(CREATE3)
        run = tck_perf.load_run(write_run(self.dir / "r", fewer, wall_seconds=1.0))
        self.assert_rejected("scenario-set mismatch", lambda: tck_perf.check(run, baseline))

    def test_cucumber_report_offered_as_run_evidence(self):
        (self.dir / "run.json").write_text(
            json.dumps({"schema_version": 3, "report_kind": "diagnostic", "suites": []})
        )
        self.assert_rejected("diagnostic-substituted input", lambda: tck_perf.load_run(self.dir))

    def test_cucumber_report_offered_as_benchexec_evidence(self):
        run = write_run(self.dir / "r", CLEAN, wall_seconds=1.0)
        (run / "benchexec.json").write_text(json.dumps({"report_kind": "diagnostic"}))
        self.assert_rejected("diagnostic-substituted input", lambda: tck_perf.load_run(run))

    def test_committed_schema_2_baseline_offered_as_baseline(self):
        path = ROOT / "tests/tck/performance_baseline.json"
        self.assert_rejected(
            "diagnostic-substituted input: .*schema-2 Cucumber baseline",
            lambda: tck_perf.load_baseline(path, "explicit"),
        )

    def test_cucumber_report_offered_as_baseline(self):
        path = self.dir / "report.json"
        path.write_text(
            json.dumps({"schema_version": 3, "report_kind": "diagnostic", "suites": []})
        )
        self.assert_rejected(
            "diagnostic-substituted input", lambda: tck_perf.load_baseline(path, "explicit")
        )

    def test_schema_invalid_baseline(self):
        path = self.dir / "baseline.json"
        path.write_text(json.dumps({"schema": tck_perf.BASELINE_SCHEMA, "provenance": {}}))
        self.assert_rejected(
            "provenance must use schema", lambda: tck_perf.load_baseline(path, "x")
        )

    def test_explicit_baseline_must_exist(self):
        self.assert_rejected(
            "explicit baseline is missing",
            lambda: tck_perf.resolve_baseline(
                self.dir / "nope.json", self.dir / "a", self.dir / "b"
            ),
        )

    def test_fault_injection_naming_an_unknown_scenario(self):
        provenance = fixture_provenance(fault_injection={"scenario": "Nope:1:x", "delay_ms": 5})
        run = write_run(self.dir / "r", CLEAN, wall_seconds=1.0, provenance=provenance)
        self.assert_rejected("unknown scenario", lambda: tck_perf.load_run(run))


class HostLocalBaselines(_TempCase):
    def test_capture_round_trips_and_compares_cleanly(self):
        baseline = self.baseline()
        self.assertEqual(baseline.timings.total_elapsed_us, 60_000_000)
        self.assertEqual(baseline.timings.scenarios[DELETE5], 41_886)
        run = tck_perf.load_run(write_run(self.dir / "again", CLEAN, wall_seconds=CLEAN_WALL))
        result = tck_perf.check(run, baseline)
        self.assertEqual(
            (result.report["baseline_status"], result.report["findings"]), ("compared", [])
        )

    def test_capture_refuses_a_fault_injected_run(self):
        provenance = fixture_provenance(fault_injection={"scenario": "*", "delay_ms": 5})
        run = tck_perf.load_run(
            write_run(self.dir / "r", CLEAN, wall_seconds=1.0, provenance=provenance)
        )
        with self.assertRaisesRegex(TckPerfError, "fault-injected"):
            tck_perf.capture_baseline(run, self.dir / "store/baseline.json", ROOT)
        self.assertFalse((self.dir / "store/baseline.json").exists())

    def test_capture_refuses_the_repository_tree(self):
        run = tck_perf.load_run(write_run(self.dir / "r", CLEAN, wall_seconds=1.0))
        destination = ROOT / "tests/tck/tck-perf-baseline.json"
        with self.assertRaisesRegex(TckPerfError, "inside the repository"):
            tck_perf.capture_baseline(run, destination, ROOT)
        self.assertFalse(destination.exists())

    def test_a_baseline_from_a_fault_injected_run_is_rejected(self):
        path = self.dir / "baseline.json"
        self.baseline()
        document = json.loads((self.dir / "store/baseline.json").read_text())
        document["provenance"]["fault_injection"] = {"scenario": "*", "delay_ms": 5}
        path.write_text(json.dumps(document))
        with self.assertRaisesRegex(TckPerfError, "fault-injected run"):
            tck_perf.load_baseline(path, "explicit")

    def test_resolution_order_is_explicit_then_host_local_then_committed(self):
        explicit, host, committed = (self.dir / name for name in ("e.json", "h.json", "c.json"))
        self.assertIsNone(tck_perf.resolve_baseline(None, host, committed))
        committed.write_text("{}")
        self.assertEqual(tck_perf.resolve_baseline(None, host, committed), (committed, "committed"))
        host.write_text("{}")
        self.assertEqual(tck_perf.resolve_baseline(None, host, committed), (host, "host_local"))
        explicit.write_text("{}")
        self.assertEqual(
            tck_perf.resolve_baseline(explicit, host, committed), (explicit, "explicit")
        )

    def test_host_local_store_is_outside_the_tree(self):
        self.assertEqual(
            tck_perf.host_local_baseline_path({"GF_TCK_PERF_HOME": "/srv/gf"}),
            Path("/srv/gf/baseline.json"),
        )
        self.assertEqual(
            tck_perf.host_local_baseline_path({"XDG_DATA_HOME": "/x", "HOME": "/h"}),
            Path("/x/graphforge/tck-perf/baseline.json"),
        )
        self.assertEqual(
            tck_perf.host_local_baseline_path({"HOME": "/h"}),
            Path("/h/.local/share/graphforge/tck-perf/baseline.json"),
        )

    def test_no_committed_baseline_exists(self):
        self.assertFalse((ROOT / tck_perf.COMMITTED_BASELINE).exists())


class KnownPositiveOnFixtures(_TempCase):
    """FIXTURE known positive; the real-host run is evidence on #1467."""

    def test_an_injected_slow_scenario_warns(self):
        fault = {"scenario": DELETE5, "delay_ms": 1300}
        slowed = dict(CLEAN, **{DELETE5: 41.886 + 1300})
        run = tck_perf.load_run(
            write_run(
                self.dir / "r",
                slowed,
                wall_seconds=CLEAN_WALL + 13,
                provenance=fixture_provenance(fault_injection=fault),
            )
        )
        findings = tck_perf.check(run, self.baseline()).report["findings"]
        self.assertEqual([finding["kind"] for finding in findings], ["scenario_regression"])
        self.assertEqual(findings[0]["key"], DELETE5)

    def test_an_injected_slowed_aggregate_warns(self):
        fault = {"scenario": "*", "delay_ms": 10}
        slowed = {key: value + 10 for key, value in CLEAN.items()}
        run = tck_perf.load_run(
            write_run(
                self.dir / "r",
                slowed,
                wall_seconds=CLEAN_WALL + 39,
                provenance=fixture_provenance(fault_injection=fault),
            )
        )
        findings = tck_perf.check(run, self.baseline()).report["findings"]
        self.assertEqual([finding["kind"] for finding in findings], ["aggregate_regression"])


class Emission(_TempCase):
    def test_workflow_message_escaping_leaves_colons_and_commas(self):
        self.assertEqual(
            tck_perf.escape_workflow_message("total: 1 ms, x%\r\n"), "total: 1 ms, x%25%0D%0A"
        )

    def test_github_annotations_use_message_escaping(self):
        result = tck_perf.check(self.slow_run(), self.baseline(), github_actions=True)
        annotations = [line for line in result.lines if line.startswith("::warning")]
        self.assertEqual(len(annotations), 2)
        self.assertTrue(
            annotations[0].startswith("::warning title=TCK performance::openCypher TCK total: ")
        )
        self.assertNotIn("%3A", annotations[0])
        self.assertNotIn("%2C", annotations[0])


class CommandLine(_TempCase):
    def main(self, *argv: str) -> tuple[int, str]:
        stderr = io.StringIO()
        with (
            patch("sys.stderr", stderr),
            patch.dict("os.environ", {"GF_TCK_PERF_HOME": str(self.dir / "home")}),
        ):
            code = tck_perf.main(["check", "--repo-root", str(ROOT), *argv])
        return code, stderr.getvalue()

    def test_check_exit_codes(self):
        base = fixture_provenance()
        self.baseline(base)
        baseline = str(self.dir / "store/baseline.json")
        run = write_run(
            self.dir / "mismatch",
            CLEAN,
            wall_seconds=141.0,
            provenance=fixture_provenance(build__profile="test"),
        )
        code, err = self.main("--baseline", baseline, str(run))
        self.assertEqual(code, 0, err)
        self.assertIn(
            "TCK PERF SKIPPED: baseline incompatible: provenance mismatch: build.profile", err
        )
        report = json.loads((run / "report.json").read_text())
        self.assertEqual(report["baseline_status"], "incompatible")
        code, _ = self.main("--baseline", baseline, "--require-compatible", str(run))
        self.assertEqual(code, 3)
        code, err = self.main(str(self.dir / "absent"))
        self.assertEqual(code, 2)
        self.assertIn("TCK perf: error: TCK perf run is missing", err)

    def test_capture_writes_the_host_local_store_by_default(self):
        run = write_run(self.dir / "r", CLEAN, wall_seconds=CLEAN_WALL)
        code, err = self.main(str(run), "--capture-baseline")
        self.assertEqual(code, 0, err)
        self.assertTrue((self.dir / "home/baseline.json").is_file())
        code, err = self.main(str(run))
        self.assertIn("baseline_status=compared", err)


class DriverHelpers(unittest.TestCase):
    VERDICT = (
        "openCypher TCK (advisory, whole corpus): 3898 passing of 3898 scenarios — "
        "baseline 3898 (0 regressed, 0 xpass)"
    )

    def test_parse_tck_verdict(self):
        self.assertEqual(
            tck_perf.parse_tck_verdict(f"noise\n{self.VERDICT}\n"),
            {"passing": 3898, "total": 3898, "baseline": 3898, "regressed": 0, "xpass": 0},
        )
        with self.assertRaisesRegex(TckPerfError, "exactly one whole-corpus verdict"):
            tck_perf.parse_tck_verdict("TCK_ONLY subset: 5 passing of 5 scenarios")

    def test_fault_announcements_must_match_the_recording(self):
        fault = {"scenario": "*", "delay_ms": 10}
        line = "TCK PERF FAULT INJECTION: delay_ms=10 scenario=*"
        tck_perf.check_fault_announcement(f"x\n{line}\n", fault, "run")
        tck_perf.check_fault_announcement("clean\n", None, "run")
        with self.assertRaisesRegex(TckPerfError, "unrecorded fault injection"):
            tck_perf.check_fault_announcement(line, None, "run")
        with self.assertRaisesRegex(TckPerfError, "did not announce"):
            tck_perf.check_fault_announcement("clean", fault, "run")

    def test_host_and_build_facts(self):
        self.assertEqual(
            tck_perf.host_facts(
                "model name\t: AMD Ryzen 7 3800X 8-Core Processor\n", "MemTotal: 2 kB\n"
            ),
            {"cpu_model": "AMD Ryzen 7 3800X 8-Core Processor", "memory_bytes": 2048},
        )
        self.assertEqual(
            tck_perf.rustc_target("rustc 1.96.0\nhost: x86_64-unknown-linux-gnu\n"),
            "x86_64-unknown-linux-gnu",
        )
        lock = (ROOT / "Cargo.lock").read_text()
        self.assertRegex(tck_perf.locked_version(lock, "codspeed-divan-compat"), r"^\d+\.\d+\.\d+$")

    def test_cargo_executable_requires_exactly_one_artifact(self):
        message = {
            "reason": "compiler-artifact",
            "target": {"name": "bdd"},
            "executable": "/t/bdd-1",
        }
        self.assertEqual(tck_perf.cargo_executable(json.dumps(message), "bdd"), Path("/t/bdd-1"))
        with self.assertRaisesRegex(TckPerfError, "exactly one bdd executable"):
            tck_perf.cargo_executable("", "bdd")

    def test_corpus_digest_is_content_addressed(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            (root / "a.feature").write_text("Feature: A\n")
            first = tck_perf.corpus_digest(root)
            self.assertEqual(first, tck_perf.corpus_digest(root))
            (root / "a.feature").write_text("Feature: B\n")
            self.assertNotEqual(first, tck_perf.corpus_digest(root))

    def test_scenario_feature_parses_the_key(self):
        self.assertEqual(
            tck_perf.scenario_feature(DELETE5),
            "Delete5 - Delete clause interoperation with other clauses",
        )
        with self.assertRaisesRegex(TckPerfError, "not <feature>:<line>:<name>"):
            tck_perf.scenario_feature("no-line")


if __name__ == "__main__":
    unittest.main()
