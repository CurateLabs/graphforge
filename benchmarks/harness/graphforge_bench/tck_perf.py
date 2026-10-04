"""Provenance-gated openCypher TCK performance measurement and consumer (#1654).

Two framework authorities feed one consumer:

* **BenchExec** measures the whole-TCK Cucumber process (the ``bdd`` test
  binary) through the existing boundary in :mod:`graphforge_bench.benchexec_authority`:
  native local admission, :func:`adapt_run_result` and :func:`normalize_run`.
  Its wall time is the aggregate. The correctness verdict is exit 0 on the
  whole corpus with zero TCK regressions.
* **Divan** measures each scenario in-process (``benches/tck_scenarios``). Its
  CodSpeed walltime ``raw_results`` supply the per-scenario medians.

The Cucumber scenario timer is diagnostic only and is never read as evidence.
The consumer compares a run with a baseline only when every provenance key
matches; otherwise it reports ``baseline_status: incompatible`` naming the
mismatched fields, emits no findings and exits 0 (``--require-compatible``
makes that a failure). Thresholds keep the pre-#1654 formulas and messages.

``run`` needs native Linux BenchExec (``/usr/bin/python3`` with system
BenchExec); ``check`` is plain Python. Both are stdlib-only apart from
BenchExec itself.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from typing import Any

from graphforge_bench.benchexec_authority import (
    SCHEMA as BENCHEXEC_SCHEMA,
)
from graphforge_bench.benchexec_authority import (
    EvidenceError,
    Limits,
    Outcome,
    adapt_run_result,
    normalize_run,
    require_local_admission,
)
from graphforge_bench.hybrid_cgroup_v2 import (
    benchexec_cgroup_version,
    is_hybrid_cgroup_layout,
    measure_hybrid_pressure,
)

RUN_SCHEMA = "graphforge-tck-perf-run/1"
BASELINE_SCHEMA = "graphforge-tck-perf-baseline/1"
PROVENANCE_SCHEMA = "graphforge-tck-perf-provenance/1"
REPORT_SCHEMA = "graphforge-tck-perf-report/1"

FIXTURE_PROFILE = "pooled-isolated-serial-v1"
BUILD_PROFILE = "release"
DEFAULT_SAMPLE_COUNT = 10
COMMITTED_BASELINE = "tests/tck/tck-perf-baseline.json"
BASELINE_FILE = "baseline.json"
FAULT_DELAY_ENV = "GF_TCK_PERF_FAULT_DELAY_MS"
FAULT_SCENARIO_ENV = "GF_TCK_PERF_FAULT_SCENARIO"
FAULT_ANNOUNCEMENT = "TCK PERF FAULT INJECTION:"
DURABLE_FILESYSTEMS = frozenset({"ext4", "xfs", "btrfs"})

# Every provenance key the compatibility check compares, in report order.
# `fault_injection` is recorded but deliberately not compared: an injected run
# must still compare against a clean baseline to prove the known positive.
PROVENANCE_KEYS: tuple[str, ...] = (
    "host.cpu_model",
    "host.logical_cpus",
    "host.memory_bytes",
    "host.runner_label",
    "build.rustc",
    "build.profile",
    "build.target",
    "build.features",
    "workload.fixture_profile",
    "workload.concurrency",
    "workload.corpus_digest",
    "workload.suite_selection",
    "workload.scenario_order",
    "workload.temp_root_filesystem",
    "workload.tool_versions",
    "workload.sample_counts",
)
_INTEGER_KEYS = frozenset({"host.logical_cpus", "host.memory_bytes", "workload.concurrency"})
_MAPPING_KEYS = frozenset(
    {"workload.suite_selection", "workload.scenario_order", "workload.tool_versions"}
)
_SAMPLE_COUNT_KEYS = ("divan_samples_per_scenario", "benchexec_runs")

_SCENARIO_NAME = re.compile(r"^scenario\[(?P<key>.+)\]$")
_SCENARIO_KEY = re.compile(r"^(?P<feature>.+?):(?P<line>\d+):")
_TCK_VERDICT = re.compile(
    r"openCypher TCK \(advisory, whole corpus\): (?P<passing>\d+) passing of "
    r"(?P<total>\d+) scenarios — baseline (?P<baseline>\d+) "
    r"\((?P<regressed>\d+) regressed, (?P<xpass>\d+) xpass\)"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class TckPerfError(ValueError):
    """Missing, malformed, partial or substituted evidence. Always fails closed."""


# --------------------------------------------------------------------------
# Thresholds: a port of the pre-#1654 `tests/bdd/timing.rs::build_report`.
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Policy:
    """Warning thresholds. Defaults are the committed pre-#1654 policy values."""

    per_scenario_multiplier: float = 2.0
    per_scenario_min_delta_ms: float = 250.0
    aggregate_multiplier: float = 1.25
    aggregate_min_delta_ms: float = 15_000.0
    absolute_slow_ms: float | None = 1_300.0
    max_warning_annotations: int = 25

    def validate(self) -> None:
        if (
            self.per_scenario_multiplier < 1.0
            or self.aggregate_multiplier < 1.0
            or self.per_scenario_min_delta_ms < 0.0
            or self.aggregate_min_delta_ms < 0.0
            or (self.absolute_slow_ms is not None and self.absolute_slow_ms <= 0.0)
            or self.max_warning_annotations == 0
        ):
            raise TckPerfError("invalid TCK timing policy values")


DEFAULT_POLICY = Policy()


@dataclass(frozen=True)
class Sample:
    """One scenario's current elapsed time in integer microseconds."""

    key: str
    feature: str
    elapsed_us: int
    passed: bool = True


@dataclass(frozen=True)
class BaselineTimings:
    total_elapsed_us: int
    scenarios: Mapping[str, int]
    features: Mapping[str, int]


@dataclass
class Comparison:
    baseline_status: str
    findings: list[dict[str, Any]] = field(default_factory=list)
    unbaselined: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)


def _ms(micros: float) -> float:
    return micros / 1_000.0


def feature_totals(samples: Sequence[Sample]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for sample in samples:
        totals[sample.feature] = totals.get(sample.feature, 0) + sample.elapsed_us
    return dict(sorted(totals.items()))


def compare(
    samples: Sequence[Sample],
    current_total_us: int,
    baseline: BaselineTimings | None,
    policy: Policy,
    *,
    partial: bool = False,
) -> Comparison:
    """Apply the pre-#1654 per-scenario and aggregate formulas unchanged.

    Only the source of `current_total_us` changed: the whole-TCK BenchExec wall
    time replaces the sum of Cucumber scenario timers.
    """
    policy.validate()
    current = {sample.key: sample for sample in samples}
    comparison = Comparison(
        "partial_not_compared" if partial else "compared" if baseline else "unbaselined"
    )
    if not partial and baseline is not None:
        for key in sorted(current):
            sample = current[key]
            baseline_us = baseline.scenarios.get(key)
            if baseline_us is None:
                comparison.unbaselined.append(key)
                continue
            if not sample.passed:
                continue
            relative = max(
                baseline_us * policy.per_scenario_multiplier,
                baseline_us + policy.per_scenario_min_delta_ms * 1_000.0,
            )
            threshold = (
                relative
                if policy.absolute_slow_ms is None
                else min(policy.absolute_slow_ms * 1_000.0, relative)
            )
            if sample.elapsed_us > threshold:
                baseline_ms = _ms(baseline_us)
                current_ms = _ms(sample.elapsed_us)
                threshold_ms = threshold / 1_000.0
                comparison.findings.append(
                    {
                        "kind": "scenario_regression",
                        "message": (
                            f"{key}: {current_ms:.3f} ms (baseline {baseline_ms:.3f} ms, "
                            f"warning threshold {threshold_ms:.3f} ms)"
                        ),
                        "key": key,
                        "baseline_ms": baseline_ms,
                        "current_ms": current_ms,
                        "threshold_ms": threshold_ms,
                        "delta_ms": current_ms - baseline_ms,
                        "contributors": [],
                    }
                )
        comparison.missing.extend(key for key in sorted(baseline.scenarios) if key not in current)

        aggregate_threshold = max(
            baseline.total_elapsed_us * policy.aggregate_multiplier,
            baseline.total_elapsed_us + policy.aggregate_min_delta_ms * 1_000.0,
        )
        if current_total_us > aggregate_threshold:
            contributors = [
                {
                    "feature": feature,
                    "delta_ms": _ms(current_us - baseline.features.get(feature, 0)),
                }
                for feature, current_us in feature_totals(list(current.values())).items()
                if current_us >= baseline.features.get(feature, 0)
            ]
            contributors.sort(key=lambda item: (-item["delta_ms"], item["feature"]))
            baseline_ms = _ms(baseline.total_elapsed_us)
            current_ms = _ms(current_total_us)
            threshold_ms = aggregate_threshold / 1_000.0
            comparison.findings.append(
                {
                    "kind": "aggregate_regression",
                    "message": (
                        f"openCypher TCK total: {current_ms:.3f} ms "
                        f"(baseline {baseline_ms:.3f} ms, warning threshold {threshold_ms:.3f} ms)"
                    ),
                    "key": None,
                    "baseline_ms": baseline_ms,
                    "current_ms": current_ms,
                    "threshold_ms": threshold_ms,
                    "delta_ms": current_ms - baseline_ms,
                    "contributors": contributors[:10],
                }
            )
    elif baseline is None:
        comparison.unbaselined.extend(sorted(current))

    comparison.findings.sort(
        key=lambda item: (
            -item["delta_ms"],
            (0, "") if item["key"] is None else (1, item["key"]),
        )
    )
    return comparison


def annotation_messages(findings: Sequence[Mapping[str, Any]], maximum: int) -> list[str]:
    """Cap warning lines exactly as the pre-#1654 Cucumber emitter did."""
    if maximum <= 0:
        raise TckPerfError("annotation maximum must be positive")
    if len(findings) <= maximum:
        return [finding["message"] for finding in findings]
    detailed = maximum - 1
    messages = [finding["message"] for finding in findings[:detailed]]
    messages.append(
        f"{len(findings)} TCK performance warning(s); {detailed} detailed annotation(s) "
        "emitted — see report.json"
    )
    return messages


def escape_workflow_message(value: str) -> str:
    """Escape a GitHub workflow-command *message*: only %, CR and LF.

    `:` and `,` need escaping only in command properties. Escaping them in the
    message made the pre-#1654 annotations render a literal `%3A`/`%2C`.
    """
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


# --------------------------------------------------------------------------
# Provenance.
# --------------------------------------------------------------------------


def _lookup(document: Mapping[str, Any], dotted: str) -> Any:
    section, name = dotted.split(".", 1)
    block = document.get(section)
    if not isinstance(block, Mapping) or name not in block:
        raise TckPerfError(f"provenance is missing {dotted}")
    return block[name]


def _valid_fault(value: Any) -> bool:
    return value is None or (
        isinstance(value, Mapping)
        and set(value) == {"scenario", "delay_ms"}
        and isinstance(value["scenario"], str)
        and value["scenario"].strip() != ""
        and type(value["delay_ms"]) is int
        and value["delay_ms"] > 0
    )


def validate_provenance(document: Any) -> Mapping[str, Any]:
    """Require every provenance key with a well-formed value."""
    if not isinstance(document, Mapping) or document.get("schema") != PROVENANCE_SCHEMA:
        raise TckPerfError(f"provenance must use schema {PROVENANCE_SCHEMA}")
    for key in PROVENANCE_KEYS:
        value = _lookup(document, key)
        if key in _INTEGER_KEYS:
            ok = type(value) is int and value > 0
        elif key in _MAPPING_KEYS:
            ok = isinstance(value, Mapping) and bool(value)
        elif key == "workload.sample_counts":
            ok = (
                isinstance(value, Mapping)
                and set(value) == set(_SAMPLE_COUNT_KEYS)
                and all(type(value[name]) is int and value[name] > 0 for name in value)
            )
        elif key == "build.features":
            ok = isinstance(value, list) and all(isinstance(item, str) for item in value)
        else:
            ok = isinstance(value, str) and value.strip() != ""
        if not ok:
            raise TckPerfError(f"provenance {key} is malformed")
    if "fault_injection" not in document or not _valid_fault(document["fault_injection"]):
        raise TckPerfError("provenance fault_injection is malformed")
    return document


def provenance_mismatches(
    baseline: Mapping[str, Any], current: Mapping[str, Any]
) -> list[tuple[str, Any, Any]]:
    """Every compared key whose value differs, as (key, baseline, current)."""
    return [
        (key, _lookup(baseline, key), _lookup(current, key))
        for key in PROVENANCE_KEYS
        if _lookup(baseline, key) != _lookup(current, key)
    ]


def _compact(value: Any) -> str:
    text = json.dumps(value, sort_keys=True, ensure_ascii=False)
    return text if len(text) <= 120 else f"{text[:117]}..."


def skip_reason(mismatches: Sequence[tuple[str, Any, Any]]) -> str:
    return "provenance mismatch: " + "; ".join(
        f"{key} (baseline {_compact(base)}, current {_compact(cur)})"
        for key, base, cur in mismatches
    )


# --------------------------------------------------------------------------
# Evidence loading. Everything here fails closed.
# --------------------------------------------------------------------------


def _read_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise TckPerfError(f"{label} is missing: {path}") from error
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise TckPerfError(f"{label} is malformed: {path}: {error}") from error


def _reject_diagnostic(document: Any, label: str) -> None:
    """Refuse Cucumber timing documents offered in place of framework evidence."""
    if not isinstance(document, Mapping):
        return
    if document.get("report_kind") == "diagnostic" or (
        "suites" in document and "schema_version" in document
    ):
        raise TckPerfError(
            f"diagnostic-substituted input: {label} is a Cucumber timing report, which is "
            "diagnostic only and never performance evidence"
        )
    if document.get("schema_version") == 2 and "total_elapsed_us" in document:
        raise TckPerfError(
            f"diagnostic-substituted input: {label} is a schema-2 Cucumber baseline "
            "(legacy diagnostic), not a BenchExec/Divan baseline"
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def key_set_digest(keys: Sequence[str]) -> str:
    return hashlib.sha256("".join(f"{key}\n" for key in sorted(keys)).encode()).hexdigest()


def scenario_feature(key: str) -> str:
    match = _SCENARIO_KEY.match(key)
    if match is None:
        raise TckPerfError(f"scenario key is not <feature>:<line>:<name>: {key!r}")
    return match.group("feature")


def load_divan_results(directory: Path, *, expected_rounds: int) -> list[Sample]:
    """Read CodSpeed walltime raw results; each median becomes one sample."""
    files = sorted(directory.glob("*.json")) if directory.is_dir() else []
    if not files:
        raise TckPerfError(
            f"Divan raw results are missing under {directory}; Divan test mode writes none "
            "and is never performance evidence"
        )
    samples: dict[str, Sample] = {}
    for path in files:
        document = _read_json(path, "Divan raw result")
        stats = document.get("stats") if isinstance(document, Mapping) else None
        name = document.get("name") if isinstance(document, Mapping) else None
        match = _SCENARIO_NAME.match(name) if isinstance(name, str) else None
        if match is None or not isinstance(stats, Mapping):
            raise TckPerfError(f"Divan raw result is not a TCK scenario: {path.name}")
        key = match.group("key")
        rounds = stats.get("rounds")
        median = stats.get("median_ns")
        minimum = stats.get("min_ns")
        if type(rounds) is not int or rounds != expected_rounds:
            raise TckPerfError(
                f"partial Divan result for {key}: {rounds!r} rounds, expected {expected_rounds}"
            )
        if stats.get("iter_per_round") != 1:
            raise TckPerfError(f"Divan result for {key} must time one scenario per sample")
        for value in (median, minimum):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise TckPerfError(f"Divan result for {key} has invalid timing")
        if key in samples:
            raise TckPerfError(f"duplicate Divan result for {key}")
        samples[key] = Sample(key, scenario_feature(key), round(median / 1_000))
    return [samples[key] for key in sorted(samples)]


@dataclass(frozen=True)
class Run:
    directory: Path
    provenance: Mapping[str, Any]
    benchexec: Mapping[str, Any]
    samples: list[Sample]
    total_us: int
    document: Mapping[str, Any]


def _require_sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or _SHA256.match(value) is None:
        raise TckPerfError(f"{label} must be a sha256 digest")
    return value


def load_run(directory: Path) -> Run:
    """Load and validate one `make tck-perf` run directory."""
    document = _read_json(directory / "run.json", "TCK perf run")
    _reject_diagnostic(document, "run.json")
    if not isinstance(document, Mapping) or document.get("schema") != RUN_SCHEMA:
        raise TckPerfError(f"run.json must use schema {RUN_SCHEMA}")
    provenance = validate_provenance(document.get("provenance"))
    if document.get("complete") is not True:
        raise TckPerfError("partial run: only a complete whole-corpus run is evidence")

    whole = document.get("whole_tck")
    if not isinstance(whole, Mapping):
        raise TckPerfError("run.json whole_tck is malformed")
    _require_sha(whole.get("binary_sha256"), "whole_tck.binary_sha256")
    benchexec_path = directory / str(whole.get("benchexec", ""))
    benchexec = _read_json(benchexec_path, "BenchExec evidence")
    _reject_diagnostic(benchexec, "BenchExec evidence")
    if _sha256_file(benchexec_path) != _require_sha(
        whole.get("benchexec_sha256"), "whole_tck.benchexec_sha256"
    ):
        raise TckPerfError("BenchExec evidence does not match its recorded sha256")
    if not isinstance(benchexec, Mapping) or benchexec.get("schema") != BENCHEXEC_SCHEMA:
        raise TckPerfError(f"BenchExec evidence must use schema {BENCHEXEC_SCHEMA}")
    if benchexec.get("outcome") != Outcome.PASSED:
        raise TckPerfError(f"BenchExec run did not pass: {benchexec.get('outcome')!r}")
    authority = benchexec.get("authority")
    wall = authority.get("wall_seconds") if isinstance(authority, Mapping) else None
    if (
        isinstance(wall, bool)
        or not isinstance(wall, (int, float))
        or not math.isfinite(wall)
        or wall <= 0
    ):
        raise TckPerfError("BenchExec wall_seconds is missing or invalid")
    total, passing, regressed = (
        whole.get(name) for name in ("tck_total", "tck_passing", "tck_regressed")
    )
    if not all(type(value) is int for value in (total, passing, regressed)) or total <= 0:
        raise TckPerfError("whole-TCK correctness counts are malformed")
    if regressed != 0 or passing != total:
        raise TckPerfError(
            f"whole-TCK correctness failed: {passing} of {total} passing, {regressed} regressed"
        )
    keys_digest = _require_sha(whole.get("tck_scenario_keys_sha256"), "tck_scenario_keys_sha256")

    divan = document.get("divan")
    if not isinstance(divan, Mapping):
        raise TckPerfError("run.json divan is malformed")
    if divan.get("mode") != "bench":
        raise TckPerfError("Divan test mode is not performance evidence")
    _require_sha(divan.get("binary_sha256"), "divan.binary_sha256")
    samples = load_divan_results(
        directory / str(divan.get("raw_results", "")),
        expected_rounds=provenance["workload"]["sample_counts"]["divan_samples_per_scenario"],
    )
    keys = [sample.key for sample in samples]
    if len(keys) != total or key_set_digest(keys) != keys_digest:
        raise TckPerfError(
            f"scenario-set mismatch: Divan measured {len(keys)} scenarios that differ from the "
            f"{total} the whole-TCK run passed"
        )
    fault = provenance["fault_injection"]
    if fault is not None and fault["scenario"] != "*" and fault["scenario"] not in set(keys):
        raise TckPerfError(f"fault injection names an unknown scenario: {fault['scenario']}")
    return Run(directory, provenance, benchexec, samples, round(wall * 1_000_000), document)


@dataclass(frozen=True)
class Baseline:
    path: Path
    source: str
    provenance: Mapping[str, Any]
    timings: BaselineTimings


def load_baseline(path: Path, source: str) -> Baseline:
    document = _read_json(path, "TCK perf baseline")
    _reject_diagnostic(document, str(path.name))
    if not isinstance(document, Mapping) or document.get("schema") != BASELINE_SCHEMA:
        raise TckPerfError(f"baseline must use schema {BASELINE_SCHEMA}")
    provenance = validate_provenance(document.get("provenance"))
    if provenance["fault_injection"] is not None:
        raise TckPerfError("baseline was captured from a fault-injected run")
    total = document.get("total_elapsed_us")
    scenarios = document.get("scenarios")
    features = document.get("features")
    if type(total) is not int or total <= 0:
        raise TckPerfError("baseline total_elapsed_us is malformed")
    for label, values in (("scenarios", scenarios), ("features", features)):
        if (
            not isinstance(values, Mapping)
            or not values
            or not all(isinstance(k, str) and type(v) is int and v >= 0 for k, v in values.items())
        ):
            raise TckPerfError(f"baseline {label} are malformed")
    if document.get("scenario_count") != len(scenarios):
        raise TckPerfError("baseline scenario_count does not match its scenarios")
    return Baseline(
        path, source, provenance, BaselineTimings(total, dict(scenarios), dict(features))
    )


def host_local_baseline_path(environ: Mapping[str, str] | None = None) -> Path:
    """Host-local baseline store, outside every checkout."""
    environ = os.environ if environ is None else environ
    if home := environ.get("GF_TCK_PERF_HOME"):
        return Path(home) / BASELINE_FILE
    data = environ.get("XDG_DATA_HOME") or str(
        Path(environ.get("HOME", "~")).expanduser() / ".local/share"
    )
    return Path(data) / "graphforge/tck-perf" / BASELINE_FILE


def resolve_baseline(
    explicit: Path | None, host_local: Path, committed: Path
) -> tuple[Path, str] | None:
    """Explicit path, then host-local, then committed. An explicit path must exist."""
    if explicit is not None:
        if not explicit.is_file():
            raise TckPerfError(f"explicit baseline is missing: {explicit}")
        return explicit, "explicit"
    for path, source in ((host_local, "host_local"), (committed, "committed")):
        if path.is_file():
            return path, source
    return None


def _inside(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def capture_baseline(run: Run, destination: Path, repo_root: Path) -> dict[str, Any]:
    """Store a complete, passing, fault-free run as a host-local baseline."""
    if run.provenance["fault_injection"] is not None:
        raise TckPerfError("refusing to capture a fault-injected run as a baseline")
    if _inside(destination, repo_root):
        raise TckPerfError(
            f"refusing to capture a baseline inside the repository ({destination}); "
            "baselines are host-local"
        )
    document = {
        "schema": BASELINE_SCHEMA,
        "provenance": run.provenance,
        "source_run": {
            "benchexec_sha256": run.document["whole_tck"]["benchexec_sha256"],
            "whole_tck_binary_sha256": run.document["whole_tck"]["binary_sha256"],
            "divan_binary_sha256": run.document["divan"]["binary_sha256"],
        },
        "total_elapsed_us": run.total_us,
        "scenario_count": len(run.samples),
        "scenarios": {sample.key: sample.elapsed_us for sample in run.samples},
        "features": feature_totals(run.samples),
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(destination)
    return document


# --------------------------------------------------------------------------
# The consumer.
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckResult:
    report: dict[str, Any]
    exit_code: int
    lines: list[str]


def check(
    run: Run,
    baseline: Baseline | None,
    *,
    policy: Policy = DEFAULT_POLICY,
    require_compatible: bool = False,
    github_actions: bool = False,
) -> CheckResult:
    """Compare one run with a baseline under the provenance gate."""
    report: dict[str, Any] = {
        "schema": REPORT_SCHEMA,
        "baseline_status": "unbaselined",
        "baseline_source": None,
        "skip_reason": None,
        "mismatched_fields": [],
        "fault_injection": run.provenance["fault_injection"],
        "provenance": run.provenance,
        "current_total_ms": _ms(run.total_us),
        "scenario_count": len(run.samples),
        "findings": [],
        "unbaselined_tck_scenarios": [],
        "missing_baseline_tck_scenarios": [],
    }
    lines: list[str] = []
    exit_code = 0
    if baseline is None:
        report["skip_reason"] = "no baseline: pass --baseline or capture a host-local baseline"
        lines.append(f"TCK PERF SKIPPED: {report['skip_reason']}")
        exit_code = 3 if require_compatible else 0
    else:
        report["baseline_source"] = baseline.source
        mismatches = provenance_mismatches(baseline.provenance, run.provenance)
        if mismatches:
            report["baseline_status"] = "incompatible"
            report["mismatched_fields"] = [key for key, _, _ in mismatches]
            report["skip_reason"] = skip_reason(mismatches)
            lines.append(f"TCK PERF SKIPPED: baseline incompatible: {report['skip_reason']}")
            exit_code = 3 if require_compatible else 0
        else:
            current_keys = {sample.key for sample in run.samples}
            if current_keys != set(baseline.timings.scenarios):
                raise TckPerfError(
                    "scenario-set mismatch: the run and a provenance-matched baseline measured "
                    "different scenarios"
                )
            comparison = compare(run.samples, run.total_us, baseline.timings, policy)
            report["baseline_status"] = comparison.baseline_status
            report["findings"] = comparison.findings
            report["unbaselined_tck_scenarios"] = comparison.unbaselined
            report["missing_baseline_tck_scenarios"] = comparison.missing
            for message in annotation_messages(comparison.findings, policy.max_warning_annotations):
                lines.append(f"TCK PERF WARNING: {message}")
                if github_actions:
                    lines.append(
                        f"::warning title=TCK performance::{escape_workflow_message(message)}"
                    )
    if run.provenance["fault_injection"] is not None:
        lines.insert(
            0,
            "TCK PERF NOTICE: this run is fault-injected (test-only known positive): "
            + _compact(run.provenance["fault_injection"]),
        )
    return CheckResult(report, exit_code, lines)


# --------------------------------------------------------------------------
# The measurement driver (`make tck-perf`).
# --------------------------------------------------------------------------


def parse_tck_verdict(log: str) -> dict[str, int]:
    matches = list(_TCK_VERDICT.finditer(log))
    if len(matches) != 1:
        raise TckPerfError("the whole-TCK run did not report exactly one whole-corpus verdict")
    return {name: int(value) for name, value in matches[0].groupdict().items()}


def check_fault_announcement(output: str, fault: Mapping[str, Any] | None, label: str) -> None:
    announced = [line.strip() for line in output.splitlines() if FAULT_ANNOUNCEMENT in line]
    if fault is None:
        if announced:
            raise TckPerfError(f"{label} ran an unrecorded fault injection")
        return
    expected = f"{FAULT_ANNOUNCEMENT} delay_ms={fault['delay_ms']} scenario={fault['scenario']}"
    if announced != [expected]:
        raise TckPerfError(f"{label} did not announce the recorded fault injection")


def corpus_digest(features: Path) -> str:
    digest = hashlib.sha256()
    paths = sorted(features.rglob("*.feature"))
    if not paths:
        raise TckPerfError(f"no TCK feature files under {features}")
    for path in paths:
        digest.update(path.relative_to(features).as_posix().encode())
        digest.update(b"\0")
        digest.update(_sha256_file(path).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def host_facts(cpuinfo: str, meminfo: str) -> dict[str, Any]:
    model = next(
        (
            line.split(":", 1)[1].strip()
            for line in cpuinfo.splitlines()
            if line.startswith("model name")
        ),
        None,
    )
    total = next(
        (line.split()[1:3] for line in meminfo.splitlines() if line.startswith("MemTotal:")), None
    )
    if not model or total is None or total[1] != "kB":
        raise TckPerfError("cannot read the host CPU model or memory")
    return {"cpu_model": model, "memory_bytes": int(total[0]) * 1024}


def rustc_target(rustc_vv: str) -> str:
    for line in rustc_vv.splitlines():
        if line.startswith("host: "):
            return line.removeprefix("host: ").strip()
    raise TckPerfError("`rustc -vV` did not report a host target")


def locked_version(cargo_lock: str, package: str) -> str:
    match = re.search(
        rf'\[\[package\]\]\nname = "{re.escape(package)}"\nversion = "([^"]+)"', cargo_lock
    )
    if match is None:
        raise TckPerfError(f"{package} is not in Cargo.lock")
    return match.group(1)


def cargo_executable(messages: str, target: str) -> Path:
    found = []
    for line in messages.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (
            message.get("reason") == "compiler-artifact"
            and message.get("target", {}).get("name") == target
            and message.get("executable")
        ):
            found.append(Path(message["executable"]))
    if len(set(found)) != 1:
        raise TckPerfError(f"cargo did not produce exactly one {target} executable")
    return found[0]


def filesystem_type(path: Path) -> str:
    return subprocess.check_output(
        ["findmnt", "--noheadings", "--output", "FSTYPE", "--target", str(path)], text=True
    ).strip()


def _cargo_build(repo: Path, args: Sequence[str], target: str, features: Sequence[str]) -> Path:
    command = ["cargo", *args, "--locked", "--profile", BUILD_PROFILE, "-p", "graphforge-api"]
    if features:
        command += ["--features", ",".join(features)]
    command += ["--no-run", "--message-format=json-render-diagnostics"]
    completed = subprocess.run(command, cwd=repo, text=True, stdout=subprocess.PIPE, check=False)
    if completed.returncode != 0:
        raise TckPerfError(f"cargo build of {target} failed")
    return cargo_executable(completed.stdout, target)


def _fault_env(fault: Mapping[str, Any] | None) -> dict[str, str]:
    if fault is None:
        return {}
    return {FAULT_DELAY_ENV: str(fault["delay_ms"]), FAULT_SCENARIO_ENV: fault["scenario"]}


def _base_env(tmp: Path) -> dict[str, str]:
    # A clean environment: no TCK_ONLY subset, no BLESS, no ambient fault.
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "TMPDIR": str(tmp),
        "TMP": str(tmp),
        "TEMP": str(tmp),
    }


def _run_divan(
    executable: Path,
    repo: Path,
    output: Path,
    tmp: Path,
    samples: int,
    fault: Mapping[str, Any] | None,
) -> None:
    codspeed_root = output / "codspeed"
    env = _base_env(tmp) | _fault_env(fault)
    env |= {"CODSPEED_ENV": "local", "CODSPEED_CARGO_WORKSPACE_ROOT": str(codspeed_root)}
    with (output / "divan.log").open("w", encoding="utf-8") as log:
        completed = subprocess.run(
            [str(executable), "--bench", "--sample-count", str(samples)],
            cwd=repo,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )
    text = (output / "divan.log").read_text(encoding="utf-8", errors="replace")
    if completed.returncode != 0:
        raise TckPerfError(f"the Divan TCK benchmark failed (exit {completed.returncode})")
    check_fault_announcement(text, fault, "the Divan TCK benchmark")
    raw = codspeed_root / "target/codspeed/walltime/raw_results/divan"
    if not raw.is_dir():
        raise TckPerfError("the Divan TCK benchmark wrote no raw results")
    shutil.copytree(raw, output / "divan")


def _run_whole_tck(
    executable: Path,
    repo: Path,
    output: Path,
    tmp: Path,
    fault: Mapping[str, Any] | None,
    limits: Limits,
) -> tuple[dict[str, Any], dict[str, Any]]:
    from benchexec.runexecutor import RunExecutor

    timings = output / "bdd-timings"
    env = _base_env(tmp) | _fault_env(fault)
    env |= {
        "CARGO_MANIFEST_DIR": str(repo / "crates/graphforge-api"),
        "BDD_TIMING_DIR": str(timings),
    }
    executor = RunExecutor(
        use_namespaces=True,
        dir_modes={
            "/": "read-only",
            "/run": "hidden",
            "/tmp": "hidden",
            str(output.resolve()): "full-access",
        },
        container_system_config=True,
        container_tmpfs=False,
    )
    log_path = output / "whole-tck.log"
    with measure_hybrid_pressure() as hybrid_pressure:
        raw = executor.execute_run(
            args=[str(executable)],
            output_filename=str(log_path),
            walltimelimit=int(limits.wall_seconds),
            hardtimelimit=int(limits.cpu_seconds),
            memlimit=limits.memory_bytes,
            cores=list(limits.cores),
            environments={"keepEnv": {}, "newEnv": env},
            workingDir=str(repo),
        )
    # On a hybrid cgroup layout BenchExec measures through v1 and omits PSI;
    # fill it from the unified hierarchy exactly as native admission does.
    if is_hybrid_cgroup_layout() and benchexec_cgroup_version() == 1:
        for key, value in hybrid_pressure().items():
            raw.setdefault(key, value)
    log = log_path.read_text(encoding="utf-8", errors="replace")
    verdict = parse_tck_verdict(log)
    check_fault_announcement(log, fault, "the whole-TCK run")
    exit_code = getattr(raw.get("exitcode"), "value", None)
    correctness = (
        exit_code == 0
        and verdict["regressed"] == 0
        and verdict["passing"] == verdict["total"] == verdict["baseline"]
    )
    diagnostic = _read_json(timings / "report.json", "Cucumber diagnostic report")
    suites = {suite["suite"]: suite for suite in diagnostic["suites"]}
    # GraphForge-side telemetry for normalize_run: the diagnostic Cucumber
    # sums, which BenchExec's authoritative wall time is checked against.
    telemetry = {
        "status": "passed" if correctness else "failed",
        "source": "cucumber-diagnostic-scenario-sums",
        "phases": [
            {
                "phase": f"{name}_scenarios",
                "duration_ms": round(suites[name]["distribution"]["sum_ms"]),
            }
            for name in ("api", "tck")
        ],
    }
    benchexec = normalize_run(
        benchexec=adapt_run_result(raw, correctness=correctness),
        graphforge=telemetry,
        limits=limits,
    )
    tck_keys = [
        scenario["key"]
        for scenario in suites["tck"]["scenarios"]
        if scenario["outcome"] == "passed"
    ]
    whole = {
        "verdict": verdict,
        "tck_keys": tck_keys,
        "api_scenarios": suites["api"]["distribution"]["count"],
        "fixture_profile": diagnostic["fixture_profile"],
        "concurrency": diagnostic["tck_concurrency"],
        "partial": diagnostic["partial"],
    }
    return benchexec, whole


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )


def measure(args: argparse.Namespace) -> Path:
    """Build, admit, measure and write one run directory."""
    import benchexec

    # The existing native admission path: it runs the admission probe under
    # the system Python with a filtered environment (an inherited TMPDIR on
    # the durable work volume breaks the probe's container).
    from graphforge_bench.progressive_run import ControllerError, _native_authority

    repo = args.repo_root.resolve()
    fault = (
        {"scenario": args.fault_scenario, "delay_ms": args.fault_delay_ms}
        if args.fault_delay_ms is not None or args.fault_scenario is not None
        else None
    )
    if fault is not None and not _valid_fault(fault):
        raise TckPerfError("--fault-delay-ms and --fault-scenario must be given together")
    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
    output = (args.output_dir or repo / "target/tck-perf" / stamp).resolve()
    output.mkdir(parents=True, exist_ok=False)
    tmp = output / "tmp"
    tmp.mkdir()
    temp_fs = filesystem_type(tmp)
    if temp_fs not in DURABLE_FILESYSTEMS:
        raise TckPerfError(f"temp root {tmp} is {temp_fs}; use ext4/xfs/btrfs storage")

    try:
        admission = _native_authority()
        _write_json(output / "admission.json", admission)
        require_local_admission(admission)
    except (ControllerError, EvidenceError) as error:
        raise TckPerfError(str(error)) from error

    rustc_vv = subprocess.check_output(["rustc", "-vV"], cwd=repo, text=True).strip()
    features = sorted(args.features)
    bdd = _cargo_build(repo, ["test", "--test", "bdd"], "bdd", features)
    bench = _cargo_build(repo, ["bench", "--bench", "tck_scenarios"], "tck_scenarios", features)

    cores = tuple(sorted(os.sched_getaffinity(0)))
    limits = Limits(
        args.wall_limit_seconds,
        args.wall_limit_seconds * len(cores),
        args.memory_limit_bytes,
        cores,
    )
    benchexec_doc, whole = _run_whole_tck(bdd, repo, output, tmp, fault, limits)
    _write_json(output / "benchexec.json", benchexec_doc)
    if benchexec_doc["outcome"] != Outcome.PASSED:
        raise TckPerfError(f"BenchExec whole-TCK run did not pass: {benchexec_doc['outcome']}")
    _run_divan(bench, repo, output, tmp, args.sample_count, fault)

    host = host_facts(Path("/proc/cpuinfo").read_text(), Path("/proc/meminfo").read_text())
    provenance = {
        "schema": PROVENANCE_SCHEMA,
        "host": {
            "cpu_model": host["cpu_model"],
            "logical_cpus": os.cpu_count(),
            "memory_bytes": host["memory_bytes"],
            "runner_label": args.runner_label,
        },
        "build": {
            "rustc": rustc_vv,
            "profile": BUILD_PROFILE,
            "target": rustc_target(rustc_vv),
            "features": features,
        },
        "workload": {
            "fixture_profile": whole["fixture_profile"],
            "concurrency": whole["concurrency"],
            "corpus_digest": corpus_digest(repo / "tests/tck/features"),
            "suite_selection": {"api_bdd": whole["api_scenarios"] > 0, "tck": "whole-corpus"},
            "scenario_order": {"whole_tck": "cucumber-file-order", "divan": "divan-name-sorted"},
            "temp_root_filesystem": temp_fs,
            "tool_versions": {
                "benchexec": benchexec.__version__,
                "codspeed-divan-compat": locked_version(
                    (repo / "Cargo.lock").read_text(encoding="utf-8"), "codspeed-divan-compat"
                ),
            },
            "sample_counts": {"divan_samples_per_scenario": args.sample_count, "benchexec_runs": 1},
        },
        "fault_injection": fault,
    }
    verdict = whole["verdict"]
    _write_json(
        output / "run.json",
        {
            "schema": RUN_SCHEMA,
            "provenance": provenance,
            "complete": whole["partial"] is False and whole["fixture_profile"] == FIXTURE_PROFILE,
            "whole_tck": {
                "benchexec": "benchexec.json",
                "benchexec_sha256": _sha256_file(output / "benchexec.json"),
                "binary_sha256": _sha256_file(bdd),
                "tck_total": verdict["total"],
                "tck_passing": verdict["passing"],
                "tck_regressed": verdict["regressed"],
                "tck_scenario_keys_sha256": key_set_digest(whole["tck_keys"]),
            },
            "divan": {
                "mode": "bench",
                "raw_results": "divan",
                "binary_sha256": _sha256_file(bench),
            },
        },
    )
    shutil.rmtree(tmp, ignore_errors=True)
    return output


def run_check(args: argparse.Namespace, run_dir: Path) -> int:
    repo = args.repo_root.resolve()
    run = load_run(run_dir)
    resolved = resolve_baseline(
        args.baseline, host_local_baseline_path(), repo / COMMITTED_BASELINE
    )
    baseline = load_baseline(*resolved) if resolved else None
    result = check(
        run,
        baseline,
        require_compatible=args.require_compatible,
        github_actions=os.environ.get("GITHUB_ACTIONS") == "true",
    )
    _write_json(run_dir / "report.json", result.report)
    for line in result.lines:
        print(line, file=sys.stderr)
    print(
        f"TCK perf: baseline_status={result.report['baseline_status']} "
        f"findings={len(result.report['findings'])} report={run_dir / 'report.json'}",
        file=sys.stderr,
    )
    if args.capture_baseline is not None:
        destination = args.capture_baseline
        capture_baseline(run, destination, repo)
        print(f"TCK perf: captured host-local baseline {destination}", file=sys.stderr)
    return result.exit_code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "check"):
        command = commands.add_parser(name)
        command.add_argument("--repo-root", type=Path, default=Path.cwd())
        command.add_argument(
            "--baseline", type=Path, help="explicit baseline (first in resolution order)"
        )
        command.add_argument(
            "--require-compatible",
            action="store_true",
            help="fail (exit 3) on a provenance mismatch or a missing baseline",
        )
        command.add_argument(
            "--capture-baseline",
            nargs="?",
            const=Path(),
            type=Path,
            help="store this complete, passing, fault-free run as the host-local baseline "
            "(or at PATH, which must be outside the repository)",
        )
    run = commands.choices["run"]
    run.add_argument("--output-dir", type=Path)
    run.add_argument("--sample-count", type=int, default=DEFAULT_SAMPLE_COUNT)
    run.add_argument("--features", nargs="*", default=[])
    run.add_argument("--runner-label", default=os.environ.get("GF_TCK_PERF_RUNNER_LABEL", "local"))
    run.add_argument("--wall-limit-seconds", type=float, default=7_200.0)
    run.add_argument("--memory-limit-bytes", type=int, default=32 * 1024**3)
    run.add_argument("--fault-delay-ms", type=int, help="test-only known positive")
    run.add_argument("--fault-scenario", help="scenario key, or * for every scenario")
    commands.choices["check"].add_argument("run_dir", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.capture_baseline == Path():
        args.capture_baseline = host_local_baseline_path()
    try:
        run_dir = measure(args) if args.command == "run" else args.run_dir
        return run_check(args, run_dir)
    except (TckPerfError, EvidenceError) as error:
        print(f"TCK perf: error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
