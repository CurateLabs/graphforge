"""Climb one GDC suite's scorecard ladder on the OVHC-AGENCY host (#952, #1893).

One rung: acquire the pinned archive from the dataset cache, then three
BenchExec runs on the work root, one per phase:

1. ``convert``: ``graphforge-benchmark-gdc-scorecard convert`` to import Parquet;
2. ``load``: ``gf import-session`` begin/register/validate/commit;
3. ``query``: ``gf storage-attribution``, then the query driver, which reopens
   the project, reconciles every per-label and per-type count with the rung's
   expected-counts document and times each query with the driver clock.

The expected counts come from the suite's count ladder (``gdc_rung_inputs``).
The driver's written results are then checked against the pinned reference
with the suite's matching rule. A Graphalytics rung also holds the archive's
``.properties`` to the ladder before converting, and derives its reference
from the archive's own reference outputs (``gdc_graphalytics_scorecard``).
Finally the rung workspace is reclaimed by
path and the work root is inventoried; a rung only passes with an empty
inventory.

Each rung gets the Graph500 per-rung envelope: one four-hour wall shared by its
three phases, a 4 GiB process peak (the largest single-process resident
high-water mark in a phase), a free-space admission with the declared reserve,
and a quiet-host launch check. A refused query counts against coverage. A query
that fails at runtime or returns a wrong answer fails the rung with a typed
cause, but every query still runs, so every failure is recorded. The ladder
stops at the first rung that does not pass, and the card is rendered from the
largest passing rung (``gdc_scorecard_card``).
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import secrets
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from typing import Any
import uuid
import xml.etree.ElementTree as ET

from graphforge_bench import gdc_dataset_cache
from graphforge_bench import gdc_graphalytics_scorecard as graphalytics
from graphforge_bench.benchexec_authority import EvidenceError, Limits, normalize_run
from graphforge_bench.gdc_contracts import GdcContractError
from graphforge_bench.gdc_measurement_policy import (
    GdcMeasurementBoundaryError,
    assert_query_latency_authority,
)
from graphforge_bench.gdc_phase import TASK_SCHEMA, TELEMETRY_SCHEMA
from graphforge_bench.gdc_rung_inputs import (
    LadderSpec,
    RungInputError,
    check_reference,
    expected_counts,
    load_ladder_spec,
    read_json,
    read_results,
    sha256_file,
    validate_schema,
)
from graphforge_bench.native_ladder_bundle import collect_inventory
from graphforge_bench.progressive_host_run import (
    DEFAULT_QUIET_HOST_WAIT_SECONDS,
    DEFAULT_RESERVE_BYTES,
    HOST_PROFILE_ID,
    MAXIMUM_WALL_SECONDS,
    SYSTEM_BENCHEXEC_PYTHON,
    HostRunError,
    _host_swap_counters,
    _require_quiet_host,
    _wrap_executable_for_tmp,
    measure_host_capacity,
    producer_files,
    reclaim_workspace,
    require_work_root,
    resolve_host_benchexec_python,
)
from graphforge_bench.progressive_run import (
    ControllerError,
    _native_authority,
    _parse_benchexec_xml,
    _resolve_executable,
    _run_benchexec,
    _stage_benchmark_xml,
    publish_json_no_clobber,
    repository_commit,
)

RESULT_SCHEMA = "graphforge-gdc-rung-result/1"
DEFINITION = "graphforge-gdc-rung-phase-v1"
DRIVER_NAME = "graphforge-benchmark-gdc-scorecard"
LAUNCHER_NAME = "graphforge-gdc-phase"
PHASES = ("convert", "load", "query")
MEMORY_LIMIT_BYTES = 4 * 1024**3
CORES = 16
SHARED_IDENTITY_KEYS = (
    "commit",
    "producer_sha256",
    "graphforge_version",
    "gf_sha256",
    "driver_sha256",
    "benchexec_python_sha256",
    "benchexec_version",
    "ladder_spec_sha256",
)


class LadderError(ValueError):
    """The ladder cannot launch or continue; nothing is recorded as a rung result."""

    def __init__(self, cause: str, message: str = "") -> None:
        super().__init__(f"{cause}: {message}" if message else cause)
        self.cause = cause


@dataclass(frozen=True)
class GdcExecutables:
    gf: Path
    driver: Path
    benchexec_python: Path


BenchExecRunner = Callable[[Path, GdcExecutables, Mapping[str, Any], Path], int]


def host_benchexec(
    stage: Path, executables: GdcExecutables, identities: Mapping[str, Any], work_root: Path
) -> int:
    """The host's BenchExec, through the progressive host run's own launcher."""
    return _run_benchexec(
        stage,
        executables,
        identities,
        durable_root=work_root,
        home=work_root,
        rundefinition=DEFINITION,
    )


def _digest(path: Path) -> str:
    return sha256_file(path)


def gdc_producer_digest(root: Path) -> str:
    """Bind the harness package, this phase definition and the GDC schemas."""
    paths = set(producer_files(root))
    paths.add(root / "definitions" / f"{DEFINITION}.xml")
    paths.update((root / "schemas").glob("gdc-*.json"))
    paths.add(root / "schemas" / "benchexec-run-evidence.json")
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def graphforge_version(gf: Path) -> str:
    completed = subprocess.run([str(gf), "--version"], capture_output=True, text=True, check=False)
    words = completed.stdout.split()
    if completed.returncode != 0 or len(words) != 2:
        raise LadderError("graphforge_version_unavailable", completed.stderr.strip())
    return words[1]


def ladder_identities(
    root: Path, spec: LadderSpec, executables: GdcExecutables, commit: str
) -> dict[str, Any]:
    return {
        "commit": commit,
        "producer_sha256": gdc_producer_digest(root),
        "graphforge_version": graphforge_version(executables.gf),
        "gf_sha256": _digest(executables.gf),
        "driver_sha256": _digest(executables.driver),
        "benchexec_python_sha256": _digest(executables.benchexec_python),
        "benchexec_version": version("BenchExec"),
        "ladder_spec_sha256": _digest(spec.path),
        "host_profile_id": HOST_PROFILE_ID,
    }


def _mount_type(path: Path) -> str:
    best, kind = "", "unknown"
    try:
        lines = Path("/proc/self/mounts").read_text(encoding="utf-8").splitlines()
    except OSError:
        return kind
    resolved = str(path.resolve())
    for line in lines:
        fields = line.split()
        if len(fields) < 3:
            continue
        point = fields[1].replace("\\040", " ")
        inside = resolved == point or resolved.startswith(point.rstrip("/") + "/")
        if inside and len(point) >= len(best):
            best, kind = point, fields[2]
    return kind


def _os_name() -> str:
    try:
        lines = Path("/etc/os-release").read_text(encoding="utf-8").splitlines()
    except OSError:
        return "unknown"
    values = dict(line.split("=", 1) for line in lines if "=" in line)
    name = values.get("NAME", "unknown").strip('"')
    release = values.get("VERSION_ID", "").strip('"')
    return f"{name} {release}".strip()


def _memory_bytes() -> int:
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) * 1024
    raise LadderError("host_memory_unavailable")


def host_facts(work_root: Path, label: str, storage_medium: str) -> dict[str, Any]:
    """Measured host facts for the card's hardware line; label and medium are declared."""
    return {
        "label": label,
        "cores": os.cpu_count() or 0,
        "memory_bytes": _memory_bytes(),
        "storage_medium": storage_medium,
        "filesystem": _mount_type(work_root),
        "os": _os_name(),
    }


def uuid7() -> str:
    millis = int(time.time() * 1000)
    value = (millis << 80) | (0x7 << 76) | (secrets.randbits(12) << 64)
    value |= (0b10 << 62) | secrets.randbits(62)
    return str(uuid.UUID(int=value))


@dataclass
class Ladder:
    """Everything one ladder climb shares across its rungs."""

    root: Path
    spec: LadderSpec
    output_dir: Path
    work_root: Path
    cache_root: Path
    executables: GdcExecutables
    identities: Mapping[str, Any]
    host: Mapping[str, Any]
    opener: gdc_dataset_cache.Opener = gdc_dataset_cache.default_opener
    benchexec: BenchExecRunner = host_benchexec


@dataclass
class PhaseRun:
    name: str
    failure: dict[str, Any] | None
    telemetry: Mapping[str, Any] | None = None
    benchexec: Mapping[str, Any] | None = None
    wall_seconds: float = 0.0


@dataclass
class Rung:
    """One rung attempt's accumulating record."""

    ladder: Ladder
    spec: Mapping[str, Any]
    failures: list[dict[str, Any]] = field(default_factory=list)
    documents: dict[str, str] = field(default_factory=dict)
    phases: dict[str, Any] = field(default_factory=dict)
    counts: dict[str, Any] | None = None

    @property
    def prefix(self) -> str:
        return f"{self.ladder.spec.suite_id}-{self.spec['id']}"

    @property
    def workspace_name(self) -> str:
        return f"gdc-{self.prefix}"

    @property
    def workspace(self) -> Path:
        return self.ladder.work_root / "workspace" / self.workspace_name

    def fail(self, phase: str, cause: str, detail: str) -> None:
        self.failures.append({"phase": phase, "cause": cause, "detail": detail[:2048]})

    def publish(self, suffix: str, document: Mapping[str, Any]) -> str:
        name = f"{self.prefix}-{suffix}.json"
        path = self.ladder.output_dir / name
        publish_json_no_clobber(path, document)
        self.documents[name] = _digest(path)
        return name


def _stage_phase(
    ladder: Ladder, parent: Path, task: dict[str, Any], wall_seconds: int
) -> tuple[Path, dict[str, Any]]:
    """Stage the definition, identity-checked executables and the phase task."""
    stage = Path(tempfile.mkdtemp(prefix="gf-gdc-phase-", dir=parent))
    _stage_benchmark_xml(ladder.root, stage, wall_seconds=wall_seconds, definition=DEFINITION)
    bin_dir = stage / "bin"
    bin_dir.mkdir()
    tmp_dir = (ladder.work_root / "tmp").resolve()
    tmp_dir.mkdir(parents=True, exist_ok=True)
    staged: dict[str, str] = {}
    for name, source, key in (
        ("gf", ladder.executables.gf, "gf_sha256"),
        (DRIVER_NAME, ladder.executables.driver, "driver_sha256"),
    ):
        target = bin_dir / name
        try:
            # Same filesystem: a hard link stages the identical bytes without a copy.
            os.link(source, target)
        except OSError:
            shutil.copy2(source, target)
        target.chmod(target.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        if _digest(target) != ladder.identities.get(key):
            raise ControllerError(f"staged executable identity mismatch: {name}")
        _wrap_executable_for_tmp(target, tmp_dir)
        staged[name] = str(target)
    harness = Path(__file__).resolve().parents[1]
    launcher = bin_dir / LAUNCHER_NAME
    launcher.write_text(
        "#!/bin/sh\n"
        f'export TMPDIR="{tmp_dir}"\n'
        f'export PYTHONPATH="{harness}"\n'
        f'exec "{sys.executable}" -m graphforge_bench.gdc_phase "$@"\n',
        encoding="utf-8",
    )
    launcher.chmod(0o755)
    task = dict(task)
    for key in ("gf", "converter", "driver"):
        if key in task:
            task[key] = staged["gf" if key == "gf" else DRIVER_NAME]
    (stage / "phase.json").write_text(json.dumps(task, indent=2) + "\n", encoding="utf-8")
    stage.chmod(0o777)
    for path in stage.rglob("*"):
        if path.is_dir():
            path.chmod(0o777)
    return stage, task


def _phase_telemetry(raw: Path) -> Mapping[str, Any] | None:
    found: list[Mapping[str, Any]] = []
    for path in sorted(raw.rglob("*.log")):
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, Mapping) and value.get("schema") == TELEMETRY_SCHEMA:
                found.append(value)
    return found[0] if len(found) == 1 else None


def _run_columns(raw: Path) -> dict[str, str]:
    documents = sorted(raw.glob("*.xml"))
    if len(documents) != 1:
        raise ControllerError("exact BenchExec result XML is missing or ambiguous")
    runs = ET.parse(documents[0]).getroot().findall(".//run")
    if len(runs) != 1:
        raise ControllerError("BenchExec result must contain exactly one run")
    return {
        str(column.attrib.get("title")): str(column.attrib.get("value"))
        for column in runs[0]
        if column.attrib.get("value") is not None
    }


def benchexec_measurements(raw: Path, *, correctness: bool) -> dict[str, Any]:
    """BenchExec's run columns, including the exit value of a tool that failed."""
    measured = dict(_parse_benchexec_xml(raw, correctness=correctness))
    columns = _run_columns(raw)
    if (
        measured["exit_code"] is None
        and not measured["timed_out"]
        and measured["termination_reason"] in (None, "")
    ):
        if columns.get("exitsignal"):
            measured["signal"] = int(columns["exitsignal"])
        elif columns.get("returnvalue") is not None:
            measured["exit_code"] = int(columns["returnvalue"])
    return measured


def classify_phase(
    measured: Mapping[str, Any] | None,
    telemetry: Mapping[str, Any] | None,
    benchexec: Mapping[str, Any] | None,
    *,
    swapped: bool,
) -> tuple[str, str] | None:
    """The typed cause of a phase that did not pass, or None.

    A resource limit BenchExec enforced comes first, since it explains a missing
    or failed telemetry line; then the phase's own failure; then the 4 GiB
    process envelope; then host swap, which makes the measurement unusable.
    """
    if measured is None:
        return "benchexec_failed", "BenchExec produced no result"
    if measured.get("timed_out") or measured.get("termination_reason") == "walltime":
        return "rung_wall_exceeded", "BenchExec stopped the phase at the rung wall"
    if measured.get("termination_reason") == "memory":
        return "memory_limit_exceeded", "BenchExec stopped the phase at its memory limit"
    if measured.get("termination_reason") not in (None, ""):
        return "benchexec_failed", f"terminated: {measured.get('termination_reason')}"
    if telemetry is None:
        return "phase_telemetry_missing", "no graphforge-gdc-phase/1 line in the run log"
    failure = telemetry.get("failure")
    if isinstance(failure, Mapping):
        return str(failure.get("cause")), f"{failure.get('step')}: {failure.get('detail')}"
    peak = telemetry.get("peak_rss_bytes")
    if not isinstance(peak, int) or isinstance(peak, bool) or peak <= 0:
        return "phase_telemetry_missing", "the phase reports no process peak RSS"
    if peak > MEMORY_LIMIT_BYTES:
        return "memory_limit_exceeded", f"process peak RSS {peak} exceeds {MEMORY_LIMIT_BYTES}"
    if swapped:
        return "host_swapped", "host swap counters rose during the phase"
    if benchexec is None or benchexec.get("outcome") != "passed":
        return "benchexec_failed", f"BenchExec outcome {benchexec and benchexec.get('outcome')}"
    return None


def run_phase(rung: Rung, name: str, task: dict[str, Any], wall_seconds: int) -> PhaseRun:
    ladder = rung.ladder
    with tempfile.TemporaryDirectory(prefix=".gf-gdc-authority-", dir=ladder.work_root) as parent:
        stage, task = _stage_phase(
            ladder, Path(parent), {"schema": TASK_SCHEMA, **task}, wall_seconds
        )
        swap_before = _host_swap_counters()
        status = ladder.benchexec(stage, ladder.executables, ladder.identities, ladder.work_root)
        swap_after = _host_swap_counters()
        swapped = any(swap_after[key] > value for key, value in swap_before.items())
        raw = stage / "raw"
        telemetry = _phase_telemetry(raw) if raw.is_dir() else None
        measured: dict[str, Any] | None
        try:
            measured = benchexec_measurements(
                raw, correctness=telemetry is not None and telemetry.get("status") == "passed"
            )
        except (ControllerError, OSError, ValueError, ET.ParseError):
            measured = None
        document = None
        invalid: str | None = None
        if measured is not None and telemetry is not None:
            limits = Limits(float(wall_seconds), None, MEMORY_LIMIT_BYTES, tuple(range(CORES)))
            try:
                document = normalize_run(benchexec=measured, graphforge=telemetry, limits=limits)
                validate_schema(ladder.root, "benchexec-run-evidence.json", document)
            except (EvidenceError, RungInputError) as error:
                document, invalid = None, str(error)
        cause = classify_phase(measured, telemetry, document, swapped=swapped)
        if invalid is not None:
            cause = ("benchexec_evidence_invalid", invalid)
        elif cause is None and status != 0:
            cause = ("benchexec_failed", f"BenchExec exited {status}")
        if cause is not None and raw.is_dir():
            shutil.copytree(raw, ladder.output_dir / f"{rung.prefix}-{name}-benchexec-raw")
    run = PhaseRun(
        name,
        None if cause is None else {"phase": name, "cause": cause[0], "detail": cause[1]},
        telemetry,
        document,
        float(measured["wall_seconds"]) if measured is not None else 0.0,
    )
    phase: dict[str, Any] = {
        "outcome": document["outcome"] if document is not None else None,
        "wall_seconds": measured["wall_seconds"] if measured is not None else None,
        "cpu_seconds": measured["cpu_seconds"] if measured is not None else None,
        "peak_rss_bytes": telemetry.get("peak_rss_bytes") if telemetry is not None else None,
        "benchexec_document": None,
    }
    if document is not None:
        phase["benchexec_document"] = rung.publish(f"{name}-benchexec", document)
    rung.phases[name] = phase
    return run


def _acquire(rung: Rung) -> Path:
    ladder, spec = rung.ladder, rung.spec
    acquired = gdc_dataset_cache.acquire(
        suite_path=ladder.spec.suite_declaration,
        profile=str(ladder.spec.document["identity_profile"]),
        dataset_ids=[spec["dataset_id"]],
        cache_root=ladder.cache_root,
        work_root=ladder.work_root,
        root=ladder.spec.profile_root,
        repo_root=ladder.root.parent,
        opener=ladder.opener,
    )
    extracted = Path(acquired["extracted"][spec["dataset_id"]])
    subdir = spec.get("input_subdir")
    return extracted / subdir if subdir else extracted


def _execute(rung: Rung) -> None:
    """Acquire, convert, load, query and check; record failures, never raise them."""
    ladder, spec = rung.ladder, rung.spec
    try:
        input_root = _acquire(rung)
    except (gdc_dataset_cache.DatasetCacheError, GdcContractError) as error:
        rung.fail("acquisition", error.cause, str(error))
        return
    if ladder.spec.document["metric_shape"] == "graphalytics":
        try:
            # The archive's .properties must agree with the committed ladder and
            # workload (counts, direction, algorithms, source vertices).
            graphalytics.check_archive(ladder.spec, spec, input_root)
        except RungInputError as error:
            rung.fail("acquisition", error.cause, str(error))
            return
    workspace = rung.workspace
    workspace.mkdir(parents=True)
    remaining = MAXIMUM_WALL_SECONDS

    def phase(name: str, task: dict[str, Any]) -> PhaseRun | None:
        nonlocal remaining
        if remaining < 1:
            rung.fail(name, "rung_wall_exceeded", "the rung's four-hour wall is spent")
            return None
        run = run_phase(rung, name, task, int(remaining))
        remaining -= int(run.wall_seconds + 0.999_999)
        if run.failure is not None:
            rung.failures.append(run.failure)
        return run

    converted = workspace / "converted"
    convert = phase(
        "convert",
        {
            "phase": "convert",
            "converter": DRIVER_NAME,
            "mapping": str(_mapping_path(ladder.spec, spec["id"]).resolve()),
            "input_root": str(input_root.resolve()),
            "output_dir": str(converted),
        },
    )
    if convert is None or convert.failure is not None:
        return
    try:
        manifest = read_json(converted / "conversion-manifest.json")
        counts = expected_counts(ladder.spec, spec["id"], manifest)
    except RungInputError as error:
        rung.fail("counts", error.cause, str(error))
        return
    rung.counts = {
        "published": counts.published,
        "published_snapshot": counts.published_snapshot,
        "discrepancies": counts.discrepancies,
    }
    expected_name = rung.publish("expected-counts", counts.expected)
    project = workspace / "project"
    load = phase(
        "load",
        {
            "phase": "load",
            "gf": "gf",
            "project": str(project),
            "converted": str(converted),
            "operation_uuid": uuid7(),
        },
    )
    if load is None or load.failure is not None:
        return
    reference = spec["reference"]
    results_dir = workspace / "results"
    if reference is not None:
        results_dir.mkdir()
    evidence_path = workspace / "query-evidence.json"
    query = phase(
        "query",
        {
            "phase": "query",
            "gf": "gf",
            "driver": DRIVER_NAME,
            "project": str(project),
            "workload": str(ladder.spec.resolve(spec["workload"]).resolve()),
            "expected_counts": str((ladder.output_dir / expected_name).resolve()),
            "evidence": str(evidence_path),
            "results_dir": str(results_dir) if reference is not None else None,
        },
    )
    # A query that failed at runtime still wrote evidence for every binding;
    # check it so every wrong answer is recorded too. Any other query-phase
    # failure (a refused reconciliation, a resource limit) leaves nothing to check.
    if query is None or (query.failure is not None and query.failure["cause"] != "query_failed"):
        return
    try:
        evidence = read_json(evidence_path)
    except RungInputError as error:
        rung.fail("query", "query_evidence_missing", str(error))
        return
    rung.publish("query-evidence", evidence)
    try:
        assert_query_latency_authority(evidence)
    except GdcMeasurementBoundaryError as error:
        rung.fail("query", "latency_authority_refused", f"{error.cause}: {error}")
        return
    _check(rung, evidence, results_dir, input_root)


def _mapping_path(spec: LadderSpec, rung_id: str) -> Path:
    ladder = spec.counts_ladder()
    if "datasets" in ladder:
        entry = next(item for item in ladder["datasets"] if item["id"] == rung_id)
        return spec.resolve(entry["load_mapping"])
    return spec.resolve(ladder["load_mapping"])


def _check(rung: Rung, evidence: Mapping[str, Any], results_dir: Path, input_root: Path) -> None:
    reference_spec = rung.spec["reference"]
    reference = None
    reference_sha256 = None
    try:
        if reference_spec is not None and "archive_outputs" in reference_spec:
            # Millions of rows at the real rungs: built by code from the
            # archive's reference outputs, not schema-validated row by row.
            reference, reference_sha256 = graphalytics.archive_reference(
                rung.ladder.spec, rung.spec, input_root
            )
        elif reference_spec is not None:
            path = rung.ladder.spec.resolve(reference_spec["path"])
            reference = read_json(path)
            validate_schema(rung.ladder.root, "gdc-rung-reference.json", reference)
            if (reference["suite_id"], reference["rung_id"]) != (
                rung.ladder.spec.suite_id,
                rung.spec["id"],
            ):
                raise RungInputError("invalid_rung_spec", "the reference belongs to another rung")
            reference_sha256 = sha256_file(path)
        results = read_results(results_dir) if reference is not None else {}
    except RungInputError as error:
        rung.fail("check", error.cause, str(error))
        return
    correctness = check_reference(
        reference=reference,
        reference_sha256=reference_sha256,
        evidence=evidence,
        results=results,
    )
    rung.publish("correctness", correctness)
    for mismatch in correctness["mismatches"]:
        if mismatch["cause"] != "query_failed":  # already the query phase's failure
            rung.fail(
                "check",
                mismatch["cause"],
                f"{mismatch['query_id']}/{mismatch['binding_id']}: {mismatch['detail']}",
            )
    if reference is not None and correctness["checked"] == 0:
        rung.fail("check", "reference_not_applied", "no measured result has a reference entry")


def _teardown(rung: Rung) -> dict[str, Any]:
    ladder = rung.ladder
    reclaim_workspace(ladder.work_root, rung.workspace_name)
    inventory = collect_inventory(ladder.work_root, None)
    rung.publish("inventory", inventory)
    if not inventory["empty"]:
        rung.fail("teardown", "teardown_incomplete", ", ".join(inventory["entries"])[:2048])
    return {"empty": inventory["empty"], "entries": inventory["entries"]}


def _result(
    rung: Rung,
    status: str,
    *,
    launch_host: Mapping[str, Any] | None,
    capacity: Mapping[str, Any] | None,
    inventory: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "schema": RESULT_SCHEMA,
        "suite_id": rung.ladder.spec.suite_id,
        "rung_id": rung.spec["id"],
        "label": rung.spec["label"],
        "status": status,
        "failure": rung.failures[0] if rung.failures else None,
        "failures": rung.failures,
        "identities": dict(rung.ladder.identities),
        "limits": {
            "wall_seconds": MAXIMUM_WALL_SECONDS,
            "memory_bytes": MEMORY_LIMIT_BYTES,
            "cores": CORES,
        },
        "launch_host": dict(launch_host) if launch_host is not None else None,
        "work_root_capacity": dict(capacity) if capacity is not None else None,
        "host": dict(rung.ladder.host),
        "phases": rung.phases,
        "counts": rung.counts,
        "documents": rung.documents,
        "inventory": inventory,
        "claim": "engineering_evidence_only",
        "certification": False,
    }


def _publish_result(rung: Rung, result: dict[str, Any]) -> dict[str, Any]:
    validate_schema(rung.ladder.root, "gdc-rung-result.json", result)
    publish_json_no_clobber(rung.ladder.output_dir / f"{rung.prefix}-result.json", result)
    return result


def run_rung(
    ladder: Ladder,
    rung_spec: Mapping[str, Any],
    *,
    launch_host: Mapping[str, Any] | None,
    capacity: Mapping[str, Any],
) -> dict[str, Any]:
    """Run one admitted rung to a published result; teardown always runs."""
    rung = Rung(ladder, rung_spec)
    if rung.workspace.is_symlink():
        raise LadderError("workspace_link_refused", str(rung.workspace))
    # A crash can leave a workspace without any published evidence; it holds
    # nothing this attempt can trust.
    reclaim_workspace(ladder.work_root, rung.workspace_name)
    try:
        _execute(rung)
    finally:
        inventory = _teardown(rung)
    status = "failed" if rung.failures else "passed"
    result = _result(rung, status, launch_host=launch_host, capacity=capacity, inventory=inventory)
    return _publish_result(rung, result)


def not_admitted(
    ladder: Ladder,
    rung_spec: Mapping[str, Any],
    *,
    launch_host: Mapping[str, Any] | None,
    cause: str,
) -> dict[str, Any]:
    rung = Rung(ladder, rung_spec)
    rung.fail("admission", cause, "the work root's free space is at or below the reserve")
    inventory = _teardown(rung)
    result = _result(
        rung, "not_admitted", launch_host=launch_host, capacity=None, inventory=inventory
    )
    return _publish_result(rung, result)


def _existing_result(ladder: Ladder, rung_spec: Mapping[str, Any]) -> dict[str, Any] | None:
    prefix = f"{ladder.spec.suite_id}-{rung_spec['id']}"
    path = ladder.output_dir / f"{prefix}-result.json"
    if not path.exists():
        if any(ladder.output_dir.glob(f"{prefix}-*")):
            raise LadderError("existing_attempt_requires_inspection", prefix)
        return None
    result = read_json(path)
    validate_schema(ladder.root, "gdc-rung-result.json", result)
    recorded = {key: result["identities"].get(key) for key in SHARED_IDENTITY_KEYS}
    current = {key: ladder.identities.get(key) for key in SHARED_IDENTITY_KEYS}
    if recorded != current:
        raise LadderError("ladder_identity_mismatch", prefix)
    return result


def climb(
    ladder: Ladder,
    *,
    reserved_headroom_bytes: int,
    quiet_host_wait_seconds: int,
    through: str | None = None,
    quiet_host: Callable[[int], Mapping[str, Any]] = _require_quiet_host,
) -> list[dict[str, Any]]:
    """Advance the ladder, resuming after its passed prefix; stop at the first non-pass."""
    rungs = list(ladder.spec.document["rungs"])
    if through is not None:
        ids = [rung["id"] for rung in rungs]
        if through not in ids:
            raise LadderError("unknown_rung", through)
        rungs = rungs[: ids.index(through) + 1]
    results: list[dict[str, Any]] = []
    for rung_spec in rungs:
        result = _existing_result(ladder, rung_spec)
        if result is None:
            try:
                launch_host = quiet_host(quiet_host_wait_seconds)
            except HostRunError as error:
                # Not a rung outcome: nothing launched, so nothing is recorded.
                raise LadderError("host_not_quiet", str(error)) from error
            try:
                capacity = measure_host_capacity(ladder.work_root, reserved_headroom_bytes)
            except HostRunError as error:
                results.append(
                    not_admitted(ladder, rung_spec, launch_host=launch_host, cause=str(error))
                )
                break
            result = run_rung(ladder, rung_spec, launch_host=launch_host, capacity=capacity)
        results.append(result)
        if result["status"] != "passed":
            break
    return results


def ladder_finished(spec: LadderSpec, results: Sequence[Mapping[str, Any]]) -> bool:
    """A ladder is finished once a rung did not pass or every rung passed."""
    if results and results[-1]["status"] != "passed":
        return True
    return len(results) == len(spec.document["rungs"])


def prepare(
    *,
    root: Path,
    ladder_path: Path,
    output_dir: Path,
    work_root: Path,
    cache_root: Path,
    executables: GdcExecutables,
    commit: str,
    host_label: str,
    storage_medium: str,
    opener: gdc_dataset_cache.Opener = gdc_dataset_cache.default_opener,
    benchexec: BenchExecRunner = host_benchexec,
) -> Ladder:
    work_root = require_work_root(work_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir.resolve()
    if output == work_root or output.is_relative_to(work_root):
        raise LadderError("output_dir_inside_work_root", str(output))
    spec = load_ladder_spec(root, ladder_path)
    return Ladder(
        root=root,
        spec=spec,
        output_dir=output,
        work_root=work_root,
        cache_root=cache_root,
        executables=executables,
        identities=ladder_identities(root, spec, executables, commit),
        host=host_facts(work_root, host_label, storage_medium),
        opener=opener,
        benchexec=benchexec,
    )


def main(argv: Sequence[str] | None = None) -> int:
    from graphforge_bench import gdc_scorecard_card as card

    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="climb a suite's ladder, then render its card")
    render = commands.add_parser("render", help="render the card of a finished ladder")
    inventory = commands.add_parser("inventory", help="inventory the work root")
    for command in (run, render):
        command.add_argument("--ladder", type=Path, required=True)
        command.add_argument("--output-dir", type=Path, required=True)
    for command in (run, inventory):
        command.add_argument("--work-root", type=Path, required=True)
    run.add_argument("--dataset-cache", type=Path, required=True)
    run.add_argument("--gf", required=True)
    run.add_argument("--driver", required=True)
    run.add_argument("--benchexec-python", default=str(SYSTEM_BENCHEXEC_PYTHON))
    run.add_argument("--reserved-headroom-bytes", type=int, default=DEFAULT_RESERVE_BYTES)
    run.add_argument("--quiet-host-wait-seconds", type=int, default=DEFAULT_QUIET_HOST_WAIT_SECONDS)
    run.add_argument("--through", help="stop after this rung id, leaving the ladder open")
    run.add_argument("--host-label", default="OVHC-AGENCY")
    run.add_argument("--storage-medium", default="NVMe")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[2]
    try:
        if args.command == "inventory":
            document = collect_inventory(require_work_root(args.work_root), None)
            print(json.dumps(document, sort_keys=True))
            return 0 if document["empty"] else 2
        if args.command == "render":
            spec = load_ladder_spec(root, args.ladder)
            paths = card.write_card(root, spec, args.output_dir.resolve())
            print(paths["text"].read_text(encoding="utf-8"), end="")
            return 0
        executables = GdcExecutables(
            gf=_resolve_executable(args.gf, "gf"),
            driver=_resolve_executable(args.driver, DRIVER_NAME),
            benchexec_python=resolve_host_benchexec_python(Path(args.benchexec_python)),
        )
        ladder = prepare(
            root=root,
            ladder_path=args.ladder,
            output_dir=args.output_dir,
            work_root=args.work_root,
            cache_root=args.dataset_cache,
            executables=executables,
            commit=repository_commit(root),
            host_label=args.host_label,
            storage_medium=args.storage_medium,
        )
        try:
            # The same native BenchExec admission the Graph500 host ladder requires.
            _native_authority()
        except (ControllerError, OSError) as error:
            raise LadderError("native_authority_unavailable", str(error)) from error
        results = climb(
            ladder,
            reserved_headroom_bytes=args.reserved_headroom_bytes,
            quiet_host_wait_seconds=args.quiet_host_wait_seconds,
            through=args.through,
        )
        summary: dict[str, Any] = {
            "suite_id": ladder.spec.suite_id,
            "rungs": [
                {"rung_id": r["rung_id"], "status": r["status"], "failure": r["failure"]}
                for r in results
            ],
            "finished": ladder_finished(ladder.spec, results),
        }
        # A ladder ends at its first typed failure by design; that is a
        # result, published on the card. Only a card that cannot be built
        # (no passing rung) or a refused launch is an error.
        if summary["finished"]:
            summary["card"] = str(card.write_card(root, ladder.spec, ladder.output_dir)["text"])
        print(json.dumps(summary, sort_keys=True))
        return 0
    except (LadderError, RungInputError, card.CardError) as error:
        print(json.dumps({"error": {"cause": error.cause, "message": str(error)}}), file=sys.stderr)
        return 2
    except (ControllerError, OSError) as error:
        print(
            json.dumps({"error": {"cause": "execution_failed", "message": str(error)}}),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
