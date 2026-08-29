"""Fail-closed controller for native-Linux progressive qualification runs.

The controller owns ordering, immutable executable identity, safe BenchExec
staging, and evidence validation.  It deliberately does not provision hosts or
invent metrics that the ordinary GraphForge lifecycle did not emit.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
from importlib.metadata import PackageNotFoundError, version
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any
import xml.etree.ElementTree as ET

from jsonschema import Draft202012Validator

from graphforge_bench.benchexec_authority import Limits, normalize_run
from graphforge_bench.local_admission import qualify_local_host
from graphforge_bench.progressive_qualification import QualificationError, load_profiles, project

PLAN_SCHEMA = "graphforge-progressive-run-plan/1"
RESULT_SCHEMA = "graphforge-progressive-run-result/1"
GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
LOCAL_RUNGS = (18, 19)


class ControllerError(ValueError):
    """The requested run is unsafe, out of order, or lacks valid evidence."""


@dataclass(frozen=True)
class Executables:
    gf: Path
    certify: Path
    generator: Path
    benchexec_python: Path


def _json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ControllerError(f"invalid evidence document: {path.name}") from error


def _validate(root: Path, schema_name: str, document: Any) -> None:
    schema = _json(root / "schemas" / schema_name)
    error = next(Draft202012Validator(schema).iter_errors(document), None)
    if error is not None:
        raise ControllerError(f"{schema_name} validation failed: {error.message}")


def _digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def _resolve_executable(value: str, expected_name: str) -> Path:
    candidate = Path(value)
    located = str(candidate) if candidate.is_absolute() else shutil.which(value)
    if located is None:
        raise ControllerError(f"required executable unavailable: {expected_name}")
    resolved = Path(located).resolve(strict=True)
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise ControllerError(f"required executable is not executable: {expected_name}")
    return resolved


def resolve_executables(
    *, gf: str, certify: str, generator: str, benchexec_python: str
) -> Executables:
    return Executables(
        gf=_resolve_executable(gf, "gf"),
        certify=_resolve_executable(certify, "graphforge-benchmark-certify"),
        generator=_resolve_executable(generator, "graphforge-benchmark-graph500-generator"),
        benchexec_python=_resolve_executable(benchexec_python, "python"),
    )


def _commit(value: str) -> str:
    if not GIT_COMMIT.fullmatch(value):
        raise ControllerError("commit must be a lowercase full Git object ID")
    return value


def repository_commit(root: Path) -> str:
    completed = subprocess.run(
        ["git", "-C", str(root.parent), "rev-parse", "HEAD"],
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise ControllerError("repository commit unavailable")
    return _commit(completed.stdout.strip())


def _profile(root: Path, scale: int) -> tuple[Path, Mapping[str, Any]]:
    if scale not in LOCAL_RUNGS:
        raise ControllerError("authoritative local controller accepts only S18 or S19")
    path = root / "profiles" / "graph500" / f"s{scale}-local.json"
    document = _json(path)
    _validate(root, "progressive-qualification-profile.json", document)
    return path, document


def _passed_rung(root: Path, output_dir: Path, scale: int) -> Mapping[str, Any] | None:
    path = output_dir / f"s{scale}-rung.json"
    if not path.exists():
        return None
    document = _json(path)
    _validate(root, "progressive-qualification-rung-evidence.json", document)
    if (
        document.get("scale") != scale
        or document.get("status") != "passed"
        or document.get("profile_id") != f"graph500-s{scale}-local"
        or document.get("source") != "progressive_profile"
        or document.get("live_edges") != 16 * (1 << scale)
    ):
        raise ControllerError(f"S{scale} evidence is not a passed matching rung")
    return document


def require_order(root: Path, output_dir: Path, scale: int) -> None:
    s18 = _passed_rung(root, output_dir, 18)
    s19 = _passed_rung(root, output_dir, 19)
    if scale == 18 and (s18 is not None or s19 is not None):
        raise ControllerError("S18 may run only as the first incomplete rung")
    if scale == 19 and (s18 is None or s19 is not None):
        raise ControllerError("S19 requires exactly one passed S18 rung")


def build_plan(
    *,
    root: Path,
    output_dir: Path,
    scale: int,
    commit: str,
    executables: Executables,
) -> dict[str, Any]:
    require_order(root, output_dir, scale)
    _, profile = _profile(root, scale)
    generator_digest = "sha256:" + _digest(root / "runners/graph500-generator/src/main.rs")
    if generator_digest != profile["generator"]["identity"]:
        raise ControllerError("generator source identity contradicts the checked-in profile")
    try:
        benchexec_version = version("BenchExec")
    except PackageNotFoundError as error:
        raise ControllerError("BenchExec package identity unavailable") from error
    identities = {
        "commit": _commit(commit),
        "profile_id": profile["id"],
        "profile_sha256": _digest(root / "profiles/graph500" / f"s{scale}-local.json"),
        "generator": generator_digest,
        "generator_executable_sha256": _digest(executables.generator),
        "gf_sha256": _digest(executables.gf),
        "certify_sha256": _digest(executables.certify),
        "benchexec_python_sha256": _digest(executables.benchexec_python),
        "benchexec_version": benchexec_version,
    }
    plan = {
        "schema": PLAN_SCHEMA,
        "rung": f"S{scale}",
        "execution": "native_linux_benchexec",
        "identities": identities,
        "limits": {"wall_seconds": 14_400, "memory_bytes": 4_294_967_296, "cores": 16},
        "outputs": [
            f"s{scale}-benchexec.json",
            f"s{scale}-graphforge.json",
            f"s{scale}-rung.json",
        ],
        "claim": "engineering_evidence_only",
    }
    _validate(root, "progressive-run-plan.json", plan)
    return plan


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_plan(output_dir: Path, plan: Mapping[str, Any]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{str(plan['rung']).lower()}-plan.json"
    _write_json(path, plan)
    return path


def _safe_stage(
    root: Path,
    profile_path: Path,
    executables: Executables,
    identities: Mapping[str, Any],
    parent: Path,
) -> Path:
    stage = Path(tempfile.mkdtemp(prefix="gf-progressive-", dir=parent))
    shutil.copyfile(profile_path, stage / "profile.json")
    shutil.copyfile(
        root / "definitions/graphforge-progressive-qualification-v1.xml", stage / "benchmark.xml"
    )
    bin_dir = stage / "bin"
    bin_dir.mkdir()
    for name, source, identity_key in (
        ("gf", executables.gf, "gf_sha256"),
        ("graphforge-benchmark-certify", executables.certify, "certify_sha256"),
        (
            "graphforge-benchmark-graph500-generator",
            executables.generator,
            "generator_executable_sha256",
        ),
    ):
        staged = bin_dir / name
        shutil.copy2(source, staged)
        if _digest(staged) != identities.get(identity_key):
            raise ControllerError(f"staged executable identity mismatch: {name}")
    return stage


def _native_authority() -> Mapping[str, Any]:
    if platform.system() != "Linux":
        raise ControllerError("native Linux BenchExec authority is required")
    evidence = qualify_local_host()
    if evidence.get("result") != "passed":
        cause = evidence.get("cause")
        raise ControllerError(f"native BenchExec admission refused: {cause}")
    return evidence


def require_bulk_ingest_capability(receipt: Mapping[str, Any]) -> Mapping[str, Any]:
    """Derive capability only from the ordinary commit receipt produced by this run."""
    construction = receipt.get("construction")
    if (
        receipt.get("contract") != "graphforge-import-session/1"
        or receipt.get("outcome") != "committed"
        or not isinstance(construction, Mapping)
        or not _is_int(construction.get("configured_batch_rows"))
        or construction["configured_batch_rows"] < 65_536
        or not _is_int(construction.get("accepted_chunks"))
        or construction["accepted_chunks"] < 1
        or construction.get("publication_committed") is not True
        or not _is_int(construction.get("input_rows"))
        or construction["input_rows"] < construction["accepted_chunks"]
        or not _is_int(construction.get("input_batches"))
        or construction.get("input_batches") != construction["accepted_chunks"]
    ):
        raise ControllerError("bulk_ingest_capability_unproven")
    return receipt


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _run_benchexec(stage: Path, executables: Executables, identities: Mapping[str, Any]) -> int:
    raw_output = stage / "raw"
    raw_output.mkdir()
    home = stage / "home"
    home.mkdir()
    environment = {
        "HOME": str(home),
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": f"{stage / 'bin'}:{Path(sys.executable).parent}:/usr/bin:/bin",
    }
    command = [
        str(executables.benchexec_python),
        "-m",
        "benchexec",
        "--no-compress-results",
        "--outputpath",
        str(raw_output),
        "--rundefinition",
        "graphforge-progressive-qualification-v1",
        str(stage / "benchmark.xml"),
    ]
    if _digest(executables.benchexec_python) != identities.get("benchexec_python_sha256"):
        raise ControllerError("BenchExec Python identity changed after planning")
    return subprocess.run(command, env=environment, check=False).returncode


def _scaled_number(value: str, *, integral: bool = False) -> int | float:
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(B|kB|MB|GB|s)?", value)
    if match is None:
        raise ControllerError("BenchExec measurement is malformed")
    number = float(match.group(1))
    factor = {None: 1, "B": 1, "kB": 1_000, "MB": 1_000_000, "GB": 1_000_000_000, "s": 1}[
        match.group(2)
    ]
    measured = number * factor
    return int(measured) if integral else measured


def _parse_graphforge_log(raw_output: Path) -> Mapping[str, Any]:
    candidates: list[Mapping[str, Any]] = []
    for path in sorted(raw_output.rglob("*.log")):
        for line in path.read_text(encoding="utf-8", errors="strict").splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (
                isinstance(value, Mapping)
                and value.get("schema") == "graphforge-public-certification/1"
            ):
                candidates.append(value)
    if len(candidates) != 1:
        raise ControllerError("exact GraphForge certification evidence is missing or ambiguous")
    return candidates[0]


def _parse_benchexec_xml(raw_output: Path, *, correctness: bool) -> Mapping[str, Any]:
    documents = sorted(raw_output.glob("*.xml"))
    if len(documents) != 1:
        raise ControllerError("exact BenchExec result XML is missing or ambiguous")
    root = ET.parse(documents[0]).getroot()
    runs = root.findall(".//run")
    if len(runs) != 1:
        raise ControllerError("BenchExec result must contain exactly one run")
    columns = {column.attrib.get("title"): column.attrib.get("value") for column in runs[0]}

    def required(name: str) -> str:
        value = columns.get(name)
        if not isinstance(value, str):
            raise ControllerError(f"BenchExec result is missing {name}")
        return value

    status = required("status")
    exit_code = 0 if status == "DONE" else None
    termination = columns.get("terminationreason")
    if status == "TIMEOUT":
        termination = "walltime"
    elif status in {"OUT OF MEMORY", "MEMORY"}:
        termination = "memory"
    return {
        "wall_seconds": _scaled_number(required("walltime")),
        "cpu_seconds": _scaled_number(required("cputime")),
        "peak_rss_bytes": _scaled_number(required("memory"), integral=True),
        "read_bytes": _scaled_number(required("blkio-read"), integral=True),
        "write_bytes": _scaled_number(required("blkio-write"), integral=True),
        "pressure_cpu_seconds": _scaled_number(required("pressure-cpu-some")),
        "pressure_io_seconds": _scaled_number(required("pressure-io-some")),
        "pressure_memory_seconds": _scaled_number(required("pressure-memory-some")),
        "termination_reason": termination,
        "exit_code": exit_code,
        "signal": None,
        "correctness": correctness,
    }


def _receipts(graphforge: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    result: list[Mapping[str, Any]] = []
    phases = graphforge.get("phases")
    if not isinstance(phases, list):
        raise ControllerError("GraphForge phases are missing")
    for phase in phases:
        if not isinstance(phase, Mapping):
            raise ControllerError("GraphForge phase is malformed")
        values = phase.get("receipts", [])
        if not isinstance(values, list):
            raise ControllerError("GraphForge phase receipts are malformed")
        if any(not isinstance(value, Mapping) for value in values):
            raise ControllerError("GraphForge receipt is malformed")
        result.extend(values)
    return result


def _phase_receipts(graphforge: Mapping[str, Any], name: str) -> list[Mapping[str, Any]]:
    phases = graphforge.get("phases")
    if not isinstance(phases, list):
        raise ControllerError("GraphForge phases are missing")
    matching = [
        phase for phase in phases if isinstance(phase, Mapping) and phase.get("phase") == name
    ]
    if len(matching) != 1 or not isinstance(matching[0].get("receipts", []), list):
        raise ControllerError(f"GraphForge phase receipts are missing: {name}")
    receipts = matching[0].get("receipts", [])
    if any(not isinstance(receipt, Mapping) for receipt in receipts):
        raise ControllerError(f"GraphForge phase receipts are malformed: {name}")
    return receipts


def _query_receipts(
    graphforge: Mapping[str, Any], phase: str, expected: int
) -> list[Mapping[str, Any]]:
    receipts = [
        receipt
        for receipt in _phase_receipts(graphforge, phase)
        if receipt.get("contract") == "graphforge-result-sink/2"
    ]
    if len(receipts) != expected:
        raise ControllerError(f"ordinary query receipts are missing or ambiguous: {phase}")
    for receipt in receipts:
        digest = receipt.get("result_sha256")
        if (
            receipt.get("complete") is not True
            or not isinstance(digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            or not isinstance(receipt.get("query_evidence"), Mapping)
            or receipt["query_evidence"].get("contract") != "graphforge-query-evidence/1"
        ):
            raise ControllerError("ordinary query receipt is incomplete")
    return receipts


def _one_receipt(graphforge: Mapping[str, Any], contract: str) -> Mapping[str, Any]:
    matching = [receipt for receipt in _receipts(graphforge) if receipt.get("contract") == contract]
    if len(matching) != 1:
        raise ControllerError(f"required ordinary receipt is missing or ambiguous: {contract}")
    return matching[0]


def _storage_receipt(graphforge: Mapping[str, Any], phase: str) -> Mapping[str, Any]:
    receipts = _phase_receipts(graphforge, phase)
    matching = [
        receipt
        for receipt in receipts
        if receipt.get("contract") == "graphforge-storage-attribution-command/1"
    ]
    if len(matching) != 1 or matching[0].get("reopen_agrees") is not True:
        raise ControllerError(f"ordinary storage receipt is missing or ambiguous: {phase}")
    storage = matching[0].get("storage")
    if (
        not isinstance(storage, Mapping)
        or storage.get("contract") != "graphforge-storage-attribution/1"
    ):
        raise ControllerError("ordinary storage receipt is incomplete")
    for name in (
        "logical_references",
        "logical_bytes",
        "retained_logical_eof_bytes",
        "allocated_physical_bytes",
        "physical_objects",
    ):
        if (
            isinstance(storage.get(name), bool)
            or not isinstance(storage.get(name), int)
            or storage[name] < 0
        ):
            raise ControllerError(f"ordinary storage receipt omitted {name}")
    return storage


def _construction_metrics(import_receipt: Mapping[str, Any]) -> dict[str, int]:
    construction = import_receipt.get("construction")
    if not isinstance(construction, Mapping):
        raise ControllerError("ordinary import construction evidence is absent")
    application_io = construction.get("application_io")
    totals = application_io.get("totals") if isinstance(application_io, Mapping) else None
    publication = construction.get("publication_work")
    values = {
        "transient_peak_allocated_bytes": construction.get("transient_peak_allocated_bytes"),
        "logical_read_bytes": totals.get("read_bytes") if isinstance(totals, Mapping) else None,
        "logical_write_bytes": totals.get("write_bytes") if isinstance(totals, Mapping) else None,
        "reader_calls": totals.get("read_calls") if isinstance(totals, Mapping) else None,
        "publication_work_units": publication.get("semantic_total_operations")
        if isinstance(publication, Mapping)
        and publication.get("contract") == "graphforge-publication-work/1"
        else None,
    }
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in values.values()
    ):
        raise ControllerError("ordinary import construction metrics are incomplete")
    return {name: int(value) for name, value in values.items()}


def assemble_rung_evidence(
    *, root: Path, scale: int, graphforge: Mapping[str, Any], benchexec: Mapping[str, Any]
) -> dict[str, Any]:
    if graphforge.get("status") != "passed" or benchexec.get("outcome") != "passed":
        raise ControllerError("a failed execution cannot produce passed rung evidence")
    imports = [
        receipt
        for receipt in _receipts(graphforge)
        if receipt.get("contract") == "graphforge-import-session/1"
        and receipt.get("outcome") == "committed"
    ]
    if len(imports) != 1:
        raise ControllerError("ordinary import commit receipt is missing or ambiguous")
    require_bulk_ingest_capability(imports[0])
    source_storage = _storage_receipt(graphforge, "reopen")
    imported_storage = _storage_receipt(graphforge, "reopen_proof")
    lifecycle_storage = _one_receipt(graphforge, "graphforge-lifecycle-storage/1")
    construction = _construction_metrics(imports[0])
    expected_edges = 16 * (1 << scale)
    source_counts = _query_receipts(graphforge, "recount", 2)
    source_hops = _query_receipts(graphforge, "query", 2)
    imported = _query_receipts(graphforge, "reopen_proof", 4)
    imported_counts, imported_hops = imported[:2], imported[2:]
    expected_counts = (1 << scale, expected_edges)
    for index, expected in enumerate(expected_counts):
        source_value = source_counts[index].get("scalar_u64")
        imported_value = imported_counts[index].get("scalar_u64")
        if source_value != expected or imported_value != expected:
            raise ControllerError("recount evidence contradicts the selected rung")
        if source_counts[index]["result_sha256"] != imported_counts[index]["result_sha256"]:
            raise ControllerError("source/imported recount evidence disagrees")
    if any(
        source.get("rows") != 1024
        or imported_receipt.get("rows") != 1024
        or source["result_sha256"] != imported_receipt["result_sha256"]
        for source, imported_receipt in zip(source_hops, imported_hops, strict=True)
    ):
        raise ControllerError("source/imported query evidence contradicts the selected rung")
    lifecycle_names = ("retained_storage_bytes", "transient_peak_storage_bytes")
    for name in lifecycle_names:
        if (
            isinstance(lifecycle_storage.get(name), bool)
            or not isinstance(lifecycle_storage.get(name), int)
            or lifecycle_storage[name] < 0
        ):
            raise ControllerError(f"lifecycle storage receipt omitted {name}")
    authority = benchexec.get("authority")
    if not isinstance(authority, Mapping):
        raise ControllerError("BenchExec authority is missing")
    phases = [phase.get("phase") for phase in graphforge.get("phases", [])]
    rung = {
        "profile_id": f"graph500-s{scale}-local",
        "source": "progressive_profile",
        "scale": scale,
        "live_edges": expected_edges,
        "status": "passed",
        "correctness": True,
        "phases": phases,
        "metrics": {
            "wall_seconds": int(float(authority["wall_seconds"]) + 0.999_999),
            "peak_rss_bytes": int(authority["peak_rss_bytes"]),
            **{name: lifecycle_storage[name] for name in lifecycle_names},
            **{
                name: construction[name]
                for name in (
                    "logical_read_bytes",
                    "logical_write_bytes",
                    "reader_calls",
                    "publication_work_units",
                )
            },
            "physical_read_bytes": int(authority["read_bytes"]),
            "physical_write_bytes": int(authority["write_bytes"]),
        },
        "metric_sources": {
            "benchexec": [
                "wall_seconds",
                "peak_rss_bytes",
                "physical_read_bytes",
                "physical_write_bytes",
            ],
            "storage_attribution": [
                "retained_storage_bytes",
                "transient_peak_storage_bytes",
                "logical_read_bytes",
                "logical_write_bytes",
                "reader_calls",
                "publication_work_units",
            ],
            "query_qualification": ["live_edges", "correctness"],
        },
        "storage_components": {
            "source_allocated_physical_bytes": source_storage["allocated_physical_bytes"],
            "source_retained_logical_eof_bytes": source_storage["retained_logical_eof_bytes"],
            "imported_allocated_physical_bytes": imported_storage["allocated_physical_bytes"],
            "imported_retained_logical_eof_bytes": imported_storage["retained_logical_eof_bytes"],
            **construction,
        },
        "failure": None,
    }
    _validate(root, "progressive-qualification-rung-evidence.json", rung)
    return rung


def ingest_benchexec_result(
    *, root: Path, stage: Path, scale: int, plan: Mapping[str, Any]
) -> tuple[dict[str, Any], Mapping[str, Any], dict[str, Any]]:
    raw_output = stage / "raw"
    graphforge = _parse_graphforge_log(raw_output)
    raw = _parse_benchexec_xml(raw_output, correctness=graphforge.get("status") == "passed")
    limits = plan["limits"]
    benchexec = normalize_run(
        benchexec=raw,
        graphforge=graphforge,
        limits=Limits(
            float(limits["wall_seconds"]),
            float(limits["wall_seconds"]),
            int(limits["memory_bytes"]),
            tuple(range(int(limits["cores"]))),
        ),
    )
    _validate(root, "certification-evidence.json", graphforge)
    _validate(root, "benchexec-run-evidence.json", benchexec)
    rung = assemble_rung_evidence(
        root=root, scale=scale, graphforge=graphforge, benchexec=benchexec
    )
    return benchexec, graphforge, rung


def validate_fixture_bundle(root: Path, bundle: Path, scale: int) -> None:
    """Validate the three closed documents a real run must ultimately produce."""
    benchexec = _json(bundle / "benchexec.json")
    graphforge = _json(bundle / "graphforge.json")
    rung = _json(bundle / "rung.json")
    _validate(root, "benchexec-run-evidence.json", benchexec)
    _validate(root, "certification-evidence.json", graphforge)
    _validate(root, "progressive-qualification-rung-evidence.json", rung)
    if rung.get("scale") != scale or graphforge.get("profile_id") != f"graph500-s{scale}-local":
        raise ControllerError("fixture evidence contradicts the selected rung")
    if benchexec.get("graphforge") != graphforge:
        raise ControllerError("BenchExec and GraphForge evidence disagree")


def run(
    *, root: Path, output_dir: Path, scale: int, plan: Mapping[str, Any], executables: Executables
) -> None:
    _native_authority()
    profile_path, _ = _profile(root, scale)
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="gf-progressive-authority-") as temporary:
        identities = plan["identities"]
        if not isinstance(identities, Mapping):
            raise ControllerError("run plan identities are malformed")
        stage = _safe_stage(root, profile_path, executables, identities, Path(temporary))
        status = _run_benchexec(stage, executables, identities)
        if status != 0:
            result = {
                "schema": RESULT_SCHEMA,
                "rung": f"S{scale}",
                "status": "failed",
                "failure": "benchexec_failed",
                "identities": plan["identities"],
                "claim": "engineering_evidence_only",
            }
            _validate(root, "progressive-run-result.json", result)
            _write_json(output_dir / f"s{scale}-result.json", result)
            raise ControllerError("benchexec_failed")
        try:
            benchexec, graphforge, rung = ingest_benchexec_result(
                root=root, stage=stage, scale=scale, plan=plan
            )
        except (ControllerError, ValueError) as error:
            result = {
                "schema": RESULT_SCHEMA,
                "rung": f"S{scale}",
                "status": "failed",
                "failure": "ordinary_receipt_missing",
                "identities": plan["identities"],
                "claim": "engineering_evidence_only",
            }
            _validate(root, "progressive-run-result.json", result)
            _write_json(output_dir / f"s{scale}-result.json", result)
            raise ControllerError("ordinary_receipt_missing") from error
        _write_json(output_dir / f"s{scale}-benchexec.json", benchexec)
        _write_json(output_dir / f"s{scale}-graphforge.json", graphforge)
        _write_json(output_dir / f"s{scale}-rung.json", rung)
        result = {
            "schema": RESULT_SCHEMA,
            "rung": f"S{scale}",
            "status": "passed",
            "failure": None,
            "identities": plan["identities"],
            "claim": "engineering_evidence_only",
        }
        _validate(root, "progressive-run-result.json", result)
        _write_json(output_dir / f"s{scale}-result.json", result)


def write_s20_projection(root: Path, output_dir: Path, capacity_path: Path) -> Path:
    s18 = _passed_rung(root, output_dir, 18)
    s19 = _passed_rung(root, output_dir, 19)
    if s18 is None or s19 is None:
        raise ControllerError("S20 projection requires passed adjacent S18 and S19 rungs")
    capacity = _json(capacity_path)
    if not isinstance(capacity, Mapping):
        raise ControllerError("provider capacity evidence must be an object")
    s20 = next(
        profile for profile in load_profiles(root / "profiles" / "graph500") if profile.scale == 20
    )
    evidence = project(s20, [s18, s19], capacity)
    _validate(root, "progressive-qualification-evidence.json", evidence)
    path = output_dir / "s20-projection.json"
    _write_json(path, evidence)
    return path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--rung", choices=("S18", "S19"))
    action.add_argument("--project-s20", action="store_true")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--gf")
    parser.add_argument("--certify")
    parser.add_argument("--generator")
    parser.add_argument("--benchexec-python", default=sys.executable)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--fixture-bundle", type=Path)
    parser.add_argument("--provider-capacity", type=Path)
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[2]
    try:
        if args.project_s20:
            if args.provider_capacity is None:
                raise ControllerError("--project-s20 requires --provider-capacity")
            write_s20_projection(root, args.output_dir, args.provider_capacity)
            return 0
        if not all((args.gf, args.certify, args.generator)):
            raise ControllerError("rung execution requires gf, certify, and generator")
        if args.fixture_bundle is not None and not args.dry_run:
            raise ControllerError("fixture bundles are accepted only with --dry-run")
        scale = int(args.rung[1:])
        executables = resolve_executables(
            gf=args.gf,
            certify=args.certify,
            generator=args.generator,
            benchexec_python=args.benchexec_python,
        )
        plan = build_plan(
            root=root,
            output_dir=args.output_dir,
            scale=scale,
            commit=repository_commit(root),
            executables=executables,
        )
        write_plan(args.output_dir, plan)
        if args.fixture_bundle is not None:
            validate_fixture_bundle(root, args.fixture_bundle, scale)
        if not args.dry_run:
            run(
                root=root,
                output_dir=args.output_dir,
                scale=scale,
                plan=plan,
                executables=executables,
            )
        return 0
    except (ControllerError, QualificationError) as error:
        print(json.dumps({"schema": RESULT_SCHEMA, "status": "failed", "failure": str(error)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
