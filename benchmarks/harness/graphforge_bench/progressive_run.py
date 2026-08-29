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

from jsonschema import Draft202012Validator

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


def _safe_stage(root: Path, profile_path: Path, executables: Executables, parent: Path) -> Path:
    stage = Path(tempfile.mkdtemp(prefix="gf-progressive-", dir=parent))
    shutil.copyfile(profile_path, stage / "profile.json")
    shutil.copyfile(
        root / "definitions/graphforge-progressive-qualification-v1.xml", stage / "benchmark.xml"
    )
    bin_dir = stage / "bin"
    bin_dir.mkdir()
    for name, source in (
        ("gf", executables.gf),
        ("graphforge-benchmark-certify", executables.certify),
        ("graphforge-benchmark-graph500-generator", executables.generator),
    ):
        (bin_dir / name).symlink_to(source)
    return stage


def _native_authority() -> Mapping[str, Any]:
    if platform.system() != "Linux":
        raise ControllerError("native Linux BenchExec authority is required")
    evidence = qualify_local_host()
    if evidence.get("result") != "passed":
        cause = evidence.get("cause")
        raise ControllerError(f"native BenchExec admission refused: {cause}")
    return evidence


def require_bulk_ingest_capability(
    root: Path, output_dir: Path, commit: str | None = None
) -> Mapping[str, Any]:
    """Require ordinary import-session proof before spending a rung.

    The current scalar durable path is not silently treated as a valid scale
    implementation.  A later ordinary-path repair must publish this closed,
    commit-bound capability document before the controller can execute.
    """
    path = output_dir / "ordinary-ingest-capability.json"
    if not path.is_file():
        raise ControllerError("bulk_ingest_capability_unproven")
    evidence = _json(path)
    _validate(root, "ordinary-ingest-capability.json", evidence)
    if commit is not None and evidence.get("commit") != commit:
        raise ControllerError("bulk_ingest_capability_commit_mismatch")
    return evidence


def _run_benchexec(stage: Path, executables: Executables) -> int:
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
    return subprocess.run(command, env=environment, check=False).returncode


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
    require_bulk_ingest_capability(root, output_dir, str(plan["identities"]["commit"]))
    _native_authority()
    profile_path, _ = _profile(root, scale)
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="gf-progressive-authority-") as temporary:
        stage = _safe_stage(root, profile_path, executables, Path(temporary))
        status = _run_benchexec(stage, executables)
        # Raw logs remain in the private temporary directory.  Until the
        # ordinary lifecycle emits every progressive metric, accepting a run
        # would fabricate schema-valid evidence.  Fail closed instead.
        result = {
            "schema": RESULT_SCHEMA,
            "rung": f"S{scale}",
            "status": "failed",
            "failure": "metrics_evidence_missing" if status == 0 else "benchexec_failed",
            "identities": plan["identities"],
            "claim": "engineering_evidence_only",
        }
        _validate(root, "progressive-run-result.json", result)
        _write_json(output_dir / f"s{scale}-result.json", result)
        raise ControllerError(str(result["failure"]))


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
