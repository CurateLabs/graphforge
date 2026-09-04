"""Fail-closed OVHC-AGENCY host ladder under local-linux-cgroups-v2.

Runs S18-S26 through ordinary BenchExec authority on a durable ext4 work root.
Fly image digests and disposable provider mounts are intentionally out of scope;
retain those adapters as optional offline tooling only.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from importlib.metadata import PackageNotFoundError, version
import json
from pathlib import Path
import shutil
import stat
import sys
import tempfile
from typing import Any

from jsonschema import Draft202012Validator

from graphforge_bench.progressive_qualification import (
    QualificationError,
    load_profiles,
    project,
)
from graphforge_bench.progressive_run import (
    ControllerError,
    Executables,
    _digest,
    _native_authority,
    _run_benchexec,
    _stage_benchmark_xml,
    ingest_benchexec_result,
    publish_json_no_clobber,
    repository_commit,
    resolve_executables,
)

PLAN_SCHEMA = "graphforge-progressive-host-run-plan/1"
RESULT_SCHEMA = "graphforge-progressive-host-run-result/1"
HOST_PROFILE_ID = "local-linux-cgroups-v2"
LADDER = (18, 19, 20, 22, 24, 25, 26)
LOCAL_SCALES = (18, 19)
PROVIDER_SCALES = (20, 22, 24, 25, 26)
CAPACITY_RATE_FIELDS = (
    "physical_read_bytes_per_second",
    "physical_write_bytes_per_second",
    "reader_calls_per_second",
    "publication_work_per_second",
)


class HostRunError(ControllerError):
    """Host-native ladder planning or execution refused."""


def _json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise HostRunError(f"invalid evidence document: {path.name}") from error


def _validate(root: Path, schema_name: str, document: Any) -> None:
    schema = _json(root / "schemas" / schema_name)
    error = next(Draft202012Validator(schema).iter_errors(document), None)
    if error is not None:
        raise HostRunError(f"{schema_name} validation failed: {error.message}")


def _profile_path(root: Path, scale: int) -> Path:
    suffix = "local" if scale in LOCAL_SCALES else "provider"
    return root / "profiles" / "graph500" / f"s{scale}-{suffix}.json"


def _profile_id(scale: int) -> str:
    suffix = "local" if scale in LOCAL_SCALES else "provider"
    return f"graph500-s{scale}-{suffix}"


def _source_for(scale: int) -> str:
    return "progressive_profile" if scale in LOCAL_SCALES else "canonical_ladder"


def require_work_root(work_root: Path) -> Path:
    """Refuse tmpfs/cross-volume work roots; durable projects must stay on process-root FS."""
    try:
        resolved = work_root.resolve(strict=True)
    except OSError as error:
        raise HostRunError("work_root_invalid") from error
    if not resolved.is_dir() or resolved.is_symlink():
        raise HostRunError("work_root_invalid")
    try:
        if resolved.stat().st_dev != Path("/").stat().st_dev:
            raise HostRunError("work_root_invalid")
    except OSError as error:
        raise HostRunError("work_root_invalid") from error
    return resolved


def load_host_capacity(root: Path, path: Path) -> Mapping[str, Any]:
    document = _json(path)
    _validate(root, "host-capacity.json", document)
    if document.get("host_profile_id") != HOST_PROFILE_ID:
        raise HostRunError("host capacity profile mismatch")
    return document


def _passed_rung(root: Path, output_dir: Path, scale: int) -> Mapping[str, Any] | None:
    path = output_dir / f"s{scale}-rung.json"
    if not path.exists():
        return None
    document = _json(path)
    _validate(root, "progressive-qualification-rung-evidence.json", document)
    if (
        document.get("scale") != scale
        or document.get("status") != "passed"
        or document.get("profile_id") != _profile_id(scale)
        or document.get("source") != _source_for(scale)
        or document.get("live_edges") != 16 * (1 << scale)
    ):
        raise HostRunError(f"S{scale} evidence is not a passed matching host rung")
    return document


def completed_prefix(root: Path, output_dir: Path) -> list[Mapping[str, Any]]:
    completed: list[Mapping[str, Any]] = []
    gap = False
    for scale in LADDER:
        rung = _passed_rung(root, output_dir, scale)
        if rung is None:
            gap = True
            continue
        if gap:
            raise HostRunError("rung evidence is out of order")
        result = _json(output_dir / f"s{scale}-result.json")
        _validate(root, "progressive-host-run-result.json", result)
        if result.get("status") != "passed" or result.get("rung") != f"S{scale}":
            raise HostRunError(f"S{scale} host result contradicts passed rung evidence")
        completed.append(rung)
    return completed


def require_order(root: Path, output_dir: Path, scale: int) -> None:
    completed = completed_prefix(root, output_dir)
    expected_completed = list(LADDER[: LADDER.index(scale)])
    actual = [int(item["scale"]) for item in completed]
    if actual != expected_completed:
        raise HostRunError(f"S{scale} requires completed prefix {expected_completed}")


def _admit_projection(
    root: Path,
    output_dir: Path,
    scale: int,
    capacity: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    if scale not in PROVIDER_SCALES:
        raise HostRunError("projection is only required for S20-S26")
    profiles = load_profiles(root / "profiles" / "graph500")
    profile = next(item for item in profiles if item.scale == scale)
    completed = completed_prefix(root, output_dir)
    rates = {name: int(capacity[name]) for name in CAPACITY_RATE_FIELDS}
    try:
        evidence = project(profile, completed, rates)
    except QualificationError as error:
        raise HostRunError("projection_refused") from error
    _validate(root, "progressive-qualification-evidence.json", evidence)
    if evidence.get("decision") != "admitted":
        raise HostRunError("projection_refused")
    path = output_dir / f"s{scale}-projection.json"
    publish_json_no_clobber(path, evidence)
    return evidence, _digest(path)


def build_plan(
    *,
    root: Path,
    output_dir: Path,
    scale: int,
    commit: str,
    executables: Executables,
    capacity: Mapping[str, Any] | None,
) -> dict[str, Any]:
    require_order(root, output_dir, scale)
    profile_path = _profile_path(root, scale)
    profile = _json(profile_path)
    _validate(root, "progressive-qualification-profile.json", profile)
    if profile.get("id") != _profile_id(scale) or profile.get("scale") != scale:
        raise HostRunError("canonical rung profile contradicts the selected scale")
    generator_digest = "sha256:" + _digest(root / "runners/graph500-generator/src/main.rs")
    if generator_digest != profile["generator"]["identity"]:
        raise HostRunError("generator source identity contradicts the checked-in profile")
    try:
        benchexec_version = version("BenchExec")
    except PackageNotFoundError as error:
        raise HostRunError("BenchExec package identity unavailable") from error
    host_profile_path = root / "profiles" / f"{HOST_PROFILE_ID}.json"
    identities: dict[str, Any] = {
        "commit": commit,
        "host_profile_id": HOST_PROFILE_ID,
        "host_profile_sha256": _digest(host_profile_path),
        "profile_id": profile["id"],
        "profile_sha256": _digest(profile_path),
        "generator": generator_digest,
        "generator_executable_sha256": _digest(executables.generator),
        "gf_sha256": _digest(executables.gf),
        "certify_sha256": _digest(executables.certify),
        "benchexec_python_sha256": _digest(executables.benchexec_python),
        "benchexec_version": benchexec_version,
    }
    if scale in PROVIDER_SCALES:
        if capacity is None:
            raise HostRunError("host capacity is required before S20+")
        _, projection_digest = _admit_projection(root, output_dir, scale, capacity)
        identities["admitted_projection_sha256"] = projection_digest
    plan = {
        "schema": PLAN_SCHEMA,
        "rung": f"S{scale}",
        "execution": "native_linux_benchexec_host",
        "identities": identities,
        "limits": {"wall_seconds": 14_400, "memory_bytes": 4_294_967_296, "cores": 16},
        "outputs": [
            f"s{scale}-plan.json",
            f"s{scale}-benchexec.json",
            f"s{scale}-graphforge.json",
            f"s{scale}-rung.json",
            f"s{scale}-result.json",
        ],
        "claim": "engineering_evidence_only",
    }
    _validate(root, "progressive-host-run-plan.json", plan)
    return plan


def _rewrite_profile_for_work_root(profile_text: str, scale: int, work_root: Path) -> str:
    relative = f"workspace/s{scale}"
    absolute = str((work_root / relative).resolve())
    return profile_text.replace(f'"{relative}/', f'"{absolute}/').replace(
        f'"{relative}"', f'"{absolute}"'
    )


def _safe_stage_host(
    root: Path,
    profile_path: Path,
    executables: Executables,
    identities: Mapping[str, Any],
    parent: Path,
    *,
    scale: int,
    work_root: Path,
) -> Path:
    stage = Path(tempfile.mkdtemp(prefix="gf-host-progressive-", dir=parent))
    profile_text = _rewrite_profile_for_work_root(
        profile_path.read_text(encoding="utf-8"), scale, work_root
    )
    (stage / "profile.json").write_text(profile_text, encoding="utf-8")
    _stage_benchmark_xml(root, stage)
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
        staged.chmod(staged.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        if _digest(staged) != identities.get(identity_key):
            raise HostRunError(f"staged executable identity mismatch: {name}")
    stage.chmod(0o777)
    for path in stage.rglob("*"):
        if path.is_dir():
            path.chmod(0o777)
    return stage


def _result(
    plan: Mapping[str, Any],
    status: str,
    failure: str | None,
    artifacts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "schema": RESULT_SCHEMA,
        "rung": plan["rung"],
        "status": status,
        "failure": failure,
        "identities": plan["identities"],
        "artifacts": artifacts,
        "claim": "engineering_evidence_only",
    }


def reclaim_rung_workspace(work_root: Path, scale: int) -> None:
    """Delete a rung's datasets/projects after evidence is accepted."""
    target = work_root / "workspace" / f"s{scale}"
    if target.exists():
        shutil.rmtree(target)


def inventory_work_root(work_root: Path) -> dict[str, Any]:
    """Return a sanitized inventory proving temporary debris state."""
    workspace = work_root / "workspace"
    remaining: list[str] = []
    if workspace.is_dir():
        for path in sorted(workspace.rglob("*")):
            if path.is_file() or path.is_dir():
                remaining.append(path.relative_to(work_root).as_posix())
    return {
        "schema": "graphforge-host-work-root-inventory/1",
        "host_profile_id": HOST_PROFILE_ID,
        "workspace_entries": remaining,
        "empty": remaining == [],
    }


def run(
    *,
    root: Path,
    output_dir: Path,
    work_root: Path,
    scale: int,
    plan: Mapping[str, Any],
    executables: Executables,
) -> None:
    work_root = require_work_root(work_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    (work_root / "workspace" / f"s{scale}").mkdir(parents=True, exist_ok=True)
    (work_root / "tmp").mkdir(parents=True, exist_ok=True)
    result_path = output_dir / f"s{scale}-result.json"
    try:
        _native_authority()
    except (ControllerError, OSError) as error:
        failed = _result(plan, "failed", "native_authority_unavailable")
        _validate(root, "progressive-host-run-result.json", failed)
        publish_json_no_clobber(result_path, failed)
        raise HostRunError("native_authority_unavailable") from error
    profile_path = _profile_path(root, scale)
    try:
        with tempfile.TemporaryDirectory(prefix=".gf-host-authority-", dir=work_root) as temporary:
            identities = plan["identities"]
            if not isinstance(identities, Mapping):
                raise HostRunError("run plan identities are malformed")
            stage = _safe_stage_host(
                root,
                profile_path,
                executables,
                identities,
                Path(temporary),
                scale=scale,
                work_root=work_root,
            )
            status = _run_benchexec(
                stage,
                executables,
                identities,
                durable_root=work_root,
                home=work_root,
            )
            if status != 0:
                failed = _result(plan, "failed", "benchexec_failed")
                _validate(root, "progressive-host-run-result.json", failed)
                publish_json_no_clobber(result_path, failed)
                raise HostRunError("benchexec_failed")
            benchexec, graphforge, rung = ingest_benchexec_result(
                root=root,
                stage=stage,
                scale=scale,
                plan=plan,
                profile_id=str(identities["profile_id"]),
                source=_source_for(scale),
            )
    except HostRunError:
        raise
    except (ControllerError, OSError, ValueError) as error:
        failure = (
            "ordinary_receipt_missing"
            if "receipt" in str(error) or "evidence" in str(error)
            else "staging_failed"
        )
        failed = _result(plan, "failed", failure)
        _validate(root, "progressive-host-run-result.json", failed)
        publish_json_no_clobber(result_path, failed)
        raise HostRunError(failure) from error
    artifact_paths = {
        "plan_sha256": output_dir / f"s{scale}-plan.json",
        "benchexec_sha256": output_dir / f"s{scale}-benchexec.json",
        "graphforge_sha256": output_dir / f"s{scale}-graphforge.json",
        "rung_sha256": output_dir / f"s{scale}-rung.json",
    }
    publish_json_no_clobber(artifact_paths["benchexec_sha256"], benchexec)
    publish_json_no_clobber(artifact_paths["graphforge_sha256"], graphforge)
    publish_json_no_clobber(artifact_paths["rung_sha256"], rung)
    artifacts = {name: _digest(path) for name, path in artifact_paths.items()}
    passed = _result(plan, "passed", None, artifacts)
    _validate(root, "progressive-host-run-result.json", passed)
    publish_json_no_clobber(result_path, passed)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument(
        "--rung",
        choices=tuple(f"S{scale}" for scale in LADDER),
    )
    action.add_argument("--inventory", action="store_true")
    action.add_argument("--reclaim", type=int, choices=LADDER)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--gf")
    parser.add_argument("--certify")
    parser.add_argument("--generator")
    parser.add_argument("--benchexec-python", default=sys.executable)
    parser.add_argument("--host-capacity", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[2]
    try:
        work_root = require_work_root(args.work_root)
        if args.inventory:
            document = inventory_work_root(work_root)
            publish_json_no_clobber(args.output_dir / "work-root-inventory.json", document)
            print(json.dumps(document, sort_keys=True))
            return 0 if document["empty"] else 2
        if args.reclaim is not None:
            reclaim_rung_workspace(work_root, args.reclaim)
            return 0
        if not all((args.gf, args.certify, args.generator)):
            raise HostRunError("rung execution requires gf, certify, and generator")
        scale = int(args.rung[1:])
        capacity = None
        if scale in PROVIDER_SCALES:
            if args.host_capacity is None:
                raise HostRunError("S20+ requires --host-capacity")
            capacity = load_host_capacity(root, args.host_capacity)
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
            capacity=capacity,
        )
        publish_json_no_clobber(args.output_dir / f"s{scale}-plan.json", plan)
        if not args.dry_run:
            run(
                root=root,
                output_dir=args.output_dir,
                work_root=work_root,
                scale=scale,
                plan=plan,
                executables=executables,
            )
        return 0
    except (HostRunError, ControllerError, QualificationError) as error:
        print(
            json.dumps(
                {
                    "schema": RESULT_SCHEMA,
                    "status": "failed",
                    "failure": str(error),
                },
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
