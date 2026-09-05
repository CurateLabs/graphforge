"""Fail-closed OVHC-AGENCY host ladder under local-linux-cgroups-v2.

Runs S18-S26 through ordinary BenchExec authority on a durable ext4 work root.
Fly image digests and disposable provider mounts are intentionally out of scope;
retain those adapters as optional offline tooling only.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import hashlib
from importlib.metadata import PackageNotFoundError, version
import json
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
from typing import Any

from jsonschema import Draft202012Validator

from graphforge_bench.native_rung import read_native_rung
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
    _preserve_failure_artifacts,
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
DEFAULT_RESERVE_BYTES = 75 * 1024**3
SYSTEM_BENCHEXEC_PYTHON = Path("/usr/bin/python3")


class HostRunError(ControllerError):
    """Host-native ladder planning or execution refused."""

    def __init__(self, code: str, *, rung: str | None = None, projection: Any = None):
        super().__init__(code)
        self.rung = rung
        self.projection = projection


def producer_files(root: Path) -> list[Path]:
    """Hash the runtime package boundary, excluding docs, tests, and outputs.

    Include all package sources rather than guessing an import closure: relative
    imports, dynamic helpers, and executable admission fixtures are runtime code.
    """
    paths = set((root / "harness/graphforge_bench").rglob("*.py"))
    paths.update(
        root / name
        for name in (
            "definitions/graphforge-progressive-qualification-v1.xml",
            "pyproject.toml",
            "uv.lock",
            "schemas/progressive-host-run-plan.json",
            "schemas/progressive-host-run-result.json",
            "schemas/progressive-qualification-profile.json",
            "schemas/progressive-qualification-evidence.json",
            "schemas/progressive-qualification-rung-evidence.json",
            "schemas/certification-evidence.json",
            "schemas/benchexec-run-evidence.json",
        )
    )
    return sorted(paths)


def producer_digest(root: Path, *, commit: str | None = None) -> str:
    """Bind producer behavior automatically; documentation changes are excluded."""
    paths = set(producer_files(root))
    if commit is not None:
        package = root / "harness/graphforge_bench"
        recorded_paths = subprocess.run(
            [
                "git",
                "-C",
                str(root.parent),
                "ls-tree",
                "-r",
                "--name-only",
                "-z",
                commit,
                "--",
                "benchmarks/harness/graphforge_bench",
            ],
            capture_output=True,
            check=False,
        )
        if recorded_paths.returncode:
            raise HostRunError("host_prefix_producer_identity_mismatch")
        # Legacy receipts have no stored producer digest. Enumerate their tree,
        # not today's files, so deleted runtime helpers remain part of the hash.
        paths = {path for path in paths if not path.is_relative_to(package)}
        paths.update(
            root.parent / name.decode("utf-8")
            for name in recorded_paths.stdout.split(b"\0")
            if name.endswith(b".py")
        )
    digest = hashlib.sha256()
    for path in sorted(paths):
        relative = path.relative_to(root).as_posix()
        if commit is None:
            content = path.read_bytes()
        else:
            recorded = subprocess.run(
                ["git", "-C", str(root.parent), "show", f"{commit}:benchmarks/{relative}"],
                capture_output=True,
                check=False,
            )
            if recorded.returncode:
                raise HostRunError("host_prefix_producer_identity_mismatch")
            content = recorded.stdout
        digest.update(relative.encode("utf-8") + b"\0" + hashlib.sha256(content).digest())
    return digest.hexdigest()


def resolve_host_benchexec_python(candidate: Path | None = None) -> Path:
    """Prefer package BenchExec with pystemd for host cgroup delegation.

    The locked harness venv installs BenchExec without pystemd. Nested under an
    already-delegated systemd scope, that build cannot attach required cgroups.
    System Python on OVHC-AGENCY carries BenchExec 3.x + pystemd and is the
    ReFrame admission interpreter.
    """
    path = Path(candidate) if candidate is not None else SYSTEM_BENCHEXEC_PYTHON
    if not path.is_file():
        raise HostRunError(f"BenchExec Python is missing: {path}")
    probe = subprocess.run(
        [
            str(path),
            "-c",
            "import benchexec, pystemd; print('ok')",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if probe.returncode != 0 or probe.stdout.strip() != "ok":
        raise HostRunError(
            "host ladder requires BenchExec Python with pystemd "
            f"(tried {path}; install system BenchExec/pystemd or pass --benchexec-python)"
        )
    cli = path.parent / "benchexec"
    if not cli.is_file():
        raise HostRunError(f"BenchExec CLI is missing beside {path}")
    return path.resolve()


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


def measure_host_capacity(work_root: Path, reserved_headroom_bytes: int) -> dict[str, int]:
    """Measure space available to this user on the actual work-root filesystem."""
    if isinstance(reserved_headroom_bytes, bool) or reserved_headroom_bytes < 0:
        raise HostRunError("work_root_capacity_invalid")
    free = shutil.disk_usage(work_root).free
    if free <= reserved_headroom_bytes:
        raise HostRunError("work_root_capacity_refused")
    return {"free_bytes": free, "reserved_headroom_bytes": reserved_headroom_bytes}


def validated_host_rung(root: Path, output_dir: Path, scale: int) -> Mapping[str, Any]:
    """Read the shared native receipt validation used by all native consumers."""
    return read_native_rung(root, output_dir, scale)["rung"]


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
    shared_identity = None
    for scale in LADDER:
        rung = _passed_rung(root, output_dir, scale)
        if rung is None:
            gap = True
            continue
        if gap:
            raise HostRunError("rung evidence is out of order")
        validated_host_rung(root, output_dir, scale)
        identities = _json(output_dir / f"s{scale}-result.json")["identities"]
        shared = {
            key: value
            for key, value in identities.items()
            if key not in {"profile_id", "profile_sha256", "admitted_projection_sha256"}
        }
        if shared_identity is not None and shared != shared_identity:
            raise HostRunError("host_prefix_identity_mismatch")
        shared_identity = shared
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
    try:
        evidence = project(profile, completed, native_capacity=capacity)
    except QualificationError as error:
        raise HostRunError("projection_refused") from error
    _validate(root, "progressive-qualification-evidence.json", evidence)
    if evidence.get("decision") != "admitted":
        raise HostRunError("projection_refused", rung=f"S{scale}", projection=evidence)
    encoded = (json.dumps(evidence, indent=2, sort_keys=True) + "\n").encode("utf-8")
    return evidence, hashlib.sha256(encoded).hexdigest()


def build_plan(
    *,
    root: Path,
    output_dir: Path,
    scale: int,
    commit: str,
    executables: Executables,
    capacity: Mapping[str, Any] | None,
    projection: tuple[dict[str, Any], str] | None = None,
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
        "producer_sha256": producer_digest(root),
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
        _, projection_digest = projection or _admit_projection(root, output_dir, scale, capacity)
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
    if capacity is not None:
        plan["work_root_capacity"] = dict(capacity)
    _validate(root, "progressive-host-run-plan.json", plan)
    return plan


def _rewrite_profile_for_work_root(profile_text: str, scale: int, work_root: Path) -> str:
    relative = f"workspace/s{scale}"
    absolute = str((work_root / relative).resolve())
    return profile_text.replace(f'"{relative}/', f'"{absolute}/').replace(
        f'"{relative}"', f'"{absolute}"'
    )


def _wrap_executable_for_tmp(staged: Path, tmp_dir: Path) -> None:
    real = staged.with_name(f"{staged.name}.real")
    staged.rename(real)
    staged.write_text(
        f'#!/bin/sh\nexport TMPDIR="{tmp_dir}"\nexec "{real}" "$@"\n',
        encoding="utf-8",
    )
    staged.chmod(staged.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


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
    tmp_dir = work_root / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
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
        _wrap_executable_for_tmp(staged, tmp_dir.resolve())
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


def inventory_work_root(work_root: Path, output_dir: Path | None = None) -> dict[str, Any]:
    """Inventory real work-root debris, binding evidence to native results when supplied."""
    from graphforge_bench.native_ladder_bundle import collect_inventory

    return collect_inventory(work_root, output_dir)


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
                _preserve_failure_artifacts(stage, output_dir, scale)
                failed = _result(plan, "failed", "benchexec_failed")
                _validate(root, "progressive-host-run-result.json", failed)
                publish_json_no_clobber(result_path, failed)
                raise HostRunError("benchexec_failed")
            try:
                benchexec, graphforge, rung = ingest_benchexec_result(
                    root=root,
                    stage=stage,
                    scale=scale,
                    plan=plan,
                    profile_id=str(identities["profile_id"]),
                    source=_source_for(scale),
                )
            except (ControllerError, OSError, ValueError):
                _preserve_failure_artifacts(stage, output_dir, scale)
                raise
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


def execute_ladder(
    *,
    root: Path,
    output_dir: Path,
    work_root: Path,
    maximum_scale: int,
    executables: Executables,
    commit: str,
    reserved_headroom_bytes: int,
    dry_run: bool = False,
    rung: int | None = None,
) -> list[dict[str, Any]]:
    """Advance the existing ladder once, stopping before any successor on failure."""
    completed = completed_prefix(root, output_dir)
    next_index = len(completed)
    if rung is not None:
        require_order(root, output_dir, rung)
        scales = [rung]
    else:
        scales = list(LADDER[next_index : LADDER.index(maximum_scale) + 1])
    shared_identity = None
    if completed:
        previous = _json(output_dir / f"s{int(completed[0]['scale'])}-result.json")["identities"]
        shared_identity = {
            key: value
            for key, value in previous.items()
            if key not in {"profile_id", "profile_sha256", "admitted_projection_sha256"}
        }
        # Documentation-only checkout changes do not invalidate existing binaries.
        # Continue with the original recorded commit after verifying all producer
        # and input identities below, rather than relabelling old measurements.
        commit = previous["commit"]
        current_producer = producer_digest(root)
        recorded_producer = previous.get("producer_sha256")
        if recorded_producer is None:
            recorded_producer = producer_digest(root, commit=commit)
        if current_producer != recorded_producer:
            raise HostRunError("host_prefix_producer_identity_mismatch")
        expected = {
            "host_profile_sha256": _digest(root / "profiles" / f"{HOST_PROFILE_ID}.json"),
            "generator": "sha256:" + _digest(root / "runners/graph500-generator/src/main.rs"),
            "gf_sha256": _digest(executables.gf),
            "certify_sha256": _digest(executables.certify),
            "generator_executable_sha256": _digest(executables.generator),
            "benchexec_python_sha256": _digest(executables.benchexec_python),
            "benchexec_version": version("BenchExec"),
        }
        if any(previous[key] != value for key, value in expected.items()):
            raise HostRunError("host_prefix_identity_mismatch")
        for accepted in completed:
            accepted_scale = int(accepted["scale"])
            identity = _json(output_dir / f"s{accepted_scale}-result.json")["identities"]
            if identity["profile_sha256"] != _digest(_profile_path(root, accepted_scale)):
                raise HostRunError("host_prefix_identity_mismatch", rung=f"S{accepted_scale}")
        if not dry_run:
            # Resume cleanup after a crash between passed-result publication and
            # reclaim, including when the requested maximum is already complete.
            for accepted in completed:
                reclaim_rung_workspace(work_root, int(accepted["scale"]))

    plans = []
    for scale in scales:
        try:
            # An actual incomplete/failed attempt is immutable. A dry-run creates none.
            if any(
                (output_dir / f"s{scale}-{name}.json").exists()
                for name in ("plan", "projection", "result", "rung", "graphforge", "benchexec")
            ):
                raise HostRunError("existing_attempt_requires_inspection")
            capacity = measure_host_capacity(work_root, reserved_headroom_bytes)
            projection = (
                _admit_projection(root, output_dir, scale, capacity) if scale >= 20 else None
            )
            plan = build_plan(
                root=root,
                output_dir=output_dir,
                scale=scale,
                commit=commit,
                executables=executables,
                capacity=capacity,
                projection=projection,
            )
            if shared_identity is not None and "producer_sha256" not in shared_identity:
                plan["identities"].pop("producer_sha256", None)
            current_shared = {
                key: value
                for key, value in plan["identities"].items()
                if key not in {"profile_id", "profile_sha256", "admitted_projection_sha256"}
            }
            if shared_identity is not None and current_shared != shared_identity:
                raise HostRunError("host_prefix_identity_mismatch")
            shared_identity = current_shared
            plans.append(plan)
            if dry_run:
                # Later projections require actual measurements from this next rung.
                break
            if projection is not None:
                publish_json_no_clobber(output_dir / f"s{scale}-projection.json", projection[0])
            publish_json_no_clobber(output_dir / f"s{scale}-plan.json", plan)
            run(
                root=root,
                output_dir=output_dir,
                work_root=work_root,
                scale=scale,
                plan=plan,
                executables=executables,
            )
            validated_host_rung(root, output_dir, scale)
            reclaim_rung_workspace(work_root, scale)
        except HostRunError as error:
            error.rung = error.rung or f"S{scale}"
            raise
        except (ControllerError, QualificationError, OSError, ValueError) as error:
            code = "execution_io_failed" if isinstance(error, OSError) else str(error)
            raise HostRunError(code, rung=f"S{scale}") from error
    return plans


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--rung", choices=tuple(f"S{scale}" for scale in LADDER))
    action.add_argument("--maximum-scale", type=int, choices=LADDER)
    action.add_argument("--inventory", action="store_true")
    action.add_argument("--reclaim", type=int, choices=LADDER)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--gf")
    parser.add_argument("--certify")
    parser.add_argument("--generator")
    parser.add_argument("--benchexec-python", default=str(SYSTEM_BENCHEXEC_PYTHON))
    parser.add_argument("--reserved-headroom-bytes", type=int, default=DEFAULT_RESERVE_BYTES)
    parser.add_argument(
        "--host-capacity",
        type=Path,
        help="legacy input: only reserve is used; space and rates are measured",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[2]
    try:
        work_root = require_work_root(args.work_root)
        if args.inventory:
            document = inventory_work_root(work_root, args.output_dir)
            publish_json_no_clobber(args.output_dir / "work-root-inventory.json", document)
            print(json.dumps(document, sort_keys=True))
            return 0 if document["empty"] else 2
        if args.reclaim is not None:
            validated_host_rung(root, args.output_dir, args.reclaim)
            reclaim_rung_workspace(work_root, args.reclaim)
            return 0
        if not all((args.gf, args.certify, args.generator)):
            raise HostRunError("rung execution requires gf, certify, and generator")
        reserve = args.reserved_headroom_bytes
        if args.host_capacity is not None:
            reserve = int(load_host_capacity(root, args.host_capacity)["reserved_headroom_bytes"])
        benchexec_python = resolve_host_benchexec_python(Path(args.benchexec_python))
        executables = resolve_executables(
            gf=args.gf,
            certify=args.certify,
            generator=args.generator,
            benchexec_python=str(benchexec_python),
        )
        rung = int(args.rung[1:]) if args.rung else None
        plans = execute_ladder(
            root=root,
            output_dir=args.output_dir,
            work_root=work_root,
            maximum_scale=rung or args.maximum_scale,
            rung=rung,
            executables=executables,
            commit=repository_commit(root),
            reserved_headroom_bytes=reserve,
            dry_run=args.dry_run,
        )
        if args.dry_run:
            print(
                json.dumps(
                    {"plans": plans, "maximum_scale": rung or args.maximum_scale}, sort_keys=True
                )
            )
        return 0
    except (ControllerError, QualificationError, OSError, ValueError) as error:
        failed: dict[str, Any] = {
            "schema": RESULT_SCHEMA,
            "status": "failed",
            "failure": "execution_io_failed" if isinstance(error, OSError) else str(error),
        }
        if isinstance(error, HostRunError):
            failed["rung"] = error.rung
            if error.projection is not None:
                failed["failed_checks"] = [
                    key for key, passed in error.projection["checks"].items() if not passed
                ]
                failed["projection"] = error.projection
        print(json.dumps(failed, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
