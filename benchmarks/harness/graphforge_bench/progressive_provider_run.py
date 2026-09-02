"""Offline, fail-closed executor for one admitted provider ladder rung.

This module runs *inside* an already admitted provider host.  It never calls a
provider API.  Provisioning and transport are deliberately outside this trust
boundary; the only default process execution is the existing native BenchExec
lifecycle.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
import hashlib
from importlib.metadata import PackageNotFoundError, version
import json
from pathlib import Path
import re
import tempfile
from typing import Any

from jsonschema import Draft202012Validator

from graphforge_bench.progressive_run import (
    ControllerError,
    Executables,
    _digest,
    _native_authority,
    _run_benchexec,
    _safe_stage,
    ingest_benchexec_result,
    publish_json_no_clobber,
    repository_commit,
    resolve_executables,
)

PLAN_SCHEMA = "graphforge-progressive-provider-execution-plan/1"
RESULT_SCHEMA = "graphforge-progressive-provider-run-result/1"
BUILD_SCHEMA = "graphforge-progressive-provider-build/1"
PROVIDER_RUNGS = (20, 22, 24, 25, 26)
LADDER_RUNGS = (18, 19, 20, 22, 24, 25, 26)
IMAGE_DIGEST = re.compile(r"^registry\.fly\.io/[a-z0-9][a-z0-9._/-]*@sha256:[0-9a-f]{64}$")
HEX_DIGEST = re.compile(r"^[0-9a-f]{64}$")
COMMIT = re.compile(r"^[0-9a-f]{40}$")

ExecutionBoundary = Callable[[Path, Executables, Mapping[str, Any]], int]
AuthorityBoundary = Callable[[], Mapping[str, Any]]


class ProviderRunError(ControllerError):
    """The admitted rung or immutable execution boundary is invalid."""


def _execution_commit(root: Path, *, require_image: bool = False) -> str:
    """Read the image attestation, falling back to Git for local no-spend tests."""
    attestation = root.parent / "commit"
    if not attestation.exists():
        if require_image:
            raise ProviderRunError("image commit attestation is unavailable")
        return repository_commit(root)
    try:
        commit = attestation.read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError) as error:
        raise ProviderRunError("image commit attestation is unavailable") from error
    if COMMIT.fullmatch(commit) is None:
        raise ProviderRunError("image commit attestation is malformed")
    return commit


def _read_build_manifest(root: Path, path: Path) -> tuple[Mapping[str, Any], Executables]:
    """Load the read-only build identity and fixed production executables."""
    manifest, _ = _read_document(path)
    executable_identities = manifest.get("executables")
    if (
        manifest.get("schema") != BUILD_SCHEMA
        or manifest.get("commit") != _execution_commit(root, require_image=True)
        or HEX_DIGEST.fullmatch(str(manifest.get("source_tree_sha256"))) is None
        or not isinstance(executable_identities, Mapping)
        or set(executable_identities)
        != {
            "gf_sha256",
            "certify_sha256",
            "generator_executable_sha256",
            "benchexec_python_sha256",
        }
        or any(HEX_DIGEST.fullmatch(str(value)) is None for value in executable_identities.values())
    ):
        raise ProviderRunError("provider build manifest is malformed")
    executables = resolve_executables(
        gf="/usr/local/bin/gf",
        certify="/usr/local/bin/graphforge-benchmark-certify",
        generator="/usr/local/bin/graphforge-benchmark-graph500-generator",
        benchexec_python="/opt/graphforge/benchmarks/.venv/bin/python",
    )
    actual = {
        "gf_sha256": _digest(executables.gf),
        "certify_sha256": _digest(executables.certify),
        "generator_executable_sha256": _digest(executables.generator),
        "benchexec_python_sha256": _digest(executables.benchexec_python),
    }
    if dict(executable_identities) != actual:
        raise ProviderRunError("provider build executable identity mismatch")
    return manifest, executables


def _confined_work_path(path: Path, *, kind: str) -> Path:
    """Require production inputs and outputs to stay below the provider volume."""
    work = Path("/work").resolve(strict=True)
    try:
        resolved = path.resolve(strict=kind == "input")
    except OSError as error:
        raise ProviderRunError(f"{kind} path is unavailable") from error
    if work not in resolved.parents or resolved == work or path.is_symlink():
        raise ProviderRunError(f"{kind} path must be confined below /work")
    if kind == "output":
        if resolved.exists() and not resolved.is_dir():
            raise ProviderRunError("output path must be a directory")
    elif not resolved.is_file():
        raise ProviderRunError("input path is unavailable")
    return resolved


def _require_fresh_outputs(output_dir: Path, plan: Mapping[str, Any]) -> None:
    outputs = plan.get("outputs")
    if not isinstance(outputs, list) or any(not isinstance(name, str) for name in outputs):
        raise ProviderRunError("provider execution output contract is malformed")
    if any((output_dir / name).exists() or (output_dir / name).is_symlink() for name in outputs):
        raise ProviderRunError("selected rung already has evidence")


def _require_work_mount() -> None:
    work = Path("/work")
    if not work.is_dir() or work.is_symlink() or work.resolve() != work or not work.is_mount():
        raise ProviderRunError("provider work volume is unavailable")


def _read_document(path: Path) -> tuple[Mapping[str, Any], str]:
    try:
        encoded = path.read_bytes()
        value = json.loads(encoded)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProviderRunError("required JSON document is unavailable or malformed") from error
    if not isinstance(value, Mapping):
        raise ProviderRunError("required JSON document must be an object")
    return value, hashlib.sha256(encoded).hexdigest()


def _schema(root: Path, name: str, value: Any) -> None:
    try:
        schema = json.loads((root / "schemas" / name).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProviderRunError(f"closed schema unavailable: {name}") from error
    error = next(Draft202012Validator(schema).iter_errors(value), None)
    if error is not None:
        raise ProviderRunError(f"{name} validation failed: {error.message}")


def _scale(value: Any) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"S(20|22|24|25|26)", value):
        raise ProviderRunError("admitted plan does not select one provider rung")
    return int(value[1:])


def _provider_profile(root: Path, scale: int) -> tuple[Path, Mapping[str, Any]]:
    if scale not in PROVIDER_RUNGS:
        raise ProviderRunError("provider executor accepts only S20 through S26")
    path = root / "profiles" / "graph500" / f"s{scale}-provider.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProviderRunError("canonical provider profile is unavailable") from error
    _schema(root, "progressive-qualification-profile.json", value)
    if (
        not isinstance(value, Mapping)
        or value.get("id") != f"graph500-s{scale}-provider"
        or value.get("scale") != scale
        or value.get("execution") != "provider"
    ):
        raise ProviderRunError("canonical provider profile contradicts the selected rung")
    return path, value


def validate_admitted_plan(
    root: Path, plan: Mapping[str, Any]
) -> tuple[int, Path, Mapping[str, Any]]:
    """Validate the planner output and bind it to this exact checkout."""
    _schema(root, "progressive-provider-plan.json", plan)
    scale = _scale(plan.get("next_rung"))
    profile_path, profile = _provider_profile(root, scale)
    expected_relative = f"profiles/graph500/s{scale}-provider.json"
    expected_profile_digest = "sha256:" + _digest(profile_path)
    projection = plan.get("projection")
    expected_completed = list(LADDER_RUNGS[: LADDER_RUNGS.index(scale)])
    expected_sources = profile.get("gate", {}).get("projection_source_scales")
    if (
        plan.get("status") != "admitted"
        or plan.get("execution") != "provider"
        or plan.get("execution_authorized") is not True
        or plan.get("execution_refusal") is not None
        or plan.get("profile_id") != profile["id"]
        or plan.get("profile_path") != expected_relative
        or plan.get("profile_sha256") != expected_profile_digest
        or plan.get("image_digest") is None
        or plan.get("commit") != _execution_commit(root)
        or plan.get("completed_scales") != expected_completed
        or not isinstance(plan.get("maximum_scale"), int)
        or int(plan["maximum_scale"]) < scale
        or not isinstance(projection, Mapping)
        or projection.get("decision") != "admitted"
        or projection.get("target") != f"S{scale}"
        or projection.get("source_scales") != expected_sources
    ):
        raise ProviderRunError("admitted provider plan identity or projection mismatch")
    _schema(root, "progressive-qualification-evidence.json", projection)
    return scale, profile_path, profile


def _benchexec_version() -> str:
    try:
        return version("BenchExec")
    except PackageNotFoundError as error:
        raise ProviderRunError("BenchExec package identity unavailable") from error


def build_execution_plan(
    *,
    root: Path,
    admitted_plan: Mapping[str, Any],
    admitted_plan_sha256: str,
    image_digest: str,
    executables: Executables,
    source_tree_sha256: str,
) -> dict[str, Any]:
    """Create the immutable plan consumed by the in-host execution boundary."""
    if HEX_DIGEST.fullmatch(admitted_plan_sha256) is None:
        raise ProviderRunError("admitted plan digest must be a bare SHA-256 digest")
    if IMAGE_DIGEST.fullmatch(image_digest) is None:
        raise ProviderRunError("provider image must be an immutable Fly OCI digest")
    if HEX_DIGEST.fullmatch(source_tree_sha256) is None:
        raise ProviderRunError("source tree digest must be a bare SHA-256 digest")
    scale, profile_path, profile = validate_admitted_plan(root, admitted_plan)
    if admitted_plan.get("image_digest") != image_digest:
        raise ProviderRunError("transport-observed image digest contradicts admission")
    generator_identity = "sha256:" + _digest(root / "runners/graph500-generator/src/main.rs")
    if generator_identity != profile.get("generator", {}).get("identity"):
        raise ProviderRunError("generator source identity contradicts the provider profile")
    identities = {
        "commit": admitted_plan["commit"],
        "profile_id": profile["id"],
        "profile_sha256": _digest(profile_path),
        "image_digest": image_digest,
        "generator": generator_identity,
        "generator_executable_sha256": _digest(executables.generator),
        "gf_sha256": _digest(executables.gf),
        "certify_sha256": _digest(executables.certify),
        "benchexec_python_sha256": _digest(executables.benchexec_python),
        "benchexec_version": _benchexec_version(),
        "admitted_plan_sha256": admitted_plan_sha256,
        "source_tree_sha256": source_tree_sha256,
    }
    plan = {
        "schema": PLAN_SCHEMA,
        "rung": f"S{scale}",
        "execution": "provider_native_linux_benchexec",
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
    _schema(root, "progressive-provider-run-plan.json", plan)
    return plan


def _assert_identities(
    root: Path,
    plan: Mapping[str, Any],
    executables: Executables,
    *,
    expected_image_digest: str,
    expected_admitted_plan_sha256: str,
    expected_source_tree_sha256: str,
) -> tuple[int, Path]:
    _schema(root, "progressive-provider-run-plan.json", plan)
    scale = _scale(plan.get("rung"))
    profile_path, profile = _provider_profile(root, scale)
    identities = plan.get("identities")
    if not isinstance(identities, Mapping):
        raise ProviderRunError("provider execution identities are malformed")
    actual = {
        "commit": _execution_commit(root),
        "profile_id": profile["id"],
        "profile_sha256": _digest(profile_path),
        "generator": "sha256:" + _digest(root / "runners/graph500-generator/src/main.rs"),
        "generator_executable_sha256": _digest(executables.generator),
        "gf_sha256": _digest(executables.gf),
        "certify_sha256": _digest(executables.certify),
        "benchexec_python_sha256": _digest(executables.benchexec_python),
        "benchexec_version": _benchexec_version(),
        "image_digest": expected_image_digest,
        "admitted_plan_sha256": expected_admitted_plan_sha256,
        "source_tree_sha256": expected_source_tree_sha256,
    }
    if any(identities.get(name) != value for name, value in actual.items()):
        raise ProviderRunError("provider execution identity changed after planning")
    expected_outputs = [
        f"s{scale}-plan.json",
        f"s{scale}-benchexec.json",
        f"s{scale}-graphforge.json",
        f"s{scale}-rung.json",
        f"s{scale}-result.json",
    ]
    if plan.get("outputs") != expected_outputs:
        raise ProviderRunError("provider execution output contract changed after planning")
    return scale, profile_path


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


def run(
    *,
    root: Path,
    output_dir: Path,
    plan: Mapping[str, Any],
    executables: Executables,
    expected_image_digest: str,
    expected_admitted_plan_sha256: str,
    expected_source_tree_sha256: str,
    execution_boundary: ExecutionBoundary = _run_benchexec,
    authority_boundary: AuthorityBoundary = _native_authority,
) -> None:
    """Execute one provider rung without performing any provider operation."""
    identities = plan.get("identities")
    if not isinstance(identities, Mapping):
        raise ProviderRunError("provider execution identities are malformed")
    scale, profile_path = _assert_identities(
        root,
        plan,
        executables,
        expected_image_digest=expected_image_digest,
        expected_admitted_plan_sha256=expected_admitted_plan_sha256,
        expected_source_tree_sha256=expected_source_tree_sha256,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    result_path = output_dir / f"s{scale}-result.json"
    try:
        authority = authority_boundary()
    except (ControllerError, OSError) as error:
        failed = _result(plan, "failed", "native_authority_unavailable")
        _schema(root, "progressive-provider-run-result.json", failed)
        publish_json_no_clobber(result_path, failed)
        raise ProviderRunError("native BenchExec authority is unavailable") from error
    if not isinstance(authority, Mapping) or authority.get("result") != "passed":
        failed = _result(plan, "failed", "native_authority_unavailable")
        _schema(root, "progressive-provider-run-result.json", failed)
        publish_json_no_clobber(result_path, failed)
        raise ProviderRunError("native BenchExec authority is unavailable")
    try:
        temporary_context = tempfile.TemporaryDirectory(
            prefix=".gf-provider-authority-", dir=output_dir
        )
    except OSError as error:
        failed = _result(plan, "failed", "staging_failed")
        _schema(root, "progressive-provider-run-result.json", failed)
        publish_json_no_clobber(result_path, failed)
        raise ProviderRunError("staging_failed") from error
    with temporary_context as temporary:
        try:
            stage = _safe_stage(
                root, profile_path, executables, identities, Path(temporary), scale=scale
            )
        except (ControllerError, OSError) as error:
            failed = _result(plan, "failed", "staging_failed")
            _schema(root, "progressive-provider-run-result.json", failed)
            publish_json_no_clobber(result_path, failed)
            raise ProviderRunError("staging_failed") from error
        try:
            execution_status = execution_boundary(stage, executables, identities)
        except Exception as error:
            failed = _result(plan, "failed", "execution_boundary_failed")
            _schema(root, "progressive-provider-run-result.json", failed)
            publish_json_no_clobber(result_path, failed)
            raise ProviderRunError("execution_boundary_failed") from error
        if execution_status != 0:
            failed = _result(plan, "failed", "benchexec_failed")
            _schema(root, "progressive-provider-run-result.json", failed)
            publish_json_no_clobber(result_path, failed)
            raise ProviderRunError("benchexec_failed")
        try:
            benchexec, graphforge, rung = ingest_benchexec_result(
                root=root,
                stage=stage,
                scale=scale,
                plan=plan,
                profile_id=str(identities["profile_id"]),
                source="canonical_ladder",
            )
        except (ControllerError, ValueError) as error:
            failed = _result(plan, "failed", "ordinary_receipt_missing")
            _schema(root, "progressive-provider-run-result.json", failed)
            publish_json_no_clobber(result_path, failed)
            raise ProviderRunError("ordinary_receipt_missing") from error
        try:
            _schema(root, "benchexec-run-evidence.json", benchexec)
            _schema(root, "certification-evidence.json", graphforge)
            _schema(root, "progressive-qualification-rung-evidence.json", rung)
        except ProviderRunError as error:
            failed = _result(plan, "failed", "ordinary_receipt_missing")
            _schema(root, "progressive-provider-run-result.json", failed)
            publish_json_no_clobber(result_path, failed)
            raise ProviderRunError("ordinary_receipt_missing") from error
        try:
            stored_plan, _ = _read_document(output_dir / f"s{scale}-plan.json")
            if dict(stored_plan) != dict(plan):
                raise ProviderRunError("stored execution plan changed during the run")
        except ProviderRunError as error:
            failed = _result(plan, "failed", "stored_plan_mismatch")
            _schema(root, "progressive-provider-run-result.json", failed)
            publish_json_no_clobber(result_path, failed)
            raise ProviderRunError("stored_plan_mismatch") from error
        artifact_paths = {
            "benchexec_sha256": output_dir / f"s{scale}-benchexec.json",
            "graphforge_sha256": output_dir / f"s{scale}-graphforge.json",
            "rung_sha256": output_dir / f"s{scale}-rung.json",
            "plan_sha256": output_dir / f"s{scale}-plan.json",
        }
        publish_json_no_clobber(artifact_paths["benchexec_sha256"], benchexec)
        publish_json_no_clobber(artifact_paths["graphforge_sha256"], graphforge)
        publish_json_no_clobber(artifact_paths["rung_sha256"], rung)
        artifacts = {name: _digest(path) for name, path in artifact_paths.items()}
        passed = _result(plan, "passed", None, artifacts)
        _schema(root, "progressive-provider-run-result.json", passed)
        publish_json_no_clobber(result_path, passed)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--admitted-plan", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--image-digest", required=True)
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[2]
    try:
        _require_work_mount()
        admitted_path = _confined_work_path(args.admitted_plan, kind="input")
        output_dir = _confined_work_path(args.output_dir, kind="output")
        admitted, admitted_digest = _read_document(admitted_path)
        build_manifest, executables = _read_build_manifest(
            root, Path("/opt/graphforge/build-manifest.json")
        )
        plan = build_execution_plan(
            root=root,
            admitted_plan=admitted,
            admitted_plan_sha256=admitted_digest,
            image_digest=args.image_digest,
            executables=executables,
            source_tree_sha256=str(build_manifest["source_tree_sha256"]),
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        _require_fresh_outputs(output_dir, plan)
        publish_json_no_clobber(output_dir / f"{str(plan['rung']).lower()}-plan.json", plan)
        run(
            root=root,
            output_dir=output_dir,
            plan=plan,
            executables=executables,
            expected_image_digest=args.image_digest,
            expected_admitted_plan_sha256=admitted_digest,
            expected_source_tree_sha256=str(build_manifest["source_tree_sha256"]),
        )
        return 0
    except (OSError, ControllerError) as error:
        print(json.dumps({"schema": RESULT_SCHEMA, "status": "failed", "failure": str(error)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
