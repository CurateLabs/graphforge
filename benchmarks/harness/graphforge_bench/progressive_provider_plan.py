"""No-spend planner for the sequential S18--S26 provider ladder.

The planner is the control-plane boundary for the real provider executor.  It
does not contact Fly, choose an image, provision a Machine, or execute a
benchmark.  It only accepts closed rung evidence, applies the checked-in
projection policy, and emits the next exact profile to execute.
"""

from __future__ import annotations

import argparse
from collections.abc import Mapping
import hashlib
import json
from pathlib import Path
import re
from typing import Any

from jsonschema import Draft202012Validator

from graphforge_bench.progressive_qualification import (
    Profile,
    QualificationError,
    load_profiles,
    project,
    select_next,
)

SCALES = (18, 19, 20, 22, 24, 25, 26)
COMMIT = re.compile(r"^[0-9a-f]{40}$")
PLAN_SCHEMA = "graphforge-progressive-provider-plan/1"
EXECUTION_REFUSAL = "provider_executor_unavailable"


class ProviderPlanError(ValueError):
    """A malformed input or a refused no-spend admission decision."""


def _read_json(path: Path, message: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProviderPlanError(message) from error


def _profile_path(root: Path, profile: Profile) -> Path:
    suffix = "local" if profile.scale in (18, 19) else "provider"
    path = root / "profiles" / "graph500" / f"s{profile.scale}-{suffix}.json"
    if not path.is_file():
        raise ProviderPlanError("checked-in progressive profile is unavailable")
    return path


def _sha256(path: Path) -> str:
    try:
        value = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError as error:
        raise ProviderPlanError("checked-in progressive profile is unavailable") from error
    return value


def _validate_rung(root: Path, value: Any, scale: int) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ProviderPlanError("completed rung evidence is malformed")
    schema = _read_json(
        root / "schemas" / "progressive-qualification-rung-evidence.json",
        "rung schema is unavailable",
    )
    error = next(Draft202012Validator(schema).iter_errors(value), None)
    if error is not None:
        raise ProviderPlanError("completed rung evidence is not schema-valid")
    expected_source = "progressive_profile" if scale in (18, 19) else None
    if (
        value.get("scale") != scale
        or value.get("status") != "passed"
        or value.get("live_edges") != 16 * (1 << scale)
        or value.get("profile_id")
        != f"graph500-s{scale}-{'local' if scale in (18, 19) else 'provider'}"
        or (expected_source is not None and value.get("source") != expected_source)
    ):
        raise ProviderPlanError("completed rung evidence does not match its canonical profile")
    return value


def _validate_result_identity(
    output_dir: Path, *, scale: int, commit: str, profile_id: str
) -> None:
    """Bind each completed rung to the exact source commit and profile."""
    value = _read_json(output_dir / f"s{scale}-result.json", "completed rung result is unavailable")
    if not isinstance(value, Mapping):
        raise ProviderPlanError("completed rung result is malformed")
    identities = value.get("identities")
    if not isinstance(identities, Mapping):
        raise ProviderPlanError("completed rung result identity is missing")
    if (
        value.get("rung") != f"S{scale}"
        or value.get("status") != "passed"
        or identities.get("commit") != commit
        or identities.get("profile_id") != profile_id
    ):
        raise ProviderPlanError(
            "completed rung result is not bound to the requested commit/profile"
        )


def completed_rungs(
    root: Path, output_dir: Path, *, commit: str | None = None
) -> list[Mapping[str, Any]]:
    """Load a contiguous prefix of passed rung evidence and reject gaps."""
    result: list[Mapping[str, Any]] = []
    gap = False
    for scale in SCALES:
        path = output_dir / f"s{scale}-rung.json"
        if not path.is_file():
            gap = True
            continue
        if gap:
            raise ProviderPlanError("rung evidence is out of order")
        rung = _validate_rung(root, _read_json(path, "completed rung evidence is malformed"), scale)
        if commit is not None:
            _validate_result_identity(
                output_dir,
                scale=scale,
                commit=commit,
                profile_id=str(rung["profile_id"]),
            )
        result.append(rung)
    return result


def _profile(profiles: tuple[Profile, ...], scale: int) -> Profile:
    try:
        return next(item for item in profiles if item.scale == scale)
    except StopIteration as error:
        raise ProviderPlanError("canonical progressive profile is unavailable") from error


def plan_provider_ladder(
    *,
    root: Path,
    output_dir: Path,
    commit: str,
    maximum_scale: int,
    provider_capacity: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return one immutable, sanitized next-rung plan without provider calls."""
    if COMMIT.fullmatch(commit) is None:
        raise ProviderPlanError("commit must be a lowercase full Git object ID")
    if maximum_scale not in SCALES:
        raise ProviderPlanError("maximum scale is not a canonical ladder rung")
    profiles = load_profiles(root / "profiles" / "graph500")
    completed = completed_rungs(root, output_dir, commit=commit)
    if any(int(item["scale"]) > maximum_scale for item in completed):
        raise ProviderPlanError("completed rung exceeds the authorized maximum scale")
    try:
        selected = select_next(profiles, completed, provider_capacity)
    except QualificationError as error:
        raise ProviderPlanError("progressive admission policy refused the next rung") from error
    if selected is None or selected.scale > maximum_scale:
        raise ProviderPlanError("next rung is not admitted within the authorized maximum scale")
    profile_path = _profile_path(root, selected)
    projection: Mapping[str, Any] | None = None
    if selected.execution == "provider":
        try:
            projection = project(selected, completed, provider_capacity)
        except QualificationError as error:
            raise ProviderPlanError("provider projection is not admitted") from error
        if projection["decision"] != "admitted":
            raise ProviderPlanError("provider projection is not admitted")
        projection_schema = _read_json(
            root / "schemas" / "progressive-qualification-evidence.json",
            "provider projection schema is unavailable",
        )
        error = next(Draft202012Validator(projection_schema).iter_errors(projection), None)
        if error is not None:
            raise ProviderPlanError("provider projection is not schema-valid")
    plan = {
        "schema": PLAN_SCHEMA,
        "status": "admitted",
        "commit": commit,
        "maximum_scale": maximum_scale,
        "completed_scales": [item["scale"] for item in completed],
        "next_rung": f"S{selected.scale}",
        "execution": selected.execution,
        "profile_id": selected.id,
        "profile_path": profile_path.relative_to(root).as_posix(),
        "profile_sha256": "sha256:" + _sha256(profile_path),
        "projection": projection,
        "execution_authorized": selected.execution == "local",
        "execution_refusal": None if selected.execution == "local" else EXECUTION_REFUSAL,
        "claim": "engineering_evidence_only",
    }
    schema_path = root / "schemas" / "progressive-provider-plan.json"
    schema = _read_json(schema_path, "provider plan schema is unavailable")
    error = next(Draft202012Validator(schema).iter_errors(plan), None)
    if error is not None:
        raise ProviderPlanError("provider plan failed its schema")
    return plan


def require_execution_authority(plan: Mapping[str, Any]) -> None:
    """Refuse provider execution until its image and resource authority exist."""
    if plan.get("execution") == "provider":
        raise ProviderPlanError(
            "provider execution is refused until a dedicated provider image and BenchExec "
            "authority exist"
        )


def main(argv: list[str] | None = None) -> int:
    """Write one no-spend provider plan for a protected workflow step."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--maximum-scale", type=int, required=True)
    parser.add_argument("--provider-capacity", type=Path)
    parser.add_argument("--plan-out", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        capacity = None
        if args.provider_capacity is not None:
            value = _read_json(args.provider_capacity, "provider capacity evidence is malformed")
            if not isinstance(value, Mapping):
                raise ProviderPlanError("provider capacity evidence is malformed")
            capacity = value
        plan = plan_provider_ladder(
            root=args.root,
            output_dir=args.output_dir,
            commit=args.commit,
            maximum_scale=args.maximum_scale,
            provider_capacity=capacity,
        )
        args.plan_out.parent.mkdir(parents=True, exist_ok=True)
        args.plan_out.write_text(
            json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    except (OSError, ProviderPlanError) as error:
        print(json.dumps({"schema": PLAN_SCHEMA, "status": "refused", "failure": str(error)}))
        return 2
    print(json.dumps(plan, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
