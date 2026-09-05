"""Validate one ordinary native rung for controller and evidence consumers."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from graphforge_bench.progressive_qualification import PHASES
from graphforge_bench.progressive_run import ControllerError, assemble_rung_evidence


class NativeRungError(ControllerError):
    """A native result or its ordinary lifecycle evidence is contradictory."""


def _read(path: Path) -> dict[str, Any]:
    try:
        if path.is_symlink():
            raise NativeRungError(f"linked evidence: {path.name}")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as error:
        raise NativeRungError(f"invalid evidence: {path.name}") from error
    if not isinstance(value, dict):
        raise NativeRungError(f"evidence is not an object: {path.name}")
    return value


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_native_rung(root: Path, source: Path, scale: int) -> dict[str, Any]:
    """Check hashes, immutable identities, exact phase success, and ordinary receipts.

    Returns the original documents, so callers can consume the same validated
    result and projection without maintaining a separate validation protocol.
    """
    documents = {}
    for kind, schema in (
        ("result", "progressive-host-run-result.json"),
        ("plan", "progressive-host-run-plan.json"),
        ("benchexec", "benchexec-run-evidence.json"),
        ("graphforge", "certification-evidence.json"),
        ("rung", "progressive-qualification-rung-evidence.json"),
    ):
        value = _read(source / f"s{scale}-{kind}.json")
        error = next(
            Draft202012Validator(_read(root / "schemas" / schema)).iter_errors(value), None
        )
        if error:
            raise NativeRungError(f"S{scale} {kind} schema: {error.message}")
        documents[kind] = value
    result, plan, rung = (documents[kind] for kind in ("result", "plan", "rung"))
    identities = result["identities"]
    if (
        result["status"] != "passed"
        or result["rung"] != f"S{scale}"
        or plan["rung"] != result["rung"]
        or plan["identities"] != identities
    ):
        raise NativeRungError(f"S{scale} plan/result identity mismatch")
    expected_artifacts = {
        f"{kind}_sha256": _digest(source / f"s{scale}-{kind}.json")
        for kind in ("plan", "benchexec", "graphforge", "rung")
    }
    if result["artifacts"] != expected_artifacts:
        raise NativeRungError(f"S{scale} artifact digest mismatch")
    profile = f"graph500-s{scale}-{'local' if scale < 20 else 'provider'}"
    graphforge, benchexec = documents["graphforge"], documents["benchexec"]
    if (
        identities["profile_id"] != profile
        or rung["profile_id"] != profile
        or rung["scale"] != scale
        or rung["status"] != "passed"
        or not rung["correctness"]
        or rung["phases"] != list(PHASES)
        or benchexec["outcome"] != "passed"
        or graphforge["status"] != "passed"
        or benchexec["graphforge"] != graphforge
        or [phase["phase"] for phase in graphforge["phases"]] != list(PHASES)
        or any(phase["status"] != "passed" for phase in graphforge["phases"])
        or graphforge["profile_id"] != profile
    ):
        raise NativeRungError(f"S{scale} lifecycle evidence contradicts success")
    try:
        derived = assemble_rung_evidence(
            root=root,
            scale=scale,
            graphforge=graphforge,
            benchexec=benchexec,
            profile_id=profile,
            source="progressive_profile" if scale < 20 else "canonical_ladder",
        )
    except (ValueError, KeyError, TypeError) as error:
        raise NativeRungError(f"S{scale} ordinary lifecycle receipts invalid: {error}") from error
    if derived != rung:
        raise NativeRungError(f"S{scale} stored summary contradicts ordinary lifecycle receipts")
    if scale == 26 and derived["live_edges"] < 1_000_000_000:
        raise NativeRungError("S26 has fewer than one billion live persisted edges")
    if scale >= 20:
        path = source / f"s{scale}-projection.json"
        projection = _read(path)
        error = next(
            Draft202012Validator(
                _read(root / "schemas/progressive-qualification-evidence.json")
            ).iter_errors(projection),
            None,
        )
        sources = {20: [18, 19], 22: [19, 20], 24: [20, 22], 25: [22, 24], 26: [24, 25]}
        if (
            error
            or _digest(path) != identities["admitted_projection_sha256"]
            or projection["target"] != f"S{scale}"
            or projection["decision"] != "admitted"
            or projection["source_scales"] != sources[scale]
        ):
            raise NativeRungError(f"S{scale} admission projection mismatch")
        documents["projection"] = projection
    return documents
