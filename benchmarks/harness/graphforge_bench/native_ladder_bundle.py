"""Read native ladder receipts and bind cleanup inventory to those exact results."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from graphforge_bench.native_rung import NativeRungError, read_native_rung
from graphforge_bench.progressive_provider_attempt import CANONICAL_RUNGS

INVENTORY_SCHEMA = "graphforge-host-work-root-inventory/2"
HOST_PROFILE_ID = "local-linux-cgroups-v2"


class NativeBundleError(ValueError):
    """Native evidence does not establish a consistent completed prefix."""


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_object(path: Path) -> dict[str, Any]:
    try:
        if path.is_symlink():
            raise NativeBundleError(f"linked evidence: {path.name}")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as error:
        raise NativeBundleError(f"invalid evidence: {path.name}") from error
    if not isinstance(value, dict):
        raise NativeBundleError(f"evidence is not an object: {path.name}")
    return value


def validate_schema(document: dict[str, Any], name: str) -> None:
    root = Path(__file__).resolve().parents[2] / "schemas"
    error = next(Draft202012Validator(read_object(root / name)).iter_errors(document), None)
    if error is not None:
        raise NativeBundleError(f"{name}: {error.message}")


def native_receipts(source: Path) -> dict[str, Any]:
    """Validate existing producer outputs; no second manifest or approval is needed."""
    paths = sorted(source.glob("*-rung.json"))
    scales = list(CANONICAL_RUNGS[: len(paths)])
    if (
        not paths
        or len(paths) > len(CANONICAL_RUNGS)
        or [p.name for p in paths] != [f"s{scale}-rung.json" for scale in scales]
    ):
        raise NativeBundleError("native rung files are not a canonical prefix")
    expected_results = [f"s{scale}-result.json" for scale in scales]
    if sorted(p.name for p in source.glob("*-result.json")) != expected_results:
        raise NativeBundleError("native result files contradict the completed prefix")
    files: list[str] = []
    results: dict[str, str] = {}
    common: dict[str, Any] | None = None
    for scale in scales:
        prefix = f"s{scale}"
        result_path = source / f"{prefix}-result.json"
        try:
            documents = read_native_rung(Path(__file__).resolve().parents[2], source, scale)
        except NativeRungError as error:
            raise NativeBundleError(str(error)) from error
        result = documents["result"]
        identities = result["identities"]
        shared = {
            key: value
            for key, value in identities.items()
            if key
            not in {
                "profile_id",
                "profile_sha256",
                "admitted_projection_sha256",
            }
        }
        if common is not None and common != shared:
            raise NativeBundleError("native rung immutable identities differ")
        common = shared
        files.extend(
            f"{prefix}-{kind}.json" for kind in ("plan", "benchexec", "graphforge", "rung")
        )
        if scale >= 20:
            files.append(f"{prefix}-projection.json")
        results[result_path.name] = digest(result_path)
        files.append(result_path.name)
    assert common is not None
    return {"commit": common["commit"], "scales": scales, "results": results, "files": files}


def validate_native_bundle(source: Path) -> dict[str, Any]:
    receipt = native_receipts(source)
    inventory = read_object(source / "work-root-inventory.json")
    validate_schema(inventory, "host-work-root-inventory.json")
    if inventory["result_sha256"] != receipt["results"]:
        raise NativeBundleError("cleanup inventory belongs to different native results")
    if inventory["empty"] != (inventory["entries"] == []):
        raise NativeBundleError("cleanup inventory contradicts its entries")
    receipt["files"].append("work-root-inventory.json")
    receipt["empty"] = inventory["empty"]
    receipt["complete"] = inventory["empty"] and receipt["scales"] == list(CANONICAL_RUNGS)
    return receipt


def collect_inventory(work_root: Path, output_dir: Path | None = None) -> dict[str, Any]:
    """Inspect the whole work root, retaining only the named evidence directory.

    Empty workspace/tmp scaffold directories are harmless. Every other entry
    denotes debris, including an entire remaining subtree. We do not enumerate
    dataset contents. No directory links are followed; unreadable scaffolding
    fails rather than producing an empty inventory.
    """
    work_root = work_root.resolve(strict=True)
    evidence = output_dir.resolve(strict=True) if output_dir is not None else None
    if evidence is not None and (evidence == work_root or work_root.is_relative_to(evidence)):
        raise NativeBundleError("evidence directory must not contain the work root")
    if evidence is not None and evidence.is_relative_to(work_root) and evidence.parent != work_root:
        raise NativeBundleError("retained evidence must be a direct work-root child")
    entries = []

    def visit(directory: Path) -> None:
        for path in sorted(directory.iterdir()):
            if path == evidence and not path.is_symlink():
                continue
            if path.is_dir() and not path.is_symlink():
                if path.parent == work_root and path.name in {"workspace", "tmp"}:
                    visit(path)
                else:
                    entries.append(path.relative_to(work_root).as_posix())
            else:
                entries.append(path.relative_to(work_root).as_posix())

    visit(work_root)
    receipt = native_receipts(evidence) if evidence is not None else None
    document = {
        "schema": INVENTORY_SCHEMA,
        "host_profile_id": HOST_PROFILE_ID,
        "scope": "work_root_except_evidence_and_empty_scaffolding",
        "retained_evidence_directory": evidence.name
        if evidence is not None and evidence.parent == work_root
        else None,
        "result_sha256": receipt["results"] if receipt is not None else {},
        "entries": entries,
        "empty": entries == [],
    }
    validate_schema(document, "host-work-root-inventory.json")
    return document
