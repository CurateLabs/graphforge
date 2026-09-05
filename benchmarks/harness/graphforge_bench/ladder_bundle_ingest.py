"""Validate and ingest completed #900 ladder bundles for #959 parity."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shutil
from typing import Any

from jsonschema import Draft202012Validator

from graphforge_bench.native_ladder_bundle import NativeBundleError, validate_native_bundle
from graphforge_bench.scale_parity import compare_ladder_bundle

RUNG_NAME = re.compile(r"^s(\d+)-rung\.json$")
MANIFEST_REQUIRED = (
    "commit",
    "image_digest",
    "generator_identity",
    "benchexec_version",
    "maximum_authorized_scale",
)
COMMIT = re.compile(r"^[0-9a-f]{40}$")
INGEST_SCHEMA = "graphforge-scale-orchestration-ladder-bundle-ingest/1"


class LadderBundleIngestError(ValueError):
    """The source bundle is missing, malformed, or fails schema validation."""


def _schema_root() -> Path:
    return Path(__file__).resolve().parents[2] / "schemas"


def _validator(name: str) -> Draft202012Validator:
    path = _schema_root() / name
    return Draft202012Validator(json.loads(path.read_text(encoding="utf-8")))


def _read_json(path: Path, message: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise LadderBundleIngestError(message) from error


def _validate_manifest(document: Any) -> None:
    if not isinstance(document, dict):
        raise LadderBundleIngestError("manifest.json must be a JSON object")
    missing = [name for name in MANIFEST_REQUIRED if name not in document]
    if missing:
        raise LadderBundleIngestError(f"manifest.json missing required keys: {', '.join(missing)}")
    commit = document.get("commit")
    if not isinstance(commit, str) or COMMIT.fullmatch(commit) is None:
        raise LadderBundleIngestError("manifest.json commit must be a lowercase Git object ID")


def _has_native_receipts(source: Path) -> bool:
    for path in (*source.glob("*-result.json"), *source.glob("*-plan.json")):
        document = _read_json(path, f"{path.name} is malformed")
        if isinstance(document, dict) and document.get("schema") in {
            "graphforge-progressive-host-run-plan/1",
            "graphforge-progressive-host-run-result/1",
        }:
            return True
    return False


def validate_ladder_bundle(source: Path) -> dict[str, Any]:
    """Validate a completed #900 bundle directory without copying it."""
    if not source.is_dir():
        raise LadderBundleIngestError("source ladder bundle directory is missing")

    if (
        not (source / "manifest.json").exists()
        or (source / "work-root-inventory.json").exists()
        or _has_native_receipts(source)
    ):
        try:
            native = validate_native_bundle(source)
        except (NativeBundleError, OSError) as error:
            raise LadderBundleIngestError(str(error)) from error
        return {
            "schema": INGEST_SCHEMA,
            "source": str(source),
            "manifest_commit": native["commit"],
            "rung_files": [f"s{scale}-rung.json" for scale in native["scales"]],
            "rung_scales": native["scales"],
            "teardown_status": "empty" if native["empty"] else "failed",
            "evidence_files": native["files"],
        }

    manifest_path = source / "manifest.json"
    teardown_path = source / "teardown-inventory.json"
    if not manifest_path.is_file():
        raise LadderBundleIngestError("manifest.json is required")
    if not teardown_path.is_file():
        raise LadderBundleIngestError("teardown-inventory.json is required")

    manifest = _read_json(manifest_path, "manifest.json is malformed")
    _validate_manifest(manifest)

    rung_validator = _validator("progressive-qualification-rung-evidence.json")
    teardown_validator = _validator("progressive-provider-teardown-inventory.json")
    teardown = _read_json(teardown_path, "teardown-inventory.json is malformed")
    teardown_validator.validate(teardown)

    rung_paths = sorted(
        path for path in source.glob("*-rung.json") if RUNG_NAME.fullmatch(path.name)
    )
    if not rung_paths:
        raise LadderBundleIngestError("at least one sN-rung.json file is required")

    rung_scales: list[int] = []
    for rung_path in rung_paths:
        match = RUNG_NAME.fullmatch(rung_path.name)
        assert match is not None
        rung_scales.append(int(match.group(1)))
        document = _read_json(rung_path, f"{rung_path.name} is malformed")
        rung_validator.validate(document)

    return {
        "schema": INGEST_SCHEMA,
        "source": str(source),
        "manifest_commit": manifest["commit"],
        "rung_files": [path.name for path in rung_paths],
        "rung_scales": rung_scales,
        "teardown_status": teardown.get("status"),
    }


def ingest_ladder_bundle(source: Path, destination: Path | None = None) -> dict[str, Any]:
    """Validate a #900 bundle and copy it into the parity fixture tree."""
    report = validate_ladder_bundle(source)
    target = destination or Path(__file__).resolve().parents[2] / "fixtures/parity/ladder-bundle"
    if target.exists() and any(target.glob("*-rung.json")):
        raise LadderBundleIngestError("destination already contains ingested rung bundles")

    target.mkdir(parents=True, exist_ok=True)
    for name in report.get(
        "evidence_files", ("manifest.json", "teardown-inventory.json", *report["rung_files"])
    ):
        shutil.copy2(source / name, target / name)

    parity = compare_ladder_bundle(target)
    report["destination"] = str(target)
    report["parity_comparisons"] = len(parity)
    report["parity_overall"] = [
        matrix["overall"] for matrix in parity if isinstance(matrix.get("overall"), str)
    ]
    return report


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    result.add_argument("--source", type=Path, required=True)
    result.add_argument("--destination", type=Path)
    result.add_argument(
        "--validate-only",
        action="store_true",
        help="validate the source bundle without copying into fixtures/parity/ladder-bundle/",
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.validate_only:
            report = validate_ladder_bundle(args.source)
        else:
            report = ingest_ladder_bundle(args.source, args.destination)
    except LadderBundleIngestError as error:
        print(f"ladder bundle ingest refused: {error}")
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
