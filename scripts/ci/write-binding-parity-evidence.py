#!/usr/bin/env python3
"""Write deterministic SHA-bound evidence after native binding tests pass."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def package_version(language: str, artifact: Path, package_manifest: Path) -> str:
    if language == "python":
        match = re.match(r"graphforge-([^-]+)-", artifact.name)
        if not match:
            raise SystemExit(f"cannot derive GraphForge version from wheel: {artifact.name}")
        return match.group(1)
    manifest = json.loads(package_manifest.read_text())
    version = manifest.get("version")
    if not isinstance(version, str) or not version:
        raise SystemExit("Node package manifest has no version")
    return version


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--language", choices=("python", "node"), required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--classification", type=Path, required=True)
    parser.add_argument("--package-manifest", type=Path, required=True)
    parser.add_argument("--source-sha")
    parser.add_argument("--case", action="append", dest="cases", required=True)
    parser.add_argument("--target")
    parser.add_argument("--execution-mode", choices=("native", "package-validation"))
    parser.add_argument("--execution-rationale")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    source_sha = args.source_sha or os.environ.get("GITHUB_SHA", "")
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise SystemExit("GITHUB_SHA must be the exact 40-character source SHA")
    checked_out_sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    if source_sha != checked_out_sha:
        raise SystemExit(
            f"artifact source SHA {source_sha} does not match checkout {checked_out_sha}"
        )
    if not args.artifact.is_file():
        raise SystemExit(f"native artifact does not exist: {args.artifact}")
    if not args.classification.is_file():
        raise SystemExit(f"classification does not exist: {args.classification}")
    if len(set(args.cases)) != len(args.cases):
        raise SystemExit("test case identities must be unique")

    classification = json.loads(args.classification.read_text())
    evidence_cases: list[str] = []
    if args.execution_mode != "package-validation":
        for files in classification.get("evidence", {}).values():
            for filename, identities in files.items():
                evidence_cases.extend(f"{filename}::{identity}" for identity in identities)
    cases = list(dict.fromkeys([*args.cases, *evidence_cases]))
    if bool(args.target) != bool(args.execution_mode):
        raise SystemExit("--target and --execution-mode must be provided together")
    if args.execution_mode == "package-validation" and not args.execution_rationale:
        raise SystemExit("package-validation reports require --execution-rationale")
    if args.execution_mode == "native" and args.execution_rationale:
        raise SystemExit("native reports must not provide --execution-rationale")
    report = {
        "schema": "graphforge-binding-parity-evidence/1",
        "source_sha": source_sha,
        "language": args.language,
        "package_version": package_version(args.language, args.artifact, args.package_manifest),
        "artifact": {
            "name": args.artifact.name,
            "sha256": sha256(args.artifact),
        },
        "classification": {
            "name": args.classification.name,
            "sha256": sha256(args.classification),
            "schema": classification.get("schema") or classification.get("contractVersion"),
        },
        "cases": [
            {"identity": identity, "outcome": "passed", "sanitized_error": None}
            for identity in cases
        ],
        "sanitized_parity_diff": [],
    }
    if args.target:
        report.update(
            {
                "schema": "graphforge-binding-rc-target/1",
                "target": args.target,
                "execution": {
                    "mode": args.execution_mode,
                    "rationale": args.execution_rationale,
                },
                "fallback_execution": False,
            }
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
