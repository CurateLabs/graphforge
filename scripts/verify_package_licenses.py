#!/usr/bin/env python3
"""Verify first-party publishable packages ship Apache LICENSE + NOTICE.

Checks package *contents* (not only source-tree copies):

- Cargo: ``cargo package --list`` includes LICENSE and NOTICE
- npm: ``npm pack --dry-run`` lists LICENSE and NOTICE
- Python: maturin/pyproject ``license-files`` exist and declare Apache-2.0

Usage:
    python3 scripts/verify_package_licenses.py
    python3 scripts/verify_package_licenses.py --report package-license-report.json
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

# Crates intended for crates.io (bindings ship via PyPI/npm).
CARGO_PUBLISH_CRATES = (
    "gf-core",
    "gf-ast",
    "gf-knowledge",
    "gf-ontology",
    "gf-provenance",
    "gf-ir",
    "gf-plan",
    "gf-storage",
    "gf-io",
    "gf-rel",
    "gf-search",
    "gf-cypher",
    "gf-exec",
    "gf-api",
    "gf-cli",
)

NPM_PACKAGES = (
    ROOT / "crates" / "gf-bindings-node",
    ROOT / "packages" / "agent-skills",
)

PYTHON_PYPROJECT = ROOT / "crates" / "gf-bindings-py" / "pyproject.toml"


def _run(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    # Keep package listing off the shared target when possible.
    env.setdefault("CARGO_TARGET_DIR", str(ROOT / "target" / "package-license-verify"))
    return subprocess.run(
        cmd,
        cwd=str(cwd or ROOT),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
    )


def verify_cargo_crate(name: str) -> list[str]:
    """Return errors for one crate's packaged file list."""
    errors: list[str] = []
    result = _run(
        [
            "cargo",
            "package",
            "-p",
            name,
            "--list",
            "--allow-dirty",
            "--no-verify",
        ]
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        errors.append(f"cargo package -p {name} --list failed: {detail}")
        return errors
    files = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    for required in ("LICENSE", "NOTICE"):
        if required not in files and f"./{required}" not in files:
            # cargo may prefix with crate-version/
            if not any(line.endswith(f"/{required}") or line == required for line in files):
                errors.append(f"{name}: packaged crate lacks {required}")
    # Metadata SPDX / license-file is also enforced by license_check.py.
    return errors


def verify_npm_package(package_dir: Path) -> list[str]:
    """Return errors for one npm package dry-run tarball listing."""
    errors: list[str] = []
    package_json = package_dir / "package.json"
    if not package_json.exists():
        return [f"{package_dir.relative_to(ROOT)}: missing package.json"]
    meta = json.loads(package_json.read_text(encoding="utf-8"))
    if meta.get("license") != "Apache-2.0":
        errors.append(f"{package_dir.relative_to(ROOT)}: license is not Apache-2.0")
    files_field = meta.get("files")
    if isinstance(files_field, list):
        for required in ("LICENSE", "NOTICE"):
            if required not in files_field:
                errors.append(
                    f"{package_dir.relative_to(ROOT)}: package.json files[] lacks {required}"
                )
    with tempfile.TemporaryDirectory(prefix="gf-npm-pack-") as tmp:
        result = _run(
            ["npm", "pack", "--dry-run", "--ignore-scripts", "--json"],
            cwd=package_dir,
        )
        # npm pack --json still prints notices to stderr; stdout should be JSON.
        if result.returncode != 0:
            # Fallback: parse human dry-run listing.
            result = _run(
                ["npm", "pack", "--dry-run", "--ignore-scripts"],
                cwd=package_dir,
            )
            if result.returncode != 0:
                errors.append(
                    f"{package_dir.relative_to(ROOT)}: npm pack --dry-run failed: "
                    f"{(result.stderr or result.stdout).strip()}"
                )
                return errors
            listing = result.stderr + "\n" + result.stdout
            for required in ("LICENSE", "NOTICE"):
                if required not in listing:
                    errors.append(
                        f"{package_dir.relative_to(ROOT)}: npm pack listing lacks {required}"
                    )
            return errors
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError:
            listing = result.stderr + "\n" + result.stdout
            for required in ("LICENSE", "NOTICE"):
                if required not in listing:
                    errors.append(
                        f"{package_dir.relative_to(ROOT)}: npm pack listing lacks {required}"
                    )
            return errors
        # npm --json pack returns a list of {files:[{path:...}], ...}
        entries = payload if isinstance(payload, list) else [payload]
        paths: set[str] = set()
        for entry in entries:
            for item in entry.get("files", []) if isinstance(entry, dict) else []:
                if isinstance(item, dict) and isinstance(item.get("path"), str):
                    paths.add(Path(item["path"]).name)
                elif isinstance(item, str):
                    paths.add(Path(item).name)
        # Some npm versions omit files in --json; fall back to stderr notice lines.
        if not paths:
            listing = result.stderr + "\n" + result.stdout
            for required in ("LICENSE", "NOTICE"):
                if required not in listing:
                    errors.append(
                        f"{package_dir.relative_to(ROOT)}: npm pack listing lacks {required}"
                    )
            return errors
        for required in ("LICENSE", "NOTICE"):
            if required not in paths:
                errors.append(f"{package_dir.relative_to(ROOT)}: npm pack tarball lacks {required}")
        _ = tmp  # keep temp dir alive for npm side effects if any
    return errors


def verify_python_package() -> list[str]:
    """Return errors for Python package license metadata and files."""
    errors: list[str] = []
    text = PYTHON_PYPROJECT.read_text(encoding="utf-8") if PYTHON_PYPROJECT.exists() else ""
    if 'license = "Apache-2.0"' not in text:
        errors.append("crates/gf-bindings-py/pyproject.toml lacks license = Apache-2.0")
    match = re.search(r"license-files\s*=\s*\[([^\]]+)\]", text)
    if not match:
        errors.append("crates/gf-bindings-py/pyproject.toml lacks license-files")
        return errors
    declared = [part.strip().strip("\"'") for part in match.group(1).split(",") if part.strip()]
    for required in ("LICENSE", "NOTICE"):
        if required not in declared:
            errors.append(f"crates/gf-bindings-py/pyproject.toml license-files lacks {required}")
        path = PYTHON_PYPROJECT.parent / required
        if not path.exists():
            errors.append(f"crates/gf-bindings-py/{required} is missing")
    if "THIRD_PARTY_NOTICES.md" in declared:
        path = PYTHON_PYPROJECT.parent / "THIRD_PARTY_NOTICES.md"
        if not path.exists():
            errors.append("crates/gf-bindings-py/THIRD_PARTY_NOTICES.md is missing")
    return errors


def run_checks() -> dict[str, Any]:
    """Run all package license content checks."""
    errors: list[str] = []
    cargo_results: dict[str, list[str]] = {}
    for name in CARGO_PUBLISH_CRATES:
        crate_errors = verify_cargo_crate(name)
        cargo_results[name] = crate_errors
        errors.extend(crate_errors)
    npm_results: dict[str, list[str]] = {}
    for package_dir in NPM_PACKAGES:
        key = str(package_dir.relative_to(ROOT))
        pkg_errors = verify_npm_package(package_dir)
        npm_results[key] = pkg_errors
        errors.extend(pkg_errors)
    python_errors = verify_python_package()
    errors.extend(python_errors)
    return {
        "schema_version": 1,
        "status": "pass" if not errors else "fail",
        "cargo": cargo_results,
        "npm": npm_results,
        "python": python_errors,
        "errors": errors,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, help="Write JSON report to this path")
    args = parser.parse_args(argv)
    report = run_checks()
    if args.report:
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if report["errors"]:
        for error in report["errors"]:
            print(f"package-licenses: {error}", file=sys.stderr)
        return 1
    print("package-licenses: compliant")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
