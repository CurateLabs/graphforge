#!/usr/bin/env python3
"""Keep Python extension linkage separate from ordinary Cargo Rust tests."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
from typing import Any

import tomllib

PYO3_CRATES = {"pyo3", "pyo3-ffi", "pyo3-build-config"}
EXTENSION_ENV = "PYO3_BUILD_EXTENSION_MODULE"


def validate_metadata(metadata: dict[str, Any], *, extension: bool) -> None:
    """Inspect resolved features, including indirect/default feature activation."""
    binding = [
        package for package in metadata["packages"] if package["name"] == "graphforge-bindings-py"
    ]
    if len(binding) != 1 or not any(
        "cdylib" in target["kind"] and target.get("test", False) for target in binding[0]["targets"]
    ):
        raise ValueError("Python binding Rust lib tests must remain enabled")
    names = {package["id"]: package["name"] for package in metadata["packages"]}
    found = set()
    for node in metadata["resolve"]["nodes"]:
        name = names[node["id"]]
        if name not in PYO3_CRATES:
            continue
        found.add(name)
        features = set(node["features"])
        if ("extension-module" in features) != extension:
            mode = "extension packaging" if extension else "ordinary Rust tests"
            raise ValueError(f"{name}: incorrect extension-module feature for {mode}")
        if "abi3-py310" not in features:
            raise ValueError(f"{name}: Python 3.10+ ABI3 contract is missing")
    if found != PYO3_CRATES:
        raise ValueError(f"missing resolved PyO3 crates: {sorted(PYO3_CRATES - found)}")


def validate_bazel(lock: dict[str, Any]) -> None:
    """Require explicit extension mode in every generated PyO3 build script."""
    found = set()
    for crate in lock["crates"].values():
        name = crate["name"]
        if name not in PYO3_CRATES:
            continue
        found.add(name)
        environment = crate.get("build_script_attrs", {}).get("build_script_env", {})
        # PyO3 treats presence as enabled, even when a caller writes "0".
        if EXTENSION_ENV not in environment.get("common", {}):
            raise ValueError(f"{name}: Bazel distribution build lacks explicit extension mode")
    if found != PYO3_CRATES:
        raise ValueError("Bazel lock is missing PyO3 build scripts")


def cargo_metadata(root: Path, features: list[str] | None = None) -> dict[str, Any]:
    command = ["cargo", "metadata", "--format-version=1", "--locked"]
    if features is not None:
        command += ["--manifest-path", "crates/graphforge-bindings-py/Cargo.toml"]
        if features:
            command += ["--features", ",".join(features)]
    return json.loads(subprocess.check_output(command, cwd=root, text=True))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[2])
    root = parser.parse_args().root.resolve()
    if EXTENSION_ENV in os.environ:
        raise SystemExit(f"unset {EXTENSION_ENV} for ordinary Rust-test/coverage validation")
    try:
        validate_metadata(cargo_metadata(root), extension=False)
        packaging = tomllib.loads(
            (root / "crates/graphforge-bindings-py/pyproject.toml").read_text(encoding="utf-8")
        )
        features = packaging["tool"]["maturin"].get("features", [])
        validate_metadata(cargo_metadata(root, features), extension=True)
        validate_bazel(json.loads((root / "cargo-bazel-lock.json").read_text(encoding="utf-8")))
    except (ValueError, KeyError, subprocess.CalledProcessError) as error:
        raise SystemExit(f"Python build-mode check failed: {error}") from error
    print(
        "Python build-mode configuration valid: Cargo tests, maturin extensions, Bazel extensions"
    )


if __name__ == "__main__":
    main()
