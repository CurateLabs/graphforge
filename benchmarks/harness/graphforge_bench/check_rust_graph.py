"""Reject local Rust dependencies outside the benchmark workspace."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

from graphforge_bench.smoke import workspace_root


def main() -> None:
    root = workspace_root().resolve()
    environment = os.environ.copy()
    for variable in ("MAKEFLAGS", "MFLAGS", "CARGO_MAKEFLAGS"):
        environment.pop(variable, None)
    output = subprocess.run(
        [
            "cargo",
            "metadata",
            "--locked",
            "--format-version",
            "1",
            "--manifest-path",
            str(root / "Cargo.toml"),
        ],
        check=True,
        env=environment,
        stdout=subprocess.PIPE,
        text=True,
    )
    metadata = json.loads(output.stdout)
    local_packages = [package for package in metadata["packages"] if package["source"] is None]
    if not local_packages:
        raise RuntimeError("benchmark Rust workspace has no local packages")
    for package in local_packages:
        manifest = Path(package["manifest_path"]).resolve()
        if root not in manifest.parents:
            raise RuntimeError(f"local dependency escapes benchmark workspace: {manifest}")
        if package["publish"] != []:
            raise RuntimeError(f"benchmark package is publishable: {package['name']}")
    print(f"benchmark Rust graph isolated: local_packages={len(local_packages)}")


if __name__ == "__main__":
    main()
