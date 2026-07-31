"""Fresh-wheel smoke test for the ``graphforge`` console script."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sysconfig
import tempfile


def console_script() -> Path:
    scripts = Path(sysconfig.get_path("scripts"))
    name = "graphforge.exe" if os.name == "nt" else "graphforge"
    script = scripts / name
    assert script.is_file(), f"missing installed console script: {script}"
    return script


def main() -> None:
    script = console_script()
    root = Path(__file__).resolve().parents[3]
    fixtures = json.loads((root / "tests/contracts/repository-cli-parity.json").read_text())

    for case in fixtures["cases"]:
        completed = subprocess.run(
            [script, *case["args"]], check=False, capture_output=True, text=True
        )
        assert completed.returncode == case["exitCode"], case["name"]
        assert completed.stdout == case["stdout"], case["name"]
        assert completed.stderr == case["stderr"], case["name"]

    invalid = subprocess.run(
        [script, "--json", "--project-dir", "/definitely/missing", "config", "validate"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert invalid.returncode == 3, invalid
    error = json.loads(invalid.stderr)
    assert list(error) == ["error"]
    assert list(error["error"]) == ["code", "message", "details"]
    assert error["error"]["code"] == "GF_IO"
    assert error["error"]["details"] == {"source": "runtime", "kind": "storage"}
    assert invalid.stdout == ""

    with tempfile.TemporaryDirectory(prefix="graphforge-infra-cli-") as directory:
        project = Path(directory)
        integration = project / ".graphforge"
        integration.mkdir()
        shutil.copyfile(
            root / "docs/contracts/examples/graphforge-v1.yaml",
            integration / "graphforge.yaml",
        )
        resolved = subprocess.run(
            [script, "--project-dir", project, "--json", "config", "resolve"],
            check=False,
            capture_output=True,
        )
        assert resolved.returncode == 0, resolved.stderr
        assert (
            resolved.stdout
            == (root / "docs/contracts/examples/graphforge-resolved-v1.json").read_bytes()
        )
        infra = subprocess.run(
            [
                script,
                "--project-dir",
                project,
                "--json",
                "infra",
                "validate",
                "--target",
                "production",
            ],
            check=False,
            capture_output=True,
        )
        assert infra.returncode == 0, infra.stderr
        assert (
            infra.stdout
            == (
                root / "docs/contracts/examples/graphforge-infra-validation-production-v1.json"
            ).read_bytes()
        )


if __name__ == "__main__":
    main()
