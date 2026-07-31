"""Fresh-wheel smoke test for the ``graphforge`` console script."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sysconfig


def console_script() -> Path:
    scripts = Path(sysconfig.get_path("scripts"))
    name = "graphforge.exe" if os.name == "nt" else "graphforge"
    script = scripts / name
    assert script.is_file(), f"missing installed console script: {script}"
    return script


def main() -> None:
    script = console_script()
    root = Path(__file__).resolve().parents[3]
    fixtures = json.loads(
        (root / "tests/contracts/repository-cli-parity.json").read_text()
    )

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
    assert list(error["error"]) == ["code", "message"]
    assert error["error"]["code"] == "GF_IO"
    assert invalid.stdout == ""


if __name__ == "__main__":
    main()
