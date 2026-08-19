"""CI-visible tests for the normative portable-v2 golden corpus."""

from pathlib import Path
import subprocess
import sys


def test_portable_v2_contract_corpus() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, str(root / "scripts/ci/portable-v2-contract.py")],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "portable-v2 contract fixtures: PASS"
