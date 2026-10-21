"""CI-visible tests for the normative portable golden corpus."""

from pathlib import Path
import subprocess
import sys


def test_portable_contract_corpus() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, str(root / "scripts/ci/portable-contract.py")],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "portable contract fixtures: PASS"


def test_portable_contract_corpus_under_optimized_python() -> None:
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, "-O", str(root / "scripts/ci/portable-contract.py")],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "portable contract fixtures: PASS"
