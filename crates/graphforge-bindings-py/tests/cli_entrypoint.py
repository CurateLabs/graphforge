"""Contract tests for the thin Python console entry point."""

from __future__ import annotations

from pathlib import Path
import sys
from unittest.mock import patch

from graphforge import cli


def test_main_forwards_arguments_and_exit_code() -> None:
    seen: list[list[str]] = []

    def execute(arguments: list[str]) -> int:
        seen.append(arguments)
        return 3

    with patch.object(cli, "_cli_execute_process", execute):
        assert cli.main(["gf", "--json", "sync"]) == 3
    assert len(seen) == 1
    assert seen[0][0] == "gf"
    assert seen[0][1] == "--skills-bundle-dir"
    assert Path(seen[0][2]).name == "_project_skills"
    assert seen[0][3:] == ["--json", "sync"]


def test_main_uses_process_arguments() -> None:
    seen: list[list[str]] = []

    def execute(arguments: list[str]) -> int:
        seen.append(arguments)
        return 0

    with (
        patch.object(cli, "_cli_execute_process", execute),
        patch.object(sys, "argv", ["graphforge", "--version"]),
    ):
        assert cli.main() == 0
    assert len(seen) == 1
    assert seen[0][0] == "graphforge"
    assert seen[0][1] == "--skills-bundle-dir"
    assert Path(seen[0][2]).name == "_project_skills"
    assert seen[0][3:] == ["--version"]


if __name__ == "__main__":
    test_main_forwards_arguments_and_exit_code()
    test_main_uses_process_arguments()
