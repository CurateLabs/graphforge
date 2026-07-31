"""Contract tests for the thin Python console entry point."""

from __future__ import annotations

import io
from pathlib import Path
import sys
from unittest.mock import patch

from graphforge import cli


class _TextStream:
    def __init__(self) -> None:
        self.buffer = io.BytesIO()


def test_main_forwards_arguments_bytes_and_exit_code() -> None:
    seen: list[list[str]] = []

    def execute(arguments: list[str]) -> tuple[int, bytes, bytes]:
        seen.append(arguments)
        return 3, b'{"ok":true}\n', b'{"error":{"code":"GF_STORAGE"}}\n'

    stdout = _TextStream()
    stderr = _TextStream()
    with (
        patch.object(cli, "_cli_execute", execute),
        patch.object(sys, "stdout", stdout),
        patch.object(sys, "stderr", stderr),
    ):
        assert cli.main(["graphforge", "--json", "sync"]) == 3
    assert len(seen) == 1
    assert seen[0][0] == "graphforge"
    assert seen[0][1] == "--skills-bundle-dir"
    assert Path(seen[0][2]).name == "_project_skills"
    assert seen[0][3:] == ["--json", "sync"]
    assert stdout.buffer.getvalue() == b'{"ok":true}\n'
    assert stderr.buffer.getvalue() == b'{"error":{"code":"GF_STORAGE"}}\n'


def test_main_uses_process_arguments() -> None:
    seen: list[list[str]] = []

    def execute(arguments: list[str]) -> tuple[int, bytes, bytes]:
        seen.append(arguments)
        return 0, b"", b""

    with (
        patch.object(cli, "_cli_execute", execute),
        patch.object(sys, "argv", ["graphforge", "--version"]),
    ):
        assert cli.main() == 0
    assert len(seen) == 1
    assert seen[0][0] == "graphforge"
    assert seen[0][1] == "--skills-bundle-dir"
    assert Path(seen[0][2]).name == "_project_skills"
    assert seen[0][3:] == ["--version"]


if __name__ == "__main__":
    test_main_forwards_arguments_bytes_and_exit_code()
    test_main_uses_process_arguments()
