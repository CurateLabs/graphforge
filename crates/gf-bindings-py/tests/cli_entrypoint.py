"""Contract tests for the thin Python console entry point."""

from __future__ import annotations

import io
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
    assert seen == [["graphforge", "--json", "sync"]]
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
    assert seen == [["graphforge", "--version"]]


if __name__ == "__main__":
    test_main_forwards_arguments_bytes_and_exit_code()
    test_main_uses_process_arguments()
