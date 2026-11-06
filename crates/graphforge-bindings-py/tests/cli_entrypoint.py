"""Contract tests for the thin Python console entry point."""

from __future__ import annotations

from pathlib import Path
import signal
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


def test_console_owns_only_python_default_interrupt_and_restores_it() -> None:
    original = signal.getsignal(signal.SIGINT)
    try:

        def custom(_signum, _frame):
            pass

        for handler in (signal.default_int_handler, signal.SIG_IGN, custom):
            signal.signal(signal.SIGINT, handler)
            expected = signal.SIG_DFL if handler is signal.default_int_handler else handler

            def execute(expected_handler=expected) -> int:
                assert signal.getsignal(signal.SIGINT) == expected_handler
                return 7

            with patch.object(cli, "main", execute):
                assert cli.console() == 7
            assert signal.getsignal(signal.SIGINT) == handler
    finally:
        signal.signal(signal.SIGINT, original)


def test_console_restores_default_interrupt_after_exception() -> None:
    original = signal.signal(signal.SIGINT, signal.default_int_handler)
    try:

        def execute() -> int:
            assert signal.getsignal(signal.SIGINT) == signal.SIG_DFL
            raise RuntimeError("native refusal")

        with patch.object(cli, "main", execute):
            try:
                cli.console()
            except RuntimeError as error:
                assert str(error) == "native refusal"
            else:
                raise AssertionError("console swallowed the native exception")
        assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
    finally:
        signal.signal(signal.SIGINT, original)


def test_embedded_main_preserves_host_interrupt_handler() -> None:
    original = signal.getsignal(signal.SIGINT)
    try:
        for handler in (signal.default_int_handler, signal.SIG_IGN):
            signal.signal(signal.SIGINT, handler)

            def execute(_arguments, expected_handler=handler) -> int:
                assert signal.getsignal(signal.SIGINT) == expected_handler
                return 0

            with patch.object(cli, "_cli_execute_process", execute):
                assert cli.main(["gf", "--version"]) == 0
            assert signal.getsignal(signal.SIGINT) == handler
    finally:
        signal.signal(signal.SIGINT, original)


if __name__ == "__main__":
    test_main_forwards_arguments_and_exit_code()
    test_main_uses_process_arguments()
    test_console_owns_only_python_default_interrupt_and_restores_it()
    test_console_restores_default_interrupt_after_exception()
    test_embedded_main_preserves_host_interrupt_handler()
