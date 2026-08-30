from __future__ import annotations

import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from graphforge_bench.qualification_operator import (
    ESC_CONTEXT,
    OperatorRefusalError,
    esc_command,
    execute_inner,
    run_under_esc,
)

ARGS = [
    "--expected-sha",
    "a" * 40,
    "--execute",
    "--confirm-disposable",
]


class QualificationOperatorTests(unittest.TestCase):
    def test_esc_command_is_shell_free_and_forwards_exact_arguments(self) -> None:
        command = esc_command("curatelabs/graphforge/qualification", "fly-tiny", ARGS)
        self.assertEqual(
            command[:5], ("pulumi", "env", "run", "curatelabs/graphforge/qualification", "--")
        )
        self.assertEqual(
            command[5:11],
            (
                "env",
                f"GRAPHFORGE_OPERATOR_CONTEXT={ESC_CONTEXT}",
                sys.executable,
                "-m",
                "graphforge_bench.qualification_operator",
                "execute",
            ),
        )
        self.assertEqual(list(command[-len(ARGS) :]), ARGS)

    def test_live_execution_requires_confirmation_and_exact_sha(self) -> None:
        with self.assertRaisesRegex(OperatorRefusalError, "--confirm-disposable"):
            esc_command("curatelabs/graphforge/qualification", "fly-tiny", ARGS[:-1])
        with self.assertRaisesRegex(OperatorRefusalError, "--expected-sha"):
            esc_command(
                "curatelabs/graphforge/qualification",
                "fly-tiny",
                ["--execute", "--confirm-disposable"],
            )
        with self.assertRaisesRegex(OperatorRefusalError, "lowercase full Git object ID"):
            esc_command(
                "curatelabs/graphforge/qualification",
                "fly-tiny",
                [
                    "--expected-sha",
                    "main",
                    "--execute",
                    "--confirm-disposable",
                ],
            )

    def test_progressive_ladder_refuses_before_opening_esc(self) -> None:
        runner_called = False

        def runner(*args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
            nonlocal runner_called
            runner_called = True
            return subprocess.CompletedProcess(args, 0)

        with self.assertRaisesRegex(OperatorRefusalError, "scale executor"):
            run_under_esc(
                "curatelabs/graphforge/qualification",
                "progressive-ladder",
                ARGS,
                runner=runner,
            )
        self.assertFalse(runner_called)

    def test_inner_execution_requires_esc_context(self) -> None:
        with (
            patch.dict(os.environ, {}, clear=True),
            self.assertRaisesRegex(OperatorRefusalError, "pulumi env run"),
        ):
            execute_inner("fly-tiny", ARGS)

    def test_inner_execution_delegates_to_existing_controller(self) -> None:
        with (
            patch.dict(os.environ, {"GRAPHFORGE_OPERATOR_CONTEXT": ESC_CONTEXT}, clear=True),
            patch(
                "graphforge_bench.qualification_operator.fly_tiny_qualification.main",
                return_value=7,
            ) as inner,
        ):
            self.assertEqual(execute_inner("fly-tiny", ARGS), 7)
        inner.assert_called_once_with(ARGS)

    def test_outer_runner_receives_argv_without_secret_values(self) -> None:
        observed: tuple[str, ...] | None = None

        def runner(argv: tuple[str, ...], *, check: bool) -> subprocess.CompletedProcess[str]:
            nonlocal observed
            observed = argv
            self.assertFalse(check)
            return subprocess.CompletedProcess(argv, 0)

        self.assertEqual(
            run_under_esc("curatelabs/graphforge/qualification", "fly-tiny", ARGS, runner=runner),
            0,
        )
        assert observed is not None
        self.assertNotIn("FLY_API_TOKEN", " ".join(observed))


if __name__ == "__main__":
    unittest.main()
