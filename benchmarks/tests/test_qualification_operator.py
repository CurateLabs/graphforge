from __future__ import annotations

import subprocess
import sys
import unittest

from graphforge_bench.fly_tiny_qualification import parser as fly_parser
from graphforge_bench.qualification_operator import (
    OperatorRefusalError,
    attest_current_main,
    esc_command,
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
            command[5:8],
            (sys.executable, "-m", "graphforge_bench.fly_tiny_qualification"),
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

    def test_outer_runner_receives_argv_without_secret_values(self) -> None:
        observed: tuple[str, ...] | None = None

        def runner(argv: tuple[str, ...], *, check: bool) -> subprocess.CompletedProcess[str]:
            nonlocal observed
            observed = argv
            self.assertFalse(check)
            return subprocess.CompletedProcess(argv, 0)

        self.assertEqual(
            run_under_esc(
                "curatelabs/graphforge/qualification",
                "fly-tiny",
                ARGS,
                runner=runner,
                attestor=lambda _commit: None,
            ),
            0,
        )
        assert observed is not None
        self.assertNotIn("FLY_API_TOKEN", " ".join(observed))

    def test_stale_main_refuses_before_pulumi(self) -> None:
        responses = iter(("a" * 40 + "\n", "", "b" * 40 + "\n"))

        def git_runner(
            argv: tuple[str, ...], **_kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            if argv[1] == "fetch":
                return subprocess.CompletedProcess(argv, 0, "", "")
            return subprocess.CompletedProcess(argv, 0, next(responses), "")

        called = False

        def pulumi_runner(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
            nonlocal called
            called = True
            return subprocess.CompletedProcess((), 0)

        with self.assertRaisesRegex(OperatorRefusalError, "current origin/main"):
            run_under_esc(
                "curatelabs/graphforge/qualification",
                "fly-tiny",
                ARGS,
                runner=pulumi_runner,
                attestor=lambda commit: attest_current_main(commit, runner=git_runner),
            )
        self.assertFalse(called)

    def test_exact_current_main_allows_pulumi(self) -> None:
        responses = iter(("a" * 40 + "\n", "", "a" * 40 + "\n"))

        def git_runner(
            argv: tuple[str, ...], **_kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            if argv[1] == "fetch":
                return subprocess.CompletedProcess(argv, 0, "", "")
            return subprocess.CompletedProcess(argv, 0, next(responses), "")

        self.assertEqual(
            run_under_esc(
                "curatelabs/graphforge/qualification",
                "fly-tiny",
                ARGS,
                runner=lambda argv, **_kwargs: subprocess.CompletedProcess(argv, 0),
                attestor=lambda commit: attest_current_main(commit, runner=git_runner),
            ),
            0,
        )

    def test_inner_controller_rejects_abbreviated_expected_sha(self) -> None:
        with self.assertRaises(SystemExit):
            fly_parser().parse_args(
                [
                    "--expected-s",
                    "b" * 40,
                    "--org",
                    "owner",
                    "--app",
                    "app",
                    "--region",
                    "iad",
                    "--volume-name",
                    "data",
                    "--machine-name",
                    "machine",
                    "--prerequisite-955",
                    "merged",
                    "--prerequisite-956",
                    "merged",
                    "--prerequisite-957",
                    "merged",
                    "--ledger",
                    "ledger.json",
                    "--evidence-out",
                    "evidence.json",
                    "--result-out",
                    "result.json",
                ]
            )


if __name__ == "__main__":
    unittest.main()
