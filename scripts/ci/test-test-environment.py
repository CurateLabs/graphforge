#!/usr/bin/env python3
"""Tests for fail-fast temporary storage and child-process propagation."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).parents[1] / "test_environment.py"
SPEC = importlib.util.spec_from_file_location("test_environment", SCRIPT)
assert SPEC and SPEC.loader
ENV = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ENV)


class TestEnvironmentTests(unittest.TestCase):
    def test_prepare_checks_writability_and_leaves_no_probe(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            actual_stat = Path.stat

            def stat(path: Path, *args: object, **kwargs: object) -> os.stat_result:
                result = list(actual_stat(path, *args, **kwargs))
                result[2] = 1  # one admitted volume, preserving real file kinds
                return os.stat_result(result)

            with (
                patch.object(ENV.sys, "platform", "linux"),
                patch.object(ENV.subprocess, "check_output", return_value="ext4\n"),
                patch.object(ENV.Path, "stat", stat),
            ):
                self.assertEqual(ENV.prepare(root), root)
            self.assertEqual(list(root.iterdir()), [])

    def test_unsupported_filesystem_fails_before_command(self) -> None:
        with (
            tempfile.TemporaryDirectory() as raw,
            patch.object(ENV.sys, "platform", "linux"),
            patch.object(ENV.subprocess, "check_output", return_value="tmpfs\n"),
            patch.object(ENV.subprocess, "run") as run,
            patch.object(sys, "argv", [str(SCRIPT), "--root", raw, "--", "sentinel"]),
        ):
            self.assertEqual(ENV.main(), 1)
            run.assert_not_called()

    def test_native_mount_below_another_volume_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            actual_stat = Path.stat

            def stat(path: Path, *args: object, **kwargs: object) -> object:
                if path == Path("/"):
                    result = list(actual_stat(path))
                    result[2] = -1  # st_dev
                    return os.stat_result(result)
                return actual_stat(path, *args, **kwargs)

            with (
                patch.object(ENV.sys, "platform", "linux"),
                patch.object(ENV.subprocess, "check_output", return_value="ext4\n"),
                patch.object(ENV.Path, "stat", stat),
                self.assertRaisesRegex(ValueError, "volume boundary"),
            ):
                ENV.prepare(root)

    def test_child_and_github_environment_use_checked_root_and_preserve_exit(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            output = root / "child.json"
            github_env = root / "github-env"
            command = [
                sys.executable,
                "-c",
                (
                    "import json, os, pathlib, sys; "
                    "pathlib.Path(sys.argv[1]).write_text(json.dumps("
                    "{k: os.environ[k] for k in ('TMPDIR', 'TMP', 'TEMP')})); sys.exit(7)"
                ),
                str(output),
            ]
            with (
                patch.object(ENV, "prepare", return_value=root),
                patch.object(
                    sys,
                    "argv",
                    [str(SCRIPT), "--root", raw, "--github-env", str(github_env), "--", *command],
                ),
            ):
                self.assertEqual(ENV.main(), 7)
            self.assertEqual(json.loads(output.read_text()), dict.fromkeys(ENV.TEMP_VARIABLES, raw))
            self.assertEqual(
                github_env.read_text().splitlines(),
                [f"{name}={raw}" for name in ENV.TEMP_VARIABLES],
            )

    def test_probe_permission_error_is_actionable_and_does_not_launch_child(self) -> None:
        with (
            patch.object(ENV, "prepare", side_effect=PermissionError("read-only root")),
            patch.object(ENV.subprocess, "run") as run,
            patch.object(sys, "argv", [str(SCRIPT), "--", "sentinel"]),
        ):
            self.assertEqual(ENV.main(), 1)
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
