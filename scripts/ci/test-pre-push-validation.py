#!/usr/bin/env python3
"""Deterministic contract tests for resumable local pre-push validation."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).parents[1] / "pre_push_validation.py"
SPEC = importlib.util.spec_from_file_location("pre_push_validation", SCRIPT)
assert SPEC and SPEC.loader
GATE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = GATE
SPEC.loader.exec_module(GATE)


class PrePushValidationTests(unittest.TestCase):
    def make_root(self, directory: Path) -> None:
        (directory / ".git").mkdir()
        (directory / "Cargo.lock").write_text("first\n", encoding="utf-8")
        (directory / "pyproject.toml").write_text("[project]\n", encoding="utf-8")

    def coordinator(self, root: Path, calls: list[tuple[str, ...]]) -> object:
        def runner(command: tuple[str, ...], _environment: object) -> None:
            calls.append(command)

        stages = (
            GATE.Stage("preflight", inputs=("pyproject.toml",)),
            GATE.Stage(
                "rust", commands=(("rust",),), dependencies=("preflight",), inputs=("Cargo.lock",)
            ),
            GATE.Stage(
                "binding",
                commands=(("binding",),),
                dependencies=("rust",),
                inputs=("pyproject.toml",),
            ),
        )
        coordinator = GATE.Coordinator(root, stages, runner=runner)
        coordinator.run_preflight = lambda _environment: None
        coordinator.command_versions = lambda _stage: {"tool": "test"}
        return coordinator

    def test_late_failure_resume_reuses_proven_upstream_stages(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            calls: list[tuple[str, ...]] = []
            first = self.coordinator(root, calls)
            first.run()
            self.assertEqual(calls, [("rust",), ("binding",)])
            calls.clear()
            second = self.coordinator(root, calls)
            second.run()
            self.assertEqual(calls, [])
            self.assertEqual(
                [item.status for item in second.results.values()], ["miss", "hit", "hit"]
            )

    def test_real_late_failure_publishes_only_complete_upstream_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            calls: list[tuple[str, ...]] = []
            first = self.coordinator(root, calls)

            def fail_binding(command: tuple[str, ...], _environment: object) -> None:
                calls.append(command)
                if command == ("binding",):
                    raise GATE.subprocess.CalledProcessError(1, command)

            first.runner = fail_binding
            with self.assertRaisesRegex(GATE.ValidationError, "stage binding failed"):
                first.run()
            self.assertIn("rust", first.results)
            self.assertNotIn("binding", first.results)

            calls.clear()
            resumed = self.coordinator(root, calls)
            resumed.run()
            self.assertEqual(calls, [("binding",)])
            self.assertEqual(resumed.results["rust"].status, "hit")

    def test_input_change_invalidates_only_affected_stage_and_dependents(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            calls: list[tuple[str, ...]] = []
            self.coordinator(root, calls).run()
            (root / "pyproject.toml").write_text("[project]\nname='changed'\n", encoding="utf-8")
            calls.clear()
            rerun = self.coordinator(root, calls)
            rerun.run()
            self.assertEqual(calls, [("binding",)])
            self.assertEqual(rerun.results["rust"].status, "hit")

    def test_unrelated_checked_in_change_does_not_invalidate_stages(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            (root / "unrelated.md").write_text("first\n", encoding="utf-8")
            calls: list[tuple[str, ...]] = []
            self.coordinator(root, calls).run()
            (root / "unrelated.md").write_text("changed\n", encoding="utf-8")
            calls.clear()
            rerun = self.coordinator(root, calls)
            rerun.run()
            self.assertEqual(calls, [])
            self.assertEqual(rerun.results["rust"].status, "hit")

    def test_corrupt_evidence_fails_closed_and_reruns_affected_stage(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            calls: list[tuple[str, ...]] = []
            initial = self.coordinator(root, calls)
            initial.run()
            evidence = initial.evidence_path(initial.stages["rust"], initial.results["rust"].digest)
            evidence.write_text("not json", encoding="utf-8")
            calls.clear()
            rerun = self.coordinator(root, calls)
            rerun.run()
            self.assertEqual(calls, [("rust",), ("binding",)])
            self.assertEqual(rerun.results["binding"].status, "miss")

    def test_evidence_missing_artifacts_key_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            calls: list[tuple[str, ...]] = []
            initial = self.coordinator(root, calls)
            initial.run()
            evidence_path = initial.evidence_path(
                initial.stages["rust"], initial.results["rust"].digest
            )
            value = GATE.json.loads(evidence_path.read_text(encoding="utf-8"))
            del value["artifacts"]
            evidence_path.write_text(GATE.json.dumps(value), encoding="utf-8")
            calls.clear()
            rerun = self.coordinator(root, calls)
            rerun.run()
            self.assertEqual(calls, [("rust",), ("binding",)])
            self.assertEqual(rerun.results["rust"].reason, "miss:malformed-evidence")

    def test_cargo_cache_digest_excludes_first_party_sources(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            (root / "rust-toolchain.toml").write_text("[toolchain]\n", encoding="utf-8")
            crate = root / "crates" / "demo"
            crate.mkdir(parents=True)
            (crate / "Cargo.toml").write_text("[package]\nname='demo'\n", encoding="utf-8")
            (crate / "lib.rs").write_text("fn first() {}\n", encoding="utf-8")
            stage = GATE.Stage("heavy", commands=(("cargo",),), heavy=True)
            coordinator = GATE.Coordinator(root, (), cache_stages=(stage,))
            coordinator.command_versions = lambda _stage: {"tool": "test"}
            before = coordinator.cargo_cache_digest(stage)
            (crate / "lib.rs").write_text("fn changed() {}\n", encoding="utf-8")
            after = coordinator.cargo_cache_digest(stage)
            self.assertEqual(before, after)
            (crate / "Cargo.toml").write_text("[package]\nname='demo2'\n", encoding="utf-8")
            self.assertNotEqual(before, coordinator.cargo_cache_digest(stage))

    def test_missing_native_artifact_rejects_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            addon = root / "native.node"
            addon.write_bytes(b"first")
            calls: list[tuple[str, ...]] = []

            def runner(command: tuple[str, ...], _environment: object) -> None:
                calls.append(command)

            stages = (
                GATE.Stage("preflight", inputs=("pyproject.toml",)),
                GATE.Stage(
                    "native",
                    commands=(("native",),),
                    dependencies=("preflight",),
                    artifacts=("*.node",),
                ),
            )
            initial = GATE.Coordinator(root, stages, runner=runner)
            initial.run_preflight = lambda _environment: None
            initial.command_versions = lambda _stage: {"tool": "test"}
            initial.run()
            addon.unlink()
            calls.clear()
            rerun = GATE.Coordinator(root, stages, runner=runner)
            rerun.run_preflight = lambda _environment: None
            rerun.command_versions = lambda _stage: {"tool": "test"}
            with self.assertRaisesRegex(GATE.ValidationError, "did not produce"):
                rerun.run()
            self.assertEqual(calls, [("native",)])

    def test_force_clean_reruns_every_stage(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            calls: list[tuple[str, ...]] = []
            self.coordinator(root, calls).run()
            calls.clear()
            rerun = self.coordinator(root, calls)
            rerun.force_clean = True
            rerun.run()
            self.assertEqual(calls, [("rust",), ("binding",)])
            self.assertTrue(all(result.status == "miss" for result in rerun.results.values()))

    def test_preflight_failure_occurs_before_heavy_runner(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            calls: list[tuple[str, ...]] = []
            coordinator = self.coordinator(root, calls)

            def fail_preflight(_environment: object) -> None:
                raise GATE.ValidationError("missing prerequisite: napi")

            coordinator.run_preflight = fail_preflight
            with self.assertRaisesRegex(GATE.ValidationError, "missing prerequisite"):
                coordinator.run()
            self.assertEqual(calls, [])
            self.assertEqual(coordinator.results, {})

    def test_command_contract_change_invalidates_stage_and_dependent(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            calls: list[tuple[str, ...]] = []
            initial = self.coordinator(root, calls)
            initial.run()
            calls.clear()
            changed = self.coordinator(root, calls)
            changed.stages["rust"] = GATE.Stage(
                "rust",
                commands=(("rust", "--new-contract"),),
                dependencies=("preflight",),
                inputs=("Cargo.lock",),
            )
            changed.run()
            self.assertEqual(calls, [("rust", "--new-contract"), ("binding",)])

    def test_evidence_publication_leaves_no_partial_file(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            coordinator = self.coordinator(root, [])
            coordinator.run()
            evidence = coordinator.evidence_path(
                coordinator.stages["rust"], coordinator.results["rust"].digest
            )
            self.assertEqual(evidence.stat().st_mode & 0o777, 0o600)
            self.assertEqual(list(evidence.parent.glob("tmp*")), [])

    def test_post_build_profiles_are_isolated_under_worktree_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            stage = GATE.Stage("node-wrapper-coverage", profile_isolation=True)
            coordinator = GATE.Coordinator(root, (stage,))
            environment = coordinator.stage_environment(stage, "digest")
            profile = Path(environment["LLVM_PROFILE_FILE"])
            self.assertTrue(profile.parent.is_relative_to(coordinator.evidence_root))
            self.assertEqual(profile.name, "%p-%m.profraw")
            self.assertTrue(profile.parent.is_dir())

    def test_disk_preflight_names_safe_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            coordinator = GATE.Coordinator(root, ())
            usage = type("DiskUsage", (), {"free": 0})()
            with (
                patch.object(GATE.shutil, "disk_usage", return_value=usage),
                self.assertRaisesRegex(GATE.ValidationError, "make clean-builds"),
            ):
                coordinator.run_preflight({"GF_VALIDATION_ROOT": str(root)})

    def test_warm_compatible_cache_reduces_estimated_disk_need(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            stage = GATE.Stage("heavy", commands=(("cargo",),), heavy=True)
            coordinator = GATE.Coordinator(root, (), cache_stages=(stage,))
            coordinator.command_versions = lambda _stage: {"tool": "test"}
            cache = (
                coordinator.shared_cache_root / "cargo" / coordinator.cargo_cache_digest(stage)[:24]
            )
            cache.mkdir(parents=True)
            (cache / "fingerprint").write_text("warm", encoding="utf-8")
            usage = type("DiskUsage", (), {"free": 30 * 1024**3})()

            def successful_run(*_args: object, **_kwargs: object) -> object:
                return type("Completed", (), {"returncode": 0})()

            with (
                patch.object(GATE.shutil, "which", return_value="/bin/tool"),
                patch.object(GATE.shutil, "disk_usage", return_value=usage),
                patch.object(GATE.subprocess, "run", side_effect=successful_run),
                patch.object(
                    GATE.subprocess,
                    "check_output",
                    return_value="llvm-tools-preview-aarch64 installed\n",
                ),
            ):
                coordinator.run_preflight({"GF_VALIDATION_ROOT": str(root)})
            self.assertEqual(coordinator.estimated_required_gib, 20)

    def test_default_graph_executes_full_rust_corpus_once(self) -> None:
        commands = [command for stage in GATE.stages() for command in stage.commands]
        self.assertNotIn(("cargo", "test", "--workspace"), commands)
        self.assertEqual(commands.count(("make", "coverage-rust")), 1)
        coverage = next(
            stage for stage in GATE.stages() if stage.name == "rust-tests-coverage-native"
        )
        self.assertEqual(
            coverage.commands[0],
            ("bash", "scripts/ci/test-coverage-rust.sh"),
        )
        self.assertEqual(coverage.dependencies, ("rust-quality",))
        self.assertTrue(coverage.python_extension)
        self.assertEqual(coverage.artifacts, ("crates/graphforge-bindings-node/*.node",))
        self.assertFalse(any(command[0] == "uv" and "maturin" in command for command in commands))
        self.assertFalse(any("napi" in command and "build" in command for command in commands))
        final_thresholds = next(
            stage for stage in GATE.stages() if stage.name == "final-thresholds"
        )
        self.assertTrue(final_thresholds.profile_isolation)

    def test_failure_summary_is_fail_closed_and_separate_from_preflight(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            self.make_root(root)
            coordinator = self.coordinator(root, [])
            coordinator.results["preflight"] = GATE.StageResult(
                "preflight", "digest", "proof", "miss", "mandatory", 0.1
            )
            path = coordinator.write_summary(
                list(coordinator.results.values()),
                outcome="failed",
                error="stage rust failed",
                filename="preflight-summary.json",
            )
            value = GATE.json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(path.name, "preflight-summary.json")
            self.assertEqual(value["outcome"], "failed")
            self.assertEqual(value["error"], "stage rust failed")
            self.assertFalse((path.parent / "summary.json").exists())


if __name__ == "__main__":
    unittest.main()
