#!/usr/bin/env python3
"""Deterministic contract tests for resumable local pre-push validation."""

from __future__ import annotations

import fcntl
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

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

    def test_policy_cache_tracks_source_size_contents_and_git_membership(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)

            def git(*arguments: str) -> None:
                GATE.subprocess.run(("git", *arguments), cwd=root, check=True, capture_output=True)

            def write(name: str, content: str) -> None:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")

            git("init", "--quiet")
            source = "crates/demo/src/build/nested.data"
            target_source = "crates/demo/src/target/other.txt"
            ordinary_source = "crates/demo/src/lib.rs"
            policy = "config/source-size-policy.json"
            adr = "docs/adr/0031-source-size-policy.md"
            for name in (source, target_source, ordinary_source, policy, adr, "unrelated.md"):
                write(name, "initial\n")
            git("add", ".")
            calls: list[tuple[str, ...]] = []
            selected = tuple(
                stage for stage in GATE.stages() if stage.name in {"preflight", "policy-static"}
            )
            policy_stage = next(stage for stage in selected if stage.name == "policy-static")

            def run(expected: str) -> None:
                calls.clear()
                coordinator = GATE.Coordinator(
                    root, selected, runner=lambda command, _environment: calls.append(command)
                )
                coordinator.run_preflight = lambda _environment: None
                coordinator.command_versions = lambda _stage: {"tool": "test"}
                coordinator.run()
                self.assertEqual(coordinator.results["policy-static"].status, expected)
                self.assertEqual(calls, list(policy_stage.commands) if expected == "miss" else [])

            run("miss")
            run("hit")
            # Every covered extension and ignored-looking source component is authoritative.
            for name in (source, target_source, ordinary_source, policy, adr):
                with self.subTest(changed=name):
                    write(name, "changed\n")
                    run("miss")
                    run("hit")
            # Same bytes and paths, different Git membership: neither operation may reuse proof.
            added = "crates/demo/src/new.bin"
            write(added, "new\n")
            run("hit")
            git("add", added)
            run("miss")
            for name in (source, adr, policy):
                with self.subTest(untracked=name):
                    git("rm", "--cached", "--force", name)
                    self.assertTrue((root / name).is_file())
                    run("miss")
                    git("add", name)
                    # Returning to the exact previously proven state can reuse its evidence.
                    run("hit")
            # A missing tracked working-tree file remains represented, forcing the checker to run.
            (root / ordinary_source).unlink()
            run("miss")
            write(ordinary_source, "restored\n")
            run("miss")
            # Invalid file kinds must not reuse proof for identical regular-file contents.
            for name in (source, adr):
                with self.subTest(symlink=name):
                    path = root / name
                    original_bytes = path.read_bytes()
                    target = root / "same-content.bin"
                    target.write_bytes(original_bytes)
                    path.unlink()
                    path.symlink_to(target)
                    run("miss")
                    path.unlink()
                    path.write_bytes(original_bytes)
                    run("hit")
            # A regular terminal file reached through an escaping directory is also invalid.
            with tempfile.TemporaryDirectory() as outside:
                directory = (root / source).parent
                original_bytes = (root / source).read_bytes()
                (root / source).unlink()
                directory.rmdir()
                external = Path(outside)
                (external / Path(source).name).write_bytes(original_bytes)
                directory.symlink_to(external, target_is_directory=True)
                run("miss")
                directory.unlink()
                directory.mkdir()
                (root / source).write_bytes(original_bytes)
                run("hit")
            # Near-miss source roots and unrelated index/content changes are not size inputs.
            for name in (
                "unrelated.md",
                "crates/group/demo/src/note.txt",
                "crates/demo/test/note.txt",
            ):
                with self.subTest(unrelated=name):
                    write(name, "unrelated change\n")
                    git("add", name)
                    run("hit")

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
                patch.object(GATE.shutil, "which", return_value="/bin/tool"),
                patch.object(GATE.shutil, "disk_usage", return_value=usage),
                self.assertRaisesRegex(
                    GATE.ValidationError,
                    "make clean-builds.*graphforge-validation-cache.*"
                    "GF_PRE_PUSH_CACHE_KEEP_ENTRIES",
                ),
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

    def heavy_cache(self, root: Path, keep: int) -> tuple[object, object, Path]:
        self.make_root(root)
        stage = GATE.Stage("heavy", commands=(("cargo",),), heavy=True)
        coordinator = GATE.Coordinator(
            root, (stage,), cache_stages=(stage,), cache_keep_entries=keep
        )
        coordinator.command_versions = lambda _stage: {"tool": "test"}
        return coordinator, stage, coordinator.shared_cache_root / "cargo"

    def cache_entry(self, cargo: Path, name: str, last_used: float | None) -> Path:
        entry = cargo / name
        entry.mkdir(parents=True)
        (entry / "fingerprint").write_text("warm", encoding="utf-8")
        if last_used is not None:
            marker = entry / GATE.CACHE_LAST_USED
            marker.touch()
            os.utime(marker, (last_used, last_used))
        return entry

    def entries(self, cargo: Path) -> set[str]:
        return {entry.name for entry in cargo.iterdir()}

    def preflight_patches(self, free_gib: int) -> tuple[object, ...]:
        usage = type("DiskUsage", (), {"free": free_gib * 1024**3})()

        def successful_run(*_args: object, **_kwargs: object) -> object:
            return type("Completed", (), {"returncode": 0})()

        return (
            patch.object(GATE.shutil, "which", return_value="/bin/tool"),
            patch.object(GATE.shutil, "disk_usage", return_value=usage),
            patch.object(GATE.subprocess, "run", side_effect=successful_run),
            patch.object(
                GATE.subprocess,
                "check_output",
                return_value="llvm-tools-preview-aarch64 installed\n",
            ),
        )

    def test_cache_prune_evicts_least_recently_used_beyond_bound(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            coordinator, stage, cargo = self.heavy_cache(Path(raw), keep=3)
            live = self.cache_entry(cargo, coordinator.cargo_cache_path(stage).name, 1.0)
            for name, used in (("a", 400.0), ("b", 100.0), ("c", 300.0), ("d", 200.0)):
                self.cache_entry(cargo, name, used)
            with coordinator.heavy_lock():
                evicted = coordinator.prune_cargo_cache()
            # One live entry plus the two most recently used others fill the bound of three,
            # even though the live entry is the least recently used of all.
            self.assertEqual(evicted, ["b", "d"])
            self.assertEqual(self.entries(cargo), {live.name, "a", "c"})

    def test_cache_prune_never_evicts_live_entries_below_bound(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            coordinator, stage, cargo = self.heavy_cache(Path(raw), keep=0)
            live = self.cache_entry(cargo, coordinator.cargo_cache_path(stage).name, None)
            self.cache_entry(cargo, "recent", 500.0)
            with coordinator.heavy_lock():
                self.assertEqual(coordinator.prune_cargo_cache(), ["recent"])
            self.assertEqual(self.entries(cargo), {live.name})

    def test_cache_prune_ranks_unrecorded_entries_least_recent(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            coordinator, stage, cargo = self.heavy_cache(Path(raw), keep=2)
            live = coordinator.cargo_cache_path(stage).name
            legacy = self.cache_entry(cargo, "legacy", None)
            os.utime(legacy, (10_000.0, 10_000.0))
            self.cache_entry(cargo, "recorded", 1.0)
            with coordinator.heavy_lock():
                self.assertEqual(coordinator.prune_cargo_cache(), ["legacy"])
            self.assertEqual(self.entries(cargo), {"recorded"})
            self.assertNotIn(live, self.entries(cargo))

    def test_heavy_stage_records_use_and_prunes_while_holding_lock(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            coordinator, stage, cargo = self.heavy_cache(root, keep=1)
            self.cache_entry(cargo, "stale", 1.0)
            observed: list[bool] = []

            def runner(_command: tuple[str, ...], environment: dict[str, str]) -> None:
                target = Path(environment["CARGO_TARGET_DIR"])
                observed.append((target / GATE.CACHE_LAST_USED).is_file())
                observed.append(not (cargo / "stale").exists())
                lock_path = coordinator.shared_cache_root / "heavy-build.lock"
                with (
                    lock_path.open("a", encoding="utf-8") as other,
                    self.assertRaises(BlockingIOError),
                ):
                    fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
                observed.append(True)

            coordinator.runner = runner
            coordinator.collect_artifacts = lambda _stage: []
            coordinator.run_stage(stage)
            self.assertEqual(observed, [True, True, True])
            self.assertEqual(self.entries(cargo), {coordinator.cargo_cache_path(stage).name})

    def test_preflight_prunes_before_disk_check_when_lock_is_free(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            coordinator, _stage, cargo = self.heavy_cache(Path(raw), keep=1)
            self.cache_entry(cargo, "stale", 1.0)
            patches = self.preflight_patches(free_gib=500)
            with patches[0], patches[1], patches[2], patches[3]:
                coordinator.run_preflight({"GF_VALIDATION_ROOT": raw})
            self.assertEqual(self.entries(cargo), set())
            self.assertEqual(coordinator.estimated_required_gib, GATE.MIN_FREE_GIB)

    def test_preflight_neither_blocks_nor_prunes_while_lock_is_held(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            coordinator, _stage, cargo = self.heavy_cache(Path(raw), keep=1)
            self.cache_entry(cargo, "stale", 1.0)
            lock_path = coordinator.shared_cache_root / "heavy-build.lock"
            patches = self.preflight_patches(free_gib=500)
            with lock_path.open("a", encoding="utf-8") as holder:
                fcntl.flock(holder, fcntl.LOCK_EX)
                with patches[0], patches[1], patches[2], patches[3]:
                    coordinator.run_preflight({"GF_VALIDATION_ROOT": raw})
            self.assertEqual(self.entries(cargo), {"stale"})

    def test_recorded_use_alone_does_not_count_as_warm_cache(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            coordinator, stage, _cargo = self.heavy_cache(Path(raw), keep=1)
            with coordinator.heavy_lock():
                coordinator.mark_cache_used(stage)
            patches = self.preflight_patches(free_gib=500)
            with patches[0], patches[1], patches[2], patches[3]:
                coordinator.run_preflight({"GF_VALIDATION_ROOT": raw})
            self.assertEqual(coordinator.estimated_required_gib, GATE.MIN_FREE_GIB)

    def test_cache_keep_entries_environment_override(self) -> None:
        for raw_value in ("-1", "six"):
            with (
                patch.dict(os.environ, {"GF_PRE_PUSH_CACHE_KEEP_ENTRIES": raw_value}),
                patch.object(GATE, "Coordinator") as constructed,
                patch("sys.stderr"),
            ):
                self.assertEqual(GATE.main(["preflight"]), 1)
                constructed.assert_not_called()
        instance = MagicMock(root=Path("/repository"))
        instance.run.return_value = []
        instance.write_summary.return_value = Path("/repository/summary.json")
        for raw_value, expected in (("2", 2), ("", GATE.CACHE_KEEP_ENTRIES)):
            with (
                patch.dict(os.environ, {"GF_PRE_PUSH_CACHE_KEEP_ENTRIES": raw_value}),
                patch.object(GATE, "Coordinator", return_value=instance) as constructed,
                patch("sys.stdout"),
            ):
                self.assertEqual(GATE.main(["preflight"]), 0)
            self.assertEqual(constructed.call_args.kwargs["cache_keep_entries"], expected)

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
        self.assertIn(".github/**/*.yml", coverage.inputs)
        self.assertIn(".github/**/*.yaml", coverage.inputs)
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
