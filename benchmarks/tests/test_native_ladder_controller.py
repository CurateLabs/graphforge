from __future__ import annotations

from collections import namedtuple
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from graphforge_bench.progressive_host_run import (
    HostRunError,
    completed_prefix,
    execute_ladder,
    measure_host_capacity,
    validated_host_rung,
)
from tests.host_run_fixture import ROOT, executables, write_host_bundle
from tests.test_progressive_host_run import COMMIT, WORK_PARENT, sha256


class NativeLadderControllerTests(unittest.TestCase):
    def test_legacy_resume_detects_deleted_runtime_helper(self) -> None:
        import shutil
        import subprocess

        from graphforge_bench.progressive_host_run import producer_digest, producer_files

        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            base = Path(temporary)
            work = base / "work"
            output = work / "evidence"
            write_host_bundle(output, 18)
            tools = executables(work)
            repository = base / "checkout"
            copied_root = repository / "benchmarks"
            for source in [
                *producer_files(ROOT),
                ROOT / "profiles/local-linux-cgroups-v2.json",
                ROOT / "profiles/graph500/s18-local.json",
                ROOT / "runners/graph500-generator/src/main.rs",
            ]:
                destination = copied_root / source.relative_to(ROOT)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, destination)
            helper = copied_root / "harness/graphforge_bench/retired_runtime.py"
            helper.write_text("value = 1\n")

            def git(*arguments: str) -> str:
                return (
                    subprocess.check_output(
                        ["git", "-C", str(repository), *arguments], stderr=subprocess.STDOUT
                    )
                    .decode()
                    .strip()
                )

            git("init", "-q")
            git("add", ".")
            git(
                "-c",
                "user.name=Codex",
                "-c",
                "user.email=codex@openai.com",
                "commit",
                "-qm",
                "original producer",
            )
            commit = git("rev-parse", "HEAD")
            original = producer_digest(copied_root)
            plan = json.loads((output / "s18-plan.json").read_text())
            plan["identities"].pop("producer_sha256")
            plan["identities"]["commit"] = commit
            write_host_bundle(output, 18, plan)
            options = {
                "root": copied_root,
                "output_dir": output,
                "work_root": work,
                "maximum_scale": 18,
                "executables": tools,
                "commit": commit,
                "reserved_headroom_bytes": 1,
            }
            (copied_root / "README.md").write_text("Documentation-only change.\n")
            self.assertEqual(execute_ladder(**options), [])
            helper.unlink()
            self.assertEqual(producer_digest(copied_root, commit=commit), original)
            self.assertNotEqual(producer_digest(copied_root), original)
            with self.assertRaisesRegex(HostRunError, "host_prefix_producer_identity_mismatch"):
                execute_ladder(**options)

    def test_plan_then_run_same_directory_and_automatic_advance(self) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            work = Path(temporary)
            output = work / "evidence"
            options = {
                "root": ROOT,
                "output_dir": output,
                "work_root": work,
                "maximum_scale": 20,
                "executables": executables(work),
                "commit": COMMIT,
                "reserved_headroom_bytes": 1,
            }
            launched = []

            def execute(**kwargs):
                scale = kwargs["scale"]
                launched.append(scale)
                payload = work / "workspace" / f"s{scale}" / "payload"
                payload.parent.mkdir(parents=True)
                payload.write_bytes(b"dataset")
                write_host_bundle(output, scale, kwargs["plan"])

            with (
                patch("graphforge_bench.progressive_host_run.version", return_value="3.35"),
                patch("graphforge_bench.progressive_host_run.run", side_effect=execute),
            ):
                preview = execute_ladder(**options, dry_run=True)
                self.assertEqual([p["rung"] for p in preview], ["S18"])
                self.assertFalse(output.exists())
                plans = execute_ladder(**options)
            self.assertEqual(launched, [18, 19, 20])
            self.assertEqual(len(plans), 3)
            self.assertEqual(len(completed_prefix(ROOT, output)), 3)
            self.assertFalse((work / "workspace/s20").exists())
            projection = json.loads((output / "s20-projection.json").read_text())
            self.assertEqual(
                projection["native_capacity"]["rate_source"], "completed_adjacent_rungs"
            )
            self.assertNotIn("image_digest", plans[-1]["identities"])
            self.assertEqual(
                plans[-1]["identities"]["admitted_projection_sha256"],
                sha256(output / "s20-projection.json"),
            )

    def test_s20_preview_never_publishes_projection(self) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            work = Path(temporary)
            output = work / "evidence"
            write_host_bundle(output, 18)
            write_host_bundle(output, 19)
            before = {p.name: p.read_bytes() for p in output.iterdir()}
            with patch("graphforge_bench.progressive_host_run.version", return_value="3.35"):
                plans = execute_ladder(
                    root=ROOT,
                    output_dir=output,
                    work_root=work,
                    maximum_scale=20,
                    executables=executables(work),
                    commit=COMMIT,
                    reserved_headroom_bytes=1,
                    dry_run=True,
                )
            self.assertEqual(plans[0]["rung"], "S20")
            self.assertEqual(before, {p.name: p.read_bytes() for p in output.iterdir()})

    def test_actual_work_root_free_space_and_reserve_refuse(self) -> None:
        usage = namedtuple("usage", "total used free")
        with patch(
            "graphforge_bench.progressive_host_run.shutil.disk_usage",
            return_value=usage(10**12, 10**12 - 100, 100),
        ) as probe:
            with self.assertRaisesRegex(HostRunError, "work_root_capacity_refused"):
                measure_host_capacity(Path("/declared/work"), 100)
            self.assertEqual(measure_host_capacity(Path("/declared/work"), 99)["free_bytes"], 100)
            probe.assert_called_with(Path("/declared/work"))

    def test_first_execution_failure_stops_and_retains_attempt(self) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            work = Path(temporary)
            output = work / "evidence"
            with (
                patch("graphforge_bench.progressive_host_run.version", return_value="3.35"),
                patch(
                    "graphforge_bench.progressive_host_run.run",
                    side_effect=HostRunError("benchexec_failed"),
                ) as run,
                self.assertRaisesRegex(HostRunError, "benchexec_failed"),
            ):
                execute_ladder(
                    root=ROOT,
                    output_dir=output,
                    work_root=work,
                    maximum_scale=26,
                    executables=executables(work),
                    commit=COMMIT,
                    reserved_headroom_bytes=1,
                )
            self.assertEqual(run.call_count, 1)
            self.assertTrue((output / "s18-plan.json").exists())
            self.assertFalse((output / "s19-plan.json").exists())

    def test_receipts_cannot_be_removed_even_with_refreshed_artifact_hash(self) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            output = Path(temporary)
            write_host_bundle(output, 18)
            gf_path = output / "s18-graphforge.json"
            gf = json.loads(gf_path.read_text())
            for phase in gf["phases"]:
                phase["receipts"] = []
            gf_path.write_text(json.dumps(gf))
            result_path = output / "s18-result.json"
            result = json.loads(result_path.read_text())
            result["artifacts"]["graphforge_sha256"] = sha256(gf_path)
            result_path.write_text(json.dumps(result))
            with self.assertRaises(ValueError):
                validated_host_rung(ROOT, output, 18)

    def test_native_projection_uses_free_capacity_and_preserves_rss_refusal(self) -> None:
        from graphforge_bench.progressive_qualification import load_profiles, project
        from tests.test_progressive_host_run import passed_rung

        profile = next(p for p in load_profiles() if p.scale == 20)
        low, high = passed_rung(18), passed_rung(19)
        for rung in (low, high):
            rung["metrics"]["reader_calls"] = 8000
            rung["metrics"]["publication_work_units"] = 9000
        capacity = {"free_bytes": 1000, "reserved_headroom_bytes": 399}
        accepted = project(profile, [low, high], native_capacity=capacity)
        self.assertEqual(accepted["decision"], "admitted")
        self.assertEqual(accepted["limits"]["volume_bytes"], 1000)
        self.assertIsNone(accepted["provider_capacity"])
        self.assertEqual(
            accepted["native_capacity"]["observed_rates"]["physical_read_bytes_per_second"],
            {"work_units": 600, "wall_seconds": 10},
        )
        rejected = project(
            profile, [low, high], native_capacity=capacity | {"reserved_headroom_bytes": 401}
        )
        self.assertEqual(rejected["decision"], "refused")
        self.assertFalse(rejected["checks"]["storage_headroom"])
        high["metrics"]["peak_rss_bytes"] = 111
        rss = project(profile, [low, high], native_capacity=capacity)
        self.assertEqual(rss["decision"], "refused")
        self.assertFalse(rss["checks"]["rss_bounded_or_plateaued"])

    def test_cli_resolves_executables_once_for_maximum_scale(self) -> None:
        from contextlib import redirect_stdout
        import io

        from graphforge_bench.progressive_host_run import main

        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            work = Path(temporary)
            tools = executables(work)
            with (
                patch(
                    "graphforge_bench.progressive_host_run.resolve_host_benchexec_python",
                    return_value=tools.benchexec_python,
                ),
                patch(
                    "graphforge_bench.progressive_host_run.resolve_executables", return_value=tools
                ) as resolve,
                patch(
                    "graphforge_bench.progressive_host_run.execute_ladder", return_value=[]
                ) as execute,
                redirect_stdout(io.StringIO()),
            ):
                code = main(
                    [
                        "--maximum-scale",
                        "26",
                        "--work-root",
                        str(work),
                        "--output-dir",
                        str(work / "evidence"),
                        "--gf",
                        str(tools.gf),
                        "--certify",
                        str(tools.certify),
                        "--generator",
                        str(tools.generator),
                    ]
                )
            self.assertEqual(code, 0)
            resolve.assert_called_once()
            self.assertEqual(execute.call_args.kwargs["maximum_scale"], 26)

    def test_native_storage_consumer_uses_ordinary_receipts_without_cloud_identity(self) -> None:
        from graphforge_bench.progressive_storage_qualification import (
            StorageQualificationError,
            build_native,
        )

        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            work = Path(temporary)
            output = work / "evidence"
            write_host_bundle(output, 20)
            write_host_bundle(output, 22)
            paths = [output / "s20-rung.json", output / "s22-rung.json"]
            qualification = build_native(paths, work_root=work, reserved_headroom_bytes=1)
            self.assertEqual(qualification["schema"], "graphforge-g500-ladder-qualification/3")
            retained = work / "workspace" / "s20"
            retained.mkdir(parents=True)
            with self.assertRaisesRegex(StorageQualificationError, "not been reclaimed"):
                build_native(paths, work_root=work, reserved_headroom_bytes=1)

    def test_zero_cached_reads_and_fractional_rates_do_not_invent_capacity_refusals(self) -> None:
        from graphforge_bench.progressive_qualification import load_profiles, project
        from tests.test_progressive_host_run import passed_rung

        profile = next(p for p in load_profiles() if p.scale == 20)
        capacity = {"free_bytes": 10**12, "reserved_headroom_bytes": 1}
        for low_reads, high_reads, low_time, high_time in (
            (16384, 0, 195, 599),
            (0, 16384, 195, 599),
            (0, 0, 195, 599),
            (1, 2, 3, 3),
        ):
            with self.subTest(observations=(low_reads, high_reads, low_time, high_time)):
                low, high = passed_rung(18), passed_rung(19)
                for rung, reads, seconds in (
                    (low, low_reads, low_time),
                    (high, high_reads, high_time),
                ):
                    rung["metrics"].update(physical_read_bytes=reads, wall_seconds=seconds)
                evidence = project(profile, [low, high], native_capacity=capacity)
                self.assertTrue(evidence["checks"]["io_reader_publication_headroom"])
                observed = evidence["native_capacity"]["observed_rates"][
                    "physical_read_bytes_per_second"
                ]
                if low_reads == high_reads == 0:
                    self.assertIsNone(observed)
                elif low_reads == 1:
                    self.assertEqual(observed, {"work_units": 1, "wall_seconds": 3})
        low, high = passed_rung(18), passed_rung(19)
        low["metrics"].update(physical_read_bytes=1, wall_seconds=10000)
        high["metrics"].update(physical_read_bytes=2, wall_seconds=10000)
        insufficient = project(profile, [low, high], native_capacity=capacity)
        self.assertFalse(insufficient["checks"]["io_reader_publication_headroom"])

    def test_resume_reclaims_accepted_final_rung_without_rerunning_or_changing_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            work = Path(temporary)
            output = work / "evidence"
            write_host_bundle(output, 18)
            tools = executables(work)
            retained = work / "workspace/s18"
            retained.mkdir(parents=True)
            (retained / "payload").write_bytes(b"accepted before interruption")
            before = {p.name: p.read_bytes() for p in output.iterdir()}
            args = {
                "root": ROOT,
                "output_dir": output,
                "work_root": work,
                "maximum_scale": 18,
                "executables": tools,
                "commit": "a" * 40,
                "reserved_headroom_bytes": 1,
            }
            with patch("graphforge_bench.progressive_host_run.run") as run:
                self.assertEqual(execute_ladder(**args, dry_run=True), [])
                self.assertTrue(retained.exists())
                self.assertEqual(execute_ladder(**args), [])
                run.assert_not_called()
            self.assertFalse(retained.exists())
            self.assertEqual(before, {p.name: p.read_bytes() for p in output.iterdir()})

    def test_changed_binary_refuses_resume_before_cleanup(self) -> None:
        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            work = Path(temporary)
            output = work / "evidence"
            write_host_bundle(output, 18)
            tools = executables(work)
            tools.gf.write_bytes(b"different binary")
            retained = work / "workspace/s18"
            retained.mkdir(parents=True)
            with self.assertRaisesRegex(HostRunError, "host_prefix_identity_mismatch"):
                execute_ladder(
                    root=ROOT,
                    output_dir=output,
                    work_root=work,
                    maximum_scale=18,
                    executables=tools,
                    commit=COMMIT,
                    reserved_headroom_bytes=1,
                )
            self.assertTrue(retained.exists())

    def test_nested_graphforge_and_phase_success_contradictions_are_rejected(self) -> None:
        for mutation in ("nested", "phase_order"):
            with (
                self.subTest(mutation=mutation),
                tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary,
            ):
                output = Path(temporary)
                write_host_bundle(output, 18)
                gf_path = output / "s18-graphforge.json"
                bx_path = output / "s18-benchexec.json"
                gf, bx = json.loads(gf_path.read_text()), json.loads(bx_path.read_text())
                if mutation == "nested":
                    bx["graphforge"]["profile_id"] = "different-profile"
                else:
                    gf["phases"].reverse()
                    bx["graphforge"] = gf
                    gf_path.write_text(json.dumps(gf))
                bx_path.write_text(json.dumps(bx))
                result_path = output / "s18-result.json"
                result = json.loads(result_path.read_text())
                result["artifacts"].update(
                    graphforge_sha256=sha256(gf_path), benchexec_sha256=sha256(bx_path)
                )
                result_path.write_text(json.dumps(result))
                with self.assertRaisesRegex(ValueError, "contradicts success"):
                    validated_host_rung(ROOT, output, 18)

    def test_projection_refusal_identifies_rung_and_keeps_actionable_measurements(self) -> None:
        from contextlib import redirect_stdout
        import io

        from graphforge_bench.progressive_host_run import main

        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            work = Path(temporary)
            output = work / "evidence"
            write_host_bundle(output, 18)
            write_host_bundle(output, 19)
            tools = executables(work)
            stream = io.StringIO()
            with (
                patch(
                    "graphforge_bench.progressive_host_run.resolve_host_benchexec_python",
                    return_value=tools.benchexec_python,
                ),
                patch(
                    "graphforge_bench.progressive_host_run.measure_host_capacity",
                    return_value={"free_bytes": 100, "reserved_headroom_bytes": 1},
                ),
                patch("graphforge_bench.progressive_host_run.run") as run,
                redirect_stdout(stream),
            ):
                code = main(
                    [
                        "--maximum-scale",
                        "26",
                        "--work-root",
                        str(work),
                        "--output-dir",
                        str(output),
                        "--gf",
                        str(tools.gf),
                        "--certify",
                        str(tools.certify),
                        "--generator",
                        str(tools.generator),
                        "--dry-run",
                    ]
                )
            self.assertEqual(code, 2)
            failure = json.loads(stream.getvalue())
            self.assertEqual(failure["rung"], "S20")
            self.assertEqual(failure["failure"], "projection_refused")
            self.assertIn("storage_headroom", failure["failed_checks"])
            self.assertGreater(failure["projection"]["projected"]["storage_peak_bytes"], 100)
            run.assert_not_called()
            self.assertFalse((output / "s20-plan.json").exists())
            self.assertFalse((output / "s20-projection.json").exists())

    def test_actual_producer_change_refuses_but_documentation_change_can_resume(self) -> None:
        import shutil

        from graphforge_bench.progressive_host_run import producer_files

        with tempfile.TemporaryDirectory(dir=WORK_PARENT) as temporary:
            base = Path(temporary)
            work = base / "work"
            output = work / "evidence"
            write_host_bundle(output, 18)
            tools = executables(work)
            copied_root = base / "checkout/benchmarks"
            copied = [
                *producer_files(ROOT),
                ROOT / "profiles/local-linux-cgroups-v2.json",
                ROOT / "profiles/graph500/s18-local.json",
                ROOT / "runners/graph500-generator/src/main.rs",
            ]
            for source in copied:
                destination = copied_root / source.relative_to(ROOT)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, destination)
            (copied_root / "README.md").write_text("Unrelated documentation correction.\n")
            args = {
                "root": copied_root,
                "output_dir": output,
                "work_root": work,
                "maximum_scale": 18,
                "executables": tools,
                "commit": "a" * 40,
                "reserved_headroom_bytes": 1,
            }
            self.assertEqual(execute_ladder(**args), [])
            producer = copied_root / "harness/graphforge_bench/progressive_host_run.py"
            producer.write_text(
                producer.read_text().replace(
                    "DEFAULT_RESERVE_BYTES = 75", "DEFAULT_RESERVE_BYTES = 74"
                )
            )
            with self.assertRaisesRegex(HostRunError, "host_prefix_producer_identity_mismatch"):
                execute_ladder(**args)
            shutil.copyfile(ROOT / producer.relative_to(copied_root), producer)
            helper = copied_root / "harness/graphforge_bench/new_runtime/helper.py"
            helper.parent.mkdir()
            helper.write_text("def changed_runtime_helper(): return 1\n")
            with self.assertRaisesRegex(HostRunError, "host_prefix_producer_identity_mismatch"):
                execute_ladder(**args)
