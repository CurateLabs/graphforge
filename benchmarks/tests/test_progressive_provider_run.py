from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from graphforge_bench.progressive_provider_run import (
    ProviderRunError,
    _execution_commit,
    _schema,
    build_execution_plan,
    main,
    run,
    validate_admitted_plan,
)
from graphforge_bench.progressive_run import ControllerError, Executables
from tests.test_progressive_run import (
    benchexec as benchexec_fixture,
)
from tests.test_progressive_run import (
    graphforge as graphforge_fixture,
)
from tests.test_progressive_run import (
    passed_rung as rung_fixture,
)

ROOT = Path(__file__).resolve().parents[1]
COMMIT = subprocess.run(
    ["git", "-C", str(ROOT.parent), "rev-parse", "HEAD"],
    capture_output=True,
    check=True,
    text=True,
).stdout.strip()
IMAGE = "registry.fly.io/graphforge-bench@sha256:" + "1" * 64
PROJECTED_FIELDS = (
    "wall_seconds",
    "peak_rss_bytes",
    "retained_storage_bytes",
    "transient_peak_storage_bytes",
    "logical_read_bytes",
    "logical_write_bytes",
    "physical_read_bytes",
    "physical_write_bytes",
    "reader_calls",
    "publication_work_units",
    "storage_peak_bytes",
)
RATE_FIELDS = (
    "physical_read_bytes_per_second",
    "physical_write_bytes_per_second",
    "reader_calls_per_second",
    "publication_work_per_second",
)
SLOPE_FIELDS = (
    "logical_read_bytes",
    "logical_write_bytes",
    "physical_read_bytes",
    "physical_write_bytes",
    "reader_calls",
    "publication_work_units",
)
CHECK_FIELDS = (
    "time_headroom",
    "rss_headroom",
    "retained_storage_headroom",
    "transient_storage_headroom",
    "storage_headroom",
    "rss_bounded_or_plateaued",
    "io_reader_publication_capacity_measured",
    "io_reader_publication_headroom",
    "correctness",
)


def admitted_plan(scale: int = 20) -> dict:
    profile = ROOT / "profiles" / "graph500" / f"s{scale}-provider.json"
    ladder = [18, 19, 20, 22, 24, 25, 26]
    sources = {20: [18, 19], 22: [19, 20], 24: [20, 22], 25: [22, 24], 26: [24, 25]}
    return {
        "schema": "graphforge-progressive-provider-plan/1",
        "status": "admitted",
        "commit": COMMIT,
        "maximum_scale": 26,
        "completed_scales": ladder[: ladder.index(scale)],
        "next_rung": f"S{scale}",
        "execution": "provider",
        "profile_id": f"graph500-s{scale}-provider",
        "profile_path": f"profiles/graph500/s{scale}-provider.json",
        "profile_sha256": "sha256:" + hashlib.sha256(profile.read_bytes()).hexdigest(),
        "image_digest": IMAGE,
        "projection": {
            "schema": "graphforge-progressive-qualification-evidence/1",
            "target": f"S{scale}",
            "source_scales": sources[scale],
            "decision": "admitted",
            "limits": {
                "wall_seconds": 14_400,
                "rss_bytes": 4_294_967_296,
                "volume_bytes": 536_870_912_000,
            },
            "headroom": {
                "time_fraction": 0.2,
                "rss_fraction": 0.2,
                "storage_fraction": 0.15,
            },
            "projected": dict.fromkeys(PROJECTED_FIELDS, 1),
            "required_rates": dict.fromkeys(RATE_FIELDS, 1),
            "provider_capacity": dict.fromkeys(RATE_FIELDS, 100),
            "slopes_observed": dict.fromkeys(SLOPE_FIELDS, 1),
            "rss_growth_fraction": 0,
            "checks": dict.fromkeys(CHECK_FIELDS, True),
            "claim": "engineering_evidence_only",
        },
        "execution_authorized": True,
        "execution_refusal": None,
        "claim": "engineering_evidence_only",
    }


class ProgressiveProviderRunTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.output = self.base / "evidence"
        generator = self.base / "graphforge-benchmark-graph500-generator"
        generator.write_bytes((ROOT / "runners/graph500-generator/src/main.rs").read_bytes())
        gf = self.base / "gf"
        certify = self.base / "graphforge-benchmark-certify"
        python = self.base / "python"
        for path in (gf, certify, python):
            path.write_bytes(b"immutable fixture executable")
        for path in (generator, gf, certify, python):
            path.chmod(0o755)
        self.executables = Executables(gf, certify, generator, python)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def execution_plan(self) -> dict:
        with patch(
            "graphforge_bench.progressive_provider_run._benchexec_version",
            return_value="3.30",
        ):
            return build_execution_plan(
                root=ROOT,
                admitted_plan=admitted_plan(),
                admitted_plan_sha256="a" * 64,
                image_digest=IMAGE,
                executables=self.executables,
                source_tree_sha256="b" * 64,
            )

    def test_plan_binds_admission_image_and_all_native_identities(self) -> None:
        plan = self.execution_plan()
        identities = plan["identities"]
        self.assertEqual(plan["rung"], "S20")
        self.assertEqual(identities["commit"], COMMIT)
        self.assertEqual(identities["image_digest"], IMAGE)
        self.assertEqual(identities["admitted_plan_sha256"], "a" * 64)
        self.assertEqual(
            identities["profile_sha256"],
            hashlib.sha256((ROOT / "profiles/graph500/s20-provider.json").read_bytes()).hexdigest(),
        )
        _schema(ROOT, "progressive-provider-run-plan.json", plan)

    def test_image_commit_attestation_works_without_git_metadata(self) -> None:
        image_root = self.base / "image" / "benchmarks"
        image_root.mkdir(parents=True)
        (image_root.parent / "commit").write_text(COMMIT + "\n", encoding="ascii")
        with patch(
            "graphforge_bench.progressive_provider_run.repository_commit",
            side_effect=AssertionError("image execution must not require .git"),
        ):
            self.assertEqual(_execution_commit(image_root), COMMIT)

        (image_root.parent / "commit").write_text("not-a-commit\n", encoding="ascii")
        with self.assertRaisesRegex(ProviderRunError, "malformed"):
            _execution_commit(image_root)

    def test_production_cli_has_no_laptop_or_executable_override_fallback(self) -> None:
        with (
            patch(
                "graphforge_bench.progressive_provider_run._require_work_mount",
                side_effect=ProviderRunError("provider work volume is unavailable"),
            ),
            patch(
                "graphforge_bench.progressive_provider_run._read_document",
                side_effect=AssertionError("must refuse before reading a caller plan"),
            ),
            patch(
                "graphforge_bench.progressive_provider_run.resolve_executables",
                side_effect=AssertionError("must refuse before selecting executables"),
            ),
        ):
            self.assertEqual(
                main(
                    [
                        "--admitted-plan",
                        str(self.base / "plan.json"),
                        "--output-dir",
                        str(self.output),
                        "--image-digest",
                        IMAGE,
                    ]
                ),
                2,
            )
        with self.assertRaises(SystemExit):
            main(
                [
                    "--admitted-plan",
                    str(self.base / "plan.json"),
                    "--output-dir",
                    str(self.output),
                    "--image-digest",
                    IMAGE,
                    "--gf",
                    str(self.executables.gf),
                ]
            )

    def test_each_provider_rung_has_one_closed_offline_plan(self) -> None:
        for scale in (20, 22, 24, 25, 26):
            with (
                self.subTest(scale=scale),
                patch(
                    "graphforge_bench.progressive_provider_run._benchexec_version",
                    return_value="3.30",
                ),
            ):
                plan = build_execution_plan(
                    root=ROOT,
                    admitted_plan=admitted_plan(scale),
                    admitted_plan_sha256="a" * 64,
                    image_digest=IMAGE,
                    executables=self.executables,
                    source_tree_sha256="b" * 64,
                )
                self.assertEqual(plan["rung"], f"S{scale}")
                self.assertEqual(plan["identities"]["profile_id"], f"graph500-s{scale}-provider")

    def test_tampered_plan_and_wrong_rung_are_refused(self) -> None:
        tampered = admitted_plan()
        tampered["profile_sha256"] = "sha256:" + "0" * 64
        with self.assertRaisesRegex(ProviderRunError, "identity or projection"):
            validate_admitted_plan(ROOT, tampered)
        wrong = admitted_plan()
        wrong["next_rung"] = "S18"
        wrong["profile_id"] = "graph500-s18-local"
        wrong["profile_path"] = "profiles/graph500/s18-local.json"
        with self.assertRaisesRegex(ProviderRunError, "provider rung|validation failed"):
            validate_admitted_plan(ROOT, wrong)

        for scale in (22, 24, 25, 26):
            expected = admitted_plan(scale)
            for completed in (
                expected["completed_scales"][:-1],
                list(reversed(expected["completed_scales"])),
                [*expected["completed_scales"], scale],
            ):
                with self.subTest(scale=scale, completed=completed):
                    mutated = {**expected, "completed_scales": completed}
                    with self.assertRaisesRegex(ProviderRunError, "identity or projection"):
                        validate_admitted_plan(ROOT, mutated)

    def test_admitted_image_must_match_the_running_image(self) -> None:
        with (
            patch(
                "graphforge_bench.progressive_provider_run._benchexec_version",
                return_value="3.30",
            ),
            self.assertRaisesRegex(ProviderRunError, "does not match"),
        ):
            build_execution_plan(
                root=ROOT,
                admitted_plan=admitted_plan(),
                admitted_plan_sha256="a" * 64,
                image_digest="registry.fly.io/graphforge-bench@sha256:" + "2" * 64,
                executables=self.executables,
                source_tree_sha256="b" * 64,
            )

    def test_execution_requires_explicit_plan_authority(self) -> None:
        refused = admitted_plan()
        refused["execution_authorized"] = False
        refused["execution_refusal"] = "provider_executor_unavailable"
        with self.assertRaisesRegex(ProviderRunError, "identity or projection"):
            validate_admitted_plan(ROOT, refused)

    def test_identity_drift_refuses_before_authority_or_execution(self) -> None:
        plan = self.execution_plan()
        self.executables.gf.write_bytes(b"changed after planning")
        execute = Mock(return_value=0)
        authority = Mock(return_value={"result": "passed"})
        with (
            patch(
                "graphforge_bench.progressive_provider_run._benchexec_version",
                return_value="3.30",
            ),
            self.assertRaisesRegex(ProviderRunError, "changed after planning"),
        ):
            run(
                root=ROOT,
                output_dir=self.output,
                plan=plan,
                executables=self.executables,
                expected_image_digest=IMAGE,
                expected_admitted_plan_sha256="a" * 64,
                expected_source_tree_sha256="b" * 64,
                execution_boundary=execute,
                authority_boundary=authority,
            )
        authority.assert_not_called()
        execute.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_admission_identity_drift_refuses_before_authority(self) -> None:
        for identity, value in (
            ("image_digest", "registry.fly.io/graphforge-bench@sha256:" + "2" * 64),
            ("admitted_plan_sha256", "c" * 64),
            ("source_tree_sha256", "d" * 64),
        ):
            with self.subTest(identity=identity):
                plan = self.execution_plan()
                plan["identities"][identity] = value
                authority = Mock(return_value={"result": "passed"})
                with (
                    patch(
                        "graphforge_bench.progressive_provider_run._benchexec_version",
                        return_value="3.30",
                    ),
                    self.assertRaisesRegex(ProviderRunError, "changed after planning"),
                ):
                    run(
                        root=ROOT,
                        output_dir=self.output,
                        plan=plan,
                        executables=self.executables,
                        expected_image_digest=IMAGE,
                        expected_admitted_plan_sha256="a" * 64,
                        expected_source_tree_sha256="b" * 64,
                        execution_boundary=Mock(return_value=0),
                        authority_boundary=authority,
                    )
                authority.assert_not_called()

    def test_boundary_exceptions_emit_closed_failures(self) -> None:
        cases = (
            ("authority", lambda: (_ for _ in ()).throw(OSError("fixture"))),
            ("execution", lambda: {"result": "passed"}),
        )
        for boundary, authority in cases:
            with self.subTest(boundary=boundary):
                plan = self.execution_plan()
                execution = (
                    (lambda *_: (_ for _ in ()).throw(OSError("fixture")))
                    if boundary == "execution"
                    else Mock(return_value=0)
                )
                with (
                    patch(
                        "graphforge_bench.progressive_provider_run._benchexec_version",
                        return_value="3.30",
                    ),
                    self.assertRaises(ProviderRunError),
                ):
                    run(
                        root=ROOT,
                        output_dir=self.output,
                        plan=plan,
                        executables=self.executables,
                        expected_image_digest=IMAGE,
                        expected_admitted_plan_sha256="a" * 64,
                        expected_source_tree_sha256="b" * 64,
                        execution_boundary=execution,
                        authority_boundary=authority,
                    )
                result = json.loads((self.output / "s20-result.json").read_text())
                self.assertIn(
                    result["failure"],
                    {"native_authority_unavailable", "execution_boundary_failed"},
                )
                _schema(ROOT, "progressive-provider-run-result.json", result)
                for path in self.output.iterdir():
                    if path.is_file():
                        path.unlink()

    def test_no_native_authority_refuses_without_execution(self) -> None:
        plan = self.execution_plan()
        execute = Mock(return_value=0)
        with (
            patch(
                "graphforge_bench.progressive_provider_run._benchexec_version",
                return_value="3.30",
            ),
            self.assertRaisesRegex(ProviderRunError, "authority is unavailable"),
        ):
            run(
                root=ROOT,
                output_dir=self.output,
                plan=plan,
                executables=self.executables,
                expected_image_digest=IMAGE,
                expected_admitted_plan_sha256="a" * 64,
                expected_source_tree_sha256="b" * 64,
                execution_boundary=execute,
                authority_boundary=lambda: {"result": "failed"},
            )
        execute.assert_not_called()
        result = json.loads((self.output / "s20-result.json").read_text())
        self.assertEqual(result["failure"], "native_authority_unavailable")

    def test_benchexec_failure_emits_closed_failed_result(self) -> None:
        plan = self.execution_plan()
        with (
            patch(
                "graphforge_bench.progressive_provider_run._benchexec_version",
                return_value="3.30",
            ),
            self.assertRaisesRegex(ProviderRunError, "benchexec_failed"),
        ):
            run(
                root=ROOT,
                output_dir=self.output,
                plan=plan,
                executables=self.executables,
                expected_image_digest=IMAGE,
                expected_admitted_plan_sha256="a" * 64,
                expected_source_tree_sha256="b" * 64,
                execution_boundary=lambda *_: 1,
                authority_boundary=lambda: {"result": "passed"},
            )
        result = json.loads((self.output / "s20-result.json").read_text())
        self.assertEqual(result["failure"], "benchexec_failed")
        _schema(ROOT, "progressive-provider-run-result.json", result)

    def test_missing_receipts_fail_closed(self) -> None:
        plan = self.execution_plan()
        with (
            patch(
                "graphforge_bench.progressive_provider_run._benchexec_version",
                return_value="3.30",
            ),
            patch(
                "graphforge_bench.progressive_provider_run.ingest_benchexec_result",
                side_effect=ControllerError("missing"),
            ),
            self.assertRaisesRegex(ProviderRunError, "ordinary_receipt_missing"),
        ):
            run(
                root=ROOT,
                output_dir=self.output,
                plan=plan,
                executables=self.executables,
                expected_image_digest=IMAGE,
                expected_admitted_plan_sha256="a" * 64,
                expected_source_tree_sha256="b" * 64,
                execution_boundary=lambda *_: 0,
                authority_boundary=lambda: {"result": "passed"},
            )
        result = json.loads((self.output / "s20-result.json").read_text())
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["failure"], "ordinary_receipt_missing")

    def test_success_emits_exactly_five_canonical_engineering_files(self) -> None:
        plan = self.execution_plan()
        self.output.mkdir()
        (self.output / "s20-plan.json").write_text(json.dumps(plan), encoding="utf-8")
        graphforge = graphforge_fixture(20)
        graphforge["profile_id"] = "graph500-s20-provider"
        benchexec = benchexec_fixture(graphforge)
        rung = rung_fixture(20)
        rung["profile_id"] = "graph500-s20-provider"
        rung["source"] = "canonical_ladder"
        with (
            patch(
                "graphforge_bench.progressive_provider_run._benchexec_version",
                return_value="3.30",
            ),
            patch(
                "graphforge_bench.progressive_provider_run.ingest_benchexec_result",
                return_value=(benchexec, graphforge, rung),
            ) as ingest,
        ):
            run(
                root=ROOT,
                output_dir=self.output,
                plan=plan,
                executables=self.executables,
                expected_image_digest=IMAGE,
                expected_admitted_plan_sha256="a" * 64,
                expected_source_tree_sha256="b" * 64,
                execution_boundary=lambda *_: 0,
                authority_boundary=lambda: {"result": "passed"},
            )
        self.assertEqual(
            {path.name for path in self.output.iterdir()},
            {
                "s20-plan.json",
                "s20-benchexec.json",
                "s20-graphforge.json",
                "s20-rung.json",
                "s20-result.json",
            },
        )
        self.assertEqual(
            ingest.call_args.kwargs,
            {
                "root": ROOT,
                "stage": ingest.call_args.kwargs["stage"],
                "scale": 20,
                "plan": plan,
                "profile_id": "graph500-s20-provider",
                "source": "canonical_ladder",
            },
        )
        result = json.loads((self.output / "s20-result.json").read_text())
        self.assertEqual(result["claim"], "engineering_evidence_only")

    def test_every_provider_rung_writes_schema_valid_hash_bound_result(self) -> None:
        for scale in (20, 22, 24, 25, 26):
            with self.subTest(scale=scale):
                output = self.base / f"s{scale}"
                output.mkdir()
                with patch(
                    "graphforge_bench.progressive_provider_run._benchexec_version",
                    return_value="3.30",
                ):
                    plan = build_execution_plan(
                        root=ROOT,
                        admitted_plan=admitted_plan(scale),
                        admitted_plan_sha256="a" * 64,
                        image_digest=IMAGE,
                        executables=self.executables,
                        source_tree_sha256="b" * 64,
                    )
                (output / f"s{scale}-plan.json").write_text(json.dumps(plan), encoding="utf-8")
                graphforge = graphforge_fixture(scale)
                graphforge["profile_id"] = f"graph500-s{scale}-provider"
                benchexec = benchexec_fixture(graphforge)
                rung = rung_fixture(scale)
                rung["profile_id"] = f"graph500-s{scale}-provider"
                rung["source"] = "canonical_ladder"
                with (
                    patch(
                        "graphforge_bench.progressive_provider_run._benchexec_version",
                        return_value="3.30",
                    ),
                    patch(
                        "graphforge_bench.progressive_provider_run.ingest_benchexec_result",
                        return_value=(benchexec, graphforge, rung),
                    ),
                ):
                    run(
                        root=ROOT,
                        output_dir=output,
                        plan=plan,
                        executables=self.executables,
                        expected_image_digest=IMAGE,
                        expected_admitted_plan_sha256="a" * 64,
                        expected_source_tree_sha256="b" * 64,
                        execution_boundary=lambda *_: 0,
                        authority_boundary=lambda: {"result": "passed"},
                    )
                result = json.loads((output / f"s{scale}-result.json").read_text())
                _schema(ROOT, "progressive-provider-run-result.json", result)
                for artifact, filename in (
                    ("plan_sha256", f"s{scale}-plan.json"),
                    ("benchexec_sha256", f"s{scale}-benchexec.json"),
                    ("graphforge_sha256", f"s{scale}-graphforge.json"),
                    ("rung_sha256", f"s{scale}-rung.json"),
                ):
                    self.assertEqual(
                        result["artifacts"][artifact],
                        hashlib.sha256((output / filename).read_bytes()).hexdigest(),
                    )


if __name__ == "__main__":
    unittest.main()
