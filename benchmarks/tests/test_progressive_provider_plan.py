from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from graphforge_bench.progressive_provider_plan import (
    ProviderPlanError,
    completed_rungs,
    plan_provider_ladder,
    require_execution_authority,
)

ROOT = Path(__file__).resolve().parents[1]
COMMIT = subprocess.run(
    ["git", "-C", str(ROOT.parent), "rev-parse", "HEAD"],
    capture_output=True,
    check=True,
    text=True,
).stdout.strip()


def rung(scale: int, *, source: str | None = None) -> dict:
    return {
        "profile_id": f"graph500-s{scale}-{'local' if scale in (18, 19) else 'provider'}",
        "source": source or ("progressive_profile" if scale in (18, 19) else "canonical_ladder"),
        "scale": scale,
        "live_edges": 16 * (1 << scale),
        "status": "passed",
        "correctness": True,
        "phases": [
            "admission",
            "generate",
            "ingest",
            "reopen",
            "recount",
            "query",
            "export",
            "verify",
            "clean_import",
            "reopen_proof",
        ],
        "metrics": {
            "wall_seconds": 10,
            "peak_rss_bytes": 100,
            "retained_storage_bytes": 200,
            "transient_peak_storage_bytes": 300,
            "logical_read_bytes": 400,
            "logical_write_bytes": 500,
            "physical_read_bytes": 600,
            "physical_write_bytes": 700,
            "reader_calls": 8,
            "publication_work_units": 9,
        },
        "metric_sources": {
            "benchexec": [
                "wall_seconds",
                "peak_rss_bytes",
                "physical_read_bytes",
                "physical_write_bytes",
            ],
            "storage_attribution": [
                "retained_storage_bytes",
                "transient_peak_storage_bytes",
                "logical_read_bytes",
                "logical_write_bytes",
                "reader_calls",
                "publication_work_units",
            ],
            "query_qualification": ["live_edges", "correctness"],
        },
        "storage_components": {
            "source_allocated_physical_bytes": 100,
            "source_retained_logical_eof_bytes": 110,
            "imported_allocated_physical_bytes": 120,
            "imported_retained_logical_eof_bytes": 130,
            "transient_peak_allocated_bytes": 300,
            "logical_read_bytes": 400,
            "logical_write_bytes": 500,
            "reader_calls": 8,
            "publication_work_units": 9,
        },
        "failure": None,
    }


def result(scale: int) -> dict:
    profile = ROOT / "profiles" / "graph500" / f"s{scale}-local.json"
    digest = hashlib.sha256(profile.read_bytes()).hexdigest()
    return {
        "schema": "graphforge-progressive-run-result/1",
        "rung": f"S{scale}",
        "status": "passed",
        "failure": None,
        "identities": {
            "commit": COMMIT,
            "profile_id": f"graph500-s{scale}-local",
            "profile_sha256": digest,
            "generator": "sha256:" + "0" * 64,
            "generator_executable_sha256": "0" * 64,
            "gf_sha256": "0" * 64,
            "certify_sha256": "0" * 64,
            "benchexec_python_sha256": "0" * 64,
            "benchexec_version": "1.0",
        },
        "claim": "engineering_evidence_only",
    }


CAPACITY = {
    "physical_read_bytes_per_second": 100,
    "physical_write_bytes_per_second": 100,
    "reader_calls_per_second": 100,
    "publication_work_per_second": 100,
}
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
IMAGE = "registry.fly.io/graphforge-bench@sha256:" + "1" * 64


class ProgressiveProviderPlanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.output = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_empty_workspace_admits_only_s18(self) -> None:
        plan = plan_provider_ladder(
            root=ROOT, output_dir=self.output, commit=COMMIT, maximum_scale=26
        )
        self.assertEqual(plan["next_rung"], "S18")
        self.assertEqual(plan["execution"], "local")
        self.assertIsNone(plan["projection"])
        self.assertIsNone(plan["image_digest"])
        self.assertEqual(plan["profile_path"], "profiles/graph500/s18-local.json")
        self.assertNotIn("app", json.dumps(plan))
        self.assertNotIn("machine", json.dumps(plan))
        self.assertNotIn("volume", json.dumps(plan))

    def test_plan_requires_checked_out_commit(self) -> None:
        with self.assertRaisesRegex(ProviderPlanError, "checked-out repository commit"):
            plan_provider_ladder(
                root=ROOT,
                output_dir=self.output,
                commit="a" * 40,
                maximum_scale=26,
            )

    def test_evidence_must_be_a_contiguous_passed_prefix(self) -> None:
        (self.output / "s19-rung.json").write_text(json.dumps(rung(19)), encoding="utf-8")
        with self.assertRaisesRegex(ProviderPlanError, "out of order"):
            completed_rungs(ROOT, self.output)
        (self.output / "s19-rung.json").unlink()
        bad = rung(18)
        bad["status"] = "failed"
        (self.output / "s18-rung.json").write_text(json.dumps(bad), encoding="utf-8")
        with self.assertRaisesRegex(ProviderPlanError, "schema-valid"):
            completed_rungs(ROOT, self.output)

        (self.output / "s18-rung.json").unlink()
        for scale in (18, 19, 20):
            (self.output / f"s{scale}-rung.json").write_text(
                json.dumps(rung(scale, source="progressive_profile")), encoding="utf-8"
            )
        with self.assertRaisesRegex(ProviderPlanError, "canonical profile"):
            completed_rungs(ROOT, self.output)

    def test_projection_gate_is_required_before_s20(self) -> None:
        with (
            patch(
                "graphforge_bench.progressive_provider_plan.completed_rungs",
                return_value=[rung(18), rung(19)],
            ),
            self.assertRaisesRegex(ProviderPlanError, "not admitted"),
        ):
            plan_provider_ladder(root=ROOT, output_dir=self.output, commit=COMMIT, maximum_scale=20)

    def test_provider_plan_contains_only_sanitized_projection(self) -> None:
        with (
            patch(
                "graphforge_bench.progressive_provider_plan.completed_rungs",
                return_value=[rung(18), rung(19)],
            ),
            patch(
                "graphforge_bench.progressive_provider_plan.project",
                return_value={
                    "schema": "graphforge-progressive-qualification-evidence/1",
                    "target": "S20",
                    "source_scales": [18, 19],
                    "decision": "admitted",
                    "limits": {
                        "wall_seconds": 14400,
                        "rss_bytes": 4294967296,
                        "volume_bytes": 536870912000,
                    },
                    "headroom": {
                        "time_fraction": 0.2,
                        "rss_fraction": 0.2,
                        "storage_fraction": 0.15,
                    },
                    "projected": dict.fromkeys(PROJECTED_FIELDS, 1),
                    "required_rates": dict.fromkeys(RATE_FIELDS, 1),
                    "provider_capacity": CAPACITY,
                    "slopes_observed": dict.fromkeys(SLOPE_FIELDS, 1),
                    "rss_growth_fraction": 0,
                    "checks": dict.fromkeys(CHECK_FIELDS, True),
                    "claim": "engineering_evidence_only",
                },
            ),
        ):
            plan = plan_provider_ladder(
                root=ROOT,
                output_dir=self.output,
                commit=COMMIT,
                maximum_scale=20,
                provider_capacity=CAPACITY,
                image_digest=IMAGE,
            )
        self.assertEqual(plan["next_rung"], "S20")
        self.assertEqual(plan["profile_id"], "graph500-s20-provider")
        self.assertEqual(plan["image_digest"], IMAGE)
        self.assertEqual(plan["projection"]["decision"], "admitted")
        encoded = json.dumps(plan).lower()
        for forbidden in ("machine_id", "volume_id", "token", "secret", "provider_id"):
            self.assertNotIn(forbidden, encoded)

    def test_max_scale_cannot_skip_the_next_rung(self) -> None:
        with (
            patch(
                "graphforge_bench.progressive_provider_plan.completed_rungs",
                return_value=[rung(18), rung(19)],
            ),
            self.assertRaisesRegex(ProviderPlanError, "not admitted"),
        ):
            plan_provider_ladder(
                root=ROOT,
                output_dir=self.output,
                commit=COMMIT,
                maximum_scale=19,
                provider_capacity=CAPACITY,
            )

    def test_plan_requires_exact_commit_bound_result_files(self) -> None:
        for scale in (18, 19):
            value = rung(scale)
            (self.output / f"s{scale}-rung.json").write_text(json.dumps(value), encoding="utf-8")
            result_doc = result(scale)
            (self.output / f"s{scale}-result.json").write_text(
                json.dumps(result_doc), encoding="utf-8"
            )
        result_doc = json.loads((self.output / "s19-result.json").read_text(encoding="utf-8"))
        result_doc["identities"]["commit"] = "b" * 40
        (self.output / "s19-result.json").write_text(json.dumps(result_doc), encoding="utf-8")
        with self.assertRaisesRegex(ProviderPlanError, "commit/profile"):
            plan_provider_ladder(
                root=ROOT,
                output_dir=self.output,
                commit=COMMIT,
                maximum_scale=20,
                provider_capacity=CAPACITY,
            )

    def test_minimal_result_document_is_rejected(self) -> None:
        for scale in (18, 19):
            value = rung(scale)
            (self.output / f"s{scale}-rung.json").write_text(json.dumps(value), encoding="utf-8")
            (self.output / f"s{scale}-result.json").write_text(
                json.dumps(
                    {
                        "rung": f"S{scale}",
                        "status": "passed",
                        "identities": {"commit": COMMIT, "profile_id": value["profile_id"]},
                    }
                ),
                encoding="utf-8",
            )
        with self.assertRaisesRegex(ProviderPlanError, "result is not schema-valid"):
            plan_provider_ladder(
                root=ROOT,
                output_dir=self.output,
                commit=COMMIT,
                maximum_scale=20,
                provider_capacity=CAPACITY,
            )

    def test_provider_result_hashes_bind_every_projection_artifact(self) -> None:
        for scale in (18, 19):
            (self.output / f"s{scale}-rung.json").write_text(
                json.dumps(rung(scale)), encoding="utf-8"
            )
            (self.output / f"s{scale}-result.json").write_text(
                json.dumps(result(scale)), encoding="utf-8"
            )
        scale = 20
        artifact_paths = {
            "plan_sha256": self.output / "s20-plan.json",
            "benchexec_sha256": self.output / "s20-benchexec.json",
            "graphforge_sha256": self.output / "s20-graphforge.json",
            "rung_sha256": self.output / "s20-rung.json",
        }
        profile = ROOT / "profiles/graph500/s20-provider.json"
        identities = {
            "commit": COMMIT,
            "profile_id": "graph500-s20-provider",
            "profile_sha256": hashlib.sha256(profile.read_bytes()).hexdigest(),
            "image_digest": IMAGE,
            "generator": "sha256:" + "0" * 64,
            "generator_executable_sha256": "0" * 64,
            "gf_sha256": "0" * 64,
            "certify_sha256": "0" * 64,
            "benchexec_python_sha256": "0" * 64,
            "benchexec_version": "1.0",
            "admitted_plan_sha256": "0" * 64,
            "source_tree_sha256": "0" * 64,
        }
        execution_plan = {
            "schema": "graphforge-progressive-provider-execution-plan/1",
            "rung": "S20",
            "execution": "provider_native_linux_benchexec",
            "identities": identities,
            "limits": {"wall_seconds": 14400, "memory_bytes": 4294967296, "cores": 16},
            "outputs": [
                "s20-plan.json",
                "s20-benchexec.json",
                "s20-graphforge.json",
                "s20-rung.json",
                "s20-result.json",
            ],
            "claim": "engineering_evidence_only",
        }
        for name, path in artifact_paths.items():
            if name == "plan_sha256":
                document = execution_plan
            elif name == "rung_sha256":
                document = rung(scale)
            else:
                document = {"fixture": name}
            path.write_text(json.dumps(document), encoding="utf-8")
        provider_result = {
            "schema": "graphforge-progressive-provider-run-result/1",
            "rung": "S20",
            "status": "passed",
            "failure": None,
            "identities": identities,
            "artifacts": {
                name: hashlib.sha256(path.read_bytes()).hexdigest()
                for name, path in artifact_paths.items()
            },
            "claim": "engineering_evidence_only",
        }
        (self.output / "s20-result.json").write_text(json.dumps(provider_result), encoding="utf-8")
        self.assertEqual(len(completed_rungs(ROOT, self.output, commit=COMMIT)), 3)

        changed_result = json.loads(json.dumps(provider_result))
        changed_result["identities"]["gf_sha256"] = "1" * 64
        (self.output / "s20-result.json").write_text(json.dumps(changed_result), encoding="utf-8")
        with self.assertRaisesRegex(ProviderPlanError, "plan/result identities disagree"):
            completed_rungs(ROOT, self.output, commit=COMMIT)
        (self.output / "s20-result.json").write_text(json.dumps(provider_result), encoding="utf-8")

        with (self.output / "s20-benchexec.json").open("a", encoding="utf-8") as stream:
            stream.write("\n")
        with self.assertRaisesRegex(ProviderPlanError, "artifacts do not match"):
            completed_rungs(ROOT, self.output, commit=COMMIT)

    def test_provider_plan_has_offline_execution_authority(self) -> None:
        with patch(
            "graphforge_bench.progressive_provider_plan.completed_rungs",
            return_value=[rung(18), rung(19)],
        ):
            plan = plan_provider_ladder(
                root=ROOT,
                output_dir=self.output,
                commit=COMMIT,
                maximum_scale=20,
                provider_capacity=CAPACITY,
                image_digest=IMAGE,
            )
        require_execution_authority(plan)
        refused = {
            **plan,
            "execution_authorized": False,
            "execution_refusal": "provider_executor_unavailable",
        }
        with self.assertRaisesRegex(ProviderPlanError, "authority is unavailable"):
            require_execution_authority(refused)

    def test_make_target_only_forwards_an_available_image_digest(self) -> None:
        variables = [
            f"COMMIT={COMMIT}",
            "MAXIMUM_SCALE=19",
            f"OUTPUT_DIR={self.output}",
            f"PLAN_OUT={self.output / 'plan.json'}",
        ]
        without_image = subprocess.run(
            ["make", "-n", "progressive-provider-plan", *variables],
            cwd=ROOT,
            capture_output=True,
            check=True,
            text=True,
        ).stdout
        self.assertNotIn("--image-digest", without_image)

        with_image = subprocess.run(
            ["make", "-n", "progressive-provider-plan", *variables, f"IMAGE_DIGEST={IMAGE}"],
            cwd=ROOT,
            capture_output=True,
            check=True,
            text=True,
        ).stdout
        self.assertIn(f'--image-digest "{IMAGE}"', with_image)

    def test_provider_plan_requires_one_immutable_admitted_image(self) -> None:
        with (
            patch(
                "graphforge_bench.progressive_provider_plan.completed_rungs",
                return_value=[rung(18), rung(19)],
            ),
            self.assertRaisesRegex(ProviderPlanError, "immutable provider image digest"),
        ):
            plan_provider_ladder(
                root=ROOT,
                output_dir=self.output,
                commit=COMMIT,
                maximum_scale=20,
                provider_capacity=CAPACITY,
            )


if __name__ == "__main__":
    unittest.main()
