from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import unittest

from graphforge_bench.progressive_qualification import (
    PHASES,
    Profile,
    QualificationError,
    load_profiles,
    project,
    select_next,
)
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
CAPACITY = {
    "physical_read_bytes_per_second": 10**9,
    "physical_write_bytes_per_second": 10**9,
    "reader_calls_per_second": 10**6,
    "publication_work_per_second": 10**6,
}


def storage_attribution(scale: int, multiplier: int) -> dict:
    category_names = (
        "topology_nodes",
        "topology_edges",
        "properties",
        "uuid_and_surrogates",
        "adjacency",
        "catalog_and_manifests",
        "construction_staging",
        "portable_package",
        "clean_imported_project",
        "other",
    )
    empty = {
        "logical_references": 0,
        "logical_bytes": 0,
        "physical_objects": 0,
        "physical_logical_bytes": 0,
        "allocated_bytes": 0,
    }

    def snapshot(node_bytes: int, edge_bytes: int) -> dict:
        categories = {name: dict(empty) for name in category_names}
        categories["topology_nodes"] = {
            "logical_references": 1,
            "logical_bytes": node_bytes,
            "physical_objects": 1,
            "physical_logical_bytes": node_bytes,
            "allocated_bytes": node_bytes,
        }
        categories["topology_edges"] = {
            "logical_references": 1,
            "logical_bytes": edge_bytes,
            "physical_objects": 1,
            "physical_logical_bytes": edge_bytes,
            "allocated_bytes": edge_bytes,
        }
        total = node_bytes + edge_bytes
        return {
            "contract": "graphforge-storage-attribution/1",
            "categories": categories,
            "logical_references": 2,
            "logical_bytes": total,
            "retained_logical_eof_bytes": total,
            "allocated_physical_bytes": total,
            "physical_objects": 2,
        }

    io_fields = (
        "read_bytes",
        "write_bytes",
        "read_calls",
        "write_calls",
        "object_count",
        "block_count",
        "fsync_calls",
    )
    phase_names = (
        "append_merge",
        "seal_authentication",
        "shape_consume_reauthentication",
        "encode_write_postwrite_authentication",
        "publication_preauthentication",
        "cas_install_read_write",
        "hydration_verification",
        "fsync_synchronization",
        "recovery_reauthentication",
    )
    phases = {name: dict.fromkeys(io_fields, 0) for name in phase_names}
    phases["append_merge"].update(
        read_bytes=2_000_000 * multiplier,
        write_bytes=3_000_000 * multiplier,
        read_calls=1_000 * multiplier,
        write_calls=500 * multiplier,
    )
    return {
        "source": snapshot(150_000 * multiplier, 250_000 * multiplier),
        "imported": snapshot(150_000 * multiplier, 250_000 * multiplier),
        "construction": {
            "application_io": {
                "phases": phases,
                "totals": {
                    field: sum(phase[field] for phase in phases.values()) for field in io_fields
                },
            },
            "transient_peak_allocated_bytes": 1_500_000 * multiplier,
        },
        "portable_package": {
            "contract": "graphforge-portable-export/2",
            "allocation_logical_bytes": 300_000 * multiplier,
            "allocation_allocated_bytes": 300_000 * multiplier,
            "allocation_physical_objects": 1,
        },
        "lifecycle": {
            "contract": "graphforge-lifecycle-storage/1",
            "source_project_current_allocated_bytes": 450_000 * multiplier,
            "retained_storage_bytes": 1_000_000 * multiplier,
            "transient_peak_storage_bytes": 1_500_000 * multiplier,
        },
        "counts": {
            "source_nodes": 1 << scale,
            "source_edges": 16 * (1 << scale),
            "imported_nodes": 1 << scale,
            "imported_edges": 16 * (1 << scale),
        },
    }


def rung(scale: int, *, rss: int = 1_000_000_000, wall: int = 100) -> dict:
    multiplier = 1 << max(0, scale - 18)
    return {
        "assembly_contract": "graphforge-progressive-rung-assembly/2",
        "profile_id": f"graph500-s{scale}-evidence",
        "source": "canonical_ladder" if scale in {24, 25} else "progressive_profile",
        "scale": scale,
        "live_edges": (1 << scale) * 16,
        "status": "passed",
        "correctness": True,
        "phases": list(PHASES),
        "metrics": {
            "wall_seconds": wall,
            "peak_rss_bytes": rss,
            "retained_storage_bytes": 1_000_000 * multiplier,
            "transient_peak_storage_bytes": 1_500_000 * multiplier,
            "logical_read_bytes": 2_000_000 * multiplier,
            "logical_write_bytes": 3_000_000 * multiplier,
            "physical_read_bytes": 1_000_000 * multiplier,
            "physical_write_bytes": 1_500_000 * multiplier,
            "reader_calls": 1_000 * multiplier,
            "publication_work_units": 2_000 * multiplier,
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
            "source_project_current_allocated_bytes": 450_000 * multiplier,
            "source_allocated_physical_bytes": 400_000 * multiplier,
            "source_retained_logical_eof_bytes": 500_000 * multiplier,
            "imported_allocated_physical_bytes": 400_000 * multiplier,
            "imported_retained_logical_eof_bytes": 500_000 * multiplier,
            "transient_peak_allocated_bytes": 1_500_000 * multiplier,
            "logical_read_bytes": 2_000_000 * multiplier,
            "logical_write_bytes": 3_000_000 * multiplier,
            "reader_calls": 1_000 * multiplier,
            "publication_work_units": 2_000 * multiplier,
        },
        "storage_attribution": storage_attribution(scale, multiplier),
        "failure": None,
    }


class ProgressiveQualificationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.profiles = load_profiles()
        cls.profile_schema = Draft202012Validator(
            json.loads((ROOT / "schemas/progressive-qualification-profile.json").read_text())
        )
        cls.evidence_schema = Draft202012Validator(
            json.loads((ROOT / "schemas/progressive-qualification-evidence.json").read_text())
        )
        cls.rung_schema = Draft202012Validator(
            json.loads((ROOT / "schemas/progressive-qualification-rung-evidence.json").read_text())
        )
        cls.certification_schema = Draft202012Validator(
            json.loads((ROOT / "schemas/certification-profile.json").read_text())
        )

    def test_distinct_profiles_pin_one_ordinary_lifecycle_without_provider_ids(self) -> None:
        raw_profiles = []
        for path in sorted((ROOT / "profiles/graph500").glob("*.json")):
            raw = json.loads(path.read_text())
            self.profile_schema.validate(raw)
            raw_profiles.append(raw)
            self.assertEqual(tuple(raw["lifecycle"]["phases"]), PHASES)
            self.assertEqual(raw["generator"]["edge_factor"], 16)
            digest = (
                "sha256:"
                + hashlib.sha256(
                    (ROOT / "runners/graph500-generator/src/main.rs").read_bytes()
                ).hexdigest()
            )
            self.assertEqual(raw["generator"]["identity"], digest)
            self.assertEqual(raw["phases"][1]["action"]["identity"], digest)
            for phase in raw["phases"]:
                action = phase["action"]
                if action["interface"] == "graph_forge_cli_workflow":
                    self.assertTrue(all(command[0] == "--json" for command in action["commands"]))
                elif action["interface"] == "graph_forge_cli":
                    if phase["phase"] == "admission":
                        self.assertEqual(action["args"], ["--info"])
                    else:
                        self.assertEqual(action["args"][0], "--json")
            ingest = raw["phases"][2]["action"]
            self.assertEqual(ingest["interface"], "graph_forge_cli_workflow")
            self.assertEqual(
                [command[command.index("import-session") + 1] for command in ingest["commands"]],
                ["begin", "register-parquet", "register-parquet", "validate", "commit"],
            )
            reopen = raw["phases"][3]["action"]["commands"]
            self.assertEqual(
                [command[-1] for command in reopen], ["recovery", "storage-attribution"]
            )
            reopen_proof = raw["phases"][-1]["action"]["commands"]
            self.assertEqual(len(reopen_proof), 5)
            self.assertTrue(all("query" in command for command in reopen_proof[:4]))
            self.assertEqual(reopen_proof[-1][-1], "storage-attribution")
            profile_uuids = {
                argument
                for phase in raw["phases"]
                for command in (
                    phase["action"].get("commands") or [phase["action"].get("args", [])]
                )
                for argument in command
                if argument.startswith("00000000-0000-4000-8000-")
            }
            self.assertEqual(
                profile_uuids,
                {f"00000000-0000-4000-8000-{raw['scale']:012d}"},
            )
            encoded = json.dumps(raw).lower()
            for forbidden in ("provider_id", "machine_id", "volume_id", "token", "secret"):
                self.assertNotIn(forbidden, encoded)
        self.assertEqual([item.scale for item in self.profiles], [18, 19, 20, 22, 24, 25, 26])
        self.assertEqual(
            [item.execution for item in self.profiles],
            ["local", "local", "provider", "provider", "provider", "provider", "provider"],
        )

    def test_selection_is_progressive_and_stops_after_first_typed_failure(self) -> None:
        self.assertEqual(select_next(self.profiles, []).scale, 18)
        self.assertEqual(select_next(self.profiles, [rung(18)]).scale, 19)
        self.assertEqual(select_next(self.profiles, [rung(18), rung(19)], CAPACITY).scale, 20)
        completed = [rung(scale) for scale in (18, 19, 20, 22, 24)]
        self.assertEqual(select_next(self.profiles, completed, CAPACITY).scale, 25)
        self.assertEqual(select_next(self.profiles, [*completed, rung(25)], CAPACITY).scale, 26)
        failed = rung(19) | {"status": "failed", "correctness": False, "failure": "correctness"}
        failed["phases"] = list(PHASES[:6])
        self.rung_schema.validate(failed)
        self.assertIsNone(select_next(self.profiles, [rung(18), failed]))

    def test_tiny_executable_fixture_is_a_real_certification_profile(self) -> None:
        fixture = json.loads((ROOT / "fixtures/progressive/tiny-executable.json").read_text())
        self.certification_schema.validate(fixture)
        self.assertEqual([phase["phase"] for phase in fixture["phases"]], list(PHASES))

    def test_failed_rung_may_preserve_partial_observations(self) -> None:
        failed = rung(18) | {
            "status": "failed",
            "correctness": False,
            "phases": list(PHASES[:3]),
            "metrics": {"wall_seconds": 5, "peak_rss_bytes": 42},
            "failure": "ingest",
        }
        self.rung_schema.validate(failed)

    def test_provider_selection_refuses_failed_projection(self) -> None:
        high = rung(19, wall=12_000)
        low = rung(18, wall=12_000)
        self.assertIsNone(select_next(self.profiles, [low, high], CAPACITY))
        self.assertIsNone(select_next(self.profiles, [rung(18), rung(19)]))

    def test_s20_requires_two_adjacent_completed_rungs(self) -> None:
        with self.assertRaisesRegex(QualificationError, "both declared"):
            project(self.profiles[2], [rung(18)])
        failed = rung(19) | {"status": "failed", "correctness": False, "failure": "correctness"}
        with self.assertRaisesRegex(QualificationError, "completed and correct"):
            project(self.profiles[2], [rung(18), failed])

    def test_plateaued_s20_projection_is_schema_valid_and_sanitized(self) -> None:
        evidence = project(
            self.profiles[2],
            [rung(18, rss=1_000_000_000, wall=100), rung(19, rss=1_050_000_000, wall=110)],
            CAPACITY,
        )
        self.evidence_schema.validate(evidence)
        self.rung_schema.validate(rung(18))
        self.rung_schema.validate(rung(19))
        self.assertEqual(evidence["decision"], "admitted")
        self.assertEqual(evidence["source_scales"], [18, 19])
        self.assertTrue(all(evidence["checks"].values()))
        self.assertEqual(evidence["claim"], "engineering_evidence_only")
        encoded = json.dumps(evidence).lower()
        for forbidden in ("provider_id", "machine_id", "volume_id", "path", "token", "secret"):
            self.assertNotIn(forbidden, encoded)
        contradictory = copy.deepcopy(evidence)
        contradictory["checks"]["rss_headroom"] = False
        self.assertFalse(self.evidence_schema.is_valid(contradictory))
        wrong_sources = copy.deepcopy(evidence)
        wrong_sources["source_scales"] = [24, 25]
        self.assertFalse(self.evidence_schema.is_valid(wrong_sources))
        missing_capacity = copy.deepcopy(evidence)
        missing_capacity["provider_capacity"] = None
        self.assertFalse(self.evidence_schema.is_valid(missing_capacity))

    def test_source_project_union_allocation_is_typed_but_additive_for_old_rungs(self) -> None:
        current = rung(18)
        self.rung_schema.validate(current)
        missing = copy.deepcopy(current)
        del missing["storage_components"]["source_project_current_allocated_bytes"]
        self.assertFalse(self.rung_schema.is_valid(missing))
        historical = copy.deepcopy(current)
        del historical["assembly_contract"]
        del historical["storage_components"]["source_project_current_allocated_bytes"]
        self.rung_schema.validate(historical)
        for malformed in (True, -1, "450000"):
            with self.subTest(malformed=malformed):
                invalid = copy.deepcopy(current)
                invalid["storage_components"]["source_project_current_allocated_bytes"] = malformed
                self.assertFalse(self.rung_schema.is_valid(invalid))

    def test_profile_schema_rejects_non_string_generator_identity(self) -> None:
        profile = json.loads((ROOT / "profiles/graph500/s18-local.json").read_text())
        profile["generator"]["identity"] = 42
        self.assertFalse(self.profile_schema.is_valid(profile))

    def test_each_capacity_dimension_refuses_independently(self) -> None:
        cases = {
            "time_headroom": ("wall_seconds", 12_000),
            "rss_headroom": ("peak_rss_bytes", 3_600_000_000),
            "retained_storage_headroom": ("retained_storage_bytes", 450 * 1024**3),
            "transient_storage_headroom": ("transient_peak_storage_bytes", 450 * 1024**3),
        }
        for check, (metric, high_value) in cases.items():
            low, high = rung(18), rung(19)
            if metric in {"wall_seconds", "peak_rss_bytes"}:
                low["metrics"][metric] = high_value
            else:
                low["metrics"][metric] = high_value // 2
            high["metrics"][metric] = high_value
            with self.subTest(check=check):
                evidence = project(self.profiles[2], [low, high], CAPACITY)
                self.assertEqual(evidence["decision"], "refused")
                self.assertFalse(evidence["checks"][check])

    def test_material_adjacent_rss_growth_is_architectural_refusal(self) -> None:
        evidence = project(
            self.profiles[2],
            [rung(18, rss=1_000_000_000), rung(19, rss=1_300_000_000)],
            CAPACITY,
        )
        self.assertFalse(evidence["checks"]["rss_bounded_or_plateaued"])
        self.assertEqual(evidence["decision"], "refused")

    def test_io_reader_and_publication_slopes_are_independently_preserved(self) -> None:
        evidence = project(self.profiles[2], [rung(18), rung(19)], CAPACITY)
        self.assertEqual(
            set(evidence["slopes_observed"]),
            {
                "logical_read_bytes",
                "logical_write_bytes",
                "physical_read_bytes",
                "physical_write_bytes",
                "reader_calls",
                "publication_work_units",
            },
        )
        self.assertTrue(all(value > 0 for value in evidence["slopes_observed"].values()))

    def test_provider_work_capacity_is_required_and_keeps_headroom(self) -> None:
        missing = project(self.profiles[2], [rung(18), rung(19)])
        self.assertEqual(missing["decision"], "refused")
        self.assertFalse(missing["checks"]["io_reader_publication_capacity_measured"])
        constrained = dict(CAPACITY)
        constrained["reader_calls_per_second"] = 0
        evidence = project(self.profiles[2], [rung(18), rung(19)], constrained)
        self.assertFalse(evidence["checks"]["io_reader_publication_headroom"])
        self.assertEqual(evidence["decision"], "refused")

    def test_provider_capacity_evidence_is_a_closed_rate_allowlist(self) -> None:
        capacity = CAPACITY | {"provider_id": "must-not-escape", "secret": "must-not-escape"}
        evidence = project(self.profiles[2], [rung(18), rung(19)], capacity)
        self.assertEqual(evidence["provider_capacity"], CAPACITY)
        self.assertNotIn("must-not-escape", json.dumps(evidence))

    def test_projection_uses_worse_latest_ratio_not_only_small_delta(self) -> None:
        low, high = rung(18), rung(19)
        low["metrics"]["retained_storage_bytes"] = 100_000_000_000
        high["metrics"]["retained_storage_bytes"] = 101_000_000_000
        evidence = project(self.profiles[2], [low, high], CAPACITY)
        self.assertEqual(evidence["projected"]["retained_storage_bytes"], 202_000_000_000)

    def test_every_provider_rung_keeps_its_declared_ladder_gate(self) -> None:
        self.assertEqual(self.profiles[3].projection_sources, (19, 20))
        self.assertEqual(self.profiles[4].projection_sources, (20, 22))
        self.assertEqual(self.profiles[5].projection_sources, (22, 24))
        self.assertEqual(self.profiles[6].projection_sources, (24, 25))
        for index, sources in ((3, (19, 20)), (4, (20, 22)), (5, (22, 24))):
            evidence = project(self.profiles[index], [rung(sources[0]), rung(sources[1])], CAPACITY)
            self.evidence_schema.validate(evidence)
            self.assertEqual(evidence["source_scales"], list(sources))
        s26 = project(self.profiles[6], [rung(24), rung(25)], CAPACITY)
        self.assertEqual(s26["source_scales"], [24, 25])
        malformed = copy.copy(self.profiles[6])
        object.__setattr__(malformed, "projection_sources", (20, 22))
        with self.assertRaisesRegex(QualificationError, "adjacent S24 and S25"):
            project(malformed, [rung(20), rung(22)], CAPACITY)
        wrong_source = rung(25)
        wrong_source["source"] = "progressive_profile"
        with self.assertRaisesRegex(QualificationError, "canonical S24 and S25"):
            project(self.profiles[6], [rung(24), wrong_source], CAPACITY)
        unknown = Profile("graph500-s23-provider", 23, "provider", (20, 22))
        with self.assertRaisesRegex(QualificationError, "not on the progressive ladder"):
            project(unknown, [rung(20), rung(22)], CAPACITY)


if __name__ == "__main__":
    unittest.main()
