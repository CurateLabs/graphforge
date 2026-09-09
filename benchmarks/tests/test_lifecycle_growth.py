from __future__ import annotations

import copy
import importlib.util
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "tiny_growth", Path(__file__).parents[1] / "scripts/test-tiny-lifecycle-certification.py"
)
assert SPEC and SPEC.loader
GROWTH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GROWTH)


def observations():
    result = []
    for scale, factor in zip((6, 7, 8), (1, 2, 4), strict=True):
        owners = {}
        for owner in GROWTH.OWNER_NAMES:
            if owner in GROWTH.ZERO_OWNERS:
                metrics = [0, 0, 0, 0, 0]
            elif owner.endswith(("-locks", "-admission_lock")):
                metrics = [1, 0, 1, 0, 0]
            elif owner in GROWTH.DATA_OWNERS:
                metrics = [2, 8192 * factor, 2, 8192 * factor, 8192 * factor]
            else:
                metrics = [1, 628, 1, 628, 4096]
            owners[owner] = {
                "totals": dict(
                    zip(
                        (
                            "logical_references",
                            "logical_bytes",
                            "physical_objects",
                            "physical_logical_bytes",
                            "allocated_bytes",
                        ),
                        metrics,
                        strict=True,
                    )
                )
            }
        receipt = {
            "contract": "graphforge-lifecycle-storage/2",
            "source_project_current_allocated_bytes": 8192 * factor,
            "retained_owners": owners,
            "retained_storage_bytes": 50000 * factor,
            "transient_peak_storage_bytes": 60000 * factor,
        }
        n = 1 << scale
        result.append(
            {
                "scale": scale,
                "live_nodes": n,
                "live_edges": n * 16,
                "query_rows": [n, n * 16],
                "imported_query_rows": [n, n * 16],
                "receipt": receipt,
                "ratios": {
                    field: {
                        "per_live_node": [receipt[field], n],
                        "per_live_edge": [receipt[field], n * 16],
                    }
                    for field in ("retained_storage_bytes", "transient_peak_storage_bytes")
                },
            }
        )
    return result


class LifecycleGrowthTests(unittest.TestCase):
    def test_declared_linear_and_fixed_policies(self):
        GROWTH.validate_growth(observations())

    def test_published_radix_object_counts_keep_positive_linear_ceiling(self):
        for owner in ("source-project-published", "imported-project-published"):
            changed = observations()
            # Real SCALE6/7/8 inventories: radix nodes 47/48/46 plus
            # 25 fixed objects. Every graph inventory has 18 logical files.
            for observation, count in zip(changed, (72, 73, 71), strict=True):
                totals = observation["receipt"]["retained_owners"][owner]["totals"]
                totals["logical_references"] = totals["physical_objects"] = count
            GROWTH.validate_growth(changed)
            for invalid in (0, -1, 577):
                with self.subTest(owner=owner, invalid=invalid):
                    refused = copy.deepcopy(changed)
                    totals = refused[2]["receipt"]["retained_owners"][owner]["totals"]
                    totals["logical_references"] = totals["physical_objects"] = invalid
                    with self.assertRaises(ValueError):
                        GROWTH.validate_growth(refused)

    def test_each_data_owner_rejects_flat_underreported_and_inflated_eof(self):
        for owner in GROWTH.DATA_OWNERS:
            for field in ("logical_bytes", "physical_logical_bytes"):
                for value in (8192, 17000, 200000):
                    with self.subTest(owner=owner, field=field, value=value):
                        changed = observations()
                        changed[2]["receipt"]["retained_owners"][owner]["totals"][field] = value
                        with self.assertRaises(ValueError):
                            GROWTH.validate_growth(changed)

    def test_missing_owner_metric_and_fabricated_ratio_refused(self):
        for mutate in (
            lambda o: o[1]["receipt"]["retained_owners"].pop("source-project-import"),
            lambda o: o[1]["receipt"]["retained_owners"]["generated-inputs"]["totals"].pop(
                "physical_objects"
            ),
            lambda o: o[1]["ratios"]["retained_storage_bytes"]["per_live_edge"].__setitem__(1, 1),
            lambda o: o[1].__setitem__("live_nodes", 0),
            lambda o: o[1].__setitem__("query_rows", [128, 1000]),
            lambda o: o[1].__setitem__("imported_query_rows", [128, 1000]),
        ):
            changed = copy.deepcopy(observations())
            mutate(changed)
            with self.assertRaises(ValueError):
                GROWTH.validate_growth(changed)

    def test_quantized_owner_plateau_allowed_but_complete_peak_flat_refused(self):
        changed = observations()
        for o in changed:
            o["receipt"]["retained_owners"]["generated-inputs"]["totals"]["allocated_bytes"] = 32768
        GROWTH.validate_growth(changed)
        changed = observations()
        for o in changed:
            o["receipt"]["transient_peak_storage_bytes"] = 120000
            o["ratios"]["transient_peak_storage_bytes"]["per_live_node"][0] = 120000
            o["ratios"]["transient_peak_storage_bytes"]["per_live_edge"][0] = 120000
        with self.assertRaises(ValueError):
            GROWTH.validate_growth(changed)
