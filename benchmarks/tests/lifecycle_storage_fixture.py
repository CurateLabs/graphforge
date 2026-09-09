"""Synthetic owner views for adapter tests; never used as measured evidence."""

from graphforge_bench.progressive_run import RETAINED_OWNER_NAMES


def retained_owners(source: int, imported: int, portable: int, staging: int = 0) -> dict:
    allocations = {
        "source-project-published": source,
        "imported-project-published": imported,
        "portable-package": portable,
        "source-project-construction": staging,
    }
    return {
        name: {
            "totals": {
                "logical_references": int(allocations.get(name, 0) > 0),
                "logical_bytes": allocations.get(name, 0),
                "physical_objects": int(allocations.get(name, 0) > 0),
                "physical_logical_bytes": allocations.get(name, 0),
                "allocated_bytes": allocations.get(name, 0),
            }
        }
        for name in RETAINED_OWNER_NAMES
    }
