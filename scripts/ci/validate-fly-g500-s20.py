#!/usr/bin/env python3
"""Validate the closed, sanitized Fly S20 evidence contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Any

import jsonschema

ROOT = Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "docs/development/evidence/fly-g500-s20.schema.json"
SENSITIVE_KEY = re.compile(r"(?:token|secret|machine[_-]?id|volume[_-]?id)", re.I)
ABSOLUTE_PATH = re.compile(r"^(?:/|\\\\|\\[^\\]|[A-Za-z]:\\)")
RAW_UUID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.I,
)


class EvidenceError(RuntimeError):
    pass


def validate(
    value: Any, sha: str, digest: str, region: str, source_snapshot: str | None = None
) -> None:
    try:
        jsonschema.validate(
            value, json.loads(SCHEMA.read_text()), format_checker=jsonschema.FormatChecker()
        )
    except jsonschema.ValidationError as error:
        raise EvidenceError(f"schema violation: {error.message}") from None
    if value["git_sha"] != sha or value["image_digest"] != digest or value["region"] != region:
        raise EvidenceError("evidence identity differs from the pinned run")
    provenance = value["build_provenance"]
    if provenance["source_sha"] != sha or (
        source_snapshot is not None and provenance["source_snapshot_sha256"] != source_snapshot
    ):
        raise EvidenceError("evidence build provenance differs from the pinned image")
    counts = value["counts"]
    expected_nodes = 1 << 20
    expected_attempts = expected_nodes * 16
    if counts["raw_attempts"] != expected_attempts:
        raise EvidenceError("S20 ef=16 requires exactly 2^20 * 16 raw attempts")
    if (
        value["result"] == "passed"
        and len({counts["generated_edges"], counts["source_edges"], counts["imported_edges"]}) != 1
    ):
        raise EvidenceError(
            "passing evidence must reconcile generated, source, and imported counts"
        )
    if counts["raw_attempts"] != (
        counts["generated_edges"] + counts["self_loops_rejected"] + counts["duplicates_rejected"]
    ):
        raise EvidenceError("generator attempt accounting does not reconcile")
    lifecycle = value["lifecycle"]
    if lifecycle["source_nodes"] != expected_nodes or lifecycle["imported_nodes"] != expected_nodes:
        raise EvidenceError("S20 lifecycle requires exactly 2^20 source/imported nodes")
    if (
        lifecycle["source_edges"] != counts["source_edges"]
        or lifecycle["imported_edges"] != counts["imported_edges"]
    ):
        raise EvidenceError("lifecycle edge counts are not bound to top-level counts")
    if lifecycle["source_nodes"] != lifecycle["imported_nodes"]:
        raise EvidenceError("source/import node counts differ")
    for suffix in ("one_hop", "two_hop"):
        if (
            lifecycle[f"source_{suffix}"]["fingerprint"]
            != lifecycle[f"imported_{suffix}"]["fingerprint"]
        ):
            raise EvidenceError(f"source/import {suffix} fingerprints differ")
    if lifecycle["source_authority_fingerprint"] != lifecycle["imported_authority_fingerprint"]:
        raise EvidenceError("source/import authority fingerprints differ")
    construction = value["rung"]["construction"]
    if (
        construction["published_generation_sha256"]
        != construction["recovered_generation_sha256"]
    ):
        raise EvidenceError("construction recovery generation identity differs")
    publication = lifecycle["publication"]
    if publication["published_generation_sha256"] != publication["recovered_generation_sha256"]:
        raise EvidenceError("lifecycle recovery generation identity differs")
    if (
        construction["published_generation_sha256"]
        != publication["published_generation_sha256"]
    ):
        raise EvidenceError("construction and lifecycle publication identities differ")
    for name in ("source_storage", "imported_storage"):
        attribution = lifecycle[name]
        fields = (
            "logical_references",
            "logical_bytes",
            "physical_objects",
            "physical_logical_bytes",
            "allocated_bytes",
        )
        for field in fields:
            observed = sum(category[field] for category in attribution["categories"].values())
            if observed != attribution[field]:
                raise EvidenceError(f"{name} category {field} does not reconcile")
        if any(attribution["categories"]["other"][field] for field in fields):
            raise EvidenceError(f"{name} contains unclassified artifacts")
    storage = value["storage"]
    expected_logical = (
        lifecycle["source_storage"]["logical_bytes"]
        + lifecycle["imported_storage"]["logical_bytes"]
        + lifecycle["package_storage"]["logical_bytes"]
        + storage["generator_logical_bytes"]
    )
    expected_allocated = (
        lifecycle["source_storage"]["allocated_bytes"]
        + lifecycle["imported_storage"]["allocated_bytes"]
        + lifecycle["package_storage"]["allocated_bytes"]
        + storage["generator_allocated_bytes"]
    )
    if (
        storage["logical_bytes"] != expected_logical
        or storage["allocated_bytes"] != expected_allocated
    ):
        raise EvidenceError("top-level storage totals do not reconcile with attributed artifacts")
    if storage["peak_allocated_bytes"] < storage["allocated_bytes"]:
        raise EvidenceError("storage peak understates retained allocated bytes")
    capacity_limit = value["volume_gb"] * 1024**3
    if not capacity_limit * 9 // 10 <= storage["capacity_bytes"] <= capacity_limit:
        raise EvidenceError("observed capacity is not bound to the declared Fly volume")
    if value["storage"]["peak_allocated_bytes"] > value["storage"]["capacity_bytes"]:
        raise EvidenceError("allocated storage exceeds volume capacity")
    for phase, memory in value["phase_memory"].items():
        if (
            memory["rss_peak_bytes"] == 0
            or memory["process_global_hwm_bytes"] < memory["rss_peak_bytes"]
        ):
            raise EvidenceError(f"phase {phase} lacks truthful RSS/HWM sampling")
        if memory["anonymous_peak_bytes"] + memory["file_peak_bytes"] < memory["rss_peak_bytes"]:
            raise EvidenceError(f"phase {phase} memory categories understate RSS")
        if memory["rss_peak_bytes"] > 4096 * 1024 * 1024:
            raise EvidenceError(f"phase {phase} exceeds the 4096 MiB S20 memory envelope")
    windows = value["ingest_memory_windows"]
    early = windows["early_rss_peak_bytes"]
    middle = windows["middle_rss_peak_bytes"]
    late = windows["late_rss_peak_bytes"]
    envelope = windows["envelope_bytes"]
    allowed = envelope // 8
    observed = max(
        0,
        middle - early,
        late - middle,
        late - early,
    )
    headroom = envelope - max(early, middle, late)
    if windows["allowed_growth_bytes"] != allowed:
        raise EvidenceError("ingest allowed growth is not one eighth of the envelope")
    if windows["observed_growth_bytes"] != observed:
        raise EvidenceError("ingest observed growth does not match window peaks")
    if observed > allowed or windows["plateau_pass"] is not True:
        raise EvidenceError("ingest RSS does not plateau within the allowed growth")
    if headroom <= 0 or windows["headroom_bytes"] != headroom:
        raise EvidenceError("ingest headroom does not match the memory envelope")

    def reject_sensitive(item: Any) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                if SENSITIVE_KEY.search(key):
                    raise EvidenceError("evidence contains a forbidden identifier or credential")
                reject_sensitive(child)
        elif isinstance(item, list):
            for child in item:
                reject_sensitive(child)
        elif isinstance(item, str):
            if ABSOLUTE_PATH.match(item):
                raise EvidenceError("evidence contains an absolute path")
            if RAW_UUID.fullmatch(item):
                raise EvidenceError("evidence contains a raw UUID")

    reject_sensitive(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--expected-image-digest", required=True)
    parser.add_argument("--expected-region", required=True)
    args = parser.parse_args()
    try:
        validate(
            json.loads(args.evidence.read_text()),
            args.expected_sha,
            args.expected_image_digest,
            args.expected_region,
        )
    except (OSError, json.JSONDecodeError, EvidenceError) as error:
        print(f"error: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
