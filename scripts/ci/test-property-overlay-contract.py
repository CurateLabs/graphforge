#!/usr/bin/env python3
"""Mutation sentinels for the source-backed property-overlay contract gate."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile

ROOT = Path(__file__).resolve().parents[2]
GATE_PATH = ROOT / "scripts/ci/property-overlay-contract.py"
SPEC = importlib.util.spec_from_file_location("property_overlay_contract", GATE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("cannot load property-overlay contract gate")
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)


def expect_failure(label: str, action) -> None:
    try:
        action()
    except (GATE.ContractError, json.JSONDecodeError):
        return
    raise AssertionError(f"mutation passed: {label}")


def write_contract(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="gf-property-overlay-contract-") as directory:
        root = Path(directory)
        for relative in (
            "tests/contracts/property-overlay-v1.json",
            "crates/graphforge-storage/src/property_overlay.rs",
            "crates/graphforge-storage/src/lib.rs",
            "crates/graphforge-storage/src/writer.rs",
            "crates/graphforge-storage/tests/property_overlay_scale.rs",
            "crates/graphforge-storage/BUILD.bazel",
        ):
            destination = root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, destination)
        contract_path = root / "tests/contracts/property-overlay-v1.json"
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        for reference in contract["evidence"].values():
            relative = reference["path"]
            destination = root / relative
            if not destination.exists():
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, destination)

        GATE.validate(root, contract_path)

        mutated = copy.deepcopy(contract)
        mutated["metrics"]["physical_rows"]["unit"] = "rows"
        write_contract(contract_path, mutated)
        expect_failure("physical_rows unit", lambda: GATE.validate(root, contract_path))

        mutated = copy.deepcopy(contract)
        mutated["authority"]["read_scope"] = "newest generation only"
        write_contract(contract_path, mutated)
        expect_failure("all-generation authority", lambda: GATE.validate(root, contract_path))

        mutated = copy.deepcopy(contract)
        mutated["platform"]["unsupported"] = "return zero RSS"
        write_contract(contract_path, mutated)
        expect_failure("zero-evidence platform", lambda: GATE.validate(root, contract_path))
        write_contract(contract_path, contract)

        writer = root / "crates/graphforge-storage/src/writer.rs"
        writer_source = writer.read_text(encoding="utf-8")
        writer.write_text(
            writer_source.replace(
                "read_authenticated_property_snapshots_for_inventory",
                "visit_authenticated_property_snapshots",
            ),
            encoding="utf-8",
        )
        expect_failure("full prior decode", lambda: GATE.validate(root, contract_path))
        writer.write_text(writer_source, encoding="utf-8")

        mutated = copy.deepcopy(contract)
        del mutated["metrics"]["range_seeks"]
        write_contract(contract_path, mutated)
        expect_failure("missing metric", lambda: GATE.validate(root, contract_path))

        write_contract(contract_path, contract)
        overlay = root / "crates/graphforge-storage/src/property_overlay.rs"
        original_overlay = overlay.read_text(encoding="utf-8")
        overlay.write_text(original_overlay.replace("pub physical_rows: u64", "pub decoded_rows: u64", 1), encoding="utf-8")
        expect_failure("Rust metric drift", lambda: GATE.validate(root, contract_path))
        overlay.write_text(original_overlay, encoding="utf-8")

        library = root / "crates/graphforge-storage/src/lib.rs"
        original_library = library.read_text(encoding="utf-8")
        library.write_text(original_library.replace("PropertyOverlayMetrics, ", "", 1), encoding="utf-8")
        expect_failure("Rust export drift", lambda: GATE.validate(root, contract_path))
        library.write_text(original_library, encoding="utf-8")

        overlay.write_text(original_overlay.replace("max_buffered_rows: 4096", "max_buffered_rows: 4097", 1), encoding="utf-8")
        expect_failure("Rust limit drift", lambda: GATE.validate(root, contract_path))
        overlay.write_text(original_overlay, encoding="utf-8")

        evidence_path = root / contract["evidence"]["canonical_fragment_identity"]["path"]
        evidence_source = evidence_path.read_text(encoding="utf-8")
        evidence_path.write_text(evidence_source.replace("fragment_identity_is_numeric_canonical_and_total", "fragment_identity_drifted", 1), encoding="utf-8")
        expect_failure("stale evidence symbol", lambda: GATE.validate(root, contract_path))

        scale = root / "crates/graphforge-storage/tests/property_overlay_scale.rs"
        scale_source = scale.read_text(encoding="utf-8")
        assertion = "assert!(phase.authentication_bytes > 0);"
        if assertion not in scale_source:
            raise AssertionError("scale fixture lost authentication assertion")
        scale.write_text(
            scale_source.replace(assertion, f"// {assertion}", 1),
            encoding="utf-8",
        )
        expect_failure("comment-only metric assertion", lambda: GATE.validate(root, contract_path))
        scale.write_text(
            scale_source.replace(assertion, f"if false {{ {assertion} }}", 1),
            encoding="utf-8",
        )
        expect_failure("dead metric assertion", lambda: GATE.validate(root, contract_path))
        scale.write_text(scale_source, encoding="utf-8")

        total_assertion = "assert!(phase.physical_bytes <= total_read_bound);"
        if total_assertion not in scale_source:
            raise AssertionError("scale fixture lost derived total read assertion")
        scale.write_text(
            scale_source.replace(total_assertion, f"// {total_assertion}", 1),
            encoding="utf-8",
        )
        expect_failure("comment-only total read bound", lambda: GATE.validate(root, contract_path))
        scale.write_text(scale_source, encoding="utf-8")

        storage_build = root / "crates/graphforge-storage/BUILD.bazel"
        build_source = storage_build.read_text(encoding="utf-8")
        storage_build.write_text(
            build_source.replace('\n        ":property_overlay_scale",', "", 1),
            encoding="utf-8",
        )
        expect_failure("scale Bazel mapping", lambda: GATE.validate(root, contract_path))
        storage_build.write_text(
            build_source.replace(
                '":property_overlay_scale",',
                '# ":property_overlay_scale",',
                1,
            ),
            encoding="utf-8",
        )
        expect_failure("comment-only Bazel mapping", lambda: GATE.validate(root, contract_path))

    print("property overlay contract mutation tests passed")


if __name__ == "__main__":
    main()
