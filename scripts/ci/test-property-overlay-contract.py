#!/usr/bin/env python3
"""Mutation sentinels for the source-backed property-overlay contract gate."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import re
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


def check_module_discovery(root: Path) -> None:
    """Exercise admitted nested sources and exact root-visible exports."""
    entry = Path("discovery/authority.rs")
    directory = root / "discovery/authority"
    (directory / "child").mkdir(parents=True)
    files = {
        root / entry: "mod child;\npub use child::{Authority, read};\n",
        directory / "child.rs": "mod nested;\npub use nested::Authority;\npub fn read() {}\n",
        directory / "child/nested.rs": (
            "pub struct Authority {}\nimpl Authority {\n    pub fn snapshot() {}\n}\n"
        ),
    }
    for path, source in files.items():
        path.write_text(source, encoding="utf-8")

    def check() -> None:
        actual = GATE.module_public_symbols(GATE.module_sources(root, entry))
        if actual != {"Authority", "read", "Authority::snapshot"}:
            raise GATE.ContractError("fixture root exports changed")

    check()
    mutations = [
        (root / entry, "mod child;", "// mod child;", "comment-only module"),
        (
            root / entry,
            "pub use child::{Authority, read};",
            "use child::{Authority, read};",
            "private root export",
        ),
        (
            root / entry,
            "pub use child::{Authority, read};",
            "// pub use child::{Authority, read};",
            "comment-only root export",
        ),
        (
            root / entry,
            "pub use child::{Authority, read};",
            "pub use child::{Authority, read};\npub use child::read;",
            "duplicate root export",
        ),
        (directory / "child.rs", "pub fn read", "pub(super) fn read", "private defining symbol"),
        (directory / "child/nested.rs", "pub fn snapshot", "fn snapshot", "lost moved method"),
        (
            directory / "child/nested.rs",
            "pub struct Authority {}",
            "pub struct Authority {}\npub struct Authority {}",
            "duplicate authority definition",
        ),
    ]
    for path, before, after, label in mutations:
        source = files[path]
        if before not in source:
            raise AssertionError(f"missing mutation marker: {label}")
        path.write_text(source.replace(before, after, 1), encoding="utf-8")
        expect_failure(label, check)
        path.write_text(source, encoding="utf-8")
    child = directory / "child.rs"
    child.unlink()
    expect_failure("missing declared child", check)
    child.write_text(files[child], encoding="utf-8")
    # An orphan source must not repair a removed declaration, and literal
    # declarations must not introduce an admitted child.
    (directory / "orphan.rs").write_text("pub fn read() {}\n", encoding="utf-8")
    child.write_text(
        files[child] + 'const TEXT: &str = r#"mod absent; pub use absent::Ghost;"#;\n',
        encoding="utf-8",
    )
    check()
    child.write_text(files[child].replace("pub fn read() {}", ""), encoding="utf-8")
    expect_failure("orphan cannot supply root authority", check)
    child.write_text(files[child], encoding="utf-8")

    # Bazel may expose each source as a separate symlink beneath real
    # runfiles directories; containment follows the resolved authority root.
    runfiles = root / "runfiles"
    for source in files:
        destination = runfiles / source.relative_to(root)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.symlink_to(source)
    actual = GATE.module_public_symbols(GATE.module_sources(runfiles, entry))
    if actual != {"Authority", "read", "Authority::snapshot"}:
        raise AssertionError("individual-file runfiles lost the public surface")
    # A redirected declared child must still stay inside that resolved tree.
    outside = root / "outside.rs"
    outside.write_text(files[directory / "child/nested.rs"], encoding="utf-8")
    linked_child = runfiles / "discovery/authority/child/nested.rs"
    linked_child.unlink()
    linked_child.symlink_to(outside)
    expect_failure(
        "runfiles child escapes resolved authority",
        lambda: GATE.module_sources(runfiles, entry),
    )


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="gf-property-overlay-contract-") as directory:
        root = Path(directory)
        check_module_discovery(root)
        for relative in (
            "tests/contracts/property-overlay-v1.json",
            "crates/graphforge-storage/src/property_overlay.rs",
            "crates/graphforge-storage/src/lib.rs",
            "crates/graphforge-storage/src/writer.rs",
            "crates/graphforge-storage/tests/property_overlay_scale.rs",
            "crates/graphforge-storage/BUILD.bazel",
            "BUILD.bazel",
        ):
            destination = root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / relative, destination)
        for relative in (
            "crates/graphforge-storage/src/property_overlay",
            "crates/graphforge-storage/src/writer",
        ):
            shutil.copytree(ROOT / relative, root / relative)
        contract_path = root / "tests/contracts/property-overlay-v1.json"
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
        for reference in contract["evidence"].values():
            relative = reference["path"]
            destination = root / relative
            if not destination.exists():
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, destination)

        GATE.validate(root, contract_path)

        for relative, symbol in (
            ("crates/graphforge-storage/src/property_overlay.rs", "PropertyTargetSnapshots"),
            ("crates/graphforge-storage/src/writer.rs", "stage_set_node_properties_authenticated"),
        ):
            export_path = root / relative
            export_source = export_path.read_text(encoding="utf-8")
            pattern = rf"(?m)^pub use \w+::{symbol};\n|\b{symbol}\s*,"
            mutated_source, count = re.subn(pattern, "", export_source, count=1)
            if count != 1:
                raise AssertionError(f"missing root reexport mutation: {symbol}")
            export_path.write_text(mutated_source, encoding="utf-8")
            expect_failure(
                f"lost root reexport {symbol}", lambda: GATE.validate(root, contract_path)
            )
            export_path.write_text(export_source, encoding="utf-8")

        mutated = copy.deepcopy(contract)
        mutated["metrics"]["physical_rows"]["unit"] = "rows"
        write_contract(contract_path, mutated)
        expect_failure("physical_rows unit", lambda: GATE.validate(root, contract_path))

        mutated = copy.deepcopy(contract)
        mutated["metrics"]["authentication_read_calls"]["unit"] = "64 KiB block-equivalents"
        write_contract(contract_path, mutated)
        expect_failure(
            "authentication call/equivalent conflation", lambda: GATE.validate(root, contract_path)
        )

        mutated = copy.deepcopy(contract)
        mutated["authority"]["read_scope"] = "newest generation only"
        write_contract(contract_path, mutated)
        expect_failure("all-generation authority", lambda: GATE.validate(root, contract_path))

        mutated = copy.deepcopy(contract)
        mutated["platform"]["unsupported"] = "return zero RSS"
        write_contract(contract_path, mutated)
        expect_failure("zero-evidence platform", lambda: GATE.validate(root, contract_path))
        write_contract(contract_path, contract)

        writer = root / "crates/graphforge-storage/src/writer/property_mutation.rs"
        writer_source = writer.read_text(encoding="utf-8")
        if "read_authenticated_property_snapshots_for_inventory" not in writer_source:
            raise AssertionError("staging fixture lost targeted reader")
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
        overlay.write_text(
            original_overlay.replace("pub physical_rows: u64", "pub decoded_rows: u64", 1),
            encoding="utf-8",
        )
        expect_failure("Rust metric drift", lambda: GATE.validate(root, contract_path))
        overlay.write_text(original_overlay, encoding="utf-8")

        library = root / "crates/graphforge-storage/src/lib.rs"
        original_library = library.read_text(encoding="utf-8")
        library.write_text(
            original_library.replace("PropertyOverlayMetrics, ", "", 1), encoding="utf-8"
        )
        expect_failure("Rust export drift", lambda: GATE.validate(root, contract_path))
        library.write_text(original_library, encoding="utf-8")

        overlay.write_text(
            original_overlay.replace("max_buffered_rows: 4096", "max_buffered_rows: 4097", 1),
            encoding="utf-8",
        )
        expect_failure("Rust limit drift", lambda: GATE.validate(root, contract_path))
        overlay.write_text(original_overlay, encoding="utf-8")

        evidence_path = root / contract["evidence"]["canonical_fragment_identity"]["path"]
        evidence_source = evidence_path.read_text(encoding="utf-8")
        evidence_path.write_text(
            evidence_source.replace(
                "fragment_identity_is_numeric_canonical_and_total", "fragment_identity_drifted", 1
            ),
            encoding="utf-8",
        )
        expect_failure("stale evidence symbol", lambda: GATE.validate(root, contract_path))
        evidence_path.write_text(evidence_source, encoding="utf-8")

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

        helper_line = "rss.saturating_mul(1024)"
        if helper_line not in scale_source:
            raise AssertionError("scale fixture lost RSS normalization helper")
        scale.write_text(
            scale_source.replace(helper_line, "rss.saturating_mul(2048)", 1),
            encoding="utf-8",
        )
        expect_failure("scale helper drift", lambda: GATE.validate(root, contract_path))
        scale.write_text(scale_source, encoding="utf-8")

        child_line = "emitted_rows += 1;"
        if child_line not in scale_source:
            raise AssertionError("scale fixture lost child emission counter")
        scale.write_text(
            scale_source.replace(child_line, "emitted_rows += 2;", 1),
            encoding="utf-8",
        )
        expect_failure("scale child drift", lambda: GATE.validate(root, contract_path))
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
        storage_build.write_text(build_source, encoding="utf-8")

        root_build = root / "BUILD.bazel"
        root_build_source = root_build.read_text(encoding="utf-8")
        root_build.write_text(
            root_build_source.replace(
                '"//crates/graphforge-storage:storage_integration_tests",',
                "",
                1,
            ),
            encoding="utf-8",
        )
        expect_failure("root integration suite mapping", lambda: GATE.validate(root, contract_path))
        root_build.write_text(
            root_build_source.replace('\n        ":integration_tests",', "", 1),
            encoding="utf-8",
        )
        expect_failure("ci Rust suite mapping", lambda: GATE.validate(root, contract_path))

    print("property overlay contract mutation tests passed")


if __name__ == "__main__":
    main()
