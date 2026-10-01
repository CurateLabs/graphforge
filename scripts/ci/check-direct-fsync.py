#!/usr/bin/env python3
"""Fail on new physical fsync expressions outside the storage commit owner.

The exceptions are exact reviewed functions and maximum source-site counts:
existing fault/corruption fixtures, two Arrow buffer flushes, and filesystem
admission probes. They are included in the separate reproducible census.
"""

from collections import Counter
import importlib.util
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "fsync_sites", ROOT / "scripts/development/fsync-sites.py"
)
CENSUS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CENSUS)
ALLOWED = {
    (
        "crates/graphforge-api/src/import_session.rs",
        "parquet_registration_copy_bounds_combined_non_sparse_cache_and_preserves_bytes",
        "observed_sync_all",
    ): 1,
    (
        "crates/graphforge-api/src/import_session.rs",
        "persist_manifest_with_source_cleanup",
        "sync",
    ): 1,
    ("crates/graphforge-api/src/import_session/journal.rs", "append", "sync"): 1,
    ("crates/graphforge-api/src/import_session/journal.rs", "open", "sync"): 1,
    (
        "crates/graphforge-storage/src/construction_lifecycle_tests.rs",
        "flip_last_byte",
        "observed_sync_all",
    ): 1,
    (
        "crates/graphforge-storage/src/construction_lifecycle_tests.rs",
        "rewrite",
        "observed_sync_all",
    ): 1,
    ("crates/graphforge-storage/src/durable_commit.rs", "acknowledge_directory", "sync"): 1,
    (
        "crates/graphforge-storage/src/durable_commit.rs",
        "seal_cache_writer",
        "sync_all_and_release",
    ): 1,
    ("crates/graphforge-storage/src/durable_commit.rs", "seal_file", "observed_sync_all"): 1,
    (
        "crates/graphforge-storage/src/durable_rewrite.rs",
        "same_inode_same_length_corrupted_temporary_fails_closed_by_checksum",
        "sync_all",
    ): 1,
    (
        "crates/graphforge-storage/src/filesystem_admission.rs",
        "complete_namespace_barrier",
        "observed_sync_all",
    ): 1,
    (
        "crates/graphforge-storage/src/filesystem_admission.rs",
        "complete_namespace_barrier_handle",
        "observed_sync_all",
    ): 1,
    (
        "crates/graphforge-storage/src/filesystem_admission.rs",
        "replace_probe_file",
        "observed_sync_all",
    ): 2,
    ("crates/graphforge-storage/src/filesystem_admission.rs", "run_probe", "observed_sync_all"): 1,
    (
        "crates/graphforge-storage/src/filesystem_admission.rs",
        "subprocess_crash_lock_holder",
        "observed_sync_all",
    ): 1,
    (
        "crates/graphforge-storage/src/graph_construction/encoding_publication/tests.rs",
        "completed_encoding_replay_reauthenticates_retained_parent_payload",
        "sync_all",
    ): 1,
    (
        "crates/graphforge-storage/src/graph_construction/encoding_publication/tests.rs",
        "flip_first_byte_in_place",
        "sync_all",
    ): 1,
    (
        "crates/graphforge-storage/src/graph_construction/encoding_publication/tests.rs",
        "publication_refuses_same_inode_encoded_payload_corruption_at_cas_install",
        "sync_all",
    ): 1,
    (
        "crates/graphforge-storage/src/graph_construction/intake.rs",
        "write_parquet_with_properties",
        "sync",
    ): 1,
    ("crates/graphforge-storage/src/graph_construction/partition_shaping.rs", "finish", "sync"): 1,
    (
        "crates/graphforge-storage/src/graph_object_store/installation/tests.rs",
        "cas_copy_isolated_from_preexisting_writable_source_descriptor",
        "sync_all",
    ): 1,
    (
        "crates/graphforge-storage/src/graph_object_store/installation/tests.rs",
        "fresh_write_descriptor_can_be_sealed_without_losing_named_identity",
        "sync_all",
    ): 1,
    (
        "crates/graphforge-storage/src/project_portable_v2_export.rs",
        "export_failed_partial_write_is_observed_before_cleanup",
        "observed_sync_all",
    ): 1,
    (
        "crates/graphforge-storage/src/storage_attribution.rs",
        "one_physical_identity_is_counted_once_for_shared_references",
        "observed_sync_all",
    ): 1,
    (
        "crates/graphforge-storage/src/uuid_membership/probing/tests.rs",
        "retained_snapshot_rehashes_manifest_and_authenticates_only_candidate_blocks",
        "sync_all",
    ): 1,
    (
        "crates/graphforge-storage/src/uuid_membership/tests.rs",
        "readonly_shared_runs_preserve_uuid_snapshot_authentication",
        "sync_all",
    ): 1,
}
METHOD = re.compile(
    r"\.\s*(observed_sync_all|observed_sync_data|sync_all|sync_data|sync|sync_all_and_release|sync_all_retained|sync_parent_dir)\b"
)
NATIVE = re.compile(
    r"\b(?:fsync|fdatasync|FlushFileBuffers)\b|::\s*(?:sync_all|sync_data|observed_sync_all|observed_sync_data)\b"
)


def sites(path, source):
    code = CENSUS.mask(source)
    functions = list(re.finditer(r"\bfn\s+(\w+)\s*(?:<[^{};]*>)?\s*\(", code))
    found = []
    for match in list(METHOD.finditer(code)) + list(NATIVE.finditer(code)):
        preceding = [function for function in functions if function.start() < match.start()]
        function = preceding[-1].group(1) if preceding else "<module>"
        method = match.group(1) if match.re is METHOD else "native/UFCS"
        found.append(((path, function, method), source.count("\n", 0, match.start()) + 1))
    return found


def check():
    paths = list((ROOT / "crates/graphforge-storage/src").rglob("*.rs"))
    paths += [
        ROOT / "crates/graphforge-api/src/import_session.rs",
        ROOT / "crates/graphforge-api/src/import_session/journal.rs",
    ]
    counts = Counter()
    violations = []
    for path in sorted(paths):
        relative = path.relative_to(ROOT).as_posix()
        for key, line in sites(relative, path.read_text()):
            counts[key] += 1
            if counts[key] > ALLOWED.get(key, 0):
                violations.append(f"{relative}:{line}: {key[1]}: direct {key[2]}")
    if violations:
        print(
            "Direct fsync must use durable_commit; new exceptions require specific review:",
            file=sys.stderr,
        )
        print("\n".join(violations), file=sys.stderr)
        return 1
    print("direct-fsync guard: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(check())
