use super::AuthenticatedUuidIndexSnapshot;
use super::FAIL_AFTER_MANIFEST_SUSPEND;
use super::INDEX_DIR;
use super::MANIFEST;
use super::Manifest;
use super::TOPOLOGY_RECEIPT;
use super::TopologyIndexReceipt;
use super::UuidIndexBuildLimits;
use super::UuidIndexKind;
use super::UuidMembershipIndex;
use super::UuidTopologyDelta;
use super::V4_ORDINAL_MANIFEST;
use super::V4_ORDINAL_RECEIPT;
use super::append_uuid_membership_delta;
use super::maintain_uuid_membership_orphans_with_ordinal_authority;
use super::rebuild::manifest_generation;
use super::rebuild::rebuild_uuid_membership_indexes;
use super::rebuild::rebuild_v4_ordinal_identity;
use super::rebuild::rebuild_v4_ordinal_identity_with_evidence;
use super::topology_delta::V4_PLAN_PREFIX;
use super::topology_delta::V4_PLAN_ROOT;
use super::topology_delta::append_uuid_membership_delta_with_tombstones;
use super::topology_delta::commit_uuid_topology_rewrite;
use super::topology_delta::hex_sha256;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

pub(super) fn pinned_v4_update(
    root: &Path,
    manifest: crate::V4OrdinalIdentityManifest,
) -> crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs {
    let index = root.join(INDEX_DIR);
    let artifacts = manifest
        .forward_identities
        .iter()
        .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
        .chain(manifest.tombstones.iter().map(|run| &run.artifact))
        .map(
            |descriptor| crate::ordinal_identity_v4::PinnedV4OrdinalArtifact {
                descriptor: descriptor.clone(),
                file: File::open(index.join(&descriptor.name)).unwrap(),
            },
        )
        .collect();
    crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs {
        manifest,
        artifacts,
    }
}

pub(super) fn install_v4_plan(batch: &crate::staging::RewriteBatch) {
    let destinations = batch
        .staged_paths()
        .map(Path::to_path_buf)
        .collect::<Vec<_>>();
    for destination in destinations {
        let temporary = batch.staged_temp(&destination).unwrap();
        fs::copy(temporary, &destination).unwrap();
    }
}

pub(super) fn write_v4_test_artifact(
    index_path: &Path,
    name: &str,
    kind: crate::V4OrdinalArtifactKind,
    generation: u64,
    bytes: &[u8],
) -> crate::V4OrdinalArtifact {
    fs::write(index_path.join(name), bytes).unwrap();
    crate::V4OrdinalArtifact {
        name: name.to_owned(),
        kind,
        generation,
        bytes: u64::try_from(bytes.len()).unwrap(),
        sha256: hex_sha256(bytes),
    }
}

pub(super) fn assert_no_v4_temporary(index: &graphforge_filesystem::StableDirectory) {
    assert!(
        index
            .child_names()
            .unwrap()
            .into_iter()
            .all(|name| !name.to_string_lossy().starts_with(".v4-"))
    );
}

fn v4_plan_sibling_count(project: &Path) -> usize {
    let root = project.join(INDEX_DIR).join(V4_PLAN_ROOT);
    root.read_dir()
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(V4_PLAN_PREFIX))
        })
        .count()
}

fn run_v4_rewrite_once(project: &Path) {
    let topology = project.join("topology/nodes.parquet");
    let mut staged = crate::staging::RewriteBatch::new();
    staged.stage_file(&topology, &topology).unwrap();
    let mut snapshot = None;
    commit_uuid_topology_rewrite(
        project,
        staged,
        &UuidTopologyDelta {
            nodes: Vec::new(),
            edges: Vec::new(),
            deleted_nodes: Vec::new(),
            deleted_edges: Vec::new(),
        },
        &mut snapshot,
    )
    .unwrap();
}

fn assert_exact_v4_reopen(project: &Path, generation: u64) {
    assert_exact_v4_reopen_values(
        project,
        generation,
        vec![
            Some(Uuid::from_u128(3)),
            Some(Uuid::from_u128(1)),
            Some(Uuid::from_u128(2)),
        ],
    );
}

fn assert_exact_v4_reopen_values(project: &Path, generation: u64, expected: Vec<Option<Uuid>>) {
    let index = project.join(INDEX_DIR);
    let manifest_bytes = fs::read(index.join(V4_ORDINAL_MANIFEST)).unwrap();
    let receipt: TopologyIndexReceipt =
        serde_json::from_slice(&fs::read(index.join(V4_ORDINAL_RECEIPT)).unwrap()).unwrap();
    assert_eq!(receipt.expected_generation, generation);
    assert_eq!(receipt.manifest_sha256, hex_sha256(&manifest_bytes));
    let authority = crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
        topology_generation: generation,
        manifest_sha256: receipt.manifest_sha256,
    };
    let mut handle = match crate::ordinal_identity_v4::V4OrdinalIdentityHandle::open(
        project,
        &authority,
        crate::V4OrdinalIdentityLimits::default(),
    )
    .unwrap()
    {
        crate::V4OrdinalIdentityOpen::Ready(handle) => handle,
        crate::V4OrdinalIdentityOpen::RebuildRequired { .. } => {
            panic!("receipt-authenticated v4 authority unexpectedly requires rebuild")
        }
    };
    let pinned = handle.pinned_update_inputs().unwrap();
    assert_eq!(pinned.manifest.topology_generation, generation);
    assert_eq!(
        pinned.artifacts.len(),
        pinned.manifest.forward_identities.len()
            + pinned.manifest.ordinal_ranges.len()
            + pinned.manifest.tombstones.len()
    );
    assert_eq!(
        handle.lookup_node_uuids(&[1, 2, 3]).unwrap().values,
        expected
    );
}

#[test]
fn v4_rewrite_subprocess_crash_retry_matrix_cleans_exact_scratch() {
    const CHILD_ROOT: &str = "GRAPHFORGE_V4_REWRITE_CHILD_ROOT";
    const RETRY: &str = "GRAPHFORGE_V4_REWRITE_RETRY";
    const TARGET: &str = "GRAPHFORGE_V4_REWRITE_TARGET";
    if let Ok(root) = std::env::var(CHILD_ROOT) {
        let root = Path::new(&root);
        crate::durable_rewrite::recover(root).unwrap();
        let target = std::env::var(TARGET).unwrap().parse::<u64>().unwrap();
        if crate::read_topology_generation(root).unwrap() < target {
            run_v4_rewrite_once(root);
        }
        if std::env::var(RETRY).is_err() {
            panic!("configured v4 rewrite failpoint did not terminate the process");
        }
        return;
    }

    for (failpoint, target) in [
        ("v4_append.after_delta_artifacts", 8),
        ("v4_append.after_receipt_stage", 8),
        ("v4_append.after_manifest_stage", 8),
        ("v4_compaction.after_outputs", 9),
        ("rewrite.before_intent", 8),
        ("rewrite.after_preparing_disarm", 8),
        ("rewrite.after_durable_intent", 8),
    ] {
        let (dir, _, _) = fixture();
        fs::write(
            dir.path().join("topology/generation.json"),
            b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
        )
        .unwrap();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        rebuild_v4_ordinal_identity(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        if target == 9 {
            run_v4_rewrite_once(dir.path());
        }

        let crashed = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg(
                    "uuid_membership::tests::v4_rewrite_subprocess_crash_retry_matrix_cleans_exact_scratch",
                )
                .arg("--nocapture")
                .env(CHILD_ROOT, dir.path())
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                .env(TARGET, target.to_string())
                .status()
                .unwrap();
        assert_eq!(
            crashed.code(),
            Some(crate::project_failpoint::exit_code()),
            "{failpoint}"
        );

        let retry = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg(
                    "uuid_membership::tests::v4_rewrite_subprocess_crash_retry_matrix_cleans_exact_scratch",
                )
                .arg("--nocapture")
                .env(CHILD_ROOT, dir.path())
                .env(RETRY, "1")
                .env(TARGET, target.to_string())
                .status()
                .unwrap();
        assert!(retry.success(), "{failpoint}");
        assert_eq!(v4_plan_sibling_count(dir.path()), 0, "{failpoint}");
        assert_eq!(crate::read_topology_generation(dir.path()).unwrap(), target);
        assert!(!dir.path().join(".graphforge-rewrite-v1.json").exists());
        let v4: crate::V4OrdinalIdentityManifest = serde_json::from_slice(
            &fs::read(dir.path().join(INDEX_DIR).join(V4_ORDINAL_MANIFEST)).unwrap(),
        )
        .unwrap();
        assert_eq!(v4.topology_generation, target, "{failpoint}");
        assert_eq!(
            receipt_manifest_digest(dir.path()),
            hex_sha256(&fs::read(dir.path().join(INDEX_DIR).join(MANIFEST)).unwrap())
        );
        assert_exact_v4_reopen(dir.path(), target);
    }

    for cleanup_failpoint in [
        "v4_plan_cleanup.before_unlink",
        "v4_plan_cleanup.after_unlink",
    ] {
        let (dir, _, _) = fixture();
        fs::write(
            dir.path().join("topology/generation.json"),
            b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
        )
        .unwrap();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        rebuild_v4_ordinal_identity(dir.path(), UuidIndexBuildLimits::default()).unwrap();

        let child = |failpoint: &str, retry: bool| {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                    .arg("--exact")
                    .arg(
                        "uuid_membership::tests::v4_rewrite_subprocess_crash_retry_matrix_cleans_exact_scratch",
                    )
                    .arg("--nocapture")
                    .env(CHILD_ROOT, dir.path())
                    .env(
                        "GRAPHFORGE_PROJECT_FAILPOINTS",
                        "graphforge-internal-subprocess-v1",
                    )
                    .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                    .env(TARGET, "8");
            if retry {
                command.env(RETRY, "1");
            }
            command.status().unwrap()
        };
        assert_eq!(
            child("v4_append.after_delta_artifacts", false).code(),
            Some(crate::project_failpoint::exit_code())
        );
        assert_eq!(v4_plan_sibling_count(dir.path()), 1);
        assert_eq!(
            child(cleanup_failpoint, false).code(),
            Some(crate::project_failpoint::exit_code()),
            "{cleanup_failpoint}"
        );
        assert!(child("", true).success(), "{cleanup_failpoint}");
        assert_eq!(v4_plan_sibling_count(dir.path()), 0, "{cleanup_failpoint}");
        assert_eq!(crate::read_topology_generation(dir.path()).unwrap(), 8);
        assert_exact_v4_reopen(dir.path(), 8);
    }
}

pub(super) fn write_uuid_parquet(path: &Path, column: &str, values: &[Uuid]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        column,
        DataType::FixedSizeBinary(16),
        false,
    )]));
    let array =
        FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.as_bytes().as_slice()))
            .unwrap();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)]).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

pub(super) fn write_node_parquet(path: &Path, values: &[Uuid]) {
    let ids = (1..=values.len() as u64).collect::<Vec<_>>();
    write_node_parquet_with_ids(path, values, &ids);
}

pub(super) fn write_node_parquet_with_ids(path: &Path, values: &[Uuid], ids: &[u64]) {
    assert_eq!(values.len(), ids.len());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("node_id", DataType::UInt64, false),
    ]));
    let uuids =
        FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.as_bytes().as_slice()))
            .unwrap();
    let ids = UInt64Array::from_iter_values(ids.iter().copied());
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(uuids), Arc::new(ids)]).unwrap();
    let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

pub(crate) fn fixture() -> (tempfile::TempDir, Vec<Uuid>, Vec<Uuid>) {
    let dir = tempfile::tempdir().unwrap();
    let nodes = vec![Uuid::from_u128(3), Uuid::from_u128(1), Uuid::from_u128(2)];
    let edges = vec![Uuid::from_u128(12), Uuid::from_u128(11)];
    write_node_parquet(&dir.path().join("topology/nodes.parquet"), &nodes);
    write_uuid_parquet(
        &dir.path().join("topology/edges/R.parquet"),
        "edge_uuid",
        &edges,
    );
    (dir, nodes, edges)
}

pub(super) fn install_test_v4_facet(
    project: &Path,
    generation: u64,
    nodes: &[Uuid],
) -> (Vec<String>, crate::AuthenticatedV4OrdinalIdentityAuthority) {
    let root = project.join(INDEX_DIR);
    fs::write(root.join("ordinal-v4.lock"), []).unwrap();
    let mappings = nodes.iter().copied().zip(1_u64..).collect::<Vec<_>>();
    let mut forward_mappings = mappings.clone();
    forward_mappings.sort_unstable_by_key(|(uuid, _)| *uuid);
    let forward_bytes = forward_mappings
        .iter()
        .flat_map(|(uuid, id)| uuid.as_bytes().iter().copied().chain(id.to_be_bytes()))
        .collect::<Vec<_>>();
    let ordinal_bytes = mappings
        .iter()
        .flat_map(|(uuid, _)| uuid.as_bytes().iter().copied())
        .collect::<Vec<_>>();
    let forward_digest = hex_sha256(&forward_bytes);
    let ordinal_digest = hex_sha256(&ordinal_bytes);
    let tombstone_bytes = (nodes.len() as u64).to_be_bytes();
    let tombstone_digest = hex_sha256(&tombstone_bytes);
    let forward_name = format!("forward-v4-{generation}-{}.uuidx", &forward_digest[..16]);
    let ordinal_name = format!("ordinal-v4-{generation}-{}.uuidx", &ordinal_digest[..16]);
    let tombstone_name = format!(
        "tombstones-v4-{generation}-{}.uuidx",
        &tombstone_digest[..16]
    );
    fs::write(root.join(&forward_name), &forward_bytes).unwrap();
    fs::write(root.join(&ordinal_name), &ordinal_bytes).unwrap();
    fs::write(root.join(&tombstone_name), tombstone_bytes).unwrap();
    let manifest = crate::V4OrdinalIdentityManifest {
        format_version: crate::ORDINAL_IDENTITY_V4,
        topology_generation: generation,
        forward_identities: vec![crate::V4OrdinalArtifact {
            name: forward_name.clone(),
            kind: crate::V4OrdinalArtifactKind::ForwardIdentities,
            generation,
            bytes: forward_bytes.len() as u64,
            sha256: forward_digest,
        }],
        ordinal_ranges: vec![crate::V4OrdinalRange {
            first_node_id: 1,
            count: nodes.len() as u64,
            artifact: crate::V4OrdinalArtifact {
                name: ordinal_name.clone(),
                kind: crate::V4OrdinalArtifactKind::OrdinalUuids,
                generation,
                bytes: ordinal_bytes.len() as u64,
                sha256: ordinal_digest,
            },
            blocks: vec![crate::V4OrdinalBlock {
                offset: 0,
                count: nodes.len() as u64,
                sha256: hex_sha256(&ordinal_bytes),
            }],
        }],
        tombstones: vec![crate::V4OrdinalTombstones {
            generation,
            artifact: crate::V4OrdinalArtifact {
                name: tombstone_name.clone(),
                kind: crate::V4OrdinalArtifactKind::NodeTombstones,
                generation,
                bytes: 8,
                sha256: tombstone_digest.clone(),
            },
            blocks: vec![crate::V4OrdinalTombstoneBlock {
                offset: 0,
                count: 1,
                first: nodes.len() as u64,
                last: nodes.len() as u64,
                sha256: tombstone_digest,
            }],
        }],
    };
    let body = serde_json::to_vec(&manifest).unwrap();
    fs::write(root.join(V4_ORDINAL_MANIFEST), &body).unwrap();
    let receipt = TopologyIndexReceipt {
        nonce: Uuid::new_v4().simple().to_string(),
        expected_generation: generation,
        topology_delta_sha256: hex_sha256(b"test-v4-facet"),
        manifest_sha256: hex_sha256(&body),
    };
    fs::write(
        root.join(V4_ORDINAL_RECEIPT),
        serde_json::to_vec(&receipt).unwrap(),
    )
    .unwrap();
    let authority = crate::AuthenticatedV4OrdinalIdentityAuthority {
        authority: crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
            topology_generation: generation,
            manifest_sha256: hex_sha256(&body),
        },
    };
    (vec![forward_name, ordinal_name, tombstone_name], authority)
}

#[test]
fn v4_orphan_cleanup_subprocess_crash_retry_preserves_authenticated_union() {
    const CHILD_ROOT: &str = "GRAPHFORGE_V4_ORPHAN_CHILD_ROOT";
    const RETRY: &str = "GRAPHFORGE_V4_ORPHAN_RETRY";
    if let Ok(root) = std::env::var(CHILD_ROOT) {
        let root = Path::new(&root);
        let manifest_bytes = fs::read(root.join(INDEX_DIR).join(V4_ORDINAL_MANIFEST)).unwrap();
        let receipt: TopologyIndexReceipt = serde_json::from_slice(
            &fs::read(root.join(INDEX_DIR).join(V4_ORDINAL_RECEIPT)).unwrap(),
        )
        .unwrap();
        assert_eq!(receipt.manifest_sha256, hex_sha256(&manifest_bytes));
        let authority = crate::AuthenticatedV4OrdinalIdentityAuthority {
            authority: crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
                topology_generation: receipt.expected_generation,
                manifest_sha256: receipt.manifest_sha256,
            },
        };
        maintain_uuid_membership_orphans_with_ordinal_authority(root, 16, Some(&authority))
            .unwrap();
        if std::env::var(RETRY).is_err() {
            panic!("configured v4 orphan cleanup failpoint did not terminate the process");
        }
        return;
    }

    for failpoint in ["v4_cleanup.before_unlink", "v4_cleanup.after_unlink"] {
        let (dir, nodes, _) = fixture();
        fs::write(
            dir.path().join("topology/generation.json"),
            b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
        )
        .unwrap();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
        let (referenced, _) = install_test_v4_facet(dir.path(), 7, &nodes);
        let orphan = dir
            .path()
            .join(INDEX_DIR)
            .join("forward-v4-7-0000000000000000.uuidx");
        fs::write(&orphan, b"authenticated-orphan-candidate").unwrap();

        let child = |retry: bool| {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                    .arg("--exact")
                    .arg(
                        "uuid_membership::tests::v4_orphan_cleanup_subprocess_crash_retry_preserves_authenticated_union",
                    )
                    .arg("--nocapture")
                    .env(CHILD_ROOT, dir.path());
            if retry {
                command.env(RETRY, "1");
            } else {
                command
                    .env(
                        "GRAPHFORGE_PROJECT_FAILPOINTS",
                        "graphforge-internal-subprocess-v1",
                    )
                    .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint);
            }
            command.status().unwrap()
        };
        assert_eq!(
            child(false).code(),
            Some(crate::project_failpoint::exit_code()),
            "{failpoint}"
        );
        assert!(child(true).success(), "{failpoint}");
        assert!(!orphan.exists(), "{failpoint}");
        for name in &referenced {
            assert!(
                dir.path().join(INDEX_DIR).join(name).is_file(),
                "{failpoint}"
            );
        }
        assert_exact_v4_reopen_values(
            dir.path(),
            7,
            vec![Some(Uuid::from_u128(3)), Some(Uuid::from_u128(1)), None],
        );
    }
}

pub(super) fn receipt_manifest_digest(project: &Path) -> String {
    let root = project.join(INDEX_DIR);
    let receipt: TopologyIndexReceipt =
        serde_json::from_slice(&fs::read(root.join(TOPOLOGY_RECEIPT)).unwrap()).unwrap();
    assert_eq!(
        receipt.manifest_sha256,
        hex_sha256(&fs::read(root.join(MANIFEST)).unwrap())
    );
    receipt.manifest_sha256
}

pub(super) fn make_installed_manifest_stale(project: &Path) -> String {
    let path = project.join(INDEX_DIR).join(MANIFEST);
    let mut manifest: Manifest = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    manifest.live_node_count = manifest.live_node_count.saturating_add(17);
    let body = serde_json::to_vec(&manifest).unwrap();
    fs::write(path, &body).unwrap();
    hex_sha256(&body)
}

#[test]
fn stale_v3_migration_receipt_survives_crash_roll_forward() {
    const CHILD_ROOT: &str = "GRAPHFORGE_UUID_MIGRATION_CHILD_ROOT";
    if let Ok(root) = std::env::var(CHILD_ROOT) {
        let _ = rebuild_uuid_membership_indexes(Path::new(&root), UuidIndexBuildLimits::default());
        panic!("child migration failpoint did not terminate the process");
    }

    let (dir, _, _) = fixture();
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    let stale_digest = make_installed_manifest_stale(dir.path());
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("uuid_membership::tests::stale_v3_migration_receipt_survives_crash_roll_forward")
        .arg("--nocapture")
        .env(CHILD_ROOT, dir.path())
        .env(
            "GRAPHFORGE_PROJECT_FAILPOINTS",
            "graphforge-internal-subprocess-v1",
        )
        .env(
            "GRAPHFORGE_PROJECT_FAILPOINT",
            "rewrite.after_durable_intent",
        )
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));

    assert_eq!(crate::read_topology_generation(dir.path()).unwrap(), 0);
    assert_eq!(crate::read_search_generation(dir.path()).unwrap(), 0);
    let installed_digest = receipt_manifest_digest(dir.path());
    assert_ne!(installed_digest, stale_digest);
    assert!(!dir.path().join(".graphforge-rewrite-v1.json").exists());
}

#[test]
fn owned_manifest_suspension_restores_exact_authority_and_rejects_tampering() {
    let (dir, _, _) = fixture();
    rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();
    let mut snapshot = AuthenticatedUuidIndexSnapshot::open_at_generation(dir.path(), 0).unwrap();
    let identity = snapshot.manifest_identity;
    let path = dir.path().join(INDEX_DIR).join(MANIFEST);
    let original = fs::read(&path).unwrap();
    snapshot.suspend_owned_manifest();
    assert!(snapshot.revalidate().is_err());
    snapshot.restore_owned_manifest().unwrap();
    assert_eq!(snapshot.manifest_identity, identity);
    snapshot.revalidate().unwrap();
    snapshot.suspend_owned_manifest();
    fs::write(&path, [original.as_slice(), b"\n"].concat()).unwrap();
    assert!(snapshot.restore_owned_manifest().is_err());
    assert!(snapshot.manifest_file.is_none());
    fs::write(&path, &original).unwrap();
    snapshot.restore_owned_manifest().unwrap();
    snapshot.suspend_owned_manifest();
    let replacement = path.with_extension("replacement");
    fs::write(&replacement, original).unwrap();
    fs::rename(&replacement, &path).unwrap();
    assert!(snapshot.restore_owned_manifest().is_err());
}

#[test]
fn owned_manifest_returned_error_restores_snapshot_for_same_process_retry() {
    let dir = tempfile::tempdir().unwrap();
    let mut snapshot = None;
    let make_batch = || {
        let mut batch = crate::RewriteBatch::new();
        batch
            .stage_bytes(&dir.path().join("topology/nodes.parquet"), b"fixture")
            .unwrap();
        batch
    };
    let first = Uuid::from_u128(201);
    let second = Uuid::from_u128(202);
    commit_uuid_topology_rewrite(
        dir.path(),
        make_batch(),
        &UuidTopologyDelta {
            nodes: vec![(first, 1)],
            edges: Vec::new(),
            deleted_nodes: Vec::new(),
            deleted_edges: Vec::new(),
        },
        &mut snapshot,
    )
    .unwrap();
    let old = snapshot.as_ref().unwrap().manifest_identity;
    FAIL_AFTER_MANIFEST_SUSPEND.set(true);
    let delta = UuidTopologyDelta {
        nodes: vec![(second, 2)],
        edges: Vec::new(),
        deleted_nodes: Vec::new(),
        deleted_edges: Vec::new(),
    };
    let error = commit_uuid_topology_rewrite(dir.path(), make_batch(), &delta, &mut snapshot)
        .err()
        .unwrap();
    assert!(error.to_string().contains("injected manifest suspension"));
    let restored = snapshot.as_ref().unwrap();
    assert_eq!(restored.manifest_identity, old);
    restored.revalidate().unwrap();
    assert_eq!(crate::read_topology_generation(dir.path()).unwrap(), 1);
    commit_uuid_topology_rewrite(dir.path(), make_batch(), &delta, &mut snapshot).unwrap();
    assert_eq!(
        snapshot
            .as_mut()
            .unwrap()
            .lookup_node_surrogates(&[first, second])
            .unwrap()
            .0,
        [Some(1), Some(2)]
    );
}

#[test]
fn owned_manifest_same_writer_successive_flushes_preserve_incremental_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let first = Uuid::from_u128(101);
    let second = Uuid::from_u128(102);
    let mut writer =
        crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Strict, 1).unwrap();
    writer
        .create_node(first, graphforge_value::EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer.flush().unwrap();
    let path = dir.path().join(INDEX_DIR).join(MANIFEST);
    let old = graphforge_filesystem::path_identity(&path).unwrap();
    writer
        .create_node(second, graphforge_value::EntityTypeId::decode(0).unwrap())
        .unwrap();
    writer
        .create_edge(Uuid::from_u128(103), "CON", &first, &second)
        .unwrap();
    writer.flush().unwrap();
    assert_ne!(graphforge_filesystem::path_identity(&path).unwrap(), old);
    assert_eq!(crate::read_topology_generation(dir.path()).unwrap(), 2);
    assert_eq!(
        writer
            .topology_write_work()
            .uuid_prior_topology_rows_decoded,
        0
    );
    let mut index = UuidMembershipIndex::open(dir.path()).unwrap();
    assert_eq!(
        index.lookup_node_surrogates(&[first, second]).unwrap().0,
        [Some(1), Some(2)]
    );
}

#[test]
fn readonly_shared_runs_preserve_uuid_snapshot_authentication() {
    for scenario in [
        "valid",
        "writable",
        "made_writable",
        "tampered",
        "replaced",
        "manifest_link",
    ] {
        let source = tempfile::tempdir().unwrap();
        let aliases = tempfile::tempdir().unwrap();
        let nodes = [(Uuid::from_u128(1), 1), (Uuid::from_u128(2), u64::MAX - 1)];
        crate::generation::force_bump_topology_generation_for_test(source.path()).unwrap();
        write_node_parquet_with_ids(
            &source.path().join("topology/nodes.parquet"),
            &nodes.map(|(uuid, _)| uuid),
            &nodes.map(|(_, surrogate)| surrogate),
        );
        rebuild_uuid_membership_indexes(source.path(), UuidIndexBuildLimits::default()).unwrap();
        let root = source.path().join(INDEX_DIR);
        let manifest: Manifest =
            serde_json::from_slice(&fs::read(root.join(MANIFEST)).unwrap()).unwrap();
        let records = manifest
            .runs
            .iter()
            .flat_map(|run| [run.identities.clone(), run.node_surrogates.clone()])
            .collect::<Vec<_>>();
        let record = records
            .iter()
            .find(|record| record.name.starts_with("identities-v5") && record.count > 0)
            .unwrap();
        let original_permissions = fs::metadata(root.join(&record.name)).unwrap().permissions();
        for record in &records {
            let path = root.join(&record.name);
            fs::hard_link(&path, aliases.path().join(&record.name)).unwrap();
            if scenario != "writable" {
                let mut permissions = fs::metadata(&path).unwrap().permissions();
                permissions.set_readonly(true);
                fs::set_permissions(&path, permissions).unwrap();
            }
        }
        if scenario == "manifest_link" {
            fs::hard_link(root.join(MANIFEST), aliases.path().join(MANIFEST)).unwrap();
        }
        // Match hydration: establish aliases before retaining immutable
        // handles. Windows prevents adding links through held handles that
        // intentionally deny DELETE sharing.
        let mut snapshot =
            AuthenticatedUuidIndexSnapshot::open_at_generation(source.path(), 1).unwrap();
        match scenario {
            "valid" => {
                snapshot.revalidate().unwrap();
                snapshot.open_retained_file(record).unwrap();
                let (values, _) = snapshot
                    .lookup_node_surrogates(&[nodes[0].0, nodes[1].0])
                    .unwrap();
                assert_eq!(values, [Some(1), Some(u64::MAX - 1)]);
            }
            "writable" => {
                assert!(snapshot.revalidate().is_err());
                assert!(snapshot.open_retained_file(record).is_err());
            }
            "made_writable" => {
                snapshot.revalidate().unwrap();
                fs::set_permissions(
                    aliases.path().join(&record.name),
                    original_permissions.clone(),
                )
                .unwrap();
                assert!(snapshot.revalidate().is_err());
                assert!(snapshot.open_retained_file(record).is_err());
            }
            "tampered" => {
                let alias = aliases.path().join(&record.name);
                fs::set_permissions(&alias, original_permissions.clone()).unwrap();
                let mut writer = fs::OpenOptions::new().write(true).open(&alias).unwrap();
                writer.write_all(&[0xff]).unwrap();
                writer.sync_all().unwrap();
                drop(writer);
                let mut permissions = original_permissions.clone();
                permissions.set_readonly(true);
                fs::set_permissions(&alias, permissions).unwrap();
                // Metadata and inode still match. The retained read must
                // authenticate bytes, not trust readonly status alone.
                snapshot.revalidate().unwrap();
                let error = snapshot.lookup_node_surrogates(&[nodes[0].0]).unwrap_err();
                assert!(
                    error.to_string().contains("block authentication"),
                    "{error}"
                );
                assert!(
                    AuthenticatedUuidIndexSnapshot::open_at_generation(source.path(), 1).is_err()
                );
            }
            "replaced" => {
                let path = root.join(&record.name);
                let bytes = fs::read(&path).unwrap();
                let replacement = fs::rename(&path, root.join("held-original"));
                #[cfg(windows)]
                {
                    // Stable retained handles intentionally omit
                    // FILE_SHARE_DELETE: Windows prevents replacement.
                    assert!(replacement.is_err());
                    assert_eq!(fs::read(&path).unwrap(), bytes);
                    snapshot.revalidate().unwrap();
                }
                #[cfg(not(windows))]
                {
                    replacement.unwrap();
                    fs::write(&path, bytes).unwrap();
                    assert!(snapshot.revalidate().is_err());
                }
            }
            "manifest_link" => {
                assert!(snapshot.revalidate().is_err());
            }
            _ => unreachable!(),
        }
        // Restore this test's owned aliases so Windows cleanup can remove
        // readonly files; production never mutates shared run permissions.
        for record in &records {
            fs::set_permissions(
                aliases.path().join(&record.name),
                original_permissions.clone(),
            )
            .unwrap();
        }
    }
}

pub(super) fn singleton_append_series(batches: u64) -> (tempfile::TempDir, u64) {
    let dir = tempfile::tempdir().unwrap();
    let mut bytes = 0;
    for generation in 1..=batches {
        crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
        let metrics = append_uuid_membership_delta(
            dir.path(),
            generation,
            &[(Uuid::from_u128(u128::from(generation)), generation)],
            &[],
        )
        .unwrap();
        assert_eq!(metrics.prior_topology_rows_decoded, 0);
        assert!(metrics.write_blocks >= 2);
        assert_eq!(metrics.write_bytes, metrics.physical_bytes_written);
        bytes += metrics.physical_bytes_written;
    }
    (dir, bytes)
}

#[test]
fn packed_membership_rebuild_reopen_and_tombstone_preserve_full_width_ids() {
    let dir = tempfile::tempdir().unwrap();
    let nodes = [
        Uuid::from_u128(2),
        Uuid::from_u128(u128::MAX - 1),
        Uuid::from_u128(u128::MAX),
    ];
    let ids = [u64::from(u32::MAX), u64::from(u32::MAX) + 1, u64::MAX];
    write_node_parquet_with_ids(&dir.path().join("topology/nodes.parquet"), &nodes, &ids);
    let edge = Uuid::from_u128(1);
    write_uuid_parquet(
        &dir.path().join("topology/edges/R.parquet"),
        "edge_uuid",
        &[edge],
    );
    rebuild_uuid_membership_indexes(
        dir.path(),
        UuidIndexBuildLimits {
            scan_batch_rows: 1,
            run_records: 1,
            merge_fan_in: 2,
        },
    )
    .unwrap();
    let mut index = UuidMembershipIndex::open(dir.path()).unwrap();
    assert_eq!(
        index.lookup_node_surrogates(&nodes).unwrap().0,
        ids.map(Some)
    );
    assert_eq!(index.probe(UuidIndexKind::Edge, &[edge]).unwrap().0, [true]);
    drop(index);
    crate::generation::force_bump_topology_generation_for_test(dir.path()).unwrap();
    append_uuid_membership_delta_with_tombstones(
        dir.path(),
        1,
        &[],
        &[],
        &[(nodes[2], u64::MAX)],
        &[],
    )
    .unwrap();
    let mut index = UuidMembershipIndex::open(dir.path()).unwrap();
    assert_eq!(
        index.lookup_node_surrogates(&nodes).unwrap().0,
        [Some(ids[0]), Some(ids[1]), None]
    );
    assert_eq!(index.probe(UuidIndexKind::Edge, &[edge]).unwrap().0, [true]);
}

#[test]
fn v4_rebuild_subprocess_crash_retry_selects_one_complete_authority() {
    const CHILD_ROOT: &str = "GRAPHFORGE_V4_REBUILD_CHILD_ROOT";
    const RETRY: &str = "GRAPHFORGE_V4_REBUILD_RETRY";
    if let Ok(root) = std::env::var(CHILD_ROOT) {
        let root = Path::new(&root);
        crate::durable_rewrite::recover(root).unwrap();
        if !root.join(INDEX_DIR).join(V4_ORDINAL_MANIFEST).exists() {
            rebuild_v4_ordinal_identity_with_evidence(root, UuidIndexBuildLimits::default())
                .unwrap();
        }
        if std::env::var(RETRY).is_err() {
            panic!("configured v4 rebuild failpoint did not terminate the process");
        }
        return;
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum PreRecoveryDisposition {
        PriorV3Only,
        IncompleteV4,
        CompleteV4,
    }

    for (failpoint, expected, expected_installed) in [
        (
            "rewrite.before_intent",
            PreRecoveryDisposition::PriorV3Only,
            0,
        ),
        (
            "rewrite.after_preparing_disarm",
            PreRecoveryDisposition::PriorV3Only,
            0,
        ),
        (
            "rewrite.after_durable_intent",
            PreRecoveryDisposition::PriorV3Only,
            0,
        ),
        (
            "rewrite.after_first_data_install",
            PreRecoveryDisposition::IncompleteV4,
            1,
        ),
        (
            "rewrite.after_middle_data_install",
            PreRecoveryDisposition::IncompleteV4,
            2,
        ),
        (
            "rewrite.after_last_data_install",
            PreRecoveryDisposition::IncompleteV4,
            usize::MAX,
        ),
        (
            "rewrite.before_generation_authority",
            PreRecoveryDisposition::IncompleteV4,
            usize::MAX,
        ),
        (
            "rewrite.after_generation_authority",
            PreRecoveryDisposition::CompleteV4,
            usize::MAX,
        ),
    ] {
        let (dir, _, _) = fixture();
        fs::write(
            dir.path().join("topology/generation.json"),
            b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
        )
        .unwrap();
        rebuild_uuid_membership_indexes(dir.path(), UuidIndexBuildLimits::default()).unwrap();

        let child = |retry: bool| {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                    .arg("--exact")
                    .arg(
                        "uuid_membership::tests::v4_rebuild_subprocess_crash_retry_selects_one_complete_authority",
                    )
                    .arg("--nocapture")
                    .env(CHILD_ROOT, dir.path());
            if retry {
                command.env(RETRY, "1");
            } else {
                command
                    .env(
                        "GRAPHFORGE_PROJECT_FAILPOINTS",
                        "graphforge-internal-subprocess-v1",
                    )
                    .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint);
            }
            command.status().unwrap()
        };
        assert_eq!(
            child(false).code(),
            Some(crate::project_failpoint::exit_code()),
            "{failpoint}"
        );
        // Inspect the crashed tree before recovery. The prior v3 authority
        // must remain valid at every boundary; v4 is either absent,
        // incomplete and therefore inadmissible, or already complete.
        UuidMembershipIndex::open_at_generation(dir.path(), 7).unwrap();
        let journal_path = dir.path().join(".graphforge-rewrite-v1.json");
        let journal = fs::read(&journal_path)
            .ok()
            .map(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).unwrap());
        let entries = journal
            .as_ref()
            .and_then(|value| value.get("entries"))
            .and_then(serde_json::Value::as_array);
        let intended_v4 = entries
            .into_iter()
            .flatten()
            .filter(|entry| {
                entry["class"] == "data"
                    && entry["destination"]
                        .as_str()
                        .is_some_and(|path| path.starts_with(INDEX_DIR))
            })
            .collect::<Vec<_>>();
        let installed_v4 = intended_v4
            .iter()
            .filter(|entry| {
                let path = dir.path().join(entry["destination"].as_str().unwrap());
                let expected = entry["temporary_file"].as_str().unwrap();
                File::open(path)
                    .ok()
                    .and_then(|file| graphforge_filesystem::file_identity(&file).ok())
                    .is_some_and(|identity| {
                        identity
                            .file_id
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>()
                            == expected
                    })
            })
            .count();
        if expected_installed == usize::MAX {
            assert_eq!(installed_v4, intended_v4.len(), "{failpoint}");
        } else {
            assert_eq!(
                installed_v4, expected_installed,
                "{failpoint}: journal={journal:?} intended={intended_v4:?}"
            );
        }
        let generation_selected = entries
            .into_iter()
            .flatten()
            .find(|entry| entry["class"] == "generation_authority")
            .is_some_and(|entry| {
                let expected = entry["temporary_file"].as_str().unwrap();
                File::open(dir.path().join("topology/generation.json"))
                    .ok()
                    .and_then(|file| graphforge_filesystem::file_identity(&file).ok())
                    .is_some_and(|identity| {
                        identity
                            .file_id
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>()
                            == expected
                    })
            });
        let index = dir.path().join(INDEX_DIR);
        let coherent_v4 = match (
            fs::read(index.join(V4_ORDINAL_MANIFEST)).ok(),
            fs::read(index.join(V4_ORDINAL_RECEIPT)).ok(),
        ) {
            (Some(manifest), Some(receipt)) => {
                let receipt: TopologyIndexReceipt = serde_json::from_slice(&receipt).unwrap();
                assert_eq!(receipt.expected_generation, 7, "{failpoint}");
                assert_eq!(
                    receipt.manifest_sha256,
                    hex_sha256(&manifest),
                    "{failpoint}"
                );
                let authority = crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
                    topology_generation: 7,
                    manifest_sha256: receipt.manifest_sha256,
                };
                matches!(
                    crate::ordinal_identity_v4::V4OrdinalIdentityHandle::open(
                        dir.path(),
                        &authority,
                        crate::V4OrdinalIdentityLimits::default()
                    ),
                    Ok(crate::V4OrdinalIdentityOpen::Ready(_))
                )
            }
            _ => false,
        };
        let observed = if generation_selected && coherent_v4 {
            PreRecoveryDisposition::CompleteV4
        } else if installed_v4 == 0 {
            PreRecoveryDisposition::PriorV3Only
        } else {
            PreRecoveryDisposition::IncompleteV4
        };
        assert_eq!(observed, expected, "{failpoint}");

        assert!(child(true).success(), "{failpoint}");
        assert!(!dir.path().join(".graphforge-rewrite-v1.json").exists());
        assert_eq!(manifest_generation(dir.path()).unwrap(), Some(7));
        UuidMembershipIndex::open(dir.path()).unwrap();

        let manifest_bytes = fs::read(index.join(V4_ORDINAL_MANIFEST)).unwrap();
        let receipt_bytes = fs::read(index.join(V4_ORDINAL_RECEIPT)).unwrap();
        let receipt: TopologyIndexReceipt = serde_json::from_slice(&receipt_bytes).unwrap();
        assert_eq!(receipt.expected_generation, 7);
        assert_eq!(receipt.manifest_sha256, hex_sha256(&manifest_bytes));
        let generation = crate::durable_rewrite::GenerationPair {
            topology: 7,
            search: 0,
            property: 0,
        };
        assert_eq!(
            crate::durable_rewrite::reconcile_auxiliary(
                dir.path(),
                generation,
                generation,
                &crate::AuxiliaryReceipt {
                    kind: "uuid-membership/v4".to_owned(),
                    schema_version: crate::ORDINAL_IDENTITY_V4,
                    path: format!("{INDEX_DIR}/{V4_ORDINAL_RECEIPT}"),
                    digest: hex_sha256(&receipt_bytes),
                    bytes: u64::try_from(receipt_bytes.len()).unwrap(),
                },
            )
            .unwrap(),
            crate::durable_rewrite::AuxiliaryReconcileOutcome::Committed
        );
        let authority = crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
            topology_generation: 7,
            manifest_sha256: receipt.manifest_sha256,
        };
        assert!(matches!(
            crate::ordinal_identity_v4::V4OrdinalIdentityHandle::open(
                dir.path(),
                &authority,
                crate::V4OrdinalIdentityLimits::default()
            ),
            Ok(crate::V4OrdinalIdentityOpen::Ready(_))
        ));
    }
}
