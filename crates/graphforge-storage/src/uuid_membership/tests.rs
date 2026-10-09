use super::maintain_uuid_membership_orphans_with_ordinal_authority;
use super::rebuild::rebuild_v4_ordinal_identity;
use super::rebuild::rebuild_v4_ordinal_identity_with_evidence;
use super::topology_delta::commit_uuid_topology_rewrite;
use super::topology_delta::hex_sha256;
use super::topology_delta::V4_PLAN_PREFIX;
use super::topology_delta::V4_PLAN_ROOT;
use super::TopologyIndexReceipt;
use super::UuidIndexBuildLimits;
use super::UuidTopologyDelta;
use super::INDEX_DIR;
use super::V4_ORDINAL_MANIFEST;
use super::V4_ORDINAL_RECEIPT;
use arrow::array::FixedSizeBinaryArray;
use arrow::array::UInt64Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs;
use std::fs::File;
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
        xxh64: crate::corruption_checksum::checksum(bytes),
    }
}

pub(super) fn assert_no_v4_temporary(index: &graphforge_filesystem::StableDirectory) {
    assert!(index
        .child_names()
        .unwrap()
        .into_iter()
        .all(|name| !name.to_string_lossy().starts_with(".v4-")));
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
            hex_sha256(&fs::read(dir.path().join(INDEX_DIR).join(V4_ORDINAL_MANIFEST)).unwrap())
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
    fs::create_dir_all(dir.path().join(INDEX_DIR)).unwrap();
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
            xxh64: crate::corruption_checksum::checksum(&forward_bytes),
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
                xxh64: crate::corruption_checksum::checksum(&ordinal_bytes),
            },
            blocks: vec![crate::V4OrdinalBlock {
                offset: 0,
                count: nodes.len() as u64,
                xxh64: crate::corruption_checksum::checksum(&ordinal_bytes),
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
                xxh64: crate::corruption_checksum::checksum(&tombstone_bytes),
            },
            blocks: vec![crate::V4OrdinalTombstoneBlock {
                offset: 0,
                count: 1,
                first: nodes.len() as u64,
                last: nodes.len() as u64,
                xxh64: crate::corruption_checksum::checksum(&tombstone_bytes),
            }],
        }],
        uuid_order_matches_ordinals: None,
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
        serde_json::from_slice(&fs::read(root.join(V4_ORDINAL_RECEIPT)).unwrap()).unwrap();
    assert_eq!(
        receipt.manifest_sha256,
        hex_sha256(&fs::read(root.join(V4_ORDINAL_MANIFEST)).unwrap())
    );
    receipt.manifest_sha256
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
        // Inspect the crashed tree before recovery. v4 is either absent,
        // incomplete and therefore inadmissible, or already complete.
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
                    kind: "uuid-membership/ordinal-v6".to_owned(),
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
