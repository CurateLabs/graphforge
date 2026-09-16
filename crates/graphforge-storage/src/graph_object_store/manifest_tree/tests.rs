use super::*;
use crate::graph_object_store::AuthenticatedGraphFile;
use crate::graph_object_store::GRAPH_OBJECTS_DIR;
use crate::graph_object_store::GRAPH_RADIX_DEPTH;
use crate::graph_object_store::GraphFilesRootV2;
use crate::graph_object_store::GraphManifestNodeKind;
use crate::graph_object_store::PathBuf;
use crate::graph_object_store::SHA256_DIR;
use crate::graph_object_store::Sha256;
use crate::graph_object_store::begin_graph_object_publication;
use crate::graph_object_store::gc_graph_objects;
use crate::graph_object_store::graph_object_path;
use crate::graph_object_store::hex_digest;
use crate::graph_object_store::read_graph_object;
use crate::graph_object_store::read_graph_object_by_digest;
use crate::graph_object_store::verify_graph_object;

#[test]
fn migrates_v1_tree_once_and_reopens_from_compact_root() {
    let container = tempfile::tempdir().unwrap();
    let graph = tempfile::tempdir().unwrap();
    fs::create_dir_all(graph.path().join("topology/edges/knows")).unwrap();
    fs::write(
        graph.path().join("topology/edges/knows/1-1.parquet"),
        b"edge",
    )
    .unwrap();
    let (inventory, _) = crate::capture_graph_files(graph.path()).unwrap();
    let lease = begin_graph_object_publication(container.path()).unwrap();
    let (root, evidence) = migrate_graph_files_v1_to_v2(&lease, graph.path(), &inventory).unwrap();
    assert_eq!(evidence.payload_objects, 1);
    assert_eq!(evidence.payload_bytes_hashed, 4);
    let (files, _) =
        crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
            read_graph_object_by_digest(container.path(), digest, 1024 * 1024)
        })
        .unwrap();
    assert_eq!(files, inventory.files);
}

#[cfg(unix)]
#[test]
fn migrates_windows_authored_v1_path_into_canonical_v2_manifest() {
    let container = tempfile::tempdir().unwrap();
    let graph = tempfile::tempdir().unwrap();
    let source = graph.path().join("topology/nodes.parquet");
    fs::create_dir_all(source.parent().unwrap()).unwrap();
    fs::write(&source, b"nodes").unwrap();
    let inventory = crate::GraphFilesInventory {
        format: "graphforge-graph-files".into(),
        format_version: 1,
        files: vec![crate::GraphFileEntry {
            relative_path: "topology\\nodes.parquet".into(),
            byte_length: 5,
            content_sha256: hex_digest(Sha256::digest(b"nodes").into()),
            role: crate::GraphFileRole::Topology,
        }],
        file_count: 1,
        total_byte_length: 5,
    };
    let lease = begin_graph_object_publication(container.path()).unwrap();
    let (root, _) = migrate_graph_files_v1_to_v2(&lease, graph.path(), &inventory).unwrap();
    let (files, _) =
        crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
            read_graph_object_by_digest(container.path(), digest, 1024 * 1024)
        })
        .unwrap();
    assert_eq!(files[0].relative_path, "topology/nodes.parquet");
}

#[test]
fn migration_rejects_noncanonical_v1_before_installing_objects() {
    let graph = tempfile::tempdir().unwrap();
    fs::write(graph.path().join("a.parquet"), b"a").unwrap();
    fs::write(graph.path().join("b.parquet"), b"bb").unwrap();
    let (valid, _) = crate::capture_graph_files(graph.path()).unwrap();
    let mut invalid = Vec::new();
    let mut wrong_count = valid.clone();
    wrong_count.file_count += 1;
    invalid.push(wrong_count);
    let mut wrong_total = valid.clone();
    wrong_total.total_byte_length += 1;
    invalid.push(wrong_total);
    let mut unordered = valid.clone();
    unordered.files.reverse();
    invalid.push(unordered);
    let mut duplicate = valid.clone();
    duplicate.files[1] = duplicate.files[0].clone();
    duplicate.total_byte_length = duplicate.files.iter().map(|entry| entry.byte_length).sum();
    invalid.push(duplicate);

    for inventory in invalid {
        let container = tempfile::tempdir().unwrap();
        let lease = begin_graph_object_publication(container.path()).unwrap();
        assert!(migrate_graph_files_v1_to_v2(&lease, graph.path(), &inventory,).is_err());
        let digest_root = container.path().join(GRAPH_OBJECTS_DIR).join(SHA256_DIR);
        assert_eq!(
            fs::read_dir(digest_root)
                .unwrap()
                .map(Result::unwrap)
                .count(),
            0
        );
    }
}

#[test]
fn repeated_v2_appends_examine_only_changed_descriptors() {
    let container = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let lease = begin_graph_object_publication(container.path()).unwrap();
    let mut state = GraphManifestState::empty();
    for ordinal in 0_u8..8 {
        let relative = PathBuf::from(format!(
            "topology/edges/knows/{ordinal:020}-{ordinal:020}.parquet"
        ));
        let path = workspace.path().join(&relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, [ordinal]).unwrap();
        let (root, evidence) =
            append_graph_files_v2(&lease, workspace.path(), &mut state, &[relative], &[]).unwrap();
        assert_eq!(evidence.changed_entries_examined, 1);
        assert_eq!(evidence.prior_entries_examined, 0);
        assert_eq!(state.root(), Some(&root));
    }
    let root = state.root().unwrap().clone();
    let (resolved, evidence) =
        crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
            read_graph_object_by_digest(container.path(), digest, 1024 * 1024)
        })
        .unwrap();
    assert_eq!(resolved.len(), 8);
    assert!(evidence.segments_examined <= 1 + u64::from(GRAPH_RADIX_DEPTH) * 8);

    let deleted = "topology/edges/knows/00000000000000000003-00000000000000000003.parquet";
    let (root, delete_evidence) =
        append_graph_files_v2(&lease, workspace.path(), &mut state, &[], &[deleted.into()])
            .unwrap();
    assert_eq!(delete_evidence.changed_entries_examined, 1);
    assert_eq!(delete_evidence.prior_entries_examined, 0);
    let (resolved, _) =
        crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
            read_graph_object_by_digest(container.path(), digest, 1024 * 1024)
        })
        .unwrap();
    assert_eq!(resolved.len(), 7);
    assert!(!resolved.iter().any(|entry| entry.relative_path == deleted));
}

#[test]
fn bounded_bucket_update_and_lookup_budgets_do_not_scale_with_inventory() {
    for count in [128_usize, 256, 512] {
        let container = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let paths = (0..count)
            .map(|index| PathBuf::from(format!("payload-{index:06}.parquet")))
            .collect::<Vec<_>>();
        for path in &paths {
            fs::write(workspace.path().join(path), b"initial").unwrap();
        }
        let mut state = GraphManifestState::empty();
        let (original, _) =
            append_graph_files_v2(&lease, workspace.path(), &mut state, &paths, &[]).unwrap();
        fs::write(workspace.path().join(&paths[count / 2]), b"replacement").unwrap();
        let (replaced, update) = append_graph_files_v2(
            &lease,
            workspace.path(),
            &mut state,
            &paths[count / 2..count / 2 + 1],
            &[],
        )
        .unwrap();
        assert_eq!(update.prior_entries_examined, 0);
        assert_eq!(update.changed_entries_examined, 1);
        // This deterministic representative ladder requires at most four radix
        // reads/writes per replacement, independent of inventory doubling.
        assert!(update.publication_io.manifest_reads.read_calls <= 4);
        assert!(update.publication_io.manifest.installed_objects <= 4);
        for root in [&original, &replaced] {
            for path in [paths[count / 2].to_str().unwrap(), "absent-payload.parquet"] {
                let mut reads = 0;
                let found = crate::graph_manifest::resolve_manifest_entry(
                    root,
                    path,
                    crate::GraphManifestLimits::default(),
                    |digest| {
                        reads += 1;
                        read_graph_object_by_digest(
                            container.path(),
                            digest,
                            crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
                        )
                    },
                )
                .unwrap();
                assert!(reads <= 4);
                assert_eq!(found.is_some(), path != "absent-payload.parquet");
                if let Some(entry) = found {
                    assert_eq!(entry.byte_length, if root == &original { 7 } else { 11 });
                }
            }
        }
        let removed = paths[count / 2].to_str().unwrap().to_owned();
        let (_, deletion) =
            append_graph_files_v2(&lease, workspace.path(), &mut state, &[], &[removed]).unwrap();
        assert_eq!(deletion.prior_entries_examined, 0);
        // Four ancestors plus at most sixteen bounded collapse probes each;
        // this is logical application I/O, not process memory or OS I/O.
        assert!(deletion.publication_io.manifest_reads.read_calls <= 68);
        assert!(deletion.publication_io.manifest.installed_objects <= 4);
        println!(
            "BUCKET_UPDATE_BUDGET entries={count} replacement_reads={} replacement_installs={} deletion_reads={}",
            update.publication_io.manifest_reads.read_calls,
            update.publication_io.manifest.installed_objects,
            deletion.publication_io.manifest_reads.read_calls
        );
    }
}

#[test]
fn bounded_bucket_split_collapse_and_replacement_preserve_old_roots() {
    let container = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let lease = begin_graph_object_publication(container.path()).unwrap();
    let paths = (0..9)
        .map(|index| PathBuf::from(format!("payload-{index}.parquet")))
        .collect::<Vec<_>>();
    for (index, path) in paths.iter().enumerate() {
        fs::write(workspace.path().join(path), [u8::try_from(index).unwrap()]).unwrap();
    }
    let mut state = GraphManifestState::empty();
    let (eight, _) =
        append_graph_files_v2(&lease, workspace.path(), &mut state, &paths[..8], &[]).unwrap();
    let read = |root: &GraphFilesRootV2| {
        let bytes = read_graph_object_by_digest(
            container.path(),
            &root.root_node_sha256,
            crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
        )
        .unwrap();
        crate::decode_graph_manifest_node(&bytes).unwrap()
    };
    assert!(
        matches!(read(&eight).kind, GraphManifestNodeKind::Bucket { entries } if entries.len() == 8)
    );
    let (nine, split) =
        append_graph_files_v2(&lease, workspace.path(), &mut state, &paths[8..], &[]).unwrap();
    assert!(matches!(
        read(&nine).kind,
        GraphManifestNodeKind::Branch { .. }
    ));
    assert_eq!(split.prior_entries_examined, 0);
    assert_eq!(split.changed_entries_examined, 1);
    assert!(split.publication_io.manifest.installed_objects <= 17);
    assert_eq!(split.publication_io.manifest_reads.read_calls, 1);

    let removed = paths[8].to_str().unwrap().to_owned();
    let (collapsed, deletion) =
        append_graph_files_v2(&lease, workspace.path(), &mut state, &[], &[removed]).unwrap();
    assert_eq!(collapsed, eight);
    assert_eq!(deletion.prior_entries_examined, 0);
    assert!(deletion.publication_io.manifest_reads.read_calls <= 18);
    fs::write(workspace.path().join(&paths[0]), b"replacement").unwrap();
    let (replaced, _) =
        append_graph_files_v2(&lease, workspace.path(), &mut state, &paths[..1], &[]).unwrap();
    assert_ne!(replaced.root_node_sha256, eight.root_node_sha256);
    for (root, expected_count, first_length) in [(&eight, 8, 1), (&nine, 9, 1), (&replaced, 8, 11)]
    {
        let (files, work) =
            crate::resolve_graph_manifest(root, crate::GraphManifestLimits::default(), |digest| {
                read_graph_object_by_digest(
                    container.path(),
                    digest,
                    crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
                )
            })
            .unwrap();
        assert_eq!(files.len(), expected_count);
        assert_eq!(files[0].byte_length, first_length);
        assert!(work.segments_examined <= 17);
        for entry in &files {
            verify_graph_object(container.path(), &entry.content_sha256, entry.byte_length)
                .unwrap();
            assert_eq!(
                crate::graph_manifest::resolve_manifest_entry(
                    root,
                    &entry.relative_path,
                    crate::GraphManifestLimits::default(),
                    |digest| {
                        read_graph_object_by_digest(
                            container.path(),
                            digest,
                            crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
                        )
                    }
                )
                .unwrap()
                .as_ref(),
                Some(entry)
            );
        }
    }
}

#[test]
fn radix_root_is_deterministic_across_descriptor_order() {
    let workspace = tempfile::tempdir().unwrap();
    let paths: Vec<_> = (0_u8..12)
        .map(|ordinal| {
            PathBuf::from(format!(
                "topology/nodes/{ordinal:020}-{ordinal:020}.parquet"
            ))
        })
        .collect();
    for (ordinal, relative) in paths.iter().enumerate() {
        let source = workspace.path().join(relative);
        fs::create_dir_all(source.parent().unwrap()).unwrap();
        fs::write(source, [u8::try_from(ordinal).unwrap()]).unwrap();
    }

    let first = tempfile::tempdir().unwrap();
    let first_lease = begin_graph_object_publication(first.path()).unwrap();
    let mut first_state = GraphManifestState::empty();
    let (first_root, _) = append_graph_files_v2(
        &first_lease,
        workspace.path(),
        &mut first_state,
        &paths,
        &[],
    )
    .unwrap();

    let second = tempfile::tempdir().unwrap();
    let second_lease = begin_graph_object_publication(second.path()).unwrap();
    let mut reversed = paths.clone();
    reversed.reverse();
    let mut second_state = GraphManifestState::empty();
    let (second_root, _) = append_graph_files_v2(
        &second_lease,
        workspace.path(),
        &mut second_state,
        &reversed,
        &[],
    )
    .unwrap();
    assert_eq!(first_root, second_root);
}

#[test]
fn patricia_delete_collapse_and_readd_converge_to_fresh_roots() {
    let workspace = tempfile::tempdir().unwrap();
    let paths = [PathBuf::from("a.parquet"), PathBuf::from("b.parquet")];
    for (ordinal, path) in paths.iter().enumerate() {
        fs::write(
            workspace.path().join(path),
            [u8::try_from(ordinal).unwrap()],
        )
        .unwrap();
    }
    let container = tempfile::tempdir().unwrap();
    let lease = begin_graph_object_publication(container.path()).unwrap();
    let mut state = GraphManifestState::empty();
    let (both, _) =
        append_graph_files_v2(&lease, workspace.path(), &mut state, &paths, &[]).unwrap();
    let (survivor, _) = append_graph_files_v2(
        &lease,
        workspace.path(),
        &mut state,
        &[],
        &["b.parquet".into()],
    )
    .unwrap();

    let fresh = tempfile::tempdir().unwrap();
    let fresh_lease = begin_graph_object_publication(fresh.path()).unwrap();
    let mut fresh_state = GraphManifestState::empty();
    let (fresh_survivor, _) = append_graph_files_v2(
        &fresh_lease,
        workspace.path(),
        &mut fresh_state,
        &paths[..1],
        &[],
    )
    .unwrap();
    assert_eq!(survivor, fresh_survivor);

    let (readded, _) =
        append_graph_files_v2(&lease, workspace.path(), &mut state, &paths[1..], &[]).unwrap();
    assert_eq!(readded, both);
}

#[test]
fn root_bound_state_opens_once_and_failed_append_is_transactional() {
    let container = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    fs::write(workspace.path().join("a.parquet"), b"a").unwrap();
    fs::write(workspace.path().join("b.parquet"), b"b").unwrap();
    let lease = begin_graph_object_publication(container.path()).unwrap();
    let mut initial = GraphManifestState::empty();
    let (root, _) = append_graph_files_v2(
        &lease,
        workspace.path(),
        &mut initial,
        &[PathBuf::from("a.parquet")],
        &[],
    )
    .unwrap();
    let (mut reopened, open_evidence) =
        GraphManifestState::open(&lease, root.clone(), crate::GraphManifestLimits::default())
            .unwrap();
    assert_eq!(open_evidence.entries_examined, 1);
    assert_eq!(open_evidence.application_read_calls, 1);
    assert_eq!(
        open_evidence.decoded_bytes,
        fs::metadata(graph_object_path(container.path(), &root.root_node_sha256).unwrap())
            .unwrap()
            .len()
    );
    let (_, append_evidence) = append_graph_files_v2(
        &lease,
        workspace.path(),
        &mut reopened,
        &[PathBuf::from("b.parquet")],
        &[],
    )
    .unwrap();
    assert_eq!(append_evidence.prior_entries_examined, 0);

    let before_root = reopened.root().unwrap().clone();
    let before_entries = reopened.entries().cloned().collect::<Vec<_>>();
    fs::remove_file(graph_object_path(container.path(), &before_root.root_node_sha256).unwrap())
        .unwrap();
    fs::write(workspace.path().join("c.parquet"), b"c").unwrap();
    assert!(
        append_graph_files_v2(
            &lease,
            workspace.path(),
            &mut reopened,
            &[PathBuf::from("c.parquet")],
            &[],
        )
        .is_err()
    );
    assert_eq!(reopened.root(), Some(&before_root));
    assert_eq!(
        reopened.entries().cloned().collect::<Vec<_>>(),
        before_entries
    );
}

#[test]
fn patricia_s20_inventory_has_linear_resolve_work() {
    const FILES: usize = 277;
    let container = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let paths = (0..FILES)
        .map(|ordinal| PathBuf::from(format!("shards/{ordinal:06}.parquet")))
        .collect::<Vec<_>>();
    fs::create_dir_all(workspace.path().join("shards")).unwrap();
    for (ordinal, path) in paths.iter().enumerate() {
        fs::write(workspace.path().join(path), ordinal.to_le_bytes()).unwrap();
    }
    let lease = begin_graph_object_publication(container.path()).unwrap();
    let mut state = GraphManifestState::empty();
    let (root, _) =
        append_graph_files_v2(&lease, workspace.path(), &mut state, &paths, &[]).unwrap();
    let (resolved, evidence) =
        crate::resolve_graph_manifest(&root, crate::GraphManifestLimits::default(), |digest| {
            read_graph_object_by_digest(container.path(), digest, 1024 * 1024)
        })
        .unwrap();
    assert_eq!(resolved.len(), FILES);
    assert!(evidence.segments_examined <= (2 * FILES - 1) as u64);
    assert!(evidence.work_units <= (4 * FILES - 2) as u64);
}

#[test]
fn publication_components_measure_fresh_reuse_and_path_copy_work() {
    let container = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let lease = begin_graph_object_publication(container.path()).unwrap();
    let relative_path = PathBuf::from("payload.parquet");
    let sealed = |payload: &[u8]| {
        fs::write(workspace.path().join(&relative_path), payload).unwrap();
        vec![AuthenticatedGraphFile {
            relative_path: relative_path.clone(),
            byte_length: payload.len() as u64,
            content_sha256: hex_digest(Sha256::digest(payload).into()),
        }]
    };
    let files = sealed(b"first");
    let mut state = GraphManifestState::empty();
    let (first_root, fresh) =
        append_authenticated_graph_files_v2(&lease, workspace.path(), &mut state, &files, &[])
            .unwrap();
    assert_eq!(fresh.publication_io.payload.read_bytes, 5);
    assert_eq!(fresh.publication_io.payload.write_bytes, 5);
    assert_eq!(fresh.publication_io.payload.installed_objects, 1);
    // Empty branch, depth-one leaf, and collapsed root leaf are distinct
    // immutable objects. Both existing nodes are read during the update.
    assert_eq!(fresh.publication_io.manifest.installed_objects, 3);
    assert_eq!(fresh.publication_io.manifest.reused_objects, 0);
    assert_eq!(fresh.publication_io.manifest_reads.read_calls, 2);
    assert_eq!(fresh.fsync_calls, 12);

    let (_, replay) = append_authenticated_graph_files_v2(
        &lease,
        workspace.path(),
        &mut GraphManifestState::empty(),
        &files,
        &[],
    )
    .unwrap();
    assert_eq!(replay.publication_io.payload.reused_objects, 1);
    assert_eq!(replay.publication_io.payload.read_bytes, 5);
    assert_eq!(replay.publication_io.manifest.reused_objects, 3);
    assert_eq!(replay.publication_io.manifest_reads.read_calls, 2);
    assert_eq!(replay.write_bytes, 0);
    assert_eq!(replay.fsync_calls, 0);
    assert_eq!(replay.bytes_installed, 0);

    let changed = sealed(b"second");
    let (second_root, update) =
        append_authenticated_graph_files_v2(&lease, workspace.path(), &mut state, &changed, &[])
            .unwrap();
    assert_eq!(update.publication_io.payload.installed_objects, 1);
    assert_eq!(update.publication_io.manifest.installed_objects, 1);
    assert_eq!(update.publication_io.manifest_reads.read_calls, 1);
    assert_eq!(update.fsync_calls, 6);
    assert_ne!(first_root, second_root);
    for (root, expected) in [
        (first_root, b"first".as_slice()),
        (second_root, b"second".as_slice()),
    ] {
        let (reopened, _) =
            GraphManifestState::open(&lease, root, crate::GraphManifestLimits::default()).unwrap();
        let entry = reopened.entries().next().unwrap();
        assert_eq!(
            read_graph_object(container.path(), &entry.content_sha256, entry.byte_length).unwrap(),
            expected
        );
    }
    for evidence in [fresh, replay, update] {
        let totals = evidence.publication_io.totals().unwrap();
        assert_eq!(totals.read_calls, evidence.read_calls);
        assert_eq!(totals.write_calls, evidence.write_calls);
        assert_eq!(totals.write_bytes, evidence.write_bytes);
        assert_eq!(totals.installed_bytes, evidence.bytes_installed);
        assert_eq!(
            totals.file_fsync_calls + totals.directory_fsync_calls,
            evidence.fsync_calls
        );
    }
}

#[test]
fn authenticated_publication_hashes_each_payload_once_with_constant_factor() {
    for files in [1_usize, 2, 4] {
        let container = tempfile::tempdir().unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let sealed = (0..files)
            .map(|ordinal| {
                let payload = vec![u8::try_from(ordinal + 1).unwrap(); 4096];
                let relative_path = PathBuf::from(format!("shards/{ordinal}.parquet"));
                let source = workspace.path().join(&relative_path);
                fs::create_dir_all(source.parent().unwrap()).unwrap();
                fs::write(source, &payload).unwrap();
                AuthenticatedGraphFile {
                    relative_path,
                    byte_length: payload.len() as u64,
                    content_sha256: hex_digest(Sha256::digest(&payload).into()),
                }
            })
            .collect::<Vec<_>>();
        let lease = begin_graph_object_publication(container.path()).unwrap();
        let mut state = GraphManifestState::empty();
        let (_, evidence) =
            append_authenticated_graph_files_v2(&lease, workspace.path(), &mut state, &sealed, &[])
                .unwrap();
        assert_eq!(
            evidence.payload_bytes_hashed,
            u64::try_from(files * 4096).unwrap()
        );
        let mut replay_state = GraphManifestState::empty();
        let (_, replay_evidence) = append_authenticated_graph_files_v2(
            &lease,
            workspace.path(),
            &mut replay_state,
            &sealed,
            &[],
        )
        .unwrap();
        assert_eq!(
            replay_evidence.payload_bytes_hashed,
            u64::try_from(files * 4096).unwrap(),
            "CAS reuse must report its one mandatory physical authentication read"
        );
        drop(lease);
        let reclaimed =
            gc_graph_objects(container.path(), &[], crate::GraphManifestLimits::default()).unwrap();
        assert!(reclaimed.objects_removed >= files as u64);
    }
}

#[test]
fn authenticated_publication_still_rejects_corrupt_writer_output() {
    let container = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let relative_path = PathBuf::from("corrupt.parquet");
    fs::write(workspace.path().join(&relative_path), b"mutated!").unwrap();
    let sealed = [AuthenticatedGraphFile {
        relative_path,
        byte_length: 8,
        content_sha256: hex_digest(Sha256::digest(b"original").into()),
    }];
    let lease = begin_graph_object_publication(container.path()).unwrap();
    let mut state = GraphManifestState::empty();
    assert!(
        append_authenticated_graph_files_v2(&lease, workspace.path(), &mut state, &sealed, &[],)
            .is_err()
    );
    assert!(state.root().is_none());
}

#[test]
fn duplicate_and_conflicting_delta_inputs_fail_before_publication() {
    let container = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    fs::write(workspace.path().join("a.parquet"), b"a").unwrap();
    let lease = begin_graph_object_publication(container.path()).unwrap();
    let mut state = GraphManifestState::empty();
    let path = PathBuf::from("a.parquet");
    assert!(
        append_graph_files_v2(
            &lease,
            workspace.path(),
            &mut state,
            &[path.clone(), path.clone()],
            &[],
        )
        .is_err()
    );
    assert!(state.root().is_none());
    assert!(
        append_graph_files_v2(
            &lease,
            workspace.path(),
            &mut state,
            &[path],
            &["a.parquet".into()],
        )
        .is_err()
    );
    assert!(state.root().is_none());
}
