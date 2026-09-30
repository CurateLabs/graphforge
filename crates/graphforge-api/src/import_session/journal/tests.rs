use super::*;
use crate::import_session::{
    ImportSessionLimits,
    tests::{edges, fixture, nodes},
};
use crate::{BulkInputKind, OperationId};

fn staged_fixture() -> (
    tempfile::TempDir,
    crate::GraphForge,
    crate::GraphImportSession,
) {
    let (directory, _, graph) = fixture();
    let mut session = graph
        .begin_import_session(
            OperationId(Uuid::now_v7()),
            ImportSessionLimits {
                batch_rows: 1,
                ..ImportSessionLimits::default()
            },
        )
        .unwrap();
    session
        .append_arrow(
            BulkInputKind::Node,
            &[nodes(&[Uuid::now_v7()]), nodes(&[Uuid::now_v7()])],
        )
        .unwrap();
    (directory, graph, session)
}

#[test]
fn append_before_fsync_recovers_complete_and_torn_unflushed_tails() {
    for tear in [false, true] {
        let (directory, graph, mut session) = staged_fixture();
        let id = session.session_uuid();
        let root = session.root.clone();
        inject("append_before_fsync");
        assert!(
            session
                .validate(&graph)
                .unwrap_err()
                .to_string()
                .contains("append_before_fsync")
        );
        drop(session);
        if tear {
            // Retain a torn header to model losing part of the unflushed append.
            std::fs::OpenOptions::new()
                .write(true)
                .open(root.join(NAME))
                .unwrap()
                .set_len(7)
                .unwrap();
        }
        drop(graph);
        let graph = crate::GraphForge::new(directory.path().join("project").to_str()).unwrap();
        let mut resumed = graph.resume_import_session(id).unwrap();
        let progress = resumed.validate(&graph).unwrap();
        assert_eq!(progress.rows_accepted, 2);
        assert_eq!(progress.construction.unwrap().accepted_chunks, 2);
        resumed.commit(&graph, None).unwrap();
        assert_eq!(graph.node_count("Person").unwrap(), 2);
    }
}

#[test]
fn accepted_chunk_without_durable_progress_replays_once() {
    let (directory, graph, mut session) = staged_fixture();
    let id = session.session_uuid();
    let root = session.root.clone();
    inject("accepted_before_progress");
    assert!(
        session
            .validate(&graph)
            .unwrap_err()
            .to_string()
            .contains("accepted_before_progress")
    );
    drop(session);
    // Simulate loss of the entire unsynchronized journal, while the durable
    // construction receipt and its deterministic chunk key survive.
    std::fs::OpenOptions::new()
        .write(true)
        .open(root.join(NAME))
        .unwrap()
        .set_len(0)
        .unwrap();
    drop(graph);
    let graph = crate::GraphForge::new(directory.path().join("project").to_str()).unwrap();
    let mut resumed = graph.resume_import_session(id).unwrap();
    let progress = resumed.validate(&graph).unwrap();
    assert_eq!(progress.rows_accepted, 2);
    let construction = progress.construction.unwrap();
    assert_eq!(construction.accepted_chunks, 2);
    assert_eq!(construction.input_batches, 2);
    resumed.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 2);
}

#[test]
fn fsync_before_seal_recovers_durable_progress_without_duplicate_rows() {
    let (directory, graph, mut session) = staged_fixture();
    let id = session.session_uuid();
    inject("fsync_before_seal");
    assert!(
        session
            .validate(&graph)
            .unwrap_err()
            .to_string()
            .contains("fsync_before_seal")
    );
    assert_eq!(session.manifest.phase, super::super::ImportPhase::Open);
    drop(session);
    drop(graph);
    let graph = crate::GraphForge::new(directory.path().join("project").to_str()).unwrap();
    let mut resumed = graph.resume_import_session(id).unwrap();
    assert_eq!(resumed.status().1.rows_accepted, 2);
    let progress = resumed.validate(&graph).unwrap();
    assert_eq!(progress.rows_accepted, 2);
    assert_eq!(progress.construction.unwrap().accepted_chunks, 2);
    resumed.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 2);
}

#[test]
fn node_phase_checkpoint_survives_loss_of_edge_progress() {
    let (directory, _, graph) = fixture();
    let ids = [Uuid::now_v7(), Uuid::now_v7()];
    let mut session = graph
        .begin_import_session(
            OperationId(Uuid::now_v7()),
            ImportSessionLimits {
                batch_rows: 1,
                ..ImportSessionLimits::default()
            },
        )
        .unwrap();
    session
        .append_arrow(BulkInputKind::Node, &[nodes(&ids[..1]), nodes(&ids[1..])])
        .unwrap();
    session
        .append_arrow(
            BulkInputKind::Edge,
            &[edges(Uuid::now_v7(), ids[0], ids[1])],
        )
        .unwrap();
    let id = session.session_uuid();
    let root = session.root.clone();
    inject("edge_accepted_before_progress");
    assert!(
        session
            .validate(&graph)
            .unwrap_err()
            .to_string()
            .contains("edge_accepted_before_progress")
    );
    drop(session);
    let checkpoint: SessionManifest =
        serde_json::from_reader(File::open(root.join(MANIFEST)).unwrap()).unwrap();
    // Find the exact checkpoint tail and discard the later edge in-flight record.
    let mut journal = File::open(root.join(NAME)).unwrap();
    let mut length = 0;
    for _ in 0..checkpoint.journal_sequence {
        let mut header = [0_u8; HEADER];
        journal.read_exact(&mut header).unwrap();
        let payload = u64::from_le_bytes(header[8..16].try_into().unwrap());
        journal
            .seek(SeekFrom::Current(
                i64::try_from(payload + DIGEST as u64).unwrap(),
            ))
            .unwrap();
        length += HEADER as u64 + payload + DIGEST as u64;
    }
    drop(journal);
    std::fs::OpenOptions::new()
        .write(true)
        .open(root.join(NAME))
        .unwrap()
        .set_len(length)
        .unwrap();
    let mut resumed = graph.resume_import_session(id).unwrap();
    let progress = resumed.validate(&graph).unwrap();
    assert_eq!(progress.rows_accepted, 3);
    assert_eq!(progress.construction.unwrap().accepted_chunks, 3);
    resumed.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 2);
    drop(directory);
}

#[test]
fn checkpointed_journal_corruption_is_refused() {
    let (_directory, graph, mut session) = staged_fixture();
    session.validate(&graph).unwrap();
    let id = session.session_uuid();
    let root = session.root.clone();
    drop(session);
    let mut bytes = std::fs::read(root.join(NAME)).unwrap();
    bytes[HEADER + 5] ^= 1;
    std::fs::write(root.join(NAME), bytes).unwrap();
    assert!(
        graph
            .resume_import_session(id)
            .err()
            .unwrap()
            .to_string()
            .contains("checksum mismatch")
    );
}

#[test]
fn byte_cadence_flushes_journal_without_replacing_manifest() {
    const CHILD: &str = "GF_TEST_IMPORT_JOURNAL_BYTE_CADENCE";
    const VERIFIED: &str = "import journal byte cadence verified";
    if std::env::var_os(CHILD).is_none() {
        // Region counters are process-wide. Keep the exact barrier proof
        // independent of unrelated API tests running in this test process.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "import_session::journal::tests::byte_cadence_flushes_journal_without_replacing_manifest",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        // A successful harness exit alone could also mean the exact filter
        // selected no test. Require completion of the real journal proof.
        assert!(stdout.lines().any(|line| line == VERIFIED), "{stdout}");
        return;
    }
    let (_directory, _graph, mut session) = staged_fixture();
    let checkpoint = std::fs::read(session.root.join(MANIFEST)).unwrap();
    let capture = graphforge_storage::concurrency_attribution::RegionCapture::start("cadence");
    let mut barriers = 0;
    let mut written_since_sync = 0;
    while barriers < 2 {
        let before = session.journal.file.metadata().unwrap().len();
        session
            .journal
            .append(&mut session.manifest, 0, None)
            .unwrap();
        let framed_bytes = session.journal.file.metadata().unwrap().len() - before;
        written_since_sync += framed_bytes;
        if written_since_sync >= SYNC_BYTES {
            assert_eq!(session.journal.pending_bytes, 0);
            barriers += 1;
            written_since_sync = 0;
        } else {
            assert_eq!(session.journal.pending_bytes, written_since_sync);
        }
    }
    let snapshot = capture.finish();
    assert!(snapshot.complete);
    assert_eq!(
        snapshot.regions["cadence/journal_sync"]
            .inclusive
            .fsync_calls,
        Some(2)
    );
    assert_eq!(
        snapshot.regions["cadence/journal_append"]
            .inclusive
            .fsync_calls,
        Some(0)
    );
    assert!(
        !snapshot
            .regions
            .contains_key("cadence/manifest_persistence")
    );
    assert!(
        !snapshot
            .regions
            .contains_key("cadence/journal_namespace_publication")
    );
    assert_eq!(
        std::fs::read(session.root.join(MANIFEST)).unwrap(),
        checkpoint
    );
    assert_eq!(session.manifest.sources[0].batches_staged, 0);
    // Single-thread libtest output can leave its test-name prefix on the same
    // line. Give the completion marker its own line in either harness mode.
    println!("\n{VERIFIED}");
}

#[test]
fn failed_cadence_sync_blocks_writes_until_fresh_format_aware_reopen() {
    let (directory, graph, mut session) = staged_fixture();
    let id = session.session_uuid();
    let construction = session.open_construction(&graph).unwrap();
    let construction_id = construction.session_uuid();
    assert_eq!(construction.progress().accepted_chunks, 0);
    drop(construction);
    let checkpoint = std::fs::read(session.root.join(MANIFEST)).unwrap();
    // A failed cadence barrier can leave a complete in-flight frame. Without
    // session-level admission, validate would resume construction and append
    // this chunk before its next journal write notices the failed barrier.
    session.manifest.sources[0].inflight_batch = Some(0);
    inject("sync");
    loop {
        let pending_before = session.journal.pending_bytes;
        let file_before = session.journal.file.metadata().unwrap().len();
        let appended = session.journal.append(&mut session.manifest, 0, None);
        let framed_bytes = session.journal.file.metadata().unwrap().len() - file_before;
        if pending_before + framed_bytes >= SYNC_BYTES {
            assert!(
                appended
                    .unwrap_err()
                    .to_string()
                    .contains("injected import journal sync")
            );
            break;
        }
        appended.unwrap();
    }
    let retained_pending = session.journal.pending_bytes;
    assert!(retained_pending >= SYNC_BYTES);
    let complete_length = session.journal.file.metadata().unwrap().len();
    assert!(
        session
            .journal
            .append(&mut session.manifest, 0, None)
            .unwrap_err()
            .to_string()
            .contains("failed write or sync")
    );
    assert!(
        session
            .journal
            .sync(None)
            .unwrap_err()
            .to_string()
            .contains("failed write or sync")
    );
    assert_eq!(session.journal.pending_bytes, retained_pending);
    assert_eq!(
        session.journal.file.metadata().unwrap().len(),
        complete_length
    );
    assert_eq!(
        std::fs::read(session.root.join(MANIFEST)).unwrap(),
        checkpoint
    );
    let before_state = serde_json::to_vec(&session.manifest).unwrap();
    let before_files = project_files(&directory.path().join("project"));
    assert_recovery_required(session.validate(&graph).unwrap_err());
    let cancelled = crate::CancellationToken::new();
    cancelled.cancel();
    assert_recovery_required(
        session
            .validate_with_cancellation(&graph, Some(&cancelled))
            .unwrap_err(),
    );
    assert_recovery_required(session.append_arrow(BulkInputKind::Node, &[]).unwrap_err());
    assert_recovery_required(
        session
            .append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
            .unwrap_err(),
    );
    assert_recovery_required(
        session
            .register_parquet(BulkInputKind::Node, &session.root.join(MANIFEST))
            .unwrap_err(),
    );
    assert_recovery_required(session.checkpoint().unwrap_err());
    assert_recovery_required(session.commit(&graph, None).unwrap_err());
    assert_eq!(serde_json::to_vec(&session.manifest).unwrap(), before_state);
    assert_eq!(
        project_files(&directory.path().join("project")),
        before_files
    );
    let construction = graph
        .resume_graph_construction(construction_id, session.construction_budgets())
        .unwrap();
    assert_eq!(construction.progress().accepted_chunks, 0);
    drop(construction);
    let session_root = session.root.clone();
    assert_recovery_required(session.abort(&graph).unwrap_err());
    assert_eq!(
        project_files(&directory.path().join("project")),
        before_files
    );
    assert_eq!(
        std::fs::read(session_root.join(MANIFEST)).unwrap(),
        checkpoint
    );
    drop(graph);
    let graph = crate::GraphForge::new(directory.path().join("project").to_str()).unwrap();
    let mut resumed = graph.resume_import_session(id).unwrap();
    assert_eq!(resumed.journal.pending_bytes, 0);
    resumed
        .journal
        .append(&mut resumed.manifest, 0, None)
        .unwrap();
    assert!(resumed.journal.pending_bytes > 0);
    resumed.journal.sync(None).unwrap();
    assert_eq!(resumed.journal.pending_bytes, 0);
    let progress = resumed.validate(&graph).unwrap();
    assert_eq!(progress.rows_accepted, 2);
    assert_eq!(progress.construction.unwrap().accepted_chunks, 2);
    resumed.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 2);
}

#[test]
fn torn_unflushed_completed_frame_replays_accepted_chunk() {
    let (_directory, graph, mut session) = staged_fixture();
    let id = session.session_uuid();
    let root = session.root.clone();
    inject("completed_before_fsync");
    assert!(
        session
            .validate(&graph)
            .unwrap_err()
            .to_string()
            .contains("completed_before_fsync")
    );
    drop(session);
    let mut bytes = std::fs::read(root.join(NAME)).unwrap();
    let last = bytes.last_mut().unwrap();
    *last ^= 1;
    std::fs::write(root.join(NAME), bytes).unwrap();
    let mut resumed = graph.resume_import_session(id).unwrap();
    let progress = resumed.validate(&graph).unwrap();
    assert_eq!(progress.rows_accepted, 2);
    assert_eq!(progress.construction.unwrap().accepted_chunks, 2);
    resumed.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 2);
}

#[test]
fn lost_progress_does_not_accept_mutated_source_replay() {
    let (_directory, graph, mut session) = staged_fixture();
    let id = session.session_uuid();
    let root = session.root.clone();
    let source_path = root.join("sources").join(&session.manifest.sources[0].name);
    inject("accepted_before_progress");
    assert!(session.validate(&graph).is_err());
    drop(session);
    std::fs::OpenOptions::new()
        .write(true)
        .open(root.join(NAME))
        .unwrap()
        .set_len(0)
        .unwrap();
    let batch = nodes(&[Uuid::now_v7()]);
    let mut writer = arrow::ipc::writer::FileWriter::try_new(
        File::create(source_path).unwrap(),
        &batch.schema(),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let mut resumed = graph.resume_import_session(id).unwrap();
    assert!(resumed.validate(&graph).is_err());
    assert_eq!(graph.node_count("Person").unwrap(), 0);
}

#[test]
fn legacy_or_missing_journal_state_is_refused() {
    let (_directory, graph, session) = staged_fixture();
    let id = session.session_uuid();
    let root = session.root.clone();
    let mut manifest = session.manifest.clone();
    drop(session);
    std::fs::remove_file(root.join(NAME)).unwrap();
    assert!(graph.resume_import_session(id).is_err());
    manifest.format_version = 1;
    write_checkpoint(&root, &manifest, None, &mut false).unwrap();
    assert!(
        graph
            .resume_import_session(id)
            .err()
            .unwrap()
            .to_string()
            .contains("incompatible")
    );
}

#[test]
fn checkpoint_failure_cleans_preparation_without_replacing_manifest() {
    let (_directory, _graph, mut session) = staged_fixture();
    let prior = std::fs::read(session.root.join(MANIFEST)).unwrap();
    inject("checkpoint_before_sync");
    assert!(
        session
            .persist_manifest()
            .unwrap_err()
            .to_string()
            .contains("checkpoint_before_sync")
    );
    assert_eq!(std::fs::read(session.root.join(MANIFEST)).unwrap(), prior);
    assert!(!std::fs::read_dir(&session.root).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with(".tmp")
    }));
}

#[test]
fn corrupt_sequence_cannot_disguise_a_checkpointed_record_as_unflushed() {
    let (_directory, graph, mut session) = staged_fixture();
    session.validate(&graph).unwrap();
    let id = session.session_uuid();
    let root = session.root.clone();
    drop(session);
    let mut bytes = std::fs::read(root.join(NAME)).unwrap();
    let mut offset = 0;
    loop {
        let payload = usize::try_from(u64::from_le_bytes(
            bytes[offset + 8..offset + 16].try_into().unwrap(),
        ))
        .unwrap();
        let next = offset + HEADER + payload + DIGEST;
        if next == bytes.len() {
            break;
        }
        offset = next;
    }
    bytes[offset + 16..offset + 24].copy_from_slice(&u64::MAX.to_le_bytes());
    std::fs::write(root.join(NAME), bytes).unwrap();
    assert!(graph.resume_import_session(id).is_err());
}

fn assert_recovery_required(error: GfError) {
    assert!(
        error.to_string().contains("failed write or sync"),
        "{error}"
    );
}

fn project_files(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    fn visit(
        root: &Path,
        directory: &Path,
        files: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>,
    ) {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &entry.path(), files);
            } else {
                files.insert(
                    entry.path().strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(entry.path()).unwrap(),
                );
            }
        }
    }
    let mut files = std::collections::BTreeMap::new();
    visit(root, root, &mut files);
    files
}

#[test]
fn source_registration_checkpoint_failure_preserves_only_possible_authority() {
    for point in ["checkpoint_before_sync", "checkpoint_after_replace"] {
        let (directory, _, graph) = fixture();
        let mut session = graph
            .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
            .unwrap();
        let id = session.session_uuid();
        let root = session.root.clone();
        let before = std::fs::read(root.join(MANIFEST)).unwrap();
        inject(point);
        let error = session
            .append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
            .unwrap_err();
        assert!(error.to_string().contains(point));
        assert_recovery_required(session.checkpoint().unwrap_err());
        let source = root.join("sources/00000000000000000000.arrow");
        let published = point == "checkpoint_after_replace";
        assert_eq!(source.exists(), published);
        let checkpoint: SessionManifest =
            serde_json::from_slice(&std::fs::read(root.join(MANIFEST)).unwrap()).unwrap();
        assert_eq!(checkpoint.sources.len(), usize::from(published));
        if !published {
            assert_eq!(std::fs::read(root.join(MANIFEST)).unwrap(), before);
        }
        assert!(!std::fs::read_dir(&root).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
        drop(session);
        drop(graph);
        let graph = crate::GraphForge::new(directory.path().join("project").to_str()).unwrap();
        let mut resumed = graph.resume_import_session(id).unwrap();
        assert_eq!(resumed.status().1.files_accepted, u64::from(published));
        if !published {
            // Repeat a fresh definite-prepublication refusal. Cleanup keeps
            // both source and preparation namespaces bounded on every attempt.
            inject(point);
            assert!(
                resumed
                    .append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
                    .is_err()
            );
            assert!(
                std::fs::read_dir(root.join("sources"))
                    .unwrap()
                    .next()
                    .is_none()
            );
            drop(resumed);
            resumed = graph.resume_import_session(id).unwrap();
            resumed
                .append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
                .unwrap();
        }
        let progress = resumed.validate(&graph).unwrap();
        assert_eq!(progress.rows_accepted, 1);
        resumed.commit(&graph, None).unwrap();
        assert_eq!(graph.node_count("Person").unwrap(), 1);
    }
}

#[test]
fn source_publication_failure_requires_fresh_reopen_before_cleanup() {
    let (directory, graph, mut session) = staged_fixture();
    let id = session.session_uuid();
    let root = session.root.clone();
    inject("source_after_replace");
    assert!(
        session
            .append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
            .unwrap_err()
            .to_string()
            .contains("source_after_replace")
    );
    let files = project_files(&directory.path().join("project"));
    assert_recovery_required(session.abort(&graph).unwrap_err());
    assert_eq!(project_files(&directory.path().join("project")), files);
    drop(graph);
    let graph = crate::GraphForge::new(directory.path().join("project").to_str()).unwrap();
    let resumed = graph.resume_import_session(id).unwrap();
    assert_eq!(resumed.status().1.files_accepted, 1);
    assert_eq!(std::fs::read_dir(root.join("sources")).unwrap().count(), 2);
    resumed.abort(&graph).unwrap();
    assert!(!root.join("sources").exists());
}

#[test]
fn failed_prepublication_source_cleanup_retains_primary_error_and_requires_recovery() {
    let (directory, _, graph) = fixture();
    let mut session = graph
        .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
        .unwrap();
    let id = session.session_uuid();
    let root = session.root.clone();
    inject("checkpoint_before_sync");
    inject("source_cleanup_before_unlink");
    let error = session
        .append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
        .unwrap_err()
        .to_string();
    assert!(error.contains("checkpoint_before_sync"));
    assert!(error.contains("source cleanup failed"));
    assert!(error.contains("source_cleanup_before_unlink"));
    assert_recovery_required(session.checkpoint().unwrap_err());
    drop(session);
    drop(graph);
    let graph = crate::GraphForge::new(directory.path().join("project").to_str()).unwrap();
    let resumed = graph.resume_import_session(id).unwrap();
    assert_eq!(resumed.status().1.files_accepted, 0);
    assert_eq!(std::fs::read_dir(root.join("sources")).unwrap().count(), 1);
    resumed.abort(&graph).unwrap();
    assert!(!root.join("sources").exists());
}

#[test]
fn registration_sync_failure_cleans_unreferenced_sources_on_every_fresh_attempt() {
    let (directory, _, graph) = fixture();
    let mut session = graph
        .begin_import_session(OperationId(Uuid::now_v7()), ImportSessionLimits::default())
        .unwrap();
    let id = session.session_uuid();
    let root = session.root.clone();
    let checkpoint = std::fs::read(root.join(MANIFEST)).unwrap();
    for _ in 0..3 {
        inject("sync");
        let error = session
            .append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
            .unwrap_err();
        assert!(error.to_string().contains("injected import journal sync"));
        assert_recovery_required(session.checkpoint().unwrap_err());
        assert!(
            std::fs::read_dir(root.join("sources"))
                .unwrap()
                .next()
                .is_none()
        );
        assert_eq!(std::fs::read(root.join(MANIFEST)).unwrap(), checkpoint);
        drop(session);
        session = graph.resume_import_session(id).unwrap();
        assert_eq!(session.status().1.files_accepted, 0);
    }
    drop(session);
    drop(graph);
    let graph = crate::GraphForge::new(directory.path().join("project").to_str()).unwrap();
    let mut resumed = graph.resume_import_session(id).unwrap();
    resumed
        .append_arrow(BulkInputKind::Node, &[nodes(&[Uuid::now_v7()])])
        .unwrap();
    assert_eq!(resumed.validate(&graph).unwrap().rows_accepted, 1);
    resumed.commit(&graph, None).unwrap();
    assert_eq!(graph.node_count("Person").unwrap(), 1);
}
