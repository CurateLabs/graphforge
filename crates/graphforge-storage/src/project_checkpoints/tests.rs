use super::*;
use std::collections::BTreeMap;
use std::io::Write as _;
use std::io::{BufRead, BufReader};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;
use tempfile::tempdir;
use wait_timeout::ChildExt;

const TEST_DEADLINE: Duration = Duration::from_secs(1);
const CHILD_DEADLINE: Duration = Duration::from_secs(10);

struct WriterLockHolder {
    release: Option<mpsc::SyncSender<()>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl WriterLockHolder {
    fn finish(mut self) -> Result<(), String> {
        let release = self
            .release
            .take()
            .ok_or_else(|| "phase=main release sender missing".to_owned())?;
        let release_result = release
            .send(())
            .map_err(|error| format!("phase=main release holder error={error}"));
        let join_result = self
            .worker
            .take()
            .ok_or_else(|| "phase=main holder worker missing".to_owned())?
            .join()
            .map_err(|_| "phase=main holder worker panicked".to_owned());
        release_result.and(join_result)
    }
}

impl Drop for WriterLockHolder {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub(super) fn while_writer_lock_is_held<T>(root: &Path, action: impl FnOnce() -> T) -> T {
    let writer_path = root.join(LOCKS_DIR).join(WRITER_LOCK_FILE);
    let worker_path = writer_path.clone();
    let (ready_sender, ready_receiver) = mpsc::sync_channel(0);
    let (release_sender, release_receiver) = mpsc::sync_channel(0);
    let worker = std::thread::Builder::new()
        .name("checkpoint-writer-lock-holder".into())
        .spawn(move || {
            let writer = open_regular_lock(&worker_path).expect("phase=holder open writer.lock");
            assert!(
                crate::file_lock::try_lock_exclusive(&writer)
                    .expect("phase=holder acquire writer.lock"),
                "phase=holder writer.lock unexpectedly busy"
            );
            ready_sender.send(()).expect("phase=holder publish ready");
            release_receiver.recv().expect("phase=holder await release");
            crate::file_lock::unlock(&writer).expect("phase=holder release writer.lock");
        })
        .expect("phase=holder spawn");
    let holder = WriterLockHolder {
        release: Some(release_sender),
        worker: Some(worker),
    };
    if let Err(error) = ready_receiver.recv_timeout(TEST_DEADLINE) {
        drop(ready_receiver);
        let cleanup = holder.finish();
        panic!("phase=main await held writer.lock error={error}; cleanup={cleanup:?}");
    }
    let result = catch_unwind(AssertUnwindSafe(action));
    let cleanup = holder.finish();
    match result {
        Ok(value) => {
            cleanup.unwrap_or_else(|error| panic!("phase=main holder cleanup error={error}"));
            value
        }
        Err(original) => {
            let _ = cleanup;
            resume_unwind(original);
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum CheckpointLockMode {
    Shared,
    Exclusive,
}

/// Hold `checkpoints.lock` (shared or exclusive) while `action` runs.
///
/// Writer is not held — the issue #275 CI signature is writer free +
/// checkpoints contended.
fn while_checkpoint_lock_is_held<T>(
    root: &Path,
    mode: CheckpointLockMode,
    action: impl FnOnce() -> T,
) -> T {
    let checkpoint_path = root.join(LOCKS_DIR).join(CHECKPOINT_LOCK_FILE);
    let worker_path = checkpoint_path.clone();
    let (ready_sender, ready_receiver) = mpsc::sync_channel(0);
    let (release_sender, release_receiver) = mpsc::sync_channel(0);
    let worker = std::thread::Builder::new()
        .name("checkpoint-lock-holder".into())
        .spawn(move || {
            let checkpoint =
                open_regular_lock(&worker_path).expect("phase=holder open checkpoints.lock");
            let acquired = match mode {
                CheckpointLockMode::Shared => crate::file_lock::try_lock_shared(&checkpoint)
                    .expect("phase=holder acquire shared checkpoints.lock"),
                CheckpointLockMode::Exclusive => crate::file_lock::try_lock_exclusive(&checkpoint)
                    .expect("phase=holder acquire exclusive checkpoints.lock"),
            };
            assert!(
                acquired,
                "phase=holder checkpoints.lock unexpectedly busy mode={mode:?}"
            );
            ready_sender.send(()).expect("phase=holder publish ready");
            release_receiver.recv().expect("phase=holder await release");
            crate::file_lock::unlock(&checkpoint).expect("phase=holder release checkpoints.lock");
        })
        .expect("phase=holder spawn");
    let holder = WriterLockHolder {
        release: Some(release_sender),
        worker: Some(worker),
    };
    if let Err(error) = ready_receiver.recv_timeout(TEST_DEADLINE) {
        drop(ready_receiver);
        let cleanup = holder.finish();
        panic!("phase=main await held checkpoints.lock error={error}; cleanup={cleanup:?}");
    }
    let result = catch_unwind(AssertUnwindSafe(action));
    let cleanup = holder.finish();
    match result {
        Ok(value) => {
            cleanup.unwrap_or_else(|error| panic!("phase=main holder cleanup error={error}"));
            value
        }
        Err(original) => {
            let _ = cleanup;
            resume_unwind(original);
        }
    }
}

/// Reproduce the issue #275 schedule: acquire writer then checkpoints, unlock
/// writer early, leave checkpoints held while `action` runs.
fn while_checkpoint_held_after_writer_released<T>(root: &Path, action: impl FnOnce() -> T) -> T {
    let lock_root = root.join(LOCKS_DIR);
    let writer_path = lock_root.join(WRITER_LOCK_FILE);
    let checkpoint_path = lock_root.join(CHECKPOINT_LOCK_FILE);
    let worker_writer = writer_path.clone();
    let worker_checkpoint = checkpoint_path.clone();
    let (ready_sender, ready_receiver) = mpsc::sync_channel(0);
    let (release_sender, release_receiver) = mpsc::sync_channel(0);
    let worker = std::thread::Builder::new()
        .name("checkpoint-early-writer-release".into())
        .spawn(move || {
            let writer = open_regular_lock(&worker_writer).expect("phase=holder open writer.lock");
            assert!(
                crate::file_lock::try_lock_exclusive(&writer)
                    .expect("phase=holder acquire writer.lock"),
                "phase=holder writer.lock unexpectedly busy"
            );
            let checkpoint =
                open_regular_lock(&worker_checkpoint).expect("phase=holder open checkpoints.lock");
            assert!(
                crate::file_lock::try_lock_exclusive(&checkpoint)
                    .expect("phase=holder acquire checkpoints.lock"),
                "phase=holder checkpoints.lock unexpectedly busy"
            );
            crate::file_lock::unlock(&writer).expect("phase=holder early release writer.lock");
            ready_sender.send(()).expect("phase=holder publish ready");
            release_receiver.recv().expect("phase=holder await release");
            crate::file_lock::unlock(&checkpoint).expect("phase=holder release checkpoints.lock");
        })
        .expect("phase=holder spawn");
    let holder = WriterLockHolder {
        release: Some(release_sender),
        worker: Some(worker),
    };
    if let Err(error) = ready_receiver.recv_timeout(TEST_DEADLINE) {
        drop(ready_receiver);
        let cleanup = holder.finish();
        panic!("phase=main await early-writer-release schedule error={error}; cleanup={cleanup:?}");
    }
    let result = catch_unwind(AssertUnwindSafe(action));
    let cleanup = holder.finish();
    match result {
        Ok(value) => {
            cleanup.unwrap_or_else(|error| panic!("phase=main holder cleanup error={error}"));
            value
        }
        Err(original) => {
            let _ = cleanup;
            resume_unwind(original);
        }
    }
}

fn assert_mutation_locks_free(root: &Path, phase: &str) {
    let lock_root = root.join(LOCKS_DIR);
    for name in [WRITER_LOCK_FILE, CHECKPOINT_LOCK_FILE] {
        let lock = open_regular_lock(&lock_root.join(name)).unwrap();
        assert!(
            crate::file_lock::try_lock_exclusive(&lock).unwrap(),
            "{phase}: {name} leaked"
        );
        crate::file_lock::unlock(&lock).unwrap();
    }
}

fn assert_checkpoint_mutation_busy_message(error: &GfError) {
    assert_eq!(error.code(), "GF_WRITER_BUSY");
    match error {
        GfError::Project { message, .. } => {
            assert_eq!(
                message, "checkpoint mutation could not acquire checkpoints.lock",
                "expected issue #275 WriterBusy message, got {message}"
            );
        }
        other => panic!("expected Project WriterBusy, got {other}"),
    }
}

struct BoundedChild {
    child: std::process::Child,
    reaped: bool,
}

impl BoundedChild {
    fn wait(mut self, phase: &str) -> std::process::ExitStatus {
        let mut failures = Vec::new();
        match self.child.wait_timeout(CHILD_DEADLINE) {
            Ok(Some(status)) => {
                self.reaped = true;
                return status;
            }
            Ok(None) => failures.push(format!("wait timeout={CHILD_DEADLINE:?}")),
            Err(error) => failures.push(format!("wait error={error}")),
        }
        if let Err(error) = self.child.kill() {
            failures.push(format!("kill error={error}"));
        }
        match self.child.wait_timeout(TEST_DEADLINE) {
            Ok(Some(status)) => {
                self.reaped = true;
                failures.push(format!("killed_status={status}"));
            }
            Ok(None) => failures.push(format!("reap timeout={TEST_DEADLINE:?}")),
            Err(error) => failures.push(format!("reap error={error}")),
        }
        panic!("phase={phase} child cleanup failures={failures:?}");
    }
}

impl Drop for BoundedChild {
    fn drop(&mut self) {
        if !self.reaped {
            let mut failures = Vec::new();
            if let Err(error) = self.child.kill() {
                failures.push(format!("kill error={error}"));
            }
            match self.child.wait_timeout(TEST_DEADLINE) {
                Ok(Some(_)) => self.reaped = true,
                Ok(None) => failures.push(format!("reap timeout={TEST_DEADLINE:?}")),
                Err(error) => failures.push(format!("reap error={error}")),
            }
            if !failures.is_empty() {
                eprintln!("phase=drop child cleanup failures={failures:?}");
            }
        }
    }
}

fn recover_checkpoint_pair_after_lock_handoff(root: &Path, phase: &str) {
    checkpoint_lock_handoff(root, phase, true);
}

pub(super) fn preserve_checkpoint_intent_after_lock_handoff(root: &Path, phase: &str) {
    checkpoint_lock_handoff(root, phase, false);
}

fn checkpoint_lock_handoff(root: &Path, phase: &str, recover_durable_intent: bool) {
    let lock_root = root.join(LOCKS_DIR);
    let writer_path = lock_root.join(WRITER_LOCK_FILE);
    let checkpoint_path = lock_root.join(CHECKPOINT_LOCK_FILE);
    let checkpoint_root = root.join(CHECKPOINTS_DIR);
    let worker_writer_path = writer_path.clone();
    let worker_checkpoint_path = checkpoint_path.clone();
    let (sender, receiver) = mpsc::sync_channel(0);
    std::thread::Builder::new()
        .name("checkpoint-lock-handoff-recovery".into())
        .spawn(move || {
            let result = (|| {
                let writer = open_regular_lock(&worker_writer_path)
                    .map_err(|error| format!("open writer.lock failed: {error}"))?;
                crate::file_lock::lock_exclusive(&writer)
                    .map_err(|error| format!("acquire writer.lock failed: {error}"))?;

                let checkpoint = match open_regular_lock(&worker_checkpoint_path) {
                    Ok(checkpoint) => checkpoint,
                    Err(error) => {
                        let writer_unlock = crate::file_lock::unlock(&writer);
                        return Err(format!(
                            "open checkpoints.lock failed: {error}; \
                                 writer_unlock={writer_unlock:?}"
                        ));
                    }
                };
                if let Err(error) = crate::file_lock::lock_exclusive(&checkpoint) {
                    let writer_unlock = crate::file_lock::unlock(&writer);
                    return Err(format!(
                        "acquire checkpoints.lock failed: {error}; writer_unlock={writer_unlock:?}"
                    ));
                }

                let recovery =
                    if recover_durable_intent && checkpoint_root.join(INTENT_FILE).exists() {
                        recover_pair(&checkpoint_root).map_err(|error| {
                            format!("recover durable checkpoint intent failed: {error}")
                        })
                    } else {
                        Ok(())
                    };
                let checkpoint_unlock = crate::file_lock::unlock(&checkpoint)
                    .map_err(|error| format!("unlock checkpoints.lock failed: {error}"));
                let writer_unlock = crate::file_lock::unlock(&writer)
                    .map_err(|error| format!("unlock writer.lock failed: {error}"));

                recovery?;
                checkpoint_unlock?;
                writer_unlock
            })();
            let _ = sender.send(result);
        })
        .unwrap();
    match receiver.recv_timeout(Duration::from_secs(1)) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!(
            "checkpoint lock handoff/recovery failed at {phase}; writer_path={}; \
                 checkpoint_path={}: {error}",
            writer_path.display(),
            checkpoint_path.display()
        ),
        Err(error) => panic!(
            "checkpoint lock handoff/recovery timed out at {phase}; writer_path={}; \
                 checkpoint_path={}; timeout=1s; channel={error}",
            writer_path.display(),
            checkpoint_path.display()
        ),
    }
}

fn publish_clone(root: &Path) -> Uuid {
    let selected = crate::resolve_project_generation(root).unwrap();
    let capabilities = selected
        .capabilities()
        .into_iter()
        .map(|entry| crate::ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let participants = selected
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .map(|entry| crate::ProjectParticipant {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
            record_family_id: entry.record_family_id,
            record_version: entry.record_version,
            encoding: match entry.encoding.as_str() {
                "arrow" => crate::ProjectParticipantEncoding::Arrow,
                "json" => crate::ProjectParticipantEncoding::Json,
                "parquet" => crate::ProjectParticipantEncoding::Parquet,
                other => panic!("unexpected participant encoding {other}"),
            },
            schema_fingerprint: entry.schema_fingerprint,
            row_count: entry.row_count,
            bytes: entry.bytes,
        })
        .collect();
    let generation_uuid = Uuid::now_v7();
    let request = crate::ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid,
        capabilities,
        participants,
    };
    let crate::ProjectStageOutcome::Staged(staged) =
        crate::stage_project_generation(root, &request).unwrap()
    else {
        panic!("fresh publication unexpectedly replayed");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    generation_uuid
}

pub(super) fn create_request(operation_uuid: Uuid, name: &str) -> CheckpointCreateRequest {
    CheckpointCreateRequest {
        operation_uuid,
        name: name.into(),
        description: Some("release candidate".into()),
        actor_uuid: Some(Uuid::parse_str("018f0f4e-7b8c-7000-8000-0000000000aa").unwrap()),
    }
}

#[test]
fn revert_publishes_child_preserves_registry_and_replays_after_delete() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    let source = crate::resolve_project_generation(directory.path()).unwrap();
    let created = create_checkpoint(
        directory.path(),
        &create_request(Uuid::from_u128(40), "Before"),
    )
    .unwrap();
    let prior_current = publish_clone(directory.path());
    let request = CheckpointRevertRequest {
        operation_uuid: Uuid::from_u128(41),
        name: "Before".into(),
        reason: " restore known state ".into(),
        actor_uuid: None,
    };
    let (receipt, restored) = revert_checkpoint(
        directory.path(),
        &request,
        || Ok(1_720_000_000_123_456),
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(restored.parent_generation_uuid(), Some(prior_current));
    assert_eq!(receipt.source_generation_uuid, source.generation_uuid());
    assert_eq!(receipt.prior_current_generation_uuid, Some(prior_current));
    assert_eq!(receipt.registry_revision, created.registry_revision);
    assert_eq!(list_checkpoints(directory.path()).unwrap().len(), 1);
    let restoration_count = restored
        .participant_descriptors()
        .unwrap()
        .iter()
        .filter(|row| row.record_family_id == RESTORATION_FAMILY)
        .count();
    assert_eq!(restoration_count, 1);

    delete_checkpoint(
        directory.path(),
        &CheckpointDeleteRequest {
            operation_uuid: Uuid::from_u128(42),
            name: "Before".into(),
            actor_uuid: None,
        },
    )
    .unwrap();
    let (replay, replayed_generation) = revert_checkpoint(
        directory.path(),
        &request,
        || panic!("published replay sampled clock"),
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(replay, receipt);
    assert_eq!(replay.prior_current_generation_uuid, Some(prior_current));
    assert_eq!(
        replayed_generation.generation_uuid(),
        restored.generation_uuid()
    );
    checkpoint_lock_handoff(
        directory.path(),
        "action=revert published-replay return",
        false,
    );

    let mut conflict = request;
    conflict.reason = "different".into();
    let conflict_error =
        revert_checkpoint(directory.path(), &conflict, || Ok(0), |_| Ok(())).unwrap_err();
    assert_eq!(conflict_error.code(), "GF_IDEMPOTENCY_CONFLICT");
    checkpoint_lock_handoff(
        directory.path(),
        "action=revert published-replay conflict return",
        false,
    );
}

#[test]
fn compact_graph_checkpoint_revert_reopens_without_a_graph_tree() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();

    let publish_compact = |payload: &[u8]| {
        let selected = crate::resolve_project_generation(directory.path()).unwrap();
        let workspace = tempdir().unwrap();
        let relative = PathBuf::from("topology/nodes/000001.parquet");
        fs::create_dir_all(workspace.path().join(relative.parent().unwrap())).unwrap();
        fs::write(workspace.path().join(&relative), payload).unwrap();
        let lease = crate::begin_graph_object_publication(directory.path()).unwrap();
        let mut state = crate::GraphManifestState::empty();
        let (graph_root, _) =
            crate::append_graph_files_v2(&lease, workspace.path(), &mut state, &[relative], &[])
                .unwrap();
        let mut participants = selected
            .participant_snapshots()
            .unwrap()
            .into_iter()
            .filter(|entry| {
                !(entry.capability_id == crate::GRAPH_CAPABILITY_ID
                    && entry.record_family_id == crate::GRAPH_FILES_FAMILY)
            })
            .map(snapshot_to_participant)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        participants.push(crate::graph_files_root_participant(&graph_root).unwrap());
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: selected
                .capabilities()
                .into_iter()
                .map(|entry| crate::ProjectCapability {
                    capability_id: entry.capability_id,
                    capability_version: entry.capability_version,
                })
                .collect(),
            participants,
        };
        let crate::ProjectStageOutcome::Staged(staged) =
            crate::stage_project_generation(directory.path(), &request).unwrap()
        else {
            panic!("fresh compact publication replayed")
        };
        staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish_with_graph_objects(&lease)
            .unwrap();
        request.generation_uuid
    };

    let checkpoint_generation = publish_compact(b"checkpoint");
    create_checkpoint(
        directory.path(),
        &create_request(Uuid::from_u128(140), "Compact"),
    )
    .unwrap();
    let newer_generation = publish_compact(b"newer");
    assert_ne!(checkpoint_generation, newer_generation);
    let (_, restored) = revert_checkpoint(
        directory.path(),
        &CheckpointRevertRequest {
            operation_uuid: Uuid::from_u128(141),
            name: "Compact".into(),
            reason: "restore compact root".into(),
            actor_uuid: None,
        },
        || Ok(1_720_000_000_123_456),
        |_| Ok(()),
    )
    .unwrap();
    assert!(!restored.graph_tree_root().exists());
    let inventory = restored.graph_files_inventory().unwrap().unwrap();
    assert_eq!(inventory.files.len(), 1);
    let bytes = crate::read_graph_object(
        directory.path(),
        &inventory.files[0].content_sha256,
        inventory.files[0].byte_length,
    )
    .unwrap();
    assert_eq!(bytes, b"checkpoint");

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let (sender, receiver) = std::sync::mpsc::channel();
    let revert_root = directory.path().to_path_buf();
    let revert_barrier = barrier.clone();
    let revert_sender = sender.clone();
    let revert_thread = std::thread::spawn(move || {
        revert_barrier.wait();
        let result = revert_checkpoint(
            &revert_root,
            &CheckpointRevertRequest {
                operation_uuid: Uuid::from_u128(142),
                name: "Compact".into(),
                reason: "concurrent compact restore".into(),
                actor_uuid: None,
            },
            || Ok(1_720_000_000_123_457),
            |_| Ok(()),
        );
        revert_sender.send(("revert", result.map(|_| ()))).unwrap();
    });
    let cleanup_root = directory.path().to_path_buf();
    let cleanup_barrier = barrier.clone();
    let cleanup_sender = sender.clone();
    let cleanup_thread = std::thread::spawn(move || {
        cleanup_barrier.wait();
        let result = crate::execute_project_cleanup(
            &cleanup_root,
            crate::ProjectRetentionPolicy::default(),
            crate::ProjectRetentionLimits::default(),
        );
        cleanup_sender
            .send(("cleanup", result.map(|_| ())))
            .unwrap();
    });
    barrier.wait();
    let mut outcomes = BTreeMap::new();
    for _ in 0..2 {
        let (operation, result) = receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("compact revert/retention lock ordering deadlocked");
        outcomes.insert(operation, result);
    }
    revert_thread.join().unwrap();
    cleanup_thread.join().unwrap();
    outcomes.remove("revert").unwrap().unwrap();
    if let Err(error) = outcomes.remove("cleanup").unwrap() {
        assert_eq!(error.code(), "GF_WRITER_BUSY");
    }
}

#[test]
fn revert_replay_lock_handoff_fails_closed_with_stable_storage_errors() {
    let checkpoint_error = finish_revert_replay_lock_handoff(
        Err(std::io::Error::other("checkpoint unlock failed")),
        Ok(()),
    )
    .unwrap_err();
    assert_eq!(checkpoint_error.code(), "GF_IO");
    assert_eq!(
        checkpoint_error.to_string(),
        "storage error: checkpoint revert replay lock handoff failed at checkpoints.lock: checkpoint unlock failed"
    );

    let writer_error = finish_revert_replay_lock_handoff(
        Ok(()),
        Err(std::io::Error::other("writer unlock failed")),
    )
    .unwrap_err();
    assert_eq!(writer_error.code(), "GF_IO");
    assert_eq!(
        writer_error.to_string(),
        "storage error: checkpoint revert replay lock handoff failed at writer.lock: writer unlock failed"
    );
}

#[test]
fn revert_validation_failure_preserves_prior_current() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    create_checkpoint(
        directory.path(),
        &create_request(Uuid::from_u128(50), "Before"),
    )
    .unwrap();
    let prior = publish_clone(directory.path());
    let error = revert_checkpoint(
        directory.path(),
        &CheckpointRevertRequest {
            operation_uuid: Uuid::from_u128(51),
            name: "Before".into(),
            reason: "must fail closed".into(),
            actor_uuid: None,
        },
        || Ok(1_720_000_000_123_456),
        |_| Err(GfError::Validation("injected composite failure".into())),
    )
    .unwrap_err();
    assert_eq!(error.code(), "GF_VALIDATION");
    checkpoint_lock_handoff(
        directory.path(),
        "action=revert validation-error return",
        false,
    );
    assert_eq!(
        crate::resolve_project_generation(directory.path())
            .unwrap()
            .generation_uuid(),
        prior
    );
    assert_eq!(list_checkpoints(directory.path()).unwrap().len(), 1);
}

#[cfg(unix)]
#[test]
fn mutation_lock_guard_unlocks_checkpoint_with_retained_duplicate_open() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    let locks = acquire_mutation_locks(directory.path()).unwrap();
    let retained = locks.checkpoint.as_ref().unwrap().try_clone().unwrap();
    drop(locks);

    let checkpoint =
        open_regular_lock(&directory.path().join(LOCKS_DIR).join(CHECKPOINT_LOCK_FILE)).unwrap();
    assert!(crate::file_lock::try_lock_exclusive(&checkpoint).unwrap());
    crate::file_lock::unlock(&checkpoint).unwrap();
    drop(retained);
}

fn expect_mutation_locks_busy(root: &Path) -> GfError {
    match acquire_mutation_locks(root) {
        Ok(_locks) => panic!("expected WriterBusy acquiring mutation locks"),
        Err(error) => error,
    }
}

fn ensure_mutation_lock_files(root: &Path) {
    let locks = acquire_mutation_locks(root).unwrap();
    drop(locks);
}

#[test]
fn shared_checkpoint_reader_blocks_mutation_with_issue_275_message() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    ensure_mutation_lock_files(directory.path());
    let error = while_checkpoint_lock_is_held(directory.path(), CheckpointLockMode::Shared, || {
        expect_mutation_locks_busy(directory.path())
    });
    assert_checkpoint_mutation_busy_message(&error);
}

#[test]
fn exclusive_checkpoint_holder_without_writer_blocks_mutation_with_issue_275_message() {
    // Models revert post-handoff / recovery-style windows: writer free,
    // checkpoints.lock still exclusive.
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    ensure_mutation_lock_files(directory.path());
    let error =
        while_checkpoint_lock_is_held(directory.path(), CheckpointLockMode::Exclusive, || {
            expect_mutation_locks_busy(directory.path())
        });
    assert_checkpoint_mutation_busy_message(&error);
}

#[test]
fn early_writer_release_while_checkpoint_held_produces_issue_275_message() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    ensure_mutation_lock_files(directory.path());
    let error = while_checkpoint_held_after_writer_released(directory.path(), || {
        expect_mutation_locks_busy(directory.path())
    });
    assert_checkpoint_mutation_busy_message(&error);
}

#[test]
fn checkpoint_read_guard_unlocks_before_a_cloned_descriptor_closes() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    let read_lock = acquire_checkpoint_read_lock(directory.path()).unwrap();
    let inherited_descriptor = read_lock.0.try_clone().unwrap();

    drop(read_lock);

    assert_mutation_locks_free(
        directory.path(),
        "after checkpoint read guard drop with a cloned descriptor",
    );
    drop(inherited_descriptor);
}

#[test]
fn open_list_then_mutation_leaves_checkpoint_locks_free() {
    // Rules out a same-thread shared-lock leak on the #275 failing sequence
    // (open/list/read then immediate mutation).
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    create_checkpoint(
        directory.path(),
        &create_request(Uuid::from_u128(275), "Before"),
    )
    .unwrap();
    let (_row, opened) = open_checkpoint_generation(directory.path(), "Before").unwrap();
    drop(opened);
    let listed = list_checkpoints(directory.path()).unwrap();
    assert_eq!(listed.len(), 1);
    assert_mutation_locks_free(
        directory.path(),
        "after open_checkpoint_generation and list",
    );

    let locks = acquire_mutation_locks(directory.path()).unwrap();
    drop(locks);
    assert_mutation_locks_free(directory.path(), "after uncontended acquire_mutation_locks");

    create_checkpoint(
        directory.path(),
        &create_request(Uuid::from_u128(276), "After"),
    )
    .unwrap();
    assert_mutation_locks_free(directory.path(), "after second create_checkpoint");
}

#[test]
fn recover_project_transactions_releases_checkpoint_before_returning() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    create_checkpoint(
        directory.path(),
        &create_request(Uuid::from_u128(277), "Retained"),
    )
    .unwrap();
    crate::recover_project_transactions(directory.path()).unwrap();
    assert_mutation_locks_free(
        directory.path(),
        "after recover_project_transactions return",
    );
}

#[test]
fn create_list_delete_and_replays_are_deterministic() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    let operation = Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000001").unwrap();
    let request = create_request(operation, "Release 1.0");
    assert_eq!(
        hex(&create_request_digest(&request, "Release 1.0")),
        "01c7bf2f2c443d85d31ff80fef4a36484e31402213e8d145371867bdb2addbe8"
    );
    let created = create_checkpoint(directory.path(), &request).unwrap();
    assert_eq!(
        created.checkpoint_uuid,
        Uuid::parse_str("4084179c-38db-8b6b-9b6e-c0b0a855e002").unwrap()
    );
    let replayed = create_checkpoint(directory.path(), &request).unwrap();
    assert_eq!(created, replayed);
    assert_eq!(created.registry_revision, 1);

    let rows = list_checkpoints(directory.path()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].checkpoint_uuid, created.checkpoint_uuid);
    assert_eq!(rows[0].generation_uuid, created.source_generation_uuid);

    let delete = CheckpointDeleteRequest {
        operation_uuid: Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000002").unwrap(),
        name: "Release 1.0".into(),
        actor_uuid: request.actor_uuid,
    };
    assert_eq!(
        hex(&delete_request_digest(&delete, "Release 1.0")),
        "9e6e15801f66ea4f7f58755c505fc1964e48ab87625459f388e2c23659135bb3"
    );
    let deleted = delete_checkpoint(directory.path(), &delete).unwrap();
    assert_eq!(
        deleted,
        delete_checkpoint(directory.path(), &delete).unwrap()
    );
    assert_eq!(deleted.registry_revision, 2);
    assert!(list_checkpoints(directory.path()).unwrap().is_empty());
    assert_eq!(
        created,
        create_checkpoint(directory.path(), &request).unwrap()
    );
    let changed_replay = create_request(operation, "Release 1.1");
    assert_eq!(
        create_checkpoint(directory.path(), &changed_replay)
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    assert!(list_checkpoints(directory.path()).unwrap().is_empty());
}

#[test]
fn identity_is_stable_across_independent_projects() {
    let first = tempdir().unwrap();
    let second = tempdir().unwrap();
    crate::open_or_initialize_project(first.path()).unwrap();
    crate::open_or_initialize_project(second.path()).unwrap();
    let request = create_request(
        Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000010").unwrap(),
        "Stable",
    );
    let left = create_checkpoint(first.path(), &request).unwrap();
    let right = create_checkpoint(second.path(), &request).unwrap();
    assert_eq!(left.checkpoint_uuid, right.checkpoint_uuid);
}

#[test]
fn opened_checkpoint_generation_remains_pinned_after_delete() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    let created = create_checkpoint(
        directory.path(),
        &create_request(Uuid::now_v7(), "Pinned View"),
    )
    .unwrap();
    let (row, opened) = open_checkpoint_generation(directory.path(), "Pinned View").unwrap();
    assert_eq!(row.checkpoint_uuid, created.checkpoint_uuid);
    assert_eq!(opened.generation_uuid(), created.source_generation_uuid);
    delete_checkpoint(
        directory.path(),
        &CheckpointDeleteRequest {
            operation_uuid: Uuid::now_v7(),
            name: "Pinned View".into(),
            actor_uuid: None,
        },
    )
    .unwrap();
    assert_eq!(
        open_checkpoint_generation(directory.path(), "Pinned View")
            .unwrap_err()
            .code(),
        "GF_CHECKPOINT_NOT_FOUND"
    );
    assert_eq!(opened.generation_uuid(), created.source_generation_uuid);
    assert!(opened.participant_snapshots().is_ok());
}

#[test]
fn conflicts_names_and_corruption_fail_closed() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    let operation = Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000020").unwrap();
    create_checkpoint(directory.path(), &create_request(operation, "Safe.Name")).unwrap();

    let conflict =
        create_checkpoint(directory.path(), &create_request(operation, "Other")).unwrap_err();
    assert_eq!(conflict.code(), "GF_IDEMPOTENCY_CONFLICT");
    let exists = create_checkpoint(
        directory.path(),
        &create_request(
            Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000021").unwrap(),
            "Safe.Name",
        ),
    )
    .unwrap_err();
    assert_eq!(exists.code(), "GF_CHECKPOINT_EXISTS");
    for invalid in ["", "../escape", "two  spaces", " e", "e ", ".", ".."] {
        let error = create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), invalid))
            .unwrap_err();
        assert_eq!(error.code(), "GF_VALIDATION", "name={invalid:?}");
    }

    fs::write(
        directory.path().join(CHECKPOINTS_DIR).join(CHECKSUM_FILE),
        b"0000000000000000000000000000000000000000000000000000000000000000\n",
    )
    .unwrap();
    let error = list_checkpoints(directory.path()).unwrap_err();
    assert_eq!(error.code(), "GF_CHECKPOINT_REGISTRY_CORRUPT");
}

#[test]
fn exact_input_bounds_and_writer_lock_are_enforced() {
    assert!(validate_name(&"a".repeat(MAX_NAME_BYTES)).is_ok());
    assert_eq!(
        validate_name(&"a".repeat(MAX_NAME_BYTES + 1))
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );
    assert!(validate_description(Some(&"d".repeat(MAX_DESCRIPTION_BYTES))).is_ok());
    assert_eq!(
        validate_description(Some(&"d".repeat(MAX_DESCRIPTION_BYTES + 1)))
            .unwrap_err()
            .code(),
        "GF_VALIDATION"
    );

    let directory = tempdir().unwrap();
    let selected = crate::open_or_initialize_project(directory.path()).unwrap();
    let root = selected.container_root().to_owned();
    let locks = acquire_mutation_locks(&root).unwrap();
    let error =
        create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), "Busy")).unwrap_err();
    assert_eq!(error.code(), "GF_WRITER_BUSY");
    drop(locks);
}

#[test]
fn no_intent_registry_corruption_wins_over_writer_contention() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), "Corrupt")).unwrap();
    let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
    fs::write(
        checkpoint_root.join(CHECKSUM_FILE),
        b"not-the-registry-digest\n",
    )
    .unwrap();

    let expected = read_registry(&checkpoint_root).unwrap_err();
    let error = while_writer_lock_is_held(directory.path(), || {
        list_checkpoints(directory.path()).unwrap_err()
    });
    assert_eq!(error.code(), "GF_CHECKPOINT_REGISTRY_CORRUPT");
    assert_eq!(error.to_string(), expected.to_string());
}

#[test]
fn intent_recovery_contention_preserves_writer_busy_and_intent() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    let checkpoint_root = directory.path().join(CHECKPOINTS_DIR);
    let child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "project_checkpoints::tests::checkpoint_failpoint_helper",
            "--ignored",
        ])
        .env(
            "GRAPHFORGE_PROJECT_FAILPOINTS",
            "graphforge-internal-subprocess-v1",
        )
        .env(
            "GRAPHFORGE_PROJECT_FAILPOINT",
            "checkpoint.registry.before_replace",
        )
        .env("GRAPHFORGE_CHECKPOINT_TEST_ROOT", directory.path())
        .spawn()
        .unwrap();
    let status = BoundedChild {
        child,
        reaped: false,
    }
    .wait("intent-recovery-contention failpoint=checkpoint.registry.before_replace");
    assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
    let intent_path = checkpoint_root.join(INTENT_FILE);
    let intent = fs::read(&intent_path).unwrap();
    let staged = fs::read_dir(&checkpoint_root)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".registry.")
        })
        .map(|entry| (entry.file_name(), fs::read(entry.path()).unwrap()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(staged.len(), 2);

    let error = while_writer_lock_is_held(directory.path(), || {
        list_checkpoints(directory.path()).unwrap_err()
    });
    assert_eq!(error.code(), "GF_WRITER_BUSY");
    assert_eq!(fs::read(intent_path).unwrap(), intent);
    let staged_after = fs::read_dir(&checkpoint_root)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".registry.")
        })
        .map(|entry| (entry.file_name(), fs::read(entry.path()).unwrap()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(staged_after, staged);
}

#[cfg(unix)]
#[test]
fn linked_project_root_is_rejected_before_checkpoint_access() {
    use std::os::unix::fs::symlink;

    let directory = tempdir().unwrap();
    let project = directory.path().join("project");
    fs::create_dir(&project).unwrap();
    crate::open_or_initialize_project(&project).unwrap();
    let linked = directory.path().join("linked-project");
    symlink(&project, &linked).unwrap();
    assert_eq!(
        list_checkpoints(&linked).unwrap_err().code(),
        "GF_UNSUPPORTED_FILESYSTEM"
    );
    assert!(!project.join(CHECKPOINTS_DIR).exists());
}

#[test]
fn checkpoint_pin_and_open_lease_control_recovery_cleanup() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    let pinned_generation = publish_clone(directory.path());
    let created =
        create_checkpoint(directory.path(), &create_request(Uuid::now_v7(), "Pinned")).unwrap();
    assert_eq!(created.source_generation_uuid, pinned_generation);
    for _ in 0..4 {
        publish_clone(directory.path());
    }
    crate::recover_project_transactions(directory.path()).unwrap();
    recover_checkpoint_pair_after_lock_handoff(
        directory.path(),
        "action=delete-pinned-checkpoint recovery-complete",
    );
    let generation_path = directory
        .path()
        .join(crate::project_publication::GENERATIONS_DIR)
        .join(pinned_generation.hyphenated().to_string());
    assert!(
        generation_path.exists(),
        "active checkpoint lost its generation"
    );

    let lease =
        crate::project_publication::open_regular_lock(&generation_path.join("lease.lock")).unwrap();
    crate::file_lock::lock_shared(&lease).unwrap();
    delete_checkpoint(
        directory.path(),
        &CheckpointDeleteRequest {
            operation_uuid: Uuid::now_v7(),
            name: "Pinned".into(),
            actor_uuid: None,
        },
    )
    .unwrap();
    crate::recover_project_transactions(directory.path()).unwrap();
    assert!(generation_path.exists(), "an open lease was invalidated");
    crate::file_lock::unlock(&lease).unwrap();
    drop(lease);
    crate::recover_project_transactions(directory.path()).unwrap();
    assert!(
        !generation_path.exists(),
        "deleted pin did not permit later GC"
    );
}

#[test]
fn checkpoint_pin_survives_process_restart() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    let pinned_generation = publish_clone(directory.path());
    create_checkpoint(
        directory.path(),
        &create_request(Uuid::from_u128(70), "Restart Pin"),
    )
    .unwrap();
    for _ in 0..4 {
        publish_clone(directory.path());
    }

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "project_checkpoints::tests::checkpoint_failpoint_helper",
            "--ignored",
            "--nocapture",
        ])
        .env("GRAPHFORGE_CHECKPOINT_TEST_ROOT", directory.path())
        .env("GRAPHFORGE_CHECKPOINT_TEST_ACTION", "hold-open")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let mut ready = String::new();
    while ready != "ready\n" {
        ready.clear();
        assert_ne!(
            output.read_line(&mut ready).unwrap(),
            0,
            "child exited before ready"
        );
    }

    delete_checkpoint(
        directory.path(),
        &CheckpointDeleteRequest {
            operation_uuid: Uuid::from_u128(71),
            name: "Restart Pin".into(),
            actor_uuid: None,
        },
    )
    .unwrap();
    crate::recover_project_transactions(directory.path()).unwrap();
    let generation_path = directory
        .path()
        .join(crate::project_publication::GENERATIONS_DIR)
        .join(pinned_generation.hyphenated().to_string());
    assert!(
        generation_path.exists(),
        "subprocess lease was not retained"
    );

    child.stdin.take().unwrap().write_all(b"release\n").unwrap();
    assert!(child.wait().unwrap().success());
    crate::recover_project_transactions(directory.path()).unwrap();
    assert!(
        !generation_path.exists(),
        "generation survived after the restarted reader released its lease"
    );
}

#[test]
fn checkpoint_cleanup_removes_all_transient_resources() {
    let directory = tempdir().unwrap();
    crate::open_or_initialize_project(directory.path()).unwrap();
    create_checkpoint(
        directory.path(),
        &create_request(Uuid::from_u128(80), "Cleanup"),
    )
    .unwrap();
    publish_clone(directory.path());
    delete_checkpoint(
        directory.path(),
        &CheckpointDeleteRequest {
            operation_uuid: Uuid::from_u128(81),
            name: "Cleanup".into(),
            actor_uuid: None,
        },
    )
    .unwrap();
    crate::recover_project_transactions(directory.path()).unwrap();

    let selected = crate::resolve_project_generation(directory.path()).unwrap();
    let root = selected.container_root().to_owned();
    drop(selected);
    let checkpoint_root = root.join(CHECKPOINTS_DIR);
    let checkpoint_entries = fs::read_dir(&checkpoint_root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        checkpoint_entries,
        BTreeSet::from([REGISTRY_FILE.into(), CHECKSUM_FILE.into()]),
        "checkpoint transaction staging leaked"
    );
    let trash = root.join("trash");
    assert!(
        !trash.exists() || fs::read_dir(&trash).unwrap().next().is_none(),
        "recovery trash was not emptied"
    );
    assert!(
        !root.join("cache").exists(),
        "checkpoint lifecycle leaked process-cache state to disk"
    );
    for entry in fs::read_dir(root.join(crate::project_publication::GENERATIONS_DIR)).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap();
        Uuid::parse_str(name).expect("generation staging entry leaked");
        let lease = open_regular_lock(&path.join("lease.lock")).unwrap();
        assert!(crate::file_lock::try_lock_exclusive(&lease).unwrap());
        crate::file_lock::unlock(&lease).unwrap();
    }
    let lock_root = root.join(LOCKS_DIR);
    for name in [WRITER_LOCK_FILE, CHECKPOINT_LOCK_FILE] {
        let lock = open_regular_lock(&lock_root.join(name)).unwrap();
        assert!(
            crate::file_lock::try_lock_exclusive(&lock).unwrap(),
            "{name} leaked"
        );
        crate::file_lock::unlock(&lock).unwrap();
    }
}

#[test]
#[ignore = "subprocess failpoint helper"]
fn checkpoint_failpoint_helper() {
    let root = std::env::var("GRAPHFORGE_CHECKPOINT_TEST_ROOT").unwrap();
    let action = std::env::var("GRAPHFORGE_CHECKPOINT_TEST_ACTION");
    if action.as_deref() == Ok("hold-open") {
        let (_, opened) = open_checkpoint_generation(root, "Restart Pin").unwrap();
        println!("ready");
        std::io::stdout().flush().unwrap();
        let mut release = String::new();
        std::io::stdin().read_line(&mut release).unwrap();
        assert_eq!(release, "release\n");
        assert!(opened.participant_snapshots().is_ok());
    } else if action.as_deref() == Ok("revert") {
        revert_checkpoint(
            root,
            &CheckpointRevertRequest {
                operation_uuid: Uuid::from_u128(61),
                name: "Base".into(),
                reason: "crash recovery".into(),
                actor_uuid: None,
            },
            || Ok(1_720_000_000_123_456),
            |_| Ok(()),
        )
        .unwrap();
    } else if action.as_deref() == Ok("delete") {
        delete_checkpoint(
            root,
            &CheckpointDeleteRequest {
                operation_uuid: Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000031").unwrap(),
                name: "Base".into(),
                actor_uuid: None,
            },
        )
        .unwrap();
    } else {
        let request = create_request(
            Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000030").unwrap(),
            "Crash",
        );
        create_checkpoint(root, &request).unwrap();
    }
}

#[test]
fn revert_publication_failpoint_matrix() {
    let failpoints = [
        ("project.after_journal_preparing", false),
        ("project.after_participant_dir_fsync", false),
        ("project.after_journal_staged", false),
        ("project.after_domain_validation", false),
        ("project.after_composite_validation", false),
        ("project.after_journal_validated", false),
        ("project.after_manifest_write", false),
        ("project.after_manifest_fsync", false),
        ("project.after_generation_dir_fsync", false),
        ("project.after_journal_durable", false),
        ("project.after_current_temp_write", false),
        ("project.after_current_temp_fsync", false),
        ("project.before_current_replace", false),
        ("project.after_current_replace", true),
        ("project.after_root_fsync", true),
        ("project.after_journal_published", true),
    ];
    for (failpoint, committed) in failpoints {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        create_checkpoint(
            directory.path(),
            &create_request(Uuid::from_u128(60), "Base"),
        )
        .unwrap();
        let prior = publish_clone(directory.path());
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "project_checkpoints::tests::checkpoint_failpoint_helper",
                "--ignored",
            ])
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
            .env("GRAPHFORGE_CHECKPOINT_TEST_ROOT", directory.path())
            .env("GRAPHFORGE_CHECKPOINT_TEST_ACTION", "revert")
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
        recover_checkpoint_pair_after_lock_handoff(
            directory.path(),
            &format!("action=revert failpoint={failpoint} committed={committed}"),
        );
        crate::recover_project_transactions(directory.path()).unwrap();
        let recovered = crate::resolve_project_generation(directory.path()).unwrap();
        assert_eq!(
            recovered.generation_uuid() != prior,
            committed,
            "{failpoint}"
        );
        recover_checkpoint_pair_after_lock_handoff(
            directory.path(),
            &format!(
                "action=revert parent-recovery-complete failpoint={failpoint} \
                     committed={committed}"
            ),
        );

        let (receipt, replayed) = revert_checkpoint(
            directory.path(),
            &CheckpointRevertRequest {
                operation_uuid: Uuid::from_u128(61),
                name: "Base".into(),
                reason: "crash recovery".into(),
                actor_uuid: None,
            },
            || Ok(1_720_000_000_123_456),
            |_| Ok(()),
        )
        .unwrap();
        assert_eq!(
            receipt.result_generation_uuid,
            Some(replayed.generation_uuid())
        );
        recover_checkpoint_pair_after_lock_handoff(
            directory.path(),
            &format!("action=revert replay-complete failpoint={failpoint} committed={committed}"),
        );
        assert_eq!(list_checkpoints(directory.path()).unwrap().len(), 1);
    }
}

#[test]
fn registry_failpoints_recover_exact_previous_or_next_revision() {
    for (failpoint, committed) in [
        ("checkpoint.registry.after_intent_file_fsync", false),
        ("checkpoint.registry.after_file_fsync", false),
        ("checkpoint.registry.before_replace", false),
        ("checkpoint.registry.after_replace", true),
        ("checkpoint.registry.after_dir_fsync", true),
    ] {
        let directory = tempdir().unwrap();
        crate::open_or_initialize_project(directory.path()).unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "project_checkpoints::tests::checkpoint_failpoint_helper",
                "--ignored",
            ])
            .env(
                "GRAPHFORGE_PROJECT_FAILPOINTS",
                "graphforge-internal-subprocess-v1",
            )
            .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
            .env("GRAPHFORGE_CHECKPOINT_TEST_ROOT", directory.path())
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
        recover_checkpoint_pair_after_lock_handoff(
            directory.path(),
            &format!("action=create-unseeded failpoint={failpoint} committed={committed}"),
        );
        let rows = list_checkpoints(directory.path()).unwrap();
        assert_eq!(rows.len(), usize::from(committed), "{failpoint}");
        if !committed {
            recover_checkpoint_pair_after_lock_handoff(
                directory.path(),
                &format!(
                    "action=create-unseeded parent-read-complete failpoint={failpoint} \
                         committed={committed}"
                ),
            );
            let request = create_request(
                Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000030").unwrap(),
                "Crash",
            );
            create_checkpoint(directory.path(), &request).unwrap();
            recover_checkpoint_pair_after_lock_handoff(
                directory.path(),
                &format!(
                    "action=create-unseeded replay-complete failpoint={failpoint} \
                         committed={committed}"
                ),
            );
            assert_eq!(list_checkpoints(directory.path()).unwrap().len(), 1);
        }
    }
}

#[test]
fn seeded_create_and_delete_failpoints_recover_exact_previous_or_next_revision() {
    let failpoints = [
        ("checkpoint.registry.after_intent_file_fsync", false),
        ("checkpoint.registry.after_file_fsync", false),
        ("checkpoint.registry.before_replace", false),
        ("checkpoint.registry.after_replace", true),
        ("checkpoint.registry.after_dir_fsync", true),
    ];
    for action in ["create", "delete"] {
        for (failpoint, committed) in failpoints {
            let directory = tempdir().unwrap();
            crate::open_or_initialize_project(directory.path()).unwrap();
            create_checkpoint(
                directory.path(),
                &create_request(
                    Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000029").unwrap(),
                    "Base",
                ),
            )
            .unwrap();
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "project_checkpoints::tests::checkpoint_failpoint_helper",
                    "--ignored",
                ])
                .env(
                    "GRAPHFORGE_PROJECT_FAILPOINTS",
                    "graphforge-internal-subprocess-v1",
                )
                .env("GRAPHFORGE_PROJECT_FAILPOINT", failpoint)
                .env("GRAPHFORGE_CHECKPOINT_TEST_ROOT", directory.path())
                .env("GRAPHFORGE_CHECKPOINT_TEST_ACTION", action)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(crate::project_failpoint::exit_code()));
            recover_checkpoint_pair_after_lock_handoff(
                directory.path(),
                &format!("action={action} failpoint={failpoint} committed={committed}"),
            );
            let rows = list_checkpoints(directory.path()).unwrap();
            let expected = match (action, committed) {
                ("create", true) => vec!["Base", "Crash"],
                ("create", false) | ("delete", false) => vec!["Base"],
                ("delete", true) => vec![],
                _ => unreachable!(),
            };
            assert_eq!(
                rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>(),
                expected,
                "{action} {failpoint}"
            );
            recover_checkpoint_pair_after_lock_handoff(
                directory.path(),
                &format!(
                    "action={action} parent-read-complete failpoint={failpoint} \
                         committed={committed}"
                ),
            );

            if action == "create" {
                let replay = create_checkpoint(
                    directory.path(),
                    &create_request(
                        Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000030").unwrap(),
                        "Crash",
                    ),
                )
                .unwrap();
                assert_eq!(replay.registry_revision, 2);
            } else {
                let replay = delete_checkpoint(
                    directory.path(),
                    &CheckpointDeleteRequest {
                        operation_uuid: Uuid::parse_str("018f0f4e-7b8c-7000-8000-000000000031")
                            .unwrap(),
                        name: "Base".into(),
                        actor_uuid: None,
                    },
                )
                .unwrap();
                assert_eq!(replay.registry_revision, 2);
            }
            recover_checkpoint_pair_after_lock_handoff(
                directory.path(),
                &format!(
                    "action={action} replay-complete failpoint={failpoint} \
                         committed={committed}"
                ),
            );
            let final_rows = list_checkpoints(directory.path()).unwrap();
            let final_names = final_rows
                .iter()
                .map(|row| row.name.as_str())
                .collect::<Vec<_>>();
            if action == "create" {
                assert_eq!(final_names, vec!["Base", "Crash"], "{failpoint}");
            } else {
                assert!(final_names.is_empty(), "{failpoint}");
            }
        }
    }
}

#[test]
fn public_checkpoint_operations_reject_cross_kind_uuid_reuse_after_reopen() {
    let root = tempdir().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let operation = Uuid::now_v7();
    create_checkpoint(root.path(), &create_request(operation, "release")).unwrap();

    let delete = CheckpointDeleteRequest {
        operation_uuid: operation,
        name: "release".into(),
        actor_uuid: None,
    };
    assert_eq!(
        delete_checkpoint(root.path(), &delete).unwrap_err().code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );

    let delete_operation = Uuid::now_v7();
    delete_checkpoint(
        root.path(),
        &CheckpointDeleteRequest {
            operation_uuid: delete_operation,
            name: "release".into(),
            actor_uuid: None,
        },
    )
    .unwrap();
    assert_eq!(
        create_checkpoint(
            root.path(),
            &create_request(delete_operation, "replacement")
        )
        .unwrap_err()
        .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
    assert!(list_checkpoints(root.path()).unwrap().is_empty());
}
