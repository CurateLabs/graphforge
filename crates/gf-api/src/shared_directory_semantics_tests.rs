use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::Duration;

use arrow::array::{Array, StringArray};
use gf_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
    ProjectStageOutcome,
};
use uuid::Uuid;

use crate::{GfError, GraphForge};

const DEADLINE: Duration = Duration::from_secs(10);
const READ_NAMES: &str = "MATCH (n:Person) RETURN n.name AS name ORDER BY name";

fn seed(root: &std::path::Path) {
    let graph = GraphForge::new(root.to_str()).expect("phase=seed open");
    graph
        .execute("CREATE (:Person {name:'Alpha'})")
        .expect("phase=seed publish");
}

fn names(graph: &GraphForge) -> Result<Vec<String>, GfError> {
    let result = graph.execute(READ_NAMES)?;
    let mut names = Vec::new();
    for batch in result.batches {
        let values = batch
            .column_by_name("name")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| GfError::Execution("ordered name result is malformed".into()))?;
        names.extend((0..values.len()).map(|row| values.value(row).to_owned()));
    }
    Ok(names)
}

fn recv<T>(receiver: &mpsc::Receiver<T>, phase: &str) -> T {
    receiver
        .recv_timeout(DEADLINE)
        .unwrap_or_else(|error| panic!("phase={phase} timeout={DEADLINE:?} channel={error}"))
}

fn publication_request(root: &std::path::Path) -> ProjectGenerationRequest {
    let selected = gf_storage::resolve_project_generation(root).expect("phase=writer-lock resolve");
    let capabilities = selected
        .capabilities()
        .into_iter()
        .map(|entry| ProjectCapability {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
        })
        .collect();
    let participants = selected
        .participant_snapshots()
        .expect("phase=writer-lock snapshots")
        .into_iter()
        .map(|entry| ProjectParticipant {
            capability_id: entry.capability_id,
            capability_version: entry.capability_version,
            record_family_id: entry.record_family_id,
            record_version: entry.record_version,
            encoding: match entry.encoding.as_str() {
                "arrow" => ProjectParticipantEncoding::Arrow,
                "json" => ProjectParticipantEncoding::Json,
                "parquet" => ProjectParticipantEncoding::Parquet,
                other => panic!("phase=writer-lock unsupported encoding={other}"),
            },
            schema_fingerprint: entry.schema_fingerprint,
            row_count: entry.row_count,
            bytes: entry.bytes,
        })
        .collect();
    ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities,
        participants,
    }
}

#[test]
fn shared_directory_reads_pin_complete_generations_and_reopen_sees_commit() {
    let project = tempfile::tempdir().expect("phase=read-write tempdir");
    seed(project.path());
    let initial = gf_storage::resolve_project_generation(project.path())
        .expect("phase=read-write initial generation");
    initial
        .validate_complete_participant_inventory()
        .expect("phase=read-write initial inventory");
    let initial_uuid = initial.generation_uuid();

    let long_reader = Arc::new(
        GraphForge::new(project.path().to_str()).expect("phase=read-read long reader open"),
    );
    let second_reader =
        GraphForge::new(project.path().to_str()).expect("phase=read-read second reader open");
    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = mpsc::sync_channel(2);
    let first_handle = {
        let reader = Arc::clone(&long_reader);
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        thread::spawn(move || {
            barrier.wait();
            let _ = sender.send(("long", names(&reader)));
        })
    };
    let second_handle = {
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        thread::spawn(move || {
            barrier.wait();
            let _ = sender.send(("second", names(&second_reader)));
        })
    };
    drop(sender);
    for completed in 0..2 {
        let (reader, result) = recv(&receiver, "read-read");
        assert_eq!(
            result.unwrap_or_else(|error| {
                panic!("phase=read-read reader={reader} completed={completed}/2 error={error}")
            }),
            ["Alpha"]
        );
    }
    first_handle
        .join()
        .expect("phase=read-read long reader panicked");
    second_handle
        .join()
        .expect("phase=read-read second reader panicked");

    let writer = GraphForge::new(project.path().to_str()).expect("phase=read-write writer open");
    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = mpsc::sync_channel(2);
    let read_handle = {
        let reader = Arc::clone(&long_reader);
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        thread::spawn(move || {
            barrier.wait();
            let _ = sender.send(("reader", names(&reader).map(|rows| rows == ["Alpha"])));
        })
    };
    let write_handle = {
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        thread::spawn(move || {
            barrier.wait();
            let result = writer
                .execute("CREATE (:Person {name:'Beta'})")
                .map(|_| true);
            let _ = sender.send(("writer", result));
        })
    };
    drop(sender);
    for completed in 0..2 {
        let (actor, result) = recv(&receiver, "read-write");
        assert!(
            result.unwrap_or_else(|error| {
                panic!("phase=read-write actor={actor} completed={completed}/2 error={error}")
            }),
            "phase=read-write actor={actor} observed unexpected state"
        );
    }
    read_handle
        .join()
        .expect("phase=read-write reader panicked");
    write_handle
        .join()
        .expect("phase=read-write writer panicked");

    assert_eq!(
        names(&long_reader).expect("phase=long-reader post-commit read"),
        ["Alpha"],
        "phase=long-reader visibility changed after another session committed"
    );
    let committed = gf_storage::resolve_project_generation(project.path())
        .expect("phase=reopen resolve committed generation");
    committed
        .validate_complete_participant_inventory()
        .expect("phase=reopen committed inventory");
    assert_ne!(committed.generation_uuid(), initial_uuid);
    let reopened = GraphForge::new(project.path().to_str()).expect("phase=reopen facade");
    assert_eq!(
        names(&reopened).expect("phase=reopen ordered read"),
        ["Alpha", "Beta"]
    );
}

#[test]
fn competing_writer_fails_before_its_staging_or_publication() {
    let project = tempfile::tempdir().expect("phase=write-write tempdir");
    seed(project.path());
    let initial = gf_storage::resolve_project_generation(project.path())
        .expect("phase=write-write initial generation");
    let initial_uuid = initial.generation_uuid();
    let holder_request = publication_request(project.path());
    let holder = match gf_storage::stage_project_generation(project.path(), &holder_request)
        .expect("phase=write-write holder stage")
    {
        ProjectStageOutcome::Staged(holder) => holder,
        ProjectStageOutcome::AlreadyPublished(_) => {
            panic!("phase=write-write fresh holder unexpectedly replayed")
        }
    };
    let transactions = project.path().join("transactions");
    let staged_before = std::fs::read_dir(&transactions)
        .expect("phase=write-write transaction inventory before")
        .count();
    let contender_request = publication_request(project.path());
    let root = project.path().to_owned();
    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = mpsc::sync_channel(1);
    let worker = {
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            let result = gf_storage::stage_project_generation(&root, &contender_request)
                .map(|_| ())
                .map_err(|error| (error.code().to_owned(), error.to_string()));
            let _ = sender.send(result);
        })
    };
    barrier.wait();
    let error = recv(&receiver, "write-write contender")
        .expect_err("phase=write-write contender unexpectedly staged");
    assert_eq!(
        error.0, "GF_WRITER_BUSY",
        "phase=write-write error={error:?}"
    );
    worker.join().expect("phase=write-write contender panicked");
    assert_eq!(
        std::fs::read_dir(&transactions)
            .expect("phase=write-write transaction inventory after")
            .count(),
        staged_before,
        "phase=write-write contender created staging state before WriterBusy"
    );
    let observed = gf_storage::resolve_project_generation(project.path())
        .expect("phase=write-write resolve while holder staged");
    observed
        .validate_complete_participant_inventory()
        .expect("phase=write-write observed inventory");
    assert_eq!(observed.generation_uuid(), initial_uuid);
    drop(holder);
}
