//! Same-binary integration coverage for checkpoint CLI contracts.

use std::io::Cursor;
use std::process::{Command, Output};

use arrow::array::FixedSizeBinaryArray;
use arrow::ipc::reader::StreamReader;
use tempfile::TempDir;
use uuid::Uuid;

fn gf(project: &TempDir, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(project.path())
        .args(args)
        .output()
        .expect("run same-build gf binary")
}

fn read_ipc(output: &Output) -> (Vec<String>, Vec<arrow::record_batch::RecordBatch>) {
    assert!(
        output.status.success(),
        "gf failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reader = StreamReader::try_new(Cursor::new(&output.stdout), None)
        .expect("stdout is an Arrow IPC stream");
    let columns = reader
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("read Arrow IPC batches");
    (columns, batches)
}

#[test]
fn checkpoint_cli_emits_arrow_and_stable_errors() {
    let project = TempDir::new().expect("temporary project");
    let create = gf(
        &project,
        &[
            "checkpoint",
            "create",
            "before-change",
            "--idempotency-key",
            "00000000-0000-0000-0000-000000000001",
        ],
    );
    let (create_columns, create_batches) = read_ipc(&create);
    assert_eq!(
        create_columns,
        [
            "operation",
            "operation_uuid",
            "checkpoint_uuid",
            "name",
            "source_generation_uuid",
            "result_generation_uuid",
            "registry_revision",
            "committed_at",
        ]
    );
    assert_eq!(
        create_batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    let checkpoint_ids = create_batches[0]
        .column_by_name("checkpoint_uuid")
        .expect("checkpoint_uuid column")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("checkpoint_uuid is fixed-size binary");
    let checkpoint_id = Uuid::from_slice(checkpoint_ids.value(0)).expect("16-byte checkpoint UUID");
    assert_eq!(checkpoint_id.as_bytes(), checkpoint_ids.value(0));

    let (list_columns, list_batches) = read_ipc(&gf(&project, &["checkpoint", "list"]));
    assert_eq!(
        list_columns,
        [
            "checkpoint_uuid",
            "name",
            "description",
            "generation_uuid",
            "generation_manifest_sha256",
            "created_at",
            "created_by",
        ]
    );
    assert_eq!(
        list_batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );

    let missing = gf(
        &project,
        &[
            "checkpoint",
            "open",
            "absent",
            "--",
            "MATCH",
            "(n)",
            "RETURN",
            "n",
        ],
    );
    assert!(!missing.status.success());
    assert!(
        String::from_utf8_lossy(&missing.stderr).starts_with("GF_CHECKPOINT_NOT_FOUND:"),
        "stable error code is emitted: {}",
        String::from_utf8_lossy(&missing.stderr)
    );
}
