//! Same-binary integration coverage for checkpoint CLI contracts.

use std::io::Cursor;
use std::process::{Command, Output};

use arrow::array::FixedSizeBinaryArray;
use arrow::ipc::reader::StreamReader;
use serde_json::Value;
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
            "prior_current_generation_uuid",
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
    let (show_columns, show_batches) =
        read_ipc(&gf(&project, &["checkpoint", "show", "before-change"]));
    assert_eq!(show_columns, list_columns);
    assert_eq!(show_batches, list_batches);

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

#[test]
fn checkpoint_cli_json_is_schema_first_and_canonical() {
    let project = TempDir::new().expect("temporary project");
    let create = gf(
        &project,
        &[
            "--json",
            "checkpoint",
            "create",
            "before-change",
            "--idempotency-key",
            "00000000-0000-0000-0000-000000000001",
        ],
    );
    assert!(
        create.status.success(),
        "{}",
        String::from_utf8_lossy(&create.stderr)
    );
    let raw = String::from_utf8(create.stdout).unwrap();
    assert!(raw.starts_with("{\"contract\":\"graphforge-cli-result/1\",\"columns\":"));
    let value: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        value["columns"][0],
        serde_json::json!({
            "name": "operation",
            "data_type": "Utf8",
            "nullable": false
        })
    );
    assert_eq!(value["rows"][0][1], "00000000-0000-0000-0000-000000000001");
    assert!(
        value["rows"][0][2]
            .as_str()
            .is_some_and(|checkpoint| Uuid::parse_str(checkpoint).is_ok())
    );
}

#[test]
fn revert_preview_is_non_mutating_and_automation_requires_yes() {
    let project = TempDir::new().expect("temporary project");
    assert!(
        gf(
            &project,
            &[
                "checkpoint",
                "create",
                "before-change",
                "--idempotency-key",
                "00000000-0000-0000-0000-000000000011",
            ],
        )
        .status
        .success()
    );
    let current_path = project.path().join("CURRENT");
    let before = std::fs::read(&current_path).unwrap();

    let preview = gf(
        &project,
        &["--json", "revert", "before-change", "--preview"],
    );
    assert!(
        preview.status.success(),
        "{}",
        String::from_utf8_lossy(&preview.stderr)
    );
    let preview: Value = serde_json::from_slice(&preview.stdout).unwrap();
    assert_eq!(preview["contract"], "graphforge-revert-preview/1");
    assert_eq!(std::fs::read(&current_path).unwrap(), before);

    let refused = gf(
        &project,
        &[
            "--json",
            "revert",
            "before-change",
            "--reason",
            "restore known state",
            "--idempotency-key",
            "00000000-0000-0000-0000-000000000012",
        ],
    );
    assert_eq!(refused.status.code(), Some(2));
    let error: Value = serde_json::from_slice(&refused.stderr).unwrap();
    assert_eq!(error["error"]["code"], "GF_VALIDATION");
    assert_eq!(std::fs::read(&current_path).unwrap(), before);

    let accepted = gf(
        &project,
        &[
            "--json",
            "revert",
            "before-change",
            "--reason",
            "restore known state",
            "--idempotency-key",
            "00000000-0000-0000-0000-000000000012",
            "--yes",
        ],
    );
    assert!(
        accepted.status.success(),
        "{}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    let accepted: Value = serde_json::from_slice(&accepted.stdout).unwrap();
    let columns = accepted["columns"].as_array().unwrap();
    let prior_index = columns
        .iter()
        .position(|column| column["name"] == "prior_current_generation_uuid")
        .unwrap();
    assert_eq!(
        accepted["rows"][0][prior_index],
        preview["current_generation_uuid"]
    );
    assert_ne!(std::fs::read(&current_path).unwrap(), before);
}

#[test]
fn preview_and_refusal_do_not_initialize_or_reconcile_a_project() {
    for preview in [true, false] {
        let project = TempDir::new().expect("temporary project");
        let args = if preview {
            vec!["--json", "revert", "missing", "--preview"]
        } else {
            vec![
                "--json",
                "revert",
                "missing",
                "--reason",
                "must not open",
                "--idempotency-key",
                "00000000-0000-0000-0000-000000000021",
            ]
        };
        let output = gf(&project, &args);
        assert!(!output.status.success());
        assert!(
            std::fs::read_dir(project.path()).unwrap().next().is_none(),
            "preview/refusal must not initialize the project"
        );
    }
}
