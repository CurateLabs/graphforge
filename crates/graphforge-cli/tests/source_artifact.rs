//! CLI integration test for the Source and Artifact lifecycle (#1349).
//!
//! Mirrors the Node binding lifecycle test: scan → OCR → preference → impact,
//! then reopens the project and verifies lineage, closure, and updated impact.
//! Skips when the underlying filesystem is not admitted (tmpfs / overlay).

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

/// Returns true if the project directory is on an unsupported filesystem.
fn filesystem_unsupported(project: &TempDir) -> bool {
    let probe = gf(project, &["source-artifact", "project-capabilities"]);
    if probe.status.success() {
        return false;
    }
    String::from_utf8_lossy(&probe.stderr).contains("GF_UNSUPPORTED_FILESYSTEM")
}

fn success_ipc(output: &Output) -> (Vec<String>, Vec<arrow::record_batch::RecordBatch>) {
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
        .map(|f| f.name().clone())
        .collect();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("read Arrow IPC batches");
    (columns, batches)
}

fn row_count(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches
        .iter()
        .map(arrow::record_batch::RecordBatch::num_rows)
        .sum()
}

fn uuid_column(batches: &[arrow::record_batch::RecordBatch], name: &str) -> Vec<Uuid> {
    batches
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            (0..batch.num_rows())
                .map(|i| Uuid::from_slice(col.value(i)).unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
#[allow(clippy::too_many_lines)]
fn source_artifact_lifecycle_survives_reopen() {
    let project = TempDir::new().expect("temporary project");

    if filesystem_unsupported(&project) {
        return;
    }

    // Fixed UUIDs to mirror the Node test data.
    let source_uuid = "018f0f4e-7b8c-7000-8000-000000001301";
    let scan_uuid = "018f0f4e-7b8c-7000-8000-000000001302";
    let ocr_uuid = "018f0f4e-7b8c-7000-8000-000000001303";
    let preference_uuid = "018f0f4e-7b8c-7000-8000-000000001304";

    // Enable required capabilities.
    let en_prov = gf(
        &project,
        &[
            "source-artifact",
            "enable-capability",
            "--operation-uuid",
            "018f0f4e-7b8c-7000-8000-000000001100",
            "--capability-id",
            "provenance",
        ],
    );
    assert!(
        en_prov.status.success(),
        "enable provenance failed: {}",
        String::from_utf8_lossy(&en_prov.stderr)
    );

    let en_know = gf(
        &project,
        &[
            "source-artifact",
            "enable-capability",
            "--operation-uuid",
            "018f0f4e-7b8c-7000-8000-000000001101",
            "--capability-id",
            "knowledge",
        ],
    );
    assert!(
        en_know.status.success(),
        "enable knowledge failed: {}",
        String::from_utf8_lossy(&en_know.stderr)
    );

    // Register the Source.
    let reg_source = gf(
        &project,
        &[
            "source-artifact",
            "register-source",
            "--operation-uuid",
            "018f0f4e-7b8c-7000-8000-000000001201",
            "--source-uuid",
            source_uuid,
            "--label",
            "Codex A",
            "--source-kind",
            "manuscript",
        ],
    );
    let (src_cols, src_batches) = success_ipc(&reg_source);
    assert!(src_cols.contains(&"source_uuid".to_owned()));
    assert_eq!(row_count(&src_batches), 1);

    // Register the raw-scan Artifact (local bytes).
    let scan_payload_file = {
        let file = tempfile::NamedTempFile::new_in(project.path()).unwrap();
        std::fs::write(file.path(), b"scan bytes").unwrap();
        file
    };
    let reg_scan = gf(
        &project,
        &[
            "source-artifact",
            "register-artifact",
            "--operation-uuid",
            "018f0f4e-7b8c-7000-8000-000000001202",
            "--artifact-uuid",
            scan_uuid,
            "--source-uuid",
            source_uuid,
            "--artifact-kind",
            "raw_scan",
            "--media-type",
            "image/tiff",
            "--payload-file",
            scan_payload_file.path().to_str().unwrap(),
        ],
    );
    let (scan_cols, scan_batches) = success_ipc(&reg_scan);
    assert!(scan_cols.contains(&"artifact_uuid".to_owned()));
    assert_eq!(row_count(&scan_batches), 1);

    // Register the OCR Artifact derived from the scan.
    let ocr_payload_file = {
        let file = tempfile::NamedTempFile::new_in(project.path()).unwrap();
        std::fs::write(file.path(), b"ocr text").unwrap();
        file
    };
    let derivation_input = format!("{scan_uuid}:artifact");
    let reg_ocr = gf(
        &project,
        &[
            "source-artifact",
            "register-artifact",
            "--operation-uuid",
            "018f0f4e-7b8c-7000-8000-000000001203",
            "--artifact-uuid",
            ocr_uuid,
            "--source-uuid",
            source_uuid,
            "--artifact-kind",
            "ocr_text",
            "--media-type",
            "text/plain",
            "--payload-file",
            ocr_payload_file.path().to_str().unwrap(),
            "--derivation-input",
            &derivation_input,
        ],
    );
    success_ipc(&reg_ocr);

    // Set scan as the initially preferred Artifact.
    let set_pref = gf(
        &project,
        &[
            "source-artifact",
            "set-preferred-artifact",
            "--operation-uuid",
            "018f0f4e-7b8c-7000-8000-000000001204",
            "--preference-event-uuid",
            preference_uuid,
            "--source-uuid",
            source_uuid,
            "--artifact-uuid",
            scan_uuid,
            "--reason",
            "initial preferred scan",
        ],
    );
    success_ipc(&set_pref);

    // Check replacement impact: switching to ocr should report scan and ocr.
    let impact = gf(
        &project,
        &[
            "source-artifact",
            "replacement-impact",
            "--source-uuid",
            source_uuid,
            "--artifact-uuid",
            ocr_uuid,
        ],
    );
    let (_, impact_batches) = success_ipc(&impact);
    assert_eq!(row_count(&impact_batches), 2, "expected 2 impact rows");
    let impacted = uuid_column(&impact_batches, "artifact_uuid");
    let scan = Uuid::parse_str(scan_uuid).unwrap();
    let ocr = Uuid::parse_str(ocr_uuid).unwrap();
    assert!(impacted.contains(&scan));
    assert!(impacted.contains(&ocr));

    // Switch preference to OCR.
    let set_pref2 = gf(
        &project,
        &[
            "source-artifact",
            "set-preferred-artifact",
            "--operation-uuid",
            "018f0f4e-7b8c-7000-8000-000000001205",
            "--preference-event-uuid",
            "018f0f4e-7b8c-7000-8000-000000001305",
            "--source-uuid",
            source_uuid,
            "--artifact-uuid",
            ocr_uuid,
            "--reason",
            "better OCR available",
        ],
    );
    success_ipc(&set_pref2);

    // --- Reopen: create a new TempDir-backed project path by keeping the same path ---
    let reopened_project_path = project.path().to_path_buf();
    let reopened = Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(&reopened_project_path)
        .args([
            "source-artifact",
            "research-lineage",
            "--subject-uuid",
            ocr_uuid,
            "--subject-kind",
            "artifact",
            "--direction",
            "backward",
            "--max-depth",
            "4",
        ])
        .output()
        .expect("run gf binary for lineage");
    let (_, lineage_batches) = success_ipc(&reopened);
    assert_eq!(row_count(&lineage_batches), 1, "expected 1 backward edge");
    let input_uuids = uuid_column(&lineage_batches, "input_uuid");
    assert_eq!(
        input_uuids,
        vec![scan],
        "backward lineage must point to scan"
    );

    // Retention dependency closure for the source should be empty (OCR is preferred, no dependents).
    let closure = Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(&reopened_project_path)
        .args([
            "source-artifact",
            "retention-dependency-closure",
            "--scope-uuid",
            source_uuid,
        ])
        .output()
        .expect("run gf binary for closure");
    let (_, closure_batches) = success_ipc(&closure);
    assert_eq!(
        row_count(&closure_batches),
        0,
        "retention closure must be empty"
    );

    // Post-reopen impact: switching back to scan should report only OCR (the current preferred).
    let post_impact = Command::new(env!("CARGO_BIN_EXE_gf"))
        .arg("--project")
        .arg(&reopened_project_path)
        .args([
            "source-artifact",
            "replacement-impact",
            "--source-uuid",
            source_uuid,
            "--artifact-uuid",
            scan_uuid,
        ])
        .output()
        .expect("run gf binary for post-reopen impact");
    let (_, post_impact_batches) = success_ipc(&post_impact);
    assert_eq!(
        row_count(&post_impact_batches),
        1,
        "post-reopen impact must be exactly ocr"
    );
    let post_impacted = uuid_column(&post_impact_batches, "artifact_uuid");
    assert_eq!(post_impacted, vec![ocr]);
}
