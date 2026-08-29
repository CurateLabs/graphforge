//! Test-only construction of a committed graph snapshot from a raw workspace.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use graphforge_core::canonical::{CANONICAL_CONTRACT_VERSION, CanonicalDomain, fingerprint};
use graphforge_core::uuid::Uuid;
use graphforge_storage::{
    GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, ProjectCapability, ProjectGenerationRequest,
    ProjectParticipant, ProjectParticipantEncoding, ProjectStageOutcome, UuidIndexBuildLimits,
    capture_graph_files, rebuild_v4_ordinal_identity, resolve_project_generation,
    stage_project_generation_with_graph_tree,
};

const GRAPH_SNAPSHOT_SCHEMA_CANONICAL_BYTES: &[u8] =
    b"graph_snapshot/1|relative_path:utf8:not-null|content:binary:not-null";

pub(crate) fn publish_graph_workspace(container: &Path, workspace: &Path) {
    let parent = graphforge_storage::open_or_initialize_project(container).unwrap();
    let mut files = Vec::new();
    collect_files(workspace, workspace, &mut files);
    files.sort_by(|left, right| left.0.cmp(&right.0));

    let schema = Arc::new(Schema::new(vec![
        Field::new("relative_path", DataType::Utf8, false),
        Field::new("content", DataType::Binary, false),
    ]));
    let paths = files
        .iter()
        .map(|(path, _)| path.as_str())
        .collect::<Vec<_>>();
    let contents = files
        .iter()
        .map(|(_, bytes)| bytes.as_slice())
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(paths)) as ArrayRef,
            Arc::new(BinaryArray::from_vec(contents)),
        ],
    )
    .unwrap();
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut bytes, &schema).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let graph_participant = ProjectParticipant {
        capability_id: "graph".into(),
        capability_version: 1,
        record_family_id: "snapshot".into(),
        record_version: 1,
        encoding: ProjectParticipantEncoding::Arrow,
        schema_fingerprint: fingerprint(
            CanonicalDomain::Schema,
            CANONICAL_CONTRACT_VERSION,
            GRAPH_SNAPSHOT_SCHEMA_CANONICAL_BYTES,
        )
        .unwrap(),
        row_count: batch.num_rows() as u64,
        bytes,
    };
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities: vec![
            ProjectCapability {
                capability_id: "graph".into(),
                capability_version: 1,
            },
            ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants: {
            let mut participants = vec![graph_participant];
            participants.extend(graphforge_storage::empty_workspace_participants().unwrap());
            participants
        },
    };
    let ProjectStageOutcome::Staged(staged) =
        graphforge_storage::stage_project_generation(container, &request).unwrap()
    else {
        panic!("fresh fixture publication unexpectedly replayed");
    };
    let expected_parent = parent.generation_uuid();
    staged
        .validate(
            |_| Ok(()),
            |actual_parent, _| {
                assert_eq!(actual_parent.generation_uuid(), expected_parent);
                Ok(())
            },
        )
        .unwrap()
        .publish()
        .unwrap();
}

/// Publish through the compact graph-files v2 authority after constructing the
/// v4 ordinal identity artifacts in the exact workspace being committed.
pub(crate) fn publish_graph_workspace_v4(container: &Path, workspace: &Path) {
    let _ = graphforge_storage::open_or_initialize_project(container).unwrap();
    rebuild_v4_ordinal_identity(workspace, UuidIndexBuildLimits::default()).unwrap();
    let parent = resolve_project_generation(container).unwrap();
    let expected_parent = parent.generation_uuid();
    drop(parent);

    let (_, graph_participant) = capture_graph_files(workspace).unwrap();
    let mut participants = graphforge_storage::empty_workspace_participants().unwrap();
    participants.insert(0, graph_participant);
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
        capabilities: vec![
            ProjectCapability {
                capability_id: GRAPH_CAPABILITY_ID.into(),
                capability_version: GRAPH_CAPABILITY_VERSION,
            },
            ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_with_graph_tree(container, &request, Some(workspace)).unwrap()
    else {
        panic!("fresh v4 fixture publication unexpectedly replayed");
    };
    staged
        .validate(
            |_| Ok(()),
            |actual_parent, _| {
                assert_eq!(actual_parent.generation_uuid(), expected_parent);
                Ok(())
            },
        )
        .unwrap()
        .publish()
        .unwrap();
}

fn collect_files(root: &Path, directory: &Path, files: &mut Vec<(String, Vec<u8>)>) {
    let mut entries = fs::read_dir(directory)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let file_type = entry.file_type().unwrap();
        if file_type.is_dir() {
            collect_files(root, &path, files);
        } else if file_type.is_file() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".lock") || name.starts_with(".gf-stage-") {
                continue;
            }
            let relative: PathBuf = path.strip_prefix(root).unwrap().into();
            files.push((
                relative.to_str().expect("fixture path is UTF-8").to_owned(),
                fs::read(path).unwrap(),
            ));
        } else {
            panic!("fixture workspace contains a special file");
        }
    }
}
