//! Inspect authenticated published payloads in existing public lifecycle tests.

use std::path::Path;

use graphforge_storage::ResolvedProjectGeneration;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;

fn assert_metadata(metadata: &ParquetMetaData, name: &str) -> usize {
    let mut columns = 0;
    for group in metadata.row_groups() {
        assert!(
            group.num_rows() <= 1_048_576,
            "{name}: default row-group ceiling"
        );
        for column in group.columns() {
            assert!(
                matches!(column.compression(), parquet::basic::Compression::ZSTD(_)),
                "{name}: permanent column {} lost Zstd",
                column.column_path()
            );
            columns += 1;
        }
    }
    columns
}

pub(crate) fn assert_file(path: &Path) {
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap()).unwrap();
    assert!(assert_metadata(reader.metadata(), &path.display().to_string()) > 0);
}

pub(crate) fn assert_graph(generation: &ResolvedProjectGeneration) {
    let inventory = generation.graph_files_inventory().unwrap().unwrap();
    let owned = generation
        .declared_graph_files_inventory()
        .unwrap()
        .is_some();
    let mut columns = 0;
    for entry in inventory
        .files
        .iter()
        .filter(|entry| entry.relative_path.ends_with(".parquet"))
    {
        let path = if owned {
            generation.graph_tree_root().join(&entry.relative_path)
        } else {
            graphforge_storage::graph_object_path(
                generation.container_root(),
                &entry.content_sha256,
            )
            .unwrap()
        };
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap()).unwrap();
        columns += assert_metadata(reader.metadata(), &entry.relative_path);
    }
    assert!(columns > 0, "published graph fixture must contain data");
}

pub(crate) fn assert_participants(root: &Path, capability: &str) {
    let generation = graphforge_storage::resolve_project_generation(root).unwrap();
    let mut columns = 0;
    for participant in generation
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .filter(|participant| {
            participant.capability_id == capability && participant.encoding == "parquet"
        })
    {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(participant.bytes))
                .unwrap();
        columns += assert_metadata(reader.metadata(), &participant.record_family_id);
        if participant.record_family_id == "restoration_transition" {
            assert_eq!(
                reader.metadata().file_metadata().created_by(),
                Some("graphforge-restoration-transition/1")
            );
        }
    }
    assert!(
        columns > 0,
        "{capability}: public fixture must publish Parquet data"
    );
}
