//! Test support: a research package read as a stream gets the verdict its
//! bundle file gets.
use crate::{
    CancellationToken, ExportResearchRequest, GraphForge, PortableVerifyRequest, verify_portable,
    verify_portable_stream,
};
use graphforge_core::portable::{PortableLimits, PortableMode, PortableSource};
use std::path::Path;
use uuid::Uuid;

/// `bundle` is a canonical research bundle file: the stream over its bytes
/// reports what the file reports, except `source`, and leaves no scratch behind.
pub(crate) fn assert_stream_matches_file(bundle: &Path) {
    let limits = PortableLimits::default();
    let file = verify_portable(
        &PortableVerifyRequest {
            input: bundle.to_path_buf(),
            mode: PortableMode::Full,
            limits,
        },
        None,
    )
    .unwrap();
    assert!(file.research_interchange);
    let scratch = tempfile::tempdir().unwrap();
    let stream = verify_portable_stream(
        &mut std::io::BufReader::new(std::fs::File::open(bundle).unwrap()),
        Some(scratch.path()),
        limits,
        None,
    )
    .unwrap();
    assert_eq!(file.source, PortableSource::File);
    assert_eq!(stream.source, PortableSource::Stream);
    let strip = |report: &graphforge_core::portable::PortableReport| {
        let mut value = serde_json::to_value(report).unwrap();
        value.as_object_mut().unwrap().remove("source").unwrap();
        value
    };
    assert_eq!(strip(&file), strip(&stream));
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
}

/// The same check for a package exported as an expanded directory: the
/// complete Version is exported again as a bundle.
pub(crate) fn assert_complete_export_streams_alike(
    graph: &GraphForge,
    version_uuid: Uuid,
    scratch: &Path,
) {
    let bundle = scratch.join(format!("{version_uuid}-stream-parity.gfpb"));
    graph
        .export_research(
            &ExportResearchRequest {
                version_uuid,
                output: bundle.clone(),
                bundled: true,
                projection: None,
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_stream_matches_file(&bundle);
}

/// A Parquet file whose pages use an encoding outside the funded decoder profile.
pub(crate) fn unfunded_parquet() -> Vec<u8> {
    use arrow::array::{ArrayRef, RecordBatch, StringArray};
    use arrow::datatypes::{Field, Schema};
    use parquet::{
        arrow::ArrowWriter,
        basic::{Compression, Encoding},
        file::properties::{WriterProperties, WriterVersion},
    };
    let values: ArrayRef = std::sync::Arc::new(StringArray::from_iter_values(
        (0..64).map(|row| format!("value-{row}")),
    ));
    let schema = std::sync::Arc::new(Schema::new(vec![Field::new(
        "value",
        values.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
    let properties = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::DELTA_BYTE_ARRAY)
        .build();
    let mut output = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut output, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    output
}
