//! A research package read as a stream gets the verdict its bundle file gets.
use graphforge_api::{
    CancellationToken, ExportResearchRequest, GraphForge, PortableVerifyRequest, verify_portable,
    verify_portable_stream,
};
use graphforge_core::portable::{PortableLimits, PortableMode, PortableSource};
use std::path::Path;
use uuid::Uuid;

/// `bundle` is a canonical research bundle file: the stream over its bytes
/// reports what the file reports, except `source`, and leaves no scratch behind.
pub fn assert_stream_matches_file(bundle: &Path) {
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
pub fn assert_complete_export_streams_alike(
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
