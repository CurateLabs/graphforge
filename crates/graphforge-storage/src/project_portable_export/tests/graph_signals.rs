//! The Parquet footer signals of a verification report: rows by kind of file,
//! the same from every package form and every way of reading it.

use super::parquet_profile::generation_with_graph_files;
use super::*;
use parquet::basic::Encoding;
use parquet::file::properties::WriterVersion;

#[path = "../../../tests/support/hostile_parquet.rs"]
mod hostile;

const NODES: usize = 100;
const EDGES: usize = 250;
const PROPERTIES: usize = 7;
const UPPERCASE: usize = 5;

fn plain(rows: usize) -> Vec<u8> {
    hostile::string_file(Encoding::PLAIN, WriterVersion::PARQUET_1_0, rows)
}

/// A graph tree with nodes, two edge shards, a property file, a `.parquet`
/// name that is not Parquet, and a file that is not Parquet by name.
fn graph_tree() -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("topology/nodes.parquet", plain(NODES)),
        ("topology/edges/0.parquet", plain(EDGES - 50)),
        ("topology/edges/1.parquet", plain(50)),
        ("properties/names.parquet", plain(PROPERTIES)),
        ("other/UPPER.PARQUET", plain(UPPERCASE)),
        ("properties/empty.parquet", b"not parquet".to_vec()),
        ("notes.txt", plain(9)),
    ]
}

fn packages(files: &[(&'static str, Vec<u8>)]) -> (tempfile::TempDir, PathBuf, PathBuf) {
    let borrowed: Vec<_> = files
        .iter()
        .map(|(path, bytes)| (*path, &bytes[..]))
        .collect();
    let (_project, generation) = generation_with_graph_files(&borrowed);
    let plan = plan_complete_portable(&generation, PortableExportLimits::default()).unwrap();
    let outputs = tempfile::tempdir().unwrap();
    let (expanded, bundle) = write_test_representations(&plan, outputs.path());
    (outputs, expanded, bundle)
}

fn row_groups(files: &[(&'static str, Vec<u8>)]) -> u64 {
    files
        .iter()
        .filter(|(path, bytes)| {
            path.to_ascii_lowercase().ends_with(".parquet") && bytes.ends_with(b"PAR1")
        })
        .map(|(_, bytes)| hostile::metadata(bytes).num_row_groups() as u64)
        .sum()
}

#[test]
fn footer_rows_are_reported_by_kind_alike_from_every_form_and_source() {
    let files = graph_tree();
    let (_outputs, expanded, bundle) = packages(&files);
    let from_directory = crate::verify_portable(
        &expanded,
        PortableMode::Full,
        PortableLimits::default(),
        None,
    )
    .unwrap();
    let from_file =
        crate::verify_portable(&bundle, PortableMode::Full, PortableLimits::default(), None)
            .unwrap();
    let mut reader = std::io::BufReader::new(fs::File::open(&bundle).unwrap());
    let from_stream =
        crate::verify_portable_stream(&mut reader, None, PortableLimits::default(), None).unwrap();
    let expected = graphforge_core::portable::PortableParquetSignals {
        // Six Parquet names, in either case; the one that is not Parquet has no
        // footer. `notes.txt` is Parquet by content only and is not counted.
        files: 6,
        files_with_footer: 5,
        row_groups: row_groups(&files),
        node_rows: NODES as u64,
        edge_rows: EDGES as u64,
        property_rows: (PROPERTIES + UPPERCASE) as u64,
    };
    for report in [&from_directory, &from_file, &from_stream] {
        assert_eq!(report.plausibility.parquet, Some(expected));
    }
    assert_eq!(from_directory.plausibility, from_file.plausibility);
    assert_eq!(from_file.plausibility, from_stream.plausibility);
    assert_eq!(from_file.bytes_by_kind, from_stream.bytes_by_kind);
    assert!(from_file.plausibility.graph_data_bytes > 0);
    assert!(from_file.plausibility.graph_data_ratio_ppm > 0);
}

/// A file whose footer is large: many row groups, each with its own metadata.
fn wide_footer() -> Vec<u8> {
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_size(4)
        .build();
    hostile::write(hostile::strings(2_000), false, properties)
}

fn footer_bytes(file: &[u8]) -> u64 {
    let end = file.len();
    u64::from(u32::from_le_bytes(
        file[end - 8..end - 4].try_into().unwrap(),
    ))
}

#[test]
fn a_footer_over_the_reporting_cap_is_admitted_but_not_reported() {
    let mut files = graph_tree();
    let wide = wide_footer();
    let small = files
        .iter()
        .filter(|(path, bytes)| path.ends_with(".parquet") && bytes.ends_with(b"PAR1"))
        .map(|(_, bytes)| footer_bytes(bytes))
        .max()
        .unwrap();
    let cap = footer_bytes(&wide) - 1;
    assert!(small <= cap, "the cap must pass the ordinary footers");
    files.push(("properties/wide.parquet", wide));
    let (_outputs, expanded, bundle) = packages(&files);
    let limits = PortableLimits {
        max_parquet_footer_bytes: cap,
        ..PortableLimits::default()
    };
    let mut reader = std::io::BufReader::new(fs::File::open(&bundle).unwrap());
    let from_stream = crate::verify_portable_stream(&mut reader, None, limits, None).unwrap();
    let default =
        crate::verify_portable(&bundle, PortableMode::Full, PortableLimits::default(), None)
            .unwrap();
    let default = default.plausibility.parquet.unwrap();
    for report in [
        crate::verify_portable(&expanded, PortableMode::Full, limits, None).unwrap(),
        crate::verify_portable(&bundle, PortableMode::Full, limits, None).unwrap(),
        from_stream,
    ] {
        assert_eq!(report.integrity, crate::PortableIntegrity::Verified);
        let parquet = report.plausibility.parquet.unwrap();
        assert_eq!(parquet.files, default.files);
        assert_eq!(parquet.files_with_footer, default.files_with_footer - 1);
        // Its rows and row groups are not reported either.
        assert_eq!(parquet.property_rows, default.property_rows - 2_000);
        assert!(parquet.row_groups < default.row_groups);
    }
}

#[test]
fn a_footer_over_the_stream_walk_threshold_reports_what_a_file_reports() {
    let files = graph_tree();
    let (_outputs, _, bundle) = packages(&files);
    // A threshold below every entry sends each through the scratch file.
    let limits = PortableLimits {
        max_parquet_walk_bytes: 16,
        ..PortableLimits::default()
    };
    let scratch = tempfile::tempdir().unwrap();
    let mut reader = std::io::BufReader::new(fs::File::open(&bundle).unwrap());
    let spilled =
        crate::verify_portable_stream(&mut reader, Some(scratch.path()), limits, None).unwrap();
    let file = crate::verify_portable(&bundle, PortableMode::Full, limits, None).unwrap();
    assert_eq!(spilled.plausibility, file.plausibility);
    assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
}

#[test]
fn a_footer_declaring_an_unfunded_encoding_is_refused_alike_everywhere() {
    let funded = plain(64);
    let lying = hostile::declaring(&funded, &[Encoding::DELTA_LENGTH_BYTE_ARRAY]);
    let files = vec![("topology/nodes.parquet", lying)];
    let (_outputs, expanded, bundle) = packages(&files);
    let mut reader = std::io::BufReader::new(fs::File::open(&bundle).unwrap());
    let streamed =
        crate::verify_portable_stream(&mut reader, None, PortableLimits::default(), None)
            .unwrap_err();
    for error in [
        crate::verify_portable(
            &expanded,
            PortableMode::Full,
            PortableLimits::default(),
            None,
        )
        .unwrap_err(),
        crate::verify_portable(&bundle, PortableMode::Full, PortableLimits::default(), None)
            .unwrap_err(),
        streamed,
    ] {
        assert_eq!(error.code, PortableErrorCode::LimitExceeded, "{error}");
        assert_eq!(
            error.entry.as_deref(),
            Some("data/components/graph-data/graph-tree/topology/nodes.parquet")
        );
    }
}
