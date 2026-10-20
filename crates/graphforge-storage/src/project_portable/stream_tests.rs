//! A bundle verified from a sequential reader gives the file verdict.
use super::*;
use std::io::Cursor;

fn file_of(bytes: &[u8]) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    fs::write(file.path(), bytes).unwrap();
    file
}

fn by_file(bytes: &[u8], limits: PortableLimits) -> Result<PortableReport, PortableError> {
    let file = file_of(bytes);
    verify_portable(file.path(), PortableMode::Full, limits, None)
}

fn by_stream(bytes: &[u8], limits: PortableLimits) -> Result<PortableReport, PortableError> {
    verify_portable_stream(&mut Cursor::new(bytes), limits, None)
}

/// A report as JSON without its `source`, the one field that may differ.
fn sans_source(report: &PortableReport) -> serde_json::Value {
    let mut value = serde_json::to_value(report).unwrap();
    assert!(value.as_object_mut().unwrap().remove("source").is_some());
    value
}

fn ontology_only_bundle() -> Vec<u8> {
    let root = super::tests::package();
    let mut paths = Vec::new();
    walk(
        root.path(),
        root.path(),
        &mut paths,
        PortableLimits::default(),
        None,
    )
    .unwrap();
    paths.sort();
    let mut bundle = Vec::new();
    for path in paths {
        bundle.extend(super::tests::tar_entry(
            &path,
            &fs::read(root.path().join(&path)).unwrap(),
        ));
    }
    bundle.extend([0u8; 1024]);
    bundle
}

/// The stream verdict equals the file verdict on `bytes`, except `source`.
pub(crate) fn assert_stream_equals_file(bytes: &[u8]) -> PortableReport {
    let file = by_file(bytes, PortableLimits::default()).unwrap();
    let stream = by_stream(bytes, PortableLimits::default()).unwrap();
    assert_eq!(file.source, PortableSource::File);
    assert_eq!(stream.source, PortableSource::Stream);
    assert_eq!(file.integrity, PortableIntegrity::Verified);
    assert_eq!(sans_source(&file), sans_source(&stream));
    file
}

#[test]
fn stream_report_equals_file_report_except_source() {
    assert_stream_equals_file(&ontology_only_bundle());
}

/// `(offset of every entry header, payload length)` of a canonical bundle
/// whose entries all use ustar headers.
fn entries(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut found = Vec::new();
    let mut at = 0;
    while bytes[at..at + 512].iter().any(|byte| *byte != 0) {
        let size = parse_octal(&bytes[at + 124..at + 136]).unwrap() as usize;
        found.push((at, size));
        at += 512 + size.div_ceil(512) * 512;
    }
    found
}

#[test]
fn truncated_bundles_fail_alike_from_a_file_and_a_stream() {
    assert_truncations_alike(&ontology_only_bundle());
}

/// Cut `bytes` mid-header, mid-payload, mid-padding, after the first end block
/// and before the end blocks: file and stream refuse every cut alike.
pub(crate) fn assert_truncations_alike(bytes: &[u8]) {
    let all = entries(bytes);
    let end = all
        .last()
        .map(|(at, size)| at + 512 + size.div_ceil(512) * 512)
        .unwrap();
    assert_eq!(bytes.len(), end + 1024);
    let (at, size) = *all
        .iter()
        .find(|(_, size)| *size >= 2 && size % 512 > 1)
        .expect("an entry with payload and padding");
    let padding = size.div_ceil(512) * 512 - size;
    assert!(padding > 1);
    let cuts = [
        ("mid-header", at + 100),
        ("mid-payload", at + 512 + size / 2),
        ("mid-padding", at + 512 + size + padding / 2),
        ("after the first end block", end + 512),
        ("before the end blocks", end),
    ];
    for (name, cut) in cuts {
        let file = by_file(&bytes[..cut], PortableLimits::default()).unwrap_err();
        let stream = by_stream(&bytes[..cut], PortableLimits::default()).unwrap_err();
        assert_eq!(file.code, PortableErrorCode::InvalidStructure, "{name}");
        assert_eq!(stream.code, file.code, "{name}");
    }
}

#[test]
fn trailing_bytes_and_the_retired_signature_are_refused_from_a_stream() {
    let mut trailing = ontology_only_bundle();
    trailing.push(7);
    let stream = by_stream(&trailing, PortableLimits::default()).unwrap_err();
    assert_eq!(stream.code, PortableErrorCode::InvalidStructure);
    assert_eq!(
        by_file(&trailing, PortableLimits::default())
            .unwrap_err()
            .code,
        stream.code
    );
    let mut retired = ontology_only_bundle();
    retired[..16].copy_from_slice(RETIRED_PROJECT_SIGNATURE);
    for result in [
        by_stream(&retired, PortableLimits::default()),
        by_file(&retired, PortableLimits::default()),
    ] {
        assert_eq!(
            result.unwrap_err().code,
            PortableErrorCode::UnsupportedProjectFormat
        );
    }
}

/// A bundle with composition documents that Full verification reads after the
/// scan: the default pool verifies it, a pool smaller than its largest document refuses it at a
/// component entry, identically from a file and a stream.
pub(crate) fn assert_pool_bounds_retention(bytes: &[u8]) {
    let default = assert_stream_equals_file(bytes);
    assert!(
        !default.ontology_composition_entries.is_empty(),
        "the bundle must carry documents that Full verification reads after the scan"
    );
    let largest = default
        .ontology_composition_entries
        .iter()
        .map(|entry| entry.length)
        .max()
        .unwrap();
    for pool in [0, largest - 1] {
        let limits = PortableLimits {
            max_retained_control_bytes: pool,
            ..PortableLimits::default()
        };
        for result in [by_file(bytes, limits), by_stream(bytes, limits)] {
            let error = result.unwrap_err();
            assert_eq!(error.code, PortableErrorCode::LimitExceeded, "pool {pool}");
            assert!(
                error
                    .entry
                    .as_deref()
                    .is_some_and(|entry| entry.starts_with("data/components/")),
                "pool {pool}: {error:?}"
            );
        }
    }
}

#[test]
fn a_package_without_research_files_writes_no_stage() {
    let scratch = tempfile::tempdir().unwrap();
    let mut source = Cursor::new(ontology_only_bundle());
    let scan = scan_stream(
        &mut source,
        PortableLimits::default(),
        None,
        Some(scratch.path()),
    )
    .unwrap();
    assert!(!scan.report.research_interchange);
    let staged = fs::read_dir(scratch.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .flat_map(|dir| walk_files(&dir))
        .count();
    assert_eq!(staged, 0, "a graph package stages no file");
}

fn walk_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(walk_files(&path));
        } else {
            files.push(path);
        }
    }
    files
}

/// A `Read` that builds its bytes one part at a time and records how much was
/// consumed and the largest single read request, so a test can tell a bounded
/// streaming reader from one that buffers its input.
struct Generated {
    parts: Box<dyn Iterator<Item = Vec<u8>>>,
    current: Cursor<Vec<u8>>,
    consumed: std::rc::Rc<std::cell::Cell<u64>>,
    largest_request: std::rc::Rc<std::cell::Cell<usize>>,
}

impl Generated {
    fn new(parts: impl Iterator<Item = Vec<u8>> + 'static) -> (Self, GeneratedStats) {
        let consumed = std::rc::Rc::new(std::cell::Cell::new(0));
        let largest_request = std::rc::Rc::new(std::cell::Cell::new(0));
        let stats = GeneratedStats {
            consumed: consumed.clone(),
            largest_request: largest_request.clone(),
        };
        (
            Self {
                parts: Box::new(parts),
                current: Cursor::new(Vec::new()),
                consumed,
                largest_request,
            },
            stats,
        )
    }
}

struct GeneratedStats {
    consumed: std::rc::Rc<std::cell::Cell<u64>>,
    largest_request: std::rc::Rc<std::cell::Cell<usize>>,
}

impl Read for Generated {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.largest_request
            .set(self.largest_request.get().max(buf.len()));
        loop {
            let count = self.current.read(buf)?;
            if count > 0 || buf.is_empty() {
                self.consumed.set(self.consumed.get() + count as u64);
                return Ok(count);
            }
            match self.parts.next() {
                Some(part) => self.current = Cursor::new(part),
                None => return Ok(0),
            }
        }
    }
}

const MIB: usize = 1024 * 1024;

#[test]
fn a_repeated_fixed_control_is_refused_at_its_second_occurrence() {
    // Fixed controls are kept outside the shared pool, so only canonical
    // order stops a stream of them from growing memory with its length.
    let parts = (0..64)
        .map(|_| super::tests::tar_entry("bag-info.txt", &vec![0; MIB]))
        .chain(std::iter::once(vec![0; 1024]));
    let (mut reader, stats) = Generated::new(parts);
    let limits = PortableLimits {
        max_retained_control_bytes: 0,
        ..PortableLimits::default()
    };
    let error = verify_portable_stream(&mut reader, limits, None).unwrap_err();
    assert_eq!(error.code, PortableErrorCode::InvalidStructure);
    assert_eq!(error.entry.as_deref(), Some("bag-info.txt"));
    // The first occurrence was read; the second was refused at its header.
    assert!(
        stats.consumed.get() < (MIB + 4096) as u64,
        "read {} bytes before refusing the second occurrence",
        stats.consumed.get()
    );
}

#[test]
fn the_stream_entry_point_does_not_buffer_its_input() {
    // 128 MiB of entries that no semantic read needs, far beyond a 1 KiB pool,
    // then a header that is not a tar header. A reader that buffered the input
    // would consume all of it before it noticed.
    let parts = (0..64)
        .map(|index| {
            super::tests::tar_entry(
                &format!("data/components/graph-data/graph-tree/part-{index:03}.bin"),
                &vec![0; 2 * MIB],
            )
        })
        .take(3)
        .chain(std::iter::once(vec![0xff; 512]))
        .chain((3..64).map(|index| {
            super::tests::tar_entry(
                &format!("data/components/graph-data/graph-tree/part-{index:03}.bin"),
                &vec![0; 2 * MIB],
            )
        }));
    let (mut reader, stats) = Generated::new(parts);
    let limits = PortableLimits {
        max_retained_control_bytes: 1024,
        ..PortableLimits::default()
    };
    let error = verify_portable_stream(&mut reader, limits, None).unwrap_err();
    assert_eq!(error.code, PortableErrorCode::InvalidStructure);
    assert!(
        stats.consumed.get() <= (3 * (2 * MIB + 512) + 1024) as u64,
        "read {} bytes of a 128 MiB stream before the fault",
        stats.consumed.get()
    );
    // Every read is a header or one bounded copy buffer.
    assert!(
        stats.largest_request.get() <= limits.copy_buffer_bytes.max(512),
        "a single read asked for {} bytes",
        stats.largest_request.get()
    );
}

#[test]
fn pax_headers_do_not_count_against_entry_or_byte_limits_on_either_path() {
    const PAX_PATH: &str = "data/components/ontology/core-ontology/ontology.json";
    let path = pax::record("path", PAX_PATH);
    let _limit = pax::test_seam::lower_ustar_size_limit(1);
    let records = format!("{path}{}", pax::record("size", "2"));
    let bundle = fs::read(super::tests::bundle_with_pax_entry(&records, 0).path()).unwrap();
    let report = assert_stream_equals_file(&bundle);
    assert_eq!(report.representation, PortableRepresentation::Bundle);
    let regular = report.entry_count;
    let bytes = report.payload_bytes;
    let at = |max_entries, max_total_bytes| PortableLimits {
        max_entries,
        max_total_bytes,
        ..PortableLimits::default()
    };
    // The census is the regular entries and their bytes, with or without PAX.
    let admitted = at(regular, bytes);
    let from_file = by_file(&bundle, admitted).unwrap();
    let from_stream = by_stream(&bundle, admitted).unwrap();
    assert_eq!(sans_source(&from_file), sans_source(&from_stream));
    for refused in [
        at(regular - 1, PortableLimits::default().max_total_bytes),
        at(PortableLimits::default().max_entries, bytes - 1),
    ] {
        let file = by_file(&bundle, refused).unwrap_err();
        let stream = by_stream(&bundle, refused).unwrap_err();
        assert_eq!(file.code, PortableErrorCode::LimitExceeded);
        assert_eq!(stream.code, file.code);
    }
}
