//! What a package's bytes are: per-kind totals and the ratio of graph data,
//! reported alike from a file and a stream, over packages the writer produced
//! and packages built to look like one.
use super::stream_tests::{Generated, MIB, by_file, by_stream, sans_source};
use super::*;

const HUB_BUNDLE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/fixtures/hub/generated/v1/objects/openalex-openalex.gfpb"
);
const EVIDENCE_PATH: &str = "data/components/evidence/scan-0001/scan.bin";
const GIB: u64 = 1024 * 1024 * 1024;

/// The `(path, payload)` of every regular entry of a canonical bundle, with
/// the paths of PAX-addressed entries restored.
fn read_tar(bytes: &[u8]) -> Vec<(String, Vec<u8>)> {
    let mut entries = Vec::new();
    let mut at = 0;
    let mut pax_path: Option<String> = None;
    while bytes[at..at + 512].iter().any(|byte| *byte != 0) {
        let header = &bytes[at..at + 512];
        let size = parse_octal(&header[124..136]).unwrap() as usize;
        let payload = &bytes[at + 512..at + 512 + size];
        at += 512 + size.div_ceil(512) * 512;
        if header[156] == b'x' {
            let records = std::str::from_utf8(payload).unwrap();
            pax_path = records
                .split('\n')
                .find_map(|record| record.split_once(" path=").map(|(_, path)| path.to_owned()));
            continue;
        }
        let path = pax_path.take().unwrap_or_else(|| {
            let name = cstr(&header[..100]).unwrap();
            match cstr(&header[345..500]).unwrap() {
                "" => name.to_owned(),
                prefix => format!("{prefix}/{name}"),
            }
        });
        entries.push((path, payload.to_vec()));
    }
    entries
}

fn bundle_of(mut entries: Vec<(String, Vec<u8>)>) -> Vec<u8> {
    entries.sort();
    let mut bundle = Vec::new();
    for (path, payload) in entries {
        super::tests::append_entry(&mut bundle, &path, payload);
    }
    bundle.extend([0u8; 1024]);
    bundle
}

/// What the report's totals must be, counted from the entries alone: the
/// directory under `data/components/` names the kind, and every entry outside
/// `data/components/` is package control.
fn census(entries: &[(String, u64)]) -> BTreeMap<String, u64> {
    let mut totals = BTreeMap::<String, u64>::new();
    for (path, length) in entries {
        let kind = path
            .strip_prefix("data/components/")
            .and_then(|rest| rest.split('/').next())
            .unwrap_or("package-control");
        *totals.entry(kind.to_owned()).or_default() += length;
    }
    totals
}

fn assert_totals_equal_the_census(report: &PortableReport, entries: &[(String, u64)]) {
    assert_eq!(report.bytes_by_kind, census(entries));
    assert_eq!(
        report.bytes_by_kind.values().sum::<u64>(),
        report.payload_bytes
    );
    let graph = report.bytes_by_kind.get("graph-data").copied().unwrap_or(0);
    assert_eq!(report.plausibility.graph_data_bytes, graph);
    assert_eq!(
        u128::from(report.plausibility.graph_data_ratio_ppm),
        u128::from(graph) * 1_000_000 / u128::from(report.payload_bytes)
    );
}

fn lengths(entries: &[(String, Vec<u8>)]) -> Vec<(String, u64)> {
    entries
        .iter()
        .map(|(path, payload)| (path.clone(), payload.len() as u64))
        .collect()
}

#[test]
fn bytes_by_kind_sum_to_the_payload_for_every_positive_package() {
    // The writer's own bundle: every kind it produces, graph data included.
    let hub = read_tar(&fs::read(HUB_BUNDLE).unwrap());
    let report = super::stream_tests::assert_stream_equals_file(&bundle_of(hub.clone()));
    assert_totals_equal_the_census(&report, &lengths(&hub));
    assert!(report.plausibility.graph_data_bytes > 0, "no graph data");
    assert!(
        report.bytes_by_kind.len() >= 4,
        "{:?}",
        report.bytes_by_kind
    );
    // The fixture package, as a bundle and as a directory.
    let fixture = super::tests::package();
    let mut paths = Vec::new();
    walk(
        fixture.path(),
        fixture.path(),
        &mut paths,
        PortableLimits::default(),
        None,
    )
    .unwrap();
    let files: Vec<_> = paths
        .iter()
        .map(|path| (path.clone(), fs::read(fixture.path().join(path)).unwrap()))
        .collect();
    let expanded = verify_portable(
        fixture.path(),
        PortableMode::Full,
        PortableLimits::default(),
        None,
    )
    .unwrap();
    let bundled = super::stream_tests::assert_stream_equals_file(&bundle_of(files.clone()));
    assert_totals_equal_the_census(&expanded, &lengths(&files));
    assert_eq!(expanded.bytes_by_kind, bundled.bytes_by_kind);
    assert_eq!(expanded.plausibility, bundled.plausibility);
}

/// One entry of a package under construction.
enum Body {
    Bytes(Vec<u8>),
    Zeros(u64),
}

impl Body {
    fn length(&self) -> u64 {
        match self {
            Body::Bytes(bytes) => bytes.len() as u64,
            Body::Zeros(length) => *length,
        }
    }
}

/// SHA-256 of `length` zero bytes, streamed.
fn zero_digest(length: u64) -> [u8; 32] {
    let mut hash = Sha256::new();
    let block = vec![0u8; MIB];
    let mut left = length;
    while left > 0 {
        let count = left.min(MIB as u64) as usize;
        hash.update(&block[..count]);
        left -= count as u64;
    }
    hash.finalize().into()
}

/// The fixture package plus one `evidence` component whose only file is
/// `length` zero bytes with SHA-256 `digest`. Every control the package carries
/// is rebuilt to describe it, so the package is valid.
fn padded_package(length: u64, digest: [u8; 32]) -> Vec<(String, Body)> {
    let mut manifest: Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/portable/ontology-only.manifest.json"
    ))
    .unwrap();
    let object = manifest.as_object_mut().unwrap();
    object.remove("package_digest");
    object.insert("package_class".into(), "component-selective".into());
    object["requirements"]["capabilities"] = serde_json::json!(["evidence@1", "ontology@1"]);
    object["selection"]["roots"] = serde_json::json!(["core-ontology", "scan-0001"]);
    object["components"].as_array_mut().unwrap().insert(
        0,
        serde_json::json!({
            "kind": "evidence",
            "files": [{
                "length": length,
                "media_type": "application/octet-stream",
                "path": EVIDENCE_PATH,
                "sha256": hex(&digest),
            }],
            "participant_id": "scan-0001",
            "required_dependencies": [],
        }),
    );
    let semantic = canonical_json(&manifest).unwrap();
    let package_digest = hex(&Sha256::digest(
        [b"graphforge-project/2\0".as_slice(), semantic.as_slice()].concat(),
    ));
    manifest["package_digest"] = format!("sha256:{package_digest}").into();
    let manifest = canonical_json(&manifest).unwrap();

    let ontology_path = "data/components/ontology/core-ontology/ontology.json";
    let data = [
        (EVIDENCE_PATH, digest),
        (ontology_path, Sha256::digest(b"{}").into()),
        (MANIFEST_PATH, Sha256::digest(&manifest).into()),
    ];
    let inventory: String = data
        .iter()
        .map(|(path, digest)| format!("{}  {path}\n", hex(digest)))
        .collect();
    let tags = format!(
        "{}  bag-info.txt\n{}  bagit.txt\n{}  manifest-sha256.txt\n",
        hex(&Sha256::digest(BAG_INFO)),
        hex(&Sha256::digest(BAGIT)),
        hex(&Sha256::digest(inventory.as_bytes()))
    );
    vec![
        ("bag-info.txt".into(), Body::Bytes(BAG_INFO.to_vec())),
        ("bagit.txt".into(), Body::Bytes(BAGIT.to_vec())),
        (EVIDENCE_PATH.into(), Body::Zeros(length)),
        (ontology_path.into(), Body::Bytes(b"{}".to_vec())),
        (MANIFEST_PATH.into(), Body::Bytes(manifest)),
        ("manifest-sha256.txt".into(), Body::Bytes(inventory.into())),
        ("tagmanifest-sha256.txt".into(), Body::Bytes(tags.into())),
    ]
}

/// The canonical bundle of `entries`, produced one part at a time so that a
/// zero-filled entry is never held whole. Zero entries must fill whole blocks.
fn bundle_stream(entries: Vec<(String, Body)>) -> impl Iterator<Item = Vec<u8>> {
    entries
        .into_iter()
        .flat_map(|(path, body)| -> Box<dyn Iterator<Item = Vec<u8>>> {
            match body {
                Body::Bytes(bytes) => {
                    Box::new(std::iter::once(super::tests::tar_entry(&path, &bytes)))
                }
                Body::Zeros(length) => {
                    assert_eq!(length % 512, 0, "a zero entry fills whole blocks");
                    let blocks = length / MIB as u64;
                    let rest = (length % MIB as u64) as usize;
                    Box::new(
                        std::iter::once(super::tests::tar_header(&path, length))
                            .chain((0..blocks).map(|_| vec![0u8; MIB]))
                            .chain((rest > 0).then(|| vec![0u8; rest])),
                    )
                }
            }
        })
        .chain(std::iter::once(vec![0u8; 1024]))
}

fn bundle_bytes(entries: Vec<(String, Body)>) -> Vec<u8> {
    bundle_stream(entries).flatten().collect()
}

#[test]
fn a_padded_package_reports_the_evidence_alike_from_a_file_and_a_stream() {
    let length = 8 * MIB as u64;
    let package = padded_package(length, zero_digest(length));
    let census_input: Vec<_> = package
        .iter()
        .map(|(path, body)| (path.clone(), body.length()))
        .collect();
    let bytes = bundle_bytes(package);
    let file = by_file(&bytes, PortableLimits::default()).unwrap();
    let stream = by_stream(&bytes, PortableLimits::default()).unwrap();
    assert_eq!(sans_source(&file), sans_source(&stream));
    assert_totals_equal_the_census(&stream, &census_input);
    assert_eq!(stream.bytes_by_kind["evidence"], length);
    assert_eq!(stream.plausibility.graph_data_bytes, 0);
    assert_eq!(stream.plausibility.graph_data_ratio_ppm, 0);
}

/// Peak resident memory of this process so far, in bytes.
#[cfg(target_os = "linux")]
fn peak_resident_bytes() -> u64 {
    let status = fs::read_to_string("/proc/self/status").unwrap();
    let line = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .expect("VmHWM");
    line.split_whitespace()
        .nth(1)
        .unwrap()
        .parse::<u64>()
        .unwrap()
        * 1024
}

/// A 3 GiB opaque artifact in an otherwise valid package: a stream verifies it
/// without holding it, writes nothing to disk, and reports it as evidence.
#[cfg(target_os = "linux")]
#[test]
fn a_three_gib_evidence_entry_streams_in_bounded_memory_with_no_disk() {
    let length = 3 * GIB;
    let package = padded_package(length, zero_digest(length));
    let census_input: Vec<_> = package
        .iter()
        .map(|(path, body)| (path.clone(), body.length()))
        .collect();
    let expected_stream: u64 = census_input
        .iter()
        .map(|(_, length)| length.div_ceil(512) * 512 + 512)
        .sum::<u64>()
        + 1024;
    let (mut reader, stats) = Generated::new(bundle_stream(package));
    let scratch = tempfile::tempdir().unwrap();
    let report = verify_portable_stream(
        &mut reader,
        Some(scratch.path()),
        PortableLimits::default(),
        None,
    )
    .unwrap();
    assert_eq!(stats.consumed.get(), expected_stream, "the whole stream");
    assert_eq!(report.source, PortableSource::Stream);
    assert_eq!(report.integrity, PortableIntegrity::Verified);
    assert_eq!(report.bytes_by_kind["evidence"], length);
    assert_totals_equal_the_census(&report, &census_input);
    assert!(
        report.plausibility.graph_data_ratio_ppm < 10_000,
        "{:?}",
        report.plausibility
    );
    assert_eq!(
        fs::read_dir(scratch.path()).unwrap().count(),
        0,
        "the stream wrote to disk"
    );
    let peak = peak_resident_bytes();
    assert!(peak < GIB, "peak resident memory was {peak} bytes");
}

/// The Hub bundle with its semantic manifest replaced by `manifest`.
fn impostor(manifest: Vec<u8>) -> Vec<u8> {
    let mut entries = read_tar(&fs::read(HUB_BUNDLE).unwrap());
    let slot = entries
        .iter_mut()
        .find(|(path, _)| path == MANIFEST_PATH)
        .unwrap();
    slot.1 = manifest;
    bundle_of(entries)
}

fn assert_refused_alike(bytes: &[u8], code: PortableErrorCode, entry: Option<&str>, detail: &str) {
    let file = by_file(bytes, PortableLimits::default()).unwrap_err();
    let stream = by_stream(bytes, PortableLimits::default()).unwrap_err();
    for error in [&file, &stream] {
        assert_eq!(error.code, code, "{error}");
        assert_eq!(error.entry.as_deref(), entry, "{error}");
        assert!(error.to_string().contains(detail), "{error}");
    }
    assert_eq!(file.to_string(), stream.to_string());
}

#[test]
fn an_impostor_manifest_is_refused_alike_from_a_file_and_a_stream() {
    // A well-formed, self-consistent manifest for a different package: its
    // digest is right, and it describes entries this archive does not hold.
    let foreign: Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/portable/ontology-only.manifest.json"
    ))
    .unwrap();
    assert_refused_alike(
        &impostor(canonical_json(&foreign).unwrap()),
        PortableErrorCode::InvalidStructure,
        Some("data/components/ontology/core-ontology/ontology.json"),
        "declared payload missing",
    );
    // Not JSON at all.
    assert_refused_alike(
        &impostor(b"this is not a manifest".to_vec()),
        PortableErrorCode::InvalidStructure,
        Some(MANIFEST_PATH),
        "",
    );
}

#[test]
fn parquet_signals_are_absent_unless_the_pages_were_read() {
    let hub = read_tar(&fs::read(HUB_BUNDLE).unwrap());
    let bytes = bundle_of(hub);
    let file = tempfile::NamedTempFile::new().unwrap();
    fs::write(file.path(), &bytes).unwrap();
    let inspected = verify_portable(
        file.path(),
        PortableMode::StructureOnly,
        PortableLimits::default(),
        None,
    )
    .unwrap();
    assert_eq!(inspected.plausibility.parquet, None);
    let full = by_file(&bytes, PortableLimits::default()).unwrap();
    let stream = by_stream(&bytes, PortableLimits::default()).unwrap();
    assert_eq!(full.plausibility.parquet, stream.plausibility.parquet);
    assert!(full.plausibility.parquet.is_some());
    assert_eq!(inspected.bytes_by_kind, full.bytes_by_kind);
}
