//! What a package's bytes are: per-kind totals and the ratio of graph data,
//! reported alike from a file and a stream, over packages the writer produced
//! and packages built to look like one.
use super::stream_tests::{Generated, MIB, by_file, by_stream, sans_source};
use super::*;
use parquet::basic::Encoding;
use parquet::file::properties::WriterVersion;

#[path = "../../tests/support/hostile_parquet.rs"]
mod hostile;

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
/// `evidence`, with SHA-256 `digest`. Every control the package carries is
/// rebuilt to describe it, so the package is valid.
fn evidence_package(evidence: Body, digest: [u8; 32]) -> Vec<(String, Body)> {
    component_package("evidence", "scan-0001", EVIDENCE_PATH, evidence, digest)
}

/// [`evidence_package`] for a component of any `kind`, whose only file is
/// `path`.
fn component_package(
    kind: &str,
    participant: &str,
    path: &str,
    file: Body,
    digest: [u8; 32],
) -> Vec<(String, Body)> {
    let evidence = file;
    let length = evidence.length();
    let mut manifest: Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/portable/ontology-only.manifest.json"
    ))
    .unwrap();
    let object = manifest.as_object_mut().unwrap();
    object.remove("package_digest");
    object.insert("package_class".into(), "component-selective".into());
    object["requirements"]["capabilities"] = serde_json::json!([format!("{kind}@1"), "ontology@1"]);
    object["selection"]["roots"] = serde_json::json!(["core-ontology", participant]);
    object["components"].as_array_mut().unwrap().insert(
        0,
        serde_json::json!({
            "kind": kind,
            "files": [{
                "length": length,
                "media_type": "application/octet-stream",
                "path": path,
                "sha256": hex(&digest),
            }],
            "participant_id": participant,
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
        (path, digest),
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
        (path.into(), evidence),
        (ontology_path.into(), Body::Bytes(b"{}".to_vec())),
        (MANIFEST_PATH.into(), Body::Bytes(manifest)),
        ("manifest-sha256.txt".into(), Body::Bytes(inventory.into())),
        ("tagmanifest-sha256.txt".into(), Body::Bytes(tags.into())),
    ]
}

/// The canonical bundle of `entries`, produced one part at a time so that a
/// zero-filled entry is never held whole. Zero entries must fill whole blocks.
/// [`evidence_package`] around `length` zero bytes.
fn padded_package(length: u64, digest: [u8; 32]) -> Vec<(String, Body)> {
    evidence_package(Body::Zeros(length), digest)
}

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
fn evidence_that_is_itself_parquet_is_never_walked_on_either_path() {
    // Unfunded pages would be refused anywhere a decoder could reach them.
    let unfunded = hostile::string_file(
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
        WriterVersion::PARQUET_1_0,
        64,
    );
    let digest = Sha256::digest(&unfunded).into();
    let bytes = bundle_bytes(evidence_package(Body::Bytes(unfunded.clone()), digest));
    // Under a lowered threshold the same bytes would spill to scratch if walked.
    let limits = PortableLimits {
        max_parquet_walk_bytes: 16,
        ..PortableLimits::default()
    };
    let file = by_file(&bytes, limits).unwrap();
    let stream = by_stream(&bytes, limits).unwrap();
    assert_eq!(sans_source(&file), sans_source(&stream));
    assert_eq!(stream.bytes_by_kind["evidence"], unfunded.len() as u64);
    assert_eq!(
        stream.plausibility.parquet.map(|parquet| parquet.files),
        Some(0)
    );
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

/// Bytes this thread has passed to `write(2)` so far, to any file. The scan
/// runs on the calling thread, so other tests' writes do not count.
#[cfg(target_os = "linux")]
fn written_bytes() -> u64 {
    let io = fs::read_to_string("/proc/thread-self/io").unwrap();
    let line = io
        .lines()
        .find(|line| line.starts_with("wchar:"))
        .expect("wchar");
    line.split_whitespace().nth(1).unwrap().parse().unwrap()
}

/// A 5 GiB opaque artifact in an otherwise valid package: a stream verifies it
/// without holding it, writes next to nothing to disk, and reports it as
/// evidence.
#[cfg(target_os = "linux")]
#[test]
fn a_five_gib_evidence_entry_streams_in_bounded_memory_with_no_disk() {
    let length = 5 * GIB;
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
    let written_before = written_bytes();
    let report = verify_portable_stream(
        &mut reader,
        Some(scratch.path()),
        PortableLimits::default(),
        None,
    )
    .unwrap();
    let written = written_bytes() - written_before;
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
    // Opaque evidence is never staged for a page walk: the scan writes nothing
    // but what the stream's own controls need, whatever the entry's size. The
    // staging writes run on this thread, so its `wchar` is all of them.
    println!("evidence_entry_bytes={length} written_bytes={written}");
    assert!(written < MIB as u64, "the stream wrote {written} bytes");
    let peak = peak_resident_bytes();
    assert!(peak < GIB, "peak resident memory was {peak} bytes");
}

/// A graph Parquet entry above `max_parquet_walk_bytes` is staged once in an
/// anonymous scratch file, whole, and removed: its writes are the entry, never
/// more, and the same entry under the default threshold is written nowhere.
#[cfg(target_os = "linux")]
#[test]
fn a_graph_entry_just_over_the_walk_threshold_is_written_once_and_removed() {
    let file = hostile::string_file(Encoding::PLAIN, WriterVersion::PARQUET_1_0, 64);
    let entry = file.len() as u64;
    let threshold = entry - 1;
    let digest = Sha256::digest(&file).into();
    let package = || {
        component_package(
            "graph-data",
            "graph-tree",
            "data/components/graph-data/graph-tree/topology/nodes.parquet",
            Body::Bytes(file.clone()),
            digest,
        )
    };
    let stream = |limits: PortableLimits| {
        let scratch = tempfile::tempdir().unwrap();
        let (mut reader, _) = Generated::new(bundle_stream(package()));
        let before = written_bytes();
        let report =
            verify_portable_stream(&mut reader, Some(scratch.path()), limits, None).unwrap();
        let written = written_bytes() - before;
        assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
        assert_eq!(report.integrity, PortableIntegrity::Verified);
        assert_eq!(report.bytes_by_kind["graph-data"], entry);
        // Walked, not merely hashed: the page walk read the staged copy.
        let parquet = report.plausibility.parquet.unwrap();
        assert_eq!((parquet.files, parquet.files_with_footer), (1, 1));
        written
    };
    let staged = stream(PortableLimits {
        max_parquet_walk_bytes: threshold,
        ..PortableLimits::default()
    });
    assert!(
        (threshold..=entry).contains(&staged),
        "staged {staged} bytes for an entry of {entry} over a threshold of {threshold}"
    );
    let kept = stream(PortableLimits::default());
    println!("entry_bytes={entry} threshold={threshold} staged_bytes={staged} kept_bytes={kept}");
    assert!(
        kept < threshold,
        "an entry under the threshold wrote {kept} bytes"
    );
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

/// `entries` with every control that describes them rebuilt: the manifest's
/// file descriptors and package digest, the payload inventory and the tag
/// manifest. An edited package is then valid in everything but the edit.
fn resigned(entries: Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>)> {
    let mut entries: BTreeMap<String, Vec<u8>> = entries.into_iter().collect();
    let mut manifest: Value = serde_json::from_slice(&entries[MANIFEST_PATH]).unwrap();
    manifest.as_object_mut().unwrap().remove("package_digest");
    for component in manifest["components"].as_array_mut().unwrap() {
        for file in component["files"].as_array_mut().unwrap() {
            let bytes = &entries[file["path"].as_str().unwrap()];
            file["length"] = bytes.len().into();
            file["sha256"] = hex(&Sha256::digest(bytes)).into();
        }
    }
    let semantic = canonical_json(&manifest).unwrap();
    let digest = hex(&Sha256::digest(
        [b"graphforge-project/2\0".as_slice(), semantic.as_slice()].concat(),
    ));
    manifest["package_digest"] = format!("sha256:{digest}").into();
    entries.insert(MANIFEST_PATH.into(), canonical_json(&manifest).unwrap());
    let inventory: String = entries
        .iter()
        .filter(|(path, _)| path.starts_with("data/"))
        .map(|(path, bytes)| format!("{}  {path}\n", hex(&Sha256::digest(bytes))))
        .collect();
    let tags = format!(
        "{}  bag-info.txt\n{}  bagit.txt\n{}  manifest-sha256.txt\n",
        hex(&Sha256::digest(&entries["bag-info.txt"])),
        hex(&Sha256::digest(&entries["bagit.txt"])),
        hex(&Sha256::digest(inventory.as_bytes()))
    );
    entries.insert("manifest-sha256.txt".into(), inventory.into());
    entries.insert("tagmanifest-sha256.txt".into(), tags.into());
    entries.into_iter().collect()
}

/// The Hub bundle with `participant`'s payload moved into an `evidence`
/// component of the same id and replaced by `payload`, its runtime mapping kept
/// and now declaring a Parquet encoding. Every control is rebuilt to match.
fn participant_moved_to_evidence(participant: &str, payload: Vec<u8>) -> Vec<(String, Vec<u8>)> {
    let mut entries: BTreeMap<String, Vec<u8>> = read_tar(&fs::read(HUB_BUNDLE).unwrap())
        .into_iter()
        .collect();
    let from = format!("data/components/settings/{participant}/participant.json");
    let to = format!("data/components/evidence/{participant}/participant.parquet");
    entries.remove(&from).expect("a settings participant");
    entries.insert(to.clone(), payload);

    let mut manifest: Value = serde_json::from_slice(&entries[MANIFEST_PATH]).unwrap();
    manifest["requirements"]["capabilities"] = serde_json::json!([
        "compatibility@1",
        "evidence@1",
        "graph-data@1",
        "ontology-composition@1",
        "ontology@1",
        "settings@1"
    ]);
    let components = manifest["components"].as_array_mut().unwrap();
    let moved = components
        .iter_mut()
        .find(|component| component["participant_id"] == participant)
        .unwrap();
    moved["kind"] = "evidence".into();
    moved["files"][0]["path"] = to.into();
    moved["files"][0]["media_type"] = "application/vnd.apache.parquet".into();
    components.sort_by(|left, right| {
        let key = |component: &Value| {
            (
                component["kind"].as_str().unwrap().to_owned(),
                component["participant_id"].as_str().unwrap().to_owned(),
            )
        };
        key(left).cmp(&key(right))
    });
    entries.insert(MANIFEST_PATH.into(), canonical_json(&manifest).unwrap());

    let runtime_path =
        "data/components/compatibility/graphforge-runtime-map/runtime-generation.json";
    let mut runtime: Value = serde_json::from_slice(&entries[runtime_path]).unwrap();
    runtime["participants"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|entry| entry["participant_id"] == participant)
        .unwrap()["encoding"] = "parquet".into();
    entries.insert(runtime_path.into(), canonical_json(&runtime).unwrap());
    resigned(entries.into_iter().collect())
}

#[test]
fn a_runtime_participant_cannot_hide_in_an_evidence_component() {
    // A payload no decoder may see: Parquet pages in an unfunded encoding.
    let unfunded = hostile::string_file(
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
        WriterVersion::PARQUET_1_0,
        64,
    );
    let entries = participant_moved_to_evidence("workspace-configuration-0b4c69ad58c5", unfunded);
    let bytes = bundle_of(entries.clone());
    let directory = tempfile::tempdir().unwrap();
    for (path, payload) in &entries {
        let target = directory.path().join(path);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(target, payload).unwrap();
    }
    let verdicts = [
        verify_portable(
            directory.path(),
            PortableMode::Full,
            PortableLimits::default(),
            None,
        )
        .unwrap_err(),
        by_file(&bytes, PortableLimits::default()).unwrap_err(),
        by_stream(&bytes, PortableLimits::default()).unwrap_err(),
    ];
    for error in &verdicts {
        // InvalidStructure, not the page walk's LimitExceeded: the refusal comes
        // from the placement rule, before any page could be read.
        assert_eq!(error.code, PortableErrorCode::InvalidStructure, "{error}");
        assert!(
            error
                .to_string()
                .contains("runtime participant outside its component kind"),
            "{error}"
        );
    }
    assert_eq!(verdicts[0].to_string(), verdicts[1].to_string());
    assert_eq!(verdicts[1].to_string(), verdicts[2].to_string());
}

#[test]
fn the_unmoved_hub_package_still_verifies_after_resigning() {
    // The control: the same rebuild with no edit is valid, so the refusal above
    // is the placement rule and not an artifact of the rebuild.
    let entries = resigned(read_tar(&fs::read(HUB_BUNDLE).unwrap()));
    super::stream_tests::assert_stream_equals_file(&bundle_of(entries));
}
