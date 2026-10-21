//! `gf portable verify --input -`: a package read from standard input is
//! verified in bounded memory with no temporary disk, whatever its size.
//! The method is `docs/development/portable-verify-qualification.md`.
#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, mpsc};

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;
const EVIDENCE_PATH: &str = "data/components/evidence/scan-0001/scan.bin";
const GRAPH_PATH: &str = "data/components/graph-data/graph-tree/topology/nodes.parquet";
const ONTOLOGY_PATH: &str = "data/components/ontology/core-ontology/ontology.json";
const MANIFEST_PATH: &str = "data/graphforge-project.json";
const BAGIT: &[u8] = b"BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n";
const BAG_INFO: &[u8] = b"Bag-Software-Agent: GraphForge portable\nBagging-Date: 1970-01-01\n";
const HUB_BUNDLE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../tests/fixtures/hub/generated/v1/objects/openalex-openalex.gfpb"
);
/// The evidence artifact: 5 GiB of zero bytes.
const EVIDENCE_LENGTH: u64 = 5 * GIB;
/// SHA-256 of [`EVIDENCE_LENGTH`] zero bytes:
/// `head -c $((5 * 1024 ** 3)) /dev/zero | sha256sum`.
const EVIDENCE_DIGEST: &str = "7f06c62352aebd8125b2a1841e2b9e1ffcbed602f381c3dcb3200200e383d1d5";
/// The memory the verifier may hold, whatever the package size.
const PEAK_RSS_LIMIT: u64 = 768 * MIB;
/// The graph Parquet entry of the staging run: more than
/// `max_parquet_walk_bytes` (64 MiB), so the walk stages it in scratch.
const GRAPH_ROWS: usize = 64;
const GRAPH_ROW_BYTES: usize = 1_150_000;

/// The limits a verifier runs under. `ulimit -f` counts 512-byte blocks in the
/// shell `sh` is here; the effective values are read back from
/// `/proc/<pid>/limits` and asserted, not trusted.
#[derive(Clone, Copy)]
struct Limits {
    address_space: u64,
    file_size: u64,
}

/// Virtual memory 1 GiB, files 64 MiB.
const OPAQUE: Limits = Limits {
    address_space: GIB,
    file_size: 64 * MIB,
};
/// The same memory; files 128 MiB, room for one staged graph entry.
const STAGED: Limits = Limits {
    address_space: GIB,
    file_size: 128 * MIB,
};

impl Limits {
    fn script(self) -> String {
        format!(
            "ulimit -v {}; ulimit -f {}; exec \"$0\" portable verify --input - --json",
            self.address_space / 1024,
            self.file_size / 512
        )
    }
}

fn gf_bin() -> PathBuf {
    std::fs::canonicalize(env!("CARGO_BIN_EXE_gf")).expect("resolve same-build gf binary")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// RFC 8785-shaped canonical JSON for the strings, integers and containers a
/// manifest holds: sorted keys, no whitespace.
fn canonical(value: &Value) -> Vec<u8> {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map.iter().collect();
            let mut out = vec![b'{'];
            for (index, (key, value)) in sorted.into_iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                out.extend(serde_json::to_vec(key).unwrap());
                out.push(b':');
                out.extend(canonical(value));
            }
            out.push(b'}');
            out
        }
        Value::Array(values) => {
            let mut out = vec![b'['];
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                out.extend(canonical(value));
            }
            out.push(b']');
            out
        }
        scalar => serde_json::to_vec(scalar).unwrap(),
    }
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

fn octal(field: &mut [u8], value: u64) {
    field.fill(0);
    let digits = format!("{:0width$o}", value, width = field.len() - 1);
    field[..digits.len()].copy_from_slice(digits.as_bytes());
}

/// The canonical ustar header of a regular entry of `size` bytes at `path`
/// (every path here fits the 100-byte name field).
fn tar_header(path: &str, size: u64) -> Vec<u8> {
    assert!(path.len() <= 100);
    let mut header = [0u8; 512];
    header[..path.len()].copy_from_slice(path.as_bytes());
    octal(&mut header[100..108], 0o644);
    octal(&mut header[108..116], 0);
    octal(&mut header[116..124], 0);
    octal(&mut header[124..136], size);
    octal(&mut header[136..148], 0);
    header[148..156].fill(b' ');
    header[156] = b'0';
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    let sum: u64 = header.iter().map(|byte| u64::from(*byte)).sum();
    header[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
    header.to_vec()
}

/// A component added to the fixture package: one file of `kind`.
struct Artifact {
    kind: &'static str,
    participant: &'static str,
    path: &'static str,
    media_type: &'static str,
    body: Body,
    /// SHA-256 of the file, as the manifest declares it.
    sha256: String,
}

/// The 5 GiB opaque artifact, with its pinned digest.
fn evidence(sha256: &str, body: Body) -> Artifact {
    Artifact {
        kind: "evidence",
        participant: "scan-0001",
        path: EVIDENCE_PATH,
        media_type: "application/octet-stream",
        body,
        sha256: sha256.into(),
    }
}

/// The fixture ontology-only package plus `artifacts`. Every control is
/// rebuilt to describe them, so the package is valid. `manifest` replaces the
/// semantic manifest bytes when given (an impostor), with the inventories
/// describing it. Returns the entries in canonical order and `package_digest`.
fn package(artifacts: Vec<Artifact>, manifest: Option<Vec<u8>>) -> (Vec<(String, Body)>, String) {
    let mut document: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/portable/ontology-only.manifest.json"
    ))
    .unwrap();
    let object = document.as_object_mut().unwrap();
    object.remove("package_digest");
    object.insert("package_class".into(), "component-selective".into());
    let mut capabilities = vec!["ontology@1".to_owned()];
    let mut roots = vec!["core-ontology".to_owned()];
    let mut components = document["components"].as_array().unwrap().clone();
    for artifact in &artifacts {
        capabilities.push(format!("{}@1", artifact.kind));
        roots.push(artifact.participant.to_owned());
        components.push(serde_json::json!({
            "kind": artifact.kind,
            "files": [{
                "length": artifact.body.length(),
                "media_type": artifact.media_type,
                "path": artifact.path,
                "sha256": artifact.sha256,
            }],
            "participant_id": artifact.participant,
            "required_dependencies": [],
        }));
    }
    capabilities.sort();
    roots.sort();
    components.sort_by_key(|component| {
        (
            component["kind"].as_str().unwrap().to_owned(),
            component["participant_id"].as_str().unwrap().to_owned(),
        )
    });
    document["requirements"]["capabilities"] = capabilities.into();
    document["selection"]["roots"] = roots.into();
    document["components"] = components.into();
    let package_digest = hex(&Sha256::digest(
        [b"graphforge-project/2\0".as_slice(), &canonical(&document)].concat(),
    ));
    document["package_digest"] = format!("sha256:{package_digest}").into();
    let manifest = manifest.unwrap_or_else(|| canonical(&document));

    let mut data = BTreeMap::from([
        (ONTOLOGY_PATH.to_owned(), hex(&Sha256::digest(b"{}"))),
        (MANIFEST_PATH.to_owned(), hex(&Sha256::digest(&manifest))),
    ]);
    let mut entries = vec![
        ("bag-info.txt".to_owned(), Body::Bytes(BAG_INFO.to_vec())),
        ("bagit.txt".to_owned(), Body::Bytes(BAGIT.to_vec())),
        (ONTOLOGY_PATH.to_owned(), Body::Bytes(b"{}".to_vec())),
        (MANIFEST_PATH.to_owned(), Body::Bytes(manifest)),
    ];
    for artifact in artifacts {
        data.insert(artifact.path.to_owned(), artifact.sha256);
        entries.push((artifact.path.to_owned(), artifact.body));
    }
    let inventory: String = data
        .iter()
        .map(|(path, digest)| format!("{digest}  {path}\n"))
        .collect();
    let tags = format!(
        "{}  bag-info.txt\n{}  bagit.txt\n{}  manifest-sha256.txt\n",
        hex(&Sha256::digest(BAG_INFO)),
        hex(&Sha256::digest(BAGIT)),
        hex(&Sha256::digest(inventory.as_bytes()))
    );
    entries.push(("manifest-sha256.txt".into(), Body::Bytes(inventory.into())));
    entries.push(("tagmanifest-sha256.txt".into(), Body::Bytes(tags.into())));
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    (entries, package_digest)
}

/// A valid graph Parquet file of about 70 MiB: plain-encoded, uncompressed,
/// one page per row, so it passes the encoding profile and is walked.
fn graph_parquet() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let values = StringArray::from_iter_values(
        (0..GRAPH_ROWS).map(|row| format!("{row:02}{}", "x".repeat(GRAPH_ROW_BYTES))),
    );
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)]).unwrap();
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_write_batch_size(1)
        .set_data_page_row_count_limit(1)
        .set_max_row_group_row_count(Some(GRAPH_ROWS))
        .build();
    let mut file = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut file, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn graph_data(file: Vec<u8>) -> Artifact {
    let sha256 = hex(&Sha256::digest(&file));
    Artifact {
        kind: "graph-data",
        participant: "graph-tree",
        path: GRAPH_PATH,
        media_type: "application/vnd.apache.parquet",
        body: Body::Bytes(file),
        sha256,
    }
}

/// A canonical bundle read one entry at a time: a zero-filled entry is
/// generated, never held. `limit` cuts the stream short.
struct Bundle {
    entries: std::vec::IntoIter<(String, Body)>,
    current: Vec<u8>,
    at: usize,
    zeros: u64,
    padding: usize,
    ended: bool,
    limit: u64,
}

impl Bundle {
    fn new(entries: Vec<(String, Body)>, limit: u64) -> Self {
        Self {
            entries: entries.into_iter(),
            current: Vec::new(),
            at: 0,
            zeros: 0,
            padding: 0,
            ended: false,
            limit,
        }
    }

    /// The stream's length, whole.
    fn length(entries: &[(String, Body)]) -> u64 {
        entries
            .iter()
            .map(|(_, body)| 512 + body.length().div_ceil(512) * 512)
            .sum::<u64>()
            + 1024
    }
}

impl Read for Bundle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let room = buf
            .len()
            .min(usize::try_from(self.limit).unwrap_or(usize::MAX));
        let buf = &mut buf[..room];
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            let count = if self.at < self.current.len() {
                let count = buf.len().min(self.current.len() - self.at);
                buf[..count].copy_from_slice(&self.current[self.at..self.at + count]);
                self.at += count;
                count
            } else if self.zeros > 0 {
                let count = buf
                    .len()
                    .min(usize::try_from(self.zeros).unwrap_or(usize::MAX));
                buf[..count].fill(0);
                self.zeros -= count as u64;
                count
            } else if self.padding > 0 {
                let count = buf.len().min(self.padding);
                buf[..count].fill(0);
                self.padding -= count;
                count
            } else if let Some((path, body)) = self.entries.next() {
                self.current = tar_header(&path, body.length());
                self.at = 0;
                self.padding = (body.length().div_ceil(512) * 512 - body.length()) as usize;
                match body {
                    Body::Bytes(bytes) => self.current.extend(bytes),
                    Body::Zeros(length) => self.zeros = length,
                }
                continue;
            } else if !self.ended {
                self.ended = true;
                self.current = vec![0u8; 1024];
                self.at = 0;
                continue;
            } else {
                return Ok(0);
            };
            self.limit -= count as u64;
            return Ok(count);
        }
    }
}

/// What the sampler read from the live child: `VmHWM` and the limits it runs
/// under.
struct Sampled {
    /// Peak resident memory, a high-water mark: one read after the payload is
    /// fed is the run's peak.
    peak_rss: u64,
    /// `Max address space` and `Max file size`, soft.
    address_space: u64,
    file_size: u64,
}

fn proc_field(text: &str, key: &str) -> Option<u64> {
    let line = text.lines().find(|line| line.starts_with(key))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

fn limit_field(text: &str, name: &str) -> Option<u64> {
    let line = text.lines().find(|line| line.starts_with(name))?;
    // "Max file size", soft, hard, "bytes".
    line.split_whitespace().nth(3)?.parse().ok()
}

/// Read the live child's peak memory and limits. Every read must succeed.
fn sample(pid: u32) -> Result<Sampled, String> {
    let read = |name: &str| {
        std::fs::read_to_string(format!("/proc/{pid}/{name}"))
            .map_err(|error| format!("/proc/{pid}/{name}: {error}"))
    };
    let missing = |what: &str| format!("/proc/{pid}: no {what}");
    let status = read("status")?;
    let limits = read("limits")?;
    // The gf image, not the shell that set the limits before `exec`.
    if !read("cmdline")?.contains("portable") {
        return Err(missing("gf command line"));
    }
    Ok(Sampled {
        peak_rss: proc_field(&status, "VmHWM:").ok_or_else(|| missing("VmHWM"))? * 1024,
        address_space: limit_field(&limits, "Max address space")
            .ok_or_else(|| missing("address-space limit"))?,
        file_size: limit_field(&limits, "Max file size")
            .ok_or_else(|| missing("file-size limit"))?,
    })
}

struct Verified {
    code: Option<i32>,
    stdout: Vec<u8>,
    stderr: String,
    /// Sampled while the child was alive with the whole payload fed.
    sampled: Option<Sampled>,
    fed: u64,
}

impl Verified {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.stdout).unwrap_or_else(|error| {
            panic!("stdout is one JSON document ({error}): {}", self.stderr)
        })
    }

    /// The refusal: one JSON document on stderr, nothing on stdout.
    fn error(&self) -> Value {
        assert!(self.stdout.is_empty(), "a refusal prints no report");
        serde_json::from_str::<Value>(&self.stderr).unwrap_or_else(|error| {
            panic!("stderr is one JSON document ({error}): {}", self.stderr)
        })["error"]
            .clone()
    }

    /// The child ran under exactly `limits` and stayed under `PEAK_RSS_LIMIT`,
    /// as read from it while it was alive.
    fn assert_ran_within(&self, limits: Limits) {
        let sampled = self.sampled.as_ref().unwrap_or_else(|| {
            panic!(
                "the verifier ended before the stream was fed: {}",
                self.stderr
            )
        });
        assert_eq!(sampled.address_space, limits.address_space);
        assert_eq!(sampled.file_size, limits.file_size);
        println!("peak_rss_bytes={}", sampled.peak_rss);
        assert!(sampled.peak_rss > 0, "VmHWM was never read");
        assert!(
            sampled.peak_rss < PEAK_RSS_LIMIT,
            "peak resident memory was {} bytes",
            sampled.peak_rss
        );
    }
}

/// How a run reads the child.
#[derive(Clone, Copy, PartialEq)]
enum Hold {
    /// Run to the end; read nothing.
    No,
    /// Keep stdin open after the last byte until the child's memory and
    /// limits were read while it was alive, then close it. The verifier reads
    /// to end of input after the end blocks, so it is alive and blocked there.
    ForSample,
}

/// Run `gf portable verify --input - --json` on `stdin` under `limits`, with
/// `tmpdir` as its `TMPDIR`, feeding it from a thread.
fn verify_stream(
    mut stdin: impl Read + Send + 'static,
    tmpdir: &Path,
    limits: Limits,
    hold: Hold,
) -> Verified {
    let mut child = Command::new("sh")
        .args(["-c", &limits.script()])
        .arg(gf_bin())
        .env("TMPDIR", tmpdir)
        .current_dir(tmpdir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn gf under ulimit");
    let mut pipe = child.stdin.take().unwrap();
    let (fed_tx, fed_rx) = mpsc::channel::<()>();
    let (sampled_tx, sampled_rx) = mpsc::channel::<()>();
    let feeder = std::thread::spawn(move || {
        let mut fed = 0u64;
        let mut block = vec![0u8; MIB as usize];
        loop {
            let count = stdin.read(&mut block).expect("generate the stream");
            if count == 0 {
                break;
            }
            // The verifier may refuse before the end of the stream.
            if pipe.write_all(&block[..count]).is_err() {
                return fed;
            }
            fed += count as u64;
        }
        if hold == Hold::ForSample {
            // Released only once the main thread acknowledges its reads.
            fed_tx.send(()).ok();
            sampled_rx.recv().ok();
        }
        fed
    });
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let out = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let err = std::thread::spawn(move || {
        let mut text = String::new();
        stderr.read_to_string(&mut text).unwrap();
        text
    });
    let sampled = (hold == Hold::ForSample)
        .then(|| fed_rx.recv().ok())
        .flatten()
        .map(|()| {
            let sampled = sample(child.id()).unwrap_or_else(|error| panic!("{error}"));
            sampled_tx.send(()).expect("release the feeder");
            sampled
        });
    let status = child.wait().expect("wait for gf");
    Verified {
        code: status.code(),
        stdout: out.join().unwrap(),
        stderr: err.join().unwrap(),
        sampled,
        fed: feeder.join().unwrap(),
    }
}

fn assert_empty(dir: &Path) {
    let left: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert!(left.is_empty(), "TMPDIR holds {left:?}");
}

fn zeros_digest(length: u64) -> String {
    let mut hash = Sha256::new();
    let block = vec![0u8; MIB as usize];
    let mut left = length;
    while left > 0 {
        let count = left.min(MIB);
        hash.update(&block[..count as usize]);
        left -= count;
    }
    hex(&hash.finalize())
}

fn bytes_by_kind(report: &Value) -> BTreeMap<String, u64> {
    report["bytes_by_kind"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(kind, bytes)| (kind.clone(), bytes.as_u64().unwrap()))
        .collect()
}

/// A 5 GiB package verifies from stdin under a 1 GiB address space and a
/// 64 MiB file size: a verifier that buffered the input would fail to allocate,
/// and one that spooled it to a file would be killed at 64 MiB.
#[test]
fn a_five_gib_package_streams_from_stdin_in_bounded_memory() {
    let (entries, package_digest) = package(
        vec![evidence(EVIDENCE_DIGEST, Body::Zeros(EVIDENCE_LENGTH))],
        None,
    );
    let length = Bundle::length(&entries);
    assert!(length > 5 * GIB);
    let tmp = TempDir::new().unwrap();
    let run = verify_stream(
        Bundle::new(entries, u64::MAX),
        tmp.path(),
        OPAQUE,
        Hold::ForSample,
    );

    run.assert_ran_within(OPAQUE);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.fed, length, "the whole stream was read");
    let report = run.json();
    assert_eq!(report["integrity"], "verified");
    assert_eq!(report["source"], "stream");
    assert_eq!(report["package_digest"], format!("sha256:{package_digest}"));
    let kinds = bytes_by_kind(&report);
    assert_eq!(kinds["evidence"], EVIDENCE_LENGTH);
    assert_eq!(
        kinds.values().sum::<u64>(),
        report["payload_bytes"].as_u64().unwrap()
    );
    assert_empty(tmp.path());
}

/// A graph Parquet entry over `max_parquet_walk_bytes` verifies end to end in
/// the process, under a file size that holds one staged entry. How much it
/// writes is proven in-process, by the storage tests.
#[test]
fn a_graph_entry_over_the_walk_threshold_verifies_from_stdin() {
    let file = graph_parquet();
    let entry = file.len() as u64;
    assert!(
        entry > 64 * MIB,
        "{entry} bytes does not exceed the walk threshold"
    );
    let (entries, package_digest) = package(vec![graph_data(file)], None);
    let length = Bundle::length(&entries);
    let tmp = TempDir::new().unwrap();
    let run = verify_stream(
        Bundle::new(entries, u64::MAX),
        tmp.path(),
        STAGED,
        Hold::ForSample,
    );

    run.assert_ran_within(STAGED);
    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.fed, length);
    let report = run.json();
    assert_eq!(report["integrity"], "verified");
    assert_eq!(report["package_digest"], format!("sha256:{package_digest}"));
    assert_eq!(bytes_by_kind(&report)["graph-data"], entry);
    assert_eq!(report["plausibility"]["parquet"]["files_with_footer"], 1);
    assert_empty(tmp.path());
}

/// A stream cut short in the evidence artifact is refused as truncated.
#[test]
fn a_stream_truncated_at_sixty_percent_is_refused() {
    let (entries, _) = package(
        vec![evidence(EVIDENCE_DIGEST, Body::Zeros(EVIDENCE_LENGTH))],
        None,
    );
    let cut = Bundle::length(&entries) / 10 * 6;
    let tmp = TempDir::new().unwrap();
    let run = verify_stream(Bundle::new(entries, cut), tmp.path(), OPAQUE, Hold::No);

    assert_eq!(run.code, Some(2), "{}", run.stderr);
    let error = run.error();
    assert_eq!(error["code"], "GF_VALIDATION");
    assert!(
        error["diagnostics"][0]["message"]
            .as_str()
            .unwrap()
            .contains("truncated payload"),
        "{error}"
    );
    assert_empty(tmp.path());
}

/// A well-formed archive whose manifest is not one is refused, and so is one
/// whose artifact is not the bytes its manifest names.
#[test]
fn impostor_and_corrupted_bundles_are_refused() {
    let digest = zeros_digest(MIB);
    let impostor = package(
        vec![evidence(&digest, Body::Zeros(MIB))],
        Some(b"this is not a manifest".to_vec()),
    )
    .0;
    let corrupted = package(
        vec![evidence(
            &hex(&Sha256::digest(b"other bytes")),
            Body::Zeros(MIB),
        )],
        None,
    )
    .0;
    for entries in [impostor, corrupted] {
        let tmp = TempDir::new().unwrap();
        let run = verify_stream(Bundle::new(entries, u64::MAX), tmp.path(), OPAQUE, Hold::No);
        assert_eq!(run.code, Some(2), "{}", run.stderr);
        assert_eq!(run.error()["code"], "GF_VALIDATION");
        assert_empty(tmp.path());
    }
}

/// A resource refusal keeps the CLI-wide exit code; its stable code is in the
/// JSON error.
#[test]
fn a_limit_refusal_reports_its_stable_code_in_json() {
    let (mut entries, _) = package(vec![evidence(&zeros_digest(MIB), Body::Zeros(MIB))], None);
    // A manifest beyond `max_manifest_bytes` (16 MiB), refused at its header.
    entries
        .iter_mut()
        .find(|(path, _)| path == MANIFEST_PATH)
        .unwrap()
        .1 = Body::Zeros(32 * MIB);
    let tmp = TempDir::new().unwrap();
    let run = verify_stream(Bundle::new(entries, u64::MAX), tmp.path(), OPAQUE, Hold::No);
    assert_eq!(run.code, Some(1), "{}", run.stderr);
    assert_eq!(run.error()["code"], "GF_LIMIT_EXCEEDED");
    assert_empty(tmp.path());
}

struct Run {
    code: Option<i32>,
    stdout: Vec<u8>,
    stderr: String,
}

fn gf(args: &[&str], stdin: &[u8]) -> Run {
    let mut child = Command::new(gf_bin())
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn gf");
    let mut pipe = child.stdin.take().unwrap();
    let bytes = stdin.to_vec();
    let feeder = std::thread::spawn(move || {
        let _ = pipe.write_all(&bytes);
    });
    let output = child.wait_with_output().expect("run gf");
    feeder.join().unwrap();
    Run {
        code: output.status.code(),
        stdout: output.stdout,
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

fn one_document(text: &[u8]) -> Value {
    let mut documents = serde_json::Deserializer::from_slice(text).into_iter::<Value>();
    let document = documents.next().expect("a document").expect("valid JSON");
    assert!(documents.next().is_none(), "more than one document");
    document
}

/// The Hub fixture verifies alike from a file and from stdin, and `--json`
/// prints exactly one document either way.
#[test]
fn stdin_verifies_the_hub_bundle_with_one_json_document() {
    let bytes = std::fs::read(HUB_BUNDLE).unwrap();
    let stdin = gf(&["portable", "verify", "--input", "-", "--json"], &bytes);
    assert_eq!(stdin.code, Some(0), "{}", stdin.stderr);
    let mut stream = one_document(&stdin.stdout);
    let file = gf(
        &["portable", "verify", "--input", HUB_BUNDLE, "--json"],
        &[],
    );
    assert_eq!(file.code, Some(0), "{}", file.stderr);
    let mut file = one_document(&file.stdout);
    for report in [&mut file, &mut stream] {
        report.as_object_mut().unwrap().remove("source");
    }
    assert_eq!(file, stream);
}

#[test]
fn stdin_input_requires_full_mode() {
    let bytes = std::fs::read(HUB_BUNDLE).unwrap();
    let run = gf(
        &[
            "portable", "verify", "--input", "-", "--mode", "inspect", "--json",
        ],
        &bytes,
    );
    assert_eq!(run.code, Some(2));
    assert!(run.stdout.is_empty());
    assert_eq!(
        one_document(run.stderr.as_bytes())["error"]["code"],
        "GF_VALIDATION"
    );
}

/// A refused argument list is the one JSON error document the embedded
/// `execute` gives, with the argument-error exit code, on the real binary.
#[test]
fn an_argument_error_under_json_is_one_json_document() {
    for args in [
        &[
            "portable", "verify", "--input", "-", "--mode", "invalid", "--json",
        ][..],
        &["--json", "portable", "verify", "--no-such-flag"][..],
        &["--json", "no-such-command"][..],
    ] {
        let run = gf(args, &[]);
        assert_eq!(run.code, Some(2), "{args:?}: {}", run.stderr);
        assert!(run.stdout.is_empty(), "{args:?}");
        let error = &one_document(run.stderr.as_bytes())["error"];
        assert_eq!(error["code"], "GF_VALIDATION", "{args:?}");
        assert_eq!(error["details"]["source"], "argument_parser", "{args:?}");
    }
    // Without --json the refusal stays clap's text.
    let run = gf(&["portable", "verify", "--mode", "invalid"], &[]);
    assert_eq!(run.code, Some(2));
    assert!(run.stdout.is_empty());
    assert!(run.stderr.contains("invalid value"), "{}", run.stderr);
}

#[test]
fn help_and_version_print_normally() {
    for args in [
        &["--help"][..],
        &["--version"][..],
        &["portable", "verify", "--help"][..],
        &["--json", "--help"][..],
        &["--json", "--version"][..],
    ] {
        let run = gf(args, &[]);
        assert_eq!(run.code, Some(0), "{args:?}: {}", run.stderr);
        assert!(run.stderr.is_empty(), "{args:?}: {}", run.stderr);
        let text = String::from_utf8_lossy(&run.stdout);
        assert!(!text.trim().is_empty(), "{args:?}");
        assert!(
            serde_json::from_str::<Value>(&text).is_err(),
            "{args:?}: {text}"
        );
    }
}
