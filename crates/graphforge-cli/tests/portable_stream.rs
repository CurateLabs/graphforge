//! `gf portable verify --input -`: a package read from standard input is
//! verified in bounded memory with no temporary disk, whatever its size.
//! The method is `docs/development/portable-verify-qualification.md`.
#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const GIB: u64 = 1024 * 1024 * 1024;
const MIB: u64 = 1024 * 1024;
const EVIDENCE_PATH: &str = "data/components/evidence/scan-0001/scan.bin";
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
const VERIFY_SCRIPT: &str = "ulimit -v 1048576; ulimit -f 65536; \
                             exec \"$0\" portable verify --input - --json";

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

/// The fixture ontology-only package plus one `evidence` component whose only
/// file is [`EVIDENCE_PATH`], with SHA-256 `digest`. Every control is rebuilt
/// to describe it, so the package is valid. `manifest` replaces the semantic
/// manifest bytes when given (an impostor), with the inventories describing it.
fn package(
    evidence: Body,
    digest: &str,
    manifest: Option<Vec<u8>>,
) -> (Vec<(String, Body)>, String) {
    let length = evidence.length();
    let mut document: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/portable/ontology-only.manifest.json"
    ))
    .unwrap();
    let object = document.as_object_mut().unwrap();
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
                "sha256": digest,
            }],
            "participant_id": "scan-0001",
            "required_dependencies": [],
        }),
    );
    let package_digest = hex(&Sha256::digest(
        [b"graphforge-project/2\0".as_slice(), &canonical(&document)].concat(),
    ));
    document["package_digest"] = format!("sha256:{package_digest}").into();
    let manifest = manifest.unwrap_or_else(|| canonical(&document));

    let inventory: String = [
        (EVIDENCE_PATH, digest.to_owned()),
        (ONTOLOGY_PATH, hex(&Sha256::digest(b"{}"))),
        (MANIFEST_PATH, hex(&Sha256::digest(&manifest))),
    ]
    .iter()
    .map(|(path, digest)| format!("{digest}  {path}\n"))
    .collect();
    let tags = format!(
        "{}  bag-info.txt\n{}  bagit.txt\n{}  manifest-sha256.txt\n",
        hex(&Sha256::digest(BAG_INFO)),
        hex(&Sha256::digest(BAGIT)),
        hex(&Sha256::digest(inventory.as_bytes()))
    );
    let entries = vec![
        ("bag-info.txt".into(), Body::Bytes(BAG_INFO.to_vec())),
        ("bagit.txt".into(), Body::Bytes(BAGIT.to_vec())),
        (EVIDENCE_PATH.into(), evidence),
        (ONTOLOGY_PATH.into(), Body::Bytes(b"{}".to_vec())),
        (MANIFEST_PATH.into(), Body::Bytes(manifest)),
        ("manifest-sha256.txt".into(), Body::Bytes(inventory.into())),
        ("tagmanifest-sha256.txt".into(), Body::Bytes(tags.into())),
    ];
    (entries, package_digest)
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

struct Verified {
    code: Option<i32>,
    stdout: Vec<u8>,
    stderr: String,
    peak_rss: u64,
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
}

fn peak_rss(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    Some(line.split_whitespace().nth(1)?.parse::<u64>().ok()? * 1024)
}

/// Run `gf portable verify --input - --json` on `stdin` under the memory and
/// file-size limits, with `tmpdir` as its `TMPDIR`, feeding it from a thread
/// and sampling the child's peak resident memory every 50 ms.
fn verify_stream(mut stdin: impl Read + Send + 'static, tmpdir: &Path) -> Verified {
    let mut child = Command::new("sh")
        .args(["-c", VERIFY_SCRIPT])
        .arg(gf_bin())
        .env("TMPDIR", tmpdir)
        .current_dir(tmpdir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn gf under ulimit");
    let mut pipe = child.stdin.take().unwrap();
    let feeder = std::thread::spawn(move || {
        let mut fed = 0u64;
        let mut block = vec![0u8; MIB as usize];
        loop {
            let count = stdin.read(&mut block).expect("generate the stream");
            if count == 0 {
                return fed;
            }
            // The verifier may refuse before the end of the stream.
            if pipe.write_all(&block[..count]).is_err() {
                return fed;
            }
            fed += count as u64;
        }
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
    let mut peak = 0;
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll gf") {
            break status;
        }
        peak = peak.max(peak_rss(child.id()).unwrap_or(0));
        std::thread::sleep(Duration::from_millis(50));
    };
    Verified {
        code: status.code(),
        stdout: out.join().unwrap(),
        stderr: err.join().unwrap(),
        peak_rss: peak,
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

#[test]
fn a_five_gib_package_streams_from_stdin_in_bounded_memory_and_disk() {
    let (entries, package_digest) = package(Body::Zeros(EVIDENCE_LENGTH), EVIDENCE_DIGEST, None);
    let length = Bundle::length(&entries);
    assert!(length > 5 * GIB);
    let tmp = TempDir::new().unwrap();
    let run = verify_stream(Bundle::new(entries, u64::MAX), tmp.path());

    assert_eq!(run.code, Some(0), "{}", run.stderr);
    assert_eq!(run.fed, length, "the whole stream was read");
    let report = run.json();
    assert_eq!(report["integrity"], "verified");
    assert_eq!(report["source"], "stream");
    assert_eq!(report["package_digest"], format!("sha256:{package_digest}"));
    let kinds: BTreeMap<String, u64> = report["bytes_by_kind"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(kind, bytes)| (kind.clone(), bytes.as_u64().unwrap()))
        .collect();
    assert_eq!(kinds["evidence"], EVIDENCE_LENGTH);
    assert_eq!(
        kinds.values().sum::<u64>(),
        report["payload_bytes"].as_u64().unwrap()
    );
    assert_empty(tmp.path());
    println!("peak_rss_bytes={}", run.peak_rss);
    assert!(run.peak_rss > 0, "VmHWM was never sampled");
    assert!(
        run.peak_rss < PEAK_RSS_LIMIT,
        "peak resident memory was {} bytes",
        run.peak_rss
    );
}

/// A stream cut short in the evidence artifact is refused as truncated.
#[test]
fn a_stream_truncated_at_sixty_percent_is_refused() {
    let (entries, _) = package(Body::Zeros(EVIDENCE_LENGTH), EVIDENCE_DIGEST, None);
    let cut = Bundle::length(&entries) / 10 * 6;
    let tmp = TempDir::new().unwrap();
    let run = verify_stream(Bundle::new(entries, cut), tmp.path());

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
    let small = MIB;
    let digest = hex(&Sha256::digest(vec![0u8; small as usize]));
    let impostor = package(
        Body::Zeros(small),
        &digest,
        Some(b"this is not a manifest".to_vec()),
    )
    .0;
    let corrupted = package(
        Body::Zeros(small),
        &hex(&Sha256::digest(b"other bytes")),
        None,
    )
    .0;
    for entries in [impostor, corrupted] {
        let tmp = TempDir::new().unwrap();
        let run = verify_stream(Bundle::new(entries, u64::MAX), tmp.path());
        assert_eq!(run.code, Some(2), "{}", run.stderr);
        assert_eq!(run.error()["code"], "GF_VALIDATION");
        assert_empty(tmp.path());
    }
}

/// A resource refusal keeps the CLI-wide exit code; its stable code is in the
/// JSON error.
#[test]
fn a_limit_refusal_reports_its_stable_code_in_json() {
    let (mut entries, _) = package(
        Body::Zeros(MIB),
        &hex(&Sha256::digest(vec![0u8; MIB as usize])),
        None,
    );
    // A manifest beyond `max_manifest_bytes` (16 MiB), refused at its header.
    entries
        .iter_mut()
        .find(|(path, _)| path == MANIFEST_PATH)
        .unwrap()
        .1 = Body::Zeros(32 * MIB);
    let tmp = TempDir::new().unwrap();
    let run = verify_stream(Bundle::new(entries, u64::MAX), tmp.path());
    assert_eq!(run.code, Some(1), "{}", run.stderr);
    assert_eq!(run.error()["code"], "GF_LIMIT_EXCEEDED");
    assert_empty(tmp.path());
}

fn gf(args: &[&str], stdin: &[u8]) -> (Option<i32>, Vec<u8>, String) {
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
    (
        output.status.code(),
        output.stdout,
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// The Hub fixture verifies alike from a file and from stdin, and `--json`
/// prints exactly one document either way.
#[test]
fn stdin_verifies_the_hub_bundle_with_one_json_document() {
    let bytes = std::fs::read(HUB_BUNDLE).unwrap();
    let (code, stdout, stderr) = gf(&["portable", "verify", "--input", "-", "--json"], &bytes);
    assert_eq!(code, Some(0), "{stderr}");
    let documents = serde_json::Deserializer::from_slice(&stdout)
        .into_iter::<Value>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(documents.len(), 1, "{}", String::from_utf8_lossy(&stdout));
    let (code, stdout, stderr) = gf(
        &["portable", "verify", "--input", HUB_BUNDLE, "--json"],
        &[],
    );
    assert_eq!(code, Some(0), "{stderr}");
    let mut file: Value = serde_json::from_slice(&stdout).unwrap();
    let mut stream = documents.into_iter().next().unwrap();
    for report in [&mut file, &mut stream] {
        report.as_object_mut().unwrap().remove("source");
    }
    assert_eq!(file, stream);
}

#[test]
fn stdin_input_requires_full_mode() {
    let bytes = std::fs::read(HUB_BUNDLE).unwrap();
    let (code, stdout, stderr) = gf(
        &[
            "portable", "verify", "--input", "-", "--mode", "inspect", "--json",
        ],
        &bytes,
    );
    assert_eq!(code, Some(2));
    assert!(stdout.is_empty());
    let documents = serde_json::Deserializer::from_str(&stderr)
        .into_iter::<Value>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0]["error"]["code"], "GF_VALIDATION");
}
