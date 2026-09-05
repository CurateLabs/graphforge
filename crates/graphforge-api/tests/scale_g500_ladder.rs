//! Billion-live-edge public-facade scale contract and first-fail ladder (#736).
//!
//! This is the M5 (#735) root gate. It defines a **versioned, bounded-memory**
//! Graph500-parameter (ef=16 undirected Kronecker/R-MAT) scale ladder that:
//!
//! 1. Generates edges with memory bounded **independently of total edge count**
//!    via external sort + spill + k-way merge (the #710 SCALE-20 reference
//!    client retains every raw tuple in memory; this one does not).
//! 2. Reconciles `raw_attempts == live_unique_edges + self_loops_rejected +
//!    duplicates_rejected`, so raw generator attempts can never be mistaken for
//!    live persisted edges.
//! 3. Stops at and records the **first** envelope (RSS / disk / time) violation
//!    instead of making an unsupported SCALE-26 claim.
//!
//! It is **not** Official-track and **not** TEPS, and it does **not** itself
//! certify one billion live edges — that is #745. Small rungs run in normal CI;
//! provider ladder execution uses `benchmarks/` progressive qualification (#900).

#![recursion_limit = "256"]

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;

use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, CancellationToken,
    GraphConstructionBudgets, GraphConstructionSession, GraphForge, OperationId, PortableSelection,
    PortableV2ExportRequest, PortableV2ImportRequest, PortableV2Limits, PortableV2Mode,
    PortableV2Output, PortableV2SelectionProfile, PortableVerifyRequest, verify_portable_v2,
};
use graphforge_core::{GfError, uuid::Uuid};
use graphforge_exec::demand::{self, DemandSnapshot};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const PROFILE_SCHEMA: &str = "graphforge-billion-edge-ladder/1";
const EVIDENCE_SCHEMA: &str = "graphforge-billion-edge-ladder-evidence/1";
const SCHEMA_VERSION: &str = "1";
const GENERATOR_NAME: &str = "graphforge-kronecker-rmat-bounded";
const GENERATOR_SOURCE: &str = "crates/graphforge-api/tests/scale_g500_ladder.rs";

const NODE_LABEL: &str = "Node";
const REL_TYPE: &str = "LINK";
/// One authoritative Arrow and durable-append row window for the scale client.
/// This intentionally matches `GraphConstructionBudgets::default()`.
const CONSTRUCTION_BATCH_ROWS: usize = 65_536;

const ONE_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000";
const TWO_HOP: &str =
    "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id ORDER BY id LIMIT 1000";
const COUNT_EDGES: &str = "MATCH ()-[r:LINK]->() RETURN count(r) AS total";
static JOURNAL_WRITE_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static INGEST_SUBPHASE: AtomicU64 = AtomicU64::new(0);
static INGEST_CHUNK_INDEX: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// Versioned profile (single source of truth for the ladder).
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ScaleProfile {
    schema: String,
    schema_version: String,
    seed: u64,
    edgefactor: u32,
    initiator: Initiator,
    envelope: Envelope,
    metrics: Vec<String>,
    invocation: String,
    rungs: Vec<Rung>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct Initiator {
    #[serde(rename = "A")]
    a: f64,
    #[serde(rename = "B")]
    b: f64,
    #[serde(rename = "C")]
    c: f64,
    #[serde(rename = "D")]
    d: f64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct Envelope {
    rss_bytes: u64,
    disk_bytes: u64,
    timeout_s: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct Rung {
    id: String,
    scale: u32,
    buffer_edges: usize,
    tier: String,
    #[serde(default)]
    #[allow(dead_code)]
    note: Option<String>,
}

/// The profile is embedded at compile time so the runner is hermetic under both
/// Cargo and Bazel (no runtime `CARGO_MANIFEST_DIR` path dependency).
const PROFILE_JSON: &str = include_str!("fixtures/scale_g500_ladder.v1.json");
const CERTIFICATION_PROFILE_JSON: &str = include_str!("fixtures/scale_g500_certification.v1.json");

#[derive(Debug, Deserialize)]
struct CertificationProfile {
    schema: String,
    scale: u32,
    edgefactor: u32,
    #[serde(rename = "target_live_edges")]
    _target_live_edges: u64,
    seed: u64,
    initiator: Initiator,
    envelope: Envelope,
    preflight_scale: u32,
    provider_decision: String,
    runner_label: String,
}

fn load_profile() -> ScaleProfile {
    serde_json::from_str(PROFILE_JSON).expect("parse ladder profile fixture")
}

fn load_certification_profile() -> CertificationProfile {
    serde_json::from_str(CERTIFICATION_PROFILE_JSON).expect("parse certification profile fixture")
}

// ---------------------------------------------------------------------------
// Bounded generation: external sort + spill + k-way merge.
// Peak buffer size is `buffer_edges`, independent of total edge count.
// ---------------------------------------------------------------------------

/// Result of the bounded generation pass (before the merge that yields live
/// unique edges). Spill runs live under `work` and are cleaned with it.
struct SpillRuns {
    runs: Vec<PathBuf>,
    raw_attempts: u64,
    self_loops_rejected: u64,
    /// Largest number of edges ever resident in memory during generation.
    peak_buffer_len: usize,
}

/// Generate `2^scale * edge_factor` Kronecker attempts, dropping self-loops,
/// canonicalizing to undirected `(lo, hi)` pairs, and spilling **sorted (not
/// deduplicated)** fixed-size runs so cross-run and intra-run duplicates are
/// both accounted for at merge time.
fn generate_spill_runs(
    scale: u32,
    edge_factor: u32,
    initiator: Initiator,
    seed: u64,
    buffer_edges: usize,
    work: &Path,
    cancelled: Option<&AtomicBool>,
) -> SpillRuns {
    assert!((1..=31).contains(&scale), "SCALE must fit u32 vertex ids");
    assert!(buffer_edges >= 1, "buffer_edges must be positive");
    let raw_attempts = (1u64 << scale)
        .checked_mul(u64::from(edge_factor))
        .expect("raw attempt count fits u64");

    let mut rng = SplitMix64(seed);
    let mut buf: Vec<(u32, u32)> = Vec::with_capacity(buffer_edges);
    let mut runs = Vec::new();
    let mut self_loops_rejected = 0u64;
    let mut peak_buffer_len = 0usize;

    for attempt in 0..raw_attempts {
        if attempt % 65_536 == 0 {
            assert!(
                !cancelled.is_some_and(|flag| flag.load(Ordering::SeqCst)),
                "certification watchdog cancelled bounded generation"
            );
        }
        let (src, dst) = kronecker_edge(scale, initiator, &mut rng);
        if src == dst {
            self_loops_rejected += 1;
            continue;
        }
        let (lo, hi) = if src < dst { (src, dst) } else { (dst, src) };
        buf.push((
            u32::try_from(lo).expect("src fits u32"),
            u32::try_from(hi).expect("dst fits u32"),
        ));
        if buf.len() >= buffer_edges {
            peak_buffer_len = peak_buffer_len.max(buf.len());
            flush_run(&mut buf, work, runs.len(), &mut runs);
        }
    }
    peak_buffer_len = peak_buffer_len.max(buf.len());
    if !buf.is_empty() {
        flush_run(&mut buf, work, runs.len(), &mut runs);
    }

    SpillRuns {
        runs,
        raw_attempts,
        self_loops_rejected,
        peak_buffer_len,
    }
}

fn flush_run(buf: &mut Vec<(u32, u32)>, work: &Path, index: usize, runs: &mut Vec<PathBuf>) {
    buf.sort_unstable();
    let path = work.join(format!("run-{index:05}.bin"));
    let mut writer = BufWriter::new(File::create(&path).expect("create spill run"));
    for &(src, dst) in buf.iter() {
        writer.write_all(&src.to_le_bytes()).expect("write src");
        writer.write_all(&dst.to_le_bytes()).expect("write dst");
    }
    writer.flush().expect("flush spill run");
    buf.clear();
    runs.push(path);
}

struct RunReader {
    reader: BufReader<File>,
}

impl RunReader {
    fn open(path: &Path) -> Self {
        RunReader {
            reader: BufReader::new(File::open(path).expect("open spill run")),
        }
    }

    fn next_pair(&mut self) -> Option<(u32, u32)> {
        let mut bytes = [0u8; 8];
        match self.reader.read_exact(&mut bytes) {
            Ok(()) => {
                let src = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
                let dst = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
                Some((src, dst))
            }
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => None,
            Err(err) => panic!("read spill run: {err}"),
        }
    }
}

/// Counts produced by the merge that deduplicates the spilled runs.
struct MergeCounts {
    live_unique_edges: u64,
    duplicates_rejected: u64,
}

/// K-way merge of sorted runs. Emits each unique undirected pair exactly once
/// in canonical sorted order and counts every dropped duplicate. Memory is
/// `O(number_of_runs)`, not `O(total_edges)`.
fn merge_runs<F: FnMut(u32, u32)>(
    runs: &[PathBuf],
    cancellation: Option<&AtomicBool>,
    mut emit: F,
) -> Result<MergeCounts, &'static str> {
    let mut readers: Vec<RunReader> = runs.iter().map(|p| RunReader::open(p)).collect();
    let mut heap: BinaryHeap<Reverse<(u32, u32, usize)>> = BinaryHeap::new();
    for (idx, reader) in readers.iter_mut().enumerate() {
        if let Some((src, dst)) = reader.next_pair() {
            heap.push(Reverse((src, dst, idx)));
        }
    }

    let mut live_unique_edges = 0u64;
    let mut duplicates_rejected = 0u64;
    let mut last: Option<(u32, u32)> = None;
    while let Some(Reverse((src, dst, idx))) = heap.pop() {
        if cancellation.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
            return Err("merge_cancelled");
        }
        let pair = (src, dst);
        if Some(pair) == last {
            duplicates_rejected += 1;
        } else {
            emit(src, dst);
            live_unique_edges += 1;
            last = Some(pair);
        }
        if let Some((nsrc, ndst)) = readers[idx].next_pair() {
            heap.push(Reverse((nsrc, ndst, idx)));
        }
    }

    Ok(MergeCounts {
        live_unique_edges,
        duplicates_rejected,
    })
}

/// Full reconciled generation summary for a rung.
struct GenSummary {
    raw_attempts: u64,
    self_loops_rejected: u64,
    duplicates_rejected: u64,
    live_unique_edges: u64,
    peak_buffer_len: usize,
    run_count: usize,
    input_fingerprint: String,
}

impl GenSummary {
    fn reconciles(&self) -> bool {
        self.raw_attempts
            == self.live_unique_edges + self.self_loops_rejected + self.duplicates_rejected
    }
}

/// Generate + merge into memory (small rungs / unit tests only).
fn bounded_generation(
    scale: u32,
    edge_factor: u32,
    initiator: Initiator,
    seed: u64,
    buffer_edges: usize,
) -> (GenSummary, Vec<(u32, u32)>) {
    let work = TempDir::new().expect("spill workspace");
    let spill = generate_spill_runs(
        scale,
        edge_factor,
        initiator,
        seed,
        buffer_edges,
        work.path(),
        None,
    );
    let mut edges = Vec::new();
    let mut hasher = Sha256::new();
    let merge = merge_runs(&spill.runs, None, |src, dst| {
        hasher.update(src.to_le_bytes());
        hasher.update(dst.to_le_bytes());
        edges.push((src, dst));
    })
    .expect("bounded merge");
    let summary = GenSummary {
        raw_attempts: spill.raw_attempts,
        self_loops_rejected: spill.self_loops_rejected,
        duplicates_rejected: merge.duplicates_rejected,
        live_unique_edges: merge.live_unique_edges,
        peak_buffer_len: spill.peak_buffer_len,
        run_count: spill.runs.len(),
        input_fingerprint: format!("sha256:{}", hex_encode(hasher.finalize())),
    };
    (summary, edges)
}

/// Append deterministic, independently seeded attempt windows until a complete
/// external merge proves the requested live-edge floor. Every window remains
/// on disk and the final merge deduplicates across window boundaries, so the
/// stopping decision never relies on a probabilistic estimate.
#[derive(Clone, Copy)]
struct TargetLiveGeneration {
    scale: u32,
    edge_factor: u32,
    initiator: Initiator,
    seed: u64,
    buffer_edges: usize,
    target_live_edges: u64,
}

fn generate_target_live_runs(
    request: &TargetLiveGeneration,
    work: &Path,
    cancelled: &AtomicBool,
) -> (SpillRuns, MergeCounts, String) {
    let TargetLiveGeneration {
        scale,
        edge_factor,
        initiator,
        seed,
        buffer_edges,
        target_live_edges,
    } = *request;
    let mut combined = SpillRuns {
        runs: Vec::new(),
        raw_attempts: 0,
        self_loops_rejected: 0,
        peak_buffer_len: 0,
    };
    for window in 0u64.. {
        assert!(
            !cancelled.load(Ordering::SeqCst),
            "certification watchdog cancelled target-live generation"
        );
        let window_dir = work.join(format!("window-{window:04}"));
        fs::create_dir_all(&window_dir).expect("target-live window directory");
        let window_seed = seed.wrapping_add(window.wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let generated = generate_spill_runs(
            scale,
            edge_factor,
            initiator,
            window_seed,
            buffer_edges,
            &window_dir,
            Some(cancelled),
        );
        combined.raw_attempts = combined
            .raw_attempts
            .checked_add(generated.raw_attempts)
            .expect("generated raw-attempt count overflow");
        combined.self_loops_rejected = combined
            .self_loops_rejected
            .checked_add(generated.self_loops_rejected)
            .expect("generated self-loop count overflow");
        combined.peak_buffer_len = combined.peak_buffer_len.max(generated.peak_buffer_len);
        combined.runs.extend(generated.runs);
        let compact_path = work.join(format!("accumulated-{window:04}.bin"));
        let mut writer = BufWriter::new(File::create(&compact_path).expect("create compact run"));
        let mut digest = Sha256::new();
        let counts = merge_runs(&combined.runs, Some(cancelled), |src, dst| {
            let src = src.to_le_bytes();
            let dst = dst.to_le_bytes();
            writer.write_all(&src).expect("write compact src");
            writer.write_all(&dst).expect("write compact dst");
            digest.update(src);
            digest.update(dst);
        })
        .expect("target-live merge was cancelled");
        writer.flush().expect("flush compact run");
        drop(writer);
        for obsolete in &combined.runs {
            fs::remove_file(obsolete).expect("remove superseded spill run");
        }
        combined.runs = vec![compact_path];
        if counts.live_unique_edges >= target_live_edges {
            let duplicates_rejected = combined
                .raw_attempts
                .checked_sub(combined.self_loops_rejected)
                .and_then(|value| value.checked_sub(counts.live_unique_edges))
                .expect("target-live generator counts reconcile");
            return (
                combined,
                MergeCounts {
                    live_unique_edges: counts.live_unique_edges,
                    duplicates_rejected,
                },
                format!("sha256:{}", hex_encode(digest.finalize())),
            );
        }
    }
    unreachable!("unbounded deterministic window iterator")
}

// ---------------------------------------------------------------------------
// Kronecker generation (bounded, no whole-graph retention).
// ---------------------------------------------------------------------------

fn kronecker_edge(scale: u32, init: Initiator, rng: &mut SplitMix64) -> (u64, u64) {
    let mut src = 0u64;
    let mut dst = 0u64;
    for bit in 0..scale {
        let sample = rng.next_unit();
        let (di, dj) = if sample < init.a {
            (0, 0)
        } else if sample < init.a + init.b {
            (0, 1)
        } else if sample < init.a + init.b + init.c {
            (1, 0)
        } else {
            (1, 1)
        };
        src |= di << bit;
        dst |= dj << bit;
    }
    (src, dst)
}

// ---------------------------------------------------------------------------
// First-fail ladder harness over the public facade.
// ---------------------------------------------------------------------------

/// A stop condition the ladder measures after every phase.
#[derive(Clone, Copy)]
struct RunEnvelope {
    rss_bytes: u64,
    disk_bytes: u64,
    timeout_s: u64,
}

impl From<Envelope> for RunEnvelope {
    fn from(e: Envelope) -> Self {
        RunEnvelope {
            rss_bytes: e.rss_bytes,
            disk_bytes: e.disk_bytes,
            timeout_s: e.timeout_s,
        }
    }
}

/// The evidence + pass/fail for a single rung attempt.
struct RungOutcome {
    passed: bool,
    evidence: Value,
}

fn exact_descriptor_allocation(paths: &[PathBuf]) -> Value {
    let mut logical_bytes = 0_u64;
    let mut allocated = 0_u64;
    for path in paths {
        logical_bytes = logical_bytes
            .checked_add(
                fs::metadata(path)
                    .expect("generator descriptor metadata")
                    .len(),
            )
            .expect("descriptor logical-byte count overflow");
        let file = File::open(path).expect("open exact allocation descriptor");
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            allocated = allocated
                .checked_add(
                    file.metadata()
                        .expect("exact descriptor metadata")
                        .blocks()
                        .checked_mul(512)
                        .expect("descriptor block allocation overflow"),
                )
                .expect("descriptor allocated-byte count overflow");
        }
        #[cfg(not(unix))]
        panic!("certification descriptor allocation requires Unix stat blocks");
    }
    json!({
        "category": "generator_spill",
        "logical_bytes": logical_bytes,
        "allocated_bytes": allocated,
        "logical_references": paths.len(),
        "physical_objects": paths.len(),
        "source": "generator_exact_descriptors",
    })
}

fn exact_descriptor_identities(paths: &[PathBuf]) -> BTreeMap<String, u64> {
    paths
        .iter()
        .map(|path| {
            let file = File::open(path).expect("open exact allocation descriptor");
            let identity = graphforge_filesystem::file_identity(&file)
                .expect("exact descriptor native identity");
            let allocation = graphforge_filesystem::file_space_usage(&file)
                .expect("exact descriptor allocation")
                .allocated_bytes;
            let mut file_id = String::with_capacity(32);
            for byte in identity.file_id {
                use std::fmt::Write as _;
                write!(&mut file_id, "{byte:02x}").expect("write identity string");
            }
            (
                format!("{:016x}:{file_id}", identity.volume_serial),
                allocation,
            )
        })
        .collect()
}

const CATEGORY_AUTHORITY_CONTRACT: &str = "graphforge-lifecycle-category-authority/2";

fn portable_export_allocation(
    receipt: &graphforge_api::PortableV2ExportFacadeResult,
    rung: u64,
    generation_sha256: &str,
    live_nodes: u64,
    live_edges: u64,
) -> Value {
    let allocated_bytes = receipt
        .allocation_identity_allocated_bytes
        .values()
        .try_fold(0_u64, |total, allocated| total.checked_add(*allocated))
        .expect("portable allocation authority overflow");
    let authority = graphforge_storage::ArtifactStorageTotals {
        logical_references: receipt.allocation_physical_objects,
        logical_bytes: receipt.allocation_logical_bytes,
        physical_objects: receipt.allocation_physical_objects,
        physical_logical_bytes: receipt.allocation_logical_bytes,
        allocated_bytes,
    };
    let native_identity_authority_sha256 =
        safe_identity_authority_digest(&receipt.allocation_identity_allocated_bytes);
    let empty_identity_authority_sha256 =
        safe_identity_authority_digest(&BTreeMap::<String, u64>::new());
    let mut native_category_identity_authority_sha256 = graphforge_storage::ArtifactCategory::ALL
        .into_iter()
        .map(|category| (category, empty_identity_authority_sha256.clone()))
        .collect::<BTreeMap<_, _>>();
    native_category_identity_authority_sha256.insert(
        graphforge_storage::ArtifactCategory::PortablePackage,
        native_identity_authority_sha256.clone(),
    );
    let context = graphforge_storage::ArtifactCategoryAuthorityContext {
        contract: CATEGORY_AUTHORITY_CONTRACT.to_owned(),
        version: 1,
        rung,
        generation_sha256: generation_sha256.to_owned(),
        owner: "portable_package".to_owned(),
        receipt_authority_sha256: safe_authority_digest(
            b"graphforge-portable-receipt-authority-v1\0",
            &[
                receipt.package_digest.as_bytes(),
                receipt.transport_digest.as_bytes(),
                &receipt.allocation_logical_bytes.to_be_bytes(),
                &receipt.allocation_physical_objects.to_be_bytes(),
            ],
        ),
        native_identity_authority_sha256,
        native_category_identity_authority_sha256,
        live_nodes,
        live_edges,
    };
    let authority_sha256 = graphforge_storage::artifact_category_authority_commitment(
        &context,
        graphforge_storage::ArtifactCategory::PortablePackage,
        &authority,
    );
    json!({
        "category": "portable_package",
        "logical_bytes": receipt.allocation_logical_bytes,
        "allocated_bytes": allocated_bytes,
        "logical_references": receipt.allocation_physical_objects,
        "physical_objects": receipt.allocation_physical_objects,
        "source": "portable_writer_receipt",
        "category_authority": authority,
        "category_authority_context": context,
        "category_authority_sha256": authority_sha256,
    })
}

fn storage_attribution_value(
    project: &Path,
    owner: &str,
    rung: u64,
    live_nodes: u64,
    live_edges: u64,
) -> (Value, graphforge_storage::ArtifactCategoryAuthorityContext) {
    let snapshot = storage_attribution(project);
    let category_authorities = snapshot
        .category_authorities()
        .expect("storage-owned category authorities");
    let context = snapshot
        .category_authority_context(
            CATEGORY_AUTHORITY_CONTRACT,
            rung,
            owner,
            live_nodes,
            live_edges,
        )
        .expect("storage-owned category authority context");
    let category_authority_sha256 = snapshot
        .category_authority_commitments(&context)
        .expect("storage-owned category commitments");
    let mut value = serde_json::to_value(snapshot).expect("serialize storage attribution");
    value
        .as_object_mut()
        .expect("storage attribution object")
        .remove("generation_uuid");
    value
        .as_object_mut()
        .expect("storage attribution object")
        .remove("physical_identity_allocated_bytes");
    value
        .as_object_mut()
        .expect("storage attribution object")
        .insert(
            "category_authorities".into(),
            serde_json::to_value(category_authorities)
                .expect("serialize storage category authorities"),
        );
    value
        .as_object_mut()
        .expect("storage attribution object")
        .insert(
            "category_authority_sha256".into(),
            serde_json::to_value(category_authority_sha256)
                .expect("serialize storage category commitments"),
        );
    value
        .as_object_mut()
        .expect("storage attribution object")
        .insert(
            "category_authority_context".into(),
            serde_json::to_value(&context).expect("serialize storage category authority context"),
        );
    (value, context)
}

fn safe_authority_digest(domain: &[u8], values: &[&[u8]]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    for value in values {
        digest.update((value.len() as u128).to_be_bytes());
        digest.update(value);
    }
    format!("sha256:{}", hex_encode(digest.finalize()))
}

fn safe_identity_authority_digest(identities: &BTreeMap<String, u64>) -> String {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-native-identity-authority-v1\0");
    for (identity, allocated_bytes) in identities {
        digest.update((identity.len() as u128).to_be_bytes());
        digest.update(identity.as_bytes());
        digest.update(allocated_bytes.to_be_bytes());
    }
    format!("sha256:{}", hex_encode(digest.finalize()))
}

fn contains_native_object_identity(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes.windows(49).enumerate().any(|(offset, candidate)| {
        candidate[16] == b':'
            && candidate[..16].iter().all(u8::is_ascii_hexdigit)
            && candidate[17..].iter().all(u8::is_ascii_hexdigit)
            && offset
                .checked_sub(1)
                .is_none_or(|before| !bytes[before].is_ascii_hexdigit())
            && bytes
                .get(offset + candidate.len())
                .is_none_or(|after| !after.is_ascii_hexdigit())
    })
}

fn reject_unsanitized_evidence(value: &Value) -> Result<(), String> {
    fn visit(value: &Value, trail: &str) -> Result<(), String> {
        match value {
            Value::Object(fields) => {
                for (key, child) in fields {
                    let normalized = key.to_ascii_lowercase();
                    if [
                        "secret",
                        "credential",
                        "password",
                        "token",
                        "machine_id",
                        "volume_id",
                        "provider_resource_id",
                        "absolute_path",
                        "host_path",
                    ]
                    .iter()
                    .any(|needle| normalized.contains(needle))
                    {
                        return Err(format!("sensitive evidence key at {trail}.{key}"));
                    }
                    visit(child, &format!("{trail}.{key}"))?;
                }
            }
            Value::Array(items) => {
                for (index, child) in items.iter().enumerate() {
                    visit(child, &format!("{trail}[{index}]"))?;
                }
            }
            Value::String(text) => {
                if Uuid::parse_str(text).is_ok() {
                    return Err(format!("raw UUID at {trail}"));
                }
                if contains_native_object_identity(text) {
                    return Err(format!("raw native object identity at {trail}"));
                }
                if text.starts_with('/')
                    || text.starts_with("\\\\")
                    || (text.len() >= 3
                        && text.as_bytes()[1] == b':'
                        && matches!(text.as_bytes()[2], b'/' | b'\\'))
                    || text.split_whitespace().any(|part| part.starts_with('/'))
                {
                    return Err(format!("absolute host path at {trail}"));
                }
            }
            _ => {}
        }
        Ok(())
    }
    visit(value, "$")
}

fn sanitized_construction_evidence(
    evidence: &graphforge_storage::GraphConstructionEvidence,
    rung: u64,
    generation_sha256: &str,
    live_nodes: u64,
    live_edges: u64,
) -> Value {
    let category_authorities = evidence
        .storage_category_authorities()
        .expect("construction receipt/identity category authorities");
    let peak_authorities = evidence
        .storage_transient_peak_authorities()
        .expect("construction receipt peak authorities");
    let context = evidence
        .storage_category_authority_context(
            CATEGORY_AUTHORITY_CONTRACT,
            rung,
            generation_sha256,
            "construction",
            live_nodes,
            live_edges,
        )
        .expect("construction category authority context");
    let (category_authority_sha256, peak_authority_sha256) = evidence
        .storage_category_authority_commitments(&context)
        .expect("construction category authority commitments");
    let mut value = serde_json::to_value(evidence).expect("serialize construction evidence");
    value
        .as_object_mut()
        .expect("construction evidence object")
        .remove("storage_active_identity_allocated_bytes");
    value
        .as_object_mut()
        .expect("construction evidence object")
        .remove("storage_allocation_transitions");
    value
        .as_object_mut()
        .expect("construction evidence object")
        .remove("storage_receipt_category_authorities");
    value
        .as_object_mut()
        .expect("construction evidence object")
        .remove("storage_receipt_transient_peak_authorities");
    value
        .as_object_mut()
        .expect("construction evidence object")
        .insert(
            "storage_category_authorities".into(),
            serde_json::to_value(category_authorities)
                .expect("serialize construction category authorities"),
        );
    value
        .as_object_mut()
        .expect("construction evidence object")
        .insert(
            "storage_category_authority_context".into(),
            serde_json::to_value(context).expect("serialize construction authority context"),
        );
    value
        .as_object_mut()
        .expect("construction evidence object")
        .insert(
            "storage_category_authority_sha256".into(),
            serde_json::to_value(category_authority_sha256)
                .expect("serialize construction category commitments"),
        );
    value
        .as_object_mut()
        .expect("construction evidence object")
        .insert(
            "storage_transient_peak_authority_sha256".into(),
            serde_json::to_value(peak_authority_sha256)
                .expect("serialize construction peak commitments"),
        );
    value
        .as_object_mut()
        .expect("construction evidence object")
        .insert(
            "storage_transient_peak_authorities".into(),
            serde_json::to_value(peak_authorities)
                .expect("serialize construction peak authorities"),
        );
    value
}

fn storage_attribution(project: &Path) -> graphforge_storage::StorageAttributionSnapshot {
    let graph = GraphForge::new(project.to_str()).expect("open attribution facade");
    let snapshot = graph
        .storage_attribution()
        .expect("capture authenticated storage attribution through public facade");
    snapshot
        .validate_reconciliation()
        .expect("storage attribution reconciliation");
    assert!(
        snapshot.is_fully_classified(),
        "qualification refuses unclassified retained artifacts"
    );
    snapshot
}

#[test]
fn public_storage_attribution_is_generation_bound_and_fully_classified() {
    let project = TempDir::new().expect("storage attribution project");
    let graph = GraphForge::new(project.path().to_str()).expect("open attribution facade");
    let snapshot = graph
        .storage_attribution()
        .expect("public facade storage attribution");
    snapshot.validate_for_qualification().unwrap();
    assert!(snapshot.is_fully_classified());
}

/// Check the envelope after a phase. Returns `Some(error_class)` on the first
/// violation so the caller can stop the ladder. `ladder_started` is the
/// ladder-level clock so the 4 h wall-clock fail-safe bounds the whole run, not each
/// rung independently.
fn envelope_violation(
    env: &RunEnvelope,
    ladder_started: Instant,
    disk_used_bytes: u64,
) -> Option<&'static str> {
    if peak_rss().is_some_and(|(rss, _)| rss > env.rss_bytes) {
        return Some("oom");
    }
    if disk_used_bytes > env.disk_bytes {
        return Some("disk_exhaustion");
    }
    if ladder_started.elapsed().as_secs() > env.timeout_s {
        return Some("timeout");
    }
    None
}

/// Current peak RSS as a JSON value (bytes or null).
fn rss_value() -> Value {
    peak_rss().map_or(Value::Null, |(bytes, _)| json!(bytes))
}

fn execute_with_bounded_evidence(
    graph: &GraphForge,
    query: &str,
) -> (
    Result<graphforge_api::ExecutionResult, GfError>,
    DemandSnapshot,
) {
    demand::capture(|| graph.execute(query))
}

fn query_work_evidence(snapshot: &DemandSnapshot) -> Value {
    json!({
        "hops": snapshot.hops.iter().map(|(edge_var, hop)| json!({
            "edge_var": edge_var,
            "input_batches": hop.input_batches,
            "input_rows": hop.input_rows,
            "candidates_generated": hop.candidates_generated,
            "adjacency_rows_examined": hop.adjacency_rows_examined,
            "rows_emitted": hop.rows_emitted,
            "edge_reads_started": hop.edge_reads_started,
            "edge_reads_completed": hop.edge_reads_completed,
            "edge_full_reads": hop.edge_full_reads,
            "edge_rows_scanned": hop.edge_rows_scanned,
            "node_reads_started": hop.node_reads_started,
            "node_reads_completed": hop.node_reads_completed,
            "node_full_reads": hop.node_full_reads,
            "node_rows_scanned": hop.node_rows_scanned,
            "projected_chunks": hop.projected_chunks,
            "projected_rows": hop.projected_rows,
            "projected_columns": hop.projected_columns,
            "identity_ranges_selected": hop.identity_ranges_selected,
            "identity_read_calls": hop.identity_read_calls,
            "identity_bytes_read": hop.identity_bytes_read,
            "identity_peak_buffer_bytes": hop.identity_peak_buffer_bytes,
            "identity_per_record_seeks": hop.identity_per_record_seeks,
            "reads_after_cancel": hop.reads_after_cancel,
        })).collect::<Vec<_>>(),
        "sorts": snapshot.sorts.iter().map(|sort| json!({
            "ordinal": sort.ordinal,
            "fetch": sort.fetch,
            "output_rows": sort.output_rows,
            "spill_count": sort.spill_count,
            "spilled_rows": sort.spilled_rows,
            "spilled_bytes": sort.spilled_bytes,
            "retained_bytes": sort.retained_bytes,
        })).collect::<Vec<_>>(),
        "operator_rss": snapshot.operator_rss.iter().map(|operator| json!({
            "ordinal": operator.ordinal,
            "operator": operator.operator,
            "before_bytes": operator.before_bytes,
            "peak_bytes": operator.peak_bytes,
            "after_bytes": operator.after_bytes,
        })).collect::<Vec<_>>(),
        "memory_reserved_before": snapshot.memory_reserved_before,
        "memory_reserved_after": snapshot.memory_reserved_after,
        "returned_batch_bytes": snapshot.returned_batch_bytes,
        "execution_batch_rows": snapshot.execution_batch_rows,
        "cancellations": snapshot.cancellations,
        "max_in_flight_reads": snapshot.max_in_flight_reads,
    })
}

const MAX_IDENTITY_BUFFER_BYTES: u64 = 16 * 1024 * 1024;

fn released_memory_is_bounded(snapshot: &DemandSnapshot) -> bool {
    snapshot
        .memory_reserved_before
        .checked_add(snapshot.returned_batch_bytes)
        .is_some_and(|bound| snapshot.memory_reserved_after <= bound)
}

fn has_no_materializing_reads(hop: &demand::HopSnapshot) -> bool {
    hop.edge_reads_started == 0
        && hop.edge_reads_completed == 0
        && hop.edge_reads_failed == 0
        && hop.edge_rows_returned == 0
        && hop.edge_rows_scanned == 0
        && hop.edge_full_reads == 0
        && hop.node_reads_started == 0
        && hop.node_reads_completed == 0
        && hop.node_reads_failed == 0
        && hop.node_rows_returned == 0
        && hop.node_rows_scanned == 0
        && hop.node_full_reads == 0
        && hop.edge_projected_columns == 0
        && hop.node_projected_columns == 0
}

fn bounded_ordered_leaf(
    snapshot: &DemandSnapshot,
    expected_hops: usize,
    limit: usize,
    live_nodes: u64,
    expected_rows: u64,
) -> bool {
    let Some(hop) = snapshot.hops.values().next() else {
        return false;
    };
    let [operator] = snapshot.operator_rss.as_slice() else {
        return false;
    };
    let expected_operator = match expected_hops {
        1 => "ordered_one_hop",
        2 => "ordered_two_hop",
        _ => return false,
    };
    snapshot.hops.len() == 1
        && snapshot.filters.is_empty()
        && snapshot.sorts.is_empty()
        && operator.ordinal == 0
        && operator.operator == expected_operator
        && operator.peak_bytes >= operator.before_bytes
        && operator.peak_bytes >= operator.after_bytes
        && (operator.after_bytes > 0 || !cfg!(target_os = "linux"))
        && snapshot.execution_batch_rows > 0
        && hop.input_batches == 0
        && hop.input_rows == 0
        && hop.rows_emitted == expected_rows
        && hop.candidates_generated >= hop.rows_emitted
        && (expected_hops != 1 || hop.candidates_generated == hop.rows_emitted)
        // Each optimized leaf probes destinations once in ordinal order.
        // Empty rows count as probes, not emitted candidates, and may exceed K.
        && hop.adjacency_rows_examined > 0
        && hop.adjacency_rows_examined <= live_nodes
        && (limit as u64)
            .checked_add(snapshot.execution_batch_rows)
            .is_some_and(|bound| hop.candidates_generated <= bound)
        && has_no_materializing_reads(hop)
        && hop.projected_chunks > 0
        && hop.projected_chunks <= hop.projected_rows
        && (expected_hops != 2 || hop.projected_chunks == hop.projected_rows)
        && hop.projected_rows <= hop.adjacency_rows_examined
        && hop.projected_rows <= limit as u64
        && hop.projected_columns == 1
        && hop.identity_ranges_selected > 0
        && hop.identity_ranges_selected <= hop.projected_rows
        && hop.identity_read_calls > 0
        && hop.identity_bytes_read > 0
        && hop.identity_peak_buffer_bytes > 0
        && hop
            .identity_ranges_selected
            .checked_mul(2)
            .and_then(|bound| bound.checked_add(2))
            .is_some_and(|bound| hop.identity_read_calls <= bound)
        && hop
            .identity_read_calls
            .checked_mul(MAX_IDENTITY_BUFFER_BYTES)
            .is_some_and(|bound| hop.identity_bytes_read <= bound)
        && hop.identity_peak_buffer_bytes <= MAX_IDENTITY_BUFFER_BYTES
        && hop.identity_per_record_seeks == 0
        && hop.reads_after_cancel == 0
        && snapshot.cancellations == 0
        && snapshot.max_in_flight_reads == 0
        && released_memory_is_bounded(snapshot)
}

fn bounded_no_sort_one_hop(snapshot: &DemandSnapshot, limit: usize, expected_rows: u64) -> bool {
    let Some(hop) = snapshot.hops.values().next() else {
        return false;
    };
    let canonical_batch_rows =
        u64::try_from(graphforge_exec::SessionResourceConfig::default().batch_size).ok();
    let row_bound = canonical_batch_rows.and_then(|batch_rows| {
        (snapshot.execution_batch_rows == batch_rows)
            .then_some(batch_rows)
            .and_then(|batch_rows| u64::try_from(limit).ok()?.checked_add(batch_rows))
    });
    let Some(row_bound) = row_bound else {
        return false;
    };
    let [operator] = snapshot.operator_rss.as_slice() else {
        return false;
    };
    snapshot.hops.len() == 1
        && snapshot.filters.is_empty()
        && snapshot.sorts.is_empty()
        && operator.ordinal == 0
        && operator.operator == "expand"
        && operator.peak_bytes >= operator.before_bytes
        && operator.peak_bytes >= operator.after_bytes
        && (operator.after_bytes > 0 || !cfg!(target_os = "linux"))
        && hop.input_batches > 0
        && hop.input_batches <= hop.input_rows
        && hop.input_rows <= row_bound
        && hop.rows_emitted >= expected_rows
        && hop.rows_emitted <= row_bound
        && hop.candidates_generated >= hop.rows_emitted
        && hop.candidates_generated <= row_bound
        && hop.reads_after_cancel == 0
        && snapshot.cancellations == 0
        && snapshot.max_in_flight_reads == 0
        && released_memory_is_bounded(snapshot)
}

fn bounded_streaming_ordered_limit(
    snapshot: &DemandSnapshot,
    expected_hops: usize,
    limit: usize,
) -> bool {
    snapshot.hops.len() == expected_hops
        && snapshot.sorts.len() == 1
        && snapshot.sorts[0].fetch == Some(limit)
        && snapshot.execution_batch_rows > 0
        // DataFusion's baseline output counter is charged before its final
        // fetch wrapper slices the terminal batch, so it may include at most
        // one physical batch beyond the returned TopK rows.
        && (limit as u64)
            .checked_add(snapshot.execution_batch_rows)
            .is_some_and(|bound| snapshot.sorts[0].output_rows <= bound)
        && snapshot.sorts[0].retained_bytes == 0
        && expected_hops
            .checked_add(1)
            .is_some_and(|expected| snapshot.operator_rss.len() == expected)
        && snapshot.operator_rss.iter().all(|operator| {
            operator.peak_bytes >= operator.before_bytes
                && operator.peak_bytes >= operator.after_bytes
                && (operator.after_bytes > 0 || !cfg!(target_os = "linux"))
        })
        && released_memory_is_bounded(snapshot)
        && snapshot
            .hops
            .values()
            .all(|hop| hop.reads_after_cancel == 0)
}

fn bounded_ordered_limit(
    snapshot: &DemandSnapshot,
    expected_hops: usize,
    limit: usize,
    live_nodes: u64,
    live_edges: u64,
) -> bool {
    let expected_rows = if expected_hops == 1 {
        (limit as u64).min(live_edges)
    } else {
        limit as u64
    };
    if snapshot.sorts.is_empty() {
        if snapshot.operator_rss.first().is_some_and(|operator| {
            matches!(operator.operator, "ordered_one_hop" | "ordered_two_hop")
        }) {
            return bounded_ordered_leaf(snapshot, expected_hops, limit, live_nodes, expected_rows);
        }
        return expected_hops == 1 && bounded_no_sort_one_hop(snapshot, limit, expected_rows);
    }
    bounded_streaming_ordered_limit(snapshot, expected_hops, limit)
}

fn no_sort_one_hop_snapshot(limit: usize) -> DemandSnapshot {
    let batch_rows = u64::try_from(graphforge_exec::SessionResourceConfig::default().batch_size)
        .expect("canonical execution batch rows fit u64");
    let mut snapshot = DemandSnapshot {
        execution_batch_rows: batch_rows,
        returned_batch_bytes: u64::try_from(limit)
            .expect("synthetic limit fits u64")
            .checked_mul(16)
            .expect("synthetic returned-byte count overflow"),
        operator_rss: vec![demand::OperatorRssSnapshot {
            ordinal: 0,
            operator: "expand",
            before_bytes: 100,
            peak_bytes: 120,
            after_bytes: 110,
        }],
        ..DemandSnapshot::default()
    };
    snapshot.hops.insert(
        1,
        demand::HopSnapshot {
            input_batches: 1,
            input_rows: 1_024,
            // Truthful no-sort one-hop observation. 4_310 was the stale mix
            // 3_300 (late one-hop) + 1_010 (fused two-hop) and must not be
            // treated as a legitimate one-hop baseline.
            candidates_generated: 3_300,
            rows_emitted: 3_300,
            ..demand::HopSnapshot::default()
        },
    );
    snapshot
}

#[test]
fn no_sort_one_hop_budget_requires_exact_expand_and_bounded_rows() {
    let limit = 1_000;
    let baseline = no_sort_one_hop_snapshot(limit);
    assert!(bounded_ordered_limit(&baseline, 1, limit, 16_384, 16_384));
    assert!(
        !bounded_ordered_limit(&baseline, 2, limit, 16_384, 16_384),
        "the no-sort one-hop policy must not weaken optimized two-hop classification"
    );

    macro_rules! reject_mutation {
        ($name:literal, $mutate:expr) => {{
            let mut snapshot = baseline.clone();
            $mutate(&mut snapshot);
            assert!(
                !bounded_ordered_limit(&snapshot, 1, limit, 16_384, 16_384),
                "accepted {}: {snapshot:#?}",
                $name
            );
        }};
    }
    reject_mutation!("missing expand", |s: &mut DemandSnapshot| {
        s.operator_rss.clear();
    });
    reject_mutation!("extra expand", |s: &mut DemandSnapshot| {
        s.operator_rss.push(demand::OperatorRssSnapshot {
            ordinal: 1,
            operator: "expand",
            before_bytes: 100,
            peak_bytes: 120,
            after_bytes: 110,
        });
    });
    reject_mutation!("wrong operator", |s: &mut DemandSnapshot| {
        s.operator_rss[0].operator = "sort";
    });
    reject_mutation!("unexpected sort", |s: &mut DemandSnapshot| {
        s.sorts.push(demand::SortSnapshot {
            fetch: Some(limit),
            ..demand::SortSnapshot::default()
        });
    });
    let row_bound = u64::try_from(limit)
        .expect("synthetic limit fits u64")
        .checked_add(baseline.execution_batch_rows)
        .expect("synthetic row bound");
    reject_mutation!("input row bound exceeded", |s: &mut DemandSnapshot| {
        s.hops.get_mut(&1).unwrap().input_rows =
            row_bound.checked_add(1).expect("synthetic excess input");
    });
    reject_mutation!("candidate row bound exceeded", |s: &mut DemandSnapshot| {
        s.hops.get_mut(&1).unwrap().candidates_generated = row_bound
            .checked_add(1)
            .expect("synthetic excess candidates");
    });
    reject_mutation!("emitted row bound exceeded", |s: &mut DemandSnapshot| {
        let excess = row_bound.checked_add(1).expect("synthetic excess output");
        let hop = s.hops.get_mut(&1).unwrap();
        hop.rows_emitted = excess;
        hop.candidates_generated = excess;
    });
    reject_mutation!("row bound overflow", |s: &mut DemandSnapshot| {
        s.execution_batch_rows = u64::MAX;
    });
}

fn counting_snapshot(limit: usize) -> DemandSnapshot {
    // Fused two-hop work retains actual rejected candidate visits and identity
    // reads, while its leaf consumes no Arrow input batches.
    let mut snapshot = DemandSnapshot {
        execution_batch_rows: 8_192,
        operator_rss: vec![demand::OperatorRssSnapshot {
            ordinal: 0,
            operator: "ordered_two_hop",
            before_bytes: 100,
            peak_bytes: 120,
            after_bytes: 110,
        }],
        returned_batch_bytes: (limit as u64)
            .checked_mul(16)
            .expect("synthetic returned-byte count overflow"),
        ..DemandSnapshot::default()
    };
    snapshot.hops.insert(
        1,
        demand::HopSnapshot {
            candidates_generated: (limit as u64)
                .checked_add(10)
                .expect("synthetic candidate count overflow"),
            rows_emitted: limit as u64,
            adjacency_rows_examined: 53,
            projected_chunks: 52,
            projected_rows: 52,
            projected_columns: 1,
            identity_ranges_selected: 52,
            identity_read_calls: 2,
            identity_bytes_read: 8_192,
            identity_peak_buffer_bytes: 4_096,
            ..demand::HopSnapshot::default()
        },
    );
    snapshot
}

#[test]
fn optimized_leaf_budget_requires_rss_probes_and_actual_identity_work() {
    let limit = 1_000;
    let live_nodes = 16_384;
    for expected_hops in [1, 2] {
        let mut baseline = counting_snapshot(limit);
        if expected_hops == 1 {
            baseline.operator_rss[0].operator = "ordered_one_hop";
            let hop = baseline.hops.get_mut(&1).unwrap();
            hop.candidates_generated = limit as u64;
            hop.projected_chunks = 1;
        }
        // Sparse leading rows may outnumber K without generating candidates.
        baseline.hops.get_mut(&1).unwrap().adjacency_rows_examined = 12_000;
        assert!(bounded_ordered_limit(
            &baseline,
            expected_hops,
            limit,
            live_nodes,
            16_384
        ));
        for mutation in 0..11 {
            let mut invalid = baseline.clone();
            match mutation {
                0 => invalid.operator_rss.clear(),
                1 => invalid.operator_rss[0].operator = "edge_count",
                2 => invalid.operator_rss[0].peak_bytes = 0,
                3 => invalid.hops.get_mut(&1).unwrap().adjacency_rows_examined = 0,
                4 => invalid.hops.get_mut(&1).unwrap().adjacency_rows_examined = live_nodes + 1,
                5 => invalid.hops.get_mut(&1).unwrap().input_batches = 1,
                6 => invalid.hops.get_mut(&1).unwrap().input_rows = 1,
                7 => invalid.hops.get_mut(&1).unwrap().identity_read_calls = 0,
                8 => invalid.hops.get_mut(&1).unwrap().identity_bytes_read = 0,
                9 => invalid.hops.get_mut(&1).unwrap().identity_peak_buffer_bytes = 0,
                10 => invalid.hops.get_mut(&1).unwrap().node_full_reads = 1,
                _ => unreachable!(),
            }
            assert!(
                !bounded_ordered_limit(&invalid, expected_hops, limit, live_nodes, 16_384),
                "accepted optimized family {expected_hops} mutation {mutation}: {invalid:?}"
            );
        }
        assert!(!bounded_ordered_limit(
            &baseline,
            expected_hops,
            limit,
            0,
            16_384
        ));
    }
}

#[test]
fn optimized_one_hop_underfilled_limit_uses_reopened_edge_count() {
    let mut snapshot = counting_snapshot(1_000);
    snapshot.operator_rss[0].operator = "ordered_one_hop";
    let hop = snapshot.hops.get_mut(&1).unwrap();
    hop.candidates_generated = 929;
    hop.rows_emitted = 929;
    hop.adjacency_rows_examined = 997;
    hop.projected_chunks = 1;
    assert!(bounded_ordered_limit(&snapshot, 1, 1_000, 1_024, 929));
    for incorrect in [928, 930] {
        let mut invalid = snapshot.clone();
        let hop = invalid.hops.get_mut(&1).unwrap();
        hop.candidates_generated = incorrect;
        hop.rows_emitted = incorrect;
        assert!(!bounded_ordered_limit(&invalid, 1, 1_000, 1_024, 929));
    }
    assert!(!bounded_ordered_limit(&snapshot, 1, 1_000, 1_024, 1_000));
}

#[test]
fn optimized_two_hop_budget_requires_complete_counting_evidence() {
    let limit = 1_000;
    let baseline = counting_snapshot(limit);
    assert!(bounded_ordered_limit(&baseline, 2, limit, 16_384, 16_384));
    assert!(
        !bounded_ordered_limit(&baseline, 1, limit, 16_384, 16_384),
        "the optimized exception must not weaken the one-hop budget"
    );
    assert!(!bounded_ordered_limit(
        &DemandSnapshot::default(),
        2,
        limit,
        16_384,
        16_384
    ));

    macro_rules! reject_mutation {
        ($name:literal, $mutate:expr) => {{
            let mut snapshot = baseline.clone();
            $mutate(&mut snapshot);
            assert!(
                !bounded_ordered_limit(&snapshot, 2, limit, 16_384, 16_384),
                "accepted {}: {snapshot:#?}",
                $name
            );
        }};
    }
    reject_mutation!("missing hop", |s: &mut DemandSnapshot| s.hops.clear());
    reject_mutation!("under-emission", |s: &mut DemandSnapshot| {
        s.hops.get_mut(&1).unwrap().rows_emitted -= 1;
    });
    reject_mutation!("candidate undercount", |s: &mut DemandSnapshot| {
        s.hops.get_mut(&1).unwrap().candidates_generated = 999;
    });
    reject_mutation!("candidate overflow", |s: &mut DemandSnapshot| {
        s.hops.get_mut(&1).unwrap().candidates_generated = 9_193;
    });
    reject_mutation!("missing projection", |s: &mut DemandSnapshot| {
        s.hops.get_mut(&1).unwrap().projected_rows = 0;
    });
    reject_mutation!("missing ranges", |s: &mut DemandSnapshot| {
        s.hops.get_mut(&1).unwrap().identity_ranges_selected = 0;
    });
    reject_mutation!("unbounded calls", |s: &mut DemandSnapshot| {
        s.hops.get_mut(&1).unwrap().identity_read_calls = 107;
    });
    reject_mutation!("unbounded bytes", |s: &mut DemandSnapshot| {
        s.hops.get_mut(&1).unwrap().identity_bytes_read = 2 * MAX_IDENTITY_BUFFER_BYTES + 1;
    });
    reject_mutation!("per-record seek", |s: &mut DemandSnapshot| {
        s.hops.get_mut(&1).unwrap().identity_per_record_seeks = 1;
    });
    reject_mutation!("edge materialization", |s: &mut DemandSnapshot| {
        s.hops.get_mut(&1).unwrap().edge_reads_started = 1;
    });
    reject_mutation!("node materialization", |s: &mut DemandSnapshot| {
        s.hops.get_mut(&1).unwrap().node_rows_scanned = 1;
    });
    reject_mutation!("stale one-hop mix", |s: &mut DemandSnapshot| {
        // Process-global capture previously accepted late one-hop events into
        // the next two-hop snapshot (4_310 = 3_300 + 1_010) together with
        // leftover expand RSS and scanned rows.
        let hop = s.hops.get_mut(&1).unwrap();
        hop.candidates_generated = 4_310;
        hop.input_batches = 1;
        hop.input_rows = 1_024;
        hop.edge_rows_scanned = 3_300;
        s.operator_rss.push(demand::OperatorRssSnapshot {
            ordinal: 0,
            operator: "expand",
            before_bytes: 100,
            peak_bytes: 120,
            after_bytes: 110,
        });
    });
    reject_mutation!("unexpected cancellation", |s: &mut DemandSnapshot| {
        s.cancellations = 1;
    });
    reject_mutation!("in-flight read", |s: &mut DemandSnapshot| {
        s.max_in_flight_reads = 1;
    });
    reject_mutation!("retained memory", |s: &mut DemandSnapshot| {
        s.memory_reserved_after = 16_001;
    });
}

fn linux_process_memory() -> Value {
    let Ok(contents) = fs::read_to_string("/proc/self/status") else {
        return Value::Null;
    };
    let read_bytes = |name: &str| {
        contents.lines().find_map(|line| {
            line.strip_prefix(name).and_then(|value| {
                value
                    .trim()
                    .trim_end_matches(" kB")
                    .trim()
                    .parse::<u64>()
                    .ok()
                    .and_then(|kb| kb.checked_mul(1024))
            })
        })
    };
    json!({
        "vmhwm_bytes": read_bytes("VmHWM:"),
        "vmrss_bytes": read_bytes("VmRSS:"),
        "rss_anon_bytes": read_bytes("RssAnon:"),
        "rss_file_bytes": read_bytes("RssFile:"),
    })
}

fn phase_journal_value(
    profile: &ScaleProfile,
    rung: &Rung,
    completed_rungs: &[Value],
    phase: &str,
    state: &str,
    steps: &[Value],
    failure: Option<(&str, &str)>,
) -> Value {
    json!({
        "schema": EVIDENCE_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "profile_schema": profile.schema,
        "run_state": state,
        "active_rung": rung.id,
        "active_scale": rung.scale,
        "active_phase": phase,
        "process_memory": linux_process_memory(),
        "completed_rungs": completed_rungs,
        "active_steps": steps,
        "first_failing_phase": failure.map(|(phase, _)| phase),
        "error_class": failure.map(|(_, class)| class),
    })
}

fn write_json_atomically(path: &Path, value: &Value) {
    let parent = path.parent().expect("journal parent");
    fs::create_dir_all(parent).expect("journal parent");
    let sequence = JOURNAL_WRITE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let file_name = path
        .file_name()
        .expect("journal file name")
        .to_string_lossy();
    let temporary = parent.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        sequence
    ));
    let mut file = fs::File::create(&temporary).expect("create journal temporary");
    serde_json::to_writer_pretty(&mut file, value).expect("serialize journal");
    file.write_all(b"\n").expect("terminate journal");
    file.sync_all().expect("sync journal temporary");
    fs::rename(&temporary, path).expect("publish journal");
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .expect("sync journal parent");
}

fn open_persisted_construction<'a>(
    graph: &'a GraphForge,
    path: &Path,
    budgets: GraphConstructionBudgets,
) -> GraphConstructionSession<'a> {
    if path.exists() {
        let opaque = fs::read_to_string(path).expect("read construction session identifier");
        let session_uuid =
            Uuid::parse_str(opaque.trim()).expect("parse construction session identifier");
        return graph
            .resume_graph_construction(session_uuid, budgets)
            .expect("resume persisted construction session");
    }
    let session = graph
        .begin_graph_construction(budgets)
        .expect("begin persisted construction session");
    let parent = path.parent().expect("construction session parent");
    fs::create_dir_all(parent).expect("construction session parent");
    let temporary = parent.join(".construction-session.uuid.tmp");
    let mut file = File::create(&temporary).expect("create construction session identifier");
    writeln!(file, "{}", session.session_uuid().hyphenated())
        .expect("write construction session identifier");
    file.sync_all()
        .expect("sync construction session identifier");
    fs::rename(&temporary, path).expect("publish construction session identifier");
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .expect("sync construction session parent");
    session
}

fn persist_phase_journal(
    profile: &ScaleProfile,
    rung: &Rung,
    completed_rungs: &[Value],
    phase: &str,
    state: &str,
    steps: &[Value],
    failure: Option<(&str, &str)>,
) {
    let Ok(path) = std::env::var("GF_G500_LADDER_JOURNAL_OUT") else {
        return;
    };
    let value = phase_journal_value(profile, rung, completed_rungs, phase, state, steps, failure);
    write_json_atomically(&PathBuf::from(path), &value);
}

fn ingest_subphase() -> &'static str {
    match INGEST_SUBPHASE.load(Ordering::Relaxed) {
        1 => "publish_nodes",
        2 => "merge_edges",
        3 => "publish_edge_chunk",
        4 => "edge_chunk_committed",
        _ => "idle",
    }
}

fn storage_io_value() -> Value {
    let io = graphforge_storage::io_stats::snapshot();
    json!({
        "node_full_reads": io.node_full_reads,
        "node_full_rows": io.node_full_rows,
        "edge_full_reads": io.edge_full_reads,
        "edge_full_rows": io.edge_full_rows,
        "rewrite_commits": io.rewrite_commits,
        "topology_rewrite_existing_rows": io.topology_rewrite_existing_rows,
        "topology_rewrite_new_rows": io.topology_rewrite_new_rows,
        "topology_rewrite_output_rows": io.topology_rewrite_output_rows,
        "topology_rewrite_peak_batch_rows": io.topology_rewrite_peak_batch_rows,
    })
}

struct IngestHeartbeat {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl IngestHeartbeat {
    fn start(
        profile: &ScaleProfile,
        rung: &Rung,
        completed_rungs: &[Value],
        steps: &[Value],
    ) -> Self {
        let Ok(path) = std::env::var("GF_G500_LADDER_JOURNAL_OUT") else {
            return Self {
                stop: Arc::new(AtomicBool::new(true)),
                handle: None,
            };
        };
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let path = PathBuf::from(path);
        let profile_schema = profile.schema.clone();
        let rung_id = rung.id.clone();
        let scale = rung.scale;
        let completed_rungs = completed_rungs.to_vec();
        let steps = steps.to_vec();
        let handle = thread::spawn(move || {
            loop {
                let value = json!({
                    "schema": EVIDENCE_SCHEMA,
                    "schema_version": SCHEMA_VERSION,
                    "profile_schema": profile_schema,
                    "run_state": "ingest_heartbeat",
                    "active_rung": rung_id,
                    "active_scale": scale,
                    "active_phase": "ingest",
                    "active_subphase": ingest_subphase(),
                    "active_chunk_index": INGEST_CHUNK_INDEX.load(Ordering::Relaxed),
                    "process_memory": linux_process_memory(),
                    "storage_io": storage_io_value(),
                    "completed_rungs": completed_rungs,
                    "active_steps": steps,
                    "first_failing_phase": null,
                    "error_class": null,
                });
                write_json_atomically(&path, &value);
                if thread_stop.load(Ordering::Relaxed) {
                    break;
                }
                thread::park_timeout(Duration::from_secs(2));
                if thread_stop.load(Ordering::Relaxed) {
                    break;
                }
            }
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn shutdown(&mut self) -> Option<thread::Result<()>> {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.take().map(|handle| {
            handle.thread().unpark();
            handle.join()
        })
    }

    fn stop(mut self) {
        if let Some(result) = self.shutdown() {
            result.expect("ingest heartbeat");
        }
    }
}

impl Drop for IngestHeartbeat {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[allow(clippy::too_many_lines)]
fn run_rung(
    profile: &ScaleProfile,
    rung: &Rung,
    env: RunEnvelope,
    edge_factor: u32,
    ladder_started: Instant,
    completed_rungs: &[Value],
) -> RungOutcome {
    let started = Instant::now();
    let temporary_workspace;
    let workspace = if let Ok(root) = std::env::var("GF_G500_LADDER_WORKSPACE") {
        PathBuf::from(root).join(&rung.id)
    } else if let Ok(journal) = std::env::var("GF_G500_LADDER_JOURNAL_OUT") {
        PathBuf::from(journal)
            .parent()
            .expect("ladder journal parent")
            .join("workspace")
            .join(&rung.id)
    } else {
        assert_ne!(
            rung.tier, "provisioned",
            "provisioned rungs require GF_G500_LADDER_WORKSPACE or GF_G500_LADDER_JOURNAL_OUT"
        );
        temporary_workspace = TempDir::new().expect("rung workspace");
        temporary_workspace.path().to_path_buf()
    };
    let spill_dir = workspace.join("spill");
    let project = workspace.join("project");
    fs::create_dir_all(&spill_dir).expect("spill dir");
    fs::create_dir_all(&project).expect("project dir");

    let mut steps: Vec<Value> = Vec::new();
    let mut first_failing_phase: Option<&'static str> = None;
    let mut error_class: Option<&'static str> = None;

    // ---- generate ----
    persist_phase_journal(
        profile,
        rung,
        completed_rungs,
        "generate",
        "running",
        &steps,
        None,
    );
    let gen_started = Instant::now();
    let spill = generate_spill_runs(
        rung.scale,
        edge_factor,
        profile.initiator,
        profile.seed,
        rung.buffer_edges,
        &spill_dir,
        None,
    );
    let generate_s = gen_started.elapsed().as_secs_f64();
    let generator_allocation = exact_descriptor_allocation(&spill.runs);
    let generator_allocated_bytes = generator_allocation["allocated_bytes"]
        .as_u64()
        .expect("generator allocated bytes");
    let gen_violation = envelope_violation(&env, ladder_started, generator_allocated_bytes);
    if let Some(class) = gen_violation {
        first_failing_phase = Some("generate");
        error_class = Some(class);
    }
    steps.push(json!({
        "id": "generate",
        "pass": gen_violation.is_none(),
        "wall_time_s": generate_s,
        "rss_peak_bytes": rss_value(),
        "detail": {
            "raw_attempts": spill.raw_attempts,
            "self_loops_rejected": spill.self_loops_rejected,
            "peak_buffer_len": spill.peak_buffer_len,
            "buffer_edges": rung.buffer_edges,
            "run_count": spill.runs.len(),
            "storage": generator_allocation,
        }
    }));
    persist_phase_journal(
        profile,
        rung,
        completed_rungs,
        "generate",
        if gen_violation.is_some() {
            "phase_failed"
        } else {
            "phase_completed"
        },
        &steps,
        first_failing_phase.zip(error_class),
    );

    // ---- ingest (merge + publish through the public facade) ----
    let mut live_unique_edges = 0u64;
    let mut duplicates_rejected = 0u64;
    let mut input_fingerprint = String::from("sha256:");
    let mut ingest_ran = false;
    if first_failing_phase.is_none() {
        persist_phase_journal(
            profile,
            rung,
            completed_rungs,
            "ingest",
            "running",
            &steps,
            None,
        );
        let ingest_started = Instant::now();
        let graph = GraphForge::new(Some(project.to_str().expect("utf8 project")))
            .expect("open GraphForge for ingest");
        graphforge_storage::io_stats::reset();
        INGEST_CHUNK_INDEX.store(0, Ordering::Relaxed);
        INGEST_SUBPHASE.store(1, Ordering::Relaxed);
        let heartbeat = IngestHeartbeat::start(profile, rung, completed_rungs, &steps);
        let mut construction = open_persisted_construction(
            &graph,
            &spill_dir.join("construction-session.uuid"),
            GraphConstructionBudgets::default(),
        );
        let (append_s, reconciliation_s) = if construction.progress().publication_committed {
            let reconciliation_started = Instant::now();
            let mut fingerprint = Sha256::new();
            let merge = merge_runs(&spill.runs, None, |src, dst| {
                fingerprint.update(src.to_le_bytes());
                fingerprint.update(dst.to_le_bytes());
            })
            .expect("reconcile already-published rung");
            live_unique_edges = merge.live_unique_edges;
            duplicates_rejected = merge.duplicates_rejected;
            input_fingerprint = format!("sha256:{}", hex_encode(fingerprint.finalize()));
            (None, Some(reconciliation_started.elapsed().as_secs_f64()))
        } else {
            let append_started = Instant::now();
            publish_nodes(&mut construction, 1u64 << rung.scale, None);
            INGEST_SUBPHASE.store(2, Ordering::Relaxed);
            let mut sink = EdgeSink::new(&mut construction, None);
            let merge =
                merge_runs(&spill.runs, None, |src, dst| sink.push(src, dst)).expect("rung merge");
            sink.flush();
            live_unique_edges = merge.live_unique_edges;
            duplicates_rejected = merge.duplicates_rejected;
            input_fingerprint = format!("sha256:{}", hex_encode(sink.finish()));
            (Some(append_started.elapsed().as_secs_f64()), None)
        };
        let seal_started = Instant::now();
        construction
            .seal_and_publish()
            .expect("publish rung construction");
        let seal_publication_s = seal_started.elapsed().as_secs_f64();
        let construction_evidence = construction.progress().evidence;
        INGEST_SUBPHASE.store(0, Ordering::Relaxed);
        heartbeat.stop();
        drop(graph);
        let committed_snapshot = storage_attribution(&project);
        let ingest_disk_used_bytes = generator_allocated_bytes
            .checked_add(committed_snapshot.allocated_bytes)
            .expect("ingest disk union overflow");
        let committed_storage =
            serde_json::to_value(committed_snapshot).expect("serialize committed storage");
        let construction_phases =
            graphforge_storage::ConstructionPhaseAttribution::from_construction(
                &construction_evidence,
            )
            .expect("derive construction phase attribution");
        construction_phases
            .validate_reconciliation()
            .expect("construction phase attribution reconciliation");
        ingest_ran = true;
        let ingest_s = ingest_started.elapsed().as_secs_f64();
        let ingest_violation = envelope_violation(&env, ladder_started, ingest_disk_used_bytes);
        if let Some(class) = ingest_violation {
            first_failing_phase = Some("ingest");
            error_class = Some(class);
        }
        steps.push(json!({
            "id": "ingest",
            "pass": ingest_violation.is_none(),
            "wall_time_s": ingest_s,
            "rss_peak_bytes": rss_value(),
            "disk_used_bytes": ingest_disk_used_bytes,
            "detail": {
                "live_unique_edges": live_unique_edges,
                "duplicates_rejected": duplicates_rejected,
                "input_fingerprint": input_fingerprint,
                "construction": {
                    "configured_rows_per_chunk": CONSTRUCTION_BATCH_ROWS,
                    "submitted_chunks": submitted_chunk_count(&construction_evidence),
                    "append_wall_time_s": append_s,
                    "reconciliation_wall_time_s": reconciliation_s,
                    "seal_publication_wall_time_s": seal_publication_s,
                    "input_rows": construction_evidence.input_rows,
                    "input_batches": construction_evidence.input_batches,
                    "parquet_shards": construction_evidence.parquet_shards,
                    "immutable_artifacts": construction_evidence.immutable_artifacts,
                    "write_bytes": construction_evidence.write_bytes,
                    "write_operations": construction_evidence.write_operations,
                    "fsync_operations": construction_evidence.fsync_operations,
                    "authentication_read_bytes": construction_evidence.authentication_read_bytes,
                    "authentication_read_operations": construction_evidence.authentication_read_operations,
                    "peak_batch_rows": construction_evidence.peak_batch_rows,
                    "peak_batch_bytes": construction_evidence.peak_batch_bytes,
                    "peak_accounted_live_bytes": construction_evidence.peak_accounted_live_bytes,
                    "peak_run_records": construction_evidence.peak_run_records,
                    "merge_read_records": construction_evidence.merge_read_records,
                    "merge_written_records": construction_evidence.merge_written_records,
                    "merge_groups": construction_evidence.merge_groups,
                    "peak_merge_inputs": construction_evidence.peak_merge_inputs,
                    "merge_read_bytes": construction_evidence.merge_read_bytes,
                    "merge_written_bytes": construction_evidence.merge_written_bytes,
                    "merge_fsync_operations": construction_evidence.merge_fsync_operations,
                    "parquet_read_bytes": construction_evidence.parquet_read_bytes,
                    "parquet_read_operations": construction_evidence.parquet_read_operations,
                    "parquet_write_bytes": construction_evidence.parquet_write_bytes,
                    "parquet_write_operations": construction_evidence.parquet_write_operations,
                    "retained_probe_read_bytes": construction_evidence.retained_probe_read_bytes,
                    "retained_probe_block_loads": construction_evidence.retained_probe_block_loads,
                    "storage_transient_peak_allocated_bytes": construction_evidence.storage_transient_peak_allocated_bytes,
                },
                "committed_storage": committed_storage,
                "application_io_phases": construction_phases,
            }
        }));
        persist_phase_journal(
            profile,
            rung,
            completed_rungs,
            "ingest",
            if ingest_violation.is_some() {
                "phase_failed"
            } else {
                "phase_completed"
            },
            &steps,
            first_failing_phase.zip(error_class),
        );
    }

    // ---- reopen + independently durable recount/query boundaries ----
    let mut node_count = 0u64;
    let mut edge_count = 0u64;
    let mut gsi = String::new();
    if first_failing_phase.is_none() {
        persist_phase_journal(
            profile,
            rung,
            completed_rungs,
            "reopen",
            "running",
            &steps,
            None,
        );
        let reopen_started = Instant::now();
        let graph = GraphForge::new(Some(project.to_str().expect("utf8 project")))
            .expect("reopen GraphForge");
        let reopen_s = reopen_started.elapsed().as_secs_f64();
        let reopen_disk_used_bytes = generator_allocated_bytes
            .checked_add(storage_attribution(&project).allocated_bytes)
            .expect("reopen disk union overflow");
        let reopen_violation = envelope_violation(&env, ladder_started, reopen_disk_used_bytes);
        if let Some(class) = reopen_violation {
            first_failing_phase = Some("reopen");
            error_class = Some(class);
        }
        steps.push(json!({
            "id": "reopen",
            "pass": reopen_violation.is_none(),
            "wall_time_s": reopen_s,
            "rss_peak_bytes": rss_value(),
            "process_memory": linux_process_memory(),
            "detail": { "opened": true }
        }));
        persist_phase_journal(
            profile,
            rung,
            completed_rungs,
            "reopen",
            if reopen_violation.is_some() {
                "phase_failed"
            } else {
                "phase_completed"
            },
            &steps,
            first_failing_phase.zip(error_class),
        );

        if first_failing_phase.is_none() {
            persist_phase_journal(
                profile,
                rung,
                completed_rungs,
                "node_count",
                "running",
                &steps,
                None,
            );
            let count_started = Instant::now();
            let result = graph.node_count(NODE_LABEL);
            let failure = result.as_ref().err().map(ToString::to_string);
            node_count = result.unwrap_or(0);
            let expected = 1u64 << rung.scale;
            let count_disk_used_bytes = generator_allocated_bytes
                .checked_add(storage_attribution(&project).allocated_bytes)
                .expect("node-count disk union overflow");
            let mut violation = envelope_violation(&env, ladder_started, count_disk_used_bytes);
            if failure.is_some() {
                violation = Some("execution_failure");
            } else if node_count != expected {
                violation = Some("result_mismatch");
            }
            if let Some(class) = violation {
                first_failing_phase = Some("node_count");
                error_class = Some(class);
            }
            steps.push(json!({
                "id": "node_count", "pass": violation.is_none(),
                "wall_time_s": count_started.elapsed().as_secs_f64(),
                "rss_peak_bytes": rss_value(), "process_memory": linux_process_memory(),
                "detail": { "count": node_count, "expected": expected, "failure": failure }
            }));
            persist_phase_journal(
                profile,
                rung,
                completed_rungs,
                "node_count",
                if violation.is_some() {
                    "phase_failed"
                } else {
                    "phase_completed"
                },
                &steps,
                first_failing_phase.zip(error_class),
            );
        }

        if first_failing_phase.is_none() {
            persist_phase_journal(
                profile,
                rung,
                completed_rungs,
                "edge_count",
                "running",
                &steps,
                None,
            );
            let count_started = Instant::now();
            let (result, work) = execute_with_bounded_evidence(&graph, COUNT_EDGES);
            let failure = result.as_ref().err().map(ToString::to_string);
            edge_count = result.as_ref().map_or(0, scalar_count);
            gsi = gsi_undirected(node_count, edge_count);
            let count_disk_used_bytes = generator_allocated_bytes
                .checked_add(storage_attribution(&project).allocated_bytes)
                .expect("edge-count disk union overflow");
            let mut violation = envelope_violation(&env, ladder_started, count_disk_used_bytes);
            if failure.is_some() {
                violation = Some("execution_failure");
            } else if edge_count != live_unique_edges {
                violation = Some("result_mismatch");
            } else if work
                .memory_reserved_before
                .checked_add(work.returned_batch_bytes)
                .is_none_or(|bound| work.memory_reserved_after > bound)
            {
                violation = Some("memory_retained");
            }
            if let Some(class) = violation {
                first_failing_phase = Some("edge_count");
                error_class = Some(class);
            }
            steps.push(json!({
                "id": "edge_count", "pass": violation.is_none(),
                "wall_time_s": count_started.elapsed().as_secs_f64(),
                "rss_peak_bytes": rss_value(), "process_memory": linux_process_memory(),
                "detail": { "count": edge_count, "expected": live_unique_edges, "gsi": gsi,
                    "failure": failure, "work": query_work_evidence(&work) }
            }));
            persist_phase_journal(
                profile,
                rung,
                completed_rungs,
                "edge_count",
                if violation.is_some() {
                    "phase_failed"
                } else {
                    "phase_completed"
                },
                &steps,
                first_failing_phase.zip(error_class),
            );
        }

        // ---- deterministic LIMIT queries, each with its own atomic journal ----
        if first_failing_phase.is_none() {
            for (phase, query, expected_hops) in
                [("one_hop", ONE_HOP, 1usize), ("two_hop", TWO_HOP, 2usize)]
            {
                if first_failing_phase.is_some() {
                    break;
                }
                persist_phase_journal(
                    profile,
                    rung,
                    completed_rungs,
                    phase,
                    "running",
                    &steps,
                    None,
                );
                let query_started = Instant::now();
                let (result, work) = execute_with_bounded_evidence(&graph, query);
                let failure = result.as_ref().err().map(ToString::to_string);
                let rows = result.as_ref().map_or(0, row_count);
                let query_disk_used_bytes = generator_allocated_bytes
                    .checked_add(storage_attribution(&project).allocated_bytes)
                    .expect("query disk union overflow");
                let mut violation = envelope_violation(&env, ladder_started, query_disk_used_bytes);
                if failure.is_some() {
                    violation = Some("execution_failure");
                } else if rows != 1_000 {
                    violation = Some("result_mismatch");
                } else if !bounded_ordered_limit(
                    &work,
                    expected_hops,
                    1_000,
                    node_count,
                    edge_count,
                ) {
                    violation = Some("operator_budget_violation");
                }
                if let Some(class) = violation {
                    first_failing_phase = Some(phase);
                    error_class = Some(class);
                }
                steps.push(json!({
                    "id": phase, "pass": violation.is_none(),
                    "wall_time_s": query_started.elapsed().as_secs_f64(),
                    "rss_peak_bytes": rss_value(), "process_memory": linux_process_memory(),
                    "detail": { "rows": rows, "failure": failure,
                        "work": query_work_evidence(&work) }
                }));
                persist_phase_journal(
                    profile,
                    rung,
                    completed_rungs,
                    phase,
                    if violation.is_some() {
                        "phase_failed"
                    } else {
                        "phase_completed"
                    },
                    &steps,
                    first_failing_phase.zip(error_class),
                );
            }
        }
        drop(graph);
    }

    let disk_used_bytes = generator_allocated_bytes
        .checked_add(
            ingest_ran
                .then(|| storage_attribution(&project).allocated_bytes)
                .unwrap_or(0),
        )
        .expect("final disk union overflow");
    // Tri-state: reconciliation is only *evaluated* once ingest has run. A rung
    // stopped in the generate phase is reported as null (not evaluated), never
    // as a forced `true`.
    let reconciles: Option<bool> = ingest_ran.then(|| {
        spill.raw_attempts == live_unique_edges + spill.self_loops_rejected + duplicates_rejected
    });
    let all_steps_pass = steps
        .iter()
        .all(|step| step["pass"].as_bool().unwrap_or(false));
    let passed = first_failing_phase.is_none() && all_steps_pass && reconciles == Some(true);
    let (rss_peak_bytes, rss_source) = match peak_rss() {
        Some((bytes, source)) => (json!(bytes), json!(source)),
        None => (Value::Null, Value::Null),
    };

    let evidence = json!({
        "schema": EVIDENCE_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "profile_schema": profile.schema,
        "track": null,
        "rung": rung.id,
        "scale": rung.scale,
        "tier": rung.tier,
        "edgefactor": edge_factor,
        "gsi": if gsi.is_empty() { Value::Null } else { Value::String(gsi) },
        "pass": passed,
        "first_failing_phase": first_failing_phase,
        "error_class": error_class,
        "reconciles": reconciles,
        "reconciliation_evaluated": reconciles.is_some(),
        "counts": {
            "raw_attempts": spill.raw_attempts,
            "self_loops_rejected": spill.self_loops_rejected,
            "duplicates_rejected": duplicates_rejected,
            "live_unique_edges": live_unique_edges,
            "reconciliation": "raw_attempts == live_unique_edges + self_loops_rejected + duplicates_rejected",
        },
        "persisted": { "node_count": node_count, "edge_count": edge_count },
        "input_fingerprint": input_fingerprint,
        "wall_time_s": started.elapsed().as_secs_f64(),
        "rss_peak_bytes": rss_peak_bytes,
        "rss_source": rss_source,
        "disk_used_bytes": disk_used_bytes,
        "machine_envelope": {
            "rss_bytes": env.rss_bytes,
            "disk_bytes": env.disk_bytes,
            "timeout_s": env.timeout_s,
        },
        "generator": {
            "name": GENERATOR_NAME,
            "source": GENERATOR_SOURCE,
            "version": "1",
            "seed": profile.seed,
            "initiator": {
                "A": profile.initiator.a, "B": profile.initiator.b,
                "C": profile.initiator.c, "D": profile.initiator.d
            },
        },
        "sut": { "name": "graphforge", "version": env!("CARGO_PKG_VERSION"), "git_sha": git_sha() },
        "teps": null,
        "notes": "Bounded-memory engineering green. NOT Official-track, NOT TEPS. Certification of one billion live edges is #745, not this profile.",
        "steps": steps,
    });

    RungOutcome { passed, evidence }
}

fn provisioned_rungs_through(profile: &ScaleProfile, max_scale: u32) -> Result<Vec<Rung>, String> {
    let is_provisioned_max = profile
        .rungs
        .iter()
        .any(|rung| rung.scale == max_scale && rung.tier == "provisioned");
    if !is_provisioned_max {
        return Err(format!(
            "GF_G500_LADDER_MAX_SCALE={max_scale} is not a provisioned profile rung"
        ));
    }
    Ok(profile
        .rungs
        .iter()
        .filter(|rung| rung.tier == "provisioned" && rung.scale <= max_scale)
        .cloned()
        .collect())
}

// ---------------------------------------------------------------------------
// Streaming edge publisher (bounded by CONSTRUCTION_BATCH_ROWS).
// ---------------------------------------------------------------------------

struct EdgeSink<'a, 'graph> {
    session: &'a mut GraphConstructionSession<'graph>,
    cancellation: Option<&'a AtomicBool>,
    buf: Vec<(u32, u32)>,
    chunk_rows: usize,
    chunk_index: u128,
    hasher: Sha256,
}

impl<'a, 'graph> EdgeSink<'a, 'graph> {
    fn new(
        session: &'a mut GraphConstructionSession<'graph>,
        cancellation: Option<&'a AtomicBool>,
    ) -> Self {
        Self::with_chunk_rows(session, cancellation, CONSTRUCTION_BATCH_ROWS)
    }

    fn with_chunk_rows(
        session: &'a mut GraphConstructionSession<'graph>,
        cancellation: Option<&'a AtomicBool>,
        chunk_rows: usize,
    ) -> Self {
        assert!(chunk_rows > 0, "edge chunk rows must be positive");
        EdgeSink {
            session,
            cancellation,
            buf: Vec::with_capacity(chunk_rows),
            chunk_rows,
            chunk_index: 0,
            hasher: Sha256::new(),
        }
    }

    fn push(&mut self, src: u32, dst: u32) {
        self.check_cancellation();
        self.hasher.update(src.to_le_bytes());
        self.hasher.update(dst.to_le_bytes());
        self.buf.push((src, dst));
        if self.buf.len() >= self.chunk_rows {
            self.flush();
        }
    }

    fn flush(&mut self) {
        self.check_cancellation();
        if self.buf.is_empty() {
            return;
        }
        let mut edge_ids = Vec::with_capacity(self.buf.len());
        let mut sources = Vec::with_capacity(self.buf.len());
        let mut targets = Vec::with_capacity(self.buf.len());
        for (local, &(src, dst)) in self.buf.iter().enumerate() {
            let ordinal = self
                .chunk_index
                .checked_mul(self.chunk_rows as u128)
                .and_then(|value| value.checked_add(u128::try_from(local).expect("edge ordinal")))
                .expect("edge ordinal overflow");
            edge_ids.push(uuidv7(0xE000_0000_0000u128 + ordinal + 1));
            sources.push(uuidv7(u128::from(src) + 1));
            targets.push(uuidv7(u128::from(dst) + 1));
        }
        let batch = RecordBatch::try_new(
            CONSTRUCTION_EDGE_SCHEMA.clone(),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(edge_ids.iter().map(Uuid::as_bytes))
                        .expect("edge_uuid column"),
                ),
                Arc::new(StringArray::from(vec![REL_TYPE; self.buf.len()])),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(sources.iter().map(Uuid::as_bytes))
                        .expect("source_uuid column"),
                ),
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(targets.iter().map(Uuid::as_bytes))
                        .expect("target_uuid column"),
                ),
            ],
        )
        .expect("edge batch");
        self.session
            .append_edges(&format!("edges-{:016x}", self.chunk_index), &batch)
            .expect("append construction edge chunk");
        INGEST_CHUNK_INDEX.store(
            u64::try_from(self.chunk_index).expect("edge chunk index exceeds u64"),
            Ordering::Relaxed,
        );
        INGEST_SUBPHASE.store(3, Ordering::Relaxed);
        INGEST_SUBPHASE.store(4, Ordering::Relaxed);
        self.chunk_index += 1;
        self.buf.clear();
    }

    fn check_cancellation(&self) {
        assert!(
            !self
                .cancellation
                .is_some_and(|flag| flag.load(Ordering::SeqCst)),
            "edge publication cancelled"
        );
    }

    fn finish(mut self) -> String {
        // Any residual edges must already be flushed by the caller.
        debug_assert!(self.buf.is_empty());
        hex_encode(std::mem::take(&mut self.hasher).finalize())
    }
}

fn publish_nodes(
    session: &mut GraphConstructionSession<'_>,
    vertex_count: u64,
    cancellation: Option<&AtomicBool>,
) {
    publish_nodes_with_chunk_rows(session, vertex_count, cancellation, CONSTRUCTION_BATCH_ROWS);
}

fn publish_nodes_with_chunk_rows(
    session: &mut GraphConstructionSession<'_>,
    vertex_count: u64,
    cancellation: Option<&AtomicBool>,
    chunk_rows: usize,
) {
    assert!(chunk_rows > 0, "node chunk rows must be positive");
    let total = usize::try_from(vertex_count).expect("vertex count fits usize");
    let mut offset = 0usize;
    while offset < total {
        assert!(
            !cancellation.is_some_and(|flag| flag.load(Ordering::SeqCst)),
            "node publication cancelled"
        );
        let end = (offset + chunk_rows).min(total);
        let count = end - offset;
        let ids = (offset..end)
            .map(|index| uuidv7(u128::try_from(index + 1).expect("node seed")))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            CONSTRUCTION_NODE_SCHEMA.clone(),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(ids.iter().map(Uuid::as_bytes))
                        .expect("node_uuid column"),
                ),
                Arc::new(StringArray::from(vec![NODE_LABEL; count])),
            ],
        )
        .expect("node batch");
        session
            .append_nodes(&format!("nodes-{offset:016x}"), &batch)
            .expect("append construction node chunk");
        offset = end;
    }
}

// ---------------------------------------------------------------------------
// Shared small helpers (kept local so the #710 SCALE-20 client stays untouched).
// ---------------------------------------------------------------------------

fn scalar_count(result: &graphforge_api::ExecutionResult) -> u64 {
    assert_eq!(row_count(result), 1, "count query must return one row");
    let column = result.batches[0].column(0);
    if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
        u64::try_from(values.value(0)).expect("non-negative count")
    } else if let Some(values) = column.as_any().downcast_ref::<UInt64Array>() {
        values.value(0)
    } else {
        panic!("unexpected count type {:?}", column.data_type());
    }
}

fn row_count(result: &graphforge_api::ExecutionResult) -> usize {
    result.batches.iter().map(RecordBatch::num_rows).sum()
}

fn gsi_undirected(vertex_count: u64, edge_count: u64) -> String {
    let (code, tag) = scale_band(vertex_count);
    format!(
        "GU-{code}-{tag}-{}",
        density_code(undirected_density(vertex_count, edge_count))
    )
}

fn scale_band(vertex_count: u64) -> (&'static str, &'static str) {
    match vertex_count {
        0..=99 => ("01", "XS"),
        100..=999 => ("02", "XS"),
        1_000..=9_999 => ("03", "XS"),
        10_000..=99_999 => ("04", "XS"),
        100_000..=999_999 => ("05", "SM"),
        1_000_000..=9_999_999 => ("06", "MD"),
        10_000_000..=99_999_999 => ("07", "LG"),
        100_000_000..=999_999_999 => ("08", "XL"),
        1_000_000_000..=9_999_999_999 => ("09", "2XL"),
        10_000_000_000..=99_999_999_999 => ("10", "3XL"),
        _ => ("**", "BIG"),
    }
}

#[allow(clippy::cast_precision_loss)]
fn undirected_density(vertex_count: u64, edge_count: u64) -> f64 {
    if vertex_count < 2 {
        return 0.0;
    }
    let denom = (vertex_count as f64) * ((vertex_count - 1) as f64);
    ((2.0 * edge_count as f64) / denom).clamp(0.0, 1.0)
}

#[allow(clippy::cast_possible_truncation)]
fn density_code(density: f64) -> String {
    let percent = (density * 100.0).round() as i32;
    format!("D{percent:02}")
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

fn uuidv7(seed: u128) -> Uuid {
    let mut bytes = seed.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn git_sha() -> Value {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map_or(Value::Null, |sha| Value::String(sha.trim().to_owned()))
}

/// Returns `(bytes, source)`. `"vmhwm"` (Linux `/proc/self/status`) is a true
/// high-water mark; `"ps_sampled"` (fallback) is the instantaneous RSS at the
/// moment of the call, i.e. a **lower bound** on the real peak. Consumers must
/// treat a `ps_sampled` value as a floor, not a ceiling.
fn peak_rss() -> Option<(u64, &'static str)> {
    if let Ok(contents) = fs::read_to_string("/proc/self/status") {
        for line in contents.lines() {
            if let Some(value) = line.strip_prefix("VmHWM:") {
                let kb = value
                    .trim()
                    .trim_end_matches(" kB")
                    .trim()
                    .parse::<u64>()
                    .ok()?;
                return kb.checked_mul(1024).map(|bytes| (bytes, "vmhwm"));
            }
        }
    }
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let kb = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u64>()
        .ok()?;
    kb.checked_mul(1024).map(|bytes| (bytes, "ps_sampled"))
}

struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    #[allow(clippy::cast_precision_loss)]
    fn next_unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / ((1u64 << 53) as f64)
    }
}

/// Naive whole-graph reference generation (small scales only) used to prove the
/// bounded external-sort path yields the identical live edge set.
fn reference_generation(
    scale: u32,
    edge_factor: u32,
    init: Initiator,
    seed: u64,
) -> (Vec<(u32, u32)>, u64, u64) {
    let raw = (1u64 << scale) * u64::from(edge_factor);
    let mut rng = SplitMix64(seed);
    let mut edges = Vec::new();
    let mut self_loops = 0u64;
    for _ in 0..raw {
        let (src, dst) = kronecker_edge(scale, init, &mut rng);
        if src == dst {
            self_loops += 1;
            continue;
        }
        let (lo, hi) = if src < dst { (src, dst) } else { (dst, src) };
        edges.push((u32::try_from(lo).unwrap(), u32::try_from(hi).unwrap()));
    }
    edges.sort_unstable();
    edges.dedup();
    (edges, raw, self_loops)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The versioned profile is well-formed and carries the M5 host envelope.
#[test]
fn ladder_profile_is_versioned_and_pinned() {
    let profile = load_profile();
    assert_eq!(profile.schema, PROFILE_SCHEMA);
    assert_eq!(profile.schema_version, SCHEMA_VERSION);
    assert_eq!(profile.edgefactor, 16, "Official parameter ef must be 16");
    // Declared Linux cloud SKU (#745): 128 GiB RSS, 1 TiB disk, 4 h fail-safe.
    assert_eq!(profile.envelope.rss_bytes, 137_438_953_472);
    assert_eq!(profile.envelope.disk_bytes, 1_099_511_627_776);
    assert_eq!(profile.envelope.timeout_s, 14_400);
    assert!(!profile.invocation.is_empty());
    for required in [
        "raw_attempts",
        "self_loops_rejected",
        "duplicates_rejected",
        "live_unique_edges",
    ] {
        assert!(
            profile.metrics.iter().any(|m| m == required),
            "profile metrics must declare {required}"
        );
    }
    // Exactly one always-on CI rung, and a SCALE-26 provisioned terminal rung.
    assert_eq!(
        profile.rungs.iter().filter(|r| r.tier == "ci").count(),
        1,
        "exactly one CI rung must run in normal validation"
    );
    assert!(
        profile
            .rungs
            .iter()
            .any(|r| r.scale == 26 && r.tier == "provisioned"),
        "ladder must include a provisioned SCALE-26 terminal rung"
    );
    // Ladder scales are strictly increasing.
    assert!(
        profile.rungs.windows(2).all(|w| w[0].scale < w[1].scale),
        "rung scales must strictly increase"
    );
}

/// Raw attempts, self-loops, duplicates, and live edges reconcile exactly.
#[test]
fn bounded_generation_reconciles_counts() {
    let init = Initiator {
        a: 0.57,
        b: 0.19,
        c: 0.19,
        d: 0.05,
    };
    let (summary, edges) = bounded_generation(10, 16, init, 1, 4096);
    assert_eq!(summary.raw_attempts, (1u64 << 10) * 16);
    assert!(
        summary.self_loops_rejected > 0,
        "Kronecker must yield self-loops"
    );
    assert!(
        summary.duplicates_rejected > 0,
        "Kronecker must yield duplicates"
    );
    assert_eq!(summary.live_unique_edges, edges.len() as u64);
    assert!(
        summary.reconciles(),
        "raw {} != live {} + self_loops {} + dups {}",
        summary.raw_attempts,
        summary.live_unique_edges,
        summary.self_loops_rejected,
        summary.duplicates_rejected
    );
    // Live edges are canonical (lo < hi) and strictly sorted/unique.
    assert!(
        edges.iter().all(|(s, d)| s < d),
        "edges must be canonical undirected"
    );
    assert!(
        edges.windows(2).all(|w| w[0] < w[1]),
        "edges must be strictly sorted & unique"
    );
}

/// Raw attempts can never be reported as live persisted edges.
#[test]
fn raw_attempts_exceed_live_edges() {
    let init = Initiator {
        a: 0.57,
        b: 0.19,
        c: 0.19,
        d: 0.05,
    };
    let (summary, _) = bounded_generation(12, 16, init, 1, 8192);
    assert!(
        summary.live_unique_edges < summary.raw_attempts,
        "live persisted edges must be strictly fewer than raw attempts"
    );
}

/// Same seed + profile ⇒ identical fingerprint and counts across two runs.
#[test]
fn bounded_generation_is_deterministic() {
    let init = Initiator {
        a: 0.57,
        b: 0.19,
        c: 0.19,
        d: 0.05,
    };
    let (a, _) = bounded_generation(11, 16, init, 1, 3000);
    // Different buffer size must not change the deduplicated result.
    let (b, _) = bounded_generation(11, 16, init, 1, 512);
    assert_eq!(a.input_fingerprint, b.input_fingerprint);
    assert_eq!(a.live_unique_edges, b.live_unique_edges);
    assert_eq!(a.self_loops_rejected, b.self_loops_rejected);
    assert_eq!(a.duplicates_rejected, b.duplicates_rejected);
}

/// The bounded external-sort path yields the identical live edge set as a
/// naive whole-graph in-memory generation.
#[test]
fn bounded_generation_matches_reference() {
    let init = Initiator {
        a: 0.57,
        b: 0.19,
        c: 0.19,
        d: 0.05,
    };
    let (reference, raw, self_loops) = reference_generation(9, 16, init, 1);
    let (summary, edges) = bounded_generation(9, 16, init, 1, 777);
    assert_eq!(summary.raw_attempts, raw);
    assert_eq!(summary.self_loops_rejected, self_loops);
    assert_eq!(
        edges, reference,
        "bounded live edge set must equal reference"
    );
}

/// A buffer smaller than the rung's edge count must spill multiple runs while
/// keeping peak resident edges at or below the buffer.
#[test]
fn bounded_generation_spills_and_stays_bounded() {
    let init = Initiator {
        a: 0.57,
        b: 0.19,
        c: 0.19,
        d: 0.05,
    };
    let buffer = 1024usize;
    let (summary, _) = bounded_generation(12, 16, init, 1, buffer);
    assert!(
        summary.run_count > 1,
        "small buffer must spill multiple runs"
    );
    assert!(
        summary.peak_buffer_len <= buffer,
        "resident edges {} must stay within buffer {buffer}",
        summary.peak_buffer_len
    );
}

/// The first envelope violation stops the ladder and records the first failing
/// phase without any pass claim. Uses a deliberately tiny RSS ceiling so the
/// stop is deterministic on any host.
#[test]
fn first_fail_stops_at_envelope_violation() {
    let profile = load_profile();
    let tiny_env = RunEnvelope {
        rss_bytes: 1,
        disk_bytes: 1,
        timeout_s: 14_400,
    };
    let ci_rung = profile
        .rungs
        .iter()
        .find(|r| r.tier == "ci")
        .cloned()
        .expect("ci rung");
    let outcome = run_rung(
        &profile,
        &ci_rung,
        tiny_env,
        profile.edgefactor,
        Instant::now(),
        &[],
    );
    assert!(!outcome.passed, "a violated envelope must not pass");
    assert_eq!(outcome.evidence["first_failing_phase"], "generate");
    assert_eq!(outcome.evidence["error_class"], "oom");
    // Reconciliation was never evaluated (stopped before ingest): tri-state null.
    assert!(outcome.evidence["reconciles"].is_null());
    assert_eq!(outcome.evidence["reconciliation_evaluated"], false);
    assert!(outcome.evidence["teps"].is_null());
    assert!(
        outcome.evidence["scale"].as_u64().unwrap() < 26,
        "a failed low rung must not carry a SCALE-26 pass claim"
    );
}

/// Always-on CI rung: full bounded generate → ingest → reopen → LIMIT flow on
/// the public facade, with reconciliation and persisted-count evidence.
#[test]
fn ci_rung_public_facade_engineering_green() {
    let profile = load_profile();
    let ci_rung = profile
        .rungs
        .iter()
        .find(|r| r.tier == "ci")
        .cloned()
        .expect("ci rung");
    let outcome = run_rung(
        &profile,
        &ci_rung,
        profile.envelope.into(),
        profile.edgefactor,
        Instant::now(),
        &[],
    );
    assert!(outcome.passed, "CI rung must pass: {:#}", outcome.evidence);
    let ev = &outcome.evidence;
    assert_eq!(ev["schema"], EVIDENCE_SCHEMA);
    assert!(ev["track"].is_null(), "must not claim Official track");
    assert!(ev["teps"].is_null());
    assert_eq!(ev["reconciles"], true);
    assert!(ev["first_failing_phase"].is_null());
    // Persisted counts equal the reconciled live edge count.
    let live = ev["counts"]["live_unique_edges"].as_u64().unwrap();
    assert_eq!(ev["persisted"]["edge_count"].as_u64().unwrap(), live);
    assert_eq!(
        ev["persisted"]["node_count"].as_u64().unwrap(),
        1u64 << ci_rung.scale
    );
    assert!(live > 0, "CI rung must persist a non-empty graph");
}

#[test]
fn provisioned_max_scale_excludes_larger_rungs() {
    let profile = load_profile();
    let selected = provisioned_rungs_through(&profile, 25).expect("S25 is provisioned");
    assert_eq!(
        selected.iter().map(|rung| rung.scale).collect::<Vec<_>>(),
        vec![20, 22, 24, 25]
    );
    assert!(selected.iter().all(|rung| rung.scale != 26));
    assert!(provisioned_rungs_through(&profile, 10).is_err());
    assert!(provisioned_rungs_through(&profile, 23).is_err());
}

#[test]
fn phase_journal_atomically_preserves_completed_rungs_and_active_state() {
    let profile = load_profile();
    let rung = profile
        .rungs
        .iter()
        .find(|rung| rung.scale == 22)
        .expect("S22 rung");
    let completed = vec![json!({"rung": "S20", "pass": true})];
    let steps = vec![json!({"id": "generate", "pass": true})];
    let journal = phase_journal_value(
        &profile,
        rung,
        &completed,
        "ingest",
        "phase_failed",
        &steps,
        Some(("ingest", "disk_limit")),
    );
    assert_eq!(journal["completed_rungs"], json!(completed));
    assert_eq!(journal["active_rung"], "S22");
    assert_eq!(journal["active_phase"], "ingest");
    assert_eq!(journal["run_state"], "phase_failed");
    assert_eq!(journal["first_failing_phase"], "ingest");
    assert_eq!(journal["error_class"], "disk_limit");

    let directory = TempDir::new().expect("journal directory");
    let path = directory.path().join("journal.json");
    write_json_atomically(&path, &journal);
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(&path).expect("read journal"))
            .expect("valid journal"),
        journal
    );
    assert_eq!(
        fs::read_dir(directory.path())
            .expect("list journal directory")
            .count(),
        1,
        "atomic publication must not leave a temporary file"
    );
}

// ---------------------------------------------------------------------------
// #745 integrated certification lifecycle. The small test proves that the
// journaled public-facade path is executable in ordinary CI. The provisioned
// entry point below uses the same phases after target-live generation.
// ---------------------------------------------------------------------------

const CERTIFICATION_PHASES: [&str; 21] = [
    "preflight",
    "generate",
    "ingest",
    "csr",
    "source_reopen",
    "source_node_count",
    "source_edge_count",
    "source_query_1hop",
    "source_query_2hop",
    "export",
    "verify",
    "import",
    "imported_reopen",
    "imported_node_count",
    "imported_edge_count",
    "imported_query_1hop",
    "imported_query_2hop",
    "drill_corruption",
    "drill_cancellation",
    "drill_resource_limit",
    "drill_interrupted_finalization",
];

struct PhaseJournal {
    path: PathBuf,
    phases: Vec<Value>,
    monitor: ResourceMonitor,
    allocation: graphforge_storage::StorageAllocationLifecycle,
}

impl PhaseJournal {
    fn new(path: PathBuf, _workspace: &Path, envelope: Envelope) -> Self {
        Self {
            path,
            phases: Vec::new(),
            monitor: ResourceMonitor::start(envelope),
            allocation: graphforge_storage::StorageAllocationLifecycle::default(),
        }
    }

    fn pass(&mut self, id: &str, started: Instant, fingerprint: Option<String>) {
        self.pass_with_detail(id, started, fingerprint, Value::Null);
    }

    fn pass_with_detail(
        &mut self,
        id: &str,
        started: Instant,
        fingerprint: Option<String>,
        detail: Value,
    ) {
        let fingerprint = fingerprint.map_or(Value::Null, Value::String);
        // Every phase owns the live allocation union for its full duration,
        // even when it does not install or remove an allocation identity.
        self.monitor
            .observe_allocated_union(self.allocation.current_allocated_bytes());
        if let Some(code) = self.monitor.failure_code() {
            self.phases.push(json!({
                "id": id, "status": "fail",
                "elapsed_ms": u64::try_from(started.elapsed().as_millis())
                    .expect("lifecycle phase elapsed milliseconds exceed u64"),
                "rss_peak_bytes": self.monitor.peak_rss.load(Ordering::Relaxed),
                "disk_peak_bytes": self.monitor.peak_disk.load(Ordering::Relaxed),
                "fingerprint": fingerprint, "detail": detail, "failure_code": code,
            }));
            self.flush();
            panic!("certification resource watchdog stopped phase {id}: {code}");
        }
        let rss_peak_bytes = self.monitor.peak_rss.swap(0, Ordering::SeqCst);
        let disk_peak_bytes = self.monitor.peak_disk.swap(0, Ordering::SeqCst);
        self.phases.push(json!({
            "id": id, "status": "pass",
            "elapsed_ms": u64::try_from(started.elapsed().as_millis())
                .expect("lifecycle phase elapsed milliseconds exceed u64"),
            "rss_peak_bytes": rss_peak_bytes,
            "disk_peak_bytes": disk_peak_bytes,
            "fingerprint": fingerprint,
            "detail": detail,
            "failure_code": null,
        }));
        self.flush();
    }

    fn fail_with_detail(&mut self, id: &str, started: Instant, code: &str, detail: Value) {
        self.monitor
            .observe_allocated_union(self.allocation.current_allocated_bytes());
        self.phases.push(json!({
            "id": id, "status": "fail",
            "elapsed_ms": u64::try_from(started.elapsed().as_millis())
                .expect("lifecycle phase elapsed milliseconds exceed u64"),
            "rss_peak_bytes": self.monitor.peak_rss.load(Ordering::Relaxed),
            "disk_peak_bytes": self.monitor.peak_disk.load(Ordering::Relaxed),
            "fingerprint": null, "detail": detail, "failure_code": code,
        }));
        self.flush();
    }

    fn cancellation(&self) -> &AtomicBool {
        self.monitor.cancellation.flag()
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.monitor.cancellation.clone()
    }

    fn replace_allocation_owner(&mut self, owner: &str, identities: &BTreeMap<String, u64>) {
        self.allocation
            .replace_owner(owner, identities)
            .expect("replace exact allocation owner");
        self.monitor
            .observe_allocated_union(self.allocation.current_allocated_bytes());
    }

    fn replace_project_owner(
        &mut self,
        owner: &str,
        generation: &graphforge_storage::ResolvedProjectGeneration,
    ) {
        let project = graphforge_storage::capture_project_storage_identity_union(generation)
            .expect("capture retained project identity union");
        self.replace_allocation_owner(owner, &project.physical_identity_allocated_bytes);
    }

    fn replay_allocation_transitions(
        &mut self,
        owner: &str,
        transitions: &[graphforge_storage::StorageAllocationTransition],
    ) {
        for transition in transitions {
            self.allocation
                .apply_owner_transition(owner, transition)
                .expect("apply writer-owned allocation transition");
            self.monitor
                .observe_allocated_union(self.allocation.current_allocated_bytes());
        }
    }

    fn remove_allocation_owner(&mut self, owner: &str) {
        self.allocation
            .remove_owner(owner)
            .expect("remove exact allocation owner");
        self.monitor
            .observe_allocated_union(self.allocation.current_allocated_bytes());
    }

    fn current_allocated_union(&self) -> u64 {
        self.allocation.current_allocated_bytes()
    }

    fn owner_allocated_union(&self, owners: &[&str]) -> u64 {
        self.allocation
            .owner_union_allocated_bytes(owners.iter().copied())
            .expect("capture authoritative owner allocation union")
    }

    fn flush(&self) {
        let staged = self.path.with_extension("json.tmp");
        fs::write(
            &staged,
            serde_json::to_vec_pretty(&self.phases).expect("phase journal JSON"),
        )
        .expect("write staged phase journal");
        fs::rename(staged, &self.path).expect("publish phase journal atomically");
    }
}

impl Drop for PhaseJournal {
    fn drop(&mut self) {
        let already_failed = self
            .phases
            .last()
            .is_some_and(|phase| phase["status"] != "pass");
        if already_failed || self.phases.len() >= CERTIFICATION_PHASES.len() {
            return;
        }
        let failure_code = self
            .monitor
            .failure_code()
            .or_else(|| std::thread::panicking().then_some("operation_failed"));
        if let Some(code) = failure_code {
            self.phases.push(json!({
                "id": CERTIFICATION_PHASES[self.phases.len()], "status": "fail",
                "elapsed_ms": 0,
                "rss_peak_bytes": self.monitor.peak_rss.load(Ordering::Relaxed),
                "disk_peak_bytes": self.monitor.peak_disk.load(Ordering::Relaxed),
                "fingerprint": null, "detail": null, "failure_code": code,
            }));
            self.flush();
        }
    }
}

fn certification_query(
    graph: &GraphForge,
    query: &str,
    phase_id: &str,
    started: Instant,
    journal: &mut PhaseJournal,
) -> (graphforge_api::ExecutionResult, DemandSnapshot) {
    let (result, work) = execute_with_bounded_evidence(graph, query);
    match result {
        Ok(result) => (result, work),
        Err(error) => {
            journal.fail_with_detail(
                phase_id,
                started,
                "execution_failure",
                json!({ "error": error.to_string(), "work": query_work_evidence(&work) }),
            );
            panic!("{phase_id} failed: {error}");
        }
    }
}

struct ResourceMonitor {
    cancellation: CancellationToken,
    stop: Arc<AtomicBool>,
    peak_rss: Arc<AtomicU64>,
    peak_disk: Arc<AtomicU64>,
    failure: Arc<AtomicU64>,
    envelope: Envelope,
    worker: Option<JoinHandle<()>>,
}

impl ResourceMonitor {
    fn start(envelope: Envelope) -> Self {
        let initial_rss = current_rss_bytes().expect("certification host must expose process RSS");
        let cancellation = CancellationToken::new();
        let stop = Arc::new(AtomicBool::new(false));
        let peak_rss = Arc::new(AtomicU64::new(initial_rss));
        let peak_disk = Arc::new(AtomicU64::new(0));
        let failure = Arc::new(AtomicU64::new(0));
        let worker_cancellation = cancellation.clone();
        let worker_stop = Arc::clone(&stop);
        let worker_peak_rss = Arc::clone(&peak_rss);
        let worker_failure = Arc::clone(&failure);
        let started = Instant::now();
        let elapsed_before_process = certification_elapsed_before_process();
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::Relaxed) {
                let rss = current_rss_bytes().expect("certification RSS probe failed");
                worker_peak_rss.fetch_max(rss, Ordering::Relaxed);
                let code = if rss > envelope.rss_bytes {
                    1
                } else if elapsed_before_process
                    .checked_add(started.elapsed())
                    .is_none_or(|elapsed| elapsed.as_secs() > envelope.timeout_s)
                {
                    3
                } else {
                    0
                };
                if code != 0 {
                    worker_failure
                        .compare_exchange(0, code, Ordering::SeqCst, Ordering::Relaxed)
                        .ok();
                    worker_cancellation.cancel();
                    break;
                }
                thread::sleep(Duration::from_millis(250));
            }
        });
        Self {
            cancellation,
            stop,
            peak_rss,
            peak_disk,
            failure,
            envelope,
            worker: Some(worker),
        }
    }

    fn observe_allocated_union(&self, bytes: u64) {
        self.peak_disk.fetch_max(bytes, Ordering::Relaxed);
        if bytes > self.envelope.disk_bytes {
            self.failure
                .compare_exchange(0, 2, Ordering::SeqCst, Ordering::Relaxed)
                .ok();
            self.cancellation.cancel();
        }
    }

    fn failure_code(&self) -> Option<&'static str> {
        match self.failure.load(Ordering::SeqCst) {
            1 => Some("rss_limit_exceeded"),
            2 => Some("disk_limit_exceeded"),
            3 => Some("wall_time_limit_exceeded"),
            _ => None,
        }
    }
}

fn certification_elapsed_before_process() -> Duration {
    let Ok(value) = std::env::var("GF_G500_CERT_STARTED_EPOCH_S") else {
        return Duration::ZERO;
    };
    let started = value
        .parse::<u64>()
        .expect("GF_G500_CERT_STARTED_EPOCH_S must be Unix seconds");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_secs();
    Duration::from_secs(now.saturating_sub(started))
}

fn current_rss_bytes() -> Result<u64, &'static str> {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|line| line.starts_with("VmRSS:"))?
                .split_whitespace()
                .nth(1)?
                .parse::<u64>()
                .ok()
        })
        .and_then(|kibibytes| kibibytes.checked_mul(1024))
        .or_else(|| peak_rss().map(|value| value.0))
        .ok_or("process RSS is unavailable")
}

impl Drop for ResourceMonitor {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(worker) = self.worker.take() {
            worker.join().expect("resource watchdog thread");
        }
    }
}

fn result_fingerprint(result: &graphforge_api::ExecutionResult) -> String {
    let mut hasher = Sha256::new();
    if let Some(batch) = result.batches.first() {
        for field in batch.schema().fields() {
            hasher.update(field.name().as_bytes());
            hasher.update([0]);
            hasher.update(field.data_type().to_string().as_bytes());
            hasher.update([u8::from(field.is_nullable())]);
        }
    }
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            for column in batch.columns() {
                let value = arrow::util::display::array_value_to_string(column, row)
                    .expect("canonical Arrow display value");
                hasher.update(value.len().to_le_bytes());
                hasher.update(value.as_bytes());
            }
        }
    }
    format!("sha256:{}", hex_encode(hasher.finalize()))
}

fn authority_fingerprint(graph: &GraphForge) -> String {
    let mut hasher = Sha256::new();
    let ontology = graph.workspace_ontology().expect("workspace ontology");
    hasher.update(
        ontology
            .to_canonical_json()
            .expect("canonical workspace ontology"),
    );
    hasher.update(
        graph
            .workspace_configuration()
            .expect("workspace configuration authority")
            .to_canonical_json()
            .expect("canonical workspace configuration"),
    );
    let capabilities = graph
        .project_capabilities()
        .expect("project capability authority");
    // The final public column is the generation identity, which is expected to
    // change during import and therefore is not capability authority.
    for field in capabilities.schema.fields().iter().take(4) {
        hasher.update(field.name().as_bytes());
        hasher.update(field.data_type().to_string().as_bytes());
    }
    for batch in &capabilities.batches {
        for row in 0..batch.num_rows() {
            for column in batch.columns().iter().take(4) {
                let value = arrow::util::display::array_value_to_string(column, row)
                    .expect("canonical capability value");
                hasher.update(value.len().to_le_bytes());
                hasher.update(value.as_bytes());
            }
        }
    }
    format!("sha256:{}", hex_encode(hasher.finalize()))
}

fn current_generation_uuid(graph: &GraphForge) -> Uuid {
    let capabilities = graph.project_capabilities().expect("project capabilities");
    let batch = capabilities
        .batches
        .first()
        .expect("project capabilities contain a generation identity");
    let generations = batch
        .column(4)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("generation_uuid capability column");
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(generations.value(0));
    Uuid::from_bytes(bytes)
}

struct DrillAllocationEvidence {
    project: BTreeMap<String, u64>,
    construction: BTreeMap<String, u64>,
    expanded: BTreeMap<String, u64>,
    cancelled_export: BTreeMap<String, u64>,
}

fn create_bounded_drill_package(
    root: &Path,
    limits: PortableV2Limits,
) -> (PathBuf, String, DrillAllocationEvidence) {
    let project = root.join("drill-source");
    let package = root.join("drill.gfpb");
    fs::create_dir_all(&project).expect("bounded drill project");
    let graph = GraphForge::new(project.to_str()).expect("open bounded drill project");
    let mut construction = graph
        .begin_graph_construction(Default::default())
        .expect("begin bounded drill construction");
    publish_nodes(&mut construction, 8, None);
    let mut sink = EdgeSink::new(&mut construction, None);
    for edge in [(0, 1), (1, 2), (2, 3), (3, 4)] {
        sink.push(edge.0, edge.1);
    }
    sink.flush();
    let _ = sink.finish();
    construction
        .seal_and_publish()
        .expect("publish bounded drill construction");
    let construction_identities = construction
        .progress()
        .evidence
        .storage_active_identity_allocated_bytes;
    drop(construction);
    drop(graph);
    let graph = GraphForge::new(project.to_str()).expect("reopen bounded drill project");
    let project_generation = graphforge_storage::resolve_project_generation(&project)
        .expect("resolve bounded drill project");
    let project_identities =
        graphforge_storage::capture_project_storage_identity_union(&project_generation)
            .expect("bounded drill retained project attribution")
            .physical_identity_allocated_bytes;
    let expanded = root.join("drill-expanded");
    let expanded_receipt = graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: expanded.clone(),
                representation: PortableV2Output::Expanded,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits,
            },
            None,
            |_| {},
        )
        .expect("export compact drill expanded package");
    verify_portable_v2(
        &PortableVerifyRequest {
            input: expanded.clone(),
            mode: PortableV2Mode::Full,
            limits,
        },
        None,
    )
    .expect("verify compact drill expanded package");
    let expanded_identities = expanded_receipt.allocation_identity_allocated_bytes;
    fs::remove_dir_all(&expanded).expect("remove bounded expanded drill package");
    let cancelled = AtomicBool::new(true);
    let cancelled_path = root.join("drill-cancelled.gfpb");
    let cancelled_error = graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: cancelled_path.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits,
            },
            Some(&cancelled),
            |_| {},
        )
        .expect_err("cancelled drill export must fail");
    assert!(!cancelled_path.exists());
    let receipt = graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits,
            },
            None,
            |_| {},
        )
        .expect("export bounded drill package");
    (
        package,
        receipt.package_digest,
        DrillAllocationEvidence {
            project: project_identities,
            construction: construction_identities,
            expanded: expanded_identities,
            cancelled_export: cancelled_error.allocation_identity_allocated_bytes,
        },
    )
}

#[allow(clippy::too_many_lines)]
fn run_integrated_certification(root: &Path, target_live: Option<u64>) -> Value {
    run_integrated_certification_with_edge_factor(root, target_live, None)
}

#[allow(clippy::too_many_lines)]
fn run_integrated_certification_with_edge_factor(
    root: &Path,
    target_live: Option<u64>,
    preflight_edge_factor: Option<u32>,
) -> Value {
    run_integrated_certification_config(
        root,
        target_live,
        preflight_edge_factor,
        1,
        CONSTRUCTION_BATCH_ROWS,
        false,
    )
    .value
}

#[derive(Clone, Copy, Debug)]
enum LinearityAxis {
    Nodes,
    Edges,
}

struct IntegratedCertificationEvidence {
    value: Value,
}

fn checked_lifecycle_peak_allocation(
    current_allocated_bytes: u64,
    transient_allocated_bytes: u64,
) -> Result<u64, String> {
    current_allocated_bytes
        .checked_add(transient_allocated_bytes)
        .ok_or_else(|| "lifecycle peak allocation overflow".to_owned())
}

fn run_integrated_certification_for_linearity(
    root: &Path,
    axis: LinearityAxis,
    factor: u32,
) -> IntegratedCertificationEvidence {
    // The production-sized append window would make every SCALE-10 preflight
    // fit in one object. A small deterministic window lets the 1x/2x/4x proof
    // observe byte, call, block, and object slopes without changing runtime
    // behavior or the provider profile.
    let (node_factor, edge_factor) = match axis {
        LinearityAxis::Nodes => (factor, 1),
        LinearityAxis::Edges => (1, factor),
    };
    run_integrated_certification_config(root, None, Some(edge_factor), node_factor, 256, true)
}

#[allow(clippy::too_many_lines)]
fn run_integrated_certification_config(
    root: &Path,
    target_live: Option<u64>,
    preflight_edge_factor: Option<u32>,
    preflight_node_factor: u32,
    proof_chunk_rows: usize,
    scale_recovery_drill: bool,
) -> IntegratedCertificationEvidence {
    let source = root.join("source");
    let imported = root.join("imported");
    let package = root.join("project.gfpb");
    fs::create_dir_all(&source).expect("source project directory");
    let journal_path = std::env::var("GF_G500_CERT_JOURNAL_OUT")
        .map_or_else(|_| root.join("phase-journal.json"), PathBuf::from);
    let certification_profile = load_certification_profile();
    assert_eq!(
        certification_profile.schema,
        "graphforge-billion-edge-certification-profile/1"
    );
    assert_eq!(
        certification_profile.provider_decision,
        "required-at-dispatch"
    );
    assert_eq!(certification_profile.runner_label, "required-at-dispatch");
    let certification_envelope = certification_profile.envelope;
    let mut journal = PhaseJournal::new(journal_path, root, certification_envelope);
    let limits = PortableV2Limits::default();
    let phase = Instant::now();
    journal.pass("preflight", phase, None);

    let phase = Instant::now();
    let scale = if target_live.is_some() {
        certification_profile.scale
    } else {
        certification_profile.preflight_scale
    };
    let edge_factor = if target_live.is_some() {
        certification_profile.edgefactor
    } else {
        preflight_edge_factor.unwrap_or(4)
    };
    let initiator = if target_live.is_some() {
        certification_profile.initiator
    } else {
        load_profile().initiator
    };
    let seed = if target_live.is_some() {
        certification_profile.seed
    } else {
        load_profile().seed
    };
    let spill_root = root.join("spill");
    fs::create_dir_all(&spill_root).expect("certification spill root");
    let generated = target_live.map(|target| {
        generate_target_live_runs(
            &TargetLiveGeneration {
                scale,
                edge_factor,
                initiator,
                seed,
                buffer_edges: if scale == 26 { 16_777_216 } else { 512 },
                target_live_edges: target,
            },
            &spill_root,
            journal.cancellation(),
        )
    });
    let (spills, generated_counts, target_live_fingerprint) = generated
        .map_or((None, None, None), |(spills, counts, fingerprint)| {
            (Some(spills), Some(counts), Some(fingerprint))
        });
    let (summary, edges) = if target_live.is_none() {
        let (summary, edges) = bounded_generation(scale, edge_factor, initiator, seed, 512);
        assert!(summary.reconciles());
        (Some(summary), Some(edges))
    } else {
        (None, None)
    };
    let generation_fingerprint = summary.as_ref().map_or_else(
        || target_live_fingerprint.expect("target-live payload fingerprint"),
        |value| value.input_fingerprint.clone(),
    );
    if let Some(spills) = &spills {
        journal.replace_allocation_owner(
            "generator_spill",
            &exact_descriptor_identities(&spills.runs),
        );
    }
    journal.pass("generate", phase, Some(generation_fingerprint.clone()));

    let phase = Instant::now();
    let graph = GraphForge::new(source.to_str()).expect("open certification source");
    let initial_generation = graphforge_storage::resolve_project_generation(&source)
        .expect("resolve initial source generation");
    journal.replace_project_owner("source_project", &initial_generation);
    let mut construction = graph
        .begin_graph_construction(Default::default())
        .expect("begin certification construction");
    let node_count = (1_u64 << scale)
        .checked_mul(u64::from(preflight_node_factor))
        .expect("linearity node count");
    publish_nodes_with_chunk_rows(
        &mut construction,
        node_count,
        Some(journal.cancellation()),
        proof_chunk_rows,
    );
    let mut sink = EdgeSink::with_chunk_rows(
        &mut construction,
        Some(journal.cancellation()),
        proof_chunk_rows,
    );
    if let Some(edges) = edges {
        for (src, dst) in edges {
            sink.push(src, dst);
        }
    } else {
        merge_runs(
            &spills.as_ref().expect("target spills").runs,
            Some(journal.cancellation()),
            |src, dst| sink.push(src, dst),
        )
        .expect("certification merge was cancelled");
    }
    sink.flush();
    let input_fingerprint = format!("sha256:{}", sink.finish());
    assert_eq!(input_fingerprint, generation_fingerprint);
    construction
        .seal_and_publish()
        .expect("publish certification construction");
    let construction_evidence = construction.progress().evidence;
    let mut construction_phases =
        graphforge_storage::ConstructionPhaseAttribution::from_construction(&construction_evidence)
            .expect("derive certification construction phases");
    construction_phases
        .validate_for_qualification()
        .expect("certification construction phase attribution");
    let pre_construction_union = journal.current_allocated_union();
    journal.replay_allocation_transitions(
        "construction",
        &construction_evidence.storage_allocation_transitions,
    );
    // Construction artifacts are private to this session and cannot alias the
    // already-open source project. The storage-owned numeric high-water mark
    // therefore restores peaks compacted out of durable checkpoint history.
    journal.monitor.observe_allocated_union(
        checked_lifecycle_peak_allocation(
            pre_construction_union,
            construction_evidence.storage_transient_peak_total_allocated_bytes,
        )
        .expect("lifecycle peak allocation overflow"),
    );
    let committed_generation = graphforge_storage::resolve_project_generation(&source)
        .expect("resolve committed ingest generation");
    journal.replace_project_owner("source_project", &committed_generation);
    journal.pass("ingest", phase, Some(input_fingerprint));

    let phase = Instant::now();
    let csr = graph
        .rebuild_adjacency(Some(journal.cancellation_token()))
        .expect("build certification CSR");
    let csr_generation = graphforge_storage::resolve_project_generation(&source)
        .expect("resolve committed CSR generation");
    journal.replace_project_owner("source_project", &csr_generation);
    journal.pass(
        "csr",
        phase,
        csr.artifact_fingerprint
            .map(|value| format!("sha256:{value}")),
    );
    drop(graph);

    let phase = Instant::now();
    let graph = GraphForge::new(source.to_str()).expect("reopen source");
    journal.pass("source_reopen", phase, None);
    let phase = Instant::now();
    let source_nodes = graph.node_count(NODE_LABEL).expect("source nodes");
    journal.pass_with_detail(
        "source_node_count",
        phase,
        None,
        json!({ "count": source_nodes }),
    );
    let phase = Instant::now();
    let (source_edge_result, source_edge_work) = certification_query(
        &graph,
        COUNT_EDGES,
        "source_edge_count",
        phase,
        &mut journal,
    );
    let source_edges = scalar_count(&source_edge_result);
    let expected_live_edges = generated_counts.as_ref().map_or_else(
        || {
            summary
                .as_ref()
                .expect("bounded generation summary")
                .live_unique_edges
        },
        |counts| counts.live_unique_edges,
    );
    assert_eq!(source_edges, expected_live_edges);
    journal.pass_with_detail(
        "source_edge_count",
        phase,
        None,
        json!({ "count": source_edges, "work": query_work_evidence(&source_edge_work) }),
    );
    let phase = Instant::now();
    let (source_1hop_result, source_1hop_work) =
        certification_query(&graph, ONE_HOP, "source_query_1hop", phase, &mut journal);
    assert!(
        bounded_ordered_limit(&source_1hop_work, 1, 1_000, source_nodes, source_edges),
        "{source_1hop_work:#?}"
    );
    assert_eq!(
        row_count(&source_1hop_result) as u64,
        1_000_u64.min(source_edges),
        "unrestricted directed one-hop returns one row per live edge up to LIMIT"
    );
    let source_1hop = result_fingerprint(&source_1hop_result);
    journal.pass_with_detail(
        "source_query_1hop",
        phase,
        Some(source_1hop.clone()),
        query_work_evidence(&source_1hop_work),
    );
    let phase = Instant::now();
    let (source_2hop_result, source_2hop_work) =
        certification_query(&graph, TWO_HOP, "source_query_2hop", phase, &mut journal);
    assert!(
        bounded_ordered_limit(&source_2hop_work, 2, 1_000, source_nodes, source_edges),
        "{source_2hop_work:#?}"
    );
    let source_2hop = result_fingerprint(&source_2hop_result);
    let source_authority_fingerprint = authority_fingerprint(&graph);
    let source_generation = current_generation_uuid(&graph);
    journal.pass_with_detail(
        "source_query_2hop",
        phase,
        Some(source_2hop.clone()),
        query_work_evidence(&source_2hop_work),
    );

    let phase = Instant::now();
    let exported = graph
        .export_portable_v2(
            &PortableV2ExportRequest {
                selection: PortableSelection::Current,
                output_path: package.clone(),
                representation: PortableV2Output::Bundle,
                profile: PortableV2SelectionProfile::Complete,
                subset: None,
                limits,
            },
            Some(journal.cancellation()),
            |_| {},
        )
        .expect("portable-v2 export");
    assert_eq!(source_generation, exported.generation_uuid);
    journal.replace_allocation_owner(
        "portable_package",
        &exported.allocation_identity_allocated_bytes,
    );
    journal.pass("export", phase, Some(exported.package_digest.clone()));
    let phase = Instant::now();
    let verified = verify_portable_v2(
        &PortableVerifyRequest {
            input: package.clone(),
            mode: PortableV2Mode::Full,
            limits,
        },
        Some(journal.cancellation()),
    )
    .expect("full portable-v2 verification");
    assert_eq!(verified.package_digest, exported.package_digest);
    assert_eq!(verified.contract, "graphforge-portable-verify/2");
    journal.pass("verify", phase, Some(verified.package_digest.clone()));

    let phase = Instant::now();
    let imported_receipt = GraphForge::import_portable_v2(
        &imported,
        &PortableV2ImportRequest {
            input: package.clone(),
            operation_id: OperationId(uuidv7(0x745)),
            limits,
        },
        Some(journal.cancellation()),
    )
    .expect("atomic portable-v2 import");
    // Replay the storage-owned operation transitions in their actual order:
    // private materialization coexisted with the published generation until
    // deterministic staging cleanup completed.
    journal.replace_allocation_owner(
        "import_materialized",
        &imported_receipt.materialized_identity_allocated_bytes,
    );
    journal.replace_allocation_owner(
        "clean_import",
        &imported_receipt.published_identity_allocated_bytes,
    );
    assert!(imported_receipt.materialized_cleanup_parent_sync_confirmed);
    assert_eq!(
        imported_receipt.materialized_cleanup_removed_identity_allocated_bytes,
        imported_receipt.materialized_identity_allocated_bytes
    );
    journal.remove_allocation_owner("import_materialized");
    assert_ne!(exported.generation_uuid, imported_receipt.generation_uuid);
    journal.pass(
        "import",
        phase,
        Some(imported_receipt.package_digest.clone()),
    );
    let phase = Instant::now();
    let imported_graph = GraphForge::new(imported.to_str()).expect("reopen import");
    journal.pass("imported_reopen", phase, None);
    let phase = Instant::now();
    let imported_generation = graphforge_storage::resolve_project_generation(&imported)
        .expect("resolve clean import generation");
    journal.replace_project_owner("clean_import_project", &imported_generation);
    let imported_nodes = imported_graph
        .node_count(NODE_LABEL)
        .expect("imported nodes");
    journal.pass_with_detail(
        "imported_node_count",
        phase,
        None,
        json!({ "count": imported_nodes }),
    );
    let phase = Instant::now();
    let (imported_edge_result, imported_edge_work) = certification_query(
        &imported_graph,
        COUNT_EDGES,
        "imported_edge_count",
        phase,
        &mut journal,
    );
    let imported_edges = scalar_count(&imported_edge_result);
    assert_eq!(
        (source_nodes, source_edges),
        (imported_nodes, imported_edges)
    );
    journal.pass_with_detail(
        "imported_edge_count",
        phase,
        None,
        json!({ "count": imported_edges, "work": query_work_evidence(&imported_edge_work) }),
    );
    let phase = Instant::now();
    let (imported_1hop_result, imported_1hop_work) = certification_query(
        &imported_graph,
        ONE_HOP,
        "imported_query_1hop",
        phase,
        &mut journal,
    );
    assert!(
        bounded_ordered_limit(
            &imported_1hop_work,
            1,
            1_000,
            imported_nodes,
            imported_edges
        ),
        "{imported_1hop_work:#?}"
    );
    assert_eq!(
        row_count(&imported_1hop_result) as u64,
        1_000_u64.min(imported_edges),
        "unrestricted directed one-hop returns one row per live edge up to LIMIT"
    );
    let imported_1hop = result_fingerprint(&imported_1hop_result);
    assert_eq!(source_1hop, imported_1hop);
    journal.pass_with_detail(
        "imported_query_1hop",
        phase,
        Some(imported_1hop.clone()),
        query_work_evidence(&imported_1hop_work),
    );
    let phase = Instant::now();
    let (imported_2hop_result, imported_2hop_work) = certification_query(
        &imported_graph,
        TWO_HOP,
        "imported_query_2hop",
        phase,
        &mut journal,
    );
    assert!(
        bounded_ordered_limit(
            &imported_2hop_work,
            2,
            1_000,
            imported_nodes,
            imported_edges
        ),
        "{imported_2hop_work:#?}"
    );
    let imported_2hop = result_fingerprint(&imported_2hop_result);
    let imported_authority_fingerprint = authority_fingerprint(&imported_graph);
    assert_eq!(
        current_generation_uuid(&imported_graph),
        imported_receipt.generation_uuid
    );
    assert_eq!(source_2hop, imported_2hop);
    assert_eq!(source_authority_fingerprint, imported_authority_fingerprint);
    journal.pass_with_detail(
        "imported_query_2hop",
        phase,
        Some(imported_2hop.clone()),
        query_work_evidence(&imported_2hop_work),
    );
    let authority_rung = target_live.map_or_else(
        || u64::from(preflight_node_factor.max(edge_factor)),
        |_| u64::from(scale),
    );
    let (source_storage, source_context) = storage_attribution_value(
        &source,
        "source",
        authority_rung,
        source_nodes,
        source_edges,
    );
    let source_project_current_allocated_bytes =
        graphforge_storage::capture_project_storage_identity_union(
            &graphforge_storage::resolve_project_generation(&source)
                .expect("resolve authoritative source project"),
        )
        .expect("capture authoritative source project identity union")
        .allocated_bytes;
    let (imported_storage, _) = storage_attribution_value(
        &imported,
        "clean_import",
        authority_rung,
        imported_nodes,
        imported_edges,
    );
    let package_storage = portable_export_allocation(
        &exported,
        authority_rung,
        &source_context.generation_sha256,
        source_nodes,
        source_edges,
    );
    let construction_storage = sanitized_construction_evidence(
        &construction_evidence,
        authority_rung,
        &source_context.generation_sha256,
        source_nodes,
        source_edges,
    );
    // Representative drills use the same verifier/import boundaries but never
    // repeat the billion-edge payload.
    let phase = Instant::now();
    let (drill_package, drill_digest, drill_allocation) =
        create_bounded_drill_package(root, limits);
    journal.replace_allocation_owner("drill_project", &drill_allocation.project);
    journal.replace_allocation_owner("drill_construction", &drill_allocation.construction);
    journal.replace_allocation_owner("drill_expanded", &drill_allocation.expanded);
    journal.remove_allocation_owner("drill_expanded");
    journal.replace_allocation_owner("drill_cancelled_export", &drill_allocation.cancelled_export);
    journal.remove_allocation_owner("drill_cancelled_export");
    journal.replace_allocation_owner(
        "drill_package",
        &exact_descriptor_identities(std::slice::from_ref(&drill_package)),
    );
    let drill_verified = verify_portable_v2(
        &PortableVerifyRequest {
            input: drill_package.clone(),
            mode: PortableV2Mode::Full,
            limits,
        },
        None,
    )
    .expect("bounded drill package verifies before mutation");
    assert_eq!(drill_verified.package_digest, drill_digest);
    let corrupt = root.join("corrupt.gfpb");
    fs::copy(&drill_package, &corrupt).expect("copy bounded corrupt drill");
    {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&corrupt)
            .expect("open corrupt drill package");
        file.write_all(b"corruption").expect("append corruption");
        file.flush().expect("flush corruption");
    }
    journal.replace_allocation_owner(
        "corrupt_drill_package",
        &exact_descriptor_identities(std::slice::from_ref(&corrupt)),
    );
    assert!(
        verify_portable_v2(
            &PortableVerifyRequest {
                input: corrupt,
                mode: PortableV2Mode::Full,
                limits
            },
            None
        )
        .is_err()
    );
    journal.pass("drill_corruption", phase, None);
    let phase = Instant::now();
    let cancelled = AtomicBool::new(true);
    assert!(
        verify_portable_v2(
            &PortableVerifyRequest {
                input: drill_package.clone(),
                mode: PortableV2Mode::Full,
                limits
            },
            Some(&cancelled)
        )
        .is_err()
    );
    journal.pass("drill_cancellation", phase, None);
    let phase = Instant::now();
    let tiny = PortableV2Limits {
        max_entries: 1,
        ..limits
    };
    assert!(
        verify_portable_v2(
            &PortableVerifyRequest {
                input: drill_package.clone(),
                mode: PortableV2Mode::Full,
                limits: tiny
            },
            None
        )
        .is_err()
    );
    journal.pass("drill_resource_limit", phase, None);
    let phase = Instant::now();
    let interrupted = root.join("interrupted-target");
    let interrupted_operation = uuidv7(0x746);
    let interrupted_generation =
        graphforge_core::uuid::portable_v2_import_generation(&interrupted_operation);
    let interrupted_cancelled = AtomicBool::new(false);
    let supported_capabilities = [
        "epistemic",
        "graph",
        "knowledge",
        "provenance",
        "valid_time",
        "workspace",
    ]
    .into_iter()
    .map(|capability_id| graphforge_storage::ProjectCapability {
        capability_id: capability_id.into(),
        capability_version: 1,
    })
    .collect::<Vec<_>>();
    let recovery_package = if scale_recovery_drill {
        &package
    } else {
        &drill_package
    };
    let interrupted_error = graphforge_storage::import_complete_portable_v2_with_progress(
        recovery_package,
        &interrupted,
        interrupted_operation,
        interrupted_generation,
        &supported_capabilities,
        limits,
        Some(&interrupted_cancelled),
        |progress| {
            if progress.phase == graphforge_storage::PortableV2ImportPhase::Materialized {
                interrupted_cancelled.store(true, Ordering::SeqCst);
            }
        },
    )
    .expect_err("cancelled finalization must fail");
    assert!(interrupted_error.recovery_reauthentication_read_bytes > 0);
    assert!(interrupted_error.recovery_reauthentication_read_calls > 0);
    construction_phases
        .add_recovery_reauthentication(
            interrupted_error.recovery_reauthentication_read_bytes,
            interrupted_error.recovery_reauthentication_read_calls,
        )
        .expect("accumulate interrupted recovery authority");
    construction_phases
        .validate_for_qualification()
        .expect("interrupted recovery phase attribution");
    journal.replace_allocation_owner(
        "interrupted_import",
        &interrupted_error.allocation_identity_allocated_bytes,
    );
    journal.remove_allocation_owner("interrupted_import");
    assert!(!interrupted.join("CURRENT").exists());
    journal.pass("drill_interrupted_finalization", phase, None);

    assert_eq!(journal.phases.len(), CERTIFICATION_PHASES.len());
    let project_fingerprint = |nodes: u64, edges: u64, one_hop: &str, two_hop: &str| {
        let mut digest = Sha256::new();
        digest.update(nodes.to_le_bytes());
        digest.update(edges.to_le_bytes());
        digest.update(one_hop.as_bytes());
        digest.update(two_hop.as_bytes());
        format!("sha256:{}", hex_encode(digest.finalize()))
    };
    let workspace_current_allocated_bytes = journal.current_allocated_union();
    let workspace_components = BTreeMap::from([
        (
            "source_project_and_construction",
            journal.owner_allocated_union(&["source_project", "construction"]),
        ),
        (
            "portable_package",
            journal.owner_allocated_union(&["portable_package"]),
        ),
        (
            "clean_import_project",
            journal.owner_allocated_union(&["clean_import", "clean_import_project"]),
        ),
        (
            "drill_project_and_construction",
            journal.owner_allocated_union(&["drill_project", "drill_construction"]),
        ),
        (
            "drill_package",
            journal.owner_allocated_union(&["drill_package"]),
        ),
        (
            "corrupt_drill_package",
            journal.owner_allocated_union(&["corrupt_drill_package"]),
        ),
    ]);
    assert_eq!(
        workspace_components
            .values()
            .try_fold(0_u64, |total, allocated| total.checked_add(*allocated))
            .expect("workspace component authority overflow"),
        workspace_current_allocated_bytes,
        "non-overlapping owner-group unions must reconcile to the workspace union"
    );
    let workspace_peak_allocated_bytes = journal
        .phases
        .iter()
        .filter_map(|phase| phase["disk_peak_bytes"].as_u64())
        .max()
        .expect("completed lifecycle phase allocation peaks");
    let evidence = json!({
        "source_export_generation_authenticated": source_generation == exported.generation_uuid,
        "import_receipt_reopen_authenticated": current_generation_uuid(&imported_graph) == imported_receipt.generation_uuid,
        "source_import_generations_distinct": exported.generation_uuid != imported_receipt.generation_uuid,
        "package": exported.package_digest, "transport": exported.transport_digest,
        "raw_attempts": spills.as_ref().map_or_else(|| summary.as_ref().unwrap().raw_attempts, |value| value.raw_attempts),
        "self_loops_rejected": spills.as_ref().map_or_else(|| summary.as_ref().unwrap().self_loops_rejected, |value| value.self_loops_rejected),
        "duplicates_rejected": generated_counts.as_ref().map_or_else(|| summary.as_ref().unwrap().duplicates_rejected, |value| value.duplicates_rejected),
        "generated_live_unique_edges": generated_counts.as_ref().map_or_else(|| summary.as_ref().unwrap().live_unique_edges, |value| value.live_unique_edges),
        "source_nodes": source_nodes, "source_edges": source_edges,
        "imported_nodes": imported_nodes, "imported_edges": imported_edges,
        "source_project_fingerprint": project_fingerprint(source_nodes, source_edges, &source_1hop, &source_2hop),
        "imported_project_fingerprint": project_fingerprint(imported_nodes, imported_edges, &imported_1hop, &imported_2hop),
        "portable_contract": verified.contract,
        "package_class": serde_json::to_value(verified.package_class).expect("package class JSON"),
        "integrity": serde_json::to_value(verified.integrity).expect("integrity JSON"),
        "compatibility": serde_json::to_value(verified.compatibility).expect("compatibility JSON"),
        "source_authority_fingerprint": source_authority_fingerprint,
        "imported_authority_fingerprint": imported_authority_fingerprint,
        "storage": {
            "source": source_storage,
            "source_project_current_allocated_bytes": source_project_current_allocated_bytes,
            "portable_package": package_storage,
            "clean_import": imported_storage,
            "construction": construction_storage,
            "application_io_phases": construction_phases,
            "workspace_current_allocated_bytes": workspace_current_allocated_bytes,
            "workspace_peak_allocated_bytes": workspace_peak_allocated_bytes,
            "workspace_components": workspace_components,
        },
        "phases": journal.phases,
    });
    reject_unsanitized_evidence(&evidence).expect("certification lifecycle evidence is sanitized");
    IntegratedCertificationEvidence { value: evidence }
}

#[test]
fn certification_lifecycle_journals_equivalent_round_trip_and_drills() {
    let root = TempDir::new().expect("certification smoke root");
    let evidence = run_integrated_certification(root.path(), None);
    assert_eq!(evidence["source_edges"], evidence["imported_edges"]);
    assert_eq!(evidence["source_export_generation_authenticated"], true);
    assert_eq!(evidence["import_receipt_reopen_authenticated"], true);
    assert_eq!(evidence["source_import_generations_distinct"], true);
    reject_unsanitized_evidence(&evidence).expect("lifecycle evidence remains sanitized");
}

#[test]
fn certification_evidence_sanitizer_rejects_identity_paths_and_sensitive_keys() {
    for (value, expected) in [
        (
            json!({"proof": "018f6e45-7f12-7c00-8000-000000000001"}),
            "raw UUID",
        ),
        (
            json!({"proof": "/var/lib/graphforge/project"}),
            "absolute host path",
        ),
        (
            json!({"tools": {"build": "artifact=0011223344556677:00112233445566778899aabbccddeeff"}}),
            "raw native object identity",
        ),
        (
            json!({"nested": {"api_token": "redacted"}}),
            "sensitive evidence key",
        ),
    ] {
        let error = reject_unsanitized_evidence(&value).expect_err("unsafe evidence must fail");
        assert!(
            error.contains(expected),
            "unexpected sanitizer failure: {error}"
        );
    }
    reject_unsanitized_evidence(&json!({
        "generation_authenticated": true,
        "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "tools": {
            "rustc": "rustc 1.90.0 (1159e78c4 2026-08-01)",
            "llvm": "llvm:19.1.7"
        }
    }))
    .expect("closed proof is safe");
}

const LINEARITY_PHASE_FIELDS: [&str; 7] = [
    "read_bytes",
    "write_bytes",
    "read_calls",
    "write_calls",
    "object_count",
    "block_count",
    "fsync_calls",
];

#[derive(Clone, Copy)]
enum RetainedMetricPolicy {
    ScaleBearing,
    FilesystemQuantized,
    InventoryDerived,
    NodeBearing,
    EdgeBearing,
    NodeFilesystemQuantized,
    EdgeFilesystemQuantized,
}

const LINEARITY_RETAINED_FIELDS: [(&str, &str, RetainedMetricPolicy); 33] = [
    (
        "source.logical_references",
        "/storage/source/logical_references",
        RetainedMetricPolicy::InventoryDerived,
    ),
    (
        "source.logical_bytes",
        "/storage/source/logical_bytes",
        RetainedMetricPolicy::ScaleBearing,
    ),
    (
        "source.physical_objects",
        "/storage/source/physical_objects",
        RetainedMetricPolicy::InventoryDerived,
    ),
    (
        "source.physical_logical_bytes",
        "/storage/source/physical_logical_bytes",
        RetainedMetricPolicy::ScaleBearing,
    ),
    (
        "source.allocated_bytes",
        "/storage/source/allocated_bytes",
        RetainedMetricPolicy::FilesystemQuantized,
    ),
    (
        "source.canonical_nodes.logical_bytes",
        "/storage/source/categories/topology_nodes/logical_bytes",
        RetainedMetricPolicy::NodeBearing,
    ),
    (
        "source.canonical_nodes.physical_logical_bytes",
        "/storage/source/categories/topology_nodes/physical_logical_bytes",
        RetainedMetricPolicy::NodeBearing,
    ),
    (
        "source.canonical_nodes.allocated_bytes",
        "/storage/source/categories/topology_nodes/allocated_bytes",
        RetainedMetricPolicy::NodeFilesystemQuantized,
    ),
    (
        "source.canonical_edges.logical_bytes",
        "/storage/source/categories/topology_edges/logical_bytes",
        RetainedMetricPolicy::EdgeBearing,
    ),
    (
        "source.canonical_edges.physical_logical_bytes",
        "/storage/source/categories/topology_edges/physical_logical_bytes",
        RetainedMetricPolicy::EdgeBearing,
    ),
    (
        "source.canonical_edges.allocated_bytes",
        "/storage/source/categories/topology_edges/allocated_bytes",
        RetainedMetricPolicy::EdgeFilesystemQuantized,
    ),
    (
        "clean_import.logical_references",
        "/storage/clean_import/logical_references",
        RetainedMetricPolicy::InventoryDerived,
    ),
    (
        "clean_import.logical_bytes",
        "/storage/clean_import/logical_bytes",
        RetainedMetricPolicy::ScaleBearing,
    ),
    (
        "clean_import.physical_objects",
        "/storage/clean_import/physical_objects",
        RetainedMetricPolicy::InventoryDerived,
    ),
    (
        "clean_import.physical_logical_bytes",
        "/storage/clean_import/physical_logical_bytes",
        RetainedMetricPolicy::ScaleBearing,
    ),
    (
        "clean_import.allocated_bytes",
        "/storage/clean_import/allocated_bytes",
        RetainedMetricPolicy::FilesystemQuantized,
    ),
    (
        "clean_import.canonical_nodes.logical_bytes",
        "/storage/clean_import/categories/topology_nodes/logical_bytes",
        RetainedMetricPolicy::NodeBearing,
    ),
    (
        "clean_import.canonical_nodes.physical_logical_bytes",
        "/storage/clean_import/categories/topology_nodes/physical_logical_bytes",
        RetainedMetricPolicy::NodeBearing,
    ),
    (
        "clean_import.canonical_nodes.allocated_bytes",
        "/storage/clean_import/categories/topology_nodes/allocated_bytes",
        RetainedMetricPolicy::NodeFilesystemQuantized,
    ),
    (
        "clean_import.canonical_edges.logical_bytes",
        "/storage/clean_import/categories/topology_edges/logical_bytes",
        RetainedMetricPolicy::EdgeBearing,
    ),
    (
        "clean_import.canonical_edges.physical_logical_bytes",
        "/storage/clean_import/categories/topology_edges/physical_logical_bytes",
        RetainedMetricPolicy::EdgeBearing,
    ),
    (
        "clean_import.canonical_edges.allocated_bytes",
        "/storage/clean_import/categories/topology_edges/allocated_bytes",
        RetainedMetricPolicy::EdgeFilesystemQuantized,
    ),
    (
        "portable_package.logical_references",
        "/storage/portable_package/logical_references",
        RetainedMetricPolicy::InventoryDerived,
    ),
    (
        "portable_package.logical_bytes",
        "/storage/portable_package/logical_bytes",
        RetainedMetricPolicy::ScaleBearing,
    ),
    (
        "portable_package.physical_objects",
        "/storage/portable_package/physical_objects",
        RetainedMetricPolicy::InventoryDerived,
    ),
    (
        "portable_package.allocated_bytes",
        "/storage/portable_package/allocated_bytes",
        RetainedMetricPolicy::FilesystemQuantized,
    ),
    (
        "construction.canonical_output_bytes",
        "/storage/construction/canonical_output_bytes",
        RetainedMetricPolicy::ScaleBearing,
    ),
    (
        "construction.staged_and_retained_disk_bytes",
        "/storage/construction/staged_and_retained_disk_bytes",
        RetainedMetricPolicy::ScaleBearing,
    ),
    (
        "construction.transient_peak_total_allocated_bytes",
        "/storage/construction/storage_transient_peak_total_allocated_bytes",
        RetainedMetricPolicy::FilesystemQuantized,
    ),
    (
        "source_project_current_allocated_bytes",
        "/storage/source_project_current_allocated_bytes",
        RetainedMetricPolicy::FilesystemQuantized,
    ),
    (
        "workspace_current_allocated_bytes",
        "/storage/workspace_current_allocated_bytes",
        RetainedMetricPolicy::FilesystemQuantized,
    ),
    (
        "workspace_peak_allocated_bytes",
        "/storage/workspace_peak_allocated_bytes",
        RetainedMetricPolicy::FilesystemQuantized,
    ),
    (
        "construction.current_merge_temporary_allocated_bytes",
        "/storage/construction/current_merge_temporary_allocated_bytes",
        RetainedMetricPolicy::FilesystemQuantized,
    ),
];

#[derive(Clone, Debug, PartialEq, Eq)]
struct LifecycleLinearityObservation {
    phases: BTreeMap<String, [u64; LINEARITY_PHASE_FIELDS.len()]>,
    retained: BTreeMap<String, u64>,
    append_objects: u64,
    input_rows: u64,
    live_nodes: u64,
    live_edges: u64,
    shape_merge_bytes: [u64; 2],
    shape_block_components: [u64; 2],
    canonical_artifact_objects: u64,
    cas_publication_io: graphforge_storage::GraphPublicationIo,
    encode_fsync_components: [u64; 4],
    hydration_files_copied: u64,
    hydration_file_fsync_operations: u64,
    hydration_directory_fsync_operations: u64,
    shape_read_component_calls: [u64; 6],
    shape_write_component_calls: [u64; 2],
    encode_write_component_calls: [u64; 5],
    category_metrics: BTreeMap<String, [u64; 6]>,
    category_authority_metrics: BTreeMap<String, [u64; 6]>,
    phase_disk_peaks: BTreeMap<String, u64>,
}

const fn category_name(category: graphforge_storage::ArtifactCategory) -> &'static str {
    use graphforge_storage::ArtifactCategory;
    match category {
        ArtifactCategory::TopologyNodes => "topology_nodes",
        ArtifactCategory::TopologyEdges => "topology_edges",
        ArtifactCategory::Properties => "properties",
        ArtifactCategory::UuidAndSurrogates => "uuid_and_surrogates",
        ArtifactCategory::Adjacency => "adjacency",
        ArtifactCategory::CatalogAndManifests => "catalog_and_manifests",
        ArtifactCategory::ConstructionStaging => "construction_staging",
        ArtifactCategory::PortablePackage => "portable_package",
        ArtifactCategory::CleanImportedProject => "clean_imported_project",
        ArtifactCategory::Other => "other",
    }
}

fn artifact_fields(totals: &graphforge_storage::ArtifactStorageTotals) -> [u64; 5] {
    let graphforge_storage::ArtifactStorageTotals {
        logical_references,
        logical_bytes,
        physical_objects,
        physical_logical_bytes,
        allocated_bytes,
    } = totals;
    [
        *logical_references,
        *logical_bytes,
        *physical_objects,
        *physical_logical_bytes,
        *allocated_bytes,
    ]
}

fn exact_category_map(
    value: Value,
    owner: &str,
) -> BTreeMap<graphforge_storage::ArtifactCategory, graphforge_storage::ArtifactStorageTotals> {
    let categories: BTreeMap<_, _> =
        serde_json::from_value(value).unwrap_or_else(|error| panic!("{owner}: {error}"));
    let observed = categories.keys().copied().collect::<BTreeSet<_>>();
    let expected = graphforge_storage::ArtifactCategory::ALL
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert_eq!(observed, expected, "{owner} category inventory");
    categories
}

const fn phase_name(phase: graphforge_storage::StorageIoPhase) -> &'static str {
    use graphforge_storage::StorageIoPhase;
    match phase {
        StorageIoPhase::AppendMerge => "append_merge",
        StorageIoPhase::SealAuthentication => "seal_authentication",
        StorageIoPhase::ShapeConsumeReauthentication => "shape_consume_reauthentication",
        StorageIoPhase::EncodeWritePostwriteAuthentication => {
            "encode_write_postwrite_authentication"
        }
        StorageIoPhase::PublicationPreauthentication => "publication_preauthentication",
        StorageIoPhase::CasInstallReadWrite => "cas_install_read_write",
        StorageIoPhase::HydrationVerification => "hydration_verification",
        StorageIoPhase::FsyncSynchronization => "fsync_synchronization",
        StorageIoPhase::RecoveryReauthentication => "recovery_reauthentication",
    }
}

fn phase_fields(totals: &graphforge_storage::PhaseIoTotals) -> [u64; LINEARITY_PHASE_FIELDS.len()] {
    let graphforge_storage::PhaseIoTotals {
        read_bytes,
        write_bytes,
        read_calls,
        write_calls,
        object_count,
        block_count,
        fsync_calls,
    } = totals;
    [
        *read_bytes,
        *write_bytes,
        *read_calls,
        *write_calls,
        *object_count,
        *block_count,
        *fsync_calls,
    ]
}

fn lifecycle_linearity_observation(evidence: &Value) -> LifecycleLinearityObservation {
    let attribution: graphforge_storage::ConstructionPhaseAttribution =
        serde_json::from_value(evidence["storage"]["application_io_phases"].clone())
            .expect("typed phase evidence");
    let phases = attribution
        .phases
        .iter()
        .map(|(phase, totals)| (phase_name(*phase).to_owned(), phase_fields(totals)))
        .collect();
    let retained = LINEARITY_RETAINED_FIELDS
        .iter()
        .map(|(name, pointer, _)| {
            (
                (*name).to_owned(),
                evidence
                    .pointer(pointer)
                    .and_then(Value::as_u64)
                    .unwrap_or_else(|| panic!("retained metric {name} is an integer")),
            )
        })
        .collect();
    let append_objects = evidence["storage"]["construction"]["input_batches"]
        .as_u64()
        .expect("construction input batches are an integer");
    let input_rows = evidence["storage"]["construction"]["input_rows"]
        .as_u64()
        .expect("construction input rows are an integer");
    let live_nodes = evidence["source_nodes"]
        .as_u64()
        .expect("authoritative live node count");
    let live_edges = evidence["source_edges"]
        .as_u64()
        .expect("authoritative live edge count");
    let construction = &evidence["storage"]["construction"];
    let merge_read_bytes = construction["merge_read_bytes"]
        .as_u64()
        .expect("merge read bytes");
    let merge_write_bytes = construction["merge_written_bytes"]
        .as_u64()
        .expect("merge write bytes");
    let shape_block_components = [
        construction["merge_read_blocks"]
            .as_u64()
            .expect("merge read block authority"),
        construction["merge_write_blocks"]
            .as_u64()
            .expect("merge write block authority"),
    ];
    let canonical_artifact_objects = construction["canonical_artifact_objects"]
        .as_u64()
        .expect("canonical artifact inventory");
    let encode_fsync_components = [
        construction["encode_output_fsync_operations"]
            .as_u64()
            .expect("encode output fsyncs"),
        construction["encode_source_spool_fsync_operations"]
            .as_u64()
            .expect("encode spool fsyncs"),
        construction["encode_membership_fsync_operations"]
            .as_u64()
            .expect("encode membership fsyncs"),
        construction["encode_ordinal_fsync_operations"]
            .as_u64()
            .expect("encode ordinal fsyncs"),
    ];
    let hydration_files_copied = construction["hydration_files_copied"]
        .as_u64()
        .expect("hydration copied-file inventory");
    let hydration_file_fsync_operations = construction["hydration_file_fsync_operations"]
        .as_u64()
        .expect("hydration file barrier inventory");
    let hydration_directory_fsync_operations = construction["hydration_directory_fsync_operations"]
        .as_u64()
        .expect("hydration directory barrier inventory");
    let shape_read_component_calls = [
        construction["shape_input_validation_read_operations"]
            .as_u64()
            .expect("shape input reads"),
        construction["merge_read_operations"]
            .as_u64()
            .expect("merge reads"),
        construction["parquet_read_operations"]
            .as_u64()
            .expect("Parquet reads"),
        construction["shaped_output_authentication_operations"]
            .as_u64()
            .expect("shape authentication reads"),
        construction["parent_catalog_read_operations"]
            .as_u64()
            .expect("parent catalog reads"),
        construction["retained_probe_block_loads"]
            .as_u64()
            .expect("retained probe reads"),
    ];
    let shape_write_component_calls = [
        construction["merge_write_operations"]
            .as_u64()
            .expect("merge writes"),
        construction["parquet_write_operations"]
            .as_u64()
            .expect("Parquet writes"),
    ];
    let encode_write_component_calls = [
        construction["encode_output_write_operations"]
            .as_u64()
            .expect("encode output writes"),
        construction["encode_membership_write_operations"]
            .as_u64()
            .expect("encode membership writes"),
        construction["encode_source_spool_write_operations"]
            .as_u64()
            .expect("encode spool writes"),
        construction["encode_ordinal_artifact_write_operations"]
            .as_u64()
            .expect("ordinal artifact writes"),
        construction["encode_ordinal_publication_write_operations"]
            .as_u64()
            .expect("ordinal publication writes"),
    ];
    let mut category_metrics = BTreeMap::new();
    let mut category_authority_metrics = BTreeMap::new();
    for owner in ["source", "clean_import"] {
        let categories =
            exact_category_map(evidence["storage"][owner]["categories"].clone(), owner);
        let authorities = exact_category_map(
            evidence["storage"][owner]["category_authorities"].clone(),
            &format!("{owner} authorities"),
        );
        for category in graphforge_storage::ArtifactCategory::ALL {
            let fields = artifact_fields(&categories[&category]);
            let authority = artifact_fields(&authorities[&category]);
            category_metrics.insert(
                format!("{owner}.{}", category_name(category)),
                [
                    fields[0], fields[1], fields[2], fields[3], fields[4], fields[4],
                ],
            );
            category_authority_metrics.insert(
                format!("{owner}.{}", category_name(category)),
                [
                    authority[0],
                    authority[1],
                    authority[2],
                    authority[3],
                    authority[4],
                    authority[4],
                ],
            );
        }
    }
    let construction_categories = exact_category_map(
        construction["storage_current"].clone(),
        "construction current",
    );
    let construction_peaks: BTreeMap<graphforge_storage::ArtifactCategory, u64> =
        serde_json::from_value(construction["storage_transient_peak_allocated_bytes"].clone())
            .expect("typed construction peak categories");
    let construction_authorities = exact_category_map(
        construction["storage_category_authorities"].clone(),
        "construction authorities",
    );
    let construction_peak_authorities: BTreeMap<graphforge_storage::ArtifactCategory, u64> =
        serde_json::from_value(construction["storage_transient_peak_authorities"].clone())
            .expect("typed construction peak authorities");
    assert_eq!(
        construction_peaks.keys().copied().collect::<BTreeSet<_>>(),
        graphforge_storage::ArtifactCategory::ALL
            .into_iter()
            .collect::<BTreeSet<_>>(),
        "construction peak category inventory"
    );
    for category in graphforge_storage::ArtifactCategory::ALL {
        let fields = artifact_fields(&construction_categories[&category]);
        category_metrics.insert(
            format!("construction.{}", category_name(category)),
            [
                fields[0],
                fields[1],
                fields[2],
                fields[3],
                fields[4],
                construction_peaks[&category],
            ],
        );
        let authority = artifact_fields(&construction_authorities[&category]);
        category_authority_metrics.insert(
            format!("construction.{}", category_name(category)),
            [
                authority[0],
                authority[1],
                authority[2],
                authority[3],
                authority[4],
                construction_peak_authorities[&category],
            ],
        );
    }
    let phase_disk_peaks = evidence["phases"]
        .as_array()
        .expect("lifecycle phase evidence")
        .iter()
        .map(|phase| {
            (
                phase["id"].as_str().expect("phase id").to_owned(),
                phase["disk_peak_bytes"].as_u64().expect("phase disk peak"),
            )
        })
        .collect();
    LifecycleLinearityObservation {
        phases,
        retained,
        append_objects,
        input_rows,
        live_nodes,
        live_edges,
        shape_merge_bytes: [merge_read_bytes, merge_write_bytes],
        shape_block_components,
        canonical_artifact_objects,
        cas_publication_io: serde_json::from_value(construction["cas_publication_io"].clone())
            .expect("native CAS component evidence"),
        encode_fsync_components,
        hydration_files_copied,
        hydration_file_fsync_operations,
        hydration_directory_fsync_operations,
        shape_read_component_calls,
        shape_write_component_calls,
        encode_write_component_calls,
        category_metrics,
        category_authority_metrics,
        phase_disk_peaks,
    }
}

const ATTRIBUTION_CATEGORY_NAMES: [&str; 10] = [
    "topology_nodes",
    "topology_edges",
    "properties",
    "uuid_and_surrogates",
    "adjacency",
    "catalog_and_manifests",
    "construction_staging",
    "portable_package",
    "clean_imported_project",
    "other",
];
const ATTRIBUTION_TOTAL_FIELDS: [(&str, &str); 5] = [
    ("logical_references", "logical_references"),
    ("logical_bytes", "logical_bytes"),
    ("physical_objects", "physical_objects"),
    ("physical_logical_bytes", "physical_logical_bytes"),
    ("allocated_bytes", "allocated_bytes"),
];

fn evidence_u64(value: &Value, pointer: &str) -> Result<u64, String> {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("missing integer storage evidence at {pointer}"))
}

fn validate_category_authority_context(
    context: &graphforge_storage::ArtifactCategoryAuthorityContext,
    owner: &str,
) -> Result<(), String> {
    let expected_categories = graphforge_storage::ArtifactCategory::ALL
        .into_iter()
        .collect::<BTreeSet<_>>();
    if context.contract != CATEGORY_AUTHORITY_CONTRACT
        || context.version != 1
        || context.owner != owner
        || context.rung == 0
        || context.live_nodes == 0
        || context.live_edges == 0
        || context
            .native_category_identity_authority_sha256
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
            != expected_categories
        || [
            context.generation_sha256.as_str(),
            context.receipt_authority_sha256.as_str(),
            context.native_identity_authority_sha256.as_str(),
        ]
        .into_iter()
        .chain(
            context
                .native_category_identity_authority_sha256
                .values()
                .map(String::as_str),
        )
        .any(|digest| {
            digest.len() != 71
                || !digest.starts_with("sha256:")
                || !digest[7..].bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    {
        return Err(format!("{owner} category authority context is invalid"));
    }
    Ok(())
}

fn validate_attribution_owner(evidence: &Value, owner: &str) -> Result<(), String> {
    let owner_value = evidence
        .pointer(&format!("/storage/{owner}"))
        .ok_or_else(|| format!("missing storage owner {owner}"))?;
    let categories = owner_value["categories"]
        .as_object()
        .ok_or_else(|| format!("{owner} categories are absent"))?;
    let authorities = owner_value["category_authorities"]
        .as_object()
        .ok_or_else(|| format!("{owner} category authorities are absent"))?;
    if categories != authorities {
        return Err(format!(
            "{owner} category totals differ from storage-owned authorities"
        ));
    }
    let typed_authorities = exact_category_map(
        owner_value["category_authorities"].clone(),
        &format!("{owner} category authorities"),
    );
    let commitments = owner_value["category_authority_sha256"]
        .as_object()
        .ok_or_else(|| format!("{owner} category commitments are absent"))?;
    if commitments.len() != graphforge_storage::ArtifactCategory::ALL.len() {
        return Err(format!(
            "{owner} category commitment inventory is incomplete"
        ));
    }
    let context: graphforge_storage::ArtifactCategoryAuthorityContext =
        serde_json::from_value(owner_value["category_authority_context"].clone())
            .map_err(|error| format!("{owner} category authority context: {error}"))?;
    validate_category_authority_context(&context, owner)?;
    for category in graphforge_storage::ArtifactCategory::ALL {
        let name = category_name(category);
        let expected = graphforge_storage::artifact_category_authority_commitment(
            &context,
            category,
            &typed_authorities[&category],
        );
        if commitments.get(name).and_then(Value::as_str) != Some(expected.as_str()) {
            return Err(format!(
                "{owner}.{name} differs from its receipt-bound category commitment"
            ));
        }
    }
    let observed = categories
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let expected = ATTRIBUTION_CATEGORY_NAMES
        .into_iter()
        .collect::<BTreeSet<_>>();
    if observed != expected {
        return Err(format!(
            "{owner} category inventory differs: observed={observed:?}"
        ));
    }
    for (category, totals) in categories {
        let fields = totals
            .as_object()
            .ok_or_else(|| format!("{owner}.{category} totals are not an object"))?;
        let observed_fields = fields.keys().map(String::as_str).collect::<BTreeSet<_>>();
        let expected_fields = ATTRIBUTION_TOTAL_FIELDS
            .into_iter()
            .map(|(field, _)| field)
            .collect::<BTreeSet<_>>();
        if observed_fields != expected_fields {
            return Err(format!("{owner}.{category} quantity inventory differs"));
        }
        let references = totals["logical_references"]
            .as_u64()
            .ok_or_else(|| format!("{owner}.{category} references are absent"))?;
        let objects = totals["physical_objects"]
            .as_u64()
            .ok_or_else(|| format!("{owner}.{category} objects are absent"))?;
        if objects > references {
            return Err(format!(
                "{owner}.{category} physical objects exceed logical references"
            ));
        }
    }
    for (category_field, owner_field) in ATTRIBUTION_TOTAL_FIELDS {
        let sum = categories.values().try_fold(0_u64, |sum, category| {
            sum.checked_add(
                category[category_field]
                    .as_u64()
                    .ok_or_else(|| format!("{owner}.{category_field} is absent"))?,
            )
            .ok_or_else(|| format!("{owner}.{category_field} sum overflow"))
        })?;
        let reported = owner_value[owner_field]
            .as_u64()
            .ok_or_else(|| format!("{owner}.{owner_field} is absent"))?;
        if sum != reported {
            return Err(format!(
                "{owner}.{owner_field} does not reconcile: categories={sum} reported={reported}"
            ));
        }
    }
    Ok(())
}

// This validates closed sanitized reconciliation, not provenance. Certification
// authority is established outside this process by the provider-result digest
// that binds the exact ordinary evidence bytes.
fn validate_retained_reconciliation(evidence: &Value) -> Result<(), String> {
    validate_attribution_owner(evidence, "source")?;
    validate_attribution_owner(evidence, "clean_import")?;
    let live_nodes = evidence_u64(evidence, "/source_nodes")?;
    let live_edges = evidence_u64(evidence, "/source_edges")?;
    if live_nodes == 0 || live_edges == 0 {
        return Err("retained evidence has an invalid live-count denominator".into());
    }
    for category in ATTRIBUTION_CATEGORY_NAMES {
        for (field, _) in ATTRIBUTION_TOTAL_FIELDS {
            let source = evidence_u64(
                evidence,
                &format!("/storage/source/categories/{category}/{field}"),
            )?;
            let imported = evidence_u64(
                evidence,
                &format!("/storage/clean_import/categories/{category}/{field}"),
            )?;
            if source != imported {
                return Err(format!(
                    "round-trip category {category}.{field} differs between source and import"
                ));
            }
        }
    }
    for owner in ["source", "clean_import"] {
        let node_bytes = evidence_u64(
            evidence,
            &format!("/storage/{owner}/categories/topology_nodes/logical_bytes"),
        )?;
        let edge_bytes = evidence_u64(
            evidence,
            &format!("/storage/{owner}/categories/topology_edges/logical_bytes"),
        )?;
        if node_bytes < live_nodes || edge_bytes < live_edges {
            return Err(format!(
                "{owner} canonical category bytes are below live-count denominators"
            ));
        }
        let topology_rows = live_nodes
            .checked_add(live_edges)
            .ok_or_else(|| "live topology denominator overflow".to_owned())?;
        for category in ["uuid_and_surrogates", "adjacency"] {
            let logical_bytes = evidence_u64(
                evidence,
                &format!("/storage/{owner}/categories/{category}/logical_bytes"),
            )?;
            if logical_bytes < topology_rows {
                return Err(format!(
                    "{owner}.{category} is below the topology denominator"
                ));
            }
        }
    }

    let portable_references =
        evidence_u64(evidence, "/storage/portable_package/logical_references")?;
    let portable_objects = evidence_u64(evidence, "/storage/portable_package/physical_objects")?;
    if portable_references == 0 || portable_references != portable_objects {
        return Err("portable receipt inventory does not reconcile".into());
    }
    let portable_authority = &evidence["storage"]["portable_package"]["category_authority"];
    for field in [
        "logical_references",
        "logical_bytes",
        "physical_objects",
        "allocated_bytes",
    ] {
        if portable_authority[field] != evidence["storage"]["portable_package"][field] {
            return Err(format!(
                "portable {field} differs from writer-owned category authority"
            ));
        }
    }
    let typed_portable_authority: graphforge_storage::ArtifactStorageTotals =
        serde_json::from_value(portable_authority.clone())
            .map_err(|error| format!("portable category authority: {error}"))?;
    let portable_context: graphforge_storage::ArtifactCategoryAuthorityContext =
        serde_json::from_value(
            evidence["storage"]["portable_package"]["category_authority_context"].clone(),
        )
        .map_err(|error| format!("portable category authority context: {error}"))?;
    validate_category_authority_context(&portable_context, "portable_package")?;
    let expected_portable_commitment = graphforge_storage::artifact_category_authority_commitment(
        &portable_context,
        graphforge_storage::ArtifactCategory::PortablePackage,
        &typed_portable_authority,
    );
    if evidence["storage"]["portable_package"]["category_authority_sha256"].as_str()
        != Some(expected_portable_commitment.as_str())
    {
        return Err("portable package differs from writer authority commitment".into());
    }

    let current = evidence["storage"]["construction"]["storage_current"]
        .as_object()
        .ok_or_else(|| "construction current category evidence is absent".to_owned())?;
    let transient = evidence["storage"]["construction"]["storage_transient_peak_allocated_bytes"]
        .as_object()
        .ok_or_else(|| "construction transient category evidence is absent".to_owned())?;
    let construction_authorities =
        evidence["storage"]["construction"]["storage_category_authorities"]
            .as_object()
            .ok_or_else(|| "construction category authorities are absent".to_owned())?;
    if current != construction_authorities {
        return Err("construction categories differ from receipt/identity authorities".into());
    }
    let peak_authorities =
        evidence["storage"]["construction"]["storage_transient_peak_authorities"]
            .as_object()
            .ok_or_else(|| "construction peak authorities are absent".to_owned())?;
    if transient != peak_authorities {
        return Err("construction peaks differ from receipt authorities".into());
    }
    let typed_construction_authorities = exact_category_map(
        evidence["storage"]["construction"]["storage_category_authorities"].clone(),
        "construction category authorities",
    );
    let typed_peak_authorities: BTreeMap<graphforge_storage::ArtifactCategory, u64> =
        serde_json::from_value(
            evidence["storage"]["construction"]["storage_transient_peak_authorities"].clone(),
        )
        .map_err(|error| format!("construction peak authorities: {error}"))?;
    let category_commitments =
        evidence["storage"]["construction"]["storage_category_authority_sha256"]
            .as_object()
            .ok_or_else(|| "construction category commitments are absent".to_owned())?;
    let peak_commitments =
        evidence["storage"]["construction"]["storage_transient_peak_authority_sha256"]
            .as_object()
            .ok_or_else(|| "construction peak commitments are absent".to_owned())?;
    let construction_context: graphforge_storage::ArtifactCategoryAuthorityContext =
        serde_json::from_value(
            evidence["storage"]["construction"]["storage_category_authority_context"].clone(),
        )
        .map_err(|error| format!("construction category authority context: {error}"))?;
    validate_category_authority_context(&construction_context, "construction")?;
    for category in graphforge_storage::ArtifactCategory::ALL {
        let name = category_name(category);
        let expected_category = graphforge_storage::artifact_category_authority_commitment(
            &construction_context,
            category,
            &typed_construction_authorities[&category],
        );
        let expected_peak = graphforge_storage::artifact_category_peak_authority_commitment(
            &construction_context,
            category,
            typed_peak_authorities[&category],
        );
        if category_commitments.get(name).and_then(Value::as_str)
            != Some(expected_category.as_str())
            || peak_commitments.get(name).and_then(Value::as_str) != Some(expected_peak.as_str())
        {
            return Err(format!(
                "construction.{name} differs from its receipt-bound category commitment"
            ));
        }
    }
    let known_categories = ATTRIBUTION_CATEGORY_NAMES
        .into_iter()
        .collect::<BTreeSet<_>>();
    let current_categories = current.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let transient_categories = transient
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if current_categories != known_categories || transient_categories != known_categories {
        return Err("construction current/transient category inventory differs".into());
    }
    let mut maximum_current = 0_u64;
    for category in ATTRIBUTION_CATEGORY_NAMES {
        let current_totals = &current[category];
        let fields = current_totals
            .as_object()
            .ok_or_else(|| format!("{category} construction totals are not an object"))?;
        let observed_fields = fields.keys().map(String::as_str).collect::<BTreeSet<_>>();
        let expected_fields = ATTRIBUTION_TOTAL_FIELDS
            .into_iter()
            .map(|(field, _)| field)
            .collect::<BTreeSet<_>>();
        if observed_fields != expected_fields {
            return Err(format!(
                "{category} construction quantity inventory differs"
            ));
        }
        let current_allocated = current_totals["allocated_bytes"]
            .as_u64()
            .ok_or_else(|| format!("{category} construction allocation is absent"))?;
        let references = current_totals["logical_references"]
            .as_u64()
            .ok_or_else(|| format!("{category} construction references are absent"))?;
        let objects = current_totals["physical_objects"]
            .as_u64()
            .ok_or_else(|| format!("{category} construction objects are absent"))?;
        if objects > references {
            return Err(format!("{category} construction objects exceed references"));
        }
        let category_peak = transient[category]
            .as_u64()
            .ok_or_else(|| format!("{category} transient peak is absent"))?;
        if category_peak < current_allocated {
            return Err(format!(
                "{category} transient allocation is below current allocation"
            ));
        }
        maximum_current = maximum_current.max(current_allocated);
    }
    let total_peak = evidence_u64(
        evidence,
        "/storage/construction/storage_transient_peak_total_allocated_bytes",
    )?;
    if total_peak < maximum_current {
        return Err("construction total peak is below a current category".into());
    }

    let source_allocated = evidence_u64(evidence, "/storage/source/allocated_bytes")?;
    let imported_allocated = evidence_u64(evidence, "/storage/clean_import/allocated_bytes")?;
    let package_allocated = evidence_u64(evidence, "/storage/portable_package/allocated_bytes")?;
    let source_project = evidence_u64(evidence, "/storage/source_project_current_allocated_bytes")?;
    if source_project < source_allocated {
        return Err("source project allocation is below its generation snapshot".into());
    }
    let workspace = evidence_u64(evidence, "/storage/workspace_current_allocated_bytes")?;
    let components = evidence["storage"]["workspace_components"]
        .as_object()
        .ok_or_else(|| "workspace component unions are absent".to_owned())?;
    let expected_components = [
        "source_project_and_construction",
        "portable_package",
        "clean_import_project",
        "drill_project_and_construction",
        "drill_package",
        "corrupt_drill_package",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if components
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != expected_components
    {
        return Err("workspace component-union inventory differs".into());
    }
    let component_sum = components.values().try_fold(0_u64, |sum, value| {
        sum.checked_add(
            value
                .as_u64()
                .ok_or_else(|| "workspace component is not an integer".to_owned())?,
        )
        .ok_or_else(|| "workspace component sum overflow".to_owned())
    })?;
    if component_sum != workspace {
        return Err("workspace authoritative identity unions do not reconcile".into());
    }
    if components["portable_package"].as_u64() != Some(package_allocated) {
        return Err("portable owner union differs from its writer receipt".into());
    }
    if components["source_project_and_construction"]
        .as_u64()
        .is_none_or(|value| value < source_project)
        || components["clean_import_project"]
            .as_u64()
            .is_none_or(|value| value < imported_allocated)
    {
        return Err("project owner union is below its generation attribution".into());
    }
    let workspace_peak = evidence_u64(evidence, "/storage/workspace_peak_allocated_bytes")?;
    let phase_peak = evidence["phases"]
        .as_array()
        .ok_or_else(|| "phase peak evidence is absent".to_owned())?
        .iter()
        .map(|phase| phase["disk_peak_bytes"].as_u64())
        .collect::<Option<Vec<_>>>()
        .and_then(|values| values.into_iter().max())
        .ok_or_else(|| "phase disk peak is absent".to_owned())?;
    if workspace_peak != phase_peak || workspace_peak < workspace {
        return Err("lifecycle peak does not reconcile to phase peaks/current union".into());
    }
    Ok(())
}

fn validate_axis_denominators(
    axis: LinearityAxis,
    observations: &[LifecycleLinearityObservation; 3],
) -> Result<[u64; 3], String> {
    let nodes = std::array::from_fn(|index| observations[index].live_nodes);
    let edges = std::array::from_fn(|index| observations[index].live_edges);
    if nodes.contains(&0) || edges.contains(&0) {
        return Err(format!(
            "{axis:?} has a zero authoritative denominator: nodes={nodes:?} edges={edges:?}"
        ));
    }
    let (varying, controlled, varying_name, controlled_name) = match axis {
        LinearityAxis::Nodes => (nodes, edges, "nodes", "edges"),
        LinearityAxis::Edges => (edges, nodes, "edges", "nodes"),
    };
    if varying[0] >= varying[1] || varying[1] >= varying[2] {
        return Err(format!(
            "{axis:?} {varying_name} denominators are not strictly increasing: {varying:?}"
        ));
    }
    if controlled[0] != controlled[1] || controlled[0] != controlled[2] {
        return Err(format!(
            "{axis:?} controlled {controlled_name} axis changed: {controlled:?}"
        ));
    }
    Ok(varying)
}

fn checked_product(name: &str, factors: &[u128]) -> Result<u128, String> {
    factors.iter().try_fold(1_u128, |product, factor| {
        product
            .checked_mul(*factor)
            .ok_or_else(|| format!("{name} rational product overflows"))
    })
}

fn checked_ceil_div(name: &str, numerator: u64, denominator: u64) -> Result<u64, String> {
    if denominator == 0 {
        return Err(format!("{name} has a zero quantization denominator"));
    }
    (numerator / denominator)
        .checked_add(u64::from(numerator % denominator != 0))
        .ok_or_else(|| format!("{name} ceil division overflows"))
}

/// Prove `fixed + slope * authoritative_denominator` without rounded ratios.
///
/// Cross multiplication compares the two adjacent rational slopes within a
/// factor of two. The intercept and legacy constant-factor ceiling are also
/// checked against the actual reopened live-count denominators.
fn validate_affine_metric(
    name: &str,
    values: [u64; 3],
    denominators: [u64; 3],
) -> Result<(), String> {
    let [one, two, four] = values;
    if one == 0 {
        return Err(format!(
            "{name} designated scale-bearing metric is zero at its baseline: {values:?}"
        ));
    }
    let base_denominator = u128::from(denominators[0]);
    for rung in 1..3 {
        let observed = checked_product(name, &[u128::from(values[rung]), base_denominator])?;
        let ceiling = checked_product(name, &[u128::from(one), u128::from(denominators[rung]), 2])?;
        if observed > ceiling {
            return Err(format!(
                "{name} exceeds the denominator-normalized constant-factor ceiling: values={values:?} denominators={denominators:?}"
            ));
        }
    }
    let d12 = two
        .checked_sub(one)
        .filter(|difference| *difference > 0)
        .ok_or_else(|| format!("{name} has no positive 1x-to-2x growth: {values:?}"))?;
    let d24 = four
        .checked_sub(two)
        .filter(|difference| *difference > 0)
        .ok_or_else(|| format!("{name} has no positive 2x-to-4x growth: {values:?}"))?;
    let denominator_12 = denominators[1] - denominators[0];
    let denominator_24 = denominators[2] - denominators[1];
    if checked_product(name, &[u128::from(d12), base_denominator])?
        > checked_product(name, &[u128::from(one), u128::from(denominator_12)])?
    {
        return Err(format!(
            "{name} implies a negative fixed-overhead intercept: values={values:?} denominators={denominators:?}"
        ));
    }
    let first_slope = checked_product(name, &[u128::from(d12), u128::from(denominator_24)])?;
    let second_slope = checked_product(name, &[u128::from(d24), u128::from(denominator_12)])?;
    if first_slope > checked_product(name, &[second_slope, 2])?
        || second_slope > checked_product(name, &[first_slope, 2])?
    {
        return Err(format!(
            "{name} has unstable rational first-difference slopes: values={values:?} denominators={denominators:?}"
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PhaseMetricPolicy {
    ScaleBearing,
    NodeBearingBytes,
    BufferedCalls {
        byte_field: usize,
        max_bytes_per_call: u64,
    },
    InventoryControlBytes {
        maximum: u64,
    },
    ShapeReadComponentCalls,
    ShapeWriteComponentCalls,
    EncodeWriteComponentCalls,
    AppendObjectInventory,
    ShapeBlockInventory,
    CasFsyncInventory,
    CasReadComponentCalls,
    CasWriteComponentCalls,
    EncodeFsyncInventory,
    HydrationFsyncInventory,
    StructurallyZero,
}

#[derive(Clone, Copy)]
struct PhasePolicyRow {
    phase: &'static str,
    fields: [PhaseMetricPolicy; LINEARITY_PHASE_FIELDS.len()],
}

const ZERO: PhaseMetricPolicy = PhaseMetricPolicy::StructurallyZero;
const SCALE: PhaseMetricPolicy = PhaseMetricPolicy::ScaleBearing;
const ENCODING_BUFFER_BYTES: u64 =
    graphforge_storage::GRAPH_CONSTRUCTION_ENCODING_BUFFER_BYTES as u64;
const OBJECT_BUFFER_BYTES: u64 = graphforge_storage::GRAPH_OBJECT_IO_BUFFER_BYTES as u64;
const HYDRATION_BUFFER_BYTES: u64 = graphforge_storage::GRAPH_FILES_IO_BUFFER_BYTES as u64;
const STAGING_BUFFER_BYTES: u64 = graphforge_storage::STAGE_FILE_BLOCK_BYTES as u64;

// This is deliberately exhaustive: adding a storage phase or counter requires
// choosing semantics here instead of silently inheriting an affine assertion.
const PHASE_METRIC_POLICIES: [PhasePolicyRow; 9] = [
    PhasePolicyRow {
        phase: "append_merge",
        fields: [
            ZERO,
            SCALE,
            ZERO,
            PhaseMetricPolicy::BufferedCalls {
                byte_field: 1,
                max_bytes_per_call: STAGING_BUFFER_BYTES,
            },
            PhaseMetricPolicy::AppendObjectInventory,
            ZERO,
            ZERO,
        ],
    },
    PhasePolicyRow {
        phase: "seal_authentication",
        fields: [ZERO, ZERO, ZERO, ZERO, ZERO, ZERO, ZERO],
    },
    PhasePolicyRow {
        phase: "shape_consume_reauthentication",
        fields: [
            SCALE,
            SCALE,
            PhaseMetricPolicy::ShapeReadComponentCalls,
            PhaseMetricPolicy::ShapeWriteComponentCalls,
            ZERO,
            PhaseMetricPolicy::ShapeBlockInventory,
            ZERO,
        ],
    },
    PhasePolicyRow {
        phase: "encode_write_postwrite_authentication",
        fields: [
            SCALE,
            SCALE,
            PhaseMetricPolicy::BufferedCalls {
                byte_field: 0,
                max_bytes_per_call: ENCODING_BUFFER_BYTES,
            },
            PhaseMetricPolicy::EncodeWriteComponentCalls,
            ZERO,
            ZERO,
            PhaseMetricPolicy::EncodeFsyncInventory,
        ],
    },
    PhasePolicyRow {
        phase: "publication_preauthentication",
        fields: [
            PhaseMetricPolicy::InventoryControlBytes {
                maximum: ENCODING_BUFFER_BYTES,
            },
            ZERO,
            PhaseMetricPolicy::BufferedCalls {
                byte_field: 0,
                max_bytes_per_call: ENCODING_BUFFER_BYTES,
            },
            ZERO,
            ZERO,
            ZERO,
            ZERO,
        ],
    },
    PhasePolicyRow {
        phase: "cas_install_read_write",
        fields: [
            SCALE,
            SCALE,
            PhaseMetricPolicy::CasReadComponentCalls,
            PhaseMetricPolicy::CasWriteComponentCalls,
            ZERO,
            ZERO,
            PhaseMetricPolicy::CasFsyncInventory,
        ],
    },
    PhasePolicyRow {
        phase: "hydration_verification",
        fields: [
            SCALE,
            PhaseMetricPolicy::NodeBearingBytes,
            PhaseMetricPolicy::BufferedCalls {
                byte_field: 0,
                max_bytes_per_call: HYDRATION_BUFFER_BYTES,
            },
            PhaseMetricPolicy::BufferedCalls {
                byte_field: 1,
                max_bytes_per_call: HYDRATION_BUFFER_BYTES,
            },
            ZERO,
            ZERO,
            PhaseMetricPolicy::HydrationFsyncInventory,
        ],
    },
    PhasePolicyRow {
        phase: "fsync_synchronization",
        fields: [ZERO, ZERO, ZERO, ZERO, ZERO, ZERO, SCALE],
    },
    PhasePolicyRow {
        phase: "recovery_reauthentication",
        fields: [
            SCALE,
            ZERO,
            PhaseMetricPolicy::BufferedCalls {
                byte_field: 0,
                max_bytes_per_call: ENCODING_BUFFER_BYTES,
            },
            ZERO,
            ZERO,
            ZERO,
            ZERO,
        ],
    },
];

fn validate_phase_policy_table(
    observed: &BTreeMap<String, [u64; LINEARITY_PHASE_FIELDS.len()]>,
) -> Result<(), String> {
    let policy_names = PHASE_METRIC_POLICIES
        .iter()
        .map(|row| row.phase)
        .collect::<BTreeSet<_>>();
    let observed_names = observed.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if policy_names.len() != PHASE_METRIC_POLICIES.len() || policy_names != observed_names {
        return Err(format!(
            "phase policy inventory differs from evidence: policy={policy_names:?} observed={observed_names:?}"
        ));
    }
    Ok(())
}

fn validate_fixed_protocol_metric(
    name: &str,
    values: [u64; 3],
    minimum: u64,
    maximum: u64,
) -> Result<(), String> {
    if minimum == 0 || minimum > maximum {
        return Err(format!("{name} has an invalid fixed-protocol policy"));
    }
    if values.windows(2).any(|pair| pair[1] < pair[0]) {
        return Err(format!("{name} fixed protocol count decreased: {values:?}"));
    }
    if values
        .iter()
        .any(|value| !(minimum..=maximum).contains(value))
    {
        return Err(format!(
            "{name} is outside fixed protocol bounds {minimum}..={maximum}: {values:?}"
        ));
    }
    Ok(())
}

fn validate_buffered_calls(
    name: &str,
    calls: [u64; 3],
    bytes: [u64; 3],
    max_bytes_per_call: u64,
) -> Result<(), String> {
    if calls.windows(2).any(|pair| pair[1] < pair[0]) {
        return Err(format!("{name} buffered calls decreased: {calls:?}"));
    }
    for (rung, (calls, bytes)) in calls.into_iter().zip(bytes).enumerate() {
        if bytes == 0 || calls == 0 {
            return Err(format!("{name} rung {rung} lacks paired buffered evidence"));
        }
        let minimum_calls = checked_ceil_div(name, bytes, max_bytes_per_call)?;
        if calls < minimum_calls || calls > bytes {
            return Err(format!(
                "{name} rung {rung} violates buffered bounds {minimum_calls}..={bytes}: calls={calls}"
            ));
        }
    }
    Ok(())
}

fn validate_shape_block_inventory(
    name: &str,
    observed: u64,
    observation: &LifecycleLinearityObservation,
    rung: usize,
) -> Result<(), String> {
    let expected = observation
        .shape_block_components
        .into_iter()
        .try_fold(0_u64, u64::checked_add)
        .ok_or_else(|| format!("{name} native component sum overflows at rung {rung}"))?;
    if observed != expected {
        return Err(format!(
            "{name} does not reconcile to merge_read_blocks + merge_write_blocks at rung {rung}: observed={observed} expected={expected}"
        ));
    }
    for (direction, blocks, bytes) in [
        (
            "read",
            observation.shape_block_components[0],
            observation.shape_merge_bytes[0],
        ),
        (
            "write",
            observation.shape_block_components[1],
            observation.shape_merge_bytes[1],
        ),
    ] {
        if bytes == 0 {
            if blocks != 0 {
                return Err(format!(
                    "{name} {direction} blocks lack merge bytes at rung {rung}"
                ));
            }
            continue;
        }
        // Production accumulates ceil(artifact_bytes / block_bytes) for every
        // authenticated artifact. Therefore ceil(total_bytes / block_bytes) is
        // a strict lower bound, while one-byte blocks are the fail-closed upper
        // bound without exposing per-artifact sizes.
        let minimum = checked_ceil_div(name, bytes, STAGING_BUFFER_BYTES)?;
        if blocks < minimum || blocks > bytes {
            return Err(format!(
                "{name} {direction} component violates per-artifact ceiling bounds {minimum}..={bytes} at rung {rung}: blocks={blocks}"
            ));
        }
    }
    Ok(())
}

fn validate_quantized_allocation(
    name: &str,
    values: [u64; 3],
    denominators: [u64; 3],
) -> Result<(), String> {
    if values[0] == 0 || values.windows(2).any(|pair| pair[1] < pair[0]) {
        return Err(format!(
            "{name} filesystem-quantized allocation is missing or decreasing: {values:?}"
        ));
    }
    validate_positive_normalized_ceiling(name, values, denominators)
}

fn validate_positive_normalized_ceiling(
    name: &str,
    values: [u64; 3],
    denominators: [u64; 3],
) -> Result<(), String> {
    if values.contains(&0) || denominators.contains(&0) {
        return Err(format!("{name} lacks positive values/denominators"));
    }
    for rung in 1..3 {
        let observed = checked_product(
            name,
            &[u128::from(values[rung]), u128::from(denominators[0])],
        )?;
        let ceiling = checked_product(
            name,
            &[u128::from(values[0]), u128::from(denominators[rung]), 2],
        )?;
        if observed > ceiling {
            return Err(format!(
                "{name} exceeds its denominator ceiling: values={values:?} denominators={denominators:?}"
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum CategoryBehavior {
    NodeBearing,
    EdgeBearing,
    Mixed,
    FixedInventory,
    StructurallyZero,
}

fn category_behavior(category: &str) -> Result<CategoryBehavior, String> {
    match category {
        "topology_nodes" => Ok(CategoryBehavior::NodeBearing),
        // The controlled fixture adds isolated nodes on its node axis. Sparse
        // CSR shards do not append trailing rows without edges; its adjacency
        // payload therefore grows on the edge axis. This is fixture-specific.
        "topology_edges" | "adjacency" => Ok(CategoryBehavior::EdgeBearing),
        "uuid_and_surrogates" => Ok(CategoryBehavior::Mixed),
        "catalog_and_manifests" => Ok(CategoryBehavior::FixedInventory),
        "properties"
        | "construction_staging"
        | "portable_package"
        | "clean_imported_project"
        | "other" => Ok(CategoryBehavior::StructurallyZero),
        _ => Err(format!("unclassified storage category {category}")),
    }
}

fn validate_category_taxonomy(
    axis: LinearityAxis,
    observations: &[LifecycleLinearityObservation; 3],
    denominators: [u64; 3],
) -> Result<(), String> {
    let expected = ["source", "clean_import", "construction"]
        .into_iter()
        .flat_map(|owner| {
            ATTRIBUTION_CATEGORY_NAMES
                .into_iter()
                .map(move |category| format!("{owner}.{category}"))
        })
        .collect::<BTreeSet<_>>();
    for observation in observations {
        if observation.category_metrics != observation.category_authority_metrics {
            return Err("retained category totals differ from storage-owned authorities".into());
        }
        let observed = observation
            .category_metrics
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        if observed != expected {
            return Err("retained category/quantity inventory differs from policy".into());
        }
        for category in ATTRIBUTION_CATEGORY_NAMES {
            if observation.category_metrics[&format!("source.{category}")]
                != observation.category_metrics[&format!("clean_import.{category}")]
            {
                return Err(format!(
                    "source/import category {category} does not reconcile field-for-field"
                ));
            }
        }
    }

    for owner in ["source", "clean_import"] {
        for category in ATTRIBUTION_CATEGORY_NAMES {
            let behavior = category_behavior(category)?;
            let key = format!("{owner}.{category}");
            for field in 0..6 {
                let values =
                    std::array::from_fn(|rung| observations[rung].category_metrics[&key][field]);
                let name = format!("{key}.field_{field}");
                match (behavior, field) {
                    (CategoryBehavior::StructurallyZero, _) if values == [0, 0, 0] => {}
                    (CategoryBehavior::StructurallyZero, _) => {
                        return Err(format!("{name} must be structurally zero: {values:?}"));
                    }
                    (CategoryBehavior::FixedInventory, 1 | 3) => {
                        // Catalog shape/object count is fixed; textual numeric
                        // lengths in its JSON manifests can gain digits.
                        validate_positive_normalized_ceiling(&name, values, denominators)?;
                    }
                    (CategoryBehavior::FixedInventory, 4 | 5) => {
                        validate_quantized_allocation(&name, values, denominators)?;
                    }
                    (_, 0 | 2)
                        if values[0] > 0 && values[0] == values[1] && values[0] == values[2] => {}
                    (_, 0 | 2) => {
                        return Err(format!("{name} object inventory changed: {values:?}"));
                    }
                    (CategoryBehavior::NodeBearing, 1 | 3)
                        if matches!(axis, LinearityAxis::Nodes) =>
                    {
                        validate_affine_metric(&name, values, denominators)?;
                    }
                    (CategoryBehavior::EdgeBearing, 1 | 3)
                        if matches!(axis, LinearityAxis::Edges) =>
                    {
                        validate_affine_metric(&name, values, denominators)?;
                    }
                    (CategoryBehavior::Mixed, 1 | 3) => {
                        validate_affine_metric(&name, values, denominators)?;
                    }
                    (CategoryBehavior::NodeBearing, 4 | 5)
                        if matches!(axis, LinearityAxis::Nodes) =>
                    {
                        validate_quantized_allocation(&name, values, denominators)?;
                    }
                    (CategoryBehavior::EdgeBearing, 4 | 5)
                        if matches!(axis, LinearityAxis::Edges) =>
                    {
                        validate_quantized_allocation(&name, values, denominators)?;
                    }
                    (CategoryBehavior::Mixed, 4 | 5) => {
                        validate_quantized_allocation(&name, values, denominators)?;
                    }
                    (_, 1 | 3 | 4 | 5) if values[0] == values[1] && values[0] == values[2] => {}
                    _ => {
                        return Err(format!("{name} changed on its controlled axis: {values:?}"));
                    }
                }
            }
        }
    }

    let staging_key = "construction.construction_staging";
    for field in 0..6 {
        let values =
            std::array::from_fn(|rung| observations[rung].category_metrics[staging_key][field]);
        if field < 4 {
            validate_affine_metric(
                &format!("{staging_key}.field_{field}"),
                values,
                denominators,
            )?;
        } else {
            validate_quantized_allocation(
                &format!("{staging_key}.field_{field}"),
                values,
                denominators,
            )?;
        }
    }
    for category in ATTRIBUTION_CATEGORY_NAMES {
        if category == "construction_staging" {
            continue;
        }
        let key = format!("construction.{category}");
        for rung in observations {
            if rung.category_metrics[&key] != [0; 6] {
                return Err(format!("{key} must be structurally zero"));
            }
        }
    }
    Ok(())
}

fn checked_category_sum(
    observation: &LifecycleLinearityObservation,
    prefix: &str,
    field: usize,
) -> Result<u64, String> {
    observation
        .category_metrics
        .iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .try_fold(0_u64, |total, (_, fields)| {
            total
                .checked_add(fields[field])
                .ok_or_else(|| format!("{prefix} category inventory overflows"))
        })
}

fn validate_fresh_cas_control_bound(
    observation: &LifecycleLinearityObservation,
) -> Result<(), String> {
    let io = &observation.cas_publication_io;
    let paths = observation.canonical_artifact_objects;
    if io.publications != 1 || io.initial_entries != 0 || io.changed_paths != paths {
        return Err("CAS growth proof requires one fresh publication with every changed path in the canonical inventory".into());
    }
    // A radix update consumes at least one of 64 nibbles per recursive branch.
    // A split installs at most three nodes; a collapse can load one extra child.
    // The fresh empty root adds one install before the P updates.
    let installs = paths
        .checked_mul(67)
        .and_then(|value| value.checked_add(1))
        .ok_or("CAS manifest install bound overflows")?;
    let reads = paths
        .checked_mul(130)
        .ok_or("CAS manifest read bound overflows")?;
    let inventory_bytes = observation.phases["publication_preauthentication"][0];
    // Encoding inventory contains each path, byte length and digest verbatim.
    // Manifest entries add at most 43 bytes of keys/role; the 1319-byte maximal
    // branch also bounds the leaf header. Storage tests verify both constants
    // with maximal branches, every role, escaped paths and u64 byte lengths.
    let node_bytes = paths
        .checked_mul(graphforge_storage::GRAPH_MANIFEST_ENTRY_ENCODING_OVERHEAD_BYTES)
        .and_then(|value| value.checked_add(inventory_bytes))
        .and_then(|value| value.checked_add(graphforge_storage::GRAPH_MANIFEST_BRANCH_MAX_BYTES))
        .ok_or("CAS manifest encoded-node bound overflows")?;
    let requests = io
        .manifest
        .installed_objects
        .checked_add(io.manifest.reused_objects)
        .ok_or("CAS manifest request count overflows")?;
    let minimum_requests = paths
        .checked_add(1)
        .ok_or("CAS minimum request bound overflows")?;
    if requests < minimum_requests
        || io.manifest.read_calls < requests
        || io.manifest_reads.read_calls < paths
    {
        return Err("CAS manifest work is below mandatory bootstrap/update authentication".into());
    }
    let install_bytes = installs
        .checked_mul(node_bytes)
        .ok_or("CAS manifest install-byte bound overflows")?;
    let path_bytes = reads
        .checked_mul(node_bytes)
        .ok_or("CAS manifest path-byte bound overflows")?;
    if requests > installs
        || io.manifest.read_bytes > install_bytes
        || io.manifest.write_bytes > install_bytes
        || io.manifest.installed_bytes > io.manifest.write_bytes
        || io.manifest.write_calls != io.manifest.install_attempts
        || io.manifest_reads.read_bytes > path_bytes
    {
        return Err("CAS manifest work exceeds native path/object/encoded-inventory bounds".into());
    }
    for component in [&io.manifest, &io.manifest_reads] {
        // Read may legally return fewer bytes than requested. Keep the physical
        // nonempty-read bounds instead of inventing one syscall per node.
        if component.read_calls < component.read_bytes.div_ceil(OBJECT_BUFFER_BYTES)
            || component.read_calls > component.read_bytes
        {
            return Err("CAS manifest reads violate native buffer bounds".into());
        }
    }
    Ok(())
}

fn validate_lifecycle_metric_policies_for_axis(
    axis: LinearityAxis,
    observations: &[LifecycleLinearityObservation; 3],
) -> Result<(), String> {
    let baseline = &observations[0];
    let denominators = validate_axis_denominators(axis, observations)?;
    validate_phase_policy_table(&baseline.phases)?;
    for observation in &observations[1..] {
        if observation.phases.keys().ne(baseline.phases.keys()) {
            return Err("application-I/O phase inventories differ between rungs".into());
        }
        if observation.retained.keys().ne(baseline.retained.keys()) {
            return Err("retained-output metric inventories differ between rungs".into());
        }
    }
    for (rung, observation) in observations.iter().enumerate() {
        let append_write_bytes = observation.phases["append_merge"][1];
        if append_write_bytes < observation.input_rows {
            return Err(format!(
                "append bytes are below accepted row count at rung {rung}"
            ));
        }
        for (metric, denominator) in [
            (
                "source.canonical_nodes.logical_bytes",
                observation.live_nodes,
            ),
            (
                "clean_import.canonical_nodes.logical_bytes",
                observation.live_nodes,
            ),
            (
                "source.canonical_edges.logical_bytes",
                observation.live_edges,
            ),
            (
                "clean_import.canonical_edges.logical_bytes",
                observation.live_edges,
            ),
        ] {
            if observation.retained[metric] < denominator {
                return Err(format!(
                    "{metric} is below its authoritative live-count denominator at rung {rung}"
                ));
            }
        }
        let topology_rows = observation
            .live_nodes
            .checked_add(observation.live_edges)
            .ok_or_else(|| "live topology denominator overflow".to_owned())?;
        for owner in ["source", "clean_import"] {
            for category in ["uuid_and_surrogates", "adjacency"] {
                let key = format!("{owner}.{category}");
                if observation.category_metrics[&key][1] < topology_rows {
                    return Err(format!(
                        "{key} logical bytes are below the authoritative topology-row denominator at rung {rung}"
                    ));
                }
            }
        }
    }
    for policy_row in PHASE_METRIC_POLICIES {
        let phase = policy_row.phase;
        for (index, (field, policy)) in LINEARITY_PHASE_FIELDS
            .iter()
            .zip(policy_row.fields)
            .enumerate()
        {
            let name = format!("{phase}.{field}");
            let values = [
                baseline.phases[phase][index],
                observations[1].phases[phase][index],
                observations[2].phases[phase][index],
            ];
            match policy {
                PhaseMetricPolicy::ScaleBearing => {
                    validate_affine_metric(&name, values, denominators)?;
                }
                PhaseMetricPolicy::NodeBearingBytes => {
                    // Only private ordinal-V4 files are copied during this
                    // fresh hydration; other immutable objects are hardlinked.
                    // The controlled edge axis retains the identical node map.
                    if matches!(axis, LinearityAxis::Nodes) {
                        validate_affine_metric(&name, values, denominators)?;
                    } else {
                        validate_fixed_protocol_metric(&name, values, values[0], values[0])?;
                    }
                }
                PhaseMetricPolicy::BufferedCalls {
                    byte_field,
                    max_bytes_per_call,
                } => {
                    validate_buffered_calls(
                        &name,
                        values,
                        [
                            baseline.phases[phase][byte_field],
                            observations[1].phases[phase][byte_field],
                            observations[2].phases[phase][byte_field],
                        ],
                        max_bytes_per_call,
                    )?;
                }
                PhaseMetricPolicy::InventoryControlBytes { maximum } => {
                    if values[0] == 0
                        || values.windows(2).any(|pair| pair[1] < pair[0])
                        || values.iter().any(|value| *value > maximum)
                    {
                        return Err(format!(
                            "{name} inventory-control bytes violate 1..={maximum}: {values:?}"
                        ));
                    }
                }
                PhaseMetricPolicy::ShapeReadComponentCalls
                | PhaseMetricPolicy::ShapeWriteComponentCalls
                | PhaseMetricPolicy::EncodeWriteComponentCalls => {
                    for (rung, observation) in observations.iter().enumerate() {
                        let components: &[u64] = match policy {
                            PhaseMetricPolicy::ShapeReadComponentCalls => {
                                &observation.shape_read_component_calls
                            }
                            PhaseMetricPolicy::ShapeWriteComponentCalls => {
                                &observation.shape_write_component_calls
                            }
                            PhaseMetricPolicy::EncodeWriteComponentCalls => {
                                &observation.encode_write_component_calls
                            }
                            _ => unreachable!("matched component policy"),
                        };
                        let expected = components.iter().try_fold(0_u64, |total, value| {
                            total
                                .checked_add(*value)
                                .ok_or_else(|| format!("{name} component sum overflows"))
                        })?;
                        if expected == 0 || values[rung] != expected {
                            return Err(format!(
                                "{name} does not reconcile to native component calls at rung {rung}"
                            ));
                        }
                    }
                }
                PhaseMetricPolicy::AppendObjectInventory => {
                    validate_affine_metric(&name, values, denominators)?;
                    for (rung, observation) in observations.iter().enumerate() {
                        if values[rung] != observation.append_objects {
                            return Err(format!(
                                "{name} does not reconcile to append inventory at rung {rung}"
                            ));
                        }
                    }
                }
                PhaseMetricPolicy::ShapeBlockInventory => {
                    for (rung, observation) in observations.iter().enumerate() {
                        validate_shape_block_inventory(&name, values[rung], observation, rung)?;
                    }
                }
                PhaseMetricPolicy::CasReadComponentCalls
                | PhaseMetricPolicy::CasWriteComponentCalls => {
                    let read = policy == PhaseMetricPolicy::CasReadComponentCalls;
                    let payload_calls = observations.each_ref().map(|observation| {
                        let payload = &observation.cas_publication_io.payload;
                        if read {
                            payload.read_calls
                        } else {
                            payload.write_calls
                        }
                    });
                    let payload_bytes = observations.each_ref().map(|observation| {
                        let payload = &observation.cas_publication_io.payload;
                        if read {
                            payload.read_bytes
                        } else {
                            payload.write_bytes
                        }
                    });
                    validate_buffered_calls(
                        &format!("{name}.payload"),
                        payload_calls,
                        payload_bytes,
                        OBJECT_BUFFER_BYTES,
                    )?;
                    for observation in observations {
                        validate_fresh_cas_control_bound(observation)?;
                    }
                }
                PhaseMetricPolicy::CasFsyncInventory => {
                    for (rung, observation) in observations.iter().enumerate() {
                        let components = &observation.cas_publication_io;
                        let totals = components.totals().map_err(|error| error.to_string())?;
                        let expected = totals
                            .file_fsync_calls
                            .checked_add(totals.directory_fsync_calls)
                            .ok_or_else(|| format!("{name} component sum overflows"))?;
                        let expected_phase = [
                            totals.read_bytes,
                            totals.write_bytes,
                            totals.read_calls,
                            totals.write_calls,
                            0,
                            0,
                            expected,
                        ];
                        if observation.phases["cas_install_read_write"] != expected_phase {
                            return Err(format!(
                                "{name} native CAS components do not reconcile at rung {rung}"
                            ));
                        }
                        let requests = components
                            .payload
                            .installed_objects
                            .checked_add(components.payload.reused_objects)
                            .ok_or_else(|| format!("{name} payload inventory overflows"))?;
                        if requests != observation.canonical_artifact_objects {
                            return Err(format!(
                                "{name} payload requests differ from canonical inventory at rung {rung}"
                            ));
                        }
                        for (kind, component) in [
                            ("payload", &components.payload),
                            ("manifest", &components.manifest),
                        ] {
                            let requests = component
                                .installed_objects
                                .checked_add(component.reused_objects)
                                .ok_or_else(|| format!("{name} object request total overflows"))?;
                            if component.install_attempts < component.installed_objects
                                || component.install_attempts > requests
                                || component.install_attempts.checked_mul(2)
                                    != Some(component.directory_fsync_calls)
                                || component.file_fsync_calls < component.install_attempts
                                || (kind == "manifest"
                                    && component.file_fsync_calls != component.install_attempts)
                            {
                                return Err(format!(
                                    "{name} {kind} durability components differ at rung {rung}"
                                ));
                            }
                        }
                        let reads = &components.manifest_reads;
                        if reads.write_bytes != 0
                            || reads.write_calls != 0
                            || reads.file_fsync_calls != 0
                            || reads.directory_fsync_calls != 0
                            || reads.installed_objects != 0
                            || reads.install_attempts != 0
                            || reads.reused_objects != 0
                            || reads.installed_bytes != 0
                        {
                            return Err(format!(
                                "{name} path authentication reports non-read work at rung {rung}"
                            ));
                        }
                    }
                }
                PhaseMetricPolicy::EncodeFsyncInventory => {
                    for (rung, observation) in observations.iter().enumerate() {
                        let expected = observation
                            .encode_fsync_components
                            .into_iter()
                            .try_fold(0_u64, |total, value| total.checked_add(value))
                            .ok_or_else(|| format!("{name} component sum overflows"))?;
                        if values[rung] != expected {
                            return Err(format!(
                                "{name} does not reconcile output/spool/membership/ordinal barriers at rung {rung}"
                            ));
                        }
                    }
                }
                PhaseMetricPolicy::HydrationFsyncInventory => {
                    for (rung, observation) in observations.iter().enumerate() {
                        let expected = observation
                            .hydration_file_fsync_operations
                            .checked_add(observation.hydration_directory_fsync_operations)
                            .ok_or_else(|| format!("{name} protocol bound overflow"))?;
                        if observation.hydration_files_copied == 0
                            || expected == 0
                            || values[rung] != expected
                        {
                            return Err(format!(
                                "{name} does not reconcile file plus directory barriers at rung {rung}"
                            ));
                        }
                    }
                }
                PhaseMetricPolicy::StructurallyZero if values == [0, 0, 0] => {}
                PhaseMetricPolicy::StructurallyZero => {
                    return Err(format!("{name} must remain structurally zero: {values:?}"));
                }
            }
        }
    }
    let retained_policy_names = LINEARITY_RETAINED_FIELDS
        .iter()
        .map(|(name, _, _)| *name)
        .collect::<BTreeSet<_>>();
    let retained_names = baseline
        .retained
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if retained_policy_names != retained_names {
        return Err("retained-output policy inventory differs from evidence".into());
    }
    for (name, _, policy) in LINEARITY_RETAINED_FIELDS {
        let values = [
            baseline.retained[name],
            observations[1].retained[name],
            observations[2].retained[name],
        ];
        match policy {
            RetainedMetricPolicy::ScaleBearing => {
                validate_affine_metric(name, values, denominators)?;
            }
            RetainedMetricPolicy::FilesystemQuantized => {
                validate_quantized_allocation(name, values, denominators)?;
            }
            RetainedMetricPolicy::NodeBearing if matches!(axis, LinearityAxis::Nodes) => {
                validate_affine_metric(name, values, denominators)?;
            }
            RetainedMetricPolicy::EdgeBearing if matches!(axis, LinearityAxis::Edges) => {
                validate_affine_metric(name, values, denominators)?;
            }
            RetainedMetricPolicy::NodeFilesystemQuantized
                if matches!(axis, LinearityAxis::Nodes) =>
            {
                validate_quantized_allocation(name, values, denominators)?;
            }
            RetainedMetricPolicy::EdgeFilesystemQuantized
                if matches!(axis, LinearityAxis::Edges) =>
            {
                validate_quantized_allocation(name, values, denominators)?;
            }
            RetainedMetricPolicy::InventoryDerived => {
                for (rung, observation) in observations.iter().enumerate() {
                    let expected = match name {
                        "source.logical_references" => {
                            checked_category_sum(observation, "source.", 0)?
                        }
                        "source.physical_objects" => {
                            checked_category_sum(observation, "source.", 2)?
                        }
                        "clean_import.logical_references" => {
                            checked_category_sum(observation, "clean_import.", 0)?
                        }
                        "clean_import.physical_objects" => {
                            checked_category_sum(observation, "clean_import.", 2)?
                        }
                        "portable_package.logical_references"
                        | "portable_package.physical_objects" => 1,
                        _ => return Err(format!("unclassified retained inventory {name}")),
                    };
                    if values[rung] != expected {
                        return Err(format!(
                            "{name} does not reconcile to authoritative inventory at rung {rung}"
                        ));
                    }
                }
            }
            RetainedMetricPolicy::NodeBearing
            | RetainedMetricPolicy::EdgeBearing
            | RetainedMetricPolicy::NodeFilesystemQuantized
            | RetainedMetricPolicy::EdgeFilesystemQuantized => {
                validate_fixed_protocol_metric(name, values, values[0], values[0])?;
            }
        }
    }
    validate_category_taxonomy(axis, observations, denominators)?;
    let expected_phase_peaks = CERTIFICATION_PHASES.into_iter().collect::<BTreeSet<_>>();
    for observation in observations {
        let observed = observation
            .phase_disk_peaks
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if observed != expected_phase_peaks {
            return Err("lifecycle phase disk-peak inventory differs".into());
        }
    }
    for phase in CERTIFICATION_PHASES {
        let values = std::array::from_fn(|rung| observations[rung].phase_disk_peaks[phase]);
        if values != [0, 0, 0] {
            validate_quantized_allocation(
                &format!("{phase}.disk_peak_bytes"),
                values,
                denominators,
            )?;
        }
    }
    Ok(())
}

fn validate_lifecycle_metric_policies(
    observations: &[LifecycleLinearityObservation; 3],
) -> Result<(), String> {
    validate_lifecycle_metric_policies_for_axis(LinearityAxis::Nodes, observations)
}

fn synthetic_category_metrics(axis: LinearityAxis, factor: u64) -> BTreeMap<String, [u64; 6]> {
    let mut metrics = BTreeMap::new();
    for owner in ["source", "clean_import"] {
        for category in ATTRIBUTION_CATEGORY_NAMES {
            let bearing = match category_behavior(category).unwrap() {
                CategoryBehavior::NodeBearing => matches!(axis, LinearityAxis::Nodes),
                CategoryBehavior::EdgeBearing => matches!(axis, LinearityAxis::Edges),
                CategoryBehavior::Mixed => true,
                CategoryBehavior::FixedInventory => false,
                CategoryBehavior::StructurallyZero => {
                    metrics.insert(format!("{owner}.{category}"), [0; 6]);
                    continue;
                }
            };
            let scale = if bearing { factor } else { 1 };
            metrics.insert(
                format!("{owner}.{category}"),
                [
                    1,
                    100 + 900 * scale,
                    1,
                    100 + 900 * scale,
                    4096 * scale,
                    4096 * scale,
                ],
            );
        }
    }
    for category in ATTRIBUTION_CATEGORY_NAMES {
        metrics.insert(format!("construction.{category}"), [0; 6]);
    }
    metrics.insert(
        "construction.construction_staging".into(),
        [
            6 + 2 * factor,
            100 + 900 * factor,
            6 + 2 * factor,
            100 + 900 * factor,
            4096 * factor,
            8192 * factor,
        ],
    );
    metrics
}

fn synthetic_linearity_observations_for_axis(
    axis: LinearityAxis,
) -> [LifecycleLinearityObservation; 3] {
    [1_u64, 2, 4].map(|factor| {
        let append_objects = 6 + 2 * factor;
        let (live_nodes, live_edges) = match axis {
            LinearityAxis::Nodes => (100 * factor, 80),
            LinearityAxis::Edges => (100, 70 * factor),
        };
        LifecycleLinearityObservation {
            phases: BTreeMap::from([
                (
                    "append_merge".into(),
                    [0, 100 + 800 * factor, 0, factor, append_objects, 0, 0],
                ),
                ("seal_authentication".into(), [0, 0, 0, 0, 0, 0, 0]),
                (
                    "shape_consume_reauthentication".into(),
                    [
                        100 + 900 * factor,
                        100 + 800 * factor,
                        factor,
                        factor,
                        0,
                        6 + 2 * factor,
                        0,
                    ],
                ),
                (
                    "encode_write_postwrite_authentication".into(),
                    [
                        100 + 900 * factor,
                        100 + 800 * factor,
                        807,
                        factor,
                        0,
                        0,
                        43,
                    ],
                ),
                (
                    "publication_preauthentication".into(),
                    [512, 0, 1, 0, 0, 0, 0],
                ),
                (
                    "cas_install_read_write".into(),
                    [
                        119 + 900 * factor,
                        100 + 800 * factor,
                        19 * factor + 39,
                        19 * factor + 1,
                        0,
                        0,
                        60,
                    ],
                ),
                (
                    "hydration_verification".into(),
                    [
                        100 + 900 * factor,
                        if matches!(axis, LinearityAxis::Nodes) {
                            100 + 800 * factor
                        } else {
                            900
                        },
                        factor,
                        factor,
                        0,
                        0,
                        38,
                    ],
                ),
                (
                    "fsync_synchronization".into(),
                    [0, 0, 0, 0, 0, 0, 2 * factor],
                ),
                (
                    "recovery_reauthentication".into(),
                    [100 + 900 * factor, 0, factor, 0, 0, 0, 0],
                ),
            ]),
            retained: LINEARITY_RETAINED_FIELDS
                .iter()
                .map(|(name, _, policy)| {
                    let value = match policy {
                        RetainedMetricPolicy::ScaleBearing => 100 + 900 * factor,
                        RetainedMetricPolicy::FilesystemQuantized => 4096 * factor,
                        RetainedMetricPolicy::InventoryDerived => {
                            if name.starts_with("portable_package") {
                                1
                            } else {
                                5
                            }
                        }
                        RetainedMetricPolicy::NodeBearing => {
                            if matches!(axis, LinearityAxis::Nodes) {
                                100 + 900 * factor
                            } else {
                                900
                            }
                        }
                        RetainedMetricPolicy::EdgeBearing => {
                            if matches!(axis, LinearityAxis::Edges) {
                                100 + 900 * factor
                            } else {
                                900
                            }
                        }
                        RetainedMetricPolicy::NodeFilesystemQuantized => {
                            if matches!(axis, LinearityAxis::Nodes) {
                                4096 * factor
                            } else {
                                4096
                            }
                        }
                        RetainedMetricPolicy::EdgeFilesystemQuantized => {
                            if matches!(axis, LinearityAxis::Edges) {
                                4096 * factor
                            } else {
                                4096
                            }
                        }
                    };
                    ((*name).to_owned(), value)
                })
                .collect(),
            append_objects,
            input_rows: live_nodes + live_edges,
            live_nodes,
            live_edges,
            shape_merge_bytes: [100 + 900 * factor, 100 + 800 * factor],
            shape_block_components: [3 + factor, 3 + factor],
            canonical_artifact_objects: 19,
            cas_publication_io: graphforge_storage::GraphPublicationIo {
                publications: 1,
                initial_entries: 0,
                changed_paths: 19,
                payload: graphforge_storage::GraphObjectIoTotals {
                    read_bytes: 900 * factor,
                    read_calls: 19 * factor,
                    write_bytes: 800 * factor,
                    write_calls: 19 * factor,
                    file_fsync_calls: 19,
                    directory_fsync_calls: 38,
                    installed_objects: 19,
                    install_attempts: 19,
                    installed_bytes: 800 * factor,
                    ..Default::default()
                },
                manifest: graphforge_storage::GraphObjectIoTotals {
                    read_bytes: 100,
                    read_calls: 20,
                    reused_objects: 19,
                    write_bytes: 100,
                    write_calls: 1,
                    file_fsync_calls: 1,
                    directory_fsync_calls: 2,
                    installed_objects: 1,
                    install_attempts: 1,
                    installed_bytes: 100,
                    ..Default::default()
                },
                manifest_reads: graphforge_storage::GraphObjectIoTotals {
                    read_bytes: 19,
                    read_calls: 19,
                    ..Default::default()
                },
            },
            encode_fsync_components: [10, 5, 8, 20],
            hydration_files_copied: 19,
            hydration_file_fsync_operations: 19,
            hydration_directory_fsync_operations: 19,
            shape_read_component_calls: [factor, 0, 0, 0, 0, 0],
            shape_write_component_calls: [factor, 0],
            encode_write_component_calls: [factor, 0, 0, 0, 0],
            category_metrics: synthetic_category_metrics(axis, factor),
            category_authority_metrics: synthetic_category_metrics(axis, factor),
            phase_disk_peaks: CERTIFICATION_PHASES
                .into_iter()
                .map(|phase| (phase.to_owned(), 4096 * factor))
                .collect(),
        }
    })
}

fn synthetic_linearity_observations() -> [LifecycleLinearityObservation; 3] {
    synthetic_linearity_observations_for_axis(LinearityAxis::Nodes)
}

fn synthetic_authority_context(
    owner: &str,
) -> graphforge_storage::ArtifactCategoryAuthorityContext {
    let native_category_identity_authority_sha256 = graphforge_storage::ArtifactCategory::ALL
        .into_iter()
        .map(|category| {
            (
                category,
                safe_authority_digest(
                    b"synthetic-category-identity-authority\0",
                    &[owner.as_bytes(), category_name(category).as_bytes()],
                ),
            )
        })
        .collect();
    graphforge_storage::ArtifactCategoryAuthorityContext {
        contract: CATEGORY_AUTHORITY_CONTRACT.to_owned(),
        version: 1,
        rung: 1,
        generation_sha256: format!("sha256:{}", "a".repeat(64)),
        owner: owner.to_owned(),
        receipt_authority_sha256: safe_authority_digest(
            b"synthetic-receipt-authority\0",
            &[owner.as_bytes()],
        ),
        native_identity_authority_sha256: safe_authority_digest(
            b"synthetic-native-identity-authority\0",
            &[owner.as_bytes()],
        ),
        native_category_identity_authority_sha256,
        live_nodes: 100,
        live_edges: 80,
    }
}

fn category_commitments(
    context: &graphforge_storage::ArtifactCategoryAuthorityContext,
    categories: &serde_json::Map<String, Value>,
) -> Value {
    Value::Object(
        graphforge_storage::ArtifactCategory::ALL
            .into_iter()
            .map(|category| {
                let name = category_name(category);
                let totals: graphforge_storage::ArtifactStorageTotals =
                    serde_json::from_value(categories[name].clone()).unwrap();
                (
                    name.to_owned(),
                    json!(graphforge_storage::artifact_category_authority_commitment(
                        context, category, &totals
                    )),
                )
            })
            .collect(),
    )
}

fn peak_commitments(
    context: &graphforge_storage::ArtifactCategoryAuthorityContext,
    peaks: &serde_json::Map<String, Value>,
) -> Value {
    Value::Object(
        graphforge_storage::ArtifactCategory::ALL
            .into_iter()
            .map(|category| {
                let name = category_name(category);
                (
                    name.to_owned(),
                    json!(
                        graphforge_storage::artifact_category_peak_authority_commitment(
                            context,
                            category,
                            peaks[name].as_u64().unwrap(),
                        )
                    ),
                )
            })
            .collect(),
    )
}

fn synthetic_attribution_owner(
    owner: &str,
) -> (Value, graphforge_storage::ArtifactCategoryAuthorityContext) {
    let mut categories = serde_json::Map::new();
    for category in ATTRIBUTION_CATEGORY_NAMES {
        categories.insert(
            category.to_owned(),
            json!({
                "logical_references": 0,
                "logical_bytes": 0,
                "physical_objects": 0,
                "physical_logical_bytes": 0,
                "allocated_bytes": 0,
            }),
        );
    }
    categories.insert(
        "topology_nodes".into(),
        json!({
            "logical_references": 1,
            "logical_bytes": 100,
            "physical_objects": 1,
            "physical_logical_bytes": 100,
            "allocated_bytes": 4096,
        }),
    );
    categories.insert(
        "topology_edges".into(),
        json!({
            "logical_references": 1,
            "logical_bytes": 80,
            "physical_objects": 1,
            "physical_logical_bytes": 80,
            "allocated_bytes": 4096,
        }),
    );
    for category in ["uuid_and_surrogates", "adjacency"] {
        categories.insert(
            category.into(),
            json!({
                "logical_references": 1,
                "logical_bytes": 180,
                "physical_objects": 1,
                "physical_logical_bytes": 180,
                "allocated_bytes": 4096,
            }),
        );
    }
    let context = synthetic_authority_context(owner);
    let authority_sha256 = category_commitments(&context, &categories);
    (
        json!({
        "categories": categories.clone(),
        "category_authorities": categories,
        "category_authority_context": context.clone(),
        "category_authority_sha256": authority_sha256,
        "logical_references": 4,
        "logical_bytes": 540,
        "physical_objects": 4,
        "physical_logical_bytes": 540,
        "allocated_bytes": 16384,
        }),
        context,
    )
}

fn synthetic_retained_evidence() -> Value {
    let mut construction_current = serde_json::Map::new();
    let mut construction_peaks = serde_json::Map::new();
    for category in ATTRIBUTION_CATEGORY_NAMES {
        construction_current.insert(
            category.to_owned(),
            json!({
                "logical_references": 0,
                "logical_bytes": 0,
                "physical_objects": 0,
                "physical_logical_bytes": 0,
                "allocated_bytes": 0,
            }),
        );
        construction_peaks.insert(category.to_owned(), json!(0));
    }
    construction_current.insert(
        "construction_staging".into(),
        json!({
            "logical_references": 1,
            "logical_bytes": 100,
            "physical_objects": 1,
            "physical_logical_bytes": 100,
            "allocated_bytes": 4096,
        }),
    );
    construction_peaks.insert("construction_staging".into(), json!(8192));
    let construction_context = synthetic_authority_context("construction");
    let construction_authority_sha256 =
        category_commitments(&construction_context, &construction_current);
    let construction_peak_authority_sha256 =
        peak_commitments(&construction_context, &construction_peaks);
    let portable_authority = graphforge_storage::ArtifactStorageTotals {
        logical_references: 1,
        logical_bytes: 200,
        physical_objects: 1,
        physical_logical_bytes: 200,
        allocated_bytes: 4096,
    };
    let portable_context = synthetic_authority_context("portable_package");
    let portable_authority_sha256 = graphforge_storage::artifact_category_authority_commitment(
        &portable_context,
        graphforge_storage::ArtifactCategory::PortablePackage,
        &portable_authority,
    );
    let (source, _) = synthetic_attribution_owner("source");
    let (clean_import, _) = synthetic_attribution_owner("clean_import");
    json!({
        "source_nodes": 100,
        "source_edges": 80,
        "storage": {
            "source": source,
            "clean_import": clean_import,
            "portable_package": {
                "logical_references": 1,
                "logical_bytes": 200,
                "physical_objects": 1,
                "allocated_bytes": 4096,
                "category_authority": portable_authority,
                "category_authority_context": portable_context,
                "category_authority_sha256": portable_authority_sha256,
            },
            "construction": {
                "storage_current": construction_current.clone(),
                "storage_category_authorities": construction_current,
                "storage_category_authority_context": construction_context,
                "storage_category_authority_sha256": construction_authority_sha256,
                "storage_transient_peak_allocated_bytes": construction_peaks.clone(),
                "storage_transient_peak_authorities": construction_peaks,
                "storage_transient_peak_authority_sha256": construction_peak_authority_sha256,
                "storage_transient_peak_total_allocated_bytes": 8192,
            },
            "source_project_current_allocated_bytes": 16384,
            "workspace_current_allocated_bytes": 49152,
            "workspace_peak_allocated_bytes": 57344,
            "workspace_components": {
                "source_project_and_construction": 16384,
                "portable_package": 4096,
                "clean_import_project": 16384,
                "drill_project_and_construction": 4096,
                "drill_package": 4096,
                "corrupt_drill_package": 4096,
            },
        },
        "phases": [
            {"id": "ingest", "disk_peak_bytes": 49152},
            {"id": "import", "disk_peak_bytes": 57344},
        ],
    })
}

#[test]
fn cas_control_proof_accepts_path_variation_and_rejects_coherent_overcounts() {
    let mut observations = synthetic_linearity_observations();
    for (observation, installed) in observations.iter_mut().zip([1, 2, 1]) {
        let io = &mut observation.cas_publication_io;
        io.manifest.installed_objects = installed;
        io.manifest.install_attempts = installed;
        io.manifest.read_calls = installed + io.manifest.reused_objects;
        io.manifest.write_calls = installed;
        io.manifest.file_fsync_calls = installed;
        io.manifest.directory_fsync_calls = 2 * installed;
        let totals = io.totals().unwrap();
        observation.phases.insert(
            "cas_install_read_write".into(),
            [
                totals.read_bytes,
                totals.write_bytes,
                totals.read_calls,
                totals.write_calls,
                0,
                0,
                totals.file_fsync_calls + totals.directory_fsync_calls,
            ],
        );
    }
    validate_lifecycle_metric_policies(&observations).unwrap();
    let mut missing_manifest = observations[0].clone();
    missing_manifest.cas_publication_io.manifest = Default::default();
    missing_manifest.cas_publication_io.manifest_reads = Default::default();
    let totals = missing_manifest.cas_publication_io.totals().unwrap();
    missing_manifest.phases.insert(
        "cas_install_read_write".into(),
        [
            totals.read_bytes,
            totals.write_bytes,
            totals.read_calls,
            totals.write_calls,
            0,
            0,
            totals.file_fsync_calls + totals.directory_fsync_calls,
        ],
    );
    assert!(
        validate_fresh_cas_control_bound(&missing_manifest)
            .unwrap_err()
            .contains("mandatory bootstrap")
    );
    let mut excessive = observations[0].clone();
    let installed = 67 * excessive.canonical_artifact_objects + 2;
    let manifest = &mut excessive.cas_publication_io.manifest;
    manifest.installed_objects = installed;
    manifest.install_attempts = installed;
    manifest.read_calls = installed + manifest.reused_objects;
    manifest.read_bytes = manifest.read_calls;
    manifest.write_calls = installed;
    manifest.file_fsync_calls = installed;
    manifest.directory_fsync_calls = 2 * installed;
    let totals = excessive.cas_publication_io.totals().unwrap();
    excessive.phases.insert(
        "cas_install_read_write".into(),
        [
            totals.read_bytes,
            totals.write_bytes,
            totals.read_calls,
            totals.write_calls,
            0,
            0,
            totals.file_fsync_calls + totals.directory_fsync_calls,
        ],
    );
    assert!(
        validate_fresh_cas_control_bound(&excessive)
            .unwrap_err()
            .contains("native path/object")
    );
    for invalid in ["repeated", "prior", "tombstoned"] {
        let mut changed = observations[0].clone();
        match invalid {
            "repeated" => changed.cas_publication_io.publications = 2,
            "prior" => changed.cas_publication_io.initial_entries = 1,
            "tombstoned" => changed.cas_publication_io.changed_paths += 1,
            _ => unreachable!(),
        }
        assert!(
            validate_fresh_cas_control_bound(&changed)
                .unwrap_err()
                .contains("one fresh publication")
        );
    }
    let mut excess_bytes = observations[0].clone();
    excess_bytes.cas_publication_io.manifest_reads.read_bytes = u64::MAX;
    excess_bytes.cas_publication_io.manifest_reads.read_calls = u64::MAX;
    assert!(validate_fresh_cas_control_bound(&excess_bytes).is_err());
}

#[test]
fn controlled_fixture_policies_reject_coherent_zero_and_excess_work() {
    for axis in [LinearityAxis::Nodes, LinearityAxis::Edges] {
        let mut observations = synthetic_linearity_observations_for_axis(axis);
        for (rung, observation) in observations.iter_mut().enumerate() {
            for owner in ["source", "clean_import"] {
                let key = format!("{owner}.catalog_and_manifests");
                for metrics in [
                    &mut observation.category_metrics,
                    &mut observation.category_authority_metrics,
                ] {
                    for field in [1, 3] {
                        metrics.get_mut(&key).unwrap()[field] += rung as u64;
                    }
                }
            }
        }
        validate_lifecycle_metric_policies_for_axis(axis, &observations)
            .expect("catalog text lengths may gain digits without adding objects");
        for (category, field) in [
            ("catalog_and_manifests", 1),
            ("catalog_and_manifests", 3),
            ("catalog_and_manifests", 4),
            ("catalog_and_manifests", 5),
            ("adjacency", 1),
            ("adjacency", 3),
        ] {
            for value in [0, u64::MAX] {
                let mut invalid = observations.clone();
                for owner in ["source", "clean_import"] {
                    let key = format!("{owner}.{category}");
                    invalid[2].category_metrics.get_mut(&key).unwrap()[field] = value;
                    invalid[2].category_authority_metrics.get_mut(&key).unwrap()[field] = value;
                }
                let error = validate_lifecycle_metric_policies_for_axis(axis, &invalid)
                    .expect_err("coherent category mutations still fail native growth policy");
                assert!(error.contains(category), "{error}");
            }
        }
        for value in [0, u64::MAX] {
            let mut invalid = observations.clone();
            invalid[2].phases.get_mut("hydration_verification").unwrap()[1] = value;
            let error = validate_lifecycle_metric_policies_for_axis(axis, &invalid)
                .expect_err("private ordinal copies cannot omit bytes or exceed their axis bound");
            assert!(error.contains("hydration_verification"), "{error}");
        }
        let mut omitted_copies = observations;
        for observation in &mut omitted_copies {
            observation
                .phases
                .get_mut("hydration_verification")
                .unwrap()[1] = 0;
        }
        assert!(validate_lifecycle_metric_policies_for_axis(axis, &omitted_copies).is_err());
    }
}

#[test]
fn lifecycle_metric_policy_accepts_bounded_fixed_protocol_and_rejects_false_growth() {
    let observations = synthetic_linearity_observations();
    validate_lifecycle_metric_policies(&observations)
        .expect("scale-bearing slopes and bounded fixed protocol overhead");
    let retained_evidence = synthetic_retained_evidence();
    validate_retained_reconciliation(&retained_evidence)
        .expect("synthetic retained evidence reconciles");

    let mut category_underreport = retained_evidence.clone();
    category_underreport["storage"]["source"]["categories"]["topology_nodes"]["logical_bytes"] =
        json!(99);
    assert!(validate_retained_reconciliation(&category_underreport).is_err());

    let mut inventory_mismatch = retained_evidence.clone();
    inventory_mismatch["storage"]["portable_package"]["physical_objects"] = json!(2);
    assert!(validate_retained_reconciliation(&inventory_mismatch).is_err());

    let mut transient_underreport = retained_evidence.clone();
    transient_underreport["storage"]["construction"]["storage_transient_peak_allocated_bytes"]["construction_staging"] =
        json!(2048);
    assert!(validate_retained_reconciliation(&transient_underreport).is_err());

    let mut retained_owner_underreport = retained_evidence.clone();
    retained_owner_underreport["storage"]["workspace_current_allocated_bytes"] = json!(8192);
    assert!(validate_retained_reconciliation(&retained_owner_underreport).is_err());

    let mut missing_construction_category = retained_evidence.clone();
    missing_construction_category["storage"]["construction"]["storage_current"]
        .as_object_mut()
        .unwrap()
        .remove("properties");
    assert!(validate_retained_reconciliation(&missing_construction_category).is_err());

    let mut extra_construction_category = retained_evidence.clone();
    extra_construction_category["storage"]["construction"]["storage_transient_peak_allocated_bytes"]
        ["unknown"] = json!(0);
    assert!(validate_retained_reconciliation(&extra_construction_category).is_err());

    let mut double_counted_component = retained_evidence.clone();
    double_counted_component["storage"]["workspace_components"]["clean_import_project"] =
        json!(20_480);
    assert!(validate_retained_reconciliation(&double_counted_component).is_err());

    let mut overflowing_components = retained_evidence.clone();
    overflowing_components["storage"]["workspace_components"]["source_project_and_construction"] =
        json!(u64::MAX);
    overflowing_components["storage"]["workspace_components"]["clean_import_project"] =
        json!(u64::MAX);
    assert!(validate_retained_reconciliation(&overflowing_components).is_err());

    assert!(checked_product("boundary", &[u128::MAX, 2]).is_err());
    assert!(checked_lifecycle_peak_allocation(u64::MAX, 1).is_err());
    assert!(
        validate_affine_metric(
            "boundary",
            [u64::MAX - 2, u64::MAX - 1, u64::MAX],
            [u64::MAX - 2, u64::MAX - 1, u64::MAX],
        )
        .is_err()
    );

    let shared = BTreeMap::from([("cas-object".to_owned(), 4096)]);
    let mut allocation = graphforge_storage::StorageAllocationLifecycle::default();
    allocation
        .replace_owner("source", &shared)
        .expect("install source CAS identity");
    allocation
        .replace_owner("import", &shared)
        .expect("shared CAS identity is a second reference, not a second allocation");
    assert_eq!(allocation.current_allocated_bytes(), 4096);
    assert!(
        allocation
            .replace_owner(
                "invalid",
                &BTreeMap::from([("cas-object".to_owned(), 8192)])
            )
            .is_err(),
        "the same CAS identity with contradictory allocation must fail"
    );

    for (phase, field, field_name) in [
        ("append_merge", 1, "append write bytes"),
        ("append_merge", 4, "append objects"),
        ("shape_consume_reauthentication", 5, "shape logical blocks"),
        ("fsync_synchronization", 6, "append/merge fsync work"),
        ("recovery_reauthentication", 0, "recovery read bytes"),
    ] {
        let mut held_constant = observations.clone();
        held_constant[1].phases.get_mut(phase).unwrap()[field] =
            held_constant[0].phases[phase][field];
        held_constant[2].phases.get_mut(phase).unwrap()[field] =
            held_constant[0].phases[phase][field];
        assert!(
            validate_lifecycle_metric_policies(&held_constant).is_err(),
            "held-constant {field_name} must fail"
        );

        let mut underreported = observations.clone();
        underreported[2].phases.get_mut(phase).unwrap()[field] =
            underreported[1].phases[phase][field] + 1;
        assert!(
            validate_lifecycle_metric_policies(&underreported).is_err(),
            "underreported 4x {field_name} must fail"
        );
    }
    let mut missing_phase = observations.clone();
    missing_phase[1].phases.remove("hydration_verification");
    assert!(validate_lifecycle_metric_policies(&missing_phase).is_err());

    let mut underreported_shape_aggregate = observations.clone();
    underreported_shape_aggregate[2]
        .phases
        .get_mut("shape_consume_reauthentication")
        .unwrap()[5] -= 1;
    assert!(validate_lifecycle_metric_policies(&underreported_shape_aggregate).is_err());

    let mut overreported_shape_aggregate = observations.clone();
    overreported_shape_aggregate[2]
        .phases
        .get_mut("shape_consume_reauthentication")
        .unwrap()[5] += 1;
    assert!(validate_lifecycle_metric_policies(&overreported_shape_aggregate).is_err());

    let mut underreported_shape_component = observations.clone();
    underreported_shape_component[2].shape_block_components[0] = 0;
    underreported_shape_component[2]
        .phases
        .get_mut("shape_consume_reauthentication")
        .unwrap()[5] = underreported_shape_component[2].shape_block_components[1];
    assert!(validate_lifecycle_metric_policies(&underreported_shape_component).is_err());

    let mut overreported_shape_component = observations.clone();
    overreported_shape_component[2].shape_block_components[1] = overreported_shape_component[2]
        .shape_merge_bytes[1]
        .checked_add(1)
        .expect("synthetic overreported component");
    overreported_shape_component[2]
        .phases
        .get_mut("shape_consume_reauthentication")
        .unwrap()[5] = overreported_shape_component[2]
        .shape_block_components
        .into_iter()
        .try_fold(0_u64, u64::checked_add)
        .expect("synthetic component total");
    assert!(validate_lifecycle_metric_policies(&overreported_shape_component).is_err());

    let mut overflowing_shape_components = observations.clone();
    overflowing_shape_components[2].shape_block_components = [u64::MAX, 1];
    overflowing_shape_components[2]
        .phases
        .get_mut("shape_consume_reauthentication")
        .unwrap()[5] = u64::MAX;
    assert!(validate_lifecycle_metric_policies(&overflowing_shape_components).is_err());

    let mut nonzero_structural_field = observations.clone();
    nonzero_structural_field[2]
        .phases
        .get_mut("append_merge")
        .unwrap()[0] = 1;
    assert!(validate_lifecycle_metric_policies(&nonzero_structural_field).is_err());

    for field in [0, 2] {
        let mut nonzero_seal_field = observations.clone();
        nonzero_seal_field[2]
            .phases
            .get_mut("seal_authentication")
            .unwrap()[field] = 1;
        assert!(
            validate_lifecycle_metric_policies(&nonzero_seal_field).is_err(),
            "seal structural field {field} must remain zero"
        );
    }

    let mut zero_scale_phase = observations.clone();
    for observation in &mut zero_scale_phase {
        observation
            .phases
            .get_mut("encode_write_postwrite_authentication")
            .unwrap()[1] = 0;
    }
    assert!(validate_lifecycle_metric_policies(&zero_scale_phase).is_err());

    let mut zero_scale_retained = observations.clone();
    for observation in &mut zero_scale_retained {
        observation
            .retained
            .insert("source.logical_bytes".into(), 0);
    }
    assert!(validate_lifecycle_metric_policies(&zero_scale_retained).is_err());

    let mut underreported_buffered_calls = observations.clone();
    underreported_buffered_calls[2]
        .phases
        .get_mut("hydration_verification")
        .unwrap()[2] = 0;
    assert!(validate_lifecycle_metric_policies(&underreported_buffered_calls).is_err());

    let mut underreported_shape_calls = observations.clone();
    underreported_shape_calls[2]
        .phases
        .get_mut("shape_consume_reauthentication")
        .unwrap()[2] = 1;
    assert!(validate_lifecycle_metric_policies(&underreported_shape_calls).is_err());

    let mut inflated_encode_calls = observations.clone();
    inflated_encode_calls[2]
        .phases
        .get_mut("encode_write_postwrite_authentication")
        .unwrap()[3] += 1;
    assert!(validate_lifecycle_metric_policies(&inflated_encode_calls).is_err());

    let mut component_call_plateau = observations.clone();
    for observation in &mut component_call_plateau {
        observation
            .phases
            .get_mut("shape_consume_reauthentication")
            .unwrap()[2] = 2;
        observation.shape_read_component_calls = [2, 0, 0, 0, 0, 0];
    }
    validate_lifecycle_metric_policies(&component_call_plateau)
        .expect("native component-call plateau");

    let mut oversized_inventory_control = observations.clone();
    oversized_inventory_control[2]
        .phases
        .get_mut("publication_preauthentication")
        .unwrap()[0] = ENCODING_BUFFER_BYTES + 1;
    assert!(validate_lifecycle_metric_policies(&oversized_inventory_control).is_err());

    let mut zero_denominator = observations.clone();
    zero_denominator[1].live_nodes = 0;
    assert!(validate_lifecycle_metric_policies(&zero_denominator).is_err());
    let mut nonmonotone_denominator = observations.clone();
    nonmonotone_denominator[2].live_nodes = nonmonotone_denominator[1].live_nodes;
    assert!(validate_lifecycle_metric_policies(&nonmonotone_denominator).is_err());
    let mut changed_controlled_axis = observations.clone();
    changed_controlled_axis[2].live_edges += 1;
    assert!(validate_lifecycle_metric_policies(&changed_controlled_axis).is_err());

    let mut below_row_bound = observations.clone();
    below_row_bound[2].phases.get_mut("append_merge").unwrap()[1] =
        below_row_bound[2].input_rows - 1;
    assert!(validate_lifecycle_metric_policies(&below_row_bound).is_err());

    let mut compensated_category_shift = observations.clone();
    for observation in &mut compensated_category_shift {
        for owner in ["source", "clean_import"] {
            let node_key = format!("{owner}.topology_nodes");
            let edge_key = format!("{owner}.topology_edges");
            for field in [1, 3] {
                observation.category_metrics.get_mut(&node_key).unwrap()[field] -= 1;
                observation.category_metrics.get_mut(&edge_key).unwrap()[field] += 1;
            }
        }
    }
    assert_eq!(
        validate_lifecycle_metric_policies(&compensated_category_shift),
        Err("retained category totals differ from storage-owned authorities".into())
    );

    let edge_observations = synthetic_linearity_observations_for_axis(LinearityAxis::Edges);
    validate_lifecycle_metric_policies_for_axis(LinearityAxis::Edges, &edge_observations)
        .expect("edge-axis denominator proof");
    let mut edge_underreport = edge_observations;
    edge_underreport[2].retained.insert(
        "source.canonical_edges.logical_bytes".into(),
        edge_underreport[1].retained["source.canonical_edges.logical_bytes"],
    );
    assert!(
        validate_lifecycle_metric_policies_for_axis(LinearityAxis::Edges, &edge_underreport)
            .is_err()
    );
    for (retained, _, policy) in LINEARITY_RETAINED_FIELDS {
        if !matches!(
            policy,
            RetainedMetricPolicy::ScaleBearing | RetainedMetricPolicy::NodeBearing
        ) {
            continue;
        }
        let mut held_constant = observations.clone();
        held_constant[1]
            .retained
            .insert(retained.to_owned(), held_constant[0].retained[retained]);
        held_constant[2]
            .retained
            .insert(retained.to_owned(), held_constant[0].retained[retained]);
        assert!(
            validate_lifecycle_metric_policies(&held_constant).is_err(),
            "held-constant retained metric {retained} must fail"
        );

        let mut underreported = observations.clone();
        underreported[2]
            .retained
            .insert(retained.to_owned(), underreported[1].retained[retained] + 1);
        assert!(
            validate_lifecycle_metric_policies(&underreported).is_err(),
            "underreported 4x retained metric {retained} must fail"
        );
    }
    let mut decreasing_allocated = observations.clone();
    decreasing_allocated[2]
        .retained
        .insert("source.allocated_bytes".into(), 1);
    assert!(validate_lifecycle_metric_policies(&decreasing_allocated).is_err());

    let mut quantized_plateau = observations.clone();
    for rung in 1..3 {
        for owner in ["source", "clean_import"] {
            let key = format!("{owner}.topology_nodes");
            for field in [4, 5] {
                let baseline = quantized_plateau[0].category_metrics[&key][field];
                quantized_plateau[rung]
                    .category_metrics
                    .get_mut(&key)
                    .unwrap()[field] = baseline;
                quantized_plateau[rung]
                    .category_authority_metrics
                    .get_mut(&key)
                    .unwrap()[field] = baseline;
            }
        }
    }
    validate_lifecycle_metric_policies(&quantized_plateau)
        .expect("legitimate filesystem-allocation plateau");

    let mut quantized_upper_bound = observations.clone();
    for owner in ["source", "clean_import"] {
        let key = format!("{owner}.topology_nodes");
        quantized_upper_bound[2]
            .category_metrics
            .get_mut(&key)
            .unwrap()[4] = u64::MAX;
        quantized_upper_bound[2]
            .category_authority_metrics
            .get_mut(&key)
            .unwrap()[4] = u64::MAX;
    }
    assert!(validate_lifecycle_metric_policies(&quantized_upper_bound).is_err());

    let mut changed_retained_inventory = observations.clone();
    changed_retained_inventory[2]
        .retained
        .insert("source.physical_objects".into(), 20);
    assert!(validate_lifecycle_metric_policies(&changed_retained_inventory).is_err());

    let mut excess_retained_shape = observations.clone();
    excess_retained_shape[2].retained.insert(
        "construction.current_merge_temporary_allocated_bytes".into(),
        u64::MAX,
    );
    assert!(validate_lifecycle_metric_policies(&excess_retained_shape).is_err());

    let fsync_index = LINEARITY_PHASE_FIELDS
        .iter()
        .position(|field| *field == "fsync_calls")
        .unwrap();
    let mut decreasing_fixed = observations.clone();
    decreasing_fixed[1]
        .phases
        .get_mut("cas_install_read_write")
        .unwrap()[fsync_index] = 38;
    assert!(
        validate_lifecycle_metric_policies(&decreasing_fixed).is_err(),
        "decreasing fixed protocol count must fail"
    );

    let mut inflated_fixed = observations.clone();
    inflated_fixed[2]
        .phases
        .get_mut("cas_install_read_write")
        .unwrap()[fsync_index] = 40;
    assert!(
        validate_lifecycle_metric_policies(&inflated_fixed).is_err(),
        "inflated fixed protocol count must fail"
    );
    let mut changed_canonical_inventory = observations.clone();
    changed_canonical_inventory[2].canonical_artifact_objects += 1;
    assert!(validate_lifecycle_metric_policies(&changed_canonical_inventory).is_err());

    let mut changed_encode_inventory = observations.clone();
    changed_encode_inventory[2].encode_fsync_components[0] += 1;
    assert!(validate_lifecycle_metric_policies(&changed_encode_inventory).is_err());

    for (file_delta, directory_delta) in [(1_i64, 0_i64), (-1, 0), (0, 1), (0, -1)] {
        let mut changed_hydration_inventory = observations.clone();
        let observation = &mut changed_hydration_inventory[2];
        observation.hydration_file_fsync_operations = observation
            .hydration_file_fsync_operations
            .checked_add_signed(file_delta)
            .unwrap();
        observation.hydration_directory_fsync_operations = observation
            .hydration_directory_fsync_operations
            .checked_add_signed(directory_delta)
            .unwrap();
        assert!(validate_lifecycle_metric_policies(&changed_hydration_inventory).is_err());
    }

    let read_calls_index = LINEARITY_PHASE_FIELDS
        .iter()
        .position(|field| *field == "read_calls")
        .unwrap();
    validate_buffered_calls(
        "legitimate plateau",
        [2, 2, 2],
        [
            ENCODING_BUFFER_BYTES + 1,
            ENCODING_BUFFER_BYTES + 2,
            ENCODING_BUFFER_BYTES + 3,
        ],
        ENCODING_BUFFER_BYTES,
    )
    .expect("quantized call plateau");
    assert!(
        validate_buffered_calls(
            "below derived bound",
            [1, 1, 1],
            [ENCODING_BUFFER_BYTES + 1; 3],
            ENCODING_BUFFER_BYTES,
        )
        .is_err()
    );
    assert!(
        validate_buffered_calls(
            "above derived bound",
            [2, 2, 3],
            [2, 2, 2],
            ENCODING_BUFFER_BYTES,
        )
        .is_err()
    );

    let mut missing_encoding_reads = observations;
    for observation in &mut missing_encoding_reads {
        observation
            .phases
            .get_mut("encode_write_postwrite_authentication")
            .unwrap()[read_calls_index] = 0;
    }
    assert!(validate_lifecycle_metric_policies(&missing_encoding_reads).is_err());

    let mut decreasing_encoding_fsyncs = synthetic_linearity_observations();
    decreasing_encoding_fsyncs[1]
        .phases
        .get_mut("encode_write_postwrite_authentication")
        .unwrap()[fsync_index] = 42;
    assert!(
        validate_lifecycle_metric_policies(&decreasing_encoding_fsyncs).is_err(),
        "decreasing encoding fsync count must fail"
    );

    let mut inflated_encoding_fsyncs = synthetic_linearity_observations();
    inflated_encoding_fsyncs[2]
        .phases
        .get_mut("encode_write_postwrite_authentication")
        .unwrap()[fsync_index] = 44;
    assert!(
        validate_lifecycle_metric_policies(&inflated_encoding_fsyncs).is_err(),
        "inflated encoding fsync count must fail"
    );

    let mut missing_encoding_fsyncs = synthetic_linearity_observations();
    for observation in &mut missing_encoding_fsyncs {
        observation
            .phases
            .get_mut("encode_write_postwrite_authentication")
            .unwrap()[fsync_index] = 0;
    }
    assert!(
        validate_lifecycle_metric_policies(&missing_encoding_fsyncs).is_err(),
        "missing encoding fsync evidence must fail"
    );
}

#[test]
fn equivalent_full_lifecycle_1x_2x_4x_has_bounded_metric_policies() {
    let mut failures = Vec::new();
    for axis in [LinearityAxis::Nodes, LinearityAxis::Edges] {
        let mut observations = Vec::new();
        let mut live_counts = Vec::new();
        for factor in [1_u32, 2, 4] {
            let root = TempDir::new().expect("full lifecycle ladder root");
            let integrated = run_integrated_certification_for_linearity(root.path(), axis, factor);
            let evidence = integrated.value;
            assert_eq!(evidence["source_nodes"], evidence["imported_nodes"]);
            assert_eq!(evidence["source_edges"], evidence["imported_edges"]);
            live_counts.push((
                evidence["source_nodes"]
                    .as_u64()
                    .expect("source node count"),
                evidence["source_edges"]
                    .as_u64()
                    .expect("source edge count"),
            ));
            let attribution: graphforge_storage::ConstructionPhaseAttribution =
                serde_json::from_value(evidence["storage"]["application_io_phases"].clone())
                    .expect("decode phase evidence");
            attribution
                .validate_for_qualification()
                .expect("full lifecycle phase qualification");
            validate_retained_reconciliation(&evidence)
                .expect("full lifecycle retained/category reconciliation");
            let recovery = &evidence["storage"]["application_io_phases"]["phases"]["recovery_reauthentication"];
            assert!(
                recovery["read_bytes"].as_u64().unwrap_or(0) > 0,
                "{axis:?} {factor}x recovery must report authenticated bytes"
            );
            assert!(
                recovery["read_calls"].as_u64().unwrap_or(0) > 0,
                "{axis:?} {factor}x recovery must report authenticated calls"
            );
            observations.push(lifecycle_linearity_observation(&evidence));
            let interrupted = evidence["phases"]
                .as_array()
                .expect("lifecycle phases")
                .iter()
                .find(|phase| phase["id"] == "drill_interrupted_finalization")
                .expect("interrupted-finalization recovery drill");
            assert_eq!(interrupted["status"], "pass");
        }
        match axis {
            LinearityAxis::Nodes => {
                assert_eq!(live_counts[1].0, live_counts[0].0 * 2);
                assert_eq!(live_counts[2].0, live_counts[0].0 * 4);
                assert_eq!(live_counts[0].1, live_counts[1].1);
                assert_eq!(live_counts[0].1, live_counts[2].1);
            }
            LinearityAxis::Edges => {
                assert_eq!(live_counts[0].0, live_counts[1].0);
                assert_eq!(live_counts[0].0, live_counts[2].0);
                assert!(live_counts[0].1 < live_counts[1].1);
                assert!(live_counts[1].1 < live_counts[2].1);
            }
        }
        let observations: [LifecycleLinearityObservation; 3] = observations
            .try_into()
            .expect("exact 1x/2x/4x observations");
        if let Err(error) = validate_lifecycle_metric_policies_for_axis(axis, &observations) {
            failures.push(format!(
                "{axis:?} full lifecycle metric proof failed: {error}; live_counts={live_counts:?}; categories={:?}; phases={:?}; retained={:?}",
                observations.each_ref().map(|observation| &observation.category_metrics),
                observations.each_ref().map(|observation| &observation.phases),
                observations.each_ref().map(|observation| &observation.retained)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn certification_watchdog_persists_typed_first_failure() {
    let root = TempDir::new().expect("watchdog root");
    let allocated = root.path().join("allocated.bin");
    fs::write(&allocated, [0_u8; 4096]).expect("allocated fixture");
    let journal_path = root.path().join("journal.json");
    let mut journal = PhaseJournal::new(
        journal_path.clone(),
        root.path(),
        Envelope {
            rss_bytes: u64::MAX,
            disk_bytes: 0,
            timeout_s: u64::MAX,
        },
    );
    journal.replace_allocation_owner(
        "watchdog_fixture",
        &exact_descriptor_identities(&[allocated]),
    );
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        journal.pass("resource_probe", Instant::now(), None);
    }));
    assert!(failure.is_err());
    assert!(journal.cancellation().load(Ordering::SeqCst));
    let persisted: Vec<Value> =
        serde_json::from_slice(&fs::read(journal_path).expect("persisted failure journal"))
            .expect("failure journal JSON");
    assert_eq!(persisted[0]["status"], "fail");
    assert_eq!(persisted[0]["failure_code"], "disk_limit_exceeded");
}

#[test]
fn target_live_windows_are_deterministic_bounded_and_reconciled() {
    let profile = load_profile();
    let first = TempDir::new().unwrap();
    let second = TempDir::new().unwrap();
    let run = |root: &Path| {
        let cancelled = AtomicBool::new(false);
        let (spills, counts, generated_fingerprint) = generate_target_live_runs(
            &TargetLiveGeneration {
                scale: 10,
                edge_factor: 1,
                initiator: profile.initiator,
                seed: profile.seed,
                buffer_edges: 128,
                target_live_edges: 1_000,
            },
            root,
            &cancelled,
        );
        let mut digest = Sha256::new();
        let replay = merge_runs(&spills.runs, Some(&cancelled), |src, dst| {
            digest.update(src.to_le_bytes());
            digest.update(dst.to_le_bytes());
        })
        .expect("replay merge");
        assert_eq!(counts.live_unique_edges, replay.live_unique_edges);
        assert_eq!(
            spills.raw_attempts,
            counts.live_unique_edges + spills.self_loops_rejected + counts.duplicates_rejected
        );
        assert!(counts.live_unique_edges >= 1_000);
        assert!(spills.peak_buffer_len <= 128);
        assert_eq!(
            generated_fingerprint,
            format!("sha256:{}", hex_encode(digest.finalize()))
        );
        (counts.live_unique_edges, generated_fingerprint)
    };
    assert_eq!(run(first.path()), run(second.path()));
}

#[test]
fn cancellation_stops_merge_and_publication_before_more_work_is_committed() {
    let profile = load_profile();
    let spill_root = TempDir::new().expect("cancellation spill root");
    let spill = generate_spill_runs(
        8,
        4,
        profile.initiator,
        profile.seed,
        64,
        spill_root.path(),
        None,
    );
    let cancelled = AtomicBool::new(true);
    let mut emitted = 0_u64;
    assert!(matches!(
        merge_runs(&spill.runs, Some(&cancelled), |_, _| emitted += 1),
        Err("merge_cancelled")
    ));
    assert_eq!(emitted, 0);

    let project = TempDir::new().expect("cancelled publication project");
    let graph = GraphForge::new(project.path().to_str()).expect("open publication project");
    let before = current_generation_uuid(&graph);
    let mut construction = graph
        .begin_graph_construction(Default::default())
        .expect("begin cancelled construction");
    let stopped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        publish_nodes(&mut construction, 8, Some(&cancelled));
    }));
    assert!(stopped.is_err());
    assert_eq!(current_generation_uuid(&graph), before);
    assert_eq!(
        graph
            .node_count(NODE_LABEL)
            .expect("node count after cancellation"),
        0
    );
}

#[test]
fn graph500_driver_has_no_bulk_publication_escape_hatch() {
    let source = include_str!("scale_g500_ladder.rs");
    for forbidden in [["publish", "bulk"].join("_"), ["bulk", "publish"].join("_")] {
        assert!(
            !source.contains(&forbidden),
            "bulk publication escape hatch reintroduced: {forbidden}"
        );
    }
}

#[test]
fn million_edge_sink_uses_sixteen_durable_chunks_and_replays_stably() {
    let project = TempDir::new().expect("million-edge construction project");
    let graph = GraphForge::new(project.path().to_str()).expect("open million-edge project");
    let budgets = GraphConstructionBudgets::default();
    assert_eq!(CONSTRUCTION_BATCH_ROWS, budgets.max_batch_rows);
    let mut session = graph
        .begin_graph_construction(budgets)
        .expect("begin million-edge construction");
    publish_nodes(&mut session, 2, None);
    let session_uuid = session.session_uuid();
    let mut sink = EdgeSink::new(&mut session, None);
    for _ in 0..1_048_576 {
        sink.push(0, 1);
    }
    sink.flush();
    let first_digest = sink.finish();
    let first = session.progress();
    assert_eq!(
        first.accepted_chunks, 17,
        "one node plus sixteen edge chunks"
    );
    assert_eq!(first.evidence.input_batches, 17);
    assert_eq!(first.evidence.parquet_shards, 17);
    assert_eq!(first.evidence.immutable_artifacts, 67);
    drop(session);

    let mut replay = graph
        .resume_graph_construction(session_uuid, budgets)
        .expect("resume million-edge construction");
    let mut sink = EdgeSink::new(&mut replay, None);
    for _ in 0..1_048_576 {
        sink.push(0, 1);
    }
    sink.flush();
    assert_eq!(sink.finish(), first_digest);
    let replayed = replay.progress();
    assert_eq!(replayed.accepted_chunks, 17);
    assert_eq!(replayed.evidence.input_batches, 17);
    assert_eq!(replayed.evidence.immutable_artifacts, 67);
    assert_eq!(replayed.evidence.replayed_chunks, 16);
    assert_eq!(submitted_chunk_count(&replayed.evidence), 33);
}

fn submitted_chunk_count(evidence: &graphforge_storage::GraphConstructionEvidence) -> u64 {
    evidence
        .input_batches
        .checked_add(evidence.replayed_chunks)
        .expect("submitted construction chunk count overflow")
}

#[test]
fn active_ingest_heartbeat_does_not_recursively_scan_storage() {
    let source = include_str!("scale_g500_ladder.rs");
    let heartbeat = source
        .split("struct IngestHeartbeat")
        .nth(1)
        .and_then(|tail| tail.split("fn run_rung").next())
        .expect("heartbeat source boundary");
    let recursive_probe = ["directory", "bytes"].join("_");
    assert!(
        !heartbeat.contains(&recursive_probe),
        "active heartbeat must consume counters, not enumerate project paths"
    );
    let monitor = source
        .split("struct ResourceMonitor")
        .nth(1)
        .and_then(|tail| tail.split("fn certification_elapsed_before_process").next())
        .expect("resource monitor source boundary");
    for forbidden in ["read_dir", "walkdir", "du\""] {
        assert!(
            !monitor.contains(forbidden),
            "resource monitor must use storage-owned observations, not {forbidden}"
        );
    }
}

#[test]
fn tiny_construction_ladder_resumes_and_scales_bounded_work_linearly() {
    let budgets = GraphConstructionBudgets {
        max_batch_rows: CONSTRUCTION_BATCH_ROWS,
        max_run_records: 4 * CONSTRUCTION_BATCH_ROWS,
        merge_fan_in: 2,
        ..GraphConstructionBudgets::default()
    };
    let base_nodes = CONSTRUCTION_BATCH_ROWS as u64;
    let mut baseline_peaks: Option<[u64; 11]> = None;
    let mut baseline_storage: Option<(u64, u64)> = None;
    let mut baseline_phase_io: Option<(u64, u64, u64, u64)> = None;
    for factor in [1_u64, 2, 4] {
        let project = TempDir::new().expect("tiny construction project");
        let graph = GraphForge::new(project.path().to_str()).expect("open tiny project");
        let before = current_generation_uuid(&graph);
        let session_file = project.path().join("session.uuid");
        let session_uuid = {
            let mut session = open_persisted_construction(&graph, &session_file, budgets);
            publish_nodes(&mut session, base_nodes * factor, None);
            session.session_uuid()
        };
        let mut resumed = graph
            .resume_graph_construction(session_uuid, budgets)
            .expect("resume tiny construction");
        let mut first_edge = EdgeSink::new(&mut resumed, None);
        first_edge.push(0, 1);
        first_edge.flush();
        let _ = first_edge.finish();
        drop(resumed);
        let mut resumed = graph
            .resume_graph_construction(session_uuid, budgets)
            .expect("resume after durable edge chunk");
        let mut sink = EdgeSink::new(&mut resumed, None);
        // Replay the acknowledged chunk from the deterministic input cursor;
        // append authenticates it idempotently instead of minting new IDs.
        sink.push(0, 1);
        sink.flush();
        for node in 1..u32::try_from(base_nodes * factor - 1).expect("tiny vertex count") {
            sink.push(node, node + 1);
        }
        sink.flush();
        let _ = sink.finish();
        let receipt = resumed
            .seal_and_publish()
            .expect("publish tiny construction");
        let progress = resumed.progress();
        assert_eq!(progress.evidence.input_rows, 2 * base_nodes * factor - 1);
        assert_eq!(progress.evidence.input_batches, 2 * factor + 1);
        assert_eq!(
            progress.evidence.parquet_shards,
            progress.evidence.input_batches
        );
        assert_eq!(progress.evidence.immutable_artifacts, 7 * factor + 4);
        // Each full node-detail run (272 * 65,536 bytes = 17 MiB) and
        // edge-detail run (304 * 65,536 bytes = 19 MiB) crosses the 16 MiB
        // per-stream cache window once. The final edge chunk is two rows
        // short and still crosses once; the separately acknowledged one-edge
        // chunk does not. These synchronized rollovers add two real barriers
        // per factor to the original artifact publication protocol on Linux.
        let rollover_fsyncs = u64::from(cfg!(target_os = "linux")) * 2 * factor;
        assert_eq!(
            progress.evidence.fsync_operations,
            23 * factor + 13 + rollover_fsyncs
        );
        assert!(progress.evidence.peak_batch_rows <= CONSTRUCTION_BATCH_ROWS as u64);
        assert!(progress.evidence.peak_accounted_live_bytes <= 64 * 1024 * 1024);
        assert!(progress.evidence.peak_run_records <= budgets.max_run_records as u64);
        assert!(progress.evidence.peak_merge_inputs <= 64);
        assert!(progress.evidence.peak_merge_name_slots <= 64);
        assert!(progress.evidence.peak_resolved_endpoint_name_slots <= 64);
        assert!(progress.evidence.peak_catalog_entries <= 64);
        assert!(progress.evidence.peak_catalog_identifier_bytes <= 64 * 1024);
        let observed_peaks = [
            progress.evidence.peak_batch_rows,
            progress.evidence.peak_batch_bytes,
            progress.evidence.peak_run_records,
            progress.evidence.peak_merge_inputs,
            progress.evidence.peak_merge_temporary_bytes,
            progress.evidence.peak_accounted_live_bytes,
            progress.evidence.peak_merge_name_slots,
            progress.evidence.peak_resolved_endpoint_name_slots,
            progress.evidence.peak_catalog_entries,
            progress.evidence.peak_catalog_identifier_bytes,
            progress.evidence.peak_catalog_decoded_batch_bytes,
        ];
        if let Some(baseline) = baseline_peaks {
            // Arrow buffer accounting includes small alignment/offset metadata
            // differences; the N rung's edge set is N-1 (eight fixed run
            // records below its window). All other saturated windows plateau.
            let allocator_tolerance = [0, 1_024, 8, 0, 0, 4_096, 0, 4, 0, 0, 0];
            for (index, ((observed, base), tolerance)) in observed_peaks
                .iter()
                .zip(baseline)
                .zip(allocator_tolerance)
                .enumerate()
            {
                if index == 4 {
                    // One bounded edge merge window may overlap its immutable
                    // identity, endpoint, and detail inputs with the unified
                    // identity output. These are the construction format's
                    // fixed record widths, so this is a derived byte bound,
                    // not general-purpose disk slack.
                    const IDENTITY_RECORD_BYTES: u64 = 16;
                    const ENDPOINT_RECORD_BYTES: u64 = 48;
                    const EDGE_DETAIL_RECORD_BYTES: u64 = 304;
                    const UNIFIED_IDENTITY_RECORD_BYTES: u64 = 32;
                    let fixed_edge_merge_window_bytes = CONSTRUCTION_BATCH_ROWS as u64
                        * (IDENTITY_RECORD_BYTES
                            + ENDPOINT_RECORD_BYTES
                            + EDGE_DETAIL_RECORD_BYTES
                            + UNIFIED_IDENTITY_RECORD_BYTES);
                    assert!(
                        *observed
                            <= base
                                .checked_mul(factor)
                                .and_then(|bound| {
                                    bound.checked_add(fixed_edge_merge_window_bytes)
                                })
                                .expect("merge footprint bound overflow"),
                        "disk-backed merge footprint exceeded linear work: baseline={base} observed={observed} factor={factor}"
                    );
                    continue;
                }
                if index == 6 {
                    assert!(
                        *observed <= 64,
                        "merge scheduler name slots exceeded fixed bound"
                    );
                    continue;
                }
                assert!(
                    *observed
                        <= base
                            .checked_add(tolerance)
                            .expect("saturated peak tolerance overflow"),
                    "saturated peak field {index} grew with scale: baseline={base} observed={observed} tolerance={tolerance}"
                );
            }
        } else {
            baseline_peaks = Some(observed_peaks);
        }
        assert!(progress.evidence.merge_read_records <= 128 * base_nodes * factor);
        assert!(progress.evidence.merge_written_records <= 128 * base_nodes * factor);
        assert!(progress.evidence.parquet_write_operations > 0);
        assert_ne!(receipt.generation_uuid, before);
        assert_eq!(current_generation_uuid(&graph), receipt.generation_uuid);
        let phases =
            graphforge_storage::ConstructionPhaseAttribution::from_construction(&progress.evidence)
                .unwrap();
        phases.validate_reconciliation().unwrap();
        let shape =
            &phases.phases[&graphforge_storage::StorageIoPhase::ShapeConsumeReauthentication];
        assert!(progress.evidence.merge_read_operations > 0);
        assert!(progress.evidence.merge_write_operations > 0);
        assert_eq!(
            shape.write_bytes,
            progress
                .evidence
                .merge_written_bytes
                .checked_add(progress.evidence.parquet_write_bytes)
                .expect("shape write-byte reconciliation overflow")
        );
        assert_eq!(
            shape.write_calls,
            progress
                .evidence
                .merge_write_operations
                .checked_add(progress.evidence.parquet_write_operations)
                .expect("shape write-operation reconciliation overflow")
        );
        assert_eq!(
            shape.read_calls,
            [
                progress.evidence.shape_input_validation_read_operations,
                progress.evidence.merge_read_operations,
                progress.evidence.parquet_read_operations,
                progress.evidence.shaped_output_authentication_operations,
                progress.evidence.parent_catalog_read_operations,
                progress.evidence.retained_probe_block_loads,
            ]
            .into_iter()
            .try_fold(0_u64, u64::checked_add)
            .expect("shape read-operation reconciliation overflow")
        );
        let phase_observation = (
            phases.totals.read_bytes,
            phases.totals.write_bytes,
            phases.totals.read_calls,
            phases.totals.write_calls,
        );
        if let Some(baseline) = baseline_phase_io {
            // Each lifecycle has fixed authenticated control work. Preserve a
            // documented 2x constant-factor ceiling around ideal linear growth
            // instead of pretending the intercept is zero at the 1x fixture.
            let ceiling = |base: u64| {
                base.checked_mul(factor)
                    .and_then(|bound| bound.checked_mul(2))
                    .expect("phase I/O ceiling overflow")
            };
            assert!(phase_observation.0 <= ceiling(baseline.0));
            assert!(phase_observation.1 <= ceiling(baseline.1));
            assert!(phase_observation.2 <= ceiling(baseline.2));
            assert!(phase_observation.3 <= ceiling(baseline.3));
        } else {
            baseline_phase_io = Some(phase_observation);
        }
        let generation = graphforge_storage::resolve_project_generation(project.path())
            .expect("resolve tiny generation");
        let storage = graphforge_storage::capture_storage_attribution(&generation)
            .expect("capture tiny storage attribution");
        storage
            .validate_reconciliation()
            .expect("reconcile tiny storage attribution");
        assert!(
            storage.is_fully_classified(),
            "unclassified tiny construction storage: {storage:#?}"
        );
        if let Some((base_logical, base_allocated)) = baseline_storage {
            assert!(
                storage.logical_bytes
                    <= base_logical
                        .checked_mul(factor)
                        .expect("storage logical-byte bound overflow"),
                "authenticated logical bytes exceeded linear growth"
            );
            assert!(
                storage.allocated_bytes
                    <= base_allocated
                        .checked_mul(factor)
                        .expect("storage allocation bound overflow"),
                "deduplicated allocated bytes exceeded linear growth"
            );
        } else {
            baseline_storage = Some((storage.logical_bytes, storage.allocated_bytes));
        }
        drop(resumed);
        let replay = graph
            .resume_graph_construction(session_uuid, budgets)
            .expect("resume published tiny construction")
            .seal_and_publish()
            .expect("replay tiny publication");
        assert!(replay.idempotent_replay);
        assert_eq!(replay.generation_uuid, receipt.generation_uuid);
        assert_eq!(current_generation_uuid(&graph), receipt.generation_uuid);
    }
}

#[test]
fn construction_session_reenters_across_processes() {
    if let Ok(workspace) = std::env::var("GF_G500_REENTRY_CHILD") {
        let workspace = PathBuf::from(workspace);
        let project = workspace.join("project");
        fs::create_dir_all(&project).expect("child project");
        let graph = GraphForge::new(project.to_str()).expect("child graph");
        let session_file = workspace.join("construction-session.uuid");
        let mut session =
            open_persisted_construction(&graph, &session_file, GraphConstructionBudgets::default());
        match std::env::var("GF_G500_REENTRY_PHASE").as_deref() {
            Ok("nodes") => publish_nodes(&mut session, 8, None),
            Ok("edges") => {
                let mut sink = EdgeSink::new(&mut session, None);
                sink.push(0, 1);
                sink.push(1, 2);
                sink.flush();
                let _ = sink.finish();
                session.seal_and_publish().expect("child publish");
            }
            Ok("recover") => {
                let receipt = session
                    .seal_and_publish()
                    .expect("recover publication receipt");
                assert!(receipt.idempotent_replay);
            }
            _ => panic!("unknown re-entry phase"),
        }
        return;
    }

    let workspace = TempDir::new().expect("re-entry workspace");
    let run_child = |phase: &str| {
        let status = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "construction_session_reenters_across_processes",
                "--nocapture",
            ])
            .env("GF_G500_REENTRY_CHILD", workspace.path())
            .env("GF_G500_REENTRY_PHASE", phase)
            .status()
            .expect("run re-entry child");
        assert!(status.success(), "re-entry child {phase} failed");
    };
    run_child("nodes");
    run_child("edges");
    let graph = GraphForge::new(workspace.path().join("project").to_str()).expect("reopen result");
    let published = current_generation_uuid(&graph);
    drop(graph);
    run_child("recover");
    let graph = GraphForge::new(workspace.path().join("project").to_str()).expect("reopen result");
    assert_eq!(current_generation_uuid(&graph), published);
    assert_eq!(graph.node_count(NODE_LABEL).unwrap(), 8);
    assert_eq!(scalar_count(&graph.execute(COUNT_EDGES).unwrap()), 2);
}
