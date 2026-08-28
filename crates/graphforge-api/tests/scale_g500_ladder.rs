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
//! large rungs are opt-in via `make bench-g500-ladder`.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
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
use graphforge_core::uuid::Uuid;
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
    target_live_edges: u64,
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
        combined.raw_attempts = combined.raw_attempts.saturating_add(generated.raw_attempts);
        combined.self_loops_rejected = combined
            .self_loops_rejected
            .saturating_add(generated.self_loops_rejected);
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

/// Check the envelope after a phase. Returns `Some(error_class)` on the first
/// violation so the caller can stop the ladder. `ladder_started` is the
/// ladder-level clock so the 4 h wall-clock fail-safe bounds the whole run, not each
/// rung independently.
fn envelope_violation(
    env: &RunEnvelope,
    ladder_started: Instant,
    project: &Path,
    spill: &Path,
) -> Option<&'static str> {
    if peak_rss().is_some_and(|(rss, _)| rss > env.rss_bytes) {
        return Some("oom");
    }
    let disk = directory_bytes(project).unwrap_or(0) + directory_bytes(spill).unwrap_or(0);
    if disk > env.disk_bytes {
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
                    .map(|kb| kb.saturating_mul(1024))
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
        project: &Path,
        spill: &Path,
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
        let project = project.to_path_buf();
        let spill = spill.to_path_buf();
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
                    "disk_used_bytes": directory_bytes(&project).unwrap_or(0)
                        .saturating_add(directory_bytes(&spill).unwrap_or(0)),
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
    let gen_violation = envelope_violation(&env, ladder_started, &project, &spill_dir);
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
        let heartbeat =
            IngestHeartbeat::start(profile, rung, completed_rungs, &steps, &project, &spill_dir);
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
        ingest_ran = true;
        let ingest_s = ingest_started.elapsed().as_secs_f64();
        let ingest_violation = envelope_violation(&env, ladder_started, &project, &spill_dir);
        if let Some(class) = ingest_violation {
            first_failing_phase = Some("ingest");
            error_class = Some(class);
        }
        steps.push(json!({
            "id": "ingest",
            "pass": ingest_violation.is_none(),
            "wall_time_s": ingest_s,
            "rss_peak_bytes": rss_value(),
            "disk_used_bytes": directory_bytes(&project).unwrap_or(0)
                .saturating_add(directory_bytes(&spill_dir).unwrap_or(0)),
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
                },
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

    // ---- reopen + recount ----
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
        node_count = graph.node_count(NODE_LABEL).expect("node_count");
        edge_count = scalar_count(&graph.execute(COUNT_EDGES).expect("edge count"));
        let reopen_s = reopen_started.elapsed().as_secs_f64();
        gsi = gsi_undirected(node_count, edge_count);
        let reopen_violation = envelope_violation(&env, ladder_started, &project, &spill_dir);
        if let Some(class) = reopen_violation {
            first_failing_phase = Some("reopen");
            error_class = Some(class);
        }
        steps.push(json!({
            "id": "reopen",
            "pass": reopen_violation.is_none(),
            "wall_time_s": reopen_s,
            "rss_peak_bytes": rss_value(),
            "detail": { "node_count": node_count, "edge_count": edge_count, "gsi": gsi }
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

        // ---- deterministic LIMIT queries ----
        if first_failing_phase.is_none() {
            let hop1_started = Instant::now();
            persist_phase_journal(
                profile,
                rung,
                completed_rungs,
                "query",
                "running",
                &steps,
                None,
            );
            let hop1 = graph.execute(ONE_HOP).expect("one-hop LIMIT");
            let hop1_rows = row_count(&hop1);
            steps.push(json!({
                "id": "cypher_limit_1hop",
                "pass": hop1_rows <= 1_000,
                "wall_time_s": hop1_started.elapsed().as_secs_f64(),
                "detail": { "rows": hop1_rows }
            }));

            let hop2_started = Instant::now();
            let hop2 = graph.execute(TWO_HOP).expect("two-hop LIMIT");
            let hop2_rows = row_count(&hop2);
            steps.push(json!({
                "id": "cypher_limit_2hop",
                "pass": hop2_rows <= 1_000,
                "wall_time_s": hop2_started.elapsed().as_secs_f64(),
                "detail": { "rows": hop2_rows }
            }));
            let query_violation = envelope_violation(&env, ladder_started, &project, &spill_dir);
            if let Some(class) = query_violation {
                first_failing_phase = Some("query");
                error_class = Some(class);
            }
            persist_phase_journal(
                profile,
                rung,
                completed_rungs,
                "query",
                if query_violation.is_some() {
                    "phase_failed"
                } else {
                    "phase_completed"
                },
                &steps,
                first_failing_phase.zip(error_class),
            );
        }
        drop(graph);
    }

    let disk_used_bytes =
        directory_bytes(&project).unwrap_or(0) + directory_bytes(&spill_dir).unwrap_or(0);
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

/// Drive the ladder rung-by-rung, stopping at the first failing rung.
fn run_ladder(profile: &ScaleProfile, env: RunEnvelope, rungs: &[Rung]) -> Vec<Value> {
    let ladder_started = Instant::now();
    let mut evidence = Vec::new();
    for rung in rungs {
        let outcome = run_rung(
            profile,
            rung,
            env,
            profile.edgefactor,
            ladder_started,
            &evidence,
        );
        let passed = outcome.passed;
        evidence.push(outcome.evidence);
        let first_failing_phase = evidence
            .last()
            .and_then(|rung| rung["first_failing_phase"].as_str());
        let error_class = evidence
            .last()
            .and_then(|rung| rung["error_class"].as_str());
        persist_phase_journal(
            profile,
            rung,
            &evidence,
            "rung",
            "rung_completed",
            &[],
            first_failing_phase.zip(error_class),
        );
        if !passed {
            break;
        }
    }
    evidence
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
    chunk_index: u128,
    hasher: Sha256,
}

impl<'a, 'graph> EdgeSink<'a, 'graph> {
    fn new(
        session: &'a mut GraphConstructionSession<'graph>,
        cancellation: Option<&'a AtomicBool>,
    ) -> Self {
        EdgeSink {
            session,
            cancellation,
            buf: Vec::with_capacity(CONSTRUCTION_BATCH_ROWS),
            chunk_index: 0,
            hasher: Sha256::new(),
        }
    }

    fn push(&mut self, src: u32, dst: u32) {
        self.check_cancellation();
        self.hasher.update(src.to_le_bytes());
        self.hasher.update(dst.to_le_bytes());
        self.buf.push((src, dst));
        if self.buf.len() >= CONSTRUCTION_BATCH_ROWS {
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
                .saturating_mul(CONSTRUCTION_BATCH_ROWS as u128)
                .saturating_add(u128::try_from(local).expect("edge ordinal"));
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
            u64::try_from(self.chunk_index).unwrap_or(u64::MAX),
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
    let total = usize::try_from(vertex_count).expect("vertex count fits usize");
    let mut offset = 0usize;
    while offset < total {
        assert!(
            !cancellation.is_some_and(|flag| flag.load(Ordering::SeqCst)),
            "node publication cancelled"
        );
        let end = (offset + CONSTRUCTION_BATCH_ROWS).min(total);
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

fn directory_bytes(path: &Path) -> std::io::Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    if path.is_file() {
        return Ok(path.metadata()?.len());
    }
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        total += if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            metadata.len()
        };
    }
    Ok(total)
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
                return Some((kb.saturating_mul(1024), "vmhwm"));
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
    Some((kb.saturating_mul(1024), "ps_sampled"))
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

/// Provisioned full ladder (SCALE-20 → SCALE-26). Opt-in via
/// `make bench-g500-ladder`. Writes one evidence object per attempted rung and
/// stops at the first rung that exceeds the declared 128 GiB / 1 TiB / 4 h
/// cloud-SKU fail-safe. Never asserts a billion-edge product claim (that is #745).
/// Certification evidence for #745 must come from a provisioned Linux cloud host.
#[test]
#[ignore = "Provisioned billion-edge scale ladder; make bench-g500-ladder"]
fn ladder_public_facade_first_fail_evidence() {
    let profile = load_profile();
    let max_scale = std::env::var("GF_G500_LADDER_MAX_SCALE")
        .expect("GF_G500_LADDER_MAX_SCALE must explicitly cap the authorized ladder")
        .parse::<u32>()
        .expect("GF_G500_LADDER_MAX_SCALE must be an integer");
    let provisioned =
        provisioned_rungs_through(&profile, max_scale).unwrap_or_else(|error| panic!("{error}"));
    let evidence = run_ladder(&profile, profile.envelope.into(), &provisioned);
    assert!(
        !evidence.is_empty(),
        "ladder must attempt at least one rung"
    );

    let out = std::env::var("GF_G500_LADDER_EVIDENCE_OUT").map_or_else(
        |_| PathBuf::from("build/g500-ladder-evidence.json"),
        PathBuf::from,
    );
    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent).expect("evidence parent");
    }
    fs::write(
        &out,
        serde_json::to_vec_pretty(&json!({
            "schema": EVIDENCE_SCHEMA,
            "schema_version": SCHEMA_VERSION,
            "profile_schema": profile.schema,
            "authorized_max_scale": max_scale,
            "rungs": evidence,
        }))
        .expect("serialize ladder evidence"),
    )
    .expect("write ladder evidence");

    // Every rung that reached ingest reconciles; a rung stopped in the generate
    // phase reports reconciles=null (not evaluated). The ladder stops at the
    // first failure.
    for rung in &evidence {
        let rec = &rung["reconciles"];
        assert!(
            rec.is_null() || rec == &Value::Bool(true),
            "an evaluated rung must reconcile; got {rec}"
        );
    }
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

const CERTIFICATION_PHASES: [&str; 17] = [
    "preflight",
    "generate",
    "ingest",
    "csr",
    "source_reopen",
    "source_query_1hop",
    "source_query_2hop",
    "export",
    "verify",
    "import",
    "imported_reopen",
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
}

impl PhaseJournal {
    fn new(path: PathBuf, workspace: &Path, envelope: Envelope) -> Self {
        Self {
            path,
            phases: Vec::new(),
            monitor: ResourceMonitor::start(workspace.to_path_buf(), envelope),
        }
    }

    fn pass(&mut self, id: &str, started: Instant, fingerprint: Option<String>) {
        let fingerprint = fingerprint.map_or(Value::Null, Value::String);
        self.monitor.sample_disk();
        if let Some(code) = self.monitor.failure_code() {
            self.phases.push(json!({
                "id": id, "status": "fail",
                "elapsed_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                "rss_peak_bytes": self.monitor.peak_rss.load(Ordering::Relaxed),
                "disk_peak_bytes": self.monitor.peak_disk.load(Ordering::Relaxed),
                "fingerprint": fingerprint, "failure_code": code,
            }));
            self.flush();
            panic!("certification resource watchdog stopped phase {id}: {code}");
        }
        let rss_peak_bytes = self.monitor.peak_rss.swap(0, Ordering::SeqCst);
        let disk_peak_bytes = self.monitor.peak_disk.swap(0, Ordering::SeqCst);
        self.phases.push(json!({
            "id": id, "status": "pass",
            "elapsed_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "rss_peak_bytes": rss_peak_bytes,
            "disk_peak_bytes": disk_peak_bytes,
            "fingerprint": fingerprint,
            "failure_code": null,
        }));
        self.flush();
    }

    fn cancellation(&self) -> &AtomicBool {
        self.monitor.cancellation.flag()
    }

    fn cancellation_token(&self) -> CancellationToken {
        self.monitor.cancellation.clone()
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
            self.monitor.sample_disk();
            self.phases.push(json!({
                "id": CERTIFICATION_PHASES[self.phases.len()], "status": "fail",
                "elapsed_ms": 0,
                "rss_peak_bytes": self.monitor.peak_rss.load(Ordering::Relaxed),
                "disk_peak_bytes": self.monitor.peak_disk.load(Ordering::Relaxed),
                "fingerprint": null, "failure_code": code,
            }));
            self.flush();
        }
    }
}

struct ResourceMonitor {
    workspace: PathBuf,
    cancellation: CancellationToken,
    stop: Arc<AtomicBool>,
    peak_rss: Arc<AtomicU64>,
    peak_disk: Arc<AtomicU64>,
    failure: Arc<AtomicU64>,
    envelope: Envelope,
    worker: Option<JoinHandle<()>>,
}

impl ResourceMonitor {
    fn start(workspace: PathBuf, envelope: Envelope) -> Self {
        let initial_rss = current_rss_bytes().expect("certification host must expose process RSS");
        let initial_disk = allocated_bytes(&workspace)
            .expect("certification host must expose allocated disk bytes");
        let cancellation = CancellationToken::new();
        let stop = Arc::new(AtomicBool::new(false));
        let peak_rss = Arc::new(AtomicU64::new(initial_rss));
        let peak_disk = Arc::new(AtomicU64::new(initial_disk));
        let failure = Arc::new(AtomicU64::new(0));
        let worker_cancellation = cancellation.clone();
        let worker_stop = Arc::clone(&stop);
        let worker_peak_rss = Arc::clone(&peak_rss);
        let worker_peak_disk = Arc::clone(&peak_disk);
        let worker_failure = Arc::clone(&failure);
        let worker_workspace = workspace.clone();
        let started = Instant::now();
        let elapsed_before_process = certification_elapsed_before_process();
        let worker = thread::spawn(move || {
            let mut samples = 0_u8;
            while !worker_stop.load(Ordering::Relaxed) {
                let rss = current_rss_bytes().expect("certification RSS probe failed");
                worker_peak_rss.fetch_max(rss, Ordering::Relaxed);
                let mut code = if rss > envelope.rss_bytes {
                    1
                } else if elapsed_before_process
                    .saturating_add(started.elapsed())
                    .as_secs()
                    > envelope.timeout_s
                {
                    3
                } else {
                    0
                };
                if samples == 0 {
                    let disk = allocated_bytes(&worker_workspace)
                        .expect("certification disk probe failed");
                    worker_peak_disk.fetch_max(disk, Ordering::Relaxed);
                    if disk > envelope.disk_bytes {
                        code = 2;
                    }
                }
                if code != 0 {
                    worker_failure
                        .compare_exchange(0, code, Ordering::SeqCst, Ordering::Relaxed)
                        .ok();
                    worker_cancellation.cancel();
                    break;
                }
                samples = (samples + 1) % 20;
                thread::sleep(Duration::from_millis(250));
            }
        });
        Self {
            workspace,
            cancellation,
            stop,
            peak_rss,
            peak_disk,
            failure,
            envelope,
            worker: Some(worker),
        }
    }

    fn sample_disk(&self) {
        let disk = allocated_bytes(&self.workspace).expect("certification disk probe failed");
        self.peak_disk.fetch_max(disk, Ordering::Relaxed);
        if disk > self.envelope.disk_bytes {
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
        .map(|kibibytes| kibibytes.saturating_mul(1024))
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

fn allocated_bytes(path: &Path) -> Result<u64, &'static str> {
    let output = Command::new("du").arg("-sk").arg(path).output();
    output
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| {
            String::from_utf8(out.stdout)
                .ok()?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()
        })
        .map(|kibibytes| kibibytes.saturating_mul(1024))
        .ok_or("allocated disk usage is unavailable")
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

fn create_bounded_drill_package(root: &Path, limits: PortableV2Limits) -> (PathBuf, String) {
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
    drop(construction);
    drop(graph);
    let graph = GraphForge::new(project.to_str()).expect("reopen bounded drill project");
    let expanded = root.join("drill-expanded");
    graph
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
            input: expanded,
            mode: PortableV2Mode::Full,
            limits,
        },
        None,
    )
    .expect("verify compact drill expanded package");
    let cancelled = AtomicBool::new(true);
    let cancelled_path = root.join("drill-cancelled.gfpb");
    assert!(
        graph
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
            .is_err()
    );
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
    (package, receipt.package_digest)
}

#[allow(clippy::too_many_lines)]
fn run_integrated_certification(root: &Path, target_live: Option<u64>) -> Value {
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
        4
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
    journal.pass("generate", phase, Some(generation_fingerprint.clone()));

    let phase = Instant::now();
    let graph = GraphForge::new(source.to_str()).expect("open certification source");
    let mut construction = graph
        .begin_graph_construction(Default::default())
        .expect("begin certification construction");
    publish_nodes(
        &mut construction,
        1u64 << scale,
        Some(journal.cancellation()),
    );
    let mut sink = EdgeSink::new(&mut construction, Some(journal.cancellation()));
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
    journal.pass("ingest", phase, Some(input_fingerprint));

    let phase = Instant::now();
    let csr = graph
        .rebuild_adjacency(Some(journal.cancellation_token()))
        .expect("build certification CSR");
    journal.pass(
        "csr",
        phase,
        csr.artifact_fingerprint
            .map(|value| format!("sha256:{value}")),
    );
    drop(graph);

    let phase = Instant::now();
    let graph = GraphForge::new(source.to_str()).expect("reopen source");
    let source_nodes = graph.node_count(NODE_LABEL).expect("source nodes");
    let source_edges = scalar_count(&graph.execute(COUNT_EDGES).expect("source edges"));
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
    journal.pass("source_reopen", phase, None);
    let phase = Instant::now();
    let source_1hop = result_fingerprint(&graph.execute(ONE_HOP).expect("source 1hop"));
    journal.pass("source_query_1hop", phase, Some(source_1hop.clone()));
    let phase = Instant::now();
    let source_2hop = result_fingerprint(&graph.execute(TWO_HOP).expect("source 2hop"));
    let source_authority_fingerprint = authority_fingerprint(&graph);
    let source_generation = current_generation_uuid(&graph);
    journal.pass("source_query_2hop", phase, Some(source_2hop.clone()));

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
    assert_ne!(exported.generation_uuid, imported_receipt.generation_uuid);
    journal.pass(
        "import",
        phase,
        Some(imported_receipt.package_digest.clone()),
    );
    let phase = Instant::now();
    let imported_graph = GraphForge::new(imported.to_str()).expect("reopen import");
    let imported_nodes = imported_graph
        .node_count(NODE_LABEL)
        .expect("imported nodes");
    let imported_edges =
        scalar_count(&imported_graph.execute(COUNT_EDGES).expect("imported edges"));
    assert_eq!(
        (source_nodes, source_edges),
        (imported_nodes, imported_edges)
    );
    journal.pass("imported_reopen", phase, None);
    let phase = Instant::now();
    let imported_1hop = result_fingerprint(&imported_graph.execute(ONE_HOP).expect("import 1hop"));
    assert_eq!(source_1hop, imported_1hop);
    journal.pass("imported_query_1hop", phase, Some(imported_1hop.clone()));
    let phase = Instant::now();
    let imported_2hop = result_fingerprint(&imported_graph.execute(TWO_HOP).expect("import 2hop"));
    let imported_authority_fingerprint = authority_fingerprint(&imported_graph);
    assert_eq!(
        current_generation_uuid(&imported_graph),
        imported_receipt.generation_uuid
    );
    assert_eq!(source_2hop, imported_2hop);
    assert_eq!(source_authority_fingerprint, imported_authority_fingerprint);
    journal.pass("imported_query_2hop", phase, Some(imported_2hop.clone()));

    // Representative drills use the same verifier/import boundaries but never
    // repeat the billion-edge payload.
    let phase = Instant::now();
    let (drill_package, drill_digest) = create_bounded_drill_package(root, limits);
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
    assert!(
        GraphForge::import_portable_v2(
            &interrupted,
            &PortableV2ImportRequest {
                input: drill_package,
                operation_id: OperationId(uuidv7(0x746)),
                limits,
            },
            Some(&AtomicBool::new(true))
        )
        .is_err()
    );
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
    json!({
        "source_generation": exported.generation_uuid.to_string(),
        "package": exported.package_digest, "transport": exported.transport_digest,
        "imported_generation": imported_receipt.generation_uuid.to_string(),
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
        "phases": journal.phases,
    })
}

#[test]
fn certification_lifecycle_journals_equivalent_round_trip_and_drills() {
    let root = TempDir::new().expect("certification smoke root");
    let evidence = run_integrated_certification(root.path(), None);
    assert_eq!(evidence["source_edges"], evidence["imported_edges"]);
    assert_ne!(
        evidence["source_generation"],
        evidence["imported_generation"]
    );
}

#[test]
fn certification_watchdog_persists_typed_first_failure() {
    let root = TempDir::new().expect("watchdog root");
    fs::write(root.path().join("allocated.bin"), [0_u8; 4096]).expect("allocated fixture");
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
        .saturating_add(evidence.replayed_chunks)
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
        assert_eq!(progress.evidence.fsync_operations, 23 * factor + 13);
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
                                .saturating_mul(factor)
                                .saturating_add(fixed_edge_merge_window_bytes),
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
                    *observed <= base.saturating_add(tolerance),
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

#[test]
#[ignore = "requires approved 128 GiB / 1 TiB Linux certification host"]
fn certification_target_live_full_lifecycle_evidence() {
    let elapsed_before_process = certification_elapsed_before_process();
    let started = Instant::now();
    let profile = load_certification_profile();
    let root = TempDir::new().expect("certification workspace");
    let lifecycle = run_integrated_certification(root.path(), Some(profile.target_live_edges));
    let phases = lifecycle["phases"].as_array().expect("phase array");
    let peak_rss = phases
        .iter()
        .filter_map(|p| p["rss_peak_bytes"].as_u64())
        .max()
        .unwrap_or(0);
    let peak_disk = phases
        .iter()
        .filter_map(|p| p["disk_peak_bytes"].as_u64())
        .max()
        .unwrap_or(0);
    let source_edges = lifecycle["source_edges"].as_u64().unwrap();
    let generated_live_edges = lifecycle["generated_live_unique_edges"].as_u64().unwrap();
    assert!(source_edges >= 1_000_000_000);
    assert_eq!(source_edges, generated_live_edges);
    let profile_digest = format!(
        "sha256:{}",
        hex_encode(Sha256::digest(include_bytes!(
            "fixtures/scale_g500_certification.v1.json"
        )))
    );
    let evidence = json!({
        "schema": "graphforge-billion-edge-certification-evidence/1",
        "git_sha": std::env::var("GF_G500_CERT_EXPECTED_SHA").unwrap_or_else(|_| git_sha().as_str().unwrap_or("unknown").to_owned()),
        "profile_sha256": profile_digest,
        "run": {
            "command": "cargo test -p graphforge-api --release --test scale_g500_ladder certification_target_live_full_lifecycle_evidence -- --ignored --exact --nocapture --test-threads=1",
            "scale": profile.scale, "edgefactor": profile.edgefactor, "seed": profile.seed,
            "directionality": "undirected", "self_loops": "drop", "duplicates": "drop"
        },
        "host": {
            "provider": std::env::var("GF_G500_CERT_PROVIDER").expect("approved provider input"),
            "region": std::env::var("GF_G500_CERT_REGION").expect("approved region input"),
            "sku": std::env::var("GF_G500_CERT_SKU").expect("approved SKU input"),
            "os_image": std::env::var("GF_G500_CERT_OS_IMAGE").expect("approved OS image input"),
            "os": command_text("uname", &["-s"]),
            "kernel": command_text("uname", &["-r"]),
            "filesystem": normalized_filesystem(root.path()),
            "memory_bytes": linux_memory_bytes(),
            "nvme_bytes": filesystem_capacity_bytes(root.path()),
        },
        "tools": { "rustc": command_text("rustc", &["--version"]), "cargo": command_text("cargo", &["--version"]) },
        "counts": {
            "raw_attempts": lifecycle["raw_attempts"], "self_loops_rejected": lifecycle["self_loops_rejected"],
            "duplicates_rejected": lifecycle["duplicates_rejected"], "live_unique_edges": generated_live_edges,
            "source_nodes": lifecycle["source_nodes"], "source_edges": source_edges,
            "imported_nodes": lifecycle["imported_nodes"], "imported_edges": lifecycle["imported_edges"],
        },
        "identities": { "source_generation": lifecycle["source_generation"], "package": lifecycle["package"], "transport": lifecycle["transport"], "imported_generation": lifecycle["imported_generation"] },
        "package": {
            "contract": lifecycle["portable_contract"], "format": "portable-project-v2-bundle",
            "class": lifecycle["package_class"], "integrity": lifecycle["integrity"],
            "compatibility": lifecycle["compatibility"],
            "policy": "complete-current-generation"
        },
        "equivalence": { "source_project_fingerprint": lifecycle["source_project_fingerprint"], "imported_project_fingerprint": lifecycle["imported_project_fingerprint"] },
        "authority": { "source_fingerprint": lifecycle["source_authority_fingerprint"], "imported_fingerprint": lifecycle["imported_authority_fingerprint"] },
        "phases": phases,
        "envelope": { "peak_rss_bytes": peak_rss, "peak_disk_bytes": peak_disk, "wall_time_s": elapsed_before_process.saturating_add(started.elapsed()).as_secs_f64() },
        "result": "pass", "first_failure": null,
    });
    let out = PathBuf::from(std::env::var("GF_G500_CERT_EVIDENCE_OUT").expect("evidence output"));
    fs::write(out, serde_json::to_vec_pretty(&evidence).unwrap())
        .expect("write certification evidence");
}

fn normalized_filesystem(path: &Path) -> String {
    match command_text("stat", &["-f", "-c", "%T", path.to_str().unwrap()]).as_str() {
        "ext2/ext3" => "ext4".to_owned(),
        value => value.to_owned(),
    }
}

fn command_text(program: &str, args: &[&str]) -> String {
    let output = Command::new(program)
        .args(args)
        .output()
        .expect("host attestation command");
    assert!(output.status.success(), "host attestation command failed");
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn linux_memory_bytes() -> u64 {
    fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|line| line.starts_with("MemTotal:"))?
                .split_whitespace()
                .nth(1)?
                .parse::<u64>()
                .ok()
        })
        .unwrap_or(0)
        .saturating_mul(1024)
}

fn filesystem_capacity_bytes(path: &Path) -> u64 {
    command_text("df", &["-k", "--output=size", path.to_str().unwrap()])
        .lines()
        .last()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_mul(1024)
}
