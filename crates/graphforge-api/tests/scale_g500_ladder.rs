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
use std::time::Instant;

use arrow::array::{Array, FixedSizeBinaryArray, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use graphforge_api::{GraphForge, OperationId, bulk_edge_input_schema, bulk_node_input_schema};
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
const BATCH_ROWS: usize = 8_192;
const EDGE_PUBLISH_ROWS: usize = 1_048_576;

const ONE_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id LIMIT 1000";
const TWO_HOP: &str = "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id LIMIT 1000";
const COUNT_EDGES: &str = "MATCH ()-[r:LINK]->() RETURN count(r) AS total";

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

fn load_profile() -> ScaleProfile {
    serde_json::from_str(PROFILE_JSON).expect("parse ladder profile fixture")
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

    for _ in 0..raw_attempts {
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
fn merge_runs<F: FnMut(u32, u32)>(runs: &[PathBuf], mut emit: F) -> MergeCounts {
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

    MergeCounts {
        live_unique_edges,
        duplicates_rejected,
    }
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
    );
    let mut edges = Vec::new();
    let mut hasher = Sha256::new();
    let merge = merge_runs(&spill.runs, |src, dst| {
        hasher.update(src.to_le_bytes());
        hasher.update(dst.to_le_bytes());
        edges.push((src, dst));
    });
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

#[allow(clippy::too_many_lines)]
fn run_rung(
    profile: &ScaleProfile,
    rung: &Rung,
    env: RunEnvelope,
    edge_factor: u32,
    ladder_started: Instant,
) -> RungOutcome {
    let started = Instant::now();
    let workspace = TempDir::new().expect("rung workspace");
    let spill_dir = workspace.path().join("spill");
    let project = workspace.path().join("project");
    fs::create_dir_all(&spill_dir).expect("spill dir");
    fs::create_dir_all(&project).expect("project dir");

    let mut steps: Vec<Value> = Vec::new();
    let mut first_failing_phase: Option<&'static str> = None;
    let mut error_class: Option<&'static str> = None;

    // ---- generate ----
    let gen_started = Instant::now();
    let spill = generate_spill_runs(
        rung.scale,
        edge_factor,
        profile.initiator,
        profile.seed,
        rung.buffer_edges,
        &spill_dir,
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

    // ---- ingest (merge + publish through the public facade) ----
    let mut live_unique_edges = 0u64;
    let mut duplicates_rejected = 0u64;
    let mut input_fingerprint = String::from("sha256:");
    let mut ingest_ran = false;
    if first_failing_phase.is_none() {
        let ingest_started = Instant::now();
        let graph = GraphForge::new(Some(project.to_str().expect("utf8 project")))
            .expect("open GraphForge for ingest");
        publish_nodes(&graph, 1u64 << rung.scale);
        let mut sink = EdgeSink::new(&graph);
        let merge = merge_runs(&spill.runs, |src, dst| sink.push(src, dst));
        sink.flush();
        live_unique_edges = merge.live_unique_edges;
        duplicates_rejected = merge.duplicates_rejected;
        input_fingerprint = format!("sha256:{}", hex_encode(sink.finish()));
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
            // NOTE: ingest RSS includes the upstream bulk-publication identity
            // set (see runbook), so an ingest-phase `oom` is not a generator
            // bottleneck. Recorded here so consumers can attribute it.
            "rss_peak_bytes": rss_value(),
            "detail": {
                "live_unique_edges": live_unique_edges,
                "duplicates_rejected": duplicates_rejected,
                "input_fingerprint": input_fingerprint,
            }
        }));
    }

    // ---- reopen + recount ----
    let mut node_count = 0u64;
    let mut edge_count = 0u64;
    let mut gsi = String::new();
    if first_failing_phase.is_none() {
        let reopen_started = Instant::now();
        let graph = GraphForge::new(Some(project.to_str().expect("utf8 project")))
            .expect("reopen GraphForge");
        node_count = graph.node_count(NODE_LABEL).expect("node_count");
        edge_count = scalar_count(&graph.execute(COUNT_EDGES).expect("edge count"));
        let reopen_s = reopen_started.elapsed().as_secs_f64();
        gsi = gsi_undirected(node_count, edge_count);
        steps.push(json!({
            "id": "reopen",
            "pass": true,
            "wall_time_s": reopen_s,
            "rss_peak_bytes": rss_value(),
            "detail": { "node_count": node_count, "edge_count": edge_count, "gsi": gsi }
        }));

        // ---- deterministic LIMIT queries ----
        let hop1_started = Instant::now();
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
        drop(graph);
        if let Some(class) = envelope_violation(&env, ladder_started, &project, &spill_dir) {
            first_failing_phase = Some("query");
            error_class = Some(class);
        }
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
        let outcome = run_rung(profile, rung, env, profile.edgefactor, ladder_started);
        let passed = outcome.passed;
        evidence.push(outcome.evidence);
        if !passed {
            break;
        }
    }
    evidence
}

// ---------------------------------------------------------------------------
// Streaming edge publisher (bounded by EDGE_PUBLISH_ROWS).
// ---------------------------------------------------------------------------

struct EdgeSink<'a> {
    graph: &'a GraphForge,
    buf: Vec<(u32, u32)>,
    chunk_index: u128,
    hasher: Sha256,
}

impl<'a> EdgeSink<'a> {
    fn new(graph: &'a GraphForge) -> Self {
        EdgeSink {
            graph,
            buf: Vec::with_capacity(EDGE_PUBLISH_ROWS),
            chunk_index: 0,
            hasher: Sha256::new(),
        }
    }

    fn push(&mut self, src: u32, dst: u32) {
        self.hasher.update(src.to_le_bytes());
        self.hasher.update(dst.to_le_bytes());
        self.buf.push((src, dst));
        if self.buf.len() >= EDGE_PUBLISH_ROWS {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let schema = bulk_edge_input_schema(Vec::new()).expect("edge schema");
        let mut batches = Vec::new();
        let mut offset = 0usize;
        while offset < self.buf.len() {
            let end = (offset + BATCH_ROWS).min(self.buf.len());
            let slice = &self.buf[offset..end];
            let mut edge_ids = Vec::with_capacity(slice.len());
            let mut sources = Vec::with_capacity(slice.len());
            let mut targets = Vec::with_capacity(slice.len());
            for (local, &(src, dst)) in slice.iter().enumerate() {
                let ordinal = self
                    .chunk_index
                    .saturating_mul(EDGE_PUBLISH_ROWS as u128)
                    .saturating_add(u128::try_from(offset + local).expect("edge ordinal"));
                edge_ids.push(uuidv7(0xE000_0000_0000u128 + ordinal + 1));
                sources.push(uuidv7(u128::from(src) + 1));
                targets.push(uuidv7(u128::from(dst) + 1));
            }
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(
                        FixedSizeBinaryArray::try_from_iter(edge_ids.iter().map(Uuid::as_bytes))
                            .expect("edge_uuid column"),
                    ),
                    Arc::new(StringArray::from(vec![REL_TYPE; slice.len()])),
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
            batches.push(batch);
            offset = end;
        }
        self.graph
            .publish_bulk_edges(OperationId(uuidv7(0xB100 + self.chunk_index)), &batches)
            .expect("publish_bulk_edges");
        self.chunk_index += 1;
        self.buf.clear();
    }

    fn finish(mut self) -> String {
        // Any residual edges must already be flushed by the caller.
        debug_assert!(self.buf.is_empty());
        hex_encode(std::mem::take(&mut self.hasher).finalize())
    }
}

fn publish_nodes(graph: &GraphForge, vertex_count: u64) {
    let total = usize::try_from(vertex_count).expect("vertex count fits usize");
    let schema = bulk_node_input_schema(Vec::new()).expect("node schema");
    let mut offset = 0usize;
    while offset < total {
        let chunk_end = (offset + EDGE_PUBLISH_ROWS).min(total);
        let mut batches = Vec::new();
        let mut inner = offset;
        while inner < chunk_end {
            let end = (inner + BATCH_ROWS).min(chunk_end);
            let count = end - inner;
            let ids = (inner..end)
                .map(|index| uuidv7(u128::try_from(index + 1).expect("node seed")))
                .collect::<Vec<_>>();
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(
                        FixedSizeBinaryArray::try_from_iter(ids.iter().map(Uuid::as_bytes))
                            .expect("node_uuid column"),
                    ),
                    Arc::new(StringArray::from(vec![NODE_LABEL; count])),
                ],
            )
            .expect("node batch");
            batches.push(batch);
            inner = end;
        }
        graph
            .publish_bulk_nodes(OperationId(uuidv7(0xB001_0000 + offset as u128)), &batches)
            .expect("publish_bulk_nodes");
        offset = chunk_end;
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
    let provisioned: Vec<Rung> = profile
        .rungs
        .iter()
        .filter(|r| r.tier == "provisioned")
        .cloned()
        .collect();
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
