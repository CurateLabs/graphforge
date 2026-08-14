//! Official-parameter Graph500 SCALE engineering green on the public facade (#710).
//!
//! SCALE-6 is required CI. SCALE-20 is ignored / `make bench-g500-scale20`.
//! This is **not** Official-track: the generator is a bench-local Kronecker,
//! not the pinned `github.com/graph500/graph500` binary. `teps` stays null;
//! GraphForge `paths(by="bfs")` is not the Graph500 BFS kernel.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    Array, FixedSizeBinaryArray, Float64Array, Int64Array, StringArray, UInt64Array,
};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    ExecutionResourcePolicy, GraphForge, GraphForgeOptions, OperationId, RankAlgorithm,
    RankOptions, ResourcePolicyMode, SpillPolicy, bulk_edge_input_schema, bulk_node_input_schema,
};
use graphforge_core::uuid::Uuid;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const EVIDENCE_SCHEMA: &str = "graphforge-official-parameter-scale20/1";
const SCHEMA_VERSION: &str = "1";
const GENERATOR_NAME: &str = "graphforge-kronecker-rmat";
const GENERATOR_SOURCE: &str = "crates/graphforge-api/tests/scale_g500_scale20.rs";
const KRONECKER_A: f64 = 0.57;
const KRONECKER_B: f64 = 0.19;
const KRONECKER_C: f64 = 0.19;
const EDGE_FACTOR: u32 = 16;
const KRONECKER_SEED: u64 = 1;
const BATCH_ROWS: usize = 8_192;
const EDGE_PUBLISH_ROWS: usize = 1_048_576;
const NODE_LABEL: &str = "Node";
const REL_TYPE: &str = "LINK";
const ONE_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id LIMIT 1000";
const TWO_HOP: &str = "MATCH (a)-[r1]->(b)-[r2]->(c) RETURN c.node_uuid AS id LIMIT 1000";
const COUNT_EDGES: &str = "MATCH ()-[r:LINK]->() RETURN count(r) AS total";

struct KroneckerGraph {
    vertex_count: u64,
    edges: Vec<(u32, u32)>,
    checksum: String,
}

struct StepTiming {
    id: &'static str,
    wall_time_s: f64,
    extra: Value,
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

#[test]
fn gsi_grades_documented_bands() {
    assert_eq!(gsi_undirected(0, 0), "GU-01-XS-D00");
    assert_eq!(gsi_undirected(64, 1_024), "GU-01-XS-D51");
    assert_eq!(gsi_undirected(256, 4_096), "GU-02-XS-D13");
    assert_eq!(gsi_undirected(1_048_576, 15_701_399), "GU-06-MD-D00");
}

#[test]
fn kronecker_scale6_drops_self_loops_and_dedups() {
    let graph = generate_kronecker(6, EDGE_FACTOR, KRONECKER_SEED);
    assert_eq!(graph.vertex_count, 64);
    assert!(
        graph.edges.len() < 1_024,
        "self-loops and duplicates must drop"
    );
    assert!(
        graph.edges.windows(2).all(|pair| pair[0] < pair[1]),
        "unique undirected pairs must be strictly sorted"
    );
    assert!(graph.edges.iter().all(|(src, dst)| src < dst));
    assert!(graph.checksum.starts_with("sha256:"));
    assert_eq!(graph.checksum.len(), "sha256:".len() + 64);
}

#[test]
fn scale6_smoke_public_facade_engineering_green() {
    let evidence = run_engineering_green(6, "GU-01-XS-", None);
    assert_eq!(evidence["schema"], EVIDENCE_SCHEMA);
    assert_eq!(evidence["schema_version"], SCHEMA_VERSION);
    assert!(evidence["track"].is_null(), "must not set track: official");
    assert_eq!(evidence["generator"]["name"], GENERATOR_NAME);
    assert_ne!(
        evidence["generator"]["source"],
        "https://github.com/graph500/graph500"
    );
    assert!(evidence["teps"].is_null());
    assert_eq!(evidence["pass"], true);
}

#[test]
#[ignore = "Official-parameter SCALE-20 ingest/reopen/LIMIT; make bench-g500-scale20"]
fn scale20_public_facade_engineering_green() {
    let out = evidence_out_path();
    let evidence = run_engineering_green(20, "GU-06-MD-", Some(out.as_path()));
    assert_eq!(evidence["schema"], EVIDENCE_SCHEMA);
    assert!(evidence["track"].is_null());
    assert_eq!(evidence["pass"], true);
    assert!(
        out.is_file(),
        "evidence JSON must be written to {}",
        out.display()
    );
}

#[allow(clippy::too_many_lines)]
fn run_engineering_green(
    scale: u32,
    expected_gsi_prefix: &str,
    evidence_out: Option<&Path>,
) -> Value {
    let generate_started = Instant::now();
    let generated = generate_kronecker(scale, EDGE_FACTOR, KRONECKER_SEED);
    let generate_s = generate_started.elapsed().as_secs_f64();
    assert_eq!(generated.vertex_count, 1u64 << scale);

    let workspace = TempDir::new().expect("temp workspace");
    let project = workspace.path().join("project");
    fs::create_dir(&project).expect("project directory");

    let ingest_started = Instant::now();
    ingest_graph(&project, &generated);
    let ingest_s = ingest_started.elapsed().as_secs_f64();

    let reopen_started = Instant::now();
    let graph = GraphForge::new(Some(project.to_str().expect("utf8 project path")))
        .expect("reopen GraphForge");
    let node_count = graph
        .node_count(NODE_LABEL)
        .expect("node_count after reopen");
    let edge_count = scalar_count(&graph.execute(COUNT_EDGES).expect("edge count"));
    let reopen_s = reopen_started.elapsed().as_secs_f64();
    assert_eq!(node_count, generated.vertex_count);
    assert_eq!(
        edge_count,
        u64::try_from(generated.edges.len()).expect("edge count")
    );

    let gsi = gsi_undirected(node_count, edge_count);
    assert!(
        gsi.starts_with(expected_gsi_prefix),
        "measured GSI {gsi} must start with {expected_gsi_prefix}"
    );
    let density = undirected_density(node_count, edge_count);
    let density_code = density_code(density);

    let hop1_started = Instant::now();
    let hop1 = graph.execute(ONE_HOP).expect("one-hop LIMIT");
    let hop1_s = hop1_started.elapsed().as_secs_f64();
    let hop1_rows = row_count(&hop1);
    assert!(
        hop1_rows <= 1_000,
        "one-hop LIMIT must cap at 1000, got {hop1_rows}"
    );
    assert!(
        hop1_rows > 0,
        "one-hop LIMIT must return rows on a non-empty graph"
    );

    let hop2_started = Instant::now();
    let hop2 = graph.execute(TWO_HOP).expect("two-hop LIMIT");
    let hop2_s = hop2_started.elapsed().as_secs_f64();
    let hop2_rows = row_count(&hop2);
    assert!(
        hop2_rows <= 1_000,
        "two-hop LIMIT must cap at 1000, got {hop2_rows}"
    );
    drop(graph);

    let rank = degree_thread_parity(&project);

    let disk_used_bytes = directory_bytes(&project).unwrap_or(0);
    let steps = vec![
        StepTiming {
            id: "generate",
            wall_time_s: generate_s,
            extra: json!({
                "raw_edges": u128::from(generated.vertex_count) * u128::from(EDGE_FACTOR),
                "unique_undirected_edges": generated.edges.len(),
            }),
        },
        StepTiming {
            id: "ingest",
            wall_time_s: ingest_s,
            extra: json!({ "publish_edge_chunk_rows": EDGE_PUBLISH_ROWS }),
        },
        StepTiming {
            id: "reopen",
            wall_time_s: reopen_s,
            extra: json!({ "node_count": node_count, "edge_count": edge_count, "gsi": gsi }),
        },
        StepTiming {
            id: "cypher_limit_1hop",
            wall_time_s: hop1_s,
            extra: json!({ "rows": hop1_rows }),
        },
        StepTiming {
            id: "cypher_limit_2hop",
            wall_time_s: hop2_s,
            extra: json!({ "rows": hop2_rows }),
        },
        StepTiming {
            id: "rank_degree_thread_parity",
            wall_time_s: rank.wall_time_s,
            extra: json!({
                "fingerprint": rank.fingerprint,
                "rows": rank.rows,
            }),
        },
    ];
    let wall_time_s = steps.iter().map(|step| step.wall_time_s).sum::<f64>();
    let evidence = json!({
        "schema": EVIDENCE_SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "track": null,
        "gsi": gsi,
        "scale": scale,
        "edgefactor": EDGE_FACTOR,
        "density_tier": null,
        "target_density": null,
        "measured_density": density,
        "density_code": density_code,
        "ldbc": null,
        "workloads_run": [
            "generate",
            "ingest",
            "reopen",
            "cypher_limit_1hop",
            "cypher_limit_2hop",
            "rank_degree_thread_parity"
        ],
        "pass": true,
        "error_class": null,
        "wall_time_s": wall_time_s,
        "rss_peak_bytes": null,
        "disk_used_bytes": disk_used_bytes,
        "artifact_checksums": {
            "edges": generated.checksum,
        },
        "generator": {
            "name": GENERATOR_NAME,
            "source": GENERATOR_SOURCE,
            "version": "1",
            "commit": null,
            "seed": KRONECKER_SEED,
            "initiator": { "A": KRONECKER_A, "B": KRONECKER_B, "C": KRONECKER_C, "D": 0.05 }
        },
        "driver": null,
        "sut": {
            "name": "graphforge",
            "version": env!("CARGO_PKG_VERSION"),
            "git_sha": git_sha(),
        },
        "machine_envelope": {
            "disk_bytes": 1_099_511_627_776u64,
            "rss_bytes": 68_719_476_736u64,
            "timeout_s": 7_200
        },
        "teps": null,
        "notes": "Official-parameter engineering green. Not Official-track and not TEPS. Generator is bench-local Kronecker, not the pinned graph500 binary. GraphForge paths(by=\"bfs\") is not the Graph500 BFS kernel.",
        "steps": steps.iter().map(|step| json!({
            "id": step.id,
            "pass": true,
            "wall_time_s": step.wall_time_s,
            "detail": step.extra,
        })).collect::<Vec<_>>(),
    });
    assert_evidence_contract(&evidence, expected_gsi_prefix);
    if let Some(path) = evidence_out {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("evidence parent directory");
        }
        fs::write(
            path,
            serde_json::to_vec_pretty(&evidence).expect("serialize evidence"),
        )
        .expect("write evidence JSON");
    }
    evidence
}

fn assert_evidence_contract(evidence: &Value, expected_gsi_prefix: &str) {
    assert_eq!(evidence["schema"], EVIDENCE_SCHEMA);
    assert_eq!(evidence["schema_version"], SCHEMA_VERSION);
    assert!(evidence["track"].is_null());
    let gsi = evidence["gsi"].as_str().expect("gsi");
    assert!(gsi.starts_with(expected_gsi_prefix), "{gsi}");
    assert_eq!(evidence["edgefactor"], EDGE_FACTOR);
    assert!(evidence["teps"].is_null());
    assert_ne!(
        evidence["generator"]["source"],
        "https://github.com/graph500/graph500"
    );
    let workloads = evidence["workloads_run"]
        .as_array()
        .expect("workloads_run")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    for required in [
        "generate",
        "ingest",
        "reopen",
        "cypher_limit_1hop",
        "cypher_limit_2hop",
    ] {
        assert!(workloads.contains(&required), "missing workload {required}");
    }
}

fn generate_kronecker(scale: u32, edge_factor: u32, seed: u64) -> KroneckerGraph {
    assert!((1..=31).contains(&scale), "SCALE must fit u32 vertex ids");
    let vertex_count = 1u64 << scale;
    let raw_edges = vertex_count
        .checked_mul(u64::from(edge_factor))
        .expect("raw edge count");
    let mut rng = SplitMix64(seed);
    let mut edges = Vec::with_capacity(usize::try_from(raw_edges).expect("raw edges fit usize"));
    for _ in 0..raw_edges {
        let (src, dst) = kronecker_edge(scale, &mut rng);
        if src == dst {
            continue;
        }
        let (lo, hi) = if src < dst { (src, dst) } else { (dst, src) };
        edges.push((
            u32::try_from(lo).expect("src"),
            u32::try_from(hi).expect("dst"),
        ));
    }
    edges.sort_unstable();
    edges.dedup();
    let checksum = checksum_edges(&edges);
    KroneckerGraph {
        vertex_count,
        edges,
        checksum,
    }
}

fn kronecker_edge(scale: u32, rng: &mut SplitMix64) -> (u64, u64) {
    let mut src = 0u64;
    let mut dst = 0u64;
    for bit in 0..scale {
        let sample = rng.next_unit();
        let (di, dj) = if sample < KRONECKER_A {
            (0, 0)
        } else if sample < KRONECKER_A + KRONECKER_B {
            (0, 1)
        } else if sample < KRONECKER_A + KRONECKER_B + KRONECKER_C {
            (1, 0)
        } else {
            (1, 1)
        };
        src |= di << bit;
        dst |= dj << bit;
    }
    (src, dst)
}

fn ingest_graph(project: &Path, generated: &KroneckerGraph) {
    let graph = GraphForge::new(Some(project.to_str().expect("utf8 project path")))
        .expect("open GraphForge for ingest");
    publish_nodes(&graph, generated.vertex_count);
    publish_edges(&graph, generated);
    drop(graph);
}

fn publish_nodes(graph: &GraphForge, vertex_count: u64) {
    let total = usize::try_from(vertex_count).expect("vertex count fits usize");
    let schema = bulk_node_input_schema(Vec::new()).expect("node schema");
    let mut batches = Vec::new();
    let mut offset = 0usize;
    while offset < total {
        let end = (offset + BATCH_ROWS).min(total);
        let count = end - offset;
        let ids = (offset..end)
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
        offset = end;
    }
    graph
        .publish_bulk_nodes(OperationId(uuidv7(0xB001)), &batches)
        .expect("publish_bulk_nodes");
}

fn publish_edges(graph: &GraphForge, generated: &KroneckerGraph) {
    let schema = bulk_edge_input_schema(Vec::new()).expect("edge schema");
    let mut chunk_index = 0u128;
    for chunk in generated.edges.chunks(EDGE_PUBLISH_ROWS) {
        let mut batches = Vec::new();
        let mut offset = 0usize;
        while offset < chunk.len() {
            let end = (offset + BATCH_ROWS).min(chunk.len());
            let slice = &chunk[offset..end];
            let mut edge_ids = Vec::with_capacity(slice.len());
            let mut sources = Vec::with_capacity(slice.len());
            let mut targets = Vec::with_capacity(slice.len());
            for (local, &(src, dst)) in slice.iter().enumerate() {
                let ordinal = chunk_index
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
        graph
            .publish_bulk_edges(OperationId(uuidv7(0xB100 + chunk_index)), &batches)
            .expect("publish_bulk_edges");
        chunk_index += 1;
    }
}

struct RankParity {
    fingerprint: String,
    rows: usize,
    wall_time_s: f64,
}

fn degree_thread_parity(project: &Path) -> RankParity {
    let started = Instant::now();
    let one = rank_degree(project, 1);
    let parallel = [4_usize, 2]
        .into_iter()
        .find(|&workers| explicit_thread_policy(workers).normalize().is_ok())
        .unwrap_or(1);
    if parallel > 1 {
        let other = rank_degree(project, parallel);
        assert_eq!(
            one.0, other.0,
            "degree fingerprint must match at 1 and {parallel} compute_threads"
        );
        assert_eq!(one.1, other.1);
    }
    RankParity {
        fingerprint: one.0,
        rows: one.1,
        wall_time_s: started.elapsed().as_secs_f64(),
    }
}

fn rank_degree(project: &Path, workers: usize) -> (String, usize) {
    let graph = GraphForge::new_with_options(
        Some(project.to_str().expect("utf8 project path")),
        GraphForgeOptions {
            resource: explicit_thread_policy(workers),
            ..GraphForgeOptions::default()
        },
    )
    .expect("open GraphForge for rank");
    let batch = graph
        .rank(
            NODE_LABEL,
            RankOptions {
                by: RankAlgorithm::Degree,
                via: Some(REL_TYPE.into()),
                directed: false,
                write_property: None,
            },
        )
        .expect("rank degree");
    (fingerprint_rank(&batch), batch.num_rows())
}

fn explicit_thread_policy(workers: usize) -> ExecutionResourcePolicy {
    ExecutionResourcePolicy {
        mode: ResourcePolicyMode::Explicit,
        tokio_worker_threads: Some(workers),
        target_partitions: Some(workers),
        io_concurrency: Some(workers),
        compute_threads: Some(workers),
        batch_size: Some(BATCH_ROWS),
        memory_budget_bytes: Some(512 * 1024 * 1024),
        spill: SpillPolicy::default(),
        max_concurrent_heavy_queries: Some(1),
    }
}

fn fingerprint_rank(batch: &RecordBatch) -> String {
    let mut hasher = Sha256::new();
    hasher.update((batch.num_rows() as u64).to_le_bytes());
    let ids = batch
        .column_by_name("node_uuid")
        .expect("rank node_uuid")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("node_uuid FixedSizeBinary");
    let scores = batch
        .column_by_name("score")
        .expect("rank score")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("score Float64");
    for row in 0..batch.num_rows() {
        hasher.update(ids.value(row));
        hasher.update(scores.value(row).to_le_bytes());
    }
    hex_encode(hasher.finalize())
}

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
        100_000_000_000..=999_999_999_999 => ("11", "4XL"),
        1_000_000_000_000..=9_999_999_999_999 => ("12", "5XL"),
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

fn density_code(density: f64) -> String {
    let percent = (density * 100.0).round() as i32;
    format!("D{percent:02}")
}

fn checksum_edges(edges: &[(u32, u32)]) -> String {
    let mut hasher = Sha256::new();
    hasher.update((edges.len() as u64).to_le_bytes());
    for &(src, dst) in edges {
        hasher.update(src.to_le_bytes());
        hasher.update(dst.to_le_bytes());
    }
    format!("sha256:{}", hex_encode(hasher.finalize()))
}

fn uuidv7(seed: u128) -> Uuid {
    let mut bytes = seed.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn evidence_out_path() -> PathBuf {
    std::env::var("GF_G500_SCALE20_EVIDENCE_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("build/g500-scale20-evidence.json"))
}

fn git_sha() -> Value {
    std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|sha| Value::String(sha.trim().to_owned()))
        .unwrap_or(Value::Null)
}

fn directory_bytes(path: &Path) -> std::io::Result<u64> {
    let mut total = 0;
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
