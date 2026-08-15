//! Manual/scheduled >200M-edge public adjacency build evidence (#336).
//!
//! Structural CI seams already prove the streaming/spill builder. This ignored
//! harness produces densified outcome evidence with a **single** file-backed
//! project publication (no per-chunk generation amplification):
//!
//! 1. Stream nodes via GraphWriter, then stream `_exploratory.parquet` edges.
//! 2. Capture once via `capture_graph_files` + `stage_project_generation_with_graph_tree`.
//! 3. `GraphForge::new` → `index_adjacency` → node_count + one-hop LIMIT.
//!
//! ```bash
//! CARGO_TARGET_DIR=/tmp/cargo-336-adj \
//! GF_ADJACENCY_SCALE_EVIDENCE_OUT=docs/development/adjacency-200m-evidence.json \
//! GF_ADJACENCY_SCALE_WORK=build/adjacency-200m-work \
//!   make bench-adjacency-200m
//! ```
//!
//! Optional knobs:
//! - `GF_ADJACENCY_SCALE_EDGES` (default `201000000`, must be >200_000_000)
//! - `GF_ADJACENCY_SCALE_NODES` (default `1048576`)
//! - `GF_ADJACENCY_SCALE_ALLOW_SMALL=1` (local diagnostics below 200M only)

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, FixedSizeBinaryArray, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    ExecutionResourcePolicy, GraphForge, GraphForgeOptions, ResourcePolicyMode, SpillPolicy,
};
use graphforge_core::OntologyMode;
use graphforge_core::uuid::Uuid;
use graphforge_storage::{
    EXPLORATORY_EDGE_SCHEMA, GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, GraphWriter,
    ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome, capture_graph_files,
    empty_workspace_participants, resolve_project_generation,
    stage_project_generation_with_graph_tree,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterVersion};
use serde_json::json;
use tempfile::TempDir;

const EVIDENCE_SCHEMA: &str = "graphforge-adjacency-200m-evidence/1";
const DEFAULT_EDGES: u64 = 201_000_000;
const DEFAULT_NODES: u64 = 1_048_576;
const MIN_EDGES: u64 = 200_000_001;
const BATCH_ROWS: usize = 262_144;
const REL_TYPE: &str = "LINK";
const MEMORY_BUDGET_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const CHUNK_ROWS: usize = 8_388_608;
const POLICY_BATCH_SIZE: usize = 65_536;
const BUILD_TS: i64 = 1_700_000_000_000_000;

#[test]
fn evidence_schema_constant_is_stable() {
    assert_eq!(EVIDENCE_SCHEMA, "graphforge-adjacency-200m-evidence/1");
    assert!(MIN_EDGES > 200_000_000);
}

#[test]
#[ignore = "manual/scheduled >200M public adjacency build; make bench-adjacency-200m"]
fn adjacency_over_200m_public_build_emits_evidence() {
    let edge_count = env_u64("GF_ADJACENCY_SCALE_EDGES", DEFAULT_EDGES);
    let allow_small = std::env::var_os("GF_ADJACENCY_SCALE_ALLOW_SMALL").is_some();
    assert!(
        allow_small || edge_count >= MIN_EDGES,
        "GF_ADJACENCY_SCALE_EDGES={edge_count} must be >= {MIN_EDGES} (or set GF_ADJACENCY_SCALE_ALLOW_SMALL=1 for local diagnostics)"
    );
    let node_count = env_u64("GF_ADJACENCY_SCALE_NODES", DEFAULT_NODES).max(2);
    let out = env_path("GF_ADJACENCY_SCALE_EVIDENCE_OUT")
        .unwrap_or_else(|| PathBuf::from("build/adjacency-200m-evidence.json"));
    let work_root = env_path("GF_ADJACENCY_SCALE_WORK");
    let (_keep, roots) = work_dirs(work_root.as_deref());
    let spill_dir = roots.root.join(".adjacency-spill");
    let tmp_dir = roots.root.join("tmp");
    fs::create_dir_all(&tmp_dir).expect("tmp dir");
    // Keep PrivateMaterialize + adjacency stage on the dedicated work volume so
    // /var/folders pressure and cross-agent build/ wipes cannot race the run.
    // SAFETY: single-threaded ignored harness; no other threads yet.
    unsafe {
        std::env::set_var("TMPDIR", &tmp_dir);
    }

    let write_started = Instant::now();
    let rss_before_write = peak_rss_bytes();
    write_workspace(&roots.workspace, node_count, edge_count);
    let write_s = write_started.elapsed().as_secs_f64();
    let rss_after_write = peak_rss_bytes();
    let edges_parquet = roots
        .workspace
        .join("topology/edges")
        .join("_exploratory.parquet");
    let edges_bytes = fs::metadata(&edges_parquet)
        .unwrap_or_else(|e| {
            panic!(
                "workspace edge parquet metadata {}: {e}",
                edges_parquet.display()
            )
        })
        .len();
    eprintln!("workspace write complete in {write_s:.1}s; exploratory parquet bytes={edges_bytes}");
    assert!(
        edges_bytes > edge_count,
        "exploratory parquet too small ({edges_bytes} bytes) for {edge_count} edges — write/capture race?"
    );

    let publish_started = Instant::now();
    let inventory_bytes = publish_file_backed_once(&roots.project, &roots.workspace);
    let publish_s = publish_started.elapsed().as_secs_f64();
    eprintln!(
        "single file-backed publish complete in {publish_s:.1}s; inventory bytes={inventory_bytes}"
    );
    assert!(
        inventory_bytes >= edges_bytes,
        "published inventory ({inventory_bytes}) smaller than workspace parquet ({edges_bytes})"
    );

    let open_opts = GraphForgeOptions {
        resource: scale_policy(&spill_dir),
        ..GraphForgeOptions::default()
    };

    let reopen_started = Instant::now();
    let graph = GraphForge::new_with_options(
        Some(roots.project.to_str().expect("utf8 project path")),
        open_opts,
    )
    .unwrap_or_else(|e| panic!("reopen GraphForge for public adjacency build: {e:?}"));
    let reopen_s = reopen_started.elapsed().as_secs_f64();
    let open_evidence = graph.graph_open_evidence().clone();
    eprintln!(
        "reopened GraphForge in {reopen_s:.1}s strategy={:?} bytes_validated={} bytes_copied={}; starting index_adjacency",
        open_evidence.strategy, open_evidence.bytes_validated, open_evidence.bytes_copied
    );
    assert_eq!(
        open_evidence.bytes_validated, inventory_bytes,
        "reopen validated bytes must match published inventory"
    );

    let build_started = Instant::now();
    let rss_before_build = peak_rss_bytes();
    let inspection = graph
        .index_adjacency()
        .unwrap_or_else(|e| panic!("public index_adjacency must succeed beyond 200M edges: {e:?}"));
    let build_s = build_started.elapsed().as_secs_f64();
    eprintln!("index_adjacency complete in {build_s:.1}s");
    let rss_after_build = peak_rss_bytes();

    let inspect = graph
        .inspect_adjacency()
        .expect("inspect adjacency after build");
    assert_eq!(inspect.state, inspection.state);
    let fingerprint = inspect
        .artifact_fingerprint
        .clone()
        .expect("fresh adjacency must expose artifact fingerprint");

    let query_started = Instant::now();
    let hop = graph
        .execute("MATCH (a)-[r:LINK]->(b) RETURN b LIMIT 1000")
        .unwrap_or_else(|e| panic!("one-hop LIMIT after adjacency build: {e:?}"));
    let hop_rows = hop.batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert!(
        hop_rows > 0 && hop_rows <= 1_000,
        "one-hop LIMIT rows={hop_rows}"
    );
    let query_s = query_started.elapsed().as_secs_f64();
    drop(graph);

    let disk_used_bytes = directory_bytes(&roots.project).unwrap_or(0);
    let evidence = json!({
        "schema": EVIDENCE_SCHEMA,
        "schema_version": "1",
        "issue": 336,
        "pass": true,
        "git_sha": git_head_sha(),
        "fixture": {
            "node_count": node_count,
            "edge_count": edge_count,
            "rel_type": REL_TYPE,
            "generator": "graphwriter-nodes + streamed-exploratory-parquet-edges + single-file-backed-publish",
        },
        "build": {
            "public_api": "GraphForge::index_adjacency",
            "publish_path": "capture_graph_files + stage_project_generation_with_graph_tree (one generation)",
            "chunk_rows": CHUNK_ROWS,
            "batch_size": POLICY_BATCH_SIZE,
            "memory_budget_bytes": MEMORY_BUDGET_BYTES,
            "spill_enabled": true,
        },
        "outcomes": {
            "adjacency_state": format!("{:?}", inspection.state),
            "artifact_fingerprint": fingerprint,
            "source_topology_generation": inspection.source_topology_generation,
            "one_hop_limit_rows": hop_rows,
        },
        "resources": {
            "disk_used_bytes": disk_used_bytes,
            "peak_rss_bytes_before_write": rss_before_write,
            "peak_rss_bytes_after_write": rss_after_write,
            "peak_rss_bytes_before_build": rss_before_build,
            "peak_rss_bytes_after_build": rss_after_build,
            "peak_rss_note": "Linux: VmHWM from /proc/self/status. macOS: sampled RSS via ps at each checkpoint (not kernel high-water).",
        },
        "timing": {
            "workspace_write_wall_time_s": write_s,
            "publish_wall_time_s": publish_s,
            "reopen_wall_time_s": reopen_s,
            "adjacency_build_wall_time_s": build_s,
            "one_hop_limit_wall_time_s": query_s,
            "total_wall_time_s": write_s + publish_s + reopen_s + build_s + query_s,
        },
        "hardware": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        },
        "notes": "Densified >200M-edge public adjacency build for #336. Single file-backed generation avoids publish_bulk per-chunk amplification. Timing is hardware-specific; no universal graph-size ceiling is claimed.",
    });

    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent).expect("evidence parent");
    }
    fs::write(
        &out,
        serde_json::to_vec_pretty(&evidence).expect("serialize evidence"),
    )
    .unwrap_or_else(|e| panic!("write {}: {e}", out.display()));
    eprintln!(
        "wrote {} fingerprint={} edges={} disk_bytes={}",
        out.display(),
        evidence["outcomes"]["artifact_fingerprint"],
        edge_count,
        disk_used_bytes
    );
    assert_eq!(evidence["pass"], true);
    assert!(evidence["fixture"]["edge_count"].as_u64().unwrap() >= MIN_EDGES || allow_small);
}

struct WorkRoots {
    root: PathBuf,
    project: PathBuf,
    workspace: PathBuf,
}

fn work_dirs(work: Option<&Path>) -> (Option<TempDir>, WorkRoots) {
    if let Some(root) = work {
        fs::create_dir_all(root).expect("work root");
        let project = root.join("project");
        let workspace = root.join("workspace");
        for path in [&project, &workspace] {
            if path.exists() {
                fs::remove_dir_all(path).expect("clean prior path");
            }
            fs::create_dir_all(path).expect("create path");
        }
        (
            None,
            WorkRoots {
                root: root.to_path_buf(),
                project,
                workspace,
            },
        )
    } else {
        let tmp = TempDir::new().expect("temp work");
        let project = tmp.path().join("project");
        let workspace = tmp.path().join("workspace");
        fs::create_dir(&project).expect("project dir");
        fs::create_dir(&workspace).expect("workspace dir");
        (
            Some(tmp),
            WorkRoots {
                root: project.parent().unwrap().to_path_buf(),
                project,
                workspace,
            },
        )
    }
}

fn write_workspace(workspace: &Path, node_count: u64, edge_count: u64) {
    // Exploratory so streamed edges land in `_exploratory.parquet`, matching the
    // default GraphForge open mode (typed LINK.parquet is invisible to Expand).
    let mut writer = GraphWriter::open_at(workspace, OntologyMode::Exploratory, BUILD_TS)
        .expect("open GraphWriter");
    let mut node_uuids = Vec::with_capacity(usize::try_from(node_count).expect("nodes fit"));
    let mut node_ids = Vec::with_capacity(usize::try_from(node_count).expect("nodes fit"));
    for index in 0..node_count {
        let uuid = uuidv7(u128::from(index) + 1);
        let id = writer
            .create_node(uuid, graphforge_core::TypeId(0))
            .unwrap_or_else(|e| panic!("create_node {index}: {e}"));
        node_uuids.push(uuid);
        node_ids.push(id);
        if (index + 1) % 100_000 == 0 || index + 1 == node_count {
            writer.flush().expect("flush nodes");
            eprintln!("created nodes {}/{}", index + 1, node_count);
        }
    }
    writer.flush().expect("final node flush");

    writer
        .create_edge(
            uuidv7(0xE000_0000_0000u128),
            REL_TYPE,
            &node_uuids[0],
            &node_uuids[1 % node_uuids.len()],
        )
        .expect("seed edge");
    writer.flush().expect("seed edge flush");
    drop(writer);

    stream_exploratory_edge_parquet(workspace, &node_uuids, &node_ids, edge_count);
}

fn stream_exploratory_edge_parquet(
    workspace: &Path,
    node_uuids: &[Uuid],
    node_ids: &[u64],
    edge_count: u64,
) {
    let edges_path = workspace
        .join("topology/edges")
        .join("_exploratory.parquet");
    fs::create_dir_all(edges_path.parent().expect("edges parent")).expect("edges dir");
    let schema = EXPLORATORY_EDGE_SCHEMA.clone();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(BATCH_ROWS))
        .build();
    let file = File::create(&edges_path).expect("create edge parquet");
    let mut parquet =
        ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");

    let node_len = node_uuids.len() as u64;
    let mut written = 0u64;
    while written < edge_count {
        let take = ((edge_count - written) as usize).min(BATCH_ROWS);
        let mut edge_uuid = Vec::with_capacity(take);
        let mut src_uuid = Vec::with_capacity(take);
        let mut dst_uuid = Vec::with_capacity(take);
        let mut edge_id = Vec::with_capacity(take);
        let mut src_id = Vec::with_capacity(take);
        let mut dst_id = Vec::with_capacity(take);
        let mut rel_type_name = Vec::with_capacity(take);
        for local in 0..take {
            let ordinal = written + local as u64;
            let src = (ordinal % node_len) as usize;
            let dst = ((ordinal + 1) % node_len) as usize;
            edge_uuid.push(uuidv7(0xE000_0000_0000u128 + u128::from(ordinal) + 1));
            src_uuid.push(node_uuids[src]);
            dst_uuid.push(node_uuids[dst]);
            edge_id.push(ordinal + 1);
            src_id.push(node_ids[src]);
            dst_id.push(node_ids[dst]);
            rel_type_name.push(REL_TYPE.to_owned());
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(edge_uuid.iter().map(Uuid::as_bytes))
                        .expect("edge_uuid"),
                ) as ArrayRef,
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(src_uuid.iter().map(Uuid::as_bytes))
                        .expect("src_uuid"),
                ) as ArrayRef,
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(dst_uuid.iter().map(Uuid::as_bytes))
                        .expect("dst_uuid"),
                ) as ArrayRef,
                Arc::new(UInt64Array::from(edge_id)) as ArrayRef,
                Arc::new(UInt64Array::from(src_id)) as ArrayRef,
                Arc::new(UInt64Array::from(dst_id)) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![BUILD_TS; take]).with_timezone("UTC"))
                    as ArrayRef,
                Arc::new(StringArray::from(rel_type_name)) as ArrayRef,
            ],
        )
        .expect("edge batch");
        parquet.write(&batch).expect("write edge batch");
        written += take as u64;
        if written % (BATCH_ROWS as u64 * 8) == 0 || written == edge_count {
            eprintln!("wrote edge rows {written}/{edge_count}");
        }
    }
    parquet.close().expect("close edge parquet");
    let edge_bytes = fs::metadata(&edges_path)
        .map(|meta| meta.len())
        .unwrap_or(0);
    eprintln!(
        "edge parquet ready path={} bytes={}",
        edges_path.display(),
        edge_bytes
    );
}

fn publish_file_backed_once(project: &Path, workspace: &Path) -> u64 {
    {
        let _seed = GraphForge::new(Some(project.to_str().expect("utf8 project path")))
            .expect("seed empty GraphForge project");
    }
    let seed = resolve_project_generation(project).expect("seed generation");
    let expected_parent = seed.generation_uuid();
    drop(seed);
    let (inventory, files_participant) =
        capture_graph_files(workspace).expect("capture graph files");
    assert!(
        inventory.file_count >= 2,
        "workspace must contain topology files"
    );
    let inventory_bytes = inventory.total_byte_length;
    let mut participants = empty_workspace_participants().expect("workspace participants");
    participants.insert(0, files_participant);
    let generation_uuid = Uuid::now_v7();
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid,
        capabilities: vec![
            ProjectCapability {
                capability_id: GRAPH_CAPABILITY_ID.into(),
                capability_version: GRAPH_CAPABILITY_VERSION,
            },
            ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };
    let ProjectStageOutcome::Staged(staged) =
        stage_project_generation_with_graph_tree(project, &request, Some(workspace))
            .expect("stage file-backed generation")
    else {
        panic!("publication unexpectedly replayed");
    };
    staged
        .validate(
            |_| Ok(()),
            |actual_parent, _| {
                assert_eq!(actual_parent.generation_uuid(), expected_parent);
                Ok(())
            },
        )
        .expect("validate staged generation")
        .publish()
        .expect("publish staged generation");
    inventory_bytes
}

fn scale_policy(spill_dir: &Path) -> ExecutionResourcePolicy {
    let spill_dir = if spill_dir.is_absolute() {
        spill_dir.to_path_buf()
    } else {
        std::env::current_dir().expect("cwd").join(spill_dir)
    };
    fs::create_dir_all(&spill_dir).expect("spill dir");
    ExecutionResourcePolicy {
        mode: ResourcePolicyMode::Explicit,
        tokio_worker_threads: Some(2),
        target_partitions: Some(2),
        io_concurrency: Some(2),
        compute_threads: Some(2),
        batch_size: Some(POLICY_BATCH_SIZE),
        memory_budget_bytes: Some(MEMORY_BUDGET_BYTES),
        spill: SpillPolicy {
            enabled: true,
            directory: Some(spill_dir),
            max_bytes: Some(128 * 1024 * 1024 * 1024),
        },
        max_concurrent_heavy_queries: Some(1),
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .map(|raw| {
            raw.parse::<u64>()
                .unwrap_or_else(|_| panic!("{key} must be u64"))
        })
        .unwrap_or(default)
}

fn env_path(key: &str) -> Option<PathBuf> {
    std::env::var_os(key).map(PathBuf::from)
}

fn peak_rss_bytes() -> Option<u64> {
    if let Ok(contents) = fs::read_to_string("/proc/self/status") {
        for line in contents.lines() {
            if let Some(value) = line.strip_prefix("VmHWM:") {
                let kb = value
                    .trim()
                    .trim_end_matches(" kB")
                    .trim()
                    .parse::<u64>()
                    .ok()?;
                return Some(kb.saturating_mul(1024));
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
    Some(kb.saturating_mul(1024))
}

fn directory_bytes(path: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    if path.is_file() {
        return Ok(path.metadata()?.len());
    }
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            total = total.saturating_add(directory_bytes(&entry.path())?);
        } else {
            total = total.saturating_add(meta.len());
        }
    }
    Ok(total)
}

fn git_head_sha() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                Some(String::from_utf8_lossy(&output.stdout).trim().to_owned())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".into())
}

fn uuidv7(seed: u128) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&seed.to_be_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}
