//! Manual/scheduled densified 8M-node / 128M-edge public file-backed evidence (#338).
//!
//! Structural CI already covers multi-file reopen and sparse >2 GiB padding.
//! This ignored harness proves the measured discovery-class fixture through the
//! supported public path:
//!
//! 1. Stream nodes + `_exploratory.parquet` edges into a workspace.
//! 2. Single `capture_graph_files` + `stage_project_generation_with_graph_tree`.
//! 3. Close → `GraphForge::new` → one-hop LIMIT query.
//!
//! ```bash
//! CARGO_TARGET_DIR=/tmp/cargo-338-fb \
//! GF_FILE_BACKED_SCALE_EVIDENCE_OUT=docs/development/file-backed-128m-evidence.json \
//! GF_FILE_BACKED_SCALE_WORK=build/file-backed-128m-work \
//!   make bench-file-backed-128m
//! ```

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{
    ArrayRef, FixedSizeBinaryArray, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use arrow::record_batch::RecordBatch;
use graphforge_api::{GraphForge, GraphForgeOptions};
use graphforge_core::OntologyMode;
use graphforge_core::uuid::Uuid;
use graphforge_storage::{
    EXPLORATORY_EDGE_SCHEMA, GRAPH_CAPABILITY_ID, GRAPH_CAPABILITY_VERSION, GraphFilesOpenStrategy,
    GraphWriter, ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome,
    capture_graph_files, empty_workspace_participants, resolve_project_generation,
    stage_project_generation_with_graph_tree,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterVersion};
use serde_json::json;
use tempfile::TempDir;

const EVIDENCE_SCHEMA: &str = "graphforge-file-backed-128m-evidence/1";
const DEFAULT_NODES: u64 = 8_000_000;
const DEFAULT_EDGES: u64 = 128_000_000;
const MIN_NODES: u64 = 8_000_000;
const MIN_EDGES: u64 = 128_000_000;
const BATCH_ROWS: usize = 262_144;
const REL_TYPE: &str = "LINK";
const BUILD_TS: i64 = 1_700_000_000_000_000;

#[test]
fn evidence_schema_constant_is_stable() {
    assert_eq!(EVIDENCE_SCHEMA, "graphforge-file-backed-128m-evidence/1");
    assert!(MIN_NODES >= 8_000_000);
    assert!(MIN_EDGES >= 128_000_000);
}

#[test]
#[ignore = "manual/scheduled 8M/128M public file-backed reopen; make bench-file-backed-128m"]
fn densified_8m_128m_public_reopen_emits_evidence() {
    let edge_count = env_u64("GF_FILE_BACKED_SCALE_EDGES", DEFAULT_EDGES);
    let node_count = env_u64("GF_FILE_BACKED_SCALE_NODES", DEFAULT_NODES).max(2);
    let allow_small = std::env::var_os("GF_FILE_BACKED_SCALE_ALLOW_SMALL").is_some();
    assert!(
        allow_small || (edge_count >= MIN_EDGES && node_count >= MIN_NODES),
        "need >= {MIN_NODES} nodes and >= {MIN_EDGES} edges (or GF_FILE_BACKED_SCALE_ALLOW_SMALL=1)"
    );
    let out = env_path("GF_FILE_BACKED_SCALE_EVIDENCE_OUT")
        .unwrap_or_else(|| PathBuf::from("build/file-backed-128m-evidence.json"));
    let work_root = env_path("GF_FILE_BACKED_SCALE_WORK");
    let (_keep, roots) = work_dirs(work_root.as_deref());
    let tmp_dir = roots.root.join("tmp");
    fs::create_dir_all(&tmp_dir).expect("tmp dir");
    // SAFETY: single-threaded ignored harness; pin materialize beside the work root.
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
        .expect("workspace edge parquet")
        .len();
    eprintln!("workspace write complete in {write_s:.1}s; exploratory bytes={edges_bytes}");

    let publish_started = Instant::now();
    let inventory_bytes = publish_file_backed_once(&roots.project, &roots.workspace);
    let publish_s = publish_started.elapsed().as_secs_f64();
    eprintln!("publish complete in {publish_s:.1}s; inventory bytes={inventory_bytes}");

    let reopen_started = Instant::now();
    let rss_before_open = peak_rss_bytes();
    let graph = GraphForge::new_with_options(
        Some(roots.project.to_str().expect("utf8")),
        GraphForgeOptions::default(),
    )
    .unwrap_or_else(|e| panic!("GraphForge::new reopen: {e:?}"));
    let reopen_s = reopen_started.elapsed().as_secs_f64();
    let open_evidence = graph.graph_open_evidence().clone();
    eprintln!(
        "reopened in {reopen_s:.1}s strategy={:?} validated={} copied={}",
        open_evidence.strategy, open_evidence.bytes_validated, open_evidence.bytes_copied
    );
    assert_ne!(
        open_evidence.strategy,
        GraphFilesOpenStrategy::LegacySnapshotHydrate
    );
    assert_eq!(open_evidence.bytes_validated, inventory_bytes);

    let query_started = Instant::now();
    let hop = graph
        .execute("MATCH (a)-[r:LINK]->(b) RETURN b LIMIT 1000")
        .unwrap_or_else(|e| panic!("one-hop after densified reopen: {e:?}"));
    let hop_rows = hop.batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert!(hop_rows > 0 && hop_rows <= 1_000, "rows={hop_rows}");
    let query_s = query_started.elapsed().as_secs_f64();
    let rss_after_query = peak_rss_bytes();
    drop(graph);

    let disk_used_bytes = directory_bytes(&roots.project).unwrap_or(0);
    let evidence = json!({
        "schema": EVIDENCE_SCHEMA,
        "schema_version": "1",
        "issue": 338,
        "pass": true,
        "git_sha": git_head_sha(),
        "strategy": "densified_8m_128m_public_facade",
        "fixture": {
            "node_count": node_count,
            "edge_count": edge_count,
            "rel_type": REL_TYPE,
            "generator": "graphwriter-nodes + streamed-exploratory-parquet-edges + single-file-backed-publish",
        },
        "publication": {
            "path": "capture_graph_files + stage_project_generation_with_graph_tree (one generation)",
            "inventory_total_byte_length": inventory_bytes,
        },
        "open": {
            "api": "GraphForge::new",
            "strategy": format!("{:?}", open_evidence.strategy),
            "files_validated": open_evidence.files_validated,
            "bytes_validated": open_evidence.bytes_validated,
            "files_copied": open_evidence.files_copied,
            "bytes_copied": open_evidence.bytes_copied,
            "files_opened_in_place": open_evidence.files_opened_in_place,
        },
        "query": {
            "cypher": "MATCH (a)-[r:LINK]->(b) RETURN b LIMIT 1000",
            "rows_produced": hop_rows,
        },
        "resources": {
            "disk_used_bytes": disk_used_bytes,
            "peak_rss_bytes_before_write": rss_before_write,
            "peak_rss_bytes_after_write": rss_after_write,
            "peak_rss_bytes_before_open": rss_before_open,
            "peak_rss_bytes_after_query": rss_after_query,
            "peak_rss_note": "Linux: VmHWM from /proc/self/status. macOS: sampled RSS via ps at each checkpoint (not kernel high-water).",
            "honest_limits": [
                "Densified synthetic fixture for the measured 8M-node/128M-edge class.",
                "Writable GraphForge::new materializes file-by-file (PrivateMaterialize).",
                "Timing/RSS are hardware-specific; no universal graph-size ceiling is claimed.",
            ],
        },
        "timing": {
            "workspace_write_wall_time_s": write_s,
            "publish_wall_time_s": publish_s,
            "reopen_wall_time_s": reopen_s,
            "one_hop_limit_wall_time_s": query_s,
            "total_wall_time_s": write_s + publish_s + reopen_s + query_s,
        },
        "hardware": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
        },
    });

    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent).expect("evidence parent");
    }
    fs::write(
        &out,
        serde_json::to_vec_pretty(&evidence).expect("serialize"),
    )
    .expect("write evidence");
    eprintln!(
        "wrote {} edges={} nodes={} disk_bytes={}",
        out.display(),
        edge_count,
        node_count,
        disk_used_bytes
    );
    assert_eq!(evidence["pass"], true);
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
                fs::remove_dir_all(path).expect("clean prior");
            }
            fs::create_dir_all(path).expect("create");
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
        let tmp = TempDir::new().expect("temp");
        let project = tmp.path().join("project");
        let workspace = tmp.path().join("workspace");
        fs::create_dir(&project).unwrap();
        fs::create_dir(&workspace).unwrap();
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
    // Do not retain O(node_count) UUID/id vectors — regenerate deterministic
    // uuidv7(index+1) / sequential ids when streaming edges (8M-class host RSS).
    let mut writer = GraphWriter::open_at(workspace, OntologyMode::Exploratory, BUILD_TS)
        .expect("open GraphWriter");
    let mut first_id = None;
    for index in 0..node_count {
        let uuid = uuidv7(u128::from(index) + 1);
        let id = writer
            .create_node(uuid, graphforge_core::TypeId(0))
            .unwrap_or_else(|e| panic!("create_node {index}: {e}"));
        if first_id.is_none() {
            first_id = Some(id);
        } else {
            assert_eq!(
                id,
                first_id.unwrap() + index,
                "GraphWriter node ids must stay sequential for densified edge streaming"
            );
        }
        if (index + 1) % 500_000 == 0 || index + 1 == node_count {
            writer.flush().expect("flush nodes");
            eprintln!("created nodes {}/{}", index + 1, node_count);
        }
    }
    writer.flush().expect("final node flush");
    let u0 = uuidv7(1);
    let u1 = uuidv7(2);
    writer
        .create_edge(uuidv7(0xE000_0000_0000u128), REL_TYPE, &u0, &u1)
        .expect("seed edge");
    writer.flush().expect("seed edge flush");
    drop(writer);
    let base_id = first_id.expect("at least one node");
    stream_exploratory_edge_parquet(workspace, node_count, base_id, edge_count);
}

fn stream_exploratory_edge_parquet(
    workspace: &Path,
    node_count: u64,
    base_node_id: u64,
    edge_count: u64,
) {
    let edges_path = workspace
        .join("topology/edges")
        .join("_exploratory.parquet");
    fs::create_dir_all(edges_path.parent().unwrap()).unwrap();
    let schema = EXPLORATORY_EDGE_SCHEMA.clone();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(BATCH_ROWS))
        .build();
    let file = File::create(&edges_path).expect("create edge parquet");
    let mut parquet =
        ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
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
            let src = ordinal % node_count;
            let dst = (ordinal + 1) % node_count;
            edge_uuid.push(uuidv7(0xE000_0000_0000u128 + u128::from(ordinal) + 1));
            src_uuid.push(uuidv7(u128::from(src) + 1));
            dst_uuid.push(uuidv7(u128::from(dst) + 1));
            edge_id.push(ordinal + 1);
            src_id.push(base_node_id + src);
            dst_id.push(base_node_id + dst);
            rel_type_name.push(REL_TYPE.to_owned());
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(edge_uuid.iter().map(Uuid::as_bytes))
                        .unwrap(),
                ) as ArrayRef,
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(src_uuid.iter().map(Uuid::as_bytes))
                        .unwrap(),
                ) as ArrayRef,
                Arc::new(
                    FixedSizeBinaryArray::try_from_iter(dst_uuid.iter().map(Uuid::as_bytes))
                        .unwrap(),
                ) as ArrayRef,
                Arc::new(UInt64Array::from(edge_id)) as ArrayRef,
                Arc::new(UInt64Array::from(src_id)) as ArrayRef,
                Arc::new(UInt64Array::from(dst_id)) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![BUILD_TS; take]).with_timezone("UTC"))
                    as ArrayRef,
                Arc::new(StringArray::from(rel_type_name)) as ArrayRef,
            ],
        )
        .unwrap();
        parquet.write(&batch).unwrap();
        written += take as u64;
        if written % (BATCH_ROWS as u64 * 16) == 0 || written == edge_count {
            eprintln!("wrote edge rows {written}/{edge_count}");
        }
    }
    parquet.close().unwrap();
    eprintln!(
        "edge parquet ready path={} bytes={}",
        edges_path.display(),
        fs::metadata(&edges_path).map(|m| m.len()).unwrap_or(0)
    );
}

fn publish_file_backed_once(project: &Path, workspace: &Path) -> u64 {
    {
        let _seed = GraphForge::new(Some(project.to_str().unwrap())).expect("seed project");
    }
    let seed = resolve_project_generation(project).expect("seed generation");
    let expected_parent = seed.generation_uuid();
    drop(seed);
    let (inventory, files_participant) =
        capture_graph_files(workspace).expect("capture graph files");
    let inventory_bytes = inventory.total_byte_length;
    let mut participants = empty_workspace_participants().unwrap();
    participants.insert(0, files_participant);
    let request = ProjectGenerationRequest {
        transaction_uuid: Uuid::now_v7(),
        generation_uuid: Uuid::now_v7(),
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
        stage_project_generation_with_graph_tree(project, &request, Some(workspace)).unwrap()
    else {
        panic!("unexpected replay");
    };
    staged
        .validate(
            |_| Ok(()),
            |actual_parent, _| {
                assert_eq!(actual_parent.generation_uuid(), expected_parent);
                Ok(())
            },
        )
        .unwrap()
        .publish()
        .unwrap();
    inventory_bytes
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
