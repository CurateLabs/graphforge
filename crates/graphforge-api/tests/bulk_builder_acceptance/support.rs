//! Fixtures shared by the bulk-builder acceptance tests (#1965).
//!
//! Everything here goes through the public facade: Parquet inputs are written
//! to disk, registered with `GraphImportSession::register_parquet`, validated
//! and committed, and the published project is reopened with
//! `GraphForge::new`.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{ArrayRef, FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use graphforge_api::{
    BulkInputKind, ExecutionResourcePolicy, GraphForge, GraphForgeOptions, GraphImportSession,
    ImportProgress, ImportSessionLimits, OperationId, ResourcePolicyMode, bulk_edge_input_schema,
    bulk_node_input_schema,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use uuid::Uuid;

/// Rows per construction batch. A decode task of the bulk builder is sixteen of
/// them, so inputs of more than sixteen batches per kind run several tasks.
pub const BATCH_ROWS: usize = 1_024;
pub const TASK_ROWS: usize = 16 * BATCH_ROWS;
pub const MIB: usize = 1 << 20;

/// Counter-based tests difference process-wide counters, so they do not overlap
/// when the binary runs its tests on threads (nextest gives each its own process).
static SERIAL: Mutex<()> = Mutex::new(());

pub fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub fn v7(value: u128) -> Uuid {
    Uuid::from_u128((value << 80) | (0x7 << 76) | (0x2 << 62) | value)
}

/// Deterministic printable text that does not compress: six bits per byte.
pub fn blob(seed: u64, bytes: usize) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = String::with_capacity(bytes);
    while out.len() < bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let mut word = state;
        for _ in 0..10 {
            if out.len() == bytes {
                break;
            }
            out.push(char::from(ALPHABET[(word & 63) as usize]));
            word >>= 6;
        }
    }
    out
}

/// What an input holds.
#[derive(Clone, Copy, Debug)]
pub struct Spec {
    pub nodes: usize,
    pub edges: usize,
    /// Leading nodes that carry a `blob` of `blob_bytes`; the rest carry none.
    pub node_blobs: usize,
    /// Leading edges that carry a `note` of `blob_bytes`.
    pub edge_blobs: usize,
    pub blob_bytes: usize,
}

impl Spec {
    pub const fn graph(nodes: usize, edges: usize) -> Self {
        Self {
            nodes,
            edges,
            node_blobs: 0,
            edge_blobs: 0,
            blob_bytes: 0,
        }
    }
}

fn uuid_array(values: impl Iterator<Item = Uuid>) -> ArrayRef {
    let values = values.collect::<Vec<_>>();
    Arc::new(
        FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.as_bytes().as_slice()))
            .unwrap(),
    )
}

/// Node `index` (0-based) is `v7(index + 1)`; edges use a disjoint range.
pub fn node_uuid(index: usize) -> Uuid {
    v7(1 + index as u128)
}

pub fn edge_uuid(index: usize) -> Uuid {
    v7(1_000_000_000 + index as u128)
}

fn node_batch(spec: &Spec, range: std::ops::Range<usize>) -> RecordBatch {
    // Property columns are ordered by name.
    let extra = vec![
        Field::new("blob", DataType::Utf8, true),
        Field::new("rank", DataType::Int64, true),
    ];
    RecordBatch::try_new(
        bulk_node_input_schema(extra).unwrap(),
        vec![
            uuid_array(range.clone().map(node_uuid)),
            Arc::new(StringArray::from(
                range.clone().map(|_| "Person").collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                range
                    .clone()
                    .map(|index| {
                        (index < spec.node_blobs).then(|| blob(index as u64 + 1, spec.blob_bytes))
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from_iter_values(
                range.map(|index| index as i64),
            )),
        ],
    )
    .unwrap()
}

/// Endpoints and the position in the file are decorrelated from the UUID so
/// the edge UUID column arrives out of order.
fn edge_position(index: usize, edges: usize) -> usize {
    (index * 7_919 + 13) % edges
}

fn edge_batch(spec: &Spec, range: std::ops::Range<usize>) -> RecordBatch {
    let nodes = spec.nodes;
    let id = |row: usize| edge_position(row, spec.edges);
    let extra = vec![
        Field::new("note", DataType::Utf8, true),
        Field::new("weight", DataType::Int64, true),
    ];
    RecordBatch::try_new(
        bulk_edge_input_schema(extra).unwrap(),
        vec![
            uuid_array(range.clone().map(|row| edge_uuid(id(row)))),
            Arc::new(StringArray::from(
                range.clone().map(|_| "KNOWS").collect::<Vec<_>>(),
            )),
            uuid_array(
                range
                    .clone()
                    .map(|row| node_uuid((id(row) * 7 + 1) % nodes)),
            ),
            uuid_array(
                range
                    .clone()
                    .map(|row| node_uuid((id(row) * 13 + 5) % nodes)),
            ),
            Arc::new(StringArray::from(
                range
                    .clone()
                    .map(|row| {
                        let index = id(row);
                        (index < spec.edge_blobs)
                            .then(|| blob(1_000_000 + index as u64, spec.blob_bytes))
                    })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from_iter_values(
                range.map(|row| id(row) as i64),
            )),
        ],
    )
    .unwrap()
}

fn write_parquet(path: &Path, rows: usize, make: impl Fn(std::ops::Range<usize>) -> RecordBatch) {
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(TASK_ROWS))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();
    let first = make(0..rows.min(1));
    let mut writer = ArrowWriter::try_new(
        File::create(path).unwrap(),
        first.schema(),
        Some(properties),
    )
    .unwrap();
    for start in (0..rows).step_by(BATCH_ROWS) {
        writer
            .write(&make(start..(start + BATCH_ROWS).min(rows)))
            .unwrap();
    }
    writer.close().unwrap();
}

pub struct Sources {
    pub spec: Spec,
    pub directory: PathBuf,
}

impl Sources {
    pub fn write(directory: &Path, spec: Spec) -> Self {
        fs::create_dir_all(directory).unwrap();
        write_parquet(&directory.join("nodes.parquet"), spec.nodes, |range| {
            node_batch(&spec, range)
        });
        write_parquet(&directory.join("edges.parquet"), spec.edges, |range| {
            edge_batch(&spec, range)
        });
        Self {
            spec,
            directory: directory.to_owned(),
        }
    }

    pub fn nodes(&self) -> PathBuf {
        self.directory.join("nodes.parquet")
    }

    pub fn edges(&self) -> PathBuf {
        self.directory.join("edges.parquet")
    }
}

pub fn limits() -> ImportSessionLimits {
    ImportSessionLimits {
        batch_rows: BATCH_ROWS,
        ..ImportSessionLimits::default()
    }
}

/// A forge whose construction runs on exactly `lanes` worker lanes.
pub fn forge_with_lanes(project: &Path, lanes: usize) -> GraphForge {
    GraphForge::new_with_options(
        project.to_str(),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                mode: ResourcePolicyMode::Explicit,
                tokio_worker_threads: Some(lanes + 1),
                compute_threads: Some(lanes + 1),
                construction_cpu_reserve: Some(1),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap()
}

pub fn empty_project(directory: &Path) -> PathBuf {
    let project = directory.join("project");
    fs::create_dir(&project).unwrap();
    project
}

/// Register both sources of `sources` in a new import session.
pub fn register(graph: &GraphForge, sources: &Sources) -> GraphImportSession {
    let mut session = graph
        .begin_import_session(OperationId(Uuid::now_v7()), limits())
        .unwrap();
    session
        .register_parquet(BulkInputKind::Node, &sources.nodes())
        .unwrap();
    session
        .register_parquet(BulkInputKind::Edge, &sources.edges())
        .unwrap();
    session
}

pub fn validate(graph: &GraphForge, session: &mut GraphImportSession) -> ImportProgress {
    session.validate(graph).unwrap()
}

/// Every regular file under `root`, relative, with its length.
pub fn tree(root: &Path) -> BTreeMap<PathBuf, u64> {
    fn walk(root: &Path, directory: &Path, into: &mut BTreeMap<PathBuf, u64>) {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).unwrap();
            if metadata.is_dir() {
                walk(root, &path, into);
            } else {
                into.insert(path.strip_prefix(root).unwrap().to_owned(), metadata.len());
            }
        }
    }
    let mut into = BTreeMap::new();
    walk(root, root, &mut into);
    into
}

/// Rendered answers of the queries every route must agree on.
pub fn answers(graph: &GraphForge) -> String {
    [
        "MATCH (n:Person) RETURN count(n) AS nodes, sum(n.rank) AS ranks, min(n.rank) AS low, \
         max(n.rank) AS high",
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN count(r) AS edges, sum(r.weight) AS weights, \
         count(DISTINCT a) AS sources, count(DISTINCT b) AS targets",
        "MATCH (n:Person) WHERE n.blob IS NOT NULL RETURN count(n) AS blobs, sum(size(n.blob)) AS bytes",
        "MATCH ()-[r:KNOWS]->() WHERE r.note IS NOT NULL RETURN count(r) AS notes, sum(size(r.note)) AS bytes",
        "MATCH (n:Person) WHERE n.rank = 7 RETURN n.rank AS rank, n.node_uuid AS id",
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) WHERE r.weight = 11 RETURN r.weight AS weight, \
         a.rank AS source, b.rank AS target",
    ]
    .iter()
    .map(|query| {
        let result = graph.execute(query).unwrap();
        pretty_format_batches(&result.batches).unwrap().to_string()
    })
    .collect::<Vec<_>>()
    .join("\n")
}

/// One entry of a build's encoded inventory: a published object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Artifact {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

/// The session's construction directory: where the bulk builder encodes the
/// generation's objects before they are installed and published.
pub fn construction_root(project: &Path) -> PathBuf {
    let mut sessions = fs::read_dir(project.join(".graphforge-construction"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(sessions.len(), 1, "{sessions:?}");
    sessions.remove(0)
}

/// What the build pinned for publication, read from the session's inventory.
pub fn inventory(project: &Path) -> Vec<Artifact> {
    let contents: serde_json::Value = serde_json::from_slice(
        &fs::read(construction_root(project).join("encoded-v1/inventory.json")).unwrap(),
    )
    .unwrap();
    contents["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|artifact| Artifact {
            path: artifact["path"].as_str().unwrap().to_owned(),
            bytes: artifact["bytes"].as_u64().unwrap(),
            sha256: artifact["sha256"].as_str().unwrap().to_owned(),
        })
        .collect()
}

/// Where the object store keeps the object with this SHA-256.
pub fn object_path(project: &Path, sha256: &str) -> PathBuf {
    project
        .join("graph-objects/sha256")
        .join(&sha256[..2])
        .join(&sha256[2..])
}
