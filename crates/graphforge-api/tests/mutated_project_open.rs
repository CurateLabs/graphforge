//! Mutated-project open cost and corruption refusal (#1388, criteria 1 and 4).
//!
//! Every mutating commit publishes a compact (V2) root and installs only the
//! files it changed, and delta runs are no longer published, so a project that
//! has been mutated opens exactly as cheaply as the construction session's
//! published generation: the manifest, the route table and the small controls,
//! never a payload.
//!
//! The mutated project here is the one a user has after real use: a bulk
//! constructed graph, then a CREATE, a SET on an existing node, an edge DELETE,
//! a node DELETE, `index_adjacency` and a composite property SET (the one commit
//! that used to publish a delta run), each through the ordinary public path.
//! Nodes and edges both grow 16x between the two sizes (edge fan-out is fixed),
//! so work proportional to either shows up in the open.
//!
//! The gate is deterministic: lifecycle-attributed read bytes and the
//! whole-process `rchar` where the platform has it, never wall time. The bound is
//! derived from what the manifest declares, not from any observed output.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray};
use graphforge_api::{
    COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeGraphMutation, CompositeKnowledgeParticipants,
    CompositeTransactionRequest, GraphForge, LifecycleIoCapture, OperationId, PropValue,
    WriteContext, lifecycle_io_snapshot,
};
use graphforge_ir::IrLiteral;
use graphforge_storage::{
    GRAPH_CAPABILITY_ID, GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION, GRAPH_FILES_FAMILY,
    GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION, GraphFilesInventory,
    resolve_project_generation,
};

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;
#[allow(dead_code, reason = "each test binary uses a subset")]
#[path = "support/project_fixture.rs"]
mod project_fixture;

const SMALL_NODES: usize = 1 << 10;
const LARGE_NODES: usize = 16 * SMALL_NODES;
const FAN_OUT: usize = 32;
const LIMIT: usize = 1_000;
const ONE_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 1000";

/// The node whose property is set, and the edge removed from the ring.
const SET_NODE: usize = 7;
const DELETED_EDGE_SOURCE: usize = 9;
const DELETED_EDGE_OFFSET: usize = 3;
/// The node whose property a composite transaction sets last.
const COMPOSITE_NODE: usize = 11;

/// Identity controls copied while hydrating are about 40 bytes a node; the open
/// cost test (`open_reads_control_bytes_not_payload_bytes`) bounds them at 64.
const COPIED_CONTROL_BYTES_PER_NODE: u64 = 64;
/// Manifest, route table and sidecar reads that do not scale with either axis.
const CONTROL_SLACK_BYTES: u64 = 64 * 1024;
/// Each copied control is read twice by hydration (to copy it and to verify the
/// private copy).
const HYDRATION_PASSES: u64 = 2;
/// The ordinal-v4 and forward-v4 identity readers make about four unreported
/// passes over the copied identity controls at open; this allows six.
const UNATTRIBUTED_PASSES: u64 = 6;

fn uuid_param(index: usize) -> IrLiteral {
    IrLiteral::Uuid(*bulk_fixture::fixture_node_uuid(index).as_bytes())
}

fn project_path(root: &tempfile::TempDir) -> PathBuf {
    root.path().join("state")
}

/// CREATE, SET, DELETE and `index_adjacency` over a bulk-constructed project,
/// then a composite property SET.
fn mutate(path: &Path) {
    let forge = GraphForge::new(Some(path.to_str().expect("utf-8 project path"))).unwrap();
    forge
        .execute("CREATE (:Extra {name: 'created'})")
        .expect("CREATE");
    forge
        .execute_with_params(
            "MATCH (n:Entity) WHERE n.node_uuid = $node SET n.tag = 'set'",
            &HashMap::from([("node".to_owned(), uuid_param(SET_NODE))]),
        )
        .expect("SET on an existing node");
    forge
        .execute_with_params(
            "MATCH (a:Entity)-[r:LINK]->(b:Entity) \
             WHERE a.node_uuid = $source AND b.node_uuid = $target DELETE r",
            &HashMap::from([
                ("source".to_owned(), uuid_param(DELETED_EDGE_SOURCE)),
                (
                    "target".to_owned(),
                    uuid_param(DELETED_EDGE_SOURCE + DELETED_EDGE_OFFSET),
                ),
            ]),
        )
        .expect("DELETE of an edge");
    forge
        .execute("MATCH (n:Extra) DELETE n")
        .expect("DELETE of a node");
    forge.index_adjacency().expect("index_adjacency");
    // A composite property SET is the commit that published a delta run, which
    // a later open would replay by reading, copying and re-streaming the graph.
    forge
        .publish_composite_transaction(CompositeTransactionRequest {
            contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
            context: WriteContext {
                operation_uuid: OperationId(uuid::Uuid::now_v7()),
                actor_uuid: None,
            },
            graph_mutations: vec![CompositeGraphMutation::SetNodeProperty {
                node_uuid: bulk_fixture::fixture_node_uuid(COMPOSITE_NODE),
                property: "rank".into(),
                value: PropValue::Int(5),
            }],
            knowledge: CompositeKnowledgeParticipants::default(),
        })
        .expect("composite property SET");
}

fn graph_record_version(project: &Path) -> u32 {
    resolve_project_generation(project)
        .unwrap()
        .participant_snapshot(GRAPH_CAPABILITY_ID, GRAPH_FILES_FAMILY)
        .unwrap()
        .expect("the project records a graph participant")
        .record_version
}

fn is_compact_root(record_version: u32) -> bool {
    matches!(
        record_version,
        GRAPH_FILES_CHECKSUM_ROOT_RECORD_VERSION | GRAPH_FILES_MAPPED_CHECKSUM_ROOT_RECORD_VERSION
    )
}

fn process_rchar() -> Option<u64> {
    std::fs::read_to_string("/proc/self/io")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("rchar: "))?
        .trim()
        .parse()
        .ok()
}

/// What the manifest declares, read without admitting a byte.
struct Layout {
    files: u64,
    node_bytes: u64,
    /// Edge objects plus the published adjacency shards: the payload an open
    /// must never read.
    edge_payload_bytes: u64,
    delta_runs: usize,
}

fn layout(inventory: &GraphFilesInventory) -> Layout {
    let mut layout = Layout {
        files: inventory.files.len() as u64,
        node_bytes: 0,
        edge_payload_bytes: 0,
        delta_runs: 0,
    };
    for file in &inventory.files {
        let path = file.relative_path.as_str();
        if path.starts_with("topology/nodes") {
            layout.node_bytes += file.byte_length;
        } else if path.starts_with("topology/edges/")
            || (path.starts_with("indexes/adjacency/") && path.ends_with(".csr"))
        {
            layout.edge_payload_bytes += file.byte_length;
        } else if path.starts_with("deltas/") {
            layout.delta_runs += 1;
        }
    }
    layout
}

#[derive(Debug)]
struct Open {
    attributed: u64,
    rchar: Option<u64>,
    copied: u64,
    checksummed: u64,
    ids: Vec<Vec<u8>>,
}

fn open_and_query(path: &Path) -> Open {
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let rchar_before = process_rchar();
    let forge = GraphForge::new(Some(path.to_str().expect("utf-8 project path"))).unwrap();
    let rchar_after = process_rchar();
    let open = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("open attribution");
    open.validate_for_qualification().expect("open reconciles");
    let evidence = forge.graph_open_evidence();
    let (copied, checksummed) = (evidence.bytes_copied, evidence.bytes_checksummed);
    let result = forge.execute(ONE_HOP).expect("one-hop query");
    let mut ids = Vec::new();
    for batch in &result.batches {
        let column = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("node_uuid is FixedSizeBinary");
        ids.extend((0..batch.num_rows()).map(|row| column.value(row).to_vec()));
    }
    Open {
        attributed: open.totals.read_bytes,
        rchar: rchar_before.zip(rchar_after).map(|(before, after)| after - before),
        copied,
        checksummed,
        ids,
    }
}

/// The ordered answer after the mutations: node `s` links to the next `FAN_OUT`
/// nodes on a ring, so each node is the destination of `FAN_OUT` edges, one
/// fewer for the node whose incoming edge was deleted.
fn expected_ids(nodes: usize) -> Vec<Vec<u8>> {
    let deleted_target = DELETED_EDGE_SOURCE + DELETED_EDGE_OFFSET;
    (0..nodes)
        .flat_map(|node| {
            let in_degree = if node == deleted_target {
                FAN_OUT - 1
            } else {
                FAN_OUT
            };
            std::iter::repeat_n(
                bulk_fixture::fixture_node_uuid(node).as_bytes().to_vec(),
                in_degree,
            )
        })
        .take(LIMIT)
        .collect()
}

struct Measured {
    nodes: usize,
    layout: Layout,
    open: Open,
}

fn run_size(nodes: usize) -> Measured {
    let project = tempfile::tempdir().expect("project directory");
    let path = project_path(&project);
    bulk_fixture::generate_bulk_graph_with_index(&path, nodes, FAN_OUT, false);
    mutate(&path);
    let record_version = graph_record_version(&path);
    assert!(
        is_compact_root(record_version),
        "{nodes} nodes: the mutated project's graph record version is {record_version}, \
         not a compact root"
    );
    let inventory = resolve_project_generation(&path)
        .unwrap()
        .unadmitted_graph_files_inventory()
        .unwrap()
        .expect("a compact generation declares an inventory");
    let layout = layout(&inventory);
    let open = open_and_query(&path);
    assert_eq!(
        open.ids,
        expected_ids(nodes),
        "{nodes} nodes: wrong answer after the mutations"
    );
    Measured {
        nodes,
        layout,
        open,
    }
}

fn assert_open_bounded(measured: &Measured) {
    let Measured {
        nodes,
        layout,
        open,
    } = measured;
    eprintln!(
        "mutated nodes={nodes} edges={} files={} node_bytes={} edge_payload_bytes={}: \
         open attributed={} rchar={:?} copied={} checksummed={}",
        nodes * FAN_OUT,
        layout.files,
        layout.node_bytes,
        layout.edge_payload_bytes,
        open.attributed,
        open.rchar,
        open.copied,
        open.checksummed
    );
    // Delta runs are never published, and an open that replayed one would read
    // the whole graph.
    assert_eq!(layout.delta_runs, 0, "{nodes} nodes: a delta run is published");
    // Hydration copies the small controls and nothing else: the copied bytes are
    // identity controls, at most `COPIED_CONTROL_BYTES_PER_NODE` a node.
    assert!(
        open.copied <= COPIED_CONTROL_BYTES_PER_NODE * *nodes as u64,
        "{nodes} nodes: {} control bytes copied",
        open.copied
    );
    // The open reads each copied control twice, plus the manifest, route table
    // and sidecars. Never a payload.
    let attributed_bound = HYDRATION_PASSES * open.copied + CONTROL_SLACK_BYTES;
    assert!(
        open.attributed <= attributed_bound,
        "{nodes} nodes: open read {} attributed bytes against a bound of {attributed_bound}",
        open.attributed
    );
    assert!(
        open.checksummed <= attributed_bound,
        "{nodes} nodes: open checksummed {} bytes against a bound of {attributed_bound}",
        open.checksummed
    );
    if let Some(rchar) = open.rchar {
        // Whole-process reads include readers that report nothing to the
        // attribution (the identity readers); they too read controls only.
        let rchar_bound = UNATTRIBUTED_PASSES * open.copied + CONTROL_SLACK_BYTES;
        assert!(
            rchar <= rchar_bound,
            "{nodes} nodes: open read {rchar} bytes (rchar) against a bound of {rchar_bound}"
        );
        assert!(
            open.attributed <= rchar,
            "{nodes} nodes: attributed reads {} exceed process reads {rchar}",
            open.attributed
        );
        // The bound must be tighter than the work it forbids, or it proves
        // nothing: reading the edge payload on top of the controls exceeds it.
        assert!(
            rchar_bound < rchar + layout.edge_payload_bytes,
            "{nodes} nodes: the bound {rchar_bound} admits a full edge payload read \
             ({} bytes)",
            layout.edge_payload_bytes
        );
    }
    assert!(
        attributed_bound < open.attributed + layout.edge_payload_bytes,
        "{nodes} nodes: the bound {attributed_bound} admits a full edge payload read"
    );
}

#[test]
fn mutated_project_open_reads_controls_not_payload_across_a_16x_range() {
    assert_eq!(LARGE_NODES, 16 * SMALL_NODES);
    if process_rchar().is_none() {
        eprintln!(
            "SKIPPED whole-process rchar assertions: /proc/self/io is unavailable on this \
             platform; the attributed assertions still run"
        );
    }
    let small = run_size(SMALL_NODES);
    assert_open_bounded(&small);
    let large = run_size(LARGE_NODES);
    assert_open_bounded(&large);
    // Nodes and edges both grew; the comparison is meaningful only if they did.
    assert!(large.layout.node_bytes > 8 * small.layout.node_bytes);
    assert!(large.layout.edge_payload_bytes > 8 * small.layout.edge_payload_bytes);
    // The file count grows by at most a handful of shards, not with the data.
    assert!(large.layout.files < 2 * small.layout.files);
}

/// Flip one byte of a content-store object in place: same inode, same length.
/// Restores it on drop so a project can exercise several payloads in turn.
#[cfg(unix)]
struct InPlaceFlip {
    object: PathBuf,
    permissions: std::fs::Permissions,
    original: Vec<u8>,
}

#[cfg(unix)]
impl InPlaceFlip {
    fn apply(project: &Path, entry: &graphforge_storage::GraphFileEntry, offset: usize) -> Self {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let object = graphforge_storage::graph_object_path(project, &entry.content_sha256).unwrap();
        let before = std::fs::metadata(&object).unwrap();
        let permissions = before.permissions();
        let original = std::fs::read(&object).unwrap();
        let mut flipped = original.clone();
        flipped[offset] ^= 0xff;
        std::fs::set_permissions(&object, std::fs::Permissions::from_mode(before.mode() | 0o200))
            .unwrap();
        let mut file = std::fs::OpenOptions::new().write(true).open(&object).unwrap();
        std::io::Write::write_all(&mut file, &flipped).unwrap();
        file.sync_all().unwrap();
        let after = std::fs::metadata(&object).unwrap();
        assert_eq!(after.ino(), before.ino(), "the flip must keep the inode");
        assert_eq!(after.len(), before.len(), "the flip must keep the length");
        Self {
            object,
            permissions,
            original,
        }
    }
}

#[cfg(unix)]
impl Drop for InPlaceFlip {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        let mut writable = self.permissions.clone();
        writable.set_mode(writable.mode() | 0o200);
        std::fs::set_permissions(&self.object, writable).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&self.object)
            .unwrap();
        std::io::Write::write_all(&mut file, &self.original).unwrap();
        file.sync_all().unwrap();
        std::fs::set_permissions(&self.object, self.permissions.clone()).unwrap();
    }
}

/// A byte no decoder reads as data, so that only a checksum can refuse its
/// change: one letter of the `created_by` string a Parquet footer carries, or
/// the middle of an adjacency shard's payload (a shard has no structure that a
/// flipped value byte breaks).
#[cfg(unix)]
fn inert_offset(project: &Path, entry: &graphforge_storage::GraphFileEntry) -> usize {
    let object = graphforge_storage::graph_object_path(project, &entry.content_sha256).unwrap();
    let bytes = std::fs::read(object).unwrap();
    if entry.relative_path.ends_with(".parquet") {
        let marker = b"graphforge permanent parquet";
        bytes
            .windows(marker.len())
            .position(|window| window == marker)
            .expect("the writer stamps created_by into the footer")
            + 3
    } else {
        bytes.len() / 2
    }
}

/// Criterion 4: a payload of a mutated (now compact) project that is corrupted
/// in place is accepted by the open, which reads no payload, and refused by the
/// query that touches it. The same project answers correctly before and after.
#[cfg(unix)]
#[test]
fn mutated_project_refuses_a_same_inode_flip_on_the_touching_query() {
    let project = tempfile::tempdir().expect("project directory");
    let path = project_path(&project);
    let nodes = SMALL_NODES / 2;
    bulk_fixture::generate_bulk_graph_with_index(&path, nodes, FAN_OUT, false);
    mutate(&path);
    assert!(is_compact_root(graph_record_version(&path)));
    let location = path.to_str().unwrap();
    let inventory = resolve_project_generation(&path)
        .unwrap()
        .graph_files_inventory()
        .unwrap()
        .unwrap();
    let find = |prefix: &str, suffix: &str| {
        inventory
            .files
            .iter()
            .find(|entry| {
                entry.relative_path.starts_with(prefix)
                    && entry.relative_path.ends_with(suffix)
                    && entry.byte_length > 16
            })
            .unwrap_or_else(|| panic!("the mutated project lacks a {prefix}*{suffix} payload"))
            .clone()
    };
    // The query that touches each payload. Property fragments are still
    // authenticated in full by the property overlay when the project opens (not
    // yet first-touch, #1388 decision 3), so their refusal may come from the open;
    // the other bulk payloads must be accepted by the open and refused by the
    // query. The adjacency index is derived: a shard that fails its own checksum
    // is never served and the hop answers from the edge table instead, so its
    // answer must stay exactly right.
    #[derive(Clone, Copy, PartialEq)]
    enum Expect {
        RefusedByQuery,
        RefusedByOpenOrQuery,
        NeverServed,
    }
    let cases = [
        (
            "node object",
            find("topology/nodes/", ".parquet"),
            "MATCH (n:Entity) RETURN count(n) AS total",
            Expect::RefusedByQuery,
        ),
        (
            "edge object",
            find("topology/edges/", ".parquet"),
            // Not `count(*)`: with the adjacency index published, the count
            // is answered from the index and touches no edge object.
            "MATCH ()-[r]->() RETURN r.edge_uuid AS id ORDER BY id LIMIT 5",
            Expect::RefusedByQuery,
        ),
        (
            "property fragment",
            find("properties/", ".parquet"),
            "MATCH (n:Entity) WHERE n.tag = 'set' RETURN n.tag AS tag",
            Expect::RefusedByOpenOrQuery,
        ),
        (
            "adjacency shard",
            // The outgoing shard: a one-hop expand follows out-edges.
            inventory
                .files
                .iter()
                .find(|entry| {
                    entry.relative_path.contains(".out.csr.shards-")
                        && entry.relative_path.ends_with(".csr")
                })
                .expect("the mutated project ships an outgoing adjacency shard")
                .clone(),
            ONE_HOP,
            Expect::NeverServed,
        ),
    ];
    for (what, entry, query, expect) in cases {
        let _flip = InPlaceFlip::apply(&path, &entry, inert_offset(&path, &entry));
        let opened = GraphForge::new(Some(location));
        if expect == Expect::NeverServed {
            drop(opened.expect("the open reads no adjacency payload"));
            assert_eq!(
                open_and_query(&path).ids,
                expected_ids(nodes),
                "{what} ({}): a corrupted shard changed an answer",
                entry.relative_path
            );
            continue;
        }
        let refused = match opened {
            Err(error) => {
                assert!(
                    expect == Expect::RefusedByOpenOrQuery,
                    "{what} ({}): the open must read no payload: {error}",
                    entry.relative_path
                );
                error
            }
            Ok(reopened) => reopened.execute(query).err().unwrap_or_else(|| {
                panic!(
                    "{what} ({}): a flipped payload answered a query",
                    entry.relative_path
                )
            }),
        };
        let message = refused.to_string().to_lowercase();
        assert!(
            message.contains("checksum") || message.contains("digest") || message.contains("corrupt"),
            "{what} ({}): refused for the wrong reason: {refused}",
            entry.relative_path
        );
    }
    // Every flip was restored: the project answers again, and correctly.
    assert_eq!(open_and_query(&path).ids, expected_ids(nodes));
}

/// A commit of each kind, from an empty project and from a constructed parent.
#[derive(Clone, Copy, Debug)]
enum Commit {
    Create,
    Set,
    Delete,
    IndexAdjacency,
}

const COMMITS: [Commit; 4] = [
    Commit::Create,
    Commit::Set,
    Commit::Delete,
    Commit::IndexAdjacency,
];
const PARENT_NODES: usize = 128;
const PARENT_FAN_OUT: usize = 4;

fn commit_over_empty_project(forge: &GraphForge, commit: Commit) {
    match commit {
        Commit::Create => forge.execute("CREATE (:Entity {name: 'a'})").map(drop),
        // The first commit is itself a SET: the node exists only to be set.
        Commit::Set => forge
            .execute("MERGE (n:Entity {name: 'a'}) SET n.tag = 'set'")
            .map(drop),
        Commit::Delete => forge
            .execute("CREATE (n:Entity {name: 'a'}) DELETE n")
            .map(drop),
        Commit::IndexAdjacency => forge.index_adjacency().map(drop),
    }
    .unwrap_or_else(|error| panic!("{commit:?} over an empty project: {error}"));
}

fn commit_over_constructed_parent(forge: &GraphForge, commit: Commit) {
    match commit {
        Commit::Create => forge.execute("CREATE (:Extra {name: 'created'})").map(drop),
        Commit::Set => forge
            .execute_with_params(
                "MATCH (n:Entity) WHERE n.node_uuid = $node SET n.tag = 'set'",
                &HashMap::from([("node".to_owned(), uuid_param(SET_NODE))]),
            )
            .map(drop),
        Commit::Delete => forge
            .execute_with_params(
                "MATCH (a:Entity)-[r:LINK]->(b:Entity) \
                 WHERE a.node_uuid = $source AND b.node_uuid = $target DELETE r",
                &HashMap::from([
                    ("source".to_owned(), uuid_param(DELETED_EDGE_SOURCE)),
                    (
                        "target".to_owned(),
                        uuid_param(DELETED_EDGE_SOURCE + DELETED_EDGE_OFFSET),
                    ),
                ]),
            )
            .map(drop),
        Commit::IndexAdjacency => forge.index_adjacency().map(drop),
    }
    .unwrap_or_else(|error| panic!("{commit:?} over a constructed parent: {error}"));
}

/// Criterion 1's precondition and decisions 1 and 2: every mutating commit
/// publishes a compact root, whatever the parent was, and no delta run. The open
/// that follows defers payload content to first touch.
#[test]
fn every_mutating_commit_publishes_a_compact_root() {
    for constructed in [false, true] {
        for commit in COMMITS {
            let project = tempfile::tempdir().expect("project directory");
            let path = project_path(&project);
            let label = format!("{commit:?} over {}", if constructed { "a constructed parent" } else { "an empty project" });
            if constructed {
                bulk_fixture::generate_bulk_graph_with_index(
                    &path,
                    PARENT_NODES,
                    PARENT_FAN_OUT,
                    false,
                );
                let parent = graph_record_version(&path);
                assert!(is_compact_root(parent), "{label}: the constructed parent is version {parent}");
            }
            let generation_before = resolve_project_generation(&path)
                .ok()
                .map(|generation| generation.generation_uuid());
            {
                let forge = GraphForge::new(path.to_str()).unwrap();
                if constructed {
                    commit_over_constructed_parent(&forge, commit);
                } else {
                    commit_over_empty_project(&forge, commit);
                }
            }
            let published = resolve_project_generation(&path).unwrap();
            assert_ne!(
                Some(published.generation_uuid()),
                generation_before,
                "{label}: the commit published nothing"
            );
            let record_version = graph_record_version(&path);
            assert!(
                is_compact_root(record_version),
                "{label}: published graph record version {record_version}, not a compact root"
            );
            assert!(
                published.declared_graph_files_inventory().unwrap().is_none(),
                "{label}: the generation owns an expanded inventory"
            );
            assert!(
                !published.graph_tree_root().exists(),
                "{label}: the generation owns a graph tree"
            );
            let inventory = published.graph_files_inventory().unwrap().unwrap();
            assert!(
                !inventory
                    .files
                    .iter()
                    .any(|file| file.relative_path.starts_with("deltas/")),
                "{label}: a delta run is published"
            );
            drop(published);
            // The open reads controls, not payload: content waits for first touch.
            let reopened = GraphForge::new(path.to_str()).unwrap();
            let evidence = reopened.graph_open_evidence();
            assert!(evidence.files_reused > 0, "{label}: nothing was hard-linked at open");
            assert!(
                evidence.bytes_checksummed
                    <= HYDRATION_PASSES * evidence.bytes_copied + CONTROL_SLACK_BYTES,
                "{label}: open checksummed {} bytes for {} copied",
                evidence.bytes_checksummed,
                evidence.bytes_copied
            );
        }
    }
}

/// Decision 2: an expanded (V1) generation that exists already stays readable
/// unchanged, and converts to a compact root on its next commit with every
/// answer the same.
#[test]
fn expanded_parent_is_converted_on_next_commit() {
    let source = tempfile::tempdir().expect("source project directory");
    let source_path = project_path(&source);
    bulk_fixture::generate_bulk_graph_with_index(&source_path, PARENT_NODES, PARENT_FAN_OUT, false);
    GraphForge::new(source_path.to_str())
        .unwrap()
        .execute("CREATE (:Extra {name: 'before'})")
        .unwrap();
    let target = tempfile::tempdir().expect("expanded project directory");
    project_fixture::publish_expanded_copy(&source_path, target.path());

    let parent = resolve_project_generation(target.path()).unwrap();
    assert!(
        parent.declared_graph_files_inventory().unwrap().is_some(),
        "the seeded parent must be an expanded inventory"
    );
    assert!(!is_compact_root(graph_record_version(target.path())));
    drop(parent);

    let answers = |forge: &GraphForge| {
        let count = |query: &str| -> i64 {
            let result = forge.execute(query).unwrap();
            result.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0)
        };
        let ids = {
            let mut ids = Vec::new();
            for batch in forge.execute(ONE_HOP).unwrap().batches {
                let column = batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap();
                ids.extend((0..batch.num_rows()).map(|row| column.value(row).to_vec()));
            }
            ids
        };
        (
            count("MATCH (n:Entity) RETURN count(n) AS total"),
            count("MATCH (n:Extra) RETURN count(n) AS total"),
            count("MATCH ()-[r]->() RETURN count(*) AS total"),
            ids,
        )
    };
    let forge = GraphForge::new(target.path().to_str()).expect("the expanded parent opens");
    let before = answers(&forge);
    assert_eq!(before.0, PARENT_NODES as i64);
    assert_eq!(before.1, 1);
    assert_eq!(before.2, (PARENT_NODES * PARENT_FAN_OUT) as i64);
    // Expanded generations still verify and copy their whole tree at open.
    assert_eq!(forge.graph_open_evidence().files_copied, forge.graph_open_evidence().files_validated);

    forge.execute("CREATE (:Extra {name: 'after'})").unwrap();
    drop(forge);
    let record_version = graph_record_version(target.path());
    assert!(
        is_compact_root(record_version),
        "the next commit published record version {record_version}, not a compact root"
    );
    let converted = GraphForge::new(target.path().to_str()).unwrap();
    assert!(
        converted.graph_open_evidence().files_reused > 0,
        "the converted generation hard-links its payloads"
    );
    let after = answers(&converted);
    assert_eq!(after.0, before.0, "every Entity answer is unchanged");
    assert_eq!(after.1, before.1 + 1, "the new node is there");
    assert_eq!(after.2, before.2, "every edge answer is unchanged");
    assert_eq!(after.3, before.3, "the ordered one-hop answer is unchanged");
}
