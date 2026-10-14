//! The overlay's targeted property read opens only the fragments whose UUID
//! range can hold a wanted key (#1984).
//!
//! A statement that rewrites a handful of nodes first reads their stored
//! properties, and that read used to open and authenticate every fragment of the
//! `_untyped` route. The fixture builds a route with the layouts the reader
//! meets at scale: one label cut into disjoint, increasing range fragments by
//! the writer's size cap, a second label held by one fragment whose range spans
//! the first, and, after mutation, a tombstone fragment and a newer fragment
//! holding rewritten rows. The tests read the fragment ranges from the Parquet
//! footers, derive which fragments the read must open from where the wanted rows
//! actually live, and compare that with the fragments the reader reports it
//! opened and the bytes it authenticated. Each test then reads the rewritten
//! nodes back, so a fragment skipped in error shows as a lost property.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int64Array, Int64Builder,
    RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ExecutionResourcePolicy,
    GraphConstructionBudgets, GraphForge, GraphForgeOptions, LifecycleIoCapture,
};
use graphforge_core::uuid::Uuid;
use graphforge_ir::IrLiteral;
use graphforge_storage::{io_stats, resolve_project_generation};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const ENTITIES: usize = 4_000;
const PAYLOAD_BYTES: usize = 4_000;

fn entity_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// Notes lie below and above every entity, so their one fragment spans the
/// entities' ranges without holding a wanted key.
fn note_uuid(index: usize) -> Uuid {
    let high = [0x0f00_u128, 0x1f00_u128][index];
    Uuid::from_u128((high << 112) | 1)
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// Incompressible and unique per node, so a fragment's size is its row count.
fn payload(node: usize) -> String {
    let mut state = (node as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = String::with_capacity(PAYLOAD_BYTES + 16);
    while out.len() < PAYLOAD_BYTES {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push_str(&format!("{state:016x}"));
    }
    out.truncate(PAYLOAD_BYTES);
    out
}

/// Entity idents whose destination rows the measured statement reads. They lie
/// in the first and third range fragments of the entities.
const WANTED: [usize; 2] = [10, 2_500];
const ANCHOR: usize = ENTITIES - 1;
/// An entity with no edges, in the last range fragment: its deletion writes a
/// tombstone no wanted key can fall in.
const UNRELATED: usize = 3_500;

fn node_batch(
    schema: &Arc<Schema>,
    uuids: &[Uuid],
    idents: &[i64],
    payloads: Vec<String>,
    label: &str,
) -> RecordBatch {
    let mut ids = FixedSizeBinaryBuilder::with_capacity(uuids.len(), 16);
    for uuid in uuids {
        ids.append_value(uuid.as_bytes()).unwrap();
    }
    let mut ident_column = Int64Builder::with_capacity(idents.len());
    for ident in idents {
        ident_column.append_value(*ident);
    }
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec![label; uuids.len()])),
            Arc::new(ident_column.finish()),
            Arc::new(StringArray::from(payloads)),
        ],
    )
    .unwrap()
}

fn construct(dir: &Path) {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("ident", DataType::Int64, true)));
    fields.push(Arc::new(Field::new("payload", DataType::Utf8, true)));
    let schema = Arc::new(Schema::new(fields));
    let forge = GraphForge::new(Some(dir.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets::default())
        .unwrap();
    let entities = (0..ENTITIES).map(entity_uuid).collect::<Vec<_>>();
    let idents = (0..ENTITIES as i64).collect::<Vec<_>>();
    session
        .append_nodes(
            "entities",
            &node_batch(
                &schema,
                &entities,
                &idents,
                (0..ENTITIES).map(payload).collect(),
                "Entity",
            ),
        )
        .unwrap();
    session
        .append_nodes(
            "notes",
            &node_batch(
                &schema,
                &[note_uuid(0), note_uuid(1)],
                &[100_000, 100_001],
                vec!["low".to_owned(), "high".to_owned()],
                "Note",
            ),
        )
        .unwrap();
    let sources = [ANCHOR, ANCHOR];
    let mut ids = FixedSizeBinaryBuilder::with_capacity(2, 16);
    let mut from = FixedSizeBinaryBuilder::with_capacity(2, 16);
    let mut to = FixedSizeBinaryBuilder::with_capacity(2, 16);
    for (edge, (source, target)) in sources.iter().zip(WANTED).enumerate() {
        ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
        from.append_value(entity_uuid(*source).as_bytes()).unwrap();
        to.append_value(entity_uuid(target).as_bytes()).unwrap();
    }
    let edges = RecordBatch::try_new(
        Arc::clone(&CONSTRUCTION_EDGE_SCHEMA),
        vec![
            Arc::new(ids.finish()) as ArrayRef,
            Arc::new(StringArray::from(vec!["LINK"; 2])),
            Arc::new(from.finish()),
            Arc::new(to.finish()),
        ],
    )
    .unwrap();
    session.append_edges("edges", &edges).unwrap();
    session.seal_and_publish().unwrap();
}

fn open(project: &Path, target_partitions: usize) -> GraphForge {
    GraphForge::new_with_options(
        Some(project.to_str().expect("utf-8 path")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(target_partitions),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .unwrap()
}

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One overlay property fragment as the manifest and its footer declare it.
#[derive(Debug, Clone)]
struct Fragment {
    path: String,
    bytes: u64,
    /// Smallest and largest `node_uuid` of the file, from its column statistics.
    range: ([u8; 16], [u8; 16]),
    /// Every `node_uuid` the file holds.
    keys: Vec<[u8; 16]>,
}

/// The `properties/` fragments of the current generation, in manifest order.
fn fragments(project: &Path) -> Vec<Fragment> {
    let generation = resolve_project_generation(project).expect("project resolves");
    let inventory = generation
        .graph_files_inventory()
        .expect("inventory reads")
        .expect("generation declares an inventory");
    // A compact generation keeps payloads in the content-addressed object
    // store; read them from a private materialization that this call drops.
    let workspace = tempfile::tempdir_in(project).expect("workspace");
    graphforge_storage::materialize_graph_objects(project, &inventory, workspace.path())
        .expect("materializes");
    let root = workspace.path();
    inventory
        .files
        .iter()
        .filter(|file| file.relative_path.starts_with("properties/"))
        .map(|file| {
            let reader = ParquetRecordBatchReaderBuilder::try_new(
                std::fs::File::open(root.join(&file.relative_path)).unwrap(),
            )
            .unwrap()
            .with_batch_size(8_192)
            .build()
            .unwrap();
            let mut keys = Vec::new();
            for batch in reader {
                let batch = batch.unwrap();
                let column = batch
                    .column_by_name("node_uuid")
                    .expect("node_uuid column")
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .expect("node_uuid is FixedSizeBinary(16)");
                keys.extend(
                    (0..column.len()).map(|row| {
                        <[u8; 16]>::try_from(column.value(row)).expect("16-byte identity")
                    }),
                );
            }
            let low = *keys.iter().min().expect("fragment holds rows");
            let high = *keys.iter().max().expect("fragment holds rows");
            Fragment {
                path: file.relative_path.clone(),
                bytes: file.byte_length,
                range: (low, high),
                keys,
            }
        })
        .collect()
}

/// The enrichment read the update must leave correct: the frontier is one
/// anchor, the wanted keys are its two neighbours, and both properties are
/// projected.
const READ: &str = "MATCH (a:Entity)-[e:LINK]->(b:Entity) WHERE a.node_uuid = $node \
                    RETURN b.ident AS ident, b.payload AS payload ORDER BY ident";

/// The statement under test. It matches two nodes that live in different range
/// fragments and rewrites one property. To do so it reads their stored
/// properties through the targeted snapshot read, then writes the merged rows;
/// a fragment wrongly skipped by that read drops a property the statement never
/// named, and `ident` is gone from the node afterwards.
fn update_statement() -> String {
    format!(
        "MATCH (n:Entity) WHERE n.ident = {} OR n.ident = {} SET n.payload = 'changed'",
        WANTED[0], WANTED[1]
    )
}

type Rows = Vec<(i64, String)>;

fn execute(forge: &GraphForge, query: &str, node: Option<usize>) -> Vec<RecordBatch> {
    let params = node
        .map(|node| {
            HashMap::from([(
                "node".to_owned(),
                IrLiteral::Uuid(*entity_uuid(node).as_bytes()),
            )])
        })
        .unwrap_or_default();
    forge
        .execute_with_params(query, &params)
        .expect("statement executes")
        .batches
}

fn rows(batches: &[RecordBatch]) -> Rows {
    let mut out = Vec::new();
    for batch in batches {
        let idents = batch
            .column_by_name("ident")
            .expect("ident column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ident is Int64");
        let payloads = batch
            .column_by_name("payload")
            .expect("payload column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload is Utf8");
        out.extend(
            (0..batch.num_rows()).map(|row| (idents.value(row), payloads.value(row).to_owned())),
        );
    }
    out
}

/// What the overlay reader reports having opened and authenticated while the
/// measured statement ran.
struct Measured {
    opened: u64,
    authentication_bytes: u64,
}

/// Runs the update once under an I/O capture. The statement mutates the
/// project, so the caller supplies a project no other measurement has touched.
fn update(project: &Path, target_partitions: usize) -> Measured {
    let forge = open(project, target_partitions);
    let _capture = LifecycleIoCapture::install();
    io_stats::reset();
    execute(&forge, &update_statement(), None);
    let io = io_stats::snapshot().expect("requested I/O statistics");
    Measured {
        opened: io.property_fragments_opened,
        authentication_bytes: io.property_fragment_authentication_bytes,
    }
}

/// The rows the enrichment read returns once both wanted nodes are rewritten.
fn rewritten_rows() -> Rows {
    WANTED
        .iter()
        .map(|node| (*node as i64, "changed".to_owned()))
        .collect()
}

/// The fragments the update's snapshot read must open, derived from where the
/// wanted rows actually live. Newest first, a fragment opens when its range can
/// hold a key that no newer opened fragment has resolved; opening it resolves
/// every wanted key it holds.
fn expected_opens(fragments: &[Fragment]) -> Vec<usize> {
    let mut unresolved = WANTED
        .iter()
        .map(|node| *entity_uuid(*node).as_bytes())
        .collect::<Vec<_>>();
    let mut opens = Vec::new();
    for (index, fragment) in fragments.iter().enumerate().rev() {
        let (low, high) = fragment.range;
        if !unresolved.iter().any(|key| low <= *key && *key <= high) {
            continue;
        }
        opens.push(index);
        unresolved.retain(|key| !fragment.keys.contains(key));
    }
    opens
}

/// Builds a project, applies `prepare`, runs the update at `partitions`, and
/// checks the reader opened exactly the fragments `expected_opens` derives from
/// the layout, authenticated exactly their bytes, and left both nodes complete.
fn assert_update_opens_exactly(
    partitions: usize,
    prepare: impl Fn(&Path, &[Fragment]),
    expected_count: usize,
) {
    let (_dir, project) = built();
    let base = fragments(&project);
    prepare(&project, &base);
    let layout = fragments(&project);
    let opens = expected_opens(&layout);
    assert_eq!(
        opens.len(),
        expected_count,
        "fixture layout changed: {:#?}",
        layout
            .iter()
            .map(|fragment| (
                &fragment.path,
                fragment.bytes,
                fragment.keys.len(),
                fragment.range
            ))
            .collect::<Vec<_>>()
    );
    let measured = update(&project, partitions);
    assert_eq!(
        measured.opened,
        opens.len() as u64,
        "{partitions} partitions: expected fragments {:?} of {}",
        opens,
        layout.len()
    );
    assert_eq!(
        measured.authentication_bytes,
        opens.iter().map(|index| layout[*index].bytes).sum::<u64>(),
        "{partitions} partitions: authenticated bytes of fragments {opens:?}"
    );
    // The rewrite kept every property it did not name: `ident` still finds the
    // nodes and `payload` carries the new value.
    let forge = open(&project, partitions);
    assert_eq!(
        rows(&execute(&forge, READ, Some(ANCHOR))),
        rewritten_rows(),
        "{partitions} partitions: the enrichment read after the update"
    );
}

fn built() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("state");
    construct(&project);
    (dir, project)
}

/// Four entity range fragments plus one whole-range fragment of another label.
fn assert_base_layout(layout: &[Fragment]) {
    assert_eq!(layout.len(), 5, "fixture layout changed");
    let entity_ranges = &layout[..4];
    for pair in entity_ranges.windows(2) {
        assert!(
            pair[0].range.1 < pair[1].range.0,
            "entity ranges are disjoint and increasing"
        );
    }
    let note = &layout[4];
    assert_eq!(note.keys.len(), 2, "the note fragment holds both notes");
    assert!(
        note.range.0 < entity_ranges[0].range.0 && entity_ranges[3].range.1 < note.range.1,
        "the note fragment spans every entity range"
    );
    let holding = |node: usize| {
        let key = *entity_uuid(node).as_bytes();
        entity_ranges
            .iter()
            .position(|fragment| fragment.keys.contains(&key))
            .expect("wanted key is in an entity fragment")
    };
    assert_ne!(
        holding(WANTED[0]),
        holding(WANTED[1]),
        "wanted keys fall in two ranges"
    );
}

#[test]
fn range_fragments_open_only_where_a_wanted_key_can_live() {
    let _serial = serial();
    for partitions in [1, 4] {
        // Newest first: the note fragment (its range spans the wanted keys, yet
        // it holds neither), then the two entity fragments that hold them. The
        // other two entity fragments are never opened.
        assert_update_opens_exactly(partitions, |_, base| assert_base_layout(base), 3);
    }
}

#[test]
fn a_tombstone_fragment_outside_the_wanted_keys_is_not_opened() {
    let _serial = serial();
    for partitions in [1, 4] {
        assert_update_opens_exactly(
            partitions,
            |project, _| {
                execute(
                    &open(project, 1),
                    &format!("MATCH (n:Entity) WHERE n.ident = {UNRELATED} DELETE n"),
                    None,
                );
                let layout = fragments(project);
                assert_eq!(layout.len(), 6, "the delete adds one tombstone fragment");
                assert_eq!(
                    layout[5].keys.len(),
                    1,
                    "the tombstone names the deleted node"
                );
                let wanted = WANTED.map(|node| *entity_uuid(node).as_bytes());
                assert!(
                    wanted
                        .iter()
                        .all(|key| *key < layout[5].range.0 || layout[5].range.1 < *key),
                    "no wanted key falls in the tombstone's range"
                );
            },
            3,
        );
    }
}

#[test]
fn a_tombstone_fragment_spanning_a_wanted_key_is_opened_and_does_not_hide_it() {
    let _serial = serial();
    for partitions in [1, 4] {
        assert_update_opens_exactly(
            partitions,
            |project, _| {
                // One statement, so one tombstone fragment whose range spans the
                // second wanted key without holding it.
                execute(
                    &open(project, 1),
                    &format!(
                        "MATCH (n:Entity) WHERE n.ident = {} OR n.ident = {UNRELATED} DELETE n",
                        WANTED[0] + 1
                    ),
                    None,
                );
                let layout = fragments(project);
                assert_eq!(layout.len(), 6, "the delete adds one tombstone fragment");
                assert_eq!(
                    layout[5].keys.len(),
                    2,
                    "the tombstone names both deleted nodes"
                );
                let wanted = *entity_uuid(WANTED[1]).as_bytes();
                assert!(
                    layout[5].range.0 < wanted
                        && wanted < layout[5].range.1
                        && !layout[5].keys.contains(&wanted),
                    "the tombstone's range spans a wanted key it does not hold"
                );
            },
            4,
        );
    }
}

#[test]
fn a_newer_fragment_holding_a_wanted_key_spares_the_older_fragment() {
    let _serial = serial();
    for partitions in [1, 4] {
        assert_update_opens_exactly(
            partitions,
            |project, _| {
                execute(
                    &open(project, 1),
                    &format!(
                        "MATCH (n:Entity) WHERE n.ident = {} SET n.payload = 'earlier'",
                        WANTED[1]
                    ),
                    None,
                );
                let layout = fragments(project);
                assert_eq!(
                    layout.len(),
                    6,
                    "the update adds one fragment of rewritten rows"
                );
                assert_eq!(layout[5].keys, vec![*entity_uuid(WANTED[1]).as_bytes()]);
            },
            // The rewritten row resolves its key, so the entity fragment that
            // still holds the stale row is not opened.
            3,
        );
    }
}
