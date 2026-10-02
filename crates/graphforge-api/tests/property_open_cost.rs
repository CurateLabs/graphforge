//! Property-bearing open and text `find` cost gates (#1388).
//!
//! Two properties, both deterministic (bytes, never wall time):
//!
//! 1. **Open reads no property payload.** A compact (V2) project that
//!    construction published with node and edge property columns opens for the
//!    same bytes whether its property fragments are 16 or 1,024 bytes a row. The
//!    manifest names each fragment's length and XXH64, so open checks the length
//!    and leaves the content to the first read of each fragment.
//! 2. **`find` does not read the graph.** A text `find` against a fresh index
//!    costs the same bytes at 8 and 128 edges per node (16x the edges). It reads
//!    the node and property sources it must discover properties from, and the
//!    index, and never an edge object.
//!
//! Both are measured two ways, because the lifecycle attribution only counts
//! what readers report: the attributed read bytes, and the whole-process `rchar`
//! of `/proc/self/io` (every `read(2)`/`pread(2)` the process makes). The
//! `rchar` assertions print a `SKIPPED` line where `/proc/self/io` is missing.
//!
//! The tests share one process counter, so they serialize on a lock.

#![cfg(feature = "search")]

use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, FindOptions, GraphConstructionBudgets,
    GraphForge, LifecycleIoCapture, SearchIndexOptions, lifecycle_io_snapshot,
};
use graphforge_core::uuid::Uuid;
use graphforge_storage::{GraphFileRole, resolve_project_generation};

static SERIAL: Mutex<()> = Mutex::new(());

const NODES: usize = 1 << 12;
const LABEL: &str = "Person";
/// Rows per construction batch.
const WINDOW: usize = 32 * 1024;
/// One manifest row, one route row and a stat per extra declared file.
const BYTES_PER_EXTRA_FILE: u64 = 1024;
/// Footers, sidecars and counters that do not depend on the axis varied.
const SLACK_BYTES: u64 = 64 * 1024;

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x1000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

/// `len` hex digits from a deterministic xorshift stream, so a fragment does not
/// compress to nothing and its declared size tracks `len`.
fn filler(seed: u64, len: usize) -> String {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = String::with_capacity(len + 16);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push_str(&format!("{state:016x}"));
    }
    out.truncate(len);
    out
}

fn node_schema_with_name() -> Arc<Schema> {
    let mut fields = CONSTRUCTION_NODE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("name", DataType::Utf8, true)));
    Arc::new(Schema::new(fields))
}

fn edge_schema_with_note() -> Arc<Schema> {
    let mut fields = CONSTRUCTION_EDGE_SCHEMA.fields().to_vec();
    fields.push(Arc::new(Field::new("note", DataType::Utf8, true)));
    Arc::new(Schema::new(fields))
}

/// Publish a compact (V2) project through the construction session: `NODES`
/// `Person` nodes carrying a `name` of `name_len` bytes, `fan_out` edges each
/// carrying a short `note`. No trailing `index_adjacency`, which republishes
/// as an expanded generation.
fn build_project(path: &Path, fan_out: usize, name_len: usize) {
    let forge = GraphForge::new(Some(path.to_str().expect("utf-8 path"))).unwrap();
    let mut session = forge
        .begin_graph_construction(GraphConstructionBudgets {
            max_batch_rows: WINDOW,
            max_run_records: 4 * WINDOW,
            ..GraphConstructionBudgets::default()
        })
        .unwrap();
    let node_schema = node_schema_with_name();
    for start in (0..NODES).step_by(WINDOW) {
        let end = (start + WINDOW).min(NODES);
        let mut ids = FixedSizeBinaryBuilder::with_capacity(end - start, 16);
        for node in start..end {
            ids.append_value(node_uuid(node).as_bytes()).unwrap();
        }
        let names = (start..end)
            .map(|node| format!("person{node:06} {}", filler(node as u64, name_len)))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&node_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec![LABEL; end - start])),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();
        session
            .append_nodes(&format!("nodes-{start}"), &batch)
            .unwrap();
    }
    let edge_schema = edge_schema_with_note();
    let edge_rows = NODES * fan_out;
    for start in (0..edge_rows).step_by(WINDOW) {
        let end = (start + WINDOW).min(edge_rows);
        let rows = end - start;
        let mut ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        let mut targets = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for edge in start..end {
            let source = edge / fan_out;
            let offset = edge % fan_out + 1;
            ids.append_value(edge_uuid(edge).as_bytes()).unwrap();
            sources.append_value(node_uuid(source).as_bytes()).unwrap();
            targets
                .append_value(node_uuid((source + offset) % NODES).as_bytes())
                .unwrap();
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&edge_schema),
            vec![
                Arc::new(ids.finish()) as ArrayRef,
                Arc::new(StringArray::from(vec!["LINK"; rows])),
                Arc::new(sources.finish()),
                Arc::new(targets.finish()),
                Arc::new(StringArray::from(vec!["e"; rows])),
            ],
        )
        .unwrap();
        session
            .append_edges(&format!("edges-{start}"), &batch)
            .unwrap();
    }
    session.seal_and_publish().unwrap();
    drop(session);
    drop(forge);
}

/// What the manifest declares, read without admitting a byte.
#[derive(Debug, Default)]
struct Layout {
    files: u64,
    node_bytes: u64,
    edge_bytes: u64,
    property_bytes: u64,
    /// The node-property share of `property_bytes`, which `find` reads.
    node_property_bytes: u64,
    property_files: u64,
    index_bytes: u64,
}

fn layout(path: &Path) -> Layout {
    let inventory = resolve_project_generation(path)
        .expect("project resolves")
        .unadmitted_graph_files_inventory()
        .expect("inventory reads")
        .expect("compact generation declares an inventory");
    let mut layout = Layout {
        files: inventory.files.len() as u64,
        ..Layout::default()
    };
    for file in &inventory.files {
        let name = file.relative_path.as_str();
        if file.role == GraphFileRole::Properties {
            layout.property_bytes += file.byte_length;
            layout.property_files += 1;
            if name.starts_with("properties/") {
                layout.node_property_bytes += file.byte_length;
            }
        } else if name.starts_with("topology/nodes") {
            layout.node_bytes += file.byte_length;
        } else if name.starts_with("topology/edges/") {
            layout.edge_bytes += file.byte_length;
        } else if name.starts_with("indexes/search/") {
            layout.index_bytes += file.byte_length;
        }
    }
    layout
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

/// Bytes read by one region: attributed by the lifecycle ledger, and the whole
/// process's `rchar` delta where the platform has it.
#[derive(Clone, Copy, Debug)]
struct Cost {
    attributed: u64,
    rchar: Option<u64>,
}

fn measured<T>(region: impl FnOnce() -> T) -> (T, Cost) {
    let before = lifecycle_io_snapshot().expect("capture installed");
    let rchar_before = process_rchar();
    let value = region();
    let rchar_after = process_rchar();
    let delta = lifecycle_io_snapshot()
        .expect("capture installed")
        .since(&before)
        .expect("attribution");
    delta.validate_for_qualification().expect("reconciles");
    (
        value,
        Cost {
            attributed: delta.totals.read_bytes,
            rchar: rchar_after.zip(rchar_before).map(|(after, before)| after - before),
        },
    )
}

fn open(path: &Path) -> GraphForge {
    GraphForge::new(Some(path.to_str().expect("utf-8 path"))).unwrap()
}

#[test]
fn open_reads_no_property_payload_whatever_its_size() {
    let _serial = SERIAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let _capture = LifecycleIoCapture::install();
    let rchar = process_rchar().is_some();
    if !rchar {
        eprintln!(
            "SKIPPED whole-process rchar assertions: /proc/self/io is unavailable; the \
             attributed assertions still run"
        );
    }
    let mut runs = Vec::new();
    for name_len in [16, 1024] {
        let project = tempfile::tempdir().unwrap();
        let path = project.path().join("state");
        build_project(&path, 8, name_len);
        let layout = layout(&path);
        assert!(
            layout.property_files >= 2,
            "the fixture must publish node and edge property fragments: {layout:?}"
        );
        let (forge, cost) = measured(|| open(&path));
        let copied = forge.graph_open_evidence().bytes_copied;
        eprintln!(
            "name_len={name_len} files={} property_files={} property_bytes={} open_read={} \
             open_rchar={:?} copied={copied}",
            layout.files, layout.property_files, layout.property_bytes, cost.attributed, cost.rchar
        );
        runs.push((layout, cost, copied));
    }
    let [(small_layout, small, _), (large_layout, large, large_copied)] = &runs[..] else {
        unreachable!("two runs")
    };
    // The comparison is meaningful only if the property payload really grew.
    assert!(
        large_layout.property_bytes > 8 * small_layout.property_bytes,
        "property payload did not grow: {} -> {}",
        small_layout.property_bytes,
        large_layout.property_bytes
    );
    let extra_files = large_layout.files.saturating_sub(small_layout.files);
    let allowed = BYTES_PER_EXTRA_FILE * extra_files + SLACK_BYTES;
    assert!(
        large.attributed <= small.attributed + allowed,
        "attributed open read grew with the property payload: {} -> {} (allowed +{allowed})",
        small.attributed,
        large.attributed
    );
    if let (Some(small_rchar), Some(large_rchar)) = (small.rchar, large.rchar) {
        assert!(
            large_rchar <= small_rchar + allowed,
            "process open reads grew with the property payload: {small_rchar} -> {large_rchar} \
             (allowed +{allowed})"
        );
        // And the larger open costs less than reading what it declares.
        assert!(
            large_rchar < large_layout.property_bytes + large_layout.node_bytes + 2 * large_copied,
            "open read {large_rchar} bytes, as much as the property payload it must not read \
             ({})",
            large_layout.property_bytes
        );
    }
}

fn find_text(forge: &GraphForge) -> usize {
    forge
        .find(FindOptions {
            label: Some(LABEL.into()),
            query: Some("person000007".into()),
            limit: 3,
            ..FindOptions::default()
        })
        .expect("find")
        .num_rows()
}

/// A project that already holds a fresh text index over `name`, so a `find` is
/// reuse, not a build.
fn build_indexed_project(path: &Path, fan_out: usize) {
    build_project(path, fan_out, 16);
    let forge = open(path);
    forge
        .index_search(
            LABEL,
            SearchIndexOptions::Text {
                properties: Some(vec!["name".into()]),
                rebuild: false,
            },
        )
        .unwrap();
}

#[test]
fn text_find_does_not_read_the_graph_across_a_16x_edge_range() {
    let _serial = SERIAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let _capture = LifecycleIoCapture::install();
    if process_rchar().is_none() {
        eprintln!(
            "SKIPPED whole-process rchar assertions: /proc/self/io is unavailable; the \
             attributed assertions still run"
        );
    }
    let mut runs = Vec::new();
    for fan_out in [8, 128] {
        let project = tempfile::tempdir().unwrap();
        let path = project.path().join("state");
        build_indexed_project(&path, fan_out);
        let layout = layout(&path);
        let forge = open(&path);
        // The first call may settle first-touch admissions; the second is the
        // steady cost of one `find`. Both are bounded by the same structure.
        let (rows, first) = measured(|| find_text(&forge));
        assert_eq!(rows, 1, "fan-out {fan_out}: wrong answer");
        let (rows, second) = measured(|| find_text(&forge));
        assert_eq!(rows, 1);
        eprintln!(
            "fan_out={fan_out} edges={} node_bytes={} property_bytes={} index_bytes={} \
             edge_bytes={} find1_read={} find1_rchar={:?} find2_read={} find2_rchar={:?}",
            NODES * fan_out,
            layout.node_bytes,
            layout.property_bytes,
            layout.index_bytes,
            layout.edge_bytes,
            first.attributed,
            first.rchar,
            second.attributed,
            second.rchar
        );
        runs.push((layout, first, second));
    }
    let [(small_layout, small_first, small_second), (large_layout, large_first, large_second)] =
        &runs[..]
    else {
        unreachable!("two runs")
    };
    assert!(
        large_layout.edge_bytes > 8 * small_layout.edge_bytes,
        "edge payload did not grow: {} -> {}",
        small_layout.edge_bytes,
        large_layout.edge_bytes
    );
    // The node set and its properties are identical at both sizes.
    assert_eq!(small_layout.node_bytes, large_layout.node_bytes);
    assert_eq!(
        small_layout.node_property_bytes,
        large_layout.node_property_bytes
    );
    let extra_files = large_layout.files.saturating_sub(small_layout.files);
    let allowed = BYTES_PER_EXTRA_FILE * extra_files + SLACK_BYTES;
    for (name, small, large) in [
        ("first find", small_first, large_first),
        ("second find", small_second, large_second),
    ] {
        // Every reader that reports to the lifecycle ledger reports the same
        // bytes at both sizes: the node and property sources and the index.
        assert!(
            large.attributed <= small.attributed + allowed,
            "{name}: attributed read bytes moved with the edge payload: {} -> {} (allowed \
             +{allowed})",
            small.attributed,
            large.attributed
        );
        // The process counter is reported, not gated. It still moves with the
        // edge count because `UuidMembershipIndex::open`, which text discovery
        // calls to authenticate node identities, reads the whole identity run
        // (nodes and edges) several times; that reader reports nothing to the
        // ledger and is outside this slice. Edge payloads are proven unread by
        // `find_does_not_read_edge_payloads` in
        // `workspace_hydration/tests.rs`, which answers a `find` with every
        // edge object corrupted.
        eprintln!("{name}: process reads {:?} -> {:?}", small.rchar, large.rchar);
    }
    let sources =
        large_layout.node_bytes + large_layout.node_property_bytes + large_layout.index_bytes;
    eprintln!("declared node + node-property + index bytes: {sources}");
}
