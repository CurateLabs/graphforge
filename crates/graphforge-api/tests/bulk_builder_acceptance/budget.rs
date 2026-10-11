//! What an over-budget build writes to scratch (ADR 0058, "Scratch route" and
//! "Bounded property scratch"; #1881 criterion "Over budget, they add exactly
//! one scratch pass").
//!
//! The report of a public build names the bytes its scratch files took, by
//! stage. The report's scratch totals include the property bytes, which it also
//! counts separately; the topology bytes are the totals less those.
//!
//! The assertions are conservation laws over the stages the ADR permits, not
//! ratios between two sizes, so a pass that is doubled at every size fails:
//!
//! - Topology: a 28-byte record per edge is scattered once, and at most 32 more
//!   bytes per edge of adjacency entries (both directions); each byte written is
//!   read once. The residual over `60 * edges` is block headers and CRC framing.
//! - Property rows: each row is written once as a run (anchored to the row
//!   bytes the fixture's columns need), each run is read once by the merge, each
//!   merged row is written once as a segment, each segment is read twice (the
//!   catalog observation and the emission scan), copied once into a window and
//!   projected once; windows and projections are read back once. No reduction
//!   level rewrites a run. The residual is Arrow IPC re-framing of the same rows
//!   (one schema message and 16-byte header per frame), bounded by 5%.

use serde_json::Value;

use super::child::{ChildSpec, Conditions, run, spec_for};
use super::routes::{Route, budget_for, floor_of};
use super::support::*;

fn field(report: &Value, name: &str) -> u64 {
    report[name]
        .as_u64()
        .unwrap_or_else(|| panic!("{name} in {report}"))
}

/// Re-framing the same rows adds at most this fraction (IPC schema message and
/// frame header per frame; measured 0.9% to 2.1%).
const REFRAMING: f64 = 0.05;

fn within(bytes: u64, base: u64, name: &str) {
    let (bytes, base) = (bytes as f64, base as f64);
    assert!(
        bytes >= base && bytes <= base * (1.0 + REFRAMING),
        "{name}: {bytes} against {base}"
    );
}

/// Arrow bytes the fixture's property rows need, without framing: per node a
/// 16-byte identity, the `Person` label, a `blob` offset, a `rank` and two
/// validity bits; per edge (endpoints are not kept) the identity, the `KNOWS`
/// type, a `note` offset, a `weight` and two validity bits; plus the blobs.
fn row_bytes(spec: &Spec) -> u64 {
    let node = 16 + (4 + 6) + 4 + 8;
    let edge = 16 + (4 + 5) + 4 + 8;
    (spec.nodes * node
        + spec.edges * edge
        + (spec.nodes + spec.edges) * 2 / 8
        + (spec.node_blobs + spec.edge_blobs) * spec.blob_bytes) as u64
}

fn assert_topology(report: &Value) {
    // Nothing else moves through scratch: a balanced single-relation graph needs
    // no refinement and no relation spool, and its node tables are resident.
    for name in [
        "edge_refinement_write_bytes",
        "edge_refinement_read_bytes",
        "csr_spool_write_bytes",
        "csr_spool_read_bytes",
        "node_scratch_write_bytes",
        "endpoint_scratch_write_bytes",
    ] {
        assert_eq!(field(report, name), 0, "{name} in {report}");
    }
    let edges = field(report, "edges");
    let write =
        field(report, "scratch_write_bytes") - field(report, "property_scratch_write_bytes");
    let read = field(report, "scratch_read_bytes") - field(report, "property_scratch_read_bytes");
    assert_eq!(
        write, read,
        "topology scratch is not read exactly as written"
    );
    let payload = edges * (28 + 32);
    assert!(
        write >= payload && write <= payload + payload / 100 + 4096,
        "{write} topology scratch bytes for {edges} edges: {payload} payload"
    );
}

fn assert_property_stages(report: &Value, spec: &Spec) {
    let get = |name: &str| field(report, &format!("property_{name}_bytes"));
    let (run, reduction, segment_write, merge_read) = (
        get("run_write"),
        get("reduction_write"),
        get("segment_write"),
        get("merge_read"),
    );
    let (merged, segments, segment_read) =
        (get("merged_input"), get("segment"), get("segment_read"));
    let (window_write, window_read) = (get("window_write"), get("window_read"));
    let (projection_write, projection_read) = (get("projection_write"), get("projection_read"));

    // Every byte is attributed to a stage.
    assert_eq!(
        field(report, "property_scratch_write_bytes"),
        run + reduction + segment_write + window_write + projection_write,
        "{report}"
    );
    assert_eq!(
        field(report, "property_scratch_read_bytes"),
        merge_read + segment_read + window_read + projection_read,
        "{report}"
    );
    // Each row is written once as a run, and no run is rewritten.
    within(
        run,
        row_bytes(spec),
        "run bytes against the fixture's row bytes",
    );
    assert_eq!(reduction, 0, "a reduction level rewrote runs: {report}");
    // Each run that is merged is read exactly once, and nothing else is.
    assert_eq!(merge_read, merged + reduction, "{report}");
    assert!(merged <= run);
    // Merged rows are written once as segments; a group of one run is its own.
    assert_eq!(segments, run - merged + segment_write, "{report}");
    if merged > 0 {
        within(
            segment_write,
            merged,
            "segment bytes against the merged runs",
        );
    }
    // The catalog observation and the emission scan read each segment once.
    assert_eq!(segment_read, 2 * segments, "{report}");
    // Each window is the segments' rows copied once, then read back once.
    assert_eq!(window_read, window_write, "{report}");
    within(window_write, segments, "window bytes against the segments");
    // Each projection is written once and read back once, and holds no more
    // than its window (it leaves out the owner column).
    assert_eq!(projection_read, projection_write, "{report}");
    assert!(
        projection_write > 0 && projection_write <= window_write,
        "{report}"
    );
}

#[test]
fn over_budget_scratch_is_one_topology_pass_and_conserved_property_stages() {
    let directory = tempfile::tempdir().unwrap();
    let mut budget = None;
    let mut merged = false;
    for (index, (nodes, edges)) in [(5_000, 10_000), (20_000, 80_000)].into_iter().enumerate() {
        let spec = Spec {
            nodes,
            edges,
            node_blobs: 20,
            edge_blobs: 20,
            blob_bytes: 24 << 10,
        };
        let sources = Sources::write(&directory.path().join(format!("input-{index}")), spec);
        let budget = *budget.get_or_insert_with(|| {
            budget_for(Route::Scratch, floor_of(directory.path(), &sources)).unwrap()
        });
        let project = directory.path().join(format!("project-{index}"));
        std::fs::create_dir(&project).unwrap();
        let outcome = run(
            &ChildSpec {
                commit: true,
                ..spec_for(&project, &sources)
            },
            &Conditions {
                budget: Some(budget),
                ..Conditions::default()
            },
        );
        assert!(outcome.succeeded(), "{outcome:?}");
        let report = outcome.report();
        assert_topology(report);
        assert_property_stages(report, &spec);
        merged |= field(report, "property_merged_input_bytes") > 0;
    }
    // One of the sizes forms several runs of a group, so the merge law is not
    // vacuous.
    assert!(merged, "no size merged runs");
}
