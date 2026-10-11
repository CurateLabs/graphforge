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

/// What one frame adds to its payload: a 16-byte header, the IPC schema message
/// (field names, and the overlay metadata of a projection) and 8-byte buffer
/// padding. Measured at 1.14 to 1.40 KiB for these schemas, in sizes of 17 to
/// 254 frames; a frame is never lighter than `FRAME_MINIMUM`.
const FRAME_OVERHEAD: u64 = 1536;
const FRAME_MINIMUM: u64 = 1024;

/// `bytes` is `payload` plus the framing of `frames` frames.
fn framed(bytes: u64, payload: u64, frames: u64, name: &str) {
    assert!(
        bytes >= payload + frames * FRAME_MINIMUM && bytes <= payload + frames * FRAME_OVERHEAD,
        "{name}: {bytes} bytes in {frames} frames against a payload of {payload}"
    );
}

/// Two framings of the same rows differ by at most the larger framing.
fn reframed(bytes: u64, other: u64, frames: u64, name: &str) {
    assert!(
        bytes.abs_diff(other) <= frames * FRAME_OVERHEAD,
        "{name}: {bytes} against {other} within {frames} frames of framing"
    );
}

/// Arrow bytes the fixture's property rows need, without framing: per node a
/// 16-byte identity, the `Person` label, a `blob` offset, a `rank`; per edge
/// (endpoints are not kept) the identity, the `KNOWS` type, a `note` offset, a
/// `weight`; one validity bit per row for the nullable string; plus the blobs.
fn row_bytes(spec: &Spec) -> u64 {
    let node = 16 + (4 + 6) + 4 + 8;
    let edge = 16 + (4 + 5) + 4 + 8;
    (spec.nodes * node
        + spec.edges * edge
        + (spec.nodes + spec.edges) / 8
        + (spec.node_blobs + spec.edge_blobs) * spec.blob_bytes) as u64
}

/// Arrow bytes of the projections: per row the identity, a tombstone bit and
/// the integer property; the string property only in a window where some row
/// has a value. The fixture's values are on its first rows, so the first
/// window of each kind (`window_rows` rows, in identity order) carries the
/// string column: an offset and a validity bit per row, and the blobs.
fn projected_bytes(spec: &Spec, window_rows: usize) -> u64 {
    let kind = |rows: usize, blobs: usize| {
        let first = rows.min(window_rows);
        rows * (16 + 8) + rows / 8 + first * 4 + first / 8 + blobs * spec.blob_bytes
    };
    (kind(spec.nodes, spec.node_blobs) + kind(spec.edges, spec.edge_blobs)) as u64
}

/// The frames each stage writes, derived from the fixture alone (rows per kind,
/// the import's batch size and a decode task of 16 batches):
///
/// - A run is formed per decode task (`property_runs` is asserted equal to the
///   tasks), and a frame is cut every `BATCH_ROWS` rows, so a kind of `rows`
///   rows has `ceil(rows / BATCH_ROWS)` run frames: every task but the last is
///   a whole number of frames.
/// - A kind with several tasks is merged. The fixture's edges are in identity
///   order, so the runs cover disjoint identity ranges and the merge cuts a
///   frame exactly where an input frame ends: the same frames again. A kind of
///   one task is its own segment and writes none.
/// - A window is `BATCH_ROWS` rows and the segment frames are whole windows
///   but for the last, so each window is one frame: the same frames again.
/// - One owner and one route per kind: one projection frame per window frame.
///
/// This holds while no frame is cut by bytes: a frame's row charge stays under
/// the 64 KiB floor of the frame target, so the fixture's values are small.
struct Frames {
    runs: u64,
    run: u64,
    segment: u64,
    window: u64,
    projection: u64,
}

fn expected_frames(spec: &Spec) -> Frames {
    let kinds = [spec.nodes, spec.edges];
    let frames = |rows: usize| rows.div_ceil(BATCH_ROWS) as u64;
    let merged = kinds.iter().filter(|rows| **rows > TASK_ROWS);
    Frames {
        runs: kinds
            .iter()
            .map(|rows| rows.div_ceil(TASK_ROWS) as u64)
            .sum(),
        run: kinds.iter().map(|rows| frames(*rows)).sum(),
        segment: merged.map(|rows| frames(*rows)).sum(),
        window: kinds.iter().map(|rows| frames(*rows)).sum(),
        projection: kinds.iter().map(|rows| frames(*rows)).sum(),
    }
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
    let frames = |name: &str| field(report, &format!("property_{name}_frames"));
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

    // Every stage writes exactly the frames the fixture implies. Counted by the
    // same counters that count the bytes, so the bytes below are checked
    // against a count the production code did not produce.
    let expected = expected_frames(spec);
    assert_eq!(field(report, "property_runs"), expected.runs, "{report}");
    for (name, want) in [
        ("run_write", expected.run),
        ("segment_write", expected.segment),
        ("window_write", expected.window),
        ("projection_write", expected.projection),
    ] {
        assert_eq!(frames(name), want, "{name} frames");
    }
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
    framed(
        run,
        row_bytes(spec),
        frames("run_write"),
        "run bytes against the fixture's row bytes",
    );
    assert_eq!(reduction, 0, "a reduction level rewrote runs: {report}");
    // Each run that is merged is read exactly once, and nothing else is.
    assert_eq!(merge_read, merged + reduction, "{report}");
    assert!(merged <= run);
    // Merged rows are written once as segments; a group of one run is its own.
    assert_eq!(segments, run - merged + segment_write, "{report}");
    if merged > 0 {
        reframed(
            segment_write,
            merged,
            frames("run_write").max(frames("segment_write")),
            "segment bytes against the merged runs",
        );
    }
    // The catalog observation and the emission scan read each segment once.
    assert_eq!(segment_read, 2 * segments, "{report}");
    // Each window is the segments' rows copied once, then read back once.
    assert_eq!(window_read, window_write, "{report}");
    reframed(
        window_write,
        segments,
        frames("window_write").max(frames("run_write") + frames("segment_write")),
        "window bytes against the segments",
    );
    // Each projection is written once and read back once, and holds exactly
    // the columns the fixture's windows project.
    assert_eq!(projection_read, projection_write, "{report}");
    // A window is one construction batch of the import's `batch_rows`.
    let window_rows = BATCH_ROWS;
    framed(
        projection_write,
        projected_bytes(spec, window_rows),
        frames("projection_write"),
        "projection bytes against the projected columns",
    );
    // One owner and one route: each window frame is projected exactly once.
    assert_eq!(
        frames("projection_write"),
        frames("window_write"),
        "a projection frame was written twice or not at all: {report}"
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
            // Small values and edges in identity order, so frames are cut by
            // rows alone and their count is derivable (see `expected_frames`).
            blob_bytes: 128,
            scattered_edges: false,
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
