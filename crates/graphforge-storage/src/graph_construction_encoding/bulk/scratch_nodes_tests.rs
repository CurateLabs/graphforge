
use super::super::scratch::crc32c;
use super::super::scratch_edges::{EDGE_RECORD, ScatteredEdges};
use super::*;
use crate::graph_construction_encoding::StableDirectory;
use std::fs::OpenOptions;
use std::io::Write;

/// One probe block: the scratch block header plus one 16-byte record.
const PROBE_BLOCK: usize = 8 + PROBE_RECORD;

/// The sorted records of the fixture's first leaf.
fn first_leaf_records() -> Vec<NodeRecord> {
    let mut records = vec![
        NodeRecord {
            uuid: monoton(0x10, 1),
            label: 0,
        },
        NodeRecord {
            uuid: monoton(0x10, 2),
            label: 0,
        },
    ];
    records.sort_unstable_by_key(|record| record.uuid);
    records
}

/// An identity that grows with `last`, so fixtures can order and split.
fn monoton(first: u8, last: u8) -> [u8; 16] {
    let mut value = [0_u8; 16];
    value[0] = first;
    value[15] = last;
    value
}

fn resolve_plan() -> ScratchPlan {
    ScratchPlan::sized(1, 1, 1, 1 << 20, 4096)
}

/// Two node leaves — `a`, `b` in the first, `c`, `d` in the second, in
/// arrival order — beside the empty ref and probe leaves the edge pass
/// fills and the edge side the join consumes.
fn fixture(
    scratch: &Scratch,
) -> Result<(ScatteredNodes, Partitions, Partitions, ScatteredEdges), GfError> {
    let (a, b, c, d) = (
        monoton(0x10, 1),
        monoton(0x10, 2),
        monoton(0x20, 1),
        monoton(0x20, 2),
    );
    let leaves = Partitions::create(scratch, "nodes", 2, NODE_RECORD)?;
    let mut scatter = Scatter::new(scratch, &leaves, 256 << 10);
    for (leaf, uuids) in [(0, [a, b].as_slice()), (1, [c, d].as_slice())] {
        for uuid in uuids {
            scatter.push(
                leaf,
                &NodeRecord {
                    uuid: *uuid,
                    label: 0,
                }
                .encode(),
            )?;
        }
    }
    scatter.finish()?;
    let lows = vec![Some(a), Some(c)];
    let scattered = ScatteredNodes {
        router: LeafRouter::new(&lows),
        counts: vec![2, 2],
        total: 4,
        label_names: vec!["Person".to_owned()],
        leaves,
        refinement_steps: 0,
        refinement_write_bytes: 0,
        refinement_read_bytes: 0,
    };
    let refs = Partitions::create(scratch, "refs", 2, REF_RECORD)?;
    let probes = Partitions::create(scratch, "probes", 2, PROBE_RECORD)?;
    let edges = ScatteredEdges {
        partitions: Partitions::create(scratch, "edges", 1, EDGE_RECORD)?,
        lows: vec![Some(monoton(0x30, 1))],
        refinement_write_bytes: 0,
        refinement_read_bytes: 0,
        refinement_steps: 0,
        counts: vec![0],
        rel_names: vec!["KNOWS".to_owned()],
        histogram: None,
        total: 0,
    };
    Ok((scattered, refs, probes, edges))
}

fn sink_of<'a>(
    nodes: &'a ScatteredNodes,
    refs: &'a Partitions,
    probes: &'a Partitions,
) -> RefSink<'a> {
    RefSink {
        router: &nodes.router,
        refs,
        probes,
    }
}

fn context<'a>(
    scratch: &'a Scratch,
    plan: &'a ScratchPlan,
    nodes: &'a ScatteredNodes,
    refs: &'a Partitions,
    probes: &'a Partitions,
    edges: &'a ScatteredEdges,
    cancel: &'a AtomicBool,
) -> ResolveContext<'a> {
    ResolveContext {
        scratch,
        plan,
        nodes,
        refs,
        probes,
        edges,
        cancel,
    }
}

#[test]
fn a_probe_whose_edge_uuid_is_a_node_uuid_sets_the_collision() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let (nodes, refs, probes, edges) = fixture(&scratch).unwrap();
    let sink = sink_of(&nodes, &refs, &probes);
    let mut scatter = Scatter::new(&scratch, &probes, 256 << 10);
    for edge in [monoton(0x10, 2), monoton(0x30, 1), monoton(0x30, 2)] {
        // The first probe is node b's UUID; the others are pure edges.
        sink.push_probe(&mut scatter, &edge).unwrap();
    }
    scatter.finish().unwrap();
    let cancel = AtomicBool::new(false);
    let plan = resolve_plan();
    let (resolved, _) = resolve_endpoints(&context(
        &scratch, &plan, &nodes, &refs, &probes, &edges, &cancel,
    ))
    .unwrap();
    assert!(resolved.collision, "a probe of a node UUID must collide");
    assert!(resolved.miss.is_none());
    // Every file the pass consumed is reclaimed, collisions included.
    for leaf in 0..2 {
        assert!(!nodes.leaves.path(leaf).exists());
        assert!(!refs.path(leaf).exists());
        assert!(!probes.path(leaf).exists());
    }
    scratch.remove().unwrap();
}

#[test]
fn endpoint_refs_resolve_alongside_compact_probes_without_nodes_in_them() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let (nodes, refs, probes, edges) = fixture(&scratch).unwrap();
    let sink = sink_of(&nodes, &refs, &probes);
    // The edge's own UUID probes its leaf, and no node holds it.
    let mut probe_scatter = Scatter::new(&scratch, &probes, 256 << 10);
    sink.push_probe(&mut probe_scatter, &monoton(0x30, 1))
        .unwrap();
    probe_scatter.finish().unwrap();
    // One edge with both endpoints in the first leaf.
    let (a, b, e) = (monoton(0x10, 1), monoton(0x10, 2), monoton(0x30, 1));
    let mut refs_scatter = Scatter::new(&scratch, &refs, 256 << 10);
    for (role, key) in [(ROLE_SRC, a), (ROLE_DST, b)] {
        assert!(
            sink.push(&mut refs_scatter, &RefRecord { key, edge: e, role })
                .unwrap()
        );
    }
    refs_scatter.finish().unwrap();
    let cancel = AtomicBool::new(false);
    let plan = resolve_plan();
    let (resolved, _) = resolve_endpoints(&context(
        &scratch, &plan, &nodes, &refs, &probes, &edges, &cancel,
    ))
    .unwrap();
    assert!(!resolved.collision, "edge UUIDs must not collide");
    assert!(resolved.miss.is_none());
    let mut joined = Vec::new();
    resolved
        .resolved
        .read(&scratch, 0, |payload| {
            joined.extend(
                payload
                    .chunks_exact(RESOLVED_RECORD)
                    .map(ResolvedRecord::decode),
            );
            Ok(())
        })
        .unwrap();
    joined.sort_unstable_by_key(|record| (record.edge, record.role));
    assert_eq!(joined.len(), 2);
    assert_eq!(
        (joined[0].role, joined[0].endpoint, joined[0].rank),
        (ROLE_SRC, a, 1)
    );
    assert_eq!(
        (joined[1].role, joined[1].endpoint, joined[1].rank),
        (ROLE_DST, b, 2)
    );
    // Probes and refs are spent; the runs and the resolved records await
    // their own consumers.
    assert!(!probes.path(0).exists());
    assert!(!refs.path(0).exists());
    assert!(resolved.runs.path(0).exists());
    assert!(resolved.resolved.path(0).exists());
    scratch.remove().unwrap();
}

#[test]
fn a_probe_file_is_deleted_only_after_a_verified_read() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let (nodes, refs, probes, edges) = fixture(&scratch).unwrap();
    let sink = sink_of(&nodes, &refs, &probes);
    let mut scatter = Scatter::new(&scratch, &probes, 256 << 10);
    sink.push_probe(&mut scatter, &monoton(0x30, 1)).unwrap();
    scatter.finish().unwrap();
    // The probe routes to the leaf that can hold it: the second one.
    let path = probes.path(1);
    let mut bytes = std::fs::read(path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(path, bytes).unwrap();
    let error = {
        let cancel = AtomicBool::new(false);
        let plan = resolve_plan();
        resolve_endpoints(&context(
            &scratch, &plan, &nodes, &refs, &probes, &edges, &cancel,
        ))
        .map(|_| ())
        .unwrap_err()
    };
    assert!(error.to_string().contains("CRC32C"), "{error}");
    assert!(path.exists(), "an unverified probe file is not reclaimed");
}

#[test]
fn a_probe_leaf_truncated_to_zero_is_not_believed_or_reclaimed() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let (nodes, refs, probes, edges) = fixture(&scratch).unwrap();
    let sink = sink_of(&nodes, &refs, &probes);
    // The probe is node b's UUID: the file about to be truncated is the sole
    // collision evidence on this route.
    let mut scatter = Scatter::new(&scratch, &probes, PROBE_RECORD);
    sink.push_probe(&mut scatter, &monoton(0x10, 2)).unwrap();
    scatter.finish().unwrap();
    let path = probes.path(0);
    assert_eq!(
        std::fs::metadata(path).unwrap().len(),
        PROBE_BLOCK as u64,
        "one whole block was written"
    );
    std::fs::write(path, []).unwrap();
    let error = {
        let cancel = AtomicBool::new(false);
        let plan = resolve_plan();
        resolve_endpoints(&context(
            &scratch, &plan, &nodes, &refs, &probes, &edges, &cancel,
        ))
        .map(|_| ())
        .unwrap_err()
    };
    assert!(
        error.to_string().contains("lost records"),
        "a truncated probe leaf must not pass as empty: {error}"
    );
    assert!(path.exists(), "an unverified probe file is not reclaimed");
    assert!(
        refs.path(0).exists(),
        "the mismatch stops the leaf before its refs are read"
    );
    scratch.remove().unwrap();
}

#[test]
fn a_probe_leaf_that_lost_a_whole_block_is_not_believed_or_reclaimed() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let (nodes, refs, probes, edges) = fixture(&scratch).unwrap();
    let sink = sink_of(&nodes, &refs, &probes);
    // Three single-record blocks, so removing one leaves a cleanly readable
    // file whose blocks all verify: only the count can catch the loss.
    let mut scatter = Scatter::new(&scratch, &probes, PROBE_RECORD);
    for edge in [monoton(0x30, 1), monoton(0x30, 2), monoton(0x30, 3)] {
        sink.push_probe(&mut scatter, &edge).unwrap();
    }
    scatter.finish().unwrap();
    let path = probes.path(1);
    let mut bytes = std::fs::read(path).unwrap();
    assert_eq!(bytes.len(), 3 * PROBE_BLOCK);
    bytes.drain(PROBE_BLOCK..2 * PROBE_BLOCK);
    std::fs::write(path, bytes).unwrap();
    let error = {
        let cancel = AtomicBool::new(false);
        let plan = resolve_plan();
        resolve_endpoints(&context(
            &scratch, &plan, &nodes, &refs, &probes, &edges, &cancel,
        ))
        .map(|_| ())
        .unwrap_err()
    };
    assert!(
        error.to_string().contains("lost records"),
        "a cleanly readable file is still short its scattered probes: {error}"
    );
    assert!(path.exists(), "an unverified probe file is not reclaimed");
    assert!(
        refs.path(1).exists(),
        "the mismatch stops the leaf before its refs are read"
    );
    scratch.remove().unwrap();
}

#[test]
fn probe_verification_counts_whole_blocks_against_the_scatter_count() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let (nodes, refs, probes, _edges) = fixture(&scratch).unwrap();
    let sink = sink_of(&nodes, &refs, &probes);
    // Two blocks: the first probe is node b's UUID, the second is a pure
    // edge UUID of the same leaf's range.
    let mut scatter = Scatter::new(&scratch, &probes, PROBE_RECORD);
    sink.push_probe(&mut scatter, &monoton(0x10, 2)).unwrap();
    sink.push_probe(&mut scatter, &monoton(0x10, 3)).unwrap();
    scatter.finish().unwrap();
    let cancel = AtomicBool::new(false);
    let collision = verify_probes(&scratch, &probes, 0, &first_leaf_records(), &cancel).unwrap();
    assert!(collision, "node b's UUID among the probes must collide");
    assert!(
        probes.path(0).exists(),
        "the helper verifies; only its caller reclaims"
    );
    scratch.remove().unwrap();
}

#[test]
fn probe_verification_rejects_records_beyond_the_scatter_count() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let (nodes, refs, probes, _edges) = fixture(&scratch).unwrap();
    let sink = sink_of(&nodes, &refs, &probes);
    let mut scatter = Scatter::new(&scratch, &probes, PROBE_RECORD);
    sink.push_probe(&mut scatter, &monoton(0x10, 3)).unwrap();
    scatter.finish().unwrap();
    // Append one extra valid block the scatter never counted.
    let payload = monoton(0x10, 9);
    let mut block = Vec::with_capacity(PROBE_BLOCK);
    block.extend_from_slice(&(PROBE_RECORD as u32).to_le_bytes());
    block.extend_from_slice(&crc32c(&payload).to_le_bytes());
    block.extend_from_slice(&payload);
    OpenOptions::new()
        .append(true)
        .open(probes.path(0))
        .unwrap()
        .write_all(&block)
        .unwrap();
    let cancel = AtomicBool::new(false);
    let error =
        verify_probes(&scratch, &probes, 0, &first_leaf_records(), &cancel).unwrap_err();
    assert!(
        error.to_string().contains("more records than were scattered"),
        "{error}"
    );
    assert!(probes.path(0).exists(), "an unverified probe file stays");
    scratch.remove().unwrap();
}

#[test]
fn probe_verification_checks_cancellation_inside_the_read() {
    let root = tempfile::tempdir().unwrap();
    let directory = StableDirectory::open(root.path()).unwrap();
    let scratch = Scratch::create(&directory).unwrap();
    let (nodes, refs, probes, _edges) = fixture(&scratch).unwrap();
    let sink = sink_of(&nodes, &refs, &probes);
    // Two blocks, so the token is observed where the blocks are processed,
    // not only by a gate before the read.
    let mut scatter = Scatter::new(&scratch, &probes, PROBE_RECORD);
    sink.push_probe(&mut scatter, &monoton(0x10, 2)).unwrap();
    sink.push_probe(&mut scatter, &monoton(0x10, 3)).unwrap();
    scatter.finish().unwrap();
    // Already cancelled: only a check inside the probe processing itself can
    // see it. The scheduler's own gates are not on this path.
    let cancel = AtomicBool::new(true);
    let error =
        verify_probes(&scratch, &probes, 0, &first_leaf_records(), &cancel).unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert!(
        probes.path(0).exists(),
        "a cancelled probe file is not reclaimed"
    );
    scratch.remove().unwrap();
}
