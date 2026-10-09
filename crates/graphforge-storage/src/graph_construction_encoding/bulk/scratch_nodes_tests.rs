use super::super::scratch_edges::{EDGE_RECORD, ScatteredEdges};
use super::*;
use crate::graph_construction_encoding::StableDirectory;

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
