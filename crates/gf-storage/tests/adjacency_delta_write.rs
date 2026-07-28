//! Write-hook tests for adjacency delta segments (#765): `GraphWriter::flush`
//! and the delete paths emit / suppress segments correctly, and a segment
//! applied to the base CSR equals a full rebuild (acceptance criterion 1,
//! end-to-end through the real writer).

use std::path::Path;

use tempfile::TempDir;

use gf_core::uuid::{Uuid, new_v7};
use gf_core::{OntologyMode, TypeId};
use gf_storage::adjacency::{
    ALL_RELATIONS_STEM, Direction, build_adjacency_index, csr_path, read_csr,
};
use gf_storage::adjacency_delta::{apply_delta_segments, delta_path, read_delta_chain};
use gf_storage::{GraphWriter, delete_edges, read_topology_generation};

const TS: i64 = 1_700_000_000_000_000;
const PERSON: TypeId = TypeId(0);

/// Create `KNOWS` edges among fresh nodes; returns the new node UUIDs.
fn write_edges(dir: &Path, pairs: &[(usize, usize)], node_count: usize) -> Vec<Uuid> {
    let mut w = GraphWriter::open_at(dir, OntologyMode::Strict, TS).unwrap();
    let uuids: Vec<Uuid> = (0..node_count).map(|_| new_v7()).collect();
    for u in &uuids {
        w.create_node(*u, PERSON).unwrap();
    }
    for &(s, d) in pairs {
        w.create_edge(new_v7(), "KNOWS", &uuids[s], &uuids[d])
            .unwrap();
    }
    w.flush().unwrap();
    uuids
}

/// A pure-append flush over a built index writes a segment whose overlay on the
/// base CSR is byte-identical to a full rebuild — for every (stem, direction).
#[test]
fn pure_append_segment_overlay_equals_full_rebuild() {
    let dir = TempDir::new().unwrap();
    write_edges(dir.path(), &[(0, 1), (1, 2), (2, 0)], 3);
    build_adjacency_index(dir.path(), TS).unwrap();
    let base_gen = read_topology_generation(dir.path()).unwrap();

    // More edges (and a new node 3) through the writer — a segment is written.
    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    let extra: Vec<Uuid> = (0..1).map(|_| new_v7()).collect();
    w.create_node(extra[0], PERSON).unwrap();
    // Reuse existing nodes by reopening is awkward (UUIDs unknown here), so add
    // edges from/to the new node plus a self-loop to exercise growth.
    w.create_edge(new_v7(), "KNOWS", &extra[0], &extra[0])
        .unwrap();
    w.flush().unwrap();
    let cur_gen = read_topology_generation(dir.path()).unwrap();
    assert!(cur_gen > base_gen, "edge flush bumped the generation");
    assert!(
        delta_path(dir.path(), cur_gen).exists(),
        "a segment was written at the new generation"
    );

    // Overlay base + chain, captured BEFORE the rebuild overwrites the CSRs.
    let chain = read_delta_chain(dir.path(), base_gen, cur_gen).expect("intact chain");
    assert!(!chain.is_empty());
    let mut overlaid = Vec::new();
    for stem in [ALL_RELATIONS_STEM, "KNOWS"] {
        for direction in [Direction::Out, Direction::In] {
            let base = read_csr(&csr_path(dir.path(), stem, direction)).unwrap();
            overlaid.push((
                stem,
                direction,
                apply_delta_segments(&base, stem, direction, &chain),
            ));
        }
    }

    // Full rebuild at the current generation = the ground truth.
    build_adjacency_index(dir.path(), TS).unwrap();
    for (stem, direction, got) in overlaid {
        let expected = read_csr(&csr_path(dir.path(), stem, direction)).unwrap();
        assert_eq!(
            got, expected,
            "overlay must equal rebuild for {stem} {direction:?}"
        );
    }
}

/// A node-only flush still writes an (empty) segment so the chain stays
/// contiguous — without it every `CREATE (n)` would force a full rebuild.
#[test]
fn node_only_flush_writes_empty_segment() {
    let dir = TempDir::new().unwrap();
    write_edges(dir.path(), &[(0, 1)], 2);
    build_adjacency_index(dir.path(), TS).unwrap();
    let base_gen = read_topology_generation(dir.path()).unwrap();

    let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    w.create_node(new_v7(), PERSON).unwrap();
    w.flush().unwrap();
    let cur_gen = read_topology_generation(dir.path()).unwrap();

    assert!(cur_gen > base_gen);
    let chain = read_delta_chain(dir.path(), base_gen, cur_gen).expect("contiguous chain");
    assert_eq!(chain.len(), 1);
    assert!(chain[0].edges.is_empty(), "node-only segment has no edges");
}

/// Without a built index, a flush writes no segment — `deltas/` must never grow
/// for a project that has no adjacency capability.
#[test]
fn no_segment_without_index() {
    let dir = TempDir::new().unwrap();
    write_edges(dir.path(), &[(0, 1)], 2);
    let generation = read_topology_generation(dir.path()).unwrap();
    assert!(!delta_path(dir.path(), generation).exists());
    assert!(
        !gf_storage::adjacency::adjacency_dir(dir.path())
            .join("deltas")
            .exists()
    );
}

/// A DELETE bumps the generation but writes no segment (and clears any file at
/// that generation), so the chain breaks there and the provider rebuilds.
#[test]
fn delete_breaks_the_chain() {
    use std::collections::HashSet;

    let dir = TempDir::new().unwrap();
    let uuids = write_edges(dir.path(), &[(0, 1), (1, 2)], 3);
    build_adjacency_index(dir.path(), TS).unwrap();
    let base_gen = read_topology_generation(dir.path()).unwrap();

    // Delete one node's incident edges via the edge UUID set is awkward here;
    // delete by a fresh edge is simplest: create an edge, build, then delete it.
    // Instead, delete an existing node to bump topology with no segment.
    let mut victims: HashSet<[u8; 16]> = HashSet::new();
    victims.insert(*uuids[2].as_bytes());
    // Deleting incident edges first keeps the invariant; here we just delete the
    // edge file rows for the (1,2) edge by deleting node 2's edges.
    let edge_uuids: HashSet<[u8; 16]> = gf_storage::incident_edge_uuids(dir.path(), &victims)
        .unwrap()
        .into_iter()
        .collect();
    let removed = delete_edges(dir.path(), &edge_uuids).unwrap();
    assert!(removed > 0, "an incident edge was deleted");

    let cur_gen = read_topology_generation(dir.path()).unwrap();
    assert!(cur_gen > base_gen, "delete bumped the generation");
    assert!(
        !delta_path(dir.path(), cur_gen).exists(),
        "delete writes no segment"
    );
    assert!(
        read_delta_chain(dir.path(), base_gen, cur_gen).is_none(),
        "the chain is broken at the delete's generation ⇒ stale ⇒ rebuild"
    );
}
