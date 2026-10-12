//! Parity of the persisted-CSR export with an independent edge-table oracle
//! (#1974).
//!
//! The oracle never touches the adjacency index: it reads the node and edge
//! Parquet tables directly into a [`ResolvedGraphProjection`] and builds the
//! graph with [`AdjacencyGraph::from_resolved_projection`]. Every algorithm the
//! dispatch registries hold must give the same output on the exported graph and
//! on the oracle graph, and the graphs themselves must agree entry for entry.
//! The fixture has sparse node ids (deleted nodes), a label subset, self-loops,
//! parallel edges and antiparallel pairs, which is what the previous export's
//! sort made right and a sequential walk has to make right on its own.

use std::collections::{BTreeSet, HashMap};

use arrow::array::{Array, ListArray, UInt32Array};
use graphforge_core::OntologyMode;
use graphforge_core::algorithms::{Algorithm, AnalyzeAlgorithm, ClusterAlgorithm, PathAlgorithm};
use graphforge_core::algorithms::{RankAlgorithm, SimilarAlgorithm};
use graphforge_core::uuid::{Uuid, to_bytes};
use graphforge_ir::{Direction, IrLiteral};
use graphforge_storage::GraphWriter;
use graphforge_value::{EntityTypeId, EntityTypeSelection};
use tempfile::TempDir;

use super::*;
use crate::adjacency::{
    AdjacencyProvider, PersistentAdjacencyProvider, ScanBuildAdjacencyProvider,
};
use crate::algorithm_analyze::register_analyze_algorithms;
use crate::algorithm_cluster::register_cluster_algorithms;
use crate::algorithm_dispatch::{
    AlgorithmCancellation, AlgorithmControl, AlgorithmLimits, AlgorithmRegistry,
};
use crate::algorithm_paths::register_path_algorithms;
use crate::algorithm_rank::register_rank_algorithms;
use crate::algorithm_similar::register_similar_algorithms;

const TS: i64 = 1_700_000_000_000_000;
const NODES: usize = 34;
const DELETED: [usize; 6] = [3, 4, 11, 17, 18, 29];

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x3000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x3100_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn label(id: u32) -> EntityTypeId {
    EntityTypeId::decode(id).unwrap()
}

/// Weighted KNOWS edges over 34 nodes, in creation order (so UUID order and id
/// order agree), after which six nodes and their incident edges are deleted.
fn fixture() -> TempDir {
    let dir = TempDir::new().unwrap();
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    for index in 0..NODES {
        let labels = match index % 3 {
            0 => vec![label(1)],
            1 => vec![label(1), label(2)],
            _ => vec![label(2)],
        };
        writer
            .create_node_with_labels(node_uuid(index), &labels)
            .unwrap();
    }
    let mut edges = Vec::new();
    for source in 0..NODES {
        edges.push((source, (source + 1) % NODES));
        edges.push((source, (source * 5 + 2) % NODES));
        if source % 7 == 0 {
            edges.push((source, source));
        }
        if source % 6 == 0 {
            edges.push((source, (source + 1) % NODES));
        }
        if source % 5 == 1 {
            edges.push(((source + 1) % NODES, source));
        }
    }
    for (index, &(source, target)) in edges.iter().enumerate() {
        writer
            .create_edge(
                edge_uuid(index),
                "KNOWS",
                &node_uuid(source),
                &node_uuid(target),
            )
            .unwrap();
        writer
            .set_edge_properties(
                &edge_uuid(index),
                Some("KNOWS"),
                HashMap::from([(
                    "cost".to_owned(),
                    IrLiteral::Float(1.0 + f64::from(u32::try_from(index * 7 % 13).unwrap()) * 0.5),
                )]),
            )
            .unwrap();
    }
    writer.flush().unwrap();
    drop(writer);

    let gone: std::collections::HashSet<[u8; 16]> =
        DELETED.iter().map(|&i| to_bytes(&node_uuid(i))).collect();
    let incident: std::collections::HashSet<[u8; 16]> =
        graphforge_storage::incident_edge_uuids(dir.path(), &gone)
            .unwrap()
            .into_iter()
            .collect();
    graphforge_storage::delete_nodes_and_edges(dir.path(), &gone, &incident).unwrap();
    graphforge_storage::adjacency::build_adjacency_index(dir.path(), TS).unwrap();
    dir
}

fn inventory(
    dir: &std::path::Path,
) -> std::sync::Arc<graphforge_storage::AuthenticatedPropertyInventory> {
    graphforge_storage::GraphCatalog::open(dir, None, &graphforge_ir::RuntimeCatalog::new())
        .unwrap()
        .admitted_inventory()
        .unwrap()
}

/// The graph an analyst verb must see, built from the tables alone.
fn oracle(
    dir: &std::path::Path,
    label: EntityTypeSelection,
    directed: bool,
    weighted: bool,
) -> AdjacencyGraph {
    let mut nodes = BTreeSet::new();
    for batch in graphforge_storage::read_nodes(dir).unwrap() {
        let uuids = fixed_binary(&batch, "node_uuid").unwrap();
        let memberships = batch
            .column_by_name("type_ids")
            .and_then(|column| column.as_any().downcast_ref::<ListArray>())
            .unwrap();
        for row in 0..batch.num_rows() {
            let members = memberships.value(row);
            let members = members.as_any().downcast_ref::<UInt32Array>().unwrap();
            let selected = match label {
                EntityTypeSelection::All => true,
                EntityTypeSelection::Known(known) => members.values().contains(&known.encode()),
                EntityTypeSelection::Missing => false,
            };
            if selected {
                nodes.insert(uuid_at(uuids, row).unwrap());
            }
        }
    }
    let mut weights = HashMap::new();
    for batch in graphforge_storage::read_edge_properties(dir, "KNOWS").unwrap() {
        let uuids = fixed_binary(&batch, "edge_uuid").unwrap();
        let costs = batch.column_by_name("cost").unwrap();
        for row in 0..batch.num_rows() {
            weights.insert(
                uuid_at(uuids, row).unwrap(),
                numeric_value(costs.as_ref(), row).unwrap(),
            );
        }
    }
    let mut edges = Vec::new();
    for batch in graphforge_storage::read_edges(dir, "*", OntologyMode::Strict).unwrap() {
        let (edge, source, target) = (
            fixed_binary(&batch, "edge_uuid").unwrap(),
            fixed_binary(&batch, "src_uuid").unwrap(),
            fixed_binary(&batch, "dst_uuid").unwrap(),
        );
        for row in 0..batch.num_rows() {
            let (edge_uuid, source_uuid, target_uuid) = (
                uuid_at(edge, row).unwrap(),
                uuid_at(source, row).unwrap(),
                uuid_at(target, row).unwrap(),
            );
            if nodes.contains(&source_uuid) && nodes.contains(&target_uuid) {
                edges.push(ResolvedGraphEdge {
                    edge_uuid,
                    source_uuid,
                    target_uuid,
                    weight: if weighted { weights[&edge_uuid] } else { 1.0 },
                });
            }
        }
    }
    AdjacencyGraph::from_resolved_projection(ResolvedGraphProjection {
        directed,
        nodes: nodes.into_iter().collect(),
        edges,
    })
    .unwrap()
}

fn export(
    provider: &dyn AdjacencyProvider,
    dir: &std::path::Path,
    label: EntityTypeSelection,
    direction: Direction,
    weighted: bool,
) -> AdjacencyGraph {
    export_adjacency(
        provider,
        dir,
        OntologyMode::Strict,
        AdjacencySelection {
            label,
            via: "KNOWS",
            direction,
            weight: weighted.then_some("cost"),
        },
    )
    .unwrap()
}

/// `(neighbor uuid, edge uuid, weight bits)` per node UUID, in adjacency order.
fn entries(graph: &AdjacencyGraph) -> Vec<([u8; 16], Vec<([u8; 16], [u8; 16], u64)>)> {
    graph
        .node_ids()
        .iter()
        .map(|&node| {
            (
                graph.node_uuid(node).unwrap(),
                graph
                    .neighbors(node)
                    .iter()
                    .map(|edge| {
                        (
                            graph.node_uuid(edge.neighbor_id).unwrap(),
                            edge.edge_uuid,
                            edge.weight.to_bits(),
                        )
                    })
                    .collect(),
            )
        })
        .collect()
}

type Outcome = Result<arrow::record_batch::RecordBatch, String>;

fn run(
    registry: &AlgorithmRegistry,
    algorithm: Algorithm,
    graph: &AdjacencyGraph,
) -> (String, Outcome) {
    let control =
        AlgorithmControl::new(AlgorithmLimits::default(), AlgorithmCancellation::default());
    (
        format!("{algorithm:?}"),
        registry
            .execute(algorithm, graph, &control)
            .map(|output| output.record_batch().clone())
            .map_err(|error| error.to_string()),
    )
}

/// Every algorithm the dispatch registries hold, run on `graph`.
fn run_registered(graph: &AdjacencyGraph) -> Vec<(String, Outcome)> {
    let directed = graph.is_directed();
    let source = graph.node_uuids().next().unwrap();
    let target = graph.node_uuids().last().unwrap();
    let mut results = Vec::new();

    let mut rank = AlgorithmRegistry::default();
    register_rank_algorithms(&mut rank).unwrap();
    for &by in RankAlgorithm::ALL {
        results.push(run(&rank, Algorithm::Rank(by), graph));
    }
    let mut cluster = AlgorithmRegistry::default();
    register_cluster_algorithms(&mut cluster).unwrap();
    for &by in ClusterAlgorithm::ALL {
        results.push(run(&cluster, Algorithm::Cluster(by), graph));
    }
    let mut analyze = AlgorithmRegistry::default();
    register_analyze_algorithms(&mut analyze, directed).unwrap();
    for &by in AnalyzeAlgorithm::ALL {
        results.push(run(&analyze, Algorithm::Analyze(by), graph));
    }
    let mut similar = AlgorithmRegistry::default();
    register_similar_algorithms(&mut similar, 3).unwrap();
    for &by in SimilarAlgorithm::ALL {
        results.push(run(&similar, Algorithm::Similar(by), graph));
    }
    let mut paths = AlgorithmRegistry::default();
    register_path_algorithms(&mut paths, source, Some(target), 3, None, None).unwrap();
    for &by in PathAlgorithm::ALL {
        results.push(run(&paths, Algorithm::Paths(by), graph));
    }
    results
}

#[test]
fn exported_graph_matches_the_edge_table_oracle_for_every_registered_algorithm() {
    let dir = fixture();
    let persistent =
        PersistentAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict)
            .with_inventory(inventory(dir.path()));
    let scanned = ScanBuildAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict)
        .with_inventory(inventory(dir.path()));
    let providers: [(&str, &dyn AdjacencyProvider); 2] =
        [("persisted CSR", &persistent), ("scan-built", &scanned)];

    let mut compared = 0;
    let mut succeeded = 0;
    for (name, provider) in providers {
        for (selection, direction, weighted) in [
            (EntityTypeSelection::All, Direction::Out, false),
            (EntityTypeSelection::All, Direction::Undirected, false),
            (EntityTypeSelection::All, Direction::Out, true),
            (EntityTypeSelection::All, Direction::Undirected, true),
            (EntityTypeSelection::Known(label(1)), Direction::Out, false),
            (
                EntityTypeSelection::Known(label(2)),
                Direction::Undirected,
                true,
            ),
        ] {
            let case = format!("{name} {selection:?} {direction:?} weighted={weighted}");
            let exported = export(provider, dir.path(), selection, direction, weighted);
            let expected = oracle(
                dir.path(),
                selection,
                !matches!(direction, Direction::Undirected),
                weighted,
            );
            assert_eq!(
                exported.node_uuids().collect::<Vec<_>>(),
                expected.node_uuids().collect::<Vec<_>>(),
                "{case}: nodes"
            );
            let (got, want) = (entries(&exported), entries(&expected));
            let first = got.iter().zip(&want).position(|(a, b)| a != b);
            if let Some(index) = first {
                let short = |row: &[([u8; 16], [u8; 16], u64)]| {
                    row.iter()
                        .map(|e| (e.0[15], e.1[15], e.2))
                        .collect::<Vec<_>>()
                };
                panic!(
                    "{case}: node {} differs\n export {:?}\n oracle {:?}",
                    got[index].0[15],
                    short(&got[index].1),
                    short(&want[index].1)
                );
            }
            assert_eq!(got.len(), want.len(), "{case}: entries");
            assert_eq!(
                exported.projection_fingerprint().unwrap(),
                expected.projection_fingerprint().unwrap(),
                "{case}: fingerprint"
            );

            let (left, right) = (run_registered(&exported), run_registered(&expected));
            assert_eq!(left.len(), right.len());
            for ((algorithm, got), (_, want)) in left.iter().zip(&right) {
                assert_eq!(got, want, "{case}: {algorithm}");
                compared += 1;
                succeeded += usize::from(got.is_ok());
            }
        }
    }
    // Most registered algorithms run on this fixture; the rest fail the same
    // way on both graphs (a missing source, vector or partition property).
    assert!(compared > 400, "compared {compared}");
    assert!(succeeded > compared / 2, "{succeeded} of {compared} ran");
}

#[test]
fn fixture_exercises_the_shapes_the_parity_claims() {
    let dir = fixture();
    let provider = PersistentAdjacencyProvider::new(dir.path().to_path_buf(), OntologyMode::Strict)
        .with_inventory(inventory(dir.path()));
    let all = export(
        &provider,
        dir.path(),
        EntityTypeSelection::All,
        Direction::Out,
        true,
    );
    // Deleted nodes leave gaps in the surrogate ids, so the export is sparse.
    assert!(matches!(all.topology().nodes, NodeSet::Sparse { .. }));
    assert_eq!(all.node_count(), NODES - DELETED.len());
    let ids = all.node_ids();
    assert!(ids.windows(2).any(|pair| pair[1] > pair[0] + 1), "gaps");

    let mut loops = 0;
    let mut parallel = 0;
    let mut antiparallel = 0;
    for &node in ids {
        let row = all.neighbors(node).to_vec();
        loops += row.iter().filter(|edge| edge.neighbor_id == node).count();
        parallel += row
            .iter()
            .filter(|edge| {
                edge.neighbor_id != node
                    && row
                        .iter()
                        .filter(|other| other.neighbor_id == edge.neighbor_id)
                        .count()
                        > 1
            })
            .count();
        antiparallel += row
            .iter()
            .filter(|edge| {
                edge.neighbor_id != node
                    && all
                        .neighbors(edge.neighbor_id)
                        .iter()
                        .any(|back| back.neighbor_id == node)
            })
            .count();
    }
    assert!(
        loops > 0 && parallel > 0 && antiparallel > 0,
        "loops {loops} parallel {parallel} antiparallel {antiparallel}"
    );

    // A label subset is a rank-bearing sparse set of its own.
    let subset = export(
        &provider,
        dir.path(),
        EntityTypeSelection::Known(label(2)),
        Direction::Out,
        false,
    );
    assert!(subset.node_count() < all.node_count());
    assert!(matches!(subset.topology().nodes, NodeSet::Sparse { .. }));
    // An undirected self-loop appears twice, once per orientation.
    let undirected = export(
        &provider,
        dir.path(),
        EntityTypeSelection::All,
        Direction::Undirected,
        false,
    );
    let looped = *ids
        .iter()
        .find(|&&n| all.neighbors(n).iter().any(|e| e.neighbor_id == n))
        .unwrap();
    assert_eq!(
        undirected
            .neighbors(looped)
            .iter()
            .filter(|e| e.neighbor_id == looped)
            .count(),
        2 * all
            .neighbors(looped)
            .iter()
            .filter(|e| e.neighbor_id == looped)
            .count()
    );
}
