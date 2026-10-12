//! Structural evidence for the analyst export (#1974): an export that needs no
//! edge identity reads no edge table and builds no hash map. The heap bound is
//! `tests/analyst_export_allocation.rs`, which needs a counting allocator.

use std::path::Path;

use graphforge_core::OntologyMode;
use graphforge_core::uuid::Uuid;
use graphforge_ir::{Direction, IrLiteral};
use graphforge_storage::GraphWriter;
use graphforge_storage::adjacency::{
    AdjacencyBuildOptions, build_adjacency_index_into_with_options,
};
use graphforge_value::{EntityTypeId, EntityTypeSelection};
use tempfile::TempDir;

use super::*;
use crate::adjacency::PersistentAdjacencyProvider;

const TS: i64 = 1_700_000_000_000_000;

/// Nodes `0..nodes`, each with `degree` out-edges to scattered targets, plus a
/// self-loop and a parallel pair per fifth node. Edge UUIDs and weights are
/// derived from the edge's position so a fixture is reproducible.
struct Generated {
    dir: TempDir,
    nodes: usize,
    edges: usize,
}

fn node_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2000_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn edge_uuid(index: usize) -> Uuid {
    Uuid::from_u128(0x2100_0000_0000_0000_0000_0000_0000_0000 | (index as u128 + 1))
}

fn generate(nodes: usize, degree: usize, weighted: bool) -> Generated {
    let dir = TempDir::new().unwrap();
    let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
    let label = EntityTypeId::decode(0).unwrap();
    for index in 0..nodes {
        writer.create_node(node_uuid(index), label).unwrap();
    }
    let mut edges = 0;
    let mut add = |writer: &mut GraphWriter, source: usize, target: usize| {
        let uuid = edge_uuid(edges);
        writer
            .create_edge(uuid, "LINK", &node_uuid(source), &node_uuid(target))
            .unwrap();
        if weighted {
            writer
                .set_edge_properties(
                    &uuid,
                    Some("LINK"),
                    std::collections::HashMap::from([(
                        "cost".to_owned(),
                        IrLiteral::Float(f64::from(u32::try_from(edges % 97).unwrap()) - 20.0),
                    )]),
                )
                .unwrap();
        }
        edges += 1;
    };
    for source in 0..nodes {
        for step in 0..degree {
            add(&mut writer, source, (source * 7 + step * 131 + 1) % nodes);
        }
        if source % 5 == 0 {
            add(&mut writer, source, source);
            add(&mut writer, source, (source + 1) % nodes);
            add(&mut writer, source, (source + 1) % nodes);
        }
    }
    writer.flush().unwrap();
    Generated { dir, nodes, edges }
}

/// Persist a CSR whose shards hold at most `shard_edges` entries.
fn index_with_shards(dir: &Path, shard_edges: usize) {
    let mut options = AdjacencyBuildOptions::default();
    options.shard_max_edges = shard_edges;
    build_adjacency_index_into_with_options(dir, dir, TS, &options, || Ok(())).unwrap();
}

fn persistent(dir: &Path) -> PersistentAdjacencyProvider {
    let inventory =
        graphforge_storage::GraphCatalog::open(dir, None, &graphforge_ir::RuntimeCatalog::new())
            .unwrap()
            .admitted_inventory()
            .unwrap();
    PersistentAdjacencyProvider::new(dir.to_path_buf(), OntologyMode::Strict)
        .with_inventory(inventory)
}

fn selection(direction: Direction, weight: Option<&'static str>) -> AdjacencySelection<'static> {
    AdjacencySelection {
        label: EntityTypeSelection::All,
        via: "LINK",
        direction,
        weight,
    }
}

#[test]
fn identity_free_export_reads_no_edge_table_and_builds_no_hash_map() {
    let graph = generate(1_500, 6, false);
    index_with_shards(graph.dir.path(), 1_024);
    let provider = persistent(graph.dir.path());
    drop(provider.adjacency("LINK", Direction::Out).unwrap());

    let _capture = graphforge_storage::io_stats::CaptureScope::install();
    let before = graphforge_storage::io_stats::snapshot().expect("capture installed");
    let plain = export_topology(
        &provider,
        graph.dir.path(),
        OntologyMode::Strict,
        selection(Direction::Out, None),
        EdgeIdentityDemand::Omit,
    )
    .unwrap();
    let after = graphforge_storage::io_stats::snapshot().unwrap();
    assert_eq!(after.edge_full_reads, before.edge_full_reads);
    assert_eq!(after.edge_full_rows, before.edge_full_rows);
    assert_eq!(after.edge_filtered_reads, before.edge_filtered_reads);
    assert_eq!(
        plain.export_evidence(),
        AlgorithmExportEvidence {
            edge_scale_hash_entries: 0,
            edge_rows_read: 0,
        }
    );
    assert!(plain.topology().identity.is_none());
    assert!(plain.topology().weights.is_none());

    // The same capture sees an identity export read the edge table, and a
    // scan-built view report the hash map the persisted CSR does not have, so
    // neither zero above is an artifact of what is being counted.
    let retained = export_topology(
        &provider,
        graph.dir.path(),
        OntologyMode::Strict,
        selection(Direction::Out, None),
        EdgeIdentityDemand::Retain,
    )
    .unwrap();
    let read = graphforge_storage::io_stats::snapshot().unwrap();
    assert_eq!(
        read.edge_full_rows - after.edge_full_rows,
        graph.edges as u64
    );
    assert_eq!(
        retained.export_evidence().edge_rows_read,
        graph.edges as u64
    );
    assert_eq!(retained.topology().neighbors, plain.topology().neighbors);
    assert_eq!(retained.topology().offsets, plain.topology().offsets);

    let scan = crate::adjacency::ScanBuildAdjacencyProvider::new(
        graph.dir.path().to_path_buf(),
        OntologyMode::Strict,
    )
    .with_inventory(
        graphforge_storage::GraphCatalog::open(
            graph.dir.path(),
            None,
            &graphforge_ir::RuntimeCatalog::new(),
        )
        .unwrap()
        .admitted_inventory()
        .unwrap(),
    );
    let scanned = export_topology(
        &scan,
        graph.dir.path(),
        OntologyMode::Strict,
        selection(Direction::Out, None),
        EdgeIdentityDemand::Omit,
    )
    .unwrap();
    assert_eq!(
        scanned.export_evidence().edge_scale_hash_entries,
        graph.edges as u64
    );
    assert_eq!(scanned.topology().neighbors, plain.topology().neighbors);
}

/// The kernels that read only neighbor ordinals export without edge identity:
/// running them reads no edge table. `degree` reports edges, keeps identity and
/// is the positive control, so a zero for the others is not an artifact of what
/// the capture counts.
#[test]
fn ordinal_kernels_run_without_reading_the_edge_table() {
    use graphforge_core::algorithms::{ClusterAlgorithm, PathAlgorithm, RankAlgorithm};
    use graphforge_core::{ClusterOptions, PathsOptions, RankOptions};

    let graph = generate(1_500, 6, false);
    index_with_shards(graph.dir.path(), 1_024);
    let provider = persistent(graph.dir.path());
    let dir = graph.dir.path();
    let edge_rows = || {
        let stats = graphforge_storage::io_stats::snapshot().expect("capture installed");
        (
            stats.edge_full_reads,
            stats.edge_full_rows,
            stats.edge_filtered_reads,
        )
    };
    let _capture = graphforge_storage::io_stats::CaptureScope::install();
    let rank = |by| {
        let options = RankOptions {
            by,
            via: Some("LINK".to_owned()),
            directed: true,
            ..RankOptions::default()
        };
        crate::algorithm_rank::rank_algorithm(
            &provider,
            dir,
            OntologyMode::Strict,
            EntityTypeSelection::All,
            &[],
            &options,
        )
        .unwrap();
    };
    let cluster = |by, directed| {
        let options = ClusterOptions {
            by,
            via: Some("LINK".to_owned()),
            directed,
            ..ClusterOptions::default()
        };
        crate::algorithm_cluster::cluster_algorithm(
            &provider,
            dir,
            OntologyMode::Strict,
            EntityTypeSelection::All,
            &[],
            &options,
        )
        .unwrap();
    };
    let paths = |by| {
        let options = PathsOptions {
            by,
            via: Some("LINK".to_owned()),
            directed: true,
            ..PathsOptions::default()
        };
        crate::algorithm_paths::paths_algorithm(
            &provider,
            dir,
            OntologyMode::Strict,
            Some(node_uuid(0).into_bytes()),
            None,
            options,
        )
        .unwrap();
    };

    let before = edge_rows();
    rank(RankAlgorithm::PageRank);
    // Not in a lean build: label propagation and clustering coefficient.
    #[cfg(feature = "algorithms-extended")]
    rank(RankAlgorithm::ClusteringCoefficient);
    for directed in [false, true] {
        cluster(ClusterAlgorithm::Components, directed);
        #[cfg(feature = "algorithms-extended")]
        cluster(ClusterAlgorithm::LabelPropagation, directed);
    }
    paths(PathAlgorithm::BfsDepth);
    assert_eq!(edge_rows(), before, "an ordinal kernel read the edge table");

    // A verb that reports edges does read it.
    rank(RankAlgorithm::Degree);
    assert_ne!(edge_rows(), before);
}
