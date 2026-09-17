// Measured composition of the transient construction high-water mark.
//
// Included from `graph_construction::tests`, which owns the batch fixtures and
// can reach `open_internal_with_allocation`.
//
// The assertions here are the ones an attribution argument has to survive:
// the composition must sum to the peak exactly, nothing may fall through to
// `Unclassified`, and the per-edge composition must hold across a scale step,
// because a peak that does not scale predictably cannot be projected at all.
#[allow(
    clippy::cast_precision_loss,
    reason = "bytes per edge is a reported diagnostic ratio, not an exact quantity"
)]
mod transient_peak_composition {
    use super::{edge_batch, node_batch};
    use crate::transient_composition::TransientComponent;
    use crate::{
        ConstructionChunkKind, GraphConstructionBudgets, GraphConstructionSession,
        StorageAllocationOperation,
    };
    use std::collections::BTreeMap;
    use tempfile::TempDir;
    use uuid::Uuid;

    /// Ladder-shaped batch size. The default construction budgets carry the
    /// ladder's merge fan-in, so a run here is comparable per edge with a rung.
    const BATCH_ROWS: usize = 65_536;

    struct Measurement {
        edges: u64,
        peak: u64,
        composition: BTreeMap<TransientComponent, u64>,
    }

    /// Edge-batch counts to measure. Two by default so the scale step is
    /// checked in ordinary test runs; `GF_PEAK_COMPOSITION_BATCHES` widens it
    /// for evidence runs without editing the test.
    fn batch_counts() -> Vec<u64> {
        std::env::var("GF_PEAK_COMPOSITION_BATCHES").map_or_else(
            |_| vec![2, 8],
            |value| {
                value
                    .split(',')
                    .map(|part| part.trim().parse().expect("batch count"))
                    .collect()
            },
        )
    }

    fn measure(edge_batches: u64) -> Measurement {
        let root = TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let paths = StorageAllocationOperation::project_paths(root.path())
            .unwrap()
            .to_vec();
        let allocation = StorageAllocationOperation::from_paths(&paths).unwrap();
        let mut session = GraphConstructionSession::open_internal_with_allocation(
            root.path(),
            root.path(),
            Uuid::new_v4(),
            0,
            graphforge_core::OntologyMode::Exploratory,
            None,
            GraphConstructionBudgets::default(),
            crate::filesystem_admission::ProjectLifecycleMode::Durable,
            Some(&allocation),
        )
        .unwrap();
        // One node per endpoint the edge fixture references, plus the tail.
        let node_batches = 2;
        for chunk in 0..node_batches {
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("nodes-{chunk}"),
                    &node_batch(1 + chunk * BATCH_ROWS as u128, BATCH_ROWS),
                )
                .unwrap();
        }
        for chunk in 0..edge_batches {
            session
                .append(
                    ConstructionChunkKind::Edge,
                    &format!("edges-{chunk}"),
                    &edge_batch(
                        1_000_000_000 + u128::from(chunk) * BATCH_ROWS as u128,
                        BATCH_ROWS,
                    ),
                )
                .unwrap();
        }
        session.seal().unwrap();
        let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
        let encoding = session.encode_canonical(&shape, 1).unwrap();
        session
            .publish_canonical(&encoding, Uuid::new_v4(), Uuid::new_v4())
            .unwrap();
        let (_, peak) = allocation.totals().unwrap();
        let composition = allocation.peak_composition().unwrap();
        let (residency, transitions, peak_transition) = allocation.residency().unwrap();
        let edges = edge_batches * BATCH_ROWS as u64;
        // Machine-readable so an evidence run can be captured without a
        // bespoke harness. Bytes per edge is the figure that projects.
        println!(
            "TRANSIENT_PEAK_COMPOSITION {}",
            serde_json::json!({
                "edges": edges,
                "peak_allocated_bytes": peak,
                "peak_bytes_per_edge": peak as f64 / edges as f64,
                "owner_transitions": transitions,
                "peak_transition": peak_transition,
                "peak_component_allocated_bytes": composition,
                "component_bytes_per_edge": composition
                    .iter()
                    .map(|(component, bytes)| (component, *bytes as f64 / edges as f64))
                    .collect::<BTreeMap<_, _>>(),
                "component_residency": residency,
            })
        );
        Measurement {
            edges,
            peak,
            composition,
        }
    }

    #[test]
    fn peak_composition_accounts_for_every_byte_of_the_peak_at_two_scales() {
        let measurements = batch_counts().into_iter().map(measure).collect::<Vec<_>>();
        assert!(
            measurements.len() >= 2,
            "a peak that is measured at one scale cannot be projected"
        );
        for measurement in &measurements {
            let total = measurement.composition.values().sum::<u64>();
            assert_eq!(
                total, measurement.peak,
                "composition must sum to the peak, not explain a fraction of it"
            );
            assert_eq!(
                measurement.composition[&TransientComponent::Unclassified],
                0,
                "an unclassified byte is a hole in the attribution, not a bucket"
            );
            assert!(measurement.peak > 0);
        }
        // The S26 projection assumes the peak is linear in edge count. Check
        // that assumption here rather than inheriting it.
        let first = &measurements[0];
        let last = measurements.last().unwrap();
        let low = first.peak as f64 / first.edges as f64;
        let high = last.peak as f64 / last.edges as f64;
        assert!(
            high < low * 1.35,
            "peak per edge grew from {low:.1} to {high:.1} across the scale step"
        );
    }
}
