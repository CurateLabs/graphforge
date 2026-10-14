use crate::GraphForge;
use graphforge_core::OntologyMode;
use graphforge_ir::IrLiteral;
use graphforge_storage::resolve_existing_edge_property_owners;
use std::collections::{BTreeMap, HashMap};
use tempfile::TempDir;
use uuid::Uuid;

/// Edges the fixture writes, as `row + 1` for `row` in `0..EDGES`.
const EDGES: u128 = 129;
/// Sequences of the fixture's repeated rewrite; each adds one fragment per
/// owner route.
const SEQUENCES: i64 = 4;
/// The routes the probe asks about: the guessed owner and the exploratory one.
const CANDIDATE_ROUTES: [&str; 2] = ["_exploratory", "REL0"];

/// The route the fixture writes edge `row` to.
fn fixture_route(row: u128) -> &'static str {
    if row % 2 == 0 { "_exploratory" } else { "REL0" }
}

/// The footer UUID range of a fragment holding every fixture edge of `route`.
fn fixture_range(route: &str) -> (u128, u128) {
    let ids = (0..EDGES)
        .filter(|row| fixture_route(*row) == route)
        .map(|row| row + 1);
    (ids.clone().min().unwrap(), ids.max().unwrap())
}

/// The fragments an owner probe for edges `1..=targets` must open: those whose
/// footer range can hold a target. A fragment whose range holds none is skipped
/// without being opened. Each candidate route has `fragments_per_route`
/// fragments, all over the route's full fixture range. The probe also stops
/// opening older fragments of a route once newer ones answer every target a
/// range holds; here each route leaves the other route's targets unanswered, and
/// those lie inside its range, so no fragment drops out for that reason.
fn fragments_whose_range_holds_a_target(targets: u128, fragments_per_route: usize) -> u64 {
    CANDIDATE_ROUTES
        .iter()
        .map(|route| {
            let (low, high) = fixture_range(route);
            if (1..=targets).any(|target| (low..=high).contains(&target)) {
                fragments_per_route as u64
            } else {
                0
            }
        })
        .sum()
}

#[test]
fn edge_owner_probe_bounds_selected_routes_and_refuses_ambiguity() {
    let graph = GraphForge::new(None).unwrap();
    let mut reference_work = None;
    for unrelated_rows in [33, 4097] {
        let root = TempDir::new().unwrap();
        let mut writer =
            graphforge_storage::GraphWriter::open_at(root.path(), OntologyMode::Exploratory, 1)
                .unwrap();
        for row in 0..EDGES {
            writer
                .set_edge_properties(
                    &Uuid::from_u128(row + 1),
                    Some(fixture_route(row)),
                    HashMap::from([("weight".into(), IrLiteral::Int(row as i64))]),
                )
                .unwrap();
        }
        for row in 0..unrelated_rows {
            writer
                .set_edge_properties(
                    &Uuid::from_u128(1000 + row),
                    Some("Unrelated"),
                    HashMap::from([("text".into(), IrLiteral::Str("unrelated".into()))]),
                )
                .unwrap();
        }
        writer.flush().unwrap();
        let inventory =
            graphforge_storage::AuthenticatedPropertyInventory::from_materialized_inventory(
                &graph.resolved_generation,
                root.path(),
                graphforge_storage::capture_graph_files(root.path())
                    .unwrap()
                    .0,
            )
            .unwrap();
        for targets in [1, 16, EDGES] {
            let mut owners = (1..=targets)
                .map(|id| (Uuid::from_u128(id), "REL0".to_owned()))
                .collect();
            let work = resolve_existing_edge_property_owners(&inventory, &mut owners).unwrap();
            for (uuid, owner) in &owners {
                assert_eq!(
                    owner,
                    if (uuid.as_u128() - 1) % 2 == 0 {
                        "_exploratory"
                    } else {
                        "REL0"
                    }
                );
            }
            assert_eq!(work.candidate_routes, 2);
            assert_eq!(work.target_memberships, 2 * targets as usize);
            assert_eq!(work.resolved_targets, targets as usize);
            assert_eq!(work.route_name_bytes, "_exploratoryREL0".len());
            // One single-row-group fragment per route: the probe opens exactly
            // those whose footer range can hold a wanted edge. For a single
            // target below the REL0 range that is the exploratory fragment alone.
            let opened = fragments_whose_range_holds_a_target(targets, 1);
            assert_eq!(work.fragments_considered, opened, "targets={targets}");
            assert_eq!(work.row_groups_considered, opened, "targets={targets}");
            assert!(work.physical_bytes <= 64 * 1024, "{work:?}");
            assert!(work.authenticated_snapshot_bytes <= 16 * 1024, "{work:?}");
            assert!(work.decoder_peak_bytes <= 32 * 1024, "{work:?}");
            eprintln!("owner-probe unrelated={unrelated_rows} targets={targets}: {work:?}");
            if targets == EDGES {
                if let Some(reference) = reference_work {
                    assert_eq!(
                        work, reference,
                        "unrelated route growth must add no probe I/O"
                    );
                } else {
                    reference_work = Some(work);
                }
            }
        }
        drop(inventory);
        for sequence in 2..=SEQUENCES {
            let mut writer = graphforge_storage::GraphWriter::open_at(
                root.path(),
                OntologyMode::Exploratory,
                sequence,
            )
            .unwrap();
            for row in 0..EDGES {
                writer
                    .set_edge_properties(
                        &Uuid::from_u128(row + 1),
                        Some(fixture_route(row)),
                        HashMap::from([("weight".into(), IrLiteral::Int(sequence))]),
                    )
                    .unwrap();
            }
            writer.flush().unwrap();
        }
        let inventory =
            graphforge_storage::AuthenticatedPropertyInventory::from_materialized_inventory(
                &graph.resolved_generation,
                root.path(),
                graphforge_storage::capture_graph_files(root.path())
                    .unwrap()
                    .0,
            )
            .unwrap();
        let mut owners = (1..=EDGES)
            .map(|id| (Uuid::from_u128(id), "REL0".to_owned()))
            .collect();
        let work = resolve_existing_edge_property_owners(&inventory, &mut owners).unwrap();
        let opened = fragments_whose_range_holds_a_target(EDGES, SEQUENCES as usize);
        assert_eq!(opened, 8, "four fragments for each of two routes");
        assert_eq!(work.fragments_considered, opened);
        assert_eq!(work.row_groups_considered, opened);
        assert_eq!(work.resolved_targets, EDGES as usize);
        assert!(work.physical_bytes <= 256 * 1024, "{work:?}");
        assert!(work.authenticated_snapshot_bytes <= 64 * 1024, "{work:?}");
        assert!(
            work.authenticated_snapshot_peak_bytes <= 16 * 1024,
            "{work:?}"
        );
        assert!(work.decoder_peak_bytes <= 32 * 1024, "{work:?}");
        eprintln!("owner-probe unrelated={unrelated_rows} fragments=8: {work:?}");
        drop(inventory);

        let mut writer =
            graphforge_storage::GraphWriter::open_at(root.path(), OntologyMode::Exploratory, 2)
                .unwrap();
        writer
            .set_edge_properties(&Uuid::from_u128(1), Some("REL0"), HashMap::new())
            .unwrap();
        writer.flush().unwrap();
        let inventory =
            graphforge_storage::AuthenticatedPropertyInventory::from_materialized_inventory(
                &graph.resolved_generation,
                root.path(),
                graphforge_storage::capture_graph_files(root.path())
                    .unwrap()
                    .0,
            )
            .unwrap();
        let mut owners = BTreeMap::from([(Uuid::from_u128(1), "REL0".to_owned())]);
        let before = owners.clone();
        assert!(
            resolve_existing_edge_property_owners(&inventory, &mut owners)
                .unwrap_err()
                .to_string()
                .contains("composite edge property owner is ambiguous")
        );
        assert_eq!(
            owners, before,
            "refusal must not install a partial resolution"
        );
    }
}
