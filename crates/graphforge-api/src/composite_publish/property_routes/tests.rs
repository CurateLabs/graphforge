use crate::GraphForge;
use graphforge_core::OntologyMode;
use graphforge_ir::IrLiteral;
use graphforge_storage::resolve_existing_edge_property_owners;
use std::collections::{BTreeMap, HashMap};
use tempfile::TempDir;
use uuid::Uuid;

#[test]
fn edge_owner_probe_bounds_selected_routes_and_refuses_ambiguity() {
    let graph = GraphForge::new(None).unwrap();
    let mut reference_work = None;
    for unrelated_rows in [33, 4097] {
        let root = TempDir::new().unwrap();
        let mut writer =
            graphforge_storage::GraphWriter::open_at(root.path(), OntologyMode::Exploratory, 1)
                .unwrap();
        for row in 0..129_u128 {
            let route = if row % 2 == 0 { "_exploratory" } else { "REL0" };
            writer
                .set_edge_properties(
                    &Uuid::from_u128(row + 1),
                    Some(route),
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
        for targets in [1, 16, 129_u128] {
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
            assert_eq!(work.fragments_considered, 2);
            assert_eq!(work.row_groups_considered, 2);
            assert!(work.physical_bytes <= 64 * 1024, "{work:?}");
            assert!(work.authenticated_snapshot_bytes <= 16 * 1024, "{work:?}");
            assert!(work.decoder_peak_bytes <= 32 * 1024, "{work:?}");
            eprintln!("owner-probe unrelated={unrelated_rows} targets={targets}: {work:?}");
            if targets == 129 {
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
        for sequence in 2..=4 {
            let mut writer = graphforge_storage::GraphWriter::open_at(
                root.path(),
                OntologyMode::Exploratory,
                sequence,
            )
            .unwrap();
            for row in 0..129_u128 {
                let route = if row % 2 == 0 { "_exploratory" } else { "REL0" };
                writer
                    .set_edge_properties(
                        &Uuid::from_u128(row + 1),
                        Some(route),
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
        let mut owners = (1..=129)
            .map(|id| (Uuid::from_u128(id), "REL0".to_owned()))
            .collect();
        let work = resolve_existing_edge_property_owners(&inventory, &mut owners).unwrap();
        assert_eq!(work.fragments_considered, 8);
        assert_eq!(work.row_groups_considered, 8);
        assert_eq!(work.resolved_targets, 129);
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
