//! Property and administrative compatibility for optimistic composite rebasing.

use super::property_routes::CompositePropertyRoutes;
use super::{build_validation_snapshot, write_conflict};
use crate::GraphForge;
use crate::composite_transaction::{CompositeGraphMutation, CompositeTransactionRequest};
use graphforge_core::GfError;
use graphforge_ir::IrLiteral;
use graphforge_storage::ResolvedProjectGeneration;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RebaseEntity {
    Node(Uuid),
    Edge(Uuid),
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RebaseField {
    entity: RebaseEntity,
    property: String,
}

type AdministrativeContract = Vec<(String, String, u32, [u8; 32])>;

#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct RebaseBaseline {
    fields: BTreeMap<RebaseField, Option<IrLiteral>>,
    node_targets: BTreeSet<Uuid>,
    edge_targets: BTreeSet<Uuid>,
    administrative_contract: AdministrativeContract,
    pub(super) non_mergeable: bool,
}

pub(super) fn capture_rebase_baseline(
    graph: &GraphForge,
    request: &CompositeTransactionRequest,
    generation: &ResolvedProjectGeneration,
    routes: &CompositePropertyRoutes,
) -> Result<RebaseBaseline, GfError> {
    let created_nodes = request
        .graph_mutations
        .iter()
        .filter_map(|mutation| match mutation {
            CompositeGraphMutation::CreateNode { node_uuid, .. } => Some(*node_uuid),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let created_edges = request
        .graph_mutations
        .iter()
        .filter_map(|mutation| match mutation {
            CompositeGraphMutation::CreateEdge { edge_uuid, .. } => Some(*edge_uuid),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let mut baseline = RebaseBaseline {
        administrative_contract: administrative_contract(generation)?,
        ..RebaseBaseline::default()
    };
    for mutation in &request.graph_mutations {
        match mutation {
            CompositeGraphMutation::SetNodeProperty {
                node_uuid,
                property,
                ..
            }
            | CompositeGraphMutation::RemoveNodeProperty {
                node_uuid,
                property,
            } if !created_nodes.contains(node_uuid) => {
                baseline.node_targets.insert(*node_uuid);
                capture_rebase_field(
                    graph,
                    &mut baseline,
                    *node_uuid,
                    property,
                    false,
                    &routes.node(node_uuid)?,
                )?;
            }
            CompositeGraphMutation::SetEdgeProperty {
                edge_uuid,
                property,
                ..
            }
            | CompositeGraphMutation::RemoveEdgeProperty {
                edge_uuid,
                property,
            } if !created_edges.contains(edge_uuid) => {
                baseline.edge_targets.insert(*edge_uuid);
                capture_rebase_field(
                    graph,
                    &mut baseline,
                    *edge_uuid,
                    property,
                    true,
                    &routes.edge(edge_uuid)?,
                )?;
            }
            CompositeGraphMutation::DeleteNode { .. }
            | CompositeGraphMutation::DeleteEdge { .. } => baseline.non_mergeable = true,
            _ => {}
        }
    }
    Ok(baseline)
}

fn capture_rebase_field(
    graph: &GraphForge,
    baseline: &mut RebaseBaseline,
    uuid: Uuid,
    property: &str,
    is_edge: bool,
    route: &str,
) -> Result<(), GfError> {
    let kind = if is_edge {
        graphforge_storage::PropertyRouteKind::Edge
    } else {
        graphforge_storage::PropertyRouteKind::Node
    };
    let inventory = graph.property_inventory_for_session();
    let (rows, _) = graphforge_storage::read_authenticated_property_snapshots_for_inventory(
        &inventory,
        kind,
        route,
        &BTreeSet::from([uuid.into_bytes()]),
    )?;
    let properties = rows
        .get(&uuid.into_bytes())
        .map_or_else(BTreeMap::new, |row| row.values.clone());
    let entity = if is_edge {
        RebaseEntity::Edge(uuid)
    } else {
        RebaseEntity::Node(uuid)
    };
    baseline.fields.insert(
        RebaseField {
            entity,
            property: property.to_owned(),
        },
        properties.get(property).cloned(),
    );
    Ok(())
}

pub(super) fn ensure_rebase_compatible(
    graph: &GraphForge,
    request: &CompositeTransactionRequest,
    latest: &ResolvedProjectGeneration,
    baseline: &RebaseBaseline,
) -> Result<(), GfError> {
    if administrative_contract(latest)? != baseline.administrative_contract {
        return Err(write_conflict(
            "concurrent operation changed project capabilities or workspace configuration",
        ));
    }
    let (snapshot, routes) = build_validation_snapshot(graph, latest, request)?;
    if !baseline.node_targets.is_subset(&snapshot.nodes)
        || !baseline.edge_targets.is_subset(&snapshot.edges)
    {
        return Err(write_conflict(
            "concurrent operation removed a graph mutation target",
        ));
    }
    let current = capture_rebase_baseline(graph, request, latest, &routes)?;
    if current.fields != baseline.fields {
        return Err(write_conflict(
            "concurrent operation changed a requested graph property",
        ));
    }
    Ok(())
}

pub(crate) fn administrative_contract(
    generation: &ResolvedProjectGeneration,
) -> Result<AdministrativeContract, GfError> {
    let mut contract = generation
        .participant_descriptors()?
        .into_iter()
        .filter(|descriptor| descriptor.capability_id == "workspace")
        .map(|descriptor| {
            (
                descriptor.capability_id,
                descriptor.record_family_id,
                descriptor.capability_version,
                descriptor.content_sha256,
            )
        })
        .collect::<Vec<_>>();
    contract.extend(generation.capabilities().into_iter().map(|capability| {
        (
            capability.capability_id,
            String::new(),
            capability.capability_version,
            [0; 32],
        )
    }));
    contract.sort();
    Ok(contract)
}

#[cfg(test)]
mod tests;
