//! Authenticated property-owner routes for composite graph mutations.

use crate::GraphForge;
use crate::composite_transaction::{CompositeGraphMutation, CompositeTransactionRequest};
use graphforge_core::GfError;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

/// Physical property owners resolved during the existing authenticated topology scan.
/// Only requested identities are retained; both publication backends use this plan.
#[derive(Default)]
pub(super) struct CompositePropertyRoutes {
    pub(super) nodes: BTreeMap<Uuid, String>,
    pub(super) edges: BTreeMap<Uuid, String>,
    pub(super) requires_canonical: bool,
    pub(super) created_node_types: HashMap<Uuid, graphforge_value::EntityTypeId>,
}

impl CompositePropertyRoutes {
    pub(super) fn node(&self, uuid: &Uuid) -> Result<String, GfError> {
        self.nodes
            .get(uuid)
            .cloned()
            .ok_or_else(|| GfError::Validation("composite node property owner is absent".into()))
    }

    pub(super) fn edge(&self, uuid: &Uuid) -> Result<String, GfError> {
        self.edges
            .get(uuid)
            .cloned()
            .ok_or_else(|| GfError::Validation("composite edge property owner is absent".into()))
    }
}

pub(super) fn composite_node_property_owner(
    graph: &GraphForge,
    primary: graphforge_value::PrimaryEntityTypeId,
) -> Result<String, GfError> {
    let Some(id) = primary.label().and_then(|id| id.tagged().ontology_id()) else {
        return Ok("_untyped".to_owned());
    };
    let bindings = graph
        .semantic_storage_bindings
        .lock()
        .expect("semantic storage binding lock poisoned");
    if let Some(bindings) = bindings.as_ref() {
        if let Some(owner) = bindings.bindings.iter().find(|binding| {
            binding.route_kind == graphforge_storage::SemanticRouteKind::Entity
                && binding.storage_id == id.0
        }) {
            return Ok(owner.route.clone());
        }
        return Err(GfError::Storage(
            "composite node property owner is absent from semantic authority".into(),
        ));
    }
    graph
        .ontology
        .as_ref()
        .and_then(|ontology| ontology.entity_type_name(id))
        .map(str::to_owned)
        .ok_or_else(|| {
            GfError::Storage(
                "composite node property owner is absent from ontology authority".into(),
            )
        })
}

pub(super) fn composite_created_owner(
    graph: &GraphForge,
    kind: graphforge_ontology::SymbolKind,
    name: &str,
) -> Result<Option<(u32, String)>, GfError> {
    let Some(context) = graph.default_composition_snapshot() else {
        return Ok(None);
    };
    let (symbol, _) = context.resolve(kind, name).map_err(|error| {
        GfError::Validation(format!(
            "composite owner resolution failed: {:?}",
            error.code
        ))
    })?;
    let graphforge_ir::SymbolBinding::Qualified(symbol) = symbol else {
        return Ok(None);
    };
    let route_kind = if kind == graphforge_ontology::SymbolKind::Entity {
        graphforge_storage::SemanticRouteKind::Entity
    } else {
        graphforge_storage::SemanticRouteKind::Relation
    };
    graph
        .semantic_storage_bindings
        .lock()
        .expect("semantic storage binding lock poisoned")
        .as_ref()
        .and_then(|bindings| {
            bindings
                .bindings
                .iter()
                .find(|binding| binding.route_kind == route_kind && binding.symbol == symbol)
        })
        .map(|binding| (binding.storage_id, binding.route.clone()))
        .ok_or_else(|| {
            GfError::Validation("composite owner lacks a generation storage binding".into())
        })
        .map(Some)
}

fn validate_composite_property_owner(
    graph: &GraphForge,
    route: &str,
    edge: bool,
    property: &str,
) -> Result<(), GfError> {
    if !route.starts_with("s-") {
        return Ok(());
    }
    let context = graph.default_composition_snapshot().ok_or_else(|| {
        GfError::Validation("composite semantic property lacks composition authority".into())
    })?;
    let bindings = graph
        .semantic_storage_bindings
        .lock()
        .expect("semantic storage binding lock poisoned");
    let kind = if edge {
        graphforge_storage::SemanticRouteKind::Relation
    } else {
        graphforge_storage::SemanticRouteKind::Entity
    };
    let owner = bindings
        .as_ref()
        .and_then(|bindings| {
            bindings
                .bindings
                .iter()
                .find(|binding| binding.route_kind == kind && binding.route == route)
        })
        .ok_or_else(|| {
            GfError::Validation("composite property owner lacks semantic authority".into())
        })?;
    context
        .resolve_owned_property(
            owner.symbol.kind,
            &owner.symbol.ambiguity_candidate(),
            property,
        )
        .map_err(|error| {
            GfError::Validation(format!(
                "composite property owner rejected: {:?}",
                error.code
            ))
        })?;
    Ok(())
}

pub(super) fn validate_composite_property_routes(
    graph: &GraphForge,
    request: &CompositeTransactionRequest,
    routes: &CompositePropertyRoutes,
) -> Result<(), GfError> {
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
            } => {
                validate_composite_property_owner(
                    graph,
                    &routes.node(node_uuid)?,
                    false,
                    property,
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
            } => {
                validate_composite_property_owner(graph, &routes.edge(edge_uuid)?, true, property)?;
            }
            CompositeGraphMutation::CreateNode {
                node_uuid,
                properties,
                ..
            } => {
                for property in properties.keys() {
                    validate_composite_property_owner(
                        graph,
                        &routes.node(node_uuid)?,
                        false,
                        property,
                    )?;
                }
            }
            CompositeGraphMutation::CreateEdge {
                edge_uuid,
                properties,
                ..
            } => {
                for property in properties.keys() {
                    validate_composite_property_owner(
                        graph,
                        &routes.edge(edge_uuid)?,
                        true,
                        property,
                    )?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
