//! Per-batch property overlay encoding shared by the staged encoder and the bulk builder.

use super::*;

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn node_property_batch(
    input: &RecordBatch,
    ordinals: &mut BTreeMap<String, u64>,
    parent_generation: u64,
    ontology_mode: OntologyMode,
    semantic_context: Option<&CompositionBindingContext>,
    semantic_bindings: Option<&SemanticStorageBindings>,
    route_table: &mut crate::route_component::RouteTable,
    encoding_lanes: &mut lanes::ParquetLanes,
    output: &StableDirectory,
    cache_window: std::num::NonZeroU64,
    evidence: &mut GraphConstructionEncodingEvidence,
    cancelled: &mut impl FnMut() -> bool,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
) -> Result<(), GfError> {
    let labels = required_string(input, "label")?;
    let mut groups = BTreeMap::<String, Vec<u32>>::new();
    for row in 0..input.num_rows() {
        groups
            .entry(labels.value(row).to_owned())
            .or_default()
            .push(u32::try_from(row).map_err(storage)?);
    }
    for (label, indexes) in groups {
        let runtime_route = if ontology_mode == OntologyMode::Exploratory {
            "_untyped"
        } else {
            label.as_str()
        };
        let owner = resolve_owner(
            semantic_context,
            semantic_bindings,
            SymbolKind::Entity,
            SemanticRouteKind::Entity,
            &label,
            runtime_route,
        )?;
        let projections = property_projections(
            input,
            2,
            &indexes,
            &owner,
            SymbolKind::Entity,
            SemanticRouteKind::NodeProperty,
            semantic_context,
            semantic_bindings,
        )?;
        for (route, fields) in projections {
            let ordinal = ordinals.entry(route.clone()).or_default();
            let property = property_batch(
                input,
                "node_uuid",
                "graphforge.entity_type",
                &owner.topology_route,
                &indexes,
                &fields,
                PropertyRouteKind::Node,
                &route,
                parent_generation + 1,
                *ordinal,
            )?;
            let property = if owner.symbol.is_some() {
                with_route_metadata_batch(
                    &property,
                    &route,
                    semantic_context
                        .expect("qualified owner has context")
                        .fingerprint(),
                )?
            } else {
                property
            };
            for fragment in split_into_fragments(&property, *ordinal)? {
                let path = format!(
                    "properties/{}/{:020}-{ordinal:020}.parquet",
                    encoded_route_component(route_table, &route)?,
                    parent_generation + 1
                );
                encoding_lanes.push(
                    output,
                    &path,
                    &fragment,
                    cache_window,
                    evidence,
                    cancelled,
                    artifacts,
                )?;
                *ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| storage("encoded ordinal overflows"))?;
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn edge_property_batch(
    input: &RecordBatch,
    ordinals: &mut BTreeMap<String, u64>,
    parent_generation: u64,
    ontology_mode: OntologyMode,
    semantic_context: Option<&CompositionBindingContext>,
    semantic_bindings: Option<&SemanticStorageBindings>,
    route_table: &mut crate::route_component::RouteTable,
    encoding_lanes: &mut lanes::ParquetLanes,
    output: &StableDirectory,
    cache_window: std::num::NonZeroU64,
    evidence: &mut GraphConstructionEncodingEvidence,
    cancelled: &mut impl FnMut() -> bool,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
) -> Result<(), GfError> {
    let routes = required_string(input, "rel_type")?;
    let mut groups = BTreeMap::<String, Vec<u32>>::new();
    for row in 0..input.num_rows() {
        groups
            .entry(routes.value(row).to_owned())
            .or_default()
            .push(u32::try_from(row).map_err(storage)?);
    }
    for (route, indexes) in groups {
        let runtime_route = if ontology_mode == OntologyMode::Exploratory {
            "_exploratory"
        } else {
            route.as_str()
        };
        let owner = resolve_owner(
            semantic_context,
            semantic_bindings,
            SymbolKind::Relation,
            SemanticRouteKind::Relation,
            &route,
            runtime_route,
        )?;
        let projections = property_projections(
            input,
            4,
            &indexes,
            &owner,
            SymbolKind::Relation,
            SemanticRouteKind::EdgeProperty,
            semantic_context,
            semantic_bindings,
        )?;
        for (property_route, fields) in projections {
            let ordinal = ordinals.entry(property_route.clone()).or_default();
            let property = property_batch(
                input,
                "edge_uuid",
                "graphforge.rel_type",
                &owner.topology_route,
                &indexes,
                &fields,
                PropertyRouteKind::Edge,
                &property_route,
                parent_generation + 1,
                *ordinal,
            )?;
            let property = if owner.symbol.is_some() {
                with_route_metadata_batch(
                    &property,
                    &property_route,
                    semantic_context
                        .expect("qualified owner has context")
                        .fingerprint(),
                )?
            } else {
                property
            };
            for fragment in split_into_fragments(&property, *ordinal)? {
                let path = format!(
                    "edge_properties/{}/{:020}-{ordinal:020}.parquet",
                    encoded_route_component(route_table, &property_route)?,
                    parent_generation + 1
                );
                encoding_lanes.push(
                    output,
                    &path,
                    &fragment,
                    cache_window,
                    evidence,
                    cancelled,
                    artifacts,
                )?;
                *ordinal = ordinal
                    .checked_add(1)
                    .ok_or_else(|| storage("encoded ordinal overflows"))?;
            }
        }
    }

    Ok(())
}
