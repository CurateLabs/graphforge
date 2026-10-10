//! Stream logical property windows without assembling their payload in memory.

use std::collections::BTreeMap;

use arrow::array::Array;

use super::super::{PropertyRouteKind, property_batch, property_projections_for_fields};
use super::emit::Semantics;
use super::property_rows::{self, BatchAccumulator, PropertyRows, SortedGroup};
use super::{
    ConstructionChunkKind, ConstructionEncodedArtifact, GfError, GraphConstructionBudgets,
    GraphConstructionEncodingEvidence, RecordBatch, SemanticRouteKind, StableDirectory, SymbolKind,
    encoded_route_component, lanes, required_string, resolve_owner, storage,
    with_route_metadata_batch,
};
use crate::property_overlay::fragment_cap::{FragmentSplitter, row_charges, with_fragment_ordinal};

struct Owner {
    active: Vec<u64>,
    ordinal: usize,
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn emit(
    rows: &PropertyRows<'_>,
    groups: &[SortedGroup],
    kind: ConstructionChunkKind,
    budgets: GraphConstructionBudgets,
    semantics: &Semantics<'_>,
    routes: &mut crate::route_component::RouteTable,
    lanes: &mut lanes::ParquetLanes,
    output: &StableDirectory,
    cache_window: std::num::NonZeroU64,
    evidence: &mut GraphConstructionEncodingEvidence,
    cancelled: &mut impl FnMut() -> bool,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
) -> Result<(), GfError> {
    let mut ordinals = BTreeMap::<String, u64>::new();
    // Scratch rows keep the identity and the owner beside the properties; an
    // edge's endpoints were resolved when its record was scattered.
    let required = property_rows::REQUIRED_COLUMNS;
    let node = matches!(kind, ConstructionChunkKind::Node);
    let owner_column = if node { "label" } else { "rel_type" };
    for group in groups {
        // A group without properties writes no overlay.
        if group.bare_owners.is_some() {
            continue;
        }
        let mut reader = rows.group_reader(group);
        let mut batch = reader.next()?;
        let mut offset = 0;
        while batch.is_some() {
            if cancelled() {
                return Err(storage("construction encoding cancelled"));
            }
            let window = rows.path()?;
            let mut owners = BTreeMap::<String, Owner>::new();
            let mut window_rows = 0;
            while window_rows < budgets.max_batch_rows {
                let Some(current) = &batch else {
                    break;
                };
                let length =
                    (budgets.max_batch_rows - window_rows).min(current.num_rows() - offset);
                let part = PropertyRows::copy_range(current, offset, length)?;
                let names = required_string(&part, owner_column)?;
                for row in 0..part.num_rows() {
                    let owner = owners
                        .entry(names.value(row).to_owned())
                        .or_insert_with(|| Owner {
                            active: vec![0; (part.num_columns() - required).div_ceil(64)],
                            ordinal: 0,
                        });
                    for column in required..part.num_columns() {
                        if !part.column(column).is_null(row) {
                            owner.active[(column - required) / 64] |=
                                1 << ((column - required) % 64);
                        }
                    }
                }
                rows.write(&window, &part)?;
                window_rows += length;
                offset += length;
                if offset == current.num_rows() {
                    batch = reader.next()?;
                    offset = 0;
                }
            }
            crate::graph_construction::construction_failpoint("bulk.after_property_window");
            // Bare schema groups participate in catalog ordering and admission
            // but, like the resident route, do not resolve property owners.
            let mut window_reader = rows.reader(&window)?;
            let first = window_reader.next()?.expect("nonempty window");
            if first.num_columns() == required {
                while window_reader.next()?.is_some() {}
                drop(window_reader);
                rows.reclaim(&window)?;
                continue;
            }
            for (ordinal, (name, owner)) in owners.iter_mut().enumerate() {
                owner.ordinal = ordinal;
                let resolved = resolve(semantics, kind, name)?;
                let (owner_kind, route_kind) = symbol_kinds(kind);
                let projections = property_projections_for_fields(
                    first.schema().as_ref(),
                    active_fields(owner, required, first.num_columns()),
                    &resolved,
                    owner_kind,
                    route_kind,
                    semantics.context,
                    semantics.bindings,
                )?;
                for (projection, _) in projections.iter().enumerate() {
                    std::fs::File::create(projected_path(&window, ordinal, projection))
                        .map_err(storage)?;
                }
            }
            // The final whole-window field selection is known now. Route each
            // compact frame once; files open only for the append, with no
            // per-owner payload buffers or retained encoders.
            let schema = first.schema();
            let mut current = Some(first);
            while let Some(part) = current {
                if cancelled() {
                    return Err(storage("construction encoding cancelled"));
                }
                let names = required_string(&part, owner_column)?;
                let mut indexes = BTreeMap::<&str, Vec<u32>>::new();
                for row in 0..part.num_rows() {
                    indexes
                        .entry(names.value(row))
                        .or_default()
                        .push(u32::try_from(row).map_err(storage)?);
                }
                for (name, indexes) in indexes {
                    let owner = &owners[name];
                    let resolved = resolve(semantics, kind, name)?;
                    let (owner_kind, route_kind) = symbol_kinds(kind);
                    let projections = property_projections_for_fields(
                        part.schema().as_ref(),
                        active_fields(owner, required, part.num_columns()),
                        &resolved,
                        owner_kind,
                        route_kind,
                        semantics.context,
                        semantics.bindings,
                    )?;
                    for (projection, (route, fields)) in projections.iter().enumerate() {
                        let path = projected_path(&window, owner.ordinal, projection);
                        let property = property_batch(
                            &part,
                            if node { "node_uuid" } else { "edge_uuid" },
                            if node {
                                "graphforge.entity_type"
                            } else {
                                "graphforge.rel_type"
                            },
                            &resolved.topology_route,
                            &indexes,
                            fields,
                            if node {
                                PropertyRouteKind::Node
                            } else {
                                PropertyRouteKind::Edge
                            },
                            route,
                            1,
                            0,
                        )?;
                        let property = if resolved.symbol.is_some() {
                            with_route_metadata_batch(
                                &property,
                                route,
                                semantics
                                    .context
                                    .expect("qualified owner has context")
                                    .fingerprint(),
                            )?
                        } else {
                            property
                        };
                        rows.write(&path, &property)?;
                    }
                }
                current = window_reader.next()?;
            }
            drop(window_reader);
            for (name, owner) in owners {
                let resolved = resolve(semantics, kind, &name)?;
                let (owner_kind, route_kind) = symbol_kinds(kind);
                let projections = property_projections_for_fields(
                    schema.as_ref(),
                    active_fields(&owner, required, schema.fields().len()),
                    &resolved,
                    owner_kind,
                    route_kind,
                    semantics.context,
                    semantics.bindings,
                )?;
                for (projection, (route, _)) in projections.into_iter().enumerate() {
                    let path = projected_path(&window, owner.ordinal, projection);
                    let ordinal = ordinals.entry(route.clone()).or_default();
                    let mut projected = rows.reader(&path)?;
                    let mut splitter = FragmentSplitter::default();
                    let mut accumulator = BatchAccumulator::new();
                    while let Some(property) = projected.next()? {
                        if cancelled() {
                            return Err(storage("construction encoding cancelled"));
                        }
                        for piece in splitter.push(&row_charges(&property)) {
                            if piece.opens_fragment
                                && let Some(fragment) = accumulator.finish()?
                            {
                                install_fragment(
                                    &fragment,
                                    &route,
                                    ordinal,
                                    kind,
                                    routes,
                                    lanes,
                                    output,
                                    cache_window,
                                    evidence,
                                    cancelled,
                                    artifacts,
                                )?;
                            }
                            accumulator.push(PropertyRows::copy_range(
                                &property,
                                piece.rows.start,
                                piece.rows.len(),
                            )?)?;
                        }
                    }
                    if let Some(fragment) = accumulator.finish()? {
                        install_fragment(
                            &fragment,
                            &route,
                            ordinal,
                            kind,
                            routes,
                            lanes,
                            output,
                            cache_window,
                            evidence,
                            cancelled,
                            artifacts,
                        )?;
                    }
                    drop(projected);
                    rows.reclaim(&path)?;
                }
            }
            rows.reclaim(&window)?;
        }
        // This group's final stream has been read to a clean, verified end;
        // the catalog scan consumed its own earlier read. Nothing reads the
        // file again, so its bytes leave the live occupancy.
        drop(reader);
        for segment in &group.segments {
            rows.reclaim(&segment.path)?;
        }
    }
    Ok(())
}

fn active_fields(
    owner: &Owner,
    required: usize,
    columns: usize,
) -> impl Iterator<Item = usize> + '_ {
    (required..columns).filter(move |column| {
        owner.active[(*column - required) / 64] & (1 << ((*column - required) % 64)) != 0
    })
}

fn projected_path(window: &std::path::Path, owner: usize, projection: usize) -> std::path::PathBuf {
    window.with_extension(format!(
        "owner-{owner:08}-projection-{projection:08}.frames"
    ))
}

fn symbol_kinds(kind: ConstructionChunkKind) -> (SymbolKind, SemanticRouteKind) {
    match kind {
        ConstructionChunkKind::Node => (SymbolKind::Entity, SemanticRouteKind::NodeProperty),
        ConstructionChunkKind::Edge => (SymbolKind::Relation, SemanticRouteKind::EdgeProperty),
    }
}

fn resolve(
    semantics: &Semantics<'_>,
    kind: ConstructionChunkKind,
    name: &str,
) -> Result<super::super::ResolvedOwner, GfError> {
    let (symbol, topology, exploratory) = match kind {
        ConstructionChunkKind::Node => (SymbolKind::Entity, SemanticRouteKind::Entity, "_untyped"),
        ConstructionChunkKind::Edge => (
            SymbolKind::Relation,
            SemanticRouteKind::Relation,
            "_exploratory",
        ),
    };
    resolve_owner(
        semantics.context,
        semantics.bindings,
        symbol,
        topology,
        name,
        if semantics.mode == super::OntologyMode::Exploratory {
            exploratory
        } else {
            name
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn install_fragment(
    fragment: &RecordBatch,
    route: &str,
    ordinal: &mut u64,
    kind: ConstructionChunkKind,
    routes: &mut crate::route_component::RouteTable,
    lanes: &mut lanes::ParquetLanes,
    output: &StableDirectory,
    cache_window: std::num::NonZeroU64,
    evidence: &mut GraphConstructionEncodingEvidence,
    cancelled: &mut impl FnMut() -> bool,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
) -> Result<(), GfError> {
    let fragment = with_fragment_ordinal(fragment, *ordinal)?;
    let prefix = match kind {
        ConstructionChunkKind::Node => "properties",
        ConstructionChunkKind::Edge => "edge_properties",
    };
    let path = format!(
        "{prefix}/{}/{:020}-{:020}.parquet",
        encoded_route_component(routes, route)?,
        1,
        *ordinal
    );
    lanes.push(
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
    Ok(())
}
