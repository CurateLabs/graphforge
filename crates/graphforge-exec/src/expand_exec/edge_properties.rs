//! Edge-property reads for traversal output: resolve the authenticated
//! property rows of a set of edges once ([`EdgePropSource`]) and assemble them
//! into hop order ([`edge_prop_children`]).
//!
//! The rows are keyed by `edge_uuid`, or, when every candidate route's index is
//! addressable by edge id ([`EdgeIdPropSource`]), by the surrogate `edge_id` a
//! traversal already holds, so the edge file's UUID column is never read.

use super::hash_table_bytes;
use arrow::array::Array;
use arrow::array::RecordBatch;
use graphforge_core::GfError;
use std::path::Path;

fn read_target_edge_properties(
    inventory: &graphforge_storage::AuthenticatedPropertyInventory,
    dir: &Path,
    stem: &str,
    targets: &std::collections::BTreeSet<[u8; 16]>,
    property_names: &[String],
    owners: &mut std::collections::BTreeSet<[u8; 16]>,
) -> Result<Vec<arrow::record_batch::RecordBatch>, GfError> {
    // A route with a current persisted index is read through its small
    // authenticated shards; any other route, as before, from its fragments.
    if let Some(batches) = crate::edge_property_index::indexed_edge_property_batches(
        inventory,
        dir,
        stem,
        targets,
        property_names,
        owners,
    )? {
        return Ok(batches);
    }
    let selected = graphforge_storage::read_authenticated_property_target_data_for_inventory(
        inventory,
        graphforge_storage::PropertyRouteKind::Edge,
        stem,
        targets,
    )?;
    if selected.present.iter().any(|uuid| !owners.insert(*uuid)) {
        return Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
            message: "edge properties have multiple authenticated owners".into(),
        });
    }
    let schema = inventory.route_schema(graphforge_storage::PropertyRouteKind::Edge, stem)?;
    match schema {
        Some(schema) => {
            let indices = schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, field)| {
                    field.name() == "edge_uuid" || property_names.contains(field.name())
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            let projected = schema
                .project(&indices)
                .map_err(GfError::from_execution_error)?;
            Ok(vec![selected.edge_batch(&projected)?])
        }
        None => Ok(Vec::new()),
    }
}

fn edge_property_stems(
    inventory: Option<&graphforge_storage::AuthenticatedPropertyInventory>,
    dir: &Path,
    rel_type_name: &str,
) -> Result<Vec<String>, GfError> {
    Ok(if rel_type_name == "*" {
        match inventory {
            Some(inventory) => inventory
                .routes(graphforge_storage::PropertyRouteKind::Edge)
                .map(str::to_owned)
                .collect(),
            None => graphforge_storage::list_edge_property_stems(dir),
        }
    } else {
        let mut candidates = vec![rel_type_name.to_owned()];
        let has_shared = match inventory {
            Some(inventory) => inventory
                .route_schema(graphforge_storage::PropertyRouteKind::Edge, "_exploratory")?
                .is_some(),
            None => graphforge_storage::list_edge_property_stems(dir)
                .iter()
                .any(|stem| stem == "_exploratory"),
        };
        if rel_type_name != "_exploratory" && has_shared {
            candidates.push("_exploratory".to_owned());
        }
        candidates.sort();
        candidates
    })
}

/// Build one child array per edge-property struct field (#755), in field order.
///
/// Authenticates candidate owners and retains values for flattened hop UUIDs.
/// Named traversals select the logical route and shared construction route;
/// wildcard traversals select every route. Tombstones participate in ownership
/// refusal but never contribute values. Missing properties become NULL under
/// the lowering's nullable union schema. Returns no children without properties.
/// `hop_edge_uuids` is in flattened hop order (matching the topology children).
pub(super) fn build_edge_prop_children(
    inventory: Option<&graphforge_storage::AuthenticatedPropertyInventory>,
    rel_type_name: &str,
    dir: &Path,
    prop_fields: &[arrow::datatypes::FieldRef],
    hop_edge_uuids: &[[u8; 16]],
) -> Result<Vec<arrow::array::ArrayRef>, GfError> {
    let targets = hop_edge_uuids.iter().copied().collect();
    let source = load_edge_prop_source(inventory, rel_type_name, dir, prop_fields, &targets)?;
    edge_prop_children(&source, prop_fields, hop_edge_uuids)
}

/// The authenticated property rows of a set of edges, resolved once and
/// assembled into hop order by [`edge_prop_children`] for as many output
/// batches as the traversal needs.
pub(crate) struct EdgePropSource {
    batches: Vec<RecordBatch>,
    /// edge_uuid -> (owning batch, row within it).
    uuid_to_loc: std::collections::HashMap<[u8; 16], (usize, u32)>,
}

impl EdgePropSource {
    /// Bytes this source holds: its row batches and its location table.
    pub(super) fn charged_bytes(&self) -> usize {
        self.batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .fold(
                hash_table_bytes(
                    self.uuid_to_loc.capacity(),
                    std::mem::size_of::<([u8; 16], (usize, u32))>(),
                ),
                usize::saturating_add,
            )
    }

    /// A source for a relation with no edge properties.
    pub(crate) fn empty() -> Self {
        Self {
            batches: Vec::new(),
            uuid_to_loc: std::collections::HashMap::new(),
        }
    }
}

/// Read the selected rows of `targets` from every candidate property route.
pub(super) fn load_edge_prop_source(
    inventory: Option<&graphforge_storage::AuthenticatedPropertyInventory>,
    rel_type_name: &str,
    dir: &Path,
    prop_fields: &[arrow::datatypes::FieldRef],
    targets: &std::collections::BTreeSet<[u8; 16]>,
) -> Result<EdgePropSource, GfError> {
    use std::collections::HashMap;

    use arrow::array::FixedSizeBinaryArray;
    use arrow::compute::concat_batches;

    let exec_err = |m: String| GfError::Execution(m);
    if prop_fields.is_empty() {
        return Ok(EdgePropSource::empty());
    }

    // Resolve selected rows once per candidate route, then assemble hop order.
    let stems = edge_property_stems(inventory, dir, rel_type_name)?;
    let mut owners = std::collections::BTreeSet::new();
    let mut prop_batches_by_rel = Vec::with_capacity(stems.len());
    let property_names = prop_fields
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    for stem in &stems {
        let batches = match inventory {
            Some(inventory) => read_target_edge_properties(
                inventory,
                dir,
                stem,
                targets,
                &property_names,
                &mut owners,
            ),
            None => graphforge_storage::read_edge_properties_projected(dir, stem, &property_names)
                .map_err(|e| exec_err(e.to_string())),
        }?;
        if let Some(first) = batches.first() {
            prop_batches_by_rel.push(
                concat_batches(&first.schema(), &batches).map_err(|e| exec_err(e.to_string()))?,
            );
        }
    }

    // edge_uuid -> (owning batch, row within it). An edge belongs to exactly
    // one physical owner; duplicate ownership is corruption, not precedence.
    let mut uuid_to_loc: HashMap<[u8; 16], (usize, u32)> = HashMap::new();
    for (bi, b) in prop_batches_by_rel.iter().enumerate() {
        let key = b
            .column_by_name("edge_uuid")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
            // `downcast_ref` accepts any fixed-width binary column; require
            // width 16 so `copy_from_slice` into `[u8; 16]` can't panic on a
            // malformed on-disk file.
            .filter(|a| a.value_length() == 16)
            .ok_or_else(|| {
                exec_err("edge-property file missing a FixedSizeBinary(16) edge_uuid column".into())
            })?;
        for r in 0..key.len() {
            // A null key would `copy_from_slice` 16 zero bytes (a bogus UUID);
            // surface a corrupt file as an error instead.
            if key.is_null(r) {
                return Err(exec_err(format!(
                    "edge-property file has a null edge_uuid at row {r}"
                )));
            }
            let mut u = [0u8; 16];
            u.copy_from_slice(key.value(r));
            let location = (
                bi,
                u32::try_from(r)
                    .map_err(|_| exec_err(format!("edge-property row {r} exceeds u32")))?,
            );
            if uuid_to_loc.insert(u, location).is_some() {
                return Err(exec_err(
                    "edge properties have multiple physical owners".into(),
                ));
            }
        }
    }

    Ok(EdgePropSource {
        batches: prop_batches_by_rel,
        uuid_to_loc,
    })
}

/// Assemble one child array per edge-property struct field for the hops
/// `hop_edge_uuids` (flattened hop order) from an already resolved source.
pub(super) fn edge_prop_children(
    source: &EdgePropSource,
    prop_fields: &[arrow::datatypes::FieldRef],
    hop_edge_uuids: &[[u8; 16]],
) -> Result<Vec<arrow::array::ArrayRef>, GfError> {
    assemble_children(&source.batches, prop_fields, hop_edge_uuids.len(), |hop| {
        source.uuid_to_loc.get(&hop_edge_uuids[hop]).copied()
    })
}

/// The authenticated property rows of a set of edges, keyed by surrogate
/// `edge_id` instead of `edge_uuid`, so locating a hop's row never needs the
/// edge file's UUID column. Assembled into hop order by
/// [`edge_prop_children_by_id`].
pub(super) struct EdgeIdPropSource {
    batches: Vec<RecordBatch>,
    /// edge_id -> (owning batch, row within it).
    id_to_loc: std::collections::HashMap<u64, (usize, u32)>,
}

impl EdgeIdPropSource {
    /// Bytes this source holds: its row batches and its location table.
    pub(super) fn charged_bytes(&self) -> usize {
        self.batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .fold(
                hash_table_bytes(
                    self.id_to_loc.capacity(),
                    std::mem::size_of::<(u64, (usize, u32))>(),
                ),
                usize::saturating_add,
            )
    }
}

/// Read the selected rows of the edges with surrogate ids `targets` from every
/// candidate property route, through the routes' edge-id-addressable indexes.
///
/// `Ok(None)` when the properties cannot be located by id: no inventory, or a
/// candidate route whose index is missing, stale, or predates ids. The caller
/// then keys the read by `edge_uuid` through [`load_edge_prop_source`], which
/// also reads such a route's property fragments.
pub(super) fn load_edge_prop_source_by_id(
    inventory: Option<&graphforge_storage::AuthenticatedPropertyInventory>,
    rel_type_name: &str,
    dir: &Path,
    prop_fields: &[arrow::datatypes::FieldRef],
    targets: &std::collections::BTreeSet<u64>,
) -> Result<Option<EdgeIdPropSource>, GfError> {
    use arrow::array::UInt64Array;
    use arrow::compute::concat_batches;

    let exec_err = |m: String| GfError::Execution(m);
    let Some(inventory) = inventory else {
        return Ok(None);
    };
    if prop_fields.is_empty() {
        return Ok(Some(EdgeIdPropSource {
            batches: Vec::new(),
            id_to_loc: std::collections::HashMap::new(),
        }));
    }
    let stems = edge_property_stems(Some(inventory), dir, rel_type_name)?;
    let mut owners = std::collections::BTreeSet::new();
    let property_names = prop_fields
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let mut batches_by_route = Vec::with_capacity(stems.len());
    for stem in &stems {
        let Some(batches) = crate::edge_property_index::indexed_edge_property_batches_by_id(
            inventory,
            dir,
            stem,
            targets,
            &property_names,
            &mut owners,
        )?
        else {
            return Ok(None);
        };
        if let Some(first) = batches.first() {
            batches_by_route.push(
                concat_batches(&first.schema(), &batches).map_err(|e| exec_err(e.to_string()))?,
            );
        }
    }

    // edge_id -> (owning batch, row within it). `owners` already refused an
    // edge two routes hold; a repeat inside one batch is refused here.
    let mut id_to_loc = std::collections::HashMap::new();
    for (bi, batch) in batches_by_route.iter().enumerate() {
        let ids = batch
            .column_by_name("edge_id")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| exec_err("edge-property index rows missing a UInt64 edge_id".into()))?;
        for (row, id) in ids.values().iter().enumerate() {
            let location = (
                bi,
                u32::try_from(row)
                    .map_err(|_| exec_err(format!("edge-property row {row} exceeds u32")))?,
            );
            if id_to_loc.insert(*id, location).is_some() {
                return Err(exec_err(
                    "edge properties have multiple physical owners".into(),
                ));
            }
        }
    }
    Ok(Some(EdgeIdPropSource {
        batches: batches_by_route,
        id_to_loc,
    }))
}

/// As [`edge_prop_children`], for the hops `hop_edge_ids` (flattened hop
/// order) from a source keyed by surrogate edge id.
pub(super) fn edge_prop_children_by_id(
    source: &EdgeIdPropSource,
    prop_fields: &[arrow::datatypes::FieldRef],
    hop_edge_ids: &[u64],
) -> Result<Vec<arrow::array::ArrayRef>, GfError> {
    assemble_children(&source.batches, prop_fields, hop_edge_ids.len(), |hop| {
        source.id_to_loc.get(&hop_edge_ids[hop]).copied()
    })
}

/// One child array per field of `prop_fields` for `hops` flattened hops, where
/// `locate(hop)` names the batch and row that hold the hop's edge, if any.
fn assemble_children(
    prop_batches_by_rel: &[RecordBatch],
    prop_fields: &[arrow::datatypes::FieldRef],
    hops: usize,
    locate: impl Fn(usize) -> Option<(usize, u32)>,
) -> Result<Vec<arrow::array::ArrayRef>, GfError> {
    use arrow::array::{ArrayRef, UInt32Array, new_null_array};
    use arrow::compute::kernels::zip::zip;
    use arrow::compute::{is_not_null, take};

    let exec_err = |m: String| GfError::Execution(m);
    if prop_fields.is_empty() {
        return Ok(Vec::new());
    }

    // One take-index per flattened hop PER BATCH: Some(row) only for the hops
    // the batch owns, so each batch's `take` yields NULL everywhere else and
    // the per-field coalesce below can simply prefer non-null.
    let take_by_batch: Vec<UInt32Array> = (0..prop_batches_by_rel.len())
        .map(|bi| {
            (0..hops)
                .map(|hop| match locate(hop) {
                    Some((owner, row)) if owner == bi => Some(row),
                    _ => None,
                })
                .collect()
        })
        .collect();

    let mut children: Vec<ArrayRef> = Vec::with_capacity(prop_fields.len());
    for field in prop_fields {
        let mut child: ArrayRef = new_null_array(field.data_type(), hops);
        for (bi, b) in prop_batches_by_rel.iter().enumerate() {
            // Field advertised in the union schema but absent in this file
            // (or absent on disk entirely) -> this relation contributes NULLs.
            let Some(col) = b.column_by_name(field.name()) else {
                continue;
            };
            // A route whose last value was removed advertises Null. Its
            // contribution is null in the concrete union type as well.
            if col.data_type() == &arrow::datatypes::DataType::Null {
                continue;
            }
            let taken = take(col, &take_by_batch[bi], None).map_err(|e| exec_err(e.to_string()))?;
            child = if prop_batches_by_rel.len() == 1 {
                // Single relation: `take` alone reproduces the #755 behavior.
                taken
            } else {
                let mask = is_not_null(&taken).map_err(|e| exec_err(e.to_string()))?;
                zip(&mask, &taken, &child).map_err(|e| exec_err(e.to_string()))?
            };
        }
        children.push(child);
    }
    Ok(children)
}
