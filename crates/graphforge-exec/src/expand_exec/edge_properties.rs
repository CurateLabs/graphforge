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
    assemble_children(
        &source.batches,
        prop_fields,
        hop_edge_uuids.len(),
        |hop| source.uuid_to_loc.get(&hop_edge_uuids[hop]).copied(),
        None,
    )
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
    provider: &dyn crate::adjacency::AdjacencyProvider,
    rel_type_name: &str,
    dir: &Path,
    prop_fields: &[arrow::datatypes::FieldRef],
    targets: &std::collections::BTreeSet<u64>,
) -> Result<Option<EdgeIdPropSource>, GfError> {
    use arrow::array::UInt64Array;
    use arrow::compute::concat_batches;

    let exec_err = |m: String| GfError::Execution(m);
    let Some(inventory) = provider.admitted_inventory() else {
        return Ok(None);
    };
    let inventory = inventory.as_ref();
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
        // The edge-id map is held by the provider across statements of a
        // generation; a provider without one reads the index directly.
        let map = provider.edge_property_id_map(inventory, stem)?;
        let Some(batches) = crate::edge_property_index::indexed_edge_property_batches_by_id(
            inventory,
            dir,
            stem,
            targets,
            &property_names,
            map.as_deref(),
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
    assemble_children(
        &source.batches,
        prop_fields,
        hop_edge_ids.len(),
        |hop| source.id_to_loc.get(&hop_edge_ids[hop]).copied(),
        None,
    )
}

/// The already-resolved source is separate from this hop's construction
/// authority. Admit its take/coalesce output before each Arrow constructor.
pub(super) fn edge_prop_children_by_id_admitted(
    source: &EdgeIdPropSource,
    prop_fields: &[arrow::datatypes::FieldRef],
    hop_edge_ids: &[u64],
    charge: &mut super::HopReadCharge,
) -> Result<Vec<arrow::array::ArrayRef>, GfError> {
    assemble_children(
        &source.batches,
        prop_fields,
        hop_edge_ids.len(),
        |hop| source.id_to_loc.get(&hop_edge_ids[hop]).copied(),
        Some(charge),
    )
}

pub(super) fn build_edge_prop_children_admitted(
    inventory: Option<&graphforge_storage::AuthenticatedPropertyInventory>,
    rel_type_name: &str,
    dir: &Path,
    prop_fields: &[arrow::datatypes::FieldRef],
    hop_edge_uuids: &[[u8; 16]],
    charge: &mut super::HopReadCharge,
) -> Result<Vec<arrow::array::ArrayRef>, GfError> {
    let targets = hop_edge_uuids.iter().copied().collect();
    let source = load_edge_prop_source(inventory, rel_type_name, dir, prop_fields, &targets)?;
    assemble_children(
        &source.batches,
        prop_fields,
        hop_edge_uuids.len(),
        |hop| source.uuid_to_loc.get(&hop_edge_uuids[hop]).copied(),
        Some(charge),
    )
}

/// One child array per field of `prop_fields` for `hops` flattened hops, where
/// `locate(hop)` names the batch and row that hold the hop's edge, if any.
fn assemble_children(
    prop_batches_by_rel: &[RecordBatch],
    prop_fields: &[arrow::datatypes::FieldRef],
    hops: usize,
    locate: impl Fn(usize) -> Option<(usize, u32)>,
    mut charge: Option<&mut super::HopReadCharge>,
) -> Result<Vec<arrow::array::ArrayRef>, GfError> {
    use arrow::array::{ArrayRef, UInt32Array, new_null_array};
    use arrow::compute::take;

    let exec_err = |m: String| GfError::Execution(m);
    if prop_fields.is_empty() {
        return Ok(Vec::new());
    }

    let index_bytes = if let Some(charge) = charge.as_deref_mut() {
        // Trusted-length nullable UInt32 collection owns 4*N values and one
        // bitmap per batch; headers/alignment plus the exact ArrayRef vector
        // coexist through all per-field takes.
        let indexes = super::hop_read::checked_mul(
            prop_batches_by_rel.len(),
            super::hop_read::checked_add(super::hop_read::checked_mul(hops, 5)?, 2048)?,
        )?;
        charge.charge(super::hop_read::checked_add(
            indexes,
            super::hop_read::checked_mul(prop_fields.len(), std::mem::size_of::<ArrayRef>())?,
        )?)?;
        indexes
    } else {
        0
    };

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
        let mut child: ArrayRef = if let Some(charge) = charge.as_deref_mut() {
            super::hop_read::construct_array(
                charge,
                super::hop_read::unused_bytes(field.data_type(), hops)?,
                || Ok(new_null_array(field.data_type(), hops)),
            )?
        } else {
            new_null_array(field.data_type(), hops)
        };
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
            let taken = match charge.as_deref_mut() {
                Some(charge) => super::hop_read::take(charge, col, &take_by_batch[bi])?,
                None => take(col, &take_by_batch[bi], None).map_err(|e| exec_err(e.to_string()))?,
            };
            let previous_bytes = if charge.is_some() {
                super::hop_read::retained_array_bytes(child.as_ref())?
            } else {
                0
            };
            let next = if prop_batches_by_rel.len() == 1 {
                // Single relation: `take` alone reproduces the #755 behavior.
                taken
            } else {
                coalesce_child(&child, taken, hops, charge.as_deref_mut())?
            };
            let previous = std::mem::replace(&mut child, next);
            drop(previous);
            if let Some(charge) = charge.as_deref_mut() {
                charge.release(previous_bytes);
            }
        }
        children.push(child);
    }
    // A single-route take may share an index validity buffer. Its full
    // capacity already has the returned child's independent retained credit.
    drop(take_by_batch);
    if let Some(charge) = charge {
        charge.release(index_bytes);
    }
    Ok(children)
}

fn coalesce_child(
    child: &arrow::array::ArrayRef,
    taken: arrow::array::ArrayRef,
    hops: usize,
    charge: Option<&mut super::HopReadCharge>,
) -> Result<arrow::array::ArrayRef, GfError> {
    use arrow::compute::{is_not_null, kernels::zip::zip};
    let exec_err = |error: arrow::error::ArrowError| GfError::Execution(error.to_string());
    let Some(charge) = charge else {
        let mask = is_not_null(&taken).map_err(exec_err)?;
        return zip(&mask, &taken, child).map_err(exec_err);
    };
    // zip uses MutableArrayData over both complete arrays. Keep the existing
    // child-capacity, growth, NULL-payload and header construction allowances.
    let copies = super::hop_read::checked_add(
        super::hop_read::zip_copy_bytes(taken.as_ref())?,
        super::hop_read::zip_copy_bytes(child.as_ref())?,
    )?;
    let envelope = super::hop_read::checked_add(
        copies,
        super::hop_read::checked_add(hops.div_ceil(8), 2048)?,
    )?;
    let taken_bytes = super::hop_read::retained_array_bytes(taken.as_ref())?;
    let next = super::hop_read::construct_array(charge, envelope, || {
        let mask = is_not_null(&taken).map_err(exec_err)?;
        zip(&mask, &taken, child).map_err(exec_err)
    })?;
    // The replacement already owns its complete backing and container
    // allowance, including any immutable aliases.
    drop(taken);
    charge.release(taken_bytes);
    Ok(next)
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use arrow::array::{ArrayRef, AsArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use std::sync::{Arc, Mutex};

    #[test]
    fn hop_property_coalescing_releases_completed_route_workspace() {
        let values = [
            format!("route-a:{}", "a".repeat(1024 * 1024)),
            format!("route-b:{}", "b".repeat(1024 * 1024)),
            format!("route-c:{}", "c".repeat(1024 * 1024)),
        ];
        let fields = vec![Arc::new(Field::new("payload", DataType::Utf8, true))];
        // The roomy oracle runs first. Twenty-four MiB also holds the three
        // source payloads plus a live child, take and full zip envelope;
        // successive zip workspaces must not remain simultaneously charged.
        for budget in [64 * 1024 * 1024, 24 * 1024 * 1024] {
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(budget));
            let input_credit = datafusion::execution::memory_pool::MemoryConsumer::new(
                "coalescing fixture inputs",
            )
            .register(&pool);
            let source = EdgeIdPropSource {
                batches: values
                    .iter()
                    .map(|value| {
                        RecordBatch::try_new(
                            Arc::new(Schema::new(fields.clone())),
                            vec![Arc::new(StringArray::from(vec![Some(value.as_str())]))],
                        )
                        .unwrap()
                    })
                    .collect(),
                id_to_loc: [(11, (0, 0)), (21, (1, 0)), (31, (2, 0))].into(),
            };
            // These preexisting inputs have their own authority; the hop
            // charge below funds only the actual assembly boundary.
            input_credit
                .try_grow(
                    source
                        .batches
                        .iter()
                        .map(RecordBatch::get_array_memory_size)
                        .sum(),
                )
                .unwrap();
            let reservation = Arc::new(Mutex::new(
                crate::chunked_reservation::ChunkedReservation::register(
                    super::super::HOP_READ_CONSUMER,
                    &pool,
                ),
            ));
            let mut charge = super::super::HopReadCharge::new(&reservation);
            let mut children =
                edge_prop_children_by_id_admitted(&source, &fields, &[31, 11, 21, 99], &mut charge)
                    .unwrap();
            let child = children.pop().unwrap();
            drop(children);
            assert_eq!(
                child.as_string::<i32>().iter().collect::<Vec<_>>(),
                vec![
                    Some(values[2].as_str()),
                    Some(values[0].as_str()),
                    Some(values[1].as_str()),
                    None,
                ]
            );
            if budget == 24 * 1024 * 1024 {
                assert!(
                    charge.held_bytes_internal() <= child.get_array_memory_size() + 64 * 1024,
                    "completed constructors must retain the output and small index/header owners"
                );
            }
            let batch =
                RecordBatch::try_new(Arc::new(Schema::new(fields.clone())), vec![child]).unwrap();
            let output = super::super::hop_read::lease_output(batch, charge).unwrap();
            let escaped = output.batch.column(0).slice(0, 1);
            drop(output);
            drop(source);
            drop(input_credit);
            drop(reservation);
            assert!(pool.reserved() >= escaped.get_buffer_memory_size());
            assert_eq!(escaped.as_string::<i32>().value(0), values[2]);
            drop(escaped);
            assert_eq!(pool.reserved(), 0);
        }
    }

    #[test]
    fn hop_property_assembly_pre_admits_wide_selected_values_and_retains_output() {
        let wide = "a distinguishable property ".repeat(32768);
        let fields = vec![Arc::new(Field::new("payload", DataType::Utf8, true))];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(fields.clone())),
            vec![Arc::new(StringArray::from(vec![
                Some(wide.as_str()),
                Some("small"),
                None,
            ]))],
        )
        .unwrap();
        let source = EdgeIdPropSource {
            batches: vec![batch],
            id_to_loc: [(10, (0, 0)), (11, (0, 1)), (12, (0, 2))].into(),
        };
        for (budget, fits, routes) in [
            (64 * 1024, false, 1),
            (8 * 1024 * 1024, true, 1),
            (32 * 1024 * 1024, true, 2),
        ] {
            let mut source = EdgeIdPropSource {
                batches: source.batches.clone(),
                id_to_loc: source.id_to_loc.clone(),
            };
            if routes == 2 {
                // A genuine nullable second contribution forces mask/zip
                // construction; no row in it owns a selected relationship.
                source.batches.push(
                    RecordBatch::try_new(
                        Arc::new(Schema::new(fields.clone())),
                        vec![Arc::new(StringArray::from(vec![None::<&str>]))],
                    )
                    .unwrap(),
                );
            }
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(budget));
            let reservation = Arc::new(Mutex::new(
                crate::chunked_reservation::ChunkedReservation::register(
                    super::super::HOP_READ_CONSUMER,
                    &pool,
                ),
            ));
            let mut charge = super::super::HopReadCharge::new(&reservation);
            let result = edge_prop_children_by_id_admitted(
                &source,
                &fields,
                &[11, 10, 99, 12, 10],
                &mut charge,
            );
            if fits {
                let mut children = result.unwrap();
                let child: ArrayRef = children.remove(0);
                let strings = child.as_string::<i32>();
                assert_eq!(
                    strings.iter().collect::<Vec<_>>(),
                    vec![
                        Some("small"),
                        Some(wide.as_str()),
                        None,
                        None,
                        Some(wide.as_str())
                    ]
                );
                assert!(charge.held_bytes_internal() >= child.get_array_memory_size());
                let output =
                    RecordBatch::try_new(Arc::new(Schema::new(fields.clone())), vec![child])
                        .unwrap();
                let output = super::super::hop_read::lease_output(output, charge).unwrap();
                let escaped = output.batch.column(0).slice(1, 1);
                drop(output);
                drop(reservation);
                assert!(pool.reserved() >= escaped.get_buffer_memory_size());
                assert_eq!(escaped.as_string::<i32>().value(0), wide);
                drop(escaped);
            } else {
                let error = result.unwrap_err();
                assert!(
                    error.to_string().contains(super::super::HOP_READ_CONSUMER),
                    "{error}"
                );
                assert!(
                    charge.held_bytes_internal() < wide.len(),
                    "the wide take must refuse before its allocation"
                );
                drop(charge);
                drop(reservation);
            }
            assert_eq!(pool.reserved(), 0);
        }
    }
}
