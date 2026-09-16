//! Property, label and deletion statement phases.

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::ArrayRef;
use arrow::array::BooleanArray;
use arrow::array::ListArray;
use arrow::array::RecordBatch;
use arrow::array::StructArray;
use arrow::array::UInt32Array;
use arrow::datatypes::DataType;

use datafusion::common::DFSchema;
use datafusion::execution::context::ExecutionProps;

use datafusion::physical_expr::create_physical_expr;

use datafusion::scalar::ScalarValue;

use graphforge_core::GfError;
use graphforge_core::uuid::to_bytes;

use graphforge_ir::ExprId;
use graphforge_ir::SetMapItem;
use graphforge_ir::SetPropItem;
use graphforge_ir::VarId;

use graphforge_rel::VarMap;
use graphforge_rel::expr::scalar_to_ir_literal;
use graphforge_value::EntityTypeId;

use crate::DeleteCol;
use crate::WriteCol;
use crate::collect_delete_targets;
use crate::fixed_binary_uuid;

use super::Frontier;
use super::PhaseEnv;
use super::StatementWriteContext;
use super::bind_expr_params;
use super::decode_memberships;
use super::positional_eval_expr;

/// Resolve each DELETE target's identity column against the frontier.
fn resolve_delete_cols(schema: &DFSchema, vars: &[VarId]) -> Result<Vec<DeleteCol>, GfError> {
    vars.iter()
        .map(|var| {
            let qual = datafusion::common::TableReference::bare(format!("var_{}", var.0));
            if let Some(uuid_idx) = schema.index_of_column_by_name(Some(&qual), "node_uuid") {
                Ok(DeleteCol {
                    uuid_idx,
                    is_edge: false,
                })
            } else if let Some(uuid_idx) = schema.index_of_column_by_name(Some(&qual), "edge_uuid")
            {
                Ok(DeleteCol {
                    uuid_idx,
                    is_edge: true,
                })
            } else {
                Err(GfError::Plan(format!(
                    "DELETE target var_{} has no node_uuid/edge_uuid column in the \
                     input — it must be bound by a preceding MATCH or CREATE",
                    var.0
                )))
            }
        })
        .collect()
}

fn collect_delete_expr_targets(
    env: &PhaseEnv<'_>,
    exprs: &[ExprId],
    frontier: &Frontier,
    var_map: &VarMap,
    nodes: &mut HashSet<[u8; 16]>,
    edges: &mut HashSet<[u8; 16]>,
) -> Result<(), GfError> {
    for expr in exprs {
        let df_expr = env.lowerer.lower_value_expr_with_input(
            env.exprs,
            var_map,
            *expr,
            Arc::new(frontier.df_schema.clone()),
        )?;
        let physical = create_physical_expr(
            &env.bind_read_expression(bind_expr_params(df_expr, env.params)?)?,
            &frontier.df_schema,
            &ExecutionProps::new(),
        )
        .map_err(GfError::from_plan_error)?;
        for batch in &frontier.batches {
            let values = physical
                .evaluate(batch)
                .and_then(|value| value.into_array(batch.num_rows()))
                .map_err(GfError::from_execution_error)?;
            for row in 0..batch.num_rows() {
                let value = ScalarValue::try_from_array(&values, row)
                    .map_err(GfError::from_execution_error)?;
                collect_delete_scalar(&value, nodes, edges)?;
            }
        }
    }
    Ok(())
}

fn collect_delete_scalar(
    value: &ScalarValue,
    nodes: &mut HashSet<[u8; 16]>,
    edges: &mut HashSet<[u8; 16]>,
) -> Result<(), GfError> {
    if value.is_null() {
        return Ok(());
    }
    if let Some(decoded) =
        graphforge_rel::expr::decode_het_scalar(value).map_err(GfError::from_execution_error)?
    {
        return collect_delete_scalar(&decoded, nodes, edges);
    }
    match value {
        ScalarValue::Struct(values) => {
            if let Some(uuid) = scalar_struct_uuid(values, "node_uuid")? {
                nodes.insert(uuid);
                return Ok(());
            }
            if let Some(uuid) = scalar_struct_uuid(values, "edge_uuid")? {
                edges.insert(uuid);
                return Ok(());
            }
            let mut found_path = false;
            for field in ["nodes", "relationships"] {
                if let Some(column) = values.column_by_name(field) {
                    found_path = true;
                    let nested = ScalarValue::try_from_array(column, 0)
                        .map_err(GfError::from_execution_error)?;
                    collect_delete_scalar(&nested, nodes, edges)?;
                }
            }
            if found_path {
                Ok(())
            } else {
                Err(GfError::Execution(
                    "DELETE target must evaluate to a node, relationship, or path".into(),
                ))
            }
        }
        ScalarValue::List(values) => collect_delete_list(&values.value(0), nodes, edges),
        ScalarValue::LargeList(values) => collect_delete_list(&values.value(0), nodes, edges),
        _ => Err(GfError::Execution(
            "DELETE target must evaluate to a node, relationship, or path".into(),
        )),
    }
}

fn collect_delete_list(
    items: &ArrayRef,
    nodes: &mut HashSet<[u8; 16]>,
    edges: &mut HashSet<[u8; 16]>,
) -> Result<(), GfError> {
    for row in 0..items.len() {
        let item =
            ScalarValue::try_from_array(items, row).map_err(GfError::from_execution_error)?;
        collect_delete_scalar(&item, nodes, edges)?;
    }
    Ok(())
}

fn scalar_struct_uuid(values: &StructArray, field: &str) -> Result<Option<[u8; 16]>, GfError> {
    let Some(column) = values.column_by_name(field) else {
        return Ok(None);
    };
    if column.is_null(0) {
        return Ok(None);
    }
    let bytes = column
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .ok_or_else(|| GfError::Execution(format!("DELETE {field} is not a UUID")))?
        .value(0);
    if bytes.len() != 16 {
        return Err(GfError::Execution(format!(
            "DELETE {field} has invalid UUID width"
        )));
    }
    let mut uuid = [0; 16];
    uuid.copy_from_slice(bytes);
    Ok(Some(uuid))
}

fn count_deleted_properties(
    env: &PhaseEnv<'_>,
    targets: &HashSet<[u8; 16]>,
    edge: bool,
) -> Result<u64, GfError> {
    match &env.inventory {
        Some(inventory) => graphforge_storage::count_entity_properties_from_inventory(
            env.dir, inventory, targets, edge,
        ),
        None => graphforge_storage::count_entity_properties(env.dir, targets, edge),
    }
}

/// DELETE phase: collect targets from the frontier, enforce the openCypher
/// incident-edge rule against committed **and** pending edges, cancel
/// pending-created targets in the buffer, and queue committed targets for the
/// commit-time rewrite.
pub(super) fn run_delete_phase(
    env: &PhaseEnv<'_>,
    vars: &[VarId],
    exprs: &[ExprId],
    detach: bool,
    frontier: &Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    let cols = resolve_delete_cols(&frontier.df_schema, vars)?;
    let mut node_targets: HashSet<[u8; 16]> = HashSet::new();
    let mut edge_targets: HashSet<[u8; 16]> = HashSet::new();
    for batch in &frontier.batches {
        collect_delete_targets(batch, &cols, &mut node_targets, &mut edge_targets)?;
    }
    collect_delete_expr_targets(
        env,
        exprs,
        frontier,
        var_map,
        &mut node_targets,
        &mut edge_targets,
    )?;
    // Deleting an already-deleted entity is a no-op.
    node_targets.retain(|u| !ctx.deleted.contains(u));
    edge_targets.retain(|u| !ctx.deleted.contains(u));

    // Incident edges: committed files for committed targets, plus the
    // statement's own pending buffer (an edge created earlier in this
    // statement counts, #792). A pending-created node cannot have committed
    // incident edges.
    let pending_nodes: HashSet<[u8; 16]> = node_targets
        .iter()
        .copied()
        .filter(|u| ctx.writer.contains_pending_node(u))
        .collect();
    let committed_nodes: HashSet<[u8; 16]> =
        node_targets.difference(&pending_nodes).copied().collect();
    let mut removed_labels = ctx.writer.pending_node_labels(&pending_nodes);
    let mut committed_labels = persisted_node_labels(env.dir, &committed_nodes)?;
    for uuid in &committed_nodes {
        let labels = committed_labels.entry(*uuid).or_default();
        if let Some(additions) = ctx.label_additions.get(uuid) {
            labels.extend(additions);
        }
        if let Some(removals) = ctx.label_removals.get(uuid) {
            labels.retain(|label| !removals.contains(label));
        }
    }
    removed_labels.extend(committed_labels.into_values().flatten());
    if !removed_labels.is_empty() {
        let surviving_labels = surviving_node_labels(env.dir, &node_targets, ctx)?;
        removed_labels.retain(|label| !surviving_labels.contains(label));
    }
    ctx.record_removed_label_tokens(removed_labels);
    let mut incident: HashSet<[u8; 16]> =
        graphforge_storage::incident_edge_uuids(env.dir, &committed_nodes)?
            .into_iter()
            .collect();
    incident.extend(ctx.writer.pending_incident_edge_uuids(&node_targets));

    let survivors: Vec<[u8; 16]> = incident
        .into_iter()
        .filter(|e| !edge_targets.contains(e) && !ctx.deleted.contains(e))
        .collect();
    if detach {
        edge_targets.extend(survivors);
    } else if !survivors.is_empty() {
        return Err(GfError::Execution(
            "Cannot delete node, because it still has relationships. To delete \
             this node, you must first delete its relationships, or use DETACH DELETE."
                .into(),
        ));
    }
    let mutation_kind = if detach {
        crate::MutationKind::DetachDelete
    } else {
        crate::MutationKind::Delete
    };
    for uuid in &node_targets {
        ctx.record_mutation_input(mutation_kind, crate::MutationSubjectKind::Node, *uuid);
    }
    for uuid in &edge_targets {
        ctx.record_mutation_input(mutation_kind, crate::MutationSubjectKind::Edge, *uuid);
    }

    // Edges: cancel pending-created ones in the buffer (they never hit disk),
    // queue committed ones for the commit-time rewrite. Both count.
    let pending_edges: HashSet<[u8; 16]> = edge_targets
        .iter()
        .copied()
        .filter(|u| ctx.writer.contains_pending_edge(u))
        .collect();
    let committed_edges: HashSet<[u8; 16]> =
        edge_targets.difference(&pending_edges).copied().collect();
    ctx.mutation.counters.properties_removed +=
        count_deleted_properties(env, &committed_edges, true)?;
    ctx.mutation.counters.edges_deleted += edge_targets.len() as u64;
    ctx.writer.cancel_edges(&pending_edges);
    ctx.pending_edge_deletes.extend(&committed_edges);
    ctx.deleted.extend(edge_targets.iter().copied());

    // Nodes, likewise.
    ctx.mutation.counters.properties_removed +=
        count_deleted_properties(env, &committed_nodes, false)?;
    ctx.mutation.counters.nodes_deleted += node_targets.len() as u64;
    ctx.writer.cancel_nodes(&pending_nodes);
    ctx.pending_node_deletes.extend(committed_nodes);
    ctx.deleted.extend(node_targets);
    Ok(())
}

fn persisted_node_labels(
    dir: &Path,
    targets: &HashSet<[u8; 16]>,
) -> Result<HashMap<[u8; 16], HashSet<EntityTypeId>>, GfError> {
    if targets.is_empty() {
        return Ok(HashMap::new());
    }
    let mut found = HashMap::new();
    for batch in
        graphforge_storage::read_nodes(dir).map_err(|error| GfError::Storage(error.to_string()))?
    {
        collect_node_label_batch(&batch, Some(targets), &mut found)?;
    }
    Ok(found)
}

fn surviving_node_labels(
    dir: &Path,
    deleting: &HashSet<[u8; 16]>,
    ctx: &StatementWriteContext,
) -> Result<HashSet<EntityTypeId>, GfError> {
    let mut nodes = HashMap::new();
    for batch in
        graphforge_storage::read_nodes(dir).map_err(|error| GfError::Storage(error.to_string()))?
    {
        collect_node_label_batch(&batch, None, &mut nodes)?;
    }
    collect_node_label_batch(&ctx.writer.pending_nodes_batch()?, None, &mut nodes)?;
    for uuid in deleting
        .iter()
        .chain(&ctx.pending_node_deletes)
        .chain(&ctx.deleted)
    {
        nodes.remove(uuid);
    }
    for (uuid, additions) in &ctx.label_additions {
        if let Some(labels) = nodes.get_mut(uuid) {
            labels.extend(additions);
        }
    }
    for (uuid, removals) in &ctx.label_removals {
        if let Some(labels) = nodes.get_mut(uuid) {
            labels.retain(|label| !removals.contains(label));
        }
    }
    Ok(nodes.into_values().flatten().collect())
}

fn collect_node_label_batch(
    batch: &RecordBatch,
    targets: Option<&HashSet<[u8; 16]>>,
    found: &mut HashMap<[u8; 16], HashSet<EntityTypeId>>,
) -> Result<(), GfError> {
    let uuids = batch
        .column_by_name("node_uuid")
        .and_then(|array| {
            array
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        })
        .ok_or_else(|| GfError::Storage("node topology missing node_uuid".into()))?;
    let labels = batch
        .column_by_name("type_ids")
        .and_then(|array| array.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| GfError::Storage("node topology missing type_ids".into()))?;
    for row in 0..batch.num_rows() {
        if uuids.is_null(row) || uuids.value_length() != 16 {
            continue;
        }
        let mut uuid = [0; 16];
        uuid.copy_from_slice(uuids.value(row));
        if targets.is_some_and(|targets| !targets.contains(&uuid)) {
            continue;
        }
        let values = labels.value(row);
        let values = values
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| GfError::Storage("node labels are not UInt32".into()))?;
        found
            .entry(uuid)
            .or_default()
            .extend(decode_memberships(values)?);
    }
    Ok(())
}

/// Mirror of the lowerer's `resolve_write_kind` against the frontier: node
/// (`false`) when `var_<n>.node_uuid` exists, edge (`true`) when
/// `var_<n>.edge_uuid` + `rel_type_name` exist.
fn resolve_kind(schema: &DFSchema, var: VarId, clause: &str) -> Result<bool, GfError> {
    let qual = datafusion::common::TableReference::bare(format!("var_{}", var.0));
    let is_node = schema
        .index_of_column_by_name(Some(&qual), "node_uuid")
        .is_some();
    let is_edge = schema
        .index_of_column_by_name(Some(&qual), "edge_uuid")
        .is_some();
    match (is_node, is_edge) {
        (true, _) => Ok(false),
        (false, true) => {
            if schema
                .index_of_column_by_name(Some(&qual), "rel_type_name")
                .is_some()
            {
                Ok(true)
            } else {
                Err(GfError::Plan(format!(
                    "{clause} on an edge requires a known relation type \
                     (e.g. `-[r:KNOWS]->`); an untyped edge write is not yet \
                     supported (follow-up to #791)"
                )))
            }
        }
        (false, false) => Err(GfError::Plan(format!(
            "{clause} target var_{} has no node_uuid/edge_uuid column in the \
             input — it must be bound by a preceding MATCH",
            var.0
        ))),
    }
}

/// Per-input-batch routing and replacement keys. Pending creations keep their
/// writer-owned route; committed edges resolve tombstone-inclusive ownership.
struct PropertyWriteBatch {
    stems: HashMap<[u8; 16], String>,
    keys: HashMap<[u8; 16], HashSet<String>>,
}

fn resolve_property_write_batch(
    env: &PhaseEnv<'_>,
    ctx: &StatementWriteContext,
    col: &WriteCol,
    batch: &RecordBatch,
    selected: Option<&[bool]>,
    replacement: bool,
) -> Result<PropertyWriteBatch, GfError> {
    use graphforge_storage::{AuthenticatedPropertyInventory, PropertyRouteKind};
    use std::collections::{BTreeMap, BTreeSet};
    let mut stems = HashMap::new();
    let mut committed = BTreeMap::new();
    for row in 0..batch.num_rows() {
        if selected.is_some_and(|mask| !mask[row]) || batch.column(col.uuid_idx).is_null(row) {
            continue;
        }
        let uuid = fixed_binary_uuid(batch, col.uuid_idx, row)?;
        let bytes = to_bytes(&uuid);
        let stem = col.stem_for_row(batch, row, env.mode, &env.type_map)?;
        stems.insert(bytes, stem.clone());
        let pending = if col.is_edge {
            ctx.writer.contains_pending_edge(&bytes)
        } else {
            ctx.writer.contains_pending_node(&bytes)
        };
        if !pending {
            committed.insert(uuid, stem);
        }
    }
    let mut keys = HashMap::new();
    if committed.is_empty() || (!col.is_edge && !replacement) {
        return Ok(PropertyWriteBatch { stems, keys });
    }
    let captured;
    let inventory = if let Some(inventory) = env.inventory.as_deref() {
        inventory
    } else {
        captured = AuthenticatedPropertyInventory::capture(env.dir)?;
        &captured
    };
    if col.is_edge {
        let work =
            graphforge_storage::resolve_existing_edge_property_owners(inventory, &mut committed)?;
        crate::demand::record_edge_owner_work(&work, committed.len());
    }
    let mut routes: BTreeMap<String, BTreeSet<[u8; 16]>> = BTreeMap::new();
    for (uuid, stem) in committed {
        let bytes = uuid.into_bytes();
        stems.insert(bytes, stem.clone());
        if replacement {
            routes.entry(stem).or_default().insert(bytes);
        }
    }
    let kind = if col.is_edge {
        PropertyRouteKind::Edge
    } else {
        PropertyRouteKind::Node
    };
    for (route, targets) in routes {
        let (rows, work) = graphforge_storage::read_authenticated_property_snapshots_for_inventory(
            inventory, kind, &route, &targets,
        )?;
        crate::demand::record_replacement_key_work(&work, targets.len());
        for (uuid, row) in rows {
            keys.insert(uuid, row.values.into_keys().collect());
        }
    }
    Ok(PropertyWriteBatch { stems, keys })
}

/// SET phase: evaluate each item's value per frontier row; route the write to
/// the pending buffer (created entities), reject deleted targets, accumulate
/// the rest for the commit-time rewrite.
pub(super) fn run_set_phase(
    env: &PhaseEnv<'_>,
    items: &[SetPropItem],
    frontier: &mut Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    run_set_phase_masked(env, items, frontier, var_map, ctx, None, true)
}

#[allow(
    clippy::too_many_lines,
    reason = "one row loop keeps masked evaluation, persistence routing, counters, and overlays aligned"
)]
pub(super) fn run_set_phase_masked(
    env: &PhaseEnv<'_>,
    items: &[SetPropItem],
    frontier: &mut Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
    mask: Option<&[bool]>,
    use_input_types: bool,
) -> Result<(), GfError> {
    if mask.is_some_and(|mask| mask.len() != frontier.num_rows()) {
        return Err(GfError::Execution(
            "SET row mask does not match frontier rows".into(),
        ));
    }
    for item in items {
        let is_edge = resolve_kind(&frontier.df_schema, item.target, "SET")?;
        let col = WriteCol::resolve(&frontier.df_schema, item.target.0, is_edge, &item.prop_name)
            .ok_or_else(|| {
            GfError::Plan(format!(
                "SET target var_{} has no identity column in the input",
                item.target.0
            ))
        })?;
        let df_expr = if use_input_types {
            env.lowerer.lower_value_expr_with_input(
                env.exprs,
                var_map,
                item.value,
                Arc::new(frontier.df_schema.clone()),
            )?
        } else {
            env.lowerer
                .lower_value_expr(env.exprs, var_map, item.value)?
        };
        let df_expr = env.bind_read_expression(bind_expr_params(df_expr, env.params)?)?;
        let (df_expr, eval_schema) = positional_eval_expr(df_expr, &frontier.df_schema)?;
        let phys = create_physical_expr(&df_expr, &eval_schema, &ExecutionProps::new())
            .map_err(GfError::from_plan_error)?;

        let mut overlay = Vec::with_capacity(frontier.batches.len());
        let mut offset = 0usize;
        for batch in &frontier.batches {
            let n = batch.num_rows();
            let values = phys
                .evaluate(batch)
                .and_then(|cv| cv.into_array(n))
                .map_err(GfError::from_execution_error)?;
            let selected = mask.map(|mask| &mask[offset..offset + n]);
            let owners = resolve_property_write_batch(env, ctx, &col, batch, selected, false)?;
            let overlay_values = if let Some(selected) = selected {
                let selection = BooleanArray::from(selected.to_vec());
                let previous = frontier
                    .df_schema
                    .index_of_column_by_name(
                        Some(&datafusion::common::TableReference::bare(format!(
                            "var_{}",
                            item.target.0
                        ))),
                        &item.prop_name,
                    )
                    .map_or_else(
                        || {
                            ScalarValue::try_new_null(values.data_type())
                                .and_then(|value| value.to_array_of_size(n))
                        },
                        |index| Ok(Arc::clone(batch.column(index))),
                    )
                    .map_err(GfError::from_execution_error)?;
                arrow::compute::kernels::zip::zip(&selection, &values, &previous)
                    .map_err(GfError::from_execution_error)?
            } else {
                Arc::clone(&values)
            };
            overlay.push(overlay_values);
            let id_col = batch.column(col.uuid_idx);
            for row in 0..n {
                if selected.is_some_and(|selected| !selected[row]) {
                    continue;
                }
                if id_col.is_null(row) {
                    continue; // SET on a NULL (unmatched OPTIONAL row) is a no-op
                }
                let uuid = to_bytes(&fixed_binary_uuid(batch, col.uuid_idx, row)?);
                if ctx.deleted.contains(&uuid) {
                    return Err(GfError::Execution(
                        "cannot SET a property on an entity deleted in this statement".into(),
                    ));
                }
                let scalar = ScalarValue::try_from_array(&values, row)
                    .map_err(GfError::from_execution_error)?;
                let stem = owners.stems[&uuid].clone();
                if scalar.is_null() {
                    let present = property_is_present(
                        &frontier.df_schema,
                        batch,
                        item.target,
                        &item.prop_name,
                        row,
                    );
                    remove_map_complement(
                        ctx,
                        is_edge,
                        &uuid,
                        &stem,
                        &HashSet::from([item.prop_name.clone()]),
                    );
                    if present {
                        ctx.mutation.counters.properties_removed += 1;
                        ctx.record_mutation_output(
                            crate::MutationKind::RemoveProperty,
                            if is_edge {
                                crate::MutationSubjectKind::Edge
                            } else {
                                crate::MutationSubjectKind::Node
                            },
                            uuid,
                        );
                    }
                    continue;
                }
                let lit = scalar_to_ir_literal(&scalar).map_err(GfError::from_execution_error)?;
                let pending = if is_edge {
                    ctx.writer.contains_pending_edge(&uuid)
                } else {
                    ctx.writer.contains_pending_node(&uuid)
                };
                if is_edge && pending {
                    ctx.writer.merge_pending_edge_props(
                        &uuid,
                        Some(&stem),
                        HashMap::from([(item.prop_name.clone(), lit)]),
                    )?;
                } else if !is_edge && pending {
                    ctx.writer.merge_pending_node_props(
                        &uuid,
                        Some(&stem),
                        HashMap::from([(item.prop_name.clone(), lit)]),
                    )?;
                } else {
                    ctx.remove_acc
                        .forget(is_edge, &stem, &uuid, &item.prop_name);
                    ctx.set_acc
                        .record(is_edge, stem, uuid, item.prop_name.clone(), lit);
                }
                let replaced = property_is_present(
                    &frontier.df_schema,
                    batch,
                    item.target,
                    &item.prop_name,
                    row,
                );
                let recorded = if pending && replaced {
                    false
                } else {
                    ctx.record_property_set(is_edge, uuid, &item.prop_name)
                };
                if recorded && replaced {
                    ctx.mutation.counters.properties_removed += 1;
                }
                ctx.record_mutation_output(
                    crate::MutationKind::SetProperty,
                    if is_edge {
                        crate::MutationSubjectKind::Edge
                    } else {
                        crate::MutationSubjectKind::Node
                    },
                    uuid,
                );
            }
            offset += n;
        }
        frontier.overlay_property(item.target, &item.prop_name, overlay)?;
        if !is_edge {
            env.lowerer
                .register_node_property_shape(item.target, &item.prop_name);
        }
    }
    Ok(())
}

pub(super) fn run_set_map_phase(
    env: &PhaseEnv<'_>,
    items: &[SetMapItem],
    frontier: &mut Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    run_set_map_phase_with_input(env, items, frontier, var_map, ctx, true)
}

#[allow(
    clippy::too_many_lines,
    reason = "plain and tagged map writes share replacement accounting and frontier overlays"
)]
pub(super) fn run_set_map_phase_with_input(
    env: &PhaseEnv<'_>,
    items: &[SetMapItem],
    frontier: &mut Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
    use_input_types: bool,
) -> Result<(), GfError> {
    for item in items {
        let is_edge = resolve_kind(&frontier.df_schema, item.target, "SET")?;
        let identity = WriteCol::resolve(&frontier.df_schema, item.target.0, is_edge, "")
            .ok_or_else(|| {
                GfError::Plan(format!(
                    "SET target var_{} has no identity column in the input",
                    item.target.0
                ))
            })?;
        let existing_names = entity_property_names(&frontier.df_schema, item.target);
        let df_expr = if use_input_types {
            env.lowerer.lower_value_expr_with_input(
                env.exprs,
                var_map,
                item.map,
                Arc::new(frontier.df_schema.clone()),
            )?
        } else {
            env.lowerer.lower_value_expr(env.exprs, var_map, item.map)?
        };
        let df_expr = env.bind_read_expression(bind_expr_params(df_expr, env.params)?)?;
        let (df_expr, eval_schema) = positional_eval_expr(df_expr, &frontier.df_schema)?;
        let phys = create_physical_expr(&df_expr, &eval_schema, &ExecutionProps::new())
            .map_err(GfError::from_plan_error)?;
        let mut overlays: HashMap<String, Vec<ArrayRef>> = HashMap::new();

        for batch in &frontier.batches {
            let values = phys
                .evaluate(batch)
                .and_then(|cv| cv.into_array(batch.num_rows()))
                .map_err(GfError::from_execution_error)?;
            let maps = values
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    GfError::Execution("SET map expression must evaluate to a map".into())
                })?;
            let selected: Vec<_> = (0..batch.num_rows())
                .map(|row| !maps.is_null(row))
                .collect();
            let owners = resolve_property_write_batch(
                env,
                ctx,
                &identity,
                batch,
                Some(&selected),
                item.replace,
            )?;
            if maps
                .column_by_name(graphforge_value::heterogeneous::TAG)
                .is_some()
            {
                for row in 0..batch.num_rows() {
                    if maps.is_null(row) || batch.column(identity.uuid_idx).is_null(row) {
                        continue;
                    }
                    let uuid = to_bytes(&fixed_binary_uuid(batch, identity.uuid_idx, row)?);
                    let stem = owners.stems[&uuid].clone();
                    let updates = decode_tagged_map_updates(maps, row)?;
                    let mut present: HashSet<_> = existing_names
                        .iter()
                        .filter(|name| {
                            property_is_present(&frontier.df_schema, batch, item.target, name, row)
                        })
                        .cloned()
                        .collect();
                    if item.replace
                        && !ctx.writer.contains_pending_node(&uuid)
                        && !ctx.writer.contains_pending_edge(&uuid)
                    {
                        present.extend(owners.keys.get(&uuid).into_iter().flatten().cloned());
                    }
                    let removals = if item.replace {
                        present
                            .difference(&updates.keys().cloned().collect())
                            .cloned()
                            .collect::<HashSet<_>>()
                    } else {
                        HashSet::new()
                    };
                    let replaced = if item.replace {
                        present
                            .iter()
                            .filter(|name| updates.contains_key(*name))
                            .count()
                    } else {
                        0
                    };
                    remove_map_complement(ctx, is_edge, &uuid, &stem, &removals);
                    ctx.mutation.counters.properties_removed += (removals.len() + replaced) as u64;
                    for name in updates.keys() {
                        let _ = ctx.record_property_set(is_edge, uuid, name);
                    }
                    apply_map_updates(ctx, is_edge, &uuid, &stem, updates)?;
                }
                continue;
            }
            for (field, column) in maps.fields().iter().zip(maps.columns()) {
                if !is_edge {
                    env.lowerer
                        .register_node_property_shape(item.target, field.name());
                }
                overlays
                    .entry(field.name().clone())
                    .or_default()
                    .push(Arc::clone(column));
            }

            let id_col = batch.column(identity.uuid_idx);
            for row in 0..batch.num_rows() {
                if id_col.is_null(row) || maps.is_null(row) {
                    continue;
                }
                let uuid = to_bytes(&fixed_binary_uuid(batch, identity.uuid_idx, row)?);
                if ctx.deleted.contains(&uuid) {
                    return Err(GfError::Execution(
                        "cannot SET properties on an entity deleted in this statement".into(),
                    ));
                }
                let stem = owners.stems[&uuid].clone();
                let mut updates = HashMap::with_capacity(maps.num_columns());
                let mut null_keys = HashSet::new();
                for (field, column) in maps.fields().iter().zip(maps.columns()) {
                    let scalar = ScalarValue::try_from_array(column, row)
                        .map_err(GfError::from_execution_error)?;
                    if scalar.is_null() {
                        null_keys.insert(field.name().clone());
                    } else {
                        let value =
                            scalar_to_ir_literal(&scalar).map_err(GfError::from_execution_error)?;
                        updates.insert(field.name().clone(), value);
                    }
                }
                let mut present: HashSet<_> = existing_names
                    .iter()
                    .filter(|name| {
                        property_is_present(&frontier.df_schema, batch, item.target, name, row)
                    })
                    .cloned()
                    .collect();
                if item.replace
                    && !ctx.writer.contains_pending_node(&uuid)
                    && !ctx.writer.contains_pending_edge(&uuid)
                {
                    present.extend(owners.keys.get(&uuid).into_iter().flatten().cloned());
                }
                let (removals, replaced) =
                    map_removals(item.replace, &present, &updates, &null_keys);
                if !removals.is_empty() {
                    ctx.record_mutation_output(
                        crate::MutationKind::RemoveProperty,
                        if is_edge {
                            crate::MutationSubjectKind::Edge
                        } else {
                            crate::MutationSubjectKind::Node
                        },
                        uuid,
                    );
                }
                if !updates.is_empty() {
                    ctx.record_mutation_output(
                        crate::MutationKind::SetProperty,
                        if is_edge {
                            crate::MutationSubjectKind::Edge
                        } else {
                            crate::MutationSubjectKind::Node
                        },
                        uuid,
                    );
                }
                remove_map_complement(ctx, is_edge, &uuid, &stem, &removals);
                ctx.mutation.counters.properties_removed += replaced as u64;
                ctx.mutation.counters.properties_set += updates.len() as u64;
                apply_map_updates(ctx, is_edge, &uuid, &stem, updates)?;
            }
        }
        overlay_map_result(frontier, item, existing_names, overlays)?;
    }
    Ok(())
}

fn decode_tagged_map_updates(
    maps: &StructArray,
    row: usize,
) -> Result<HashMap<String, graphforge_ir::IrLiteral>, GfError> {
    let tagged = ScalarValue::try_from_array(maps, row).map_err(GfError::from_execution_error)?;
    let decoded = graphforge_rel::expr::decode_het_scalar(&tagged)
        .map_err(GfError::from_execution_error)?
        .ok_or_else(|| GfError::Execution("SET source is not a map".into()))?;
    let ScalarValue::Struct(values) = decoded else {
        return Err(GfError::Execution("SET source is not a map".into()));
    };
    let mut updates = HashMap::new();
    for (field, column) in values.fields().iter().zip(values.columns()) {
        let tagged_value =
            ScalarValue::try_from_array(column, 0).map_err(GfError::from_execution_error)?;
        let value = graphforge_rel::expr::decode_het_scalar(&tagged_value)
            .map_err(GfError::from_execution_error)?
            .unwrap_or(tagged_value);
        if !value.is_null() {
            updates.insert(
                field.name().clone(),
                scalar_to_ir_literal(&value).map_err(GfError::from_execution_error)?,
            );
        }
    }
    Ok(updates)
}

fn map_removals(
    replace: bool,
    present: &HashSet<String>,
    updates: &HashMap<String, graphforge_ir::IrLiteral>,
    null_keys: &HashSet<String>,
) -> (HashSet<String>, usize) {
    if replace {
        let removals = present
            .iter()
            .filter(|name| !updates.contains_key(*name))
            .cloned()
            .collect();
        (removals, present.len())
    } else {
        let removals = present.intersection(null_keys).cloned().collect();
        let replaced = present
            .iter()
            .filter(|name| updates.contains_key(*name) || null_keys.contains(*name))
            .count();
        (removals, replaced)
    }
}

fn overlay_map_result(
    frontier: &mut Frontier,
    item: &SetMapItem,
    existing_names: HashSet<String>,
    overlays: HashMap<String, Vec<ArrayRef>>,
) -> Result<(), GfError> {
    if item.replace {
        for name in existing_names {
            if !overlays.contains_key(&name) {
                let values = frontier
                    .batches
                    .iter()
                    .map(|batch| arrow::array::new_null_array(&DataType::Null, batch.num_rows()))
                    .collect();
                frontier.overlay_property(item.target, &name, values)?;
            }
        }
    }
    for (name, values) in overlays {
        frontier.overlay_property(item.target, &name, values)?;
    }
    Ok(())
}

fn entity_property_names(schema: &DFSchema, var: VarId) -> HashSet<String> {
    let qualifier = format!("var_{}", var.0);
    schema
        .iter()
        .filter(|(q, field)| {
            q.is_some_and(|q| q.to_string() == qualifier)
                && !matches!(
                    field.name().as_str(),
                    "node_uuid"
                        | "node_id"
                        | "type_id"
                        | "type_ids"
                        | "created_at"
                        | "updated_at"
                        | "edge_uuid"
                        | "src_uuid"
                        | "dst_uuid"
                        | "edge_id"
                        | "src_id"
                        | "dst_id"
                        | "rel_type_name"
                )
        })
        .map(|(_, field)| field.name().clone())
        .collect()
}

fn property_is_present(
    schema: &DFSchema,
    batch: &RecordBatch,
    var: VarId,
    name: &str,
    row: usize,
) -> bool {
    let qualifier = datafusion::common::TableReference::bare(format!("var_{}", var.0));
    schema
        .index_of_column_by_name(Some(&qualifier), name)
        .is_some_and(|index| !batch.column(index).is_null(row))
}

fn apply_map_updates(
    ctx: &mut StatementWriteContext,
    is_edge: bool,
    uuid: &[u8; 16],
    stem: &str,
    updates: HashMap<String, graphforge_ir::IrLiteral>,
) -> Result<(), GfError> {
    if is_edge && ctx.writer.contains_pending_edge(uuid) {
        ctx.writer
            .merge_pending_edge_props(uuid, Some(stem), updates)?;
    } else if !is_edge && ctx.writer.contains_pending_node(uuid) {
        ctx.writer
            .merge_pending_node_props(uuid, Some(stem), updates)?;
    } else {
        for (name, value) in updates {
            ctx.remove_acc.forget(is_edge, stem, uuid, &name);
            ctx.set_acc
                .record(is_edge, stem.to_owned(), *uuid, name, value);
        }
    }
    Ok(())
}

fn remove_map_complement(
    ctx: &mut StatementWriteContext,
    is_edge: bool,
    uuid: &[u8; 16],
    stem: &str,
    keys: &HashSet<String>,
) {
    if is_edge && ctx.writer.contains_pending_edge(uuid) {
        ctx.writer.remove_pending_edge_props(uuid, keys);
    } else if !is_edge && ctx.writer.contains_pending_node(uuid) {
        ctx.writer.remove_pending_node_props(uuid, keys);
    } else {
        for name in keys {
            ctx.set_acc.forget(is_edge, stem, uuid, name);
            ctx.remove_acc
                .record(is_edge, stem.to_owned(), *uuid, name.clone());
        }
    }
}

/// REMOVE phase: the value-less dual of [`run_set_phase`].
pub(super) fn run_remove_phase(
    env: &PhaseEnv<'_>,
    items: &[graphforge_ir::RemovePropItem],
    frontier: &mut Frontier,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    for item in items {
        let is_edge = resolve_kind(&frontier.df_schema, item.target, "REMOVE")?;
        let col = WriteCol::resolve(&frontier.df_schema, item.target.0, is_edge, &item.prop_name)
            .ok_or_else(|| {
            GfError::Plan(format!(
                "REMOVE target var_{} has no identity column in the input",
                item.target.0
            ))
        })?;
        // No matched batches means no mutation; retain the known property schema.
        if frontier.batches.is_empty() {
            continue;
        }
        let existing = frontier.df_schema.index_of_column_by_name(
            Some(&datafusion::common::TableReference::bare(format!(
                "var_{}",
                item.target.0
            ))),
            &item.prop_name,
        );
        let overlay_type = existing.map_or(DataType::Null, |index| {
            frontier.batches[0].column(index).data_type().clone()
        });
        let mut overlay = Vec::with_capacity(frontier.batches.len());
        for batch in &frontier.batches {
            let selected: Vec<_> = (0..batch.num_rows())
                .map(|row| {
                    property_is_present(
                        &frontier.df_schema,
                        batch,
                        item.target,
                        &item.prop_name,
                        row,
                    )
                })
                .collect();
            let owners =
                resolve_property_write_batch(env, ctx, &col, batch, Some(&selected), false)?;
            let id_col = batch.column(col.uuid_idx);
            for row in 0..batch.num_rows() {
                if id_col.is_null(row) {
                    continue;
                }
                let uuid = to_bytes(&fixed_binary_uuid(batch, col.uuid_idx, row)?);
                if ctx.deleted.contains(&uuid) {
                    return Err(GfError::Execution(
                        "cannot REMOVE a property from an entity deleted in this statement".into(),
                    ));
                }
                if !property_is_present(
                    &frontier.df_schema,
                    batch,
                    item.target,
                    &item.prop_name,
                    row,
                ) {
                    continue;
                }
                let stem = owners.stems[&uuid].clone();
                let keys = HashSet::from([item.prop_name.clone()]);
                if is_edge && ctx.writer.contains_pending_edge(&uuid) {
                    ctx.writer.remove_pending_edge_props(&uuid, &keys);
                } else if !is_edge && ctx.writer.contains_pending_node(&uuid) {
                    ctx.writer.remove_pending_node_props(&uuid, &keys);
                } else {
                    ctx.set_acc.forget(is_edge, &stem, &uuid, &item.prop_name);
                    ctx.remove_acc
                        .record(is_edge, stem, uuid, item.prop_name.clone());
                }
                ctx.mutation.counters.properties_removed += 1;
                ctx.record_mutation_output(
                    crate::MutationKind::RemoveProperty,
                    if is_edge {
                        crate::MutationSubjectKind::Edge
                    } else {
                        crate::MutationSubjectKind::Node
                    },
                    uuid,
                );
            }
            overlay.push(
                ScalarValue::try_from(&overlay_type)
                    .unwrap_or(ScalarValue::Null)
                    .to_array_of_size(batch.num_rows())
                    .map_err(GfError::from_execution_error)?,
            );
        }
        frontier.overlay_property(item.target, &item.prop_name, overlay)?;
    }
    Ok(())
}

pub(super) fn run_label_phase(
    items: &[graphforge_ir::LabelItem],
    add: bool,
    frontier: &mut Frontier,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    for item in items {
        let identity = WriteCol::resolve(&frontier.df_schema, item.target.0, false, "")
            .ok_or_else(|| GfError::Plan("label mutation target is not a bound node".into()))?;
        let qualifier = datafusion::common::TableReference::bare(format!("var_{}", item.target.0));
        let type_ids_idx = frontier
            .df_schema
            .index_of_column_by_name(Some(&qualifier), "type_ids")
            .ok_or_else(|| GfError::Plan("label mutation target has no type_ids".into()))?;
        let requested = item.labels.clone();
        let mut seen = HashSet::new();
        for batch in &frontier.batches {
            let id_col = batch.column(identity.uuid_idx);
            let labels = batch
                .column(type_ids_idx)
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| GfError::Execution("node type_ids are not a list".into()))?;
            for row in 0..batch.num_rows() {
                if id_col.is_null(row) {
                    continue;
                }
                let uuid = to_bytes(&fixed_binary_uuid(batch, identity.uuid_idx, row)?);
                if !seen.insert(uuid) {
                    continue;
                }
                if ctx.deleted.contains(&uuid) {
                    return Err(GfError::Execution(
                        "cannot mutate labels on an entity deleted in this statement".into(),
                    ));
                }
                let values = labels.value(row);
                let values = values
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| GfError::Execution("node type_ids are not UInt32".into()))?;
                let changed = requested
                    .iter()
                    .copied()
                    .filter(|label| values.values().contains(&label.encode()) != add)
                    .collect::<Vec<_>>();
                if changed.is_empty() {
                    continue;
                }
                let count = if ctx.writer.contains_pending_node(&uuid) {
                    if add {
                        ctx.writer.add_pending_node_labels(&uuid, &changed)
                    } else {
                        ctx.writer.remove_pending_node_labels(&uuid, &changed)
                    }
                } else if add {
                    if let Some(removals) = ctx.label_removals.get_mut(&uuid) {
                        for label in &changed {
                            removals.remove(label);
                        }
                    }
                    let additions = ctx.label_additions.entry(uuid).or_default();
                    let before = additions.len();
                    additions.extend(changed.iter().copied());
                    (additions.len() - before) as u64
                } else {
                    if let Some(additions) = ctx.label_additions.get_mut(&uuid) {
                        for label in &changed {
                            additions.remove(label);
                        }
                    }
                    let removals = ctx.label_removals.entry(uuid).or_default();
                    let before = removals.len();
                    removals.extend(changed.iter().copied());
                    (removals.len() - before) as u64
                };
                if count > 0 {
                    if add {
                        ctx.record_label_tokens(changed.iter().copied());
                    } else {
                        ctx.record_removed_label_tokens(changed.iter().copied());
                    }
                    ctx.record_mutation_output(
                        if add {
                            crate::MutationKind::AddLabel
                        } else {
                            crate::MutationKind::RemoveLabel
                        },
                        crate::MutationSubjectKind::Node,
                        uuid,
                    );
                }
            }
        }
        if add {
            let mask = vec![true; frontier.num_rows()];
            frontier.add_node_labels(item.target, &requested, &mask)?;
        } else {
            frontier.remove_node_labels(item.target, &requested)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
