//! CREATE and MERGE statement phases.

use std::collections::HashMap;
use std::collections::HashSet;

use std::sync::Arc;

use arrow::array::Array;
use arrow::array::ListArray;
use arrow::array::RecordBatch;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;

use datafusion::execution::context::ExecutionProps;

use datafusion::physical_expr::create_physical_expr;

use datafusion::scalar::ScalarValue;

use graphforge_core::GfError;
use graphforge_core::OntologyMode;
use graphforge_core::uuid::to_bytes;

use graphforge_ir::CreatePattern;
use graphforge_ir::Direction;
use graphforge_ir::MergeSetItem;
use graphforge_ir::VarId;
use graphforge_plan::ResolvedEdgeSpec;
use graphforge_plan::ResolvedNodeSpec;
use graphforge_rel::VarMap;
use graphforge_rel::expr::scalar_to_ir_literal;

use crate::CreateConfig;
use crate::WriteCol;
use crate::build_ref_by_var;
use crate::fixed_binary_uuid;
use crate::validate_edge_specs;
use crate::write_batch_creates;

use super::CreateRecorder;
use super::Frontier;
use super::PhaseEnv;
use super::StatementWriteContext;
use super::bind_expr_params;
use super::mutation_phases::run_set_map_phase_with_input;
use super::mutation_phases::run_set_phase_masked;
use super::positional_eval_expr;

/// CREATE phase: mint per frontier row into the shared writer, then extend
/// the frontier with the created variables.
pub(super) fn run_create_phase(
    env: &PhaseEnv<'_>,
    pattern: &CreatePattern,
    frontier: &mut Frontier,
    var_map: &mut VarMap,
    ctx: &mut StatementWriteContext,
    retain_created: Option<&HashSet<VarId>>,
) -> Result<(), GfError> {
    let (mut nodes, mut edges) = env.lowerer.resolve_create_pattern(
        pattern,
        env.exprs,
        var_map,
        &Arc::new(frontier.df_schema.clone()),
    )?;
    for name in nodes.iter().flat_map(|node| {
        node.properties
            .iter()
            .map(|(name, _)| name)
            .chain(node.computed_properties.iter().map(|(name, _)| name))
    }) {
        if matches!(name.as_str(), "node_uuid" | "node_id" | "type_id") {
            return Err(GfError::Plan(format!(
                "CREATE property `{name}` collides with a reserved node topology field"
            )));
        }
    }
    for (_, expr) in nodes
        .iter_mut()
        .flat_map(|node| node.computed_properties.iter_mut())
        .chain(
            edges
                .iter_mut()
                .flat_map(|edge| edge.computed_properties.iter_mut()),
        )
    {
        *expr = bind_expr_params(expr.clone(), env.params)?;
    }
    env.lowerer.register_created_node_shapes(&nodes);
    let cfg = CreateConfig {
        ref_cols: nodes
            .iter()
            .filter(|n| n.is_reference)
            .filter_map(|n| {
                let alias = var_map
                    .get(VarId(n.var))
                    .map_or_else(|| format!("var_{}", n.var), ToString::to_string);
                crate::RefNodeCols::resolve_with_alias(&frontier.df_schema, n.var, &alias)
            })
            .collect(),
        nodes,
        edges,
        in_df_schema: Arc::new(frontier.df_schema.clone()),
        dir: env.dir.to_path_buf(),
        mode: env.mode,
        semantic_composition_fingerprint: None,
        out_schema: graphforge_plan::GraphCreateNode::summary_schema(),
    };
    validate_edge_specs(&cfg)?;
    let ref_by_var = build_ref_by_var(&cfg);

    let mut recorder = CreateRecorder::default();
    let mut tally = crate::CreateTally::default();
    let mut computed_batches = Vec::with_capacity(frontier.batches.len());
    for batch in &frontier.batches {
        // Evaluate any row-dependent property values against this batch (#814).
        let computed =
            crate::eval_create_computed(&cfg, batch).map_err(GfError::from_execution_error)?;
        write_batch_creates(
            &cfg,
            &mut ctx.writer,
            batch,
            &ref_by_var,
            crate::CreateExtras {
                deleted: Some(&ctx.deleted),
                recorder: Some(&mut recorder),
                computed: Some(&computed),
                persisted_ids: None,
            },
            &mut tally,
        )?;
        computed_batches.push(computed);
    }
    // Fold the CREATE phase tallies into the statement's write ledger.
    if tally.nodes_created > 0 {
        ctx.record_label_tokens(
            cfg.nodes
                .iter()
                .filter(|node| !node.is_reference)
                .flat_map(|node| node.label_ids.iter().copied()),
        );
    }
    ctx.mutation.counters.nodes_created += tally.nodes_created;
    ctx.mutation.counters.edges_created += tally.edges_created;
    ctx.mutation.counters.properties_set += tally.properties_set;
    recorder.record_create_receipt(ctx);
    recorder.extend_frontier(
        frontier,
        var_map,
        &cfg.nodes,
        &cfg.edges,
        &computed_batches,
        retain_created,
    )
}

pub(super) fn run_merge_phase(
    env: &PhaseEnv<'_>,
    pattern: &CreatePattern,
    on_create: &[MergeSetItem],
    on_match: &[MergeSetItem],
    frontier: &mut Frontier,
    var_map: &mut VarMap,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    let (nodes, edges) = env.lowerer.resolve_create_pattern(
        pattern,
        env.exprs,
        var_map,
        &Arc::new(frontier.df_schema.clone()),
    )?;
    env.lowerer.register_created_node_shapes(&nodes);
    if edges.len() == 1 && nodes.iter().all(|node| node.is_reference) {
        return run_relationship_merge_phase(
            env, &edges[0], on_create, on_match, frontier, var_map, ctx,
        );
    }
    if nodes.len() != 1 || !edges.is_empty() || nodes[0].is_reference {
        return Err(GfError::Plan(
            "relationship and multi-node MERGE execution is not implemented yet".into(),
        ));
    }
    let specs = resolve_merge_node_properties_by_row(env, &nodes[0], frontier)?;
    let mut merged = Vec::new();
    let mut created = Vec::new();
    let mut source_rows = Vec::new();
    for (source_row, spec) in specs.iter().enumerate() {
        reject_null_merge_properties(&spec.properties)?;
        let found = find_matching_merge_nodes(env, &ctx.writer, spec, &ctx.deleted)?;
        if found.is_empty() {
            merged.push(create_single_merge_node(spec, ctx)?);
            created.push(true);
            source_rows.push(source_row as u64);
        } else {
            for node in found {
                merged.push(node);
                created.push(false);
                source_rows.push(source_row as u64);
            }
        }
    }
    frontier.take_rows(&source_rows)?;
    let property_names = merged
        .iter()
        .flat_map(|row| row.properties.keys().cloned())
        .collect::<HashSet<_>>();
    frontier.rename_unqualified_collisions(&property_names, var_map)?;
    frontier.append_merged_node_rows(nodes[0].var, &merged)?;
    var_map.insert(VarId(nodes[0].var), format!("var_{}", nodes[0].var));
    for (row, was_created) in merged.iter().zip(&created) {
        if *was_created {
            ctx.record_mutation_output(
                crate::MutationKind::MergeCreate,
                crate::MutationSubjectKind::Node,
                row.uuid,
            );
        } else {
            ctx.record_mutation_input(
                crate::MutationKind::MergeMatchedNoop,
                crate::MutationSubjectKind::Node,
                row.uuid,
            );
        }
    }

    let any_created = created.iter().any(|value| *value);
    let any_matched = created.iter().any(|value| !*value);
    match (any_created, any_matched) {
        (true, false) => run_merge_actions(env, on_create, frontier, var_map, ctx)?,
        (false, true) => run_merge_actions(env, on_match, frontier, var_map, ctx)?,
        (true, true) => {
            run_merge_actions_masked(env, on_create, frontier, var_map, ctx, &created)?;
            let matched = created.iter().map(|value| !value).collect::<Vec<_>>();
            run_merge_actions_masked(env, on_match, frontier, var_map, ctx, &matched)?;
        }
        (false, false) => {}
    }
    Ok(())
}

fn create_single_merge_node(
    spec: &ResolvedNodeSpec,
    ctx: &mut StatementWriteContext,
) -> Result<MatchedMergeNode, GfError> {
    let uuid = graphforge_core::uuid::new_v7();
    let labels = spec.label_ids.clone();
    let node_id = ctx.writer.create_node_with_labels(uuid, &labels)?;
    ctx.writer.set_properties(
        &uuid,
        spec.label_names.first().map(String::as_str),
        spec.properties.iter().cloned().collect(),
    )?;
    ctx.mutation.counters.nodes_created += 1;
    ctx.mutation.counters.properties_set += spec.properties.len() as u64;
    ctx.record_label_tokens(spec.label_ids.iter().copied());
    let type_id = spec.label_ids.first().copied().map_or_else(
        graphforge_value::PrimaryEntityTypeId::absent,
        graphforge_value::PrimaryEntityTypeId::known,
    );
    Ok(MatchedMergeNode {
        uuid: to_bytes(&uuid),
        node_id,
        type_id,
        label_ids: spec.label_ids.clone(),
        properties: spec.properties.iter().cloned().collect(),
    })
}

fn resolve_merge_node_properties_by_row(
    env: &PhaseEnv<'_>,
    spec: &ResolvedNodeSpec,
    frontier: &Frontier,
) -> Result<Vec<ResolvedNodeSpec>, GfError> {
    let mut physical = Vec::with_capacity(spec.computed_properties.len());
    for (name, expr) in &spec.computed_properties {
        let expr = env.bind_read_expression(bind_expr_params(expr.clone(), env.params)?)?;
        let (expr, eval_schema) = positional_eval_expr(expr, &frontier.df_schema)?;
        let expr = create_physical_expr(&expr, &eval_schema, &ExecutionProps::new())
            .map_err(GfError::from_plan_error)?;
        physical.push((name, expr));
    }
    let mut resolved_rows = Vec::with_capacity(frontier.num_rows());
    for batch in &frontier.batches {
        let evaluated = physical
            .iter()
            .map(|(name, expr)| {
                expr.evaluate(batch)
                    .and_then(|value| value.into_array(batch.num_rows()))
                    .map(|values| ((*name).clone(), values))
                    .map_err(GfError::from_execution_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            let mut resolved = spec.clone();
            for (name, values) in &evaluated {
                let scalar = ScalarValue::try_from_array(values, row)
                    .map_err(GfError::from_execution_error)?;
                resolved.properties.push((
                    name.clone(),
                    scalar_to_ir_literal(&scalar).map_err(GfError::from_execution_error)?,
                ));
            }
            resolved.computed_properties.clear();
            resolved_rows.push(resolved);
        }
    }
    Ok(resolved_rows)
}

pub(super) struct MatchedMergeNode {
    pub(super) uuid: [u8; 16],
    pub(super) node_id: u64,
    pub(super) type_id: graphforge_value::PrimaryEntityTypeId,
    pub(super) label_ids: Vec<graphforge_value::EntityTypeId>,
    pub(super) properties: HashMap<String, graphforge_ir::IrLiteral>,
}

pub(super) struct MatchedMergeEdge {
    pub(super) uuid: [u8; 16],
    pub(super) src_uuid: [u8; 16],
    pub(super) dst_uuid: [u8; 16],
    pub(super) rel_type: String,
    pub(super) properties: HashMap<String, graphforge_ir::IrLiteral>,
}

fn find_matching_merge_nodes(
    env: &PhaseEnv<'_>,
    writer: &graphforge_storage::GraphWriter,
    spec: &ResolvedNodeSpec,
    deleted: &HashSet<[u8; 16]>,
) -> Result<Vec<MatchedMergeNode>, GfError> {
    let mut matches = writer
        .find_pending_nodes(&spec.label_ids, &spec.properties)
        .into_iter()
        .map(|found| MatchedMergeNode {
            uuid: found.0,
            node_id: found.1,
            type_id: found.2,
            label_ids: found.3,
            properties: found.4,
        })
        .collect::<Vec<_>>();
    let batches =
        graphforge_storage::read_nodes(env.dir).map_err(|e| GfError::Storage(e.to_string()))?;
    for batch in batches {
        let uuids = batch
            .column_by_name("node_uuid")
            .and_then(|a| {
                a.as_any()
                    .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            })
            .ok_or_else(|| GfError::Storage("node topology missing node_uuid".into()))?;
        let node_ids = batch
            .column_by_name("node_id")
            .and_then(|a| a.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| GfError::Storage("node topology missing node_id".into()))?;
        let primary = batch
            .column_by_name("type_id")
            .and_then(|a| a.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| GfError::Storage("node topology missing type_id".into()))?;
        let labels = batch
            .column_by_name("type_ids")
            .and_then(|a| a.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| GfError::Storage("node topology missing type_ids".into()))?;
        for row in 0..batch.num_rows() {
            if uuids.is_null(row)
                || node_ids.is_null(row)
                || primary.is_null(row)
                || labels.is_null(row)
            {
                return Err(GfError::Storage(
                    "node topology contains null identity data".into(),
                ));
            }
            let row_labels = labels.value(row);
            let row_labels = row_labels
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| GfError::Storage("node type_ids are not UInt32".into()))?;
            let checked_labels = row_labels
                .iter()
                .map(|value| {
                    let encoded =
                        value.ok_or_else(|| GfError::Storage("null node membership".into()))?;
                    graphforge_value::EntityTypeId::decode(encoded)
                        .map_err(|error| GfError::Storage(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if !spec
                .label_ids
                .iter()
                .all(|wanted| checked_labels.contains(wanted))
            {
                continue;
            }
            let mut uuid = [0u8; 16];
            uuid.copy_from_slice(uuids.value(row));
            if deleted.contains(&uuid) {
                continue;
            }
            let stem = if matches!(env.mode, OntologyMode::Exploratory) {
                "_untyped"
            } else {
                spec.label_names.first().map_or("_untyped", String::as_str)
            };
            let props = graphforge_storage::read_entity_properties(env.dir, stem, &uuid, false)?;
            if spec
                .properties
                .iter()
                .all(|(name, value)| props.get(name) == Some(value))
            {
                matches.push(MatchedMergeNode {
                    uuid,
                    node_id: node_ids.value(row),
                    type_id: graphforge_value::PrimaryEntityTypeId::decode(primary.value(row))
                        .map_err(|error| GfError::Storage(error.to_string()))?,
                    label_ids: checked_labels,
                    properties: props,
                });
            }
        }
    }
    Ok(matches)
}

#[allow(
    clippy::too_many_lines,
    reason = "per-row match expansion and create routing share one ordered frontier walk"
)]
fn run_relationship_merge_phase(
    env: &PhaseEnv<'_>,
    spec: &ResolvedEdgeSpec,
    on_create: &[MergeSetItem],
    on_match: &[MergeSetItem],
    frontier: &mut Frontier,
    var_map: &mut VarMap,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    let row_specs = resolve_merge_edge_properties_by_row(env, spec, frontier)?;
    let rel_name = spec.rel_type_name.as_deref().ok_or_else(|| {
        GfError::Plan("relationship MERGE requires exactly one relationship type".into())
    })?;
    let src = WriteCol::resolve(&frontier.df_schema, spec.src, false, "")
        .ok_or_else(|| GfError::Plan("MERGE source node is not bound".into()))?;
    let dst = WriteCol::resolve(&frontier.df_schema, spec.dst, false, "")
        .ok_or_else(|| GfError::Plan("MERGE destination node is not bound".into()))?;
    let src_id_idx = frontier
        .df_schema
        .index_of_column_by_name(
            Some(&datafusion::common::TableReference::bare(format!(
                "var_{}",
                spec.src
            ))),
            "node_id",
        )
        .ok_or_else(|| GfError::Plan("MERGE source node has no node_id".into()))?;
    let dst_id_idx = frontier
        .df_schema
        .index_of_column_by_name(
            Some(&datafusion::common::TableReference::bare(format!(
                "var_{}",
                spec.dst
            ))),
            "node_id",
        )
        .ok_or_else(|| GfError::Plan("MERGE destination node has no node_id".into()))?;
    let edge_batches = match &env.inventory {
        Some(inventory) => {
            graphforge_storage::read_edges_from_inventory(inventory, rel_name, env.mode)
        }
        None => graphforge_storage::read_edges(env.dir, rel_name, env.mode),
    }
    .map_err(|e| GfError::Storage(e.to_string()))?;
    let mut edge_rows = Vec::with_capacity(frontier.num_rows());
    let mut created = Vec::with_capacity(frontier.num_rows());
    let mut input_rows = Vec::with_capacity(frontier.num_rows());

    let mut spec_row = 0usize;
    let mut input_row = 0u64;
    for batch in &frontier.batches {
        for row in 0..batch.num_rows() {
            let row_spec = &row_specs[spec_row];
            spec_row += 1;
            reject_null_merge_properties(&row_spec.properties)?;
            let src_uuid = fixed_binary_uuid(batch, src.uuid_idx, row)?;
            let dst_uuid = fixed_binary_uuid(batch, dst.uuid_idx, row)?;
            let src_bytes = to_bytes(&src_uuid);
            let dst_bytes = to_bytes(&dst_uuid);
            let matches = find_matching_merge_edges(
                env,
                &ctx.writer,
                row_spec,
                rel_name,
                &src_bytes,
                &dst_bytes,
                &edge_batches,
                &ctx.deleted,
            )?;
            if !matches.is_empty() {
                for matched in matches {
                    edge_rows.push(matched);
                    created.push(false);
                    input_rows.push(input_row);
                }
                input_row += 1;
                continue;
            }

            let src_id = merge_node_id_at(batch, src_id_idx, row, "source")?;
            let dst_id = merge_node_id_at(batch, dst_id_idx, row, "destination")?;
            ctx.writer.register_existing_node(src_uuid, src_id)?;
            ctx.writer.register_existing_node(dst_uuid, dst_id)?;
            let edge_value = graphforge_core::uuid::new_v7();
            ctx.writer
                .create_edge(edge_value, rel_name, &src_uuid, &dst_uuid)?;
            ctx.writer.set_edge_properties(
                &edge_value,
                Some(rel_name),
                row_spec.properties.iter().cloned().collect(),
            )?;
            edge_rows.push(MatchedMergeEdge {
                uuid: graphforge_core::uuid::to_bytes(&edge_value),
                src_uuid: src_bytes,
                dst_uuid: dst_bytes,
                rel_type: rel_name.to_owned(),
                properties: row_spec.properties.iter().cloned().collect(),
            });
            created.push(true);
            input_rows.push(input_row);
            input_row += 1;
            ctx.mutation.counters.edges_created += 1;
            ctx.mutation.counters.properties_set += row_spec.properties.len() as u64;
        }
    }
    frontier.take_rows(&input_rows)?;
    let property_names = edge_rows
        .iter()
        .flat_map(|row| row.properties.keys().cloned())
        .collect::<HashSet<_>>();
    frontier.rename_unqualified_collisions(&property_names, var_map)?;
    frontier.append_merged_edge_rows(spec.var, &edge_rows)?;
    var_map.insert(VarId(spec.var), format!("var_{}", spec.var));
    for (row, was_created) in edge_rows.iter().zip(&created) {
        let kind = if *was_created {
            crate::MutationKind::MergeCreate
        } else {
            crate::MutationKind::MergeMatchedNoop
        };
        ctx.record_mutation_input(kind, crate::MutationSubjectKind::Node, row.src_uuid);
        ctx.record_mutation_input(kind, crate::MutationSubjectKind::Node, row.dst_uuid);
        if *was_created {
            ctx.record_mutation_output(kind, crate::MutationSubjectKind::Edge, row.uuid);
        } else {
            ctx.record_mutation_input(kind, crate::MutationSubjectKind::Edge, row.uuid);
        }
    }
    let any_created = created.iter().any(|value| *value);
    let any_matched = created.iter().any(|value| !*value);
    match (any_created, any_matched) {
        (true, false) => run_merge_actions(env, on_create, frontier, var_map, ctx),
        (false, true) => run_merge_actions(env, on_match, frontier, var_map, ctx),
        (true, true) => {
            run_merge_actions_masked(env, on_create, frontier, var_map, ctx, &created)?;
            let matched = created.iter().map(|value| !value).collect::<Vec<_>>();
            run_merge_actions_masked(env, on_match, frontier, var_map, ctx, &matched)
        }
        (false, false) => Ok(()),
    }
}

fn resolve_merge_edge_properties_by_row(
    env: &PhaseEnv<'_>,
    spec: &ResolvedEdgeSpec,
    frontier: &Frontier,
) -> Result<Vec<ResolvedEdgeSpec>, GfError> {
    let mut physical = Vec::with_capacity(spec.computed_properties.len());
    for (name, expr) in &spec.computed_properties {
        let expr = env.bind_read_expression(bind_expr_params(expr.clone(), env.params)?)?;
        let (expr, eval_schema) = positional_eval_expr(expr, &frontier.df_schema)?;
        let expr = create_physical_expr(&expr, &eval_schema, &ExecutionProps::new())
            .map_err(GfError::from_plan_error)?;
        physical.push((name, expr));
    }
    let mut resolved_rows = Vec::with_capacity(frontier.num_rows());
    for batch in &frontier.batches {
        let evaluated = physical
            .iter()
            .map(|(name, expr)| {
                expr.evaluate(batch)
                    .and_then(|value| value.into_array(batch.num_rows()))
                    .map(|values| ((*name).clone(), values))
                    .map_err(GfError::from_execution_error)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for row in 0..batch.num_rows() {
            let mut resolved = spec.clone();
            for (name, values) in &evaluated {
                let scalar = ScalarValue::try_from_array(values, row)
                    .map_err(GfError::from_execution_error)?;
                resolved.properties.push((
                    name.clone(),
                    scalar_to_ir_literal(&scalar).map_err(GfError::from_execution_error)?,
                ));
            }
            resolved.computed_properties.clear();
            resolved_rows.push(resolved);
        }
    }
    Ok(resolved_rows)
}

fn merge_node_id_at(
    batch: &RecordBatch,
    index: usize,
    row: usize,
    endpoint: &str,
) -> Result<u64, GfError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .map(|ids| ids.value(row))
        .ok_or_else(|| GfError::Execution(format!("MERGE {endpoint} node_id is not UInt64")))
}

fn reject_null_merge_properties(
    properties: &[(String, graphforge_ir::IrLiteral)],
) -> Result<(), GfError> {
    if let Some((name, _)) = properties
        .iter()
        .find(|(_, value)| matches!(value, graphforge_ir::IrLiteral::Null))
    {
        return Err(GfError::Execution(format!(
            "MERGE property `{name}` cannot be null"
        )));
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "edge matching requires storage, pattern, endpoints, batches, and statement deletes"
)]
fn find_matching_merge_edges(
    env: &PhaseEnv<'_>,
    writer: &graphforge_storage::GraphWriter,
    spec: &ResolvedEdgeSpec,
    rel_name: &str,
    wanted_src: &[u8; 16],
    wanted_dst: &[u8; 16],
    batches: &[RecordBatch],
    deleted: &HashSet<[u8; 16]>,
) -> Result<Vec<MatchedMergeEdge>, GfError> {
    let mut matches = Vec::new();
    if let Some((uuid, src_uuid, dst_uuid, properties)) = writer.find_pending_edge(
        rel_name,
        wanted_src,
        wanted_dst,
        matches!(spec.direction, Direction::Undirected),
        &spec.properties,
    ) {
        matches.push(MatchedMergeEdge {
            uuid,
            src_uuid,
            dst_uuid,
            rel_type: rel_name.to_owned(),
            properties,
        });
    }
    for batch in batches {
        let edge = uuid_column(batch, "edge_uuid")?;
        let src = uuid_column(batch, "src_uuid")?;
        let dst = uuid_column(batch, "dst_uuid")?;
        let names = batch
            .column_by_name("rel_type_name")
            .and_then(|a| a.as_any().downcast_ref::<arrow::array::StringArray>());
        for row in 0..batch.num_rows() {
            if names.is_some_and(|names| names.value(row) != rel_name) {
                continue;
            }
            let directed = src.value(row) == wanted_src && dst.value(row) == wanted_dst;
            let reverse = src.value(row) == wanted_dst && dst.value(row) == wanted_src;
            if !(directed || matches!(spec.direction, Direction::Undirected) && reverse) {
                continue;
            }
            let mut uuid = [0u8; 16];
            uuid.copy_from_slice(edge.value(row));
            if deleted.contains(&uuid) {
                continue;
            }
            let props = graphforge_storage::read_entity_properties(env.dir, rel_name, &uuid, true)?;
            if spec
                .properties
                .iter()
                .all(|(name, value)| props.get(name) == Some(value))
            {
                let mut src_uuid = [0u8; 16];
                src_uuid.copy_from_slice(src.value(row));
                let mut dst_uuid = [0u8; 16];
                dst_uuid.copy_from_slice(dst.value(row));
                matches.push(MatchedMergeEdge {
                    uuid,
                    src_uuid,
                    dst_uuid,
                    rel_type: rel_name.to_owned(),
                    properties: props,
                });
            }
        }
    }
    Ok(matches)
}

fn uuid_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a arrow::array::FixedSizeBinaryArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref())
        .ok_or_else(|| GfError::Storage(format!("edge topology missing {name}")))
}

fn run_merge_actions(
    env: &PhaseEnv<'_>,
    actions: &[MergeSetItem],
    frontier: &mut Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
) -> Result<(), GfError> {
    let mask = vec![true; frontier.num_rows()];
    run_merge_actions_masked(env, actions, frontier, var_map, ctx, &mask)
}

fn run_merge_actions_masked(
    env: &PhaseEnv<'_>,
    actions: &[MergeSetItem],
    frontier: &mut Frontier,
    var_map: &VarMap,
    ctx: &mut StatementWriteContext,
    mask: &[bool],
) -> Result<(), GfError> {
    if mask.len() != frontier.num_rows() {
        return Err(GfError::Execution(
            "MERGE action mask does not match frontier rows".into(),
        ));
    }
    let props = actions
        .iter()
        .filter_map(|action| match action {
            MergeSetItem::Property(item) => Some(item.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let maps = actions
        .iter()
        .filter_map(|action| match action {
            MergeSetItem::Map(item) => Some(item.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    for action in actions {
        let MergeSetItem::AddLabels { target, labels } = action else {
            continue;
        };
        let identity = WriteCol::resolve(&frontier.df_schema, target.0, false, "")
            .ok_or_else(|| GfError::Plan("MERGE label target is not a bound node".into()))?;
        let qualifier = datafusion::common::TableReference::bare(format!("var_{}", target.0));
        let type_ids_idx = frontier
            .df_schema
            .index_of_column_by_name(Some(&qualifier), "type_ids")
            .ok_or_else(|| GfError::Plan("MERGE label target has no type_ids".into()))?;
        let label_ids = labels.clone();
        let mut offset = 0usize;
        for batch in &frontier.batches {
            for row in 0..batch.num_rows() {
                let selected = mask[offset + row];
                if !selected {
                    continue;
                }
                let uuid = to_bytes(&fixed_binary_uuid(batch, identity.uuid_idx, row)?);
                let existing = batch
                    .column(type_ids_idx)
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| GfError::Execution("node type_ids are not a list".into()))?
                    .value(row);
                let existing = existing
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(|| GfError::Execution("node type_ids are not UInt32".into()))?;
                let missing = label_ids
                    .iter()
                    .copied()
                    .filter(|label| !existing.values().contains(&label.encode()))
                    .collect::<Vec<_>>();
                let added = if ctx.writer.contains_pending_node(&uuid) {
                    ctx.writer.add_pending_node_labels(&uuid, &missing)
                } else {
                    let entry = ctx.label_additions.entry(uuid).or_default();
                    let before = entry.len();
                    entry.extend(missing.iter().copied());
                    (entry.len() - before) as u64
                };
                if added > 0 {
                    ctx.record_label_tokens(missing.iter().copied());
                }
            }
            offset += batch.num_rows();
        }
        frontier.add_node_labels(*target, &label_ids, mask)?;
    }
    run_set_phase_masked(env, &props, frontier, var_map, ctx, Some(mask), false)?;
    if !maps.is_empty() && mask.iter().any(|selected| !selected) {
        return Err(GfError::Plan(
            "row-conditional MERGE map actions are not implemented yet".into(),
        ));
    }
    run_set_map_phase_with_input(env, &maps, frontier, var_map, ctx, false)
}
