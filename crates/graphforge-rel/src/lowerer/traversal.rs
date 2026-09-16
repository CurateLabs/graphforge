//! Traversal lowering.

use super::scans::{
    edge_property_candidate_stems, edge_property_schema, join_edge_properties, lower_edge_scan,
    lower_node_scan, lower_typed_edge_scan, project_typed_edge_route,
};
use super::{
    Arc, DfExpr, Direction, EXPLORATORY_EDGE_SCHEMA, Extension, HashMap, HashSet, JoinType,
    LogicalPlan, LogicalPlanBuilder, LoweringError, LoweringSnapshot, MapUnsupportedExpr,
    OntologyMode, RelationTypeId, TOPOLOGY_NODES_SCHEMA, TYPED_EDGE_SCHEMA, VarId,
    VarLenExpandNode, VarMap, var_alias,
};

/// Lower a variable-length `Expand` (`min_hops != 1 || max_hops != Some(1)`)
/// into the `VarLenExpandNode` Extension whose physical execution (physical execution) runs an
/// iterative BFS over the edge table.
#[allow(clippy::too_many_arguments)]
#[allow(
    clippy::too_many_lines,
    reason = "property discovery, bound-variable correlation, and extension shaping are one lowering operation"
)]
fn lower_var_len_expand(
    src: VarId,
    edge: VarId,
    dst: VarId,
    rel_ty: Option<RelationTypeId>,
    dir: Direction,
    min_hops: u16,
    max_hops: Option<u16>,
    input: LogicalPlan,
    var_map: &mut VarMap,
    type_id_to_rel_name: &HashMap<RelationTypeId, String>,
    inference_rules: &HashMap<RelationTypeId, Vec<(String, String)>>,
    target: Option<(&LoweringSnapshot, OntologyMode)>,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::logical_expr::col;

    let bound_edge_list = var_map.get(edge).map(str::to_owned);
    let bound_dst = var_map.get(dst).map(str::to_owned);
    let traversal_dst = bound_dst
        .as_ref()
        .map_or(dst, |_| VarId(u32::MAX.saturating_sub(dst.0)));
    let rel_name = match rel_ty {
        Some(rt) => type_id_to_rel_name.get(&rt).cloned().ok_or_else(|| {
            LoweringError::UnsupportedExpr(format!(
                "VarLenExpand: TypeId({}) has no known relation name; \
                 ontology may be incomplete or stale",
                rt.encode()
            ))
        })?,
        None => "*".to_owned(),
    };
    // Ontology inference (#605): if this relation carries semantic rules
    // (transitive/symmetric), wrap the var-len traversal in OntologyInferNode(s)
    // so the closure is auditable. Empty in exploratory mode (no ontology) → the
    // TCK-safety gate. Captured here before `rel_name` is moved into the node.
    let infer_rules: Vec<(String, String)> = rel_ty
        .and_then(|rt| inference_rules.get(&rt))
        .cloned()
        .unwrap_or_default();
    let rel_for_infer = rel_name.clone();
    // Lowering discovers the logical output schema here; execution resolves
    // the graph resource from its own session context.
    let (dir_path, _mode) = target.ok_or_else(|| {
        LoweringError::UnsupportedExpr(
            "variable-length expand requires a dataset snapshot; \
             lower via new_for_writes or new_for_reads"
                .into(),
        )
    })?;
    // Discover the relation's persisted edge-property columns (#755) so the
    // edge-list struct carries them and `r[i].<prop>` resolves. Mirrors the
    // fixed-hop `join_edge_properties`: read the dynamic on-disk schema and drop
    // the `edge_uuid` key + any name colliding with the struct's four topology
    // fields (`edge_uuid`/`src_uuid`/`dst_uuid`/`rel_type`). A wildcard (`*`)
    // has no single property file, so it unions EVERY relation's fields (#1023)
    // — sorted stem order for determinism, first concrete type of a name wins,
    // forced nullable since an edge from a relation without the column is NULL
    // (the exec coalesces each edge's values from its own relation's file).
    let topology_names = ["edge_uuid", "src_uuid", "dst_uuid", "rel_type"];
    let prop_fields: Vec<datafusion::arrow::datatypes::Field> = {
        let mut positions = std::collections::HashMap::<String, usize>::new();
        let mut fields: Vec<datafusion::arrow::datatypes::Field> = Vec::new();
        for stem in edge_property_candidate_stems(dir_path, &rel_name) {
            let prop_table = edge_property_schema(dir_path, &stem);
            for f in prop_table.fields() {
                if topology_names.contains(&f.name().as_str()) {
                    continue;
                }
                if let Some(&position) = positions.get(f.name()) {
                    if fields[position].data_type() == &datafusion::arrow::datatypes::DataType::Null
                    {
                        fields[position] = f.as_ref().clone().with_nullable(true);
                    }
                } else {
                    positions.insert(f.name().clone(), fields.len());
                    fields.push(f.as_ref().clone().with_nullable(true));
                }
            }
        }
        fields
    };

    // The output extends the input with the destination node's columns,
    // qualified `var_<dst>` (mirrors a `NodeScan(dst)`), then a trailing
    // edge-list column qualified `var_<edge>` (#709).  Pass the field lists in
    // so graphforge-plan need not depend on graphforge-storage; the edge-list field type is the
    // shared single source of truth in graphforge-plan.
    let dst_fields = TOPOLOGY_NODES_SCHEMA.fields().iter().cloned().collect();
    let node = VarLenExpandNode::new(
        Arc::new(input),
        rel_name,
        min_hops,
        max_hops,
        src.0,
        traversal_dst.0,
        edge.0,
        dir,
        rel_ty,
        dst_fields,
        graphforge_plan::var_len_edge_list_field(&prop_fields),
    );
    // Register the destination var so the binder's trailing NodeScan(dst)
    // recognises it as already bound (and stays a no-op, preserving this
    // Extension node as the plan root).
    if bound_dst.is_none() {
        var_map.insert(dst, var_alias(dst));
    }
    // Register the edge var bound to the single `List<Struct>` relationship-list
    // column.  Unlike a node var (which stores the bare `var_<n>` qualifier and
    // resolves only via `n.<field>`), the edge var maps to one concrete column,
    // so it is registered fully-qualified: `RETURN r` lowers to
    // `col("var_<edge>.rels")` and `length(r)` to `array_length` over it.
    var_map.insert(
        edge,
        format!(
            "var_{}.{}",
            edge.0,
            graphforge_plan::VAR_LEN_EDGE_LIST_FIELD
        ),
    );
    let mut base = LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    });
    if let Some(bound_dst) = bound_dst {
        let traversal_alias = var_alias(traversal_dst);
        base = LogicalPlanBuilder::from(base)
            .filter(
                col(format!("{traversal_alias}.node_uuid"))
                    .eq(col(format!("{bound_dst}.node_uuid"))),
            )
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;
        let traversal_ref = datafusion::common::TableReference::bare(traversal_alias);
        let projection = base
            .schema()
            .iter()
            .filter(|(qualifier, _)| {
                qualifier
                    .as_ref()
                    .is_none_or(|qualifier| **qualifier != traversal_ref)
            })
            .map(|(qualifier, field)| {
                DfExpr::Column(datafusion::common::Column::new(
                    qualifier.cloned(),
                    field.name(),
                ))
            })
            .collect::<Vec<_>>();
        base = LogicalPlanBuilder::from(base)
            .project(projection)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;
    }
    if let Some(bound_edge_list) = bound_edge_list {
        let produced = format!(
            "var_{}.{}",
            edge.0,
            graphforge_plan::VAR_LEN_EDGE_LIST_FIELD
        );
        base = LogicalPlanBuilder::from(base)
            .filter(col(bound_edge_list).eq(col(produced)))
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;
    }
    // Wrap in one OntologyInferNode per applicable rule (#605); the execution
    // session records a kind="inference" provenance event per rule. Pass-through
    // physically — VarLenExpand already computes the closure.
    let plan = infer_rules
        .into_iter()
        .fold(base, |acc, (rule_id, conf_model)| {
            LogicalPlan::Extension(Extension {
                node: Arc::new(graphforge_plan::OntologyInferNode::new(
                    Arc::new(acc),
                    rel_for_infer.clone(),
                    rule_id,
                    conf_model,
                )),
            })
        });
    Ok(plan)
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
pub(super) fn lower_expand(
    src: VarId,
    edge: VarId,
    dst: VarId,
    rel_ty: Option<RelationTypeId>,
    dir: Direction,
    min_hops: u16,
    max_hops: Option<u16>,
    input: LogicalPlan,
    var_map: &mut VarMap,
    catalog: Option<&LoweringSnapshot>,
    type_id_to_rel_name: &HashMap<RelationTypeId, String>,
    inference_rules: &HashMap<RelationTypeId, Vec<(String, String)>>,
    target: Option<(&LoweringSnapshot, OntologyMode)>,
    #[cfg(feature = "differential-testing")] relational_reference: bool,
) -> Result<LogicalPlan, LoweringError> {
    // Variable-length expand cannot be expressed in relational algebra; emit
    // the graphforge-plan Extension node whose physical execution (physical execution) performs an
    // iterative BFS over the edge table.
    if min_hops != 1 || max_hops != Some(1) {
        return lower_var_len_expand(
            src,
            edge,
            dst,
            rel_ty,
            dir,
            min_hops,
            max_hops,
            input,
            var_map,
            type_id_to_rel_name,
            inference_rules,
            target,
        );
    }

    // The source must be bound BEFORE either single-hop strategy: the
    // adjacency path would otherwise reach execution with no
    // `var_<src>.node_id` and silently seed from column 0.
    let src_alias = var_map
        .get(src)
        .ok_or(LoweringError::UnboundVar(src.0))?
        .to_owned();

    // A project-backed fixed hop always uses the provider-backed Extension
    // node (#1248). The provider owns hit/miss/building fallback, keeping the
    // physical shape stable so a terminal LIMIT can cancel traversal work.
    #[cfg(not(feature = "differential-testing"))]
    let relational_reference = false;
    if !relational_reference
        && let Some(plan) = try_lower_provider_expand(
            src,
            edge,
            dst,
            rel_ty,
            dir,
            &input,
            var_map,
            type_id_to_rel_name,
            target,
        )?
    {
        return Ok(plan);
    }

    match dir {
        Direction::Out => expand_single_dir(
            src,
            edge,
            dst,
            rel_ty,
            &src_alias,
            true,
            input,
            var_map,
            catalog,
            type_id_to_rel_name,
            target,
        ),
        Direction::In => expand_single_dir(
            src,
            edge,
            dst,
            rel_ty,
            &src_alias,
            false,
            input,
            var_map,
            catalog,
            type_id_to_rel_name,
            target,
        ),
        Direction::Undirected => {
            use datafusion::common::Column;
            use datafusion::logical_expr::col;

            // Undirected = Out ∪ In. Build both legs with independent VarMaps
            // (both register the same `var_<n>` aliases), then copy the
            // edge/dst registrations back into the outer map.
            let mut vm_out = var_map.clone();
            let mut vm_in = var_map.clone();
            let out_plan = expand_single_dir(
                src,
                edge,
                dst,
                rel_ty,
                &src_alias,
                true,
                input.clone(),
                &mut vm_out,
                catalog,
                type_id_to_rel_name,
                target,
            )?;
            let in_plan = expand_single_dir(
                src,
                edge,
                dst,
                rel_ty,
                &src_alias,
                false,
                input,
                &mut vm_in,
                catalog,
                type_id_to_rel_name,
                target,
            )?;
            // Copy the newly registered edge/dst vars from the Out map.
            if let Some(edge_col) = vm_out.get(edge) {
                var_map.insert(edge, edge_col.to_owned());
            }
            if let Some(dst_col) = vm_out.get(dst) {
                var_map.insert(dst, dst_col.to_owned());
            }

            // The ONLY full-row duplicate between the two legs is a self-loop:
            // an edge with `src_id == dst_id` is matched by BOTH the Out join
            // (`src = edge.src_id`) and the In join (`src = edge.dst_id`) for the
            // same bound node, yielding an identical row; every other edge
            // matches exactly one leg per bound node. Drop self-loops from the
            // In leg so the union is duplicate-free WITHOUT a `Distinct` — a
            // wrapping `Distinct` over the merged multi-`var_<n>` schema trips
            // DataFusion's duplicate-field-name check at physical planning (the
            // #825 failure). This mirrors the adjacency path, which likewise
            // collapses only the self-loop's double entry.
            let edge_alias = var_alias(edge);
            let in_plan = LogicalPlanBuilder::from(in_plan)
                .filter(
                    col(format!("{edge_alias}.src_id")).not_eq(col(format!("{edge_alias}.dst_id"))),
                )
                .and_then(LogicalPlanBuilder::build)
                .map_unsupported_expr()?;

            // DataFusion's `union` DROPS all relation qualifiers and, for
            // same-named columns (`var_0.node_id` and `var_2.node_id` both become
            // bare `node_id`), appends positional suffixes (`node_id_1`). Those
            // synthetic names are UNSTABLE under the `optimize_projections` pass
            // (pruning a collision shifts the suffix), so a projection that
            // references them post-union breaks the optimizer. Instead, rename
            // each leg's columns to collision-free positional names (`__u{i}`)
            // BEFORE the union — so the union output has no collisions to
            // disambiguate — then restore the original `var_<n>` qualifiers AFTER
            // it from the captured leg schema, referencing only the stable names
            // (mirroring `join_*_properties`' `alias_qualified` re-qualification).
            // Without this, downstream `b.<prop>` / `r.<prop>` refs and the
            // trailing `NodeScan{dst}` property join cannot resolve their columns.
            let leg_schema = out_plan.schema().clone();
            let stable = |plan: LogicalPlan| -> Result<LogicalPlan, LoweringError> {
                let proj: Vec<DfExpr> = plan
                    .schema()
                    .iter()
                    .enumerate()
                    .map(|(i, (q, f))| {
                        DfExpr::Column(Column::new(q.cloned(), f.name())).alias(format!("__u{i}"))
                    })
                    .collect();
                LogicalPlanBuilder::from(plan)
                    .project(proj)
                    .and_then(LogicalPlanBuilder::build)
                    .map_unsupported_expr()
            };
            let out_plan = stable(out_plan)?;
            let in_plan = stable(in_plan)?;
            let unioned = LogicalPlanBuilder::from(out_plan)
                .union(in_plan)
                .and_then(LogicalPlanBuilder::build)
                .map_unsupported_expr()?;
            let projections: Vec<DfExpr> = leg_schema
                .iter()
                .enumerate()
                .map(|(i, (q, f))| {
                    col(format!("__u{i}")).alias_qualified(q.cloned(), f.name().as_str())
                })
                .collect();
            LogicalPlanBuilder::from(unioned)
                .project(projections)
                .and_then(LogicalPlanBuilder::build)
                .map_unsupported_expr()
        }
    }
}

/// Emit the provider-backed [`graphforge_plan::ExpandNode`] for a project-backed fixed
/// hop (#1248).
///
/// The provider, not lowering, owns hit/miss/building fallback. Schema-only
/// lowering and already-bound relationship variables keep the relational path;
/// the latter is already a cheap row-local constraint rather than a graph
/// expansion. A repeated destination is expanded under a private binding and
/// filtered back to the existing node, preserving cyclic-pattern semantics.
#[allow(clippy::too_many_arguments)]
#[allow(
    clippy::too_many_lines,
    reason = "schema discovery, repeated-destination correlation, and extension shaping are one lowering operation"
)]
fn try_lower_provider_expand(
    src: VarId,
    edge: VarId,
    dst: VarId,
    rel_ty: Option<RelationTypeId>,
    dir: Direction,
    input: &LogicalPlan,
    var_map: &mut VarMap,
    type_id_to_rel_name: &HashMap<RelationTypeId, String>,
    target: Option<(&LoweringSnapshot, OntologyMode)>,
) -> Result<Option<LogicalPlan>, LoweringError> {
    let Some((dir_path, mode)) = target else {
        return Ok(None); // schema-only lowering has no execution provider
    };
    if var_map.get(edge).is_some() {
        return Ok(None); // already-bound relationship: row-local filter path
    }
    let rel_name = match rel_ty {
        Some(rt) => {
            let Some(name) = type_id_to_rel_name.get(&rt) else {
                return Ok(None); // relational path reports the unknown TypeId
            };
            name.clone()
        }
        None => "*".to_owned(),
    };

    let bound_dst = var_map.get(dst).map(str::to_owned);
    let traversal_dst = bound_dst
        .as_ref()
        .map_or(dst, |_| VarId(u32::MAX.saturating_sub(dst.0)));

    // Edge property fields, discovered exactly like `join_edge_properties`:
    // wildcard traversal unions every relation's fields, first concrete type wins.
    let edge_schema = if rel_ty.is_none() || matches!(mode, OntologyMode::Exploratory) {
        &*EXPLORATORY_EDGE_SCHEMA
    } else {
        &*TYPED_EDGE_SCHEMA
    };
    let edge_fields: Vec<Arc<datafusion::arrow::datatypes::Field>> =
        edge_schema.fields().iter().cloned().collect();
    let base_names: HashSet<&str> = edge_fields.iter().map(|f| f.name().as_str()).collect();
    let stems = edge_property_candidate_stems(dir_path, &rel_name);
    let mut positions = std::collections::HashMap::<String, usize>::new();
    let mut edge_prop_fields: Vec<Arc<datafusion::arrow::datatypes::Field>> = Vec::new();
    for stem in stems {
        let prop_table = edge_property_schema(dir_path, &stem);
        for field in prop_table.fields() {
            if field.name() != "edge_uuid" && !base_names.contains(field.name().as_str()) {
                if let Some(&position) = positions.get(field.name()) {
                    if edge_prop_fields[position].data_type()
                        == &datafusion::arrow::datatypes::DataType::Null
                    {
                        edge_prop_fields[position] = Arc::clone(field);
                    }
                } else {
                    positions.insert(field.name().clone(), edge_prop_fields.len());
                    edge_prop_fields.push(Arc::clone(field));
                }
            }
        }
    }
    let dst_fields: Vec<Arc<datafusion::arrow::datatypes::Field>> =
        TOPOLOGY_NODES_SCHEMA.fields().iter().cloned().collect();

    let node = graphforge_plan::ExpandNode::new(
        Arc::new(input.clone()),
        rel_name.clone(),
        src.0,
        traversal_dst.0,
        edge.0,
        dir,
        rel_ty,
        edge_fields,
        edge_prop_fields,
        dst_fields,
    );
    // Register new bindings exactly like the join path. A repeated destination
    // keeps its existing registration and is correlated below.
    var_map.insert(edge, var_alias(edge));
    if bound_dst.is_none() {
        var_map.insert(dst, var_alias(dst));
    }

    let mut base = LogicalPlan::Extension(datafusion::logical_expr::Extension {
        node: Arc::new(node),
    });
    if rel_ty.is_some() && !matches!(mode, OntologyMode::Exploratory) {
        base = project_typed_edge_route(base, &var_alias(edge), &rel_name)?;
    }
    if let Some(bound_dst) = bound_dst {
        use datafusion::logical_expr::col;

        let traversal_alias = var_alias(traversal_dst);
        base = LogicalPlanBuilder::from(base)
            .filter(
                col(format!("{traversal_alias}.node_uuid"))
                    .eq(col(format!("{bound_dst}.node_uuid"))),
            )
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;
        let traversal_ref = datafusion::common::TableReference::bare(traversal_alias);
        let projection = base
            .schema()
            .iter()
            .filter(|(qualifier, _)| {
                qualifier
                    .as_ref()
                    .is_none_or(|qualifier| **qualifier != traversal_ref)
            })
            .map(|(qualifier, field)| {
                DfExpr::Column(datafusion::common::Column::new(
                    qualifier.cloned(),
                    field.name(),
                ))
            })
            .collect::<Vec<_>>();
        base = LogicalPlanBuilder::from(base)
            .project(projection)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;
    }
    Ok(Some(base))
}

/// Lower a single-direction (Out or In) fixed-hop expand.
///
/// `out_direction = true`  → join on `src.node_id = edge.src_id` (Out)
/// `out_direction = false` → join on `src.node_id = edge.dst_id` (In)
#[allow(clippy::too_many_arguments)]
fn expand_single_dir(
    _src: VarId,
    edge: VarId,
    dst: VarId,
    rel_ty: Option<RelationTypeId>,
    src_alias: &str,
    out_direction: bool,
    input: LogicalPlan,
    var_map: &mut VarMap,
    catalog: Option<&LoweringSnapshot>,
    type_id_to_rel_name: &HashMap<RelationTypeId, String>,
    target: Option<(&LoweringSnapshot, OntologyMode)>,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::logical_expr::col;

    let bound_dst = var_map.get(dst).and_then(|alias| {
        let qualifier = datafusion::common::TableReference::bare(alias);
        if input
            .schema()
            .index_of_column_by_name(Some(&qualifier), "node_id")
            .is_some()
        {
            Some((alias.to_owned(), "node_id", false))
        } else {
            input
                .schema()
                .index_of_column_by_name(Some(&qualifier), "node_uuid")
                .is_some()
                .then(|| (alias.to_owned(), "node_uuid", true))
        }
    });
    let dir = target.map(|(d, _)| d);
    let mode = target.map_or(OntologyMode::Exploratory, |(_, m)| m);
    if let Some(edge_alias) = var_map.get(edge).map(str::to_owned) {
        return expand_bound_edge_single_dir(
            dst,
            rel_ty,
            src_alias,
            &edge_alias,
            out_direction,
            input,
            var_map,
            type_id_to_rel_name,
            target,
        );
    }

    // Produce the edge scan (registers edge var in var_map).
    let edge_plan = match rel_ty {
        Some(rt) => {
            lower_typed_edge_scan(edge, rt, var_map, catalog, type_id_to_rel_name, dir, mode)?
        }
        None => lower_edge_scan(edge, None, var_map, type_id_to_rel_name, dir, mode)?,
    };
    let edge_alias = var_map
        .get(edge)
        .ok_or(LoweringError::UnboundVar(edge.0))?
        .to_owned();

    // Enrich the edge scan with its persisted properties (#784) so a downstream
    // `RETURN r.<prop>` resolves to a real column. A wildcard edge scan
    // (`rel_ty == None`) has no single relation file, so no property join.
    let edge_plan = join_edge_properties(
        &edge_alias,
        rel_ty,
        type_id_to_rel_name,
        catalog,
        dir,
        edge_plan,
    )?;

    // Join input (has src) with edge scan.
    let src_col = col(format!("{src_alias}.node_id"));
    let edge_src_col = col(if out_direction {
        format!("{edge_alias}.src_id")
    } else {
        format!("{edge_alias}.dst_id")
    });

    // Join input (has src) with edge scan using ON expression.
    let join_pred = src_col.eq(edge_src_col);
    let joined = LogicalPlanBuilder::from(input)
        .join_on(edge_plan, JoinType::Inner, vec![join_pred])
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()?;

    let edge_dst_col = col(
        match (out_direction, bound_dst.as_ref().map(|(_, _, uuid)| *uuid)) {
            (true, Some(true)) => format!("{edge_alias}.dst_uuid"),
            (false, Some(true)) => format!("{edge_alias}.src_uuid"),
            (true, _) => format!("{edge_alias}.dst_id"),
            (false, _) => format!("{edge_alias}.src_id"),
        },
    );
    // Reusing a node variable constrains this hop to the node already present
    // in the row; it must not append a second scan with the same qualifier.
    if let Some((dst_alias, dst_field, _)) = bound_dst {
        return LogicalPlanBuilder::from(joined)
            .filter(edge_dst_col.eq(col(format!("{dst_alias}.{dst_field}"))))
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr();
    }

    // Produce the dst node scan.
    let dst_plan = lower_node_scan(dst, None, var_map, dir, None)?;
    let dst_alias = var_map
        .get(dst)
        .ok_or(LoweringError::UnboundVar(dst.0))?
        .to_owned();

    let dst_col = col(format!("{dst_alias}.node_id"));

    let join_pred2 = edge_dst_col.eq(dst_col);
    LogicalPlanBuilder::from(joined)
        .join_on(dst_plan, JoinType::Inner, vec![join_pred2])
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn expand_bound_edge_single_dir(
    dst: VarId,
    rel_ty: Option<RelationTypeId>,
    src_alias: &str,
    edge_alias: &str,
    out_direction: bool,
    input: LogicalPlan,
    var_map: &mut VarMap,
    type_id_to_rel_name: &HashMap<RelationTypeId, String>,
    target: Option<(&LoweringSnapshot, OntologyMode)>,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::common::TableReference;
    use datafusion::logical_expr::{col, lit};

    let edge_src_field = if out_direction { "src_id" } else { "dst_id" };
    let edge_dst_field = if out_direction { "dst_id" } else { "src_id" };
    let mut predicate =
        col(format!("{src_alias}.node_id")).eq(col(format!("{edge_alias}.{edge_src_field}")));

    if let Some(rt) = rel_ty {
        let rel_name = type_id_to_rel_name.get(&rt).ok_or_else(|| {
            LoweringError::UnsupportedExpr(format!(
                "bound edge TypeId({}) has no known relation name; ontology may be incomplete or stale",
                rt.encode()
            ))
        })?;
        let qual = TableReference::bare(edge_alias);
        if input
            .schema()
            .index_of_column_by_name(Some(&qual), "rel_type_name")
            .is_some()
        {
            predicate = predicate
                .and(col(format!("{edge_alias}.rel_type_name")).eq(lit(rel_name.as_str())));
        }
    }

    let filtered = LogicalPlanBuilder::from(input)
        .filter(predicate)
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()?;

    let edge_dst = col(format!("{edge_alias}.{edge_dst_field}"));
    if let Some(dst_alias) = var_map.get(dst).map(str::to_owned) {
        return LogicalPlanBuilder::from(filtered)
            .filter(edge_dst.eq(col(format!("{dst_alias}.node_id"))))
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr();
    }

    let dir = target.map(|(d, _)| d);
    let dst_plan = lower_node_scan(dst, None, var_map, dir, None)?;
    let dst_alias = var_map
        .get(dst)
        .ok_or(LoweringError::UnboundVar(dst.0))?
        .to_owned();
    LogicalPlanBuilder::from(filtered)
        .join_on(
            dst_plan,
            JoinType::Inner,
            vec![edge_dst.eq(col(format!("{dst_alias}.node_id")))],
        )
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()
}

#[cfg(test)]
mod tests;
