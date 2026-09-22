//! Scans lowering.

use super::{
    Arc, DfExpr, EXPLORATORY_EDGE_SCHEMA, EntityTypeId, GraphPlanLowerer, HashMap, JoinType,
    LogicalPlan, LogicalPlanBuilder, LoweringError, LoweringSnapshot, MapUnsupportedExpr,
    OntologyMode, RecordBatch, RelationTypeId, TOPOLOGY_NODES_SCHEMA, TYPED_EDGE_SCHEMA, VarId,
    VarMap, var_alias,
};

impl GraphPlanLowerer {
    /// The property-file stem for a node of type `ty` — the table
    /// `join_node_properties` reads `var_N.<prop>` from, and `node_prop_cols`
    /// reads the column names from.
    ///
    /// - labelled: the ontology entity name, or `_untyped` when the label is only
    ///   known to the runtime catalog (exploratory mode);
    /// - unlabelled in **exploratory** mode: `_untyped`, where all exploratory
    ///   properties are written — so `MATCH (n) RETURN n` carries its props (#889);
    /// - unlabelled in an **ontology** mode: `None` — properties are spread across
    ///   per-entity tables with no single stem (a multi-table union; out of scope).
    fn prop_table_stem(&self, ty: Option<EntityTypeId>) -> Option<String> {
        match ty {
            Some(type_id) => Some(
                self.type_id_to_entity_name
                    .get(&type_id)
                    .cloned()
                    .unwrap_or_else(|| "_untyped".to_owned()),
            ),
            None if matches!(self.read_mode(), OntologyMode::Exploratory) => {
                Some("_untyped".to_owned())
            }
            None => None,
        }
    }

    /// The persisted property column names a node of type `ty` carries — the
    /// columns `join_node_properties` materializes as `var_N.<name>`. Empty in
    /// schema-only lowering or when no single property table applies (see
    /// [`prop_table_stem`](Self::prop_table_stem)).
    pub(super) fn node_prop_cols(&self, ty: Option<EntityTypeId>) -> Vec<String> {
        if let Some(columns) = self.semantic_node_property_columns() {
            return columns;
        }
        let Some(dir) = self.read_snapshot() else {
            return Vec::new();
        };
        let Some(stem) = self.prop_table_stem(ty) else {
            return Vec::new();
        };
        let prop_table = node_property_schema(dir, &stem);
        prop_table
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| TOPOLOGY_NODES_SCHEMA.field_with_name(n).is_err())
            .collect()
    }

    /// LEFT-join the property table for a labelled node `var` onto `scan`, so a
    /// later `PropertyAccess` (`var_N.<prop>`) resolves to a real column (#704).
    ///
    /// The property values live in `properties/<Entity>.parquet` (strict /
    /// advisory) or `properties/_untyped.parquet` (exploratory), keyed by
    /// `node_uuid`. We join on `var_N.node_uuid = <props>.node_uuid`, pass **every
    /// column already in `scan` through unchanged**, then append each property
    /// column (except the duplicate `node_uuid` key) **re-qualified** under
    /// `var_N` so it resolves as `var_N.<prop>`.
    ///
    /// Preserving all of `scan`'s existing columns (rather than just the joined
    /// node's topology columns) is what lets this run on **both** a fresh
    /// single-var node scan *and* an already-joined multi-var plan — e.g. the
    /// destination of a fixed single-hop `Expand`, whose plan carries the source
    /// and edge columns too (#789). On a fresh single-var scan the preserved set
    /// is exactly that node's topology columns, so the result is unchanged.
    ///
    /// No-ops (returns `scan` unchanged) when:
    /// - there is no project directory (schema-only explain/golden lowering),
    /// - no single property table applies — an unlabelled node in an ontology
    ///   mode, whose properties are spread across per-entity tables (see
    ///   [`prop_table_stem`](Self::prop_table_stem); an unlabelled node in
    ///   *exploratory* mode resolves to `_untyped` and DOES join), or
    /// - the property table has no columns beyond `node_uuid`.
    ///
    /// A LEFT join preserves nodes that have no property row written yet.
    pub(super) fn join_node_properties(
        &self,
        var: VarId,
        ty: Option<EntityTypeId>,
        scan: LogicalPlan,
    ) -> Result<LogicalPlan, LoweringError> {
        use datafusion::common::Column;
        use datafusion::logical_expr::col;

        if self.semantic_node_property_columns().is_some() {
            return self.join_semantic_node_properties(var, ty, scan);
        }

        let Some(dir) = self.read_snapshot() else {
            return Ok(scan); // schema-only lowering: no real provider to join
        };
        let Some(stem) = self.prop_table_stem(ty) else {
            return Ok(scan); // no single property table applies (see prop_table_stem)
        };

        let prop_table = node_property_schema(dir, &stem);
        let prop_schema = prop_table;
        let node_alias = var_alias(var);

        // Property columns already present under this var's qualifier. A var whose
        // properties were joined upstream and then re-matched — e.g. forwarded
        // through `WITH a` and matched again in `MATCH (a)-[…]->(b)` — must NOT
        // have them joined a second time: that re-qualifies a second `var_N.<prop>`
        // and a later `RETURN *` projects two columns of the same name, which
        // DataFusion rejects ("Projections require unique expression names").
        let existing: std::collections::HashSet<String> = scan
            .schema()
            .iter()
            .filter(|(q, _)| q.is_some_and(|t| t.table() == node_alias.as_str()))
            .map(|(_, f)| f.name().clone())
            .collect();

        // The non-key property columns to ADD. Exclude every node-topology column
        // name (not just the `node_uuid` join key): a property that happens to
        // share a topology column name — `node_id`, `type_id`, `created_at`,
        // `updated_at` — would re-qualify to a second `var_N.<name>` field and
        // build a duplicate-qualified schema DataFusion rejects. Dropping the
        // collision keeps the topology column authoritative. Also exclude any
        // property already present under this var (idempotent re-join, above). If
        // nothing remains, the join would add nothing — skip it.
        let prop_cols: Vec<String> = prop_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| TOPOLOGY_NODES_SCHEMA.field_with_name(n).is_err())
            .filter(|n| !existing.contains(n))
            .collect();
        if prop_cols.is_empty() {
            return Ok(scan);
        }

        let prop_alias = format!("{node_alias}__props");
        let prop_src = graphforge_plan::GraphReadSource::new(
            graphforge_plan::GraphReadTable::Properties(stem.clone()),
            &prop_schema,
            self.catalog
                .as_ref()
                .and_then(LoweringSnapshot::semantic_composition_fingerprint)
                .map(str::to_owned),
        );
        let prop_scan = LogicalPlanBuilder::scan(prop_alias.clone(), prop_src, None)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;

        // Snapshot the input's existing qualified columns BEFORE the join, so the
        // projection preserves exactly what `scan` carried (one var for a fresh
        // node scan; src + edge + dst for an already-joined Expand plan) — and
        // never accidentally re-projects the joined-in `<prop>` columns by their
        // bare names.
        let input_cols: Vec<Column> = scan
            .schema()
            .iter()
            .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
            .collect();

        // LEFT join: node ⟕ props ON node.node_uuid = props.node_uuid.
        let join_pred =
            col(format!("{node_alias}.node_uuid")).eq(col(format!("{prop_alias}.node_uuid")));
        let joined = LogicalPlanBuilder::from(scan)
            .join_on(prop_scan, JoinType::Left, vec![join_pred])
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;

        // Project: every pre-join input column through unchanged, then each
        // property column re-qualified under var_N so `var_N.<prop>` resolves.
        let mut projections: Vec<DfExpr> = input_cols.into_iter().map(DfExpr::Column).collect();
        for name in &prop_cols {
            projections.push(
                crate::expr::qualified_col(&prop_alias, name)
                    .alias_qualified(Some(node_alias.as_str()), name.as_str()),
            );
        }

        LogicalPlanBuilder::from(joined)
            .project(projections)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()
    }
}

/// Wrap a schema in a [`LogicalTableSource`] suitable for
/// [`LogicalPlanBuilder::scan`].
pub(super) fn table_source(
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> Arc<datafusion::logical_expr::logical_plan::LogicalTableSource> {
    Arc::new(datafusion::logical_expr::logical_plan::LogicalTableSource::new(schema))
}

/// The data source for a node scan.
///
/// Discover the admitted project's schema when available, then retain only a
/// logical descriptor. Execution resolves its provider from the session.
fn node_scan_source(
    dir: Option<&LoweringSnapshot>,
) -> Result<Arc<dyn datafusion::logical_expr::TableSource>, LoweringError> {
    let schema = match dir {
        Some(d) => d.node_schema.clone().ok_or_else(|| {
            LoweringError::UnsupportedExpr("dataset snapshot has no node schema".into())
        })?,
        // Schema-only plans retain their existing non-executable placeholder
        // and optimizer/explain contract. Admitted reads use descriptors below.
        None => return Ok(table_source(TOPOLOGY_NODES_SCHEMA.clone())),
    };
    Ok(graphforge_plan::GraphReadSource::new(
        graphforge_plan::GraphReadTable::Nodes,
        &schema,
        None,
    ))
}

/// Logical edge source over a relation stem or the wildcard `_exploratory`.
/// Execution selects the typed union or shared exploratory file from its mode.
fn edge_scan_source(
    dir: Option<&LoweringSnapshot>,
    stem: &str,
    schema: &datafusion::arrow::datatypes::SchemaRef,
    mode: OntologyMode,
) -> Arc<dyn datafusion::logical_expr::TableSource> {
    if dir.is_none() {
        return table_source(schema.clone());
    }
    let _ = mode; // Layout selection belongs to the execution resource.
    graphforge_plan::GraphReadSource::new(
        graphforge_plan::GraphReadTable::Edges(stem.to_owned()),
        schema,
        None,
    )
}

/// Filter an already-bound node variable by its label type.
///
/// Used when a destination node's `NodeScan` is a no-op (the var was bound by a
/// preceding `Expand`) but still carries a label: the label predicate is applied
/// against the already-present `<alias>.type_id` column.
pub(super) fn filter_node_by_type(
    input: LogicalPlan,
    alias: &str,
    type_id: EntityTypeId,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::functions_nested::expr_fn::array_has;
    use datafusion::logical_expr::{col, lit};
    LogicalPlanBuilder::from(input)
        .filter(array_has(
            col(format!("{alias}.type_ids")),
            lit(type_id.encode()),
        ))
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()
}

pub(super) fn enrich_bound_node_identity(
    input: &LogicalPlan,
    alias: &str,
    dir: Option<&LoweringSnapshot>,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::common::Column;
    use datafusion::logical_expr::col;

    let identity_alias = format!("__gf_identity_{alias}");
    let identity = LogicalPlanBuilder::scan(identity_alias.clone(), node_scan_source(dir)?, None)
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()?;
    let joined = LogicalPlanBuilder::from(input.clone())
        .join(
            identity,
            datafusion::logical_expr::JoinType::Inner,
            (
                vec![Column::from_qualified_name(format!("{alias}.node_uuid"))],
                vec![Column::from_qualified_name(format!(
                    "{identity_alias}.node_uuid"
                ))],
            ),
            None,
        )
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()?;
    let mut projection = input
        .schema()
        .iter()
        .map(|(qualifier, field)| DfExpr::Column(Column::new(qualifier.cloned(), field.name())))
        .collect::<Vec<_>>();
    for name in ["node_id", "type_id", "type_ids"] {
        projection.push(col(format!("{identity_alias}.{name}")).alias_qualified(Some(alias), name));
    }
    LogicalPlanBuilder::from(joined)
        .project(projection)
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()
}

pub(super) fn lower_node_scan(
    var: VarId,
    ty: Option<EntityTypeId>,
    var_map: &mut VarMap,
    dir: Option<&LoweringSnapshot>,
    pending_nodes: Option<&RecordBatch>,
) -> Result<LogicalPlan, LoweringError> {
    let alias = var_alias(var);
    var_map.insert(var, alias.clone());

    let mut builder = LogicalPlanBuilder::scan(alias.clone(), node_scan_source(dir)?, None)
        .map_unsupported_expr()?;
    if let Some(batch) = pending_nodes.filter(|batch| batch.num_rows() > 0) {
        use datafusion::datasource::{MemTable, provider_as_source};
        let table =
            MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).map_unsupported_expr()?;
        let pending =
            LogicalPlanBuilder::scan(alias.clone(), provider_as_source(Arc::new(table)), None)
                .and_then(LogicalPlanBuilder::build)
                .map_unsupported_expr()?;
        builder = builder
            .union(pending)
            .and_then(|builder| builder.alias(alias.clone()))
            .map_unsupported_expr()?;
    }
    if let Some(type_id) = ty {
        use datafusion::functions_nested::expr_fn::array_has;
        use datafusion::logical_expr::{col, lit};
        builder = builder
            .filter(array_has(
                col(format!("{alias}.type_ids")),
                lit(type_id.encode()),
            ))
            .map_unsupported_expr()?;
    }

    builder.build().map_unsupported_expr()
}

/// Carry the catalog-resolved physical route into the write frontier. Typed
/// topology omits this constant on disk; mutation ownership still needs it.
pub(super) fn project_typed_edge_route(
    scan: LogicalPlan,
    alias: &str,
    route: &str,
) -> Result<LogicalPlan, LoweringError> {
    let mut columns: Vec<_> = scan
        .schema()
        .columns()
        .into_iter()
        .map(DfExpr::Column)
        .collect();
    columns
        .push(datafusion::logical_expr::lit(route).alias_qualified(Some(alias), "rel_type_name"));
    LogicalPlanBuilder::from(scan)
        .project(columns)
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()
}

pub(super) fn lower_typed_edge_scan(
    var: VarId,
    rel_ty: RelationTypeId,
    var_map: &mut VarMap,
    catalog: Option<&LoweringSnapshot>,
    type_id_to_rel_name: &HashMap<RelationTypeId, String>,
    dir: Option<&LoweringSnapshot>,
    mode: OntologyMode,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::logical_expr::{col, lit};

    let alias = var_alias(var);
    var_map.insert(var, alias.clone());

    // Require a known relation name — silently falling back to _exploratory
    // would change query semantics (wrong table / over-scan).
    let rel_name = type_id_to_rel_name.get(&rel_ty).ok_or_else(|| {
        LoweringError::UnsupportedExpr(format!(
            "TypedEdgeScan: TypeId({}) has no known relation name; \
             ontology may be incomplete or stale",
            rel_ty.encode()
        ))
    })?;

    // Generation-bound semantic relations must consume the exact provider
    // authenticated and registered by LoweringSnapshot. Reconstructing a provider
    // from a string route loses that authority and must never fall back to the
    // exploratory relation-name filter.
    if catalog.is_some_and(|catalog| catalog.semantic_rel_routes().contains_key(&rel_ty)) {
        let provider = catalog
            .and_then(|catalog| catalog.semantic_edge_schema(rel_ty))
            .ok_or_else(|| {
                LoweringError::UnsupportedExpr(format!(
                    "semantic relation TypeId({}) has no authenticated catalog provider",
                    rel_ty.encode()
                ))
            })?;
        let scan = LogicalPlanBuilder::scan(
            alias.clone(),
            graphforge_plan::GraphReadSource::new(
                graphforge_plan::GraphReadTable::SemanticEdges(rel_ty),
                &provider,
                catalog
                    .and_then(LoweringSnapshot::semantic_composition_fingerprint)
                    .map(str::to_owned),
            ),
            None,
        )
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()?;
        return project_typed_edge_route(scan, &alias, rel_name);
    }

    // Check if the catalog has a typed edge table for this relation.
    let use_exploratory =
        catalog.is_none_or(|c| !c.typed_edge_tables.contains(&format!("edges_{rel_name}")));

    if use_exploratory {
        let src = edge_scan_source(dir, "_exploratory", &EXPLORATORY_EDGE_SCHEMA, mode);
        let filter_expr = col("rel_type_name").eq(lit(rel_name.as_str()));
        // Use alias as the scan qualifier so var_map column refs resolve correctly.
        LogicalPlanBuilder::scan(alias, src, None)
            .and_then(|b| b.filter(filter_expr))
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()
    } else {
        let src = edge_scan_source(dir, rel_name, &TYPED_EDGE_SCHEMA, mode);
        // Use alias as the scan qualifier so downstream join predicates
        // (var_map.get(edge) → "var_N") can resolve edge columns correctly.
        let scan = LogicalPlanBuilder::scan(alias.clone(), src, None)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;
        project_typed_edge_route(scan, &alias, rel_name)
    }
}

pub(super) fn lower_edge_scan(
    var: VarId,
    ty: Option<RelationTypeId>,
    var_map: &mut VarMap,
    type_id_to_rel_name: &HashMap<RelationTypeId, String>,
    dir: Option<&LoweringSnapshot>,
    mode: OntologyMode,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::logical_expr::{col, lit};

    let alias = var_alias(var);
    var_map.insert(var, alias.clone());

    let src = edge_scan_source(dir, "_exploratory", &EXPLORATORY_EDGE_SCHEMA, mode);
    let mut builder = LogicalPlanBuilder::scan(alias, src, None).map_unsupported_expr()?;

    if let Some(type_id) = ty
        && let Some(name) = type_id_to_rel_name.get(&type_id)
    {
        builder = builder
            .filter(col("rel_type_name").eq(lit(name.as_str())))
            .map_unsupported_expr()?;
    }

    builder.build().map_unsupported_expr()
}

fn edge_property_read_source(
    stem: &str,
    rel_ty: Option<RelationTypeId>,
    catalog: Option<&LoweringSnapshot>,
    schema: &datafusion::arrow::datatypes::SchemaRef,
) -> Arc<graphforge_plan::GraphReadSource> {
    let semantic_id =
        rel_ty.filter(|id| catalog.is_some_and(|c| c.semantic_edge_property_schema(*id).is_some()));
    let composition = catalog
        .and_then(LoweringSnapshot::semantic_composition_fingerprint)
        .map(str::to_owned);
    let table = graphforge_plan::GraphReadTable::EdgeProperties(stem.to_owned(), semantic_id);
    graphforge_plan::GraphReadSource::new(table, schema, composition)
}

/// LEFT-join an edge scan with its persisted properties (#784), the edge
/// analogue of [`GraphPlanLowerer::join_node_properties`].
///
/// Returns `scan` unchanged when:
/// - there is no read directory (schema-only lowering),
/// - the edge-property table has no columns beyond the `edge_uuid` join key.
///
/// Otherwise it reads selected property owners, LEFT-joins on `edge_uuid`
/// (preserving edges with no property row yet), and re-qualifies each property
/// column under the edge var alias so `var_<edge>.<prop>` resolves. The base
/// topology columns are read from the edge `scan`'s own schema (typed and
/// exploratory edge files differ in width), keeping the projection in lock-step
/// with whichever edge file the scan reads.
pub(super) fn join_edge_properties(
    edge_alias: &str,
    rel_ty: Option<RelationTypeId>,
    type_id_to_rel_name: &HashMap<RelationTypeId, String>,
    catalog: Option<&LoweringSnapshot>,
    dir: Option<&LoweringSnapshot>,
    scan: LogicalPlan,
) -> Result<LogicalPlan, LoweringError> {
    use datafusion::logical_expr::{col, lit};
    use std::collections::{HashMap as StdHashMap, HashSet};

    let Some(dir) = dir else {
        return Ok(scan); // schema-only lowering: no real provider to join
    };

    // Base topology columns come from the edge scan's own schema (typed: 9 cols;
    // exploratory: + `rel_type_name`), so the projection matches the live file.
    let base_cols: Vec<String> = scan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let mut prop_sources = Vec::new();
    let mut prop_order = Vec::new();
    let mut seen_props = HashSet::new();
    let mut push_source =
        |stem: String, registered: Option<datafusion::arrow::datatypes::SchemaRef>| {
            let table = registered.unwrap_or_else(|| edge_property_schema(dir, &stem));
            let prop_cols: Vec<String> = table
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .filter(|n| !base_cols.contains(n))
                .collect();
            if prop_cols.is_empty() {
                return;
            }
            for name in &prop_cols {
                if seen_props.insert(name.clone()) {
                    prop_order.push(name.clone());
                }
            }
            prop_sources.push((stem, table, prop_cols));
        };
    if let Some(rel_ty) = rel_ty {
        let Some(rel_name) = type_id_to_rel_name.get(&rel_ty) else {
            return Ok(scan); // unknown relation name: nothing to resolve
        };
        let registered = catalog.and_then(|catalog| catalog.semantic_edge_property_schema(rel_ty));
        push_source(rel_name.clone(), registered);
        if rel_name != "_exploratory" && dir.edge_properties.contains_key("_exploratory") {
            push_source("_exploratory".into(), None);
        }
    } else {
        for stem in dir.edge_property_stems.clone() {
            push_source(stem, None);
        }
    }
    if prop_sources.is_empty() {
        return Ok(scan);
    }
    validate_edge_property_owner_types(&prop_sources)?;

    let wildcard = rel_ty.is_none();
    let mut joined = scan;
    let mut prop_refs: StdHashMap<String, Vec<DfExpr>> = StdHashMap::new();
    for (idx, (stem, prop_table, prop_cols)) in prop_sources.into_iter().enumerate() {
        let prop_alias = format!("{edge_alias}__eprops_{idx}");
        let source_type = if stem == "_exploratory" { None } else { rel_ty };
        let prop_src = edge_property_read_source(&stem, source_type, catalog, &prop_table);
        let prop_scan = LogicalPlanBuilder::scan(prop_alias.clone(), prop_src, None)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;

        // LEFT join: edge ⟕ props ON edge.edge_uuid = props.edge_uuid. For a
        // wildcard edge scan, constrain each property table to its relation so a
        // relation-specific property file never contributes to another relation.
        let mut join_pred =
            col(format!("{edge_alias}.edge_uuid")).eq(col(format!("{prop_alias}.edge_uuid")));
        if wildcard && stem != "_exploratory" {
            join_pred = join_pred.and(col(format!("{edge_alias}.rel_type_name")).eq(lit(stem)));
        }
        joined = LogicalPlanBuilder::from(joined)
            .join_on(prop_scan, JoinType::Left, vec![join_pred])
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()?;
        for name in prop_cols {
            prop_refs
                .entry(name.clone())
                .or_default()
                .push(crate::expr::qualified_col(&prop_alias, &name));
        }
    }

    // Project: all edge topology columns (qualified var_N) unchanged, then each
    // property column re-qualified under var_N so `var_N.<prop>` resolves.
    let mut projections: Vec<DfExpr> = base_cols
        .iter()
        .map(|name| col(format!("{edge_alias}.{name}")))
        .collect();
    for name in prop_order {
        let mut refs = prop_refs.remove(&name).unwrap_or_default();
        let value = if refs.len() == 1 {
            refs.remove(0)
        } else {
            datafusion::functions::core::expr_fn::coalesce(refs)
        };
        projections.push(value.alias_qualified(Some(edge_alias), name.as_str()));
    }

    LogicalPlanBuilder::from(joined)
        .project(projections)
        .and_then(LogicalPlanBuilder::build)
        .map_unsupported_expr()
}

fn node_property_schema(
    snapshot: &LoweringSnapshot,
    stem: &str,
) -> datafusion::arrow::datatypes::SchemaRef {
    snapshot
        .node_properties
        .get(stem)
        .cloned()
        .unwrap_or_else(|| graphforge_ir::arrow_schema::PROPERTY_BASE_SCHEMA.clone())
}

fn validate_edge_property_owner_types(
    prop_sources: &[(String, datafusion::arrow::datatypes::SchemaRef, Vec<String>)],
) -> Result<(), LoweringError> {
    // The relational reference must not silently coerce two concrete owner
    // schemas where provider hydration would reject them. Null is absence.
    let mut property_types = HashMap::new();
    for (_, schema, names) in prop_sources {
        for name in names {
            let datatype = schema
                .field_with_name(name)
                .map_unsupported_expr()?
                .data_type();
            if datatype == &datafusion::arrow::datatypes::DataType::Null {
                continue;
            }
            if let Some(prior) = property_types.insert(name, datatype)
                && prior != datatype
            {
                return Err(LoweringError::UnsupportedExpr(
                    "incompatible concrete edge property owner types".into(),
                ));
            }
        }
    }

    Ok(())
}

// A named relation can have ordinary per-name properties and constructed
// exploratory properties. These are current physical owners, not aliases.
pub(super) fn edge_property_candidate_stems(
    snapshot: &LoweringSnapshot,
    relation: &str,
) -> Vec<String> {
    let mut stems = if relation == "*" {
        snapshot.edge_property_stems.clone()
    } else {
        let mut candidates = vec![relation.to_owned()];
        if relation != "_exploratory" && snapshot.edge_properties.contains_key("_exploratory") {
            candidates.push("_exploratory".to_owned());
        }
        candidates
    };
    stems.sort();
    stems.dedup();
    stems
}

pub(super) fn edge_property_schema(
    snapshot: &LoweringSnapshot,
    stem: &str,
) -> datafusion::arrow::datatypes::SchemaRef {
    snapshot
        .edge_properties
        .get(stem)
        .cloned()
        .unwrap_or_else(|| graphforge_ir::arrow_schema::EDGE_PROPERTY_BASE_SCHEMA.clone())
}

#[cfg(test)]
mod tests;
