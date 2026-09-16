//! Graph values, metadata, and neutral path hydration descriptors.

use super::{
    ExprId, ExprLowerer, IrExpr, IrLiteral, LoweringError, VarId, col_literal, decode_het,
    qualified_col, validate_heterogeneous_arguments,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, Expr as DfExpr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, cast, col, lit, when,
};
use datafusion::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::LazyLock;

impl ExprLowerer<'_> {
    pub(super) fn lower_value(&self, id: ExprId) -> Result<DfExpr, LoweringError> {
        if let IrExpr::VarRef(var_id) = self.arena.get(id) {
            let base = self
                .var_map
                .get(*var_id)
                .ok_or(LoweringError::UnboundVar(var_id.0))?;
            if self.node_shapes.contains_key(&var_id.0) || self.is_node_var(base) {
                let prop_names = self
                    .node_shapes
                    .get(&var_id.0)
                    .map(|s| s.prop_names.clone())
                    .unwrap_or_default();
                return Ok(node_value_struct(
                    base,
                    None,
                    &self.type_id_to_entity_name,
                    &prop_names,
                ));
            }
            if self.is_edge_var(base) {
                let props = self.edge_prop_names(base);
                let value =
                    relationship_value_struct(base, col(format!("{base}.rel_type_name")), &props);
                return Ok(null_unless(edge_present_qual(base), value));
            }
        }
        self.lower(id)
    }

    pub(super) fn lower_path_builtin_arg(&self, id: ExprId) -> Result<DfExpr, LoweringError> {
        if let IrExpr::VarRef(v) = self.arena.get(id) {
            return Ok(col_literal(
                self.var_map.get(*v).ok_or(LoweringError::UnboundVar(v.0))?,
            ));
        }
        self.lower(id)
    }

    /// Lower a `_node_struct(VarRef)` call (emitted by the binder for a bare
    /// `RETURN n`, #785) into a whole node value — `Struct{node_uuid, labels,
    /// <props…>}`. The node's shape (label + property columns) comes from the
    /// `node_shapes` map this lowerer was seeded with; an absent shape yields a
    /// uuid-only struct.
    pub(super) fn lower_node_struct(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        // args[0] = the node `VarRef`; args[1] (optional) = a `Str` literal label
        // captured from the bind-time pattern.
        let Some(&base_id) = args.first() else {
            return Err(LoweringError::UnsupportedExpr(
                "_node_struct expects at least one argument".into(),
            ));
        };
        let IrExpr::VarRef(var_id) = self.arena.get(base_id) else {
            return Err(LoweringError::UnsupportedExpr(
                "_node_struct argument must be a node variable".into(),
            ));
        };
        let base = self
            .var_map
            .get(*var_id)
            .ok_or(LoweringError::UnboundVar(var_id.0))?;
        let label = args.get(1).and_then(|&id| match self.arena.get(id) {
            IrExpr::Literal(IrLiteral::Str(s)) => Some(s.as_str()),
            _ => None,
        });
        let prop_names = self
            .node_shapes
            .get(&var_id.0)
            .map(|s| s.prop_names.clone())
            .unwrap_or_default();
        let labels = self.input_schema.as_ref().and_then(|schema| {
            let qualifier = datafusion::common::TableReference::bare(base);
            schema
                .index_of_column_by_name(Some(&qualifier), "labels")
                .is_some()
                .then(|| qualified_col(base, "labels"))
        });
        Ok(labels.map_or_else(
            || node_value_struct(base, label, &self.type_id_to_entity_name, &prop_names),
            |labels| node_value_struct_with_labels(base, labels, &prop_names),
        ))
    }

    pub(super) fn lower_node_struct_list(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        use datafusion::functions_nested::expr_fn::make_array;

        let [first, second, edge] = args else {
            return Err(LoweringError::UnsupportedExpr(
                "_node_struct_list expects two nodes and one relationship".into(),
            ));
        };
        let node_vars = [first, second].map(|id| match self.arena.get(*id) {
            IrExpr::VarRef(var) => Ok(*var),
            _ => Err(LoweringError::UnsupportedExpr(
                "_node_struct_list node arguments must be variables".into(),
            )),
        });
        let [first_var, second_var] = node_vars;
        let node_vars = [first_var?, second_var?];
        let prop_names = node_vars
            .iter()
            .filter_map(|var| self.node_shapes.get(&var.0))
            .flat_map(|shape| shape.prop_names.iter().cloned())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let nodes = node_vars
            .iter()
            .map(|var| {
                use datafusion::functions::core::expr_fn::named_struct;
                let base = self
                    .var_map
                    .get(*var)
                    .ok_or(LoweringError::UnboundVar(var.0))?;
                let mut fields = vec![
                    lit("node_uuid"),
                    qualified_col(base, "node_uuid"),
                    lit("labels"),
                    node_labels_list(base, None, &self.type_id_to_entity_name),
                ];
                for name in &prop_names {
                    fields.extend([
                        lit(name.as_str()),
                        self.path_node_property(base, name, &node_vars)?,
                    ]);
                }
                Ok(null_unless(
                    qualified_col(base, "node_uuid").is_not_null(),
                    named_struct(fields),
                ))
            })
            .collect::<Result<Vec<_>, LoweringError>>()?;
        let edge = self.lower_path_builtin_arg(*edge)?;
        let present = edge_present(&edge).ok_or_else(|| {
            LoweringError::UnsupportedExpr(
                "_node_struct_list relationship argument must be a bound edge".into(),
            )
        })?;
        Ok(null_unless(present, make_array(nodes)))
    }

    /// Fixed-hop nodes share a property shape, but SET can add a column to only
    /// one endpoint. Preserve available values and type missing values from the
    /// other endpoint rather than referencing a nonexistent qualified column.
    fn path_node_property(
        &self,
        base: &str,
        name: &str,
        nodes: &[VarId],
    ) -> Result<DfExpr, LoweringError> {
        let value = qualified_col(base, name);
        let Some(schema) = &self.input_schema else {
            return Ok(value);
        };
        let qualifier = datafusion::common::TableReference::bare(base);
        if schema
            .index_of_column_by_name(Some(&qualifier), name)
            .is_some()
        {
            return Ok(value);
        }
        let data_type = nodes
            .iter()
            .find_map(|other| {
                let other_base = self.var_map.get(*other)?;
                self.expr_data_type(&qualified_col(other_base, name))
            })
            .ok_or_else(|| {
                LoweringError::UnsupportedExpr(format!(
                    "path node property `{name}` has no available column"
                ))
            })?;
        let null = ScalarValue::try_from(&data_type)
            .map_err(|error| LoweringError::UnsupportedExpr(error.to_string()))?;
        Ok(lit(null))
    }

    pub(super) fn lower_rel_struct(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let (base, rel_type) = self.lower_rel_struct_args(args)?;
        let props = self.edge_prop_names(base);
        let value = relationship_value_struct(base, rel_type, &props);
        Ok(null_unless(edge_present_qual(base), value))
    }

    pub(super) fn lower_rel_struct_list(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        use datafusion::functions_nested::expr_fn::make_array;

        let (base, rel_type) = self.lower_rel_struct_args(args)?;
        let props = self.edge_prop_names(base);
        let value = relationship_value_struct(base, rel_type, &props);
        Ok(null_unless(
            edge_present_qual(base),
            make_array(vec![value]),
        ))
    }

    fn lower_rel_struct_args(&self, args: &[ExprId]) -> Result<(&str, DfExpr), LoweringError> {
        let Some(&base_id) = args.first() else {
            return Err(LoweringError::UnsupportedExpr(
                "_rel_struct expects an edge variable".into(),
            ));
        };
        let IrExpr::VarRef(var_id) = self.arena.get(base_id) else {
            return Err(LoweringError::UnsupportedExpr(
                "_rel_struct argument must be a relationship variable".into(),
            ));
        };
        let base = self
            .var_map
            .get(*var_id)
            .ok_or(LoweringError::UnboundVar(var_id.0))?;
        let rel_type = match args.get(1).map(|&id| (id, self.arena.get(id))) {
            Some((_, IrExpr::Literal(IrLiteral::Null))) | None => {
                col(format!("{base}.rel_type_name"))
            }
            Some((id, _)) => self.lower(id)?,
        };
        Ok((base, rel_type))
    }

    pub(super) fn edge_prop_names(&self, base: &str) -> Vec<String> {
        let Some(schema) = self.input_schema.as_ref() else {
            return Vec::new();
        };
        schema
            .iter()
            .filter_map(|(qualifier, field)| {
                let q = qualifier?;
                if q.to_string() != base
                    || is_edge_value_topology_field(field.name())
                    || matches!(field.data_type(), DataType::Null)
                {
                    return None;
                }
                Some(field.name().clone())
            })
            .collect()
    }

    /// `labels(node)` — the node's complete label set as a list, with
    /// optional/unmatched nodes and `labels(null)` propagating to null.
    pub(super) fn lower_labels(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let Some(&base_id) = args.first() else {
            return Err(LoweringError::UnsupportedExpr(
                "labels() expects one argument".into(),
            ));
        };
        match self.arena.get(base_id) {
            IrExpr::Literal(IrLiteral::Null) => Ok(null_utf8_list()),
            IrExpr::VarRef(var_id) if self.node_shapes.contains_key(&var_id.0) => {
                let base = self
                    .var_map
                    .get(*var_id)
                    .ok_or(LoweringError::UnboundVar(var_id.0))?;
                Ok(null_unless(
                    col(format!("{base}.node_uuid")).is_not_null(),
                    node_labels_list(base, None, &self.type_id_to_entity_name),
                ))
            }
            IrExpr::VarRef(var_id) => {
                let base = self
                    .var_map
                    .get(*var_id)
                    .ok_or(LoweringError::UnboundVar(var_id.0))?;
                if self.is_node_var(base) {
                    Ok(null_unless(
                        col(format!("{base}.node_uuid")).is_not_null(),
                        node_labels_list(base, None, &self.type_id_to_entity_name),
                    ))
                } else {
                    let value = self.lower(base_id)?;
                    Ok(CYPHER_LABELS.call(vec![value]))
                }
            }
            _ => {
                let value = self.lower(base_id)?;
                Ok(CYPHER_LABELS.call(vec![value]))
            }
        }
    }

    pub(super) fn is_edge_var(&self, base: &str) -> bool {
        let Some(schema) = self.input_schema.as_ref() else {
            return false;
        };
        let qual = datafusion::common::TableReference::bare(base);
        schema
            .index_of_column_by_name(Some(&qual), "edge_uuid")
            .is_some()
    }

    pub(super) fn is_node_var(&self, base: &str) -> bool {
        let Some(schema) = self.input_schema.as_ref() else {
            return false;
        };
        let qual = datafusion::common::TableReference::bare(base);
        schema
            .index_of_column_by_name(Some(&qual), "node_uuid")
            .is_some()
    }

    /// The lowering-baked context for hydrating `nodes(p)` elements (#1024):
    /// the element fields are `node_uuid`, `labels`, then the **union** of
    /// every `properties/<stem>.parquet` schema's columns (sorted stems, first
    /// occurrence of a name wins, forced nullable — a node without the column
    /// is NULL). `None` without a read target (schema-only lowering), keeping
    /// the UDF's original `node_uuid`-only shape.
    pub(super) fn path_node_hydration(&self) -> Result<Option<PathNodeHydration>, LoweringError> {
        use datafusion::arrow::datatypes::Field;
        let Some(dir) = self.read_target.as_ref() else {
            return Ok(None);
        };
        let stems = dir.node_property_stems.clone();
        let mut fields = vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
        ];
        let mut seen: std::collections::HashSet<String> =
            fields.iter().map(|f| f.name().clone()).collect();
        for stem in &stems {
            let table = dir.node_properties.get(stem).ok_or_else(|| {
                LoweringError::UnsupportedExpr(format!(
                    "lowering snapshot missing node property schema for stem {stem}"
                ))
            })?;
            for f in table.fields() {
                if f.name() == "node_uuid" || !seen.insert(f.name().clone()) {
                    continue;
                }
                fields.push(f.as_ref().clone().with_nullable(true));
            }
        }
        let mut labels_by_type: Vec<(graphforge_value::EntityTypeId, String)> = self
            .type_id_to_entity_name
            .iter()
            .map(|(id, name)| (*id, name.clone()))
            .collect();
        labels_by_type.sort_by(|(left, left_name), (right, right_name)| {
            left.encode()
                .cmp(&right.encode())
                .then_with(|| left_name.cmp(right_name))
        });
        Ok(Some(PathNodeHydration {
            labels_by_type,
            prop_stems: stems,
            fields: fields.into(),
        }))
    }
}

pub(super) fn is_path_builtin_name(name: &str) -> bool {
    matches!(name, "_path_nodes" | "_path_fixed_length" | "_path_struct")
}

/// The named-path internal builtins (#754): the binder rewrites `nodes(p)` /
/// `relationships(p)` / `length(p)` / bare `p` into these, split by whether
/// the path's single segment is variable-length (list column) or a fixed hop
/// (scalar edge/node columns).
///
/// Every form null-propagates: an unmatched `OPTIONAL MATCH` row's path is
/// Cypher `null`, so its functions must be too. The var-length forms inherit
/// this from their inputs (`array_length`/`cypher_path_nodes` of a null list
/// are null); the fixed-hop forms gate on the edge's `edge_uuid` being
/// non-null (composed values like `named_struct` would otherwise be non-null
/// even over all-null columns).
pub(super) fn resolve_path_builtin(
    name: &str,
    args: Vec<DfExpr>,
    hydration: impl FnOnce() -> Option<PathNodeHydration>,
) -> Option<DfExpr> {
    use datafusion::functions::core::expr_fn::named_struct;

    let mut a = args;
    match name {
        // `nodes(p)` over a variable-length segment: recover the traversal
        // node sequence by walking the relationship-list column from the
        // start node (`cypher_path_nodes`). With a read target, the elements
        // are hydrated with labels + properties (#1024); without one they
        // stay `node_uuid`-only.
        "_path_nodes" => {
            let seed = node_uuid_col(a.remove(0));
            let rels = a.remove(0);
            Some(match hydration() {
                Some(h) => ScalarUDF::new_from_impl(CypherPathNodes::with_hydration(h))
                    .call(vec![seed, rels]),
                None => CYPHER_PATH_NODES.call(vec![seed, rels]),
            })
        }

        // `length(p)` over a fixed single hop: exactly one relationship when
        // the hop matched. UInt64 so fixed and var-length (`array_length`)
        // agree on the output type.
        "_path_fixed_length" => {
            let present = edge_present(&a.remove(0))?;
            Some(
                when(present, lit(1_u64))
                    .otherwise(lit(ScalarValue::UInt64(None)))
                    .expect("CASE build is infallible for a single WHEN + ELSE"),
            )
        }

        // A bare path value (`RETURN p`): Struct{nodes, relationships} over
        // the already-lowered component expressions. Gated on the nodes list
        // (null exactly when the path is unmatched, for both segment kinds) —
        // a bare `named_struct` would be non-null even over null fields.
        "_path_struct" => {
            let nodes = a.remove(0);
            let rels = a.remove(0);
            let struct_expr = named_struct(vec![
                lit("nodes"),
                nodes.clone(),
                lit("relationships"),
                rels,
            ]);
            Some(null_unless(nodes.is_not_null(), struct_expr))
        }

        _ => None,
    }
}

/// `edge_uuid IS NOT NULL` for a lowered edge `VarRef` — whether the hop
/// matched (false only on an unmatched `OPTIONAL MATCH` row). `None` when the
/// edge did not lower to its bare scan qualifier.
fn edge_present(edge: &DfExpr) -> Option<DfExpr> {
    let DfExpr::Column(c) = edge else {
        return None;
    };
    Some(col(format!("{}.edge_uuid", c.name)).is_not_null())
}

/// `CASE WHEN <present> THEN <value> ELSE NULL END` — null-propagation for
/// composed path values whose parts would otherwise build non-null containers
/// over all-null columns.
pub(super) fn null_unless(present: DfExpr, value: DfExpr) -> DfExpr {
    when(present, value)
        .otherwise(lit(ScalarValue::Null))
        .expect("CASE build is infallible for a single WHEN + ELSE")
}

/// Compose the `node_uuid` column of a lowered node `VarRef`.
///
/// A node variable lowers to its bare scan qualifier (`col("var_<n>")`), so
/// the uuid column is the dotted composition — the same rule
/// `resolve_prop_col` applies to property columns. Falls back to `get_field`
/// for a computed base.
fn node_uuid_col(base: DfExpr) -> DfExpr {
    match &base {
        DfExpr::Column(c) => col(format!("{}.node_uuid", c.name)),
        _ => datafusion::functions::core::expr_fn::get_field(base, "node_uuid"),
    }
}

pub(super) fn edge_present_qual(base: &str) -> DfExpr {
    col(format!("{base}.edge_uuid")).is_not_null()
}

fn is_edge_value_topology_field(name: &str) -> bool {
    matches!(
        name,
        "edge_uuid"
            | "src_uuid"
            | "dst_uuid"
            | "edge_id"
            | "src_id"
            | "dst_id"
            | "created_at"
            | "rel_type_name"
    )
}

pub(super) fn empty_utf8_list() -> DfExpr {
    DfExpr::Literal(
        ScalarValue::List(ScalarValue::new_list(&[], &DataType::Utf8, true)),
        None,
    )
}

pub(super) fn null_utf8_list() -> DfExpr {
    null_unless(lit(false), empty_utf8_list())
}

fn node_labels_list(
    base: &str,
    label: Option<&str>,
    type_id_map: &HashMap<graphforge_value::EntityTypeId, String>,
) -> DfExpr {
    use datafusion::functions_nested::expr_fn::{array_concat, array_has, make_array};

    if type_id_map.is_empty() {
        return label.map_or_else(empty_utf8_list, |name| make_array(vec![lit(name)]));
    }

    let mut entries: Vec<(u32, &str)> = type_id_map
        .iter()
        .map(|(id, name)| (id.encode(), name.as_str()))
        .collect();
    entries.sort_by_key(|(id, _)| *id);
    let labels = col(format!("{base}.type_ids"));
    let parts = entries
        .into_iter()
        .map(|(id, name)| {
            when(
                array_has(labels.clone(), lit(id)),
                make_array(vec![lit(name)]),
            )
            .otherwise(empty_utf8_list())
            .expect("CASE build is infallible for a single WHEN + ELSE")
        })
        .collect();
    array_concat(parts)
}

/// Assemble a whole node value for a bare `RETURN n` (#785):
/// `Struct{node_uuid, labels: List<Utf8>, <prop…>}` over the node var's lowered
/// scan qualifier `base` (e.g. `"var_0"`). Property columns are referenced as
/// `base.<prop>` (already materialized by `join_node_properties`). Gated
/// `null_unless(node_uuid present)` so an unmatched OPTIONAL row yields null.
fn node_value_struct(
    base: &str,
    label: Option<&str>,
    type_id_map: &HashMap<graphforge_value::EntityTypeId, String>,
    prop_names: &[String],
) -> DfExpr {
    let labels = node_labels_list(base, label, type_id_map);
    node_value_struct_with_labels(base, labels, prop_names)
}

fn node_value_struct_with_labels(base: &str, labels: DfExpr, prop_names: &[String]) -> DfExpr {
    use datafusion::functions::core::expr_fn::named_struct;

    let mut fields = vec![
        lit("node_uuid"),
        col(format!("{base}.node_uuid")),
        lit("labels"),
        labels,
    ];
    for name in prop_names {
        fields.push(lit(name.as_str()));
        fields.push(qualified_col(base, name));
    }
    let value = named_struct(fields);
    null_unless(qualified_col(base, "node_uuid").is_not_null(), value)
}

/// Assemble a whole relationship value for a bare `RETURN r` / fixed-hop
/// `relationships(p)` element (#889): `Struct{edge_uuid, src_uuid, dst_uuid,
/// rel_type, <prop…>}` over the edge var's lowered scan qualifier `base`.
/// Property columns are referenced as `base.<prop>` after
/// `join_edge_properties` has materialized them.
fn relationship_value_struct(base: &str, rel_type: DfExpr, prop_names: &[String]) -> DfExpr {
    use datafusion::functions::core::expr_fn::named_struct;

    let mut fields = vec![
        lit("edge_uuid"),
        col(format!("{base}.edge_uuid")),
        lit("src_uuid"),
        col(format!("{base}.src_uuid")),
        lit("dst_uuid"),
        col(format!("{base}.dst_uuid")),
        lit("rel_type"),
        cast(rel_type, DataType::Utf8),
    ];
    for name in prop_names {
        fields.push(lit(name.as_str()));
        fields.push(qualified_col(base, name));
    }
    named_struct(fields)
}

// ---------------------------------------------------------------------------
// Graph metadata UDFs
// ---------------------------------------------------------------------------

static CYPHER_LABELS: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherGraphMetadata::new(GraphMetadataKind::Labels)));
pub(super) static CYPHER_REL_TYPE: LazyLock<ScalarUDF> = LazyLock::new(|| {
    ScalarUDF::new_from_impl(CypherGraphMetadata::new(
        GraphMetadataKind::RelationshipType,
    ))
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum GraphMetadataKind {
    Labels,
    RelationshipType,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherGraphMetadata {
    kind: GraphMetadataKind,
    signature: Signature,
}

impl CypherGraphMetadata {
    fn new(kind: GraphMetadataKind) -> Self {
        Self {
            kind,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherGraphMetadata {
    fn name(&self) -> &'static str {
        match self.kind {
            GraphMetadataKind::Labels => "cypher_labels",
            GraphMetadataKind::RelationshipType => "cypher_relationship_type",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(match self.kind {
            GraphMetadataKind::Labels => DataType::new_list(DataType::Utf8, true),
            GraphMetadataKind::RelationshipType => DataType::Utf8,
        })
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::new_empty_array;
        use datafusion::error::DataFusionError;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let values = args.args[0].to_array(rows)?;
        let field = match self.kind {
            GraphMetadataKind::Labels => "labels",
            GraphMetadataKind::RelationshipType => "rel_type",
        };
        let identity_field = match self.kind {
            GraphMetadataKind::Labels => "node_uuid",
            GraphMetadataKind::RelationshipType => "edge_uuid",
        };
        let mut output = Vec::with_capacity(rows);
        for row in 0..rows {
            let value = ScalarValue::try_from_array(&values, row)?;
            let value = decode_het(&value).unwrap_or(value);
            if value.is_null() {
                output.push(ScalarValue::try_new_null(&self.return_type(&[])?)?);
                continue;
            }
            let ScalarValue::Struct(entity) = value else {
                return Err(DataFusionError::Execution(format!(
                    "InvalidArgumentValue: {}() requires a graph element",
                    self.name()
                )));
            };
            if entity.column_by_name(identity_field).is_none() {
                return Err(DataFusionError::Execution(format!(
                    "InvalidArgumentValue: {}() received the wrong graph element kind",
                    self.name()
                )));
            }
            let column = entity.column_by_name(field).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "InvalidArgumentValue: {}() received the wrong graph element kind",
                    self.name()
                ))
            })?;
            output.push(ScalarValue::try_from_array(column, 0)?);
        }
        let data_type = self.return_type(&[])?;
        let array = if output.is_empty() {
            new_empty_array(&data_type)
        } else {
            ScalarValue::iter_to_array(output)?
        };
        Ok(ColumnarValue::Array(array))
    }
}

// ---------------------------------------------------------------------------
// cypher_path_nodes UDF
// ---------------------------------------------------------------------------

/// The traversed node sequence of a named path (#754): given the start node's
/// uuid and the path's relationship list (the #709 edge-list column), emit
/// `List<Struct{node_uuid}>` with `hops + 1` entries in traversal order.
///
/// The edge structs store `src_uuid`/`dst_uuid` in **storage** orientation,
/// while the BFS traverses `In`/`Undirected` edges against it — but every
/// emission is a connected walk, so the sequence is recovered per hop as "the
/// edge's other endpoint": `next = (cur == src ? dst : src)`. Self-loops
/// resolve to `cur`; an edge matching neither endpoint is impossible for a
/// well-formed emission and raises an execution error.
///
/// A null seed or null list yields null (an unmatched `OPTIONAL MATCH` row);
/// an empty list is the 0-hop self-path and yields `[{seed}]`.
static CYPHER_PATH_NODES: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherPathNodes::new()));

/// The `node_uuid`-only struct fields of one `cypher_path_nodes` list element.
///
/// A struct (rather than a bare `FixedSizeBinary`) mirrors the edge-list
/// element shape. The hydrated variant adds node properties and labels through
/// the same container kind.
fn path_node_struct_fields() -> datafusion::arrow::datatypes::Fields {
    use datafusion::arrow::datatypes::Field;
    vec![Field::new(
        "node_uuid",
        DataType::FixedSizeBinary(16),
        false,
    )]
    .into()
}

/// Lowering-baked context for hydrating path-node elements with labels and
/// properties (#1024). Sorted `Vec`s rather than maps so the UDF stays
/// `Hash`/`Eq`.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct PathNodeHydration {
    /// `type_id → label` (ontology + runtime catalog), sorted by id.
    pub labels_by_type: Vec<(graphforge_value::EntityTypeId, String)>,
    /// The `properties/<stem>.parquet` stems whose fields form the union,
    /// sorted — the invoke coalesces each node's values across them.
    pub prop_stems: Vec<String>,
    /// The full element fields: `node_uuid`, `labels`, then the property union.
    pub fields: datafusion::arrow::datatypes::Fields,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherPathNodes {
    signature: Signature,
    hydrate: Option<PathNodeHydration>,
}

/// Inspect the neutral descriptor of a hydrated path-node function.
#[must_use]
pub fn path_node_hydration_descriptor(function: &ScalarUDF) -> Option<&PathNodeHydration> {
    function
        .inner()
        .downcast_ref::<CypherPathNodes>()?
        .hydrate
        .as_ref()
}

impl CypherPathNodes {
    fn new() -> Self {
        // (seed_uuid, relationship_list); immutable.
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            hydrate: None,
        }
    }

    fn with_hydration(hydrate: PathNodeHydration) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            hydrate: Some(hydrate),
        }
    }

    fn element_fields(&self) -> datafusion::arrow::datatypes::Fields {
        self.hydrate
            .as_ref()
            .map_or_else(path_node_struct_fields, |h| h.fields.clone())
    }
}

impl ScalarUDFImpl for CypherPathNodes {
    fn name(&self) -> &'static str {
        "cypher_path_nodes"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::new_list(
            DataType::Struct(self.element_fields()),
            true,
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        if self.hydrate.is_some() {
            return Err(datafusion::error::DataFusionError::Plan(
                "GF_READ_RESOURCE_MISSING: path hydration".into(),
            ));
        }
        evaluate_path_nodes(&args, self.element_fields(), |_, _| Ok(Vec::new()))
    }
}

/// Assemble path-node lists while an execution-owned callback supplies children.
/// This function performs no storage reads and preserves expression-local evaluation.
pub fn evaluate_path_nodes(
    args: &ScalarFunctionArgs,
    element_fields: datafusion::arrow::datatypes::Fields,
    mut hydrate: impl FnMut(
        &[[u8; 16]],
        usize,
    ) -> datafusion::error::Result<Vec<datafusion::arrow::array::ArrayRef>>,
) -> datafusion::error::Result<ColumnarValue> {
    use datafusion::arrow::array::{
        Array, ArrayRef, FixedSizeBinaryArray, ListArray, StructArray, new_empty_array,
    };
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::datatypes::Field;
    use datafusion::common::cast::as_list_array;
    use datafusion::error::DataFusionError;
    use std::sync::Arc;

    validate_heterogeneous_arguments(&args.args)?;
    let exec_err = |m: String| DataFusionError::Execution(m);
    let as_fsb16 =
        |array: &dyn Array, what: &str| -> datafusion::error::Result<FixedSizeBinaryArray> {
            array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .filter(|a| a.value_length() == 16)
                .cloned()
                .ok_or_else(|| {
                    exec_err(format!(
                        "cypher_path_nodes: expected FixedSizeBinary(16) {what}, got {:?}",
                        array.data_type()
                    ))
                })
        };

    let seeds = args.args[0].to_array(args.number_rows)?;
    let seeds = as_fsb16(seeds.as_ref(), "start-node uuid")?;
    let rels = args.args[1].to_array(args.number_rows)?;
    let rels = as_list_array(&rels)?.clone();

    // Walk each row's relationship list from its seed, flattening the node
    // sequences: one uuid per visited node, one (length, validity) per row.
    let mut flat: Vec<[u8; 16]> = Vec::new();
    let mut lengths: Vec<usize> = Vec::with_capacity(rels.len());
    let mut valid: Vec<bool> = Vec::with_capacity(rels.len());
    for row in 0..rels.len() {
        if seeds.is_null(row) || rels.is_null(row) {
            lengths.push(0);
            valid.push(false);
            continue;
        }
        let start = flat.len();
        let mut cur = [0u8; 16];
        cur.copy_from_slice(seeds.value(row));
        flat.push(cur);

        let edges = rels.value(row);
        let edges = edges
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                exec_err("cypher_path_nodes: relationship-list items must be structs".into())
            })?;
        // Topology children by name — property fields are appended after
        // them, so positional access would be wrong (#755).
        let src = edges.column_by_name("src_uuid").ok_or_else(|| {
            exec_err("cypher_path_nodes: relationship struct has no src_uuid".into())
        })?;
        let src = as_fsb16(src.as_ref(), "src_uuid")?;
        let dst = edges.column_by_name("dst_uuid").ok_or_else(|| {
            exec_err("cypher_path_nodes: relationship struct has no dst_uuid".into())
        })?;
        let dst = as_fsb16(dst.as_ref(), "dst_uuid")?;

        for i in 0..edges.len() {
            let s = src.value(i);
            let d = dst.value(i);
            let next = if cur == s {
                d
            } else if cur == d {
                s
            } else {
                return Err(exec_err(format!(
                    "cypher_path_nodes: edge {i} is disconnected from the \
                         path (corrupt traversal emission)"
                )));
            };
            cur.copy_from_slice(next);
            flat.push(cur);
        }
        lengths.push(flat.len() - start);
        valid.push(true);
    }

    // node_uuid child — width-16 even when there are zero total nodes
    // (`try_from_iter` would infer width 0 and fail the schema check).
    let uuid_child: ArrayRef = if flat.is_empty() {
        new_empty_array(&DataType::FixedSizeBinary(16))
    } else {
        Arc::new(
            FixedSizeBinaryArray::try_from_iter(flat.iter())
                .map_err(|e| exec_err(e.to_string()))?,
        )
    };
    let fields = element_fields;
    let mut children: Vec<ArrayRef> = vec![uuid_child];
    let batch_size = args.config_options.execution.batch_size.max(1);
    children.extend(hydrate(&flat, batch_size)?);

    let struct_arr = StructArray::try_new(fields.clone(), children, None)
        .map_err(|e| exec_err(e.to_string()))?;
    let offsets = OffsetBuffer::<i32>::from_lengths(lengths);
    let item = Arc::new(Field::new("item", DataType::Struct(fields), true));
    let list = ListArray::try_new(
        item,
        offsets,
        Arc::new(struct_arr),
        Some(NullBuffer::from(valid)),
    )
    .map_err(|e| exec_err(e.to_string()))?;
    Ok(ColumnarValue::Array(Arc::new(list)))
}

#[cfg(test)]
mod tests;
