//! IR expression arena → DataFusion [`Expr`] lowering.
//!
//! [`ExprLowerer`] is a pure, I/O-free transformation: it walks the
//! [`ExprArena`] from a [`GraphPlan`] and produces DataFusion [`Expr`] values
//! that can be consumed by operator lowering (#575, #576).
//!
//! Private domain modules own list construction, graph/path values, scalar
//! execution, temporal/spatial lowering, access, comparisons, and aggregates.
//! The parent retains lowerer state, central dispatch, and shared shape contracts.

mod aggregates;
mod graph_values;
use graph_values::{
    CYPHER_REL_TYPE, edge_present_qual, empty_utf8_list, is_path_builtin_name, null_unless,
    null_utf8_list, resolve_path_builtin,
};
pub use graph_values::{PathNodeHydration, evaluate_path_nodes, path_node_hydration_descriptor};
mod list_values;
#[cfg(test)]
use list_values::het_depth;
use list_values::{
    CYPHER_LIST_PLUS, build_het_struct, const_map_scalar, het_fields, is_plain_map_struct_type,
    lower_list_literal, unwrap_het,
};
mod list_execution;
pub use list_execution::rewrite_embedded_expressions;
use list_execution::{CypherInvariantQuantifier, CypherListComp, CypherQuantifier};
#[cfg(test)]
use list_execution::{INVARIANT_QUANTIFIER_ROWS, reduce_invariant_quantifier, reduce_quantifier};
mod scalar_execution;
pub use scalar_execution::is_cypher_row_marker;
use scalar_execution::{
    CYPHER_CONTAINS, CYPHER_ENDS_WITH, CYPHER_REVERSE, CYPHER_STARTS_WITH, one_based_index,
    resolve_builtin,
};
pub(crate) use scalar_execution::{CYPHER_ROW_MARKER, list_index_range};
mod scalar_adapters;
use scalar_adapters::{
    CYPHER_TO_BOOLEAN, CYPHER_TO_FLOAT, CYPHER_TO_INTEGER, CYPHER_TO_STRING, render_temporal,
    spatial_scalar,
};
#[cfg(test)]
use scalar_adapters::{CypherToString, cypher_float_string};
pub use scalar_adapters::{ir_literal_to_scalar, scalar_to_ir_literal};

mod spatial_lowering;
mod temporal_lowering;
mod temporal_udfs;
use temporal_udfs::{
    CYPHER_DATE_COMPONENT, CYPHER_DATE_PROJECT, CYPHER_DATE_TRUNCATE, CYPHER_DATETIME_PROJECT,
    CYPHER_DATETIME_TRUNCATE, CYPHER_DURATION_ADD, CYPHER_DURATION_BETWEEN,
    CYPHER_DURATION_COMPONENT, CYPHER_DURATION_PARSE, CYPHER_DURATION_SCALE,
    CYPHER_LOCALDATETIME_PROJECT, CYPHER_LOCALDATETIME_TRUNCATE, CYPHER_LOCALTIME_PROJECT,
    CYPHER_LOCALTIME_TRUNCATE, CYPHER_TEMPORAL_ARITH, CYPHER_TEMPORAL_COMPONENT,
    CYPHER_TEMPORAL_ZONE_STR, CYPHER_TIME_PROJECT, CYPHER_TIME_TRUNCATE, date_scalar,
    date_struct_value, datetime_scalar, datetime_struct_parts, dur_secs_nanos, duration_scalar,
    duration_struct_parts, duration_value_to_ir, is_date_struct, is_datetime_struct,
    is_duration_struct, is_localdatetime_struct, is_temporal_clock_fn, is_time_struct,
    localdatetime_scalar, localdatetime_struct_parts, temporal_accessor_valid,
    temporal_null_scalar, time_scalar, time_struct_parts,
};
mod value_access;
use value_access::{
    CYPHER_MAP_KEYS, CYPHER_VALUE_ACCESS, CypherEntityProperties, CypherStaticValueAccess,
};
mod value_semantics;

pub use value_semantics::decode_het_scalar;
use value_semantics::{
    CYPHER_AND, CYPHER_CMP_PRED, CYPHER_EQ, CYPHER_IN, CYPHER_OR, CYPHER_XOR, decode_het,
    scalar_as_f64, scalar_as_i128, validate_heterogeneous_arguments,
};
pub(crate) use value_semantics::{CYPHER_ORDER_KEY, needs_cypher_order_key_type};

pub(crate) fn is_comparison_predicate(
    function: &datafusion::logical_expr::expr::ScalarFunction,
) -> bool {
    value_semantics::is_comparison_predicate(function)
}

pub(crate) use aggregates::{
    CYPHER_COLLECT, CYPHER_COLLECT_DISTINCT, CYPHER_MAX, CYPHER_MIN, CYPHER_PERCENTILE_CONT,
    CYPHER_PERCENTILE_DISC,
};

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{Array, new_empty_array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::expr::Placeholder;
use datafusion::logical_expr::{
    ColumnarValue, Expr as DfExpr, ExprSchemable, Operator, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility, cast, col, lit, not, when,
};
use datafusion::scalar::ScalarValue;

use graphforge_ir::expr::{BinaryOpKind, IrExpr, IrLiteral, UnaryOpKind};
use graphforge_ir::{ExprArena, ExprId, VarId};
use graphforge_ontology::OntologyHandle;
use graphforge_value::PropertyId;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Maps [`VarId`]s to DataFusion column name strings.
///
/// The binder allocates one `VarId` per distinct bound variable in a query
/// (e.g. `a`, `b`, `r`).  `VarMap` records the DataFusion column name each
/// variable resolves to at execution time — typically `"<alias>.node_id"` for
/// node variables and `"<alias>.edge_id"` for edge variables.
#[derive(Debug, Clone, Default)]
pub struct VarMap(HashMap<u32, String>);

impl VarMap {
    /// Creates an empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that variable `var` maps to DataFusion column `col_name`.
    pub fn insert(&mut self, var: VarId, col_name: impl Into<String>) {
        self.0.insert(var.0, col_name.into());
    }

    /// Drops every registered variable. Used to install a fresh `WITH` scope so
    /// pre-`WITH` variables stop resolving (mirrors the binder's scope reset).
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Returns the column name for `var`, or `None` if it has not been registered.
    #[must_use]
    pub fn get(&self, var: VarId) -> Option<&str> {
        self.0.get(&var.0).map(String::as_str)
    }

    /// Iterates the [`VarId`]s currently registered, in arbitrary order.
    ///
    /// Used to compute the variables shared between two scopes (e.g. the outer
    /// and optional sides of an `OPTIONAL MATCH`, which become its join keys).
    pub fn var_ids(&self) -> impl Iterator<Item = VarId> + '_ {
        self.0.keys().map(|&k| VarId(k))
    }
}

pub use graphforge_core::LoweringError;

/// The shape of a node value materialized for a bare `RETURN n` (#785): the
/// node's resolved label (if known at lowering) and the persisted property
/// columns its scan joined in (available as `var_N.<prop>`).
#[derive(Clone, Debug)]
pub struct NodeShape {
    /// Persisted property column names, materialized as `var_N.<name>` by
    /// `join_node_properties`. The label is supplied separately by the binder
    /// (the ontology map is empty in exploratory mode).
    pub prop_names: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EntityIdentityKind {
    Node,
    Edge,
}

// ---------------------------------------------------------------------------
// ExprLowerer
// ---------------------------------------------------------------------------

/// Lowers IR expressions from an [`ExprArena`] into DataFusion [`Expr`] values.
///
/// Construct once per plan lowering pass and call [`lower`](Self::lower) for
/// each [`ExprId`] you need to convert.
pub struct ExprLowerer<'a> {
    arena: &'a ExprArena,
    var_map: &'a VarMap,
    /// Reverse map `PropId.0` → property column name. Built from the ontology
    /// and/or the runtime catalog. Falls back to `"prop_<id>"` for any `PropId`
    /// not present (e.g. a strict-mode unresolved property).
    prop_names: HashMap<PropertyId, String>,
    /// `VarId.0` → node shape, for materializing a bare `RETURN n` as a whole
    /// node value (#785). Empty unless the plan projects a node var by value.
    node_shapes: HashMap<u32, NodeShape>,
    /// Reverse map `TypeId.0` → entity-type (node label) name, merged from the
    /// ontology and runtime catalog. Used to render a real label for an
    /// *unlabelled* node value (`MATCH (n) RETURN n`) by switching on the node's
    /// stored `type_id` (#889). Empty in schema-only/no-catalog lowering.
    type_id_to_entity_name: HashMap<graphforge_value::EntityTypeId, String>,
    /// Forward map of entity-type (node label) name to `TypeId.0`. Derived once
    /// from `type_id_to_entity_name` so a literal `'<label>' IN labels(node)` can
    /// lower directly to topology membership without rebuilding the complete
    /// string label list for every pattern predicate.
    entity_name_to_type_id: HashMap<String, graphforge_value::EntityTypeId>,
    /// Whether `node_shapes`' property lists are AUTHORITATIVE — i.e. read from a
    /// real backing dataset, so an absent property name truly means the node lacks
    /// it (→ Cypher `null`). False for schema-only / explain lowering (no dataset),
    /// where an empty `prop_names` just means "unknown", not "absent" — there a
    /// property access must stay an unresolved column reference, never be nulled.
    /// Gates the missing-property→null rewrite (#598). See `node_prop_cols`.
    props_authoritative: bool,
    /// The input plan's schema, when lowering a relational op's expressions
    /// (set per-op via [`with_input_schema`](Self::with_input_schema)). Lets a
    /// `PropertyAccess` consult the base column's Arrow type — so `d.year` on a
    /// `Date32` lowers to a temporal-component extraction rather than a property
    /// column reference (ADR 0009 / #920). `None` in schema-only lowering.
    input_schema: Option<datafusion::common::DFSchemaRef>,
    /// The chain of synthetic per-element column names in scope while lowering
    /// a quantifier / list-comprehension predicate (#1004): one entry per
    /// enclosing loop, outermost first (`__gf_elem`, `__gf_elem_1`, …), so
    /// nested loops keep distinct bindings (#1021). A `PropertyAccess` whose
    /// base resolves to ANY of these columns is lowered via struct-aware
    /// `get_field` rather than a dotted property-column name — the element is
    /// a single struct column, not a table whose fields are top-level columns.
    /// Empty elsewhere; its length is the current nesting depth.
    elem_struct_cols: Vec<String>,
    /// Immutable dataset schemas used to describe hydrated `nodes(p)` values.
    /// Schema-only explanation retains the original UUID-only shape.
    read_target: Option<graphforge_ir::LoweringSnapshot>,
    /// Wall-clock instant captured ONCE per lowering (lazily, on first use) so all
    /// zero-arg current-time constructors — `date()`/`localtime()`/…/`datetime()`
    /// — in one query fold to the SAME value, making
    /// `duration.inSeconds(localtime(), localtime())` exactly zero. (#1007)
    now: std::sync::OnceLock<chrono::NaiveDateTime>,
}

impl<'a> ExprLowerer<'a> {
    /// Creates a new lowerer, building the `PropId → name` map from the
    /// ontology only (empty in exploratory mode).
    ///
    /// - `arena`: the expression arena from the [`GraphPlan`] being lowered.
    /// - `ontology`: the ontology handle, if one is loaded (may be `None` in
    ///   exploratory mode).
    /// - `var_map`: maps variable IDs to DataFusion column name strings.
    ///
    /// Prefer [`with_prop_names`](Self::with_prop_names) when the caller has a
    /// map that also covers runtime-catalog (exploratory) property names.
    #[must_use]
    pub fn new(
        arena: &'a ExprArena,
        ontology: Option<&'a OntologyHandle>,
        var_map: &'a VarMap,
    ) -> Self {
        Self {
            arena,
            var_map,
            prop_names: build_prop_names(ontology),
            node_shapes: HashMap::new(),
            type_id_to_entity_name: HashMap::new(),
            entity_name_to_type_id: HashMap::new(),
            props_authoritative: false,
            input_schema: None,
            elem_struct_cols: Vec::new(),
            read_target: None,
            now: std::sync::OnceLock::new(),
        }
    }

    /// Creates a lowerer with a prebuilt `PropId → name` map.
    ///
    /// The [`GraphPlanLowerer`](crate::GraphPlanLowerer) builds the map once
    /// (merging ontology + runtime catalog) and clones it into each
    /// per-operator `ExprLowerer` (the maps are small).
    #[must_use]
    pub fn with_prop_names(
        arena: &'a ExprArena,
        var_map: &'a VarMap,
        prop_names: HashMap<PropertyId, String>,
    ) -> Self {
        Self {
            arena,
            var_map,
            prop_names,
            node_shapes: HashMap::new(),
            type_id_to_entity_name: HashMap::new(),
            entity_name_to_type_id: HashMap::new(),
            props_authoritative: false,
            input_schema: None,
            elem_struct_cols: Vec::new(),
            read_target: None,
            now: std::sync::OnceLock::new(),
        }
    }

    /// Like [`with_prop_names`](Self::with_prop_names) but also seeded with the
    /// `VarId.0 → NodeShape` map for bare-node-value materialization (#785).
    ///
    /// `props_authoritative` is true when `node_shapes`' property lists come from
    /// a real backing dataset (so an absent property is genuinely absent → `null`,
    /// #598); pass false for schema-only / explain lowering. See the field docs.
    #[must_use]
    pub fn with_prop_names_and_nodes(
        arena: &'a ExprArena,
        var_map: &'a VarMap,
        prop_names: HashMap<PropertyId, String>,
        node_shapes: HashMap<u32, NodeShape>,
        type_id_to_entity_name: HashMap<graphforge_value::EntityTypeId, String>,
        props_authoritative: bool,
    ) -> Self {
        let entity_name_to_type_id = type_id_to_entity_name
            .iter()
            .map(|(id, name)| (name.clone(), *id))
            .collect();
        Self {
            arena,
            var_map,
            prop_names,
            node_shapes,
            type_id_to_entity_name,
            entity_name_to_type_id,
            props_authoritative,
            input_schema: None,
            elem_struct_cols: Vec::new(),
            read_target: None,
            now: std::sync::OnceLock::new(),
        }
    }

    /// Attach the input plan's schema so a relational op's `PropertyAccess` can
    /// resolve a temporal-component accessor (`d.year`) by the base column's
    /// Arrow type (ADR 0009 / #920).
    #[must_use]
    pub fn with_input_schema(mut self, schema: datafusion::common::DFSchemaRef) -> Self {
        self.input_schema = Some(schema);
        self
    }

    /// Push `col` onto the chain of synthetic per-element columns in scope, so
    /// a `PropertyAccess` on it lowers via struct-aware `get_field` rather than
    /// a dotted property-column name (#1004). Called once per enclosing loop,
    /// outermost first, so nested loops keep distinct bindings (#1021).
    #[must_use]
    pub fn with_elem_struct_col(mut self, col: String) -> Self {
        self.elem_struct_cols.push(col);
        self
    }

    /// Attach immutable dataset schemas for the logical `nodes(p)` output.
    #[must_use]
    pub fn with_read_target(mut self, dir: graphforge_ir::LoweringSnapshot) -> Self {
        self.read_target = Some(dir);
        self
    }

    /// Lower the expression identified by `id` to a DataFusion [`Expr`].
    ///
    /// # Errors
    /// Returns [`LoweringError`] if a variable is unbound, a function is
    /// unknown, or an expression variant is not yet supported.
    #[allow(
        clippy::too_many_lines,
        reason = "one cohesive dispatch match over every IrExpr variant plus the \
                  namespaced temporal builtins (date/time/datetime truncate); \
                  splitting the arms would scatter the lowering logic"
    )]
    pub fn lower(&self, id: ExprId) -> Result<DfExpr, LoweringError> {
        match self.arena.get(id) {
            IrExpr::Literal(lit_val) => Ok(lower_literal(lit_val)),

            IrExpr::VarRef(var_id) => {
                let col_name = self
                    .var_map
                    .get(*var_id)
                    .ok_or(LoweringError::UnboundVar(var_id.0))?;
                if let Some(schema) = self.input_schema.as_ref()
                    && schema.field_with_unqualified_name(col_name).is_err()
                {
                    let qual = datafusion::common::TableReference::bare(col_name);
                    if schema
                        .index_of_column_by_name(Some(&qual), "node_uuid")
                        .is_some()
                    {
                        return Ok(col(format!("{col_name}.node_uuid")));
                    }
                    if schema
                        .index_of_column_by_name(Some(&qual), "edge_uuid")
                        .is_some()
                    {
                        return Ok(col(format!("{col_name}.edge_uuid")));
                    }
                }
                // Reference the variable's column by its LITERAL name (#957):
                // `col()` parses + lowercases unquoted identifiers, silently
                // breaking a mixed-case alias (`WITH v AS otherDate RETURN
                // otherDate` → "No field named otherdate").
                if self
                    .input_schema
                    .as_ref()
                    .is_some_and(|schema| schema.index_of_column_by_name(None, col_name).is_some())
                {
                    Ok(DfExpr::Column(datafusion::common::Column::new_unqualified(
                        col_name,
                    )))
                } else {
                    Ok(col_literal(col_name))
                }
            }

            IrExpr::PropertyAccess { base, prop } => {
                if let Some(prop_name) = self.prop_names.get(prop).cloned()
                    && let Some(out) = self.lower_static_value_access(*base, &prop_name)?
                {
                    return Ok(out);
                }
                // Cypher: reading a property a node does not have yields `null`,
                // not an error. The columns resolvable under a node var's
                // qualifier are its TOPOLOGY columns (`node_uuid`, `type_id`, …)
                // PLUS the property columns its scan joined in
                // (`NodeShape::prop_names`, authoritative — see `node_prop_cols`,
                // which excludes the topology columns precisely because they are
                // always present). A property is genuinely absent only when in
                // NEITHER set; then the dotted column `var_N.<prop>` does not
                // exist, so emit a null literal rather than a dangling column
                // reference DataFusion rejects at planning. (Without the topology
                // exemption an access like `n.node_uuid` — used by the bindings —
                // would be wrongly nulled.) The name is resolved exactly as
                // `resolve_prop_col` resolves it, so the membership test matches
                // the column it would build. (#598, Null1)
                if self.props_authoritative
                    && let IrExpr::VarRef(v) = self.arena.get(*base)
                    && let Some(shape) = self.node_shapes.get(&v.0)
                {
                    let prop_name = self
                        .prop_names
                        .get(prop)
                        .cloned()
                        .unwrap_or_else(|| format!("prop_{prop}"));
                    let is_topology = graphforge_ir::arrow_schema::TOPOLOGY_NODES_SCHEMA
                        .field_with_name(&prop_name)
                        .is_ok();
                    if !is_topology && !shape.prop_names.contains(&prop_name) {
                        return Ok(lit(ScalarValue::Null));
                    }
                }
                // Temporal component accessor (#920): `d.year` where `d` is a
                // date-struct-typed column (`Struct{epoch_day}`, ADR 0012) lowers to
                // component extraction, not a property column. Dispatch needs the
                // base's type, so it only fires when the input schema is known and
                // the base is a var whose column is the date struct. (Other types'
                // accessors follow once those types are typed.)
                if let IrExpr::VarRef(v) = self.arena.get(*base)
                    && let Some(col_name) = self.var_map.get(*v)
                    && let Some(prop_name) = self.prop_names.get(prop)
                    && crate::temporal::is_date_accessor(prop_name)
                    && let Some(schema) = self.input_schema.as_ref()
                    && let Ok(field) = schema.field_with_unqualified_name(col_name)
                    && is_date_struct(field.data_type())
                {
                    return Ok(CYPHER_DATE_COMPONENT
                        .call(vec![col_literal(col_name), lit(prop_name.as_str())]));
                }
                // Duration component accessor (#920): `d.days`/`d.seconds`/… where
                // `d` is a typed `duration` struct column.
                if let IrExpr::VarRef(v) = self.arena.get(*base)
                    && let Some(col_name) = self.var_map.get(*v)
                    && let Some(prop_name) = self.prop_names.get(prop)
                    && crate::temporal::is_duration_accessor(prop_name)
                    && let Some(schema) = self.input_schema.as_ref()
                    && let Ok(field) = schema.field_with_unqualified_name(col_name)
                    && is_duration_struct(field.data_type())
                {
                    return Ok(CYPHER_DURATION_COMPONENT
                        .call(vec![col_literal(col_name), lit(prop_name.as_str())]));
                }
                // Other typed-temporal component accessors (#1008): `localtime`
                // (`Time64`), `time`/`localdatetime`/`datetime` (structs). `Date32`
                // and duration are handled above; here we extract time-of-day, date
                // (for localdatetime/datetime), zone, and epoch (datetime)
                // components. Zone strings (`timezone`/`offset`) → `Utf8`, all else
                // `Int64`; the UDFs inspect the column's Arrow type to pick the field.
                if let IrExpr::VarRef(v) = self.arena.get(*base)
                    && let Some(col_name) = self.var_map.get(*v)
                    && let Some(prop_name) = self.prop_names.get(prop)
                    && let Some(schema) = self.input_schema.as_ref()
                    && let Ok(field) = schema.field_with_unqualified_name(col_name)
                    && temporal_accessor_valid(field.data_type(), prop_name)
                {
                    let args = vec![col_literal(col_name), lit(prop_name.as_str())];
                    return Ok(if crate::temporal::is_zone_str_accessor(prop_name) {
                        CYPHER_TEMPORAL_ZONE_STR.call(args)
                    } else {
                        CYPHER_TEMPORAL_COMPONENT.call(args)
                    });
                }
                // Struct-field access on a plain-map column (#1017): a variable bound
                // to a map value — `UNWIND [{k: …}] AS m` then `m.k`, or `WITH {…} AS
                // m` — is a single `Struct` column, so its fields are struct fields,
                // not dotted property columns. Resolve via struct-aware `get_field`,
                // mirroring the quantifier element case (#1004). Entities keep dotted
                // property columns (via `resolve_prop_col`); temporal structs are
                // handled above.
                if let IrExpr::VarRef(v) = self.arena.get(*base)
                    && let Some(col_name) = self.var_map.get(*v)
                    && let Some(schema) = self.input_schema.as_ref()
                    && let Ok(field) = schema.field_with_unqualified_name(col_name)
                    && is_plain_map_struct_type(field.data_type())
                {
                    let prop_name = self
                        .prop_names
                        .get(prop)
                        .cloned()
                        .unwrap_or_else(|| format!("prop_{prop}"));
                    return Ok(datafusion::functions::core::expr_fn::get_field(
                        col_literal(col_name),
                        prop_name,
                    ));
                }
                // Lower the base expression (typically a VarRef) and append the
                // property name. For a VarRef base, keep the qualifier itself
                // (`var_N`), not the scalarized entity identity (`var_N.node_uuid`)
                // used when a bare entity variable appears in scalar contexts.
                let base_expr = if let IrExpr::VarRef(v) = self.arena.get(*base) {
                    col_literal(self.var_map.get(*v).ok_or(LoweringError::UnboundVar(v.0))?)
                } else {
                    self.lower(*base)?
                };
                if self.is_known_non_value_access_container(&base_expr) {
                    let prop_name = self
                        .prop_names
                        .get(prop)
                        .cloned()
                        .unwrap_or_else(|| format!("prop_{prop}"));
                    return Err(LoweringError::InvalidType(format!(
                        "property access `{prop_name}` requires a map or graph element"
                    )));
                }
                let prop_col = self.resolve_prop_col(base_expr, *prop);
                Ok(prop_col)
            }

            IrExpr::BinaryOp { op, left, right } => self.lower_binary(*op, *left, *right),

            IrExpr::UnaryOp { op, expr } => self.lower_unary(*op, *expr),

            IrExpr::FunctionCall { name, args } if name == "_node_struct" => {
                self.lower_node_struct(args)
            }

            IrExpr::FunctionCall { name, args } if name == "_node_struct_list" => {
                self.lower_node_struct_list(args)
            }

            IrExpr::FunctionCall { name, args } if name == "_rel_struct" => {
                self.lower_rel_struct(args)
            }

            IrExpr::FunctionCall { name, args } if name == "_rel_struct_list" => {
                self.lower_rel_struct_list(args)
            }

            IrExpr::FunctionCall { name, args } if name == "keys" => self.lower_keys(args),

            IrExpr::FunctionCall { name, args } if name == "properties" => {
                self.lower_properties(args)
            }

            IrExpr::FunctionCall { name, args } if name == "labels" => self.lower_labels(args),

            IrExpr::FunctionCall { name, args } if name.eq_ignore_ascii_case("point") => {
                self.lower_spatial_point(args)
            }

            IrExpr::FunctionCall { name, args } if name.eq_ignore_ascii_case("distance") => {
                self.lower_spatial_distance(args)
            }

            IrExpr::FunctionCall { name, args }
                if matches!(name.as_str(), "nodes" | "relationships") =>
            {
                let [arg] = args.as_slice() else {
                    return Err(LoweringError::InvalidType(format!(
                        "{name}() expects one path argument"
                    )));
                };
                Ok(datafusion::functions::core::expr_fn::get_field(
                    self.lower(*arg)?,
                    name,
                ))
            }

            IrExpr::FunctionCall { name, args } if name == "_subscript" => {
                self.lower_subscript(args)
            }

            IrExpr::FunctionCall { name, args }
                if matches!(
                    name.as_str(),
                    "date" | "localtime" | "time" | "localdatetime" | "datetime" | "duration"
                ) =>
            {
                self.lower_temporal(name, args)
            }

            IrExpr::FunctionCall { name, args }
                if matches!(
                    name.as_str(),
                    "datetime.fromepoch" | "datetime.fromepochmillis"
                ) =>
            {
                self.lower_from_epoch(name, args)
            }

            IrExpr::FunctionCall { name, args } if name == "date.truncate" => {
                self.lower_date_truncate(args)
            }
            IrExpr::FunctionCall { name, args } if name == "localtime.truncate" => {
                self.lower_localtime_truncate(args)
            }
            IrExpr::FunctionCall { name, args } if name == "localdatetime.truncate" => {
                self.lower_localdatetime_truncate(args)
            }
            IrExpr::FunctionCall { name, args } if name == "time.truncate" => {
                self.lower_time_truncate(args)
            }
            IrExpr::FunctionCall { name, args } if name == "datetime.truncate" => {
                self.lower_datetime_truncate(args)
            }
            // Clock functions `<type>.transaction/.statement/.realtime` (#920).
            // A non-deterministic current-time clock is not modelled; the corpus
            // only exercises the null-propagating form (`date.realtime(null)` →
            // `null`, Temporal4 [13]), so handle that and leave the live-clock
            // form unsupported.
            IrExpr::FunctionCall { name, args } if is_temporal_clock_fn(name) => {
                if self.sole_arg_is_null(args) {
                    Ok(DfExpr::Literal(temporal_null_scalar(name), None))
                } else {
                    Err(LoweringError::UnsupportedExpr(format!(
                        "{name}: temporal clock functions are not supported in a \
                         deterministic query context"
                    )))
                }
            }
            // `duration.between(a, b)` and the single-unit `inMonths`/`inDays`/
            // `inSeconds` (#920). Matched case-insensitively (function names are
            // case-insensitive in Cypher).
            IrExpr::FunctionCall { name, args }
                if matches!(
                    name.to_ascii_lowercase().as_str(),
                    "duration.between"
                        | "duration.inmonths"
                        | "duration.indays"
                        | "duration.inseconds"
                ) =>
            {
                self.lower_duration_between(&name.to_ascii_lowercase(), args)
            }

            IrExpr::FunctionCall { name, args } => {
                let lowered = if is_path_builtin_name(name) {
                    args.iter()
                        .map(|&a| self.lower_path_builtin_arg(a))
                        .collect::<Result<Vec<_>, _>>()?
                } else {
                    args.iter()
                        .map(|&a| self.lower(a))
                        .collect::<Result<Vec<_>, _>>()?
                };
                // `reverse` is polymorphic: a string reverses its characters, a
                // list its elements. Dispatch on the lowered argument's type —
                // only a statically-known string takes the char path; a list (or
                // an unknown type) reverses as a list. (#955)
                if name == "reverse"
                    && let [arg] = lowered.as_slice()
                {
                    return Ok(if self.is_string_typed(arg) {
                        datafusion::functions::unicode::expr_fn::reverse(arg.clone())
                    } else if self.is_list_typed(arg) {
                        datafusion::functions_nested::expr_fn::array_reverse(arg.clone())
                    } else {
                        // Type not known at plan time (parameter / unresolved
                        // property) — dispatch at runtime rather than assuming a
                        // list (which would mis-plan a string). (#955)
                        CYPHER_REVERSE.call(vec![arg.clone()])
                    });
                }
                resolve_builtin(name, lowered, {
                    let hydration = if name.eq_ignore_ascii_case("_path_nodes") {
                        self.path_node_hydration()?
                    } else {
                        None
                    };
                    || hydration
                })
                .ok_or_else(|| LoweringError::UnknownFunction(name.clone()))
            }

            IrExpr::Parameter(name) => Ok(DfExpr::Placeholder(Placeholder {
                // DataFusion's named-parameter substitution (`ParamValues::Map`)
                // strips the leading char of the placeholder id before looking
                // it up (it assumes `$name` ids keyed by bare `name`), so the id
                // must carry the `$` the lexer stripped — otherwise binding by
                // name fails. See `ExecutionSession::execute_plan_with_params`.
                id: format!("${name}"),
                field: None,
            })),

            IrExpr::Case {
                operand,
                arms,
                else_expr,
            } => self.lower_case(operand.as_ref().copied(), arms, else_expr.as_ref().copied()),

            IrExpr::ListLiteral(ids) => {
                let elems: Vec<DfExpr> = ids
                    .iter()
                    .map(|&id| self.lower_value(id))
                    .collect::<Result<_, _>>()?;
                Ok(lower_list_literal(elems, self.input_schema.as_deref()))
            }

            IrExpr::MapLiteral(entries) => self.lower_map_literal(entries),

            IrExpr::Quantifier {
                kind,
                loop_var,
                list,
                predicate,
            } => self.lower_quantifier(*kind, *loop_var, *list, *predicate),

            IrExpr::ListComprehension {
                loop_var,
                list,
                filter,
                projection,
            } => self.lower_list_comprehension(*loop_var, *list, *filter, *projection),
        }
    }

    fn lower_subscript(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let [base_id, key_id] = args else {
            return Err(LoweringError::UnsupportedExpr(
                "_subscript expects two arguments".into(),
            ));
        };
        let key_expr = self.lower(*key_id)?;
        if let Some(key) = const_string_key(&key_expr) {
            match key {
                ConstStringKey::Null => return Ok(lit(ScalarValue::Null)),
                ConstStringKey::Value(k) => {
                    if let Some(value) = self.lower_static_value_access(*base_id, &k)? {
                        return Ok(value);
                    }
                }
            }
        }
        if let Some(container) = self.lower_dynamic_access_container(*base_id)? {
            return Ok(CYPHER_VALUE_ACCESS.call(vec![container, key_expr]));
        }
        let base = self.lower(*base_id)?;
        if self.expr_data_type(&base).is_some_and(|dt| {
            !matches!(
                dt,
                DataType::Null
                    | DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _)
            ) && !is_plain_map_struct_type(&dt)
                && !is_het_struct_type(Some(&dt))
        }) {
            return Err(LoweringError::InvalidType(
                "subscript requires a list, map, node, relationship, or null".into(),
            ));
        }
        if self.is_list_typed(&base) {
            match self.expr_data_type(&key_expr) {
                Some(dt) if is_integer_data_type(&dt) => {
                    return Ok(datafusion::functions_nested::expr_fn::array_element(
                        base,
                        one_based_index(key_expr),
                    ));
                }
                Some(DataType::Null) | None => {}
                Some(_) => {
                    return Err(LoweringError::InvalidType(
                        "list subscript index must be an integer or null".into(),
                    ));
                }
            }
        }
        Ok(CYPHER_VALUE_ACCESS.call(vec![base, key_expr]))
    }

    fn lower_static_value_access(
        &self,
        base: ExprId,
        key: &str,
    ) -> Result<Option<DfExpr>, LoweringError> {
        if matches!(self.arena.get(base), IrExpr::Literal(IrLiteral::Null)) {
            return Ok(Some(lit(ScalarValue::Null)));
        }
        if let Some(value) = self.lower_static_indexed_value_access(base, key)? {
            return Ok(Some(value));
        }
        if let Some(value) = self.lower_map_literal_static_field(base, key)? {
            return Ok(Some(value));
        }
        if let IrExpr::VarRef(var_id) = self.arena.get(base)
            && let Some(base_name) = self.var_map.get(*var_id)
        {
            if let Some(value) = self.lower_entity_static_property(*var_id, base_name, key) {
                return Ok(Some(value));
            }
            if let Some(value) = self.lower_struct_static_field(col_literal(base_name), key) {
                return Ok(Some(value));
            }
        }

        let base_expr = self.lower(base)?;
        Ok(self.lower_struct_static_field(base_expr, key))
    }

    fn lower_map_literal_static_field(
        &self,
        base: ExprId,
        key: &str,
    ) -> Result<Option<DfExpr>, LoweringError> {
        let IrExpr::MapLiteral(entries) = self.arena.get(base) else {
            return Ok(None);
        };
        entries
            .iter()
            .find(|(k, _)| k == key)
            .map_or(Ok(Some(lit(ScalarValue::Null))), |(_, id)| {
                self.lower(*id).map(Some)
            })
    }

    fn lower_entity_static_property(&self, var_id: VarId, base: &str, key: &str) -> Option<DfExpr> {
        let qual = datafusion::common::TableReference::bare(base);
        if let Some(schema) = self.input_schema.as_ref() {
            if schema.index_of_column_by_name(Some(&qual), key).is_some() {
                return Some(qualified_col(base, key));
            }
            if self.is_node_var(base) || self.is_edge_var(base) {
                return Some(lit(ScalarValue::Null));
            }
        }
        if let Some(shape) = self.node_shapes.get(&var_id.0) {
            let is_topology = graphforge_ir::arrow_schema::TOPOLOGY_NODES_SCHEMA
                .field_with_name(key)
                .is_ok();
            if is_topology || shape.prop_names.iter().any(|p| p == key) {
                return Some(qualified_col(base, key));
            }
            if self.props_authoritative {
                return Some(lit(ScalarValue::Null));
            }
        }
        None
    }

    fn lower_static_indexed_value_access(
        &self,
        base: ExprId,
        key: &str,
    ) -> Result<Option<DfExpr>, LoweringError> {
        let IrExpr::FunctionCall { name, args } = self.arena.get(base) else {
            return Ok(None);
        };
        if name != "_subscript" {
            return Ok(None);
        }
        let [list_id, index_id] = args.as_slice() else {
            return Ok(None);
        };
        let IrExpr::ListLiteral(items) = self.arena.get(*list_id) else {
            return Ok(None);
        };
        let IrExpr::Literal(IrLiteral::Int(idx)) = self.arena.get(*index_id) else {
            return Ok(None);
        };
        let len = i64::try_from(items.len()).map_err(|_| {
            LoweringError::UnsupportedExpr("list literal length exceeds i64 range".into())
        })?;
        let pos = if *idx < 0 { len + idx } else { *idx };
        if pos < 0 || pos >= len {
            return Ok(Some(lit(ScalarValue::Null)));
        }
        let pos = usize::try_from(pos)
            .map_err(|_| LoweringError::UnsupportedExpr("list index exceeds usize".into()))?;
        self.lower_static_value_access(items[pos], key)
    }

    fn lower_struct_static_field(&self, base_expr: DfExpr, key: &str) -> Option<DfExpr> {
        let dt = self.expr_data_type(&base_expr)?;
        match dt {
            DataType::Null => Some(lit(ScalarValue::Null)),
            dt if is_het_struct_type(Some(&dt)) => Some(
                ScalarUDF::new_from_impl(CypherStaticValueAccess::new(key.to_owned()))
                    .call(vec![base_expr]),
            ),
            DataType::Struct(fields)
                if is_plain_map_struct_type(&DataType::Struct(fields.clone())) =>
            {
                if fields.iter().any(|f| f.name() == key) {
                    Some(datafusion::functions::core::expr_fn::get_field(
                        base_expr,
                        key.to_owned(),
                    ))
                } else {
                    Some(lit(ScalarValue::Null))
                }
            }
            DataType::Struct(fields) => {
                let is_entity = fields
                    .iter()
                    .any(|field| matches!(field.name().as_str(), "node_uuid" | "edge_uuid"));
                if is_entity && fields.iter().any(|field| field.name() == key) {
                    Some(datafusion::functions::core::expr_fn::get_field(
                        base_expr,
                        key.to_owned(),
                    ))
                } else if is_entity {
                    Some(lit(ScalarValue::Null))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn lower_dynamic_access_container(
        &self,
        base: ExprId,
    ) -> Result<Option<DfExpr>, LoweringError> {
        if matches!(self.arena.get(base), IrExpr::Literal(IrLiteral::Null)) {
            return Ok(Some(lit(ScalarValue::Null)));
        }
        if matches!(self.arena.get(base), IrExpr::MapLiteral(_)) {
            return self.lower(base).map(Some);
        }
        if let IrExpr::VarRef(var_id) = self.arena.get(base)
            && let Some(base_name) = self.var_map.get(*var_id)
        {
            if let Some(expr) = self.entity_property_bag(*var_id, base_name) {
                return Ok(Some(expr));
            }
            let base_expr = col_literal(base_name);
            if self.expr_data_type(&base_expr).is_some_and(|dt| {
                matches!(dt, DataType::Null)
                    || is_plain_map_struct_type(&dt)
                    || is_het_struct_type(Some(&dt))
            }) {
                return Ok(Some(base_expr));
            }
        }
        let base_expr = self.lower(base)?;
        Ok(self
            .expr_data_type(&base_expr)
            .is_some_and(|dt| {
                matches!(dt, DataType::Null)
                    || is_plain_map_struct_type(&dt)
                    || is_het_struct_type(Some(&dt))
            })
            .then_some(base_expr))
    }

    fn entity_property_bag(&self, var_id: VarId, base: &str) -> Option<DfExpr> {
        self.entity_property_bag_inner(var_id, base, false)
    }

    fn entity_property_bag_with_empty(&self, var_id: VarId, base: &str) -> Option<DfExpr> {
        self.entity_property_bag_inner(var_id, base, true)
    }

    fn entity_property_bag_inner(
        &self,
        var_id: VarId,
        base: &str,
        empty_map_for_present_entity: bool,
    ) -> Option<DfExpr> {
        use datafusion::functions::core::expr_fn::named_struct;

        let (prop_names, present) = if let Some(shape) = self.node_shapes.get(&var_id.0) {
            let has_node_uuid = self.input_schema.as_ref().is_some_and(|schema| {
                let qual = datafusion::common::TableReference::bare(base);
                schema
                    .index_of_column_by_name(Some(&qual), "node_uuid")
                    .is_some()
            });
            let present = if has_node_uuid {
                col(format!("{base}.node_uuid")).is_not_null()
            } else {
                lit(true)
            };
            (shape.prop_names.clone(), present)
        } else if self.is_edge_var(base) {
            (self.edge_prop_names(base), edge_present_qual(base))
        } else {
            return None;
        };
        if prop_names.is_empty() {
            let value = if empty_map_for_present_entity {
                ScalarUDF::new_from_impl(CypherEntityProperties::new(1)).call(vec![present.clone()])
            } else {
                lit(ScalarValue::Null)
            };
            return Some(null_unless(present, value));
        }
        if empty_map_for_present_entity {
            let mut args = Vec::with_capacity(1 + prop_names.len() * 2);
            args.push(present);
            for prop in prop_names {
                args.push(lit(prop.as_str()));
                args.push(qualified_col(base, &prop));
            }
            return Some(
                ScalarUDF::new_from_impl(CypherEntityProperties::new(args.len())).call(args),
            );
        }
        let mut args = Vec::with_capacity(prop_names.len() * 2);
        for prop in prop_names {
            args.push(lit(prop.as_str()));
            args.push(qualified_col(base, &prop));
        }
        Some(null_unless(present, named_struct(args)))
    }

    /// Lower `all/any/none/single(loop_var IN list WHERE predicate)` (#955) to a
    /// `cypher_quantifier` UDF call. The predicate is lowered with `loop_var`
    /// mapped to a synthetic element column `__gf_elem` and any OUTER variables to
    /// their real columns (correlated quantifiers); the UDF evaluates the
    /// predicate per list element (building a per-element batch) and folds the
    /// booleans with three-valued logic.
    fn lower_quantifier(
        &self,
        kind: graphforge_ir::QuantifierKind,
        loop_var: VarId,
        list: ExprId,
        predicate: ExprId,
    ) -> Result<DfExpr, LoweringError> {
        // One synthetic element column per nesting level (#1021): the outermost
        // loop keeps the historical `__gf_elem`; a nested loop gets
        // `__gf_elem_<depth>` so its binding cannot shadow an enclosing one.
        let elem_name = match self.elem_struct_cols.len() {
            0 => "__gf_elem".to_owned(),
            d => format!("__gf_elem_{d}"),
        };
        let list_expr = self.lower(list)?;
        if let IrExpr::Literal(IrLiteral::Bool(predicate)) = self.arena.get(predicate) {
            let udf =
                ScalarUDF::new_from_impl(CypherInvariantQuantifier::new(kind, Some(*predicate)));
            return Ok(udf.call(vec![list_expr]));
        }
        if matches!(self.arena.get(predicate), IrExpr::Literal(IrLiteral::Null)) {
            let udf = ScalarUDF::new_from_impl(CypherInvariantQuantifier::new(kind, None));
            return Ok(udf.call(vec![list_expr]));
        }
        // Lower the predicate with the loop var bound to the synthetic element
        // column (outer vars keep their real columns — correlation).
        let mut elem_vars = VarMap::new();
        for v in self.var_map.var_ids() {
            if let Some(name) = self.var_map.get(v) {
                elem_vars.insert(v, name.to_owned());
            }
        }
        elem_vars.insert(loop_var, elem_name.as_str());
        let pred_lowerer = {
            let mut l =
                ExprLowerer::with_prop_names(self.arena, &elem_vars, self.prop_names.clone());
            if let Some(s) = self.input_schema.as_ref() {
                l = l.with_input_schema(s.clone());
            }
            if let Some(t) = self.read_target.as_ref() {
                l = l.with_read_target(t.clone());
            }
            // Ancestor element columns stay in scope — an inner predicate may
            // access an OUTER element's struct fields — then this loop's own.
            for c in &self.elem_struct_cols {
                l = l.with_elem_struct_col(c.clone());
            }
            l.with_elem_struct_col(elem_name.clone())
        };
        let pred_expr = pred_lowerer.lower(predicate)?;

        // OUTER columns referenced by the predicate (everything but the element).
        // An enclosing loop's element column counts as outer: it flows in as a
        // UDF argument the enclosing invoke broadcasts per element (#1021).
        let mut outer: Vec<String> = pred_expr
            .column_refs()
            .into_iter()
            .map(|c| c.name.clone())
            .filter(|n| n != &elem_name)
            .collect();
        outer.sort();
        outer.dedup();

        // Plan-time type validation (#955): when the element type is statically
        // known (a literal list, or a typed list column) AND there is no outer
        // correlation to schema-resolve, try to plan the predicate over it. A
        // failure is a genuine Cypher type error (`x % 2` over a string list →
        // `InvalidArgumentType`), surfaced as a `plan error` rather than a
        // runtime capability-gap error so it counts as deliberate validation.
        // A statically-EMPTY list is exempt: the predicate never runs (the result
        // is the trivial `all`/`none` = true, `any`/`single` = false), so its type
        // is irrelevant — `none(x IN [] WHERE x.a = 2)` must not be rejected (#1005).
        if outer.is_empty()
            && !is_empty_list_literal(&list_expr)
            && let Some(elem_type) = self.list_element_type(&list_expr)
        {
            use datafusion::arrow::datatypes::{Field, Schema};
            use datafusion::common::DFSchema;
            use datafusion::logical_expr::execution_props::ExecutionProps;
            use datafusion::physical_expr::create_physical_expr;
            let schema = Schema::new(vec![Field::new(&elem_name, elem_type, true)]);
            if let Ok(df_schema) = DFSchema::try_from(schema)
                && create_physical_expr(&pred_expr, &df_schema, &ExecutionProps::new()).is_err()
            {
                return Err(LoweringError::InvalidType(format!(
                    "quantifier predicate cannot apply to the list's element type ({kind:?})"
                )));
            }
        }

        let mut call_args = Vec::with_capacity(1 + outer.len());
        call_args.push(list_expr);
        for name in &outer {
            call_args.push(col_literal(name));
        }
        let udf =
            ScalarUDF::new_from_impl(CypherQuantifier::new(kind, pred_expr, elem_name, outer));
        Ok(udf.call(call_args))
    }

    /// Lower `[loop_var IN list WHERE filter | projection]` (#955) to a
    /// `CypherListComp` UDF call. Both clauses are lowered over the synthetic
    /// element column `__gf_elem` plus any outer columns they reference
    /// (correlation); the UDF builds a per-element batch per row, filters, maps,
    /// and rebuilds the result `ListArray`.
    #[allow(
        clippy::too_many_lines,
        reason = "schema synthesis, correlation rebinding, and UDF construction stay aligned"
    )]
    fn lower_list_comprehension(
        &self,
        loop_var: VarId,
        list: ExprId,
        filter: Option<ExprId>,
        projection: Option<ExprId>,
    ) -> Result<DfExpr, LoweringError> {
        // One synthetic element column per nesting level (#1021), mirroring
        // `lower_quantifier` — the two forms nest through each other, so they
        // share the same depth-derived naming.
        let elem_name = match self.elem_struct_cols.len() {
            0 => "__gf_elem".to_owned(),
            d => format!("__gf_elem_{d}"),
        };
        let list_expr = self.lower(list)?;
        let clause_schema = self.list_element_type(&list_expr).and_then(|element_type| {
            let mut fields = self.input_schema.as_ref().map_or_else(Vec::new, |schema| {
                schema
                    .iter()
                    .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
                    .collect()
            });
            fields.push((
                None,
                Arc::new(datafusion::arrow::datatypes::Field::new(
                    &elem_name,
                    element_type,
                    true,
                )),
            ));
            datafusion::common::DFSchema::new_with_metadata(fields, HashMap::new())
                .ok()
                .map(Arc::new)
        });

        // Lower the clauses with the loop var bound to the synthetic element
        // column; outer vars keep their real columns (correlation).
        let mut elem_vars = VarMap::new();
        for v in self.var_map.var_ids() {
            if let Some(name) = self.var_map.get(v) {
                elem_vars.insert(v, name.to_owned());
            }
        }
        elem_vars.insert(loop_var, elem_name.as_str());
        let clause_lowerer = {
            let mut l =
                ExprLowerer::with_prop_names(self.arena, &elem_vars, self.prop_names.clone());
            if let Some(s) = clause_schema.as_ref().or(self.input_schema.as_ref()) {
                l = l.with_input_schema(s.clone());
            }
            if let Some(t) = self.read_target.as_ref() {
                l = l.with_read_target(t.clone());
            }
            // Ancestor element columns stay in scope, then this loop's own.
            for c in &self.elem_struct_cols {
                l = l.with_elem_struct_col(c.clone());
            }
            l.with_elem_struct_col(elem_name.clone())
        };
        let mut filter_expr = filter.map(|f| clause_lowerer.lower(f)).transpose()?;
        let mut projection_expr = projection.map(|p| clause_lowerer.lower(p)).transpose()?;

        // OUTER columns referenced by either clause (everything but the element).
        let mut outer_columns = Vec::new();
        for e in [filter_expr.as_ref(), projection_expr.as_ref()]
            .into_iter()
            .flatten()
        {
            for c in e.column_refs() {
                if c.name != elem_name && !outer_columns.contains(c) {
                    outer_columns.push(c.clone());
                }
            }
        }
        outer_columns.sort_by_key(datafusion::common::Column::flat_name);
        let outer = (0..outer_columns.len())
            .map(|index| format!("__gf_outer_{index}"))
            .collect::<Vec<_>>();
        if !outer_columns.is_empty() {
            use datafusion::common::tree_node::{Transformed, TreeNode};
            let rewrite = |expr: DfExpr| {
                expr.transform_up(|expr| {
                    let DfExpr::Column(column) = &expr else {
                        return Ok(Transformed::no(expr));
                    };
                    let Some(index) = outer_columns.iter().position(|outer| outer == column) else {
                        return Ok(Transformed::no(expr));
                    };
                    Ok(Transformed::yes(DfExpr::Column(
                        datafusion::common::Column::from_name(outer[index].clone()),
                    )))
                })
                .map(|transformed| transformed.data)
            };
            filter_expr = filter_expr
                .map(&rewrite)
                .transpose()
                .map_err(|error| LoweringError::UnsupportedExpr(error.to_string()))?;
            projection_expr = projection_expr
                .map(rewrite)
                .transpose()
                .map_err(|error| LoweringError::UnsupportedExpr(error.to_string()))?;
        }

        // Plan-time validation (#955), mirroring `lower_quantifier`: when the
        // element type is statically known and there is no outer correlation,
        // confirm the filter predicate can actually plan over that element type.
        // A failure (`WHERE x % 2 = 0` over a string list) is a genuine Cypher
        // type error, surfaced as a clean `plan error` rather than a runtime
        // failure inside the UDF.
        if let Some(fexpr) = filter_expr.as_ref()
            && outer.is_empty()
            && let Some(elem_type) = self.list_element_type(&list_expr)
        {
            use datafusion::arrow::datatypes::{Field, Schema};
            use datafusion::common::DFSchema;
            use datafusion::logical_expr::execution_props::ExecutionProps;
            use datafusion::physical_expr::create_physical_expr;
            let schema = Schema::new(vec![Field::new(&elem_name, elem_type, true)]);
            if let Ok(df_schema) = DFSchema::try_from(schema)
                && create_physical_expr(fexpr, &df_schema, &ExecutionProps::new()).is_err()
            {
                return Err(LoweringError::InvalidType(
                    "list comprehension filter cannot apply to the list's element type".into(),
                ));
            }
        }

        let mut call_args = Vec::with_capacity(1 + outer.len());
        call_args.push(list_expr);
        for column in &outer_columns {
            call_args.push(DfExpr::Column(column.clone()));
        }
        let udf = ScalarUDF::new_from_impl(CypherListComp::new(
            filter_expr,
            projection_expr,
            elem_name,
            outer,
        ));
        Ok(udf.call(call_args))
    }

    /// Lower a map literal `{k: v, …}` to an Arrow `Struct` via `named_struct`
    /// (keys become field names, values the fields) — the same representation
    /// node/relationship property bags use, so `m.k` resolves through
    /// `resolve_prop_col`'s `get_field` fallback and the renderer prints it as
    /// `{k: v, …}` (#600). An empty map `{}` builds an empty struct.
    ///
    /// A constant map stays a `named_struct` call here (so an all-map list keeps
    /// its `make_array` coercion path, #1004); `lower_list_literal` folds it to a
    /// `ScalarValue::Struct` on demand when a mixed list needs the tagged het path
    /// (via [`list_values::try_const_scalar`], #1005).
    fn lower_map_literal(&self, entries: &[(String, ExprId)]) -> Result<DfExpr, LoweringError> {
        use datafusion::functions::core::expr_fn::named_struct;
        if entries.is_empty() {
            // `named_struct()` rejects zero args; an empty map is an empty struct.
            return Ok(empty_map_struct());
        }
        let mut args: Vec<DfExpr> = Vec::with_capacity(entries.len() * 2);
        for (key, value) in entries {
            args.push(lit(key.as_str()));
            args.push(self.lower_value(*value)?);
        }
        Ok(named_struct(args))
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// `keys(map|node|relationship)` — map keys include null-valued entries;
    /// entity keys include only non-null stored property columns for each row.
    fn lower_keys(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        use datafusion::functions_nested::expr_fn::{array_concat, make_array};

        let Some(&base_id) = args.first() else {
            return Err(LoweringError::UnsupportedExpr(
                "keys() expects one argument".into(),
            ));
        };
        if let Some(map_keys) = self.lower_map_keys(base_id)? {
            return Ok(map_keys);
        }
        let var_id = match self.arena.get(base_id) {
            IrExpr::VarRef(var_id) => *var_id,
            _ => {
                return Err(LoweringError::InvalidType(
                    "keys() requires a map, node, relationship, or null".into(),
                ));
            }
        };
        let base = self
            .var_map
            .get(var_id)
            .ok_or(LoweringError::UnboundVar(var_id.0))?;
        let (prop_names, present) = if let Some(shape) = self.node_shapes.get(&var_id.0) {
            let has_node_uuid = self.input_schema.as_ref().is_some_and(|schema| {
                let qual = datafusion::common::TableReference::bare(base);
                schema
                    .index_of_column_by_name(Some(&qual), "node_uuid")
                    .is_some()
            });
            let node_present = if has_node_uuid {
                col(format!("{base}.node_uuid")).is_not_null()
            } else {
                lit(true)
            };
            (shape.prop_names.clone(), node_present)
        } else if self.is_edge_var(base) {
            (self.edge_prop_names(base), edge_present_qual(base))
        } else {
            return Err(LoweringError::UnsupportedExpr(
                "keys() requires an entity with a known shape".into(),
            ));
        };
        // An empty `List<Utf8>` — the result for a node with no properties, and
        // the "absent" branch each property folds in.
        let empty = empty_utf8_list();
        let parts: Vec<DfExpr> = prop_names
            .iter()
            .map(|p| {
                when(
                    qualified_col(base, p).is_not_null(),
                    make_array(vec![lit(p.as_str())]),
                )
                .otherwise(empty.clone())
                .expect("CASE build")
            })
            .collect();
        let value = parts
            .into_iter()
            .reduce(|acc, part| array_concat(vec![acc, part]))
            .unwrap_or(empty);
        Ok(null_unless(present, value))
    }

    fn lower_map_keys(&self, base_id: ExprId) -> Result<Option<DfExpr>, LoweringError> {
        match self.arena.get(base_id) {
            IrExpr::Literal(IrLiteral::Null) => return Ok(Some(null_utf8_list())),
            IrExpr::MapLiteral(_) => {
                return Ok(Some(CYPHER_MAP_KEYS.call(vec![self.lower(base_id)?])));
            }
            IrExpr::ListLiteral(_) => {
                return Err(LoweringError::InvalidType(
                    "keys() requires a map, node, relationship, or null".into(),
                ));
            }
            _ => {}
        }
        if let IrExpr::VarRef(var_id) = self.arena.get(base_id)
            && let Some(base) = self.var_map.get(*var_id)
        {
            if self.node_shapes.contains_key(&var_id.0) || self.is_edge_var(base) {
                return Ok(None);
            }
            if let Some(schema) = self.input_schema.as_ref()
                && let Ok(field) = schema.field_with_unqualified_name(base)
            {
                if matches!(field.data_type(), DataType::Null)
                    || is_plain_map_struct_type(field.data_type())
                    || is_het_struct_type(Some(field.data_type()))
                {
                    return Ok(Some(CYPHER_MAP_KEYS.call(vec![col_literal(base)])));
                }
                return Ok(None);
            }
        }
        let value = self.lower(base_id)?;
        if let Some(dt) = self.expr_data_type(&value) {
            if matches!(dt, DataType::Null)
                || is_plain_map_struct_type(&dt)
                || is_het_struct_type(Some(&dt))
            {
                return Ok(Some(CYPHER_MAP_KEYS.call(vec![value])));
            }
            return Err(LoweringError::InvalidType(
                "keys() requires a map, node, relationship, or null".into(),
            ));
        }
        Ok(Some(CYPHER_MAP_KEYS.call(vec![value])))
    }

    fn lower_properties(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let Some(&base_id) = args.first() else {
            return Err(LoweringError::UnsupportedExpr(
                "properties() expects one argument".into(),
            ));
        };
        match self.arena.get(base_id) {
            IrExpr::Literal(IrLiteral::Null) => return Ok(lit(ScalarValue::Null)),
            IrExpr::MapLiteral(_) => return self.lower(base_id),
            IrExpr::ListLiteral(_) => {
                return Err(LoweringError::InvalidType(
                    "properties() requires a map, node, relationship, or null".into(),
                ));
            }
            _ => {}
        }
        if let IrExpr::VarRef(var_id) = self.arena.get(base_id)
            && let Some(base) = self.var_map.get(*var_id)
        {
            if let Some(value) = self.entity_property_bag_with_empty(*var_id, base) {
                return Ok(value);
            }
            if let Some(schema) = self.input_schema.as_ref()
                && let Ok(field) = schema.field_with_unqualified_name(base)
            {
                return match field.data_type() {
                    DataType::Null => Ok(lit(ScalarValue::Null)),
                    dt if is_plain_map_struct_type(dt) => Ok(col_literal(base)),
                    _ => Err(LoweringError::InvalidType(
                        "properties() requires a map, node, relationship, or null".into(),
                    )),
                };
            }
        }
        let value = self.lower(base_id)?;
        if let Some(dt) = self.expr_data_type(&value) {
            return match dt {
                DataType::Null => Ok(lit(ScalarValue::Null)),
                dt if is_plain_map_struct_type(&dt) => Ok(value),
                _ => Err(LoweringError::InvalidType(
                    "properties() requires a map, node, relationship, or null".into(),
                )),
            };
        }
        Ok(value)
    }

    /// `col("var_<v>.node_uuid")` / `col("var_<v>.edge_uuid")` when `id` is a
    /// bare `VarRef` to an entity variable — the entity's identity, for
    /// comparisons. A bare entity var is a multi-column qualifier with no scalar
    /// lowering, so its identity column is the comparison contract (#598/#962).
    fn identity_uuid_of(&self, id: ExprId) -> Option<(EntityIdentityKind, DfExpr)> {
        if let IrExpr::VarRef(v) = self.arena.get(id)
            && self.node_shapes.contains_key(&v.0)
        {
            let base = self.var_map.get(*v)?;
            return Some((EntityIdentityKind::Node, col(format!("{base}.node_uuid"))));
        }
        if let IrExpr::VarRef(v) = self.arena.get(id) {
            let base = self.var_map.get(*v)?;
            let qual = datafusion::common::TableReference::bare(base);
            if let Some(schema) = self.input_schema.as_ref()
                && schema
                    .index_of_column_by_name(Some(&qual), "edge_uuid")
                    .is_some()
            {
                return Some((EntityIdentityKind::Edge, col(format!("{base}.edge_uuid"))));
            }
        }
        None
    }

    /// Whether a lowered expression is list-typed — a list literal, or a column
    /// whose type in the input schema is a `List`/`LargeList`/`FixedSizeList`.
    /// Drives `+`'s list-concatenation path (#957).
    fn is_list_typed(&self, e: &DfExpr) -> bool {
        if is_list_literal(e) {
            return true;
        }
        if let Some(schema) = self.input_schema.as_ref()
            && let Ok(dt) = e.get_type(schema)
        {
            return matches!(
                dt,
                DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
            );
        }
        false
    }

    /// Whether a lowered expression is string-typed — a `Utf8` literal, or a
    /// column whose type in the input schema is `Utf8`/`LargeUtf8`. Drives `+`'s
    /// string-concatenation path (#957).
    fn is_string_typed(&self, e: &DfExpr) -> bool {
        if matches!(
            e,
            DfExpr::Literal(ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_), _)
        ) {
            return true;
        }
        if let Some(schema) = self.input_schema.as_ref()
            && let Ok(dt) = e.get_type(schema)
        {
            return matches!(
                dt,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            );
        }
        false
    }

    fn is_known_non_string(&self, e: &DfExpr) -> bool {
        if is_list_literal(e)
            || matches!(e, DfExpr::ScalarFunction(f) if f.func.name() == "named_struct")
        {
            return true;
        }
        self.expr_data_type(e).is_some_and(|dt| {
            !matches!(
                dt,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
            )
        })
    }

    /// The Arrow type of a lowered expression — a literal's type, or a column's
    /// type from the input schema. Drives temporal±duration dispatch (#920).
    fn expr_data_type(&self, e: &DfExpr) -> Option<DataType> {
        if let DfExpr::Literal(sv, _) = e {
            return Some(sv.data_type());
        }
        self.input_schema.as_ref().and_then(|s| e.get_type(s).ok())
    }

    /// Whether a lowered expression's type is statically KNOWN to be
    /// non-boolean — so a boolean operator (`AND`/`OR`/`XOR`/`NOT`) over it is a
    /// compile-time type error (openCypher `InvalidArgumentType`, #956).
    ///
    /// Conservative: an UNKNOWN type (a parameter, or an untyped property with
    /// no input-schema entry) returns `false` — Cypher rejects only a PROVEN
    /// mismatch. `Null` also returns `false` (three-valued logic: `null AND x`
    /// is valid).
    fn is_known_non_bool(&self, e: &DfExpr) -> bool {
        // A list or map literal is a known composite value, never a boolean
        // (`NOT {k: v}` / `[1] AND x`). A map lowers to a `named_struct` call,
        // whose type `expr_data_type` cannot see, so match it directly.
        if is_list_literal(e)
            || matches!(e, DfExpr::ScalarFunction(f) if f.func.name() == "named_struct")
        {
            return true;
        }
        !matches!(
            self.expr_data_type(e),
            None | Some(DataType::Boolean | DataType::Null)
        )
    }

    /// Whether a lowered expression's type is statically KNOWN to be
    /// non-numeric — so arithmetic that requires a number (unary `-`, `%`, `^`)
    /// over it is a compile-time type error (#956). Unknown types and `Null`
    /// return `false` (same conservative rule as [`is_known_non_bool`]).
    fn is_known_non_numeric(&self, e: &DfExpr) -> bool {
        match self.expr_data_type(e) {
            None | Some(DataType::Null) => false,
            Some(dt) => !dt.is_numeric(),
        }
    }

    fn is_known_non_list(&self, e: &DfExpr) -> bool {
        if matches!(e, DfExpr::ScalarFunction(f) if f.func.name() == "named_struct") {
            return true;
        }
        match self.expr_data_type(e) {
            None
            | Some(
                DataType::Null
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _),
            ) => false,
            Some(dt) if is_het_struct_type(Some(&dt)) => false,
            Some(_) => true,
        }
    }

    /// Whether a lowered expression is a typed temporal value (date / localtime /
    /// time / localdatetime / datetime). (#920)
    fn is_temporal_typed(&self, e: &DfExpr) -> bool {
        match self.expr_data_type(e) {
            Some(DataType::Time64(_)) => true,
            Some(dt) => {
                is_date_struct(&dt)
                    || is_localdatetime_struct(&dt)
                    || is_time_struct(&dt)
                    || is_datetime_struct(&dt)
            }
            None => false,
        }
    }

    fn is_known_non_value_access_container(&self, e: &DfExpr) -> bool {
        match self.expr_data_type(e) {
            None | Some(DataType::Null) => false,
            Some(dt) => {
                !is_plain_map_struct_type(&dt)
                    && !is_het_struct_type(Some(&dt))
                    && !matches!(dt, DataType::Struct(_))
            }
        }
    }

    /// Whether a lowered expression is a typed `duration` struct. (#920)
    fn is_duration_typed(&self, e: &DfExpr) -> bool {
        self.expr_data_type(e)
            .is_some_and(|dt| is_duration_struct(&dt))
    }

    /// The element type of a list expression, when statically known (a literal
    /// list or a typed list column). Drives plan-time quantifier validation (#955).
    fn list_element_type(&self, list: &DfExpr) -> Option<DataType> {
        let schema = self
            .input_schema
            .clone()
            .unwrap_or_else(|| std::sync::Arc::new(datafusion::common::DFSchema::empty()));
        match list.get_type(&schema).ok()? {
            DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
                Some(f.data_type().clone())
            }
            _ => None,
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one arm per Cypher binary operator, several with temporal/list/string dispatch"
    )]
    fn lower_binary(
        &self,
        op: BinaryOpKind,
        left: ExprId,
        right: ExprId,
    ) -> Result<DfExpr, LoweringError> {
        // Multi-label patterns arrive as `'<label>' IN labels(node)` for every
        // label after the first. Recognize that raw IR shape before lowering
        // either operand: lowering `labels(node)` first materializes the full
        // runtime-catalog label list, once per predicate, and makes plan work
        // quadratic in the number of labels (#1275).
        if op == BinaryOpKind::In
            && let Some(membership) = self.lower_known_label_membership(left, right)?
        {
            return Ok(membership);
        }

        // Entity identity comparison: `a = b` / `a <> b` over node/relationship
        // variables compares UUID columns (#598/#962). Different entity kinds are
        // never equal, but optional nulls still propagate as Cypher null.
        if matches!(op, BinaryOpKind::Eq | BinaryOpKind::Neq)
            && let (Some((lk, lc)), Some((rk, rc))) =
                (self.identity_uuid_of(left), self.identity_uuid_of(right))
        {
            if lk != rk {
                let value = matches!(op, BinaryOpKind::Neq);
                return Ok(when(
                    lc.clone().is_null().or(rc.clone().is_null()),
                    lit(ScalarValue::Boolean(None)),
                )
                .otherwise(lit(value))
                .expect("CASE build is infallible for mismatched entity comparison"));
            }
            return Ok(if matches!(op, BinaryOpKind::Eq) {
                lc.eq(rc)
            } else {
                lc.not_eq(rc)
            });
        }
        let l = self.lower(left)?;
        let r = self.lower(right)?;
        let expr = match op {
            // Cypher equality is type-tolerant: comparing values of different
            // types is `false` (`<>` → `true`), never an error — unlike SQL `=`,
            // which DataFusion rejects at planning for incompatible types. Route
            // through `cypher_eq` (ADR 0009); `<>` is its three-valued negation
            // (`not(null)` stays `null`).
            BinaryOpKind::Eq => CYPHER_EQ.call(vec![l, r]),
            BinaryOpKind::Neq => datafusion::logical_expr::not(CYPHER_EQ.call(vec![l, r])),
            // Order comparisons are Cypher comparability, not SQL ordering:
            // cross-type ordering is null, numeric NaN comparisons are false, and
            // lists compare lexicographically. Route all four operators through a
            // boolean UDF instead of native DataFusion comparisons (#962).
            BinaryOpKind::Lt | BinaryOpKind::Lte | BinaryOpKind::Gt | BinaryOpKind::Gte => {
                let code = match op {
                    BinaryOpKind::Lt => 0i8,
                    BinaryOpKind::Lte => 1i8,
                    BinaryOpKind::Gt => 2i8,
                    _ => 3i8,
                };
                CYPHER_CMP_PRED.call(vec![l, r, lit(code)])
            }
            // Boolean operators require boolean (or null/unknown) operands. A
            // proven non-boolean is an openCypher InvalidArgumentType (#956);
            // route it to a clean `plan error` rather than a DataFusion coercion
            // failure. Keep XOR as one UDF node rather than expanding it into
            // shared AND/OR subtrees, which makes chained lowering exponential.
            BinaryOpKind::And | BinaryOpKind::Or | BinaryOpKind::Xor => {
                let keyword = match op {
                    BinaryOpKind::And => "AND",
                    BinaryOpKind::Or => "OR",
                    BinaryOpKind::Xor => "XOR",
                    _ => unreachable!("matched boolean operator"),
                };
                if self.is_known_non_bool(&l) || self.is_known_non_bool(&r) {
                    return Err(LoweringError::InvalidType(format!(
                        "{keyword} requires boolean operands"
                    )));
                }
                match op {
                    BinaryOpKind::And => CYPHER_AND.call(vec![l, r]),
                    BinaryOpKind::Or => CYPHER_OR.call(vec![l, r]),
                    BinaryOpKind::Xor => CYPHER_XOR.call(vec![l, r]),
                    _ => unreachable!("matched boolean operator"),
                }
            }
            BinaryOpKind::Add => {
                // Cypher `+` is polymorphic: two lists CONCATENATE (`[1,2] + [3,4]`
                // → `[1,2,3,4]`), two strings CONCATENATE (`'a' + 'b'` → `'ab'`),
                // and numbers add. DataFusion's `Plus` only does arithmetic (and
                // rejects lists/strings at planning), so route list operands to
                // `array_concat` and string operands to the null-propagating `||`
                // (`StringConcat`) — Cypher `+` with a null operand is null, which
                // `||` matches (unlike `concat`, which skips nulls).
                if self.is_temporal_typed(&l) && self.is_duration_typed(&r) {
                    // temporal + duration (#920)
                    CYPHER_TEMPORAL_ARITH.call(vec![l, r, lit(1i64)])
                } else if self.is_duration_typed(&l) && self.is_temporal_typed(&r) {
                    // duration + temporal (commutative)
                    CYPHER_TEMPORAL_ARITH.call(vec![r, l, lit(1i64)])
                } else if self.is_duration_typed(&l) && self.is_duration_typed(&r) {
                    // duration + duration (component-wise)
                    CYPHER_DURATION_ADD.call(vec![l, r, lit(1i64)])
                } else if let (Some(le), Some(re)) =
                    (self.list_element_type(&l), self.list_element_type(&r))
                {
                    if le == re && !is_het_struct_type(Some(&le)) {
                        datafusion::functions_nested::expr_fn::array_concat(vec![l, r])
                    } else if graph_value_types_compatible(&le, &re) {
                        // DF54: cast both sides to the nullability-widened
                        // common type — never narrow nested fields (#467).
                        let left_ty = self.expr_data_type(&l).ok_or_else(|| {
                            LoweringError::UnsupportedExpr(
                                "cannot resolve path-list type for concatenation".into(),
                            )
                        })?;
                        let right_ty = self.expr_data_type(&r).unwrap_or_else(|| left_ty.clone());
                        let target = unify_graph_value_nullability(&left_ty, &right_ty)
                            .unwrap_or_else(|| left_ty.clone());
                        let left = if left_ty == target {
                            l
                        } else {
                            cast(l, target.clone())
                        };
                        let right = if right_ty == target {
                            r
                        } else {
                            cast(r, target)
                        };
                        datafusion::functions_nested::expr_fn::array_concat(vec![left, right])
                    } else {
                        CYPHER_LIST_PLUS.call(vec![l, r])
                    }
                } else if let Some(le) = self.list_element_type(&l) {
                    if !is_het_struct_type(Some(&le))
                        && self
                            .expr_data_type(&r)
                            .is_some_and(|rt| rt == le || matches!(rt, DataType::Null))
                    {
                        datafusion::functions_nested::expr_fn::array_append(l, r)
                    } else {
                        CYPHER_LIST_PLUS.call(vec![l, r])
                    }
                } else if let Some(re) = self.list_element_type(&r) {
                    if !is_het_struct_type(Some(&re))
                        && self
                            .expr_data_type(&l)
                            .is_some_and(|lt| lt == re || matches!(lt, DataType::Null))
                    {
                        datafusion::functions_nested::expr_fn::array_prepend(l, r)
                    } else {
                        CYPHER_LIST_PLUS.call(vec![l, r])
                    }
                } else if self.is_list_typed(&l) || self.is_list_typed(&r) {
                    CYPHER_LIST_PLUS.call(vec![l, r])
                } else if (self.is_string_typed(&l) && !self.is_known_non_string(&r))
                    || (self.is_string_typed(&r) && !self.is_known_non_string(&l))
                {
                    DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                        left: Box::new(l),
                        op: Operator::StringConcat,
                        right: Box::new(r),
                    })
                } else {
                    DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                        left: Box::new(l),
                        op: Operator::Plus,
                        right: Box::new(r),
                    })
                }
            }
            BinaryOpKind::Sub => {
                if self.is_temporal_typed(&l) && self.is_duration_typed(&r) {
                    // temporal - duration (#920)
                    CYPHER_TEMPORAL_ARITH.call(vec![l, r, lit(-1i64)])
                } else if self.is_duration_typed(&l) && self.is_duration_typed(&r) {
                    // duration - duration (component-wise)
                    CYPHER_DURATION_ADD.call(vec![l, r, lit(-1i64)])
                } else {
                    DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                        left: Box::new(l),
                        op: Operator::Minus,
                        right: Box::new(r),
                    })
                }
            }
            BinaryOpKind::Mul => {
                // `duration * number` (commutative) scales the duration (#920).
                if self.is_duration_typed(&l) {
                    CYPHER_DURATION_SCALE.call(vec![l, r, lit(false)])
                } else if self.is_duration_typed(&r) {
                    CYPHER_DURATION_SCALE.call(vec![r, l, lit(false)])
                } else {
                    DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                        left: Box::new(l),
                        op: Operator::Multiply,
                        right: Box::new(r),
                    })
                }
            }
            BinaryOpKind::Div => {
                // `duration / number` scales the duration (not commutative) (#920).
                if self.is_duration_typed(&l) {
                    CYPHER_DURATION_SCALE.call(vec![l, r, lit(true)])
                } else {
                    let (l, r) = match (self.expr_data_type(&l), self.expr_data_type(&r)) {
                        (Some(DataType::Float64), Some(rt)) if is_integer_data_type(&rt) => {
                            (l, cast(r, DataType::Float64))
                        }
                        (Some(lt), Some(DataType::Float64)) if is_integer_data_type(&lt) => {
                            (cast(l, DataType::Float64), r)
                        }
                        _ => (l, r),
                    };
                    DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                        left: Box::new(l),
                        op: Operator::Divide,
                        right: Box::new(r),
                    })
                }
            }
            BinaryOpKind::Mod => {
                if self.is_known_non_numeric(&l) || self.is_known_non_numeric(&r) {
                    return Err(LoweringError::InvalidType(
                        "% requires numeric operands".into(),
                    ));
                }
                DfExpr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                    left: Box::new(l),
                    op: Operator::Modulo,
                    right: Box::new(r),
                })
            }
            BinaryOpKind::Pow => {
                if self.is_known_non_numeric(&l) || self.is_known_non_numeric(&r) {
                    return Err(LoweringError::InvalidType(
                        "^ requires numeric operands".into(),
                    ));
                }
                datafusion::functions::math::expr_fn::power(l, r)
            }
            // Cypher `x IN list` is structural three-valued list MEMBERSHIP, never
            // SQL's `in_list` (which treats the whole list as one element).
            // Statically known non-lists are compile-time InvalidArgumentType;
            // parameters/untyped values stay conservative and dispatch at runtime.
            BinaryOpKind::In => {
                if self.is_known_non_list(&r) {
                    return Err(LoweringError::InvalidType(
                        "IN requires a list or null right-hand operand".into(),
                    ));
                }
                CYPHER_IN.call(vec![l, r])
            }
            BinaryOpKind::StartsWith => CYPHER_STARTS_WITH.call(vec![l, r]),
            BinaryOpKind::EndsWith => CYPHER_ENDS_WITH.call(vec![l, r]),
            BinaryOpKind::Contains => CYPHER_CONTAINS.call(vec![l, r]),
            BinaryOpKind::RegexMatch => {
                // DataFusion regexp_like(str, pattern)
                datafusion::functions::regex::expr_fn::regexp_like(l, r, None)
            }
        };
        Ok(expr)
    }

    /// Lower a known literal label-membership predicate directly against the
    /// node topology's canonical `type_ids` list.
    ///
    /// Unknown literals and dynamic expressions deliberately return `None` so
    /// the generic three-valued `cypher_in` path remains authoritative.
    fn lower_known_label_membership(
        &self,
        left: ExprId,
        right: ExprId,
    ) -> Result<Option<DfExpr>, LoweringError> {
        use datafusion::functions_nested::expr_fn::array_has;

        let IrExpr::Literal(IrLiteral::Str(label)) = self.arena.get(left) else {
            return Ok(None);
        };
        let IrExpr::FunctionCall { name, args } = self.arena.get(right) else {
            return Ok(None);
        };
        let [arg] = args.as_slice() else {
            return Ok(None);
        };
        if name != "labels" {
            return Ok(None);
        }
        let IrExpr::VarRef(var_id) = self.arena.get(*arg) else {
            return Ok(None);
        };
        let Some(type_id) = self.entity_name_to_type_id.get(label) else {
            return Ok(None);
        };
        let base = self
            .var_map
            .get(*var_id)
            .ok_or(LoweringError::UnboundVar(var_id.0))?;
        Ok(Some(array_has(
            col(format!("{base}.type_ids")),
            lit(type_id.encode()),
        )))
    }

    fn lower_unary(&self, op: UnaryOpKind, expr: ExprId) -> Result<DfExpr, LoweringError> {
        let e = self.lower(expr)?;
        let result = match op {
            UnaryOpKind::Not => {
                if self.is_known_non_bool(&e) {
                    return Err(LoweringError::InvalidType(
                        "NOT requires a boolean operand".into(),
                    ));
                }
                not(e)
            }
            UnaryOpKind::Neg => {
                if self.is_known_non_numeric(&e) {
                    return Err(LoweringError::InvalidType(
                        "unary minus requires a numeric operand".into(),
                    ));
                }
                DfExpr::Negative(Box::new(e))
            }
            UnaryOpKind::IsNull => e.is_null(),
            UnaryOpKind::IsNotNull => e.is_not_null(),
        };
        Ok(result)
    }

    fn lower_case(
        &self,
        operand: Option<ExprId>,
        arms: &[graphforge_ir::expr::CaseArm],
        else_expr: Option<ExprId>,
    ) -> Result<DfExpr, LoweringError> {
        let when_thens: Result<Vec<_>, _> = arms
            .iter()
            .map(|arm| {
                let when = self.lower(arm.when)?;
                let then = self.lower(arm.then)?;
                Ok((Box::new(when), Box::new(then)))
            })
            .collect();
        let when_thens = when_thens?;
        let else_expr_df = else_expr.map(|id| self.lower(id)).transpose()?;

        Ok(DfExpr::Case(datafusion::logical_expr::expr::Case {
            expr: operand.map(|id| self.lower(id)).transpose()?.map(Box::new),
            when_then_expr: when_thens,
            else_expr: else_expr_df.map(Box::new),
        }))
    }

    /// Build a DataFusion column reference for a property access.
    ///
    /// If `base` resolved to a plain `col("a")`, the property column is
    /// `col("a.prop_name")`.  Falls back to `"prop_<id>"` for runtime-catalog
    /// properties not present in the ontology.
    fn resolve_prop_col(&self, base_expr: DfExpr, prop: PropertyId) -> DfExpr {
        let prop_name = self
            .prop_names
            .get(&prop)
            .cloned()
            .unwrap_or_else(|| format!("prop_{prop}"));

        // If the base is a plain column, compose a dotted column name — UNLESS it
        // is a synthetic quantifier/comprehension element column (#1004) at any
        // nesting depth (#1021), whose fields are struct fields, not top-level
        // property columns: access those via struct-aware `get_field` so `x.a` in
        // `none(x IN [{a:2}] WHERE x.a=2)` resolves against the element's `Struct`
        // type rather than a missing dotted column `__gf_elem.a`.
        if let DfExpr::Column(col_ref) = &base_expr
            && !self
                .elem_struct_cols
                .iter()
                .any(|c| c == col_ref.name.as_str())
        {
            return qualified_col(&col_ref.name, &prop_name);
        }

        // Fallback: get_field(base, "prop_name") — handles computed bases and the
        // struct-element column above.
        datafusion::functions::core::expr_fn::get_field(base_expr, prop_name)
    }
}

// ---------------------------------------------------------------------------
// Free functions
// ---------------------------------------------------------------------------

/// Reference a column by its LITERAL name, preserving case (#957).
///
/// `col(name)` runs DataFusion's SQL-identifier parser, which **lowercases**
/// unquoted identifiers — so a mixed-case alias (`WITH v AS otherDate`) becomes
/// `otherdate` and fails to resolve. A simple (undotted) name is therefore built
/// as an unqualified [`Column`] verbatim. A dotted name keeps `col()`'s parsing,
/// preserving the lowercase dotted-property-column scheme (`graphforge-plan`).
fn col_literal(name: &str) -> DfExpr {
    if name.contains('.') {
        col(name)
    } else {
        DfExpr::Column(datafusion::common::Column::new_unqualified(name))
    }
}

pub(crate) fn qualified_col(relation: &str, name: &str) -> DfExpr {
    DfExpr::Column(datafusion::common::Column::new(
        Some(datafusion::common::TableReference::bare(relation)),
        name,
    ))
}

/// Build a `PropId.0 → column_name` reverse map.
///
/// The binder interns properties into the [`RuntimeCatalog`](graphforge_ir::RuntimeCatalog)
/// (runtime `PropId`s) in every mode that admits property reads, so the
/// authoritative name map comes from there, supplied by the
/// [`GraphPlanLowerer`](crate::GraphPlanLowerer) via
/// [`with_prop_names`](ExprLowerer::with_prop_names). This ontology-only
/// constructor path has no runtime catalog, so it returns an empty map and
/// property accesses fall back to `"prop_<id>"`.
fn build_prop_names(_ontology: Option<&OntologyHandle>) -> HashMap<PropertyId, String> {
    HashMap::new()
}

/// `ScalarValue::List` whose single row has zero elements. Used to exempt an empty
/// list from quantifier plan-time type validation (its predicate never runs).
fn is_empty_list_literal(e: &DfExpr) -> bool {
    use datafusion::arrow::array::Array;
    matches!(e, DfExpr::Literal(ScalarValue::List(arr), _) if arr.value(0).is_empty())
}

enum ConstStringKey {
    Null,
    Value(String),
}

fn const_string_key(e: &DfExpr) -> Option<ConstStringKey> {
    match e {
        DfExpr::Literal(ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v), _) => Some(
            v.clone()
                .map_or(ConstStringKey::Null, ConstStringKey::Value),
        ),
        DfExpr::BinaryExpr(b) if b.op == Operator::StringConcat => {
            let l = const_string_key(&b.left)?;
            let r = const_string_key(&b.right)?;
            Some(match (l, r) {
                (ConstStringKey::Value(l), ConstStringKey::Value(r)) => {
                    ConstStringKey::Value(format!("{l}{r}"))
                }
                _ => ConstStringKey::Null,
            })
        }
        _ => None,
    }
}

static CYPHER_RELATIONSHIP_DISJOINT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherRelationshipDisjoint::new()));

pub(crate) fn relationship_disjoint(left: DfExpr, right: DfExpr) -> DfExpr {
    CYPHER_RELATIONSHIP_DISJOINT.call(vec![left, right])
}

// Exact fixed-hop residual admitted by the input-predicate optimizer. Do not
// identify UDFs by name: another implementation may use the same name.
pub(crate) fn is_fixed_relationship_disjoint(
    expr: &DfExpr,
    schema: &datafusion::common::DFSchema,
) -> bool {
    use datafusion::logical_expr::ExprSchemable;
    let DfExpr::ScalarFunction(function) = expr else {
        return false;
    };
    function.func.inner().downcast_ref::<CypherRelationshipDisjoint>().is_some()
        && function.args.len() == 2
        && function.args.iter().all(|arg| {
            matches!(arg, DfExpr::Column(column) if column.relation.is_some() && column.name == "edge_id")
                && arg.get_type(schema).ok() == Some(DataType::UInt64)
        })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherRelationshipDisjoint {
    signature: Signature,
}

impl CypherRelationshipDisjoint {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherRelationshipDisjoint {
    fn name(&self) -> &'static str {
        "cypher_relationship_disjoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::BooleanArray;

        validate_heterogeneous_arguments(&args.args)?;
        let rows = args.number_rows;
        let left = args.args[0].to_array(rows)?;
        let right = args.args[1].to_array(rows)?;
        let values = (0..rows)
            .map(|row| {
                let left = ScalarValue::try_from_array(&left, row)?;
                let right = ScalarValue::try_from_array(&right, row)?;
                let mut left_ids = Vec::new();
                let mut right_ids = Vec::new();
                relationship_ids(&left, &mut left_ids);
                relationship_ids(&right, &mut right_ids);
                Ok(!left_ids.iter().any(|id| right_ids.contains(id)))
            })
            .collect::<datafusion::error::Result<BooleanArray>>()?;
        Ok(ColumnarValue::Array(std::sync::Arc::new(values)))
    }
}

fn relationship_ids(value: &ScalarValue, ids: &mut Vec<Vec<u8>>) {
    match value {
        ScalarValue::FixedSizeBinary(_, Some(uuid)) => ids.push(uuid.clone()),
        ScalarValue::UInt64(Some(edge_id)) => ids.push(edge_id.to_le_bytes().to_vec()),
        ScalarValue::List(list) if !list.is_null(0) => {
            let values = list.value(0);
            for index in 0..values.len() {
                if let Ok(value) = ScalarValue::try_from_array(&values, index) {
                    relationship_ids(&value, ids);
                }
            }
        }
        ScalarValue::Struct(value) if !value.is_null(0) => {
            if let Some(uuid) = value.column_by_name("edge_uuid")
                && let Ok(uuid) = ScalarValue::try_from_array(uuid, 0)
            {
                relationship_ids(&uuid, ids);
            }
        }
        _ => {}
    }
}

/// Graph-value structs produced by separate physical paths can differ only in
/// Arrow field nullability. They are still the same Cypher value shape and can
/// be normalized with a cast before native list concatenation.
fn graph_value_types_compatible(left: &DataType, right: &DataType) -> bool {
    let (DataType::Struct(left), DataType::Struct(right)) = (left, right) else {
        return false;
    };
    left.len() == right.len()
        && left.iter().zip(right.iter()).all(|(left, right)| {
            left.name() == right.name()
                && match (left.data_type(), right.data_type()) {
                    (DataType::Struct(_), DataType::Struct(_)) => {
                        graph_value_types_compatible(left.data_type(), right.data_type())
                    }
                    (DataType::List(left), DataType::List(right))
                    | (DataType::LargeList(left), DataType::LargeList(right)) => {
                        left.data_type() == right.data_type()
                    }
                    (
                        DataType::FixedSizeList(left, left_len),
                        DataType::FixedSizeList(right, right_len),
                    ) => left_len == right_len && left.data_type() == right.data_type(),
                    (left, right) => left == right,
                }
        })
}

/// Widen field nullability across two compatible graph-value (or list) types.
///
/// DataFusion 54 rejects casts that *narrow* nested nullability (nullable →
/// non-null inside `List<Struct>`). Mixed named-path segments can emit the same
/// Cypher shape with different Arrow nullability (`cypher_path_nodes` declares
/// non-null `node_uuid`; fixed-hop `named_struct` inherits nullable scan
/// columns), so concatenation must cast both sides to a shared widened type.
fn unify_graph_value_nullability(left: &DataType, right: &DataType) -> Option<DataType> {
    use datafusion::arrow::datatypes::{Field, Fields};

    match (left, right) {
        (DataType::Struct(left), DataType::Struct(right)) if left.len() == right.len() => {
            let mut fields = Vec::with_capacity(left.len());
            for (left, right) in left.iter().zip(right.iter()) {
                if left.name() != right.name() {
                    return None;
                }
                let data_type = unify_graph_value_nullability(left.data_type(), right.data_type())?;
                fields.push(Field::new(
                    left.name(),
                    data_type,
                    left.is_nullable() || right.is_nullable(),
                ));
            }
            Some(DataType::Struct(Fields::from(fields)))
        }
        (DataType::List(left), DataType::List(right)) => {
            let data_type = unify_graph_value_nullability(left.data_type(), right.data_type())?;
            Some(DataType::new_list(
                data_type,
                left.is_nullable() || right.is_nullable(),
            ))
        }
        (DataType::LargeList(left), DataType::LargeList(right)) => {
            let data_type = unify_graph_value_nullability(left.data_type(), right.data_type())?;
            Some(DataType::new_large_list(
                data_type,
                left.is_nullable() || right.is_nullable(),
            ))
        }
        (DataType::FixedSizeList(left, left_len), DataType::FixedSizeList(right, right_len))
            if left_len == right_len =>
        {
            let data_type = unify_graph_value_nullability(left.data_type(), right.data_type())?;
            Some(DataType::new_fixed_size_list(
                data_type,
                *left_len,
                left.is_nullable() || right.is_nullable(),
            ))
        }
        (left, right) if left == right => Some(left.clone()),
        _ => None,
    }
}

/// Lower an [`IrLiteral`] to a DataFusion [`Expr::Literal`].
fn lower_literal(lit_val: &IrLiteral) -> DfExpr {
    lit(ir_literal_to_scalar(lit_val))
}

fn empty_map_struct() -> DfExpr {
    DfExpr::Literal(
        ScalarValue::Struct(std::sync::Arc::new(
            datafusion::arrow::array::StructArray::new_empty_fields(1, None),
        )),
        None,
    )
}

fn is_integer_data_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

// ---------------------------------------------------------------------------
// Cypher conversion UDFs
// ---------------------------------------------------------------------------

fn decoded_scalar_at(
    array: &datafusion::arrow::array::ArrayRef,
    row: usize,
) -> datafusion::error::Result<ScalarValue> {
    let value = ScalarValue::try_from_array(array, row)?;
    Ok(unwrap_het(value))
}

/// Whether a lowered expression is a constant `List` literal.
fn is_list_literal(e: &DfExpr) -> bool {
    matches!(e, DfExpr::Literal(ScalarValue::List(_), _))
}

/// Whether a DataType is the ADR-0011 heterogeneous tagged-struct element type
/// (so `min`/`max` over it must use Cypher orderability, not native min/max).
pub(crate) fn is_het_struct_type(t: Option<&DataType>) -> bool {
    matches!(t, Some(DataType::Struct(fields)) if fields.iter().any(|f| f.name() == graphforge_value::heterogeneous::TAG))
}

// ---------------------------------------------------------------------------
// cypher_to_string UDF
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
