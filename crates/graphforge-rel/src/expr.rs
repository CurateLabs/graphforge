//! IR expression arena → DataFusion [`Expr`] lowering.
//!
//! [`ExprLowerer`] is a pure, I/O-free transformation: it walks the
//! [`ExprArena`] from a [`GraphPlan`] and produces DataFusion [`Expr`] values
//! that can be consumed by operator lowering (#575, #576).

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::{
    Array, FixedSizeListArray, LargeListArray, ListArray, new_empty_array,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::logical_expr::expr::Placeholder;
use datafusion::logical_expr::{
    ColumnarValue, Expr as DfExpr, ExprSchemable, Operator, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility, cast, col, lit, not, when,
};
use datafusion::scalar::ScalarValue;

use graphforge_core::PropId;
use graphforge_ir::expr::{BinaryOpKind, IrExpr, IrLiteral, UnaryOpKind};
use graphforge_ir::{ExprArena, ExprId, VarId};
use graphforge_ontology::OntologyHandle;

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

/// Errors that can occur when lowering an IR expression to DataFusion.
#[derive(Debug, Clone, thiserror::Error)]
pub enum LoweringError {
    /// A [`FunctionCall`](graphforge_ir::expr::IrExpr::FunctionCall) name has no
    /// DataFusion built-in equivalent.
    #[error("unknown built-in function: {0}")]
    UnknownFunction(String),

    /// An [`IrExpr`] variant cannot be lowered yet (e.g. `MapLiteral`).
    #[error("unsupported expression: {0}")]
    UnsupportedExpr(String),

    /// A [`VarId`] referenced by a `VarRef` or `PropertyAccess` is not in the
    /// [`VarMap`].
    #[error("unbound variable: VarId({0})")]
    UnboundVar(u32),

    /// A genuine Cypher type error caught at planning — e.g. a quantifier
    /// predicate that cannot apply to the list's element type (`x % 2` over a
    /// string list). A deliberate validation rejection (openCypher
    /// `InvalidArgumentType`), distinct from a capability gap. (#955)
    #[error("invalid argument type: {0}")]
    InvalidType(String),
}

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
    prop_names: HashMap<u32, String>,
    /// `VarId.0` → node shape, for materializing a bare `RETURN n` as a whole
    /// node value (#785). Empty unless the plan projects a node var by value.
    node_shapes: HashMap<u32, NodeShape>,
    /// Reverse map `TypeId.0` → entity-type (node label) name, merged from the
    /// ontology and runtime catalog. Used to render a real label for an
    /// *unlabelled* node value (`MATCH (n) RETURN n`) by switching on the node's
    /// stored `type_id` (#889). Empty in schema-only/no-catalog lowering.
    type_id_to_entity_name: HashMap<u32, String>,
    /// Forward map of entity-type (node label) name to `TypeId.0`. Derived once
    /// from `type_id_to_entity_name` so a literal `'<label>' IN labels(node)` can
    /// lower directly to topology membership without rebuilding the complete
    /// string label list for every pattern predicate.
    entity_name_to_type_id: HashMap<String, u32>,
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
    /// The project directory for read-side lowering, when one is attached
    /// (#1024): lets `nodes(p)` bake a hydrating `cypher_path_nodes` whose
    /// elements carry labels + the property union discovered from
    /// `properties/*.parquet`. `None` in schema-only/explain lowering — the
    /// UDF then keeps its `node_uuid`-only shape.
    read_target: Option<std::path::PathBuf>,
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
        prop_names: HashMap<u32, String>,
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
        prop_names: HashMap<u32, String>,
        node_shapes: HashMap<u32, NodeShape>,
        type_id_to_entity_name: HashMap<u32, String>,
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

    /// Attach the project directory for read-side lowering, so `nodes(p)` bakes
    /// a hydrating `cypher_path_nodes` (labels + property union, #1024).
    #[must_use]
    pub fn with_read_target(mut self, dir: std::path::PathBuf) -> Self {
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
                if let Some(prop_name) = self.prop_names.get(&prop.0).cloned()
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
                        .get(&prop.0)
                        .cloned()
                        .unwrap_or_else(|| format!("prop_{}", prop.0));
                    let is_topology = graphforge_storage::TOPOLOGY_NODES_SCHEMA
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
                    && let Some(prop_name) = self.prop_names.get(&prop.0)
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
                    && let Some(prop_name) = self.prop_names.get(&prop.0)
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
                    && let Some(prop_name) = self.prop_names.get(&prop.0)
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
                        .get(&prop.0)
                        .cloned()
                        .unwrap_or_else(|| format!("prop_{}", prop.0));
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
                        .get(&prop.0)
                        .cloned()
                        .unwrap_or_else(|| format!("prop_{}", prop.0));
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
                resolve_builtin(name, lowered, || self.path_node_hydration())
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

    fn lower_value(&self, id: ExprId) -> Result<DfExpr, LoweringError> {
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

    fn lower_path_builtin_arg(&self, id: ExprId) -> Result<DfExpr, LoweringError> {
        if let IrExpr::VarRef(v) = self.arena.get(id) {
            return Ok(col_literal(
                self.var_map.get(*v).ok_or(LoweringError::UnboundVar(v.0))?,
            ));
        }
        self.lower(id)
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
            let is_topology = graphforge_storage::TOPOLOGY_NODES_SCHEMA
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
    /// (via [`try_const_scalar`], #1005).
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

    /// Lower a temporal constructor — `date`/`localtime`/`time`/
    /// `localdatetime`/`datetime`/`duration`. When the single argument is a
    /// constant the openCypher TCK uses — an ISO string literal or a map of
    /// literal fields — parse and canonicalise it at lowering time and emit the
    /// quoted-ISO `Utf8` literal the TCK renders, so no runtime UDF is needed.
    /// The supported forms are documented in [`crate::temporal`]. Non-constant /
    /// unsupported-form arguments fall back to `resolve_builtin` (today only
    /// `date` has a runtime path, via `to_date`/`to_char`; the others error).
    /// (#599)
    /// Whether the call has a single argument that is a literal `null` — a
    /// temporal constructor / clock function of `null` propagates to `null`
    /// (openCypher Temporal4 [13]). (#920)
    fn sole_arg_is_null(&self, args: &[ExprId]) -> bool {
        matches!(args, [a] if matches!(self.arena.get(*a), IrExpr::Literal(graphforge_ir::IrLiteral::Null)))
    }

    /// Fold a zero-arg current-time constructor (`date()`/`localtime()`/`time()`/
    /// `localdatetime()`/`datetime()`) to a constant scalar from a single `now`
    /// captured once per lowering, so two calls in one query fold IDENTICALLY
    /// (`duration.inSeconds(localtime(), localtime())` → `PT0S`). `time`/`datetime`
    /// use UTC (offset 0). Non-deterministic by nature — a deliberate exception to
    /// the engine's constant-folding determinism; only the difference-invariant is
    /// exercised by the TCK. (#1007, Temporal10 [12])
    fn lower_clock_now(&self, name: &str) -> DfExpr {
        use chrono::Timelike;
        let now = *self.now.get_or_init(|| chrono::Utc::now().naive_utc());
        let days = crate::temporal::date_to_epoch_days(now.date()).unwrap_or(0);
        let nanos = i64::from(now.time().num_seconds_from_midnight()) * 1_000_000_000
            + i64::from(now.time().nanosecond());
        let scalar = match name {
            "date" => date_scalar(Some(days)),
            "localtime" => ScalarValue::Time64Nanosecond(Some(nanos)),
            "localdatetime" => localdatetime_scalar(Some((days, nanos))),
            "time" => time_scalar(Some((nanos, 0))),
            // "datetime": UTC instant, offset 0, no named zone.
            _ => datetime_scalar(Some((days, nanos, 0, None))),
        };
        lit(scalar)
    }

    fn lower_temporal(&self, name: &str, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        // A temporal constructor of a literal `null` is `null`, typed so the
        // temporal Arrow contract survives null propagation (#920).
        if self.sole_arg_is_null(args) {
            return Ok(DfExpr::Literal(temporal_null_scalar(name), None));
        }
        // A zero-arg current-time constructor `date()`/`localtime()`/`time()`/
        // `localdatetime()`/`datetime()` (#1007). `duration()` has no clock form.
        if args.is_empty() && name != "duration" {
            return Ok(self.lower_clock_now(name));
        }
        if let [arg] = args {
            // `date` is a typed `Struct{epoch_day: Int64}` value (ADR 0009/0012). A
            // constant lowers to a date-struct scalar; a runtime argument (a column,
            // or `{date: other, …overrides}`) goes through the `cypher_date_project`
            // UDF (#920).
            if name == "date" {
                if let Some(days) = self.const_date(*arg) {
                    return Ok(DfExpr::Literal(date_scalar(Some(days)), None));
                }
                // `date(other)` / `date({date: other, …})` → projection UDF. A
                // map *without* a `date` anchor (runtime field construction)
                // returns None and falls through to the runtime builtin path.
                if let Some(projected) = self.lower_date_runtime(*arg)? {
                    return Ok(projected);
                }
            }
            // `localtime` is a typed `Time64(Nanosecond)` value (ADR 0009). A
            // constant lowers to a scalar; a runtime argument (a column, or
            // `{time: other, …overrides}`) goes through `cypher_localtime_project`.
            if name == "localtime" {
                if let Some(nanos) = self.const_local_time(*arg) {
                    return Ok(DfExpr::Literal(
                        ScalarValue::Time64Nanosecond(Some(nanos)),
                        None,
                    ));
                }
                if let Some(projected) = self.lower_localtime_runtime(*arg)? {
                    return Ok(projected);
                }
            }
            // `localdatetime` is a typed `Struct{date: Date32, time: Time64(ns)}`
            // value (ADR 0009) — a date + time-of-day with no zone, two-field so
            // it spans the full year range at nanosecond precision. A constant
            // lowers to a struct scalar; a runtime argument goes through
            // `cypher_localdatetime_project`.
            if name == "localdatetime" {
                if let Some((days, nanos)) = self.const_local_date_time(*arg) {
                    return Ok(DfExpr::Literal(
                        localdatetime_scalar(Some((days, nanos))),
                        None,
                    ));
                }
                if let Some(projected) = self.lower_localdatetime_runtime(*arg)? {
                    return Ok(projected);
                }
            }
            // `time` is a typed `Struct{time: Time64(ns), offset: Int32}` value
            // (ADR 0009) — a time of day with a zone offset. A constant lowers to
            // a struct scalar; a runtime argument goes through `cypher_time_project`.
            if name == "time" {
                if let Some((nanos, offset)) = self.const_time(*arg) {
                    return Ok(DfExpr::Literal(time_scalar(Some((nanos, offset))), None));
                }
                if let Some(projected) = self.lower_time_runtime(*arg)? {
                    return Ok(projected);
                }
            }
            // `datetime` is a typed `Struct{date: Date32, time: Time64(ns),
            // offset: Int32, zone: Utf8?}` value (ADR 0009) — a date + time + zone
            // (resolved offset plus an optional named-IANA-zone label). A constant
            // lowers to a struct scalar; a runtime argument goes through
            // `cypher_datetime_project`.
            if name == "datetime" {
                if let Some(parts) = self.const_datetime(*arg) {
                    return Ok(DfExpr::Literal(datetime_scalar(Some(parts)), None));
                }
                if let Some(projected) = self.lower_datetime_runtime(*arg)? {
                    return Ok(projected);
                }
            }
            // `duration` is a typed `Struct{months, days, seconds, nanos}` value (ADR
            // 0009). A constant (literal ISO string or field map) lowers to a
            // struct scalar; a runtime ISO-string argument (e.g.
            // `duration(toString(d))`) goes through `cypher_duration_parse`.
            if name == "duration" {
                if let Some(dur) = self.const_duration(*arg) {
                    return Ok(DfExpr::Literal(duration_scalar(Some(dur)), None));
                }
                let lowered = self.lower(*arg)?;
                if self.is_string_typed(&lowered) {
                    return Ok(CYPHER_DURATION_PARSE.call(vec![lowered]));
                }
            }
            match self.arena.get(*arg) {
                IrExpr::Literal(IrLiteral::Str(s)) => {
                    if let Some(rendered) = render_temporal(name, s) {
                        return Ok(lit(rendered));
                    }
                }
                IrExpr::MapLiteral(entries) => {
                    if let Some(fields) = self.extract_temporal_fields(entries)
                        && let Some(rendered) = crate::temporal::render_temporal_map(name, &fields)
                    {
                        return Ok(lit(rendered));
                    }
                }
                _ => {}
            }
        }
        let lowered: Vec<DfExpr> = args
            .iter()
            .map(|&a| self.lower(a))
            .collect::<Result<_, _>>()?;
        resolve_builtin(name, lowered, || self.path_node_hydration())
            .ok_or_else(|| LoweringError::UnknownFunction(name.to_string()))
    }

    /// Resolve a `date(<arg>)` argument to constant i64 epoch-days when the
    /// argument is a literal ISO string or a literal field map. (ADR 0009/0012)
    fn const_date(&self, arg: ExprId) -> Option<i64> {
        match self.arena.get(arg) {
            IrExpr::Literal(IrLiteral::Str(s)) => crate::temporal::parse_date_string(s),
            IrExpr::MapLiteral(entries) => {
                let fields = self.extract_temporal_fields(entries)?;
                crate::temporal::date_from_map(&fields)
            }
            _ => None,
        }
    }

    /// Lower a runtime (non-constant) `date(<arg>)` to a `cypher_date_project`
    /// call returning `Date32`: `date({date: base, …overrides})` projects the
    /// base date's components; a bare `date(<expr>)` extracts the date from a
    /// `Date32` or ISO date/datetime string (no overrides). Returns `None` for a
    /// map *without* a `date` anchor (runtime field construction — not a
    /// projection), so the caller falls through to the runtime builtin. (#920)
    fn lower_date_runtime(&self, arg: ExprId) -> Result<Option<DfExpr>, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let (base, overrides) = match self.arena.get(arg) {
            IrExpr::MapLiteral(entries) if entries.iter().any(|(k, _)| k == "date") => {
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let base = self.lower(field("date").expect("checked `date` key exists"))?;
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                let overrides = [
                    ov("year")?,
                    ov("month")?,
                    ov("day")?,
                    ov("week")?,
                    ov("dayOfWeek")?,
                    ov("ordinalDay")?,
                    ov("quarter")?,
                    ov("dayOfQuarter")?,
                ];
                (base, overrides)
            }
            // A map without a `date` anchor is field-construction, not a
            // projection — let the caller's runtime path handle it.
            IrExpr::MapLiteral(_) => return Ok(None),
            // A bare `date(<expr>)` — extract the date, no overrides.
            _ => (self.lower(arg)?, std::array::from_fn(|_| null_i64())),
        };
        let mut call_args = Vec::with_capacity(9);
        call_args.push(base);
        call_args.extend(overrides);
        Ok(Some(CYPHER_DATE_PROJECT.call(call_args)))
    }

    /// Resolve a `localtime(<arg>)` argument to constant nanoseconds-of-day when
    /// the argument is a literal ISO string or a literal field map. (ADR 0009)
    fn const_local_time(&self, arg: ExprId) -> Option<i64> {
        match self.arena.get(arg) {
            IrExpr::Literal(IrLiteral::Str(s)) => crate::temporal::localtime_nanos_from_str(s),
            IrExpr::MapLiteral(entries) => {
                let fields = self.extract_temporal_fields(entries)?;
                crate::temporal::localtime_nanos_from_map(&fields)
            }
            _ => None,
        }
    }

    /// Lower a runtime `localtime(<arg>)` to a `cypher_localtime_project` call
    /// returning `Time64(Nanosecond)`: `localtime({time: base, …overrides})`
    /// projects the base's time-of-day; a bare `localtime(<expr>)` extracts the
    /// time-of-day from a `Time64` or any ISO temporal string. Returns `None` for
    /// a map *without* a `time` anchor (field-construction, not projection), so
    /// the caller falls through to the runtime builtin. (ADR 0009)
    fn lower_localtime_runtime(&self, arg: ExprId) -> Result<Option<DfExpr>, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let (base, overrides) = match self.arena.get(arg) {
            IrExpr::MapLiteral(entries) if entries.iter().any(|(k, _)| k == "time") => {
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let base = self.lower(field("time").expect("checked `time` key exists"))?;
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                let overrides = [
                    ov("hour")?,
                    ov("minute")?,
                    ov("second")?,
                    ov("millisecond")?,
                    ov("microsecond")?,
                    ov("nanosecond")?,
                ];
                (base, overrides)
            }
            IrExpr::MapLiteral(_) => return Ok(None),
            _ => (self.lower(arg)?, std::array::from_fn(|_| null_i64())),
        };
        let mut call_args = Vec::with_capacity(7);
        call_args.push(base);
        call_args.extend(overrides);
        Ok(Some(CYPHER_LOCALTIME_PROJECT.call(call_args)))
    }

    /// Resolve a `localdatetime(<arg>)` argument to constant `(date_days,
    /// nanoseconds_of_day)` when the argument is a literal ISO string or a literal
    /// field map. (ADR 0009)
    fn const_local_date_time(&self, arg: ExprId) -> Option<(i64, i64)> {
        match self.arena.get(arg) {
            IrExpr::Literal(IrLiteral::Str(s)) => crate::temporal::localdatetime_parts_from_str(s),
            IrExpr::MapLiteral(entries) => {
                let fields = self.extract_temporal_fields(entries)?;
                crate::temporal::localdatetime_parts_from_map(&fields)
            }
            _ => None,
        }
    }

    /// Lower a runtime `localdatetime(<arg>)` to a `cypher_localdatetime_project`
    /// call returning `Timestamp(Nanosecond, None)`. The map's `datetime`/`date`
    /// anchor (or a bare `localdatetime(<expr>)`) supplies the base date and the
    /// `datetime`/`time` anchor the base time; the remaining fields are date and
    /// time overrides (a missing date defaults to the epoch, a missing time to
    /// midnight, so explicit fields act as construction defaults). (ADR 0009)
    fn lower_localdatetime_runtime(&self, arg: ExprId) -> Result<Option<DfExpr>, LoweringError> {
        let null = || DfExpr::Literal(ScalarValue::Null, None);
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let (date_src, time_src, overrides) =
            if let IrExpr::MapLiteral(entries) = self.arena.get(arg) {
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let lower_or = |id: Option<ExprId>, default: &dyn Fn() -> DfExpr| match id {
                    Some(id) => self.lower(id),
                    None => Ok(default()),
                };
                // A `datetime:` anchor supplies BOTH base date and base time; a
                // `date:`/`time:` anchor supplies one each.
                let date_anchor = field("datetime").or_else(|| field("date"));
                let time_anchor = field("datetime").or_else(|| field("time"));
                let date_src = lower_or(date_anchor, &null)?;
                let time_src = lower_or(time_anchor, &null)?;
                let ov = |name: &str| lower_or(field(name), &null_i64);
                let overrides = [
                    ov("year")?,
                    ov("month")?,
                    ov("day")?,
                    ov("week")?,
                    ov("dayOfWeek")?,
                    ov("ordinalDay")?,
                    ov("quarter")?,
                    ov("dayOfQuarter")?,
                    ov("hour")?,
                    ov("minute")?,
                    ov("second")?,
                    ov("millisecond")?,
                    ov("microsecond")?,
                    ov("nanosecond")?,
                ];
                (date_src, time_src, overrides)
            } else {
                // A bare `localdatetime(<expr>)` — the value is both date and time
                // source, no overrides.
                let base = self.lower(arg)?;
                (base.clone(), base, std::array::from_fn(|_| null_i64()))
            };
        let mut call_args = Vec::with_capacity(16);
        call_args.push(date_src);
        call_args.push(time_src);
        call_args.extend(overrides);
        Ok(Some(CYPHER_LOCALDATETIME_PROJECT.call(call_args)))
    }

    /// Resolve a `time(<arg>)` argument to constant `(nanoseconds_of_day,
    /// offset_seconds)` when the argument is a literal ISO string or a literal
    /// field map. (ADR 0009)
    fn const_time(&self, arg: ExprId) -> Option<(i64, i32)> {
        match self.arena.get(arg) {
            IrExpr::Literal(IrLiteral::Str(s)) => crate::temporal::time_value_from_str(s),
            IrExpr::MapLiteral(entries) => {
                let fields = self.extract_temporal_fields(entries)?;
                crate::temporal::time_value_from_map(&fields)
            }
            _ => None,
        }
    }

    /// Lower a runtime `time(<arg>)` to a `cypher_time_project` call returning the
    /// `time` struct: `time({time: base, …overrides, timezone})` / a bare
    /// `time(<expr>)`. The base's time-of-day comes from a `Time64`/`time`-struct/
    /// `localdatetime`-struct/temporal-string; component overrides and the zone
    /// (`timezone`) apply on top. Returns `None` for a map without a `time` anchor
    /// (field-construction, not projection). (ADR 0009)
    fn lower_time_runtime(&self, arg: ExprId) -> Result<Option<DfExpr>, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let null_str = || DfExpr::Literal(ScalarValue::Utf8(None), None);
        let (base, overrides, timezone) = match self.arena.get(arg) {
            IrExpr::MapLiteral(entries) if entries.iter().any(|(k, _)| k == "time") => {
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let base = self.lower(field("time").expect("checked `time` key exists"))?;
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                let overrides = [
                    ov("hour")?,
                    ov("minute")?,
                    ov("second")?,
                    ov("millisecond")?,
                    ov("microsecond")?,
                    ov("nanosecond")?,
                ];
                let timezone = match field("timezone") {
                    Some(id) => self.lower(id)?,
                    None => null_str(),
                };
                (base, overrides, timezone)
            }
            // A map without a `time` anchor is field-construction, not
            // projection; and a string literal that `const_time` rejected (e.g.
            // no offset) must NOT be leniently re-parsed by the UDF —
            // `time('21:40')` is an error, not `21:40Z`. Both fall through so the
            // untyped path handles them.
            IrExpr::MapLiteral(_) | IrExpr::Literal(IrLiteral::Str(_)) => return Ok(None),
            _ => (
                self.lower(arg)?,
                std::array::from_fn(|_| null_i64()),
                null_str(),
            ),
        };
        let mut call_args = Vec::with_capacity(8);
        call_args.push(base);
        call_args.extend(overrides);
        call_args.push(timezone);
        Ok(Some(CYPHER_TIME_PROJECT.call(call_args)))
    }

    /// Resolve a `datetime(<arg>)` argument to constant `(date_days, nanos,
    /// offset_seconds, zone_label)` when the argument is a literal ISO string or a
    /// literal field map. (ADR 0009)
    fn const_datetime(&self, arg: ExprId) -> Option<(i64, i64, i32, Option<String>)> {
        match self.arena.get(arg) {
            IrExpr::Literal(IrLiteral::Str(s)) => crate::temporal::datetime_value_from_str(s),
            IrExpr::MapLiteral(entries) => {
                let fields = self.extract_temporal_fields(entries)?;
                crate::temporal::datetime_value_from_map(&fields)
            }
            _ => None,
        }
    }

    /// Resolve a `duration(<arg>)` argument to a constant [`DurationValue`] when
    /// the argument is a literal ISO string or a literal field map. (#920)
    fn const_duration(&self, arg: ExprId) -> Option<crate::temporal::DurationValue> {
        match self.arena.get(arg) {
            IrExpr::Literal(IrLiteral::Str(s)) => crate::temporal::duration_value_from_str(s),
            IrExpr::MapLiteral(entries) => {
                let fields = self.extract_temporal_fields(entries)?;
                crate::temporal::duration_value_from_map(&fields)
            }
            _ => None,
        }
    }

    /// Lower a runtime `datetime(<arg>)` to a `cypher_datetime_project` call
    /// returning the `datetime` struct. The map's `datetime`/`date` anchor (or a
    /// bare `datetime(<expr>)`) supplies the base date, the `datetime`/`time`
    /// anchor the base time (and its offset/zone); the remaining fields are date
    /// and time overrides, and `timezone` re-zones the result. Returns `None` for
    /// a string literal (`const_datetime` already validated it). (ADR 0009)
    fn lower_datetime_runtime(&self, arg: ExprId) -> Result<Option<DfExpr>, LoweringError> {
        let null = || DfExpr::Literal(ScalarValue::Null, None);
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let null_str = || DfExpr::Literal(ScalarValue::Utf8(None), None);
        let (date_src, time_src, overrides, timezone) = match self.arena.get(arg) {
            IrExpr::MapLiteral(entries) => {
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let lower_or = |id: Option<ExprId>, default: &dyn Fn() -> DfExpr| match id {
                    Some(id) => self.lower(id),
                    None => Ok(default()),
                };
                let date_anchor = field("datetime").or_else(|| field("date"));
                let time_anchor = field("datetime").or_else(|| field("time"));
                let date_src = lower_or(date_anchor, &null)?;
                let time_src = lower_or(time_anchor, &null)?;
                let ov = |name: &str| lower_or(field(name), &null_i64);
                let overrides = [
                    ov("year")?,
                    ov("month")?,
                    ov("day")?,
                    ov("week")?,
                    ov("dayOfWeek")?,
                    ov("ordinalDay")?,
                    ov("quarter")?,
                    ov("dayOfQuarter")?,
                    ov("hour")?,
                    ov("minute")?,
                    ov("second")?,
                    ov("millisecond")?,
                    ov("microsecond")?,
                    ov("nanosecond")?,
                ];
                (
                    date_src,
                    time_src,
                    overrides,
                    lower_or(field("timezone"), &null_str)?,
                )
            }
            // A string literal that `const_datetime` rejected must not be lenient-
            // projected; fall through to the untyped path.
            IrExpr::Literal(IrLiteral::Str(_)) => return Ok(None),
            _ => {
                let base = self.lower(arg)?;
                (
                    base.clone(),
                    base,
                    std::array::from_fn(|_| null_i64()),
                    null_str(),
                )
            }
        };
        let mut call_args = Vec::with_capacity(17);
        call_args.push(date_src);
        call_args.push(time_src);
        call_args.extend(overrides);
        call_args.push(timezone);
        Ok(Some(CYPHER_DATETIME_PROJECT.call(call_args)))
    }

    /// Lower `date.truncate(unit, value [, map])` to a `cypher_date_truncate`
    /// call (`Temporal9`): truncate `value`'s date to `unit`, then apply the
    /// optional override map's components. (#920)
    fn lower_date_truncate(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let [unit_id, value_id, rest @ ..] = args else {
            return Err(LoweringError::UnknownFunction("date.truncate".to_string()));
        };
        let unit = self.lower(*unit_id)?;
        let value = self.lower(*value_id)?;
        // The optional third argument is a component-override map. Overrides are
        // extracted at lowering time, so only a *literal* map is supported; a
        // non-literal third argument (e.g. `$m` or a variable) would otherwise
        // be silently dropped and return a subtly wrong date — error instead.
        let overrides: [DfExpr; 8] = match rest.first() {
            None => std::array::from_fn(|_| null_i64()),
            Some(map_id) => {
                let IrExpr::MapLiteral(entries) = self.arena.get(*map_id) else {
                    return Err(LoweringError::UnsupportedExpr(
                        "date.truncate override map must be a literal map".to_string(),
                    ));
                };
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                [
                    ov("year")?,
                    ov("month")?,
                    ov("day")?,
                    ov("week")?,
                    ov("dayOfWeek")?,
                    ov("ordinalDay")?,
                    ov("quarter")?,
                    ov("dayOfQuarter")?,
                ]
            }
        };
        let mut call_args = Vec::with_capacity(10);
        call_args.push(value);
        call_args.push(unit);
        call_args.extend(overrides);
        Ok(CYPHER_DATE_TRUNCATE.call(call_args))
    }

    /// Lower `localtime.truncate(unit, value [, map])` to a `cypher_localtime_truncate`
    /// call (`Temporal9`): truncate `value`'s time-of-day to `unit`, then apply the
    /// optional override map's time components. (#920)
    fn lower_localtime_truncate(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let [unit_id, value_id, rest @ ..] = args else {
            return Err(LoweringError::UnknownFunction(
                "localtime.truncate".to_string(),
            ));
        };
        let unit = self.lower(*unit_id)?;
        let value = self.lower(*value_id)?;
        // Optional third arg is a literal component-override map (see `lower_date_truncate`).
        let overrides: [DfExpr; 6] = match rest.first() {
            None => std::array::from_fn(|_| null_i64()),
            Some(map_id) => {
                let IrExpr::MapLiteral(entries) = self.arena.get(*map_id) else {
                    return Err(LoweringError::UnsupportedExpr(
                        "localtime.truncate override map must be a literal map".to_string(),
                    ));
                };
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                [
                    ov("hour")?,
                    ov("minute")?,
                    ov("second")?,
                    ov("millisecond")?,
                    ov("microsecond")?,
                    ov("nanosecond")?,
                ]
            }
        };
        let mut call_args = Vec::with_capacity(8);
        call_args.push(value);
        call_args.push(unit);
        call_args.extend(overrides);
        Ok(CYPHER_LOCALTIME_TRUNCATE.call(call_args))
    }

    /// Lower `localdatetime.truncate(unit, value [, map])` to a
    /// `cypher_localdatetime_truncate` call (`Temporal9`): truncate `value` to
    /// `unit` (date component for day-and-coarser units, time component for finer
    /// units), then apply the optional override map's date + time components. (#920)
    fn lower_localdatetime_truncate(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let [unit_id, value_id, rest @ ..] = args else {
            return Err(LoweringError::UnknownFunction(
                "localdatetime.truncate".to_string(),
            ));
        };
        let unit = self.lower(*unit_id)?;
        let value = self.lower(*value_id)?;
        // Optional third arg is a literal override map carrying date AND time
        // components (see `lower_date_truncate`).
        let overrides: [DfExpr; 14] = match rest.first() {
            None => std::array::from_fn(|_| null_i64()),
            Some(map_id) => {
                let IrExpr::MapLiteral(entries) = self.arena.get(*map_id) else {
                    return Err(LoweringError::UnsupportedExpr(
                        "localdatetime.truncate override map must be a literal map".to_string(),
                    ));
                };
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                [
                    ov("year")?,
                    ov("month")?,
                    ov("day")?,
                    ov("week")?,
                    ov("dayOfWeek")?,
                    ov("ordinalDay")?,
                    ov("quarter")?,
                    ov("dayOfQuarter")?,
                    ov("hour")?,
                    ov("minute")?,
                    ov("second")?,
                    ov("millisecond")?,
                    ov("microsecond")?,
                    ov("nanosecond")?,
                ]
            }
        };
        let mut call_args = Vec::with_capacity(16);
        call_args.push(value);
        call_args.push(unit);
        call_args.extend(overrides);
        Ok(CYPHER_LOCALDATETIME_TRUNCATE.call(call_args))
    }

    /// Lower `time.truncate(unit, value [, map])` to a `cypher_time_truncate` call
    /// (`Temporal9`): truncate `value`'s time-of-day to `unit` (keeping its zone
    /// offset), then apply the override map's time components and optional
    /// `timezone`. (#920)
    fn lower_time_truncate(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let null_str = || DfExpr::Literal(ScalarValue::Utf8(None), None);
        let [unit_id, value_id, rest @ ..] = args else {
            return Err(LoweringError::UnknownFunction("time.truncate".to_string()));
        };
        let unit = self.lower(*unit_id)?;
        let value = self.lower(*value_id)?;
        let (overrides, timezone): ([DfExpr; 6], DfExpr) = match rest.first() {
            None => (std::array::from_fn(|_| null_i64()), null_str()),
            Some(map_id) => {
                let IrExpr::MapLiteral(entries) = self.arena.get(*map_id) else {
                    return Err(LoweringError::UnsupportedExpr(
                        "time.truncate override map must be a literal map".to_string(),
                    ));
                };
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                let tz = match field("timezone") {
                    Some(id) => self.lower(id)?,
                    None => null_str(),
                };
                (
                    [
                        ov("hour")?,
                        ov("minute")?,
                        ov("second")?,
                        ov("millisecond")?,
                        ov("microsecond")?,
                        ov("nanosecond")?,
                    ],
                    tz,
                )
            }
        };
        let mut call_args = Vec::with_capacity(9);
        call_args.push(value);
        call_args.push(unit);
        call_args.extend(overrides);
        call_args.push(timezone);
        Ok(CYPHER_TIME_TRUNCATE.call(call_args))
    }

    /// Lower `datetime.truncate(unit, value [, map])` to a `cypher_datetime_truncate`
    /// call (`Temporal9`): truncate `value` to `unit` (date for day-and-coarser
    /// units, time for finer units; keeping its zone), then apply the override
    /// map's date + time components and optional `timezone`. (#920)
    fn lower_datetime_truncate(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let null_i64 = || DfExpr::Literal(ScalarValue::Int64(None), None);
        let null_str = || DfExpr::Literal(ScalarValue::Utf8(None), None);
        let [unit_id, value_id, rest @ ..] = args else {
            return Err(LoweringError::UnknownFunction(
                "datetime.truncate".to_string(),
            ));
        };
        let unit = self.lower(*unit_id)?;
        let value = self.lower(*value_id)?;
        let (overrides, timezone): ([DfExpr; 14], DfExpr) = match rest.first() {
            None => (std::array::from_fn(|_| null_i64()), null_str()),
            Some(map_id) => {
                let IrExpr::MapLiteral(entries) = self.arena.get(*map_id) else {
                    return Err(LoweringError::UnsupportedExpr(
                        "datetime.truncate override map must be a literal map".to_string(),
                    ));
                };
                let field = |name: &str| entries.iter().find(|(k, _)| k == name).map(|(_, v)| *v);
                let ov = |name: &str| match field(name) {
                    Some(id) => self.lower(id),
                    None => Ok(null_i64()),
                };
                let tz = match field("timezone") {
                    Some(id) => self.lower(id)?,
                    None => null_str(),
                };
                (
                    [
                        ov("year")?,
                        ov("month")?,
                        ov("day")?,
                        ov("week")?,
                        ov("dayOfWeek")?,
                        ov("ordinalDay")?,
                        ov("quarter")?,
                        ov("dayOfQuarter")?,
                        ov("hour")?,
                        ov("minute")?,
                        ov("second")?,
                        ov("millisecond")?,
                        ov("microsecond")?,
                        ov("nanosecond")?,
                    ],
                    tz,
                )
            }
        };
        let mut call_args = Vec::with_capacity(17);
        call_args.push(value);
        call_args.push(unit);
        call_args.extend(overrides);
        call_args.push(timezone);
        Ok(CYPHER_DATETIME_TRUNCATE.call(call_args))
    }

    /// Lower `duration.between(a, b)` / `inMonths` / `inDays` / `inSeconds` to a
    /// `cypher_duration_between` call `[a, b, mode]`, where `mode` is the
    /// (lowercased) function name. (#920)
    fn lower_duration_between(&self, name: &str, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let [a_id, b_id] = args else {
            return Err(LoweringError::UnknownFunction(name.to_string()));
        };
        let a = self.lower(*a_id)?;
        let b = self.lower(*b_id)?;
        Ok(CYPHER_DURATION_BETWEEN.call(vec![a, b, lit(name)]))
    }

    fn extract_temporal_fields(
        &self,
        entries: &[(String, ExprId)],
    ) -> Option<std::collections::HashMap<String, crate::temporal::TemporalField>> {
        let mut fields = std::collections::HashMap::with_capacity(entries.len());
        for (key, value) in entries {
            fields.insert(key.clone(), self.extract_temporal_field(*value)?);
        }
        Some(fields)
    }

    /// Read a single temporal map field value as a constant. A nested `date(…)`
    /// anchor (the `Temporal1` week forms) is rendered and re-parsed to a date.
    fn extract_temporal_field(&self, id: ExprId) -> Option<crate::temporal::TemporalField> {
        use crate::temporal::TemporalField;
        use graphforge_ir::expr::UnaryOpKind;
        match self.arena.get(id) {
            IrExpr::Literal(IrLiteral::Int(n)) => Some(TemporalField::Int(*n)),
            IrExpr::Literal(IrLiteral::Float(x)) => Some(TemporalField::Float(*x)),
            IrExpr::Literal(IrLiteral::Str(s)) => Some(TemporalField::Str(s.clone())),
            // A negative field (`days: -14`) lowers to unary-minus over a literal,
            // not a negative literal — fold it so the map still constant-folds.
            IrExpr::UnaryOp {
                op: UnaryOpKind::Neg,
                expr,
            } => match self.arena.get(*expr) {
                IrExpr::Literal(IrLiteral::Int(n)) => Some(TemporalField::Int(-n)),
                IrExpr::Literal(IrLiteral::Float(x)) => Some(TemporalField::Float(-x)),
                _ => None,
            },
            IrExpr::FunctionCall { name, args } if name == "date" => {
                if let [a] = args.as_slice()
                    && let IrExpr::Literal(IrLiteral::Str(s)) = self.arena.get(*a)
                {
                    crate::temporal::parse_date_string(s).map(TemporalField::Date)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Lower `datetime.fromepoch(seconds, nanoseconds)` /
    /// `datetime.fromepochmillis(milliseconds)` from integer-literal arguments
    /// to the canonical UTC datetime string. (#599)
    fn lower_from_epoch(&self, name: &str, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        self.try_from_epoch(name, args)
            .map(lit)
            .ok_or_else(|| LoweringError::UnknownFunction(name.to_string()))
    }

    fn try_from_epoch(&self, name: &str, args: &[ExprId]) -> Option<String> {
        match (name, args) {
            ("datetime.fromepoch", &[a, b]) => {
                crate::temporal::render_from_epoch(self.int_literal(a)?, self.int_literal(b)?)
            }
            ("datetime.fromepochmillis", &[a]) => {
                crate::temporal::render_from_epoch_millis(self.int_literal(a)?)
            }
            _ => None,
        }
    }

    /// Read an integer literal argument, or `None` if it isn't one.
    fn int_literal(&self, id: ExprId) -> Option<i64> {
        match self.arena.get(id) {
            IrExpr::Literal(IrLiteral::Int(n)) => Some(*n),
            _ => None,
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Lower a `_node_struct(VarRef)` call (emitted by the binder for a bare
    /// `RETURN n`, #785) into a whole node value — `Struct{node_uuid, labels,
    /// <props…>}`. The node's shape (label + property columns) comes from the
    /// `node_shapes` map this lowerer was seeded with; an absent shape yields a
    /// uuid-only struct.
    fn lower_node_struct(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
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

    fn lower_node_struct_list(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
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
                let base = self
                    .var_map
                    .get(*var)
                    .ok_or(LoweringError::UnboundVar(var.0))?;
                Ok(node_value_struct(
                    base,
                    None,
                    &self.type_id_to_entity_name,
                    &prop_names,
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

    fn lower_rel_struct(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
        let (base, rel_type) = self.lower_rel_struct_args(args)?;
        let props = self.edge_prop_names(base);
        let value = relationship_value_struct(base, rel_type, &props);
        Ok(null_unless(edge_present_qual(base), value))
    }

    fn lower_rel_struct_list(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
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

    fn edge_prop_names(&self, base: &str) -> Vec<String> {
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
    fn lower_labels(&self, args: &[ExprId]) -> Result<DfExpr, LoweringError> {
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

    fn is_edge_var(&self, base: &str) -> bool {
        let Some(schema) = self.input_schema.as_ref() else {
            return false;
        };
        let qual = datafusion::common::TableReference::bare(base);
        schema
            .index_of_column_by_name(Some(&qual), "edge_uuid")
            .is_some()
    }

    fn is_node_var(&self, base: &str) -> bool {
        let Some(schema) = self.input_schema.as_ref() else {
            return false;
        };
        let qual = datafusion::common::TableReference::bare(base);
        schema
            .index_of_column_by_name(Some(&qual), "node_uuid")
            .is_some()
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
            lit(*type_id),
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
    fn resolve_prop_col(&self, base_expr: DfExpr, prop: PropId) -> DfExpr {
        let prop_name = self
            .prop_names
            .get(&prop.0)
            .cloned()
            .unwrap_or_else(|| format!("prop_{}", prop.0));

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

    /// The lowering-baked context for hydrating `nodes(p)` elements (#1024):
    /// the element fields are `node_uuid`, `labels`, then the **union** of
    /// every `properties/<stem>.parquet` schema's columns (sorted stems, first
    /// occurrence of a name wins, forced nullable — a node without the column
    /// is NULL). `None` without a read target (schema-only lowering), keeping
    /// the UDF's original `node_uuid`-only shape.
    fn path_node_hydration(&self) -> Option<PathNodeHydration> {
        use datafusion::arrow::datatypes::Field;
        let dir = self.read_target.as_ref()?;
        let stems = graphforge_storage::list_property_stems(dir);
        let mut fields = vec![
            Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
        ];
        let mut seen: std::collections::HashSet<String> =
            fields.iter().map(|f| f.name().clone()).collect();
        for stem in &stems {
            let table = graphforge_storage::PropertyTable::open_discovered(dir, stem);
            for f in table.schema_ref().fields() {
                if f.name() == "node_uuid" || !seen.insert(f.name().clone()) {
                    continue;
                }
                fields.push(f.as_ref().clone().with_nullable(true));
            }
        }
        let mut labels_by_type: Vec<(u32, String)> = self
            .type_id_to_entity_name
            .iter()
            .map(|(id, name)| (*id, name.clone()))
            .collect();
        labels_by_type.sort();
        Some(PathNodeHydration {
            dir: dir.clone(),
            labels_by_type,
            prop_stems: stems,
            fields: fields.into(),
        })
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
fn build_prop_names(_ontology: Option<&OntologyHandle>) -> HashMap<u32, String> {
    HashMap::new()
}

/// Lower a list literal's (already-lowered) elements to a DataFusion list `Expr`.
///
/// When every element is a constant, fold into a single `ScalarValue::List`
/// literal (the form `UnwindExec` consumes directly). Otherwise, build a
/// `make_array(...)` scalar-function call so per-row expression elements are
/// evaluated at execution time. An empty list folds to an empty `Int64` list
/// (`UNWIND []` yields zero rows regardless of element type).
/// The fixed Arrow layout of a heterogeneous ("tagged") FLAT-scalar list element
/// (ADR 0010). `__het_key` is the element's numeric value (null for non-numeric)
/// and is FIRST so DataFusion's native lexicographic `Struct` min/max orders
/// numeric elements by value and returns the original — no custom aggregate UDF.
/// `__het_tag`: `0`=int, `1`=float, `2`=string, `3`=bool; exactly one of the
/// `__het_int`/`__het_float`/`__het_str`/`__het_bool` fields is populated. The
/// names are reserved so the result renderer can detect and decode the element.
///
/// NOTE (ADR 0010 limitation): this representation is **flat-scalar only**. A list
/// whose elements are themselves lists/maps (nested heterogeneous values) cannot
/// be a tagged struct — an Arrow type cannot be recursive — so such lists are left
/// to `make_array` (and currently error). See the ADR's "nested" limitation.
/// Whether `e` is a statically-EMPTY list literal (`[]`) — a folded
/// `ScalarValue::List` whose single row has zero elements. Used to exempt an empty
/// list from quantifier plan-time type validation (its predicate never runs).
fn is_empty_list_literal(e: &DfExpr) -> bool {
    use datafusion::arrow::array::Array;
    matches!(e, DfExpr::Literal(ScalarValue::List(arr), _) if arr.value(0).is_empty())
}

/// Resolve a lowered list element to a constant `ScalarValue` — a literal, or a
/// `named_struct(...)` map whose keys are string literals and whose values are
/// themselves constant (recursively). `None` for a non-constant element. Lets a
/// list literal that mixes maps with scalars/containers fold every element and
/// reach the tagged het path without folding maps everywhere (#1005).
fn try_const_scalar(e: &DfExpr) -> Option<ScalarValue> {
    match e {
        DfExpr::Literal(s, _) => Some(s.clone()),
        DfExpr::ScalarFunction(f) if f.func.name() == "named_struct" => {
            let mut entries: Vec<(String, ScalarValue)> = Vec::with_capacity(f.args.len() / 2);
            let pairs = f.args.chunks_exact(2);
            if !pairs.remainder().is_empty() {
                return None;
            }
            for pair in pairs {
                let DfExpr::Literal(ScalarValue::Utf8(Some(k)), _) = &pair[0] else {
                    return None;
                };
                entries.push((k.clone(), try_const_scalar(&pair[1])?));
            }
            const_map_scalar(&entries)
        }
        _ => None,
    }
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

/// Build a constant map `{k: v, …}` as a `ScalarValue::Struct` from constant
/// entries — the exact `Struct` shape `named_struct` produces (each field
/// nullable), so `m.k` access, equality, and rendering are unchanged.
fn const_map_scalar(entries: &[(String, ScalarValue)]) -> Option<ScalarValue> {
    use datafusion::arrow::array::{ArrayRef, StructArray};
    use datafusion::arrow::datatypes::{Field, Fields};
    use std::sync::Arc;
    if entries.is_empty() {
        return Some(ScalarValue::Struct(Arc::new(
            StructArray::new_empty_fields(1, None),
        )));
    }
    let mut fields: Vec<Field> = Vec::with_capacity(entries.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(entries.len());
    for (k, sv) in entries {
        let arr = sv.to_array().ok()?; // length-1 array
        fields.push(Field::new(k, arr.data_type().clone(), true));
        arrays.push(arr);
    }
    let s = StructArray::try_new(Fields::from(fields), arrays, None).ok()?;
    Some(ScalarValue::Struct(Arc::new(s)))
}

/// Whether a `Struct` value is a plain Cypher MAP — three-valued structural value
/// — rather than one of the reserved struct shapes that must NOT be encoded as a
/// het map element (#1005): a het-tagged element, a node/relationship/path entity,
/// or a typed temporal value (`date`/`time`/`localdatetime`/`datetime`/`duration`).
fn is_plain_map_struct(arr: &datafusion::arrow::array::StructArray) -> bool {
    use datafusion::arrow::array::Array;
    is_plain_map_struct_type(arr.data_type())
}

/// [`is_plain_map_struct`] on a `DataType` — a `Struct` that is a plain Cypher map,
/// not a het-tagged element, a typed temporal value, or a node/relationship/path
/// entity. Used to route `m.k` on a map-typed column to `get_field` (#1017).
fn is_plain_map_struct_type(dt: &DataType) -> bool {
    let DataType::Struct(fields) = dt else {
        return false;
    };
    // Reserved entity field names (mirror `is_entity_struct`).
    let is_entity = fields.iter().any(|f| {
        matches!(
            f.name().as_str(),
            "node_uuid" | "src_uuid" | "dst_uuid" | "nodes" | "relationships" | "labels"
        )
    });
    !is_entity
        && !is_het_struct_type(Some(dt))
        && !is_date_struct(dt)
        && !is_localdatetime_struct(dt)
        && !is_duration_struct(dt)
        && !is_time_struct(dt)
        && !is_datetime_struct(dt)
}

/// The entry-struct fields of a het map element (ADR 0011 slice 2, #1005): a
/// `__het_mkey: Utf8` key paired with a `__het_mval` tagged value one level
/// shallower — so a map's values recurse by value exactly like list children.
fn het_map_entry_fields(depth: usize) -> datafusion::arrow::datatypes::Fields {
    use datafusion::arrow::datatypes::{DataType, Field};
    datafusion::arrow::datatypes::Fields::from(vec![
        Field::new("__het_mkey", DataType::Utf8, false),
        Field::new("__het_mval", DataType::Struct(het_fields(depth)), true),
    ])
}

/// Arrow fields of a tagged heterogeneous list element (ADR 0010/0011) that can
/// nest to `depth` levels. `__het_key` is first (native `Struct` min/max orders
/// flat numeric lists by value — ADR 0010). `__het_tag`: 0=int, 1=float, 2=str,
/// 3=bool, 4=list, 5=map. For `depth >= 1` a `__het_list: List<Struct{…depth-1…}>`
/// field holds a nested-list element's tagged children and a `__het_map:
/// List<Struct{__het_mkey, __het_mval: Struct{…depth-1…}}>` field holds a map
/// element's key/tagged-value entries — a *distinct, shallower, finite* type per
/// level (recursion by value, not a recursive Arrow type; the literal's depth is
/// known at lowering time — ADR 0011).
fn het_fields(depth: usize) -> datafusion::arrow::datatypes::Fields {
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::sync::Arc;
    let mut v = vec![
        Field::new("__het_key", DataType::Float64, true),
        Field::new("__het_tag", DataType::Int8, false),
        Field::new("__het_int", DataType::Int64, true),
        Field::new("__het_float", DataType::Float64, true),
        Field::new("__het_str", DataType::Utf8, true),
        Field::new("__het_bool", DataType::Boolean, true),
    ];
    if depth >= 1 {
        let inner = Field::new("item", DataType::Struct(het_fields(depth - 1)), true);
        v.push(Field::new(
            "__het_list",
            DataType::List(Arc::new(inner)),
            true,
        ));
        let entry = Field::new(
            "item",
            DataType::Struct(het_map_entry_fields(depth - 1)),
            true,
        );
        v.push(Field::new(
            "__het_map",
            DataType::List(Arc::new(entry)),
            true,
        ));
    }
    datafusion::arrow::datatypes::Fields::from(v)
}

/// The nesting depth of a value as a het element: a scalar is `0`, a list or map
/// is `1 + max child depth` (empty container = 1). `None` if the value cannot be a
/// het element (a node/relationship/path entity or a typed temporal value).
fn het_depth(s: &ScalarValue) -> Option<usize> {
    use datafusion::arrow::array::Array;
    let s = unwrap_het(s.clone());
    match &s {
        ScalarValue::Int64(_)
        | ScalarValue::Float64(_)
        | ScalarValue::Utf8(_)
        | ScalarValue::LargeUtf8(_)
        | ScalarValue::Utf8View(_)
        | ScalarValue::Boolean(_)
        | ScalarValue::Null => Some(0),
        ScalarValue::List(arr) => {
            let inner = arr.value(0);
            let mut d = 0;
            for i in 0..inner.len() {
                // An inner list that is itself heterogeneous was already lowered to
                // a tagged struct (bottom-up lowering); unwrap it back to the plain
                // value so depth/encoding are computed uniformly.
                let e = unwrap_het(ScalarValue::try_from_array(&inner, i).ok()?);
                d = d.max(het_depth(&e)?);
            }
            Some(1 + d)
        }
        // A plain map (#1005): 1 + the deepest value; an empty map is depth 1. A
        // value already tagged (a het list value) unwraps back to its plain form.
        ScalarValue::Struct(arr) if is_plain_map_struct(arr) => {
            let mut d = 0;
            for i in 0..arr.num_columns() {
                let v = unwrap_het(ScalarValue::try_from_array(arr.column(i), 0).ok()?);
                d = d.max(het_depth(&v)?);
            }
            Some(1 + d)
        }
        _ => None,
    }
}

/// Decode an already-tagged het element back to its plain value (for re-encoding
/// uniformly at an outer level); leaves a non-tagged value unchanged.
fn unwrap_het(s: ScalarValue) -> ScalarValue {
    if let ScalarValue::Dictionary(_, value) = s {
        return unwrap_het(*value);
    }
    decode_het(&s).unwrap_or(s)
}

/// Build the tagged-struct array for `scalars`, every element encoded uniformly at
/// `depth` (ADR 0011). List elements recurse their children at `depth - 1`.
#[allow(
    clippy::too_many_lines,
    reason = "one cohesive per-field array builder; splitting it would obscure the field/offset bookkeeping"
)]
fn build_het_struct(
    scalars: &[ScalarValue],
    depth: usize,
) -> Option<datafusion::arrow::array::StructArray> {
    use datafusion::arrow::array::{
        ArrayRef, BooleanArray, Float64Array, Int8Array, Int64Array, ListArray, StringArray,
        StructArray,
    };
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    let n = scalars.len();
    let mut keys: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut tags: Vec<i8> = Vec::with_capacity(n);
    let mut ints: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut floats: Vec<Option<f64>> = Vec::with_capacity(n);
    let mut strs: Vec<Option<String>> = Vec::with_capacity(n);
    let mut bools: Vec<Option<bool>> = Vec::with_capacity(n);
    let mut valid: Vec<bool> = Vec::with_capacity(n);
    // Children of list elements, flattened, with one (offset, valid) per element.
    let mut child_elems: Vec<ScalarValue> = Vec::new();
    let mut child_offsets: Vec<i32> = vec![0];
    let mut child_valid: Vec<bool> = Vec::new();
    // Entries of map elements, flattened as parallel (key, tagged-value) columns,
    // with one (offset, valid) per element (mirrors the list-child bookkeeping).
    let mut map_keys: Vec<String> = Vec::new();
    let mut map_vals: Vec<ScalarValue> = Vec::new();
    let mut map_offsets: Vec<i32> = vec![0];
    let mut map_valid: Vec<bool> = Vec::new();
    for scalar in scalars {
        let scalar = unwrap_het(scalar.clone());
        // Scalar fields default to null/false; each arm sets only what it needs.
        let (mut key, mut tag, mut int_v, mut float_v, mut str_v, mut bool_v, mut ok) =
            (None, 0i8, None, None, None, None, true);
        let mut child: Option<Vec<ScalarValue>> = None;
        let mut map_child: Option<Vec<(String, ScalarValue)>> = None;
        match &scalar {
            ScalarValue::Int64(Some(x)) => {
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "key feeds only min/max ORDERING; the exact integer is preserved in __het_int"
                )]
                let k = Some(*x as f64);
                key = k;
                tag = 0;
                int_v = Some(*x);
            }
            ScalarValue::Float64(Some(x)) => {
                key = Some(*x);
                tag = 1;
                float_v = Some(*x);
            }
            ScalarValue::Utf8(Some(x))
            | ScalarValue::LargeUtf8(Some(x))
            | ScalarValue::Utf8View(Some(x)) => {
                tag = 2;
                str_v = Some(x.clone());
            }
            ScalarValue::Boolean(Some(x)) => {
                tag = 3;
                bool_v = Some(*x);
            }
            ScalarValue::List(arr) => {
                if depth == 0 {
                    return None; // shape deeper than the computed depth — defensive
                }
                tag = 4;
                let inner = arr.value(0);
                let mut elems = Vec::with_capacity(inner.len());
                for idx in 0..inner.len() {
                    elems.push(unwrap_het(ScalarValue::try_from_array(&inner, idx).ok()?));
                }
                child = Some(elems);
            }
            // A plain map (#1005): tag 5; its (key, value) entries recurse as
            // tagged values one level shallower (empty map → zero entries).
            ScalarValue::Struct(arr) if is_plain_map_struct(arr) => {
                if depth == 0 {
                    return None; // shape deeper than the computed depth — defensive
                }
                tag = 5;
                let mut kv = Vec::with_capacity(arr.num_columns());
                for (i, f) in arr.fields().iter().enumerate() {
                    let v = unwrap_het(ScalarValue::try_from_array(arr.column(i), 0).ok()?);
                    kv.push((f.name().clone(), v));
                }
                map_child = Some(kv);
            }
            // Null (typed or untyped) → a null element.
            ScalarValue::Int64(None)
            | ScalarValue::Float64(None)
            | ScalarValue::Utf8(None)
            | ScalarValue::LargeUtf8(None)
            | ScalarValue::Utf8View(None)
            | ScalarValue::Boolean(None)
            | ScalarValue::Null => ok = false,
            _ => return None, // entity/temporal/other struct → not this slice
        }
        keys.push(key);
        tags.push(tag);
        ints.push(int_v);
        floats.push(float_v);
        strs.push(str_v);
        bools.push(bool_v);
        valid.push(ok);
        if depth >= 1 {
            if let Some(elems) = child {
                child_elems.extend(elems);
                child_valid.push(true);
            } else {
                child_valid.push(false);
            }
            child_offsets.push(i32::try_from(child_elems.len()).ok()?);
            if let Some(kv) = map_child {
                for (k, v) in kv {
                    map_keys.push(k);
                    map_vals.push(v);
                }
                map_valid.push(true);
            } else {
                map_valid.push(false);
            }
            map_offsets.push(i32::try_from(map_keys.len()).ok()?);
        }
    }

    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(keys)),
        Arc::new(Int8Array::from(tags)),
        Arc::new(Int64Array::from(ints)),
        Arc::new(Float64Array::from(floats)),
        Arc::new(StringArray::from(strs)),
        Arc::new(BooleanArray::from(bools)),
    ];
    if depth >= 1 {
        let child_struct = build_het_struct(&child_elems, depth - 1)?;
        let inner_field = Arc::new(Field::new(
            "item",
            DataType::Struct(het_fields(depth - 1)),
            true,
        ));
        let het_list = ListArray::new(
            inner_field,
            OffsetBuffer::new(child_offsets.into()),
            Arc::new(child_struct),
            Some(NullBuffer::from(child_valid)),
        );
        arrays.push(Arc::new(het_list));

        // __het_map: a List<Struct{__het_mkey, __het_mval}> — each map element's
        // key/tagged-value entries, with values encoded one level shallower.
        let entry_fields = het_map_entry_fields(depth - 1);
        let mkey_arr = Arc::new(StringArray::from(map_keys)) as ArrayRef;
        let mval_struct = build_het_struct(&map_vals, depth - 1)?;
        let entry_struct = StructArray::new(
            entry_fields.clone(),
            vec![mkey_arr, Arc::new(mval_struct)],
            None,
        );
        let entry_field = Arc::new(Field::new("item", DataType::Struct(entry_fields), true));
        let het_map = ListArray::new(
            entry_field,
            OffsetBuffer::new(map_offsets.into()),
            Arc::new(entry_struct),
            Some(NullBuffer::from(map_valid)),
        );
        arrays.push(Arc::new(het_map));
    }
    Some(StructArray::new(
        het_fields(depth),
        arrays,
        Some(NullBuffer::from(valid)),
    ))
}

/// Build a heterogeneous list literal as the ADR-0010/0011 tagged struct
/// (`List<Struct{__het_*}>`) when `scalars` is a constant list that cannot be a
/// homogeneous Arrow array — a flat mix of `int`/`float`/`string`/`bool`, or a
/// list with nested-list elements (`[1, [1, 2]]`). Returns `None` if any element
/// is a map/struct/entity (deferred to a later ADR-0011 slice) — those fall to
/// `make_array`. Only called after the homogeneous const-fold has been ruled out,
/// so homogeneous lists keep their primitive `new_list` representation untouched.
fn tagged_numeric_list(scalars: &[ScalarValue]) -> Option<DfExpr> {
    use datafusion::arrow::array::ListArray;
    use datafusion::arrow::buffer::OffsetBuffer;
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    // Every element must be het-representable (scalar or nested list); compute the
    // literal's nesting depth so the per-level struct types are finite and exact.
    let mut depth = 0usize;
    for s in scalars {
        depth = depth.max(het_depth(s)?);
    }
    let n = scalars.len();
    let elem = build_het_struct(scalars, depth)?;
    let list_field = Arc::new(Field::new(
        "item",
        DataType::Struct(het_fields(depth)),
        true,
    ));
    let list = ListArray::new(
        list_field,
        OffsetBuffer::from_lengths([n]),
        Arc::new(elem),
        None,
    );
    Some(DfExpr::Literal(ScalarValue::List(Arc::new(list)), None))
}

static CYPHER_DYNAMIC_HET_LIST: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDynamicHetList::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherDynamicHetList {
    signature: Signature,
}

impl CypherDynamicHetList {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

fn dynamic_het_type(arg_types: &[DataType]) -> DataType {
    use datafusion::arrow::datatypes::Fields;
    let mut fields = Vec::with_capacity(arg_types.len() + 1);
    fields.push(Field::new("__het_tag", DataType::Int8, false));
    fields.extend(
        arg_types
            .iter()
            .enumerate()
            .map(|(i, ty)| Field::new(format!("__het_value_{i}"), ty.clone(), true)),
    );
    DataType::new_list(DataType::Struct(Fields::from(fields)), true)
}

impl ScalarUDFImpl for CypherDynamicHetList {
    fn name(&self) -> &'static str {
        "cypher_dynamic_het_list"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(dynamic_het_type(arg_types))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        let arg_types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        Ok(Arc::new(Field::new(
            self.name(),
            dynamic_het_type(&arg_types),
            false,
        )))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{ArrayRef, Int8Array, Int32Array, StructArray};
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
        use datafusion::arrow::compute::take;
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        let width = args.args.len();
        let width_i8 = i8::try_from(width).map_err(|_| {
            DataFusionError::Plan("heterogeneous list literal exceeds 127 elements".into())
        })?;
        let values = args
            .args
            .iter()
            .map(|value| value.to_array(rows))
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        let tags = Int8Array::from_iter_values((0..rows).flat_map(|_| 0..width_i8));
        let mut columns: Vec<ArrayRef> = vec![Arc::new(tags)];
        for (value_idx, value) in values.iter().enumerate() {
            let indices = (0..rows)
                .flat_map(|row| {
                    (0..width).map(move |element_idx| {
                        (element_idx == value_idx)
                            .then(|| i32::try_from(row).ok())
                            .flatten()
                    })
                })
                .collect::<Int32Array>();
            columns.push(take(value.as_ref(), &indices, None)?);
        }
        let valid = (0..rows)
            .flat_map(|row| values.iter().map(move |value| !value.is_null(row)))
            .collect::<NullBuffer>();
        let DataType::List(item) = args.return_field.data_type() else {
            return Err(DataFusionError::Internal(
                "dynamic heterogeneous list has a non-list return type".into(),
            ));
        };
        let DataType::Struct(fields) = item.data_type() else {
            return Err(DataFusionError::Internal(
                "dynamic heterogeneous list has a non-struct element type".into(),
            ));
        };
        let elements = StructArray::new(fields.clone(), columns, Some(valid));
        let list = ListArray::new(
            item.clone(),
            OffsetBuffer::from_lengths(std::iter::repeat_n(width, rows)),
            Arc::new(elements),
            None,
        );
        Ok(ColumnarValue::Array(Arc::new(list)))
    }
}

fn lower_list_literal(
    elems: Vec<DfExpr>,
    input_schema: Option<&datafusion::common::DFSchema>,
) -> DfExpr {
    // Resolve every element to a constant if possible (a literal, or a const map
    // folded from `named_struct`, #1005). `None` if any element is non-constant.
    if let Some(scalars) = elems
        .iter()
        .map(try_const_scalar)
        .collect::<Option<Vec<ScalarValue>>>()
    {
        // Element type: the first non-null element's type, else Int64.
        let elem_type = scalars
            .iter()
            .map(ScalarValue::data_type)
            .find(|t| *t != DataType::Null)
            .unwrap_or(DataType::Int64);
        // Re-type untyped `Null`s to `elem_type` so the list can be a nullable array
        // of that type (`[1, null]` → `Int64[1, null]`). Without this, `new_list`
        // panics building a homogeneous array from an untyped null.
        let typed: Vec<ScalarValue> = scalars
            .iter()
            .map(|s| {
                if matches!(s, ScalarValue::Null) {
                    ScalarValue::try_from(&elem_type).unwrap_or(ScalarValue::Null)
                } else {
                    s.clone()
                }
            })
            .collect();
        // Const-fold a HOMOGENEOUS list to a single `ScalarValue::List` — including
        // a same-shape all-map list, which stays a PLAIN `List<Struct>` (so `x.field`
        // access in a quantifier resolves, #1004) AND is a literal (so it can nest
        // inside an outer tagged het list, #1005).
        if typed.iter().all(|s| s.data_type() == elem_type) {
            let list = ScalarValue::new_list(&typed, &elem_type, true);
            return DfExpr::Literal(ScalarValue::List(list), None);
        }
        // A list whose elements are ALL maps (with any nulls) keeps each element a
        // PLAIN map so `x.field` access in a quantifier resolves (#1004): a
        // DIFFERENT-shape all-map list is padded to the union of keys (missing key
        // → null) into a homogeneous `List<Struct>` literal — which `make_array`
        // itself cannot unify. Only a genuinely MIXED list (maps alongside
        // scalars/lists) uses the tagged het path, where map elements carry no
        // accessible fields. (#1005)
        let all_maps = scalars
            .iter()
            .any(|s| matches!(s, ScalarValue::Struct(a) if is_plain_map_struct(a)))
            && scalars.iter().all(|s| {
                s.is_null() || matches!(s, ScalarValue::Struct(a) if is_plain_map_struct(a))
            });
        if all_maps {
            if let Some(padded) = all_map_union_list(&scalars) {
                return padded;
            }
        } else if let Some(tagged) = tagged_numeric_list(&scalars) {
            return tagged;
        }
    }
    if let Some(schema) = input_schema
        && let Some(types) = elems
            .iter()
            .map(|elem| elem.get_type(schema).ok())
            .collect::<Option<Vec<_>>>()
        && types.windows(2).any(|pair| pair[0] != pair[1])
    {
        return CYPHER_DYNAMIC_HET_LIST.call(elems);
    }
    datafusion::functions_nested::expr_fn::make_array(elems)
}

static CYPHER_LIST_PLUS: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherListPlus::new()));

static CYPHER_RELATIONSHIP_DISJOINT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherRelationshipDisjoint::new()));

pub(crate) fn relationship_disjoint(left: DfExpr, right: DfExpr) -> DfExpr {
    CYPHER_RELATIONSHIP_DISJOINT.call(vec![left, right])
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherListPlus {
    signature: Signature,
}

impl CypherListPlus {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherListPlus {
    fn name(&self) -> &'static str {
        "cypher_list_plus"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        if list_plus_has_graph_value(arg_types) {
            Ok(list_plus_return_type(arg_types))
        } else {
            Ok(DataType::new_list(
                DataType::Struct(het_fields(list_plus_depth(arg_types))),
                true,
            ))
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "list/list and list/element shaping share one offset and validity pass"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, ArrayRef, ListArray};
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use datafusion::arrow::datatypes::DataType;
        use datafusion::error::DataFusionError;
        use std::sync::Arc;

        let rows = args.number_rows;
        let left = args.args[0].to_array(rows)?;
        let right = args.args[1].to_array(rows)?;
        let left_is_list = list_item_type(left.data_type()).is_some();
        let right_is_list = list_item_type(right.data_type()).is_some();
        if !left_is_list && !right_is_list {
            return Err(DataFusionError::Execution(
                "list + requires at least one list operand".into(),
            ));
        }

        if left_is_list
            && !right_is_list
            && let Some(result) =
                invoke_tagged_list_element_plus(&left, &right, args.return_field.data_type())?
        {
            return Ok(ColumnarValue::Array(result));
        }

        let mut flat: Vec<ScalarValue> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(rows + 1);
        let mut validity: Vec<bool> = Vec::with_capacity(rows);
        offsets.push(0);

        for row in 0..rows {
            let row_values = match (left_is_list, right_is_list) {
                (true, true) => match (
                    list_elements_at(&left, row)?,
                    list_elements_at(&right, row)?,
                ) {
                    (Some(mut l), Some(r)) => {
                        l.extend(r);
                        Some(l)
                    }
                    _ => None,
                },
                (true, false) => match list_elements_at(&left, row)? {
                    Some(mut l) => {
                        let r = decoded_scalar_at(&right, row)?;
                        if let Some(r) = scalar_list_elements(&r)? {
                            l.extend(r);
                        } else {
                            l.push(r);
                        }
                        Some(l)
                    }
                    None => None,
                },
                (false, true) => match list_elements_at(&right, row)? {
                    Some(mut r) => {
                        let l = decoded_scalar_at(&left, row)?;
                        let mut l = scalar_list_elements(&l)?.unwrap_or_else(|| vec![l]);
                        l.append(&mut r);
                        Some(l)
                    }
                    None => None,
                },
                (false, false) => unreachable!("checked above"),
            };

            match row_values {
                Some(values) => {
                    flat.extend(values);
                    validity.push(true);
                }
                None => validity.push(false),
            }
            offsets.push(i32::try_from(flat.len()).map_err(|_| {
                DataFusionError::Execution("cypher_list_plus: list too long".into())
            })?);
        }

        let DataType::List(field) = args.return_field.data_type() else {
            return Err(DataFusionError::Internal(
                "cypher_list_plus return type is not a list".into(),
            ));
        };
        if is_het_struct_type(Some(field.data_type()))
            && !is_dynamic_variant_struct(field.data_type())
        {
            let depth = list_plus_depth(&[left.data_type().clone(), right.data_type().clone()]);
            let values = build_het_struct(&flat, depth).ok_or_else(|| {
                DataFusionError::Execution(
                    "cypher_list_plus: cannot encode value in heterogeneous list".into(),
                )
            })?;
            let out = ListArray::new(
                field.clone(),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                Arc::new(values) as ArrayRef,
                Some(NullBuffer::from(validity)),
            );
            return Ok(ColumnarValue::Array(Arc::new(out)));
        }
        if !is_dynamic_variant_struct(field.data_type()) {
            let flat = flat
                .into_iter()
                .map(|value| {
                    if value.data_type() == *field.data_type() {
                        Ok(value)
                    } else {
                        value.cast_to(field.data_type())
                    }
                })
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            let values = if flat.is_empty() {
                new_empty_array(field.data_type())
            } else {
                ScalarValue::iter_to_array(flat)?
            };
            let out = ListArray::new(
                field.clone(),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                values,
                Some(NullBuffer::from(validity)),
            );
            return Ok(ColumnarValue::Array(Arc::new(out)));
        }
        let DataType::Struct(fields) = field.data_type() else {
            return Err(DataFusionError::Internal(
                "cypher_list_plus element type is not tagged".into(),
            ));
        };
        let variants = fields
            .iter()
            .filter(|field| field.name().starts_with("__het_value_"))
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        let mut tags = Vec::with_capacity(flat.len());
        let mut valid = Vec::with_capacity(flat.len());
        let mut columns = Vec::with_capacity(variants.len() + 1);
        for value in &flat {
            let tag = variants
                .iter()
                .position(|variant| {
                    value.data_type() == *variant
                        || graph_value_types_compatible(&value.data_type(), variant)
                })
                .unwrap_or(0);
            tags.push(i8::try_from(tag).map_err(|_| {
                DataFusionError::Execution("cypher_list_plus has too many value variants".into())
            })?);
            valid.push(!value.is_null());
        }
        columns.push(Arc::new(datafusion::arrow::array::Int8Array::from(tags.clone())) as ArrayRef);
        for (variant_index, variant) in variants.iter().enumerate() {
            let null = ScalarValue::try_new_null(variant)?;
            let values = flat.iter().zip(&tags).map(|(value, tag)| {
                if usize::try_from(*tag).ok() == Some(variant_index) {
                    value.clone()
                } else {
                    null.clone()
                }
            });
            columns.push(ScalarValue::iter_to_array(values)?);
        }
        let values = datafusion::arrow::array::StructArray::new(
            fields.clone(),
            columns,
            Some(NullBuffer::from(valid)),
        );
        let out = ListArray::new(
            field.clone(),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(values) as ArrayRef,
            Some(NullBuffer::from(validity)),
        );
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

/// Append a tagged heterogeneous element to each list row without round-tripping
/// every existing element through [`ScalarValue`]. A runtime element whose tag is
/// itself a list still uses Cypher's dynamic list concatenation semantics; only
/// those nested children are promoted to the enclosing tagged depth.
#[allow(
    clippy::too_many_lines,
    reason = "one range-assembly pass keeps offsets, validity, and three Arrow sources synchronized"
)]
fn invoke_tagged_list_element_plus(
    left: &datafusion::arrow::array::ArrayRef,
    right: &datafusion::arrow::array::ArrayRef,
    return_type: &DataType,
) -> datafusion::error::Result<Option<datafusion::arrow::array::ArrayRef>> {
    use arrow_data::transform::MutableArrayData;
    use datafusion::arrow::array::{Array, Int8Array, ListArray, StructArray, make_array};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::error::ArrowError;
    use datafusion::error::DataFusionError;
    use std::sync::Arc;

    let Some(left) = left.as_any().downcast_ref::<ListArray>() else {
        return Ok(None);
    };
    let Some(right) = right.as_any().downcast_ref::<StructArray>() else {
        return Ok(None);
    };
    let DataType::List(return_field) = return_type else {
        return Ok(None);
    };
    if left.value_type() != right.data_type().clone()
        || return_field.data_type() != right.data_type()
        || !is_het_struct_type(Some(right.data_type()))
    {
        return Ok(None);
    }

    let Some(tags) = right
        .column_by_name("__het_tag")
        .and_then(|column| column.as_any().downcast_ref::<Int8Array>())
    else {
        return Ok(None);
    };
    let Some(nested) = right
        .column_by_name("__het_list")
        .and_then(|column| column.as_any().downcast_ref::<ListArray>())
    else {
        return Ok(None);
    };

    let nested_values = nested.values();
    let nested_offsets = nested.value_offsets();
    let mut promoted_ranges = vec![None; right.len()];
    for row in 0..right.len() {
        if right.is_null(row) || tags.value(row) != 4 || nested.is_null(row) {
            continue;
        }
        let start = usize::try_from(nested_offsets[row]).map_err(|_| {
            DataFusionError::ArrowError(
                Box::new(ArrowError::ComputeError(
                    "negative heterogeneous-list offset".into(),
                )),
                None,
            )
        })?;
        let end = usize::try_from(nested_offsets[row + 1]).map_err(|_| {
            DataFusionError::ArrowError(
                Box::new(ArrowError::ComputeError(
                    "negative heterogeneous-list offset".into(),
                )),
                None,
            )
        })?;
        promoted_ranges[row] = Some((start, end));
    }
    let promoted = if nested_values.is_empty() {
        Arc::new(right.slice(0, 0)) as datafusion::arrow::array::ArrayRef
    } else {
        promote_het_array(nested_values, right.data_type())?
    };

    let left_data = left.values().to_data();
    let right_data = right.to_data();
    let promoted_data = promoted.to_data();
    let capacity = left.values().len() + right.len() + promoted.len();
    let mut values = MutableArrayData::new(
        vec![&left_data, &right_data, &promoted_data],
        true,
        capacity,
    );
    let left_offsets = left.value_offsets();
    let mut offsets = Vec::with_capacity(left.len() + 1);
    let mut validity = Vec::with_capacity(left.len());
    let mut output_len = 0usize;
    offsets.push(0i32);
    for row in 0..left.len() {
        if left.is_null(row) {
            validity.push(false);
            offsets.push(i32::try_from(output_len).map_err(|_| {
                DataFusionError::Execution("cypher_list_plus: list too long".into())
            })?);
            continue;
        }
        validity.push(true);
        let start = usize::try_from(left_offsets[row]).map_err(|_| {
            DataFusionError::Execution("cypher_list_plus: negative list offset".into())
        })?;
        let end = usize::try_from(left_offsets[row + 1]).map_err(|_| {
            DataFusionError::Execution("cypher_list_plus: negative list offset".into())
        })?;
        values.extend(0, start, end);
        output_len += end - start;
        if let Some((start, end)) = promoted_ranges[row] {
            values.extend(2, start, end);
            output_len += end - start;
        } else {
            values.extend(1, row, row + 1);
            output_len += 1;
        }
        offsets.push(
            i32::try_from(output_len).map_err(|_| {
                DataFusionError::Execution("cypher_list_plus: list too long".into())
            })?,
        );
    }

    let values = make_array(values.freeze());
    Ok(Some(Arc::new(ListArray::new(
        return_field.clone(),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        Some(NullBuffer::from(validity)),
    ))))
}

/// Promote a tagged value array to a deeper version of the same recursive
/// heterogeneous schema. Existing buffers are reused; only missing deeper
/// list/map fields are introduced as null arrays.
fn promote_het_array(
    source: &datafusion::arrow::array::ArrayRef,
    target: &DataType,
) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef> {
    use datafusion::arrow::array::{Array, ListArray, StructArray, new_null_array};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::error::DataFusionError;
    use std::sync::Arc;

    if source.data_type() == target {
        return Ok(source.clone());
    }
    match (source.data_type(), target) {
        (DataType::Struct(_), DataType::Struct(target_fields)) => {
            let source = source
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "heterogeneous value has a non-struct physical array".into(),
                    )
                })?;
            let columns = target_fields
                .iter()
                .map(|field| {
                    source.column_by_name(field.name()).map_or_else(
                        || Ok(new_null_array(field.data_type(), source.len())),
                        |column| promote_het_array(column, field.data_type()),
                    )
                })
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::new(
                target_fields.clone(),
                columns,
                source.nulls().cloned(),
            )))
        }
        (DataType::List(_), DataType::List(target_field)) => {
            let source = source.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                DataFusionError::Internal("heterogeneous list has a non-list physical array".into())
            })?;
            let values = promote_het_array(source.values(), target_field.data_type())?;
            Ok(Arc::new(ListArray::new(
                target_field.clone(),
                source.offsets().clone(),
                values,
                source.nulls().cloned(),
            )))
        }
        _ => Ok(cast(source, target)?),
    }
}

fn list_plus_return_type(arg_types: &[DataType]) -> DataType {
    use datafusion::arrow::datatypes::Fields;

    let value_types = arg_types
        .iter()
        .flat_map(|arg_type| {
            let value_type = list_item_type(arg_type).unwrap_or(arg_type);
            dynamic_variant_types(value_type)
        })
        .filter(|value_type| !matches!(value_type, DataType::Null))
        .collect::<Vec<_>>();
    if let Some(first) = value_types.first()
        && is_graph_value_struct(first)
        && value_types
            .iter()
            .all(|value_type| graph_value_types_compatible(first, value_type))
    {
        // Widen nested nullability across variants so invoke-time
        // `ScalarValue::cast_to` never narrows under DF54 (#467).
        let unified = value_types
            .iter()
            .skip(1)
            .try_fold((*first).clone(), |acc, ty| {
                unify_graph_value_nullability(&acc, ty)
            });
        return DataType::new_list(unified.unwrap_or_else(|| (*first).clone()), true);
    }

    let mut variants = Vec::new();
    for arg_type in arg_types {
        let value_type = list_item_type(arg_type).unwrap_or(arg_type);
        if let DataType::Struct(fields) = value_type
            && fields
                .iter()
                .any(|field| field.name().starts_with("__het_value_"))
        {
            for field in fields
                .iter()
                .filter(|field| field.name().starts_with("__het_value_"))
            {
                if !variants.contains(field.data_type()) {
                    variants.push(field.data_type().clone());
                }
            }
        } else if let DataType::Struct(fields) = value_type
            && fields.iter().any(|field| field.name() == "__het_tag")
        {
            for (name, data_type) in [
                ("__het_int", DataType::Int64),
                ("__het_float", DataType::Float64),
                ("__het_str", DataType::Utf8),
                ("__het_bool", DataType::Boolean),
            ] {
                if fields.iter().any(|field| field.name() == name) && !variants.contains(&data_type)
                {
                    variants.push(data_type);
                }
            }
        } else if !matches!(value_type, DataType::Null) && !variants.contains(value_type) {
            variants.push(value_type.clone());
        }
    }
    if variants.is_empty() {
        variants.push(DataType::Null);
    }
    let mut fields = vec![Field::new("__het_tag", DataType::Int8, false)];
    fields.extend(
        variants
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| Field::new(format!("__het_value_{index}"), data_type, true)),
    );
    DataType::new_list(DataType::Struct(Fields::from(fields)), true)
}

fn list_plus_has_graph_value(arg_types: &[DataType]) -> bool {
    arg_types.iter().any(|arg_type| {
        let value_type = list_item_type(arg_type).unwrap_or(arg_type);
        dynamic_variant_types(value_type)
            .into_iter()
            .any(is_graph_value_struct)
    })
}

fn is_graph_value_struct(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Struct(fields) if fields.iter().any(|field| {
        matches!(
            field.name().as_str(),
            "node_uuid" | "edge_uuid" | "nodes" | "relationships"
        )
    }))
}

fn is_dynamic_variant_struct(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Struct(fields) if fields
        .iter()
        .any(|field| field.name().starts_with("__het_value_")))
}

fn dynamic_variant_types(data_type: &DataType) -> Vec<&DataType> {
    if let DataType::Struct(fields) = data_type {
        let variants = fields
            .iter()
            .filter(|field| field.name().starts_with("__het_value_"))
            .map(|field| field.data_type())
            .collect::<Vec<_>>();
        if !variants.is_empty() {
            return variants;
        }
    }
    vec![data_type]
}

fn list_item_type(dt: &DataType) -> Option<&DataType> {
    match dt {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
            Some(f.data_type())
        }
        _ => None,
    }
}

fn list_plus_depth(arg_types: &[DataType]) -> usize {
    arg_types
        .iter()
        .filter_map(|data_type| {
            list_item_type(data_type)
                .or(Some(data_type))
                .and_then(het_depth_for_data_type)
        })
        .max()
        .unwrap_or(0)
}

fn het_depth_for_data_type(data_type: &DataType) -> Option<usize> {
    if is_het_struct_type(Some(data_type)) {
        return het_struct_type_depth(data_type);
    }
    match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8 => Some(0),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Some(1 + het_depth_for_data_type(field.data_type())?)
        }
        DataType::Struct(fields) if is_plain_map_struct_type(data_type) => fields
            .iter()
            .filter_map(|field| het_depth_for_data_type(field.data_type()))
            .max()
            .map_or(Some(1), |depth| Some(1 + depth)),
        _ => None,
    }
}

fn het_struct_type_depth(data_type: &DataType) -> Option<usize> {
    let DataType::Struct(fields) = data_type else {
        return None;
    };
    let Some(list_field) = fields.iter().find(|field| field.name() == "__het_list") else {
        return Some(0);
    };
    match list_field.data_type() {
        DataType::List(inner) => het_struct_type_depth(inner.data_type()).map(|depth| depth + 1),
        _ => Some(0),
    }
}

fn list_elements_at(
    array: &datafusion::arrow::array::ArrayRef,
    row: usize,
) -> datafusion::error::Result<Option<Vec<ScalarValue>>> {
    use datafusion::arrow::array::{Array, FixedSizeListArray, LargeListArray, ListArray};

    let values = if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        if list.is_null(row) {
            return Ok(None);
        }
        list.value(row)
    } else if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        if list.is_null(row) {
            return Ok(None);
        }
        list.value(row)
    } else if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
        if list.is_null(row) {
            return Ok(None);
        }
        list.value(row)
    } else {
        return Ok(None);
    };

    (0..values.len())
        .map(|i| ScalarValue::try_from_array(&values, i).map(unwrap_het))
        .collect::<datafusion::error::Result<Vec<_>>>()
        .map(Some)
}

fn scalar_list_elements(
    value: &ScalarValue,
) -> datafusion::error::Result<Option<Vec<ScalarValue>>> {
    use datafusion::arrow::array::Array;

    match value {
        ScalarValue::List(list) => {
            if list.is_null(0) {
                return Ok(None);
            }
            let values = list.value(0);
            (0..values.len())
                .map(|i| ScalarValue::try_from_array(&values, i).map(unwrap_het))
                .collect::<datafusion::error::Result<Vec<_>>>()
                .map(Some)
        }
        ScalarValue::LargeList(list) => {
            if list.is_null(0) {
                return Ok(None);
            }
            let values = list.value(0);
            (0..values.len())
                .map(|i| ScalarValue::try_from_array(&values, i).map(unwrap_het))
                .collect::<datafusion::error::Result<Vec<_>>>()
                .map(Some)
        }
        _ => Ok(None),
    }
}

/// Const-fold an all-map list of DIFFERENT shapes into a homogeneous
/// `List<Struct<union-of-keys>>` literal — each map padded with a typed null for
/// keys it lacks (Cypher: a missing key reads as `null`), so `x.field` access
/// works and the list is a literal that can nest. `None` on an unresolvable key
/// type conflict (same key, two different non-null types) — left to `make_array`.
/// (#1005)
fn all_map_union_list(scalars: &[ScalarValue]) -> Option<DfExpr> {
    use datafusion::arrow::array::{Array, ArrayRef, StructArray, new_null_array};
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
    use datafusion::arrow::compute::{cast, concat};
    use datafusion::arrow::datatypes::{Field, Fields};
    use std::collections::HashMap;
    use std::sync::Arc;

    // Ordered union of key → resolved (non-null) type; a null-typed field yields
    // to a later real type, two differing real types are a conflict.
    let mut order: Vec<String> = Vec::new();
    let mut types: HashMap<String, DataType> = HashMap::new();
    for s in scalars {
        let arr = match s {
            ScalarValue::Struct(a) => a,
            ScalarValue::Null => continue,
            _ => return None,
        };
        for f in arr.fields() {
            let t = f.data_type().clone();
            match types.get(f.name()) {
                None => {
                    order.push(f.name().clone());
                    types.insert(f.name().clone(), t);
                }
                Some(prev) if *prev == DataType::Null => {
                    types.insert(f.name().clone(), t);
                }
                Some(prev) if t != DataType::Null && t != *prev => return None,
                _ => {}
            }
        }
    }
    let union_fields: Fields = order
        .iter()
        .map(|n| Field::new(n, types.get(n).cloned().unwrap_or(DataType::Null), true))
        .collect::<Vec<_>>()
        .into();

    // One concatenated column per union key (each row cast to the union type, a
    // missing key or a null-list-element → a typed null).
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(order.len());
    for name in &order {
        let ut = types.get(name).cloned().unwrap_or(DataType::Null);
        let mut pieces: Vec<ArrayRef> = Vec::with_capacity(scalars.len());
        for s in scalars {
            let piece = match s {
                ScalarValue::Struct(a) => a
                    .column_by_name(name)
                    .and_then(|c| cast(c, &ut).ok())
                    .unwrap_or_else(|| new_null_array(&ut, 1)),
                _ => new_null_array(&ut, 1),
            };
            pieces.push(piece);
        }
        let refs: Vec<&dyn Array> = pieces.iter().map(AsRef::as_ref).collect();
        columns.push(concat(&refs).ok()?);
    }
    // A `null` list element (not a map) → a null struct row.
    let valid: NullBuffer = scalars.iter().map(|s| !s.is_null()).collect();
    let elem = StructArray::try_new(union_fields, columns, Some(valid)).ok()?;
    let n = scalars.len();
    let list_field = Arc::new(Field::new("item", elem.data_type().clone(), true));
    let list = datafusion::arrow::array::ListArray::new(
        list_field,
        OffsetBuffer::from_lengths([n]),
        Arc::new(elem),
        None,
    );
    Some(DfExpr::Literal(ScalarValue::List(Arc::new(list)), None))
}

/// Lower an [`IrLiteral`] to a DataFusion [`Expr::Literal`].
fn lower_literal(lit_val: &IrLiteral) -> DfExpr {
    lit(ir_literal_to_scalar(lit_val))
}

/// Convert an [`IrLiteral`] to a DataFusion [`ScalarValue`].
///
/// The single source of truth for the IR-literal → Arrow-scalar mapping, used
/// both for literal expression lowering ([`lower_literal`]) and for binding
/// query parameters to placeholder values (`$param` injection, #584).
#[must_use]
pub fn ir_literal_to_scalar(lit_val: &IrLiteral) -> ScalarValue {
    match lit_val {
        IrLiteral::Null => ScalarValue::Null,
        IrLiteral::Bool(b) => ScalarValue::Boolean(Some(*b)),
        IrLiteral::Int(n) => ScalarValue::Int64(Some(*n)),
        IrLiteral::Float(f) => ScalarValue::Float64(Some(*f)),
        IrLiteral::Str(s) => ScalarValue::Utf8(Some(s.clone())),
        IrLiteral::Uuid(uuid) => ScalarValue::FixedSizeBinary(16, Some(uuid.to_vec())),
        IrLiteral::Duration {
            months,
            days,
            seconds,
            nanos,
        } => duration_scalar(Some(crate::temporal::DurationValue {
            months: *months,
            days: *days,
            seconds: *seconds,
            nanos: *nanos,
        })),
        IrLiteral::DateTime(us) => ScalarValue::TimestampMicrosecond(Some(*us), Some("UTC".into())),
        IrLiteral::Date(days) => date_scalar(Some(*days)),
        IrLiteral::LocalDateTime { days, nanos } => localdatetime_scalar(Some((*days, *nanos))),
        IrLiteral::Time(nanos) => ScalarValue::Time64Nanosecond(Some(*nanos)),
        IrLiteral::ZonedTime { nanos, offset } => time_scalar(Some((*nanos, *offset))),
        IrLiteral::ZonedDateTime {
            days,
            nanos,
            offset,
            zone,
        } => datetime_scalar(Some((*days, *nanos, *offset, zone.clone()))),
        // A homogeneous list → a `ScalarValue::List` of the element scalars; the
        // inner type is the first element's (re-typing untyped nulls to it so the
        // array stays homogeneous, as the list-literal lowering does). (#1006)
        IrLiteral::List(items) => {
            let scalars: Vec<ScalarValue> = items.iter().map(ir_literal_to_scalar).collect();
            let elem_type = scalars
                .iter()
                .find(|s| !s.is_null())
                .map_or(DataType::Null, ScalarValue::data_type);
            let typed: Vec<ScalarValue> = scalars
                .iter()
                .map(|s| {
                    if matches!(s, ScalarValue::Null) {
                        ScalarValue::try_from(&elem_type).unwrap_or(ScalarValue::Null)
                    } else {
                        s.clone()
                    }
                })
                .collect();
            ScalarValue::List(ScalarValue::new_list(&typed, &elem_type, true))
        }
        IrLiteral::Map(entries) => {
            let scalars: Vec<(String, ScalarValue)> = entries
                .iter()
                .map(|(key, value)| (key.clone(), ir_literal_to_scalar(value)))
                .collect();
            const_map_scalar(&scalars).expect("IR map literal should lower to an Arrow struct")
        }
    }
}

/// Convert a DataFusion [`ScalarValue`] back to an [`IrLiteral`] for storage as
/// a property value (#791 SET).
///
/// The inverse of [`ir_literal_to_scalar`], used by the SET execution node:
/// after evaluating a value expression per row it gets a `ScalarValue` that must
/// be stored as an `IrLiteral`. Numeric and temporal widths the writer does not
/// have a dedicated literal for are normalised to the nearest `IrLiteral`
/// (smaller ints → `Int`, `Float32` → `Float`, dates/times → `DateTime`). A
/// `null` scalar (typed or untyped) → [`IrLiteral::Null`].
///
/// Lists and temporal structs are converted recursively. Maps, graph values,
/// and other non-property shapes return an invalid-property-type error.
///
/// # Errors
/// Returns [`LoweringError::InvalidType`] for a value that openCypher does not
/// permit as a stored property.
pub fn scalar_to_ir_literal(value: &ScalarValue) -> Result<IrLiteral, LoweringError> {
    // Any null (typed `Int64(None)` or untyped `Null`) stores as Cypher null.
    if value.is_null() {
        return Ok(IrLiteral::Null);
    }
    let lit = match value {
        ScalarValue::Boolean(Some(b)) => IrLiteral::Bool(*b),
        ScalarValue::Int8(Some(n)) => IrLiteral::Int(i64::from(*n)),
        ScalarValue::Int16(Some(n)) => IrLiteral::Int(i64::from(*n)),
        ScalarValue::Int32(Some(n)) => IrLiteral::Int(i64::from(*n)),
        ScalarValue::Int64(Some(n)) => IrLiteral::Int(*n),
        ScalarValue::UInt8(Some(n)) => IrLiteral::Int(i64::from(*n)),
        ScalarValue::UInt16(Some(n)) => IrLiteral::Int(i64::from(*n)),
        ScalarValue::UInt32(Some(n)) => IrLiteral::Int(i64::from(*n)),
        ScalarValue::UInt64(Some(n)) => i64::try_from(*n).map(IrLiteral::Int).map_err(|_| {
            LoweringError::UnsupportedExpr(format!("SET value {n} exceeds the i64 range"))
        })?,
        ScalarValue::Float32(Some(f)) => IrLiteral::Float(f64::from(*f)),
        ScalarValue::Float64(Some(f)) => IrLiteral::Float(*f),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => IrLiteral::Str(s.clone()),
        ScalarValue::FixedSizeBinary(16, Some(_)) => {
            return Err(LoweringError::InvalidType(
                "UUID values cannot be stored as graph properties".into(),
            ));
        }
        // Flat native-Arrow duration widths carry no month/day part. Split each
        // unit into whole seconds + non-negative nanos-of-second WITHOUT forming a
        // `*1e9` total (which would overflow i64 for large native durations — the
        // seconds field stores them directly). (#1011)
        ScalarValue::DurationSecond(Some(s)) => duration_value_to_ir(dur_secs_nanos(*s, 0)),
        ScalarValue::DurationMillisecond(Some(ms)) => duration_value_to_ir(dur_secs_nanos(
            ms.div_euclid(1_000),
            ms.rem_euclid(1_000) * 1_000_000,
        )),
        ScalarValue::DurationMicrosecond(Some(us)) => duration_value_to_ir(dur_secs_nanos(
            us.div_euclid(1_000_000),
            us.rem_euclid(1_000_000) * 1_000,
        )),
        ScalarValue::DurationNanosecond(Some(ns)) => {
            duration_value_to_ir(crate::temporal::DurationValue::from_total_nanos(0, 0, *ns))
        }
        ScalarValue::TimestampMicrosecond(Some(us), _) => IrLiteral::DateTime(*us),
        ScalarValue::TimestampSecond(Some(s), _) => IrLiteral::DateTime(s * 1_000_000),
        ScalarValue::TimestampMillisecond(Some(ms), _) => IrLiteral::DateTime(ms * 1_000),
        ScalarValue::TimestampNanosecond(Some(ns), _) => IrLiteral::DateTime(ns / 1_000),
        // A date keeps its date identity (ADR 0009/0012): a `Struct{epoch_day}` of
        // i64 days, not coerced to a `DateTime` — so it reads back and renders as a
        // date.
        ScalarValue::Struct(arr) if is_date_struct(&DataType::Struct(arr.fields().clone())) => {
            match date_struct_value(arr, 0) {
                Some(days) => IrLiteral::Date(days),
                None => IrLiteral::Null,
            }
        }
        // A typed `duration` struct (#920) keeps its months/days/seconds/nanos model.
        ScalarValue::Struct(arr) if is_duration_struct(&DataType::Struct(arr.fields().clone())) => {
            match duration_struct_parts(arr, 0) {
                Some(d) => duration_value_to_ir(d),
                None => IrLiteral::Null,
            }
        }
        // A typed `localdatetime` struct (#920): date-days + nanos-of-day.
        ScalarValue::Struct(arr)
            if is_localdatetime_struct(&DataType::Struct(arr.fields().clone())) =>
        {
            match localdatetime_struct_parts(arr, 0) {
                Some((days, nanos)) => IrLiteral::LocalDateTime { days, nanos },
                None => IrLiteral::Null,
            }
        }
        // A typed `localtime` (#920): nanoseconds-of-day, no zone.
        ScalarValue::Time64Nanosecond(Some(n)) => IrLiteral::Time(*n),
        // A typed `time` struct (#920): time-of-day + UTC offset.
        ScalarValue::Struct(arr) if is_time_struct(&DataType::Struct(arr.fields().clone())) => {
            match time_struct_parts(arr, 0) {
                Some((nanos, offset)) => IrLiteral::ZonedTime { nanos, offset },
                None => IrLiteral::Null,
            }
        }
        // A typed `datetime` struct (#920): date + time + offset + named zone.
        ScalarValue::Struct(arr) if is_datetime_struct(&DataType::Struct(arr.fields().clone())) => {
            match datetime_struct_parts(arr, 0) {
                Some((days, nanos, offset, zone)) => IrLiteral::ZonedDateTime {
                    days,
                    nanos,
                    offset,
                    zone,
                },
                None => IrLiteral::Null,
            }
        }
        // A homogeneous list (#1006): recurse element-wise (a null element →
        // `IrLiteral::Null`; an element type the gate rejects propagates its
        // error). Single-row `List`/`LargeList` scalars carry the elements at
        // row 0.
        ScalarValue::List(arr) => list_scalar_to_ir_literal(&arr.value(0))?,
        ScalarValue::LargeList(arr) => list_scalar_to_ir_literal(&arr.value(0))?,
        other => {
            // Plain maps, graph structs, and any width not handled above are
            // invalid openCypher property values.
            return Err(LoweringError::InvalidType(format!(
                "invalid property type for SET value: {:?}",
                other.data_type()
            )));
        }
    };
    Ok(lit)
}

/// Build an [`IrLiteral::List`] from a list scalar's element array, recursing
/// through [`scalar_to_ir_literal`] per element. (#1006)
fn list_scalar_to_ir_literal(
    elems: &datafusion::arrow::array::ArrayRef,
) -> Result<IrLiteral, LoweringError> {
    let mut items = Vec::with_capacity(elems.len());
    for j in 0..elems.len() {
        let ev = ScalarValue::try_from_array(elems, j).map_err(|e| {
            LoweringError::UnsupportedExpr(format!("list element is not a scalar value: {e}"))
        })?;
        items.push(scalar_to_ir_literal(&ev)?);
    }
    Ok(IrLiteral::List(items))
}

/// Canonicalise a literal-string temporal-constructor argument. Returns `None`
/// when the constructor doesn't take a string, or the string isn't a form the
/// `temporal` module recognises (the caller then falls back to the runtime
/// path). (#599)
fn render_temporal(name: &str, s: &str) -> Option<String> {
    use crate::temporal;
    match name {
        "date" => temporal::render_date(s),
        "localtime" => temporal::render_local_time(s),
        "time" => temporal::render_time(s),
        "localdatetime" => temporal::render_local_date_time(s),
        "datetime" => temporal::render_date_time(s),
        "duration" => temporal::render_duration(s),
        _ => None,
    }
}

/// Look up a Cypher built-in function name and produce the DataFusion call.
///
/// Returns `None` if the name is not in the built-in table.
#[allow(
    clippy::too_many_lines,
    reason = "a flat one-arm-per-builtin dispatch table; clearest kept inline"
)]
fn resolve_builtin(
    name: &str,
    args: Vec<DfExpr>,
    path_hydration: impl FnOnce() -> Option<PathNodeHydration>,
) -> Option<DfExpr> {
    use datafusion::functions::math::expr_fn as mfn;
    use datafusion::functions::string::expr_fn as sfn;

    let name = name.to_ascii_lowercase();
    let mut a = args;
    match name.as_str() {
        // String functions
        "toupper" | "upper" => Some(sfn::upper(a.remove(0))),
        "tolower" | "lower" => Some(sfn::lower(a.remove(0))),
        "trim" => Some(sfn::btrim(a)),
        "ltrim" => Some(sfn::ltrim(a)),
        "rtrim" => Some(sfn::rtrim(a)),
        "string.concat" | "concat" => Some(sfn::concat(a)),
        "replace" => Some(sfn::replace(a.remove(0), a.remove(0), a.remove(0))),
        "substring" if a.len() == 2 || a.len() == 3 => Some(cypher_substring(a)),
        // Unambiguously a string character-count operation.
        "char_length" | "character_length" => Some(
            datafusion::functions::unicode::expr_fn::char_length(a.remove(0)),
        ),
        // Type conversion. `toString` routes through `cypher_to_string` so a
        // typed temporal (Date32/Time64/`localdatetime`/`time` struct) renders to
        // its canonical openCypher string; every other type falls back to a plain
        // `Utf8` cast (unchanged behaviour). The `datetime` renderer arm lands with
        // the `datetime`-struct migration. (ADR 0009)
        "tostring" => Some(CYPHER_TO_STRING.call(vec![a.remove(0)])),
        "tointeger" => Some(CYPHER_TO_INTEGER.call(vec![a.remove(0)])),
        "tofloat" => Some(CYPHER_TO_FLOAT.call(vec![a.remove(0)])),
        "toboolean" => Some(CYPHER_TO_BOOLEAN.call(vec![a.remove(0)])),

        // Runtime `date(<expr>)` (a non-constant argument; constants are handled
        // as a `Date32` scalar in `lower_temporal`). `to_date` already yields a
        // typed `Date32`, so dates round-trip as one type — e.g.
        // `date(toString(d)) = d` compares `Date32 == Date32` (ADR 0009). A
        // `Date32` argument passes through unchanged; an ISO string is parsed.
        "date" if a.len() == 1 => {
            use datafusion::functions::datetime::expr_fn::to_date;
            Some(to_date(vec![a.remove(0)]))
        }

        // Math functions
        "abs" => Some(mfn::abs(a.remove(0))),
        "ceil" => Some(mfn::ceil(a.remove(0))),
        "floor" => Some(mfn::floor(a.remove(0))),
        "round" => Some(mfn::round(a)),
        "sqrt" => Some(mfn::sqrt(a.remove(0))),
        "log" => Some(mfn::log(a.remove(0), a.remove(0))),
        "exp" => Some(mfn::exp(a.remove(0))),
        "power" => Some(mfn::power(a.remove(0), a.remove(0))),
        // `rand()` → a random float in [0, 1). Non-deterministic, but the
        // Quantifier9-12 invariant scenarios only use it to build a random
        // sub-list and then assert a result that holds for ANY list. (#955)
        "rand" if a.is_empty() => Some(mfn::random()),
        // `sign(n)` → -1 / 0 / 1 (openCypher returns an integer). Arity-guarded so
        // a wrong-arity call errors rather than panicking on `remove(0)`.
        "sign" if a.len() == 1 => Some(cast(mfn::signum(a.remove(0)), DataType::Int64)),
        // `coalesce(a, b, …)` → first non-null argument (at least one).
        "coalesce" if !a.is_empty() => Some(datafusion::functions::core::expr_fn::coalesce(a)),
        // `tail(list)` → every element but the first.
        "tail" if a.len() == 1 => Some(datafusion::functions_nested::expr_fn::array_pop_front(
            a.remove(0),
        )),
        // `split(str, delim)` → list of substrings.
        "split" if a.len() == 2 => Some(datafusion::functions_nested::expr_fn::string_to_array(
            a.remove(0),
            a.remove(0),
            DfExpr::Literal(ScalarValue::Utf8(None), None),
        )),

        // Cypher `length(<list>)` → element count. openCypher `length` is
        // path/list-oriented, so it maps cleanly to `array_length` (#709) — for
        // a variable-length edge list `r`, `length(r)` is the hop count per path.
        "length" => Some(datafusion::functions_nested::expr_fn::array_length(
            a.remove(0),
        )),

        // Cypher `size()` — element count of a list OR character count of a
        // string. Polymorphic, so it dispatches on the argument's runtime type
        // in `cypher_size` (a static `ScalarUDF`) rather than statically mapping
        // to `array_length` (which would mis-handle `size("str")`).
        "size" => Some(CYPHER_SIZE.call(vec![a.remove(0)])),

        // ---- list / relationship-list access (#743) ----
        // openCypher list indexing is 0-based with negative-from-end and
        // null-on-out-of-range; DataFusion `array_element` is 1-based (negatives
        // already count from the end, OOB → null), so a non-negative index is
        // shifted +1 and negatives pass through. See `one_based_index`.
        "_subscript" => {
            let list = a.remove(0);
            let idx = a.remove(0);
            Some(datafusion::functions_nested::expr_fn::array_element(
                list,
                one_based_index(idx),
            ))
        }

        // `head(list)` / `last(list)` — first / last element.
        "head" => Some(datafusion::functions_nested::expr_fn::array_element(
            a.remove(0),
            lit(1_i64),
        )),
        "last" => Some(datafusion::functions_nested::expr_fn::array_element(
            a.remove(0),
            lit(-1_i64),
        )),

        // `r[start..end]` slicing. The parser emits distinct internal function
        // names for omitted bounds so explicit `null` can propagate to a null
        // list (#962). openCypher: 0-based, start inclusive, end **exclusive**,
        // negatives from end; DataFusion
        // `array_slice(list, begin, end)` is 1-based and end **inclusive**.
        // Translation (per bound, via CASE on sign):
        //   begin: omitted → 1; s>=0 → s+1; s<0 → s (from end)
        //   end:   omitted → array_length(list); e>=0 → e (excl e == incl e-1,
        //          and 1-based incl == 0-based e-1, so the 1-based bound is just
        //          e); e<0 → e-1 (exclusive → inclusive shifts one toward start)
        "_slice" => {
            let list = a.remove(0);
            let start = a.remove(0);
            let end = a.remove(0);
            Some(cypher_slice(list, Some(start), Some(end)))
        }
        "_slice_from_start" => {
            let list = a.remove(0);
            let end = a.remove(0);
            Some(cypher_slice(list, None, Some(end)))
        }
        "_slice_to_end" => {
            let list = a.remove(0);
            let start = a.remove(0);
            Some(cypher_slice(list, Some(start), None))
        }

        // `range(start, end [, step])` — an integer list INCLUSIVE of `end`
        // (Cypher), unlike DataFusion's `range(start, stop, step)` which is
        // exclusive (`[start, stop)`). Shift the stop one past `end` so `end` is
        // included: `+1` for an ascending range, `-1` for a literal-negative
        // step. `step` defaults to 1. (A non-literal step is assumed ascending.)
        "range" if a.len() == 2 || a.len() == 3 => {
            let from = a.remove(0);
            let end = a.remove(0);
            let by = if a.is_empty() {
                lit(1_i64)
            } else {
                a.remove(0)
            };
            Some(CYPHER_RANGE.call(vec![from, end, by]))
        }

        // `type(rel)` — the relation-type name. For a relationship-list element
        // (a `Struct<…, rel_type>`), read the `rel_type` field.
        "type" => Some(CYPHER_REL_TYPE.call(vec![a.remove(0)])),

        // Named-path internal builtins (#754) live in their own table.
        other => resolve_path_builtin(other, a, path_hydration),
    }
}

/// Build zero-based element ordinals for a list while preserving null versus
/// empty input (`null -> null`, `[] -> []`). Used by relationally lifted list
/// comprehensions whose element order must survive an unwind/regroup cycle.
pub(crate) fn list_index_range(list: DfExpr) -> DfExpr {
    let len = cast(
        datafusion::functions_nested::expr_fn::array_length(list),
        DataType::Int64,
    );
    CYPHER_RANGE.call(vec![lit(0_i64), len - lit(1_i64), lit(1_i64)])
}

fn cypher_substring(mut args: Vec<DfExpr>) -> DfExpr {
    let original = args.remove(0);
    let start = args.remove(0) + lit(1_i64);
    let substring = if args.is_empty() {
        datafusion::functions::unicode::expr_fn::substr(original, start)
    } else {
        datafusion::functions::unicode::expr_fn::substring(original, start, args.remove(0))
    };
    cast(substring, DataType::Utf8)
}

fn is_path_builtin_name(name: &str) -> bool {
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
fn resolve_path_builtin(
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
fn null_unless(present: DfExpr, value: DfExpr) -> DfExpr {
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

fn edge_present_qual(base: &str) -> DfExpr {
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

fn empty_utf8_list() -> DfExpr {
    DfExpr::Literal(
        ScalarValue::List(ScalarValue::new_list(&[], &DataType::Utf8, true)),
        None,
    )
}

fn empty_map_struct() -> DfExpr {
    DfExpr::Literal(
        ScalarValue::Struct(std::sync::Arc::new(
            datafusion::arrow::array::StructArray::new_empty_fields(1, None),
        )),
        None,
    )
}

fn null_utf8_list() -> DfExpr {
    null_unless(lit(false), empty_utf8_list())
}

fn node_labels_list(base: &str, label: Option<&str>, type_id_map: &HashMap<u32, String>) -> DfExpr {
    use datafusion::functions_nested::expr_fn::{array_concat, array_has, make_array};

    if type_id_map.is_empty() {
        return label.map_or_else(empty_utf8_list, |name| make_array(vec![lit(name)]));
    }

    let mut entries: Vec<(u32, &str)> = type_id_map
        .iter()
        .map(|(id, name)| (*id, name.as_str()))
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
    type_id_map: &HashMap<u32, String>,
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

/// openCypher 0-based index → DataFusion `array_element` 1-based index.
///
/// `CASE WHEN idx >= 0 THEN idx + 1 ELSE idx END` — non-negative indices shift
/// up by one; negative indices already count from the end in both systems.
fn one_based_index(idx: DfExpr) -> DfExpr {
    when(idx.clone().gt_eq(lit(0_i64)), idx.clone() + lit(1_i64))
        .otherwise(idx)
        .expect("CASE build is infallible for a single WHEN + ELSE")
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

/// Whether a lowered expression is the `Null` literal.
fn as_null_literal(e: &DfExpr) -> bool {
    matches!(e, DfExpr::Literal(ScalarValue::Null, _))
}

fn cypher_slice(list: DfExpr, start: Option<DfExpr>, end: Option<DfExpr>) -> DfExpr {
    let mut present = lit(true);
    let begin_expr = match start {
        Some(start) if as_null_literal(&start) => {
            present = lit(false);
            lit(1_i64)
        }
        Some(start) => {
            present = present.and(start.clone().is_not_null());
            when(start.clone().gt_eq(lit(0_i64)), start.clone() + lit(1_i64))
                .otherwise(start)
                .expect("CASE build")
        }
        None => lit(1_i64),
    };
    let end_expr = match end {
        Some(end) if as_null_literal(&end) => {
            present = lit(false);
            cast(
                datafusion::functions_nested::expr_fn::array_length(list.clone()),
                DataType::Int64,
            )
        }
        Some(end) => {
            present = present.and(end.clone().is_not_null());
            when(end.clone().gt_eq(lit(0_i64)), end.clone())
                .otherwise(end - lit(1_i64))
                .expect("CASE build")
        }
        None => cast(
            datafusion::functions_nested::expr_fn::array_length(list.clone()),
            DataType::Int64,
        ),
    };
    let slice =
        datafusion::functions_nested::expr_fn::array_slice(list, begin_expr, end_expr, None);
    null_unless(present, slice)
}

// ---------------------------------------------------------------------------
// Cypher conversion UDFs
// ---------------------------------------------------------------------------

static CYPHER_TO_INTEGER: LazyLock<ScalarUDF> = LazyLock::new(|| {
    ScalarUDF::new_from_impl(CypherConversion::new(CypherConversionKind::Integer))
});
static CYPHER_TO_FLOAT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherConversion::new(CypherConversionKind::Float)));
static CYPHER_TO_BOOLEAN: LazyLock<ScalarUDF> = LazyLock::new(|| {
    ScalarUDF::new_from_impl(CypherConversion::new(CypherConversionKind::Boolean))
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CypherConversionKind {
    Integer,
    Float,
    Boolean,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherConversion {
    kind: CypherConversionKind,
    signature: Signature,
}

impl CypherConversion {
    fn new(kind: CypherConversionKind) -> Self {
        Self {
            kind,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherConversion {
    fn name(&self) -> &'static str {
        match self.kind {
            CypherConversionKind::Integer => "cypher_to_integer",
            CypherConversionKind::Float => "cypher_to_float",
            CypherConversionKind::Boolean => "cypher_to_boolean",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(match self.kind {
            CypherConversionKind::Integer => DataType::Int64,
            CypherConversionKind::Float => DataType::Float64,
            CypherConversionKind::Boolean => DataType::Boolean,
        })
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{BooleanArray, Float64Array, Int64Array};

        let array = args.args[0].to_array(args.number_rows)?;
        match self.kind {
            CypherConversionKind::Integer => {
                let out: datafusion::error::Result<Int64Array> = (0..array.len())
                    .map(|i| {
                        let value = decoded_scalar_at(&array, i)?;
                        to_cypher_integer(&value)
                    })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(out?)))
            }
            CypherConversionKind::Float => {
                let out: datafusion::error::Result<Float64Array> = (0..array.len())
                    .map(|i| {
                        let value = decoded_scalar_at(&array, i)?;
                        to_cypher_float(&value)
                    })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(out?)))
            }
            CypherConversionKind::Boolean => {
                let out: datafusion::error::Result<BooleanArray> = (0..array.len())
                    .map(|i| {
                        let value = decoded_scalar_at(&array, i)?;
                        to_cypher_boolean(&value)
                    })
                    .collect();
                Ok(ColumnarValue::Array(std::sync::Arc::new(out?)))
            }
        }
    }
}

fn decoded_scalar_at(
    array: &datafusion::arrow::array::ArrayRef,
    row: usize,
) -> datafusion::error::Result<ScalarValue> {
    let value = ScalarValue::try_from_array(array, row)?;
    Ok(unwrap_het(value))
}

fn conversion_type_error(fn_name: &str, value: &ScalarValue) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::Execution(format!(
        "{fn_name}() cannot convert value of type {:?}",
        value.data_type()
    ))
}

fn to_cypher_integer(value: &ScalarValue) -> datafusion::error::Result<Option<i64>> {
    if value.is_null() {
        return Ok(None);
    }
    if let Some(i) = scalar_as_i128(value) {
        return i64::try_from(i)
            .map(Some)
            .map_err(|_| conversion_type_error("toInteger", value));
    }
    match value {
        ScalarValue::Float32(Some(f)) => Ok(trunc_float_to_i64(f64::from(*f))),
        ScalarValue::Float64(Some(f)) => Ok(trunc_float_to_i64(*f)),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Ok(s
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .and_then(trunc_float_to_i64)),
        _ => Err(conversion_type_error("toInteger", value)),
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "openCypher toInteger truncates finite floating values toward zero"
)]
fn trunc_float_to_i64(f: f64) -> Option<i64> {
    if !f.is_finite() {
        return None;
    }
    let truncated = f.trunc();
    if truncated < i64::MIN as f64 || truncated > i64::MAX as f64 {
        return None;
    }
    Some(truncated as i64)
}

fn to_cypher_float(value: &ScalarValue) -> datafusion::error::Result<Option<f64>> {
    if value.is_null() {
        return Ok(None);
    }
    if let Some(f) = scalar_as_f64(value) {
        return Ok(Some(f));
    }
    match value {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            Ok(s.parse::<f64>().ok().filter(|f| f.is_finite()))
        }
        _ => Err(conversion_type_error("toFloat", value)),
    }
}

fn to_cypher_boolean(value: &ScalarValue) -> datafusion::error::Result<Option<bool>> {
    if value.is_null() {
        return Ok(None);
    }
    match value {
        ScalarValue::Boolean(Some(b)) => Ok(Some(*b)),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => match s.as_str() {
            "true" => Ok(Some(true)),
            "false" => Ok(Some(false)),
            _ => Ok(None),
        },
        _ => Err(conversion_type_error("toBoolean", value)),
    }
}

fn cypher_float_string(f: f64) -> String {
    if f == 0.0 {
        "0.0".to_owned()
    } else if f.is_nan() {
        "NaN".to_owned()
    } else if f.is_infinite() {
        if f.is_sign_positive() {
            "Infinity".to_owned()
        } else {
            "-Infinity".to_owned()
        }
    } else {
        let mut s = f.to_string();
        if !s.contains('.') && !s.contains('e') && !s.contains('E') {
            s.push_str(".0");
        }
        s
    }
}

fn to_cypher_string(value: &ScalarValue) -> datafusion::error::Result<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }
    match value {
        ScalarValue::Int8(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::Int16(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::Int32(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::Int64(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::UInt8(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::UInt16(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::UInt32(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::UInt64(Some(n)) => Ok(Some(n.to_string())),
        ScalarValue::Float32(Some(f)) => Ok(Some(cypher_float_string(f64::from(*f)))),
        ScalarValue::Float64(Some(f)) => Ok(Some(cypher_float_string(*f))),
        ScalarValue::Boolean(Some(b)) => Ok(Some(b.to_string())),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Ok(Some(s.clone())),
        _ => Err(conversion_type_error("toString", value)),
    }
}

// ---------------------------------------------------------------------------
// cypher_size UDF
// ---------------------------------------------------------------------------

/// The Cypher `size()` scalar function: element count of a list **or** character
/// count of a string, dispatched on the argument's runtime type.
///
/// openCypher `size()` is polymorphic, so it cannot be statically mapped to a
/// single DataFusion function (`array_length` would mis-handle a string,
/// `char_length` a list). This UDF inspects the argument's [`DataType`] and
/// delegates to the matching Arrow kernel; an unsupported type yields `Null`.
///
/// Defined as a static [`ScalarUDF`] and invoked inline via
/// [`ScalarUDF::call`], so it carries its own implementation in the produced
/// `Expr` and needs no `SessionContext` registration.
static CYPHER_SIZE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherSize::new()));

/// Opaque per-row marker used beside literal-null grouping keys. DataFusion
/// otherwise removes the null key and changes an empty grouped aggregate into
/// a one-row global aggregate.
pub(crate) static CYPHER_ROW_MARKER: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherRowMarker::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherRowMarker {
    signature: Signature,
}

impl CypherRowMarker {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherRowMarker {
    fn name(&self) -> &'static str {
        "cypher_row_marker"
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
        let values = datafusion::arrow::array::BooleanArray::from(vec![true; args.number_rows]);
        Ok(ColumnarValue::Array(Arc::new(values)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherSize {
    signature: Signature,
}

impl CypherSize {
    fn new() -> Self {
        // One argument of any type; immutable (same input → same output).
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherSize {
    fn name(&self) -> &'static str {
        "cypher_size"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, Int64Array};
        use datafusion::arrow::compute::kernels::length::length;
        use datafusion::common::cast::{as_large_list_array, as_list_array};

        let array = args.args[0].to_array(args.number_rows)?;
        let out: Int64Array = match array.data_type() {
            // Element count per list row (null lists → null).
            DataType::List(_) => {
                let list = as_list_array(&array)?;
                (0..list.len())
                    .map(|i| {
                        (!list.is_null(i))
                            .then(|| i64::try_from(list.value(i).len()).unwrap_or(i64::MAX))
                    })
                    .collect()
            }
            DataType::LargeList(_) => {
                let list = as_large_list_array(&array)?;
                (0..list.len())
                    .map(|i| {
                        (!list.is_null(i))
                            .then(|| i64::try_from(list.value(i).len()).unwrap_or(i64::MAX))
                    })
                    .collect()
            }
            // Character/byte count for strings — Arrow's `length` kernel returns
            // the count as an integer array; cast to Int64 for a uniform return.
            DataType::Utf8 | DataType::LargeUtf8 => {
                let lengths = length(&array)?;
                let casted = datafusion::arrow::compute::cast(&lengths, &DataType::Int64)?;
                casted
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("cast to Int64 yields Int64Array")
                    .clone()
            }
            // A heterogeneous tagged element (ADR 0011) — e.g. the loop variable
            // of `none(x IN [[1, 2, 3], ['a']] WHERE size(x) = 3)`. Decode per
            // row and count a list payload's elements or a string payload's
            // bytes; any other payload falls through to null like the untyped
            // arm below.
            t if is_het_struct_type(Some(t)) => (0..array.len())
                .map(|i| {
                    let sv = ScalarValue::try_from_array(&array, i).ok()?;
                    match decode_het(&sv)? {
                        ScalarValue::List(l) => (!l.is_null(0))
                            .then(|| i64::try_from(l.value(0).len()).unwrap_or(i64::MAX)),
                        ScalarValue::LargeList(l) => (!l.is_null(0))
                            .then(|| i64::try_from(l.value(0).len()).unwrap_or(i64::MAX)),
                        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                            i64::try_from(s.len()).ok()
                        }
                        _ => None,
                    }
                })
                .collect(),
            // Unsupported argument type → all-null (Cypher `size` of a non-
            // list/string is undefined; null is the lenient choice).
            _ => (0..array.len()).map(|_| None).collect(),
        };
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// Graph metadata UDFs
// ---------------------------------------------------------------------------

static CYPHER_LABELS: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherGraphMetadata::new(GraphMetadataKind::Labels)));
static CYPHER_REL_TYPE: LazyLock<ScalarUDF> = LazyLock::new(|| {
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
// cypher_map_keys UDF
// ---------------------------------------------------------------------------

const ENTITY_PROPERTY_MAP_HET_DEPTH: usize = 3;

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherEntityProperties {
    signature: Signature,
}

impl CypherEntityProperties {
    fn new(arity: usize) -> Self {
        Self {
            signature: Signature::any(arity, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherEntityProperties {
    fn name(&self) -> &'static str {
        "cypher_entity_properties"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(het_fields(ENTITY_PROPERTY_MAP_HET_DEPTH)))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::ArrayRef;
        use datafusion::error::DataFusionError;
        use std::sync::Arc;

        let rows = args.number_rows;
        if args.args.is_empty() || !(args.args.len() - 1).is_multiple_of(2) {
            return Err(DataFusionError::Plan(
                "properties() entity map expects present plus key/value pairs".into(),
            ));
        }
        let cols: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|arg| arg.to_array(rows))
            .collect::<datafusion::error::Result<_>>()?;
        let mut maps = Vec::with_capacity(rows);
        for row in 0..rows {
            let present = match ScalarValue::try_from_array(&cols[0], row)? {
                ScalarValue::Boolean(Some(true)) => true,
                ScalarValue::Boolean(Some(false) | None) | ScalarValue::Null => false,
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "properties() entity presence must be boolean, got {other:?}"
                    )));
                }
            };
            if !present {
                maps.push(ScalarValue::Null);
                continue;
            }
            let mut entries = Vec::with_capacity((cols.len() - 1) / 2);
            for pair in cols[1..].chunks_exact(2) {
                let key = ScalarValue::try_from_array(&pair[0], row)?;
                let Some(key) = scalar_access_key(&key)? else {
                    continue;
                };
                let value = ScalarValue::try_from_array(&pair[1], row)?;
                if value.is_null() {
                    continue;
                }
                entries.push((key, unwrap_het(value)));
            }
            let map = const_map_scalar(&entries).ok_or_else(|| {
                DataFusionError::Execution(
                    "properties() could not encode entity property map".into(),
                )
            })?;
            maps.push(map);
        }
        let out = build_het_struct(&maps, ENTITY_PROPERTY_MAP_HET_DEPTH).ok_or_else(|| {
            DataFusionError::Execution("properties() could not encode entity property map".into())
        })?;
        Ok(ColumnarValue::Array(Arc::new(out)))
    }
}

static CYPHER_MAP_KEYS: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherMapKeys::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherMapKeys {
    signature: Signature,
}

impl CypherMapKeys {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherMapKeys {
    fn name(&self) -> &'static str {
        "cypher_map_keys"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::new_list(DataType::Utf8, true))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, ArrayRef, ListArray, StringArray, StructArray};
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use datafusion::error::DataFusionError;
        use std::sync::Arc;

        let rows = args.number_rows;
        let values = args.args[0].to_array(rows)?;
        if matches!(values.data_type(), DataType::Null) {
            let nulls = NullBuffer::from(vec![false; rows]);
            let list = ListArray::new(
                Arc::new(Field::new("item", DataType::Utf8, true)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i32; rows + 1])),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())) as ArrayRef,
                Some(nulls),
            );
            return Ok(ColumnarValue::Array(Arc::new(list)));
        }
        let DataType::Struct(fields) = values.data_type() else {
            return Err(DataFusionError::Execution(format!(
                "keys() requires a map, node, relationship, or null, got {:?}",
                values.data_type()
            )));
        };
        let map = values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("keys() expected a struct map".into()))?;
        if is_het_struct_type(Some(values.data_type())) {
            return tagged_map_keys(map, rows);
        }
        if !is_plain_map_struct_type(values.data_type()) {
            return Err(DataFusionError::Execution(format!(
                "keys() requires a map, node, relationship, or null, got {:?}",
                values.data_type()
            )));
        }
        let names: Vec<String> = fields.iter().map(|f| f.name().clone()).collect();
        let mut offsets = Vec::with_capacity(rows + 1);
        let mut values = Vec::new();
        let mut valid = Vec::with_capacity(rows);
        offsets.push(0_i32);
        for row in 0..rows {
            if map.is_null(row) {
                valid.push(false);
            } else {
                valid.push(true);
                values.extend(names.iter().cloned().map(Some));
            }
            offsets.push(i32::try_from(values.len()).map_err(|_| {
                DataFusionError::Execution("keys() result exceeded i32 list offsets".into())
            })?);
        }
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(StringArray::from(values)) as ArrayRef,
            Some(NullBuffer::from(valid)),
        );
        Ok(ColumnarValue::Array(Arc::new(list)))
    }
}

fn tagged_map_keys(
    map: &datafusion::arrow::array::StructArray,
    rows: usize,
) -> datafusion::error::Result<ColumnarValue> {
    use datafusion::arrow::array::{
        Array, ArrayRef, Int8Array, ListArray, StringArray, StructArray,
    };
    use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use datafusion::error::DataFusionError;
    use std::sync::Arc;

    let tags = map
        .column_by_name("__het_tag")
        .and_then(|c| c.as_any().downcast_ref::<Int8Array>())
        .ok_or_else(|| DataFusionError::Plan("tagged map is missing __het_tag".into()))?;
    let entries = map
        .column_by_name("__het_map")
        .and_then(|c| c.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| DataFusionError::Plan("tagged map is missing __het_map".into()))?;
    let mut offsets = Vec::with_capacity(rows + 1);
    let mut values = Vec::new();
    let mut valid = Vec::with_capacity(rows);
    offsets.push(0_i32);
    for row in 0..rows {
        if map.is_null(row) {
            valid.push(false);
        } else {
            if tags.value(row) != 5 {
                return Err(DataFusionError::Execution(
                    "keys() requires a map, node, relationship, or null".into(),
                ));
            }
            valid.push(true);
            if !entries.is_null(row) {
                let entry_values = entries.value(row);
                let entry_struct = entry_values
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        DataFusionError::Plan("tagged map entries must be structs".into())
                    })?;
                let map_keys = entry_struct
                    .column_by_name("__het_mkey")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                    .ok_or_else(|| {
                        DataFusionError::Plan("tagged map entries must carry __het_mkey".into())
                    })?;
                for idx in 0..entry_struct.len() {
                    if !map_keys.is_null(idx) {
                        values.push(Some(map_keys.value(idx).to_owned()));
                    }
                }
            }
        }
        offsets.push(i32::try_from(values.len()).map_err(|_| {
            DataFusionError::Execution("keys() result exceeded i32 list offsets".into())
        })?);
    }
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(StringArray::from(values)) as ArrayRef,
        Some(NullBuffer::from(valid)),
    );
    Ok(ColumnarValue::Array(Arc::new(list)))
}

// ---------------------------------------------------------------------------
// cypher_value_access UDF
// ---------------------------------------------------------------------------

static CYPHER_VALUE_ACCESS: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherValueAccess::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherStaticValueAccess {
    key: String,
    signature: Signature,
}

impl CypherStaticValueAccess {
    fn new(key: String) -> Self {
        Self {
            key,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherStaticValueAccess {
    fn name(&self) -> &'static str {
        "cypher_static_value_access"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        static_value_access_return_type(arg_types.first(), &self.key)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            static_value_access_return_type(
                args.arg_fields.first().map(|field| field.data_type()),
                &self.key,
            )?,
            true,
        )))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        let values = args.args[0].to_array(rows)?;
        let return_type = args.return_field.data_type();
        let null_value = ScalarValue::try_new_null(return_type)?;
        let output = (0..rows)
            .map(|row| {
                let value = ScalarValue::try_from_array(&values, row)?;
                let value = decode_het(&value).unwrap_or(value);
                if value.is_null() {
                    return Ok(null_value.clone());
                }
                let ScalarValue::Struct(value) = value else {
                    return Err(DataFusionError::Execution(
                        "InvalidArgumentValue: property access requires a map or graph element"
                            .into(),
                    ));
                };
                let Some(column) = value.column_by_name(&self.key) else {
                    return Ok(null_value.clone());
                };
                let result = ScalarValue::try_from_array(column, 0)?;
                if result.data_type() == *return_type {
                    Ok(result)
                } else if result.is_null() {
                    Ok(null_value.clone())
                } else {
                    Err(DataFusionError::Execution(format!(
                        "property `{}` has incompatible runtime type {:?}; expected {:?}",
                        self.key,
                        result.data_type(),
                        return_type
                    )))
                }
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        Ok(ColumnarValue::Array(ScalarValue::iter_to_array(output)?))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherValueAccess {
    signature: Signature,
}

impl CypherValueAccess {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherValueAccess {
    fn name(&self) -> &'static str {
        "cypher_value_access"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        value_access_return_type(arg_types.first())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        Ok(std::sync::Arc::new(Field::new(
            self.name(),
            value_access_return_type(args.arg_fields.first().map(|f| f.data_type()))?,
            true,
        )))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::StructArray;
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        let values = args.args[0].to_array(rows)?;
        let keys = args.args[1].to_array(rows)?;
        let return_type = args.return_field.data_type().clone();
        let null_value = ScalarValue::try_from(&return_type).unwrap_or(ScalarValue::Null);
        if matches!(values.data_type(), DataType::Null) {
            return Ok(ColumnarValue::Array(ScalarValue::iter_to_array(
                (0..rows).map(|_| null_value.clone()),
            )?));
        }
        if let Some(list) = ListView::from_array(&values) {
            let out = (0..rows)
                .map(|i| list_access_value(&list, &keys, i, &null_value))
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            return Ok(ColumnarValue::Array(ScalarValue::iter_to_array(out)?));
        }
        let map = values
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "dynamic subscript requires a list or map/entity struct, got {:?}",
                    values.data_type()
                ))
            })?;
        if is_het_struct_type(Some(values.data_type())) {
            let out = (0..rows)
                .map(|i| het_map_access_value(map, &keys, i, &null_value))
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            return Ok(ColumnarValue::Array(ScalarValue::iter_to_array(out)?));
        }
        let out = (0..rows)
            .map(|i| {
                if map.is_null(i) {
                    return Ok(null_value.clone());
                }
                let key = ScalarValue::try_from_array(&keys, i)?;
                let Some(key) = scalar_access_key(&key)? else {
                    return Ok(null_value.clone());
                };
                let Some(col) = map.column_by_name(&key) else {
                    return Ok(null_value.clone());
                };
                ScalarValue::try_from_array(col, i)
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        Ok(ColumnarValue::Array(ScalarValue::iter_to_array(out)?))
    }
}

fn value_access_return_type(dt: Option<&DataType>) -> datafusion::error::Result<DataType> {
    let Some(dt) = dt else {
        return Ok(DataType::Null);
    };
    match dt {
        DataType::Null => Ok(DataType::Null),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            Ok(field.data_type().clone())
        }
        dt if is_het_struct_type(Some(dt)) => het_value_access_return_type(dt),
        DataType::Struct(fields) => common_struct_field_type(fields),
        // Parameter values are bound after logical lowering. If a parameter later
        // turns out not to be a list/map/entity, defer the invalid-argument failure to
        // UDF invocation so Cypher observes it as a runtime type error.
        _ => Ok(DataType::Null),
    }
}

fn static_value_access_return_type(
    dt: Option<&DataType>,
    key: &str,
) -> datafusion::error::Result<DataType> {
    let Some(DataType::Struct(fields)) = dt else {
        return Ok(DataType::Null);
    };
    if !is_het_struct_type(dt) {
        return fields
            .iter()
            .find(|field| field.name() == key)
            .map_or(Ok(DataType::Null), |field| Ok(field.data_type().clone()));
    }
    if fields.iter().any(|field| field.name() == "__het_map") {
        return het_value_access_return_type(&DataType::Struct(fields.clone()));
    }
    let mut data_type = None;
    for variant in fields
        .iter()
        .filter(|field| field.name().starts_with("__het_value_"))
    {
        let DataType::Struct(value_fields) = variant.data_type() else {
            continue;
        };
        let Some(property) = value_fields.iter().find(|field| field.name() == key) else {
            continue;
        };
        if matches!(property.data_type(), DataType::Null) {
            continue;
        }
        match &data_type {
            None => data_type = Some(property.data_type().clone()),
            Some(existing) if existing == property.data_type() => {}
            Some(existing) => {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "property `{key}` has incompatible graph-value types {existing:?} and {:?}",
                    property.data_type()
                )));
            }
        }
    }
    Ok(data_type.unwrap_or(DataType::Null))
}

fn list_access_value(
    list: &ListView<'_>,
    keys: &datafusion::arrow::array::ArrayRef,
    row: usize,
    null_value: &ScalarValue,
) -> datafusion::error::Result<ScalarValue> {
    if list.is_null(row) {
        return Ok(null_value.clone());
    }
    let key = ScalarValue::try_from_array(keys, row)?;
    let Some(idx) = scalar_list_index(&key)? else {
        return Ok(null_value.clone());
    };
    let elems = list.value(row);
    let len = i64::try_from(elems.len()).map_err(|_| {
        datafusion::error::DataFusionError::Execution(
            "dynamic list access length exceeds i64 range".into(),
        )
    })?;
    let pos = if idx < 0 { len + idx } else { idx };
    if pos < 0 || pos >= len {
        return Ok(null_value.clone());
    }
    let pos = usize::try_from(pos).map_err(|_| {
        datafusion::error::DataFusionError::Execution(
            "dynamic list access index exceeds usize range".into(),
        )
    })?;
    ScalarValue::try_from_array(&elems, pos)
}

fn scalar_list_index(s: &ScalarValue) -> datafusion::error::Result<Option<i64>> {
    if s.is_null() {
        return Ok(None);
    }
    macro_rules! signed_index {
        ($value:expr) => {
            $value.map(i64::from)
        };
    }
    let idx = match s {
        ScalarValue::Int8(v) => signed_index!(*v),
        ScalarValue::Int16(v) => signed_index!(*v),
        ScalarValue::Int32(v) => signed_index!(*v),
        ScalarValue::Int64(v) => *v,
        ScalarValue::UInt8(v) => v.map(i64::from),
        ScalarValue::UInt16(v) => v.map(i64::from),
        ScalarValue::UInt32(v) => v.map(i64::from),
        ScalarValue::UInt64(v) => v.map(i64::try_from).transpose().map_err(|_| {
            datafusion::error::DataFusionError::Execution(
                "dynamic list access index exceeds i64 range".into(),
            )
        })?,
        other => {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "dynamic list access index must be an integer, got {other:?}"
            )));
        }
    };
    Ok(idx)
}

fn het_value_access_return_type(dt: &DataType) -> datafusion::error::Result<DataType> {
    use datafusion::error::DataFusionError;

    let DataType::Struct(fields) = dt else {
        unreachable!("caller checked het struct type")
    };
    let Some(map_field) = fields.iter().find(|f| f.name() == "__het_map") else {
        return Err(DataFusionError::Plan(
            "dynamic value access requires a tagged map element".into(),
        ));
    };
    let DataType::List(entry_field) = map_field.data_type() else {
        return Err(DataFusionError::Plan(
            "tagged map field must be a list".into(),
        ));
    };
    let DataType::Struct(entry_fields) = entry_field.data_type() else {
        return Err(DataFusionError::Plan(
            "tagged map entries must be structs".into(),
        ));
    };
    entry_fields
        .iter()
        .find(|f| f.name() == "__het_mval")
        .map(|f| f.data_type().clone())
        .ok_or_else(|| DataFusionError::Plan("tagged map entries must carry __het_mval".into()))
}

fn het_map_access_value(
    map: &datafusion::arrow::array::StructArray,
    keys: &datafusion::arrow::array::ArrayRef,
    row: usize,
    null_value: &ScalarValue,
) -> datafusion::error::Result<ScalarValue> {
    use datafusion::arrow::array::{Array, Int8Array, ListArray, StringArray, StructArray};
    use datafusion::error::DataFusionError;

    if map.is_null(row) {
        return Ok(null_value.clone());
    }
    let key = ScalarValue::try_from_array(keys, row)?;
    let Some(key) = scalar_access_key(&key)? else {
        return Ok(null_value.clone());
    };
    let tag = map
        .column_by_name("__het_tag")
        .and_then(|c| c.as_any().downcast_ref::<Int8Array>())
        .ok_or_else(|| DataFusionError::Plan("tagged value is missing __het_tag".into()))?;
    if tag.value(row) != 5 {
        return Err(DataFusionError::Execution(
            "invalid argument type: dynamic value access requires a map".into(),
        ));
    }
    let entries = map
        .column_by_name("__het_map")
        .and_then(|c| c.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| DataFusionError::Plan("tagged map value is missing __het_map".into()))?;
    if entries.is_null(row) {
        return Ok(null_value.clone());
    }
    let entry_values = entries.value(row);
    let entry_struct = entry_values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Plan("tagged map entries must be structs".into()))?;
    let map_keys = entry_struct
        .column_by_name("__het_mkey")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| DataFusionError::Plan("tagged map entries must carry __het_mkey".into()))?;
    let map_values = entry_struct
        .column_by_name("__het_mval")
        .ok_or_else(|| DataFusionError::Plan("tagged map entries must carry __het_mval".into()))?;
    for idx in 0..entry_struct.len() {
        if !map_keys.is_null(idx) && map_keys.value(idx) == key {
            return ScalarValue::try_from_array(map_values, idx);
        }
    }
    Ok(null_value.clone())
}

fn common_struct_field_type(
    fields: &datafusion::arrow::datatypes::Fields,
) -> datafusion::error::Result<DataType> {
    let mut dtype: Option<DataType> = None;
    for field in fields {
        let field_type = field.data_type();
        if matches!(field_type, DataType::Null) {
            continue;
        }
        match &dtype {
            None => dtype = Some(field_type.clone()),
            Some(prev) if prev == field_type => {}
            Some(prev) => {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "dynamic value access over mixed field types is not supported: {prev:?} and {field_type:?}"
                )));
            }
        }
    }
    Ok(dtype.unwrap_or(DataType::Null))
}

fn scalar_access_key(s: &ScalarValue) -> datafusion::error::Result<Option<String>> {
    if s.is_null() {
        return Ok(None);
    }
    match s {
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
            Ok(v.clone())
        }
        other => Err(datafusion::error::DataFusionError::Execution(format!(
            "dynamic map/property access key must be a string, got {other:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// cypher_reverse UDF
// ---------------------------------------------------------------------------

/// `reverse(x)` for an argument whose type is unknown at plan time (e.g. a
/// parameter or an unresolved property). Dispatches at runtime: a string
/// reverses its characters, a list its elements. The known-string / known-list
/// cases are routed directly to `unicode::reverse` / `array_reverse` at lowering
/// and never reach this UDF. (#955)
static CYPHER_REVERSE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherReverse::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherReverse {
    signature: Signature,
}

impl CypherReverse {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherReverse {
    fn name(&self) -> &'static str {
        "cypher_reverse"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        // Same type in, same type out.
        Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{
            Array, LargeStringArray, ListArray, StringArray, UInt32Array,
        };
        use datafusion::common::cast::as_list_array;
        use datafusion::error::DataFusionError;
        use std::sync::Arc;

        let array = args.args[0].to_array(args.number_rows)?;
        match array.data_type() {
            DataType::Utf8 => {
                let s = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("cypher_reverse: not a string array".into())
                    })?;
                let out: StringArray = (0..s.len())
                    .map(|i| (!s.is_null(i)).then(|| s.value(i).chars().rev().collect::<String>()))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(out)))
            }
            DataType::LargeUtf8 => {
                let s = array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("cypher_reverse: not a large string array".into())
                    })?;
                let out: LargeStringArray = (0..s.len())
                    .map(|i| (!s.is_null(i)).then(|| s.value(i).chars().rev().collect::<String>()))
                    .collect();
                Ok(ColumnarValue::Array(Arc::new(out)))
            }
            DataType::List(_) => {
                let list = as_list_array(&array)?;
                let values = list.values();
                let offsets = list.offsets();
                // Per row, emit element indices in reverse — the row's length is
                // unchanged, so the original offsets/nulls are reused verbatim.
                let mut idx: Vec<u32> = Vec::with_capacity(values.len());
                for w in offsets.windows(2) {
                    let (start, end) = (w[0], w[1]);
                    for j in (start..end).rev() {
                        idx.push(u32::try_from(j).unwrap_or(0));
                    }
                }
                let taken =
                    datafusion::arrow::compute::take(values, &UInt32Array::from(idx), None)?;
                let field = match list.data_type() {
                    DataType::List(f) => Arc::clone(f),
                    _ => unreachable!("matched List above"),
                };
                let reversed = ListArray::new(field, offsets.clone(), taken, list.nulls().cloned());
                Ok(ColumnarValue::Array(Arc::new(reversed)))
            }
            other => Err(DataFusionError::Plan(format!(
                "reverse() expects a string or list, got {other:?}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Cypher boolean / predicate UDFs
// ---------------------------------------------------------------------------

static CYPHER_AND: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherBoolOp::new(CypherBoolOpKind::And)));
static CYPHER_OR: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherBoolOp::new(CypherBoolOpKind::Or)));
static CYPHER_XOR: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherBoolOp::new(CypherBoolOpKind::Xor)));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CypherBoolOpKind {
    And,
    Or,
    Xor,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherBoolOp {
    signature: Signature,
    kind: CypherBoolOpKind,
}

impl CypherBoolOp {
    fn new(kind: CypherBoolOpKind) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            kind,
        }
    }
}

impl ScalarUDFImpl for CypherBoolOp {
    fn name(&self) -> &'static str {
        match self.kind {
            CypherBoolOpKind::And => "cypher_and",
            CypherBoolOpKind::Or => "cypher_or",
            CypherBoolOpKind::Xor => "cypher_xor",
        }
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
        let rows = args.number_rows;
        let lhs = args.args[0].to_array(rows)?;
        let rhs = args.args[1].to_array(rows)?;
        let out: BooleanArray = (0..rows)
            .map(|i| {
                let l = ScalarValue::try_from_array(&lhs, i)?;
                let r = ScalarValue::try_from_array(&rhs, i)?;
                let l = scalar_as_bool(&l)?;
                let r = scalar_as_bool(&r)?;
                Ok::<Option<bool>, datafusion::error::DataFusionError>(match self.kind {
                    CypherBoolOpKind::And => match (l, r) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    },
                    CypherBoolOpKind::Or => match (l, r) {
                        (Some(true), _) | (_, Some(true)) => Some(true),
                        (Some(false), Some(false)) => Some(false),
                        _ => None,
                    },
                    CypherBoolOpKind::Xor => match (l, r) {
                        (Some(left), Some(right)) => Some(left ^ right),
                        _ => None,
                    },
                })
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?
            .into_iter()
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

fn scalar_as_bool(s: &ScalarValue) -> datafusion::error::Result<Option<bool>> {
    let s = unwrap_het(s.clone());
    if s.is_null() {
        return Ok(None);
    }
    match s {
        ScalarValue::Boolean(v) => Ok(v),
        other => Err(datafusion::error::DataFusionError::Plan(format!(
            "expected boolean operand, got {other:?}"
        ))),
    }
}

static CYPHER_CMP_PRED: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherCmpPred::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherCmpPred {
    signature: Signature,
}

impl CypherCmpPred {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherCmpPred {
    fn name(&self) -> &'static str {
        "cypher_cmp_pred"
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
        let rows = args.number_rows;
        let lhs = args.args[0].to_array(rows)?;
        let rhs = args.args[1].to_array(rows)?;
        let op = args.args[2].to_array(rows)?;
        let out: BooleanArray = (0..rows)
            .map(|i| {
                let l = ScalarValue::try_from_array(&lhs, i)?;
                let r = ScalarValue::try_from_array(&rhs, i)?;
                let op = ScalarValue::try_from_array(&op, i)?;
                let op = scalar_as_i8(&op)?;
                Ok::<Option<bool>, datafusion::error::DataFusionError>(cypher_compare_pred(
                    &l, &r, op,
                ))
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?
            .into_iter()
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

fn scalar_as_i8(s: &ScalarValue) -> datafusion::error::Result<i8> {
    match s {
        ScalarValue::Int8(Some(v)) => Ok(*v),
        ScalarValue::Int64(Some(v)) => i8::try_from(*v).map_err(|_| {
            datafusion::error::DataFusionError::Plan(format!(
                "comparison opcode {v} is outside i8 range"
            ))
        }),
        other => Err(datafusion::error::DataFusionError::Plan(format!(
            "comparison opcode must be an integer, got {other:?}"
        ))),
    }
}

fn cypher_compare_pred(l: &ScalarValue, r: &ScalarValue, op: i8) -> Option<bool> {
    let l = unwrap_het(l.clone());
    let r = unwrap_het(r.clone());
    if l.is_null() || r.is_null() {
        return None;
    }
    if is_numeric_scalar(&l) && is_numeric_scalar(&r) {
        let lf = scalar_as_f64(&l)?;
        let rf = scalar_as_f64(&r)?;
        if lf.is_nan() || rf.is_nan() {
            return Some(false);
        }
    }
    let cmp = cypher_compare(&l, &r)?;
    Some(match op {
        0 => cmp < 0,
        1 => cmp <= 0,
        2 => cmp > 0,
        3 => cmp >= 0,
        _ => false,
    })
}

fn is_numeric_scalar(s: &ScalarValue) -> bool {
    scalar_as_f64(s).is_some()
}

static CYPHER_STARTS_WITH: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherStringPredicate::new(StringPredicate::Starts)));
static CYPHER_ENDS_WITH: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherStringPredicate::new(StringPredicate::Ends)));
static CYPHER_CONTAINS: LazyLock<ScalarUDF> = LazyLock::new(|| {
    ScalarUDF::new_from_impl(CypherStringPredicate::new(StringPredicate::Contains))
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StringPredicate {
    Starts,
    Ends,
    Contains,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherStringPredicate {
    signature: Signature,
    kind: StringPredicate,
}

impl CypherStringPredicate {
    fn new(kind: StringPredicate) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            kind,
        }
    }
}

impl ScalarUDFImpl for CypherStringPredicate {
    fn name(&self) -> &'static str {
        match self.kind {
            StringPredicate::Starts => "cypher_starts_with",
            StringPredicate::Ends => "cypher_ends_with",
            StringPredicate::Contains => "cypher_contains",
        }
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
        let rows = args.number_rows;
        let lhs = args.args[0].to_array(rows)?;
        let rhs = args.args[1].to_array(rows)?;
        let out: BooleanArray = (0..rows)
            .map(|i| {
                let l = ScalarValue::try_from_array(&lhs, i).ok()?;
                let r = ScalarValue::try_from_array(&rhs, i).ok()?;
                let l = scalar_as_string(&l)?;
                let r = scalar_as_string(&r)?;
                Some(match self.kind {
                    StringPredicate::Starts => l.starts_with(&r),
                    StringPredicate::Ends => l.ends_with(&r),
                    StringPredicate::Contains => l.contains(&r),
                })
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

fn scalar_as_string(s: &ScalarValue) -> Option<String> {
    let s = unwrap_het(s.clone());
    if s.is_null() {
        return None;
    }
    match s {
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) => v,
        _ => None,
    }
}

static CYPHER_RANGE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherRange::new()));
pub(crate) static CYPHER_ORDER_KEY: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherOrderKey::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherRange {
    signature: Signature,
}

impl CypherRange {
    fn new() -> Self {
        Self {
            // Keep literal invalid-argument cases in the runtime phase; the TCK
            // asserts `range()` argument errors at runtime, and constant-folding
            // would otherwise wrap them as DataFusion planning failures.
            signature: Signature::any(3, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for CypherRange {
    fn name(&self) -> &'static str {
        "cypher_range"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::new_list(DataType::Int64, true))
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Int64Builder, ListBuilder};
        let rows = args.number_rows;
        let starts = args.args[0].to_array(rows)?;
        let ends = args.args[1].to_array(rows)?;
        let steps = args.args[2].to_array(rows)?;
        let mut out = ListBuilder::new(Int64Builder::new());
        for i in 0..rows {
            let start = ScalarValue::try_from_array(&starts, i)?;
            let end = ScalarValue::try_from_array(&ends, i)?;
            let step = ScalarValue::try_from_array(&steps, i)?;
            if start.is_null() || end.is_null() || step.is_null() {
                out.append_null();
                continue;
            }
            let start = scalar_as_i64_arg(&start, "range start")?;
            let end = scalar_as_i64_arg(&end, "range end")?;
            let step = scalar_as_i64_arg(&step, "range step")?;
            if step == 0 {
                return Err(datafusion::error::DataFusionError::Plan(
                    "range step must not be zero".into(),
                ));
            }
            if (step > 0 && start > end) || (step < 0 && start < end) {
                out.append(true);
                continue;
            }
            let mut cur = start;
            loop {
                out.values().append_value(cur);
                if cur == end {
                    break;
                }
                let Some(next) = cur.checked_add(step) else {
                    return Err(datafusion::error::DataFusionError::Plan(
                        "range overflowed i64".into(),
                    ));
                };
                if (step > 0 && next > end) || (step < 0 && next < end) {
                    break;
                }
                cur = next;
            }
            out.append(true);
        }
        Ok(ColumnarValue::Array(std::sync::Arc::new(out.finish())))
    }
}

fn scalar_as_i64_arg(s: &ScalarValue, name: &str) -> datafusion::error::Result<i64> {
    match s {
        ScalarValue::Int8(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int32(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::UInt8(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt16(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt32(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).map_err(|_| {
            datafusion::error::DataFusionError::Plan(format!("{name} exceeds i64::MAX"))
        }),
        other => Err(datafusion::error::DataFusionError::Plan(format!(
            "{name} must be an integer, got {other:?}"
        ))),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherOrderKey {
    signature: Signature,
}

impl CypherOrderKey {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherOrderKey {
    fn name(&self) -> &'static str {
        "cypher_order_key"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::StringArray;
        let rows = args.number_rows;
        let values = args.args[0].to_array(rows)?;
        let out: StringArray = (0..rows)
            .map(|i| {
                let v = ScalarValue::try_from_array(&values, i).ok()?;
                Some(cypher_order_key(&v))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

pub(crate) fn needs_cypher_order_key_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
    ) || (matches!(t, DataType::Struct(_))
        && !is_date_struct(t)
        && !is_localdatetime_struct(t)
        && !is_duration_struct(t))
}

fn cypher_order_key(v: &ScalarValue) -> String {
    let v = unwrap_het(v.clone());
    if v.is_null() {
        return "99:null".to_string();
    }
    match &v {
        ScalarValue::Struct(s) if is_time_struct(&v.data_type()) => {
            let Some((nanos, offset)) = time_struct_parts(s, 0) else {
                return "99:null".to_string();
            };
            let instant = i128::from(nanos) - i128::from(offset) * 1_000_000_000;
            format!("55:time:{}", ordered_i128_key(instant))
        }
        ScalarValue::Struct(s) if is_datetime_struct(&v.data_type()) => {
            let Some((days, nanos, offset, _)) = datetime_struct_parts(s, 0) else {
                return "99:null".to_string();
            };
            let instant = i128::from(days) * 86_400_000_000_000 + i128::from(nanos)
                - i128::from(offset) * 1_000_000_000;
            format!("55:datetime:{}", ordered_i128_key(instant))
        }
        ScalarValue::Struct(s) if is_path_struct(s) => "50:path".to_string(),
        ScalarValue::Struct(s) if is_rel_struct(s) => "30:rel".to_string(),
        ScalarValue::Struct(s) if is_node_struct(s) => "20:node".to_string(),
        ScalarValue::Struct(_) => "10:map".to_string(),
        ScalarValue::List(a) => format!("40:list:{}", cypher_list_order_key(&a.value(0))),
        ScalarValue::LargeList(a) => format!("40:list:{}", cypher_list_order_key(&a.value(0))),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => format!("60:str:{s}"),
        ScalarValue::Boolean(Some(b)) => format!("70:bool:{}", u8::from(*b)),
        ScalarValue::Int64(Some(n)) => {
            #[allow(
                clippy::cast_precision_loss,
                reason = "Cypher numeric order shares a number bucket across ints and floats"
            )]
            let n = *n as f64;
            format!("80:num:{}", ordered_f64_key(n))
        }
        ScalarValue::Float64(Some(f)) if f.is_nan() => "90:nan".to_string(),
        ScalarValue::Float64(Some(f)) => format!("80:num:{}", ordered_f64_key(*f)),
        _ => "98:other".to_string(),
    }
}

fn cypher_list_order_key(values: &datafusion::arrow::array::ArrayRef) -> String {
    let mut out = String::new();
    for i in 0..values.len() {
        let v = ScalarValue::try_from_array(values, i).unwrap_or(ScalarValue::Null);
        out.push_str(&cypher_order_key(&v));
        out.push('|');
    }
    out.push_str("00:end");
    out
}

fn ordered_f64_key(f: f64) -> String {
    let bits = f.to_bits();
    let key = if (bits >> 63) == 0 {
        bits | (1 << 63)
    } else {
        !bits
    };
    format!("{key:016x}")
}

fn ordered_i128_key(value: i128) -> String {
    let key = value.cast_unsigned() ^ (1_u128 << 127);
    format!("{key:032x}")
}

fn is_node_struct(s: &datafusion::arrow::array::StructArray) -> bool {
    s.column_by_name("node_uuid").is_some() || s.column_by_name("labels").is_some()
}

fn is_rel_struct(s: &datafusion::arrow::array::StructArray) -> bool {
    s.column_by_name("edge_uuid").is_some()
        || s.column_by_name("src_uuid").is_some()
        || s.column_by_name("dst_uuid").is_some()
}

fn is_path_struct(s: &datafusion::arrow::array::StructArray) -> bool {
    s.column_by_name("nodes").is_some() || s.column_by_name("relationships").is_some()
}

// ---------------------------------------------------------------------------
// cypher_eq UDF
// ---------------------------------------------------------------------------

/// Cypher equality (`=`; `<>` is its negation). Unlike SQL `=`, comparing two
/// values of **different types** is `false` rather than a planning error, and
/// `null = x` is `null` (three-valued). Same-type and mixed-numeric operands use
/// Arrow's null-propagating `eq` kernel; everything else (string-vs-number,
/// temporal-vs-other, …) is `false` where both operands are non-null. (ADR 0009)
static CYPHER_EQ: LazyLock<ScalarUDF> = LazyLock::new(|| ScalarUDF::new_from_impl(CypherEq::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherEq {
    signature: Signature,
}

impl CypherEq {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherEq {
    fn name(&self) -> &'static str {
        "cypher_eq"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Boolean)
    }

    /// Rewrite to the native `=` so DataFusion keeps its optimizations (filter
    /// pushdown, join-key recognition) whenever the operands are statically the
    /// **same** primitive type, or a type can't be resolved (e.g. a `$param`,
    /// which native `=` coerces and pushes down). The type-tolerant UDF is kept
    /// for everything else: **nested** operands (three-valued structural
    /// equality) and **differing** types — including differing numeric widths
    /// (`UInt64` vs `Int64`), where native `=` not only risks a planning error
    /// but trips DataFusion's interval analysis (`lhs_type == rhs_type`); the
    /// UDF compares those via `f64` at runtime instead.
    fn simplify(
        &self,
        args: Vec<DfExpr>,
        info: &datafusion::logical_expr::simplify::SimplifyContext,
    ) -> datafusion::error::Result<datafusion::logical_expr::simplify::ExprSimplifyResult> {
        use datafusion::logical_expr::simplify::ExprSimplifyResult;
        let [l, r] = args.as_slice() else {
            return Ok(ExprSimplifyResult::Original(args));
        };
        let nested = |t: &DataType| {
            matches!(
                t,
                DataType::List(_) | DataType::LargeList(_) | DataType::Struct(_)
            )
        };
        let floaty =
            |t: &DataType| matches!(t, DataType::Float16 | DataType::Float32 | DataType::Float64);
        // A `$param` placeholder has no statically-fixed type here; native `=`
        // coerces it to the other operand at bind time and pushes down (the
        // common `WHERE prop = $x`), so don't trap it in the UDF.
        let placeholder = |e: &DfExpr| matches!(e, DfExpr::Placeholder(_));
        let keep_udf = if placeholder(l) || placeholder(r) {
            false
        } else if let (Ok(lt), Ok(rt)) = (info.get_data_type(l), info.get_data_type(r)) {
            nested(&lt) || nested(&rt) || floaty(&lt) || floaty(&rt) || lt != rt
        } else {
            false // unresolved operand type(s) ⇒ native `=` handles it (and pushes down)
        };
        if keep_udf {
            return Ok(ExprSimplifyResult::Original(args));
        }
        let [l, r]: [DfExpr; 2] = args.try_into().expect("checked length 2 above");
        Ok(ExprSimplifyResult::Simplified(l.eq(r)))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::BooleanArray;
        use datafusion::arrow::compute::kernels::cmp::eq;

        let rows = args.number_rows;
        let lhs = args.args[0].to_array(rows)?;
        let rhs = args.args[1].to_array(rows)?;
        let (lt, rt) = (lhs.data_type(), rhs.data_type());

        let nested = |t: &DataType| {
            matches!(
                t,
                DataType::List(_) | DataType::LargeList(_) | DataType::Struct(_)
            )
        };

        let floaty =
            |t: &DataType| matches!(t, DataType::Float16 | DataType::Float32 | DataType::Float64);

        // Fast path: same non-float primitive type → Arrow's null-propagating
        // equality kernel (vectorised, and identical to the prior native `=`
        // behaviour). Floats stay on the Cypher path so NaN never equals itself.
        if lt == rt && !nested(lt) && !floaty(lt) {
            let res = eq(&lhs, &rhs)?;
            return Ok(ColumnarValue::Array(std::sync::Arc::new(res)));
        }

        // Float, nested, numeric-coercion and cross-type comparisons need Cypher's three-valued
        // structural equality, which the scalar `=` kernel doesn't provide:
        // compare value-by-value via `ScalarValue`.
        let out: BooleanArray = (0..rows)
            .map(|i| {
                let l = ScalarValue::try_from_array(&lhs, i).ok()?;
                let r = ScalarValue::try_from_array(&rhs, i).ok()?;
                cypher_value_eq(&l, &r)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

/// `x IN list` with Cypher three-valued structural membership (ADR 0011): used
/// when the list is a heterogeneous/nested tagged list, which DataFusion's native
/// `in_list` cannot compare. Decodes each element and reuses [`cypher_value_eq`];
/// a definitive match wins, else any `null` comparison yields `null`, else `false`
/// (an empty list is `false`, even for a `null` left operand).
static CYPHER_IN: LazyLock<ScalarUDF> = LazyLock::new(|| ScalarUDF::new_from_impl(CypherIn::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherIn {
    signature: Signature,
}

impl CypherIn {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

enum ListView<'a> {
    Fixed(&'a FixedSizeListArray),
    List(&'a ListArray),
    Large(&'a LargeListArray),
}

impl ListView<'_> {
    fn from_array(array: &datafusion::arrow::array::ArrayRef) -> Option<ListView<'_>> {
        if let Some(list) = array.as_any().downcast_ref::<FixedSizeListArray>() {
            Some(ListView::Fixed(list))
        } else if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
            Some(ListView::List(list))
        } else {
            array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .map(ListView::Large)
        }
    }

    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Fixed(a) => a.is_null(row),
            Self::List(a) => a.is_null(row),
            Self::Large(a) => a.is_null(row),
        }
    }

    fn value(&self, row: usize) -> datafusion::arrow::array::ArrayRef {
        match self {
            Self::Fixed(a) => a.value(row),
            Self::List(a) => a.value(row),
            Self::Large(a) => a.value(row),
        }
    }
}

fn cypher_in_elems(lhs: &ScalarValue, elems: &datafusion::arrow::array::ArrayRef) -> Option<bool> {
    let mut saw_null = false;
    for j in 0..elems.len() {
        let ev = ScalarValue::try_from_array(elems, j).ok()?;
        match cypher_value_eq(lhs, &ev) {
            Some(true) => return Some(true),
            None => saw_null = true,
            Some(false) => {}
        }
    }
    if saw_null { None } else { Some(false) }
}

fn cypher_in_tagged_list(
    lhs: &ScalarValue,
    rhs: &datafusion::arrow::array::ArrayRef,
    row: usize,
) -> Option<bool> {
    let rv = ScalarValue::try_from_array(rhs, row).ok()?;
    match unwrap_het(rv) {
        ScalarValue::List(list) => {
            if list.is_null(0) {
                None
            } else {
                cypher_in_elems(lhs, &list.value(0))
            }
        }
        ScalarValue::LargeList(list) => {
            if list.is_null(0) {
                None
            } else {
                cypher_in_elems(lhs, &list.value(0))
            }
        }
        _ => None,
    }
}

impl ScalarUDFImpl for CypherIn {
    fn name(&self) -> &'static str {
        "cypher_in"
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
        let rows = args.number_rows;
        let lhs = args.args[0].to_array(rows)?;
        let rhs = args.args[1].to_array(rows)?;
        if is_het_struct_type(Some(rhs.data_type())) {
            let out: BooleanArray = (0..rows)
                .map(|i| {
                    let lv = ScalarValue::try_from_array(&lhs, i).ok()?;
                    cypher_in_tagged_list(&lv, &rhs, i)
                })
                .collect();
            return Ok(ColumnarValue::Array(std::sync::Arc::new(out)));
        }
        let list = if let Some(list) = rhs.as_any().downcast_ref::<FixedSizeListArray>() {
            ListView::Fixed(list)
        } else if let Some(list) = rhs.as_any().downcast_ref::<ListArray>() {
            ListView::List(list)
        } else if let Some(list) = rhs.as_any().downcast_ref::<LargeListArray>() {
            ListView::Large(list)
        } else {
            return Ok(ColumnarValue::Array(std::sync::Arc::new(
                BooleanArray::new_null(rows),
            )));
        };
        let out: BooleanArray = (0..rows)
            .map(|i| {
                if list.is_null(i) {
                    return None; // `x IN null` → null
                }
                let lv = ScalarValue::try_from_array(&lhs, i).ok()?;
                let elems = list.value(i);
                cypher_in_elems(&lv, &elems)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

/// Cypher order COMPARABILITY of `<`/`<=`/`>`/`>=`: `Some(-1|0|1)` when the two
/// values are comparable, `None` (= `null`) when either is null or the types are
/// not order-comparable (e.g. a list vs a boolean). Numbers compare across
/// `Int`/`Float`; same-typed strings/booleans compare directly; two lists compare
/// lexicographically (a shorter prefix sorts first), with an incomparable element
/// making the whole comparison `null`. Unlike *orderability* (used by min/max),
/// comparability does NOT impose a cross-type order.
fn cypher_compare(a: &ScalarValue, b: &ScalarValue) -> Option<i8> {
    let to_i8 = |o: std::cmp::Ordering| o as i8;
    let a = unwrap_het(a.clone());
    let b = unwrap_het(b.clone());
    if a.is_null() || b.is_null() {
        return None;
    }
    match (&a, &b) {
        _ if is_numeric_scalar(&a) && is_numeric_scalar(&b) => {
            if let (Some(x), Some(y)) = (scalar_as_i128(&a), scalar_as_i128(&b)) {
                Some(to_i8(x.cmp(&y)))
            } else {
                scalar_as_f64(&a)?
                    .partial_cmp(&scalar_as_f64(&b)?)
                    .map(to_i8)
            }
        }
        (ScalarValue::Utf8(Some(x)), ScalarValue::Utf8(Some(y))) => Some(to_i8(x.cmp(y))),
        (ScalarValue::Boolean(Some(x)), ScalarValue::Boolean(Some(y))) => Some(to_i8(x.cmp(y))),
        (ScalarValue::Time64Nanosecond(Some(x)), ScalarValue::Time64Nanosecond(Some(y))) => {
            Some(to_i8(x.cmp(y)))
        }
        (ScalarValue::List(x), ScalarValue::List(y)) => {
            cypher_seq_compare(&x.value(0), &y.value(0))
        }
        (ScalarValue::Struct(x), ScalarValue::Struct(y))
            if is_date_struct(&a.data_type()) && is_date_struct(&b.data_type()) =>
        {
            Some(to_i8(
                date_struct_value(x, 0)?.cmp(&date_struct_value(y, 0)?),
            ))
        }
        (ScalarValue::Struct(x), ScalarValue::Struct(y))
            if is_localdatetime_struct(&a.data_type())
                && is_localdatetime_struct(&b.data_type()) =>
        {
            Some(to_i8(
                localdatetime_struct_parts(x, 0)?.cmp(&localdatetime_struct_parts(y, 0)?),
            ))
        }
        // `time` orders by its UTC instant (`time - offset`), not the struct's
        // native lexicographic `(time, offset)`. (#1008, Temporal7 [3])
        (ScalarValue::Struct(x), ScalarValue::Struct(y))
            if is_time_struct(&a.data_type()) && is_time_struct(&b.data_type()) =>
        {
            let (xn, xo) = time_struct_parts(x, 0)?;
            let (yn, yo) = time_struct_parts(y, 0)?;
            let xi = i128::from(xn) - i128::from(xo) * 1_000_000_000;
            let yi = i128::from(yn) - i128::from(yo) * 1_000_000_000;
            Some(to_i8(xi.cmp(&yi)))
        }
        (ScalarValue::Struct(x), ScalarValue::Struct(y))
            if is_datetime_struct(&a.data_type()) && is_datetime_struct(&b.data_type()) =>
        {
            let (xd, xn, xo, _) = datetime_struct_parts(x, 0)?;
            let (yd, yn, yo, _) = datetime_struct_parts(y, 0)?;
            let xi = i128::from(xd) * 86_400_000_000_000 + i128::from(xn)
                - i128::from(xo) * 1_000_000_000;
            let yi = i128::from(yd) * 86_400_000_000_000 + i128::from(yn)
                - i128::from(yo) * 1_000_000_000;
            Some(to_i8(xi.cmp(&yi)))
        }
        _ => None, // incomparable types
    }
}

/// Lexicographic comparability of two list element arrays (see [`cypher_compare`]).
fn cypher_seq_compare(
    a: &datafusion::arrow::array::ArrayRef,
    b: &datafusion::arrow::array::ArrayRef,
) -> Option<i8> {
    let common = a.len().min(b.len());
    for i in 0..common {
        let av = ScalarValue::try_from_array(a, i).ok()?;
        let bv = ScalarValue::try_from_array(b, i).ok()?;
        match cypher_compare(&av, &bv)? {
            0 => {}
            c => return Some(c),
        }
    }
    Some(a.len().cmp(&b.len()) as i8) // a shared prefix → the shorter list sorts first
}

/// Whether a lowered expression is a constant `List` literal.
fn is_list_literal(e: &DfExpr) -> bool {
    matches!(e, DfExpr::Literal(ScalarValue::List(_), _))
}

/// Whether a DataType is the ADR-0011 heterogeneous tagged-struct element type
/// (so `min`/`max` over it must use Cypher orderability, not native min/max).
pub(crate) fn is_het_struct_type(t: Option<&DataType>) -> bool {
    matches!(t, Some(DataType::Struct(fields)) if fields.iter().any(|f| f.name() == "__het_tag"))
}

/// Cypher ORDERABILITY total order, used by `min`/`max` (which exclude nulls).
/// Ascending type rank `list < string < boolean < number < map`, then by value
/// within a type (numbers by `f64`, strings/booleans naturally, lists
/// lexicographically, maps by sorted `(key, value)` entries — ADR 0011 slice 5).
/// Distinct from [`cypher_compare`]: orderability is a TOTAL order across types,
/// whereas comparability (`<`) is three-valued with no cross-type order.
fn cypher_order(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let a = unwrap_het(a.clone());
    let b = unwrap_het(b.clone());
    let rank = |v: &ScalarValue| -> u8 {
        match v {
            ScalarValue::List(_) | ScalarValue::LargeList(_) => 1,
            ScalarValue::Utf8(_) | ScalarValue::LargeUtf8(_) => 2,
            ScalarValue::Boolean(_) => 3,
            _ if is_numeric_scalar(v) => 4,
            ScalarValue::Struct(_) => 5,
            _ => 0,
        }
    };
    let (ra, rb) = (rank(&a), rank(&b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (&a, &b) {
        (ScalarValue::List(x), ScalarValue::List(y)) => cypher_seq_order(&x.value(0), &y.value(0)),
        (ScalarValue::Utf8(Some(x)), ScalarValue::Utf8(Some(y))) => x.cmp(y),
        (ScalarValue::Boolean(Some(x)), ScalarValue::Boolean(Some(y))) => x.cmp(y),
        (ScalarValue::Struct(x), ScalarValue::Struct(y)) => cypher_map_order(x, y),
        _ => match (scalar_as_f64(&a), scalar_as_f64(&b)) {
            (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
            _ => Ordering::Equal,
        },
    }
}

/// Orderability of two maps (ADR 0011 slice 5): compare their entries sorted by
/// key — key-by-key (lexicographic), then value-by-value ([`cypher_order`]); a
/// map with fewer keys sorts first when it is a prefix of the other.
fn cypher_map_order(
    a: &datafusion::arrow::array::StructArray,
    b: &datafusion::arrow::array::StructArray,
) -> std::cmp::Ordering {
    let sorted = |s: &datafusion::arrow::array::StructArray| -> Vec<(String, ScalarValue)> {
        let mut e: Vec<(String, ScalarValue)> = s
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                (
                    f.name().clone(),
                    ScalarValue::try_from_array(s.column(i), 0).unwrap_or(ScalarValue::Null),
                )
            })
            .collect();
        e.sort_by(|x, y| x.0.cmp(&y.0));
        e
    };
    let (ea, eb) = (sorted(a), sorted(b));
    for ((ka, va), (kb, vb)) in ea.iter().zip(eb.iter()) {
        match ka.cmp(kb) {
            std::cmp::Ordering::Equal => {}
            c => return c,
        }
        match cypher_order(va, vb) {
            std::cmp::Ordering::Equal => {}
            c => return c,
        }
    }
    ea.len().cmp(&eb.len())
}

/// Lexicographic orderability of two list element arrays (shorter prefix sorts
/// first), decoding tagged elements.
fn cypher_seq_order(
    a: &datafusion::arrow::array::ArrayRef,
    b: &datafusion::arrow::array::ArrayRef,
) -> std::cmp::Ordering {
    let common = a.len().min(b.len());
    for i in 0..common {
        let av = ScalarValue::try_from_array(a, i).unwrap_or(ScalarValue::Null);
        let bv = ScalarValue::try_from_array(b, i).unwrap_or(ScalarValue::Null);
        match cypher_order(&av, &bv) {
            std::cmp::Ordering::Equal => {}
            c => return c,
        }
    }
    a.len().cmp(&b.len())
}

/// `min`/`max` over a heterogeneous (tagged) column use Cypher orderability
/// ([`cypher_order`]) — native struct min/max would order by `__het_key` (null for
/// non-numeric elements). Returns the original tagged element so it renders right.
pub(crate) static CYPHER_MAX: LazyLock<datafusion::logical_expr::AggregateUDF> =
    LazyLock::new(|| {
        datafusion::logical_expr::AggregateUDF::new_from_impl(CypherExtreme::new(true))
    });
pub(crate) static CYPHER_MIN: LazyLock<datafusion::logical_expr::AggregateUDF> =
    LazyLock::new(|| {
        datafusion::logical_expr::AggregateUDF::new_from_impl(CypherExtreme::new(false))
    });
pub(crate) static CYPHER_COLLECT: LazyLock<datafusion::logical_expr::AggregateUDF> =
    LazyLock::new(|| {
        datafusion::logical_expr::AggregateUDF::new_from_impl(CypherCollect::new(false))
    });
pub(crate) static CYPHER_COLLECT_DISTINCT: LazyLock<datafusion::logical_expr::AggregateUDF> =
    LazyLock::new(|| {
        datafusion::logical_expr::AggregateUDF::new_from_impl(CypherCollect::new(true))
    });
pub(crate) static CYPHER_PERCENTILE_DISC: LazyLock<datafusion::logical_expr::AggregateUDF> =
    LazyLock::new(|| {
        datafusion::logical_expr::AggregateUDF::new_from_impl(CypherPercentile::new(false))
    });
pub(crate) static CYPHER_PERCENTILE_CONT: LazyLock<datafusion::logical_expr::AggregateUDF> =
    LazyLock::new(|| {
        datafusion::logical_expr::AggregateUDF::new_from_impl(CypherPercentile::new(true))
    });

#[derive(Debug)]
struct CypherExtreme {
    signature: Signature,
    is_max: bool,
}
impl CypherExtreme {
    fn new(is_max: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            is_max,
        }
    }
}
impl PartialEq for CypherExtreme {
    fn eq(&self, o: &Self) -> bool {
        self.is_max == o.is_max
    }
}
impl Eq for CypherExtreme {}
impl std::hash::Hash for CypherExtreme {
    fn hash<H: std::hash::Hasher>(&self, st: &mut H) {
        self.is_max.hash(st);
    }
}
impl datafusion::logical_expr::AggregateUDFImpl for CypherExtreme {
    fn name(&self) -> &str {
        if self.is_max {
            "cypher_max"
        } else {
            "cypher_min"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(arg_types[0].clone())
    }
    fn accumulator(
        &self,
        args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> datafusion::error::Result<Box<dyn datafusion::logical_expr::Accumulator>> {
        Ok(Box::new(ExtremeAcc {
            is_max: self.is_max,
            dtype: args.return_field.data_type().clone(),
            best: None,
        }))
    }
    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> datafusion::error::Result<Vec<datafusion::arrow::datatypes::FieldRef>> {
        use datafusion::arrow::datatypes::Field;
        Ok(vec![std::sync::Arc::new(Field::new(
            "best",
            args.return_field.data_type().clone(),
            true,
        ))])
    }
}

#[derive(Debug)]
struct ExtremeAcc {
    is_max: bool,
    dtype: DataType,
    best: Option<ScalarValue>,
}
impl datafusion::logical_expr::Accumulator for ExtremeAcc {
    fn update_batch(
        &mut self,
        values: &[datafusion::arrow::array::ArrayRef],
    ) -> datafusion::error::Result<()> {
        use datafusion::arrow::array::Array;
        let arr = &values[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let v = ScalarValue::try_from_array(arr, i)?;
            if v.is_null() {
                continue;
            }
            let take = match &self.best {
                None => true,
                Some(b) => {
                    let ord = cypher_order(&v, b);
                    (self.is_max && ord == std::cmp::Ordering::Greater)
                        || (!self.is_max && ord == std::cmp::Ordering::Less)
                }
            };
            if take {
                self.best = Some(v);
            }
        }
        Ok(())
    }
    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        match &self.best {
            Some(b) => Ok(b.clone()),
            None => ScalarValue::try_from(&self.dtype),
        }
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.best.as_ref().map_or(0, ScalarValue::size)
    }
    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }
    fn merge_batch(
        &mut self,
        states: &[datafusion::arrow::array::ArrayRef],
    ) -> datafusion::error::Result<()> {
        self.update_batch(states)
    }
}

#[derive(Debug)]
struct CypherCollect {
    signature: Signature,
    distinct: bool,
}

impl CypherCollect {
    fn new(distinct: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            distinct,
        }
    }
}

impl PartialEq for CypherCollect {
    fn eq(&self, o: &Self) -> bool {
        self.distinct == o.distinct
    }
}

impl Eq for CypherCollect {}

impl std::hash::Hash for CypherCollect {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.distinct.hash(state);
    }
}

impl datafusion::logical_expr::AggregateUDFImpl for CypherCollect {
    fn name(&self) -> &str {
        if self.distinct {
            "cypher_collect_distinct"
        } else {
            "cypher_collect"
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::new_list(arg_types[0].clone(), true))
    }
    fn accumulator(
        &self,
        args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> datafusion::error::Result<Box<dyn datafusion::logical_expr::Accumulator>> {
        let DataType::List(field) = args.return_field.data_type() else {
            return Err(datafusion::error::DataFusionError::Plan(
                "cypher_collect return type must be a list".into(),
            ));
        };
        Ok(Box::new(CollectAcc {
            distinct: self.distinct,
            elem_type: field.data_type().clone(),
            values: Vec::new(),
        }))
    }
    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> datafusion::error::Result<Vec<datafusion::arrow::datatypes::FieldRef>> {
        use datafusion::arrow::datatypes::Field;
        Ok(vec![std::sync::Arc::new(Field::new(
            "values",
            args.return_field.data_type().clone(),
            true,
        ))])
    }
}

#[derive(Debug)]
struct CollectAcc {
    distinct: bool,
    elem_type: DataType,
    values: Vec<ScalarValue>,
}

impl CollectAcc {
    fn push_value(&mut self, v: ScalarValue) {
        if v.is_null() {
            return;
        }
        if self.distinct
            && self
                .values
                .iter()
                .any(|seen| cypher_value_eq(seen, &v) == Some(true))
        {
            return;
        }
        self.values.push(v);
    }

    fn as_list(&self) -> ScalarValue {
        ScalarValue::List(ScalarValue::new_list(&self.values, &self.elem_type, true))
    }
}

impl datafusion::logical_expr::Accumulator for CollectAcc {
    fn update_batch(
        &mut self,
        values: &[datafusion::arrow::array::ArrayRef],
    ) -> datafusion::error::Result<()> {
        use datafusion::arrow::array::Array;
        let arr = &values[0];
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            self.push_value(ScalarValue::try_from_array(arr, i)?);
        }
        Ok(())
    }
    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        Ok(self.as_list())
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.iter().map(ScalarValue::size).sum::<usize>()
    }
    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        Ok(vec![self.as_list()])
    }
    fn merge_batch(
        &mut self,
        states: &[datafusion::arrow::array::ArrayRef],
    ) -> datafusion::error::Result<()> {
        use datafusion::arrow::array::{Array, ListArray};
        let arr = &states[0];
        let Some(list) = arr.as_any().downcast_ref::<ListArray>() else {
            return Err(datafusion::error::DataFusionError::Plan(
                "cypher_collect state must be a list".into(),
            ));
        };
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            let values = list.value(row);
            for i in 0..values.len() {
                if values.is_null(i) {
                    continue;
                }
                self.push_value(ScalarValue::try_from_array(&values, i)?);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct CypherPercentile {
    signature: Signature,
    continuous: bool,
}

impl CypherPercentile {
    fn new(continuous: bool) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            continuous,
        }
    }
}

impl PartialEq for CypherPercentile {
    fn eq(&self, o: &Self) -> bool {
        self.continuous == o.continuous
    }
}

impl Eq for CypherPercentile {}

impl std::hash::Hash for CypherPercentile {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.continuous.hash(state);
    }
}

impl datafusion::logical_expr::AggregateUDFImpl for CypherPercentile {
    fn name(&self) -> &str {
        if self.continuous {
            "cypher_percentile_cont"
        } else {
            "cypher_percentile_disc"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        let Some(value_type) = arg_types.first() else {
            return Err(datafusion::error::DataFusionError::Plan(
                "percentile aggregate requires value and percentile arguments".into(),
            ));
        };
        if !is_percentile_numeric_type(value_type) {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "percentile value expression must be numeric, got {value_type}"
            )));
        }
        if self.continuous {
            Ok(DataType::Float64)
        } else {
            Ok(value_type.clone())
        }
    }

    fn accumulator(
        &self,
        args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> datafusion::error::Result<Box<dyn datafusion::logical_expr::Accumulator>> {
        let value_type = args.expr_fields.first().map_or_else(
            || args.return_field.data_type().clone(),
            |f| f.data_type().clone(),
        );
        Ok(Box::new(PercentileAcc {
            continuous: self.continuous,
            value_type,
            result_type: args.return_field.data_type().clone(),
            values: Vec::new(),
            percentile: None,
        }))
    }

    fn state_fields(
        &self,
        args: datafusion::logical_expr::function::StateFieldsArgs,
    ) -> datafusion::error::Result<Vec<datafusion::arrow::datatypes::FieldRef>> {
        use datafusion::arrow::datatypes::Field;
        let value_type = args.input_fields.first().map_or_else(
            || args.return_field.data_type().clone(),
            |f| f.data_type().clone(),
        );
        Ok(vec![
            std::sync::Arc::new(Field::new(
                "values",
                DataType::new_list(value_type, true),
                true,
            )),
            std::sync::Arc::new(Field::new("percentile", DataType::Float64, true)),
        ])
    }
}

#[derive(Debug)]
struct PercentileAcc {
    continuous: bool,
    value_type: DataType,
    result_type: DataType,
    values: Vec<ScalarValue>,
    percentile: Option<f64>,
}

impl PercentileAcc {
    fn push_value(&mut self, v: ScalarValue) -> datafusion::error::Result<()> {
        if v.is_null() {
            return Ok(());
        }
        if scalar_as_f64(&v).is_none() {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "percentile value expression must be numeric, got {}",
                v.data_type()
            )));
        }
        self.values.push(v);
        Ok(())
    }

    fn observe_percentile(&mut self, p: Option<f64>) -> datafusion::error::Result<()> {
        let Some(p) = p else {
            return Ok(());
        };
        if !p.is_finite() || !(0.0..=1.0).contains(&p) {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "percentile argument must be a finite number between 0.0 and 1.0 inclusive, got {p}"
            )));
        }
        match self.percentile {
            Some(existing) if (existing - p).abs() > f64::EPSILON => {
                Err(datafusion::error::DataFusionError::Execution(
                    "percentile argument must be constant within an aggregate group".into(),
                ))
            }
            Some(_) => Ok(()),
            None => {
                self.percentile = Some(p);
                Ok(())
            }
        }
    }

    fn null_result(&self) -> datafusion::error::Result<ScalarValue> {
        ScalarValue::try_from(&self.result_type)
    }

    fn percentile_scalar(
        values: &[datafusion::arrow::array::ArrayRef],
        row: usize,
    ) -> datafusion::error::Result<Option<f64>> {
        use datafusion::arrow::array::Array;
        let arr = &values[1];
        if arr.is_null(row) {
            return Ok(None);
        }
        let scalar = ScalarValue::try_from_array(arr, row)?;
        if scalar.is_null() {
            Ok(None)
        } else {
            scalar_as_f64(&scalar).map(Some).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "percentile argument must be numeric, got {}",
                    scalar.data_type()
                ))
            })
        }
    }
}

impl datafusion::logical_expr::Accumulator for PercentileAcc {
    fn update_batch(
        &mut self,
        values: &[datafusion::arrow::array::ArrayRef],
    ) -> datafusion::error::Result<()> {
        use datafusion::arrow::array::Array;
        let value_arr = &values[0];
        for row in 0..value_arr.len() {
            self.observe_percentile(Self::percentile_scalar(values, row)?)?;
            if value_arr.is_null(row) {
                continue;
            }
            self.push_value(ScalarValue::try_from_array(value_arr, row)?)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion::error::Result<ScalarValue> {
        let Some(percentile) = self.percentile else {
            return self.null_result();
        };
        if self.values.is_empty() {
            return self.null_result();
        }
        let mut values: Vec<(f64, ScalarValue)> = self
            .values
            .iter()
            .filter_map(|v| scalar_as_f64(v).map(|f| (f, v.clone())))
            .collect();
        if values.is_empty() {
            return self.null_result();
        }
        values.sort_by(|(l, _), (r, _)| l.total_cmp(r));

        if self.continuous {
            let len = values.len();
            if len == 1 {
                return Ok(ScalarValue::Float64(Some(values[0].0)));
            }
            let (lower_index, upper_index, fraction) = percentile_cont_indices(percentile, len);
            let result = if lower_index == upper_index {
                values[lower_index].0
            } else {
                let lower = values[lower_index].0;
                let upper = values[upper_index].0;
                lower + (upper - lower) * fraction
            };
            Ok(ScalarValue::Float64(Some(result)))
        } else {
            let index = percentile_disc_index(percentile, values.len());
            Ok(values[index].1.clone())
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.values.iter().map(ScalarValue::size).sum::<usize>()
    }

    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list(&self.values, &self.value_type, true)),
            ScalarValue::Float64(self.percentile),
        ])
    }

    fn merge_batch(
        &mut self,
        states: &[datafusion::arrow::array::ArrayRef],
    ) -> datafusion::error::Result<()> {
        use datafusion::arrow::array::{Array, Float64Array, ListArray};
        let values = &states[0];
        let Some(lists) = values.as_any().downcast_ref::<ListArray>() else {
            return Err(datafusion::error::DataFusionError::Plan(
                "percentile state values must be a list".into(),
            ));
        };
        let Some(percentiles) = states[1].as_any().downcast_ref::<Float64Array>() else {
            return Err(datafusion::error::DataFusionError::Plan(
                "percentile state percentile must be Float64".into(),
            ));
        };
        for row in 0..lists.len() {
            self.observe_percentile(if percentiles.is_null(row) {
                None
            } else {
                Some(percentiles.value(row))
            })?;
            if lists.is_null(row) {
                continue;
            }
            let values = lists.value(row);
            for i in 0..values.len() {
                if values.is_null(i) {
                    continue;
                }
                self.push_value(ScalarValue::try_from_array(&values, i)?)?;
            }
        }
        Ok(())
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "percentile ranks are defined by converting bounded [0, 1] floats into sorted indexes"
)]
fn percentile_cont_indices(percentile: f64, len: usize) -> (usize, usize, f64) {
    let index = percentile * ((len - 1) as f64);
    let lower = index.floor() as usize;
    let upper = index.ceil() as usize;
    (lower, upper, index.fract())
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "percentile ranks are defined by converting bounded [0, 1] floats into sorted indexes"
)]
fn percentile_disc_index(percentile: f64, len: usize) -> usize {
    if percentile <= f64::EPSILON {
        0
    } else {
        ((percentile * (len as f64)).ceil() as usize)
            .saturating_sub(1)
            .min(len - 1)
    }
}

fn is_percentile_numeric_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Null
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

/// Three-valued Cypher equality of two scalar values: `Some(true)`,
/// `Some(false)`, or `None` (= `null`). Numbers compare across `Int`/`Float`;
/// lists and maps compare structurally with the Cypher rules — a length or
/// key-set mismatch is `false`, a `null` element with no definitive inequality
/// is `null`; otherwise different types are `false`. (ADR 0009)
fn cypher_value_eq(l: &ScalarValue, r: &ScalarValue) -> Option<bool> {
    if l.is_null() || r.is_null() {
        return None;
    }
    // A heterogeneous-list element (ADR 0011 tagged struct) decodes to its plain
    // value, then compares by normal Cypher rules — so a tagged `int = 1` equals a
    // native `1`, and a native list equals a tagged list element-by-element.
    if let Some(dl) = decode_het(l) {
        return cypher_value_eq(&dl, r);
    }
    if let Some(dr) = decode_het(r) {
        return cypher_value_eq(l, &dr);
    }
    match (l, r) {
        (ScalarValue::List(a), ScalarValue::List(b)) => cypher_seq_eq(&a.value(0), &b.value(0)),
        (ScalarValue::LargeList(a), ScalarValue::LargeList(b)) => {
            cypher_seq_eq(&a.value(0), &b.value(0))
        }
        // Node/relationship/path structs compare by **identity** — structurally
        // equal (a null property counts as equal). Plain maps use three-valued
        // structural equality (a null value propagates: `{a: null} = {a: null}`
        // is `null`, not `true`).
        (ScalarValue::Struct(a), ScalarValue::Struct(b)) => {
            if is_entity_struct(a) || is_entity_struct(b) {
                Some(l == r)
            } else {
                cypher_struct_eq(a, b)
            }
        }
        _ if is_numeric_scalar(l) && is_numeric_scalar(r) => {
            if let (Some(li), Some(ri)) = (scalar_as_i128(l), scalar_as_i128(r)) {
                return Some(li == ri);
            }
            let (Some(lf), Some(rf)) = (scalar_as_f64(l), scalar_as_f64(r)) else {
                return None;
            };
            Some(!lf.is_nan() && !rf.is_nan() && lf == rf)
        }
        // Same-typed scalar ⇒ direct equality; different types ⇒ false.
        _ if std::mem::discriminant(l) == std::mem::discriminant(r) => Some(l == r),
        _ => Some(false),
    }
}

/// Decode a heterogeneous-list element (ADR 0011 tagged struct) to its plain
/// `ScalarValue`; `None` for any non-tagged value (so normal values pass through
/// `cypher_value_eq` unchanged). A null element → `Null`; a list element
/// (`__het_tag == 4`) → a `List` of tagged children; a map element
/// (`__het_tag == 5`) → a `Struct` map whose values stay tagged (decoded in turn
/// by recursion through `cypher_value_eq`).
fn decode_het(s: &ScalarValue) -> Option<ScalarValue> {
    use datafusion::arrow::array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int8Array, Int64Array, ListArray, StringArray,
        StructArray,
    };
    use datafusion::arrow::datatypes::{Field, Fields};
    use std::sync::Arc;
    let ScalarValue::Struct(arr) = s else {
        return None;
    };
    arr.column_by_name("__het_tag")?; // not a tagged element → leave as-is
    if arr.is_null(0) {
        return Some(ScalarValue::Null);
    }
    let tag = arr
        .column_by_name("__het_tag")?
        .as_any()
        .downcast_ref::<Int8Array>()?
        .value(0);
    let col = |name: &str| arr.column_by_name(name);
    if let Some(value) = col(&format!("__het_value_{tag}")) {
        return ScalarValue::try_from_array(value, 0).ok();
    }
    let v = match tag {
        0 => ScalarValue::Int64(Some(
            col("__het_int")?
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(0),
        )),
        1 => ScalarValue::Float64(Some(
            col("__het_float")?
                .as_any()
                .downcast_ref::<Float64Array>()?
                .value(0),
        )),
        2 => ScalarValue::Utf8(Some(
            col("__het_str")?
                .as_any()
                .downcast_ref::<StringArray>()?
                .value(0)
                .to_string(),
        )),
        3 => ScalarValue::Boolean(Some(
            col("__het_bool")?
                .as_any()
                .downcast_ref::<BooleanArray>()?
                .value(0),
        )),
        4 => ScalarValue::try_from_array(col("__het_list")?, 0).ok()?,
        5 => {
            let entries = col("__het_map")?
                .as_any()
                .downcast_ref::<ListArray>()?
                .value(0);
            let es = entries.as_any().downcast_ref::<StructArray>()?;
            if es.is_empty() {
                return Some(ScalarValue::Struct(Arc::new(
                    StructArray::new_empty_fields(1, None),
                )));
            }
            let mkeys = es
                .column_by_name("__het_mkey")?
                .as_any()
                .downcast_ref::<StringArray>()?;
            let mvals = es.column_by_name("__het_mval")?;
            let mut fields: Vec<Field> = Vec::with_capacity(es.len());
            let mut cols: Vec<ArrayRef> = Vec::with_capacity(es.len());
            for i in 0..es.len() {
                // Keep the value tagged (its length-1 slice); `cypher_value_eq`
                // decodes it per-field, mirroring the list (`tag 4`) decode.
                let varr = mvals.slice(i, 1);
                fields.push(Field::new(mkeys.value(i), varr.data_type().clone(), true));
                cols.push(varr);
            }
            ScalarValue::Struct(Arc::new(
                StructArray::try_new(Fields::from(fields), cols, None).ok()?,
            ))
        }
        _ => return None,
    };
    Some(v)
}

/// Decode one tagged heterogeneous scalar into its logical value.
#[must_use]
pub fn decode_het_scalar(value: &ScalarValue) -> Option<ScalarValue> {
    decode_het(value)
}

/// Whether a `Struct` is a node / relationship / path value (whose equality is
/// identity-based) rather than a Cypher map (three-valued structural equality),
/// detected by the reserved field names those values carry.
fn is_entity_struct(s: &datafusion::arrow::array::StructArray) -> bool {
    s.fields().iter().any(|f| {
        matches!(
            f.name().as_str(),
            "node_uuid" | "src_uuid" | "dst_uuid" | "nodes" | "relationships" | "labels"
        )
    })
}

// ---------------------------------------------------------------------------
// cypher_date_component UDF
// ---------------------------------------------------------------------------

/// `date` component accessor (`Temporal5`): `cypher_date_component(date, name)`
/// where `date` is a `Date32` and `name` is the accessor (`year`/`quarter`/
/// `month`/`week`/`weekYear`/`day`/`ordinalDay`/`weekDay`/`dayOfQuarter`).
/// Returns the component as `Int64`. (ADR 0009 / #920)
static CYPHER_DATE_COMPONENT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDateComponent::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherDateComponent {
    signature: Signature,
}

impl CypherDateComponent {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDateComponent {
    fn name(&self) -> &'static str {
        "cypher_date_component"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::date_component;
        use datafusion::arrow::array::{Array, Int64Array, StringArray, StructArray};

        let rows = args.number_rows;
        let dates = args.args[0].to_array(rows)?;
        let names = args.args[1].to_array(rows)?;
        let d = dates.as_any().downcast_ref::<StructArray>();
        let n = names.as_any().downcast_ref::<StringArray>();
        let out: Int64Array = (0..rows)
            .map(|i| {
                let (d, n) = (d?, n?);
                if n.is_null(i) {
                    return None;
                }
                date_component(date_struct_value(d, i)?, n.value(i))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// cypher_duration_component UDF
// ---------------------------------------------------------------------------

/// `duration` component accessor (`d.days`/`d.seconds`/`d.monthsOfQuarter`/…):
/// `[interval_value, component_name]` → `Int64`. (#920)
static CYPHER_DURATION_COMPONENT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDurationComponent::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherDurationComponent {
    signature: Signature,
}

impl CypherDurationComponent {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDurationComponent {
    fn name(&self) -> &'static str {
        "cypher_duration_component"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::duration_component;
        use datafusion::arrow::array::{Array, Int64Array, StringArray, StructArray};

        let rows = args.number_rows;
        let durs = args.args[0].to_array(rows)?;
        let names = args.args[1].to_array(rows)?;
        let d = durs.as_any().downcast_ref::<StructArray>();
        let n = names.as_any().downcast_ref::<StringArray>();
        let out: Int64Array = (0..rows)
            .map(|i| {
                let (d, n) = (d?, n?);
                if d.is_null(i) || n.is_null(i) {
                    return None;
                }
                duration_component(&duration_struct_parts(d, i)?, n.value(i))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// cypher_temporal_component / cypher_temporal_zone_str UDFs (Temporal5 accessors)
// ---------------------------------------------------------------------------

/// Whether `name` is a valid component accessor for a typed-temporal COLUMN type
/// other than `Date32`/duration (which dispatch separately): `localtime`
/// (`Time64`), or a `time`/`localdatetime`/`datetime` struct. Gates the accessor
/// dispatch so a non-temporal property access still falls through to a column
/// lookup. (#1008)
fn temporal_accessor_valid(dt: &DataType, name: &str) -> bool {
    use crate::temporal::{
        is_date_accessor, is_epoch_accessor, is_time_accessor, is_zone_int_accessor,
        is_zone_str_accessor,
    };
    match dt {
        DataType::Time64(_) => is_time_accessor(name),
        DataType::Struct(_) if is_time_struct(dt) => {
            is_time_accessor(name) || is_zone_int_accessor(name) || is_zone_str_accessor(name)
        }
        DataType::Struct(_) if is_localdatetime_struct(dt) => {
            is_date_accessor(name) || is_time_accessor(name)
        }
        DataType::Struct(_) if is_datetime_struct(dt) => {
            is_date_accessor(name)
                || is_time_accessor(name)
                || is_zone_int_accessor(name)
                || is_zone_str_accessor(name)
                || is_epoch_accessor(name)
        }
        _ => false,
    }
}

/// `Temporal5` INT component accessor (`d.hour`/`d.year`/`d.offsetSeconds`/
/// `d.epochMillis`/…): `[value, name]` → `Int64`. `value` is a typed `localtime`
/// (`Time64`) or `time`/`localdatetime`/`datetime` struct; the UDF inspects the
/// Arrow type to extract the relevant field. (#1008)
static CYPHER_TEMPORAL_COMPONENT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherTemporalComponent::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherTemporalComponent {
    signature: Signature,
}

impl CypherTemporalComponent {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherTemporalComponent {
    fn name(&self) -> &'static str {
        "cypher_temporal_component"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            date_component, epoch_component, is_date_accessor, is_time_accessor,
            is_zone_int_accessor, time_component, zone_int_component,
        };
        use datafusion::arrow::array::{
            Array, Int64Array, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::datatypes::TimeUnit;

        let rows = args.number_rows;
        let vals = args.args[0].to_array(rows)?;
        let names = args.args[1].to_array(rows)?;
        let n = names.as_any().downcast_ref::<StringArray>();
        let out: Int64Array = (0..rows)
            .map(|i| {
                let n = n?;
                if vals.is_null(i) || n.is_null(i) {
                    return None;
                }
                let name = n.value(i);
                match vals.data_type() {
                    DataType::Time64(TimeUnit::Nanosecond) => {
                        let v = vals.as_any().downcast_ref::<Time64NanosecondArray>()?;
                        time_component(v.value(i), name)
                    }
                    DataType::Struct(_) => {
                        let s = vals.as_any().downcast_ref::<StructArray>()?;
                        if is_time_struct(vals.data_type()) {
                            let (nanos, offset) = time_struct_parts(s, i)?;
                            if is_zone_int_accessor(name) {
                                zone_int_component(offset, name)
                            } else {
                                time_component(nanos, name)
                            }
                        } else if is_localdatetime_struct(vals.data_type()) {
                            let (days, nanos) = localdatetime_struct_parts(s, i)?;
                            if is_date_accessor(name) {
                                date_component(days, name)
                            } else {
                                time_component(nanos, name)
                            }
                        } else if is_datetime_struct(vals.data_type()) {
                            let (days, nanos, offset, _) = datetime_struct_parts(s, i)?;
                            if is_date_accessor(name) {
                                date_component(days, name)
                            } else if is_time_accessor(name) {
                                time_component(nanos, name)
                            } else if is_zone_int_accessor(name) {
                                zone_int_component(offset, name)
                            } else {
                                epoch_component(days, nanos, offset, name)
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

/// `Temporal5` STRING component accessor (`d.timezone`/`d.offset`): `[value,
/// name]` → `Utf8`. `value` is a typed `time` or `datetime` struct. (#1008)
static CYPHER_TEMPORAL_ZONE_STR: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherTemporalZoneStr::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherTemporalZoneStr {
    signature: Signature,
}

impl CypherTemporalZoneStr {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherTemporalZoneStr {
    fn name(&self) -> &'static str {
        "cypher_temporal_zone_str"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::zone_str_component;
        use datafusion::arrow::array::{Array, StringArray, StructArray};

        let rows = args.number_rows;
        let vals = args.args[0].to_array(rows)?;
        let names = args.args[1].to_array(rows)?;
        let n = names.as_any().downcast_ref::<StringArray>();
        let out: StringArray = (0..rows)
            .map(|i| {
                let n = n?;
                if vals.is_null(i) || n.is_null(i) {
                    return None;
                }
                let name = n.value(i);
                let s = vals.as_any().downcast_ref::<StructArray>()?;
                if is_time_struct(vals.data_type()) {
                    let (_, offset) = time_struct_parts(s, i)?;
                    zone_str_component(offset, None, name)
                } else if is_datetime_struct(vals.data_type()) {
                    let (_, _, offset, zone) = datetime_struct_parts(s, i)?;
                    zone_str_component(offset, zone.as_deref(), name)
                } else {
                    None
                }
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// cypher_duration_between UDF
// ---------------------------------------------------------------------------

/// `duration.between(a, b)` / `inMonths` / `inDays` / `inSeconds` (`Temporal10`):
/// `[a, b, mode]` → `Interval(MonthDayNano)`. `a`/`b` are typed temporals
/// (`Date32`/`Time64`/`localdatetime`/`time`/`datetime` struct); `mode` selects
/// the family member. (#920)
static CYPHER_DURATION_BETWEEN: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDurationBetween::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherDurationBetween {
    signature: Signature,
}

impl CypherDurationBetween {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

/// Extract a [`BetweenOperand`](crate::temporal::BetweenOperand) — `(date, nanos,
/// offset)` — from a typed temporal array at row `i`.
fn between_operand(
    arr: &datafusion::arrow::array::ArrayRef,
    i: usize,
) -> Option<crate::temporal::BetweenOperand> {
    use datafusion::arrow::array::{Array, StructArray, Time64NanosecondArray};
    use datafusion::arrow::datatypes::TimeUnit;
    if arr.is_null(i) {
        return None;
    }
    match arr.data_type() {
        DataType::Time64(TimeUnit::Nanosecond) => Some((
            None,
            arr.as_any()
                .downcast_ref::<Time64NanosecondArray>()?
                .value(i),
            None,
            None,
        )),
        DataType::Struct(_) => {
            let s = arr.as_any().downcast_ref::<StructArray>()?;
            if is_date_struct(arr.data_type()) {
                Some((Some(date_struct_value(s, i)?), 0, None, None))
            } else if is_datetime_struct(arr.data_type()) {
                // Keep the named zone (if any) so DST is re-resolvable across a
                // span (#1007).
                let (days, nanos, offset, zone) = datetime_struct_parts(s, i)?;
                Some((Some(days), nanos, Some(offset), zone))
            } else if is_time_struct(arr.data_type()) {
                let (nanos, offset) = time_struct_parts(s, i)?;
                Some((None, nanos, Some(offset), None))
            } else {
                let (days, nanos) = localdatetime_struct_parts(s, i)?;
                Some((Some(days), nanos, None, None))
            }
        }
        _ => None,
    }
}

impl ScalarUDFImpl for CypherDurationBetween {
    fn name(&self) -> &'static str {
        "cypher_duration_between"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(
            graphforge_storage::schemas::duration_struct_fields(),
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{BetweenMode, duration_between};
        use datafusion::arrow::array::{Array, StringArray};
        use datafusion::arrow::compute::cast;
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let mode_arr = cast(&cols[2], &DataType::Utf8).map_err(DataFusionError::from)?;
        let modes = mode_arr.as_any().downcast_ref::<StringArray>();

        let parts: Vec<Option<crate::temporal::DurationValue>> = (0..rows)
            .map(|i| {
                let m = modes?;
                if m.is_null(i) {
                    return None;
                }
                let mode = match m.value(i) {
                    "duration.between" => BetweenMode::Between,
                    "duration.inmonths" => BetweenMode::Months,
                    "duration.indays" => BetweenMode::Days,
                    "duration.inseconds" => BetweenMode::Seconds,
                    _ => return None,
                };
                let a = between_operand(&cols[0], i)?;
                let b = between_operand(&cols[1], i)?;
                duration_between(&a, &b, mode)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_duration_struct(&parts),
        )))
    }
}

// ---------------------------------------------------------------------------
// cypher_temporal_arith UDF (temporal ± duration)
// ---------------------------------------------------------------------------

/// `temporal ± duration` (`Temporal8`): `[temporal, duration_struct, sign]` →
/// the SAME type as the temporal operand (`return_type` echoes `arg_types[0]`).
/// `sign` is `+1` (add) or `-1` (subtract). Dispatches on the temporal type:
/// date adds months+days (sub-day time → whole days); localtime/time wrap the
/// time-of-day mod 24h; localdatetime/datetime add months+days+time carrying
/// overflow (zone offset / named zone preserved). (#920)
static CYPHER_TEMPORAL_ARITH: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherTemporalArith::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherTemporalArith {
    signature: Signature,
}

impl CypherTemporalArith {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherTemporalArith {
    fn name(&self) -> &'static str {
        "cypher_temporal_arith"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        // The result is the same temporal type as the first (temporal) operand.
        Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one cohesive per-temporal-type dispatch (date/localtime/time/\
                  localdatetime/datetime) applying a signed duration"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            date_plus_duration, datetime_plus_duration, localtime_plus_duration,
        };
        use datafusion::arrow::array::{
            Array, ArrayRef, Int64Array, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::datatypes::TimeUnit;

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let temporal = &cols[0];
        let dur = cols[1].as_any().downcast_ref::<StructArray>();
        let signs = cols[2].as_any().downcast_ref::<Int64Array>();

        // The signed [`DurationValue`] for row `i` (None if either operand is null).
        let signed = |i: usize| -> Option<crate::temporal::DurationValue> {
            let (d, sg) = (dur?, signs?);
            if d.is_null(i) || sg.is_null(i) {
                return None;
            }
            let dv = duration_struct_parts(d, i)?;
            Some(if sg.value(i) < 0 {
                crate::temporal::DurationValue {
                    months: -dv.months,
                    days: -dv.days,
                    seconds: -dv.seconds,
                    nanos: -dv.nanos,
                }
            } else {
                dv
            })
        };
        // Total signed sub-day nanoseconds of a duration (time-of-day arithmetic).
        // Sub-day nanoseconds of a duration for time-of-day (mod-24h) arithmetic.
        // Reduce `seconds` mod a day FIRST so a huge duration can't overflow the
        // `* 1e9` (the result is used mod a day anyway, so this is exact). (#1011)
        let sub_day_nanos = |d: &crate::temporal::DurationValue| {
            d.seconds.rem_euclid(86_400) * 1_000_000_000 + d.nanos
        };

        let out: ArrayRef = match temporal.data_type() {
            DataType::Struct(_) if is_date_struct(temporal.data_type()) => {
                let t = temporal.as_any().downcast_ref::<StructArray>();
                let days: Vec<Option<i64>> = (0..rows)
                    .map(|i| {
                        let t = t?;
                        let d = date_struct_value(t, i)?;
                        let dv = signed(i)?;
                        Some(date_plus_duration(d, &dv))
                    })
                    .collect();
                std::sync::Arc::new(build_date_struct(&days))
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                let t = temporal.as_any().downcast_ref::<Time64NanosecondArray>();
                let a: Time64NanosecondArray = (0..rows)
                    .map(|i| {
                        let t = t?;
                        if t.is_null(i) {
                            return None;
                        }
                        let dv = signed(i)?;
                        Some(localtime_plus_duration(t.value(i), sub_day_nanos(&dv)))
                    })
                    .collect();
                std::sync::Arc::new(a)
            }
            DataType::Struct(_) if is_time_struct(temporal.data_type()) => {
                let s = temporal.as_any().downcast_ref::<StructArray>();
                let parts: Vec<Option<(i64, i32)>> = (0..rows)
                    .map(|i| {
                        let s = s?;
                        let (nanos, offset) = time_struct_parts(s, i)?;
                        let dv = signed(i)?;
                        Some((localtime_plus_duration(nanos, sub_day_nanos(&dv)), offset))
                    })
                    .collect();
                std::sync::Arc::new(build_time_struct(&parts))
            }
            DataType::Struct(_) if is_datetime_struct(temporal.data_type()) => {
                let s = temporal.as_any().downcast_ref::<StructArray>();
                let parts: Vec<DateTimeRow> = (0..rows)
                    .map(|i| {
                        let s = s?;
                        let (days, nanos, offset, zone) = datetime_struct_parts(s, i)?;
                        let dv = signed(i)?;
                        let (date, no) = datetime_plus_duration(days, nanos, &dv);
                        Some((date, no, offset, zone))
                    })
                    .collect();
                std::sync::Arc::new(build_datetime_struct(&parts))
            }
            // localdatetime struct (date + time, no zone).
            DataType::Struct(_) if is_localdatetime_struct(temporal.data_type()) => {
                let s = temporal.as_any().downcast_ref::<StructArray>();
                let parts: Vec<Option<(i64, i64)>> = (0..rows)
                    .map(|i| {
                        let s = s?;
                        let (days, nanos) = localdatetime_struct_parts(s, i)?;
                        let dv = signed(i)?;
                        let (date, no) = datetime_plus_duration(days, nanos, &dv);
                        Some((date, no))
                    })
                    .collect();
                std::sync::Arc::new(build_localdatetime_struct(&parts))
            }
            other => {
                return Err(datafusion::error::DataFusionError::Internal(format!(
                    "cypher_temporal_arith: left operand is not a temporal value ({other:?})"
                )));
            }
        };
        Ok(ColumnarValue::Array(out))
    }
}

// ---------------------------------------------------------------------------
// cypher_duration_add UDF (duration ± duration)
// ---------------------------------------------------------------------------

/// Runtime `duration(<string-expr>)` (`Temporal6`): parse an ISO-8601 duration
/// string per row into a `Struct{months, days, seconds, nanos}` (null on unparseable or
/// null input), the inverse of the `toString` render. Used when the argument is
/// not a constant (e.g. `duration(toString(d))`). (#920)
static CYPHER_DURATION_PARSE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDurationParse::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherDurationParse {
    signature: Signature,
}

impl CypherDurationParse {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDurationParse {
    fn name(&self) -> &'static str {
        "cypher_duration_parse"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(
            graphforge_storage::schemas::duration_struct_fields(),
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, StringArray};
        use datafusion::arrow::compute::cast;

        let rows = args.number_rows;
        let arr = cast(&args.args[0].to_array(rows)?, &DataType::Utf8)?;
        let s = arr.as_any().downcast_ref::<StringArray>();
        let parts: Vec<Option<crate::temporal::DurationValue>> = (0..rows)
            .map(|i| {
                let s = s?;
                if s.is_null(i) {
                    return Option::None;
                }
                crate::temporal::duration_value_from_str(s.value(i))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_duration_struct(&parts),
        )))
    }
}

/// `duration ± duration` (`Temporal8`): `[a, b, sign]` → component-wise
/// `(a.months + sign·b.months, …days, …nanos)` as a duration struct. (#920)
static CYPHER_DURATION_ADD: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDurationAdd::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherDurationAdd {
    signature: Signature,
}

impl CypherDurationAdd {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDurationAdd {
    fn name(&self) -> &'static str {
        "cypher_duration_add"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(
            graphforge_storage::schemas::duration_struct_fields(),
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, Int64Array, StructArray};

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let a = cols[0].as_any().downcast_ref::<StructArray>();
        let b = cols[1].as_any().downcast_ref::<StructArray>();
        let signs = cols[2].as_any().downcast_ref::<Int64Array>();

        let parts: Vec<Option<crate::temporal::DurationValue>> = (0..rows)
            .map(|i| {
                let (a, b, sg) = (a?, b?, signs?);
                let av = duration_struct_parts(a, i)?;
                let bv = duration_struct_parts(b, i)?;
                let s: i64 = if sg.is_null(i) || sg.value(i) >= 0 {
                    1
                } else {
                    -1
                };
                // Add componentwise and normalise the nanos carry into seconds —
                // WITHOUT forming a `seconds * 1e9` total (which would overflow
                // i64 for combined sub-day spans > ~292 years, defeating the
                // widened `seconds` field). `nanos` sums into (-1e9, 2e9), so
                // div/rem_euclid re-canonicalise to a non-negative `[0, 1e9)`. (#1011)
                let nanos_sum = av.nanos + s * bv.nanos;
                let seconds = av.seconds + s * bv.seconds + nanos_sum.div_euclid(1_000_000_000);
                Some(crate::temporal::DurationValue {
                    months: av.months + s * bv.months,
                    days: av.days + s * bv.days,
                    seconds,
                    nanos: nanos_sum.rem_euclid(1_000_000_000),
                })
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_duration_struct(&parts),
        )))
    }
}

/// `duration * number` / `duration / number` (#920 Temporal8 [7]). Args are
/// `[duration_struct, number, is_div]`; scales each component and re-normalises
/// via [`crate::temporal::scale_duration`] (fractional months → days → time).
static CYPHER_DURATION_SCALE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDurationScale::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherDurationScale {
    signature: Signature,
}

impl CypherDurationScale {
    fn new() -> Self {
        Self {
            signature: Signature::any(3, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDurationScale {
    fn name(&self) -> &'static str {
        "cypher_duration_scale"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(
            graphforge_storage::schemas::duration_struct_fields(),
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{Array, BooleanArray, Float64Array, StructArray};
        use datafusion::arrow::compute::cast;

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let dur = cols[0].as_any().downcast_ref::<StructArray>();
        // The numeric factor may arrive as any int/float width — cast to f64.
        let num = cast(&cols[1], &DataType::Float64)?;
        let num = num.as_any().downcast_ref::<Float64Array>();
        let is_div = cols[2].as_any().downcast_ref::<BooleanArray>();

        let parts: Vec<Option<crate::temporal::DurationValue>> = (0..rows)
            .map(|i| {
                let (dur, num, is_div) = (dur?, num?, is_div?);
                if num.is_null(i) {
                    return Option::None; // duration ∘ null = null
                }
                let dv = duration_struct_parts(dur, i)?;
                let divide = !is_div.is_null(i) && is_div.value(i);
                Some(crate::temporal::scale_duration(&dv, num.value(i), divide))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_duration_struct(&parts),
        )))
    }
}

// ---------------------------------------------------------------------------
// cypher_quantifier UDF (all/any/none/single)
// ---------------------------------------------------------------------------

/// Fold per-element predicate results with three-valued logic per quantifier
/// (#955). `n` is the element count; `bools[j]` is the predicate on element `j`
/// (null = unknown). Empty list: all/none → true, any/single → false.
#[allow(
    clippy::match_same_arms,
    reason = "per-quantifier arms read clearest grouped by kind, even where two \
              fallback bodies coincide (all/none both default true)"
)]
fn reduce_quantifier(
    kind: graphforge_ir::QuantifierKind,
    bools: &datafusion::arrow::array::BooleanArray,
    n: usize,
) -> Option<bool> {
    use datafusion::arrow::array::Array;
    use graphforge_ir::QuantifierKind as Q;
    let (mut any_true, mut any_false, mut any_null, mut count_true) = (false, false, false, 0u32);
    for j in 0..n {
        if bools.is_null(j) {
            any_null = true;
        } else if bools.value(j) {
            any_true = true;
            count_true += 1;
        } else {
            any_false = true;
        }
    }
    // Three-valued logic: an unknown (null) element only matters when no
    // definitive element already settles the result.
    match kind {
        Q::All if any_false => Some(false),
        Q::All => (!any_null).then_some(true),
        Q::Any if any_true => Some(true),
        Q::Any => (!any_null).then_some(false),
        Q::None if any_true => Some(false),
        Q::None => (!any_null).then_some(true),
        Q::Single if count_true > 1 => Some(false),
        Q::Single => (!any_null).then_some(count_true == 1),
    }
}

/// `all/any/none/single(loop_var IN list WHERE predicate)` (#955). Holds the
/// predicate as a logical `Expr` over a synthetic element column + the outer
/// columns it references; at invoke time it builds a per-element `RecordBatch`,
/// evaluates the predicate, and folds with [`reduce_quantifier`]. Returns `Boolean`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherQuantifier {
    kind: graphforge_ir::QuantifierKind,
    predicate: DfExpr,
    elem_name: String,
    outer_names: Vec<String>,
    signature: Signature,
}

impl CypherQuantifier {
    fn new(
        kind: graphforge_ir::QuantifierKind,
        predicate: DfExpr,
        elem_name: String,
        outer_names: Vec<String>,
    ) -> Self {
        let arity = 1 + outer_names.len();
        Self {
            kind,
            predicate,
            elem_name,
            outer_names,
            // The predicate is embedded in the UDF rather than represented as
            // a call argument and may itself be volatile.
            signature: Signature::any(arity, Volatility::Volatile),
        }
    }
}

/// A quantifier whose predicate is statically true, false, or null. The input
/// list is still evaluated eagerly by DataFusion, but no per-element predicate
/// batch is needed; only list nullability and cardinality affect the result.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherInvariantQuantifier {
    kind: graphforge_ir::QuantifierKind,
    predicate: Option<bool>,
    signature: Signature,
}

#[cfg(test)]
static INVARIANT_QUANTIFIER_ROWS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

impl CypherInvariantQuantifier {
    fn new(kind: graphforge_ir::QuantifierKind, predicate: Option<bool>) -> Self {
        Self {
            kind,
            predicate,
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

fn reduce_invariant_quantifier(
    kind: graphforge_ir::QuantifierKind,
    predicate: Option<bool>,
    len: usize,
) -> Option<bool> {
    use graphforge_ir::QuantifierKind as Q;
    if len == 0 {
        return Some(matches!(kind, Q::All | Q::None));
    }
    match (kind, predicate) {
        (_, None) => None,
        (Q::All | Q::Any, Some(value)) => Some(value),
        (Q::None, Some(value)) => Some(!value),
        (Q::Single, Some(true)) => Some(len == 1),
        (Q::Single, Some(false)) => Some(false),
    }
}

impl ScalarUDFImpl for CypherInvariantQuantifier {
    fn name(&self) -> &'static str {
        "cypher_invariant_quantifier"
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
        use datafusion::arrow::array::{Array, BooleanArray, ListArray};
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        #[cfg(test)]
        INVARIANT_QUANTIFIER_ROWS.fetch_add(rows, std::sync::atomic::Ordering::SeqCst);
        let list = args.args[0].to_array(rows)?;
        let list = list.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Internal("cypher_invariant_quantifier: argument is not a list".into())
        })?;
        let values = (0..rows).map(|row| {
            if list.is_null(row) {
                None
            } else {
                reduce_invariant_quantifier(self.kind, self.predicate, list.value(row).len())
            }
        });
        Ok(ColumnarValue::Array(Arc::new(
            values.collect::<BooleanArray>(),
        )))
    }
}

impl ScalarUDFImpl for CypherQuantifier {
    fn name(&self) -> &'static str {
        "cypher_quantifier"
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
        use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, ListArray, RecordBatch};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::common::DFSchema;
        use datafusion::error::DataFusionError;
        use datafusion::logical_expr::execution_props::ExecutionProps;
        use datafusion::physical_expr::create_physical_expr;
        use std::sync::Arc;

        let rows = args.number_rows;
        let cols: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|a| a.to_array(rows))
            .collect::<datafusion::error::Result<_>>()?;
        let list = cols[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("cypher_quantifier: first argument is not a list".into())
            })?;
        let elem_type = match list.data_type() {
            DataType::List(f) | DataType::LargeList(f) => f.data_type().clone(),
            _ => DataType::Null,
        };

        // Synthetic schema: the element column + each referenced outer column.
        let mut fields = vec![Field::new(&self.elem_name, elem_type, true)];
        for (i, name) in self.outer_names.iter().enumerate() {
            fields.push(Field::new(name, cols[i + 1].data_type().clone(), true));
        }
        let schema = Arc::new(Schema::new(fields));
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
        // Deferred build: a predicate that cannot plan over the element type
        // (e.g. `x.a = 2` against the Int64 default of a statically-empty
        // list) only errors if a non-empty row actually evaluates it — every
        // empty row short-circuits to the fold identity below.
        let phys = create_physical_expr(&self.predicate, &df_schema, &ExecutionProps::new());

        let mut out = BooleanArray::builder(rows);
        for row in 0..rows {
            if list.is_null(row) {
                out.append_null(); // a quantifier over a null list is null
                continue;
            }
            let elems = list.value(row);
            let n = elems.len();
            if n == 0 {
                // The n = 0 fold yields the identity (`all`/`none` → true,
                // `any`/`single` → false) without running the predicate, whose
                // type over an empty list is irrelevant.
                out.append_option(reduce_quantifier(self.kind, &BooleanArray::new_null(0), 0));
                continue;
            }
            let phys = phys
                .as_ref()
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            let verdict = (|| {
                let mut batch_cols: Vec<ArrayRef> = Vec::with_capacity(1 + self.outer_names.len());
                batch_cols.push(elems);
                for i in 0..self.outer_names.len() {
                    let sv = ScalarValue::try_from_array(&cols[i + 1], row).ok()?;
                    batch_cols.push(sv.to_array_of_size(n).ok()?);
                }
                let batch = RecordBatch::try_new(Arc::clone(&schema), batch_cols).ok()?;
                let evaluated = phys.evaluate(&batch).ok()?.into_array(n).ok()?;
                // A typeless evaluation (`WHERE x` over untyped elements) is
                // 3VL unknown per element, not a row failure.
                if evaluated.data_type() == &DataType::Null {
                    return reduce_quantifier(self.kind, &BooleanArray::new_null(n), n);
                }
                let bools = evaluated.as_any().downcast_ref::<BooleanArray>()?;
                reduce_quantifier(self.kind, bools, n)
            })();
            out.append_option(verdict);
        }
        Ok(ColumnarValue::Array(std::sync::Arc::new(out.finish())))
    }
}

/// `[loop_var IN list WHERE filter | projection]` (#955). Holds the optional
/// filter + projection as logical `Expr`s over a synthetic element column +
/// referenced outer columns. At invoke time it builds a per-element
/// `RecordBatch` per row, keeps the elements the filter accepts (3VL: only
/// definitively-true), maps them through the projection, and reassembles a
/// `ListArray`. A bare `[x IN list]` (no clauses) is the list itself; a null
/// list row yields a null list.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherListComp {
    filter: Option<DfExpr>,
    projection: Option<DfExpr>,
    elem_name: String,
    outer_names: Vec<String>,
    signature: Signature,
}

impl CypherListComp {
    fn new(
        filter: Option<DfExpr>,
        projection: Option<DfExpr>,
        elem_name: String,
        outer_names: Vec<String>,
    ) -> Self {
        let arity = 1 + outer_names.len();
        Self {
            filter,
            projection,
            elem_name,
            outer_names,
            // Filter/projection expressions are embedded in the UDF and may
            // contain rand() or another volatile function.
            signature: Signature::any(arity, Volatility::Volatile),
        }
    }

    /// The element type of the result list: the projection's output type over
    /// the synthetic schema, or the input element type when there is no
    /// projection. Computed identically at plan time (`return_type`) and invoke
    /// time so the produced `ListArray`'s child type matches the declared type.
    fn item_type(
        &self,
        elem_type: &DataType,
        outer_types: &[DataType],
    ) -> datafusion::error::Result<DataType> {
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::common::DFSchema;
        use datafusion::logical_expr::execution_props::ExecutionProps;
        use datafusion::physical_expr::create_physical_expr;
        let Some(proj) = &self.projection else {
            return Ok(elem_type.clone());
        };
        let mut fields = vec![Field::new(&self.elem_name, elem_type.clone(), true)];
        for (name, dt) in self.outer_names.iter().zip(outer_types) {
            fields.push(Field::new(name, dt.clone(), true));
        }
        let schema = Schema::new(fields);
        let df_schema = DFSchema::try_from(schema.clone())?;
        let phys = create_physical_expr(proj, &df_schema, &ExecutionProps::new())?;
        phys.data_type(&schema)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one flatten/filter/project/reassemble pass keeps volatile evaluation and row offsets aligned"
    )]
    fn invoke_uncorrelated(
        list: &datafusion::arrow::array::ListArray,
        schema: datafusion::arrow::datatypes::SchemaRef,
        filter_phys: Option<&Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        proj_phys: Option<&Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        item_type: &DataType,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{
            Array, ArrayRef, BooleanArray, ListArray, UInt32Array, new_empty_array,
        };
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use datafusion::arrow::compute::{cast, filter_record_batch, take};
        use datafusion::arrow::datatypes::Field;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::error::DataFusionError;

        let rows = list.len();
        let mut validity = Vec::with_capacity(rows);
        let mut lengths = Vec::with_capacity(rows);
        let mut indices = Vec::new();
        let offsets = list.value_offsets();
        for row in 0..rows {
            let valid = list.is_valid(row);
            validity.push(valid);
            let length = if valid {
                usize::try_from(offsets[row + 1] - offsets[row]).map_err(|_| {
                    DataFusionError::Internal("negative list-comprehension length".into())
                })?
            } else {
                0
            };
            lengths.push(length);
            if valid {
                for index in offsets[row]..offsets[row + 1] {
                    indices.push(u32::try_from(index).map_err(|_| {
                        DataFusionError::Internal(
                            "list-comprehension element index exceeds u32::MAX".into(),
                        )
                    })?);
                }
            }
        }

        let flat: ArrayRef = if indices.len() == list.values().len()
            && indices.first().is_none_or(|first| *first == 0)
        {
            Arc::clone(list.values())
        } else {
            take(list.values(), &UInt32Array::from(indices), None)?
        };
        let total = flat.len();
        let batch = RecordBatch::try_new(schema, vec![flat])?;
        let mask = if let Some(filter) = filter_phys
            && total > 0
        {
            let evaluated = filter.evaluate(&batch)?.into_array(total)?;
            let evaluated = evaluated
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "cypher_list_comprehension: filter did not evaluate to boolean".into(),
                    )
                })?;
            Some(
                (0..total)
                    .map(|index| evaluated.is_valid(index) && evaluated.value(index))
                    .collect::<BooleanArray>(),
            )
        } else {
            None
        };
        let kept = if let Some(mask) = &mask {
            filter_record_batch(&batch, mask)?
        } else {
            batch
        };
        let kept_rows = kept.num_rows();
        let projected = if let Some(projection) = proj_phys {
            if kept_rows == 0 {
                new_empty_array(item_type)
            } else {
                projection.evaluate(&kept)?.into_array(kept_rows)?
            }
        } else {
            Arc::clone(kept.column(0))
        };
        let projected = if projected.data_type() == item_type {
            projected
        } else {
            cast(&projected, item_type)?
        };

        let mut output_offsets = Vec::with_capacity(rows + 1);
        output_offsets.push(0i32);
        let mut input_offset = 0usize;
        let mut output_offset = 0i32;
        for length in lengths {
            let kept = mask.as_ref().map_or(length, |mask| {
                (input_offset..input_offset + length)
                    .filter(|index| mask.value(*index))
                    .count()
            });
            input_offset += length;
            let kept = i32::try_from(kept).map_err(|_| {
                DataFusionError::Internal(
                    "cypher_list_comprehension: list length exceeds i32::MAX".into(),
                )
            })?;
            output_offset = output_offset.checked_add(kept).ok_or_else(|| {
                DataFusionError::Internal(
                    "cypher_list_comprehension: total list length exceeds i32::MAX".into(),
                )
            })?;
            output_offsets.push(output_offset);
        }
        let list = ListArray::try_new(
            Arc::new(Field::new("item", item_type.clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(output_offsets)),
            projected,
            Some(NullBuffer::from(validity)),
        )?;
        Ok(ColumnarValue::Array(Arc::new(list)))
    }
}

impl ScalarUDFImpl for CypherListComp {
    fn name(&self) -> &'static str {
        "cypher_list_comprehension"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        use datafusion::arrow::datatypes::Field;
        // Cypher lists lower to Arrow `List` (i32 offsets), which is also what
        // this UDF produces — keep input handling aligned with `invoke` (which
        // downcasts to `ListArray`) so a `LargeList` cannot pass planning and
        // then fail at runtime.
        let elem_type = match arg_types.first() {
            Some(DataType::List(f)) => f.data_type().clone(),
            _ => DataType::Null,
        };
        let item = self.item_type(&elem_type, arg_types.get(1..).unwrap_or(&[]))?;
        Ok(DataType::List(std::sync::Arc::new(Field::new(
            "item", item, true,
        ))))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the per-row filter/project/reassemble loop reads clearest inline"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::array::{
            Array, ArrayRef, BooleanArray, ListArray, RecordBatch, new_empty_array,
        };
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use datafusion::arrow::compute::{cast, concat, filter_record_batch};
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::common::DFSchema;
        use datafusion::error::DataFusionError;
        use datafusion::logical_expr::execution_props::ExecutionProps;
        use datafusion::physical_expr::create_physical_expr;
        use std::sync::Arc;

        let rows = args.number_rows;
        let cols: Vec<ArrayRef> = args
            .args
            .iter()
            .map(|a| a.to_array(rows))
            .collect::<datafusion::error::Result<_>>()?;
        let list = cols[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "cypher_list_comprehension: first argument is not a list".into(),
                )
            })?;
        let elem_type = match list.data_type() {
            DataType::List(f) => f.data_type().clone(),
            _ => DataType::Null,
        };
        let outer_types: Vec<DataType> = (0..self.outer_names.len())
            .map(|i| cols[i + 1].data_type().clone())
            .collect();
        let item_type = self.item_type(&elem_type, &outer_types)?;

        // Synthetic schema: the element column + each referenced outer column.
        let mut fields = vec![Field::new(&self.elem_name, elem_type, true)];
        for (i, name) in self.outer_names.iter().enumerate() {
            fields.push(Field::new(name, cols[i + 1].data_type().clone(), true));
        }
        let schema = Arc::new(Schema::new(fields));
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
        let props = ExecutionProps::new();
        let filter_phys = self
            .filter
            .as_ref()
            .map(|f| create_physical_expr(f, &df_schema, &props))
            .transpose()?;
        let proj_phys = self
            .projection
            .as_ref()
            .map(|p| create_physical_expr(p, &df_schema, &props))
            .transpose()?;

        if self.outer_names.is_empty() {
            return Self::invoke_uncorrelated(
                list,
                schema,
                filter_phys.as_ref(),
                proj_phys.as_ref(),
                &item_type,
            );
        }

        let mut pieces: Vec<ArrayRef> = Vec::new();
        let mut offsets: Vec<i32> = Vec::with_capacity(rows + 1);
        offsets.push(0);
        let mut validity: Vec<bool> = Vec::with_capacity(rows);
        let mut cur: i32 = 0;

        for row in 0..rows {
            if list.is_null(row) {
                validity.push(false);
                offsets.push(cur);
                continue;
            }
            validity.push(true);
            let elems = list.value(row);
            let n = elems.len();
            let mut batch_cols: Vec<ArrayRef> = Vec::with_capacity(1 + self.outer_names.len());
            batch_cols.push(elems);
            for i in 0..self.outer_names.len() {
                let sv = ScalarValue::try_from_array(&cols[i + 1], row)?;
                batch_cols.push(sv.to_array_of_size(n)?);
            }
            let batch = RecordBatch::try_new(Arc::clone(&schema), batch_cols)?;

            // Filter: keep only elements the predicate accepts (3VL → null/false drop).
            let kept = if let Some(fp) = &filter_phys {
                let mask = fp.evaluate(&batch)?.into_array(n)?;
                let mask = mask
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal(
                            "cypher_list_comprehension: filter did not evaluate to boolean".into(),
                        )
                    })?;
                let clean: BooleanArray =
                    (0..n).map(|j| mask.is_valid(j) && mask.value(j)).collect();
                filter_record_batch(&batch, &clean)?
            } else {
                batch
            };

            // Projection: map each surviving element (or the element itself).
            let projected: ArrayRef = if let Some(pp) = &proj_phys {
                let m = kept.num_rows();
                if m == 0 {
                    new_empty_array(&item_type)
                } else {
                    pp.evaluate(&kept)?.into_array(m)?
                }
            } else {
                Arc::clone(kept.column(0))
            };
            let projected = if projected.data_type() == &item_type {
                projected
            } else {
                cast(&projected, &item_type)?
            };

            let len = i32::try_from(projected.len()).map_err(|_| {
                DataFusionError::Internal("cypher_list_comprehension: list too long".into())
            })?;
            cur = cur.checked_add(len).ok_or_else(|| {
                DataFusionError::Internal(
                    "cypher_list_comprehension: total list length exceeds i32::MAX".into(),
                )
            })?;
            offsets.push(cur);
            pieces.push(projected);
        }

        let child: ArrayRef = if pieces.is_empty() {
            new_empty_array(&item_type)
        } else {
            let refs: Vec<&dyn Array> = pieces.iter().map(AsRef::as_ref).collect();
            concat(&refs)?
        };
        let field = Arc::new(Field::new("item", item_type, true));
        let list_arr = ListArray::try_new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            child,
            Some(NullBuffer::from(validity)),
        )?;
        Ok(ColumnarValue::Array(Arc::new(list_arr)))
    }
}

// ---------------------------------------------------------------------------
// cypher_date_project UDF
// ---------------------------------------------------------------------------

fn udf_argument_arrays(
    args: &ScalarFunctionArgs,
) -> datafusion::error::Result<Vec<datafusion::arrow::array::ArrayRef>> {
    args.args
        .iter()
        .map(|value| value.to_array(args.number_rows))
        .collect()
}

fn cast_argument_arrays(
    arrays: &[datafusion::arrow::array::ArrayRef],
    data_type: &DataType,
) -> datafusion::error::Result<Vec<datafusion::arrow::array::ArrayRef>> {
    arrays
        .iter()
        .map(|array| datafusion::arrow::compute::cast(array, data_type).map_err(Into::into))
        .collect()
}

/// `date`-from-value projection (`Temporal3`): `date(base)` / `date({date: base,
/// …overrides})`. Args are `[base, year, month, day, week, dayOfWeek,
/// ordinalDay, quarter, dayOfQuarter]` — `base` is a `Date32` or an ISO date/
/// datetime string, the eight overrides are nullable integers (null ⇒ keep the
/// base's component). Returns `Date32`. (ADR 0009 / #920)
static CYPHER_DATE_PROJECT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDateProject::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherDateProject {
    signature: Signature,
}

impl CypherDateProject {
    fn new() -> Self {
        Self {
            signature: Signature::any(9, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDateProject {
    fn name(&self) -> &'static str {
        "cypher_date_project"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(
            graphforge_storage::schemas::date_struct_fields(),
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{DateOverrides, parse_date_or_datetime_prefix};
        use datafusion::arrow::array::{Array, StringArray, StructArray};
        use datafusion::arrow::compute::cast;

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        // A typed temporal-struct base (date / localdatetime / time / datetime) is
        // read directly; a string base is cast to `Utf8` (handling `Utf8View`).
        let base = if is_date_struct(cols[0].data_type())
            || is_localdatetime_struct(cols[0].data_type())
            || is_time_struct(cols[0].data_type())
            || is_datetime_struct(cols[0].data_type())
        {
            std::sync::Arc::clone(&cols[0])
        } else {
            cast(&cols[0], &DataType::Utf8).map_err(datafusion::error::DataFusionError::from)?
        };
        // Overrides: cast each to Int64 once (a null/absent override stays null).
        let ov: Vec<_> = cols[1..9]
            .iter()
            .map(|c| cast(c, &DataType::Int64))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(datafusion::error::DataFusionError::from)?;

        let base_date = |i: usize| -> Option<i64> {
            if base.is_null(i) {
                return None;
            }
            match base.data_type() {
                DataType::Struct(_) => {
                    let s = base.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(base.data_type()) {
                        date_struct_value(s, i)
                    } else {
                        // A `localdatetime`/`datetime` value — take its date component.
                        Some(localdatetime_struct_parts(s, i)?.0)
                    }
                }
                DataType::Utf8 => parse_date_or_datetime_prefix(
                    base.as_any().downcast_ref::<StringArray>()?.value(i),
                ),
                _ => None,
            }
        };

        let out: Vec<Option<i64>> = (0..rows)
            .map(|i| {
                let overrides = DateOverrides {
                    year: optional_i64_at(&ov[0], i),
                    month: optional_i64_at(&ov[1], i),
                    day: optional_i64_at(&ov[2], i),
                    week: optional_i64_at(&ov[3], i),
                    day_of_week: optional_i64_at(&ov[4], i),
                    ordinal_day: optional_i64_at(&ov[5], i),
                    quarter: optional_i64_at(&ov[6], i),
                    day_of_quarter: optional_i64_at(&ov[7], i),
                };
                crate::temporal::project_date(base_date(i)?, &overrides)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_date_struct(&out),
        )))
    }
}

// ---------------------------------------------------------------------------
// cypher_localtime_project UDF
// ---------------------------------------------------------------------------

/// `localtime`-from-value projection (`Temporal3`): `localtime(base)` /
/// `localtime({time: base, …overrides})`. Args are `[base, hour, minute, second,
/// millisecond, microsecond, nanosecond]` — `base` is a `Time64(Nanosecond)` or
/// any ISO temporal string (its time-of-day is extracted), the six overrides are
/// nullable integers (null ⇒ keep the base's component). Returns
/// `Time64(Nanosecond)`. (ADR 0009)
static CYPHER_LOCALTIME_PROJECT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherLocalTimeProject::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherLocalTimeProject {
    signature: Signature,
}

impl CypherLocalTimeProject {
    fn new() -> Self {
        Self {
            signature: Signature::any(7, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherLocalTimeProject {
    fn name(&self) -> &'static str {
        "cypher_localtime_project"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Time64(
            datafusion::arrow::datatypes::TimeUnit::Nanosecond,
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{LocalTimeOverrides, project_localtime, time_of_day_nanos_any};
        use datafusion::arrow::array::{
            Array, ArrayRef, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        // A `Time64(Nanosecond)` or `localdatetime`-struct base is read directly;
        // any other (string) base is cast to `Utf8` (handling `Utf8View`).
        let base: ArrayRef =
            if matches!(cols[0].data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_localdatetime_struct(cols[0].data_type())
                || is_time_struct(cols[0].data_type())
                || is_datetime_struct(cols[0].data_type())
            {
                std::sync::Arc::clone(&cols[0])
            } else {
                cast(&cols[0], &DataType::Utf8).map_err(DataFusionError::from)?
            };
        let ov = cast_argument_arrays(&cols[1..7], &DataType::Int64)?;

        let base_nanos = |i: usize| -> Option<i64> {
            if base.is_null(i) {
                return None;
            }
            match base.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some(
                    base.as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                ),
                // A `localdatetime` or `time` value — take its time-of-day.
                DataType::Struct(_) => {
                    let s = base.as_any().downcast_ref::<StructArray>()?;
                    if is_time_struct(base.data_type()) {
                        Some(time_struct_parts(s, i)?.0)
                    } else {
                        Some(localdatetime_struct_parts(s, i)?.1)
                    }
                }
                DataType::Utf8 => {
                    time_of_day_nanos_any(base.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let out: Time64NanosecondArray = (0..rows)
            .map(|i| {
                let overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[0], i),
                    minute: optional_i64_at(&ov[1], i),
                    second: optional_i64_at(&ov[2], i),
                    millisecond: optional_i64_at(&ov[3], i),
                    microsecond: optional_i64_at(&ov[4], i),
                    nanosecond: optional_i64_at(&ov[5], i),
                };
                project_localtime(base_nanos(i)?, &overrides)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// cypher_localtime_truncate UDF
// ---------------------------------------------------------------------------

/// `localtime.truncate(unit, value, map)` (`Temporal9`): truncate `value`'s
/// time-of-day to `unit`, then apply the override `map`. Args are `[value, unit,
/// hour, minute, second, millisecond, microsecond, nanosecond]` — `value` is a
/// `Time64(ns)` / `localdatetime`/`time`/`datetime` struct / ISO string, `unit` a
/// string, the six overrides nullable integers. Returns `Time64(ns)`. (#920)
static CYPHER_LOCALTIME_TRUNCATE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherLocalTimeTruncate::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherLocalTimeTruncate {
    signature: Signature,
}

impl CypherLocalTimeTruncate {
    fn new() -> Self {
        Self {
            signature: Signature::any(8, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherLocalTimeTruncate {
    fn name(&self) -> &'static str {
        "cypher_localtime_truncate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Time64(
            datafusion::arrow::datatypes::TimeUnit::Nanosecond,
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            LocalTimeOverrides, project_localtime, time_of_day_nanos_any, truncate_time_nanos,
        };
        use datafusion::arrow::array::{
            Array, ArrayRef, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let base: ArrayRef =
            if matches!(cols[0].data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_localdatetime_struct(cols[0].data_type())
                || is_time_struct(cols[0].data_type())
                || is_datetime_struct(cols[0].data_type())
            {
                std::sync::Arc::clone(&cols[0])
            } else {
                cast(&cols[0], &DataType::Utf8).map_err(DataFusionError::from)?
            };
        let units_arr = cast(&cols[1], &DataType::Utf8).map_err(DataFusionError::from)?;
        let units = units_arr.as_any().downcast_ref::<StringArray>();
        let ov = cast_argument_arrays(&cols[2..8], &DataType::Int64)?;

        let base_nanos = |i: usize| -> Option<i64> {
            if base.is_null(i) {
                return None;
            }
            match base.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some(
                    base.as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                ),
                DataType::Struct(_) => {
                    let s = base.as_any().downcast_ref::<StructArray>()?;
                    if is_time_struct(base.data_type()) {
                        Some(time_struct_parts(s, i)?.0)
                    } else {
                        Some(localdatetime_struct_parts(s, i)?.1)
                    }
                }
                DataType::Utf8 => {
                    time_of_day_nanos_any(base.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let out: Time64NanosecondArray = (0..rows)
            .map(|i| {
                let u = units?;
                if u.is_null(i) {
                    return None;
                }
                let truncated = truncate_time_nanos(base_nanos(i)?, u.value(i))?;
                let overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[0], i),
                    minute: optional_i64_at(&ov[1], i),
                    second: optional_i64_at(&ov[2], i),
                    millisecond: optional_i64_at(&ov[3], i),
                    microsecond: optional_i64_at(&ov[4], i),
                    nanosecond: optional_i64_at(&ov[5], i),
                };
                project_localtime(truncated, &overrides)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(out)))
    }
}

// ---------------------------------------------------------------------------
// cypher_localdatetime_project UDF
// ---------------------------------------------------------------------------

/// The Arrow fields of a standalone `date` value — `Struct{epoch_day: Int64}`
/// (ADR 0012). A one-field struct (not a bare `Int64`) so a `date` is
/// self-describing on storage decode — a plain integer property would be
/// indistinguishable — while spanning the full openCypher year range (#1011).
fn date_fields() -> datafusion::arrow::datatypes::Fields {
    graphforge_storage::schemas::date_struct_fields()
}

/// True if `dt` is the standalone `date` struct type (`Struct{epoch_day: Int64}`),
/// distinguished from the other temporal structs by its single field name.
fn is_date_struct(dt: &DataType) -> bool {
    matches!(dt, DataType::Struct(fields)
        if fields.len() == 1
            && fields[0].name() == "epoch_day"
            && *fields[0].data_type() == DataType::Int64)
}

/// Build a standalone `date` struct array from per-row i64 epoch-days (`None` ⇒ a
/// null row).
fn build_date_struct(rows: &[Option<i64>]) -> datafusion::arrow::array::StructArray {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::buffer::NullBuffer;
    let days: Int64Array = rows.iter().copied().collect();
    let nulls = rows.iter().map(Option::is_some).collect::<NullBuffer>();
    datafusion::arrow::array::StructArray::new(
        date_fields(),
        vec![std::sync::Arc::new(days)],
        Some(nulls),
    )
}

/// A standalone `date` scalar (`None` ⇒ a null value).
fn date_scalar(days: Option<i64>) -> ScalarValue {
    ScalarValue::Struct(std::sync::Arc::new(build_date_struct(&[days])))
}

/// The i64 epoch-day of a standalone `date` struct row (`None` for a null row).
fn date_struct_value(arr: &datafusion::arrow::array::StructArray, i: usize) -> Option<i64> {
    use datafusion::arrow::array::{Array, Int64Array};
    if arr.is_null(i) {
        return None;
    }
    let days = arr.column(0).as_any().downcast_ref::<Int64Array>()?;
    days.is_valid(i).then(|| days.value(i))
}

/// Read one nullable Int64 override from an already-normalized Arrow column.
/// Temporal projectors cast override columns once before their row loop, so a
/// failed physical downcast or a null row has the same absent-override meaning.
fn optional_i64_at(array: &datafusion::arrow::array::ArrayRef, row: usize) -> Option<i64> {
    use datafusion::arrow::array::{Array, Int64Array};

    let values = array.as_any().downcast_ref::<Int64Array>()?;
    (!values.is_null(row)).then(|| values.value(row))
}

/// The Arrow fields of a `localdatetime` value — `Struct{date: Int64, time:
/// Time64(Nanosecond)}`. `date` is first so DataFusion's row-format sort orders
/// chronologically (date, then time-of-day); `date` is i64 days (#1011).
fn localdatetime_fields() -> datafusion::arrow::datatypes::Fields {
    graphforge_storage::schemas::localdatetime_struct_fields()
}

/// True if `dt` is the `localdatetime` struct type (used to dispatch base
/// extraction and rendering without colliding with user maps, whose `date`/
/// `time` fields would not have these exact `Int64`/`Time64` types).
fn is_localdatetime_struct(dt: &DataType) -> bool {
    use datafusion::arrow::datatypes::TimeUnit;
    matches!(dt, DataType::Struct(fields)
        if fields.len() == 2
            && fields[0].name() == "date"
            && *fields[0].data_type() == DataType::Int64
            && fields[1].name() == "time"
            && *fields[1].data_type() == DataType::Time64(TimeUnit::Nanosecond))
}

/// Build a `localdatetime` struct array from per-row `(date_days, nanos_of_day)`
/// (`None` ⇒ a null row).
fn build_localdatetime_struct(
    rows: &[Option<(i64, i64)>],
) -> datafusion::arrow::array::StructArray {
    use datafusion::arrow::array::{Int64Array, Time64NanosecondArray};
    use datafusion::arrow::buffer::NullBuffer;
    let days: Int64Array = rows.iter().map(|r| r.map(|(d, _)| d)).collect();
    let nanos: Time64NanosecondArray = rows.iter().map(|r| r.map(|(_, n)| n)).collect();
    let nulls = rows.iter().map(Option::is_some).collect::<NullBuffer>();
    datafusion::arrow::array::StructArray::new(
        localdatetime_fields(),
        vec![std::sync::Arc::new(days), std::sync::Arc::new(nanos)],
        Some(nulls),
    )
}

/// A `localdatetime` scalar (`None` ⇒ a null value).
fn localdatetime_scalar(parts: Option<(i64, i64)>) -> ScalarValue {
    ScalarValue::Struct(std::sync::Arc::new(build_localdatetime_struct(&[parts])))
}

/// True if `dt` is the typed `duration` struct (`Struct{months, days, seconds, nanos}`,
/// ADR 0009) — distinguished from the other temporal structs by its field names.
fn is_duration_struct(dt: &DataType) -> bool {
    matches!(dt, DataType::Struct(fields)
        if fields.len() == 4
            && fields[0].name() == "months"
            && fields[1].name() == "days"
            && fields[2].name() == "seconds"
            && fields[3].name() == "nanos")
}

/// Whether a function name is a temporal clock accessor —
/// `<type>.transaction` / `.statement` / `.realtime` for an instant type
/// (`date`/`localtime`/`time`/`localdatetime`/`datetime`). (#920)
fn is_temporal_clock_fn(name: &str) -> bool {
    // Cypher function names are case-insensitive (`Date.Realtime` ≡ `date.realtime`).
    matches!(
        name.to_ascii_lowercase().split_once('.'),
        Some((
            "date" | "localtime" | "time" | "localdatetime" | "datetime",
            "transaction" | "statement" | "realtime",
        ))
    )
}

/// The typed null for a temporal constructor / clock function — `date` →
/// `Date32(None)`, `localtime` → `Time64(None)`, etc. — so null propagation
/// preserves the Arrow temporal type rather than erasing it to `Null` (which
/// would defeat downstream `is_temporal_typed` checks). The base name is the
/// part before any `.clock` suffix, matched case-insensitively. (#920)
fn temporal_null_scalar(name: &str) -> ScalarValue {
    let lower = name.to_ascii_lowercase();
    let base = lower.split('.').next().unwrap_or(&lower);
    match base {
        "date" => date_scalar(None),
        "localtime" => ScalarValue::Time64Nanosecond(None),
        "time" => time_scalar(None),
        "localdatetime" => localdatetime_scalar(None),
        "datetime" => datetime_scalar(None),
        "duration" => duration_scalar(None),
        _ => ScalarValue::Null,
    }
}

/// Build a `duration` struct array from per-row [`DurationValue`]s (`None` ⇒ a
/// null row). The on-disk + query representation of a Cypher duration —
/// `Struct{months,days,seconds,nanos}` all Int64 (Parquet cannot persist Arrow
/// `Interval(MonthDayNano)`). (#920/#1011)
fn build_duration_struct(
    rows: &[Option<crate::temporal::DurationValue>],
) -> datafusion::arrow::array::StructArray {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::buffer::NullBuffer;
    let months: Int64Array = rows.iter().map(|r| r.map(|d| d.months)).collect();
    let days: Int64Array = rows.iter().map(|r| r.map(|d| d.days)).collect();
    let seconds: Int64Array = rows.iter().map(|r| r.map(|d| d.seconds)).collect();
    let nanos: Int64Array = rows.iter().map(|r| r.map(|d| d.nanos)).collect();
    let nulls = rows.iter().map(Option::is_some).collect::<NullBuffer>();
    datafusion::arrow::array::StructArray::new(
        graphforge_storage::schemas::duration_struct_fields(),
        vec![
            std::sync::Arc::new(months),
            std::sync::Arc::new(days),
            std::sync::Arc::new(seconds),
            std::sync::Arc::new(nanos),
        ],
        Some(nulls),
    )
}

/// Extract a [`DurationValue`] from a `duration` struct array at row `i`
/// (`None` for a null row). (#920/#1011)
fn duration_struct_parts(
    arr: &datafusion::arrow::array::StructArray,
    i: usize,
) -> Option<crate::temporal::DurationValue> {
    use datafusion::arrow::array::{Array, Int64Array};
    if arr.is_null(i) {
        return None;
    }
    let col = |idx: usize| arr.column(idx).as_any().downcast_ref::<Int64Array>();
    Some(crate::temporal::DurationValue {
        months: col(0)?.value(i),
        days: col(1)?.value(i),
        seconds: col(2)?.value(i),
        nanos: col(3)?.value(i),
    })
}

/// Build a typed `duration` scalar from a [`DurationValue`] (`None` ⇒ null). (#920)
fn duration_scalar(parts: Option<crate::temporal::DurationValue>) -> ScalarValue {
    ScalarValue::Struct(std::sync::Arc::new(build_duration_struct(&[parts])))
}

/// A sub-day-only [`DurationValue`] from whole `seconds` + non-negative
/// `nanos`-of-second (no month/day part) — for the native-Arrow duration arms. (#1011)
fn dur_secs_nanos(seconds: i64, nanos: i64) -> crate::temporal::DurationValue {
    crate::temporal::DurationValue {
        months: 0,
        days: 0,
        seconds,
        nanos,
    }
}

/// Convert a [`DurationValue`] to an `IrLiteral::Duration` (storage form). (#1011)
fn duration_value_to_ir(d: crate::temporal::DurationValue) -> IrLiteral {
    IrLiteral::Duration {
        months: d.months,
        days: d.days,
        seconds: d.seconds,
        nanos: d.nanos,
    }
}

/// Extract `(date_days, nanos_of_day)` from a `localdatetime` struct array at
/// row `i` (`None` for a null row). Also used to read the local date+time of a
/// `datetime` struct (whose leading two fields are the same `Int64`+`Time64`),
/// dropping its zone — the correct semantics for `date`/`localtime`/
/// `localdatetime` projections from a `datetime`.
fn localdatetime_struct_parts(
    arr: &datafusion::arrow::array::StructArray,
    i: usize,
) -> Option<(i64, i64)> {
    use datafusion::arrow::array::{Array, Int64Array, Time64NanosecondArray};
    if arr.is_null(i) {
        return None;
    }
    let d = arr.column(0).as_any().downcast_ref::<Int64Array>()?;
    let t = arr
        .column(1)
        .as_any()
        .downcast_ref::<Time64NanosecondArray>()?;
    (!d.is_null(i) && !t.is_null(i)).then(|| (d.value(i), t.value(i)))
}

/// `localdatetime`-from-value projection (`Temporal3`). Args are `[date_source,
/// time_source, year, month, day, week, dayOfWeek, ordinalDay, quarter,
/// dayOfQuarter, hour, minute, second, millisecond, microsecond, nanosecond]`.
/// `date_source`/`time_source` are the lowered `datetime`/`date`/`time` anchors
/// (a `Date32`/`Time64`/`localdatetime`-struct/temporal-string, or null),
/// interpreted as a date and a time-of-day respectively (a null date ⇒ epoch, a
/// null time ⇒ midnight). The 14 overrides are nullable integers (null ⇒ keep
/// the base's component). Returns the `localdatetime` struct. (ADR 0009)
static CYPHER_LOCALDATETIME_PROJECT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherLocalDateTimeProject::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherLocalDateTimeProject {
    signature: Signature,
}

impl CypherLocalDateTimeProject {
    fn new() -> Self {
        Self {
            signature: Signature::any(16, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherLocalDateTimeProject {
    fn name(&self) -> &'static str {
        "cypher_localdatetime_project"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(localdatetime_fields()))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one cohesive per-row projection: two typed base extractions \
                  (date + time) plus 14 component overrides — splitting it would \
                  scatter the row logic across helpers without aiding clarity"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            DateOverrides, LocalTimeOverrides, parse_date_or_datetime_prefix, project_date,
            project_localtime, time_of_day_nanos_any,
        };
        use datafusion::arrow::array::{
            Array, ArrayRef, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        // Typed sources (`date`/`localdatetime`/`time`/`datetime` struct or
        // `Time64`) are read directly; a string source is cast to `Utf8`
        // (handling `Utf8View`).
        let typed_or_utf8 = |a: &ArrayRef| -> datafusion::error::Result<ArrayRef> {
            if matches!(a.data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_date_struct(a.data_type())
                || is_localdatetime_struct(a.data_type())
                || is_time_struct(a.data_type())
                || is_datetime_struct(a.data_type())
            {
                Ok(std::sync::Arc::clone(a))
            } else {
                cast(a, &DataType::Utf8).map_err(DataFusionError::from)
            }
        };
        let date_src = typed_or_utf8(&cols[0])?;
        let time_src = typed_or_utf8(&cols[1])?;
        let ov = cast_argument_arrays(&cols[2..16], &DataType::Int64)?;

        let base_date = |i: usize| -> Option<i64> {
            if date_src.is_null(i) {
                return Some(0); // a missing date defaults to the epoch (day 0)
            }
            match date_src.data_type() {
                DataType::Struct(_) => {
                    let s = date_src.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(date_src.data_type()) {
                        date_struct_value(s, i)
                    } else {
                        // A `localdatetime`/`datetime` value — take its date.
                        Some(localdatetime_struct_parts(s, i)?.0)
                    }
                }
                DataType::Utf8 => parse_date_or_datetime_prefix(
                    date_src.as_any().downcast_ref::<StringArray>()?.value(i),
                ),
                _ => None,
            }
        };
        let base_time = |i: usize| -> Option<i64> {
            if time_src.is_null(i) {
                return Some(0); // a missing time defaults to midnight
            }
            match time_src.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some(
                    time_src
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                ),
                DataType::Struct(_) => {
                    let s = time_src.as_any().downcast_ref::<StructArray>()?;
                    // A date-only source (bare `localdatetime(date(…))`, where the
                    // same value feeds both slots) has no time-of-day → midnight,
                    // matching `localdatetime({date: d})`. (A time-only source in
                    // the *date* slot correctly stays null — no date to fabricate.)
                    if is_date_struct(time_src.data_type()) {
                        Some(0)
                    } else if is_time_struct(time_src.data_type()) {
                        Some(time_struct_parts(s, i)?.0)
                    } else {
                        Some(localdatetime_struct_parts(s, i)?.1)
                    }
                }
                DataType::Utf8 => {
                    time_of_day_nanos_any(time_src.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let parts: Vec<Option<(i64, i64)>> = (0..rows)
            .map(|i| {
                let date_overrides = DateOverrides {
                    year: optional_i64_at(&ov[0], i),
                    month: optional_i64_at(&ov[1], i),
                    day: optional_i64_at(&ov[2], i),
                    week: optional_i64_at(&ov[3], i),
                    day_of_week: optional_i64_at(&ov[4], i),
                    ordinal_day: optional_i64_at(&ov[5], i),
                    quarter: optional_i64_at(&ov[6], i),
                    day_of_quarter: optional_i64_at(&ov[7], i),
                };
                let time_overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[8], i),
                    minute: optional_i64_at(&ov[9], i),
                    second: optional_i64_at(&ov[10], i),
                    millisecond: optional_i64_at(&ov[11], i),
                    microsecond: optional_i64_at(&ov[12], i),
                    nanosecond: optional_i64_at(&ov[13], i),
                };
                let date = project_date(base_date(i)?, &date_overrides)?;
                let time = project_localtime(base_time(i)?, &time_overrides)?;
                Some((date, time))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_localdatetime_struct(&parts),
        )))
    }
}

// ---------------------------------------------------------------------------
// cypher_localdatetime_truncate UDF
// ---------------------------------------------------------------------------

/// `localdatetime.truncate(unit, value, map)` (`Temporal9`): truncate `value` to
/// `unit` — for `day`-and-coarser units the date is truncated and the time zeroed
/// to midnight; for finer units (`hour`…`microsecond`) the date is kept and the
/// time-of-day floored — then the override `map` is applied. Args are `[value,
/// unit, year, month, day, week, dayOfWeek, ordinalDay, quarter, dayOfQuarter,
/// hour, minute, second, millisecond, microsecond, nanosecond]`. Returns the
/// `localdatetime` struct. (#920)
static CYPHER_LOCALDATETIME_TRUNCATE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherLocalDateTimeTruncate::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherLocalDateTimeTruncate {
    signature: Signature,
}

impl CypherLocalDateTimeTruncate {
    fn new() -> Self {
        Self {
            signature: Signature::any(16, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherLocalDateTimeTruncate {
    fn name(&self) -> &'static str {
        "cypher_localdatetime_truncate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(localdatetime_fields()))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one cohesive per-row truncation: typed source extraction, the \
                  date/time granularity split, and 14 component overrides"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            DateOverrides, LocalTimeOverrides, parse_date_or_datetime_prefix, project_date,
            project_localtime, time_of_day_nanos_any, truncate_date, truncate_time_nanos,
        };
        use datafusion::arrow::array::{
            Array, ArrayRef, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        // One `value` source feeds both the date and time components.
        let value: ArrayRef =
            if matches!(cols[0].data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_date_struct(cols[0].data_type())
                || is_localdatetime_struct(cols[0].data_type())
                || is_time_struct(cols[0].data_type())
                || is_datetime_struct(cols[0].data_type())
            {
                std::sync::Arc::clone(&cols[0])
            } else {
                cast(&cols[0], &DataType::Utf8).map_err(DataFusionError::from)?
            };
        let units_arr = cast(&cols[1], &DataType::Utf8).map_err(DataFusionError::from)?;
        let units = units_arr.as_any().downcast_ref::<StringArray>();
        let ov = cast_argument_arrays(&cols[2..16], &DataType::Int64)?;

        let base_date = |i: usize| -> Option<i64> {
            if value.is_null(i) {
                return None;
            }
            match value.data_type() {
                DataType::Struct(_) => {
                    let s = value.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(value.data_type()) {
                        date_struct_value(s, i)
                    } else {
                        Some(localdatetime_struct_parts(s, i)?.0)
                    }
                }
                DataType::Utf8 => parse_date_or_datetime_prefix(
                    value.as_any().downcast_ref::<StringArray>()?.value(i),
                ),
                _ => None,
            }
        };
        let base_time = |i: usize| -> Option<i64> {
            if value.is_null(i) {
                return None;
            }
            match value.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some(
                    value
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                ),
                DataType::Struct(_) => {
                    let s = value.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(value.data_type()) {
                        Some(0) // date-only → midnight
                    } else if is_time_struct(value.data_type()) {
                        Some(time_struct_parts(s, i)?.0)
                    } else {
                        Some(localdatetime_struct_parts(s, i)?.1)
                    }
                }
                DataType::Utf8 => {
                    time_of_day_nanos_any(value.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let parts: Vec<Option<(i64, i64)>> = (0..rows)
            .map(|i| {
                let u = units?;
                if u.is_null(i) {
                    return None;
                }
                // A `day`-and-coarser unit truncates the date and zeroes the time;
                // a finer unit keeps the date and floors the time-of-day.
                let (date, time) = match truncate_date(base_date(i)?, u.value(i)) {
                    Some(d) => (d, 0i64),
                    None => (
                        base_date(i)?,
                        truncate_time_nanos(base_time(i)?, u.value(i))?,
                    ),
                };
                let date_overrides = DateOverrides {
                    year: optional_i64_at(&ov[0], i),
                    month: optional_i64_at(&ov[1], i),
                    day: optional_i64_at(&ov[2], i),
                    week: optional_i64_at(&ov[3], i),
                    day_of_week: optional_i64_at(&ov[4], i),
                    ordinal_day: optional_i64_at(&ov[5], i),
                    quarter: optional_i64_at(&ov[6], i),
                    day_of_quarter: optional_i64_at(&ov[7], i),
                };
                let time_overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[8], i),
                    minute: optional_i64_at(&ov[9], i),
                    second: optional_i64_at(&ov[10], i),
                    millisecond: optional_i64_at(&ov[11], i),
                    microsecond: optional_i64_at(&ov[12], i),
                    nanosecond: optional_i64_at(&ov[13], i),
                };
                let date = project_date(date, &date_overrides)?;
                let time = project_localtime(time, &time_overrides)?;
                Some((date, time))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_localdatetime_struct(&parts),
        )))
    }
}

// ---------------------------------------------------------------------------
// cypher_time_project UDF + time-struct helpers
// ---------------------------------------------------------------------------

/// The Arrow fields of a `time` value — `Struct{time: Time64(Nanosecond),
/// offset: Int32}` (nanoseconds-of-day + zone offset in seconds).
fn time_fields() -> datafusion::arrow::datatypes::Fields {
    graphforge_storage::schemas::time_struct_fields()
}

/// True if `dt` is the `time` struct type (dispatches base extraction/rendering
/// without colliding with the `localdatetime` struct or user maps).
fn is_time_struct(dt: &DataType) -> bool {
    use datafusion::arrow::datatypes::TimeUnit;
    matches!(dt, DataType::Struct(fields)
        if fields.len() == 2
            && fields[0].name() == "time"
            && *fields[0].data_type() == DataType::Time64(TimeUnit::Nanosecond)
            && fields[1].name() == "offset"
            && *fields[1].data_type() == DataType::Int32)
}

/// Build a `time` struct array from per-row `(nanos_of_day, offset_seconds)`
/// (`None` ⇒ a null row).
fn build_time_struct(rows: &[Option<(i64, i32)>]) -> datafusion::arrow::array::StructArray {
    use datafusion::arrow::array::{Int32Array, Time64NanosecondArray};
    use datafusion::arrow::buffer::NullBuffer;
    let nanos: Time64NanosecondArray = rows.iter().map(|r| r.map(|(n, _)| n)).collect();
    let offset: Int32Array = rows.iter().map(|r| r.map(|(_, o)| o)).collect();
    let nulls = rows.iter().map(Option::is_some).collect::<NullBuffer>();
    datafusion::arrow::array::StructArray::new(
        time_fields(),
        vec![std::sync::Arc::new(nanos), std::sync::Arc::new(offset)],
        Some(nulls),
    )
}

/// A `time` scalar (`None` ⇒ a null value).
fn time_scalar(parts: Option<(i64, i32)>) -> ScalarValue {
    ScalarValue::Struct(std::sync::Arc::new(build_time_struct(&[parts])))
}

/// Extract `(nanos_of_day, offset_seconds)` from a `time` struct array at row
/// `i` (`None` for a null row).
fn time_struct_parts(arr: &datafusion::arrow::array::StructArray, i: usize) -> Option<(i64, i32)> {
    use datafusion::arrow::array::{Array, Int32Array, Time64NanosecondArray};
    if arr.is_null(i) {
        return None;
    }
    let t = arr
        .column(0)
        .as_any()
        .downcast_ref::<Time64NanosecondArray>()?;
    let o = arr.column(1).as_any().downcast_ref::<Int32Array>()?;
    (!t.is_null(i) && !o.is_null(i)).then(|| (t.value(i), o.value(i)))
}

/// `time`-from-value projection (`Temporal3`). Args are `[base, hour, minute,
/// second, millisecond, microsecond, nanosecond, timezone]`. `base` is a
/// `Time64`/`time`-struct/`localdatetime`-struct/temporal-string (its time-of-day
/// and, for `time`/`datetime` bases, its offset are read); the six integer
/// overrides adjust the time-of-day; `timezone` (a `+HH:MM`/`Z` string, or null)
/// attaches a zone — shifting the wall-clock time if the base already had one.
/// Returns the `time` struct. (ADR 0009)
static CYPHER_TIME_PROJECT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherTimeProject::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherTimeProject {
    signature: Signature,
}

impl CypherTimeProject {
    fn new() -> Self {
        Self {
            signature: Signature::any(8, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherTimeProject {
    fn name(&self) -> &'static str {
        "cypher_time_project"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(time_fields()))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            LocalTimeOverrides, parse_offset_seconds, project_localtime, project_time,
            time_of_day_with_offset,
        };
        use datafusion::arrow::array::{
            Array, ArrayRef, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let base: ArrayRef =
            if matches!(cols[0].data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_time_struct(cols[0].data_type())
                || is_localdatetime_struct(cols[0].data_type())
                || is_datetime_struct(cols[0].data_type())
            {
                std::sync::Arc::clone(&cols[0])
            } else {
                cast(&cols[0], &DataType::Utf8).map_err(DataFusionError::from)?
            };
        let ov = cast_argument_arrays(&cols[1..7], &DataType::Int64)?;
        let tz_arr = cast(&cols[7], &DataType::Utf8).map_err(DataFusionError::from)?;
        let tz = tz_arr.as_any().downcast_ref::<StringArray>();

        // Base time-of-day + whether it carried an offset (`None` ⇒ attach a new
        // zone; `Some` ⇒ shift to preserve the instant).
        let base_parts = |i: usize| -> Option<(i64, Option<i32>)> {
            if base.is_null(i) {
                return None;
            }
            match base.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some((
                    base.as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                    None,
                )),
                DataType::Struct(_) => {
                    let s = base.as_any().downcast_ref::<StructArray>()?;
                    if is_time_struct(base.data_type()) {
                        let (n, o) = time_struct_parts(s, i)?;
                        Some((n, Some(o)))
                    } else if is_datetime_struct(base.data_type()) {
                        // A `datetime` carries its zone offset — keep it so a new
                        // zone shifts the instant (`time(datetime)`).
                        let (_, n, o, _) = datetime_struct_parts(s, i)?;
                        Some((n, Some(o)))
                    } else {
                        // localdatetime struct — its time-of-day, no offset.
                        Some((localdatetime_struct_parts(s, i)?.1, None))
                    }
                }
                DataType::Utf8 => {
                    time_of_day_with_offset(base.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let parts: Vec<Option<(i64, i32)>> = (0..rows)
            .map(|i| {
                let (base_nanos, base_offset) = base_parts(i)?;
                let overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[0], i),
                    minute: optional_i64_at(&ov[1], i),
                    second: optional_i64_at(&ov[2], i),
                    millisecond: optional_i64_at(&ov[3], i),
                    microsecond: optional_i64_at(&ov[4], i),
                    nanosecond: optional_i64_at(&ov[5], i),
                };
                let nanos = project_localtime(base_nanos, &overrides)?;
                // A `timezone` override (offset string) re-zones the value.
                let new_offset = match tz {
                    Some(a) if !a.is_null(i) => Some(parse_offset_seconds(a.value(i))?),
                    _ => None,
                };
                Some(project_time(nanos, base_offset, new_offset))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_time_struct(&parts),
        )))
    }
}

// ---------------------------------------------------------------------------
// cypher_time_truncate UDF
// ---------------------------------------------------------------------------

/// `time.truncate(unit, value, map)` (`Temporal9`): truncate `value`'s
/// time-of-day to `unit` (keeping its zone offset), then apply the override
/// `map`. Args are `[value, unit, hour, minute, second, millisecond,
/// microsecond, nanosecond, timezone]`. Returns the `time` struct. (#920)
static CYPHER_TIME_TRUNCATE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherTimeTruncate::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherTimeTruncate {
    signature: Signature,
}

impl CypherTimeTruncate {
    fn new() -> Self {
        Self {
            signature: Signature::any(9, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherTimeTruncate {
    fn name(&self) -> &'static str {
        "cypher_time_truncate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(time_fields()))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            LocalTimeOverrides, parse_offset_seconds, project_localtime, project_time,
            time_of_day_with_offset, truncate_time_nanos,
        };
        use datafusion::arrow::array::{
            Array, ArrayRef, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let base: ArrayRef =
            if matches!(cols[0].data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_time_struct(cols[0].data_type())
                || is_localdatetime_struct(cols[0].data_type())
                || is_datetime_struct(cols[0].data_type())
            {
                std::sync::Arc::clone(&cols[0])
            } else {
                cast(&cols[0], &DataType::Utf8).map_err(DataFusionError::from)?
            };
        let units_arr = cast(&cols[1], &DataType::Utf8).map_err(DataFusionError::from)?;
        let units = units_arr.as_any().downcast_ref::<StringArray>();
        let ov = cast_argument_arrays(&cols[2..8], &DataType::Int64)?;
        let tz_arr = cast(&cols[8], &DataType::Utf8).map_err(DataFusionError::from)?;
        let tz = tz_arr.as_any().downcast_ref::<StringArray>();

        let base_parts = |i: usize| -> Option<(i64, Option<i32>)> {
            if base.is_null(i) {
                return None;
            }
            match base.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some((
                    base.as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                    None,
                )),
                DataType::Struct(_) => {
                    let s = base.as_any().downcast_ref::<StructArray>()?;
                    if is_time_struct(base.data_type()) {
                        let (n, o) = time_struct_parts(s, i)?;
                        Some((n, Some(o)))
                    } else if is_datetime_struct(base.data_type()) {
                        let (_, n, o, _) = datetime_struct_parts(s, i)?;
                        Some((n, Some(o)))
                    } else {
                        Some((localdatetime_struct_parts(s, i)?.1, None))
                    }
                }
                DataType::Utf8 => {
                    time_of_day_with_offset(base.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let parts: Vec<Option<(i64, i32)>> = (0..rows)
            .map(|i| {
                let u = units?;
                if u.is_null(i) {
                    return None;
                }
                let (base_nanos, base_offset) = base_parts(i)?;
                let truncated = truncate_time_nanos(base_nanos, u.value(i))?;
                let overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[0], i),
                    minute: optional_i64_at(&ov[1], i),
                    second: optional_i64_at(&ov[2], i),
                    millisecond: optional_i64_at(&ov[3], i),
                    microsecond: optional_i64_at(&ov[4], i),
                    nanosecond: optional_i64_at(&ov[5], i),
                };
                let nanos = project_localtime(truncated, &overrides)?;
                let new_offset = match tz {
                    Some(a) if !a.is_null(i) => Some(parse_offset_seconds(a.value(i))?),
                    _ => None,
                };
                // A `timezone` override ATTACHES to the truncated wall-clock (the
                // instant is not shifted) — pass `None` as the base offset so
                // `project_time` attaches, mirroring datetime.truncate (#990).
                // (#1008, Temporal9 [5])
                let eff_offset = if new_offset.is_some() {
                    None
                } else {
                    base_offset
                };
                Some(project_time(nanos, eff_offset, new_offset))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_time_struct(&parts),
        )))
    }
}

// ---------------------------------------------------------------------------
// cypher_datetime_project UDF + datetime-struct helpers
// ---------------------------------------------------------------------------

/// One `datetime` row: `(date_days, nanos_of_day, offset_seconds, zone_label)`,
/// or `None` for a null value.
type DateTimeRow = Option<(i64, i64, i32, Option<String>)>;

/// The Arrow fields of a `datetime` value — `Struct{date: Int64, time:
/// Time64(Nanosecond), offset: Int32, zone: Utf8}` (the date = i64 days,
/// time-of-day, resolved zone offset in seconds, and an optional named-IANA-zone
/// label). (#1011)
fn datetime_fields() -> datafusion::arrow::datatypes::Fields {
    graphforge_storage::schemas::datetime_struct_fields()
}

/// True if `dt` is the `datetime` struct type.
fn is_datetime_struct(dt: &DataType) -> bool {
    use datafusion::arrow::datatypes::TimeUnit;
    matches!(dt, DataType::Struct(fields)
        if fields.len() == 4
            && fields[0].name() == "date" && *fields[0].data_type() == DataType::Int64
            && fields[1].name() == "time"
            && *fields[1].data_type() == DataType::Time64(TimeUnit::Nanosecond)
            && fields[2].name() == "offset" && *fields[2].data_type() == DataType::Int32
            && fields[3].name() == "zone" && *fields[3].data_type() == DataType::Utf8)
}

/// Build a `datetime` struct array from per-row `(date_days, nanos_of_day,
/// offset_seconds, zone_label)` (`None` ⇒ a null row).
fn build_datetime_struct(rows: &[DateTimeRow]) -> datafusion::arrow::array::StructArray {
    use datafusion::arrow::array::{Int32Array, Int64Array, StringArray, Time64NanosecondArray};
    use datafusion::arrow::buffer::NullBuffer;
    let days: Int64Array = rows.iter().map(|r| r.as_ref().map(|t| t.0)).collect();
    let nanos: Time64NanosecondArray = rows.iter().map(|r| r.as_ref().map(|t| t.1)).collect();
    let offset: Int32Array = rows.iter().map(|r| r.as_ref().map(|t| t.2)).collect();
    // The zone field is empty (NOT null) when there is no named zone, so two
    // offset-only datetimes compare equal — `cypher_struct_eq` propagates null,
    // and a null=null field would make the whole equality null (Temporal7 [5]).
    let zone: StringArray = rows
        .iter()
        .map(|r| r.as_ref().map(|t| t.3.clone().unwrap_or_default()))
        .collect();
    let nulls = rows.iter().map(Option::is_some).collect::<NullBuffer>();
    datafusion::arrow::array::StructArray::new(
        datetime_fields(),
        vec![
            std::sync::Arc::new(days),
            std::sync::Arc::new(nanos),
            std::sync::Arc::new(offset),
            std::sync::Arc::new(zone),
        ],
        Some(nulls),
    )
}

/// A `datetime` scalar (`None` ⇒ a null value).
fn datetime_scalar(parts: DateTimeRow) -> ScalarValue {
    ScalarValue::Struct(std::sync::Arc::new(build_datetime_struct(&[parts])))
}

/// Extract `(date_days, nanos_of_day, offset_seconds, zone_label)` from a
/// `datetime` struct array at row `i` (`None` for a null row).
fn datetime_struct_parts(arr: &datafusion::arrow::array::StructArray, i: usize) -> DateTimeRow {
    use datafusion::arrow::array::{
        Array, Int32Array, Int64Array, StringArray, Time64NanosecondArray,
    };
    if arr.is_null(i) {
        return None;
    }
    let days = arr.column(0).as_any().downcast_ref::<Int64Array>()?;
    let nanos = arr
        .column(1)
        .as_any()
        .downcast_ref::<Time64NanosecondArray>()?;
    let offset = arr.column(2).as_any().downcast_ref::<Int32Array>()?;
    let zone = arr.column(3).as_any().downcast_ref::<StringArray>()?;
    if days.is_null(i) || nanos.is_null(i) || offset.is_null(i) {
        return None;
    }
    // An empty zone label means "no named zone" (offset-only datetime).
    let zone_label =
        (!zone.is_null(i) && !zone.value(i).is_empty()).then(|| zone.value(i).to_string());
    Some((days.value(i), nanos.value(i), offset.value(i), zone_label))
}

/// `datetime`-from-value projection (`Temporal3` [8]-[11]). Args are `[date_src,
/// time_src, year, month, day, week, dayOfWeek, ordinalDay, quarter,
/// dayOfQuarter, hour, minute, second, millisecond, microsecond, nanosecond,
/// timezone]`. The date/time sources are any temporal value/string (the time
/// source also carries the source offset + named zone); the 14 integer overrides
/// adjust the local date/time; `timezone` re-zones. Returns the `datetime`
/// struct. (ADR 0009)
static CYPHER_DATETIME_PROJECT: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDateTimeProject::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherDateTimeProject {
    signature: Signature,
}

impl CypherDateTimeProject {
    fn new() -> Self {
        Self {
            signature: Signature::any(17, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDateTimeProject {
    fn name(&self) -> &'static str {
        "cypher_datetime_project"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(datetime_fields()))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one cohesive per-row projection: typed date/time/zone source \
                  extraction, 14 component overrides, and zone re-resolution"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            DateOverrides, LocalTimeOverrides, parse_date_or_datetime_prefix, project_date,
            project_datetime, project_localtime, time_offset_zone,
        };
        use datafusion::arrow::array::{
            Array, ArrayRef, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        let typed_or_utf8 = |a: &ArrayRef| -> datafusion::error::Result<ArrayRef> {
            if matches!(a.data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_date_struct(a.data_type())
                || is_localdatetime_struct(a.data_type())
                || is_time_struct(a.data_type())
                || is_datetime_struct(a.data_type())
            {
                Ok(std::sync::Arc::clone(a))
            } else {
                cast(a, &DataType::Utf8).map_err(DataFusionError::from)
            }
        };
        let date_src = typed_or_utf8(&cols[0])?;
        let time_src = typed_or_utf8(&cols[1])?;
        let ov = cast_argument_arrays(&cols[2..16], &DataType::Int64)?;
        let tz_arr = cast(&cols[16], &DataType::Utf8).map_err(DataFusionError::from)?;
        let tz = tz_arr.as_any().downcast_ref::<StringArray>();

        let base_date = |i: usize| -> Option<i64> {
            if date_src.is_null(i) {
                return Some(0); // a missing date defaults to the epoch (day 0)
            }
            match date_src.data_type() {
                DataType::Struct(_) => {
                    let s = date_src.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(date_src.data_type()) {
                        date_struct_value(s, i)
                    } else if is_datetime_struct(date_src.data_type()) {
                        Some(datetime_struct_parts(s, i)?.0)
                    } else {
                        Some(localdatetime_struct_parts(s, i)?.0)
                    }
                }
                DataType::Utf8 => parse_date_or_datetime_prefix(
                    date_src.as_any().downcast_ref::<StringArray>()?.value(i),
                ),
                _ => None,
            }
        };
        // Base time-of-day plus the source's offset and named zone (if any).
        let base_time = |i: usize| -> Option<(i64, Option<i32>, Option<String>)> {
            if time_src.is_null(i) {
                return Some((0, None, None));
            }
            match time_src.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some((
                    time_src
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                    None,
                    None,
                )),
                DataType::Struct(_) => {
                    let s = time_src.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(time_src.data_type()) {
                        // A bare date carries no time-of-day (matches the pre-#1011
                        // `Date32` fall-through: `datetime(date(…))` → null).
                        None
                    } else if is_datetime_struct(time_src.data_type()) {
                        let (_, n, o, z) = datetime_struct_parts(s, i)?;
                        Some((n, Some(o), z))
                    } else if is_time_struct(time_src.data_type()) {
                        let (n, o) = time_struct_parts(s, i)?;
                        Some((n, Some(o), None))
                    } else {
                        Some((localdatetime_struct_parts(s, i)?.1, None, None))
                    }
                }
                DataType::Utf8 => {
                    time_offset_zone(time_src.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let parts: Vec<DateTimeRow> = (0..rows)
            .map(|i| {
                let date_overrides = DateOverrides {
                    year: optional_i64_at(&ov[0], i),
                    month: optional_i64_at(&ov[1], i),
                    day: optional_i64_at(&ov[2], i),
                    week: optional_i64_at(&ov[3], i),
                    day_of_week: optional_i64_at(&ov[4], i),
                    ordinal_day: optional_i64_at(&ov[5], i),
                    quarter: optional_i64_at(&ov[6], i),
                    day_of_quarter: optional_i64_at(&ov[7], i),
                };
                let time_overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[8], i),
                    minute: optional_i64_at(&ov[9], i),
                    second: optional_i64_at(&ov[10], i),
                    millisecond: optional_i64_at(&ov[11], i),
                    microsecond: optional_i64_at(&ov[12], i),
                    nanosecond: optional_i64_at(&ov[13], i),
                };
                let (base_nanos, src_offset, src_zone) = base_time(i)?;
                let date = project_date(base_date(i)?, &date_overrides)?;
                let nanos = project_localtime(base_nanos, &time_overrides)?;
                let new_tz = tz.and_then(|a| (!a.is_null(i)).then(|| a.value(i)));
                let (date, nanos, offset, zone) =
                    project_datetime(date, nanos, src_offset, src_zone.as_deref(), new_tz)?;
                Some((date, nanos, offset, zone))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_datetime_struct(&parts),
        )))
    }
}

// ---------------------------------------------------------------------------
// cypher_datetime_truncate UDF
// ---------------------------------------------------------------------------

/// `datetime.truncate(unit, value, map)` (`Temporal9`): truncate `value` to
/// `unit` — date for `day`-and-coarser units (time zeroed), time-of-day for finer
/// units — keeping the source zone, then apply the override `map` and optional
/// `timezone`. Args are `[value, unit, year, month, day, week, dayOfWeek,
/// ordinalDay, quarter, dayOfQuarter, hour, minute, second, millisecond,
/// microsecond, nanosecond, timezone]`. Returns the `datetime` struct. (#920)
static CYPHER_DATETIME_TRUNCATE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDateTimeTruncate::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherDateTimeTruncate {
    signature: Signature,
}

impl CypherDateTimeTruncate {
    fn new() -> Self {
        Self {
            signature: Signature::any(17, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDateTimeTruncate {
    fn name(&self) -> &'static str {
        "cypher_datetime_truncate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(datetime_fields()))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one cohesive per-row truncation: typed source extraction, the \
                  date/time granularity split, 14 overrides, and zone re-resolution"
    )]
    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            DateOverrides, LocalTimeOverrides, parse_date_or_datetime_prefix, project_date,
            project_datetime, project_localtime, time_offset_zone, truncate_date,
            truncate_time_nanos,
        };
        use datafusion::arrow::array::{
            Array, ArrayRef, StringArray, StructArray, Time64NanosecondArray,
        };
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        // One `value` source feeds both the date and time components.
        let value: ArrayRef =
            if matches!(cols[0].data_type(), DataType::Time64(TimeUnit::Nanosecond))
                || is_date_struct(cols[0].data_type())
                || is_localdatetime_struct(cols[0].data_type())
                || is_time_struct(cols[0].data_type())
                || is_datetime_struct(cols[0].data_type())
            {
                std::sync::Arc::clone(&cols[0])
            } else {
                cast(&cols[0], &DataType::Utf8).map_err(DataFusionError::from)?
            };
        let units_arr = cast(&cols[1], &DataType::Utf8).map_err(DataFusionError::from)?;
        let units = units_arr.as_any().downcast_ref::<StringArray>();
        let ov = cast_argument_arrays(&cols[2..16], &DataType::Int64)?;
        let tz_arr = cast(&cols[16], &DataType::Utf8).map_err(DataFusionError::from)?;
        let tz = tz_arr.as_any().downcast_ref::<StringArray>();

        let base_date = |i: usize| -> Option<i64> {
            if value.is_null(i) {
                return None;
            }
            match value.data_type() {
                DataType::Struct(_) => {
                    let s = value.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(value.data_type()) {
                        date_struct_value(s, i)
                    } else if is_datetime_struct(value.data_type()) {
                        Some(datetime_struct_parts(s, i)?.0)
                    } else {
                        Some(localdatetime_struct_parts(s, i)?.0)
                    }
                }
                DataType::Utf8 => parse_date_or_datetime_prefix(
                    value.as_any().downcast_ref::<StringArray>()?.value(i),
                ),
                _ => None,
            }
        };
        // Base time-of-day plus the source's offset and named zone (if any).
        let base_time = |i: usize| -> Option<(i64, Option<i32>, Option<String>)> {
            if value.is_null(i) {
                return None;
            }
            match value.data_type() {
                DataType::Time64(TimeUnit::Nanosecond) => Some((
                    value
                        .as_any()
                        .downcast_ref::<Time64NanosecondArray>()?
                        .value(i),
                    None,
                    None,
                )),
                DataType::Struct(_) => {
                    let s = value.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(value.data_type()) {
                        Some((0, None, None)) // date-only → midnight
                    } else if is_datetime_struct(value.data_type()) {
                        let (_, n, o, z) = datetime_struct_parts(s, i)?;
                        Some((n, Some(o), z))
                    } else if is_time_struct(value.data_type()) {
                        let (n, o) = time_struct_parts(s, i)?;
                        Some((n, Some(o), None))
                    } else {
                        Some((localdatetime_struct_parts(s, i)?.1, None, None))
                    }
                }
                DataType::Utf8 => {
                    time_offset_zone(value.as_any().downcast_ref::<StringArray>()?.value(i))
                }
                _ => None,
            }
        };

        let parts: Vec<DateTimeRow> = (0..rows)
            .map(|i| {
                let u = units?;
                if u.is_null(i) {
                    return None;
                }
                let (bt_nanos, src_offset, src_zone) = base_time(i)?;
                // A `day`-and-coarser unit truncates the date and zeroes the time;
                // a finer unit keeps the date and floors the time-of-day.
                let (date0, nanos0) = match truncate_date(base_date(i)?, u.value(i)) {
                    Some(d) => (d, 0i64),
                    None => (base_date(i)?, truncate_time_nanos(bt_nanos, u.value(i))?),
                };
                let date_overrides = DateOverrides {
                    year: optional_i64_at(&ov[0], i),
                    month: optional_i64_at(&ov[1], i),
                    day: optional_i64_at(&ov[2], i),
                    week: optional_i64_at(&ov[3], i),
                    day_of_week: optional_i64_at(&ov[4], i),
                    ordinal_day: optional_i64_at(&ov[5], i),
                    quarter: optional_i64_at(&ov[6], i),
                    day_of_quarter: optional_i64_at(&ov[7], i),
                };
                let time_overrides = LocalTimeOverrides {
                    hour: optional_i64_at(&ov[8], i),
                    minute: optional_i64_at(&ov[9], i),
                    second: optional_i64_at(&ov[10], i),
                    millisecond: optional_i64_at(&ov[11], i),
                    microsecond: optional_i64_at(&ov[12], i),
                    nanosecond: optional_i64_at(&ov[13], i),
                };
                let date = project_date(date0, &date_overrides)?;
                let nanos = project_localtime(nanos0, &time_overrides)?;
                let new_tz = tz.and_then(|a| (!a.is_null(i)).then(|| a.value(i)));
                // Truncation is WALL-CLOCK preserving: a `{timezone: …}` override
                // ATTACHES the zone to the truncated local time (midnight stays
                // midnight), it does not re-express the source instant. So drop
                // the source offset when a new zone is given, forcing
                // `project_datetime`'s attach path instead of an instant shift
                // (#920 — otherwise `truncate(…, {timezone: 'Europe/Stockholm'})`
                // shifted midnight by the source offset).
                let src_offset = if new_tz.is_some() { None } else { src_offset };
                let (date, nanos, offset, zone) =
                    project_datetime(date, nanos, src_offset, src_zone.as_deref(), new_tz)?;
                Some((date, nanos, offset, zone))
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_datetime_struct(&parts),
        )))
    }
}

// ---------------------------------------------------------------------------
// cypher_to_string UDF
// ---------------------------------------------------------------------------

/// `toString(x)`: a typed temporal value renders to its canonical openCypher
/// string (a plain `cast` to `Utf8` would emit a fixed, untrimmed form for
/// `Time64`, and outright fail for the temporal structs). Handles `Date32`/
/// `Time64`/`localdatetime`/`time`; every other type — including `datetime`,
/// which is still a `Utf8` value until its migration — falls back to the same
/// `Utf8` cast as before, so non-temporal `toString` behaviour is unchanged.
/// (ADR 0009)
static CYPHER_TO_STRING: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherToString::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherToString {
    signature: Signature,
}

impl CypherToString {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherToString {
    fn name(&self) -> &'static str {
        "cypher_to_string"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            format_date, render_localdatetime, render_localtime_nanos, render_time_value,
        };
        use datafusion::arrow::array::{Array, StringArray, StructArray, Time64NanosecondArray};
        use datafusion::arrow::compute::cast;
        use datafusion::arrow::datatypes::TimeUnit;

        let arr = args.args[0].to_array(args.number_rows)?;
        let rows = arr.len();
        // Render one canonical string per row from a closure, preserving nulls.
        let render = |f: &dyn Fn(usize) -> Option<String>| -> ColumnarValue {
            let out: StringArray = (0..rows)
                .map(|i| if arr.is_null(i) { None } else { f(i) })
                .collect();
            ColumnarValue::Array(std::sync::Arc::new(out))
        };

        let result = match arr.data_type() {
            DataType::Struct(_) if is_date_struct(arr.data_type()) => {
                let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
                render(&|i| date_struct_value(s, i).map(format_date))
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                let a = arr
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap();
                render(&|i| Some(render_localtime_nanos(a.value(i))))
            }
            DataType::Struct(_) if is_localdatetime_struct(arr.data_type()) => {
                let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
                render(&|i| {
                    localdatetime_struct_parts(s, i).map(|(d, n)| render_localdatetime(d, n))
                })
            }
            DataType::Struct(_) if is_time_struct(arr.data_type()) => {
                let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
                render(&|i| time_struct_parts(s, i).map(|(n, o)| render_time_value(n, o)))
            }
            DataType::Struct(_) if is_datetime_struct(arr.data_type()) => {
                let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
                render(&|i| {
                    datetime_struct_parts(s, i).map(|(d, n, o, z)| {
                        crate::temporal::render_datetime_value(d, n, o, z.as_deref())
                    })
                })
            }
            DataType::Struct(_) if is_duration_struct(arr.data_type()) => {
                let s = arr.as_any().downcast_ref::<StructArray>().unwrap();
                render(&|i| {
                    duration_struct_parts(s, i).map(|d| crate::temporal::render_duration_value(&d))
                })
            }
            DataType::Struct(_) if is_het_struct_type(Some(arr.data_type())) => {
                let out: datafusion::error::Result<StringArray> = (0..rows)
                    .map(|i| {
                        let value = decoded_scalar_at(&arr, i)?;
                        to_cypher_string(&value)
                    })
                    .collect();
                ColumnarValue::Array(std::sync::Arc::new(out?))
            }
            // Non-temporal (or non-temporal struct): the original `Utf8` cast.
            _ => ColumnarValue::Array(
                cast(&arr, &DataType::Utf8).map_err(datafusion::error::DataFusionError::from)?,
            ),
        };
        Ok(result)
    }
}

/// Integer scalar as `i128` for exact cross-width integer equality/comparison.
fn scalar_as_i128(v: &ScalarValue) -> Option<i128> {
    match v {
        ScalarValue::Int8(Some(n)) => Some(i128::from(*n)),
        ScalarValue::Int16(Some(n)) => Some(i128::from(*n)),
        ScalarValue::Int32(Some(n)) => Some(i128::from(*n)),
        ScalarValue::Int64(Some(n)) => Some(i128::from(*n)),
        ScalarValue::UInt8(Some(n)) => Some(i128::from(*n)),
        ScalarValue::UInt16(Some(n)) => Some(i128::from(*n)),
        ScalarValue::UInt32(Some(n)) => Some(i128::from(*n)),
        ScalarValue::UInt64(Some(n)) => Some(i128::from(*n)),
        _ => None,
    }
}

/// Numeric scalar as `f64` for cross integer/float equality and ordering.
fn scalar_as_f64(v: &ScalarValue) -> Option<f64> {
    #[allow(
        clippy::cast_precision_loss,
        reason = "only used for mixed integer/float numeric semantics; pure integers use i128"
    )]
    match v {
        ScalarValue::Int8(Some(n)) => Some(f64::from(*n)),
        ScalarValue::Int16(Some(n)) => Some(f64::from(*n)),
        ScalarValue::Int32(Some(n)) => Some(f64::from(*n)),
        ScalarValue::Int64(Some(n)) => Some(*n as f64),
        ScalarValue::UInt8(Some(n)) => Some(f64::from(*n)),
        ScalarValue::UInt16(Some(n)) => Some(f64::from(*n)),
        ScalarValue::UInt32(Some(n)) => Some(f64::from(*n)),
        ScalarValue::UInt64(Some(n)) => Some(*n as f64),
        ScalarValue::Float32(Some(f)) => Some(f64::from(*f)),
        ScalarValue::Float64(Some(f)) => Some(*f),
        _ => None,
    }
}

/// Three-valued Cypher equality of two list element arrays.
fn cypher_seq_eq(
    a: &datafusion::arrow::array::ArrayRef,
    b: &datafusion::arrow::array::ArrayRef,
) -> Option<bool> {
    if a.len() != b.len() {
        return Some(false); // length mismatch ⇒ false, even with nulls present
    }
    let mut saw_null = false;
    for i in 0..a.len() {
        let av = ScalarValue::try_from_array(a, i).ok()?;
        let bv = ScalarValue::try_from_array(b, i).ok()?;
        match cypher_value_eq(&av, &bv) {
            Some(false) => return Some(false),
            None => saw_null = true,
            Some(true) => {}
        }
    }
    if saw_null { None } else { Some(true) }
}

/// Three-valued Cypher equality of two map (`Struct`) values.
fn cypher_struct_eq(
    a: &datafusion::arrow::array::StructArray,
    b: &datafusion::arrow::array::StructArray,
) -> Option<bool> {
    // Key sets must match — a key with a `null` value still counts as present.
    let mut ka: Vec<&str> = a.fields().iter().map(|f| f.name().as_str()).collect();
    let mut kb: Vec<&str> = b.fields().iter().map(|f| f.name().as_str()).collect();
    ka.sort_unstable();
    kb.sort_unstable();
    if ka != kb {
        return Some(false);
    }
    let mut saw_null = false;
    for key in ka {
        let av = ScalarValue::try_from_array(a.column_by_name(key)?, 0).ok()?;
        let bv = ScalarValue::try_from_array(b.column_by_name(key)?, 0).ok()?;
        match cypher_value_eq(&av, &bv) {
            Some(false) => return Some(false),
            None => saw_null = true,
            Some(true) => {}
        }
    }
    if saw_null { None } else { Some(true) }
}

// ---------------------------------------------------------------------------
// cypher_date_truncate UDF
// ---------------------------------------------------------------------------

/// `date.truncate(unit, value, map)` (`Temporal9`): truncate `value`'s date to
/// `unit`, then apply the override `map` (same fields as projection). Args are
/// `[value, unit, year, month, day, week, dayOfWeek, ordinalDay, quarter,
/// dayOfQuarter]` — `value` is a `Date32` or ISO date/datetime string, `unit` a
/// string, the eight overrides nullable integers. Returns `Date32`. (#920)
static CYPHER_DATE_TRUNCATE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(CypherDateTruncate::new()));

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherDateTruncate {
    signature: Signature,
}

impl CypherDateTruncate {
    fn new() -> Self {
        Self {
            signature: Signature::any(10, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CypherDateTruncate {
    fn name(&self) -> &'static str {
        "cypher_date_truncate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(DataType::Struct(
            graphforge_storage::schemas::date_struct_fields(),
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        use crate::temporal::{
            DateOverrides, parse_date_or_datetime_prefix, project_date, truncate_date,
        };
        use datafusion::arrow::array::{Array, ArrayRef, StringArray, StructArray};
        use datafusion::arrow::compute::cast;
        use datafusion::error::DataFusionError;

        let rows = args.number_rows;
        let cols = udf_argument_arrays(&args)?;
        // DataFusion emits `Utf8View` for string *columns* by default (string
        // literals stay `Utf8`), and our downcasts target `StringArray` — cast
        // string inputs to `Utf8` so a column-typed value/unit isn't silently
        // nulled. A `date`/`localdatetime`/`time`/`datetime` struct is taken directly.
        let value: ArrayRef = if is_date_struct(cols[0].data_type())
            || is_localdatetime_struct(cols[0].data_type())
            || is_time_struct(cols[0].data_type())
            || is_datetime_struct(cols[0].data_type())
        {
            std::sync::Arc::clone(&cols[0])
        } else {
            cast(&cols[0], &DataType::Utf8).map_err(DataFusionError::from)?
        };
        let units_arr = cast(&cols[1], &DataType::Utf8).map_err(DataFusionError::from)?;
        let units = units_arr.as_any().downcast_ref::<StringArray>();
        let ov = cast_argument_arrays(&cols[2..10], &DataType::Int64)?;

        let base_date = |i: usize| -> Option<i64> {
            if value.is_null(i) {
                return None;
            }
            match value.data_type() {
                DataType::Struct(_) => {
                    let s = value.as_any().downcast_ref::<StructArray>()?;
                    if is_date_struct(value.data_type()) {
                        date_struct_value(s, i)
                    } else {
                        // A `localdatetime`/`datetime` value — truncate its date.
                        Some(localdatetime_struct_parts(s, i)?.0)
                    }
                }
                DataType::Utf8 => parse_date_or_datetime_prefix(
                    value.as_any().downcast_ref::<StringArray>()?.value(i),
                ),
                _ => None,
            }
        };

        let out: Vec<Option<i64>> = (0..rows)
            .map(|i| {
                let u = units?;
                if u.is_null(i) {
                    return None;
                }
                let truncated = truncate_date(base_date(i)?, u.value(i))?;
                let overrides = DateOverrides {
                    year: optional_i64_at(&ov[0], i),
                    month: optional_i64_at(&ov[1], i),
                    day: optional_i64_at(&ov[2], i),
                    week: optional_i64_at(&ov[3], i),
                    day_of_week: optional_i64_at(&ov[4], i),
                    ordinal_day: optional_i64_at(&ov[5], i),
                    quarter: optional_i64_at(&ov[6], i),
                    day_of_quarter: optional_i64_at(&ov[7], i),
                };
                project_date(truncated, &overrides)
            })
            .collect();
        Ok(ColumnarValue::Array(std::sync::Arc::new(
            build_date_struct(&out),
        )))
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
/// element shape and leaves room to add node properties/labels later without
/// changing the container kind.
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
#[derive(Debug, PartialEq, Eq, Hash)]
struct PathNodeHydration {
    /// The project directory the invoke reads node/property files from.
    dir: std::path::PathBuf,
    /// `type_id → label` (ontology + runtime catalog), sorted by id.
    labels_by_type: Vec<(u32, String)>,
    /// The `properties/<stem>.parquet` stems whose fields form the union,
    /// sorted — the invoke coalesces each node's values across them.
    prop_stems: Vec<String>,
    /// The full element fields: `node_uuid`, `labels`, then the property union.
    fields: datafusion::arrow::datatypes::Fields,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CypherPathNodes {
    signature: Signature,
    hydrate: Option<PathNodeHydration>,
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
        use datafusion::arrow::array::{
            Array, ArrayRef, FixedSizeBinaryArray, ListArray, StructArray, new_empty_array,
        };
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
        use datafusion::arrow::datatypes::Field;
        use datafusion::common::cast::as_list_array;
        use datafusion::error::DataFusionError;
        use std::sync::Arc;

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
        let fields = self.element_fields();
        let mut children: Vec<ArrayRef> = vec![uuid_child];
        if let Some(h) = self.hydrate.as_ref() {
            children.extend(hydrate_path_node_children(h, &flat)?);
        }

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
}

/// Build the `labels` + property-union children for hydrated path-node
/// elements (#1024), one entry per flattened node uuid.
fn hydrate_path_node_children(
    h: &PathNodeHydration,
    flat: &[[u8; 16]],
) -> datafusion::error::Result<Vec<datafusion::arrow::array::ArrayRef>> {
    let mut children = vec![path_node_labels_child(h, flat)?];
    children.extend(path_node_prop_children(h, flat)?);
    Ok(children)
}

/// A `FixedSizeBinary(16)` column by name, for the hydration readers.
fn hydration_fsb16(
    b: &datafusion::arrow::array::RecordBatch,
    name: &str,
) -> datafusion::error::Result<datafusion::arrow::array::FixedSizeBinaryArray> {
    use datafusion::arrow::array::FixedSizeBinaryArray;
    b.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>().cloned())
        .filter(|a| a.value_length() == 16)
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "cypher_path_nodes: no FixedSizeBinary(16) {name} column"
            ))
        })
}

/// The `labels` child (#1024 / #705): full `List<Utf8>` per flattened node from
/// authoritative `topology/nodes.parquet` `type_ids`, resolved through the
/// baked (id-sorted) catalog map — the same complete set `node_labels_list`
/// projects for direct node values. Unknown catalog ids are skipped; a missing
/// topology row keeps a single-null list element.
fn path_node_labels_child(
    h: &PathNodeHydration,
    flat: &[[u8; 16]],
) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef> {
    use datafusion::arrow::array::{Array, ListArray, ListBuilder, StringBuilder, UInt32Array};
    use datafusion::error::DataFusionError;
    use std::collections::HashMap;

    let exec_err = |m: String| DataFusionError::Execution(m);
    let node_batches =
        graphforge_storage::read_nodes(&h.dir).map_err(|e| exec_err(e.to_string()))?;
    let mut label_ids_of: HashMap<[u8; 16], Vec<u32>> = HashMap::new();
    for b in &node_batches {
        let uuids = hydration_fsb16(b, "node_uuid")?;
        let type_ids = b
            .column_by_name("type_ids")
            .and_then(|c| c.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| exec_err("cypher_path_nodes: no List type_ids column".into()))?;
        for r in 0..b.num_rows() {
            if uuids.is_null(r) || type_ids.is_null(r) {
                continue;
            }
            let mut u = [0u8; 16];
            u.copy_from_slice(uuids.value(r));
            let values = type_ids.value(r);
            let values = values
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    exec_err("cypher_path_nodes: type_ids values are not UInt32".into())
                })?;
            let mut ids = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                if !values.is_null(i) {
                    ids.push(values.value(i));
                }
            }
            label_ids_of.insert(u, ids);
        }
    }
    let mut labels_b = ListBuilder::new(StringBuilder::new());
    for u in flat {
        if let Some(ids) = label_ids_of.get(u) {
            for id in ids {
                if let Ok(i) = h.labels_by_type.binary_search_by_key(id, |(tid, _)| *tid) {
                    labels_b.values().append_value(&h.labels_by_type[i].1);
                }
            }
            labels_b.append(true);
        } else {
            labels_b.values().append_null();
            labels_b.append(true);
        }
    }
    Ok(std::sync::Arc::new(labels_b.finish()))
}

/// The property-union children (#1024), one array per union field: each node
/// takes values from the stem file owning its `node_uuid`, coalesced across
/// files — NULL where the owning file lacks the column or the row (LEFT-join
/// parity with `join_node_properties`).
fn path_node_prop_children(
    h: &PathNodeHydration,
    flat: &[[u8; 16]],
) -> datafusion::error::Result<Vec<datafusion::arrow::array::ArrayRef>> {
    use datafusion::arrow::array::{Array, ArrayRef, UInt32Array, new_null_array};
    use datafusion::arrow::compute::kernels::zip::zip;
    use datafusion::arrow::compute::{concat_batches, is_not_null, take};
    use datafusion::error::DataFusionError;
    use std::collections::HashMap;

    let exec_err = |m: String| DataFusionError::Execution(m);

    // One concatenated property batch per stem, then uuid → (owning batch,
    // row). A node belongs to one property file, so first-wins is a no-op for
    // well-formed data.
    let mut batches = Vec::with_capacity(h.prop_stems.len());
    for stem in &h.prop_stems {
        let bs = graphforge_storage::read_properties(&h.dir, stem)
            .map_err(|e| exec_err(e.to_string()))?;
        if let Some(first) = bs.first() {
            batches
                .push(concat_batches(&first.schema(), &bs).map_err(|e| exec_err(e.to_string()))?);
        }
    }
    let mut uuid_to_loc: HashMap<[u8; 16], (usize, u32)> = HashMap::new();
    for (bi, b) in batches.iter().enumerate() {
        let key = hydration_fsb16(b, "node_uuid")?;
        for r in 0..key.len() {
            if key.is_null(r) {
                continue;
            }
            let mut u = [0u8; 16];
            u.copy_from_slice(key.value(r));
            uuid_to_loc.entry(u).or_insert((
                bi,
                u32::try_from(r).map_err(|_| exec_err(format!("property row {r} exceeds u32")))?,
            ));
        }
    }
    let take_by_batch: Vec<UInt32Array> = (0..batches.len())
        .map(|bi| {
            flat.iter()
                .map(|u| match uuid_to_loc.get(u) {
                    Some(&(owner, row)) if owner == bi => Some(row),
                    _ => None,
                })
                .collect()
        })
        .collect();

    let mut children = Vec::with_capacity(h.fields.len().saturating_sub(2));
    for field in h.fields.iter().skip(2) {
        let mut child: ArrayRef = new_null_array(field.data_type(), flat.len());
        for (bi, b) in batches.iter().enumerate() {
            // Field in the union but absent in this stem's file — this stem's
            // nodes contribute NULLs.
            let Some(col) = b.column_by_name(field.name()) else {
                continue;
            };
            let taken = take(col, &take_by_batch[bi], None).map_err(|e| exec_err(e.to_string()))?;
            child = if batches.len() == 1 {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use graphforge_core::PropId;
    use graphforge_ir::expr::{BinaryOpKind, IrExpr, IrLiteral, UnaryOpKind};
    use graphforge_ir::{ExprArena, VarId};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static VOLATILE_CALLS: AtomicUsize = AtomicUsize::new(0);
    static VOLATILE_ROWS: AtomicUsize = AtomicUsize::new(0);

    fn invoke_test_udf<U: ScalarUDFImpl>(
        udf: &U,
        values: Vec<ScalarValue>,
    ) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef> {
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        let types = values
            .iter()
            .map(ScalarValue::data_type)
            .collect::<Vec<_>>();
        let return_type = udf.return_type(&types)?;
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args: values.into_iter().map(ColumnarValue::Scalar).collect(),
            arg_fields: types
                .iter()
                .enumerate()
                .map(|(index, data_type)| {
                    Arc::new(Field::new(format!("arg_{index}"), data_type.clone(), true))
                })
                .collect(),
            number_rows: 1,
            return_field: Arc::new(Field::new("result", return_type.clone(), true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        let array = match result {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(value) => value.to_array_of_size(1)?,
        };
        assert_eq!(array.data_type(), &return_type);
        Ok(array)
    }

    fn invoke_test_udf_with_return_type<U: ScalarUDFImpl>(
        udf: &U,
        values: Vec<ScalarValue>,
        return_type: DataType,
    ) -> datafusion::error::Result<ColumnarValue> {
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        let types = values
            .iter()
            .map(ScalarValue::data_type)
            .collect::<Vec<_>>();
        udf.invoke_with_args(ScalarFunctionArgs {
            args: values.into_iter().map(ColumnarValue::Scalar).collect(),
            arg_fields: types
                .iter()
                .enumerate()
                .map(|(index, data_type)| {
                    Arc::new(Field::new(format!("arg_{index}"), data_type.clone(), true))
                })
                .collect(),
            number_rows: 1,
            return_field: Arc::new(Field::new("result", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct CountingVolatilePredicate {
        signature: Signature,
    }

    impl CountingVolatilePredicate {
        fn new() -> Self {
            Self {
                signature: Signature::nullary(Volatility::Volatile),
            }
        }
    }

    impl ScalarUDFImpl for CountingVolatilePredicate {
        fn name(&self) -> &'static str {
            "counting_volatile_predicate"
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
            VOLATILE_CALLS.fetch_add(1, Ordering::SeqCst);
            VOLATILE_ROWS.fetch_add(args.number_rows, Ordering::SeqCst);
            Ok(ColumnarValue::Array(std::sync::Arc::new(
                BooleanArray::from(vec![true; args.number_rows]),
            )))
        }
    }

    #[test]
    fn temporal_clock_fn_recognition() {
        // Every instant type × clock accessor is recognised (#920).
        for base in ["date", "localtime", "time", "localdatetime", "datetime"] {
            for clock in ["transaction", "statement", "realtime"] {
                assert!(is_temporal_clock_fn(&format!("{base}.{clock}")));
            }
        }
        // Case-insensitive (Cypher function names are).
        assert!(is_temporal_clock_fn("Date.Realtime"));
        assert!(is_temporal_clock_fn("DATETIME.TRANSACTION"));
        // Non-clock and non-temporal names are not.
        assert!(!is_temporal_clock_fn("datetime.truncate"));
        assert!(!is_temporal_clock_fn("duration.realtime")); // duration has no clock
        assert!(!is_temporal_clock_fn("date"));
        assert!(!is_temporal_clock_fn("foo.realtime"));

        // A null temporal arg lowers to a TYPED null, not generic Null.
        assert_eq!(
            temporal_null_scalar("date").data_type(),
            date_scalar(None).data_type()
        );
        assert_eq!(
            temporal_null_scalar("localtime"),
            ScalarValue::Time64Nanosecond(None)
        );
        assert_eq!(
            temporal_null_scalar("datetime.realtime").data_type(),
            datetime_scalar(None).data_type()
        );
        assert_eq!(temporal_null_scalar("unknown"), ScalarValue::Null);
    }

    #[test]
    fn temporal_struct_builders_and_extractors_preserve_values_and_nulls() {
        use crate::temporal::DurationValue;
        use datafusion::arrow::array::{ArrayRef, Int32Array, StructArray};
        use datafusion::arrow::datatypes::{Field, Fields};

        let dates = build_date_struct(&[Some(19_723), None]);
        assert_eq!(date_struct_value(&dates, 0), Some(19_723));
        assert_eq!(date_struct_value(&dates, 1), None);

        let local = build_localdatetime_struct(&[(Some((19_723, 45_000))), None]);
        assert_eq!(
            localdatetime_struct_parts(&local, 0),
            Some((19_723, 45_000))
        );
        assert_eq!(localdatetime_struct_parts(&local, 1), None);

        let duration = DurationValue {
            months: 14,
            days: -2,
            seconds: 90,
            nanos: 123,
        };
        let durations = build_duration_struct(&[Some(duration), None]);
        assert_eq!(duration_struct_parts(&durations, 0), Some(duration));
        assert_eq!(duration_struct_parts(&durations, 1), None);
        assert_eq!(
            dur_secs_nanos(-3, 7),
            DurationValue {
                months: 0,
                days: 0,
                seconds: -3,
                nanos: 7,
            }
        );
        assert_eq!(
            duration_value_to_ir(duration),
            IrLiteral::Duration {
                months: 14,
                days: -2,
                seconds: 90,
                nanos: 123,
            }
        );

        let times = build_time_struct(&[Some((86_399_000_000_000, -25_200)), None]);
        assert_eq!(
            time_struct_parts(&times, 0),
            Some((86_399_000_000_000, -25_200))
        );
        assert_eq!(time_struct_parts(&times, 1), None);

        let datetimes = build_datetime_struct(&[
            Some((19_723, 45_000, 3_600, Some("Europe/Paris".into()))),
            Some((19_724, 46_000, 0, None)),
            None,
        ]);
        assert_eq!(
            datetime_struct_parts(&datetimes, 0),
            Some((19_723, 45_000, 3_600, Some("Europe/Paris".into())))
        );
        assert_eq!(
            datetime_struct_parts(&datetimes, 1),
            Some((19_724, 46_000, 0, None))
        );
        assert_eq!(datetime_struct_parts(&datetimes, 2), None);

        // Extractors reject a structurally wrong child type without fabricating
        // a value. This exercises the defensive downcast path with valid Arrow.
        let wrong_fields = Fields::from(vec![Field::new("epoch_day", DataType::Int32, true)]);
        let wrong_children: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![Some(1)]))];
        let wrong_date = StructArray::new(wrong_fields, wrong_children, None);
        assert_eq!(date_struct_value(&wrong_date, 0), None);

        let overrides: ArrayRef = Arc::new(datafusion::arrow::array::Int64Array::from(vec![
            Some(8),
            None,
        ]));
        assert_eq!(optional_i64_at(&overrides, 0), Some(8));
        assert_eq!(optional_i64_at(&overrides, 1), None);
        let wrong_override: ArrayRef = Arc::new(Int32Array::from(vec![Some(8)]));
        assert_eq!(optional_i64_at(&wrong_override, 0), None);
    }

    #[test]
    fn temporal_project_and_truncate_udfs_execute_all_typed_families() {
        use datafusion::arrow::array::Array;

        let null_ints = |count| vec![ScalarValue::Int64(None); count];
        let assert_value = |array: datafusion::arrow::array::ArrayRef| {
            assert_eq!(array.len(), 1);
            assert!(!array.is_null(0));
        };

        let mut args = vec![ScalarValue::Utf8(Some("2024-02-29".into()))];
        args.extend(null_ints(8));
        assert_value(invoke_test_udf(&CypherDateProject::new(), args).unwrap());
        let mut null_args = vec![ScalarValue::Utf8(None)];
        null_args.extend(null_ints(8));
        let null_date = invoke_test_udf(&CypherDateProject::new(), null_args).unwrap();
        assert!(null_date.is_null(0));
        let mut typed_args = vec![date_scalar(Some(19_782))];
        typed_args.extend(null_ints(8));
        assert_value(invoke_test_udf(&CypherDateProject::new(), typed_args).unwrap());

        let mut args = vec![ScalarValue::Utf8(Some("12:34:56.123".into()))];
        args.extend(null_ints(6));
        assert_value(invoke_test_udf(&CypherLocalTimeProject::new(), args).unwrap());
        let mut typed_args = vec![ScalarValue::Time64Nanosecond(Some(45_296_123_000_000))];
        typed_args.extend(null_ints(6));
        assert_value(invoke_test_udf(&CypherLocalTimeProject::new(), typed_args).unwrap());

        let mut args = vec![
            ScalarValue::Utf8(Some("12:34:56.123".into())),
            ScalarValue::Utf8(Some("second".into())),
        ];
        args.extend(null_ints(6));
        assert_value(invoke_test_udf(&CypherLocalTimeTruncate::new(), args).unwrap());

        let mut args = vec![
            ScalarValue::Utf8(Some("2024-02-29".into())),
            ScalarValue::Utf8(Some("12:34:56.123".into())),
        ];
        args.extend(null_ints(14));
        assert_value(invoke_test_udf(&CypherLocalDateTimeProject::new(), args).unwrap());
        let mut typed_args = vec![
            date_scalar(Some(19_782)),
            ScalarValue::Time64Nanosecond(Some(45_296_123_000_000)),
        ];
        typed_args.extend(null_ints(14));
        assert_value(invoke_test_udf(&CypherLocalDateTimeProject::new(), typed_args).unwrap());

        let mut args = vec![
            ScalarValue::Utf8(Some("2024-02-29T12:34:56.123".into())),
            ScalarValue::Utf8(Some("day".into())),
        ];
        args.extend(null_ints(14));
        assert_value(invoke_test_udf(&CypherLocalDateTimeTruncate::new(), args).unwrap());

        let mut args = vec![ScalarValue::Utf8(Some("12:34:56+01:00".into()))];
        args.extend(null_ints(6));
        args.push(ScalarValue::Utf8(None));
        assert_value(invoke_test_udf(&CypherTimeProject::new(), args).unwrap());
        let mut typed_args = vec![time_scalar(Some((45_296_000_000_000, 3_600)))];
        typed_args.extend(null_ints(6));
        typed_args.push(ScalarValue::Utf8(None));
        assert_value(invoke_test_udf(&CypherTimeProject::new(), typed_args).unwrap());

        let mut args = vec![
            ScalarValue::Utf8(Some("12:34:56+01:00".into())),
            ScalarValue::Utf8(Some("minute".into())),
        ];
        args.extend(null_ints(6));
        args.push(ScalarValue::Utf8(None));
        assert_value(invoke_test_udf(&CypherTimeTruncate::new(), args).unwrap());

        let mut args = vec![
            ScalarValue::Utf8(Some("2024-02-29".into())),
            ScalarValue::Utf8(Some("12:34:56+01:00".into())),
        ];
        args.extend(null_ints(14));
        args.push(ScalarValue::Utf8(None));
        assert_value(invoke_test_udf(&CypherDateTimeProject::new(), args).unwrap());
        let mut typed_args = vec![
            date_scalar(Some(19_782)),
            time_scalar(Some((45_296_000_000_000, 3_600))),
        ];
        typed_args.extend(null_ints(14));
        typed_args.push(ScalarValue::Utf8(None));
        assert_value(invoke_test_udf(&CypherDateTimeProject::new(), typed_args).unwrap());

        let mut args = vec![
            ScalarValue::Utf8(Some("2024-02-29T12:34:56+01:00".into())),
            ScalarValue::Utf8(Some("hour".into())),
        ];
        args.extend(null_ints(14));
        args.push(ScalarValue::Utf8(None));
        assert_value(invoke_test_udf(&CypherDateTimeTruncate::new(), args).unwrap());

        let mut args = vec![
            ScalarValue::Utf8(Some("2024-02-29".into())),
            ScalarValue::Utf8(Some("month".into())),
        ];
        args.extend(null_ints(8));
        assert_value(invoke_test_udf(&CypherDateTruncate::new(), args).unwrap());
    }

    #[test]
    fn quantifier_three_valued_reduce() {
        use datafusion::arrow::array::BooleanArray;
        use graphforge_ir::QuantifierKind::{All, Any, None, Single};
        let b = |v: Vec<Option<bool>>| BooleanArray::from(v);
        let r = |k, v: Vec<Option<bool>>| {
            let arr = b(v.clone());
            reduce_quantifier(k, &arr, v.len())
        };
        // Empty list: all/none → true, any/single → false.
        assert_eq!(r(All, vec![]), Some(true));
        assert_eq!(r(None, vec![]), Some(true));
        assert_eq!(r(Any, vec![]), Some(false));
        assert_eq!(r(Single, vec![]), Some(false));
        // Definitive results.
        assert_eq!(r(All, vec![Some(true), Some(true)]), Some(true));
        assert_eq!(r(All, vec![Some(true), Some(false)]), Some(false));
        assert_eq!(r(Any, vec![Some(false), Some(true)]), Some(true));
        assert_eq!(r(None, vec![Some(false), Some(false)]), Some(true));
        assert_eq!(r(Single, vec![Some(true), Some(false)]), Some(true));
        assert_eq!(r(Single, vec![Some(true), Some(true)]), Some(false));
        // Three-valued: a null only matters when nothing definitive settles it.
        assert_eq!(r(All, vec![Some(true), Option::None]), Option::None); // unknown
        assert_eq!(r(All, vec![Some(false), Option::None]), Some(false)); // false wins
        assert_eq!(r(Any, vec![Some(false), Option::None]), Option::None);
        assert_eq!(r(Any, vec![Some(true), Option::None]), Some(true)); // true wins
        assert_eq!(
            r(Single, vec![Some(true), Some(true), Option::None]),
            Some(false)
        ); // >1 wins
        assert_eq!(r(Single, vec![Some(true), Option::None]), Option::None);
    }

    #[test]
    fn invariant_quantifier_truth_matrix_preserves_cardinality_and_nulls() {
        use graphforge_ir::QuantifierKind::{All, Any, None as NoneQ, Single};

        for kind in [All, Any, NoneQ, Single] {
            for predicate in [Some(true), Some(false), Option::None] {
                for length in [0, 1, 4] {
                    assert_eq!(
                        reduce_invariant_quantifier(kind, predicate, length),
                        match predicate {
                            Some(value) => {
                                let values = datafusion::arrow::array::BooleanArray::from(vec![
                                        value;
                                        length
                                    ]);
                                reduce_quantifier(kind, &values, length)
                            }
                            Option::None => {
                                let values =
                                    datafusion::arrow::array::BooleanArray::new_null(length);
                                reduce_quantifier(kind, &values, length)
                            }
                        },
                        "{kind:?}, predicate={predicate:?}, length={length}"
                    );
                }
            }
        }
    }

    #[test]
    fn invariant_quantifier_scaling_counts_rows_not_heterogeneous_elements() {
        use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, ListArray};
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;
        use datafusion::scalar::ScalarValue as S;
        use graphforge_ir::QuantifierKind::{All, Any, None as NoneQ, Single};
        use std::sync::Arc;

        let pattern = [
            S::Int64(Some(1)),
            S::Null,
            S::Boolean(Some(true)),
            S::Utf8(Some("x".to_owned())),
        ];
        let flat = pattern
            .iter()
            .cloned()
            .chain(pattern.iter().cloned().cycle().take(40))
            .collect::<Vec<_>>();
        let values: ArrayRef = Arc::new(build_het_struct(&flat, 0).unwrap());
        let item = Arc::new(Field::new("item", values.data_type().clone(), true));
        let list: ArrayRef = Arc::new(ListArray::new(
            item,
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 4, 44, 44])),
            values,
            Some(NullBuffer::from(vec![true, true, false])),
        ));
        INVARIANT_QUANTIFIER_ROWS.store(0, Ordering::SeqCst);

        for kind in [All, Any, NoneQ, Single] {
            for predicate in [Some(true), Some(false)] {
                let udf = CypherInvariantQuantifier::new(kind, predicate);
                let output = udf
                    .invoke_with_args(ScalarFunctionArgs {
                        args: vec![ColumnarValue::Array(Arc::clone(&list))],
                        arg_fields: vec![Arc::new(Field::new(
                            "list",
                            list.data_type().clone(),
                            true,
                        ))],
                        number_rows: 3,
                        return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
                        config_options: Arc::new(ConfigOptions::default()),
                    })
                    .unwrap()
                    .into_array(3)
                    .unwrap();
                let output = output.as_any().downcast_ref::<BooleanArray>().unwrap();
                assert_eq!(
                    output.value(0),
                    reduce_invariant_quantifier(kind, predicate, 4).unwrap()
                );
                assert_eq!(
                    output.value(1),
                    reduce_invariant_quantifier(kind, predicate, 40).unwrap()
                );
                assert!(output.is_null(2));
            }
        }
        assert_eq!(
            INVARIANT_QUANTIFIER_ROWS.load(Ordering::SeqCst),
            4 * 2 * 3,
            "1x and 10x element counts must keep invariant work at one fold per list row"
        );
    }

    #[test]
    fn invariant_quantifier_lowering_uses_cardinality_only_udf() {
        use graphforge_ir::QuantifierKind::None as NoneQ;

        for (predicate, expected) in [
            (IrLiteral::Bool(true), Some(true)),
            (IrLiteral::Bool(false), Some(false)),
            (IrLiteral::Null, Option::None),
        ] {
            let mut arena = ExprArena::new();
            let list = arena.push(IrExpr::VarRef(VarId(0)));
            let predicate = arena.push(IrExpr::Literal(predicate));
            let quantifier = arena.push(IrExpr::Quantifier {
                kind: NoneQ,
                loop_var: VarId(1),
                list,
                predicate,
            });
            let mut vars = VarMap::new();
            vars.insert(VarId(0), "list");
            let lowered = make_lowerer(&arena, &vars).lower(quantifier).unwrap();
            let DfExpr::ScalarFunction(function) = lowered else {
                panic!("invariant quantifier must lower to a scalar UDF")
            };
            let invariant = function
                .func
                .inner()
                .downcast_ref::<CypherInvariantQuantifier>()
                .expect("cardinality-only quantifier UDF");
            assert_eq!(invariant.predicate, expected);
        }
    }

    #[test]
    fn uncorrelated_list_comprehension_batches_volatile_predicate_once() {
        use datafusion::arrow::array::{Array, Int64Builder, ListArray, ListBuilder};
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;
        use std::sync::Arc;

        VOLATILE_CALLS.store(0, Ordering::SeqCst);
        VOLATILE_ROWS.store(0, Ordering::SeqCst);
        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.append_null();
        builder.append(true);
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true);
        builder.values().append_value(3);
        builder.append(true);
        let input = Arc::new(builder.finish()) as datafusion::arrow::array::ArrayRef;
        let predicate = ScalarUDF::new_from_impl(CountingVolatilePredicate::new()).call(vec![]);
        let udf = CypherListComp::new(Some(predicate), None, "__gf_elem".to_owned(), vec![]);
        let return_type = udf.return_type(&[input.data_type().clone()]).unwrap();
        let output = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(input)],
                arg_fields: vec![Arc::new(Field::new(
                    "list",
                    DataType::new_list(DataType::Int64, true),
                    true,
                ))],
                number_rows: 4,
                return_field: Arc::new(Field::new("out", return_type, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap()
            .into_array(4)
            .unwrap();
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();

        assert!(output.is_null(0));
        assert_eq!(output.value(1).len(), 0);
        assert_eq!(output.value(2).len(), 2);
        assert_eq!(output.value(3).len(), 1);
        assert_eq!(VOLATILE_CALLS.load(Ordering::SeqCst), 1);
        assert_eq!(VOLATILE_ROWS.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn correlated_list_comprehension_filters_and_projects_with_outer_value() {
        use datafusion::arrow::array::{Array, ListArray};
        use datafusion::logical_expr::{col, lit};

        let input = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(3)),
                ScalarValue::Int64(Some(5)),
            ],
            &DataType::Int64,
            true,
        ));
        let udf = CypherListComp::new(
            Some(col("__gf_elem").gt(col("threshold"))),
            Some(col("__gf_elem") + col("threshold") + lit(0_i64)),
            "__gf_elem".into(),
            vec!["threshold".into()],
        );
        let output = invoke_test_udf(&udf, vec![input, ScalarValue::Int64(Some(2))]).unwrap();
        let output = output.as_any().downcast_ref::<ListArray>().expect("List");
        assert!(!output.is_null(0));
        let values = output.value(0);
        assert_eq!(
            (0..values.len())
                .map(|row| ScalarValue::try_from_array(&values, row).unwrap())
                .collect::<Vec<_>>(),
            vec![ScalarValue::Int64(Some(5)), ScalarValue::Int64(Some(7))]
        );
    }

    #[test]
    fn percentile_and_comparison_error_contract_matrix_is_exact() {
        use datafusion::logical_expr::AggregateUDFImpl;

        let continuous = CypherPercentile::new(true);
        assert_eq!(
            continuous.return_type(&[]).unwrap_err().to_string(),
            "Error during planning: percentile aggregate requires value and percentile arguments"
        );
        assert_eq!(
            continuous
                .return_type(&[DataType::Utf8, DataType::Float64])
                .unwrap_err()
                .to_string(),
            "Error during planning: percentile value expression must be numeric, got Utf8"
        );
        assert_eq!(
            continuous
                .return_type(&[DataType::Int32, DataType::Float64])
                .unwrap(),
            DataType::Float64
        );
        assert_eq!(
            CypherPercentile::new(false)
                .return_type(&[DataType::Int32, DataType::Float64])
                .unwrap(),
            DataType::Int32
        );

        let mut accumulator = PercentileAcc {
            continuous: true,
            value_type: DataType::Int64,
            result_type: DataType::Float64,
            values: vec![],
            percentile: None,
        };
        accumulator.push_value(ScalarValue::Null).unwrap();
        assert_eq!(
            accumulator
                .push_value(ScalarValue::Utf8(Some("bad".into())))
                .unwrap_err()
                .to_string(),
            "Execution error: percentile value expression must be numeric, got Utf8"
        );
        for invalid in [f64::NAN, -0.1, 1.1] {
            assert!(
                accumulator
                    .observe_percentile(Some(invalid))
                    .unwrap_err()
                    .to_string()
                    .contains("finite number between 0.0 and 1.0")
            );
        }
        accumulator.observe_percentile(Some(0.25)).unwrap();
        assert_eq!(
            accumulator
                .observe_percentile(Some(0.75))
                .unwrap_err()
                .to_string(),
            "Execution error: percentile argument must be constant within an aggregate group"
        );

        assert_eq!(scalar_as_i8(&ScalarValue::Int8(Some(-7))).unwrap(), -7);
        assert_eq!(scalar_as_i8(&ScalarValue::Int64(Some(7))).unwrap(), 7);
        assert!(
            scalar_as_i8(&ScalarValue::Int64(Some(128)))
                .unwrap_err()
                .to_string()
                .contains("outside i8 range")
        );
        assert_eq!(
            scalar_as_i8(&ScalarValue::Utf8(Some("1".into())))
                .unwrap_err()
                .to_string(),
            "Error during planning: comparison opcode must be an integer, got Utf8(\"1\")"
        );
    }

    #[test]
    fn shared_temporal_cast_helper_preserves_arrow_values_nulls_and_error() {
        use datafusion::arrow::array::{Array, ArrayRef, Int32Array, Int64Array};

        let integers: ArrayRef = Arc::new(Int32Array::from(vec![Some(7), None]));
        let casted = cast_argument_arrays(&[integers], &DataType::Int64).unwrap();
        let casted = casted[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64");
        assert_eq!(casted.value(0), 7);
        assert!(casted.is_null(1));

        let invalid = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Int64(Some(1))],
            &DataType::Int64,
            true,
        ))
        .to_array_of_size(1)
        .unwrap();
        let direct_error = datafusion::arrow::compute::cast(&invalid, &DataType::Int64)
            .map_err(datafusion::error::DataFusionError::from)
            .unwrap_err()
            .to_string();
        let helper_error = cast_argument_arrays(&[invalid], &DataType::Int64)
            .unwrap_err()
            .to_string();
        assert_eq!(helper_error, direct_error);
    }

    #[test]
    fn list_comprehension_rejects_non_list_input_through_public_udf_contract() {
        let udf = CypherListComp::new(None, None, "__gf_elem".into(), vec![]);
        let error = invoke_test_udf(&udf, vec![ScalarValue::Int64(Some(1))]).unwrap_err();
        let datafusion::error::DataFusionError::Internal(message) = error else {
            panic!("expected DataFusion internal contract error")
        };
        assert_eq!(
            message,
            "cypher_list_comprehension: first argument is not a list"
        );
    }

    #[test]
    fn public_map_and_value_access_error_null_and_success_matrix() {
        use datafusion::arrow::array::{Array, ListArray};

        let null_keys = invoke_test_udf(&CypherMapKeys::new(), vec![ScalarValue::Null]).unwrap();
        let null_keys = null_keys
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("List");
        assert!(null_keys.is_null(0));

        let keys_error =
            invoke_test_udf(&CypherMapKeys::new(), vec![ScalarValue::Int64(Some(1))]).unwrap_err();
        assert_eq!(
            keys_error.to_string(),
            "Execution error: keys() requires a map, node, relationship, or null, got Int64"
        );

        let map = const_map_scalar(&[
            ("answer".into(), ScalarValue::Int64(Some(42))),
            ("empty".into(), ScalarValue::Null),
        ])
        .expect("map scalar");
        let keys = invoke_test_udf(&CypherMapKeys::new(), vec![map.clone()]).unwrap();
        let keys = keys.as_any().downcast_ref::<ListArray>().expect("List");
        assert_eq!(keys.value(0).len(), 2);

        let answer = invoke_test_udf(
            &CypherStaticValueAccess::new("answer".into()),
            vec![map.clone()],
        )
        .unwrap();
        assert_eq!(
            ScalarValue::try_from_array(&answer, 0).unwrap(),
            ScalarValue::Int64(Some(42))
        );
        let missing =
            invoke_test_udf(&CypherStaticValueAccess::new("missing".into()), vec![map]).unwrap();
        assert!(ScalarValue::try_from_array(&missing, 0).unwrap().is_null());

        let static_error = invoke_test_udf(
            &CypherStaticValueAccess::new("answer".into()),
            vec![ScalarValue::Int64(Some(1))],
        )
        .unwrap_err();
        assert_eq!(
            static_error.to_string(),
            "Execution error: InvalidArgumentValue: property access requires a map or graph element"
        );

        let dynamic_error = invoke_test_udf(
            &CypherValueAccess::new(),
            vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Utf8(Some("answer".into())),
            ],
        )
        .unwrap_err();
        assert_eq!(
            dynamic_error.to_string(),
            "Execution error: dynamic subscript requires a list or map/entity struct, got Int64"
        );
    }

    #[test]
    fn entity_properties_and_percentile_state_validation_errors_are_exact() {
        use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array};
        use datafusion::logical_expr::Accumulator;

        let arity_error = invoke_test_udf(&CypherEntityProperties::new(0), vec![]).unwrap_err();
        assert_eq!(
            arity_error.to_string(),
            "Error during planning: properties() entity map expects present plus key/value pairs"
        );
        let presence_error = invoke_test_udf(
            &CypherEntityProperties::new(3),
            vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Utf8(Some("name".into())),
                ScalarValue::Utf8(Some("Ada".into())),
            ],
        )
        .unwrap_err();
        assert_eq!(
            presence_error.to_string(),
            "Execution error: properties() entity presence must be boolean, got Int64(1)"
        );

        let mut accumulator = PercentileAcc {
            continuous: true,
            value_type: DataType::Int64,
            result_type: DataType::Float64,
            values: vec![],
            percentile: None,
        };
        let wrong_values: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let percentile: ArrayRef = Arc::new(Float64Array::from(vec![0.5]));
        assert_eq!(
            accumulator
                .merge_batch(&[wrong_values, percentile])
                .unwrap_err()
                .to_string(),
            "Error during planning: percentile state values must be a list"
        );
    }

    /// Invoke a `cypher_quantifier` UDF (no outer columns) over a single list
    /// column and return the per-row boolean verdicts.
    fn invoke_cypher_quantifier(
        kind: graphforge_ir::QuantifierKind,
        predicate: DfExpr,
        list: datafusion::arrow::array::ArrayRef,
    ) -> datafusion::error::Result<datafusion::arrow::array::BooleanArray> {
        use datafusion::arrow::array::BooleanArray;
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;
        use std::sync::Arc;

        let n = list.len();
        let field = Arc::new(Field::new("l", list.data_type().clone(), true));
        let ret = Arc::new(Field::new("q", DataType::Boolean, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(list)],
            arg_fields: vec![field],
            number_rows: n,
            return_field: ret,
            config_options: Arc::new(ConfigOptions::default()),
        };
        let udf = CypherQuantifier::new(kind, predicate, "__gf_elem".to_owned(), vec![]);
        let arr = match udf.invoke_with_args(args)? {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(n)?,
        };
        Ok(arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean verdicts")
            .clone())
    }

    #[test]
    fn cypher_quantifier_empty_list_yields_identity_without_predicate() {
        use datafusion::arrow::array::{Array, ListArray};
        use datafusion::arrow::datatypes::Int64Type;
        use graphforge_ir::QuantifierKind::{All, Any, None as NoneQ, Single};

        // One empty Int64 list row — the shape a statically-empty `[]` takes.
        let empty = || {
            std::sync::Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(Vec::<Option<i64>>::new()),
            ])) as datafusion::arrow::array::ArrayRef
        };

        // `x.a = 2` cannot plan over Int64 elements; `x` alone evaluates to
        // Int64, not Boolean. Neither may disturb the empty-list identity
        // (#1020) — the predicate must not run at all.
        let field_pred =
            datafusion::functions::core::expr_fn::get_field(col_literal("__gf_elem"), "a")
                .eq(DfExpr::Literal(ScalarValue::Int64(Some(2)), Option::None));
        let elem_pred = col_literal("__gf_elem");

        for (kind, expected) in [(All, true), (NoneQ, true), (Any, false), (Single, false)] {
            for pred in [field_pred.clone(), elem_pred.clone()] {
                let out = invoke_cypher_quantifier(kind, pred, empty()).expect("no error");
                assert!(!out.is_null(0), "{kind:?}: empty list must be definitive");
                assert_eq!(out.value(0), expected, "{kind:?} identity");
            }
        }
    }

    #[test]
    fn cypher_quantifier_mixed_batch_and_unplannable_predicate() {
        use datafusion::arrow::array::{Array, BooleanBuilder, ListBuilder};
        use graphforge_ir::QuantifierKind::Any;

        // Rows: null list, empty list, [true, false].
        let mut b = ListBuilder::new(BooleanBuilder::new());
        b.append_null();
        b.append(true);
        b.values().append_value(true);
        b.values().append_value(false);
        b.append(true);
        let list = std::sync::Arc::new(b.finish()) as datafusion::arrow::array::ArrayRef;

        // `any(x IN ... WHERE x)` over Boolean elements: per-row verdicts.
        let out = invoke_cypher_quantifier(Any, col_literal("__gf_elem"), list.clone())
            .expect("no error");
        assert!(out.is_null(0), "null list → null");
        assert!(!out.is_null(1) && !out.value(1), "any over [] is false");
        assert!(out.value(2), "any over [true, false] is true");

        // An unbuildable predicate still errors once a non-empty row needs it.
        let bad = datafusion::functions::core::expr_fn::get_field(col_literal("__gf_elem"), "a");
        assert!(
            invoke_cypher_quantifier(Any, bad, list).is_err(),
            "non-empty row against an unbuildable predicate must error"
        );
    }

    /// Build a minimal lowerer with no ontology.
    fn make_lowerer<'a>(arena: &'a ExprArena, var_map: &'a VarMap) -> ExprLowerer<'a> {
        ExprLowerer::new(arena, None, var_map)
    }

    // -----------------------------------------------------------------------
    // Conservative operand type-check (#956, InvalidArgumentType)
    // -----------------------------------------------------------------------

    #[test]
    fn boolean_and_numeric_operators_reject_known_bad_operands() {
        use graphforge_ir::expr::{BinaryOpKind as B, IrExpr, IrLiteral, UnaryOpKind as U};
        let vm = VarMap::new();

        // A statically-known incompatible operand is rejected as InvalidType.
        let reject = |build: &dyn Fn(&mut ExprArena) -> ExprId| {
            let mut a = ExprArena::new();
            let id = build(&mut a);
            let err = make_lowerer(&a, &vm).lower(id).expect_err("should reject");
            assert!(
                matches!(err, LoweringError::InvalidType(_)),
                "expected InvalidType, got {err:?}"
            );
        };
        // `1 AND true`
        reject(&|a| {
            let l = a.push(IrExpr::Literal(IrLiteral::Int(1)));
            let r = a.push(IrExpr::Literal(IrLiteral::Bool(true)));
            a.push(IrExpr::BinaryOp {
                op: B::And,
                left: l,
                right: r,
            })
        });
        // `1 XOR true`
        reject(&|a| {
            let l = a.push(IrExpr::Literal(IrLiteral::Int(1)));
            let r = a.push(IrExpr::Literal(IrLiteral::Bool(true)));
            a.push(IrExpr::BinaryOp {
                op: B::Xor,
                left: l,
                right: r,
            })
        });
        // `NOT 'x'`
        reject(&|a| {
            let e = a.push(IrExpr::Literal(IrLiteral::Str("x".into())));
            a.push(IrExpr::UnaryOp {
                op: U::Not,
                expr: e,
            })
        });
        // `-true`
        reject(&|a| {
            let e = a.push(IrExpr::Literal(IrLiteral::Bool(true)));
            a.push(IrExpr::UnaryOp {
                op: U::Neg,
                expr: e,
            })
        });
        // `'a' % 2`
        reject(&|a| {
            let l = a.push(IrExpr::Literal(IrLiteral::Str("a".into())));
            let r = a.push(IrExpr::Literal(IrLiteral::Int(2)));
            a.push(IrExpr::BinaryOp {
                op: B::Mod,
                left: l,
                right: r,
            })
        });
        // `NOT {k: 1}` — a map literal is a known non-boolean.
        reject(&|a| {
            let v = a.push(IrExpr::Literal(IrLiteral::Int(1)));
            let m = a.push(IrExpr::MapLiteral(vec![("k".into(), v)]));
            a.push(IrExpr::UnaryOp {
                op: U::Not,
                expr: m,
            })
        });
    }

    #[test]
    fn operator_type_check_accepts_valid_and_unknown_operands() {
        use graphforge_ir::VarId;
        use graphforge_ir::expr::{BinaryOpKind as B, IrExpr, IrLiteral, UnaryOpKind as U};
        let accept = |vm: &VarMap, build: &dyn Fn(&mut ExprArena) -> ExprId| {
            let mut a = ExprArena::new();
            let id = build(&mut a);
            make_lowerer(&a, vm)
                .lower(id)
                .expect("valid/unknown operands must lower cleanly");
        };
        // `true AND false`
        accept(&VarMap::new(), &|a| {
            let l = a.push(IrExpr::Literal(IrLiteral::Bool(true)));
            let r = a.push(IrExpr::Literal(IrLiteral::Bool(false)));
            a.push(IrExpr::BinaryOp {
                op: B::And,
                left: l,
                right: r,
            })
        });
        // `null AND true` — null is valid in three-valued logic.
        accept(&VarMap::new(), &|a| {
            let l = a.push(IrExpr::Literal(IrLiteral::Null));
            let r = a.push(IrExpr::Literal(IrLiteral::Bool(true)));
            a.push(IrExpr::BinaryOp {
                op: B::And,
                left: l,
                right: r,
            })
        });
        // `NOT x` where x is an untyped variable (unknown type — do not reject).
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "x");
        accept(&vm, &|a| {
            let e = a.push(IrExpr::VarRef(VarId(0)));
            a.push(IrExpr::UnaryOp {
                op: U::Not,
                expr: e,
            })
        });
    }

    #[test]
    fn nested_quantifiers_get_per_depth_elem_columns() {
        use graphforge_ir::QuantifierKind::{None as NoneQ, Single};

        // none(x IN list WHERE single(y IN list WHERE x + y = 15)): the inner
        // predicate references BOTH loop elements, so the bindings must stay
        // distinct across depths (#1021).
        let mut arena = ExprArena::new();
        let list_outer = arena.push(IrExpr::VarRef(VarId(0)));
        let list_inner = arena.push(IrExpr::VarRef(VarId(0)));
        let x = arena.push(IrExpr::VarRef(VarId(1)));
        let y = arena.push(IrExpr::VarRef(VarId(2)));
        let sum = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Add,
            left: x,
            right: y,
        });
        let fifteen = arena.push(IrExpr::Literal(IrLiteral::Int(15)));
        let eq = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Eq,
            left: sum,
            right: fifteen,
        });
        let inner = arena.push(IrExpr::Quantifier {
            kind: Single,
            loop_var: VarId(2),
            list: list_inner,
            predicate: eq,
        });
        let outer = arena.push(IrExpr::Quantifier {
            kind: NoneQ,
            loop_var: VarId(1),
            list: list_outer,
            predicate: inner,
        });

        let mut vm = VarMap::new();
        vm.insert(VarId(0), "list");
        let lowered = make_lowerer(&arena, &vm).lower(outer).expect("lower");

        let as_quant = |e: &DfExpr| -> Option<(String, Vec<String>, Vec<DfExpr>)> {
            let DfExpr::ScalarFunction(f) = e else {
                return Option::None;
            };
            let q = f.func.inner().downcast_ref::<CypherQuantifier>()?;
            Some((q.elem_name.clone(), q.outer_names.clone(), f.args.clone()))
        };

        // Outer loop keeps the historical name; its only outer column is the
        // real `list` (its own element must NOT leak into its args).
        let (outer_elem, outer_outers, _) = as_quant(&lowered).expect("outer quantifier UDF");
        assert_eq!(outer_elem, "__gf_elem");
        assert_eq!(outer_outers, vec!["list".to_owned()]);

        // The inner loop gets the depth-1 name, and the OUTER element flows in
        // as one of its outer columns (broadcast per outer element at invoke).
        let DfExpr::ScalarFunction(outer_fn) = &lowered else {
            panic!("outer is a scalar function")
        };
        let outer_q = outer_fn
            .func
            .inner()
            .downcast_ref::<CypherQuantifier>()
            .unwrap();
        let (inner_elem, inner_outers, inner_args) =
            as_quant(&outer_q.predicate).expect("inner quantifier UDF");
        assert_eq!(inner_elem, "__gf_elem_1");
        assert_eq!(
            inner_outers,
            vec!["__gf_elem".to_owned()],
            "the outer element is an outer column of the inner UDF (the list \
             argument resolves in the enclosing batch, not via outer_names)"
        );
        // Call args: the list, then one arg per outer_names entry.
        let arg_names: Vec<String> = inner_args.iter().map(|a| a.to_string()).collect();
        assert_eq!(arg_names, vec!["list", "__gf_elem"]);
    }

    #[test]
    fn hydrated_path_nodes_return_type_carries_labels_and_props() {
        use datafusion::arrow::datatypes::Field;

        // Without hydration: the original node_uuid-only element (#754).
        let bare = CypherPathNodes::new().return_type(&[]).unwrap();
        let DataType::List(item) = &bare else {
            panic!("list return, got {bare:?}")
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("struct element")
        };
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name(), "node_uuid");

        // With hydration: node_uuid, labels, then the baked property union
        // (#1024) — the shape `render_node_struct` and `x.<prop>` need.
        let hydrated = CypherPathNodes::with_hydration(PathNodeHydration {
            dir: std::path::PathBuf::from("/nonexistent"),
            labels_by_type: vec![(0, "A".to_owned())],
            prop_stems: vec!["_untyped".to_owned()],
            fields: vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
                Field::new("name", DataType::Utf8, true),
            ]
            .into(),
        })
        .return_type(&[])
        .unwrap();
        let DataType::List(item) = &hydrated else {
            panic!("list return, got {hydrated:?}")
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("struct element")
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["node_uuid", "labels", "name"]);
    }

    #[test]
    fn elem_struct_col_routes_property_to_get_field() {
        // #1004: a property access on the synthetic quantifier/comprehension
        // element column (`__gf_elem`) is a STRUCT-FIELD access, so it must lower
        // via `get_field(__gf_elem, "a")` — not the dotted property-column
        // `__gf_elem.a` used for node properties (which DataFusion reads as a
        // qualified column that does not exist on a single struct column, giving
        // "No field named a"). Any OTHER base still uses the dotted form.
        let mut arena = ExprArena::new();
        let base = arena.push(IrExpr::VarRef(VarId(0)));
        let access = arena.push(IrExpr::PropertyAccess {
            base,
            prop: PropId(0),
        });
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "__gf_elem");
        let mut prop_names = HashMap::new();
        prop_names.insert(0u32, "a".to_owned());

        // Without the marker: node-property dotted-column form — a QUALIFIED
        // column `__gf_elem.a` (relation `__gf_elem`, column `a`), which is
        // exactly what DataFusion cannot resolve against a single struct column.
        let dotted = ExprLowerer::with_prop_names(&arena, &vm, prop_names.clone())
            .lower(access)
            .unwrap();
        assert!(
            matches!(&dotted, DfExpr::Column(_)) && dotted.to_string() == "__gf_elem.a",
            "expected dotted column, got {dotted:?}"
        );

        // With the marker: struct-aware `get_field` (a scalar-function call, not a
        // Column), so plan-time validation resolves against the element's Struct.
        let via_get_field = ExprLowerer::with_prop_names(&arena, &vm, prop_names.clone())
            .with_elem_struct_col("__gf_elem".to_owned())
            .lower(access)
            .unwrap();
        assert!(
            !matches!(&via_get_field, DfExpr::Column(_)),
            "element field access must not be a dotted column: {via_get_field:?}"
        );
        assert!(
            via_get_field.to_string().contains("get_field"),
            "expected a get_field call, got {via_get_field}"
        );
    }

    #[test]
    fn map_column_field_access_uses_get_field_via_schema() {
        // #1017: a `PropertyAccess` on a plain-map-typed column (e.g. `input.list`
        // where `input` was bound by `UNWIND [{list: …}] AS input`) resolves via
        // struct-aware `get_field` — NOT a dotted qualified column `input.list`,
        // which fails "No field named input.list" against a single struct column.
        use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
        use datafusion::common::DFSchema;

        let map_ty = DataType::Struct(Fields::from(vec![
            Field::new("list", DataType::new_list(DataType::Int64, true), true),
            Field::new("fixed", DataType::Boolean, true),
        ]));
        let schema = Schema::new(vec![Field::new("input", map_ty, true)]);
        let df_schema = std::sync::Arc::new(DFSchema::try_from(schema).unwrap());

        let mut arena = ExprArena::new();
        let base = arena.push(IrExpr::VarRef(VarId(0)));
        let access = arena.push(IrExpr::PropertyAccess {
            base,
            prop: PropId(0),
        });
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "input");
        let mut prop_names = HashMap::new();
        prop_names.insert(0u32, "list".to_owned());

        let out = ExprLowerer::with_prop_names(&arena, &vm, prop_names)
            .with_input_schema(df_schema)
            .lower(access)
            .unwrap();
        assert!(
            !matches!(&out, DfExpr::Column(_)),
            "map field must not be a dotted column: {out:?}"
        );
        assert!(
            out.to_string().contains("get_field"),
            "expected a get_field call, got {out}"
        );
    }

    #[test]
    fn list_plus_uses_native_ops_for_homogeneous_schema_types() {
        // #1017: with the input schema attached, `is_list_typed` types a list-valued
        // COLUMN operand. Homogeneous list ops stay in native Arrow list functions
        // so downstream quantifiers keep concrete element types; heterogeneous
        // list ops route to Cypher list-plus instead of numeric `+`.
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::DFSchema;
        use graphforge_ir::expr::BinaryOpKind;

        let schema = Schema::new(vec![
            Field::new("xs", DataType::new_list(DataType::Int64, true), true),
            Field::new("y", DataType::Int64, true),
            Field::new("ys", DataType::new_list(DataType::Int64, true), true),
            Field::new("s", DataType::Utf8, true),
            Field::new("ss", DataType::new_list(DataType::Utf8, true), true),
        ]);
        let df_schema = std::sync::Arc::new(DFSchema::try_from(schema).unwrap());

        let mut arena = ExprArena::new();
        let xs = arena.push(IrExpr::VarRef(VarId(0)));
        let y = arena.push(IrExpr::VarRef(VarId(1)));
        let ys = arena.push(IrExpr::VarRef(VarId(2)));
        let s = arena.push(IrExpr::VarRef(VarId(3)));
        let ss = arena.push(IrExpr::VarRef(VarId(4)));
        let append = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Add,
            left: xs,
            right: y,
        });
        let concat = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Add,
            left: xs,
            right: ys,
        });
        let hetero_append = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Add,
            left: xs,
            right: s,
        });
        let hetero_concat = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Add,
            left: xs,
            right: ss,
        });
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "xs");
        vm.insert(VarId(1), "y");
        vm.insert(VarId(2), "ys");
        vm.insert(VarId(3), "s");
        vm.insert(VarId(4), "ss");

        let lowerer =
            ExprLowerer::with_prop_names(&arena, &vm, HashMap::new()).with_input_schema(df_schema);
        assert!(
            lowerer
                .lower(append)
                .unwrap()
                .to_string()
                .contains("array_append"),
            "list + element should append"
        );
        assert!(
            lowerer
                .lower(concat)
                .unwrap()
                .to_string()
                .contains("array_concat"),
            "list + list should concat"
        );
        assert!(
            lowerer
                .lower(hetero_append)
                .unwrap()
                .to_string()
                .contains("cypher_list_plus"),
            "list + heterogeneous element should use tagged list-plus"
        );
        assert!(
            lowerer
                .lower(hetero_concat)
                .unwrap()
                .to_string()
                .contains("cypher_list_plus"),
            "list + heterogeneous list should use tagged list-plus"
        );
    }

    #[test]
    fn cypher_list_plus_concats_decoded_list_element() {
        use datafusion::arrow::array::{Array, ArrayRef, ListArray};
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;
        use datafusion::scalar::ScalarValue as S;
        use std::sync::Arc;

        let list = |items: Vec<S>| S::List(S::new_list(&items, &DataType::Int64, true));
        let left_items = vec![
            list(vec![S::Int64(Some(1))]),
            list(vec![S::Int64(Some(2)), S::Int64(Some(3))]),
            list(vec![S::Int64(Some(4)), S::Int64(Some(5))]),
        ];
        let left = S::List(S::new_list(&left_items, &left_items[0].data_type(), true))
            .to_array()
            .unwrap();

        let right_list = list(vec![S::Int64(Some(8)), S::Int64(Some(9))]);
        let right: ArrayRef = Arc::new(build_het_struct(&[right_list], 1).unwrap());
        let udf = CypherListPlus::new();
        let arg_types = vec![left.data_type().clone(), right.data_type().clone()];
        let return_type = udf.return_type(&arg_types).unwrap();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(left), ColumnarValue::Array(right)],
            arg_fields: vec![
                Arc::new(Field::new(
                    "l",
                    DataType::new_list(DataType::Int64, true),
                    true,
                )),
                Arc::new(Field::new("r", DataType::Struct(het_fields(1)), true)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("out", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let out = match udf.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array().unwrap(),
        };
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(!out.is_null(0));
        let values = out.value(0);
        assert_eq!(values.len(), 5);
        assert_eq!(
            decode_het(&ScalarValue::try_from_array(&values, 3).unwrap()),
            Some(S::Int64(Some(8)))
        );
        assert_eq!(
            decode_het(&ScalarValue::try_from_array(&values, 4).unwrap()),
            Some(S::Int64(Some(9)))
        );
    }

    #[test]
    fn tagged_list_element_plus_preserves_dynamic_concat_and_null_rows() {
        use datafusion::arrow::array::{Array, ArrayRef, ListArray};
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use datafusion::arrow::datatypes::Field;
        use datafusion::scalar::ScalarValue as S;
        use std::sync::Arc;

        let values: ArrayRef = Arc::new(
            build_het_struct(
                &[S::Int64(Some(1)), S::Boolean(Some(true)), S::Int64(Some(2))],
                1,
            )
            .unwrap(),
        );
        let item = Arc::new(Field::new("item", values.data_type().clone(), true));
        let left: ArrayRef = Arc::new(ListArray::new(
            item.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 3, 3, 3])),
            values,
            Some(NullBuffer::from(vec![true, true, true, false])),
        ));
        let nested = S::List(S::new_list(
            &[S::Int64(Some(8)), S::Int64(Some(7))],
            &DataType::Int64,
            true,
        ));
        let right: ArrayRef = Arc::new(
            build_het_struct(&[S::Int64(Some(9)), nested, S::Null, S::Int64(Some(1))], 1).unwrap(),
        );
        let return_type = DataType::List(item);
        let output = invoke_tagged_list_element_plus(&left, &right, &return_type)
            .unwrap()
            .expect("tagged fast path");
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        let decode_row = |row: usize| {
            let values = output.value(row);
            (0..values.len())
                .map(|index| {
                    decode_het(&ScalarValue::try_from_array(&values, index).unwrap()).unwrap()
                })
                .collect::<Vec<_>>()
        };

        assert_eq!(
            decode_row(0),
            vec![S::Int64(Some(1)), S::Boolean(Some(true)), S::Int64(Some(9))]
        );
        assert_eq!(
            decode_row(1),
            vec![S::Int64(Some(2)), S::Int64(Some(8)), S::Int64(Some(7))]
        );
        assert_eq!(decode_row(2), vec![S::Null]);
        assert!(output.is_null(3));
    }

    #[test]
    fn cypher_conversions_decode_tagged_values() {
        use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;
        use datafusion::scalar::ScalarValue as S;
        use std::sync::Arc;

        let values: ArrayRef = Arc::new(
            build_het_struct(
                &[
                    S::Int64(Some(2)),
                    S::Float64(Some(2.9)),
                    S::Utf8(Some("foo".to_owned())),
                ],
                0,
            )
            .unwrap(),
        );
        let invoke = |kind: CypherConversionKind| {
            let udf = CypherConversion::new(kind);
            let return_type = udf.return_type(&[values.data_type().clone()]).unwrap();
            let args = ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::clone(&values))],
                arg_fields: vec![Arc::new(Field::new("v", values.data_type().clone(), true))],
                number_rows: values.len(),
                return_field: Arc::new(Field::new("out", return_type, true)),
                config_options: Arc::new(ConfigOptions::default()),
            };
            match udf.invoke_with_args(args).unwrap() {
                ColumnarValue::Array(a) => a,
                ColumnarValue::Scalar(s) => s.to_array_of_size(values.len()).unwrap(),
            }
        };

        let ints = invoke(CypherConversionKind::Integer);
        let ints = ints.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.value(0), 2);
        assert_eq!(ints.value(1), 2);
        assert!(ints.is_null(2));

        let floats = invoke(CypherConversionKind::Float);
        let floats = floats.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(floats.value(0), 2.0);
        assert_eq!(floats.value(1), 2.9);
        assert!(floats.is_null(2));

        let strings = match CYPHER_TO_STRING
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(values)],
                arg_fields: vec![Arc::new(Field::new(
                    "v",
                    DataType::Struct(het_fields(0)),
                    true,
                ))],
                number_rows: 3,
                return_field: Arc::new(Field::new("out", DataType::Utf8, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap()
        {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(3).unwrap(),
        };
        let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0), "2");
        assert_eq!(strings.value(1), "2.9");
        assert_eq!(strings.value(2), "foo");
    }

    #[test]
    fn het_list_map_roundtrip_and_order() {
        use datafusion::scalar::ScalarValue as S;
        // A plain map is recognised; a typed temporal struct is NOT (#1005).
        let map = const_map_scalar(&[
            ("a".to_owned(), S::Int64(Some(2))),
            ("b".to_owned(), S::Boolean(Some(true))),
        ])
        .expect("map scalar");
        let S::Struct(m) = &map else {
            panic!("map is a struct")
        };
        assert!(is_plain_map_struct(m));
        let S::Struct(d) = date_scalar(Some(0)) else {
            panic!("date is a struct")
        };
        assert!(!is_plain_map_struct(&d));

        // A mixed het list `[1, {a: 2, b: true}]` encodes to a tagged struct whose
        // map element (tag 5) decodes back to a structurally-equal map.
        let scalars = vec![S::Int64(Some(1)), map.clone()];
        assert_eq!(het_depth(&scalars[0]), Some(0));
        assert_eq!(het_depth(&scalars[1]), Some(1));
        let depth = scalars.iter().filter_map(het_depth).max().unwrap();
        let elem = build_het_struct(&scalars, depth).expect("build tagged struct");
        let e0 = ScalarValue::try_from_array(&elem, 0).unwrap();
        assert_eq!(decode_het(&e0), Some(S::Int64(Some(1))));
        let e1 = ScalarValue::try_from_array(&elem, 1).unwrap();
        let decoded = decode_het(&e1).expect("decode map element");
        assert_eq!(cypher_value_eq(&decoded, &map), Some(true));

        // Orderability (ADR 0011 slice 5): maps rank above numbers; two maps order
        // by their (sorted) entries.
        let m1 = const_map_scalar(&[("a".to_owned(), S::Int64(Some(1)))]).unwrap();
        let m2 = const_map_scalar(&[("a".to_owned(), S::Int64(Some(2)))]).unwrap();
        assert_eq!(cypher_order(&m1, &m2), std::cmp::Ordering::Less);
        assert_eq!(
            cypher_order(&S::Int64(Some(99)), &m1),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn empty_map_scalar_has_one_row_and_no_fields() {
        use datafusion::arrow::array::Array;
        use datafusion::scalar::ScalarValue as S;

        let map = const_map_scalar(&[]).expect("empty map scalar");
        let S::Struct(values) = map else {
            panic!("empty map is a struct")
        };
        assert_eq!(values.len(), 1);
        assert_eq!(values.num_columns(), 0);
        assert!(is_plain_map_struct(&values));

        let scalars = vec![S::Int64(Some(1)), S::Struct(values.clone())];
        let encoded = build_het_struct(&scalars, 1).expect("encode empty map");
        let tagged = S::try_from_array(&encoded, 1).expect("tagged empty map");
        let S::Struct(decoded) = decode_het(&tagged).expect("decode empty map") else {
            panic!("decoded empty map is a struct")
        };
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded.num_columns(), 0);
    }

    #[test]
    fn dictionary_scalar_is_normalized_for_heterogeneous_encoding() {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::scalar::ScalarValue as S;

        let dictionary = S::Dictionary(
            Box::new(DataType::Int32),
            Box::new(S::Utf8(Some("value".to_owned()))),
        );
        let scalars = vec![S::Int64(Some(1)), dictionary];
        assert_eq!(het_depth(&scalars[1]), Some(0));

        let encoded = build_het_struct(&scalars, 0).expect("encode dictionary scalar");
        let tagged = S::try_from_array(&encoded, 1).expect("tagged dictionary scalar");
        assert_eq!(decode_het(&tagged), Some(S::Utf8(Some("value".to_owned()))));
    }

    #[test]
    fn cypher_value_eq_scalars() {
        use datafusion::scalar::ScalarValue as S;
        let i = |n| S::Int64(Some(n));

        assert_eq!(cypher_value_eq(&i(1), &i(1)), Some(true));
        assert_eq!(cypher_value_eq(&i(1), &i(2)), Some(false));
        // number vs string ⇒ false (different types are never equal, not an error)
        assert_eq!(
            cypher_value_eq(&i(1), &S::Utf8(Some("1".into()))),
            Some(false)
        );
        // 1 = 1.0 (cross numeric)
        assert_eq!(cypher_value_eq(&i(1), &S::Float64(Some(1.0))), Some(true));
        assert_eq!(cypher_value_eq(&i(1), &S::UInt64(Some(1))), Some(true));
        assert_eq!(
            cypher_value_eq(&S::Float64(Some(f64::NAN)), &S::Float64(Some(f64::NAN))),
            Some(false)
        );
        // null propagates
        assert_eq!(cypher_value_eq(&i(1), &S::Int64(None)), None);
        assert_eq!(cypher_value_eq(&S::Null, &i(1)), None);
    }

    #[test]
    fn cypher_comparison_predicate_handles_nan_and_cross_type() {
        use datafusion::scalar::ScalarValue as S;

        assert_eq!(
            cypher_compare_pred(&S::Float64(Some(f64::NAN)), &S::Int64(Some(1)), 2),
            Some(false)
        );
        assert_eq!(
            cypher_compare_pred(&S::Utf8(Some("1".to_owned())), &S::Int64(Some(1)), 0,),
            None
        );
        assert_eq!(
            cypher_compare_pred(&S::Int64(Some(1)), &S::Float64(Some(2.0)), 0),
            Some(true)
        );
        assert_eq!(
            cypher_compare_pred(&S::UInt64(Some(1)), &S::Int64(Some(2)), 0),
            Some(true)
        );
    }

    // -----------------------------------------------------------------------
    // Literal tests
    // -----------------------------------------------------------------------

    #[test]
    fn literal_null() {
        let mut arena = ExprArena::new();
        let id = arena.push(IrExpr::Literal(IrLiteral::Null));
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        assert!(matches!(result, DfExpr::Literal(ScalarValue::Null, _)));
    }

    #[test]
    fn literal_bool() {
        let mut arena = ExprArena::new();
        let id = arena.push(IrExpr::Literal(IrLiteral::Bool(true)));
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        assert!(matches!(
            result,
            DfExpr::Literal(ScalarValue::Boolean(Some(true)), _)
        ));
    }

    #[test]
    fn literal_int() {
        let mut arena = ExprArena::new();
        let id = arena.push(IrExpr::Literal(IrLiteral::Int(42)));
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        assert!(matches!(
            result,
            DfExpr::Literal(ScalarValue::Int64(Some(42)), _)
        ));
    }

    #[test]
    fn literal_float() {
        let mut arena = ExprArena::new();
        let id = arena.push(IrExpr::Literal(IrLiteral::Float(2.71)));
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        assert!(matches!(
            result,
            DfExpr::Literal(ScalarValue::Float64(Some(_)), _)
        ));
    }

    #[test]
    fn literal_str() {
        let mut arena = ExprArena::new();
        let id = arena.push(IrExpr::Literal(IrLiteral::Str("hello".into())));
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        assert!(matches!(
            result,
            DfExpr::Literal(ScalarValue::Utf8(Some(_)), _)
        ));
    }

    #[test]
    fn literal_duration() {
        let mut arena = ExprArena::new();
        let id = arena.push(IrExpr::Literal(IrLiteral::Duration {
            months: 0,
            days: 0,
            seconds: 1,
            nanos: 0,
        }));
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        assert!(matches!(result, DfExpr::Literal(ScalarValue::Struct(_), _)));
    }

    #[test]
    fn literal_datetime() {
        let mut arena = ExprArena::new();
        let id = arena.push(IrExpr::Literal(IrLiteral::DateTime(0)));
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        assert!(matches!(
            result,
            DfExpr::Literal(ScalarValue::TimestampMicrosecond(Some(0), Some(_)), _)
        ));
    }

    // -----------------------------------------------------------------------
    // VarRef tests
    // -----------------------------------------------------------------------

    #[test]
    fn var_ref_bound() {
        let mut arena = ExprArena::new();
        let id = arena.push(IrExpr::VarRef(VarId(0)));
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "node_id");
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        assert!(matches!(result, DfExpr::Column(_)));
        if let DfExpr::Column(col) = result {
            assert_eq!(col.name, "node_id");
        }
    }

    #[test]
    fn var_ref_unbound_returns_error() {
        let mut arena = ExprArena::new();
        let id = arena.push(IrExpr::VarRef(VarId(99)));
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id);
        assert!(matches!(result, Err(LoweringError::UnboundVar(99))));
    }

    // -----------------------------------------------------------------------
    // UnaryOp tests
    // -----------------------------------------------------------------------

    #[test]
    fn unary_not() {
        let mut arena = ExprArena::new();
        let inner = arena.push(IrExpr::Literal(IrLiteral::Bool(true)));
        let id = arena.push(IrExpr::UnaryOp {
            op: UnaryOpKind::Not,
            expr: inner,
        });
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        assert!(matches!(result, DfExpr::Not(_)));
    }

    #[test]
    fn unary_is_null() {
        let mut arena = ExprArena::new();
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "x");
        let inner = arena.push(IrExpr::VarRef(VarId(0)));
        let id = arena.push(IrExpr::UnaryOp {
            op: UnaryOpKind::IsNull,
            expr: inner,
        });
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        assert!(matches!(result, DfExpr::IsNull(_)));
    }

    #[test]
    fn unary_is_not_null() {
        let mut arena = ExprArena::new();
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "x");
        let inner = arena.push(IrExpr::VarRef(VarId(0)));
        let id = arena.push(IrExpr::UnaryOp {
            op: UnaryOpKind::IsNotNull,
            expr: inner,
        });
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        assert!(matches!(result, DfExpr::IsNotNull(_)));
    }

    // -----------------------------------------------------------------------
    // BinaryOp tests
    // -----------------------------------------------------------------------

    #[test]
    fn binary_eq() {
        let mut arena = ExprArena::new();
        let l = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
        let r = arena.push(IrExpr::Literal(IrLiteral::Int(2)));
        let id = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Eq,
            left: l,
            right: r,
        });
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        // `=` lowers to the type-tolerant `cypher_eq` UDF (ADR 0009), not a
        // native `BinaryExpr` (which would plan-error on mismatched types).
        let DfExpr::ScalarFunction(sf) = result else {
            panic!("expected a cypher_eq scalar-function call, got {result:?}");
        };
        assert_eq!(sf.func.name(), "cypher_eq");
        assert_eq!(sf.args.len(), 2);
    }

    #[test]
    fn binary_in_list() {
        let mut arena = ExprArena::new();
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "x");
        let l = arena.push(IrExpr::VarRef(VarId(0)));
        let one = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
        let two = arena.push(IrExpr::Literal(IrLiteral::Int(2)));
        let list = arena.push(IrExpr::ListLiteral(vec![one, two]));
        let id = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::In,
            left: l,
            right: list,
        });
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        // Cypher `IN` lowers to the structural three-valued `cypher_in` UDF
        // (ADR 0011), not DataFusion's `in_list` (which treats the whole list as a
        // single element and type-errors on `3 IN ([1,2,3])`).
        let DfExpr::ScalarFunction(sf) = result else {
            panic!("expected a cypher_in scalar-function call, got {result:?}");
        };
        assert_eq!(sf.func.name(), "cypher_in");
        assert_eq!(sf.args.len(), 2);
    }

    fn label_membership_expr(arena: &mut ExprArena, left: IrExpr, node_var: VarId) -> ExprId {
        let left = arena.push(left);
        let node = arena.push(IrExpr::VarRef(node_var));
        let labels = arena.push(IrExpr::FunctionCall {
            name: "labels".into(),
            args: vec![node],
        });
        arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::In,
            left,
            right: labels,
        })
    }

    fn label_lowerer<'a>(arena: &'a ExprArena, var_map: &'a VarMap) -> ExprLowerer<'a> {
        ExprLowerer::with_prop_names_and_nodes(
            arena,
            var_map,
            HashMap::new(),
            HashMap::from([(0, NodeShape { prop_names: vec![] })]),
            HashMap::from([(7, "Known".to_owned())]),
            false,
        )
    }

    #[test]
    fn known_literal_in_labels_lowers_to_direct_type_id_membership() {
        let mut arena = ExprArena::new();
        let id = label_membership_expr(
            &mut arena,
            IrExpr::Literal(IrLiteral::Str("Known".into())),
            VarId(0),
        );
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "var_0");

        let result = label_lowerer(&arena, &vm).lower(id).unwrap();
        let DfExpr::ScalarFunction(sf) = &result else {
            panic!("expected array_has scalar function, got {result:?}");
        };
        assert_eq!(sf.func.name(), "array_has");
        let rendered = result.to_string();
        assert!(rendered.contains("var_0.type_ids"));
        assert!(rendered.contains("UInt32(7)"));
        assert!(!rendered.contains("cypher_in"));
        assert!(!rendered.contains("array_concat"));
    }

    #[test]
    fn unknown_literal_in_labels_retains_generic_membership() {
        let mut arena = ExprArena::new();
        let id = label_membership_expr(
            &mut arena,
            IrExpr::Literal(IrLiteral::Str("Unknown".into())),
            VarId(0),
        );
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "var_0");

        let result = label_lowerer(&arena, &vm).lower(id).unwrap();
        let DfExpr::ScalarFunction(sf) = result else {
            panic!("expected cypher_in scalar function");
        };
        assert_eq!(sf.func.name(), "cypher_in");
    }

    #[test]
    fn dynamic_in_labels_retains_generic_membership() {
        let mut arena = ExprArena::new();
        let id = label_membership_expr(&mut arena, IrExpr::VarRef(VarId(1)), VarId(0));
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "var_0");
        vm.insert(VarId(1), "label_name");

        let result = label_lowerer(&arena, &vm).lower(id).unwrap();
        let DfExpr::ScalarFunction(sf) = result else {
            panic!("expected cypher_in scalar function");
        };
        assert_eq!(sf.func.name(), "cypher_in");
    }

    // -----------------------------------------------------------------------
    // Compound predicate: a.age > 30 AND b.name = $name
    // -----------------------------------------------------------------------

    #[test]
    fn compound_predicate() {
        let mut arena = ExprArena::new();
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "a.age"); // a.age resolved to column
        vm.insert(VarId(1), "b.name"); // b.name resolved to column

        let a_age = arena.push(IrExpr::VarRef(VarId(0)));
        let thirty = arena.push(IrExpr::Literal(IrLiteral::Int(30)));
        let gt = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Gt,
            left: a_age,
            right: thirty,
        });

        let b_name = arena.push(IrExpr::VarRef(VarId(1)));
        let param = arena.push(IrExpr::Parameter("name".into()));
        let eq = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Eq,
            left: b_name,
            right: param,
        });

        let and = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::And,
            left: gt,
            right: eq,
        });

        let result = make_lowerer(&arena, &vm).lower(and).unwrap();
        assert!(matches!(result, DfExpr::ScalarFunction(_)));
        if let DfExpr::ScalarFunction(sf) = result {
            assert_eq!(sf.func.name(), "cypher_and");
        }
    }

    #[test]
    fn xor_chain_lowering_has_one_udf_per_source_operator() {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};

        let lower_and_count = |operands: usize| {
            let mut arena = ExprArena::new();
            let mut root = arena.push(IrExpr::Literal(IrLiteral::Bool(true)));
            for index in 1..operands {
                let right = match index % 3 {
                    0 => IrLiteral::Bool(true),
                    1 => IrLiteral::Bool(false),
                    _ => IrLiteral::Null,
                };
                let right = arena.push(IrExpr::Literal(right));
                root = arena.push(IrExpr::BinaryOp {
                    op: BinaryOpKind::Xor,
                    left: root,
                    right,
                });
            }

            let lowered = make_lowerer(&arena, &VarMap::new())
                .lower(root)
                .expect("XOR chain lowers");
            let mut udf_count = 0;
            lowered
                .apply(|expr| {
                    if matches!(
                        expr,
                        DfExpr::ScalarFunction(function)
                            if function.func.name() == "cypher_xor"
                    ) {
                        udf_count += 1;
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .expect("expression traversal succeeds");
            udf_count
        };

        let eleven = lower_and_count(11);
        let twenty_two = lower_and_count(22);
        assert_eq!(eleven, 10);
        assert_eq!(twenty_two, 21);
        assert!(
            twenty_two <= eleven * 3,
            "doubling operands must keep deterministic lowering work within 3x"
        );
    }

    #[test]
    fn cypher_xor_implements_three_valued_truth_table() {
        use datafusion::arrow::array::{Array, BooleanArray};
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        let left = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            None,
            None,
            None,
        ]);
        let right = BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
        ]);
        let expected = [
            Some(false),
            Some(true),
            None,
            Some(true),
            Some(false),
            None,
            None,
            None,
            None,
        ];
        let field = Arc::new(Field::new("value", DataType::Boolean, true));
        let arguments = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(left)),
                ColumnarValue::Array(Arc::new(right)),
            ],
            arg_fields: vec![Arc::clone(&field), field],
            number_rows: expected.len(),
            return_field: Arc::new(Field::new("xor", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = CypherBoolOp::new(CypherBoolOpKind::Xor)
            .invoke_with_args(arguments)
            .expect("XOR evaluates");
        let ColumnarValue::Array(result) = result else {
            panic!("array inputs must produce an array")
        };
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("XOR result is boolean");

        for (index, expected) in expected.into_iter().enumerate() {
            let actual = (!result.is_null(index)).then(|| result.value(index));
            assert_eq!(actual, expected, "truth-table row {index}");
        }
    }

    // -----------------------------------------------------------------------
    // Function call tests
    // -----------------------------------------------------------------------

    #[test]
    fn function_call_to_upper() {
        let mut arena = ExprArena::new();
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "n.name");
        let arg = arena.push(IrExpr::VarRef(VarId(0)));
        let id = arena.push(IrExpr::FunctionCall {
            name: "toUpper".into(),
            args: vec![arg],
        });
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        // upper() produces a ScalarFunction expr
        assert!(matches!(result, DfExpr::ScalarFunction(_)));
    }

    #[test]
    fn function_call_unknown_returns_error() {
        let mut arena = ExprArena::new();
        let id = arena.push(IrExpr::FunctionCall {
            name: "unknownFn".into(),
            args: vec![],
        });
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id);
        assert!(matches!(result, Err(LoweringError::UnknownFunction(_))));
    }

    // -----------------------------------------------------------------------
    // Relationship-list access lowering (#743)
    // -----------------------------------------------------------------------

    /// Lower `fn_name(VarRef("r"), <int args>)` over a var `r` and return the
    /// rendered DataFusion expression string.
    fn lower_rel_fn(name: &str, int_args: &[i64]) -> String {
        let mut arena = ExprArena::new();
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "var_1.rels");
        let mut args = vec![arena.push(IrExpr::VarRef(VarId(0)))];
        for &n in int_args {
            args.push(arena.push(IrExpr::Literal(IrLiteral::Int(n))));
        }
        let id = arena.push(IrExpr::FunctionCall {
            name: name.into(),
            args,
        });
        let expr = make_lowerer(&arena, &vm).lower(id).expect("lower");
        format!("{expr}")
    }

    #[test]
    fn subscript_lowers_to_cypher_value_access() {
        // r[0] routes through Cypher's runtime subscript UDF so unknown and
        // parameterized list/map containers get Cypher error/null semantics.
        let s = lower_rel_fn("_subscript", &[0]);
        assert!(s.contains("cypher_value_access"), "got {s}");
        assert!(s.contains("var_1.rels"), "got {s}");
    }

    #[test]
    fn head_and_last_lower_to_array_element() {
        assert!(lower_rel_fn("head", &[]).contains("array_element"));
        assert!(lower_rel_fn("last", &[]).contains("array_element"));
    }

    #[test]
    fn slice_lowers_to_array_slice() {
        // r[0..2] → array_slice(var_1.rels, begin, end)
        let s = lower_rel_fn("_slice", &[0, 2]);
        assert!(s.contains("array_slice"), "got {s}");
        assert!(s.contains("var_1.rels"), "got {s}");
    }

    #[test]
    fn slice_with_null_bounds_uses_array_length() {
        // r[..2]: start is Null → begin defaults to 1 (no array_length needed).
        // r[1..]: end is Null → end defaults to array_length(list).
        let mut arena = ExprArena::new();
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "var_1.rels");
        let list = arena.push(IrExpr::VarRef(VarId(0)));
        let start = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
        let end_null = arena.push(IrExpr::Literal(IrLiteral::Null));
        let id = arena.push(IrExpr::FunctionCall {
            name: "_slice".into(),
            args: vec![list, start, end_null],
        });
        let expr = make_lowerer(&arena, &vm).lower(id).expect("lower");
        let s = format!("{expr}");
        assert!(s.contains("array_slice"), "got {s}");
        assert!(
            s.contains("array_length"),
            "an unbounded end must default to array_length: {s}"
        );
    }

    #[test]
    fn type_of_element_lowers_to_runtime_graph_metadata_dispatch() {
        let mut arena = ExprArena::new();
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "var_1.rels");
        let list = arena.push(IrExpr::VarRef(VarId(0)));
        let idx = arena.push(IrExpr::Literal(IrLiteral::Int(0)));
        let elem = arena.push(IrExpr::FunctionCall {
            name: "_subscript".into(),
            args: vec![list, idx],
        });
        let id = arena.push(IrExpr::FunctionCall {
            name: "type".into(),
            args: vec![elem],
        });
        let expr = make_lowerer(&arena, &vm).lower(id).expect("lower");
        let s = format!("{expr}");
        assert!(
            s.contains("cypher_relationship_type"),
            "must dispatch graph metadata by runtime value: {s}"
        );
        assert!(
            s.contains("cypher_value_access"),
            "over the indexed element: {s}"
        );
    }

    fn invoke_graph_metadata(
        kind: GraphMetadataKind,
        value: ScalarValue,
    ) -> datafusion::error::Result<ScalarValue> {
        use datafusion::config::ConfigOptions;

        let udf = CypherGraphMetadata::new(kind);
        let return_type = udf.return_type(&[])?;
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(value.clone())],
            arg_fields: vec![Arc::new(Field::new("value", value.data_type(), true))],
            number_rows: 1,
            return_field: Arc::new(Field::new("metadata", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        match result {
            ColumnarValue::Array(array) => ScalarValue::try_from_array(&array, 0),
            ColumnarValue::Scalar(value) => Ok(value),
        }
    }

    #[test]
    fn graph_metadata_runtime_dispatch_validates_entity_kind() {
        use datafusion::arrow::array::{Int64Array, StringArray, StructArray};
        use datafusion::arrow::datatypes::Fields;

        let labels = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Utf8(Some("Person".into()))],
            &DataType::Utf8,
            true,
        ));
        let node = ScalarValue::Struct(Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("node_uuid", DataType::Int64, false),
                Field::new("labels", labels.data_type(), true),
                Field::new("rel_type", DataType::Utf8, true),
            ]),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                match &labels {
                    ScalarValue::List(array) => Arc::clone(array) as _,
                    _ => unreachable!(),
                },
                Arc::new(StringArray::from(vec![Some("property, not metadata")])),
            ],
            None,
        )));
        let relationship = ScalarValue::Struct(Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("edge_uuid", DataType::Int64, false),
                Field::new("rel_type", DataType::Utf8, false),
            ]),
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec!["KNOWS"])),
            ],
            None,
        )));

        let actual_labels =
            invoke_graph_metadata(GraphMetadataKind::Labels, node.clone()).expect("labels(node)");
        assert_eq!(actual_labels, labels);
        assert_eq!(
            invoke_graph_metadata(GraphMetadataKind::RelationshipType, relationship.clone())
                .expect("type(relationship)"),
            ScalarValue::Utf8(Some("KNOWS".into()))
        );
        assert!(invoke_graph_metadata(GraphMetadataKind::RelationshipType, node).is_err());
        assert!(invoke_graph_metadata(GraphMetadataKind::Labels, relationship).is_err());
        assert!(
            invoke_graph_metadata(GraphMetadataKind::Labels, ScalarValue::Null)
                .expect("labels(null)")
                .is_null()
        );

        let colliding_map = ScalarValue::Struct(Arc::new(StructArray::new(
            Fields::from(vec![Field::new("labels", labels.data_type(), true)]),
            vec![match labels {
                ScalarValue::List(array) => array as _,
                _ => unreachable!(),
            }],
            None,
        )));
        assert!(invoke_graph_metadata(GraphMetadataKind::Labels, colliding_map).is_err());
    }

    #[test]
    fn size_lowers_to_cypher_size_udf() {
        let mut arena = ExprArena::new();
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "var_1.rels");
        let list = arena.push(IrExpr::VarRef(VarId(0)));
        let id = arena.push(IrExpr::FunctionCall {
            name: "size".into(),
            args: vec![list],
        });
        let expr = make_lowerer(&arena, &vm).lower(id).expect("lower");
        assert!(format!("{expr}").contains("cypher_size"), "got {expr}");
    }

    #[test]
    fn cypher_in_decodes_tagged_list_rhs() {
        use datafusion::arrow::array::{Array, BooleanArray};
        use datafusion::scalar::ScalarValue as S;
        use std::sync::Arc;

        let lhs = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
        ]));
        let rhs_values = vec![
            S::List(S::new_list(
                &[S::Boolean(Some(true))],
                &DataType::Boolean,
                true,
            )),
            S::List(S::new_list(
                &[S::Boolean(Some(false))],
                &DataType::Boolean,
                true,
            )),
            S::List(S::new_list(&[S::Boolean(None)], &DataType::Boolean, true)),
            S::List(S::new_list(&[], &DataType::Boolean, true)),
        ];
        let depth = rhs_values.iter().filter_map(het_depth).max().unwrap();
        let rhs = Arc::new(build_het_struct(&rhs_values, depth).expect("tagged RHS"));
        let out = invoke_cypher_in(lhs, rhs);
        let bools = out.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert!(bools.value(0), "true IN [true]");
        assert!(!bools.value(1), "true IN [false]");
        assert!(bools.is_null(2), "true IN [null] -> null");
        assert!(!bools.value(3), "true IN []");
    }

    fn invoke_cypher_in(
        lhs: datafusion::arrow::array::ArrayRef,
        rhs: datafusion::arrow::array::ArrayRef,
    ) -> datafusion::arrow::array::ArrayRef {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        let n = lhs.len();
        let lhs_field = Arc::new(Field::new("lhs", lhs.data_type().clone(), true));
        let rhs_field = Arc::new(Field::new("rhs", rhs.data_type().clone(), true));
        let ret = Arc::new(Field::new("in", DataType::Boolean, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)],
            arg_fields: vec![lhs_field, rhs_field],
            number_rows: n,
            return_field: ret,
            config_options: Arc::new(ConfigOptions::default()),
        };
        match CypherIn::new().invoke_with_args(args).unwrap() {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(n).unwrap(),
        }
    }

    // -----------------------------------------------------------------------
    // cypher_size UDF — runtime type dispatch (#743)
    // -----------------------------------------------------------------------

    #[test]
    fn cypher_size_counts_list_elements() {
        use datafusion::arrow::array::{Int64Array, ListArray};
        use datafusion::arrow::datatypes::Int32Type;

        // Two list rows: [10,20,30] and [].
        let arr = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(10), Some(20), Some(30)]),
            Some(vec![]),
        ]);
        let out = invoke_cypher_size(std::sync::Arc::new(arr));
        let counts = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(counts.value(0), 3);
        assert_eq!(counts.value(1), 0);
    }

    #[test]
    fn cypher_size_counts_string_chars() {
        use datafusion::arrow::array::{Array, Int64Array, StringArray};

        let arr = StringArray::from(vec![Some("abc"), Some(""), None]);
        let out = invoke_cypher_size(std::sync::Arc::new(arr));
        let counts = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(counts.value(0), 3);
        assert_eq!(counts.value(1), 0);
        assert!(counts.is_null(2), "null string → null size");
    }

    #[test]
    fn cypher_size_counts_het_tagged_elements() {
        use datafusion::arrow::array::{Array, Int64Array};
        use datafusion::scalar::ScalarValue as S;

        // The four element shapes a quantifier loop variable can take over
        // `[[1, 2, 3], 'ab', true, null]`: a list payload counts its elements,
        // a string its bytes, and a non-list/string or null element is null.
        let scalars = vec![
            S::List(S::new_list(
                &[S::Int64(Some(1)), S::Int64(Some(2)), S::Int64(Some(3))],
                &DataType::Int64,
                true,
            )),
            S::Utf8(Some("ab".to_owned())),
            S::Boolean(Some(true)),
            S::Null,
        ];
        let depth = scalars.iter().filter_map(het_depth).max().unwrap();
        let elems = build_het_struct(&scalars, depth).expect("build tagged struct");
        let out = invoke_cypher_size(std::sync::Arc::new(elems));
        let counts = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(counts.value(0), 3, "tag-4 list element → element count");
        assert_eq!(counts.value(1), 2, "tag-2 string element → char count");
        assert!(counts.is_null(2), "non-list/string element → null");
        assert!(counts.is_null(3), "null element → null");
    }

    /// Invoke the `cypher_size` UDF over a single-column array and return the
    /// result array.
    fn invoke_cypher_size(
        array: datafusion::arrow::array::ArrayRef,
    ) -> datafusion::arrow::array::ArrayRef {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        let n = array.len();
        let field = Arc::new(Field::new("x", array.data_type().clone(), true));
        let ret = Arc::new(Field::new("size", DataType::Int64, true));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(array)],
            arg_fields: vec![field],
            number_rows: n,
            return_field: ret,
            config_options: Arc::new(ConfigOptions::default()),
        };
        match CypherSize::new().invoke_with_args(args).unwrap() {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(n).unwrap(),
        }
    }

    // -----------------------------------------------------------------------
    // cypher_path_nodes UDF — traversal node sequence (#754)
    // -----------------------------------------------------------------------

    #[test]
    fn path_nodes_lowers_to_udf_over_node_uuid() {
        let mut arena = ExprArena::new();
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "var_0"); // start node — bare scan qualifier
        vm.insert(VarId(1), "var_1.rels"); // var-length edge list
        let start = arena.push(IrExpr::VarRef(VarId(0)));
        let rels = arena.push(IrExpr::VarRef(VarId(1)));
        let id = arena.push(IrExpr::FunctionCall {
            name: "_path_nodes".into(),
            args: vec![start, rels],
        });
        let expr = make_lowerer(&arena, &vm).lower(id).expect("lower");
        let s = format!("{expr}");
        assert!(s.contains("cypher_path_nodes"), "got {s}");
        assert!(
            s.contains("var_0.node_uuid"),
            "seed is the uuid column: {s}"
        );
    }

    /// A 16-byte uuid stand-in: byte `b` repeated.
    fn uuid16(b: u8) -> Vec<u8> {
        vec![b; 16]
    }

    /// Build a `List<Struct{src_uuid, dst_uuid}>` edge-list column. Each edge
    /// is `(src_byte, dst_byte)` in **storage** orientation; `None` rows are
    /// null lists.
    fn edge_list(rows: &[Option<&[(u8, u8)]>]) -> datafusion::arrow::array::ArrayRef {
        use datafusion::arrow::array::{FixedSizeBinaryBuilder, ListBuilder, StructBuilder};
        use datafusion::arrow::datatypes::Field;

        let fields: datafusion::arrow::datatypes::Fields = vec![
            Field::new("src_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("dst_uuid", DataType::FixedSizeBinary(16), false),
        ]
        .into();
        let mut b = ListBuilder::new(StructBuilder::new(
            fields,
            vec![
                Box::new(FixedSizeBinaryBuilder::new(16)),
                Box::new(FixedSizeBinaryBuilder::new(16)),
            ],
        ));
        for row in rows {
            let Some(edges) = row else {
                b.append_null();
                continue;
            };
            for (src, dst) in *edges {
                b.values()
                    .field_builder::<FixedSizeBinaryBuilder>(0)
                    .unwrap()
                    .append_value(uuid16(*src))
                    .unwrap();
                b.values()
                    .field_builder::<FixedSizeBinaryBuilder>(1)
                    .unwrap()
                    .append_value(uuid16(*dst))
                    .unwrap();
                b.values().append(true);
            }
            b.append(true);
        }
        std::sync::Arc::new(b.finish())
    }

    /// Build the seed-uuid column; `None` entries are null seeds.
    fn seed_uuids(vals: &[Option<u8>]) -> datafusion::arrow::array::ArrayRef {
        use datafusion::arrow::array::FixedSizeBinaryBuilder;
        let mut b = FixedSizeBinaryBuilder::new(16);
        for v in vals {
            match v {
                Some(x) => b.append_value(uuid16(*x)).unwrap(),
                None => b.append_null(),
            }
        }
        std::sync::Arc::new(b.finish())
    }

    fn invoke_path_nodes(
        seed: datafusion::arrow::array::ArrayRef,
        rels: datafusion::arrow::array::ArrayRef,
    ) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef> {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        let udf = CypherPathNodes::new();
        let n = seed.len();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::clone(&seed)),
                ColumnarValue::Array(Arc::clone(&rels)),
            ],
            arg_fields: vec![
                Arc::new(Field::new("seed", seed.data_type().clone(), true)),
                Arc::new(Field::new("rels", rels.data_type().clone(), true)),
            ],
            number_rows: n,
            return_field: Arc::new(Field::new("nodes", udf.return_type(&[])?, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        udf.invoke_with_args(args).map(|v| match v {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(n).unwrap(),
        })
    }

    /// Row `i` of the result as the node uuids' first bytes, or `None` for a
    /// null path.
    fn path_node_bytes(out: &datafusion::arrow::array::ArrayRef, i: usize) -> Option<Vec<u8>> {
        use datafusion::arrow::array::{Array, FixedSizeBinaryArray, ListArray, StructArray};
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        if list.is_null(i) {
            return None;
        }
        let items = list.value(i);
        let items = items.as_any().downcast_ref::<StructArray>().unwrap();
        let uuids = items.column_by_name("node_uuid").unwrap();
        let uuids = uuids
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        Some((0..uuids.len()).map(|j| uuids.value(j)[0]).collect())
    }

    #[test]
    fn path_nodes_walks_forward_chain() {
        let out = invoke_path_nodes(
            seed_uuids(&[Some(1)]),
            edge_list(&[Some(&[(1, 2), (2, 3)])]),
        )
        .unwrap();
        assert_eq!(path_node_bytes(&out, 0), Some(vec![1, 2, 3]));
    }

    #[test]
    fn path_nodes_flips_reversed_storage_orientation() {
        // Edge stored 2→1 but traversed from 1 (an `In`/`Undirected` hop):
        // the next node is the *other* endpoint, not blindly dst_uuid.
        let out = invoke_path_nodes(seed_uuids(&[Some(1)]), edge_list(&[Some(&[(2, 1)])])).unwrap();
        assert_eq!(path_node_bytes(&out, 0), Some(vec![1, 2]));
    }

    #[test]
    fn path_nodes_mixed_orientation_walk() {
        // 1 →(stored 1→2)→ 2 →(stored 3→2, traversed against storage)→ 3.
        let out = invoke_path_nodes(
            seed_uuids(&[Some(1)]),
            edge_list(&[Some(&[(1, 2), (3, 2)])]),
        )
        .unwrap();
        assert_eq!(path_node_bytes(&out, 0), Some(vec![1, 2, 3]));
    }

    #[test]
    fn path_nodes_self_loop_stays_put() {
        let out = invoke_path_nodes(seed_uuids(&[Some(1)]), edge_list(&[Some(&[(1, 1)])])).unwrap();
        assert_eq!(path_node_bytes(&out, 0), Some(vec![1, 1]));
    }

    #[test]
    fn path_nodes_zero_hop_is_seed_only() {
        let out = invoke_path_nodes(seed_uuids(&[Some(7)]), edge_list(&[Some(&[])])).unwrap();
        assert_eq!(path_node_bytes(&out, 0), Some(vec![7]));
    }

    #[test]
    fn path_nodes_null_seed_or_list_is_null() {
        // Unmatched OPTIONAL MATCH rows: null seed (row 0) or null list (row 1).
        let out = invoke_path_nodes(
            seed_uuids(&[None, Some(1)]),
            edge_list(&[Some(&[(1, 2)]), None]),
        )
        .unwrap();
        assert_eq!(path_node_bytes(&out, 0), None);
        assert_eq!(path_node_bytes(&out, 1), None);
    }

    #[test]
    fn path_nodes_disconnected_edge_errors() {
        let err = invoke_path_nodes(seed_uuids(&[Some(1)]), edge_list(&[Some(&[(5, 6)])]))
            .expect_err("an edge touching neither endpoint is a corrupt emission");
        assert!(err.to_string().contains("disconnected"), "got {err}");
    }

    #[test]
    fn path_nodes_output_matches_declared_return_type() {
        // DataFusion verifies the produced array against `return_type` at
        // execution; catch any list-field/nullability drift here first.
        let out = invoke_path_nodes(seed_uuids(&[Some(1)]), edge_list(&[Some(&[(1, 2)])])).unwrap();
        assert_eq!(
            out.data_type(),
            &CypherPathNodes::new().return_type(&[]).unwrap()
        );
    }

    /// Hydrated `cypher_path_nodes` invoke over a real topology directory (#705).
    fn invoke_hydrated_path_nodes(
        hydrate: PathNodeHydration,
        seed: datafusion::arrow::array::ArrayRef,
        rels: datafusion::arrow::array::ArrayRef,
    ) -> datafusion::error::Result<datafusion::arrow::array::ArrayRef> {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        let udf = CypherPathNodes::with_hydration(hydrate);
        let n = seed.len();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::clone(&seed)),
                ColumnarValue::Array(Arc::clone(&rels)),
            ],
            arg_fields: vec![
                Arc::new(Field::new("seed", seed.data_type().clone(), true)),
                Arc::new(Field::new("rels", rels.data_type().clone(), true)),
            ],
            number_rows: n,
            return_field: Arc::new(Field::new("nodes", udf.return_type(&[])?, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        udf.invoke_with_args(args).map(|v| match v {
            ColumnarValue::Array(a) => a,
            ColumnarValue::Scalar(s) => s.to_array_of_size(n).unwrap(),
        })
    }

    fn path_node_label_lists(
        out: &datafusion::arrow::array::ArrayRef,
        row: usize,
    ) -> Option<Vec<Vec<Option<String>>>> {
        use datafusion::arrow::array::{Array, ListArray, StringArray, StructArray};
        let list = out.as_any().downcast_ref::<ListArray>().unwrap();
        if list.is_null(row) {
            return None;
        }
        let items = list.value(row);
        let items = items.as_any().downcast_ref::<StructArray>().unwrap();
        let labels = items
            .column_by_name("labels")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        Some(
            (0..labels.len())
                .map(|i| {
                    let values = labels.value(i);
                    let strings = values.as_any().downcast_ref::<StringArray>().unwrap();
                    (0..strings.len())
                        .map(|j| (!strings.is_null(j)).then(|| strings.value(j).to_owned()))
                        .collect()
                })
                .collect(),
        )
    }

    #[test]
    fn hydrated_path_nodes_preserve_full_type_ids_labels() {
        // #705: multi-label nodes keep every catalog-resolved label from
        // authoritative `type_ids` (not the legacy primary `type_id` alone).
        use datafusion::arrow::array::FixedSizeBinaryArray;
        use datafusion::arrow::datatypes::Field;
        use graphforge_core::uuid::{new_v7, to_bytes};
        use graphforge_core::{OntologyMode, TypeId};
        use graphforge_storage::GraphWriter;

        let dir = tempfile::TempDir::new().unwrap();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, 0).unwrap();
        let multi = new_v7();
        let single = new_v7();
        let unknown_only = new_v7();
        w.create_node_with_labels(multi, &[TypeId(1), TypeId(3)])
            .unwrap();
        w.create_node_with_labels(single, &[TypeId(2)]).unwrap();
        // type_ids present but absent from the baked catalog → empty label list.
        w.create_node_with_labels(unknown_only, &[TypeId(99)])
            .unwrap();
        w.flush().unwrap();

        let multi_bytes = to_bytes(&multi);
        let single_bytes = to_bytes(&single);
        let unknown_bytes = to_bytes(&unknown_only);
        let missing_bytes = [0xABu8; 16];

        let hydrate = PathNodeHydration {
            dir: dir.path().to_path_buf(),
            labels_by_type: vec![
                (1, "Person".to_owned()),
                (2, "Company".to_owned()),
                (3, "Employee".to_owned()),
            ],
            prop_stems: vec![],
            fields: vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
            ]
            .into(),
        };

        // Zero-hop walk over four seeds: multi-label, single-label, missing
        // catalog id, and uuid absent from topology.
        let seed = std::sync::Arc::new(
            FixedSizeBinaryArray::try_from_iter(
                [multi_bytes, single_bytes, unknown_bytes, missing_bytes]
                    .iter()
                    .copied(),
            )
            .unwrap(),
        ) as datafusion::arrow::array::ArrayRef;
        let rels = edge_list(&[Some(&[]), Some(&[]), Some(&[]), Some(&[])]);
        let out = invoke_hydrated_path_nodes(hydrate, seed, rels).unwrap();
        let labels = path_node_label_lists(&out, 0).unwrap();
        assert_eq!(
            labels[0],
            vec![Some("Person".into()), Some("Employee".into())],
            "multi-label node keeps full type_ids set in catalog order"
        );
        // Remaining seeds are separate rows (one zero-hop path each).
        let labels1 = path_node_label_lists(&out, 1).unwrap();
        assert_eq!(labels1[0], vec![Some("Company".into())]);
        let labels2 = path_node_label_lists(&out, 2).unwrap();
        assert_eq!(
            labels2[0],
            Vec::<Option<String>>::new(),
            "unknown catalog ids skipped"
        );
        let labels3 = path_node_label_lists(&out, 3).unwrap();
        assert_eq!(
            labels3[0],
            vec![None],
            "missing topology row keeps a single-null labels element"
        );

        // Repeated node on a self-loop walk must repeat the full label set.
        let seed_loop = std::sync::Arc::new(
            FixedSizeBinaryArray::try_from_iter([multi_bytes].iter().copied()).unwrap(),
        ) as datafusion::arrow::array::ArrayRef;
        // Build a one-edge list with real uuids (not the byte-tag helper).
        use datafusion::arrow::array::{FixedSizeBinaryBuilder, ListBuilder, StructBuilder};
        let fields: datafusion::arrow::datatypes::Fields = vec![
            Field::new("src_uuid", DataType::FixedSizeBinary(16), false),
            Field::new("dst_uuid", DataType::FixedSizeBinary(16), false),
        ]
        .into();
        let mut b = ListBuilder::new(StructBuilder::new(
            fields,
            vec![
                Box::new(FixedSizeBinaryBuilder::new(16)),
                Box::new(FixedSizeBinaryBuilder::new(16)),
            ],
        ));
        b.values()
            .field_builder::<FixedSizeBinaryBuilder>(0)
            .unwrap()
            .append_value(multi_bytes)
            .unwrap();
        b.values()
            .field_builder::<FixedSizeBinaryBuilder>(1)
            .unwrap()
            .append_value(multi_bytes)
            .unwrap();
        b.values().append(true);
        b.append(true);
        let loop_rels = std::sync::Arc::new(b.finish()) as datafusion::arrow::array::ArrayRef;

        let hydrate2 = PathNodeHydration {
            dir: dir.path().to_path_buf(),
            labels_by_type: vec![
                (1, "Person".to_owned()),
                (2, "Company".to_owned()),
                (3, "Employee".to_owned()),
            ],
            prop_stems: vec![],
            fields: vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new("labels", DataType::new_list(DataType::Utf8, true), true),
            ]
            .into(),
        };
        let looped = invoke_hydrated_path_nodes(hydrate2, seed_loop, loop_rels).unwrap();
        let loop_labels = path_node_label_lists(&looped, 0).unwrap();
        assert_eq!(loop_labels.len(), 2);
        assert_eq!(loop_labels[0], loop_labels[1]);
        assert_eq!(
            loop_labels[0],
            vec![Some("Person".into()), Some("Employee".into())]
        );
    }

    // -----------------------------------------------------------------------
    // Parameter test
    // -----------------------------------------------------------------------

    #[test]
    fn parameter_produces_placeholder() {
        let mut arena = ExprArena::new();
        let id = arena.push(IrExpr::Parameter("eid".into()));
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        if let DfExpr::Placeholder(p) = result {
            // The `$` is reattached so DataFusion's named-param binding resolves.
            assert_eq!(p.id, "$eid");
        } else {
            panic!("expected Placeholder, got {result:?}");
        }
    }

    // -----------------------------------------------------------------------
    // List literal tests (#714)
    // -----------------------------------------------------------------------

    #[test]
    fn list_literal_of_ints_folds_to_scalar_list() {
        use datafusion::arrow::array::Array;
        // [1, 2, 3] → a single ScalarValue::List literal with 3 Int64 elements.
        let mut arena = ExprArena::new();
        let e1 = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
        let e2 = arena.push(IrExpr::Literal(IrLiteral::Int(2)));
        let e3 = arena.push(IrExpr::Literal(IrLiteral::Int(3)));
        let id = arena.push(IrExpr::ListLiteral(vec![e1, e2, e3]));
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        let DfExpr::Literal(ScalarValue::List(arr), _) = result else {
            panic!("expected a ScalarValue::List literal, got {result:?}");
        };
        // One list row holding 3 elements.
        assert_eq!(arr.len(), 1);
        assert_eq!(arr.value(0).len(), 3);
        assert_eq!(arr.value(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn empty_list_literal_folds_to_empty_int64_list() {
        use datafusion::arrow::array::Array;
        let mut arena = ExprArena::new();
        let id = arena.push(IrExpr::ListLiteral(vec![]));
        let vm = VarMap::new();
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        let DfExpr::Literal(ScalarValue::List(arr), _) = result else {
            panic!("expected an empty ScalarValue::List literal, got {result:?}");
        };
        assert_eq!(arr.len(), 1);
        assert_eq!(arr.value(0).len(), 0, "no elements");
    }

    #[test]
    fn list_literal_with_expression_element_uses_make_array() {
        // [n.age, 1] has a non-constant element, so it lowers to make_array(...).
        let mut arena = ExprArena::new();
        let var = arena.push(IrExpr::VarRef(VarId(0)));
        let age = arena.push(IrExpr::PropertyAccess {
            base: var,
            prop: PropId(0),
        });
        let one = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
        let id = arena.push(IrExpr::ListLiteral(vec![age, one]));
        let mut vm = VarMap::new();
        vm.insert(VarId(0), "var_0");
        let result = make_lowerer(&arena, &vm).lower(id).unwrap();
        let DfExpr::ScalarFunction(f) = result else {
            panic!("expected a make_array ScalarFunction, got {result:?}");
        };
        assert_eq!(f.name(), "make_array");
        assert_eq!(f.args.len(), 2);
    }

    // -----------------------------------------------------------------------
    // scalar_to_ir_literal (#791)
    // -----------------------------------------------------------------------

    #[test]
    fn scalar_to_ir_literal_round_trips_each_kind() {
        // Each IrLiteral → ScalarValue → IrLiteral is identity for the canonical
        // widths `ir_literal_to_scalar` emits.
        for lit in [
            IrLiteral::Bool(true),
            IrLiteral::Int(42),
            IrLiteral::Float(1.5),
            IrLiteral::Str("hi".into()),
            IrLiteral::Duration {
                months: 14,
                days: 3,
                seconds: 5,
                nanos: 1000,
            },
            IrLiteral::DateTime(1_700_000_000_000_000),
        ] {
            let scalar = ir_literal_to_scalar(&lit);
            assert_eq!(scalar_to_ir_literal(&scalar).unwrap(), lit);
        }
    }

    #[test]
    fn scalar_to_ir_literal_rejects_graph_identity_as_a_property_value() {
        let scalar = ir_literal_to_scalar(&IrLiteral::Uuid([0x42; 16]));
        assert!(matches!(
            scalar_to_ir_literal(&scalar),
            Err(LoweringError::InvalidType(message))
                if message == "UUID values cannot be stored as graph properties"
        ));
    }

    #[test]
    fn scalar_to_ir_literal_null_variants_map_to_null() {
        assert_eq!(
            scalar_to_ir_literal(&ScalarValue::Null).unwrap(),
            IrLiteral::Null
        );
        assert_eq!(
            scalar_to_ir_literal(&ScalarValue::Int64(None)).unwrap(),
            IrLiteral::Null
        );
    }

    #[test]
    fn scalar_to_ir_literal_widens_smaller_ints() {
        assert_eq!(
            scalar_to_ir_literal(&ScalarValue::Int32(Some(7))).unwrap(),
            IrLiteral::Int(7)
        );
        assert_eq!(
            scalar_to_ir_literal(&ScalarValue::UInt8(Some(255))).unwrap(),
            IrLiteral::Int(255)
        );
    }

    #[test]
    fn scalar_to_ir_literal_normalizes_all_native_widths_and_rejects_overflow() {
        let cases = [
            (ScalarValue::Int8(Some(-8)), IrLiteral::Int(-8)),
            (ScalarValue::Int16(Some(-16)), IrLiteral::Int(-16)),
            (ScalarValue::UInt16(Some(16)), IrLiteral::Int(16)),
            (ScalarValue::UInt32(Some(32)), IrLiteral::Int(32)),
            (ScalarValue::UInt64(Some(64)), IrLiteral::Int(64)),
            (ScalarValue::Float32(Some(1.25)), IrLiteral::Float(1.25)),
            (
                ScalarValue::LargeUtf8(Some("large".into())),
                IrLiteral::Str("large".into()),
            ),
            (
                ScalarValue::Utf8View(Some("view".into())),
                IrLiteral::Str("view".into()),
            ),
            (
                ScalarValue::TimestampSecond(Some(2), None),
                IrLiteral::DateTime(2_000_000),
            ),
            (
                ScalarValue::TimestampMillisecond(Some(3), None),
                IrLiteral::DateTime(3_000),
            ),
            (
                ScalarValue::TimestampNanosecond(Some(4_000), None),
                IrLiteral::DateTime(4),
            ),
            (ScalarValue::Time64Nanosecond(Some(5)), IrLiteral::Time(5)),
        ];
        for (scalar, expected) in cases {
            assert_eq!(scalar_to_ir_literal(&scalar).unwrap(), expected);
        }
        assert!(matches!(
            scalar_to_ir_literal(&ScalarValue::UInt64(Some(u64::MAX))),
            Err(LoweringError::UnsupportedExpr(message)) if message.contains("exceeds the i64 range")
        ));
        assert!(matches!(
            scalar_to_ir_literal(&ScalarValue::Binary(Some(vec![1, 2]))),
            Err(LoweringError::InvalidType(message)) if message.contains("invalid property type")
        ));
    }

    #[test]
    fn dynamic_access_helpers_cover_null_bounds_types_and_schema_errors() {
        use datafusion::arrow::datatypes::{Field, Fields};

        for (scalar, expected) in [
            (ScalarValue::Int8(Some(-1)), Some(-1)),
            (ScalarValue::Int16(Some(2)), Some(2)),
            (ScalarValue::Int32(Some(3)), Some(3)),
            (ScalarValue::Int64(Some(4)), Some(4)),
            (ScalarValue::UInt8(Some(5)), Some(5)),
            (ScalarValue::UInt16(Some(6)), Some(6)),
            (ScalarValue::UInt32(Some(7)), Some(7)),
            (ScalarValue::UInt64(Some(8)), Some(8)),
            (ScalarValue::Null, None),
        ] {
            assert_eq!(scalar_list_index(&scalar).unwrap(), expected);
        }
        assert!(scalar_list_index(&ScalarValue::UInt64(Some(u64::MAX))).is_err());
        assert!(scalar_list_index(&ScalarValue::Utf8(Some("one".into()))).is_err());
        assert_eq!(scalar_access_key(&ScalarValue::Null).unwrap(), None);
        assert_eq!(
            scalar_access_key(&ScalarValue::LargeUtf8(Some("key".into()))).unwrap(),
            Some("key".into())
        );
        assert!(scalar_access_key(&ScalarValue::Int64(Some(1))).is_err());

        let homogeneous = Fields::from(vec![
            Field::new("a", DataType::Null, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, false),
        ]);
        assert_eq!(
            common_struct_field_type(&homogeneous).unwrap(),
            DataType::Int64
        );
        let mixed = Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        assert!(common_struct_field_type(&mixed).is_err());

        for dtype in [
            DataType::Struct(Fields::empty()),
            DataType::Struct(Fields::from(vec![Field::new(
                "__het_map",
                DataType::Utf8,
                true,
            )])),
            DataType::Struct(Fields::from(vec![Field::new(
                "__het_map",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            )])),
        ] {
            assert!(het_value_access_return_type(&dtype).is_err());
        }
    }

    #[test]
    fn heterogeneous_map_access_returns_exact_values_and_rejects_non_maps() {
        use datafusion::arrow::array::{ArrayRef, StringArray};
        use datafusion::scalar::ScalarValue as S;

        let map = const_map_scalar(&[
            ("answer".to_owned(), S::Int64(Some(42))),
            ("empty".to_owned(), S::Int64(None)),
        ])
        .expect("map scalar");
        let encoded = build_het_struct(&[map], 1).expect("tagged map");
        let return_type =
            het_value_access_return_type(encoded.data_type()).expect("map value type");
        let null_value = S::try_from(&return_type).expect("typed null");
        let keys = |key: Option<&str>| -> ArrayRef { Arc::new(StringArray::from(vec![key])) };

        let found = het_map_access_value(&encoded, &keys(Some("answer")), 0, &null_value)
            .expect("existing map key");
        assert_eq!(decode_het(&found), Some(S::Int64(Some(42))));

        let stored_null = het_map_access_value(&encoded, &keys(Some("empty")), 0, &null_value)
            .expect("stored null");
        assert_eq!(decode_het(&stored_null), Some(S::Null));
        assert_eq!(
            het_map_access_value(&encoded, &keys(Some("missing")), 0, &null_value)
                .expect("missing key"),
            null_value
        );
        assert_eq!(
            het_map_access_value(&encoded, &keys(None), 0, &null_value).expect("null key"),
            null_value
        );

        let non_map = build_het_struct(&[S::Int64(Some(7))], 0).expect("tagged integer");
        let error = het_map_access_value(&non_map, &keys(Some("answer")), 0, &null_value)
            .expect_err("a tagged integer is not dynamically property-readable");
        assert_eq!(
            error.to_string(),
            "Execution error: invalid argument type: dynamic value access requires a map"
        );
    }

    #[test]
    fn dynamic_struct_access_observes_missing_null_type_and_row_null_semantics() {
        use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray, StructArray};
        use datafusion::arrow::buffer::NullBuffer;
        use datafusion::arrow::datatypes::{Field, Fields};
        use datafusion::config::ConfigOptions;

        let values: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("score", DataType::Int64, true)]),
            vec![Arc::new(Int64Array::from(vec![Some(9), None, Some(11)]))],
            Some(NullBuffer::from(vec![true, true, false])),
        ));
        let invoke = |keys: ArrayRef| -> datafusion::error::Result<ArrayRef> {
            let udf = CypherValueAccess::new();
            let result = udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::clone(&values)),
                    ColumnarValue::Array(keys),
                ],
                arg_fields: vec![
                    Arc::new(Field::new("value", values.data_type().clone(), true)),
                    Arc::new(Field::new("key", DataType::Utf8, true)),
                ],
                number_rows: 3,
                return_field: Arc::new(Field::new("out", DataType::Int64, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })?;
            match result {
                ColumnarValue::Array(array) => Ok(array),
                ColumnarValue::Scalar(value) => value.to_array_of_size(3),
            }
        };

        let result = invoke(Arc::new(StringArray::from(vec![
            Some("score"),
            Some("missing"),
            Some("score"),
        ])))
        .expect("dynamic struct access");
        let result = result.as_any().downcast_ref::<Int64Array>().expect("Int64");
        assert_eq!(result.value(0), 9);
        assert!(result.is_null(1), "an absent property is null");
        assert!(result.is_null(2), "a null graph-element row is null");

        let bad_keys: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let error = invoke(bad_keys).expect_err("numeric property key");
        assert!(
            error
                .to_string()
                .contains("dynamic map/property access key must be a string"),
            "{error}"
        );
    }

    #[test]
    fn temporal_accessor_type_matrix_distinguishes_values_from_properties() {
        // Date and duration dispatch through their dedicated lowering paths.
        assert!(!temporal_accessor_valid(&DataType::Date32, "year"));
        assert!(!temporal_accessor_valid(&DataType::Date32, "timezone"));
        assert!(temporal_accessor_valid(
            &ScalarValue::Time64Nanosecond(None).data_type(),
            "nanosecond"
        ));
        assert!(!temporal_accessor_valid(
            &duration_scalar(None).data_type(),
            "monthsOfYear"
        ));
        assert!(temporal_accessor_valid(
            &localdatetime_scalar(None).data_type(),
            "year"
        ));
        assert!(temporal_accessor_valid(
            &datetime_scalar(None).data_type(),
            "offsetSeconds"
        ));
        assert!(!temporal_accessor_valid(&DataType::Utf8, "year"));
        assert!(!temporal_accessor_valid(&DataType::Int64, "day"));
    }

    #[test]
    fn scalar_to_ir_literal_round_trips_a_list() {
        // A homogeneous list now stores (#1006): scalar List → IrLiteral::List
        // and back, element-wise — including a list of typed temporals.
        for lit in [
            IrLiteral::List(vec![IrLiteral::Int(1), IrLiteral::Int(2)]),
            IrLiteral::List(vec![IrLiteral::Date(5428), IrLiteral::Date(5429)]),
        ] {
            let scalar = ir_literal_to_scalar(&lit);
            assert_eq!(scalar_to_ir_literal(&scalar).unwrap(), lit);
        }
    }

    #[test]
    fn zoned_temporal_order_keys_compare_absolute_instants() {
        let hour = 3_600_000_000_000_i64;
        let early_time = time_scalar(Some((12 * hour + 35 * 60_000_000_000, 5 * 3_600)));
        let late_time = time_scalar(Some((10 * hour + 35 * 60_000_000_000, -8 * 3_600)));
        assert!(cypher_order_key(&early_time) < cypher_order_key(&late_time));

        let earlier_datetime = datetime_scalar(Some((5_000, 12 * hour, 3_600, None)));
        let later_datetime = datetime_scalar(Some((5_000, 12 * hour, 0, None)));
        assert!(cypher_order_key(&earlier_datetime) < cypher_order_key(&later_datetime));
    }

    #[test]
    fn zoned_temporal_structs_require_cypher_order_keys() {
        assert!(needs_cypher_order_key_type(&time_scalar(None).data_type()));
        assert!(needs_cypher_order_key_type(
            &datetime_scalar(None).data_type()
        ));
    }

    #[test]
    fn dynamic_heterogeneous_list_preserves_graph_value_payloads() {
        use datafusion::arrow::array::{Int64Array, StructArray};
        use datafusion::arrow::datatypes::{Field, Fields};
        use datafusion::config::ConfigOptions;

        let node_array = StructArray::new(
            Fields::from(vec![Field::new("node_uuid", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![7]))],
            None,
        );
        let node = ScalarValue::Struct(Arc::new(node_array));
        let number = ScalarValue::Int64(Some(42));
        let arg_types = vec![node.data_type(), number.data_type()];
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(node.clone()),
                ColumnarValue::Scalar(number.clone()),
            ],
            arg_fields: arg_types
                .iter()
                .enumerate()
                .map(|(i, ty)| Arc::new(Field::new(format!("arg_{i}"), ty.clone(), true)))
                .collect(),
            number_rows: 1,
            return_field: Arc::new(Field::new("out", dynamic_het_type(&arg_types), false)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let out = match CypherDynamicHetList::new()
            .invoke_with_args(args)
            .expect("dynamic heterogeneous list")
        {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(value) => value.to_array().expect("scalar list"),
        };
        let list = out.as_any().downcast_ref::<ListArray>().expect("List");
        let values = list.value(0);
        let first = ScalarValue::try_from_array(&values, 0).expect("node element");
        let second = ScalarValue::try_from_array(&values, 1).expect("number element");
        assert_eq!(decode_het(&first), Some(node));
        assert_eq!(decode_het(&second), Some(number));
        assert!(cypher_order_key(&first).starts_with("20:node"));
        assert!(cypher_order_key(&second).starts_with("80:num"));
    }

    #[test]
    fn cypher_reverse_runtime_dispatches_strings_lists_and_type_errors() {
        use datafusion::arrow::array::{Array, LargeStringArray, ListArray};
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        let invoke = |value: ScalarValue| {
            let data_type = value.data_type();
            CypherReverse::new().invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(value)],
                arg_fields: vec![Arc::new(Field::new("value", data_type.clone(), true))],
                number_rows: 1,
                return_field: Arc::new(Field::new("out", data_type, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
        };

        let text = invoke(ScalarValue::LargeUtf8(Some("Áda".into()))).unwrap();
        let text = match text {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
        };
        let text = text.as_any().downcast_ref::<LargeStringArray>().unwrap();
        assert_eq!(text.value(0), "ad́A");

        use datafusion::arrow::array::StringArray;
        let utf8 = invoke(ScalarValue::Utf8(Some("Graph".into()))).unwrap();
        let utf8 = match utf8 {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
        };
        let utf8 = utf8.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(utf8.value(0), "hparG");

        let list = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2)),
                ScalarValue::Int64(Some(3)),
            ],
            &DataType::Int64,
            true,
        ));
        let reversed = invoke(list).unwrap();
        let reversed = match reversed {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
        };
        let reversed = reversed.as_any().downcast_ref::<ListArray>().unwrap();
        let values = reversed.value(0);
        assert_eq!(
            (0..values.len())
                .map(|row| ScalarValue::try_from_array(&values, row).unwrap())
                .collect::<Vec<_>>(),
            [
                ScalarValue::Int64(Some(3)),
                ScalarValue::Int64(Some(2)),
                ScalarValue::Int64(Some(1)),
            ]
        );

        let error = invoke(ScalarValue::Int64(Some(7))).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Error during planning: reverse() expects a string or list, got Int64"
        );
    }

    #[test]
    fn cypher_list_plus_runtime_covers_each_operand_shape() {
        use datafusion::arrow::array::{Array, ListArray};
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        let list = |values: &[i64]| {
            ScalarValue::List(ScalarValue::new_list(
                &values
                    .iter()
                    .copied()
                    .map(|value| ScalarValue::Int64(Some(value)))
                    .collect::<Vec<_>>(),
                &DataType::Int64,
                true,
            ))
        };
        let invoke = |left: ScalarValue, right: ScalarValue| {
            let udf = CypherListPlus::new();
            let types = [left.data_type(), right.data_type()];
            let return_type = udf.return_type(&types).unwrap();
            udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)],
                arg_fields: types
                    .iter()
                    .enumerate()
                    .map(|(index, data_type)| {
                        Arc::new(Field::new(format!("arg_{index}"), data_type.clone(), true))
                    })
                    .collect(),
                number_rows: 1,
                return_field: Arc::new(Field::new("out", return_type, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
        };
        let values = |result: ColumnarValue| {
            let array = match result {
                ColumnarValue::Array(array) => array,
                ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
            };
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = list.value(0);
            (0..values.len())
                .map(|row| {
                    let value = ScalarValue::try_from_array(&values, row).unwrap();
                    decode_het(&value).unwrap_or(value)
                })
                .collect::<Vec<_>>()
        };

        assert_eq!(
            values(invoke(list(&[1, 2]), list(&[3, 4])).unwrap()),
            [1, 2, 3, 4]
                .map(|value| ScalarValue::Int64(Some(value)))
                .to_vec()
        );
        assert_eq!(
            values(invoke(list(&[1, 2]), ScalarValue::Int64(Some(3))).unwrap()),
            [1, 2, 3]
                .map(|value| ScalarValue::Int64(Some(value)))
                .to_vec()
        );
        assert_eq!(
            values(invoke(ScalarValue::Int64(Some(1)), list(&[2, 3])).unwrap()),
            [1, 2, 3]
                .map(|value| ScalarValue::Int64(Some(value)))
                .to_vec()
        );
        let error = invoke(ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Execution error: list + requires at least one list operand"
        );
    }

    #[test]
    fn cypher_list_plus_executes_large_list_operands_and_null_rows() {
        use datafusion::arrow::array::{Array, ArrayRef, Int64Array, LargeListArray, ListArray};
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use datafusion::arrow::datatypes::Field;

        let large = |values: &[i64], valid: bool| {
            let values: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
            ScalarValue::LargeList(Arc::new(LargeListArray::new(
                Arc::new(Field::new("item", DataType::Int64, true)),
                OffsetBuffer::new(ScalarBuffer::from(vec![
                    0_i64,
                    i64::try_from(values.len()).unwrap(),
                ])),
                values,
                Some(NullBuffer::from(vec![valid])),
            )))
        };
        let values = |array: ArrayRef| {
            let list = array.as_any().downcast_ref::<ListArray>().expect("List");
            if list.is_null(0) {
                return None;
            }
            let values = list.value(0);
            Some(
                (0..values.len())
                    .map(|row| {
                        let value = ScalarValue::try_from_array(&values, row).unwrap();
                        decode_het(&value).unwrap_or(value)
                    })
                    .collect::<Vec<_>>(),
            )
        };

        assert_eq!(
            values(
                invoke_test_udf(
                    &CypherListPlus::new(),
                    vec![large(&[1, 2], true), ScalarValue::Int64(Some(3))],
                )
                .unwrap()
            ),
            Some(vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2)),
                ScalarValue::Int64(Some(3)),
            ])
        );
        assert_eq!(
            values(
                invoke_test_udf(
                    &CypherListPlus::new(),
                    vec![ScalarValue::Int64(Some(0)), large(&[1, 2], true)],
                )
                .unwrap()
            ),
            Some(vec![
                ScalarValue::Int64(Some(0)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2)),
            ])
        );
        assert_eq!(
            values(
                invoke_test_udf(
                    &CypherListPlus::new(),
                    vec![large(&[1], false), large(&[2], true)],
                )
                .unwrap()
            ),
            None
        );
    }

    #[test]
    fn scalar_udf_runtime_truth_tables_strings_and_ranges() {
        use datafusion::arrow::array::{Array, BooleanArray, ListArray};
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        fn invoke<U: ScalarUDFImpl>(
            udf: &U,
            values: Vec<ScalarValue>,
            return_type: DataType,
        ) -> datafusion::error::Result<ColumnarValue> {
            let fields = values
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    Arc::new(Field::new(format!("arg_{index}"), value.data_type(), true))
                })
                .collect();
            udf.invoke_with_args(ScalarFunctionArgs {
                args: values.into_iter().map(ColumnarValue::Scalar).collect(),
                arg_fields: fields,
                number_rows: 1,
                return_field: Arc::new(Field::new("out", return_type, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
        }

        let booleans = [None, Some(false), Some(true)];
        for kind in [
            CypherBoolOpKind::And,
            CypherBoolOpKind::Or,
            CypherBoolOpKind::Xor,
        ] {
            for left in booleans {
                for right in booleans {
                    let output = invoke(
                        &CypherBoolOp::new(kind),
                        vec![ScalarValue::Boolean(left), ScalarValue::Boolean(right)],
                        DataType::Boolean,
                    )
                    .unwrap();
                    let output = match output {
                        ColumnarValue::Array(array) => array,
                        ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
                    };
                    let output = output.as_any().downcast_ref::<BooleanArray>().unwrap();
                    let actual = (!output.is_null(0)).then(|| output.value(0));
                    let expected = match kind {
                        CypherBoolOpKind::And => match (left, right) {
                            (Some(false), _) | (_, Some(false)) => Some(false),
                            (Some(true), Some(true)) => Some(true),
                            _ => None,
                        },
                        CypherBoolOpKind::Or => match (left, right) {
                            (Some(true), _) | (_, Some(true)) => Some(true),
                            (Some(false), Some(false)) => Some(false),
                            _ => None,
                        },
                        CypherBoolOpKind::Xor => left.zip(right).map(|(l, r)| l ^ r),
                    };
                    assert_eq!(actual, expected);
                }
            }
        }
        assert!(
            invoke(
                &CypherBoolOp::new(CypherBoolOpKind::And),
                vec![
                    ScalarValue::Int64(Some(1)),
                    ScalarValue::Boolean(Some(true))
                ],
                DataType::Boolean,
            )
            .unwrap_err()
            .to_string()
            .contains("expected boolean operand")
        );

        for (kind, expected) in [
            (StringPredicate::Starts, true),
            (StringPredicate::Ends, false),
            (StringPredicate::Contains, true),
        ] {
            let output = invoke(
                &CypherStringPredicate::new(kind),
                vec![
                    ScalarValue::LargeUtf8(Some("GraphForge".into())),
                    ScalarValue::Utf8(Some("Graph".into())),
                ],
                DataType::Boolean,
            )
            .unwrap();
            let output = match output {
                ColumnarValue::Array(array) => array,
                ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
            };
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(0),
                expected
            );
        }

        let range_type = DataType::new_list(DataType::Int64, true);
        for (start, end, step, expected) in [(1, 5, 2, vec![1, 3, 5]), (5, 1, -2, vec![5, 3, 1])] {
            let output = invoke(
                &CypherRange::new(),
                vec![
                    ScalarValue::Int64(Some(start)),
                    ScalarValue::Int64(Some(end)),
                    ScalarValue::Int64(Some(step)),
                ],
                range_type.clone(),
            )
            .unwrap();
            let output = match output {
                ColumnarValue::Array(array) => array,
                ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
            };
            let list = output
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0);
            assert_eq!(
                (0..list.len())
                    .map(|row| ScalarValue::try_from_array(&list, row).unwrap())
                    .collect::<Vec<_>>(),
                expected
                    .into_iter()
                    .map(|value| ScalarValue::Int64(Some(value)))
                    .collect::<Vec<_>>()
            );
        }
        for (step, fragment) in [(0, "must not be zero"), (2, "overflowed i64")] {
            let start = if step == 0 { 1 } else { i64::MAX - 1 };
            let end = if step == 0 { 2 } else { i64::MAX };
            let error = invoke(
                &CypherRange::new(),
                vec![
                    ScalarValue::Int64(Some(start)),
                    ScalarValue::Int64(Some(end)),
                    ScalarValue::Int64(Some(step)),
                ],
                range_type.clone(),
            )
            .unwrap_err();
            assert!(error.to_string().contains(fragment));
        }
    }

    #[test]
    fn scalar_conversion_helpers_exhaust_every_numeric_width_null_and_error_contract() {
        let integers = [
            (ScalarValue::Int8(Some(-8)), -8_i64),
            (ScalarValue::Int16(Some(-16)), -16),
            (ScalarValue::Int32(Some(-32)), -32),
            (ScalarValue::Int64(Some(-64)), -64),
            (ScalarValue::UInt8(Some(8)), 8),
            (ScalarValue::UInt16(Some(16)), 16),
            (ScalarValue::UInt32(Some(32)), 32),
            (ScalarValue::UInt64(Some(64)), 64),
        ];
        for (value, expected) in &integers {
            assert_eq!(scalar_as_i128(value), Some(i128::from(*expected)));
            assert_eq!(scalar_as_f64(value), Some(*expected as f64));
            assert_eq!(to_cypher_integer(value).unwrap(), Some(*expected));
            assert_eq!(to_cypher_float(value).unwrap(), Some(*expected as f64));
            assert_eq!(to_cypher_string(value).unwrap(), Some(expected.to_string()));
        }

        for (value, integer, float, text) in [
            (
                ScalarValue::Float32(Some(12.75)),
                Some(12),
                Some(12.75),
                Some("12.75".to_owned()),
            ),
            (
                ScalarValue::Float64(Some(-12.75)),
                Some(-12),
                Some(-12.75),
                Some("-12.75".to_owned()),
            ),
            (
                ScalarValue::Utf8(Some("42.9".into())),
                Some(42),
                Some(42.9),
                Some("42.9".to_owned()),
            ),
            (
                ScalarValue::LargeUtf8(Some("-3".into())),
                Some(-3),
                Some(-3.0),
                Some("-3".to_owned()),
            ),
        ] {
            assert_eq!(to_cypher_integer(&value).unwrap(), integer);
            assert_eq!(to_cypher_float(&value).unwrap(), float);
            assert_eq!(to_cypher_string(&value).unwrap(), text);
        }

        for null in [
            ScalarValue::Null,
            ScalarValue::Int64(None),
            ScalarValue::Float64(None),
            ScalarValue::Utf8(None),
            ScalarValue::Boolean(None),
        ] {
            assert_eq!(to_cypher_integer(&null).unwrap(), None);
            assert_eq!(to_cypher_float(&null).unwrap(), None);
            assert_eq!(to_cypher_boolean(&null).unwrap(), None);
            assert_eq!(to_cypher_string(&null).unwrap(), None);
        }

        for invalid_float in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, f64::MAX] {
            assert_eq!(trunc_float_to_i64(invalid_float), None);
        }
        assert_eq!(trunc_float_to_i64(-9.99), Some(-9));
        for invalid_text in ["", "not-a-number", "NaN", "inf"] {
            let value = ScalarValue::Utf8(Some(invalid_text.into()));
            assert_eq!(to_cypher_integer(&value).unwrap(), None);
            assert_eq!(to_cypher_float(&value).unwrap(), None);
        }
        assert_eq!(
            to_cypher_boolean(&ScalarValue::Boolean(Some(true))).unwrap(),
            Some(true)
        );
        assert_eq!(
            to_cypher_boolean(&ScalarValue::Utf8(Some("true".into()))).unwrap(),
            Some(true)
        );
        assert_eq!(
            to_cypher_boolean(&ScalarValue::LargeUtf8(Some("false".into()))).unwrap(),
            Some(false)
        );
        assert_eq!(
            to_cypher_boolean(&ScalarValue::Utf8(Some("TRUE".into()))).unwrap(),
            None
        );

        for invalid in [
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Binary(Some(vec![1])),
        ] {
            assert!(to_cypher_integer(&invalid).is_err());
            assert!(to_cypher_float(&invalid).is_err());
        }
        assert!(to_cypher_boolean(&ScalarValue::Int64(Some(1))).is_err());
        assert!(to_cypher_string(&ScalarValue::Binary(Some(vec![1]))).is_err());
        assert!(to_cypher_integer(&ScalarValue::UInt64(Some(u64::MAX))).is_err());
    }

    #[test]
    fn canonical_float_strings_and_scalar_range_arguments_cover_boundaries() {
        for (value, expected) in [
            (0.0, "0.0"),
            (-0.0, "0.0"),
            (f64::NAN, "NaN"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
            (1.0, "1.0"),
            (1.5, "1.5"),
            (1e20, "100000000000000000000.0"),
        ] {
            assert_eq!(cypher_float_string(value), expected);
        }

        for (value, expected) in [
            (ScalarValue::Int8(Some(-1)), -1),
            (ScalarValue::Int16(Some(-2)), -2),
            (ScalarValue::Int32(Some(-3)), -3),
            (ScalarValue::Int64(Some(-4)), -4),
            (ScalarValue::UInt8(Some(1)), 1),
            (ScalarValue::UInt16(Some(2)), 2),
            (ScalarValue::UInt32(Some(3)), 3),
            (ScalarValue::UInt64(Some(4)), 4),
        ] {
            assert_eq!(scalar_as_i64_arg(&value, "bound").unwrap(), expected);
        }
        assert!(
            scalar_as_i64_arg(&ScalarValue::UInt64(Some(u64::MAX)), "bound")
                .unwrap_err()
                .to_string()
                .contains("exceeds i64::MAX")
        );
        assert!(
            scalar_as_i64_arg(&ScalarValue::Utf8(Some("1".into())), "bound")
                .unwrap_err()
                .to_string()
                .contains("must be an integer")
        );
    }

    #[test]
    fn temporal_literal_render_dispatch_and_ir_scalar_round_trip_matrix() {
        for (name, input) in [
            ("date", "2024-02-29"),
            ("localtime", "12:34:56"),
            ("time", "12:34:56+01:00"),
            ("localdatetime", "2024-02-29T12:34:56"),
            ("datetime", "2024-02-29T12:34:56Z"),
            ("duration", "P1M2DT3S"),
        ] {
            assert!(render_temporal(name, input).is_some(), "{name}({input})");
        }
        assert_eq!(render_temporal("unknown", "2024-01-01"), None);
        assert_eq!(render_temporal("date", "not-a-date"), None);

        let literals = [
            IrLiteral::Null,
            IrLiteral::Bool(true),
            IrLiteral::Int(-7),
            IrLiteral::Float(1.25),
            IrLiteral::Str("value".into()),
            IrLiteral::Duration {
                months: 1,
                days: 2,
                seconds: 3,
                nanos: 4,
            },
            IrLiteral::DateTime(123),
            IrLiteral::Date(20_000),
            IrLiteral::LocalDateTime {
                days: 20_000,
                nanos: 123,
            },
            IrLiteral::Time(456),
            IrLiteral::ZonedTime {
                nanos: 789,
                offset: 3_600,
            },
            IrLiteral::ZonedDateTime {
                days: 20_000,
                nanos: 999,
                offset: -3_600,
                zone: Some("America/Denver".into()),
            },
            IrLiteral::List(vec![IrLiteral::Int(1), IrLiteral::Null]),
            IrLiteral::Map(vec![("answer".into(), IrLiteral::Int(42))]),
        ];
        for literal in literals {
            let scalar = ir_literal_to_scalar(&literal);
            if !matches!(literal, IrLiteral::Map(_)) {
                assert_eq!(scalar_to_ir_literal(&scalar).unwrap(), literal);
            }
        }

        for (scalar, expected) in [
            (
                ScalarValue::DurationSecond(Some(-2)),
                IrLiteral::Duration {
                    months: 0,
                    days: 0,
                    seconds: -2,
                    nanos: 0,
                },
            ),
            (
                ScalarValue::DurationMillisecond(Some(-1)),
                IrLiteral::Duration {
                    months: 0,
                    days: 0,
                    seconds: -1,
                    nanos: 999_000_000,
                },
            ),
            (
                ScalarValue::DurationMicrosecond(Some(-1)),
                IrLiteral::Duration {
                    months: 0,
                    days: 0,
                    seconds: -1,
                    nanos: 999_999_000,
                },
            ),
            (
                ScalarValue::DurationNanosecond(Some(-1)),
                IrLiteral::Duration {
                    months: 0,
                    days: 0,
                    seconds: -1,
                    nanos: 999_999_999,
                },
            ),
        ] {
            assert_eq!(scalar_to_ir_literal(&scalar).unwrap(), expected);
        }
    }

    #[test]
    fn temporal_truncate_lowering_covers_arity_default_literal_override_and_rejection_paths() {
        for name in [
            "date.truncate",
            "localtime.truncate",
            "localdatetime.truncate",
            "time.truncate",
            "datetime.truncate",
        ] {
            let mut missing = ExprArena::new();
            let call = missing.push(IrExpr::FunctionCall {
                name: name.into(),
                args: vec![],
            });
            assert!(matches!(
                make_lowerer(&missing, &VarMap::new()).lower(call),
                Err(LoweringError::UnknownFunction(function)) if function == name
            ));

            let mut defaults = ExprArena::new();
            let unit = defaults.push(IrExpr::Literal(IrLiteral::Str("day".into())));
            let value = defaults.push(IrExpr::Literal(IrLiteral::Null));
            let call = defaults.push(IrExpr::FunctionCall {
                name: name.into(),
                args: vec![unit, value],
            });
            let lowered = make_lowerer(&defaults, &VarMap::new()).lower(call).unwrap();
            assert!(format!("{lowered}").contains("truncate"));

            let mut overrides = ExprArena::new();
            let unit = overrides.push(IrExpr::Literal(IrLiteral::Str("day".into())));
            let value = overrides.push(IrExpr::Literal(IrLiteral::Null));
            let one = overrides.push(IrExpr::Literal(IrLiteral::Int(1)));
            let zone = overrides.push(IrExpr::Literal(IrLiteral::Str("UTC".into())));
            let map = overrides.push(IrExpr::MapLiteral(vec![
                ("year".into(), one),
                ("month".into(), one),
                ("day".into(), one),
                ("week".into(), one),
                ("dayOfWeek".into(), one),
                ("ordinalDay".into(), one),
                ("quarter".into(), one),
                ("dayOfQuarter".into(), one),
                ("hour".into(), one),
                ("minute".into(), one),
                ("second".into(), one),
                ("millisecond".into(), one),
                ("microsecond".into(), one),
                ("nanosecond".into(), one),
                ("timezone".into(), zone),
            ]));
            let call = overrides.push(IrExpr::FunctionCall {
                name: name.into(),
                args: vec![unit, value, map],
            });
            assert!(make_lowerer(&overrides, &VarMap::new()).lower(call).is_ok());

            let mut dynamic = ExprArena::new();
            let unit = dynamic.push(IrExpr::Literal(IrLiteral::Str("day".into())));
            let value = dynamic.push(IrExpr::Literal(IrLiteral::Null));
            let parameter = dynamic.push(IrExpr::Parameter("overrides".into()));
            let call = dynamic.push(IrExpr::FunctionCall {
                name: name.into(),
                args: vec![unit, value, parameter],
            });
            assert!(
                make_lowerer(&dynamic, &VarMap::new())
                    .lower(call)
                    .unwrap_err()
                    .to_string()
                    .contains("override map must be a literal map")
            );
        }

        for name in [
            "duration.between",
            "duration.inmonths",
            "duration.indays",
            "duration.inseconds",
        ] {
            let mut arena = ExprArena::new();
            let call = arena.push(IrExpr::FunctionCall {
                name: name.into(),
                args: vec![],
            });
            assert!(matches!(
                make_lowerer(&arena, &VarMap::new()).lower(call),
                Err(LoweringError::UnknownFunction(function)) if function == name
            ));

            let left = arena.push(IrExpr::Literal(IrLiteral::Null));
            let right = arena.push(IrExpr::Literal(IrLiteral::Null));
            let call = arena.push(IrExpr::FunctionCall {
                name: name.into(),
                args: vec![left, right],
            });
            assert!(make_lowerer(&arena, &VarMap::new()).lower(call).is_ok());
        }
    }

    #[test]
    fn cypher_value_comparison_and_order_helpers_cover_cross_type_edges() {
        use datafusion::scalar::ScalarValue as S;

        for value in [
            S::Int8(Some(1)),
            S::Int16(Some(1)),
            S::Int32(Some(1)),
            S::Int64(Some(1)),
            S::UInt8(Some(1)),
            S::UInt16(Some(1)),
            S::UInt32(Some(1)),
            S::UInt64(Some(1)),
            S::Float32(Some(1.0)),
            S::Float64(Some(1.0)),
        ] {
            assert_eq!(scalar_as_f64(&value), Some(1.0));
        }
        assert_eq!(scalar_as_i128(&S::Float64(Some(1.0))), None);
        assert_eq!(scalar_as_f64(&S::Boolean(Some(true))), None);

        let one = S::List(S::new_list(&[S::Int64(Some(1))], &DataType::Int64, true));
        let one_null = S::List(S::new_list(
            &[S::Int64(Some(1)), S::Int64(None)],
            &DataType::Int64,
            true,
        ));
        let two = S::List(S::new_list(
            &[S::Int64(Some(1)), S::Int64(Some(2))],
            &DataType::Int64,
            true,
        ));
        assert_eq!(cypher_value_eq(&one, &one), Some(true));
        assert_eq!(cypher_value_eq(&one, &two), Some(false));
        assert_eq!(cypher_value_eq(&one_null, &one_null), None);
        assert_eq!(cypher_value_eq(&S::Null, &S::Int64(Some(1))), None);
        assert_eq!(
            cypher_value_eq(&S::Int64(Some(1)), &S::Float64(Some(1.0))),
            Some(true)
        );
        assert_eq!(
            cypher_value_eq(&S::Utf8(Some("a".into())), &S::Utf8(Some("b".into()))),
            Some(false)
        );

        assert!(cypher_order_key(&S::Null).starts_with("99:null"));
        assert!(cypher_order_key(&S::Utf8(Some("a".into()))).starts_with("60:str"));
        assert!(cypher_order_key(&S::Boolean(Some(true))).starts_with("70:bool"));
        assert!(cypher_order_key(&S::Float64(Some(f64::NAN))).starts_with("90:nan"));
        assert!(cypher_order_key(&S::Binary(Some(vec![1]))).starts_with("98:other"));
        assert!(cypher_order_key(&one).starts_with("40:list"));
    }

    #[test]
    fn expression_lowering_error_and_static_access_matrix_reaches_contract_branches() {
        let lower_call = |name: &str, args: Vec<IrExpr>| {
            let mut arena = ExprArena::new();
            let args = args
                .into_iter()
                .map(|expr| arena.push(expr))
                .collect::<Vec<_>>();
            let call = arena.push(IrExpr::FunctionCall {
                name: name.into(),
                args,
            });
            make_lowerer(&arena, &VarMap::new()).lower(call)
        };

        for (name, expected) in [
            ("_subscript", "expects two arguments"),
            ("_node_struct", "expects at least one argument"),
            (
                "_node_struct_list",
                "expects two nodes and one relationship",
            ),
            ("_rel_struct", "expects an edge variable"),
            ("_rel_struct_list", "expects an edge variable"),
            ("keys", "expects one argument"),
            ("properties", "expects one argument"),
            ("labels", "expects one argument"),
        ] {
            assert!(
                lower_call(name, vec![])
                    .unwrap_err()
                    .to_string()
                    .contains(expected),
                "{name}"
            );
        }
        for name in ["nodes", "relationships"] {
            assert!(
                lower_call(name, vec![])
                    .unwrap_err()
                    .to_string()
                    .contains("expects one path argument")
            );
        }

        assert!(
            lower_call("_node_struct", vec![IrExpr::Literal(IrLiteral::Int(1))])
                .unwrap_err()
                .to_string()
                .contains("must be a node variable")
        );
        assert!(
            lower_call("_rel_struct", vec![IrExpr::Literal(IrLiteral::Int(1))])
                .unwrap_err()
                .to_string()
                .contains("must be a relationship variable")
        );
        assert!(
            lower_call(
                "_node_struct_list",
                vec![
                    IrExpr::Literal(IrLiteral::Int(1)),
                    IrExpr::Literal(IrLiteral::Int(2)),
                    IrExpr::Literal(IrLiteral::Null),
                ],
            )
            .unwrap_err()
            .to_string()
            .contains("node arguments must be variables")
        );

        for name in ["keys", "properties"] {
            assert!(
                lower_call(name, vec![IrExpr::ListLiteral(vec![])],)
                    .unwrap_err()
                    .to_string()
                    .contains("requires a map, node, relationship, or null")
            );
            assert!(lower_call(name, vec![IrExpr::Literal(IrLiteral::Null)]).is_ok());
            assert!(lower_call(name, vec![IrExpr::MapLiteral(vec![])],).is_ok());
        }
        assert!(lower_call("labels", vec![IrExpr::Literal(IrLiteral::Null)]).is_ok());

        let mut arena = ExprArena::new();
        let null = arena.push(IrExpr::Literal(IrLiteral::Null));
        let null_key = arena.push(IrExpr::Literal(IrLiteral::Null));
        let access = arena.push(IrExpr::FunctionCall {
            name: "_subscript".into(),
            args: vec![null, null_key],
        });
        let null_access = make_lowerer(&arena, &VarMap::new()).lower(access).unwrap();
        assert!(format!("{null_access}").contains("cypher_value_access"));

        let mut arena = ExprArena::new();
        let answer = arena.push(IrExpr::Literal(IrLiteral::Int(42)));
        let map = arena.push(IrExpr::MapLiteral(vec![("answer".into(), answer)]));
        let key = arena.push(IrExpr::Literal(IrLiteral::Str("answer".into())));
        let missing = arena.push(IrExpr::Literal(IrLiteral::Str("missing".into())));
        let found = arena.push(IrExpr::FunctionCall {
            name: "_subscript".into(),
            args: vec![map, key],
        });
        let absent = arena.push(IrExpr::FunctionCall {
            name: "_subscript".into(),
            args: vec![map, missing],
        });
        assert_eq!(
            format!(
                "{}",
                make_lowerer(&arena, &VarMap::new()).lower(found).unwrap()
            ),
            "Int64(42)"
        );
        assert!(matches!(
            make_lowerer(&arena, &VarMap::new()).lower(absent).unwrap(),
            DfExpr::Literal(ScalarValue::Null, _)
        ));

        let mut arena = ExprArena::new();
        let scalar = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
        let index = arena.push(IrExpr::Literal(IrLiteral::Int(0)));
        let invalid = arena.push(IrExpr::FunctionCall {
            name: "_subscript".into(),
            args: vec![scalar, index],
        });
        assert!(
            make_lowerer(&arena, &VarMap::new())
                .lower(invalid)
                .unwrap_err()
                .to_string()
                .contains("subscript requires a list")
        );

        for argument in [
            IrExpr::Literal(IrLiteral::Str("abc".into())),
            IrExpr::ListLiteral(vec![]),
            IrExpr::Parameter("value".into()),
        ] {
            assert!(lower_call("reverse", vec![argument]).is_ok());
        }
    }

    #[test]
    fn static_nested_list_map_access_handles_negative_oob_and_nonliteral_indices() {
        let mut arena = ExprArena::new();
        let one = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
        let two = arena.push(IrExpr::Literal(IrLiteral::Int(2)));
        let first_map = arena.push(IrExpr::MapLiteral(vec![("value".into(), one)]));
        let second_map = arena.push(IrExpr::MapLiteral(vec![("value".into(), two)]));
        let list = arena.push(IrExpr::ListLiteral(vec![first_map, second_map]));
        let negative = arena.push(IrExpr::Literal(IrLiteral::Int(-1)));
        let oob = arena.push(IrExpr::Literal(IrLiteral::Int(9)));
        let dynamic = arena.push(IrExpr::Parameter("index".into()));
        let key = arena.push(IrExpr::Literal(IrLiteral::Str("value".into())));

        for (index, expected) in [(negative, Some("Int64(2)")), (oob, None)] {
            let indexed = arena.push(IrExpr::FunctionCall {
                name: "_subscript".into(),
                args: vec![list, index],
            });
            let field = arena.push(IrExpr::FunctionCall {
                name: "_subscript".into(),
                args: vec![indexed, key],
            });
            let lowered = make_lowerer(&arena, &VarMap::new()).lower(field).unwrap();
            match expected {
                Some(expected) => assert_eq!(format!("{lowered}"), expected),
                None => assert!(matches!(lowered, DfExpr::Literal(ScalarValue::Null, _))),
            }
        }

        let indexed = arena.push(IrExpr::FunctionCall {
            name: "_subscript".into(),
            args: vec![list, dynamic],
        });
        let field = arena.push(IrExpr::FunctionCall {
            name: "_subscript".into(),
            args: vec![indexed, key],
        });
        assert!(
            format!(
                "{}",
                make_lowerer(&arena, &VarMap::new()).lower(field).unwrap()
            )
            .contains("cypher_value_access")
        );
    }

    #[test]
    fn aggregate_accumulators_cover_update_merge_state_and_empty_contracts() {
        use datafusion::arrow::array::{
            ArrayRef, Float64Array, Int64Array, ListArray, StringArray,
        };
        use datafusion::logical_expr::Accumulator;

        let ints: ArrayRef = Arc::new(Int64Array::from(vec![Some(4), None, Some(-2), Some(9)]));
        for (is_max, expected) in [(true, 9), (false, -2)] {
            let mut acc = ExtremeAcc {
                is_max,
                dtype: DataType::Int64,
                best: None,
            };
            assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(None));
            acc.update_batch(std::slice::from_ref(&ints)).unwrap();
            assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(expected)));
            assert_eq!(
                acc.state().unwrap(),
                vec![ScalarValue::Int64(Some(expected))]
            );
            assert!(acc.size() >= std::mem::size_of::<ExtremeAcc>());
            let merged: ArrayRef =
                Arc::new(Int64Array::from(vec![Some(if is_max { 12 } else { -7 })]));
            acc.merge_batch(&[merged]).unwrap();
            assert_eq!(
                acc.evaluate().unwrap(),
                ScalarValue::Int64(Some(if is_max { 12 } else { -7 }))
            );
        }

        for distinct in [false, true] {
            let mut acc = CollectAcc {
                distinct,
                elem_type: DataType::Int64,
                values: Vec::new(),
            };
            acc.update_batch(std::slice::from_ref(&ints)).unwrap();
            let merge: ArrayRef = Arc::new(ListArray::from_iter_primitive::<
                datafusion::arrow::datatypes::Int64Type,
                _,
                _,
            >([Some(vec![Some(4), Some(11)])]));
            acc.merge_batch(&[merge]).unwrap();
            let ScalarValue::List(values) = acc.evaluate().unwrap() else {
                panic!("collect must return a list")
            };
            let expected_len = if distinct { 4 } else { 5 };
            assert_eq!(values.value(0).len(), expected_len);
            assert_eq!(acc.state().unwrap().len(), 1);
            assert!(acc.size() >= std::mem::size_of::<CollectAcc>());
            let bad: ArrayRef = Arc::new(Int64Array::from(vec![1]));
            assert!(
                acc.merge_batch(&[bad])
                    .unwrap_err()
                    .to_string()
                    .contains("must be a list")
            );
        }

        for continuous in [false, true] {
            let mut acc = PercentileAcc {
                continuous,
                value_type: DataType::Int64,
                result_type: if continuous {
                    DataType::Float64
                } else {
                    DataType::Int64
                },
                values: Vec::new(),
                percentile: None,
            };
            assert!(acc.evaluate().unwrap().is_null());
            let p: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.5); 4]));
            acc.update_batch(&[Arc::clone(&ints), p]).unwrap();
            assert_eq!(
                acc.evaluate().unwrap(),
                if continuous {
                    ScalarValue::Float64(Some(4.0))
                } else {
                    ScalarValue::Int64(Some(4))
                }
            );
            assert_eq!(acc.state().unwrap().len(), 2);
            assert!(acc.size() >= std::mem::size_of::<PercentileAcc>());
            assert!(acc.observe_percentile(Some(f64::NAN)).is_err());
            assert!(acc.observe_percentile(Some(0.75)).is_err());
            let bad_values: ArrayRef = Arc::new(StringArray::from(vec!["not-list"]));
            let good_p: ArrayRef = Arc::new(Float64Array::from(vec![0.5]));
            assert!(
                acc.merge_batch(&[bad_values, good_p])
                    .unwrap_err()
                    .to_string()
                    .contains("must be a list")
            );
        }
    }

    #[test]
    fn duration_and_temporal_runtime_udfs_cover_each_value_family_and_nulls() {
        let d1 = crate::temporal::DurationValue {
            months: 1,
            days: 2,
            seconds: 3,
            nanos: 750_000_000,
        };
        let d2 = crate::temporal::DurationValue {
            months: 2,
            days: 3,
            seconds: 4,
            nanos: 500_000_000,
        };
        let d1 = duration_scalar(Some(d1));
        let d2 = duration_scalar(Some(d2));

        let parsed = invoke_test_udf(
            &CypherDurationParse::new(),
            vec![ScalarValue::Utf8(Some("P1M2DT3.5S".into()))],
        )
        .unwrap();
        assert!(duration_struct_parts(parsed.as_any().downcast_ref().unwrap(), 0).is_some());
        let invalid = invoke_test_udf(
            &CypherDurationParse::new(),
            vec![ScalarValue::Utf8(Some("invalid".into()))],
        )
        .unwrap();
        assert!(duration_struct_parts(invalid.as_any().downcast_ref().unwrap(), 0).is_none());

        for sign in [1, -1] {
            let out = invoke_test_udf(
                &CypherDurationAdd::new(),
                vec![d1.clone(), d2.clone(), ScalarValue::Int64(Some(sign))],
            )
            .unwrap();
            assert!(duration_struct_parts(out.as_any().downcast_ref().unwrap(), 0).is_some());
        }
        for (factor, divide) in [(2.0, false), (2.0, true)] {
            let out = invoke_test_udf(
                &CypherDurationScale::new(),
                vec![
                    d1.clone(),
                    ScalarValue::Float64(Some(factor)),
                    ScalarValue::Boolean(Some(divide)),
                ],
            )
            .unwrap();
            assert!(duration_struct_parts(out.as_any().downcast_ref().unwrap(), 0).is_some());
        }

        let temporal_values = [
            date_scalar(Some(20_000)),
            ScalarValue::Time64Nanosecond(Some(10)),
            time_scalar(Some((10, 3_600))),
            localdatetime_scalar(Some((20_000, 10))),
            datetime_scalar(Some((20_000, 10, 0, Some("UTC".into())))),
        ];
        for temporal in temporal_values {
            for sign in [1, -1] {
                let out = invoke_test_udf(
                    &CypherTemporalArith::new(),
                    vec![temporal.clone(), d1.clone(), ScalarValue::Int64(Some(sign))],
                )
                .unwrap();
                assert_eq!(out.data_type(), &temporal.data_type());
                assert!(!out.is_null(0));
            }
        }
        assert!(
            invoke_test_udf(
                &CypherTemporalArith::new(),
                vec![ScalarValue::Int64(Some(1)), d1, ScalarValue::Int64(Some(1))],
            )
            .unwrap_err()
            .to_string()
            .contains("not a temporal value")
        );
    }

    #[test]
    fn exact_zero_large_list_legacy_variant_order_and_percentile_branches() {
        use datafusion::arrow::array::{Array, ArrayRef, Int64Array, LargeListArray};
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
        use datafusion::arrow::datatypes::{Field, Fields};
        use datafusion::logical_expr::Accumulator;

        let large = |values: Vec<i64>, valid: bool| {
            let len = i64::try_from(values.len()).unwrap();
            ScalarValue::LargeList(Arc::new(LargeListArray::new(
                Arc::new(Field::new("item", DataType::Int64, true)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0, len])),
                Arc::new(Int64Array::from(values)) as ArrayRef,
                Some(NullBuffer::from(vec![valid])),
            )))
        };
        assert_eq!(
            scalar_list_elements(&large(vec![1, 2], true)).unwrap(),
            Some(vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2))
            ])
        );
        assert_eq!(scalar_list_elements(&large(vec![], false)).unwrap(), None);

        let size = invoke_test_udf(&CypherSize::new(), vec![large(vec![1, 2, 3], true)]).unwrap();
        assert_eq!(
            ScalarValue::try_from_array(&size, 0).unwrap(),
            ScalarValue::Int64(Some(3))
        );
        let null_size = invoke_test_udf(&CypherSize::new(), vec![large(vec![], false)]).unwrap();
        assert!(null_size.is_null(0));

        let reversed = invoke_test_udf(
            &CypherReverse::new(),
            vec![ScalarValue::LargeUtf8(Some("a😀b".into()))],
        )
        .unwrap();
        assert_eq!(
            ScalarValue::try_from_array(&reversed, 0).unwrap(),
            ScalarValue::LargeUtf8(Some("b😀a".into()))
        );

        let shorter: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let longer: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let different: ArrayRef = Arc::new(Int64Array::from(vec![1, 9]));
        assert_eq!(
            cypher_seq_order(&shorter, &longer),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            cypher_seq_order(&different, &shorter),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            cypher_seq_order(&shorter, &shorter),
            std::cmp::Ordering::Equal
        );

        let legacy = DataType::Struct(Fields::from(vec![
            Field::new("__het_tag", DataType::Int8, false),
            Field::new("__het_int", DataType::Int64, true),
            Field::new("__het_float", DataType::Float64, true),
            Field::new("__het_str", DataType::Utf8, true),
            Field::new("__het_bool", DataType::Boolean, true),
        ]));
        let return_type = list_plus_return_type(&[
            DataType::new_list(legacy, true),
            DataType::new_list(DataType::Int64, true),
        ]);
        let DataType::List(item) = return_type else {
            panic!("list return")
        };
        let DataType::Struct(variants) = item.data_type() else {
            panic!("variant struct")
        };
        assert!(
            variants
                .iter()
                .any(|field| field.data_type() == &DataType::Int64)
        );
        assert!(
            variants
                .iter()
                .any(|field| field.data_type() == &DataType::Utf8)
        );

        let mut percentile = PercentileAcc {
            continuous: true,
            value_type: DataType::Int64,
            result_type: DataType::Float64,
            values: Vec::new(),
            percentile: None,
        };
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let bad_percentile: ArrayRef =
            Arc::new(datafusion::arrow::array::StringArray::from(vec!["half"]));
        assert!(
            percentile
                .update_batch(&[values, bad_percentile])
                .unwrap_err()
                .to_string()
                .contains("must be numeric")
        );
    }

    #[test]
    fn exact_zero_temporal_udf_metadata_contracts_are_total() {
        fn check<U: ScalarUDFImpl + 'static>(udf: U) {
            assert!((&udf as &dyn ScalarUDFImpl).is::<U>());
            assert!(!udf.name().is_empty());
            let _ = udf.signature();
            assert!(udf.return_type(&[DataType::Null]).is_ok());
        }

        check(CypherDurationBetween::new());
        check(CypherTemporalArith::new());
        check(CypherDurationParse::new());
        check(CypherDurationAdd::new());
        check(CypherDurationScale::new());
        check(CypherDateProject::new());
        check(CypherLocalTimeProject::new());
        check(CypherLocalTimeTruncate::new());
        check(CypherLocalDateTimeProject::new());
        check(CypherLocalDateTimeTruncate::new());
        check(CypherTimeProject::new());
        check(CypherTimeTruncate::new());
        check(CypherDateTimeProject::new());
        check(CypherDateTimeTruncate::new());
        check(CypherToString::new());
        check(CypherDateTruncate::new());
    }

    #[test]
    fn exact_zero_access_map_metadata_and_type_helpers() {
        use datafusion::arrow::datatypes::{Field, Fields};

        let map = const_map_scalar(&[
            ("k".into(), ScalarValue::Int64(Some(7))),
            ("other".into(), ScalarValue::Int64(None)),
        ])
        .unwrap();
        let accessed =
            invoke_test_udf(&CypherStaticValueAccess::new("k".into()), vec![map.clone()]).unwrap();
        assert_eq!(
            ScalarValue::try_from_array(&accessed, 0).unwrap(),
            ScalarValue::Int64(Some(7))
        );
        let missing = invoke_test_udf(
            &CypherStaticValueAccess::new("missing".into()),
            vec![map.clone()],
        )
        .unwrap();
        assert!(ScalarValue::try_from_array(&missing, 0).unwrap().is_null());

        let keys = invoke_test_udf(&CypherMapKeys::new(), vec![map]).unwrap();
        let ScalarValue::List(keys) = ScalarValue::try_from_array(&keys, 0).unwrap() else {
            panic!("keys must return a list")
        };
        assert_eq!(keys.value(0).len(), 2);

        let props = invoke_test_udf(
            &CypherEntityProperties::new(3),
            vec![
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Utf8(Some("k".into())),
                ScalarValue::Int64(Some(7)),
            ],
        )
        .unwrap();
        assert!(!props.is_null(0));
        let absent = invoke_test_udf(
            &CypherEntityProperties::new(3),
            vec![
                ScalarValue::Boolean(Some(false)),
                ScalarValue::Utf8(Some("k".into())),
                ScalarValue::Int64(Some(7)),
            ],
        )
        .unwrap();
        assert!(absent.is_null(0));

        let null_access = invoke_test_udf(
            &CypherValueAccess::new(),
            vec![ScalarValue::Null, ScalarValue::Utf8(Some("k".into()))],
        )
        .unwrap();
        assert!(
            ScalarValue::try_from_array(&null_access, 0)
                .unwrap()
                .is_null()
        );

        for (value, expected) in [
            (ScalarValue::Int8(Some(-1)), Some(-1)),
            (ScalarValue::Int16(Some(-2)), Some(-2)),
            (ScalarValue::Int32(Some(-3)), Some(-3)),
            (ScalarValue::Int64(Some(-4)), Some(-4)),
            (ScalarValue::UInt8(Some(1)), Some(1)),
            (ScalarValue::UInt16(Some(2)), Some(2)),
            (ScalarValue::UInt32(Some(3)), Some(3)),
            (ScalarValue::UInt64(Some(4)), Some(4)),
            (ScalarValue::Int64(None), None),
        ] {
            assert_eq!(scalar_list_index(&value).unwrap(), expected);
        }
        assert!(scalar_list_index(&ScalarValue::UInt64(Some(u64::MAX))).is_err());
        assert!(scalar_list_index(&ScalarValue::Utf8(Some("0".into()))).is_err());

        let nested_a = DataType::Struct(Fields::from(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("items", DataType::new_list(DataType::Utf8, true), true),
        ]));
        let nested_b = DataType::Struct(Fields::from(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("items", DataType::new_list(DataType::Utf8, true), false),
        ]));
        assert!(graph_value_types_compatible(&nested_a, &nested_b));
        assert!(!graph_value_types_compatible(&nested_a, &DataType::Int64));
        assert!(!graph_value_types_compatible(
            &nested_a,
            &DataType::Struct(Fields::from(vec![Field::new(
                "other",
                DataType::Int64,
                true
            )]))
        ));
        let unified = unify_graph_value_nullability(&nested_a, &nested_b).unwrap();
        let DataType::Struct(fields) = &unified else {
            panic!("struct");
        };
        assert!(fields[0].is_nullable(), "value nullability is widened");
        assert!(fields[1].is_nullable(), "items nullability is widened");
        let non_null_uuid = DataType::Struct(Fields::from(vec![Field::new(
            "node_uuid",
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let nullable_uuid = DataType::Struct(Fields::from(vec![Field::new(
            "node_uuid",
            DataType::FixedSizeBinary(16),
            true,
        )]));
        let list_target = unify_graph_value_nullability(
            &DataType::new_list(non_null_uuid.clone(), true),
            &DataType::new_list(nullable_uuid.clone(), true),
        )
        .unwrap();
        let DataType::List(item) = &list_target else {
            panic!("list");
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("struct element");
        };
        assert!(
            fields[0].is_nullable(),
            "DF54 path-list concat must widen node_uuid nullability"
        );
        let large = unify_graph_value_nullability(
            &DataType::new_large_list(non_null_uuid.clone(), true),
            &DataType::new_large_list(nullable_uuid.clone(), true),
        )
        .unwrap();
        assert!(matches!(large, DataType::LargeList(_)));
        let fixed = unify_graph_value_nullability(
            &DataType::new_fixed_size_list(non_null_uuid, 2, true),
            &DataType::new_fixed_size_list(nullable_uuid, 2, true),
        )
        .unwrap();
        assert!(matches!(fixed, DataType::FixedSizeList(_, 2)));
        assert!(
            unify_graph_value_nullability(
                &DataType::new_fixed_size_list(
                    DataType::Struct(Fields::from(vec![Field::new(
                        "node_uuid",
                        DataType::FixedSizeBinary(16),
                        false,
                    )])),
                    2,
                    true,
                ),
                &DataType::new_fixed_size_list(
                    DataType::Struct(Fields::from(vec![Field::new(
                        "node_uuid",
                        DataType::FixedSizeBinary(16),
                        true,
                    )])),
                    3,
                    true,
                ),
            )
            .is_none(),
            "FixedSizeList widths must match"
        );

        for (name, value) in [
            ("date", "2020-01-02"),
            ("localtime", "12:34:56"),
            ("time", "12:34:56Z"),
            ("localdatetime", "2020-01-02T12:34:56"),
            ("datetime", "2020-01-02T12:34:56Z"),
            ("duration", "P1D"),
        ] {
            assert!(render_temporal(name, value).is_some());
        }
        assert_eq!(render_temporal("unknown", "P1D"), None);
    }

    #[test]
    fn exact_zero_dynamic_heterogeneous_list_builds_row_aligned_variants() {
        use datafusion::arrow::array::{Array, ListArray, StructArray};

        let output = invoke_test_udf(
            &CypherDynamicHetList::new(),
            vec![
                ScalarValue::Int64(Some(7)),
                ScalarValue::Utf8(Some("seven".into())),
                ScalarValue::Boolean(None),
            ],
        )
        .unwrap();
        let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.len(), 1);
        assert_eq!(lists.value_length(0), 3);
        let values = lists.value(0);
        let variants = values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(variants.len(), 3);
        assert!(!variants.is_null(0));
        assert!(!variants.is_null(1));
        assert!(variants.is_null(2));
    }

    #[test]
    fn exact_zero_list_plus_handles_each_operand_shape_and_null_propagation() {
        use datafusion::arrow::array::{Array, ArrayRef, Int64Array, ListArray};
        use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};

        let list = |values: &[i64]| {
            ScalarValue::List(ScalarValue::new_list(
                &values
                    .iter()
                    .copied()
                    .map(|value| ScalarValue::Int64(Some(value)))
                    .collect::<Vec<_>>(),
                &DataType::Int64,
                true,
            ))
        };
        for (left, right, expected) in [
            (list(&[1, 2]), list(&[3, 4]), vec![1, 2, 3, 4]),
            (list(&[1, 2]), ScalarValue::Int64(Some(3)), vec![1, 2, 3]),
            (ScalarValue::Int64(Some(1)), list(&[2, 3]), vec![1, 2, 3]),
        ] {
            let output = invoke_test_udf(&CypherListPlus::new(), vec![left, right]).unwrap();
            let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
            let values = lists.value(0);
            assert_eq!(
                (0..values.len())
                    .map(|row| { unwrap_het(ScalarValue::try_from_array(&values, row).unwrap()) })
                    .collect::<Vec<_>>(),
                expected
                    .into_iter()
                    .map(|value| ScalarValue::Int64(Some(value)))
                    .collect::<Vec<_>>()
            );
        }

        let null_list = ScalarValue::List(Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 0])),
            Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef,
            Some(NullBuffer::from(vec![false])),
        )));
        let output = invoke_test_udf(
            &CypherListPlus::new(),
            vec![null_list, ScalarValue::Int64(Some(1))],
        )
        .unwrap();
        assert!(output.is_null(0));

        assert!(
            invoke_test_udf(
                &CypherListPlus::new(),
                vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))],
            )
            .unwrap_err()
            .to_string()
            .contains("at least one list operand")
        );
    }

    #[test]
    fn exact_zero_total_order_keys_distinguish_core_cypher_value_domains() {
        let list = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))],
            &DataType::Int64,
            true,
        ));
        let values = [
            ScalarValue::Null,
            list,
            ScalarValue::Utf8(Some("text".into())),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Int64(Some(-2)),
            ScalarValue::Float64(Some(2.5)),
            ScalarValue::Float64(Some(f64::NAN)),
        ];
        let keys = values.iter().map(cypher_order_key).collect::<Vec<_>>();
        assert!(keys[0].starts_with("99:null"));
        assert!(keys[1].starts_with("40:list"));
        assert!(keys[2].starts_with("60:str"));
        assert!(keys[3].starts_with("70:bool"));
        assert!(keys[4].starts_with("80:num"));
        assert!(keys[5].starts_with("80:num"));
        assert!(keys[6].starts_with("90:nan"));
        assert_eq!(
            cypher_order(
                &ScalarValue::Utf8(Some("a".into())),
                &ScalarValue::Boolean(Some(false))
            ),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn exact_zero_dynamic_list_access_supports_negative_null_and_missing_indexes() {
        let values = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::Utf8(Some("first".into())),
                ScalarValue::Utf8(Some("second".into())),
            ],
            &DataType::Utf8,
            true,
        ));
        for (index, expected) in [
            (ScalarValue::Int64(Some(0)), Some("first")),
            (ScalarValue::Int64(Some(-1)), Some("second")),
            (ScalarValue::Int64(Some(9)), None),
            (ScalarValue::Int64(Some(-9)), None),
            (ScalarValue::Int64(None), None),
        ] {
            let output =
                invoke_test_udf(&CypherValueAccess::new(), vec![values.clone(), index]).unwrap();
            assert_eq!(
                ScalarValue::try_from_array(&output, 0).unwrap(),
                ScalarValue::Utf8(expected.map(str::to_owned))
            );
        }
        assert!(
            invoke_test_udf(
                &CypherValueAccess::new(),
                vec![values, ScalarValue::Utf8(Some("not-an-index".into())),],
            )
            .unwrap_err()
            .to_string()
            .contains("index must be an integer")
        );
    }

    #[test]
    fn exact_zero_udf_return_shape_guards_report_contract_errors() {
        let too_wide = (0..128)
            .map(|value| ScalarValue::Int64(Some(value)))
            .collect::<Vec<_>>();
        assert!(
            invoke_test_udf(&CypherDynamicHetList::new(), too_wide)
                .unwrap_err()
                .to_string()
                .contains("exceeds 127 elements")
        );
        assert!(
            invoke_test_udf_with_return_type(
                &CypherDynamicHetList::new(),
                vec![ScalarValue::Int64(Some(1))],
                DataType::Int64,
            )
            .unwrap_err()
            .to_string()
            .contains("non-list return type")
        );
        assert!(
            invoke_test_udf_with_return_type(
                &CypherDynamicHetList::new(),
                vec![ScalarValue::Int64(Some(1))],
                DataType::new_list(DataType::Int64, true),
            )
            .unwrap_err()
            .to_string()
            .contains("non-struct element type")
        );
        assert!(
            invoke_test_udf_with_return_type(
                &CypherListPlus::new(),
                vec![
                    ScalarValue::List(ScalarValue::new_list(
                        &[ScalarValue::Int64(Some(1))],
                        &DataType::Int64,
                        true,
                    )),
                    ScalarValue::Int64(Some(2)),
                ],
                DataType::Int64,
            )
            .unwrap_err()
            .to_string()
            .contains("return type is not a list")
        );
    }

    #[test]
    fn exact_zero_tagged_append_rejects_incompatible_arrow_shapes() {
        use datafusion::arrow::array::{ArrayRef, Int64Array};

        let scalar: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        assert!(
            invoke_tagged_list_element_plus(&scalar, &scalar, &DataType::Int64)
                .unwrap()
                .is_none()
        );
        let list = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Int64(Some(1))],
            &DataType::Int64,
            true,
        ))
        .to_array_of_size(1)
        .unwrap();
        assert!(
            invoke_tagged_list_element_plus(&list, &scalar, &DataType::Int64)
                .unwrap()
                .is_none()
        );

        let map = const_map_scalar(&[("value".into(), ScalarValue::Int64(Some(1)))])
            .unwrap()
            .to_array_of_size(1)
            .unwrap();
        assert!(
            invoke_tagged_list_element_plus(&list, &map, &DataType::Int64)
                .unwrap()
                .is_none()
        );
        assert!(
            invoke_tagged_list_element_plus(
                &list,
                &map,
                &DataType::new_list(map.data_type().clone(), true),
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn exact_zero_heterogeneous_depth_and_builder_type_matrix_is_total() {
        use datafusion::arrow::datatypes::{Field, Fields};

        let primitives = [
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
        ];
        for data_type in primitives {
            assert_eq!(het_depth_for_data_type(&data_type), Some(0));
        }
        assert_eq!(
            het_depth_for_data_type(&DataType::new_list(
                DataType::new_list(DataType::Int64, true),
                true,
            )),
            Some(2)
        );
        assert_eq!(het_depth_for_data_type(&DataType::Binary), None);

        let map_type = DataType::Struct(Fields::from(vec![Field::new(
            "value",
            DataType::new_list(DataType::Int64, true),
            true,
        )]));
        assert_eq!(het_depth_for_data_type(&map_type), Some(2));
        assert!(build_het_struct(&[ScalarValue::Binary(Some(vec![1]))], 0).is_none());

        let nested_list = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Int64(Some(1))],
            &DataType::Int64,
            true,
        ));
        assert!(build_het_struct(std::slice::from_ref(&nested_list), 0).is_none());
        assert!(build_het_struct(&[const_map_scalar(&[]).unwrap()], 0).is_none());
        let built = build_het_struct(
            &[
                ScalarValue::Int64(Some(1)),
                ScalarValue::Float64(Some(2.0)),
                ScalarValue::LargeUtf8(Some("three".into())),
                ScalarValue::Boolean(Some(true)),
                nested_list,
                const_map_scalar(&[("k".into(), ScalarValue::Int64(Some(4)))]).unwrap(),
                ScalarValue::Null,
            ],
            1,
        )
        .unwrap();
        assert_eq!(built.len(), 7);
        assert!(built.is_null(6));
    }

    #[test]
    fn exact_zero_map_union_rejects_non_maps_and_conflicting_key_types() {
        assert!(all_map_union_list(&[ScalarValue::Int64(Some(1))]).is_none());
        let int_map = const_map_scalar(&[("key".into(), ScalarValue::Int64(Some(1)))]).unwrap();
        let text_map =
            const_map_scalar(&[("key".into(), ScalarValue::Utf8(Some("one".into())))]).unwrap();
        assert!(all_map_union_list(&[int_map, text_map]).is_none());

        let left = const_map_scalar(&[("left".into(), ScalarValue::Int64(Some(1)))]).unwrap();
        let right =
            const_map_scalar(&[("right".into(), ScalarValue::Utf8(Some("r".into())))]).unwrap();
        let union = all_map_union_list(&[left, ScalarValue::Null, right]).unwrap();
        let DfExpr::Literal(ScalarValue::List(values), None) = union else {
            panic!("map union must const-fold to a list")
        };
        assert_eq!(values.value(0).len(), 3);
        assert!(values.value(0).is_null(1));
    }

    #[test]
    fn exact_zero_map_and_subscript_error_guards_are_precise() {
        let null_keys = invoke_test_udf(&CypherMapKeys::new(), vec![ScalarValue::Null]).unwrap();
        assert!(null_keys.is_null(0));
        assert!(
            invoke_test_udf(&CypherMapKeys::new(), vec![ScalarValue::Int64(Some(1))])
                .unwrap_err()
                .to_string()
                .contains("keys() requires a map")
        );
        assert!(
            invoke_test_udf(
                &CypherStaticValueAccess::new("key".into()),
                vec![ScalarValue::Int64(Some(1))],
            )
            .unwrap_err()
            .to_string()
            .contains("property access requires a map")
        );
        assert!(
            invoke_test_udf(
                &CypherValueAccess::new(),
                vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(0)),],
            )
            .unwrap_err()
            .to_string()
            .contains("requires a list or map")
        );

        let map = const_map_scalar(&[("key".into(), ScalarValue::Int64(Some(7)))]).unwrap();
        let mismatch = invoke_test_udf_with_return_type(
            &CypherStaticValueAccess::new("key".into()),
            vec![map],
            DataType::Utf8,
        )
        .unwrap_err();
        assert!(mismatch.to_string().contains("incompatible runtime type"));

        assert_eq!(value_access_return_type(None).unwrap(), DataType::Null);
        assert_eq!(
            value_access_return_type(Some(&DataType::Null)).unwrap(),
            DataType::Null
        );
        assert_eq!(
            value_access_return_type(Some(&DataType::new_list(DataType::Int64, true))).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            static_value_access_return_type(None, "key").unwrap(),
            DataType::Null
        );
    }

    #[test]
    fn exact_zero_heterogeneous_promotion_preserves_struct_and_list_validity() {
        use datafusion::arrow::array::{Array, Float64Array, ListArray, StructArray};
        use datafusion::arrow::datatypes::{Field, Fields};

        let source_map = const_map_scalar(&[("present".into(), ScalarValue::Int64(Some(7)))])
            .unwrap()
            .to_array_of_size(1)
            .unwrap();
        assert!(Arc::ptr_eq(
            &source_map,
            &promote_het_array(&source_map, source_map.data_type()).unwrap()
        ));
        let target = DataType::Struct(Fields::from(vec![
            Field::new("present", DataType::Float64, true),
            Field::new("missing", DataType::Utf8, true),
        ]));
        let promoted = promote_het_array(&source_map, &target).unwrap();
        let promoted = promoted.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(promoted.num_columns(), 2);
        assert_eq!(
            promoted
                .column_by_name("present")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            7.0
        );
        assert!(promoted.column_by_name("missing").unwrap().is_null(0));

        let source_list = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Int64(Some(1)), ScalarValue::Int64(None)],
            &DataType::Int64,
            true,
        ))
        .to_array_of_size(1)
        .unwrap();
        let target_list = DataType::new_list(DataType::Float64, true);
        let promoted = promote_het_array(&source_list, &target_list).unwrap();
        let promoted = promoted.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(promoted.value_length(0), 2);
        assert!(promoted.value(0).is_null(1));
    }

    #[test]
    fn exact_zero_uncorrelated_list_comprehension_preserves_null_and_empty_rows() {
        use datafusion::arrow::array::{Array, Int64Builder, ListArray, ListBuilder};
        use datafusion::arrow::datatypes::Field;
        use datafusion::config::ConfigOptions;

        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.append_null();
        builder.append(true);
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true);
        let input = Arc::new(builder.finish()) as datafusion::arrow::array::ArrayRef;
        let udf = CypherListComp::new(None, None, "__gf_elem".into(), vec![]);
        let return_type = udf.return_type(&[input.data_type().clone()]).unwrap();
        let output = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(input)],
                arg_fields: vec![Arc::new(Field::new(
                    "list",
                    DataType::new_list(DataType::Int64, true),
                    true,
                ))],
                number_rows: 3,
                return_field: Arc::new(Field::new("out", return_type, true)),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap()
            .into_array(3)
            .unwrap();
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(output.is_null(0));
        assert_eq!(output.value_length(1), 0);
        assert_eq!(output.value_length(2), 2);
    }
}
