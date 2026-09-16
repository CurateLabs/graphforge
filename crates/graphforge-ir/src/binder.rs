//! Binder — lowers an [`AstQuery`] to a [`GraphPlan`].
//!
//! The binder is the stage between the parser and the execution engine.  It:
//! - Resolves label/relation-type/property strings to integer IDs
//! - Tracks variable scope and allocates [`VarId`]s
//! - Emits the correct operators (e.g. relationship patterns become `Expand`,
//!   node patterns become `NodeScan`)
//! - Populates the [`crate::ExprArena`] while lowering WHERE / RETURN expressions
//! - Supports three ontology modes (exploratory / advisory / strict)

mod expressions;
mod patterns;
mod projection;
mod writes;

use self::projection::expr_contains_aggregate;
use crate::catalog::RuntimeCatalog;
use crate::composition_binding::CompositionBindingContext;
use crate::expr::{IrExpr, IrLiteral};
use crate::plan::{GraphOp, GraphPlan, GraphPlanBuilder, OntologyMode};
use crate::{OntologyVersion, ProcedureRegistry, ProcedureYield, VarId};
use graphforge_ast::{
    AstClause, AstQuery, CallClause, DialectVersion, Expr, FunctionCall, Literal, VarRef,
};
use graphforge_core::Span;
use graphforge_ontology::OntologyHandle;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// BindError
// ---------------------------------------------------------------------------

pub use graphforge_core::{BindError, BindErrorKind};

/// The semantic kind a pattern variable is bound to, tracked so a later use
/// with an incompatible kind is rejected (openCypher `VariableTypeConflict`,
/// #956). Value-typed WITH/RETURN aliases are deliberately NOT tracked — only
/// pattern bindings (node / relationship / path) participate in conflict
/// detection. Value aliases are detected when a later pattern tries to reuse
/// their already-bound name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VarKind {
    /// Runtime-polymorphic value, such as an UNWIND element or literal null.
    Unknown,
    /// Bound by a node pattern element `(n)`.
    Node,
    /// Bound by a relationship pattern element `-[r]-` (fixed or variable-length).
    Relationship,
}

impl std::fmt::Display for VarKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            VarKind::Unknown => "a runtime value",
            VarKind::Node => "a node",
            VarKind::Relationship => "a relationship",
        })
    }
}

// ---------------------------------------------------------------------------
// Binder
// ---------------------------------------------------------------------------

/// Lowers an [`AstQuery`] to a typed [`GraphPlan`].
///
/// # Construction
///
/// ```
/// use std::sync::{Arc, Mutex};
/// use graphforge_ir::{Binder, OntologyMode, RuntimeCatalog};
///
/// let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
/// let binder = Binder::new(None, catalog, OntologyMode::Exploratory);
/// ```
pub struct Binder {
    ontology: Option<OntologyHandle>,
    catalog: Arc<Mutex<RuntimeCatalog>>,
    mode: OntologyMode,
    procedures: Arc<ProcedureRegistry>,
    typed_uuid_params: HashMap<String, UuidParamClass>,
    composition: Option<Arc<CompositionBindingContext>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UuidParamClass {
    ExactUuid,
    ContainsUuid,
}

impl Binder {
    /// Creates a new binder.
    ///
    /// - `ontology`: the compiled ontology handle, or `None` in exploratory mode.
    /// - `catalog`: shared runtime catalog for auto-assigning IDs to unknown types.
    /// - `mode`: controls how unknown labels/types are handled.
    #[must_use]
    pub fn new(
        ontology: Option<OntologyHandle>,
        catalog: Arc<Mutex<RuntimeCatalog>>,
        mode: OntologyMode,
    ) -> Self {
        Self {
            ontology,
            catalog,
            mode,
            procedures: Arc::new(ProcedureRegistry::new()),
            typed_uuid_params: HashMap::new(),
            composition: None,
        }
    }

    /// Supplies compiled multi-ontology binding authority.
    #[must_use]
    pub fn with_composition(mut self, composition: Arc<CompositionBindingContext>) -> Self {
        self.composition = Some(composition);
        self
    }

    /// Supplies the procedures available while binding `CALL` clauses.
    #[must_use]
    pub fn with_procedures(mut self, procedures: Arc<ProcedureRegistry>) -> Self {
        self.procedures = procedures;
        self
    }

    /// Supplies query literals for bind-time validation of typed UUID predicates.
    #[must_use]
    pub fn with_parameter_literals(mut self, params: &HashMap<String, IrLiteral>) -> Self {
        self.typed_uuid_params = params
            .iter()
            .filter_map(|(name, value)| {
                classify_uuid_parameter(value).map(|class| (name.clone(), class))
            })
            .collect();
        self
    }

    /// Bind an `AstQuery` and produce a [`GraphPlan`].
    ///
    /// In strict mode, all errors are collected before returning so that a
    /// single call surfaces every problem in the query.
    ///
    /// # Errors
    ///
    /// Returns a non-empty `Vec<BindError>` when the query has semantic errors
    /// (strict-mode type violations, undeclared variables, etc.).
    pub fn bind(&self, ast: &AstQuery) -> Result<GraphPlan, Vec<BindError>> {
        // Bind against an isolated catalog snapshot while holding the shared
        // lock. A successful bind publishes exactly the snapshot whose IDs
        // appear in the plan; a failed bind drops every staged observation.
        let mut shared_catalog = self.catalog.lock().expect("runtime catalog poisoned");
        let staged_catalog = Arc::new(Mutex::new(shared_catalog.clone()));
        let staged_binder = Self {
            ontology: self.ontology.clone(),
            catalog: Arc::clone(&staged_catalog),
            mode: self.mode,
            procedures: Arc::clone(&self.procedures),
            typed_uuid_params: self.typed_uuid_params.clone(),
            composition: self.composition.clone(),
        };
        let result = staged_binder.bind_staged(ast);
        if result.is_ok() {
            *shared_catalog = staged_catalog
                .lock()
                .expect("staged runtime catalog poisoned")
                .clone();
        }
        result
    }

    fn bind_staged(&self, ast: &AstQuery) -> Result<GraphPlan, Vec<BindError>> {
        let dialect = match ast.dialect {
            DialectVersion::OpenCypher9 => "openCypher",
        };

        let ontology_version: Option<OntologyVersion> = self
            .ontology
            .as_ref()
            .map(|h| OntologyVersion::from(format!("{}:{}", h.version(), h.checksum())));

        if ast
            .clauses
            .iter()
            .any(|clause| matches!(clause, AstClause::Union(_)))
        {
            return self.bind_union_query(ast, dialect, ontology_version);
        }

        let mut builder = GraphPlan::builder(dialect).ontology_mode(self.mode);
        if let Some(composition) = &self.composition {
            builder = builder.composition_fingerprint(composition.fingerprint());
        }
        if let Some(v) = ontology_version {
            builder = builder.ontology_version(v);
        }

        let mut state = BinderState {
            vars: HashMap::new(),
            path_vars: HashMap::new(),
            node_vars: HashMap::new(),
            edge_vars: HashMap::new(),
            edge_rel_names: HashMap::new(),
            scalar_list_edges: HashSet::new(),
            var_kinds: HashMap::new(),
            next_var: 0,
            builder,
            errors: Vec::new(),
            warnings: Vec::new(),
            captured_pattern_comprehensions: None,
            existential_depth: 0,
            standalone_call: ast.clauses.len() == 1 && matches!(ast.clauses[0], AstClause::Call(_)),
        };

        for clause in &ast.clauses {
            self.lower_clause(clause, &mut state);
        }

        if !state.errors.is_empty() {
            return Err(state.errors);
        }
        Ok(state.builder.build())
    }

    fn bind_union_query(
        &self,
        ast: &AstQuery,
        dialect: &str,
        ontology_version: Option<OntologyVersion>,
    ) -> Result<GraphPlan, Vec<BindError>> {
        let markers: Vec<(usize, &graphforge_ast::UnionClause)> = ast
            .clauses
            .iter()
            .enumerate()
            .filter_map(|(index, clause)| match clause {
                AstClause::Union(union) => Some((index, union)),
                _ => None,
            })
            .collect();
        let all = markers[0].1.all;
        if markers.iter().any(|(_, marker)| marker.all != all) {
            return Err(vec![BindError::new(
                BindErrorKind::InvalidArgument,
                markers[0].1.span,
                "InvalidCombinationOfUnion: UNION and UNION ALL cannot be mixed",
            )]);
        }

        let mut starts = vec![0];
        starts.extend(markers.iter().map(|(index, _)| index + 1));
        let mut ends: Vec<usize> = markers.iter().map(|(index, _)| *index).collect();
        ends.push(ast.clauses.len());
        let mut inputs = Vec::with_capacity(starts.len());
        let mut errors = Vec::new();
        for (start, end) in starts.into_iter().zip(ends) {
            if start == end {
                errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    ast.span,
                    "UNION requires a query on both sides",
                ));
                continue;
            }
            let branch = AstQuery {
                dialect: ast.dialect,
                clauses: ast.clauses[start..end].to_vec(),
                span: ast.span,
            };
            match self.bind_staged(&branch) {
                Ok(plan) => inputs.push(plan),
                Err(mut branch_errors) => errors.append(&mut branch_errors),
            }
        }
        if !errors.is_empty() {
            return Err(errors);
        }

        let expected = union_output_names(&inputs[0]);
        if expected.is_none()
            || inputs
                .iter()
                .skip(1)
                .any(|branch| union_output_names(branch) != expected)
        {
            return Err(vec![BindError::new(
                BindErrorKind::InvalidArgument,
                ast.span,
                "DifferentColumnsInUnion: all UNION branches must return the same columns",
            )]);
        }

        let mut builder = GraphPlan::builder(dialect).ontology_mode(self.mode);
        if let Some(composition) = &self.composition {
            builder = builder.composition_fingerprint(composition.fingerprint());
        }
        if let Some(version) = ontology_version {
            builder = builder.ontology_version(version);
        }
        Ok(builder.push_op(GraphOp::Union { all, inputs }).build())
    }

    // -----------------------------------------------------------------------
    // Clause lowering
    // -----------------------------------------------------------------------

    fn lower_clause(&self, clause: &AstClause, s: &mut BinderState) {
        match clause {
            AstClause::Match(m) => self.lower_match(m, false, s),
            AstClause::OptionalMatch(m) => self.lower_match(m, true, s),
            AstClause::Where(w) => self.lower_where(w, s),
            AstClause::With(w) => self.lower_with(w, s),
            AstClause::Return(r) => self.lower_return(r, s),
            AstClause::Unwind(u) => self.lower_unwind(u, s),
            AstClause::Create(c) => self.lower_create(c, s),
            AstClause::Merge(m) => self.lower_merge(m, s),
            AstClause::Union(u) => {
                s.builder.push_op_mut(GraphOp::Union {
                    all: u.all,
                    inputs: vec![],
                });
            }
            AstClause::Delete(d) => self.lower_delete(d, s),
            AstClause::Set(st) => self.lower_set(st, s),
            AstClause::Remove(r) => self.lower_remove(r, s),
            AstClause::Call(c) => self.lower_call(c, s),
            // `AstClause` is `#[non_exhaustive]`, so a catch-all is required.
            // Keep it an explicit *error* (never a silent no-op) so any clause
            // added upstream surfaces loudly until it is wired in here.
            _ => s.errors.push(BindError::new(
                BindErrorKind::UnsupportedClause,
                Span::default(),
                "clause is not yet implemented",
            )),
        }
    }

    fn lower_unwind(&self, u: &graphforge_ast::UnwindClause, s: &mut BinderState) {
        let list_expr = self.lower_expr(&u.expr, u.span, s);
        let alias = ensure_var_name(&u.alias, s);
        s.var_kinds.insert(alias, VarKind::Unknown);
        s.builder.push_op_mut(GraphOp::Unwind { list_expr, alias });
    }

    /// Bind a registered procedure call and introduce its yielded outputs.
    #[allow(clippy::too_many_lines)]
    fn lower_call(&self, call: &CallClause, s: &mut BinderState) {
        if call.procedure.is_empty() {
            s.errors.push(BindError::new(
                BindErrorKind::UnsupportedClause,
                call.span,
                "CALL subqueries are not procedure calls",
            ));
            return;
        }

        let name = call.procedure.join(".");
        let Some(procedure) = self.procedures.get(&name).cloned() else {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                call.span,
                format!("ProcedureNotFound: `{name}` is not registered"),
            ));
            return;
        };

        if call.args.iter().any(expr_contains_aggregate) {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                call.span,
                "InvalidAggregation: aggregate expressions are not valid procedure arguments",
            ));
            return;
        }

        let args = if call.args_explicit {
            if call.args.len() != procedure.inputs.len() {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    call.span,
                    format!(
                        "InvalidNumberOfArguments: `{name}` expects {}, found {}",
                        procedure.inputs.len(),
                        call.args.len()
                    ),
                ));
                return;
            }
            for (arg, field) in call.args.iter().zip(&procedure.inputs) {
                if !procedure_argument_type_matches(arg, field) {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        arg.span(),
                        format!("InvalidArgumentType: expected {}", field.type_name),
                    ));
                }
            }
            call.args
                .iter()
                .map(|arg| self.lower_expr(arg, call.span, s))
                .collect()
        } else {
            if !s.standalone_call && !procedure.inputs.is_empty() {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    call.span,
                    "InvalidArgumentPassingMode: in-query calls require explicit arguments",
                ));
                return;
            }
            procedure
                .inputs
                .iter()
                .map(|field| s.builder.push_expr(IrExpr::Parameter(field.name.clone())))
                .collect()
        };

        let yield_all = call.yield_items.len() == 1
            && matches!(&call.yield_items[0].expr, Expr::Var(VarRef { name, .. }) if name == "*");
        if yield_all && !s.standalone_call {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                call.span,
                "UnexpectedSyntax: YIELD * is only valid for standalone calls",
            ));
            return;
        }
        let selected: Vec<(String, String)> = if call.yield_items.is_empty() && !s.standalone_call {
            vec![]
        } else if call.yield_items.is_empty() || yield_all {
            procedure
                .outputs
                .iter()
                .map(|field| (field.name.clone(), field.name.clone()))
                .collect()
        } else {
            let mut selected = Vec::with_capacity(call.yield_items.len());
            for item in &call.yield_items {
                let Expr::Var(VarRef { name: field, .. }) = &item.expr else {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        item.span,
                        "YIELD items must name procedure outputs",
                    ));
                    continue;
                };
                if !procedure.outputs.iter().any(|output| output.name == *field) {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        item.span,
                        format!("ProcedureOutputNotFound: `{name}` has no output `{field}`"),
                    ));
                    continue;
                }
                selected.push((
                    field.clone(),
                    item.alias.clone().unwrap_or_else(|| field.clone()),
                ));
            }
            selected
        };

        let mut yields = Vec::with_capacity(selected.len());
        for (field, alias) in selected {
            if s.vars.contains_key(&alias) {
                s.errors.push(BindError::new(
                    BindErrorKind::DuplicateVariable,
                    call.span,
                    format!("VariableAlreadyBound: `{alias}`"),
                ));
                continue;
            }
            let var = ensure_var_name(&alias, s);
            yields.push(ProcedureYield { field, alias, var });
        }

        s.builder.push_op_mut(GraphOp::Call {
            procedure,
            args,
            yields,
        });
    }

    // -----------------------------------------------------------------------
    // Variable management
    // -----------------------------------------------------------------------
}

fn is_function_named(call: &FunctionCall, name: &str) -> bool {
    matches!(call.name.as_slice(), [n] if n.eq_ignore_ascii_case(name))
}

fn procedure_argument_type_matches(expr: &Expr, field: &crate::ProcedureField) -> bool {
    let expected = field.type_name.to_ascii_uppercase();
    match expr {
        Expr::Literal(Literal::Null(_)) => field.nullable,
        Expr::Literal(Literal::Int(_, _)) => {
            matches!(expected.as_str(), "INTEGER" | "FLOAT" | "NUMBER")
        }
        Expr::Literal(Literal::Float(_, _)) => matches!(expected.as_str(), "FLOAT" | "NUMBER"),
        Expr::Literal(Literal::Str(_, _)) => expected == "STRING",
        Expr::Literal(Literal::Bool(_, _)) => expected == "BOOLEAN",
        Expr::Parenthesized { inner, .. } => procedure_argument_type_matches(inner, field),
        _ => true,
    }
}

fn union_output_names(plan: &GraphPlan) -> Option<Vec<String>> {
    match plan.ops.last() {
        Some(GraphOp::Project { items, .. } | GraphOp::With { items, .. }) => Some(
            items
                .iter()
                .map(|item| item.alias.clone().unwrap_or_default())
                .collect(),
        ),
        Some(GraphOp::Aggregate {
            group_aliases,
            aggs,
            ..
        }) => Some(
            group_aliases
                .iter()
                .map(|alias| alias.clone().unwrap_or_default())
                .chain(aggs.iter().map(|agg| agg.alias.clone()))
                .collect(),
        ),
        _ => None,
    }
}

fn ensure_var(name: Option<&String>, s: &mut BinderState) -> VarId {
    if let Some(n) = name {
        ensure_var_name(n, s)
    } else {
        alloc_anon_var(s)
    }
}

/// Record a named pattern variable's [`VarKind`], or emit a
/// `VariableKindConflict` bind error if `id` was already bound to a different
/// kind (openCypher `VariableTypeConflict`, e.g. a relationship variable reused
/// as a node pattern — #956). Only called for NAMED variables; anonymous
/// anonymous elements cannot conflict but still record their owner-relevant kind.
fn ensure_pattern_var(name: Option<&str>, kind: VarKind, span: Span, s: &mut BinderState) -> VarId {
    let Some(name) = name else {
        let id = alloc_anon_var(s);
        s.var_kinds.insert(id, kind);
        return id;
    };
    if let Some(&existing) = s.vars.get(name)
        && !s.var_kinds.contains_key(&existing)
        && !s.node_vars.contains_key(&existing)
        && !s.edge_rel_names.contains_key(&existing)
    {
        s.errors.push(BindError::new(
            BindErrorKind::VariableKindConflict,
            span,
            format!("variable `{name}` is bound as a value but used here as {kind}"),
        ));
        return existing;
    }
    let id = ensure_var_name(name, s);
    bind_var_kind(id, kind, name, span, s);
    id
}

fn bind_var_kind(id: VarId, kind: VarKind, name: &str, span: Span, s: &mut BinderState) {
    // A name already bound as a PATH variable (`MATCH r = ()-[]-()`) reused as a
    // node/relationship pattern (`MATCH (r)`) is a kind conflict too. Path vars
    // live in `path_vars` (no `VarId` in `vars`/`var_kinds`), so `ensure_var`
    // would otherwise mint a fresh var and miss the clash (#956).
    if s.path_vars.contains_key(name) {
        s.errors.push(BindError::new(
            BindErrorKind::VariableKindConflict,
            span,
            format!("variable `{name}` is bound as a path but used here as {kind}"),
        ));
        return;
    }
    match s.var_kinds.get(&id) {
        Some(VarKind::Unknown) => {
            s.var_kinds.insert(id, kind);
        }
        Some(prev) if *prev != kind => s.errors.push(BindError::new(
            BindErrorKind::VariableKindConflict,
            span,
            format!("variable `{name}` is bound as {prev} but used here as {kind}"),
        )),
        _ => {
            s.var_kinds.insert(id, kind);
        }
    }
}

fn alloc_anon_var(s: &mut BinderState) -> VarId {
    let id = VarId(s.next_var);
    s.next_var += 1;
    id
}

fn ensure_var_name(name: &str, s: &mut BinderState) -> VarId {
    if let Some(&existing) = s.vars.get(name) {
        return existing;
    }
    let id = VarId(s.next_var);
    s.next_var += 1;
    s.vars.insert(name.to_owned(), id);
    id
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BoundPropertyOwner {
    Entity(Option<String>),
    Relationship(Option<String>),
    Value,
}
fn property_owner_for_expr(expr: &Expr, s: &BinderState) -> BoundPropertyOwner {
    let Expr::Var(VarRef { name, .. }) = expr else {
        return BoundPropertyOwner::Value;
    };
    s.vars.get(name).map_or(BoundPropertyOwner::Value, |var| {
        property_owner_for_var(*var, s)
    })
}
fn property_owner_for_var(var: VarId, s: &BinderState) -> BoundPropertyOwner {
    let kind = s.var_kinds.get(&var).copied().or_else(|| {
        if s.node_vars.contains_key(&var) {
            Some(VarKind::Node)
        } else if s.edge_rel_names.contains_key(&var) {
            Some(VarKind::Relationship)
        } else {
            None
        }
    });
    match kind {
        Some(VarKind::Node) => BoundPropertyOwner::Entity(s.node_vars.get(&var).cloned().flatten()),
        Some(VarKind::Relationship) => {
            BoundPropertyOwner::Relationship(s.edge_rel_names.get(&var).cloned().flatten())
        }
        Some(VarKind::Unknown) | None => BoundPropertyOwner::Value,
    }
}

// ---------------------------------------------------------------------------
// BinderState
// ---------------------------------------------------------------------------

struct BinderState {
    vars: HashMap<String, VarId>,
    /// Named path variables (`MATCH p = (a)-[*]->(b)`), kept out of `vars`:
    /// a path is not a column-backed value — `nodes(p)` / `relationships(p)` /
    /// `length(p)` are rewritten at bind time onto the constituent variables
    /// recorded here, and `p` itself never reaches the plan (#754).
    path_vars: HashMap<String, PathBinding>,
    /// Variables bound to a node pattern element (those with a `NodeScan`),
    /// mapped to the pattern's label name (if any). A bare `RETURN n` over one
    /// of these materializes a whole node value (#785); the label is passed
    /// through to lowering since the ontology map is empty in exploratory mode.
    node_vars: HashMap<VarId, Option<String>>,
    /// Fixed-hop relationship variables mapped to their `(src, dst)` node vars,
    /// recorded at `Expand` build. `startNode(r)` / `endNode(r)` over a matched
    /// relationship rewrite onto these endpoints (#753) — so they return the
    /// node value (reusing #785), not the raw UUID. Variable-length edges bind
    /// to a list, not one relationship, so they are deliberately not recorded.
    edge_vars: HashMap<VarId, (VarId, VarId)>,
    /// Fixed-hop relationship variables mapped to their relation type name as
    /// written in the pattern. A bare `RETURN r` uses this to materialize a
    /// whole relationship value; variable-length relationship vars are lists and
    /// deliberately absent here.
    edge_rel_names: HashMap<VarId, Option<String>>,
    /// Explicit variable-length syntax whose `1..1` bounds route through the
    /// scalar fixed-hop executor but whose relationship variable is still a list.
    scalar_list_edges: HashSet<VarId>,
    /// Each named pattern variable's semantic kind (node vs relationship),
    /// used to reject a later incompatible use as a `VariableKindConflict`
    /// (#956). Keyed by the immutable `VarId`; entries never need clearing
    /// across a WITH scope reset because a fresh scope mints fresh `VarId`s.
    var_kinds: HashMap<VarId, VarKind>,
    next_var: u32,
    builder: GraphPlanBuilder,
    errors: Vec<BindError>,
    warnings: Vec<BindError>,
    /// Nested pattern comprehensions captured while binding a graph-valued list
    /// comprehension. `None` means ordinary relational pattern-comprehension
    /// lowering; `Some` lifts the child into one list-element graph operation.
    captured_pattern_comprehensions: Option<Vec<(Box<GraphPlan>, VarId)>>,
    /// Lexical nesting level of `exists { ... }` bodies.
    existential_depth: usize,
    /// True only when the whole query consists of one procedure call.
    standalone_call: bool,
}

/// The ordered composition of a named path: the node variables along the
/// traversal and one entry per relationship segment, in pattern order.
#[derive(Clone)]
struct PathBinding {
    /// Node variables in traversal order (`segments.len() + 1` entries).
    nodes: Vec<VarId>,
    segments: Vec<PathSegment>,
}

/// One relationship segment of a named path.
#[derive(Clone)]
struct PathSegment {
    edge: VarId,
    /// `true` for a variable-length hop (`[*..]`) — the edge var binds to a
    /// relationship-list column; `false` for a fixed single hop.
    var_len: bool,
    /// The relation-type name as written (`KNOWS` in `[:KNOWS]`), captured so
    /// a fixed segment's `relationships(p)` struct can carry `rel_type`
    /// without the lowerer needing catalog access. `None` for an untyped hop.
    rel_name: Option<String>,
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

fn classify_uuid_parameter(value: &IrLiteral) -> Option<UuidParamClass> {
    match value {
        IrLiteral::Uuid(_) => Some(UuidParamClass::ExactUuid),
        IrLiteral::List(items) if items.iter().any(ir_literal_contains_uuid) => {
            Some(UuidParamClass::ContainsUuid)
        }
        IrLiteral::Map(entries)
            if entries
                .iter()
                .any(|(_, value)| ir_literal_contains_uuid(value)) =>
        {
            Some(UuidParamClass::ContainsUuid)
        }
        _ => None,
    }
}

fn ir_literal_contains_uuid(value: &IrLiteral) -> bool {
    classify_uuid_parameter(value).is_some()
}

#[cfg(test)]
mod tests;
