//! Binder — lowers an [`AstQuery`] to a [`GraphPlan`].
//!
//! The binder is the stage between the parser and the execution engine.  It:
//! - Resolves label/relation-type/property strings to integer IDs
//! - Tracks variable scope and allocates [`VarId`]s
//! - Emits the correct operators (e.g. relationship patterns become `Expand`,
//!   node patterns become `NodeScan`)
//! - Populates the [`ExprArena`] while lowering WHERE / RETURN expressions
//! - Supports three ontology modes (exploratory / advisory / strict)

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use gf_ast::{
    AstClause, AstQuery, BinaryOpKind as AstBinOp, CallClause, CaseExpr, CreateClause,
    DialectVersion, ExistentialSubqueryBody, Expr, FunctionCall, LabelPredicate, Literal,
    MapLiteral, PathElement, PathPattern, PatternPredicate, PropertyAccess, RemoveClause,
    RemoveItem, ReturnItem, SetClause, SetItem, SortItem, SortOrder as AstSortOrder, StringOpKind,
    UnaryOpKind as AstUnOp, VarRef, WhereClause, WithClause,
};
use gf_core::{PropId, Span, TypeId};
use gf_ontology::OntologyHandle;

use crate::catalog::RuntimeCatalog;
use crate::expr::{BinaryOpKind, CaseArm, IrExpr, IrLiteral, UnaryOpKind};
use crate::plan::{
    GraphOp, GraphPlan, GraphPlanBuilder, OntologyMode, PATTERN_COMPREHENSION_VALUE_ALIAS, SortKey,
};
use crate::{
    AggExpr, AggFunc, CreateEdgeSpec, CreateNodeSpec, CreatePattern, Direction, ExprId,
    MergeSetItem, OntologyVersion, ProcedureRegistry, ProcedureYield, ProjectItem, RemovePropItem,
    SetMapItem, SetPropItem, SortOrder, VarId,
};

// ---------------------------------------------------------------------------
// BindError
// ---------------------------------------------------------------------------

/// The kind of a binder error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindErrorKind {
    /// A label name was not found in the ontology (strict mode).
    UnknownLabel,
    /// A relation type name was not found in the ontology (strict mode).
    UnknownRelationType,
    /// A property name was not found in the ontology (strict mode).
    UnknownProperty,
    /// A variable was referenced before it was introduced by a MATCH pattern.
    UndeclaredVariable,
    /// The same variable was introduced more than once in a conflicting way.
    DuplicateVariable,
    /// A property name is ambiguous across multiple owner labels.
    AmbiguousProperty,
    /// A clause is recognized by the parser but not yet implemented by the
    /// binder/executor (SET/REMOVE/CALL). Surfaced as an error instead
    /// of a silent no-op (#724).
    UnsupportedClause,
    /// A `DELETE` target was not a plain bound variable (e.g. `DELETE n.prop`
    /// or `DELETE n + 1`). Only `DELETE <var>` is supported (#740).
    InvalidDeleteTarget,
    /// A variable is used with two incompatible kinds — e.g. a relationship
    /// variable reused as a node pattern (`MATCH ()-[r]-() MATCH (r)`).
    /// openCypher rejects this as a compile-time `VariableTypeConflict` (#956).
    VariableKindConflict,
    /// An already-bound variable is re-declared by a CREATE/MERGE pattern
    /// (`MATCH (a) CREATE (a)`, a reused relationship variable, a bound node
    /// given new labels/properties). openCypher `VariableAlreadyBound` (#956).
    VariableAlreadyBound,
    /// A clause or function argument is semantically invalid — a non-integer
    /// `range()`/`SKIP`/`LIMIT` argument, an aggregate in `WHERE`, a CREATE
    /// relationship with no/var-length/undirected type, a UNION column
    /// mismatch, etc. Covers openCypher's `ArgumentError` / `InvalidAggregation`
    /// / `NoSingleRelationshipType` / … — the harness checks the phase, not the
    /// sub-code, so one kind with a descriptive message suffices (#956).
    InvalidArgument,
}

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

/// A semantic error or warning produced by the binder.
#[derive(Debug, Clone)]
pub struct BindError {
    /// The kind of error.
    pub kind: BindErrorKind,
    /// Source location of the offending token.
    pub span: Span,
    /// Human-readable message.
    pub message: String,
}

impl BindError {
    fn new(kind: BindErrorKind, span: Span, message: impl Into<String>) -> Self {
        Self {
            kind,
            span,
            message: message.into(),
        }
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
/// use gf_ir::{Binder, OntologyMode, RuntimeCatalog};
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
        }
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
        let markers: Vec<(usize, &gf_ast::UnionClause)> = ast
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

    fn lower_match(&self, m: &gf_ast::MatchClause, optional: bool, s: &mut BinderState) {
        if optional {
            // Collect ops into a sub-builder, then wrap in Optional.
            let mut sub_state = BinderState {
                vars: s.vars.clone(),
                path_vars: s.path_vars.clone(),
                node_vars: s.node_vars.clone(),
                edge_vars: s.edge_vars.clone(),
                edge_rel_names: s.edge_rel_names.clone(),
                scalar_list_edges: s.scalar_list_edges.clone(),
                var_kinds: s.var_kinds.clone(),
                next_var: s.next_var,
                builder: GraphPlan::builder("openCypher").ontology_mode(self.mode),
                errors: Vec::new(),
                warnings: Vec::new(),
                captured_pattern_comprehensions: None,
                existential_depth: s.existential_depth,
                standalone_call: false,
            };
            for pat in &m.patterns {
                self.lower_path_pattern(pat, &mut sub_state);
            }
            if let Some(w) = &m.where_clause {
                self.lower_where(w, &mut sub_state);
            }
            // Propagate state back up
            s.next_var = sub_state.next_var;
            // Merge any newly introduced vars (do not overwrite existing)
            for (name, id) in sub_state.vars {
                s.vars.entry(name).or_insert(id);
            }
            for (name, binding) in sub_state.path_vars {
                s.path_vars.entry(name).or_insert(binding);
            }
            for (v, label) in sub_state.node_vars {
                s.node_vars.entry(v).or_insert(label);
            }
            for (v, rel_name) in sub_state.edge_rel_names {
                s.edge_rel_names.entry(v).or_insert(rel_name);
            }
            for (v, kind) in sub_state.var_kinds {
                s.var_kinds.entry(v).or_insert(kind);
            }
            // Deliberately do NOT propagate `edge_vars` out of an OPTIONAL MATCH.
            // After an unmatched optional row `r` is null, and Cypher requires
            // `startNode(r)`/`endNode(r)` to be null — but the rewrite resolves
            // to the endpoint node var, which on such a row is the *outer*,
            // non-null var (e.g. `a` in `MATCH (a) OPTIONAL MATCH (a)-[r]->(b)`).
            // Without an edge-uuid null gate on endpoint materialization that
            // would return a wrong non-null value, so optional edges stay
            // unresolved here and `startNode`/`endNode` fall through to
            // UnknownFunction. (Edges still resolve inside the optional's own
            // WHERE, where matched rows have a non-null `r`.) Null-gated endpoint
            // values are a node-value-completeness follow-up (#889).
            s.errors.extend(sub_state.errors);
            s.warnings.extend(sub_state.warnings);
            let mut child = sub_state.builder.build();
            let referenced_vars = (0..child.exprs.len())
                .filter_map(|index| {
                    let index = u32::try_from(index).ok()?;
                    match child.exprs.get(ExprId(index)) {
                        IrExpr::VarRef(var) => Some(*var),
                        _ => None,
                    }
                })
                .collect::<HashSet<_>>();
            let bound_vars = child
                .ops
                .iter()
                .flat_map(graph_op_bound_vars)
                .collect::<HashSet<_>>();
            let mut correlated_scans = referenced_vars
                .difference(&bound_vars)
                .filter(|var| s.node_vars.contains_key(var))
                .copied()
                .collect::<Vec<_>>();
            correlated_scans.sort_by_key(|var| var.0);
            for var in correlated_scans.into_iter().rev() {
                child.ops.insert(0, GraphOp::NodeScan { var, ty: None });
            }
            let mut correlated_edges = referenced_vars
                .difference(&bound_vars)
                .filter(|var| s.edge_rel_names.contains_key(var))
                .copied()
                .collect::<Vec<_>>();
            correlated_edges.sort_by_key(|var| var.0);
            for var in correlated_edges.into_iter().rev() {
                child.ops.insert(0, GraphOp::EdgeScan { var, ty: None });
            }
            s.builder.push_op_mut(GraphOp::Optional {
                child: Box::new(child),
            });
        } else {
            for pat in &m.patterns {
                self.lower_path_pattern(pat, s);
            }
            if let Some(w) = &m.where_clause {
                self.lower_where(w, s);
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    fn lower_path_pattern(&self, pat: &PathPattern, s: &mut BinderState) {
        let mut prev_node_var: Option<VarId> = None;
        let mut iter = pat.elements.iter().peekable();
        // A node that follows a relationship IS that relationship's destination:
        // reuse the `dst` var the Rel arm already computed (carried in
        // `pending_dst`) so the trailing `NodeScan` binds to the SAME var as the
        // `Expand`'s `dst`. Otherwise an anonymous destination would mint a fresh
        // var here (`ensure_var(None)`), leaving the scan disconnected from the
        // expansion — the lowerer would then drop or cross-join it (#718/#598).
        let mut pending_dst: Option<VarId> = None;
        let mut path_nodes: Vec<VarId> = Vec::new();
        let mut path_segments: Vec<PathSegment> = Vec::new();
        let mut path_edges: Vec<VarId> = Vec::new();

        while let Some(elem) = iter.next() {
            match elem {
                PathElement::Node(node) => {
                    let var = pending_dst.take().unwrap_or_else(|| {
                        ensure_pattern_var(node.var.as_deref(), VarKind::Node, node.span, s)
                    });
                    let ty = node
                        .labels
                        .first()
                        .map(|label| self.resolve_label(label, node.span, s));
                    s.builder.push_op_mut(GraphOp::NodeScan { var, ty });
                    s.node_vars.insert(var, node.labels.first().cloned());
                    if node.labels.len() > 1 {
                        let predicate =
                            self.lower_node_label_predicate(var, &node.labels[1..], node.span, s);
                        s.builder.push_op_mut(GraphOp::Filter { predicate });
                    }
                    // An inline property map `(a:Person {name:'x'})` is an
                    // exact-equality constraint — lower it to a Filter over the
                    // just-scanned rows (#748).
                    self.lower_inline_property_filter(var, node.properties.as_ref(), node.span, s);
                    prev_node_var = Some(var);
                }
                PathElement::Rel(rel) => {
                    let edge_var =
                        ensure_pattern_var(rel.var.as_deref(), VarKind::Relationship, rel.span, s);

                    // Peek ahead for the destination node. When it exists, hand
                    // its var to the next loop iteration via `pending_dst` so the
                    // trailing NodeScan binds to this same dst (see top of fn).
                    let dst_var = if let Some(PathElement::Node(dst)) = iter.peek() {
                        let v = ensure_pattern_var(dst.var.as_deref(), VarKind::Node, dst.span, s);
                        pending_dst = Some(v);
                        v
                    } else {
                        alloc_anon_var(s)
                    };

                    let src_var = prev_node_var.unwrap_or_else(|| alloc_anon_var(s));
                    let dir = lower_direction(rel.direction);
                    let rel_name = (rel.types.len() == 1)
                        .then(|| rel.types.first().cloned())
                        .flatten();
                    let rel_ty = rel_name
                        .as_ref()
                        .map(|t| self.resolve_relation_type(t, rel.span, s));
                    let is_var_hop = rel.min_hops.is_some() || rel.max_hops.is_some();

                    // Both fixed (`-[:R]->`) and variable-length (`-[:R*1..3]->`)
                    // hops lower to a single `Expand`: it is the only op that
                    // carries the src/dst node vars, so the relational layer can
                    // connect `(a)` and `(b)` with a join. A bare `TypedEdgeScan`
                    // / `EdgeScan` would drop those vars and leave the pattern's
                    // node scans disconnected (#718). A fixed hop is encoded as
                    // `min_hops == 1 && max_hops == Some(1)`, which the lowerer
                    // routes to a relational join; any other bound goes to the
                    // BFS Extension.
                    let (min_hops, max_hops) = if is_var_hop {
                        let min = u16::try_from(rel.min_hops.unwrap_or(1)).unwrap_or(u16::MAX);
                        let max = rel.max_hops.map(|h| u16::try_from(h).unwrap_or(u16::MAX));
                        (min, max)
                    } else {
                        (1, Some(1))
                    };
                    let is_scalar_hop = min_hops == 1 && max_hops == Some(1);
                    if is_var_hop && is_scalar_hop {
                        s.scalar_list_edges.insert(edge_var);
                    }
                    let bound_rel_type_conflict =
                        bound_rel_type_conflict(edge_var, rel_name.as_deref(), is_scalar_hop, s);
                    s.builder.push_op_mut(GraphOp::Expand {
                        src: src_var,
                        edge: edge_var,
                        dst: dst_var,
                        rel_ty,
                        dir,
                        min_hops,
                        max_hops,
                    });
                    if rel.types.len() > 1 {
                        if is_scalar_hop {
                            s.edge_rel_names.insert(edge_var, None);
                        }
                        let predicate = self
                            .lower_relationship_type_predicate(edge_var, &rel.types, rel.span, s);
                        s.builder.push_op_mut(GraphOp::Filter { predicate });
                    }
                    let prior_edges = path_edges.clone();
                    if path_edges.contains(&edge_var) {
                        s.errors.push(BindError::new(
                            BindErrorKind::InvalidArgument,
                            rel.span,
                            "RelationshipUniquenessViolation: a relationship variable may not be reused within one pattern",
                        ));
                    }
                    if !prior_edges.is_empty() {
                        s.builder.push_op_mut(GraphOp::RelationshipUnique {
                            edge: edge_var,
                            prior_edges,
                        });
                    }
                    path_edges.push(edge_var);
                    if bound_rel_type_conflict {
                        push_false_filter(s);
                    }

                    // Record the relationship's endpoints so `startNode(r)` /
                    // `endNode(r)` can return the start/end node value (#753).
                    // `src_var`/`dst_var` are the pattern's traversal-left/right
                    // vars; the relationship's true start/end follow its
                    // *direction*: an outgoing `(a)-[r]->(b)` starts at `a`, an
                    // incoming `(a)<-[r]-(b)` starts at `b`. An undirected edge's
                    // orientation is per matched row, so a static left/right
                    // rewrite could pick the wrong endpoint — skip it (startNode/
                    // endNode then fall through to UnknownFunction). Only scalar
                    // single hops qualify: a true variable-length edge var binds
                    // to a *list*, not one relationship.
                    if is_scalar_hop {
                        s.edge_rel_names.insert(edge_var, rel_name.clone());
                        let endpoints = match dir {
                            Direction::Out => Some((src_var, dst_var)),
                            Direction::In => Some((dst_var, src_var)),
                            Direction::Undirected => None,
                        };
                        if let Some(endpoints) = endpoints {
                            s.edge_vars.insert(edge_var, endpoints);
                        }
                    }

                    if path_nodes.is_empty() {
                        path_nodes.push(src_var);
                    }
                    path_nodes.push(dst_var);
                    path_segments.push(PathSegment {
                        edge: edge_var,
                        // Mirror the lowerer's routing, not the syntax: an
                        // explicit `*1..1` goes to the relational join like a
                        // fixed hop, so its edge var binds to scalar edge
                        // columns — only true BFS bounds get the list column.
                        var_len: !(min_hops == 1 && max_hops == Some(1)),
                        rel_name: rel_name.clone(),
                    });

                    // An inline relationship-property map `-[r:KNOWS {since:2020}]->`
                    // is an exact-equality constraint on the edge — lower it to a
                    // Filter over the just-expanded rows, mirroring the node arm
                    // (#748/#750). The read side already materialises
                    // `var_<edge>.<prop>` for a scalar hop (`join_edge_properties`,
                    // #784). A true variable-length edge var binds to a *list*
                    // column, not scalar props, so an inline filter there has
                    // nothing to resolve against (that's #755).
                    if is_scalar_hop {
                        self.lower_inline_property_filter(
                            edge_var,
                            rel.properties.as_ref(),
                            rel.span,
                            s,
                        );
                    } else {
                        self.lower_varlen_inline_property_filter(
                            edge_var,
                            rel_name.as_deref(),
                            rel.properties.as_ref(),
                            rel.span,
                            s,
                        );
                    }

                    prev_node_var = Some(dst_var);
                }
            }
        }

        if let Some(name) = &pat.var {
            if path_nodes.is_empty()
                && let Some(node) = prev_node_var
            {
                path_nodes.push(node);
            }
            Self::bind_path_var(name, pat.span, path_nodes, path_segments, s);
        }
    }

    /// Register a named path variable (`MATCH p = (a)-[*]->(b)`, #754).
    ///
    /// The binding retains every node and segment in traversal order so path
    /// functions can compose fixed and variable-length segments later.
    fn bind_path_var(
        name: &str,
        span: Span,
        nodes: Vec<VarId>,
        segments: Vec<PathSegment>,
        s: &mut BinderState,
    ) {
        if s.vars.contains_key(name) || s.path_vars.contains_key(name) {
            s.errors.push(BindError::new(
                BindErrorKind::DuplicateVariable,
                span,
                format!("path variable `{name}` conflicts with an existing variable"),
            ));
            return;
        }
        s.path_vars
            .insert(name.to_owned(), PathBinding { nodes, segments });
    }

    /// Lower an inline node-property map (`(a {name:'x', age:30})`) into a
    /// [`GraphOp::Filter`] of AND-ed equality predicates over `var` (#748).
    ///
    /// openCypher treats inline node properties as exact-equality conjunction,
    /// so `{k1:v1, k2:v2}` becomes `var.k1 = v1 AND var.k2 = v2`. A no-property
    /// node (or an empty `{}`) adds no filter.
    fn lower_inline_property_filter(
        &self,
        var: VarId,
        properties: Option<&Expr>,
        span: Span,
        s: &mut BinderState,
    ) {
        let Some(Expr::Map(map)) = properties else {
            return;
        };
        // `HashMap` iteration order is non-deterministic; sort by key so a
        // multi-property filter produces a stable predicate order (reproducible
        // plans).
        let mut keys: Vec<&String> = map.entries.keys().collect();
        keys.sort();
        let mut combined: Option<ExprId> = None;
        for key in keys {
            let value = self.lower_expr(&map.entries[key], span, s);
            let owner = property_owner_for_var(var, s);
            let prop_span = map.key_spans.get(key).copied().unwrap_or(span);
            let prop = self.resolve_property(key, prop_span, owner, s);
            let base = s.builder.push_expr(IrExpr::VarRef(var));
            let access = s.builder.push_expr(IrExpr::PropertyAccess { base, prop });
            let eq = s.builder.push_expr(IrExpr::BinaryOp {
                op: BinaryOpKind::Eq,
                left: access,
                right: value,
            });
            combined = Some(match combined {
                None => eq,
                Some(acc) => s.builder.push_expr(IrExpr::BinaryOp {
                    op: BinaryOpKind::And,
                    left: acc,
                    right: eq,
                }),
            });
        }
        if let Some(predicate) = combined {
            s.builder.push_op_mut(GraphOp::Filter { predicate });
        }
    }

    fn lower_varlen_inline_property_filter(
        &self,
        edge_var: VarId,
        rel_name: Option<&str>,
        properties: Option<&Expr>,
        span: Span,
        s: &mut BinderState,
    ) {
        let Some(Expr::Map(map)) = properties else {
            return;
        };
        let mut keys: Vec<&String> = map.entries.keys().collect();
        keys.sort();
        let mut combined = None;
        for key in keys {
            let value = self.lower_expr(&map.entries[key], span, s);
            let owner = BoundPropertyOwner::Relationship(rel_name.map(str::to_owned));
            let prop_span = map.key_spans.get(key).copied().unwrap_or(span);
            let prop = self.resolve_property(key, prop_span, owner, s);
            let loop_var = alloc_anon_var(s);
            let element = s.builder.push_expr(IrExpr::VarRef(loop_var));
            let access = s.builder.push_expr(IrExpr::PropertyAccess {
                base: element,
                prop,
            });
            let predicate = s.builder.push_expr(IrExpr::BinaryOp {
                op: BinaryOpKind::Eq,
                left: access,
                right: value,
            });
            let list = s.builder.push_expr(IrExpr::VarRef(edge_var));
            let all = s.builder.push_expr(IrExpr::Quantifier {
                kind: gf_ast::QuantifierKind::All,
                loop_var,
                list,
                predicate,
            });
            combined = Some(match combined {
                None => all,
                Some(acc) => s.builder.push_expr(IrExpr::BinaryOp {
                    op: BinaryOpKind::And,
                    left: acc,
                    right: all,
                }),
            });
        }
        if let Some(predicate) = combined {
            s.builder.push_op_mut(GraphOp::Filter { predicate });
        }
    }

    /// Lower a `CREATE` clause into a single [`GraphOp::Create`].
    ///
    /// Walks each path pattern's node/relationship elements (mirroring
    /// [`lower_path_pattern`](Self::lower_path_pattern)) and accumulates the
    /// node and edge specs.  Variables are shared across the clause's patterns
    /// via the binder scope, so an edge in one pattern may reference a node
    /// bound in another.  Property maps are lowered into the [`ExprArena`] and
    /// referenced by [`ExprId`]; the relational lowering layer resolves them to
    /// literals at write time.
    fn lower_create(&self, c: &CreateClause, s: &mut BinderState) {
        let pattern = self.bind_create_patterns(&c.patterns, false, s);
        s.builder.push_op_mut(GraphOp::Create { pattern });
    }

    fn lower_merge(&self, m: &gf_ast::MergeClause, s: &mut BinderState) {
        let pattern = self.bind_create_patterns(std::slice::from_ref(&m.pattern), true, s);
        let on_create = self.lower_merge_actions(&m.on_create, s);
        let on_match = self.lower_merge_actions(&m.on_match, s);
        s.builder.push_op_mut(GraphOp::Merge {
            pattern,
            on_create,
            on_match,
        });
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one stateful walk keeps node, relationship, and named-path bindings aligned"
    )]
    fn bind_create_patterns(
        &self,
        patterns: &[gf_ast::PathPattern],
        allow_undirected_relationship: bool,
        s: &mut BinderState,
    ) -> CreatePattern {
        let mut pattern = CreatePattern::default();
        let mut created_property_bindings: Vec<ReturnItem> = Vec::new();

        // Variables bound BEFORE this CREATE (by a preceding MATCH/WITH/…). A
        // node spec for such a var is a *reference* (resolve the matched node per
        // row), not a mint (#703). Snapshot before `ensure_var` introduces any
        // of this clause's new vars. `ensure_var_name` reuses an existing VarId
        // for a re-named var, so membership here is exactly "came from earlier".
        let bound_before: std::collections::HashSet<VarId> = s.vars.values().copied().collect();

        for pat in patterns {
            let mut path_nodes = Vec::new();
            let mut path_segments = Vec::new();
            // A standalone single-node pattern whose variable is already bound
            // (`MATCH (a) CREATE (a)`) re-declares it — invalid even without a
            // new shape (`VariableAlreadyBound`, #956). A bound var used as an
            // edge endpoint (a multi-element pattern) is a valid reference.
            if let [PathElement::Node(node)] = pat.elements.as_slice()
                && let Some(name) = node.var.as_deref()
                && s.vars.get(name).is_some_and(|v| bound_before.contains(v))
            {
                s.errors.push(BindError::new(
                    BindErrorKind::VariableAlreadyBound,
                    node.span,
                    format!("variable `{name}` is already bound and cannot be re-created"),
                ));
            }
            let mut prev_node_var: Option<VarId> = None;
            // A node following a relationship is that relationship's destination:
            // reuse the `dst` var the Rel arm computed (mirrors lower_path_pattern,
            // #895) so an anonymous dst node's create-spec matches the edge spec's
            // `dst`. Otherwise `ensure_var(None)` mints a fresh var and the edge
            // references an unbound dst (`CREATE (:A)-[:R]->(:B)`).
            let mut pending_dst: Option<VarId> = None;
            let mut iter = pat.elements.iter().peekable();

            while let Some(elem) = iter.next() {
                match elem {
                    PathElement::Node(node) => {
                        let var = pending_dst
                            .take()
                            .unwrap_or_else(|| ensure_var(node.var.as_ref(), s));
                        if let Some(name) = node.var.as_deref() {
                            bind_var_kind(var, VarKind::Node, name, node.span, s);
                        }
                        // Re-declaring an already-bound node (from an earlier
                        // clause or earlier in this CREATE) with NEW labels or
                        // properties is invalid — `MATCH (a) CREATE (a {…})`,
                        // `CREATE (n:Foo) CREATE (n:Bar)…` (`VariableAlreadyBound`,
                        // #956). A bare reference (endpoint, no shape) is fine.
                        let already_specced = pattern.nodes.iter().any(|n| n.var == var);
                        let has_new_shape = !node.labels.is_empty() || node.properties.is_some();
                        if (bound_before.contains(&var) || already_specced) && has_new_shape {
                            s.errors.push(BindError::new(
                                BindErrorKind::VariableAlreadyBound,
                                node.span,
                                format!(
                                    "variable `{}` is already bound and cannot be re-declared \
                                     with new labels or properties",
                                    node.var.as_deref().unwrap_or("?")
                                ),
                            ));
                        }
                        // Only emit a create-spec the first time a variable
                        // appears in this CREATE; a repeated var (e.g.
                        // `CREATE (a), (a)-[:R]->(b)`) references the same node
                        // rather than creating a duplicate.
                        if !pattern.nodes.iter().any(|n| n.var == var) {
                            let labels = node
                                .labels
                                .iter()
                                .map(|label| self.resolve_label(label, node.span, s))
                                .collect();
                            let resolved_properties = node.properties.as_ref().map(|expr| {
                                rewrite_projection_alias_refs(
                                    expr.clone(),
                                    &created_property_bindings,
                                )
                            });
                            let properties = resolved_properties
                                .as_ref()
                                .map(|expr| self.lower_expr(expr, node.span, s));
                            let is_reference = bound_before.contains(&var);
                            pattern.nodes.push(CreateNodeSpec {
                                var,
                                labels,
                                properties,
                                is_reference,
                            });
                            // A freshly-created node var is node-valued, so a
                            // trailing `RETURN n` / `n.prop` treats it as a node
                            // (write-result RETURN, #814). A reference var is
                            // already registered by the preceding MATCH — don't
                            // clobber its label.
                            if !is_reference {
                                s.node_vars
                                    .entry(var)
                                    .or_insert_with(|| node.labels.first().cloned());
                                if let Some(name) = node.var.as_deref() {
                                    created_property_bindings.push(ReturnItem {
                                        expr: resolved_properties.unwrap_or_else(|| {
                                            Expr::Map(MapLiteral {
                                                entries: HashMap::new(),
                                                key_spans: HashMap::new(),
                                                span: node.span,
                                            })
                                        }),
                                        alias: Some(name.to_owned()),
                                        display: Some(name.to_owned()),
                                        span: node.span,
                                    });
                                }
                            }
                        }
                        prev_node_var = Some(var);
                        if path_nodes.is_empty() {
                            path_nodes.push(var);
                        }
                    }
                    PathElement::Rel(rel) => {
                        validate_created_rel(rel, &bound_before, allow_undirected_relationship, s);
                        let edge_var = ensure_var(rel.var.as_ref(), s);
                        if let Some(name) = rel.var.as_deref() {
                            bind_var_kind(edge_var, VarKind::Relationship, name, rel.span, s);
                        }
                        let dst_var = if let Some(PathElement::Node(dst)) = iter.peek() {
                            let v = ensure_var(dst.var.as_ref(), s);
                            pending_dst = Some(v);
                            v
                        } else {
                            alloc_anon_var(s)
                        };
                        let src_var = prev_node_var.unwrap_or_else(|| alloc_anon_var(s));
                        let rel_type = rel
                            .types
                            .first()
                            .map(|t| self.resolve_relation_type(t, rel.span, s));
                        let properties = rel
                            .properties
                            .as_ref()
                            .map(|expr| self.lower_expr(expr, rel.span, s));
                        pattern.edges.push(CreateEdgeSpec {
                            var: edge_var,
                            src: src_var,
                            dst: dst_var,
                            rel_type,
                            direction: lower_direction(rel.direction),
                            properties,
                        });
                        s.edge_rel_names
                            .insert(edge_var, rel.types.first().cloned());
                        let endpoints = match lower_direction(rel.direction) {
                            Direction::In => (dst_var, src_var),
                            Direction::Out | Direction::Undirected => (src_var, dst_var),
                        };
                        s.edge_vars.insert(edge_var, endpoints);
                        path_nodes.push(dst_var);
                        path_segments.push(PathSegment {
                            edge: edge_var,
                            var_len: false,
                            rel_name: rel.types.first().cloned(),
                        });
                        prev_node_var = Some(dst_var);
                    }
                }
            }
            if let Some(name) = &pat.var {
                Self::bind_path_var(name, pat.span, path_nodes, path_segments, s);
            }
        }

        pattern
    }

    fn lower_merge_actions(&self, actions: &[SetItem], s: &mut BinderState) -> Vec<MergeSetItem> {
        let mut lowered = Vec::with_capacity(actions.len());
        for action in actions {
            match action {
                SetItem::Property {
                    target,
                    value,
                    span,
                } => {
                    if let Some((var, prop, prop_name)) = self.resolve_write_target(target, s) {
                        lowered.push(MergeSetItem::Property(SetPropItem {
                            target: var,
                            prop,
                            prop_name,
                            value: self.lower_expr(value, *span, s),
                        }));
                    }
                }
                SetItem::PropertyMerge { var, map, span }
                | SetItem::PropertyReplace { var, map, span } => {
                    if let Some(&target) = s.vars.get(var) {
                        lowered.push(MergeSetItem::Map(SetMapItem {
                            target,
                            map: self.lower_set_map_source(map, *span, s),
                            replace: matches!(action, SetItem::PropertyReplace { .. }),
                        }));
                    } else {
                        s.errors.push(BindError::new(
                            BindErrorKind::UndeclaredVariable,
                            *span,
                            format!("undefined variable `{var}`"),
                        ));
                    }
                }
                SetItem::Label { var, labels, span } => {
                    if let Some(&target) = s.vars.get(var) {
                        lowered.push(MergeSetItem::AddLabels {
                            target,
                            labels: labels
                                .iter()
                                .map(|label| self.resolve_label(label, *span, s))
                                .collect(),
                        });
                    } else {
                        s.errors.push(BindError::new(
                            BindErrorKind::UndeclaredVariable,
                            *span,
                            format!("undefined variable `{var}`"),
                        ));
                    }
                }
                _ => unreachable!("future SET forms are rejected by the parser contract"),
            }
        }
        lowered
    }

    /// Lower a `DELETE` / `DETACH DELETE` clause into a [`GraphOp::Delete`].
    ///
    /// Direct entity variables retain their identity-column fast path. Named
    /// paths expand to their constituent variables, while list/map access and
    /// other runtime value expressions are evaluated by the statement driver.
    fn lower_delete(&self, d: &gf_ast::DeleteClause, s: &mut BinderState) {
        let mut vars: Vec<VarId> = Vec::with_capacity(d.exprs.len());
        let mut exprs = Vec::new();
        for expr in &d.exprs {
            match expr {
                Expr::Var(VarRef { name, span }) => match s.vars.get(name) {
                    Some(&var_id) => vars.push(var_id),
                    None => {
                        if let Some(path) = s.path_vars.get(name) {
                            vars.extend(path.nodes.iter().copied());
                            vars.extend(path.segments.iter().map(|segment| segment.edge));
                        } else {
                            s.errors.push(BindError::new(
                                BindErrorKind::UndeclaredVariable,
                                *span,
                                format!("DELETE target `{name}` is not a bound variable"),
                            ));
                        }
                    }
                },
                Expr::Property(_) => exprs.push(self.lower_expr(expr, expr.span(), s)),
                Expr::FunctionCall(call) if call.name.as_slice() == ["_subscript"] => {
                    exprs.push(self.lower_expr(expr, expr.span(), s));
                }
                Expr::Parenthesized { inner, .. } => {
                    if matches!(inner.as_ref(), Expr::Property(_))
                        || matches!(inner.as_ref(), Expr::FunctionCall(call) if call.name.as_slice() == ["_subscript"])
                    {
                        exprs.push(self.lower_expr(inner, inner.span(), s));
                    } else {
                        if self.typed_uuid_param_in(inner).is_some() {
                            self.lower_expr(inner, inner.span(), s);
                        }
                        s.errors.push(BindError::new(
                            BindErrorKind::InvalidDeleteTarget,
                            expr.span(),
                            "DELETE target must be a node, relationship, or path value",
                        ));
                    }
                }
                other => {
                    if self.typed_uuid_param_in(other).is_some() {
                        self.lower_expr(other, other.span(), s);
                    }
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidDeleteTarget,
                        other.span(),
                        "DELETE target must be a node, relationship, or path value",
                    ));
                }
            }
        }
        vars.sort_unstable_by_key(|var| var.0);
        vars.dedup();
        s.builder.push_op_mut(GraphOp::Delete {
            vars,
            exprs,
            detach: d.detach,
        });
    }

    /// Lower a `SET` clause into a [`GraphOp::Set`] (#791).
    ///
    /// Property assignments and bulk map assignments carry full runtime
    /// expressions through to lowering; label additions carry resolved type ids.
    fn lower_set(&self, st: &SetClause, s: &mut BinderState) {
        let mut items: Vec<SetPropItem> = Vec::with_capacity(st.items.len());
        let mut map_items = Vec::new();
        let mut label_items = Vec::new();
        for item in &st.items {
            match item {
                SetItem::Property {
                    target,
                    value,
                    span,
                } => {
                    let Some((var, prop, prop_name)) = self.resolve_write_target(target, s) else {
                        continue;
                    };
                    let value = self.lower_expr(value, *span, s);
                    items.push(SetPropItem {
                        target: var,
                        prop,
                        prop_name,
                        value,
                    });
                }
                SetItem::PropertyMerge { var, map, span }
                | SetItem::PropertyReplace { var, map, span } => {
                    let Some(&target) = s.vars.get(var) else {
                        s.errors.push(BindError::new(
                            BindErrorKind::UndeclaredVariable,
                            *span,
                            format!("undefined variable `{var}`"),
                        ));
                        continue;
                    };
                    map_items.push(SetMapItem {
                        target,
                        map: self.lower_set_map_source(map, *span, s),
                        replace: matches!(item, SetItem::PropertyReplace { .. }),
                    });
                }
                SetItem::Label { var, labels, span } => match s.vars.get(var).copied() {
                    Some(target) => label_items.push(crate::LabelItem {
                        target,
                        labels: labels
                            .iter()
                            .map(|label| self.resolve_label(label, *span, s))
                            .collect(),
                    }),
                    None => s.errors.push(BindError::new(
                        BindErrorKind::UndeclaredVariable,
                        *span,
                        format!("undefined variable `{var}`"),
                    )),
                },
                _ => unreachable!("future SET forms rejected above"),
            }
        }
        s.builder.push_op_mut(GraphOp::Set {
            items,
            map_items,
            label_items,
        });
    }

    fn lower_set_map_source(&self, map: &Expr, span: Span, s: &mut BinderState) -> ExprId {
        if let Expr::Var(VarRef { name, .. }) = map
            && let Some(&var) = s.vars.get(name)
            && (s.node_vars.contains_key(&var) || s.edge_rel_names.contains_key(&var))
        {
            let value = s.builder.push_expr(IrExpr::VarRef(var));
            return s.builder.push_expr(IrExpr::FunctionCall {
                name: "properties".into(),
                args: vec![value],
            });
        }
        self.lower_expr(map, span, s)
    }

    /// Lower a `REMOVE` clause into a [`GraphOp::Remove`] (#791).
    ///
    /// Property removals carry resolved property ids; label removals carry
    /// resolved type ids for statement-driver execution.
    fn lower_remove(&self, r: &RemoveClause, s: &mut BinderState) {
        let mut items: Vec<RemovePropItem> = Vec::with_capacity(r.items.len());
        let mut label_items = Vec::new();
        for item in &r.items {
            match item {
                RemoveItem::Property(target, _span) => {
                    let Some((var, prop, prop_name)) = self.resolve_write_target(target, s) else {
                        continue;
                    };
                    items.push(RemovePropItem {
                        target: var,
                        prop,
                        prop_name,
                    });
                }
                RemoveItem::Label { var, labels, span } => match s.vars.get(var).copied() {
                    Some(target) => label_items.push(crate::LabelItem {
                        target,
                        labels: labels
                            .iter()
                            .map(|label| self.resolve_label(label, *span, s))
                            .collect(),
                    }),
                    None => s.errors.push(BindError::new(
                        BindErrorKind::UndeclaredVariable,
                        *span,
                        format!("undefined variable `{var}`"),
                    )),
                },
                _ => s.errors.push(BindError::new(
                    BindErrorKind::UnsupportedClause,
                    r.span,
                    "this form of REMOVE is not yet supported",
                )),
            }
        }
        s.builder
            .push_op_mut(GraphOp::Remove { items, label_items });
    }

    /// Resolve a `SET`/`REMOVE` property target (`n.prop`) into its bound
    /// variable, [`PropId`], and property name.
    ///
    /// The target's object must be a bound variable (`Expr::Var`); a property
    /// access on anything else — a literal, an expression, a nested property —
    /// is an [`InvalidDeleteTarget`](BindErrorKind::InvalidDeleteTarget) (the
    /// kind means "write target must be a bound variable"). Returns `None` (and
    /// records an error) when the target is malformed or the variable unbound,
    /// so the caller skips that item.
    fn resolve_write_target(
        &self,
        target: &PropertyAccess,
        s: &mut BinderState,
    ) -> Option<(VarId, PropId, String)> {
        if matches!(target.key.as_str(), "node_uuid" | "edge_uuid") {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                target.span,
                format!("structural identity field `{}` is read-only", target.key),
            ));
            return None;
        }
        let Expr::Var(VarRef { name, span }) = target.object.as_ref() else {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidDeleteTarget,
                target.span,
                "write target must be a bound variable's property \
                 (e.g. `SET n.prop = …`)",
            ));
            return None;
        };
        let Some(&var) = s.vars.get(name) else {
            s.errors.push(BindError::new(
                BindErrorKind::UndeclaredVariable,
                *span,
                format!("write target `{name}` is not a bound variable"),
            ));
            return None;
        };
        let owner = property_owner_for_var(var, s);
        let prop = self.resolve_property(&target.key, target.span, owner, s);
        Some((var, prop, target.key.clone()))
    }

    fn lower_where(&self, w: &WhereClause, s: &mut BinderState) {
        // An aggregate in WHERE is invalid — aggregation happens in WITH/RETURN,
        // and a predicate filtering on `count(...)` must use a WITH first
        // (openCypher `InvalidAggregation`, #956).
        if expr_contains_aggregate(&w.predicate) {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                w.span,
                "an aggregate function may not be used in WHERE".to_string(),
            ));
        }
        self.lower_where_predicate(&w.predicate, w.span, s);
    }

    fn lower_where_predicate(&self, expr: &Expr, parent_span: Span, s: &mut BinderState) {
        match expr {
            Expr::Parenthesized { inner, .. } => self.lower_where_predicate(inner, parent_span, s),
            Expr::PatternPredicate(pp) => self.lower_pattern_predicate(pp, false, s),
            Expr::ExistentialSubquery(es) => self.lower_existential_subquery(es, s),
            Expr::UnaryOp(gf_ast::UnaryOp {
                op: AstUnOp::Not,
                expr: inner,
                ..
            }) if matches_pattern_predicate(inner) => {
                let Expr::PatternPredicate(pp) = strip_parens(inner) else {
                    unreachable!("matches_pattern_predicate ensured the inner shape");
                };
                self.lower_pattern_predicate(pp, true, s);
            }
            Expr::BinaryOp(gf_ast::BinaryOp {
                op: AstBinOp::And,
                left,
                right,
                ..
            }) => {
                self.lower_where_predicate(left, parent_span, s);
                self.lower_where_predicate(right, parent_span, s);
            }
            Expr::BinaryOp(gf_ast::BinaryOp {
                op: AstBinOp::Or, ..
            }) => {
                let mut alternatives = Vec::new();
                if collect_pattern_disjunction(expr, &mut alternatives) {
                    self.lower_pattern_predicate_alternatives(&alternatives, false, s);
                } else if expr_contains_pattern_predicate(expr) {
                    let mut branches = Vec::new();
                    if collect_mixed_pattern_disjunction(expr, &mut branches) {
                        self.lower_mixed_pattern_predicate_alternatives(&branches, s);
                    } else {
                        s.errors.push(BindError::new(
                            BindErrorKind::InvalidArgument,
                            expr.span(),
                            "each OR branch containing a pattern predicate must contain exactly one pattern alternative",
                        ));
                    }
                } else {
                    Self::reject_bare_graph_value_predicate(expr, parent_span, s);
                    let pred = self.lower_expr(expr, parent_span, s);
                    s.builder.push_op_mut(GraphOp::Filter { predicate: pred });
                }
            }
            other if expr_contains_pattern_predicate(other) => {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    other.span(),
                    "pattern predicates are currently supported only as single-relationship \
                     WHERE predicates, with optional NOT and AND",
                ));
            }
            other => {
                Self::reject_bare_graph_value_predicate(other, parent_span, s);
                let pred = self.lower_expr(other, parent_span, s);
                s.builder.push_op_mut(GraphOp::Filter { predicate: pred });
            }
        }
    }

    fn lower_pattern_predicate(&self, pp: &PatternPredicate, negated: bool, s: &mut BinderState) {
        self.lower_pattern_predicate_alternatives(&[pp], negated, s);
    }

    fn lower_existential_subquery(&self, es: &gf_ast::ExistentialSubquery, s: &mut BinderState) {
        let prior_error_count = s.errors.len();
        let outer_vars = s.vars.values().copied().collect::<HashSet<_>>();

        let mut sub_state = BinderState {
            vars: s.vars.clone(),
            path_vars: s.path_vars.clone(),
            node_vars: s.node_vars.clone(),
            edge_vars: s.edge_vars.clone(),
            edge_rel_names: s.edge_rel_names.clone(),
            scalar_list_edges: s.scalar_list_edges.clone(),
            var_kinds: s.var_kinds.clone(),
            next_var: s.next_var,
            builder: GraphPlan::builder("openCypher").ontology_mode(self.mode),
            errors: Vec::new(),
            warnings: Vec::new(),
            captured_pattern_comprehensions: None,
            existential_depth: s.existential_depth + 1,
            standalone_call: false,
        };

        match &es.body {
            ExistentialSubqueryBody::Simple { pattern, filter } => {
                self.lower_path_pattern(pattern, &mut sub_state);
                if let Some(filter) = filter {
                    if expr_contains_aggregate(filter) {
                        sub_state.errors.push(BindError::new(
                            BindErrorKind::InvalidArgument,
                            es.span,
                            "an aggregate function may not be used in a simple existential subquery",
                        ));
                    }
                    self.lower_where_predicate(filter, es.span, &mut sub_state);
                }
            }
            ExistentialSubqueryBody::Full(query) => {
                let last = query.clauses.len().saturating_sub(1);
                for (index, clause) in query.clauses.iter().enumerate() {
                    let allowed = matches!(
                        clause,
                        AstClause::Match(_)
                            | AstClause::OptionalMatch(_)
                            | AstClause::With(_)
                            | AstClause::Unwind(_)
                    ) || matches!(clause, AstClause::Return(_)) && index == last;
                    if !allowed {
                        sub_state.errors.push(BindError::new(
                            BindErrorKind::InvalidArgument,
                            clause.span(),
                            "a full existential subquery must contain only read clauses and end in RETURN",
                        ));
                        continue;
                    }
                    self.lower_clause(clause, &mut sub_state);
                }
                if !matches!(query.clauses.last(), Some(AstClause::Return(_))) {
                    sub_state.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        es.span,
                        "a full existential subquery must end in RETURN",
                    ));
                }
            }
        }

        let child = sub_state.builder.build();
        let references_outer = plan_references_any_var(&child, &outer_vars);
        s.next_var = s.next_var.max(sub_state.next_var);
        s.errors.extend(sub_state.errors);
        s.warnings.extend(sub_state.warnings);
        if !references_outer {
            s.errors.push(BindError::new(
                BindErrorKind::UndeclaredVariable,
                es.span,
                "existential subquery must reference at least one outer variable",
            ));
        }
        if s.errors.len() == prior_error_count {
            s.builder.push_op_mut(GraphOp::Exists {
                child: Box::new(child),
                negated: false,
            });
        }
    }

    fn lower_pattern_predicate_alternatives(
        &self,
        alternatives: &[&PatternPredicate],
        negated: bool,
        s: &mut BinderState,
    ) {
        let prior_error_count = s.errors.len();
        let mut children = Vec::new();
        for pp in alternatives {
            if let Some(name) = pp.pattern.var.as_deref() {
                s.errors.push(BindError::new(
                    BindErrorKind::UndeclaredVariable,
                    pp.span,
                    format!("path variable `{name}` is not bound in this pattern predicate scope"),
                ));
                continue;
            }
            if !is_single_relationship_pattern(&pp.pattern) && s.existential_depth < 2 {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    pp.span,
                    "multi-relationship pattern predicates are supported only in nested existential subqueries",
                ));
                continue;
            }
            if pattern_has_var_length_relationship_properties(&pp.pattern) {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    pp.span,
                    "variable-length relationships in pattern predicates cannot have property maps",
                ));
                continue;
            }
            for pattern in relationship_type_alternatives(&pp.pattern) {
                if let Some(child) = self.bind_pattern_predicate_child(&pattern, pp.span, s) {
                    children.push(child);
                }
            }
        }

        if children.is_empty() || s.errors.len() > prior_error_count {
            return;
        }
        let child = if children.len() == 1 {
            children.pop().expect("one child remains")
        } else {
            GraphPlan::builder("openCypher")
                .push_op(GraphOp::Union {
                    all: true,
                    inputs: children,
                })
                .build()
        };
        s.builder.push_op_mut(GraphOp::Exists {
            child: Box::new(child),
            negated,
        });
    }

    fn lower_mixed_pattern_predicate_alternatives(
        &self,
        branches: &[MixedPatternBranch<'_>],
        s: &mut BinderState,
    ) {
        let prior_error_count = s.errors.len();
        let mut children = Vec::new();
        for branch in branches {
            for pattern in relationship_type_alternatives(&branch.pattern.pattern) {
                if let Some(child) = self.bind_pattern_predicate_child_with_filters(
                    &pattern,
                    branch.pattern.span,
                    &branch.scalar_filters,
                    s,
                ) {
                    children.push(child);
                }
            }
        }
        if children.is_empty() || s.errors.len() > prior_error_count {
            return;
        }
        let child = if children.len() == 1 {
            children.pop().expect("one child remains")
        } else {
            GraphPlan::builder("openCypher")
                .push_op(GraphOp::Union {
                    all: true,
                    inputs: children,
                })
                .build()
        };
        s.builder.push_op_mut(GraphOp::Exists {
            child: Box::new(child),
            negated: false,
        });
    }

    fn bind_pattern_predicate_child(
        &self,
        pattern: &PathPattern,
        span: Span,
        s: &mut BinderState,
    ) -> Option<GraphPlan> {
        self.bind_pattern_predicate_child_with_filters(pattern, span, &[], s)
    }

    fn bind_pattern_predicate_child_with_filters(
        &self,
        pattern: &PathPattern,
        span: Span,
        scalar_filters: &[&Expr],
        s: &mut BinderState,
    ) -> Option<GraphPlan> {
        let prior_error_count = s.errors.len();
        let mut sub_state = BinderState {
            vars: s.vars.clone(),
            path_vars: s.path_vars.clone(),
            node_vars: s.node_vars.clone(),
            edge_vars: s.edge_vars.clone(),
            edge_rel_names: s.edge_rel_names.clone(),
            scalar_list_edges: s.scalar_list_edges.clone(),
            var_kinds: s.var_kinds.clone(),
            next_var: s.next_var,
            builder: GraphPlan::builder("openCypher").ontology_mode(self.mode),
            errors: Vec::new(),
            warnings: Vec::new(),
            captured_pattern_comprehensions: None,
            existential_depth: s.existential_depth,
            standalone_call: false,
        };
        self.lower_path_pattern(pattern, &mut sub_state);
        for filter in scalar_filters {
            self.lower_where_predicate(filter, filter.span(), &mut sub_state);
        }

        for name in sub_state.vars.keys() {
            if !s.vars.contains_key(name) {
                sub_state.errors.push(BindError::new(
                    BindErrorKind::UndeclaredVariable,
                    span,
                    format!("variable `{name}` is not bound in this pattern predicate scope"),
                ));
            }
        }

        if !pattern_references_bound_var(pattern, s) {
            sub_state.errors.push(BindError::new(
                BindErrorKind::UndeclaredVariable,
                span,
                "pattern predicate must reference at least one bound variable",
            ));
        }

        s.errors.extend(sub_state.errors);
        s.warnings.extend(sub_state.warnings);
        if s.errors.len() > prior_error_count {
            return None;
        }

        Some(sub_state.builder.build())
    }

    fn lower_pattern_comprehension(
        &self,
        pc: &gf_ast::PatternComprehension,
        s: &mut BinderState,
    ) -> ExprId {
        let prior_error_count = s.errors.len();
        let output = VarId(s.next_var);
        s.next_var += 1;

        let mut pattern = pc.pattern.clone();
        pattern.var.clone_from(&pc.var);
        let alternatives = relationship_type_alternatives(&pattern);
        let mut children = Vec::with_capacity(alternatives.len());
        for pattern in alternatives {
            let mut sub_state = BinderState {
                vars: s.vars.clone(),
                path_vars: s.path_vars.clone(),
                node_vars: s.node_vars.clone(),
                edge_vars: s.edge_vars.clone(),
                edge_rel_names: s.edge_rel_names.clone(),
                scalar_list_edges: s.scalar_list_edges.clone(),
                var_kinds: s.var_kinds.clone(),
                next_var: s.next_var,
                builder: GraphPlan::builder("openCypher").ontology_mode(self.mode),
                errors: Vec::new(),
                warnings: Vec::new(),
                captured_pattern_comprehensions: None,
                existential_depth: s.existential_depth,
                standalone_call: false,
            };
            self.lower_path_pattern(&pattern, &mut sub_state);

            if let Some(filter) = &pc.filter {
                if expr_contains_aggregate(filter) {
                    sub_state.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        pc.span,
                        "an aggregate function may not be used in a pattern comprehension filter",
                    ));
                }
                self.lower_where_predicate(filter, pc.span, &mut sub_state);
            }
            if expr_contains_aggregate(&pc.projection) {
                sub_state.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    pc.span,
                    "an aggregate function may not be used in a pattern comprehension projection",
                ));
            }

            let projection = self
                .lower_projection_value_expr(&pc.projection, pc.span, &mut sub_state, true)
                .unwrap_or_else(|| self.lower_expr(&pc.projection, pc.span, &mut sub_state));
            sub_state.builder.push_op_mut(GraphOp::Project {
                items: vec![ProjectItem {
                    expr: projection,
                    alias: Some(PATTERN_COMPREHENSION_VALUE_ALIAS.into()),
                    out_var: None,
                }],
                distinct: false,
            });

            s.next_var = s.next_var.max(sub_state.next_var);
            s.errors.extend(sub_state.errors);
            s.warnings.extend(sub_state.warnings);
            children.push(sub_state.builder.build());
        }
        if s.errors.len() > prior_error_count {
            return s.builder.push_expr(IrExpr::Literal(IrLiteral::Null));
        }

        let mut outputs = Vec::with_capacity(children.len());
        for (index, child) in children.into_iter().enumerate() {
            let child_output = if index == 0 {
                output
            } else {
                let next = VarId(s.next_var);
                s.next_var += 1;
                next
            };
            let child = Box::new(child);
            if let Some(captured) = s.captured_pattern_comprehensions.as_mut() {
                captured.push((child, child_output));
            } else {
                s.builder.push_op_mut(GraphOp::PatternComprehension {
                    child,
                    output: child_output,
                });
            }
            outputs.push(s.builder.push_expr(IrExpr::VarRef(child_output)));
        }
        let mut outputs = outputs.into_iter();
        let Some(mut combined) = outputs.next() else {
            return s.builder.push_expr(IrExpr::ListLiteral(Vec::new()));
        };
        for next in outputs {
            combined = s.builder.push_expr(IrExpr::BinaryOp {
                op: BinaryOpKind::Add,
                left: combined,
                right: next,
            });
        }
        combined
    }

    fn reject_bare_graph_value_predicate(expr: &Expr, span: Span, s: &mut BinderState) {
        let Expr::Var(VarRef { name, .. }) = strip_parens(expr) else {
            return;
        };
        if s.path_vars.contains_key(name) {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                span,
                format!("WHERE predicate `{name}` must be a boolean expression"),
            ));
            return;
        }
        let Some(var_id) = s.vars.get(name).copied() else {
            return;
        };
        if s.node_vars.contains_key(&var_id) || s.edge_rel_names.contains_key(&var_id) {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                span,
                format!("WHERE predicate `{name}` must be a boolean expression"),
            ));
        }
    }

    // Item classification (aggregate/node-forward/path/scalar) + scope reset +
    // ORDER BY/SKIP/LIMIT make this long but linear, like the RETURN lowering.
    #[allow(clippy::too_many_lines)]
    fn lower_with(&self, w: &WithClause, s: &mut BinderState) {
        type ForwardedEdge = (VarId, Option<String>, Option<(VarId, VarId)>);

        let items_ast = expand_projection_wildcard(&w.items, s);
        check_duplicate_aliases(&items_ast, s);
        if w.order_by
            .as_ref()
            .is_some_and(|order_by| has_unprojected_order_aggregate(&items_ast, &order_by.items))
        {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                w.order_by.as_ref().expect("checked above").span,
                "an aggregate function in ORDER BY must also appear in the WITH projection",
            ));
            return;
        }
        // WITH projects/renames the pipeline and then *resets the scope*: only
        // the projected aliases are visible to subsequent clauses (#814).
        //
        // Each item's expression is lowered in the CURRENT scope (it references
        // upstream vars), then a fresh `out_var` is minted for its alias and
        // becomes the downstream scope. The lowerer maps `out_var` to the
        // projected column.
        //
        // Aggregation in WITH (#958): a WITH with a top-level aggregate
        // (`WITH a.name AS name, count(*) AS c`) lowers to a `GraphOp::Aggregate`
        // (mirroring RETURN's implicit grouping) instead of a `Project`. A NESTED
        // aggregate inside a larger expression (`count(n) + 1 AS c`) uses an
        // aggregate→project decomposition with a re-bound scope. An aggregate
        // inside another aggregate's argument (`sum(count(*))`) is invalid.
        let has_nested_agg = items_ast.iter().any(|i| match agg_func_of(&i.expr) {
            // A top-level aggregate is fine only if its arguments hold no further
            // aggregate (Cypher forbids nesting, so this is a malformed shape).
            Some(_) => match &i.expr {
                Expr::FunctionCall(call) => call.args.iter().any(expr_contains_aggregate),
                _ => false,
            },
            // A non-aggregate item must contain no aggregate at all.
            None => expr_contains_aggregate(&i.expr),
        });
        let has_aggregate_inside_aggregate = items_ast
            .iter()
            .any(|item| expr_contains_aggregate_inside_aggregate(&item.expr, false));
        if has_aggregate_inside_aggregate {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                w.span,
                "an aggregate function may not contain another aggregate function",
            ));
            return;
        }
        if has_nested_agg && items_ast.iter().any(|i| expr_contains_aggregate(&i.expr)) {
            self.lower_with_aggregate_arith(w, &items_ast, s);
            return;
        }
        if !has_nested_agg && items_ast.iter().any(|i| agg_func_of(&i.expr).is_some()) {
            self.lower_with_aggregate(w, &items_ast, s);
            return;
        }
        let mut items: Vec<ProjectItem> = Vec::with_capacity(items_ast.len());
        let mut new_scope: Vec<(String, VarId)> = Vec::new();
        // Entity variables forwarded WHOLE (`WITH n`, `WITH r`): restore the
        // binder-side metadata after the scope reset, so a later `RETURN n` /
        // `n.x` or `RETURN r` / `r.x` still treats them as entities.
        let mut forwarded_nodes: Vec<(VarId, Option<String>)> = Vec::new();
        let mut forwarded_edges: Vec<ForwardedEdge> = Vec::new();
        let mut forwarded_paths: Vec<(String, PathBinding)> = Vec::new();
        let mut forwarded_path_vars = HashSet::new();
        for item in &items_ast {
            // Reject (rather than silently mishandle) the parts not yet
            // supported in WITH — these would otherwise produce wrong results.
            // The aggregate check is recursive: `WITH count(n) + 1 AS c` must be
            // caught too, not only a top-level `WITH count(n) AS c`.
            if expr_contains_aggregate(&item.expr) {
                s.errors.push(BindError::new(
                    BindErrorKind::UnsupportedClause,
                    item.span,
                    "aggregation in WITH is not yet supported (#814 follow-up)".to_string(),
                ));
            }

            // A bare entity variable (`WITH n` or `WITH n AS m`) is carried
            // through whole. Each output alias gets a fresh var while the
            // source expression retains the incoming var, allowing the lowerer
            // to re-qualify every entity column independently. Forwarding a
            // path is still deferred.
            let node_forward = match &item.expr {
                Expr::Var(VarRef { name, .. }) => s
                    .vars
                    .get(name)
                    .copied()
                    .filter(|v| s.node_vars.contains_key(v)),
                _ => None,
            };
            let edge_forward = match &item.expr {
                Expr::Var(VarRef { name, .. }) => s
                    .vars
                    .get(name)
                    .copied()
                    .filter(|v| s.edge_rel_names.contains_key(v)),
                _ => None,
            };
            let computed_node_label = match &item.expr {
                Expr::FunctionCall(call)
                    if is_function_named(call, "coalesce") && !call.args.is_empty() =>
                {
                    call.args
                        .iter()
                        .map(|arg| match arg {
                            Expr::Var(VarRef { name, .. }) => s
                                .vars
                                .get(name)
                                .and_then(|var| s.node_vars.get(var))
                                .cloned(),
                            _ => None,
                        })
                        .collect::<Option<Vec<_>>>()
                        .map(|labels| labels.into_iter().flatten().next())
                }
                _ => None,
            };
            let path_forward = match &item.expr {
                Expr::Var(VarRef { name, .. }) => s.path_vars.get(name).cloned(),
                _ => None,
            };
            let expr_id = if computed_node_label.is_some() || path_forward.is_some() {
                self.lower_return_item_expr(&item.expr, item.span, s, true)
            } else {
                self.lower_expr(&item.expr, item.span, s)
            };

            if let Some(v) = node_forward {
                // Each alias gets its own output var. Reusing `v` for both
                // `WITH n AS a, n AS b` would project the same qualified entity
                // columns twice and make the downstream schema ambiguous.
                let label = s.node_vars.get(&v).cloned().flatten();
                let expr_name = match &item.expr {
                    Expr::Var(VarRef { name, .. }) => name.clone(),
                    _ => unreachable!("node_forward is Some only for a Var item"),
                };
                let name = item.alias.clone().unwrap_or(expr_name);
                let out_var = alloc_anon_var(s);
                forwarded_nodes.push((out_var, label));
                new_scope.push((name.clone(), out_var));
                items.push(ProjectItem {
                    expr: expr_id,
                    alias: Some(name),
                    out_var: Some(out_var),
                });
                continue;
            }
            if let Some(v) = edge_forward {
                let rel_name = s.edge_rel_names.get(&v).cloned().flatten();
                let endpoints = s.edge_vars.get(&v).copied();
                let expr_name = match &item.expr {
                    Expr::Var(VarRef { name, .. }) => name.clone(),
                    _ => unreachable!("edge_forward is Some only for a Var item"),
                };
                let name = item.alias.clone().unwrap_or(expr_name);
                let out_var = alloc_anon_var(s);
                forwarded_edges.push((out_var, rel_name, endpoints));
                new_scope.push((name.clone(), out_var));
                items.push(ProjectItem {
                    expr: expr_id,
                    alias: Some(name),
                    out_var: Some(out_var),
                });
                continue;
            }

            // Alias: explicit (`expr AS name`), or the variable's own name for a
            // bare scalar-variable pass-through (`WITH x`).
            let alias = item.alias.clone().or_else(|| match &item.expr {
                Expr::Var(VarRef { name, .. }) => Some(name.clone()),
                _ => None,
            });
            let Some(alias) = alias else {
                s.errors.push(BindError::new(
                    BindErrorKind::UnsupportedClause,
                    item.span,
                    "a non-variable WITH item must be aliased (`expr AS name`)".to_string(),
                ));
                continue;
            };
            let out_var = alloc_anon_var(s);
            if matches!(item.expr, Expr::Literal(Literal::Null(_))) {
                s.var_kinds.insert(out_var, VarKind::Unknown);
            }
            if let Some(label) = computed_node_label {
                forwarded_nodes.push((out_var, label));
            }
            if let Some(binding) = path_forward {
                s.var_kinds.insert(out_var, VarKind::Unknown);
                forwarded_path_vars.extend(binding.nodes.iter().copied());
                forwarded_path_vars.extend(binding.segments.iter().map(|segment| segment.edge));
                forwarded_paths.push((alias.clone(), binding));
            }
            if let Expr::List(gf_ast::ListLiteral { elements, .. }) = &item.expr
                && !elements.is_empty()
                && elements.iter().all(|element| {
                    matches!(element, Expr::Var(VarRef { name, .. })
                        if s.vars.get(name).is_some_and(|var| s.edge_rel_names.contains_key(var)))
                })
            {
                forwarded_edges.push((out_var, None, None));
            }
            new_scope.push((alias.clone(), out_var));
            items.push(ProjectItem {
                expr: expr_id,
                alias: Some(alias),
                out_var: Some(out_var),
            });
        }

        let mut forwarded_path_vars = forwarded_path_vars.into_iter().collect::<Vec<_>>();
        forwarded_path_vars.sort_by_key(|var| var.0);
        for var in forwarded_path_vars {
            let expr = s.builder.push_expr(IrExpr::VarRef(var));
            items.push(ProjectItem {
                expr,
                alias: Some(format!("__gf_path_component_{}", var.0)),
                out_var: Some(var),
            });
            if let Some(label) = s.node_vars.get(&var).cloned() {
                forwarded_nodes.push((var, label));
            }
            if let Some(rel_name) = s.edge_rel_names.get(&var).cloned() {
                forwarded_edges.push((var, rel_name, s.edge_vars.get(&var).copied()));
            }
        }

        // A scalar WITH WHERE can be evaluated against the incoming row after
        // substituting projected aliases back to their source expressions. This
        // keeps hidden incoming columns available for both WHERE and ORDER BY,
        // then lets the WITH operator perform only the final scope reset.
        if let Some(wc) = w
            .where_clause
            .as_ref()
            .filter(|wc| !expr_contains_pattern_predicate(&wc.predicate))
        {
            let predicate = rewrite_projection_alias_refs(wc.predicate.clone(), &items_ast);
            let predicate = self.lower_expr(&predicate, wc.span, s);
            s.builder.push_op_mut(GraphOp::Filter { predicate });
        }
        let where_pattern_predicate = w
            .where_clause
            .as_ref()
            .filter(|wc| expr_contains_pattern_predicate(&wc.predicate))
            .cloned();

        let projection_bindings: Vec<(Expr, String, VarId)> = items_ast
            .iter()
            .filter_map(|item| {
                let alias = item.alias.clone().or_else(|| match &item.expr {
                    Expr::Var(VarRef { name, .. }) => Some(name.clone()),
                    _ => None,
                })?;
                let var = new_scope
                    .iter()
                    .find_map(|(name, var)| (name == &alias).then_some(*var))?;
                Some((item.expr.clone(), alias, var))
            })
            .collect();

        // A non-DISTINCT ORDER BY may use incoming expressions that are not
        // projected. Bind it while that scope still exists and execute it before
        // WITH drops the hidden columns. Projected aliases are substituted back
        // to their source expressions for this pre-projection sort.
        let projected_names = projection_bindings
            .iter()
            .map(|(_, alias, _)| alias.as_str())
            .collect::<std::collections::HashSet<_>>();
        let sort_fits_projection = w.order_by.as_ref().is_none_or(|order_by| {
            order_by.items.iter().all(|item| {
                let rewritten = rewrite_grouping_refs(item.expr.clone(), &projection_bindings);
                let mut refs = Vec::new();
                collect_grouping_refs(&rewritten, &mut refs);
                refs.iter().all(|reference| {
                    grouping_ref_root_name(reference)
                        .is_some_and(|name| projected_names.contains(name))
                })
            })
        });
        let sort_before_projection = w.order_by.is_some() && !w.distinct && !sort_fits_projection;
        if sort_before_projection {
            self.push_sort_before_projection(
                &w.order_by.as_ref().expect("checked above").items,
                &items_ast,
                s,
            );
        }

        // Scope reset: drop everything, then introduce only the WITH aliases
        // (and re-register forwarded whole-node vars as nodes).
        s.vars.clear();
        s.node_vars.clear();
        s.edge_vars.clear();
        s.edge_rel_names.clear();
        s.path_vars.clear();
        for (name, v) in new_scope {
            s.vars.insert(name, v);
        }
        for (v, label) in forwarded_nodes {
            s.node_vars.insert(v, label);
        }
        for (v, rel_name, endpoints) in forwarded_edges {
            s.edge_rel_names.insert(v, rel_name);
            if let Some(endpoints) = endpoints {
                s.edge_vars.insert(v, endpoints);
            }
        }
        for (name, binding) in forwarded_paths {
            s.path_vars.insert(name, binding);
        }

        s.builder.push_op_mut(GraphOp::With {
            items,
            distinct: w.distinct,
            where_predicate: None,
        });
        if let Some(wc) = where_pattern_predicate {
            self.lower_where_predicate(&wc.predicate, wc.span, s);
        }
        if let Some(ob) = &w.order_by
            && !sort_before_projection
        {
            self.push_sort_rewritten(&ob.items, &projection_bindings, s);
        }
        push_skip_limit(self, w.skip.as_ref(), w.limit.as_ref(), s);
    }

    /// Lower a WITH containing a top-level aggregate (#958) into a
    /// [`GraphOp::Aggregate`] that also introduces the post-aggregate scope:
    /// non-aggregate items become group-by keys, aggregates become
    /// [`AggExpr`]s, and every output column is bound to a fresh `out_var` so
    /// the lowerer's decomposed-aggregate path resets the scope to exactly the
    /// projected aliases (the same scope-reset contract a plain WITH enforces).
    /// A following `WHERE` becomes a `Filter` over those aliases; `ORDER BY` /
    /// `SKIP` / `LIMIT` follow as for a non-aggregate WITH.
    ///
    /// Whole node, relationship, and path keys retain their original variables;
    /// the relational lowerer groups their qualified physical columns and keeps
    /// those variables available to downstream graph operators.
    #[allow(clippy::too_many_lines)]
    fn lower_with_aggregate(&self, w: &WithClause, items: &[ReturnItem], s: &mut BinderState) {
        let mut group_by: Vec<ExprId> = Vec::new();
        let mut group_aliases: Vec<Option<String>> = Vec::new();
        let mut group_vars: Vec<Option<VarId>> = Vec::new();
        let mut aggs: Vec<AggExpr> = Vec::new();
        let mut new_scope: Vec<(String, VarId)> = Vec::new();
        let mut forwarded_nodes: Vec<(VarId, Option<String>)> = Vec::new();
        let mut forwarded_edges: Vec<ForwardedEdgeBinding> = Vec::new();
        let mut forwarded_paths: Vec<(String, PathBinding)> = Vec::new();
        let mut projection_bindings: Vec<(Expr, String, VarId)> = Vec::new();

        for item in items {
            // Output column name: explicit `AS alias`, or a bare variable's name.
            let alias = item.alias.clone().or_else(|| match &item.expr {
                Expr::Var(VarRef { name, .. }) => Some(name.clone()),
                _ => None,
            });

            if let Some(func) = agg_func_of(&item.expr) {
                let Expr::FunctionCall(call) = &item.expr else {
                    unreachable!("agg_func_of only matches FunctionCall");
                };
                let Some(alias) = alias else {
                    s.errors.push(BindError::new(
                        BindErrorKind::UnsupportedClause,
                        item.span,
                        "an aggregate in WITH must be aliased (`count(*) AS name`)".to_string(),
                    ));
                    continue;
                };
                let out_var = alloc_anon_var(s);
                aggs.push(self.build_agg(call, func, alias.clone(), Some(out_var), s));
                projection_bindings.push((item.expr.clone(), alias.clone(), out_var));
                new_scope.push((alias, out_var));
            } else {
                if let Expr::Var(VarRef { name, .. }) = &item.expr
                    && let Some(binding) = s.path_vars.get(name).cloned()
                {
                    let Some(alias) = alias else {
                        unreachable!("a bare path always has its variable name as alias")
                    };
                    let mut vars = binding.nodes.clone();
                    vars.extend(binding.segments.iter().map(|segment| segment.edge));
                    vars.sort_by_key(|var| var.0);
                    vars.dedup();
                    for var in vars {
                        let expr = s.builder.push_expr(IrExpr::VarRef(var));
                        group_by.push(expr);
                        group_aliases.push(None);
                        group_vars.push(Some(var));
                        if let Some(label) = s.node_vars.get(&var).cloned() {
                            forwarded_nodes.push((var, label));
                        }
                        if let Some(rel_name) = s.edge_rel_names.get(&var).cloned() {
                            forwarded_edges.push(ForwardedEdgeBinding {
                                var,
                                rel_name,
                                endpoints: s.edge_vars.get(&var).copied(),
                            });
                        }
                    }
                    forwarded_paths.push((alias, binding));
                    continue;
                }

                let entity_var = match &item.expr {
                    Expr::Var(VarRef { name, .. }) => s.vars.get(name).copied().filter(|var| {
                        s.node_vars.contains_key(var) || s.edge_rel_names.contains_key(var)
                    }),
                    Expr::FunctionCall(call) => Self::resolve_endpoint_node(call, s),
                    _ => None,
                };
                if let Some(var) = entity_var {
                    let Some(alias) = alias else {
                        unreachable!("a graph variable always has an output alias")
                    };
                    let expr = s.builder.push_expr(IrExpr::VarRef(var));
                    group_by.push(expr);
                    group_aliases.push(None);
                    group_vars.push(Some(var));
                    projection_bindings.push((item.expr.clone(), alias.clone(), var));
                    new_scope.push((alias, var));
                    if let Some(label) = s.node_vars.get(&var).cloned() {
                        forwarded_nodes.push((var, label));
                    }
                    if let Some(rel_name) = s.edge_rel_names.get(&var).cloned() {
                        forwarded_edges.push(ForwardedEdgeBinding {
                            var,
                            rel_name,
                            endpoints: s.edge_vars.get(&var).copied(),
                        });
                    }
                    continue;
                }
                let Some(alias) = alias else {
                    s.errors.push(BindError::new(
                        BindErrorKind::UnsupportedClause,
                        item.span,
                        "a non-variable WITH item must be aliased (`expr AS name`)".to_string(),
                    ));
                    continue;
                };
                let expr_id = self.lower_expr(&item.expr, item.span, s);
                let out_var = alloc_anon_var(s);
                group_by.push(expr_id);
                group_aliases.push(Some(alias.clone()));
                group_vars.push(Some(out_var));
                projection_bindings.push((item.expr.clone(), alias.clone(), out_var));
                new_scope.push((alias, out_var));
            }
        }
        // Scope reset: WITH exposes only its projected aliases downstream.
        s.vars.clear();
        s.node_vars.clear();
        s.edge_vars.clear();
        s.edge_rel_names.clear();
        s.path_vars.clear();
        for (name, v) in new_scope {
            s.vars.insert(name, v);
        }
        for (var, label) in forwarded_nodes {
            s.node_vars.insert(var, label);
        }
        for edge in forwarded_edges {
            s.edge_rel_names.insert(edge.var, edge.rel_name);
            if let Some(endpoints) = edge.endpoints {
                s.edge_vars.insert(edge.var, endpoints);
            }
        }
        for (name, binding) in forwarded_paths {
            s.path_vars.insert(name, binding);
        }

        s.builder.push_op_mut(GraphOp::Aggregate {
            group_by,
            group_aliases,
            group_vars,
            aggs,
        });

        // WHERE over the post-aggregate aliases cannot inline into the Aggregate,
        // so it becomes a Filter over the new scope.
        if let Some(wc) = &w.where_clause {
            let pred = self.lower_expr(&wc.predicate, wc.span, s);
            s.builder.push_op_mut(GraphOp::Filter { predicate: pred });
        }
        if let Some(ob) = &w.order_by {
            self.push_sort_rewritten(&ob.items, &projection_bindings, s);
        }
        push_skip_limit(self, w.skip.as_ref(), w.limit.as_ref(), s);
    }

    /// Lower aggregate calls nested inside WITH expressions as Aggregate -> Project.
    /// Non-aggregate WITH items define the only legal implicit grouping leaves.
    #[allow(clippy::too_many_lines)]
    fn lower_with_aggregate_arith(
        &self,
        w: &WithClause,
        items: &[ReturnItem],
        s: &mut BinderState,
    ) {
        let group_items: Vec<&ReturnItem> = items
            .iter()
            .filter(|item| !expr_contains_aggregate(&item.expr))
            .collect();
        let mut group_by = Vec::with_capacity(group_items.len());
        let mut group_aliases = Vec::with_capacity(group_items.len());
        let mut group_vars = Vec::with_capacity(group_items.len());
        let mut group_bindings: Vec<(Expr, String, VarId)> = Vec::with_capacity(group_items.len());
        let mut grouped_nodes: Vec<(String, VarId, Option<String>)> = Vec::new();
        let mut grouped_edges: Vec<GroupedEdgeBinding> = Vec::new();
        let mut grouped_paths: Vec<(String, PathBinding)> = Vec::new();
        let mut grouped_path_nodes: Vec<(VarId, Option<String>)> = Vec::new();
        let mut grouped_path_edges: Vec<ForwardedEdgeBinding> = Vec::new();

        for item in &group_items {
            let alias = item.alias.clone().or_else(|| match &item.expr {
                Expr::Var(VarRef { name, .. }) => Some(name.clone()),
                _ => None,
            });
            let Some(alias) = alias else {
                s.errors.push(BindError::new(
                    BindErrorKind::UnsupportedClause,
                    item.span,
                    "a non-variable WITH item must be aliased (`expr AS name`)",
                ));
                continue;
            };
            if let Expr::Var(VarRef { name, .. }) = &item.expr
                && let Some(binding) = s.path_vars.get(name).cloned()
            {
                let mut vars = binding.nodes.clone();
                vars.extend(binding.segments.iter().map(|segment| segment.edge));
                vars.sort_by_key(|var| var.0);
                vars.dedup();
                for var in vars {
                    group_by.push(s.builder.push_expr(IrExpr::VarRef(var)));
                    group_aliases.push(None);
                    group_vars.push(Some(var));
                    if let Some(label) = s.node_vars.get(&var).cloned() {
                        grouped_path_nodes.push((var, label));
                    }
                    if let Some(rel_name) = s.edge_rel_names.get(&var).cloned() {
                        grouped_path_edges.push(ForwardedEdgeBinding {
                            var,
                            rel_name,
                            endpoints: s.edge_vars.get(&var).copied(),
                        });
                    }
                }
                grouped_paths.push((alias, binding));
                continue;
            }
            let entity_var = match &item.expr {
                Expr::Var(VarRef { name, .. }) => s.vars.get(name).copied().filter(|var| {
                    s.node_vars.contains_key(var) || s.edge_rel_names.contains_key(var)
                }),
                Expr::FunctionCall(call) => Self::resolve_endpoint_node(call, s),
                _ => None,
            };
            if let Some(var) = entity_var {
                group_by.push(s.builder.push_expr(IrExpr::VarRef(var)));
                group_aliases.push(None);
                group_vars.push(Some(var));
                group_bindings.push((item.expr.clone(), alias.clone(), var));
                if let Some(label) = s.node_vars.get(&var).cloned() {
                    grouped_nodes.push((alias, var, label));
                } else if let Some(rel_name) = s.edge_rel_names.get(&var).cloned() {
                    grouped_edges.push(GroupedEdgeBinding {
                        alias,
                        var,
                        rel_name,
                        endpoints: s.edge_vars.get(&var).copied(),
                    });
                }
                continue;
            }

            let out_var = alloc_anon_var(s);
            group_by.push(self.lower_expr(&item.expr, item.span, s));
            group_aliases.push(Some(alias.clone()));
            group_vars.push(Some(out_var));
            group_bindings.push((item.expr.clone(), alias, out_var));
        }

        let mut aggs = Vec::new();
        let mut aggregate_bindings = Vec::new();
        let mut rewritten_items = Vec::with_capacity(items.len());
        for item in items {
            let alias = item.alias.clone().or_else(|| match &item.expr {
                Expr::Var(VarRef { name, .. }) => Some(name.clone()),
                _ => None,
            });
            let Some(alias) = alias else {
                s.errors.push(BindError::new(
                    BindErrorKind::UnsupportedClause,
                    item.span,
                    "an aggregate expression in WITH must be aliased (`expr AS name`)",
                ));
                continue;
            };

            if expr_contains_aggregate(&item.expr) {
                let mut refs = Vec::new();
                collect_grouping_refs(&item.expr, &mut refs);
                let ambiguous = refs.iter().any(|reference| {
                    !group_bindings.iter().any(|(group, _, _)| {
                        is_atomic_grouping_expr(group) && same_grouping_expr(reference, group)
                    })
                });
                if ambiguous {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        item.span,
                        "ambiguous aggregation expression: every variable or property outside an \
                         aggregate must be projected as its own WITH grouping key",
                    ));
                }
                let first_agg = aggs.len();
                let rewritten = self.rewrite_aggs(&item.expr, &mut aggs, s);
                for (index, agg) in aggs.iter().enumerate().skip(first_agg) {
                    if let Some(var) = agg.out_var {
                        aggregate_bindings.push((format!("__agg_{index}"), var));
                    }
                }
                if aggs.len() == first_agg + 1 {
                    aggs[first_agg].alias.clone_from(&alias);
                }
                rewritten_items.push((rewritten, alias, item.span));
            } else {
                rewritten_items.push((
                    Expr::Var(VarRef {
                        name: alias.clone(),
                        span: item.span,
                    }),
                    alias,
                    item.span,
                ));
            }
        }

        let aggregate_scope: Vec<(String, VarId)> = group_bindings
            .iter()
            .map(|(_, alias, var)| (alias.clone(), *var))
            .chain(aggregate_bindings)
            .collect();

        s.vars.clear();
        s.node_vars.clear();
        s.edge_vars.clear();
        s.edge_rel_names.clear();
        s.path_vars.clear();
        for (name, var) in &aggregate_scope {
            s.vars.insert(name.clone(), *var);
        }
        for (_, var, label) in &grouped_nodes {
            s.node_vars.insert(*var, label.clone());
        }
        for edge in &grouped_edges {
            s.edge_rel_names.insert(edge.var, edge.rel_name.clone());
            if let Some(endpoints) = edge.endpoints {
                s.edge_vars.insert(edge.var, endpoints);
            }
        }
        for (var, label) in &grouped_path_nodes {
            s.node_vars.insert(*var, label.clone());
        }
        for edge in &grouped_path_edges {
            s.edge_rel_names.insert(edge.var, edge.rel_name.clone());
            if let Some(endpoints) = edge.endpoints {
                s.edge_vars.insert(edge.var, endpoints);
            }
        }
        for (alias, binding) in &grouped_paths {
            s.path_vars.insert(alias.clone(), binding.clone());
        }

        s.builder.push_op_mut(GraphOp::Aggregate {
            group_by,
            group_aliases,
            group_vars,
            aggs,
        });

        let mut project = Vec::with_capacity(rewritten_items.len());
        let mut final_scope = Vec::with_capacity(rewritten_items.len());
        let mut final_nodes = Vec::new();
        let mut final_edges = Vec::new();
        let mut final_paths = Vec::new();
        for (expr, alias, span) in rewritten_items {
            let expr = rewrite_grouping_refs(expr, &group_bindings);
            let expr = self.lower_expr(&expr, span, s);
            let node_binding = grouped_nodes
                .iter()
                .find(|(group_alias, _, _)| group_alias == &alias);
            let edge_binding = grouped_edges.iter().find(|binding| binding.alias == alias);
            let out_var = node_binding
                .map(|(_, var, _)| *var)
                .or_else(|| edge_binding.map(|binding| binding.var))
                .unwrap_or_else(|| alloc_anon_var(s));
            project.push(ProjectItem {
                expr,
                alias: Some(alias.clone()),
                out_var: Some(out_var),
            });
            if let Some((_, _, label)) = node_binding {
                final_nodes.push((out_var, label.clone()));
            }
            if let Some(binding) = edge_binding {
                final_edges.push((out_var, binding.rel_name.clone(), binding.endpoints));
            }
            if let Some((_, binding)) = grouped_paths
                .iter()
                .find(|(group_alias, _)| group_alias == &alias)
            {
                final_paths.push((alias.clone(), binding.clone()));
            }
            final_scope.push((alias, out_var));
        }
        // Keep path components as hidden WITH outputs. They are not installed
        // as named scope entries, so `RETURN *` exposes only declared aliases,
        // while downstream path functions can still rebuild the path value.
        let mut hidden_path_vars = grouped_paths
            .iter()
            .flat_map(|(_, binding)| {
                binding
                    .nodes
                    .iter()
                    .copied()
                    .chain(binding.segments.iter().map(|segment| segment.edge))
            })
            .collect::<Vec<_>>();
        hidden_path_vars.sort_by_key(|var| var.0);
        hidden_path_vars.dedup();
        for var in hidden_path_vars {
            project.push(ProjectItem {
                expr: s.builder.push_expr(IrExpr::VarRef(var)),
                alias: Some(format!("__path_{}", var.0)),
                out_var: Some(var),
            });
        }
        s.builder.push_op_mut(GraphOp::With {
            items: project,
            distinct: w.distinct,
            where_predicate: None,
        });

        s.vars.clear();
        s.node_vars.clear();
        s.edge_vars.clear();
        s.edge_rel_names.clear();
        s.path_vars.clear();
        for (name, var) in final_scope {
            s.vars.insert(name, var);
        }
        for (var, label) in final_nodes {
            s.node_vars.insert(var, label);
        }
        for (var, rel_name, endpoints) in final_edges {
            s.edge_rel_names.insert(var, rel_name);
            if let Some(endpoints) = endpoints {
                s.edge_vars.insert(var, endpoints);
            }
        }
        for (name, binding) in final_paths {
            s.path_vars.insert(name, binding);
        }
        for (var, label) in grouped_path_nodes {
            s.node_vars.insert(var, label);
        }
        for edge in grouped_path_edges {
            s.edge_rel_names.insert(edge.var, edge.rel_name);
            if let Some(endpoints) = edge.endpoints {
                s.edge_vars.insert(edge.var, endpoints);
            }
        }
        if let Some(wc) = &w.where_clause {
            self.lower_where_predicate(&wc.predicate, wc.span, s);
        }
        if let Some(ob) = &w.order_by {
            self.push_sort(&ob.items, s);
        }
        push_skip_limit(self, w.skip.as_ref(), w.limit.as_ref(), s);
    }

    fn lower_return(&self, r: &gf_ast::ReturnClause, s: &mut BinderState) {
        if reject_empty_projection_wildcard(&r.items, s) {
            return;
        }
        check_duplicate_aliases(&r.items, s);
        // Expand a `RETURN *` wildcard to one item per in-scope named variable.
        let items = expand_projection_wildcard(&r.items, s);
        if r.order_by
            .as_ref()
            .is_some_and(|order_by| has_unprojected_order_aggregate(&items, &order_by.items))
        {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                r.order_by.as_ref().expect("checked above").span,
                "an aggregate function in ORDER BY must also appear in the RETURN projection",
            ));
            return;
        }
        // A NESTED aggregate (`count(*) + 1`, `count(a) + count(b)`) is one that
        // is not the whole item. Reuse WITH's aggregate-to-project path: it
        // validates implicit grouping leaves and supports aggregates nested in
        // maps, lists, and arithmetic expressions.
        let has_nested = items
            .iter()
            .any(|i| agg_func_of(&i.expr).is_none() && expr_contains_aggregate(&i.expr));
        if items
            .iter()
            .any(|item| expr_contains_aggregate_inside_aggregate(&item.expr, false))
        {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                r.span,
                "an aggregate function may not contain another aggregate function",
            ));
            return;
        }
        let mut aggregate_exprs = Vec::new();
        for item in &items {
            collect_aggregate_exprs(&item.expr, &mut aggregate_exprs);
        }
        if aggregate_exprs.iter().any(|expr| match expr {
            Expr::FunctionCall(call) => call.args.iter().any(expr_contains_volatile_function),
            _ => false,
        }) {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                r.span,
                "non-deterministic functions are not allowed inside aggregate arguments",
            ));
            return;
        }
        // If any item is a top-level aggregate (`count(...)`, `sum(...)`, …),
        // emit an `Aggregate` rather than a `Project`: the non-aggregate items
        // become group-by keys and the aggregates become `AggExpr`s. openCypher
        // implicit grouping = group by every non-aggregated return expression.
        if has_nested {
            self.lower_nested_return(r, &items, s);
        } else if items.iter().any(|i| agg_func_of(&i.expr).is_some()) {
            let bindings = self.lower_return_aggregate(&items, s);
            if let Some(ob) = &r.order_by {
                self.push_sort_rewritten(&ob.items, &bindings, s);
            }
            push_skip_limit(self, r.skip.as_ref(), r.limit.as_ref(), s);
        } else {
            let sort_before_projection = r.order_by.is_some() && !r.distinct;
            let (lowered, projection_scope) =
                self.lower_return_items(&items, s, true, r.order_by.is_some());
            if sort_before_projection {
                self.push_sort_before_projection(
                    &r.order_by.as_ref().expect("checked above").items,
                    &items,
                    s,
                );
            }
            let projection_bindings: Vec<(Expr, String, VarId)> = items
                .iter()
                .zip(&projection_scope)
                .map(|(item, (alias, var))| (item.expr.clone(), alias.clone(), *var))
                .collect();
            s.builder.push_op_mut(GraphOp::Project {
                items: lowered,
                distinct: r.distinct,
            });
            if r.distinct {
                s.vars.clear();
                s.node_vars.clear();
                s.edge_vars.clear();
                s.edge_rel_names.clear();
                s.path_vars.clear();
            }
            for (name, v) in &projection_scope {
                s.vars.insert(name.clone(), *v);
            }
            if let Some(ob) = &r.order_by
                && !sort_before_projection
            {
                self.push_sort_rewritten(&ob.items, &projection_bindings, s);
            }
            push_skip_limit(self, r.skip.as_ref(), r.limit.as_ref(), s);
        }
    }

    fn lower_nested_return(
        &self,
        r: &gf_ast::ReturnClause,
        items: &[ReturnItem],
        s: &mut BinderState,
    ) {
        let aggregate_items = items
            .iter()
            .cloned()
            .map(|mut item| {
                if item.alias.is_none() {
                    item.alias.clone_from(&item.display);
                }
                item
            })
            .collect::<Vec<_>>();
        let aggregate_return = WithClause {
            distinct: r.distinct,
            items: aggregate_items.clone(),
            order_by: r.order_by.clone(),
            skip: r.skip.clone(),
            limit: r.limit.clone(),
            where_clause: None,
            span: r.span,
        };
        self.lower_with_aggregate_arith(&aggregate_return, &aggregate_items, s);
        let terminal_items = aggregate_items
            .iter()
            .filter_map(|item| {
                let name = item.alias.clone()?;
                Some(ReturnItem {
                    expr: Expr::Var(VarRef {
                        name: name.clone(),
                        span: item.span,
                    }),
                    alias: Some(name.clone()),
                    display: Some(name),
                    span: item.span,
                })
            })
            .collect();
        self.lower_return(
            &gf_ast::ReturnClause {
                distinct: false,
                items: terminal_items,
                order_by: None,
                skip: None,
                limit: None,
                span: r.span,
            },
            s,
        );
    }

    fn lower_unwind(&self, u: &gf_ast::UnwindClause, s: &mut BinderState) {
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

    fn push_sort(&self, items: &[SortItem], s: &mut BinderState) {
        let keys: Vec<SortKey> = items
            .iter()
            .map(|item| {
                let expr = self.lower_expr(&item.expr, item.span, s);
                SortKey {
                    expr,
                    order: match item.order {
                        AstSortOrder::Ascending => SortOrder::Asc,
                        AstSortOrder::Descending => SortOrder::Desc,
                    },
                    nulls_first: false,
                }
            })
            .collect();
        if !keys.is_empty() {
            s.builder.push_op_mut(GraphOp::Sort { keys });
        }
    }

    fn push_sort_rewritten(
        &self,
        items: &[SortItem],
        bindings: &[(Expr, String, VarId)],
        s: &mut BinderState,
    ) {
        let keys = items
            .iter()
            .map(|item| {
                let rewritten = rewrite_grouping_refs(item.expr.clone(), bindings);
                SortKey {
                    expr: self.lower_expr(&rewritten, item.span, s),
                    order: match item.order {
                        AstSortOrder::Ascending => SortOrder::Asc,
                        AstSortOrder::Descending => SortOrder::Desc,
                    },
                    nulls_first: false,
                }
            })
            .collect::<Vec<_>>();
        if !keys.is_empty() {
            s.builder.push_op_mut(GraphOp::Sort { keys });
        }
    }

    fn push_sort_before_projection(
        &self,
        items: &[SortItem],
        projections: &[ReturnItem],
        s: &mut BinderState,
    ) {
        let rewritten = items
            .iter()
            .map(|item| {
                let expr = rewrite_projection_alias_refs(item.expr.clone(), projections);
                SortKey {
                    expr: self.lower_expr(&expr, item.span, s),
                    order: match item.order {
                        AstSortOrder::Ascending => SortOrder::Asc,
                        AstSortOrder::Descending => SortOrder::Desc,
                    },
                    nulls_first: false,
                }
            })
            .collect::<Vec<_>>();
        if !rewritten.is_empty() {
            s.builder.push_op_mut(GraphOp::Sort { keys: rewritten });
        }
    }

    fn lower_return_items(
        &self,
        items: &[ReturnItem],
        s: &mut BinderState,
        materialize_nodes: bool,
        bind_projection_scope: bool,
    ) -> (Vec<ProjectItem>, Vec<(String, VarId)>) {
        let mut projection_scope = Vec::new();
        let out = items
            .iter()
            .map(|item| {
                let expr = self.lower_return_item_expr(&item.expr, item.span, s, materialize_nodes);
                // Column name: an explicit `AS alias` wins. Otherwise a bare path
                // (`RETURN p`, #754) or — in a terminal RETURN — node (`RETURN n`,
                // #785) variable rewrites to a composed expression whose generated
                // column name would be the expression display string, so alias it
                // back to the variable name the query wrote. Every other un-aliased
                // item (`n.prop`, `count(*)`, `a.x IS NULL`) is named by its verbatim
                // source text (`item.display`), matching openCypher (#598).
                let alias = item.alias.clone().or_else(|| match &item.expr {
                    Expr::Var(VarRef { name, .. })
                        if s.path_vars.contains_key(name)
                            || (materialize_nodes
                                && s.vars.get(name).is_some_and(|v| {
                                    s.node_vars.contains_key(v) || s.edge_rel_names.contains_key(v)
                                })) =>
                    {
                        Some(name.clone())
                    }
                    _ => item.display.clone(),
                });
                let out_var = if bind_projection_scope {
                    alias.as_ref().map(|name| {
                        let v = alloc_anon_var(s);
                        projection_scope.push((name.clone(), v));
                        v
                    })
                } else {
                    None
                };
                ProjectItem {
                    expr,
                    alias,
                    out_var,
                }
            })
            .collect();
        (out, projection_scope)
    }

    /// Lower a single RETURN item's expression. A bare node variable (`RETURN n`)
    /// over a `NodeScan`-bound var rewrites to a `_node_struct` call so the
    /// lowerer materializes a whole node value (identity + labels + properties) —
    /// #785. A bare fixed-hop relationship variable (`RETURN r`) rewrites to a
    /// `_rel_struct` call so it materializes as a whole relationship value —
    /// identity + type + properties (#889). A property access (`n.name`,
    /// `r.since`) keeps its `VarRef` base unchanged.
    fn lower_return_item_expr(
        &self,
        expr: &Expr,
        span: Span,
        s: &mut BinderState,
        materialize_nodes: bool,
    ) -> ExprId {
        if let Some(id) = self.lower_projection_value_expr(expr, span, s, materialize_nodes) {
            return id;
        }
        self.lower_expr(expr, span, s)
    }

    fn lower_projection_value_expr(
        &self,
        expr: &Expr,
        span: Span,
        s: &mut BinderState,
        materialize_nodes: bool,
    ) -> Option<ExprId> {
        if materialize_nodes {
            // A bare node var (`RETURN n`, #785) or a relationship endpoint
            // (`RETURN startNode(r)` / `endNode(r)`, #753) materializes a whole
            // node value. Both resolve to a node var bound to a `NodeScan`.
            let node_var = match expr {
                Expr::Var(VarRef { name, .. }) => s
                    .vars
                    .get(name)
                    .copied()
                    .filter(|v| s.node_vars.contains_key(v)),
                Expr::FunctionCall(call) => Self::resolve_endpoint_node(call, s),
                _ => None,
            };
            if let Some(v) = node_var {
                return Some(Self::node_struct_expr(v, s));
            }
        }

        match expr {
            Expr::Var(VarRef { name, .. }) => {
                let var = s
                    .vars
                    .get(name)
                    .copied()
                    .filter(|v| s.edge_rel_names.contains_key(v))?;
                Some(if s.scalar_list_edges.contains(&var) {
                    Self::relationship_struct_list_expr(var, s)
                } else {
                    Self::relationship_struct_expr(var, s)
                })
            }
            Expr::FunctionCall(call)
                if is_function_named(call, "type")
                    && call.args.len() == 1
                    && !Self::invalid_direct_graph_function_argument(call, s) =>
            {
                let arg = self
                    .lower_projection_value_expr(&call.args[0], call.span, s, materialize_nodes)
                    .unwrap_or_else(|| self.lower_expr(&call.args[0], call.span, s));
                Some(s.builder.push_expr(IrExpr::FunctionCall {
                    name: "type".into(),
                    args: vec![arg],
                }))
            }
            Expr::FunctionCall(call)
                if materialize_nodes
                    && is_function_named(call, "coalesce")
                    && !call.args.is_empty() =>
            {
                let args = call
                    .args
                    .iter()
                    .map(|arg| self.lower_projection_value_expr(arg, span, s, true))
                    .collect::<Option<Vec<_>>>()?;
                Some(s.builder.push_expr(IrExpr::FunctionCall {
                    name: "coalesce".into(),
                    args,
                }))
            }
            Expr::List(gf_ast::ListLiteral { elements, .. }) => {
                let ids: Vec<ExprId> = elements
                    .iter()
                    .map(|e| {
                        self.lower_projection_value_expr(e, span, s, materialize_nodes)
                            .unwrap_or_else(|| self.lower_expr(e, span, s))
                    })
                    .collect();
                Some(s.builder.push_expr(IrExpr::ListLiteral(ids)))
            }
            _ => None,
        }
    }

    /// Lower a RETURN that contains at least one aggregate into a
    /// [`GraphOp::Aggregate`]. Non-aggregate items become group-by keys;
    /// aggregate items become [`AggExpr`]s.
    fn lower_return_aggregate(
        &self,
        items: &[ReturnItem],
        s: &mut BinderState,
    ) -> Vec<(Expr, String, VarId)> {
        let mut group_by: Vec<ExprId> = Vec::new();
        let mut group_aliases: Vec<Option<String>> = Vec::new();
        let mut group_vars: Vec<Option<VarId>> = Vec::new();
        let mut aggs: Vec<AggExpr> = Vec::new();
        let mut bindings = Vec::with_capacity(items.len());
        for (idx, item) in items.iter().enumerate() {
            if let Some(func) = agg_func_of(&item.expr) {
                let Expr::FunctionCall(call) = &item.expr else {
                    unreachable!("agg_func_of only matches FunctionCall");
                };
                // openCypher names an un-aliased aggregate column by its source
                // text (`count(*)`, `min(x)`); fall back to the verbatim `display`
                // before the synthetic `agg_N` (#598/#599). Matches the non-
                // aggregate naming in `lower_return_items`.
                let alias = item
                    .alias
                    .clone()
                    .or_else(|| item.display.clone())
                    .unwrap_or_else(|| format!("agg_{idx}"));
                let out_var = alloc_anon_var(s);
                aggs.push(self.build_agg(call, func, alias.clone(), Some(out_var), s));
                bindings.push((item.expr.clone(), alias, out_var));
            } else {
                // Materialize whole-node / path group keys (`RETURN n, count(*)`)
                // the same way terminal RETURN items do — a bare node var rewrites
                // to `_node_struct`, not a bare `var_N` (which is not a real column
                // and would lower against an unbound reference). Non-node exprs
                // (`n.name`) fall through to the same `lower_expr` inside.
                group_by.push(self.lower_return_item_expr(&item.expr, item.span, s, true));
                // Name the group-key column by the RETURN item's source text
                // (or its `AS` alias) so a mixed `RETURN n.name, count(*)` yields
                // the `n.name` header openCypher expects (#599). A bare variable
                // (`RETURN n, count(*)`) keeps its lowered name — `display` is the
                // var name, which is the right column name anyway.
                let alias = item
                    .alias
                    .clone()
                    .or_else(|| item.display.clone())
                    .unwrap_or_else(|| format!("group_{idx}"));
                let out_var = alloc_anon_var(s);
                group_aliases.push(Some(alias.clone()));
                group_vars.push(Some(out_var));
                bindings.push((item.expr.clone(), alias, out_var));
            }
        }
        s.builder.push_op_mut(GraphOp::Aggregate {
            group_by,
            group_aliases,
            group_vars,
            aggs,
        });
        s.vars.clear();
        s.node_vars.clear();
        s.edge_vars.clear();
        s.edge_rel_names.clear();
        s.path_vars.clear();
        for (_, alias, var) in &bindings {
            s.vars.insert(alias.clone(), *var);
        }
        bindings
    }

    /// Build one [`AggExpr`] from an aggregate call. `count(*)` has no argument;
    /// `count(n)` over a bare variable counts bound rows (equivalent to `count(*)`
    /// — a MATCH variable is always bound — and avoids referencing the bare
    /// `var_N`, which is not a real column). Distinct aggregates keep the
    /// argument so optional/unbound values can be filtered correctly. Distinct
    /// aggregate calls are preserved in the IR so lowering can pick the matching
    /// DataFusion/Cypher implementation. `out_var`, when set, binds the result
    /// column for a following `Project` (#599 nested aggregates).
    fn build_agg(
        &self,
        call: &gf_ast::FunctionCall,
        func: AggFunc,
        alias: String,
        out_var: Option<VarId>,
        s: &mut BinderState,
    ) -> AggExpr {
        let bare_var_arg = matches!(call.args.first(), Some(Expr::Var(_)));
        let is_percentile = matches!(func, AggFunc::PercentileDisc | AggFunc::PercentileCont);
        if is_percentile {
            if call.star || call.args.len() != 2 {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    call.span,
                    "percentile aggregate functions require value and percentile arguments",
                ));
            }
            if call.distinct {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    call.span,
                    "percentile aggregate functions do not support DISTINCT",
                ));
            }
        }
        let arg = if !is_percentile
            && (call.star || (func == AggFunc::Count && bare_var_arg && !call.distinct))
        {
            None
        } else {
            call.args.first().map(|a| {
                if func == AggFunc::Collect {
                    self.lower_projection_value_expr(a, call.span, s, true)
                        .unwrap_or_else(|| self.lower_expr(a, call.span, s))
                } else {
                    self.lower_expr(a, call.span, s)
                }
            })
        };
        let percentile = if is_percentile {
            call.args.get(1).map(|p| self.lower_expr(p, call.span, s))
        } else {
            None
        };
        let func = match (call.distinct, func, arg.is_some()) {
            (true, AggFunc::Count, true) => AggFunc::CountDistinct,
            (true, AggFunc::Sum, true) => AggFunc::SumDistinct,
            (true, AggFunc::Avg, true) => AggFunc::AvgDistinct,
            (true, AggFunc::Collect, true) => AggFunc::CollectDistinct,
            _ => func,
        };
        AggExpr {
            func,
            arg,
            percentile,
            alias,
            out_var,
        }
    }

    /// Lower a RETURN where some item NESTS an aggregate inside a larger
    /// Rewrite an expression, replacing each aggregate call with a fresh synthetic
    /// variable (registered in scope, bound to the aggregate's output column) and
    /// recording the [`AggExpr`]. Non-aggregate container nodes are rebuilt with
    /// rewritten children; leaves are cloned. Used by
    /// [`lower_return_aggregate_arith`]. (#599)
    #[allow(clippy::too_many_lines)]
    fn rewrite_aggs(&self, expr: &Expr, aggs: &mut Vec<AggExpr>, s: &mut BinderState) -> Expr {
        if let Some(func) = agg_func_of(expr) {
            let Expr::FunctionCall(call) = expr else {
                unreachable!("agg_func_of only matches FunctionCall");
            };
            let name = format!("__agg_{}", aggs.len());
            let out_var = ensure_var_name(&name, s);
            let agg = self.build_agg(call, func, name.clone(), Some(out_var), s);
            aggs.push(agg);
            return Expr::Var(VarRef {
                name,
                span: call.span,
            });
        }
        match expr {
            Expr::BinaryOp(b) => Expr::BinaryOp(gf_ast::BinaryOp {
                op: b.op,
                left: Box::new(self.rewrite_aggs(&b.left, aggs, s)),
                right: Box::new(self.rewrite_aggs(&b.right, aggs, s)),
                span: b.span,
            }),
            Expr::UnaryOp(u) => Expr::UnaryOp(gf_ast::UnaryOp {
                op: u.op,
                expr: Box::new(self.rewrite_aggs(&u.expr, aggs, s)),
                span: u.span,
            }),
            Expr::Parenthesized { inner, span } => Expr::Parenthesized {
                inner: Box::new(self.rewrite_aggs(inner, aggs, s)),
                span: *span,
            },
            Expr::ListComprehension(lc) => Expr::ListComprehension(gf_ast::ListComprehension {
                var: lc.var.clone(),
                list: Box::new(self.rewrite_aggs(&lc.list, aggs, s)),
                // Aggregation inside a comprehension body is invalid Cypher. Keep
                // those children intact so lower_expr emits the established error.
                filter: lc.filter.clone(),
                projection: lc.projection.clone(),
                span: lc.span,
            }),
            Expr::FunctionCall(c) => Expr::FunctionCall(gf_ast::FunctionCall {
                name: c.name.clone(),
                distinct: c.distinct,
                star: c.star,
                args: c
                    .args
                    .iter()
                    .map(|a| self.rewrite_aggs(a, aggs, s))
                    .collect(),
                span: c.span,
            }),
            Expr::List(l) => Expr::List(gf_ast::ListLiteral {
                elements: l
                    .elements
                    .iter()
                    .map(|e| self.rewrite_aggs(e, aggs, s))
                    .collect(),
                span: l.span,
            }),
            Expr::Map(m) => Expr::Map(gf_ast::MapLiteral {
                entries: m
                    .entries
                    .iter()
                    .map(|(key, value)| (key.clone(), self.rewrite_aggs(value, aggs, s)))
                    .collect(),
                key_spans: m.key_spans.clone(),
                span: m.span,
            }),
            Expr::Case(c) => Expr::Case(gf_ast::CaseExpr {
                subject: c
                    .subject
                    .as_deref()
                    .map(|subject| Box::new(self.rewrite_aggs(subject, aggs, s))),
                when_clauses: c
                    .when_clauses
                    .iter()
                    .map(|when| gf_ast::WhenClause {
                        condition: self.rewrite_aggs(&when.condition, aggs, s),
                        result: self.rewrite_aggs(&when.result, aggs, s),
                        span: when.span,
                    })
                    .collect(),
                else_expr: c
                    .else_expr
                    .as_deref()
                    .map(|else_expr| Box::new(self.rewrite_aggs(else_expr, aggs, s))),
                span: c.span,
            }),
            Expr::Quantifier(q) => Expr::Quantifier(gf_ast::Quantifier {
                kind: q.kind,
                var: q.var.clone(),
                list: Box::new(self.rewrite_aggs(&q.list, aggs, s)),
                predicate: Box::new(self.rewrite_aggs(&q.predicate, aggs, s)),
                span: q.span,
            }),
            Expr::PatternComprehension(pc) => {
                Expr::PatternComprehension(gf_ast::PatternComprehension {
                    var: pc.var.clone(),
                    pattern: pc.pattern.clone(),
                    filter: pc.filter.clone(),
                    projection: pc.projection.clone(),
                    span: pc.span,
                })
            }
            Expr::ExistentialSubquery(es) => Expr::ExistentialSubquery(es.clone()),
            Expr::Property(p) => Expr::Property(gf_ast::PropertyAccess {
                object: Box::new(self.rewrite_aggs(&p.object, aggs, s)),
                key: p.key.clone(),
                span: p.span,
            }),
            Expr::IsNull {
                expr,
                negated,
                span,
            } => Expr::IsNull {
                expr: Box::new(self.rewrite_aggs(expr, aggs, s)),
                negated: *negated,
                span: *span,
            },
            Expr::InList {
                expr,
                list,
                negated,
                span,
            } => Expr::InList {
                expr: Box::new(self.rewrite_aggs(expr, aggs, s)),
                list: Box::new(self.rewrite_aggs(list, aggs, s)),
                negated: *negated,
                span: *span,
            },
            Expr::StringOp {
                expr,
                op,
                pattern,
                span,
            } => Expr::StringOp {
                expr: Box::new(self.rewrite_aggs(expr, aggs, s)),
                op: *op,
                pattern: Box::new(self.rewrite_aggs(pattern, aggs, s)),
                span: *span,
            },
            Expr::RegexMatch {
                expr,
                pattern,
                span,
            } => Expr::RegexMatch {
                expr: Box::new(self.rewrite_aggs(expr, aggs, s)),
                pattern: Box::new(self.rewrite_aggs(pattern, aggs, s)),
                span: *span,
            },
            other => other.clone(),
        }
    }

    // -----------------------------------------------------------------------
    // Expression lowering
    // -----------------------------------------------------------------------

    fn typed_uuid_param_in<'a>(&self, expr: &'a Expr) -> Option<&'a str> {
        match expr {
            Expr::Param(param) if self.typed_uuid_params.contains_key(&param.name) => {
                Some(&param.name)
            }
            Expr::Parenthesized { inner, .. }
            | Expr::UnaryOp(gf_ast::UnaryOp { expr: inner, .. }) => self.typed_uuid_param_in(inner),
            Expr::List(list) => list
                .elements
                .iter()
                .find_map(|item| self.typed_uuid_param_in(item)),
            Expr::Map(map) => map
                .entries
                .values()
                .find_map(|value| self.typed_uuid_param_in(value)),
            Expr::FunctionCall(call) => call
                .args
                .iter()
                .find_map(|arg| self.typed_uuid_param_in(arg)),
            Expr::BinaryOp(binary) => self
                .typed_uuid_param_in(&binary.left)
                .or_else(|| self.typed_uuid_param_in(&binary.right)),
            Expr::Case(case) => case
                .subject
                .as_deref()
                .and_then(|subject| self.typed_uuid_param_in(subject))
                .or_else(|| {
                    case.when_clauses.iter().find_map(|when| {
                        self.typed_uuid_param_in(&when.condition)
                            .or_else(|| self.typed_uuid_param_in(&when.result))
                    })
                })
                .or_else(|| {
                    case.else_expr
                        .as_deref()
                        .and_then(|otherwise| self.typed_uuid_param_in(otherwise))
                }),
            Expr::ListComprehension(comprehension) => self
                .typed_uuid_param_in(&comprehension.list)
                .or_else(|| {
                    comprehension
                        .filter
                        .as_deref()
                        .and_then(|filter| self.typed_uuid_param_in(filter))
                })
                .or_else(|| {
                    comprehension
                        .projection
                        .as_deref()
                        .and_then(|projection| self.typed_uuid_param_in(projection))
                }),
            Expr::Quantifier(quantifier) => self
                .typed_uuid_param_in(&quantifier.list)
                .or_else(|| self.typed_uuid_param_in(&quantifier.predicate)),
            Expr::PatternComprehension(comprehension) => comprehension
                .filter
                .as_deref()
                .and_then(|filter| self.typed_uuid_param_in(filter))
                .or_else(|| self.typed_uuid_param_in(&comprehension.projection)),
            Expr::IsNull { expr, .. } => self.typed_uuid_param_in(expr),
            Expr::InList { expr, list, .. } => self
                .typed_uuid_param_in(expr)
                .or_else(|| self.typed_uuid_param_in(list)),
            Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => self
                .typed_uuid_param_in(expr)
                .or_else(|| self.typed_uuid_param_in(pattern)),
            _ => None,
        }
    }

    fn direct_typed_uuid_identity_parameter(
        &self,
        property_expr: &Expr,
        value_expr: &Expr,
        s: &BinderState,
    ) -> Option<String> {
        let mut value_expr = value_expr;
        while let Expr::Parenthesized { inner, .. } = value_expr {
            value_expr = inner;
        }
        let Expr::Param(param) = value_expr else {
            return None;
        };
        if self.typed_uuid_params.get(&param.name) != Some(&UuidParamClass::ExactUuid) {
            return None;
        }
        let mut property_expr = property_expr;
        while let Expr::Parenthesized { inner, .. } = property_expr {
            property_expr = inner;
        }
        let Expr::Property(PropertyAccess { object, key, .. }) = property_expr else {
            return None;
        };
        let mut object = object.as_ref();
        while let Expr::Parenthesized { inner, .. } = object {
            object = inner;
        }
        let actual = match object {
            Expr::Var(VarRef { name, .. }) => s
                .vars
                .get(name)
                .and_then(|var| s.var_kinds.get(var))
                .copied(),
            _ => None,
        };
        let compatible = matches!(
            (key.as_str(), actual),
            ("node_uuid", Some(VarKind::Node)) | ("edge_uuid", Some(VarKind::Relationship))
        );
        compatible.then(|| param.name.clone())
    }

    fn lower_expr_with_direct_uuid_allowed(
        &self,
        expr: &Expr,
        parent_span: Span,
        allowed: Option<&str>,
        s: &mut BinderState,
    ) -> ExprId {
        let mut direct = expr;
        while let Expr::Parenthesized { inner, .. } = direct {
            direct = inner;
        }
        if let Expr::Param(param) = direct
            && allowed == Some(param.name.as_str())
        {
            return s.builder.push_expr(IrExpr::Parameter(param.name.clone()));
        }
        self.lower_expr(expr, parent_span, s)
    }

    #[allow(clippy::only_used_in_recursion, clippy::too_many_lines)]
    fn lower_expr(&self, expr: &Expr, parent_span: Span, s: &mut BinderState) -> ExprId {
        match expr {
            Expr::Literal(lit) => {
                if let Literal::Float(f, span) = lit
                    && !f.is_finite()
                {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        *span,
                        "float literal is outside the supported f64 range",
                    ));
                }
                s.builder.push_expr(IrExpr::Literal(lower_literal(lit)))
            }

            Expr::Var(VarRef { name, span }) => {
                if let Some(&var_id) = s.vars.get(name) {
                    s.builder.push_expr(IrExpr::VarRef(var_id))
                } else if let Some(binding) = s.path_vars.get(name).cloned() {
                    // A bare path value (`RETURN p`): a Struct{nodes,
                    // relationships} assembled from the path's constituents.
                    Self::path_struct_expr(&binding, s)
                } else {
                    s.errors.push(BindError::new(
                        BindErrorKind::UndeclaredVariable,
                        *span,
                        format!("variable `{name}` used before it was introduced"),
                    ));
                    s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
                }
            }

            Expr::Property(gf_ast::PropertyAccess { object, key, span }) => {
                if matches!(object.as_ref(), Expr::Var(VarRef { name, .. }) if s.path_vars.contains_key(name))
                {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        *span,
                        "property access is not valid on a path value",
                    ));
                    return s.builder.push_expr(IrExpr::Literal(IrLiteral::Null));
                }
                if matches!(key.as_str(), "node_uuid" | "edge_uuid") {
                    let expected = if key == "node_uuid" {
                        VarKind::Node
                    } else {
                        VarKind::Relationship
                    };
                    let actual = match object.as_ref() {
                        Expr::Var(VarRef { name, .. }) => s
                            .vars
                            .get(name)
                            .and_then(|var| s.var_kinds.get(var))
                            .copied(),
                        _ => None,
                    };
                    if actual != Some(expected) {
                        s.errors.push(BindError::new(
                            BindErrorKind::InvalidArgument,
                            *span,
                            format!(
                                "structural identity field `{key}` is valid only on {expected}"
                            ),
                        ));
                        return s.builder.push_expr(IrExpr::Literal(IrLiteral::Null));
                    }
                }
                let owner = property_owner_for_expr(object, s);
                let base = self.lower_expr(object, *span, s);
                let prop = self.resolve_property(key, *span, owner, s);
                s.builder.push_expr(IrExpr::PropertyAccess { base, prop })
            }

            Expr::BinaryOp(gf_ast::BinaryOp {
                op,
                left,
                right,
                span,
            }) => {
                let right_allowed = (*op == AstBinOp::Eq)
                    .then(|| self.direct_typed_uuid_identity_parameter(left, right, s))
                    .flatten();
                let left_allowed = (*op == AstBinOp::Eq)
                    .then(|| self.direct_typed_uuid_identity_parameter(right, left, s))
                    .flatten();
                match op {
                    AstBinOp::Concat => {
                        let a = self.lower_expr_with_direct_uuid_allowed(
                            left,
                            *span,
                            left_allowed.as_deref(),
                            s,
                        );
                        let b = self.lower_expr_with_direct_uuid_allowed(
                            right,
                            *span,
                            right_allowed.as_deref(),
                            s,
                        );
                        s.builder.push_expr(IrExpr::FunctionCall {
                            name: "string.concat".into(),
                            args: vec![a, b],
                        })
                    }
                    other => {
                        let l = self.lower_expr_with_direct_uuid_allowed(
                            left,
                            *span,
                            left_allowed.as_deref(),
                            s,
                        );
                        let r = self.lower_expr_with_direct_uuid_allowed(
                            right,
                            *span,
                            right_allowed.as_deref(),
                            s,
                        );
                        s.builder.push_expr(IrExpr::BinaryOp {
                            op: lower_binop(*other),
                            left: l,
                            right: r,
                        })
                    }
                }
            }

            Expr::UnaryOp(gf_ast::UnaryOp {
                op,
                expr: inner,
                span,
            }) => {
                if let Some(name) = self.typed_uuid_param_in(inner) {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        *span,
                        format!(
                            "typed UUID parameter `${name}` is only supported as a direct node_uuid or edge_uuid identity equality predicate"
                        ),
                    ));
                }
                let ir_op = match op {
                    AstUnOp::Not => UnaryOpKind::Not,
                    AstUnOp::Neg => UnaryOpKind::Neg,
                };
                let e = self.lower_expr(inner, *span, s);
                s.builder.push_expr(IrExpr::UnaryOp { op: ir_op, expr: e })
            }

            Expr::IsNull {
                expr: inner,
                negated,
                span,
            } => {
                let op = if *negated {
                    UnaryOpKind::IsNotNull
                } else {
                    UnaryOpKind::IsNull
                };
                let e = self.lower_expr(inner, *span, s);
                s.builder.push_expr(IrExpr::UnaryOp { op, expr: e })
            }

            Expr::InList {
                expr: lhs,
                list: rhs,
                negated,
                span,
            } => {
                let l = self.lower_expr(lhs, *span, s);
                let r = self.lower_expr(rhs, *span, s);
                let in_id = s.builder.push_expr(IrExpr::BinaryOp {
                    op: BinaryOpKind::In,
                    left: l,
                    right: r,
                });
                if *negated {
                    s.builder.push_expr(IrExpr::UnaryOp {
                        op: UnaryOpKind::Not,
                        expr: in_id,
                    })
                } else {
                    in_id
                }
            }

            Expr::StringOp {
                expr: lhs,
                op,
                pattern: rhs,
                span,
            } => {
                let ir_op = match op {
                    StringOpKind::StartsWith => BinaryOpKind::StartsWith,
                    StringOpKind::EndsWith => BinaryOpKind::EndsWith,
                    StringOpKind::Contains => BinaryOpKind::Contains,
                };
                let l = self.lower_expr(lhs, *span, s);
                let r = self.lower_expr(rhs, *span, s);
                s.builder.push_expr(IrExpr::BinaryOp {
                    op: ir_op,
                    left: l,
                    right: r,
                })
            }

            Expr::RegexMatch {
                expr: lhs,
                pattern: rhs,
                span,
            } => {
                let l = self.lower_expr(lhs, *span, s);
                let r = self.lower_expr(rhs, *span, s);
                s.builder.push_expr(IrExpr::BinaryOp {
                    op: BinaryOpKind::RegexMatch,
                    left: l,
                    right: r,
                })
            }

            Expr::Parenthesized { inner, .. } => self.lower_expr(inner, parent_span, s),

            Expr::FunctionCall(call) => {
                if !is_known_cypher_function(call) {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        call.span,
                        format!("unknown function `{}`", call.name.join(".")),
                    ));
                    s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
                } else if Self::invalid_direct_graph_function_argument(call, s) {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        call.span,
                        format!(
                            "{}() does not accept this graph value type",
                            call.name.join(".")
                        ),
                    ));
                    s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
                } else if Self::is_size_of_path_variable(call, s) {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        call.span,
                        "size() is not valid for paths; use length(path) instead",
                    ));
                    s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
                } else if is_function_named(call, "type") && call.args.len() == 1 {
                    let arg = match &call.args[0] {
                        Expr::Var(VarRef { name, .. }) => {
                            let edge_var = s
                                .vars
                                .get(name)
                                .copied()
                                .filter(|var| s.edge_rel_names.contains_key(var));
                            if let Some(var) = edge_var {
                                Self::relationship_struct_expr(var, s)
                            } else {
                                self.lower_expr(&call.args[0], parent_span, s)
                            }
                        }
                        other => self.lower_expr(other, parent_span, s),
                    };
                    s.builder.push_expr(IrExpr::FunctionCall {
                        name: "type".into(),
                        args: vec![arg],
                    })
                } else if let Some(id) = Self::lower_path_function(call, s) {
                    id
                } else if let Some(node_var) = Self::resolve_endpoint_node(call, s) {
                    // `startNode(r)` / `endNode(r)` over a matched relationship
                    // is the src / dst node var (#753). As a bare value it is a
                    // node reference; property access (`startNode(r).name`)
                    // resolves against that var's columns. A terminal RETURN
                    // upgrades it to a whole node value in `lower_return_item_expr`.
                    s.builder.push_expr(IrExpr::VarRef(node_var))
                } else {
                    let fn_name = call.name.join(".");
                    let ir_args: Vec<ExprId> = call
                        .args
                        .iter()
                        .map(|a| self.lower_expr(a, parent_span, s))
                        .collect();
                    s.builder.push_expr(IrExpr::FunctionCall {
                        name: fn_name,
                        args: ir_args,
                    })
                }
            }

            Expr::Param(gf_ast::ParamRef { name, span }) => {
                if self.typed_uuid_params.contains_key(name) {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        *span,
                        format!(
                            "typed UUID parameter `${name}` is only supported as a direct node_uuid or edge_uuid identity equality predicate"
                        ),
                    ));
                }
                s.builder.push_expr(IrExpr::Parameter(name.clone()))
            }

            Expr::Case(CaseExpr {
                subject,
                when_clauses,
                else_expr,
                ..
            }) => {
                let operand = subject
                    .as_deref()
                    .map(|e| self.lower_expr(e, parent_span, s));
                let arms: Vec<CaseArm> = when_clauses
                    .iter()
                    .map(|w| {
                        let when = self.lower_expr(&w.condition, w.span, s);
                        let when = operand.map_or(when, |subject| {
                            s.builder.push_expr(IrExpr::BinaryOp {
                                op: BinaryOpKind::Eq,
                                left: subject,
                                right: when,
                            })
                        });
                        CaseArm {
                            when,
                            then: self.lower_expr(&w.result, w.span, s),
                        }
                    })
                    .collect();
                let else_id = else_expr
                    .as_deref()
                    .map(|e| self.lower_expr(e, parent_span, s));
                s.builder.push_expr(IrExpr::Case {
                    operand: None,
                    arms,
                    else_expr: else_id,
                })
            }

            Expr::List(gf_ast::ListLiteral { elements, .. }) => {
                let ids: Vec<ExprId> = elements
                    .iter()
                    .map(|e| self.lower_expr(e, parent_span, s))
                    .collect();
                s.builder.push_expr(IrExpr::ListLiteral(ids))
            }

            Expr::Map(MapLiteral { entries, .. }) => {
                let mut pairs: Vec<(&String, &Expr)> = entries.iter().collect();
                pairs.sort_by_key(|(k, _)| k.as_str());
                let ids: Vec<(String, ExprId)> = pairs
                    .into_iter()
                    .map(|(k, v)| (k.clone(), self.lower_expr(v, parent_span, s)))
                    .collect();
                s.builder.push_expr(IrExpr::MapLiteral(ids))
            }

            // `[var IN list WHERE filter | projection]` (#955): the list is
            // lowered with the loop var OUT of scope; the loop var is then bound
            // (shadowing) only while lowering the filter + projection, and
            // restored after — mirrors `Quantifier` below.
            Expr::ListComprehension(lc) => {
                if lc.filter.as_deref().is_some_and(expr_contains_aggregate)
                    || lc
                        .projection
                        .as_deref()
                        .is_some_and(expr_contains_aggregate)
                {
                    s.errors.push(BindError::new(
                        BindErrorKind::InvalidArgument,
                        lc.span,
                        "an aggregate function may not be used inside a list \
                         comprehension filter or projection",
                    ));
                }
                let list = self.lower_expr(&lc.list, parent_span, s);
                let has_nested_pattern = lc
                    .filter
                    .as_deref()
                    .is_some_and(expr_contains_pattern_comprehension)
                    || lc
                        .projection
                        .as_deref()
                        .is_some_and(expr_contains_pattern_comprehension);
                let prev = s.vars.get(&lc.var).copied();
                let loop_var = VarId(s.next_var);
                s.next_var += 1;
                s.vars.insert(lc.var.clone(), loop_var);
                let previous_node = s.node_vars.get(&loop_var).cloned();
                let previous_kind = s.var_kinds.get(&loop_var).copied();
                if has_nested_pattern {
                    // The currently supported graph-valued source is `nodes(path)`,
                    // whose elements are whole node structs at execution time.
                    s.node_vars.insert(loop_var, None);
                    s.var_kinds.insert(loop_var, VarKind::Node);
                }
                let previous_capture = if has_nested_pattern {
                    s.captured_pattern_comprehensions.replace(Vec::new())
                } else {
                    None
                };
                let filter = lc
                    .filter
                    .as_ref()
                    .map(|f| self.lower_expr(f, parent_span, s));
                let projection = lc
                    .projection
                    .as_ref()
                    .map(|p| self.lower_expr(p, parent_span, s));
                let captured = if has_nested_pattern {
                    let captured = s.captured_pattern_comprehensions.take().unwrap_or_default();
                    s.captured_pattern_comprehensions = previous_capture;
                    captured
                } else {
                    Vec::new()
                };
                match prev {
                    Some(v) => {
                        s.vars.insert(lc.var.clone(), v);
                    }
                    None => {
                        s.vars.remove(&lc.var);
                    }
                }
                match previous_node {
                    Some(shape) => {
                        s.node_vars.insert(loop_var, shape);
                    }
                    None => {
                        s.node_vars.remove(&loop_var);
                    }
                }
                match previous_kind {
                    Some(kind) => {
                        s.var_kinds.insert(loop_var, kind);
                    }
                    None => {
                        s.var_kinds.remove(&loop_var);
                    }
                }
                if has_nested_pattern {
                    let [(child, pattern_output)] = captured.try_into().unwrap_or_else(|captured: Vec<_>| {
                        s.errors.push(BindError::new(
                            BindErrorKind::InvalidArgument,
                            lc.span,
                            format!(
                                "a list comprehension currently supports exactly one nested pattern comprehension, found {}",
                                captured.len()
                            ),
                        ));
                        [(Box::new(GraphPlan::builder("openCypher").build()), loop_var)]
                    });
                    let output = VarId(s.next_var);
                    s.next_var += 1;
                    s.builder
                        .push_op_mut(GraphOp::ListElementPatternComprehension {
                            list_expr: list,
                            loop_var,
                            child,
                            pattern_output,
                            filter,
                            projection,
                            output,
                        });
                    return s.builder.push_expr(IrExpr::VarRef(output));
                }
                s.builder.push_expr(IrExpr::ListComprehension {
                    loop_var,
                    list,
                    filter,
                    projection,
                })
            }

            // `all/any/none/single(var IN list WHERE pred)` (#955): the list is
            // lowered with the loop var OUT of scope; the loop var is then bound
            // (shadowing) only while lowering the predicate, and restored after.
            Expr::Quantifier(q) => {
                let list = self.lower_expr(&q.list, parent_span, s);
                let prev = s.vars.get(&q.var).copied();
                let loop_var = VarId(s.next_var);
                s.next_var += 1;
                s.vars.insert(q.var.clone(), loop_var);
                let predicate = self.lower_expr(&q.predicate, parent_span, s);
                match prev {
                    Some(v) => {
                        s.vars.insert(q.var.clone(), v);
                    }
                    None => {
                        s.vars.remove(&q.var);
                    }
                }
                s.builder.push_expr(IrExpr::Quantifier {
                    kind: q.kind,
                    loop_var,
                    list,
                    predicate,
                })
            }

            Expr::PatternPredicate(pp) => {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    pp.span,
                    "pattern predicates are invalid outside WHERE",
                ));
                s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
            }

            Expr::ExistentialSubquery(es) => {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    es.span,
                    "existential subqueries are currently valid only as WHERE predicates",
                ));
                s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
            }

            Expr::PatternComprehension(pc) => self.lower_pattern_comprehension(pc, s),

            Expr::LabelPredicate(lp) => self.lower_label_predicate(lp, s),

            // Expr is #[non_exhaustive] — catch future variants gracefully.
            _ => s.builder.push_expr(IrExpr::Literal(IrLiteral::Null)),
        }
    }

    fn lower_label_predicate(&self, lp: &LabelPredicate, s: &mut BinderState) -> ExprId {
        let Some(&var_id) = s.vars.get(&lp.var) else {
            s.errors.push(BindError::new(
                BindErrorKind::UndeclaredVariable,
                lp.span,
                format!("variable `{}` used before it was introduced", lp.var),
            ));
            return s.builder.push_expr(IrExpr::Literal(IrLiteral::Null));
        };

        let kind = s.var_kinds.get(&var_id).copied().or_else(|| {
            if s.node_vars.contains_key(&var_id) {
                Some(VarKind::Node)
            } else if s.edge_rel_names.contains_key(&var_id) {
                Some(VarKind::Relationship)
            } else {
                None
            }
        });

        match kind {
            Some(VarKind::Node) => self.lower_node_label_predicate(var_id, &lp.labels, lp.span, s),
            Some(VarKind::Relationship) => {
                self.lower_relationship_type_predicate(var_id, &lp.labels, lp.span, s)
            }
            Some(VarKind::Unknown) | None => {
                s.errors.push(BindError::new(
                    BindErrorKind::InvalidArgument,
                    lp.span,
                    format!(
                        "`{}:{}` requires a node or relationship variable",
                        lp.var,
                        lp.labels.join(":")
                    ),
                ));
                s.builder.push_expr(IrExpr::Literal(IrLiteral::Null))
            }
        }
    }

    fn lower_node_label_predicate(
        &self,
        var_id: VarId,
        labels: &[String],
        span: Span,
        s: &mut BinderState,
    ) -> ExprId {
        let var_expr = s.builder.push_expr(IrExpr::VarRef(var_id));
        let node_labels_expr = s.builder.push_expr(IrExpr::FunctionCall {
            name: "labels".into(),
            args: vec![var_expr],
        });
        let mut predicates = Vec::with_capacity(labels.len());
        for label in labels {
            self.resolve_label(label, span, s);
            let expected_label_expr = s
                .builder
                .push_expr(IrExpr::Literal(IrLiteral::Str(label.clone())));
            predicates.push(s.builder.push_expr(IrExpr::BinaryOp {
                op: BinaryOpKind::In,
                left: expected_label_expr,
                right: node_labels_expr,
            }));
        }
        push_conjunction(predicates, s)
    }

    fn lower_relationship_type_predicate(
        &self,
        var_id: VarId,
        types: &[String],
        span: Span,
        s: &mut BinderState,
    ) -> ExprId {
        let rel_expr = if s.edge_rel_names.contains_key(&var_id) {
            Self::relationship_struct_expr(var_id, s)
        } else {
            s.builder.push_expr(IrExpr::VarRef(var_id))
        };
        let type_expr = s.builder.push_expr(IrExpr::FunctionCall {
            name: "type".into(),
            args: vec![rel_expr],
        });
        let mut predicates = Vec::with_capacity(types.len());
        for rel_type in types {
            self.resolve_relation_type(rel_type, span, s);
            let type_lit = s
                .builder
                .push_expr(IrExpr::Literal(IrLiteral::Str(rel_type.clone())));
            predicates.push(s.builder.push_expr(IrExpr::BinaryOp {
                op: BinaryOpKind::Eq,
                left: type_expr,
                right: type_lit,
            }));
        }
        let mut predicates = predicates.into_iter();
        let Some(mut predicate) = predicates.next() else {
            return s.builder.push_expr(IrExpr::Literal(IrLiteral::Bool(false)));
        };
        for next in predicates {
            predicate = s.builder.push_expr(IrExpr::BinaryOp {
                op: BinaryOpKind::Or,
                left: predicate,
                right: next,
            });
        }
        predicate
    }

    /// Rewrite `nodes(p)` / `relationships(p)` / `length(p)` over a named path
    /// variable onto the path's constituent variables (#754).
    ///
    /// Returns `None` when the call is not a path function — including these
    /// names over a non-path argument (`length(r)` on an edge list keeps its
    /// generic lowering) — so the caller falls through unchanged.
    fn lower_path_function(call: &FunctionCall, s: &mut BinderState) -> Option<ExprId> {
        let [fn_name] = call.name.as_slice() else {
            return None;
        };
        let fn_name = fn_name.to_ascii_lowercase();
        if !matches!(fn_name.as_str(), "nodes" | "relationships" | "length") {
            return None;
        }
        let [Expr::Var(VarRef { name, .. })] = call.args.as_slice() else {
            return None;
        };
        let binding = s.path_vars.get(name)?.clone();
        let id = match fn_name.as_str() {
            "nodes" => Self::path_nodes_expr(&binding, s),
            "relationships" => Self::path_rels_expr(&binding, s),
            "length" => Self::path_length_expr(&binding, s),
            _ => unreachable!("guarded by the name match above"),
        };
        Some(id)
    }

    fn invalid_direct_graph_function_argument(call: &FunctionCall, s: &BinderState) -> bool {
        let [name] = call.name.as_slice() else {
            return false;
        };
        let [Expr::Var(VarRef { name: var_name, .. })] = call.args.as_slice() else {
            return false;
        };
        let function = name.to_ascii_lowercase();
        if s.path_vars.contains_key(var_name) {
            return matches!(function.as_str(), "labels" | "type");
        }
        let Some(var) = s.vars.get(var_name) else {
            return false;
        };
        let is_node = s.node_vars.contains_key(var);
        let is_relationship = s.edge_rel_names.contains_key(var);
        matches!(function.as_str(), "type" | "length") && is_node
            || matches!(function.as_str(), "labels" | "length") && is_relationship
    }

    fn is_size_of_path_variable(call: &FunctionCall, s: &BinderState) -> bool {
        let [fn_name] = call.name.as_slice() else {
            return false;
        };
        if !fn_name.eq_ignore_ascii_case("size") {
            return false;
        }
        let [Expr::Var(VarRef { name, .. })] = call.args.as_slice() else {
            return false;
        };
        s.path_vars.contains_key(name)
    }

    /// Resolve `startNode(r)` / `endNode(r)` over a fixed-hop matched
    /// relationship to the relationship's src / dst node variable (#753).
    ///
    /// Returns `None` for anything else — a non-matching name, a non-variable
    /// argument, an unbound name, or an edge var that is not a recorded
    /// fixed-hop endpoint (variable-length edges bind to a list, not one
    /// relationship) — so the caller falls through to generic lowering, where
    /// the function name surfaces as the usual `UnknownFunction` error.
    fn resolve_endpoint_node(call: &FunctionCall, s: &BinderState) -> Option<VarId> {
        let [fn_name] = call.name.as_slice() else {
            return None;
        };
        let start = match fn_name.to_ascii_lowercase().as_str() {
            "startnode" => true,
            "endnode" => false,
            _ => return None,
        };
        let [Expr::Var(VarRef { name, .. })] = call.args.as_slice() else {
            return None;
        };
        let edge_var = s.vars.get(name)?;
        let (src, dst) = s.edge_vars.get(edge_var)?;
        Some(if start { *src } else { *dst })
    }

    /// Build a `_node_struct` call that materializes the given node var as a
    /// whole node value — identity + labels + properties (#785). Shared by a
    /// bare `RETURN n` and by `RETURN startNode(r)` / `endNode(r)` (#753).
    fn node_struct_expr(var: VarId, s: &mut BinderState) -> ExprId {
        // The pattern's label name, passed through because the lowerer's
        // ontology map is empty in exploratory mode; absent for an unlabelled
        // match (one arg).
        let label_opt = s.node_vars.get(&var).cloned().flatten();
        let var_ref = s.builder.push_expr(IrExpr::VarRef(var));
        let mut args = vec![var_ref];
        if let Some(label) = label_opt {
            let lit = s.builder.push_expr(IrExpr::Literal(IrLiteral::Str(label)));
            args.push(lit);
        }
        s.builder.push_expr(IrExpr::FunctionCall {
            name: "_node_struct".to_string(),
            args,
        })
    }

    fn relationship_struct_expr(var: VarId, s: &mut BinderState) -> ExprId {
        let rel_name = s.edge_rel_names.get(&var).cloned().flatten();
        let edge = s.builder.push_expr(IrExpr::VarRef(var));
        let rel_name = s.builder.push_expr(IrExpr::Literal(match rel_name {
            Some(name) => IrLiteral::Str(name),
            None => IrLiteral::Null,
        }));
        s.builder.push_expr(IrExpr::FunctionCall {
            name: "_rel_struct".into(),
            args: vec![edge, rel_name],
        })
    }

    fn relationship_struct_list_expr(var: VarId, s: &mut BinderState) -> ExprId {
        let edge = s.builder.push_expr(IrExpr::VarRef(var));
        let rel_name = s.builder.push_expr(IrExpr::Literal(
            match s.edge_rel_names.get(&var).cloned().flatten() {
                Some(name) => IrLiteral::Str(name),
                None => IrLiteral::Null,
            },
        ));
        s.builder.push_expr(IrExpr::FunctionCall {
            name: "_rel_struct_list".into(),
            args: vec![edge, rel_name],
        })
    }

    /// IR for `nodes(p)`: the traversal node sequence as a
    /// `List<Struct{node_uuid}>` value.
    ///
    /// Variable-length: recovered at runtime by walking the edge list from the
    /// start node (`_path_nodes`, gf-rel). Fixed single hop: the two endpoint
    /// node vars directly (`_node_struct_list`) — traversal order comes from
    /// the binder's pattern walk, so `(a)<-[r]-(b)` yields `[a, b]` regardless
    /// of storage orientation.
    fn path_nodes_expr(binding: &PathBinding, s: &mut BinderState) -> ExprId {
        if binding.segments.is_empty() {
            let node = Self::node_struct_expr(binding.nodes[0], s);
            return s.builder.push_expr(IrExpr::ListLiteral(vec![node]));
        }
        if binding.segments.len() > 1 {
            let mut combined = None;
            for (index, segment) in binding.segments.iter().enumerate() {
                let part = PathBinding {
                    nodes: binding.nodes[index..=index + 1].to_vec(),
                    segments: vec![segment.clone()],
                };
                let mut nodes = Self::path_nodes_expr(&part, s);
                if index > 0 {
                    nodes = s.builder.push_expr(IrExpr::FunctionCall {
                        name: "tail".into(),
                        args: vec![nodes],
                    });
                }
                combined = Some(match combined {
                    None => nodes,
                    Some(left) => s.builder.push_expr(IrExpr::BinaryOp {
                        op: BinaryOpKind::Add,
                        left,
                        right: nodes,
                    }),
                });
            }
            return combined.expect("multi-segment path has node parts");
        }
        let seg = &binding.segments[0];
        if seg.var_len {
            let start = s.builder.push_expr(IrExpr::VarRef(binding.nodes[0]));
            let rels = s.builder.push_expr(IrExpr::VarRef(seg.edge));
            s.builder.push_expr(IrExpr::FunctionCall {
                name: "_path_nodes".into(),
                args: vec![start, rels],
            })
        } else {
            // The trailing edge VarRef is the null gate: an unmatched
            // OPTIONAL MATCH row's path is Cypher null, and gf-rel keys that
            // on the edge's `edge_uuid` being null.
            let a = s.builder.push_expr(IrExpr::VarRef(binding.nodes[0]));
            let b = s.builder.push_expr(IrExpr::VarRef(binding.nodes[1]));
            let edge = s.builder.push_expr(IrExpr::VarRef(seg.edge));
            s.builder.push_expr(IrExpr::FunctionCall {
                name: "_node_struct_list".into(),
                args: vec![a, b, edge],
            })
        }
    }

    /// IR for `relationships(p)`: the relationship sequence.
    ///
    /// Variable-length: the #709 relationship-list column verbatim. Fixed
    /// single hop: a one-element list built from the edge var's scalar columns
    /// (`_rel_struct_list`), with the bind-time relation name as `rel_type` —
    /// topology fields only, no edge properties (a documented gap vs the
    /// var-length list; see #754 follow-ups).
    fn path_rels_expr(binding: &PathBinding, s: &mut BinderState) -> ExprId {
        if binding.segments.is_empty() {
            return s.builder.push_expr(IrExpr::ListLiteral(Vec::new()));
        }
        if binding.segments.len() > 1 {
            let mut combined = None;
            for (index, segment) in binding.segments.iter().enumerate() {
                let part = PathBinding {
                    nodes: binding.nodes[index..=index + 1].to_vec(),
                    segments: vec![segment.clone()],
                };
                let rels = Self::path_rels_expr(&part, s);
                combined = Some(match combined {
                    None => rels,
                    Some(left) => s.builder.push_expr(IrExpr::BinaryOp {
                        op: BinaryOpKind::Add,
                        left,
                        right: rels,
                    }),
                });
            }
            return combined.expect("multi-segment path has relationship parts");
        }
        let seg = &binding.segments[0];
        if seg.var_len {
            s.builder.push_expr(IrExpr::VarRef(seg.edge))
        } else {
            let edge = s.builder.push_expr(IrExpr::VarRef(seg.edge));
            let rel_name = s.builder.push_expr(IrExpr::Literal(match &seg.rel_name {
                Some(n) => IrLiteral::Str(n.clone()),
                None => IrLiteral::Null,
            }));
            s.builder.push_expr(IrExpr::FunctionCall {
                name: "_rel_struct_list".into(),
                args: vec![edge, rel_name],
            })
        }
    }

    /// IR for `length(p)`: the relationship count (openCypher path length).
    ///
    /// Variable-length: the edge list's element count (0-hop → 0). Fixed
    /// single hop: the constant 1 when the hop matched, null otherwise
    /// (`_path_fixed_length` over the edge VarRef gate; UInt64 so both forms
    /// agree on the output type).
    fn path_length_expr(binding: &PathBinding, s: &mut BinderState) -> ExprId {
        if binding.segments.is_empty() {
            return s.builder.push_expr(IrExpr::Literal(IrLiteral::Int(0)));
        }
        if binding.segments.len() > 1 {
            let mut total = None;
            for (index, segment) in binding.segments.iter().enumerate() {
                let part = PathBinding {
                    nodes: binding.nodes[index..=index + 1].to_vec(),
                    segments: vec![segment.clone()],
                };
                let length = Self::path_length_expr(&part, s);
                total = Some(match total {
                    None => length,
                    Some(left) => s.builder.push_expr(IrExpr::BinaryOp {
                        op: BinaryOpKind::Add,
                        left,
                        right: length,
                    }),
                });
            }
            return total.expect("multi-segment path has lengths");
        }
        let seg = &binding.segments[0];
        let rels = s.builder.push_expr(IrExpr::VarRef(seg.edge));
        if seg.var_len {
            s.builder.push_expr(IrExpr::FunctionCall {
                name: "length".into(),
                args: vec![rels],
            })
        } else {
            s.builder.push_expr(IrExpr::FunctionCall {
                name: "_path_fixed_length".into(),
                args: vec![rels],
            })
        }
    }

    /// IR for a bare path value (`RETURN p`): a
    /// `Struct{nodes, relationships}` assembled from the same expressions the
    /// path functions use (`_path_struct` → `named_struct` in gf-rel).
    fn path_struct_expr(binding: &PathBinding, s: &mut BinderState) -> ExprId {
        let nodes = Self::path_nodes_expr(binding, s);
        let rels = Self::path_rels_expr(binding, s);
        s.builder.push_expr(IrExpr::FunctionCall {
            name: "_path_struct".into(),
            args: vec![nodes, rels],
        })
    }

    // -----------------------------------------------------------------------
    // Type resolution
    // -----------------------------------------------------------------------

    fn resolve_label(&self, name: &str, span: Span, s: &mut BinderState) -> TypeId {
        if let Some(handle) = &self.ontology
            && let Some(id) = handle.entity_type_id(name)
        {
            return id;
        }
        match self.mode {
            OntologyMode::Strict => {
                s.errors.push(BindError::new(
                    BindErrorKind::UnknownLabel,
                    span,
                    format!("unknown label `{name}` (strict mode)"),
                ));
                TypeId(u32::MAX)
            }
            OntologyMode::Advisory => {
                s.warnings.push(BindError::new(
                    BindErrorKind::UnknownLabel,
                    span,
                    format!("unknown label `{name}` — using runtime catalog"),
                ));
                TypeId(self.catalog.lock().unwrap().intern_label(name).0)
            }
            OntologyMode::Exploratory => TypeId(self.catalog.lock().unwrap().intern_label(name).0),
        }
    }

    fn resolve_relation_type(&self, name: &str, span: Span, s: &mut BinderState) -> TypeId {
        if let Some(handle) = &self.ontology
            && let Some(id) = handle.relation_type_id(name)
        {
            return id;
        }
        match self.mode {
            OntologyMode::Strict => {
                s.errors.push(BindError::new(
                    BindErrorKind::UnknownRelationType,
                    span,
                    format!("unknown relation type `{name}` (strict mode)"),
                ));
                TypeId(u32::MAX)
            }
            OntologyMode::Advisory => {
                s.warnings.push(BindError::new(
                    BindErrorKind::UnknownRelationType,
                    span,
                    format!("unknown relation type `{name}` — using runtime catalog"),
                ));
                crate::runtime_relation_type_id(
                    self.catalog.lock().unwrap().intern_relation_type(name),
                )
            }
            OntologyMode::Exploratory => crate::runtime_relation_type_id(
                self.catalog.lock().unwrap().intern_relation_type(name),
            ),
        }
    }

    fn resolve_property(
        &self,
        name: &str,
        span: Span,
        owner: BoundPropertyOwner,
        s: &mut BinderState,
    ) -> PropId {
        // Durable graph identity columns are structural fields supplied by the
        // engine, not user-defined ontology properties. They remain readable
        // in strict mode so internal and public graph projections can address
        // persisted entities without weakening ontology validation.
        if matches!(name, "node_uuid" | "edge_uuid") {
            return PropId(self.catalog.lock().unwrap().intern_property(name, None).0);
        }
        match self.mode {
            OntologyMode::Strict => self.resolve_strict_property(name, span, owner, s),
            OntologyMode::Advisory => {
                s.warnings.push(BindError::new(
                    BindErrorKind::UnknownProperty,
                    span,
                    format!("unknown property `{name}` — using runtime catalog"),
                ));
                PropId(self.catalog.lock().unwrap().intern_property(name, None).0)
            }
            OntologyMode::Exploratory => {
                PropId(self.catalog.lock().unwrap().intern_property(name, None).0)
            }
        }
    }

    fn resolve_strict_property(
        &self,
        name: &str,
        span: Span,
        owner: BoundPropertyOwner,
        s: &mut BinderState,
    ) -> PropId {
        if owner == BoundPropertyOwner::Value {
            return PropId(self.catalog.lock().unwrap().intern_property(name, None).0);
        }
        let Some(handle) = &self.ontology else {
            s.errors.push(BindError::new(
                BindErrorKind::UnknownProperty,
                span,
                format!("unknown property `{name}` (strict mode has no ontology)"),
            ));
            return PropId(u32::MAX);
        };

        let (declarations, description, runtime_owner, entity_owner) = match owner {
            BoundPropertyOwner::Entity(Some(owner)) => (
                handle
                    .entity_type_id(&owner)
                    .map(|id| handle.entity_property_declarations(id, name))
                    .unwrap_or_default(),
                format!("entity `{owner}`"),
                Some(owner),
                true,
            ),
            BoundPropertyOwner::Entity(None) => (
                handle.all_entity_property_declarations(name),
                "unlabeled entity".to_owned(),
                None,
                true,
            ),
            BoundPropertyOwner::Relationship(Some(owner)) => (
                handle
                    .relation_type_id(&owner)
                    .map(|id| handle.relation_property_declarations(id, name))
                    .unwrap_or_default(),
                format!("relationship `{owner}`"),
                Some(owner),
                false,
            ),
            BoundPropertyOwner::Relationship(None) => (
                handle.all_relation_property_declarations(name),
                "untyped relationship".to_owned(),
                None,
                false,
            ),
            BoundPropertyOwner::Value => unreachable!("value properties returned above"),
        };

        if declarations.len() == 1 {
            return PropId(
                self.catalog
                    .lock()
                    .unwrap()
                    .intern_property(name, runtime_owner.as_deref())
                    .0,
            );
        }
        let (kind, message) = if declarations.is_empty() {
            (
                BindErrorKind::UnknownProperty,
                format!("property `{name}` is not declared for {description} (strict mode)"),
            )
        } else {
            let owners = declarations
                .iter()
                .filter_map(|(id, _)| {
                    if entity_owner {
                        handle.entity_type_name(*id)
                    } else {
                        handle.relation_type_name(*id)
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            (
                BindErrorKind::AmbiguousProperty,
                format!("property `{name}` is ambiguous for {description}; declarations: {owners}"),
            )
        };
        s.errors.push(BindError::new(kind, span, message));
        PropId(u32::MAX)
    }

    // -----------------------------------------------------------------------
    // Variable management
    // -----------------------------------------------------------------------
}

/// Classify a return expression as a top-level aggregate function call.
///
/// Returns the [`AggFunc`] when `expr` is a bare `count(...)`/`sum(...)`/…
/// call (the openCypher aggregating functions). `DISTINCT` variants are refined
/// by the caller using the call's `distinct` flag. Names are matched
/// case-insensitively and unqualified.
fn agg_func_of(expr: &Expr) -> Option<AggFunc> {
    let Expr::FunctionCall(call) = expr else {
        return None;
    };
    // Only bare (unqualified) names are aggregates here.
    let [name] = call.name.as_slice() else {
        return None;
    };
    match name.to_ascii_lowercase().as_str() {
        "count" => Some(AggFunc::Count),
        "sum" => Some(AggFunc::Sum),
        "avg" => Some(AggFunc::Avg),
        "min" => Some(AggFunc::Min),
        "max" => Some(AggFunc::Max),
        "collect" => Some(AggFunc::Collect),
        "percentiledisc" => Some(AggFunc::PercentileDisc),
        "percentilecont" => Some(AggFunc::PercentileCont),
        _ => None,
    }
}

fn is_function_named(call: &FunctionCall, name: &str) -> bool {
    matches!(call.name.as_slice(), [n] if n.eq_ignore_ascii_case(name))
}

#[allow(
    clippy::too_many_lines,
    reason = "flat compile-time registry mirrors the scalar-function dispatch table"
)]
fn is_known_cypher_function(call: &FunctionCall) -> bool {
    let name = call.name.join(".").to_ascii_lowercase();
    matches!(
        name.as_str(),
        "abs"
            | "allshortestpaths"
            | "avg"
            | "ceil"
            | "char_length"
            | "character_length"
            | "coalesce"
            | "collect"
            | "concat"
            | "count"
            | "date"
            | "datetime"
            | "datetime.fromepoch"
            | "datetime.fromepochmillis"
            | "date.truncate"
            | "datetime.truncate"
            | "duration"
            | "duration.between"
            | "duration.indays"
            | "duration.inmonths"
            | "duration.inseconds"
            | "elementid"
            | "endnode"
            | "exp"
            | "exists"
            | "extract"
            | "filter"
            | "floor"
            | "head"
            | "id"
            | "keys"
            | "labels"
            | "last"
            | "length"
            | "localdatetime"
            | "localdatetime.truncate"
            | "localtime"
            | "localtime.truncate"
            | "log"
            | "lower"
            | "ltrim"
            | "max"
            | "min"
            | "nodes"
            | "percentilecont"
            | "percentiledisc"
            | "point"
            | "power"
            | "properties"
            | "rand"
            | "range"
            | "reduce"
            | "relationships"
            | "replace"
            | "reverse"
            | "round"
            | "rtrim"
            | "shortestpath"
            | "sign"
            | "size"
            | "split"
            | "sqrt"
            | "startnode"
            | "string.concat"
            | "substring"
            | "sum"
            | "tail"
            | "time"
            | "time.truncate"
            | "toboolean"
            | "tofloat"
            | "tointeger"
            | "tolower"
            | "tostring"
            | "toupper"
            | "trim"
            | "type"
            | "upper"
            | "timestamp"
            | "_slice"
            | "_slice_from_start"
            | "_slice_to_end"
            | "_subscript"
    ) || matches!(
        name.as_str(),
        "date.transaction"
            | "date.statement"
            | "date.realtime"
            | "datetime.transaction"
            | "datetime.statement"
            | "datetime.realtime"
            | "localdatetime.transaction"
            | "localdatetime.statement"
            | "localdatetime.realtime"
            | "localtime.transaction"
            | "localtime.statement"
            | "localtime.realtime"
            | "time.transaction"
            | "time.statement"
            | "time.realtime"
    )
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    if agg_func_of(expr).is_some() {
        return true;
    }
    match expr {
        Expr::BinaryOp(b) => expr_contains_aggregate(&b.left) || expr_contains_aggregate(&b.right),
        Expr::UnaryOp(u) => expr_contains_aggregate(&u.expr),
        Expr::Parenthesized { inner, .. } => expr_contains_aggregate(inner),
        Expr::FunctionCall(c) => c.args.iter().any(expr_contains_aggregate),
        Expr::Property(p) => expr_contains_aggregate(&p.object),
        Expr::List(l) => l.elements.iter().any(expr_contains_aggregate),
        Expr::Map(m) => m.entries.values().any(expr_contains_aggregate),
        Expr::Case(c) => {
            c.subject.as_deref().is_some_and(expr_contains_aggregate)
                || c.when_clauses.iter().any(|when| {
                    expr_contains_aggregate(&when.condition)
                        || expr_contains_aggregate(&when.result)
                })
                || c.else_expr.as_deref().is_some_and(expr_contains_aggregate)
        }
        Expr::ListComprehension(lc) => {
            expr_contains_aggregate(&lc.list)
                || lc.filter.as_deref().is_some_and(expr_contains_aggregate)
                || lc
                    .projection
                    .as_deref()
                    .is_some_and(expr_contains_aggregate)
        }
        Expr::Quantifier(q) => {
            expr_contains_aggregate(&q.list) || expr_contains_aggregate(&q.predicate)
        }
        Expr::PatternComprehension(pc) => {
            pc.filter.as_deref().is_some_and(expr_contains_aggregate)
                || expr_contains_aggregate(&pc.projection)
        }
        Expr::ExistentialSubquery(es) => match &es.body {
            ExistentialSubqueryBody::Simple { filter, .. } => {
                filter.as_deref().is_some_and(expr_contains_aggregate)
            }
            ExistentialSubqueryBody::Full(_) => false,
        },
        Expr::IsNull { expr, .. } => expr_contains_aggregate(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate(expr) || expr_contains_aggregate(list)
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            expr_contains_aggregate(expr) || expr_contains_aggregate(pattern)
        }
        _ => false,
    }
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

fn collect_aggregate_exprs(expr: &Expr, out: &mut Vec<Expr>) {
    if agg_func_of(expr).is_some() {
        if !out.iter().any(|existing| same_expr_shape(existing, expr)) {
            out.push(expr.clone());
        }
        return;
    }
    match expr {
        Expr::BinaryOp(binary) => {
            collect_aggregate_exprs(&binary.left, out);
            collect_aggregate_exprs(&binary.right, out);
        }
        Expr::UnaryOp(unary) => collect_aggregate_exprs(&unary.expr, out),
        Expr::Parenthesized { inner, .. } => collect_aggregate_exprs(inner, out),
        Expr::FunctionCall(call) => {
            for arg in &call.args {
                collect_aggregate_exprs(arg, out);
            }
        }
        Expr::Property(property) => collect_aggregate_exprs(&property.object, out),
        Expr::List(list) => {
            for element in &list.elements {
                collect_aggregate_exprs(element, out);
            }
        }
        Expr::Map(map) => {
            for value in map.entries.values() {
                collect_aggregate_exprs(value, out);
            }
        }
        Expr::Case(case) => {
            if let Some(subject) = &case.subject {
                collect_aggregate_exprs(subject, out);
            }
            for when in &case.when_clauses {
                collect_aggregate_exprs(&when.condition, out);
                collect_aggregate_exprs(&when.result, out);
            }
            if let Some(else_expr) = &case.else_expr {
                collect_aggregate_exprs(else_expr, out);
            }
        }
        Expr::IsNull { expr, .. } => collect_aggregate_exprs(expr, out),
        Expr::InList { expr, list, .. } => {
            collect_aggregate_exprs(expr, out);
            collect_aggregate_exprs(list, out);
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            collect_aggregate_exprs(expr, out);
            collect_aggregate_exprs(pattern, out);
        }
        _ => {}
    }
}

fn has_unprojected_order_aggregate(projections: &[ReturnItem], order_by: &[SortItem]) -> bool {
    let mut projected = Vec::new();
    for item in projections {
        collect_aggregate_exprs(&item.expr, &mut projected);
    }
    let mut ordered = Vec::new();
    for item in order_by {
        collect_aggregate_exprs(&item.expr, &mut ordered);
    }
    ordered
        .iter()
        .any(|order_agg| !projected.iter().any(|agg| same_expr_shape(agg, order_agg)))
}

fn expr_contains_aggregate_inside_aggregate(expr: &Expr, inside_aggregate: bool) -> bool {
    let is_aggregate = agg_func_of(expr).is_some();
    if is_aggregate && inside_aggregate {
        return true;
    }
    let inside_aggregate = inside_aggregate || is_aggregate;
    match expr {
        Expr::BinaryOp(b) => {
            expr_contains_aggregate_inside_aggregate(&b.left, inside_aggregate)
                || expr_contains_aggregate_inside_aggregate(&b.right, inside_aggregate)
        }
        Expr::UnaryOp(u) => expr_contains_aggregate_inside_aggregate(&u.expr, inside_aggregate),
        Expr::Parenthesized { inner, .. } => {
            expr_contains_aggregate_inside_aggregate(inner, inside_aggregate)
        }
        Expr::FunctionCall(c) => c
            .args
            .iter()
            .any(|arg| expr_contains_aggregate_inside_aggregate(arg, inside_aggregate)),
        Expr::Property(p) => expr_contains_aggregate_inside_aggregate(&p.object, inside_aggregate),
        Expr::List(l) => l
            .elements
            .iter()
            .any(|element| expr_contains_aggregate_inside_aggregate(element, inside_aggregate)),
        Expr::Map(m) => m
            .entries
            .values()
            .any(|value| expr_contains_aggregate_inside_aggregate(value, inside_aggregate)),
        Expr::Case(c) => {
            c.subject.as_deref().is_some_and(|subject| {
                expr_contains_aggregate_inside_aggregate(subject, inside_aggregate)
            }) || c.when_clauses.iter().any(|when| {
                expr_contains_aggregate_inside_aggregate(&when.condition, inside_aggregate)
                    || expr_contains_aggregate_inside_aggregate(&when.result, inside_aggregate)
            }) || c.else_expr.as_deref().is_some_and(|else_expr| {
                expr_contains_aggregate_inside_aggregate(else_expr, inside_aggregate)
            })
        }
        Expr::ListComprehension(lc) => {
            expr_contains_aggregate_inside_aggregate(&lc.list, inside_aggregate)
                || lc.filter.as_deref().is_some_and(|filter| {
                    expr_contains_aggregate_inside_aggregate(filter, inside_aggregate)
                })
                || lc.projection.as_deref().is_some_and(|projection| {
                    expr_contains_aggregate_inside_aggregate(projection, inside_aggregate)
                })
        }
        Expr::Quantifier(q) => {
            expr_contains_aggregate_inside_aggregate(&q.list, inside_aggregate)
                || expr_contains_aggregate_inside_aggregate(&q.predicate, inside_aggregate)
        }
        Expr::PatternComprehension(pc) => {
            pc.filter.as_deref().is_some_and(|filter| {
                expr_contains_aggregate_inside_aggregate(filter, inside_aggregate)
            }) || expr_contains_aggregate_inside_aggregate(&pc.projection, inside_aggregate)
        }
        Expr::ExistentialSubquery(es) => match &es.body {
            ExistentialSubqueryBody::Simple { filter, .. } => {
                filter.as_deref().is_some_and(|filter| {
                    expr_contains_aggregate_inside_aggregate(filter, inside_aggregate)
                })
            }
            ExistentialSubqueryBody::Full(_) => false,
        },
        Expr::IsNull { expr, .. } => {
            expr_contains_aggregate_inside_aggregate(expr, inside_aggregate)
        }
        Expr::InList { expr, list, .. } => {
            expr_contains_aggregate_inside_aggregate(expr, inside_aggregate)
                || expr_contains_aggregate_inside_aggregate(list, inside_aggregate)
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            expr_contains_aggregate_inside_aggregate(expr, inside_aggregate)
                || expr_contains_aggregate_inside_aggregate(pattern, inside_aggregate)
        }
        _ => false,
    }
}

fn expr_contains_volatile_function(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall(call) => {
            is_function_named(call, "rand") || call.args.iter().any(expr_contains_volatile_function)
        }
        Expr::BinaryOp(binary) => {
            expr_contains_volatile_function(&binary.left)
                || expr_contains_volatile_function(&binary.right)
        }
        Expr::UnaryOp(unary) => expr_contains_volatile_function(&unary.expr),
        Expr::Parenthesized { inner, .. } => expr_contains_volatile_function(inner),
        Expr::Property(property) => expr_contains_volatile_function(&property.object),
        Expr::List(list) => list.elements.iter().any(expr_contains_volatile_function),
        Expr::Map(map) => map.entries.values().any(expr_contains_volatile_function),
        Expr::Case(case) => {
            case.subject
                .as_deref()
                .is_some_and(expr_contains_volatile_function)
                || case.when_clauses.iter().any(|when| {
                    expr_contains_volatile_function(&when.condition)
                        || expr_contains_volatile_function(&when.result)
                })
                || case
                    .else_expr
                    .as_deref()
                    .is_some_and(expr_contains_volatile_function)
        }
        Expr::IsNull { expr, .. } => expr_contains_volatile_function(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_volatile_function(expr) || expr_contains_volatile_function(list)
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            expr_contains_volatile_function(expr) || expr_contains_volatile_function(pattern)
        }
        _ => false,
    }
}

fn is_atomic_grouping_expr(expr: &Expr) -> bool {
    matches!(strip_parens(expr), Expr::Var(_) | Expr::Property(_))
}

fn grouping_ref_root_name(expr: &Expr) -> Option<&str> {
    match strip_parens(expr) {
        Expr::Var(var) => Some(var.name.as_str()),
        Expr::Property(property) => grouping_ref_root_name(&property.object),
        _ => None,
    }
}

fn same_grouping_expr(left: &Expr, right: &Expr) -> bool {
    match (strip_parens(left), strip_parens(right)) {
        (Expr::Var(a), Expr::Var(b)) => a.name == b.name,
        (Expr::Property(a), Expr::Property(b)) => {
            a.key == b.key && same_grouping_expr(&a.object, &b.object)
        }
        (Expr::FunctionCall(a), Expr::FunctionCall(b)) => {
            a.name
                .iter()
                .map(|part| part.to_ascii_lowercase())
                .eq(b.name.iter().map(|part| part.to_ascii_lowercase()))
                && a.distinct == b.distinct
                && a.star == b.star
                && a.args.len() == b.args.len()
                && a.args
                    .iter()
                    .zip(&b.args)
                    .all(|(left, right)| same_expr_shape(left, right))
        }
        (Expr::List(a), Expr::List(b)) => {
            a.elements.len() == b.elements.len()
                && a.elements
                    .iter()
                    .zip(&b.elements)
                    .all(|(left, right)| same_expr_shape(left, right))
        }
        (Expr::Map(a), Expr::Map(b)) => {
            a.entries.len() == b.entries.len()
                && a.entries.iter().all(|(key, left)| {
                    b.entries
                        .get(key)
                        .is_some_and(|right| same_expr_shape(left, right))
                })
        }
        _ => false,
    }
}

fn same_expr_shape(left: &Expr, right: &Expr) -> bool {
    match (strip_parens(left), strip_parens(right)) {
        (Expr::Literal(Literal::Int(a, _)), Expr::Literal(Literal::Int(b, _))) => a == b,
        (Expr::Literal(Literal::Float(a, _)), Expr::Literal(Literal::Float(b, _))) => {
            a.to_bits() == b.to_bits()
        }
        (Expr::Literal(Literal::Str(a, _)), Expr::Literal(Literal::Str(b, _))) => a == b,
        (Expr::Literal(Literal::Bool(a, _)), Expr::Literal(Literal::Bool(b, _))) => a == b,
        (Expr::Literal(Literal::Null(_)), Expr::Literal(Literal::Null(_))) => true,
        (Expr::Param(a), Expr::Param(b)) => a.name == b.name,
        (Expr::Var(a), Expr::Var(b)) => a.name == b.name,
        (Expr::Property(a), Expr::Property(b)) => {
            a.key == b.key && same_expr_shape(&a.object, &b.object)
        }
        (Expr::BinaryOp(a), Expr::BinaryOp(b)) => {
            a.op == b.op && same_expr_shape(&a.left, &b.left) && same_expr_shape(&a.right, &b.right)
        }
        (Expr::UnaryOp(a), Expr::UnaryOp(b)) => a.op == b.op && same_expr_shape(&a.expr, &b.expr),
        (Expr::FunctionCall(a), Expr::FunctionCall(b)) => {
            a.name
                .iter()
                .map(|part| part.to_ascii_lowercase())
                .eq(b.name.iter().map(|part| part.to_ascii_lowercase()))
                && a.distinct == b.distinct
                && a.star == b.star
                && a.args.len() == b.args.len()
                && a.args
                    .iter()
                    .zip(&b.args)
                    .all(|(left, right)| same_expr_shape(left, right))
        }
        _ => false,
    }
}

fn rewrite_projection_alias_refs(expr: Expr, projections: &[ReturnItem]) -> Expr {
    rewrite_projection_alias_refs_except(expr, projections, &[])
}

#[allow(clippy::too_many_lines)]
fn rewrite_projection_alias_refs_except(
    expr: Expr,
    projections: &[ReturnItem],
    hidden: &[String],
) -> Expr {
    if let Expr::Var(var) = &expr
        && !hidden.contains(&var.name)
        && let Some(projection) = projections
            .iter()
            .find(|projection| projection.alias.as_deref() == Some(var.name.as_str()))
    {
        return projection.expr.clone();
    }
    match expr {
        Expr::BinaryOp(binary) => Expr::BinaryOp(gf_ast::BinaryOp {
            op: binary.op,
            left: Box::new(rewrite_projection_alias_refs_except(
                *binary.left,
                projections,
                hidden,
            )),
            right: Box::new(rewrite_projection_alias_refs_except(
                *binary.right,
                projections,
                hidden,
            )),
            span: binary.span,
        }),
        Expr::UnaryOp(unary) => Expr::UnaryOp(gf_ast::UnaryOp {
            op: unary.op,
            expr: Box::new(rewrite_projection_alias_refs_except(
                *unary.expr,
                projections,
                hidden,
            )),
            span: unary.span,
        }),
        Expr::Parenthesized { inner, span } => Expr::Parenthesized {
            inner: Box::new(rewrite_projection_alias_refs_except(
                *inner,
                projections,
                hidden,
            )),
            span,
        },
        Expr::Property(mut property) => {
            property.object = Box::new(rewrite_projection_alias_refs_except(
                *property.object,
                projections,
                hidden,
            ));
            Expr::Property(property)
        }
        Expr::FunctionCall(mut call) => {
            call.args = call
                .args
                .into_iter()
                .map(|arg| rewrite_projection_alias_refs_except(arg, projections, hidden))
                .collect();
            Expr::FunctionCall(call)
        }
        Expr::List(mut list) => {
            list.elements = list
                .elements
                .into_iter()
                .map(|element| rewrite_projection_alias_refs_except(element, projections, hidden))
                .collect();
            Expr::List(list)
        }
        Expr::Map(mut map) => {
            map.entries = map
                .entries
                .into_iter()
                .map(|(key, value)| {
                    (
                        key,
                        rewrite_projection_alias_refs_except(value, projections, hidden),
                    )
                })
                .collect();
            Expr::Map(map)
        }
        Expr::Case(mut case) => {
            case.subject = case.subject.map(|subject| {
                Box::new(rewrite_projection_alias_refs_except(
                    *subject,
                    projections,
                    hidden,
                ))
            });
            for when in &mut case.when_clauses {
                when.condition = rewrite_projection_alias_refs_except(
                    when.condition.clone(),
                    projections,
                    hidden,
                );
                when.result =
                    rewrite_projection_alias_refs_except(when.result.clone(), projections, hidden);
            }
            case.else_expr = case.else_expr.map(|else_expr| {
                Box::new(rewrite_projection_alias_refs_except(
                    *else_expr,
                    projections,
                    hidden,
                ))
            });
            Expr::Case(case)
        }
        Expr::ListComprehension(mut comprehension) => {
            comprehension.list = Box::new(rewrite_projection_alias_refs_except(
                *comprehension.list,
                projections,
                hidden,
            ));
            let mut body_hidden = hidden.to_vec();
            body_hidden.push(comprehension.var.clone());
            comprehension.filter = comprehension.filter.map(|filter| {
                Box::new(rewrite_projection_alias_refs_except(
                    *filter,
                    projections,
                    &body_hidden,
                ))
            });
            comprehension.projection = comprehension.projection.map(|projection| {
                Box::new(rewrite_projection_alias_refs_except(
                    *projection,
                    projections,
                    &body_hidden,
                ))
            });
            Expr::ListComprehension(comprehension)
        }
        Expr::Quantifier(mut quantifier) => {
            quantifier.list = Box::new(rewrite_projection_alias_refs_except(
                *quantifier.list,
                projections,
                hidden,
            ));
            let mut body_hidden = hidden.to_vec();
            body_hidden.push(quantifier.var.clone());
            quantifier.predicate = Box::new(rewrite_projection_alias_refs_except(
                *quantifier.predicate,
                projections,
                &body_hidden,
            ));
            Expr::Quantifier(quantifier)
        }
        Expr::IsNull {
            expr,
            negated,
            span,
        } => Expr::IsNull {
            expr: Box::new(rewrite_projection_alias_refs_except(
                *expr,
                projections,
                hidden,
            )),
            negated,
            span,
        },
        Expr::InList {
            expr,
            list,
            negated,
            span,
        } => Expr::InList {
            expr: Box::new(rewrite_projection_alias_refs_except(
                *expr,
                projections,
                hidden,
            )),
            list: Box::new(rewrite_projection_alias_refs_except(
                *list,
                projections,
                hidden,
            )),
            negated,
            span,
        },
        Expr::StringOp {
            expr,
            op,
            pattern,
            span,
        } => Expr::StringOp {
            expr: Box::new(rewrite_projection_alias_refs_except(
                *expr,
                projections,
                hidden,
            )),
            op,
            pattern: Box::new(rewrite_projection_alias_refs_except(
                *pattern,
                projections,
                hidden,
            )),
            span,
        },
        Expr::RegexMatch {
            expr,
            pattern,
            span,
        } => Expr::RegexMatch {
            expr: Box::new(rewrite_projection_alias_refs_except(
                *expr,
                projections,
                hidden,
            )),
            pattern: Box::new(rewrite_projection_alias_refs_except(
                *pattern,
                projections,
                hidden,
            )),
            span,
        },
        other => other,
    }
}

fn collect_grouping_refs(expr: &Expr, out: &mut Vec<Expr>) {
    collect_grouping_refs_except(expr, out, &[]);
}

#[allow(clippy::too_many_lines)]
fn collect_grouping_refs_except(expr: &Expr, out: &mut Vec<Expr>, hidden: &[String]) {
    if agg_func_of(expr).is_some() {
        return;
    }
    match expr {
        Expr::Var(_) | Expr::Property(_) if !grouping_ref_is_hidden(expr, hidden) => {
            out.push(expr.clone());
        }
        Expr::BinaryOp(binary) => {
            collect_grouping_refs_except(&binary.left, out, hidden);
            collect_grouping_refs_except(&binary.right, out, hidden);
        }
        Expr::UnaryOp(unary) => collect_grouping_refs_except(&unary.expr, out, hidden),
        Expr::Parenthesized { inner, .. } => collect_grouping_refs_except(inner, out, hidden),
        Expr::FunctionCall(call) => {
            for arg in &call.args {
                collect_grouping_refs_except(arg, out, hidden);
            }
        }
        Expr::List(list) => {
            for element in &list.elements {
                collect_grouping_refs_except(element, out, hidden);
            }
        }
        Expr::Map(map) => {
            for value in map.entries.values() {
                collect_grouping_refs_except(value, out, hidden);
            }
        }
        Expr::Case(case) => {
            if let Some(subject) = &case.subject {
                collect_grouping_refs_except(subject, out, hidden);
            }
            for when in &case.when_clauses {
                collect_grouping_refs_except(&when.condition, out, hidden);
                collect_grouping_refs_except(&when.result, out, hidden);
            }
            if let Some(else_expr) = &case.else_expr {
                collect_grouping_refs_except(else_expr, out, hidden);
            }
        }
        Expr::ListComprehension(lc) => {
            collect_grouping_refs_except(&lc.list, out, hidden);
            let mut body_hidden = hidden.to_vec();
            body_hidden.push(lc.var.clone());
            if let Some(filter) = &lc.filter {
                collect_grouping_refs_except(filter, out, &body_hidden);
            }
            if let Some(projection) = &lc.projection {
                collect_grouping_refs_except(projection, out, &body_hidden);
            }
        }
        Expr::Quantifier(q) => {
            collect_grouping_refs_except(&q.list, out, hidden);
            let mut predicate_hidden = hidden.to_vec();
            predicate_hidden.push(q.var.clone());
            collect_grouping_refs_except(&q.predicate, out, &predicate_hidden);
        }
        Expr::PatternComprehension(pc) => {
            let body_hidden = hidden_with_pattern_vars(hidden, &pc.pattern, pc.var.as_ref());
            if let Some(filter) = &pc.filter {
                collect_grouping_refs_except(filter, out, &body_hidden);
            }
            collect_grouping_refs_except(&pc.projection, out, &body_hidden);
        }
        Expr::ExistentialSubquery(es) => {
            if let ExistentialSubqueryBody::Simple { pattern, filter } = &es.body {
                let body_hidden = hidden_with_pattern_vars(hidden, pattern, None);
                if let Some(filter) = filter {
                    collect_grouping_refs_except(filter, out, &body_hidden);
                }
            }
        }
        Expr::LabelPredicate(label) if !hidden.contains(&label.var) => {
            out.push(Expr::Var(VarRef {
                name: label.var.clone(),
                span: label.span,
            }));
        }
        Expr::IsNull { expr, .. } => collect_grouping_refs_except(expr, out, hidden),
        Expr::InList { expr, list, .. } => {
            collect_grouping_refs_except(expr, out, hidden);
            collect_grouping_refs_except(list, out, hidden);
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            collect_grouping_refs_except(expr, out, hidden);
            collect_grouping_refs_except(pattern, out, hidden);
        }
        _ => {}
    }
}

fn grouping_ref_is_hidden(expr: &Expr, hidden: &[String]) -> bool {
    match strip_parens(expr) {
        Expr::Var(var) => hidden.contains(&var.name),
        Expr::Property(property) => grouping_ref_is_hidden(&property.object, hidden),
        _ => false,
    }
}

fn hidden_with_pattern_vars(
    hidden: &[String],
    pattern: &PathPattern,
    path_var: Option<&String>,
) -> Vec<String> {
    let mut body_hidden = hidden.to_vec();
    body_hidden.extend(pattern.elements.iter().filter_map(|element| match element {
        PathElement::Node(node) => node.var.clone(),
        PathElement::Rel(rel) => rel.var.clone(),
    }));
    body_hidden.extend(path_var.cloned());
    body_hidden
}

#[allow(clippy::too_many_lines)]
fn rewrite_grouping_refs(expr: Expr, bindings: &[(Expr, String, VarId)]) -> Expr {
    rewrite_grouping_refs_except(expr, bindings, &[])
}

#[allow(clippy::too_many_lines)]
fn rewrite_grouping_refs_except(
    expr: Expr,
    bindings: &[(Expr, String, VarId)],
    hidden: &[String],
) -> Expr {
    if !grouping_ref_is_hidden(&expr, hidden)
        && let Some((_, alias, _)) = bindings
            .iter()
            .find(|(group, _, _)| same_grouping_expr(&expr, group))
    {
        return Expr::Var(VarRef {
            name: alias.clone(),
            span: expr.span(),
        });
    }
    match expr {
        Expr::BinaryOp(binary) => Expr::BinaryOp(gf_ast::BinaryOp {
            op: binary.op,
            left: Box::new(rewrite_grouping_refs_except(*binary.left, bindings, hidden)),
            right: Box::new(rewrite_grouping_refs_except(
                *binary.right,
                bindings,
                hidden,
            )),
            span: binary.span,
        }),
        Expr::UnaryOp(unary) => Expr::UnaryOp(gf_ast::UnaryOp {
            op: unary.op,
            expr: Box::new(rewrite_grouping_refs_except(*unary.expr, bindings, hidden)),
            span: unary.span,
        }),
        Expr::Parenthesized { inner, span } => Expr::Parenthesized {
            inner: Box::new(rewrite_grouping_refs_except(*inner, bindings, hidden)),
            span,
        },
        Expr::FunctionCall(call) => Expr::FunctionCall(gf_ast::FunctionCall {
            name: call.name,
            distinct: call.distinct,
            star: call.star,
            args: call
                .args
                .into_iter()
                .map(|arg| rewrite_grouping_refs_except(arg, bindings, hidden))
                .collect(),
            span: call.span,
        }),
        Expr::Property(mut property) => {
            property.object = Box::new(rewrite_grouping_refs_except(
                *property.object,
                bindings,
                hidden,
            ));
            Expr::Property(property)
        }
        Expr::List(list) => Expr::List(gf_ast::ListLiteral {
            elements: list
                .elements
                .into_iter()
                .map(|element| rewrite_grouping_refs_except(element, bindings, hidden))
                .collect(),
            span: list.span,
        }),
        Expr::Map(mut map) => {
            map.entries = map
                .entries
                .into_iter()
                .map(|(key, value)| (key, rewrite_grouping_refs_except(value, bindings, hidden)))
                .collect();
            Expr::Map(map)
        }
        Expr::Case(mut case) => {
            case.subject = case
                .subject
                .map(|subject| Box::new(rewrite_grouping_refs_except(*subject, bindings, hidden)));
            for when in &mut case.when_clauses {
                when.condition =
                    rewrite_grouping_refs_except(when.condition.clone(), bindings, hidden);
                when.result = rewrite_grouping_refs_except(when.result.clone(), bindings, hidden);
            }
            case.else_expr = case.else_expr.map(|else_expr| {
                Box::new(rewrite_grouping_refs_except(*else_expr, bindings, hidden))
            });
            Expr::Case(case)
        }
        Expr::ListComprehension(mut lc) => {
            lc.list = Box::new(rewrite_grouping_refs_except(*lc.list, bindings, hidden));
            let mut body_hidden = hidden.to_vec();
            body_hidden.push(lc.var.clone());
            lc.filter = lc.filter.map(|filter| {
                Box::new(rewrite_grouping_refs_except(
                    *filter,
                    bindings,
                    &body_hidden,
                ))
            });
            lc.projection = lc.projection.map(|projection| {
                Box::new(rewrite_grouping_refs_except(
                    *projection,
                    bindings,
                    &body_hidden,
                ))
            });
            Expr::ListComprehension(lc)
        }
        Expr::Quantifier(mut q) => {
            q.list = Box::new(rewrite_grouping_refs_except(*q.list, bindings, hidden));
            let mut predicate_hidden = hidden.to_vec();
            predicate_hidden.push(q.var.clone());
            q.predicate = Box::new(rewrite_grouping_refs_except(
                *q.predicate,
                bindings,
                &predicate_hidden,
            ));
            Expr::Quantifier(q)
        }
        Expr::PatternComprehension(mut pc) => {
            let body_hidden = hidden_with_pattern_vars(hidden, &pc.pattern, pc.var.as_ref());
            pc.filter = pc.filter.map(|filter| {
                Box::new(rewrite_grouping_refs_except(
                    *filter,
                    bindings,
                    &body_hidden,
                ))
            });
            pc.projection = Box::new(rewrite_grouping_refs_except(
                *pc.projection,
                bindings,
                &body_hidden,
            ));
            Expr::PatternComprehension(pc)
        }
        Expr::ExistentialSubquery(mut es) => {
            if let ExistentialSubqueryBody::Simple { pattern, filter } = &mut es.body {
                let body_hidden = hidden_with_pattern_vars(hidden, pattern, None);
                *filter = filter.take().map(|filter| {
                    Box::new(rewrite_grouping_refs_except(
                        *filter,
                        bindings,
                        &body_hidden,
                    ))
                });
            }
            Expr::ExistentialSubquery(es)
        }
        Expr::LabelPredicate(mut label) => {
            let reference = Expr::Var(VarRef {
                name: label.var.clone(),
                span: label.span,
            });
            if !hidden.contains(&label.var)
                && let Some((_, alias, _)) = bindings
                    .iter()
                    .find(|(group, _, _)| same_grouping_expr(&reference, group))
            {
                label.var.clone_from(alias);
            }
            Expr::LabelPredicate(label)
        }
        Expr::IsNull {
            expr,
            negated,
            span,
        } => Expr::IsNull {
            expr: Box::new(rewrite_grouping_refs_except(*expr, bindings, hidden)),
            negated,
            span,
        },
        Expr::InList {
            expr,
            list,
            negated,
            span,
        } => Expr::InList {
            expr: Box::new(rewrite_grouping_refs_except(*expr, bindings, hidden)),
            list: Box::new(rewrite_grouping_refs_except(*list, bindings, hidden)),
            negated,
            span,
        },
        Expr::StringOp {
            expr,
            op,
            pattern,
            span,
        } => Expr::StringOp {
            expr: Box::new(rewrite_grouping_refs_except(*expr, bindings, hidden)),
            op,
            pattern: Box::new(rewrite_grouping_refs_except(*pattern, bindings, hidden)),
            span,
        },
        Expr::RegexMatch {
            expr,
            pattern,
            span,
        } => Expr::RegexMatch {
            expr: Box::new(rewrite_grouping_refs_except(*expr, bindings, hidden)),
            pattern: Box::new(rewrite_grouping_refs_except(*pattern, bindings, hidden)),
            span,
        },
        other => other,
    }
}

fn expr_contains_pattern_comprehension(expr: &Expr) -> bool {
    match expr {
        Expr::PatternComprehension(_) => true,
        Expr::Parenthesized { inner, .. } => expr_contains_pattern_comprehension(inner),
        Expr::BinaryOp(b) => {
            expr_contains_pattern_comprehension(&b.left)
                || expr_contains_pattern_comprehension(&b.right)
        }
        Expr::UnaryOp(u) => expr_contains_pattern_comprehension(&u.expr),
        Expr::IsNull { expr, .. } => expr_contains_pattern_comprehension(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_pattern_comprehension(expr) || expr_contains_pattern_comprehension(list)
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            expr_contains_pattern_comprehension(expr)
                || expr_contains_pattern_comprehension(pattern)
        }
        Expr::FunctionCall(call) => call.args.iter().any(expr_contains_pattern_comprehension),
        Expr::Property(property) => expr_contains_pattern_comprehension(&property.object),
        Expr::List(list) => list
            .elements
            .iter()
            .any(expr_contains_pattern_comprehension),
        Expr::Map(map) => map
            .entries
            .values()
            .any(expr_contains_pattern_comprehension),
        Expr::Case(case) => {
            case.subject
                .as_deref()
                .is_some_and(expr_contains_pattern_comprehension)
                || case.when_clauses.iter().any(|when| {
                    expr_contains_pattern_comprehension(&when.condition)
                        || expr_contains_pattern_comprehension(&when.result)
                })
                || case
                    .else_expr
                    .as_deref()
                    .is_some_and(expr_contains_pattern_comprehension)
        }
        Expr::ListComprehension(lc) => {
            expr_contains_pattern_comprehension(&lc.list)
                || lc
                    .filter
                    .as_deref()
                    .is_some_and(expr_contains_pattern_comprehension)
                || lc
                    .projection
                    .as_deref()
                    .is_some_and(expr_contains_pattern_comprehension)
        }
        Expr::Quantifier(q) => {
            expr_contains_pattern_comprehension(&q.list)
                || expr_contains_pattern_comprehension(&q.predicate)
        }
        _ => false,
    }
}

fn strip_parens(expr: &Expr) -> &Expr {
    match expr {
        Expr::Parenthesized { inner, .. } => strip_parens(inner),
        other => other,
    }
}

fn matches_pattern_predicate(expr: &Expr) -> bool {
    matches!(strip_parens(expr), Expr::PatternPredicate(_))
}

fn expr_contains_pattern_predicate(expr: &Expr) -> bool {
    match expr {
        Expr::PatternPredicate(_) => true,
        Expr::Parenthesized { inner, .. } => expr_contains_pattern_predicate(inner),
        Expr::BinaryOp(b) => {
            expr_contains_pattern_predicate(&b.left) || expr_contains_pattern_predicate(&b.right)
        }
        Expr::UnaryOp(u) => expr_contains_pattern_predicate(&u.expr),
        Expr::IsNull { expr, .. } => expr_contains_pattern_predicate(expr),
        Expr::InList { expr, list, .. } => {
            expr_contains_pattern_predicate(expr) || expr_contains_pattern_predicate(list)
        }
        Expr::StringOp { expr, pattern, .. } | Expr::RegexMatch { expr, pattern, .. } => {
            expr_contains_pattern_predicate(expr) || expr_contains_pattern_predicate(pattern)
        }
        Expr::FunctionCall(call) => call.args.iter().any(expr_contains_pattern_predicate),
        Expr::List(list) => list.elements.iter().any(expr_contains_pattern_predicate),
        Expr::Map(map) => map.entries.values().any(expr_contains_pattern_predicate),
        Expr::Case(case) => {
            case.subject
                .as_deref()
                .is_some_and(expr_contains_pattern_predicate)
                || case.when_clauses.iter().any(|when| {
                    expr_contains_pattern_predicate(&when.condition)
                        || expr_contains_pattern_predicate(&when.result)
                })
                || case
                    .else_expr
                    .as_deref()
                    .is_some_and(expr_contains_pattern_predicate)
        }
        Expr::ListComprehension(lc) => {
            expr_contains_pattern_predicate(&lc.list)
                || lc
                    .filter
                    .as_deref()
                    .is_some_and(expr_contains_pattern_predicate)
                || lc
                    .projection
                    .as_deref()
                    .is_some_and(expr_contains_pattern_predicate)
        }
        Expr::Quantifier(q) => {
            expr_contains_pattern_predicate(&q.list)
                || expr_contains_pattern_predicate(&q.predicate)
        }
        Expr::PatternComprehension(pc) => {
            pc.filter
                .as_deref()
                .is_some_and(expr_contains_pattern_predicate)
                || expr_contains_pattern_predicate(&pc.projection)
        }
        _ => false,
    }
}

fn relationship_type_alternatives(pattern: &PathPattern) -> Vec<PathPattern> {
    let Some(PathElement::Rel(rel)) = pattern.elements.get(1) else {
        return vec![pattern.clone()];
    };
    if rel.types.len() <= 1 {
        return vec![pattern.clone()];
    }

    rel.types
        .iter()
        .map(|rel_type| {
            let mut alternative = pattern.clone();
            let PathElement::Rel(rel) = &mut alternative.elements[1] else {
                unreachable!("single-relationship pattern shape was checked")
            };
            rel.types = vec![rel_type.clone()];
            alternative
        })
        .collect()
}

fn is_single_relationship_pattern(pattern: &PathPattern) -> bool {
    matches!(
        pattern.elements.as_slice(),
        [
            PathElement::Node(_),
            PathElement::Rel(_),
            PathElement::Node(_)
        ]
    )
}

fn pattern_has_var_length_relationship_properties(pattern: &PathPattern) -> bool {
    pattern.elements.iter().any(|element| {
        let PathElement::Rel(rel) = element else {
            return false;
        };
        (rel.min_hops.is_some() || rel.max_hops.is_some()) && rel.properties.is_some()
    })
}

fn collect_pattern_disjunction<'a>(
    expr: &'a Expr,
    alternatives: &mut Vec<&'a PatternPredicate>,
) -> bool {
    match expr {
        Expr::Parenthesized { inner, .. } => collect_pattern_disjunction(inner, alternatives),
        Expr::PatternPredicate(pp) => {
            alternatives.push(pp);
            true
        }
        Expr::BinaryOp(gf_ast::BinaryOp {
            op: AstBinOp::Or,
            left,
            right,
            ..
        }) => {
            collect_pattern_disjunction(left, alternatives)
                && collect_pattern_disjunction(right, alternatives)
        }
        _ => false,
    }
}

struct MixedPatternBranch<'a> {
    pattern: &'a PatternPredicate,
    scalar_filters: Vec<&'a Expr>,
}

fn collect_mixed_pattern_disjunction<'a>(
    expr: &'a Expr,
    branches: &mut Vec<MixedPatternBranch<'a>>,
) -> bool {
    match expr {
        Expr::Parenthesized { inner, .. } => collect_mixed_pattern_disjunction(inner, branches),
        Expr::BinaryOp(gf_ast::BinaryOp {
            op: AstBinOp::Or,
            left,
            right,
            ..
        }) => {
            collect_mixed_pattern_disjunction(left, branches)
                && collect_mixed_pattern_disjunction(right, branches)
        }
        branch => {
            let mut conjuncts = Vec::new();
            collect_conjuncts(branch, &mut conjuncts);
            let mut pattern = None;
            let mut scalar_filters = Vec::new();
            for conjunct in conjuncts {
                let conjunct = strip_parens(conjunct);
                if let Expr::PatternPredicate(found) = conjunct {
                    if pattern.replace(found).is_some() {
                        return false;
                    }
                } else if expr_contains_pattern_predicate(conjunct) {
                    return false;
                } else {
                    scalar_filters.push(conjunct);
                }
            }
            let Some(pattern) = pattern else {
                return false;
            };
            branches.push(MixedPatternBranch {
                pattern,
                scalar_filters,
            });
            true
        }
    }
}

fn collect_conjuncts<'a>(expr: &'a Expr, conjuncts: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Parenthesized { inner, .. } => collect_conjuncts(inner, conjuncts),
        Expr::BinaryOp(gf_ast::BinaryOp {
            op: AstBinOp::And,
            left,
            right,
            ..
        }) => {
            collect_conjuncts(left, conjuncts);
            collect_conjuncts(right, conjuncts);
        }
        other => conjuncts.push(other),
    }
}

fn plan_references_any_var(plan: &GraphPlan, vars: &HashSet<VarId>) -> bool {
    let expression_reference = (0..plan.exprs.len()).any(|index| {
        let index = u32::try_from(index).expect("ExprArena length is capped at u32::MAX");
        matches!(
            plan.exprs.get(ExprId(index)),
            IrExpr::VarRef(var) if vars.contains(var)
        )
    });
    expression_reference
        || plan.ops.iter().any(|op| match op {
            GraphOp::NodeScan { var, .. }
            | GraphOp::EdgeScan { var, .. }
            | GraphOp::TypedEdgeScan { var, .. } => vars.contains(var),
            GraphOp::Expand { src, edge, dst, .. } => {
                vars.contains(src) || vars.contains(edge) || vars.contains(dst)
            }
            GraphOp::Optional { child }
            | GraphOp::Exists { child, .. }
            | GraphOp::PatternComprehension { child, .. }
            | GraphOp::ListElementPatternComprehension { child, .. } => {
                plan_references_any_var(child, vars)
            }
            GraphOp::Union { inputs, .. } => inputs
                .iter()
                .any(|input| plan_references_any_var(input, vars)),
            _ => false,
        })
}

fn graph_op_bound_vars(op: &GraphOp) -> Vec<VarId> {
    match op {
        GraphOp::NodeScan { var, .. }
        | GraphOp::EdgeScan { var, .. }
        | GraphOp::TypedEdgeScan { var, .. } => vec![*var],
        GraphOp::Expand { src, edge, dst, .. } => vec![*src, *edge, *dst],
        _ => Vec::new(),
    }
}

fn pattern_references_bound_var(pattern: &PathPattern, s: &BinderState) -> bool {
    pattern.elements.iter().any(|element| match element {
        PathElement::Node(node) => node
            .var
            .as_deref()
            .is_some_and(|name| s.vars.contains_key(name)),
        PathElement::Rel(rel) => rel
            .var
            .as_deref()
            .is_some_and(|name| s.vars.contains_key(name)),
    })
}

/// Expand a `RETURN *` wildcard into one item per in-scope NAMED variable
/// (#598). `*` parses as a `Var` named `"*"`; here it becomes the current
/// variables (e.g. `a, b` for `MATCH (a)-->(b) RETURN *`). The TCK compares
/// result columns by header name, so order is immaterial — emit them sorted for
/// a deterministic plan. Non-wildcard items pass through unchanged; a `*` with
/// no in-scope variables expands to nothing (the projection then errors, as
/// Cypher requires at least one).
///
/// Anonymous pattern elements (no user name) are not in scope and so are not
/// returned. Named path bindings live outside `vars`, so include both maps.
fn expand_projection_wildcard(items: &[ReturnItem], s: &BinderState) -> Vec<ReturnItem> {
    let is_star = |e: &Expr| matches!(e, Expr::Var(VarRef { name, .. }) if name == "*");
    if !items.iter().any(|i| is_star(&i.expr)) {
        return items.to_vec();
    }
    let mut names: Vec<String> = s.vars.keys().chain(s.path_vars.keys()).cloned().collect();
    names.sort();
    names.dedup();
    let mut out = Vec::new();
    for item in items {
        if is_star(&item.expr) {
            for name in &names {
                out.push(ReturnItem {
                    expr: Expr::Var(VarRef {
                        name: name.clone(),
                        span: item.span,
                    }),
                    alias: None,
                    // `RETURN *` names each column by its variable.
                    display: Some(name.clone()),
                    span: item.span,
                });
            }
        } else {
            out.push(item.clone());
        }
    }
    out
}

fn reject_empty_projection_wildcard(items: &[ReturnItem], s: &mut BinderState) -> bool {
    let Some(star) = items
        .iter()
        .find(|item| matches!(&item.expr, Expr::Var(VarRef { name, .. }) if name == "*"))
    else {
        return false;
    };
    if !s.vars.is_empty() || !s.path_vars.is_empty() {
        return false;
    }
    s.errors.push(BindError::new(
        BindErrorKind::InvalidArgument,
        star.span,
        "projection wildcard requires at least one variable in scope",
    ));
    true
}

fn ensure_var(name: Option<&String>, s: &mut BinderState) -> VarId {
    if let Some(n) = name {
        ensure_var_name(n, s)
    } else {
        alloc_anon_var(s)
    }
}

fn bound_rel_type_conflict(
    edge_var: VarId,
    rel_name: Option<&str>,
    is_scalar_hop: bool,
    s: &BinderState,
) -> bool {
    is_scalar_hop
        && matches!(
            (s.edge_rel_names.get(&edge_var).and_then(Option::as_deref), rel_name),
            (Some(prev), Some(next)) if prev != next
        )
}

fn push_false_filter(s: &mut BinderState) {
    let predicate = s.builder.push_expr(IrExpr::Literal(IrLiteral::Bool(false)));
    s.builder.push_op_mut(GraphOp::Filter { predicate });
}

fn push_conjunction(mut predicates: Vec<ExprId>, s: &mut BinderState) -> ExprId {
    let Some(mut acc) = predicates.pop() else {
        return s.builder.push_expr(IrExpr::Literal(IrLiteral::Bool(true)));
    };
    while let Some(next) = predicates.pop() {
        acc = s.builder.push_expr(IrExpr::BinaryOp {
            op: BinaryOpKind::And,
            left: next,
            right: acc,
        });
    }
    acc
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

struct ForwardedEdgeBinding {
    var: VarId,
    rel_name: Option<String>,
    endpoints: Option<(VarId, VarId)>,
}

struct GroupedEdgeBinding {
    alias: String,
    var: VarId,
    rel_name: Option<String>,
    endpoints: Option<(VarId, VarId)>,
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

fn lower_literal(lit: &Literal) -> IrLiteral {
    match lit {
        Literal::Int(n, _) => IrLiteral::Int(*n),
        Literal::Float(f, _) => IrLiteral::Float(*f),
        Literal::Str(s, _) => IrLiteral::Str(s.clone()),
        Literal::Bool(b, _) => IrLiteral::Bool(*b),
        Literal::Null(_) => IrLiteral::Null,
    }
}

fn lower_binop(op: AstBinOp) -> BinaryOpKind {
    match op {
        AstBinOp::Eq => BinaryOpKind::Eq,
        AstBinOp::Neq => BinaryOpKind::Neq,
        AstBinOp::Lt => BinaryOpKind::Lt,
        AstBinOp::Lte => BinaryOpKind::Lte,
        AstBinOp::Gt => BinaryOpKind::Gt,
        AstBinOp::Gte => BinaryOpKind::Gte,
        AstBinOp::And => BinaryOpKind::And,
        AstBinOp::Or => BinaryOpKind::Or,
        AstBinOp::Xor => BinaryOpKind::Xor,
        AstBinOp::Add => BinaryOpKind::Add,
        AstBinOp::Sub => BinaryOpKind::Sub,
        AstBinOp::Mul => BinaryOpKind::Mul,
        AstBinOp::Div => BinaryOpKind::Div,
        AstBinOp::Mod => BinaryOpKind::Mod,
        AstBinOp::Pow => BinaryOpKind::Pow,
        AstBinOp::Concat => unreachable!("handled separately"),
    }
}

fn lower_direction(dir: gf_ast::Direction) -> Direction {
    match dir {
        gf_ast::Direction::Out => Direction::Out,
        gf_ast::Direction::In => Direction::In,
        gf_ast::Direction::Undirected => Direction::Undirected,
    }
}

#[derive(Clone, Copy)]
enum RowCountOp {
    Skip,
    Limit,
}

impl RowCountOp {
    fn keyword(self) -> &'static str {
        match self {
            Self::Skip => "SKIP",
            Self::Limit => "LIMIT",
        }
    }

    fn graph_op(self, count: u64) -> GraphOp {
        match self {
            Self::Skip => GraphOp::Skip { count },
            Self::Limit => GraphOp::Limit { count },
        }
    }

    fn graph_param_op(self, name: String) -> GraphOp {
        match self {
            Self::Skip => GraphOp::SkipParam { name },
            Self::Limit => GraphOp::LimitParam { name },
        }
    }

    fn graph_expr_op(self, expr: ExprId) -> GraphOp {
        match self {
            Self::Skip => GraphOp::SkipExpr { expr },
            Self::Limit => GraphOp::LimitExpr { expr },
        }
    }
}

fn push_skip_limit(
    binder: &Binder,
    skip: Option<&Expr>,
    limit: Option<&Expr>,
    s: &mut BinderState,
) {
    if let Some(expr) = skip {
        push_row_count_op(binder, RowCountOp::Skip, expr, s);
    }
    if let Some(expr) = limit {
        push_row_count_op(binder, RowCountOp::Limit, expr, s);
    }
}

fn push_row_count_op(binder: &Binder, kind: RowCountOp, expr: &Expr, s: &mut BinderState) {
    if binder.typed_uuid_param_in(expr).is_some() {
        binder.lower_expr(expr, expr.span(), s);
        return;
    }
    if let Some(n) = extract_non_negative_int_constant(expr) {
        s.builder.push_op_mut(kind.graph_op(n));
        return;
    }
    if let Some(name) = extract_parameter_name(expr) {
        s.builder.push_op_mut(kind.graph_param_op(name));
        return;
    }
    if extract_int_constant(expr).is_some() || is_float_constant(expr) {
        s.errors.push(BindError::new(
            BindErrorKind::InvalidArgument,
            expr.span(),
            format!("{} requires a non-negative integer value", kind.keyword()),
        ));
        return;
    }
    let mut refs = Vec::new();
    collect_grouping_refs(expr, &mut refs);
    if refs.is_empty() && !expr_contains_aggregate(expr) && row_count_expr_is_integer(expr) {
        let expr = binder.lower_expr(expr, expr.span(), s);
        s.builder.push_op_mut(kind.graph_expr_op(expr));
        return;
    }
    s.errors.push(BindError::new(
        BindErrorKind::InvalidArgument,
        expr.span(),
        format!(
            "{} requires a non-negative variable-independent integer expression",
            kind.keyword()
        ),
    ));
}

fn row_count_expr_is_integer(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(Literal::Int(_, _)) | Expr::Param(_) => true,
        Expr::Parenthesized { inner, .. } => row_count_expr_is_integer(inner),
        Expr::UnaryOp(gf_ast::UnaryOp {
            op: AstUnOp::Neg,
            expr,
            ..
        }) => row_count_expr_is_integer(expr),
        Expr::BinaryOp(binary) => {
            row_count_expr_is_integer(&binary.left) && row_count_expr_is_integer(&binary.right)
        }
        Expr::FunctionCall(call) => is_function_named(call, "toInteger"),
        _ => false,
    }
}

fn is_float_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(Literal::Float(_, _)) => true,
        Expr::Parenthesized { inner, .. } => is_float_constant(inner),
        Expr::UnaryOp(gf_ast::UnaryOp {
            op: AstUnOp::Neg,
            expr,
            ..
        }) => is_float_constant(expr),
        _ => false,
    }
}

fn extract_parameter_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Param(gf_ast::ParamRef { name, .. }) => Some(name.clone()),
        Expr::Parenthesized { inner, .. } => extract_parameter_name(inner),
        _ => None,
    }
}

fn extract_non_negative_int_constant(expr: &Expr) -> Option<u64> {
    let n = extract_int_constant(expr)?;
    u64::try_from(n).ok()
}

fn extract_int_constant(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(Literal::Int(n, _)) => Some(*n),
        Expr::Parenthesized { inner, .. } => extract_int_constant(inner),
        Expr::UnaryOp(gf_ast::UnaryOp {
            op: AstUnOp::Neg,
            expr,
            ..
        }) => extract_int_constant(expr)?.checked_neg(),
        _ => None,
    }
}

/// Reject a duplicate EXPLICIT output column name in a RETURN/WITH projection
/// (`RETURN 1 AS a, 2 AS a` → openCypher `ColumnNameConflict`, #956). Only
/// explicit `AS` aliases are checked; implicit column names mirror the source
/// text, where a collision is both rare and self-inflicted.
/// Validate a relationship element in a CREATE/MERGE pattern (#956): exactly
/// one type (`NoSingleRelationshipType`), fixed-length (`CreatingVarLength`),
/// directed (`RequiresDirectedRelationship`), and not a reused already-bound
/// variable (`VariableAlreadyBound`). `bound_before` is the set of variables
/// bound before this clause.
fn validate_created_rel(
    rel: &gf_ast::RelPattern,
    bound_before: &std::collections::HashSet<VarId>,
    allow_undirected: bool,
    s: &mut BinderState,
) {
    let mut err = |m: &str| {
        s.errors.push(BindError::new(
            BindErrorKind::InvalidArgument,
            rel.span,
            m.to_string(),
        ));
    };
    if rel.types.len() != 1 {
        err("a created relationship must have exactly one type");
    }
    if rel.min_hops.is_some() || rel.max_hops.is_some() {
        err("cannot create a variable-length relationship");
    }
    if !allow_undirected && matches!(rel.direction, gf_ast::Direction::Undirected) {
        err("a created relationship must be directed");
    }
    if let Some(name) = rel.var.as_deref()
        && s.vars.get(name).is_some_and(|v| bound_before.contains(v))
    {
        s.errors.push(BindError::new(
            BindErrorKind::VariableAlreadyBound,
            rel.span,
            format!("relationship variable `{name}` is already bound"),
        ));
    }
}

fn check_duplicate_aliases(items: &[ReturnItem], s: &mut BinderState) {
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for item in items {
        if let Some(a) = item.alias.as_deref()
            && !seen.insert(a)
        {
            s.errors.push(BindError::new(
                BindErrorKind::InvalidArgument,
                item.span,
                format!("multiple result columns with the same name `{a}`"),
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProcedureDefinition, ProcedureField};
    use arrow::array::{StringArray, UInt32Array, UInt64Array};
    use gf_cypher::parse;
    use gf_ontology::{OntologyCompiler, OntologyDoc, OntologyHandle};

    fn make_binder(mode: OntologyMode) -> (Binder, Arc<Mutex<RuntimeCatalog>>) {
        let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
        let binder = Binder::new(None, Arc::clone(&catalog), mode);
        (binder, catalog)
    }

    fn catalog_entry(catalog: &RuntimeCatalog, kind: &str, name: &str) -> (u32, u64) {
        let batch = catalog.to_record_batch();
        let kinds = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let counts = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let row = (0..batch.num_rows())
            .find(|&row| kinds.value(row) == kind && names.value(row) == name)
            .unwrap_or_else(|| panic!("missing catalog entry {kind}:{name}"));
        (ids.value(row), counts.value(row))
    }

    fn strict_property_binder(shadow_inherited: bool) -> (Binder, Arc<Mutex<RuntimeCatalog>>) {
        let shadow = if shadow_inherited {
            r#", {"owner":"Host","name":"inherited","type":"utf8"}"#
        } else {
            ""
        };
        let doc: OntologyDoc = serde_json::from_str(&format!(
            r#"{{"ontology_id":"strict-properties","version":"1","entity_types":[{{"name":"Asset"}},{{"name":"Host","parent":"Asset"}}],"relation_types":[{{"name":"R","src":"Host","dst":"Host"}},{{"name":"S","src":"Host","dst":"Host"}}],"properties":[{{"owner":"Asset","name":"inherited","type":"utf8"}},{{"owner":"Host","name":"direct","type":"utf8"}},{{"owner":"R","name":"weight","type":"int64"}},{{"owner":"S","name":"weight","type":"int64"}},{{"owner":"Asset","name":"shared","type":"utf8"}},{{"owner":"R","name":"shared","type":"utf8"}}{shadow}]}}"#
        ))
        .unwrap();
        let ontology = OntologyCompiler::compile(&doc).unwrap();
        let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
        catalog.lock().unwrap().intern_property("preexisting", None);
        (
            Binder::new(
                Some(OntologyHandle::new(ontology)),
                Arc::clone(&catalog),
                OntologyMode::Strict,
            ),
            catalog,
        )
    }

    fn plan_property_ids(plan: &GraphPlan) -> Vec<PropId> {
        (0..u32::try_from(plan.exprs.len()).unwrap())
            .filter_map(|index| match plan.exprs.get(ExprId(index)) {
                IrExpr::PropertyAccess { prop, .. } => Some(*prop),
                _ => None,
            })
            .collect()
    }

    fn property_error(binder: &Binder, query: &str, kind: BindErrorKind, span: &str) -> BindError {
        let errors = binder.bind(&parse(query).unwrap()).expect_err(query);
        assert_eq!(errors.len(), 1, "{query}: {errors:?}");
        let error = errors.into_iter().next().unwrap();
        assert_eq!(error.kind, kind);
        assert_eq!(&query[error.span.start..error.span.end], span);
        error
    }

    #[test]
    fn exploratory_unknown_label_succeeds() {
        let (binder, catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a:UnknownLabel)-[:UNKNOWN_REL]->(b) RETURN a").unwrap();
        let plan = binder.bind(&ast).expect("exploratory bind should succeed");

        assert!(
            plan.ops
                .iter()
                .any(|op| matches!(op, GraphOp::NodeScan { .. }))
        );
        let cat = catalog.lock().unwrap();
        assert!(cat.contains_entity_type("UnknownLabel"));
        assert!(cat.relation_types().contains(&"UNKNOWN_REL"));
    }

    #[test]
    fn bind_catalog_mutations_commit_only_after_success() {
        let (binder, catalog) = make_binder(OntologyMode::Exploratory);
        {
            let mut catalog = catalog.lock().unwrap();
            assert_eq!(catalog.intern_label("Seed").0, 0);
            assert_eq!(catalog.intern_relation_type("SEED_REL").0, 1);
            assert_eq!(catalog.intern_property("seed", None).0, 0);
        }
        let before = catalog.lock().unwrap().to_record_batch();

        let errors = binder
            .bind(
                &parse(
                    "MATCH (n:Rejected)-[:REJECTED_REL]->() \
                     RETURN n.rejected, missing",
                )
                .unwrap(),
            )
            .expect_err("a later semantic error must reject every staged observation");
        assert!(
            errors
                .iter()
                .any(|error| error.kind == BindErrorKind::UndeclaredVariable)
        );
        assert_eq!(
            catalog.lock().unwrap().to_record_batch(),
            before,
            "failed binding must leave entries, observations, timestamps, and IDs unchanged"
        );

        binder
            .bind(
                &parse(
                    "MATCH (n:Accepted)-[:ACCEPTED_REL]->() \
                     RETURN n.accepted",
                )
                .unwrap(),
            )
            .expect("successful binding must publish the staged catalog");
        let catalog = catalog.lock().unwrap();
        assert_eq!(catalog_entry(&catalog, "entity_type", "Accepted"), (2, 1));
        assert_eq!(
            catalog_entry(&catalog, "relation_type", "ACCEPTED_REL"),
            (3, 1)
        );
        assert_eq!(catalog_entry(&catalog, "property", "accepted"), (1, 1));
    }

    #[test]
    fn fixed_pattern_predicate_lowers_to_exists() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (n) WHERE (n)-[:REL]->() RETURN n").unwrap();
        let plan = binder.bind(&ast).expect("pattern predicate binds");

        let exists = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Exists { child, negated } => Some((child, negated)),
                _ => None,
            })
            .expect("pattern predicate should lower to Exists");
        assert!(!*exists.1);
        assert!(
            exists
                .0
                .ops
                .iter()
                .any(|op| matches!(op, GraphOp::Expand { .. })),
            "child plan must match the relationship pattern"
        );
    }

    #[test]
    fn relationship_uniqueness_is_scoped_to_each_path_pattern() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a)-[r1]->(b)-[r2]->(c), (x)-[r3]->(y) RETURN a").unwrap();
        let plan = binder.bind(&ast).expect("pattern binds");

        let constraints: Vec<_> = plan
            .ops
            .iter()
            .filter_map(|op| match op {
                GraphOp::RelationshipUnique { edge, prior_edges } => {
                    Some((*edge, prior_edges.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(constraints.len(), 1);
        assert_eq!(constraints[0].1.len(), 1);
    }

    #[test]
    fn simple_existential_subquery_allows_child_local_variables() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (n) WHERE exists { (n)-[r]->(m) WHERE type(r) = 'REL' } RETURN n")
            .unwrap();
        let plan = binder.bind(&ast).expect("existential subquery binds");

        let child = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Exists { child, negated } => {
                    assert!(!negated);
                    Some(child)
                }
                _ => None,
            })
            .expect("expected Exists op");
        assert!(
            child
                .ops
                .iter()
                .any(|op| matches!(op, GraphOp::Expand { .. }))
        );
        assert!(
            child
                .ops
                .iter()
                .any(|op| matches!(op, GraphOp::Filter { .. }))
        );
    }

    #[test]
    fn full_existential_correlation_is_scope_aware() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let correlated = parse(
            "MATCH (n) WHERE exists { MATCH (m) WHERE m.prop = n.prop RETURN true } RETURN n",
        )
        .unwrap();
        binder
            .bind(&correlated)
            .expect("an outer variable used only in a child expression must bind");

        let shadowed = parse("MATCH (n) WHERE exists { WITH 1 AS n RETURN n } RETURN n").unwrap();
        let errors = binder
            .bind(&shadowed)
            .expect_err("a child-local alias must not count as outer correlation");
        assert!(
            errors.iter().any(|error| {
                error.kind == BindErrorKind::UndeclaredVariable
                    && error
                        .message
                        .contains("must reference at least one outer variable")
            }),
            "expected uncorrelated-subquery error, got {errors:?}"
        );
    }

    #[test]
    fn with_nested_aggregate_lowers_to_aggregate_then_scope_reset() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast =
            parse("MATCH (me)--(you) WITH me.age AS age, me.age + count(you.age) AS agg RETURN *")
                .unwrap();
        let plan = binder.bind(&ast).expect("nested WITH aggregate binds");
        let aggregate = plan
            .ops
            .iter()
            .position(|op| matches!(op, GraphOp::Aggregate { .. }))
            .expect("aggregate op");
        let with = plan
            .ops
            .iter()
            .skip(aggregate + 1)
            .position(|op| matches!(op, GraphOp::With { .. }))
            .expect("post-aggregate scope reset");
        assert_eq!(with, 0);
    }

    #[test]
    fn with_rejects_ambiguous_and_nested_aggregation() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ambiguous =
            parse("MATCH (me)--(you) WITH me.age + count(you.age) AS agg RETURN agg").unwrap();
        let error = binder
            .bind(&ambiguous)
            .expect_err("ambiguous grouping must fail");
        assert!(
            error
                .iter()
                .any(|error| error.message.contains("ambiguous aggregation expression"))
        );

        let nested = parse("MATCH (n) WITH count(count(*)) AS c RETURN c").unwrap();
        let error = binder
            .bind(&nested)
            .expect_err("nested aggregates must fail");
        assert!(
            error
                .iter()
                .any(|error| { error.message.contains("may not contain another aggregate") })
        );
    }

    #[test]
    fn return_rejects_nested_and_volatile_aggregation() {
        for (query, message) in [
            (
                "RETURN count(count(*))",
                "may not contain another aggregate",
            ),
            (
                "RETURN count(rand())",
                "non-deterministic functions are not allowed",
            ),
        ] {
            let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
            let ast = parse(query).unwrap();
            let errors = binder.bind(&ast).expect_err("query must fail at bind");
            assert!(
                errors.iter().any(|error| error.message.contains(message)),
                "missing {message:?} for {query}: {errors:?}"
            );
        }
    }

    #[test]
    fn return_rejects_unknown_function_at_bind() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a) RETURN foo(a)").unwrap();
        let errors = binder.bind(&ast).expect_err("unknown function must fail");
        assert!(
            errors
                .iter()
                .any(|error| error.message.contains("unknown function `foo`"))
        );
    }

    #[test]
    fn negated_pattern_predicate_lowers_to_anti_exists() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (n) WHERE NOT (n)-[:REL]-() RETURN n").unwrap();
        let plan = binder.bind(&ast).expect("negated pattern predicate binds");

        assert!(
            plan.ops
                .iter()
                .any(|op| { matches!(op, GraphOp::Exists { negated: true, .. }) })
        );
    }

    #[test]
    fn multi_type_pattern_predicate_lowers_to_union_exists() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (n), (m) WHERE (n)-[:REL1|REL2]-(m) RETURN n").unwrap();
        let plan = binder.bind(&ast).expect("multi-type predicate binds");

        let inputs = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Exists { child, .. } => match child.ops.as_slice() {
                    [GraphOp::Union { inputs, .. }] => Some(inputs),
                    _ => None,
                },
                _ => None,
            })
            .expect("multi-type predicate should lower to union-backed Exists");
        assert_eq!(inputs.len(), 2);
    }

    #[test]
    fn or_pattern_predicate_lowers_to_union_exists() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (n) WHERE (n)-[:REL1]-() OR (n)-[:REL2]-() RETURN n").unwrap();
        let plan = binder.bind(&ast).expect("OR predicate binds");

        let inputs = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Exists { child, .. } => match child.ops.as_slice() {
                    [GraphOp::Union { inputs, .. }] => Some(inputs),
                    _ => None,
                },
                _ => None,
            })
            .expect("OR predicate should lower to union-backed Exists");
        assert_eq!(inputs.len(), 2);
    }

    #[test]
    fn pattern_predicate_rejects_new_named_variables() {
        expect_bind_error(
            "MATCH (n) WHERE (n)-[r]->() RETURN n",
            BindErrorKind::UndeclaredVariable,
        );
        expect_bind_error(
            "MATCH (n) WHERE (n)-->(m) RETURN n",
            BindErrorKind::UndeclaredVariable,
        );
    }

    #[test]
    fn pattern_predicate_rejects_named_path_binding() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let mut ast = parse("MATCH (n) WHERE (n)-[:REL]->() RETURN n").unwrap();
        let AstClause::Match(m) = &mut ast.clauses[0] else {
            panic!("expected MATCH clause");
        };
        let where_clause = m.where_clause.as_mut().expect("expected WHERE");
        let Expr::PatternPredicate(pp) = &mut where_clause.predicate else {
            panic!("expected pattern predicate");
        };
        pp.pattern.var = Some("p".into());

        let errs = binder
            .bind(&ast)
            .expect_err("predicate-local path binding should fail");
        assert!(
            errs.iter()
                .any(|err| err.kind == BindErrorKind::UndeclaredVariable),
            "expected undeclared path binding error, got {errs:?}"
        );
    }

    #[test]
    fn var_length_pattern_predicate_rejects_relationship_properties() {
        expect_bind_error(
            "MATCH (n) WHERE (n)-[:REL* {k: 1}]->() RETURN n",
            BindErrorKind::InvalidArgument,
        );
    }

    #[test]
    fn pattern_predicate_rejects_uncorrelated_patterns() {
        expect_bind_error(
            "MATCH (n) WHERE ()-[:REL]->() RETURN n",
            BindErrorKind::UndeclaredVariable,
        );
    }

    #[test]
    fn named_pattern_comprehension_binds_correlated_child_projection() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (n) RETURN [p = (n)-[:REL]->() | p] AS paths").unwrap();
        let plan = binder.bind(&ast).expect("pattern comprehension binds");

        let (child, output) = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::PatternComprehension { child, output } => Some((child, output)),
                _ => None,
            })
            .expect("expected a PatternComprehension op");
        assert!(
            child
                .ops
                .iter()
                .any(|op| matches!(op, GraphOp::Expand { .. }))
        );
        let projection = child
            .ops
            .last()
            .and_then(|op| match op {
                GraphOp::Project { items, .. } => items.first(),
                _ => None,
            })
            .expect("child must end with one value projection");
        assert_eq!(
            projection.alias.as_deref(),
            Some(PATTERN_COMPREHENSION_VALUE_ALIAS)
        );
        assert!(matches!(
            child.exprs.get(projection.expr),
            IrExpr::FunctionCall { name, .. } if name == "_path_struct"
        ));
        let outer_projection = plan
            .ops
            .iter()
            .rev()
            .find_map(|op| match op {
                GraphOp::Project { items, .. } => items.first(),
                _ => None,
            })
            .expect("outer RETURN must project the collected result");
        assert!(matches!(
            plan.exprs.get(outer_projection.expr),
            IrExpr::VarRef(var) if var == output
        ));
    }

    #[test]
    fn pattern_comprehension_binds_local_node_and_relationship_properties() {
        for query in [
            "MATCH (n) RETURN [(n)-[:REL]->(b) | b.name] AS names",
            "MATCH (n) RETURN [(n)-[r:REL]->() | r.name] AS names",
        ] {
            let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
            let ast = parse(query).unwrap();
            let plan = binder.bind(&ast).expect("local projection binds");
            let child = plan
                .ops
                .iter()
                .find_map(|op| match op {
                    GraphOp::PatternComprehension { child, .. } => Some(child),
                    _ => None,
                })
                .expect("expected a PatternComprehension op");
            let projection = child
                .ops
                .last()
                .and_then(|op| match op {
                    GraphOp::Project { items, .. } => items.first(),
                    _ => None,
                })
                .expect("child must end with a projection");
            assert!(matches!(
                child.exprs.get(projection.expr),
                IrExpr::PropertyAccess { .. }
            ));
        }
    }

    #[test]
    fn pattern_comprehension_binds_filter_and_variable_length_match() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast =
            parse("MATCH (n) RETURN [(n)-[r:REL*]->(b) WHERE b.ok = true | b] AS matches").unwrap();
        let plan = binder
            .bind(&ast)
            .expect("filtered var-length comprehension binds");
        let child = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::PatternComprehension { child, .. } => Some(child),
                _ => None,
            })
            .expect("expected a PatternComprehension op");

        assert!(child.ops.iter().any(|op| {
            matches!(
                op,
                GraphOp::Expand {
                    min_hops: 1,
                    max_hops: None,
                    ..
                }
            )
        }));
        assert!(
            child
                .ops
                .iter()
                .any(|op| matches!(op, GraphOp::Filter { .. }))
        );
        assert!(matches!(child.ops.last(), Some(GraphOp::Project { .. })));
    }

    #[test]
    fn pattern_comprehension_local_variables_do_not_leak() {
        expect_bind_error(
            "MATCH (n) RETURN [(n)-->(b) | b] AS matches, b",
            BindErrorKind::UndeclaredVariable,
        );
    }

    #[test]
    fn pattern_comprehension_in_list_element_scope_lifts_to_graph_op() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (n) RETURN [x IN [n] | [(x)-->() | x]] AS nested").unwrap();
        let plan = binder
            .bind(&ast)
            .expect("bind nested pattern comprehension");
        let lifted = plan.ops.iter().find_map(|op| match op {
            GraphOp::ListElementPatternComprehension { child, .. } => Some(child),
            _ => None,
        });
        let child = lifted.expect("list-element graph operation");
        assert!(matches!(child.ops.last(), Some(GraphOp::Project { .. })));
    }

    #[test]
    fn bare_graph_value_where_predicate_is_rejected() {
        expect_bind_error(
            "MATCH (n) WHERE (n) RETURN n",
            BindErrorKind::InvalidArgument,
        );
    }

    #[test]
    fn bare_path_value_where_predicate_is_rejected() {
        expect_bind_error(
            "MATCH p = (n)-[:REL]->() WHERE p RETURN p",
            BindErrorKind::InvalidArgument,
        );
    }

    #[test]
    fn with_where_pattern_predicate_lowers_after_with() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (n) WITH n WHERE (n)-[:REL]->() RETURN n").unwrap();
        let plan = binder.bind(&ast).expect("WITH pattern predicate binds");

        let with_idx = plan
            .ops
            .iter()
            .position(|op| matches!(op, GraphOp::With { .. }))
            .expect("WITH should lower to a With op");
        let exists_idx = plan
            .ops
            .iter()
            .position(|op| matches!(op, GraphOp::Exists { .. }))
            .expect("WITH WHERE pattern predicate should lower to Exists");
        assert!(
            with_idx < exists_idx,
            "WITH projection must run before its pattern predicate"
        );
    }

    #[test]
    fn create_single_node_populates_pattern() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("CREATE (:Person {name: 'Alice', age: 30})").unwrap();
        let plan = binder.bind(&ast).expect("create bind should succeed");

        let create = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Create { pattern } => Some(pattern),
                _ => None,
            })
            .expect("expected a Create op");
        assert_eq!(create.nodes.len(), 1);
        assert!(create.edges.is_empty());
        let node = &create.nodes[0];
        assert_eq!(node.labels.len(), 1, "Person label should resolve");
        let props = node.properties.expect("node should have a property map");
        // The property expr should be a MapLiteral in the arena.
        assert!(matches!(plan.exprs.get(props), IrExpr::MapLiteral(_)));
    }

    #[test]
    fn create_edge_threads_src_and_dst_vars() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("CREATE (a:Person)-[:KNOWS]->(b:Person)").unwrap();
        let plan = binder.bind(&ast).expect("create bind should succeed");

        let create = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Create { pattern } => Some(pattern),
                _ => None,
            })
            .expect("expected a Create op");
        assert_eq!(create.nodes.len(), 2);
        assert_eq!(create.edges.len(), 1);
        let edge = &create.edges[0];
        // The edge's src/dst must match the two node vars, in order.
        assert_eq!(edge.src, create.nodes[0].var);
        assert_eq!(edge.dst, create.nodes[1].var);
        assert_eq!(edge.direction, Direction::Out);
        assert!(edge.rel_type.is_some());
    }

    #[test]
    fn standalone_create_node_is_not_a_reference() {
        // No preceding clause → the CREATE introduces the var → mint, not ref.
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("CREATE (a:Person)").unwrap();
        let plan = binder.bind(&ast).expect("bind");
        let create = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Create { pattern } => Some(pattern),
                _ => None,
            })
            .expect("Create op");
        assert_eq!(create.nodes.len(), 1);
        assert!(
            !create.nodes[0].is_reference,
            "a CREATE-introduced var must be a mint, not a reference"
        );
    }

    // -----------------------------------------------------------------------
    // Variable-kind conflict validation (#956, VariableTypeConflict)
    // -----------------------------------------------------------------------

    /// Bind and assert the query is REJECTED with a `VariableKindConflict`.
    fn expect_kind_conflict(query: &str) {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse(query).expect("parse");
        let errs = binder
            .bind(&ast)
            .expect_err(&format!("expected a kind conflict for: {query}"));
        assert!(
            errs.iter()
                .any(|e| e.kind == BindErrorKind::VariableKindConflict),
            "expected VariableKindConflict for {query}, got {errs:?}"
        );
    }

    #[test]
    fn relationship_var_reused_as_node_conflicts() {
        // A relationship variable used as a node pattern is a VariableTypeConflict.
        for q in [
            "MATCH ()-[r]-() MATCH (r) RETURN r",
            "MATCH ()-[r]->() MATCH (r) RETURN r",
            "MATCH (), ()-[r]-() MATCH (r) RETURN r",
        ] {
            expect_kind_conflict(q);
        }
    }

    #[test]
    fn path_var_reused_as_node_conflicts() {
        // A (single-segment) path variable reused as a node pattern conflicts.
        expect_kind_conflict("MATCH r = ()-[]-() MATCH (r) RETURN r");
    }

    #[test]
    fn scalar_aliases_reused_as_pattern_entities_conflict() {
        expect_kind_conflict("WITH 42 AS n MATCH (n) RETURN n");
        expect_kind_conflict("WITH true AS r MATCH ()-[r]->() RETURN r");
        expect_kind_conflict(
            "MATCH (n) WITH collect(n) AS users MATCH (users)-[:R]->() RETURN users",
        );
    }

    #[test]
    fn runtime_polymorphic_pattern_values_remain_bindable() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        for query in [
            "WITH null AS a OPTIONAL MATCH p = (a)-[r]->() RETURN relationships(p)",
            "MATCH (a) WITH collect(a) AS nodes UNWIND nodes AS n MATCH (n) RETURN n",
        ] {
            let ast = parse(query).expect("parse");
            binder
                .bind(&ast)
                .unwrap_or_else(|errors| panic!("expected clean bind for {query}: {errors:?}"));
        }
    }

    #[test]
    fn direct_graph_function_kind_mismatches_are_rejected() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        for query in [
            "MATCH (n) RETURN type(n)",
            "MATCH ()-[r]->() RETURN labels(r)",
            "MATCH (n) RETURN length(n)",
            "MATCH ()-[r]->() RETURN length(r)",
            "MATCH p = (n) RETURN labels(p)",
        ] {
            let ast = parse(query).expect("parse");
            let errors = binder.bind(&ast).expect_err("expected invalid argument");
            assert!(
                errors
                    .iter()
                    .any(|error| error.kind == BindErrorKind::InvalidArgument),
                "expected InvalidArgument for {query}, got {errors:?}"
            );
        }
    }

    #[test]
    fn compatible_variable_reuse_is_accepted() {
        // Guardrail: re-using a variable with the SAME kind, distinct variables,
        // and WITH-rescoping must all still bind cleanly (no false conflict).
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        for q in [
            "MATCH (a) MATCH (a) RETURN a",              // node reused as node
            "MATCH (a), (b) RETURN a, b",                // distinct node vars
            "MATCH (a)-[r]->(b), (b)-[s]->(c) RETURN a", // b: dst then src, both nodes
            "MATCH (a) WITH a MATCH (b) RETURN a, b",    // WITH-rescoped
            "MATCH (a) CREATE (a)-[:R]->(b)",            // matched node referenced in CREATE
        ] {
            let ast = parse(q).expect("parse");
            binder
                .bind(&ast)
                .unwrap_or_else(|e| panic!("expected clean bind for {q}, got {e:?}"));
        }
    }

    #[test]
    fn relationship_var_reused_with_different_type_adds_false_filter() {
        // A relationship variable forwarded through WITH keeps its known type.
        // Reusing it with a different known type is a valid match that returns no
        // rows; encode that independently of whether the edge scan schema carries
        // a `rel_type_name` column.
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH ()-[r:T]->() WITH r MATCH ()-[r:Y]->() RETURN r").expect("parse");
        let plan = binder.bind(&ast).expect("bind");

        let has_false_filter = plan.ops.iter().any(|op| match op {
            GraphOp::Filter { predicate } => {
                matches!(
                    plan.exprs.get(*predicate),
                    IrExpr::Literal(IrLiteral::Bool(false))
                )
            }
            _ => false,
        });

        assert!(
            has_false_filter,
            "reusing a known relationship variable with a different type should filter to no rows"
        );
    }

    // -----------------------------------------------------------------------
    // Validator tail (#956): CREATE-pattern, rebind, aggregate-in-WHERE, alias
    // -----------------------------------------------------------------------

    /// Bind and assert the query is REJECTED with any bind error of `kind`.
    fn expect_bind_error(query: &str, kind: BindErrorKind) {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse(query).expect("parse");
        let errs = binder
            .bind(&ast)
            .expect_err(&format!("expected a bind error for: {query}"));
        assert!(
            errs.iter().any(|e| e.kind == kind),
            "expected {kind:?} for {query}, got {errs:?}"
        );
    }

    #[test]
    fn create_pattern_validation() {
        // A created relationship must have exactly one type, be fixed-length,
        // and be directed (#956).
        for q in [
            "CREATE ()-->()",         // no type
            "CREATE ()-[:FOO*2]->()", // variable-length
            "CREATE (a)-[:FOO]-(b)",  // undirected
        ] {
            expect_bind_error(q, BindErrorKind::InvalidArgument);
        }
    }

    #[test]
    fn rebinding_a_bound_variable_in_create_is_rejected() {
        for q in [
            "MATCH (a) CREATE (a)",                          // bare re-create
            "MATCH (a) CREATE (a {name: 'x'})",              // re-declared with props
            "CREATE (n:Foo) CREATE (n:Bar)-[:OWNS]->(:Dog)", // re-declared with a label
            "MATCH ()-[r]->() CREATE ()-[r]->()",            // reused relationship var
        ] {
            expect_bind_error(q, BindErrorKind::VariableAlreadyBound);
        }
    }

    #[test]
    fn create_reference_without_new_shape_is_accepted() {
        // Guardrail: a bound node referenced as an edge endpoint (no new labels
        // or properties) is a valid reference, not a rebind.
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a) CREATE (a)-[:R]->(b)").expect("parse");
        binder.bind(&ast).expect("valid reference must bind");
    }

    #[test]
    fn aggregate_in_where_and_duplicate_alias_are_rejected() {
        expect_bind_error(
            "MATCH (a) WHERE count(a) > 10 RETURN a",
            BindErrorKind::InvalidArgument,
        );
        expect_bind_error("RETURN 1 AS a, 2 AS a", BindErrorKind::InvalidArgument);
        expect_bind_error(
            "WITH 1 AS a, 2 AS a RETURN a",
            BindErrorKind::InvalidArgument,
        );
    }

    #[test]
    fn skip_limit_reject_negative_and_non_parameter_arguments() {
        for q in [
            "RETURN 1 SKIP -1",
            "RETURN 1 LIMIT -1",
            "RETURN 1 SKIP (-1)",
            "WITH 1 AS x SKIP -1 RETURN x",
            "WITH 1 AS x, count(*) AS c SKIP -1 RETURN x, c",
            "MATCH (n) RETURN n SKIP n.count",
            "MATCH (n) RETURN n LIMIT rand()",
            "MATCH (n) WITH n SKIP n.count RETURN n",
            "MATCH (n) WITH n, count(*) AS c LIMIT rand() RETURN n, c",
        ] {
            expect_bind_error(q, BindErrorKind::InvalidArgument);
        }
    }

    #[test]
    fn skip_limit_accept_non_negative_integer_constants_and_parameters() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        for q in [
            "RETURN 1 SKIP 0 LIMIT 1",
            "WITH 1 AS x SKIP (1) LIMIT 2 RETURN x",
            "WITH 1 AS x, count(*) AS c SKIP 0 LIMIT 1 RETURN x, c",
            "RETURN 1 SKIP $s LIMIT $l",
            "WITH 1 AS x SKIP ($s) LIMIT ($l) RETURN x",
        ] {
            let ast = parse(q).expect("parse");
            binder
                .bind(&ast)
                .unwrap_or_else(|e| panic!("expected clean bind for {q}, got {e:?}"));
        }
    }

    #[test]
    fn overflowing_float_literal_is_rejected_at_bind() {
        expect_bind_error("RETURN 1.34E999", BindErrorKind::InvalidArgument);
    }

    #[test]
    fn matched_var_in_create_is_a_reference_not_a_duplicate_mint() {
        // #703: `MATCH (a) CREATE (a)-[:KNOWS]->(b)` — `a` was bound by the
        // MATCH, so its CREATE node spec must be a REFERENCE (resolve the matched
        // node), and there must be exactly ONE spec for `a` (not a second mint).
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a:Person) CREATE (a)-[:KNOWS]->(b:Person)").expect("parse");
        let plan = binder.bind(&ast).expect("bind");
        let create = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Create { pattern } => Some(pattern),
                _ => None,
            })
            .expect("Create op");

        // The matched `a` is the edge src; the new `b` is the dst.
        let a_var = create.edges[0].src;
        let b_var = create.edges[0].dst;
        let a_specs: Vec<_> = create.nodes.iter().filter(|n| n.var == a_var).collect();
        assert_eq!(a_specs.len(), 1, "exactly one spec for the matched var `a`");
        assert!(a_specs[0].is_reference, "matched `a` must be a reference");
        let b_spec = create
            .nodes
            .iter()
            .find(|n| n.var == b_var)
            .expect("spec for new `b`");
        assert!(!b_spec.is_reference, "CREATE-introduced `b` must be a mint");
    }

    #[test]
    fn delete_clause_lowers_to_delete_op() {
        // #740: DELETE now lowers to GraphOp::Delete (no longer rejected).
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (p:Person) DELETE p").unwrap();
        let plan = binder.bind(&ast).expect("DELETE binds");
        let delete = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Delete { vars, detach, .. } => Some((vars.clone(), *detach)),
                _ => None,
            })
            .expect("a GraphOp::Delete op");
        assert_eq!(delete.0.len(), 1, "one target var");
        assert!(!delete.1, "plain DELETE is not DETACH");
    }

    #[test]
    fn detach_delete_sets_detach_flag() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (p:Person) DETACH DELETE p").unwrap();
        let plan = binder.bind(&ast).expect("DETACH DELETE binds");
        assert!(
            plan.ops
                .iter()
                .any(|op| matches!(op, GraphOp::Delete { detach: true, .. })),
            "DETACH DELETE must set detach=true, got {:?}",
            plan.ops
        );
    }

    #[test]
    fn delete_property_target_lowers_to_runtime_expression() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (p:Person) DELETE p.name").unwrap();
        let plan = binder
            .bind(&ast)
            .expect("property value binds for runtime typing");
        assert!(matches!(
            plan.ops.last(),
            Some(GraphOp::Delete { vars, exprs, .. }) if vars.is_empty() && exprs.len() == 1
        ));
    }

    #[test]
    fn delete_scalar_expression_is_rejected() {
        expect_bind_error("MATCH () DELETE 1 + 1", BindErrorKind::InvalidDeleteTarget);
    }

    #[test]
    fn delete_undeclared_variable_is_rejected() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (p:Person) DELETE q").unwrap();
        let errors = binder
            .bind(&ast)
            .expect_err("DELETE of an unbound var must be rejected");
        assert!(
            errors
                .iter()
                .any(|e| e.kind == BindErrorKind::UndeclaredVariable),
            "expected UndeclaredVariable, got {errors:?}"
        );
    }

    #[test]
    fn set_property_clause_lowers_to_set_op() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (p:Person) SET p.age = 30").unwrap();
        let plan = binder.bind(&ast).expect("SET p.age = 30 must lower");
        let set = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Set { items, .. } => Some(items),
                _ => None,
            })
            .expect("expected a GraphOp::Set");
        assert_eq!(set.len(), 1);
        assert_eq!(set[0].prop_name, "age");
    }

    #[test]
    fn set_runtime_expr_value_lowers_to_non_literal() {
        // The value `p.age + 1` is a runtime expression, not a literal — it must
        // survive lowering as a compound `IrExpr`, not be collapsed to a literal.
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (p:Person) SET p.age = p.age + 1").unwrap();
        let plan = binder.bind(&ast).expect("SET p.age = p.age + 1 must lower");
        let items = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Set { items, .. } => Some(items),
                _ => None,
            })
            .expect("expected a GraphOp::Set");
        let value = plan.exprs.get(items[0].value);
        assert!(
            matches!(value, IrExpr::BinaryOp { .. }),
            "expected a BinaryOp value expr, got {value:?}"
        );
    }

    #[test]
    fn set_multiple_items_lower_to_one_op() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (p:Person) SET p.age = 30, p.name = 'Al'").unwrap();
        let plan = binder.bind(&ast).expect("multi-item SET must lower");
        let count = plan
            .ops
            .iter()
            .filter(|op| matches!(op, GraphOp::Set { .. }))
            .count();
        assert_eq!(count, 1, "expected exactly one GraphOp::Set");
    }

    #[test]
    fn set_labels_lower_to_resolved_label_item() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (p:Person) SET p:Admin:Staff").unwrap();
        let plan = binder.bind(&ast).expect("SET labels must lower");
        let labels = plan.ops.iter().find_map(|op| match op {
            GraphOp::Set { label_items, .. } => Some(label_items),
            _ => None,
        });
        let labels = labels.expect("SET label items");
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].labels.len(), 2);
    }

    #[test]
    fn set_property_merge_lowers_to_map_item() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (p:Person) SET p += {age: 30}").unwrap();
        let plan = binder.bind(&ast).expect("SET += must lower");
        let map_items = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Set { map_items, .. } => Some(map_items),
                _ => None,
            })
            .expect("expected SET op");
        assert_eq!(map_items.len(), 1);
        assert!(!map_items[0].replace);
    }

    #[test]
    fn merge_lowers_real_pattern_specs() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse(
            "MERGE (p:Person {name:'Alice'}) \
             ON CREATE SET p.created = 1, p:New \
             ON MATCH SET p += {seen:true}",
        )
        .unwrap();
        let plan = binder.bind(&ast).expect("MERGE binds");
        let (pattern, on_create, on_match) = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Merge {
                    pattern,
                    on_create,
                    on_match,
                } => Some((pattern, on_create, on_match)),
                _ => None,
            })
            .expect("MERGE op");
        assert_eq!(pattern.nodes.len(), 1);
        assert!(pattern.nodes[0].properties.is_some());
        assert_eq!(on_create.len(), 2);
        assert_eq!(on_match.len(), 1);
    }

    #[test]
    fn set_undeclared_variable_rejected() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (p:Person) SET q.age = 30").unwrap();
        let errors = binder.bind(&ast).expect_err("SET on unbound var must fail");
        assert!(
            errors
                .iter()
                .any(|e| e.kind == BindErrorKind::UndeclaredVariable),
            "expected UndeclaredVariable, got {errors:?}"
        );
    }

    #[test]
    fn remove_property_clause_lowers_to_remove_op() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (p:Person) REMOVE p.age").unwrap();
        let plan = binder.bind(&ast).expect("REMOVE p.age must lower");
        let items = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Remove { items, .. } => Some(items),
                _ => None,
            })
            .expect("expected a GraphOp::Remove");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].prop_name, "age");
    }

    #[test]
    fn remove_labels_lower_to_resolved_label_item() {
        let (binder, _catalog) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (p:Person) REMOVE p:Admin:Staff").unwrap();
        let plan = binder.bind(&ast).expect("REMOVE labels must lower");
        let labels = plan.ops.iter().find_map(|op| match op {
            GraphOp::Remove { label_items, .. } => Some(label_items),
            _ => None,
        });
        let labels = labels.expect("REMOVE label items");
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].labels.len(), 2);
    }

    #[test]
    fn advisory_unknown_label_produces_warnings() {
        let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
        let binder = Binder::new(None, Arc::clone(&catalog), OntologyMode::Advisory);
        let ast = parse("MATCH (a:UnknownLabel) RETURN a").unwrap();

        let mut state = BinderState {
            vars: HashMap::new(),
            path_vars: HashMap::new(),
            node_vars: HashMap::new(),
            edge_vars: HashMap::new(),
            edge_rel_names: HashMap::new(),
            scalar_list_edges: HashSet::new(),
            var_kinds: HashMap::new(),
            next_var: 0,
            builder: GraphPlan::builder("openCypher").ontology_mode(OntologyMode::Advisory),
            errors: Vec::new(),
            warnings: Vec::new(),
            captured_pattern_comprehensions: None,
            existential_depth: 0,
            standalone_call: false,
        };
        for clause in &ast.clauses {
            binder.lower_clause(clause, &mut state);
        }

        assert!(
            !state.warnings.is_empty(),
            "advisory mode should produce warnings"
        );
        assert!(
            state.errors.is_empty(),
            "advisory mode should not produce errors"
        );
        assert!(
            state
                .warnings
                .iter()
                .any(|w| w.kind == BindErrorKind::UnknownLabel)
        );
    }

    #[test]
    fn strict_unknown_label_produces_error() {
        let (binder, _) = make_binder(OntologyMode::Strict);
        let ast = parse("MATCH (a:UnknownLabel) RETURN a").unwrap();
        let result = binder.bind(&ast);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.kind == BindErrorKind::UnknownLabel));
    }

    #[test]
    fn strict_mode_allows_durable_identity_fields() {
        let (binder, _) = make_binder(OntologyMode::Strict);
        let ast = parse(
            "MATCH (source)-[relationship]->(target) RETURN source.node_uuid, relationship.edge_uuid, target.node_uuid",
        )
        .unwrap();

        binder
            .bind(&ast)
            .expect("structural identity fields are not ontology properties");
    }

    #[test]
    fn strict_properties_emit_owner_scoped_runtime_ids() {
        let (binder, catalog) = strict_property_binder(false);
        let ast = parse("MATCH (host:Host)-[connection:R]->() RETURN host.direct, host.inherited, host.shared, connection.weight, connection.shared").unwrap();
        let plan = binder.bind(&ast).unwrap();
        let ids = plan_property_ids(&plan);
        let catalog = catalog.lock().unwrap();
        let names = ids
            .iter()
            .map(|id| catalog.property_name(crate::RuntimePropId(id.0)).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(names, ["direct", "inherited", "shared", "weight", "shared"]);
        assert!(ids.iter().all(|id| id.0 > 0));
        assert_ne!(ids[2], ids[4]);
    }

    #[test]
    fn strict_property_writes_and_inline_filters_share_owner_rules() {
        let (binder, catalog) = strict_property_binder(false);
        let ast = parse("MATCH (host:Host)-[connection:R]->() SET host.direct = 'ready', host.inherited = 'asset', connection.weight = 7 REMOVE host.direct, connection.weight").unwrap();
        let plan = binder.bind(&ast).unwrap();
        let catalog = catalog.lock().unwrap();
        let write_ids = plan
            .ops
            .iter()
            .flat_map(|op| match op {
                GraphOp::Set { items, .. } => items.iter().map(|item| item.prop).collect(),
                GraphOp::Remove { items, .. } => items.iter().map(|item| item.prop).collect(),
                _ => Vec::new(),
            })
            .collect::<Vec<_>>();
        assert_eq!(write_ids.len(), 5);
        assert!(
            write_ids
                .iter()
                .all(|id| catalog.property_name(crate::RuntimePropId(id.0)).is_some())
        );
        drop(catalog);

        for query in [
            "MATCH (:Host {direct: 'ready'})-[:R {weight: 7}]->() RETURN 1",
            "MATCH (:Host {inherited: 'asset'})-[:R*1..2 {weight: 7}]->() RETURN 1",
        ] {
            binder
                .bind(&parse(query).unwrap())
                .expect("anonymous fixed and variable-length owners should bind");
        }
    }

    #[test]
    fn strict_properties_reject_wrong_owner_and_ambiguity_with_exact_spans() {
        let (binder, _) = strict_property_binder(false);
        for (query, span) in [
            ("MATCH (host:Host) RETURN host.weight", "host.weight"),
            ("MATCH ()-[r:R]->() RETURN r.direct", "r.direct"),
            ("MATCH (host:Host) RETURN host.missing", "host.missing"),
            (
                "MATCH (host:Host) WITH host AS forwarded RETURN forwarded.weight",
                "forwarded.weight",
            ),
            (
                "MATCH ()-[r:R]->() WITH r AS forwarded RETURN forwarded.direct",
                "forwarded.direct",
            ),
        ] {
            property_error(&binder, query, BindErrorKind::UnknownProperty, span);
        }
        for (query, span) in [
            ("MATCH (:Host {weight: 7}) RETURN 1", "weight"),
            ("MATCH ()-[:R {missing: 7}]->() RETURN 1", "missing"),
        ] {
            property_error(&binder, query, BindErrorKind::UnknownProperty, span);
        }
        property_error(
            &binder,
            "MATCH ()-[*1..2 {weight: 7}]->() RETURN 1",
            BindErrorKind::AmbiguousProperty,
            "weight",
        );

        let (binder, _) = strict_property_binder(true);
        let query = "MATCH (host:Host) RETURN host.inherited";
        let error = property_error(
            &binder,
            query,
            BindErrorKind::AmbiguousProperty,
            "host.inherited",
        );
        assert!(error.message.contains("Asset, Host"));
        property_error(
            &binder,
            "MATCH (:Host {inherited: 7}) RETURN 1",
            BindErrorKind::AmbiguousProperty,
            "inherited",
        );
    }

    #[test]
    fn strict_mode_rejects_mismatched_durable_identity_fields() {
        for query in [
            "MATCH (node) RETURN node.edge_uuid",
            "MATCH ()-[relationship]->() RETURN relationship.node_uuid",
        ] {
            let (binder, _) = make_binder(OntologyMode::Strict);
            let errors = binder
                .bind(&parse(query).unwrap())
                .expect_err("identity fields must match the bound entity kind");
            assert!(errors.iter().any(|error| {
                error.kind == BindErrorKind::InvalidArgument
                    && error.message.contains("valid only on")
            }));
        }
    }

    #[test]
    fn structural_identity_fields_are_read_only() {
        for query in [
            "MATCH (node) SET node.node_uuid = 'replacement'",
            "MATCH (node) REMOVE node.node_uuid",
            "MATCH ()-[relationship]->() SET relationship.edge_uuid = 'replacement'",
            "MATCH ()-[relationship]->() REMOVE relationship.edge_uuid",
        ] {
            let (binder, _) = make_binder(OntologyMode::Strict);
            let errors = binder
                .bind(&parse(query).unwrap())
                .expect_err("identity fields must not be mutable");
            assert!(errors.iter().any(|error| {
                error.kind == BindErrorKind::InvalidArgument
                    && error.message.contains("is read-only")
            }));
        }
    }

    #[test]
    fn fixed_hop_with_known_relation_emits_expand() {
        use gf_ontology::{
            EntityTypeDef, OntologyCompiler, OntologyDoc, OntologyHandle, RelationTypeDef,
            SemanticFlags,
        };

        let doc = OntologyDoc {
            ontology_id: "test".into(),
            version: "1.0".into(),
            entity_types: vec![
                EntityTypeDef {
                    name: "Person".into(),
                    r#abstract: false,
                    parent: None,
                },
                EntityTypeDef {
                    name: "Organization".into(),
                    r#abstract: false,
                    parent: None,
                },
            ],
            relation_types: vec![RelationTypeDef {
                name: "WORKS_AT".into(),
                src: "Person".into(),
                dst: "Organization".into(),
                inverse: None,
                semantic: SemanticFlags::default(),
            }],
            properties: vec![],
            constraints: vec![],
            migrations: vec![],
        };
        let runtime = OntologyCompiler::compile(&doc).unwrap();
        let handle = OntologyHandle::new(runtime);
        let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
        let binder = Binder::new(Some(handle), catalog, OntologyMode::Strict);

        let ast = parse("MATCH (a:Person)-[:WORKS_AT]->(b:Organization) RETURN a").unwrap();
        let plan = binder.bind(&ast).expect("known ontology should succeed");

        // A fixed single hop lowers to a single `Expand` carrying both node vars
        // and the resolved relation type — never a bare edge scan (#718).
        let expand = plan.ops.iter().find_map(|op| match op {
            GraphOp::Expand {
                rel_ty,
                min_hops,
                max_hops,
                ..
            } => Some((rel_ty, *min_hops, *max_hops)),
            _ => None,
        });
        let (rel_ty, min_hops, max_hops) = expand.expect("fixed hop should emit Expand");
        assert!(
            rel_ty.is_some(),
            "WORKS_AT should resolve to a relation type"
        );
        assert_eq!((min_hops, max_hops), (1, Some(1)), "fixed hop is 1..1");
        assert!(
            !plan
                .ops
                .iter()
                .any(|op| matches!(op, GraphOp::TypedEdgeScan { .. } | GraphOp::EdgeScan { .. }))
        );
    }

    #[test]
    fn wildcard_fixed_hop_emits_untyped_expand() {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a)-[r]->(b) RETURN r").unwrap();
        let plan = binder.bind(&ast).expect("wildcard bind should succeed");

        // An untyped fixed hop is still an `Expand` (rel_ty None), not an
        // EdgeScan, so the source/destination nodes stay connected (#718).
        let expand = plan.ops.iter().find_map(|op| match op {
            GraphOp::Expand {
                rel_ty,
                min_hops,
                max_hops,
                ..
            } => Some((rel_ty, *min_hops, *max_hops)),
            _ => None,
        });
        let (rel_ty, min_hops, max_hops) = expand.expect("wildcard hop should emit Expand");
        assert!(rel_ty.is_none(), "wildcard hop has no relation type");
        assert_eq!((min_hops, max_hops), (1, Some(1)));
        assert!(
            !plan
                .ops
                .iter()
                .any(|op| matches!(op, GraphOp::TypedEdgeScan { .. } | GraphOp::EdgeScan { .. }))
        );
    }

    #[test]
    fn undeclared_variable_in_return_is_error() {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a:Person) RETURN x").unwrap();
        let result = binder.bind(&ast);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.kind == BindErrorKind::UndeclaredVariable)
        );
    }

    #[test]
    fn multi_error_collection_in_strict_mode() {
        let (binder, _) = make_binder(OntologyMode::Strict);
        let ast = parse("MATCH (a:LabelA)-[:REL_B]->(b:LabelC) RETURN a").unwrap();
        let result = binder.bind(&ast);
        assert!(result.is_err());
        let errors = result.unwrap_err();
        // LabelA, REL_B, LabelC → at least 3 errors
        assert!(
            errors.len() >= 3,
            "expected ≥3 errors, got {}",
            errors.len()
        );
    }

    #[test]
    fn where_clause_emits_filter_op() {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a:Person) WHERE a.age > 30 RETURN a").unwrap();
        let plan = binder.bind(&ast).unwrap();
        assert!(
            plan.ops
                .iter()
                .any(|op| matches!(op, GraphOp::Filter { .. }))
        );
    }

    #[test]
    fn inline_node_property_emits_filter() {
        // #748: an inline property map becomes a Filter over the scanned node.
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a:Person {name:'Alice'}) RETURN a.name").unwrap();
        let plan = binder.bind(&ast).unwrap();
        assert!(
            plan.ops
                .iter()
                .any(|op| matches!(op, GraphOp::NodeScan { .. })),
            "scan present"
        );
        assert!(
            plan.ops
                .iter()
                .any(|op| matches!(op, GraphOp::Filter { .. })),
            "inline property must emit a Filter"
        );
    }

    #[test]
    fn inline_multi_property_emits_single_filter() {
        // Multiple inline properties AND-combine into one Filter op.
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a:Person {name:'Alice', age:30}) RETURN a.name").unwrap();
        let plan = binder.bind(&ast).unwrap();
        let filters = plan
            .ops
            .iter()
            .filter(|op| matches!(op, GraphOp::Filter { .. }))
            .count();
        assert_eq!(filters, 1, "multi-property map → one AND-ed Filter");
    }

    #[test]
    fn node_without_inline_properties_emits_no_filter() {
        // Regression: a property-free node pattern must not add a spurious Filter.
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a:Person) RETURN a.name").unwrap();
        let plan = binder.bind(&ast).unwrap();
        assert!(
            !plan
                .ops
                .iter()
                .any(|op| matches!(op, GraphOp::Filter { .. })),
            "no inline properties → no Filter"
        );
    }

    #[test]
    fn inline_rel_property_emits_filter() {
        // #750: an inline relationship-property map becomes a Filter over the
        // just-expanded edge, mirroring the node case (#748).
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast =
            parse("MATCH (a:Person)-[r:KNOWS {since:2020}]->(b:Person) RETURN b.name").unwrap();
        let plan = binder.bind(&ast).unwrap();
        assert!(
            plan.ops
                .iter()
                .any(|op| matches!(op, GraphOp::Expand { .. })),
            "expand present"
        );
        assert!(
            plan.ops
                .iter()
                .any(|op| matches!(op, GraphOp::Filter { .. })),
            "inline relationship property must emit a Filter"
        );
    }

    #[test]
    fn inline_multi_rel_property_emits_single_filter() {
        // Multiple inline rel properties AND-combine into one Filter op.
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast =
            parse("MATCH (a:Person)-[r:KNOWS {since:2020, weight:5}]->(b:Person) RETURN b.name")
                .unwrap();
        let plan = binder.bind(&ast).unwrap();
        let filters = plan
            .ops
            .iter()
            .filter(|op| matches!(op, GraphOp::Filter { .. }))
            .count();
        assert_eq!(filters, 1, "multi-property rel map → one AND-ed Filter");
    }

    #[test]
    fn rel_without_inline_properties_emits_no_filter() {
        // Regression: a property-free relationship must not add a spurious Filter.
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN b.name").unwrap();
        let plan = binder.bind(&ast).unwrap();
        assert!(
            !plan
                .ops
                .iter()
                .any(|op| matches!(op, GraphOp::Filter { .. })),
            "no inline rel properties → no Filter"
        );
    }

    #[test]
    fn inline_property_on_var_length_rel_emits_all_filter() {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a:Person)-[r:KNOWS*1..2 {since:2020}]->(b:Person) RETURN b.name")
            .unwrap();
        let plan = binder.bind(&ast).unwrap();
        let predicate = plan.ops.iter().find_map(|op| match op {
            GraphOp::Filter { predicate } => Some(*predicate),
            _ => None,
        });
        assert!(
            predicate.is_some_and(|predicate| matches!(
                plan.exprs.get(predicate),
                IrExpr::Quantifier {
                    kind: gf_ast::QuantifierKind::All,
                    ..
                }
            )),
            "variable-length relationship properties require an all() filter"
        );
    }

    #[test]
    fn return_clause_emits_project_op() {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (a:Person) RETURN a.name").unwrap();
        let plan = binder.bind(&ast).unwrap();
        assert!(
            plan.ops
                .iter()
                .any(|op| matches!(op, GraphOp::Project { .. }))
        );
    }

    #[test]
    fn return_with_count_emits_aggregate() {
        // `RETURN count(n)` lowers to an Aggregate (one Count agg, no group keys),
        // not a Project (#729).
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (n:Person) RETURN count(n) AS total").unwrap();
        let plan = binder.bind(&ast).unwrap();
        let agg = plan.ops.iter().find_map(|op| match op {
            GraphOp::Aggregate { group_by, aggs, .. } => Some((group_by, aggs)),
            _ => None,
        });
        let (group_by, aggs) = agg.expect("count should emit an Aggregate");
        assert!(
            group_by.is_empty(),
            "no non-aggregate items → no group keys"
        );
        assert_eq!(aggs.len(), 1);
        assert_eq!(aggs[0].func, AggFunc::Count);
        assert_eq!(aggs[0].alias, "total");
        assert!(
            !plan
                .ops
                .iter()
                .any(|op| matches!(op, GraphOp::Project { .. })),
            "an aggregate RETURN must not also emit a Project"
        );
    }

    #[test]
    fn return_with_grouping_keys_and_aggregate() {
        // `RETURN n.name, count(n)` groups by the non-aggregate item.
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH (n:Person) RETURN n.name AS name, count(n) AS total").unwrap();
        let plan = binder.bind(&ast).unwrap();
        let (group_by, aggs) = plan
            .ops
            .iter()
            .find_map(|op| match op {
                GraphOp::Aggregate { group_by, aggs, .. } => Some((group_by, aggs)),
                _ => None,
            })
            .expect("Aggregate");
        assert_eq!(group_by.len(), 1, "n.name is the group key");
        assert_eq!(aggs.len(), 1);
    }

    // -----------------------------------------------------------------------
    // Named path variables (#754)
    // -----------------------------------------------------------------------

    /// Bind `query` and return the errors (panics if the bind succeeds).
    fn bind_errors(query: &str) -> Vec<BindError> {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse(query).unwrap();
        binder
            .bind(&ast)
            .expect_err("bind should fail for this query")
    }

    #[test]
    fn path_var_functions_bind_with_anonymous_edge() {
        // The binder allocates an anon VarId for `[*1..2]`, so the rewrites
        // have an edge var to target even without `[r:...]`.
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse(
            "MATCH p = (a:Person)-[*1..2]->(b) \
             RETURN nodes(p) AS ns, relationships(p) AS rs, length(p) AS l",
        )
        .unwrap();
        binder.bind(&ast).expect("path functions should bind");
    }

    #[test]
    fn path_function_on_non_path_falls_through() {
        // `length(r)` on a var-length edge var keeps its generic lowering and
        // `nodes(a)` on a node var stays a generic function call — neither is
        // intercepted by the path rewrite.
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast =
            parse("MATCH (a)-[r:KNOWS*1..2]->(b) RETURN length(r) AS l, nodes(a) AS ns").unwrap();
        let plan = binder.bind(&ast).expect("bind succeeds");
        let names: Vec<&str> = (0..plan.exprs.len())
            .filter_map(
                |i| match plan.exprs.get(ExprId(u32::try_from(i).unwrap())) {
                    IrExpr::FunctionCall { name, .. } => Some(name.as_str()),
                    _ => None,
                },
            )
            .collect();
        assert!(names.contains(&"length"), "generic length kept: {names:?}");
        assert!(names.contains(&"nodes"), "generic nodes kept: {names:?}");
        assert!(
            !names.contains(&"_path_nodes"),
            "no path rewrite without a path var: {names:?}"
        );
    }

    #[test]
    fn bare_path_var_binds_to_path_struct() {
        // `RETURN p` rewrites to `_path_struct(<nodes>, <relationships>)`.
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH p = (a)-[:KNOWS*1..2]->(b) RETURN p").unwrap();
        let plan = binder.bind(&ast).expect("bare path value binds");
        let has_struct = (0..plan.exprs.len()).any(|i| {
            matches!(
                plan.exprs.get(ExprId(u32::try_from(i).unwrap())),
                IrExpr::FunctionCall { name, .. } if name == "_path_struct"
            )
        });
        assert!(has_struct, "RETURN p must rewrite to _path_struct");
    }

    #[test]
    fn multi_segment_path_var_composes_path_functions() {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse(
            "MATCH p = (a)-[:KNOWS]->(b)-[:KNOWS]->(c) \
             RETURN length(p), nodes(p), relationships(p)",
        )
        .unwrap();
        let plan = binder.bind(&ast).expect("multi-segment path binds");
        let add_count = (0..plan.exprs.len())
            .map(|index| plan.exprs.get(ExprId(index as u32)))
            .filter(|expr| {
                matches!(
                    expr,
                    IrExpr::BinaryOp {
                        op: BinaryOpKind::Add,
                        ..
                    }
                )
            })
            .count();
        assert!(add_count >= 3, "each path function composes its segments");
    }

    #[test]
    fn fixed_segment_path_functions_bind() {
        // A fixed single hop composes from scalar edge/node columns:
        // length → _path_fixed_length, nodes → _node_struct_list,
        // relationships → _rel_struct_list.
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse(
            "MATCH p = (a)-[:KNOWS]->(b) \
             RETURN length(p) AS l, nodes(p) AS ns, relationships(p) AS rs",
        )
        .unwrap();
        let plan = binder.bind(&ast).expect("fixed path functions bind");
        let names: Vec<&str> = (0..plan.exprs.len())
            .filter_map(
                |i| match plan.exprs.get(ExprId(u32::try_from(i).unwrap())) {
                    IrExpr::FunctionCall { name, .. } => Some(name.as_str()),
                    _ => None,
                },
            )
            .collect();
        for expected in [
            "_path_fixed_length",
            "_node_struct_list",
            "_rel_struct_list",
        ] {
            assert!(names.contains(&expected), "missing {expected}: {names:?}");
        }
    }

    #[test]
    fn explicit_one_hop_routes_as_fixed_segment() {
        // `*1..1` goes to the relational join (no list column), so the path
        // rewrite must treat it as a fixed segment too.
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse("MATCH p = (a)-[:KNOWS*1..1]->(b) RETURN length(p) AS l").unwrap();
        let plan = binder.bind(&ast).expect("explicit 1..1 binds as fixed");
        let has_fixed = (0..plan.exprs.len()).any(|i| {
            matches!(
                plan.exprs.get(ExprId(u32::try_from(i).unwrap())),
                IrExpr::FunctionCall { name, .. } if name == "_path_fixed_length"
            )
        });
        assert!(
            has_fixed,
            "explicit *1..1 must use the fixed-segment rewrite"
        );
    }

    #[test]
    fn path_var_name_conflict_is_rejected() {
        let errors = bind_errors("MATCH p = (p)-[:KNOWS*1..2]->(b) RETURN length(p)");
        assert!(
            errors
                .iter()
                .any(|e| matches!(e.kind, BindErrorKind::DuplicateVariable)),
            "expected DuplicateVariable, got {errors:?}"
        );
    }

    #[test]
    fn optional_match_path_var_functions_bind() {
        // path_vars introduced inside OPTIONAL MATCH must propagate to the
        // outer scope so a later RETURN can rewrite against them.
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let ast = parse(
            "MATCH (a:Person) \
             OPTIONAL MATCH p = (a)-[:KNOWS*1..2]->(b) \
             RETURN nodes(p) AS ns, length(p) AS l",
        )
        .unwrap();
        binder.bind(&ast).expect("optional path functions bind");
    }

    fn procedure_binder() -> Binder {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let procedure = ProcedureDefinition {
            name: "test.proc".into(),
            inputs: vec![ProcedureField {
                name: "in".into(),
                type_name: "INTEGER".into(),
                nullable: true,
            }],
            outputs: vec![ProcedureField {
                name: "out".into(),
                type_name: "INTEGER".into(),
                nullable: true,
            }],
            rows: vec![vec![IrLiteral::Int(1), IrLiteral::Int(2)]],
        };
        binder.with_procedures(Arc::new(ProcedureRegistry::from([(
            procedure.name.clone(),
            procedure,
        )])))
    }

    #[test]
    fn call_binds_explicit_args_and_yield_alias() {
        let plan = procedure_binder()
            .bind(&parse("CALL test.proc(1) YIELD out AS value RETURN value").unwrap())
            .expect("CALL should bind");
        let GraphOp::Call { args, yields, .. } = &plan.ops[0] else {
            panic!("expected CALL op")
        };
        assert_eq!(args.len(), 1);
        assert_eq!(yields[0].field, "out");
        assert_eq!(yields[0].alias, "value");
    }

    #[test]
    fn call_without_parentheses_uses_implicit_parameters() {
        let plan = procedure_binder()
            .bind(&parse("CALL test.proc YIELD out").unwrap())
            .expect("implicit CALL should bind");
        let GraphOp::Call { args, .. } = &plan.ops[0] else {
            panic!("expected CALL op")
        };
        assert!(matches!(plan.exprs.get(args[0]), IrExpr::Parameter(name) if name == "in"));
    }

    #[test]
    fn call_rejects_unknown_procedure_argument_count_and_yield() {
        for (query, message) in [
            ("CALL missing.proc()", "ProcedureNotFound"),
            ("CALL test.proc()", "InvalidNumberOfArguments"),
            ("CALL test.proc(1) YIELD missing", "ProcedureOutputNotFound"),
        ] {
            let errors = procedure_binder()
                .bind(&parse(query).unwrap())
                .expect_err("CALL should fail");
            assert!(
                errors.iter().any(|error| error.message.contains(message)),
                "expected {message}, got {errors:?}"
            );
        }
    }

    #[test]
    fn union_binds_branch_plans_and_mode() {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let plan = binder
            .bind(&parse("RETURN 1 AS x UNION ALL RETURN 2 AS x").unwrap())
            .expect("UNION ALL binds");
        let [GraphOp::Union { all, inputs }] = plan.ops.as_slice() else {
            panic!("expected one UNION op")
        };
        assert!(*all);
        assert_eq!(inputs.len(), 2);
        assert!(inputs.iter().all(|branch| !branch.ops.is_empty()));
    }

    #[test]
    fn union_rejects_mixed_modes_and_different_columns() {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        for (query, message) in [
            (
                "RETURN 1 AS x UNION RETURN 2 AS x UNION ALL RETURN 3 AS x",
                "InvalidCombinationOfUnion",
            ),
            (
                "RETURN 1 AS x UNION RETURN 2 AS y",
                "DifferentColumnsInUnion",
            ),
            ("CREATE (:A) UNION CREATE (:B)", "DifferentColumnsInUnion"),
        ] {
            let errors = binder
                .bind(&parse(query).unwrap())
                .expect_err("UNION should fail");
            assert!(errors.iter().any(|error| error.message.contains(message)));
        }
    }

    #[test]
    fn return_wildcard_requires_a_variable_in_scope() {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let errors = binder
            .bind(&parse("RETURN *").unwrap())
            .expect_err("empty-scope RETURN wildcard must fail");
        assert!(errors.iter().any(|error| {
            error
                .message
                .contains("wildcard requires at least one variable")
        }));
        assert_ne!(errors[0].span, Span::default());
    }

    #[test]
    fn with_wildcard_preserves_an_empty_named_scope() {
        let (binder, _) = make_binder(OntologyMode::Exploratory);
        let plan = binder
            .bind(&parse("CREATE () WITH * CREATE ()").unwrap())
            .expect("empty-scope WITH wildcard should preserve pipeline rows");
        assert_eq!(plan.ops.len(), 3);
        assert!(matches!(&plan.ops[1], GraphOp::With { items, .. } if items.is_empty()));
    }

    #[test]
    fn typed_uuid_parameters_are_identity_only_across_expression_surfaces() {
        let params = HashMap::from([("id".into(), IrLiteral::Uuid([0x55; 16]))]);
        for query in [
            "MATCH (n:Person) WHERE NOT (n.node_uuid = $id) RETURN n",
            "MATCH (n:Person) WHERE n.node_uuid <> $id RETURN n",
            "MATCH (n:Person) WHERE n.node_uuid > $id RETURN n",
            "MATCH (n:Person) WHERE n.node_uuid IN [$id] RETURN n",
            "RETURN $id",
            "RETURN toString($id)",
            "RETURN size([$id])",
            "RETURN [$id]",
            "RETURN 1 AS value SKIP $id",
            "RETURN 1 AS value LIMIT $id",
            "MATCH (n:Person) WHERE (($id = n.name) OR false) RETURN n",
            "MATCH (n:Person) WHERE n.name IN [$id] RETURN n",
            "MATCH (n:Person) RETURN n.name = $id AS bad",
            "MATCH (n:Person) WITH n, n.name = $id AS bad RETURN bad",
            "MATCH (n:Person) RETURN n.name AS name ORDER BY n.name = $id",
            "MATCH (n:Person) UNWIND [n.name = $id] AS bad RETURN bad",
            "MATCH (n:Person {probe: n.name = $id}) RETURN n",
            "MATCH (n:Person) SET n.probe = (n.name = $id) RETURN n",
            "MATCH (n:Person) MERGE (m:Other {probe: n.name = $id}) RETURN m",
            "MATCH (n:Person) DELETE (n.name = $id)",
            "MATCH (n:Person)-[r:KNOWS]->() WHERE r.node_uuid = $id RETURN r",
            "MATCH (n:Person)-[r:KNOWS]->() WHERE n.edge_uuid = $id RETURN n",
        ] {
            let (binder, _) = make_binder(OntologyMode::Exploratory);
            let errors = binder
                .with_parameter_literals(&params)
                .bind(&parse(query).unwrap())
                .expect_err("typed UUID must not reach a non-identity expression");
            assert!(
                errors.iter().any(|error| {
                    error.kind == BindErrorKind::InvalidArgument
                        && error.message
                            == "typed UUID parameter `$id` is only supported as a direct node_uuid or edge_uuid identity equality predicate"
                }),
                "query={query} errors={errors:?}"
            );
        }

        let errors = procedure_binder()
            .with_parameter_literals(&params)
            .bind(
                &parse("MATCH (n:Person) CALL test.proc(n.name = $id) YIELD out RETURN out")
                    .unwrap(),
            )
            .expect_err("CALL argument must enforce typed UUID identity semantics");
        assert!(
            errors.iter().any(|error| {
                error.kind == BindErrorKind::InvalidArgument
                    && error.message.starts_with("typed UUID parameter `$id`")
            }),
            "errors={errors:?}"
        );
    }

    #[test]
    fn typed_uuid_parameters_allow_only_kind_correct_identity_fields() {
        let params = HashMap::from([("id".into(), IrLiteral::Uuid([0x55; 16]))]);
        for query in [
            "MATCH (n:Person) WHERE n.node_uuid = $id RETURN n.node_uuid",
            "MATCH (n:Person) WHERE $id = n.node_uuid RETURN n.node_uuid",
            "MATCH ()-[r:KNOWS]->() WHERE r.edge_uuid = $id RETURN r.edge_uuid",
            "MATCH ()-[r:KNOWS]->() WHERE $id = r.edge_uuid RETURN r.edge_uuid",
        ] {
            let (binder, _) = make_binder(OntologyMode::Exploratory);
            binder
                .with_parameter_literals(&params)
                .bind(&parse(query).unwrap())
                .unwrap_or_else(|errors| panic!("query={query} errors={errors:?}"));
        }

        for nested in [
            IrLiteral::List(vec![IrLiteral::Uuid([0x55; 16])]),
            IrLiteral::Map(vec![(
                "nested".into(),
                IrLiteral::List(vec![IrLiteral::Uuid([0x55; 16])]),
            )]),
        ] {
            let (binder, _) = make_binder(OntologyMode::Exploratory);
            let errors = binder
                .with_parameter_literals(&HashMap::from([("id".into(), nested)]))
                .bind(
                    &parse("MATCH (n:Person) WHERE n.node_uuid = $id RETURN n.node_uuid").unwrap(),
                )
                .expect_err("containers containing UUID values are never identity scalars");
            assert!(errors.iter().any(|error| {
                error.kind == BindErrorKind::InvalidArgument
                    && error.message.starts_with("typed UUID parameter `$id`")
            }));
        }
    }
}
