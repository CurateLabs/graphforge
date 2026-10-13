//! Which stored properties a read plan consumes for each node variable.
//!
//! Binding a node variable to its property table (the node enrichment) costs
//! one column read per stored property. [`node_property_demand`] reports, per
//! variable, the property columns the rest of the plan can observe, so a
//! lowerer reads only those. A variable the plan uses only by identity (count
//! it, collect it, compare it, test its labels) demands no property at all.
//!
//! The analysis is conservative: a variable demands every property unless each
//! use of it is one of the identity-only forms below. Demand is tracked per
//! variable number across the whole plan, nested plans included, so a number a
//! child scope reuses only widens the answer. Variables linked by an alias
//! (`WITH n AS m`, a group key that keeps its variable) share one answer.
//!
//! A bare node variable is identity-only in exactly these positions:
//!
//! - the argument of `count`, `count(DISTINCT ..)`, `collect` and
//!   `collect(DISTINCT ..)` (a collected node whose value is observed is bound
//!   as `_node_struct(n)`, which is a use of the whole node);
//! - an operand of `=` or `<>` whose other operand is also a bare variable
//!   (the comparison is on node identity);
//! - the left operand of `IN`;
//! - the argument of `labels`;
//! - the operand of `IS NULL` and `IS NOT NULL`;
//! - a `WITH` item or group key that keeps the variable under another name.
//!
//! Every other use, `RETURN n`, `properties(n)`, `n + 1`, `ORDER BY n`, a
//! path, a write or a procedure call included, demands the whole node.

use std::collections::{BTreeSet, HashMap};
use std::hash::BuildHasher;

use crate::{ExprArena, ExprId, GraphOp, GraphPlan, IrExpr, PropertyId, VarId};

/// What a plan reads from one node variable's stored properties.
#[derive(Debug, Clone, PartialEq, Eq)]
enum VarDemand {
    /// The whole node is observed: every property column.
    All,
    /// Exactly these property columns are observed.
    Names(BTreeSet<String>),
}

impl VarDemand {
    fn merge(&mut self, other: Self) {
        match (&mut *self, other) {
            (Self::All, _) => {}
            (this, Self::All) => *this = Self::All,
            (Self::Names(names), Self::Names(more)) => names.extend(more),
        }
    }

    fn needs(&self, column: &str) -> bool {
        match self {
            Self::All => true,
            Self::Names(names) => names.contains(column),
        }
    }
}

/// The stored property columns a read plan consumes, per node variable.
///
/// A variable the plan never reads a property of, or observes whole, demands
/// no column. [`NodePropertyDemand::everything`] is the answer for a plan the
/// analysis was not run on, and the answer to give whenever the plan being
/// lowered is not the whole statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePropertyDemand {
    /// Every variable demands every column (an operator the analysis does not
    /// model, such as a write or a procedure call, can read any node).
    all: bool,
    /// The merged demand of each alias class, keyed by every member.
    vars: HashMap<VarId, VarDemand>,
}

impl Default for NodePropertyDemand {
    fn default() -> Self {
        Self::everything()
    }
}

impl NodePropertyDemand {
    /// Every variable demands every property column.
    #[must_use]
    pub fn everything() -> Self {
        Self {
            all: true,
            vars: HashMap::new(),
        }
    }

    /// Whether the plan can observe property column `column` of node variable
    /// `var`.
    #[must_use]
    pub fn needs(&self, var: VarId, column: &str) -> bool {
        self.all
            || self
                .vars
                .get(&var)
                .is_some_and(|demand| demand.needs(column))
    }
}

/// The node property demand of `plan`, given the catalog's `PropertyId → name`
/// map (the names are the stored column names).
#[must_use]
pub fn node_property_demand<S: BuildHasher>(
    plan: &GraphPlan,
    property_names: &HashMap<PropertyId, String, S>,
) -> NodePropertyDemand {
    let mut collector = Collector {
        property_names,
        all: false,
        demands: HashMap::new(),
        links: Vec::new(),
    };
    collector.plan(plan);
    collector.finish()
}

struct Collector<'a, S> {
    property_names: &'a HashMap<PropertyId, String, S>,
    all: bool,
    demands: HashMap<VarId, VarDemand>,
    /// Pairs of variables that name the same node.
    links: Vec<(VarId, VarId)>,
}

impl<S: BuildHasher> Collector<'_, S> {
    fn demand(&mut self, var: VarId, demand: VarDemand) {
        match self.demands.get_mut(&var) {
            Some(existing) => existing.merge(demand),
            None => {
                self.demands.insert(var, demand);
            }
        }
    }

    fn whole(&mut self, var: VarId) {
        self.demand(var, VarDemand::All);
    }

    fn column(&mut self, var: VarId, name: String) {
        self.demand(var, VarDemand::Names(BTreeSet::from([name])));
    }

    fn plan(&mut self, plan: &GraphPlan) {
        let exprs = &plan.exprs;
        for op in &plan.ops {
            match op {
                GraphOp::NodeScan { .. }
                | GraphOp::EdgeScan { .. }
                | GraphOp::TypedEdgeScan { .. }
                | GraphOp::Expand { .. }
                | GraphOp::RelationshipUnique { .. }
                | GraphOp::Limit { .. }
                | GraphOp::LimitParam { .. }
                | GraphOp::Skip { .. }
                | GraphOp::SkipParam { .. } => {}
                GraphOp::Filter { predicate } => self.value(exprs, *predicate),
                GraphOp::Project { items, .. } => {
                    for item in items {
                        self.value(exprs, item.expr);
                    }
                }
                GraphOp::With {
                    items,
                    where_predicate,
                    ..
                } => {
                    for item in items {
                        match exprs.get(item.expr) {
                            // `WITH n AS m` keeps the node under a new name;
                            // `WITH n` keeps both the name and the node.
                            IrExpr::VarRef(var) => {
                                if let Some(out) = item.out_var
                                    && out != *var
                                {
                                    self.links.push((*var, out));
                                }
                            }
                            _ => self.value(exprs, item.expr),
                        }
                    }
                    if let Some(predicate) = where_predicate {
                        self.value(exprs, *predicate);
                    }
                }
                GraphOp::Aggregate {
                    group_by,
                    group_vars,
                    aggs,
                    ..
                } => {
                    for (index, key) in group_by.iter().enumerate() {
                        match exprs.get(*key) {
                            // A node group key that keeps its own variable is
                            // passed through under the same name.
                            IrExpr::VarRef(var)
                                if group_vars.get(index).copied().flatten() == Some(*var) => {}
                            _ => self.value(exprs, *key),
                        }
                    }
                    for agg in aggs {
                        if let Some(arg) = agg.arg {
                            if identity_only_aggregate(agg.func) {
                                self.identity_or_value(exprs, arg);
                            } else {
                                self.value(exprs, arg);
                            }
                        }
                        if let Some(percentile) = agg.percentile {
                            self.value(exprs, percentile);
                        }
                    }
                }
                GraphOp::Sort { keys } => {
                    for key in keys {
                        self.value(exprs, key.expr);
                    }
                }
                GraphOp::LimitExpr { expr } | GraphOp::SkipExpr { expr } => {
                    self.value(exprs, *expr);
                }
                GraphOp::Unwind { list_expr, .. } => self.value(exprs, *list_expr),
                GraphOp::Optional { child }
                | GraphOp::Exists { child, .. }
                | GraphOp::PatternComprehension { child, .. } => self.plan(child),
                GraphOp::Union { inputs, .. } => {
                    for input in inputs {
                        self.plan(input);
                    }
                }
                // Writes, procedure calls, graph-valued list comprehensions
                // and any operator added later can read any node whole.
                _ => self.all = true,
            }
        }
    }

    /// `id` is an operand whose bare-variable form is identity-only.
    fn identity_or_value(&mut self, exprs: &ExprArena, id: ExprId) {
        if !matches!(exprs.get(id), IrExpr::VarRef(_)) {
            self.value(exprs, id);
        }
    }

    /// `id` is evaluated as a value: a bare variable is observed whole and a
    /// property access observes one property.
    fn value(&mut self, exprs: &ExprArena, id: ExprId) {
        match exprs.get(id) {
            IrExpr::Literal(_) | IrExpr::Parameter(_) => {}
            IrExpr::VarRef(var) => self.whole(*var),
            IrExpr::PropertyAccess { base, prop } => match exprs.get(*base) {
                IrExpr::VarRef(var) => match self.property_names.get(prop) {
                    Some(name) => self.column(*var, name.clone()),
                    // A property the catalog cannot name could be any column.
                    None => self.whole(*var),
                },
                _ => self.value(exprs, *base),
            },
            IrExpr::BinaryOp { op, left, right } => {
                use crate::BinaryOpKind::{Eq, In, Neq};
                let bare = |id: &ExprId| matches!(exprs.get(*id), IrExpr::VarRef(_));
                match op {
                    Eq | Neq if bare(left) && bare(right) => {}
                    In => {
                        self.identity_or_value(exprs, *left);
                        self.value(exprs, *right);
                    }
                    _ => {
                        self.value(exprs, *left);
                        self.value(exprs, *right);
                    }
                }
            }
            IrExpr::UnaryOp { op, expr } => {
                use crate::UnaryOpKind::{IsNotNull, IsNull};
                if matches!(op, IsNull | IsNotNull) {
                    self.identity_or_value(exprs, *expr);
                } else {
                    self.value(exprs, *expr);
                }
            }
            IrExpr::FunctionCall { name, args } => {
                if name == "labels" && args.len() == 1 {
                    self.identity_or_value(exprs, args[0]);
                } else {
                    for arg in args {
                        self.value(exprs, *arg);
                    }
                }
            }
            IrExpr::Case {
                operand,
                arms,
                else_expr,
            } => {
                for id in operand.iter().chain(else_expr) {
                    self.value(exprs, *id);
                }
                for arm in arms {
                    self.value(exprs, arm.when);
                    self.value(exprs, arm.then);
                }
            }
            IrExpr::ListLiteral(items) => {
                for item in items {
                    self.value(exprs, *item);
                }
            }
            IrExpr::MapLiteral(entries) => {
                for (_, entry) in entries {
                    self.value(exprs, *entry);
                }
            }
            IrExpr::Quantifier {
                list, predicate, ..
            } => {
                self.value(exprs, *list);
                self.value(exprs, *predicate);
            }
            IrExpr::ListComprehension {
                list,
                filter,
                projection,
                ..
            } => {
                self.value(exprs, *list);
                for id in filter.iter().chain(projection) {
                    self.value(exprs, *id);
                }
            }
        }
    }

    /// Merge each alias class's demand into every member.
    fn finish(self) -> NodePropertyDemand {
        let Self {
            all,
            demands,
            links,
            ..
        } = self;
        let mut classes = Classes::default();
        for var in demands.keys() {
            classes.add(*var);
        }
        for (a, b) in &links {
            classes.union(*a, *b);
        }
        let mut merged: HashMap<VarId, VarDemand> = HashMap::new();
        for (var, demand) in demands {
            let root = classes.find(var);
            match merged.get_mut(&root) {
                Some(existing) => existing.merge(demand),
                None => {
                    merged.insert(root, demand);
                }
            }
        }
        let mut vars = HashMap::new();
        for var in classes.members() {
            if let Some(demand) = merged.get(&classes.find(var)) {
                vars.insert(var, demand.clone());
            }
        }
        NodePropertyDemand { all, vars }
    }
}

/// Aggregates whose argument is a node observed only by identity when it is a
/// bare variable.
fn identity_only_aggregate(func: crate::AggFunc) -> bool {
    use crate::AggFunc::{Collect, CollectDistinct, Count, CountDistinct};
    matches!(func, Count | CountDistinct | Collect | CollectDistinct)
}

/// Union-find over variable numbers.
#[derive(Default)]
struct Classes {
    parent: HashMap<VarId, VarId>,
}

impl Classes {
    fn add(&mut self, var: VarId) {
        self.parent.entry(var).or_insert(var);
    }

    fn find(&mut self, var: VarId) -> VarId {
        self.add(var);
        let mut root = var;
        while self.parent[&root] != root {
            root = self.parent[&root];
        }
        let mut current = var;
        while self.parent[&current] != root {
            let next = self.parent[&current];
            self.parent.insert(current, root);
            current = next;
        }
        root
    }

    fn union(&mut self, a: VarId, b: VarId) {
        let (a, b) = (self.find(a), self.find(b));
        if a != b {
            self.parent.insert(a, b);
        }
    }

    fn members(&self) -> Vec<VarId> {
        self.parent.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests;
