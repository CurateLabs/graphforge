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
//!
//! # Late columns
//!
//! A plan that ends `.. Sort, Project, [Skip,] Limit` reads some columns only
//! to build its returned rows: nothing before the `Limit` filters, orders or
//! groups on them. [`NodePropertyDemand::needs_late`] names those columns, so
//! a lowerer reads them for the rows the `Limit` keeps instead of for every
//! row that reaches it. A column is late only when all of these hold for its
//! variable:
//!
//! - the variable is scanned once in the whole plan, in the run of scans,
//!   expansions, filters and sorts directly below the final `Project`;
//! - it is not renamed or passed through a `WITH`, an aggregate group key or
//!   an alias that shares its demand;
//! - nothing observes it whole, before or in the `Project`;
//! - the final `Project` is the only reader of the column.
//!
//! Every other column of the variable is early: [`NodePropertyDemand::needs_early`].

use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::BuildHasher;

use crate::binder::node_struct_operand;
use crate::{AggFunc, ExprArena, ExprId, GraphOp, GraphPlan, IrExpr, PropertyId, VarId};

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
    /// The merged early demand of each alias class, keyed by every member.
    vars: HashMap<VarId, VarDemand>,
    /// The columns only the final projection reads, per deferrable variable.
    late: HashMap<VarId, BTreeSet<String>>,
    /// The index of the final `Project` when some column is late.
    deferred_projection: Option<usize>,
    /// The nodes whose collected value is read only through an `UNWIND`, so
    /// the collected struct carries just the columns the unwound rows use.
    narrowed_collects: HashSet<VarId>,
}

impl Default for NodePropertyDemand {
    fn default() -> Self {
        Self::everything()
    }
}

impl NodePropertyDemand {
    /// Every variable demands every property column, and none is late.
    #[must_use]
    pub fn everything() -> Self {
        Self {
            all: true,
            vars: HashMap::new(),
            late: HashMap::new(),
            deferred_projection: None,
            narrowed_collects: HashSet::new(),
        }
    }

    /// Whether the plan can observe property column `column` of node variable
    /// `var`, early or late.
    #[must_use]
    pub fn needs(&self, var: VarId, column: &str) -> bool {
        self.needs_early(var, column) || self.needs_late(var, column)
    }

    /// Whether the plan observes `column` of `var` before its final
    /// projection: in a filter, a sort key, a group key, an aggregate, a
    /// later pattern or any other operator.
    #[must_use]
    pub fn needs_early(&self, var: VarId, column: &str) -> bool {
        self.all
            || self
                .vars
                .get(&var)
                .is_some_and(|demand| demand.needs(column))
    }

    /// Whether `column` of `var` is observed only by the final projection, so
    /// it can be read after the plan's `Limit` for the rows it keeps.
    #[must_use]
    pub fn needs_late(&self, var: VarId, column: &str) -> bool {
        !self.all
            && self
                .late
                .get(&var)
                .is_some_and(|columns| columns.contains(column))
    }

    /// Whether any column of `var` is late.
    #[must_use]
    pub fn defers(&self, var: VarId) -> bool {
        !self.all
            && self
                .late
                .get(&var)
                .is_some_and(|columns| !columns.is_empty())
    }

    /// The index in the plan's operators of the final `Project`, when some
    /// variable has a late column. The `Project` is followed by the plan's
    /// optional `Skip` and its `Limit`, which are the last operators.
    #[must_use]
    pub fn deferred_projection(&self) -> Option<usize> {
        self.deferred_projection
    }

    /// Whether `var` is collected as a node value (`collect(n)` bound as
    /// `_node_struct(n)`) that the plan only unwinds, so the collected struct
    /// holds exactly the columns [`NodePropertyDemand::needs`] names for
    /// `var` instead of every stored property.
    #[must_use]
    pub fn narrows_collected_value(&self, var: VarId) -> bool {
        !self.all && self.narrowed_collects.contains(&var)
    }
}

/// The node property demand of `plan`, given the catalog's `PropertyId → name`
/// map (the names are the stored column names).
#[must_use]
pub fn node_property_demand<S: BuildHasher>(
    plan: &GraphPlan,
    property_names: &HashMap<PropertyId, String, S>,
) -> NodePropertyDemand {
    let mut base = Collector::new(property_names);
    let Some(deferral) = Deferral::of(plan) else {
        base.plan(plan);
        return base.finish(HashMap::new(), None);
    };
    // The final projection is collected on its own: what only it reads is late.
    let mut projection = Collector::new(property_names);
    for (index, op) in plan.ops.iter().enumerate() {
        if index == deferral.project {
            projection.op(&plan.exprs, op);
        } else {
            base.op(&plan.exprs, op);
        }
    }
    let mut late: HashMap<VarId, BTreeSet<String>> = HashMap::new();
    let projected = std::mem::take(&mut projection.demands);
    for (var, demand) in projected {
        let early = base.demands.get(&var);
        let deferrable = !base.all
            && deferral.scanned.contains(&var)
            && base.scans.get(&var) == Some(&1)
            && !base.links.iter().any(|(a, b)| *a == var || *b == var)
            && !matches!(early, Some(VarDemand::All));
        match demand {
            VarDemand::Names(columns) if deferrable => {
                let columns: BTreeSet<String> = columns
                    .into_iter()
                    .filter(|column| !early.is_some_and(|early| early.needs(column)))
                    .collect();
                if !columns.is_empty() {
                    late.insert(var, columns);
                }
            }
            other => base.demand(var, other),
        }
    }
    let project = (!late.is_empty()).then_some(deferral.project);
    base.finish(late, project)
}

/// Where a plan's final projection may be read late.
struct Deferral {
    /// The index of the final `Project`.
    project: usize,
    /// The node variables scanned in the run of operators directly below it.
    scanned: HashSet<VarId>,
}

impl Deferral {
    /// The plan ends `.. Project { distinct: false }, [Skip,] Limit` with at
    /// least one node scan directly below the `Project`.
    ///
    /// The run below the `Project` is made of scans, expansions, filters and
    /// sorts. Any other operator (a `WITH`, an aggregate, an `UNWIND`, an
    /// optional or existential sub-plan, a union, a write) ends it: a variable
    /// bound before that operator keeps its early read.
    fn of(plan: &GraphPlan) -> Option<Self> {
        let ops = &plan.ops;
        let mut end = ops.len().checked_sub(1)?;
        if !matches!(ops[end], GraphOp::Limit { .. }) {
            return None;
        }
        if end > 0 && matches!(ops[end - 1], GraphOp::Skip { .. }) {
            end -= 1;
        }
        let project = end.checked_sub(1)?;
        if !matches!(
            ops[project],
            GraphOp::Project {
                distinct: false,
                ..
            }
        ) {
            return None;
        }
        let mut scanned = HashSet::new();
        for op in ops[..project].iter().rev() {
            match op {
                GraphOp::NodeScan { var, .. } => {
                    scanned.insert(*var);
                }
                GraphOp::EdgeScan { .. }
                | GraphOp::TypedEdgeScan { .. }
                | GraphOp::Expand { .. }
                | GraphOp::RelationshipUnique { .. }
                | GraphOp::Filter { .. }
                | GraphOp::Sort { .. } => {}
                _ => break,
            }
        }
        (!scanned.is_empty()).then_some(Self { project, scanned })
    }
}

struct Collector<'a, S> {
    property_names: &'a HashMap<PropertyId, String, S>,
    all: bool,
    demands: HashMap<VarId, VarDemand>,
    /// Pairs of variables that name the same node.
    links: Vec<(VarId, VarId)>,
    /// How many times each variable is scanned, nested plans included.
    scans: HashMap<VarId, u32>,
    /// `(node, list)`: `list` is the output of a `collect` of `node` as a
    /// value. The collect does not yet count as a read of the whole node.
    collects: Vec<(VarId, VarId)>,
    /// `(list, alias)`: `UNWIND list AS alias` over a bare variable.
    unwinds: Vec<(VarId, VarId)>,
}

impl<'a, S: BuildHasher> Collector<'a, S> {
    fn new(property_names: &'a HashMap<PropertyId, String, S>) -> Self {
        Self {
            property_names,
            all: false,
            demands: HashMap::new(),
            links: Vec::new(),
            scans: HashMap::new(),
            collects: Vec::new(),
            unwinds: Vec::new(),
        }
    }

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
        for op in &plan.ops {
            self.op(&plan.exprs, op);
        }
    }

    fn op(&mut self, exprs: &ExprArena, op: &GraphOp) {
        {
            match op {
                GraphOp::NodeScan { var, .. } => {
                    *self.scans.entry(*var).or_default() += 1;
                }
                GraphOp::EdgeScan { .. }
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
                            if let Some((node, list)) = collected_node_value(exprs, agg) {
                                self.collects.push((node, list));
                            } else if identity_only_aggregate(agg.func) {
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
                GraphOp::Unwind { list_expr, alias } => match exprs.get(*list_expr) {
                    IrExpr::VarRef(list) => self.unwinds.push((*list, *alias)),
                    _ => self.value(exprs, *list_expr),
                },
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

    /// Resolve the collected-node reads, then merge each alias class's demand
    /// into every member.
    fn finish(
        self,
        late: HashMap<VarId, BTreeSet<String>>,
        deferred_projection: Option<usize>,
    ) -> NodePropertyDemand {
        let Self {
            all,
            mut demands,
            links,
            collects,
            unwinds,
            ..
        } = self;
        let mut classes = Classes::default();
        for var in demands.keys() {
            classes.add(*var);
        }
        for (a, b) in &links {
            classes.union(*a, *b);
        }
        let narrowed = narrow_collects(
            &mut demands,
            &mut classes,
            &links,
            &collects,
            &unwinds,
            &late,
        );
        let merged = merge_by_class(demands, &mut classes);
        let mut vars = HashMap::new();
        for var in classes.members() {
            if let Some(demand) = merged.get(&classes.find(var)) {
                vars.insert(var, demand.clone());
            }
        }
        NodePropertyDemand {
            all,
            vars,
            late,
            deferred_projection,
            narrowed_collects: narrowed,
        }
    }
}

/// The merged demand of each alias class, keyed by the class root.
fn merge_by_class(
    demands: HashMap<VarId, VarDemand>,
    classes: &mut Classes,
) -> HashMap<VarId, VarDemand> {
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
    merged
}

/// `agg` collects a whole node as a value: `(node, list)` where `list` is the
/// variable the collect binds.
fn collected_node_value(exprs: &ExprArena, agg: &crate::AggExpr) -> Option<(VarId, VarId)> {
    if !matches!(agg.func, AggFunc::Collect | AggFunc::CollectDistinct) {
        return None;
    }
    let node = node_struct_operand(exprs, agg.arg?)?;
    Some((node, agg.out_var?))
}

/// Charge each collected node value for what its `UNWIND` rows read.
///
/// A node collected as a value (`collect(DISTINCT friend)`) is a struct of
/// every stored property, so by default it reads the whole node. When the list
/// is only unwound, and every unwound element reads named columns, the struct
/// need only carry those columns: the element's fields are exactly the
/// struct's. Any other use of the list (returned, sized, indexed, passed
/// through a `WITH` that renames it, never unwound) or any whole-node read of
/// an element keeps the whole read.
///
/// Returns the nodes whose collected value is narrowed.
fn narrow_collects(
    demands: &mut HashMap<VarId, VarDemand>,
    classes: &mut Classes,
    links: &[(VarId, VarId)],
    collects: &[(VarId, VarId)],
    unwinds: &[(VarId, VarId)],
    late: &HashMap<VarId, BTreeSet<String>>,
) -> HashSet<VarId> {
    let collected: HashSet<VarId> = collects.iter().map(|(_, list)| *list).collect();
    // An unwound list that is not a collected node value is read as before.
    for (list, _) in unwinds {
        if !collected.contains(list) {
            merge_demand(demands, *list, VarDemand::All);
        }
    }
    // The demand of every variable outside the collected values, which is
    // what the elements' own reads add up to.
    let provisional = merge_by_class(demands.clone(), classes);
    let mut narrowed: HashSet<VarId> = HashSet::new();
    let mut whole: HashSet<VarId> = HashSet::new();
    for (node, list) in collects {
        let used_elsewhere =
            demands.contains_key(list) || links.iter().any(|(a, b)| a == list || b == list);
        let aliases: Vec<VarId> = unwinds
            .iter()
            .filter(|(unwound, _)| unwound == list)
            .map(|(_, alias)| *alias)
            .collect();
        let mut columns: BTreeSet<String> = BTreeSet::new();
        let mut exact = !used_elsewhere && !aliases.is_empty();
        for alias in &aliases {
            let root = classes.find(*alias);
            // An alias whose own demand is not final (it is itself collected,
            // or it shares a class with the collected node) is read whole.
            let unsettled = classes.find(*node) == root
                || collects
                    .iter()
                    .any(|(other, _)| classes.find(*other) == root);
            match provisional.get(&root) {
                _ if unsettled => exact = false,
                Some(VarDemand::All) => exact = false,
                Some(VarDemand::Names(names)) => columns.extend(names.iter().cloned()),
                None => {}
            }
            if let Some(names) = late.get(alias) {
                columns.extend(names.iter().cloned());
            }
        }
        if exact {
            narrowed.insert(*node);
            merge_demand(demands, *node, VarDemand::Names(columns));
        } else {
            whole.insert(*node);
        }
    }
    for node in &whole {
        merge_demand(demands, *node, VarDemand::All);
    }
    narrowed.retain(|node| !whole.contains(node));
    narrowed
}

fn merge_demand(demands: &mut HashMap<VarId, VarDemand>, var: VarId, demand: VarDemand) {
    match demands.get_mut(&var) {
        Some(existing) => existing.merge(demand),
        None => {
            demands.insert(var, demand);
        }
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
