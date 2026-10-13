//! Which parts of a hydrated path node an expression over `nodes(p)` observes
//! (#1984).
//!
//! `[n IN nodes(p) | n.id]` needs one property column, not every label and
//! property of every node on the path. The analysis walks the clauses of the
//! comprehension or quantifier that consumes the path-node list and records
//! what the loop variable is used for. Any use it does not recognise demands
//! the whole node, so a narrowed descriptor can never turn an observed value
//! into `NULL`.

use super::{
    DfExpr, ExprArena, ExprId, ExprLowerer, IrExpr, LoweringError, VarId, resolve_builtin,
};
use graphforge_value::PropertyId;
use std::collections::{BTreeSet, HashSet};

/// What an expression observes of each hydrated path node.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum PathNodeDemand {
    /// The node value escapes or is used in a way the analysis does not
    /// understand: hydrate its identity, labels, and every stored property.
    Whole,
    /// Only the node identity, the label list when `labels` is set, and the
    /// named stored properties are read.
    Observed {
        /// Whether `labels(n)` or `n.labels` is read.
        labels: bool,
        /// Property names read through `n.<name>`.
        properties: BTreeSet<String>,
    },
}

impl PathNodeDemand {
    /// Whether the element carries the `labels` field.
    #[must_use]
    pub fn labels(&self) -> bool {
        match self {
            Self::Whole => true,
            Self::Observed { labels, .. } => *labels,
        }
    }

    /// Whether the stored property `name` is part of the element.
    #[must_use]
    pub fn wants_property(&self, name: &str) -> bool {
        match self {
            Self::Whole => true,
            Self::Observed { properties, .. } => properties.contains(name),
        }
    }

    /// The demand of `clauses` over `loop_var`, bound to each path node.
    ///
    /// `property_name` resolves a property ID to the element field name the
    /// lowerer would read for it. The walk is exhaustive over `IrExpr`: a
    /// bare reference to the loop variable, in any position other than the
    /// recognised property and label reads, demands [`Self::Whole`].
    pub(super) fn of_clauses(
        arena: &ExprArena,
        loop_var: VarId,
        clauses: &[ExprId],
        property_name: &dyn Fn(PropertyId) -> String,
    ) -> Self {
        let mut labels = false;
        let mut properties = BTreeSet::new();
        let mut visited = HashSet::new();
        let mut stack = clauses.to_vec();
        while let Some(id) = stack.pop() {
            if !visited.insert(id) {
                continue;
            }
            match arena.get(id) {
                IrExpr::Literal(_) | IrExpr::Parameter(_) => {}
                IrExpr::VarRef(var) => {
                    if *var == loop_var {
                        return Self::Whole;
                    }
                }
                IrExpr::PropertyAccess { base, prop } => {
                    if matches!(arena.get(*base), IrExpr::VarRef(var) if *var == loop_var) {
                        let name = property_name(*prop);
                        // A stored property named `labels` is shadowed by the
                        // `labels` field, so `n.labels` reads the label list.
                        if name == "labels" {
                            labels = true;
                        } else {
                            properties.insert(name);
                        }
                    } else {
                        stack.push(*base);
                    }
                }
                IrExpr::FunctionCall { name, args } => {
                    if name == "labels"
                        && let [arg] = args.as_slice()
                        && matches!(arena.get(*arg), IrExpr::VarRef(var) if *var == loop_var)
                    {
                        labels = true;
                    } else {
                        stack.extend(args.iter().copied());
                    }
                }
                IrExpr::BinaryOp { left, right, .. } => stack.extend([*left, *right]),
                IrExpr::UnaryOp { expr, .. } => stack.push(*expr),
                IrExpr::Case {
                    operand,
                    arms,
                    else_expr,
                } => {
                    stack.extend(operand.iter().copied());
                    for arm in arms {
                        stack.extend([arm.when, arm.then]);
                    }
                    stack.extend(else_expr.iter().copied());
                }
                IrExpr::ListLiteral(items) => stack.extend(items.iter().copied()),
                IrExpr::MapLiteral(entries) => {
                    stack.extend(entries.iter().map(|(_, value)| *value));
                }
                IrExpr::Quantifier {
                    list, predicate, ..
                } => stack.extend([*list, *predicate]),
                IrExpr::ListComprehension {
                    list,
                    filter,
                    projection,
                    ..
                } => {
                    stack.push(*list);
                    stack.extend(filter.iter().copied());
                    stack.extend(projection.iter().copied());
                }
            }
        }
        Self::Observed { labels, properties }
    }
}

impl ExprLowerer<'_> {
    /// Lower the list a comprehension or quantifier iterates.
    ///
    /// A list that is directly `nodes(p)` over a variable-length segment is
    /// hydrated for what `clauses` observe of `loop_var` rather than for every
    /// property of every node (#1984). `escapes` is set when the loop variable
    /// itself becomes part of the result, which needs the whole node. Any
    /// other list lowers as usual.
    pub(super) fn lower_clause_list(
        &self,
        list: ExprId,
        loop_var: VarId,
        clauses: &[ExprId],
        escapes: bool,
    ) -> Result<DfExpr, LoweringError> {
        if !escapes
            && self.read_target.is_some()
            && let IrExpr::FunctionCall { name, args } = self.arena.get(list)
            && name.eq_ignore_ascii_case("_path_nodes")
        {
            let demand = self.path_node_demand(loop_var, clauses);
            if demand != PathNodeDemand::Whole {
                let lowered = args
                    .iter()
                    .map(|&arg| self.lower_path_builtin_arg(arg))
                    .collect::<Result<Vec<_>, _>>()?;
                let hydration = self.path_node_hydration(demand)?;
                return resolve_builtin(name, lowered, || hydration)
                    .ok_or_else(|| LoweringError::UnknownFunction(name.clone()));
            }
        }
        self.lower(list)
    }

    fn path_node_demand(&self, loop_var: VarId, clauses: &[ExprId]) -> PathNodeDemand {
        PathNodeDemand::of_clauses(self.arena, loop_var, clauses, &|prop| {
            self.prop_names
                .get(&prop)
                .cloned()
                .unwrap_or_else(|| format!("prop_{prop}"))
        })
    }
}

#[cfg(test)]
mod tests;
