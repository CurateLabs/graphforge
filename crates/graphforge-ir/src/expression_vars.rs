//! Which variables an expression reads, and whether re-evaluating it is safe.
//!
//! Lowering uses these to decide whether a predicate can move across a
//! pipeline boundary: a conjunct that reads only variables bound on one side
//! can be evaluated on that side, and only a deterministic conjunct evaluates
//! to the same value wherever it runs.

use std::collections::BTreeSet;

use crate::reachability::visit;
use crate::{ExprArena, ExprId, IrExpr, VarId};

/// The variables `id` reads from its environment, in ascending order.
///
/// A quantifier or list-comprehension loop variable is bound inside its own
/// expression and is not an environment read, so it is excluded.
#[must_use]
pub fn referenced_vars(exprs: &ExprArena, id: ExprId) -> Vec<VarId> {
    let mut read = BTreeSet::new();
    let mut bound = BTreeSet::new();
    visit(exprs, id, &mut |expr| match expr {
        IrExpr::VarRef(var) => {
            read.insert(var.0);
        }
        IrExpr::Quantifier { loop_var, .. } | IrExpr::ListComprehension { loop_var, .. } => {
            bound.insert(loop_var.0);
        }
        _ => {}
    });
    read.difference(&bound).map(|var| VarId(*var)).collect()
}

/// Whether `id` calls a function whose result differs between evaluations of
/// the same row, so that evaluating it a different number of times, or in a
/// different order, changes the query result.
#[must_use]
pub fn calls_volatile_function(exprs: &ExprArena, id: ExprId) -> bool {
    let mut volatile = false;
    visit(exprs, id, &mut |expr| {
        if let IrExpr::FunctionCall { name, .. } = expr
            && (name.eq_ignore_ascii_case("rand") || name.eq_ignore_ascii_case("randomUUID"))
        {
            volatile = true;
        }
    });
    volatile
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BinaryOpKind, IrLiteral};

    #[test]
    fn reads_exclude_loop_variables_and_are_sorted() {
        let mut arena = ExprArena::new();
        let outer = arena.push(IrExpr::VarRef(VarId(5)));
        let loop_ref = arena.push(IrExpr::VarRef(VarId(9)));
        let other = arena.push(IrExpr::VarRef(VarId(2)));
        let predicate = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Eq,
            left: loop_ref,
            right: outer,
        });
        let list = arena.push(IrExpr::ListLiteral(vec![other]));
        let quantifier = arena.push(IrExpr::Quantifier {
            kind: crate::QuantifierKind::Any,
            loop_var: VarId(9),
            list,
            predicate,
        });
        assert_eq!(
            referenced_vars(&arena, quantifier),
            vec![VarId(2), VarId(5)]
        );
    }

    #[test]
    fn volatile_functions_are_found_at_any_depth() {
        let mut arena = ExprArena::new();
        let rand = arena.push(IrExpr::FunctionCall {
            name: "rand".to_owned(),
            args: Vec::new(),
        });
        let one = arena.push(IrExpr::Literal(IrLiteral::Int(1)));
        let sum = arena.push(IrExpr::BinaryOp {
            op: BinaryOpKind::Add,
            left: one,
            right: rand,
        });
        assert!(calls_volatile_function(&arena, sum));
        assert!(!calls_volatile_function(&arena, one));
    }
}
