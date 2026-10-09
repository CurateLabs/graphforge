//! Exact UUID nominations from completed join filters.

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, InListExpr, Literal,
};

pub(crate) fn is_uuid_dynamic_filter(expression: &Arc<dyn PhysicalExpr>, key: &str) -> bool {
    expression
        .downcast_ref::<DynamicFilterPhysicalExpr>()
        .is_some_and(|dynamic| {
            let children = dynamic
                .remapped_children()
                .unwrap_or_else(|| dynamic.original_children());
            children.len() == 1 && is_key(&children[0], key)
        })
}

fn is_key(expression: &Arc<dyn PhysicalExpr>, key: &str) -> bool {
    expression
        .downcast_ref::<Column>()
        .is_some_and(|column| column.name() == key)
}

/// A missing exact nomination keeps the ordinary scan. A conjunction may use
/// either side's set: every row satisfying the conjunction is in that set.
pub(crate) fn uuid_candidates(
    expression: &Arc<dyn PhysicalExpr>,
    key: &str,
) -> Option<BTreeSet<[u8; 16]>> {
    if let Some(binary) = expression.downcast_ref::<BinaryExpr>()
        && binary.op() == &Operator::And
    {
        return match (
            uuid_candidates(binary.left(), key),
            uuid_candidates(binary.right(), key),
        ) {
            (Some(left), Some(right)) => Some(&left & &right),
            (left, right) => left.or(right),
        };
    }
    if let Some(literal) = expression.downcast_ref::<Literal>()
        && literal.value() == &ScalarValue::Boolean(Some(false))
    {
        return Some(BTreeSet::new());
    }
    let list = expression.downcast_ref::<InListExpr>()?;
    if list.negated() || !is_key(list.expr(), key) {
        return None;
    }
    let mut uuids = BTreeSet::new();
    for expression in list.list() {
        let literal = expression.downcast_ref::<Literal>()?;
        match literal.value() {
            ScalarValue::FixedSizeBinary(16, Some(bytes)) => {
                uuids.insert(bytes.as_slice().try_into().ok()?);
            }
            value if value.is_null() => {}
            _ => return None,
        }
    }
    Some(uuids)
}
