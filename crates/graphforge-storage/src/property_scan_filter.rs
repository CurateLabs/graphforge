//! Exact UUID nominations from completed join filters.

use std::sync::Arc;

use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr};

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
