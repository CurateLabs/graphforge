//! Composition-bound node properties follow immutable primary storage ownership.
use super::{GraphPlanLowerer, LoweringError, MapUnsupportedExpr, VarId, var_alias};
use datafusion::{
    common::Column,
    logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder, col},
};
use std::collections::{BTreeMap, BTreeSet};

impl GraphPlanLowerer {
    pub(super) fn semantic_node_property_columns(&self) -> Option<Vec<String>> {
        let snapshot = self.read_snapshot()?;
        snapshot.semantic_composition_fingerprint()?;
        Some(
            snapshot
                .node_properties
                .values()
                .flat_map(|schema| schema.fields())
                .map(|field| field.name().clone())
                .filter(|name| {
                    graphforge_ir::arrow_schema::TOPOLOGY_NODES_SCHEMA
                        .field_with_name(name)
                        .is_err()
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        )
    }

    fn primary_property_route(&self, alias: &str, stem: &str) -> Expr {
        use datafusion::logical_expr::lit;
        // Historical exploratory writes may retain a UUID-owned `_untyped`
        // row even after the profile changes. Admit it by UUID; reconciliation
        // rejects competing non-null authorities instead of preferring a route.
        if stem == "_untyped" {
            return lit(true);
        }
        let mut selected = lit(false);
        for (id, route) in &self.type_id_to_entity_name {
            if route == stem {
                selected = selected.or(col(format!("{alias}.type_id")).eq(lit(id.encode())));
            }
        }
        selected
    }

    pub(super) fn join_semantic_node_properties(
        &self,
        var: VarId,
        ty: Option<super::EntityTypeId>,
        scan: LogicalPlan,
    ) -> Result<LogicalPlan, LoweringError> {
        let snapshot = self.read_snapshot().expect("admitted semantic snapshot");
        let alias = var_alias(var);
        let mut projections: Vec<Expr> = scan
            .schema()
            .iter()
            .map(|(qualifier, field)| Expr::Column(Column::new(qualifier.cloned(), field.name())))
            .collect();
        let existing: BTreeSet<_> = scan
            .schema()
            .iter()
            .filter(|(q, _)| q.is_some_and(|q| q.table() == alias))
            .map(|(_, field)| field.name().clone())
            .collect();
        let mut references: BTreeMap<String, Vec<Expr>> = BTreeMap::new();
        let mut types: BTreeMap<String, Vec<datafusion::arrow::datatypes::FieldRef>> =
            BTreeMap::new();
        let preferred = ty
            .and_then(|id| self.type_id_to_entity_name.get(&id))
            .and_then(|stem| snapshot.node_properties.get(stem));
        let mut joined = scan;
        // A secondary label is a membership filter, not a physical property route.
        // Authenticated routes retain UUID identity and immutable primary ownership.
        for (index, (stem, schema)) in snapshot.node_properties.iter().enumerate() {
            let columns: Vec<_> = schema
                .fields()
                .iter()
                .filter(|field| {
                    graphforge_ir::arrow_schema::TOPOLOGY_NODES_SCHEMA
                        .field_with_name(field.name())
                        .is_err()
                        && !existing.contains(field.name())
                })
                .collect();
            if columns.is_empty() {
                continue;
            }
            for field in &columns {
                types
                    .entry(field.name().clone())
                    .or_default()
                    .push((*field).clone());
            }
            let property_alias = format!("{alias}__primary_props_{index}");
            let source = graphforge_plan::GraphReadSource::new(
                graphforge_plan::GraphReadTable::Properties(stem.clone()),
                schema,
                snapshot
                    .semantic_composition_fingerprint()
                    .map(str::to_owned),
            );
            let properties = LogicalPlanBuilder::scan(property_alias.clone(), source, None)
                .and_then(LogicalPlanBuilder::build)
                .map_unsupported_expr()?;
            joined = LogicalPlanBuilder::from(joined)
                .join_on(
                    properties,
                    JoinType::Left,
                    vec![
                        col(format!("{alias}.node_uuid"))
                            .eq(col(format!("{property_alias}.node_uuid")))
                            .and(self.primary_property_route(&alias, stem)),
                    ],
                )
                .and_then(LogicalPlanBuilder::build)
                .map_unsupported_expr()?;
            for field in columns {
                references
                    .entry(field.name().clone())
                    .or_default()
                    .push(crate::expr::qualified_col(&property_alias, field.name()));
            }
        }
        for (name, mut refs) in references {
            let fields = types.remove(&name).expect("property fields");
            let compatible = fields.iter().all(|field| {
                field.data_type() == fields[0].data_type()
                    && field.metadata() == fields[0].metadata()
            });
            let expected = preferred
                .and_then(|schema| schema.field_with_name(&name).ok())
                .map(|field| std::sync::Arc::new(field.clone()))
                .or_else(|| compatible.then(|| fields[0].clone()));
            let value = if refs.len() == 1 {
                refs.remove(0)
            } else {
                super::primary_property_value::expression(refs, expected.clone())
            };
            projections.push(value.alias_qualified_with_metadata(
                Some(alias.as_str()),
                name,
                expected.map(|field| field.metadata().clone().into()),
            ));
        }
        LogicalPlanBuilder::from(joined)
            .project(projections)
            .and_then(LogicalPlanBuilder::build)
            .map_unsupported_expr()
    }
}
