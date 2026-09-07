//! Execution-owned binding for the current graph's logical read resources.
use datafusion::common::{DataFusionError, Result, tree_node::Transformed};
use datafusion::datasource::{TableProvider, provider_as_source};
use datafusion::logical_expr::{LogicalPlan, TableSource};
use graphforge_core::OntologyMode;
use graphforge_plan::{GraphReadSource, GraphReadTable};
use graphforge_storage::GraphCatalog;
use std::path::PathBuf;
use std::sync::Arc;

/// Session-local authority. Logical plans never own this value.
pub struct GraphReadContext {
    pub(crate) dir: PathBuf,
    pub(crate) mode: OntologyMode,
    pub(crate) catalog: Arc<GraphCatalog>,
    pub(crate) ontology: Option<graphforge_ontology::OntologyHandle>,
}

impl GraphReadContext {
    pub(crate) fn validate(
        &self,
        contract: Option<&graphforge_plan::GraphReadContract>,
    ) -> Result<()> {
        if let Some(contract) = contract {
            let actual =
                graphforge_rel::GraphPlanLowerer::new(Some(&self.catalog), self.ontology.as_ref())
                    .map_err(|e| DataFusionError::Plan(e.to_string()))?
                    .read_contract();
            if *contract != actual {
                return Err(DataFusionError::Plan(
                    "GF_READ_RESOURCE_INCOMPATIBLE: logical identities".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn resolve(&self, source: &GraphReadSource) -> Result<Arc<dyn TableProvider>> {
        self.validate(source.contract.as_ref())?;
        if source.composition.as_deref().is_some_and(|expected| {
            Some(expected) != self.catalog.semantic_composition_fingerprint()
        }) {
            return Err(DataFusionError::Plan(
                "GF_READ_RESOURCE_INCOMPATIBLE: semantic composition".into(),
            ));
        }
        let table: Arc<dyn TableProvider> = match &source.table {
            GraphReadTable::Nodes => Arc::new(graphforge_storage::TopologyNodeTable::open_project(
                &self.dir,
            )?),
            GraphReadTable::Edges(stem)
                if stem == "_exploratory" && self.mode != OntologyMode::Exploratory =>
            {
                Arc::new(graphforge_storage::UnionEdgeTable::open(&self.dir))
            }
            GraphReadTable::Edges(stem) => {
                Arc::new(graphforge_storage::TypedEdgeTable::open(&self.dir, stem))
            }
            GraphReadTable::SemanticEdges(id) => {
                self.catalog.semantic_edge_table(*id).ok_or_else(|| {
                    DataFusionError::Plan("GF_READ_RESOURCE_INCOMPATIBLE: semantic relation".into())
                })?
            }
            GraphReadTable::Properties(stem) => {
                Arc::new(self.catalog.property_table(&self.dir, stem))
            }
            GraphReadTable::EdgeProperties(_, Some(id)) => self
                .catalog
                .semantic_edge_property_table(*id)
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "GF_READ_RESOURCE_INCOMPATIBLE: semantic edge properties".into(),
                    )
                })?,
            GraphReadTable::EdgeProperties(stem, None) => {
                Arc::new(self.catalog.edge_property_table(&self.dir, stem))
            }
        };
        if graphforge_plan::read_resource::semantic_read_schema(&table.schema()) != source.schema()
        {
            return Err(DataFusionError::Plan(
                "GF_READ_RESOURCE_INCOMPATIBLE: read schema".into(),
            ));
        }
        Ok(table)
    }
}

pub(crate) fn required(
    state: &datafusion::execution::SessionState,
) -> Result<Arc<GraphReadContext>> {
    state
        .config()
        .get_extension::<GraphReadContext>()
        .filter(|r| !r.dir.as_os_str().is_empty())
        .ok_or_else(|| {
            DataFusionError::Plan("GF_READ_RESOURCE_MISSING: current graph is not bound".into())
        })
}

pub(crate) fn bind(
    plan: &LogicalPlan,
    state: &datafusion::execution::SessionState,
) -> Result<LogicalPlan> {
    let resource = state
        .config()
        .get_extension::<GraphReadContext>()
        .filter(|resource| !resource.dir.as_os_str().is_empty());
    let contract = resource
        .as_ref()
        .map(|r| {
            graphforge_rel::GraphPlanLowerer::new(Some(&r.catalog), r.ontology.as_ref())
                .map(|l| l.read_contract())
        })
        .transpose()
        .map_err(|e| DataFusionError::Plan(e.to_string()))?;
    plan.clone()
        .transform_up_with_subqueries(|mut node| {
            if let LogicalPlan::TableScan(scan) = &mut node
                && let Some(source) = scan.source.downcast_ref::<GraphReadSource>()
            {
                let provider = required(state)?.resolve(source)?;
                // Restore the selected provider's public schema only in this
                // execution clone; logical identity excludes observation counts.
                *scan = datafusion::logical_expr::TableScan::try_new(
                    scan.table_name.clone(),
                    provider_as_source(provider),
                    scan.projection.clone(),
                    scan.filters.clone(),
                    scan.fetch,
                )?;
            }
            node.map_expressions(|expr| {
                graphforge_rel::expr::bind_graph_read_expression(
                    expr,
                    resource.as_ref().map(|r| r.dir.as_path()),
                    contract.as_ref().map(|c| c.labels.as_slice()),
                )
                .map(Transformed::yes)
            })?
            .data
            .recompute_schema()
            .map(Transformed::yes)
        })
        .map(|result| result.data)
}
