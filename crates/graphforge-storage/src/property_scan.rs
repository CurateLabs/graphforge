//! Bounded DataFusion execution for authenticated immutable property overlays.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream;

pub(crate) struct PropertyScanOptions<'a> {
    pub(crate) projection: Option<&'a Vec<usize>>,
    pub(crate) limit: Option<usize>,
    pub(crate) batch_size: usize,
}

#[derive(Clone)]
pub(crate) struct PropertyOverlayExec {
    project: PathBuf,
    inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
    route: String,
    is_edge: bool,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    batch_size: usize,
    row_upper_bound: Option<usize>,
    props: Arc<PlanProperties>,
}

impl fmt::Debug for PropertyOverlayExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PropertyOverlayExec")
            .field("route", &self.route)
            .field("is_edge", &self.is_edge)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl PropertyOverlayExec {
    #[allow(
        clippy::needless_pass_by_value,
        reason = "execution plan takes shared schema ownership"
    )]
    pub(crate) fn try_new(
        project: PathBuf,
        inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
        route: String,
        is_edge: bool,
        base_schema: SchemaRef,
        options: PropertyScanOptions<'_>,
    ) -> Result<Self, DataFusionError> {
        let projection = options.projection.cloned();
        let schema = projection.as_ref().map_or_else(
            || Ok(Arc::clone(&base_schema)),
            |indices| {
                base_schema
                    .project(indices)
                    .map(Arc::new)
                    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
            },
        )?;
        let kind = if is_edge {
            crate::PropertyRouteKind::Edge
        } else {
            crate::PropertyRouteKind::Node
        };
        let row_upper_bound = inventory.as_ref().map(|inventory| {
            let rows = inventory.route_row_upper_bound(kind, &route);
            options.limit.map_or(rows, |limit| rows.min(limit))
        });
        let props = Arc::new(
            PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )
            // Decode runs on the blocking pool and hands bounded batches to a
            // backpressured channel, so polling never blocks the async worker.
            .with_scheduling_type(SchedulingType::Cooperative),
        );
        Ok(Self {
            project,
            inventory,
            route,
            is_edge,
            schema,
            projection,
            limit: options.limit,
            batch_size: options.batch_size.max(1),
            row_upper_bound,
            props,
        })
    }
}

impl DisplayAs for PropertyOverlayExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PropertyOverlayExec: route={}", self.route)
    }
}

impl ExecutionPlan for PropertyOverlayExec {
    fn name(&self) -> &'static str {
        "PropertyOverlayExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "PropertyOverlayExec cannot have children".into(),
            ));
        }
        Ok(self)
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<Arc<Statistics>, DataFusionError> {
        let rows = match partition {
            None | Some(0) => self.row_upper_bound,
            Some(_) => None,
        };
        let num_rows = match rows {
            Some(0) => Precision::Exact(0),
            // Physical footer rows are a sound upper bound, but overlays and
            // tombstones can reduce the logical output.
            Some(rows) => Precision::Inexact(rows),
            None => Precision::Absent,
        };
        Ok(Arc::new(Statistics {
            num_rows,
            total_byte_size: Precision::Absent,
            column_statistics: self
                .schema
                .fields()
                .iter()
                .map(|_| ColumnStatistics::new_unknown())
                .collect(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "PropertyOverlayExec has one partition".into(),
            ));
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let project = self.project.clone();
        let inventory = self.inventory.clone();
        let route = self.route.clone();
        let is_edge = self.is_edge;
        let projection = self.projection.clone();
        let mut remaining = self.limit;
        let batch_size = self.batch_size;
        tokio::task::spawn_blocking(move || {
            let result = crate::catalog::visit_property_overlay_batched_with_inventory(
                &project,
                inventory.as_deref(),
                &route,
                is_edge,
                batch_size,
                |batch| {
                    let mut batch = projection.as_ref().map_or_else(
                        || Ok(batch.clone()),
                        |indices| {
                            batch
                                .project(indices)
                                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
                        },
                    )?;
                    if let Some(rows) = remaining.as_mut() {
                        if *rows == 0 {
                            return Ok(true);
                        }
                        if batch.num_rows() > *rows {
                            batch = batch.slice(0, *rows);
                        }
                        *rows -= batch.num_rows();
                    }
                    sender.blocking_send(Ok(batch)).map_err(|_| {
                        DataFusionError::Execution("property scan consumer closed".into())
                    })?;
                    Ok(true)
                },
            );
            if let Err(error) = result {
                let _ = sender.blocking_send(Err(error));
            }
        });
        let schema = Arc::clone(&self.schema);
        let output = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}
