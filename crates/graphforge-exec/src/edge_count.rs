//! CSR/catalog edge-count short-circuit for unconstrained `count(r)` (#1094).
//!
//! `MATCH ()-[r]->() RETURN count(r)` otherwise expands every adjacency entry
//! into Arrow batches before aggregating. That retains process RSS ~linear in
//! edge count and fails the progressive S18→S19 plateau gate. When the physical
//! plan is a global nonnull literal, compiler row-marker, or matched edge-identity
//! count over a single
//! unconstrained outward Expand (including a validated Partial/Final pair),
//! replace it with the adjacency view's edge-entry count.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use graphforge_ir::Direction;

use crate::adjacency::AdjacencyProvider;
use crate::demand;

struct EdgeCountSpec {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    rel_type_name: String,
    direction: Direction,
    provider: Arc<dyn AdjacencyProvider>,
}

pub(crate) struct EdgeCountExec {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    rel_type_name: String,
    direction: Direction,
    provider: Arc<dyn AdjacencyProvider>,
    capture_epoch: u64,
}

impl EdgeCountExec {
    /// Build from parts chosen from the Graph IR (ADR 0050).
    pub(crate) fn from_parts(
        schema: SchemaRef,
        props: Arc<PlanProperties>,
        rel_type_name: String,
        direction: Direction,
        provider: Arc<dyn AdjacencyProvider>,
    ) -> Self {
        Self::new(EdgeCountSpec {
            schema,
            props,
            rel_type_name,
            direction,
            provider,
        })
    }

    fn new(spec: EdgeCountSpec) -> Self {
        Self {
            schema: spec.schema,
            props: spec.props,
            rel_type_name: spec.rel_type_name,
            direction: spec.direction,
            provider: spec.provider,
            capture_epoch: demand::stamp_capture_epoch().unwrap_or(0),
        }
    }
}

impl fmt::Debug for EdgeCountExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EdgeCountExec")
            .field("rel", &self.rel_type_name)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for EdgeCountExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "EdgeCountExec: rel={}, dir={:?}",
            self.rel_type_name, self.direction
        )
    }
}

impl ExecutionPlan for EdgeCountExec {
    fn name(&self) -> &str {
        "EdgeCountExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "EdgeCountExec has no children".into(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "EdgeCountExec only has partition 0, got {partition}"
            )));
        }
        let count = self
            .provider
            .edge_cardinality(&self.rel_type_name, self.direction)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        // No edge candidates are materialized by the cardinality query.
        demand::record_candidates(self.capture_epoch, 0, 0);
        demand::record_emitted(self.capture_epoch, 0, 1);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for _ in self.schema.fields() {
            let value = i64::try_from(count)
                .map_err(|_| DataFusionError::Internal("edge count exceeds Int64".into()))?;
            columns.push(Arc::new(Int64Array::from(vec![value])));
        }
        let batch = arrow::record_batch::RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        Ok(Box::pin(EdgeCountStream {
            schema: Arc::clone(&self.schema),
            batch: Some(batch),
        }))
    }
}

struct EdgeCountStream {
    schema: SchemaRef,
    batch: Option<arrow::record_batch::RecordBatch>,
}

impl Stream for EdgeCountStream {
    type Item = Result<arrow::record_batch::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.batch.take().map(Ok))
    }
}

impl RecordBatchStream for EdgeCountStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::EquivalenceProperties;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::{ExecutionPlan, Partitioning, PlanProperties};
    use futures::StreamExt;
    use graphforge_core::GfError;
    use graphforge_ir::Direction;

    use super::*;
    use crate::adjacency::{Adjacency, AdjacencyProvider, AdjacencyStatus};

    struct CardinalityOnlyProvider {
        count: u64,
    }

    impl AdjacencyProvider for CardinalityOnlyProvider {
        fn adjacency(
            &self,
            _rel_type_name: &str,
            _direction: Direction,
        ) -> Result<Arc<Adjacency>, GfError> {
            panic!("EdgeCountExec must not open the adjacency view");
        }

        fn status(&self, _rel_type_name: &str, _direction: Direction) -> AdjacencyStatus {
            AdjacencyStatus::Hit
        }

        fn edge_cardinality(
            &self,
            _rel_type_name: &str,
            _direction: Direction,
        ) -> Result<u64, GfError> {
            Ok(self.count)
        }
    }

    #[test]
    fn edge_count_exec_uses_cardinality_without_opening_view() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "total",
            DataType::Int64,
            false,
        )]));
        let exec = EdgeCountExec::new(EdgeCountSpec {
            schema: Arc::clone(&schema),
            props: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
            rel_type_name: "*".into(),
            direction: Direction::Out,
            provider: Arc::new(CardinalityOnlyProvider { count: 42 }),
        });
        let mut stream = exec
            .execute(0, Arc::new(TaskContext::default()))
            .expect("cardinality short-circuit");
        let batch = futures::executor::block_on(stream.next())
            .expect("one count batch")
            .expect("batch ok");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 count");
        assert_eq!(values.values(), &[42]);
        assert!(futures::executor::block_on(stream.next()).is_none());
    }
}
