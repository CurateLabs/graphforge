//! Order-aware two-hop path counting for `ORDER BY destination_uuid LIMIT K` (#966).
//!
//! When the terminal sort is a single ascending key on the destination node UUID
//! and both hops are fixed, identity-only expansions, materializing every
//! intermediate candidate before TopK is correct but scales with total path count.
//! This module replaces the expand chain with destination-order path counting:
//! iterate destinations in node-id order (equivalent to UUID order for monotonic
//! ordinal identity), count two-hop paths with optional edge-disjointness, emit
//! path multiplicity until the limit is satisfied.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder};
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

struct OrderedTwoHopSpec {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    fetch: usize,
    rel_type_name: String,
    provider: Arc<dyn AdjacencyProvider>,
    ordinal_identities: Arc<crate::V4OrdinalIdentitySession>,
    require_edge_disjoint: bool,
}

pub struct OrderedTwoHopPathCountExec {
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    fetch: usize,
    rel_type_name: String,
    provider: Arc<dyn AdjacencyProvider>,
    ordinal_identities: Arc<crate::V4OrdinalIdentitySession>,
    require_edge_disjoint: bool,
    capture_epoch: u64,
}

impl OrderedTwoHopPathCountExec {
    /// Build from parts chosen from the Graph IR (ADR 0050).
    pub(crate) fn from_parts(
        schema: SchemaRef,
        props: Arc<PlanProperties>,
        fetch: usize,
        rel_type_name: String,
        provider: Arc<dyn AdjacencyProvider>,
        ordinal_identities: Arc<crate::V4OrdinalIdentitySession>,
        require_edge_disjoint: bool,
    ) -> Self {
        Self::new(OrderedTwoHopSpec {
            schema,
            props,
            fetch,
            rel_type_name,
            provider,
            ordinal_identities,
            require_edge_disjoint,
        })
    }

    fn new(spec: OrderedTwoHopSpec) -> Self {
        Self {
            schema: spec.schema,
            props: spec.props,
            fetch: spec.fetch,
            rel_type_name: spec.rel_type_name,
            provider: spec.provider,
            ordinal_identities: spec.ordinal_identities,
            require_edge_disjoint: spec.require_edge_disjoint,
            capture_epoch: demand::stamp_capture_epoch().unwrap_or(0),
        }
    }
}

impl fmt::Debug for OrderedTwoHopPathCountExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderedTwoHopPathCountExec")
            .field("fetch", &self.fetch)
            .field("rel", &self.rel_type_name)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for OrderedTwoHopPathCountExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OrderedTwoHopPathCountExec: rel={}, fetch={}",
            self.rel_type_name, self.fetch
        )
    }
}

impl ExecutionPlan for OrderedTwoHopPathCountExec {
    fn name(&self) -> &str {
        "OrderedTwoHopPathCountExec"
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "OrderedTwoHopPathCountExec has no children".into(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "OrderedTwoHopPathCountExec only has partition 0, got {partition}"
            )));
        }
        let mut inbound = crate::adjacency::AdjacencyReader::new(
            self.provider.as_ref(),
            &self.rel_type_name,
            Direction::In,
        )
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let node_extent = inbound.node_extent();

        let mut remaining = self.fetch;
        let mut uuids = Vec::new();
        let mut candidates = 0_u64;
        let epoch = self.capture_epoch;
        for destination in 0..node_extent {
            if remaining == 0 {
                break;
            }
            demand::record_adjacency_row(self.capture_epoch, 1);
            let (path_count, examined) = count_two_hop_paths_to(
                &mut inbound,
                destination,
                self.require_edge_disjoint,
                remaining,
            )
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
            candidates = candidates.saturating_add(examined);
            if path_count == 0 {
                continue;
            }
            let lookup = self
                .ordinal_identities
                .lookup_node_uuids(&[destination])
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            demand::record_identity_projection(epoch, 1, 1, 1, &lookup.metrics);
            let uuid = *lookup.values[0]
                .as_ref()
                .ok_or_else(|| DataFusionError::Internal("missing destination uuid".into()))?
                .as_bytes();
            let emit = usize_from_u64(path_count).min(remaining);
            uuids.extend(std::iter::repeat_n(uuid, emit));
            remaining -= emit;
        }

        demand::record_candidates(epoch, 1, usize_from_u64(candidates));
        demand::record_emitted(epoch, 1, uuids.len());

        let mut builder = FixedSizeBinaryBuilder::with_capacity(uuids.len(), 16);
        for uuid in uuids {
            builder
                .append_value(uuid)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
        }
        let column: ArrayRef = Arc::new(builder.finish());
        let batch =
            arrow::record_batch::RecordBatch::try_new(Arc::clone(&self.schema), vec![column])
                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        Ok(Box::pin(OrderedTwoHopStream {
            schema: Arc::clone(&self.schema),
            batch: Some(batch),
        }))
    }
}

fn usize_from_u64(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn count_two_hop_paths_to(
    inbound: &mut crate::adjacency::AdjacencyReader<'_>,
    destination: u64,
    require_edge_disjoint: bool,
    remaining: usize,
) -> std::result::Result<(u64, u64), graphforge_core::GfError> {
    const CHUNK: usize = 256;
    let mut count = 0_u64;
    let mut examined = 0_u64;
    let mut outer_offset = 0;
    loop {
        let outer = inbound.neighbor_chunk(destination, outer_offset, CHUNK)?;
        if outer.is_empty() {
            return Ok((count, examined));
        }
        outer_offset += outer.len();
        for (r2, middle) in outer {
            let mut inner_offset = 0;
            loop {
                let inner = inbound.neighbor_chunk(middle, inner_offset, CHUNK)?;
                if inner.is_empty() {
                    break;
                }
                inner_offset += inner.len();
                for (r1, _) in inner {
                    examined = examined.saturating_add(1);
                    if !require_edge_disjoint || r1 != r2 {
                        count = count.saturating_add(1);
                        if count >= remaining as u64 {
                            return Ok((count, examined));
                        }
                    }
                }
            }
        }
    }
}

struct OrderedTwoHopStream {
    schema: SchemaRef,
    batch: Option<arrow::record_batch::RecordBatch>,
}

impl Stream for OrderedTwoHopStream {
    type Item = Result<arrow::record_batch::RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.batch.take().map(Ok))
    }
}

impl RecordBatchStream for OrderedTwoHopStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
