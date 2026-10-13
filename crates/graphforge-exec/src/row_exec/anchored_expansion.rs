//! Physical counterpart of [`AnchoredExpansionNode`].
//!
//! A pattern that starts at a node the input already bound is expanded once per
//! distinct anchor instead of once per input row. The node executes its input a
//! single time, binds the distinct anchors into the expansion's
//! [`CorrelatedSeedExec`](super::CorrelatedSeedExec) leaf, and hash-joins the
//! expansion back to the input. The input is the build side and the expansion is
//! the probe stream, so the retained state is proportional to the input rows,
//! never to the expansion.

use super::bind_correlated_seed;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, JoinType, NullEquality, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, SendableRecordBatchStream,
};
use datafusion_datasource::memory::MemorySourceConfig;
use futures::{StreamExt, TryStreamExt};
use graphforge_plan::AnchoredExpansionNode;
use std::fmt;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Executes an [`AnchoredExpansionNode`]: its output is the input columns
/// followed by the expansion columns at `expansion_keep_idx`, one row per
/// matching (input row, expansion row) pair.
///
/// The output has the expansion's partition count; every partition probes one
/// shared build side. Execution is lazy: the first partition polled collects the
/// input (reserving it in the query memory pool), binds the distinct anchors
/// into the expansion, and builds the join all partitions then share.
pub struct AnchoredExpansionExec {
    shared: Arc<Shared>,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

/// What every partition of one planned node shares.
struct Shared {
    input: Arc<dyn ExecutionPlan>,
    expansion: Arc<dyn ExecutionPlan>,
    join_keys: Vec<(usize, usize)>,
    expansion_keep_idx: Vec<usize>,
    seed: u64,
    prepared: OnceCell<Prepared>,
}

/// The collected input and the join built over it.
struct Prepared {
    /// `None` when the input had no rows: nothing can match, so the expansion
    /// is never executed.
    join: Option<Arc<HashJoinExec>>,
    /// Keeps the collected input charged to the query memory pool for as long
    /// as the join, which retains it, is alive.
    _reservation: MemoryReservation,
}

impl AnchoredExpansionExec {
    /// Build the physical node from its logical counterpart and planned inputs.
    ///
    /// # Errors
    ///
    /// Returns an error when a key or kept column index is out of range, when
    /// the two sides of a key do not share a data type (the hash join would
    /// otherwise compare them by coercion), or when the output schema does not
    /// match the node's.
    pub fn try_new(
        node: &AnchoredExpansionNode,
        input: Arc<dyn ExecutionPlan>,
        expansion: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let schema: SchemaRef = Arc::new(node.schema().as_arrow().clone());
        let input_schema = input.schema();
        let expansion_schema = expansion.schema();
        for &(input_col, expansion_col) in &node.join_keys {
            let (Some(left), Some(right)) = (
                input_schema.fields().get(input_col),
                expansion_schema.fields().get(expansion_col),
            ) else {
                return Err(DataFusionError::Internal(format!(
                    "AnchoredExpansionExec key ({input_col}, {expansion_col}) is out of range"
                )));
            };
            if left.data_type() != right.data_type() {
                return Err(DataFusionError::Internal(format!(
                    "AnchoredExpansionExec key {} is {} on the input but {} on the expansion",
                    left.name(),
                    left.data_type(),
                    right.data_type()
                )));
            }
        }
        if node.join_keys.is_empty() {
            return Err(DataFusionError::Internal(
                "AnchoredExpansionExec needs at least one join key".into(),
            ));
        }
        if let Some(&bad) = node
            .expansion_keep_idx
            .iter()
            .find(|&&index| index >= expansion_schema.fields().len())
        {
            return Err(DataFusionError::Internal(format!(
                "AnchoredExpansionExec kept column {bad} is out of range"
            )));
        }
        if schema.fields().len() != input_schema.fields().len() + node.expansion_keep_idx.len() {
            return Err(DataFusionError::Internal(
                "AnchoredExpansionExec schema does not match its inputs".into(),
            ));
        }
        Ok(Self::assemble(
            input,
            expansion,
            node.join_keys.clone(),
            node.expansion_keep_idx.clone(),
            node.seed,
            schema,
        ))
    }

    fn assemble(
        input: Arc<dyn ExecutionPlan>,
        expansion: Arc<dyn ExecutionPlan>,
        join_keys: Vec<(usize, usize)>,
        expansion_keep_idx: Vec<usize>,
        seed: u64,
        schema: SchemaRef,
    ) -> Self {
        let partitions = expansion.output_partitioning().partition_count();
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            shared: Arc::new(Shared {
                input,
                expansion,
                join_keys,
                expansion_keep_idx,
                seed,
                prepared: OnceCell::new(),
            }),
            schema,
            props,
        }
    }
}

impl Shared {
    /// Collect the input once, bind its anchors into the expansion, and build
    /// the join every partition probes.
    async fn prepare(&self, context: &Arc<TaskContext>) -> Result<Prepared> {
        let reservation =
            MemoryConsumer::new("AnchoredExpansionInput").register(context.memory_pool());
        let input_schema = self.input.schema();
        let source: Arc<dyn ExecutionPlan> =
            if self.input.output_partitioning().partition_count() > 1 {
                Arc::new(CoalescePartitionsExec::new(Arc::clone(&self.input)))
            } else {
                Arc::clone(&self.input)
            };
        let mut stream = source.execute(0, Arc::clone(context))?;
        let mut batches: Vec<RecordBatch> = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            if batch.num_rows() == 0 {
                continue;
            }
            reservation.try_grow(batch.get_array_memory_size())?;
            batches.push(batch);
        }
        if batches.is_empty() {
            return Ok(Prepared {
                join: None,
                _reservation: reservation,
            });
        }

        // The expansion starts from the rows just collected, not from a second
        // evaluation of the input.
        let expansion = bind_correlated_seed(Arc::clone(&self.expansion), self.seed, &batches)?;
        let expansion_schema = expansion.schema();
        let build = MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&input_schema), None)?;

        let on = self
            .join_keys
            .iter()
            .map(|&(input_col, expansion_col)| {
                (
                    Arc::new(Column::new(input_schema.field(input_col).name(), input_col))
                        as Arc<dyn PhysicalExpr>,
                    Arc::new(Column::new(
                        expansion_schema.field(expansion_col).name(),
                        expansion_col,
                    )) as Arc<dyn PhysicalExpr>,
                )
            })
            .collect();
        let input_width = input_schema.fields().len();
        let projection = (0..input_width)
            .chain(self.expansion_keep_idx.iter().map(|&i| input_width + i))
            .collect();
        let join = HashJoinExec::try_new(
            build,
            expansion,
            on,
            None,
            &JoinType::Inner,
            Some(projection),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        let declared = self.expansion.output_partitioning().partition_count();
        let actual = join.properties().output_partitioning().partition_count();
        if declared != actual {
            return Err(DataFusionError::Internal(format!(
                "anchored expansion declared {declared} partitions but its join has {actual}"
            )));
        }
        Ok(Prepared {
            join: Some(Arc::new(join)),
            _reservation: reservation,
        })
    }
}

impl fmt::Debug for AnchoredExpansionExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AnchoredExpansionExec {{ join_keys: {} }}",
            self.shared.join_keys.len()
        )
    }
}

impl DisplayAs for AnchoredExpansionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AnchoredExpansionExec: keys={}",
            self.shared.join_keys.len()
        )
    }
}

impl ExecutionPlan for AnchoredExpansionExec {
    fn name(&self) -> &str {
        "AnchoredExpansionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.shared.input, &self.shared.expansion]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut it = children.into_iter();
        let (Some(input), Some(expansion), None) = (it.next(), it.next(), it.next()) else {
            return Err(DataFusionError::Internal(
                "AnchoredExpansionExec needs two children".into(),
            ));
        };
        // A fresh build: the previous one belongs to the old children.
        Ok(Arc::new(Self::assemble(
            input,
            expansion,
            self.shared.join_keys.clone(),
            self.shared.expansion_keep_idx.clone(),
            self.shared.seed,
            Arc::clone(&self.schema),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partitions = self.props.output_partitioning().partition_count();
        if partition >= partitions {
            return Err(DataFusionError::Internal(format!(
                "AnchoredExpansionExec has {partitions} partitions, got {partition}"
            )));
        }
        let shared = Arc::clone(&self.shared);
        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&schema);
        let probe = futures::stream::once(async move {
            let prepared = shared
                .prepared
                .get_or_try_init(|| shared.prepare(&context))
                .await?;
            let stream: SendableRecordBatchStream = match &prepared.join {
                Some(join) => join.execute(partition, context)?,
                None => Box::pin(EmptyRecordBatchStream::new(Arc::clone(&stream_schema))),
            };
            Ok::<_, DataFusionError>(stream)
        })
        .try_flatten()
        .map(move |batch| {
            // The join labels its output by its own schema; carry the node's.
            batch.and_then(|batch| {
                RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())
                    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            probe,
        )))
    }
}
