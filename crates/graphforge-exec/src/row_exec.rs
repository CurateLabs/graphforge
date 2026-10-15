//! OPTIONAL MATCH and UNWIND physical execution.

use crate::to_df_err;
use arrow::array::Array;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::Expr as DfExpr;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, RecordOutput,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use graphforge_core::GfError;
use graphforge_plan::CorrelatedSeedNode;
use graphforge_plan::OptionalMatchNode;
use graphforge_plan::UnwindNode;
use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

// ---------------------------------------------------------------------------
// OptionalMatchExec — physical node for OPTIONAL MATCH
// ---------------------------------------------------------------------------

/// Physical execution node for `OPTIONAL MATCH`, the physical counterpart of
/// [`OptionalMatchNode`].
///
/// Left-joins the `outer` (mandatory) input against the `optional` sub-plan on
/// the shared-variable [`join_keys`], preserving every outer row and setting the
/// optional-side columns to **null** when there is no match (openCypher
/// null-shaping). The shared join-key columns are carried by the outer side and
/// dropped from the inner side of the output (consistent with
/// [`OptionalMatchNode`]'s schema). When `join_keys` is empty the match is
/// unconditional (every outer row pairs with every inner row, or null-shapes if
/// the inner side is empty).
pub struct OptionalMatchExec {
    outer: Arc<dyn ExecutionPlan>,
    inner: Arc<dyn ExecutionPlan>,
    join_keys: Vec<(usize, usize)>,
    /// Inner column indices to append to the output, in order (every shared-
    /// variable column already excluded — those come from the outer side).
    inner_keep_idx: Vec<usize>,
    /// The [`CorrelatedSeedExec`] leaves of `inner` that stand for the outer
    /// rows; bound to the executed outer rows on every execution.
    correlated_seed: Option<u64>,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    /// What `EXPLAIN ANALYZE` shows; shared by every copy of this node.
    metrics: ExecutionPlanMetricsSet,
    /// The bound copy of `inner` this node executed. A correlated execution
    /// runs a rebound copy of `inner`, and the operators of that copy own
    /// their metrics, so `children` shows the executed copy once it exists.
    /// Only the first execution is kept.
    executed_inner: Arc<OnceLock<Arc<dyn ExecutionPlan>>>,
}

impl OptionalMatchExec {
    /// Build the physical node from its logical counterpart and planned inputs.
    #[must_use]
    pub fn new(
        node: &OptionalMatchNode,
        outer: Arc<dyn ExecutionPlan>,
        inner: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let schema: SchemaRef = Arc::new(node.schema().as_arrow().clone());
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            outer,
            inner,
            join_keys: node.join_keys.clone(),
            inner_keep_idx: node.inner_keep_idx.clone(),
            correlated_seed: node.correlated_seed,
            schema,
            props,
            metrics: ExecutionPlanMetricsSet::new(),
            executed_inner: Arc::new(OnceLock::new()),
        }
    }

    /// Split a demand on this node's output columns into the columns each
    /// input must really produce: `(outer, inner)`.
    ///
    /// The output is the outer columns followed by `inner_keep_idx` inner
    /// columns. Beyond the demanded output, the join reads the key columns of
    /// both sides. A correlated sub-plan is seeded with the executed outer
    /// rows, so the outer side must also produce whatever the sub-plan reads
    /// through its [`CorrelatedSeedExec`] leaves; that demand is only known
    /// once the inner plan has been rewritten, so the caller adds
    /// [`correlated_seed_demand`] to the returned outer set.
    pub(crate) fn input_demand(
        &self,
        required: &BTreeSet<usize>,
    ) -> (BTreeSet<usize>, BTreeSet<usize>) {
        let outer_width = self.outer.schema().fields().len();
        let mut outer_required: BTreeSet<usize> = required
            .iter()
            .copied()
            .filter(|index| *index < outer_width)
            .collect();
        let mut inner_required = BTreeSet::new();
        for &(outer_key, inner_key) in &self.join_keys {
            outer_required.insert(outer_key);
            inner_required.insert(inner_key);
        }
        inner_required.extend(
            required
                .iter()
                .filter_map(|index| index.checked_sub(outer_width))
                .filter_map(|kept| self.inner_keep_idx.get(kept).copied()),
        );
        (outer_required, inner_required)
    }

    /// The seed id the inner plan reads the outer rows through, when the
    /// sub-plan is correlated.
    pub(crate) fn correlated_seed_id(&self) -> Option<u64> {
        self.correlated_seed
    }
}

/// The outer columns the sub-plan `inner` reads through its
/// [`CorrelatedSeedExec`] leaves bound to `seed`: the union of what each leaf
/// was asked to produce, and every column for a leaf no demand reached.
pub(crate) fn correlated_seed_demand(inner: &Arc<dyn ExecutionPlan>, seed: u64) -> BTreeSet<usize> {
    let mut demand = BTreeSet::new();
    let mut pending = vec![inner];
    while let Some(plan) = pending.pop() {
        match plan.downcast_ref::<CorrelatedSeedExec>() {
            Some(leaf) if leaf.seed == seed => demand.extend(leaf.demanded_columns()),
            _ => pending.extend(plan.children()),
        }
    }
    demand
}

impl fmt::Debug for OptionalMatchExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OptionalMatchExec {{ join_keys: {} }}",
            self.join_keys.len()
        )
    }
}

impl DisplayAs for OptionalMatchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OptionalMatchExec: keys={}", self.join_keys.len())
    }
}

impl ExecutionPlan for OptionalMatchExec {
    fn name(&self) -> &str {
        "OptionalMatchExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![
            &self.outer,
            self.executed_inner.get().unwrap_or(&self.inner),
        ]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut it = children.into_iter();
        let (Some(outer), Some(inner)) = (it.next(), it.next()) else {
            return Err(DataFusionError::Internal(
                "OptionalMatchExec needs two children".into(),
            ));
        };
        Ok(Arc::new(Self {
            outer,
            inner,
            join_keys: self.join_keys.clone(),
            inner_keep_idx: self.inner_keep_idx.clone(),
            correlated_seed: self.correlated_seed,
            schema: self.schema.clone(),
            props: self.props.clone(),
            metrics: self.metrics.clone(),
            executed_inner: Arc::new(OnceLock::new()),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "OptionalMatchExec only has partition 0, got {partition}"
            )));
        }
        let outer = self.outer.clone();
        let inner = self.inner.clone();
        let correlated_seed = self.correlated_seed;
        let cfg = OptionalConfig {
            join_keys: self.join_keys.clone(),
            inner_keep_idx: self.inner_keep_idx.clone(),
            null_safe_keys: self.correlated_seed.is_some(),
            out_schema: self.schema.clone(),
            // Carry the child schemas so concat_batches has a schema even when a
            // child yields zero batches (an empty inner must null-shape, not
            // error; an empty outer must use the outer — not full — schema).
            outer_schema: self.outer.schema(),
            inner_schema: self.inner.schema(),
        };
        let schema = self.schema.clone();
        let metrics = OptionalMetrics::new(&self.metrics, partition);
        let executed_inner = Arc::clone(&self.executed_inner);
        let fut = async move {
            let budget = optional_memory::Budget::new(&context);
            let outer_batches = budget.collect(outer, context.clone()).await?;
            metrics.outer_rows.add(row_count(&outer_batches.batches));
            // A correlated sub-plan starts from the rows just collected, not
            // from a second evaluation of the outer plan (#1919).
            let (inner, seed_release) = match correlated_seed {
                Some(seed) => {
                    let _timer = metrics.baseline.elapsed_compute().timer();
                    budget.seed(&outer_batches.batches)?;
                    let (bound, rows) = bind_seed_rows(inner, seed, &outer_batches.batches)?;
                    let release = SeedReleaseGuard(rows);
                    // Keep the copy that runs: its operators own the metrics.
                    let _ = executed_inner.set(Arc::clone(&bound));
                    (bound, Some(release))
                }
                None => (inner, None),
            };
            let collected = budget.collect(inner, context).await;
            // The executed copy stays for display; release its rows on success
            // or error here, and on cancellation or panic through the guard.
            drop(seed_release);
            let inner_batches = collected?;
            metrics.inner_rows.add(row_count(&inner_batches.batches));
            let _timer = metrics.baseline.elapsed_compute().timer();
            let joined = optional_memory::join(&cfg, outer_batches, inner_batches, &budget)?;
            budget.output(&joined)?;
            Ok::<_, DataFusionError>((joined.record_output(&metrics.baseline), budget))
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            // A yielded batch remains owned by the consumer until its next poll.
            // Keep its charge in the stream state through that handoff and drop.
            futures::stream::try_unfold((Some(fut), None), |(future, held)| async move {
                drop(held);
                let Some(future) = future else {
                    return Ok(None);
                };
                let (batch, budget) = future.await?;
                Ok::<_, DataFusionError>(Some((batch, (None, Some(budget)))))
            }),
        )))
    }
}

/// The outer rows every bound [`CorrelatedSeedExec`] of one execution reads.
/// The executed sub-plan stays reachable for display after the execution, so
/// the owner releases the rows once the sub-plan has been collected.
#[derive(Clone)]
struct SeedRows(Arc<Mutex<Option<Vec<RecordBatch>>>>);

impl SeedRows {
    fn new(rows: Vec<RecordBatch>) -> Self {
        Self(Arc::new(Mutex::new(Some(rows))))
    }

    /// A copy of the rows, or `None` once they were released.
    #[cfg(test)]
    fn get(&self) -> Option<Vec<RecordBatch>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn release(&self) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

/// One execution's ownership of the bound seed rows. The display plan keeps
/// the bound leaves, so dropping the execution must release their buffers even
/// when collection is cancelled or unwinds before returning.
struct SeedReleaseGuard(SeedRows);

impl Drop for SeedReleaseGuard {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// Replace every [`CorrelatedSeedExec`] with this `seed` id in `plan` by one
/// that yields `rows`.
fn bind_correlated_seed(
    plan: Arc<dyn ExecutionPlan>,
    seed: u64,
    rows: &[RecordBatch],
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    bind_seed_rows(plan, seed, rows).map(|(bound, _)| bound)
}

/// [`bind_correlated_seed`], also returning the handle that releases the rows
/// the bound leaves share.
fn bind_seed_rows(
    plan: Arc<dyn ExecutionPlan>,
    seed: u64,
    rows: &[RecordBatch],
) -> Result<(Arc<dyn ExecutionPlan>, SeedRows), DataFusionError> {
    use datafusion::common::tree_node::{Transformed, TreeNode};

    let rows = SeedRows::new(rows.to_vec());
    let bound = plan.transform_up(|node| {
        let leaf = node
            .downcast_ref::<CorrelatedSeedExec>()
            .filter(|leaf| leaf.seed == seed);
        match leaf {
            Some(leaf) => {
                let mut bound = CorrelatedSeedExec::with_rows(seed, leaf.schema.clone(), None);
                bound.rows = Some(rows.clone());
                Ok(Transformed::yes(Arc::new(bound) as Arc<dyn ExecutionPlan>))
            }
            None => Ok(Transformed::no(node)),
        }
    })?;
    if !bound.transformed {
        return Err(DataFusionError::Internal(format!(
            "correlated optional match has no seed leaf {seed} in its sub-plan"
        )));
    }
    Ok((bound.data, rows))
}

/// Physical leaf of a correlated sub-plan: the outer rows of the
/// [`OptionalMatchExec`] that owns it, the counterpart of
/// [`graphforge_plan::CorrelatedSeedNode`].
///
/// Planned unbound; the owning [`OptionalMatchExec`] replaces it with a bound
/// copy holding the rows it collected from its outer input. Executing an
/// unbound seed is an error.
pub struct CorrelatedSeedExec {
    seed: u64,
    schema: SchemaRef,
    rows: Option<SeedRows>,
    /// The output columns the sub-plan above this leaf reads, once the demand
    /// pass has run; `None` means every column.
    demanded: Option<BTreeSet<usize>>,
    props: Arc<PlanProperties>,
}

impl CorrelatedSeedExec {
    /// An unbound seed leaf for `node`.
    #[must_use]
    pub fn new(node: &CorrelatedSeedNode) -> Self {
        let schema: SchemaRef = Arc::new(node.schema().as_arrow().clone());
        Self::with_rows(node.id(), schema, None)
    }

    fn with_rows(seed: u64, schema: SchemaRef, rows: Option<SeedRows>) -> Self {
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            seed,
            schema,
            rows,
            demanded: None,
            props,
        }
    }

    /// This leaf recording that the sub-plan above it reads only `demanded`
    /// of its columns.
    pub(crate) fn with_demand(&self, demanded: BTreeSet<usize>) -> Self {
        let width = self.schema.fields().len();
        let mut leaf = Self::with_rows(self.seed, self.schema.clone(), self.rows.clone());
        leaf.demanded = Some(
            demanded
                .into_iter()
                .filter(|index| *index < width)
                .collect(),
        );
        leaf
    }

    /// The columns the sub-plan above this leaf reads: the recorded demand,
    /// or every column when none was recorded.
    pub(crate) fn demanded_columns(&self) -> BTreeSet<usize> {
        self.demanded
            .clone()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect())
    }
}

impl fmt::Debug for CorrelatedSeedExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CorrelatedSeedExec {{ seed: {} }}", self.seed)
    }
}

impl DisplayAs for CorrelatedSeedExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CorrelatedSeedExec")?;
        match &self.demanded {
            Some(demanded) if demanded.len() < self.schema.fields().len() => {
                write!(f, ": projection={{{}}}", demanded.len())
            }
            _ => Ok(()),
        }
    }
}

impl ExecutionPlan for CorrelatedSeedExec {
    fn name(&self) -> &str {
        "CorrelatedSeedExec"
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
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "CorrelatedSeedExec has no children".into(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "CorrelatedSeedExec only has partition 0, got {partition}"
            )));
        }
        let bound = self.rows.as_ref().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "correlated seed {} executed before its outer rows were bound",
                self.seed
            ))
        })?;
        // Each leaf execution owns a fresh batch/column handle collection.
        // Admit under the seed lock before cloning, from the same query pool.
        let rows = bound
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let rows = rows.as_ref().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "correlated seed {} executed after its outer rows were released",
                self.seed
            ))
        })?;
        let reservation =
            datafusion::execution::memory_pool::MemoryConsumer::new("OptionalMatch seed handles")
                .register(context.memory_pool());
        // MemoryStream retains the collection and clones a batch on poll. Two
        // complete handle sets cover that handoff without charging payload twice.
        reservation.try_grow(optional_memory::mul(
            optional_memory::handle_bytes(rows)?,
            2,
        )?)?;
        let stream = MemoryStream::try_new(rows.clone(), self.schema.clone(), None)?
            .with_reservation(reservation);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::try_unfold(stream, |mut stream| async move {
                use futures::TryStreamExt;
                Ok::<_, DataFusionError>(stream.try_next().await?.map(|batch| (batch, stream)))
            }),
        )))
    }
}

/// The metrics of one execution of an [`OptionalMatchExec`]: the baseline
/// (`output_rows`, `elapsed_compute`) plus the rows it joined.
struct OptionalMetrics {
    baseline: BaselineMetrics,
    outer_rows: Count,
    inner_rows: Count,
}

impl OptionalMetrics {
    fn new(set: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(set, partition),
            outer_rows: MetricBuilder::new(set).counter("outer_rows", partition),
            inner_rows: MetricBuilder::new(set).counter("inner_rows", partition),
        }
    }
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Owned config for [`optional_memory::join`] (so it can run in a `'static` future).
pub(super) struct OptionalConfig {
    pub(super) join_keys: Vec<(usize, usize)>,
    /// Inner column indices to append (in order); every shared-variable column
    /// already excluded. Source of truth shared with the node's output schema.
    pub(super) inner_keep_idx: Vec<usize>,
    /// Null keys equal null keys and keys may be any value type (a correlated
    /// sub-plan keyed on every outer column, #1887 D15).
    pub(super) null_safe_keys: bool,
    pub(super) out_schema: SchemaRef,
    /// Outer child's schema — used for `concat_batches` so an empty outer
    /// stream still yields a correctly-typed (zero-row) batch.
    pub(super) outer_schema: SchemaRef,
    /// Inner child's schema — used so an empty inner stream null-shapes (rather
    /// than erroring) instead of having no schema to concat against.
    pub(super) inner_schema: SchemaRef,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum OptionalJoinKey {
    U64(u64),
    Uuid([u8; 16]),
    /// Any value, null included, for null-safe keys.
    Value(datafusion::scalar::ScalarValue),
}

// ---------------------------------------------------------------------------
// UnwindExec — physical node for UNWIND
// ---------------------------------------------------------------------------

/// Physical execution node for `UNWIND`, the physical counterpart of
/// [`UnwindNode`].
///
/// For each input row, evaluates `list_expr` to a list and emits one output row
/// per element — the input columns plus a new `alias` column bound to the
/// element. A null or empty list yields **zero** rows for that input row
/// (the input row is dropped), matching openCypher.
///
/// Copying an input column once per exploded element is the cost of this node,
/// and a wide input (a collected list of structs, a node's properties) makes it
/// large. [`UnwindExec::with_required_output`] records the output columns the
/// parent reads; the others are emitted as nulls instead of being copied. The
/// schema never changes, and a column whose field is not nullable is always
/// copied.
pub struct UnwindExec {
    input: Arc<dyn ExecutionPlan>,
    list_expr: DfExpr,
    input_schema: SchemaRef,
    /// The logical input's QUALIFIED schema (`var_0.name`, …). `list_expr`
    /// references qualified columns, so it must be planned against this — NOT a
    /// `DFSchema` rebuilt from the physical Arrow schema, whose field names are
    /// unqualified (`name`) and would fail to resolve `var_0.name` (#599/#28).
    input_dfschema: datafusion::common::DFSchemaRef,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
    /// Output columns the parent reads; `None` means every column. Only input
    /// columns outside this set are skipped. The element column is always built.
    required_output: Option<BTreeSet<usize>>,
}

impl UnwindExec {
    /// Build the physical node from its logical counterpart and planned input.
    #[must_use]
    pub fn new(node: &UnwindNode, input: Arc<dyn ExecutionPlan>) -> Self {
        let schema: SchemaRef = Arc::new(node.schema().as_arrow().clone());
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        let input_schema = input.schema();
        Self {
            input,
            list_expr: node.list_expr.clone(),
            input_schema,
            input_dfschema: node.input.schema().clone(),
            schema,
            props,
            required_output: None,
        }
    }

    /// The input columns this node needs to produce `required` output columns:
    /// the demanded input columns plus every column the list expression reads.
    /// The list column itself is needed to explode the rows even when the
    /// parent never reads it.
    ///
    /// # Errors
    ///
    /// Returns the planning error if the list expression cannot be planned
    /// against the input schema.
    pub(crate) fn input_demand(
        &self,
        required: &BTreeSet<usize>,
    ) -> Result<BTreeSet<usize>, DataFusionError> {
        let input_width = self.input_schema.fields().len();
        let mut demand: BTreeSet<usize> = required
            .iter()
            .copied()
            .filter(|index| *index < input_width)
            .collect();
        let list = create_physical_expr(
            &self.list_expr,
            &self.input_dfschema,
            &ExecutionProps::new(),
        )?;
        demand.extend(
            datafusion::physical_expr::utils::collect_columns(&list)
                .iter()
                .map(datafusion::physical_expr::expressions::Column::index),
        );
        Ok(demand)
    }

    /// The same node emitting only the `required` output columns (plus the
    /// element column). Call it on a node whose input was already narrowed to
    /// [`UnwindExec::input_demand`].
    #[must_use]
    pub(crate) fn with_required_output(&self, required: BTreeSet<usize>) -> Arc<dyn ExecutionPlan> {
        let input_width = self.input_schema.fields().len();
        let narrowed = (0..input_width).any(|index| !required.contains(&index));
        Arc::new(Self {
            input: Arc::clone(&self.input),
            list_expr: self.list_expr.clone(),
            input_schema: self.input_schema.clone(),
            input_dfschema: self.input_dfschema.clone(),
            schema: self.schema.clone(),
            props: self.props.clone(),
            required_output: narrowed.then_some(required),
        })
    }

    /// How many input columns are copied per element: the demanded ones, and
    /// the non-nullable ones, which a null array cannot stand in for.
    fn copied_columns(&self) -> usize {
        let width = self.input_schema.fields().len();
        self.required_output.as_ref().map_or(width, |required| {
            (0..width)
                .filter(|index| {
                    required.contains(index) || !self.schema.field(*index).is_nullable()
                })
                .count()
        })
    }
}

impl fmt::Debug for UnwindExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnwindExec {{ list_expr: {} }}", self.list_expr)
    }
}

impl DisplayAs for UnwindExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnwindExec: list={}", self.list_expr)?;
        // Printed only for a narrowed node so an unmasked plan reads as before.
        if self.required_output.is_some() {
            write!(f, ", copied={}", self.copied_columns())?;
        }
        Ok(())
    }
}

impl ExecutionPlan for UnwindExec {
    fn name(&self) -> &str {
        "UnwindExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = children
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("UnwindExec needs one child".into()))?;
        // Derive the input schema from the NEW child (not the stale self value):
        // an optimizer may replace the child with a different-schema plan, and
        // unwind_explode concats/evaluates against this schema.
        let input_schema = input.schema();
        Ok(Arc::new(Self {
            input,
            list_expr: self.list_expr.clone(),
            input_schema,
            // Qualifiers + column order are preserved across child replacement, so
            // the original qualified DFSchema stays valid for resolving list_expr.
            input_dfschema: self.input_dfschema.clone(),
            schema: self.schema.clone(),
            props: self.props.clone(),
            required_output: self.required_output.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "UnwindExec only has partition 0, got {partition}"
            )));
        }
        let input = self.input.clone();
        let cfg = UnwindConfig {
            list_expr: self.list_expr.clone(),
            input_schema: self.input_schema.clone(),
            input_dfschema: self.input_dfschema.clone(),
            out_schema: self.schema.clone(),
            required_output: self.required_output.clone(),
        };
        let schema = self.schema.clone();
        let fut = async move {
            let input_batches = collect(input, context).await?;
            unwind_explode(&cfg, &input_batches).map_err(to_df_err)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(fut),
        )))
    }
}

/// Owned config for [`unwind_explode`] (so it can run in a `'static` future).
struct UnwindConfig {
    list_expr: DfExpr,
    input_schema: SchemaRef,
    input_dfschema: datafusion::common::DFSchemaRef,
    out_schema: SchemaRef,
    /// Output columns the parent reads (`None`: all). See [`UnwindExec`].
    required_output: Option<BTreeSet<usize>>,
}

/// Evaluate `list_expr` per input row and explode the resulting list into one
/// output row per element (input columns + the element column).
fn unwind_explode(
    cfg: &UnwindConfig,
    input_batches: &[RecordBatch],
) -> Result<RecordBatch, GfError> {
    use arrow::array::{ListArray, UInt32Array};
    use arrow::compute::{concat, take};

    let exec_err = |m: String| GfError::Execution(m);

    let input = arrow::compute::concat_batches(&cfg.input_schema, input_batches)
        .map_err(|e| exec_err(e.to_string()))?;

    // Evaluate the list expression against the input batch → one list per row.
    // Plan it against the QUALIFIED logical input schema: `list_expr` references
    // qualified columns (`var_0.name`), which a `DFSchema` rebuilt from the
    // physical Arrow schema (unqualified field names) cannot resolve (#599/#28).
    // `create_physical_expr` resolves them to column indices, which align with the
    // physical batch (same column order).
    let phys = create_physical_expr(&cfg.list_expr, &cfg.input_dfschema, &ExecutionProps::new())
        .map_err(|e| exec_err(e.to_string()))?;
    let list_values = phys
        .evaluate(&input)
        .and_then(|cv| cv.into_array(input.num_rows()))
        .map_err(|e| exec_err(e.to_string()))?;

    if matches!(list_values.data_type(), arrow::datatypes::DataType::Null) {
        let columns = cfg
            .out_schema
            .fields()
            .iter()
            .map(|field| arrow::array::new_empty_array(field.data_type()))
            .collect();
        return RecordBatch::try_new(cfg.out_schema.clone(), columns)
            .map_err(|e| exec_err(e.to_string()));
    }

    let lists = list_values
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| {
            exec_err(format!(
                "UNWIND expression must evaluate to a list, got {:?}",
                list_values.data_type()
            ))
        })?;

    // For each input row, repeat its index once per list element (null/empty
    // list → contributes nothing), and collect the element sub-arrays.
    let mut input_take: Vec<u32> = Vec::new();
    let mut element_parts: Vec<arrow::array::ArrayRef> = Vec::new();
    for row in 0..input.num_rows() {
        if lists.is_null(row) {
            continue; // null list → zero rows
        }
        let elems = lists.value(row); // the element sub-array for this row
        let n = elems.len();
        if n == 0 {
            continue; // empty list → zero rows
        }
        let row_u32 =
            u32::try_from(row).map_err(|_| exec_err(format!("row index {row} exceeds u32")))?;
        input_take.extend(std::iter::repeat_n(row_u32, n));
        element_parts.push(elems);
    }

    let input_take = UInt32Array::from(input_take);
    let mut columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(input.num_columns() + 1);
    for (index, col) in input.columns().iter().enumerate() {
        let field = cfg.out_schema.field(index);
        let skipped = field.is_nullable()
            && cfg
                .required_output
                .as_ref()
                .is_some_and(|required| !required.contains(&index));
        columns.push(if skipped {
            // Nobody reads this column: a null array is not a copy of it.
            arrow::array::new_null_array(field.data_type(), input_take.len())
        } else {
            take(col, &input_take, None).map_err(|e| exec_err(e.to_string()))?
        });
    }
    // The element column is the concatenation of every row's element sub-array,
    // already aligned with `input_take` (which repeats each row per element).
    let element_col: arrow::array::ArrayRef = if element_parts.is_empty() {
        // No elements at all — build an empty array of the output element type.
        let field = cfg.out_schema.field(cfg.out_schema.fields().len() - 1);
        arrow::array::new_empty_array(field.data_type())
    } else {
        let refs: Vec<&dyn Array> = element_parts.iter().map(AsRef::as_ref).collect();
        concat(&refs).map_err(|e| exec_err(e.to_string()))?
    };
    if cfg.out_schema.fields().len() == input.num_columns() + 1 {
        columns.push(element_col);
    } else {
        let values = element_col
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .ok_or_else(|| {
                exec_err(format!(
                    "UNWIND entity output requires a struct element, got {:?}",
                    element_col.data_type()
                ))
            })?;
        columns.extend(values.columns().iter().cloned());
    }

    RecordBatch::try_new(cfg.out_schema.clone(), columns).map_err(|e| exec_err(e.to_string()))
}

mod anchored_expansion;
mod optional_memory;
pub use anchored_expansion::AnchoredExpansionExec;

#[cfg(test)]
pub(super) mod tests;

#[cfg(test)]
mod optional_memory_tests;
