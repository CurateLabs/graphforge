//! Keyed property hydration for a row set a `LIMIT` has already bounded.
//!
//! A node's RETURN-only properties are enriched by a LEFT hash join of the
//! rows against the property route (`join_node_properties` in the relational
//! lowerer). When the preserved side is bounded by a `LIMIT` or a top-k sort
//! the join still streams the whole route: a few tens of rows pay for every
//! row group of a multi-million-row route.
//!
//! [`rewrite`] replaces such a join by a [`KeyedPropertyLookupNode`]. Its
//! physical form, [`KeyedPropertyLookupExec`], collects the bounded rows,
//! reads only those rows' node UUIDs through
//! [`PropertyTable::visit_authenticated_targets`], and appends the route's
//! columns to each row. The decision is made from the plan shape alone; no
//! statistic is consulted.
//!
//! Preserved by construction, because the lookup is the join with its probe
//! side restricted to the keys the build side holds:
//!
//! * LEFT semantics: a row whose UUID has no live property row, or whose UUID
//!   is NULL, receives NULL in every route column, including the right key.
//! * Tombstoned and replaced targets: the storage visitor yields the newest
//!   live row per target and nothing for a tombstoned one, as the streaming
//!   merge does.
//! * Row order and multiplicity: every input row yields exactly one output
//!   row, in input order. A route that yielded two live rows for one UUID
//!   would make the join fan out; the lookup reports that as an error instead
//!   of answering differently.
//! * Authentication: the storage visitor authenticates every row group it
//!   reads against the project's property inventory.

use crate::read_resource::GraphReadContext;
use arrow::array::{
    Array, ArrayRef, BooleanBuilder, FixedSizeBinaryArray, RecordBatch, UInt32Builder,
    new_null_array,
};
use arrow::compute::{concat_batches, filter_record_batch, take};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::{
    DFSchemaRef, DataFusionError, JoinConstraint, JoinType, NullEquality, Result,
    tree_node::Transformed,
};
use datafusion::datasource::source_as_provider;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::{Expr, Extension, Join, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::TryStreamExt;
use graphforge_storage::PropertyTable;
use graphforge_storage::lifecycle_io::CaptureContext;
use std::cmp::Ordering;
use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// The key column of every property route.
const NODE_UUID: &str = "node_uuid";
/// Width of a node UUID.
const UUID_WIDTH: i32 = 16;
/// A slot with no live property row.
const NO_ROW: u32 = u32::MAX;
/// Bytes reserved per distinct lookup key: the key, its slot and set overhead.
const KEY_RESERVATION: usize = 32;

// ---------------------------------------------------------------------------
// Logical rewrite
// ---------------------------------------------------------------------------

/// Replace each LEFT property-enrichment join whose preserved side is bounded
/// by a `LIMIT` or top-k sort with a keyed lookup.
///
/// Call it on a bound plan: the property scans must already carry their
/// providers.
///
/// # Errors
/// Propagates a plan-traversal error.
pub(crate) fn rewrite(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|node| match node {
        LogicalPlan::Join(join) => match keyed_lookup(&join) {
            Some(lookup) => Ok(Transformed::yes(lookup)),
            None => Ok(Transformed::no(LogicalPlan::Join(join))),
        },
        other => Ok(Transformed::no(other)),
    })
    .map(|transformed| transformed.data)
}

/// The keyed lookup for `join`, or `None` when the join is not exactly a
/// bounded LEFT enrichment of node UUIDs by a property route.
fn keyed_lookup(join: &Join) -> Option<LogicalPlan> {
    if join.join_type != JoinType::Left
        || join.join_constraint != JoinConstraint::On
        || join.filter.is_some()
        || join.null_equality != NullEquality::NullEqualsNothing
        || join.null_aware
    {
        return None;
    }
    let [(Expr::Column(left_key), Expr::Column(right_key))] = join.on.as_slice() else {
        return None;
    };
    if right_key.name != NODE_UUID {
        return None;
    }
    let LogicalPlan::TableScan(scan) = join.right.as_ref() else {
        return None;
    };
    if !scan.filters.is_empty() || scan.fetch.is_some() {
        return None;
    }
    let provider = source_as_provider(&scan.source).ok()?;
    let table = provider.downcast_ref::<PropertyTable>()?.clone();
    let uuid = DataType::FixedSizeBinary(UUID_WIDTH);
    let key_index = join.left.schema().index_of_column(left_key).ok()?;
    if join.left.schema().field(key_index).data_type() != &uuid {
        return None;
    }
    let right_index = join.right.schema().index_of_column(right_key).ok()?;
    if join.right.schema().field(right_index).data_type() != &uuid {
        return None;
    }
    let columns: Vec<String> = scan
        .projected_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let route_schema = table.schema_ref();
    if !columns.iter().any(|name| name != NODE_UUID)
        || columns
            .iter()
            .any(|name| route_schema.column_with_name(name).is_none())
        || join.schema.fields().len()
            != join.left.schema().fields().len() + scan.projected_schema.fields().len()
        || !is_bounded(&join.left)
    {
        return None;
    }
    Some(LogicalPlan::Extension(Extension {
        node: Arc::new(KeyedPropertyLookupNode {
            input: Arc::clone(&join.left),
            route: RouteHandle(Arc::new(table)),
            table_name: scan.table_name.to_string(),
            key_index,
            columns,
            schema: Arc::clone(&join.schema),
        }),
    }))
}

/// Whether `plan` emits at most a fixed number of rows because a `LIMIT` or a
/// top-k sort bounds it, reached through operators that never add rows.
///
/// A join or an aggregate ends the walk: the bound would not survive it. An
/// expansion keeps the walk going because the lookup collects its input just
/// as the join's build side does; it reads a subset of the route that join
/// would stream.
fn is_bounded(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Limit(limit) => limit.fetch.is_some() || is_bounded(&limit.input),
        LogicalPlan::Sort(sort) => sort.fetch.is_some() || is_bounded(&sort.input),
        LogicalPlan::Projection(projection) => is_bounded(&projection.input),
        LogicalPlan::Filter(filter) => is_bounded(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => is_bounded(&alias.input),
        LogicalPlan::Window(window) => is_bounded(&window.input),
        LogicalPlan::Extension(extension) => {
            let node = &extension.node;
            let any = node.as_any();
            (any.is::<KeyedPropertyLookupNode>() || any.is::<graphforge_plan::ExpandNode>())
                && node.inputs().first().is_some_and(|input| is_bounded(input))
        }
        _ => false,
    }
}

/// The property route a lookup reads, compared and hashed by identity.
#[derive(Clone, Debug)]
struct RouteHandle(Arc<PropertyTable>);

impl PartialEq for RouteHandle {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for RouteHandle {}

impl Hash for RouteHandle {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

/// Logical form of the keyed lookup. Its schema is the replaced join's: the
/// input's columns followed by the route's.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct KeyedPropertyLookupNode {
    input: Arc<LogicalPlan>,
    route: RouteHandle,
    /// The scan's table name, for display only.
    table_name: String,
    /// Index of the node UUID column in the input.
    key_index: usize,
    /// The route column behind each appended field, in schema order.
    columns: Vec<String>,
    schema: DFSchemaRef,
}

impl PartialOrd for KeyedPropertyLookupNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for KeyedPropertyLookupNode {
    fn name(&self) -> &str {
        "KeyedPropertyLookup"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "KeyedPropertyLookup: route={} columns=[{}]",
            self.table_name,
            self.columns.join(", ")
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        let input = inputs.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("KeyedPropertyLookup needs an input".into())
        })?;
        Ok(Self {
            input: Arc::new(input),
            ..self.clone()
        })
    }
}

// ---------------------------------------------------------------------------
// Physical plan
// ---------------------------------------------------------------------------

/// Physical keyed lookup: collects its input, reads the property rows of the
/// input's node UUIDs, and appends their columns to every input row.
pub(crate) struct KeyedPropertyLookupExec {
    input: Arc<dyn ExecutionPlan>,
    route: RouteHandle,
    table_name: String,
    key_index: usize,
    columns: Vec<String>,
    schema: SchemaRef,
    graph: Arc<GraphReadContext>,
    lifecycle: CaptureContext,
    props: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl KeyedPropertyLookupExec {
    /// Plan `node` over its physical input.
    ///
    /// # Errors
    /// Rejects an input whose width does not match the node's.
    pub(crate) fn try_new(
        node: &KeyedPropertyLookupNode,
        input: Arc<dyn ExecutionPlan>,
        graph: Arc<GraphReadContext>,
    ) -> Result<Self> {
        let schema: SchemaRef = Arc::new(node.schema.as_arrow().clone());
        if input.schema().fields().len() + node.columns.len() != schema.fields().len()
            || node.key_index >= input.schema().fields().len()
        {
            return Err(DataFusionError::Internal(
                "KeyedPropertyLookup input does not match its schema".into(),
            ));
        }
        Ok(Self {
            props: Self::properties_for(&schema, &input),
            input,
            route: node.route.clone(),
            table_name: node.table_name.clone(),
            key_index: node.key_index,
            columns: node.columns.clone(),
            schema,
            graph,
            lifecycle: CaptureContext::current(),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Output properties: the input's ordering carries over because rows leave
    /// in input order and the input's columns keep their positions.
    fn properties_for(schema: &SchemaRef, input: &Arc<dyn ExecutionPlan>) -> Arc<PlanProperties> {
        let orderings = input
            .properties()
            .output_ordering()
            .map(|ordering| ordering.iter().cloned().collect::<Vec<_>>());
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new_with_orderings(Arc::clone(schema), orderings),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            input.properties().boundedness,
        ))
    }

    /// The route columns to read: every appended column except the key.
    fn selected(&self) -> BTreeSet<String> {
        self.columns
            .iter()
            .filter(|name| name.as_str() != NODE_UUID)
            .cloned()
            .collect()
    }
}

impl fmt::Debug for KeyedPropertyLookupExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KeyedPropertyLookupExec {{ route: {} }}",
            self.table_name
        )
    }
}

impl DisplayAs for KeyedPropertyLookupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KeyedPropertyLookupExec: route={}, key=#{}, columns=[{}]",
            self.table_name,
            self.key_index,
            self.columns.join(", ")
        )
    }
}

impl ExecutionPlan for KeyedPropertyLookupExec {
    fn name(&self) -> &str {
        "KeyedPropertyLookupExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = children.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("KeyedPropertyLookupExec needs one child".into())
        })?;
        if input.schema().fields().len() + self.columns.len() != self.schema.fields().len() {
            return Err(DataFusionError::Internal(
                "KeyedPropertyLookupExec child changed width".into(),
            ));
        }
        Ok(Arc::new(Self {
            props: Self::properties_for(&self.schema, &input),
            input,
            route: self.route.clone(),
            table_name: self.table_name.clone(),
            key_index: self.key_index,
            columns: self.columns.clone(),
            schema: Arc::clone(&self.schema),
            graph: Arc::clone(&self.graph),
            lifecycle: self.lifecycle.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "KeyedPropertyLookupExec only has partition 0, got {partition}"
            )));
        }
        let input = self.input.execute(0, Arc::clone(&context))?;
        let left_width = self.schema.fields().len() - self.columns.len();
        let right_schema = Arc::new(Schema::new(self.schema.fields()[left_width..].to_vec()));
        let job = Job {
            input,
            reservation: MemoryConsumer::new("KeyedPropertyLookupExec")
                .register(context.memory_pool()),
            table: Arc::clone(&self.route.0),
            key_index: self.key_index,
            columns: self.columns.clone(),
            selected: self.selected(),
            right_schema,
            batch_size: context.session_config().batch_size(),
            graph: Arc::clone(&self.graph),
            lifecycle: self.lifecycle.clone(),
            schema: Arc::clone(&self.schema),
            targets_metric: MetricBuilder::new(&self.metrics).counter("keyed_targets", 0),
            rows_metric: MetricBuilder::new(&self.metrics).counter("keyed_rows", 0),
        };
        let stream = futures::stream::once(job.prepare())
            .map_ok(|hydrating| {
                futures::stream::try_unfold(hydrating, |mut state| async move {
                    match state.pending.pop_front() {
                        None => Ok(None),
                        Some(batch) => {
                            let out = state.hydrate(&batch)?;
                            state.reservation.shrink(batch.get_array_memory_size());
                            Ok(Some((out, state)))
                        }
                    }
                })
            })
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

/// Everything the lookup needs once its stream is polled.
struct Job {
    input: SendableRecordBatchStream,
    reservation: MemoryReservation,
    table: Arc<PropertyTable>,
    key_index: usize,
    columns: Vec<String>,
    selected: BTreeSet<String>,
    right_schema: SchemaRef,
    batch_size: usize,
    graph: Arc<GraphReadContext>,
    lifecycle: CaptureContext,
    schema: SchemaRef,
    targets_metric: Count,
    rows_metric: Count,
}

/// The collected input and the property rows found for its keys.
struct Hydrating {
    pending: VecDeque<RecordBatch>,
    /// Distinct non-null input keys, strictly ascending.
    targets: Vec<[u8; 16]>,
    /// Per target, its row in `matched`, or [`NO_ROW`].
    slots: Vec<u32>,
    /// The live property rows, columns in output order.
    matched: RecordBatch,
    key_index: usize,
    schema: SchemaRef,
    reservation: MemoryReservation,
}

impl Job {
    /// Collect the input under the memory pool, then read its keys' rows.
    async fn prepare(mut self) -> Result<Hydrating> {
        let mut pending = VecDeque::new();
        let mut keys = BTreeSet::new();
        while let Some(batch) = self.input.try_next().await? {
            self.reservation.try_grow(batch.get_array_memory_size())?;
            for key in key_column(&batch, self.key_index)?.iter().flatten() {
                let key = <[u8; 16]>::try_from(key)
                    .map_err(|_| DataFusionError::Execution("node UUID is not 16 bytes".into()))?;
                if keys.insert(key) {
                    self.reservation.try_grow(KEY_RESERVATION)?;
                }
            }
            pending.push_back(batch);
        }
        let targets: Vec<[u8; 16]> = keys.into_iter().collect();
        self.targets_metric.add(targets.len());
        let read = Read {
            table: self.table,
            targets,
            selected: self.selected,
            columns: self.columns,
            schema: self.right_schema,
            batch_size: self.batch_size,
            graph: self.graph,
            lifecycle: self.lifecycle,
            reservation: self.reservation,
        };
        let gathered = tokio::task::spawn_blocking(move || read.run())
            .await
            .map_err(|error| {
                DataFusionError::Execution(format!("keyed property read: {error}"))
            })??;
        self.rows_metric.add(gathered.matched.num_rows());
        Ok(Hydrating {
            pending,
            targets: gathered.targets,
            slots: gathered.slots,
            matched: gathered.matched,
            key_index: self.key_index,
            schema: self.schema,
            reservation: gathered.reservation,
        })
    }
}

/// The synchronous storage read, moved to a blocking thread.
struct Read {
    table: Arc<PropertyTable>,
    targets: Vec<[u8; 16]>,
    selected: BTreeSet<String>,
    columns: Vec<String>,
    schema: SchemaRef,
    batch_size: usize,
    graph: Arc<GraphReadContext>,
    lifecycle: CaptureContext,
    reservation: MemoryReservation,
}

struct Gathered {
    targets: Vec<[u8; 16]>,
    slots: Vec<u32>,
    matched: RecordBatch,
    reservation: MemoryReservation,
}

impl Read {
    fn run(self) -> Result<Gathered> {
        let Self {
            table,
            targets,
            selected,
            columns,
            schema,
            batch_size,
            graph,
            lifecycle,
            reservation,
        } = self;
        let mut slots = vec![NO_ROW; targets.len()];
        reservation.try_grow(targets.len() * std::mem::size_of::<u32>())?;
        let mut kept: Vec<RecordBatch> = Vec::new();
        let mut kept_bytes = 0_usize;
        let mut found = 0_usize;
        let mut offset = 0_usize;
        {
            // The guard is thread-local and the visit is synchronous.
            let _capture = lifecycle.attach();
            table.visit_authenticated_targets(&targets, &selected, batch_size, |batch| {
                graph
                    .health
                    .check()
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                let uuids = key_column(batch, batch.schema().index_of(NODE_UUID)?)?;
                let mut keep = BooleanBuilder::with_capacity(uuids.len());
                let mut hits = Vec::new();
                for row in 0..uuids.len() {
                    let position = (!uuids.is_null(row))
                        .then(|| {
                            let uuid = uuids.value(row);
                            targets
                                .binary_search_by(|target| target.as_slice().cmp(uuid))
                                .ok()
                        })
                        .flatten();
                    match position {
                        Some(at) if slots[at] != NO_ROW => {
                            return Err(DataFusionError::Execution(
                                "property route holds two live rows for one node".into(),
                            ));
                        }
                        Some(at) => {
                            slots[at] = NO_ROW - 1;
                            hits.push(at);
                            keep.append_value(true);
                        }
                        None => keep.append_value(false),
                    }
                }
                if hits.is_empty() {
                    return Ok(found < targets.len());
                }
                let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
                for (name, field) in columns.iter().zip(schema.fields()) {
                    let array = batch.column(batch.schema().index_of(name)?);
                    if array.data_type() != field.data_type() {
                        return Err(DataFusionError::Execution(format!(
                            "property column {name} is {} where the plan expects {}",
                            array.data_type(),
                            field.data_type()
                        )));
                    }
                    arrays.push(Arc::clone(array));
                }
                let projected = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
                let filtered = filter_record_batch(&projected, &keep.finish())?;
                for (row, at) in hits.into_iter().enumerate() {
                    slots[at] = u32::try_from(offset + row)
                        .map_err(|_| DataFusionError::Execution("too many hydrated rows".into()))?;
                }
                found += filtered.num_rows();
                offset += filtered.num_rows();
                let bytes = filtered.get_array_memory_size();
                reservation.try_grow(bytes)?;
                kept_bytes += bytes;
                kept.push(filtered);
                Ok(found < targets.len())
            })?;
        }
        let matched = match kept.len() {
            0 => RecordBatch::new_empty(Arc::clone(&schema)),
            1 => kept.remove(0),
            _ => {
                let combined = concat_batches(&schema, &kept)?;
                reservation.try_grow(combined.get_array_memory_size())?;
                reservation.shrink(kept_bytes);
                combined
            }
        };
        Ok(Gathered {
            targets,
            slots,
            matched,
            reservation,
        })
    }
}

impl Hydrating {
    /// `batch` with the matched property columns appended, one output row per
    /// input row.
    fn hydrate(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let keys = key_column(batch, self.key_index)?;
        let mut indices = UInt32Builder::with_capacity(batch.num_rows());
        for row in 0..keys.len() {
            let slot = (!keys.is_null(row))
                .then(|| {
                    let key = keys.value(row);
                    self.targets
                        .binary_search_by(|target| target.as_slice().cmp(key))
                        .ok()
                        .map(|at| self.slots[at])
                        .filter(|slot| *slot != NO_ROW)
                })
                .flatten();
            indices.append_option(slot);
        }
        let indices = indices.finish();
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        for property in self.matched.columns() {
            columns.push(if self.matched.num_rows() == 0 {
                new_null_array(property.data_type(), batch.num_rows())
            } else {
                take(property.as_ref(), &indices, None)?
            });
        }
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

/// `batch`'s column `index` as 16-byte node UUIDs.
fn key_column(batch: &RecordBatch, index: usize) -> Result<&FixedSizeBinaryArray> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .filter(|array| array.value_length() == UUID_WIDTH)
        .ok_or_else(|| {
            DataFusionError::Execution("node UUID column is not FixedSizeBinary(16)".into())
        })
}
