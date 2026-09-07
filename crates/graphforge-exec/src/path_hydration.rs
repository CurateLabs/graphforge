//! Execution-owned path-node hydration and query-local resources.
use arrow::array::Array;
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use graphforge_rel::expr::PathNodeHydration;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) struct HydrationResource {
    pub(crate) graph: Arc<crate::read_resource::GraphReadContext>,
    pub(crate) pool: Arc<dyn MemoryPool>,
    cancelled: AtomicBool,
    used: AtomicBool,
    peak_gathered: AtomicU64,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    counters: HashMap<&'static str, Count>,
}
impl std::fmt::Debug for HydrationResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HydrationResource").finish_non_exhaustive()
    }
}
impl HydrationResource {
    pub(crate) fn new(
        graph: Arc<crate::read_resource::GraphReadContext>,
        pool: Arc<dyn MemoryPool>,
    ) -> Arc<Self> {
        let metrics = ExecutionPlanMetricsSet::new();
        let counters = [
            "requested",
            "resolved",
            "node_batches",
            "node_rows",
            "node_gathered",
            "stems",
            "property_batches",
            "property_rows",
            "property_gathered",
            "gathered_entries",
        ]
        .into_iter()
        .map(|name| {
            (
                name,
                MetricBuilder::new(&metrics).counter(format!("hydration_{name}"), 0),
            )
        })
        .collect();
        Arc::new(Self {
            graph,
            pool,
            cancelled: AtomicBool::new(false),
            used: AtomicBool::new(false),
            peak_gathered: AtomicU64::new(0),
            metrics,
            counters,
        })
    }
    fn check(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(DataFusionError::ResourcesExhausted(
                "cypher_path_nodes: hydration cancelled".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
    pub(crate) fn used(&self) -> bool {
        self.used.load(Ordering::Acquire)
    }
    fn record(&self, name: &'static str, count: u64) {
        let count = if name == "gathered_entries" {
            let old = self.peak_gathered.fetch_max(count, Ordering::Relaxed);
            count.saturating_sub(old)
        } else {
            count
        };
        self.counters[name].add(usize::try_from(count).unwrap_or(usize::MAX));
    }
    pub(crate) fn bind(self: &Arc<Self>, expr: Expr) -> Result<Expr> {
        graphforge_rel::expr::rewrite_embedded_expressions(expr, &mut |mut expr| {
            if let Expr::ScalarFunction(call) = &mut expr {
                if let Some(descriptor) =
                    graphforge_rel::expr::path_node_hydration_descriptor(&call.func)
                {
                    let snapshot = self
                        .graph
                        .catalog
                        .lowering_snapshot(Some(&self.graph.dir))
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;
                    let mut fields = vec![
                        arrow::datatypes::Field::new(
                            "node_uuid",
                            DataType::FixedSizeBinary(16),
                            false,
                        ),
                        arrow::datatypes::Field::new(
                            "labels",
                            DataType::new_list(DataType::Utf8, true),
                            true,
                        ),
                    ];
                    let mut seen = fields
                        .iter()
                        .map(|f| f.name().clone())
                        .collect::<std::collections::HashSet<_>>();
                    for stem in &snapshot.node_property_stems {
                        let schema = snapshot.node_properties.get(stem).ok_or_else(|| {
                            DataFusionError::Plan(
                                "GF_READ_RESOURCE_INCOMPATIBLE: path hydration schema".into(),
                            )
                        })?;
                        for field in schema.fields() {
                            if seen.insert(field.name().clone()) {
                                fields.push(field.as_ref().clone().with_nullable(true));
                            }
                        }
                    }
                    let mut labels = graphforge_rel::GraphPlanLowerer::new(
                        Some(&snapshot),
                        self.graph.ontology.as_ref(),
                    )
                    .map_err(|error| DataFusionError::Plan(error.to_string()))?
                    .read_contract()
                    .labels;
                    labels.sort_by(|a, b| {
                        a.0.encode().cmp(&b.0.encode()).then_with(|| a.1.cmp(&b.1))
                    });
                    if descriptor.prop_stems != snapshot.node_property_stems
                        || descriptor.fields != fields.into()
                        || descriptor.labels_by_type != labels
                    {
                        return Err(DataFusionError::Plan(
                            "GF_READ_RESOURCE_INCOMPATIBLE: path hydration schema or labels".into(),
                        ));
                    }
                    self.used.store(true, Ordering::Release);
                    call.func = Arc::new(ScalarUDF::new_from_impl(HydratedPathNodes {
                        signature: Signature::any(2, Volatility::Volatile),
                        tables: descriptor
                            .prop_stems
                            .iter()
                            .map(|stem| self.graph.catalog.property_table(&self.graph.dir, stem))
                            .collect(),
                        descriptor: descriptor.clone(),
                        resource: Arc::clone(self),
                    }));
                }
            }
            Ok(expr)
        })
    }
}

#[derive(Debug)]
struct HydratedPathNodes {
    signature: Signature,
    descriptor: PathNodeHydration,
    tables: Vec<graphforge_storage::PropertyTable>,
    resource: Arc<HydrationResource>,
}
impl PartialEq for HydratedPathNodes {
    fn eq(&self, other: &Self) -> bool {
        self.descriptor == other.descriptor && Arc::ptr_eq(&self.resource, &other.resource)
    }
}
impl Eq for HydratedPathNodes {}
impl Hash for HydratedPathNodes {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.descriptor.hash(state);
        Arc::as_ptr(&self.resource).hash(state);
    }
}
impl ScalarUDFImpl for HydratedPathNodes {
    fn name(&self) -> &'static str {
        "cypher_path_nodes"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::new_list(
            DataType::Struct(self.descriptor.fields.clone()),
            true,
        ))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.resource.check()?;
        let invocation = Invocation {
            descriptor: &self.descriptor,
            tables: &self.tables,
            resource: &self.resource,
            reservation: Mutex::new(
                MemoryConsumer::new("path_node_hydration").register(&self.resource.pool),
            ),
        };
        graphforge_rel::expr::evaluate_path_nodes(
            args,
            self.descriptor.fields.clone(),
            |flat, batch_size| hydrate_path_node_children(&invocation, flat, batch_size),
        )
    }
}
struct Invocation<'a> {
    descriptor: &'a PathNodeHydration,
    tables: &'a [graphforge_storage::PropertyTable],
    resource: &'a HydrationResource,
    reservation: Mutex<MemoryReservation>,
}
impl std::ops::Deref for Invocation<'_> {
    type Target = PathNodeHydration;
    fn deref(&self) -> &Self::Target {
        self.descriptor
    }
}
impl Invocation<'_> {
    fn reserve(&self, bytes: usize) -> Result<()> {
        self.resource.check()?;
        self.reservation
            .lock()
            .expect("hydration reservation poisoned")
            .try_grow(bytes)
    }
}

/// Build the `labels` + property-union children for hydrated path-node
/// elements (#1024 / #706 / #807), one entry per flattened node uuid.
///
/// Demand-first: unique requested UUIDs are gathered once from batchwise
/// topology/property reads (no full-stem `concat_batches`), then expanded back
/// to flattened public positions so repeats keep identical values. Property
/// rows for the same UUID are retained across stems and coalesced on expand.
fn hydrate_path_node_children(
    h: &Invocation<'_>,
    flat: &[[u8; 16]],
    batch_size: usize,
) -> datafusion::error::Result<Vec<datafusion::arrow::array::ArrayRef>> {
    // Reserve bucket capacity (including spare buckets and control bytes), not
    // merely occupied entries. One scratch set is live at a time during gather.
    h.reserve(hash_table_bytes::<(
        [u8; 16],
        Vec<graphforge_value::EntityTypeId>,
    )>(flat.len())?)?;
    h.reserve(hash_table_bytes::<([u8; 16], Vec<PropRowLoc>)>(flat.len())?)?;
    h.reserve(hash_table_bytes::<[u8; 16]>(flat.len())?)?;
    h.reserve(allocation_bytes::<[u8; 16]>(flat.len())?)?;
    let unique = unique_path_uuids(flat);
    h.resource.record("requested", unique.len() as u64);
    let labels_of = gather_path_node_labels(h, &unique, batch_size)?;
    let props_of = gather_path_node_props(h, &unique, batch_size)?;
    h.resource.check()?;
    let mut children = vec![expand_path_node_labels(h, flat, &labels_of)?];
    children.extend(expand_path_node_props(h, flat, &props_of)?);
    Ok(children)
}

fn allocation_bytes<T>(count: usize) -> Result<usize> {
    count.checked_mul(std::mem::size_of::<T>()).ok_or_else(|| {
        DataFusionError::ResourcesExhausted("path hydration allocation size overflow".into())
    })
}

fn hash_table_bytes<T>(entries: usize) -> Result<usize> {
    if entries == 0 {
        return Ok(0);
    }
    // std's hash table reserves at least one empty bucket, with a 7/8 load
    // factor for larger tables. Round up conservatively and include control
    // bytes plus a mirrored SIMD control group.
    let bytes = entries
        .checked_mul(8)
        .and_then(|n| n.checked_add(6))
        .map(|n| (n / 7).max(4))
        .and_then(usize::checked_next_power_of_two)
        .and_then(|buckets| buckets.checked_mul(std::mem::size_of::<T>() + 1))
        .and_then(|bytes| bytes.checked_add(32));
    bytes.ok_or_else(|| {
        DataFusionError::ResourcesExhausted("path hydration allocation size overflow".into())
    })
}

/// Stable-first unique UUIDs from a flattened path-node sequence (#706).
fn unique_path_uuids(flat: &[[u8; 16]]) -> Vec<[u8; 16]> {
    use std::collections::HashSet;
    let mut seen = HashSet::with_capacity(flat.len());
    let mut unique = Vec::with_capacity(flat.len());
    for u in flat {
        if seen.insert(*u) {
            unique.push(*u);
        }
    }
    unique
}

/// A `FixedSizeBinary(16)` column by name, for the hydration readers.
fn hydration_fsb16(
    b: &datafusion::arrow::array::RecordBatch,
    name: &str,
) -> datafusion::error::Result<datafusion::arrow::array::FixedSizeBinaryArray> {
    use datafusion::arrow::array::FixedSizeBinaryArray;
    b.column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>().cloned())
        .filter(|a| a.value_length() == 16)
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "cypher_path_nodes: no FixedSizeBinary(16) {name} column"
            ))
        })
}

/// Gather authoritative `type_ids` for the requested UUID set only (#705 / #706).
fn gather_path_node_labels(
    h: &Invocation<'_>,
    unique: &[[u8; 16]],
    batch_size: usize,
) -> datafusion::error::Result<
    std::collections::HashMap<[u8; 16], Vec<graphforge_value::EntityTypeId>>,
> {
    use datafusion::arrow::array::{Array, ListArray, UInt32Array};
    use datafusion::error::DataFusionError;
    use std::collections::{HashMap, HashSet};

    let exec_err = |m: String| DataFusionError::Execution(m);
    if unique.is_empty() {
        return Ok(HashMap::new());
    }
    let mut remaining: HashSet<[u8; 16]> = unique.iter().copied().collect();
    let mut label_ids_of: HashMap<[u8; 16], Vec<graphforge_value::EntityTypeId>> =
        HashMap::with_capacity(unique.len());

    graphforge_storage::visit_nodes_batched(&h.resource.graph.dir, batch_size, |b| {
        h.resource.check()?;
        h.resource.record("node_batches", 1);
        h.resource.record("node_rows", b.num_rows() as u64);
        if remaining.is_empty() {
            return Ok(false);
        }
        let uuids = hydration_fsb16(b, "node_uuid")?;
        let type_ids = b
            .column_by_name("type_ids")
            .and_then(|c| c.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| exec_err("cypher_path_nodes: no List type_ids column".into()))?;
        for r in 0..b.num_rows() {
            if uuids.is_null(r) || type_ids.is_null(r) {
                continue;
            }
            let mut u = [0u8; 16];
            u.copy_from_slice(uuids.value(r));
            if !remaining.remove(&u) {
                continue;
            }
            let values = type_ids.value(r);
            let values = values
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    exec_err("cypher_path_nodes: type_ids values are not UInt32".into())
                })?;
            h.reserve(
                values
                    .len()
                    .checked_mul(std::mem::size_of::<graphforge_value::EntityTypeId>())
                    .ok_or_else(|| {
                        DataFusionError::ResourcesExhausted(
                            "path hydration label allocation overflow".into(),
                        )
                    })?,
            )?;
            let mut ids = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                if !values.is_null(i) {
                    ids.push(
                        graphforge_value::EntityTypeId::decode(values.value(i)).map_err(
                            |error| {
                                exec_err(format!(
                                    "cypher_path_nodes: invalid membership identity: {error}"
                                ))
                            },
                        )?,
                    );
                }
            }
            label_ids_of.insert(u, ids);
            h.resource.record("node_gathered", 1);
            h.resource
                .record("gathered_entries", label_ids_of.len() as u64);
            if remaining.is_empty() {
                break;
            }
        }
        Ok(!remaining.is_empty())
    })?;
    h.resource.record("resolved", label_ids_of.len() as u64);
    Ok(label_ids_of)
}

/// Expand gathered label ids to a `List<Utf8>` child aligned with `flat` (#705).
fn expand_path_node_labels(
    h: &Invocation<'_>,
    flat: &[[u8; 16]],
    label_ids_of: &std::collections::HashMap<[u8; 16], Vec<graphforge_value::EntityTypeId>>,
) -> Result<datafusion::arrow::array::ArrayRef> {
    use datafusion::arrow::array::{ListBuilder, StringBuilder};

    let mut labels_b = ListBuilder::new(StringBuilder::new());
    for u in flat {
        h.resource.check()?;
        if let Some(ids) = label_ids_of.get(u) {
            for id in ids {
                if let Ok(i) = h
                    .labels_by_type
                    .binary_search_by_key(&id.encode(), |(tid, _)| tid.encode())
                {
                    labels_b.values().append_value(&h.labels_by_type[i].1);
                }
            }
            labels_b.append(true);
        } else {
            labels_b.values().append_null();
            labels_b.append(true);
        }
    }
    Ok(std::sync::Arc::new(labels_b.finish()))
}

/// Location of one gathered property row inside a stem's kept batches.
struct PropRowLoc {
    stem: usize,
    batch: usize,
    row: u32,
}

/// Kept property batches per stem, plus UUID → row locations across stems
/// (#706 / #807). Complementary fields for one UUID may live in more than one
/// stem; expand coalesces them.
type GatheredPathProps = (
    Vec<Vec<datafusion::arrow::array::RecordBatch>>,
    std::collections::HashMap<[u8; 16], Vec<PropRowLoc>>,
);

/// Gather property rows for the requested UUID set only — batchwise, no
/// complete-stem `concat_batches` (#706). Each stem is scanned independently
/// so a UUID found in an earlier stem is still sought in later stems (#807).
fn gather_path_node_props(
    h: &Invocation<'_>,
    unique: &[[u8; 16]],
    batch_size: usize,
) -> datafusion::error::Result<GatheredPathProps> {
    use datafusion::arrow::array::UInt32Array;
    use datafusion::error::DataFusionError;
    use std::collections::{HashMap, HashSet};

    let exec_err = |m: String| DataFusionError::Execution(m);
    let mut uuid_to_loc: HashMap<[u8; 16], Vec<PropRowLoc>> = HashMap::with_capacity(unique.len());
    h.reserve(allocation_bytes::<
        Vec<datafusion::arrow::array::RecordBatch>,
    >(h.prop_stems.len())?)?;
    let mut kept_by_stem: Vec<Vec<datafusion::arrow::array::RecordBatch>> =
        Vec::with_capacity(h.prop_stems.len());

    if unique.is_empty() {
        return Ok((kept_by_stem, uuid_to_loc));
    }

    for si in 0..h.prop_stems.len() {
        h.resource.check()?;
        h.resource.record("stems", 1);
        let mut remaining: HashSet<[u8; 16]> = unique.iter().copied().collect();
        let mut kept: Vec<datafusion::arrow::array::RecordBatch> = Vec::new();
        h.tables[si].visit_authenticated_batches(batch_size, |b| {
            h.resource.check()?;
            h.resource.record("property_batches", 1);
            h.resource.record("property_rows", b.num_rows() as u64);
            if remaining.is_empty() {
                return Ok(false);
            }
            let key = hydration_fsb16(b, "node_uuid")?;
            let mut take_rows: Vec<u32> = Vec::new();
            let mut take_uuids: Vec<[u8; 16]> = Vec::new();
            for r in 0..key.len() {
                if key.is_null(r) {
                    continue;
                }
                let mut u = [0u8; 16];
                u.copy_from_slice(key.value(r));
                if !remaining.contains(&u) {
                    continue;
                }
                take_rows.push(
                    u32::try_from(r)
                        .map_err(|_| exec_err(format!("property row {r} exceeds u32")))?,
                );
                take_uuids.push(u);
            }
            if take_rows.is_empty() {
                return Ok(true);
            }
            let indices = UInt32Array::from(take_rows);
            h.reserve(allocation_bytes::<datafusion::arrow::array::ArrayRef>(
                b.num_columns(),
            )?)?;
            let upper = b.get_array_memory_size();
            h.reserve(upper)?;
            let filtered = take_record_batch_rows(b, &indices)?;
            let retained = filtered.get_array_memory_size();
            if retained < upper {
                h.reservation
                    .lock()
                    .expect("hydration reservation poisoned")
                    .shrink(upper - retained);
            } else if retained > upper {
                h.reserve(retained - upper)?;
            }
            let batch_idx = kept.len();
            for (local_row, u) in take_uuids.into_iter().enumerate() {
                if remaining.remove(&u) {
                    let row = u32::try_from(local_row).map_err(|_| {
                        exec_err(format!("gathered property row {local_row} exceeds u32"))
                    })?;
                    let locations = uuid_to_loc.entry(u).or_default();
                    if locations.len() == locations.capacity() {
                        h.reserve(std::mem::size_of::<PropRowLoc>())?;
                        locations.try_reserve_exact(1).map_err(|error| {
                            DataFusionError::ResourcesExhausted(error.to_string())
                        })?;
                    }
                    locations.push(PropRowLoc {
                        stem: si,
                        batch: batch_idx,
                        row,
                    });
                    h.resource.record("property_gathered", 1);
                }
            }
            h.resource
                .record("gathered_entries", uuid_to_loc.len() as u64);
            if kept.len() == kept.capacity() {
                h.reserve(std::mem::size_of::<datafusion::arrow::array::RecordBatch>())?;
                kept.try_reserve_exact(1)
                    .map_err(|error| DataFusionError::ResourcesExhausted(error.to_string()))?;
            }
            kept.push(filtered);
            Ok(!remaining.is_empty())
        })?;
        kept_by_stem.push(kept);
    }
    Ok((kept_by_stem, uuid_to_loc))
}

/// `take` every column of `batch` at `indices` into a new batch (#706 gather).
fn take_record_batch_rows(
    batch: &datafusion::arrow::array::RecordBatch,
    indices: &datafusion::arrow::array::UInt32Array,
) -> datafusion::error::Result<datafusion::arrow::array::RecordBatch> {
    use datafusion::arrow::compute::take;
    use datafusion::error::DataFusionError;

    let cols: datafusion::error::Result<Vec<_>, _> = batch
        .columns()
        .iter()
        .map(|c| {
            take(c.as_ref(), indices, None).map_err(|e| DataFusionError::Execution(e.to_string()))
        })
        .collect();
    datafusion::arrow::array::RecordBatch::try_new(batch.schema(), cols?)
        .map_err(|e| DataFusionError::Execution(e.to_string()))
}

/// Expand gathered property rows into one nullable union child per field (#1024).
///
/// Each UUID may contribute a row from more than one stem (#807); `zip` keeps
/// the last non-null value in sorted-stem order.
fn expand_path_node_props(
    h: &Invocation<'_>,
    flat: &[[u8; 16]],
    gathered: &GatheredPathProps,
) -> datafusion::error::Result<Vec<datafusion::arrow::array::ArrayRef>> {
    use datafusion::arrow::array::{ArrayRef, UInt32Array, new_null_array};
    use datafusion::arrow::compute::kernels::zip::zip;
    use datafusion::arrow::compute::{is_not_null, take};
    use datafusion::error::DataFusionError;

    let exec_err = |m: String| DataFusionError::Execution(m);
    let (kept_by_stem, uuid_to_loc) = gathered;
    let mut children = Vec::with_capacity(h.fields.len().saturating_sub(2));
    for field in h.fields.iter().skip(2) {
        h.resource.check()?;
        let mut child: ArrayRef = new_null_array(field.data_type(), flat.len());
        for (si, batches) in kept_by_stem.iter().enumerate() {
            for (bi, b) in batches.iter().enumerate() {
                h.resource.check()?;
                let Some(col) = b.column_by_name(field.name()) else {
                    continue;
                };
                let indices = UInt32Array::from(
                    flat.iter()
                        .map(|u| {
                            uuid_to_loc.get(u).and_then(|locs| {
                                locs.iter()
                                    .find(|loc| loc.stem == si && loc.batch == bi)
                                    .map(|loc| loc.row)
                            })
                        })
                        .collect::<Vec<_>>(),
                );
                let taken = take(col, &indices, None).map_err(|e| exec_err(e.to_string()))?;
                let mask = is_not_null(&taken).map_err(|e| exec_err(e.to_string()))?;
                child = zip(&mask, &taken, &child).map_err(|e| exec_err(e.to_string()))?;
            }
        }
        children.push(child);
    }
    Ok(children)
}

/// Transparent ownership/evidence node; it does not change partitioning or reads.
#[derive(Debug)]
pub(crate) struct HydrationExec {
    input: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    resource: Arc<HydrationResource>,
}
impl datafusion::physical_plan::DisplayAs for HydrationExec {
    fn fmt_as(
        &self,
        _: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "PathHydrationExec")
    }
}
impl datafusion::physical_plan::ExecutionPlan for HydrationExec {
    fn name(&self) -> &str {
        "PathHydrationExec"
    }
    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "PathHydrationExec requires one input".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: Arc::clone(&children[0]),
            resource: Arc::clone(&self.resource),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        if !Arc::ptr_eq(context.memory_pool(), &self.resource.pool) {
            return Err(DataFusionError::Execution(
                "path hydration execution memory pool differs from planned query".into(),
            ));
        }
        self.resource.check()?;
        self.input.execute(partition, context)
    }
    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.resource.metrics.clone_inner())
    }
    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<Arc<datafusion::common::Statistics>> {
        self.input.partition_statistics(partition)
    }
}
pub(crate) fn wrap(
    input: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    resource: Option<Arc<HydrationResource>>,
) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
    match resource.filter(|r| r.used()) {
        Some(resource) => Arc::new(HydrationExec { input, resource }),
        None => input,
    }
}
pub(crate) fn cancel_plan(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) {
    if let Some(hydration) = plan.downcast_ref::<HydrationExec>() {
        hydration.resource.cancel();
    }
    for child in plan.children() {
        cancel_plan(child);
    }
}

/// Query-level cancellation: never tied to a single partition's EOF.
pub(crate) struct QueryGuard(pub(crate) Arc<dyn datafusion::physical_plan::ExecutionPlan>);
impl Drop for QueryGuard {
    fn drop(&mut self) {
        cancel_plan(&self.0);
    }
}

#[cfg(test)]
#[path = "path_hydration_tests.rs"]
mod tests;

/// Collect one complete planned query while retaining its cancellation owner.
pub(crate) async fn collect_guarded(
    physical: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    context: Arc<datafusion::execution::TaskContext>,
) -> Result<Vec<arrow::record_batch::RecordBatch>> {
    let _guard = QueryGuard(Arc::clone(&physical));
    #[cfg(test)]
    tests::pause_collect(&physical).await;
    datafusion::physical_plan::collect(physical, context).await
}
