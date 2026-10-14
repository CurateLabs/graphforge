//! Bounded DataFusion execution for authenticated immutable property overlays.

use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream;

/// The most a limited scan holds back while the rest of its route validates.
const MAX_HELD_BYTES: usize = 64 << 20;

pub(crate) struct PropertyScanOptions<'a> {
    pub(crate) projection: Option<&'a Vec<usize>>,
    pub(crate) limit: Option<usize>,
    pub(crate) batch_size: usize,
    /// Whether planning may admit the route for a footer row bound. A
    /// key-only scan of an unread route reports a manifest estimate instead.
    pub(crate) footer_statistics: bool,
    /// An equality the scan answers from footer statistics. The plan keeps the
    /// filter above the scan, so the scan may return more rows than match.
    pub(crate) equality: Option<crate::property_overlay::PropertyEquality>,
    /// Output partitions the scan exposes. Each reads a disjoint UUID interval
    /// of the route, so a key's base, mutation and tombstone rows meet in one
    /// partition. A scan with a limit or an equality, or without a route
    /// inventory, keeps one partition.
    pub(crate) partitions: usize,
}

/// The fewest route rows worth one output partition: below this the per-partition
/// footer and cursor setup costs more than the parallel decode saves.
const MIN_ROWS_PER_PARTITION: usize = 8_192;

/// How many output partitions a scan of `route` should expose when the session
/// allows `target_partitions`: one per [`MIN_ROWS_PER_PARTITION`] of the route's
/// manifest row estimate, at most the target, and never fewer than one.
///
/// The estimate is the fragments' byte length divided by 16 (see
/// `route_row_estimate`), not an exact row count. Wide payloads make it
/// overestimate, which the target bounds; a partition that ends up with no rows
/// emits none.
pub(crate) fn partitions_for_route(
    target_partitions: usize,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
    is_edge: bool,
    route: &str,
) -> usize {
    let Some(inventory) = inventory else {
        return 1;
    };
    let kind = if is_edge {
        crate::PropertyRouteKind::Edge
    } else {
        crate::PropertyRouteKind::Node
    };
    let by_size = inventory.route_row_estimate(kind, route) / MIN_ROWS_PER_PARTITION;
    target_partitions.min(by_size).max(1)
}

#[derive(Clone)]
pub(crate) struct PropertyOverlayExec {
    project: PathBuf,
    inventory: Option<Arc<crate::AuthenticatedPropertyInventory>>,
    route: String,
    is_edge: bool,
    schema: SchemaRef,
    projection: Option<Vec<String>>,
    limit: Option<usize>,
    batch_size: usize,
    planned_rows: Option<usize>,
    /// Output partitions; partition `i` reads the `i`-th UUID interval of the route.
    partitions: usize,
    /// The route's partition cuts, computed once and shared by every partition
    /// and every copy of this scan (copies differ only in pruning hints, never
    /// in route, inventory or partition count).
    cuts: Arc<SharedCuts>,
    equality: Option<crate::property_overlay::PropertyEquality>,
    /// Whether a current persisted equality index answers `equality`.
    equality_index: bool,
    /// A range on one property column whose row groups the merge skips when
    /// their statistics rule the range out. The plan keeps the filter above the
    /// scan, so the scan may return more rows than satisfy it.
    range: Option<crate::property_overlay::PropertyRange>,
    uuid_filters: Vec<Arc<dyn PhysicalExpr>>,
    uuid_nominations: Vec<Arc<crate::property_join_nomination::UuidBuildKeyNomination>>,
    props: Arc<PlanProperties>,
    #[cfg(any(test, feature = "test-support"))]
    digest_context: graphforge_core::hash_observation::operation::Context,
    metrics: Option<ExecutionPlanMetricsSet>,
    work_counts: Option<WorkCounts>,
    /// Partition streams started, under lifecycle measurement.
    partitions_executed: Option<Count>,
    lifecycle_context: crate::lifecycle_io::CaptureContext,
}

/// Work counters and the decoder-peak gauge a scan reports under lifecycle
/// measurement.
type WorkCounts = ([Count; 3], Gauge);

/// The counters a scan reports under lifecycle measurement, or none outside it.
fn lifecycle_metrics() -> (
    Option<ExecutionPlanMetricsSet>,
    Option<WorkCounts>,
    Option<Count>,
) {
    if !crate::lifecycle_io::is_active() {
        return (None, None, None);
    }
    let metrics = ExecutionPlanMetricsSet::new();
    let work_counts = [
        "property_spill_bytes",
        "property_authentication_bytes",
        "property_physical_rows",
    ]
    .map(|name| MetricBuilder::new(&metrics).counter(name, 0));
    let decoder_peak = MetricBuilder::new(&metrics).gauge("property_decoder_peak_bytes", 0);
    let executed = MetricBuilder::new(&metrics).counter("property_partitions_executed", 0);
    (
        Some(metrics),
        Some((work_counts, decoder_peak)),
        Some(executed),
    )
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
        let projection = projection.map(|_| {
            schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect()
        });
        let kind = if is_edge {
            crate::PropertyRouteKind::Edge
        } else {
            crate::PropertyRouteKind::Node
        };
        // A key-only scan of an unread route must not admit it for a footer
        // bound; the manifest's declared lengths still give the planner an
        // estimate, so a small route is not repartitioned as if unbounded.
        let planned_rows = inventory
            .as_ref()
            .map(|inventory| {
                let rows = if options.footer_statistics {
                    inventory
                        .route_row_upper_bound(kind, &route)
                        .map_err(|error| DataFusionError::External(Box::new(error)))?
                } else {
                    inventory.route_row_estimate(kind, &route)
                };
                Ok::<_, DataFusionError>(options.limit.map_or(rows, |limit| rows.min(limit)))
            })
            .transpose()?;
        // A limit holds rows back until the whole route validates and an
        // equality reads by statistics, both of which want one ordered stream;
        // a scan without a pinned inventory has no footers to cut.
        let partitions = if options.limit.is_some() || options.equality.is_some() {
            1
        } else {
            inventory.as_ref().map_or(1, |_| options.partitions.max(1))
        };
        let props = Arc::new(
            PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )
            // Decode runs on the blocking pool and hands bounded batches to a
            // backpressured channel, so polling never blocks the async worker.
            .with_scheduling_type(SchedulingType::Cooperative),
        );
        let (metrics, work_counts, partitions_executed) = lifecycle_metrics();
        let equality_bound = match (&options.equality, inventory.as_deref(), is_edge) {
            (Some(equality), Some(inventory), false) => {
                crate::property_equality_index::matching_rows_bound(
                    &project, inventory, &route, equality,
                )
                .map_err(|error| DataFusionError::External(Box::new(error)))?
            }
            _ => None,
        };
        // A current index answers the equality, so the scan emits only the
        // objects holding the value: the manifest's bound replaces the
        // route-wide row count the planner would otherwise charge to a lookup.
        // A value no shard can hold stays one row, since zero is the exact
        // statistic that declares the route empty and a bound is no proof.
        let planned_rows = match (planned_rows, equality_bound) {
            (Some(rows), Some(bound)) => {
                Some(rows.min(usize::try_from(bound).unwrap_or(usize::MAX).max(1)))
            }
            (rows, _) => rows,
        };
        let equality_index = equality_bound.is_some();
        Ok(Self {
            project,
            inventory,
            route,
            is_edge,
            schema,
            projection,
            limit: options.limit,
            batch_size: options.batch_size.max(1),
            planned_rows,
            partitions,
            cuts: Arc::default(),
            equality: options.equality,
            equality_index,
            range: None,
            uuid_filters: Vec::new(),
            uuid_nominations: Vec::new(),
            props,
            metrics,
            work_counts,
            partitions_executed,
            #[cfg(any(test, feature = "test-support"))]
            digest_context: graphforge_core::hash_observation::operation::Context::capture(),
            lifecycle_context: crate::lifecycle_io::CaptureContext::current(),
        })
    }

    /// Lets the scan skip row groups whose statistics rule `range` out. A
    /// limited scan validates the whole route before it emits and an equality
    /// scan reads by statistics, so neither takes a range.
    #[must_use]
    pub(crate) fn with_range(
        mut self,
        range: Option<crate::property_overlay::PropertyRange>,
    ) -> Self {
        if self.limit.is_none() && self.equality.is_none() {
            self.range = range;
        }
        self
    }

    fn projects_property_payload(&self) -> bool {
        let key = if self.is_edge {
            "edge_uuid"
        } else {
            "node_uuid"
        };
        self.schema.fields().iter().any(|field| field.name() != key)
    }

    pub(crate) fn uuid_filter_candidates(&self) -> Vec<PropertyUuidFilterCandidate> {
        // A UUID hint must not move filtering ahead of a scan-level LIMIT:
        // `[u, v] LIMIT 1` joined to `{v}` is empty before pruning, but would
        // match if the hint removed `u` before LIMIT.
        if self.limit.is_some() || !self.projects_property_payload() {
            return Vec::new();
        }
        let key = if self.is_edge {
            "edge_uuid"
        } else {
            "node_uuid"
        };
        self.uuid_filters
            .iter()
            .filter_map(|filter| {
                let dynamic = filter.downcast_ref::<DynamicFilterPhysicalExpr>()?;
                let expression_id = dynamic.expression_id()?;
                let original = dynamic.original_children();
                let remapped = dynamic.remapped_children().unwrap_or(original);
                if original.len() != 1 || remapped.len() != 1 {
                    return None;
                }
                let source = remapped[0].downcast_ref::<Column>()?;
                let field = self.schema.fields().get(source.index())?;
                if source.name() != key
                    || field.name() != source.name()
                    || field.data_type() != &arrow::datatypes::DataType::FixedSizeBinary(16)
                {
                    return None;
                }
                Some(PropertyUuidFilterCandidate {
                    expression_id,
                    original_probe_key: Arc::clone(&original[0]),
                })
            })
            .collect()
    }

    /// Returns this scan's canonical UUID column only when reading it with a
    /// join-key nomination cannot change LIMIT or equality semantics.
    pub(crate) fn nomination_uuid_column(&self) -> Option<usize> {
        // Limit this optimization to scans that project property payload. A
        // full identity frontier can exhaust the query pool on a UUID set even
        // when the original partitioned identity join fits, as in graph counts.
        if self.limit.is_some() || self.equality.is_some() || !self.projects_property_payload() {
            return None;
        }
        let key = if self.is_edge {
            "edge_uuid"
        } else {
            "node_uuid"
        };
        let mut matching = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                field.name() == key
                    && field.data_type() == &arrow::datatypes::DataType::FixedSizeBinary(16)
                    && !field.is_nullable()
            })
            .map(|(index, _)| index);
        let index = matching.next()?;
        matching.next().is_none().then_some(index)
    }

    /// A new physical join association must not reuse another producer's scan.
    pub(crate) fn fresh_nomination_uuid_column(&self) -> Option<usize> {
        self.uuid_nominations
            .is_empty()
            .then(|| self.nomination_uuid_column())
            .flatten()
    }

    /// A key-only destination scan is eligible only when the exec-owned
    /// selected-endpoint callback proves that this exact frontier nominates
    /// it. Ordinary nomination keeps the stricter payload and identity rules.
    pub(crate) fn fresh_selected_endpoint_uuid_column(&self) -> Option<usize> {
        if self.limit.is_some() || self.equality.is_some() || !self.uuid_nominations.is_empty() {
            return None;
        }
        let key = if self.is_edge {
            "edge_uuid"
        } else {
            "node_uuid"
        };
        let mut matching = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                field.name() == key
                    && field.data_type() == &arrow::datatypes::DataType::FixedSizeBinary(16)
                    && !field.is_nullable()
            })
            .map(|(index, _)| index);
        let index = matching.next()?;
        matching.next().is_none().then_some(index)
    }

    /// A strict equality scan can be an INNER join's build side. It must not
    /// wait on a nomination that was produced by that same join's old build.
    pub(crate) fn equality_build_uuid_column(&self) -> Option<usize> {
        if !self.uuid_nominations.is_empty() {
            return None;
        }
        self.equality_uuid_column()
    }

    /// Return this strict equality scan's canonical UUID column even when an
    /// earlier optimizer pass has already attached a nomination. This is only
    /// used to verify an existing equality-seed join; it does not authorize a
    /// new build nomination.
    pub(crate) fn equality_uuid_column(&self) -> Option<usize> {
        if self.limit.is_some() || self.equality.is_none() {
            return None;
        }
        let key = if self.is_edge {
            "edge_uuid"
        } else {
            "node_uuid"
        };
        let mut matching = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                field.name() == key
                    && field.data_type() == &arrow::datatypes::DataType::FixedSizeBinary(16)
                    && !field.is_nullable()
            })
            .map(|(index, _)| index);
        let index = matching.next()?;
        matching.next().is_none().then_some(index)
    }

    /// Recognize the retained, strict physical predicate for this scan's hint.
    /// Computed expressions and coercions cannot authorize moving its build.
    pub(crate) fn equality_filter_matches(&self, expression: &dyn PhysicalExpr) -> bool {
        use crate::property_overlay::EqualityValue;
        use datafusion::common::ScalarValue;
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Literal};

        let Some(equality) = &self.equality else {
            return false;
        };
        let Some(binary) = expression.downcast_ref::<BinaryExpr>() else {
            return false;
        };
        if binary.op() != &Operator::Eq {
            return false;
        }
        [
            (binary.left(), binary.right()),
            (binary.right(), binary.left()),
        ]
        .into_iter()
        .any(|(column, literal)| {
            let Some(column) = column.downcast_ref::<Column>() else {
                return false;
            };
            let Some(literal) = literal.downcast_ref::<Literal>() else {
                return false;
            };
            let Some(field) = self.schema.fields().get(column.index()) else {
                return false;
            };
            if column.name() != equality.column
                || field.name() != column.name()
                || field.data_type() != &equality.value.data_type()
            {
                return false;
            }
            match (&equality.value, literal.value()) {
                (EqualityValue::Int(expected), ScalarValue::Int64(Some(actual))) => {
                    expected == actual
                }
                (EqualityValue::Str(expected), ScalarValue::Utf8(Some(actual))) => {
                    expected == actual
                }
                (EqualityValue::Bool(expected), ScalarValue::Boolean(Some(actual))) => {
                    expected == actual
                }
                _ => false,
            }
        })
    }

    /// The route's planned row count: the authenticated footer upper bound
    /// when footer statistics are on, else the manifest's inexact estimate,
    /// capped by any limit. `None` without a route inventory.
    pub(crate) const fn planned_rows(&self) -> Option<usize> {
        self.planned_rows
    }

    pub(crate) fn with_uuid_nomination(
        &self,
        nomination: Arc<crate::property_join_nomination::UuidBuildKeyNomination>,
    ) -> Self {
        let mut replacement = self.clone();
        if !replacement
            .uuid_nominations
            .iter()
            .any(|existing| Arc::ptr_eq(existing, &nomination))
        {
            replacement.uuid_nominations.push(nomination);
        }
        replacement
    }
}

pub(crate) struct PropertyUuidFilterCandidate {
    pub(crate) expression_id: u64,
    pub(crate) original_probe_key: Arc<dyn PhysicalExpr>,
}

impl DisplayAs for PropertyOverlayExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PropertyOverlayExec: route={}", self.route)?;
        if self.partitions > 1 {
            write!(f, ", partitions={}", self.partitions)?;
        }
        if let Some(range) = &self.range {
            write!(f, ", range={}", range.column)?;
        }
        if let Some(equality) = &self.equality {
            write!(f, ", equality={}={:?}", equality.column, equality.value)?;
            if self.equality_index {
                write!(f, ", index=property_equality({})", equality.column)?;
            }
        }
        Ok(())
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
            None => self.planned_rows,
            // The cuts balance rows by footer statistics, not exactly, so a
            // partition's share is an estimate of the route's rows.
            Some(partition) if partition < self.partitions => {
                self.planned_rows.map(|rows| rows.div_ceil(self.partitions))
            }
            Some(_) => None,
        };
        let num_rows = match rows {
            Some(0) => Precision::Exact(0),
            // Physical footer rows are a sound upper bound, but overlays and
            // tombstones can reduce the logical output; a key-only scan's
            // manifest estimate is no bound at all. Zero is exact either way:
            // the route declares no object, or the limit admits no row.
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

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>, DataFusionError> {
        let mut replacement = self.clone();
        if phase == FilterPushdownPhase::Post {
            let key = if self.is_edge {
                "edge_uuid"
            } else {
                "node_uuid"
            };
            for filter in &child_pushdown_result.parent_filters {
                if super::property_scan_filter::is_uuid_dynamic_filter(&filter.filter, key) {
                    replacement.uuid_filters.push(Arc::clone(&filter.filter));
                }
            }
        }
        // These are pruning hints. Keep the join's own predicate authoritative,
        // including filters whose final form cannot nominate an exact UUID set.
        Ok(FilterPushdownPropagation {
            filters: vec![PushedDown::No; child_pushdown_result.parent_filters.len()],
            updated_node: (!replacement.uuid_filters.is_empty())
                .then(|| Arc::new(replacement) as Arc<dyn ExecutionPlan>),
        })
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut reset = (*self).clone();
        reset.uuid_filters.clear();
        reset.uuid_nominations.clear();
        Ok(Arc::new(reset))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.metrics
            .as_ref()
            .map(ExecutionPlanMetricsSet::clone_inner)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one blocking task owns the scan, its limit hold-back and its work counters"
    )]
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition >= self.partitions {
            return Err(DataFusionError::Internal(format!(
                "PropertyOverlayExec has {} partitions, not partition {partition}",
                self.partitions
            )));
        }
        if let Some(executed) = &self.partitions_executed {
            executed.add(1);
        }
        let partitions = self.partitions;
        let cuts = Arc::clone(&self.cuts);
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let project = self.project.clone();
        let inventory = self.inventory.clone();
        let route = self.route.clone();
        let is_edge = self.is_edge;
        let projection = self.projection.clone();
        let mut remaining = self.limit;
        let hold_until_validated = self.limit.is_some();
        let batch_size = self.batch_size;
        let work_counts = self.work_counts.clone();
        let equality = self.equality.clone();
        let range = self.range.clone();
        #[cfg(any(test, feature = "test-support"))]
        let digest_context = self.digest_context.clone();
        let lifecycle_context = self.lifecycle_context.clone();
        let uuid_nominations = self.uuid_nominations.clone();
        let output_schema = Arc::clone(&self.schema);
        tokio::spawn(async move {
            for nomination in &uuid_nominations {
                let ready = tokio::select! {
                    result = nomination.wait(&sender) => result,
                    () = sender.closed() => return,
                };
                match ready {
                    Ok(true) => {}
                    Ok(false) => return,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
            }
            // An abandoned nomination (its frontier outgrew its bound) has no
            // key set by design: the scan reads unpruned for it.
            if uuid_nominations
                .iter()
                .any(|nomination| nomination.ids().is_none() && !nomination.is_abandoned())
            {
                let _ = sender
                    .send(Err(DataFusionError::Internal(
                        "completed UUID nomination has no key set".into(),
                    )))
                    .await;
                return;
            }
            tokio::task::spawn_blocking(move || {
                #[cfg(any(test, feature = "test-support"))]
                let _digest_guard = digest_context.attach();
                let _lifecycle_capture = lifecycle_context.attach();

                let uuid_sets = uuid_nominations
                    .iter()
                    .filter_map(|nomination| nomination.ids())
                    .map(|ids| ids as &dyn crate::uuid_set::UuidMembership)
                    .collect::<Vec<_>>();
                let selected_properties = projection
                    .as_ref()
                    .map(|names| names.iter().cloned().collect());
                let mut held = Vec::new();
                let mut held_bytes = 0_usize;
                let limit = remaining;
                let mut replay_after_validation = false;
                let send = |batch: RecordBatch| {
                    // Batches decode under the route's inventory schema, which
                    // carries storage observation metadata; emit them under
                    // the query-facing schema this stream advertises.
                    let batch = RecordBatch::try_new_with_options(
                        Arc::clone(&output_schema),
                        batch.columns().to_vec(),
                        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                    )
                    .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
                    sender.blocking_send(Ok(batch)).map_err(|_| {
                        DataFusionError::Execution("property scan consumer closed".into())
                    })
                };
                let project_batch = |batch: &RecordBatch| {
                    projection.as_ref().map_or_else(
                        || Ok(batch.clone()),
                        |names| {
                            let indices = names
                                .iter()
                                .map(|name| batch.schema().index_of(name))
                                .collect::<Result<Vec<_>, _>>()
                                .map_err(|error| {
                                    DataFusionError::ArrowError(Box::new(error), None)
                                })?;
                            batch
                                .project(&indices)
                                .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))
                        },
                    )
                };
                let mut total_work = None::<crate::PropertyOverlayMetrics>;
                // Every partition cuts the route the same way from the same
                // pinned footers, so the intervals tile the key space: a key's
                // base, mutation and tombstone rows are read by one partition.
                let interval = match partition_interval_of(
                    &cuts,
                    inventory.as_deref(),
                    is_edge,
                    &route,
                    partitions,
                    partition,
                ) {
                    Ok(interval) => interval,
                    Err(error) => {
                        let _ = sender.blocking_send(Err(error));
                        return;
                    }
                };
                let first_pass = crate::catalog::visit_property_overlay_batched_selected(
                    &project,
                    inventory.as_deref(),
                    &route,
                    is_edge,
                    batch_size,
                    selected_properties.as_ref(),
                    equality.as_ref(),
                    range.as_ref(),
                    &uuid_sets,
                    interval,
                    |batch| {
                        let mut batch = project_batch(batch)?;
                        if let Some(rows) = remaining.as_mut() {
                            if *rows == 0 {
                                return Ok(true);
                            }
                            if batch.num_rows() > *rows {
                                batch = batch.slice(0, *rows);
                            }
                            *rows -= batch.num_rows();
                        }
                        if hold_until_validated {
                            if replay_after_validation {
                                return Ok(true);
                            }
                            let batch_bytes = batch.get_array_memory_size();
                            if held_bytes.saturating_add(batch_bytes) > MAX_HELD_BYTES {
                                // Keep memory bounded and preserve the validation
                                // barrier. A successful first pass is replayed
                                // from this same pinned inventory for emission.
                                held.clear();
                                held_bytes = 0;
                                replay_after_validation = true;
                                return Ok(true);
                            }
                            held_bytes += batch_bytes;
                            held.push(batch);
                            return Ok(true);
                        }
                        send(batch)?;
                        Ok(true)
                    },
                );
                let first_pass = first_pass.map(|work| {
                    if let Some(work) = work {
                        total_work.get_or_insert_default().absorb(&work);
                    }
                });
                // A limit stops its consumer after the first rows, so a failure in
                // the rest of the route would go unobserved. Its rows wait for the
                // whole route to validate: the limit changes emission, not authority.
                // If the held prefix would exceed the cap, the first pass validates
                // without retaining or emitting it; replay only after validation.
                let result = first_pass.and_then(|()| {
                    if replay_after_validation {
                        let mut replay_remaining = limit;
                        crate::catalog::visit_property_overlay_batched_selected(
                            &project,
                            inventory.as_deref(),
                            &route,
                            is_edge,
                            batch_size,
                            selected_properties.as_ref(),
                            equality.as_ref(),
                            range.as_ref(),
                            &uuid_sets,
                            interval,
                            |batch| {
                                let mut batch = project_batch(batch)?;
                                let Some(rows) = replay_remaining.as_mut() else {
                                    send(batch)?;
                                    return Ok(true);
                                };
                                if *rows == 0 {
                                    return Ok(false);
                                }
                                if batch.num_rows() > *rows {
                                    batch = batch.slice(0, *rows);
                                }
                                *rows -= batch.num_rows();
                                send(batch)?;
                                Ok(*rows > 0)
                            },
                        )
                        .map(|work| {
                            if let Some(work) = work {
                                total_work.get_or_insert_default().absorb(&work);
                            }
                        })
                    } else {
                        for batch in held {
                            send(batch)?;
                        }
                        Ok(())
                    }
                });
                let result = result.and_then(|_work| {
                    let (Some((work_counts, decoder_peak)), Some(work)) =
                        (work_counts, total_work.as_ref())
                    else {
                        return Ok(());
                    };
                    // Completed reader work only; these logical counters are not native RSS.
                    let measured = |value| {
                        usize::try_from(value).map_err(|_| {
                            DataFusionError::Execution(
                                "property metric exceeds platform range".into(),
                            )
                        })
                    };
                    for (counter, value) in work_counts.iter().zip([
                        work.spill_bytes,
                        work.authentication_bytes,
                        work.physical_rows,
                    ]) {
                        counter.add(measured(value)?);
                    }
                    decoder_peak.set_max(measured(work.decoder_peak_bytes)?);
                    Ok(())
                });
                if let Err(error) = result {
                    let _ = sender.blocking_send(Err(error));
                }
            });
        });
        let schema = Arc::clone(&self.schema);
        let output = stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
    }
}

/// The cuts that split one route's UUID space into the scan's partitions.
///
/// Every partition needs the same cuts, and deriving them walks every fragment
/// footer, so the first partition to ask computes them while the others wait,
/// and all share the result. A failure is not kept: the next partition retries
/// and reports its own error.
#[derive(Default)]
struct SharedCuts(Mutex<Option<Arc<Vec<[u8; 16]>>>>);

impl SharedCuts {
    fn get_or_compute(
        &self,
        compute: impl FnOnce() -> Result<Vec<[u8; 16]>, crate::GfError>,
    ) -> Result<Arc<Vec<[u8; 16]>>, DataFusionError> {
        let mut slot = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(cuts) = slot.as_ref() {
            return Ok(Arc::clone(cuts));
        }
        let cuts = Arc::new(compute().map_err(|error| DataFusionError::External(Box::new(error)))?);
        *slot = Some(Arc::clone(&cuts));
        Ok(cuts)
    }
}

/// The UUID interval partition `partition` of `partitions` reads, or `None`
/// when the scan has one partition and reads the whole route.
fn partition_interval_of(
    cuts: &SharedCuts,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
    is_edge: bool,
    route: &str,
    partitions: usize,
    partition: usize,
) -> Result<Option<crate::uuid_set::UuidInterval>, DataFusionError> {
    let Some(inventory) = inventory.filter(|_| partitions > 1) else {
        return Ok(None);
    };
    let kind = if is_edge {
        crate::PropertyRouteKind::Edge
    } else {
        crate::PropertyRouteKind::Node
    };
    let cuts = cuts.get_or_compute(|| inventory.route_partition_cuts(kind, route, partitions))?;
    Ok(Some(crate::property_overlay::partition_interval(
        &cuts, partition,
    )))
}

#[cfg(test)]
#[path = "property_scan_tests.rs"]
mod property_scan_tests;

#[cfg(test)]
#[path = "property_scan_partition_tests.rs"]
pub(crate) mod property_scan_partition_tests;
