//! Streaming GraphForge Parquet [`ExecutionPlan`] (#339).
//!
//! Query-facing table providers build this plan during `TableProvider::scan`
//! without reading or concatenating Parquet payloads. I/O happens in
//! [`ExecutionPlan::execute`], which emits bounded batches sized from the
//! session/`TaskContext` batch size and honors the optional I/O concurrency
//! budget registered by #337.

use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, SchedulingType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use futures::stream::{self, StreamExt, TryStreamExt};
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::catalog::{normalize_topology_nodes, tag_rel_type_name};

/// One deterministic natural partition: a single Parquet path (file fragment).
#[derive(Clone, Debug)]
pub struct ParquetFragment {
    /// Absolute or project-relative path to the Parquet file.
    pub path: PathBuf,
    /// When `false`, execute yields an empty batch without opening the file.
    pub exists: bool,
    /// Optional relation stem used by [`UnionEdgeTable`](crate::catalog::UnionEdgeTable)
    /// to tag typed edge rows with `rel_type_name`.
    pub rel_type_name: Option<String>,
    /// Apply [`normalize_topology_nodes`] after decode (legacy `type_id` files).
    pub normalize_topology: bool,
    /// Footer-only row count when known (no row-group decode).
    pub exact_rows: Option<usize>,
}

impl ParquetFragment {
    /// Fragment for a single known path; `exists` is probed without reading bytes.
    ///
    /// When the file exists, the Parquet footer is opened for an exact row count
    /// so DataFusion can keep CollectLeft joins (MemTable parity). Row groups are
    /// not decoded.
    #[must_use]
    pub fn for_path(path: PathBuf, normalize_topology: bool) -> Self {
        let exists = path.exists();
        let exact_rows = if exists {
            footer_num_rows(&path)
        } else {
            Some(0)
        };
        Self {
            path,
            exists,
            rel_type_name: None,
            normalize_topology,
            exact_rows,
        }
    }

    /// Union-edge fragment tagged with `rel_type_name` (file already listed).
    #[must_use]
    pub fn for_union_edge(path: PathBuf, rel_type_name: String) -> Self {
        let exact_rows = footer_num_rows(&path);
        Self {
            path,
            exists: true,
            rel_type_name: Some(rel_type_name),
            normalize_topology: false,
            exact_rows,
        }
    }
}

fn footer_num_rows(path: &Path) -> Option<usize> {
    let file = File::open(path).ok()?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
    usize::try_from(builder.metadata().file_metadata().num_rows()).ok()
}

/// Session extension carrying the #337 I/O concurrency semaphore.
#[derive(Clone, Debug)]
pub struct IoConcurrencyExt(pub Arc<tokio::sync::Semaphore>);

impl IoConcurrencyExt {
    /// Build a semaphore with at least one permit.
    #[must_use]
    pub fn new(permits: usize) -> Self {
        Self(Arc::new(tokio::sync::Semaphore::new(permits.max(1))))
    }
}

/// Streaming Parquet scan over deterministic file fragments.
#[derive(Clone)]
pub struct GraphForgeParquetExec {
    /// Provider / output schema after projection.
    schema: SchemaRef,
    /// Full table schema before projection (normalize / tag target).
    base_schema: SchemaRef,
    /// Arrow field indices into `base_schema`, when projected.
    projection: Option<Vec<usize>>,
    fragments: Vec<ParquetFragment>,
    /// Remaining row cap across this plan (optional).
    limit: Option<usize>,
    /// Planning-time batch size hint (execute re-reads TaskContext).
    batch_size: usize,
    props: Arc<PlanProperties>,
}

impl fmt::Debug for GraphForgeParquetExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphForgeParquetExec")
            .field("fragments", &self.fragments.len())
            .field("batch_size", &self.batch_size)
            .field("limit", &self.limit)
            .field("projection", &self.projection)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl GraphForgeParquetExec {
    /// Build a streaming plan. Does not decode Parquet row groups.
    ///
    /// Fragments may already carry footer-only row counts for DataFusion
    /// statistics; this constructor itself performs no I/O.
    ///
    /// # Errors
    /// Returns [`DataFusionError`] when the projection indices are invalid.
    pub fn try_new(
        base_schema: SchemaRef,
        fragments: Vec<ParquetFragment>,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
        batch_size: usize,
    ) -> Result<Self, DataFusionError> {
        let projection = projection.cloned();
        let schema = match projection.as_ref() {
            Some(indices) => Arc::new(base_schema.project(indices).map_err(|e| {
                DataFusionError::ArrowError(Box::new(e), Some("parquet projection".into()))
            })?),
            None => Arc::clone(&base_schema),
        };
        let partition_count = fragments.len().max(1);
        let props = Arc::new(
            PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(partition_count),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )
            // Match MemTable / MemorySourceConfig so DF keeps CollectLeft joins
            // and does not insert eager RoundRobinBatch exchanges (#339 / #1269).
            .with_scheduling_type(SchedulingType::Cooperative),
        );
        Ok(Self {
            schema,
            base_schema,
            projection,
            fragments,
            limit,
            batch_size: batch_size.max(1),
            props,
        })
    }

    /// Natural fragment count declared by this plan (at least one).
    #[must_use]
    pub fn fragment_count(&self) -> usize {
        self.fragments.len().max(1)
    }

    /// Planning-time batch size captured from the session.
    #[must_use]
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Wrap multi-fragment plans so consumers observe fragment order.
    #[must_use]
    pub fn into_ordered_plan(self) -> Arc<dyn ExecutionPlan> {
        let inner: Arc<dyn ExecutionPlan> = Arc::new(self);
        if inner.output_partitioning().partition_count() <= 1 {
            return inner;
        }
        Arc::new(OrderedPartitionStreamExec::new(inner))
    }
}

impl DisplayAs for GraphForgeParquetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GraphForgeParquetExec: fragments={}, batch_size={}, limit={}",
            self.fragment_count(),
            self.batch_size,
            self.limit
                .map_or_else(|| "none".to_owned(), |n| n.to_string())
        )
    }
}

impl ExecutionPlan for GraphForgeParquetExec {
    fn name(&self) -> &'static str {
        "GraphForgeParquetExec"
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
                "GraphForgeParquetExec cannot have children".into(),
            ));
        }
        Ok(self)
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let mut cloned = self.clone();
        cloned.limit = match (cloned.limit, limit) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        Some(Arc::new(cloned))
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<Arc<Statistics>, DataFusionError> {
        let column_statistics = self
            .schema
            .fields()
            .iter()
            .map(|_| ColumnStatistics::new_unknown())
            .collect();
        let num_rows = match partition {
            None => {
                if self.fragments.is_empty() {
                    Some(0usize)
                } else {
                    self.fragments
                        .iter()
                        .map(|f| f.exact_rows)
                        .try_fold(0usize, |acc, rows| Some(acc.saturating_add(rows?)))
                }
            }
            Some(idx) => {
                if self.fragments.is_empty() {
                    (idx == 0).then_some(0usize)
                } else {
                    self.fragments.get(idx).and_then(|f| f.exact_rows)
                }
            }
        };
        Ok(Arc::new(Statistics {
            num_rows: match num_rows {
                Some(n) => Precision::Exact(n),
                None => Precision::Absent,
            },
            total_byte_size: Precision::Absent,
            column_statistics,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let fragment = if self.fragments.is_empty() {
            if partition != 0 {
                return Err(DataFusionError::Internal(format!(
                    "GraphForgeParquetExec partition {partition} out of range for empty fragments"
                )));
            }
            None
        } else {
            Some(self.fragments.get(partition).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "GraphForgeParquetExec partition {partition} out of range ({})",
                    self.fragments.len()
                ))
            })?)
        };

        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&schema);
        let base_schema = Arc::clone(&self.base_schema);
        let projection = self.projection.clone();
        let limit = self.limit;
        let batch_size = context.session_config().batch_size().max(1);
        let io = context
            .session_config()
            .get_extension::<IoConcurrencyExt>()
            .map(|ext| Arc::clone(&ext.0));
        let fragment = fragment.cloned();

        let stream = stream::once(async move {
            let _permit = if let Some(sem) = io {
                Some(sem.acquire_owned().await.map_err(|e| {
                    DataFusionError::External(format!("io concurrency closed: {e}").into())
                })?)
            } else {
                None
            };
            let batches = match fragment {
                None => vec![RecordBatch::new_empty(Arc::clone(&schema))],
                Some(frag) if !frag.exists => {
                    vec![RecordBatch::new_empty(Arc::clone(&schema))]
                }
                Some(frag) => {
                    read_fragment_batches(&frag, &base_schema, projection.as_deref(), batch_size)?
                }
            };
            let batches = apply_limit(batches, limit);
            Ok::<_, DataFusionError>(stream::iter(batches.into_iter().map(Ok)))
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}

fn apply_limit(batches: Vec<RecordBatch>, limit: Option<usize>) -> Vec<RecordBatch> {
    let Some(mut remaining) = limit else {
        return batches;
    };
    let mut out = Vec::new();
    for batch in batches {
        if remaining == 0 {
            break;
        }
        if batch.num_rows() > remaining {
            out.push(batch.slice(0, remaining));
            break;
        }
        remaining -= batch.num_rows();
        out.push(batch);
    }
    out
}

fn empty_projected_batch(
    base_schema: &SchemaRef,
    projection: Option<&[usize]>,
) -> Result<RecordBatch, DataFusionError> {
    match projection {
        Some(indices) => Ok(RecordBatch::new_empty(Arc::new(
            base_schema.project(indices).map_err(|e| {
                DataFusionError::ArrowError(Box::new(e), Some("empty projection".into()))
            })?,
        ))),
        None => Ok(RecordBatch::new_empty(Arc::clone(base_schema))),
    }
}

fn read_fragment_batches(
    fragment: &ParquetFragment,
    base_schema: &SchemaRef,
    projection: Option<&[usize]>,
    batch_size: usize,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    // Match `read_parquet_or_empty`: a missing path is an empty schema-shaped
    // batch, never a hard error — including TOCTOU where planning saw the file
    // and execute does not (clear / workspace teardown races).
    let file = match File::open(&fragment.path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(vec![empty_projected_batch(base_schema, projection)?]);
        }
        Err(e) => {
            return Err(DataFusionError::External(
                format!("open parquet {}: {e}", path_label(&fragment.path)).into(),
            ));
        }
    };
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        DataFusionError::External(format!("corrupt or unreadable parquet: {e}").into())
    })?;

    // Projection in the reader is only safe when we do not post-process columns
    // (topology normalize / union rel_type_name tagging need the full row first).
    let post_process = fragment.normalize_topology || fragment.rel_type_name.is_some();
    let builder = if post_process {
        builder.with_batch_size(batch_size)
    } else if let Some(indices) = projection {
        let mask = ProjectionMask::roots(builder.parquet_schema(), indices.iter().copied());
        builder.with_batch_size(batch_size).with_projection(mask)
    } else {
        builder.with_batch_size(batch_size)
    };

    let reader = builder.build().map_err(|e| {
        DataFusionError::External(format!("parquet reader build failed: {e}").into())
    })?;

    let mut out = Vec::new();
    for batch in reader {
        let mut batch = batch
            .map_err(|e| DataFusionError::External(format!("parquet decode failed: {e}").into()))?;
        if fragment.normalize_topology {
            batch = normalize_topology_nodes(vec![batch])?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    DataFusionError::Execution("normalize_topology_nodes returned no batch".into())
                })?;
        }
        if let Some(stem) = fragment.rel_type_name.as_deref() {
            batch = tag_rel_type_name(&batch, stem)?;
        }
        if post_process && let Some(indices) = projection {
            batch = batch.project(indices).map_err(|e| {
                DataFusionError::ArrowError(Box::new(e), Some("post-process projection".into()))
            })?;
        }
        // Ensure output schema matches the plan (field nullability / metadata).
        let batch = align_schema(batch, base_schema, projection)?;
        out.push(batch);
    }
    if out.is_empty() {
        out.push(empty_projected_batch(base_schema, projection)?);
    }
    Ok(out)
}

fn align_schema(
    batch: RecordBatch,
    base_schema: &SchemaRef,
    projection: Option<&[usize]>,
) -> Result<RecordBatch, DataFusionError> {
    let target = match projection {
        Some(indices) => Arc::new(base_schema.project(indices).map_err(|e| {
            DataFusionError::ArrowError(Box::new(e), Some("align projection".into()))
        })?),
        None => Arc::clone(base_schema),
    };
    if batch.schema().as_ref() == target.as_ref() {
        return Ok(batch);
    }
    // Rebuild with the canonical schema when field order/names match.
    if batch.num_columns() != target.fields().len() {
        return Err(DataFusionError::Plan(format!(
            "parquet batch column count {} != schema {}",
            batch.num_columns(),
            target.fields().len()
        )));
    }
    RecordBatch::try_new(target, batch.columns().to_vec())
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), Some("align schema".into())))
}

fn path_label(path: &Path) -> String {
    path.file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("parquet")
        .to_owned()
}

/// Serial merge that reads child partitions in index order (canonical fragment order).
#[derive(Debug)]
struct OrderedPartitionStreamExec {
    input: Arc<dyn ExecutionPlan>,
    props: Arc<PlanProperties>,
}

impl OrderedPartitionStreamExec {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = input.schema();
        let props = Arc::new(
            PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )
            .with_scheduling_type(SchedulingType::Cooperative),
        );
        Self { input, props }
    }
}

impl DisplayAs for OrderedPartitionStreamExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "OrderedPartitionStreamExec: input_partitions={}",
            self.input.output_partitioning().partition_count()
        )
    }
}

impl ExecutionPlan for OrderedPartitionStreamExec {
    fn name(&self) -> &'static str {
        "OrderedPartitionStreamExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = children.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("OrderedPartitionStreamExec needs one child".into())
        })?;
        Ok(Arc::new(Self::new(input)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "OrderedPartitionStreamExec only has partition 0 (got {partition})"
            )));
        }
        let input = Arc::clone(&self.input);
        let schema = input.schema();
        let n = input.output_partitioning().partition_count();
        let stream = stream::iter(0..n)
            .then(move |part| {
                let input = Arc::clone(&input);
                let context = Arc::clone(&context);
                async move { input.execute(part, context) }
            })
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Helper used by catalog providers to build the scan plan from session state.
pub fn scan_fragments(
    base_schema: SchemaRef,
    fragments: Vec<ParquetFragment>,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
    batch_size: usize,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let exec =
        GraphForgeParquetExec::try_new(base_schema, fragments, projection, limit, batch_size)?;
    Ok(exec.into_ordered_plan())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    fn edge_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("edge_id", DataType::UInt64, false),
            Field::new("src_id", DataType::UInt64, false),
            Field::new("dst_id", DataType::UInt64, false),
        ]))
    }

    fn write_edges(path: &Path, n: usize) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let schema = edge_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from((1..=n as u64).collect::<Vec<_>>())),
                Arc::new(UInt64Array::from(vec![1u64; n])),
                Arc::new(UInt64Array::from(vec![2u64; n])),
            ],
        )
        .unwrap();
        let file = File::create(path).unwrap();
        let mut writer =
            ArrowWriter::try_new(file, schema, Some(WriterProperties::builder().build())).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn scan_plan_does_not_require_readable_payload_until_execute() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("edges.parquet");
        write_edges(&path, 8);
        let table_schema = edge_schema();
        let fragments = vec![ParquetFragment::for_path(path.clone(), false)];
        let plan = GraphForgeParquetExec::try_new(table_schema.clone(), fragments, None, None, 4)
            .unwrap()
            .into_ordered_plan();
        assert_eq!(plan.name(), "GraphForgeParquetExec");
        // Corrupt after planning — proves scan did not consume the payload.
        std::fs::write(&path, b"not-a-parquet-file").unwrap();
        let ctx = SessionContext::new();
        let task = ctx.task_ctx();
        let err = collect(plan, task).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("corrupt") || msg.contains("unreadable") || msg.contains("parquet"),
            "structured parquet failure, got: {msg}"
        );
    }

    #[tokio::test]
    async fn execute_emits_bounded_batches_honoring_batch_size() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("edges.parquet");
        write_edges(&path, 10);
        let plan = GraphForgeParquetExec::try_new(
            edge_schema(),
            vec![ParquetFragment::for_path(path, false)],
            None,
            None,
            4,
        )
        .unwrap();
        let config = datafusion::prelude::SessionConfig::new().with_batch_size(4);
        let state = datafusion::execution::SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .build();
        let ctx = SessionContext::new_with_state(state);
        let task = ctx.task_ctx();
        let batches = collect(Arc::new(plan) as Arc<dyn ExecutionPlan>, task)
            .await
            .unwrap();
        assert!(
            batches.len() >= 2,
            "expected multiple bounded batches, got {}",
            batches.len()
        );
        assert!(
            batches.iter().all(|b| b.num_rows() <= 4),
            "batch_size 4 violated: {:?}",
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>()
        );
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 10);
    }

    #[tokio::test]
    async fn projection_returns_selected_columns_only() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("edges.parquet");
        write_edges(&path, 3);
        let projection = vec![0usize, 2]; // edge_id, dst_id
        let plan = GraphForgeParquetExec::try_new(
            edge_schema(),
            vec![ParquetFragment::for_path(path, false)],
            Some(&projection),
            None,
            1024,
        )
        .unwrap();
        assert_eq!(plan.schema().fields().len(), 2);
        assert_eq!(plan.schema().field(0).name(), "edge_id");
        assert_eq!(plan.schema().field(1).name(), "dst_id");
        let ctx = SessionContext::new();
        let batches = collect(Arc::new(plan) as Arc<dyn ExecutionPlan>, ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn missing_fragment_yields_empty_schema_shaped_batch() {
        let path = PathBuf::from("/nonexistent/graphforge-339.parquet");
        let plan = GraphForgeParquetExec::try_new(
            edge_schema(),
            vec![ParquetFragment::for_path(path, false)],
            None,
            None,
            8,
        )
        .unwrap();
        let ctx = SessionContext::new();
        let batches = collect(Arc::new(plan) as Arc<dyn ExecutionPlan>, ctx.task_ctx())
            .await
            .unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 0);
        assert_eq!(batches[0].schema(), edge_schema());
    }

    #[tokio::test]
    async fn stale_exists_flag_missing_file_yields_empty_not_error() {
        // Planning-time `exists: true` must still match `read_parquet_or_empty`
        // when the path is gone by execute (TOCTOU / clear).
        let path = PathBuf::from("/nonexistent/graphforge-339-stale.parquet");
        let fragment = ParquetFragment {
            path,
            exists: true,
            rel_type_name: None,
            normalize_topology: false,
            exact_rows: None,
        };
        let plan = GraphForgeParquetExec::try_new(edge_schema(), vec![fragment], None, None, 8)
            .unwrap();
        let ctx = SessionContext::new();
        let batches = collect(Arc::new(plan) as Arc<dyn ExecutionPlan>, ctx.task_ctx())
            .await
            .expect("missing path must not hard-error");
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 0);
        assert_eq!(batches[0].schema(), edge_schema());
    }

    #[test]
    fn plan_shape_exposes_fragments_and_batch_size() {
        let plan = GraphForgeParquetExec::try_new(
            edge_schema(),
            vec![ParquetFragment::for_path(PathBuf::from("a.parquet"), false)],
            None,
            Some(5),
            128,
        )
        .unwrap();
        assert_eq!(plan.fragment_count(), 1);
        assert_eq!(plan.batch_size(), 128);
        let display = format!(
            "{}",
            datafusion::physical_plan::displayable(&plan).one_line()
        );
        assert!(display.contains("GraphForgeParquetExec"), "{display}");
        assert!(display.contains("batch_size=128"), "{display}");
    }

    #[tokio::test]
    async fn ordered_merge_preserves_fragment_order() {
        let dir = TempDir::new().unwrap();
        let a = dir.path().join("a.parquet");
        let b = dir.path().join("b.parquet");
        write_edges(&a, 2);
        write_edges(&b, 2);
        // Overwrite with distinct edge_ids via raw batches
        let schema = edge_schema();
        for (path, start) in [(&a, 10u64), (&b, 20u64)] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(vec![start, start + 1])),
                    Arc::new(UInt64Array::from(vec![1u64, 1])),
                    Arc::new(UInt64Array::from(vec![2u64, 2])),
                ],
            )
            .unwrap();
            let file = File::create(path).unwrap();
            let mut writer = ArrowWriter::try_new(
                file,
                schema.clone(),
                Some(WriterProperties::builder().build()),
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        let plan = GraphForgeParquetExec::try_new(
            schema,
            vec![
                ParquetFragment::for_path(a, false),
                ParquetFragment::for_path(b, false),
            ],
            None,
            None,
            1024,
        )
        .unwrap()
        .into_ordered_plan();
        assert_eq!(plan.name(), "OrderedPartitionStreamExec");
        let ctx = SessionContext::new();
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        let ids = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![10, 11, 20, 21]);
    }
}
