//! Native pinned query reads and allocation accounting.
use super::{GfError, SliceLimits, SliceRequest, SliceSource, Uuid, checkpoint, invalid, limit};
use crate::{CancellationToken, GraphForge, IrLiteral};
use arrow::array::{Array, FixedSizeBinaryArray, ListArray, StringArray};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

pub(super) struct Budget<'a> {
    pub limits: &'a SliceLimits,
    pub cancellation: Option<&'a CancellationToken>,
    rows: u64,
    bytes: u64,
}
impl<'a> Budget<'a> {
    pub(super) fn new(
        request: &'a SliceRequest,
        cancellation: Option<&'a CancellationToken>,
    ) -> Self {
        Self {
            limits: &request.limits,
            cancellation,
            rows: 0,
            bytes: 0,
        }
    }
    pub(super) fn charge(&mut self, rows: u64, bytes: u64) -> Result<(), GfError> {
        checkpoint(self.cancellation)?;
        self.rows = self.rows.checked_add(rows).ok_or_else(limit)?;
        self.bytes = self.bytes.checked_add(bytes).ok_or_else(limit)?;
        if self.rows > u64::from(self.limits.scanned_rows) || self.bytes > self.limits.working_bytes
        {
            return Err(limit());
        }
        Ok(())
    }
}
pub(super) fn validate(request: &SliceRequest) -> Result<(), GfError> {
    let l = &request.limits;
    if request.request_uuid.is_nil() {
        return Err(invalid("Slice request_uuid must be non-nil"));
    }
    if l.scanned_rows == 0
        || l.scanned_rows > 1_000_000
        || l.selected_objects == 0
        || l.selected_objects > 100_000
        || l.boundary_references > 100_000
        || l.dependencies > 100_000
        || !(1024..=64 * 1024 * 1024).contains(&l.working_bytes)
        || !(1024..=16 * 1024 * 1024).contains(&l.response_bytes)
    {
        return Err(invalid("Slice limits exceed native ceilings or are zero"));
    }
    // Bound control strings/sets before generating queries or fingerprints.
    let bytes = serde_json::to_vec(request).map_err(|_| invalid("invalid Slice contract"))?;
    if bytes.len() > 1024 * 1024 {
        return Err(limit());
    }
    Ok(())
}
pub(super) fn open(
    owner: &GraphForge,
    request: &SliceRequest,
) -> Result<(GraphForge, Uuid), GfError> {
    let (mut view, id) = match request.source {
        SliceSource::Current => {
            let generation = owner.generation_for_read()?;
            let id = generation.generation_uuid();
            let mut view = GraphForge::open_resolved_with_lifecycle_mode(
                generation.container_root().to_path_buf(),
                generation,
                true,
                owner.lifecycle_mode,
            )?;
            view.runtime_catalog = Arc::new(Mutex::new(
                owner
                    .runtime_catalog
                    .lock()
                    .map_err(|_| invalid("runtime catalog is unavailable"))?
                    .clone(),
            ));
            view.tempdir.clone_from(&owner.tempdir);
            view.research_materialization
                .clone_from(&owner.research_materialization);
            (view, id)
        }
        SliceSource::Version { version_uuid } => (
            owner
                .open_research_version(version_uuid)?
                .into_slice_graph()?,
            version_uuid,
        ),
    };
    // The engine memory pool is bounded separately from Slice-owned collections.
    view.resource_policy.memory_budget_bytes = request.limits.working_bytes;
    view.resource_policy.batch_size = 256;
    view.resource_policy.target_partitions = 1;
    view.resource_policy.spill_enabled = false;
    Ok((view, id))
}
pub(super) fn stream(
    view: &GraphForge,
    query: &str,
    params: &HashMap<String, IrLiteral>,
    budget: &mut Budget<'_>,
    mut consume: impl FnMut(&RecordBatch) -> Result<(), GfError>,
) -> Result<(), GfError> {
    checkpoint(budget.cancellation)?;
    let mut stream = view.execute_stream_with_params(query, params).map_err(
        |error| match error {
            GfError::Parse { .. } | GfError::Bind { .. } | GfError::Validation(_) => invalid(
                "Slice selector must be a valid read-only query returning canonical UUID columns",
            ),
            other => other,
        },
    )?;
    loop {
        checkpoint(budget.cancellation)?;
        let next = view.block_on(async {
            // Poll cancellation even while an executor is building its next batch.
            loop {
                tokio::select! {
                    batch = stream.next() => return Ok(batch),
                    () = tokio::time::sleep(std::time::Duration::from_millis(10)) => checkpoint(budget.cancellation)?,
                }
            }
        })?;
        let Some(batch) = next else { break };
        let batch = batch.map_err(GfError::from_execution_error)?;
        // Charge conservatively for decoded rows, strings and ordered-map entries.
        budget.charge(
            batch.num_rows() as u64,
            batch.get_array_memory_size() as u64 + batch.num_rows() as u64 * 256,
        )?;
        consume(&batch)?;
    }
    Ok(())
}
pub(super) fn uuid(batch: &RecordBatch, name: &str, row: usize) -> Result<Uuid, GfError> {
    let column = batch
        .column_by_name(name)
        .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| {
            invalid("Slice query must return canonical node_uuid or edge_uuid identity columns")
        })?;
    if column.is_null(row) {
        return Err(invalid("Slice identity cannot be null"));
    }
    Uuid::from_slice(column.value(row)).map_err(|_| invalid("Slice identity must be a UUID"))
}
#[derive(Clone)]
pub(super) struct Edge {
    pub source: Uuid,
    pub target: Uuid,
    pub label: String,
}
#[derive(Default)]
pub(super) struct Topology {
    pub nodes: BTreeMap<Uuid, Vec<String>>,
    pub edges: BTreeMap<Uuid, Edge>,
}
pub(super) fn topology(view: &GraphForge, budget: &mut Budget<'_>) -> Result<Topology, GfError> {
    let mut topology = Topology::default();
    stream(
        view,
        "MATCH (n) RETURN n.node_uuid AS node_uuid, labels(n) AS labels",
        &HashMap::new(),
        budget,
        |batch| {
            let labels = batch
                .column_by_name("labels")
                .and_then(|a| a.as_any().downcast_ref::<ListArray>())
                .ok_or_else(|| invalid("Slice labels schema mismatch"))?;
            for row in 0..batch.num_rows() {
                let values = labels.value(row);
                let values = values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| invalid("Slice labels schema mismatch"))?;
                let mut labels: Vec<_> = values.iter().flatten().map(str::to_owned).collect();
                labels.sort();
                labels.dedup();
                topology
                    .nodes
                    .insert(uuid(batch, "node_uuid", row)?, labels);
            }
            Ok(())
        },
    )?;
    stream(
        view,
        "MATCH (src)-[r]->(dst) RETURN r.edge_uuid AS edge_uuid, src.node_uuid AS source_uuid, dst.node_uuid AS target_uuid, type(r) AS relationship_type",
        &HashMap::new(),
        budget,
        |batch| {
            let labels = batch
                .column_by_name("relationship_type")
                .and_then(|a| a.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| invalid("Slice relationship schema mismatch"))?;
            for row in 0..batch.num_rows() {
                topology.edges.insert(
                    uuid(batch, "edge_uuid", row)?,
                    Edge {
                        source: uuid(batch, "source_uuid", row)?,
                        target: uuid(batch, "target_uuid", row)?,
                        label: labels.value(row).into(),
                    },
                );
            }
            Ok(())
        },
    )?;
    Ok(topology)
}
