//! Publication-independent canonical encoding for sealed construction shapes.
//!
//! This layer deliberately stops before graph-object installation or `CURRENT`.
//! It converts the shaper's authenticated, UUID-ordered streams into the exact
//! ordinary storage schemas and prepares a streamed UUID-membership manifest.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::path::{Component, Path};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, ListArray, StringArray, TimestampMicrosecondArray,
    UInt32Array, UInt64Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_core::OntologyMode;
use graphforge_filesystem::{StableDirectory, file_identity};
use graphforge_ir::runtime_entity_type_id;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::graph_construction::{ConstructionShape, GraphConstructionBudgets};
use crate::schemas::{TOPOLOGY_NODES_SCHEMA, TYPED_EDGE_SCHEMA, uuid_field};
use crate::uuid_membership::{AuthenticatedUuidIndexSnapshot, ConstructionIndexOutput};

const ENCODED_ROOT: &str = "encoded-v1";
const INVENTORY: &str = "inventory.json";
const MAX_INVENTORY_BYTES: u64 = 16 << 20;
const IDENTITY_WIDTH: usize = 32;
const NODE_DETAIL_WIDTH: usize = 272;
const EDGE_DETAIL_WIDTH: usize = 304;
const RESOLVED_ENDPOINT_WIDTH: usize = 32;
const COPY_BUFFER_BYTES: usize = 1 << 20;

fn storage(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(format!("graph construction encoding: {error}"))
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// One private canonical artifact authenticated by the completed inventory.
pub struct ConstructionEncodedArtifact {
    /// Normalized graph-root-relative path.
    pub path: String,
    /// Exact physical bytes.
    pub bytes: u64,
    /// Lowercase SHA-256 of the file.
    pub sha256: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
/// Measured bounded work for canonical encoding.
pub struct GraphConstructionEncodingEvidence {
    /// Shaped input bytes read sequentially.
    pub input_read_bytes: u64,
    /// Bounded input read operations.
    pub input_read_operations: u64,
    /// Canonical output bytes written.
    pub output_write_bytes: u64,
    /// Canonical output write operations.
    pub output_write_operations: u64,
    /// Completed file and directory durability barriers.
    pub fsync_operations: u64,
    /// Topology rows decoded from the retained parent. Always zero.
    pub prior_topology_rows_decoded: u64,
    /// Retained topology bytes copied. Always zero.
    pub retained_topology_bytes_copied: u64,
    /// Largest decoded Arrow window.
    pub peak_batch_rows: u64,
    /// Largest decoded Arrow window in bytes.
    pub peak_batch_bytes: u64,
    /// Largest number of simultaneously live shard writers. Always one.
    pub peak_open_writers: u64,
    /// New identity records streamed into the v3 index.
    pub membership_records: u64,
    /// New v3 membership bytes written.
    pub membership_write_bytes: u64,
    /// Retained v3 run descriptors structurally reused.
    pub retained_index_runs: u64,
    /// Retained v3 payload bytes read only for required binary-carry compaction.
    pub retained_index_payload_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
/// Completed private canonical artifact inventory.
pub struct GraphConstructionEncoding {
    /// Directory below the private operation root containing this inventory.
    pub root: String,
    /// Generation the eventual publisher must bind.
    pub generation: u64,
    /// Physical routing authority pinned by the construction checkpoint.
    pub ontology_mode: OntologyMode,
    /// Shaper authority digest for normalized catalog inputs.
    pub shape_inputs_sha256: String,
    /// Sorted, unique canonical artifact records.
    pub artifacts: Vec<ConstructionEncodedArtifact>,
    /// Measured bounded work.
    pub evidence: GraphConstructionEncodingEvidence,
}

pub(crate) fn encode(
    source: &StableDirectory,
    shape: &ConstructionShape,
    generation: u64,
    ontology_mode: OntologyMode,
    parent_index: Option<&AuthenticatedUuidIndexSnapshot>,
    budgets: GraphConstructionBudgets,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<GraphConstructionEncoding, GfError> {
    if shape.ontology_mode != ontology_mode {
        return Err(storage(
            "shape ontology mode differs from session authority",
        ));
    }
    if generation == 0 || generation <= shape.parent_topology_generation {
        return Err(storage("encoded generation is not newer than its parent"));
    }
    if cancelled() {
        return Err(storage("construction encoding cancelled"));
    }
    let output = source
        .create_child_directory(OsStr::new(ENCODED_ROOT))
        .map_err(storage)?;
    if let Some(existing) = read_inventory(&output)? {
        authenticate_inventory(&output, &existing)?;
        if existing.generation != generation
            || existing.ontology_mode != shape.ontology_mode
            || existing.shape_inputs_sha256 != shape.runtime_catalog_inputs_sha256
        {
            return Err(storage("completed encoding belongs to another generation"));
        }
        return Ok(existing);
    }

    let label_ids = read_runtime_label_ids(source, &shape.runtime_catalog, budgets)?;
    let mut artifacts = Vec::new();
    let mut evidence = GraphConstructionEncodingEvidence {
        peak_open_writers: 1,
        ..Default::default()
    };

    encode_nodes(
        source,
        &output,
        shape,
        &label_ids,
        ontology_mode,
        budgets,
        cancelled,
        &mut artifacts,
        &mut evidence,
    )?;
    encode_edges(
        source,
        &output,
        shape,
        ontology_mode,
        budgets,
        cancelled,
        &mut artifacts,
        &mut evidence,
    )?;
    copy_artifact(
        source,
        &shape.runtime_catalog,
        &output,
        "topology/runtime_catalog.parquet",
        &mut artifacts,
        &mut evidence,
    )?;
    write_surrogate_tails(
        &output,
        shape.max_node_surrogate,
        shape.max_edge_surrogate,
        &mut artifacts,
        &mut evidence,
    )?;

    let index = crate::uuid_membership::encode_construction_index(
        source,
        &shape.identities,
        &output,
        generation,
        shape.parent_topology_generation,
        parent_index,
        shape.node_count,
        shape.edge_count,
        cancelled,
    )?;
    evidence.membership_records = index.input_records;
    evidence.membership_write_bytes = index.write_bytes;
    evidence.retained_index_runs = index.retained_runs;
    evidence.retained_index_payload_bytes = index.retained_payload_bytes;
    artifacts.extend(index.artifacts.into_iter().map(index_artifact));

    artifacts.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    if artifacts
        .windows(2)
        .any(|pair| pair[0].path == pair[1].path)
    {
        return Err(storage("canonical encoding contains duplicate paths"));
    }
    let completed = GraphConstructionEncoding {
        root: ENCODED_ROOT.to_owned(),
        generation,
        ontology_mode: shape.ontology_mode,
        shape_inputs_sha256: shape.runtime_catalog_inputs_sha256.clone(),
        artifacts,
        evidence,
    };
    install_json(&output, INVENTORY, &completed)?;
    authenticate_inventory(&output, &completed)?;
    Ok(completed)
}

fn index_artifact(value: ConstructionIndexOutput) -> ConstructionEncodedArtifact {
    ConstructionEncodedArtifact {
        path: format!(".graphforge-cache/uuid-membership/{}", value.name),
        bytes: value.bytes,
        sha256: value.sha256,
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn encode_nodes(
    source: &StableDirectory,
    output: &StableDirectory,
    shape: &ConstructionShape,
    label_ids: &BTreeMap<String, u32>,
    ontology_mode: OntologyMode,
    budgets: GraphConstructionBudgets,
    cancelled: &mut impl FnMut() -> bool,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    if shape.node_rows.len() > 1 {
        return Err(storage(
            "canonical node encoding requires one stable schema",
        ));
    }
    let Some(rows_name) = shape.node_rows.first() else {
        return Ok(());
    };
    let details_name = shape
        .node_details
        .as_deref()
        .ok_or_else(|| storage("node rows lack canonical details"))?;
    let mut identities = FixedReader::<IDENTITY_WIDTH>::open(source, &shape.identities, evidence)?;
    let mut details = FixedReader::<NODE_DETAIL_WIDTH>::open(source, details_name, evidence)?;
    let row_file = source
        .open_child_file(OsStr::new(rows_name))
        .map_err(storage)?;
    let mut rows = ParquetRecordBatchReaderBuilder::try_new(row_file)
        .map_err(storage)?
        .with_batch_size(budgets.max_batch_rows)
        .build()
        .map_err(storage)?;
    let mut ordinal = 0_u64;
    for input in &mut rows {
        if cancelled() {
            return Err(storage("construction encoding cancelled"));
        }
        let input = input.map_err(storage)?;
        account_batch(&input, budgets, evidence)?;
        let uuids = required_uuid(&input, "node_uuid")?;
        let labels = required_string(&input, "label")?;
        let mut out_uuid = Vec::with_capacity(input.num_rows());
        let mut out_id = Vec::with_capacity(input.num_rows());
        let mut out_type = Vec::with_capacity(input.num_rows());
        let mut groups = BTreeMap::<String, Vec<u32>>::new();
        for row in 0..input.num_rows() {
            let uuid: [u8; 16] = uuids.value(row).try_into().expect("fixed UUID");
            let identity = next_kind(&mut identities, 0)?
                .ok_or_else(|| storage("node identity stream ended early"))?;
            let detail = details
                .next()?
                .ok_or_else(|| storage("node detail stream ended early"))?;
            if identity[..16] != uuid || detail[..16] != uuid || identity[17] != 0 {
                return Err(storage("node row/detail/identity streams differ"));
            }
            let label_len = usize::from(detail[16]);
            let label = std::str::from_utf8(&detail[17..17 + label_len]).map_err(storage)?;
            if label != labels.value(row) {
                return Err(storage("node detail label differs from normalized row"));
            }
            let type_id = *label_ids
                .get(label)
                .ok_or_else(|| storage("node label is absent from runtime catalog"))?;
            out_uuid.push(uuid);
            out_id.push(u64::from_be_bytes(
                identity[24..32].try_into().expect("fixed"),
            ));
            out_type.push(type_id);
            let route = if ontology_mode == OntologyMode::Exploratory {
                "_untyped"
            } else {
                label
            };
            groups.entry(route.to_owned()).or_default().push(row as u32);
        }
        if out_id.is_empty() {
            continue;
        }
        let canonical = node_batch(
            &out_uuid,
            &out_id,
            &out_type,
            shape.runtime_catalog_now_micros,
        )?;
        let first = *out_id.first().expect("nonempty");
        let last = *out_id.last().expect("nonempty");
        let path = format!("topology/nodes/{first:020}-{last:020}.parquet");
        artifacts.push(write_parquet(output, &path, &canonical, evidence)?);
        for (label, indexes) in groups {
            if input.num_columns() == 2 {
                continue;
            }
            let property = property_batch(
                &input,
                2,
                "node_uuid",
                "graphforge.entity_type",
                &label,
                &indexes,
            )?;
            let path = format!(
                "properties/{label}/{:020}-{ordinal:020}.parquet",
                shape.parent_topology_generation + 1
            );
            artifacts.push(write_parquet(output, &path, &property, evidence)?);
            ordinal = ordinal.saturating_add(1);
        }
    }
    if details.next()?.is_some() || next_kind(&mut identities, 0)?.is_some() {
        return Err(storage("node streams contain unconsumed rows"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn encode_edges(
    source: &StableDirectory,
    output: &StableDirectory,
    shape: &ConstructionShape,
    ontology_mode: OntologyMode,
    budgets: GraphConstructionBudgets,
    cancelled: &mut impl FnMut() -> bool,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    if shape.edge_rows.len() > 1 {
        return Err(storage(
            "canonical edge encoding requires one stable schema",
        ));
    }
    let Some(rows_name) = shape.edge_rows.first() else {
        return Ok(());
    };
    let details_name = shape
        .edge_details
        .as_deref()
        .ok_or_else(|| storage("edge rows lack canonical details"))?;
    let endpoints_name = shape
        .edge_endpoints
        .as_deref()
        .ok_or_else(|| storage("edge rows lack resolved endpoints"))?;
    let mut identities = FixedReader::<IDENTITY_WIDTH>::open(source, &shape.identities, evidence)?;
    let mut details = FixedReader::<EDGE_DETAIL_WIDTH>::open(source, details_name, evidence)?;
    let mut endpoints =
        FixedReader::<RESOLVED_ENDPOINT_WIDTH>::open(source, endpoints_name, evidence)?;
    let row_file = source
        .open_child_file(OsStr::new(rows_name))
        .map_err(storage)?;
    let mut rows = ParquetRecordBatchReaderBuilder::try_new(row_file)
        .map_err(storage)?
        .with_batch_size(budgets.max_batch_rows)
        .build()
        .map_err(storage)?;
    let mut ordinal = 0_u64;
    for input in &mut rows {
        if cancelled() {
            return Err(storage("construction encoding cancelled"));
        }
        let input = input.map_err(storage)?;
        account_batch(&input, budgets, evidence)?;
        let uuids = required_uuid(&input, "edge_uuid")?;
        let srcs = required_uuid(&input, "source_uuid")?;
        let dsts = required_uuid(&input, "target_uuid")?;
        let routes = required_string(&input, "rel_type")?;
        let mut out_uuid = Vec::with_capacity(input.num_rows());
        let mut out_src = Vec::with_capacity(input.num_rows());
        let mut out_dst = Vec::with_capacity(input.num_rows());
        let mut out_id = Vec::with_capacity(input.num_rows());
        let mut out_src_id = Vec::with_capacity(input.num_rows());
        let mut out_dst_id = Vec::with_capacity(input.num_rows());
        let mut groups = BTreeMap::<String, Vec<u32>>::new();
        for row in 0..input.num_rows() {
            let uuid: [u8; 16] = uuids.value(row).try_into().expect("fixed UUID");
            let identity = next_kind(&mut identities, 1)?
                .ok_or_else(|| storage("edge identity stream ended early"))?;
            let detail = details
                .next()?
                .ok_or_else(|| storage("edge detail stream ended early"))?;
            let source_endpoint = endpoints
                .next()?
                .ok_or_else(|| storage("edge endpoint stream ended early"))?;
            let target_endpoint = endpoints
                .next()?
                .ok_or_else(|| storage("edge endpoint stream ended early"))?;
            if identity[..16] != uuid
                || detail[..16] != uuid
                || identity[17] != 0
                || source_endpoint[..16] != uuid
                || target_endpoint[..16] != uuid
                || source_endpoint[16] != 0
                || target_endpoint[16] != 1
            {
                return Err(storage("edge row/detail/identity/endpoint streams differ"));
            }
            let route_len = usize::from(detail[48]);
            let route = std::str::from_utf8(&detail[49..49 + route_len]).map_err(storage)?;
            if route != routes.value(row)
                || detail[16..32] != srcs.value(row)[..]
                || detail[32..48] != dsts.value(row)[..]
            {
                return Err(storage("edge canonical detail differs from normalized row"));
            }
            out_uuid.push(uuid);
            out_src.push(srcs.value(row).try_into().expect("fixed UUID"));
            out_dst.push(dsts.value(row).try_into().expect("fixed UUID"));
            out_id.push(u64::from_be_bytes(
                identity[24..32].try_into().expect("fixed"),
            ));
            out_src_id.push(u64::from_be_bytes(
                source_endpoint[24..32].try_into().expect("fixed"),
            ));
            out_dst_id.push(u64::from_be_bytes(
                target_endpoint[24..32].try_into().expect("fixed"),
            ));
            groups.entry(route.to_owned()).or_default().push(row as u32);
        }
        if out_id.is_empty() {
            continue;
        }
        let canonical = edge_batch(
            &out_uuid,
            &out_src,
            &out_dst,
            &out_id,
            &out_src_id,
            &out_dst_id,
            shape.runtime_catalog_now_micros,
        )?;
        for (route, indexes) in groups {
            let mut selected = select_rows(&canonical, &indexes)?;
            let topology_route = if ontology_mode == OntologyMode::Exploratory {
                let routes = StringArray::from(vec![route.as_str(); selected.num_rows()]);
                let mut fields = selected
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone())
                    .collect::<Vec<_>>();
                fields.push(Field::new("rel_type_name", DataType::Utf8, false));
                let mut columns = selected.columns().to_vec();
                columns.push(Arc::new(routes));
                selected =
                    RecordBatch::try_new(crate::schemas::EXPLORATORY_EDGE_SCHEMA.clone(), columns)
                        .map_err(storage)?;
                "_exploratory"
            } else {
                route.as_str()
            };
            let ids = selected
                .column_by_name("edge_id")
                .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| storage("canonical edge ids are incompatible"))?;
            let first = ids.value(0);
            let last = ids.value(ids.len() - 1);
            let path = format!("topology/edges/{topology_route}/{first:020}-{last:020}.parquet");
            artifacts.push(write_parquet(output, &path, &selected, evidence)?);
            if input.num_columns() > 4 {
                let property = property_batch(
                    &input,
                    4,
                    "edge_uuid",
                    "graphforge.rel_type",
                    &route,
                    &indexes,
                )?;
                let path = format!(
                    "edge_properties/{route}/{:020}-{ordinal:020}.parquet",
                    shape.parent_topology_generation + 1
                );
                artifacts.push(write_parquet(output, &path, &property, evidence)?);
                ordinal = ordinal.saturating_add(1);
            }
        }
    }
    if details.next()?.is_some()
        || endpoints.next()?.is_some()
        || next_kind(&mut identities, 1)?.is_some()
    {
        return Err(storage("edge streams contain unconsumed rows"));
    }
    Ok(())
}

fn next_kind(
    identities: &mut FixedReader<IDENTITY_WIDTH>,
    kind: u8,
) -> Result<Option<[u8; IDENTITY_WIDTH]>, GfError> {
    while let Some(record) = identities.next()? {
        if record[16] == kind {
            return Ok(Some(record));
        }
        if !matches!(record[16], 0 | 1) {
            return Err(storage("identity stream contains invalid kind"));
        }
    }
    Ok(None)
}

fn node_batch(
    uuids: &[[u8; 16]],
    ids: &[u64],
    types: &[u32],
    now: i64,
) -> Result<RecordBatch, GfError> {
    let nullable = ListArray::from_iter_primitive::<arrow::datatypes::UInt32Type, _, _>(
        types.iter().map(|value| Some([Some(*value)])),
    );
    let labels = ListArray::new(
        Arc::new(Field::new("item", DataType::UInt32, false)),
        nullable.offsets().clone(),
        nullable.values().clone(),
        None,
    );
    RecordBatch::try_new(
        TOPOLOGY_NODES_SCHEMA.clone(),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(uuids.iter().copied()).map_err(storage)?),
            Arc::new(UInt64Array::from(ids.to_vec())),
            Arc::new(UInt32Array::from(types.to_vec())),
            Arc::new(labels),
            Arc::new(TimestampMicrosecondArray::from(vec![now; ids.len()]).with_timezone("UTC")),
            Arc::new(TimestampMicrosecondArray::from(vec![now; ids.len()]).with_timezone("UTC")),
        ],
    )
    .map_err(storage)
}

fn edge_batch(
    uuids: &[[u8; 16]],
    srcs: &[[u8; 16]],
    dsts: &[[u8; 16]],
    ids: &[u64],
    src_ids: &[u64],
    dst_ids: &[u64],
    now: i64,
) -> Result<RecordBatch, GfError> {
    RecordBatch::try_new(
        TYPED_EDGE_SCHEMA.clone(),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(uuids.iter().copied()).map_err(storage)?),
            Arc::new(FixedSizeBinaryArray::try_from_iter(srcs.iter().copied()).map_err(storage)?),
            Arc::new(FixedSizeBinaryArray::try_from_iter(dsts.iter().copied()).map_err(storage)?),
            Arc::new(UInt64Array::from(ids.to_vec())),
            Arc::new(UInt64Array::from(src_ids.to_vec())),
            Arc::new(UInt64Array::from(dst_ids.to_vec())),
            Arc::new(TimestampMicrosecondArray::from(vec![now; ids.len()]).with_timezone("UTC")),
        ],
    )
    .map_err(storage)
}

fn property_batch(
    input: &RecordBatch,
    required: usize,
    uuid_name: &str,
    metadata_key: &str,
    owner: &str,
    indexes: &[u32],
) -> Result<RecordBatch, GfError> {
    let indexes = UInt32Array::from(indexes.to_vec());
    let mut fields = vec![uuid_field(uuid_name)];
    fields.extend(
        input.schema().fields()[required..]
            .iter()
            .map(|field| field.as_ref().clone()),
    );
    let schema = Schema::new(fields).with_metadata(
        [(metadata_key.to_owned(), owner.to_owned())]
            .into_iter()
            .collect(),
    );
    let columns = std::iter::once(input.column(0))
        .chain(input.columns()[required..].iter())
        .map(|array| take(array.as_ref(), &indexes, None).map_err(storage))
        .collect::<Result<Vec<ArrayRef>, _>>()?;
    RecordBatch::try_new(Arc::new(schema), columns).map_err(storage)
}

fn select_rows(batch: &RecordBatch, indexes: &[u32]) -> Result<RecordBatch, GfError> {
    let indexes = UInt32Array::from(indexes.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|array| take(array.as_ref(), &indexes, None).map_err(storage))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(storage)
}

fn required_uuid<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| storage(format!("{name} is not FixedSizeBinary(16)")))
}

fn required_string<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, GfError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| storage(format!("{name} is not Utf8")))
}

fn account_batch(
    batch: &RecordBatch,
    budgets: GraphConstructionBudgets,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let bytes = batch.get_array_memory_size();
    if batch.num_rows() > budgets.max_batch_rows || bytes > budgets.max_batch_bytes {
        return Err(storage("decoded canonical batch exceeds encoding budget"));
    }
    evidence.peak_batch_rows = evidence.peak_batch_rows.max(batch.num_rows() as u64);
    evidence.peak_batch_bytes = evidence.peak_batch_bytes.max(bytes as u64);
    Ok(())
}

fn read_runtime_label_ids(
    source: &StableDirectory,
    name: &str,
    budgets: GraphConstructionBudgets,
) -> Result<BTreeMap<String, u32>, GfError> {
    let file = source.open_child_file(OsStr::new(name)).map_err(storage)?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(storage)?
        .with_batch_size(budgets.max_batch_rows)
        .build()
        .map_err(storage)?;
    let mut labels = BTreeMap::new();
    for batch in &mut reader {
        let batch = batch.map_err(storage)?;
        let kinds = required_string(&batch, "entry_kind")?;
        let names = required_string(&batch, "name")?;
        let ids = batch
            .column_by_name("runtime_id")
            .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| storage("runtime catalog id is not UInt32"))?;
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "entity_type" {
                let tagged = runtime_entity_type_id(graphforge_ir::RuntimeTypeId(ids.value(row))).0;
                if labels.insert(names.value(row).to_owned(), tagged).is_some() {
                    return Err(storage("runtime catalog repeats an entity type"));
                }
            }
        }
    }
    Ok(labels)
}

fn write_surrogate_tails(
    output: &StableDirectory,
    max_node_id: u64,
    max_edge_id: u64,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("max_node_id", DataType::UInt64, false),
        Field::new("max_edge_id", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![max_node_id])),
            Arc::new(UInt64Array::from(vec![max_edge_id])),
        ],
    )
    .map_err(storage)?;
    artifacts.push(write_parquet(
        output,
        "topology/surrogate_tails.parquet",
        &batch,
        evidence,
    )?);
    Ok(())
}

fn write_parquet(
    root: &StableDirectory,
    relative: &str,
    batch: &RecordBatch,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<ConstructionEncodedArtifact, GfError> {
    let (directory, name) = directory_for(root, relative)?;
    let temporary = format!(".{}-{}.tmp", name, Uuid::new_v4().simple());
    let file = directory
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).map_err(storage)?;
    writer.write(batch).map_err(storage)?;
    writer.close().map_err(storage)?;
    let mut file = directory
        .open_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    file.sync_all().map_err(storage)?;
    let artifact = authenticate_file(relative, &mut file)?;
    directory
        .replace_child(OsStr::new(&temporary), identity, OsStr::new(&name))
        .map_err(storage)?;
    directory.sync().map_err(storage)?;
    evidence.output_write_bytes = evidence.output_write_bytes.saturating_add(artifact.bytes);
    evidence.output_write_operations = evidence.output_write_operations.saturating_add(1);
    evidence.fsync_operations = evidence.fsync_operations.saturating_add(2);
    Ok(artifact)
}

fn copy_artifact(
    source: &StableDirectory,
    source_name: &str,
    output: &StableDirectory,
    relative: &str,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let mut input = BufReader::with_capacity(
        COPY_BUFFER_BYTES,
        source
            .open_child_file(OsStr::new(source_name))
            .map_err(storage)?,
    );
    let (directory, name) = directory_for(output, relative)?;
    let temporary = format!(".{}-{}.tmp", name, Uuid::new_v4().simple());
    let file = directory
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut writer = BufWriter::with_capacity(COPY_BUFFER_BYTES, file);
    let bytes = std::io::copy(&mut input, &mut writer).map_err(storage)?;
    writer.flush().map_err(storage)?;
    writer.get_ref().sync_all().map_err(storage)?;
    drop(writer);
    let mut file = directory
        .open_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let artifact = authenticate_file(relative, &mut file)?;
    if artifact.bytes != bytes {
        return Err(storage("copied canonical artifact length changed"));
    }
    directory
        .replace_child(OsStr::new(&temporary), identity, OsStr::new(&name))
        .map_err(storage)?;
    directory.sync().map_err(storage)?;
    evidence.input_read_bytes = evidence.input_read_bytes.saturating_add(bytes);
    evidence.input_read_operations = evidence
        .input_read_operations
        .saturating_add(bytes.div_ceil(COPY_BUFFER_BYTES as u64));
    evidence.output_write_bytes = evidence.output_write_bytes.saturating_add(bytes);
    evidence.output_write_operations = evidence
        .output_write_operations
        .saturating_add(bytes.div_ceil(COPY_BUFFER_BYTES as u64));
    evidence.fsync_operations = evidence.fsync_operations.saturating_add(2);
    artifacts.push(artifact);
    Ok(())
}

fn directory_for(
    root: &StableDirectory,
    relative: &str,
) -> Result<(StableDirectory, String), GfError> {
    let path = Path::new(relative);
    if path.is_absolute() {
        return Err(storage("canonical artifact path is absolute"));
    }
    let mut components = path.components().collect::<Vec<_>>();
    let name = match components.pop() {
        Some(Component::Normal(name)) => name
            .to_str()
            .ok_or_else(|| storage("canonical artifact name is not UTF-8"))?
            .to_owned(),
        _ => return Err(storage("canonical artifact path has no file name")),
    };
    let mut directory = root
        .create_child_directory(OsStr::new("graph"))
        .map_err(storage)?;
    for component in components {
        let Component::Normal(name) = component else {
            return Err(storage("canonical artifact path is not normalized"));
        };
        directory = directory.create_child_directory(name).map_err(storage)?;
    }
    Ok((directory, name))
}

fn authenticate_file(path: &str, file: &mut File) -> Result<ConstructionEncodedArtifact, GfError> {
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; COPY_BUFFER_BYTES];
    file.rewind().map_err(storage)?;
    loop {
        let read = file.read(&mut buffer).map_err(storage)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        bytes = bytes.saturating_add(read as u64);
    }
    Ok(ConstructionEncodedArtifact {
        path: path.to_owned(),
        bytes,
        sha256: hex(&digest.finalize()),
    })
}

fn read_inventory(root: &StableDirectory) -> Result<Option<GraphConstructionEncoding>, GfError> {
    let mut file = match root.open_child_file(OsStr::new(INVENTORY)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage(error)),
    };
    if file.metadata().map_err(storage)?.len() > MAX_INVENTORY_BYTES {
        return Err(storage("canonical inventory exceeds bound"));
    }
    serde_json::from_reader(&mut file)
        .map(Some)
        .map_err(storage)
}

fn authenticate_inventory(
    root: &StableDirectory,
    inventory: &GraphConstructionEncoding,
) -> Result<(), GfError> {
    if inventory.root != ENCODED_ROOT
        || inventory.shape_inputs_sha256.len() != 64
        || !inventory
            .shape_inputs_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || inventory
            .artifacts
            .windows(2)
            .any(|pair| pair[0].path >= pair[1].path)
        || inventory.evidence.prior_topology_rows_decoded != 0
        || inventory.evidence.retained_topology_bytes_copied != 0
    {
        return Err(storage("canonical inventory invariants are invalid"));
    }
    for expected in &inventory.artifacts {
        let (directory, name) = directory_for(root, &expected.path)?;
        let mut file = directory
            .open_child_file(OsStr::new(&name))
            .map_err(storage)?;
        let actual = authenticate_file(&expected.path, &mut file)?;
        if &actual != expected {
            return Err(storage("canonical artifact differs from inventory"));
        }
    }
    Ok(())
}

fn install_json<T: Serialize>(
    root: &StableDirectory,
    name: &str,
    value: &T,
) -> Result<(), GfError> {
    let temporary = format!(".{name}-{}.tmp", Uuid::new_v4().simple());
    let mut file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    serde_json::to_writer(&mut file, value).map_err(storage)?;
    file.flush().map_err(storage)?;
    file.sync_all().map_err(storage)?;
    drop(file);
    root.replace_child(OsStr::new(&temporary), identity, OsStr::new(name))
        .map_err(storage)?;
    root.sync().map_err(storage)
}

struct FixedReader<const N: usize> {
    reader: BufReader<File>,
}

impl<const N: usize> FixedReader<N> {
    fn open(
        root: &StableDirectory,
        name: &str,
        evidence: &mut GraphConstructionEncodingEvidence,
    ) -> Result<Self, GfError> {
        let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
        if file.metadata().map_err(storage)?.len() % N as u64 != 0 {
            return Err(storage("fixed-width construction stream is truncated"));
        }
        let bytes = file.metadata().map_err(storage)?.len();
        evidence.input_read_bytes = evidence.input_read_bytes.saturating_add(bytes);
        evidence.input_read_operations = evidence
            .input_read_operations
            .saturating_add(bytes.div_ceil(COPY_BUFFER_BYTES as u64));
        Ok(Self {
            reader: BufReader::with_capacity(COPY_BUFFER_BYTES, file),
        })
    }

    fn next(&mut self) -> Result<Option<[u8; N]>, GfError> {
        let mut record = [0_u8; N];
        let mut read = 0;
        while read < N {
            let amount = self.reader.read(&mut record[read..]).map_err(storage)?;
            if amount == 0 {
                if read == 0 {
                    return Ok(None);
                }
                return Err(storage("fixed-width construction stream is truncated"));
            }
            read += amount;
        }
        Ok(Some(record))
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}
