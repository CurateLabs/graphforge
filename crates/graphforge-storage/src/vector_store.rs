//! UUID-keyed primary vector snapshots and deterministic exact cosine search.
//!
//! This module owns the backend-neutral vector data contract for M19.  It does
//! not resolve public selectors or graph labels: callers supply already
//! resolved UUID membership and publish the resulting `vectors.parquet` file
//! through the shared search-publication foundation.

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, FixedSizeListArray,
    FixedSizeListBuilder, Float32Array, Float32Builder, TimestampMicrosecondArray,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{
    PublishedSearchArtifact, SearchArtifactError, SearchArtifactKey, SearchCoordinationLimits,
    SearchPublicationMode, SearchPublicationOutcome, SearchPublicationPlan, SearchSourceSnapshot,
    SearchUpdateBuild, coordinate_search_update,
};

/// File stored inside one immutable vector publication.
pub const VECTOR_DATA_FILE: &str = "vectors.parquet";
/// Persisted Arrow/Parquet backend contract.
pub const VECTOR_BACKEND_VERSION: &str = "arrow-parquet-58";
/// Cosine, validation, and ordering contract.
pub const VECTOR_CONTRACT_VERSION: &str = "graphforge_vector_v1";

/// Named limits for vector persistence and exact search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VectorStoreLimits {
    /// Maximum dimension of one stored or query vector.
    pub dimensions: usize,
    /// Maximum rows decoded from one vector snapshot.
    pub stored_vectors: usize,
    /// Maximum `stored_vectors * dimensions` cells decoded or searched.
    pub vector_cells: usize,
    /// Maximum eligible UUIDs supplied by the current graph-label projection.
    pub eligible_nodes: usize,
    /// Maximum requested result count.
    pub results: usize,
    /// Maximum bytes in one Parquet vector file.
    pub parquet_bytes: u64,
}

impl Default for VectorStoreLimits {
    fn default() -> Self {
        Self {
            dimensions: 4_096,
            stored_vectors: 1_000_000,
            vector_cells: 100_000_000,
            eligible_nodes: 1_000_000,
            results: 10_000,
            parquet_bytes: 4 * 1024 * 1024 * 1024,
        }
    }
}

/// One canonical row in a vector snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct StoredVector {
    /// Stable graph identity; no numeric execution surrogate is persisted.
    pub node_uuid: [u8; 16],
    /// Fixed-dimension finite non-zero vector.
    pub vector: Vec<f32>,
    /// Diagnostic transaction time as Arrow `Timestamp(us, UTC)`.
    pub updated_at_micros: i64,
}

/// Whether an upsert changed primary vector data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VectorUpsertChange {
    /// The UUID already stored the same vector; its timestamp is unchanged.
    Unchanged,
    /// A row was inserted or its vector was atomically replaced.
    Changed,
}

/// One exact cosine result before public Arrow shaping.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VectorSearchHit {
    /// Stable graph identity.
    pub node_uuid: [u8; 16],
    /// Finite cosine similarity in `[-1, 1]`.
    pub score: f64,
}

/// Canonical schema for one fixed vector dimension.
///
/// # Errors
/// Returns a named dimension error for zero, oversized, or Arrow-inexpressible
/// dimensions.
pub fn vector_schema(
    dimension: usize,
    limits: VectorStoreLimits,
) -> Result<SchemaRef, SearchArtifactError> {
    validate_dimension(dimension, limits)?;
    let dimension =
        i32::try_from(dimension).map_err(|_| exhausted("vector_dimensions", limits.dimensions))?;
    Ok(Arc::new(Schema::new(vec![
        Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                dimension,
            ),
            false,
        ),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ])))
}

/// Validate a supplied or persisted vector and return its squared norm.
///
/// # Errors
/// Rejects empty, oversized, non-finite, and zero-norm vectors.
pub fn validate_vector(
    vector: &[f32],
    limits: VectorStoreLimits,
) -> Result<f64, SearchArtifactError> {
    validate_dimension(vector.len(), limits)?;
    let mut norm = 0.0_f64;
    for &value in vector {
        if !value.is_finite() {
            return Err(invalid("vector", "values must be finite Float32"));
        }
        let value = f64::from(value);
        norm = value.mul_add(value, norm);
        if !norm.is_finite() {
            return Err(invalid("vector", "norm exceeds supported range"));
        }
    }
    if norm == 0.0 {
        return Err(invalid("vector", "norm must be non-zero"));
    }
    Ok(norm)
}

/// Insert or replace one UUID row in canonical UUID order.
///
/// Repeating the same UUID/vector is idempotent and does not rewrite the
/// diagnostic timestamp.
///
/// # Errors
/// Rejects invalid vectors, dimension changes, corrupt input ordering, and
/// configured row/cell limits.
pub fn apply_vector_upsert(
    rows: &mut Vec<StoredVector>,
    node_uuid: [u8; 16],
    vector: &[f32],
    updated_at_micros: i64,
    limits: VectorStoreLimits,
) -> Result<VectorUpsertChange, SearchArtifactError> {
    validate_vector(vector, limits)?;
    validate_rows(rows, Some(vector.len()), limits, Path::new("<memory>"))?;
    match rows.binary_search_by_key(&node_uuid, |row| row.node_uuid) {
        Ok(index) if rows[index].vector == vector => Ok(VectorUpsertChange::Unchanged),
        Ok(index) => {
            rows[index].vector.clear();
            rows[index].vector.extend_from_slice(vector);
            rows[index].updated_at_micros = updated_at_micros;
            Ok(VectorUpsertChange::Changed)
        }
        Err(index) => {
            if rows.len() >= limits.stored_vectors {
                return Err(exhausted("stored_vectors", limits.stored_vectors));
            }
            checked_cells(rows.len() + 1, vector.len(), limits)?;
            rows.insert(
                index,
                StoredVector {
                    node_uuid,
                    vector: vector.to_vec(),
                    updated_at_micros,
                },
            );
            Ok(VectorUpsertChange::Changed)
        }
    }
}

/// Atomically upsert one resolved UUID into its label/space vector snapshot.
///
/// The shared per-key publication lock protects the read-modify-write cycle.
/// `is_current_member` is evaluated inside that lock and again if the graph
/// snapshot changes during the first attempt.  An identical UUID/vector reuses
/// the current immutable publication without advancing storage state.
///
/// # Errors
/// Rejects invalid selectors/vectors, missing label membership, dimension
/// changes, corrupt primary data, limits, cancellation, and publication errors.
#[allow(clippy::too_many_arguments)]
pub fn upsert_published_vector<S, M, C>(
    project_dir: &Path,
    label: &str,
    space: &str,
    node_uuid: [u8; 16],
    vector: &[f32],
    updated_at_micros: i64,
    limits: VectorStoreLimits,
    coordination: SearchCoordinationLimits,
    snapshot: S,
    mut is_current_member: M,
    checkpoint: C,
) -> Result<SearchPublicationOutcome, SearchArtifactError>
where
    S: FnMut() -> Result<SearchSourceSnapshot, SearchArtifactError>,
    M: FnMut([u8; 16]) -> Result<bool, SearchArtifactError>,
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    validate_vector(vector, limits)?;
    let key = SearchArtifactKey::vector(label, space)?;
    let dimension = u32::try_from(vector.len())
        .map_err(|_| exhausted("vector_dimensions", limits.dimensions))?;
    let plan = SearchPublicationPlan {
        key: &key,
        backend_version: VECTOR_BACKEND_VERSION,
        contract_version: VECTOR_CONTRACT_VERSION,
        dimension: Some(dimension),
        mode: SearchPublicationMode::Replace,
    };
    coordinate_search_update(
        project_dir,
        plan,
        coordination,
        snapshot,
        |current, build_dir, _, checkpoint| {
            if !is_current_member(node_uuid)? {
                return Err(invalid(
                    "node",
                    "UUID does not exist with the required label",
                ));
            }
            let mut rows = match current {
                Some(artifact) => {
                    validate_vector_manifest(artifact, &key, dimension)?;
                    read_vector_snapshot(&artifact.path, vector.len(), limits, &mut *checkpoint)?
                }
                None => Vec::new(),
            };
            match apply_vector_upsert(&mut rows, node_uuid, vector, updated_at_micros, limits)? {
                VectorUpsertChange::Unchanged => Ok(SearchUpdateBuild::ReuseCurrent),
                VectorUpsertChange::Changed => {
                    write_vector_snapshot(
                        build_dir,
                        &rows,
                        vector.len(),
                        limits,
                        &mut *checkpoint,
                    )?;
                    Ok(SearchUpdateBuild::Publish)
                }
            }
        },
        checkpoint,
    )
}

/// Write one complete canonical vector snapshot into a publication build dir.
///
/// # Errors
/// Returns structured validation, resource, Arrow, Parquet, cancellation, or
/// filesystem errors.  Callers publish the directory only after this succeeds.
pub fn write_vector_snapshot<C>(
    build_dir: &Path,
    rows: &[StoredVector],
    dimension: usize,
    limits: VectorStoreLimits,
    mut checkpoint: C,
) -> Result<PathBuf, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    validate_rows(rows, Some(dimension), limits, build_dir)?;
    let schema = vector_schema(dimension, limits)?;
    let batch = rows_to_batch(rows, schema.clone(), dimension, &mut checkpoint)?;
    let path = build_dir.join(VECTOR_DATA_FILE);
    let file = File::create(&path).map_err(|source| io("create vector snapshot", &path, source))?;
    let mut writer = ArrowWriter::try_new(file, schema, None)
        .map_err(|error| build(format!("create vector Parquet writer: {error}")))?;
    writer
        .write(&batch)
        .map_err(|error| build(format!("write vector Parquet batch: {error}")))?;
    checkpoint()?;
    writer
        .close()
        .map_err(|error| build(format!("close vector Parquet writer: {error}")))?;
    sync_vector_snapshot(&path)?;
    let bytes = std::fs::metadata(&path)
        .map_err(|source| io("inspect vector snapshot", &path, source))?
        .len();
    if bytes > limits.parquet_bytes {
        return Err(SearchArtifactError::ResourceExhausted {
            resource: "vector_parquet_bytes",
            limit: limits.parquet_bytes,
        });
    }
    Ok(path)
}

fn sync_vector_snapshot(path: &Path) -> Result<(), SearchArtifactError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| io("sync vector snapshot", path, source))
}

/// Read and validate one immutable vector snapshot.
///
/// # Errors
/// Primary-data schema, ordering, dimension, duplicate, null, and decode
/// failures are returned as `CorruptPrimaryVectors`; configured resource and
/// cancellation limits remain distinct.
pub fn read_vector_snapshot<C>(
    artifact_dir: &Path,
    dimension: usize,
    limits: VectorStoreLimits,
    mut checkpoint: C,
) -> Result<Vec<StoredVector>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    let path = artifact_dir.join(VECTOR_DATA_FILE);
    let metadata = std::fs::metadata(&path).map_err(|source| corrupt_io(&path, &source))?;
    if metadata.len() > limits.parquet_bytes {
        return Err(SearchArtifactError::ResourceExhausted {
            resource: "vector_parquet_bytes",
            limit: limits.parquet_bytes,
        });
    }
    let expected = vector_schema(dimension, limits)?;
    let file = File::open(&path).map_err(|source| corrupt_io(&path, &source))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| corrupt(&path, format!("open Parquet: {error}")))?
        .build()
        .map_err(|error| corrupt(&path, format!("build Parquet reader: {error}")))?;
    let mut rows = Vec::new();
    for batch in reader {
        checkpoint()?;
        let batch = batch.map_err(|error| corrupt(&path, format!("read Parquet: {error}")))?;
        if batch.schema().as_ref() != expected.as_ref() {
            return Err(corrupt(
                &path,
                format!(
                    "schema mismatch: expected {expected:?}, found {:?}",
                    batch.schema()
                ),
            ));
        }
        if rows.len().saturating_add(batch.num_rows()) > limits.stored_vectors {
            return Err(exhausted("stored_vectors", limits.stored_vectors));
        }
        checked_cells(rows.len() + batch.num_rows(), dimension, limits)?;
        decode_batch(&path, &batch, dimension, &mut rows, &mut checkpoint)?;
    }
    validate_rows(&rows, Some(dimension), limits, &path)?;
    Ok(rows)
}

/// Validate the data file and dimension of a published vector artifact.
///
/// # Errors
/// Returns primary corruption when the manifest omits a dimension or the file
/// violates the persisted contract.
pub fn validate_published_vectors<C>(
    artifact: &PublishedSearchArtifact,
    limits: VectorStoreLimits,
    checkpoint: C,
) -> Result<(), SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let dimension = published_vector_dimension(artifact)?;
    read_vector_snapshot(&artifact.path, dimension, limits, checkpoint).map(|_| ())
}

/// Deterministic exhaustive cosine search over a validated snapshot.
///
/// Only UUIDs in `eligible_nodes` participate, so removed nodes and nodes that
/// lost the required label can never escape as stale vector hits.
///
/// # Errors
/// Rejects invalid queries, dimension mismatches, oversized eligibility/result
/// requests, corrupt stored rows, and cooperative cancellation.
pub fn exact_cosine_search<C>(
    rows: &[StoredVector],
    query: &[f32],
    eligible_nodes: &BTreeSet<[u8; 16]>,
    limit: usize,
    limits: VectorStoreLimits,
    mut checkpoint: C,
) -> Result<Vec<VectorSearchHit>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let query_norm = validate_vector(query, limits)?;
    validate_rows(rows, Some(query.len()), limits, Path::new("<memory>"))?;
    if eligible_nodes.len() > limits.eligible_nodes {
        return Err(exhausted("eligible_nodes", limits.eligible_nodes));
    }
    if limit == 0 {
        return Err(invalid("limit", "must be greater than zero"));
    }
    if limit > limits.results {
        return Err(exhausted("search_results", limits.results));
    }
    checked_cells(rows.len(), query.len(), limits)?;

    let mut hits = Vec::with_capacity(rows.len().min(limit));
    for row in rows {
        checkpoint()?;
        if !eligible_nodes.contains(&row.node_uuid) {
            continue;
        }
        let (dot, stored_norm) = dot_and_norm(query, &row.vector)?;
        let denominator = (query_norm * stored_norm).sqrt();
        let score = (dot / denominator).clamp(-1.0, 1.0);
        if !score.is_finite() {
            return Err(corrupt(
                Path::new("<memory>"),
                "cosine score is not finite".to_owned(),
            ));
        }
        hits.push(VectorSearchHit {
            node_uuid: row.node_uuid,
            score,
        });
    }
    hits.sort_unstable_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.node_uuid.cmp(&right.node_uuid))
    });
    hits.truncate(limit);
    Ok(hits)
}

/// Read one published artifact and run exact cosine search.
///
/// # Errors
/// Combines manifest-dimension, primary-data, validation, limit, and
/// cancellation errors without serving partial output.
pub fn search_published_vectors<C>(
    artifact: &PublishedSearchArtifact,
    query: &[f32],
    eligible_nodes: &BTreeSet<[u8; 16]>,
    limit: usize,
    limits: VectorStoreLimits,
    mut checkpoint: C,
) -> Result<Vec<VectorSearchHit>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let dimension = published_vector_dimension(artifact)?;
    if query.len() != dimension {
        return Err(invalid(
            "vector",
            format!(
                "dimension {} does not match stored dimension {dimension}",
                query.len()
            ),
        ));
    }
    let rows = read_vector_snapshot(&artifact.path, dimension, limits, &mut checkpoint)?;
    exact_cosine_search(&rows, query, eligible_nodes, limit, limits, checkpoint)
}

fn rows_to_batch<C>(
    rows: &[StoredVector],
    schema: SchemaRef,
    dimension: usize,
    checkpoint: &mut C,
) -> Result<RecordBatch, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let mut uuids = FixedSizeBinaryBuilder::with_capacity(rows.len(), 16);
    let values = Float32Builder::with_capacity(rows.len().saturating_mul(dimension));
    let mut vectors = FixedSizeListBuilder::with_capacity(
        values,
        i32::try_from(dimension).map_err(|_| build("vector dimension exceeds Arrow i32"))?,
        rows.len(),
    )
    .with_field(Arc::new(Field::new("item", DataType::Float32, false)));
    let timestamp_type = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let mut updated =
        TimestampMicrosecondBuilder::with_capacity(rows.len()).with_data_type(timestamp_type);
    for row in rows {
        checkpoint()?;
        uuids
            .append_value(row.node_uuid)
            .map_err(|error| build(format!("append node_uuid: {error}")))?;
        for &value in &row.vector {
            vectors.values().append_value(value);
        }
        vectors.append(true);
        updated.append_value(row.updated_at_micros);
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(uuids.finish()) as ArrayRef,
            Arc::new(vectors.finish()) as ArrayRef,
            Arc::new(updated.finish()) as ArrayRef,
        ],
    )
    .map_err(|error| build(format!("construct vector record batch: {error}")))
}

fn validate_vector_manifest(
    artifact: &PublishedSearchArtifact,
    key: &SearchArtifactKey,
    dimension: u32,
) -> Result<(), SearchArtifactError> {
    let manifest = &artifact.manifest;
    if manifest.index_kind != key.kind()
        || manifest.label != key.label()
        || manifest.space.as_deref() != key.space()
        || manifest.properties.is_some()
        || manifest.backend_version != VECTOR_BACKEND_VERSION
        || manifest.contract_version != VECTOR_CONTRACT_VERSION
        || !manifest.completed
    {
        return Err(corrupt(
            &artifact.path,
            "vector manifest does not match the requested backend key",
        ));
    }
    if manifest.dimension != Some(dimension) {
        return Err(invalid(
            "vector",
            format!(
                "dimension {dimension} does not match stored dimension {}",
                manifest.dimension.unwrap_or_default()
            ),
        ));
    }
    Ok(())
}

fn published_vector_dimension(
    artifact: &PublishedSearchArtifact,
) -> Result<usize, SearchArtifactError> {
    let manifest = &artifact.manifest;
    let space = manifest
        .space
        .as_deref()
        .ok_or_else(|| corrupt(&artifact.path, "vector manifest omits its space"))?;
    let key = SearchArtifactKey::vector(&manifest.label, space)
        .map_err(|error| corrupt(&artifact.path, error.to_string()))?;
    let dimension = manifest
        .dimension
        .ok_or_else(|| corrupt(&artifact.path, "vector manifest omits its fixed dimension"))?;
    validate_vector_manifest(artifact, &key, dimension)
        .map_err(|error| corrupt(&artifact.path, error.to_string()))?;
    usize::try_from(dimension)
        .map_err(|_| corrupt(&artifact.path, "vector dimension exceeds usize"))
}

fn decode_batch<C>(
    path: &Path,
    batch: &RecordBatch,
    dimension: usize,
    rows: &mut Vec<StoredVector>,
    checkpoint: &mut C,
) -> Result<(), SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let uuids = downcast::<FixedSizeBinaryArray>(path, batch, 0, "node_uuid")?;
    let vectors = downcast::<FixedSizeListArray>(path, batch, 1, "vector")?;
    let values = vectors
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| corrupt(path, "vector child is not Float32".to_owned()))?;
    let updated = downcast::<TimestampMicrosecondArray>(path, batch, 2, "updated_at")?;
    if uuids.null_count() != 0
        || vectors.null_count() != 0
        || values.null_count() != 0
        || updated.null_count() != 0
    {
        return Err(corrupt(
            path,
            "vector snapshot contains NULL values".to_owned(),
        ));
    }
    for row in 0..batch.num_rows() {
        checkpoint()?;
        let uuid: [u8; 16] = uuids
            .value(row)
            .try_into()
            .map_err(|_| corrupt(path, "node_uuid is not 16 bytes".to_owned()))?;
        let offset = row
            .checked_mul(dimension)
            .ok_or_else(|| corrupt(path, "vector offset overflow".to_owned()))?;
        let end = offset
            .checked_add(dimension)
            .ok_or_else(|| corrupt(path, "vector offset overflow".to_owned()))?;
        if end > values.len() {
            return Err(corrupt(path, "vector child length is truncated".to_owned()));
        }
        rows.push(StoredVector {
            node_uuid: uuid,
            vector: (offset..end).map(|index| values.value(index)).collect(),
            updated_at_micros: updated.value(row),
        });
    }
    Ok(())
}

fn downcast<'a, T: 'static>(
    path: &Path,
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a T, SearchArtifactError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| corrupt(path, format!("column {name:?} has the wrong Arrow type")))
}

fn validate_rows(
    rows: &[StoredVector],
    expected_dimension: Option<usize>,
    limits: VectorStoreLimits,
    path: &Path,
) -> Result<(), SearchArtifactError> {
    if rows.len() > limits.stored_vectors {
        return Err(exhausted("stored_vectors", limits.stored_vectors));
    }
    let dimension = expected_dimension.or_else(|| rows.first().map(|row| row.vector.len()));
    if let Some(dimension) = dimension {
        validate_dimension(dimension, limits)?;
        checked_cells(rows.len(), dimension, limits)?;
        for (index, row) in rows.iter().enumerate() {
            if row.vector.len() != dimension {
                return Err(corrupt(
                    path,
                    format!(
                        "row {index} dimension {} does not match {dimension}",
                        row.vector.len()
                    ),
                ));
            }
            validate_vector(&row.vector, limits)
                .map_err(|error| corrupt(path, error.to_string()))?;
        }
    }
    for pair in rows.windows(2) {
        if pair[0].node_uuid >= pair[1].node_uuid {
            let reason = if pair[0].node_uuid == pair[1].node_uuid {
                "duplicate node_uuid"
            } else {
                "rows are not sorted by ascending node_uuid"
            };
            return Err(corrupt(path, reason.to_owned()));
        }
    }
    Ok(())
}

fn dot_and_norm(left: &[f32], right: &[f32]) -> Result<(f64, f64), SearchArtifactError> {
    let mut dot = 0.0_f64;
    let mut norm = 0.0_f64;
    for (&left, &right) in left.iter().zip(right) {
        let left = f64::from(left);
        let right = f64::from(right);
        dot = left.mul_add(right, dot);
        norm = right.mul_add(right, norm);
        if !dot.is_finite() || !norm.is_finite() {
            return Err(corrupt(
                Path::new("<memory>"),
                "cosine accumulation exceeds supported range".to_owned(),
            ));
        }
    }
    if norm == 0.0 {
        return Err(corrupt(
            Path::new("<memory>"),
            "stored vector has zero norm".to_owned(),
        ));
    }
    Ok((dot, norm))
}

fn validate_dimension(
    dimension: usize,
    limits: VectorStoreLimits,
) -> Result<(), SearchArtifactError> {
    if dimension == 0 {
        return Err(invalid("vector", "dimension must be greater than zero"));
    }
    if dimension > limits.dimensions || i32::try_from(dimension).is_err() {
        return Err(exhausted("vector_dimensions", limits.dimensions));
    }
    Ok(())
}

fn checked_cells(
    rows: usize,
    dimension: usize,
    limits: VectorStoreLimits,
) -> Result<usize, SearchArtifactError> {
    let cells = rows
        .checked_mul(dimension)
        .ok_or_else(|| exhausted("vector_cells", limits.vector_cells))?;
    if cells > limits.vector_cells {
        return Err(exhausted("vector_cells", limits.vector_cells));
    }
    Ok(cells)
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

fn exhausted(resource: &'static str, limit: usize) -> SearchArtifactError {
    SearchArtifactError::ResourceExhausted {
        resource,
        limit: u64::try_from(limit).unwrap_or(u64::MAX),
    }
}

fn build(reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::Build(reason.into())
}

fn corrupt(path: &Path, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::CorruptPrimaryVectors {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

fn corrupt_io(path: &Path, source: &std::io::Error) -> SearchArtifactError {
    corrupt(path, source.to_string())
}

fn io(operation: &'static str, path: &Path, source: std::io::Error) -> SearchArtifactError {
    SearchArtifactError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use tempfile::TempDir;

    use super::*;
    use crate::{SearchManifest, current_search_artifact};

    fn uuid(value: u8) -> [u8; 16] {
        let mut uuid = [0_u8; 16];
        uuid[15] = value;
        uuid
    }

    fn row(value: u8, vector: &[f32], updated_at_micros: i64) -> StoredVector {
        StoredVector {
            node_uuid: uuid(value),
            vector: vector.to_vec(),
            updated_at_micros,
        }
    }

    fn source_snapshot() -> Result<SearchSourceSnapshot, SearchArtifactError> {
        Ok(SearchSourceSnapshot {
            generation: 7,
            fingerprint: format!("gf-fnv1a256:{:064x}", 7),
        })
    }

    #[test]
    fn schema_is_uuid_fixed_vector_and_utc_timestamp() {
        let schema = vector_schema(3, VectorStoreLimits::default()).unwrap();
        assert_eq!(schema.field(0).name(), "node_uuid");
        assert_eq!(schema.field(0).data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            schema.field(1).data_type(),
            &DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3,)
        );
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert!(schema.fields().iter().all(|field| !field.is_nullable()));
    }

    #[test]
    fn upsert_is_sorted_idempotent_and_replaces_atomically() {
        let limits = VectorStoreLimits::default();
        let mut rows = vec![row(2, &[0.0, 1.0], 20)];
        assert_eq!(
            apply_vector_upsert(&mut rows, uuid(1), &[1.0, 0.0], 10, limits).unwrap(),
            VectorUpsertChange::Changed
        );
        assert_eq!(
            rows.iter().map(|row| row.node_uuid).collect::<Vec<_>>(),
            vec![uuid(1), uuid(2)]
        );
        assert_eq!(
            apply_vector_upsert(&mut rows, uuid(1), &[1.0, 0.0], 99, limits).unwrap(),
            VectorUpsertChange::Unchanged
        );
        assert_eq!(rows[0].updated_at_micros, 10);
        assert_eq!(
            apply_vector_upsert(&mut rows, uuid(1), &[-1.0, 0.0], 30, limits).unwrap(),
            VectorUpsertChange::Changed
        );
        assert_eq!(rows[0], row(1, &[-1.0, 0.0], 30));
    }

    #[test]
    fn vector_validation_rejects_dimension_nonfinite_and_zero_norm() {
        let limits = VectorStoreLimits {
            dimensions: 2,
            ..VectorStoreLimits::default()
        };
        for vector in [
            &[][..],
            &[0.0, 0.0],
            &[f32::NAN],
            &[f32::INFINITY],
            &[1.0, 2.0, 3.0],
        ] {
            assert!(validate_vector(vector, limits).is_err(), "{vector:?}");
        }
        assert!((validate_vector(&[3.0, 4.0], limits).unwrap() - 25.0).abs() < f64::EPSILON);
    }

    #[test]
    fn parquet_round_trip_preserves_fixed_dimension_and_order() {
        let dir = TempDir::new().unwrap();
        let rows = vec![row(1, &[1.0, 2.0], 11), row(3, &[-3.0, 4.0], 33)];
        let writes = Cell::new(0);
        let path =
            write_vector_snapshot(dir.path(), &rows, 2, VectorStoreLimits::default(), || {
                writes.set(writes.get() + 1);
                Ok(())
            })
            .unwrap();
        assert_eq!(path, dir.path().join(VECTOR_DATA_FILE));
        assert!(writes.get() >= 3);
        let decoded =
            read_vector_snapshot(dir.path(), 2, VectorStoreLimits::default(), || Ok(())).unwrap();
        assert_eq!(decoded, rows);
    }

    #[test]
    fn vector_snapshot_sync_preserves_file_contents() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(VECTOR_DATA_FILE);
        std::fs::write(&path, b"complete snapshot").unwrap();

        sync_vector_snapshot(&path).unwrap();

        assert_eq!(std::fs::read(path).unwrap(), b"complete snapshot");
    }

    #[test]
    fn write_enforces_the_parquet_byte_limit_before_publication() {
        let dir = TempDir::new().unwrap();
        let limits = VectorStoreLimits {
            parquet_bytes: 1,
            ..VectorStoreLimits::default()
        };
        assert!(matches!(
            write_vector_snapshot(dir.path(), &[row(1, &[1.0], 1)], 1, limits, || Ok(())),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "vector_parquet_bytes",
                limit: 1,
            })
        ));
    }

    #[test]
    fn published_artifact_validation_and_search_use_pinned_contracts() {
        let dir = TempDir::new().unwrap();
        let limits = VectorStoreLimits::default();
        let rows = vec![row(1, &[1.0, 0.0], 11), row(2, &[0.0, 1.0], 22)];
        write_vector_snapshot(dir.path(), &rows, 2, limits, || Ok(())).unwrap();
        let key = SearchArtifactKey::vector("Person", "semantic").unwrap();
        let manifest = SearchManifest::for_key(
            &key,
            VECTOR_BACKEND_VERSION,
            VECTOR_CONTRACT_VERSION,
            Some(2),
            &SearchSourceSnapshot {
                generation: 1,
                fingerprint: format!("gf-fnv1a256:{:064x}", 1),
            },
            true,
        )
        .unwrap();
        let artifact = PublishedSearchArtifact {
            path: dir.path().to_path_buf(),
            manifest,
        };
        validate_published_vectors(&artifact, limits, || Ok(())).unwrap();
        let hits = search_published_vectors(
            &artifact,
            &[1.0, 0.0],
            &BTreeSet::from([uuid(1), uuid(2)]),
            1,
            limits,
            || Ok(()),
        )
        .unwrap();
        assert_eq!(
            hits,
            vec![VectorSearchHit {
                node_uuid: uuid(1),
                score: 1.0,
            }]
        );
    }

    #[test]
    fn published_reads_reject_a_mismatched_backend_contract() {
        let dir = TempDir::new().unwrap();
        let limits = VectorStoreLimits::default();
        write_vector_snapshot(dir.path(), &[row(1, &[1.0], 1)], 1, limits, || Ok(())).unwrap();
        let key = SearchArtifactKey::vector("Person", "semantic").unwrap();
        let artifact = PublishedSearchArtifact {
            path: dir.path().to_path_buf(),
            manifest: SearchManifest::for_key(
                &key,
                "other-backend-1",
                VECTOR_CONTRACT_VERSION,
                Some(1),
                &source_snapshot().unwrap(),
                true,
            )
            .unwrap(),
        };
        assert!(matches!(
            validate_published_vectors(&artifact, limits, || Ok(())),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
    }

    #[test]
    fn atomic_upsert_publishes_reuses_replaces_and_preserves_dimension() {
        let dir = TempDir::new().unwrap();
        let limits = VectorStoreLimits::default();
        let first = upsert_published_vector(
            dir.path(),
            "Person",
            "semantic",
            uuid(1),
            &[1.0, 0.0],
            11,
            limits,
            SearchCoordinationLimits::default(),
            source_snapshot,
            |_| Ok(true),
            || Ok(()),
        )
        .unwrap();
        let first_path = match first {
            SearchPublicationOutcome::Published {
                artifact,
                attempts: 1,
            } => artifact.path,
            other => panic!("unexpected first outcome: {other:?}"),
        };

        let repeated = upsert_published_vector(
            dir.path(),
            "Person",
            "semantic",
            uuid(1),
            &[1.0, 0.0],
            99,
            limits,
            SearchCoordinationLimits::default(),
            source_snapshot,
            |_| Ok(true),
            || Ok(()),
        )
        .unwrap();
        assert!(matches!(
            repeated,
            SearchPublicationOutcome::Reused(ref artifact) if artifact.path == first_path
        ));
        assert_eq!(
            read_vector_snapshot(&first_path, 2, limits, || Ok(())).unwrap()[0].updated_at_micros,
            11
        );

        let replaced = upsert_published_vector(
            dir.path(),
            "Person",
            "semantic",
            uuid(1),
            &[-1.0, 0.0],
            22,
            limits,
            SearchCoordinationLimits::default(),
            source_snapshot,
            |_| Ok(true),
            || Ok(()),
        )
        .unwrap();
        let replaced_path = match replaced {
            SearchPublicationOutcome::Published { artifact, .. } => artifact.path,
            other @ SearchPublicationOutcome::Reused(_) => {
                panic!("unexpected replacement outcome: {other:?}")
            }
        };
        assert_ne!(replaced_path, first_path);
        assert!(first_path.exists());

        let key = SearchArtifactKey::vector("Person", "semantic").unwrap();
        let before_error = current_search_artifact(dir.path(), &key)
            .unwrap()
            .unwrap()
            .path;
        let mismatch = upsert_published_vector(
            dir.path(),
            "Person",
            "semantic",
            uuid(2),
            &[1.0, 0.0, 0.0],
            33,
            limits,
            SearchCoordinationLimits::default(),
            source_snapshot,
            |_| Ok(true),
            || Ok(()),
        );
        assert!(matches!(
            mismatch,
            Err(SearchArtifactError::InvalidSelector {
                field: "vector",
                ..
            })
        ));
        assert_eq!(
            current_search_artifact(dir.path(), &key)
                .unwrap()
                .unwrap()
                .path,
            before_error
        );
    }

    #[test]
    fn atomic_upsert_requires_current_label_membership() {
        let dir = TempDir::new().unwrap();
        let result = upsert_published_vector(
            dir.path(),
            "Person",
            "semantic",
            uuid(1),
            &[1.0],
            1,
            VectorStoreLimits::default(),
            SearchCoordinationLimits::default(),
            || {
                Ok(SearchSourceSnapshot {
                    generation: 1,
                    fingerprint: format!("gf-fnv1a256:{:064x}", 1),
                })
            },
            |_| Ok(false),
            || Ok(()),
        );
        assert!(matches!(
            result,
            Err(SearchArtifactError::InvalidSelector { field: "node", .. })
        ));
        assert!(
            current_search_artifact(
                dir.path(),
                &SearchArtifactKey::vector("Person", "semantic").unwrap()
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn cosine_keeps_negative_scores_filters_orphans_and_breaks_ties_by_uuid() {
        let rows = vec![
            row(1, &[1.0, 0.0], 1),
            row(2, &[1.0, 0.0], 2),
            row(3, &[0.0, 1.0], 3),
            row(4, &[-1.0, 0.0], 4),
        ];
        let eligible = BTreeSet::from([uuid(1), uuid(2), uuid(4)]);
        let hits = exact_cosine_search(
            &rows,
            &[1.0, 0.0],
            &eligible,
            10,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(
            hits,
            vec![
                VectorSearchHit {
                    node_uuid: uuid(1),
                    score: 1.0
                },
                VectorSearchHit {
                    node_uuid: uuid(2),
                    score: 1.0
                },
                VectorSearchHit {
                    node_uuid: uuid(4),
                    score: -1.0
                },
            ]
        );
    }

    #[test]
    fn insertion_order_and_result_limit_do_not_change_ranking() {
        let canonical = vec![
            row(1, &[1.0, 1.0], 1),
            row(2, &[2.0, 2.0], 2),
            row(3, &[0.0, 1.0], 3),
        ];
        let eligible = canonical.iter().map(|row| row.node_uuid).collect();
        let hits = exact_cosine_search(
            &canonical,
            &[1.0, 1.0],
            &eligible,
            2,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(
            hits.iter().map(|hit| hit.node_uuid).collect::<Vec<_>>(),
            vec![uuid(1), uuid(2)]
        );
    }

    #[test]
    fn malformed_primary_data_is_never_repaired_or_silently_sorted() {
        let limits = VectorStoreLimits::default();
        let duplicate = vec![row(1, &[1.0], 1), row(1, &[2.0], 2)];
        assert!(matches!(
            exact_cosine_search(
                &duplicate,
                &[1.0],
                &BTreeSet::from([uuid(1)]),
                1,
                limits,
                || Ok(())
            ),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));

        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join(VECTOR_DATA_FILE), b"not parquet").unwrap();
        assert!(matches!(
            read_vector_snapshot(dir.path(), 1, limits, || Ok(())),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
    }

    #[test]
    fn limits_and_cancellation_return_no_partial_hits() {
        let rows = vec![row(1, &[1.0, 0.0], 1), row(2, &[0.0, 1.0], 2)];
        let eligible = BTreeSet::from([uuid(1), uuid(2)]);
        let limited = VectorStoreLimits {
            vector_cells: 3,
            ..VectorStoreLimits::default()
        };
        assert!(matches!(
            exact_cosine_search(&rows, &[1.0, 0.0], &eligible, 2, limited, || Ok(())),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "vector_cells",
                ..
            })
        ));

        let checks = Cell::new(0);
        let result = exact_cosine_search(
            &rows,
            &[1.0, 0.0],
            &eligible,
            2,
            VectorStoreLimits::default(),
            || {
                checks.set(checks.get() + 1);
                if checks.get() == 2 {
                    Err(SearchArtifactError::Cancelled)
                } else {
                    Ok(())
                }
            },
        );
        assert!(matches!(result, Err(SearchArtifactError::Cancelled)));
    }

    #[test]
    fn every_vector_row_search_and_persistence_limit_is_named_and_atomic() {
        let base = VectorStoreLimits::default();
        let rows = vec![row(1, &[1.0, 0.0], 1), row(2, &[0.0, 1.0], 2)];
        let eligible = BTreeSet::from([uuid(1), uuid(2)]);

        for (limits, resource) in [
            (
                VectorStoreLimits {
                    stored_vectors: 1,
                    ..base
                },
                "stored_vectors",
            ),
            (
                VectorStoreLimits {
                    vector_cells: 3,
                    ..base
                },
                "vector_cells",
            ),
        ] {
            assert!(matches!(
                validate_rows(&rows, Some(2), limits, Path::new("<memory>")),
                Err(SearchArtifactError::ResourceExhausted { resource: actual, .. }) if actual == resource
            ));
        }
        assert!(matches!(
            exact_cosine_search(&rows, &[1.0, 0.0], &eligible, 0, base, || Ok(())),
            Err(SearchArtifactError::InvalidSelector { field: "limit", .. })
        ));
        assert!(matches!(
            exact_cosine_search(
                &rows,
                &[1.0, 0.0],
                &eligible,
                2,
                VectorStoreLimits {
                    eligible_nodes: 1,
                    ..base
                },
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "eligible_nodes",
                ..
            })
        ));
        assert!(matches!(
            exact_cosine_search(
                &rows,
                &[1.0, 0.0],
                &eligible,
                2,
                VectorStoreLimits { results: 1, ..base },
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "search_results",
                ..
            })
        ));

        let mut at_limit = vec![row(1, &[1.0], 1)];
        let before = at_limit.clone();
        assert!(matches!(
            apply_vector_upsert(
                &mut at_limit,
                uuid(2),
                &[1.0],
                2,
                VectorStoreLimits {
                    stored_vectors: 1,
                    ..base
                }
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "stored_vectors",
                ..
            })
        ));
        assert_eq!(at_limit, before);
    }

    #[test]
    fn vector_row_validation_rejects_dimension_order_duplicate_and_zero_norm() {
        let limits = VectorStoreLimits::default();
        for (rows, fragment) in [
            (vec![row(1, &[1.0, 0.0], 1), row(2, &[1.0], 2)], "dimension"),
            (vec![row(2, &[1.0], 1), row(1, &[1.0], 2)], "not sorted"),
            (
                vec![row(1, &[1.0], 1), row(1, &[1.0], 2)],
                "duplicate node_uuid",
            ),
            (vec![row(1, &[0.0], 1)], "zero"),
        ] {
            let error = validate_rows(&rows, None, limits, Path::new("fixture")).unwrap_err();
            assert!(error.to_string().contains(fragment), "{error}");
        }
        assert!(validate_rows(&[], None, limits, Path::new("fixture")).is_ok());
        assert!(matches!(
            vector_schema(0, limits),
            Err(SearchArtifactError::InvalidSelector {
                field: "vector",
                ..
            })
        ));
        assert!(matches!(
            vector_schema(
                2,
                VectorStoreLimits {
                    dimensions: 1,
                    ..limits
                }
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "vector_dimensions",
                ..
            })
        ));
    }

    #[test]
    fn snapshot_read_write_failures_preserve_structured_error_kinds() {
        let missing = TempDir::new().unwrap();
        assert!(matches!(
            read_vector_snapshot(missing.path(), 1, VectorStoreLimits::default(), || Ok(())),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));

        let cancelled_write = TempDir::new().unwrap();
        assert!(matches!(
            write_vector_snapshot(
                cancelled_write.path(),
                &[row(1, &[1.0], 1)],
                1,
                VectorStoreLimits::default(),
                || Err(SearchArtifactError::Cancelled)
            ),
            Err(SearchArtifactError::Cancelled)
        ));
        assert!(!cancelled_write.path().join(VECTOR_DATA_FILE).exists());

        let dir = TempDir::new().unwrap();
        write_vector_snapshot(
            dir.path(),
            &[row(1, &[1.0], 1)],
            1,
            VectorStoreLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert!(matches!(
            read_vector_snapshot(
                dir.path(),
                1,
                VectorStoreLimits {
                    parquet_bytes: 1,
                    ..VectorStoreLimits::default()
                },
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "vector_parquet_bytes",
                ..
            })
        ));
    }

    #[test]
    fn wave10_private_vector_error_contracts_are_structured() {
        let limits = VectorStoreLimits::default();
        assert!(matches!(
            dot_and_norm(&[1.0], &[0.0]),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
        assert!(matches!(
            validate_rows(
                &[row(1, &[1.0, 0.0], 1), row(2, &[1.0], 2)],
                Some(2),
                limits,
                Path::new("fixture")
            ),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
        assert!(matches!(
            checked_cells(
                2,
                2,
                VectorStoreLimits {
                    vector_cells: 3,
                    ..limits
                }
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "vector_cells",
                ..
            })
        ));
        assert!(matches!(build("failed"), SearchArtifactError::Build(_)));
        assert!(matches!(
            io(
                "read",
                Path::new("fixture"),
                std::io::Error::other("failed")
            ),
            SearchArtifactError::Io { .. }
        ));
    }

    #[test]
    fn wave12_snapshot_reader_rejects_schema_row_and_query_dimension_mismatches() {
        let dir = TempDir::new().unwrap();
        let limits = VectorStoreLimits::default();
        write_vector_snapshot(dir.path(), &[row(1, &[1.0, 0.0], 1)], 2, limits, || Ok(())).unwrap();

        assert!(matches!(
            read_vector_snapshot(dir.path(), 1, limits, || Ok(())),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
        assert!(matches!(
            read_vector_snapshot(
                dir.path(),
                2,
                VectorStoreLimits {
                    stored_vectors: 0,
                    ..limits
                },
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "stored_vectors",
                ..
            })
        ));

        let key = SearchArtifactKey::vector("Person", "semantic").unwrap();
        let artifact = PublishedSearchArtifact {
            path: dir.path().to_path_buf(),
            manifest: SearchManifest::for_key(
                &key,
                VECTOR_BACKEND_VERSION,
                VECTOR_CONTRACT_VERSION,
                Some(2),
                &source_snapshot().unwrap(),
                true,
            )
            .unwrap(),
        };
        assert!(matches!(
            search_published_vectors(&artifact, &[1.0], &BTreeSet::new(), 1, limits, || Ok(())),
            Err(SearchArtifactError::InvalidSelector {
                field: "vector",
                ..
            })
        ));
    }
}
