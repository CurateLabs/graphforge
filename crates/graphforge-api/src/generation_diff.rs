//! Consumer-neutral semantic graph changes between immutable generations.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, ListArray, ListBuilder, StringArray, StringBuilder,
    StructArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use graphforge_core::{ApiErrorCode, GfError};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{CancellationToken, GraphForge};

/// Exact identity of one immutable committed project generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommittedGenerationIdentity {
    /// Durable UUID naming the immutable generation directory and manifest.
    pub generation_uuid: Uuid,
    /// SHA-256 of the exact canonical generation manifest bytes.
    pub manifest_sha256: [u8; 32],
}

/// Hard preflight limits. No stream bytes are returned when either is exceeded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GenerationDiffLimits {
    /// Maximum combined node and edge records admitted at either endpoint.
    pub max_records_per_generation: usize,
    /// Maximum combined encoded size of all six IPC streams.
    pub max_output_bytes: usize,
}

impl Default for GenerationDiffLimits {
    fn default() -> Self {
        Self {
            max_records_per_generation: 1_000_000,
            max_output_bytes: 256 * 1024 * 1024,
        }
    }
}

/// One exact, retry-stable semantic diff request.
#[derive(Clone, Debug)]
pub struct GenerationDiffRequest {
    /// Exact earlier committed generation.
    pub source: CommittedGenerationIdentity,
    /// Exact later committed generation.
    pub target: CommittedGenerationIdentity,
    /// Hard request resource limits.
    pub limits: GenerationDiffLimits,
    /// Optional cooperative cancellation shared with the caller.
    pub cancellation: Option<CancellationToken>,
}

/// Typed reason a consumer must discard incremental state and perform a full load.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReloadRequiredReason {
    /// The requested generation has been compacted or is otherwise absent.
    GenerationUnavailable,
    /// A generation UUID was paired with the wrong manifest fingerprint.
    IdentityMismatch,
    /// A retained generation failed integrity validation.
    CorruptGeneration,
    /// The retained graph contract cannot be decoded by this runtime.
    IncompatibleGraph,
    /// The request exceeds its record or encoded-byte budget.
    ResourceLimit,
}

/// One self-describing Arrow IPC stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphChangeStream {
    /// Deterministic logical row count in the stream.
    pub row_count: usize,
    /// Complete Arrow IPC stream bytes, including schema metadata.
    pub ipc: Vec<u8>,
}

/// Six independently consumable streams plus exact changed-property names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationGraphDiff {
    /// Exact source identity used for every stream.
    pub source: CommittedGenerationIdentity,
    /// Exact target identity used for every stream.
    pub target: CommittedGenerationIdentity,
    /// Complete target-state rows for nodes absent from the source.
    pub added_nodes: GraphChangeStream,
    /// Source rows identifying nodes absent from the target.
    pub removed_nodes: GraphChangeStream,
    /// Complete target-state rows for changed nodes.
    pub modified_nodes: GraphChangeStream,
    /// Complete target-state rows for edges absent from the source.
    pub added_edges: GraphChangeStream,
    /// Source rows identifying edges absent from the target.
    pub removed_edges: GraphChangeStream,
    /// Complete target-state rows for changed edges.
    pub modified_edges: GraphChangeStream,
    /// Canonical changed-property names keyed by modified node UUID.
    pub modified_node_properties: BTreeMap<Uuid, Vec<String>>,
    /// Canonical changed-property names keyed by modified edge UUID.
    pub modified_edge_properties: BTreeMap<Uuid, Vec<String>>,
    /// SHA-256 binding both endpoint identities and all six stream digests.
    pub checkpoint_binding: [u8; 32],
}

/// Successful all-stream result or a typed full-reload requirement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GenerationDiffDisposition {
    /// Every stream was derived and encoded within the request limits.
    Ready(Box<GenerationGraphDiff>),
    /// No partial output is returned; the consumer must perform a full load.
    ReloadRequired(ReloadRequiredReason),
}

#[derive(Clone)]
struct Row {
    batch: RecordBatch,
    index: usize,
    fingerprint: [u8; 32],
    properties: BTreeMap<String, String>,
}

struct SemanticRows {
    schema: Arc<Schema>,
    rows: BTreeMap<Uuid, Row>,
}

type ClassifiedChanges = (
    GraphChangeStream,
    GraphChangeStream,
    GraphChangeStream,
    BTreeMap<Uuid, Vec<String>>,
);

impl GraphForge {
    /// Return the exact currently selected committed-generation identity.
    pub fn committed_generation_identity(&self) -> Result<CommittedGenerationIdentity, GfError> {
        let generation = self.generation_for_read()?;
        Ok(CommittedGenerationIdentity {
            generation_uuid: generation.generation_uuid(),
            manifest_sha256: generation.manifest_sha256(),
        })
    }

    /// Derive semantic graph changes from immutable graph states, never journals.
    #[allow(clippy::too_many_lines)]
    pub fn diff_committed_generations(
        &self,
        request: &GenerationDiffRequest,
    ) -> Result<GenerationDiffDisposition, GfError> {
        checkpoint(request)?;
        if request.source.generation_uuid == request.target.generation_uuid
            && request.source.manifest_sha256 != request.target.manifest_sha256
        {
            return Ok(GenerationDiffDisposition::ReloadRequired(
                ReloadRequiredReason::IdentityMismatch,
            ));
        }
        let resolve = |identity: CommittedGenerationIdentity| {
            graphforge_storage::resolve_verified_generation(
                self.resolved_generation.container_root(),
                identity.generation_uuid,
                identity.manifest_sha256,
            )
        };
        let source = match resolve(request.source) {
            Ok(v) => v,
            Err(e) => {
                return Ok(GenerationDiffDisposition::ReloadRequired(map_resolution(
                    &e,
                )));
            }
        };
        let target = match resolve(request.target) {
            Ok(v) => v,
            Err(e) => {
                return Ok(GenerationDiffDisposition::ReloadRequired(map_resolution(
                    &e,
                )));
            }
        };
        let source_graph = match GraphForge::open_resolved_with_lifecycle_mode(
            source.container_root().to_path_buf(),
            source,
            true,
            self.lifecycle_mode,
        ) {
            Ok(graph) => graph,
            Err(error) => return reload_or_cancel(error),
        };
        let target_graph = match GraphForge::open_resolved_with_lifecycle_mode(
            target.container_root().to_path_buf(),
            target,
            true,
            self.lifecycle_mode,
        ) {
            Ok(graph) => graph,
            Err(error) => return reload_or_cancel(error),
        };
        let source_nodes = match semantic_rows(&source_graph, node_query(), request) {
            Ok(rows) => rows,
            Err(error) => return reload_or_cancel(error),
        };
        let target_nodes = match semantic_rows(&target_graph, node_query(), request) {
            Ok(rows) => rows,
            Err(error) => return reload_or_cancel(error),
        };
        let source_edges = match semantic_rows(&source_graph, edge_query(), request) {
            Ok(rows) => rows,
            Err(error) => return reload_or_cancel(error),
        };
        let target_edges = match semantic_rows(&target_graph, edge_query(), request) {
            Ok(rows) => rows,
            Err(error) => return reload_or_cancel(error),
        };
        if source_nodes
            .rows
            .len()
            .saturating_add(source_edges.rows.len())
            > request.limits.max_records_per_generation
            || target_nodes
                .rows
                .len()
                .saturating_add(target_edges.rows.len())
                > request.limits.max_records_per_generation
        {
            return Ok(GenerationDiffDisposition::ReloadRequired(
                ReloadRequiredReason::ResourceLimit,
            ));
        }
        let (added_nodes, removed_nodes, modified_nodes, modified_node_properties) =
            classify(&source_nodes, &target_nodes, request.source, request.target)?;
        let (added_edges, removed_edges, modified_edges, modified_edge_properties) =
            classify(&source_edges, &target_edges, request.source, request.target)?;
        checkpoint(request)?;
        let total = [
            &added_nodes,
            &removed_nodes,
            &modified_nodes,
            &added_edges,
            &removed_edges,
            &modified_edges,
        ]
        .iter()
        .map(|s| s.ipc.len())
        .sum::<usize>();
        if total > request.limits.max_output_bytes {
            return Ok(GenerationDiffDisposition::ReloadRequired(
                ReloadRequiredReason::ResourceLimit,
            ));
        }
        let mut digest = Sha256::new();
        digest.update(b"graphforge-semantic-generation-diff/1");
        digest.update(request.source.generation_uuid.as_bytes());
        digest.update(request.source.manifest_sha256);
        digest.update(request.target.generation_uuid.as_bytes());
        digest.update(request.target.manifest_sha256);
        for stream in [
            &added_nodes,
            &removed_nodes,
            &modified_nodes,
            &added_edges,
            &removed_edges,
            &modified_edges,
        ] {
            digest.update(Sha256::digest(&stream.ipc));
        }
        Ok(GenerationDiffDisposition::Ready(Box::new(
            GenerationGraphDiff {
                source: request.source,
                target: request.target,
                added_nodes,
                removed_nodes,
                modified_nodes,
                added_edges,
                removed_edges,
                modified_edges,
                modified_node_properties,
                modified_edge_properties,
                checkpoint_binding: digest.finalize().into(),
            },
        )))
    }
}

fn node_query() -> &'static str {
    "MATCH (n) RETURN n.node_uuid AS record_uuid, labels(n) AS labels, properties(n) AS properties ORDER BY record_uuid"
}
fn edge_query() -> &'static str {
    "MATCH (s)-[r]->(t) RETURN r.edge_uuid AS record_uuid, s.node_uuid AS source_uuid, t.node_uuid AS target_uuid, type(r) AS relationship_type, properties(r) AS properties ORDER BY record_uuid"
}

fn semantic_rows(
    graph: &GraphForge,
    query: &str,
    request: &GenerationDiffRequest,
) -> Result<SemanticRows, GfError> {
    checkpoint(request)?;
    let result = graph.execute_read_only(query)?;
    let result_schema = result.schema.clone();
    let mut out = BTreeMap::new();
    for batch in result.batches {
        let uuids = batch
            .column_by_name("record_uuid")
            .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| schema("semantic UUID column is incompatible"))?;
        for row in 0..batch.num_rows() {
            checkpoint(request)?;
            let uuid = Uuid::from_slice(uuids.value(row))
                .map_err(|_| schema("semantic UUID is invalid"))?;
            let fingerprint = row_fingerprint(&batch, row)?;
            let properties = property_values(&batch, row)?;
            if out
                .insert(
                    uuid,
                    Row {
                        batch: batch.clone(),
                        index: row,
                        fingerprint,
                        properties,
                    },
                )
                .is_some()
            {
                return Err(schema("semantic UUID is duplicated"));
            }
        }
    }
    Ok(SemanticRows {
        schema: result_schema,
        rows: out,
    })
}

fn classify(
    source: &SemanticRows,
    target: &SemanticRows,
    from: CommittedGenerationIdentity,
    to: CommittedGenerationIdentity,
) -> Result<ClassifiedChanges, GfError> {
    let added = target
        .rows
        .iter()
        .filter(|(id, _)| !source.rows.contains_key(id))
        .map(|(_, r)| r)
        .collect::<Vec<_>>();
    let removed = source
        .rows
        .iter()
        .filter(|(id, _)| !target.rows.contains_key(id))
        .map(|(_, r)| r)
        .collect::<Vec<_>>();
    let modified = target
        .rows
        .iter()
        .filter(|(id, r)| {
            source
                .rows
                .get(id)
                .is_some_and(|old| old.fingerprint != r.fingerprint)
        })
        .map(|(_, r)| r)
        .collect::<Vec<_>>();
    let mut changed = BTreeMap::new();
    for (id, new) in &target.rows {
        if let Some(old) = source
            .rows
            .get(id)
            .filter(|old| old.fingerprint != new.fingerprint)
        {
            let keys = old
                .properties
                .keys()
                .chain(new.properties.keys())
                .cloned()
                .collect::<BTreeSet<_>>();
            changed.insert(
                *id,
                keys.into_iter()
                    .filter(|key| old.properties.get(key) != new.properties.get(key))
                    .collect(),
            );
        }
    }
    Ok((
        encode(&added, &target.schema, "added", from, to, None)?,
        encode(&removed, &source.schema, "removed", from, to, None)?,
        encode(
            &modified,
            &target.schema,
            "modified",
            from,
            to,
            Some(&changed),
        )?,
        changed,
    ))
}

fn encode(
    rows: &[&Row],
    row_schema: &Arc<Schema>,
    kind: &str,
    from: CommittedGenerationIdentity,
    to: CommittedGenerationIdentity,
    changed: Option<&BTreeMap<Uuid, Vec<String>>>,
) -> Result<GraphChangeStream, GfError> {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        "graphforge.contract".into(),
        "semantic-generation-diff/1".into(),
    );
    metadata.insert("graphforge.change_kind".into(), kind.into());
    metadata.insert(
        "graphforge.source_generation_uuid".into(),
        from.generation_uuid.to_string(),
    );
    metadata.insert(
        "graphforge.source_manifest_sha256".into(),
        hex(&from.manifest_sha256),
    );
    metadata.insert(
        "graphforge.target_generation_uuid".into(),
        to.generation_uuid.to_string(),
    );
    metadata.insert(
        "graphforge.target_manifest_sha256".into(),
        hex(&to.manifest_sha256),
    );
    let mut fields = row_schema.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        "changed_properties",
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        false,
    )));
    let schema_ref = Arc::new(Schema::new_with_metadata(fields, metadata));
    let slices = rows
        .iter()
        .map(|r| {
            let b = r.batch.slice(r.index, 1);
            let uuid = row_uuid(&r.batch, r.index)?;
            let names = changed.and_then(|values| values.get(&uuid));
            let mut builder = ListBuilder::new(StringBuilder::new());
            if let Some(names) = names {
                for name in names {
                    builder.values().append_value(name);
                }
            }
            builder.append(true);
            let mut columns = b.columns().to_vec();
            columns.push(Arc::new(builder.finish()) as ArrayRef);
            RecordBatch::try_new(schema_ref.clone(), columns).map_err(|e| schema(e.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(Cursor::new(&mut ipc), &schema_ref)
            .map_err(|e| schema(e.to_string()))?;
        if !slices.is_empty() {
            let batch = concat_batches(&schema_ref, &slices).map_err(|e| schema(e.to_string()))?;
            writer.write(&batch).map_err(|e| schema(e.to_string()))?;
        }
        writer.finish().map_err(|e| schema(e.to_string()))?;
    }
    Ok(GraphChangeStream {
        row_count: rows.len(),
        ipc,
    })
}

fn row_uuid(batch: &RecordBatch, row: usize) -> Result<Uuid, GfError> {
    let values = batch
        .column_by_name("record_uuid")
        .and_then(|array| array.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| schema("semantic UUID column is incompatible"))?;
    Uuid::from_slice(values.value(row)).map_err(|_| schema("semantic UUID is invalid"))
}

fn row_fingerprint(batch: &RecordBatch, row: usize) -> Result<[u8; 32], GfError> {
    let mut d = Sha256::new();
    d.update(b"graphforge-semantic-row/1");
    for (f, a) in batch.schema().fields().iter().zip(batch.columns()) {
        d.update(f.name().as_bytes());
        let s = arrow::util::display::array_value_to_string(a.as_ref(), row)
            .map_err(|e| schema(e.to_string()))?;
        d.update(s.as_bytes());
    }
    Ok(d.finalize().into())
}
fn property_values(batch: &RecordBatch, row: usize) -> Result<BTreeMap<String, String>, GfError> {
    let Some(a) = batch.column_by_name("properties") else {
        return Ok(BTreeMap::new());
    };
    let s = a
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| schema("properties column is incompatible"))?;
    if let Some(map) = s.column_by_name("__het_map") {
        let map = map
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| schema("properties map column is incompatible"))?;
        if map.is_null(row) {
            return Ok(BTreeMap::new());
        }
        let entries = map.value(row);
        let entries = entries
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| schema("properties map entries are incompatible"))?;
        let keys = entries
            .column_by_name("__het_mkey")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| schema("properties map keys are incompatible"))?;
        let values = entries
            .column_by_name("__het_mval")
            .ok_or_else(|| schema("properties map values are incompatible"))?;
        let mut out = BTreeMap::new();
        for index in 0..entries.len() {
            out.insert(
                keys.value(index).to_owned(),
                arrow::util::display::array_value_to_string(values.as_ref(), index)
                    .map_err(|error| schema(error.to_string()))?,
            );
        }
        return Ok(out);
    }
    let mut out = BTreeMap::new();
    for (f, a) in s.fields().iter().zip(s.columns()) {
        let value = if a.is_null(row) {
            "<null>".into()
        } else {
            arrow::util::display::array_value_to_string(a.as_ref(), row)
                .map_err(|e| schema(e.to_string()))?
        };
        out.insert(f.name().clone(), value);
    }
    Ok(out)
}
fn checkpoint(r: &GenerationDiffRequest) -> Result<(), GfError> {
    r.cancellation
        .as_ref()
        .map_or(Ok(()), CancellationToken::checkpoint)
}
fn map_resolution(e: &GfError) -> ReloadRequiredReason {
    let message = e.to_string();
    if message.contains("manifest digest does not match") {
        return ReloadRequiredReason::IdentityMismatch;
    }
    if message.contains("required directory is missing")
        || message.contains("selected generation manifest is missing")
    {
        return ReloadRequiredReason::GenerationUnavailable;
    }
    match e.code() {
        "GF_PROJECT_CORRUPT" => ReloadRequiredReason::CorruptGeneration,
        "GF_UNSUPPORTED_PROJECT_FORMAT" => ReloadRequiredReason::IncompatibleGraph,
        "GF_SCHEMA_MISMATCH" | "GF_PARSE" | "GF_PLAN" | "GF_EXECUTION" | "GF_VALIDATION" => {
            ReloadRequiredReason::IncompatibleGraph
        }
        _ => ReloadRequiredReason::GenerationUnavailable,
    }
}
fn reload_or_cancel(error: GfError) -> Result<GenerationDiffDisposition, GfError> {
    if error.code() == "GF_CANCELLED" {
        Err(error)
    } else {
        Ok(GenerationDiffDisposition::ReloadRequired(map_resolution(
            &error,
        )))
    }
}
fn schema(message: impl Into<String>) -> GfError {
    GfError::Api {
        code: ApiErrorCode::SchemaMismatch,
        message: message.into(),
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len().saturating_mul(2)),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        },
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;

    use arrow::ipc::reader::StreamReader;

    use super::*;
    use crate::PropValue;

    fn identity(graph: &GraphForge) -> CommittedGenerationIdentity {
        let generation = graph.generation_for_read().unwrap();
        CommittedGenerationIdentity {
            generation_uuid: generation.generation_uuid(),
            manifest_sha256: generation.manifest_sha256(),
        }
    }

    fn request(
        source: CommittedGenerationIdentity,
        target: CommittedGenerationIdentity,
    ) -> GenerationDiffRequest {
        GenerationDiffRequest {
            source,
            target,
            limits: GenerationDiffLimits::default(),
            cancellation: None,
        }
    }

    fn decoded_rows(stream: &GraphChangeStream) -> usize {
        StreamReader::try_new(Cursor::new(&stream.ipc), None)
            .unwrap()
            .map(|batch| batch.unwrap().num_rows())
            .sum()
    }

    fn decoded_uuids(stream: &GraphChangeStream) -> Vec<Uuid> {
        StreamReader::try_new(Cursor::new(&stream.ipc), None)
            .unwrap()
            .flat_map(|batch| {
                let batch = batch.unwrap();
                let values = batch
                    .column_by_name("record_uuid")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
                    .clone();
                (0..values.len()).map(move |row| Uuid::from_slice(values.value(row)).unwrap())
            })
            .collect()
    }

    fn canonical_rows(batches: &[RecordBatch]) -> BTreeMap<Uuid, BTreeMap<String, String>> {
        let mut rows = BTreeMap::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let uuid = row_uuid(batch, row).unwrap();
                let values = batch
                    .schema()
                    .fields()
                    .iter()
                    .zip(batch.columns())
                    .filter(|(field, _)| field.name() != "changed_properties")
                    .map(|(field, array)| {
                        (
                            field.name().clone(),
                            arrow::util::display::array_value_to_string(array.as_ref(), row)
                                .unwrap(),
                        )
                    })
                    .collect();
                rows.insert(uuid, values);
            }
        }
        rows
    }

    fn stream_rows(stream: &GraphChangeStream) -> BTreeMap<Uuid, BTreeMap<String, String>> {
        let batches = StreamReader::try_new(Cursor::new(&stream.ipc), None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        canonical_rows(&batches)
    }

    fn assert_public_stream_schema(
        stream: &GraphChangeStream,
        change_kind: &str,
        source: CommittedGenerationIdentity,
        target: CommittedGenerationIdentity,
        edge: bool,
    ) {
        let reader = StreamReader::try_new(Cursor::new(&stream.ipc), None).unwrap();
        let schema = reader.schema();
        let metadata = schema.metadata();
        assert_eq!(
            metadata.get("graphforge.contract").map(String::as_str),
            Some("semantic-generation-diff/1")
        );
        assert_eq!(
            metadata.get("graphforge.change_kind").map(String::as_str),
            Some(change_kind)
        );
        assert_eq!(
            metadata.get("graphforge.source_generation_uuid"),
            Some(&source.generation_uuid.to_string())
        );
        assert_eq!(
            metadata.get("graphforge.source_manifest_sha256"),
            Some(&hex(&source.manifest_sha256))
        );
        assert_eq!(
            metadata.get("graphforge.target_generation_uuid"),
            Some(&target.generation_uuid.to_string())
        );
        assert_eq!(
            metadata.get("graphforge.target_manifest_sha256"),
            Some(&hex(&target.manifest_sha256))
        );
        let record_uuid = schema.field_with_name("record_uuid").unwrap();
        assert_eq!(record_uuid.data_type(), &DataType::FixedSizeBinary(16));
        assert!(!record_uuid.is_nullable());
        let changed = schema.field_with_name("changed_properties").unwrap();
        assert!(!changed.is_nullable());
        assert_eq!(
            changed.data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
        if edge {
            for name in ["source_uuid", "target_uuid"] {
                let field = schema.field_with_name(name).unwrap();
                assert_eq!(field.data_type(), &DataType::FixedSizeBinary(16));
                assert!(!field.is_nullable());
            }
            let relation = schema.field_with_name("relationship_type").unwrap();
            assert_eq!(relation.data_type(), &DataType::Utf8);
            assert!(relation.is_nullable());
        } else {
            assert!(schema.field_with_name("labels").is_ok());
        }
        assert!(schema.field_with_name("properties").is_ok());
    }

    fn pinned_graph(graph: &GraphForge, identity: CommittedGenerationIdentity) -> GraphForge {
        let generation = graphforge_storage::resolve_verified_generation(
            graph.resolved_generation.container_root(),
            identity.generation_uuid,
            identity.manifest_sha256,
        )
        .unwrap();
        GraphForge::open_resolved_with_lifecycle_mode(
            generation.container_root().to_path_buf(),
            generation,
            true,
            graph.lifecycle_mode,
        )
        .unwrap()
    }

    fn apply_streams(
        state: &mut BTreeMap<Uuid, BTreeMap<String, String>>,
        removed: &GraphChangeStream,
        added: &GraphChangeStream,
        modified: &GraphChangeStream,
    ) {
        for uuid in decoded_uuids(removed) {
            state.remove(&uuid);
        }
        state.extend(stream_rows(added));
        state.extend(stream_rows(modified));
    }

    #[test]
    fn generation_bound_add_is_retry_deterministic_and_every_stream_is_valid_ipc() {
        let graph = GraphForge::new(None).unwrap();
        let existing = graph
            .add_node(
                "Person",
                &HashMap::from([("name".into(), PropValue::Str("Grace".into()))]),
            )
            .unwrap();
        let source = graph.committed_generation_identity().unwrap();
        let added = graph
            .add_node(
                "Person",
                &HashMap::from([("name".into(), PropValue::Str("Ada".into()))]),
            )
            .unwrap();
        graph
            .add_edge(
                &existing,
                "KNOWS",
                &added,
                &HashMap::from([("since".into(), PropValue::Int(2026))]),
            )
            .unwrap();
        let target = graph.committed_generation_identity().unwrap();

        let first = graph
            .diff_committed_generations(&request(source, target))
            .unwrap();
        let again = graph
            .diff_committed_generations(&request(source, target))
            .unwrap();
        assert_eq!(first, again);
        let GenerationDiffDisposition::Ready(diff) = first else {
            panic!("retained generations must be diffable")
        };
        assert_eq!(diff.added_nodes.row_count, 1);
        assert_eq!(decoded_rows(&diff.added_nodes), 1);
        assert_eq!(decoded_uuids(&diff.added_nodes), vec![added.uuid]);
        let mut added_reader =
            StreamReader::try_new(Cursor::new(&diff.added_nodes.ipc), None).unwrap();
        let added_batch = added_reader.next().unwrap().unwrap();
        assert!(
            property_values(&added_batch, 0)
                .unwrap()
                .contains_key("name")
        );
        assert_eq!(diff.added_edges.row_count, 1);
        assert_eq!(decoded_rows(&diff.added_edges), 1);
        let added_node_ids = decoded_uuids(&diff.added_nodes)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let reconstructed_target_nodes = BTreeSet::from([existing.uuid])
            .union(&added_node_ids)
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            reconstructed_target_nodes,
            BTreeSet::from([existing.uuid, added.uuid])
        );
        let source_graph = pinned_graph(&graph, source);
        let target_graph = pinned_graph(&graph, target);
        let mut reconstructed_nodes = canonical_rows(
            &source_graph
                .execute_read_only(node_query())
                .unwrap()
                .batches,
        );
        let mut reconstructed_edges = canonical_rows(
            &source_graph
                .execute_read_only(edge_query())
                .unwrap()
                .batches,
        );
        apply_streams(
            &mut reconstructed_nodes,
            &diff.removed_nodes,
            &diff.added_nodes,
            &diff.modified_nodes,
        );
        apply_streams(
            &mut reconstructed_edges,
            &diff.removed_edges,
            &diff.added_edges,
            &diff.modified_edges,
        );
        assert_eq!(
            reconstructed_nodes,
            canonical_rows(
                &target_graph
                    .execute_read_only(node_query())
                    .unwrap()
                    .batches
            )
        );
        assert_eq!(
            reconstructed_edges,
            canonical_rows(
                &target_graph
                    .execute_read_only(edge_query())
                    .unwrap()
                    .batches
            )
        );
        for stream in [
            &diff.removed_nodes,
            &diff.modified_nodes,
            &diff.removed_edges,
            &diff.modified_edges,
        ] {
            assert_eq!(stream.row_count, 0);
            assert_eq!(decoded_rows(stream), 0);
        }
        let reader = StreamReader::try_new(Cursor::new(&diff.added_nodes.ipc), None).unwrap();
        assert_eq!(
            reader
                .schema()
                .metadata()
                .get("graphforge.target_generation_uuid"),
            Some(&target.generation_uuid.to_string())
        );
        let stream_schema = reader.schema();
        let metadata = stream_schema.metadata();
        assert_eq!(
            metadata.get("graphforge.change_kind").map(String::as_str),
            Some("added")
        );
        assert_eq!(
            metadata.get("graphforge.source_generation_uuid"),
            Some(&source.generation_uuid.to_string())
        );
        assert_eq!(
            metadata.get("graphforge.source_manifest_sha256"),
            Some(&hex(&source.manifest_sha256))
        );
        assert_eq!(
            metadata.get("graphforge.target_manifest_sha256"),
            Some(&hex(&target.manifest_sha256))
        );
        assert!(diff.modified_node_properties.get(&added.uuid).is_none());
        assert_ne!(diff.checkpoint_binding, [0; 32]);
        for (stream, kind, edge) in [
            (&diff.added_nodes, "added", false),
            (&diff.removed_nodes, "removed", false),
            (&diff.modified_nodes, "modified", false),
            (&diff.added_edges, "added", true),
            (&diff.removed_edges, "removed", true),
            (&diff.modified_edges, "modified", true),
        ] {
            assert_public_stream_schema(stream, kind, source, target, edge);
        }
    }

    #[test]
    fn generation_ladder_and_direct_range_reconstruct_the_exact_target() {
        let graph = GraphForge::new(None).unwrap();
        let first = graph
            .add_node(
                "Person",
                &HashMap::from([("name".into(), PropValue::Str("Ada".into()))]),
            )
            .unwrap();
        let source = identity(&graph);
        let second = graph
            .add_node(
                "Person",
                &HashMap::from([("name".into(), PropValue::Str("Grace".into()))]),
            )
            .unwrap();
        graph
            .add_edge(&first, "KNOWS", &second, &HashMap::new())
            .unwrap();
        let middle = identity(&graph);
        graph.execute("MATCH (n) SET n.active = true").unwrap();
        let target = identity(&graph);

        let ready = |from, to| {
            let GenerationDiffDisposition::Ready(diff) = graph
                .diff_committed_generations(&request(from, to))
                .unwrap()
            else {
                panic!("retained generation ladder must be diffable")
            };
            diff
        };
        let source_graph = pinned_graph(&graph, source);
        let target_graph = pinned_graph(&graph, target);
        let expected_nodes = canonical_rows(
            &target_graph
                .execute_read_only(node_query())
                .unwrap()
                .batches,
        );
        let expected_edges = canonical_rows(
            &target_graph
                .execute_read_only(edge_query())
                .unwrap()
                .batches,
        );
        let initial_nodes = canonical_rows(
            &source_graph
                .execute_read_only(node_query())
                .unwrap()
                .batches,
        );
        let initial_edges = canonical_rows(
            &source_graph
                .execute_read_only(edge_query())
                .unwrap()
                .batches,
        );

        let mut ladder_nodes = initial_nodes.clone();
        let mut ladder_edges = initial_edges.clone();
        for diff in [ready(source, middle), ready(middle, target)] {
            apply_streams(
                &mut ladder_nodes,
                &diff.removed_nodes,
                &diff.added_nodes,
                &diff.modified_nodes,
            );
            apply_streams(
                &mut ladder_edges,
                &diff.removed_edges,
                &diff.added_edges,
                &diff.modified_edges,
            );
        }
        assert_eq!(ladder_nodes, expected_nodes);
        assert_eq!(ladder_edges, expected_edges);

        let direct = ready(source, target);
        let mut direct_nodes = initial_nodes;
        let mut direct_edges = initial_edges;
        apply_streams(
            &mut direct_nodes,
            &direct.removed_nodes,
            &direct.added_nodes,
            &direct.modified_nodes,
        );
        apply_streams(
            &mut direct_edges,
            &direct.removed_edges,
            &direct.added_edges,
            &direct.modified_edges,
        );
        assert_eq!(direct_nodes, expected_nodes);
        assert_eq!(direct_edges, expected_edges);
        assert_eq!(direct.source, source);
        assert_eq!(direct.target, target);
    }

    #[test]
    fn cancellation_and_resource_limit_fail_before_partial_output() {
        let graph = GraphForge::new(None).unwrap();
        let source = identity(&graph);
        graph
            .add_node("Person", &HashMap::<String, PropValue>::new())
            .unwrap();
        let target = identity(&graph);
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut cancelled = request(source, target);
        cancelled.cancellation = Some(cancellation);
        assert_eq!(
            graph
                .diff_committed_generations(&cancelled)
                .unwrap_err()
                .code(),
            "GF_CANCELLED"
        );

        let mut bounded = request(source, target);
        bounded.limits.max_records_per_generation = 0;
        assert_eq!(
            graph.diff_committed_generations(&bounded).unwrap(),
            GenerationDiffDisposition::ReloadRequired(ReloadRequiredReason::ResourceLimit)
        );

        let mut byte_bounded = request(source, target);
        byte_bounded.limits.max_output_bytes = 1;
        assert_eq!(
            graph.diff_committed_generations(&byte_bounded).unwrap(),
            GenerationDiffDisposition::ReloadRequired(ReloadRequiredReason::ResourceLimit)
        );
    }

    #[test]
    fn modified_stream_contains_complete_target_row_and_changed_property_names() {
        let graph = GraphForge::new(None).unwrap();
        let node = graph
            .add_node(
                "Person",
                &HashMap::from([
                    ("name".into(), PropValue::Str("Ada".into())),
                    ("score".into(), PropValue::Int(1)),
                ]),
            )
            .unwrap();
        let source = identity(&graph);
        graph
            .execute("MATCH (n) SET n.name = 'Ada Lovelace'")
            .unwrap();
        let target = identity(&graph);
        let GenerationDiffDisposition::Ready(diff) = graph
            .diff_committed_generations(&request(source, target))
            .unwrap()
        else {
            panic!("retained generations must be diffable")
        };
        assert_eq!(diff.modified_nodes.row_count, 1);
        assert_eq!(
            diff.modified_node_properties.get(&node.uuid),
            Some(&vec!["name".into()])
        );
        let mut reader =
            StreamReader::try_new(Cursor::new(&diff.modified_nodes.ipc), None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert!(batch.column_by_name("labels").is_some());
        assert!(batch.column_by_name("properties").is_some());
        let changed = batch
            .column_by_name("changed_properties")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap()
            .value(0);
        let changed = changed
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(changed.value(0), "name");
    }

    #[test]
    fn edge_modify_and_remove_are_semantic_and_uuid_exact() {
        let graph = GraphForge::new(None).unwrap();
        let left = graph
            .add_node("Person", &HashMap::<String, PropValue>::new())
            .unwrap();
        let right = graph
            .add_node("Person", &HashMap::<String, PropValue>::new())
            .unwrap();
        let edge = graph
            .add_edge(
                &left,
                "KNOWS",
                &right,
                &HashMap::from([("weight".into(), PropValue::Int(1))]),
            )
            .unwrap();
        let before_modify = identity(&graph);
        graph.execute("MATCH ()-[r]->() SET r.weight = 2").unwrap();
        let after_modify = identity(&graph);
        let GenerationDiffDisposition::Ready(modified) = graph
            .diff_committed_generations(&request(before_modify, after_modify))
            .unwrap()
        else {
            panic!("retained generations must be diffable")
        };
        assert_eq!(decoded_uuids(&modified.modified_edges), vec![edge.uuid]);
        assert_eq!(
            modified.modified_edge_properties.get(&edge.uuid),
            Some(&vec!["weight".into()])
        );

        graph.execute("MATCH ()-[r]->() DELETE r").unwrap();
        let after_remove = identity(&graph);
        let GenerationDiffDisposition::Ready(removed) = graph
            .diff_committed_generations(&request(after_modify, after_remove))
            .unwrap()
        else {
            panic!("retained generations must be diffable")
        };
        assert_eq!(decoded_uuids(&removed.removed_edges), vec![edge.uuid]);
    }

    #[test]
    fn node_remove_and_multi_row_order_are_exact() {
        let graph = GraphForge::new(None).unwrap();
        let source = identity(&graph);
        graph
            .execute("CREATE (:Person {name:'c'}), (:Person {name:'a'}), (:Person {name:'b'})")
            .unwrap();
        let target = identity(&graph);
        let GenerationDiffDisposition::Ready(diff) = graph
            .diff_committed_generations(&request(source, target))
            .unwrap()
        else {
            panic!("retained generations must be diffable")
        };
        let added = decoded_uuids(&diff.added_nodes);
        assert_eq!(added.len(), 3);
        assert!(added.windows(2).all(|pair| pair[0] < pair[1]));

        graph
            .execute("MATCH (n:Person {name:'b'}) DELETE n")
            .unwrap();
        let GenerationDiffDisposition::Ready(removed) = graph
            .diff_committed_generations(&request(target, identity(&graph)))
            .unwrap()
        else {
            panic!("retained generations must be diffable")
        };
        assert_eq!(removed.removed_nodes.row_count, 1);
    }

    #[test]
    fn wrong_manifest_binding_is_typed_reload_required() {
        let graph = GraphForge::new(None).unwrap();
        let mut bad = identity(&graph);
        bad.manifest_sha256[0] ^= 1;
        assert_eq!(
            graph
                .diff_committed_generations(&request(bad, identity(&graph)))
                .unwrap(),
            GenerationDiffDisposition::ReloadRequired(ReloadRequiredReason::IdentityMismatch)
        );

        let unavailable = CommittedGenerationIdentity {
            generation_uuid: Uuid::now_v7(),
            manifest_sha256: [0; 32],
        };
        assert_eq!(
            graph
                .diff_committed_generations(&request(unavailable, identity(&graph)))
                .unwrap(),
            GenerationDiffDisposition::ReloadRequired(ReloadRequiredReason::GenerationUnavailable)
        );
    }

    #[test]
    fn corrupt_and_incompatible_generations_are_typed_reload_required() {
        let corrupt_graph = GraphForge::new(None).unwrap();
        corrupt_graph
            .add_node("Person", &HashMap::<String, PropValue>::new())
            .unwrap();
        let corrupt = identity(&corrupt_graph);
        let generation = corrupt_graph.generation_for_read().unwrap();
        let inventory = generation.graph_files_inventory().unwrap().unwrap();
        let relative = &inventory.files[0].relative_path;
        fs::write(
            generation.graph_tree_root().join(relative),
            b"corrupt fixture",
        )
        .unwrap();
        assert_eq!(
            corrupt_graph
                .diff_committed_generations(&request(corrupt, corrupt))
                .unwrap(),
            GenerationDiffDisposition::ReloadRequired(ReloadRequiredReason::CorruptGeneration)
        );

        let incompatible_graph = GraphForge::new(None).unwrap();
        incompatible_graph
            .add_node("Person", &HashMap::<String, PropValue>::new())
            .unwrap();
        let generation = incompatible_graph.generation_for_read().unwrap();
        let manifest_path = generation.generation_root().join("manifest.json");
        let manifest = String::from_utf8(fs::read(&manifest_path).unwrap()).unwrap();
        let graph_participant = manifest.find("\"record_family_id\":\"files\"").unwrap();
        let relative_version = manifest[graph_participant..]
            .find("\"record_version\":1")
            .unwrap();
        let version_start = graph_participant + relative_version;
        let mut bytes = manifest.into_bytes();
        bytes.splice(
            version_start..version_start + "\"record_version\":1".len(),
            "\"record_version\":4294967295".bytes(),
        );
        fs::write(&manifest_path, &bytes).unwrap();
        let incompatible = CommittedGenerationIdentity {
            generation_uuid: generation.generation_uuid(),
            manifest_sha256: Sha256::digest(&bytes).into(),
        };
        assert_eq!(
            incompatible_graph
                .diff_committed_generations(&request(incompatible, incompatible))
                .unwrap(),
            GenerationDiffDisposition::ReloadRequired(ReloadRequiredReason::IncompatibleGraph)
        );
    }
}
