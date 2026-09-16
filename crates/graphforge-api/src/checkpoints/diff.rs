//! Logical checkpoint comparisons and Arrow diff rendering.

use super::{
    CheckpointDiffDetail, CheckpointDiffScope, CheckpointSelector, DiffCheckpointsRequest,
    arrow_error, cancellation, execution, make_batch, page_bounds, page_cursor, schema_mismatch,
};
use crate::{ExecutionResult, GraphForge, PageRequest, PageToken};
use arrow::array::{
    Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, StringBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use graphforge_core::{ApiErrorCode, GfError};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

impl GraphForge {
    /// Compare two checkpoint/current manifest inventories.
    pub fn diff_checkpoints(
        &self,
        request: DiffCheckpointsRequest,
    ) -> Result<ExecutionResult, GfError> {
        let DiffCheckpointsRequest {
            from,
            to,
            scope,
            detail,
            page,
        } = request;
        cancellation(&page)?;
        let binding = diff_request_binding_parts(&from, &to, scope, detail);
        let resolve = |selector| {
            self.resolve_selector(selector).map_err(|error| {
                if page.after.is_some() && error.code() == "GF_CHECKPOINT_NOT_FOUND" {
                    GfError::Api {
                        code: ApiErrorCode::PageSnapshotGone,
                        message: "checkpoint diff endpoint no longer exists".into(),
                    }
                } else {
                    error
                }
            })
        };
        let from = resolve(&from)?;
        let to = resolve(&to)?;
        match detail {
            CheckpointDiffDetail::Summary => summary_diff(&from, &to, scope, binding, &page),
            CheckpointDiffDetail::Records => {
                record_diff(&from, &to, scope, binding, &page, self.lifecycle_mode)
            }
        }
    }

    fn resolve_selector(&self, selector: &CheckpointSelector) -> Result<DiffEndpoint, GfError> {
        let (checkpoint_uuid, generation) = match selector {
            CheckpointSelector::Named(name) => {
                let (row, generation) = graphforge_storage::open_checkpoint_generation_with_mode(
                    self.resolved_generation.container_root(),
                    name,
                    self.lifecycle_mode,
                )?;
                (row.checkpoint_uuid, generation)
            }
            CheckpointSelector::Current => {
                let generation = graphforge_storage::resolve_project_generation(
                    self.resolved_generation.container_root(),
                )?;
                (current_endpoint_uuid(&generation), generation)
            }
        };
        Ok(DiffEndpoint {
            checkpoint_uuid,
            generation,
        })
    }
}

struct DiffEndpoint {
    checkpoint_uuid: Uuid,
    generation: graphforge_storage::ResolvedProjectGeneration,
}

fn summary_diff(
    from: &DiffEndpoint,
    to: &DiffEndpoint,
    scope: CheckpointDiffScope,
    binding: Uuid,
    page: &PageRequest,
) -> Result<ExecutionResult, GfError> {
    let left = inventory(&from.generation, scope)?;
    let right = inventory(&to.generation, scope)?;
    let mut keys = left.keys().chain(right.keys()).cloned().collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    let snapshot = diff_endpoint_snapshot(from, to);
    let cursors = keys
        .iter()
        .map(|key| page_cursor(&[key.0.as_bytes(), key.1.as_bytes(), key.2.as_bytes()]))
        .collect::<Vec<_>>();
    let (start, end) = page_bounds("checkpoint-diff-summary", binding, page, snapshot, &cursors)?;
    let next = (end < keys.len()).then(|| {
        PageToken::new_bound(
            "checkpoint-diff-summary",
            binding,
            snapshot,
            page.limit,
            end,
            cursors[end - 1],
        )
    });
    summary_batch(
        from.checkpoint_uuid,
        to.checkpoint_uuid,
        &keys[start..end],
        &left,
        &right,
        next.as_ref(),
    )
}

#[derive(Clone)]
struct RecordAdapter {
    capability_version: u32,
    record_version: u32,
    encoding: &'static str,
    schema: SchemaRef,
    schema_fingerprint: [u8; 32],
    identity_fields: &'static [&'static str],
    record_uuid_field: Option<&'static str>,
    identity_fingerprint_domain: graphforge_core::canonical::CanonicalDomain,
    record_fingerprint_domain: graphforge_core::canonical::CanonicalDomain,
    max_rows: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LogicalRecord {
    record_uuid: Option<Uuid>,
    fingerprint: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordChange {
    scope: String,
    family: String,
    record_uuid: Option<Uuid>,
    identity: [u8; 32],
    kind: &'static str,
    from: Option<[u8; 32]>,
    to: Option<[u8; 32]>,
}

fn record_diff(
    from: &DiffEndpoint,
    to: &DiffEndpoint,
    scope: CheckpointDiffScope,
    binding: Uuid,
    page: &PageRequest,
    lifecycle_mode: graphforge_storage::filesystem_admission::ProjectLifecycleMode,
) -> Result<ExecutionResult, GfError> {
    let left = logical_records(&from.generation, scope, page, lifecycle_mode)?;
    let right = logical_records(&to.generation, scope, page, lifecycle_mode)?;
    let mut keys = left.keys().chain(right.keys()).cloned().collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    let mut changes = Vec::new();
    for key in keys {
        cancellation(page)?;
        let old = left.get(&key);
        let new = right.get(&key);
        let kind = match (old, new) {
            (None, Some(_)) => "added",
            (Some(_), None) => "removed",
            (Some(a), Some(b)) if a.fingerprint != b.fingerprint => "modified",
            _ => continue,
        };
        changes.push(RecordChange {
            scope: key.0,
            family: key.1,
            identity: key.2,
            record_uuid: old
                .and_then(|row| row.record_uuid)
                .or_else(|| new.and_then(|row| row.record_uuid)),
            kind,
            from: old.map(|row| row.fingerprint),
            to: new.map(|row| row.fingerprint),
        });
    }
    changes.sort_by(|a, b| {
        (&a.scope, &a.family, a.record_uuid, a.identity, a.kind).cmp(&(
            &b.scope,
            &b.family,
            b.record_uuid,
            b.identity,
            b.kind,
        ))
    });
    let snapshot = diff_endpoint_snapshot(from, to);
    let cursors = changes
        .iter()
        .map(|row| {
            let uuid = row.record_uuid.map_or([0; 16], |value| *value.as_bytes());
            page_cursor(&[
                row.scope.as_bytes(),
                row.family.as_bytes(),
                &uuid,
                &row.identity,
                row.kind.as_bytes(),
            ])
        })
        .collect::<Vec<_>>();
    let (start, end) = page_bounds("checkpoint-diff-records", binding, page, snapshot, &cursors)?;
    let next = (end < changes.len()).then(|| {
        PageToken::new_bound(
            "checkpoint-diff-records",
            binding,
            snapshot,
            page.limit,
            end,
            cursors[end - 1],
        )
    });
    record_batch(
        from.checkpoint_uuid,
        to.checkpoint_uuid,
        &changes[start..end],
        next.as_ref(),
    )
}

pub(super) type LogicalRecords = BTreeMap<(String, String, [u8; 32]), LogicalRecord>;

#[allow(clippy::too_many_lines)]
pub(super) fn logical_records(
    generation: &graphforge_storage::ResolvedProjectGeneration,
    scope: CheckpointDiffScope,
    page: &PageRequest,
    lifecycle_mode: graphforge_storage::filesystem_admission::ProjectLifecycleMode,
) -> Result<LogicalRecords, GfError> {
    let adapters = record_adapters()?;
    let mut out = BTreeMap::new();
    for descriptor in generation.participant_descriptors()? {
        let domain = participant_scope(&descriptor.capability_id, &descriptor.record_family_id);
        if !scope_matches(scope, domain) {
            continue;
        }
        cancellation(page)?;
        if descriptor.capability_id == "graph"
            && matches!(descriptor.record_family_id.as_str(), "snapshot" | "files")
        {
            let records = crate::checkpoint_graph_diff::extract_logical_graph_records_with_mode(
                generation,
                page.cancellation.as_ref(),
                lifecycle_mode,
            )?;
            for (family, records) in [("nodes", records.nodes), ("edges", records.edges)] {
                for record in records {
                    let identity: [u8; 32] = Sha256::digest(record.record_uuid.as_bytes()).into();
                    out.insert(
                        (domain.into(), family.into(), identity),
                        LogicalRecord {
                            record_uuid: Some(record.record_uuid),
                            fingerprint: record.fingerprint,
                        },
                    );
                }
            }
            continue;
        }
        if descriptor.capability_id == graphforge_storage::GRAPH_CAPABILITY_ID
            && descriptor.record_family_id == graphforge_storage::GRAPH_SEMANTIC_BINDINGS_FAMILY
        {
            let bindings = graphforge_storage::semantic_storage_bindings(generation)?
                .ok_or_else(|| schema_mismatch("semantic binding participant disappeared"))?;
            let bytes = bindings.to_canonical_json()?;
            let identity: [u8; 32] = Sha256::digest(b"graph:semantic_bindings").into();
            let fingerprint: [u8; 32] = Sha256::digest(bytes).into();
            out.insert(
                (domain.into(), descriptor.record_family_id.clone(), identity),
                LogicalRecord {
                    record_uuid: None,
                    fingerprint,
                },
            );
            continue;
        }
        if descriptor.capability_id == graphforge_storage::WORKSPACE_CAPABILITY_ID {
            if descriptor.record_family_id == "restoration_transition" {
                // Revert validation treats this canonical, storage-owned row as
                // the sole permitted delta from the checkpoint snapshot.
                continue;
            }
            let snapshot = generation
                .participant_snapshot(&descriptor.capability_id, &descriptor.record_family_id)?
                .ok_or_else(|| {
                    schema_mismatch("workspace participant disappeared during checkpoint diff")
                })?;
            match descriptor.record_family_id.as_str() {
                graphforge_storage::WORKSPACE_ONTOLOGY_FAMILY => {
                    graphforge_storage::WorkspaceOntology::from_canonical_json(&snapshot.bytes)?;
                }
                graphforge_storage::WORKSPACE_CONFIGURATION_FAMILY => {
                    graphforge_storage::WorkspaceConfiguration::from_canonical_json(
                        &snapshot.bytes,
                    )?;
                }
                graphforge_storage::WORKSPACE_ONTOLOGY_COMPOSITION_FAMILY => {
                    graphforge_storage::WorkspaceOntologyComposition::from_canonical_json(
                        &snapshot.bytes,
                    )?;
                }
                graphforge_storage::WORKSPACE_REPOSITORY_SNAPSHOT_FAMILY => {
                    graphforge_storage::WorkspaceRepositorySnapshot::from_canonical_json(
                        &snapshot.bytes,
                    )?;
                }
                _ => {
                    return Err(schema_mismatch(
                        "unregistered workspace checkpoint diff participant",
                    ));
                }
            }
            let identity: [u8; 32] =
                Sha256::digest(format!("workspace:{}", descriptor.record_family_id).as_bytes())
                    .into();
            let fingerprint: [u8; 32] = Sha256::digest(&snapshot.bytes).into();
            out.insert(
                (domain.into(), descriptor.record_family_id.clone(), identity),
                LogicalRecord {
                    record_uuid: None,
                    fingerprint,
                },
            );
            continue;
        }
        let key = (
            descriptor.capability_id.as_str(),
            descriptor.record_family_id.as_str(),
        );
        let adapter = adapters.get(&key).ok_or_else(|| {
            schema_mismatch(format!(
                "no logical checkpoint diff adapter for {}@{}",
                descriptor.capability_id, descriptor.record_family_id
            ))
        })?;
        if descriptor.encoding != adapter.encoding
            || descriptor.capability_version != adapter.capability_version
            || descriptor.record_version != adapter.record_version
            || descriptor.schema_fingerprint != adapter.schema_fingerprint
            || descriptor.row_count > adapter.max_rows as u64
        {
            return Err(schema_mismatch(format!(
                "checkpoint diff contract mismatch for {}@{}",
                descriptor.capability_id, descriptor.record_family_id
            )));
        }
        let snapshot = generation
            .participant_snapshot(&descriptor.capability_id, &descriptor.record_family_id)?
            .ok_or_else(|| {
                schema_mismatch("manifest participant disappeared during checkpoint diff")
            })?;
        let batches = read_parquet(&snapshot.bytes, page)?;
        let decoded_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let expected_rows = usize::try_from(descriptor.row_count).map_err(|_| {
            schema_mismatch("checkpoint participant row count exceeds this platform")
        })?;
        if decoded_rows != expected_rows {
            return Err(schema_mismatch(
                "checkpoint participant row count does not match its manifest",
            ));
        }
        for batch in batches {
            if batch.schema().fields() != adapter.schema.fields() {
                return Err(schema_mismatch(
                    "checkpoint participant Arrow schema is incompatible",
                ));
            }
            for row in 0..batch.num_rows() {
                if row % 4096 == 0 {
                    cancellation(page)?;
                }
                let identity_batch = project_row(&batch, row, adapter.identity_fields)?;
                let identity_payload =
                    crate::canonical_arrow::result_fingerprint(&[identity_batch])
                        .map_err(|error| GfError::Validation(error.to_string()))?;
                let identity = graphforge_core::canonical::fingerprint(
                    adapter.identity_fingerprint_domain,
                    graphforge_core::canonical::CANONICAL_CONTRACT_VERSION,
                    &identity_payload,
                )
                .map_err(|error| GfError::Validation(error.to_string()))?;
                let record = batch.slice(row, 1);
                let record_payload = crate::canonical_arrow::result_fingerprint(&[record])
                    .map_err(|error| GfError::Validation(error.to_string()))?;
                let fingerprint = graphforge_core::canonical::fingerprint(
                    adapter.record_fingerprint_domain,
                    graphforge_core::canonical::CANONICAL_CONTRACT_VERSION,
                    &record_payload,
                )
                .map_err(|error| GfError::Validation(error.to_string()))?;
                let record_uuid = adapter
                    .record_uuid_field
                    .map(|field| record_uuid(&batch, row, field))
                    .transpose()?;
                if out
                    .insert(
                        (domain.into(), descriptor.record_family_id.clone(), identity),
                        LogicalRecord {
                            record_uuid,
                            fingerprint,
                        },
                    )
                    .is_some()
                {
                    return Err(schema_mismatch(
                        "checkpoint participant has duplicate logical identity",
                    ));
                }
            }
        }
    }
    Ok(out)
}

fn record_adapters() -> Result<BTreeMap<(&'static str, &'static str), RecordAdapter>, GfError> {
    let mut out = BTreeMap::new();
    for entry in graphforge_knowledge::schema_registry() {
        if entry.diff_identity_fields.is_empty() {
            return Err(schema_mismatch(
                "checkpoint diff adapter has no identity fields",
            ));
        }
        let prior = out.insert(
            (entry.capability_id, entry.record_family),
            RecordAdapter {
                capability_version: entry.capability_version,
                record_version: entry.record_version,
                encoding: "parquet",
                schema: Arc::clone(&entry.schema),
                schema_fingerprint: entry.schema_fingerprint,
                identity_fields: entry.diff_identity_fields,
                record_uuid_field: entry.diff_record_uuid_field,
                identity_fingerprint_domain: entry.diff_identity_fingerprint_domain(),
                record_fingerprint_domain: entry.diff_record_fingerprint_domain(),
                max_rows: entry.max_rows,
            },
        );
        if prior.is_some() {
            return Err(schema_mismatch(
                "duplicate checkpoint diff adapter registration",
            ));
        }
    }
    for entry in graphforge_provenance::schema_registry() {
        if entry.diff_identity_fields.is_empty() {
            return Err(schema_mismatch(
                "checkpoint diff adapter has no identity fields",
            ));
        }
        let prior = out.insert(
            (entry.capability_id, entry.record_family),
            RecordAdapter {
                capability_version: entry.capability_version,
                record_version: entry.record_version,
                encoding: "parquet",
                schema: Arc::clone(&entry.schema),
                schema_fingerprint: entry.schema_fingerprint,
                identity_fields: entry.diff_identity_fields,
                record_uuid_field: entry.diff_record_uuid_field,
                identity_fingerprint_domain: entry.diff_identity_fingerprint_domain(),
                record_fingerprint_domain: entry.diff_record_fingerprint_domain(),
                max_rows: entry.max_rows,
            },
        );
        if prior.is_some() {
            return Err(schema_mismatch(
                "duplicate checkpoint diff adapter registration",
            ));
        }
    }
    Ok(out)
}

fn read_parquet(bytes: &[u8], page: &PageRequest) -> Result<Vec<RecordBatch>, GfError> {
    let file =
        tempfile::NamedTempFile::new().map_err(|error| GfError::Storage(error.to_string()))?;
    std::fs::write(file.path(), bytes).map_err(|error| GfError::Storage(error.to_string()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(
        file.reopen()
            .map_err(|error| GfError::Storage(error.to_string()))?,
    )
    .map_err(|error| GfError::Validation(format!("invalid checkpoint parquet: {error}")))?
    .with_batch_size(4096)
    .build()
    .map_err(|error| GfError::Validation(format!("invalid checkpoint parquet: {error}")))?;
    let mut batches = Vec::new();
    for batch in reader {
        cancellation(page)?;
        batches.push(batch.map_err(|error| {
            GfError::Validation(format!("invalid checkpoint parquet: {error}"))
        })?);
    }
    Ok(batches)
}

fn project_row(batch: &RecordBatch, row: usize, fields: &[&str]) -> Result<RecordBatch, GfError> {
    let mut projected_fields = Vec::with_capacity(fields.len());
    let mut columns = Vec::with_capacity(fields.len());
    for name in fields {
        let index = batch.schema().index_of(name).map_err(|_| GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: format!("checkpoint diff identity field {name} is absent"),
        })?;
        projected_fields.push(batch.schema().field(index).clone());
        columns.push(batch.column(index).slice(row, 1));
    }
    RecordBatch::try_new(Arc::new(Schema::new(projected_fields)), columns)
        .map_err(|error| GfError::Execution(error.to_string()))
}

fn record_uuid(batch: &RecordBatch, row: usize, field: &str) -> Result<Uuid, GfError> {
    let values = batch
        .column_by_name(field)
        .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: format!("checkpoint diff UUID field {field} is incompatible"),
        })?;
    if values.is_null(row) || values.value_length() != 16 {
        return Err(GfError::Api {
            code: ApiErrorCode::SchemaMismatch,
            message: format!("checkpoint diff UUID field {field} is invalid"),
        });
    }
    Uuid::from_slice(values.value(row)).map_err(|_| GfError::Api {
        code: ApiErrorCode::SchemaMismatch,
        message: format!("checkpoint diff UUID field {field} is invalid"),
    })
}

type Inventory =
    BTreeMap<(String, String, String), graphforge_storage::ProjectParticipantDescriptor>;

fn inventory(
    generation: &graphforge_storage::ResolvedProjectGeneration,
    scope: CheckpointDiffScope,
) -> Result<Inventory, GfError> {
    let mut out = BTreeMap::new();
    for row in generation.participant_descriptors()? {
        let domain = participant_scope(&row.capability_id, &row.record_family_id);
        if scope_matches(scope, domain) {
            out.insert(
                (
                    domain.into(),
                    row.capability_id.clone(),
                    row.record_family_id.clone(),
                ),
                row,
            );
        }
    }
    Ok(out)
}

pub(super) fn participant_scope(capability: &str, family: &str) -> &'static str {
    match capability {
        "graph" => "graph",
        "ontology" => "ontology",
        "provenance" => "provenance",
        "knowledge" => "knowledge",
        "epistemic" | "valid_time" => "epistemic",
        "workspace" if family == "ontology" => "ontology",
        "workspace" if family == "configuration" => "configuration",
        _ => "capabilities",
    }
}
pub(super) fn scope_matches(requested: CheckpointDiffScope, actual: &str) -> bool {
    matches!(
        requested,
        CheckpointDiffScope::Summary | CheckpointDiffScope::All
    ) || matches!(
        (requested, actual),
        (CheckpointDiffScope::Graph, "graph")
            | (CheckpointDiffScope::Ontology, "ontology")
            | (CheckpointDiffScope::Configuration, "configuration")
            | (CheckpointDiffScope::Capabilities, "capabilities")
            | (CheckpointDiffScope::Provenance, "provenance")
            | (CheckpointDiffScope::Knowledge, "knowledge")
            | (CheckpointDiffScope::Epistemic, "epistemic")
    )
}

fn current_endpoint_uuid(g: &graphforge_storage::ResolvedProjectGeneration) -> Uuid {
    let mut h = Sha256::new();
    h.update(b"graphforge-current-checkpoint-endpoint/1");
    h.update(g.generation_uuid().as_bytes());
    h.update(g.manifest_sha256());
    graphforge_core::canonical::uuid_v8(h.finalize().into())
}
fn diff_endpoint_snapshot(from: &DiffEndpoint, to: &DiffEndpoint) -> Uuid {
    let mut h = Sha256::new();
    h.update(b"graphforge-checkpoint-diff-page/1");
    h.update(from.checkpoint_uuid.as_bytes());
    h.update(from.generation.manifest_sha256());
    h.update(to.checkpoint_uuid.as_bytes());
    h.update(to.generation.manifest_sha256());
    graphforge_core::canonical::uuid_v8(h.finalize().into())
}

fn diff_request_binding_parts(
    from: &CheckpointSelector,
    to: &CheckpointSelector,
    scope: CheckpointDiffScope,
    detail: CheckpointDiffDetail,
) -> Uuid {
    let mut h = Sha256::new();
    h.update(b"graphforge-checkpoint-diff-request/1");
    for selector in [from, to] {
        match selector {
            CheckpointSelector::Named(name) => {
                h.update([0]);
                h.update((name.len() as u64).to_be_bytes());
                h.update(name.as_bytes());
            }
            CheckpointSelector::Current => h.update([1]),
        }
    }
    h.update([scope as u8, detail as u8]);
    graphforge_core::canonical::uuid_v8(h.finalize().into())
}

fn summary_batch(
    from_id: Uuid,
    to_id: Uuid,
    keys: &[(String, String, String)],
    left: &Inventory,
    right: &Inventory,
    next: Option<&PageToken>,
) -> Result<ExecutionResult, GfError> {
    let row_count = keys.len();
    let mut from = FixedSizeBinaryBuilder::with_capacity(row_count, 16);
    let mut to = FixedSizeBinaryBuilder::with_capacity(row_count, 16);
    let mut scopes = StringBuilder::new();
    let mut caps = StringBuilder::new();
    let mut families = StringBuilder::new();
    let mut kinds = StringBuilder::new();
    let mut lrows = UInt64Builder::new();
    let mut rrows = UInt64Builder::new();
    let mut lschema = FixedSizeBinaryBuilder::with_capacity(row_count, 32);
    let mut rschema = FixedSizeBinaryBuilder::with_capacity(row_count, 32);
    let mut lcontent = FixedSizeBinaryBuilder::with_capacity(row_count, 32);
    let mut rcontent = FixedSizeBinaryBuilder::with_capacity(row_count, 32);
    for key in keys {
        let left_row = left.get(key);
        let right_row = right.get(key);
        from.append_value(from_id.as_bytes()).map_err(arrow_error)?;
        to.append_value(to_id.as_bytes()).map_err(arrow_error)?;
        scopes.append_value(&key.0);
        caps.append_value(&key.1);
        families.append_value(&key.2);
        let kind = match (left_row, right_row) {
            (None, Some(_)) => "added",
            (Some(_), None) => "removed",
            (Some(left_value), Some(right_value)) if left_value == right_value => "unchanged",
            _ => "modified",
        };
        kinds.append_value(kind);
        append_u64(&mut lrows, left_row.map(|value| value.row_count));
        append_u64(&mut rrows, right_row.map(|value| value.row_count));
        append_fixed(
            &mut lschema,
            left_row.map(|value| &value.schema_fingerprint),
        )
        .map_err(arrow_error)?;
        append_fixed(
            &mut rschema,
            right_row.map(|value| &value.schema_fingerprint),
        )
        .map_err(arrow_error)?;
        append_fixed(&mut lcontent, left_row.map(|value| &value.content_sha256))
            .map_err(arrow_error)?;
        append_fixed(&mut rcontent, right_row.map(|value| &value.content_sha256))
            .map_err(arrow_error)?;
    }
    let fields = vec![
        Field::new("from_checkpoint_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("to_checkpoint_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("scope", DataType::Utf8, false),
        Field::new("capability_id", DataType::Utf8, false),
        Field::new("record_family_id", DataType::Utf8, false),
        Field::new("change_kind", DataType::Utf8, false),
        Field::new("from_row_count", DataType::UInt64, true),
        Field::new("to_row_count", DataType::UInt64, true),
        Field::new(
            "from_schema_fingerprint",
            DataType::FixedSizeBinary(32),
            true,
        ),
        Field::new("to_schema_fingerprint", DataType::FixedSizeBinary(32), true),
        Field::new("from_content_sha256", DataType::FixedSizeBinary(32), true),
        Field::new("to_content_sha256", DataType::FixedSizeBinary(32), true),
    ];
    Ok(execution(make_batch(
        "checkpoint_summary_diff",
        fields,
        vec![
            Arc::new(from.finish()),
            Arc::new(to.finish()),
            Arc::new(scopes.finish()),
            Arc::new(caps.finish()),
            Arc::new(families.finish()),
            Arc::new(kinds.finish()),
            Arc::new(lrows.finish()),
            Arc::new(rrows.finish()),
            Arc::new(lschema.finish()),
            Arc::new(rschema.finish()),
            Arc::new(lcontent.finish()),
            Arc::new(rcontent.finish()),
        ],
        next,
    )?))
}

fn record_batch(
    from_id: Uuid,
    to_id: Uuid,
    rows: &[RecordChange],
    next: Option<&PageToken>,
) -> Result<ExecutionResult, GfError> {
    let n = rows.len();
    let mut from_checkpoint = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut to_checkpoint = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut scopes = StringBuilder::new();
    let mut families = StringBuilder::new();
    let mut uuids = FixedSizeBinaryBuilder::with_capacity(n, 16);
    let mut identities = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut kinds = StringBuilder::new();
    let mut from_fingerprints = FixedSizeBinaryBuilder::with_capacity(n, 32);
    let mut to_fingerprints = FixedSizeBinaryBuilder::with_capacity(n, 32);
    for row in rows {
        from_checkpoint
            .append_value(from_id.as_bytes())
            .map_err(arrow_error)?;
        to_checkpoint
            .append_value(to_id.as_bytes())
            .map_err(arrow_error)?;
        scopes.append_value(&row.scope);
        families.append_value(&row.family);
        match row.record_uuid {
            Some(value) => uuids.append_value(value.as_bytes()).map_err(arrow_error)?,
            None => uuids.append_null(),
        }
        identities.append_value(row.identity).map_err(arrow_error)?;
        kinds.append_value(row.kind);
        append_fixed(&mut from_fingerprints, row.from.as_ref()).map_err(arrow_error)?;
        append_fixed(&mut to_fingerprints, row.to.as_ref()).map_err(arrow_error)?;
    }
    let fields = vec![
        Field::new("from_checkpoint_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("to_checkpoint_uuid", DataType::FixedSizeBinary(16), false),
        Field::new("scope", DataType::Utf8, false),
        Field::new("record_family_id", DataType::Utf8, false),
        Field::new("record_uuid", DataType::FixedSizeBinary(16), true),
        Field::new(
            "record_identity_fingerprint",
            DataType::FixedSizeBinary(32),
            false,
        ),
        Field::new("change_kind", DataType::Utf8, false),
        Field::new(
            "from_record_fingerprint",
            DataType::FixedSizeBinary(32),
            true,
        ),
        Field::new("to_record_fingerprint", DataType::FixedSizeBinary(32), true),
    ];
    Ok(execution(make_batch(
        "checkpoint_record_diff",
        fields,
        vec![
            Arc::new(from_checkpoint.finish()),
            Arc::new(to_checkpoint.finish()),
            Arc::new(scopes.finish()),
            Arc::new(families.finish()),
            Arc::new(uuids.finish()),
            Arc::new(identities.finish()),
            Arc::new(kinds.finish()),
            Arc::new(from_fingerprints.finish()),
            Arc::new(to_fingerprints.finish()),
        ],
        next,
    )?))
}
fn append_u64(b: &mut UInt64Builder, v: Option<u64>) {
    match v {
        Some(v) => b.append_value(v),
        None => b.append_null(),
    }
}
fn append_fixed(
    b: &mut FixedSizeBinaryBuilder,
    v: Option<&[u8; 32]>,
) -> Result<(), arrow::error::ArrowError> {
    if let Some(v) = v {
        b.append_value(v)?;
    } else {
        b.append_null();
    }
    Ok(())
}

#[cfg(test)]
mod tests;
