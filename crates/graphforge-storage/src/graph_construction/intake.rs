//! Intake for graph construction.

use super::{
    Array, ArrowWriter, ArtifactReceipt, BLOCK_BYTES, BufWriter, CONSTRUCTION_EDGE_SCHEMA,
    CONSTRUCTION_NODE_SCHEMA, Checkpoint, ChunkIntent, ConstructionChunkKind,
    ConstructionChunkReceipt, CountingChunkReader, Deserialize, DetailCodec, Digest,
    EDGE_DETAIL_WIDTH, ENDPOINT_WIDTH, FixedSizeBinaryArray, GfError, GraphConstructionBudgets,
    GraphConstructionEvidence, GraphConstructionSession, GraphConstructionState, HashingWriter,
    IDENTITY_WIDTH, INTENT, IdentityRecord, IoCounter, NODE_DETAIL_WIDTH, Ordering, OsStr,
    ParquetRecordBatchReaderBuilder, ReadWork, RecordBatch, Schema, Serialize, Sha256,
    StableDirectory, StringArray, UInt32Array, Uuid, Write, account_cache_release, artifact_temp,
    authenticate_artifact, combine_cache_cleanup, construction_failpoint, decode_bounded,
    file_identity, hex, install_control, is_canonical_lower_hex, is_canonical_sha256,
    merge_cache_release_evidence, persist_shape_receipt, record_active_identity_install,
    record_category_install, reject_cancelled, replace_checkpoint_control, replace_control,
    run_record_bytes, sha256, storage, take, unlink_named,
};

impl GraphConstructionSession {
    /// Append one canonical bounded Arrow chunk.
    pub fn append(
        &mut self,
        kind: ConstructionChunkKind,
        chunk_id: &str,
        batch: &RecordBatch,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.append_with_cancellation(kind, chunk_id, batch, || false)
    }

    /// Append while polling a caller-owned cancellation signal at durable
    /// artifact boundaries. Cancellation leaves an intent that the next open
    /// authenticates and rolls back without changing public authority.
    #[allow(clippy::too_many_lines)]
    pub fn append_with_cancellation(
        &mut self,
        kind: ConstructionChunkKind,
        chunk_id: &str,
        batch: &RecordBatch,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<ConstructionChunkReceipt, GfError> {
        self.revalidate_authority()?;
        self.recover_intent()?;
        reject_cancelled(&mut cancelled)?;
        if self.checkpoint.state != GraphConstructionState::Staging {
            return Err(storage("session is not accepting chunks"));
        }
        validate_chunk_id(chunk_id)?;
        validate_schema(kind, batch)?;
        if batch.num_rows() == 0 {
            return Err(storage("empty construction chunk"));
        }
        let input_bytes = batch.get_array_memory_size();
        let required_columns = if kind == ConstructionChunkKind::Node {
            2
        } else {
            4
        };
        if batch.num_columns().saturating_sub(required_columns)
            > self.checkpoint.budgets.max_property_columns
        {
            return Err(storage("construction property-column budget exhausted"));
        }
        if batch.num_rows() > self.checkpoint.budgets.max_batch_rows
            || input_bytes > self.checkpoint.budgets.max_batch_bytes
            || self.checkpoint.next_sequence >= self.checkpoint.budgets.max_chunks
        {
            return Err(storage("construction resource window exhausted"));
        }
        if kind == ConstructionChunkKind::Node && self.checkpoint.saw_edge {
            return Err(storage("node chunk cannot follow edge staging"));
        }
        let input_sha256 = logical_batch_digest(kind, batch)?;
        let schema_sha256 = normalized_schema_digest(batch.schema().as_ref());
        let key_name = chunk_key_name(chunk_id);
        if let Ok(mut key_file) = self.root.open_child_file(OsStr::new(&key_name)) {
            let pointer: ReceiptPointer = decode_bounded(&mut key_file)?;
            let receipt = self.read_receipt(pointer.sequence)?;
            let receipt_body = serde_json::to_vec(&receipt).map_err(storage)?;
            if pointer.operation_uuid == self.checkpoint.operation_uuid
                && pointer.project_identity == self.checkpoint.project_identity
                && pointer.session_identity == self.checkpoint.session_identity
                && pointer.receipt_sha256 == sha256(&receipt_body)
                && receipt.chunk_id == chunk_id
                && receipt.kind == kind
                && receipt.rows == batch.num_rows() as u64
                && receipt.input_sha256 == input_sha256
                && receipt.schema_sha256 == schema_sha256
            {
                let work = validate_receipt_artifacts(
                    &self.root,
                    &receipt,
                    DetailCodec::from_version(self.checkpoint.format_version).map_err(storage)?,
                )?;
                self.checkpoint.evidence.replay_validation_read_bytes = self
                    .checkpoint
                    .evidence
                    .replay_validation_read_bytes
                    .checked_add(work.bytes)
                    .ok_or_else(|| storage("replay validation byte count overflows"))?;
                self.checkpoint.evidence.replay_validation_read_operations = self
                    .checkpoint
                    .evidence
                    .replay_validation_read_operations
                    .checked_add(work.operations)
                    .ok_or_else(|| storage("replay validation operation count overflows"))?;
                self.checkpoint.evidence.replayed_chunks = self
                    .checkpoint
                    .evidence
                    .replayed_chunks
                    .checked_add(1)
                    .ok_or_else(|| storage("replayed chunk count overflows"))?;
                replace_checkpoint_control(&self.root, &self.checkpoint)?;
                return Ok(receipt);
            }
            return Err(storage("conflicting construction chunk replay"));
        }
        let (known_schemas, other_schemas) = match kind {
            ConstructionChunkKind::Node => (
                &self.checkpoint.node_schema_sha256,
                &self.checkpoint.edge_schema_sha256,
            ),
            ConstructionChunkKind::Edge => (
                &self.checkpoint.edge_schema_sha256,
                &self.checkpoint.node_schema_sha256,
            ),
        };
        let schema_groups = known_schemas
            .len()
            .checked_add(other_schemas.len())
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| storage("construction schema-group count overflow"))?;
        if !known_schemas.contains(&schema_sha256)
            && schema_groups > self.checkpoint.budgets.max_schema_groups
        {
            return Err(storage("construction schema-group budget exhausted"));
        }
        let arrays = extract_runs(kind, batch)?;
        let run_records = arrays
            .identities
            .len()
            .checked_add(arrays.endpoints.len())
            .and_then(|count| count.checked_add(batch.num_rows()))
            .ok_or_else(|| storage("construction run record count overflow"))?;
        if run_records > self.checkpoint.budgets.max_run_records {
            return Err(storage("construction run window exhausted"));
        }
        let sequence = self.checkpoint.next_sequence;
        let mut intent = ChunkIntent {
            format_version: self.checkpoint.format_version,
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            sequence,
            chunk_id: chunk_id.to_owned(),
            chunk_key: key_name,
            kind,
            rows: batch.num_rows() as u64,
            input_bytes: input_bytes as u64,
            input_sha256,
            schema_sha256,
            parent_topology_generation: self.checkpoint.parent_topology_generation,
            ontology_mode: self.checkpoint.ontology_mode,
            semantic_authority_sha256: self.checkpoint.semantic_authority_sha256.clone(),
            prior_receipt_sha256: self.checkpoint.last_receipt_sha256.clone(),
            run_records: run_records as u64,
            accounted_live_bytes: 0,
            parquet: None,
            identities: None,
            endpoints: None,
            details: None,
        };
        install_control(&self.root, INTENT, &intent)?;
        reject_cancelled(&mut cancelled)?;
        let stem = artifact_stem(sequence, kind);
        let sorted_batch = uuid_sorted_batch(kind, batch)?;
        let fixed_bytes = arrays
            .identities
            .len()
            .checked_mul(IDENTITY_WIDTH)
            .and_then(|bytes| {
                arrays
                    .endpoints
                    .len()
                    .checked_mul(ENDPOINT_WIDTH)
                    .and_then(|endpoint_bytes| bytes.checked_add(endpoint_bytes))
            })
            .and_then(|bytes| {
                let detail_bytes = match &arrays.details {
                    DetailRuns::Node(records) => records.len().checked_mul(NODE_DETAIL_WIDTH),
                    DetailRuns::Edge(records) => records.len().checked_mul(EDGE_DETAIL_WIDTH),
                };
                detail_bytes.and_then(|detail_bytes| bytes.checked_add(detail_bytes))
            })
            .ok_or_else(|| storage("construction fixed-width byte count overflow"))?;
        let sorted_bytes = sorted_batch.get_array_memory_size();
        intent.accounted_live_bytes = u64::try_from(
            input_bytes
                .checked_add(fixed_bytes)
                .and_then(|bytes| {
                    sorted_bytes
                        .checked_mul(2)
                        .and_then(|sorted| bytes.checked_add(sorted))
                })
                .and_then(|bytes| {
                    BLOCK_BYTES
                        .checked_mul(2)
                        .and_then(|blocks| bytes.checked_add(blocks))
                })
                .ok_or_else(|| storage("construction live-byte count overflow"))?,
        )
        .map_err(storage)?;
        replace_control(&self.root, INTENT, &intent)?;
        intent.parquet = Some(write_parquet(
            &self.root,
            &format!("{stem}.parquet"),
            &sorted_batch,
            &mut self.checkpoint.evidence,
        )?);
        replace_control(&self.root, INTENT, &intent)?;
        reject_cancelled(&mut cancelled)?;
        intent.identities = Some(write_fixed_run(
            &self.root,
            &format!("{stem}.identities.run"),
            &arrays.identities,
            &mut self.checkpoint.evidence,
        )?);
        replace_control(&self.root, INTENT, &intent)?;
        reject_cancelled(&mut cancelled)?;
        if !arrays.endpoints.is_empty() {
            intent.endpoints = Some(write_fixed_run(
                &self.root,
                &format!("{stem}.endpoints.run"),
                &arrays.endpoints,
                &mut self.checkpoint.evidence,
            )?);
            replace_control(&self.root, INTENT, &intent)?;
            reject_cancelled(&mut cancelled)?;
        }
        intent.details = Some(match &arrays.details {
            DetailRuns::Node(records) => write_run(
                &self.root,
                &format!("{stem}.node-details.run"),
                records,
                &mut self.checkpoint.evidence,
                Some(DetailCodec::from_version(self.checkpoint.format_version).map_err(storage)?),
            )?,
            DetailRuns::Edge(records) => write_run(
                &self.root,
                &format!("{stem}.edge-details.run"),
                records,
                &mut self.checkpoint.evidence,
                Some(DetailCodec::from_version(self.checkpoint.format_version).map_err(storage)?),
            )?,
        });
        replace_control(&self.root, INTENT, &intent)?;
        reject_cancelled(&mut cancelled)?;
        let receipt = receipt_from_intent(&intent)?;
        let receipt_name = receipt_name(sequence);
        install_control(&self.root, &receipt_name, &receipt)?;
        let receipt_bytes = serde_json::to_vec(&receipt).map_err(storage)?;
        let pointer = ReceiptPointer {
            operation_uuid: self.checkpoint.operation_uuid,
            project_identity: self.checkpoint.project_identity.clone(),
            session_identity: self.checkpoint.session_identity.clone(),
            sequence,
            receipt_sha256: sha256(&receipt_bytes),
        };
        install_control(&self.root, &intent.chunk_key, &pointer)?;
        self.advance_checkpoint(&receipt, &receipt_bytes)?;
        unlink_named(&self.root, INTENT)?;
        Ok(receipt)
    }

    pub(super) fn read_receipt(&self, sequence: u64) -> Result<ConstructionChunkReceipt, GfError> {
        let mut file = self
            .root
            .open_child_file(OsStr::new(&receipt_name(sequence)))
            .map_err(storage)?;
        let receipt = decode_bounded(&mut file)?;
        validate_receipt_semantics(
            &receipt,
            sequence,
            self.checkpoint.budgets,
            DetailCodec::from_version(self.checkpoint.format_version).map_err(storage)?,
        )?;
        if receipt.operation_uuid != self.checkpoint.operation_uuid
            || receipt.project_identity != self.checkpoint.project_identity
            || receipt.session_identity != self.checkpoint.session_identity
            || receipt.parent_topology_generation != self.checkpoint.parent_topology_generation
            || receipt.ontology_mode != self.checkpoint.ontology_mode
            || receipt.semantic_authority_sha256 != self.checkpoint.semantic_authority_sha256
        {
            return Err(storage("receipt authority differs from session checkpoint"));
        }
        Ok(receipt)
    }

    pub(super) fn advance_checkpoint(
        &mut self,
        receipt: &ConstructionChunkReceipt,
        receipt_bytes: &[u8],
    ) -> Result<(), GfError> {
        if receipt.sequence < self.checkpoint.next_sequence {
            return Ok(());
        }
        if receipt.sequence != self.checkpoint.next_sequence {
            return Err(storage("receipt sequence is not checkpoint successor"));
        }
        let evidence = &mut self.checkpoint.evidence;
        evidence.input_rows = evidence
            .input_rows
            .checked_add(receipt.rows)
            .ok_or_else(|| storage("construction input row count overflows"))?;
        evidence.input_batches = evidence
            .input_batches
            .checked_add(1)
            .ok_or_else(|| storage("construction input batch count overflows"))?;
        evidence.parquet_shards = evidence
            .parquet_shards
            .checked_add(1)
            .ok_or_else(|| storage("construction Parquet shard count overflows"))?;
        evidence.run_records = evidence
            .run_records
            .checked_add(receipt.run_records)
            .ok_or_else(|| storage("construction run record count overflows"))?;
        evidence.peak_batch_rows = evidence.peak_batch_rows.max(receipt.rows);
        evidence.peak_batch_bytes = evidence.peak_batch_bytes.max(receipt.input_bytes);
        evidence.peak_run_records = evidence.peak_run_records.max(receipt.run_records);
        evidence.peak_accounted_live_bytes = evidence
            .peak_accounted_live_bytes
            .max(receipt.accounted_live_bytes);
        // Chunk receipts describe private construction inputs. They are not
        // canonical topology until shaping, encoding, and generation-last
        // publication succeed, so attribution must keep them in staging.
        for artifact in [&receipt.parquet, &receipt.identities, &receipt.details]
            .into_iter()
            .chain(receipt.endpoints.iter())
        {
            evidence.immutable_artifacts = evidence
                .immutable_artifacts
                .checked_add(1)
                .ok_or_else(|| storage("construction artifact count overflows"))?;
            let identity_key = format!(
                "{:016x}:{}",
                artifact.identity.volume_serial, artifact.identity.file_id
            );
            record_active_identity_install(
                evidence,
                identity_key,
                artifact.allocated_bytes,
                "construction artifact identity was already active",
            )?;
            evidence.write_bytes = evidence
                .write_bytes
                .checked_add(artifact.bytes)
                .ok_or_else(|| storage("construction write byte count overflows"))?;
            evidence.write_operations = evidence
                .write_operations
                .checked_add(artifact.write_operations)
                .ok_or_else(|| storage("construction write operation count overflows"))?;
            evidence.fsync_operations = evidence
                .fsync_operations
                .checked_add(artifact.fsync_operations)
                .ok_or_else(|| storage("construction fsync count overflows"))?;
            record_category_install(evidence, artifact.bytes, artifact.allocated_bytes)?;
        }
        self.checkpoint.next_sequence = self
            .checkpoint
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| storage("construction sequence overflows"))?;
        self.checkpoint.saw_edge |= receipt.kind == ConstructionChunkKind::Edge;
        match receipt.kind {
            ConstructionChunkKind::Node => {
                self.checkpoint
                    .node_schema_sha256
                    .insert(receipt.schema_sha256.clone());
            }
            ConstructionChunkKind::Edge => {
                self.checkpoint
                    .edge_schema_sha256
                    .insert(receipt.schema_sha256.clone());
            }
        }
        self.checkpoint.last_receipt_sha256 = Some(sha256(receipt_bytes));
        replace_checkpoint_control(&self.root, &self.checkpoint)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct ReceiptPointer {
    pub(super) operation_uuid: Uuid,
    pub(super) project_identity: IdentityRecord,
    pub(super) session_identity: IdentityRecord,
    pub(super) sequence: u64,
    pub(super) receipt_sha256: String,
}

struct RunArrays {
    identities: Vec<[u8; IDENTITY_WIDTH]>,
    endpoints: Vec<[u8; ENDPOINT_WIDTH]>,
    details: DetailRuns,
}

enum DetailRuns {
    Node(Vec<[u8; NODE_DETAIL_WIDTH]>),
    Edge(Vec<[u8; EDGE_DETAIL_WIDTH]>),
}

fn extract_runs(kind: ConstructionChunkKind, batch: &RecordBatch) -> Result<RunArrays, GfError> {
    let identity = uuid_column(
        batch,
        if kind == ConstructionChunkKind::Node {
            "node_uuid"
        } else {
            "edge_uuid"
        },
    )?;
    let mut identities = (0..batch.num_rows())
        .map(|row| uuid_value(identity, row))
        .collect::<Result<Vec<_>, _>>()?;
    identities.sort_unstable();
    if identities.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(storage("duplicate identity inside chunk"));
    }
    let mut endpoints = Vec::new();
    let details = if kind == ConstructionChunkKind::Edge {
        let edges = uuid_column(batch, "edge_uuid")?;
        let src = uuid_column(batch, "source_uuid")?;
        let dst = uuid_column(batch, "target_uuid")?;
        let routes = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| storage("canonical edge route is not Utf8"))?;
        endpoints.reserve(
            batch
                .num_rows()
                .checked_mul(2)
                .ok_or_else(|| storage("edge endpoint capacity overflow"))?,
        );
        let mut details = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let edge = uuid_value(edges, row)?;
            for (role, endpoint) in [uuid_value(src, row)?, uuid_value(dst, row)?]
                .into_iter()
                .enumerate()
            {
                let mut record = [0_u8; ENDPOINT_WIDTH];
                record[..16].copy_from_slice(&endpoint);
                record[16..32].copy_from_slice(&edge);
                record[32] = u8::try_from(role).expect("endpoint role is zero or one");
                endpoints.push(record);
            }
            let route = routes.value(row).as_bytes();
            let mut detail = [0_u8; EDGE_DETAIL_WIDTH];
            detail[..16].copy_from_slice(&edge);
            detail[16..32].copy_from_slice(&uuid_value(src, row)?);
            detail[32..48].copy_from_slice(&uuid_value(dst, row)?);
            detail[48] = u8::try_from(route.len())
                .map_err(|_| storage("canonical edge route exceeds identifier bound"))?;
            detail[49..49 + route.len()].copy_from_slice(route);
            details.push(detail);
        }
        endpoints.sort_unstable();
        details.sort_unstable();
        DetailRuns::Edge(details)
    } else {
        let nodes = uuid_column(batch, "node_uuid")?;
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| storage("canonical node label is not Utf8"))?;
        let mut details = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let mut detail = [0_u8; NODE_DETAIL_WIDTH];
            detail[..16].copy_from_slice(&uuid_value(nodes, row)?);
            let label = labels.value(row).as_bytes();
            detail[16] = u8::try_from(label.len())
                .map_err(|_| storage("canonical node label exceeds identifier bound"))?;
            detail[17..17 + label.len()].copy_from_slice(label);
            details.push(detail);
        }
        details.sort_unstable();
        DetailRuns::Node(details)
    };
    Ok(RunArrays {
        identities,
        endpoints,
        details,
    })
}

fn validate_schema(kind: ConstructionChunkKind, batch: &RecordBatch) -> Result<(), GfError> {
    let expected = match kind {
        ConstructionChunkKind::Node => &*CONSTRUCTION_NODE_SCHEMA,
        ConstructionChunkKind::Edge => &*CONSTRUCTION_EDGE_SCHEMA,
    };
    if batch.num_columns() < expected.fields().len()
        || batch.schema().fields()[..expected.fields().len()] != expected.fields()[..]
        || batch.schema().fields()[expected.fields().len()..]
            .iter()
            .any(|field| {
                matches!(
                    field.name().as_str(),
                    "node_uuid"
                        | "label"
                        | "edge_uuid"
                        | "rel_type"
                        | "source_uuid"
                        | "target_uuid"
                ) || !graphforge_core::identifier::is_graph_identifier(field.name())
            })
    {
        return Err(storage("construction batch schema is not canonical"));
    }
    for column in &batch.columns()[..expected.fields().len()] {
        if column.null_count() != 0 {
            return Err(storage("required construction columns are non-null"));
        }
    }
    if batch.schema().fields()[expected.fields().len()..]
        .iter()
        .any(|field| !crate::schemas::property_data_type_supported(field.data_type()))
    {
        return Err(storage("unsupported construction property type"));
    }
    let identifiers = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| storage("canonical label or relation is not Utf8"))?;
    if identifiers
        .iter()
        .flatten()
        .any(|value| !is_construction_identifier(value))
    {
        return Err(storage("invalid canonical label or relation"));
    }
    Ok(())
}

fn is_construction_identifier(value: &str) -> bool {
    if graphforge_core::identifier::is_graph_identifier(value) {
        return true;
    }
    let parts = value.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        [module, local] => {
            graphforge_core::identifier::is_graph_identifier(module)
                && graphforge_core::identifier::is_graph_identifier(local)
        }
        [module, kind, local] => {
            graphforge_core::identifier::is_graph_identifier(module)
                && matches!(*kind, "entity" | "relation")
                && graphforge_core::identifier::is_graph_identifier(local)
        }
        _ => false,
    }
}

fn logical_batch_digest(
    kind: ConstructionChunkKind,
    batch: &RecordBatch,
) -> Result<String, GfError> {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-construction-logical-arrow/v1\0");
    digest.update(kind.tag().as_bytes());
    digest.update((batch.num_rows() as u64).to_be_bytes());
    match kind {
        ConstructionChunkKind::Node => {
            let uuids = uuid_column(batch, "node_uuid")?;
            let labels = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| storage("canonical node label is not Utf8"))?;
            for row in 0..batch.num_rows() {
                digest.update(uuid_value(uuids, row)?);
                let label = labels.value(row).as_bytes();
                digest.update((label.len() as u64).to_be_bytes());
                digest.update(label);
            }
        }
        ConstructionChunkKind::Edge => {
            let edge = uuid_column(batch, "edge_uuid")?;
            let src = uuid_column(batch, "source_uuid")?;
            let dst = uuid_column(batch, "target_uuid")?;
            let route = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| storage("canonical edge route is not Utf8"))?;
            for row in 0..batch.num_rows() {
                digest.update(uuid_value(edge, row)?);
                digest.update(uuid_value(src, row)?);
                digest.update(uuid_value(dst, row)?);
                let value = route.value(row).as_bytes();
                digest.update((value.len() as u64).to_be_bytes());
                digest.update(value);
            }
        }
    }
    let required = if kind == ConstructionChunkKind::Node {
        2
    } else {
        4
    };
    let schema = batch.schema();
    for (field, column) in schema.fields()[required..]
        .iter()
        .zip(expected_property_columns(kind, batch))
    {
        digest.update((field.name().len() as u64).to_be_bytes());
        digest.update(field.name().as_bytes());
        digest.update(column.data_type().to_string().as_bytes());
        for row in 0..column.len() {
            if column.is_null(row) {
                digest.update([0]);
            } else {
                digest.update([1]);
                let value = arrow::util::display::array_value_to_string(column.as_ref(), row)
                    .map_err(storage)?;
                digest.update((value.len() as u64).to_be_bytes());
                digest.update(value.as_bytes());
            }
        }
    }
    Ok(hex(&digest.finalize()))
}

fn normalized_schema_digest(schema: &Schema) -> String {
    let mut digest = Sha256::new();
    for field in schema.fields() {
        digest.update((field.name().len() as u64).to_be_bytes());
        digest.update(field.name().as_bytes());
        let data_type = format!("{:?}", field.data_type());
        digest.update((data_type.len() as u64).to_be_bytes());
        digest.update(data_type.as_bytes());
        digest.update([u8::from(field.is_nullable())]);
        let mut metadata = field.metadata().iter().collect::<Vec<_>>();
        metadata.sort_unstable();
        for (key, value) in metadata {
            digest.update((key.len() as u64).to_be_bytes());
            digest.update(key.as_bytes());
            digest.update((value.len() as u64).to_be_bytes());
            digest.update(value.as_bytes());
        }
    }
    let mut metadata = schema.metadata().iter().collect::<Vec<_>>();
    metadata.sort_unstable();
    for (key, value) in metadata {
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key.as_bytes());
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    hex(&digest.finalize())
}

fn uuid_sorted_batch(
    kind: ConstructionChunkKind,
    batch: &RecordBatch,
) -> Result<RecordBatch, GfError> {
    let identity = uuid_column(
        batch,
        if kind == ConstructionChunkKind::Node {
            "node_uuid"
        } else {
            "edge_uuid"
        },
    )?;
    let mut order = (0..batch.num_rows()).collect::<Vec<_>>();
    order.sort_unstable_by_key(|&row| identity.value(row));
    let indices = UInt32Array::from(
        order
            .into_iter()
            .map(|row| u32::try_from(row).map_err(|_| storage("row index exceeds UInt32")))
            .collect::<Result<Vec<_>, _>>()?,
    );
    let columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None).map_err(storage))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(storage)
}

fn expected_property_columns(
    kind: ConstructionChunkKind,
    batch: &RecordBatch,
) -> impl Iterator<Item = &arrow::array::ArrayRef> {
    let required = if kind == ConstructionChunkKind::Node {
        2
    } else {
        4
    };
    batch.columns()[required..].iter()
}

pub(super) fn uuid_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a FixedSizeBinaryArray, GfError> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| storage(format!("missing canonical column {name}")))?;
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| storage(format!("{name} is not FixedSizeBinary(16)")))?;
    if array.value_length() != 16 || array.null_count() != 0 {
        return Err(storage(format!("{name} is not non-null UUID data")));
    }
    Ok(array)
}

pub(super) fn uuid_value(array: &FixedSizeBinaryArray, row: usize) -> Result<[u8; 16], GfError> {
    array
        .value(row)
        .try_into()
        .map_err(|_| storage("UUID width changed"))
}

pub(super) fn write_parquet(
    root: &StableDirectory,
    name: &str,
    batch: &RecordBatch,
    evidence: &mut GraphConstructionEvidence,
) -> Result<ArtifactReceipt, GfError> {
    write_parquet_with_properties(root, name, batch, None, evidence)
}

pub(super) fn write_parquet_with_properties(
    root: &StableDirectory,
    name: &str,
    batch: &RecordBatch,
    properties: Option<parquet::file::properties::WriterProperties>,
    evidence: &mut GraphConstructionEvidence,
) -> Result<ArtifactReceipt, GfError> {
    let temporary = artifact_temp(name);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let hashing = HashingWriter::new(file)?;
    let buffered = BufWriter::with_capacity(BLOCK_BYTES, hashing);
    let mut parquet =
        ArrowWriter::try_new(buffered, batch.schema(), properties).map_err(storage)?;
    parquet.write(batch).map_err(storage)?;
    parquet.finish().map_err(storage)?;
    parquet.sync().map_err(storage)?;
    let hashing = parquet.inner_mut().get_mut();
    hashing.inner.sync_all_and_release().map_err(storage)?;
    let cache_release = hashing.inner.evidence();
    account_cache_release(cache_release, evidence)?;
    construction_failpoint(&format!("artifact.after_temp_fsync.{name}"));
    let receipt = ArtifactReceipt {
        name: name.to_owned(),
        bytes: hashing.bytes,
        allocated_bytes: graphforge_filesystem::file_space_usage(hashing.inner.file())
            .map_err(storage)?
            .allocated_bytes,
        sha256: hex(&hashing.digest.clone().finalize()),
        identity: identity.into(),
        write_operations: hashing.operations,
        fsync_operations: cache_release
            .sync_operations
            .checked_add(3)
            .ok_or_else(|| storage("artifact synchronization count overflows"))?,
    };
    root.sync().map_err(storage)?;
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(name))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("artifact.after_install.{name}"));
    persist_shape_receipt(root, &receipt)?;
    Ok(receipt)
}

pub(super) fn write_fixed_run<const N: usize>(
    root: &StableDirectory,
    name: &str,
    records: &[[u8; N]],
    evidence: &mut GraphConstructionEvidence,
) -> Result<ArtifactReceipt, GfError> {
    write_run(root, name, records, evidence, None)
}

pub(super) fn write_run<const N: usize>(
    root: &StableDirectory,
    name: &str,
    records: &[[u8; N]],
    evidence: &mut GraphConstructionEvidence,
    codec: Option<DetailCodec>,
) -> Result<ArtifactReceipt, GfError> {
    let temporary = artifact_temp(name);
    let file = root
        .create_replaceable_child_file(OsStr::new(&temporary))
        .map_err(storage)?;
    let identity = file_identity(&file).map_err(storage)?;
    let mut writer = HashingWriter::new(file)?;
    let records_per_block = (BLOCK_BYTES / N).max(1);
    let mut block = Vec::with_capacity(records_per_block * N);
    for group in records.chunks(records_per_block) {
        block.clear();
        for record in group {
            block.extend_from_slice(run_record_bytes(record, codec)?);
        }
        writer.write_all(&block).map_err(storage)?;
    }
    writer.flush().map_err(storage)?;
    writer.inner.sync_all_and_release().map_err(storage)?;
    let cache_release = writer.inner.evidence();
    account_cache_release(cache_release, evidence)?;
    construction_failpoint(&format!("artifact.after_temp_fsync.{name}"));
    let receipt = ArtifactReceipt {
        name: name.to_owned(),
        bytes: writer.bytes,
        allocated_bytes: graphforge_filesystem::file_space_usage(writer.inner.file())
            .map_err(storage)?
            .allocated_bytes,
        sha256: hex(&writer.digest.finalize()),
        identity: identity.into(),
        write_operations: writer.operations,
        fsync_operations: cache_release
            .sync_operations
            .checked_add(2)
            .ok_or_else(|| storage("artifact synchronization count overflows"))?,
    };
    root.sync().map_err(storage)?;
    root.install_child(OsStr::new(&temporary), identity, OsStr::new(name))
        .map_err(storage)?;
    root.sync().map_err(storage)?;
    construction_failpoint(&format!("artifact.after_install.{name}"));
    persist_shape_receipt(root, &receipt)?;
    Ok(receipt)
}

pub(super) fn validate_artifact_name(receipt: &ArtifactReceipt) -> Result<(), GfError> {
    let valid_suffix = receipt.name.ends_with(".parquet")
        || receipt.name.ends_with(".identities.run")
        || receipt.name.ends_with(".endpoints.run")
        || receipt.name.ends_with(".node-details.run")
        || receipt.name.ends_with(".edge-details.run");
    if !valid_suffix
        || receipt.name.starts_with('.')
        || receipt.name.contains('/')
        || receipt.name.contains('\\')
        || !is_canonical_sha256(&receipt.sha256)
        || !is_canonical_lower_hex(&receipt.identity.file_id, 32)
        || receipt.bytes == 0
        || receipt.write_operations == 0
        || receipt.fsync_operations == 0
    {
        return Err(storage("invalid construction artifact receipt"));
    }
    if receipt.name.ends_with(".identities.run")
        && !receipt.bytes.is_multiple_of(IDENTITY_WIDTH as u64)
    {
        return Err(storage("truncated identity run"));
    }
    if receipt.name.ends_with(".endpoints.run")
        && !receipt.bytes.is_multiple_of(ENDPOINT_WIDTH as u64)
    {
        return Err(storage("truncated endpoint run"));
    }
    Ok(())
}

pub(super) fn receipt_from_intent(
    intent: &ChunkIntent,
) -> Result<ConstructionChunkReceipt, GfError> {
    Ok(ConstructionChunkReceipt {
        operation_uuid: intent.operation_uuid,
        project_identity: intent.project_identity.clone(),
        session_identity: intent.session_identity.clone(),
        parent_topology_generation: intent.parent_topology_generation,
        ontology_mode: intent.ontology_mode,
        semantic_authority_sha256: intent.semantic_authority_sha256.clone(),
        prior_receipt_sha256: intent.prior_receipt_sha256.clone(),
        chunk_id: intent.chunk_id.clone(),
        sequence: intent.sequence,
        kind: intent.kind,
        rows: intent.rows,
        input_bytes: intent.input_bytes,
        input_sha256: intent.input_sha256.clone(),
        schema_sha256: intent.schema_sha256.clone(),
        run_records: intent.run_records,
        accounted_live_bytes: intent.accounted_live_bytes,
        parquet: intent
            .parquet
            .clone()
            .ok_or_else(|| storage("intent lacks Parquet artifact"))?,
        identities: intent
            .identities
            .clone()
            .ok_or_else(|| storage("intent lacks identity run"))?,
        endpoints: intent.endpoints.clone(),
        details: intent
            .details
            .clone()
            .ok_or_else(|| storage("intent lacks detail run"))?,
    })
}

pub(super) fn validate_receipt_semantics(
    receipt: &ConstructionChunkReceipt,
    sequence: u64,
    budgets: GraphConstructionBudgets,
    codec: DetailCodec,
) -> Result<(), GfError> {
    validate_chunk_id(&receipt.chunk_id)?;
    if receipt.sequence != sequence
        || receipt.operation_uuid.is_nil()
        || receipt.rows == 0
        || receipt.rows > budgets.max_batch_rows as u64
        || receipt.input_bytes > budgets.max_batch_bytes as u64
        || receipt.run_records > budgets.max_run_records as u64
        || receipt.accounted_live_bytes == 0
        || !is_canonical_sha256(&receipt.input_sha256)
        || !is_canonical_sha256(&receipt.schema_sha256)
        || receipt.parquet.name != format!("{}.parquet", artifact_stem(sequence, receipt.kind))
        || receipt.identities.name
            != format!("{}.identities.run", artifact_stem(sequence, receipt.kind))
        || receipt.details.name
            != format!(
                "{}.{}-details.run",
                artifact_stem(sequence, receipt.kind),
                if receipt.kind == ConstructionChunkKind::Node {
                    "node"
                } else {
                    "edge"
                }
            )
        || (receipt.kind == ConstructionChunkKind::Node && receipt.endpoints.is_some())
        || (receipt.kind == ConstructionChunkKind::Edge && receipt.endpoints.is_none())
        || receipt.identities.bytes / IDENTITY_WIDTH as u64 != receipt.rows
        || receipt.run_records
            != receipt.rows
                * if receipt.kind == ConstructionChunkKind::Edge {
                    4
                } else {
                    2
                }
    {
        return Err(storage("receipt semantics are inconsistent"));
    }
    if let Some(endpoints) = &receipt.endpoints
        && (endpoints.name != format!("{}.endpoints.run", artifact_stem(sequence, receipt.kind))
            || endpoints.bytes / ENDPOINT_WIDTH as u64 != receipt.rows * 2)
    {
        return Err(storage("endpoint receipt semantics are inconsistent"));
    }
    let detail_width = if receipt.kind == ConstructionChunkKind::Node {
        NODE_DETAIL_WIDTH
    } else {
        EDGE_DETAIL_WIDTH
    };
    codec
        .validate_size(detail_width, receipt.rows, receipt.details.bytes)
        .map_err(storage)?;
    validate_artifact_name(&receipt.parquet)?;
    validate_artifact_name(&receipt.identities)?;
    validate_artifact_name(&receipt.details)?;
    if let Some(endpoints) = &receipt.endpoints {
        validate_artifact_name(endpoints)?;
    }
    Ok(())
}

pub(super) fn validate_receipt_artifacts(
    root: &StableDirectory,
    receipt: &ConstructionChunkReceipt,
    codec: DetailCodec,
) -> Result<ReadWork, GfError> {
    let mut work = ReadWork::default();
    for artifact in [&receipt.parquet, &receipt.identities, &receipt.details]
        .into_iter()
        .chain(receipt.endpoints.iter())
    {
        let artifact_work = authenticate_artifact(root, artifact, codec)?;
        if artifact.name == receipt.details.name && artifact_work.detail_records != receipt.rows {
            return Err(storage(
                "detail receipt row count differs from authenticated records",
            ));
        }
        work.bytes = work
            .bytes
            .checked_add(artifact_work.bytes)
            .ok_or_else(|| storage("bytes overflows"))?;
        work.operations = work
            .operations
            .checked_add(artifact_work.operations)
            .ok_or_else(|| storage("operations overflows"))?;
        merge_cache_release_evidence(&mut work.cache_release, artifact_work.cache_release)?;
    }
    let parquet_work = validate_parquet_shape(root, receipt)?;
    work.bytes = work
        .bytes
        .checked_add(parquet_work.bytes)
        .ok_or_else(|| storage("bytes overflows"))?;
    work.operations = work
        .operations
        .checked_add(parquet_work.operations)
        .ok_or_else(|| storage("operations overflows"))?;
    merge_cache_release_evidence(&mut work.cache_release, parquet_work.cache_release)?;
    Ok(work)
}

fn validate_parquet_shape(
    root: &StableDirectory,
    receipt: &ConstructionChunkReceipt,
) -> Result<ReadWork, GfError> {
    let file = root
        .open_child_file(OsStr::new(&receipt.parquet.name))
        .map_err(storage)?;
    if !receipt
        .parquet
        .identity
        .matches(file_identity(&file).map_err(storage)?)
    {
        return Err(storage("Parquet identity changed during schema reopen"));
    }
    let counter = IoCounter::default();
    let chunk_reader = CountingChunkReader::new(file, counter.clone());
    let cache_release = chunk_reader.cache_release_tracker();
    let validated = (|| -> Result<(), GfError> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(chunk_reader).map_err(storage)?;
        let expected_prefix = match receipt.kind {
            ConstructionChunkKind::Node => &*CONSTRUCTION_NODE_SCHEMA,
            ConstructionChunkKind::Edge => &*CONSTRUCTION_EDGE_SCHEMA,
        };
        let schema = builder.schema();
        if schema.fields().len() < expected_prefix.fields().len()
            || schema.fields()[..expected_prefix.fields().len()] != expected_prefix.fields()[..]
            || normalized_schema_digest(schema.as_ref()) != receipt.schema_sha256
            || builder.metadata().file_metadata().num_rows()
                != i64::try_from(receipt.rows)
                    .map_err(|_| storage("receipt row count exceeds i64"))?
        {
            return Err(storage("Parquet schema or row count differs from receipt"));
        }
        let reader = builder.with_batch_size(4096).build().map_err(storage)?;
        let identity_name = if receipt.kind == ConstructionChunkKind::Node {
            "node_uuid"
        } else {
            "edge_uuid"
        };
        let mut previous: Option<[u8; 16]> = None;
        for batch in reader {
            let batch = batch.map_err(storage)?;
            let identities = uuid_column(&batch, identity_name)?;
            for row in 0..batch.num_rows() {
                let value = uuid_value(identities, row)?;
                if previous.is_some_and(|prior| prior >= value) {
                    return Err(storage("row artifact is not strictly UUID sorted"));
                }
                previous = Some(value);
            }
        }
        Ok(())
    })();
    combine_cache_cleanup(
        validated,
        cache_release.check_error().map_err(storage),
        "Parquet shape",
    )?;
    Ok(ReadWork {
        detail_records: 0,
        bytes: counter.bytes.load(Ordering::Relaxed),
        operations: counter.operations.load(Ordering::Relaxed),
        cache_release: cache_release.evidence(),
    })
}

pub(super) fn validate_parquet_metadata(
    root: &StableDirectory,
    receipt: &ConstructionChunkReceipt,
) -> Result<ReadWork, GfError> {
    let file = root
        .open_child_file(OsStr::new(&receipt.parquet.name))
        .map_err(storage)?;
    if !receipt
        .parquet
        .identity
        .matches(file_identity(&file).map_err(storage)?)
    {
        return Err(storage("Parquet identity changed during metadata reopen"));
    }
    let counter = IoCounter::default();
    let chunk_reader = CountingChunkReader::new(file, counter.clone());
    let cache_release = chunk_reader.cache_release_tracker();
    let validated = (|| -> Result<(), GfError> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(chunk_reader).map_err(storage)?;
        let expected_prefix = match receipt.kind {
            ConstructionChunkKind::Node => &*CONSTRUCTION_NODE_SCHEMA,
            ConstructionChunkKind::Edge => &*CONSTRUCTION_EDGE_SCHEMA,
        };
        let schema = builder.schema();
        if schema.fields().len() < expected_prefix.fields().len()
            || schema.fields()[..expected_prefix.fields().len()] != expected_prefix.fields()[..]
            || normalized_schema_digest(schema.as_ref()) != receipt.schema_sha256
            || builder.metadata().file_metadata().num_rows()
                != i64::try_from(receipt.rows)
                    .map_err(|_| storage("receipt row count exceeds i64"))?
        {
            return Err(storage("Parquet schema or row count differs from receipt"));
        }
        Ok(())
    })();
    combine_cache_cleanup(
        validated,
        cache_release.check_error().map_err(storage),
        "Parquet metadata",
    )?;
    Ok(ReadWork {
        detail_records: 0,
        bytes: counter.bytes.load(Ordering::Relaxed),
        operations: counter.operations.load(Ordering::Relaxed),
        cache_release: cache_release.evidence(),
    })
}

pub(super) fn validate_intent(
    intent: &ChunkIntent,
    checkpoint: &Checkpoint,
) -> Result<(), GfError> {
    let stem = artifact_stem(intent.sequence, intent.kind);
    let expected_run_records = intent
        .rows
        .checked_mul(if intent.kind == ConstructionChunkKind::Edge {
            4
        } else {
            2
        })
        .ok_or_else(|| storage("intent run record count overflow"))?;
    let prior_sequence = intent.sequence.checked_add(1);
    let expected_identity_bytes = intent
        .rows
        .checked_mul(IDENTITY_WIDTH as u64)
        .ok_or_else(|| storage("intent identity byte count overflow"))?;
    let expected_endpoint_bytes = intent
        .rows
        .checked_mul(2)
        .and_then(|rows| rows.checked_mul(ENDPOINT_WIDTH as u64))
        .ok_or_else(|| storage("intent endpoint byte count overflow"))?;
    let detail_width = if intent.kind == ConstructionChunkKind::Node {
        NODE_DETAIL_WIDTH
    } else {
        EDGE_DETAIL_WIDTH
    };
    let codec = DetailCodec::from_version(checkpoint.format_version).map_err(storage)?;
    if intent.format_version != checkpoint.format_version
        || intent.operation_uuid != checkpoint.operation_uuid
        || intent.project_identity != checkpoint.project_identity
        || intent.session_identity != checkpoint.session_identity
        || !(intent.sequence == checkpoint.next_sequence
            || prior_sequence == Some(checkpoint.next_sequence))
        || intent.parent_topology_generation != checkpoint.parent_topology_generation
        || intent.ontology_mode != checkpoint.ontology_mode
        || intent.semantic_authority_sha256 != checkpoint.semantic_authority_sha256
        || (intent.sequence == checkpoint.next_sequence
            && intent.prior_receipt_sha256 != checkpoint.last_receipt_sha256)
        || intent.chunk_key != chunk_key_name(&intent.chunk_id)
        || intent.rows == 0
        || intent.rows > checkpoint.budgets.max_batch_rows as u64
        || intent.input_bytes > checkpoint.budgets.max_batch_bytes as u64
        || intent.run_records > checkpoint.budgets.max_run_records as u64
        || intent.accounted_live_bytes > 0 && intent.accounted_live_bytes < intent.input_bytes
        || intent.run_records != expected_run_records
        || !is_canonical_sha256(&intent.input_sha256)
        || !is_canonical_sha256(&intent.schema_sha256)
        || intent
            .parquet
            .as_ref()
            .is_some_and(|artifact| artifact.name != format!("{stem}.parquet"))
        || intent.identities.as_ref().is_some_and(|artifact| {
            artifact.name != format!("{stem}.identities.run")
                || artifact.bytes != expected_identity_bytes
        })
        || intent.endpoints.as_ref().is_some_and(|artifact| {
            intent.kind != ConstructionChunkKind::Edge
                || artifact.name != format!("{stem}.endpoints.run")
                || artifact.bytes != expected_endpoint_bytes
        })
        || intent.details.as_ref().is_some_and(|artifact| {
            artifact.name
                != format!(
                    "{stem}.{}-details.run",
                    if intent.kind == ConstructionChunkKind::Node {
                        "node"
                    } else {
                        "edge"
                    }
                )
                || codec
                    .validate_size(detail_width, intent.rows, artifact.bytes)
                    .is_err()
        })
    {
        return Err(storage("durable intent is inconsistent with checkpoint"));
    }
    Ok(())
}

pub(super) fn artifact_stem(sequence: u64, kind: ConstructionChunkKind) -> String {
    format!("chunk-{sequence:020}-{}", kind.tag())
}

pub(super) fn receipt_name(sequence: u64) -> String {
    format!("receipt-{sequence:020}.json")
}

pub(super) fn chunk_key_name(chunk_id: &str) -> String {
    format!("key-{}.json", sha256(chunk_id.as_bytes()))
}

fn validate_chunk_id(value: &str) -> Result<(), GfError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(storage("invalid chunk id"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
