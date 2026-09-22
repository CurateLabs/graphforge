//! Exact membership capsules: Arrow data and immutable Version commitments.
use super::{
    CancellationToken, ExecutionResult, GfError, GraphForge, Inclusion, Object, PageRequest,
    Selection, SliceLimits, SliceMembers, SlicePageKind, SliceRequest, SliceRevisionRequest,
    SliceSelector, SliceSource, Uuid, checkpoint, engine, genealogy, graph, invalid, limit, output,
    unavailable,
};
use arrow::array::{
    Array, ArrayRef, ListArray, ListBuilder, StringArray, StringBuilder, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow::record_batch::RecordBatch;
use graphforge_storage::research_versions::{
    ResearchEvidenceReference, ResearchParticipantCommitment,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
const CONTEXT: &str = "graphforge.slice.frozen_context";
const DIGEST: &str = "graphforge.slice.sha256";
const MAX_BYTES: usize = 16 * 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Context {
    contract: u32,
    request_uuid: Uuid,
    version_uuid: Uuid,
    generation_uuid: Uuid,
    version_sha256: [u8; 32],
    selector_sha256: [u8; 32],
    ontology: Vec<ResearchParticipantCommitment>,
    evidence: Vec<ResearchEvidenceReference>,
    limits: SliceLimits,
}
impl GraphForge {
    /// Freeze exact membership and evidence/ontology commitments at an explicitly
    /// retained Version. The returned Arrow capsule is not an independent graph
    /// copy and does not establish a new retention root.
    pub fn freeze_slice(
        &self,
        request: &SliceRequest,
        cancellation: &CancellationToken,
    ) -> Result<ExecutionResult, GfError> {
        graph::validate(request)?;
        checkpoint(Some(cancellation))?;
        let SliceSource::Version { version_uuid } = request.source else {
            return Err(invalid(
                "freezing requires an explicitly retained research Version",
            ));
        };
        let version = self.research_version(version_uuid)?;
        let (view, _) = graph::open(self, request)?;
        let mut selection = engine::evaluate(&view, request, Some(cancellation))?;
        genealogy(self, request, &mut selection)?;
        let context = Context {
            contract: 1,
            request_uuid: request.request_uuid,
            version_uuid,
            generation_uuid: version.content.generation_uuid,
            version_sha256: Sha256::digest(
                serde_json::to_vec(&version.content)
                    .map_err(|_| invalid("invalid Version commitment"))?,
            )
            .into(),
            selector_sha256: Sha256::digest(
                serde_json::to_vec(request).map_err(|_| invalid("invalid Slice request"))?,
            )
            .into(),
            ontology: version
                .content
                .participants
                .iter()
                .filter(|p| p.key.capability == "workspace" && p.key.family.contains("ontology"))
                .cloned()
                .collect(),
            evidence: version
                .content
                .evidence
                .iter()
                .filter(|e| {
                    let id = match e {
                        ResearchEvidenceReference::Local { artifact_uuid, .. }
                        | ResearchEvidenceReference::ExternalOnly { artifact_uuid, .. }
                        | ResearchEvidenceReference::Unverifiable { artifact_uuid } => {
                            *artifact_uuid
                        }
                    };
                    let object = Object::new("artifact", id);
                    selection.active.contains_key(&object)
                        || selection.required.contains_key(&object)
                })
                .cloned()
                .collect(),
            limits: request.limits.clone(),
        };
        for object in selection
            .active
            .keys()
            .chain(selection.required.keys())
            .filter(|o| o.kind == "artifact")
        {
            if !context.evidence.iter().any(|e| match e {
                ResearchEvidenceReference::Local { artifact_uuid, .. }
                | ResearchEvidenceReference::ExternalOnly { artifact_uuid, .. }
                | ResearchEvidenceReference::Unverifiable { artifact_uuid } => {
                    *artifact_uuid == object.uuid
                }
            }) {
                return Err(unavailable());
            }
        }
        capsule(&selection, &context, Some(cancellation))
    }

    /// Inspect frozen membership without reevaluating its selector or reading
    /// CURRENT. Membership remains inspectable after source payload release.
    pub fn inspect_frozen_slice(
        &self,
        ipc: &[u8],
        kind: SlicePageKind,
        page: PageRequest,
    ) -> Result<ExecutionResult, GfError> {
        let (selection, context) = decode(ipc, page.cancellation.as_ref())?;
        let request = frozen_request(&context);
        let mut result = output::page(
            &selection,
            &request,
            context.version_uuid,
            kind,
            page,
            Some(&serde_json::to_vec(&context).map_err(|_| invalid("invalid frozen context"))?),
        )?;
        let mut metadata = result.schema.metadata().clone();
        metadata.insert(
            CONTEXT.into(),
            serde_json::to_string(&context).map_err(|_| invalid("invalid frozen context"))?,
        );
        result.schema = Arc::new(Schema::new_with_metadata(
            result.schema.fields().clone(),
            metadata,
        ));
        result.batches = result
            .batches
            .into_iter()
            .map(|batch| {
                RecordBatch::try_new(result.schema.clone(), batch.columns().to_vec())
                    .map_err(|_| invalid("invalid Slice schema"))
            })
            .collect::<Result<_, _>>()?;
        for batch in &result.batches {
            output::check_response(batch, context.limits.response_bytes)?;
        }
        Ok(result)
    }

    /// Expand or contract exact membership. Outside history is read only when a
    /// separately retained Version is explicitly named; CURRENT is never inferred.
    pub fn revise_frozen_slice(
        &self,
        ipc: &[u8],
        revision: &SliceRevisionRequest,
        cancellation: &CancellationToken,
    ) -> Result<ExecutionResult, GfError> {
        let (selection, context) = decode(ipc, Some(cancellation))?;
        let mut members = SliceMembers::default();
        for object in selection.active.keys() {
            match object.kind.as_str() {
                "node" => &mut members.nodes,
                "edge" => &mut members.edges,
                "source" => &mut members.sources,
                "artifact" => &mut members.artifacts,
                "assertion" => &mut members.assertions,
                _ => return Err(invalid("invalid active frozen object kind")),
            }
            .insert(object.uuid);
        }
        self.freeze_slice(
            &SliceRequest {
                request_uuid: revision.request_uuid,
                source: SliceSource::Version {
                    version_uuid: revision.source_version.unwrap_or(context.version_uuid),
                },
                selector: SliceSelector::Direct { members },
                include: revision.include.clone(),
                exclude: revision.exclude.clone(),
                limits: context.limits,
            },
            cancellation,
        )
    }
}
fn frozen_request(context: &Context) -> SliceRequest {
    SliceRequest {
        request_uuid: context.request_uuid,
        source: SliceSource::Version {
            version_uuid: context.version_uuid,
        },
        selector: SliceSelector::Direct {
            members: SliceMembers::default(),
        },
        include: SliceMembers::default(),
        exclude: SliceMembers::default(),
        limits: context.limits.clone(),
    }
}
fn schema(metadata: HashMap<String, String>) -> Schema {
    Schema::new_with_metadata(
        vec![
            Field::new("role", DataType::Utf8, false),
            Field::new("object_kind", DataType::Utf8, false),
            Field::new("object_uuid", DataType::Utf8, false),
            Field::new("reason", DataType::Utf8, false),
            Field::new("root_uuid", DataType::Utf8, false),
            Field::new("predecessor_uuid", DataType::Utf8, true),
            Field::new("via_edge_uuid", DataType::Utf8, true),
            Field::new("depth", DataType::UInt32, false),
            Field::new(
                "labels",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
        ],
        metadata,
    )
}
fn batch(selection: &Selection, context: &Context) -> Result<RecordBatch, GfError> {
    let mut roles = Vec::new();
    let mut kinds = Vec::new();
    let mut ids = Vec::new();
    let mut reasons = Vec::new();
    let mut roots = Vec::new();
    let mut predecessors = Vec::new();
    let mut edges = Vec::new();
    let mut depths = Vec::new();
    let mut labels = ListBuilder::new(StringBuilder::new());
    for (role, rows) in [
        ("active", &selection.active),
        ("required", &selection.required),
        ("boundary", &selection.boundary),
    ] {
        for (object, why) in rows {
            roles.push(role);
            kinds.push(object.kind.as_str());
            ids.push(object.uuid.to_string());
            reasons.push(why.reason.as_str());
            roots.push(why.root.to_string());
            predecessors.push(why.predecessor.map(|id| id.to_string()));
            edges.push(why.via_edge.map(|id| id.to_string()));
            depths.push(why.depth);
            if role == "active" {
                for label in selection.labels.get(object).into_iter().flatten() {
                    labels.values().append_value(label);
                }
            }
            labels.append(true);
        }
    }
    let metadata = HashMap::from([(
        CONTEXT.into(),
        serde_json::to_string(context).map_err(|_| invalid("invalid frozen context"))?,
    )]);
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(roles)),
        Arc::new(StringArray::from(kinds)),
        Arc::new(StringArray::from(ids)),
        Arc::new(StringArray::from(reasons)),
        Arc::new(StringArray::from(roots)),
        Arc::new(StringArray::from(predecessors)),
        Arc::new(StringArray::from(edges)),
        Arc::new(UInt32Array::from(depths)),
        Arc::new(labels.finish()),
    ];
    RecordBatch::try_new(Arc::new(schema(metadata)), arrays)
        .map_err(|_| invalid("invalid Slice capsule schema"))
}
fn encode(batch: &RecordBatch) -> Result<Vec<u8>, GfError> {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, batch.schema().as_ref())
        .map_err(|_| invalid("Slice encoding failed"))?;
    writer
        .write(batch)
        .and_then(|()| writer.finish())
        .map_err(|_| invalid("Slice encoding failed"))?;
    Ok(bytes)
}
fn capsule(
    selection: &Selection,
    context: &Context,
    cancellation: Option<&CancellationToken>,
) -> Result<ExecutionResult, GfError> {
    checkpoint(cancellation)?;
    let batch = batch(selection, context)?;
    if batch.get_array_memory_size() as u64 > context.limits.response_bytes {
        return Err(limit());
    }
    let bytes = encode(&batch)?;
    if bytes.len() as u64 > context.limits.response_bytes {
        return Err(limit());
    }
    let mut metadata = batch.schema().metadata().clone();
    metadata.insert(DIGEST.into(), digest_hex(&bytes));
    let schema = Arc::new(schema(metadata));
    let batch = RecordBatch::try_new(schema.clone(), batch.columns().to_vec())
        .map_err(|_| invalid("invalid Slice capsule"))?;
    if encode(&batch)?.len() as u64 > context.limits.response_bytes {
        return Err(limit());
    }
    checkpoint(cancellation)?;
    Ok(ExecutionResult {
        schema,
        batches: vec![batch],
        stats: crate::ExecutionStats::default(),
        side_effects: None,
        mutation_receipt: None,
    })
}
fn decode(
    ipc: &[u8],
    cancellation: Option<&CancellationToken>,
) -> Result<(Selection, Context), GfError> {
    checkpoint(cancellation)?;
    validate_frames(ipc)?;
    let mut reader = StreamReader::try_new(Cursor::new(ipc), None)
        .map_err(|_| invalid("invalid frozen Slice IPC"))?;
    let schema = reader.schema();
    if schema.fields() != self::schema(HashMap::new()).fields() {
        return Err(invalid("invalid frozen Slice schema"));
    }
    let context: Context = serde_json::from_str(
        schema
            .metadata()
            .get(CONTEXT)
            .ok_or_else(|| invalid("missing frozen context"))?,
    )
    .map_err(|_| invalid("invalid frozen context"))?;
    if context.contract != 1 || context.version_uuid.is_nil() {
        return Err(invalid("unsupported frozen Slice contract"));
    }
    graph::validate(&frozen_request(&context))?;
    if ipc.len() as u64 > context.limits.response_bytes {
        return Err(limit());
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("missing frozen membership"))?
        .map_err(|_| invalid("invalid frozen membership"))?;
    if reader.next().is_some() {
        return Err(invalid("frozen capsule requires one canonical batch"));
    }
    let mut selection = Selection::default();
    let columns: Vec<_> = (0..7)
        .map(|i| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid("invalid frozen string column"))
        })
        .collect::<Result<_, _>>()?;
    let depths = batch
        .column(7)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| invalid("invalid frozen depth"))?;
    let labels = batch
        .column(8)
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| invalid("invalid frozen labels"))?;
    for row in 0..batch.num_rows() {
        checkpoint(cancellation)?;
        if columns[..5].iter().any(|c| c.is_null(row)) || depths.is_null(row) || labels.is_null(row)
        {
            return Err(invalid("null frozen membership"));
        }
        let parse = |i: usize| {
            Uuid::parse_str(columns[i].value(row)).map_err(|_| invalid("invalid frozen identity"))
        };
        let object = Object::new(columns[1].value(row), parse(2)?);
        let why = Inclusion {
            reason: columns[3].value(row).into(),
            root: parse(4)?,
            predecessor: if columns[5].is_null(row) {
                None
            } else {
                Some(parse(5)?)
            },
            via_edge: if columns[6].is_null(row) {
                None
            } else {
                Some(parse(6)?)
            },
            depth: depths.value(row),
        };
        let (rows, max) = match columns[0].value(row) {
            "active" => (&mut selection.active, context.limits.selected_objects),
            "required" => (&mut selection.required, context.limits.dependencies),
            "boundary" => (&mut selection.boundary, context.limits.boundary_references),
            _ => return Err(invalid("invalid frozen role")),
        };
        if rows.insert(object.clone(), why).is_some() || rows.len() > max as usize {
            return Err(invalid("duplicate or oversized frozen membership"));
        }
        let values = labels.value(row);
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| invalid("invalid frozen labels"))?;
        if columns[0].value(row) == "active" {
            selection
                .labels
                .insert(object, values.iter().flatten().map(str::to_owned).collect());
        }
    }
    let canonical = encode(&self::batch(&selection, &context)?)?;
    if schema.metadata().get(DIGEST) != Some(&digest_hex(&canonical)) {
        return Err(invalid("frozen Slice fingerprint mismatch"));
    }
    Ok((selection, context))
}
// Check framing and compression before Arrow allocates decoded arrays. Capsules
// use one uncompressed flat batch; compressed expansion and dictionary payloads
// are outside the contract, even when the input byte length is small.
fn validate_frames(ipc: &[u8]) -> Result<(), GfError> {
    if ipc.len() > MAX_BYTES {
        return Err(limit());
    }
    let mut offset = 0usize;
    let mut messages = 0;
    loop {
        if ipc.get(offset..offset + 4) != Some(&[255; 4]) {
            return Err(invalid("invalid frozen IPC framing"));
        }
        let len = u32::from_le_bytes(
            ipc.get(offset + 4..offset + 8)
                .ok_or_else(|| invalid("invalid frozen IPC framing"))?
                .try_into()
                .map_err(|_| invalid("invalid frozen IPC framing"))?,
        ) as usize;
        offset += 8;
        if len == 0 {
            if offset != ipc.len() {
                return Err(invalid("trailing frozen IPC data"));
            }
            break;
        }
        messages += 1;
        if messages > 2 {
            return Err(invalid("unexpected frozen IPC message"));
        }
        let header = ipc
            .get(offset..offset.checked_add(len).ok_or_else(limit)?)
            .ok_or_else(|| invalid("invalid frozen IPC header"))?;
        let message = arrow::ipc::root_as_message(header)
            .map_err(|_| invalid("invalid frozen IPC message"))?;
        if let Some(batch) = message.header_as_record_batch() {
            if batch.compression().is_some() || batch.length() < 0 || batch.length() > 300_000 {
                return Err(invalid("compressed or oversized frozen IPC is unsupported"));
            }
        } else if message.header_as_schema().is_none() {
            return Err(invalid("unexpected frozen IPC message"));
        }
        let body = usize::try_from(message.bodyLength())
            .map_err(|_| invalid("invalid frozen IPC body"))?;
        offset = offset
            .checked_add(len)
            .and_then(|n| n.checked_add(body))
            .filter(|n| *n <= ipc.len())
            .ok_or_else(|| invalid("invalid frozen IPC body"))?;
    }
    if messages != 2 {
        return Err(invalid("incomplete frozen IPC capsule"));
    }
    Ok(())
}

fn digest_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            write!(text, "{byte:02x}").expect("writing a String cannot fail");
            text
        })
}
