//! Immutable origins and independently advancing per-field incorporated baselines.
use super::{
    fields::{self, Fields, Key},
    private_view,
};
use crate::{CancellationToken, GfError, GraphForge};
use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use graphforge_storage::{
    ProjectParticipant, ProjectParticipantEncoding,
    research_versions::{
        PreparedBranchContent, ResearchParticipantKey, replace_prepared_branch_domains,
    },
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use uuid::Uuid;
const FAMILY: &str = "branch_fields";
const COLUMNS: [&str; 12] = [
    "object_kind",
    "object_uuid",
    "field",
    "origin_version",
    "incorporated_version",
    "origin_sha256",
    "baseline_sha256",
    "current_sha256",
    "contribution_uuid",
    "role",
    "status",
    "contract",
];
#[derive(Clone)]
struct Row {
    key: Key,
    origin: Uuid,
    incorporated: Option<Uuid>,
    original: String,
    baseline: String,
    current: String,
    contribution: Uuid,
    role: String,
}
impl Row {
    fn status(&self) -> &'static str {
        if self.current.is_empty() {
            "suppressed"
        } else if self.current == self.baseline {
            "inherited"
        } else {
            "local"
        }
    }
}
fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(
        COLUMNS
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ))
}
fn invalid() -> GfError {
    GfError::Validation("invalid Branch field baseline participant".into())
}
fn hash(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, b| {
        write!(out, "{b:02x}").expect("string");
        out
    })
}
fn contribution(branch: Uuid, operation: Uuid, key: &Key) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(b"graphforge-branch-contribution/1");
    digest.update(branch.as_bytes());
    digest.update(operation.as_bytes());
    digest.update(serde_json::to_vec(key).expect("semantic key"));
    graphforge_core::canonical::uuid_v8(digest.finalize().into())
}
fn batch(rows: &BTreeMap<Key, Row>) -> Result<RecordBatch, GfError> {
    let mut columns: [Vec<String>; 12] = std::array::from_fn(|_| Vec::new());
    for row in rows.values() {
        let values = [
            row.key.0.clone(),
            row.key.1.to_string(),
            row.key.2.clone(),
            row.origin.to_string(),
            row.incorporated
                .map(|id| id.to_string())
                .unwrap_or_default(),
            row.original.clone(),
            row.baseline.clone(),
            row.current.clone(),
            row.contribution.to_string(),
            row.role.clone(),
            row.status().into(),
            "1".into(),
        ];
        for (column, value) in columns.iter_mut().zip(values) {
            column.push(value);
        }
    }
    RecordBatch::try_new(
        schema(),
        columns
            .into_iter()
            .map(|c| Arc::new(StringArray::from(c)) as arrow::array::ArrayRef)
            .collect(),
    )
    .map_err(|_| invalid())
}
fn read(graph: &GraphForge) -> Result<BTreeMap<Key, Row>, GfError> {
    let generation = graph.generation_for_read()?;
    super::domain_bounds::preflight(&generation)?;
    let Some(snapshot) = generation.participant_snapshot("workspace", FAMILY)? else {
        return Ok(BTreeMap::new());
    };
    if snapshot.bytes.len() > 64 * 1024 * 1024
        || snapshot.row_count > 1_000_000
        || snapshot.record_version != 1
        || snapshot.schema_fingerprint != fingerprint()
    {
        return Err(invalid());
    }
    let mut rows = BTreeMap::new();
    for batch in crate::knowledge::read_parquet(&snapshot.bytes)? {
        if batch.schema().fields() != schema().fields() {
            return Err(invalid());
        }
        let columns = batch
            .columns()
            .iter()
            .map(|c| c.as_any().downcast_ref::<StringArray>().ok_or_else(invalid))
            .collect::<Result<Vec<_>, _>>()?;
        for index in 0..batch.num_rows() {
            use arrow::array::Array;
            if columns.iter().any(|c| c.is_null(index)) {
                return Err(invalid());
            }
            let text = |n: usize| columns[n].value(index).to_owned();
            let uuid = |n: usize| Uuid::parse_str(columns[n].value(index)).map_err(|_| invalid());
            let row = Row {
                key: (text(0), uuid(1)?, text(2)),
                origin: uuid(3)?,
                incorporated: if text(4).is_empty() {
                    None
                } else {
                    Some(uuid(4)?)
                },
                original: text(5),
                baseline: text(6),
                current: text(7),
                contribution: uuid(8)?,
                role: text(9),
            };
            if text(10) != row.status()
                || text(11) != "1"
                || rows.insert(row.key.clone(), row).is_some()
            {
                return Err(invalid());
            }
        }
    }
    Ok(rows)
}
fn fingerprint() -> [u8; 32] {
    Sha256::digest(b"graphforge-branch-fields/1").into()
}
fn install(
    root: &std::path::Path,
    prepared: &mut PreparedBranchContent,
    rows: &BTreeMap<Key, Row>,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let batch = batch(rows)?;
    if batch.get_array_memory_size() > 64 * 1024 * 1024 {
        return Err(invalid());
    }
    let mut bytes = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut bytes, batch.schema(), None)
        .map_err(|_| invalid())?;
    writer.write(&batch).map_err(|_| invalid())?;
    writer.close().map_err(|_| invalid())?;
    let participant = ProjectParticipant {
        capability_id: "workspace".into(),
        capability_version: 1,
        record_family_id: FAMILY.into(),
        record_version: 1,
        encoding: ProjectParticipantEncoding::Parquet,
        schema_fingerprint: fingerprint(),
        row_count: rows.len() as u64,
        bytes,
    };
    let keep: BTreeSet<ResearchParticipantKey> = prepared
        .version
        .content
        .participants
        .iter()
        .map(|p| p.key.clone())
        .collect();
    replace_prepared_branch_domains(root, prepared, &keep, &[participant], cancellation.flag())
}

pub(super) fn initialize(
    owner: &GraphForge,
    prepared: &mut PreparedBranchContent,
    operation: Uuid,
    active: Option<&BTreeSet<(String, Uuid)>>,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let graph = private_view::open(owner, prepared)?;
    let inherited = read(&graph)?;
    let current = fields::read(&graph, cancellation)?;
    let origin = prepared
        .version
        .content
        .source_version
        .ok_or_else(invalid)?;
    let rows = current
        .into_iter()
        .map(|(key, digest)| {
            let prior = inherited.get(&key);
            let row = Row {
                origin: prior.map_or(origin, |r| r.origin),
                incorporated: Some(origin),
                original: prior.map_or_else(|| hash(&digest), |r| r.original.clone()),
                baseline: hash(&digest),
                current: hash(&digest),
                contribution: prior.map_or_else(
                    || contribution(prepared.version.context_uuid, operation, &key),
                    |r| r.contribution,
                ),
                role: match active {
                    Some(set) if set.contains(&(key.0.clone(), key.1)) => "active".into(),
                    Some(_) => "required".into(),
                    None => prior.map_or_else(|| "active".into(), |r| r.role.clone()),
                },
                key: key.clone(),
            };
            (key, row)
        })
        .collect();
    install(
        owner.resolved_generation.container_root(),
        prepared,
        &rows,
        cancellation,
    )
}
pub(super) fn update(
    owner: &GraphForge,
    graph: &GraphForge,
    prepared: &mut PreparedBranchContent,
    operation: Uuid,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let mut rows = read(graph)?;
    let current: Fields = fields::read(graph, cancellation)?;
    for row in rows.values_mut() {
        row.current = current.get(&row.key).map(hash).unwrap_or_default();
    }
    for (key, digest) in current {
        rows.entry(key.clone()).or_insert_with(|| Row {
            key: key.clone(),
            origin: prepared.version.version_uuid,
            incorporated: None,
            original: hash(&digest),
            baseline: String::new(),
            current: hash(&digest),
            contribution: contribution(prepared.version.context_uuid, operation, &key),
            role: "active".into(),
        });
    }
    install(
        owner.resolved_generation.container_root(),
        prepared,
        &rows,
        cancellation,
    )
}

pub(super) fn inspect(graph: &GraphForge) -> Result<crate::ExecutionResult, GfError> {
    Ok(crate::knowledge::assertion_result(batch(&read(graph)?)?))
}

pub(super) fn preserve_selected(
    root: &std::path::Path,
    source: &GraphForge,
    prepared: &mut PreparedBranchContent,
    selected: &BTreeSet<(String, Uuid)>,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let rows = read(source)?
        .into_iter()
        .filter(|(key, _)| selected.contains(&(key.0.clone(), key.1)))
        .collect();
    install(root, prepared, &rows, cancellation)
}

pub(super) fn incorporate(
    owner: &GraphForge,
    source: &GraphForge,
    prepared: &mut PreparedBranchContent,
    source_version: Uuid,
    active: &BTreeSet<(String, Uuid)>,
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    let graph = private_view::open(owner, prepared)?;
    let mut rows = read(&graph)?;
    let incoming = read(source)?;
    let current = fields::read(&graph, cancellation)?;
    for row in rows.values_mut() {
        row.current = current.get(&row.key).map(hash).unwrap_or_default();
    }
    for (key, digest) in current {
        if let Some(existing) = rows.get_mut(&key) {
            if active.contains(&(key.0.clone(), key.1)) {
                existing.role = "active".into();
            }
            continue;
        }
        let prior = incoming.get(&key);
        rows.insert(
            key.clone(),
            Row {
                key: key.clone(),
                origin: prior.map_or(source_version, |r| r.origin),
                incorporated: Some(source_version),
                original: prior.map_or_else(|| hash(&digest), |r| r.original.clone()),
                baseline: hash(&digest),
                current: hash(&digest),
                contribution: prior.map_or_else(
                    || contribution(prepared.version.context_uuid, source_version, &key),
                    |r| r.contribution,
                ),
                role: if active.contains(&(key.0.clone(), key.1)) {
                    "active"
                } else {
                    "required"
                }
                .into(),
            },
        );
    }
    install(
        owner.resolved_generation.container_root(),
        prepared,
        &rows,
        cancellation,
    )
}

/// The permanent base supplies the canonical exact-membership selector. This
/// definition is inspectable without retaining the source's unrelated payload.
pub(super) fn selection(
    graph: &GraphForge,
    record: &graphforge_storage::research_versions::ResearchBranchRecord,
) -> Result<crate::ExecutionResult, GfError> {
    let rows = read(graph)?;
    let selected: Vec<_> = rows.values().filter(|r| r.key.2 == "$object").collect();
    let metadata = std::collections::HashMap::from([
        (
            "graphforge.branch.selection_contract".into(),
            "exact_membership/1".into(),
        ),
        (
            "graphforge.branch.origin_version".into(),
            record.origin_version_uuid.to_string(),
        ),
        (
            "graphforge.branch.base_version".into(),
            record.base_version_uuid.to_string(),
        ),
        (
            "graphforge.branch.source_selector_sha256".into(),
            hash(&record.selection_sha256),
        ),
    ]);
    let schema = Arc::new(Schema::new_with_metadata(
        ["object_kind", "object_uuid", "role"]
            .into_iter()
            .map(|n| Field::new(n, DataType::Utf8, false))
            .collect::<Vec<_>>(),
        metadata,
    ));
    let columns = [
        selected.iter().map(|r| r.key.0.clone()).collect::<Vec<_>>(),
        selected.iter().map(|r| r.key.1.to_string()).collect(),
        selected.iter().map(|r| r.role.clone()).collect(),
    ]
    .into_iter()
    .map(|values| Arc::new(StringArray::from(values)) as arrow::array::ArrayRef)
    .collect();
    let batch = RecordBatch::try_new(schema, columns).map_err(|_| invalid())?;
    Ok(crate::knowledge::assertion_result(batch))
}
