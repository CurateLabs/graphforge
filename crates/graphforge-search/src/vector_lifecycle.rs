//! Graph-membership lifecycle for UUID-keyed vector publications.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::path::Path;

use arrow::array::{Array, FixedSizeBinaryArray, ListArray, UInt32Array, UInt64Array};
use graphforge_storage::{
    PublishedSearchArtifact, SearchArtifactError, SearchArtifactKey, SearchCoordinationLimits,
    SearchPublicationOutcome, SearchSourceSnapshot, VECTOR_BACKEND_VERSION,
    VECTOR_CONTRACT_VERSION, VectorSearchHit, VectorStoreLimits, current_search_artifact,
    search_published_vectors, upsert_published_vector, validate_vector,
};

/// Bounds for graph membership, vector work, and atomic publication.
#[derive(Clone, Copy, Debug)]
pub struct VectorLifecycleLimits {
    /// Primary vector persistence and exact-search bounds.
    pub vector: VectorStoreLimits,
    /// Per-key lock and cleanup bounds used by atomic upserts.
    pub coordination: SearchCoordinationLimits,
    /// Maximum topology rows inspected for label membership.
    pub topology_rows: usize,
    /// Maximum committed topology bytes read for a source snapshot.
    pub source_bytes: u64,
}

impl Default for VectorLifecycleLimits {
    fn default() -> Self {
        Self {
            vector: VectorStoreLimits::default(),
            coordination: SearchCoordinationLimits::default(),
            topology_rows: 1_000_000,
            source_bytes: 4 * 1024 * 1024 * 1024,
        }
    }
}

/// Caller-resolved vector artifact identity and local label membership ID.
#[derive(Clone, Copy, Debug)]
pub struct VectorIndexRequest<'a> {
    /// Normalized graph label persisted in the artifact key.
    pub label: &'a str,
    /// Local catalog identity used only for topology membership projection.
    pub label_id: u32,
    /// Normalized caller-defined vector space persisted in the artifact key.
    pub space: &'a str,
}

/// Atomically insert or replace one UUID vector after validating current label membership.
///
/// The membership read and topology fingerprint are enclosed by the storage
/// coordinator's bounded mutation check. A second graph race fails closed.
///
/// # Errors
/// Returns structured selector, source, corruption, cancellation, resource,
/// lock, I/O, or repeated-concurrent-mutation errors.
#[allow(clippy::too_many_arguments)]
pub fn upsert_graph_vector<C>(
    project_dir: &Path,
    request: VectorIndexRequest<'_>,
    node_uuid: [u8; 16],
    vector: &[f32],
    updated_at_micros: i64,
    limits: VectorLifecycleLimits,
    checkpoint: C,
) -> Result<SearchPublicationOutcome, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let key = SearchArtifactKey::vector(request.label, request.space)?;
    let checkpoint = RefCell::new(checkpoint);
    let projection = RefCell::new(None::<LabelMemberProjection>);
    upsert_published_vector(
        project_dir,
        key.label(),
        key.space().expect("vector keys always contain a space"),
        node_uuid,
        vector,
        updated_at_micros,
        limits.vector,
        limits.coordination,
        || {
            if projection.borrow().is_none() {
                *projection.borrow_mut() = Some(project_label_members_snapshot(
                    project_dir,
                    request.label_id,
                    limits,
                    || checkpoint.borrow_mut()(),
                )?);
            }
            let borrowed = projection.borrow();
            let projected = borrowed.as_ref().expect("projection initialized");
            if SearchSourceSnapshot::generation(project_dir)? != projected.snapshot.generation {
                return Err(SearchArtifactError::ConcurrentMutation);
            }
            Ok(projected.snapshot.clone())
        },
        |candidate| {
            Ok(projection
                .borrow()
                .as_ref()
                .is_some_and(|projected| projected.members.contains(&candidate)))
        },
        || checkpoint.borrow_mut()(),
    )
}

/// Search one vector space against a stable current label-membership projection.
///
/// A missing requested publication is a stable empty result. Published primary
/// vectors are immutable and are never rebuilt here. One topology race retries
/// the whole read; a second returns `ConcurrentMutation` without partial hits.
///
/// # Errors
/// Returns structured selector, source, corruption, cancellation, resource,
/// I/O, or repeated-concurrent-mutation errors.
pub fn search_graph_vectors<C>(
    project_dir: &Path,
    request: VectorIndexRequest<'_>,
    query: &[f32],
    limit: usize,
    limits: VectorLifecycleLimits,
    mut checkpoint: C,
) -> Result<Vec<VectorSearchHit>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let key = SearchArtifactKey::vector(request.label, request.space)?;
    validate_vector(query, limits.vector)?;
    validate_result_limit(limit, limits.vector)?;

    for attempt in 1_u8..=2 {
        let projection =
            project_label_members_snapshot(project_dir, request.label_id, limits, &mut checkpoint)?;
        let expected_generation = projection.snapshot.generation;

        let hits = match current_search_artifact(project_dir, &key)? {
            Some(artifact) => {
                validate_requested_artifact(&artifact, &key)?;
                search_published_vectors(
                    &artifact,
                    query,
                    &projection.members,
                    limit,
                    limits.vector,
                    &mut checkpoint,
                )?
            }
            None => Vec::new(),
        };
        if SearchSourceSnapshot::generation(project_dir)? == expected_generation {
            return Ok(hits);
        }
        if attempt == 2 {
            return Err(SearchArtifactError::ConcurrentMutation);
        }
    }
    unreachable!("the bounded vector search loop returns on both terminal paths")
}

/// Project the current committed UUID membership for one caller-resolved label.
///
/// This is shared by exact-vector retrieval and public Arrow result shaping so
/// secondary labels use the same bounded topology interpretation everywhere.
///
/// # Errors
/// Returns structured source, cancellation, or resource errors for malformed
/// topology or an exhausted membership projection.
pub fn project_label_members<C>(
    project_dir: &Path,
    label_id: u32,
    limits: VectorLifecycleLimits,
    checkpoint: C,
) -> Result<BTreeSet<[u8; 16]>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    project_label_members_snapshot(project_dir, label_id, limits, checkpoint)
        .map(|projection| projection.members)
}

pub(crate) struct LabelMemberProjection {
    pub(crate) members: BTreeSet<[u8; 16]>,
    pub(crate) snapshot: SearchSourceSnapshot,
}

#[allow(clippy::too_many_lines)] // one streaming callback preserves one admitted handle
pub(crate) fn project_label_members_snapshot<C>(
    project_dir: &Path,
    label_id: u32,
    limits: VectorLifecycleLimits,
    mut checkpoint: C,
) -> Result<LabelMemberProjection, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    let source_generation = SearchSourceSnapshot::generation(project_dir)?;
    let mut source_evidence = Vec::new();
    let mut eligible = BTreeSet::new();
    let mut rows = 0_usize;
    let mut last_surrogate = None;
    let mut index = graphforge_storage::uuid_membership_index_present(project_dir)
        .then(|| graphforge_storage::UuidMembershipIndex::open(project_dir))
        .transpose()
        .map_err(|error| source(error.to_string()))?;
    let mut legacy_seen = index.is_none().then(BTreeSet::new);
    let mut failure = None;
    graphforge_storage::visit_node_fragments_admitted(
        project_dir,
        8192,
        limits.source_bytes,
        &mut source_evidence,
        |batch| {
            let result: Result<(), SearchArtifactError> = (|| {
                checkpoint()?;
                let uuids = batch
                    .column_by_name("node_uuid")
                    .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                    .ok_or_else(|| source("topology node_uuid is not FixedSizeBinary(16)"))?;
                let labels = batch
                    .column_by_name("type_ids")
                    .and_then(|column| column.as_any().downcast_ref::<ListArray>())
                    .ok_or_else(|| source("topology type_ids is not List<UInt32>"))?;
                let surrogates = batch
                    .column_by_name("node_id")
                    .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                    .ok_or_else(|| source("topology node_id is not UInt64"))?;
                let mut batch_uuids = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    let bytes: [u8; 16] = uuids
                        .value(row)
                        .try_into()
                        .map_err(|_| source("topology node_uuid is not 16 bytes"))?;
                    batch_uuids.push(uuid::Uuid::from_bytes(bytes));
                }
                let indexed = index
                    .as_mut()
                    .map(|index| index.lookup_node_surrogates(&batch_uuids))
                    .transpose()
                    .map_err(|error| source(error.to_string()))?
                    .map(|(values, _)| values);
                for row in 0..batch.num_rows() {
                    checkpoint()?;
                    rows = rows.saturating_add(1);
                    if rows > limits.topology_rows {
                        return Err(exhausted("vector_topology_rows", limits.topology_rows));
                    }
                    if uuids.is_null(row) || labels.is_null(row) || surrogates.is_null(row) {
                        return Err(source("topology contains null node identity data"));
                    }
                    let node_uuid: [u8; 16] = uuids
                        .value(row)
                        .try_into()
                        .map_err(|_| source("topology node_uuid is not 16 bytes"))?;
                    let surrogate = surrogates.value(row);
                    if last_surrogate.is_some_and(|prior| surrogate <= prior)
                        || indexed
                            .as_ref()
                            .is_some_and(|values| values[row] != Some(surrogate))
                        || legacy_seen
                            .as_mut()
                            .is_some_and(|seen| !seen.insert(node_uuid))
                    {
                        return Err(source(
                            "topology identity disagrees with authenticated UUID index",
                        ));
                    }
                    last_surrogate = Some(surrogate);
                    let values = labels.value(row);
                    let values = values
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .ok_or_else(|| source("topology type_ids child is not UInt32"))?;
                    if values.null_count() != 0 {
                        return Err(source("topology type_ids contains null labels"));
                    }
                    if values.values().contains(&label_id) {
                        eligible.insert(node_uuid);
                        if eligible.len() > limits.vector.eligible_nodes {
                            return Err(exhausted("eligible_nodes", limits.vector.eligible_nodes));
                        }
                    }
                }
                Ok(())
            })();
            if let Err(error) = result {
                failure = Some(error);
                return Ok(false);
            }
            Ok(true)
        },
    )
    .map_err(|error| source(error.to_string()))?;
    if let Some(error) = failure {
        return Err(error);
    }
    if index
        .as_ref()
        .is_some_and(|index| rows as u64 != index.count(graphforge_storage::UuidIndexKind::Node))
    {
        return Err(source(
            "topology row count disagrees with authenticated UUID index",
        ));
    }
    let snapshot = SearchSourceSnapshot::from_admitted_files(
        project_dir,
        source_generation,
        &source_evidence,
    )?;
    Ok(LabelMemberProjection {
        members: eligible,
        snapshot,
    })
}

#[cfg(test)]
pub(crate) fn capture_topology_snapshot<C>(
    project_dir: &Path,
    limits: VectorLifecycleLimits,
    mut checkpoint: C,
) -> Result<SearchSourceSnapshot, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    let paths = graphforge_storage::topology_node_files(project_dir)
        .map_err(|error| source(error.to_string()))?;
    let mut named = Vec::with_capacity(paths.len());
    for path in paths {
        checkpoint()?;
        let relative = path
            .strip_prefix(project_dir)
            .map_err(|_| source("topology source escaped project root"))?
            .to_str()
            .ok_or_else(|| source("topology source path is not UTF-8"))?
            .to_owned();
        named.push((relative, path));
    }
    checkpoint()?;
    SearchSourceSnapshot::capture_files(
        project_dir,
        &named,
        limits.source_bytes,
        "vector_source_bytes",
    )
}

fn validate_requested_artifact(
    artifact: &PublishedSearchArtifact,
    key: &SearchArtifactKey,
) -> Result<(), SearchArtifactError> {
    let manifest = &artifact.manifest;
    if manifest.index_kind != key.kind()
        || manifest.label != key.label()
        || manifest.space.as_deref() != key.space()
        || manifest.properties.is_some()
        || manifest.backend_version != VECTOR_BACKEND_VERSION
        || manifest.contract_version != VECTOR_CONTRACT_VERSION
        || manifest.dimension.is_none()
        || !manifest.completed
    {
        return Err(SearchArtifactError::CorruptPrimaryVectors {
            path: artifact.path.clone(),
            reason: "vector manifest does not match the requested backend key".to_owned(),
        });
    }
    Ok(())
}

pub(crate) fn validate_result_limit(
    limit: usize,
    limits: VectorStoreLimits,
) -> Result<(), SearchArtifactError> {
    if limit == 0 {
        return Err(SearchArtifactError::InvalidSelector {
            field: "limit",
            reason: "must be greater than zero".to_owned(),
        });
    }
    if limit > limits.results {
        return Err(exhausted("search_results", limits.results));
    }
    Ok(())
}

fn exhausted(resource: &'static str, limit: usize) -> SearchArtifactError {
    exhausted_u64(resource, u64::try_from(limit).unwrap_or(u64::MAX))
}

fn exhausted_u64(resource: &'static str, limit: u64) -> SearchArtifactError {
    SearchArtifactError::ResourceExhausted { resource, limit }
}

fn source(reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::SourceSnapshot {
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashSet;

    use graphforge_core::uuid::Uuid;
    use graphforge_ir::{OntologyMode, TypeId};
    use graphforge_storage::{
        GraphWriter, SearchPublicationOutcome, delete_nodes, generation::bump_search_generation,
    };
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    use super::*;

    fn uuid(value: u8) -> Uuid {
        let mut bytes = [0_u8; 16];
        bytes[15] = value;
        Uuid::from_bytes(bytes)
    }

    fn bytes(value: u8) -> [u8; 16] {
        *uuid(value).as_bytes()
    }

    fn request() -> VectorIndexRequest<'static> {
        VectorIndexRequest {
            label: "Person",
            label_id: 9,
            space: "semantic",
        }
    }

    #[test]
    fn public_result_limit_validation_is_exact_at_zero_boundary_and_cap() {
        let limits = VectorStoreLimits::default();
        assert!(matches!(
            validate_result_limit(0, limits),
            Err(SearchArtifactError::InvalidSelector { field: "limit", .. })
        ));
        validate_result_limit(limits.results, limits).unwrap();
        assert!(matches!(
            validate_result_limit(limits.results + 1, limits),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "search_results",
                ..
            })
        ));
    }

    fn write_members(dir: &TempDir, members: &[(u8, Vec<u32>)]) {
        let mut writer = GraphWriter::open_at(dir.path(), OntologyMode::Strict, 1).unwrap();
        for (value, labels) in members {
            writer
                .create_node_with_labels(
                    uuid(*value),
                    &labels.iter().copied().map(TypeId).collect::<Vec<_>>(),
                )
                .unwrap();
        }
        writer.flush().unwrap();
    }

    fn corrupt_node_surrogate_to_null(root: &Path) {
        let path = graphforge_storage::topology_node_files(root)
            .unwrap()
            .remove(0);
        let batch = graphforge_storage::read_nodes(root).unwrap().remove(0);
        let mut fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        let index = batch.schema().index_of("node_id").unwrap();
        fields[index] = fields[index].clone().with_nullable(true);
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
        let mut columns = batch.columns().to_vec();
        columns[index] = std::sync::Arc::new(arrow::array::UInt64Array::from(vec![None]));
        let corrupted = arrow::array::RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&corrupted).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn vector_membership_rejects_null_node_surrogate_explicitly() {
        let dir = TempDir::new().unwrap();
        write_members(&dir, &[(1, vec![9])]);
        corrupt_node_surrogate_to_null(dir.path());
        std::fs::remove_dir_all(dir.path().join(".graphforge-cache/uuid-membership")).unwrap();
        assert!(matches!(
            project_label_members(dir.path(), 9, VectorLifecycleLimits::default(), || Ok(())),
            Err(SearchArtifactError::SourceSnapshot { .. })
        ));
    }

    #[test]
    fn topology_snapshot_and_byte_limit_cover_every_node_shard() {
        let dir = TempDir::new().unwrap();
        for ordinal in 1_u8..=2 {
            let mut writer =
                GraphWriter::open_at(dir.path(), OntologyMode::Strict, i64::from(ordinal)).unwrap();
            writer.create_node(uuid(ordinal), TypeId(9)).unwrap();
            writer.flush().unwrap();
        }
        let paths = graphforge_storage::topology_node_files(dir.path()).unwrap();
        assert_eq!(paths.len(), 2);
        let before =
            capture_topology_snapshot(dir.path(), VectorLifecycleLimits::default(), || Ok(()))
                .unwrap();
        use std::io::Write as _;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&paths[1])
            .unwrap()
            .write_all(b"changed without generation")
            .unwrap();
        let after =
            capture_topology_snapshot(dir.path(), VectorLifecycleLimits::default(), || Ok(()))
                .unwrap();
        assert_eq!(before.generation, after.generation);
        assert_ne!(before.fingerprint, after.fingerprint);

        let mut limits = VectorLifecycleLimits::default();
        limits.source_bytes = std::fs::metadata(&paths[0]).unwrap().len();
        assert!(matches!(
            capture_topology_snapshot(dir.path(), limits, || Ok(())),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "vector_source_bytes",
                ..
            })
        ));
    }

    #[test]
    fn upsert_reopen_idempotence_replacement_and_secondary_membership() {
        let dir = TempDir::new().unwrap();
        write_members(&dir, &[(1, vec![1, 9])]);
        let first = upsert_graph_vector(
            dir.path(),
            request(),
            bytes(1),
            &[1.0, 0.0],
            11,
            VectorLifecycleLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert!(matches!(
            first,
            SearchPublicationOutcome::Published { attempts: 1, .. }
        ));

        let repeated = upsert_graph_vector(
            dir.path(),
            request(),
            bytes(1),
            &[1.0, 0.0],
            99,
            VectorLifecycleLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert!(matches!(repeated, SearchPublicationOutcome::Reused(_)));
        upsert_graph_vector(
            dir.path(),
            request(),
            bytes(1),
            &[0.0, 1.0],
            22,
            VectorLifecycleLimits::default(),
            || Ok(()),
        )
        .unwrap();
        let hits = search_graph_vectors(
            dir.path(),
            request(),
            &[0.0, 1.0],
            1,
            VectorLifecycleLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(hits[0].node_uuid, bytes(1));
        assert_eq!(hits[0].score, 1.0);
        assert!(matches!(
            upsert_graph_vector(
                dir.path(),
                request(),
                bytes(2),
                &[1.0, 0.0],
                33,
                VectorLifecycleLimits::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::InvalidSelector { field: "node", .. })
        ));
    }

    #[test]
    fn missing_is_empty_and_orphans_are_filtered_deterministically() {
        let dir = TempDir::new().unwrap();
        write_members(&dir, &[(1, vec![9]), (2, vec![9])]);
        assert!(
            search_graph_vectors(
                dir.path(),
                request(),
                &[1.0, 0.0],
                2,
                VectorLifecycleLimits::default(),
                || Ok(()),
            )
            .unwrap()
            .is_empty()
        );
        for value in [2, 1] {
            upsert_graph_vector(
                dir.path(),
                request(),
                bytes(value),
                &[1.0, 0.0],
                i64::from(value),
                VectorLifecycleLimits::default(),
                || Ok(()),
            )
            .unwrap();
        }
        assert_eq!(
            delete_nodes(dir.path(), &HashSet::from([bytes(2)])).unwrap(),
            1
        );
        let hits = search_graph_vectors(
            dir.path(),
            request(),
            &[1.0, 0.0],
            2,
            VectorLifecycleLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(
            hits.iter().map(|hit| hit.node_uuid).collect::<Vec<_>>(),
            [bytes(1)]
        );
    }

    #[test]
    fn membership_limits_cancellation_and_repeated_mutation_fail_closed() {
        let dir = TempDir::new().unwrap();
        write_members(&dir, &[(1, vec![9])]);
        let limited = VectorLifecycleLimits {
            topology_rows: 0,
            ..Default::default()
        };
        assert!(matches!(
            search_graph_vectors(dir.path(), request(), &[1.0], 1, limited, || Ok(())),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "vector_topology_rows",
                ..
            })
        ));
        assert!(matches!(
            search_graph_vectors(
                dir.path(),
                request(),
                &[1.0],
                1,
                VectorLifecycleLimits::default(),
                || Err(SearchArtifactError::Cancelled),
            ),
            Err(SearchArtifactError::Cancelled)
        ));

        let checks = Cell::new(0_u8);
        let raced = search_graph_vectors(
            dir.path(),
            request(),
            &[1.0],
            1,
            VectorLifecycleLimits::default(),
            || {
                checks.set(checks.get().saturating_add(1));
                bump_search_generation(dir.path()).unwrap();
                Ok(())
            },
        );
        assert!(matches!(
            raced,
            Err(SearchArtifactError::ConcurrentMutation)
        ));
    }

    #[test]
    fn corrupt_primary_vectors_are_not_rebuilt() {
        let dir = TempDir::new().unwrap();
        write_members(&dir, &[(1, vec![9])]);
        let outcome = upsert_graph_vector(
            dir.path(),
            request(),
            bytes(1),
            &[1.0],
            1,
            VectorLifecycleLimits::default(),
            || Ok(()),
        )
        .unwrap();
        let artifact = match outcome {
            SearchPublicationOutcome::Published { artifact, .. }
            | SearchPublicationOutcome::Reused(artifact) => artifact,
        };
        std::fs::write(
            artifact.path.join(graphforge_storage::VECTOR_DATA_FILE),
            b"not parquet",
        )
        .unwrap();
        assert!(matches!(
            search_graph_vectors(
                dir.path(),
                request(),
                &[1.0],
                1,
                VectorLifecycleLimits::default(),
                || Ok(()),
            ),
            Err(SearchArtifactError::CorruptPrimaryVectors { .. })
        ));
    }
}
