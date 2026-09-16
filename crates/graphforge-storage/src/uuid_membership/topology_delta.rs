//! UUID topology delta preparation, commit, and reconciliation.

use super::AuthenticatedUuidIndexSnapshot;
use super::BULK_IO_BYTES;
use super::BlockRecord;
use super::CommittedUuidTopologyRewrite;
use super::DEFAULT_ORPHAN_GC_LIMIT;
#[cfg(test)]
use super::FAIL_AFTER_MANIFEST_SUSPEND;
use super::FORMAT_VERSION;
use super::FileRecord;
use super::IDENTITY_RECORD_BYTES;
use super::IDENTITY_RECORD_WIDTH;
use super::INDEX_DIR;
use super::MANIFEST;
use super::MAX_MANIFEST_BYTES;
use super::Manifest;
use super::NODE_LOOKUP_RECORD_BYTES;
#[cfg(test)]
use super::OpenRun;
use super::PreparedUuidIndexDelta;
use super::PreparedV4OrdinalDelta;
use super::RunRecord;
use super::TOPOLOGY_RECEIPT;
use super::TopologyIndexReceipt;
use super::UuidIndexAppendMetrics;
use super::UuidIndexBuildLimits;
use super::UuidIndexBuildMetrics;
use super::UuidIndexKind;
use super::UuidIndexOrphanGcWork;
use super::UuidTopologyDelta;
use super::V4_ORDINAL_BLOCK_BYTES;
use super::V4_ORDINAL_MANIFEST;
use super::V4_ORDINAL_RECEIPT;
use super::V4OrdinalAppendMetrics;
use super::block_matches;
use super::create_uuid_file;
use super::describe_run;
use super::hex_bytes;
use super::identity_codec;
use super::injected_snapshot_refresh_failure;
use super::maintenance::authenticated_v3_membership_authority;
#[cfg(test)]
use super::maintenance::cleanup_superseded_files;
use super::maintenance::collect_uuid_orphans_locked;
use super::maintenance::manifest_file_names;
use super::maintenance::selected_generation_for_graph_root;
use super::maintenance::standalone_v4_pinned_update;
use super::open_uuid_child_file;
use super::open_uuid_file;
#[cfg(test)]
use super::open_verified;
use super::ordinal_artifacts::V4AuthorityTransactionProof;
use super::ordinal_artifacts::V4ConstructionArtifactBundle;
use super::ordinal_artifacts::V4OrdinalConstructionWriter;
use super::ordinal_artifacts::admit_v4_construction_manifest;
use super::ordinal_artifacts::clone_pinned_v4_file;
use super::ordinal_artifacts::commit_v4_publications;
use super::ordinal_artifacts::retain_v4_publication;
use super::ordinal_artifacts::v4_manifest_artifact_names;
use super::ordinal_artifacts::write_v4_tombstone_artifact;
use super::ordinal_compaction::compact_v4_binary_carry;
use super::ordinal_compaction::read_v4_forward_record;
use super::rebuild::build_surrogate_run;
use super::rebuild::ensure_uuid_membership_migrated;
use super::rebuild::flush_entity_surrogate_run;
use super::rebuild::merge_node_surrogate_runs;
#[cfg(test)]
use super::rebuild::merge_surrogate_runs;
#[cfg(test)]
use super::rebuild::publish_data;
use super::rebuild::read_exact_record;
use super::rebuild::read_node_surrogate_record;
use super::rebuild::read_surrogate_record;
use super::record_length;
use super::reject_retained_identity_collisions;
use super::reject_retained_surrogate_collisions;
use super::storage_err;
use super::sync_uuid_file;
use super::uuid_membership_index_present;
use super::v4_publication_failure;
use super::validate_block_records;
#[cfg(test)]
use super::validate_run_contents;
use super::validate_run_descriptors;
use graphforge_core::GfError;
use sha2::Digest;
use sha2::Sha256;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

/// Commit topology and its UUID participant under the one durable rewrite lock.
#[allow(clippy::too_many_lines)] // One sealed participant lifecycle; order is the invariant.
pub(crate) fn commit_uuid_topology_rewrite(
    project_dir: &Path,
    staged: crate::staging::RewriteBatch,
    delta: &UuidTopologyDelta,
    snapshot: &mut Option<AuthenticatedUuidIndexSnapshot>,
) -> Result<CommittedUuidTopologyRewrite, GfError> {
    let delta_is_empty = delta.nodes.is_empty()
        && delta.edges.is_empty()
        && delta.deleted_nodes.is_empty()
        && delta.deleted_edges.is_empty();
    if delta_is_empty && staged.is_empty() {
        return Ok(CommittedUuidTopologyRewrite::NoTopologyChange);
    }
    ensure_uuid_membership_migrated(project_dir)?;
    let selected = selected_generation_for_graph_root(project_dir)?;
    let ordinal_authority = selected
        .as_ref()
        .map(crate::ResolvedProjectGeneration::authenticated_v4_ordinal_authority)
        .transpose()?
        .flatten();
    let ordinal_inputs = ordinal_authority
        .as_ref()
        .map(|authority| {
            match authority
                .open(project_dir, crate::V4OrdinalIdentityLimits::default())
                .map_err(storage_err)?
            {
                crate::V4OrdinalIdentityOpen::Ready(handle) => {
                    handle.pinned_update_inputs().map(Some).map_err(storage_err)
                }
                crate::V4OrdinalIdentityOpen::RebuildRequired { .. } => Err(storage_err(
                    "v4 ordinal identity requires rebuild before mutation",
                )),
            }
        })
        .transpose()?
        .flatten();
    let ordinal_inputs = if ordinal_inputs.is_some() {
        ordinal_inputs
    } else if selected.is_none() {
        standalone_v4_pinned_update(project_dir, crate::read_topology_generation(project_dir)?)?
    } else {
        None
    };
    let membership_authority = selected
        .as_ref()
        .map(authenticated_v3_membership_authority)
        .transpose()?
        .flatten();
    let prepared = std::rc::Rc::new(std::cell::RefCell::new(None));
    let prepared_from_callback = std::rc::Rc::clone(&prepared);
    let prepared_v4 = std::rc::Rc::new(std::cell::RefCell::new(None));
    let prepared_v4_from_callback = std::rc::Rc::clone(&prepared_v4);
    let generations = std::rc::Rc::new(std::cell::Cell::new(None));
    let generations_from_callback = std::rc::Rc::clone(&generations);
    let root = project_dir.to_path_buf();
    let expected_root = root.clone();
    let participant: crate::durable_rewrite::RewriteParticipantPreparer<'_> =
        Box::new(|context, batch| {
            if context.project_root != expected_root {
                return Err(storage_err("rewrite participant project root changed"));
            }
            context.project.revalidate_named().map_err(storage_err)?;
            generations_from_callback.set(Some((context.prior, context.next)));
            if context.next.topology == context.prior.topology {
                if !delta_is_empty {
                    return Err(storage_err(
                        "UUID identity delta did not stage a topology transition",
                    ));
                }
                return Ok(None);
            }
            // Standalone graph roots have no selected project-generation
            // inventory capable of authenticating reachability. Topology
            // publication remains valid there, but orphan deletion must be
            // conservatively deferred rather than self-authorizing the live
            // manifest. Project-generation roots retain the authenticated GC
            // path, including the v3/v4 union.
            let orphan_gc = if membership_authority.is_some() {
                collect_uuid_orphans_locked(
                    context.project,
                    context.project_root,
                    DEFAULT_ORPHAN_GC_LIMIT,
                    membership_authority.as_ref(),
                    ordinal_authority.as_ref(),
                )?
            } else {
                UuidIndexOrphanGcWork::default()
            };
            if !uuid_membership_index_present(context.project_root) {
                return Err(storage_err(
                    "UUID membership index migration is required before topology mutation",
                ));
            }
            if snapshot
                .as_ref()
                .is_none_or(|value| value.topology_generation() != context.prior.topology)
            {
                *snapshot = Some(AuthenticatedUuidIndexSnapshot::open_at_generation(
                    context.project_root,
                    context.prior.topology,
                )?);
            }
            let (deleted_nodes, deleted_edges) = if let Some(index) = snapshot.as_mut() {
                let (surrogates, _) = index.lookup_node_surrogates(&delta.deleted_nodes)?;
                let nodes = delta
                    .deleted_nodes
                    .iter()
                    .copied()
                    .zip(surrogates)
                    .filter_map(|(uuid, surrogate)| surrogate.map(|id| (uuid, id)))
                    .collect::<Vec<_>>();
                let (present, _) = index.probe(UuidIndexKind::Edge, &delta.deleted_edges)?;
                let edges = delta
                    .deleted_edges
                    .iter()
                    .copied()
                    .zip(present)
                    .filter_map(|(uuid, present)| present.then_some(uuid))
                    .collect::<Vec<_>>();
                (nodes, edges)
            } else {
                (Vec::new(), Vec::new())
            };
            let mut token = super::prepare_uuid_membership_delta(
                context.project_root,
                context.prior.topology,
                context.next.topology,
                snapshot.as_mut(),
                batch,
                &delta.nodes,
                &delta.edges,
                &deleted_nodes,
                &deleted_edges,
            )?;
            if let Some(token) = token.as_mut() {
                token.metrics.orphan_gc_candidates = orphan_gc.candidates;
                token.metrics.orphan_gc_removed = orphan_gc.removed;
                token.metrics.orphan_gc_deferred = orphan_gc.deferred;
                token.metrics.orphan_gc_deferred_limit = orphan_gc.deferred_limit;
                token.metrics.orphan_gc_deferred_linked = orphan_gc.deferred_linked;
                token.metrics.orphan_gc_bytes = orphan_gc.bytes;
            }
            let receipt = token
                .as_ref()
                .map(PreparedUuidIndexDelta::auxiliary_receipt);
            let mut prepared_ordinal = ordinal_inputs
                .as_ref()
                .map(|pinned| {
                    let deleted_node_ids = deleted_nodes
                        .iter()
                        .map(|(_, node_id)| *node_id)
                        .collect::<Vec<_>>();
                    super::prepare_v4_ordinal_delta(
                        context.project_root,
                        context.prior.topology,
                        context.next.topology,
                        pinned,
                        batch,
                        &delta.nodes,
                        &deleted_node_ids,
                        &topology_delta_sha256(
                            &delta.nodes,
                            &delta.edges,
                            &deleted_nodes,
                            &deleted_edges,
                        ),
                    )
                })
                .transpose()?;
            if let Some(token) = prepared_ordinal.as_mut() {
                token.metrics.orphan_gc_candidates = orphan_gc.candidates;
                token.metrics.orphan_gc_removed = orphan_gc.removed;
                token.metrics.orphan_gc_deferred = orphan_gc.deferred;
                token.metrics.orphan_gc_bytes = orphan_gc.bytes;
            }
            let receipt = prepared_ordinal
                .as_ref()
                .map(PreparedV4OrdinalDelta::auxiliary_receipt)
                .or(receipt);
            if token.is_some()
                && let Some(value) = snapshot.as_mut()
            {
                value.suspend_owned_manifest();
            }
            *prepared_from_callback.borrow_mut() = token;
            *prepared_v4_from_callback.borrow_mut() = prepared_ordinal;
            #[cfg(test)]
            if FAIL_AFTER_MANIFEST_SUSPEND.replace(false) {
                return Err(storage_err(
                    "injected manifest suspension preparation error",
                ));
            }
            Ok(receipt)
        });
    let commit =
        crate::generation::commit_topology_aware_with_participant(staged, &root, participant);
    let token = prepared.borrow_mut().take();
    let v4_token = prepared_v4.borrow_mut().take();
    let committed = match commit {
        Ok(value) => value,
        Err(error) => {
            let outcome = (|| {
                let (Some(token), Some((prior, next))) = (token.as_ref(), generations.get()) else {
                    return Ok(None);
                };
                let membership = reconcile_uuid_auxiliary(&root, prior, next, token)?;
                if let Some(v4) = v4_token.as_ref() {
                    let ordinal = reconcile_v4_ordinal_auxiliary(&root, prior, next, v4)?;
                    if membership != ordinal {
                        return Err(storage_err(
                            "v3 and v4 auxiliary reconciliation outcomes disagree",
                        ));
                    }
                }
                Ok(Some((membership, next.topology)))
            })();
            match outcome {
                Ok(Some((
                    crate::durable_rewrite::AuxiliaryReconcileOutcome::Committed,
                    generation,
                ))) => Some(generation),
                Ok(Some((crate::durable_rewrite::AuxiliaryReconcileOutcome::NotCommitted, _))) => {
                    if let Some(value) = snapshot.as_mut()
                        && let Err(restore) = value.restore_owned_manifest()
                    {
                        *snapshot = None;
                        return Err(storage_err(format!(
                            "{error}; UUID snapshot restoration failed: {restore}"
                        )));
                    }
                    return Err(error);
                }
                Err(reconcile) => {
                    if snapshot
                        .as_ref()
                        .is_some_and(|value| value.manifest_file.is_none())
                    {
                        *snapshot = None;
                    }
                    return Err(storage_err(format!(
                        "{error}; UUID reconciliation failed: {reconcile}"
                    )));
                }
                Ok(None) => {
                    if snapshot
                        .as_ref()
                        .is_some_and(|value| value.manifest_file.is_none())
                    {
                        *snapshot = None;
                    }
                    return Err(error);
                }
            }
        }
    };
    let mut committed_metrics = UuidIndexAppendMetrics::default();
    let committed_v4_metrics = v4_token.as_ref().map(|token| token.metrics().clone());
    if let (Some(generation), Some(token)) = (committed, token.as_ref()) {
        if let Err(error) = token.verify_generation(generation).and_then(|()| {
            v4_token
                .as_ref()
                .map_or(Ok(()), |v4| v4.verify_generation(generation))
        }) {
            *snapshot = None;
            return Err(error);
        }
        committed_metrics = token.metrics().clone();
        let refresh = injected_snapshot_refresh_failure().map_or_else(
            || {
                if let Some(value) = snapshot.as_mut() {
                    token.advance_snapshot(value).map(|_| ())
                } else {
                    AuthenticatedUuidIndexSnapshot::open_at_generation(&root, generation)
                        .map(|value| *snapshot = Some(value))
                }
            },
            Err,
        );
        if let Err(error) = refresh {
            *snapshot = None;
            return Ok(CommittedUuidTopologyRewrite::CommittedNeedsRefresh {
                generation,
                metrics: committed_metrics,
                v4_metrics: committed_v4_metrics,
                error,
            });
        }
    }
    Ok(committed.map_or(
        CommittedUuidTopologyRewrite::NoTopologyChange,
        |generation| CommittedUuidTopologyRewrite::Committed {
            generation,
            metrics: committed_metrics,
            v4_metrics: committed_v4_metrics,
        },
    ))
}

/// Commit a topology rewrite that changes no UUID membership while advancing
/// the authenticated membership manifest to the new topology generation.
pub(crate) fn commit_uuid_neutral_topology_rewrite(
    project_dir: &Path,
    staged: crate::staging::RewriteBatch,
) -> Result<Option<u64>, GfError> {
    let mut snapshot = None;
    match commit_uuid_topology_rewrite(
        project_dir,
        staged,
        &UuidTopologyDelta {
            nodes: Vec::new(),
            edges: Vec::new(),
            deleted_nodes: Vec::new(),
            deleted_edges: Vec::new(),
        },
        &mut snapshot,
    )? {
        CommittedUuidTopologyRewrite::NoTopologyChange => Ok(None),
        CommittedUuidTopologyRewrite::Committed { generation, .. } => Ok(Some(generation)),
        CommittedUuidTopologyRewrite::CommittedNeedsRefresh {
            generation, error, ..
        } => Err(GfError::Storage(format!(
            "topology generation {generation} committed but UUID index snapshot refresh failed: {error}"
        ))),
    }
}

/// Stage one bounded v3 UUID-index delta and its authenticated receipt into the
/// caller's generation-last topology rewrite transaction.
#[allow(clippy::too_many_arguments)] // Mirrors the four disjoint UUID delta domains.
pub(crate) fn prepare_uuid_membership_delta(
    project_dir: &Path,
    current: u64,
    generation: u64,
    snapshot: Option<&mut AuthenticatedUuidIndexSnapshot>,
    batch: &mut crate::staging::RewriteBatch,
    nodes: &[(Uuid, u64)],
    edges: &[Uuid],
    deleted_nodes: &[(Uuid, u64)],
    deleted_edges: &[Uuid],
) -> Result<Option<PreparedUuidIndexDelta>, GfError> {
    if generation != current.saturating_add(1) {
        return Err(storage_err(
            "prepared UUID delta generation is not the next generation",
        ));
    }
    let source_root = project_dir.join(INDEX_DIR);
    if current != 0 && !source_root.join(MANIFEST).is_file() {
        return Err(storage_err(
            "UUID membership index migration is required before topology mutation",
        ));
    }
    fs::create_dir_all(&source_root).map_err(storage_err)?;
    let parent = project_dir
        .parent()
        .ok_or_else(|| storage_err("project directory has no staging parent"))?;
    let scratch = tempfile::Builder::new()
        .prefix("uuid-membership-plan-")
        .tempdir_in(parent)
        .map_err(storage_err)?;
    let (manifest, outputs, _superseded, metrics) = plan_uuid_membership_delta(
        &source_root,
        current,
        generation,
        snapshot,
        scratch.path(),
        nodes,
        edges,
        deleted_nodes,
        deleted_edges,
    )?;
    for (record, path) in outputs {
        batch.stage_file(&source_root.join(record.name), &path)?;
    }
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(storage_err)?;
    batch.stage_bytes(&source_root.join(MANIFEST), &manifest_bytes)?;
    let nonce = Uuid::new_v4().simple().to_string();
    let receipt = TopologyIndexReceipt {
        nonce,
        expected_generation: generation,
        topology_delta_sha256: topology_delta_sha256(nodes, edges, deleted_nodes, deleted_edges),
        manifest_sha256: hex_sha256(&manifest_bytes),
    };
    let receipt_bytes = serde_json::to_vec(&receipt).map_err(storage_err)?;
    let receipt_path = source_root.join(TOPOLOGY_RECEIPT);
    batch.stage_bytes(&receipt_path, &receipt_bytes)?;
    let digest = Sha256::digest(&receipt_bytes);
    Ok(Some(PreparedUuidIndexDelta {
        expected_generation: generation,
        metrics,
        manifest,
        auxiliary: crate::AuxiliaryReceipt {
            kind: "uuid-membership/v5".to_owned(),
            schema_version: FORMAT_VERSION,
            path: format!("{INDEX_DIR}/{TOPOLOGY_RECEIPT}"),
            digest: hex_bytes(&digest),
            bytes: receipt_bytes.len() as u64,
        },
    }))
}

pub(super) fn hex_sha256(bytes: &[u8]) -> String {
    hex_bytes(&Sha256::digest(bytes))
}

fn reconcile_uuid_auxiliary(
    project_dir: &Path,
    prior: crate::durable_rewrite::GenerationPair,
    next: crate::durable_rewrite::GenerationPair,
    prepared: &PreparedUuidIndexDelta,
) -> Result<crate::durable_rewrite::AuxiliaryReconcileOutcome, GfError> {
    let auxiliary = prepared.auxiliary_receipt();
    let outcome =
        crate::durable_rewrite::reconcile_auxiliary(project_dir, prior, next, &auxiliary)?;
    if outcome == crate::durable_rewrite::AuxiliaryReconcileOutcome::NotCommitted {
        return Ok(outcome);
    }
    let project = graphforge_filesystem::StableDirectory::open(project_dir).map_err(storage_err)?;
    let topology = project
        .open_child_directory(std::ffi::OsStr::new("topology"))
        .map_err(storage_err)?;
    let index = topology
        .open_child_directory(std::ffi::OsStr::new("uuid-membership"))
        .map_err(storage_err)?;
    let mut receipt_file = index
        .open_child_file(std::ffi::OsStr::new(TOPOLOGY_RECEIPT))
        .map_err(storage_err)?;
    let receipt_body = read_bounded(&mut receipt_file, MAX_MANIFEST_BYTES)?;
    let receipt: TopologyIndexReceipt =
        serde_json::from_slice(&receipt_body).map_err(storage_err)?;
    let mut manifest_file = index
        .open_child_file(std::ffi::OsStr::new(MANIFEST))
        .map_err(storage_err)?;
    let manifest_body = read_bounded(&mut manifest_file, MAX_MANIFEST_BYTES)?;
    project.revalidate_named().map_err(storage_err)?;
    topology.revalidate_named().map_err(storage_err)?;
    index.revalidate_named().map_err(storage_err)?;
    if receipt.expected_generation != next.topology
        || receipt.manifest_sha256 != hex_sha256(&manifest_body)
        || receipt.manifest_sha256
            != hex_sha256(&serde_json::to_vec(&prepared.manifest).map_err(storage_err)?)
    {
        return Err(storage_err(
            "committed UUID receipt does not authenticate the expected manifest",
        ));
    }
    Ok(outcome)
}

fn reconcile_v4_ordinal_auxiliary(
    project_dir: &Path,
    prior: crate::durable_rewrite::GenerationPair,
    next: crate::durable_rewrite::GenerationPair,
    prepared: &PreparedV4OrdinalDelta,
) -> Result<crate::durable_rewrite::AuxiliaryReconcileOutcome, GfError> {
    let outcome = crate::durable_rewrite::reconcile_auxiliary(
        project_dir,
        prior,
        next,
        &prepared.auxiliary_receipt(),
    )?;
    if outcome == crate::durable_rewrite::AuxiliaryReconcileOutcome::NotCommitted {
        return Ok(outcome);
    }
    let index = graphforge_filesystem::StableDirectory::open(&project_dir.join(INDEX_DIR))
        .map_err(storage_err)?;
    let mut receipt_file = index
        .open_child_file(std::ffi::OsStr::new(V4_ORDINAL_RECEIPT))
        .map_err(storage_err)?;
    let receipt_body = read_bounded(
        &mut receipt_file,
        crate::ordinal_identity_v4::MAX_MANIFEST_BYTES,
    )?;
    let receipt: TopologyIndexReceipt =
        serde_json::from_slice(&receipt_body).map_err(storage_err)?;
    let mut manifest_file = index
        .open_child_file(std::ffi::OsStr::new(V4_ORDINAL_MANIFEST))
        .map_err(storage_err)?;
    let manifest_body = read_bounded(
        &mut manifest_file,
        crate::ordinal_identity_v4::MAX_MANIFEST_BYTES,
    )?;
    index.revalidate_named().map_err(storage_err)?;
    let expected = serde_json::to_vec(&prepared.manifest).map_err(storage_err)?;
    if receipt.expected_generation != next.topology
        || receipt.manifest_sha256 != hex_sha256(&manifest_body)
        || manifest_body != expected
    {
        return Err(storage_err(
            "committed v4 ordinal receipt does not authenticate the expected manifest",
        ));
    }
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity, clippy::too_many_lines)] // Pure planner returns its authenticated transaction bundle.
pub(super) fn plan_uuid_membership_delta(
    root: &Path,
    current: u64,
    generation: u64,
    mut snapshot: Option<&mut AuthenticatedUuidIndexSnapshot>,
    scratch: &Path,
    nodes: &[(Uuid, u64)],
    edges: &[Uuid],
    deleted_nodes: &[(Uuid, u64)],
    deleted_edges: &[Uuid],
) -> Result<
    (
        Manifest,
        Vec<(FileRecord, PathBuf)>,
        Vec<(String, graphforge_filesystem::FileIdentity)>,
        UuidIndexAppendMetrics,
    ),
    GfError,
> {
    let mut manifest = if let Some(retained) = snapshot.as_deref_mut() {
        retained.revalidate()?;
        if retained.manifest.current_generation != current {
            return Err(storage_err("retained manifest generation is stale"));
        }
        retained.manifest.clone()
    } else if current == 0 {
        Manifest {
            format_version: FORMAT_VERSION,
            base_generation: 0,
            current_generation: 0,
            live_node_count: 0,
            live_edge_count: 0,
            runs: Vec::new(),
        }
    } else {
        return Err(storage_err("authenticated UUID snapshot is required"));
    };

    let prior_names = manifest_file_names(&manifest);
    let mut identities = nodes
        .iter()
        .map(|(uuid, id)| (*uuid, 0_u8, *id))
        .chain(edges.iter().map(|uuid| (*uuid, 1_u8, 0)))
        .chain(deleted_nodes.iter().map(|(uuid, id)| (*uuid, 2_u8, *id)))
        .chain(deleted_edges.iter().map(|uuid| (*uuid, 3_u8, 0)))
        .collect::<Vec<_>>();
    identities.sort_unstable_by_key(|entry| *entry.0.as_bytes());
    if identities.windows(2).any(|pair| pair[0].0 == pair[1].0)
        || nodes.iter().any(|(_, id)| *id == 0)
    {
        return Err(storage_err(
            "new identity run contains duplicate/invalid identity",
        ));
    }
    let mut surrogates = nodes
        .iter()
        .map(|(uuid, id)| (*id, *uuid))
        .chain(deleted_nodes.iter().map(|(uuid, id)| (*id, *uuid)))
        .collect::<Vec<_>>();
    surrogates.sort_unstable();
    if surrogates.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(storage_err("new node run contains duplicate surrogate"));
    }

    let (retained_authentication_bytes, retained_authentication_blocks) =
        snapshot.as_deref_mut().map_or(
            (0, 0),
            AuthenticatedUuidIndexSnapshot::take_authentication_work,
        );
    let mut validation_metrics = UuidIndexAppendMetrics::default();
    if let Some(retained) = snapshot.as_deref_mut() {
        for run in &mut retained.runs {
            reject_retained_identity_collisions(run, &identities, &mut validation_metrics)?;
            reject_retained_surrogate_collisions(run, &surrogates, &mut validation_metrics)?;
        }
    }

    let identity_path = scratch.join("identities-l0.run");
    let surrogate_path = scratch.join("surrogates-l0.run");
    let identity_write_blocks = write_identity_records(&identity_path, &identities)?;
    let surrogate_write_blocks = write_surrogate_records(&surrogate_path, &surrogates)?;
    let identity_record = describe_run(
        &identity_path,
        "identities-v5",
        generation,
        IDENTITY_RECORD_BYTES,
    )?;
    let surrogate_record = describe_run(
        &surrogate_path,
        "node-surrogates-v5",
        generation,
        NODE_LOOKUP_RECORD_BYTES,
    )?;
    let mut sources = HashMap::from([
        (identity_record.name.clone(), identity_path),
        (surrogate_record.name.clone(), surrogate_path),
    ]);
    if current == 0 && manifest.runs.is_empty() {
        let empty_identity_path = scratch.join("identities-base.run");
        let empty_surrogate_path = scratch.join("surrogates-base.run");
        let empty_identity = create_uuid_file(&empty_identity_path)?;
        sync_uuid_file(&empty_identity)?;
        let empty_surrogate = create_uuid_file(&empty_surrogate_path)?;
        sync_uuid_file(&empty_surrogate)?;
        let base_identities = describe_run(
            &empty_identity_path,
            "identities-v5-base",
            0,
            IDENTITY_RECORD_BYTES,
        )?;
        let base_surrogates = describe_run(
            &empty_surrogate_path,
            "node-surrogates-v5-base",
            0,
            NODE_LOOKUP_RECORD_BYTES,
        )?;
        sources.insert(base_identities.name.clone(), empty_identity_path);
        sources.insert(base_surrogates.name.clone(), empty_surrogate_path);
        manifest.runs.push(RunRecord {
            base: true,
            level: 0,
            first_generation: 0,
            last_generation: 0,
            identities: base_identities,
            node_surrogates: base_surrogates,
            node_count: 0,
            edge_count: 0,
            deleted_node_count: 0,
            deleted_edge_count: 0,
        });
    }
    manifest.runs.push(RunRecord {
        base: false,
        level: 0,
        first_generation: generation,
        last_generation: generation,
        identities: identity_record,
        node_surrogates: surrogate_record,
        node_count: nodes.len() as u64,
        edge_count: edges.len() as u64,
        deleted_node_count: deleted_nodes.len() as u64,
        deleted_edge_count: deleted_edges.len() as u64,
    });
    let mut metrics = UuidIndexAppendMetrics {
        input_records: identities.len() as u64,
        physical_bytes_written: identities
            .iter()
            .map(|(_, kind, _)| if *kind == 1 { 17_u64 } else { 25_u64 })
            .sum::<u64>()
            + surrogates.len() as u64 * NODE_LOOKUP_RECORD_BYTES,
        write_bytes: identities
            .iter()
            .map(|(_, kind, _)| if *kind == 1 { 17_u64 } else { 25_u64 })
            .sum::<u64>()
            + surrogates.len() as u64 * NODE_LOOKUP_RECORD_BYTES,
        write_blocks: identity_write_blocks + surrogate_write_blocks,
        peak_buffered_records: identities.len() + surrogates.len(),
        peak_buffered_bytes: identities.len() * 32 + surrogates.len() * 24,
        validation_random_seeks: 0,
        validation_scan_bytes: validation_metrics.validation_scan_bytes,
        validation_scan_blocks: validation_metrics.validation_scan_blocks,
        snapshot_admission_authentication_bytes: retained_authentication_bytes,
        snapshot_admission_authentication_blocks: retained_authentication_blocks,
        ..Default::default()
    };
    compact_planned_levels(
        root,
        scratch,
        &mut manifest,
        &mut sources,
        snapshot.as_deref(),
        &mut metrics,
    )?;
    manifest.current_generation = generation;
    manifest.live_node_count = manifest
        .live_node_count
        .checked_add(nodes.len() as u64)
        .and_then(|v| v.checked_sub(deleted_nodes.len() as u64))
        .ok_or_else(|| storage_err("node live-count delta is invalid"))?;
    manifest.live_edge_count = manifest
        .live_edge_count
        .checked_add(edges.len() as u64)
        .and_then(|v| v.checked_sub(deleted_edges.len() as u64))
        .ok_or_else(|| storage_err("edge live-count delta is invalid"))?;
    manifest
        .runs
        .sort_unstable_by_key(|run| run.first_generation);
    validate_run_descriptors(&manifest)?;
    let retained = manifest_file_names(&manifest);
    let mut outputs = sources
        .into_iter()
        .filter(|(name, _)| retained.contains(name))
        .map(|(name, path)| {
            let record = manifest
                .runs
                .iter()
                .flat_map(|run| [&run.identities, &run.node_surrogates])
                .find(|record| record.name == name)
                .expect("planned output is retained")
                .clone();
            (record, path)
        })
        .collect::<Vec<_>>();
    outputs.sort_unstable_by(|left, right| left.0.name.cmp(&right.0.name));
    metrics.new_output_authentication_bytes = outputs
        .iter()
        .map(|(record, _)| {
            record
                .blocks
                .iter()
                .map(|block| u64::from(block.len))
                .sum::<u64>()
        })
        .sum();
    metrics.new_output_authentication_blocks = outputs
        .iter()
        .map(|(record, _)| record.blocks.len() as u64)
        .sum();
    let mut superseded = Vec::new();
    if let Some(snapshot) = snapshot.as_deref() {
        for name in prior_names.difference(&retained) {
            let file = open_uuid_child_file(&snapshot.root, std::ffi::OsStr::new(name))?;
            superseded.push((
                name.clone(),
                graphforge_filesystem::file_identity(&file).map_err(storage_err)?,
            ));
        }
    }
    metrics.retained_runs = manifest.runs.len();
    Ok((manifest, outputs, superseded, metrics))
}

fn compact_planned_levels(
    root: &Path,
    scratch: &Path,
    manifest: &mut Manifest,
    sources: &mut HashMap<String, PathBuf>,
    snapshot: Option<&AuthenticatedUuidIndexSnapshot>,
    metrics: &mut UuidIndexAppendMetrics,
) -> Result<(), GfError> {
    for level in 0..63_u8 {
        let mut indexes = manifest
            .runs
            .iter()
            .enumerate()
            .filter(|(_, run)| !run.base && run.level == level)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if indexes.len() < 2 {
            continue;
        }
        if indexes.len() != 2 {
            return Err(storage_err(
                "manifest has more than one retained run at a level",
            ));
        }
        indexes.sort_unstable_by_key(|index| manifest.runs[*index].first_generation);
        let right = manifest.runs.remove(indexes[1]);
        let left = manifest.runs.remove(indexes[0]);
        if left.last_generation.saturating_add(1) != right.first_generation {
            return Err(storage_err("equal-level runs are not adjacent"));
        }
        let identity_path = scratch.join(format!("identities-level-{}.run", level + 1));
        let surrogate_path = scratch.join(format!("surrogates-level-{}.run", level + 1));
        let identity_inputs = [
            (
                planned_file(root, sources, snapshot, &left.identities.name, true)?,
                left.identities.clone(),
            ),
            (
                planned_file(root, sources, snapshot, &right.identities.name, true)?,
                right.identities.clone(),
            ),
        ];
        let surrogate_inputs = [
            (
                planned_file(root, sources, snapshot, &left.node_surrogates.name, false)?,
                left.node_surrogates.clone(),
            ),
            (
                planned_file(root, sources, snapshot, &right.node_surrogates.name, false)?,
                right.node_surrogates.clone(),
            ),
        ];
        merge_identity_handles(identity_inputs, &identity_path, metrics)?;
        merge_surrogate_handles(surrogate_inputs, &surrogate_path, metrics)?;
        let identities = describe_run(
            &identity_path,
            &format!("identities-v5-l{}", level + 1),
            right.last_generation,
            IDENTITY_RECORD_BYTES,
        )?;
        let node_surrogates = describe_run(
            &surrogate_path,
            &format!("node-surrogates-v5-l{}", level + 1),
            right.last_generation,
            NODE_LOOKUP_RECORD_BYTES,
        )?;
        let bytes = record_length(&identities, IDENTITY_RECORD_BYTES)?
            + node_surrogates.count * NODE_LOOKUP_RECORD_BYTES;
        metrics.physical_bytes_written = metrics.physical_bytes_written.saturating_add(bytes);
        metrics.write_bytes = metrics.write_bytes.saturating_add(bytes);
        metrics.write_blocks = metrics
            .write_blocks
            .saturating_add(bytes.div_ceil(BULK_IO_BYTES as u64));
        let counts = count_identity_states(&identity_path)?;
        sources.insert(identities.name.clone(), identity_path);
        sources.insert(node_surrogates.name.clone(), surrogate_path);
        manifest.runs.push(RunRecord {
            base: false,
            level: level + 1,
            first_generation: left.first_generation,
            last_generation: right.last_generation,
            identities,
            node_surrogates,
            node_count: counts.0,
            edge_count: counts.1,
            deleted_node_count: counts.2,
            deleted_edge_count: counts.3,
        });
    }
    Ok(())
}

fn planned_file(
    root: &Path,
    sources: &HashMap<String, PathBuf>,
    snapshot: Option<&AuthenticatedUuidIndexSnapshot>,
    name: &str,
    identities: bool,
) -> Result<File, GfError> {
    if let Some(path) = sources.get(name) {
        return open_uuid_file(path);
    }
    if let Some(snapshot) = snapshot {
        for run in &snapshot.runs {
            if identities && run.descriptor.identities.name == name {
                return run.identities.try_clone().map_err(storage_err);
            }
            if !identities && run.descriptor.node_surrogates.name == name {
                return run.node_surrogates.try_clone().map_err(storage_err);
            }
        }
        return Err(storage_err("planned compaction input is not retained"));
    }
    open_uuid_file(&root.join(name))
}

struct VerifiedBlockReader {
    file: File,
    blocks: Vec<BlockRecord>,
    next: usize,
    bytes: Vec<u8>,
    cursor: usize,
    authenticated_bytes: u64,
    authenticated_blocks: u64,
    width: usize,
}

impl VerifiedBlockReader {
    fn new(file: File, record: &FileRecord, width: u64) -> Result<Self, GfError> {
        validate_block_records(record, width)?;
        Ok(Self {
            file,
            blocks: record.blocks.clone(),
            next: 0,
            bytes: Vec::new(),
            cursor: 0,
            authenticated_bytes: 0,
            authenticated_blocks: 0,
            width: usize::try_from(width)
                .map_err(|_| storage_err("record width does not fit address space"))?,
        })
    }
}

impl Read for VerifiedBlockReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if self.cursor == self.bytes.len() {
            let Some(block) = self.blocks.get(self.next) else {
                return Ok(0);
            };
            self.file.seek(SeekFrom::Start(block.offset))?;
            self.bytes.resize(block.len as usize, 0);
            self.file.read_exact(&mut self.bytes)?;
            if !block_matches(&self.bytes, block, self.width) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "UUID compaction block authentication failed",
                ));
            }
            self.next += 1;
            self.cursor = 0;
            self.authenticated_bytes = self
                .authenticated_bytes
                .saturating_add(self.bytes.len() as u64);
            self.authenticated_blocks = self.authenticated_blocks.saturating_add(1);
        }
        let available = &self.bytes[self.cursor..];
        let copied = available.len().min(output.len());
        output[..copied].copy_from_slice(&available[..copied]);
        self.cursor += copied;
        Ok(copied)
    }
}

fn merge_identity_handles(
    inputs: [(File, FileRecord); 2],
    output: &Path,
    metrics: &mut UuidIndexAppendMetrics,
) -> Result<(), GfError> {
    let mut readers = inputs
        .into_iter()
        .map(|(file, record)| VerifiedBlockReader::new(file, &record, IDENTITY_RECORD_BYTES))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<([u8; IDENTITY_RECORD_WIDTH], usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = identity_codec::read(reader)? {
            heap.push(Reverse((record, index)));
        }
    }
    let mut out = create_uuid_file(output)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    while let Some(Reverse((mut record, index))) = heap.pop() {
        let key: [u8; 16] = record[..16].try_into().expect("fixed");
        let mut newest = index;
        if let Some(next) = identity_codec::read(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
        while heap
            .peek()
            .is_some_and(|Reverse((candidate, _))| candidate[..16] == key)
        {
            let Reverse((candidate, source)) = heap.pop().expect("peeked");
            if source > newest {
                record = candidate;
                newest = source;
            }
            if let Some(next) = identity_codec::read(&mut readers[source])? {
                heap.push(Reverse((next, source)));
            }
        }
        if block.len() + IDENTITY_RECORD_WIDTH > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(identity_codec::encoded(&record)?);
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    sync_uuid_file(&out)?;
    for reader in readers {
        metrics.validation_scan_bytes = metrics
            .validation_scan_bytes
            .saturating_add(reader.authenticated_bytes);
        metrics.validation_scan_blocks = metrics
            .validation_scan_blocks
            .saturating_add(reader.authenticated_blocks);
    }
    Ok(())
}

fn merge_surrogate_handles(
    inputs: [(File, FileRecord); 2],
    output: &Path,
    metrics: &mut UuidIndexAppendMetrics,
) -> Result<(), GfError> {
    let mut readers = inputs
        .into_iter()
        .map(|(file, record)| VerifiedBlockReader::new(file, &record, NODE_LOOKUP_RECORD_BYTES))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<([u8; 24], usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = read_exact_record::<24>(reader)? {
            heap.push(Reverse((record, index)));
        }
    }
    let mut out = create_uuid_file(output)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    while let Some(Reverse((mut record, index))) = heap.pop() {
        let key: [u8; 8] = record[..8].try_into().expect("fixed");
        let mut newest = index;
        if let Some(next) = read_exact_record::<24>(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
        while heap
            .peek()
            .is_some_and(|Reverse((candidate, _))| candidate[..8] == key)
        {
            let Reverse((candidate, source)) = heap.pop().expect("peeked");
            if source > newest {
                record = candidate;
                newest = source;
            }
            if let Some(next) = read_exact_record::<24>(&mut readers[source])? {
                heap.push(Reverse((next, source)));
            }
        }
        if block.len() + 24 > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(&record);
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    sync_uuid_file(&out)?;
    for reader in readers {
        metrics.validation_scan_bytes = metrics
            .validation_scan_bytes
            .saturating_add(reader.authenticated_bytes);
        metrics.validation_scan_blocks = metrics
            .validation_scan_blocks
            .saturating_add(reader.authenticated_blocks);
    }
    Ok(())
}

pub(super) fn topology_delta_sha256(
    nodes: &[(Uuid, u64)],
    edges: &[Uuid],
    deleted_nodes: &[(Uuid, u64)],
    deleted_edges: &[Uuid],
) -> String {
    let mut nodes = nodes.to_vec();
    nodes.sort_unstable_by_key(|(uuid, _)| *uuid.as_bytes());
    let mut edges = edges.to_vec();
    edges.sort_unstable_by_key(|uuid| *uuid.as_bytes());
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge/uuid-index-topology-delta/v1");
    for (uuid, surrogate) in nodes {
        hasher.update([0]);
        hasher.update(uuid.as_bytes());
        hasher.update(surrogate.to_be_bytes());
    }
    for uuid in edges {
        hasher.update([1]);
        hasher.update(uuid.as_bytes());
    }
    let mut deleted_nodes = deleted_nodes.to_vec();
    deleted_nodes.sort_unstable_by_key(|(uuid, _)| *uuid.as_bytes());
    for (uuid, surrogate) in deleted_nodes {
        hasher.update([2]);
        hasher.update(uuid.as_bytes());
        hasher.update(surrogate.to_be_bytes());
    }
    let mut deleted_edges = deleted_edges.to_vec();
    deleted_edges.sort_unstable_by_key(|uuid| *uuid.as_bytes());
    for uuid in deleted_edges {
        hasher.update([3]);
        hasher.update(uuid.as_bytes());
    }
    let mut encoded = String::with_capacity(64);
    for byte in hasher.finalize() {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

pub(super) fn read_bounded(
    file: &mut (impl Read + Seek),
    maximum: u64,
) -> Result<Vec<u8>, GfError> {
    let length = file.seek(SeekFrom::End(0)).map_err(storage_err)?;
    file.seek(SeekFrom::Start(0)).map_err(storage_err)?;
    if length > maximum {
        return Err(storage_err("recovery control record exceeds size limit"));
    }
    let capacity = usize::try_from(length)
        .map_err(|_| storage_err("control record length does not fit address space"))?;
    let mut body = Vec::with_capacity(capacity);
    file.read_to_end(&mut body).map_err(storage_err)?;
    Ok(body)
}

/// Publish one committed topology batch as an immutable authenticated v3 run.
#[cfg(test)]
pub(crate) fn append_uuid_membership_delta(
    project_dir: &Path,
    generation: u64,
    nodes: &[(Uuid, u64)],
    edges: &[Uuid],
) -> Result<UuidIndexAppendMetrics, GfError> {
    append_uuid_membership_delta_with_tombstones(project_dir, generation, nodes, edges, &[], &[])
}

#[cfg(test)]
pub(super) fn append_uuid_membership_delta_with_tombstones(
    project_dir: &Path,
    generation: u64,
    nodes: &[(Uuid, u64)],
    edges: &[Uuid],
    deleted_nodes: &[(Uuid, u64)],
    deleted_edges: &[Uuid],
) -> Result<UuidIndexAppendMetrics, GfError> {
    if crate::read_topology_generation(project_dir)? != generation {
        return Err(storage_err(
            "topology generation changed before index append",
        ));
    }
    let root = project_dir.join(INDEX_DIR);
    fs::create_dir_all(&root).map_err(storage_err)?;
    let staging = project_dir
        .parent()
        .ok_or_else(|| storage_err("project directory has no staging parent"))?;
    let mut manifest: Manifest = match fs::read(root.join(MANIFEST)) {
        Ok(body) => serde_json::from_slice(&body).map_err(storage_err)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound && generation == 1 => {
            let scratch = tempfile::Builder::new()
                .prefix("uuid-v3-empty-")
                .tempdir_in(staging)
                .map_err(storage_err)?;
            let empty = scratch.path().join("empty.run");
            File::create(&empty)
                .and_then(|file| file.sync_all())
                .map_err(storage_err)?;
            let identities = publish_data(
                &empty,
                &root,
                staging,
                "identities-v5-base",
                0,
                IDENTITY_RECORD_BYTES,
            )?;
            let node_surrogates = publish_data(
                &empty,
                &root,
                staging,
                "node-surrogates-v5-base",
                0,
                NODE_LOOKUP_RECORD_BYTES,
            )?;
            Manifest {
                format_version: FORMAT_VERSION,
                base_generation: 0,
                current_generation: 0,
                live_node_count: 0,
                live_edge_count: 0,
                runs: vec![RunRecord {
                    base: true,
                    level: 0,
                    first_generation: 0,
                    last_generation: 0,
                    identities,
                    node_surrogates,
                    node_count: 0,
                    edge_count: 0,
                    deleted_node_count: 0,
                    deleted_edge_count: 0,
                }],
            }
        }
        Err(error) => return Err(storage_err(error)),
    };
    if manifest.format_version != FORMAT_VERSION || manifest.current_generation + 1 != generation {
        return Err(storage_err(
            "index append is not a v3 generation continuation",
        ));
    }
    validate_run_descriptors(&manifest)?;
    let prior_files = manifest_file_names(&manifest);
    let mut open_runs = Vec::new();
    for descriptor in &manifest.runs {
        let identities = open_verified(&root, &descriptor.identities, IDENTITY_RECORD_BYTES)?;
        let node_surrogates =
            open_verified(&root, &descriptor.node_surrogates, NODE_LOOKUP_RECORD_BYTES)?;
        validate_run_contents(
            identities.try_clone().map_err(storage_err)?,
            node_surrogates.try_clone().map_err(storage_err)?,
            descriptor,
        )?;
        open_runs.push(OpenRun {
            identities,
            node_surrogates,
            descriptor: descriptor.clone(),
        });
    }
    let mut identities = nodes
        .iter()
        .map(|(uuid, surrogate)| (*uuid, 0_u8, *surrogate))
        .chain(edges.iter().map(|uuid| (*uuid, 1_u8, 0)))
        .chain(
            deleted_nodes
                .iter()
                .map(|(uuid, surrogate)| (*uuid, 2_u8, *surrogate)),
        )
        .chain(deleted_edges.iter().map(|uuid| (*uuid, 3_u8, 0)))
        .collect::<Vec<_>>();
    identities.sort_unstable_by_key(|entry| *entry.0.as_bytes());
    if identities.windows(2).any(|pair| pair[0].0 == pair[1].0)
        || nodes.iter().any(|(_, surrogate)| *surrogate == 0)
    {
        return Err(storage_err(
            "new identity run contains duplicate/invalid identity",
        ));
    }
    let mut surrogate_keys = nodes
        .iter()
        .map(|(uuid, surrogate)| (*surrogate, *uuid))
        .chain(
            deleted_nodes
                .iter()
                .map(|(uuid, surrogate)| (*surrogate, *uuid)),
        )
        .collect::<Vec<_>>();
    surrogate_keys.sort_unstable();
    if surrogate_keys.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(storage_err("new node run contains duplicate surrogate"));
    }
    let mut validation_bytes = 0_u64;
    let mut validation_blocks = 0_u64;
    for run in &open_runs {
        let identity_bytes = reject_identity_collisions(
            BufReader::with_capacity(
                BULK_IO_BYTES,
                File::open(root.join(&run.descriptor.identities.name)).map_err(storage_err)?,
            ),
            &identities,
        )?;
        validation_bytes = validation_bytes.saturating_add(identity_bytes);
        validation_blocks =
            validation_blocks.saturating_add(identity_bytes.div_ceil(BULK_IO_BYTES as u64));
        let surrogate_bytes = reject_surrogate_collisions(
            BufReader::with_capacity(
                BULK_IO_BYTES,
                File::open(root.join(&run.descriptor.node_surrogates.name)).map_err(storage_err)?,
            ),
            &surrogate_keys,
        )?;
        validation_bytes = validation_bytes.saturating_add(surrogate_bytes);
        validation_blocks =
            validation_blocks.saturating_add(surrogate_bytes.div_ceil(BULK_IO_BYTES as u64));
    }
    let scratch = tempfile::Builder::new()
        .prefix("uuid-v3-append-")
        .tempdir_in(staging)
        .map_err(storage_err)?;
    let identity_path = scratch.path().join("identities.run");
    let surrogate_path = scratch.path().join("surrogates.run");
    let identity_write_blocks = write_identity_records(&identity_path, &identities)?;
    let surrogate_write_blocks = write_surrogate_records(&surrogate_path, &surrogate_keys)?;
    let identity_record = publish_data(
        &identity_path,
        &root,
        staging,
        "identities-v5",
        generation,
        IDENTITY_RECORD_BYTES,
    )?;
    let surrogate_record = publish_data(
        &surrogate_path,
        &root,
        staging,
        "node-surrogates-v5",
        generation,
        NODE_LOOKUP_RECORD_BYTES,
    )?;
    manifest.runs.push(RunRecord {
        base: false,
        level: 0,
        first_generation: generation,
        last_generation: generation,
        identities: identity_record,
        node_surrogates: surrogate_record,
        node_count: nodes.len() as u64,
        edge_count: edges.len() as u64,
        deleted_node_count: deleted_nodes.len() as u64,
        deleted_edge_count: deleted_edges.len() as u64,
    });
    let mut metrics = UuidIndexAppendMetrics {
        input_records: identities.len() as u64,
        physical_bytes_written: identities
            .iter()
            .map(|(_, kind, _)| if *kind == 1 { 17_u64 } else { 25_u64 })
            .sum::<u64>()
            + surrogate_keys.len() as u64 * NODE_LOOKUP_RECORD_BYTES,
        write_blocks: identity_write_blocks + surrogate_write_blocks,
        write_bytes: identities
            .iter()
            .map(|(_, kind, _)| if *kind == 1 { 17_u64 } else { 25_u64 })
            .sum::<u64>()
            + surrogate_keys.len() as u64 * NODE_LOOKUP_RECORD_BYTES,
        peak_buffered_records: identities.len() + surrogate_keys.len(),
        peak_buffered_bytes: identities.len() * 32 + surrogate_keys.len() * 24,
        validation_scan_bytes: validation_bytes,
        validation_scan_blocks: validation_blocks,
        ..Default::default()
    };
    compact_manifest_levels(&root, staging, scratch.path(), &mut manifest, &mut metrics)?;
    manifest.current_generation = generation;
    manifest.live_node_count = manifest
        .live_node_count
        .checked_add(nodes.len() as u64)
        .and_then(|count| count.checked_sub(deleted_nodes.len() as u64))
        .ok_or_else(|| storage_err("node live-count delta is invalid"))?;
    manifest.live_edge_count = manifest
        .live_edge_count
        .checked_add(edges.len() as u64)
        .and_then(|count| count.checked_sub(deleted_edges.len() as u64))
        .ok_or_else(|| storage_err("edge live-count delta is invalid"))?;
    manifest
        .runs
        .sort_unstable_by_key(|run| run.first_generation);
    publish_manifest(&root, staging, &manifest)?;
    cleanup_superseded_files(&root, prior_files, &manifest)?;
    metrics.retained_runs = manifest.runs.len();
    Ok(metrics)
}

#[cfg(test)]
fn reject_identity_collisions(
    mut retained: BufReader<File>,
    incoming: &[(Uuid, u8, u64)],
) -> Result<u64, GfError> {
    let mut incoming_index = 0;
    let mut bytes = 0_u64;
    while incoming_index < incoming.len() {
        let Some(record) = identity_codec::read(&mut retained)? else {
            break;
        };
        bytes += identity_codec::encoded(&record)?.len() as u64;
        let retained_uuid = &record[..16];
        while incoming_index < incoming.len()
            && incoming[incoming_index].0.as_bytes().as_slice() < retained_uuid
        {
            incoming_index += 1;
        }
        if incoming_index < incoming.len()
            && incoming[incoming_index].0.as_bytes().as_slice() == retained_uuid
        {
            let incoming_record = incoming[incoming_index];
            let retained_kind = record[16];
            let retained_surrogate = u64::from_be_bytes(record[17..25].try_into().expect("fixed"));
            if matches!(incoming_record.1, 2 | 3)
                && incoming_record.1 - 2 == retained_kind
                && incoming_record.2 == retained_surrogate
            {
                continue;
            }
            return Err(storage_err(
                "UUID already exists in an authenticated retained run",
            ));
        }
    }
    Ok(bytes)
}

#[cfg(test)]
fn reject_surrogate_collisions(
    mut retained: BufReader<File>,
    incoming: &[(u64, Uuid)],
) -> Result<u64, GfError> {
    let mut incoming_index = 0;
    let mut bytes = 0_u64;
    while incoming_index < incoming.len() {
        let Some(record) = read_exact_record::<24>(&mut retained)? else {
            break;
        };
        bytes += NODE_LOOKUP_RECORD_BYTES;
        let retained_surrogate = u64::from_be_bytes(record[..8].try_into().expect("fixed"));
        while incoming_index < incoming.len() && incoming[incoming_index].0 < retained_surrogate {
            incoming_index += 1;
        }
        if incoming_index < incoming.len() && incoming[incoming_index].0 == retained_surrogate {
            if incoming[incoming_index].1.as_bytes() == &record[8..24] {
                continue;
            }
            return Err(storage_err(
                "node surrogate already exists in an authenticated retained run",
            ));
        }
    }
    Ok(bytes)
}

pub(super) fn write_identity_records(
    path: &Path,
    records: &[(Uuid, u8, u64)],
) -> Result<u64, GfError> {
    let mut blocks = 0;
    let mut bytes = Vec::with_capacity(BULK_IO_BYTES);
    let mut file = create_uuid_file(path)?;
    for (uuid, kind, surrogate) in records {
        let mut record = [0_u8; IDENTITY_RECORD_WIDTH];
        record[..16].copy_from_slice(uuid.as_bytes());
        record[16] = *kind;
        record[17..].copy_from_slice(&surrogate.to_be_bytes());
        let encoded = identity_codec::encoded(&record)?;
        if bytes.len() + encoded.len() > BULK_IO_BYTES {
            file.write_all(&bytes).map_err(storage_err)?;
            blocks += 1;
            bytes.clear();
        }
        bytes.extend_from_slice(encoded);
    }
    if !bytes.is_empty() {
        file.write_all(&bytes).map_err(storage_err)?;
        blocks += 1;
    }
    sync_uuid_file(&file)?;
    Ok(blocks)
}

fn write_surrogate_records(path: &Path, records: &[(u64, Uuid)]) -> Result<u64, GfError> {
    let mut blocks = 0;
    let mut bytes = Vec::with_capacity(BULK_IO_BYTES);
    let mut file = create_uuid_file(path)?;
    for (surrogate, uuid) in records {
        if bytes.len() + 24 > BULK_IO_BYTES {
            file.write_all(&bytes).map_err(storage_err)?;
            blocks += 1;
            bytes.clear();
        }
        bytes.extend_from_slice(&surrogate.to_be_bytes());
        bytes.extend_from_slice(uuid.as_bytes());
    }
    if !bytes.is_empty() {
        file.write_all(&bytes).map_err(storage_err)?;
        blocks += 1;
    }
    sync_uuid_file(&file)?;
    Ok(blocks)
}

#[cfg(test)]
fn publish_manifest(root: &Path, staging: &Path, manifest: &Manifest) -> Result<(), GfError> {
    let _ = staging;
    let directory = graphforge_filesystem::StableDirectory::open(root).map_err(storage_err)?;
    let temp_name = std::ffi::OsString::from(format!(".manifest-{}.tmp", Uuid::new_v4()));
    let mut temp = directory
        .create_child_file(&temp_name)
        .map_err(storage_err)?;
    let identity = graphforge_filesystem::file_identity(&temp).map_err(storage_err)?;
    let result = (|| -> Result<(), GfError> {
        serde_json::to_writer(&mut temp, manifest).map_err(storage_err)?;
        temp.flush().map_err(storage_err)?;
        temp.sync_all().map_err(storage_err)?;
        directory
            .replace_child(&temp_name, identity, std::ffi::OsStr::new(MANIFEST))
            .map_err(storage_err)?;
        directory.sync().map_err(storage_err)
    })();
    if result.is_err() {
        let _ = directory.unlink_child_if_identity(&temp_name, identity);
    }
    result
}

#[cfg(test)]
fn compact_manifest_levels(
    root: &Path,
    staging: &Path,
    scratch: &Path,
    manifest: &mut Manifest,
    metrics: &mut UuidIndexAppendMetrics,
) -> Result<(), GfError> {
    for level in 0..63_u8 {
        loop {
            let mut indexes = manifest
                .runs
                .iter()
                .enumerate()
                .filter(|(_, run)| !run.base && run.level == level)
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            if indexes.len() < 2 {
                break;
            }
            indexes.sort_unstable_by_key(|index| manifest.runs[*index].first_generation);
            let right = manifest.runs.remove(indexes[1]);
            let left = manifest.runs.remove(indexes[0]);
            if left.last_generation.saturating_add(1) != right.first_generation {
                return Err(storage_err("equal-level runs are not adjacent"));
            }
            let identity_path = scratch.join(format!("identities-level-{}.run", level + 1));
            let surrogate_path = scratch.join(format!("surrogates-level-{}.run", level + 1));
            merge_identity_v3(
                &[
                    root.join(&left.identities.name),
                    root.join(&right.identities.name),
                ],
                &identity_path,
            )?;
            merge_surrogate_runs(
                &[
                    root.join(&left.node_surrogates.name),
                    root.join(&right.node_surrogates.name),
                ],
                &surrogate_path,
            )?;
            let identities = publish_data(
                &identity_path,
                root,
                staging,
                &format!("identities-v5-l{}", level + 1),
                right.last_generation,
                IDENTITY_RECORD_BYTES,
            )?;
            let node_surrogates = publish_data(
                &surrogate_path,
                root,
                staging,
                &format!("node-surrogates-v5-l{}", level + 1),
                right.last_generation,
                NODE_LOOKUP_RECORD_BYTES,
            )?;
            let bytes = record_length(&identities, IDENTITY_RECORD_BYTES)?
                + node_surrogates.count * NODE_LOOKUP_RECORD_BYTES;
            metrics.physical_bytes_written = metrics.physical_bytes_written.saturating_add(bytes);
            metrics.write_bytes = metrics.write_bytes.saturating_add(bytes);
            metrics.write_blocks = metrics
                .write_blocks
                .saturating_add(bytes.div_ceil(BULK_IO_BYTES as u64));
            let (node_count, edge_count, deleted_node_count, deleted_edge_count) =
                count_identity_states(&root.join(&identities.name))?;
            manifest.runs.push(RunRecord {
                base: false,
                level: level + 1,
                first_generation: left.first_generation,
                last_generation: right.last_generation,
                identities,
                node_surrogates,
                node_count,
                edge_count,
                deleted_node_count,
                deleted_edge_count,
            });
        }
    }
    Ok(())
}

fn count_identity_states(path: &Path) -> Result<(u64, u64, u64, u64), GfError> {
    let mut reader =
        BufReader::with_capacity(BULK_IO_BYTES, File::open(path).map_err(storage_err)?);
    let mut counts = [0_u64; 4];
    while let Some(record) = identity_codec::read(&mut reader)? {
        let kind = usize::from(record[16]);
        if kind >= counts.len() {
            return Err(storage_err("invalid compacted identity kind"));
        }
        counts[kind] += 1;
    }
    Ok((counts[0], counts[1], counts[2], counts[3]))
}

#[cfg(test)]
fn merge_identity_v3(inputs: &[PathBuf], output: &Path) -> Result<(), GfError> {
    let mut readers = inputs
        .iter()
        .map(|path| File::open(path).map(BufReader::new).map_err(storage_err))
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = BinaryHeap::<Reverse<([u8; IDENTITY_RECORD_WIDTH], usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(record) = identity_codec::read(reader)? {
            heap.push(Reverse((record, index)));
        }
    }
    let mut out = File::create(output).map_err(storage_err)?;
    let mut block = Vec::with_capacity(BULK_IO_BYTES);
    while let Some(Reverse((mut record, index))) = heap.pop() {
        let uuid: [u8; 16] = record[..16].try_into().expect("fixed");
        let mut newest_index = index;
        if let Some(next) = identity_codec::read(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
        while heap
            .peek()
            .is_some_and(|Reverse((candidate, _))| candidate[..16] == uuid)
        {
            let Reverse((candidate, candidate_index)) = heap.pop().expect("peeked");
            if candidate_index > newest_index {
                record = candidate;
                newest_index = candidate_index;
            }
            if let Some(next) = identity_codec::read(&mut readers[candidate_index])? {
                heap.push(Reverse((next, candidate_index)));
            }
        }
        if block.len() + IDENTITY_RECORD_WIDTH > BULK_IO_BYTES {
            out.write_all(&block).map_err(storage_err)?;
            block.clear();
        }
        block.extend_from_slice(identity_codec::encoded(&record)?);
    }
    if !block.is_empty() {
        out.write_all(&block).map_err(storage_err)?;
    }
    out.flush().map_err(storage_err)?;
    out.sync_all().map_err(storage_err)
}

/// Stage one incremental v4 node-ordinal delta beside the canonical topology
/// mutation. The caller supplies a receipt-authenticated, lifetime-pinned prior
/// snapshot and owns the enclosing generation-last rewrite transaction.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
// One manifest-last planner lifecycle; the ordering between pinned admission,
// delta artifacts, compaction, receipt, manifest, and evidence is the invariant.
pub(crate) fn prepare_v4_ordinal_delta(
    project_dir: &Path,
    current: u64,
    generation: u64,
    pinned: &crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs,
    batch: &mut crate::staging::RewriteBatch,
    nodes: &[(Uuid, u64)],
    deleted_node_ids: &[u64],
    topology_delta_sha256: &str,
) -> Result<PreparedV4OrdinalDelta, GfError> {
    if generation
        != current
            .checked_add(1)
            .ok_or_else(|| storage_err("v4 generation overflow"))?
        || pinned.manifest.topology_generation != current
    {
        return Err(storage_err(
            "prepared v4 ordinal delta is not the next authenticated generation",
        ));
    }
    if topology_delta_sha256.len() != 64
        || !topology_delta_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(storage_err("v4 topology delta digest is noncanonical"));
    }

    let plan_root = open_v4_plan_root(project_dir)?;
    cleanup_abandoned_v4_plan_directories(&plan_root)?;
    let plan_root_path = project_dir.join(INDEX_DIR).join(V4_PLAN_ROOT);
    let scratch = tempfile::Builder::new()
        .prefix(V4_PLAN_PREFIX)
        .tempdir_in(&plan_root_path)
        .map_err(storage_err)?;
    // The retained capability proves that path lookup did not escape or replace
    // the authenticated, project-owned planner namespace during creation.
    plan_root.revalidate_named().map_err(storage_err)?;
    let artifacts_path = scratch.path().join("artifacts");
    fs::create_dir(&artifacts_path).map_err(storage_err)?;
    let artifacts =
        graphforge_filesystem::StableDirectory::open(&artifacts_path).map_err(storage_err)?;

    let mut build_metrics = UuidIndexBuildMetrics::default();
    let uuid_sorted = external_sort_v4_nodes(nodes, scratch.path(), &mut build_metrics)?;
    reject_prior_v4_uuid_reuse(pinned, &uuid_sorted)?;
    let ordinal_sorted = build_surrogate_run(
        &uuid_sorted,
        scratch.path(),
        UuidIndexBuildLimits::default(),
        &mut build_metrics,
    )?;
    let mut deleted = deleted_node_ids.to_vec();
    deleted.sort_unstable();
    if deleted.contains(&0) || deleted.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(storage_err(
            "v4 tombstone delta is not sorted unique nonzero",
        ));
    }

    let prior_max = pinned
        .manifest
        .ordinal_ranges
        .last()
        .map(|range| {
            range
                .first_node_id
                .checked_add(range.count.saturating_sub(1))
                .ok_or_else(|| storage_err("retained v4 ordinal range overflows"))
        })
        .transpose()?
        .unwrap_or(0);
    let mut ordinal_probe = BufReader::new(File::open(&ordinal_sorted).map_err(storage_err)?);
    let first_new = read_surrogate_record(&mut ordinal_probe)?;
    if first_new.is_some_and(|(id, _)| id <= prior_max) {
        return Err(storage_err(
            "v4 node surrogate is not monotonic or is reused",
        ));
    }
    if deleted.iter().any(|id| {
        !pinned.manifest.ordinal_ranges.iter().any(|range| {
            range
                .first_node_id
                .checked_add(range.count.saturating_sub(1))
                .is_some_and(|last| (range.first_node_id..=last).contains(id))
        })
    }) {
        return Err(storage_err(
            "v4 tombstone does not name retained ordinal authority",
        ));
    }

    let mut writer = V4OrdinalConstructionWriter::start(generation, &artifacts)?;
    let mut cancelled = || false;
    let mut forward = BufReader::with_capacity(
        BULK_IO_BYTES,
        File::open(&uuid_sorted).map_err(storage_err)?,
    );
    while let Some((uuid, node_id)) = read_node_surrogate_record(&mut forward)? {
        writer.push_forward(Uuid::from_bytes(uuid), node_id, &mut cancelled)?;
    }
    let mut ordinal = BufReader::with_capacity(
        BULK_IO_BYTES,
        File::open(&ordinal_sorted).map_err(storage_err)?,
    );
    while let Some((node_id, uuid)) = read_surrogate_record(&mut ordinal)? {
        writer.push_ordinal(node_id, Uuid::from_bytes(uuid), &mut cancelled)?;
    }
    let V4ConstructionArtifactBundle {
        manifest: delta_manifest,
        metrics: build,
        publications: delta_publications,
    } = writer.finish()?;
    let (tombstones, tombstone_bytes, tombstone_blocks) =
        write_v4_tombstone_artifact(&artifacts, generation, &deleted)?;
    let tombstone_name = tombstones.run.artifact.name.clone();
    crate::project_failpoint::hit(
        "v4_append.after_delta_artifacts",
        None,
        None,
        "V4_APPEND_ARTIFACTS",
        false,
    )?;

    let mut manifest = pinned.manifest.clone();
    manifest.topology_generation = generation;
    manifest
        .forward_identities
        .extend(delta_manifest.forward_identities);
    manifest
        .ordinal_ranges
        .extend(delta_manifest.ordinal_ranges);
    manifest.tombstones.push(tombstones.run);
    manifest
        .ordinal_ranges
        .sort_unstable_by_key(|range| range.first_node_id);

    let mut created = manifest
        .forward_identities
        .iter()
        .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
        .chain(manifest.tombstones.iter().map(|run| &run.artifact))
        .filter(|artifact| artifact.generation == generation)
        .map(|artifact| (artifact.name.clone(), artifacts_path.join(&artifact.name)))
        .collect::<HashMap<_, _>>();
    let delta_created_artifacts = u64::try_from(created.len()).map_err(storage_err)?;
    let mut compaction = compact_v4_binary_carry(
        pinned,
        &artifacts,
        &artifacts_path,
        &mut manifest,
        &mut created,
    )?;
    if compaction.compactions != 0 {
        crate::project_failpoint::hit(
            "v4_compaction.after_outputs",
            None,
            None,
            "V4_COMPACTION_OUTPUTS",
            false,
        )?;
    }
    v4_publication_failure("manifest_update")?;
    admit_v4_construction_manifest(&manifest)?;

    let destination = project_dir.join(INDEX_DIR);
    let retained_names = v4_manifest_artifact_names(&manifest);
    let mut outputs = created
        .into_iter()
        .filter(|(name, _)| retained_names.contains(name))
        .collect::<Vec<_>>();
    outputs.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    for (name, path) in &outputs {
        batch.stage_file(&destination.join(name), path)?;
    }
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(storage_err)?;
    let receipt = TopologyIndexReceipt {
        nonce: Uuid::new_v4().simple().to_string(),
        expected_generation: generation,
        topology_delta_sha256: topology_delta_sha256.to_owned(),
        manifest_sha256: hex_sha256(&manifest_bytes),
    };
    let receipt_bytes = serde_json::to_vec(&receipt).map_err(storage_err)?;
    let receipt_path = destination.join(V4_ORDINAL_RECEIPT);
    let manifest_path = destination.join(V4_ORDINAL_MANIFEST);
    batch.stage_bytes(&receipt_path, &receipt_bytes)?;
    crate::project_failpoint::hit(
        "v4_append.after_receipt_stage",
        None,
        None,
        "V4_APPEND_RECEIPT",
        false,
    )?;
    batch.stage_bytes(&manifest_path, &manifest_bytes)?;
    batch.move_staged_destination_to_end(&manifest_path);
    crate::project_failpoint::hit(
        "v4_append.after_manifest_stage",
        None,
        None,
        "V4_APPEND_MANIFEST",
        false,
    )?;

    let prior_names = v4_manifest_artifact_names(&pinned.manifest);
    let staged_artifact_bytes = outputs.iter().try_fold(0_u64, |sum, (_, path)| {
        sum.checked_add(path.metadata().map_err(storage_err)?.len())
            .ok_or_else(|| storage_err("v4 staged artifact byte count overflow"))
    })?;
    let control_bytes =
        u64::try_from(receipt_bytes.len() + manifest_bytes.len()).map_err(storage_err)?;
    let submitted_write_bytes = build
        .artifact_bytes
        .saturating_add(tombstone_bytes)
        .saturating_add(compaction.write_bytes)
        .saturating_add(staged_artifact_bytes)
        .checked_add(control_bytes)
        .ok_or_else(|| storage_err("v4 submitted write byte count overflow"))?;
    let staged_write_blocks = outputs.iter().try_fold(0_u64, |sum, (_, path)| {
        let bytes = path.metadata().map_err(storage_err)?.len();
        sum.checked_add(bytes.div_ceil(crate::staging::STAGE_FILE_BLOCK_BYTES as u64))
            .ok_or_else(|| storage_err("v4 staged write block count overflow"))
    })?;
    let sorting_temporary_upper = u64::try_from(nodes.len())
        .map_err(storage_err)?
        .checked_mul(24 * 4)
        .ok_or_else(|| storage_err("v4 sorting temporary byte count overflow"))?;
    let external_peak_buffer = build_metrics
        .peak_buffered_records
        .checked_mul(24)
        .ok_or_else(|| storage_err("v4 external-sort buffer charge overflow"))?;
    let metrics = V4OrdinalAppendMetrics {
        input_identities: u64::try_from(nodes.len()).map_err(storage_err)?,
        input_tombstones: u64::try_from(deleted.len()).map_err(storage_err)?,
        created_artifacts: delta_created_artifacts.saturating_add(compaction.created_artifacts),
        retained_artifacts: u64::try_from(retained_names.intersection(&prior_names).count())
            .map_err(storage_err)?,
        physical_bytes_written: submitted_write_bytes,
        compactions: compaction.compactions,
        sequential_read_bytes: compaction.read_bytes,
        sequential_read_calls: compaction.read_calls,
        sequential_read_blocks: compaction.read_blocks,
        write_bytes: submitted_write_bytes,
        write_blocks: build
            .write_blocks
            .saturating_add(tombstone_blocks)
            .saturating_add(compaction.write_blocks)
            .saturating_add(staged_write_blocks)
            .saturating_add(2),
        peak_buffer_bytes: build
            .peak_buffer_bytes
            .max(V4_ORDINAL_BLOCK_BYTES * 3)
            .max(crate::staging::STAGE_FILE_BLOCK_BYTES)
            .max(external_peak_buffer),
        peak_temporary_bytes: build
            .peak_temporary_bytes
            .saturating_add(tombstone_bytes)
            .saturating_add(compaction.write_bytes)
            .saturating_add(staged_artifact_bytes)
            .saturating_add(control_bytes)
            .saturating_add(sorting_temporary_upper),
        fsync_operations: build
            .fsync_operations
            .saturating_add(1)
            .saturating_add(compaction.fsync_operations)
            .saturating_add(u64::try_from(outputs.len()).map_err(storage_err)?)
            .saturating_add(2),
        cache_release: compaction.cache_release,
        peak_configured_cache_window_bytes: compaction.peak_configured_cache_window_bytes,
        ..Default::default()
    };
    let prepared = PreparedV4OrdinalDelta {
        expected_generation: generation,
        auxiliary: crate::AuxiliaryReceipt {
            kind: "uuid-membership/v4".to_owned(),
            schema_version: crate::ORDINAL_IDENTITY_V4,
            path: format!("{INDEX_DIR}/{V4_ORDINAL_RECEIPT}"),
            digest: hex_sha256(&receipt_bytes),
            bytes: u64::try_from(receipt_bytes.len()).map_err(storage_err)?,
        },
        metrics,
        manifest,
    };
    let mut authority_publications = Vec::new();
    if retained_names.contains(&tombstone_name) {
        retain_v4_publication(
            &mut authority_publications,
            tombstone_name,
            tombstones.publication,
        );
    }
    for (name, publication) in delta_publications {
        if retained_names.contains(&name) {
            authority_publications.push((name, publication));
        }
    }
    for (name, publication) in compaction.publications.drain(..) {
        if retained_names.contains(&name) {
            authority_publications.push((name, publication));
        }
    }
    commit_v4_publications(authority_publications, V4AuthorityTransactionProof)?;
    Ok(prepared)
}

pub(super) const V4_PLAN_PREFIX: &str = "uuid-membership-v4-plan-";
pub(super) const V4_PLAN_ROOT: &str = ".uuid-membership-v4-plans";
const V4_PLAN_CLEANUP_CANDIDATES: usize = 16;
const V4_PLAN_CLEANUP_ENTRIES: usize = 4096;
const V4_PLAN_CLEANUP_BYTES: u64 = 2 * 1024 * 1024 * 1024;

fn open_v4_plan_root(
    project_dir: &Path,
) -> Result<graphforge_filesystem::StableDirectory, GfError> {
    let index = graphforge_filesystem::StableDirectory::open(&project_dir.join(INDEX_DIR))
        .map_err(storage_err)?;
    let name = std::ffi::OsStr::new(V4_PLAN_ROOT);
    let root = match index.create_child_directory(name) {
        Ok(root) => {
            index.sync().map_err(storage_err)?;
            root
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            index.open_child_directory(name).map_err(storage_err)?
        }
        Err(error) => return Err(storage_err(error)),
    };
    index.revalidate_named().map_err(storage_err)?;
    Ok(root)
}

/// Reclaim planner directories from the authenticated project-owned namespace.
/// Atomic child creation makes every visible entry recognizable immediately;
/// no forgeable marker or shared-parent scan participates in ownership.
fn cleanup_abandoned_v4_plan_directories(
    plan_root: &graphforge_filesystem::StableDirectory,
) -> Result<(), GfError> {
    let candidates = plan_root
        .child_names_bounded(V4_PLAN_CLEANUP_CANDIDATES + 1)
        .map_err(|error| storage_err(format!("v4 planner namespace inventory: {error}")))?;
    if candidates.len() > V4_PLAN_CLEANUP_CANDIDATES {
        return Err(storage_err("v4 planner cleanup candidate bound exceeded"));
    }
    for name in candidates {
        if !name.to_str().is_some_and(|text| {
            text.strip_prefix(V4_PLAN_PREFIX).is_some_and(|nonce| {
                !nonce.is_empty() && nonce.bytes().all(|byte| byte.is_ascii_alphanumeric())
            })
        }) {
            return Err(storage_err(
                "v4 planner namespace contains an unknown child",
            ));
        }
        let directory = plan_root.open_child_directory(&name).map_err(storage_err)?;
        cleanup_v4_plan_directory(&directory)?;
        crate::project_failpoint::hit(
            "v4_plan_cleanup.before_unlink",
            None,
            None,
            "V4_PLAN_CLEANUP_BEFORE_UNLINK",
            false,
        )?;
        let directory_identity = directory.identity();
        drop(directory);
        plan_root
            .remove_child_directory_if_identity(&name, directory_identity)
            .map_err(storage_err)?;
        crate::project_failpoint::hit(
            "v4_plan_cleanup.after_unlink",
            None,
            None,
            "V4_PLAN_CLEANUP_AFTER_UNLINK",
            false,
        )?;
        plan_root.sync().map_err(storage_err)?;
    }
    plan_root.revalidate_named().map_err(storage_err)?;
    Ok(())
}

fn cleanup_v4_plan_directory(
    directory: &graphforge_filesystem::StableDirectory,
) -> Result<(), GfError> {
    let names = directory
        .child_names_bounded(V4_PLAN_CLEANUP_ENTRIES)
        .map_err(|error| storage_err(format!("v4 planner directory inventory: {error}")))?;
    let mut bytes = 0_u64;
    for name in names {
        if name == std::ffi::OsStr::new("artifacts") {
            let artifacts = directory.open_child_directory(&name).map_err(storage_err)?;
            cleanup_v4_plan_files(&artifacts, &mut bytes)?;
            let artifacts_identity = artifacts.identity();
            drop(artifacts);
            directory
                .remove_child_directory_if_identity(&name, artifacts_identity)
                .map_err(storage_err)?;
        } else {
            cleanup_v4_plan_file(directory, &name, &mut bytes)?;
        }
    }
    directory.sync().map_err(storage_err)
}

fn cleanup_v4_plan_files(
    directory: &graphforge_filesystem::StableDirectory,
    bytes: &mut u64,
) -> Result<(), GfError> {
    let names = directory
        .child_names_bounded(V4_PLAN_CLEANUP_ENTRIES)
        .map_err(|error| storage_err(format!("v4 planner artifact inventory: {error}")))?;
    for name in names {
        cleanup_v4_plan_file(directory, &name, bytes)?;
    }
    directory.sync().map_err(storage_err)
}

fn cleanup_v4_plan_file(
    directory: &graphforge_filesystem::StableDirectory,
    name: &std::ffi::OsStr,
    bytes: &mut u64,
) -> Result<(), GfError> {
    let file = directory.open_child_file(name).map_err(storage_err)?;
    if graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1 {
        return Err(storage_err("v4 planner cleanup file has unexpected links"));
    }
    *bytes = bytes
        .checked_add(file.metadata().map_err(storage_err)?.len())
        .ok_or_else(|| storage_err("v4 planner cleanup byte count overflow"))?;
    if *bytes > V4_PLAN_CLEANUP_BYTES {
        return Err(storage_err("v4 planner cleanup byte bound exceeded"));
    }
    let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
    drop(file);
    directory
        .unlink_child_if_identity(name, identity)
        .map_err(storage_err)
}

/// Prove that an appended UUID has never appeared in retained forward
/// authority. Tombstones do not release identity: ordinals are monotonic and
/// UUIDs are never reusable. Both sides are sorted, so each retained run is
/// checked by one bounded sequential merge before anything enters the caller's
/// rewrite batch.
fn reject_prior_v4_uuid_reuse(
    pinned: &crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs,
    uuid_sorted: &Path,
) -> Result<(), GfError> {
    for artifact in &pinned.manifest.forward_identities {
        let mut prior = BufReader::with_capacity(
            V4_ORDINAL_BLOCK_BYTES,
            clone_pinned_v4_file(pinned, &artifact.name)?,
        );
        let mut appended = BufReader::with_capacity(
            V4_ORDINAL_BLOCK_BYTES,
            File::open(uuid_sorted).map_err(storage_err)?,
        );
        let mut prior_head = read_v4_forward_record(&mut prior)?;
        let mut appended_head = read_node_surrogate_record(&mut appended)?;
        while let (Some((prior_uuid, _)), Some((appended_uuid, _))) = (prior_head, appended_head) {
            match prior_uuid.cmp(&appended_uuid) {
                std::cmp::Ordering::Less => prior_head = read_v4_forward_record(&mut prior)?,
                std::cmp::Ordering::Greater => {
                    appended_head = read_node_surrogate_record(&mut appended)?;
                }
                std::cmp::Ordering::Equal => {
                    return Err(storage_err(
                        "v4 node UUID already exists in retained forward authority",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn external_sort_v4_nodes(
    nodes: &[(Uuid, u64)],
    scratch: &Path,
    metrics: &mut UuidIndexBuildMetrics,
) -> Result<PathBuf, GfError> {
    let limits = UuidIndexBuildLimits::default();
    let mut runs = Vec::new();
    let mut buffer = Vec::with_capacity(limits.run_records);
    for &(uuid, node_id) in nodes {
        if uuid.is_nil() || node_id == 0 {
            return Err(storage_err("v4 node delta contains a zero identity"));
        }
        buffer.push((*uuid.as_bytes(), node_id));
        metrics.peak_buffered_records = metrics.peak_buffered_records.max(buffer.len());
        if buffer.len() == limits.run_records {
            flush_entity_surrogate_run(&mut buffer, scratch, "v4-delta", &mut runs, metrics)?;
        }
    }
    if !buffer.is_empty() {
        flush_entity_surrogate_run(&mut buffer, scratch, "v4-delta", &mut runs, metrics)?;
    }
    if runs.is_empty() {
        let path = scratch.join("v4-delta-empty.run");
        File::create(&path)
            .and_then(|file| file.sync_all())
            .map_err(storage_err)?;
        runs.push(path);
    }
    merge_node_surrogate_runs(runs, scratch, limits.merge_fan_in, metrics)
}

#[cfg(test)]
mod tests;
