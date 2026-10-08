//! UUID topology delta preparation, commit, and reconciliation.

use super::BULK_IO_BYTES;
use super::CommittedUuidTopologyRewrite;
use super::DEFAULT_ORPHAN_GC_LIMIT;
use super::INDEX_DIR;
use super::PreparedV4OrdinalDelta;
use super::TopologyIndexReceipt;
use super::UuidIndexBuildLimits;
use super::UuidIndexBuildMetrics;
use super::UuidIndexOrphanGcWork;
use super::UuidTopologyDelta;
use super::V4_ORDINAL_BLOCK_BYTES;
use super::V4_ORDINAL_MANIFEST;
use super::V4_ORDINAL_RECEIPT;
use super::V4OrdinalAppendMetrics;
use super::hex_bytes;
use super::maintenance::collect_uuid_orphans_locked;
use super::maintenance::selected_generation_for_graph_root;
use super::maintenance::standalone_v4_pinned_update;
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
use super::rebuild::read_surrogate_record;
use crate::UuidIndexKind;

use super::rebuild::flush_entity_surrogate_run;
use super::rebuild::merge_node_surrogate_runs;
use super::rebuild::read_node_surrogate_record;
use super::storage_err;
use super::v4_publication_failure;
use graphforge_core::GfError;
use graphforge_core::hash_observation::ControlSha256 as Sha256;
use sha2::Digest;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

/// What [`check_and_record_identities`] learned from the published topology.
struct RecordedIdentities {
    /// Topology generation the identities were checked against.
    generation: u64,
    /// Deleted nodes with their `node_id`.
    deleted_nodes: Vec<(Uuid, u64)>,
    /// Probe work spent checking them.
    metrics: crate::UuidProbeMetrics,
}

/// Resolve the identities a rewrite adds and deletes against the published
/// topology, stage the record of deleted UUIDs, and return the deleted nodes
/// with their `node_id`.
///
/// A new UUID may not equal any live node, live edge, or deleted entity: node
/// and edge UUIDs share one namespace and a deleted UUID is never reused.
fn check_and_record_identities(
    project_dir: &Path,
    staged: &mut crate::staging::RewriteBatch,
    delta: &UuidTopologyDelta,
    probe: &mut Option<crate::TopologyIdentityProbe>,
) -> Result<RecordedIdentities, GfError> {
    let generation = crate::read_topology_generation(project_dir)?;
    if probe
        .as_ref()
        .is_none_or(|value| value.topology_generation() != generation)
    {
        let files = match staged.topology_authority() {
            Some(authority) => crate::enumerate_topology_files(authority, None)?,
            None => crate::TopologyFiles::discover_legacy(project_dir)?,
        };
        *probe = Some(crate::TopologyIdentityProbe::open(
            project_dir,
            &files,
            generation,
        )?);
    }
    let probe = probe.as_mut().expect("probe opened above");
    let mut metrics = crate::UuidProbeMetrics::default();

    let mut incoming = delta
        .nodes
        .iter()
        .map(|(uuid, _)| *uuid)
        .chain(delta.edges.iter().copied())
        .collect::<Vec<_>>();
    if incoming.iter().any(Uuid::is_nil) {
        return Err(storage_err("topology delta contains a nil UUID"));
    }
    if !incoming.is_empty() {
        let (taken, work) = probe.taken(&incoming)?;
        if taken.into_iter().any(|taken| taken) {
            return Err(storage_err(
                "UUID already exists in the published topology or among deleted identities",
            ));
        }
        metrics.absorb(&work);
        incoming.sort_unstable();
        if incoming.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(storage_err("topology delta repeats a UUID"));
        }
    }

    let (surrogates, work) = probe.lookup_node_surrogates(&delta.deleted_nodes)?;
    metrics.absorb(&work);
    let deleted_nodes = delta
        .deleted_nodes
        .iter()
        .copied()
        .zip(surrogates)
        .filter_map(|(uuid, surrogate)| surrogate.map(|id| (uuid, id)))
        .collect::<Vec<_>>();
    let (present, work) = probe.probe(UuidIndexKind::Edge, &delta.deleted_edges)?;
    metrics.absorb(&work);
    let deleted_edges = delta
        .deleted_edges
        .iter()
        .copied()
        .zip(present)
        .filter_map(|(uuid, present)| present.then_some(uuid))
        .collect::<Vec<_>>();
    let spent = deleted_nodes
        .iter()
        .map(|(uuid, _)| *uuid)
        .chain(deleted_edges.iter().copied())
        .collect::<Vec<_>>();
    crate::stage_deleted_identities(staged, project_dir, &spent)?;
    Ok(RecordedIdentities {
        generation,
        deleted_nodes,
        metrics,
    })
}

/// Commit topology and its identity participant under the one durable rewrite lock.
#[allow(clippy::too_many_lines)] // One sealed participant lifecycle; order is the invariant.
pub(crate) fn commit_uuid_topology_rewrite(
    project_dir: &Path,
    mut staged: crate::staging::RewriteBatch,
    delta: &UuidTopologyDelta,
    probe: &mut Option<crate::TopologyIdentityProbe>,
) -> Result<CommittedUuidTopologyRewrite, GfError> {
    let delta_is_empty = delta.nodes.is_empty()
        && delta.edges.is_empty()
        && delta.deleted_nodes.is_empty()
        && delta.deleted_edges.is_empty();
    if delta_is_empty && staged.is_empty() {
        return Ok(CommittedUuidTopologyRewrite::NoTopologyChange);
    }
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
                crate::V4OrdinalIdentityOpen::Ready(mut handle) => {
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
    let RecordedIdentities {
        generation: probed_generation,
        deleted_nodes,
        metrics: probe_metrics,
    } = check_and_record_identities(project_dir, &mut staged, delta, probe)?;
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
            if context.prior.topology != probed_generation {
                return Err(storage_err(
                    "topology generation changed between identity checks and commit",
                ));
            }
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
            // manifest.
            let orphan_gc = if selected.is_some() {
                collect_uuid_orphans_locked(
                    context.project,
                    context.project_root,
                    DEFAULT_ORPHAN_GC_LIMIT,
                    ordinal_authority.as_ref(),
                )?
            } else {
                UuidIndexOrphanGcWork::default()
            };
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
                            &delta.deleted_edges,
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
                .map(PreparedV4OrdinalDelta::auxiliary_receipt);
            *prepared_v4_from_callback.borrow_mut() = prepared_ordinal;
            Ok(receipt)
        });
    // Retain this batch's exact membership before the rewrite consumes it.
    // A reconciled durable commit must install the same topology as ordinary
    // success.
    let topology = staged.topology_authority().cloned();
    let topology_candidate = topology
        .as_ref()
        .map(|authority| authority.prepare_installed(&staged))
        .transpose()?;
    let mut reconciled = false;
    let commit =
        crate::generation::commit_topology_aware_with_participant(staged, &root, participant);
    let v4_token = prepared_v4.borrow_mut().take();
    let committed = match commit {
        Ok(value) => value,
        Err(error) => {
            *probe = None;
            let outcome = (|| -> Result<_, GfError> {
                let Some((prior, next)) = generations.get() else {
                    return Ok(None);
                };
                // The v4 receipt names the exact outcome. A root without the
                // ordinal facet carries no receipt, so a rewrite that advanced
                // the topology is decided by the generation state under the
                // rewrite lock. A rewrite that left the topology generation
                // alone changes no identity and is not reconciled here.
                let outcome = match v4_token.as_ref() {
                    Some(v4) => reconcile_v4_ordinal_auxiliary(&root, prior, next, v4)?,
                    None if next.topology == prior.topology => return Ok(None),
                    None => {
                        crate::durable_rewrite::reconcile_generation_transition(&root, prior, next)?
                    }
                };
                Ok(Some((outcome, next.topology)))
            })();
            match outcome {
                Ok(Some((
                    crate::durable_rewrite::AuxiliaryReconcileOutcome::Committed,
                    generation,
                ))) => {
                    reconciled = true;
                    Some(generation)
                }
                Ok(Some((crate::durable_rewrite::AuxiliaryReconcileOutcome::NotCommitted, _))) => {
                    return Err(error);
                }
                Err(reconcile) => {
                    return Err(storage_err(format!(
                        "{error}; identity reconciliation failed: {reconcile}"
                    )));
                }
                Ok(None) => return Err(error),
            }
        }
    };
    let committed_v4_metrics = v4_token
        .as_ref()
        .map(|token| Box::new(token.metrics().clone()));
    if let Some(generation) = committed {
        *probe = None;
        if let Some(v4) = v4_token.as_ref() {
            v4.verify_generation(generation)?;
        }
        if reconciled && let (Some(topology), Some(candidate)) = (topology, topology_candidate) {
            topology.install(candidate);
        }
    }
    Ok(committed.map_or(
        CommittedUuidTopologyRewrite::NoTopologyChange,
        |generation| CommittedUuidTopologyRewrite::Committed {
            generation,
            probe: probe_metrics,
            v4_metrics: committed_v4_metrics,
        },
    ))
}

/// Commit a topology rewrite that changes no UUID identity.
pub(crate) fn commit_uuid_neutral_topology_rewrite(
    project_dir: &Path,
    staged: crate::staging::RewriteBatch,
) -> Result<Option<u64>, GfError> {
    let mut probe = None;
    Ok(commit_uuid_topology_rewrite(
        project_dir,
        staged,
        &UuidTopologyDelta {
            nodes: Vec::new(),
            edges: Vec::new(),
            deleted_nodes: Vec::new(),
            deleted_edges: Vec::new(),
        },
        &mut probe,
    )?
    .generation())
}

pub(super) fn hex_sha256(bytes: &[u8]) -> String {
    hex_bytes(&Sha256::digest(bytes))
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
    let mut hasher = graphforge_core::hash_observation::ContractSha256::new();
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
        first_ordinal_uuid: delta_first_uuid,
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

    let delta_uuid_order = delta_manifest.uuid_order_matches_ordinals;
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
    // Derived from the streamed delta and the authenticated tail of the parent,
    // never assumed. Compaction below re-packs the same sequence, so it keeps it.
    manifest.uuid_order_matches_ordinals = crate::ordinal_identity_v4::combine_uuid_order(
        pinned.manifest.uuid_order_matches_ordinals,
        pinned.last_ordinal_uuid()?,
        delta_uuid_order,
        delta_first_uuid,
    );

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
            kind: "uuid-membership/ordinal-v6".to_owned(),
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
            crate::durable_commit::acknowledge_directory(&index).map_err(storage_err)?;
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
        crate::durable_commit::acknowledge_directory(plan_root).map_err(storage_err)?;
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
    crate::durable_commit::acknowledge_directory(directory).map_err(storage_err)
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
    crate::durable_commit::acknowledge_directory(directory).map_err(storage_err)
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
        File::create(&path).map_err(storage_err)?;
        runs.push(path);
    }
    merge_node_surrogate_runs(runs, scratch, limits.merge_fan_in, metrics)
}

#[cfg(test)]
mod tests;
