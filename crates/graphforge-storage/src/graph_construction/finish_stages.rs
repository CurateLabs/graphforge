//! Durable shaping finish stages (#1562).
//!
//! Once staged input has been retired behind the progress chain (#1418), the
//! sealed partition segments are the only authority for the rows they hold.
//! Before this module every family's segments therefore stayed on disk until
//! the whole shape completed, and at the shaping→encoding handoff they were
//! the largest single component of the transient peak (#1393 phase two).
//!
//! A finish stage hands that authority forward. Each step of the finish
//! pipeline installs one `shape-stage-<index>.json` control naming the
//! installed successor it produced, and only then unlinks the inputs that
//! successor replaces: a family's segments once its sorted output exists, a
//! staged domain once its derived domain exists. A resumed shape adopts every
//! recorded successor — authenticated against the recorded receipt — instead
//! of re-deriving it from inputs that no longer exist.
//!
//! Stages are strictly ordered ([`ShapeStageKind::ORDER`]), append-only and
//! chained by body digest to the progress chain's head, exactly like the
//! boundary controls. A stage is only ever installed once routing is
//! complete, so a resumed shape never re-routes into a family whose segments
//! have already been retired.

use super::partition_shaping::{PartitionFamily, parse_segment_name};
use super::progress::LoadedShapeProgress;
use super::shape::{
    SHAPED_EDGE_DETAILS, SHAPED_EDGE_ENDPOINTS, SHAPED_IDENTITIES, SHAPED_NODE_DETAILS,
    STAGED_ENDPOINTS, STAGED_IDENTITIES,
};
use super::{
    ArtifactReceipt, Checkpoint, GfError, GraphConstructionBudgets, IdentityRecord,
    MAX_SHAPE_CONTROL_BYTES, OsStr, StableDirectory, Uuid, control_sha256, decode_bounded,
    install_control, is_canonical_sha256, read_bounded_limit, storage,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Prefix of every finish-stage control.
pub(super) const SHAPE_STAGE_PREFIX: &str = "shape-stage-";

/// One step of the finish pipeline, in the only order it can complete.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum ShapeStageKind {
    /// The joint identity family is sorted into `staged-identities.run`.
    Identities,
    /// Node details are sorted into `shaped-node-details.run`.
    NodeDetails,
    /// Edge details are sorted into `shaped-edge-details.run`.
    EdgeDetails,
    /// Staged endpoints are sorted into `staged-endpoints.run`.
    Endpoints,
    /// Surrogates are assigned into `shaped-identities.run`; the staged
    /// identity domain is retired.
    Assigned,
    /// Endpoints are resolved and routed into sealed `resolved` segments; the
    /// staged endpoint domain is retired.
    ResolvedRouted,
    /// Resolved segments are sorted into `shaped-edge-endpoints.run`.
    Resolved,
}

impl ShapeStageKind {
    /// Completion order. A stage's index in this array is its durable index.
    pub(super) const ORDER: [Self; 7] = [
        Self::Identities,
        Self::NodeDetails,
        Self::EdgeDetails,
        Self::Endpoints,
        Self::Assigned,
        Self::ResolvedRouted,
        Self::Resolved,
    ];

    /// Stable lower-case name, used in failpoints.
    pub(super) const fn tag(self) -> &'static str {
        match self {
            Self::Identities => "identities",
            Self::NodeDetails => "node-details",
            Self::EdgeDetails => "edge-details",
            Self::Endpoints => "endpoints",
            Self::Assigned => "assigned",
            Self::ResolvedRouted => "resolved-routed",
            Self::Resolved => "resolved",
        }
    }

    /// The partition family whose segments this stage retires, if any.
    const fn retired_family(self) -> Option<PartitionFamily> {
        match self {
            Self::Identities => Some(PartitionFamily::Identities),
            Self::NodeDetails => Some(PartitionFamily::NodeDetails),
            Self::EdgeDetails => Some(PartitionFamily::EdgeDetails),
            Self::Endpoints => Some(PartitionFamily::Endpoints),
            Self::Resolved => Some(PartitionFamily::Resolved),
            Self::Assigned | Self::ResolvedRouted => None,
        }
    }

    /// The earlier successor this stage consumes and retires, if any.
    const fn retired_output(self) -> Option<Self> {
        match self {
            Self::Assigned => Some(Self::Identities),
            Self::ResolvedRouted => Some(Self::Endpoints),
            Self::Resolved => Some(Self::ResolvedRouted),
            Self::Identities | Self::NodeDetails | Self::EdgeDetails | Self::Endpoints => None,
        }
    }

    /// Whether `outputs` is a shape this stage can have produced.
    fn admits(self, outputs: &[ArtifactReceipt]) -> bool {
        let single = |name: &str, required: bool| match outputs {
            [] => !required,
            [output] => output.name == name,
            _ => false,
        };
        match self {
            Self::Identities => single(STAGED_IDENTITIES, true),
            Self::NodeDetails => single(SHAPED_NODE_DETAILS, false),
            Self::EdgeDetails => single(SHAPED_EDGE_DETAILS, false),
            Self::Endpoints => single(STAGED_ENDPOINTS, false),
            Self::Assigned => single(SHAPED_IDENTITIES, true),
            Self::Resolved => single(SHAPED_EDGE_ENDPOINTS, false),
            Self::ResolvedRouted => {
                let mut partitions = BTreeSet::new();
                outputs.iter().all(|output| {
                    parse_segment_name(&output.name).is_some_and(|segment| {
                        segment.family == Some(PartitionFamily::Resolved)
                            && partitions.insert(segment.partition)
                    })
                })
            }
        }
    }
}

/// One installed finish stage.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct ShapeStage {
    format_version: u32,
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    parent_topology_generation: u64,
    ontology_mode: graphforge_core::OntologyMode,
    semantic_authority_sha256: Option<String>,
    budgets: GraphConstructionBudgets,
    last_receipt_sha256: Option<String>,
    /// Position in [`ShapeStageKind::ORDER`].
    index: u32,
    stage: ShapeStageKind,
    /// Body digest of the previous stage, or of the progress chain's head for
    /// the first stage.
    prior_sha256: String,
    /// The installed successors this stage hands forward.
    pub(super) outputs: Vec<ArtifactReceipt>,
    /// `ResolvedRouted` only: rows routed into each resolved partition.
    pub(super) rows: Vec<u64>,
    /// `Assigned` only: new node and edge counts.
    pub(super) new_nodes: u64,
    pub(super) new_edges: u64,
}

/// What one stage produced, before it is bound into a control.
#[derive(Default)]
pub(super) struct StageResult {
    pub(super) outputs: Vec<ArtifactReceipt>,
    pub(super) rows: Vec<u64>,
    pub(super) new_nodes: u64,
    pub(super) new_edges: u64,
}

/// The durable name of a stage control.
pub(super) fn shape_stage_name(kind: ShapeStageKind) -> String {
    let index = ShapeStageKind::ORDER
        .iter()
        .position(|candidate| *candidate == kind)
        .expect("every stage kind is ordered");
    format!("{SHAPE_STAGE_PREFIX}{index:02}.json")
}

/// Parse a stage control name into its index.
pub(super) fn parse_shape_stage_name(name: &str) -> Option<usize> {
    let body = name
        .strip_prefix(SHAPE_STAGE_PREFIX)?
        .strip_suffix(".json")?;
    if body.len() != 2 || !body.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    body.parse().ok()
}

/// The ordered, chain-validated finish stages of one incomplete shape.
///
/// Empty for a shape that has not finished routing, and for every
/// boundary-less shape: those keep their staged input, so nothing needs to be
/// handed forward.
#[derive(Clone, Debug, Default)]
pub(super) struct ShapeStages {
    stages: Vec<ShapeStage>,
    /// Digest the next installed stage chains to.
    next_prior_sha256: Option<String>,
}

impl ShapeStages {
    /// A fresh log chaining to the progress head `head_sha256`.
    pub(super) fn after_progress(head_sha256: String) -> Self {
        Self {
            stages: Vec::new(),
            next_prior_sha256: Some(head_sha256),
        }
    }

    /// Whether any stage completed before this process.
    pub(super) fn has_stages(&self) -> bool {
        !self.stages.is_empty()
    }

    /// The recorded stage of `kind`, when it completed before this process.
    pub(super) fn completed(&self, kind: ShapeStageKind) -> Option<&ShapeStage> {
        self.stages.iter().find(|stage| stage.stage == kind)
    }

    /// Whether the segments of `family` were retired by a completed stage.
    pub(super) fn retires_family(&self, family: PartitionFamily) -> bool {
        self.stages
            .iter()
            .any(|stage| stage.stage.retired_family() == Some(family))
    }

    /// Recorded successors that are still the authority for their rows: every
    /// stage output that no later completed stage consumed.
    pub(super) fn live_outputs(&self) -> Vec<&ArtifactReceipt> {
        self.stages
            .iter()
            .filter(|stage| {
                !self
                    .stages
                    .iter()
                    .any(|later| later.stage.retired_output() == Some(stage.stage))
            })
            .flat_map(|stage| stage.outputs.iter())
            .collect()
    }

    /// Names of [`Self::live_outputs`].
    pub(super) fn live_output_names(&self) -> BTreeSet<String> {
        self.live_outputs()
            .into_iter()
            .map(|receipt| receipt.name.clone())
            .collect()
    }

    /// Whether a segment named `name` may be claimed through the progress
    /// chain. Resolved segments are never routed by the boundary loop, so only
    /// a `ResolvedRouted` stage can claim them; a retired family's segments
    /// are superseded by the stage that retired them.
    pub(super) fn boundary_may_claim(&self, name: &str) -> bool {
        parse_segment_name(name).is_none_or(|segment| match segment.family {
            Some(PartitionFamily::Resolved) => false,
            Some(family) => !self.retires_family(family),
            None => true,
        })
    }

    /// Install the next stage, returning once it is durable. Stages must be
    /// recorded in [`ShapeStageKind::ORDER`].
    pub(super) fn record(
        &mut self,
        root: &StableDirectory,
        checkpoint: &Checkpoint,
        kind: ShapeStageKind,
        result: StageResult,
    ) -> Result<(), GfError> {
        let index = self.stages.len();
        if ShapeStageKind::ORDER.get(index) != Some(&kind) || !kind.admits(&result.outputs) {
            return Err(storage("construction shape stage is out of order"));
        }
        let prior_sha256 = self
            .next_prior_sha256
            .clone()
            .ok_or_else(|| storage("construction shape stage lacks a progress head"))?;
        let stage = ShapeStage {
            format_version: checkpoint.format_version,
            operation_uuid: checkpoint.operation_uuid,
            project_identity: checkpoint.project_identity.clone(),
            session_identity: checkpoint.session_identity.clone(),
            parent_topology_generation: checkpoint.parent_topology_generation,
            ontology_mode: checkpoint.ontology_mode,
            semantic_authority_sha256: checkpoint.semantic_authority_sha256.clone(),
            budgets: checkpoint.budgets,
            last_receipt_sha256: checkpoint.last_receipt_sha256.clone(),
            index: u32::try_from(index).map_err(storage)?,
            stage: kind,
            prior_sha256,
            outputs: result.outputs,
            rows: result.rows,
            new_nodes: result.new_nodes,
            new_edges: result.new_edges,
        };
        let digest = control_sha256(&stage)?;
        install_control(root, &shape_stage_name(kind), &stage)?;
        self.next_prior_sha256 = Some(digest);
        self.stages.push(stage);
        Ok(())
    }
}

fn validate_shape_stage(stage: &ShapeStage, checkpoint: &Checkpoint) -> Result<(), GfError> {
    if stage.format_version != checkpoint.format_version
        || stage.operation_uuid != checkpoint.operation_uuid
        || stage.project_identity != checkpoint.project_identity
        || stage.session_identity != checkpoint.session_identity
        || stage.parent_topology_generation != checkpoint.parent_topology_generation
        || stage.ontology_mode != checkpoint.ontology_mode
        || stage.semantic_authority_sha256 != checkpoint.semantic_authority_sha256
        || stage.budgets != checkpoint.budgets
        || stage.last_receipt_sha256 != checkpoint.last_receipt_sha256
        || !is_canonical_sha256(&stage.prior_sha256)
        || !stage
            .outputs
            .iter()
            .all(|output| is_canonical_sha256(&output.sha256))
        || (stage.stage != ShapeStageKind::ResolvedRouted && !stage.rows.is_empty())
        || (stage.stage != ShapeStageKind::Assigned
            && (stage.new_nodes != 0 || stage.new_edges != 0))
        || !stage.stage.admits(&stage.outputs)
    {
        return Err(storage("construction shape stage authority changed"));
    }
    Ok(())
}

/// Load the finish stages chained to the progress head.
///
/// A stage exists only after routing completed, so any stage at all requires
/// the head boundary to cover every staged chunk. Indices must be exactly
/// `0..n` in [`ShapeStageKind::ORDER`] and chained by body digest from the
/// head: a gap, a reorder or a rewritten stage inside the chain is refused.
/// The last stage's body is not referenced by anything yet; its outputs are
/// authenticated against their payloads and writer receipts at the resume
/// boundary instead, and its kind is fixed by its index.
pub(super) fn load_shape_stages(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
    chain: &[LoadedShapeProgress],
) -> Result<ShapeStages, GfError> {
    let mut indices = Vec::new();
    for name in root.child_names().map_err(storage)? {
        let Some(name) = name.to_str() else { continue };
        if name.starts_with(SHAPE_STAGE_PREFIX) {
            let index = parse_shape_stage_name(name)
                .ok_or_else(|| storage("construction shape stage name is invalid"))?;
            indices.push(index);
        }
    }
    indices.sort_unstable();
    let Some(head) = chain.last() else {
        if indices.is_empty() {
            return Ok(ShapeStages::default());
        }
        return Err(storage("construction shape stage exists without progress"));
    };
    let mut stages = ShapeStages::after_progress(head.body_sha256.clone());
    if !indices.is_empty() && head.retired_through() != checkpoint.next_sequence {
        return Err(storage(
            "construction shape stage precedes routing completion",
        ));
    }
    for (expected, index) in indices.into_iter().enumerate() {
        let kind = ShapeStageKind::ORDER
            .get(index)
            .copied()
            .filter(|_| index == expected)
            .ok_or_else(|| storage("construction shape stages are not contiguous"))?;
        let name = shape_stage_name(kind);
        let mut file = root.open_child_file(OsStr::new(&name)).map_err(storage)?;
        let body = read_bounded_limit(&mut file, MAX_SHAPE_CONTROL_BYTES)?;
        let stage: ShapeStage = serde_json::from_slice(&body).map_err(storage)?;
        validate_shape_stage(&stage, checkpoint)?;
        if stage.stage != kind
            || usize::try_from(stage.index).ok() != Some(index)
            || stage.prior_sha256.as_str()
                != stages
                    .next_prior_sha256
                    .as_deref()
                    .expect("a progress head seeds the chain")
        {
            return Err(storage("construction shape stage chain changed"));
        }
        stages.next_prior_sha256 = Some(control_sha256(&stage)?);
        stages.stages.push(stage);
    }
    Ok(stages)
}

/// Authenticate every live stage successor once at the resume boundary and
/// record its installation in the allocation ledger, exactly as claimed
/// segments are (#1418).
///
/// Each successor must still be the file its writer installed: its writer
/// receipt must equal the one the stage recorded, and its payload must hash
/// to it. A successor mutated in place after its stage retired the inputs it
/// replaced is refused rather than consumed (the #1269 class): nothing else
/// can reproduce its rows.
pub(super) fn authenticate_stage_outputs(
    root: &StableDirectory,
    stages: &ShapeStages,
    evidence: &mut super::GraphConstructionEvidence,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<(), GfError> {
    for receipt in stages.live_outputs() {
        super::reject_cancelled(cancelled)?;
        let mut file = root
            .open_child_file(OsStr::new(&super::shape_receipt_name(&receipt.name)))
            .map_err(storage)?;
        if super::file_link_count(&file).map_err(storage)? != 1 {
            return Err(storage("shaped writer capability has extra links"));
        }
        let written: ArtifactReceipt = decode_bounded(&mut file)?;
        drop(file);
        if written != *receipt {
            return Err(storage("construction shape stage output receipt changed"));
        }
        let work = super::supersession::authenticate_payload(root, receipt, cancelled)?;
        super::progress::charge_segment_reads(
            evidence,
            work.bytes,
            work.operations,
            &work.cache_release,
        )?;
        let key = format!(
            "{:016x}:{}",
            receipt.identity.volume_serial, receipt.identity.file_id
        );
        if evidence.storage_active_identity_allocated_bytes.get(&key)
            != Some(&receipt.allocated_bytes)
        {
            super::record_shape_artifact_install(evidence, receipt)?;
        }
    }
    Ok(())
}

/// Remove every stage control. Only valid once the shape is complete and
/// superseded, alongside the progress controls they chain to.
pub(super) fn unlink_shape_stages(root: &StableDirectory) -> Result<(), GfError> {
    let mut names = Vec::new();
    for name in root.child_names().map_err(storage)? {
        let Some(name) = name.to_str() else { continue };
        if parse_shape_stage_name(name).is_some() {
            names.push(name.to_owned());
        }
    }
    for name in names {
        let mut file = root.open_child_file(OsStr::new(&name)).map_err(storage)?;
        // A resolved-routed stage lists one segment per partition, so it is
        // bounded like the other shape controls, not like a checkpoint.
        let _: ShapeStage =
            serde_json::from_slice(&read_bounded_limit(&mut file, MAX_SHAPE_CONTROL_BYTES)?)
                .map_err(storage)?;
        drop(file);
        super::unlink_named(root, &name)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::partition_shaping::{PartitionFamily, fixed_spill_name};
    use super::super::progress::{
        IDENTITY_TAG, ShapeProgress, ShapeProgressPartition, install_shape_progress,
        load_shape_progress_chain,
    };
    use super::super::tests::open;
    use super::super::{ArtifactReceipt, IDENTITY_WIDTH, MAX_CONTROL_BYTES};
    use super::{
        ShapeStageKind, ShapeStages, StageResult, load_shape_stages, parse_shape_stage_name,
        shape_stage_name, unlink_shape_stages,
    };
    use tempfile::TempDir;

    /// A resolved-routed stage names one segment per partition, and the
    /// partition count saturates at 4096: its control is larger than the
    /// general 1 MiB control bound at admission scale, and every reader must
    /// accept it (found at S22, where supersession refused its own stage).
    #[test]
    fn resolved_stage_controls_at_the_partition_ceiling_round_trip() {
        let root = TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut session = open(&root, 0x1562);
        session
            .append(
                super::super::ConstructionChunkKind::Node,
                "nodes",
                &super::super::tests::node_batch(1, 64),
            )
            .unwrap();
        let receipt = session.read_receipt(0).unwrap();
        let progress = ShapeProgress::new(
            &session.checkpoint,
            session.checkpoint.next_sequence,
            None,
            vec![ShapeProgressPartition {
                tag: IDENTITY_TAG.to_owned(),
                rows: vec![receipt.identities.bytes / IDENTITY_WIDTH as u64],
            }],
        );
        let head = install_shape_progress(&session.root, &progress).unwrap();
        let mut stages = ShapeStages::after_progress(head);
        let identities = receipt.identities.clone();
        for kind in &ShapeStageKind::ORDER[..5] {
            let outputs = match kind {
                ShapeStageKind::Identities => vec![ArtifactReceipt {
                    name: super::STAGED_IDENTITIES.to_owned(),
                    ..identities.clone()
                }],
                ShapeStageKind::Assigned => vec![ArtifactReceipt {
                    name: super::SHAPED_IDENTITIES.to_owned(),
                    ..identities.clone()
                }],
                _ => Vec::new(),
            };
            stages
                .record(
                    &session.root,
                    &session.checkpoint,
                    *kind,
                    StageResult {
                        outputs,
                        ..StageResult::default()
                    },
                )
                .unwrap();
        }
        let partitions = 4096;
        let segments = (0..partitions)
            .map(|partition| ArtifactReceipt {
                name: fixed_spill_name(
                    PartitionFamily::Resolved,
                    session.checkpoint.next_sequence,
                    partition,
                ),
                ..identities.clone()
            })
            .collect();
        stages
            .record(
                &session.root,
                &session.checkpoint,
                ShapeStageKind::ResolvedRouted,
                StageResult {
                    outputs: segments,
                    rows: vec![1; partitions],
                    ..StageResult::default()
                },
            )
            .unwrap();
        let control = session
            .root
            .path()
            .join(shape_stage_name(ShapeStageKind::ResolvedRouted));
        assert!(
            std::fs::metadata(&control).unwrap().len() > MAX_CONTROL_BYTES,
            "the fixture must exceed the general control bound"
        );
        let chain = load_shape_progress_chain(&session.root, &session.checkpoint).unwrap();
        let loaded = load_shape_stages(&session.root, &session.checkpoint, &chain).unwrap();
        assert_eq!(
            loaded
                .completed(ShapeStageKind::ResolvedRouted)
                .unwrap()
                .outputs
                .len(),
            partitions
        );
        unlink_shape_stages(&session.root).unwrap();
        assert!(!control.exists());
    }

    #[test]
    fn stage_names_round_trip_in_completion_order() {
        for (index, kind) in ShapeStageKind::ORDER.into_iter().enumerate() {
            assert_eq!(parse_shape_stage_name(&shape_stage_name(kind)), Some(index));
        }
        assert_eq!(parse_shape_stage_name("shape-stage-1.json"), None);
        assert_eq!(parse_shape_stage_name("shape-stage-0a.json"), None);
        assert_eq!(parse_shape_stage_name("shape-progress-00.json"), None);
    }
}
