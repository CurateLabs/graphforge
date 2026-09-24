//! Durable shaping progress controls (#1418).
//!
//! At a group boundary the routing loop seals every open partition spill,
//! installs one `shape-progress-<boundary>.json` control, and only then
//! unlinks the boundary's retired staged inputs. The control is what makes
//! those unlinks safe: it exists only after the group's routed bytes are
//! sealed and durable, so a resumed shape can rebuild its partitioners from
//! the sealed segments the controls claim instead of re-reading inputs that
//! no longer exist.
//!
//! Controls are append-only — one per boundary, named by the boundary — and
//! chain by body digest. A corrupt, gapped or partially rewritten chain is
//! refused rather than silently re-scoped: a forward-lying boundary would
//! make recovery resume past inputs that were never sealed, which is exactly
//! the silent-partial-result hazard this module exists to prevent.

use super::{
    Checkpoint, GfError, GraphConstructionBudgets, IdentityRecord, MAX_SHAPE_CONTROL_BYTES, OsStr,
    StableDirectory, Uuid, control_sha256, decode_bounded, install_control, is_canonical_sha256,
    read_bounded_limit, storage,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Prefix of every shaping progress control.
pub(super) const SHAPE_PROGRESS_PREFIX: &str = "shape-progress-";

impl ShapeProgress {
    /// Bind a boundary to the session checkpoint's authority.
    pub(super) fn new(
        checkpoint: &Checkpoint,
        retired_through: u64,
        prior_shape_progress_sha256: Option<String>,
        partition_rows: Vec<ShapeProgressPartition>,
    ) -> Self {
        Self {
            format_version: checkpoint.format_version,
            operation_uuid: checkpoint.operation_uuid,
            project_identity: checkpoint.project_identity.clone(),
            session_identity: checkpoint.session_identity.clone(),
            parent_topology_generation: checkpoint.parent_topology_generation,
            ontology_mode: checkpoint.ontology_mode,
            semantic_authority_sha256: checkpoint.semantic_authority_sha256.clone(),
            budgets: checkpoint.budgets,
            last_receipt_sha256: checkpoint.last_receipt_sha256.clone(),
            retired_through,
            prior_shape_progress_sha256,
            partition_rows,
        }
    }
}

/// One durable group boundary: the chunks `[0, retired_through)` are fully
/// routed, their spill segments are sealed and durable, and their staged
/// inputs are unlinked.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct ShapeProgress {
    format_version: u32,
    operation_uuid: Uuid,
    project_identity: IdentityRecord,
    session_identity: IdentityRecord,
    parent_topology_generation: u64,
    ontology_mode: graphforge_core::OntologyMode,
    semantic_authority_sha256: Option<String>,
    budgets: GraphConstructionBudgets,
    last_receipt_sha256: Option<String>,
    /// Exclusive chunk sequence covered by this boundary.
    retired_through: u64,
    /// Body digest of the previous boundary's control; `None` for the first.
    prior_shape_progress_sha256: Option<String>,
    /// Routing counts charged per partitioner during this boundary's group.
    /// Cumulative balance is the sum over the whole chain, which is how a
    /// resumed shape restores exact per-partition row counts.
    partition_rows: Vec<ShapeProgressPartition>,
}

/// Routing counts for one partitioner over one boundary's group.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct ShapeProgressPartition {
    /// Partitioner tag: a fixed family name, or `rows:<16 hex>` for a row
    /// schema group.
    pub(super) tag: String,
    /// Rows routed into each partition, in partition order.
    pub(super) rows: Vec<u64>,
}

/// One loaded and chain-validated progress control.
#[derive(Clone, Debug)]
pub(super) struct LoadedShapeProgress {
    pub(super) progress: ShapeProgress,
    /// Body digest, chaining into the next boundary's control.
    pub(super) body_sha256: String,
}

impl LoadedShapeProgress {
    pub(super) fn retired_through(&self) -> u64 {
        self.progress.retired_through
    }

    /// The cumulative per-partition routing counts over the chain ending at
    /// this boundary.
    pub(super) fn cumulative_rows(
        chain: &[LoadedShapeProgress],
    ) -> Result<BTreeMap<String, Vec<u64>>, GfError> {
        let mut totals: BTreeMap<String, Vec<u64>> = BTreeMap::new();
        for loaded in chain {
            for partition in &loaded.progress.partition_rows {
                let rows = totals.entry(partition.tag.clone()).or_default();
                if rows.is_empty() {
                    rows.resize(partition.rows.len(), 0);
                }
                if rows.len() != partition.rows.len() {
                    return Err(storage(
                        "shape progress partition count differs between boundaries",
                    ));
                }
                for (slot, rows) in rows.iter_mut().zip(&partition.rows) {
                    *slot = slot
                        .checked_add(*rows)
                        .ok_or_else(|| storage("shape progress row count overflows"))?;
                }
            }
        }
        Ok(totals)
    }
}

impl ShapeResume {
    /// Cumulative restored routing rows for `tag`, sized to `partitions`.
    pub(super) fn rows_for(&self, tag: &str, partitions: usize) -> Vec<u64> {
        self.segment_rows.get(tag).map_or_else(
            || vec![0; partitions],
            |rows| {
                let mut rows = rows.clone();
                rows.resize(partitions, 0);
                rows
            },
        )
    }

    /// Restored sealed segments for `tag`, empty when none were claimed.
    pub(super) fn segments_for(&self, tag: &str) -> &[Vec<super::ArtifactReceipt>] {
        self.segments.get(tag).map_or(&[], Vec::as_slice)
    }
}

/// The durable name of the boundary's control.
pub(super) fn shape_progress_name(boundary: u64) -> String {
    format!("{SHAPE_PROGRESS_PREFIX}{boundary:020}.json")
}

/// Parse a progress control name into its boundary.
pub(super) fn parse_shape_progress_name(name: &str) -> Option<u64> {
    let body = name.strip_prefix(SHAPE_PROGRESS_PREFIX)?;
    let body = body.strip_suffix(".json")?;
    if body.len() != 20 || !body.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    body.parse().ok()
}

fn validate_shape_progress(
    progress: &ShapeProgress,
    checkpoint: &Checkpoint,
) -> Result<(), GfError> {
    if progress.format_version != checkpoint.format_version
        || progress.operation_uuid != checkpoint.operation_uuid
        || progress.project_identity != checkpoint.project_identity
        || progress.session_identity != checkpoint.session_identity
        || progress.parent_topology_generation != checkpoint.parent_topology_generation
        || progress.ontology_mode != checkpoint.ontology_mode
        || progress.semantic_authority_sha256 != checkpoint.semantic_authority_sha256
        || progress.budgets != checkpoint.budgets
        || progress.last_receipt_sha256 != checkpoint.last_receipt_sha256
        || progress.retired_through == 0
        || progress.retired_through > checkpoint.next_sequence
        || progress
            .prior_shape_progress_sha256
            .as_ref()
            .is_some_and(|digest| !is_canonical_sha256(digest))
    {
        return Err(storage("construction shape progress authority changed"));
    }
    Ok(())
}

/// Install the boundary's control, returning its body digest for chaining.
pub(super) fn install_shape_progress(
    root: &StableDirectory,
    progress: &ShapeProgress,
) -> Result<String, GfError> {
    let digest = control_sha256(progress)?;
    install_control(
        root,
        &shape_progress_name(progress.retired_through),
        progress,
    )?;
    Ok(digest)
}

/// Load the complete boundary chain, in boundary order, validating every
/// binding and the digest chain.
///
/// The head boundary gets one more proof: the chain's cumulative identity-row
/// count must equal the identity records recorded in the chunk receipts below
/// the boundary. The chain digest protects every control but the head —
/// nothing references the head's digest yet — so a same-inode corruption that
/// moved the head forward would otherwise make a resumed shape resume past
/// inputs that were never sealed. The receipts are the independent count.
pub(super) fn load_shape_progress_chain(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
) -> Result<Vec<LoadedShapeProgress>, GfError> {
    let mut boundaries = Vec::new();
    for name in root.child_names().map_err(storage)? {
        let Some(name) = name.to_str() else { continue };
        if let Some(boundary) = parse_shape_progress_name(name) {
            boundaries.push((boundary, name.to_owned()));
        }
    }
    boundaries.sort();
    let mut chain = Vec::with_capacity(boundaries.len());
    let mut previous_boundary = None;
    let mut previous_digest = None;
    for (boundary, name) in boundaries {
        if Some(boundary) <= previous_boundary {
            return Err(storage("construction shape progress boundaries regress"));
        }
        previous_boundary = Some(boundary);
        let mut file = root.open_child_file(OsStr::new(&name)).map_err(storage)?;
        let body = read_bounded_limit(&mut file, MAX_SHAPE_CONTROL_BYTES)?;
        let progress: ShapeProgress = serde_json::from_slice(&body).map_err(storage)?;
        validate_shape_progress(&progress, checkpoint)?;
        if progress.retired_through != boundary {
            return Err(storage(
                "construction shape progress name differs from boundary",
            ));
        }
        if progress.prior_shape_progress_sha256 != previous_digest {
            return Err(storage("construction shape progress chain changed"));
        }
        previous_digest = Some(sha256_of_body(&body));
        chain.push(LoadedShapeProgress {
            progress,
            body_sha256: previous_digest.clone().expect("just computed"),
        });
    }
    if let Some(head) = chain.last() {
        let routed = LoadedShapeProgress::cumulative_rows(&chain)?
            .get(IDENTITY_TAG)
            .map_or(0, |rows| rows.iter().copied().sum());
        let mut staged = 0_u64;
        for sequence in 0..head.progress.retired_through {
            staged = staged
                .checked_add(staged_identity_records(root, checkpoint, sequence)?)
                .ok_or_else(|| storage("shape progress identity record count overflows"))?;
        }
        if routed != staged {
            return Err(storage(
                "construction shape progress boundary exceeds routed input",
            ));
        }
    }
    Ok(chain)
}

/// The partitioner tag of the joint identity family.
pub(super) const IDENTITY_TAG: &str = "identities";

/// Identity records staged by one chunk receipt, per the receipt's own
/// authority.
fn staged_identity_records(
    root: &StableDirectory,
    checkpoint: &Checkpoint,
    sequence: u64,
) -> Result<u64, GfError> {
    use super::IDENTITY_WIDTH;
    let mut file = root
        .open_child_file(OsStr::new(&*super::receipt_name(sequence)))
        .map_err(storage)?;
    let receipt: super::ConstructionChunkReceipt = decode_bounded(&mut file)?;
    if receipt.operation_uuid != checkpoint.operation_uuid
        || receipt.project_identity != checkpoint.project_identity
        || receipt.session_identity != checkpoint.session_identity
    {
        return Err(storage("shape progress receipt authority changed"));
    }
    if !receipt
        .identities
        .bytes
        .is_multiple_of(IDENTITY_WIDTH as u64)
    {
        return Err(storage("shape progress identity run is not record aligned"));
    }
    Ok(receipt.identities.bytes / IDENTITY_WIDTH as u64)
}

fn sha256_of_body(body: &[u8]) -> String {
    use sha2::Digest;
    super::hex(sha2::Sha256::digest(body).as_slice())
}

/// Remove every progress control. Only valid once the shape is complete and
/// superseded: the controls are dead weight from the first boundary onward.
pub(super) fn unlink_shape_progress(root: &StableDirectory) -> Result<(), GfError> {
    let mut names = Vec::new();
    for name in root.child_names().map_err(storage)? {
        let Some(name) = name.to_str() else { continue };
        if parse_shape_progress_name(name).is_some() {
            names.push(name.to_owned());
        }
    }
    if names.is_empty() {
        return Ok(());
    }
    for name in names {
        let mut file = root.open_child_file(OsStr::new(&name)).map_err(storage)?;
        let _: ShapeProgress = decode_bounded(&mut file)?;
        drop(file);
        super::unlink_named(root, &name)?;
    }
    Ok(())
}

/// Sealed-segment state a resumed shape rebuilds its partitioners from
/// (#1418).
///
/// Everything here was authenticated at the resume boundary: receipts against
/// the durable name grammar and payloads against their recorded digests. The
/// balance rows are the exact cumulative routing counts restored from the
/// progress chain, so a resumed session records the same partition balance,
/// evidence counters and `partition_identity_rows` an uninterrupted shape
/// would have.
pub(super) struct ShapeResume {
    /// Exclusive chunk sequence the routing loop resumes at.
    pub(super) retired_through: u64,
    /// Baseline evidence recorded by the installed incomplete shape intent;
    /// the completed intent must carry the same baseline.
    pub(super) baseline_evidence: super::GraphConstructionEvidence,
    /// Recorded joint splitters from the installed intent.
    pub(super) splitters: Vec<[u8; 16]>,
    /// Recorded node-only splitters from the installed intent.
    pub(super) node_splitters: Vec<[u8; 16]>,
    /// Claimed sealed segments per partitioner tag, per partition, in
    /// boundary order.
    pub(super) segments: BTreeMap<String, Vec<Vec<super::ArtifactReceipt>>>,
    /// Cumulative routing rows per partitioner tag, per partition.
    pub(super) segment_rows: BTreeMap<String, Vec<u64>>,
    /// Arrow IPC schema of each claimed row-group tag.
    pub(super) row_schemas: BTreeMap<String, arrow::datatypes::SchemaRef>,
    /// Body digest of the chain head, chaining the next installed boundary.
    pub(super) last_progress_sha256: Option<String>,
    /// Finish stages completed before the interruption (#1562), whose
    /// successors were authenticated at the resume boundary.
    pub(super) stages: super::finish_stages::ShapeStages,
}

/// Sweep the construction root's partition-spill segments.
///
/// A segment is **claimed** when its sealing boundary is at or below
/// `retired_through` and a valid writer receipt names it; claimed segments are
/// the resume state. Everything else — a segment beyond the recorded boundary
/// (a crash between sealing and the control install) or a segment whose
/// writer receipt is absent — is unclaimed: its records were never covered by
/// a boundary, its chunks are still on disk, and re-routing reproduces it
/// deterministically. An unclaimed segment is authenticated against its
/// receipt (when one exists) **before** it is unlinked, then removed together
/// with that receipt; the authentication keeps the discard fail-closed against
/// in-place payload corruption (#1392).
///
/// Finish stages (#1562) narrow the claim: a segment a recorded stage still
/// hands forward is neither claimed nor discarded here (the stage owns it),
/// and a segment whose family a completed stage already retired is unclaimed
/// even below the boundary — its rows live on in the stage's successor, so a
/// copy left by a crash between the stage install and the unlink is garbage.
///
/// The returned scan carries claimed receipt sets only.
pub(super) fn scan_shape_segments(
    root: &StableDirectory,
    retired_through: u64,
    stages: &super::finish_stages::ShapeStages,
    evidence: &mut super::GraphConstructionEvidence,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<BTreeMap<String, Vec<Vec<super::ArtifactReceipt>>>, GfError> {
    use super::partition_shaping::parse_segment_name;
    let mut claimed: BTreeMap<String, Vec<Vec<super::ArtifactReceipt>>> = BTreeMap::new();
    let mut unclaimed: Vec<(String, Option<super::ArtifactReceipt>)> = Vec::new();
    let handed_forward = stages.live_output_names();
    for name in root.child_names().map_err(storage)? {
        let Some(text) = name.to_str() else { continue };
        let Some(segment) = parse_segment_name(text) else {
            continue;
        };
        if handed_forward.contains(text) {
            continue;
        }
        let tag = segment.tag();
        let partition = segment.partition;
        let receipt = read_segment_writer_receipt(root, text)?;
        if segment.boundary > retired_through || !stages.boundary_may_claim(text) {
            unclaimed.push((text.to_owned(), receipt));
            continue;
        }
        let Some(receipt) = receipt else {
            unclaimed.push((text.to_owned(), None));
            continue;
        };
        let slot = claimed.entry(tag).or_default();
        if slot.len() <= partition {
            slot.resize(partition + 1, Vec::new());
        }
        slot[partition].push(receipt);
    }
    for (name, receipt) in unclaimed {
        if let Some(receipt) = &receipt {
            let work = super::supersession::authenticate_payload(root, receipt, cancelled)?;
            charge_segment_reads(evidence, work.bytes, work.operations, &work.cache_release)?;
            unlink_segment_file(root, &name)?;
        } else {
            unlink_segment_file(root, &name)?;
        }
    }
    for segments in claimed.values_mut() {
        for partition in segments.iter_mut() {
            // Boundary order is routing order within a partition.
            partition.sort_by_key(|receipt| {
                parse_segment_name(&receipt.name).map_or(0, |segment| segment.boundary)
            });
        }
    }
    Ok(claimed)
}

/// Read and validate the writer receipt naming one installed segment.
fn read_segment_writer_receipt(
    root: &StableDirectory,
    name: &str,
) -> Result<Option<super::ArtifactReceipt>, GfError> {
    let capability = super::shape_receipt_name(name);
    let mut file = match root.open_child_file(OsStr::new(&capability)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage(error)),
    };
    if super::file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("shaped writer capability has extra links"));
    }
    let receipt: super::ArtifactReceipt = decode_bounded(&mut file)?;
    if super::shape_receipt_name(receipt.name.as_str()) != capability
        || receipt.name != name
        || !super::is_shape_artifact_name(&receipt.name)
    {
        return Err(storage("segment writer receipt ownership changed"));
    }
    Ok(Some(receipt))
}

/// Unlink one segment payload against the identity the file currently has.
fn unlink_segment_file(root: &StableDirectory, name: &str) -> Result<(), GfError> {
    let file = root.open_child_file(OsStr::new(name)).map_err(storage)?;
    if super::file_link_count(&file).map_err(storage)? != 1 {
        return Err(storage("unclaimed segment has extra links"));
    }
    let identity = super::file_identity(&file).map_err(storage)?;
    drop(file);
    root.unlink_child_if_identity(OsStr::new(name), identity)
        .map_err(storage)?;
    root.sync().map_err(storage)
}

/// Authenticate every claimed segment payload once at the resume boundary and
/// record its installation in the session's allocation ledger, so supersession
/// retires it through the ordinary balanced path.
///
/// Fixed-width segments stream against their SHA-256 receipt; row segments
/// stream their corruption checksum and contribute their Arrow IPC schema for
/// the restored partitioner.
pub(super) fn authenticate_shape_segments(
    root: &StableDirectory,
    segments: &BTreeMap<String, Vec<Vec<super::ArtifactReceipt>>>,
    evidence: &mut super::GraphConstructionEvidence,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<BTreeMap<String, arrow::datatypes::SchemaRef>, GfError> {
    let mut row_schemas = BTreeMap::new();
    for (tag, partitions) in segments {
        for partition in partitions {
            for receipt in partition {
                super::reject_cancelled(cancelled)?;
                if super::partition_shaping::parse_segment_name(&receipt.name)
                    .is_none_or(|segment| segment.tag() != *tag)
                {
                    return Err(storage("claimed segment differs from its scan"));
                }
                if tag.starts_with("rows:") {
                    let (file, work) = super::recovery::authenticate_row_spill(root, receipt)?;
                    let schema = arrow::ipc::reader::StreamReader::try_new(&file, None)
                        .map_err(storage)?
                        .schema();
                    drop(file);
                    charge_segment_reads(
                        evidence,
                        work.bytes,
                        work.operations,
                        &work.cache_release,
                    )?;
                    row_schemas.insert(tag.clone(), schema);
                } else {
                    let work = super::supersession::authenticate_payload(root, receipt, cancelled)?;
                    charge_segment_reads(
                        evidence,
                        work.bytes,
                        work.operations,
                        &work.cache_release,
                    )?;
                }
                // A same-facade retry of an interrupted shape re-authenticates
                // segments whose ledger installs survived in memory from the
                // failed attempt; only a first sight (a fresh resume, whose
                // durable checkpoint predates the segments) installs.
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
        }
    }
    Ok(row_schemas)
}

pub(super) fn charge_segment_reads(
    evidence: &mut super::GraphConstructionEvidence,
    bytes: u64,
    operations: u64,
    cache_release: &graphforge_filesystem::FileCacheReleaseEvidence,
) -> Result<(), GfError> {
    evidence.recovery_application_read_bytes = evidence
        .recovery_application_read_bytes
        .checked_add(bytes)
        .ok_or_else(|| storage("resume segment read bytes overflow"))?;
    evidence.recovery_application_read_operations = evidence
        .recovery_application_read_operations
        .checked_add(operations)
        .ok_or_else(|| storage("resume segment read operations overflow"))?;
    super::account_cache_release(*cache_release, evidence)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::partition_shaping::{
        PartitionFamily, SegmentName, fixed_spill_name, parse_segment_name,
    };
    use super::super::tests::open;
    use super::{
        IDENTITY_TAG, ShapeProgress, ShapeProgressPartition, install_shape_progress,
        load_shape_progress_chain, parse_shape_progress_name,
    };
    use tempfile::TempDir;

    fn identity_tag_rows(rows: Vec<u64>) -> Vec<ShapeProgressPartition> {
        vec![ShapeProgressPartition {
            tag: IDENTITY_TAG.to_owned(),
            rows,
        }]
    }

    #[test]
    fn head_boundary_is_proven_against_the_receipted_input() {
        let root = TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut session = open(&root, 0x1418);
        session
            .append(
                super::super::ConstructionChunkKind::Node,
                "nodes",
                &super::super::tests::node_batch(1, 64),
            )
            .unwrap();
        let receipt = session.read_receipt(0).unwrap();
        let records = receipt.identities.bytes / super::super::IDENTITY_WIDTH as u64;
        let progress = ShapeProgress::new(
            &session.checkpoint,
            1,
            None,
            identity_tag_rows(vec![records]),
        );
        install_shape_progress(&session.root, &progress).unwrap();
        let chain = load_shape_progress_chain(&session.root, &session.checkpoint).unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].retired_through(), 1);

        // A same-inode corruption that moves the head past inputs whose
        // records the chain never routed must be refused, not resumed past.
        let name = super::shape_progress_name(1);
        let mut body = std::fs::read(session.root.path().join(&name)).unwrap();
        let rewritten = String::from_utf8(body.clone())
            .unwrap()
            .replace("\"retired_through\":1", "\"retired_through\":2");
        assert_ne!(rewritten, String::from_utf8(body.clone()).unwrap());
        body = rewritten.into_bytes();
        std::fs::write(session.root.path().join(&name), &body).unwrap();
        assert!(
            load_shape_progress_chain(&session.root, &session.checkpoint).is_err(),
            "a forward-lying head boundary was accepted"
        );
    }

    #[test]
    fn boundary_chains_refuse_digest_and_monotonicity_breaks() {
        let root = TempDir::new().unwrap();
        crate::open_or_initialize_project(root.path()).unwrap();
        let mut session = open(&root, 0x1419);
        for chunk in 0..2 {
            session
                .append(
                    super::super::ConstructionChunkKind::Node,
                    &format!("nodes-{chunk}"),
                    &super::super::tests::node_batch(1 + chunk * 64, 64),
                )
                .unwrap();
        }
        let records = |sequence: u64| {
            let receipt = session.read_receipt(sequence).unwrap();
            receipt.identities.bytes / super::super::IDENTITY_WIDTH as u64
        };
        let first = ShapeProgress::new(
            &session.checkpoint,
            1,
            None,
            identity_tag_rows(vec![records(0)]),
        );
        let first_digest = install_shape_progress(&session.root, &first).unwrap();
        let second = ShapeProgress::new(
            &session.checkpoint,
            2,
            Some(first_digest.clone()),
            identity_tag_rows(vec![records(1)]),
        );
        install_shape_progress(&session.root, &second).unwrap();
        assert_eq!(
            load_shape_progress_chain(&session.root, &session.checkpoint)
                .unwrap()
                .len(),
            2
        );

        // Tampering with a covered boundary breaks the next link's digest.
        let name = super::shape_progress_name(1);
        let body = std::fs::read(session.root.path().join(&name)).unwrap();
        let tampered = String::from_utf8(body)
            .unwrap()
            .replace("\"rows\":[64]", "\"rows\":[63]");
        std::fs::write(session.root.path().join(&name), tampered.as_bytes()).unwrap();
        assert!(
            load_shape_progress_chain(&session.root, &session.checkpoint).is_err(),
            "a rewritten boundary inside the chain was accepted"
        );
    }

    #[test]
    fn segment_names_round_trip_and_reject_lookalikes() {
        for family in PartitionFamily::ALL {
            let name = fixed_spill_name(family, 7, 4095);
            assert_eq!(
                parse_segment_name(&name),
                Some(SegmentName {
                    family: Some(family),
                    namespace: String::new(),
                    boundary: 7,
                    partition: 4095,
                }),
                "{name}"
            );
        }
        let rows = "part-rows-deadbeefdeadbeef-g00000000000000000042-p00007.arrow";
        assert_eq!(
            parse_segment_name(rows),
            Some(SegmentName {
                family: None,
                namespace: "deadbeefdeadbeef".to_owned(),
                boundary: 42,
                partition: 7,
            })
        );
        // Without the boundary group the name is not in the grammar.
        assert_eq!(parse_segment_name("part-identities-p00000.run"), None);
        assert_eq!(
            parse_segment_name("part-identities-g0000000000000000002-p00000.run"),
            None
        );
        assert_eq!(
            parse_segment_name("part-rows-deadbeefdeadbeef-p00000.run"),
            None
        );
        assert_eq!(
            parse_segment_name("chunk-00000000000000000000-node.parquet"),
            None
        );
        assert_eq!(
            parse_shape_progress_name("shape-progress-00000000000000000003.json"),
            Some(3)
        );
        assert_eq!(parse_shape_progress_name("shape-progress-3.json"), None);
    }
}
