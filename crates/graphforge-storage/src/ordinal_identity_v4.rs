//! Read-only authenticated v4 `node_id -> node_uuid` authority.
//!
//! This module deliberately exposes no publication API. Version-three state is
//! reported as rebuild-required and is never interpreted through this format.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::fmt::Write as _;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path};
use std::time::SystemTime;

use graphforge_filesystem::{FileIdentity, StableDirectory, file_identity, file_link_count};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Immutable v4 manifest version.
pub const ORDINAL_IDENTITY_V4: u32 = 4;
/// Canonical location below a graph project.
pub const ORDINAL_IDENTITY_MANIFEST: &str = "topology/uuid-membership/ordinal-v4-manifest.json";
const INDEX_DIR: &str = "topology/uuid-membership";
const MANIFEST_NAME: &str = "ordinal-v4-manifest.json";
const LOCK_NAME: &str = "ordinal-v4.lock";
const RECEIPT_FILE_NAME: &str = "ordinal-v4-receipt.json";
const RECEIPT_NAME: &str = "topology/uuid-membership/ordinal-v4-receipt.json";
const GENERATION_NAME: &str = "topology/generation.json";
const UUID_WIDTH: u64 = 16;
const FORWARD_RECORD_WIDTH: u64 = UUID_WIDTH + 8;
const FORWARD_RECORD_WIDTH_USIZE: usize = UUID_WIDTH_USIZE + 8;
const UUID_WIDTH_USIZE: usize = 16;
const TOMBSTONE_WIDTH: u64 = 8;
const TOMBSTONE_WIDTH_USIZE: usize = 8;
const TOMBSTONE_BLOCK_BYTES: u64 = 64 * 1024;
const ORDINAL_BLOCK_BYTES: u64 = 64 * 1024;
const ORDINAL_BLOCK_BYTES_USIZE: usize = 64 * 1024;
pub(crate) const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;
const STREAM_BYTES: usize = 1024 * 1024;
// BTree nodes, sorted request storage, caller-order output, and resolved-map
// entries are charged conservatively rather than as payload-only scalars.
const REQUEST_ENTRY_CHARGE: u64 = 128;
// Empty BTreeMap + VecDeque + accounting field retained in the handle.
const TOMBSTONE_CACHE_FIXED_CHARGE: usize = 64;
const TOMBSTONE_CACHE_ENTRY_CHARGE: usize = 192;
const DESCRIPTOR_FIXED_CHARGE: usize = 512;
const ARTIFACT_DESCRIPTOR_CHARGE: u64 = 256;
const ORDINAL_BLOCK_DESCRIPTOR_CHARGE: u64 = 192;
const TOMBSTONE_BLOCK_DESCRIPTOR_CHARGE: u64 = 224;
const ADMISSION_TRANSIENT_FIXED_CHARGE: u64 = 256;

/// Hard anonymous-memory and read-coalescing limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct V4OrdinalIdentityLimits {
    /// Maximum caller entries, including duplicates, in one lookup.
    pub max_requested: usize,
    /// Maximum byte gap folded into one range read.
    pub coalesce_gap_bytes: u64,
    /// Maximum allocation and payload size for one coalesced read.
    pub max_coalesced_read_bytes: usize,
    /// Total charged capacity retained by the handle-global tombstone cache.
    pub max_tombstone_cache_bytes: usize,
    /// Maximum conservatively charged retained manifest/descriptor metadata.
    pub max_descriptor_metadata_bytes: usize,
}

impl Default for V4OrdinalIdentityLimits {
    fn default() -> Self {
        Self {
            max_requested: 65_536,
            coalesce_gap_bytes: 4_096,
            max_coalesced_read_bytes: STREAM_BYTES,
            max_tombstone_cache_bytes: STREAM_BYTES,
            max_descriptor_metadata_bytes: 16 * STREAM_BYTES,
        }
    }
}

/// Physical artifact purpose; a descriptor cannot be reused across domains.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum V4OrdinalArtifactKind {
    /// UUID-sorted forward identity state (authenticated but not read here).
    ForwardIdentities,
    /// Packed UUIDs in contiguous node-id ordinal order.
    OrdinalUuids,
    /// Sorted deleted node IDs.
    NodeTombstones,
}

/// One immutable generation-bound artifact.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct V4OrdinalArtifact {
    /// Plain child filename under the index directory.
    pub name: String,
    /// Domain binding.
    pub kind: V4OrdinalArtifactKind,
    /// Topology generation that published the artifact.
    pub generation: u64,
    /// Exact authenticated byte length.
    pub bytes: u64,
    /// Lowercase SHA-256 digest.
    pub sha256: String,
}

/// A packed contiguous reverse-identity range.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct V4OrdinalRange {
    /// First nonzero node surrogate represented by byte zero.
    pub first_node_id: u64,
    /// Number of consecutive UUID records.
    pub count: u64,
    /// Immutable packed UUID artifact.
    pub artifact: V4OrdinalArtifact,
    /// Canonical authenticated fixed-size read fences.
    pub blocks: Vec<V4OrdinalBlock>,
}

/// One authenticated block of packed ordinal UUIDs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct V4OrdinalBlock {
    /// Byte offset in the ordinal artifact.
    pub offset: u64,
    /// Number of UUID records in this block.
    pub count: u64,
    /// Lowercase SHA-256 of the exact block bytes.
    pub sha256: String,
}

/// A newest-generation sparse deletion override.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct V4OrdinalTombstones {
    /// Generation whose deletions this file records.
    pub generation: u64,
    /// Sorted packed `u64` artifact.
    pub artifact: V4OrdinalArtifact,
    /// Authenticated fixed-size search fences for bounded selected reads.
    pub blocks: Vec<V4OrdinalTombstoneBlock>,
}

/// One authenticated sorted tombstone block fence.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct V4OrdinalTombstoneBlock {
    /// Byte offset in the tombstone artifact.
    pub offset: u64,
    /// Number of packed IDs in this block.
    pub count: u64,
    /// First ID in the block.
    pub first: u64,
    /// Last ID in the block.
    pub last: u64,
    /// Lowercase SHA-256 of the exact block bytes.
    pub sha256: String,
}

/// Generation-pinned v4 authority descriptor.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct V4OrdinalIdentityManifest {
    /// Must equal [`ORDINAL_IDENTITY_V4`].
    pub format_version: u32,
    /// Exact topology generation served by this snapshot.
    pub(crate) topology_generation: u64,
    /// Individually UUID-sorted immutable forward runs in strictly increasing
    /// publication-generation order, included to reject mixed authority.
    pub forward_identities: Vec<V4OrdinalArtifact>,
    /// Nonoverlapping packed ordinal ranges.
    pub ordinal_ranges: Vec<V4OrdinalRange>,
    /// Sparse deletion overrides in strictly increasing generation order.
    pub tombstones: Vec<V4OrdinalTombstones>,
}

/// Generation authority pinned by the caller's authenticated project receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct V4OrdinalIdentityAuthority {
    /// Exact topology generation authorized by the project root.
    pub topology_generation: u64,
    /// Lowercase SHA-256 of the authorized membership manifest bytes.
    pub(crate) manifest_sha256: String,
}

/// Opaque ordinal authority authenticated through one pinned project generation.
#[derive(Clone, Debug)]
pub struct AuthenticatedV4OrdinalIdentityAuthority {
    pub(crate) authority: V4OrdinalIdentityAuthority,
}

impl AuthenticatedV4OrdinalIdentityAuthority {
    #[cfg(test)]
    pub(crate) fn authority(&self) -> &V4OrdinalIdentityAuthority {
        &self.authority
    }

    /// Open the selected ordinal facet at an admitted graph root.
    pub fn open(
        &self,
        graph_root: &Path,
        limits: V4OrdinalIdentityLimits,
    ) -> Result<V4OrdinalIdentityOpen, V4OrdinalIdentityError> {
        V4OrdinalIdentityHandle::open(graph_root, &self.authority, limits)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedOrdinalReceipt {
    nonce: String,
    expected_generation: u64,
    topology_delta_sha256: String,
    manifest_sha256: String,
}

impl crate::ResolvedProjectGeneration {
    /// Resolve the ordinal receipt through the pinned generation's authenticated
    /// graph/files participant. Absence is a clean rebuild requirement; any
    /// partial or malformed residue fails closed.
    pub fn authenticated_v4_ordinal_authority(
        &self,
    ) -> Result<Option<AuthenticatedV4OrdinalIdentityAuthority>, graphforge_core::GfError> {
        let mut targeted_state = crate::graph_manifest::GraphManifestTargetedState::default();
        let receipt = self.authenticated_graph_file_bytes_with_state(
            RECEIPT_NAME,
            MAX_MANIFEST_BYTES,
            Some(&mut targeted_state),
        )?;
        let manifest = self.authenticated_graph_file_bytes_with_state(
            ORDINAL_IDENTITY_MANIFEST,
            MAX_MANIFEST_BYTES,
            Some(&mut targeted_state),
        )?;
        match (receipt, manifest) {
            (None, None) => Ok(None),
            (None, Some(_)) | (Some(_), None) => Err(graphforge_core::GfError::Validation(
                "selected ordinal facet has incomplete authority residue".into(),
            )),
            (Some((_, receipt_bytes)), Some((manifest_entry, manifest_bytes))) => {
                let receipt: SelectedOrdinalReceipt = serde_json::from_slice(&receipt_bytes)
                    .map_err(|_| {
                        graphforge_core::GfError::Validation(
                            "selected ordinal receipt is malformed".into(),
                        )
                    })?;
                let generation = self
                    .authenticated_graph_file_bytes_with_state(
                        GENERATION_NAME,
                        MAX_MANIFEST_BYTES,
                        Some(&mut targeted_state),
                    )?
                    .ok_or_else(|| {
                        graphforge_core::GfError::Validation(
                            "selected topology generation authority is absent".into(),
                        )
                    })?;
                let generation: serde_json::Value =
                    serde_json::from_slice(&generation.1).map_err(|_| {
                        graphforge_core::GfError::Validation(
                            "selected topology generation authority is malformed".into(),
                        )
                    })?;
                let selected_generation = generation
                    .get("topology_generation")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| {
                        graphforge_core::GfError::Validation(
                            "selected topology generation is missing".into(),
                        )
                    })?;
                let manifest_digest = hex(&Sha256::digest(&manifest_bytes));
                let canonical_hex = |value: &str, length: usize| {
                    value.len() == length
                        && value
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
                };
                if !canonical_hex(&receipt.nonce, 32)
                    || !canonical_hex(&receipt.topology_delta_sha256, 64)
                    || receipt.expected_generation != selected_generation
                    || receipt.manifest_sha256 != manifest_entry.content_sha256
                    || receipt.manifest_sha256 != manifest_digest
                {
                    return Err(graphforge_core::GfError::Validation(
                        "selected ordinal receipt does not authenticate its manifest".into(),
                    ));
                }
                Ok(Some(AuthenticatedV4OrdinalIdentityAuthority {
                    authority: V4OrdinalIdentityAuthority {
                        topology_generation: selected_generation,
                        manifest_sha256: manifest_digest,
                    },
                }))
            }
        }
    }
}

/// Typed open disposition. V3 is never parsed as v4.
#[derive(Debug)]
pub enum V4OrdinalIdentityOpen {
    /// Fully authenticated v4 handle.
    Ready(Box<V4OrdinalIdentityHandle>),
    /// A valid version marker that requires an explicit rebuild.
    RebuildRequired {
        /// Version found in the manifest.
        found_version: u32,
    },
}

/// Discovery result for the additive ordinal facet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum V4OrdinalIdentityDiscovery {
    /// The v4 path exists and must be authenticated by [`V4OrdinalIdentityHandle::open`].
    Present,
    /// Current canonical v3 node/edge authority is valid but ordinal v4 is absent.
    RebuildRequired {
        /// Canonical legacy facet version that requires ordinal construction.
        found_version: u32,
    },
}

/// Fail-closed v4 admission or lookup error.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum V4OrdinalIdentityError {
    /// Filesystem or JSON operation failed.
    #[error("v4 ordinal identity I/O failed")]
    Io,
    /// Descriptor or manifest is noncanonical.
    #[error("v4 ordinal identity descriptor is invalid: {0}")]
    InvalidDescriptor(&'static str),
    /// Expected and manifest topology generations differ.
    #[error("v4 ordinal identity generation mismatch: expected {expected}, found {found}")]
    GenerationMismatch {
        /// Requested topology generation.
        expected: u64,
        /// Manifest topology generation.
        found: u64,
    },
    /// Immutable artifact authentication failed.
    #[error("v4 ordinal identity artifact authentication failed")]
    Authentication,
    /// Lookup was rejected before request-sized allocation.
    #[error("v4 ordinal identity request exceeds bound {maximum}: {requested}")]
    RequestLimit {
        /// Caller entry count including duplicates.
        requested: usize,
        /// Configured maximum.
        maximum: usize,
    },
}

/// Aggregate-only admission work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct V4OrdinalAdmissionMetrics {
    /// Immutable files authenticated.
    pub artifacts: u64,
    /// Bytes read for authentication and bounded cross-run validation.
    pub authenticated_bytes: u64,
    /// Successful sequential artifact reads during admission.
    pub sequential_read_calls: u64,
    /// Largest single bounded admission buffer.
    pub peak_buffer_bytes: u64,
    /// Serialized manifest bytes retained transiently during admission.
    pub manifest_bytes: u64,
    /// Conservative metadata retained by the admitted handle.
    pub retained_descriptor_bytes: u64,
    /// Ordinal ranges admitted.
    pub ranges: u64,
    /// Tombstone runs admitted.
    pub tombstone_runs: u64,
}

/// Aggregate-only bounded lookup work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct V4OrdinalLookupMetrics {
    /// Caller entries including duplicates.
    pub requested: u64,
    /// Unique requested node IDs.
    pub unique_requested: u64,
    /// Unique live identities found.
    pub found: u64,
    /// Range descriptors intersected.
    pub ranges_selected: u64,
    /// Coalesced ordinal payload calls.
    pub sequential_read_calls: u64,
    /// Ordinal and tombstone bytes read.
    pub bytes_read: u64,
    /// Requested identities hidden by newest tombstones.
    pub tombstoned: u64,
    /// Maximum anonymous request/index buffer charged by this lookup.
    pub peak_buffer_bytes: u64,
    /// Maximum retained tombstone-cache charge, including container metadata.
    pub retained_cache_bytes: u64,
    /// Maximum transient tombstone decode/clone charge.
    pub transient_buffer_bytes: u64,
    /// Per-identity seeks are forbidden by contract.
    pub per_record_seeks: u64,
}

/// One caller-ordered lookup result and its sanitized evidence.
#[derive(Debug, PartialEq, Eq)]
pub struct V4OrdinalLookup {
    /// UUID for each caller ID, or `None` when missing/deleted.
    pub values: Vec<Option<Uuid>>,
    /// Aggregate work evidence.
    pub metrics: V4OrdinalLookupMetrics,
}

#[derive(Debug)]
struct OpenArtifact {
    file: File,
    stamp: ArtifactStamp,
    descriptor: V4OrdinalArtifact,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ArtifactStamp {
    identity: FileIdentity,
    length: u64,
    modified: SystemTime,
}

#[derive(Debug)]
struct OpenRange {
    descriptor: V4OrdinalRange,
    artifact: OpenArtifact,
}

#[derive(Debug)]
struct OpenTombstones {
    artifact: OpenArtifact,
    blocks: Vec<TombstoneBlock>,
}

/// One immutable artifact retained by an authenticated v4 handle for a
/// generation-advancing writer. The cloned file pins the exact admitted inode;
/// callers must not reopen `descriptor.name` to obtain update input.
#[derive(Debug)]
pub(crate) struct PinnedV4OrdinalArtifact {
    pub(crate) descriptor: V4OrdinalArtifact,
    pub(crate) file: File,
}

/// Authenticated, inode-pinned inputs for planning the next v4 generation.
#[derive(Debug)]
pub(crate) struct V4OrdinalPinnedUpdateInputs {
    pub(crate) manifest: V4OrdinalIdentityManifest,
    pub(crate) artifacts: Vec<PinnedV4OrdinalArtifact>,
}

#[derive(Debug)]
struct TombstoneBlockCache {
    entries: BTreeMap<(usize, u64), Vec<u64>>,
    order: VecDeque<(usize, u64)>,
    charged_bytes: usize,
}

impl Default for TombstoneBlockCache {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            order: VecDeque::new(),
            charged_bytes: TOMBSTONE_CACHE_FIXED_CHARGE,
        }
    }
}

impl TombstoneBlockCache {
    fn entry_charge(ids: &Vec<u64>) -> usize {
        TOMBSTONE_CACHE_ENTRY_CHARGE
            .saturating_add(ids.capacity().saturating_mul(std::mem::size_of::<u64>()))
    }
}

type TombstoneBlock = V4OrdinalTombstoneBlock;

/// Authenticated generation-pinned read handle.
#[derive(Debug)]
pub struct V4OrdinalIdentityHandle {
    root: StableDirectory,
    coordination_file: File,
    coordination_identity: FileIdentity,
    manifest_file: File,
    manifest_stamp: ArtifactStamp,
    topology_generation: u64,
    forward: Vec<OpenArtifact>,
    ranges: Vec<OpenRange>,
    tombstones: Vec<OpenTombstones>,
    tombstone_cache: TombstoneBlockCache,
    limits: V4OrdinalIdentityLimits,
    admission: V4OrdinalAdmissionMetrics,
}

impl V4OrdinalIdentityHandle {
    /// Revalidate this retained generation and clone its already-authenticated
    /// artifact handles for a bounded append/compaction planner.
    pub(crate) fn pinned_update_inputs(
        &self,
    ) -> Result<V4OrdinalPinnedUpdateInputs, V4OrdinalIdentityError> {
        self.revalidate()?;
        let manifest = V4OrdinalIdentityManifest {
            format_version: ORDINAL_IDENTITY_V4,
            topology_generation: self.topology_generation,
            forward_identities: self
                .forward
                .iter()
                .map(|artifact| artifact.descriptor.clone())
                .collect(),
            ordinal_ranges: self
                .ranges
                .iter()
                .map(|range| range.descriptor.clone())
                .collect(),
            tombstones: self
                .tombstones
                .iter()
                .map(|run| V4OrdinalTombstones {
                    generation: run.artifact.descriptor.generation,
                    artifact: run.artifact.descriptor.clone(),
                    blocks: run.blocks.clone(),
                })
                .collect(),
        };
        let artifacts = self
            .forward
            .iter()
            .map(|artifact| artifact)
            .chain(self.ranges.iter().map(|range| &range.artifact))
            .chain(self.tombstones.iter().map(|run| &run.artifact))
            .map(|artifact| {
                Ok(PinnedV4OrdinalArtifact {
                    descriptor: artifact.descriptor.clone(),
                    file: artifact.file.try_clone().map_err(io_error)?,
                })
            })
            .collect::<Result<Vec<_>, V4OrdinalIdentityError>>()?;
        Ok(V4OrdinalPinnedUpdateInputs {
            manifest,
            artifacts,
        })
    }

    /// Return the exact v4 facet names admitted through this retained,
    /// generation-authenticated handle.
    pub(crate) fn referenced_file_names(&self) -> BTreeSet<String> {
        std::iter::once(MANIFEST_NAME.to_owned())
            .chain(std::iter::once(RECEIPT_FILE_NAME.to_owned()))
            .chain(std::iter::once(LOCK_NAME.to_owned()))
            .chain(
                self.forward
                    .iter()
                    .map(|artifact| artifact.descriptor.name.clone()),
            )
            .chain(
                self.ranges
                    .iter()
                    .map(|range| range.artifact.descriptor.name.clone()),
            )
            .chain(
                self.tombstones
                    .iter()
                    .map(|run| run.artifact.descriptor.name.clone()),
            )
            .collect()
    }

    /// Classify the additive ordinal facet without treating a present file as
    /// trusted. A present malformed v4 remains `Present` and subsequently
    /// fails authenticated open; discovery never falls back around it.
    pub fn discover(
        project_dir: &Path,
        topology_generation: u64,
    ) -> Result<V4OrdinalIdentityDiscovery, V4OrdinalIdentityError> {
        let root = StableDirectory::open(&project_dir.join(INDEX_DIR)).map_err(io_error)?;
        match root.open_child_file(MANIFEST_NAME.as_ref()) {
            Ok(_) => return Ok(V4OrdinalIdentityDiscovery::Present),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
        if crate::UuidMembershipIndex::open_at_generation(project_dir, topology_generation).is_err()
        {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "current v3 authority failed authentication",
            ));
        }
        Ok(V4OrdinalIdentityDiscovery::RebuildRequired { found_version: 3 })
    }

    /// Open and authenticate one immutable v4 generation.
    #[allow(clippy::too_many_lines)] // One admission lifecycle; ordering is the authority invariant.
    pub(crate) fn open(
        project_dir: &Path,
        authority: &V4OrdinalIdentityAuthority,
        limits: V4OrdinalIdentityLimits,
    ) -> Result<V4OrdinalIdentityOpen, V4OrdinalIdentityError> {
        validate_limits(limits)?;
        let root = StableDirectory::open(&project_dir.join(INDEX_DIR)).map_err(io_error)?;
        let coordination_file = root.open_child_file(LOCK_NAME.as_ref()).map_err(io_error)?;
        if file_link_count(&coordination_file).map_err(io_error)? != 1 {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        <File as fs4::FileExt>::lock_shared(&coordination_file).map_err(io_error)?;
        let coordination_identity = file_identity(&coordination_file).map_err(io_error)?;
        let mut manifest_file = root
            .open_child_file(MANIFEST_NAME.as_ref())
            .map_err(io_error)?;
        let manifest_stamp = artifact_stamp(&manifest_file)?;
        if file_link_count(&manifest_file).map_err(io_error)? != 1 {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        let body = read_bounded(&mut manifest_file, MAX_MANIFEST_BYTES)?;
        authenticate_manifest_authority(&body, authority)?;
        let manifest = parse_manifest(&body, authority.topology_generation)?.ok_or(
            V4OrdinalIdentityError::InvalidDescriptor("v3 occupies the v4 manifest path"),
        )?;
        validate_manifest(&manifest, authority.topology_generation)?;

        let mut admission =
            initial_admission_metrics(&manifest, body.len(), body.capacity(), limits)?;
        let mut names = BTreeSet::new();
        let mut forward_commitment = MappingCommitment::default();
        let mut ordinal_commitment = MappingCommitment::default();
        let mut forward = Vec::with_capacity(manifest.forward_identities.len());
        for artifact in &manifest.forward_identities {
            require_kind(
                artifact,
                V4OrdinalArtifactKind::ForwardIdentities,
                &manifest,
            )?;
            if !names.insert(artifact.name.clone()) {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "artifact filename is reused",
                ));
            }
            forward.push(admit_forward_artifact(
                &root,
                artifact,
                &manifest.ordinal_ranges,
                &mut forward_commitment,
                &mut admission,
            )?);
        }
        validate_unique_forward_runs(&mut forward, &mut admission)?;
        let mut ranges = Vec::with_capacity(manifest.ordinal_ranges.len());
        for range in &manifest.ordinal_ranges {
            if !names.insert(range.artifact.name.clone()) {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "artifact filename is reused",
                ));
            }
            ranges.push(OpenRange {
                descriptor: range.clone(),
                artifact: admit_ordinal_artifact(
                    &root,
                    range,
                    &mut ordinal_commitment,
                    &mut admission,
                )?,
            });
        }
        if forward_commitment != ordinal_commitment {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "forward and ordinal identity authorities disagree",
            ));
        }
        let mut tombstones = Vec::with_capacity(manifest.tombstones.len());
        for run in &manifest.tombstones {
            if !names.insert(run.artifact.name.clone()) {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "artifact filename is reused",
                ));
            }
            let artifact =
                admit_tombstone_artifact(&root, run, &manifest.ordinal_ranges, &mut admission)?;
            tombstones.push(OpenTombstones {
                artifact,
                blocks: run.blocks.clone(),
            });
        }
        // The coordination lock protects admission from a concurrent writer,
        // not the lifetime of an immutable snapshot. Releasing it here keeps
        // retained handles from starving publication. Revalidation below
        // still pins the manifest, lock inode, and every immutable artifact.
        <File as fs4::FileExt>::unlock(&coordination_file).map_err(io_error)?;
        admission.ranges = ranges.len() as u64;
        admission.tombstone_runs = tombstones.len() as u64;
        Ok(V4OrdinalIdentityOpen::Ready(Box::new(Self {
            root,
            coordination_file,
            coordination_identity,
            manifest_file,
            manifest_stamp,
            topology_generation: authority.topology_generation,
            forward,
            ranges,
            tombstones,
            tombstone_cache: TombstoneBlockCache::default(),
            limits,
            admission,
        })))
    }

    /// Admission evidence for this retained handle.
    #[must_use]
    pub const fn admission_metrics(&self) -> V4OrdinalAdmissionMetrics {
        self.admission
    }

    /// Authenticated topology generation.
    #[must_use]
    pub const fn topology_generation(&self) -> u64 {
        self.topology_generation
    }

    /// Resolve a bounded caller batch while preserving caller order.
    pub fn lookup_node_uuids(
        &mut self,
        requested: &[u64],
    ) -> Result<V4OrdinalLookup, V4OrdinalIdentityError> {
        if requested.len() > self.limits.max_requested {
            return Err(V4OrdinalIdentityError::RequestLimit {
                requested: requested.len(),
                maximum: self.limits.max_requested,
            });
        }
        self.revalidate()?;
        let mut metrics = V4OrdinalLookupMetrics {
            requested: requested.len() as u64,
            peak_buffer_bytes: (requested.len() as u64).saturating_mul(REQUEST_ENTRY_CHARGE),
            retained_cache_bytes: self.tombstone_cache.charged_bytes as u64,
            ..Default::default()
        };
        let request_buffer_bytes = metrics.peak_buffer_bytes;
        let unique = requested.iter().copied().collect::<BTreeSet<_>>();
        metrics.unique_requested = unique.len() as u64;
        let deleted = self.lookup_tombstones(&unique, request_buffer_bytes, &mut metrics)?;
        let live = unique
            .iter()
            .copied()
            .filter(|id| !deleted.contains(id))
            .collect::<Vec<_>>();
        let mut resolved = BTreeMap::new();
        for range in &mut self.ranges {
            let first = range.descriptor.first_node_id;
            let last = first + range.descriptor.count - 1;
            let start = live.partition_point(|id| *id < first);
            let end = live.partition_point(|id| *id <= last);
            if start == end {
                continue;
            }
            metrics.ranges_selected = metrics.ranges_selected.saturating_add(1);
            read_range_coalesced(
                range,
                &live[start..end],
                self.limits.coalesce_gap_bytes,
                self.limits.max_coalesced_read_bytes,
                request_buffer_bytes.saturating_add(self.tombstone_cache.charged_bytes as u64),
                &mut resolved,
                &mut metrics,
            )?;
        }
        metrics.found = resolved.len() as u64;
        metrics.tombstoned = deleted.len() as u64;
        Ok(V4OrdinalLookup {
            values: requested
                .iter()
                .map(|id| resolved.get(id).copied())
                .collect(),
            metrics,
        })
    }

    fn lookup_tombstones(
        &mut self,
        requested: &BTreeSet<u64>,
        request_buffer_bytes: u64,
        metrics: &mut V4OrdinalLookupMetrics,
    ) -> Result<BTreeSet<u64>, V4OrdinalIdentityError> {
        let mut deleted = BTreeSet::new();
        for (run_index, run) in self.tombstones.iter_mut().enumerate() {
            for block_index in 0..run.blocks.len() {
                let block = run.blocks[block_index].clone();
                if requested.range(block.first..=block.last).next().is_none() {
                    continue;
                }
                let ids = read_tombstone_block(
                    run,
                    run_index,
                    block_index,
                    self.limits.max_tombstone_cache_bytes,
                    request_buffer_bytes,
                    &mut self.tombstone_cache,
                    metrics,
                )?;
                for id in ids {
                    if requested.contains(&id) {
                        deleted.insert(id);
                    }
                }
            }
        }
        Ok(deleted)
    }

    fn revalidate(&self) -> Result<(), V4OrdinalIdentityError> {
        self.root.revalidate_named().map_err(io_error)?;
        let named_coordination = self
            .root
            .open_child_file(LOCK_NAME.as_ref())
            .map_err(io_error)?;
        if file_identity(&self.coordination_file).map_err(io_error)? != self.coordination_identity
            || file_identity(&named_coordination).map_err(io_error)? != self.coordination_identity
        {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        if artifact_stamp(&self.manifest_file)? != self.manifest_stamp {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        let named_manifest = self
            .root
            .open_child_file(MANIFEST_NAME.as_ref())
            .map_err(io_error)?;
        if artifact_stamp(&named_manifest)? != self.manifest_stamp {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        for artifact in self
            .forward
            .iter()
            .chain(self.ranges.iter().map(|range| &range.artifact))
            .chain(self.tombstones.iter().map(|run| &run.artifact))
        {
            if artifact_stamp(&artifact.file)? != artifact.stamp {
                return Err(V4OrdinalIdentityError::Authentication);
            }
            let named = self
                .root
                .open_child_file(artifact.descriptor.name.as_ref())
                .map_err(io_error)?;
            if artifact_stamp(&named)? != artifact.stamp {
                return Err(V4OrdinalIdentityError::Authentication);
            }
        }
        Ok(())
    }
}

fn authenticate_manifest_authority(
    body: &[u8],
    authority: &V4OrdinalIdentityAuthority,
) -> Result<(), V4OrdinalIdentityError> {
    if authority.manifest_sha256.len() != 64
        || !authority
            .manifest_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || hex(&Sha256::digest(body)) != authority.manifest_sha256
    {
        return Err(V4OrdinalIdentityError::Authentication);
    }
    Ok(())
}

fn initial_admission_metrics(
    manifest: &V4OrdinalIdentityManifest,
    manifest_bytes: usize,
    manifest_capacity: usize,
    limits: V4OrdinalIdentityLimits,
) -> Result<V4OrdinalAdmissionMetrics, V4OrdinalIdentityError> {
    let retained_descriptor_bytes = descriptor_metadata_charge(manifest);
    if retained_descriptor_bytes > limits.max_descriptor_metadata_bytes as u64 {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "descriptor metadata exceeds admission bound",
        ));
    }
    Ok(V4OrdinalAdmissionMetrics {
        manifest_bytes: manifest_bytes as u64,
        retained_descriptor_bytes,
        peak_buffer_bytes: retained_descriptor_bytes
            .saturating_add(manifest_capacity as u64)
            .saturating_add(ADMISSION_TRANSIENT_FIXED_CHARGE),
        ..Default::default()
    })
}

fn validate_limits(limits: V4OrdinalIdentityLimits) -> Result<(), V4OrdinalIdentityError> {
    if limits.max_requested == 0
        || limits.max_coalesced_read_bytes < ORDINAL_BLOCK_BYTES_USIZE
        || limits.max_tombstone_cache_bytes < TOMBSTONE_CACHE_FIXED_CHARGE
        || limits.max_descriptor_metadata_bytes < DESCRIPTOR_FIXED_CHARGE
    {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "lookup bounds are invalid",
        ));
    }
    Ok(())
}

fn descriptor_metadata_charge(manifest: &V4OrdinalIdentityManifest) -> u64 {
    let artifact = |descriptor: &V4OrdinalArtifact| {
        ARTIFACT_DESCRIPTOR_CHARGE
            .saturating_add(descriptor.name.len() as u64)
            .saturating_add(descriptor.sha256.len() as u64)
    };
    let forward = manifest
        .forward_identities
        .iter()
        .fold(0_u64, |sum, item| sum.saturating_add(artifact(item)));
    let ordinal = manifest.ordinal_ranges.iter().fold(0_u64, |sum, range| {
        sum.saturating_add(artifact(&range.artifact))
            .saturating_add(
                (range.blocks.len() as u64).saturating_mul(ORDINAL_BLOCK_DESCRIPTOR_CHARGE),
            )
    });
    let tombstones = manifest.tombstones.iter().fold(0_u64, |sum, run| {
        sum.saturating_add(artifact(&run.artifact)).saturating_add(
            (run.blocks.len() as u64).saturating_mul(TOMBSTONE_BLOCK_DESCRIPTOR_CHARGE),
        )
    });
    (DESCRIPTOR_FIXED_CHARGE as u64)
        .saturating_add(forward)
        .saturating_add(ordinal)
        .saturating_add(tombstones)
}

fn parse_manifest(
    body: &[u8],
    expected_generation: u64,
) -> Result<Option<V4OrdinalIdentityManifest>, V4OrdinalIdentityError> {
    let value: serde_json::Value = serde_json::from_slice(body).map_err(io_error)?;
    let version = value
        .get("format_version")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or(V4OrdinalIdentityError::InvalidDescriptor(
            "format version is absent",
        ))?;
    if version == 3 {
        if crate::uuid_membership::canonical_v3_manifest_marker(body, expected_generation) {
            return Ok(None);
        }
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "v3 rebuild marker is noncanonical",
        ));
    }
    if version != ORDINAL_IDENTITY_V4 {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "format version is unsupported",
        ));
    }
    serde_json::from_value(value).map(Some).map_err(io_error)
}

fn validate_manifest(
    manifest: &V4OrdinalIdentityManifest,
    expected_generation: u64,
) -> Result<(), V4OrdinalIdentityError> {
    if manifest.topology_generation != expected_generation {
        return Err(V4OrdinalIdentityError::GenerationMismatch {
            expected: expected_generation,
            found: manifest.topology_generation,
        });
    }
    if manifest.forward_identities.is_empty() {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "forward identity authority is absent",
        ));
    }
    let mut prior_forward_generation = 0;
    for run in &manifest.forward_identities {
        require_kind(run, V4OrdinalArtifactKind::ForwardIdentities, manifest)?;
        if run.generation <= prior_forward_generation {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "forward runs are not in canonical generation order",
            ));
        }
        prior_forward_generation = run.generation;
    }
    let mut prior_end = 0_u64;
    for range in &manifest.ordinal_ranges {
        require_kind(
            &range.artifact,
            V4OrdinalArtifactKind::OrdinalUuids,
            manifest,
        )?;
        let end = range
            .first_node_id
            .checked_add(range.count.checked_sub(1).ok_or(
                V4OrdinalIdentityError::InvalidDescriptor("ordinal range is empty"),
            )?)
            .ok_or(V4OrdinalIdentityError::InvalidDescriptor(
                "ordinal range overflows",
            ))?;
        if range.first_node_id == 0 || range.first_node_id <= prior_end {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "ordinal ranges overlap or descend",
            ));
        }
        let bytes = range.count.checked_mul(UUID_WIDTH).ok_or(
            V4OrdinalIdentityError::InvalidDescriptor("ordinal byte length overflows"),
        )?;
        if range.artifact.bytes != bytes {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "ordinal artifact length is not packed",
            ));
        }
        prior_end = end;
    }
    let mut prior_generation = 0;
    for run in &manifest.tombstones {
        require_kind(
            &run.artifact,
            V4OrdinalArtifactKind::NodeTombstones,
            manifest,
        )?;
        if run.generation == 0
            || run.generation <= prior_generation
            || run.generation != run.artifact.generation
            || run.artifact.bytes % TOMBSTONE_WIDTH != 0
        {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "tombstone runs are noncanonical",
            ));
        }
        prior_generation = run.generation;
    }
    Ok(())
}

fn require_kind(
    artifact: &V4OrdinalArtifact,
    kind: V4OrdinalArtifactKind,
    manifest: &V4OrdinalIdentityManifest,
) -> Result<(), V4OrdinalIdentityError> {
    if artifact.kind != kind
        || artifact.generation == 0
        || artifact.generation > manifest.topology_generation
        || artifact.sha256.len() != 64
        || !artifact
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || !plain_name(&artifact.name)
    {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "artifact binding is noncanonical",
        ));
    }
    if kind == V4OrdinalArtifactKind::ForwardIdentities
        && !artifact.bytes.is_multiple_of(FORWARD_RECORD_WIDTH)
    {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "forward identity artifact is truncated",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MappingCommitment {
    count: u64,
    limbs: [u64; 4],
}

impl MappingCommitment {
    fn add(&mut self, uuid: &[u8; UUID_WIDTH_USIZE], node_id: u64) {
        let mut digest = Sha256::new();
        digest.update(b"graphforge-v4-ordinal-mapping\0");
        digest.update(uuid);
        digest.update(node_id.to_be_bytes());
        let digest = digest.finalize();
        for (limb, bytes) in self.limbs.iter_mut().zip(digest.chunks_exact(8)) {
            *limb = limb.wrapping_add(u64::from_be_bytes(bytes.try_into().expect("SHA limb")));
        }
        self.count = self.count.saturating_add(1);
    }
}

fn admit_ordinal_artifact(
    root: &StableDirectory,
    range: &V4OrdinalRange,
    commitment: &mut MappingCommitment,
    metrics: &mut V4OrdinalAdmissionMetrics,
) -> Result<OpenArtifact, V4OrdinalIdentityError> {
    let mut artifact = open_admission_file(root, &range.artifact)?;
    let block_bytes =
        usize::try_from(ORDINAL_BLOCK_BYTES.min(artifact.descriptor.bytes.max(UUID_WIDTH)))
            .map_err(|_| V4OrdinalIdentityError::Authentication)?;
    let mut buffer = vec![0_u8; block_bytes];
    let mut whole = Sha256::new();
    let mut declared = range.blocks.iter();
    let mut offset = 0_u64;
    let mut ordinal = 0_u64;
    loop {
        let read = read_fill_or_eof(&mut artifact.file, &mut buffer, metrics)?;
        if read == 0 {
            break;
        }
        whole.update(&buffer[..read]);
        if !read.is_multiple_of(UUID_WIDTH_USIZE) {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        let actual = V4OrdinalBlock {
            offset,
            count: read as u64 / UUID_WIDTH,
            sha256: hex(&Sha256::digest(&buffer[..read])),
        };
        if declared.next() != Some(&actual) {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "ordinal block fences are noncanonical",
            ));
        }
        for record in buffer[..read].chunks_exact(UUID_WIDTH_USIZE) {
            let uuid: [u8; UUID_WIDTH_USIZE] = record.try_into().expect("fixed UUID");
            if uuid == [0; UUID_WIDTH_USIZE] {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "ordinal UUID is zero",
                ));
            }
            let node_id = range
                .first_node_id
                .checked_add(ordinal)
                .ok_or(V4OrdinalIdentityError::Authentication)?;
            commitment.add(&uuid, node_id);
            ordinal = ordinal.saturating_add(1);
        }
        offset = offset.saturating_add(read as u64);
    }
    if declared.next().is_some() {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "ordinal block fences are noncanonical",
        ));
    }
    finish_admission(&mut artifact, whole, metrics)?;
    Ok(artifact)
}

fn admit_forward_artifact(
    root: &StableDirectory,
    descriptor: &V4OrdinalArtifact,
    ranges: &[V4OrdinalRange],
    commitment: &mut MappingCommitment,
    metrics: &mut V4OrdinalAdmissionMetrics,
) -> Result<OpenArtifact, V4OrdinalIdentityError> {
    let mut artifact = open_admission_file(root, descriptor)?;
    let maximum_stream = (STREAM_BYTES / FORWARD_RECORD_WIDTH_USIZE) * FORWARD_RECORD_WIDTH_USIZE;
    let artifact_bytes = usize::try_from(descriptor.bytes).unwrap_or(usize::MAX);
    let stream_bytes = maximum_stream
        .min(artifact_bytes)
        .max(FORWARD_RECORD_WIDTH_USIZE);
    let mut buffer = vec![0_u8; stream_bytes];
    let mut whole = Sha256::new();
    let mut remaining = descriptor.bytes;
    let mut prior_uuid = None;
    while remaining != 0 {
        let read = usize::try_from(remaining.min(stream_bytes as u64))
            .map_err(|_| V4OrdinalIdentityError::Authentication)?;
        artifact
            .file
            .read_exact(&mut buffer[..read])
            .map_err(io_error)?;
        record_admission_read(metrics, read, buffer.len());
        whole.update(&buffer[..read]);
        for record in buffer[..read].chunks_exact(FORWARD_RECORD_WIDTH_USIZE) {
            let uuid: [u8; UUID_WIDTH_USIZE] =
                record[..UUID_WIDTH_USIZE].try_into().expect("fixed UUID");
            let node_id = u64::from_be_bytes(
                record[UUID_WIDTH_USIZE..]
                    .try_into()
                    .expect("fixed surrogate"),
            );
            if uuid == [0; UUID_WIDTH_USIZE]
                || prior_uuid.is_some_and(|prior| prior >= uuid)
                || !range_contains(ranges, node_id)
            {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "forward identity records are noncanonical",
                ));
            }
            prior_uuid = Some(uuid);
            commitment.add(&uuid, node_id);
        }
        remaining -= read as u64;
    }
    finish_admission(&mut artifact, whole, metrics)?;
    Ok(artifact)
}

/// Prove UUID uniqueness across independently sorted forward generations with
/// one cursor per retained run. This is bounded by descriptor/run count rather
/// than graph cardinality and performs only sequential reads.
fn validate_unique_forward_runs(
    runs: &mut [OpenArtifact],
    metrics: &mut V4OrdinalAdmissionMetrics,
) -> Result<(), V4OrdinalIdentityError> {
    let per_run_buffer = (STREAM_BYTES / runs.len().max(1) / FORWARD_RECORD_WIDTH_USIZE).max(1)
        * FORWARD_RECORD_WIDTH_USIZE;
    let buffer_lengths = runs
        .iter()
        .map(|run| {
            usize::try_from(run.descriptor.bytes)
                .unwrap_or(usize::MAX)
                .min(per_run_buffer)
                .max(FORWARD_RECORD_WIDTH_USIZE)
        })
        .collect::<Vec<_>>();
    let cursor_bytes = buffer_lengths.iter().sum::<usize>();
    metrics.peak_buffer_bytes = metrics.peak_buffer_bytes.max(
        metrics
            .retained_descriptor_bytes
            .saturating_add(metrics.manifest_bytes)
            .saturating_add(cursor_bytes as u64),
    );
    let mut cursors = runs
        .iter()
        .zip(buffer_lengths)
        .map(|(run, buffer_len)| ForwardRunCursor {
            buffer: vec![0; buffer_len],
            cursor: 0,
            valid: 0,
            remaining: run.descriptor.bytes,
        })
        .collect::<Vec<_>>();
    let mut heap = BinaryHeap::with_capacity(runs.len());
    for (index, run) in runs.iter_mut().enumerate() {
        run.file.rewind().map_err(io_error)?;
        if let Some(uuid) = cursors[index].next_uuid(run, metrics)? {
            heap.push(Reverse((uuid, index)));
        }
    }
    let mut prior = None;
    while let Some(Reverse((uuid, index))) = heap.pop() {
        if prior == Some(uuid) {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "forward identity UUID is repeated across generations",
            ));
        }
        prior = Some(uuid);
        if let Some(next) = cursors[index].next_uuid(&mut runs[index], metrics)? {
            heap.push(Reverse((next, index)));
        }
    }
    Ok(())
}

struct ForwardRunCursor {
    buffer: Vec<u8>,
    cursor: usize,
    valid: usize,
    remaining: u64,
}

impl ForwardRunCursor {
    fn next_uuid(
        &mut self,
        run: &mut OpenArtifact,
        metrics: &mut V4OrdinalAdmissionMetrics,
    ) -> Result<Option<[u8; UUID_WIDTH_USIZE]>, V4OrdinalIdentityError> {
        if self.cursor == self.valid {
            if self.remaining == 0 {
                return Ok(None);
            }
            self.valid = usize::try_from(self.remaining.min(self.buffer.len() as u64))
                .map_err(|_| V4OrdinalIdentityError::Authentication)?;
            run.file
                .read_exact(&mut self.buffer[..self.valid])
                .map_err(io_error)?;
            record_admission_read(metrics, self.valid, self.buffer.len());
            self.remaining -= self.valid as u64;
            self.cursor = 0;
        }
        let record = &self.buffer[self.cursor..self.cursor + FORWARD_RECORD_WIDTH_USIZE];
        self.cursor += FORWARD_RECORD_WIDTH_USIZE;
        Ok(Some(
            record[..UUID_WIDTH_USIZE].try_into().expect("fixed UUID"),
        ))
    }
}
fn admit_tombstone_artifact(
    root: &StableDirectory,
    run: &V4OrdinalTombstones,
    ranges: &[V4OrdinalRange],
    metrics: &mut V4OrdinalAdmissionMetrics,
) -> Result<OpenArtifact, V4OrdinalIdentityError> {
    let mut artifact = open_admission_file(root, &run.artifact)?;
    let block_bytes = TOMBSTONE_BLOCK_BYTES.min(run.artifact.bytes.max(TOMBSTONE_WIDTH));
    let block_bytes =
        usize::try_from(block_bytes).map_err(|_| V4OrdinalIdentityError::Authentication)?;
    let mut buffer = vec![0_u8; block_bytes];
    let mut whole = Sha256::new();
    let mut prior = None;
    let mut offset = 0_u64;
    let mut declared = run.blocks.iter();
    loop {
        let read = read_fill_or_eof(&mut artifact.file, &mut buffer, metrics)?;
        if read == 0 {
            break;
        }
        whole.update(&buffer[..read]);
        if !read.is_multiple_of(TOMBSTONE_WIDTH_USIZE) {
            return Err(V4OrdinalIdentityError::Authentication);
        }
        let mut first = None;
        let mut last = 0;
        for record in buffer[..read].chunks_exact(TOMBSTONE_WIDTH_USIZE) {
            let id = u64::from_be_bytes(record.try_into().expect("fixed tombstone"));
            if id == 0 || prior.is_some_and(|value| value >= id) || !range_contains(ranges, id) {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "tombstone IDs are noncanonical",
                ));
            }
            first.get_or_insert(id);
            last = id;
            prior = Some(id);
        }
        if let Some(first) = first {
            let actual = V4OrdinalTombstoneBlock {
                offset,
                count: read as u64 / TOMBSTONE_WIDTH,
                first,
                last,
                sha256: hex(&Sha256::digest(&buffer[..read])),
            };
            if declared.next() != Some(&actual) {
                return Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "tombstone block fences are noncanonical",
                ));
            }
        }
        offset = offset.saturating_add(read as u64);
    }
    if declared.next().is_some() {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "tombstone block fences are noncanonical",
        ));
    }
    finish_admission(&mut artifact, whole, metrics)?;
    Ok(artifact)
}
fn read_tombstone_block(
    run: &mut OpenTombstones,
    run_index: usize,
    block_index: usize,
    maximum_cache_bytes: usize,
    request_buffer_bytes: u64,
    cache: &mut TombstoneBlockCache,
    metrics: &mut V4OrdinalLookupMetrics,
) -> Result<Vec<u64>, V4OrdinalIdentityError> {
    let block = run.blocks[block_index].clone();
    let cache_key = (run_index, block.offset);
    if let Some(ids) = cache.entries.get(&cache_key) {
        let clone_charge = ids.capacity().saturating_mul(std::mem::size_of::<u64>());
        metrics.retained_cache_bytes = metrics.retained_cache_bytes.max(cache.charged_bytes as u64);
        metrics.transient_buffer_bytes = metrics.transient_buffer_bytes.max(clone_charge as u64);
        metrics.peak_buffer_bytes = metrics.peak_buffer_bytes.max(
            request_buffer_bytes
                .saturating_add(cache.charged_bytes as u64)
                .saturating_add(clone_charge as u64),
        );
        return Ok(ids.clone());
    }
    let bytes_len = block
        .count
        .checked_mul(TOMBSTONE_WIDTH)
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or(V4OrdinalIdentityError::Authentication)?;
    let mut bytes = vec![0_u8; bytes_len];
    run.artifact
        .file
        .seek(SeekFrom::Start(block.offset))
        .map_err(io_error)?;
    run.artifact.file.read_exact(&mut bytes).map_err(io_error)?;
    if hex(&Sha256::digest(&bytes)) != block.sha256 {
        return Err(V4OrdinalIdentityError::Authentication);
    }
    metrics.sequential_read_calls = metrics.sequential_read_calls.saturating_add(1);
    metrics.bytes_read = metrics.bytes_read.saturating_add(bytes_len as u64);
    let ids = bytes
        .chunks_exact(TOMBSTONE_WIDTH_USIZE)
        .map(|record| u64::from_be_bytes(record.try_into().expect("fixed tombstone")))
        .collect::<Vec<_>>();
    let decoded_charge = ids.capacity().saturating_mul(std::mem::size_of::<u64>());
    let retained_entry_charge = TombstoneBlockCache::entry_charge(&ids);
    let transient_charge = bytes
        .capacity()
        .saturating_add(decoded_charge.saturating_mul(2))
        .saturating_add(TOMBSTONE_CACHE_ENTRY_CHARGE);
    metrics.transient_buffer_bytes = metrics.transient_buffer_bytes.max(transient_charge as u64);
    // The input byte buffer, decoded result, insertion clone, retained cache,
    // and prospective map/queue allocator metadata can coexist.
    metrics.peak_buffer_bytes = metrics.peak_buffer_bytes.max(
        request_buffer_bytes
            .saturating_add(cache.charged_bytes as u64)
            .saturating_add(bytes.capacity() as u64)
            .saturating_add((decoded_charge as u64).saturating_mul(2))
            .saturating_add(TOMBSTONE_CACHE_ENTRY_CHARGE as u64),
    );
    if TOMBSTONE_CACHE_FIXED_CHARGE.saturating_add(retained_entry_charge) <= maximum_cache_bytes {
        while cache.charged_bytes.saturating_add(retained_entry_charge) > maximum_cache_bytes {
            let Some(oldest) = cache.order.pop_front() else {
                break;
            };
            if let Some(evicted) = cache.entries.remove(&oldest) {
                cache.charged_bytes = cache
                    .charged_bytes
                    .saturating_sub(TombstoneBlockCache::entry_charge(&evicted));
            }
        }
        cache.entries.insert(cache_key, ids.clone());
        cache.order.push_back(cache_key);
        cache.charged_bytes = cache.charged_bytes.saturating_add(retained_entry_charge);
        metrics.retained_cache_bytes = metrics.retained_cache_bytes.max(cache.charged_bytes as u64);
    }
    Ok(ids)
}

fn range_contains(ranges: &[V4OrdinalRange], id: u64) -> bool {
    let index = ranges.partition_point(|range| range.first_node_id <= id);
    index > 0
        && ranges[index - 1]
            .first_node_id
            .checked_add(ranges[index - 1].count)
            .is_some_and(|end| id < end)
}

fn plain_name(name: &str) -> bool {
    let mut components = Path::new(name).components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

fn open_admission_file(
    root: &StableDirectory,
    descriptor: &V4OrdinalArtifact,
) -> Result<OpenArtifact, V4OrdinalIdentityError> {
    let file = root
        .open_child_file(descriptor.name.as_ref())
        .map_err(io_error)?;
    let stamp = artifact_stamp(&file)?;
    if file_link_count(&file).map_err(io_error)? != 1 || stamp.length != descriptor.bytes {
        return Err(V4OrdinalIdentityError::Authentication);
    }
    Ok(OpenArtifact {
        file,
        stamp,
        descriptor: descriptor.clone(),
    })
}

fn record_admission_read(metrics: &mut V4OrdinalAdmissionMetrics, read: usize, capacity: usize) {
    metrics.sequential_read_calls = metrics.sequential_read_calls.saturating_add(1);
    metrics.authenticated_bytes = metrics.authenticated_bytes.saturating_add(read as u64);
    metrics.peak_buffer_bytes = metrics.peak_buffer_bytes.max(
        metrics
            .retained_descriptor_bytes
            .saturating_add(metrics.manifest_bytes)
            .saturating_add(capacity as u64)
            .saturating_add(ADMISSION_TRANSIENT_FIXED_CHARGE),
    );
}

fn read_fill_or_eof<R: Read>(
    reader: &mut R,
    buffer: &mut [u8],
    metrics: &mut V4OrdinalAdmissionMetrics,
) -> Result<usize, V4OrdinalIdentityError> {
    let mut filled = 0;
    while filled < buffer.len() {
        let read = reader.read(&mut buffer[filled..]).map_err(io_error)?;
        if read == 0 {
            break;
        }
        filled += read;
        record_admission_read(metrics, read, buffer.len());
    }
    Ok(filled)
}

fn finish_admission(
    artifact: &mut OpenArtifact,
    digest: Sha256,
    metrics: &mut V4OrdinalAdmissionMetrics,
) -> Result<(), V4OrdinalIdentityError> {
    if hex(&digest.finalize()) != artifact.descriptor.sha256 {
        return Err(V4OrdinalIdentityError::Authentication);
    }
    artifact.file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    metrics.artifacts = metrics.artifacts.saturating_add(1);
    Ok(())
}
fn artifact_stamp(file: &File) -> Result<ArtifactStamp, V4OrdinalIdentityError> {
    let metadata = file.metadata().map_err(io_error)?;
    Ok(ArtifactStamp {
        identity: file_identity(file).map_err(io_error)?,
        length: metadata.len(),
        modified: metadata.modified().map_err(io_error)?,
    })
}

fn read_range_coalesced(
    range: &mut OpenRange,
    ids: &[u64],
    gap_bytes: u64,
    maximum_read_bytes: usize,
    retained_buffer_bytes: u64,
    resolved: &mut BTreeMap<u64, Uuid>,
    metrics: &mut V4OrdinalLookupMetrics,
) -> Result<(), V4OrdinalIdentityError> {
    let first = range.descriptor.first_node_id;
    let selected = range
        .descriptor
        .blocks
        .iter()
        .enumerate()
        .filter_map(|(index, block)| {
            let block_first = first + block.offset / UUID_WIDTH;
            let block_last = block_first + block.count - 1;
            (ids.partition_point(|id| *id < block_first)
                != ids.partition_point(|id| *id <= block_last))
            .then_some(index)
        })
        .collect::<Vec<_>>();
    let mut selected_at = 0;
    while selected_at < selected.len() {
        let first_index = selected[selected_at];
        let mut last_index = first_index;
        let mut next = selected_at + 1;
        while next < selected.len() {
            let candidate = selected[next];
            let prior = &range.descriptor.blocks[last_index];
            let block = &range.descriptor.blocks[candidate];
            let prior_end = prior.offset + prior.count * UUID_WIDTH;
            let combined = block.offset + block.count * UUID_WIDTH
                - range.descriptor.blocks[first_index].offset;
            if block.offset.saturating_sub(prior_end) > gap_bytes
                || combined > maximum_read_bytes as u64
            {
                break;
            }
            last_index = candidate;
            next += 1;
        }
        let first_block = &range.descriptor.blocks[first_index];
        let last_block = &range.descriptor.blocks[last_index];
        let bytes = last_block.offset + last_block.count * UUID_WIDTH - first_block.offset;
        let length = usize::try_from(bytes).map_err(|_| V4OrdinalIdentityError::Authentication)?;
        if length > maximum_read_bytes {
            return Err(V4OrdinalIdentityError::InvalidDescriptor(
                "ordinal block exceeds read bound",
            ));
        }
        let mut buffer = vec![0_u8; length];
        range
            .artifact
            .file
            .seek(SeekFrom::Start(first_block.offset))
            .map_err(io_error)?;
        range
            .artifact
            .file
            .read_exact(&mut buffer)
            .map_err(io_error)?;
        metrics.sequential_read_calls = metrics.sequential_read_calls.saturating_add(1);
        metrics.bytes_read = metrics.bytes_read.saturating_add(bytes);
        metrics.peak_buffer_bytes = metrics
            .peak_buffer_bytes
            .max(retained_buffer_bytes.saturating_add(bytes));
        for block in &range.descriptor.blocks[first_index..=last_index] {
            let slice_start = usize::try_from(block.offset - first_block.offset)
                .map_err(|_| V4OrdinalIdentityError::Authentication)?;
            let slice_len = usize::try_from(block.count * UUID_WIDTH)
                .map_err(|_| V4OrdinalIdentityError::Authentication)?;
            if hex(&Sha256::digest(
                &buffer[slice_start..slice_start + slice_len],
            )) != block.sha256
            {
                return Err(V4OrdinalIdentityError::Authentication);
            }
        }
        for block in &range.descriptor.blocks[first_index..=last_index] {
            let block_first = first + block.offset / UUID_WIDTH;
            let block_last = block_first + block.count - 1;
            let start = ids.partition_point(|id| *id < block_first);
            let end = ids.partition_point(|id| *id <= block_last);
            if start == end {
                continue;
            }
            for id in &ids[start..end] {
                let at = usize::try_from(
                    block.offset - first_block.offset + (id - block_first) * UUID_WIDTH,
                )
                .map_err(|_| V4OrdinalIdentityError::Authentication)?;
                let uuid = Uuid::from_bytes(
                    buffer[at..at + UUID_WIDTH_USIZE]
                        .try_into()
                        .expect("fixed UUID"),
                );
                resolved.insert(*id, uuid);
            }
        }
        selected_at = next;
    }
    Ok(())
}

fn read_bounded(file: &mut File, maximum: u64) -> Result<Vec<u8>, V4OrdinalIdentityError> {
    let length = file.seek(SeekFrom::End(0)).map_err(io_error)?;
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    if length > maximum {
        return Err(V4OrdinalIdentityError::InvalidDescriptor(
            "manifest exceeds size bound",
        ));
    }
    let capacity = usize::try_from(length).map_err(|_| V4OrdinalIdentityError::Authentication)?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes).map_err(io_error)?;
    Ok(bytes)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut encoded, byte| {
            write!(encoded, "{byte:02x}").expect("writing to a string cannot fail");
            encoded
        },
    )
}

fn io_error(error: impl std::fmt::Display) -> V4OrdinalIdentityError {
    let _ = error;
    V4OrdinalIdentityError::Io
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Cursor;
    use std::process::Command;

    use super::*;

    struct ShortReader {
        inner: Cursor<Vec<u8>>,
        maximum: usize,
    }

    impl Read for ShortReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let available = buffer.len().min(self.maximum);
            self.inner.read(&mut buffer[..available])
        }
    }

    #[test]
    fn block_fill_preserves_ordinal_and_tombstone_bytes_across_short_reads() {
        for (width, records) in [(UUID_WIDTH_USIZE, 7_usize), (TOMBSTONE_WIDTH_USIZE, 11)] {
            let expected = (0..width * records)
                .map(|value| u8::try_from(value % 251).unwrap())
                .collect::<Vec<_>>();
            let mut reader = ShortReader {
                inner: Cursor::new(expected.clone()),
                maximum: 3,
            };
            let mut actual = vec![0_u8; expected.len()];
            let mut metrics = V4OrdinalAdmissionMetrics::default();
            assert_eq!(
                read_fill_or_eof(&mut reader, &mut actual, &mut metrics).unwrap(),
                expected.len()
            );
            assert_eq!(actual, expected);
            assert_eq!(Sha256::digest(&actual), Sha256::digest(&expected));
            assert!(metrics.sequential_read_calls > 1);
            assert_eq!(metrics.authenticated_bytes, expected.len() as u64);
        }
    }

    struct Fixture {
        root: tempfile::TempDir,
        manifest: V4OrdinalIdentityManifest,
    }

    impl Fixture {
        fn new(range_sizes: &[u64], tombstones: &[u64]) -> Self {
            let root = tempfile::TempDir::new().unwrap();
            let index = root.path().join(INDEX_DIR);
            fs::create_dir_all(&index).unwrap();
            fs::write(index.join(LOCK_NAME), []).unwrap();
            let mut next = 1_u64;
            let mut ordinal_ranges = Vec::new();
            let mut mappings = Vec::new();
            for (ordinal, count) in range_sizes.iter().copied().enumerate() {
                let mut bytes = Vec::new();
                for id in next..next + count {
                    let uuid = *Uuid::from_u128(id as u128).as_bytes();
                    bytes.extend_from_slice(&uuid);
                    mappings.push((uuid, id));
                }
                let name = format!("ordinal-{ordinal}.uuidx");
                fs::write(index.join(&name), &bytes).unwrap();
                ordinal_ranges.push(V4OrdinalRange {
                    first_node_id: next,
                    count,
                    artifact: artifact(name, V4OrdinalArtifactKind::OrdinalUuids, 7, &bytes),
                    blocks: ordinal_blocks(&bytes),
                });
                next += count + 3; // prove sparse ranges are packed, never max-id padded
            }
            let tombstone_bytes = tombstones
                .iter()
                .flat_map(|id| id.to_be_bytes())
                .collect::<Vec<_>>();
            fs::write(index.join("tombstones.uuidx"), &tombstone_bytes).unwrap();
            mappings.sort_unstable_by_key(|(uuid, _)| *uuid);
            let forward_bytes = mappings
                .iter()
                .flat_map(|(uuid, id)| uuid.iter().copied().chain(id.to_be_bytes()))
                .collect::<Vec<_>>();
            fs::write(index.join("forward.uuidx"), &forward_bytes).unwrap();
            let manifest = V4OrdinalIdentityManifest {
                format_version: ORDINAL_IDENTITY_V4,
                topology_generation: 7,
                forward_identities: vec![artifact(
                    "forward.uuidx".into(),
                    V4OrdinalArtifactKind::ForwardIdentities,
                    7,
                    &forward_bytes,
                )],
                ordinal_ranges,
                tombstones: vec![V4OrdinalTombstones {
                    generation: 7,
                    artifact: artifact(
                        "tombstones.uuidx".into(),
                        V4OrdinalArtifactKind::NodeTombstones,
                        7,
                        &tombstone_bytes,
                    ),
                    blocks: tombstone_blocks(tombstones),
                }],
            };
            fs::write(
                index.join(MANIFEST_NAME),
                serde_json::to_vec(&manifest).unwrap(),
            )
            .unwrap();
            Self { root, manifest }
        }

        fn publish(&self) {
            fs::write(
                self.root.path().join(INDEX_DIR).join(MANIFEST_NAME),
                serde_json::to_vec(&self.manifest).unwrap(),
            )
            .unwrap();
        }

        fn open(&self, limits: V4OrdinalIdentityLimits) -> V4OrdinalIdentityHandle {
            match V4OrdinalIdentityHandle::open(self.root.path(), &self.authority(7), limits)
                .unwrap()
            {
                V4OrdinalIdentityOpen::Ready(handle) => *handle,
                V4OrdinalIdentityOpen::RebuildRequired { .. } => panic!("fixture is v4"),
            }
        }

        fn authority(&self, topology_generation: u64) -> V4OrdinalIdentityAuthority {
            let body = fs::read(self.root.path().join(INDEX_DIR).join(MANIFEST_NAME)).unwrap();
            V4OrdinalIdentityAuthority {
                topology_generation,
                manifest_sha256: hex(&Sha256::digest(body)),
            }
        }
    }

    #[test]
    fn retained_authenticated_handle_never_reenumerates_replaced_manifest_names() {
        let fixture = Fixture::new(&[2], &[1]);
        let handle = fixture.open(V4OrdinalIdentityLimits::default());
        let mut replacement = fixture.manifest.clone();
        replacement.forward_identities[0].name = "planted-forward.uuidx".into();
        fs::write(
            fixture.root.path().join(INDEX_DIR).join(MANIFEST_NAME),
            serde_json::to_vec(&replacement).unwrap(),
        )
        .unwrap();

        let referenced = handle.referenced_file_names();
        assert!(referenced.contains("forward.uuidx"));
        assert!(!referenced.contains("planted-forward.uuidx"));
    }

    fn artifact(
        name: String,
        kind: V4OrdinalArtifactKind,
        generation: u64,
        bytes: &[u8],
    ) -> V4OrdinalArtifact {
        V4OrdinalArtifact {
            name,
            kind,
            generation,
            bytes: bytes.len() as u64,
            sha256: hex(&Sha256::digest(bytes)),
        }
    }

    fn tombstone_blocks(ids: &[u64]) -> Vec<V4OrdinalTombstoneBlock> {
        ids.chunks((TOMBSTONE_BLOCK_BYTES / TOMBSTONE_WIDTH) as usize)
            .enumerate()
            .map(|(index, chunk)| V4OrdinalTombstoneBlock {
                offset: index as u64 * TOMBSTONE_BLOCK_BYTES,
                count: chunk.len() as u64,
                first: chunk[0],
                last: chunk[chunk.len() - 1],
                sha256: hex(&Sha256::digest(
                    &chunk
                        .iter()
                        .flat_map(|id| id.to_be_bytes())
                        .collect::<Vec<_>>(),
                )),
            })
            .collect()
    }

    fn ordinal_blocks(bytes: &[u8]) -> Vec<V4OrdinalBlock> {
        bytes
            .chunks(ORDINAL_BLOCK_BYTES_USIZE)
            .enumerate()
            .map(|(index, block)| V4OrdinalBlock {
                offset: index as u64 * ORDINAL_BLOCK_BYTES,
                count: block.len() as u64 / UUID_WIDTH,
                sha256: hex(&Sha256::digest(block)),
            })
            .collect()
    }

    #[test]
    fn shuffled_repeated_missing_and_tombstoned_lookup_is_exact() {
        let fixture = Fixture::new(&[6, 3], &[3]);
        let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
        let result = handle.lookup_node_uuids(&[6, 3, 1, 6, 7, 10]).unwrap();
        assert_eq!(
            result.values,
            vec![
                Some(Uuid::from_u128(6)),
                None,
                Some(Uuid::from_u128(1)),
                Some(Uuid::from_u128(6)),
                None,
                Some(Uuid::from_u128(10)),
            ]
        );
        assert_eq!(result.metrics.requested, 6);
        assert_eq!(result.metrics.unique_requested, 5);
        assert_eq!(result.metrics.found, 3);
        assert_eq!(result.metrics.tombstoned, 1);
        assert_eq!(result.metrics.per_record_seeks, 0);
    }

    #[test]
    fn generated_lookup_orders_preserve_identity_and_linear_bounds() {
        let fixture = Fixture::new(&[96], &[7, 31, 63]);
        for rotation in 0..32 {
            let mut requested = (1..=96).step_by(3).collect::<Vec<_>>();
            requested.rotate_left(rotation);
            requested.extend([7, 31, 63, 97, requested[0]]);
            if rotation.is_multiple_of(2) {
                requested.reverse();
            }
            let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
            let result = handle.lookup_node_uuids(&requested).unwrap();
            let expected = requested
                .iter()
                .map(|id| {
                    (!matches!(*id, 7 | 31 | 63) && *id <= 96)
                        .then(|| Uuid::from_u128(u128::from(*id)))
                })
                .collect::<Vec<_>>();
            assert_eq!(result.values, expected);
            assert_eq!(result.metrics.per_record_seeks, 0);
            assert!(result.metrics.bytes_read <= 96 * UUID_WIDTH + TOMBSTONE_BLOCK_BYTES);
            assert!(result.metrics.peak_buffer_bytes <= 4 * TOMBSTONE_BLOCK_BYTES);
        }
    }

    #[test]
    fn absent_v4_classifies_valid_v3_and_present_malformed_v4_never_falls_back() {
        let (root, _, _) = crate::uuid_membership::tests::fixture();
        fs::write(
            root.path().join("topology/generation.json"),
            b"{\"topology_generation\":7,\"search_generation\":0,\"property_generation\":0}\n",
        )
        .unwrap();
        crate::rebuild_uuid_membership_indexes(root.path(), crate::UuidIndexBuildLimits::default())
            .unwrap();
        let index = root.path().join(INDEX_DIR);
        fs::write(index.join(LOCK_NAME), []).unwrap();
        let v4_path = index.join(MANIFEST_NAME);
        let v3_path = index.join("manifest.json");
        let v3 = fs::read(&v3_path).unwrap();
        assert!(matches!(
            V4OrdinalIdentityHandle::discover(root.path(), 7).unwrap(),
            V4OrdinalIdentityDiscovery::RebuildRequired { found_version: 3 }
        ));
        assert!(matches!(
            V4OrdinalIdentityHandle::open(
                root.path(),
                &V4OrdinalIdentityAuthority {
                    topology_generation: 7,
                    manifest_sha256: "00".repeat(32),
                },
                V4OrdinalIdentityLimits::default()
            ),
            Err(V4OrdinalIdentityError::Io)
        ));
        fs::remove_file(&v3_path).unwrap();
        assert!(matches!(
            V4OrdinalIdentityHandle::discover(root.path(), 7),
            Err(V4OrdinalIdentityError::InvalidDescriptor(_))
        ));
        fs::write(&v3_path, &v3).unwrap();
        let v3_manifest: serde_json::Value = serde_json::from_slice(&v3).unwrap();
        let run_name = v3_manifest["runs"][0]["identities"]["name"]
            .as_str()
            .unwrap();
        let run_path = index.join(run_name);
        let mut run = fs::read(&run_path).unwrap();
        run[0] ^= 1;
        fs::write(&run_path, run).unwrap();
        assert!(matches!(
            V4OrdinalIdentityHandle::discover(root.path(), 7),
            Err(V4OrdinalIdentityError::InvalidDescriptor(_))
        ));

        fs::write(&v4_path, b"{\"format_version\":4}").unwrap();
        assert_eq!(
            V4OrdinalIdentityHandle::discover(root.path(), 7).unwrap(),
            V4OrdinalIdentityDiscovery::Present
        );
        let malformed_digest = hex(&Sha256::digest(b"{\"format_version\":4}"));
        assert!(matches!(
            V4OrdinalIdentityHandle::open(
                root.path(),
                &V4OrdinalIdentityAuthority {
                    topology_generation: 7,
                    manifest_sha256: malformed_digest,
                },
                V4OrdinalIdentityLimits::default()
            ),
            Err(V4OrdinalIdentityError::InvalidDescriptor(_)) | Err(V4OrdinalIdentityError::Io)
        ));

        let fixture = Fixture::new(&[2], &[]);
        fixture.publish();
        let mut mixed = fixture.manifest.clone();
        mixed.ordinal_ranges[0].artifact.kind = V4OrdinalArtifactKind::ForwardIdentities;
        fs::write(
            fixture.root.path().join(INDEX_DIR).join(MANIFEST_NAME),
            serde_json::to_vec(&mixed).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            V4OrdinalIdentityHandle::open(
                fixture.root.path(),
                &fixture.authority(7),
                V4OrdinalIdentityLimits::default()
            ),
            Err(V4OrdinalIdentityError::InvalidDescriptor(_))
        ));
    }

    #[test]
    fn post_open_same_inode_same_length_mutation_invalidates_capability() {
        let fixture = Fixture::new(&[3], &[]);
        let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
        let path = fixture.root.path().join(INDEX_DIR).join("ordinal-0.uuidx");
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert_eq!(
            handle.lookup_node_uuids(&[1]).unwrap_err(),
            V4OrdinalIdentityError::Authentication
        );
    }

    #[test]
    fn pinned_generation_authority_rejects_whole_artifact_and_manifest_substitution() {
        let original = Fixture::new(&[3], &[]);
        let authority = original.authority(7);
        let replacement = Fixture::new(&[5], &[2]);
        let original_index = original.root.path().join(INDEX_DIR);
        let replacement_index = replacement.root.path().join(INDEX_DIR);
        for entry in fs::read_dir(&replacement_index).unwrap() {
            let entry = entry.unwrap();
            fs::copy(entry.path(), original_index.join(entry.file_name())).unwrap();
        }

        assert_eq!(
            V4OrdinalIdentityHandle::open(
                original.root.path(),
                &authority,
                V4OrdinalIdentityLimits::default()
            )
            .unwrap_err(),
            V4OrdinalIdentityError::Authentication
        );
    }

    #[cfg(unix)]
    #[test]
    fn cross_process_mutation_with_restored_mtime_fails_block_authentication() {
        let fixture = Fixture::new(&[3], &[]);
        let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
        let artifact = fixture.root.path().join(INDEX_DIR).join("ordinal-0.uuidx");
        let reference = fixture.root.path().join("original-time");
        assert!(
            Command::new("cp")
                .args(["-p"])
                .arg(&artifact)
                .arg(&reference)
                .status()
                .unwrap()
                .success()
        );
        assert!(Command::new("sh")
            .arg("-c")
            .arg("printf '\\001' | dd of=\"$ARTIFACT\" bs=1 seek=0 conv=notrunc 2>/dev/null && touch -r \"$REFERENCE\" \"$ARTIFACT\"")
            .env("ARTIFACT", &artifact)
            .env("REFERENCE", &reference)
            .status()
            .unwrap()
            .success());
        assert_eq!(
            handle.lookup_node_uuids(&[1]).unwrap_err(),
            V4OrdinalIdentityError::Authentication
        );
    }

    #[test]
    fn many_tombstone_runs_share_one_cache_budget_and_peak_charge() {
        let mut fixture = Fixture::new(&[32], &[]);
        let index = fixture.root.path().join(INDEX_DIR);
        fixture.manifest.topology_generation = 20;
        fixture.manifest.tombstones.clear();
        for generation in 1_u64..=20 {
            let bytes = generation.to_be_bytes();
            let name = format!("tombstones-{generation}.uuidx");
            fs::write(index.join(&name), bytes).unwrap();
            fixture.manifest.tombstones.push(V4OrdinalTombstones {
                generation,
                artifact: artifact(
                    name,
                    V4OrdinalArtifactKind::NodeTombstones,
                    generation,
                    &bytes,
                ),
                blocks: tombstone_blocks(&[generation]),
            });
        }
        fixture.publish();
        let mut handle = match V4OrdinalIdentityHandle::open(
            fixture.root.path(),
            &fixture.authority(20),
            V4OrdinalIdentityLimits {
                max_tombstone_cache_bytes: TOMBSTONE_CACHE_FIXED_CHARGE
                    + 2 * (TOMBSTONE_CACHE_ENTRY_CHARGE + TOMBSTONE_WIDTH_USIZE),
                ..V4OrdinalIdentityLimits::default()
            },
        )
        .unwrap()
        {
            V4OrdinalIdentityOpen::Ready(handle) => handle,
            _ => panic!("v4"),
        };
        let result = handle
            .lookup_node_uuids(&(1..=20).collect::<Vec<_>>())
            .unwrap();
        let cache_budget = TOMBSTONE_CACHE_FIXED_CHARGE
            + 2 * (TOMBSTONE_CACHE_ENTRY_CHARGE + TOMBSTONE_WIDTH_USIZE);
        assert_eq!(handle.tombstone_cache.charged_bytes, cache_budget);
        assert_eq!(handle.tombstone_cache.entries.len(), 2);
        assert_eq!(result.metrics.retained_cache_bytes, cache_budget as u64);
        assert!(result.metrics.transient_buffer_bytes > TOMBSTONE_WIDTH);
        assert!(
            result.metrics.peak_buffer_bytes
                <= 20 * REQUEST_ENTRY_CHARGE
                    + cache_budget as u64
                    + TOMBSTONE_CACHE_ENTRY_CHARGE as u64
                    + 3 * TOMBSTONE_WIDTH
        );
    }

    #[test]
    fn descriptor_corruption_overlap_truncation_and_substitution_fail_closed() {
        let mut fixture = Fixture::new(&[3, 2], &[]);
        fixture.manifest.ordinal_ranges[1].first_node_id = 3;
        fixture.publish();
        assert!(matches!(
            V4OrdinalIdentityHandle::open(
                fixture.root.path(),
                &fixture.authority(7),
                V4OrdinalIdentityLimits::default()
            ),
            Err(V4OrdinalIdentityError::InvalidDescriptor(_))
        ));

        let fixture = Fixture::new(&[3], &[]);
        let path = fixture.root.path().join(INDEX_DIR).join("ordinal-0.uuidx");
        fs::write(&path, [0_u8; 15]).unwrap();
        assert!(matches!(
            V4OrdinalIdentityHandle::open(
                fixture.root.path(),
                &fixture.authority(7),
                V4OrdinalIdentityLimits::default()
            ),
            Err(V4OrdinalIdentityError::Authentication)
        ));

        let mut fixture = Fixture::new(&[2], &[]);
        fixture.manifest.ordinal_ranges[0].first_node_id = u64::MAX;
        fixture.publish();
        assert!(matches!(
            V4OrdinalIdentityHandle::open(
                fixture.root.path(),
                &fixture.authority(7),
                V4OrdinalIdentityLimits::default()
            ),
            Err(V4OrdinalIdentityError::InvalidDescriptor(_))
        ));
    }

    #[test]
    fn forward_authority_mismatch_duplicate_uuid_and_surrogate_fail_closed() {
        fn publish_forward(fixture: &mut Fixture, records: &[(u128, u64)]) {
            let bytes = records
                .iter()
                .flat_map(|(uuid, id)| {
                    Uuid::from_u128(*uuid)
                        .into_bytes()
                        .into_iter()
                        .chain(id.to_be_bytes())
                })
                .collect::<Vec<_>>();
            fs::write(
                fixture.root.path().join(INDEX_DIR).join("forward.uuidx"),
                &bytes,
            )
            .unwrap();
            fixture.manifest.forward_identities[0] = artifact(
                "forward.uuidx".into(),
                V4OrdinalArtifactKind::ForwardIdentities,
                7,
                &bytes,
            );
            fixture.publish();
        }

        for records in [
            vec![(1, 2), (2, 1), (3, 3)],
            vec![(1, 1), (1, 2), (3, 3)],
            vec![(1, 1), (2, 1), (3, 3)],
        ] {
            let mut fixture = Fixture::new(&[3], &[]);
            publish_forward(&mut fixture, &records);
            assert!(matches!(
                V4OrdinalIdentityHandle::open(
                    fixture.root.path(),
                    &fixture.authority(7),
                    V4OrdinalIdentityLimits::default()
                ),
                Err(V4OrdinalIdentityError::InvalidDescriptor(_))
            ));
        }
    }

    #[test]
    fn generation_ordered_forward_runs_may_have_interleaved_uuid_keys() {
        let mut fixture = Fixture::new(&[4], &[]);
        let index = fixture.root.path().join(INDEX_DIR);
        let encode = |records: &[(u128, u64)]| {
            records
                .iter()
                .flat_map(|(uuid, id)| {
                    Uuid::from_u128(*uuid)
                        .into_bytes()
                        .into_iter()
                        .chain(id.to_be_bytes())
                })
                .collect::<Vec<_>>()
        };
        let older = encode(&[(1, 1), (3, 3)]);
        let newer = encode(&[(2, 2), (4, 4)]);
        fs::write(index.join("forward-6.uuidx"), &older).unwrap();
        fs::write(index.join("forward-7.uuidx"), &newer).unwrap();
        fixture.manifest.forward_identities = vec![
            artifact(
                "forward-6.uuidx".into(),
                V4OrdinalArtifactKind::ForwardIdentities,
                6,
                &older,
            ),
            artifact(
                "forward-7.uuidx".into(),
                V4OrdinalArtifactKind::ForwardIdentities,
                7,
                &newer,
            ),
        ];
        fixture.publish();

        let _handle = fixture.open(V4OrdinalIdentityLimits::default());
    }

    #[test]
    fn forward_runs_reject_cross_generation_duplicates_and_noncanonical_order() {
        let mut fixture = Fixture::new(&[4], &[]);
        let index = fixture.root.path().join(INDEX_DIR);
        let encode = |records: &[(u128, u64)]| {
            records
                .iter()
                .flat_map(|(uuid, id)| {
                    Uuid::from_u128(*uuid)
                        .into_bytes()
                        .into_iter()
                        .chain(id.to_be_bytes())
                })
                .collect::<Vec<_>>()
        };
        let older = encode(&[(1, 1), (3, 3)]);
        let duplicate = encode(&[(1, 1), (2, 2), (4, 4)]);
        fs::write(index.join("forward-6.uuidx"), &older).unwrap();
        fs::write(index.join("forward-7.uuidx"), &duplicate).unwrap();
        fixture.manifest.forward_identities = vec![
            artifact(
                "forward-6.uuidx".into(),
                V4OrdinalArtifactKind::ForwardIdentities,
                6,
                &older,
            ),
            artifact(
                "forward-7.uuidx".into(),
                V4OrdinalArtifactKind::ForwardIdentities,
                7,
                &duplicate,
            ),
        ];
        fixture.publish();
        assert!(matches!(
            V4OrdinalIdentityHandle::open(
                fixture.root.path(),
                &fixture.authority(7),
                V4OrdinalIdentityLimits::default()
            ),
            Err(V4OrdinalIdentityError::InvalidDescriptor(
                "forward identity UUID is repeated across generations"
            ))
        ));

        fixture.manifest.forward_identities.swap(0, 1);
        fixture.publish();
        assert!(matches!(
            V4OrdinalIdentityHandle::open(
                fixture.root.path(),
                &fixture.authority(7),
                V4OrdinalIdentityLimits::default()
            ),
            Err(V4OrdinalIdentityError::InvalidDescriptor(
                "forward runs are not in canonical generation order"
            ))
        ));
    }

    #[test]
    fn request_limit_fails_before_request_allocation() {
        let fixture = Fixture::new(&[3], &[]);
        let mut handle = fixture.open(V4OrdinalIdentityLimits {
            max_requested: 2,
            ..V4OrdinalIdentityLimits::default()
        });
        assert_eq!(
            handle.lookup_node_uuids(&[1, 2, 3]).unwrap_err(),
            V4OrdinalIdentityError::RequestLimit {
                requested: 3,
                maximum: 2,
            }
        );

        for cache_budget in [0, TOMBSTONE_CACHE_FIXED_CHARGE - 1] {
            assert!(matches!(
                V4OrdinalIdentityHandle::open(
                    fixture.root.path(),
                    &fixture.authority(7),
                    V4OrdinalIdentityLimits {
                        max_tombstone_cache_bytes: cache_budget,
                        ..V4OrdinalIdentityLimits::default()
                    }
                ),
                Err(V4OrdinalIdentityError::InvalidDescriptor(
                    "lookup bounds are invalid"
                ))
            ));
        }
    }

    #[test]
    fn generation_uppercase_digest_and_unknown_tombstone_fail_closed() {
        let fixture = Fixture::new(&[3], &[]);
        assert_eq!(
            V4OrdinalIdentityHandle::open(
                fixture.root.path(),
                &fixture.authority(8),
                V4OrdinalIdentityLimits::default()
            )
            .unwrap_err(),
            V4OrdinalIdentityError::GenerationMismatch {
                expected: 8,
                found: 7
            }
        );

        let mut uppercase = fixture.manifest.clone();
        uppercase.ordinal_ranges[0].artifact.sha256 =
            uppercase.ordinal_ranges[0].artifact.sha256.to_uppercase();
        fs::write(
            fixture.root.path().join(INDEX_DIR).join(MANIFEST_NAME),
            serde_json::to_vec(&uppercase).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            V4OrdinalIdentityHandle::open(
                fixture.root.path(),
                &fixture.authority(7),
                V4OrdinalIdentityLimits::default()
            ),
            Err(V4OrdinalIdentityError::InvalidDescriptor(_))
        ));

        let fixture = Fixture::new(&[3], &[]);
        let manifest_path = fixture.root.path().join(INDEX_DIR).join(MANIFEST_NAME);
        let mut manifest = serde_json::to_value(&fixture.manifest).unwrap();
        manifest["untrusted_extension"] = serde_json::json!(true);
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert!(matches!(
            V4OrdinalIdentityHandle::open(
                fixture.root.path(),
                &fixture.authority(7),
                V4OrdinalIdentityLimits::default()
            ),
            Err(V4OrdinalIdentityError::Io)
        ));

        let mut fixture = Fixture::new(&[3], &[]);
        let unknown = 99_u64.to_be_bytes();
        fs::write(
            fixture.root.path().join(INDEX_DIR).join("tombstones.uuidx"),
            unknown,
        )
        .unwrap();
        fixture.manifest.tombstones[0].artifact = artifact(
            "tombstones.uuidx".into(),
            V4OrdinalArtifactKind::NodeTombstones,
            7,
            &unknown,
        );
        fixture.manifest.tombstones[0].blocks = tombstone_blocks(&[99]);
        fixture.publish();
        assert!(matches!(
            V4OrdinalIdentityHandle::open(
                fixture.root.path(),
                &fixture.authority(7),
                V4OrdinalIdentityLimits::default()
            ),
            Err(V4OrdinalIdentityError::InvalidDescriptor(_))
        ));
    }

    #[test]
    fn selected_tombstone_blocks_are_cached_without_full_file_rescans() {
        let count = (TOMBSTONE_BLOCK_BYTES / TOMBSTONE_WIDTH) * 3;
        let tombstones = (1..=count).collect::<Vec<_>>();
        let fixture = Fixture::new(&[count], &tombstones);
        let mut handle = fixture.open(V4OrdinalIdentityLimits {
            max_coalesced_read_bytes: TOMBSTONE_BLOCK_BYTES as usize,
            ..V4OrdinalIdentityLimits::default()
        });
        let first = handle.lookup_node_uuids(&[1]).unwrap();
        let repeated = handle.lookup_node_uuids(&[1]).unwrap();
        let far = handle.lookup_node_uuids(&[count]).unwrap();
        assert_eq!(first.metrics.bytes_read, TOMBSTONE_BLOCK_BYTES);
        assert_eq!(repeated.metrics.bytes_read, 0);
        assert_eq!(far.metrics.bytes_read, TOMBSTONE_BLOCK_BYTES);
        assert!(
            first.metrics.bytes_read + repeated.metrics.bytes_read + far.metrics.bytes_read
                < 3 * count * TOMBSTONE_WIDTH
        );
        assert_eq!(far.metrics.per_record_seeks, 0);
    }

    #[test]
    fn authenticated_block_read_has_no_per_record_seeks() {
        let fixture = Fixture::new(&[16], &[]);
        let mut handle = fixture.open(V4OrdinalIdentityLimits::default());
        let result = handle
            .lookup_node_uuids(&(1..=16).collect::<Vec<_>>())
            .unwrap();
        assert_eq!(result.metrics.sequential_read_calls, 1);
        assert_eq!(result.metrics.bytes_read, 16 * UUID_WIDTH);
        assert_eq!(result.metrics.per_record_seeks, 0);
        assert!(
            result.metrics.peak_buffer_bytes
                <= 16 * REQUEST_ENTRY_CHARGE
                    + TOMBSTONE_CACHE_FIXED_CHARGE as u64
                    + 16 * UUID_WIDTH
        );
    }

    #[test]
    fn adjacent_authenticated_blocks_coalesce_within_gap_and_read_cap() {
        let fixture = Fixture::new(&[8_200], &[]);
        let requested = [1, 4_097, 8_200];
        let mut coalesced = fixture.open(V4OrdinalIdentityLimits::default());
        let one = coalesced.lookup_node_uuids(&requested).unwrap();
        assert_eq!(one.metrics.sequential_read_calls, 1);
        assert_eq!(one.metrics.bytes_read, 8_200 * UUID_WIDTH);

        let mut split = fixture.open(V4OrdinalIdentityLimits {
            max_coalesced_read_bytes: ORDINAL_BLOCK_BYTES_USIZE,
            ..V4OrdinalIdentityLimits::default()
        });
        let three = split.lookup_node_uuids(&requested).unwrap();
        assert_eq!(three.metrics.sequential_read_calls, 3);
        assert_eq!(three.values, one.values);
        assert_eq!(three.metrics.per_record_seeks, 0);
    }

    #[test]
    fn one_two_four_x_work_is_linear_constant_factor() {
        let mut prior_bytes = 0;
        for count in [64_u64, 128, 256] {
            let fixture = Fixture::new(&[count], &[]);
            let mut handle = fixture.open(V4OrdinalIdentityLimits {
                max_requested: count as usize,
                ..V4OrdinalIdentityLimits::default()
            });
            let result = handle
                .lookup_node_uuids(&(1..=count).collect::<Vec<_>>())
                .unwrap();
            assert_eq!(result.metrics.bytes_read, count * UUID_WIDTH);
            assert_eq!(result.metrics.sequential_read_calls, 1);
            assert_eq!(result.metrics.per_record_seeks, 0);
            if prior_bytes != 0 {
                assert_eq!(result.metrics.bytes_read, prior_bytes * 2);
            }
            prior_bytes = result.metrics.bytes_read;
        }
    }

    #[test]
    fn admission_streams_artifacts_with_bounded_cross_run_validation_counters() {
        let mut prior_bytes = 0;
        let mut prior_calls = 0;
        let mut prior_metadata = 0;
        for (count, expected_calls) in [(4_096_u64, 3_u64), (8_192, 4), (16_384, 6)] {
            let fixture = Fixture::new(&[count], &[]);
            let handle = fixture.open(V4OrdinalIdentityLimits::default());
            let metrics = handle.admission_metrics();
            assert_eq!(metrics.artifacts, 3);
            assert_eq!(
                metrics.authenticated_bytes,
                count * (FORWARD_RECORD_WIDTH * 2 + UUID_WIDTH)
            );
            assert_eq!(metrics.sequential_read_calls, expected_calls);
            assert!(metrics.peak_buffer_bytes <= STREAM_BYTES as u64);
            assert!(metrics.peak_buffer_bytes >= metrics.retained_descriptor_bytes);
            assert!(metrics.manifest_bytes > 0);
            if prior_bytes != 0 {
                assert_eq!(metrics.authenticated_bytes, prior_bytes * 2);
                assert!(metrics.sequential_read_calls <= prior_calls * 2);
                assert!(metrics.retained_descriptor_bytes >= prior_metadata);
                assert!(metrics.retained_descriptor_bytes <= prior_metadata * 2);
            }
            prior_bytes = metrics.authenticated_bytes;
            prior_calls = metrics.sequential_read_calls;
            prior_metadata = metrics.retained_descriptor_bytes;
        }
    }

    #[test]
    fn descriptor_metadata_budget_is_enforced_before_artifact_admission() {
        let fixture = Fixture::new(&[8_192], &[]);
        let admitted = fixture.open(V4OrdinalIdentityLimits::default());
        let required = admitted.admission_metrics().retained_descriptor_bytes as usize;
        assert!(required > DESCRIPTOR_FIXED_CHARGE);
        assert!(matches!(
            V4OrdinalIdentityHandle::open(
                fixture.root.path(),
                &fixture.authority(7),
                V4OrdinalIdentityLimits {
                    max_descriptor_metadata_bytes: required - 1,
                    ..V4OrdinalIdentityLimits::default()
                }
            ),
            Err(V4OrdinalIdentityError::InvalidDescriptor(
                "descriptor metadata exceeds admission bound"
            ))
        ));
    }

    #[test]
    fn tombstones_must_be_sorted_known_ordinals_but_newer_duplicates_are_safe() {
        let fixture = Fixture::new(&[4], &[2]);
        let index = fixture.root.path().join(INDEX_DIR);
        let duplicate = 2_u64.to_be_bytes();
        fs::write(index.join("tombstones-new.uuidx"), duplicate).unwrap();
        let mut manifest = fixture.manifest.clone();
        manifest.topology_generation = 8;
        manifest.tombstones.push(V4OrdinalTombstones {
            generation: 8,
            artifact: artifact(
                "tombstones-new.uuidx".into(),
                V4OrdinalArtifactKind::NodeTombstones,
                8,
                &duplicate,
            ),
            blocks: tombstone_blocks(&[2]),
        });
        // Retained range generation is allowed; newest deletion still wins.
        fs::write(
            index.join(MANIFEST_NAME),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let mut handle = match V4OrdinalIdentityHandle::open(
            fixture.root.path(),
            &fixture.authority(8),
            V4OrdinalIdentityLimits::default(),
        )
        .unwrap()
        {
            V4OrdinalIdentityOpen::Ready(handle) => *handle,
            _ => panic!("v4"),
        };
        assert_eq!(handle.lookup_node_uuids(&[2]).unwrap().values, [None]);
    }
}
