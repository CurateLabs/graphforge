//! Verification-first bridge from discovery responses to portable.
//!
//! Discovery validates repository/ref selection and names an expected semantic
//! package digest. This module then delegates package integrity, compatibility,
//! and authenticity exclusively to [`crate::verify_portable`]. A successful
//! result is constructed only after both authorities agree.

use crate::{PortableVerifyRequest, verify_portable};
use graphforge_core::portable::{PortableError, PortableLimits, PortableMode, PortableReport};
use graphforge_discovery::{
    DiscoveryError, DiscoveryLimits, DiscoveryManifest, RefSet, RepositoryIdentity,
};
use std::fmt;
use std::path::Path;
use std::sync::atomic::AtomicBool;

/// Stable cross-contract mismatch classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiscoveryPortableMismatch {
    /// The response names a repository other than the requested identity.
    Repository,
    /// The refs snapshot and manifest select different immutable versions.
    ImmutableVersion,
    /// The verified portable package digest differs from the discovery reference.
    PackageDigest,
    /// The verified package does not carry the requested exact ontology module.
    ModuleIdentity,
    /// The module document's recomputed canonical digest differs from its identity.
    ModuleContentDigest,
    /// The verified package's research registry does not carry the selected
    /// research Version with the lineage identity digest and kind.
    ResearchVersionIdentity,
}

/// Failure at one of the explicit discovery-to-package trust boundaries.
#[derive(Debug)]
pub enum DiscoveryPortableError {
    /// The discovery response itself is invalid or unsupported.
    Discovery(DiscoveryError),
    /// Valid discovery documents disagree with the requested or verified identity.
    ReferenceMismatch(DiscoveryPortableMismatch),
    /// The storage-owned portable verifier rejected the package.
    Portable(PortableError),
    /// A verified package participant is not a valid canonical record.
    Participant {
        /// Stable participant name such as `workspace/research_metadata`.
        participant: &'static str,
        /// Owner-supplied validation message.
        message: String,
    },
}

impl fmt::Display for DiscoveryPortableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Discovery(error) => write!(formatter, "{error}"),
            Self::ReferenceMismatch(kind) => {
                write!(formatter, "discovery portable reference mismatch: {kind:?}")
            }
            Self::Portable(error) => write!(formatter, "{error}"),
            Self::Participant {
                participant,
                message,
            } => write!(formatter, "invalid {participant} participant: {message}"),
        }
    }
}

impl std::error::Error for DiscoveryPortableError {}

/// Fully accepted immutable discovery selection and storage verification report.
#[derive(Debug)]
pub struct DiscoveredPortable {
    /// Canonical repository identity requested by the caller.
    pub repository: RepositoryIdentity,
    /// Ref resolved by the discovery manifest.
    pub resolved_ref: String,
    /// Immutable repository version selected by the refs snapshot.
    pub immutable_version: String,
    /// Report produced by the storage-owned portable verifier.
    pub report: PortableReport,
}

/// Inputs for one verification-first discovery/package admission attempt.
pub struct DiscoveryPortableRequest<'a> {
    /// Untrusted discovery manifest bytes.
    pub manifest_json: &'a [u8],
    /// Untrusted refs snapshot bytes.
    pub refs_json: &'a [u8],
    /// Canonical repository identity requested by the caller.
    pub expected_repository: &'a RepositoryIdentity,
    /// Complete downloaded portable file or expanded directory.
    pub package: &'a Path,
    /// Bounds applied while parsing discovery documents.
    pub discovery_limits: DiscoveryLimits,
    /// Bounds applied by the portable verifier.
    pub portable_limits: PortableLimits,
    /// Portable verification depth.
    pub mode: PortableMode,
    /// Optional cooperative cancellation signal.
    pub cancelled: Option<&'a AtomicBool>,
}

/// Parse discovery documents and bind repository and immutable-version selection.
///
/// Performs no package I/O. Shared by every verification-first entry point so
/// each rejects discovery and binding failures before touching a package path.
pub(crate) fn bind_discovery(
    manifest_json: &[u8],
    refs_json: &[u8],
    expected_repository: &RepositoryIdentity,
    limits: DiscoveryLimits,
) -> Result<DiscoveryManifest, DiscoveryPortableError> {
    let manifest = DiscoveryManifest::from_json(manifest_json, limits)
        .map_err(DiscoveryPortableError::Discovery)?;
    let refs = RefSet::from_json(refs_json, limits).map_err(DiscoveryPortableError::Discovery)?;
    if &manifest.repository != expected_repository || &refs.repository != expected_repository {
        return Err(DiscoveryPortableError::ReferenceMismatch(
            DiscoveryPortableMismatch::Repository,
        ));
    }
    refs.validate_manifest(&manifest).map_err(|error| {
        if error.field == Some("immutable_version") || error.field == Some("resolved_ref") {
            DiscoveryPortableError::ReferenceMismatch(DiscoveryPortableMismatch::ImmutableVersion)
        } else {
            DiscoveryPortableError::Discovery(error)
        }
    })?;
    Ok(manifest)
}

/// Validate discovery documents, bind their immutable selection, and verify the package.
///
/// No partially accepted value is returned: package verification and the final
/// package-digest comparison complete before [`DiscoveredPortable`] exists.
pub fn verify_discovered_portable(
    request: &DiscoveryPortableRequest<'_>,
) -> Result<DiscoveredPortable, DiscoveryPortableError> {
    let manifest = bind_discovery(
        request.manifest_json,
        request.refs_json,
        request.expected_repository,
        request.discovery_limits,
    )?;
    let report = verify_portable(
        &PortableVerifyRequest {
            input: request.package.to_path_buf(),
            mode: request.mode,
            limits: request.portable_limits,
        },
        request.cancelled,
    )
    .map_err(DiscoveryPortableError::Portable)?;
    if report.package_digest != manifest.package.package_digest.0 {
        return Err(DiscoveryPortableError::ReferenceMismatch(
            DiscoveryPortableMismatch::PackageDigest,
        ));
    }
    Ok(DiscoveredPortable {
        repository: manifest.repository,
        resolved_ref: manifest.resolved_ref,
        immutable_version: manifest.immutable_version.0,
        report,
    })
}
