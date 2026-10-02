//! Verification-first bridge from discovery responses to portable-v2.
//!
//! Discovery validates repository/ref selection and names an expected semantic
//! package digest. This module then delegates package integrity, compatibility,
//! and authenticity exclusively to [`crate::verify_portable_v2`]. A successful
//! result is constructed only after both authorities agree.

use crate::{PortableVerifyRequest, verify_portable_v2};
use graphforge_core::portable::{
    PortableV2Error, PortableV2Limits, PortableV2Mode, PortableV2Report,
};
use graphforge_discovery::{
    DiscoveryError, DiscoveryLimits, DiscoveryManifest, RefSet, RepositoryIdentity,
};
use std::fmt;
use std::path::Path;
use std::sync::atomic::AtomicBool;

/// Stable cross-contract mismatch classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiscoveryPortableV2Mismatch {
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
}

/// Failure at one of the explicit discovery-to-package trust boundaries.
#[derive(Debug)]
pub enum DiscoveryPortableV2Error {
    /// The discovery response itself is invalid or unsupported.
    Discovery(DiscoveryError),
    /// Valid discovery documents disagree with the requested or verified identity.
    ReferenceMismatch(DiscoveryPortableV2Mismatch),
    /// The storage-owned portable-v2 verifier rejected the package.
    Portable(PortableV2Error),
    /// A verified package participant is not a valid canonical record.
    Participant {
        /// Stable participant name such as `workspace/research_metadata`.
        participant: &'static str,
        /// Owner-supplied validation message.
        message: String,
    },
}

impl fmt::Display for DiscoveryPortableV2Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Discovery(error) => write!(formatter, "{error}"),
            Self::ReferenceMismatch(kind) => {
                write!(
                    formatter,
                    "discovery portable-v2 reference mismatch: {kind:?}"
                )
            }
            Self::Portable(error) => write!(formatter, "{error}"),
            Self::Participant {
                participant,
                message,
            } => write!(formatter, "invalid {participant} participant: {message}"),
        }
    }
}

impl std::error::Error for DiscoveryPortableV2Error {}

/// Fully accepted immutable discovery selection and storage verification report.
#[derive(Debug)]
pub struct DiscoveredPortableV2 {
    /// Canonical repository identity requested by the caller.
    pub repository: RepositoryIdentity,
    /// Ref resolved by the discovery manifest.
    pub resolved_ref: String,
    /// Immutable repository version selected by the refs snapshot.
    pub immutable_version: String,
    /// Report produced by the storage-owned portable-v2 verifier.
    pub report: PortableV2Report,
}

/// Inputs for one verification-first discovery/package admission attempt.
pub struct DiscoveryPortableV2Request<'a> {
    /// Untrusted discovery manifest bytes.
    pub manifest_json: &'a [u8],
    /// Untrusted refs snapshot bytes.
    pub refs_json: &'a [u8],
    /// Canonical repository identity requested by the caller.
    pub expected_repository: &'a RepositoryIdentity,
    /// Complete downloaded portable-v2 file or expanded directory.
    pub package: &'a Path,
    /// Bounds applied while parsing discovery documents.
    pub discovery_limits: DiscoveryLimits,
    /// Bounds applied by the portable-v2 verifier.
    pub portable_limits: PortableV2Limits,
    /// Portable verification depth.
    pub mode: PortableV2Mode,
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
) -> Result<DiscoveryManifest, DiscoveryPortableV2Error> {
    let manifest = DiscoveryManifest::from_json(manifest_json, limits)
        .map_err(DiscoveryPortableV2Error::Discovery)?;
    let refs = RefSet::from_json(refs_json, limits).map_err(DiscoveryPortableV2Error::Discovery)?;
    if &manifest.repository != expected_repository || &refs.repository != expected_repository {
        return Err(DiscoveryPortableV2Error::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::Repository,
        ));
    }
    refs.validate_manifest(&manifest).map_err(|error| {
        if error.field == Some("immutable_version") || error.field == Some("resolved_ref") {
            DiscoveryPortableV2Error::ReferenceMismatch(
                DiscoveryPortableV2Mismatch::ImmutableVersion,
            )
        } else {
            DiscoveryPortableV2Error::Discovery(error)
        }
    })?;
    Ok(manifest)
}

/// Validate discovery documents, bind their immutable selection, and verify the package.
///
/// No partially accepted value is returned: package verification and the final
/// package-digest comparison complete before [`DiscoveredPortableV2`] exists.
pub fn verify_discovered_portable_v2(
    request: &DiscoveryPortableV2Request<'_>,
) -> Result<DiscoveredPortableV2, DiscoveryPortableV2Error> {
    let manifest = bind_discovery(
        request.manifest_json,
        request.refs_json,
        request.expected_repository,
        request.discovery_limits,
    )?;
    let report = verify_portable_v2(
        &PortableVerifyRequest {
            input: request.package.to_path_buf(),
            mode: request.mode,
            limits: request.portable_limits,
        },
        request.cancelled,
    )
    .map_err(DiscoveryPortableV2Error::Portable)?;
    if report.package_digest != manifest.package.package_digest.0 {
        return Err(DiscoveryPortableV2Error::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::PackageDigest,
        ));
    }
    Ok(DiscoveredPortableV2 {
        repository: manifest.repository,
        resolved_ref: manifest.resolved_ref,
        immutable_version: manifest.immutable_version.0,
        report,
    })
}
