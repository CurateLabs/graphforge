//! Verification-first bridge from discovery lineage to a research Version package.
//!
//! Discovery validates repository, refs, and lineage binding, then names an
//! expected semantic package digest for one research Version. Package integrity
//! belongs to the storage-owned portable-v2 verifier; native research validation
//! of every archived Version runs exactly as in [`crate::verify_portable_v2`].
//! This module additionally requires the verified research registry to carry the
//! selected Version with the lineage's identity digest and kind, before any
//! destination exists.

use crate::discovery_portable_v2::DiscoveryPortableV2Mismatch;
use graphforge_core::portable::{
    PortableV2Error, PortableV2Limits, PortableV2Mode, PortableV2Report,
};
use graphforge_discovery::{
    DiscoveryError, DiscoveryLimits, DiscoveryManifest, LineageVersion, RefSet, RepositoryIdentity,
    ResearchLineage,
};
use std::fmt;
use std::path::Path;
use std::sync::atomic::AtomicBool;

/// Failure at one of the explicit discovery-to-research-package trust boundaries.
#[derive(Debug)]
pub enum DiscoveryResearchVersionError {
    /// The discovery response itself is invalid or unsupported.
    Discovery(DiscoveryError),
    /// Valid discovery documents disagree with the requested or verified identity.
    ReferenceMismatch(DiscoveryPortableV2Mismatch),
    /// The storage-owned portable-v2 verifier rejected the package.
    Portable(PortableV2Error),
}

impl fmt::Display for DiscoveryResearchVersionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Discovery(error) => write!(formatter, "{error}"),
            Self::ReferenceMismatch(kind) => {
                write!(
                    formatter,
                    "discovery research-version reference mismatch: {kind:?}"
                )
            }
            Self::Portable(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for DiscoveryResearchVersionError {}

/// Fully accepted research Version selection and storage verification report.
#[derive(Debug)]
pub struct DiscoveredResearchVersion {
    /// Canonical repository identity requested by the caller.
    pub repository: RepositoryIdentity,
    /// Immutable research Version UUID selected by the caller.
    pub version_uuid: String,
    /// Lineage Version metadata bound before package verification.
    pub version: LineageVersion,
    /// Report produced by the storage-owned portable-v2 verifier.
    pub report: PortableV2Report,
}

/// Inputs for one verification-first research Version admission attempt.
pub struct DiscoveryResearchVersionRequest<'a> {
    /// Untrusted discovery manifest bytes.
    pub manifest_json: &'a [u8],
    /// Untrusted refs snapshot bytes.
    pub refs_json: &'a [u8],
    /// Untrusted research lineage document bytes.
    pub lineage_json: &'a [u8],
    /// Canonical repository identity requested by the caller.
    pub expected_repository: &'a RepositoryIdentity,
    /// Research Version UUID to resolve through lineage.
    pub version_uuid: &'a str,
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
    /// Directory in which native research validation materializes the
    /// package; `None` uses the system temporary directory. Pass a directory
    /// on the import destination's filesystem.
    pub scratch: Option<&'a Path>,
}

/// Validate discovery documents, bind lineage, and verify the Version package.
pub fn verify_discovered_research_version(
    request: &DiscoveryResearchVersionRequest<'_>,
) -> Result<DiscoveredResearchVersion, DiscoveryResearchVersionError> {
    let manifest = DiscoveryManifest::from_json(request.manifest_json, request.discovery_limits)
        .map_err(DiscoveryResearchVersionError::Discovery)?;
    let refs = RefSet::from_json(request.refs_json, request.discovery_limits)
        .map_err(DiscoveryResearchVersionError::Discovery)?;
    let lineage = ResearchLineage::from_json(request.lineage_json, request.discovery_limits)
        .map_err(DiscoveryResearchVersionError::Discovery)?;
    if &manifest.repository != request.expected_repository
        || &refs.repository != request.expected_repository
    {
        return Err(DiscoveryResearchVersionError::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::Repository,
        ));
    }
    refs.validate_manifest(&manifest)
        .map_err(map_immutable_mismatch)
        .and_then(|()| {
            manifest
                .bind_lineage(&refs, &lineage)
                .map_err(DiscoveryResearchVersionError::Discovery)
        })?;
    let (version, _object) = manifest
        .research_version_object(&lineage, request.version_uuid)
        .map_err(DiscoveryResearchVersionError::Discovery)?;
    let package = version.package.as_ref().expect(
        "research_version_object returns only Versions with an advertised package reference",
    );
    let report = graphforge_storage::verify_portable_v2(
        request.package,
        request.mode,
        request.portable_limits,
        request.cancelled,
    )
    .map_err(DiscoveryResearchVersionError::Portable)?;
    if report.package_digest != package.package_digest.0 {
        return Err(DiscoveryResearchVersionError::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::PackageDigest,
        ));
    }
    // A research Version package must carry research interchange; a package
    // without it cannot prove which Version it holds.
    if !report.research_interchange {
        return Err(DiscoveryResearchVersionError::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::ResearchVersionIdentity,
        ));
    }
    let report = verify_selected_research_version(request, version)?;
    Ok(DiscoveredResearchVersion {
        repository: manifest.repository,
        version_uuid: request.version_uuid.to_owned(),
        version: version.clone(),
        report,
    })
}

/// Run native research validation of every archived Version and require the
/// selected Version's record to match the lineage identity digest and kind.
#[cfg(feature = "research")]
fn verify_selected_research_version(
    request: &DiscoveryResearchVersionRequest<'_>,
    expected: &LineageVersion,
) -> Result<PortableV2Report, DiscoveryResearchVersionError> {
    use graphforge_storage::research_versions::{ResearchRegistry, ResearchVersionRecord};
    let selected = uuid::Uuid::parse_str(&expected.version_uuid).map_err(|_| {
        DiscoveryResearchVersionError::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::ResearchVersionIdentity,
        )
    })?;
    let mut matched = None;
    let mut validator = |generation: &graphforge_storage::ResolvedProjectGeneration,
                         version: &ResearchVersionRecord,
                         registry: &ResearchRegistry| {
        crate::research_interchange::validation::validate(generation, version, registry)?;
        if version.version_uuid == selected {
            matched = Some(selected_matches(expected, version, registry));
        }
        Ok(())
    };
    let report = graphforge_storage::validate_research_package(
        request.package,
        request.scratch,
        request.portable_limits,
        request.cancelled,
        &mut validator,
    )
    .map_err(DiscoveryResearchVersionError::Portable)?;
    if matched != Some(true) {
        return Err(DiscoveryResearchVersionError::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::ResearchVersionIdentity,
        ));
    }
    Ok(report)
}

#[cfg(not(feature = "research"))]
fn verify_selected_research_version(
    _request: &DiscoveryResearchVersionRequest<'_>,
    _expected: &LineageVersion,
) -> Result<PortableV2Report, DiscoveryResearchVersionError> {
    Err(DiscoveryResearchVersionError::Portable(
        PortableV2Error::new(
            graphforge_core::portable::PortableV2ErrorCode::Incompatible,
            "research package validation requires the `research` feature",
        ),
    ))
}

/// True when the archived record is exactly the lineage Version: the registry
/// commitment and the record's recomputed identity both equal the lineage
/// identity digest, and complete/projection kind and source agree.
#[cfg(feature = "research")]
fn selected_matches(
    expected: &LineageVersion,
    version: &graphforge_storage::research_versions::ResearchVersionRecord,
    registry: &graphforge_storage::research_versions::ResearchRegistry,
) -> bool {
    let hex = |bytes: &[u8; 32]| {
        use std::fmt::Write as _;
        bytes
            .iter()
            .fold(String::from("sha256:"), |mut output, byte| {
                write!(output, "{byte:02x}").expect("writing to a string cannot fail");
                output
            })
    };
    let committed = registry.identities.get(&version.version_uuid).map(hex);
    let recomputed = version.identity_sha256().ok().map(|bytes| hex(&bytes));
    let Ok(source) = crate::discovery_research_lineage::projection_source(registry, version) else {
        return false;
    };
    let source = source.map(|id| id.to_string());
    let kind = if source.is_some() {
        "projection"
    } else {
        "complete"
    };
    committed.as_deref() == Some(expected.identity_digest.0.as_str())
        && recomputed.as_deref() == Some(expected.identity_digest.0.as_str())
        && kind == expected.kind
        && source == expected.source_version_uuid
}

fn map_immutable_mismatch(error: DiscoveryError) -> DiscoveryResearchVersionError {
    if error.field == Some("immutable_version") || error.field == Some("resolved_ref") {
        DiscoveryResearchVersionError::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::ImmutableVersion,
        )
    } else {
        DiscoveryResearchVersionError::Discovery(error)
    }
}
