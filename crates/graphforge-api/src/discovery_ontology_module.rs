//! Consumer-side resolution of one exact ontology module from a discovered package.
//!
//! Discovery names a module by exact identity and advertises a per-module
//! portable-v2 package. Three identities stay distinct here: the module identity
//! (`id`, `version`, canonical content digest), the module document's file
//! SHA-256, and the package digest. Two Projects that adopt the same exact
//! module publish packages with different `package_digest`s (each package binds
//! its own source generation) but the same module identity, document bytes, and
//! file SHA-256. Resolution therefore verifies the package, then proves the
//! module independently of the package identity.

use crate::discovery_portable_v2::{
    DiscoveryPortableV2Error, DiscoveryPortableV2Mismatch, bind_discovery,
};
use crate::{PortableVerifyRequest, verify_portable_v2};
use graphforge_core::portable::{
    PortableV2CompositionEntry, PortableV2Limits, PortableV2Mode, PortableV2Report,
};
use graphforge_discovery::{DiscoveryLimits, ExactIdentity, RepositoryIdentity};
use graphforge_ontology::{OntologyDoc, OntologyModuleId, module_document_digest};
use graphforge_storage::{PortableV2FileRef, PortableV2PackageIndex};
use std::path::Path;
use std::sync::atomic::AtomicBool;

/// Inputs for resolving one exact ontology module from a discovered package.
pub struct DiscoveryOntologyModuleRequest<'a> {
    /// Untrusted discovery manifest bytes.
    pub manifest_json: &'a [u8],
    /// Untrusted refs snapshot bytes.
    pub refs_json: &'a [u8],
    /// Canonical repository identity requested by the caller.
    pub expected_repository: &'a RepositoryIdentity,
    /// Exact module requested, as advertised by the manifest.
    pub module: &'a ExactIdentity,
    /// Complete downloaded per-module portable-v2 file or expanded directory.
    pub package: &'a Path,
    /// Bounds applied while parsing discovery documents.
    pub discovery_limits: DiscoveryLimits,
    /// Bounds applied by the portable-v2 verifier and package reader.
    pub portable_limits: PortableV2Limits,
    /// Optional cooperative cancellation signal.
    pub cancelled: Option<&'a AtomicBool>,
}

/// An exact ontology module proven against its advertised identity.
#[derive(Debug)]
pub struct ResolvedOntologyModule {
    /// Exact module identity; `canonical_digest` is lowercase hex without a prefix.
    pub module: OntologyModuleId,
    /// Semantic digest (`sha256:...`) of the package that carried the module.
    ///
    /// This identifies the package, not the module, and differs between
    /// Projects that publish the same module.
    pub package_digest: String,
    /// Lowercase hex SHA-256 of `document`, as listed by the package manifest.
    pub module_sha256: String,
    /// Exact bytes of the module document as stored in the package.
    pub document: Vec<u8>,
    /// Report produced by the storage-owned portable-v2 verifier.
    pub report: PortableV2Report,
}

/// Validate and bind discovery documents, verify the module package, and prove
/// the requested exact module.
///
/// Order is fixed: discovery parsing, repository and refs binding, and
/// descriptor selection all complete before any package path is read. Then the
/// package is fully verified; its `package_digest` must equal the descriptor's;
/// the module identity must appear in the verified composition; and the module
/// document is read through the manifest-authenticated reader and its
/// domain-separated canonical digest is recomputed and compared with the
/// advertised `content_digest`. No value is returned unless every step agrees.
///
/// # Errors
/// Returns a discovery error (for example `missing_object` when the identity is
/// not advertised, or `malformed_response` when the descriptor names the Project
/// package), a reference mismatch, or the portable verifier's error.
pub fn resolve_discovered_ontology_module(
    request: &DiscoveryOntologyModuleRequest<'_>,
) -> Result<ResolvedOntologyModule, DiscoveryPortableV2Error> {
    let manifest = bind_discovery(
        request.manifest_json,
        request.refs_json,
        request.expected_repository,
        request.discovery_limits,
    )?;
    let (_descriptor, advertised, _object) = manifest
        .ontology_module_selection(request.module)
        .map_err(DiscoveryPortableV2Error::Discovery)?;

    let report = verify_portable_v2(
        &PortableVerifyRequest {
            input: request.package.to_path_buf(),
            mode: PortableV2Mode::Full,
            limits: request.portable_limits,
        },
        request.cancelled,
    )
    .map_err(DiscoveryPortableV2Error::Portable)?;
    if report.package_digest != advertised.package_digest.0 {
        return Err(DiscoveryPortableV2Error::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::PackageDigest,
        ));
    }

    let module = request.module;
    let identity_matches = |id: &str, version: &str, content_digest: &str| {
        id == module.id && version == module.version && content_digest == module.content_digest.0
    };
    let carried = report.ontology_composition.as_ref().is_some_and(|control| {
        control.modules.iter().any(|candidate| {
            identity_matches(
                &candidate.ontology_id,
                &candidate.version,
                &candidate.content_digest,
            )
        })
    });
    let entry = report
        .ontology_composition_entries
        .iter()
        .find(|entry| {
            entry.kind == "ontology"
                && identity_matches(
                    &entry.identity.id,
                    &entry.identity.version,
                    &entry.identity.content_digest,
                )
        })
        .filter(|_| carried)
        .ok_or(DiscoveryPortableV2Error::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::ModuleIdentity,
        ))?;

    let (document, recomputed, module_sha256) = read_and_prove_module(request, &report, entry)?;

    Ok(ResolvedOntologyModule {
        module: OntologyModuleId {
            ontology_id: module.id.clone(),
            authored_version: module.version.clone(),
            canonical_digest: recomputed,
        },
        package_digest: report.package_digest.clone(),
        module_sha256,
        document,
        report,
    })
}

/// Read the module document through the authenticated reader and prove that its
/// recomputed canonical digest is the requested identity.
///
/// Returns `(document, canonical digest hex, file sha256 hex)`.
fn read_and_prove_module(
    request: &DiscoveryOntologyModuleRequest<'_>,
    report: &PortableV2Report,
    entry: &PortableV2CompositionEntry,
) -> Result<(Vec<u8>, String, String), DiscoveryPortableV2Error> {
    let module = request.module;
    let index = PortableV2PackageIndex::open(
        request.package,
        &report.package_digest,
        request.portable_limits,
        request.cancelled,
    )
    .map_err(DiscoveryPortableV2Error::Portable)?;
    let file = PortableV2FileRef {
        path: entry.path.clone(),
        length: entry.length,
        sha256: entry.sha256.clone(),
    };
    let document = index
        .read(
            &file,
            request.portable_limits.max_manifest_bytes,
            request.cancelled,
        )
        .map_err(DiscoveryPortableV2Error::Portable)?;

    // The module's identity is its domain-separated canonical content digest,
    // independent of the package that carried it and of the file's SHA-256.
    let parsed: OntologyDoc = serde_json::from_slice(&document).map_err(|error| {
        DiscoveryPortableV2Error::Participant {
            participant: "ontology module document",
            message: error.to_string(),
        }
    })?;
    let recomputed = module_document_digest(&parsed).map_err(|message| {
        DiscoveryPortableV2Error::Participant {
            participant: "ontology module document",
            message,
        }
    })?;
    // The recomputed digest is the authority. The id and version comparison is
    // defence in depth and mirrors the portable verifier, which exempts
    // `legacy:` identities (their document carries the authored id, not the
    // synthetic one).
    let document_names_identity = module.id.starts_with("legacy:")
        || (parsed.ontology_id == module.id && parsed.version == module.version);
    if format!("sha256:{recomputed}") != module.content_digest.0 || !document_names_identity {
        return Err(DiscoveryPortableV2Error::ReferenceMismatch(
            DiscoveryPortableV2Mismatch::ModuleContentDigest,
        ));
    }
    Ok((document, recomputed, file.sha256))
}
