//! Publisher-side derivation of a Project summary from a verified portable-v2 package.
//!
//! One function, [`summarize_verified_portable_v2`], is the only producer of
//! summary bytes: the package is fully verified first, then only the
//! `workspace/research_metadata` and `workspace/configuration` participants are
//! read, through the storage-owned authenticated reader. Publishers and
//! verifiers run the same function over the same package and obtain identical
//! canonical bytes, whether the package is a bundle or an expanded directory.
//! Local paths, Project identity, and anything outside the verified package are
//! never consulted.

use crate::discovery_portable_v2::DiscoveryPortableV2Error;
use crate::{PortableVerifyRequest, verify_portable_v2};
use graphforge_core::portable::{
    PortableV2Limits, PortableV2Mode, PortableV2OntologyComposition, PortableV2PackageClass,
    PortableV2ParticipantId,
};
use graphforge_discovery::{
    BridgeSetDescriptor, DiscoveryLimits, PORTABLE_V2_FORMAT, PROJECT_SUMMARY_CAPABILITY,
    PROJECT_SUMMARY_FORMAT, ProjectSummary, ProtocolRequirement, ProtocolVersion,
    RepositoryIdentity, Sha256Digest, SummaryAccess, SummaryCorpusSize, SummaryFacts,
    SummaryGeographicCoverage, SummaryMetadata, SummaryOntologyComposition, SummaryOntologyModule,
    SummaryPackageReference, SummaryTemporalCoverage,
};
use graphforge_storage::{
    MAX_WORKSPACE_RESEARCH_METADATA_BYTES, PortableV2PackageIndex, ResearchAccessPolicyMetadata,
    ResearchCorpusSize, ResearchGeographicCoverage, ResearchTemporalCoverage,
    WORKSPACE_CAPABILITY_ID, WORKSPACE_CONFIGURATION_FAMILY, WORKSPACE_RESEARCH_METADATA_FAMILY,
    WorkspaceConfiguration, WorkspaceOntologyMode, WorkspaceResearchMetadata,
};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::AtomicBool;

/// Inputs for deriving one Project summary.
pub struct ProjectSummaryRequest<'a> {
    /// Repository the summary describes.
    pub repository: &'a RepositoryIdentity,
    /// Immutable repository version the summary describes.
    pub immutable_version: &'a Sha256Digest,
    /// Complete portable-v2 bundle file or expanded directory.
    pub package: &'a Path,
    /// Bounds applied while validating the produced summary.
    pub discovery_limits: DiscoveryLimits,
    /// Bounds applied by the portable-v2 verifier and package reader.
    pub portable_limits: PortableV2Limits,
    /// Optional cooperative cancellation signal.
    pub cancelled: Option<&'a AtomicBool>,
}

/// Fully verify a portable-v2 package, then derive its validated [`ProjectSummary`].
///
/// Absent `workspace/research_metadata` yields empty metadata and absent
/// `workspace/configuration` yields ontology mode `none`. Counts are the
/// metadata's declared `corpus_size` plus facts read from the authenticated
/// manifest; no graph payload is read.
///
/// # Errors
/// Returns the portable verifier's error for any package defect,
/// [`DiscoveryPortableV2Error::Participant`] when a read participant is not a
/// valid canonical record, and a discovery error when the derived document
/// fails summary validation.
pub fn summarize_verified_portable_v2(
    request: &ProjectSummaryRequest<'_>,
) -> Result<ProjectSummary, DiscoveryPortableV2Error> {
    let report = verify_portable_v2(
        &PortableVerifyRequest {
            input: request.package.to_path_buf(),
            mode: PortableV2Mode::Full,
            limits: request.portable_limits,
        },
        request.cancelled,
    )
    .map_err(DiscoveryPortableV2Error::Portable)?;
    let index = PortableV2PackageIndex::open(
        request.package,
        &report.package_digest,
        request.portable_limits,
        request.cancelled,
    )
    .map_err(DiscoveryPortableV2Error::Portable)?;

    let (metadata, ontology_mode) = read_workspace_records(&index, request)?;
    let components = index.component_kind_counts();
    let facts = SummaryFacts {
        ontology_mode: ontology_mode.into(),
        research_present: components.contains_key("research"),
        evidence_present: components.contains_key("evidence"),
        components,
        payload_bytes: report.payload_bytes,
        ontology_composition: report.ontology_composition.as_ref().map(composition_facts),
    };
    let summary = ProjectSummary {
        format: PROJECT_SUMMARY_FORMAT.into(),
        version: ProtocolVersion::CURRENT,
        repository: request.repository.clone(),
        immutable_version: request.immutable_version.clone(),
        package: SummaryPackageReference {
            format: PORTABLE_V2_FORMAT.into(),
            package_digest: Sha256Digest(report.package_digest),
            package_class: package_class_label(report.package_class).into(),
        },
        requirements: vec![ProtocolRequirement {
            capability: PROJECT_SUMMARY_CAPABILITY.into(),
            major: 1,
        }],
        capabilities: Vec::new(),
        metadata: project_public_metadata(&metadata),
        facts,
        extensions: BTreeMap::new(),
    };
    summary
        .validate(request.discovery_limits)
        .map_err(DiscoveryPortableV2Error::Discovery)?;
    Ok(summary)
}

/// Read the two workspace participants a summary depends on.
///
/// Absent research metadata is empty metadata; absent configuration is ontology
/// mode `none`.
fn read_workspace_records(
    index: &PortableV2PackageIndex,
    request: &ProjectSummaryRequest<'_>,
) -> Result<(WorkspaceResearchMetadata, &'static str), DiscoveryPortableV2Error> {
    let read_participant = |family: &'static str, max_bytes: u64| {
        let participant = PortableV2ParticipantId {
            capability_id: WORKSPACE_CAPABILITY_ID.into(),
            record_family_id: family.into(),
        };
        index
            .participant_file(&participant)
            .and_then(|file| {
                file.map(|file| index.read(&file, max_bytes, request.cancelled))
                    .transpose()
            })
            .map_err(DiscoveryPortableV2Error::Portable)
    };
    let metadata = match read_participant(
        WORKSPACE_RESEARCH_METADATA_FAMILY,
        MAX_WORKSPACE_RESEARCH_METADATA_BYTES as u64,
    )? {
        Some(bytes) => WorkspaceResearchMetadata::from_canonical_json(&bytes).map_err(|error| {
            DiscoveryPortableV2Error::Participant {
                participant: "workspace/research_metadata",
                message: error.to_string(),
            }
        })?,
        None => WorkspaceResearchMetadata::empty(),
    };
    let ontology_mode = match read_participant(
        WORKSPACE_CONFIGURATION_FAMILY,
        request.portable_limits.max_manifest_bytes,
    )? {
        Some(bytes) => ontology_mode_label(
            WorkspaceConfiguration::from_canonical_json(&bytes)
                .map_err(|error| DiscoveryPortableV2Error::Participant {
                    participant: "workspace/configuration",
                    message: error.to_string(),
                })?
                .ontology_mode,
        ),
        None => "none",
    };
    Ok((metadata, ontology_mode))
}

/// Mirror the verified composition control in the summary's `sha256:` form.
fn composition_facts(composition: &PortableV2OntologyComposition) -> SummaryOntologyComposition {
    SummaryOntologyComposition {
        composition_digest: Sha256Digest(composition.composition_digest.clone()),
        modules: composition
            .modules
            .iter()
            .map(|module| SummaryOntologyModule {
                id: module.ontology_id.clone(),
                version: module.version.clone(),
                content_digest: Sha256Digest(module.content_digest.clone()),
                dialect: module.dialect.clone(),
                profile: module.profile.clone(),
            })
            .collect(),
        bridge_sets: composition
            .bridge_sets
            .iter()
            .map(|bridge| BridgeSetDescriptor {
                id: bridge.bridge_id.clone(),
                version: bridge.version.clone(),
                content_digest: Sha256Digest(bridge.content_digest.clone()),
            })
            .collect(),
    }
}

/// Project the public-safe subset of research metadata.
///
/// This is the single place that decides which Project metadata fields leave
/// the Project. Every field of [`WorkspaceResearchMetadata`] and its nested
/// records is destructured by name, so adding a field fails to compile here
/// until a maintainer decides whether it is public. The deliberate exclusions
/// are `access.collaborators`, `extensions`, and `discovery_facets`.
fn project_public_metadata(record: &WorkspaceResearchMetadata) -> SummaryMetadata {
    let WorkspaceResearchMetadata {
        // Record contract version is a storage detail, not Project content.
        contract_version: _,
        title,
        description,
        authors,
        subjects,
        languages,
        geographic_coverage,
        temporal_coverage,
        source_types,
        corpus_size,
        ontologies,
        license,
        access,
        tags,
        originating_projects,
        related_projects,
        canonical_identifiers,
        external_identifiers,
        created_at,
        updated_at,
        // Excluded: community extension fields may carry arbitrary private data.
        extensions: _,
        // Excluded: index counters are not authored Project metadata.
        discovery_facets: _,
    } = record;
    let ResearchAccessPolicyMetadata {
        visibility,
        access_policy,
        // Excluded: collaborator labels identify people.
        collaborators: _,
    } = access;
    SummaryMetadata {
        title: title.clone(),
        description: description.clone(),
        authors: authors.clone(),
        subjects: subjects.clone(),
        languages: languages.clone(),
        geographic_coverage: geographic_coverage.as_ref().map(|coverage| {
            let ResearchGeographicCoverage { label, regions } = coverage;
            SummaryGeographicCoverage {
                label: label.clone(),
                regions: regions.clone(),
            }
        }),
        temporal_coverage: temporal_coverage.as_ref().map(|coverage| {
            let ResearchTemporalCoverage { start, end, label } = coverage;
            SummaryTemporalCoverage {
                start: start.clone(),
                end: end.clone(),
                label: label.clone(),
            }
        }),
        source_types: source_types.clone(),
        corpus_size: corpus_size.as_ref().map(|size| {
            let ResearchCorpusSize {
                node_count,
                relationship_count,
                source_count,
                artifact_count,
            } = size;
            SummaryCorpusSize {
                node_count: *node_count,
                relationship_count: *relationship_count,
                source_count: *source_count,
                artifact_count: *artifact_count,
            }
        }),
        ontologies: ontologies.clone(),
        license: license.clone(),
        access: SummaryAccess {
            visibility: visibility.clone(),
            access_policy: access_policy.clone(),
        },
        tags: tags.clone(),
        originating_projects: originating_projects.clone(),
        related_projects: related_projects.clone(),
        canonical_identifiers: canonical_identifiers.clone(),
        external_identifiers: external_identifiers.clone(),
        created_at: created_at.clone(),
        updated_at: updated_at.clone(),
    }
}

/// Storage ontology mode to the summary's closed label set.
const fn ontology_mode_label(mode: WorkspaceOntologyMode) -> &'static str {
    match mode {
        WorkspaceOntologyMode::None => "none",
        WorkspaceOntologyMode::Advisory => "advisory",
        WorkspaceOntologyMode::Strict => "strict",
    }
}

/// Portable package class to the summary's closed label set.
const fn package_class_label(class: PortableV2PackageClass) -> &'static str {
    match class {
        PortableV2PackageClass::Complete => "complete",
        PortableV2PackageClass::OntologyOnly => "ontology-only",
        PortableV2PackageClass::ComponentSelective => "component-selective",
        PortableV2PackageClass::GraphDataSubset => "graph-data-subset",
    }
}
