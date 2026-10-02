//! Bounded, versioned Project summary document (`graphforge-project-summary/1`).
//!
//! A summary lets a Hub render a Project page without downloading graph data.
//! It carries only public-safe metadata and verified package facts, and it has
//! no URL or location field: transport locations live solely in the manifest's
//! `objects`, so summary bytes and `summary_digest` do not depend on where the
//! bytes are hosted.

use crate::{
    BridgeSetDescriptor, DiscoveryError, DiscoveryErrorCode, DiscoveryLimits,
    DiscoveryVersionDetails, DiscoveryVersionSubject, PORTABLE_V2_FORMAT,
    PROJECT_SUMMARY_CAPABILITY, PROJECT_SUMMARY_FORMAT, PROJECT_SUMMARY_FORMAT_NAME,
    ProtocolCapability, ProtocolRequirement, ProtocolVersion, RepositoryIdentity, Sha256Digest,
    canonical_json, check_ascending, check_identity_text, check_string, format_major, limit,
    parse_unique_json, sha256_digest, validate_extensions, validate_semantics,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// Maximum UTF-8 bytes of one bounded metadata string, matching Project metadata.
const MAX_METADATA_STRING_BYTES: usize = 4096;
/// Maximum entries of one metadata string list, matching Project metadata.
const MAX_METADATA_LIST_ENTRIES: usize = 256;
/// Portable package classes a summary may name.
const PACKAGE_CLASSES: [&str; 4] = [
    "complete",
    "ontology-only",
    "component-selective",
    "graph-data-subset",
];
/// Portable component kinds counted in `facts.components`.
const COMPONENT_KINDS: [&str; 10] = [
    "ontology",
    "schema",
    "migration",
    "settings",
    "graph-data",
    "derived-artifact",
    "evidence",
    "provenance",
    "compatibility",
    "research",
];
/// Ontology enforcement modes a summary may report.
const ONTOLOGY_MODES: [&str; 3] = ["none", "advisory", "strict"];

/// Portable package the summary describes, by semantic identity only.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryPackageReference {
    /// Must be [`PORTABLE_V2_FORMAT`] in summary v1.
    pub format: String,
    /// Portable semantic package identity.
    pub package_digest: Sha256Digest,
    /// Portable package class: `complete`, `ontology-only`,
    /// `component-selective`, or `graph-data-subset`.
    pub package_class: String,
}

/// Geographic coverage declared in Project metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryGeographicCoverage {
    /// Optional human-readable label.
    pub label: Option<String>,
    /// Region labels in strictly ascending order.
    pub regions: Vec<String>,
}

/// Temporal coverage declared in Project metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryTemporalCoverage {
    /// Optional inclusive start label or ISO date.
    pub start: Option<String>,
    /// Optional inclusive end label or ISO date.
    pub end: Option<String>,
    /// Optional human-readable period label.
    pub label: Option<String>,
}

/// Caller-declared corpus scale. These are declarations, not computed counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryCorpusSize {
    /// Optional declared node count.
    pub node_count: Option<u64>,
    /// Optional declared relationship count.
    pub relationship_count: Option<u64>,
    /// Optional declared source count.
    pub source_count: Option<u64>,
    /// Optional declared artifact count.
    pub artifact_count: Option<u64>,
}

/// Consumer-facing access metadata. Core does not enforce it; collaborators are
/// deliberately not part of the public projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryAccess {
    /// Optional visibility label such as `private`, `shared`, or `public`.
    pub visibility: Option<String>,
    /// Optional access-policy description.
    pub access_policy: Option<String>,
}

/// Public-safe projection of Project research metadata.
///
/// Mirrors every Project metadata field except `access.collaborators`,
/// `extensions`, and `discovery_facets`. String and list bounds are those of the
/// source record: strings are non-empty, at most 4096 bytes, and free of ASCII
/// control characters; lists hold at most 256 strictly ascending entries.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryMetadata {
    /// Human title.
    pub title: Option<String>,
    /// Bounded description.
    pub description: Option<String>,
    /// Authors or maintainers.
    pub authors: Vec<String>,
    /// Subject labels.
    pub subjects: Vec<String>,
    /// Language labels.
    pub languages: Vec<String>,
    /// Geographic coverage.
    pub geographic_coverage: Option<SummaryGeographicCoverage>,
    /// Temporal coverage.
    pub temporal_coverage: Option<SummaryTemporalCoverage>,
    /// Declared source-type labels.
    pub source_types: Vec<String>,
    /// Declared corpus scale.
    pub corpus_size: Option<SummaryCorpusSize>,
    /// Ontology or knowledge-standard labels.
    pub ontologies: Vec<String>,
    /// SPDX or other license label.
    pub license: Option<String>,
    /// Consumer access metadata.
    pub access: SummaryAccess,
    /// Free-form tags.
    pub tags: Vec<String>,
    /// Originating Project identifiers.
    pub originating_projects: Vec<String>,
    /// Related Project identifiers.
    pub related_projects: Vec<String>,
    /// Canonical identifiers such as DOI or ARK.
    pub canonical_identifiers: Vec<String>,
    /// External identifiers from other systems.
    pub external_identifiers: Vec<String>,
    /// Creation time label.
    pub created_at: Option<String>,
    /// Last metadata update time label.
    pub updated_at: Option<String>,
}

/// One module of the composition a summary reports.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryOntologyModule {
    /// Opaque ontology identifier.
    pub id: String,
    /// Opaque authored version.
    pub version: String,
    /// Canonical content digest identifying this exact module.
    pub content_digest: Sha256Digest,
    /// Ontology dialect label.
    pub dialect: String,
    /// Ontology profile label.
    pub profile: String,
}

/// Ontology composition verified in the described package.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryOntologyComposition {
    /// Semantic identity of the whole composition.
    pub composition_digest: Sha256Digest,
    /// Modules in strictly ascending `(id, version, content_digest)` order.
    pub modules: Vec<SummaryOntologyModule>,
    /// Bridge sets in strictly ascending `(id, version, content_digest)` order.
    pub bridge_sets: Vec<BridgeSetDescriptor>,
}

/// Facts a publisher verified from the portable package without graph payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryFacts {
    /// Ontology enforcement mode: `none`, `advisory`, or `strict`.
    pub ontology_mode: String,
    /// Histogram of package components by kind. Only kinds with at least one
    /// component appear, so each fact has exactly one representation.
    pub components: BTreeMap<String, u64>,
    /// Whether the package carries a research component.
    pub research_present: bool,
    /// Whether the package carries an evidence component.
    pub evidence_present: bool,
    /// Total verified payload bytes of the package.
    pub payload_bytes: u64,
    /// Ontology composition, when the package carries one.
    pub ontology_composition: Option<SummaryOntologyComposition>,
}

/// Validated Project summary document.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSummary {
    /// Contract identifier; must equal [`PROJECT_SUMMARY_FORMAT`].
    pub format: String,
    /// Protocol reader/writer version.
    pub version: ProtocolVersion,
    /// Repository the summary describes.
    pub repository: RepositoryIdentity,
    /// Immutable repository version the summary describes.
    pub immutable_version: Sha256Digest,
    /// Portable package the summary describes.
    pub package: SummaryPackageReference,
    /// Required semantics; only `project-summary@1` is understood.
    pub requirements: Vec<ProtocolRequirement>,
    /// Optional advertised semantics.
    pub capabilities: Vec<ProtocolCapability>,
    /// Public-safe Project metadata.
    pub metadata: SummaryMetadata,
    /// Verified package facts.
    pub facts: SummaryFacts,
    /// Explicit optional extension values, preserved canonically by readers.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, Value>,
}

impl ProjectSummary {
    /// Parse and fully validate untrusted JSON without performing object I/O.
    ///
    /// The input is bounded by both `max_response_bytes` and `max_summary_bytes`.
    pub fn from_json(bytes: &[u8], limits: DiscoveryLimits) -> Result<Self, DiscoveryError> {
        if bytes.len() > limits.max_response_bytes {
            return Err(limit("response"));
        }
        if bytes.len() > limits.max_summary_bytes {
            return Err(limit("summary"));
        }
        let summary: Self = parse_unique_json(bytes)?;
        summary.validate(limits)?;
        Ok(summary)
    }

    /// Validate all summary invariants. Unknown required semantics and unknown
    /// format major versions fail before any other content is considered.
    pub fn validate(&self, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
        if self.format != PROJECT_SUMMARY_FORMAT {
            let error = DiscoveryError::new(
                DiscoveryErrorCode::UnsupportedFuture,
                Some("format"),
                "project summary format is unsupported",
            );
            return Err(
                match format_major(&self.format, PROJECT_SUMMARY_FORMAT_NAME) {
                    Some(requested_major) => error.with_version(DiscoveryVersionDetails {
                        subject: DiscoveryVersionSubject::ProjectSummary,
                        supported_major: Some(1),
                        requested_major,
                    }),
                    None => error,
                },
            );
        }
        self.version.validate()?;
        self.repository.validate()?;
        self.immutable_version.validate()?;
        self.validate_package()?;
        validate_semantics(
            &self.requirements,
            &self.capabilities,
            PROJECT_SUMMARY_CAPABILITY,
            limits,
        )?;
        validate_extensions(&self.extensions, limits)?;
        validate_metadata(&self.metadata, limits)?;
        validate_facts(&self.facts, limits)
    }

    fn validate_package(&self) -> Result<(), DiscoveryError> {
        if self.package.format != PORTABLE_V2_FORMAT {
            let error = DiscoveryError::new(
                DiscoveryErrorCode::UnsupportedFuture,
                Some("package.format"),
                "portable package format is unsupported",
            );
            return Err(
                match format_major(&self.package.format, "graphforge-project") {
                    Some(requested_major) => error.with_version(DiscoveryVersionDetails {
                        subject: DiscoveryVersionSubject::PortablePackage,
                        supported_major: Some(2),
                        requested_major,
                    }),
                    None => error,
                },
            );
        }
        self.package.package_digest.validate()?;
        if !PACKAGE_CLASSES.contains(&self.package.package_class.as_str()) {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::MalformedResponse,
                Some("package.package_class"),
                "portable package class is unknown",
            ));
        }
        Ok(())
    }

    /// Encode deterministic compact JSON with recursively sorted keys.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, DiscoveryError> {
        self.validate(DiscoveryLimits::default())?;
        canonical_json(self)
    }

    /// Compute SHA-256 over [`Self::to_canonical_json`]. This is the
    /// `summary_digest` a manifest references.
    pub fn canonical_digest(&self) -> Result<Sha256Digest, DiscoveryError> {
        Ok(sha256_digest(&self.to_canonical_json()?))
    }
}

fn metadata_limits(limits: DiscoveryLimits) -> DiscoveryLimits {
    DiscoveryLimits {
        max_string_bytes: limits.max_string_bytes.min(MAX_METADATA_STRING_BYTES),
        ..limits
    }
}

fn check_text(
    value: &str,
    field: &'static str,
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    check_string(value, field, limits)?;
    if value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(DiscoveryError::new(
            DiscoveryErrorCode::MalformedResponse,
            Some(field),
            "text contains control characters",
        ));
    }
    Ok(())
}

fn check_optional_text(
    value: Option<&String>,
    field: &'static str,
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    value.map_or(Ok(()), |value| check_text(value, field, limits))
}

fn check_list(
    values: &[String],
    field: &'static str,
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    if values.len() > MAX_METADATA_LIST_ENTRIES {
        return Err(limit(field));
    }
    let mut prior: Option<&str> = None;
    for value in values {
        check_text(value, field, limits)?;
        if prior.is_some_and(|prior| prior >= value.as_str()) {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::Duplicate,
                Some(field),
                "entries are duplicated or not canonically ordered",
            ));
        }
        prior = Some(value);
    }
    Ok(())
}

fn validate_metadata(
    metadata: &SummaryMetadata,
    limits: DiscoveryLimits,
) -> Result<(), DiscoveryError> {
    // Exhaustive destructuring: adding a field fails to compile until it is
    // validated here.
    let SummaryMetadata {
        title,
        description,
        authors,
        subjects,
        languages,
        geographic_coverage,
        temporal_coverage,
        source_types,
        corpus_size: _,
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
    } = metadata;
    let limits = metadata_limits(limits);
    check_optional_text(title.as_ref(), "metadata.title", limits)?;
    check_optional_text(description.as_ref(), "metadata.description", limits)?;
    check_list(authors, "metadata.authors", limits)?;
    check_list(subjects, "metadata.subjects", limits)?;
    check_list(languages, "metadata.languages", limits)?;
    if let Some(geographic) = geographic_coverage {
        check_optional_text(
            geographic.label.as_ref(),
            "metadata.geographic_coverage",
            limits,
        )?;
        check_list(&geographic.regions, "metadata.geographic_coverage", limits)?;
    }
    if let Some(temporal) = temporal_coverage {
        check_optional_text(
            temporal.start.as_ref(),
            "metadata.temporal_coverage",
            limits,
        )?;
        check_optional_text(temporal.end.as_ref(), "metadata.temporal_coverage", limits)?;
        check_optional_text(
            temporal.label.as_ref(),
            "metadata.temporal_coverage",
            limits,
        )?;
    }
    check_list(source_types, "metadata.source_types", limits)?;
    check_list(ontologies, "metadata.ontologies", limits)?;
    check_optional_text(license.as_ref(), "metadata.license", limits)?;
    check_optional_text(access.visibility.as_ref(), "metadata.access", limits)?;
    check_optional_text(access.access_policy.as_ref(), "metadata.access", limits)?;
    check_list(tags, "metadata.tags", limits)?;
    check_list(
        originating_projects,
        "metadata.originating_projects",
        limits,
    )?;
    check_list(related_projects, "metadata.related_projects", limits)?;
    check_list(
        canonical_identifiers,
        "metadata.canonical_identifiers",
        limits,
    )?;
    check_list(
        external_identifiers,
        "metadata.external_identifiers",
        limits,
    )?;
    check_optional_text(created_at.as_ref(), "metadata.created_at", limits)?;
    check_optional_text(updated_at.as_ref(), "metadata.updated_at", limits)
}

fn validate_facts(facts: &SummaryFacts, limits: DiscoveryLimits) -> Result<(), DiscoveryError> {
    if !ONTOLOGY_MODES.contains(&facts.ontology_mode.as_str()) {
        return Err(DiscoveryError::new(
            DiscoveryErrorCode::MalformedResponse,
            Some("facts.ontology_mode"),
            "ontology mode is unknown",
        ));
    }
    for (kind, count) in &facts.components {
        if !COMPONENT_KINDS.contains(&kind.as_str()) || *count == 0 {
            return Err(DiscoveryError::new(
                DiscoveryErrorCode::MalformedResponse,
                Some("facts.components"),
                "component histogram entry is invalid",
            ));
        }
    }
    let present = |kind: &str| facts.components.contains_key(kind);
    if facts.research_present != present("research")
        || facts.evidence_present != present("evidence")
    {
        return Err(DiscoveryError::new(
            DiscoveryErrorCode::MalformedResponse,
            Some("facts"),
            "presence flags disagree with component histogram",
        ));
    }
    let Some(composition) = &facts.ontology_composition else {
        return Ok(());
    };
    composition.composition_digest.validate()?;
    if composition.modules.len() > limits.max_ontology_entries {
        return Err(limit("facts.ontology_composition.modules"));
    }
    if composition.bridge_sets.len() > limits.max_ontology_entries {
        return Err(limit("facts.ontology_composition.bridge_sets"));
    }
    let mut prior: Option<(&str, &str, &str)> = None;
    for module in &composition.modules {
        const FIELD: &str = "facts.ontology_composition.modules";
        check_identity_text(&module.id, &module.version, FIELD, limits)?;
        module.content_digest.validate()?;
        check_string(&module.dialect, FIELD, limits)?;
        check_string(&module.profile, FIELD, limits)?;
        check_ascending(
            &mut prior,
            (
                module.id.as_str(),
                module.version.as_str(),
                module.content_digest.0.as_str(),
            ),
            FIELD,
        )?;
    }
    let mut prior: Option<(&str, &str, &str)> = None;
    for bridge in &composition.bridge_sets {
        const FIELD: &str = "facts.ontology_composition.bridge_sets";
        check_identity_text(&bridge.id, &bridge.version, FIELD, limits)?;
        bridge.content_digest.validate()?;
        check_ascending(
            &mut prior,
            (
                bridge.id.as_str(),
                bridge.version.as_str(),
                bridge.content_digest.0.as_str(),
            ),
            FIELD,
        )?;
    }
    Ok(())
}
