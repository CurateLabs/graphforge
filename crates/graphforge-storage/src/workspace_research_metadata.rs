//! Generation-managed research Project metadata and bounded local discovery.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use graphforge_core::{GfError, ProjectErrorCode};
use graphforge_filesystem::FileIdentity;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    ProjectParticipant, ProjectParticipantEncoding, WORKSPACE_CAPABILITY_ID,
    WORKSPACE_CAPABILITY_VERSION,
};

/// Canonical research-metadata record family.
pub const WORKSPACE_RESEARCH_METADATA_FAMILY: &str = "research_metadata";
/// Frozen research-metadata contract version.
pub const WORKSPACE_RESEARCH_METADATA_VERSION: u32 = 1;
/// Maximum canonical research-metadata participant size.
pub const MAX_WORKSPACE_RESEARCH_METADATA_BYTES: usize = 256 * 1024;
/// Maximum UTF-8 bytes for one bounded metadata string.
pub const MAX_RESEARCH_METADATA_STRING_BYTES: usize = 4_096;
/// Maximum entries in one bounded metadata string list.
pub const MAX_RESEARCH_METADATA_LIST_ENTRIES: usize = 256;
/// Maximum community extension fields.
pub const MAX_RESEARCH_METADATA_EXTENSION_FIELDS: usize = 64;

/// Bounded geographic coverage metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchGeographicCoverage {
    /// Optional human-readable label.
    pub label: Option<String>,
    /// Canonical region labels in sorted order.
    pub regions: Vec<String>,
}

/// Bounded temporal coverage metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchTemporalCoverage {
    /// Optional inclusive start label or ISO date.
    pub start: Option<String>,
    /// Optional inclusive end label or ISO date.
    pub end: Option<String>,
    /// Optional human-readable period label.
    pub label: Option<String>,
}

/// Caller-declared corpus scale without opening graph payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchCorpusSize {
    /// Optional declared node count.
    pub node_count: Option<u64>,
    /// Optional declared relationship count.
    pub relationship_count: Option<u64>,
    /// Optional declared source count.
    pub source_count: Option<u64>,
    /// Optional declared artifact count.
    pub artifact_count: Option<u64>,
}

/// Policy metadata for consumers; Core does not enforce access.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchAccessPolicyMetadata {
    /// Optional visibility label such as `private`, `shared`, or `public`.
    pub visibility: Option<String>,
    /// Optional access-policy description.
    pub access_policy: Option<String>,
    /// Maintainer or collaborator labels in canonical order.
    pub collaborators: Vec<String>,
}

/// Entry-point availability counts for discovery summaries.
///
/// Later lifecycle issues populate these without changing the contract.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchDiscoveryFacets {
    /// Text search entry points.
    pub text_entry_points: u64,
    /// Source entry points.
    pub source_entry_points: u64,
    /// Entity entry points.
    pub entity_entry_points: u64,
    /// Relationship entry points.
    pub relationship_entry_points: u64,
    /// Story or document entry points.
    pub story_document_entry_points: u64,
    /// Event or location entry points.
    pub event_location_entry_points: u64,
    /// Ontology or type entry points.
    pub ontology_type_entry_points: u64,
    /// Linguistic-property entry points.
    pub linguistic_property_entry_points: u64,
    /// Traversal or structured-query entry points.
    pub traversal_query_entry_points: u64,
    /// Existing Branch entry points.
    pub branch_entry_points: u64,
}

/// Canonical authoritative research Project metadata participant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceResearchMetadata {
    /// Frozen record contract version.
    pub contract_version: u32,
    /// Human title.
    pub title: Option<String>,
    /// Bounded description.
    pub description: Option<String>,
    /// Authors or maintainers in canonical order.
    pub authors: Vec<String>,
    /// Subject labels in canonical order.
    pub subjects: Vec<String>,
    /// Language labels in canonical order.
    pub languages: Vec<String>,
    /// Geographic coverage.
    pub geographic_coverage: Option<ResearchGeographicCoverage>,
    /// Temporal coverage.
    pub temporal_coverage: Option<ResearchTemporalCoverage>,
    /// Declared source-type labels.
    pub source_types: Vec<String>,
    /// Declared corpus scale.
    pub corpus_size: Option<ResearchCorpusSize>,
    /// Ontology or knowledge-standard labels.
    pub ontologies: Vec<String>,
    /// SPDX or other license label.
    pub license: Option<String>,
    /// Access metadata for consumers.
    pub access: ResearchAccessPolicyMetadata,
    /// Free-form tags in canonical order.
    pub tags: Vec<String>,
    /// Originating Project identifiers.
    pub originating_projects: Vec<String>,
    /// Related Project identifiers.
    pub related_projects: Vec<String>,
    /// Canonical identifiers such as DOI or ARK.
    pub canonical_identifiers: Vec<String>,
    /// External identifiers from other systems.
    pub external_identifiers: Vec<String>,
    /// Creation time in RFC 3339 microsecond precision.
    pub created_at: Option<String>,
    /// Last metadata update time in RFC 3339 microsecond precision.
    pub updated_at: Option<String>,
    /// Community extension fields in canonical key order.
    pub extensions: BTreeMap<String, Value>,
    /// Discovery facet counts; zero until later issues populate indexes.
    pub discovery_facets: ResearchDiscoveryFacets,
}

/// Stable durable Project identity separate from one graph snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchProjectIdentity {
    /// Native volume serial for the durable container root.
    pub volume_serial: u64,
    /// Canonical hexadecimal file identity.
    pub file_id_hex: String,
    /// Committed generation UUID at summary time.
    pub generation_uuid: Uuid,
}

/// One metadata-only Project summary for discovery or inspection.
#[derive(Debug, Clone, PartialEq)]
pub struct ResearchProjectSummary {
    /// Caller-supplied local path to the durable container root.
    pub project_path: PathBuf,
    /// Stable Project identity and current generation.
    pub identity: ResearchProjectIdentity,
    /// Authoritative metadata record.
    pub metadata: WorkspaceResearchMetadata,
}

/// Structured discovery filters over caller-supplied local Projects.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResearchProjectDiscoveryQuery {
    /// Optional case-insensitive substring across title, description, tags, and subjects.
    pub free_text: Option<String>,
    /// Require every listed language label.
    pub languages: Vec<String>,
    /// Require every listed subject label.
    pub subjects: Vec<String>,
    /// Require every listed ontology label.
    pub ontologies: Vec<String>,
    /// Require every listed source-type label.
    pub source_types: Vec<String>,
    /// Optional temporal label substring match.
    pub temporal_label: Option<String>,
}

/// Resource bounds for bounded local discovery.
#[derive(Clone, Copy, Debug)]
pub struct ResearchProjectDiscoveryLimits {
    /// Maximum summaries returned.
    pub max_projects: usize,
    /// Maximum candidate roots inspected.
    pub max_candidates: usize,
}

impl Default for ResearchProjectDiscoveryLimits {
    fn default() -> Self {
        Self {
            max_projects: 1_024,
            max_candidates: 1_024,
        }
    }
}

impl WorkspaceResearchMetadata {
    /// Construct empty metadata for a new Project.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            contract_version: WORKSPACE_RESEARCH_METADATA_VERSION,
            title: None,
            description: None,
            authors: Vec::new(),
            subjects: Vec::new(),
            languages: Vec::new(),
            geographic_coverage: None,
            temporal_coverage: None,
            source_types: Vec::new(),
            corpus_size: None,
            ontologies: Vec::new(),
            license: None,
            access: ResearchAccessPolicyMetadata {
                visibility: None,
                access_policy: None,
                collaborators: Vec::new(),
            },
            tags: Vec::new(),
            originating_projects: Vec::new(),
            related_projects: Vec::new(),
            canonical_identifiers: Vec::new(),
            external_identifiers: Vec::new(),
            created_at: None,
            updated_at: None,
            extensions: BTreeMap::new(),
            discovery_facets: ResearchDiscoveryFacets::default(),
        }
    }

    /// Validate and return canonical JSON plus LF.
    ///
    /// # Errors
    /// Returns `GF_PROJECT_CORRUPT` for invalid metadata.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, GfError> {
        validate_research_metadata(self)?;
        canonical_json(self, "workspace research metadata")
    }

    /// Parse exact canonical JSON plus LF.
    ///
    /// # Errors
    /// Returns `GF_PROJECT_CORRUPT` for future, malformed, or noncanonical data.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, GfError> {
        if bytes.len() > MAX_WORKSPACE_RESEARCH_METADATA_BYTES {
            return Err(corrupt("workspace research metadata exceeds size limit"));
        }
        parse_canonical_json(
            bytes,
            "workspace research metadata",
            validate_research_metadata,
        )
    }

    /// Encode this record as one registered workspace participant.
    ///
    /// # Errors
    /// Returns a structured error when metadata violates its contract.
    pub fn to_project_participant(&self) -> Result<ProjectParticipant, GfError> {
        Ok(participant(
            WORKSPACE_RESEARCH_METADATA_FAMILY,
            self.to_canonical_json()?,
        ))
    }
}

/// Read authoritative research metadata from one committed generation.
///
/// Absence returns the empty contract rather than inferring defaults from graph data.
///
/// # Errors
/// Returns a structured project error when the participant is corrupt.
pub fn read_workspace_research_metadata(
    generation: &crate::ResolvedProjectGeneration,
) -> Result<WorkspaceResearchMetadata, GfError> {
    match generation
        .participant_snapshot(WORKSPACE_CAPABILITY_ID, WORKSPACE_RESEARCH_METADATA_FAMILY)?
    {
        Some(snapshot) => WorkspaceResearchMetadata::from_canonical_json(&snapshot.bytes),
        None => Ok(WorkspaceResearchMetadata::empty()),
    }
}

/// Build one metadata-only summary for a durable Project root.
///
/// # Errors
/// Returns project-format errors for unsupported roots and structured errors for corrupt metadata.
pub fn summarize_research_project(
    project_path: impl AsRef<Path>,
) -> Result<ResearchProjectSummary, GfError> {
    let project_path = project_path.as_ref();
    let generation = crate::resolve_project_generation(project_path)?;
    let metadata = read_workspace_research_metadata(&generation)?;
    let identity = project_identity(project_path, generation.generation_uuid())?;
    Ok(ResearchProjectSummary {
        project_path: project_path.to_path_buf(),
        identity,
        metadata,
    })
}

/// Discover Projects from caller-supplied local roots without opening graph payloads.
///
/// Non-Project roots and uninitialized Projects are skipped. Results are sorted by
/// canonical project path for deterministic pagination.
///
/// # Errors
/// Returns structured validation errors for invalid query bounds.
pub fn discover_research_projects(
    roots: &[PathBuf],
    query: &ResearchProjectDiscoveryQuery,
    limits: ResearchProjectDiscoveryLimits,
) -> Result<Vec<ResearchProjectSummary>, GfError> {
    validate_discovery_limits(limits)?;
    validate_discovery_query(query)?;
    let mut summaries = Vec::new();
    for (inspected, root) in roots.iter().enumerate() {
        if inspected >= limits.max_candidates {
            break;
        }
        let Ok(summary) = summarize_research_project(root) else {
            continue;
        };
        if matches_discovery_query(&summary.metadata, query) {
            summaries.push(summary);
            if summaries.len() >= limits.max_projects {
                break;
            }
        }
    }
    summaries.sort_by(|left, right| left.project_path.cmp(&right.project_path));
    Ok(summaries)
}

fn project_identity(
    project_path: &Path,
    generation_uuid: Uuid,
) -> Result<ResearchProjectIdentity, GfError> {
    let file = std::fs::File::open(project_path).map_err(|_| {
        project_error(
            ProjectErrorCode::UnsupportedProjectFormat,
            "project root is inaccessible",
        )
    })?;
    let identity = graphforge_filesystem::file_identity(&file).map_err(|_| {
        project_error(
            ProjectErrorCode::UnsupportedProjectFormat,
            "project root identity is unavailable",
        )
    })?;
    Ok(ResearchProjectIdentity {
        volume_serial: identity.volume_serial,
        file_id_hex: encode_identity_hex(&identity),
        generation_uuid,
    })
}

fn encode_identity_hex(identity: &FileIdentity) -> String {
    let mut encoded = String::with_capacity(32);
    for byte in identity.file_id {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn validate_discovery_limits(limits: ResearchProjectDiscoveryLimits) -> Result<(), GfError> {
    if limits.max_projects == 0 || limits.max_candidates == 0 {
        return Err(validation("discovery limits must be positive"));
    }
    Ok(())
}

fn validate_discovery_query(query: &ResearchProjectDiscoveryQuery) -> Result<(), GfError> {
    validate_optional_string(query.free_text.as_deref(), "free-text filter")?;
    validate_optional_string(query.temporal_label.as_deref(), "temporal filter")?;
    validate_string_list(&query.languages, "language filter")?;
    validate_string_list(&query.subjects, "subject filter")?;
    validate_string_list(&query.ontologies, "ontology filter")?;
    validate_string_list(&query.source_types, "source-type filter")?;
    Ok(())
}

fn matches_discovery_query(
    metadata: &WorkspaceResearchMetadata,
    query: &ResearchProjectDiscoveryQuery,
) -> bool {
    if let Some(free_text) = query.free_text.as_deref() {
        let needle = free_text.to_ascii_lowercase();
        if !contains_needle(metadata.title.as_deref(), &needle)
            && !contains_needle(metadata.description.as_deref(), &needle)
            && !metadata
                .tags
                .iter()
                .any(|tag| contains_needle(Some(tag), &needle))
            && !metadata
                .subjects
                .iter()
                .any(|subject| contains_needle(Some(subject), &needle))
        {
            return false;
        }
    }
    if !contains_all(&metadata.languages, &query.languages) {
        return false;
    }
    if !contains_all(&metadata.subjects, &query.subjects) {
        return false;
    }
    if !contains_all(&metadata.ontologies, &query.ontologies) {
        return false;
    }
    if !contains_all(&metadata.source_types, &query.source_types) {
        return false;
    }
    if let Some(temporal_label) = query.temporal_label.as_deref() {
        let needle = temporal_label.to_ascii_lowercase();
        let temporal = metadata.temporal_coverage.as_ref();
        let matches = temporal
            .and_then(|coverage| coverage.label.as_deref())
            .is_some_and(|label| contains_needle(Some(label), &needle))
            || temporal
                .and_then(|coverage| coverage.start.as_deref())
                .is_some_and(|start| contains_needle(Some(start), &needle))
            || temporal
                .and_then(|coverage| coverage.end.as_deref())
                .is_some_and(|end| contains_needle(Some(end), &needle));
        if !matches {
            return false;
        }
    }
    true
}

fn contains_needle(value: Option<&str>, needle: &str) -> bool {
    value.is_some_and(|value| value.to_ascii_lowercase().contains(needle))
}

fn contains_all(haystack: &[String], needles: &[String]) -> bool {
    needles
        .iter()
        .all(|needle| haystack.iter().any(|value| value == needle))
}

fn validate_research_metadata(record: &WorkspaceResearchMetadata) -> Result<(), GfError> {
    if record.contract_version != WORKSPACE_RESEARCH_METADATA_VERSION {
        return Err(corrupt("unsupported workspace research metadata contract"));
    }
    validate_optional_string(record.title.as_deref(), "title")?;
    validate_optional_string(record.description.as_deref(), "description")?;
    validate_string_list(&record.authors, "authors")?;
    validate_string_list(&record.subjects, "subjects")?;
    validate_string_list(&record.languages, "languages")?;
    validate_string_list(&record.source_types, "source types")?;
    validate_string_list(&record.ontologies, "ontologies")?;
    validate_optional_string(record.license.as_deref(), "license")?;
    validate_string_list(&record.tags, "tags")?;
    validate_string_list(&record.originating_projects, "originating projects")?;
    validate_string_list(&record.related_projects, "related projects")?;
    validate_string_list(&record.canonical_identifiers, "canonical identifiers")?;
    validate_string_list(&record.external_identifiers, "external identifiers")?;
    validate_optional_string(record.created_at.as_deref(), "created_at")?;
    validate_optional_string(record.updated_at.as_deref(), "updated_at")?;
    if let Some(geographic) = &record.geographic_coverage {
        validate_optional_string(geographic.label.as_deref(), "geographic label")?;
        validate_string_list(&geographic.regions, "geographic regions")?;
    }
    if let Some(temporal) = &record.temporal_coverage {
        validate_optional_string(temporal.start.as_deref(), "temporal start")?;
        validate_optional_string(temporal.end.as_deref(), "temporal end")?;
        validate_optional_string(temporal.label.as_deref(), "temporal label")?;
    }
    validate_optional_string(record.access.visibility.as_deref(), "visibility")?;
    validate_optional_string(record.access.access_policy.as_deref(), "access policy")?;
    validate_string_list(&record.access.collaborators, "collaborators")?;
    if record.extensions.len() > MAX_RESEARCH_METADATA_EXTENSION_FIELDS {
        return Err(corrupt("research metadata extensions exceed limit"));
    }
    for value in record.extensions.values() {
        if !value.is_string() && !value.is_number() && !value.is_boolean() && !value.is_null() {
            return Err(corrupt(
                "research metadata extension must be a scalar JSON value",
            ));
        }
    }
    Ok(())
}

fn validate_optional_string(value: Option<&str>, name: &str) -> Result<(), GfError> {
    if let Some(value) = value {
        validate_bounded_string(value, name)?;
    }
    Ok(())
}

fn validate_string_list(values: &[String], name: &str) -> Result<(), GfError> {
    if values.len() > MAX_RESEARCH_METADATA_LIST_ENTRIES {
        return Err(corrupt(format!("{name} exceeds entry limit")));
    }
    let mut prior: Option<&str> = None;
    for value in values {
        validate_bounded_string(value, name)?;
        if prior.is_some_and(|prior| prior >= value.as_str()) {
            return Err(corrupt(format!("{name} are not in canonical order")));
        }
        prior = Some(value);
    }
    Ok(())
}

fn validate_bounded_string(value: &str, name: &str) -> Result<(), GfError> {
    if value.is_empty() || value.len() > MAX_RESEARCH_METADATA_STRING_BYTES {
        return Err(corrupt(format!("{name} exceeds UTF-8 bounds")));
    }
    if value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(corrupt(format!("{name} contains control characters")));
    }
    Ok(())
}

fn participant(family: &str, bytes: Vec<u8>) -> ProjectParticipant {
    ProjectParticipant {
        capability_id: WORKSPACE_CAPABILITY_ID.into(),
        capability_version: WORKSPACE_CAPABILITY_VERSION,
        record_family_id: family.into(),
        record_version: WORKSPACE_RESEARCH_METADATA_VERSION,
        encoding: ProjectParticipantEncoding::Json,
        schema_fingerprint: Sha256::digest(format!("workspace/{family}@1")).into(),
        row_count: 1,
        bytes,
    }
}

fn canonical_json<T: Serialize>(value: &T, label: &str) -> Result<Vec<u8>, GfError> {
    let mut bytes = serde_json::to_vec(value).map_err(|error| corrupt(error.to_string()))?;
    if bytes.last() != Some(&b'\n') {
        bytes.push(b'\n');
    }
    if bytes.len() > MAX_WORKSPACE_RESEARCH_METADATA_BYTES {
        return Err(corrupt(format!("{label} exceeds size limit")));
    }
    Ok(bytes)
}

fn parse_canonical_json<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    label: &str,
    validate: impl Fn(&T) -> Result<(), GfError>,
) -> Result<T, GfError> {
    if !bytes.ends_with(b"\n") {
        return Err(corrupt(format!("{label} is not canonical JSON plus LF")));
    }
    let value =
        serde_json::from_slice(bytes).map_err(|_| corrupt(format!("{label} is malformed")))?;
    validate(&value)?;
    Ok(value)
}

fn corrupt(message: impl Into<String>) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ProjectCorrupt,
        message: message.into(),
    }
}

fn validation(message: impl Into<String>) -> GfError {
    GfError::Validation(message.into())
}

fn project_error(code: ProjectErrorCode, message: impl Into<String>) -> GfError {
    GfError::Project {
        code,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn discovery_query_matches_structured_filters_in_memory() {
        let metadata = WorkspaceResearchMetadata {
            contract_version: WORKSPACE_RESEARCH_METADATA_VERSION,
            title: Some("Arabian Nights".into()),
            description: None,
            authors: Vec::new(),
            subjects: vec!["literature".into()],
            languages: vec!["ar".into()],
            geographic_coverage: None,
            temporal_coverage: Some(ResearchTemporalCoverage {
                start: Some("800".into()),
                end: Some("1500".into()),
                label: Some("800-1500 CE".into()),
            }),
            source_types: Vec::new(),
            corpus_size: None,
            ontologies: vec!["narrative-events".into()],
            license: None,
            access: ResearchAccessPolicyMetadata {
                visibility: None,
                access_policy: None,
                collaborators: Vec::new(),
            },
            tags: Vec::new(),
            originating_projects: Vec::new(),
            related_projects: Vec::new(),
            canonical_identifiers: Vec::new(),
            external_identifiers: Vec::new(),
            created_at: None,
            updated_at: None,
            extensions: BTreeMap::new(),
            discovery_facets: ResearchDiscoveryFacets::default(),
        };
        assert!(matches_discovery_query(
            &metadata,
            &ResearchProjectDiscoveryQuery {
                languages: vec!["ar".into()],
                subjects: vec!["literature".into()],
                ontologies: vec!["narrative-events".into()],
                temporal_label: Some("800-1500".into()),
                ..ResearchProjectDiscoveryQuery::default()
            },
        ));
        assert!(!matches_discovery_query(
            &metadata,
            &ResearchProjectDiscoveryQuery {
                languages: vec!["la".into()],
                ..ResearchProjectDiscoveryQuery::default()
            },
        ));
    }

    #[test]
    fn empty_metadata_round_trips_canonically() {
        let metadata = WorkspaceResearchMetadata::empty();
        let bytes = metadata.to_canonical_json().unwrap();
        let decoded = WorkspaceResearchMetadata::from_canonical_json(&bytes).unwrap();
        assert_eq!(decoded, metadata);
    }

    fn open_admitted_project(root: &Path) -> Option<crate::ResolvedProjectGeneration> {
        match crate::open_or_initialize_project(root) {
            Ok(generation) => Some(generation),
            Err(GfError::Project {
                code: ProjectErrorCode::UnsupportedFilesystem,
                ..
            }) => None,
            Err(error) => panic!("{error}"),
        }
    }

    #[test]
    fn discovery_filters_language_and_subject_without_graph_open() {
        let first_root = TempDir::new().unwrap();
        let second_root = TempDir::new().unwrap();
        if open_admitted_project(first_root.path()).is_none()
            || open_admitted_project(second_root.path()).is_none()
        {
            return;
        }
        let first = crate::open_or_initialize_project(first_root.path()).unwrap();
        let second = crate::open_or_initialize_project(second_root.path()).unwrap();
        drop(first);
        drop(second);

        let mut arabic = WorkspaceResearchMetadata::empty();
        arabic.title = Some("Arabian Nights".into());
        arabic.languages = vec!["ar".into()];
        arabic.subjects = vec!["literature".into()];
        arabic.temporal_coverage = Some(ResearchTemporalCoverage {
            start: Some("800".into()),
            end: Some("1500".into()),
            label: Some("800-1500 CE".into()),
        });
        publish_metadata(first_root.path(), &arabic);

        let mut latin = WorkspaceResearchMetadata::empty();
        latin.title = Some("Medieval Latin Corpus".into());
        latin.languages = vec!["la".into()];
        latin.subjects = vec!["history".into()];
        publish_metadata(second_root.path(), &latin);

        let summaries = discover_research_projects(
            &[
                first_root.path().to_path_buf(),
                second_root.path().to_path_buf(),
            ],
            &ResearchProjectDiscoveryQuery {
                languages: vec!["ar".into()],
                subjects: vec!["literature".into()],
                ..ResearchProjectDiscoveryQuery::default()
            },
            ResearchProjectDiscoveryLimits::default(),
        )
        .unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(
            summaries[0].metadata.title.as_deref(),
            Some("Arabian Nights")
        );
        assert!(summaries[0].metadata.languages.contains(&"ar".to_string()));
    }

    fn publish_metadata(root: &Path, metadata: &WorkspaceResearchMetadata) {
        let current = crate::resolve_project_generation(root).unwrap();
        let mut participants = current
            .participant_snapshots()
            .unwrap()
            .into_iter()
            .filter(|snapshot| {
                !(snapshot.capability_id == WORKSPACE_CAPABILITY_ID
                    && snapshot.record_family_id == WORKSPACE_RESEARCH_METADATA_FAMILY)
            })
            .map(|snapshot| crate::ProjectParticipant {
                capability_id: snapshot.capability_id,
                capability_version: snapshot.capability_version,
                record_family_id: snapshot.record_family_id,
                record_version: snapshot.record_version,
                encoding: match snapshot.encoding.as_str() {
                    "json" => ProjectParticipantEncoding::Json,
                    "arrow" => ProjectParticipantEncoding::Arrow,
                    "parquet" => ProjectParticipantEncoding::Parquet,
                    _ => panic!("unsupported encoding"),
                },
                schema_fingerprint: snapshot.schema_fingerprint,
                row_count: snapshot.row_count,
                bytes: snapshot.bytes,
            })
            .collect::<Vec<_>>();
        participants.push(metadata.to_project_participant().unwrap());
        participants.sort_by(|left, right| {
            (&left.capability_id, &left.record_family_id)
                .cmp(&(&right.capability_id, &right.record_family_id))
        });
        let request = crate::ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: current
                .capabilities()
                .into_iter()
                .map(|capability| crate::ProjectCapability {
                    capability_id: capability.capability_id,
                    capability_version: capability.capability_version,
                })
                .collect(),
            participants,
        };
        let staged = crate::stage_project_generation(root, &request).unwrap();
        if let crate::ProjectStageOutcome::Staged(staged) = staged {
            staged
                .validate(|_| Ok(()), |_, _| Ok(()))
                .unwrap()
                .publish()
                .unwrap();
        }
    }
}
