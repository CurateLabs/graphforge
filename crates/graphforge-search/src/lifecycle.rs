//! Freshness-aware, atomic lifecycle for derived Tantivy text indexes.

use std::cell::{Cell, RefCell};
use std::path::{Path, PathBuf};

use graphforge_storage::{
    PublishedSearchArtifact, SearchArtifactError, SearchArtifactKey, SearchCoordinationLimits,
    SearchPublicationMode, SearchPublicationOutcome, SearchPublicationPlan, SearchSourcePart,
    SearchSourceSnapshot, coordinate_search_publication,
};

use crate::TextSearchLimits;
use crate::analyzer::{TEXT_CONTRACT_VERSION, analyze_query};
use crate::source::{TextSourceProjection, project_text_source};
use crate::text_index::{
    TEXT_BACKEND_VERSION, TextIndexBuildOutcome, TextSearchHit, build_text_index,
    search_text_index, validate_text_index,
};

const EMPTY_MARKER_FILE: &str = "empty-text-v1.marker";
const EMPTY_MARKER_BYTES: &[u8] = b"graphforge-empty-text-v1\n";
const MANIFEST_FILE: &str = "manifest.json";

/// Combined bounds for text backend work and shared publication coordination.
#[derive(Clone, Copy, Debug, Default)]
pub struct TextLifecycleLimits {
    /// Projection, Tantivy build/search, and source-snapshot bounds.
    pub text: TextSearchLimits,
    /// Per-key lock wait and abandoned-build cleanup bounds.
    pub coordination: SearchCoordinationLimits,
}

/// Caller-resolved identity and explicit property set for one text artifact.
#[derive(Clone, Copy, Debug)]
pub struct TextIndexRequest<'a> {
    /// Normalized graph label persisted in the artifact key.
    pub label: &'a str,
    /// Local catalog identity used only for Parquet membership projection.
    pub label_id: u32,
    /// Explicit non-empty property selectors persisted in canonical order.
    pub properties: &'a [String],
}

/// Caller-resolved identity for lazy search over the stable default projection.
#[derive(Clone, Copy, Debug)]
pub struct LazyTextRequest<'a> {
    /// Normalized graph label persisted in the discovered artifact key.
    pub label: &'a str,
    /// Local catalog identity used only for Parquet membership projection.
    pub label_id: u32,
}

/// One verified, immutable text publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishedTextIndex {
    /// The requested label has no matching UUID documents.
    Empty(PublishedSearchArtifact),
    /// A complete Tantivy directory is ready for search.
    Tantivy(PublishedSearchArtifact),
}

/// Result of stable default-property discovery and text preparation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TextIndexPreparation {
    /// The stable label projection contains no observed string properties.
    NoTextProperties,
    /// An exact discovered-property artifact was reused or published.
    Published(PublishedTextIndex),
}

/// Public, bounded freshness state for one text-index projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextIndexFreshnessState {
    /// No artifact has been published for the resolved projection.
    Missing,
    /// The published artifact exactly matches the committed source.
    Current,
    /// The source generation or content fingerprint has changed.
    Stale,
    /// The artifact cannot be consumed by this binary's contract.
    Incompatible,
}

impl TextIndexFreshnessState {
    /// Stable binding spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Current => "current",
            Self::Stale => "stale",
            Self::Incompatible => "incompatible",
        }
    }
}

/// Stable reason vocabulary for non-current text-index state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextIndexFreshnessReason {
    /// The resolved projection has no string properties.
    NoTextProperties,
    /// No publication pointer exists for the projection.
    NotBuilt,
    /// The committed search generation changed.
    SourceGenerationChanged,
    /// Committed source bytes changed without a generation match.
    SourceFingerprintChanged,
    /// The persisted manifest version is unsupported.
    ManifestVersion,
    /// The persisted backend version is unsupported.
    BackendVersion,
    /// The persisted text contract version is unsupported.
    ContractVersion,
    /// The persisted label/property selector does not match its key.
    ArtifactSelector,
}

impl TextIndexFreshnessReason {
    /// Stable binding spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoTextProperties => "no_text_properties",
            Self::NotBuilt => "not_built",
            Self::SourceGenerationChanged => "source_generation_changed",
            Self::SourceFingerprintChanged => "source_fingerprint_changed",
            Self::ManifestVersion => "manifest_version",
            Self::BackendVersion => "backend_version",
            Self::ContractVersion => "contract_version",
            Self::ArtifactSelector => "artifact_selector",
        }
    }
}

/// Canonical Rust-owned inspection of one text-index projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextIndexFreshnessInspection {
    /// Canonically ordered indexed properties.
    pub properties: Vec<String>,
    /// Current committed search-source generation.
    pub source_generation: u64,
    /// Current canonical committed-source fingerprint.
    pub source_fingerprint: String,
    /// Immutable publication generation name, when readable.
    pub artifact_generation: Option<String>,
    /// Source generation recorded by the artifact, when readable.
    pub artifact_source_generation: Option<u64>,
    /// Source fingerprint recorded by the artifact, when readable.
    pub artifact_source_fingerprint: Option<String>,
    /// Bounded freshness state.
    pub state: TextIndexFreshnessState,
    /// Bounded reason for a non-current state.
    pub reason: Option<TextIndexFreshnessReason>,
}

/// Inspect one explicit or default-discovered text projection without building it.
///
/// # Errors
/// Returns structured source, selector, corruption, cancellation, resource, or
/// repeated-concurrent-mutation errors. Unsupported manifest versions are
/// projected as `Incompatible`, not served as current.
pub fn inspect_text_index_freshness<C>(
    project_dir: &Path,
    request: LazyTextRequest<'_>,
    explicit_properties: Option<&[String]>,
    limits: TextLifecycleLimits,
    mut checkpoint: C,
) -> Result<TextIndexFreshnessInspection, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let mut retry = true;
    loop {
        let before = capture_text_snapshot(project_dir, limits.text, &mut checkpoint)?;
        let projection = project_text_source(
            project_dir,
            request.label_id,
            explicit_properties,
            limits.text,
            &mut checkpoint,
        )?;
        if explicit_properties.is_some() {
            validate_observed_properties(&projection)?;
        }
        let after = capture_text_snapshot(project_dir, limits.text, &mut checkpoint)?;
        if before != after {
            if !retry {
                return Err(SearchArtifactError::ConcurrentMutation);
            }
            retry = false;
            continue;
        }
        let properties = explicit_properties
            .map(<[String]>::to_vec)
            .unwrap_or(projection.properties);
        if properties.is_empty() {
            return Ok(TextIndexFreshnessInspection {
                properties,
                source_generation: after.generation,
                source_fingerprint: after.fingerprint,
                artifact_generation: None,
                artifact_source_generation: None,
                artifact_source_fingerprint: None,
                state: TextIndexFreshnessState::Missing,
                reason: Some(TextIndexFreshnessReason::NoTextProperties),
            });
        }
        let key = SearchArtifactKey::text(request.label, &properties)?;
        return inspect_published_text(project_dir, &key, after);
    }
}

fn inspect_published_text(
    project_dir: &Path,
    key: &SearchArtifactKey,
    source: SearchSourceSnapshot,
) -> Result<TextIndexFreshnessInspection, SearchArtifactError> {
    let artifact = match graphforge_storage::current_search_artifact(project_dir, key) {
        Ok(Some(artifact)) => artifact,
        Ok(None) | Err(SearchArtifactError::Missing { .. }) => {
            return Ok(TextIndexFreshnessInspection {
                properties: key.properties().unwrap_or_default().to_vec(),
                source_generation: source.generation,
                source_fingerprint: source.fingerprint,
                artifact_generation: None,
                artifact_source_generation: None,
                artifact_source_fingerprint: None,
                state: TextIndexFreshnessState::Missing,
                reason: Some(TextIndexFreshnessReason::NotBuilt),
            });
        }
        Err(SearchArtifactError::IncompatibleManifest { path, .. }) => {
            return Ok(TextIndexFreshnessInspection {
                properties: key.properties().unwrap_or_default().to_vec(),
                source_generation: source.generation,
                source_fingerprint: source.fingerprint,
                artifact_generation: path
                    .parent()
                    .and_then(Path::file_name)
                    .and_then(|name| name.to_str())
                    .map(ToOwned::to_owned),
                artifact_source_generation: None,
                artifact_source_fingerprint: None,
                state: TextIndexFreshnessState::Incompatible,
                reason: Some(TextIndexFreshnessReason::ManifestVersion),
            });
        }
        Err(error) => return Err(error),
    };
    let manifest = &artifact.manifest;
    let (state, reason) =
        if manifest.label != key.label() || manifest.properties.as_deref() != key.properties() {
            (
                TextIndexFreshnessState::Incompatible,
                Some(TextIndexFreshnessReason::ArtifactSelector),
            )
        } else if manifest.backend_version != TEXT_BACKEND_VERSION {
            (
                TextIndexFreshnessState::Incompatible,
                Some(TextIndexFreshnessReason::BackendVersion),
            )
        } else if manifest.contract_version != TEXT_CONTRACT_VERSION {
            (
                TextIndexFreshnessState::Incompatible,
                Some(TextIndexFreshnessReason::ContractVersion),
            )
        } else if manifest.source_generation != source.generation {
            (
                TextIndexFreshnessState::Stale,
                Some(TextIndexFreshnessReason::SourceGenerationChanged),
            )
        } else if manifest.source_fingerprint != source.fingerprint {
            (
                TextIndexFreshnessState::Stale,
                Some(TextIndexFreshnessReason::SourceFingerprintChanged),
            )
        } else {
            (TextIndexFreshnessState::Current, None)
        };
    Ok(TextIndexFreshnessInspection {
        properties: key.properties().unwrap_or_default().to_vec(),
        source_generation: source.generation,
        source_fingerprint: source.fingerprint,
        artifact_generation: artifact
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .map(ToOwned::to_owned),
        artifact_source_generation: Some(manifest.source_generation),
        artifact_source_fingerprint: Some(manifest.source_fingerprint.clone()),
        state,
        reason,
    })
}

impl PublishedTextIndex {
    /// Shared manifest and immutable publication directory.
    #[must_use]
    pub const fn artifact(&self) -> &PublishedSearchArtifact {
        match self {
            Self::Empty(artifact) | Self::Tantivy(artifact) => artifact,
        }
    }

    /// Whether this publication represents the stable empty result.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        matches!(self, Self::Empty(_))
    }
}

/// Publish or reuse one verified text index for a caller-resolved label.
///
/// The label name and explicit property set form the persisted artifact key;
/// `label_id` remains a local graph-catalog input and never crosses the index
/// boundary. Builds read graph-native topology and node-property Parquet only,
/// run inside the shared coordinator's private directory, and become visible
/// only through its atomic completed-manifest publication.
///
/// # Errors
/// Returns structured selector, source, corruption, cancellation, resource,
/// lock, build, I/O, or repeated-concurrent-mutation errors.
pub fn prepare_text_index<C>(
    project_dir: &Path,
    request: TextIndexRequest<'_>,
    mode: SearchPublicationMode,
    limits: TextLifecycleLimits,
    checkpoint: C,
) -> Result<PublishedTextIndex, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    prepare_text_index_with_budget(
        project_dir,
        request,
        mode,
        limits,
        checkpoint,
        TextPreparationPolicy::default(),
    )
}

/// Discover a stable default string-property set and prepare its exact index.
///
/// A label with no observed string properties returns `NoTextProperties`
/// without publishing a placeholder key. Discovery, publication, and the
/// final freshness check share one bounded retry.
///
/// # Errors
/// Returns structured source, corruption, cancellation, resource, lock,
/// build, I/O, or repeated-concurrent-mutation errors.
pub fn prepare_default_text_index<C>(
    project_dir: &Path,
    request: LazyTextRequest<'_>,
    mode: SearchPublicationMode,
    limits: TextLifecycleLimits,
    checkpoint: C,
) -> Result<TextIndexPreparation, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    prepare_stable_text_index(project_dir, request, None, mode, limits, checkpoint)
}

/// Validate explicit string properties and prepare their exact text index.
///
/// Empty or normalized-duplicate selections are invalid. Every selected
/// property must be observed as a string on at least one currently eligible
/// UUID in the same stable graph snapshot used for publication.
///
/// # Errors
/// Returns structured selector, source, corruption, cancellation, resource,
/// lock, build, I/O, or repeated-concurrent-mutation errors.
pub fn prepare_explicit_text_index<C>(
    project_dir: &Path,
    request: TextIndexRequest<'_>,
    mode: SearchPublicationMode,
    limits: TextLifecycleLimits,
    checkpoint: C,
) -> Result<PublishedTextIndex, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let key = SearchArtifactKey::text(request.label, request.properties)?;
    let properties = key
        .properties()
        .expect("text artifact keys always contain properties");
    if properties.len() != request.properties.len() {
        return Err(invalid(
            "properties",
            "normalized property names must not contain duplicates",
        ));
    }
    match prepare_stable_text_index(
        project_dir,
        LazyTextRequest {
            label: key.label(),
            label_id: request.label_id,
        },
        Some(properties),
        mode,
        limits,
        checkpoint,
    )? {
        TextIndexPreparation::Published(index) => Ok(index),
        TextIndexPreparation::NoTextProperties => {
            unreachable!("explicit property validation rejects a no-text projection")
        }
    }
}

fn prepare_stable_text_index<C>(
    project_dir: &Path,
    request: LazyTextRequest<'_>,
    explicit_properties: Option<&[String]>,
    mode: SearchPublicationMode,
    limits: TextLifecycleLimits,
    mut checkpoint: C,
) -> Result<TextIndexPreparation, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let label_probe = SearchArtifactKey::text(request.label, ["_"])?;
    let label = label_probe.label().to_owned();
    let explicit_properties = explicit_properties.map(<[String]>::to_vec);
    let retry_budget = Cell::new(true);

    loop {
        let before = capture_text_snapshot(project_dir, limits.text, &mut checkpoint)?;
        let projection = project_text_source(
            project_dir,
            request.label_id,
            explicit_properties.as_deref(),
            limits.text,
            &mut checkpoint,
        )?;
        if explicit_properties.is_some() {
            validate_observed_properties(&projection)?;
        }
        let discovered = capture_text_snapshot(project_dir, limits.text, &mut checkpoint)?;
        if before != discovered {
            consume_retry(Some(&retry_budget))?;
            continue;
        }
        let properties = explicit_properties.clone().unwrap_or(projection.properties);
        if properties.is_empty() {
            return Ok(TextIndexPreparation::NoTextProperties);
        }
        let key = SearchArtifactKey::text(&label, &properties)?;
        match prepare_text_index_with_budget(
            project_dir,
            TextIndexRequest {
                label: key.label(),
                label_id: request.label_id,
                properties: &properties,
            },
            mode,
            limits,
            &mut checkpoint,
            TextPreparationPolicy {
                retry_budget: Some(&retry_budget),
                expected_snapshot: Some(&discovered),
                require_observed_properties: explicit_properties.is_some(),
            },
        ) {
            Ok(index) => {
                let after = capture_text_snapshot(project_dir, limits.text, &mut checkpoint)?;
                match index.artifact().manifest.verify_fresh(
                    &key,
                    TEXT_BACKEND_VERSION,
                    TEXT_CONTRACT_VERSION,
                    None,
                    &after,
                ) {
                    Ok(()) if after == discovered => {
                        return Ok(TextIndexPreparation::Published(index));
                    }
                    Ok(()) | Err(SearchArtifactError::Stale { .. }) => {
                        consume_retry(Some(&retry_budget))?;
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(SearchArtifactError::Stale { .. }) => consume_retry(Some(&retry_budget))?,
            Err(error) => return Err(error),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct TextPreparationPolicy<'a> {
    retry_budget: Option<&'a Cell<bool>>,
    expected_snapshot: Option<&'a SearchSourceSnapshot>,
    require_observed_properties: bool,
}

fn prepare_text_index_with_budget<C>(
    project_dir: &Path,
    request: TextIndexRequest<'_>,
    mode: SearchPublicationMode,
    limits: TextLifecycleLimits,
    checkpoint: C,
    policy: TextPreparationPolicy<'_>,
) -> Result<PublishedTextIndex, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let key = SearchArtifactKey::text(request.label, request.properties)?;
    let properties = key
        .properties()
        .expect("text artifact keys always contain properties")
        .to_vec();
    let checkpoint = RefCell::new(checkpoint);
    let build_calls = Cell::new(0_u8);
    let outcome = coordinate_search_publication(
        project_dir,
        SearchPublicationPlan {
            key: &key,
            backend_version: TEXT_BACKEND_VERSION,
            contract_version: TEXT_CONTRACT_VERSION,
            dimension: None,
            mode,
        },
        limits.coordination,
        || {
            let snapshot =
                capture_text_snapshot(project_dir, limits.text, || checkpoint.borrow_mut()())?;
            if policy
                .expected_snapshot
                .is_some_and(|expected| expected != &snapshot)
            {
                return Err(SearchArtifactError::Stale {
                    reason: "graph changed after text property discovery".to_owned(),
                });
            }
            Ok(snapshot)
        },
        |artifact| {
            inspect_text_artifact(artifact, &properties, limits.text, || {
                checkpoint.borrow_mut()()
            })
            .map(|_| ())
        },
        |build_dir, _snapshot| {
            if build_calls.replace(build_calls.get().saturating_add(1)) > 0 {
                consume_retry(policy.retry_budget)?;
            }
            let projection = project_text_source(
                project_dir,
                request.label_id,
                Some(&properties),
                limits.text,
                || checkpoint.borrow_mut()(),
            )?;
            if policy.require_observed_properties {
                validate_observed_properties(&projection)?;
            }
            match build_text_index(build_dir, &projection, limits.text, || {
                checkpoint.borrow_mut()()
            })? {
                TextIndexBuildOutcome::Empty => write_empty_marker(build_dir)?,
                TextIndexBuildOutcome::Built { .. } => {}
            }
            inspect_build(build_dir, &properties, limits.text, || {
                checkpoint.borrow_mut()()
            })
        },
        || checkpoint.borrow_mut()(),
    )?;
    let artifact = match outcome {
        SearchPublicationOutcome::Reused(artifact)
        | SearchPublicationOutcome::Published { artifact, .. } => artifact,
    };
    inspect_text_artifact(&artifact, &properties, limits.text, || {
        checkpoint.borrow_mut()()
    })
}

/// Lazily reuse/build and search one explicit text artifact.
///
/// A post-search generation/fingerprint check prevents a supported graph
/// mutation racing the read from silently exposing older hits. One retry is
/// allowed; a second race returns `ConcurrentMutation`. Rebuildable corruption
/// discovered between publication validation and search receives the same one
/// bounded retry.
///
/// # Errors
/// Returns structured query, source, corruption, cancellation, resource,
/// lock, build, I/O, or repeated-concurrent-mutation errors. Partial hits are
/// never returned.
pub fn search_published_text<C>(
    project_dir: &Path,
    request: TextIndexRequest<'_>,
    query: &str,
    limit: usize,
    limits: TextLifecycleLimits,
    mut checkpoint: C,
) -> Result<Vec<TextSearchHit>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let key = SearchArtifactKey::text(request.label, request.properties)?;
    let properties = key
        .properties()
        .expect("text artifact keys always contain properties")
        .to_vec();
    validate_search_request(query, limit, limits.text)?;
    let retry_budget = Cell::new(true);
    loop {
        match search_text_attempt(
            project_dir,
            TextSearchAttemptRequest {
                index: TextIndexRequest {
                    label: key.label(),
                    label_id: request.label_id,
                    properties: &properties,
                },
                key: &key,
                query,
                limit,
                expected_snapshot: None,
                retry_budget: &retry_budget,
            },
            limits,
            &mut checkpoint,
        ) {
            Ok(TextSearchAttempt::Complete(hits)) => return Ok(hits),
            Ok(TextSearchAttempt::Retry) | Err(SearchArtifactError::CorruptDerivedIndex { .. }) => {
                consume_retry(Some(&retry_budget))?;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Discover, lazily publish, and search the stable default text projection.
///
/// Query and result bounds are validated before graph source work. The exact
/// sorted discovered property set forms the artifact key. Discovery, recovery,
/// and freshness share one two-attempt lifecycle budget. A stable projection
/// without strings returns empty without publishing an ambiguous artifact.
///
/// # Errors
/// Returns structured selector, query, source, corruption, cancellation,
/// resource, lock, build, I/O, or repeated-concurrent-mutation errors. Partial
/// hits are never returned.
pub fn search_default_text<C>(
    project_dir: &Path,
    request: LazyTextRequest<'_>,
    query: &str,
    limit: usize,
    limits: TextLifecycleLimits,
    mut checkpoint: C,
) -> Result<Vec<TextSearchHit>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    validate_search_request(query, limit, limits.text)?;

    let label_probe = SearchArtifactKey::text(request.label, ["_"])?;
    let label = label_probe.label().to_owned();
    let retry_budget = Cell::new(true);

    loop {
        let before = capture_text_snapshot(project_dir, limits.text, &mut checkpoint)?;
        let projection = project_text_source(
            project_dir,
            request.label_id,
            None,
            limits.text,
            &mut checkpoint,
        )?;
        let after_discovery = capture_text_snapshot(project_dir, limits.text, &mut checkpoint)?;
        if before != after_discovery {
            consume_retry(Some(&retry_budget))?;
            continue;
        }
        if projection.properties.is_empty() {
            return Ok(Vec::new());
        }

        let key = SearchArtifactKey::text(&label, &projection.properties)?;
        match search_text_attempt(
            project_dir,
            TextSearchAttemptRequest {
                index: TextIndexRequest {
                    label: key.label(),
                    label_id: request.label_id,
                    properties: &projection.properties,
                },
                key: &key,
                query,
                limit,
                expected_snapshot: Some(&after_discovery),
                retry_budget: &retry_budget,
            },
            limits,
            &mut checkpoint,
        ) {
            Ok(TextSearchAttempt::Complete(hits)) => return Ok(hits),
            Ok(TextSearchAttempt::Retry) | Err(SearchArtifactError::CorruptDerivedIndex { .. }) => {
                consume_retry(Some(&retry_budget))?;
            }
            Err(error) => return Err(error),
        }
    }
}

enum TextSearchAttempt {
    Complete(Vec<TextSearchHit>),
    Retry,
}

#[derive(Clone, Copy)]
struct TextSearchAttemptRequest<'a> {
    index: TextIndexRequest<'a>,
    key: &'a SearchArtifactKey,
    query: &'a str,
    limit: usize,
    expected_snapshot: Option<&'a SearchSourceSnapshot>,
    retry_budget: &'a Cell<bool>,
}

fn search_text_attempt<C>(
    project_dir: &Path,
    request: TextSearchAttemptRequest<'_>,
    limits: TextLifecycleLimits,
    checkpoint: &mut C,
) -> Result<TextSearchAttempt, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let properties = request
        .key
        .properties()
        .expect("text artifact keys always contain properties");
    let publication = prepare_text_index_with_budget(
        project_dir,
        request.index,
        SearchPublicationMode::ReuseFresh,
        limits,
        &mut *checkpoint,
        TextPreparationPolicy {
            retry_budget: Some(request.retry_budget),
            ..TextPreparationPolicy::default()
        },
    )?;
    let hits = match &publication {
        PublishedTextIndex::Empty(_) => Vec::new(),
        PublishedTextIndex::Tantivy(artifact) => search_text_index(
            &artifact.path,
            properties,
            request.query,
            request.limit,
            limits.text,
            &mut *checkpoint,
        )?,
    };
    let after = capture_text_snapshot(project_dir, limits.text, &mut *checkpoint)?;
    if request
        .expected_snapshot
        .is_some_and(|expected| expected != &after)
    {
        return Ok(TextSearchAttempt::Retry);
    }
    match publication.artifact().manifest.verify_fresh(
        request.key,
        TEXT_BACKEND_VERSION,
        TEXT_CONTRACT_VERSION,
        None,
        &after,
    ) {
        Ok(()) => Ok(TextSearchAttempt::Complete(hits)),
        Err(SearchArtifactError::Stale { .. }) => Ok(TextSearchAttempt::Retry),
        Err(error) => Err(error),
    }
}

fn validate_search_request(
    query: &str,
    limit: usize,
    limits: TextSearchLimits,
) -> Result<(), SearchArtifactError> {
    analyze_query(query, limits)?;
    if limit == 0 {
        return Err(invalid("limit", "must be greater than zero"));
    }
    if limit > limits.results {
        return Err(SearchArtifactError::ResourceExhausted {
            resource: "text_results",
            limit: u64::try_from(limits.results).unwrap_or(u64::MAX),
        });
    }
    Ok(())
}

fn consume_retry(retry_budget: Option<&Cell<bool>>) -> Result<(), SearchArtifactError> {
    if retry_budget.is_none_or(|budget| budget.replace(false)) {
        Ok(())
    } else {
        Err(SearchArtifactError::ConcurrentMutation)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TextArtifactKind {
    Empty,
    Tantivy,
}

fn inspect_build<C>(
    path: &Path,
    properties: &[String],
    limits: TextSearchLimits,
    checkpoint: C,
) -> Result<(), SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    inspect_text_path(path, properties, limits, checkpoint).map(|_| ())
}

fn inspect_text_artifact<C>(
    artifact: &PublishedSearchArtifact,
    properties: &[String],
    limits: TextSearchLimits,
    checkpoint: C,
) -> Result<PublishedTextIndex, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    match inspect_text_path(&artifact.path, properties, limits, checkpoint)? {
        TextArtifactKind::Empty => Ok(PublishedTextIndex::Empty(artifact.clone())),
        TextArtifactKind::Tantivy => Ok(PublishedTextIndex::Tantivy(artifact.clone())),
    }
}

fn inspect_text_path<C>(
    path: &Path,
    properties: &[String],
    limits: TextSearchLimits,
    mut checkpoint: C,
) -> Result<TextArtifactKind, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    let marker_path = path.join(EMPTY_MARKER_FILE);
    match std::fs::read(&marker_path) {
        Ok(bytes) => {
            if bytes != EMPTY_MARKER_BYTES {
                return Err(corrupt(path, "empty text marker has invalid contents"));
            }
            validate_empty_layout(path, &mut checkpoint)?;
            Ok(TextArtifactKind::Empty)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            validate_text_index(path, properties, limits, checkpoint)?;
            Ok(TextArtifactKind::Tantivy)
        }
        Err(source) => Err(SearchArtifactError::Io {
            operation: "read empty text marker",
            path: marker_path,
            source,
        }),
    }
}

fn write_empty_marker(path: &Path) -> Result<(), SearchArtifactError> {
    let marker = path.join(EMPTY_MARKER_FILE);
    std::fs::write(&marker, EMPTY_MARKER_BYTES).map_err(|source| SearchArtifactError::Io {
        operation: "write empty text marker",
        path: marker,
        source,
    })
}

fn validate_empty_layout<C>(path: &Path, checkpoint: &mut C) -> Result<(), SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let entries = std::fs::read_dir(path)
        .map_err(|error| corrupt(path, format!("read empty text artifact: {error}")))?;
    for entry in entries {
        checkpoint()?;
        let entry = entry.map_err(|error| corrupt(path, format!("read empty entry: {error}")))?;
        let file_type = entry
            .file_type()
            .map_err(|error| corrupt(path, format!("inspect empty entry: {error}")))?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| corrupt(path, "empty artifact contains a non-UTF-8 entry"))?;
        if !file_type.is_file() || !matches!(name, EMPTY_MARKER_FILE | MANIFEST_FILE) {
            return Err(corrupt(
                path,
                format!("empty artifact contains unexpected entry {name:?}"),
            ));
        }
    }
    Ok(())
}

fn capture_text_snapshot<C>(
    project_dir: &Path,
    limits: TextSearchLimits,
    mut checkpoint: C,
) -> Result<SearchSourceSnapshot, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    checkpoint()?;
    let mut paths = Vec::new();
    let topology = project_dir.join("topology").join("nodes.parquet");
    match std::fs::symlink_metadata(&topology) {
        Ok(_) => paths.push(("topology/nodes.parquet".to_owned(), topology)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(source(format!("inspect {}: {error}", topology.display())));
        }
    }
    paths.extend(property_source_paths(project_dir, &mut checkpoint)?);
    paths.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));

    let mut total = 0_u64;
    let mut owned = Vec::with_capacity(paths.len());
    for (name, path) in paths {
        checkpoint()?;
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|error| source(format!("inspect {}: {error}", path.display())))?;
        if !metadata.file_type().is_file() {
            return Err(source(format!(
                "search source {} is not a regular file",
                path.display()
            )));
        }
        total = total
            .checked_add(metadata.len())
            .ok_or_else(|| exhausted_source(limits.source_bytes))?;
        if total > limits.source_bytes {
            return Err(exhausted_source(limits.source_bytes));
        }
        let bytes = std::fs::read(&path)
            .map_err(|error| source(format!("read {}: {error}", path.display())))?;
        let actual =
            u64::try_from(bytes.len()).map_err(|_| exhausted_source(limits.source_bytes))?;
        if actual > metadata.len() {
            total = total
                .checked_add(actual - metadata.len())
                .ok_or_else(|| exhausted_source(limits.source_bytes))?;
            if total > limits.source_bytes {
                return Err(exhausted_source(limits.source_bytes));
            }
        }
        owned.push((name, bytes));
    }
    checkpoint()?;
    let parts = owned
        .iter()
        .map(|(name, bytes)| SearchSourcePart {
            name,
            bytes: bytes.as_slice(),
        })
        .collect::<Vec<_>>();
    SearchSourceSnapshot::capture(project_dir, &parts)
}

fn property_source_paths<C>(
    project_dir: &Path,
    checkpoint: &mut C,
) -> Result<Vec<(String, PathBuf)>, SearchArtifactError>
where
    C: FnMut() -> Result<(), SearchArtifactError>,
{
    let directory = project_dir.join("properties");
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(source(format!(
                "read property source directory {}: {error}",
                directory.display()
            )));
        }
    };
    let mut paths = Vec::new();
    for entry in entries {
        checkpoint()?;
        let entry =
            entry.map_err(|error| source(format!("read property source entry: {error}")))?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("parquet") {
            continue;
        }
        let file_name = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| source("property source contains a non-UTF-8 Parquet name"))?;
        paths.push((format!("properties/{file_name}"), path));
    }
    paths.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    Ok(paths)
}

fn exhausted_source(limit: u64) -> SearchArtifactError {
    SearchArtifactError::ResourceExhausted {
        resource: "text_source_bytes",
        limit,
    }
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

fn validate_observed_properties(
    projection: &TextSourceProjection,
) -> Result<(), SearchArtifactError> {
    for property in &projection.properties {
        if !projection
            .documents
            .iter()
            .any(|document| document.fields.contains_key(property))
        {
            return Err(invalid(
                "property",
                format!("{property:?} is not observed as a string for the selected label"),
            ));
        }
    }
    Ok(())
}

fn source(reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::SourceSnapshot {
        reason: reason.into(),
    }
}

fn corrupt(path: &Path, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::CorruptDerivedIndex {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashMap;

    use graphforge_core::uuid::{Uuid, to_bytes};
    use graphforge_ir::{IrLiteral, OntologyMode, TypeId};
    use graphforge_storage::generation::bump_search_generation;
    use graphforge_storage::{
        GraphWriter, SearchPublicationMode, current_search_artifact, set_node_properties,
    };
    use tempfile::TempDir;

    use super::*;

    const LABEL: &str = "Person";
    const LABEL_ID: u32 = 1;

    fn uuid(value: u8) -> Uuid {
        let mut bytes = [0_u8; 16];
        bytes[15] = value;
        Uuid::from_bytes(bytes)
    }

    fn properties() -> Vec<String> {
        vec!["name".to_owned()]
    }

    fn request(properties: &[String]) -> TextIndexRequest<'_> {
        TextIndexRequest {
            label: LABEL,
            label_id: LABEL_ID,
            properties,
        }
    }

    fn lazy_request() -> LazyTextRequest<'static> {
        LazyTextRequest {
            label: LABEL,
            label_id: LABEL_ID,
        }
    }

    fn lazy_search(project_dir: &Path, query: &str) -> Vec<TextSearchHit> {
        search_default_text(
            project_dir,
            lazy_request(),
            query,
            10,
            TextLifecycleLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    fn write_person(project_dir: &Path, value: &str) {
        let mut writer = GraphWriter::open_at(project_dir, OntologyMode::Strict, 1).unwrap();
        writer.create_node(uuid(1), TypeId(LABEL_ID)).unwrap();
        writer
            .set_properties(
                &uuid(1),
                Some(LABEL),
                HashMap::from([("name".to_owned(), IrLiteral::Str(value.to_owned()))]),
            )
            .unwrap();
        writer.flush().unwrap();
    }

    fn set_person_name(project_dir: &Path, value: &str) {
        let updates = HashMap::from([(
            to_bytes(&uuid(1)),
            HashMap::from([("name".to_owned(), IrLiteral::Str(value.to_owned()))]),
        )]);
        assert_eq!(
            set_node_properties(project_dir, LABEL, &updates).unwrap(),
            1
        );
    }

    fn prepare(project_dir: &Path) -> PublishedTextIndex {
        let properties = properties();
        prepare_text_index(
            project_dir,
            request(&properties),
            SearchPublicationMode::ReuseFresh,
            TextLifecycleLimits::default(),
            || Ok(()),
        )
        .unwrap()
    }

    #[test]
    fn missing_index_builds_searches_and_reuses_across_reopen() {
        let dir = TempDir::new().unwrap();
        write_person(dir.path(), "Alice Example");

        let first = prepare(dir.path());
        assert!(!first.is_empty());
        let hits = search_published_text(
            dir.path(),
            request(&properties()),
            "ALICE",
            10,
            TextLifecycleLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node_uuid[15], 1);

        let reopened = prepare(dir.path());
        assert_eq!(reopened.artifact().path, first.artifact().path);
    }

    #[test]
    fn empty_label_marker_is_published_reused_and_searched_as_empty() {
        let dir = TempDir::new().unwrap();
        let first = prepare(dir.path());
        assert!(first.is_empty());
        assert_eq!(
            std::fs::read(first.artifact().path.join(EMPTY_MARKER_FILE)).unwrap(),
            EMPTY_MARKER_BYTES
        );
        let reopened = prepare(dir.path());
        assert!(reopened.is_empty());
        assert_eq!(reopened.artifact().path, first.artifact().path);
        assert!(
            search_published_text(
                dir.path(),
                request(&properties()),
                "anything",
                10,
                TextLifecycleLimits::default(),
                || Ok(())
            )
            .unwrap()
            .is_empty()
        );
        assert!(matches!(
            search_published_text(
                dir.path(),
                request(&properties()),
                "!!!",
                10,
                TextLifecycleLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::InvalidSelector { field: "query", .. })
        ));
    }

    #[test]
    fn corrupt_empty_marker_and_tantivy_are_rebuilt_once() {
        let empty = TempDir::new().unwrap();
        let first_empty = prepare(empty.path());
        std::fs::write(
            first_empty.artifact().path.join(EMPTY_MARKER_FILE),
            b"corrupt",
        )
        .unwrap();
        let repaired_empty = prepare(empty.path());
        assert!(repaired_empty.is_empty());
        assert_ne!(repaired_empty.artifact().path, first_empty.artifact().path);

        let populated = TempDir::new().unwrap();
        write_person(populated.path(), "Alice");
        let first_index = prepare(populated.path());
        std::fs::remove_file(first_index.artifact().path.join("meta.json")).unwrap();
        let repaired_index = prepare(populated.path());
        assert!(!repaired_index.is_empty());
        assert_ne!(repaired_index.artifact().path, first_index.artifact().path);
    }

    #[test]
    fn stale_mutation_rebuilds_and_failed_or_cancelled_build_keeps_pointer() {
        let dir = TempDir::new().unwrap();
        write_person(dir.path(), "Alice");
        let first = prepare(dir.path());
        let key = SearchArtifactKey::text(LABEL, properties()).unwrap();

        set_person_name(dir.path(), "Bob");
        let bob = search_published_text(
            dir.path(),
            request(&properties()),
            "bob",
            10,
            TextLifecycleLimits::default(),
            || Ok(()),
        )
        .unwrap();
        assert_eq!(bob.len(), 1);
        let fresh = current_search_artifact(dir.path(), &key).unwrap().unwrap();
        assert_ne!(fresh.path, first.artifact().path);

        let cancelled = prepare_text_index(
            dir.path(),
            request(&properties()),
            SearchPublicationMode::Replace,
            TextLifecycleLimits::default(),
            || Err(SearchArtifactError::Cancelled),
        );
        assert!(matches!(cancelled, Err(SearchArtifactError::Cancelled)));
        assert_eq!(
            current_search_artifact(dir.path(), &key)
                .unwrap()
                .unwrap()
                .path,
            fresh.path
        );

        std::fs::write(dir.path().join("properties/Person.parquet"), b"not parquet").unwrap();
        assert!(matches!(
            prepare_text_index(
                dir.path(),
                request(&properties()),
                SearchPublicationMode::ReuseFresh,
                TextLifecycleLimits::default(),
                || Ok(())
            ),
            Err(SearchArtifactError::SourceSnapshot { .. })
        ));
        assert_eq!(
            current_search_artifact(dir.path(), &key)
                .unwrap()
                .unwrap()
                .path,
            fresh.path
        );
    }

    #[test]
    fn source_limits_cancellation_and_two_mutations_are_structured() {
        let dir = TempDir::new().unwrap();
        write_person(dir.path(), "Alice");

        let mut limits = TextLifecycleLimits::default();
        limits.text.source_bytes = 0;
        assert!(matches!(
            prepare_text_index(
                dir.path(),
                request(&properties()),
                SearchPublicationMode::ReuseFresh,
                limits,
                || Ok(())
            ),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "text_source_bytes",
                ..
            })
        ));

        assert!(matches!(
            capture_text_snapshot(dir.path(), TextSearchLimits::default(), || Err(
                SearchArtifactError::Cancelled
            )),
            Err(SearchArtifactError::Cancelled)
        ));

        let calls = Cell::new(0_usize);
        let result = prepare_text_index(
            dir.path(),
            request(&properties()),
            SearchPublicationMode::Replace,
            TextLifecycleLimits::default(),
            || {
                calls.set(calls.get() + 1);
                bump_search_generation(dir.path()).unwrap();
                Ok(())
            },
        );
        assert!(matches!(
            result,
            Err(SearchArtifactError::ConcurrentMutation)
        ));
        assert!(calls.get() > 2);
    }

    #[test]
    fn mutation_during_post_search_snapshot_is_retried_before_return() {
        let dir = TempDir::new().unwrap();
        write_person(dir.path(), "Alice");
        let first = prepare(dir.path());

        let baseline_calls = Cell::new(0_usize);
        search_published_text(
            dir.path(),
            request(&properties()),
            "alice",
            10,
            TextLifecycleLimits::default(),
            || {
                baseline_calls.set(baseline_calls.get() + 1);
                Ok(())
            },
        )
        .unwrap();

        let calls = Cell::new(0_usize);
        let mutated = Cell::new(false);
        let hits = search_published_text(
            dir.path(),
            request(&properties()),
            "alice",
            10,
            TextLifecycleLimits::default(),
            || {
                calls.set(calls.get() + 1);
                if !mutated.get() && calls.get() == baseline_calls.get() {
                    bump_search_generation(dir.path()).unwrap();
                    mutated.set(true);
                }
                Ok(())
            },
        )
        .unwrap();
        assert!(mutated.get());
        assert_eq!(hits.len(), 1);
        assert_ne!(prepare(dir.path()).artifact().path, first.artifact().path);
    }

    #[test]
    fn lazy_default_reuses_and_tracks_the_exact_discovered_key() {
        let dir = TempDir::new().unwrap();
        write_person(dir.path(), "Alice Example");

        let first_hits = lazy_search(dir.path(), "alice");
        assert_eq!(first_hits.len(), 1);
        let key = SearchArtifactKey::text(LABEL, ["name"]).unwrap();
        let first = current_search_artifact(dir.path(), &key).unwrap().unwrap();
        assert_eq!(lazy_search(dir.path(), "example").len(), 1);
        assert_eq!(
            current_search_artifact(dir.path(), &key)
                .unwrap()
                .unwrap()
                .path,
            first.path
        );

        let updates = HashMap::from([(
            to_bytes(&uuid(1)),
            HashMap::from([(
                "summary".to_owned(),
                IrLiteral::Str("Graph native search".to_owned()),
            )]),
        )]);
        assert_eq!(set_node_properties(dir.path(), LABEL, &updates).unwrap(), 1);
        assert_eq!(lazy_search(dir.path(), "graph native").len(), 1);
        let exact = SearchArtifactKey::text(LABEL, ["name", "summary"]).unwrap();
        assert!(
            current_search_artifact(dir.path(), &exact)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn lazy_default_validates_before_source_and_publishes_no_empty_key() {
        let dir = TempDir::new().unwrap();
        let checkpoints = Cell::new(0_usize);
        let invalid = search_default_text(
            dir.path(),
            lazy_request(),
            "!!!",
            10,
            TextLifecycleLimits::default(),
            || {
                checkpoints.set(checkpoints.get() + 1);
                Ok(())
            },
        );
        assert!(matches!(
            invalid,
            Err(SearchArtifactError::InvalidSelector { field: "query", .. })
        ));
        assert_eq!(checkpoints.get(), 0, "invalid queries must not read source");
        assert!(lazy_search(dir.path(), "valid").is_empty());
        assert!(!dir.path().join("indexes/search/text").exists());
    }

    #[test]
    fn lazy_default_bounds_repeated_mutation() {
        let racing = TempDir::new().unwrap();
        write_person(racing.path(), "Alice");
        let result = search_default_text(
            racing.path(),
            lazy_request(),
            "alice",
            10,
            TextLifecycleLimits::default(),
            || {
                bump_search_generation(racing.path()).unwrap();
                Ok(())
            },
        );
        assert!(matches!(
            result,
            Err(SearchArtifactError::ConcurrentMutation)
        ));
    }
}
