//! Canonical identity, manifests, and freshness for search artifacts.
//!
//! Text indexes and caller-supplied vector stores share this metadata contract.
//! Raw selectors never become path components: normalized UTF-8 bytes are
//! length-framed, hexadecimal encoded, and split into bounded safe segments.

use std::io::{Read, Seek};
use std::path::{Path, PathBuf};

use graphforge_core::GfError;

/// Search manifest format implemented by this release.
pub const SEARCH_MANIFEST_VERSION: u32 = 1;
/// Maximum accepted JSON manifest size.
pub const MAX_SEARCH_MANIFEST_BYTES: usize = 64 * 1024;
/// Maximum bytes in one normalized label, property, or vector-space selector.
pub const MAX_SEARCH_SELECTOR_BYTES: usize = 128;
/// Maximum combined normalized bytes represented by one artifact key.
pub const MAX_SEARCH_ARTIFACT_KEY_BYTES: usize = 384;

/// Errors produced by shared search storage and publication.
#[derive(Debug, thiserror::Error)]
pub enum SearchArtifactError {
    /// A caller selector cannot be normalized into the v0.5 contract.
    #[error("invalid search {field}: {reason}")]
    InvalidSelector {
        /// Selector field.
        field: &'static str,
        /// Stable validation reason.
        reason: String,
    },
    /// No published artifact exists for the requested key.
    #[error("search artifact is missing at {}", path.display())]
    Missing {
        /// Expected path.
        path: PathBuf,
    },
    /// A JSON manifest or publication pointer is malformed.
    #[error("corrupt search manifest at {}: {reason}", path.display())]
    CorruptManifest {
        /// Corrupt metadata path.
        path: PathBuf,
        /// Parse or contract failure.
        reason: String,
    },
    /// Backend files for a rebuildable derived text index failed validation.
    #[error("corrupt derived search index at {}: {reason}", path.display())]
    CorruptDerivedIndex {
        /// Corrupt derived artifact.
        path: PathBuf,
        /// Backend validation failure.
        reason: String,
    },
    /// A manifest uses a version this binary cannot consume.
    #[error(
        "incompatible search manifest at {}: version {found}, supported {supported}",
        path.display()
    )]
    IncompatibleManifest {
        /// Manifest path.
        path: PathBuf,
        /// Version found on disk.
        found: u64,
        /// Version supported by this binary.
        supported: u32,
    },
    /// A verified manifest does not describe the current source snapshot.
    #[error("stale search artifact: {reason}")]
    Stale {
        /// Deterministic mismatch reason.
        reason: String,
    },
    /// Caller-supplied vector data is corrupt and must not be discarded.
    #[error("corrupt primary vector data at {}: {reason}", path.display())]
    CorruptPrimaryVectors {
        /// Corrupt vector artifact.
        path: PathBuf,
        /// Validation failure.
        reason: String,
    },
    /// The graph changed twice while the bounded build retry ran.
    #[error("graph changed during both search publication attempts")]
    ConcurrentMutation,
    /// The per-artifact writer lock could not be acquired.
    #[error("search writer lock failed at {}: {reason}", path.display())]
    Lock {
        /// Lock-file path.
        path: PathBuf,
        /// I/O or timeout reason.
        reason: String,
    },
    /// Cooperative cancellation stopped work before publication.
    #[error("search operation cancelled")]
    Cancelled,
    /// A named search resource limit was exceeded.
    #[error("search resource limit exceeded for {resource}: limit {limit}")]
    ResourceExhausted {
        /// Bounded resource.
        resource: &'static str,
        /// Configured maximum.
        limit: u64,
    },
    /// Backend construction failed without publishing partial state.
    #[error("search artifact build failed: {0}")]
    Build(String),
    /// Capturing the committed graph source snapshot failed.
    #[error("search source snapshot failed: {reason}")]
    SourceSnapshot {
        /// Storage or fingerprint failure.
        reason: String,
    },
    /// Filesystem work failed.
    #[error("search storage {operation} failed at {}: {source}", path.display())]
    Io {
        /// Stable operation name.
        operation: &'static str,
        /// Affected path.
        path: PathBuf,
        /// Operating-system failure.
        #[source]
        source: std::io::Error,
    },
}

impl From<SearchArtifactError> for GfError {
    fn from(error: SearchArtifactError) -> Self {
        let message = error.to_string();
        match error {
            SearchArtifactError::InvalidSelector { .. } => Self::Validation(message),
            SearchArtifactError::Cancelled
            | SearchArtifactError::ResourceExhausted { .. }
            | SearchArtifactError::Build(_) => Self::Execution(message),
            SearchArtifactError::ConcurrentMutation => Self::Lifecycle(message),
            SearchArtifactError::Missing { .. }
            | SearchArtifactError::CorruptManifest { .. }
            | SearchArtifactError::CorruptDerivedIndex { .. }
            | SearchArtifactError::IncompatibleManifest { .. }
            | SearchArtifactError::Stale { .. }
            | SearchArtifactError::CorruptPrimaryVectors { .. }
            | SearchArtifactError::SourceSnapshot { .. }
            | SearchArtifactError::Lock { .. }
            | SearchArtifactError::Io { .. } => Self::Storage(message),
        }
    }
}

/// Physical search backend represented by an artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchIndexKind {
    /// Rebuildable Tantivy text index.
    Text,
    /// Caller-supplied primary vector data.
    Vector,
}

impl SearchIndexKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Vector => "vector",
        }
    }

    fn parse(value: &str, path: &Path) -> Result<Self, SearchArtifactError> {
        match value {
            "text" => Ok(Self::Text),
            "vector" => Ok(Self::Vector),
            other => Err(corrupt(path, format!("unknown index_kind {other:?}"))),
        }
    }
}

/// Canonical normalized identity of one search artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchArtifactKey {
    kind: SearchIndexKind,
    label: String,
    properties: Option<Vec<String>>,
    space: Option<String>,
}

impl SearchArtifactKey {
    /// Construct a text key. Property names are trimmed, sorted, and deduplicated.
    ///
    /// # Errors
    /// Returns [`SearchArtifactError::InvalidSelector`] for empty, control-
    /// containing, overlong, or collectively oversized selectors.
    pub fn text<I, S>(label: &str, properties: I) -> Result<Self, SearchArtifactError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let label = normalize_selector("label", label)?;
        let mut properties = properties
            .into_iter()
            .map(|property| normalize_selector("property", property.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        properties.sort_unstable();
        properties.dedup();
        if properties.is_empty() {
            return Err(invalid("properties", "at least one property is required"));
        }
        let key = Self {
            kind: SearchIndexKind::Text,
            label,
            properties: Some(properties),
            space: None,
        };
        key.validate_total_size()?;
        Ok(key)
    }

    /// Construct a vector key for one required label and normalized space.
    ///
    /// # Errors
    /// Returns [`SearchArtifactError::InvalidSelector`] for an invalid selector.
    pub fn vector(label: &str, space: &str) -> Result<Self, SearchArtifactError> {
        let key = Self {
            kind: SearchIndexKind::Vector,
            label: normalize_selector("label", label)?,
            properties: None,
            space: Some(normalize_selector("space", space)?),
        };
        key.validate_total_size()?;
        Ok(key)
    }

    /// Backend kind.
    #[must_use]
    pub const fn kind(&self) -> SearchIndexKind {
        self.kind
    }

    /// Normalized graph label.
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Canonical sorted text properties.
    #[must_use]
    pub fn properties(&self) -> Option<&[String]> {
        self.properties.as_deref()
    }

    /// Normalized vector space.
    #[must_use]
    pub fn space(&self) -> Option<&str> {
        self.space.as_deref()
    }

    /// Collision-free, filesystem-safe root for this key.
    #[must_use]
    pub fn artifact_root(&self, project_dir: &Path) -> PathBuf {
        match self.kind {
            SearchIndexKind::Text => {
                let mut path = project_dir.join("indexes").join("search").join("text");
                push_encoded(&mut path, "label", self.label.as_bytes());
                let properties = self
                    .properties
                    .as_deref()
                    .expect("text keys always carry properties");
                push_encoded(&mut path, "properties", &encode_sequence(properties));
                path
            }
            SearchIndexKind::Vector => {
                let mut path = project_dir.join("embeddings");
                push_encoded(
                    &mut path,
                    "space",
                    self.space
                        .as_deref()
                        .expect("vector keys always carry a space")
                        .as_bytes(),
                );
                push_encoded(&mut path, "label", self.label.as_bytes());
                path
            }
        }
    }

    fn validate_total_size(&self) -> Result<(), SearchArtifactError> {
        let property_bytes = self
            .properties
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(String::len)
            .sum::<usize>();
        let bytes = self.label.len() + property_bytes + self.space.as_deref().map_or(0, str::len);
        if bytes > MAX_SEARCH_ARTIFACT_KEY_BYTES {
            return Err(invalid(
                "artifact key",
                format!("{bytes} normalized bytes exceeds {MAX_SEARCH_ARTIFACT_KEY_BYTES}"),
            ));
        }
        Ok(())
    }
}

/// Stable graph snapshot identity stored in a search manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchSourceSnapshot {
    /// Committed search-source generation.
    pub generation: u64,
    /// Canonical content fingerprint.
    pub fingerprint: String,
}

impl SearchSourceSnapshot {
    /// Capture the current search generation and fingerprint the supplied
    /// logical source parts.
    ///
    /// # Errors
    /// Returns a storage error for an unreadable generation or invalid source
    /// part list.
    pub fn capture(
        project_dir: &Path,
        parts: &[SearchSourcePart<'_>],
    ) -> Result<Self, SearchArtifactError> {
        let generation =
            crate::generation::read_search_generation(project_dir).map_err(|error| {
                SearchArtifactError::SourceSnapshot {
                    reason: error.to_string(),
                }
            })?;
        let fingerprint = canonical_source_fingerprint(parts)?;
        Ok(Self {
            generation,
            fingerprint,
        })
    }

    /// Capture a canonical source snapshot by streaming regular files through
    /// one fixed-size buffer. Each pathname is opened once and the resulting
    /// handle is used for both admission and hashing, so replacement after open
    /// cannot redirect the read to a different object.
    pub fn capture_files(
        project_dir: &Path,
        files: &[(String, PathBuf)],
        byte_limit: u64,
        resource: &'static str,
    ) -> Result<Self, SearchArtifactError> {
        let generation =
            crate::generation::read_search_generation(project_dir).map_err(|error| {
                SearchArtifactError::SourceSnapshot {
                    reason: error.to_string(),
                }
            })?;
        let fingerprint = canonical_file_source_fingerprint(files, byte_limit, resource)?;
        Ok(Self {
            generation,
            fingerprint,
        })
    }
}

/// One deterministically named byte source in a committed search snapshot.
#[derive(Clone, Copy, Debug)]
pub struct SearchSourcePart<'a> {
    /// Stable logical name, not an absolute machine path.
    pub name: &'a str,
    /// Exact committed bytes.
    pub bytes: &'a [u8],
}

/// Produce a deterministic 256-bit content fingerprint.
///
/// The four-domain FNV-1a construction is a change detector, not a security
/// primitive. Supported GraphForge mutations are protected by the generation
/// counter; this fingerprint additionally detects observable external file
/// changes without claiming adversarial tamper resistance.
///
/// # Errors
/// Rejects empty, control-containing, or duplicate logical part names.
pub fn canonical_source_fingerprint(
    parts: &[SearchSourcePart<'_>],
) -> Result<String, SearchArtifactError> {
    let mut ordered = parts.to_vec();
    ordered.sort_unstable_by(|left, right| left.name.as_bytes().cmp(right.name.as_bytes()));
    for pair in ordered.windows(2) {
        if pair[0].name == pair[1].name {
            return Err(invalid(
                "source fingerprint",
                format!("duplicate source part {:?}", pair[0].name),
            ));
        }
    }

    let mut states = [
        0xcbf2_9ce4_8422_2325_u64,
        0x8422_2325_cbf2_9ce4_u64,
        0x9e37_79b9_7f4a_7c15_u64,
        0xd6e8_feb8_6659_fd93_u64,
    ];
    for part in ordered {
        let name = normalize_source_name(part.name)?;
        hash_frame(&mut states, name.as_bytes());
        hash_frame(&mut states, part.bytes);
    }
    Ok(format!(
        "gf-fnv1a256:{:016x}{:016x}{:016x}{:016x}",
        states[0], states[1], states[2], states[3]
    ))
}

/// Stream canonical named files into the same fingerprint format as
/// [`canonical_source_fingerprint`] without retaining their contents.
pub fn canonical_file_source_fingerprint(
    files: &[(String, PathBuf)],
    byte_limit: u64,
    resource: &'static str,
) -> Result<String, SearchArtifactError> {
    let mut ordered = files.to_vec();
    ordered.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    for pair in ordered.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(invalid(
                "source fingerprint",
                format!("duplicate source part {:?}", pair[0].0),
            ));
        }
    }
    let mut states = [
        0xcbf2_9ce4_8422_2325_u64,
        0x8422_2325_cbf2_9ce4_u64,
        0x9e37_79b9_7f4a_7c15_u64,
        0xd6e8_feb8_6659_fd93_u64,
    ];
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    for (name, path) in ordered {
        let name = normalize_source_name(&name)?;
        let mut file = open_source_nofollow(&path)?;
        let metadata = file.metadata().map_err(|source| SearchArtifactError::Io {
            operation: "inspect search source",
            path: path.clone(),
            source,
        })?;
        if !metadata.file_type().is_file() {
            return Err(SearchArtifactError::SourceSnapshot {
                reason: format!("search source {} is not a regular file", path.display()),
            });
        }
        total =
            total
                .checked_add(metadata.len())
                .ok_or(SearchArtifactError::ResourceExhausted {
                    resource,
                    limit: byte_limit,
                })?;
        if total > byte_limit {
            return Err(SearchArtifactError::ResourceExhausted {
                resource,
                limit: byte_limit,
            });
        }
        hash_frame_prefix(&mut states, name.len() as u64);
        hash_bytes(&mut states, name.as_bytes());
        hash_frame_finish(&mut states);
        hash_frame_prefix(&mut states, metadata.len());
        let mut read = 0_u64;
        loop {
            let count = file
                .read(&mut buffer)
                .map_err(|source| SearchArtifactError::Io {
                    operation: "read search source",
                    path: path.clone(),
                    source,
                })?;
            if count == 0 {
                break;
            }
            read =
                read.checked_add(count as u64)
                    .ok_or(SearchArtifactError::ResourceExhausted {
                        resource,
                        limit: byte_limit,
                    })?;
            if read > metadata.len() {
                return Err(SearchArtifactError::SourceSnapshot {
                    reason: format!("search source {} changed while hashing", path.display()),
                });
            }
            hash_bytes(&mut states, &buffer[..count]);
        }
        if read != metadata.len() || file.stream_position().ok() != Some(metadata.len()) {
            return Err(SearchArtifactError::SourceSnapshot {
                reason: format!("search source {} changed while hashing", path.display()),
            });
        }
        hash_frame_finish(&mut states);
    }
    Ok(format!(
        "gf-fnv1a256:{:016x}{:016x}{:016x}{:016x}",
        states[0], states[1], states[2], states[3]
    ))
}

fn open_source_nofollow(path: &Path) -> Result<std::fs::File, SearchArtifactError> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    options
        .open(path)
        .map_err(|source| SearchArtifactError::Io {
            operation: "open search source",
            path: path.to_path_buf(),
            source,
        })
}

/// Versioned JSON metadata for one complete search artifact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchManifest {
    /// Search-manifest version.
    pub manifest_version: u32,
    /// Text or vector backend.
    pub index_kind: SearchIndexKind,
    /// Pinned backend/semantic version.
    pub backend_version: String,
    /// Normalized graph label.
    pub label: String,
    /// Sorted text properties; absent for vector artifacts.
    pub properties: Option<Vec<String>>,
    /// Normalized vector space; absent for text artifacts.
    pub space: Option<String>,
    /// Fixed vector dimension; absent for text artifacts.
    pub dimension: Option<u32>,
    /// Committed graph mutation generation.
    pub source_generation: u64,
    /// Canonical committed-source fingerprint.
    pub source_fingerprint: String,
    /// Scoring/tokenization/vector contract version.
    pub contract_version: String,
    /// True only in the atomically published manifest.
    pub completed: bool,
}

impl SearchManifest {
    /// Build a manifest for a normalized artifact key and source snapshot.
    ///
    /// # Errors
    /// Rejects invalid versions or a missing/unexpected vector dimension.
    pub fn for_key(
        key: &SearchArtifactKey,
        backend_version: &str,
        contract_version: &str,
        dimension: Option<u32>,
        source: &SearchSourceSnapshot,
        completed: bool,
    ) -> Result<Self, SearchArtifactError> {
        let backend_version = normalize_version("backend_version", backend_version)?;
        let contract_version = normalize_version("contract_version", contract_version)?;
        if !canonical_fingerprint(&source.fingerprint) {
            return Err(invalid(
                "source_fingerprint",
                "must be gf-fnv1a256 followed by 64 lowercase hexadecimal digits",
            ));
        }
        match (key.kind, dimension) {
            (SearchIndexKind::Text, None) => {}
            (SearchIndexKind::Vector, Some(value)) if value > 0 => {}
            (SearchIndexKind::Text, Some(_)) => {
                return Err(invalid("dimension", "text manifests omit dimension"));
            }
            (SearchIndexKind::Vector, _) => {
                return Err(invalid(
                    "dimension",
                    "vector manifests require a non-zero dimension",
                ));
            }
        }
        Ok(Self {
            manifest_version: SEARCH_MANIFEST_VERSION,
            index_kind: key.kind,
            backend_version,
            label: key.label.clone(),
            properties: key.properties.clone(),
            space: key.space.clone(),
            dimension,
            source_generation: source.generation,
            source_fingerprint: source.fingerprint.clone(),
            contract_version,
            completed,
        })
    }

    /// Serialize canonical compact JSON. Optional backend-specific fields are
    /// omitted rather than encoded as null.
    ///
    /// # Errors
    /// Returns a corruption error only if JSON serialization unexpectedly
    /// fails.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>, SearchArtifactError> {
        let mut value = serde_json::Map::new();
        value.insert(
            "manifest_version".to_owned(),
            serde_json::Value::from(self.manifest_version),
        );
        value.insert(
            "index_kind".to_owned(),
            serde_json::Value::from(self.index_kind.as_str()),
        );
        value.insert(
            "backend_version".to_owned(),
            serde_json::Value::from(self.backend_version.clone()),
        );
        value.insert(
            "label".to_owned(),
            serde_json::Value::from(self.label.clone()),
        );
        if let Some(properties) = &self.properties {
            value.insert(
                "properties".to_owned(),
                serde_json::Value::Array(
                    properties
                        .iter()
                        .cloned()
                        .map(serde_json::Value::from)
                        .collect(),
                ),
            );
        }
        if let Some(space) = &self.space {
            value.insert("space".to_owned(), serde_json::Value::from(space.clone()));
        }
        if let Some(dimension) = self.dimension {
            value.insert("dimension".to_owned(), serde_json::Value::from(dimension));
        }
        value.insert(
            "source_generation".to_owned(),
            serde_json::Value::from(self.source_generation),
        );
        value.insert(
            "source_fingerprint".to_owned(),
            serde_json::Value::from(self.source_fingerprint.clone()),
        );
        value.insert(
            "contract_version".to_owned(),
            serde_json::Value::from(self.contract_version.clone()),
        );
        value.insert(
            "completed".to_owned(),
            serde_json::Value::from(self.completed),
        );
        serde_json::to_vec(&serde_json::Value::Object(value)).map_err(|error| {
            SearchArtifactError::CorruptManifest {
                path: PathBuf::from("<memory>"),
                reason: error.to_string(),
            }
        })
    }

    /// Parse and validate a persisted manifest.
    ///
    /// # Errors
    /// Distinguishes oversized, corrupt, and incompatible manifests.
    pub fn from_json(path: &Path, bytes: &[u8]) -> Result<Self, SearchArtifactError> {
        if bytes.len() > MAX_SEARCH_MANIFEST_BYTES {
            return Err(SearchArtifactError::ResourceExhausted {
                resource: "manifest_bytes",
                limit: MAX_SEARCH_MANIFEST_BYTES as u64,
            });
        }
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|error| corrupt(path, error.to_string()))?;
        let object = value
            .as_object()
            .ok_or_else(|| corrupt(path, "expected a JSON object"))?;
        let found = required_u64(object, "manifest_version", path)?;
        if found != u64::from(SEARCH_MANIFEST_VERSION) {
            return Err(SearchArtifactError::IncompatibleManifest {
                path: path.to_path_buf(),
                found,
                supported: SEARCH_MANIFEST_VERSION,
            });
        }
        let kind = SearchIndexKind::parse(required_str(object, "index_kind", path)?, path)?;
        let label = required_str(object, "label", path)?;
        let properties = optional_strings(object, "properties", path)?;
        let space = optional_str(object, "space", path)?;
        let key = match kind {
            SearchIndexKind::Text => SearchArtifactKey::text(
                label,
                properties
                    .as_deref()
                    .ok_or_else(|| corrupt(path, "text manifest omits properties"))?,
            )
            .map_err(|error| corrupt(path, error.to_string()))?,
            SearchIndexKind::Vector => SearchArtifactKey::vector(
                label,
                space.ok_or_else(|| corrupt(path, "vector manifest omits space"))?,
            )
            .map_err(|error| corrupt(path, error.to_string()))?,
        };
        if key.label() != label || key.properties() != properties.as_deref() || key.space() != space
        {
            return Err(corrupt(
                path,
                "artifact selectors are not in canonical normalized order",
            ));
        }
        let backend_version = required_str(object, "backend_version", path)?;
        let contract_version = required_str(object, "contract_version", path)?;
        let dimension = optional_u64(object, "dimension", path)?
            .map(|value| {
                u32::try_from(value).map_err(|_| corrupt(path, "dimension exceeds u32 range"))
            })
            .transpose()?;
        let source = SearchSourceSnapshot {
            generation: required_u64(object, "source_generation", path)?,
            fingerprint: required_str(object, "source_fingerprint", path)?.to_owned(),
        };
        let manifest = Self::for_key(
            &key,
            backend_version,
            contract_version,
            dimension,
            &source,
            required_bool(object, "completed", path)?,
        )
        .map_err(|error| corrupt(path, error.to_string()))?;
        if manifest.backend_version != backend_version
            || manifest.contract_version != contract_version
        {
            return Err(corrupt(path, "version fields are not normalized"));
        }
        Ok(manifest)
    }

    /// Verify exact selector, version, completion, generation, and fingerprint
    /// equality against the current committed snapshot.
    ///
    /// # Errors
    /// Returns [`SearchArtifactError::Stale`] with the first deterministic
    /// mismatch. Derived text callers rebuild; vector callers surface primary
    /// corruption or staleness according to their backend contract.
    pub fn verify_fresh(
        &self,
        key: &SearchArtifactKey,
        backend_version: &str,
        contract_version: &str,
        dimension: Option<u32>,
        source: &SearchSourceSnapshot,
    ) -> Result<(), SearchArtifactError> {
        let expected = Self::for_key(
            key,
            backend_version,
            contract_version,
            dimension,
            source,
            true,
        )?;
        let reason = if !self.completed {
            Some("manifest is incomplete".to_owned())
        } else if self.index_kind != expected.index_kind
            || self.label != expected.label
            || self.properties != expected.properties
            || self.space != expected.space
        {
            Some("normalized artifact key changed".to_owned())
        } else if self.backend_version != expected.backend_version {
            Some("backend version changed".to_owned())
        } else if self.contract_version != expected.contract_version {
            Some("search contract version changed".to_owned())
        } else if self.dimension != expected.dimension {
            Some("vector dimension changed".to_owned())
        } else if self.source_generation != expected.source_generation {
            Some(format!(
                "source generation changed from {} to {}",
                self.source_generation, expected.source_generation
            ))
        } else if self.source_fingerprint != expected.source_fingerprint {
            Some("source fingerprint changed".to_owned())
        } else {
            None
        };
        match reason {
            Some(reason) => Err(SearchArtifactError::Stale { reason }),
            None => Ok(()),
        }
    }
}

fn invalid(field: &'static str, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::InvalidSelector {
        field,
        reason: reason.into(),
    }
}

fn corrupt(path: &Path, reason: impl Into<String>) -> SearchArtifactError {
    SearchArtifactError::CorruptManifest {
        path: path.to_path_buf(),
        reason: reason.into(),
    }
}

fn normalize_selector(field: &'static str, value: &str) -> Result<String, SearchArtifactError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid(field, "must not be empty"));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid(field, "must not contain control characters"));
    }
    if value.len() > MAX_SEARCH_SELECTOR_BYTES {
        return Err(invalid(
            field,
            format!(
                "{} UTF-8 bytes exceeds {MAX_SEARCH_SELECTOR_BYTES}",
                value.len()
            ),
        ));
    }
    Ok(value.to_owned())
}

fn normalize_source_name(value: &str) -> Result<&str, SearchArtifactError> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(invalid(
            "source fingerprint",
            "logical part names must be non-empty and control-free",
        ));
    }
    Ok(value)
}

fn normalize_version(field: &'static str, value: &str) -> Result<String, SearchArtifactError> {
    normalize_selector(field, value)
}

fn canonical_fingerprint(value: &str) -> bool {
    value.strip_prefix("gf-fnv1a256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn encode_sequence(values: &[String]) -> Vec<u8> {
    let mut bytes = Vec::new();
    let value_count =
        u32::try_from(values.len()).expect("validated search keys contain a bounded item count");
    bytes.extend_from_slice(&value_count.to_be_bytes());
    for value in values {
        let value_len =
            u32::try_from(value.len()).expect("validated search selectors have bounded lengths");
        bytes.extend_from_slice(&value_len.to_be_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
    bytes
}

fn push_encoded(path: &mut PathBuf, field: &str, bytes: &[u8]) {
    path.push(field);
    let encoded = hex(bytes);
    for (index, chunk) in encoded.as_bytes().chunks(120).enumerate() {
        let chunk = std::str::from_utf8(chunk).expect("hex is always UTF-8");
        path.push(format!("{index:04}-{chunk}"));
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn hash_frame(states: &mut [u64; 4], bytes: &[u8]) {
    hash_frame_prefix(states, bytes.len() as u64);
    hash_bytes(states, bytes);
    hash_frame_finish(states);
}

fn hash_frame_prefix(states: &mut [u64; 4], length: u64) {
    for state in &mut *states {
        for byte in length.to_be_bytes() {
            *state ^= u64::from(byte);
            *state = state.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

fn hash_bytes(states: &mut [u64; 4], bytes: &[u8]) {
    for state in &mut *states {
        for byte in bytes {
            *state ^= u64::from(*byte);
            *state = state.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

fn hash_frame_finish(states: &mut [u64; 4]) {
    for (index, state) in states.iter_mut().enumerate() {
        *state ^= (index as u64 + 1) * 0x9e37_79b9;
    }
}

fn required_str<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
    path: &Path,
) -> Result<&'a str, SearchArtifactError> {
    object
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| corrupt(path, format!("missing or non-string {field}")))
}

fn optional_str<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
    path: &Path,
) -> Result<Option<&'a str>, SearchArtifactError> {
    object
        .get(field)
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| corrupt(path, format!("{field} must be a string")))
        })
        .transpose()
}

fn required_u64(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    path: &Path,
) -> Result<u64, SearchArtifactError> {
    object
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| corrupt(path, format!("missing or non-u64 {field}")))
}

fn optional_u64(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    path: &Path,
) -> Result<Option<u64>, SearchArtifactError> {
    object
        .get(field)
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| corrupt(path, format!("{field} must be a u64")))
        })
        .transpose()
}

fn required_bool(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    path: &Path,
) -> Result<bool, SearchArtifactError> {
    object
        .get(field)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| corrupt(path, format!("missing or non-boolean {field}")))
}

fn optional_strings(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    path: &Path,
) -> Result<Option<Vec<String>>, SearchArtifactError> {
    object
        .get(field)
        .map(|value| {
            let values = value
                .as_array()
                .ok_or_else(|| corrupt(path, format!("{field} must be an array")))?;
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| corrupt(path, format!("{field} must contain strings")))
                })
                .collect()
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> SearchSourceSnapshot {
        SearchSourceSnapshot {
            generation: 7,
            fingerprint: format!("gf-fnv1a256:{:064x}", 7),
        }
    }

    #[test]
    fn key_normalization_and_paths_are_stable_and_safe() {
        let key = SearchArtifactKey::text(" Person ", [" bio ", "name", "bio"]).unwrap();
        assert_eq!(key.label(), "Person");
        assert_eq!(
            key.properties().unwrap(),
            &["bio".to_owned(), "name".to_owned()]
        );
        let path = key.artifact_root(Path::new("/project"));
        let rendered = path.to_string_lossy();
        assert!(rendered.starts_with("/project/indexes/search/text/label/"));
        assert!(!rendered.contains("Person"));
        assert!(!rendered.contains("bio"));

        let vector = SearchArtifactKey::vector("Person", " model/v1 ").unwrap();
        let rendered = vector
            .artifact_root(Path::new("/project"))
            .to_string_lossy()
            .into_owned();
        assert!(rendered.starts_with("/project/embeddings/space/"));
        assert!(!rendered.contains("model/v1"));
    }

    #[test]
    fn key_encoding_is_collision_free_for_framed_properties() {
        let left = SearchArtifactKey::text("L", ["ab", "c"]).unwrap();
        let right = SearchArtifactKey::text("L", ["a", "bc"]).unwrap();
        assert_ne!(
            left.artifact_root(Path::new("/p")),
            right.artifact_root(Path::new("/p"))
        );
    }

    #[test]
    fn invalid_selectors_and_key_budget_are_structured() {
        assert!(matches!(
            SearchArtifactKey::vector("", "space"),
            Err(SearchArtifactError::InvalidSelector { field: "label", .. })
        ));
        assert!(matches!(
            SearchArtifactKey::vector("Person", "bad\nspace"),
            Err(SearchArtifactError::InvalidSelector { field: "space", .. })
        ));
        let long = "x".repeat(MAX_SEARCH_SELECTOR_BYTES + 1);
        assert!(SearchArtifactKey::text("Person", [&long]).is_err());
    }

    #[test]
    fn fingerprint_is_order_independent_and_content_sensitive() {
        let a = SearchSourcePart {
            name: "properties/Person",
            bytes: b"alice",
        };
        let b = SearchSourcePart {
            name: "topology/nodes",
            bytes: b"uuid",
        };
        assert_eq!(
            canonical_source_fingerprint(&[a, b]).unwrap(),
            canonical_source_fingerprint(&[b, a]).unwrap()
        );
        assert_ne!(
            canonical_source_fingerprint(&[a]).unwrap(),
            canonical_source_fingerprint(&[SearchSourcePart {
                name: a.name,
                bytes: b"bob",
            }])
            .unwrap()
        );
        assert!(canonical_source_fingerprint(&[a, a]).is_err());

        let dir = tempfile::tempdir().unwrap();
        let a_path = dir.path().join("a");
        let b_path = dir.path().join("b");
        std::fs::write(&a_path, a.bytes).unwrap();
        std::fs::write(&b_path, b.bytes).unwrap();
        let files = vec![(a.name.to_owned(), a_path), (b.name.to_owned(), b_path)];
        assert_eq!(
            canonical_file_source_fingerprint(&files, 1024, "test_source_bytes").unwrap(),
            canonical_source_fingerprint(&[a, b]).unwrap()
        );
        assert!(matches!(
            canonical_file_source_fingerprint(&files, 3, "test_source_bytes"),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "test_source_bytes",
                limit: 3
            })
        ));
    }

    #[test]
    fn source_snapshot_rejects_corrupt_generation_and_noncanonical_fingerprint() {
        let dir = tempfile::TempDir::new().unwrap();
        let generation = crate::generation::generation_path(dir.path());
        std::fs::create_dir_all(generation.parent().unwrap()).unwrap();
        std::fs::write(&generation, b"corrupt").unwrap();
        assert!(matches!(
            SearchSourceSnapshot::capture(dir.path(), &[]),
            Err(SearchArtifactError::SourceSnapshot { .. })
        ));

        let key = SearchArtifactKey::text("Person", ["name"]).unwrap();
        let source = SearchSourceSnapshot {
            generation: 1,
            fingerprint: "not-canonical".to_owned(),
        };
        assert!(matches!(
            SearchManifest::for_key(&key, "tantivy-0.25", "text-v1", None, &source, true),
            Err(SearchArtifactError::InvalidSelector {
                field: "source_fingerprint",
                ..
            })
        ));
    }

    #[test]
    fn text_manifest_round_trips_canonical_json() {
        let key = SearchArtifactKey::text("Person", ["name", "bio"]).unwrap();
        let manifest = SearchManifest::for_key(
            &key,
            "tantivy-0.25",
            "graphforge_text_v1",
            None,
            &source(),
            true,
        )
        .unwrap();
        let bytes = manifest.to_canonical_json().unwrap();
        let parsed = SearchManifest::from_json(Path::new("manifest.json"), &bytes).unwrap();
        assert_eq!(parsed, manifest);
        let json = String::from_utf8(bytes).unwrap();
        assert!(json.contains(r#""properties":["bio","name"]"#));
        assert!(!json.contains(r#""space""#));
        assert!(!json.contains(r#""dimension""#));
        let noncanonical = json.replace(r#""label":"Person""#, r#""label":" Person ""#);
        assert!(matches!(
            SearchManifest::from_json(Path::new("manifest.json"), noncanonical.as_bytes()),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
    }

    #[test]
    fn vector_manifest_requires_dimension_and_omits_properties() {
        let key = SearchArtifactKey::vector("Person", "semantic").unwrap();
        assert!(
            SearchManifest::for_key(&key, "exact-cosine-v1", "vector-v1", None, &source(), true)
                .is_err()
        );
        let manifest = SearchManifest::for_key(
            &key,
            "exact-cosine-v1",
            "vector-v1",
            Some(3),
            &source(),
            true,
        )
        .unwrap();
        let json = String::from_utf8(manifest.to_canonical_json().unwrap()).unwrap();
        assert!(json.contains(r#""dimension":3"#));
        assert!(json.contains(r#""space":"semantic""#));
        assert!(!json.contains(r#""properties""#));
        assert!(matches!(
            manifest.verify_fresh(&key, "exact-cosine-v1", "vector-v1", Some(4), &source(),),
            Err(SearchArtifactError::Stale { .. })
        ));
    }

    #[test]
    fn parsing_distinguishes_corrupt_incompatible_and_oversized() {
        let path = Path::new("manifest.json");
        assert!(matches!(
            SearchManifest::from_json(path, b"no"),
            Err(SearchArtifactError::CorruptManifest { .. })
        ));
        assert!(matches!(
            SearchManifest::from_json(path, br#"{"manifest_version":99}"#),
            Err(SearchArtifactError::IncompatibleManifest { found: 99, .. })
        ));
        assert!(matches!(
            SearchManifest::from_json(path, &vec![b' '; MAX_SEARCH_MANIFEST_BYTES + 1]),
            Err(SearchArtifactError::ResourceExhausted {
                resource: "manifest_bytes",
                ..
            })
        ));
    }

    #[test]
    fn freshness_checks_completion_versions_generation_and_fingerprint() {
        let key = SearchArtifactKey::text("Person", ["name"]).unwrap();
        let mut manifest =
            SearchManifest::for_key(&key, "tantivy-0.25", "text-v1", None, &source(), true)
                .unwrap();
        manifest
            .verify_fresh(&key, "tantivy-0.25", "text-v1", None, &source())
            .unwrap();

        manifest.completed = false;
        assert!(matches!(
            manifest.verify_fresh(&key, "tantivy-0.25", "text-v1", None, &source()),
            Err(SearchArtifactError::Stale { .. })
        ));
        manifest.completed = true;
        let changed = SearchSourceSnapshot {
            generation: 8,
            fingerprint: source().fingerprint,
        };
        assert!(
            manifest
                .verify_fresh(&key, "tantivy-0.25", "text-v1", None, &changed)
                .is_err()
        );
    }
}
