//! Bounded, read-only verification for portable project v2 packages.
//!
//! This module deliberately does not use a general tar decoder: portable-v2's
//! canonical bundle bytes are narrower than the formats those decoders accept.
#![allow(missing_docs)]

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path};
use std::sync::atomic::{AtomicBool, Ordering};
use unicode_normalization::UnicodeNormalization;

const MANIFEST_PATH: &str = "data/graphforge-project.json";
const BAGIT: &[u8] = b"BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n";
const BAG_INFO: &[u8] = b"Bag-Software-Agent: GraphForge portable-v2\nBagging-Date: 1970-01-01\n";

#[derive(Clone, Copy, Debug)]
pub struct PortableV2Limits {
    pub max_components: u64,
    pub max_entries: u64,
    pub max_entry_bytes: u64,
    pub max_total_bytes: u64,
    pub max_manifest_bytes: u64,
    pub max_tag_manifest_bytes: u64,
    pub max_path_bytes: usize,
    pub copy_buffer_bytes: usize,
}

impl Default for PortableV2Limits {
    fn default() -> Self {
        Self {
            max_components: 10_000,
            max_entries: 1_000_000,
            max_entry_bytes: 16 * 1024_u64.pow(4),
            max_total_bytes: 1024 * 1024_u64.pow(4),
            max_manifest_bytes: 16 * 1024 * 1024,
            max_tag_manifest_bytes: 4 * 1024 * 1024,
            max_path_bytes: 4096,
            copy_buffer_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PortableV2Mode {
    StructureOnly,
    Full,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableV2Representation {
    Expanded,
    Bundle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PortableV2PackageClass {
    Complete,
    OntologyOnly,
    ComponentSelective,
    GraphDataSubset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableV2Integrity {
    NotChecked,
    Verified,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableV2Compatibility {
    Supported,
    UnsupportedFuture,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableV2Authenticity {
    NotEvaluated,
    Unsigned,
    Verified,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PortableV2Report {
    pub contract: &'static str,
    pub representation: PortableV2Representation,
    pub package_digest: String,
    pub package_class: PortableV2PackageClass,
    pub component_count: u64,
    pub entry_count: u64,
    pub payload_bytes: u64,
    pub integrity: PortableV2Integrity,
    pub compatibility: PortableV2Compatibility,
    pub authenticity: PortableV2Authenticity,
    pub transport_digest: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PortableV2ErrorCode {
    Cancelled,
    LimitExceeded,
    Io,
    InvalidStructure,
    InvalidPath,
    DuplicateEntry,
    UnsupportedFuture,
    Incompatible,
    DigestMismatch,
    ConcurrentMutation,
}

#[derive(Debug)]
pub struct PortableV2Error {
    pub code: PortableV2ErrorCode,
    pub entry: Option<String>,
    detail: &'static str,
}

impl PortableV2Error {
    fn new(code: PortableV2ErrorCode, detail: &'static str) -> Self {
        Self {
            code,
            entry: None,
            detail,
        }
    }
    fn at(code: PortableV2ErrorCode, entry: &str, detail: &'static str) -> Self {
        Self {
            code,
            entry: Some(entry.chars().take(4096).collect()),
            detail,
        }
    }
}
impl fmt::Display for PortableV2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "portable-v2 {:?}: {}", self.code, self.detail)
    }
}
impl std::error::Error for PortableV2Error {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: String,
    package_digest: String,
    package_class: String,
    source_generation: SourceGeneration,
    selection: Selection,
    components: Vec<ManifestComponent>,
    requirements: Requirements,
    states: States,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceGeneration {
    generation_uuid: String,
    manifest_sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Selection {
    roots: Vec<String>,
    omissions: Vec<String>,
    redactions: Vec<String>,
    graph_subset: Option<GraphSubset>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphSubset {
    selector: String,
    closure: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestComponent {
    kind: String,
    participant_id: String,
    required_dependencies: Vec<String>,
    files: Vec<ManifestFile>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestFile {
    media_type: String,
    path: String,
    length: u64,
    sha256: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Requirements {
    capabilities: Vec<String>,
    dependency_rule: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct States {
    integrity: String,
    compatibility: String,
    authenticity: String,
}

struct Entry {
    path: String,
    length: u64,
    digest: [u8; 32],
    bytes: Option<Vec<u8>>,
}

/// Verify an expanded directory or canonical uncompressed bundle without mutation.
pub fn verify_portable_v2(
    source: impl AsRef<Path>,
    mode: PortableV2Mode,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableV2Report, PortableV2Error> {
    if limits.copy_buffer_bytes == 0 {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
            "copy buffer is zero",
        ));
    }
    check_cancel(cancelled)?;
    let source = source.as_ref();
    let metadata = fs::symlink_metadata(source)
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "source unavailable"))?;
    if metadata.file_type().is_symlink() {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "linked source",
        ));
    }
    if metadata.is_dir() {
        verify_expanded(source, mode, limits, cancelled)
    } else if metadata.is_file() {
        verify_bundle(source, mode, limits, cancelled)
    } else {
        Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "source is not a regular file or directory",
        ))
    }
}

fn verify_expanded(
    root: &Path,
    mode: PortableV2Mode,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableV2Report, PortableV2Error> {
    let mut paths = Vec::new();
    walk(root, root, &mut paths, limits, cancelled)?;
    paths.sort();
    validate_path_set(&paths)?;
    let mut entries = Vec::with_capacity(paths.len());
    let mut total = 0u64;
    for path in &paths {
        check_cancel(cancelled)?;
        let full = root.join(path);
        let before = fs::metadata(&full)
            .map_err(|_| PortableV2Error::at(PortableV2ErrorCode::Io, path, "cannot stat entry"))?;
        if !before.is_file() || before.file_type().is_symlink() {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                path,
                "non-regular entry",
            ));
        }
        if has_multiple_links(&before) {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                path,
                "hard-linked entry",
            ));
        }
        let length = before.len();
        enforce_length(path, length, &mut total, limits)?;
        let (digest, bytes) = hash_file(
            &full,
            path,
            length,
            limits.copy_buffer_bytes,
            retained(path),
            cancelled,
        )?;
        let after = fs::metadata(&full).map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::ConcurrentMutation,
                path,
                "entry disappeared",
            )
        })?;
        if !same_identity(&before, &after)
            || before.len() != after.len()
            || modified(&before) != modified(&after)
        {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::ConcurrentMutation,
                path,
                "entry changed",
            ));
        }
        entries.push(Entry {
            path: path.clone(),
            length,
            digest,
            bytes,
        });
    }
    let transport = expanded_transport(&entries)?;
    validate_package(
        &entries,
        PortableV2Representation::Expanded,
        mode,
        limits,
        Some(transport),
    )
}

fn walk(
    root: &Path,
    dir: &Path,
    out: &mut Vec<String>,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableV2Error> {
    check_cancel(cancelled)?;
    let mut children = fs::read_dir(dir)
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "cannot read directory"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::Io, "cannot read directory entry")
        })?;
    children.sort_by_key(std::fs::DirEntry::file_name);
    for child in children {
        check_cancel(cancelled)?;
        let ty = child
            .file_type()
            .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "cannot inspect entry"))?;
        let relative = child
            .path()
            .strip_prefix(root)
            .map_err(|_| {
                PortableV2Error::new(PortableV2ErrorCode::InvalidPath, "entry escaped root")
            })?
            .to_str()
            .ok_or_else(|| {
                PortableV2Error::new(PortableV2ErrorCode::InvalidPath, "non-UTF-8 path")
            })?
            .replace(std::path::MAIN_SEPARATOR, "/");
        validate_path(&relative, limits.max_path_bytes)?;
        if ty.is_symlink() || (!ty.is_file() && !ty.is_dir()) {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                &relative,
                "link or special entry",
            ));
        }
        if ty.is_dir() {
            walk(root, &child.path(), out, limits, cancelled)?;
        } else {
            out.push(relative);
            if out.len() as u64 > limits.max_entries {
                return Err(PortableV2Error::new(
                    PortableV2ErrorCode::LimitExceeded,
                    "entry count",
                ));
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn verify_bundle(
    path: &Path,
    mode: PortableV2Mode,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableV2Report, PortableV2Error> {
    let before = fs::metadata(path)
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "cannot stat bundle"))?;
    if has_multiple_links(&before) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "hard-linked bundle",
        ));
    }
    let mut file = File::open(path)
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "cannot open bundle"))?;
    let mut transport = Sha256::new();
    let mut entries = Vec::new();
    let mut total = 0u64;
    let mut pending_pax: Option<(String, String)> = None;
    loop {
        check_cancel(cancelled)?;
        let mut header = [0u8; 512];
        read_exact_hash(&mut file, &mut header, &mut transport, "bundle header")?;
        if header == [0; 512] {
            let mut second = [1u8; 512];
            read_exact_hash(&mut file, &mut second, &mut transport, "second end block")?;
            if second != [0; 512] {
                return Err(PortableV2Error::new(
                    PortableV2ErrorCode::InvalidStructure,
                    "invalid end marker",
                ));
            }
            let mut extra = [0u8; 1];
            if file
                .read(&mut extra)
                .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "bundle read"))?
                != 0
            {
                return Err(PortableV2Error::new(
                    PortableV2ErrorCode::InvalidStructure,
                    "trailing bytes",
                ));
            }
            break;
        }
        verify_header(&header)?;
        let kind = header[156];
        let size = parse_octal(&header[124..136])?;
        let raw_path = header_path(&header)?;
        if kind == b'x' {
            if pending_pax.is_some() || !raw_path.starts_with("PaxHeaders/") {
                return Err(PortableV2Error::new(
                    PortableV2ErrorCode::InvalidStructure,
                    "invalid PAX sequence",
                ));
            }
            let bytes = read_payload(
                &mut file,
                size,
                &mut transport,
                limits.max_path_bytes + 32,
                cancelled,
            )?;
            let text = std::str::from_utf8(&bytes).map_err(|_| {
                PortableV2Error::new(PortableV2ErrorCode::InvalidPath, "PAX path is not UTF-8")
            })?;
            let pax_path = parse_pax(text)?;
            let suffix = &hex(&Sha256::digest(pax_path.as_bytes()))[..16];
            if raw_path != format!("PaxHeaders/{suffix}") {
                return Err(PortableV2Error::new(
                    PortableV2ErrorCode::InvalidStructure,
                    "non-canonical PAX header name",
                ));
            }
            pending_pax = Some((pax_path, suffix.to_owned()));
            continue;
        }
        if kind != b'0' {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::InvalidStructure,
                "non-regular tar entry",
            ));
        }
        let used_pax = pending_pax.is_some();
        let entry_path = if let Some((path, suffix)) = pending_pax.take() {
            if raw_path != format!("PaxFiles/{suffix}") {
                return Err(PortableV2Error::at(
                    PortableV2ErrorCode::InvalidStructure,
                    &path,
                    "non-canonical PAX placeholder",
                ));
            }
            path
        } else {
            raw_path.clone()
        };
        verify_canonical_header_path(&header, &entry_path, used_pax)?;
        validate_path(&entry_path, limits.max_path_bytes)?;
        enforce_length(&entry_path, size, &mut total, limits)?;
        let (digest, bytes) = hash_payload(
            &mut file,
            size,
            &mut transport,
            limits.copy_buffer_bytes,
            retained(&entry_path),
            cancelled,
        )?;
        entries.push(Entry {
            path: entry_path,
            length: size,
            digest,
            bytes,
        });
        if entries.len() as u64 > limits.max_entries {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::LimitExceeded,
                "entry count",
            ));
        }
    }
    if pending_pax.is_some() {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "orphan PAX header",
        ));
    }
    if entries.windows(2).any(|w| w[0].path >= w[1].path) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "bundle entries are not canonical order",
        ));
    }
    validate_path_set(&entries.iter().map(|e| e.path.clone()).collect::<Vec<_>>())?;
    let after = file.metadata().map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::ConcurrentMutation,
            "bundle disappeared",
        )
    })?;
    if !same_identity(&before, &after)
        || before.len() != after.len()
        || modified(&before) != modified(&after)
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::ConcurrentMutation,
            "bundle changed",
        ));
    }
    validate_package(
        &entries,
        PortableV2Representation::Bundle,
        mode,
        limits,
        Some(hex(&transport.finalize())),
    )
}

fn validate_package(
    entries: &[Entry],
    representation: PortableV2Representation,
    mode: PortableV2Mode,
    limits: PortableV2Limits,
    transport: Option<String>,
) -> Result<PortableV2Report, PortableV2Error> {
    let map: BTreeMap<_, _> = entries.iter().map(|e| (e.path.as_str(), e)).collect();
    require_exact(&map, "bagit.txt", BAGIT)?;
    require_exact(&map, "bag-info.txt", BAG_INFO)?;
    let manifest_entry = map.get(MANIFEST_PATH).ok_or_else(|| {
        PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            MANIFEST_PATH,
            "missing semantic manifest",
        )
    })?;
    if manifest_entry.length > limits.max_manifest_bytes {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::LimitExceeded,
            MANIFEST_PATH,
            "manifest size",
        ));
    }
    let manifest_bytes = read_entry_bytes(entries, MANIFEST_PATH)?;
    let (manifest, canonical_without_digest) = parse_manifest(&manifest_bytes, limits)?;
    let expected = format!(
        "sha256:{}",
        hex(&Sha256::digest(
            [
                b"graphforge-project/2\0".as_slice(),
                canonical_without_digest.as_slice()
            ]
            .concat()
        ))
    );
    if !constant_time_eq(expected.as_bytes(), manifest.package_digest.as_bytes()) {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::DigestMismatch,
            MANIFEST_PATH,
            "package digest",
        ));
    }
    validate_semantics(&manifest, limits)?;
    validate_bag_manifests(&map, &manifest)?;
    let full = mode == PortableV2Mode::Full;
    Ok(PortableV2Report {
        contract: "graphforge-portable-verify/2",
        representation,
        package_digest: manifest.package_digest.clone(),
        package_class: package_class(&manifest.package_class)?,
        component_count: manifest.components.len() as u64,
        entry_count: entries.len() as u64,
        payload_bytes: entries.iter().map(|e| e.length).sum(),
        integrity: if full {
            PortableV2Integrity::Verified
        } else {
            PortableV2Integrity::NotChecked
        },
        compatibility: PortableV2Compatibility::Supported,
        authenticity: PortableV2Authenticity::Unsigned,
        transport_digest: transport.map(|x| format!("sha256:{x}")),
    })
}

// Verification retains no payloads. For the small semantic/tag records, callers
// provide their bytes through this per-entry cache in the next writer/verifier
// integration. Expanded and bundle readers currently re-open/seek them below.
fn read_entry_bytes(entries: &[Entry], path: &str) -> Result<Vec<u8>, PortableV2Error> {
    entries
        .iter()
        .find(|e| e.path == path)
        .and_then(|e| e.bytes.clone())
        .ok_or_else(|| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                path,
                "tag bytes unavailable",
            )
        })
}

fn parse_manifest(
    bytes: &[u8],
    _limits: PortableV2Limits,
) -> Result<(Manifest, Vec<u8>), PortableV2Error> {
    let value = UniqueValue::deserialize(&mut serde_json::Deserializer::from_slice(bytes))
        .map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                MANIFEST_PATH,
                "invalid JSON",
            )
        })?
        .0;
    if serde_json::to_vec(&value).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "manifest canonicalization",
        )
    })? != bytes
    {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            MANIFEST_PATH,
            "manifest is not JCS canonical",
        ));
    }
    let mut without = value.clone();
    without
        .as_object_mut()
        .ok_or_else(|| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                MANIFEST_PATH,
                "manifest is not object",
            )
        })?
        .remove("package_digest");
    let canonical_without = serde_json::to_vec(&without).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            MANIFEST_PATH,
            "cannot canonicalize manifest",
        )
    })?;
    let manifest: Manifest = serde_json::from_value(value).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            MANIFEST_PATH,
            "manifest schema",
        )
    })?;
    Ok((manifest, canonical_without))
}

#[allow(clippy::too_many_lines)]
fn validate_semantics(m: &Manifest, limits: PortableV2Limits) -> Result<(), PortableV2Error> {
    if m.format != "graphforge-project/2" {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::UnsupportedFuture,
            "unsupported format",
        ));
    }
    if m.components.len() as u64 > limits.max_components {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
            "component count",
        ));
    }
    if m.requirements.dependency_rule != "required-transitive-closure/1" {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::UnsupportedFuture,
            "dependency rule",
        ));
    }
    let supported = [
        "ontology@1",
        "schema@1",
        "migration@1",
        "settings@1",
        "graph-data@1",
        "derived-artifact@1",
        "evidence@1",
        "provenance@1",
        "compatibility@1",
    ];
    if m.requirements
        .capabilities
        .iter()
        .any(|c| !supported.contains(&c.as_str()))
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::UnsupportedFuture,
            "capability",
        ));
    }
    unique_sorted(&m.requirements.capabilities, "capabilities")?;
    let mut ids = BTreeSet::new();
    let mut paths = BTreeSet::new();
    let mut last = None;
    for c in &m.components {
        let key = (&c.kind, &c.participant_id);
        if last >= Some(key) {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "component order",
            ));
        }
        last = Some(key);
        if !valid_kind(&c.kind)
            || !valid_id(&c.participant_id)
            || !ids.insert(c.participant_id.as_str())
        {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "component identity",
            ));
        }
        unique_sorted(&c.required_dependencies, "dependencies")?;
        let mut previous = None;
        for f in &c.files {
            validate_path(&f.path, limits.max_path_bytes)?;
            if previous >= Some(&f.path) || !paths.insert(f.path.as_str()) {
                return Err(PortableV2Error::at(
                    PortableV2ErrorCode::DuplicateEntry,
                    &f.path,
                    "file descriptor order/duplicate",
                ));
            }
            previous = Some(&f.path);
            if f.length > limits.max_entry_bytes || !sha(&f.sha256) || !valid_media(&f.media_type) {
                return Err(PortableV2Error::at(
                    PortableV2ErrorCode::Incompatible,
                    &f.path,
                    "file descriptor",
                ));
            }
        }
    }
    for c in &m.components {
        for d in &c.required_dependencies {
            if d == &c.participant_id || !ids.contains(d.as_str()) {
                return Err(PortableV2Error::new(
                    PortableV2ErrorCode::Incompatible,
                    "dependency closure",
                ));
            }
        }
    }
    detect_cycles(&m.components)?;
    for values in [
        &m.selection.roots,
        &m.selection.omissions,
        &m.selection.redactions,
    ] {
        unique_sorted(values, "selection")?;
    }
    if m.selection
        .roots
        .iter()
        .any(|id| !ids.contains(id.as_str()))
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "unknown selection root",
        ));
    }
    let generation = uuid::Uuid::parse_str(&m.source_generation.generation_uuid).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Incompatible, "source generation UUID")
    })?;
    if generation.is_nil() || !sha(&m.source_generation.manifest_sha256) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "source generation identity",
        ));
    }
    if m.states.integrity != "verified"
        || m.states.compatibility != "supported"
        || !matches!(m.states.authenticity.as_str(), "unsigned" | "not_evaluated")
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "manifest state declaration",
        ));
    }
    match (&m.package_class[..], &m.selection.graph_subset) {
        ("graph-data-subset", Some(s))
            if !s.selector.is_empty()
                && matches!(
                    s.closure.as_str(),
                    "selected-only" | "induced-edges" | "referential"
                ) => {}
        ("graph-data-subset", _) | (_, Some(_)) => {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "graph subset/class mismatch",
            ));
        }
        _ => {}
    }
    match m.package_class.as_str() {
        "complete" if !m.selection.omissions.is_empty() || !m.selection.redactions.is_empty() => {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "complete package has omissions/redactions",
            ));
        }
        "ontology-only"
            if m.components
                .iter()
                .any(|component| component.kind != "ontology") =>
        {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "ontology-only package contains another kind",
            ));
        }
        "graph-data-subset"
            if !m
                .components
                .iter()
                .any(|component| component.kind == "graph-data") =>
        {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::Incompatible,
                "graph subset has no graph-data component",
            ));
        }
        _ => {}
    }
    package_class(&m.package_class)?;
    Ok(())
}

fn validate_bag_manifests(
    map: &BTreeMap<&str, &Entry>,
    manifest: &Manifest,
) -> Result<(), PortableV2Error> {
    let declared: BTreeMap<_, _> = manifest
        .components
        .iter()
        .flat_map(|c| &c.files)
        .map(|f| (f.path.as_str(), (f.length, f.sha256.as_str())))
        .collect();
    for (path, (length, digest)) in declared {
        let entry = map.get(path).ok_or_else(|| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                path,
                "declared payload missing",
            )
        })?;
        if entry.length != length
            || !constant_time_eq(hex(&entry.digest).as_bytes(), digest.as_bytes())
        {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::DigestMismatch,
                path,
                "payload digest/length",
            ));
        }
    }
    for path in map.keys().filter(|p| p.starts_with("data/components/")) {
        if !manifest
            .components
            .iter()
            .flat_map(|c| &c.files)
            .any(|f| f.path == **path)
        {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                path,
                "extra payload",
            ));
        }
    }
    for required in ["manifest-sha256.txt", "tagmanifest-sha256.txt"] {
        if !map.contains_key(required) {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                required,
                "missing tag manifest",
            ));
        }
    }
    let data_manifest = parse_digest_manifest(
        &read_entry_bytes_from_map(map, "manifest-sha256.txt")?,
        "manifest-sha256.txt",
    )?;
    let expected_data: BTreeMap<_, _> = map
        .iter()
        .filter(|(p, _)| p.starts_with("data/"))
        .map(|(p, e)| ((*p).to_owned(), hex(&e.digest)))
        .collect();
    if data_manifest != expected_data {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::DigestMismatch,
            "manifest-sha256.txt",
            "data inventory manifest",
        ));
    }
    let tag_manifest = parse_digest_manifest(
        &read_entry_bytes_from_map(map, "tagmanifest-sha256.txt")?,
        "tagmanifest-sha256.txt",
    )?;
    let expected_tags: BTreeMap<_, _> = ["bag-info.txt", "bagit.txt", "manifest-sha256.txt"]
        .into_iter()
        .map(|p| (p.to_owned(), hex(&map[p].digest)))
        .collect();
    if tag_manifest != expected_tags {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::DigestMismatch,
            "tagmanifest-sha256.txt",
            "tag inventory manifest",
        ));
    }
    let mut allowed: BTreeSet<String> = expected_data
        .keys()
        .chain(expected_tags.keys())
        .cloned()
        .collect();
    allowed.insert("tagmanifest-sha256.txt".to_owned());
    if map.keys().any(|p| !allowed.contains(*p)) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "unmanifested extra entry",
        ));
    }
    Ok(())
}

fn read_entry_bytes_from_map(
    map: &BTreeMap<&str, &Entry>,
    path: &str,
) -> Result<Vec<u8>, PortableV2Error> {
    map.get(path).and_then(|e| e.bytes.clone()).ok_or_else(|| {
        PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            path,
            "tag bytes unavailable",
        )
    })
}

fn parse_digest_manifest(
    bytes: &[u8],
    entry: &str,
) -> Result<BTreeMap<String, String>, PortableV2Error> {
    if bytes.last() != Some(&b'\n') {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            entry,
            "tag manifest termination",
        ));
    }
    let text = std::str::from_utf8(bytes).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            entry,
            "tag manifest UTF-8",
        )
    })?;
    let mut result = BTreeMap::new();
    let mut previous: Option<&str> = None;
    for line in text.lines() {
        let (digest, path) = line.split_once("  ").ok_or_else(|| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                entry,
                "tag manifest record",
            )
        })?;
        validate_path(path, 4096)?;
        if !sha(digest)
            || previous >= Some(path)
            || result.insert(path.to_owned(), digest.to_owned()).is_some()
        {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                entry,
                "tag manifest order/duplicate",
            ));
        }
        previous = Some(path);
    }
    Ok(result)
}

fn expanded_transport(entries: &[Entry]) -> Result<String, PortableV2Error> {
    let mut hash = Sha256::new();
    hash.update(b"graphforge-expanded/2\0");
    for e in entries
        .iter()
        .filter(|e| e.path != "tagmanifest-sha256.txt")
    {
        hash.update((e.path.len() as u64).to_be_bytes());
        hash.update(e.path.as_bytes());
        hash.update(e.length.to_be_bytes());
        hash.update(e.digest);
    }
    hash.update(read_entry_bytes(entries, "tagmanifest-sha256.txt")?);
    Ok(hex(&hash.finalize()))
}

fn require_exact(
    map: &BTreeMap<&str, &Entry>,
    path: &str,
    expected: &[u8],
) -> Result<(), PortableV2Error> {
    let e = map.get(path).ok_or_else(|| {
        PortableV2Error::at(PortableV2ErrorCode::InvalidStructure, path, "missing tag")
    })?;
    if e.length != expected.len() as u64 || e.digest != Sha256::digest(expected)[..] {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::DigestMismatch,
            path,
            "tag bytes",
        ));
    }
    Ok(())
}
fn check_cancel(c: Option<&AtomicBool>) -> Result<(), PortableV2Error> {
    if c.is_some_and(|x| x.load(Ordering::Relaxed)) {
        Err(PortableV2Error::new(
            PortableV2ErrorCode::Cancelled,
            "cancelled",
        ))
    } else {
        Ok(())
    }
}
fn enforce_length(
    path: &str,
    length: u64,
    total: &mut u64,
    limits: PortableV2Limits,
) -> Result<(), PortableV2Error> {
    if length > limits.max_entry_bytes {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::LimitExceeded,
            path,
            "entry size",
        ));
    }
    *total = total.checked_add(length).ok_or_else(|| {
        PortableV2Error::new(PortableV2ErrorCode::LimitExceeded, "total overflow")
    })?;
    if *total > limits.max_total_bytes {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
            "declared total",
        ));
    }
    Ok(())
}
fn hash_file(
    path: &Path,
    entry: &str,
    length: u64,
    buffer: usize,
    retain: bool,
    cancelled: Option<&AtomicBool>,
) -> Result<([u8; 32], Option<Vec<u8>>), PortableV2Error> {
    let mut f = File::open(path)
        .map_err(|_| PortableV2Error::at(PortableV2ErrorCode::Io, entry, "cannot open entry"))?;
    let mut h = Sha256::new();
    let mut kept = if retain {
        Some(Vec::with_capacity(usize::try_from(length).map_err(
            |_| {
                PortableV2Error::at(
                    PortableV2ErrorCode::LimitExceeded,
                    entry,
                    "retained tag does not fit address space",
                )
            },
        )?))
    } else {
        None
    };
    let mut left = length;
    let mut b = vec![0u8; buffer];
    while left > 0 {
        check_cancel(cancelled)?;
        let n = f
            .read(&mut b[..usize::try_from(left.min(buffer as u64)).unwrap()])
            .map_err(|_| {
                PortableV2Error::at(PortableV2ErrorCode::Io, entry, "cannot read entry")
            })?;
        if n == 0 {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::ConcurrentMutation,
                entry,
                "entry truncated",
            ));
        }
        h.update(&b[..n]);
        if let Some(bytes) = &mut kept {
            bytes.extend_from_slice(&b[..n]);
        }
        left -= n as u64;
    }
    Ok((h.finalize().into(), kept))
}
fn modified(m: &fs::Metadata) -> Option<std::time::SystemTime> {
    m.modified().ok()
}
#[cfg(unix)]
fn has_multiple_links(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.nlink() > 1
}
#[cfg(not(unix))]
fn has_multiple_links(_: &fs::Metadata) -> bool {
    false
}
#[cfg(unix)]
fn same_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}
#[cfg(not(unix))]
fn same_identity(_: &fs::Metadata, _: &fs::Metadata) -> bool {
    true
}
fn validate_path(path: &str, max: usize) -> Result<(), PortableV2Error> {
    if path.is_empty()
        || path.len() > max
        || path
            .as_bytes()
            .iter()
            .any(|b| *b == 0 || *b < 0x20 || *b == 0x7f)
        || path.contains('\\')
        || path.nfc().collect::<String>() != path
        || Path::new(path).is_absolute()
        || Path::new(path)
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::InvalidPath,
            path,
            "unsafe/non-canonical path",
        ));
    }
    Ok(())
}
fn validate_path_set(paths: &[String]) -> Result<(), PortableV2Error> {
    let mut exact = BTreeSet::new();
    let mut folded = BTreeSet::new();
    for p in paths {
        if !exact.insert(p) || !folded.insert(p.to_lowercase()) {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::DuplicateEntry,
                p,
                "duplicate/case-fold collision",
            ));
        }
    }
    Ok(())
}
fn read_exact_hash(
    r: &mut File,
    b: &mut [u8],
    h: &mut Sha256,
    detail: &'static str,
) -> Result<(), PortableV2Error> {
    r.read_exact(b)
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, detail))?;
    h.update(b);
    Ok(())
}
fn parse_octal(field: &[u8]) -> Result<u64, PortableV2Error> {
    if field.first().is_some_and(|b| b & 0x80 != 0) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "base-256 numeric field",
        ));
    }
    let s = std::str::from_utf8(field)
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, "numeric field"))?
        .trim_matches(['\0', ' ']);
    u64::from_str_radix(if s.is_empty() { "0" } else { s }, 8)
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, "numeric field"))
}
fn cstr(field: &[u8]) -> Result<&str, PortableV2Error> {
    let end = field.iter().position(|b| *b == 0).unwrap_or(field.len());
    std::str::from_utf8(&field[..end])
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::InvalidPath, "tar path UTF-8"))
}
fn header_path(h: &[u8; 512]) -> Result<String, PortableV2Error> {
    let n = cstr(&h[..100])?;
    let p = cstr(&h[345..500])?;
    Ok(if p.is_empty() {
        n.into()
    } else {
        format!("{p}/{n}")
    })
}
fn verify_canonical_header_path(
    h: &[u8; 512],
    path: &str,
    used_pax: bool,
) -> Result<(), PortableV2Error> {
    if used_pax {
        return Ok(());
    }
    let (prefix, name) = canonical_ustar_split(path).ok_or_else(|| {
        PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            path,
            "missing required PAX header",
        )
    })?;
    if cstr(&h[..100])? != name || cstr(&h[345..500])? != prefix {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            path,
            "non-canonical ustar path split",
        ));
    }
    Ok(())
}
fn canonical_ustar_split(path: &str) -> Option<(&str, &str)> {
    if path.len() <= 100 {
        return Some(("", path));
    }
    path.match_indices('/')
        .filter_map(|(i, _)| {
            let (p, n) = path.split_at(i);
            let n = &n[1..];
            (p.len() <= 155 && n.len() <= 100).then_some((p, n))
        })
        .next_back()
}
fn verify_header(h: &[u8; 512]) -> Result<(), PortableV2Error> {
    if &h[257..263] != b"ustar\0"
        || &h[263..265] != b"00"
        || parse_octal(&h[100..108])? != 0o644
        || parse_octal(&h[108..116])? != 0
        || parse_octal(&h[116..124])? != 0
        || parse_octal(&h[136..148])? != 0
        || h[265..329].iter().any(|b| *b != 0)
    {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "non-canonical tar header",
        ));
    }
    let expected = parse_octal(&h[148..156])?;
    let actual = h
        .iter()
        .enumerate()
        .map(|(i, b)| {
            if (148..156).contains(&i) {
                u64::from(b' ')
            } else {
                u64::from(*b)
            }
        })
        .sum::<u64>();
    if actual != expected {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::DigestMismatch,
            "tar header checksum",
        ));
    }
    Ok(())
}
fn read_payload(
    r: &mut File,
    size: u64,
    h: &mut Sha256,
    max: usize,
    c: Option<&AtomicBool>,
) -> Result<Vec<u8>, PortableV2Error> {
    if size > max as u64 {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
            "PAX record",
        ));
    }
    let allocation = usize::try_from(size).map_err(|_| {
        PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
            "PAX record does not fit address space",
        )
    })?;
    let mut v = vec![0; allocation];
    read_exact_hash(r, &mut v, h, "truncated payload")?;
    read_padding(r, size, h)?;
    check_cancel(c)?;
    Ok(v)
}
fn hash_payload(
    reader: &mut File,
    size: u64,
    transport_hash: &mut Sha256,
    buffer: usize,
    retain: bool,
    cancelled: Option<&AtomicBool>,
) -> Result<([u8; 32], Option<Vec<u8>>), PortableV2Error> {
    let mut payload_hash = Sha256::new();
    let mut kept = if retain {
        Some(Vec::with_capacity(usize::try_from(size).map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::LimitExceeded,
                "retained tag does not fit address space",
            )
        })?))
    } else {
        None
    };
    let mut left = size;
    let mut copy_buffer = vec![0; buffer];
    while left > 0 {
        check_cancel(cancelled)?;
        let chunk_len = usize::try_from(left.min(buffer as u64)).unwrap();
        read_exact_hash(
            reader,
            &mut copy_buffer[..chunk_len],
            transport_hash,
            "truncated payload",
        )?;
        payload_hash.update(&copy_buffer[..chunk_len]);
        if let Some(v) = &mut kept {
            v.extend_from_slice(&copy_buffer[..chunk_len]);
        }
        left -= chunk_len as u64;
    }
    read_padding(reader, size, transport_hash)?;
    Ok((payload_hash.finalize().into(), kept))
}
fn read_padding(r: &mut File, size: u64, h: &mut Sha256) -> Result<(), PortableV2Error> {
    let n = (512 - size % 512) % 512;
    let mut p = vec![0; n as usize];
    read_exact_hash(r, &mut p, h, "truncated padding")?;
    if p.iter().any(|b| *b != 0) {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "non-zero padding",
        ));
    }
    Ok(())
}
fn parse_pax(s: &str) -> Result<String, PortableV2Error> {
    let space = s
        .find(' ')
        .ok_or_else(|| PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, "PAX record"))?;
    let n = s[..space]
        .parse::<usize>()
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::InvalidStructure, "PAX length"))?;
    if n != s.len() || !s.ends_with('\n') || !s[space + 1..].starts_with("path=") {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::InvalidStructure,
            "PAX record",
        ));
    }
    Ok(s[space + 6..s.len() - 1].into())
}
fn package_class(s: &str) -> Result<PortableV2PackageClass, PortableV2Error> {
    match s {
        "complete" => Ok(PortableV2PackageClass::Complete),
        "ontology-only" => Ok(PortableV2PackageClass::OntologyOnly),
        "component-selective" => Ok(PortableV2PackageClass::ComponentSelective),
        "graph-data-subset" => Ok(PortableV2PackageClass::GraphDataSubset),
        _ => Err(PortableV2Error::new(
            PortableV2ErrorCode::UnsupportedFuture,
            "package class",
        )),
    }
}
fn valid_kind(s: &str) -> bool {
    matches!(
        s,
        "ontology"
            | "schema"
            | "migration"
            | "settings"
            | "graph-data"
            | "derived-artifact"
            | "evidence"
            | "provenance"
            | "compatibility"
    )
}
fn valid_id(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 256
        && s.bytes().next().is_some_and(|b| b.is_ascii_lowercase())
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
}
fn sha(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn valid_media(s: &str) -> bool {
    s.len() <= 255
        && s.split_once('/').is_some()
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"!#$&^_.+-/".contains(&b))
}
fn unique_sorted(v: &[String], _: &'static str) -> Result<(), PortableV2Error> {
    if v.windows(2).any(|w| w[0] >= w[1]) {
        Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "unordered/duplicate list",
        ))
    } else {
        Ok(())
    }
}
fn detect_cycles(c: &[ManifestComponent]) -> Result<(), PortableV2Error> {
    fn visit<'a>(
        id: &'a str,
        m: &BTreeMap<&'a str, &'a ManifestComponent>,
        vis: &mut BTreeSet<&'a str>,
        done: &mut BTreeSet<&'a str>,
    ) -> bool {
        if done.contains(id) {
            return false;
        }
        if !vis.insert(id) {
            return true;
        }
        let cycle = m.get(id).is_some_and(|x| {
            x.required_dependencies
                .iter()
                .any(|d| visit(d, m, vis, done))
        });
        vis.remove(id);
        done.insert(id);
        cycle
    }
    let m = c.iter().map(|x| (x.participant_id.as_str(), x)).collect();
    let mut v = BTreeSet::new();
    let mut d = BTreeSet::new();
    if c.iter()
        .any(|x| visit(&x.participant_id, &m, &mut v, &mut d))
    {
        Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "dependency cycle",
        ))
    } else {
        Ok(())
    }
}
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |x, (a, b)| x | (a ^ b)) == 0
}
fn hex(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(H[(b >> 4) as usize] as char);
        s.push(H[(b & 15) as usize] as char);
    }
    s
}
fn retained(path: &str) -> bool {
    matches!(
        path,
        "bagit.txt"
            | "bag-info.txt"
            | "manifest-sha256.txt"
            | "tagmanifest-sha256.txt"
            | MANIFEST_PATH
    )
}

struct UniqueValue(Value);
impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(UniqueVisitor)
    }
}
struct UniqueVisitor;
impl<'de> Visitor<'de> for UniqueVisitor {
    type Value = UniqueValue;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JSON value without duplicate object members")
    }
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Bool(v)))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(v.into())))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Number(v.into())))
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
        serde_json::Number::from_f64(v)
            .map(Value::Number)
            .map(UniqueValue)
            .ok_or_else(|| E::custom("non-finite number"))
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(v.into())))
    }
    fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::String(v)))
    }
    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }
    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(UniqueValue(Value::Null))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
        let mut v = Vec::new();
        while let Some(x) = a.next_element::<UniqueValue>()? {
            v.push(x.0);
        }
        Ok(UniqueValue(Value::Array(v)))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Self::Value, A::Error> {
        let mut m = serde_json::Map::new();
        while let Some(k) = a.next_key::<String>()? {
            let v = a.next_value::<UniqueValue>()?;
            if m.insert(k, v.0).is_some() {
                return Err(de::Error::custom("duplicate object member"));
            }
        }
        Ok(UniqueValue(Value::Object(m)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    fn package() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        let mut value: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/portable-v2/ontology-only.manifest.json"
        ))
        .unwrap();
        value.as_object_mut().unwrap().remove("package_digest");
        let semantic = serde_json::to_vec(&value).unwrap();
        let digest = hex(&Sha256::digest(
            [b"graphforge-project/2\0".as_slice(), semantic.as_slice()].concat(),
        ));
        value.as_object_mut().unwrap().insert(
            "package_digest".into(),
            Value::String(format!("sha256:{digest}")),
        );
        let manifest = serde_json::to_vec(&value).unwrap();
        let payload_path = "data/components/ontology/core-ontology/ontology.json";
        let manifest_path = root.path().join(MANIFEST_PATH);
        fs::create_dir_all(manifest_path.parent().unwrap()).unwrap();
        fs::write(&manifest_path, &manifest).unwrap();
        let payload = root.path().join(payload_path);
        fs::create_dir_all(payload.parent().unwrap()).unwrap();
        fs::write(payload, b"{}").unwrap();
        fs::write(root.path().join("bagit.txt"), BAGIT).unwrap();
        fs::write(root.path().join("bag-info.txt"), BAG_INFO).unwrap();
        let data_manifest = format!(
            "{}  {}\n{}  {}\n",
            hex(&Sha256::digest(b"{}")),
            payload_path,
            hex(&Sha256::digest(&manifest)),
            MANIFEST_PATH
        );
        fs::write(root.path().join("manifest-sha256.txt"), &data_manifest).unwrap();
        let tag = format!(
            "{}  bag-info.txt\n{}  bagit.txt\n{}  manifest-sha256.txt\n",
            hex(&Sha256::digest(BAG_INFO)),
            hex(&Sha256::digest(BAGIT)),
            hex(&Sha256::digest(data_manifest.as_bytes()))
        );
        fs::write(root.path().join("tagmanifest-sha256.txt"), tag).unwrap();
        root
    }

    #[test]
    fn expanded_full_and_structure_only_have_honest_distinct_integrity() {
        let root = package();
        let full = verify_portable_v2(
            root.path(),
            PortableV2Mode::Full,
            PortableV2Limits::default(),
            None,
        )
        .unwrap();
        let structure = verify_portable_v2(
            root.path(),
            PortableV2Mode::StructureOnly,
            PortableV2Limits::default(),
            None,
        )
        .unwrap();
        assert_eq!(
            full.package_digest,
            "sha256:869da25f99c90864c321bf8c42aa3f1f3642c877b92bc34255c900d3083a525d"
        );
        assert_eq!(full.integrity, PortableV2Integrity::Verified);
        assert_eq!(structure.integrity, PortableV2Integrity::NotChecked);
        assert_eq!(full.entry_count, 6);
        assert!(full.transport_digest.is_some());
    }

    #[test]
    fn changed_payload_fails_at_bounded_relative_entry() {
        let root = package();
        let path = "data/components/ontology/core-ontology/ontology.json";
        fs::write(root.path().join(path), b"[]").unwrap();
        let error = verify_portable_v2(
            root.path(),
            PortableV2Mode::Full,
            PortableV2Limits::default(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::DigestMismatch);
        assert_eq!(error.entry.as_deref(), Some(path));
    }

    #[test]
    fn cancellation_and_limits_fail_before_success() {
        let root = package();
        let cancelled = AtomicBool::new(true);
        let error = verify_portable_v2(
            root.path(),
            PortableV2Mode::Full,
            PortableV2Limits::default(),
            Some(&cancelled),
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Cancelled);
        let limits = PortableV2Limits {
            max_entries: 2,
            ..PortableV2Limits::default()
        };
        let error =
            verify_portable_v2(root.path(), PortableV2Mode::Full, limits, None).unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::LimitExceeded);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_and_case_fold_collision_fail_closed() {
        use std::os::unix::fs::symlink;
        let root = package();
        symlink(root.path().join("bagit.txt"), root.path().join("linked")).unwrap();
        let error = verify_portable_v2(
            root.path(),
            PortableV2Mode::Full,
            PortableV2Limits::default(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::InvalidStructure);
        fs::remove_file(root.path().join("linked")).unwrap();
        fs::write(root.path().join("BAGIT.TXT"), BAGIT).unwrap();
        if fs::read_dir(root.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().to_lowercase() == "bagit.txt")
            .count()
            < 2
        {
            return;
        }
        let error = verify_portable_v2(
            root.path(),
            PortableV2Mode::Full,
            PortableV2Limits::default(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::DuplicateEntry);
    }

    #[test]
    fn duplicate_json_members_are_rejected() {
        let duplicate = br#"{"a":1,"a":2}"#;
        assert!(
            UniqueValue::deserialize(&mut serde_json::Deserializer::from_slice(duplicate)).is_err()
        );
    }

    fn octal(field: &mut [u8], value: u64) {
        field.fill(0);
        let digits = format!("{:0width$o}", value, width = field.len() - 1);
        field[..digits.len()].copy_from_slice(digits.as_bytes());
    }
    fn tar_entry(path: &str, payload: &[u8]) -> Vec<u8> {
        let mut h = [0u8; 512];
        let (prefix, name) = canonical_ustar_split(path).unwrap();
        h[..name.len()].copy_from_slice(name.as_bytes());
        h[345..345 + prefix.len()].copy_from_slice(prefix.as_bytes());
        octal(&mut h[100..108], 0o644);
        octal(&mut h[108..116], 0);
        octal(&mut h[116..124], 0);
        octal(&mut h[124..136], payload.len() as u64);
        octal(&mut h[136..148], 0);
        h[148..156].fill(b' ');
        h[156] = b'0';
        h[257..263].copy_from_slice(b"ustar\0");
        h[263..265].copy_from_slice(b"00");
        let sum: u64 = h.iter().map(|b| *b as u64).sum();
        let checksum = format!("{:06o}\0 ", sum);
        h[148..156].copy_from_slice(checksum.as_bytes());
        let mut out = h.to_vec();
        out.extend_from_slice(payload);
        out.resize(out.len() + ((512 - payload.len() % 512) % 512), 0);
        out
    }

    #[test]
    fn equivalent_bundle_reports_same_semantic_identity() {
        let root = package();
        let mut paths = Vec::new();
        walk(
            root.path(),
            root.path(),
            &mut paths,
            PortableV2Limits::default(),
            None,
        )
        .unwrap();
        paths.sort();
        let mut bundle = Vec::new();
        for path in paths {
            bundle.extend(tar_entry(
                &path,
                &fs::read(root.path().join(&path)).unwrap(),
            ));
        }
        bundle.extend([0u8; 1024]);
        let file = tempfile::NamedTempFile::new().unwrap();
        fs::write(file.path(), bundle).unwrap();
        let expanded = verify_portable_v2(
            root.path(),
            PortableV2Mode::Full,
            PortableV2Limits::default(),
            None,
        )
        .unwrap();
        let bundled = verify_portable_v2(
            file.path(),
            PortableV2Mode::Full,
            PortableV2Limits::default(),
            None,
        )
        .unwrap();
        assert_eq!(expanded.package_digest, bundled.package_digest);
        assert_eq!(expanded.component_count, bundled.component_count);
        assert_eq!(bundled.representation, PortableV2Representation::Bundle);
        assert_ne!(expanded.transport_digest, bundled.transport_digest);
    }
}
