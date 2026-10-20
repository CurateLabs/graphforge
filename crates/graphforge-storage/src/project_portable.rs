//! Bounded, read-only verification for portable project packages.
//!
//! This module deliberately does not use a general tar decoder: portable's
//! canonical bundle bytes are narrower than the formats those decoders accept.
#![allow(missing_docs)]

mod materialization;
pub use materialization::materialize_verified_portable;
pub(crate) use materialization::{
    CapturedCompositionControl, CapturedPortableSource, MaterializedCapture,
    capture_import_adjacency, materialize_verified_portable_observed,
    persist_composition_authority, persist_staged_composition, publish_compact_import_root,
};
mod authenticated_entries;
mod inventory;
use authenticated_entries::StreamHash;
mod participant_files;
pub(crate) mod pax;
pub use participant_files::{PortableFileRef, PortablePackageIndex};
pub(crate) mod research;
mod semantic_validation;
pub(crate) use semantic_validation::decode_runtime_map;
use semantic_validation::{
    admit_composition_features, package_class, validate_ontology_composition, validate_runtime_map,
    validate_semantics,
};

use graphforge_core::hash_observation::{ContractSha256, ControlSha256, PortableSha256 as Sha256};
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use sha2::Digest;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path};
use std::sync::atomic::{AtomicBool, Ordering};
use unicode_normalization::UnicodeNormalization;

const MANIFEST_PATH: &str = "data/graphforge-project.json";
pub(crate) const RUNTIME_MAP_PATH: &str =
    "data/components/compatibility/graphforge-runtime-map/runtime-generation.json";
pub(crate) const ONTOLOGY_COMPOSITION_PATH: &str =
    "data/components/compatibility/graphforge-ontology-composition/composition.json";
const BAGIT: &[u8] = b"BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n";
const BAG_INFO: &[u8] = b"Bag-Software-Agent: GraphForge portable\nBagging-Date: 1970-01-01\n";

pub use graphforge_core::portable::*;

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
pub(crate) struct RuntimeGenerationMap {
    pub(crate) contract: String,
    pub(crate) capabilities: Vec<RuntimeCapability>,
    pub(crate) participants: Vec<RuntimeParticipant>,
    pub(crate) graph_tree: Option<RuntimeGraphTree>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeCapability {
    pub(crate) capability_id: String,
    pub(crate) capability_version: u32,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeParticipant {
    pub(crate) participant_id: String,
    pub(crate) capability_id: String,
    pub(crate) capability_version: u32,
    pub(crate) record_family_id: String,
    pub(crate) record_version: u32,
    pub(crate) encoding: String,
    pub(crate) schema_fingerprint: String,
    pub(crate) row_count: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeGraphTree {
    pub(crate) component_id: String,
    pub(crate) inventory_participant_id: String,
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
    offset: Option<u64>,
    source_metadata: Option<fs::Metadata>,
}

/// Verify an expanded directory or canonical uncompressed bundle without mutation.
pub fn verify_portable(
    source: impl AsRef<Path>,
    mode: PortableMode,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableReport, PortableError> {
    preflight(source.as_ref(), limits, cancelled)?;
    scan(source.as_ref(), mode, limits, cancelled, None, None)
}

fn preflight(
    source: &Path,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableError> {
    if limits.copy_buffer_bytes == 0 {
        return Err(PortableError::new(
            PortableErrorCode::LimitExceeded,
            "copy buffer is zero",
        ));
    }
    check_cancel(cancelled)?;
    let metadata = fs::symlink_metadata(source)
        .map_err(|_| PortableError::new(PortableErrorCode::Io, "source unavailable"))?;
    if metadata.file_type().is_symlink() {
        return Err(PortableError::new(
            PortableErrorCode::InvalidStructure,
            "linked source",
        ));
    }
    if metadata.is_dir() {
        preflight_expanded(source, limits, cancelled)?;
    } else if metadata.is_file() {
        preflight_bundle(source, limits, cancelled)?;
    }
    if !metadata.is_dir() && !metadata.is_file() {
        return Err(PortableError::new(
            PortableErrorCode::InvalidStructure,
            "source is not a regular file or directory",
        ));
    }
    Ok(())
}

fn scan(
    source: &Path,
    mode: PortableMode,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    sink: Option<&mut materialization::CopySink<'_>>,
    written: Option<&crate::project_portable_export::WrittenPackage>,
) -> Result<PortableReport, PortableError> {
    scan_entries(source, mode, limits, cancelled, sink, written).map(|(report, _)| report)
}

/// Scan and authenticate a package, returning the report with the scanned entries.
fn scan_entries(
    source: &Path,
    mode: PortableMode,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    sink: Option<&mut materialization::CopySink<'_>>,
    written: Option<&crate::project_portable_export::WrittenPackage>,
) -> Result<(PortableReport, Vec<Entry>), PortableError> {
    let metadata = fs::symlink_metadata(source)
        .map_err(|_| PortableError::new(PortableErrorCode::Io, "source unavailable"))?;
    let (report, entries) = if metadata.is_dir() {
        verify_expanded(source, mode, limits, cancelled, sink, written)
    } else if metadata.is_file() {
        verify_bundle(source, mode, limits, cancelled, sink, written)
    } else {
        return Err(PortableError::new(
            PortableErrorCode::InvalidStructure,
            "source is not a regular file or directory",
        ));
    }?;
    if written.is_some_and(|written| written.entry_count() != entries.len()) {
        return Err(PortableError::new(
            PortableErrorCode::DigestMismatch,
            "written entry set changed",
        ));
    }
    if mode == PortableMode::Full {
        authenticated_entries::validate_semantics(source, &entries, &report, limits, cancelled)?;
    }
    let after = fs::symlink_metadata(source).map_err(|_| {
        PortableError::new(
            PortableErrorCode::ConcurrentMutation,
            "source disappeared after semantic admission",
        )
    })?;
    if !same_identity(&metadata, &after)
        || metadata.len() != after.len()
        || modified(&metadata) != modified(&after)
        || after.file_type().is_symlink()
    {
        return Err(PortableError::new(
            PortableErrorCode::ConcurrentMutation,
            "source changed after semantic admission",
        ));
    }
    for entry in &entries {
        if let Some(before) = &entry.source_metadata {
            let after = fs::symlink_metadata(source.join(&entry.path)).map_err(|_| {
                PortableError::at(
                    PortableErrorCode::ConcurrentMutation,
                    &entry.path,
                    "entry disappeared after admission",
                )
            })?;
            if !same_identity(before, &after)
                || before.len() != after.len()
                || modified(before) != modified(&after)
                || after.file_type().is_symlink()
                || has_multiple_links(&after)
            {
                return Err(PortableError::at(
                    PortableErrorCode::ConcurrentMutation,
                    &entry.path,
                    "entry changed after admission",
                ));
            }
        }
    }
    Ok((report, entries))
}

pub(crate) fn verify_written_package(
    source: &Path,
    limits: PortableLimits,
    cancelled: &AtomicBool,
    written: &crate::project_portable_export::WrittenPackage,
) -> Result<PortableReport, PortableError> {
    preflight(source, limits, Some(cancelled))?;
    scan(
        source,
        PortableMode::Full,
        limits,
        Some(cancelled),
        None,
        Some(written),
    )
}

fn expected_entry(
    written: Option<&crate::project_portable_export::WrittenPackage>,
    path: &str,
    length: u64,
) -> Result<Option<([u8; 32], u64)>, PortableError> {
    written
        .map(|written| {
            written.entry_identity(path, length).ok_or_else(|| {
                PortableError::at(
                    PortableErrorCode::DigestMismatch,
                    path,
                    "written entry identity changed",
                )
            })
        })
        .transpose()
}

/// Refuse a semantic manifest longer than `bound` before reading or parsing
/// it. `bound` is [`PortableLimits::semantic_manifest_bound`] for the
/// package's entry count: parsing holds the manifest as a JSON tree several
/// times its size, so a package may present only the manifest its entries
/// justify (#900).
fn admit_semantic_manifest_length(length: u64, bound: u64) -> Result<(), PortableError> {
    if length > bound {
        return Err(PortableError::at(
            PortableErrorCode::LimitExceeded,
            MANIFEST_PATH,
            "semantic manifest exceeds its entry-count bound",
        ));
    }
    Ok(())
}

fn admit_manifest(
    bytes: &[u8],
    bound: u64,
    limits: PortableLimits,
) -> Result<Manifest, PortableError> {
    admit_semantic_manifest_length(bytes.len() as u64, bound)?;
    let (manifest, canonical_without_digest) = parse_manifest(bytes, limits)?;
    let expected = format!(
        "sha256:{}",
        hex(&ControlSha256::digest(
            [
                b"graphforge-project/2\0".as_slice(),
                canonical_without_digest.as_slice()
            ]
            .concat()
        ))
    );
    if !constant_time_eq(expected.as_bytes(), manifest.package_digest.as_bytes()) {
        return Err(PortableError::at(
            PortableErrorCode::DigestMismatch,
            MANIFEST_PATH,
            "package digest",
        ));
    }
    validate_semantics(&manifest, limits)?;
    Ok(manifest)
}

fn read_bounded_file(path: &Path, limit: u64, entry: &str) -> Result<Vec<u8>, PortableError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        PortableError::at(
            PortableErrorCode::InvalidStructure,
            entry,
            "control unavailable",
        )
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > limit {
        return Err(PortableError::at(
            PortableErrorCode::LimitExceeded,
            entry,
            "control bound",
        ));
    }
    let mut file = crate::project_portable_export::open_source_no_follow(path).map_err(|_| {
        PortableError::at(PortableErrorCode::InvalidStructure, entry, "unsafe control")
    })?;
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| PortableError::at(PortableErrorCode::Io, entry, "cannot read control"))?;
    if bytes.len() as u64 > limit {
        return Err(PortableError::at(
            PortableErrorCode::LimitExceeded,
            entry,
            "control bound",
        ));
    }
    Ok(bytes)
}

fn preflight_expanded(
    root: &Path,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableError> {
    let mut paths = Vec::new();
    walk(root, root, &mut paths, limits, cancelled)?;
    let bound = limits.semantic_manifest_bound(paths.len() as u64);
    drop(paths);
    let length = fs::symlink_metadata(root.join(MANIFEST_PATH))
        .map_err(|_| {
            PortableError::at(
                PortableErrorCode::InvalidStructure,
                MANIFEST_PATH,
                "control unavailable",
            )
        })?
        .len();
    admit_semantic_manifest_length(length, bound)?;
    let manifest = read_bounded_file(&root.join(MANIFEST_PATH), bound, MANIFEST_PATH)?;
    let admitted = admit_manifest(&manifest, bound, limits)?;
    if admitted
        .requirements
        .capabilities
        .iter()
        .any(|capability| capability == "ontology-composition@1")
    {
        let control = read_bounded_file(
            &root.join(ONTOLOGY_COMPOSITION_PATH),
            limits.max_manifest_bytes,
            ONTOLOGY_COMPOSITION_PATH,
        )?;
        admit_composition_features(&control)?;
    }
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "keeps bounded header census and admission ordering in one pre-mutation audit path"
)]
fn preflight_bundle(
    path: &Path,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableError> {
    let mut input = crate::project_portable_export::open_source_no_follow(path)
        .map_err(|_| PortableError::new(PortableErrorCode::Io, "cannot open bundle"))?;
    let mut pending_pax: Option<pax::PaxHeader> = None;
    let mut admitted_manifest = None;
    let mut pending_composition: Option<Vec<u8>> = None;
    let mut admitted_composition = false;
    let mut entries = 0_u64;
    let mut regular_entries = 0_u64;
    let mut total = 0_u64;
    let mut paths = Vec::new();
    loop {
        check_cancel(cancelled)?;
        let mut header = [0_u8; 512];
        // Reject the retired format signature before materialization or target admission.
        // This is not a decoder or compatibility path.
        if entries == 0 {
            input.read_exact(&mut header[..16]).map_err(|_| {
                PortableError::new(PortableErrorCode::InvalidStructure, "truncated bundle")
            })?;
            if &header[..16] == b"graphforge-exp\0\n" {
                return Err(PortableError::new(
                    PortableErrorCode::UnsupportedProjectFormat,
                    "unsupported project format",
                ));
            }
            input.read_exact(&mut header[16..]).map_err(|_| {
                PortableError::new(PortableErrorCode::InvalidStructure, "truncated bundle")
            })?;
        } else {
            input.read_exact(&mut header).map_err(|_| {
                PortableError::new(PortableErrorCode::InvalidStructure, "truncated bundle")
            })?;
        }
        if header.iter().all(|byte| *byte == 0) {
            break;
        }
        let field_size = parse_octal(&header[124..136])?;
        let raw_path = header_path(&header)?;
        let pending_size = pending_pax.as_ref().and_then(|records| records.size);
        let size = if header[156] == b'x' {
            field_size
        } else {
            pax::entry_size(field_size, pending_size)?
        };
        entries = entries.checked_add(1).ok_or_else(|| {
            PortableError::new(PortableErrorCode::LimitExceeded, "bundle entry count")
        })?;
        if entries > limits.max_entries {
            return Err(PortableError::new(
                PortableErrorCode::LimitExceeded,
                "bundle entry count",
            ));
        }
        total = total.checked_add(size).ok_or_else(|| {
            PortableError::new(PortableErrorCode::LimitExceeded, "bundle total bytes")
        })?;
        if total > limits.max_total_bytes {
            return Err(PortableError::new(
                PortableErrorCode::LimitExceeded,
                "bundle total bytes",
            ));
        }
        if header[156] == b'x' {
            if pending_pax.is_some() {
                return Err(PortableError::new(
                    PortableErrorCode::InvalidStructure,
                    "invalid PAX sequence",
                ));
            }
            let bytes = read_unhashed_payload(
                &mut input,
                size,
                limits.max_path_bytes + pax::PAX_RECORD_OVERHEAD_BYTES,
            )?;
            pending_pax = Some(pax::parse(std::str::from_utf8(&bytes).map_err(|_| {
                PortableError::new(PortableErrorCode::InvalidPath, "PAX path is not UTF-8")
            })?)?);
            continue;
        }
        let entry = pending_pax.take().map_or(raw_path, |records| records.path);
        // No entry is extracted until every path is creatable everywhere.
        validate_path(&entry, limits.max_path_bytes)?;
        paths.push(entry.clone());
        regular_entries += 1;
        if entry == MANIFEST_PATH || entry == ONTOLOGY_COMPOSITION_PATH {
            // Canonical order puts every component entry before the semantic
            // manifest and only the two tag manifests after it, so the entries
            // seen so far, plus two, are the package's entries.
            let semantic_bound = limits.semantic_manifest_bound(regular_entries.saturating_add(2));
            let cap = if entry == MANIFEST_PATH {
                admit_semantic_manifest_length(size, semantic_bound)?;
                semantic_bound
            } else {
                limits.max_manifest_bytes
            };
            let bytes = read_unhashed_payload(
                &mut input,
                size,
                usize::try_from(cap).unwrap_or(usize::MAX),
            )?;
            if entry == MANIFEST_PATH {
                let admitted = admit_manifest(&bytes, semantic_bound, limits)?;
                let needs_composition = admitted
                    .requirements
                    .capabilities
                    .iter()
                    .any(|capability| capability == "ontology-composition@1");
                if !needs_composition {
                    admitted_manifest = Some(admitted);
                    continue;
                }
                admitted_manifest = Some(admitted);
                if let Some(control) = pending_composition.take() {
                    admit_composition_features(&control)?;
                    admitted_composition = true;
                }
            } else if admitted_manifest.is_some() {
                admit_composition_features(&bytes)?;
                admitted_composition = true;
            } else {
                // Canonical bundles sort paths, so the composition control can
                // precede the root manifest. Retain only this bounded control;
                // no component payload is read before both admission documents
                // have been checked.
                pending_composition = Some(bytes);
            }
        } else {
            let padding = (512 - size % 512) % 512;
            let skip = i64::try_from(size.saturating_add(padding)).map_err(|_| {
                PortableError::new(PortableErrorCode::LimitExceeded, "bundle entry size")
            })?;
            input.seek(SeekFrom::Current(skip)).map_err(|_| {
                PortableError::new(PortableErrorCode::InvalidStructure, "truncated bundle")
            })?;
        }
    }
    require_canonical_order(paths.iter().map(String::as_str))?;
    validate_path_set(&paths)?;
    let Some(manifest) = admitted_manifest else {
        return Err(PortableError::at(
            PortableErrorCode::InvalidStructure,
            MANIFEST_PATH,
            "required preflight control is missing",
        ));
    };
    if manifest
        .requirements
        .capabilities
        .iter()
        .any(|capability| capability == "ontology-composition@1")
        && !admitted_composition
    {
        return Err(PortableError::at(
            PortableErrorCode::InvalidStructure,
            ONTOLOGY_COMPOSITION_PATH,
            "required preflight control is missing",
        ));
    }
    Ok(())
}

pub(crate) struct VerifiedMaterialization {
    pub(crate) report: PortableReport,
    pub(crate) application_read_bytes: u64,
    pub(crate) application_read_operations: u64,
    pub(crate) captures: BTreeMap<String, MaterializedCapture>,
}

fn skip_padding(input: &mut File, length: u64) -> Result<(), PortableError> {
    let padding = (512 - length % 512) % 512;
    let mut bytes = [0u8; 512];
    input
        .read_exact(&mut bytes[..padding as usize])
        .map_err(|_| {
            PortableError::new(PortableErrorCode::InvalidStructure, "truncated padding")
        })?;
    Ok(())
}
fn read_unhashed_payload(
    input: &mut File,
    length: u64,
    limit: usize,
) -> Result<Vec<u8>, PortableError> {
    if length > limit as u64 {
        return Err(PortableError::new(
            PortableErrorCode::LimitExceeded,
            "PAX path exceeds limit",
        ));
    }
    let length = usize::try_from(length).map_err(|_| {
        PortableError::new(PortableErrorCode::LimitExceeded, "PAX path exceeds limit")
    })?;
    let mut bytes = vec![0; length];
    input.read_exact(&mut bytes).map_err(|_| {
        PortableError::new(PortableErrorCode::InvalidStructure, "truncated PAX payload")
    })?;
    skip_padding(input, length as u64)?;
    Ok(bytes)
}

fn verify_expanded(
    root: &Path,
    mode: PortableMode,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    mut sink: Option<&mut materialization::CopySink<'_>>,
    written: Option<&crate::project_portable_export::WrittenPackage>,
) -> Result<(PortableReport, Vec<Entry>), PortableError> {
    let root_before = fs::symlink_metadata(root)
        .map_err(|_| PortableError::new(PortableErrorCode::Io, "expanded root unavailable"))?;
    let mut paths = Vec::new();
    walk(root, root, &mut paths, limits, cancelled)?;
    paths.sort();
    validate_path_set(&paths)?;
    let mut entries = Vec::with_capacity(paths.len());
    let mut total = 0u64;
    let mut inventory_verdict = None;
    for path in &paths {
        check_cancel(cancelled)?;
        let full = root.join(path);
        let before = fs::metadata(&full)
            .map_err(|_| PortableError::at(PortableErrorCode::Io, path, "cannot stat entry"))?;
        if !before.is_file() || before.file_type().is_symlink() {
            return Err(PortableError::at(
                PortableErrorCode::InvalidStructure,
                path,
                "non-regular entry",
            ));
        }
        if has_multiple_links(&before) {
            return Err(PortableError::at(
                PortableErrorCode::InvalidStructure,
                path,
                "hard-linked entry",
            ));
        }
        let length = before.len();
        enforce_length(path, length, &mut total, limits)?;
        let mut check = (path == inventory::INVENTORY_PATH)
            .then(|| inventory::InventoryCheck::new(&entries, limits.max_path_bytes));
        let (digest, bytes) = hash_file(
            &full,
            path,
            length,
            limits.copy_buffer_bytes,
            retained_limit(path, limits, paths.len() as u64),
            check.as_mut(),
            cancelled,
            sink.as_deref_mut(),
            expected_entry(written, path, length)?,
        )?;
        if let Some(check) = check {
            inventory_verdict = Some(check.finish());
        }
        let after = fs::metadata(&full).map_err(|_| {
            PortableError::at(
                PortableErrorCode::ConcurrentMutation,
                path,
                "entry disappeared",
            )
        })?;
        if !same_identity(&before, &after)
            || before.len() != after.len()
            || modified(&before) != modified(&after)
        {
            return Err(PortableError::at(
                PortableErrorCode::ConcurrentMutation,
                path,
                "entry changed",
            ));
        }
        entries.push(Entry {
            path: path.clone(),
            length,
            digest,
            bytes,
            offset: None,
            source_metadata: Some(before),
        });
    }
    validate_expanded_namespace(root, &paths, &root_before, limits, cancelled)?;
    let transport = expanded_transport(&entries)?;
    let report = validate_package(
        &entries,
        PortableRepresentation::Expanded,
        mode,
        limits,
        Some(transport),
        inventory_verdict,
    )?;
    Ok((report, entries))
}

fn validate_expanded_namespace(
    root: &Path,
    paths: &[String],
    root_before: &fs::Metadata,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableError> {
    let mut final_paths = Vec::new();
    walk(root, root, &mut final_paths, limits, cancelled)?;
    final_paths.sort();
    let root_after = fs::symlink_metadata(root).map_err(|_| {
        PortableError::new(
            PortableErrorCode::ConcurrentMutation,
            "expanded root disappeared",
        )
    })?;
    if paths != final_paths.as_slice()
        || !same_identity(root_before, &root_after)
        || root_after.file_type().is_symlink()
    {
        return Err(PortableError::new(
            PortableErrorCode::ConcurrentMutation,
            "expanded entry set changed",
        ));
    }
    Ok(())
}

fn walk(
    root: &Path,
    dir: &Path,
    out: &mut Vec<String>,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableError> {
    check_cancel(cancelled)?;
    let mut children = fs::read_dir(dir)
        .map_err(|_| PortableError::new(PortableErrorCode::Io, "cannot read directory"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| PortableError::new(PortableErrorCode::Io, "cannot read directory entry"))?;
    children.sort_by_key(std::fs::DirEntry::file_name);
    for child in children {
        check_cancel(cancelled)?;
        let ty = child
            .file_type()
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "cannot inspect entry"))?;
        let relative = child
            .path()
            .strip_prefix(root)
            .map_err(|_| PortableError::new(PortableErrorCode::InvalidPath, "entry escaped root"))?
            .to_str()
            .ok_or_else(|| PortableError::new(PortableErrorCode::InvalidPath, "non-UTF-8 path"))?
            .replace(std::path::MAIN_SEPARATOR, "/");
        validate_path(&relative, limits.max_path_bytes)?;
        if ty.is_symlink() || (!ty.is_file() && !ty.is_dir()) {
            return Err(PortableError::at(
                PortableErrorCode::InvalidStructure,
                &relative,
                "link or special entry",
            ));
        }
        if ty.is_dir() {
            walk(root, &child.path(), out, limits, cancelled)?;
        } else {
            out.push(relative);
            if out.len() as u64 > limits.max_entries {
                return Err(PortableError::new(
                    PortableErrorCode::LimitExceeded,
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
    mode: PortableMode,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    mut sink: Option<&mut materialization::CopySink<'_>>,
    written: Option<&crate::project_portable_export::WrittenPackage>,
) -> Result<(PortableReport, Vec<Entry>), PortableError> {
    let before = fs::metadata(path)
        .map_err(|_| PortableError::new(PortableErrorCode::Io, "cannot stat bundle"))?;
    if has_multiple_links(&before) {
        return Err(PortableError::new(
            PortableErrorCode::InvalidStructure,
            "hard-linked bundle",
        ));
    }
    let mut file = crate::project_portable_export::open_source_no_follow(path)
        .map_err(|_| PortableError::new(PortableErrorCode::Io, "cannot open bundle"))?;
    let mut transport = StreamHash::new(
        written.and_then(crate::project_portable_export::WrittenPackage::bundle_identity),
    );
    let mut entries = Vec::new();
    let mut total = 0u64;
    let mut inventory_verdict = None;
    let mut pending_pax: Option<(pax::PaxHeader, String)> = None;
    loop {
        check_cancel(cancelled)?;
        let mut header = [0u8; 512];
        read_exact_hash(&mut file, &mut header, &mut transport, "bundle header")?;
        if header == [0; 512] {
            let mut second = [1u8; 512];
            read_exact_hash(&mut file, &mut second, &mut transport, "second end block")?;
            if second != [0; 512] {
                return Err(PortableError::new(
                    PortableErrorCode::InvalidStructure,
                    "invalid end marker",
                ));
            }
            let mut extra = [0u8; 1];
            if file
                .read(&mut extra)
                .map_err(|_| PortableError::new(PortableErrorCode::Io, "bundle read"))?
                != 0
            {
                let current = file
                    .metadata()
                    .map_err(|_| PortableError::new(PortableErrorCode::Io, "bundle metadata"))?;
                if current.len() != before.len() || modified(&current) != modified(&before) {
                    return Err(PortableError::new(
                        PortableErrorCode::ConcurrentMutation,
                        "bundle changed during copy",
                    ));
                }
                return Err(PortableError::new(
                    PortableErrorCode::InvalidStructure,
                    "trailing bytes",
                ));
            }
            break;
        }
        verify_header(&header)?;
        let kind = header[156];
        let field_size = parse_octal(&header[124..136])?;
        let raw_path = header_path(&header)?;
        if kind == b'x' {
            let size = field_size;
            if pending_pax.is_some() || !raw_path.starts_with("PaxHeaders/") {
                return Err(PortableError::new(
                    PortableErrorCode::InvalidStructure,
                    "invalid PAX sequence",
                ));
            }
            let bytes = read_payload(
                &mut file,
                size,
                &mut transport,
                limits.max_path_bytes + pax::PAX_RECORD_OVERHEAD_BYTES,
                cancelled,
            )?;
            let text = std::str::from_utf8(&bytes).map_err(|_| {
                PortableError::new(PortableErrorCode::InvalidPath, "PAX path is not UTF-8")
            })?;
            let records = pax::parse(text)?;
            let suffix = hex(&ContractSha256::digest(records.path.as_bytes()))[..16].to_owned();
            if raw_path != format!("PaxHeaders/{suffix}") {
                return Err(PortableError::new(
                    PortableErrorCode::InvalidStructure,
                    "non-canonical PAX header name",
                ));
            }
            pending_pax = Some((records, suffix));
            continue;
        }
        if kind != b'0' {
            return Err(PortableError::new(
                PortableErrorCode::InvalidStructure,
                "non-regular tar entry",
            ));
        }
        let used_pax = pending_pax.is_some();
        let (entry_path, size) = if let Some((records, suffix)) = pending_pax.take() {
            if raw_path != format!("PaxFiles/{suffix}") {
                return Err(PortableError::at(
                    PortableErrorCode::InvalidStructure,
                    &records.path,
                    "non-canonical PAX placeholder",
                ));
            }
            if records.size.is_none() && canonical_ustar_split(&records.path).is_some() {
                return Err(PortableError::at(
                    PortableErrorCode::InvalidStructure,
                    &records.path,
                    "unnecessary PAX header",
                ));
            }
            let size = pax::entry_size(field_size, records.size)?;
            (records.path, size)
        } else {
            (raw_path.clone(), pax::entry_size(field_size, None)?)
        };
        verify_canonical_header_path(&header, &entry_path, used_pax)?;
        validate_path(&entry_path, limits.max_path_bytes)?;
        enforce_length(&entry_path, size, &mut total, limits)?;
        let offset = file
            .stream_position()
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "bundle position"))?;
        let mut check = (entry_path == inventory::INVENTORY_PATH)
            .then(|| inventory::InventoryCheck::new(&entries, limits.max_path_bytes));
        let (digest, bytes) = hash_payload(
            &mut file,
            size,
            &mut transport,
            limits.copy_buffer_bytes,
            retained_limit(&entry_path, limits, entries.len() as u64 + 3),
            check.as_mut(),
            cancelled,
            &entry_path,
            sink.as_deref_mut(),
            expected_entry(written, &entry_path, size)?,
        )?;
        if let Some(check) = check {
            inventory_verdict = Some(check.finish());
        }
        entries.push(Entry {
            path: entry_path,
            length: size,
            digest,
            bytes,
            offset: Some(offset),
            source_metadata: None,
        });
        if entries.len() as u64 > limits.max_entries {
            return Err(PortableError::new(
                PortableErrorCode::LimitExceeded,
                "entry count",
            ));
        }
    }
    if pending_pax.is_some() {
        return Err(PortableError::new(
            PortableErrorCode::InvalidStructure,
            "orphan PAX header",
        ));
    }
    require_canonical_order(entries.iter().map(|e| e.path.as_str()))?;
    validate_path_set(&entries.iter().map(|e| e.path.clone()).collect::<Vec<_>>())?;
    let after = file.metadata().map_err(|_| {
        PortableError::new(PortableErrorCode::ConcurrentMutation, "bundle disappeared")
    })?;
    let named = fs::symlink_metadata(path).map_err(|_| {
        PortableError::new(PortableErrorCode::ConcurrentMutation, "bundle disappeared")
    })?;
    if !same_identity(&after, &named)
        || !same_identity(&before, &after)
        || before.len() != after.len()
        || modified(&before) != modified(&after)
    {
        return Err(PortableError::new(
            PortableErrorCode::ConcurrentMutation,
            "bundle changed",
        ));
    }
    let report = validate_package(
        &entries,
        PortableRepresentation::Bundle,
        mode,
        limits,
        Some(hex(&transport.finish()?)),
        inventory_verdict,
    )?;
    Ok((report, entries))
}

fn validate_package(
    entries: &[Entry],
    representation: PortableRepresentation,
    mode: PortableMode,
    limits: PortableLimits,
    transport: Option<String>,
    inventory: Option<inventory::InventoryVerdict>,
) -> Result<PortableReport, PortableError> {
    let map: BTreeMap<_, _> = entries.iter().map(|e| (e.path.as_str(), e)).collect();
    require_exact(&map, "bagit.txt", BAGIT)?;
    refuse_other_producer_revision(&map, "bag-info.txt", BAG_INFO)?;
    require_exact(&map, "bag-info.txt", BAG_INFO)?;
    let manifest_entry = map.get(MANIFEST_PATH).ok_or_else(|| {
        PortableError::at(
            PortableErrorCode::InvalidStructure,
            MANIFEST_PATH,
            "missing semantic manifest",
        )
    })?;
    admit_semantic_manifest_length(
        manifest_entry.length,
        limits.semantic_manifest_bound(entries.len() as u64),
    )?;
    let manifest_bytes = read_entry_bytes(entries, MANIFEST_PATH)?;
    let (manifest, canonical_without_digest) = parse_manifest(&manifest_bytes, limits)?;
    let expected = format!(
        "sha256:{}",
        hex(&ControlSha256::digest(
            [
                b"graphforge-project/2\0".as_slice(),
                canonical_without_digest.as_slice()
            ]
            .concat()
        ))
    );
    if !constant_time_eq(expected.as_bytes(), manifest.package_digest.as_bytes()) {
        return Err(PortableError::at(
            PortableErrorCode::DigestMismatch,
            MANIFEST_PATH,
            "package digest",
        ));
    }
    validate_semantics(&manifest, limits)?;
    validate_bag_manifests(&map, &manifest, inventory)?;
    validate_runtime_map(&map, &manifest, limits)?;
    let (ontology_composition, ontology_composition_entries) =
        validate_ontology_composition(&map, &manifest, limits)?;
    let full = mode == PortableMode::Full;
    Ok(PortableReport {
        research_entries: manifest
            .components
            .iter()
            .filter(|component| component.kind == "research")
            .flat_map(|component| {
                component.files.iter().map(|file| {
                    graphforge_core::portable::PortableResearchEntry {
                        component_id: component.participant_id.clone(),
                        path: file.path.clone(),
                        length: file.length,
                        sha256: file.sha256.clone(),
                    }
                })
            })
            .collect(),
        research_interchange: manifest
            .components
            .iter()
            .any(|component| component.kind == "research"),
        contract: "graphforge-portable-verify/1",
        representation,
        package_digest: manifest.package_digest.clone(),
        package_class: package_class(&manifest.package_class)?,
        component_count: manifest.components.len() as u64,
        entry_count: entries.len() as u64,
        payload_bytes: entries.iter().map(|e| e.length).sum(),
        integrity: if full {
            PortableIntegrity::Verified
        } else {
            PortableIntegrity::NotChecked
        },
        compatibility: PortableCompatibility::Supported,
        authenticity: PortableAuthenticity::Unsigned,
        transport_digest: transport.map(|x| format!("sha256:{x}")),
        ontology_composition,
        ontology_composition_entries,
    })
}

// Verification retains no payloads. For the small semantic/tag records, callers
// provide their bytes through this per-entry cache in the next writer/verifier
// integration. Expanded and bundle readers currently re-open/seek them below.
fn read_entry_bytes(entries: &[Entry], path: &str) -> Result<Vec<u8>, PortableError> {
    entries
        .iter()
        .find(|e| e.path == path)
        .and_then(|e| e.bytes.clone())
        .ok_or_else(|| {
            PortableError::at(
                PortableErrorCode::InvalidStructure,
                path,
                "tag bytes unavailable",
            )
        })
}

pub(crate) fn canonical_json(value: &Value) -> Result<Vec<u8>, PortableError> {
    fn write(value: &Value, output: &mut Vec<u8>) -> Result<(), PortableError> {
        match value {
            Value::Null => output.extend_from_slice(b"null"),
            Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
            Value::Number(value) => output.extend(value.to_string().bytes()),
            Value::String(value) => output.extend(serde_json::to_vec(value).map_err(|_| {
                PortableError::new(PortableErrorCode::InvalidStructure, "JSON canonicalization")
            })?),
            Value::Array(values) => {
                output.push(b'[');
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        output.push(b',');
                    }
                    write(value, output)?;
                }
                output.push(b']');
            }
            Value::Object(values) => {
                output.push(b'{');
                let mut keys = values.keys().collect::<Vec<_>>();
                keys.sort();
                for (index, key) in keys.into_iter().enumerate() {
                    if index != 0 {
                        output.push(b',');
                    }
                    output.extend(serde_json::to_vec(key).map_err(|_| {
                        PortableError::new(
                            PortableErrorCode::InvalidStructure,
                            "JSON canonicalization",
                        )
                    })?);
                    output.push(b':');
                    write(&values[key], output)?;
                }
                output.push(b'}');
            }
        }
        Ok(())
    }
    let mut output = Vec::new();
    write(value, &mut output)?;
    Ok(output)
}

fn parse_manifest(
    bytes: &[u8],
    _limits: PortableLimits,
) -> Result<(Manifest, Vec<u8>), PortableError> {
    // One JSON tree is resident at a time: the typed manifest is read from it
    // by reference, then the digest member is removed in place to form the
    // signed preimage. No clone of the tree is made (#900).
    let mut value = UniqueValue::deserialize(&mut serde_json::Deserializer::from_slice(bytes))
        .map_err(|_| {
            PortableError::at(
                PortableErrorCode::InvalidStructure,
                MANIFEST_PATH,
                "invalid JSON",
            )
        })?
        .0;
    if canonical_json(&value).map_err(|_| {
        PortableError::new(
            PortableErrorCode::InvalidStructure,
            "manifest canonicalization",
        )
    })? != bytes
    {
        return Err(PortableError::at(
            PortableErrorCode::InvalidStructure,
            MANIFEST_PATH,
            "manifest is not JCS canonical",
        ));
    }
    if !value.is_object() {
        return Err(PortableError::at(
            PortableErrorCode::InvalidStructure,
            MANIFEST_PATH,
            "manifest is not object",
        ));
    }
    let manifest = Manifest::deserialize(&value).map_err(|_| {
        PortableError::at(
            PortableErrorCode::InvalidStructure,
            MANIFEST_PATH,
            "manifest schema",
        )
    })?;
    if let Some(object) = value.as_object_mut() {
        object.remove("package_digest");
    }
    let canonical_without = serde_json::to_vec(&value).map_err(|_| {
        PortableError::at(
            PortableErrorCode::InvalidStructure,
            MANIFEST_PATH,
            "cannot canonicalize manifest",
        )
    })?;
    Ok((manifest, canonical_without))
}

fn validate_bag_manifests(
    map: &BTreeMap<&str, &Entry>,
    manifest: &Manifest,
    inventory: Option<inventory::InventoryVerdict>,
) -> Result<(), PortableError> {
    let declared: BTreeMap<_, _> = manifest
        .components
        .iter()
        .flat_map(|c| &c.files)
        .map(|f| (f.path.as_str(), (f.length, f.sha256.as_str())))
        .collect();
    for (&path, &(length, digest)) in &declared {
        let entry = map.get(path).ok_or_else(|| {
            PortableError::at(
                PortableErrorCode::InvalidStructure,
                path,
                "declared payload missing",
            )
        })?;
        if entry.length != length
            || !constant_time_eq(hex(&entry.digest).as_bytes(), digest.as_bytes())
        {
            return Err(PortableError::at(
                PortableErrorCode::DigestMismatch,
                path,
                "payload digest/length",
            ));
        }
    }
    // A map lookup, not a scan of every declared file per entry: the scan was
    // quadratic in the entry count (#900).
    for path in map.keys().filter(|p| p.starts_with("data/components/")) {
        if !declared.contains_key(path) {
            return Err(PortableError::at(
                PortableErrorCode::InvalidStructure,
                path,
                "extra payload",
            ));
        }
    }
    for required in [inventory::INVENTORY_PATH, "tagmanifest-sha256.txt"] {
        if !map.contains_key(required) {
            return Err(PortableError::at(
                PortableErrorCode::InvalidStructure,
                required,
                "missing tag manifest",
            ));
        }
    }
    // The payload inventory was checked row by row against the preceding
    // `data/` entries while it streamed; its outcome is reported here.
    inventory.ok_or_else(|| {
        PortableError::at(
            PortableErrorCode::InvalidStructure,
            inventory::INVENTORY_PATH,
            "tag bytes unavailable",
        )
    })??;
    let tag_manifest = parse_digest_manifest(
        &read_entry_bytes_from_map(map, "tagmanifest-sha256.txt")?,
        "tagmanifest-sha256.txt",
    )?;
    let expected_tags: BTreeMap<_, _> = ["bag-info.txt", "bagit.txt", "manifest-sha256.txt"]
        .into_iter()
        .map(|p| (p.to_owned(), hex(&map[p].digest)))
        .collect();
    if tag_manifest != expected_tags {
        return Err(PortableError::at(
            PortableErrorCode::DigestMismatch,
            "tagmanifest-sha256.txt",
            "tag inventory manifest",
        ));
    }
    if map.keys().any(|p| {
        !p.starts_with("data/") && !expected_tags.contains_key(*p) && *p != "tagmanifest-sha256.txt"
    }) {
        return Err(PortableError::new(
            PortableErrorCode::InvalidStructure,
            "unmanifested extra entry",
        ));
    }
    Ok(())
}

fn read_entry_bytes_from_map(
    map: &BTreeMap<&str, &Entry>,
    path: &str,
) -> Result<Vec<u8>, PortableError> {
    map.get(path).and_then(|e| e.bytes.clone()).ok_or_else(|| {
        PortableError::at(
            PortableErrorCode::InvalidStructure,
            path,
            "tag bytes unavailable",
        )
    })
}

fn parse_digest_manifest(
    bytes: &[u8],
    entry: &str,
) -> Result<BTreeMap<String, String>, PortableError> {
    if bytes.last() != Some(&b'\n') {
        return Err(PortableError::at(
            PortableErrorCode::InvalidStructure,
            entry,
            "tag manifest termination",
        ));
    }
    let text = std::str::from_utf8(bytes).map_err(|_| {
        PortableError::at(
            PortableErrorCode::InvalidStructure,
            entry,
            "tag manifest UTF-8",
        )
    })?;
    let mut result = BTreeMap::new();
    let mut previous: Option<&str> = None;
    for line in text.lines() {
        let (digest, path) = line.split_once("  ").ok_or_else(|| {
            PortableError::at(
                PortableErrorCode::InvalidStructure,
                entry,
                "tag manifest record",
            )
        })?;
        validate_path(path, 4096)?;
        if !sha(digest)
            || previous >= Some(path)
            || result.insert(path.to_owned(), digest.to_owned()).is_some()
        {
            return Err(PortableError::at(
                PortableErrorCode::InvalidStructure,
                entry,
                "tag manifest order/duplicate",
            ));
        }
        previous = Some(path);
    }
    Ok(result)
}

fn expanded_transport(entries: &[Entry]) -> Result<String, PortableError> {
    let mut hash = ContractSha256::new();
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

/// The `Bag-Software-Agent` value of a `bag-info.txt`: the remainder of its first line.
fn bag_agent(bytes: &[u8]) -> Option<&[u8]> {
    let line = bytes.split(|byte| *byte == b'\n').next()?;
    line.strip_prefix(b"Bag-Software-Agent: ")
}

/// Another GraphForge producer revision wrote the package when its agent is exactly the
/// current agent name followed by `-<token>` (non-empty, no whitespace): refuse it as
/// incompatible before the exact tag comparison can call it tampered. Any other
/// deviation, including stray whitespace or a carriage return, is left to that comparison.
fn refuse_other_producer_revision(
    map: &BTreeMap<&str, &Entry>,
    path: &str,
    current: &[u8],
) -> Result<(), PortableError> {
    let Some(current) = bag_agent(current) else {
        return Ok(());
    };
    let Some(agent) = map
        .get(path)
        .and_then(|entry| entry.bytes.as_deref())
        .and_then(bag_agent)
    else {
        return Ok(());
    };
    let revision = agent
        .strip_prefix(current)
        .and_then(|rest| rest.strip_prefix(b"-"));
    if revision.is_some_and(|token| !token.is_empty() && !token.iter().any(u8::is_ascii_whitespace))
    {
        return Err(PortableError::at(
            PortableErrorCode::Incompatible,
            path,
            "package was written by a different GraphForge producer revision",
        ));
    }
    Ok(())
}
fn require_exact(
    map: &BTreeMap<&str, &Entry>,
    path: &str,
    expected: &[u8],
) -> Result<(), PortableError> {
    let e = map.get(path).ok_or_else(|| {
        PortableError::at(PortableErrorCode::InvalidStructure, path, "missing tag")
    })?;
    if e.length != expected.len() as u64 || e.digest != ControlSha256::digest(expected)[..] {
        return Err(PortableError::at(
            PortableErrorCode::DigestMismatch,
            path,
            "tag bytes",
        ));
    }
    Ok(())
}
fn check_cancel(c: Option<&AtomicBool>) -> Result<(), PortableError> {
    if c.is_some_and(|x| x.load(Ordering::Relaxed)) {
        Err(PortableError::new(
            PortableErrorCode::Cancelled,
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
    limits: PortableLimits,
) -> Result<(), PortableError> {
    if length > limits.max_entry_bytes {
        return Err(PortableError::at(
            PortableErrorCode::LimitExceeded,
            path,
            "entry size",
        ));
    }
    *total = total
        .checked_add(length)
        .ok_or_else(|| PortableError::new(PortableErrorCode::LimitExceeded, "total overflow"))?;
    if *total > limits.max_total_bytes {
        return Err(PortableError::new(
            PortableErrorCode::LimitExceeded,
            "declared total",
        ));
    }
    Ok(())
}
fn retain_payload(
    length: u64,
    limit: Option<u64>,
    entry: &str,
) -> Result<Option<Vec<u8>>, PortableError> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    if length > limit {
        return Err(PortableError::at(
            PortableErrorCode::LimitExceeded,
            entry,
            "retained control entry exceeds limit",
        ));
    }
    let capacity = usize::try_from(length).map_err(|_| {
        PortableError::at(
            PortableErrorCode::LimitExceeded,
            entry,
            "retained tag does not fit address space",
        )
    })?;
    Ok(Some(Vec::with_capacity(capacity)))
}

#[expect(
    clippy::too_many_arguments,
    reason = "entry authentication carries bounded retention, copy and private writer evidence"
)]
fn hash_file(
    path: &Path,
    entry: &str,
    length: u64,
    buffer: usize,
    retain_limit: Option<u64>,
    mut inventory: Option<&mut inventory::InventoryCheck<'_>>,
    cancelled: Option<&AtomicBool>,
    mut sink: Option<&mut materialization::CopySink<'_>>,
    expected: Option<([u8; 32], u64)>,
) -> Result<([u8; 32], Option<Vec<u8>>), PortableError> {
    let mut f = crate::project_portable_export::open_source_no_follow(path).map_err(|_| {
        PortableError::at(
            PortableErrorCode::InvalidStructure,
            entry,
            "cannot safely open entry",
        )
    })?;
    let before = f.metadata().map_err(|_| {
        PortableError::at(PortableErrorCode::Io, entry, "cannot inspect open entry")
    })?;
    if !before.is_file() || has_multiple_links(&before) || before.len() != length {
        return Err(PortableError::at(
            PortableErrorCode::ConcurrentMutation,
            entry,
            "opened entry identity differs from inventory",
        ));
    }
    let mut h = StreamHash::new(expected);
    let mut kept = retain_payload(length, retain_limit, entry)?;
    let mut output = sink
        .as_deref_mut()
        .map(|sink| sink.open(entry))
        .transpose()?
        .flatten();
    let mut checksum = output
        .as_ref()
        .map(|_| crate::corruption_checksum::Checksum::new());
    let mut left = length;
    let mut b = vec![0u8; buffer];
    while left > 0 {
        check_cancel(cancelled)?;
        let n = f
            .read(&mut b[..usize::try_from(left.min(buffer as u64)).unwrap()])
            .map_err(|_| PortableError::at(PortableErrorCode::Io, entry, "cannot read entry"))?;
        if n == 0 {
            return Err(PortableError::at(
                PortableErrorCode::ConcurrentMutation,
                entry,
                "entry truncated",
            ));
        }
        h.update(&b[..n]);
        if let Some(checksum) = &mut checksum {
            checksum.update(&b[..n]);
        }
        if let (Some(sink), Some(output)) = (sink.as_deref_mut(), output.as_mut()) {
            sink.write(entry, output, &b[..n])?;
        }
        if let Some(bytes) = &mut kept {
            bytes.extend_from_slice(&b[..n]);
        }
        if let Some(inventory) = inventory.as_deref_mut() {
            inventory.update(&b[..n]);
        }
        left -= n as u64;
    }
    let after = f.metadata().map_err(|_| {
        PortableError::at(PortableErrorCode::Io, entry, "cannot re-inspect open entry")
    })?;
    if !same_identity(&before, &after)
        || before.len() != after.len()
        || modified(&before) != modified(&after)
    {
        return Err(PortableError::at(
            PortableErrorCode::ConcurrentMutation,
            entry,
            "open entry changed while hashing",
        ));
    }
    let digest = h.finish()?;
    if let (Some(sink), Some(output)) = (sink, output) {
        sink.finish(
            entry,
            output,
            length,
            digest,
            checksum
                .expect("materialized output has a checksum capture")
                .finish(),
        )?;
    }
    Ok((digest, kept))
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
/// Longest path component, in bytes. ext4, XFS and Btrfs limit a name to 255
/// bytes; NTFS and APFS limit it to 255 UTF-16 units or characters, which a
/// 255-byte name never exceeds because every UTF-8 sequence is at least as
/// long as its UTF-16 encoding.
const MAX_COMPONENT_BYTES: usize = 255;

/// Whether `component` is a Windows reserved device name, with or without an
/// extension. Windows ignores case, strips trailing spaces from the base name
/// and counts `¹`, `²` and `³` as digits in `COM#` and `LPT#`.
/// <https://learn.microsoft.com/windows/win32/fileio/naming-a-file>
fn is_windows_device_name(component: &str) -> bool {
    let base = component
        .split('.')
        .next()
        .unwrap_or(component)
        .trim_end_matches(' ');
    if ["CON", "PRN", "AUX", "NUL"]
        .iter()
        .any(|name| base.eq_ignore_ascii_case(name))
    {
        return true;
    }
    let mut chars = base.chars();
    let (Some(a), Some(b), Some(c), Some(digit), None) = (
        chars.next(),
        chars.next(),
        chars.next(),
        chars.next(),
        chars.next(),
    ) else {
        return false;
    };
    let name = [a, b, c].iter().collect::<String>();
    (name.eq_ignore_ascii_case("COM") || name.eq_ignore_ascii_case("LPT"))
        && matches!(digit, '1'..='9' | '\u{b9}' | '\u{b2}' | '\u{b3}')
}

/// The portable rule for one path component: a name that can be created on
/// Linux, macOS and Windows, so a package verified anywhere extracts anywhere.
fn component_violation(component: &str) -> Option<&'static str> {
    if component.len() > MAX_COMPONENT_BYTES {
        Some("path component exceeds 255 bytes")
    } else if component.contains(['<', '>', ':', '"', '|', '?', '*']) {
        Some("path component has a character that Windows forbids")
    } else if component.ends_with([' ', '.']) {
        Some("path component ends with a space or a dot")
    } else if is_windows_device_name(component) {
        Some("path component is a Windows reserved device name")
    } else {
        None
    }
}

/// Why `path` cannot be a portable package path, if it cannot. This is the
/// one path rule; the verifier and the exporter both apply it.
pub(crate) fn path_violation(path: &str, max: usize) -> Option<&'static str> {
    if path.is_empty()
        || path.len() > max
        || path
            .as_bytes()
            .iter()
            .any(|b| *b == 0 || *b < 0x20 || *b == 0x7f)
        || path.contains('\\')
        || path.nfc().collect::<String>() != path
        || Path::new(path).is_absolute()
        || path
            .split('/')
            .any(|c| c.is_empty() || c == "." || c == "..")
        || Path::new(path)
            .components()
            .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Some("unsafe/non-canonical path");
    }
    path.split('/').find_map(component_violation)
}

/// The key under which the case- and normalization-insensitive file systems
/// (the macOS and Windows defaults) treat two paths as one: canonical caseless
/// matching, so NFD, full Unicode case folding (`CaseFolding.txt` statuses C
/// and F: `ß` folds to `ss`, `ς` to `σ`), then NFD again. The dotless `ı`,
/// which Turkic folding alone keeps apart and which some Windows upcase tables
/// map to `I`, is read as `i`: a key may only err toward refusing.
pub(crate) fn path_collision_key(path: &str) -> String {
    let decomposed = path.nfd().collect::<String>();
    icu_casemap::CaseMapper::new()
        .fold_string(&decomposed)
        .nfd()
        .map(|c| if c == '\u{131}' { 'i' } else { c })
        .collect()
}

fn validate_path(path: &str, max: usize) -> Result<(), PortableError> {
    match path_violation(path, max) {
        Some(detail) => Err(PortableError::at(
            PortableErrorCode::InvalidPath,
            path,
            detail,
        )),
        None => Ok(()),
    }
}
fn require_canonical_order<'a>(
    paths: impl Iterator<Item = &'a str> + Clone,
) -> Result<(), PortableError> {
    if paths.clone().zip(paths.skip(1)).any(|(a, b)| a >= b) {
        return Err(PortableError::new(
            PortableErrorCode::InvalidStructure,
            "bundle entries are not canonical order",
        ));
    }
    Ok(())
}
/// Refuse a package in which two entries could be one on a case- or
/// normalization-insensitive file system, or in which a file is another
/// entry's directory (`a` and `a/b`), so extraction can never meet a
/// conflict half-way. Sorted sets keep this O(n log n) in the entry count.
fn validate_path_set(paths: &[String]) -> Result<(), PortableError> {
    let mut files = BTreeMap::new();
    let mut directories = BTreeSet::new();
    for p in paths {
        let key = path_collision_key(p);
        for (slash, _) in key.match_indices('/') {
            directories.insert(key[..slash].to_owned());
        }
        if files.insert(key, p).is_some() {
            return Err(PortableError::at(
                PortableErrorCode::DuplicateEntry,
                p,
                "duplicate/case-fold collision",
            ));
        }
    }
    if let Some((_, p)) = files.iter().find(|(key, _)| directories.contains(*key)) {
        return Err(PortableError::at(
            PortableErrorCode::DuplicateEntry,
            p,
            "file/directory path collision",
        ));
    }
    Ok(())
}
fn read_exact_hash(
    r: &mut File,
    b: &mut [u8],
    h: &mut StreamHash,
    detail: &'static str,
) -> Result<(), PortableError> {
    r.read_exact(b)
        .map_err(|_| PortableError::new(PortableErrorCode::InvalidStructure, detail))?;
    h.update(b);
    Ok(())
}
fn parse_octal(field: &[u8]) -> Result<u64, PortableError> {
    if field.first().is_some_and(|b| b & 0x80 != 0) {
        return Err(PortableError::new(
            PortableErrorCode::InvalidStructure,
            "base-256 numeric field",
        ));
    }
    let s = std::str::from_utf8(field)
        .map_err(|_| PortableError::new(PortableErrorCode::InvalidStructure, "numeric field"))?
        .trim_matches(['\0', ' ']);
    u64::from_str_radix(if s.is_empty() { "0" } else { s }, 8)
        .map_err(|_| PortableError::new(PortableErrorCode::InvalidStructure, "numeric field"))
}
fn cstr(field: &[u8]) -> Result<&str, PortableError> {
    let end = field.iter().position(|b| *b == 0).unwrap_or(field.len());
    std::str::from_utf8(&field[..end])
        .map_err(|_| PortableError::new(PortableErrorCode::InvalidPath, "tar path UTF-8"))
}
fn header_path(h: &[u8; 512]) -> Result<String, PortableError> {
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
) -> Result<(), PortableError> {
    if used_pax {
        return Ok(());
    }
    let (prefix, name) = canonical_ustar_split(path).ok_or_else(|| {
        PortableError::at(
            PortableErrorCode::InvalidStructure,
            path,
            "missing required PAX header",
        )
    })?;
    if cstr(&h[..100])? != name || cstr(&h[345..500])? != prefix {
        return Err(PortableError::at(
            PortableErrorCode::InvalidStructure,
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
fn verify_header(h: &[u8; 512]) -> Result<(), PortableError> {
    if &h[257..263] != b"ustar\0"
        || &h[263..265] != b"00"
        || parse_octal(&h[100..108])? != 0o644
        || parse_octal(&h[108..116])? != 0
        || parse_octal(&h[116..124])? != 0
        || parse_octal(&h[136..148])? != 0
        || h[265..329].iter().any(|b| *b != 0)
    {
        return Err(PortableError::new(
            PortableErrorCode::InvalidStructure,
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
        return Err(PortableError::new(
            PortableErrorCode::DigestMismatch,
            "tar header checksum",
        ));
    }
    Ok(())
}
fn read_payload(
    r: &mut File,
    size: u64,
    h: &mut StreamHash,
    max: usize,
    c: Option<&AtomicBool>,
) -> Result<Vec<u8>, PortableError> {
    if size > max as u64 {
        return Err(PortableError::new(
            PortableErrorCode::LimitExceeded,
            "PAX record",
        ));
    }
    let allocation = usize::try_from(size).map_err(|_| {
        PortableError::new(
            PortableErrorCode::LimitExceeded,
            "PAX record does not fit address space",
        )
    })?;
    let mut v = vec![0; allocation];
    read_exact_hash(r, &mut v, h, "truncated payload")?;
    read_padding(r, size, h)?;
    check_cancel(c)?;
    Ok(v)
}
#[expect(
    clippy::too_many_arguments,
    reason = "canonical member authentication carries transport, bounded copy and private writer evidence"
)]
fn hash_payload(
    reader: &mut File,
    size: u64,
    transport_hash: &mut StreamHash,
    buffer: usize,
    retain_limit: Option<u64>,
    mut inventory: Option<&mut inventory::InventoryCheck<'_>>,
    cancelled: Option<&AtomicBool>,
    entry: &str,
    mut sink: Option<&mut materialization::CopySink<'_>>,
    expected: Option<([u8; 32], u64)>,
) -> Result<([u8; 32], Option<Vec<u8>>), PortableError> {
    let mut payload_hash = StreamHash::new(expected);
    let mut kept = retain_payload(size, retain_limit, entry)?;
    let mut output = sink
        .as_deref_mut()
        .map(|sink| sink.open(entry))
        .transpose()?
        .flatten();
    let mut checksum = output
        .as_ref()
        .map(|_| crate::corruption_checksum::Checksum::new());
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
        if let Some(checksum) = &mut checksum {
            checksum.update(&copy_buffer[..chunk_len]);
        }
        if let (Some(sink), Some(output)) = (sink.as_deref_mut(), output.as_mut()) {
            sink.write(entry, output, &copy_buffer[..chunk_len])?;
        }
        if let Some(v) = &mut kept {
            v.extend_from_slice(&copy_buffer[..chunk_len]);
        }
        if let Some(inventory) = inventory.as_deref_mut() {
            inventory.update(&copy_buffer[..chunk_len]);
        }
        left -= chunk_len as u64;
    }
    read_padding(reader, size, transport_hash)?;
    let digest = payload_hash.finish()?;
    if let (Some(sink), Some(output)) = (sink, output) {
        sink.finish(
            entry,
            output,
            size,
            digest,
            checksum
                .expect("materialized output has a checksum capture")
                .finish(),
        )?;
    }
    Ok((digest, kept))
}
fn read_padding(r: &mut File, size: u64, h: &mut StreamHash) -> Result<(), PortableError> {
    let n = (512 - size % 512) % 512;
    let mut p = vec![0; n as usize];
    read_exact_hash(r, &mut p, h, "truncated padding")?;
    if p.iter().any(|b| *b != 0) {
        return Err(PortableError::new(
            PortableErrorCode::InvalidStructure,
            "non-zero padding",
        ));
    }
    Ok(())
}
fn sha(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
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
/// Bytes the reader may retain of `path`, in a package of `package_entries`
/// regular entries.
fn retained_limit(path: &str, limits: PortableLimits, package_entries: u64) -> Option<u64> {
    match path {
        MANIFEST_PATH => Some(limits.semantic_manifest_bound(package_entries)),
        RUNTIME_MAP_PATH | ONTOLOGY_COMPOSITION_PATH => Some(limits.max_manifest_bytes),
        // The payload inventory, `manifest-sha256.txt`, grows with the entry
        // count and is checked as it streams instead (`inventory.rs`).
        "bagit.txt" | "bag-info.txt" | "tagmanifest-sha256.txt" => {
            Some(limits.max_tag_manifest_bytes)
        }
        _ => None,
    }
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
    use uuid::Uuid;

    pub(super) fn package() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        let mut value: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/portable/ontology-only.manifest.json"
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
        let full = verify_portable(
            root.path(),
            PortableMode::Full,
            PortableLimits::default(),
            None,
        )
        .unwrap();
        let structure = verify_portable(
            root.path(),
            PortableMode::StructureOnly,
            PortableLimits::default(),
            None,
        )
        .unwrap();
        assert_eq!(
            full.package_digest,
            "sha256:869da25f99c90864c321bf8c42aa3f1f3642c877b92bc34255c900d3083a525d"
        );
        assert_eq!(full.integrity, PortableIntegrity::Verified);
        assert_eq!(structure.integrity, PortableIntegrity::NotChecked);
        assert_eq!(full.entry_count, 6);
        assert!(full.transport_digest.is_some());
    }

    #[test]
    fn package_from_another_producer_revision_is_incompatible_not_tampered() {
        let root = package();
        let current = String::from_utf8(BAG_INFO.to_vec()).unwrap();
        let older = current.replace(
            "GraphForge portable\n",
            &format!("GraphForge portable-{}\n", "v2"),
        );
        let unrelated = current.replace("GraphForge portable\n", "GraphForge portable-x\n");
        for tag in [older, unrelated] {
            for mode in [PortableMode::Full, PortableMode::StructureOnly] {
                fs::write(root.path().join("bag-info.txt"), &tag).unwrap();
                let error = verify_portable(root.path(), mode, PortableLimits::default(), None)
                    .unwrap_err();
                assert_eq!(error.code, PortableErrorCode::Incompatible, "{tag:?}");
            }
        }
        // Any other deviation from the current tag stays a digest mismatch.
        for tag in [
            "Bag-Software-Agent: GraphForge portable\r\nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: GraphForge portable \nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: GraphForge portable-x \nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: GraphForge portable-\nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: GraphForge portable-x\r\nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: GraphForge portables\nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: other\nBagging-Date: 1970-01-01\n",
            "Bag-Software-Agent: GraphForge portable\nBagging-Date: 1971-01-01\n",
        ] {
            fs::write(root.path().join("bag-info.txt"), tag).unwrap();
            let error = verify_portable(
                root.path(),
                PortableMode::StructureOnly,
                PortableLimits::default(),
                None,
            )
            .unwrap_err();
            assert_eq!(error.code, PortableErrorCode::DigestMismatch, "{tag:?}");
        }
    }

    #[test]
    fn complete_import_refuses_selective_package_without_target_mutation() {
        let root = package();
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("project");
        let error = crate::import_complete_portable(
            root.path(),
            &target,
            Uuid::new_v4(),
            Uuid::new_v4(),
            &[],
            PortableLimits::default(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::Incompatible);
        assert!(!target.exists());
    }

    #[test]
    fn changed_payload_fails_at_bounded_relative_entry() {
        let root = package();
        let path = "data/components/ontology/core-ontology/ontology.json";
        fs::write(root.path().join(path), b"[]").unwrap();
        let error = verify_portable(
            root.path(),
            PortableMode::Full,
            PortableLimits::default(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::DigestMismatch);
        assert_eq!(error.entry.as_deref(), Some(path));
    }

    #[test]
    fn cancellation_and_limits_fail_before_success() {
        let root = package();
        let cancelled = AtomicBool::new(true);
        let error = verify_portable(
            root.path(),
            PortableMode::Full,
            PortableLimits::default(),
            Some(&cancelled),
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::Cancelled);
        let limits = PortableLimits {
            max_entries: 2,
            ..PortableLimits::default()
        };
        let error = verify_portable(root.path(), PortableMode::Full, limits, None).unwrap_err();
        assert_eq!(error.code, PortableErrorCode::LimitExceeded);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_and_case_fold_collision_fail_closed() {
        use std::os::unix::fs::symlink;
        let root = package();
        symlink(root.path().join("bagit.txt"), root.path().join("linked")).unwrap();
        let error = verify_portable(
            root.path(),
            PortableMode::Full,
            PortableLimits::default(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::InvalidStructure);
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
        let error = verify_portable(
            root.path(),
            PortableMode::Full,
            PortableLimits::default(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::DuplicateEntry);
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
    pub(super) fn tar_entry(path: &str, payload: &[u8]) -> Vec<u8> {
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
    fn bundle_preflight_bounds_header_scan_before_admission() {
        let parent = tempfile::tempdir().unwrap();
        let count_bundle = parent.path().join("count.gfpb");
        let mut bytes = Vec::new();
        for index in 0..3 {
            bytes.extend(tar_entry(&format!("data/payload-{index}"), b"x"));
        }
        bytes.extend([0_u8; 1024]);
        fs::write(&count_bundle, bytes).unwrap();
        let error = preflight_bundle(
            &count_bundle,
            PortableLimits {
                max_entries: 2,
                ..PortableLimits::default()
            },
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::LimitExceeded);

        let bytes_bundle = parent.path().join("bytes.gfpb");
        let mut bytes = tar_entry("data/payload", b"xx");
        bytes.extend([0_u8; 1024]);
        fs::write(&bytes_bundle, bytes).unwrap();
        let error = preflight_bundle(
            &bytes_bundle,
            PortableLimits {
                max_total_bytes: 1,
                ..PortableLimits::default()
            },
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::LimitExceeded);
    }

    #[test]
    fn equivalent_bundle_reports_same_semantic_identity() {
        let root = package();
        let mut paths = Vec::new();
        walk(
            root.path(),
            root.path(),
            &mut paths,
            PortableLimits::default(),
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
        let expanded = verify_portable(
            root.path(),
            PortableMode::Full,
            PortableLimits::default(),
            None,
        )
        .unwrap();
        let bundled = verify_portable(
            file.path(),
            PortableMode::Full,
            PortableLimits::default(),
            None,
        )
        .unwrap();
        assert_eq!(expanded.package_digest, bundled.package_digest);
        assert_eq!(expanded.component_count, bundled.component_count);
        assert_eq!(bundled.representation, PortableRepresentation::Bundle);
        assert_ne!(expanded.transport_digest, bundled.transport_digest);
    }

    /// One raw header with an explicit name, typeflag and `size` field.
    fn raw_header(name: &str, kind: u8, size_field: u64) -> Vec<u8> {
        let mut h = [0u8; 512];
        h[..name.len()].copy_from_slice(name.as_bytes());
        octal(&mut h[100..108], 0o644);
        octal(&mut h[108..116], 0);
        octal(&mut h[116..124], 0);
        octal(&mut h[124..136], size_field);
        octal(&mut h[136..148], 0);
        h[148..156].fill(b' ');
        h[156] = kind;
        h[257..263].copy_from_slice(b"ustar\0");
        h[263..265].copy_from_slice(b"00");
        let sum: u64 = h.iter().map(|b| u64::from(*b)).sum();
        h[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        h.to_vec()
    }

    fn padded(mut bytes: Vec<u8>) -> Vec<u8> {
        bytes.resize(bytes.len().div_ceil(512) * 512, 0);
        bytes
    }

    /// The fixture package as a bundle whose ontology payload is carried by
    /// a local PAX header with `records` and the given ustar `size` field.
    fn bundle_with_pax_entry(records: &str, size_field: u64) -> tempfile::NamedTempFile {
        const PAX_PATH: &str = "data/components/ontology/core-ontology/ontology.json";
        let root = package();
        let mut paths = Vec::new();
        walk(
            root.path(),
            root.path(),
            &mut paths,
            PortableLimits::default(),
            None,
        )
        .unwrap();
        paths.sort();
        let mut bundle = Vec::new();
        for path in paths {
            let payload = fs::read(root.path().join(&path)).unwrap();
            let length = payload.len() as u64;
            // Every other entry keeps its canonical encoding at the current limit.
            let (records, size_field) = if path == PAX_PATH {
                (records.to_owned(), size_field)
            } else if length > pax::ustar_size_limit() {
                (pax::encode(&path, length), 0)
            } else {
                bundle.extend(tar_entry(&path, &payload));
                continue;
            };
            let suffix = &hex(&ContractSha256::digest(path.as_bytes()))[..16];
            bundle.extend(raw_header(
                &format!("PaxHeaders/{suffix}"),
                b'x',
                records.len() as u64,
            ));
            bundle.extend(padded(records.as_bytes().to_vec()));
            bundle.extend(raw_header(&format!("PaxFiles/{suffix}"), b'0', size_field));
            bundle.extend(padded(payload));
        }
        bundle.extend([0u8; 1024]);
        let file = tempfile::NamedTempFile::new().unwrap();
        fs::write(file.path(), bundle).unwrap();
        file
    }

    /// Run the bounded preflight census and the authenticating scan
    /// independently, so each bundle reader is held to the refusal.
    fn read_both(bundle: &Path) -> [Result<(), PortableError>; 2] {
        let limits = PortableLimits::default();
        [
            preflight(bundle, limits, None),
            scan(bundle, PortableMode::Full, limits, None, None, None).map(|_| ()),
        ]
    }

    /// The fixture package as a bundle that also carries `extra` paths, each
    /// as a PAX-addressed entry so that any path can be written.
    fn bundle_with_extra_paths(extra: &[&str]) -> tempfile::NamedTempFile {
        let root = package();
        let mut paths = Vec::new();
        walk(
            root.path(),
            root.path(),
            &mut paths,
            PortableLimits::default(),
            None,
        )
        .unwrap();
        let mut entries = paths
            .into_iter()
            .map(|path| {
                let payload = fs::read(root.path().join(&path)).unwrap();
                (path, payload)
            })
            .collect::<Vec<_>>();
        entries.extend(extra.iter().map(|path| ((*path).to_owned(), b"x".to_vec())));
        entries.sort();
        let mut bundle = Vec::new();
        for (path, payload) in entries {
            if canonical_ustar_split(&path).is_some() {
                bundle.extend(tar_entry(&path, &payload));
                continue;
            }
            let records = pax::record("path", &path);
            let suffix = &hex(&ContractSha256::digest(path.as_bytes()))[..16];
            bundle.extend(raw_header(
                &format!("PaxHeaders/{suffix}"),
                b'x',
                records.len() as u64,
            ));
            bundle.extend(padded(records.into_bytes()));
            bundle.extend(raw_header(
                &format!("PaxFiles/{suffix}"),
                b'0',
                payload.len() as u64,
            ));
            bundle.extend(padded(payload));
        }
        bundle.extend([0u8; 1024]);
        let file = tempfile::NamedTempFile::new().unwrap();
        fs::write(file.path(), bundle).unwrap();
        file
    }

    fn list_files(dir: &Path, prefix: &str, out: &mut Vec<String>) {
        for child in fs::read_dir(dir).unwrap() {
            let child = child.unwrap();
            let name = format!("{prefix}{}", child.file_name().to_string_lossy());
            if child.file_type().unwrap().is_dir() {
                list_files(&child.path(), &format!("{name}/"), out);
            } else {
                out.push(name);
            }
        }
    }

    /// The fixture package expanded on disk with `extra` added, or `None`
    /// (said on stderr) when this host's file system cannot hold every one of
    /// those names as distinct entries spelled exactly so: Windows refuses
    /// reserved names, ext4 refuses 256-byte names, and the default macOS and
    /// Windows volumes fold case variants into one file.
    fn expanded_with(extra: &[&str]) -> Option<tempfile::TempDir> {
        let root = package();
        let mut baseline = Vec::new();
        list_files(root.path(), "", &mut baseline);
        for path in extra {
            let file = root.path().join(path);
            let created =
                fs::create_dir_all(file.parent().unwrap()).and_then(|()| fs::write(&file, b"x"));
            if let Err(error) = created {
                eprintln!("expanded form skipped for {extra:?}: host refuses it ({error})");
                return None;
            }
        }
        let mut found = Vec::new();
        list_files(root.path(), "", &mut found);
        if found.len() != baseline.len() + extra.len()
            || extra.iter().any(|p| !found.contains(&(*p).to_owned()))
        {
            eprintln!("expanded form skipped for {extra:?}: host folds or rewrites the names");
            return None;
        }
        Some(root)
    }

    /// Every source form must be refused with `code` and `detail` by
    /// verification, and by materialization before it creates its destination
    /// or writes a byte. The bundle form is always built; the expanded form
    /// only where the host file system can represent the paths.
    fn assert_refused_before_extraction(extra: &[&str], code: PortableErrorCode, detail: &str) {
        let bundle = bundle_with_extra_paths(extra);
        let expanded = expanded_with(extra);
        let mut sources = vec![bundle.path().to_owned()];
        sources.extend(expanded.as_ref().map(|root| root.path().to_owned()));
        for source in sources {
            let verified =
                verify_portable(&source, PortableMode::Full, PortableLimits::default(), None)
                    .unwrap_err();
            let parent = tempfile::tempdir().unwrap();
            let destination = parent.path().join("extracted");
            let mut writes = 0;
            let materialized = materialization::materialize_verified_portable_observed(
                &source,
                &destination,
                PortableLimits::default(),
                None,
                |_, _| {
                    writes += 1;
                    Ok(())
                },
                true,
            )
            .err()
            .expect("an unportable path must not materialize");
            for error in [verified, materialized] {
                assert_eq!(error.code, code, "{extra:?}: {error}");
                assert!(error.to_string().contains(detail), "{extra:?}: {error}");
            }
            assert_eq!(writes, 0, "{extra:?}: bytes written before refusal");
            assert!(!destination.exists(), "{extra:?}: destination created");
            assert_eq!(fs::read_dir(parent.path()).unwrap().count(), 0);
        }
    }

    #[test]
    fn windows_reserved_device_names_are_refused_before_extraction() {
        for name in [
            "CON",
            "con",
            "Prn",
            "AUX",
            "NuL",
            "COM1",
            "com9",
            "LPT1",
            "lpt9",
            "COM\u{b9}",
            "COM\u{b2}",
            "COM\u{b3}",
            "LPT\u{b9}",
            "LPT\u{b2}",
            "LPT\u{b3}",
            "CON.txt",
            "nul.tar.gz",
            "COM1.json",
            "AUX .txt",
        ] {
            // Any directory or file component counts, not only the last.
            for path in [format!("data/{name}"), format!("data/{name}/entry")] {
                assert_refused_before_extraction(
                    &[&path],
                    PortableErrorCode::InvalidPath,
                    "Windows reserved device name",
                );
            }
        }
        // Names that merely contain or resemble a device name stay valid.
        for name in [
            "console",
            "COM0",
            "COM10",
            "LPT",
            "CONSOLE.txt",
            "com.json",
            "xCON",
        ] {
            assert_eq!(
                path_violation(&format!("data/{name}"), 4096),
                None,
                "{name}"
            );
        }
    }

    #[test]
    fn characters_windows_forbids_are_refused_before_extraction() {
        for character in ['<', '>', ':', '"', '|', '?', '*'] {
            assert_refused_before_extraction(
                &[&format!("data/a{character}b")],
                PortableErrorCode::InvalidPath,
                "character that Windows forbids",
            );
        }
        for control in ["\t", "\u{1f}", "\u{7f}"] {
            assert_refused_before_extraction(
                &[&format!("data/a{control}b")],
                PortableErrorCode::InvalidPath,
                "unsafe/non-canonical path",
            );
        }
    }

    #[test]
    fn components_ending_in_a_space_or_dot_are_refused_before_extraction() {
        for name in ["name ", "name.", "name..", "dir./entry", "dir /entry"] {
            assert_refused_before_extraction(
                &[&format!("data/{name}")],
                PortableErrorCode::InvalidPath,
                "ends with a space or a dot",
            );
        }
        // A leading dot is a valid Windows name.
        assert_eq!(path_violation("data/.hidden", 4096), None);
    }

    #[test]
    fn over_long_components_are_refused_before_extraction() {
        let longest = "a".repeat(MAX_COMPONENT_BYTES);
        assert_eq!(path_violation(&format!("data/{longest}"), 4096), None);
        // 256 bytes cannot be created on ext4, so only a bundle can carry it.
        let over = "a".repeat(MAX_COMPONENT_BYTES + 1);
        assert_refused_before_extraction(
            &[&format!("data/{over}")],
            PortableErrorCode::InvalidPath,
            "exceeds 255 bytes",
        );
        // 128 two-byte characters are 128 UTF-16 units but 256 bytes.
        let wide = "\u{e9}".repeat(128);
        assert_refused_before_extraction(
            &[&format!("data/{wide}")],
            PortableErrorCode::InvalidPath,
            "exceeds 255 bytes",
        );
    }

    #[test]
    fn case_insensitive_collisions_are_refused_before_extraction() {
        assert_refused_before_extraction(
            &["data/Readme.txt", "data/readme.txt"],
            PortableErrorCode::DuplicateEntry,
            "case-fold collision",
        );
        assert_refused_before_extraction(
            &["Data/a.txt", "data/A.TXT"],
            PortableErrorCode::DuplicateEntry,
            "case-fold collision",
        );
    }

    #[test]
    fn full_case_folding_collisions_are_refused_before_extraction() {
        for pair in [
            // Final and medial sigma fold to one letter.
            ["data/\u{3c3}.txt", "data/\u{3c2}.txt"],
            [
                "data/\u{39f}\u{394}\u{39f}\u{3a3}",
                "data/\u{3bf}\u{3b4}\u{3bf}\u{3c2}",
            ],
            // Full folding expands the sharp s.
            ["data/stra\u{df}e.txt", "data/strasse.txt"],
            ["data/STRASSE.txt", "data/stra\u{df}e.txt"],
            // Dotless i: full folding keeps it apart; some Windows tables join it.
            ["data/\u{131}.txt", "data/I.txt"],
            // Case differences in a directory component.
            ["data/Dir/a.txt", "data/dir/A.TXT"],
        ] {
            assert_refused_before_extraction(
                &pair,
                PortableErrorCode::DuplicateEntry,
                "case-fold collision",
            );
        }
        // Distinct names stay distinct.
        assert_ne!(
            path_collision_key("data/strase.txt"),
            path_collision_key("data/strasse.txt")
        );
    }

    #[test]
    fn a_file_may_not_be_another_entrys_directory() {
        for pair in [
            ["data/a", "data/a/b"],
            ["data/A", "data/a/b"],
            ["data/x/y", "data/X/Y/z"],
            ["data/stra\u{df}e", "data/STRASSE/z"],
        ] {
            assert_refused_before_extraction(
                &pair,
                PortableErrorCode::DuplicateEntry,
                "file/directory path collision",
            );
        }
        // A shared directory is fine.
        validate_path_set(&["data/a/b".into(), "data/a/c".into(), "data/A2".into()]).unwrap();
    }

    #[test]
    fn unicode_normalization_variants_are_refused_before_extraction() {
        // NFD `e` + combining acute: macOS would fold it onto the NFC name.
        assert_refused_before_extraction(
            &["data/caf\u{65}\u{301}.txt"],
            PortableErrorCode::InvalidPath,
            "unsafe/non-canonical path",
        );
        // A canonical pair collides under normalization after case folding.
        assert_eq!(
            path_collision_key("data/\u{c9}.txt"),
            path_collision_key("data/\u{e9}.txt")
        );
    }

    #[test]
    fn empty_and_dot_components_are_refused() {
        for path in ["data//a", "data/./a", "data/a/", "data/../a"] {
            assert!(path_violation(path, 4096).is_some(), "{path}");
        }
    }

    #[test]
    fn bundle_readers_accept_a_canonical_pax_size_and_refuse_malformed_ones() {
        const PAX_PATH: &str = "data/components/ontology/core-ontology/ontology.json";
        let path = pax::record("path", PAX_PATH);
        let size = |value: &str| format!("{path}{}", pax::record("size", value));
        // The payload is `{}`; a limit of 1 makes it an oversized entry.
        let _limit = pax::test_seam::lower_ustar_size_limit(1);

        let canonical = bundle_with_pax_entry(&size("2"), 0);
        for result in read_both(canonical.path()) {
            result.unwrap();
        }
        let report = verify_portable(
            canonical.path(),
            PortableMode::Full,
            PortableLimits::default(),
            None,
        )
        .unwrap();
        assert_eq!(report.representation, PortableRepresentation::Bundle);

        let refused = [
            // The ustar field contradicts the size record.
            ("contradictory field", bundle_with_pax_entry(&size("2"), 2)),
            ("contradictory field", bundle_with_pax_entry(&size("2"), 1)),
            // A size above the limit without a size record.
            ("missing record", bundle_with_pax_entry(&path, 2)),
            // A size record the ustar field could carry.
            ("record fits field", bundle_with_pax_entry(&size("1"), 0)),
            // Malformed or non-canonical records.
            ("leading zero", bundle_with_pax_entry(&size("02"), 0)),
            ("non-digit", bundle_with_pax_entry(&size("2x"), 0)),
            (
                "overflow",
                bundle_with_pax_entry(&size("18446744073709551616"), 0),
            ),
            (
                "wrong record length",
                bundle_with_pax_entry(&format!("{path}8 size=2\n"), 0),
            ),
            (
                "non-minimal record length",
                bundle_with_pax_entry(&format!("{path}10 size=2\n"), 0),
            ),
            (
                "size before path",
                bundle_with_pax_entry(&format!("{}{path}", pax::record("size", "2")), 0),
            ),
            (
                "unknown keyword",
                bundle_with_pax_entry(&format!("{path}{}", pax::record("mtime", "0")), 0),
            ),
            (
                "duplicate size",
                bundle_with_pax_entry(&format!("{}{}", size("2"), pax::record("size", "2")), 0),
            ),
            // A declared size beyond the archive cannot be misread as payload.
            ("beyond archive", bundle_with_pax_entry(&size("1048576"), 0)),
        ];
        for (case, bundle) in refused {
            for result in read_both(bundle.path()) {
                assert_eq!(
                    result.unwrap_err().code,
                    PortableErrorCode::InvalidStructure,
                    "{case}"
                );
            }
        }
    }

    #[test]
    fn a_pax_header_for_a_path_that_fits_ustar_is_not_canonical() {
        let records = pax::record(
            "path",
            "data/components/ontology/core-ontology/ontology.json",
        );
        let bundle = bundle_with_pax_entry(&records, 2);
        let error = scan(
            bundle.path(),
            PortableMode::Full,
            PortableLimits::default(),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, PortableErrorCode::InvalidStructure);
    }
}
