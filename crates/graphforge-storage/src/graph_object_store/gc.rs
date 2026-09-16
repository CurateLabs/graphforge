//! gc ownership for immutable graph objects.

use super::BTreeMap;
use super::BTreeSet;
use super::File;
use super::GRAPH_OBJECTS_DIR;
use super::GfError;
use super::GraphFilesRootV2;
use super::GraphObjectGcEvidence;
use super::GraphObjectGcGuard;
use super::Path;
use super::ProjectErrorCode;
use super::ReadOnlyCasRoot;
use super::read_graph_object_by_digest_from_cas;
use super::storage;
use super::try_begin_graph_object_gc;
use super::validate_digest;
use super::validation;

/// Capture every sealed CAS object and its lifecycle control by native identity.
///
/// This is a storage-owned, bounded phase-boundary inventory. It is never used
/// during active ingest; qualification calls it only while holding the CAS
/// shared lifecycle lock, so installed-but-unreferenced objects remain charged
/// until an explicit GC receipt removes them.
pub(crate) fn capture_retained_graph_object_identities(
    root: &Path,
) -> Result<BTreeMap<String, u64>, GfError> {
    const MAX_RETAINED_OBJECTS: usize = 4_000_000;
    if !root.join(GRAPH_OBJECTS_DIR).exists() {
        return Ok(BTreeMap::new());
    }
    let cas = ReadOnlyCasRoot::open(root)?;
    let mut identities = BTreeMap::new();
    add_retained_identity(&mut identities, &cas.lifecycle)?;
    let prefixes = cas
        .sha256
        .child_names_bounded(256)
        .map_err(|error| storage("inventory stable graph object prefixes", root, error))?;
    let mut remaining = MAX_RETAINED_OBJECTS;
    for prefix in prefixes {
        let prefix_text = prefix
            .to_str()
            .ok_or_else(|| validation("graph object prefix is not UTF-8"))?;
        if prefix_text.len() != 2
            || !prefix_text
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(validation("graph object prefix is not canonical"));
        }
        let bucket = cas
            .sha256
            .open_child_directory(&prefix)
            .map_err(|error| storage("open retained graph object bucket", root, error))?;
        let objects = bucket
            .child_names_bounded(remaining)
            .map_err(|error| storage("inventory retained graph object bucket", root, error))?;
        remaining = remaining.checked_sub(objects.len()).ok_or_else(|| {
            validation("retained graph object inventory exceeds attribution bound")
        })?;
        for object in objects {
            let suffix = object
                .to_str()
                .ok_or_else(|| validation("graph object name is not UTF-8"))?;
            validate_digest(&format!("{prefix_text}{suffix}"))?;
            let file = bucket
                .open_child_file(&object)
                .map_err(|error| storage("open retained graph object", root, error))?;
            add_retained_identity(&mut identities, &file)?;
        }
    }
    Ok(identities)
}

fn add_retained_identity(
    identities: &mut BTreeMap<String, u64>,
    file: &File,
) -> Result<(), GfError> {
    let identity = graphforge_filesystem::file_identity(file).map_err(|error| {
        storage(
            "identify retained graph object",
            Path::new(GRAPH_OBJECTS_DIR),
            error,
        )
    })?;
    let usage = graphforge_filesystem::file_space_usage(file).map_err(|error| {
        storage(
            "measure retained graph object",
            Path::new(GRAPH_OBJECTS_DIR),
            error,
        )
    })?;
    let key = retained_identity_key(identity);
    match identities.insert(key, usage.allocated_bytes) {
        Some(existing) if existing != usage.allocated_bytes => {
            Err(validation("retained graph object allocation changed"))
        }
        _ => Ok(()),
    }
}

fn retained_identity_key(identity: graphforge_filesystem::FileIdentity) -> String {
    use std::fmt::Write as _;
    let mut key = format!("{:016x}:", identity.volume_serial);
    for byte in identity.file_id {
        write!(&mut key, "{byte:02x}").expect("writing to String cannot fail");
    }
    key
}

/// Trace compact generation roots, then sweep unreachable CAS objects.
/// Marking completes successfully before any deletion begins.
pub fn gc_graph_objects(
    root: &Path,
    roots: &[GraphFilesRootV2],
    limits: crate::GraphManifestLimits,
) -> Result<GraphObjectGcEvidence, GfError> {
    let guard = try_begin_graph_object_gc(root)?.ok_or_else(|| GfError::Project {
        code: ProjectErrorCode::WriterBusy,
        message: "phase=GRAPH_OBJECT_GC committed=false cause=live_publication".into(),
    })?;
    gc_graph_objects_guarded(&guard, roots, limits)
}
#[allow(clippy::too_many_lines)]
pub(crate) fn gc_graph_objects_guarded(
    guard: &GraphObjectGcGuard,
    roots: &[GraphFilesRootV2],
    limits: crate::GraphManifestLimits,
) -> Result<GraphObjectGcEvidence, GfError> {
    guard.cas.revalidate_named()?;
    let mut marked = BTreeSet::new();
    for graph_root in roots {
        let mut segment_digests = Vec::new();
        let (files, _) = crate::resolve_graph_manifest(graph_root, limits, |digest| {
            segment_digests.push(digest.to_owned());
            read_graph_object_by_digest_from_cas(
                &guard.cas,
                digest,
                crate::graph_manifest::GRAPH_MANIFEST_NODE_MAX_BYTES,
            )
        })?;
        marked.extend(segment_digests);
        marked.extend(files.into_iter().map(|entry| entry.content_sha256));
    }
    let mut candidates = Vec::new();
    for prefix in guard.cas.sha256.child_names().map_err(|error| {
        storage(
            "read stable graph object prefixes",
            &guard.cas.diagnostic_root,
            error,
        )
    })? {
        let prefix_text = prefix
            .to_str()
            .ok_or_else(|| validation("graph object prefix is not UTF-8"))?;
        if prefix_text.len() != 2
            || !prefix_text
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(validation("graph object prefix is not canonical"));
        }
        let bucket = guard
            .cas
            .sha256
            .open_child_directory(&prefix)
            .map_err(|error| {
                storage(
                    "open stable graph object bucket",
                    &guard.cas.diagnostic_root,
                    error,
                )
            })?;
        for object in bucket.child_names().map_err(|error| {
            storage(
                "read stable graph object bucket",
                &guard.cas.diagnostic_root,
                error,
            )
        })? {
            let suffix = object
                .to_str()
                .ok_or_else(|| validation("graph object name is not UTF-8"))?;
            let digest = format!("{prefix_text}{suffix}");
            validate_digest(&digest)?;
            let file = bucket.open_child_file(&object).map_err(|error| {
                storage(
                    "open stable graph object candidate",
                    &guard.cas.diagnostic_root,
                    error,
                )
            })?;
            let metadata = file.metadata().map_err(|error| {
                storage(
                    "inspect stable graph object candidate",
                    &guard.cas.diagnostic_root,
                    error,
                )
            })?;
            if !metadata.is_file() {
                return Err(validation(
                    "graph object bucket contains a non-regular object",
                ));
            }
            if !marked.contains(&digest) {
                let identity = graphforge_filesystem::file_identity(&file).map_err(|error| {
                    storage(
                        "identify graph object candidate",
                        &guard.cas.diagnostic_root,
                        error,
                    )
                })?;
                let allocation =
                    graphforge_filesystem::file_space_usage(&file).map_err(|error| {
                        storage(
                            "measure graph object candidate",
                            &guard.cas.diagnostic_root,
                            error,
                        )
                    })?;
                candidates.push((
                    prefix.clone(),
                    object,
                    identity,
                    metadata.len(),
                    allocation.allocated_bytes,
                ));
            }
        }
    }
    let mut evidence = GraphObjectGcEvidence {
        objects_marked: u64::try_from(marked.len())
            .map_err(|_| validation("CAS marked object count exceeds u64"))?,
        ..GraphObjectGcEvidence::default()
    };
    for (prefix, object, identity, bytes, allocated) in candidates {
        let bucket = guard
            .cas
            .sha256
            .open_child_directory(&prefix)
            .map_err(|error| {
                storage(
                    "reopen stable graph object bucket",
                    &guard.cas.diagnostic_root,
                    error,
                )
            })?;
        bucket
            .unlink_child_if_identity(&object, identity)
            .map_err(|error| {
                storage(
                    "remove unreachable stable graph object",
                    &guard.cas.diagnostic_root,
                    error,
                )
            })?;
        bucket.sync().map_err(|error| {
            storage(
                "sync graph object bucket after GC removal",
                &guard.cas.diagnostic_root,
                error,
            )
        })?;
        evidence.objects_removed = evidence
            .objects_removed
            .checked_add(1)
            .ok_or_else(|| validation("CAS removed object count overflows"))?;
        evidence.bytes_removed = evidence
            .bytes_removed
            .checked_add(bytes)
            .ok_or_else(|| validation("CAS removed byte count overflows"))?;
        evidence
            .removed_identity_allocated_bytes
            .insert(retained_identity_key(identity), allocated);
    }
    Ok(evidence)
}

#[cfg(test)]
mod tests;
