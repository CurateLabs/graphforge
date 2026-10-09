//! Authenticated identity authority and orphan maintenance.

use super::storage_err;
use super::topology_delta::hex_sha256;
use super::topology_delta::read_bounded;
use super::TopologyIndexReceipt;
use super::UuidIndexOrphanGcWork;
use super::INDEX_DIR;
use super::V4_ORDINAL_MANIFEST;
use super::V4_ORDINAL_RECEIPT;
use graphforge_core::GfError;
use std::collections::BTreeSet;
use std::path::Path;

/// Reclaim a bounded number of unreachable immutable UUID runs under the
/// recovered project rewrite lock.
pub fn maintain_uuid_membership_orphans(
    project_dir: &Path,
    maximum: usize,
) -> Result<UuidIndexOrphanGcWork, GfError> {
    let selected = selected_generation_for_graph_root(project_dir)?;
    let ordinal_authority = selected
        .as_ref()
        .map(crate::ResolvedProjectGeneration::authenticated_v4_ordinal_authority)
        .transpose()?
        .flatten();
    maintain_uuid_membership_orphans_with_authorities(
        project_dir,
        maximum,
        ordinal_authority.as_ref(),
    )
}

/// Retain the selected project generation whenever `graph_root` is its
/// generation-owned graph tree. Standalone graph roots deliberately have no
/// project-generation provenance and therefore cannot authorize a present v4
/// facet.
pub(super) fn selected_generation_for_graph_root(
    graph_root: &Path,
) -> Result<Option<crate::ResolvedProjectGeneration>, GfError> {
    let Some(generation_root) = graph_root
        .file_name()
        .filter(|name| *name == std::ffi::OsStr::new("graph"))
        .and_then(|_| graph_root.parent())
    else {
        return Ok(None);
    };
    let Some(project_generations_dir) = generation_root.parent() else {
        return Ok(None);
    };
    if project_generations_dir.file_name() != Some(std::ffi::OsStr::new("generations")) {
        return Ok(None);
    }
    let Some(container_root) = project_generations_dir.parent() else {
        return Ok(None);
    };
    let selected = crate::resolve_project_generation(container_root)?;
    let supplied = graph_root.canonicalize().map_err(storage_err)?;
    let authenticated = selected
        .graph_tree_root()
        .canonicalize()
        .map_err(storage_err)?;
    if supplied != authenticated {
        return Err(storage_err(
            "graph root is not the currently selected project generation",
        ));
    }
    Ok(Some(selected))
}

/// Pin a standalone v4 facet through the admitted local receipt while the
/// caller is about to enter the one project rewrite critical section. Project
/// generation roots instead require their externally selected graph/files
/// authority and never use this local path.
pub(super) fn standalone_v4_pinned_update(
    project_root: &Path,
    generation: u64,
) -> Result<Option<crate::ordinal_identity_v4::V4OrdinalPinnedUpdateInputs>, GfError> {
    let index_path = project_root.join(INDEX_DIR);
    let index = match graphforge_filesystem::StableDirectory::open(&index_path) {
        Ok(index) => index,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage_err(error)),
    };
    let mut manifest_file = match index.open_child_file(std::ffi::OsStr::new(V4_ORDINAL_MANIFEST)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(storage_err(error)),
    };
    let mut receipt_file = index
        .open_child_file(std::ffi::OsStr::new(V4_ORDINAL_RECEIPT))
        .map_err(storage_err)?;
    let manifest = read_bounded(
        &mut manifest_file,
        crate::ordinal_identity_v4::MAX_MANIFEST_BYTES,
    )?;
    let receipt = read_bounded(
        &mut receipt_file,
        crate::ordinal_identity_v4::MAX_MANIFEST_BYTES,
    )?;
    let receipt: TopologyIndexReceipt = serde_json::from_slice(&receipt).map_err(storage_err)?;
    let manifest_sha256 = hex_sha256(&manifest);
    if receipt.expected_generation != generation
        || receipt.manifest_sha256 != manifest_sha256
        || receipt.nonce.len() != 32
        || !receipt
            .nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(storage_err(
            "standalone v4 receipt does not authenticate current authority",
        ));
    }
    index.revalidate_named().map_err(storage_err)?;
    let authority = crate::ordinal_identity_v4::V4OrdinalIdentityAuthority {
        topology_generation: generation,
        manifest_sha256,
    };
    match crate::ordinal_identity_v4::V4OrdinalIdentityHandle::open(
        project_root,
        &authority,
        crate::V4OrdinalIdentityLimits::default(),
    )
    .map_err(storage_err)?
    {
        crate::V4OrdinalIdentityOpen::Ready(mut handle) => {
            handle.pinned_update_inputs().map(Some).map_err(storage_err)
        }
        crate::V4OrdinalIdentityOpen::RebuildRequired { .. } => Err(storage_err(
            "standalone v4 ordinal identity requires rebuild before mutation",
        )),
    }
}

#[cfg(test)]
pub(crate) fn maintain_uuid_membership_orphans_with_ordinal_authority(
    project_dir: &Path,
    maximum: usize,
    ordinal_authority: Option<&crate::AuthenticatedV4OrdinalIdentityAuthority>,
) -> Result<UuidIndexOrphanGcWork, GfError> {
    maintain_uuid_membership_orphans_with_authorities(project_dir, maximum, ordinal_authority)
}

fn maintain_uuid_membership_orphans_with_authorities(
    project_dir: &Path,
    maximum: usize,
    ordinal_authority: Option<&crate::AuthenticatedV4OrdinalIdentityAuthority>,
) -> Result<UuidIndexOrphanGcWork, GfError> {
    crate::durable_rewrite::with_rewrite_lock(project_dir, |project| {
        collect_uuid_orphans_locked(project, project_dir, maximum, ordinal_authority)
    })
}

pub(super) fn collect_uuid_orphans_locked(
    project: &graphforge_filesystem::StableDirectory,
    project_root: &Path,
    maximum: usize,
    ordinal_authority: Option<&crate::AuthenticatedV4OrdinalIdentityAuthority>,
) -> Result<UuidIndexOrphanGcWork, GfError> {
    let topology = match project.open_child_directory(std::ffi::OsStr::new("topology")) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(UuidIndexOrphanGcWork::default());
        }
        Err(error) => return Err(storage_err(error)),
    };
    let index = match topology.open_child_directory(std::ffi::OsStr::new("uuid-membership")) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(UuidIndexOrphanGcWork::default());
        }
        Err(error) => return Err(storage_err(error)),
    };
    // Legacy membership runs (`identities-v5-*`, `node-surrogates-v5-*`) are
    // not authority any more; they are never candidates for collection.
    let referenced = authenticated_v4_references(project_root, ordinal_authority)?;
    let mut names = index.child_names().map_err(storage_err)?;
    names.sort();
    let mut work = UuidIndexOrphanGcWork::default();
    let mut retirement =
        crate::durable_commit::RetirementBatch::new(&index).map_err(storage_err)?;
    for name in names {
        let Some(text) = name.to_str() else { continue };
        if referenced.contains(text) || !is_canonical_run_name(text) {
            continue;
        }
        work.candidates = work.candidates.saturating_add(1);
        if usize::try_from(work.candidates).unwrap_or(usize::MAX) > maximum {
            work.deferred = work.deferred.saturating_add(1);
            work.deferred_limit = work.deferred_limit.saturating_add(1);
            continue;
        }
        let file = index.open_child_file(&name).map_err(storage_err)?;
        if graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1 {
            work.deferred = work.deferred.saturating_add(1);
            work.deferred_linked = work.deferred_linked.saturating_add(1);
            continue;
        }
        let bytes = file.metadata().map_err(storage_err)?.len();
        let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
        if is_canonical_v4_artifact_prefix(
            text.strip_suffix(".uuidx")
                .and_then(|stem| stem.rsplit_once('-').map(|(prefix, _)| prefix))
                .unwrap_or_default(),
        ) {
            crate::project_failpoint::hit(
                "v4_cleanup.before_unlink",
                None,
                None,
                "V4_CLEANUP_BEFORE_UNLINK",
                false,
            )?;
        }
        retirement.unlink(&name, identity).map_err(storage_err)?;
        if text.starts_with("forward-v4-")
            || text.starts_with("ordinal-v4-")
            || text.starts_with("tombstones-v4-")
        {
            crate::project_failpoint::hit(
                "v4_cleanup.after_unlink",
                None,
                None,
                "V4_CLEANUP_AFTER_UNLINK",
                false,
            )?;
        }
        work.removed = work.removed.saturating_add(1);
        work.bytes = work.bytes.saturating_add(bytes);
    }
    if work.removed != 0 {
        retirement.acknowledge().map_err(storage_err)?;
    }
    index.revalidate_named().map_err(storage_err)?;
    topology.revalidate_named().map_err(storage_err)?;
    project.revalidate_named().map_err(storage_err)?;
    Ok(work)
}

fn authenticated_v4_references(
    project_root: &Path,
    authority: Option<&crate::AuthenticatedV4OrdinalIdentityAuthority>,
) -> Result<BTreeSet<String>, GfError> {
    let Some(authority) = authority else {
        let index = graphforge_filesystem::StableDirectory::open(&project_root.join(INDEX_DIR))
            .map_err(storage_err)?;
        return match index.open_child_file(std::ffi::OsStr::new(V4_ORDINAL_MANIFEST)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(BTreeSet::new()),
            Ok(_) => Err(storage_err(
                "v4 ordinal authority requires an authenticated selected generation",
            )),
            Err(error) => Err(storage_err(error)),
        };
    };
    match authority
        .open(project_root, crate::V4OrdinalIdentityLimits::default())
        .map_err(storage_err)?
    {
        crate::V4OrdinalIdentityOpen::Ready(handle) => Ok(handle.referenced_file_names()),
        crate::V4OrdinalIdentityOpen::RebuildRequired { .. } => {
            Err(storage_err("v4 ordinal authority requires rebuild"))
        }
    }
}

fn is_canonical_run_name(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".uuidx") else {
        return false;
    };
    let Some((prefix, digest)) = stem.rsplit_once('-') else {
        return false;
    };
    let is_v4 = is_canonical_v4_artifact_prefix(prefix);
    is_v4
        && digest.len() == 16
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn is_canonical_v4_artifact_prefix(prefix: &str) -> bool {
    let Some((kind, generation)) = prefix.rsplit_once('-') else {
        return false;
    };
    matches!(kind, "forward-v4" | "ordinal-v4" | "tombstones-v4")
        && generation.parse::<u64>().is_ok_and(|value| value != 0)
}

#[cfg(test)]
mod tests;
