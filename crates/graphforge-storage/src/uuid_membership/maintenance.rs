//! Authenticated identity authority and orphan maintenance.

use super::INDEX_DIR;
use super::MANIFEST;
use super::MAX_MANIFEST_BYTES;
use super::Manifest;
use super::TOPOLOGY_RECEIPT;
use super::TopologyIndexReceipt;
use super::UuidIndexOrphanGcWork;
use super::V4_ORDINAL_MANIFEST;
use super::V4_ORDINAL_RECEIPT;
use super::storage_err;
use super::topology_delta::hex_sha256;
use super::topology_delta::read_bounded;
use super::validate_run_descriptors;
use graphforge_core::GfError;
use std::collections::BTreeSet;
#[cfg(test)]
use std::fs;
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
    let membership_authority = selected
        .as_ref()
        .map(authenticated_v3_membership_authority)
        .transpose()?
        .flatten();
    maintain_uuid_membership_orphans_with_authorities(
        project_dir,
        maximum,
        membership_authority.as_ref(),
        ordinal_authority.as_ref(),
    )
}

#[derive(Clone, Debug)]
pub(super) struct AuthenticatedV3MembershipAuthority {
    topology_generation: u64,
    manifest_sha256: String,
}

pub(super) fn authenticated_v3_membership_authority(
    selected: &crate::ResolvedProjectGeneration,
) -> Result<Option<AuthenticatedV3MembershipAuthority>, GfError> {
    let mut state = crate::graph_manifest::GraphManifestTargetedState::default();
    let receipt = selected.authenticated_graph_file_bytes_with_state(
        &format!("{INDEX_DIR}/{TOPOLOGY_RECEIPT}"),
        MAX_MANIFEST_BYTES,
        Some(&mut state),
    )?;
    let manifest = selected.authenticated_graph_file_bytes_with_state(
        &format!("{INDEX_DIR}/{MANIFEST}"),
        MAX_MANIFEST_BYTES,
        Some(&mut state),
    )?;
    match (receipt, manifest) {
        (None, None) => Ok(None),
        (None, Some(_)) | (Some(_), None) => Err(storage_err(
            "selected UUID membership facet has incomplete authority residue",
        )),
        (Some((_, receipt_bytes)), Some((manifest_entry, manifest_bytes))) => {
            let receipt: TopologyIndexReceipt =
                serde_json::from_slice(&receipt_bytes).map_err(storage_err)?;
            let generation = selected
                .authenticated_graph_file_bytes_with_state(
                    "topology/generation.json",
                    MAX_MANIFEST_BYTES,
                    Some(&mut state),
                )?
                .ok_or_else(|| storage_err("selected topology generation authority is absent"))?;
            let generation: serde_json::Value =
                serde_json::from_slice(&generation.1).map_err(storage_err)?;
            let topology_generation = generation
                .get("topology_generation")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| storage_err("selected topology generation is missing"))?;
            let manifest_sha256 = hex_sha256(&manifest_bytes);
            let canonical_hex = |value: &str, length: usize| {
                value.len() == length
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            };
            if !canonical_hex(&receipt.nonce, 32)
                || !canonical_hex(&receipt.topology_delta_sha256, 64)
                || !canonical_hex(&receipt.manifest_sha256, 64)
                || receipt.expected_generation != topology_generation
                || receipt.manifest_sha256 != manifest_entry.content_sha256
                || receipt.manifest_sha256 != manifest_sha256
            {
                return Err(storage_err(
                    "selected UUID membership receipt does not authenticate its manifest",
                ));
            }
            Ok(Some(AuthenticatedV3MembershipAuthority {
                topology_generation,
                manifest_sha256,
            }))
        }
    }
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
        crate::V4OrdinalIdentityOpen::Ready(handle) => {
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
    let manifest_bytes =
        fs::read(project_dir.join(INDEX_DIR).join(MANIFEST)).map_err(storage_err)?;
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).map_err(storage_err)?;
    let membership_authority = AuthenticatedV3MembershipAuthority {
        topology_generation: manifest.current_generation,
        manifest_sha256: hex_sha256(&manifest_bytes),
    };
    maintain_uuid_membership_orphans_with_authorities(
        project_dir,
        maximum,
        Some(&membership_authority),
        ordinal_authority,
    )
}

fn maintain_uuid_membership_orphans_with_authorities(
    project_dir: &Path,
    maximum: usize,
    membership_authority: Option<&AuthenticatedV3MembershipAuthority>,
    ordinal_authority: Option<&crate::AuthenticatedV4OrdinalIdentityAuthority>,
) -> Result<UuidIndexOrphanGcWork, GfError> {
    crate::durable_rewrite::with_rewrite_lock(project_dir, |project| {
        collect_uuid_orphans_locked(
            project,
            project_dir,
            maximum,
            membership_authority,
            ordinal_authority,
        )
    })
}

pub(super) fn collect_uuid_orphans_locked(
    project: &graphforge_filesystem::StableDirectory,
    project_root: &Path,
    maximum: usize,
    membership_authority: Option<&AuthenticatedV3MembershipAuthority>,
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
    let mut manifest_file = index
        .open_child_file(std::ffi::OsStr::new(MANIFEST))
        .map_err(storage_err)?;
    let manifest_bytes = read_bounded(&mut manifest_file, MAX_MANIFEST_BYTES)?;
    let membership_authority = membership_authority.ok_or_else(|| {
        storage_err("UUID membership orphan maintenance requires selected generation authority")
    })?;
    if hex_sha256(&manifest_bytes) != membership_authority.manifest_sha256 {
        return Err(storage_err(
            "UUID membership manifest differs from selected generation authority",
        ));
    }
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).map_err(storage_err)?;
    if manifest.current_generation != membership_authority.topology_generation {
        return Err(storage_err(
            "UUID membership generation differs from selected generation authority",
        ));
    }
    validate_run_descriptors(&manifest)?;
    let mut referenced = manifest_file_names(&manifest);
    referenced.extend(authenticated_v4_references(
        project_root,
        ordinal_authority,
    )?);
    let mut names = index.child_names().map_err(storage_err)?;
    names.sort();
    let mut work = UuidIndexOrphanGcWork::default();
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
        index
            .unlink_child_if_identity(&name, identity)
            .map_err(storage_err)?;
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
        index.sync().map_err(storage_err)?;
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
    ((prefix.starts_with("identities-v5") || prefix.starts_with("node-surrogates-v5")) || is_v4)
        && digest.len() == 16
        && if is_v4 {
            digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        } else {
            digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        }
}

fn is_canonical_v4_artifact_prefix(prefix: &str) -> bool {
    let Some((kind, generation)) = prefix.rsplit_once('-') else {
        return false;
    };
    matches!(kind, "forward-v4" | "ordinal-v4" | "tombstones-v4")
        && generation.parse::<u64>().is_ok_and(|value| value != 0)
}

pub(super) fn manifest_file_names(manifest: &Manifest) -> BTreeSet<String> {
    manifest
        .runs
        .iter()
        .flat_map(|run| {
            [
                run.identities.name.clone(),
                run.node_surrogates.name.clone(),
            ]
        })
        .collect()
}

#[cfg(test)]
pub(super) fn cleanup_superseded_files(
    root: &Path,
    prior: BTreeSet<String>,
    manifest: &Manifest,
) -> Result<(), GfError> {
    let retained = manifest_file_names(manifest);
    let directory = graphforge_filesystem::StableDirectory::open(root).map_err(storage_err)?;
    for name in prior.difference(&retained) {
        let file = directory
            .open_child_file(std::ffi::OsStr::new(name))
            .map_err(storage_err)?;
        let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
        directory
            .unlink_child_if_identity(std::ffi::OsStr::new(name), identity)
            .map_err(storage_err)?;
    }
    directory.sync().map_err(storage_err)
}

#[cfg(test)]
mod tests;
