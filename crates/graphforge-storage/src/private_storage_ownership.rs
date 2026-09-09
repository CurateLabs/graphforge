//! Quiescent allocation ownership of retained private project state.
//!
//! This is separate from the authenticated published-project union. Callers
//! must hold the lifecycle boundary quiescent; this inventory is not an ingest
//! progress probe and does not read graph payload bytes.

use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

use graphforge_core::GfError;
use graphforge_filesystem::StableDirectory;

use crate::{ArtifactCategory, ArtifactStorageTotals};

/// Raw allocation facts for one live private owner. Native identities remain
/// process-local and are deliberately not serializable.
#[derive(Debug, Clone, Default)]
pub struct PrivateStorageOwner {
    /// Reconciled owner facts, including multiple references to shared files.
    pub totals: ArtifactStorageTotals,
    /// Physical identity allocation for cross-owner union accounting.
    pub physical_identity_allocated_bytes: BTreeMap<String, u64>,
    physical_identity_logical_bytes: BTreeMap<String, u64>,
}

/// Complete private/control owner inventory at one quiescent boundary.
#[derive(Debug, Clone, Default)]
pub struct PrivateStorageOwnership {
    /// Canonical owner names and their category/facts. Names contain no paths
    /// or operation identities.
    pub owners: BTreeMap<String, (ArtifactCategory, PrivateStorageOwner)>,
}

impl PrivateStorageOwnership {
    /// Aggregate category facts while deduplicating physical files shared by owners.
    ///
    /// # Errors
    /// Refuses inconsistent physical facts or arithmetic overflow.
    pub fn category_totals(
        &self,
    ) -> Result<BTreeMap<ArtifactCategory, ArtifactStorageTotals>, GfError> {
        let mut categories = BTreeMap::<ArtifactCategory, PrivateStorageOwner>::new();
        for (category, owner) in self.owners.values() {
            let combined = categories.entry(*category).or_default();
            combined.totals.logical_references = add(
                combined.totals.logical_references,
                owner.totals.logical_references,
            )?;
            combined.totals.logical_bytes =
                add(combined.totals.logical_bytes, owner.totals.logical_bytes)?;
            for (identity, allocated) in &owner.physical_identity_allocated_bytes {
                let logical = owner
                    .physical_identity_logical_bytes
                    .get(identity)
                    .ok_or_else(|| storage("private owner omitted physical logical bytes"))?;
                if let Some(previous) = combined.physical_identity_allocated_bytes.get(identity) {
                    if previous != allocated
                        || combined.physical_identity_logical_bytes.get(identity) != Some(logical)
                    {
                        return Err(storage("private owners disagree on physical identity"));
                    }
                    continue;
                }
                combined
                    .physical_identity_allocated_bytes
                    .insert(identity.clone(), *allocated);
                combined
                    .physical_identity_logical_bytes
                    .insert(identity.clone(), *logical);
                combined.totals.physical_objects = add(combined.totals.physical_objects, 1)?;
                combined.totals.physical_logical_bytes =
                    add(combined.totals.physical_logical_bytes, *logical)?;
                combined.totals.allocated_bytes = add(combined.totals.allocated_bytes, *allocated)?;
            }
        }
        Ok(categories
            .into_iter()
            .map(|(category, owner)| (category, owner.totals))
            .collect())
    }
}

/// Capture raw facts for explicitly declared completed non-project artifacts.
///
/// # Errors
/// Rejects missing paths, links, special files, and oversized inventories.
pub fn capture_artifact_storage_ownership(
    paths: &[std::path::PathBuf],
) -> Result<PrivateStorageOwner, GfError> {
    let mut owner = PrivateStorageOwner::default();
    let mut remaining = 1_000_000;
    for path in paths {
        let metadata = std::fs::symlink_metadata(path).map_err(storage)?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            capture_directory(
                StableDirectory::open(path).map_err(storage)?,
                &mut owner,
                &mut remaining,
            )?;
        } else if metadata.is_file() && !metadata.file_type().is_symlink() {
            let parent = StableDirectory::open(
                path.parent()
                    .ok_or_else(|| storage("artifact has no parent"))?,
            )
            .map_err(storage)?;
            capture_file(
                &parent
                    .open_child_file(
                        path.file_name()
                            .ok_or_else(|| storage("artifact has no name"))?,
                    )
                    .map_err(storage)?,
                &mut owner,
            )?;
        } else {
            return Err(storage("artifact is a link or special file"));
        }
    }
    Ok(owner)
}

/// Capture published raw reference/object facts from the explicit published
/// roots and require exact agreement with authenticated published ownership.
///
/// # Errors
/// Rejects links, unknown/missing physical ownership, and changed allocation.
pub fn capture_published_storage_ownership(
    selected: &crate::ResolvedProjectGeneration,
) -> Result<PrivateStorageOwner, GfError> {
    let admitted = crate::capture_project_storage_identity_union(selected)?;
    let root = StableDirectory::open(selected.container_root()).map_err(storage)?;
    let mut owner = PrivateStorageOwner::default();
    for name in ["FORMAT", "CURRENT"] {
        capture_file(
            &root.open_child_file(name.as_ref()).map_err(storage)?,
            &mut owner,
        )?;
    }
    let excluded = admitted
        .retained_generation_uuids
        .iter()
        .map(|uuid| {
            selected
                .container_root()
                .join("generations")
                .join(uuid.to_string())
                .join("lease.lock")
        })
        .collect::<std::collections::BTreeSet<_>>();
    let mut remaining = 1_000_000;
    for name in ["generations", crate::graph_object_store::GRAPH_OBJECTS_DIR] {
        match root.open_child_directory(name.as_ref()) {
            Ok(directory) => {
                capture_directory_skipping(directory, &mut owner, &mut remaining, &excluded)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && name != "generations" => {
            }
            Err(error) => return Err(storage(error)),
        }
    }
    if owner.physical_identity_allocated_bytes != admitted.physical_identity_allocated_bytes {
        return Err(storage(
            "published raw ownership differs from authenticated identity union",
        ));
    }
    Ok(owner)
}

/// Capture live construction, import and transaction-control files without
/// following links. Published generations/CAS are intentionally not traversed.
///
/// # Errors
/// Refuses links, special files, changed physical facts, overflow, and an
/// inventory exceeding the bounded entry budget.
pub fn capture_private_storage_ownership(
    project: &Path,
) -> Result<PrivateStorageOwnership, GfError> {
    let (project, admission_parent, admission_name) =
        crate::filesystem_admission::retained_project_control_paths(project)?;
    let root = StableDirectory::open(&project).map_err(storage)?;
    let names = root.child_names_bounded(4096).map_err(storage)?;
    let mut result = PrivateStorageOwnership::default();
    let mut remaining = 1_000_000_usize;
    for (directory, owner, category) in [
        (
            ".graphforge-construction",
            "construction",
            ArtifactCategory::ConstructionStaging,
        ),
        (
            "import-sessions",
            "import",
            ArtifactCategory::ConstructionStaging,
        ),
        (
            "transactions",
            "transactions",
            ArtifactCategory::CatalogAndManifests,
        ),
        ("locks", "locks", ArtifactCategory::CatalogAndManifests),
    ] {
        let mut snapshot = PrivateStorageOwner::default();
        if names.iter().any(|name| name == directory) {
            let retained = root
                .open_child_directory(directory.as_ref())
                .map_err(storage)?;
            capture_directory(retained, &mut snapshot, &mut remaining)?;
        }
        result.owners.insert(owner.to_owned(), (category, snapshot));
    }
    if names.iter().any(|name| name == "generations") {
        let generations = root
            .open_child_directory("generations".as_ref())
            .map_err(storage)?;
        for name in generations.child_names_bounded(4096).map_err(storage)? {
            uuid::Uuid::parse_str(
                name.to_str()
                    .ok_or_else(|| storage("generation name is not UTF-8"))?,
            )
            .map_err(storage)?;
            let generation = generations.open_child_directory(&name).map_err(storage)?;
            let lease = generation
                .open_child_file("lease.lock".as_ref())
                .map_err(storage)?;
            let (_, locks) = result
                .owners
                .get_mut("locks")
                .ok_or_else(|| storage("lock owner missing"))?;
            capture_file(&lease, locks)?;
        }
    }
    let parent = StableDirectory::open(&admission_parent).map_err(storage)?;
    let mut snapshot = PrivateStorageOwner::default();
    match parent.open_child_file(admission_name.as_ref()) {
        Ok(file) => capture_file(&file, &mut snapshot)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(storage(error)),
    }
    result.owners.insert(
        "admission_lock".to_owned(),
        (ArtifactCategory::CatalogAndManifests, snapshot),
    );
    Ok(result)
}

fn capture_directory(
    directory: StableDirectory,
    owner: &mut PrivateStorageOwner,
    remaining: &mut usize,
) -> Result<(), GfError> {
    capture_directory_skipping(
        directory,
        owner,
        remaining,
        &std::collections::BTreeSet::new(),
    )
}

fn capture_directory_skipping(
    directory: StableDirectory,
    owner: &mut PrivateStorageOwner,
    remaining: &mut usize,
    excluded: &std::collections::BTreeSet<std::path::PathBuf>,
) -> Result<(), GfError> {
    let mut pending = vec![directory];
    while let Some(directory) = pending.pop() {
        let names = directory.child_names_bounded(*remaining).map_err(storage)?;
        *remaining = remaining
            .checked_sub(names.len())
            .ok_or_else(|| storage("private inventory exceeded entry bound"))?;
        for name in names {
            if excluded.contains(&directory.path().join(&name)) {
                // Only exact, authenticated generation lease paths are controls.
                // Still open the file to reject a substituted link/directory.
                directory.open_child_file(&name).map_err(storage)?;
                continue;
            }
            if let Ok(child) = directory.open_child_directory(&name) {
                pending.push(child);
            } else {
                let file = directory.open_child_file(&name).map_err(storage)?;
                capture_file(&file, owner)?;
            }
        }
    }
    Ok(())
}

fn capture_file(file: &File, owner: &mut PrivateStorageOwner) -> Result<(), GfError> {
    let identity = graphforge_filesystem::file_identity(file).map_err(storage)?;
    let usage = graphforge_filesystem::file_space_usage(file).map_err(storage)?;
    let key =
        crate::storage_attribution::native_identity_key(identity.volume_serial, &identity.file_id);
    owner.totals.logical_references = add(owner.totals.logical_references, 1)?;
    owner.totals.logical_bytes = add(owner.totals.logical_bytes, usage.logical_bytes)?;
    if let Some(existing) = owner.physical_identity_allocated_bytes.get(&key) {
        if *existing != usage.allocated_bytes
            || owner.physical_identity_logical_bytes.get(&key) != Some(&usage.logical_bytes)
        {
            return Err(storage("private physical identity changed during capture"));
        }
    } else {
        owner
            .physical_identity_allocated_bytes
            .insert(key.clone(), usage.allocated_bytes);
        owner
            .physical_identity_logical_bytes
            .insert(key, usage.logical_bytes);
        owner.totals.physical_objects = add(owner.totals.physical_objects, 1)?;
        owner.totals.physical_logical_bytes =
            add(owner.totals.physical_logical_bytes, usage.logical_bytes)?;
        owner.totals.allocated_bytes = add(owner.totals.allocated_bytes, usage.allocated_bytes)?;
    }
    Ok(())
}

fn add(left: u64, right: u64) -> Result<u64, GfError> {
    left.checked_add(right)
        .ok_or_else(|| storage("private storage allocation overflow"))
}

fn storage(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_raw_union_keeps_exact_authority_and_classifies_generation_lease() {
        let root = tempfile::tempdir().unwrap();
        let selected = crate::open_or_initialize_project(root.path()).unwrap();
        let published = capture_published_storage_ownership(&selected).unwrap();
        let authoritative = crate::capture_project_storage_identity_union(&selected).unwrap();
        assert_eq!(
            published.physical_identity_allocated_bytes,
            authoritative.physical_identity_allocated_bytes
        );
        let controls = capture_private_storage_ownership(root.path()).unwrap();
        assert!(controls.owners["locks"].1.totals.logical_references >= 1);
        let lease = selected.generation_root().join("lease.lock");
        let file = std::fs::File::open(lease).unwrap();
        let mut expected = PrivateStorageOwner::default();
        capture_file(&file, &mut expected).unwrap();
        for identity in expected.physical_identity_allocated_bytes.keys() {
            assert!(
                controls.owners["locks"]
                    .1
                    .physical_identity_allocated_bytes
                    .contains_key(identity)
            );
            assert!(
                !published
                    .physical_identity_allocated_bytes
                    .contains_key(identity)
            );
        }
        std::fs::write(
            selected.generation_root().join("unknown.bin"),
            b"unknown published file",
        )
        .unwrap();
        assert!(
            capture_published_storage_ownership(&selected)
                .unwrap_err()
                .to_string()
                .contains("differs from authenticated")
        );
    }

    #[test]
    fn private_owners_preserve_raw_facts_and_deduplicate_shared_files() {
        let root = tempfile::tempdir().unwrap();
        for directory in [
            ".graphforge-construction/session",
            "import-sessions/session",
            "transactions",
            "locks",
        ] {
            std::fs::create_dir_all(root.path().join(directory)).unwrap();
        }
        let original = root.path().join(".graphforge-construction/session/data");
        std::fs::write(&original, [7_u8; 8192]).unwrap();
        std::fs::hard_link(
            &original,
            root.path().join(".graphforge-construction/session/alias"),
        )
        .unwrap();
        std::fs::hard_link(
            &original,
            root.path().join("import-sessions/session/source"),
        )
        .unwrap();
        std::fs::write(root.path().join("transactions/receipt.json"), b"{}").unwrap();
        std::fs::write(root.path().join("locks/write.lock"), []).unwrap();
        let snapshot = capture_private_storage_ownership(root.path()).unwrap();
        let construction = &snapshot.owners["construction"].1;
        assert_eq!(construction.totals.logical_references, 2);
        assert_eq!(construction.totals.physical_objects, 1);
        assert_eq!(construction.totals.logical_bytes, 16384);
        assert_eq!(construction.totals.physical_logical_bytes, 8192);
        assert_eq!(snapshot.owners["locks"].1.totals.physical_objects, 1);
        let mut lifecycle = crate::StorageAllocationLifecycle::default();
        for (name, (_, owner)) in &snapshot.owners {
            lifecycle
                .replace_owner(name, &owner.physical_identity_allocated_bytes)
                .unwrap();
        }
        let categories = snapshot.category_totals().unwrap();
        let staging = &categories[&ArtifactCategory::ConstructionStaging];
        assert_eq!(staging.logical_references, 3);
        assert_eq!(staging.logical_bytes, 24_576);
        assert_eq!(staging.physical_objects, 1);
        assert_eq!(staging.physical_logical_bytes, 8192);
        assert_eq!(staging.allocated_bytes, construction.totals.allocated_bytes);
        let expected = construction.totals.allocated_bytes
            + snapshot.owners["transactions"].1.totals.allocated_bytes
            + snapshot.owners["locks"].1.totals.allocated_bytes;
        assert_eq!(lifecycle.current_allocated_bytes(), expected);
        assert_eq!(
            snapshot.owners["import"]
                .1
                .physical_identity_allocated_bytes,
            construction.physical_identity_allocated_bytes
        );
    }

    #[test]
    fn relative_project_captures_only_its_resolved_admission_lock() {
        let cwd = std::env::current_dir().unwrap();
        let root = tempfile::tempdir_in(&cwd).unwrap();
        let relative = root.path().strip_prefix(&cwd).unwrap();
        let (_, parent, lock_name) =
            crate::filesystem_admission::retained_project_control_paths(root.path()).unwrap();
        let lock = parent.join(lock_name);
        std::fs::write(&lock, b"").unwrap();
        let neighbor = parent.join(format!(
            "{}.unrelated",
            lock.file_name().unwrap().to_str().unwrap()
        ));
        std::fs::write(&neighbor, b"unrelated").unwrap();
        let absolute = capture_private_storage_ownership(root.path()).unwrap();
        let relative = capture_private_storage_ownership(relative).unwrap();
        std::fs::remove_file(lock).unwrap();
        std::fs::remove_file(neighbor).unwrap();
        let absolute = &absolute.owners["admission_lock"].1;
        let relative = &relative.owners["admission_lock"].1;
        assert_eq!(
            absolute.physical_identity_allocated_bytes,
            relative.physical_identity_allocated_bytes
        );
        assert_eq!(absolute.totals.physical_objects, 1);
        assert_eq!(absolute.totals.logical_references, 1);
        assert_eq!(absolute.totals.logical_bytes, 0);
        assert_eq!(relative.totals, absolute.totals);
    }

    #[test]
    fn deep_private_tree_uses_iterative_descriptor_walk() {
        let root = tempfile::tempdir().unwrap();
        let mut path = root.path().join("transactions");
        for _ in 0..256 {
            std::fs::create_dir(&path).unwrap();
            path.push("d");
        }
        std::fs::write(path, b"deep").unwrap();
        let snapshot = capture_private_storage_ownership(root.path()).unwrap();
        assert_eq!(
            snapshot.owners["transactions"].1.totals.logical_references,
            1
        );
        assert_eq!(snapshot.owners["transactions"].1.totals.logical_bytes, 4);
    }

    #[cfg(unix)]
    #[test]
    fn private_owner_capture_refuses_links() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("transactions")).unwrap();
        std::fs::write(outside.path().join("receipt"), b"outside").unwrap();
        std::os::unix::fs::symlink(
            outside.path().join("receipt"),
            root.path().join("transactions/receipt"),
        )
        .unwrap();
        assert!(capture_private_storage_ownership(root.path()).is_err());
        std::fs::remove_file(root.path().join("transactions/receipt")).unwrap();
        std::fs::remove_dir(root.path().join("transactions")).unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("transactions")).unwrap();
        assert!(capture_private_storage_ownership(root.path()).is_err());
    }
}
