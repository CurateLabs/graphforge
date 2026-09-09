//! Explicit, operation-scoped allocation accounting for first-party diagnostics.
//!
//! The context retains the current owner union and a numeric high-water mark,
//! never an event history. Writers report actual file facts at their existing
//! installation/removal boundaries. No process-global observer is installed.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};

use graphforge_core::GfError;

use crate::{StorageAllocationLifecycle, StorageAllocationTransition};

/// First-party operation evidence context. Native identities must remain on
/// the private diagnostic channel and must not enter ordinary CLI receipts.
#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct StorageAllocationOperation {
    state: Arc<Mutex<StorageAllocationLifecycle>>,
}

impl StorageAllocationOperation {
    /// Resolve the project and its exact sibling admission control for a private baseline.
    ///
    /// # Errors
    /// Rejects paths that cannot use the normal filesystem admission resolution.
    pub fn project_paths(path: &Path) -> Result<[std::path::PathBuf; 2], GfError> {
        let (root, parent, lock) =
            crate::filesystem_admission::retained_project_control_paths(path)?;
        Ok([root, parent.join(lock)])
    }

    /// Capture a quiescent baseline from explicitly declared artifact paths.
    /// This never runs during an active writer and never reads payload bytes.
    ///
    /// # Errors
    /// Rejects links, special files, unresolved paths, and oversized inventories.
    pub fn from_paths(paths: &[std::path::PathBuf]) -> Result<Self, GfError> {
        let operation = Self::default();
        let mut directories = Vec::new();
        let mut remaining = 1_000_000_usize;
        for path in paths {
            Self::file_owner(path)?;
            let metadata = match std::fs::symlink_metadata(path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(GfError::Storage(error.to_string())),
            };
            if metadata.is_dir() && !metadata.file_type().is_symlink() {
                directories.push(
                    graphforge_filesystem::StableDirectory::open(path)
                        .map_err(|error| GfError::Storage(error.to_string()))?,
                );
            } else if metadata.is_file() && !metadata.file_type().is_symlink() {
                let parent = path
                    .parent()
                    .ok_or_else(|| GfError::Storage("allocation path has no parent".into()))?;
                let directory = graphforge_filesystem::StableDirectory::open(parent)
                    .map_err(|error| GfError::Storage(error.to_string()))?;
                let file =
                    directory
                        .open_child_file(path.file_name().ok_or_else(|| {
                            GfError::Storage("allocation path has no name".into())
                        })?)
                        .map_err(|error| GfError::Storage(error.to_string()))?;
                operation.replace_file_at(path, &file)?;
            } else {
                return Err(GfError::Storage(
                    "allocation baseline contains a link or special file".into(),
                ));
            }
        }
        while let Some(directory) = directories.pop() {
            let names = directory
                .child_names_bounded(remaining)
                .map_err(|error| GfError::Storage(error.to_string()))?;
            remaining = remaining.checked_sub(names.len()).ok_or_else(|| {
                GfError::Storage("allocation baseline exceeds entry bound".into())
            })?;
            for name in names {
                if let Ok(child) = directory.open_child_directory(&name) {
                    directories.push(child);
                } else {
                    let file = directory
                        .open_child_file(&name)
                        .map_err(|error| GfError::Storage(error.to_string()))?;
                    operation.replace_file_at(&directory.path().join(&name), &file)?;
                }
            }
        }
        Ok(operation)
    }

    /// Read a bounded private continuation before any project mutation.
    ///
    /// # Errors
    /// Rejects missing, oversized, malformed, or inconsistent ownership input.
    pub fn read_private(input: impl std::io::Read) -> Result<Self, GfError> {
        Self::read_private_bounded(input, 256 * 1024 * 1024)
    }

    fn read_private_bounded(
        mut input: impl std::io::Read,
        max_bytes: u64,
    ) -> Result<Self, GfError> {
        use std::io::Read as _;
        // One million bounded owner references plus active identities fit this
        // channel limit; this is not a graph payload or public receipt channel.
        let mut bytes = Vec::new();
        input
            .by_ref()
            .take(max_bytes + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| GfError::Storage(error.to_string()))?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > max_bytes {
            return Err(GfError::Storage(
                "private allocation input exceeds bound".into(),
            ));
        }
        let state = serde_json::from_slice(&bytes)
            .map_err(|_| GfError::Storage("private allocation input is malformed".into()))?;
        Self::from_lifecycle(state)
    }

    /// Retain the first-party certifier's existing union across one subprocess.
    pub fn from_lifecycle(state: StorageAllocationLifecycle) -> Result<Self, GfError> {
        state.validate_continuation()?;
        Ok(Self {
            state: Arc::new(Mutex::new(state)),
        })
    }

    /// Return bounded state for the private first-party continuation channel.
    ///
    /// # Errors
    /// Refuses a poisoned accounting context.
    pub fn snapshot(&self) -> Result<StorageAllocationLifecycle, GfError> {
        Ok(self.state.lock().map_err(poisoned)?.clone())
    }

    /// Start from the actual owners retained immediately before the operation.
    ///
    /// # Errors
    /// Rejects inconsistent native identity facts and allocation overflow.
    pub fn from_owners(owners: &BTreeMap<String, BTreeMap<String, u64>>) -> Result<Self, GfError> {
        let mut state = StorageAllocationLifecycle::default();
        for (owner, identities) in owners {
            state.replace_owner(owner, identities)?;
        }
        Ok(Self {
            state: Arc::new(Mutex::new(state)),
        })
    }

    /// Record an actual writer transition, retaining aliases held by other owners.
    ///
    /// # Errors
    /// Rejects inconsistent native identity facts and allocation overflow.
    pub fn transition(
        &self,
        owner: &str,
        transition: &StorageAllocationTransition,
    ) -> Result<(), GfError> {
        self.state
            .lock()
            .map_err(poisoned)?
            .apply_owner_transition(owner, transition)
    }

    /// Resolve the project using the ordinary admission path authority.
    ///
    /// # Errors
    /// Rejects unsafe paths using ordinary admission rules.
    pub fn resolve_project_path(path: &Path) -> Result<std::path::PathBuf, GfError> {
        crate::filesystem_admission::retained_project_control_paths(path).map(|(root, _, _)| root)
    }

    /// Stable private owner key for one resolved file route.
    ///
    /// # Errors
    /// Rejects unresolved relative routes.
    pub fn file_owner(path: &Path) -> Result<String, GfError> {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        if !path.is_absolute() {
            return Err(GfError::Storage(
                "allocation file owner requires a resolved absolute path".into(),
            ));
        }
        let digest = Sha256::digest(path.as_os_str().as_encoded_bytes());
        let mut key = String::with_capacity(69);
        key.push_str("file:");
        for byte in digest {
            key.push(char::from(HEX[usize::from(byte >> 4)]));
            key.push(char::from(HEX[usize::from(byte & 15)]));
        }
        Ok(key)
    }

    /// Snapshot one resolved writer route without following its pathname.
    ///
    /// # Errors
    /// Requires an absolute route and consistent actual file metadata.
    pub fn replace_file_at(&self, path: &Path, file: &File) -> Result<(), GfError> {
        self.replace_file(&Self::file_owner(path)?, file)
    }

    /// Remove a successfully unlinked resolved writer route.
    ///
    /// # Errors
    /// Requires an absolute route and a healthy accounting context.
    pub fn remove_file_at(&self, path: &Path) -> Result<(), GfError> {
        self.remove_owner(&Self::file_owner(path)?)
    }

    /// Record the allocated bytes of an open file before its installation.
    ///
    /// # Errors
    /// Returns metadata, identity reconciliation, and accounting errors.
    pub fn replace_file(&self, owner: &str, file: &File) -> Result<(), GfError> {
        let usage = graphforge_filesystem::file_space_usage(file)
            .map_err(|error| GfError::Storage(error.to_string()))?;
        let identity = graphforge_filesystem::file_identity(file)
            .map_err(|error| GfError::Storage(error.to_string()))?;
        let identity = crate::storage_attribution::native_identity_key(
            identity.volume_serial,
            &identity.file_id,
        );
        self.state
            .lock()
            .map_err(poisoned)?
            .replace_owner(owner, &BTreeMap::from([(identity, usage.allocated_bytes)]))
    }

    /// Remove a writer's reference after successful unlink/replacement.
    ///
    /// # Errors
    /// Returns accounting errors without discarding other owners' aliases.
    pub fn remove_owner(&self, owner: &str) -> Result<(), GfError> {
        self.state.lock().map_err(poisoned)?.remove_owner(owner)
    }

    /// Return the exact current union and ordered high-water mark.
    ///
    /// # Errors
    /// Refuses a poisoned accounting context.
    pub fn totals(&self) -> Result<(u64, u64), GfError> {
        let state = self.state.lock().map_err(poisoned)?;
        Ok((
            state.current_allocated_bytes(),
            state.peak_allocated_bytes(),
        ))
    }
}

fn poisoned<T>(_: std::sync::PoisonError<T>) -> GfError {
    GfError::Storage("operation allocation accounting lock poisoned".into())
}

#[cfg(test)]
mod tests {
    #[test]
    fn private_continuation_rejects_missing_malformed_and_oversized_input() {
        use super::StorageAllocationOperation;
        assert!(StorageAllocationOperation::read_private(&b""[..]).is_err());
        assert!(StorageAllocationOperation::read_private(&b"not-json"[..]).is_err());
        assert!(
            StorageAllocationOperation::read_private_bounded(std::io::repeat(b' '), 32)
                .unwrap_err()
                .to_string()
                .contains("exceeds bound")
        );
        let original = StorageAllocationOperation::default();
        let encoded = serde_json::to_vec(&original.snapshot().unwrap()).unwrap();
        let admitted = StorageAllocationOperation::read_private(encoded.as_slice()).unwrap();
        assert_eq!(admitted.totals().unwrap(), (0, 0));
        let mut invalid = serde_json::to_value(original.snapshot().unwrap()).unwrap();
        invalid["current_allocated_bytes"] = serde_json::json!(1);
        assert!(
            StorageAllocationOperation::read_private(
                serde_json::to_vec(&invalid).unwrap().as_slice()
            )
            .is_err()
        );
    }

    use super::*;

    #[test]
    fn atomic_control_replacement_removes_the_actual_baseline_owner() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("CURRENT");
        let temporary = root.path().join("CURRENT.tmp");
        std::fs::write(&target, vec![0_u8; 8192]).unwrap();
        let before = StorageAllocationOperation::default();
        let old = File::open(&target).unwrap();
        let old_bytes = graphforge_filesystem::file_space_usage(&old)
            .unwrap()
            .allocated_bytes;
        before
            .replace_file("project-control-CURRENT", &old)
            .unwrap();
        drop(old);
        let operation =
            StorageAllocationOperation::from_lifecycle(before.snapshot().unwrap()).unwrap();
        std::fs::write(&temporary, vec![1_u8; 16384]).unwrap();
        let new = File::open(&temporary).unwrap();
        let new_bytes = graphforge_filesystem::file_space_usage(&new)
            .unwrap()
            .allocated_bytes;
        operation
            .replace_file("project-control-CURRENT.tmp", &new)
            .unwrap();
        assert_eq!(
            operation.totals().unwrap(),
            (old_bytes + new_bytes, old_bytes + new_bytes)
        );
        std::fs::rename(&temporary, &target).unwrap();
        operation.remove_owner("project-control-CURRENT").unwrap();
        operation
            .replace_file("project-control-CURRENT", &new)
            .unwrap();
        operation
            .remove_owner("project-control-CURRENT.tmp")
            .unwrap();
        assert_eq!(
            operation.totals().unwrap(),
            (new_bytes, old_bytes + new_bytes)
        );
        let actual = File::open(&target).unwrap();
        assert_eq!(
            graphforge_filesystem::file_space_usage(&actual)
                .unwrap()
                .allocated_bytes,
            operation.totals().unwrap().0
        );
    }

    #[test]
    fn operation_peak_preserves_aliases_and_transition_order() {
        let baseline = BTreeMap::from([("baseline".into(), BTreeMap::from([("a".into(), 4096)]))]);
        let early = StorageAllocationOperation::from_owners(&baseline).unwrap();
        let late = StorageAllocationOperation::from_owners(&baseline).unwrap();
        let alias_temp = StorageAllocationTransition {
            installed: BTreeMap::from([("a".into(), 4096), ("t".into(), 32768)]),
            removed: Default::default(),
        };
        let final_file = StorageAllocationTransition {
            installed: BTreeMap::from([("f".into(), 16384)]),
            removed: Default::default(),
        };
        let remove_temp = StorageAllocationTransition {
            installed: BTreeMap::new(),
            removed: ["t".into()].into_iter().collect(),
        };
        for operation in [&early, &late] {
            operation.transition("construction", &alias_temp).unwrap();
        }
        early.transition("construction", &remove_temp).unwrap();
        early.transition("construction", &final_file).unwrap();
        late.transition("construction", &final_file).unwrap();
        late.transition("construction", &remove_temp).unwrap();
        assert_eq!(early.totals().unwrap(), (20480, 36864));
        assert_eq!(late.totals().unwrap(), (20480, 53248));
        let resumed =
            StorageAllocationOperation::from_lifecycle(early.snapshot().unwrap()).unwrap();
        assert_eq!(resumed.totals().unwrap(), (20480, 36864));
        resumed.remove_owner("construction").unwrap();
        assert_eq!(resumed.totals().unwrap(), (4096, 36864));
        assert_eq!(early.totals().unwrap(), (20480, 36864));
    }
}
