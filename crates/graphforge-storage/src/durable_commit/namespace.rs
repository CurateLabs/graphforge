//! Retained no-replace promotion and batched retirement.
use super::{Visibility, acknowledge_directory};
use graphforge_filesystem::{FileIdentity, StableDirectory};
use std::ffi::OsStr;
use std::io;
use std::path::Path;

#[derive(Debug)]
pub struct NamespaceFailure {
    pub visibility: Visibility,
    pub cause: io::Error,
}
impl std::fmt::Display for NamespaceFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::error::Error for NamespaceFailure {}
fn failure(visibility: Visibility, cause: io::Error) -> NamespaceFailure {
    NamespaceFailure { visibility, cause }
}

/// Promote a caller-verified regular file or directory without replacement.
/// Native visibility uncertainty is preserved for the format owner's cleanup.
pub fn promote_no_replace(
    source: &Path,
    destination: &Path,
    before_visible: impl FnOnce() -> io::Result<()>,
    after_visible: impl FnOnce() -> io::Result<()>,
) -> Result<(), NamespaceFailure> {
    let expected = graphforge_filesystem::path_identity(source)
        .map_err(|e| failure(Visibility::NotPublished, e))?;
    promote_no_replace_authenticated(source, destination, expected, before_visible, after_visible)
}

/// Promote the exact producer identity admitted by the format owner's source
/// verification, without recapturing a replacement as fresh authority.
pub fn promote_no_replace_authenticated(
    source: &Path,
    destination: &Path,
    expected: FileIdentity,
    before_visible: impl FnOnce() -> io::Result<()>,
    after_visible: impl FnOnce() -> io::Result<()>,
) -> Result<(), NamespaceFailure> {
    let prepare = || -> io::Result<_> {
        let source_parent = StableDirectory::open(
            source
                .parent()
                .ok_or_else(|| io::Error::other("promotion source has no parent"))?,
        )?;
        let destination_parent = StableDirectory::open(
            destination
                .parent()
                .ok_or_else(|| io::Error::other("promotion destination has no parent"))?,
        )?;
        let metadata = std::fs::symlink_metadata(source)?;
        if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
            return Err(io::Error::other(
                "promotion source is not a regular artifact",
            ));
        }
        let identity = graphforge_filesystem::path_identity(source)?;
        if identity != expected {
            return Err(io::Error::other("promotion producer identity changed"));
        }
        Ok((source_parent, destination_parent, identity))
    };
    let (source_parent, destination_parent, identity) =
        prepare().map_err(|e| failure(Visibility::NotPublished, e))?;
    before_visible().map_err(|e| failure(Visibility::NotPublished, e))?;
    let revalidate = || -> io::Result<()> {
        source_parent.revalidate_named()?;
        destination_parent.revalidate_named()?;
        if graphforge_filesystem::path_identity(source)? != expected {
            return Err(io::Error::other(
                "promotion producer changed before visibility",
            ));
        }
        Ok(())
    };
    revalidate().map_err(|e| failure(Visibility::NotPublished, e))?;
    graphforge_filesystem::rename_no_replace(source, destination)
        .map_err(|e| failure(Visibility::StateUnknown, e))?;
    let acknowledge = || -> io::Result<()> {
        source_parent.revalidate_named()?;
        destination_parent.revalidate_named()?;
        if graphforge_filesystem::path_identity(destination)? != identity {
            return Err(io::Error::other("promoted artifact identity changed"));
        }
        after_visible()?;
        acknowledge_directory(&destination_parent)?;
        if source_parent.identity() != destination_parent.identity() {
            acknowledge_directory(&source_parent)?;
        }
        Ok(())
    };
    acknowledge().map_err(|e| failure(Visibility::VisibleUnacknowledged, e))
}

/// Retire a bounded caller-authenticated batch and acknowledge once per parent.
pub fn retire_files<'a>(
    parent: &StableDirectory,
    files: impl IntoIterator<Item = (&'a OsStr, FileIdentity)>,
) -> io::Result<()> {
    for (name, identity) in files {
        parent.unlink_child_if_identity(name, identity)?;
    }
    acknowledge_directory(parent)
}

/// Retire an already-emptied retained child directory and acknowledge its parent.
pub fn retire_directory(
    parent: &StableDirectory,
    name: &OsStr,
    identity: FileIdentity,
) -> io::Result<()> {
    parent.remove_child_directory_if_identity(name, identity)?;
    acknowledge_directory(parent)
}

/// One already-authenticated child in a bounded mixed retirement batch.
pub enum RetireEntry<'a> {
    File {
        name: &'a OsStr,
        identity: FileIdentity,
    },
    Directory {
        name: &'a OsStr,
        identity: FileIdentity,
    },
}
/// Retire exact authenticated children, acknowledging their parent once.
pub fn retire_entries<'a>(
    parent: &StableDirectory,
    entries: impl IntoIterator<Item = RetireEntry<'a>>,
) -> io::Result<()> {
    for entry in entries {
        match entry {
            RetireEntry::File { name, identity } => {
                parent.unlink_child_if_identity(name, identity)?;
            }
            RetireEntry::Directory { name, identity } => {
                parent.remove_child_directory_if_identity(name, identity)?;
            }
        }
    }
    acknowledge_directory(parent)
}

/// Retire a caller-authorized idle tree and acknowledge its retained parent.
/// Naming, reachability, transaction locks and deletion budgets remain with
/// the format owner; this primitive revalidates the exact namespace identity.
pub fn retire_owned_tree(
    parent: &StableDirectory,
    name: &OsStr,
    expected: FileIdentity,
) -> io::Result<()> {
    let directory = parent.open_child_directory(name)?;
    if directory.identity() != expected {
        return Err(io::Error::other("retirement directory identity changed"));
    }
    parent.revalidate_named()?;
    directory.revalidate_named()?;
    let path = directory.path().to_path_buf();
    // Release our directory handle before recursive retirement on Windows.
    drop(directory);
    std::fs::remove_dir_all(path)?;
    parent.revalidate_named()?;
    acknowledge_directory(parent)
}

/// Establish a retained directory namespace before descendants are published.
pub fn create_directory(parent: &StableDirectory, name: &OsStr) -> io::Result<StableDirectory> {
    let child = parent.create_child_directory(name)?;
    acknowledge_directory(parent)?;
    Ok(child)
}

/// An owned same-parent retirement batch. Format hooks and allocation updates
/// can run after each exact unlink while the final namespace fence stays here.
#[derive(Debug)]
pub struct RetirementBatch {
    parent: StableDirectory,
}
impl RetirementBatch {
    pub fn new(parent: &StableDirectory) -> io::Result<Self> {
        Ok(Self {
            parent: parent.try_clone()?,
        })
    }
    pub fn unlink(&mut self, name: &OsStr, expected: FileIdentity) -> io::Result<()> {
        self.parent.unlink_child_if_identity(name, expected)
    }
    pub fn remove_empty_directory(
        &mut self,
        name: &OsStr,
        expected: FileIdentity,
    ) -> io::Result<()> {
        self.parent
            .remove_child_directory_if_identity(name, expected)
    }
    /// Share one fence with visible outputs in this same retained namespace.
    /// Failures retain both retirement and output continuations for retry.
    pub fn acknowledge_with(
        self,
        pending: Vec<super::PendingCommit>,
        allocation: Option<&crate::StorageAllocationOperation>,
    ) -> Result<(), super::GroupCommitFailure> {
        if pending
            .iter()
            .any(|commit| commit.parent_identity() != self.parent.identity())
        {
            return Err(super::GroupCommitFailure {
                visibility: Visibility::VisibleUnacknowledged,
                cause: io::Error::other("retirement/output batch has different parent authorities"),
                pending,
                retirement: Some(self),
            });
        }
        if pending.is_empty() {
            return acknowledge_directory(&self.parent).map_err(|cause| {
                super::GroupCommitFailure {
                    visibility: Visibility::VisibleUnacknowledged,
                    cause,
                    pending,
                    retirement: Some(self),
                }
            });
        }
        super::PendingCommit::acknowledge_group(pending, allocation).map_err(|mut error| {
            error.retirement = Some(self);
            error
        })
    }
    pub fn acknowledge(self) -> io::Result<()> {
        acknowledge_directory(&self.parent)
    }
}
