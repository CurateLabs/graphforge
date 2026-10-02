//! Storage-owned physical durability and namespace commits.
//!
//! Callers retain content authentication and logical authority. This module
//! owns file sealing, namespace visibility, acknowledgment and owned cleanup.
use graphforge_filesystem::{DurableFileCacheWriter, ObservedSync, StableDirectory};
use std::fs::File;
use std::io;
use std::path::Path;

mod atomic;
mod barrier_observation;
pub use barrier_observation::observe_barriers;
#[cfg(test)]
pub(crate) mod fault;
mod namespace;
#[cfg(all(test, windows))]
mod windows_allocation_tests;
pub use atomic::{
    AtomicHooks, CommitCause, CommitFailure, GroupCommitFailure, PendingCommit, PublishMode,
    SealedArtifact, Visibility, install_immutable, publish_atomic, publish_atomic_in,
    stage_private_file, stage_writer,
};
pub use namespace::{
    NamespaceFailure, RetireEntry, RetirementBatch, create_directory, promote_no_replace,
    promote_no_replace_authenticated, retire_directory, retire_entries, retire_files,
    retire_owned_tree,
};

/// Admit the exact private source descriptor for sealing and native publication.
/// This grants namespace/durability access only, never content authentication.
pub fn open_publisher(
    parent: &StableDirectory,
    name: &std::ffi::OsStr,
    expected: graphforge_filesystem::FileIdentity,
) -> io::Result<File> {
    parent.open_publishing_child_file(name, expected)
}

/// Complete the durability barrier for an admitted writer's actual descriptor.
pub fn seal_file(file: &(impl ObservedSync + ?Sized)) -> io::Result<()> {
    let _wait = crate::concurrency_attribution::RegionScope::named("fsync");
    #[cfg(test)]
    fault::hit(fault::Point::FileFence)?;
    barrier_observation::record_attempt();
    file.observed_sync_all()
}

/// Finish the final dirty-cache window before the caller releases its writer.
pub fn seal_cache_writer(writer: &mut DurableFileCacheWriter) -> io::Result<()> {
    #[cfg(test)]
    fault::hit(fault::Point::FileFence)?;
    writer.sync_all_and_release()
}

/// Acknowledge a namespace batch through its retained directory authority.
pub fn acknowledge_directory(directory: &StableDirectory) -> io::Result<()> {
    let _wait = crate::concurrency_attribution::RegionScope::named("fsync");
    #[cfg(test)]
    fault::hit(fault::Point::ParentFence)?;
    barrier_observation::record_attempt();
    directory.sync()
}

/// Retain and acknowledge an existing directory namespace.
pub fn sync_directory(path: &Path) -> io::Result<()> {
    acknowledge_directory(&StableDirectory::open(path)?)
}

/// Acknowledge the namespace containing one already-admitted artifact.
pub fn sync_parent(path: &Path) -> io::Result<()> {
    sync_directory(
        path.parent()
            .ok_or_else(|| io::Error::other("artifact has no parent"))?,
    )
}

/// Acknowledge an append on the same retained descriptor and exact end offset.
/// Framing, replay and poisoning remain with the journal's format owner.
pub fn acknowledge_append(file: &File, expected_end: u64) -> io::Result<()> {
    if file.metadata()?.len() != expected_end {
        return Err(io::Error::other("append length differs from accepted end"));
    }
    seal_file(file)
}

/// Acknowledge an unpublished guard without surrendering its cleanup authority.
pub fn acknowledge_guard(
    guard: &mut graphforge_filesystem::UnpublishedArtifactGuard,
) -> io::Result<()> {
    guard.sync_parent_with(acknowledge_directory)
}

/// Install and acknowledge one already-sealed guarded artifact.
pub fn install_guarded(
    guard: &mut graphforge_filesystem::UnpublishedArtifactGuard,
    target: &std::ffi::OsStr,
) -> io::Result<()> {
    #[cfg(test)]
    fault::hit(fault::Point::BeforeVisible)?;
    guard.install_child(target)?;
    #[cfg(test)]
    fault::hit(fault::Point::NativeUnknown)?;
    acknowledge_guard(guard)
}

/// An opaque proof of the barrier on this exact producer descriptor.
#[derive(Debug)]
pub struct FileSeal {
    identity: graphforge_filesystem::FileIdentity,
    length: u64,
    modified: Option<std::time::SystemTime>,
}
impl FileSeal {
    #[must_use]
    pub const fn identity(&self) -> graphforge_filesystem::FileIdentity {
        self.identity
    }
    fn capture(file: &File) -> io::Result<Self> {
        let metadata = file.metadata()?;
        Ok(Self {
            identity: graphforge_filesystem::file_identity(file)?,
            length: metadata.len(),
            modified: metadata.modified().ok(),
        })
    }
    fn consume(self, file: &File) -> io::Result<graphforge_filesystem::FileIdentity> {
        self.validate(file)?;
        let Self {
            identity,
            length: _,
            modified: _,
        } = self;
        Ok(identity)
    }
    fn validate(&self, file: &File) -> io::Result<()> {
        let metadata = file.metadata()?;
        if graphforge_filesystem::file_identity(file)? != self.identity
            || metadata.len() != self.length
            || metadata.modified().ok() != self.modified
        {
            return Err(io::Error::other("sealed producer descriptor changed"));
        }
        Ok(())
    }
}

/// Seal once and return opaque exact-descriptor durability evidence.
pub fn seal_file_witness(file: &File) -> io::Result<FileSeal> {
    seal_file(file)?;
    FileSeal::capture(file)
}

/// Finish the final cache fence once and capture its exact producer descriptor.
pub fn seal_cache_writer_witness(writer: &mut DurableFileCacheWriter) -> io::Result<FileSeal> {
    seal_cache_writer(writer)?;
    FileSeal::capture(writer.file())
}

/// Seal and transfer an unpublished guard to the physical commit owner.
pub fn seal_guarded(
    guard: graphforge_filesystem::UnpublishedArtifactGuard,
    file: File,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> io::Result<SealedArtifact> {
    let (parent, name, identity) = guard.transfer_unpublished_owner(&file)?;
    SealedArtifact::seal_existing(&parent, &name, file, identity, allocation)
}

/// A leased append descriptor bound to one retained no-follow namespace.
#[derive(Debug)]
pub struct AppendLease {
    parent: StableDirectory,
    name: std::ffi::OsString,
    descriptor: File,
    identity: graphforge_filesystem::FileIdentity,
}
impl AppendLease {
    pub fn admit(
        parent: &StableDirectory,
        name: &std::ffi::OsStr,
        file: &File,
    ) -> io::Result<Self> {
        parent.revalidate_named()?;
        let retained = parent.try_clone()?;
        let named = retained.open_child_file(name)?;
        let identity = graphforge_filesystem::file_identity(file)?;
        if retained.identity() != parent.identity()
            || graphforge_filesystem::file_identity(&named)? != identity
            || graphforge_filesystem::file_link_count(file)? != 1
        {
            return Err(io::Error::other("append lease namespace identity changed"));
        }
        Ok(Self {
            parent: retained,
            name: name.to_owned(),
            descriptor: file.try_clone()?,
            identity,
        })
    }
    fn validate(&self, file: &File, expected_end: u64) -> io::Result<()> {
        self.parent.revalidate_named()?;
        let named = self.parent.open_child_file(&self.name)?;
        if graphforge_filesystem::file_identity(&self.descriptor)? != self.identity
            || graphforge_filesystem::file_identity(file)? != self.identity
            || graphforge_filesystem::file_identity(&named)? != self.identity
            || file.metadata()?.len() != expected_end
            || graphforge_filesystem::file_link_count(file)? != 1
        {
            return Err(io::Error::other(
                "append lease descriptor, namespace or accepted length changed",
            ));
        }
        Ok(())
    }
    pub fn acknowledge(&self, file: &File, expected_end: u64) -> io::Result<()> {
        self.validate(file, expected_end)?;
        seal_file(file)?;
        self.validate(file, expected_end)
    }
}

/// Consume exact-descriptor seal evidence while preserving a semantic guard.
pub fn install_guarded_sealed(
    guard: &mut graphforge_filesystem::UnpublishedArtifactGuard,
    target: &std::ffi::OsStr,
    witness: FileSeal,
) -> io::Result<()> {
    let named = guard.open_sibling(guard.temporary_name()?)?;
    let identity = witness.consume(&named)?;
    if guard.identity()? != identity {
        return Err(io::Error::other("guarded seal witness identity changed"));
    }
    // A normal Windows reader excludes DELETE sharing. Validation is complete;
    // release this auxiliary handle before opening the native rename handle.
    drop(named);
    install_guarded(guard, target)
}

/// Seal and acknowledge an already-created, privately owned named descriptor.
/// The caller retains any logical publication lease on that descriptor.
pub fn acknowledge_created(
    parent: &StableDirectory,
    name: &std::ffi::OsStr,
    file: &File,
    after_seal: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    parent.revalidate_named()?;
    let named = parent.open_child_file(name)?;
    let identity = graphforge_filesystem::file_identity(file)?;
    if graphforge_filesystem::file_identity(&named)? != identity
        || graphforge_filesystem::file_link_count(file)? != 1
    {
        return Err(io::Error::other("created child namespace identity changed"));
    }
    seal_file(file)?;
    after_seal()?;
    let named = parent.open_child_file(name)?;
    if graphforge_filesystem::file_identity(&named)? != identity {
        return Err(io::Error::other("created child changed after sealing"));
    }
    acknowledge_directory(parent)
}
