//! Retained atomic publication and acknowledgment, including uncertain outcomes.
use super::{acknowledge_directory, seal_file};
use crate::StorageAllocationOperation;
use graphforge_core::hash_observation::ContractSha256;
use graphforge_filesystem::{FileIdentity, ReplaceFileError, StableDirectory};
use sha2::Digest;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
#[cfg(not(test))]
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Visibility {
    NotPublished,
    VisibleUnacknowledged,
    StateUnknown,
}

#[derive(Debug)]
pub enum CommitCause {
    Io(io::Error),
    Replacement(ReplaceFileError),
}
impl std::fmt::Display for CommitCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => e.fmt(f),
            Self::Replacement(e) => e.fmt(f),
        }
    }
}
#[derive(Debug)]
pub struct CommitFailure {
    pub visibility: Visibility,
    pub cause: CommitCause,
    pub pending: Option<Box<PendingCommit>>,
}
impl std::fmt::Display for CommitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::error::Error for CommitFailure {}

/// An owned temporary. Drop never touches a published target name.
#[derive(Debug)]
pub struct SealedArtifact {
    parent: Arc<StableDirectory>,
    temporary: OsString,
    file: Option<File>,
    identity: FileIdentity,
    locked: bool,
    allocation: Option<StorageAllocationOperation>,
    cleanup: bool,
}
impl Drop for SealedArtifact {
    fn drop(&mut self) {
        if self.locked
            && let Some(file) = &self.file
        {
            let _ = crate::file_lock::unlock(file);
        }
        // Windows sealed handles can exclude deletion; release only our descriptor first.
        drop(self.file.take());
        if self.cleanup
            && self
                .parent
                .unlink_child_if_identity(&self.temporary, self.identity)
                .is_ok()
            && let Some(allocation) = &self.allocation
        {
            let _ = allocation.remove_file_at(&self.parent.path().join(&self.temporary));
        }
    }
}

#[derive(Debug)]
pub struct PendingCommit {
    staged: SealedArtifact,
    target_parent: Arc<StableDirectory>,
    target: OsString,
    allocation_recorded: bool,
    lease: NamespaceLease,
}
impl PendingCommit {
    #[must_use]
    pub fn matches_target(&self, target: &OsStr, identity: FileIdentity) -> bool {
        self.target == target && self.staged.identity == identity
    }

    pub(super) fn parent_identity(&self) -> FileIdentity {
        self.staged.parent.identity()
    }

    fn unlock_producer(&mut self) -> io::Result<()> {
        if !self.staged.locked {
            return Ok(());
        }
        let result = crate::file_lock::unlock(self.staged.file());
        self.staged.locked = false;
        if result.is_err() {
            // Closing the exclusive Windows handle releases its kernel lock;
            // reconciliation must be able to reopen the actual visible inode.
            drop(self.staged.file.take());
        }
        result
    }

    /// Close an unlocked producer after native visibility. Retained namespace
    /// identity remains authoritative for the eventual bounded batch fence.
    pub fn release_unlocked_producer(&mut self) -> io::Result<()> {
        if self.staged.locked {
            return Err(io::Error::other(
                "locked atomic producer must remain retained",
            ));
        }
        let named = self.target_parent.open_child_file(&self.target)?;
        if graphforge_filesystem::file_identity(&named)? != self.staged.identity {
            return Err(io::Error::other("visible producer identity changed"));
        }
        drop(self.staged.file.take());
        Ok(())
    }

    /// Verify actual target identity and acknowledge this same retained parent.
    pub fn acknowledge(
        mut self,
        allocation: Option<&StorageAllocationOperation>,
    ) -> Result<(), CommitFailure> {
        let _lease = &self.lease;
        let result = (|| -> io::Result<()> {
            let allocation = allocation.or(self.staged.allocation.as_ref());
            let installed = self.target_parent.open_child_file(&self.target)?;
            if graphforge_filesystem::file_identity(&installed)? != self.staged.identity {
                return Err(io::Error::other(
                    "published child identity changed before acknowledgment",
                ));
            }
            if !self.allocation_recorded {
                if let Some(allocation) = allocation {
                    let target = self.target_parent.path().join(&self.target);
                    allocation
                        .remove_file_at(&target)
                        .map_err(io::Error::other)?;
                    allocation
                        .replace_file_at(&target, &installed)
                        .map_err(io::Error::other)?;
                    allocation
                        .remove_file_at(&self.staged.parent.path().join(&self.staged.temporary))
                        .map_err(io::Error::other)?;
                }
                self.allocation_recorded = true;
            }
            if self.staged.locked {
                crate::file_lock::unlock(self.staged.file())?;
                self.staged.locked = false;
            }
            if let Some(allocation) = allocation {
                allocation
                    .replace_file_at(&self.target_parent.path().join(&self.target), &installed)
                    .map_err(io::Error::other)?;
            }
            acknowledge_directory(&self.target_parent)?;
            if self.target_parent.identity() != self.staged.parent.identity() {
                acknowledge_directory(&self.staged.parent)?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => Ok(()),
            Err(cause) => Err(CommitFailure {
                visibility: Visibility::VisibleUnacknowledged,
                cause: CommitCause::Io(cause),
                pending: Some(Box::new(self)),
            }),
        }
    }
}

pub struct AtomicHooks<W, S, B> {
    pub after_write: W,
    pub after_seal: S,
    pub before_visible: B,
}

/// Preserve the recovery-admitted `.graphforge-atomic-{64hex}.tmp` spelling.
fn temporary_name(target: &OsStr) -> io::Result<OsString> {
    let target = target
        .to_str()
        .ok_or_else(|| io::Error::other("atomic publication target is not UTF-8"))?;
    let mut hash = ContractSha256::new();
    hash.update(target.as_bytes());
    hash.update(Uuid::now_v7().as_bytes());
    let mut encoded = String::with_capacity(64);
    for byte in hash.finalize() {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing hex to String");
    }
    Ok(format!(".graphforge-atomic-{encoded}.tmp").into())
}
#[derive(Debug, Default)]
struct TargetLock {
    held: Mutex<bool>,
    available: Condvar,
}
#[derive(Debug)]
struct NamespaceLease {
    path: PathBuf,
    target: Arc<TargetLock>,
}
static NAMESPACE_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<TargetLock>>>> = OnceLock::new();
impl Drop for NamespaceLease {
    fn drop(&mut self) {
        *self
            .target
            .held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = false;
        self.target.available.notify_one();
        let mut locks = NAMESPACE_LOCKS
            .get()
            .expect("namespace registry exists")
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if Arc::strong_count(&self.target) == 1 {
            locks.remove(&self.path);
        }
    }
}
fn namespace_lock(path: &Path) -> NamespaceLease {
    let path = path.to_path_buf();
    let target = {
        let mut locks = NAMESPACE_LOCKS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let target = locks.get(&path).and_then(Weak::upgrade).unwrap_or_default();
        locks.insert(path.clone(), Arc::downgrade(&target));
        target
    };
    let mut held = target
        .held
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while *held {
        held = target
            .available
            .wait(held)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
    *held = true;
    drop(held);
    NamespaceLease { path, target }
}
fn unpublished(cause: io::Error) -> CommitFailure {
    CommitFailure {
        visibility: Visibility::NotPublished,
        cause: CommitCause::Io(cause),
        pending: None,
    }
}

/// Create, write, seal and publish one control record, leaving its exact parent
/// acknowledgment in a retained continuation for logical reconciliation.
pub fn publish_atomic_in<W, S, B>(
    parent: StableDirectory,
    target: &OsStr,
    bytes: &[u8],
    hooks: AtomicHooks<W, S, B>,
    allocation: Option<&StorageAllocationOperation>,
) -> Result<PendingCommit, CommitFailure>
where
    W: FnOnce() -> io::Result<()>,
    S: FnOnce() -> io::Result<()>,
    B: FnOnce() -> io::Result<()>,
{
    let temporary = temporary_name(target).map_err(unpublished)?;
    let mut guard = parent
        .create_unpublished_replaceable_child(&temporary)
        .map_err(unpublished)?;
    let file = guard.take_file().map_err(unpublished)?;
    let (parent, temporary, identity) = guard
        .transfer_unpublished_owner_in(parent, &file)
        .map_err(unpublished)?;
    let mut staged = SealedArtifact {
        parent: Arc::new(parent),
        temporary,
        file: Some(file),
        identity,
        locked: false,
        allocation: allocation.cloned(),
        cleanup: true,
    };
    crate::file_lock::lock_exclusive(staged.file()).map_err(unpublished)?;
    staged.locked = true;
    let temporary_path = staged.parent.path().join(&staged.temporary);
    #[cfg(test)]
    let written = super::fault::write(staged.file_mut(), bytes);
    #[cfg(not(test))]
    let written = staged.file_mut().write_all(bytes);
    // Always observe actual allocation even when the producer stopped partway.
    let observed = allocation.map_or(Ok(()), |a| {
        a.replace_file_at(&temporary_path, staged.file())
    });
    written.map_err(unpublished)?;
    observed.map_err(|e| unpublished(io::Error::other(e)))?;
    (hooks.after_write)().map_err(unpublished)?;
    seal_file(staged.file()).map_err(unpublished)?;
    if let Some(allocation) = allocation {
        allocation
            .replace_file_at(&temporary_path, staged.file())
            .map_err(|e| unpublished(io::Error::other(e)))?;
    }
    (hooks.after_seal)().map_err(unpublished)?;
    let path = staged.parent.path().join(target);
    let lease = namespace_lock(&path);
    // The final logical predicate executes under namespace serialization.
    #[cfg(test)]
    super::fault::hit(super::fault::Point::BeforeVisible).map_err(unpublished)?;
    (hooks.before_visible)().map_err(unpublished)?;
    let replaced = staged
        .parent
        .replace_child_typed(&staged.temporary, staged.identity, target);
    #[cfg(test)]
    let replaced = replaced.and_then(|()| {
        super::fault::hit(super::fault::Point::NativeUnknown)
            .map_err(ReplaceFileError::StateUnknown)
    });
    let mut pending = PendingCommit {
        target_parent: Arc::clone(&staged.parent),
        staged,
        target: target.to_owned(),
        allocation_recorded: false,
        lease,
    };
    // Release the temporary kernel lock before logical reconciliation opens
    // CURRENT through another handle. The namespace lease still serializes
    // target visibility through its exact parent acknowledgment.
    let unlocked = pending.unlock_producer();
    match replaced {
        Err(ReplaceFileError::NotReplaced(error)) => Err(CommitFailure {
            visibility: Visibility::NotPublished,
            cause: CommitCause::Replacement(ReplaceFileError::NotReplaced(error)),
            pending: None,
        }),
        Err(ReplaceFileError::StateUnknown(error)) => Err(CommitFailure {
            visibility: Visibility::StateUnknown,
            cause: CommitCause::Replacement(ReplaceFileError::StateUnknown(error)),
            pending: Some(Box::new(pending)),
        }),
        Ok(()) => {
            if let Err(error) = unlocked {
                return Err(CommitFailure {
                    visibility: Visibility::VisibleUnacknowledged,
                    cause: CommitCause::Io(error),
                    pending: Some(Box::new(pending)),
                });
            }
            let recorded = allocation.map_or(Ok(()), |a| {
                a.remove_file_at(&path)?;
                a.replace_file_at(&path, pending.staged.file())?;
                a.remove_file_at(&temporary_path)
            });
            if let Err(error) = recorded {
                return Err(CommitFailure {
                    visibility: Visibility::VisibleUnacknowledged,
                    cause: CommitCause::Io(io::Error::other(error)),
                    pending: Some(Box::new(pending)),
                });
            }
            pending.allocation_recorded = true;
            Ok(pending)
        }
    }
}

pub fn publish_atomic<W, S, B>(
    path: &Path,
    bytes: &[u8],
    hooks: AtomicHooks<W, S, B>,
    allocation: Option<&StorageAllocationOperation>,
) -> Result<(), CommitFailure>
where
    W: FnOnce() -> io::Result<()>,
    S: FnOnce() -> io::Result<()>,
    B: FnOnce() -> io::Result<()>,
{
    let parent = path
        .parent()
        .ok_or_else(|| unpublished(io::Error::other("atomic publication target has no parent")))?;
    let target = path
        .file_name()
        .ok_or_else(|| unpublished(io::Error::other("atomic publication target has no name")))?;
    let parent = StableDirectory::open(parent).map_err(unpublished)?;
    publish_atomic_in(parent, target, bytes, hooks, allocation)?.acknowledge(allocation)
}

#[derive(Debug, Clone, Copy)]
pub enum PublishMode {
    CreateOnly,
    Replace,
    ReplaceAuthenticated(FileIdentity),
}
impl SealedArtifact {
    /// Durable intent already owns this temporary; failed replay must retain it.
    #[must_use]
    pub fn retain_for_recovery(mut self) -> Self {
        self.cleanup = false;
        self
    }

    /// Adopt the exact descriptor covered by an opaque completed seal witness.
    pub fn adopt_sealed(
        parent: &StableDirectory,
        temporary: &OsStr,
        file: File,
        witness: super::FileSeal,
        allocation: Option<&StorageAllocationOperation>,
    ) -> io::Result<Self> {
        Self::adopt_sealed_shared(
            Arc::new(parent.try_clone()?),
            temporary,
            file,
            witness,
            allocation,
        )
    }

    /// Adopt an opaque seal while sharing one retained parent across a batch.
    pub fn adopt_sealed_shared(
        parent: Arc<StableDirectory>,
        temporary: &OsStr,
        file: File,
        witness: super::FileSeal,
        allocation: Option<&StorageAllocationOperation>,
    ) -> io::Result<Self> {
        let identity = witness.consume(&file)?;
        parent.revalidate_named()?;
        let named = parent.open_child_file(temporary)?;
        if graphforge_filesystem::file_identity(&named)? != identity
            || graphforge_filesystem::file_link_count(&file)? != 1
        {
            return Err(io::Error::other("sealed temporary identity changed"));
        }
        Ok(Self {
            parent,
            temporary: temporary.to_owned(),
            file: Some(file),
            identity,
            locked: false,
            allocation: allocation.cloned(),
            cleanup: true,
        })
    }

    /// Adopt an existing privately owned temporary and seal its actual descriptor.
    /// This establishes durability authority only, never content authentication.
    pub fn seal_existing(
        parent: &StableDirectory,
        temporary: &OsStr,
        file: File,
        expected: FileIdentity,
        allocation: Option<&StorageAllocationOperation>,
    ) -> io::Result<Self> {
        Self::seal_existing_shared(
            Arc::new(parent.try_clone()?),
            temporary,
            file,
            expected,
            allocation,
        )
    }

    /// Seal one output while sharing its retained parent across a batch.
    pub fn seal_existing_shared(
        parent: Arc<StableDirectory>,
        temporary: &OsStr,
        file: File,
        expected: FileIdentity,
        allocation: Option<&StorageAllocationOperation>,
    ) -> io::Result<Self> {
        let sealed =
            Self::seal_existing_inner(parent, temporary, file, expected, allocation, true, true)?;
        super::producer_seals::record(sealed.file())?;
        Ok(sealed)
    }

    /// A durable intent owns this input, including a failed sealing attempt.
    pub fn seal_recoverable_existing(
        parent: &StableDirectory,
        temporary: &OsStr,
        file: File,
        expected: FileIdentity,
        allocation: Option<&StorageAllocationOperation>,
    ) -> io::Result<Self> {
        Self::seal_existing_inner(
            Arc::new(parent.try_clone()?),
            temporary,
            file,
            expected,
            allocation,
            false,
            true,
        )
    }

    /// Adopt a privately owned temporary whose producer has already run the
    /// file's durability barrier, without running a second one. The check of
    /// the temporary's identity and link count is the same as for
    /// [`Self::seal_recoverable_existing`]; only the barrier is skipped. The
    /// caller owns the proof that the producer sealed this exact descriptor
    /// after its last write: this establishes no durability itself.
    pub fn adopt_producer_sealed(
        parent: &StableDirectory,
        temporary: &OsStr,
        file: File,
        expected: FileIdentity,
        allocation: Option<&StorageAllocationOperation>,
    ) -> io::Result<Self> {
        Self::seal_existing_inner(
            Arc::new(parent.try_clone()?),
            temporary,
            file,
            expected,
            allocation,
            false,
            false,
        )
    }

    fn seal_existing_inner(
        parent: Arc<StableDirectory>,
        temporary: &OsStr,
        file: File,
        expected: FileIdentity,
        allocation: Option<&StorageAllocationOperation>,
        cleanup: bool,
        barrier: bool,
    ) -> io::Result<Self> {
        let staged = Self {
            parent,
            temporary: temporary.to_owned(),
            file: Some(file),
            identity: expected,
            locked: false,
            allocation: allocation.cloned(),
            cleanup,
        };
        staged.parent.revalidate_named()?;
        let named = staged.parent.open_child_file(temporary)?;
        if graphforge_filesystem::file_identity(staged.file())? != expected
            || graphforge_filesystem::file_identity(&named)? != expected
            || graphforge_filesystem::file_link_count(staged.file())? != 1
        {
            return Err(io::Error::other(
                "staged temporary identity changed before sealing",
            ));
        }
        let sealed = if barrier {
            seal_file(staged.file())
        } else {
            Ok(())
        };
        let observed = allocation.map_or(Ok(()), |a| {
            a.replace_file_at(&staged.parent.path().join(temporary), staged.file())
        });
        sealed?;
        observed.map_err(io::Error::other)?;
        Ok(staged)
    }

    /// Publish an exact sealed child and retain its parent acknowledgment.
    pub fn make_visible(
        self,
        target: &OsStr,
        mode: PublishMode,
        before_visible: impl FnOnce() -> io::Result<()>,
    ) -> Result<PendingCommit, CommitFailure> {
        let lease = namespace_lock(&self.parent.path().join(target));
        #[cfg(test)]
        super::fault::hit(super::fault::Point::BeforeVisible).map_err(unpublished)?;
        before_visible().map_err(unpublished)?;
        let result = match mode {
            PublishMode::CreateOnly => {
                self.parent
                    .install_child_typed(&self.temporary, self.identity, target)
            }
            PublishMode::Replace => {
                self.parent
                    .replace_child_typed(&self.temporary, self.identity, target)
            }
            PublishMode::ReplaceAuthenticated(prior) => self
                .parent
                .replace_authenticated_child_typed(&self.temporary, self.identity, target, prior),
        };
        #[cfg(test)]
        let result = result.and_then(|()| {
            super::fault::hit(super::fault::Point::NativeUnknown)
                .map_err(ReplaceFileError::StateUnknown)
        });
        let pending = PendingCommit {
            target_parent: Arc::clone(&self.parent),
            staged: self,
            target: target.to_owned(),
            allocation_recorded: false,
            lease,
        };
        match result {
            Ok(()) => Ok(pending),
            Err(ReplaceFileError::NotReplaced(cause)) => Err(CommitFailure {
                visibility: Visibility::NotPublished,
                cause: CommitCause::Replacement(ReplaceFileError::NotReplaced(cause)),
                pending: None,
            }),
            Err(ReplaceFileError::StateUnknown(cause)) => Err(CommitFailure {
                visibility: Visibility::StateUnknown,
                cause: CommitCause::Replacement(ReplaceFileError::StateUnknown(cause)),
                pending: Some(Box::new(pending)),
            }),
        }
    }

    /// Replace one exact authenticated target in another retained directory.
    /// This is restricted to authenticated replacement; creation and ordinary
    /// replacement continue to use the source's own parent authority.
    pub(crate) fn make_visible_into(
        #[cfg_attr(not(windows), allow(unused_mut))] mut self,
        destination: &StableDirectory,
        target: &OsStr,
        expected_target: FileIdentity,
        before_visible: impl FnOnce() -> io::Result<()>,
    ) -> Result<PendingCommit, CommitFailure> {
        let target_parent = Arc::new(destination.try_clone().map_err(unpublished)?);
        let lease = namespace_lock(&destination.path().join(target));
        #[cfg(test)]
        super::fault::hit(super::fault::Point::BeforeVisible).map_err(unpublished)?;
        before_visible().map_err(unpublished)?;
        // A Windows sealed reader denies write and delete sharing, so this
        // producer's own retained reader would refuse the native rename of
        // the very inode it holds. The replacement authenticates the exact
        // source identity on its rename handle, and acknowledgement reopens
        // the published name, so only the descriptor is released here.
        #[cfg(windows)]
        if !self.locked {
            drop(self.file.take());
        }
        let result = destination.replace_authenticated_child_from(
            &self.parent,
            &self.temporary,
            self.identity,
            target,
            expected_target,
        );
        #[cfg(test)]
        let result = result.and_then(|()| {
            super::fault::hit(super::fault::Point::NativeUnknown)
                .map_err(ReplaceFileError::StateUnknown)
        });
        let pending = PendingCommit {
            staged: self,
            target_parent,
            target: target.to_owned(),
            allocation_recorded: false,
            lease,
        };
        match result {
            Ok(()) => Ok(pending),
            Err(ReplaceFileError::NotReplaced(cause)) => Err(CommitFailure {
                visibility: Visibility::NotPublished,
                cause: CommitCause::Replacement(ReplaceFileError::NotReplaced(cause)),
                pending: None,
            }),
            Err(ReplaceFileError::StateUnknown(cause)) => Err(CommitFailure {
                visibility: Visibility::StateUnknown,
                cause: CommitCause::Replacement(ReplaceFileError::StateUnknown(cause)),
                pending: Some(Box::new(pending)),
            }),
        }
    }
}

/// Create and produce one owned temporary; failed writes still record allocation.
pub fn stage_writer(
    parent: &StableDirectory,
    temporary: &OsStr,
    producer: impl FnOnce(&mut File) -> io::Result<()>,
    allocation: Option<&StorageAllocationOperation>,
) -> io::Result<SealedArtifact> {
    stage_writer_with_hook(parent, temporary, producer, || Ok(()), allocation)
}

/// Produce a private staged file, with a format-owned postwrite hook before
/// its sole file barrier. The enclosing generation owns later namespace ack.
pub fn stage_private_file(
    parent: &StableDirectory,
    name: &OsStr,
    producer: impl FnOnce(&mut File) -> io::Result<()>,
    after_write: impl FnOnce() -> io::Result<()>,
    allocation: Option<&StorageAllocationOperation>,
) -> io::Result<File> {
    let mut staged = stage_writer_with_hook(parent, name, producer, after_write, allocation)?;
    staged.cleanup = false;
    Ok(staged
        .file
        .take()
        .expect("private staged descriptor is present"))
}

fn stage_writer_with_hook(
    parent: &StableDirectory,
    temporary: &OsStr,
    producer: impl FnOnce(&mut File) -> io::Result<()>,
    after_write: impl FnOnce() -> io::Result<()>,
    allocation: Option<&StorageAllocationOperation>,
) -> io::Result<SealedArtifact> {
    let mut guard = parent.create_unpublished_replaceable_child(temporary)?;
    let mut file = guard.take_file()?;
    let written = producer(&mut file);
    let observed = allocation.map_or(Ok(()), |a| {
        a.replace_file_at(&parent.path().join(temporary), &file)
    });
    let prepared = written
        .and_then(|()| observed.map_err(io::Error::other))
        .and_then(|()| after_write());
    if let Err(error) = prepared {
        drop(file);
        drop(guard);
        if matches!(parent.path().join(temporary).try_exists(), Ok(false))
            && let Some(allocation) = allocation
        {
            let _ = allocation.remove_file_at(&parent.path().join(temporary));
        }
        return Err(error);
    }
    super::seal_guarded(guard, file, allocation)
}

impl SealedArtifact {
    #[must_use]
    pub fn file(&self) -> &File {
        self.file
            .as_ref()
            .expect("owned staged descriptor is present")
    }
    fn file_mut(&mut self) -> &mut File {
        self.file
            .as_mut()
            .expect("owned staged descriptor is present")
    }
    #[must_use]
    pub const fn identity(&self) -> FileIdentity {
        self.identity
    }
}

/// Link an immutable sealed artifact, acknowledge its bucket, then retire only
/// the owned temporary alias and acknowledge that namespace. The content owner
/// authenticates every unrecognized existing/concurrent winner.
pub fn install_immutable<A, B, D, T>(
    mut sealed: SealedArtifact,
    destination: &StableDirectory,
    target: &OsStr,
    authenticate_winner: A,
    before_destination_ack: B,
    after_destination_ack: D,
    after_temporary_retire: T,
) -> io::Result<(File, FileIdentity, bool)>
where
    A: FnOnce(&File, FileIdentity) -> io::Result<()>,
    B: FnOnce(bool, &File) -> io::Result<()>,
    D: FnOnce(bool, &File) -> io::Result<()>,
    T: FnOnce() -> io::Result<()>,
{
    // Unknown native outcomes leave crash residue for the existing recovery
    // trust boundary; Drop cannot retire a source before destination ack.
    sealed.cleanup = false;
    let (installed, identity, reused) = link_sealed(
        &sealed,
        destination,
        target,
        authenticate_winner,
        before_destination_ack,
        after_destination_ack,
    )?;
    drop(sealed.file.take());
    sealed
        .parent
        .unlink_child_if_identity(&sealed.temporary, sealed.identity)?;
    if let Some(allocation) = &sealed.allocation {
        allocation
            .remove_file_at(&sealed.parent.path().join(&sealed.temporary))
            .map_err(io::Error::other)?;
    }
    after_temporary_retire()?;
    acknowledge_directory(&sealed.parent)?;
    Ok((installed, identity, reused))
}

/// Give an already-sealed, privately staged file its immutable name without
/// copying it: link the exact inode into `destination`, then acknowledge the
/// destination namespace. The staged name is left in place for its owner to
/// retire with the rest of its private tree, so the staged inode stays valid
/// for a retried publication, and a crash at any point leaves the staged name
/// intact. The content owner authenticates every unrecognized existing winner;
/// a winner's inode is returned and the staged inode is not aliased.
///
/// The staged name must be on the same filesystem as `destination`; a link
/// across filesystems fails rather than falling back to a copy.
///
/// # Errors
/// Returns the first failed identity check, link, authentication, hook or
/// namespace acknowledgment. Nothing is removed on failure.
pub fn link_immutable<A, B, D>(
    sealed: &SealedArtifact,
    destination: &StableDirectory,
    target: &OsStr,
    authenticate_winner: A,
    before_destination_ack: B,
    after_destination_ack: D,
) -> io::Result<(File, FileIdentity, bool)>
where
    A: FnOnce(&File, FileIdentity) -> io::Result<()>,
    B: FnOnce(bool, &File) -> io::Result<()>,
    D: FnOnce(bool, &File) -> io::Result<()>,
{
    if sealed.cleanup {
        return Err(io::Error::other(
            "a linked staged file must be owned by its private tree, not a cleanup guard",
        ));
    }
    link_sealed(
        sealed,
        destination,
        target,
        authenticate_winner,
        before_destination_ack,
        after_destination_ack,
    )
}

fn link_sealed<A, B, D>(
    sealed: &SealedArtifact,
    destination: &StableDirectory,
    target: &OsStr,
    authenticate_winner: A,
    before_destination_ack: B,
    after_destination_ack: D,
) -> io::Result<(File, FileIdentity, bool)>
where
    A: FnOnce(&File, FileIdentity) -> io::Result<()>,
    B: FnOnce(bool, &File) -> io::Result<()>,
    D: FnOnce(bool, &File) -> io::Result<()>,
{
    let (installed, identity, reused) = match sealed.parent.link_child_into(
        &sealed.temporary,
        sealed.file(),
        sealed.identity,
        destination,
        target,
    ) {
        Ok((file, identity)) => (file, identity, false),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            #[cfg(not(windows))]
            let file = destination.open_child_file(target)?;
            #[cfg(windows)]
            let file = destination.open_cas_child_file(target)?.into_file();
            let identity = graphforge_filesystem::file_identity(&file)?;
            authenticate_winner(&file, identity)?;
            (file, identity, true)
        }
        Err(error) => return Err(error),
    };
    if !reused && let Some(allocation) = &sealed.allocation {
        // Native allocation can change when a small resident file gains a
        // second name. Refresh the still-owned temporary before the content
        // owner records the new alias of this same identity. A reused winner
        // is a different inode; its temporary must remain charged separately.
        allocation
            .replace_file_at(&sealed.parent.path().join(&sealed.temporary), &installed)
            .map_err(io::Error::other)?;
    }
    before_destination_ack(reused, &installed)?;
    acknowledge_directory(destination)?;
    after_destination_ack(reused, &installed)?;
    Ok((installed, identity, reused))
}

#[derive(Debug)]
pub struct GroupCommitFailure {
    pub visibility: Visibility,
    pub cause: io::Error,
    pub pending: Vec<PendingCommit>,
    pub retirement: Option<super::RetirementBatch>,
}
impl std::fmt::Display for GroupCommitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.cause.fmt(f)
    }
}
impl std::error::Error for GroupCommitFailure {}
impl PendingCommit {
    /// Acknowledge one caller-bounded, logically serialized same-parent batch.
    /// Every visible inode is checked before the sole parent fence; failures
    /// retain the same owners, including their exact namespace leases.
    pub fn acknowledge_group(
        mut pending: Vec<Self>,
        allocation: Option<&StorageAllocationOperation>,
    ) -> Result<(), GroupCommitFailure> {
        let result = (|| -> io::Result<()> {
            let Some(first) = pending.first() else {
                return Ok(());
            };
            let identity = first.staged.parent.identity();
            for commit in &mut pending {
                commit.staged.parent.revalidate_named()?;
                if commit.staged.parent.identity() != identity {
                    return Err(io::Error::other(
                        "commit batch has different parent authorities",
                    ));
                }
                let installed = commit.staged.parent.open_child_file(&commit.target)?;
                if graphforge_filesystem::file_identity(&installed)? != commit.staged.identity {
                    return Err(io::Error::other("commit batch visible identity changed"));
                }
                let allocation = allocation.or(commit.staged.allocation.as_ref());
                if !commit.allocation_recorded {
                    if let Some(allocation) = allocation {
                        let target = commit.staged.parent.path().join(&commit.target);
                        allocation
                            .remove_file_at(&target)
                            .map_err(io::Error::other)?;
                        allocation
                            .replace_file_at(&target, &installed)
                            .map_err(io::Error::other)?;
                        allocation
                            .remove_file_at(
                                &commit.staged.parent.path().join(&commit.staged.temporary),
                            )
                            .map_err(io::Error::other)?;
                    }
                    commit.allocation_recorded = true;
                }
            }
            for commit in &mut pending {
                if commit.staged.locked {
                    crate::file_lock::unlock(commit.staged.file())?;
                    commit.staged.locked = false;
                }
            }
            acknowledge_directory(&pending[0].staged.parent)
        })();
        match result {
            Ok(()) => Ok(()),
            Err(cause) => Err(GroupCommitFailure {
                visibility: Visibility::VisibleUnacknowledged,
                cause,
                pending,
                retirement: None,
            }),
        }
    }
}
