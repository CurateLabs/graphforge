//! Construction-local retained directory plus explicit diagnostic ownership.
//! Reads preserve the underlying no-follow authority; observed mutations are
//! named methods rather than implicit dereferencing or ambient callbacks.
use crate::StorageAllocationOperation;
use graphforge_filesystem::{FileIdentity, StableDirectory};
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct ConstructionCommits {
    seals: Vec<(FileIdentity, crate::durable_commit::FileSeal)>,
    pending: Vec<crate::durable_commit::PendingCommit>,
    retirement: Option<crate::durable_commit::RetirementBatch>,
}

pub(crate) struct ConstructionDirectory {
    directory: Arc<StableDirectory>,
    allocation: Option<StorageAllocationOperation>,
    commits: Arc<Mutex<ConstructionCommits>>,
}
impl ConstructionDirectory {
    pub(crate) fn from_physical(
        directory: &StableDirectory,
        allocation: Option<&StorageAllocationOperation>,
    ) -> io::Result<Self> {
        Ok(Self {
            directory: Arc::new(directory.try_clone()?),
            allocation: allocation.cloned(),
            commits: Arc::default(),
        })
    }

    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            directory: Arc::new(StableDirectory::open(path)?),
            allocation: None,
            commits: Arc::default(),
        })
    }
    pub(crate) fn with_allocation(
        mut self,
        allocation: Option<StorageAllocationOperation>,
    ) -> Self {
        self.allocation = allocation;
        self
    }
    pub(crate) fn allocation(&self) -> Option<&StorageAllocationOperation> {
        self.allocation.as_ref()
    }
    pub(crate) fn physical(&self) -> &StableDirectory {
        &self.directory
    }
    pub(crate) fn path(&self) -> &Path {
        self.directory.path()
    }
    pub(crate) fn identity(&self) -> FileIdentity {
        self.directory.identity()
    }
    pub(crate) fn revalidate_named(&self) -> io::Result<()> {
        self.directory.revalidate_named()
    }
    pub(crate) fn acknowledge(&self) -> io::Result<()> {
        let (pending, retirement) = {
            let mut state = self
                .commits
                .lock()
                .map_err(|_| io::Error::other("construction commit batch poisoned"))?;
            (std::mem::take(&mut state.pending), state.retirement.take())
        };
        if pending.is_empty() && retirement.is_none() {
            return crate::durable_commit::acknowledge_directory(&self.directory);
        }
        let acknowledged = match retirement {
            Some(retirement) => retirement.acknowledge_with(pending, self.allocation.as_ref()),
            None => crate::durable_commit::PendingCommit::acknowledge_group(
                pending,
                self.allocation.as_ref(),
            ),
        };
        match acknowledged {
            Ok(()) => Ok(()),
            Err(failure) => {
                let mut state = self
                    .commits
                    .lock()
                    .map_err(|_| io::Error::other("construction commit batch poisoned"))?;
                state.pending.extend(failure.pending);
                state.retirement = failure.retirement;
                Err(failure.cause)
            }
        }
    }

    pub(crate) fn install_guarded(
        &self,
        guard: &mut graphforge_filesystem::UnpublishedArtifactGuard,
        target: &OsStr,
    ) -> io::Result<()> {
        let identity = guard.identity()?;
        let witness = {
            let mut state = self
                .commits
                .lock()
                .map_err(|_| io::Error::other("construction commit batch poisoned"))?;
            state
                .seals
                .iter()
                .position(|(candidate, _)| *candidate == identity)
                .map(|position| state.seals.swap_remove(position).1)
        }
        .ok_or_else(|| io::Error::other("construction guarded producer is not sealed"))?;
        crate::durable_commit::install_guarded_sealed(guard, target, witness)
    }
    pub(crate) fn seal_file(&self, file: &File) -> io::Result<()> {
        self.remember_seal(crate::durable_commit::seal_file_witness(file)?)
    }
    pub(crate) fn seal_cache_writer(
        &self,
        writer: &mut graphforge_filesystem::DurableFileCacheWriter,
    ) -> io::Result<()> {
        self.remember_seal(crate::durable_commit::seal_cache_writer_witness(writer)?)
    }
    fn remember_seal(&self, witness: crate::durable_commit::FileSeal) -> io::Result<()> {
        let identity = witness.identity();
        let mut state = self
            .commits
            .lock()
            .map_err(|_| io::Error::other("construction commit batch poisoned"))?;
        state.seals.retain(|(prior, _)| *prior != identity);
        state.seals.push((identity, witness));
        Ok(())
    }
    pub(crate) fn try_clone(&self) -> io::Result<Self> {
        self.directory.revalidate_named()?;
        Ok(Self {
            directory: self.directory.clone(),
            allocation: self.allocation.clone(),
            commits: self.commits.clone(),
        })
    }
    pub(crate) fn open_child_directory(&self, name: &OsStr) -> io::Result<Self> {
        Ok(Self {
            directory: Arc::new(self.directory.open_child_directory(name)?),
            allocation: self.allocation.clone(),
            commits: Arc::default(),
        })
    }
    pub(crate) fn create_child_directory(&self, name: &OsStr) -> io::Result<Self> {
        Ok(Self {
            directory: Arc::new(self.directory.create_child_directory(name)?),
            allocation: self.allocation.clone(),
            commits: Arc::default(),
        })
    }
    pub(crate) fn open_child_file(&self, name: &OsStr) -> io::Result<File> {
        self.directory.open_child_file(name)
    }
    pub(crate) fn create_replaceable_child_file(&self, name: &OsStr) -> io::Result<File> {
        self.directory.create_replaceable_child_file(name)
    }
    pub(crate) fn open_or_create_child_file(&self, name: &OsStr) -> io::Result<File> {
        let file = self.directory.open_or_create_child_file(name)?;
        self.observe_file(name, &file)?;
        Ok(file)
    }
    pub(crate) fn child_names_bounded(&self, bound: usize) -> io::Result<Vec<OsString>> {
        self.directory.child_names_bounded(bound)
    }
    pub(crate) fn remove_child_directory_if_identity(
        &self,
        name: &OsStr,
        identity: FileIdentity,
    ) -> io::Result<()> {
        self.directory
            .remove_child_directory_if_identity(name, identity)
    }
    pub(crate) fn create_unpublished_replaceable_child(
        &self,
        name: &OsStr,
    ) -> io::Result<graphforge_filesystem::UnpublishedArtifactGuard> {
        self.directory.create_unpublished_replaceable_child(name)
    }
    pub(crate) fn child_names(&self) -> io::Result<Vec<OsString>> {
        self.directory.child_names()
    }
    pub(crate) fn observe_file(&self, name: &OsStr, file: &File) -> io::Result<()> {
        if let Some(allocation) = &self.allocation {
            allocation
                .replace_file_at(&self.path().join(name), file)
                .map_err(io::Error::other)?;
        }
        Ok(())
    }
    pub(crate) fn install_child(
        &self,
        temporary: &OsStr,
        identity: FileIdentity,
        target: &OsStr,
    ) -> io::Result<()> {
        self.publish_child(
            temporary,
            identity,
            target,
            crate::durable_commit::PublishMode::CreateOnly,
        )
    }
    pub(crate) fn replace_child(
        &self,
        temporary: &OsStr,
        identity: FileIdentity,
        target: &OsStr,
    ) -> io::Result<()> {
        self.publish_child(
            temporary,
            identity,
            target,
            crate::durable_commit::PublishMode::Replace,
        )
    }
    fn publish_child(
        &self,
        temporary: &OsStr,
        identity: FileIdentity,
        target: &OsStr,
        mode: crate::durable_commit::PublishMode,
    ) -> io::Result<()> {
        let file = crate::durable_commit::open_publisher(&self.directory, temporary, identity)?;
        let witness = {
            let mut state = self
                .commits
                .lock()
                .map_err(|_| io::Error::other("construction commit batch poisoned"))?;
            state
                .seals
                .iter()
                .position(|(candidate, _)| *candidate == identity)
                .map(|position| state.seals.swap_remove(position).1)
        };
        let staged = match witness {
            Some(witness) => crate::durable_commit::SealedArtifact::adopt_sealed_shared(
                self.directory.clone(),
                temporary,
                file,
                witness,
                self.allocation.as_ref(),
            )?,
            None => crate::durable_commit::SealedArtifact::seal_existing_shared(
                self.directory.clone(),
                temporary,
                file,
                identity,
                self.allocation.as_ref(),
            )?,
        };
        let mut pending = staged
            .make_visible(target, mode, || Ok(()))
            .map_err(io::Error::other)?;
        pending.release_unlocked_producer()?;
        self.commits
            .lock()
            .map_err(|_| io::Error::other("construction commit batch poisoned"))?
            .pending
            .push(pending);
        Ok(())
    }
    pub(crate) fn record_replacement(
        &self,
        temporary: &OsStr,
        target: &OsStr,
        file: &File,
    ) -> io::Result<()> {
        if let Some(allocation) = &self.allocation {
            allocation
                .remove_file_at(&self.path().join(target))
                .map_err(io::Error::other)?;
            self.observe_file(target, file)?;
            allocation
                .remove_file_at(&self.path().join(temporary))
                .map_err(io::Error::other)?;
        }
        Ok(())
    }
    pub(crate) fn unlink_child_if_identity(
        &self,
        name: &OsStr,
        identity: FileIdentity,
    ) -> io::Result<()> {
        {
            let mut state = self
                .commits
                .lock()
                .map_err(|_| io::Error::other("construction commit batch poisoned"))?;
            if state.retirement.is_none() {
                state.retirement = Some(crate::durable_commit::RetirementBatch::new(
                    &self.directory,
                )?);
            }
            state
                .retirement
                .as_mut()
                .expect("retirement initialized")
                .unlink(name, identity)?;
            state
                .pending
                .retain(|pending| !pending.matches_target(name, identity));
            state.seals.retain(|(sealed, _)| *sealed != identity);
        }
        if let Some(allocation) = &self.allocation {
            allocation
                .remove_file_at(&self.path().join(name))
                .map_err(io::Error::other)?;
        }
        Ok(())
    }
}
