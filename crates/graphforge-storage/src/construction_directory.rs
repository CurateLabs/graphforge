//! Construction-local retained directory plus explicit diagnostic ownership.
//! Reads preserve the underlying no-follow authority; observed mutations are
//! named methods rather than implicit dereferencing or ambient callbacks.
use crate::StorageAllocationOperation;
use graphforge_filesystem::{FileIdentity, StableDirectory};
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::path::Path;

pub(crate) struct ConstructionDirectory {
    directory: StableDirectory,
    allocation: Option<StorageAllocationOperation>,
}
impl ConstructionDirectory {
    pub(crate) fn from_physical(
        directory: &StableDirectory,
        allocation: Option<&StorageAllocationOperation>,
    ) -> io::Result<Self> {
        Ok(Self {
            directory: directory.try_clone()?,
            allocation: allocation.cloned(),
        })
    }

    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            directory: StableDirectory::open(path)?,
            allocation: None,
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
    pub(crate) fn sync(&self) -> io::Result<()> {
        self.directory.sync()
    }
    pub(crate) fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            directory: self.directory.try_clone()?,
            allocation: self.allocation.clone(),
        })
    }
    pub(crate) fn open_child_directory(&self, name: &OsStr) -> io::Result<Self> {
        Ok(Self {
            directory: self.directory.open_child_directory(name)?,
            allocation: self.allocation.clone(),
        })
    }
    pub(crate) fn create_child_directory(&self, name: &OsStr) -> io::Result<Self> {
        Ok(Self {
            directory: self.directory.create_child_directory(name)?,
            allocation: self.allocation.clone(),
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
        if self.allocation.is_none() {
            return self.directory.install_child(temporary, identity, target);
        }
        let file = self.directory.open_child_file(temporary)?;
        self.observe_file(temporary, &file)?;
        self.directory.install_child(temporary, identity, target)?;
        self.record_replacement(temporary, target, &file)
    }
    pub(crate) fn replace_child(
        &self,
        temporary: &OsStr,
        identity: FileIdentity,
        target: &OsStr,
    ) -> io::Result<()> {
        if self.allocation.is_none() {
            return self.directory.replace_child(temporary, identity, target);
        }
        let file = self.directory.open_child_file(temporary)?;
        self.observe_file(temporary, &file)?;
        self.directory.replace_child(temporary, identity, target)?;
        self.record_replacement(temporary, target, &file)
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
        self.directory.unlink_child_if_identity(name, identity)?;
        if let Some(allocation) = &self.allocation {
            allocation
                .remove_file_at(&self.path().join(name))
                .map_err(io::Error::other)?;
        }
        Ok(())
    }
}
