//! Single-pass authentication and private component materialization.
use super::{
    check_cancel, fs, hex, preflight, scan, AtomicBool, BTreeMap, BTreeSet, File, Path,
    PortableV2Error, PortableV2ErrorCode, PortableV2Limits, PortableV2Mode, PortableV2Report, Read,
    VerifiedMaterialization,
};
use std::io::Write;
mod derived;
pub(crate) use derived::capture_import_adjacency;
mod compact_root;
pub(crate) use compact_root::publish_compact_import_root;
mod composition_control;
pub(crate) use composition_control::{
    persist_composition_authority, persist_staged_composition, CapturedCompositionControl,
};

/// A private exact-byte capture from the authenticated import copy.
/// Only the authenticated copier or actual derived writer can mint it.
pub(crate) struct MaterializedCapture {
    identity: graphforge_filesystem::FileIdentity,
    length: u64,
    digest: [u8; 32],
    checksum: u64,
    allocated_bytes: u64,
}
impl MaterializedCapture {
    pub(crate) fn open_source(
        &self,
        path: &Path,
    ) -> Result<CapturedPortableSource<'_>, PortableV2Error> {
        let parent =
            graphforge_filesystem::StableDirectory::open(path.parent().ok_or_else(|| {
                PortableV2Error::new(PortableV2ErrorCode::InvalidPath, "captured source parent")
            })?)
            .map_err(|_| {
                PortableV2Error::new(
                    PortableV2ErrorCode::ConcurrentMutation,
                    "captured source directory changed",
                )
            })?;
        let name = path
            .file_name()
            .ok_or_else(|| {
                PortableV2Error::new(PortableV2ErrorCode::InvalidPath, "captured source name")
            })?
            .to_os_string();
        let file = parent.open_child_file(&name).map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::ConcurrentMutation,
                "captured source changed",
            )
        })?;
        let source = CapturedPortableSource {
            parent,
            name,
            file,
            capture: self,
            digest: hex(&self.digest),
        };
        source.revalidate().map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::ConcurrentMutation,
                "captured source identity changed",
            )
        })?;
        Ok(source)
    }
    pub(crate) fn matches_file(
        &self,
        file: &File,
        length: u64,
        digest: [u8; 32],
        checksum: u64,
    ) -> bool {
        self.length == length
            && self.digest == digest
            && self.checksum == checksum
            && graphforge_filesystem::file_identity(file)
                .is_ok_and(|identity| identity == self.identity)
    }
    pub(crate) fn authenticate(
        &self,
        path: &Path,
        cancelled: Option<&AtomicBool>,
    ) -> Result<[u8; 32], PortableV2Error> {
        let mut file = crate::project_portable_v2_export::open_source_no_follow(path)?;
        if graphforge_filesystem::file_identity(&file).map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::Io, "materialized file identity")
        })? != self.identity
            || file
                .metadata()
                .map_err(|_| {
                    PortableV2Error::new(PortableV2ErrorCode::Io, "materialized file metadata")
                })?
                .len()
                != self.length
        {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::ConcurrentMutation,
                "materialized file changed",
            ));
        }
        let mut checksum = crate::corruption_checksum::Checksum::new();
        let mut bytes = 0_u64;
        let mut buffer = vec![0; 64 * 1024];
        let bound = self.length.checked_add(1).ok_or_else(|| {
            PortableV2Error::new(
                PortableV2ErrorCode::LimitExceeded,
                "materialized length overflow",
            )
        })?;
        let mut reader = (&mut file).take(bound);
        loop {
            check_cancel(cancelled)?;
            let count = reader.read(&mut buffer).map_err(|_| {
                PortableV2Error::new(PortableV2ErrorCode::Io, "cannot read materialized file")
            })?;
            if count == 0 {
                break;
            }
            bytes += count as u64;
            checksum.update(&buffer[..count]);
        }
        if bytes != self.length
            || checksum.finish() != self.checksum
            || graphforge_filesystem::path_identity(path).map_err(|_| {
                PortableV2Error::new(
                    PortableV2ErrorCode::ConcurrentMutation,
                    "materialized path changed",
                )
            })? != self.identity
        {
            return Err(PortableV2Error::new(
                PortableV2ErrorCode::ConcurrentMutation,
                "materialized checksum changed",
            ));
        }
        Ok(self.digest)
    }
}

/// Retained authority from a successful authenticated copy or private derived writer.
/// Metadata tuples and public installers cannot construct this capability.
pub(crate) struct CapturedPortableSource<'a> {
    parent: graphforge_filesystem::StableDirectory,
    name: std::ffi::OsString,
    file: File,
    capture: &'a MaterializedCapture,
    digest: String,
}
impl CapturedPortableSource<'_> {
    pub(crate) fn content_sha256(&self) -> &str {
        &self.digest
    }
    pub(crate) fn bytes(&self) -> u64 {
        self.capture.length
    }
    pub(crate) fn checksum(&self) -> u64 {
        self.capture.checksum
    }
    pub(crate) fn source(&self) -> &File {
        &self.file
    }
    pub(crate) fn revalidate(&self) -> Result<(), graphforge_core::GfError> {
        let check = || -> std::io::Result<bool> {
            self.parent.revalidate_named()?;
            let named = self.parent.open_child_file(&self.name)?;
            let metadata = self.file.metadata()?;
            Ok(metadata.is_file()
                && metadata.len() == self.bytes()
                && graphforge_filesystem::file_identity(&self.file)? == self.capture.identity
                && graphforge_filesystem::file_identity(&named)? == self.capture.identity
                && graphforge_filesystem::file_link_count(&self.file)? == 1
                && graphforge_filesystem::file_space_usage(&self.file)?.allocated_bytes
                    == self.capture.allocated_bytes)
        };
        if check().unwrap_or(false) {
            Ok(())
        } else {
            Err(graphforge_core::GfError::Validation(
                "captured portable source identity, length, links or allocation changed".into(),
            ))
        }
    }
}

/// Authenticate the exact bytes copied into a new private directory.
/// No project authority is published until the complete package is admitted.
pub fn materialize_verified_portable_v2(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableV2Report, PortableV2Error> {
    materialize_verified_portable_v2_observed(
        source,
        destination,
        limits,
        cancelled,
        |_, _| Ok(()),
        false,
    )
    .map(|materialized| materialized.report)
}

pub(crate) fn materialize_verified_portable_v2_observed(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
    mut observed: impl FnMut(&Path, Option<&File>) -> Result<(), PortableV2Error>,
    track_removals: bool,
) -> Result<VerifiedMaterialization, PortableV2Error> {
    let destination = destination.as_ref();
    if destination.exists() {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Io,
            "materialization destination exists",
        ));
    }
    // Admission happens before creation; no malformed/future package can allocate payload staging.
    preflight(source.as_ref(), limits, cancelled)?;
    fs::create_dir(destination).map_err(|_| {
        PortableV2Error::new(PortableV2ErrorCode::Io, "cannot create materialization")
    })?;
    let mut sink = CopySink {
        destination,
        observed: &mut observed,
        routes: BTreeSet::new(),
        track_removals,
        bytes: 0,
        operations: 0,
        captures: BTreeMap::new(),
    };
    let result = (|| {
        let report = scan(
            source.as_ref(),
            PortableV2Mode::Full,
            limits,
            cancelled,
            Some(&mut sink),
            None,
        )?;
        // Refuse a successful write whose physical bytes changed before the
        // private directory becomes available. This is a checksum readback of
        // writer-owned output, not another authentication of the source.
        for (relative, capture) in &sink.captures {
            capture.authenticate(&destination.join(relative), cancelled)?;
        }
        sync_materialized_tree(destination)?;
        Ok(VerifiedMaterialization {
            report,
            application_read_bytes: sink.bytes,
            application_read_operations: sink.operations,
            captures: std::mem::take(&mut sink.captures),
        })
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(destination);
        for path in &sink.routes {
            if matches!(fs::symlink_metadata(path), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
            {
                let _ = (sink.observed)(path, None);
            }
        }
    }
    result
}

type EntryObserver<'a> = dyn FnMut(&Path, Option<&File>) -> Result<(), PortableV2Error> + 'a;

pub(super) struct CopySink<'a> {
    destination: &'a Path,
    observed: &'a mut EntryObserver<'a>,
    routes: BTreeSet<std::path::PathBuf>,
    track_removals: bool,
    bytes: u64,
    operations: u64,
    captures: BTreeMap<String, MaterializedCapture>,
}
impl CopySink<'_> {
    pub(super) fn open(&mut self, relative: &str) -> Result<Option<File>, PortableV2Error> {
        if !relative.starts_with("data/components/") {
            return Ok(None);
        }
        let path = self.destination.join(relative);
        fs::create_dir_all(path.parent().expect("component parent")).map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::Io,
                relative,
                "cannot create component parent",
            )
        })?;
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|_| {
                PortableV2Error::at(PortableV2ErrorCode::Io, relative, "cannot stage entry")
            })?;
        if self.track_removals {
            self.routes.insert(path.clone());
        }
        (self.observed)(&path, Some(&file))?;
        Ok(Some(file))
    }
    pub(super) fn write(
        &mut self,
        relative: &str,
        file: &mut File,
        bytes: &[u8],
    ) -> Result<(), PortableV2Error> {
        let result = file.write_all(bytes).map_err(|_| {
            PortableV2Error::at(PortableV2ErrorCode::Io, relative, "cannot copy entry")
        });
        let refreshed = (self.observed)(&self.destination.join(relative), Some(file));
        result?;
        refreshed?;
        self.bytes = self.bytes.saturating_add(bytes.len() as u64);
        self.operations = self.operations.saturating_add(1);
        Ok(())
    }
    pub(super) fn finish(
        &mut self,
        relative: &str,
        file: File,
        length: u64,
        digest: [u8; 32],
        checksum: u64,
    ) -> Result<(), PortableV2Error> {
        let result = crate::durable_commit::seal_file(&file).map_err(|_| {
            PortableV2Error::at(PortableV2ErrorCode::Io, relative, "cannot sync entry")
        });
        let refreshed = (self.observed)(&self.destination.join(relative), Some(&file));
        result?;
        refreshed?;
        let identity = graphforge_filesystem::file_identity(&file).map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::Io,
                relative,
                "cannot capture materialized identity",
            )
        })?;
        // Closing the last writer can release speculative filesystem allocation.
        // Bind the reopened reader to the actual written inode before recording it.
        drop(file);
        let path = self.destination.join(relative);
        let directory =
            graphforge_filesystem::StableDirectory::open(path.parent().expect("component parent"))
                .map_err(|_| {
                    PortableV2Error::at(
                        PortableV2ErrorCode::ConcurrentMutation,
                        relative,
                        "materialized directory changed",
                    )
                })?;
        let file = directory
            .open_child_file(path.file_name().expect("component name"))
            .map_err(|_| {
                PortableV2Error::at(
                    PortableV2ErrorCode::ConcurrentMutation,
                    relative,
                    "materialized file changed after writer close",
                )
            })?;
        directory.revalidate_named().map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::ConcurrentMutation,
                relative,
                "materialized directory changed",
            )
        })?;
        let metadata = file.metadata().map_err(|_| {
            PortableV2Error::at(PortableV2ErrorCode::Io, relative, "materialized metadata")
        })?;
        if !metadata.is_file()
            || metadata.len() != length
            || graphforge_filesystem::file_identity(&file).ok() != Some(identity)
            || graphforge_filesystem::file_link_count(&file).ok() != Some(1)
        {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::ConcurrentMutation,
                relative,
                "materialized file changed after writer close",
            ));
        }
        (self.observed)(&path, Some(&file))?;
        let allocated_bytes = graphforge_filesystem::file_space_usage(&file)
            .map_err(|_| {
                PortableV2Error::at(
                    PortableV2ErrorCode::Io,
                    relative,
                    "cannot capture materialized allocation",
                )
            })?
            .allocated_bytes;
        self.captures.insert(
            relative.into(),
            MaterializedCapture {
                identity,
                length,
                digest,
                checksum,
                allocated_bytes,
            },
        );
        Ok(())
    }
}

fn sync_materialized_tree(root: &Path) -> Result<(), PortableV2Error> {
    let mut directories = vec![root.to_owned()];
    let mut index = 0;
    while index < directories.len() {
        for entry in fs::read_dir(&directories[index]).map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::Io, "cannot read staged directory")
        })? {
            let entry = entry.map_err(|_| {
                PortableV2Error::new(PortableV2ErrorCode::Io, "cannot read staged entry")
            })?;
            if entry
                .file_type()
                .map_err(|_| {
                    PortableV2Error::new(PortableV2ErrorCode::Io, "cannot inspect staged entry")
                })?
                .is_dir()
            {
                directories.push(entry.path());
            }
        }
        index += 1;
    }
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        crate::project_publication::sync_directory(&directory).map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::Io, "cannot sync staged directory")
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
