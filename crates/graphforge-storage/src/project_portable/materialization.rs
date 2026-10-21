//! Single-pass authentication and private component materialization.
use super::{
    AtomicBool, BTreeMap, BTreeSet, File, Path, PortableError, PortableErrorCode, PortableLimits,
    PortableMode, PortableReport, Read, VerifiedMaterialization, check_cancel, fs, hex, preflight,
    scan,
};
use std::io::Write;
mod derived;
pub(crate) use derived::capture_import_adjacency;
mod compact_root;
pub(crate) use compact_root::publish_compact_import_root;
mod composition_control;
pub(crate) use composition_control::{
    CapturedCompositionControl, persist_composition_authority, persist_staged_composition,
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
    ) -> Result<CapturedPortableSource<'_>, PortableError> {
        let parent =
            graphforge_filesystem::StableDirectory::open(path.parent().ok_or_else(|| {
                PortableError::new(PortableErrorCode::InvalidPath, "captured source parent")
            })?)
            .map_err(|_| {
                PortableError::new(
                    PortableErrorCode::ConcurrentMutation,
                    "captured source directory changed",
                )
            })?;
        let name = path
            .file_name()
            .ok_or_else(|| {
                PortableError::new(PortableErrorCode::InvalidPath, "captured source name")
            })?
            .to_os_string();
        let file = parent.open_child_file(&name).map_err(|_| {
            PortableError::new(
                PortableErrorCode::ConcurrentMutation,
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
            PortableError::new(
                PortableErrorCode::ConcurrentMutation,
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
    ) -> Result<[u8; 32], PortableError> {
        let mut file = crate::project_portable_export::open_source_no_follow(path)?;
        if graphforge_filesystem::file_identity(&file)
            .map_err(|_| PortableError::new(PortableErrorCode::Io, "materialized file identity"))?
            != self.identity
            || file
                .metadata()
                .map_err(|_| {
                    PortableError::new(PortableErrorCode::Io, "materialized file metadata")
                })?
                .len()
                != self.length
        {
            return Err(PortableError::new(
                PortableErrorCode::ConcurrentMutation,
                "materialized file changed",
            ));
        }
        let mut checksum = crate::corruption_checksum::Checksum::new();
        let mut bytes = 0_u64;
        let mut buffer = vec![0; 64 * 1024];
        let bound = self.length.checked_add(1).ok_or_else(|| {
            PortableError::new(
                PortableErrorCode::LimitExceeded,
                "materialized length overflow",
            )
        })?;
        let mut reader = (&mut file).take(bound);
        loop {
            check_cancel(cancelled)?;
            let count = reader.read(&mut buffer).map_err(|_| {
                PortableError::new(PortableErrorCode::Io, "cannot read materialized file")
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
                PortableError::new(
                    PortableErrorCode::ConcurrentMutation,
                    "materialized path changed",
                )
            })? != self.identity
        {
            return Err(PortableError::new(
                PortableErrorCode::ConcurrentMutation,
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
pub fn materialize_verified_portable(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
) -> Result<PortableReport, PortableError> {
    materialize_verified_portable_observed(
        source,
        destination,
        limits,
        cancelled,
        |_, _| Ok(()),
        false,
    )
    .map(|materialized| materialized.report)
}

pub(crate) fn materialize_verified_portable_observed(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    mut observed: impl FnMut(&Path, Option<&File>) -> Result<(), PortableError>,
    track_removals: bool,
) -> Result<VerifiedMaterialization, PortableError> {
    let destination = destination.as_ref();
    if destination.exists() {
        return Err(PortableError::new(
            PortableErrorCode::Io,
            "materialization destination exists",
        ));
    }
    // Admission happens before creation; no malformed/future package can allocate payload staging.
    preflight(source.as_ref(), limits, cancelled)?;
    fs::create_dir(destination)
        .map_err(|_| PortableError::new(PortableErrorCode::Io, "cannot create materialization"))?;
    let mut sink = CopySink::new(
        destination.to_owned(),
        COMPONENT_NAMESPACE,
        Some(&mut observed),
        track_removals,
    );
    let result = (|| {
        let report = scan(
            source.as_ref(),
            PortableMode::Full,
            limits,
            cancelled,
            Some(&mut sink),
            None,
        )?;
        // Refuse a successful write whose physical bytes changed before the
        // private directory becomes available. This is a checksum readback of
        // writer-owned output, not another authentication of the source.
        sink.authenticate_and_sync(cancelled)?;
        Ok(VerifiedMaterialization {
            report,
            application_read_bytes: sink.bytes,
            application_read_operations: sink.operations,
            captures: std::mem::take(&mut sink.captures),
        })
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(destination);
        for path in std::mem::take(&mut sink.routes) {
            if matches!(fs::symlink_metadata(&path), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
            {
                let _ = sink.observe(&path, None);
            }
        }
    }
    result
}

type EntryObserver<'a> = dyn FnMut(&Path, Option<&File>) -> Result<(), PortableError> + 'a;

/// Every component file an import stages.
const COMPONENT_NAMESPACE: &str = "data/components/";

/// Where a scan sends the bytes of the entries it stages, one entry at a time
/// in canonical order. The import's [`CopySink`] stages every component file; the
/// research driver stages one Version's files and validates them before the
/// next Version arrives.
pub(super) trait EntrySink {
    /// The directory every staged entry is copied under, when it is one stage.
    fn destination(&self) -> Option<&Path> {
        None
    }
    /// The file to copy an entry of `length` bytes into, or `None` to only authenticate it.
    fn open(&mut self, relative: &str, length: u64) -> Result<Option<File>, PortableError>;
    fn write(&mut self, relative: &str, file: &mut File, bytes: &[u8])
    -> Result<(), PortableError>;
    fn finish(
        &mut self,
        relative: &str,
        file: File,
        length: u64,
        digest: [u8; 32],
        checksum: u64,
    ) -> Result<(), PortableError>;
    /// An entry that `open` declined, with the digest the scan computed over it.
    fn declined(
        &mut self,
        _relative: &str,
        _length: u64,
        _digest: [u8; 32],
    ) -> Result<(), PortableError> {
        Ok(())
    }
    /// True when the sink validates research files as they stream, so a
    /// finished scan has nothing to read back.
    fn validates_research(&self) -> bool {
        false
    }
    /// True when the sink answers for the Parquet page profile of this entry
    /// itself (it walks its staged copy, or the entry's Version was refused), so
    /// the scan keeps no copy of it for the walk.
    fn owns_profile(&self, _relative: &str) -> bool {
        false
    }
}

impl EntrySink for CopySink<'_> {
    fn destination(&self) -> Option<&Path> {
        Some(CopySink::destination(self))
    }
    fn open(&mut self, relative: &str, _length: u64) -> Result<Option<File>, PortableError> {
        CopySink::open(self, relative)
    }
    fn write(
        &mut self,
        relative: &str,
        file: &mut File,
        bytes: &[u8],
    ) -> Result<(), PortableError> {
        CopySink::write(self, relative, file, bytes)
    }
    fn finish(
        &mut self,
        relative: &str,
        file: File,
        length: u64,
        digest: [u8; 32],
        checksum: u64,
    ) -> Result<(), PortableError> {
        CopySink::finish(self, relative, file, length, digest, checksum)
    }
}

pub(super) struct CopySink<'a> {
    destination: std::path::PathBuf,
    /// Only entries whose path starts with this prefix are copied.
    namespace: &'static str,
    /// A private directory created in this parent when the first entry is
    /// copied; `destination` is inside it. A sink without one writes to a
    /// `destination` its caller created.
    lazy_parent: Option<std::path::PathBuf>,
    owner: Option<tempfile::TempDir>,
    observed: Option<&'a mut EntryObserver<'a>>,
    routes: BTreeSet<std::path::PathBuf>,
    track_removals: bool,
    bytes: u64,
    operations: u64,
    captures: BTreeMap<String, MaterializedCapture>,
}
impl<'a> CopySink<'a> {
    pub(super) fn new(
        destination: std::path::PathBuf,
        namespace: &'static str,
        observed: Option<&'a mut EntryObserver<'a>>,
        track_removals: bool,
    ) -> Self {
        Self {
            destination,
            namespace,
            lazy_parent: None,
            owner: None,
            observed,
            routes: BTreeSet::new(),
            track_removals,
            bytes: 0,
            operations: 0,
            captures: BTreeMap::new(),
        }
    }
    /// A sink that creates nothing until it copies its first entry, then
    /// stages the entries under `namespace` in a private directory inside
    /// `parent`; dropping the sink removes it.
    pub(super) fn staged_in(parent: std::path::PathBuf, namespace: &'static str) -> Self {
        let mut sink = Self::new(std::path::PathBuf::new(), namespace, None, false);
        sink.lazy_parent = Some(parent);
        sink
    }
    pub(super) fn destination(&self) -> &Path {
        &self.destination
    }
    /// The stage, once an entry has been copied into a lazily created one.
    pub(super) fn staged(&self) -> Option<&Path> {
        (self.lazy_parent.is_none() || self.owner.is_some()).then_some(self.destination.as_path())
    }
    fn ensure_stage(&mut self, relative: &str) -> Result<(), PortableError> {
        let Some(parent) = &self.lazy_parent else {
            return Ok(());
        };
        if self.owner.is_none() {
            let owner = tempfile::Builder::new()
                .prefix(".graphforge-research-validation-")
                .tempdir_in(parent)
                .map_err(|error| {
                    PortableError::at(
                        PortableErrorCode::Io,
                        relative,
                        "cannot create research validation scratch",
                    )
                    .with_cause(error.kind().to_string())
                })?;
            self.destination = owner.path().join("verified");
            self.owner = Some(owner);
        }
        Ok(())
    }
    pub(super) fn captures(&self) -> &BTreeMap<String, MaterializedCapture> {
        &self.captures
    }
    fn observe(&mut self, path: &Path, file: Option<&File>) -> Result<(), PortableError> {
        match &mut self.observed {
            Some(observed) => observed(path, file),
            None => Ok(()),
        }
    }
    /// Read back every copied file against the checksum captured while it was
    /// written, then sync the staged tree.
    pub(super) fn authenticate_and_sync(
        &self,
        cancelled: Option<&AtomicBool>,
    ) -> Result<(), PortableError> {
        for (relative, capture) in &self.captures {
            capture.authenticate(&self.destination.join(relative), cancelled)?;
        }
        if self.staged().is_some() && self.destination.exists() {
            sync_materialized_tree(&self.destination)?;
        }
        Ok(())
    }
    pub(super) fn open(&mut self, relative: &str) -> Result<Option<File>, PortableError> {
        if !relative.starts_with(self.namespace) {
            return Ok(None);
        }
        self.ensure_stage(relative)?;
        let path = self.destination.join(relative);
        fs::create_dir_all(path.parent().expect("component parent")).map_err(|_| {
            PortableError::at(
                PortableErrorCode::Io,
                relative,
                "cannot create component parent",
            )
        })?;
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|_| {
                PortableError::at(PortableErrorCode::Io, relative, "cannot stage entry")
            })?;
        if self.track_removals {
            self.routes.insert(path.clone());
        }
        self.observe(&path, Some(&file))?;
        Ok(Some(file))
    }
    pub(super) fn write(
        &mut self,
        relative: &str,
        file: &mut File,
        bytes: &[u8],
    ) -> Result<(), PortableError> {
        let result = file
            .write_all(bytes)
            .map_err(|_| PortableError::at(PortableErrorCode::Io, relative, "cannot copy entry"));
        let refreshed = self.observe(&self.destination.join(relative), Some(file));
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
    ) -> Result<(), PortableError> {
        let result = crate::durable_commit::seal_file(&file)
            .map_err(|_| PortableError::at(PortableErrorCode::Io, relative, "cannot sync entry"));
        let refreshed = self.observe(&self.destination.join(relative), Some(&file));
        result?;
        refreshed?;
        let identity = graphforge_filesystem::file_identity(&file).map_err(|_| {
            PortableError::at(
                PortableErrorCode::Io,
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
                    PortableError::at(
                        PortableErrorCode::ConcurrentMutation,
                        relative,
                        "materialized directory changed",
                    )
                })?;
        let file = directory
            .open_child_file(path.file_name().expect("component name"))
            .map_err(|_| {
                PortableError::at(
                    PortableErrorCode::ConcurrentMutation,
                    relative,
                    "materialized file changed after writer close",
                )
            })?;
        directory.revalidate_named().map_err(|_| {
            PortableError::at(
                PortableErrorCode::ConcurrentMutation,
                relative,
                "materialized directory changed",
            )
        })?;
        let metadata = file.metadata().map_err(|_| {
            PortableError::at(PortableErrorCode::Io, relative, "materialized metadata")
        })?;
        if !metadata.is_file()
            || metadata.len() != length
            || graphforge_filesystem::file_identity(&file).ok() != Some(identity)
            || graphforge_filesystem::file_link_count(&file).ok() != Some(1)
        {
            return Err(PortableError::at(
                PortableErrorCode::ConcurrentMutation,
                relative,
                "materialized file changed after writer close",
            ));
        }
        self.observe(&path, Some(&file))?;
        let allocated_bytes = graphforge_filesystem::file_space_usage(&file)
            .map_err(|_| {
                PortableError::at(
                    PortableErrorCode::Io,
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

fn sync_materialized_tree(root: &Path) -> Result<(), PortableError> {
    let mut directories = vec![root.to_owned()];
    let mut index = 0;
    while index < directories.len() {
        for entry in fs::read_dir(&directories[index]).map_err(|_| {
            PortableError::new(PortableErrorCode::Io, "cannot read staged directory")
        })? {
            let entry = entry.map_err(|_| {
                PortableError::new(PortableErrorCode::Io, "cannot read staged entry")
            })?;
            if entry
                .file_type()
                .map_err(|_| {
                    PortableError::new(PortableErrorCode::Io, "cannot inspect staged entry")
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
            PortableError::new(PortableErrorCode::Io, "cannot sync staged directory")
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
