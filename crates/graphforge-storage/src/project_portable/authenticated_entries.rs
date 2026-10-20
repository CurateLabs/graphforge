//! Bounded semantic reads tied to the entries authenticated by the package scanner.

use super::{
    AtomicBool, ControlSha256, Digest, Entry, Path, PortableError, PortableErrorCode,
    PortableLimits, PortableReport, Read, Seek, SeekFrom, Sha256, canonical_json, check_cancel, fs,
    has_multiple_links, hex, modified, research, same_identity, semantic_validation,
};

/// Where a semantic read finds an entry whose bytes the scan did not retain.
/// Nothing is ever read back from the bundle being scanned.
#[derive(Clone, Copy)]
pub(super) enum Reopen<'a> {
    /// An expanded package: every entry is a file under this directory.
    Directory(&'a Path),
    /// A bundle: only the research files, which the scan copied to this stage.
    Bundle(Option<&'a Path>),
    /// A bundle file on disk: the research files, read back at the offsets the
    /// scan recorded. Validation reads them after the scan, as it always has,
    /// so a local file needs no staging directory.
    BundleFile(&'a Path),
    /// A bundle file served by [`super::PortablePackageIndex`]: random-access
    /// reads of an already scanned package at the offsets the scan recorded.
    /// Verification never uses this.
    Indexed(&'a Path),
}

/// Authentication for untrusted bytes, or corruption refusal against evidence
/// privately captured while this process wrote the exact canonical output.
pub(super) enum StreamHash {
    Portable(Sha256),
    Written {
        checksum: crate::corruption_checksum::Checksum,
        digest: [u8; 32],
        expected: u64,
    },
}
impl StreamHash {
    pub(super) fn new(expected: Option<([u8; 32], u64)>) -> Self {
        expected.map_or_else(
            || Self::Portable(Sha256::new()),
            |(digest, expected)| Self::Written {
                checksum: crate::corruption_checksum::Checksum::new(),
                digest,
                expected,
            },
        )
    }
    pub(super) fn update(&mut self, bytes: impl AsRef<[u8]>) {
        match self {
            Self::Portable(hash) => hash.update(bytes.as_ref()),
            Self::Written { checksum, .. } => checksum.update(bytes.as_ref()),
        }
    }
    pub(super) fn finish(self) -> Result<[u8; 32], PortableError> {
        match self {
            Self::Portable(hash) => Ok(hash.finalize().into()),
            Self::Written {
                checksum,
                digest,
                expected,
            } if checksum.finish() == expected => Ok(digest),
            Self::Written { .. } => Err(PortableError::new(
                PortableErrorCode::DigestMismatch,
                "written output checksum changed",
            )),
        }
    }
}

pub(super) fn validate_semantics(
    source: Reopen<'_>,
    entries: &[Entry],
    report: &PortableReport,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableError> {
    validate_saved_queries(source, entries, limits, cancelled)?;
    semantic_validation::validate_with_reader(report, limits, cancelled, |descriptor| {
        let bytes = read(
            source,
            entries,
            &descriptor.path,
            limits.max_manifest_bytes,
            cancelled,
        )?;
        let value = serde_json::from_slice(&bytes).map_err(|_| {
            PortableError::at(
                PortableErrorCode::InvalidStructure,
                &descriptor.path,
                "semantic payload JSON",
            )
        })?;
        if canonical_json(&value)? != bytes {
            return Err(PortableError::at(
                PortableErrorCode::Incompatible,
                &descriptor.path,
                "semantic payload is noncanonical",
            ));
        }
        Ok(value)
    })?;
    research::validate_with_reader(report, limits, cancelled, |path, bound| {
        read(source, entries, path, bound, cancelled)
    })?;
    Ok(())
}

pub(super) fn read(
    source: Reopen<'_>,
    entries: &[Entry],
    path: &str,
    bound: u64,
    cancelled: Option<&AtomicBool>,
) -> Result<Vec<u8>, PortableError> {
    check_cancel(cancelled)?;
    let entry = entries
        .iter()
        .find(|entry| entry.path == path)
        .ok_or_else(|| {
            PortableError::at(
                PortableErrorCode::InvalidStructure,
                path,
                "semantic entry is not authenticated",
            )
        })?;
    if entry.length > bound {
        return Err(PortableError::at(
            PortableErrorCode::LimitExceeded,
            path,
            "semantic entry bound",
        ));
    }
    if let Some(bytes) = &entry.bytes {
        return Ok(bytes.clone());
    }
    let input_path = locate(source, path)?;
    let mut file =
        crate::project_portable_export::open_source_no_follow(&input_path).map_err(|_| {
            PortableError::at(
                PortableErrorCode::ConcurrentMutation,
                path,
                "cannot reopen authenticated control",
            )
        })?;
    let before = file
        .metadata()
        .map_err(|_| PortableError::at(PortableErrorCode::Io, path, "cannot inspect control"))?;
    if has_multiple_links(&before) {
        return Err(PortableError::at(
            PortableErrorCode::InvalidStructure,
            path,
            "hard-linked control",
        ));
    }
    if let (Reopen::Indexed(_) | Reopen::BundleFile(_), Some(offset)) = (source, entry.offset) {
        file.seek(SeekFrom::Start(offset))
            .map_err(|_| PortableError::at(PortableErrorCode::Io, path, "cannot seek control"))?;
    } else if before.len() != entry.length {
        return Err(PortableError::at(
            PortableErrorCode::ConcurrentMutation,
            path,
            "control length changed",
        ));
    }
    let length = usize::try_from(entry.length).map_err(|_| {
        PortableError::at(PortableErrorCode::LimitExceeded, path, "control allocation")
    })?;
    let mut bytes = vec![0; length];
    file.read_exact(&mut bytes).map_err(|_| {
        PortableError::at(
            PortableErrorCode::ConcurrentMutation,
            path,
            "control truncated",
        )
    })?;
    check_cancel(cancelled)?;
    let after = file
        .metadata()
        .map_err(|_| PortableError::at(PortableErrorCode::Io, path, "cannot inspect control"))?;
    let named = fs::symlink_metadata(&input_path).map_err(|_| {
        PortableError::at(
            PortableErrorCode::ConcurrentMutation,
            path,
            "control disappeared",
        )
    })?;
    if !same_identity(&before, &after)
        || !same_identity(&after, &named)
        || before.len() != after.len()
        || modified(&before) != modified(&after)
        || <[u8; 32]>::from(ControlSha256::digest(&bytes)) != entry.digest
    {
        return Err(PortableError::at(
            PortableErrorCode::ConcurrentMutation,
            path,
            "authenticated control changed",
        ));
    }
    Ok(bytes)
}

/// The file that holds `path` when the scan did not retain its bytes.
fn locate(source: Reopen<'_>, path: &str) -> Result<std::path::PathBuf, PortableError> {
    Ok(match source {
        Reopen::Directory(root) => root.join(path),
        Reopen::Indexed(bundle) => bundle.to_path_buf(),
        Reopen::BundleFile(bundle) if path.starts_with(research::NAMESPACE) => bundle.to_path_buf(),
        Reopen::Bundle(Some(stage)) if path.starts_with(research::NAMESPACE) => stage.join(path),
        Reopen::Bundle(None) if path.starts_with(research::NAMESPACE) => {
            return Err(PortableError::at(
                PortableErrorCode::Io,
                path,
                "research file was not staged",
            ));
        }
        Reopen::Bundle(_) | Reopen::BundleFile(_) => {
            return Err(PortableError::at(
                PortableErrorCode::LimitExceeded,
                path,
                "retained control budget",
            ));
        }
    })
}

/// A byte range of an authenticated entry, served to the Parquet page walk.
struct EntryWindow {
    file: fs::File,
    base: u64,
    length: u64,
}

impl parquet::file::reader::Length for EntryWindow {
    fn len(&self) -> u64 {
        self.length
    }
}

impl parquet::file::reader::ChunkReader for EntryWindow {
    type T = std::io::Cursor<bytes::Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let length = usize::try_from(self.length.saturating_sub(start))
            .map_err(|_| parquet::errors::ParquetError::General("entry window overflows".into()))?;
        Ok(std::io::Cursor::new(self.get_bytes(start, length)?))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        start
            .checked_add(length as u64)
            .filter(|end| *end <= self.length)
            .ok_or_else(|| {
                parquet::errors::ParquetError::General("read escapes the entry window".into())
            })?;
        let mut file = &self.file;
        file.seek(SeekFrom::Start(self.base + start))?;
        let mut buffer = vec![0_u8; length];
        file.read_exact(&mut buffer)?;
        Ok(buffer.into())
    }
}

/// What the Parquet page walk needs of one streamed entry. A stream cannot be
/// read back, so the entry is held in memory while it streams past, up to
/// `max_parquet_walk_bytes`, together with its last four bytes. If it ends in
/// the footer magic it is walked exactly as a file entry is; if it is larger
/// than the bound it is refused rather than left unwalked.
pub(super) struct ParquetWalk {
    kept: Option<Vec<u8>>,
    tail: [u8; 4],
}

impl ParquetWalk {
    pub(super) fn new(size: u64, limit: u64) -> Self {
        Self {
            kept: (size <= limit).then(|| {
                Vec::with_capacity(usize::try_from(size).unwrap_or(usize::MAX).min(1 << 30))
            }),
            tail: [0; 4],
        }
    }
    pub(super) fn update(&mut self, chunk: &[u8]) {
        if let Some(kept) = &mut self.kept {
            kept.extend_from_slice(chunk);
        }
        if chunk.len() >= 4 {
            self.tail.copy_from_slice(&chunk[chunk.len() - 4..]);
        } else {
            self.tail.rotate_left(chunk.len());
            let at = 4 - chunk.len();
            self.tail[at..].copy_from_slice(chunk);
        }
    }
    pub(super) fn finish(self, path: &str) -> Result<(), PortableError> {
        if &self.tail != b"PAR1" {
            return Ok(());
        }
        let Some(bytes) = self.kept else {
            return Err(PortableError::at(
                PortableErrorCode::LimitExceeded,
                path,
                "parquet entry exceeds the stream page-walk bound",
            ));
        };
        crate::parquet_encoding_profile::admit_pages(&bytes::Bytes::from(bytes))
            .map_err(|error| refuse_profile(path, error))
    }
}

/// The refusal for a Parquet entry the funded decoder profile does not admit.
pub(super) fn refuse_profile(
    path: &str,
    error: crate::parquet_encoding_profile::PageAdmissionError,
) -> PortableError {
    use crate::parquet_encoding_profile::PageAdmissionError;
    match error {
        PageAdmissionError::Unfunded => PortableError::at(
            PortableErrorCode::LimitExceeded,
            path,
            "parquet page encoding is outside the funded decoder profile",
        ),
        PageAdmissionError::Malformed(message) => {
            PortableError::at(PortableErrorCode::InvalidStructure, path, message)
        }
    }
}

/// Refuse any Parquet component entry whose actual pages use an encoding outside
/// the funded profile. This is the trust boundary for external Parquet: portable
/// verification and import both pass here before any project decoder sees a byte.
///
/// An entry is Parquet when it ends in the `PAR1` footer magic, which is all the
/// Parquet reader requires; paths and declared encodings are not trusted.
pub(super) fn validate_parquet_profile(
    source: &Path,
    entries: &[Entry],
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableError> {
    use crate::parquet_encoding_profile::admit_pages;

    for entry in entries
        .iter()
        .filter(|entry| entry.path.starts_with("data/components/") && entry.length >= 12)
    {
        check_cancel(cancelled)?;
        if let Some(bytes) = &entry.bytes {
            if bytes.ends_with(b"PAR1") {
                let bytes = bytes::Bytes::copy_from_slice(bytes);
                admit_pages(&bytes).map_err(|error| refuse_profile(&entry.path, error))?;
            }
            continue;
        }
        let input_path = if entry.offset.is_some() {
            source.to_path_buf()
        } else {
            source.join(&entry.path)
        };
        let mut file =
            crate::project_portable_export::open_source_no_follow(&input_path).map_err(|_| {
                PortableError::at(
                    PortableErrorCode::ConcurrentMutation,
                    &entry.path,
                    "cannot reopen authenticated entry",
                )
            })?;
        let before = file.metadata().map_err(|_| {
            PortableError::at(PortableErrorCode::Io, &entry.path, "cannot inspect entry")
        })?;
        let window = EntryWindow {
            file: file.try_clone().map_err(|_| {
                PortableError::at(PortableErrorCode::Io, &entry.path, "cannot read entry")
            })?,
            base: entry.offset.unwrap_or(0),
            length: entry.length,
        };
        if entry.offset.is_none() && before.len() != entry.length {
            return Err(PortableError::at(
                PortableErrorCode::ConcurrentMutation,
                &entry.path,
                "entry length changed",
            ));
        }
        let mut tail = [0_u8; 4];
        file.seek(SeekFrom::Start(window.base + entry.length - 4))
            .and_then(|_| file.read_exact(&mut tail))
            .map_err(|_| {
                PortableError::at(
                    PortableErrorCode::ConcurrentMutation,
                    &entry.path,
                    "entry truncated",
                )
            })?;
        if &tail == b"PAR1" {
            admit_pages(&window).map_err(|error| refuse_profile(&entry.path, error))?;
        }
        let after = file.metadata().map_err(|_| {
            PortableError::at(PortableErrorCode::Io, &entry.path, "cannot inspect entry")
        })?;
        if !same_identity(&before, &after)
            || before.len() != after.len()
            || modified(&before) != modified(&after)
        {
            return Err(PortableError::at(
                PortableErrorCode::ConcurrentMutation,
                &entry.path,
                "authenticated entry changed",
            ));
        }
    }
    Ok(())
}

/// Native admission uses authenticated runtime identities rather than payload paths.
fn validate_saved_queries(
    source: Reopen<'_>,
    entries: &[Entry],
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableError> {
    if !entries
        .iter()
        .any(|entry| entry.path == super::RUNTIME_MAP_PATH)
    {
        return Ok(());
    }
    let map_bytes = read(
        source,
        entries,
        super::RUNTIME_MAP_PATH,
        limits.max_manifest_bytes,
        cancelled,
    )?;
    let (_, runtime) = super::decode_runtime_map(&map_bytes)?;
    let mut matching = runtime.participants.iter().filter(|participant| {
        participant.capability_id == crate::WORKSPACE_CAPABILITY_ID
            && participant.record_family_id == crate::WORKSPACE_SAVED_QUERIES_FAMILY
    });
    let Some(participant) = matching.next() else {
        return Ok(());
    };
    let refuse = || {
        PortableError::new(
            PortableErrorCode::Incompatible,
            "invalid native saved-query definitions",
        )
    };
    if matching.next().is_some()
        || participant.capability_version != crate::WORKSPACE_CAPABILITY_VERSION
        || participant.record_version != crate::WORKSPACE_SAVED_QUERIES_VERSION
        || participant.encoding != "json"
        || participant.schema_fingerprint
            != hex(&crate::workspace_saved_queries::schema_fingerprint())
    {
        return Err(refuse());
    }
    let manifest_bytes = read(
        source,
        entries,
        super::MANIFEST_PATH,
        limits.semantic_manifest_bound(entries.len() as u64),
        cancelled,
    )?;
    let (manifest, _) = super::parse_manifest(&manifest_bytes, limits)?;
    let component = manifest
        .components
        .iter()
        .find(|component| component.participant_id == participant.participant_id)
        .ok_or_else(refuse)?;
    let [file] = component.files.as_slice() else {
        return Err(refuse());
    };
    if component.kind != "settings" {
        return Err(refuse());
    }
    let bytes = read(
        source,
        entries,
        &file.path,
        crate::MAX_WORKSPACE_SAVED_QUERIES_BYTES as u64,
        cancelled,
    )?;
    let record = crate::WorkspaceSavedQueries::from_canonical_json(&bytes).map_err(|_| refuse())?;
    if participant.row_count != record.queries.len() as u64 {
        return Err(refuse());
    }
    Ok(())
}
