//! Bounded semantic reads tied to the entries authenticated by the package scanner.

use super::{
    AtomicBool, ControlSha256, Digest, Entry, Path, PortableV2Error, PortableV2ErrorCode,
    PortableV2Limits, PortableV2Report, Read, Seek, SeekFrom, Sha256, canonical_json, check_cancel,
    fs, has_multiple_links, modified, research, same_identity, semantic_validation,
};

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
    pub(super) fn finish(self) -> Result<[u8; 32], PortableV2Error> {
        match self {
            Self::Portable(hash) => Ok(hash.finalize().into()),
            Self::Written {
                checksum,
                digest,
                expected,
            } if checksum.finish() == expected => Ok(digest),
            Self::Written { .. } => Err(PortableV2Error::new(
                PortableV2ErrorCode::DigestMismatch,
                "written output checksum changed",
            )),
        }
    }
}

pub(super) fn validate_semantics(
    source: &Path,
    entries: &[Entry],
    report: &PortableV2Report,
    limits: PortableV2Limits,
    cancelled: Option<&AtomicBool>,
) -> Result<(), PortableV2Error> {
    semantic_validation::validate_with_reader(report, limits, cancelled, |descriptor| {
        let bytes = read(
            source,
            entries,
            &descriptor.path,
            limits.max_manifest_bytes,
            cancelled,
        )?;
        let value = serde_json::from_slice(&bytes).map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                &descriptor.path,
                "semantic payload JSON",
            )
        })?;
        if canonical_json(&value)? != bytes {
            return Err(PortableV2Error::at(
                PortableV2ErrorCode::Incompatible,
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

fn read(
    source: &Path,
    entries: &[Entry],
    path: &str,
    bound: u64,
    cancelled: Option<&AtomicBool>,
) -> Result<Vec<u8>, PortableV2Error> {
    check_cancel(cancelled)?;
    let entry = entries
        .iter()
        .find(|entry| entry.path == path)
        .ok_or_else(|| {
            PortableV2Error::at(
                PortableV2ErrorCode::InvalidStructure,
                path,
                "semantic entry is not authenticated",
            )
        })?;
    if entry.length > bound {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::LimitExceeded,
            path,
            "semantic entry bound",
        ));
    }
    if let Some(bytes) = &entry.bytes {
        return Ok(bytes.clone());
    }
    let input_path = if entry.offset.is_some() {
        source.to_path_buf()
    } else {
        source.join(path)
    };
    let mut file =
        crate::project_portable_v2_export::open_source_no_follow(&input_path).map_err(|_| {
            PortableV2Error::at(
                PortableV2ErrorCode::ConcurrentMutation,
                path,
                "cannot reopen authenticated control",
            )
        })?;
    let before = file.metadata().map_err(|_| {
        PortableV2Error::at(PortableV2ErrorCode::Io, path, "cannot inspect control")
    })?;
    if has_multiple_links(&before) {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::InvalidStructure,
            path,
            "hard-linked control",
        ));
    }
    if let Some(offset) = entry.offset {
        file.seek(SeekFrom::Start(offset)).map_err(|_| {
            PortableV2Error::at(PortableV2ErrorCode::Io, path, "cannot seek control")
        })?;
    } else if before.len() != entry.length {
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::ConcurrentMutation,
            path,
            "control length changed",
        ));
    }
    let length = usize::try_from(entry.length).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::LimitExceeded,
            path,
            "control allocation",
        )
    })?;
    let mut bytes = vec![0; length];
    file.read_exact(&mut bytes).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::ConcurrentMutation,
            path,
            "control truncated",
        )
    })?;
    check_cancel(cancelled)?;
    let after = file.metadata().map_err(|_| {
        PortableV2Error::at(PortableV2ErrorCode::Io, path, "cannot inspect control")
    })?;
    let named = fs::symlink_metadata(&input_path).map_err(|_| {
        PortableV2Error::at(
            PortableV2ErrorCode::ConcurrentMutation,
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
        return Err(PortableV2Error::at(
            PortableV2ErrorCode::ConcurrentMutation,
            path,
            "authenticated control changed",
        ));
    }
    Ok(bytes)
}
