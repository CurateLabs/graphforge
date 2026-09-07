//! Local portable package byte operations; no transport or credentials.
use graphforge_core::portable::{
    PortableV2Error, PortableV2ErrorCode, PortableV2Limits, PortableV2Report,
    PortableV2Representation,
};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Bound canonical uncompressed bundle bytes from verifier admission limits.
/// Each regular entry has one 512-byte header and at most 511 padding bytes.
/// At most one PAX header precedes it, whose payload is bounded by
/// `max_path_bytes + 32` in the verifier, plus its header and padding.
/// The verifier requires exactly two terminal blocks and rejects trailing bytes.
pub fn portable_bundle_byte_limit(limits: PortableV2Limits) -> Result<u64, PortableV2Error> {
    let overflow = || {
        PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
            "bundle byte limit overflow",
        )
    };
    let pax_payload = u64::try_from(limits.max_path_bytes)
        .map_err(|_| overflow())?
        .checked_add(32)
        .ok_or_else(overflow)?;
    let regular_framing = 512_u64 + 511;
    let pax_framing = 512_u64 + 511;
    pax_payload
        .checked_add(pax_framing)
        .and_then(|pax| pax.checked_add(regular_framing))
        .and_then(|per_entry| per_entry.checked_mul(limits.max_entries))
        .and_then(|framing| framing.checked_add(limits.max_total_bytes))
        .and_then(|bytes| bytes.checked_add(2 * 512))
        .ok_or_else(overflow)
}

/// Read a verified bundle with a finite byte bound, including concurrent growth.
pub fn read_package_bytes(
    path: &Path,
    report: &PortableV2Report,
    max_bytes: u64,
) -> Result<Vec<u8>, PortableV2Error> {
    if report.representation != PortableV2Representation::Bundle {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::Incompatible,
            "OCI publish currently requires a portable-v2 bundle representation",
        ));
    }
    let file = File::open(path)
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "failed to open package"))?;
    let mut bytes = Vec::new();
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| PortableV2Error::new(PortableV2ErrorCode::Io, "failed to read package"))?;
    if bytes.len() as u64 > max_bytes {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
            "package exceeds byte limit",
        ));
    }
    Ok(bytes)
}

/// Owns only the exclusively created local staging file. Dropping removes it.
#[derive(Debug)]
pub struct PortablePackageStage {
    path: PathBuf,
    published: bool,
}
impl PortablePackageStage {
    /// Copy a bounded byte source into a new staging file and synchronize it.
    /// A preexisting staging file is never owned or removed by this operation.
    pub fn create(
        path: &Path,
        source: &mut impl Read,
        max_bytes: u64,
    ) -> Result<Self, PortableV2Error> {
        let mut file = File::create_new(path).map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::Io, "failed to create staging file")
        })?;
        let stage = Self {
            path: path.to_owned(),
            published: false,
        };
        let mut remaining = max_bytes;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let read = source.read(&mut buffer).map_err(|_| {
                PortableV2Error::new(PortableV2ErrorCode::Io, "failed to read package")
            })?;
            if read == 0 {
                break;
            }
            remaining = remaining.checked_sub(read as u64).ok_or_else(|| {
                PortableV2Error::new(
                    PortableV2ErrorCode::LimitExceeded,
                    "package exceeds byte limit",
                )
            })?;
            file.write_all(&buffer[..read]).map_err(|_| {
                PortableV2Error::new(PortableV2ErrorCode::Io, "failed to write staging file")
            })?;
        }
        file.sync_all().map_err(|_| {
            PortableV2Error::new(PortableV2ErrorCode::Io, "failed to sync staging file")
        })?;
        Ok(stage)
    }
    /// The exclusively owned file to verify before publication.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// Atomically publish without replacing any concurrent destination.
    pub fn publish(mut self, destination: &Path) -> Result<(), PortableV2Error> {
        crate::project_portable_v2_export::publish_no_replace(&self.path, destination).map_err(
            |error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    PortableV2Error::new(
                        PortableV2ErrorCode::InvalidPath,
                        "destination already exists",
                    )
                } else {
                    PortableV2Error::new(
                        PortableV2ErrorCode::Io,
                        "failed to publish pulled package",
                    )
                }
            },
        )?;
        self.published = true;
        Ok(())
    }
}
impl Drop for PortablePackageStage {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_never_removes_an_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stage");
        fs::write(&path, b"other operation").unwrap();
        assert!(PortablePackageStage::create(&path, &mut &b"new"[..], 10).is_err());
        assert_eq!(fs::read(path).unwrap(), b"other operation");
    }

    #[test]
    fn stage_cleans_owned_file_on_abort_and_limit_failure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stage");
        let stage = PortablePackageStage::create(&path, &mut &b"new"[..], 10).unwrap();
        assert_eq!(fs::read(stage.path()).unwrap(), b"new");
        drop(stage);
        assert!(!path.exists());
        let error = PortablePackageStage::create(&path, &mut &b"too large"[..], 2).unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::LimitExceeded);
        assert!(!path.exists());
    }

    #[test]
    fn publication_never_clobbers_a_destination_created_after_staging() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stage");
        let destination = dir.path().join("published");
        let stage = PortablePackageStage::create(&path, &mut &b"new"[..], 10).unwrap();
        fs::write(&destination, b"concurrent owner").unwrap();
        let error = stage.publish(&destination).unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::InvalidPath);
        assert_eq!(fs::read(destination).unwrap(), b"concurrent owner");
        assert!(!path.exists());
    }
}
