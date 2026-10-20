//! Research validation one Version at a time, driven by the package scan.
//!
//! A research package stores its registry first and then each Version's
//! retained objects in one contiguous run (`research-retained/<version>/`), so
//! the scan can stage a Version, validate it with the native owners and remove
//! it before the next Version is read. Temporary disk is bounded by one Version
//! (`max_research_version_bytes`), not by the package. The same driver serves a
//! bundle file, an expanded directory and a sequential stream, so the verdict
//! never depends on how the bytes arrive.
use super::{
    NAMESPACE, check_version, evidence_only, invalid, provided_by_report, registry_path,
    retained_key,
};
use crate::project_portable::{
    PortableError, PortableErrorCode, PortableLimits, PortableMode, PortableReport, PortableSource,
    authenticated_entries,
    authenticated_entries::Reopen,
    check_cancel, finish_bundle,
    materialization::{CopySink, EntrySink},
    preflight, read_bounded_file, read_bundle, scan_entries,
};
use crate::project_portable_import::NativeResearchValidator;
use crate::research_versions::{
    MAX_REGISTRY_BYTES, ResearchRegistry, ResearchVersionRecord, interchange::portable,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::AtomicBool,
};
use uuid::Uuid;

/// The Version whose objects are being staged.
struct VersionStage {
    id: Uuid,
    /// Stages `<scratch>/.graphforge-research-validation-*/verified`; dropping it removes everything.
    sink: CopySink<'static>,
    /// Staged bytes, which `max_research_version_bytes` bounds.
    bytes: u64,
    /// The objects that were staged, by owning Version and digest.
    staged: BTreeMap<(Uuid, String), u64>,
}

pub(crate) struct Driver<'a, 'v> {
    scratch: PathBuf,
    limits: PortableLimits,
    cancelled: Option<&'a AtomicBool>,
    /// The native owner, or `None` to check only each Version's closure.
    validator: Option<&'a mut NativeResearchValidator<'v>>,
    registry: Option<ResearchRegistry>,
    current: Option<VersionStage>,
    /// Every retained object seen so far, staged or not, with its length.
    seen: BTreeMap<(Uuid, String), u64>,
    finished: BTreeSet<Uuid>,
}

enum Slot {
    Registry,
    Retained(Uuid, String),
    Other,
}

impl<'a, 'v> Driver<'a, 'v> {
    pub(crate) fn new(
        scratch: &Path,
        limits: PortableLimits,
        cancelled: Option<&'a AtomicBool>,
        validator: Option<&'a mut NativeResearchValidator<'v>>,
    ) -> Self {
        Self {
            scratch: scratch.to_path_buf(),
            limits,
            cancelled,
            validator,
            registry: None,
            current: None,
            seen: BTreeMap::new(),
            finished: BTreeSet::new(),
        }
    }

    fn slot(relative: &str) -> Slot {
        if let Some((version, digest)) = retained_key(relative) {
            Slot::Retained(version, digest)
        } else if relative == format!("{NAMESPACE}{}/participant.json", super::registry_id()) {
            Slot::Registry
        } else {
            Slot::Other
        }
    }

    /// Validate the Version being staged against its own objects, then remove it.
    fn finish_version(&mut self) -> Result<(), PortableError> {
        let Some(stage) = self.current.take() else {
            return Ok(());
        };
        check_cancel(self.cancelled)?;
        let registry = self.registry.as_ref().ok_or_else(invalid)?;
        let version = registry.versions.get(&stage.id).ok_or_else(invalid)?;
        let staged_dir = stage.sink.staged().map(Path::to_path_buf);
        stage.sink.authenticate_and_sync(self.cancelled)?;
        check_version(
            &stage.id,
            version,
            &self.seen,
            self.limits,
            self.cancelled,
            &mut |path, bound| match &staged_dir {
                Some(dir) => read_bounded_file(&dir.join(path), bound, path),
                None => Err(invalid()),
            },
        )?;
        if let (Some(validator), Some(dir)) = (self.validator.as_deref_mut(), &staged_dir) {
            crate::project_portable_import::validate_version(
                dir,
                registry,
                version,
                &stage.staged,
                stage.sink.captures(),
                self.cancelled,
                validator,
            )?;
        }
        self.finished.insert(stage.id);
        Ok(())
    }

    /// The end of the scan, once the manifest is authenticated: the objects seen
    /// are exactly the ones the manifest names, and every Version was validated.
    pub(crate) fn finish(
        &mut self,
        report: &graphforge_core::portable::PortableReport,
    ) -> Result<(), PortableError> {
        self.finish_version()?;
        if !report.research_interchange {
            return if self.registry.is_none() && self.seen.is_empty() {
                Ok(())
            } else {
                Err(invalid())
            };
        }
        registry_path(report)?;
        let registry = self.registry.as_ref().ok_or_else(invalid)?;
        if provided_by_report(report, registry)? != self.seen
            || self.finished.len() != registry.versions.len()
        {
            return Err(invalid());
        }
        check_cancel(self.cancelled)
    }

    fn read_registry(&mut self, mut file: File) -> Result<(), PortableError> {
        file.seek(SeekFrom::Start(0)).map_err(|_| {
            PortableError::new(PortableErrorCode::Io, "cannot read research registry")
        })?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|_| {
            PortableError::new(PortableErrorCode::Io, "cannot read research registry")
        })?;
        self.registry = Some(portable::portable_registry(&bytes).map_err(|_| invalid())?);
        Ok(())
    }

    fn start_version(&mut self, id: Uuid) -> Result<(), PortableError> {
        if self.current.as_ref().is_some_and(|stage| stage.id == id) {
            return Ok(());
        }
        self.finish_version()?;
        // Canonical order keeps a Version's objects together; one that returns
        // after another began is a layout this reader does not accept.
        if self.finished.contains(&id) || self.finished.last().is_some_and(|last| *last > id) {
            return Err(invalid());
        }
        self.current = Some(VersionStage {
            id,
            sink: CopySink::staged_in(self.scratch.clone(), NAMESPACE),
            bytes: 0,
            staged: BTreeMap::new(),
        });
        Ok(())
    }

    fn version(&self, id: &Uuid) -> Result<&ResearchVersionRecord, PortableError> {
        self.registry
            .as_ref()
            .and_then(|registry| registry.versions.get(id))
            .ok_or_else(invalid)
    }
}

/// One pass over a bundle read from a sequential reader. A research package's
/// Versions are validated by `validator` as they stream past, or only checked
/// against their closures when there is none.
pub(crate) fn scan_stream(
    source: &mut dyn Read,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    scratch: Option<&Path>,
    validator: Option<&mut crate::project_portable_import::NativeResearchValidator<'_>>,
) -> Result<PortableReport, PortableError> {
    check_cancel(cancelled)?;
    let scratch = scratch.map_or_else(std::env::temp_dir, Path::to_path_buf);
    let mut research = Driver::new(&scratch, limits, cancelled, validator);
    let scan = read_bundle(
        source,
        limits,
        cancelled,
        Some(&mut research),
        None,
        Some(&scratch),
        || PortableError::new(PortableErrorCode::InvalidStructure, "trailing bytes"),
    )?;
    let (report, entries) =
        finish_bundle(scan, PortableMode::Full, limits, PortableSource::Stream)?;
    authenticated_entries::validate_semantics(
        Reopen::Driven,
        &entries,
        &report,
        limits,
        cancelled,
    )?;
    research.finish(&report)?;
    Ok(report)
}

/// [`scan_stream`] for a bundle file or an expanded directory: the same driver
/// validates each research Version as the scan reaches it.
pub(crate) fn validate_research_path(
    source: &Path,
    scratch: Option<&Path>,
    limits: PortableLimits,
    cancelled: Option<&AtomicBool>,
    validator: Option<&mut crate::project_portable_import::NativeResearchValidator<'_>>,
) -> Result<PortableReport, PortableError> {
    preflight(source, limits, cancelled)?;
    let scratch = scratch.map_or_else(std::env::temp_dir, Path::to_path_buf);
    let mut research = Driver::new(&scratch, limits, cancelled, validator);
    let (report, _) = scan_entries(
        source,
        PortableMode::Full,
        limits,
        cancelled,
        Some(&mut research),
        None,
    )?;
    research.finish(&report)?;
    Ok(report)
}

impl EntrySink for Driver<'_, '_> {
    fn validates_research(&self) -> bool {
        true
    }

    fn open(&mut self, relative: &str, length: u64) -> Result<Option<File>, PortableError> {
        check_cancel(self.cancelled)?;
        match Self::slot(relative) {
            Slot::Registry => {
                if self.registry.is_some() || length > MAX_REGISTRY_BYTES as u64 {
                    return Err(invalid());
                }
                // The registry is a small control document the scan reads back
                // at once: an unnamed file, never a staged path.
                tempfile::tempfile_in(&self.scratch)
                    .map(Some)
                    .map_err(|error| {
                        PortableError::at(
                            PortableErrorCode::Io,
                            relative,
                            "cannot create research validation scratch",
                        )
                        .with_cause(error.kind().to_string())
                    })
            }
            Slot::Retained(id, digest) => {
                if self.registry.is_none() {
                    return Err(PortableError::at(
                        PortableErrorCode::InvalidStructure,
                        relative,
                        "research content precedes the registry",
                    ));
                }
                self.version(&id)?;
                self.start_version(id)?;
                if evidence_only(self.version(&id)?).contains(&digest) {
                    return Ok(None);
                }
                let limit = self.limits.max_research_version_bytes;
                let stage = self.current.as_mut().expect("Version stage was started");
                stage.bytes = stage.bytes.saturating_add(length);
                if stage.bytes > limit {
                    return Err(PortableError::at(
                        PortableErrorCode::LimitExceeded,
                        relative,
                        "research version bytes",
                    ));
                }
                stage.sink.open(relative)
            }
            Slot::Other => {
                // The research run ended; validate its last Version before the scan goes on.
                self.finish_version()?;
                Ok(None)
            }
        }
    }

    fn write(
        &mut self,
        relative: &str,
        file: &mut File,
        bytes: &[u8],
    ) -> Result<(), PortableError> {
        if matches!(Self::slot(relative), Slot::Registry) {
            return file.write_all(bytes).map_err(|_| {
                PortableError::at(PortableErrorCode::Io, relative, "cannot copy entry")
            });
        }
        let stage = self.current.as_mut().expect("Version stage was started");
        stage.sink.write(relative, file, bytes)
    }

    fn finish(
        &mut self,
        relative: &str,
        file: File,
        length: u64,
        digest: [u8; 32],
        checksum: u64,
    ) -> Result<(), PortableError> {
        match Self::slot(relative) {
            Slot::Registry => self.read_registry(file),
            Slot::Retained(id, hex) => {
                authenticate(relative, &hex, digest)?;
                let stage = self.current.as_mut().expect("Version stage was started");
                stage
                    .sink
                    .finish(relative, file, length, digest, checksum)?;
                stage.staged.insert((id, hex.clone()), length);
                self.seen.insert((id, hex), length);
                Ok(())
            }
            Slot::Other => Ok(()),
        }
    }

    fn declined(
        &mut self,
        relative: &str,
        length: u64,
        digest: [u8; 32],
    ) -> Result<(), PortableError> {
        if let Slot::Retained(id, hex) = Self::slot(relative) {
            authenticate(relative, &hex, digest)?;
            self.seen.insert((id, hex), length);
        }
        Ok(())
    }
}

/// An object's path names its SHA-256; bytes that hash to anything else
/// never reach a native owner.
fn authenticate(relative: &str, hex: &str, digest: [u8; 32]) -> Result<(), PortableError> {
    if crate::project_portable::hex(&digest) != hex {
        return Err(PortableError::at(
            PortableErrorCode::DigestMismatch,
            relative,
            "research object digest differs from its path",
        ));
    }
    Ok(())
}
