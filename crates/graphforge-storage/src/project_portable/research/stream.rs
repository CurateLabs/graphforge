//! Research validation one Version at a time, driven by the package scan.
//!
//! A research package stores its registry first and then each Version's
//! retained objects in one contiguous run (`research-retained/<version>/`);
//! objects only evidence needs sit apart in `research-retained-evidence/`, so
//! the scan can stage a Version, validate it with the native owners and remove
//! it before the next Version is read. Temporary disk is bounded by one Version
//! (`max_research_version_bytes`), not by the package. The same driver serves a
//! bundle file, an expanded directory and a sequential stream, so the verdict
//! never depends on how the bytes arrive.
use super::{
    Held, NAMESPACE, Objects, check_version, invalid, object_key, provided_by_report, registry_path,
};
use crate::project_portable::{
    PortableError, PortableErrorCode, PortableLimits, PortableMode, PortableReport, PortableSource,
    authenticated_entries,
    authenticated_entries::Reopen,
    authenticated_entries::walk_staged,
    check_cancel, finish_bundle,
    materialization::{CopySink, EntrySink},
    preflight, read_bounded_file, read_bundle, scan_entries,
};
use crate::project_portable_import::NativeResearchValidator;
use crate::research_versions::{
    MAX_REGISTRY_BYTES, ResearchRegistry, ResearchVersionRecord, interchange::portable,
};
use std::{
    collections::BTreeSet,
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
    staged: Objects,
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
    seen: Objects,
    finished: BTreeSet<Uuid>,
    /// Versions whose first staged object has arrived.
    started: BTreeSet<Uuid>,
    /// The first refusal of a Version (its closure, a native owner, a limit). The
    /// scan goes on to authenticate the rest of the package without staging or
    /// validating anything more, and a failed authentication outranks this
    /// refusal, whichever way the bytes arrive.
    refusal: Option<PortableError>,
}

enum Slot {
    Registry,
    /// Staged under `research-retained/`.
    Retained(Uuid, String),
    /// Authenticated and not staged: only evidence needs it.
    Evidence(Uuid, String),
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
            seen: Objects::new(),
            finished: BTreeSet::new(),
            started: BTreeSet::new(),
            refusal: None,
        }
    }

    fn slot(relative: &str) -> Slot {
        if let Some((version, digest, evidence_only)) = object_key(relative) {
            if evidence_only {
                Slot::Evidence(version, digest)
            } else {
                Slot::Retained(version, digest)
            }
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
        if self.refusal.is_some() {
            return Ok(());
        }
        match self.validate_stage(&stage) {
            Ok(()) => {
                self.finished.insert(stage.id);
                Ok(())
            }
            Err(refusal) => self.defer(refusal),
        }
    }

    /// Validate the Versions below `before` (all, when `None`) that no staged
    /// object ever started: their closure is empty or only evidence, and they
    /// owe the same checks and native owner as any other.
    fn validate_unstarted(&mut self, before: Option<Uuid>) -> Result<(), PortableError> {
        if self.refusal.is_some() {
            return Ok(());
        }
        let Some(registry) = &self.registry else {
            return Ok(());
        };
        let unstarted: Vec<Uuid> = registry
            .versions
            .keys()
            .copied()
            .filter(|id| {
                before.is_none_or(|bound| *id < bound)
                    && !self.started.contains(id)
                    && !self.finished.contains(id)
            })
            .collect();
        for id in unstarted {
            let stage = VersionStage {
                id,
                sink: CopySink::staged_in(self.scratch.clone(), NAMESPACE),
                bytes: 0,
                staged: Objects::new(),
            };
            match self.validate_stage(&stage) {
                Ok(()) => {
                    self.finished.insert(id);
                }
                Err(refusal) => return self.defer(refusal),
            }
        }
        Ok(())
    }

    /// A refusal of a Version's content waits for the end of the scan; any other
    /// failure, authentication included, ends it now.
    fn defer(&mut self, error: PortableError) -> Result<(), PortableError> {
        if matches!(
            error.code,
            PortableErrorCode::Incompatible | PortableErrorCode::LimitExceeded
        ) {
            self.refuse(error);
            Ok(())
        } else {
            Err(error)
        }
    }

    /// The first refused Version, in scan order, answers for the package unless
    /// authentication fails. Its stage is reclaimed now, and nothing more is
    /// staged, spilled or validated: later entries are only hashed.
    fn refuse(&mut self, error: PortableError) {
        self.refusal.get_or_insert(error);
        self.current = None;
    }

    fn validate_stage(&mut self, stage: &VersionStage) -> Result<(), PortableError> {
        check_cancel(self.cancelled)?;
        let registry = self.registry.as_ref().ok_or_else(invalid)?;
        let version = registry.versions.get(&stage.id).ok_or_else(invalid)?;
        // A Version with no staged object still has a closure to check and a
        // native owner to run; its stage is then an empty private directory.
        let empty_stage = match stage.sink.staged() {
            Some(_) => None,
            None => Some(
                tempfile::Builder::new()
                    .prefix(".graphforge-research-validation-")
                    .tempdir_in(&self.scratch)
                    .map_err(|error| {
                        PortableError::new(
                            PortableErrorCode::Io,
                            "cannot create research validation scratch",
                        )
                        .with_cause(error.kind().to_string())
                    })?,
            ),
        };
        let staged_dir = stage.sink.staged().map(Path::to_path_buf).or_else(|| {
            empty_stage
                .as_ref()
                .map(|owner| owner.path().join("verified"))
        });
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
        Ok(())
    }

    /// The end of the scan, once the manifest is authenticated: the objects seen
    /// are exactly the ones the manifest names, and every Version was validated.
    pub(crate) fn finish(
        &mut self,
        report: &graphforge_core::portable::PortableReport,
    ) -> Result<(), PortableError> {
        self.finish_version()?;
        self.validate_unstarted(None)?;
        if let Some(refusal) = self.refusal.take() {
            return Err(refusal);
        }
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
        self.validate_unstarted(Some(id))?;
        // Canonical order keeps a Version's objects together; one that returns
        // after another began is a layout this reader does not accept.
        if self.finished.contains(&id) || self.finished.last().is_some_and(|last| *last > id) {
            return Err(invalid());
        }
        self.started.insert(id);
        self.current = Some(VersionStage {
            id,
            sink: CopySink::staged_in(self.scratch.clone(), NAMESPACE),
            bytes: 0,
            staged: Objects::new(),
        });
        Ok(())
    }

    /// Open the staged file for a retained object of Version `id`.
    fn stage_entry(
        &mut self,
        id: Uuid,
        relative: &str,
        length: u64,
    ) -> Result<Option<File>, PortableError> {
        if self.registry.is_none() {
            return Err(PortableError::at(
                PortableErrorCode::InvalidStructure,
                relative,
                "research content precedes the registry",
            ));
        }
        self.version(&id)?;
        self.start_version(id)?;
        if self.refusal.is_some() {
            self.current = None;
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

    /// Remember an object; the same Version and digest in both directories is refused.
    fn record(&mut self, id: Uuid, digest: String, held: Held) -> Result<(), PortableError> {
        match self.seen.insert((id, digest), held) {
            None => Ok(()),
            Some(_) => Err(invalid()),
        }
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
        Reopen::Driven(None),
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

    fn owns_profile(&self, relative: &str) -> bool {
        matches!(Self::slot(relative), Slot::Retained(..))
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
            Slot::Evidence(..) => Ok(None),
            Slot::Retained(id, _) => {
                if self.refusal.is_some() {
                    return Ok(None);
                }
                match self.stage_entry(id, relative, length) {
                    Ok(file) => Ok(file),
                    Err(refusal) => self.defer(refusal).map(|()| None),
                }
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
                let held = Held {
                    length,
                    evidence_only: false,
                };
                stage.staged.insert((id, hex.clone()), held);
                // The scan does not spill a second copy for the page walk: the
                // staged copy is the one walked, like a file's entry in place.
                let profile = stage.sink.staged().map_or(Ok(()), |dir| {
                    walk_staged(&dir.join(relative), length, relative)
                });
                self.record(id, hex, held)?;
                match profile {
                    Ok(()) => Ok(()),
                    // A refusal of the profile is a refusal of this Version.
                    Err(refusal)
                        if matches!(
                            refusal.code,
                            PortableErrorCode::LimitExceeded | PortableErrorCode::InvalidStructure
                        ) =>
                    {
                        self.refuse(refusal);
                        Ok(())
                    }
                    Err(failure) => Err(failure),
                }
            }
            Slot::Evidence(..) | Slot::Other => Ok(()),
        }
    }

    fn declined(
        &mut self,
        relative: &str,
        length: u64,
        digest: [u8; 32],
    ) -> Result<(), PortableError> {
        match Self::slot(relative) {
            Slot::Evidence(id, hex) => {
                authenticate(relative, &hex, digest)?;
                let held = Held {
                    length,
                    evidence_only: true,
                };
                self.record(id, hex, held)
            }
            // Not staged because an earlier Version was refused.
            Slot::Retained(id, hex) => {
                authenticate(relative, &hex, digest)?;
                let held = Held {
                    length,
                    evidence_only: false,
                };
                self.record(id, hex, held)
            }
            Slot::Registry | Slot::Other => Ok(()),
        }
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
