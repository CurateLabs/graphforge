//! Publish an encoded object by linking the file the encoder already wrote.
//!
//! The encoder streams every published object to a private staged name on the
//! project filesystem, hashing it with SHA-256 and XXH64 as it writes. Copying
//! that file into the content-addressed store would write the final output a
//! second time and read it back to check the copy. This module instead gives
//! the exact staged inode its immutable name, so each published byte is
//! written once. ADR 0013 still applies in full: the payload is made durable
//! before its content address becomes visible, and the address's bucket
//! directory is acknowledged before the install is reported.
//!
//! The staged name stays in place. It belongs to the session's private tree,
//! which is retired as a whole, so a crash at any point leaves the staged file
//! for a rerun and never changes `CURRENT`. A rerun that finds the object
//! already installed treats it as deduplication and authenticates it.
//!
//! Nothing here reads the staged file back, so an edit between the encoder's
//! write and publication is caught later, by the commit boundary's XXH64
//! admission. Two rules keep that refusal from leaving a lasting mark. An
//! object this lease linked and admission refused is unlinked again
//! ([`retire_unadmitted_link`]). And an existing object whose SHA-256 does not
//! name its address, for example one a crash left behind before admission,
//! is replaced by the correct install, never treated as permanent.

use super::{
    ExistingObjectReuse, GraphObjectInstallEvidence, GraphObjectPublicationLease, HashDomain,
    InstalledObject, ObjectAuthentication, ReadIoEvidence, capture_installed_object,
    immutable_commit_error, record_completed_install, seal_graph_object, try_reuse_existing_object,
    verify_and_seal_graph_object_counted,
};
use super::{
    GfError, graph_object_path, returned_error_boundary, storage, validate_digest, validation,
};
use crate::durable_commit::{acknowledge_directory, observe_barriers};
use crate::graph_construction::{CapturedEncodedArtifact, construction_failpoint};
use graphforge_filesystem::{FileIdentity, StableDirectory};

/// What already sits at the object's content address.
enum Existing {
    Absent,
    Reused(InstalledObject),
    /// An object that is provably not the bytes its address names.
    Mismatched(FileIdentity),
}

/// How the link attempt ended.
enum Linked {
    Installed(InstalledObject),
    /// The object store could not take a link; `file_fsyncs` barriers already ran.
    CrossDevice {
        file_fsyncs: u64,
    },
}

pub(super) fn install_staged_encoded_artifact(
    lease: &GraphObjectPublicationLease,
    source: &CapturedEncodedArtifact<'_>,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<GraphObjectInstallEvidence, GfError> {
    source.revalidate()?;
    crate::graph_construction::reject_cancelled(cancelled)?;
    let digest = source.content_sha256();
    validate_digest(digest)?;
    let cas = &lease.cas;
    let captured_identity = lease
        .installed_objects
        .lock()
        .map_err(|_| validation("graph object installation authority poisoned"))?
        .get(digest)
        .filter(|capture| {
            capture.byte_length == source.bytes() && capture.content_xxh64 == source.checksum()
        })
        .map(|capture| capture.identity);
    let authentication = ObjectAuthentication::CapturedChecksum {
        checksum: source.checksum(),
        identity: captured_identity,
    };
    let bucket = cas.digest_bucket(digest, true)?;
    let installed = match existing_object(lease, source, &bucket, authentication)? {
        Existing::Reused(mut reused) => {
            // A second name on the staged inode must be this very object-store
            // entry. Any other alias is not something this install created.
            if staged_links(source)? > 1 && reused.identity != source.identity() {
                return Err(validation(
                    "staged encoded source has an alias that is not its content address",
                ));
            }
            construction_failpoint(&format!(
                "cas.install.after_dedupe.{}",
                source.relative_path()
            ));
            // The entry may be one an earlier attempt linked and crashed
            // before acknowledging. ADR 0013 requires its namespace barrier
            // before anything can reference it.
            acknowledge_directory(&bucket).map_err(|error| {
                storage(
                    "acknowledge reused graph object",
                    &cas.diagnostic_root,
                    error,
                )
            })?;
            reused.evidence.directory_fsync_calls += 1;
            reused.evidence.fsync_calls += 1;
            reused
        }
        existing => {
            let mut retire_barriers = 0;
            if let Existing::Mismatched(prior) = existing {
                retire_mismatched(lease, &bucket, digest, prior)?;
                retire_barriers = 1;
            }
            match link_staged(lease, source, &bucket, authentication)? {
                Linked::Installed(mut installed) => {
                    installed.evidence.directory_fsync_calls += retire_barriers;
                    installed.evidence.fsync_calls += retire_barriers;
                    installed
                }
                // The object store is not on the encoder's filesystem (a bind
                // mount, say), so the inode cannot take a second name there.
                // Say so by copying through the ordinary install, whose copy,
                // authentication and barriers are unchanged; the barrier the
                // failed attempt already ran is still counted.
                Linked::CrossDevice { file_fsyncs } => {
                    let mut evidence = super::install_captured_source_with_lease(
                        lease,
                        &super::CapturedSource::Encoded(source),
                        false,
                        cancelled,
                    )?;
                    evidence.file_fsync_calls += file_fsyncs;
                    evidence.directory_fsync_calls += retire_barriers;
                    evidence.fsync_calls += file_fsyncs + retire_barriers;
                    return Ok(evidence);
                }
            }
        }
    };
    source.revalidate()?;
    let InstalledObject { evidence, identity } = installed;
    capture_installed_object(
        lease,
        digest,
        source.bytes(),
        identity,
        evidence.content_xxh64,
    )?;
    record_completed_install(&evidence, 0);
    Ok(evidence)
}

fn staged_links(source: &CapturedEncodedArtifact<'_>) -> Result<u64, GfError> {
    graphforge_filesystem::file_link_count(source.source())
        .map_err(|error| storage("count staged encoded links", source.parent().path(), error))
}

/// Classify what is at the address. The cheap path checks length and XXH64
/// against the staged file's captured values; only if that refuses is the
/// object decided by SHA-256, which is what names it.
fn existing_object(
    lease: &GraphObjectPublicationLease,
    source: &CapturedEncodedArtifact<'_>,
    bucket: &StableDirectory,
    cheap: ObjectAuthentication,
) -> Result<Existing, GfError> {
    let cas = &lease.cas;
    let digest = source.content_sha256();
    let name = std::ffi::OsStr::new(&digest[2..]);
    match bucket.open_child_file(name) {
        Ok(file) => {
            let length = file
                .metadata()
                .map_err(|error| {
                    storage("inspect existing graph object", &cas.diagnostic_root, error)
                })?
                .len();
            if length != source.bytes() {
                // No SHA-256 over a different length can be the address.
                let identity = graphforge_filesystem::file_identity(&file).map_err(|error| {
                    storage(
                        "identify existing graph object",
                        &cas.diagnostic_root,
                        error,
                    )
                })?;
                return Ok(Existing::Mismatched(identity));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Existing::Absent),
        Err(error) => {
            return Err(storage(
                "open existing graph object",
                &cas.diagnostic_root,
                error,
            ));
        }
    }
    let mut ignored = None;
    match try_reuse_existing_object(
        cas,
        bucket,
        name,
        digest,
        source.bytes(),
        &mut ExistingObjectReuse {
            authentication: cheap,
            repair_corrupt_existing: false,
            replace_prior: &mut ignored,
        },
    ) {
        Ok(Some(reused)) => return Ok(Existing::Reused(reused)),
        Ok(None) => return Ok(Existing::Absent),
        Err(GfError::Validation(_)) => {}
        Err(error) => return Err(error),
    }
    let mut replace_prior = None;
    match try_reuse_existing_object(
        cas,
        bucket,
        name,
        digest,
        source.bytes(),
        &mut ExistingObjectReuse {
            authentication: ObjectAuthentication::Sha(HashDomain::ArtifactPayload),
            repair_corrupt_existing: true,
            replace_prior: &mut replace_prior,
        },
    )? {
        Some(reused) => {
            if reused.evidence.content_xxh64 != Some(source.checksum()) {
                return Err(validation(
                    "staged inventory checksum differs from the object at its address",
                ));
            }
            Ok(Existing::Reused(reused))
        }
        None => Ok(replace_prior.map_or(Existing::Absent, Existing::Mismatched)),
    }
}

/// Remove an object proven not to hash to its address, so the correct install
/// can take the name. The publication lease excludes collection meanwhile.
fn retire_mismatched(
    lease: &GraphObjectPublicationLease,
    bucket: &StableDirectory,
    digest: &str,
    identity: FileIdentity,
) -> Result<(), GfError> {
    let cas = &lease.cas;
    let object_path = graph_object_path(&cas.diagnostic_root, digest)?;
    bucket
        .unlink_child_if_identity(std::ffi::OsStr::new(&digest[2..]), identity)
        .map_err(|error| storage("remove mis-addressed graph object", &object_path, error))?;
    if let Some(allocation) = &cas.allocation {
        allocation.remove_file_at(&object_path)?;
    }
    construction_failpoint(&format!("cas.install.after_mismatch_unlink.{digest}"));
    acknowledge_directory(bucket)
        .map_err(|error| storage("acknowledge mis-addressed removal", &object_path, error))
}

/// Unlink an object this lease linked that commit-boundary admission refused.
/// Anything else at the address is not this lease's to remove.
pub(in crate::graph_object_store) fn retire_unadmitted_link(
    lease: &GraphObjectPublicationLease,
    digest: &str,
    identity: FileIdentity,
) -> Result<(), GfError> {
    let linked = lease
        .linked_objects
        .lock()
        .map_err(|_| validation("graph object installation authority poisoned"))?
        .remove(digest);
    if linked != Some(identity) {
        return Ok(());
    }
    let bucket = lease.cas.digest_bucket(digest, false)?;
    retire_mismatched(lease, &bucket, digest, identity)
}

fn link_staged(
    lease: &GraphObjectPublicationLease,
    source: &CapturedEncodedArtifact<'_>,
    bucket: &StableDirectory,
    authentication: ObjectAuthentication,
) -> Result<Linked, GfError> {
    let cas = &lease.cas;
    let digest = source.content_sha256();
    let relative = source.relative_path();
    // Linking needs the staged name and the object store on one filesystem.
    // ADR 0013 puts the project on one admitted volume, so this is the normal
    // case; the exception is reported to the caller instead of failing.
    if source.parent().identity().volume_serial != bucket.identity().volume_serial {
        return Ok(Linked::CrossDevice { file_fsyncs: 0 });
    }
    if staged_links(source)? != 1 {
        return Err(validation(
            "staged encoded source has an alias that is not its content address",
        ));
    }
    let staged_path = source.parent().path().join(source.name());
    // Objects are immutable. Seal the exact inode before it can gain the
    // content-addressed name, then make its bytes durable with the sole file
    // barrier of this object.
    seal_graph_object(source.source(), &staged_path, &cas.diagnostic_root)?;
    let (sealed, file_fsyncs) = observe_barriers(|| {
        crate::durable_commit::SealedArtifact::seal_recoverable_existing(
            source.parent().physical(),
            source.name(),
            source.source().try_clone()?,
            source.identity(),
            cas.allocation.as_ref(),
        )
    });
    let staged =
        sealed.map_err(|error| storage("seal staged encoded source", &staged_path, error))?;
    construction_failpoint(&format!("cas.install.after_object_sync.{relative}"));
    returned_error_boundary("install:temp-sealed")?;
    let destination_name = std::ffi::OsStr::new(&digest[2..]);
    let object_path = graph_object_path(&cas.diagnostic_root, digest)?;
    let mut concurrent_io = ReadIoEvidence::default();
    let (linked, directory_fsyncs) = observe_barriers(|| {
        if super::super::cross_device_link_forced() {
            return Err(std::io::Error::from(std::io::ErrorKind::CrossesDevices));
        }
        crate::durable_commit::link_immutable(
            &staged,
            bucket,
            destination_name,
            |existing, _identity| {
                concurrent_io = verify_and_seal_graph_object_counted(
                    existing,
                    digest,
                    source.bytes(),
                    &object_path,
                    &cas.diagnostic_root,
                    authentication,
                )
                .map_err(std::io::Error::other)?;
                Ok(())
            },
            |_reused, file| {
                if let Some(allocation) = &cas.allocation {
                    allocation
                        .replace_file_at(&object_path, file)
                        .map_err(std::io::Error::other)?;
                }
                construction_failpoint(&format!("cas.install.after_link.{relative}"));
                returned_error_boundary("install:final-linked").map_err(std::io::Error::other)
            },
            |_reused, _file| {
                returned_error_boundary("install:bucket-synced").map_err(std::io::Error::other)?;
                construction_failpoint(&format!("cas.install.after_bucket_sync.{relative}"));
                Ok(())
            },
        )
    });
    let (_file, identity, reused) = match linked {
        Ok(linked) => linked,
        // The kernel is the authority on whether a link can cross: a mount
        // can share a device number and still refuse.
        Err(error) if error.kind() == std::io::ErrorKind::CrossesDevices => {
            return Ok(Linked::CrossDevice { file_fsyncs });
        }
        Err(error) => return Err(immutable_commit_error(error, cas)),
    };
    if !reused {
        lease
            .linked_objects
            .lock()
            .map_err(|_| validation("graph object installation authority poisoned"))?
            .insert(digest.to_owned(), identity);
    }
    let bytes_hashed = concurrent_io.sha_bytes;
    let checksum_read_bytes = concurrent_io
        .bytes
        .checked_sub(bytes_hashed)
        .ok_or_else(|| validation("CAS SHA read count exceeds native reads"))?;
    Ok(Linked::Installed(InstalledObject {
        evidence: GraphObjectInstallEvidence {
            // Computed from the exact bytes the encoder wrote.
            content_xxh64: Some(source.checksum()),
            bytes_hashed,
            checksum_read_bytes,
            bytes_installed: if reused { 0 } else { source.bytes() },
            reused_existing: reused,
            attempted_install: true,
            read_calls: concurrent_io.calls,
            // The staged file is the object: nothing is copied.
            write_calls: 0,
            write_bytes: 0,
            // Counted from the barriers that actually ran.
            file_fsync_calls: file_fsyncs,
            directory_fsync_calls: directory_fsyncs,
            fsync_calls: file_fsyncs + directory_fsyncs,
        },
        identity,
    }))
}
