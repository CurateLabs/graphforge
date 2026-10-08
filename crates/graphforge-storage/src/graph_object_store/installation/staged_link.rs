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

use super::{
    ExistingObjectReuse, GraphObjectInstallEvidence, GraphObjectPublicationLease, InstalledObject,
    ObjectAuthentication, ReadIoEvidence, capture_installed_object, immutable_commit_error,
    record_completed_install, seal_graph_object, try_reuse_existing_object,
    verify_and_seal_graph_object_counted,
};
use super::{
    GfError, graph_object_path, returned_error_boundary, storage, validate_digest, validation,
};
use crate::graph_construction::{CapturedEncodedArtifact, construction_failpoint};

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
    let destination_name = std::ffi::OsStr::new(&digest[2..]);
    let mut replace_prior = None;
    let installed = if let Some(reused) = try_reuse_existing_object(
        cas,
        &bucket,
        destination_name,
        digest,
        source.bytes(),
        &mut ExistingObjectReuse {
            authentication,
            repair_corrupt_existing: false,
            replace_prior: &mut replace_prior,
        },
    )? {
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
        reused
    } else {
        match link_staged(lease, source, &bucket, authentication)? {
            Some(installed) => installed,
            // The object store is not on the encoder's filesystem (a bind
            // mount, say), so the inode cannot take a second name there. Say
            // so by copying through the ordinary install, whose copy,
            // authentication and barriers are unchanged.
            None => {
                return super::install_captured_source_with_lease(
                    lease,
                    &super::CapturedSource::Encoded(source),
                    false,
                    cancelled,
                );
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

fn link_staged(
    lease: &GraphObjectPublicationLease,
    source: &CapturedEncodedArtifact<'_>,
    bucket: &graphforge_filesystem::StableDirectory,
    authentication: ObjectAuthentication,
) -> Result<Option<InstalledObject>, GfError> {
    let cas = &lease.cas;
    let digest = source.content_sha256();
    let relative = source.relative_path();
    // Linking needs the staged name and the object store on one filesystem.
    // ADR 0013 puts the project on one admitted volume, so this is the normal
    // case; `None` reports the exception to the caller instead of failing.
    if source.parent().identity().volume_serial != bucket.identity().volume_serial {
        return Ok(None);
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
    let staged = crate::durable_commit::SealedArtifact::seal_recoverable_existing(
        source.parent().physical(),
        source.name(),
        source
            .source()
            .try_clone()
            .map_err(|error| storage("clone staged encoded source", &staged_path, error))?,
        source.identity(),
        cas.allocation.as_ref(),
    )
    .map_err(|error| storage("seal staged encoded source", &staged_path, error))?;
    construction_failpoint(&format!("cas.install.after_object_sync.{relative}"));
    returned_error_boundary("install:temp-sealed")?;
    let destination_name = std::ffi::OsStr::new(&digest[2..]);
    let object_path = graph_object_path(&cas.diagnostic_root, digest)?;
    let mut concurrent_io = ReadIoEvidence::default();
    let linked = if super::super::cross_device_link_forced() {
        Err(std::io::Error::from(std::io::ErrorKind::CrossesDevices))
    } else {
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
    };
    let (_file, identity, reused) = match linked {
        Ok(linked) => linked,
        // The kernel is the authority on whether a link can cross: a mount
        // can share a device number and still refuse.
        Err(error) if error.kind() == std::io::ErrorKind::CrossesDevices => return Ok(None),
        Err(error) => return Err(immutable_commit_error(error, cas)),
    };
    let bytes_hashed = concurrent_io.sha_bytes;
    let checksum_read_bytes = concurrent_io
        .bytes
        .checked_sub(bytes_hashed)
        .ok_or_else(|| validation("CAS SHA read count exceeds native reads"))?;
    Ok(Some(InstalledObject {
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
            file_fsync_calls: 1,
            directory_fsync_calls: 1,
            fsync_calls: 2,
        },
        identity,
    }))
}
