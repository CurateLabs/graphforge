//! installation ownership for immutable graph objects.
use super::{Seek, fs};

use super::BUFFER_BYTES;
use super::CasRoot;
use super::Digest;
use super::File;
use super::GRAPH_OBJECTS_DIR;
use super::GfError;
use super::GraphObjectInstallEvidence;
use super::GraphObjectPublicationLease;
use super::Path;
use super::Read;
use super::ReadIoEvidence;
use super::Sha256;
use super::StableDirectory;
use super::TEMP_DIR;
use super::Uuid;
use super::Write;
use super::begin_graph_object_publication;
use super::checked_read_io_sum;
use super::graph_object_path;
use super::hex_digest;
use super::returned_error_boundary;
use super::storage;
use super::validate_digest;
use super::validation;
use super::verify_file_counted;
use super::verify_stream_counted;

struct TemporaryObject {
    name: std::ffi::OsString,
    #[cfg(unix)]
    file: File,
    #[cfg(windows)]
    file: graphforge_filesystem::WindowsCasWriter,
    identity: graphforge_filesystem::FileIdentity,
}

struct SealedTemporaryObject {
    name: std::ffi::OsString,
    file: File,
    identity: graphforge_filesystem::FileIdentity,
}

#[cfg(unix)]
type CasTemporaryWriter = File;
#[cfg(windows)]
type CasTemporaryWriter = graphforge_filesystem::WindowsCasWriter;
/// Install exact in-memory bytes under their SHA-256 identity.
pub fn install_graph_object_bytes(
    root: &Path,
    bytes: &[u8],
) -> Result<(String, GraphObjectInstallEvidence), GfError> {
    let lease = begin_graph_object_publication(root)?;
    install_graph_object_bytes_with_lease(&lease, bytes)
}

pub(super) fn install_graph_object_bytes_with_lease(
    lease: &GraphObjectPublicationLease,
    bytes: &[u8],
) -> Result<(String, GraphObjectInstallEvidence), GfError> {
    let digest = hex_digest(Sha256::digest(bytes).into());
    let expected_length =
        u64::try_from(bytes.len()).map_err(|_| validation("graph object bytes exceed u64"))?;
    install_object(&lease.cas, &digest, expected_length, false, |file| {
        file.write_all(bytes).map_err(|error| {
            storage(
                "write temporary graph object",
                &lease.cas.diagnostic_root,
                error,
            )
        })?;
        file.sync_all().map_err(|error| {
            storage(
                "fsync temporary graph object",
                &lease.cas.diagnostic_root,
                error,
            )
        })?;
        // The source is already resident memory; only the mandatory temporary
        // file verification below is an application-observed payload read.
        Ok(0)
    })
    .and_then(|mut evidence| {
        if evidence.attempted_install {
            evidence.write_bytes = expected_length;
            evidence.write_calls = u64::from(!bytes.is_empty());
            evidence.file_fsync_calls = 1;
            evidence.fsync_calls = evidence
                .fsync_calls
                .checked_add(1)
                .ok_or_else(|| validation("CAS fsync count overflows"))?;
        }
        Ok((digest, evidence))
    })
}

/// Stream, hash, and install a new payload object from a regular source file.
pub fn install_graph_object_file(
    root: &Path,
    source: &Path,
    expected_digest: &str,
    expected_length: u64,
) -> Result<GraphObjectInstallEvidence, GfError> {
    let lease = begin_graph_object_publication(root)?;
    install_graph_object_file_with_lease(&lease, source, expected_digest, expected_length)
}

#[allow(clippy::too_many_lines)] // The streamed copy keeps source authentication and destination durability atomic.
pub(super) fn install_graph_object_file_with_lease(
    lease: &GraphObjectPublicationLease,
    source: &Path,
    expected_digest: &str,
    expected_length: u64,
) -> Result<GraphObjectInstallEvidence, GfError> {
    validate_digest(expected_digest)?;
    let metadata = fs::symlink_metadata(source)
        .map_err(|error| storage("inspect graph object source", source, error))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() != expected_length
    {
        return Err(validation(
            "graph object source is not the declared regular file",
        ));
    }
    let read_calls = std::cell::Cell::new(0_u64);
    let write_calls = std::cell::Cell::new(0_u64);
    let file_sync_calls = std::cell::Cell::new(0_u64);
    let result =
        install_object(
            &lease.cas,
            expected_digest,
            expected_length,
            true,
            |output| {
                let cache_window = graphforge_filesystem::cache_release_window_for_streams(2)
                    .map_err(|error| {
                        storage(
                            "derive graph object cache budget",
                            &lease.cas.diagnostic_root,
                            error,
                        )
                    })?;
                let input = File::open(source)
                    .map_err(|error| storage("open graph object source", source, error))?;
                let mut input = graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
                    input,
                    cache_window,
                    graphforge_filesystem::FileCacheReleaseTracker::default(),
                )
                .map_err(|error| storage("open bounded graph object source", source, error))?;
                #[cfg(unix)]
                let mut bounded_output =
                    graphforge_filesystem::DurableFileCacheWriter::with_window_bytes(
                        output.try_clone().map_err(|error| {
                            storage(
                                "clone temporary graph object",
                                &lease.cas.diagnostic_root,
                                error,
                            )
                        })?,
                        cache_window,
                    )
                    .map_err(|error| {
                        storage(
                            "open bounded temporary graph object",
                            &lease.cas.diagnostic_root,
                            error,
                        )
                    })?;
                let mut hasher = Sha256::new();
                let mut total = 0_u64;
                let mut buffer = vec![0_u8; BUFFER_BYTES];
                let copied =
                    (|| -> Result<u64, GfError> {
                        loop {
                            let read = input.read(&mut buffer).map_err(|error| {
                                storage("read graph object source", source, error)
                            })?;
                            if read == 0 {
                                break;
                            }
                            read_calls.set(
                                read_calls.get().checked_add(1).ok_or_else(|| {
                                    validation("object install read calls overflow")
                                })?,
                            );
                            #[cfg(unix)]
                            bounded_output.write_all(&buffer[..read]).map_err(|error| {
                                storage(
                                    "write temporary graph object",
                                    &lease.cas.diagnostic_root,
                                    error,
                                )
                            })?;
                            #[cfg(windows)]
                            output.write_all(&buffer[..read]).map_err(|error| {
                                storage(
                                    "write temporary graph object",
                                    &lease.cas.diagnostic_root,
                                    error,
                                )
                            })?;
                            write_calls.set(write_calls.get().checked_add(1).ok_or_else(|| {
                                validation("object install write calls overflow")
                            })?);
                            hasher.update(&buffer[..read]);
                            total = total
                                .checked_add(u64::try_from(read).map_err(|_| {
                                    validation("object install read length exceeds u64")
                                })?)
                                .ok_or_else(|| validation("object install byte count overflows"))?;
                        }
                        if total != expected_length
                            || hex_digest(hasher.finalize().into()) != expected_digest
                        {
                            return Err(validation(
                                "graph object source digest or length changed during install",
                            ));
                        }
                        Ok(total)
                    })();
                let released = input
                    .finish()
                    .map_err(|error| storage("release graph object source cache", source, error));
                let total = match (copied, released) {
                    (Ok(total), Ok(_)) => total,
                    (Ok(_), Err(release)) => return Err(release),
                    (Err(primary), Ok(_)) => return Err(primary),
                    (Err(primary), Err(release)) => {
                        return Err(storage(
                            "copy and release graph object source",
                            source,
                            format!("{primary}; cache release also failed: {release}"),
                        ));
                    }
                };
                #[cfg(unix)]
                {
                    bounded_output.sync_all_and_release().map_err(|error| {
                        storage(
                            "fsync temporary graph object",
                            &lease.cas.diagnostic_root,
                            error,
                        )
                    })?;
                    file_sync_calls.set(bounded_output.evidence().sync_operations);
                }
                #[cfg(windows)]
                {
                    output.sync_all().map_err(|error| {
                        storage(
                            "fsync temporary graph object",
                            &lease.cas.diagnostic_root,
                            error,
                        )
                    })?;
                    file_sync_calls.set(1);
                }
                Ok(total)
            },
        );
    result.and_then(|mut evidence| {
        if evidence.attempted_install {
            evidence.read_calls = evidence
                .read_calls
                .checked_add(read_calls.get())
                .ok_or_else(|| validation("object install read calls overflow"))?;
            evidence.write_calls = evidence
                .write_calls
                .checked_add(write_calls.get())
                .ok_or_else(|| validation("object install write calls overflow"))?;
            evidence.write_bytes = evidence
                .write_bytes
                .checked_add(expected_length)
                .ok_or_else(|| validation("object install write bytes overflow"))?;
            evidence.file_fsync_calls = file_sync_calls.get();
            evidence.fsync_calls = evidence
                .fsync_calls
                .checked_add(file_sync_calls.get())
                .ok_or_else(|| validation("CAS file synchronization count overflows"))?;
        }
        Ok(evidence)
    })
}
fn install_object<F>(
    cas: &CasRoot,
    digest: &str,
    expected_length: u64,
    writer_authenticated: bool,
    write_temporary: F,
) -> Result<GraphObjectInstallEvidence, GfError>
where
    F: FnOnce(&mut CasTemporaryWriter) -> Result<u64, GfError>,
{
    validate_digest(digest)?;
    let bucket = cas.digest_bucket(digest, true)?;
    let destination_name = std::ffi::OsStr::new(&digest[2..]);
    if let Some(evidence) =
        try_reuse_existing_object(cas, &bucket, destination_name, digest, expected_length)?
    {
        return Ok(evidence);
    }
    let temporary_name = std::ffi::OsString::from(Uuid::new_v4().hyphenated().to_string());
    #[cfg(unix)]
    let temporary = cas.tmp.create_child_file(&temporary_name);
    #[cfg(windows)]
    let temporary = cas.tmp.create_cas_child_file(&temporary_name);
    let mut temporary = temporary.map_err(|error| {
        storage(
            "create stable temporary graph object",
            &cas.diagnostic_root,
            error,
        )
    })?;
    #[cfg(unix)]
    let temporary_identity = graphforge_filesystem::file_identity(&temporary).map_err(|error| {
        storage(
            "inspect temporary graph object",
            &cas.diagnostic_root,
            error,
        )
    })?;
    #[cfg(windows)]
    let temporary_identity = temporary.identity();
    let written = write_temporary(&mut temporary);
    let temporary_path = cas
        .diagnostic_root
        .join(GRAPH_OBJECTS_DIR)
        .join(TEMP_DIR)
        .join(&temporary_name);
    #[cfg(unix)]
    let observed_file = &temporary;
    #[cfg(windows)]
    let observed_file = temporary.as_file();
    let observed = cas.allocation.as_ref().map_or(Ok(()), |allocation| {
        allocation.replace_file_at(&temporary_path, observed_file)
    });
    let bytes_hashed = written?;
    observed?;
    let preseal_io = if writer_authenticated || cfg!(windows) {
        ReadIoEvidence::default()
    } else {
        temporary.rewind().map_err(|error| {
            storage("rewind temporary graph object", &cas.diagnostic_root, error)
        })?;
        verify_stream_counted(
            &mut temporary,
            digest,
            expected_length,
            &cas.diagnostic_root,
        )?
    };
    // Windows must close the writable handle and reopen an exact-identity,
    // protected read handle before publication. That transition authenticates
    // the complete payload below, so a second pre-seal read would be redundant.
    let (installed, sealed_bytes_hashed, concurrent_io) = finalize_temporary_object(
        cas,
        &bucket,
        TemporaryObject {
            name: temporary_name,
            file: temporary,
            identity: temporary_identity,
        },
        digest,
        expected_length,
    )?;
    let bytes_hashed = [
        preseal_io.bytes,
        sealed_bytes_hashed,
        if installed { 0 } else { expected_length },
    ]
    .into_iter()
    .try_fold(bytes_hashed, u64::checked_add)
    .ok_or_else(|| validation("graph object hashed byte count overflows"))?;
    let read_calls = preseal_io
        .calls
        .checked_add(concurrent_io.calls)
        .ok_or_else(|| validation("graph object read call count overflows"))?;
    Ok(GraphObjectInstallEvidence {
        bytes_hashed,
        bytes_installed: if installed { expected_length } else { 0 },
        reused_existing: !installed,
        attempted_install: true,
        read_calls,
        // The source-copy/authentication submissions are added by the caller.
        // Finalization synchronizes both namespaces even if a concurrent winner
        // supplied the retained object. Early reuse returns before this path.
        fsync_calls: 2,
        directory_fsync_calls: 2,
        ..GraphObjectInstallEvidence::default()
    })
}

fn reused_object_evidence(expected_length: u64, io: ReadIoEvidence) -> GraphObjectInstallEvidence {
    debug_assert_eq!(io.bytes, expected_length);
    GraphObjectInstallEvidence {
        bytes_hashed: io.bytes,
        reused_existing: true,
        read_calls: io.calls,
        ..GraphObjectInstallEvidence::default()
    }
}

#[cfg(unix)]
fn try_reuse_existing_object(
    cas: &CasRoot,
    bucket: &StableDirectory,
    destination_name: &std::ffi::OsStr,
    digest: &str,
    expected_length: u64,
) -> Result<Option<GraphObjectInstallEvidence>, GfError> {
    let file = match bucket.open_child_file(destination_name) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(storage(
                "open existing graph object",
                &cas.diagnostic_root,
                error,
            ));
        }
    };
    let io = verify_and_seal_graph_object_counted(
        &file,
        digest,
        expected_length,
        &graph_object_path(&cas.diagnostic_root, digest)?,
        &cas.diagnostic_root,
    )?;
    if let Some(allocation) = &cas.allocation {
        allocation.replace_file_at(&graph_object_path(&cas.diagnostic_root, digest)?, &file)?;
    }
    Ok(Some(reused_object_evidence(expected_length, io)))
}

#[cfg(windows)]
fn try_reuse_existing_object(
    cas: &CasRoot,
    bucket: &StableDirectory,
    destination_name: &std::ffi::OsStr,
    digest: &str,
    expected_length: u64,
) -> Result<Option<GraphObjectInstallEvidence>, GfError> {
    let mut adoption_io = ReadIoEvidence::default();
    let file = match bucket.open_cas_child_file(destination_name) {
        Ok(file) => file.into_file(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(canonical_error) => {
            let mut legacy = bucket
                .open_legacy_cas_child_for_adoption(destination_name)
                .map_err(|legacy_error| {
                    storage(
                        "reject unsealed or busy graph object",
                        &cas.diagnostic_root,
                        if legacy_error.kind() == std::io::ErrorKind::NotFound {
                            canonical_error
                        } else {
                            legacy_error
                        },
                    )
                })?;
            adoption_io =
                verify_stream_counted(&mut legacy, digest, expected_length, &cas.diagnostic_root)?;
            bucket
                .adopt_legacy_cas_child(destination_name, legacy)
                .map(graphforge_filesystem::WindowsSealedCasFile::into_file)
                .map_err(|error| {
                    storage(
                        "adopt authenticated legacy graph object",
                        &cas.diagnostic_root,
                        error,
                    )
                })?
        }
    };
    let io = verify_and_seal_graph_object_counted(
        &file,
        digest,
        expected_length,
        &graph_object_path(&cas.diagnostic_root, digest)?,
        &cas.diagnostic_root,
    )?;
    let mut evidence = reused_object_evidence(expected_length, io);
    evidence.bytes_hashed = adoption_io
        .bytes
        .checked_add(io.bytes)
        .ok_or_else(|| validation("reused object hashed byte count overflows"))?;
    evidence.read_calls = adoption_io
        .calls
        .checked_add(io.calls)
        .ok_or_else(|| validation("reused object read call count overflows"))?;
    if let Some(allocation) = &cas.allocation {
        allocation.replace_file_at(&graph_object_path(&cas.diagnostic_root, digest)?, &file)?;
    }
    Ok(Some(evidence))
}

fn finalize_temporary_object(
    cas: &CasRoot,
    bucket: &StableDirectory,
    temporary: TemporaryObject,
    digest: &str,
    expected_length: u64,
) -> Result<(bool, u64, ReadIoEvidence), GfError> {
    let destination_name = std::ffi::OsStr::new(&digest[2..]);
    let temporary_path = cas
        .diagnostic_root
        .join(GRAPH_OBJECTS_DIR)
        .join(TEMP_DIR)
        .join(&temporary.name);
    #[cfg(unix)]
    let (temporary, sealed_io) = {
        seal_graph_object(&temporary.file, &temporary_path, &cas.diagnostic_root)?;
        (
            SealedTemporaryObject {
                name: temporary.name,
                file: temporary.file,
                identity: temporary.identity,
            },
            ReadIoEvidence::default(),
        )
    };
    #[cfg(windows)]
    let (temporary, sealed_io) = transition_temporary_to_sealed_reader(
        &cas.tmp,
        temporary,
        digest,
        expected_length,
        &cas.diagnostic_root,
    )?;
    if let Some(allocation) = &cas.allocation {
        allocation.replace_file_at(&temporary_path, &temporary.file)?;
    }
    let sealed_bytes_hashed = sealed_io.bytes;
    validate_sealed_temporary(&temporary, expected_length, &cas.diagnostic_root)?;
    returned_error_boundary("install:temp-sealed")?;
    let mut concurrent_io = ReadIoEvidence::default();
    let installed = if let Ok((installed, _identity)) = cas.tmp.link_child_into(
        &temporary.name,
        &temporary.file,
        temporary.identity,
        bucket,
        destination_name,
    ) {
        if let Some(allocation) = &cas.allocation {
            allocation.replace_file_at(
                &graph_object_path(&cas.diagnostic_root, digest)?,
                &installed,
            )?;
        }
        true
    } else {
        #[cfg(unix)]
        let existing = bucket.open_child_file(destination_name);
        #[cfg(windows)]
        let existing = bucket
            .open_cas_child_file(destination_name)
            .map(graphforge_filesystem::WindowsSealedCasFile::into_file);
        let existing = existing.map_err(|error| {
            storage(
                "open concurrently installed graph object",
                &cas.diagnostic_root,
                error,
            )
        })?;
        concurrent_io = verify_and_seal_graph_object_counted(
            &existing,
            digest,
            expected_length,
            &graph_object_path(&cas.diagnostic_root, digest)?,
            &cas.diagnostic_root,
        )?;
        if let Some(allocation) = &cas.allocation {
            allocation
                .replace_file_at(&graph_object_path(&cas.diagnostic_root, digest)?, &existing)?;
        }
        false
    };
    returned_error_boundary("install:final-linked")?;
    // The destination namespace must be durable before its temporary alias is
    // removed; after a crash, retry can therefore authenticate the final CAS
    // name without depending on the temporary namespace.
    bucket.sync().map_err(|error| {
        storage(
            "sync stable graph object bucket",
            &cas.diagnostic_root,
            error,
        )
    })?;
    returned_error_boundary("install:bucket-synced")?;
    // Windows cannot open a deletion handle while the original temporary
    // handle remains open without delete sharing. Publication and concurrent
    // winner authentication are complete, so release it before exact-identity
    // cleanup; the fresh CAS-owned inode remains sealed at its final name.
    drop(temporary.file);
    remove_finalized_temporary(cas, &temporary.name, temporary.identity, &temporary_path)?;
    returned_error_boundary("install:temp-unlinked")?;
    cas.tmp.sync().map_err(|error| {
        storage(
            "sync stable graph object temporary directory",
            &cas.diagnostic_root,
            error,
        )
    })?;
    Ok((
        installed,
        sealed_bytes_hashed,
        checked_read_io_sum(sealed_io, concurrent_io)?,
    ))
}

fn remove_finalized_temporary(
    cas: &CasRoot,
    name: &std::ffi::OsStr,
    identity: graphforge_filesystem::FileIdentity,
    path: &Path,
) -> Result<(), GfError> {
    cas.tmp
        .unlink_child_if_identity(name, identity)
        .map_err(|error| {
            storage(
                "remove stable temporary graph object",
                &cas.diagnostic_root,
                error,
            )
        })?;
    if let Some(allocation) = &cas.allocation {
        allocation.remove_file_at(path)?;
    }
    Ok(())
}
fn validate_sealed_temporary(
    temporary: &SealedTemporaryObject,
    expected_length: u64,
    diagnostic_root: &Path,
) -> Result<(), GfError> {
    let metadata = temporary
        .file
        .metadata()
        .map_err(|error| storage("reinspect fresh graph object", diagnostic_root, error))?;
    let identity = graphforge_filesystem::file_identity(&temporary.file)
        .map_err(|error| storage("reidentify fresh graph object", diagnostic_root, error))?;
    if identity != temporary.identity
        || !metadata.is_file()
        || metadata.len() != expected_length
        || !metadata.permissions().readonly()
    {
        return Err(validation("fresh graph object post-hash authority changed"));
    }
    Ok(())
}

#[cfg(windows)]
fn transition_temporary_to_sealed_reader(
    temporary_directory: &StableDirectory,
    temporary: TemporaryObject,
    digest: &str,
    expected_length: u64,
    diagnostic: &Path,
) -> Result<(SealedTemporaryObject, ReadIoEvidence), GfError> {
    temporary.file.sync_all().map_err(|error| {
        storage(
            "sync temporary graph object before sealing",
            diagnostic,
            error,
        )
    })?;
    let identity = temporary.identity;
    let name = temporary.name;
    let file = temporary_directory
        .seal_cas_child_file(&name, temporary.file)
        .map(graphforge_filesystem::WindowsSealedCasFile::into_file)
        .map_err(|error| storage("reopen sealed temporary graph object", diagnostic, error))?;
    if graphforge_filesystem::file_identity(&file).map_err(|error| {
        storage(
            "reidentify sealed temporary graph object",
            diagnostic,
            error,
        )
    })? != identity
    {
        return Err(validation(
            "temporary graph object identity changed while sealing",
        ));
    }
    let io = verify_file_counted(
        file.try_clone().map_err(|error| {
            storage(
                "clone sealed temporary graph object for authentication",
                diagnostic,
                error,
            )
        })?,
        digest,
        expected_length,
        diagnostic,
    )?;
    Ok((
        SealedTemporaryObject {
            name,
            file,
            identity,
        },
        io,
    ))
}

fn verify_and_seal_graph_object(
    file: &File,
    digest: &str,
    expected_length: u64,
    object_path: &Path,
    diagnostic: &Path,
) -> Result<(), GfError> {
    verify_and_seal_graph_object_counted(file, digest, expected_length, object_path, diagnostic)
        .map(|_| ())
}

fn verify_and_seal_graph_object_counted(
    file: &File,
    digest: &str,
    expected_length: u64,
    object_path: &Path,
    diagnostic: &Path,
) -> Result<ReadIoEvidence, GfError> {
    // Reuse is safe only after the exact opened inode is no longer writable.
    // Hashing first would leave a window in which the already-authenticated
    // bytes could be changed before the subsequent chmod.
    #[cfg(unix)]
    seal_graph_object(file, object_path, diagnostic)?;
    #[cfg(windows)]
    {
        let _ = object_path;
        if !file
            .metadata()
            .map_err(|error| storage("inspect sealed graph object", diagnostic, error))?
            .permissions()
            .readonly()
        {
            return Err(validation("graph object is not canonically sealed"));
        }
    }
    let io = verify_file_counted(
        file.try_clone()
            .map_err(|error| storage("clone graph object for authentication", diagnostic, error))?,
        digest,
        expected_length,
        diagnostic,
    )?;
    if !file
        .metadata()
        .map_err(|error| storage("reinspect sealed graph object", diagnostic, error))?
        .permissions()
        .readonly()
    {
        return Err(validation(
            "graph object became writable during authentication",
        ));
    }
    Ok(io)
}

#[cfg(unix)]
fn seal_graph_object(file: &File, object_path: &Path, diagnostic: &Path) -> Result<(), GfError> {
    let _ = object_path;
    let mut permissions = file
        .metadata()
        .map_err(|error| storage("inspect graph object permissions", diagnostic, error))?
        .permissions();
    permissions.set_readonly(true);
    file.set_permissions(permissions)
        .map_err(|error| storage("seal graph object permissions", diagnostic, error))?;
    Ok(())
}

#[cfg(test)]
mod tests;
