//! installation ownership for immutable graph objects.
use super::{Seek, fs};

use super::BUFFER_BYTES;
use super::CasRoot;
#[cfg(test)]
use super::Digest;
use super::File;
use super::GRAPH_OBJECTS_DIR;
use super::GfError;
use super::GraphObjectInstallEvidence;
use super::GraphObjectPublicationLease;
use super::Path;
use super::Read;
use super::ReadIoEvidence;
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
use super::verify_file_counted_in_domain;
use super::verify_stream_counted_in_domain;
use graphforge_core::hash_observation::HashDomain;

#[cfg(test)]
type CapturedCopyHook = Box<dyn FnMut(&str)>;
#[cfg(test)]
thread_local! {
    static CAPTURED_COPY_HOOK: std::cell::RefCell<Option<CapturedCopyHook>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
pub(crate) fn set_captured_copy_hook(hook: Option<CapturedCopyHook>) {
    CAPTURED_COPY_HOOK.with(|slot| *slot.borrow_mut() = hook);
}
fn captured_copy_boundary(_phase: &str) {
    #[cfg(test)]
    CAPTURED_COPY_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook(_phase);
        }
    });
}

struct InstalledObject {
    evidence: GraphObjectInstallEvidence,
    identity: graphforge_filesystem::FileIdentity,
}

struct TemporaryObject {
    name: std::ffi::OsString,
    #[cfg(unix)]
    file: File,
    #[cfg(windows)]
    file: graphforge_filesystem::WindowsCasWriter,
    identity: graphforge_filesystem::FileIdentity,
    seal: crate::durable_commit::FileSeal,
}

struct SealedTemporaryObject {
    name: std::ffi::OsString,
    file: File,
    identity: graphforge_filesystem::FileIdentity,
    seal: crate::durable_commit::FileSeal,
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
    install_graph_object_bytes_in_domain(lease, bytes, HashDomain::ArtifactPayload)
}

/// Only a validated, bounded radix manifest node selects the control domain.
pub(super) fn install_graph_manifest_node_with_lease(
    lease: &GraphObjectPublicationLease,
    node: &crate::GraphManifestNode,
) -> Result<(String, GraphObjectInstallEvidence), GfError> {
    let bytes = crate::encode_graph_manifest_node(node)?;
    install_graph_object_bytes_in_domain(lease, &bytes, HashDomain::ControlAuthentication)
}

fn install_graph_object_bytes_in_domain(
    lease: &GraphObjectPublicationLease,
    bytes: &[u8],
    domain: HashDomain,
) -> Result<(String, GraphObjectInstallEvidence), GfError> {
    let mut hasher = crate::payload_digest::PayloadSha256::for_domain(domain);
    hasher.update(bytes);
    let digest = hex_digest(hasher.finalize().into());
    let expected_length =
        u64::try_from(bytes.len()).map_err(|_| validation("graph object bytes exceed u64"))?;
    // The name was computed from these exact resident bytes, which are then
    // written and synchronized, so the writer authenticates the temporary as
    // the streamed file install does. Readers admit it by length and XXH64.
    install_object(&lease.cas, &digest, expected_length, domain, true, |file| {
        file.write_all(bytes).map_err(|error| {
            storage(
                "write temporary graph object",
                &lease.cas.diagnostic_root,
                error,
            )
        })?;
        #[cfg(unix)]
        let descriptor = &*file;
        #[cfg(windows)]
        let descriptor = file.as_file();
        let seal = crate::durable_commit::seal_file_witness(descriptor).map_err(|error| {
            storage(
                "fsync temporary graph object",
                &lease.cas.diagnostic_root,
                error,
            )
        })?;
        // The source is already resident memory; only the mandatory temporary
        // file verification below is an application-observed payload read.
        Ok((0, seal))
    })
    .and_then(
        |InstalledObject {
             mut evidence,
             identity,
         }| {
            evidence.content_xxh64 = Some(crate::corruption_checksum::checksum(bytes));
            if evidence.attempted_install {
                evidence.write_bytes = expected_length;
                evidence.write_calls = u64::from(!bytes.is_empty());
                evidence.file_fsync_calls = 1;
                evidence.fsync_calls = evidence
                    .fsync_calls
                    .checked_add(1)
                    .ok_or_else(|| validation("CAS fsync count overflows"))?;
            }
            capture_installed_object(
                lease,
                &digest,
                expected_length,
                identity,
                evidence.content_xxh64,
            )?;
            Ok((digest, evidence))
        },
    )
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
pub(crate) fn install_graph_object_file_with_lease(
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
    let payload_checksum = std::cell::Cell::new(None);
    let result =
        install_object(
            &lease.cas,
            expected_digest,
            expected_length,
            HashDomain::ArtifactPayload,
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
                let mut hasher = crate::payload_digest::PayloadSha256::new();
                let mut checksum = crate::corruption_checksum::Checksum::new();
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
                            checksum.update(&buffer[..read]);
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
                        payload_checksum.set(Some(checksum.finish()));
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
                let seal = {
                    let seal =
                        crate::durable_commit::seal_cache_writer_witness(&mut bounded_output)
                            .map_err(|error| {
                                storage(
                                    "fsync temporary graph object",
                                    &lease.cas.diagnostic_root,
                                    error,
                                )
                            })?;
                    file_sync_calls.set(bounded_output.evidence().sync_operations);
                    seal
                };
                #[cfg(windows)]
                let seal = {
                    let seal = crate::durable_commit::seal_file_witness(output.as_file()).map_err(
                        |error| {
                            storage(
                                "fsync temporary graph object",
                                &lease.cas.diagnostic_root,
                                error,
                            )
                        },
                    )?;
                    file_sync_calls.set(1);
                    seal
                };
                Ok((total, seal))
            },
        );
    result.and_then(
        |InstalledObject {
             mut evidence,
             identity,
         }| {
            if evidence.attempted_install {
                evidence.content_xxh64 = payload_checksum.get();
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
            capture_installed_object(
                lease,
                expected_digest,
                expected_length,
                identity,
                evidence.content_xxh64,
            )?;
            Ok(evidence)
        },
    )
}

/// Only a checkpoint-admitted encoded source can choose checksum authentication.
/// Public file and byte installers remain full SHA trust boundaries.
pub(crate) fn install_captured_encoded_artifact_with_lease(
    lease: &GraphObjectPublicationLease,
    source: &crate::graph_construction::CapturedEncodedArtifact<'_>,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<GraphObjectInstallEvidence, GfError> {
    install_captured_source_with_lease(lease, &CapturedSource::Encoded(source), cancelled)
}

/// Install a workspace file that a capture hashed and kept open, checking the
/// copied bytes against the checksum taken while hashing rather than hashing
/// them with SHA-256 again. Only [`crate::graph_files::capture_workspace_over_parent`]
/// mints the capability; public file and byte installers stay SHA boundaries.
pub(crate) fn install_captured_workspace_file_with_lease(
    lease: &GraphObjectPublicationLease,
    source: &crate::graph_files::CapturedWorkspaceFile,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<GraphObjectInstallEvidence, GfError> {
    install_captured_source_with_lease(lease, &CapturedSource::Workspace(source), cancelled)
}

pub(crate) fn install_captured_portable_source_with_lease(
    lease: &GraphObjectPublicationLease,
    source: &crate::project_portable_v2::CapturedPortableSource<'_>,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<GraphObjectInstallEvidence, GfError> {
    install_captured_source_with_lease(lease, &CapturedSource::Portable(source), cancelled)
}

/// A closed set of concrete, privately minted source capabilities.
enum CapturedSource<'a, 'b> {
    Encoded(&'a crate::graph_construction::CapturedEncodedArtifact<'b>),
    Portable(&'a crate::project_portable_v2::CapturedPortableSource<'b>),
    Workspace(&'a crate::graph_files::CapturedWorkspaceFile),
}
impl CapturedSource<'_, '_> {
    fn kind(&self) -> &'static str {
        match self {
            Self::Encoded(_) => "encoded",
            Self::Portable(_) => "portable",
            Self::Workspace(_) => "workspace",
        }
    }
    fn content_sha256(&self) -> &str {
        match self {
            Self::Encoded(s) => s.content_sha256(),
            Self::Portable(s) => s.content_sha256(),
            Self::Workspace(s) => s.content_sha256(),
        }
    }
    fn bytes(&self) -> u64 {
        match self {
            Self::Encoded(s) => s.bytes(),
            Self::Portable(s) => s.bytes(),
            Self::Workspace(s) => s.bytes(),
        }
    }
    fn checksum(&self) -> u64 {
        match self {
            Self::Encoded(s) => s.checksum(),
            Self::Portable(s) => s.checksum(),
            Self::Workspace(s) => s.checksum(),
        }
    }
    fn source(&self) -> &File {
        match self {
            Self::Encoded(s) => s.source(),
            Self::Portable(s) => s.source(),
            Self::Workspace(s) => s.source(),
        }
    }
    fn revalidate(&self) -> Result<(), GfError> {
        match self {
            Self::Encoded(s) => s.revalidate(),
            Self::Portable(s) => s.revalidate(),
            Self::Workspace(s) => s.revalidate(),
        }
    }
}

fn install_captured_source_with_lease(
    lease: &GraphObjectPublicationLease,
    source: &CapturedSource<'_, '_>,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<GraphObjectInstallEvidence, GfError> {
    source.revalidate()?;
    crate::graph_construction::reject_cancelled(cancelled)?;
    let reads = std::cell::Cell::new(0_u64);
    let writes = std::cell::Cell::new(0_u64);
    let syncs = std::cell::Cell::new(0_u64);
    let captured_identity = lease
        .installed_objects
        .lock()
        .map_err(|_| validation("graph object installation authority poisoned"))?
        .get(source.content_sha256())
        .filter(|capture| {
            capture.byte_length == source.bytes() && capture.content_xxh64 == source.checksum()
        })
        .map(|capture| capture.identity);
    let installed = install_object_admitted(
        &lease.cas,
        source.content_sha256(),
        source.bytes(),
        ObjectAuthentication::CapturedChecksum {
            checksum: source.checksum(),
            identity: captured_identity,
        },
        true,
        |output| copy_captured_source(source, output, cancelled, &reads, &writes, &syncs),
    )?;
    source.revalidate()?;
    let InstalledObject {
        mut evidence,
        identity,
    } = installed;
    if evidence.attempted_install {
        evidence.content_xxh64 = Some(source.checksum());
        evidence.read_calls = evidence
            .read_calls
            .checked_add(reads.get())
            .ok_or_else(|| validation("captured read calls overflow"))?;
        evidence.write_calls = writes.get();
        evidence.write_bytes = source.bytes();
        evidence.file_fsync_calls = syncs.get();
        evidence.fsync_calls = evidence
            .fsync_calls
            .checked_add(syncs.get())
            .ok_or_else(|| validation("captured synchronization calls overflow"))?;
    }
    capture_installed_object(
        lease,
        source.content_sha256(),
        source.bytes(),
        identity,
        evidence.content_xxh64,
    )?;
    Ok(evidence)
}

fn copy_captured_source(
    source: &CapturedSource<'_, '_>,
    output: &mut CasTemporaryWriter,
    cancelled: &mut impl FnMut() -> bool,
    reads: &std::cell::Cell<u64>,
    writes: &std::cell::Cell<u64>,
    syncs: &std::cell::Cell<u64>,
) -> Result<(u64, crate::durable_commit::FileSeal), GfError> {
    let window = graphforge_filesystem::cache_release_window_for_streams(2)
        .map_err(|error| validation(error.to_string()))?;
    let mut input = graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
        source
            .source()
            .try_clone()
            .map_err(|error| validation(error.to_string()))?,
        window,
        graphforge_filesystem::FileCacheReleaseTracker::default(),
    )
    .map_err(|error| validation(error.to_string()))?;
    input
        .rewind()
        .map_err(|error| validation(error.to_string()))?;
    #[cfg(unix)]
    let mut output_stream = graphforge_filesystem::DurableFileCacheWriter::with_window_bytes(
        output
            .try_clone()
            .map_err(|error| validation(error.to_string()))?,
        window,
    )
    .map_err(|error| validation(error.to_string()))?;
    let mut checksum = crate::corruption_checksum::Checksum::new();
    let mut total = 0_u64;
    let mut buffer = vec![0; BUFFER_BYTES];
    let copied = (|| {
        loop {
            crate::graph_construction::reject_cancelled(cancelled)?;
            captured_copy_boundary("before_read");
            let count = input
                .read(&mut buffer)
                .map_err(|error| validation(error.to_string()))?;
            captured_copy_boundary("after_read");
            if count == 0 {
                break;
            }
            total = total
                .checked_add(count as u64)
                .ok_or_else(|| validation("captured source length overflow"))?;
            if total > source.bytes() {
                return Err(validation(format!(
                    "captured {} source grew during copy",
                    source.kind()
                )));
            }
            reads.set(
                reads
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| validation("captured read calls overflow"))?,
            );
            #[cfg(unix)]
            output_stream
                .write_all(&buffer[..count])
                .map_err(|error| validation(error.to_string()))?;
            #[cfg(windows)]
            output
                .write_all(&buffer[..count])
                .map_err(|error| validation(error.to_string()))?;
            writes.set(
                writes
                    .get()
                    .checked_add(1)
                    .ok_or_else(|| validation("captured write calls overflow"))?,
            );
            checksum.update(&buffer[..count]);
        }
        source.revalidate()?;
        if total != source.bytes() || checksum.finish() != source.checksum() {
            return Err(validation(format!(
                "captured {} source checksum or length changed during copy",
                source.kind()
            )));
        }
        Ok(total)
    })();
    let cleanup = input
        .finish()
        .map_err(|error| validation(error.to_string()));
    let total = finish_captured_source_copy(copied, cleanup)?;
    #[cfg(unix)]
    let seal = {
        let seal = crate::durable_commit::seal_cache_writer_witness(&mut output_stream)
            .map_err(|error| validation(error.to_string()))?;
        syncs.set(output_stream.evidence().sync_operations);
        seal
    };
    #[cfg(windows)]
    let seal = {
        let seal = crate::durable_commit::seal_file_witness(output.as_file())
            .map_err(|error| validation(error.to_string()))?;
        syncs.set(1);
        seal
    };
    Ok((total, seal))
}

fn finish_captured_source_copy(
    copied: Result<u64, GfError>,
    cleanup: Result<graphforge_filesystem::FileCacheReleaseEvidence, GfError>,
) -> Result<u64, GfError> {
    match (copied, cleanup) {
        (Ok(total), Ok(_)) => Ok(total),
        (Err(primary), Ok(_)) | (Ok(_), Err(primary)) => Err(primary),
        (Err(primary), Err(cleanup)) => Err(validation(format!(
            "{primary}; captured source cache cleanup also failed: {cleanup}"
        ))),
    }
}

fn capture_installed_object(
    lease: &GraphObjectPublicationLease,
    digest: &str,
    byte_length: u64,
    identity: graphforge_filesystem::FileIdentity,
    content_xxh64: Option<u64>,
) -> Result<(), GfError> {
    let content_xxh64 = content_xxh64
        .ok_or_else(|| validation("authenticated installation lacks its captured checksum"))?;
    let mut captures = lease
        .installed_objects
        .lock()
        .map_err(|_| validation("graph object installation authority poisoned"))?;
    let limits = crate::GraphManifestLimits::default();
    let maximum = limits.max_entries.saturating_add(limits.max_segments);
    if captures.len() < maximum || captures.contains_key(digest) {
        captures.insert(
            digest.to_owned(),
            super::CapturedGraphObject {
                identity,
                byte_length,
                content_xxh64,
            },
        );
    }
    // A full optional capture budget grants no authority: staging falls back to
    // first SHA authentication for uncaptured objects.
    Ok(())
}

#[derive(Clone, Copy)]
enum ObjectAuthentication {
    Sha(HashDomain),
    CapturedChecksum {
        checksum: u64,
        identity: Option<graphforge_filesystem::FileIdentity>,
    },
}

impl ObjectAuthentication {
    #[cfg(windows)]
    fn without_existing_identity(self) -> Self {
        match self {
            Self::Sha(_) => self,
            Self::CapturedChecksum { .. } => Self::Sha(HashDomain::ArtifactPayload),
        }
    }

    fn sha_bytes(self, bytes: u64) -> u64 {
        match self {
            Self::Sha(_) => bytes,
            Self::CapturedChecksum { .. } => 0,
        }
    }
}

fn install_object<F>(
    cas: &CasRoot,
    digest: &str,
    expected_length: u64,
    domain: HashDomain,
    writer_authenticated: bool,
    write_temporary: F,
) -> Result<InstalledObject, GfError>
where
    F: FnOnce(&mut CasTemporaryWriter) -> Result<(u64, crate::durable_commit::FileSeal), GfError>,
{
    install_object_admitted(
        cas,
        digest,
        expected_length,
        ObjectAuthentication::Sha(domain),
        writer_authenticated,
        write_temporary,
    )
}

fn verify_stream_admitted(
    file: &mut impl Read,
    digest: &str,
    expected_length: u64,
    diagnostic: &Path,
    authentication: ObjectAuthentication,
) -> Result<ReadIoEvidence, GfError> {
    let ObjectAuthentication::CapturedChecksum {
        checksum: expected, ..
    } = authentication
    else {
        let ObjectAuthentication::Sha(domain) = authentication else {
            unreachable!()
        };
        return verify_stream_counted_in_domain(file, digest, expected_length, diagnostic, domain);
    };
    let mut checksum = crate::corruption_checksum::Checksum::new();
    let mut io = ReadIoEvidence::default();
    let mut buffer = vec![0; BUFFER_BYTES];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|error| storage("read captured CAS object", diagnostic, error))?;
        if count == 0 {
            break;
        }
        io.bytes = io
            .bytes
            .checked_add(count as u64)
            .ok_or_else(|| validation("captured CAS read bytes overflow"))?;
        io.calls = io
            .calls
            .checked_add(1)
            .ok_or_else(|| validation("captured CAS read calls overflow"))?;
        if io.bytes > expected_length {
            return Err(validation("captured CAS object grew"));
        }
        checksum.update(&buffer[..count]);
    }
    if io.bytes != expected_length || checksum.finish() != expected {
        return Err(validation("captured CAS object checksum or length changed"));
    }
    io.content_xxh64 = Some(expected);
    crate::lifecycle_io::record_read(
        crate::StorageIoPhase::HydrationVerification,
        io.bytes,
        io.calls,
    );
    crate::lifecycle_io::record_objects(crate::StorageIoPhase::HydrationVerification, 1);
    Ok(io)
}

fn verify_file_admitted(
    file: File,
    digest: &str,
    expected_length: u64,
    diagnostic: &Path,
    authentication: ObjectAuthentication,
) -> Result<ReadIoEvidence, GfError> {
    if let ObjectAuthentication::Sha(domain) = authentication {
        return verify_file_counted_in_domain(file, digest, expected_length, diagnostic, domain);
    }
    let identity = graphforge_filesystem::file_identity(&file)
        .map_err(|error| storage("identify captured CAS object", diagnostic, error))?;
    if file
        .metadata()
        .map_err(|error| storage("inspect captured CAS object", diagnostic, error))?
        .len()
        != expected_length
    {
        return Err(validation("captured CAS object length changed"));
    }
    let mut reader = graphforge_filesystem::FileCacheReleasingReader::new(file)
        .map_err(|error| storage("bound captured CAS object", diagnostic, error))?;
    reader
        .rewind()
        .map_err(|error| storage("rewind captured CAS object", diagnostic, error))?;
    let checked = verify_stream_admitted(
        &mut reader,
        digest,
        expected_length,
        diagnostic,
        authentication,
    )
    .and_then(|io| {
        if graphforge_filesystem::file_identity(reader.file())
            .map_err(|error| storage("reidentify captured CAS object", diagnostic, error))?
            != identity
            || reader
                .file()
                .metadata()
                .map_err(|error| storage("reinspect captured CAS object", diagnostic, error))?
                .len()
                != expected_length
        {
            return Err(validation("captured CAS object identity changed"));
        }
        Ok(io)
    });
    let cleanup = reader
        .finish()
        .map_err(|error| storage("release captured CAS cache", diagnostic, error));
    match (checked, cleanup) {
        (Ok(io), Ok(_)) => Ok(io),
        (Err(primary), Ok(_)) | (Ok(_), Err(primary)) => Err(primary),
        (Err(primary), Err(cleanup)) => Err(validation(format!("{primary}; {cleanup}"))),
    }
}

fn install_object_admitted<F>(
    cas: &CasRoot,
    digest: &str,
    expected_length: u64,
    authentication: ObjectAuthentication,
    writer_authenticated: bool,
    write_temporary: F,
) -> Result<InstalledObject, GfError>
where
    F: FnOnce(&mut CasTemporaryWriter) -> Result<(u64, crate::durable_commit::FileSeal), GfError>,
{
    validate_digest(digest)?;
    let bucket = cas.digest_bucket(digest, true)?;
    let destination_name = std::ffi::OsStr::new(&digest[2..]);
    if let Some(evidence) = try_reuse_existing_object(
        cas,
        &bucket,
        destination_name,
        digest,
        expected_length,
        authentication,
    )? {
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
    let temporary_path = temporary_object_path(cas, &temporary_name);
    #[cfg(unix)]
    let observed_file = &temporary;
    #[cfg(windows)]
    let observed_file = temporary.as_file();
    let observed = cas.allocation.as_ref().map_or(Ok(()), |allocation| {
        allocation.replace_file_at(&temporary_path, observed_file)
    });
    let (bytes_hashed, seal) = written?;
    observed?;
    let preseal_io = if writer_authenticated || cfg!(windows) {
        ReadIoEvidence::default()
    } else {
        temporary.rewind().map_err(|error| {
            storage("rewind temporary graph object", &cas.diagnostic_root, error)
        })?;
        verify_stream_admitted(
            &mut temporary,
            digest,
            expected_length,
            &cas.diagnostic_root,
            authentication,
        )?
    };
    // Windows must close the writable handle and reopen an exact-identity,
    // protected read handle before publication. That transition authenticates
    // the complete payload below, so a second pre-seal read would be redundant.
    let (installed, identity, _sealed_bytes_hashed, concurrent_io) = finalize_temporary_object(
        cas,
        &bucket,
        TemporaryObject {
            name: temporary_name,
            file: temporary,
            identity: temporary_identity,
            seal,
        },
        digest,
        expected_length,
        authentication,
    )?;
    Ok(InstalledObject {
        evidence: installation_evidence(
            expected_length,
            installed,
            bytes_hashed,
            authentication,
            preseal_io,
            concurrent_io,
        )?,
        identity,
    })
}

fn installation_evidence(
    expected_length: u64,
    installed: bool,
    bytes_hashed: u64,
    authentication: ObjectAuthentication,
    preseal_io: ReadIoEvidence,
    concurrent_io: ReadIoEvidence,
) -> Result<GraphObjectInstallEvidence, GfError> {
    let read_bytes = [preseal_io.bytes, concurrent_io.bytes]
        .into_iter()
        .try_fold(bytes_hashed, u64::checked_add)
        .ok_or_else(|| validation("graph object read byte count overflows"))?;
    let bytes_hashed = [preseal_io.sha_bytes, concurrent_io.sha_bytes]
        .into_iter()
        .try_fold(authentication.sha_bytes(bytes_hashed), u64::checked_add)
        .ok_or_else(|| validation("graph object hashed byte count overflows"))?;
    let read_calls = preseal_io
        .calls
        .checked_add(concurrent_io.calls)
        .ok_or_else(|| validation("graph object read call count overflows"))?;
    Ok(GraphObjectInstallEvidence {
        content_xxh64: preseal_io.content_xxh64.or(concurrent_io.content_xxh64),
        bytes_hashed,
        checksum_read_bytes: read_bytes
            .checked_sub(bytes_hashed)
            .ok_or_else(|| validation("CAS SHA read count exceeds native reads"))?,
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
        content_xxh64: io.content_xxh64,
        bytes_hashed: io.sha_bytes,
        checksum_read_bytes: io.bytes - io.sha_bytes,
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
    authentication: ObjectAuthentication,
) -> Result<Option<InstalledObject>, GfError> {
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
        authentication,
    )?;
    if let Some(allocation) = &cas.allocation {
        allocation.replace_file_at(&graph_object_path(&cas.diagnostic_root, digest)?, &file)?;
    }
    let identity = graphforge_filesystem::file_identity(&file).map_err(|error| {
        storage(
            "identify authenticated graph object",
            &cas.diagnostic_root,
            error,
        )
    })?;
    Ok(Some(InstalledObject {
        evidence: reused_object_evidence(expected_length, io),
        identity,
    }))
}

#[cfg(windows)]
fn try_reuse_existing_object(
    cas: &CasRoot,
    bucket: &StableDirectory,
    destination_name: &std::ffi::OsStr,
    digest: &str,
    expected_length: u64,
    authentication: ObjectAuthentication,
) -> Result<Option<InstalledObject>, GfError> {
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
            adoption_io = verify_stream_admitted(
                &mut legacy,
                digest,
                expected_length,
                &cas.diagnostic_root,
                authentication.without_existing_identity(),
            )?;
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
        authentication,
    )?;
    let mut evidence = reused_object_evidence(expected_length, io);
    let combined = checked_read_io_sum(adoption_io, io)?;
    evidence.bytes_hashed = combined.sha_bytes;
    evidence.checksum_read_bytes = combined.bytes - combined.sha_bytes;
    evidence.read_calls = adoption_io
        .calls
        .checked_add(io.calls)
        .ok_or_else(|| validation("reused object read call count overflows"))?;
    if let Some(allocation) = &cas.allocation {
        allocation.replace_file_at(&graph_object_path(&cas.diagnostic_root, digest)?, &file)?;
    }
    let identity = graphforge_filesystem::file_identity(&file).map_err(|error| {
        storage(
            "identify authenticated graph object",
            &cas.diagnostic_root,
            error,
        )
    })?;
    Ok(Some(InstalledObject { evidence, identity }))
}

fn finalize_temporary_object(
    cas: &CasRoot,
    bucket: &StableDirectory,
    temporary: TemporaryObject,
    digest: &str,
    expected_length: u64,
    authentication: ObjectAuthentication,
) -> Result<
    (
        bool,
        graphforge_filesystem::FileIdentity,
        u64,
        ReadIoEvidence,
    ),
    GfError,
> {
    let destination_name = std::ffi::OsStr::new(&digest[2..]);
    let temporary_path = temporary_object_path(cas, &temporary.name);
    #[cfg(unix)]
    let (temporary, sealed_io) = {
        seal_graph_object(&temporary.file, &temporary_path, &cas.diagnostic_root)?;
        (
            SealedTemporaryObject {
                name: temporary.name,
                file: temporary.file,
                identity: temporary.identity,
                seal: temporary.seal,
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
        authentication,
    )?;
    if let Some(allocation) = &cas.allocation {
        allocation.replace_file_at(&temporary_path, &temporary.file)?;
    }
    let sealed_bytes_hashed = sealed_io.bytes;
    validate_sealed_temporary(&temporary, expected_length, &cas.diagnostic_root)?;
    returned_error_boundary("install:temp-sealed")?;
    let sealed = crate::durable_commit::SealedArtifact::adopt_sealed(
        &cas.tmp,
        &temporary.name,
        temporary.file,
        temporary.seal,
        cas.allocation.as_ref(),
    )
    .map_err(|error| storage("adopt sealed graph object", &cas.diagnostic_root, error))?;
    let mut concurrent_io = ReadIoEvidence::default();
    let installed = crate::durable_commit::install_immutable(
        sealed,
        bucket,
        destination_name,
        |existing, _identity| {
            concurrent_io = verify_and_seal_graph_object_counted(
                existing,
                digest,
                expected_length,
                &graph_object_path(&cas.diagnostic_root, digest).map_err(std::io::Error::other)?,
                &cas.diagnostic_root,
                authentication,
            )
            .map_err(std::io::Error::other)?;
            Ok(())
        },
        |_reused, file| {
            if let Some(allocation) = &cas.allocation {
                allocation
                    .replace_file_at(
                        &graph_object_path(&cas.diagnostic_root, digest)
                            .map_err(std::io::Error::other)?,
                        file,
                    )
                    .map_err(std::io::Error::other)?;
            }
            returned_error_boundary("install:final-linked").map_err(std::io::Error::other)
        },
        |_reused, _file| {
            returned_error_boundary("install:bucket-synced").map_err(std::io::Error::other)
        },
        || returned_error_boundary("install:temp-unlinked").map_err(std::io::Error::other),
    )
    .map_err(|error| immutable_commit_error(error, cas))?;
    let (_file, identity, reused) = installed;
    let installed = !reused;
    Ok((
        installed,
        identity,
        sealed_bytes_hashed,
        checked_read_io_sum(sealed_io, concurrent_io)?,
    ))
}

fn immutable_commit_error(error: std::io::Error, cas: &CasRoot) -> GfError {
    let message = error.to_string();
    if let Some(cause) = error.into_inner()
        && let Ok(cause) = cause.downcast::<GfError>()
    {
        return *cause;
    }
    storage(
        "commit immutable graph object",
        &cas.diagnostic_root,
        message,
    )
}

fn temporary_object_path(cas: &CasRoot, name: &std::ffi::OsStr) -> std::path::PathBuf {
    cas.diagnostic_root
        .join(GRAPH_OBJECTS_DIR)
        .join(TEMP_DIR)
        .join(name)
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
    authentication: ObjectAuthentication,
) -> Result<(SealedTemporaryObject, ReadIoEvidence), GfError> {
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
    let io = verify_file_admitted(
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
        authentication,
    )?;
    Ok((
        SealedTemporaryObject {
            name,
            file,
            identity,
            seal: temporary.seal,
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
    verify_and_seal_graph_object_counted(
        file,
        digest,
        expected_length,
        object_path,
        diagnostic,
        ObjectAuthentication::Sha(HashDomain::ArtifactPayload),
    )
    .map(|_| ())
}

fn verify_and_seal_graph_object_counted(
    file: &File,
    digest: &str,
    expected_length: u64,
    object_path: &Path,
    diagnostic: &Path,
    authentication: ObjectAuthentication,
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
    let authentication = match authentication {
        ObjectAuthentication::CapturedChecksum { identity, .. }
            if identity
                != Some(graphforge_filesystem::file_identity(file).map_err(|error| {
                    storage("identify existing CAS authority", diagnostic, error)
                })?) =>
        {
            ObjectAuthentication::Sha(HashDomain::ArtifactPayload)
        }
        value => value,
    };
    let io = verify_file_admitted(
        file.try_clone()
            .map_err(|error| storage("clone graph object for authentication", diagnostic, error))?,
        digest,
        expected_length,
        diagnostic,
        authentication,
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
