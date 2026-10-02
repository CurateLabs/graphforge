//! materialization ownership for immutable graph objects.
use super::Seek;

use super::BUFFER_BYTES;
use super::CasRoot;
use super::Component;
use super::Digest;
use super::File;
use super::GfError;
use super::GraphFilesInventory;
use super::GraphFilesOpenEvidence;
use super::GraphFilesOpenStrategy;
use super::GraphObjectIoTotals;
#[cfg(windows)]
use super::OpenOptions;
use super::Path;
#[cfg(not(unix))]
use super::PathBuf;
use super::Read;
use super::ReadIoEvidence;
use super::Sha256;
use super::StableDirectory;
use super::Uuid;
use super::Write;
use super::begin_graph_object_publication;
#[cfg(windows)]
use super::fs;
use super::hex_digest;
use super::read_graph_control_object_counted;
use super::storage;
#[cfg(windows)]
use super::validate_directory_identity;
use super::validate_logical_path;
use super::validation;

/// Materialize a verified logical inventory into a private graph tree. Ordinary
/// immutable payloads reuse CAS inodes; the v4 ordinal authority facet is
/// copied into single-link files because its reader intentionally rejects
/// shared inodes. The target must be empty.
pub fn materialize_graph_objects(
    root: &Path,
    inventory: &GraphFilesInventory,
    target: &Path,
) -> Result<GraphFilesOpenEvidence, GfError> {
    let lease = begin_graph_object_publication(root)?;
    let mut route_io = GraphObjectIoTotals::default();
    let routes =
        crate::route_component::materialize::MaterializationRoutes::prepare(inventory, |entry| {
            read_graph_control_object_counted(
                root,
                &entry.content_sha256,
                64 * 1024 * 1024,
                &mut route_io,
            )
        })?;
    let _target_guard = open_empty_materialization_target(target)?;
    let target_directory = StableDirectory::open(target)
        .map_err(|error| storage("retain stable materialization target", target, error))?;
    let admission_root = target
        .canonicalize()
        .map_err(|error| storage("resolve stable materialization target", target, error))?;
    let admission = crate::graph_admission::AdmissionBatch::begin();
    let mut evidence = GraphFilesOpenEvidence {
        strategy: GraphFilesOpenStrategy::PrivateMaterialize,
        files_validated: inventory.file_count,
        // Length-validated: hard-linked payloads are checksummed on first
        // touch (`graph_admission`), not here. Control files that are copied
        // below are checksummed as they are copied and counted in
        // `bytes_checksummed`.
        bytes_validated: inventory.total_byte_length,
        application_read_bytes: route_io.read_bytes,
        application_read_calls: route_io.read_calls,
        ..GraphFilesOpenEvidence::default()
    };
    for (entry, destination) in inventory.files.iter().zip(&routes.destinations) {
        let mut translated = entry.clone();
        translated.relative_path.clone_from(destination);
        let copy_route =
            routes.legacy && crate::route_component::route_position(destination)?.is_some();
        let materialized = materialize_from_cas(
            &lease.cas,
            &target_directory,
            &translated,
            copy_route,
            admission,
            &admission_root,
        )?;
        evidence.bytes_checksummed = evidence
            .bytes_checksummed
            .checked_add(materialized.checksummed_bytes)
            .ok_or_else(|| validation("object hydration checksummed byte count overflows"))?;
        evidence.application_read_bytes = evidence
            .application_read_bytes
            .checked_add(materialized.read_bytes)
            .ok_or_else(|| validation("object hydration read byte count overflows"))?;
        evidence.application_read_calls = evidence
            .application_read_calls
            .checked_add(materialized.read_calls)
            .ok_or_else(|| validation("object hydration read call count overflows"))?;
        evidence.application_write_bytes = evidence
            .application_write_bytes
            .checked_add(materialized.write_bytes)
            .ok_or_else(|| validation("object hydration write byte count overflows"))?;
        evidence.application_write_calls = evidence
            .application_write_calls
            .checked_add(materialized.write_calls)
            .ok_or_else(|| validation("object hydration write call count overflows"))?;
        evidence.fsync_calls = evidence
            .fsync_calls
            .checked_add(materialized.fsync_calls)
            .ok_or_else(|| validation("object hydration fsync count overflows"))?;
        evidence.file_fsync_calls = evidence
            .file_fsync_calls
            .checked_add(materialized.file_fsync_calls)
            .ok_or_else(|| validation("object hydration file barrier count overflows"))?;
        evidence.directory_fsync_calls = evidence
            .directory_fsync_calls
            .checked_add(materialized.directory_fsync_calls)
            .ok_or_else(|| validation("object hydration directory barrier count overflows"))?;
        if materialized.copied {
            evidence.files_copied = evidence
                .files_copied
                .checked_add(1)
                .ok_or_else(|| validation("object hydration copied-file count overflows"))?;
            evidence.bytes_copied = evidence
                .bytes_copied
                .checked_add(entry.byte_length)
                .ok_or_else(|| validation("object hydration copied-byte count overflows"))?;
        } else {
            evidence.files_reused = evidence
                .files_reused
                .checked_add(1)
                .ok_or_else(|| validation("object hydration reused-file count overflows"))?;
            evidence.bytes_reused = evidence
                .bytes_reused
                .checked_add(entry.byte_length)
                .ok_or_else(|| validation("object hydration reused-byte count overflows"))?;
        }
    }
    routes.install_table(target, &mut evidence)?;
    lease.revalidate_for_publish()?;
    Ok(evidence)
}

/// Explain the one reachable cross-volume hardlink failure instead of a
/// truncated `Invalid cross-device link`: hydration hard links content-store
/// objects, and a link cannot cross filesystems, so a reader workspace under a
/// `TMPDIR` on another volume fail-closes the commit. The guidance must stay
/// short enough to survive publication-error redaction and free of paths,
/// which redaction strips.
fn materialization_install_error(error: std::io::Error, root: &Path) -> GfError {
    if error.kind() == std::io::ErrorKind::CrossesDevices {
        return GfError::Storage(
            "hard link crossed filesystems (EXDEV); set TMPDIR onto the project volume".to_string(),
        );
    }
    storage("install stable materialized object", root, error)
}

fn materialize_from_cas(
    cas: &CasRoot,
    target: &StableDirectory,
    entry: &crate::GraphFileEntry,
    copy_route: bool,
    admission: crate::graph_admission::AdmissionBatch,
    admission_root: &Path,
) -> Result<MaterializeIoEvidence, GfError> {
    validate_logical_path(Path::new(&entry.relative_path))?;
    let bucket = cas.digest_bucket(&entry.content_sha256, false)?;
    let source_name = std::ffi::OsStr::new(&entry.content_sha256[2..]);
    let source = bucket
        .open_child_file(source_name)
        .map_err(|error| storage("open materialization source", &cas.diagnostic_root, error))?;
    let source_identity = graphforge_filesystem::file_identity(&source).map_err(|error| {
        storage(
            "identify materialization source",
            &cas.diagnostic_root,
            error,
        )
    })?;
    let path = Path::new(&entry.relative_path);
    let mut parent: Option<StableDirectory> = None;
    let mut components = path.components().peekable();
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(validation("invalid materialization path component"));
        };
        if components.peek().is_some() {
            let directory = parent.as_ref().unwrap_or(target);
            parent = Some(directory.create_child_directory(name).map_err(|error| {
                storage(
                    "create stable materialization directory",
                    &cas.diagnostic_root,
                    error,
                )
            })?);
        } else {
            let parent = parent.as_ref().unwrap_or(target);
            if copy_route || requires_single_link_materialization(&entry.relative_path) {
                return copy_single_link_materialized_object(cas, &source, parent, name, entry);
            }
            let (installed, installed_identity) = bucket
                .link_child_into(source_name, &source, source_identity, parent, name)
                .map_err(|error| materialization_install_error(error, &cas.diagnostic_root))?;
            let verified = verify_hardlinked_materialization(
                &installed,
                installed_identity,
                source_identity,
                entry.byte_length,
                &cas.diagnostic_root,
            );
            match verified {
                Ok(()) => {
                    use crate::graph_admission::PayloadClass;
                    // The link is the content-store inode.
                    let mut io = MaterializeIoEvidence::default();
                    match crate::graph_admission::classify(&entry.relative_path) {
                        // Bulk data registers a first-touch ticket; nothing
                        // here reads it.
                        PayloadClass::FirstTouch => admission.register(
                            installed_identity,
                            entry,
                            super::graph_object_path(&cas.diagnostic_root, &entry.content_sha256)?,
                            admission_root,
                            admission_root.join(&entry.relative_path),
                        ),
                        PayloadClass::SelfAuthenticating => {}
                        // Small sidecars have too many by-name readers to
                        // enumerate, so they are checked now.
                        PayloadClass::Eager => {
                            match crate::graph_admission::admit_now(
                                &installed,
                                entry,
                                &cas.diagnostic_root,
                            ) {
                                // Recorded by the check itself under
                                // `read_path_scan`; `read_bytes` here would
                                // report it again as hydration copy I/O.
                                Ok(_) => io.checksummed_bytes = entry.byte_length,
                                Err(error) => {
                                    let _ =
                                        parent.unlink_child_if_identity(name, installed_identity);
                                    return Err(error);
                                }
                            }
                        }
                    }
                    return Ok(io);
                }
                Err(error) => {
                    let _ = parent.unlink_child_if_identity(name, installed_identity);
                    return Err(error);
                }
            }
        }
    }
    Err(validation("materialization path has no final component"))
}

/// Authenticate a just-installed hardlink without re-reading its bytes.
///
/// `bucket.link_child_into` above already confirmed, via `file_identity`,
/// that the newly-created directory entry resolves to the exact same inode
/// as `source`. That is an identity guarantee, not a content one: rewriting
/// a file in place (same length, same inode) is invisible to it and to the
/// checks below.
///
/// Opening a generation no longer checksums its payloads: its inventory is
/// admitted by presence and exact length only, and each hard link registers a
/// first-touch ticket or is checked in `materialize_from_cas` according to
/// `graph_admission::classify`. Inode identity alone cannot detect an
/// in-place byte mutation, so the topology and all-role mutation tests in
/// `workspace_hydration/tests.rs` remain required. SHA-256 still names the
/// object and remains required at installation and trust boundaries.
/// This keeps the checks that stay meaningful after the hardlink: identity
/// (defense in depth — cheap, `stat`-only, even though `link_child_into`
/// already enforced it), declared length (mirrors `verify_file_counted`'s
/// own pre-hash guard, `graph_object_store.rs`'s `verify_file_counted`), and
/// link count (confirms the destination really is a second name for the CAS
/// inode rather than some other single-link file that happened to be
/// substituted in the window between `link_child_into`'s internal checks and
/// this one).
fn verify_hardlinked_materialization(
    installed: &File,
    installed_identity: graphforge_filesystem::FileIdentity,
    source_identity: graphforge_filesystem::FileIdentity,
    expected_length: u64,
    diagnostic: &Path,
) -> Result<(), GfError> {
    if installed_identity != source_identity {
        return Err(validation(
            "materialized graph object is not the hard-linked CAS object",
        ));
    }
    let metadata = installed
        .metadata()
        .map_err(|error| storage("inspect materialized graph object", diagnostic, error))?;
    if !metadata.is_file() || metadata.len() != expected_length {
        return Err(validation(
            "materialized graph object length does not match manifest",
        ));
    }
    let link_count = graphforge_filesystem::file_link_count(installed)
        .map_err(|error| storage("inspect materialized graph object links", diagnostic, error))?;
    if link_count < 2 {
        return Err(validation(
            "materialized graph object was not hard-linked from the CAS store",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MaterializeIoEvidence {
    copied: bool,
    checksummed_bytes: u64,
    read_bytes: u64,
    read_calls: u64,
    write_bytes: u64,
    write_calls: u64,
    fsync_calls: u64,
    file_fsync_calls: u64,
    directory_fsync_calls: u64,
}

/// Files of a search or embedding artifact that the lifecycle replaces by name
/// or opens for writing, so they cannot share a content-store inode:
///
/// - `current.json` is an artifact pointer, and `mutations.json` is the
///   embedding mutation journal; each is published with a replacing rename,
///   which refuses a multiply linked target.
/// - `.tantivy-meta.lock` is flocked by every Tantivy reader, opened
///   `O_WRONLY|O_CREAT`. It is empty, so every empty lock in every project and
///   every index would otherwise be one content-store object: a read-only
///   inode that cannot be opened for writing and, if it could, a lock shared
///   by unrelated indexes. A reader of a compact text index failed with
///   `EACCES` here.
///
/// - An embedding artifact's `.writer.lock` is opened for writing by every
///   upsert. New publications exclude it from the inventory
///   (`graph_files.rs`); a manifest published before that exclusion matched the
///   real layout still lists it, and is copied here instead of linked.
///
/// The segments, manifests and `meta.json` stay shared and are admitted on
/// first touch. The Tantivy writer lock is not copied: a build writes a new
/// version directory and creates its own.
fn is_mutable_artifact_control(relative_path: &str) -> bool {
    let name = Path::new(relative_path)
        .file_name()
        .and_then(std::ffi::OsStr::to_str);
    if relative_path.starts_with("indexes/search/") {
        matches!(name, Some("current.json" | ".tantivy-meta.lock"))
    } else if relative_path.starts_with("embeddings/") {
        matches!(
            name,
            Some(
                "catalog.json" | "active.json" | "current.json" | "mutations.json" | ".writer.lock"
            )
        )
    } else {
        false
    }
}

fn requires_single_link_materialization(relative_path: &str) -> bool {
    if relative_path == crate::route_component::TABLE_FILE
        || is_mutable_artifact_control(relative_path)
    {
        return true;
    }
    let Some(name) = relative_path.strip_prefix("topology/uuid-membership/") else {
        return false;
    };
    !name.contains('/')
        && (matches!(
            name,
            "manifest.json"
                | "topology-receipt.json"
                | "ordinal-v4-manifest.json"
                | "ordinal-v4-receipt.json"
                | "ordinal-v4.lock"
        ) || (!name.starts_with(".v4-")
            // Node-linear identity runs are immutable and read-only; they are
            // hard-linked and authenticated block by block (#1388).
            && !name.starts_with("forward-v4-")
            && !name.starts_with("ordinal-v4-")
            && crate::uuid_membership::is_exact_private_v4_name(name)))
}

#[allow(clippy::too_many_lines)] // One guarded publication owns copy, release, install, and verification cleanup.
fn copy_single_link_materialized_object(
    cas: &CasRoot,
    source: &File,
    parent: &StableDirectory,
    name: &std::ffi::OsStr,
    entry: &crate::GraphFileEntry,
) -> Result<MaterializeIoEvidence, GfError> {
    let temporary_name = std::ffi::OsString::from(format!(
        ".graph-control-materialize-{}.tmp",
        Uuid::new_v4().simple()
    ));
    let input = source
        .try_clone()
        .map_err(|error| storage("clone materialization source", &cas.diagnostic_root, error))?;
    let cache_window =
        graphforge_filesystem::cache_release_window_for_streams(2).map_err(|error| {
            storage(
                "derive materialization cache budget",
                &cas.diagnostic_root,
                error,
            )
        })?;
    let mut input = graphforge_filesystem::FileCacheReleasingReader::with_window_bytes(
        input,
        cache_window,
        graphforge_filesystem::FileCacheReleaseTracker::default(),
    )
    .map_err(|error| {
        storage(
            "open bounded materialization source",
            &cas.diagnostic_root,
            error,
        )
    })?;
    input
        .rewind()
        .map_err(|error| storage("rewind materialization source", &cas.diagnostic_root, error))?;
    let output = parent
        .create_replaceable_child_file(&temporary_name)
        .map_err(|error| {
            storage(
                "create private materialization file",
                &cas.diagnostic_root,
                error,
            )
        })?;
    let output_identity = graphforge_filesystem::file_identity(&output).map_err(|error| {
        storage(
            "identify private materialization file",
            &cas.diagnostic_root,
            error,
        )
    })?;
    let mut output =
        graphforge_filesystem::DurableFileCacheWriter::with_window_bytes(output, cache_window)
            .map_err(|error| {
                storage(
                    "open bounded private materialization file",
                    &cas.diagnostic_root,
                    error,
                )
            })?;
    let result = (|| -> Result<MaterializeIoEvidence, GfError> {
        let copied = copy_and_authenticate_materialized_object(
            &mut input,
            &mut output,
            entry,
            &cas.diagnostic_root,
        );
        let released = input.finish().map_err(|error| {
            storage(
                "release materialization source cache",
                &cas.diagnostic_root,
                error,
            )
        });
        let mut io = match (copied, released) {
            (Ok(io), Ok(_)) => io,
            (Ok(_), Err(error)) => return Err(error),
            (Err(primary), Ok(_)) => return Err(primary),
            (Err(primary), Err(release)) => {
                return Err(storage(
                    "copy and release private materialization source",
                    &cas.diagnostic_root,
                    format!("{primary}; cache release also failed: {release}"),
                ));
            }
        };
        let seal =
            crate::durable_commit::seal_cache_writer_witness(&mut output).map_err(|error| {
                storage(
                    "sync private materialization file",
                    &cas.diagnostic_root,
                    error,
                )
            })?;
        io.fsync_calls = output.evidence().sync_operations;
        io.file_fsync_calls = io.fsync_calls;
        // Close the cache writer before admitting the exact sealed source. The
        // publisher descriptor permits Windows rename and the acknowledgment
        // fence; immutable readers keep their stricter sharing policy.
        drop(output.into_file());
        let file = crate::durable_commit::open_publisher(parent, &temporary_name, seal.identity())
            .map_err(|error| {
                storage(
                    "reopen private materialization file",
                    &cas.diagnostic_root,
                    error,
                )
            })?;
        crate::durable_commit::SealedArtifact::adopt_sealed(
            parent,
            &temporary_name,
            file,
            seal,
            cas.allocation.as_ref(),
        )
        .map_err(|error| {
            storage(
                "adopt private materialization file",
                &cas.diagnostic_root,
                error,
            )
        })?
        .make_visible(name, crate::durable_commit::PublishMode::Replace, || Ok(()))
        .and_then(|pending| pending.acknowledge(cas.allocation.as_ref()))
        .map_err(|error| {
            storage(
                "commit private materialization file",
                &cas.diagnostic_root,
                error,
            )
        })?;
        io.fsync_calls = io
            .fsync_calls
            .checked_add(1)
            .ok_or_else(|| validation("object hydration fsync count overflows"))?;
        io.directory_fsync_calls = io
            .directory_fsync_calls
            .checked_add(1)
            .ok_or_else(|| validation("object hydration directory barrier count overflows"))?;
        let installed = parent.open_child_file(name).map_err(|error| {
            storage(
                "open private materialization file",
                &cas.diagnostic_root,
                error,
            )
        })?;
        if graphforge_filesystem::file_link_count(&installed).map_err(|error| {
            storage(
                "inspect private materialization links",
                &cas.diagnostic_root,
                error,
            )
        })? != 1
        {
            return Err(validation(
                "private materialization file is multiply linked",
            ));
        }
        let verified = checksum_materialized_file(installed, entry, &cas.diagnostic_root)?;
        add_materialization_verification(&mut io, verified)?;
        io.checksummed_bytes = io.read_bytes;
        io.copied = true;
        Ok(io)
    })();
    if result.is_err() {
        // A failed namespace operation may already have installed the target.
        // Only the exact preparation name remains safe for local cleanup.
        let _ = crate::durable_commit::retire_files(
            parent,
            [(temporary_name.as_os_str(), output_identity)],
        );
    }
    result
}

fn add_materialization_verification(
    io: &mut MaterializeIoEvidence,
    verified: ReadIoEvidence,
) -> Result<(), GfError> {
    io.read_bytes = io
        .read_bytes
        .checked_add(verified.bytes)
        .ok_or_else(|| validation("object hydration verification bytes overflow"))?;
    io.read_calls = io
        .read_calls
        .checked_add(verified.calls)
        .ok_or_else(|| validation("object hydration verification calls overflow"))?;
    Ok(())
}

fn copy_and_authenticate_materialized_object(
    input: &mut impl Read,
    output: &mut impl Write,
    entry: &crate::GraphFileEntry,
    diagnostic_root: &Path,
) -> Result<MaterializeIoEvidence, GfError> {
    let bound = entry
        .byte_length
        .checked_add(1)
        .ok_or_else(|| validation("materialization length overflow"))?;
    let mut bounded = input.take(bound);
    let mut digest = crate::corruption_checksum::Checksum::new();
    let mut length = 0_u64;
    let mut read_calls = 0_u64;
    let mut write_calls = 0_u64;
    let mut buffer = vec![0_u8; BUFFER_BYTES].into_boxed_slice();
    loop {
        let read = bounded
            .read(&mut buffer)
            .map_err(|error| storage("read materialization source", diagnostic_root, error))?;
        if read == 0 {
            break;
        }
        read_calls = read_calls
            .checked_add(1)
            .ok_or_else(|| validation("object hydration read calls overflow"))?;
        output.write_all(&buffer[..read]).map_err(|error| {
            storage("write private materialization file", diagnostic_root, error)
        })?;
        write_calls = write_calls
            .checked_add(1)
            .ok_or_else(|| validation("object hydration write calls overflow"))?;
        digest.update(&buffer[..read]);
        length = length
            .checked_add(read as u64)
            .ok_or_else(|| validation("private materialization length overflows"))?;
    }
    // The copy's source read is application I/O of the opening phase.
    crate::lifecycle_io::record_read(
        crate::StorageIoPhase::HydrationVerification,
        length,
        read_calls,
    );
    if length != entry.byte_length || digest.finish() != entry.content_xxh64 {
        return Err(validation(
            "private materialization bytes do not match inventory",
        ));
    }
    Ok(MaterializeIoEvidence {
        copied: true,
        checksummed_bytes: 0,
        read_bytes: length,
        read_calls,
        write_bytes: length,
        write_calls,
        fsync_calls: 0,
        file_fsync_calls: 0,
        directory_fsync_calls: 0,
    })
}

fn checksum_materialized_file(
    file: File,
    entry: &crate::GraphFileEntry,
    diagnostic_root: &Path,
) -> Result<ReadIoEvidence, GfError> {
    if file
        .metadata()
        .map_err(|error| storage("inspect private materialization", diagnostic_root, error))?
        .len()
        != entry.byte_length
    {
        return Err(validation(
            "private materialization length does not match inventory",
        ));
    }
    let identity = graphforge_filesystem::file_identity(&file)
        .map_err(|error| storage("identify private materialization", diagnostic_root, error))?;
    let read_bound = entry
        .byte_length
        .checked_add(1)
        .ok_or_else(|| validation("private checksum length overflow"))?;
    let mut reader =
        graphforge_filesystem::FileCacheReleasingReader::new(file).map_err(|error| {
            storage(
                "bound private materialization verification",
                diagnostic_root,
                error,
            )
        })?;
    let mut checksum = crate::corruption_checksum::Checksum::new();
    let mut io = ReadIoEvidence::default();
    let mut buffer = vec![0_u8; 64 * 1024];
    let checked = (|| {
        let mut bounded = (&mut reader).take(read_bound);
        loop {
            let read = bounded.read(&mut buffer).map_err(|error| {
                storage("verify private materialization", diagnostic_root, error)
            })?;
            if read == 0 {
                break;
            }
            io.bytes = io
                .bytes
                .checked_add(read as u64)
                .ok_or_else(|| validation("private verification bytes overflow"))?;
            io.calls += 1;
            checksum.update(&buffer[..read]);
        }
        if io.bytes != entry.byte_length
            || checksum.finish() != entry.content_xxh64
            || reader
                .file()
                .metadata()
                .map_err(|error| {
                    storage("reinspect private materialization", diagnostic_root, error)
                })?
                .len()
                != entry.byte_length
            || graphforge_filesystem::file_identity(reader.file()).map_err(|error| {
                storage("reidentify private materialization", diagnostic_root, error)
            })? != identity
        {
            return Err(validation(
                "private materialization checksum does not match inventory",
            ));
        }
        Ok(io)
    })();
    let released = reader
        .finish()
        .map_err(|error| storage("release private verification cache", diagnostic_root, error));
    match (checked, released) {
        (Ok(io), Ok(_)) => {
            // The verification re-read of the installed private copy.
            crate::lifecycle_io::record_read(
                crate::StorageIoPhase::HydrationVerification,
                io.bytes,
                io.calls,
            );
            Ok(io)
        }
        (Err(primary), Ok(_)) | (Ok(_), Err(primary)) => Err(primary),
        (Err(primary), Err(cleanup)) => Err(validation(format!("{primary}; {cleanup}"))),
    }
}

#[cfg(unix)]
fn open_empty_materialization_target(target: &Path) -> Result<std::os::fd::OwnedFd, GfError> {
    use rustix::fs::{Mode, OFlags};

    let parent = target
        .parent()
        .ok_or_else(|| validation("graph object target has no parent"))?;
    let name = target
        .file_name()
        .ok_or_else(|| validation("graph object target has no final component"))?;
    let canonical_parent = parent
        .canonicalize()
        .map_err(|error| storage("resolve graph object target parent", parent, error))?;
    let parent_fd = open_directory_no_follow(&canonical_parent)?;
    match rustix::fs::mkdirat(&parent_fd, name, Mode::from_bits_truncate(0o700)) {
        Ok(()) | Err(rustix::io::Errno::EXIST) => {}
        Err(error) => return Err(storage("create graph object target", target, error)),
    }
    let directory = rustix::fs::openat(
        &parent_fd,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| {
        storage(
            "open graph object target without following links",
            target,
            error,
        )
    })?;
    if target
        .read_dir()
        .map_err(|error| storage("read graph object target", target, error))?
        .next()
        .is_some()
    {
        return Err(validation(
            "graph object materialization target is not empty",
        ));
    }
    Ok(directory)
}

#[cfg(unix)]
fn open_directory_no_follow(path: &Path) -> Result<std::os::fd::OwnedFd, GfError> {
    use rustix::fs::{Mode, OFlags};

    let mut directory = rustix::fs::open(
        if path.is_absolute() {
            Path::new("/")
        } else {
            Path::new(".")
        },
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| storage("open graph object directory root", path, error))?;
    for component in path.components() {
        let Component::Normal(name) = component else {
            continue;
        };
        directory = rustix::fs::openat(
            &directory,
            name,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| {
            storage(
                "open graph object directory without following links",
                path,
                error,
            )
        })?;
    }
    Ok(directory)
}

#[cfg(unix)]
#[allow(dead_code)]
fn link_materialized_object(
    target: &std::os::fd::OwnedFd,
    source: &Path,
    relative: &str,
    expected_digest: &str,
    expected_length: u64,
) -> Result<(), GfError> {
    use rustix::fs::{AtFlags, Mode, OFlags};

    let path = Path::new(relative);
    validate_logical_path(path)?;
    let mut directory = target
        .try_clone()
        .map_err(|error| storage("clone graph object target", path, error))?;
    let mut components = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(name) => Some(name),
            _ => None,
        })
        .peekable();
    while let Some(component) = components.next() {
        if components.peek().is_none() {
            rustix::fs::linkat(
                rustix::fs::CWD,
                source,
                &directory,
                component,
                AtFlags::empty(),
            )
            .map_err(|error| storage("link logical graph object", path, error))?;
            let verified = (|| {
                let linked = rustix::fs::openat(
                    &directory,
                    component,
                    OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(|error| {
                    storage(
                        "open materialized object without following links",
                        path,
                        error,
                    )
                })?;
                let mut file: File = linked.into();
                let metadata = file
                    .metadata()
                    .map_err(|error| storage("inspect materialized object", path, error))?;
                if !metadata.is_file() || metadata.len() != expected_length {
                    return Err(validation("materialized graph object identity is invalid"));
                }
                let mut hasher = Sha256::new();
                let mut buffer = vec![0_u8; BUFFER_BYTES];
                loop {
                    let read = file
                        .read(&mut buffer)
                        .map_err(|error| storage("verify materialized object", path, error))?;
                    if read == 0 {
                        break;
                    }
                    hasher.update(&buffer[..read]);
                }
                if hex_digest(hasher.finalize().into()) != expected_digest {
                    return Err(validation("materialized graph object digest mismatch"));
                }
                Ok(())
            })();
            if verified.is_err() {
                let _ = rustix::fs::unlinkat(&directory, component, AtFlags::empty());
            }
            return verified;
        }
        match rustix::fs::mkdirat(&directory, component, Mode::from_bits_truncate(0o700)) {
            Ok(()) | Err(rustix::io::Errno::EXIST) => {}
            Err(error) => {
                return Err(storage(
                    "create logical graph object directory",
                    path,
                    error,
                ));
            }
        }
        directory = rustix::fs::openat(
            &directory,
            component,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| {
            storage(
                "open logical graph object directory without following links",
                path,
                error,
            )
        })?;
    }
    Err(validation("graph object logical path is empty"))
}

#[cfg(windows)]
struct WindowsMaterializationTarget {
    path: PathBuf,
    _guards: Vec<File>,
}

#[cfg(windows)]
fn open_empty_materialization_target(
    target: &Path,
) -> Result<WindowsMaterializationTarget, GfError> {
    let parent = target
        .parent()
        .ok_or_else(|| validation("graph object target has no parent"))?;
    let mut guards = windows_directory_guards(parent)?;
    match fs::create_dir(target) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => {
            return Err(storage(
                "create graph object materialization target",
                target,
                error,
            ));
        }
    }
    let target_handle = crate::filesystem_admission::open_directory_handle(target)
        .map_err(|error| storage("open graph object materialization target", target, error))?;
    validate_directory_identity(&target_handle, target)?;
    guards.push(target_handle);
    if target
        .read_dir()
        .map_err(|error| storage("read graph object target", target, error))?
        .next()
        .is_some()
    {
        return Err(validation(
            "graph object materialization target is not empty",
        ));
    }
    Ok(WindowsMaterializationTarget {
        path: target.to_path_buf(),
        _guards: guards,
    })
}

#[cfg(windows)]
fn windows_directory_guards(path: &Path) -> Result<Vec<File>, GfError> {
    use std::os::windows::fs::MetadataExt as _;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
    let mut paths = path.ancestors().collect::<Vec<_>>();
    paths.reverse();
    let mut guards = Vec::with_capacity(paths.len());
    for component in paths {
        let metadata = fs::symlink_metadata(component)
            .map_err(|error| storage("inspect materialization directory", component, error))?;
        if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(validation("materialization path contains a reparse point"));
        }
        let guard = crate::filesystem_admission::open_directory_handle(component)
            .map_err(|error| storage("retain materialization directory", component, error))?;
        validate_directory_identity(&guard, component)?;
        guards.push(guard);
    }
    Ok(guards)
}

#[cfg(windows)]
fn windows_create_guarded_directories(
    root: &Path,
    relative_parent: &Path,
) -> Result<Vec<File>, GfError> {
    let mut path = root.to_path_buf();
    let mut guards = Vec::new();
    for component in relative_parent.components() {
        let Component::Normal(name) = component else {
            return Err(validation("invalid materialization directory component"));
        };
        path.push(name);
        match fs::create_dir(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(storage("create materialization directory", &path, error)),
        }
        let guard = crate::filesystem_admission::open_directory_handle(&path)
            .map_err(|error| storage("retain materialization directory", &path, error))?;
        validate_directory_identity(&guard, &path)?;
        guards.push(guard);
    }
    Ok(guards)
}

#[cfg(all(not(unix), not(windows)))]
fn open_empty_materialization_target(target: &Path) -> Result<PathBuf, GfError> {
    let _ = target;
    Err(validation(
        "graph object materialization is unsupported on this platform",
    ))
}

#[cfg(windows)]
#[allow(dead_code)]
fn link_materialized_object(
    target: &WindowsMaterializationTarget,
    source: &Path,
    relative: &str,
    expected_digest: &str,
    expected_length: u64,
) -> Result<(), GfError> {
    use std::os::windows::fs::OpenOptionsExt as _;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    let destination = target.path.join(relative);
    let relative_parent = Path::new(relative)
        .parent()
        .ok_or_else(|| validation("materialized object has no parent"))?;
    let _guards = windows_create_guarded_directories(&target.path, relative_parent)?;
    let source_file = OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(source)
        .map_err(|error| {
            storage(
                "open graph object source without following reparse points",
                source,
                error,
            )
        })?;
    let source_identity = graphforge_filesystem::file_identity(&source_file)
        .map_err(|error| storage("inspect graph object source identity", source, error))?;
    if source_identity
        != graphforge_filesystem::path_identity(source)
            .map_err(|error| storage("inspect graph object source identity", source, error))?
    {
        return Err(validation("graph object source identity changed"));
    }
    fs::hard_link(source, &destination)
        .map_err(|error| storage("link logical graph object", &destination, error))?;
    let verified = (|| {
        let mut linked = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(&destination)
            .map_err(|error| storage("open materialized object", &destination, error))?;
        let metadata = linked
            .metadata()
            .map_err(|error| storage("inspect materialized object", &destination, error))?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() != expected_length
        {
            return Err(validation("materialized graph object identity is invalid"));
        }
        let linked_identity = graphforge_filesystem::file_identity(&linked).map_err(|error| {
            storage("inspect materialized object identity", &destination, error)
        })?;
        if linked_identity != source_identity
            || linked_identity
                != graphforge_filesystem::path_identity(&destination).map_err(|error| {
                    storage("inspect materialized object identity", &destination, error)
                })?
            || source_identity
                != graphforge_filesystem::path_identity(source).map_err(|error| {
                    storage("revalidate graph object source identity", source, error)
                })?
        {
            return Err(validation("materialized graph object identity changed"));
        }
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; BUFFER_BYTES];
        loop {
            let read = linked
                .read(&mut buffer)
                .map_err(|error| storage("verify materialized object", &destination, error))?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        (hex_digest(hasher.finalize().into()) == expected_digest)
            .then_some(())
            .ok_or_else(|| validation("materialized graph object digest mismatch"))
    })();
    if let Err(error) = verified {
        let _ = fs::remove_file(&destination);
        return Err(error);
    }
    Ok(())
}

#[cfg(all(not(unix), not(windows)))]
#[allow(dead_code)]
fn link_materialized_object(
    _target: &PathBuf,
    _source: &Path,
    _relative: &str,
    _expected_digest: &str,
    _expected_length: u64,
) -> Result<(), GfError> {
    Err(validation(
        "graph object materialization is unsupported on this platform",
    ))
}

#[cfg(test)]
mod tests;
