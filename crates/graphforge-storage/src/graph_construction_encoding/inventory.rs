//! Checksum admission of checkpoint-authenticated encoded inventory.

use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::{Component, Path};

use super::{
    account_cache_release, add_evidence_counter, file_identity, file_link_count, storage,
    ConstructionEncodedArtifact, GfError, GraphConstructionEncoding,
    GraphConstructionEncodingEvidence, StableDirectory, COPY_BUFFER_BYTES, ENCODED_ROOT,
    ENCODING_FORMAT_VERSION,
};

pub(crate) fn authenticate_inventory(
    root: &StableDirectory,
    inventory: &GraphConstructionEncoding,
) -> Result<GraphConstructionEncodingEvidence, GfError> {
    let _diagnostic_scope =
        crate::graph_construction::diagnostics::Scope::start("inventory_authentication");
    let evidence = authenticate_inventory_payloads(root, inventory, &mut || false)?;
    Ok(evidence)
}

/// The control half of [`authenticate_inventory`]: the inventory's structural
/// invariants, without reading a payload byte. The encoder runs this after installing the inventory it just wrote.
///
/// The payloads are not re-read here. Every artifact digest in the inventory
/// was computed by the single pass that wrote the bytes, and the boundary that
/// consumes those bytes — the private CAS install at publication — checksums
/// the actual copied bytes against checkpoint-admitted inventory authority and
/// refuses a mismatch (`install_captured_encoded_artifact_with_lease`). Re-reading bytes this process
/// wrote a moment ago, from its own page cache, names no failure that the
/// consuming copy does not already refuse (#1384; the reasoning #1392 applied
/// to shape outputs).
pub(crate) fn authenticate_inventory_control(
    inventory: &GraphConstructionEncoding,
) -> Result<(), GfError> {
    validate_inventory_invariants(inventory)
}

fn validate_inventory_invariants(inventory: &GraphConstructionEncoding) -> Result<(), GfError> {
    if inventory.format_version != ENCODING_FORMAT_VERSION
        || inventory.root != ENCODED_ROOT
        || inventory.shape_inputs_sha256.len() != 64
        || inventory.shape_authority_sha256.len() != 64
        || inventory
            .semantic_authority_sha256
            .as_ref()
            .is_some_and(|digest| {
                digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        || !inventory
            .shape_inputs_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || !inventory
            .shape_authority_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || inventory
            .artifacts
            .windows(2)
            .any(|pair| pair[0].path >= pair[1].path)
        || inventory.evidence.prior_topology_rows_decoded != 0
        || inventory.evidence.retained_topology_bytes_copied != 0
    {
        return Err(storage("canonical inventory invariants are invalid"));
    }
    Ok(())
}

pub(crate) fn authenticate_inventory_payloads(
    root: &StableDirectory,
    inventory: &GraphConstructionEncoding,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<GraphConstructionEncodingEvidence, GfError> {
    let _diagnostic_scope =
        crate::graph_construction::diagnostics::Scope::start("inventory_payload_authentication");
    validate_inventory_invariants(inventory)?;
    let mut evidence = GraphConstructionEncodingEvidence::default();
    for expected in &inventory.artifacts {
        let (directory, name) = open_artifact_directory(root, &expected.path)?;
        let file = directory
            .open_child_file(OsStr::new(&name))
            .map_err(storage)?;
        let identity = file_identity(&file).map_err(storage)?;
        let (released, operations) =
            authenticate_encoded_checksum(file, expected, directory.path(), cancelled)?;
        directory.revalidate_named().map_err(storage)?;
        let named = directory
            .open_child_file(OsStr::new(&name))
            .map_err(storage)?;
        if file_identity(&named).map_err(storage)? != identity {
            return Err(storage("canonical artifact path identity changed"));
        }
        add_evidence_counter(
            &mut evidence.input_read_bytes,
            expected.bytes,
            "input read bytes",
        )?;
        add_evidence_counter(
            &mut evidence.input_read_operations,
            operations,
            "input read operations",
        )?;
        account_cache_release(released, &mut evidence)?;
    }
    Ok(evidence)
}

fn open_artifact_directory(
    root: &StableDirectory,
    relative: &str,
) -> Result<(StableDirectory, String), GfError> {
    root.revalidate_named().map_err(storage)?;
    let mut components = Path::new(relative).components().collect::<Vec<_>>();
    let name = match components.pop() {
        Some(Component::Normal(name)) => name
            .to_str()
            .ok_or_else(|| storage("canonical artifact name is not UTF-8"))?
            .to_owned(),
        _ => return Err(storage("canonical artifact path has no file name")),
    };
    let mut directory = root
        .open_child_directory(OsStr::new("graph"))
        .map_err(storage)?;
    for component in components {
        let Component::Normal(name) = component else {
            return Err(storage("canonical artifact path is not normalized"));
        };
        directory = directory.open_child_directory(name).map_err(storage)?;
    }
    Ok((directory, name))
}

/// An encoded artifact has one name until publication links the same inode
/// into the object store (unix). A publication that stopped after that link is
/// retried by reopening this inventory, and a published project can hydrate
/// the object into reader workspaces, so more names are legitimate, but only
/// when the artifact's content address resolves to this very inode. Any other
/// extra name is refused.
pub(crate) fn staged_links_admitted(
    encoded: &std::path::Path,
    sha256: &str,
    file: &File,
) -> Result<bool, GfError> {
    let project = encoded.ancestors().find_map(|ancestor| {
        (ancestor.file_name() == Some(OsStr::new(".graphforge-construction")))
            .then(|| ancestor.parent())
            .flatten()
    });
    match project {
        Some(project) => encoded_links_expected(project, sha256, file),
        None => Ok(file_link_count(file).map_err(storage)? == 1),
    }
}

/// [`staged_links_admitted`] once the project root is known.
pub(crate) fn encoded_links_expected(
    project: &std::path::Path,
    sha256: &str,
    file: &File,
) -> Result<bool, GfError> {
    match file_link_count(file).map_err(storage)? {
        1 => Ok(true),
        2.. if cfg!(unix) => {
            let address = crate::graph_object_path(project, sha256)?;
            Ok(graphforge_filesystem::path_identity(&address).ok()
                == Some(file_identity(file).map_err(storage)?))
        }
        _ => Ok(false),
    }
}

fn authenticate_encoded_checksum(
    file: File,
    expected: &ConstructionEncodedArtifact,
    encoded: &std::path::Path,
    cancelled: &mut impl FnMut() -> bool,
) -> Result<(graphforge_filesystem::FileCacheReleaseEvidence, u64), GfError> {
    let identity = file_identity(&file).map_err(storage)?;
    if !staged_links_admitted(encoded, &expected.sha256, &file)?
        || file.metadata().map_err(storage)?.len() != expected.bytes
    {
        return Err(storage("canonical artifact identity or length changed"));
    }
    let mut reader = graphforge_filesystem::FileCacheReleasingReader::new(file).map_err(storage)?;
    let mut checksum = crate::corruption_checksum::Checksum::new();
    let mut bytes = 0_u64;
    let mut calls = 0_u64;
    let mut buffer = vec![0; COPY_BUFFER_BYTES];
    let checked = (|| {
        loop {
            crate::graph_construction::reject_cancelled(cancelled)?;
            let count = reader.read(&mut buffer).map_err(storage)?;
            if count == 0 {
                break;
            }
            add_evidence_counter(&mut bytes, count as u64, "encoded checksum bytes")?;
            add_evidence_counter(&mut calls, 1, "encoded checksum calls")?;
            if bytes > expected.bytes {
                return Err(storage("canonical artifact grew"));
            }
            checksum.update(&buffer[..count]);
        }
        if bytes != expected.bytes
            || checksum.finish() != expected.xxh64
            || file_identity(reader.file()).map_err(storage)? != identity
            || !staged_links_admitted(encoded, &expected.sha256, reader.file())?
            || reader.file().metadata().map_err(storage)?.len() != expected.bytes
        {
            return Err(storage(
                "canonical artifact checksum differs from inventory",
            ));
        }
        Ok(())
    })();
    let released = reader.finish().map_err(storage);
    match (checked, released) {
        (Ok(()), Ok(released)) => {
            crate::graph_construction::diagnostics::hashed_bytes(bytes, 1);
            Ok((released, calls))
        }
        (Err(primary), Ok(_)) | (Ok(()), Err(primary)) => Err(primary),
        (Err(primary), Err(cleanup)) => Err(storage(format!(
            "{primary}; encoded cache cleanup also failed: {cleanup}"
        ))),
    }
}
