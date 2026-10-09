//! Construction identity encoding, merge cursors, and guarded recovery.

use super::BULK_IO_BYTES;
use super::ConstructionIndexOutput;
use super::MAX_MANIFEST_BYTES;
use super::TopologyIndexReceipt;
use super::V4_ORDINAL_MANIFEST;
use super::V4_ORDINAL_RECEIPT;
use super::hex_bytes;
use super::ordinal_artifacts::V4PublicationGuard;
use super::ordinal_artifacts::admit_v4_construction_manifest;
use super::ordinal_artifacts::cleanup_v4_publication;
use super::storage_err;
use super::topology_delta::hex_sha256;
use super::topology_delta::read_bounded;
use super::v4_authority_failure;
use graphforge_core::GfError;
use graphforge_core::hash_observation::ArtifactSha256 as Sha256;
use sha2::Digest;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use uuid::Uuid;

/// Writes performed while installing one construction control file.
#[derive(Default)]
pub(super) struct ConstructionIndexWork {
    pub(super) write_bytes: u64,
    pub(super) write_operations: u64,
    pub(super) fsync_operations: u64,
}

pub(super) fn merge_cache_release_evidence(
    target: &mut graphforge_filesystem::FileCacheReleaseEvidence,
    source: graphforge_filesystem::FileCacheReleaseEvidence,
) {
    target.sync_operations = target
        .sync_operations
        .saturating_add(source.sync_operations);
    target.release_operations = target
        .release_operations
        .saturating_add(source.release_operations);
    target.unsupported_operations = target
        .unsupported_operations
        .saturating_add(source.unsupported_operations);
    target.released_bytes = target.released_bytes.saturating_add(source.released_bytes);
    target.peak_window_bytes = target.peak_window_bytes.max(source.peak_window_bytes);
}

pub(super) fn authenticate_private_v4_artifact_file(
    file: File,
    artifact: &crate::V4OrdinalArtifact,
) -> Result<(), GfError> {
    if graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1
        || file.metadata().map_err(storage_err)?.len() != artifact.bytes
        || checksum_reader_streaming(file)? != (artifact.xxh64, artifact.bytes)
    {
        return Err(storage_err("private v4 artifact authentication failed"));
    }
    Ok(())
}

pub(crate) fn is_exact_private_v4_name(name: &str) -> bool {
    if let Some(rest) = name.strip_prefix(".v4-") {
        let Some((role, nonce)) = rest.strip_suffix(".tmp").and_then(|v| v.rsplit_once('-')) else {
            return false;
        };
        let role_ok = role == "forward"
            || role == "tombstones"
            || role.strip_prefix("ordinal-").is_some_and(|ordinal| {
                ordinal.len() == 8 && ordinal.bytes().all(|b| b.is_ascii_digit())
            });
        return role_ok && canonical_lower_hex(nonce, 32);
    }
    let prefix = if name.starts_with("forward-v4-") {
        "forward-v4-"
    } else if name.starts_with("ordinal-v4-") {
        "ordinal-v4-"
    } else if name.starts_with("tombstones-v4-") {
        "tombstones-v4-"
    } else {
        return false;
    };
    let Some((generation, digest)) = name
        .strip_prefix(prefix)
        .and_then(|value| value.strip_suffix(".uuidx"))
        .and_then(|value| value.rsplit_once('-'))
    else {
        return false;
    };
    generation
        .parse::<u64>()
        .is_ok_and(|value| value != 0 && value.to_string() == generation)
        && canonical_lower_hex(digest, 16)
}

fn canonical_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn checksum_reader_streaming(file: File) -> Result<(u64, u64), GfError> {
    let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
    let expected_bytes = file.metadata().map_err(storage_err)?.len();
    let mut reader =
        graphforge_filesystem::FileCacheReleasingReader::new(file).map_err(storage_err)?;
    let checked = (|| {
        let mut checksum = crate::corruption_checksum::Checksum::new();
        let mut bytes = 0_u64;
        let mut buffer = vec![0; BULK_IO_BYTES];
        loop {
            let count = reader.read(&mut buffer).map_err(storage_err)?;
            if count == 0 {
                break;
            }
            bytes = bytes
                .checked_add(count as u64)
                .ok_or_else(|| storage_err("private ordinal length overflow"))?;
            if bytes > expected_bytes {
                return Err(storage_err("private ordinal artifact grew"));
            }
            checksum.update(&buffer[..count]);
        }
        if bytes != expected_bytes
            || graphforge_filesystem::file_identity(reader.file()).map_err(storage_err)? != identity
            || graphforge_filesystem::file_link_count(reader.file()).map_err(storage_err)? != 1
            || reader.file().metadata().map_err(storage_err)?.len() != expected_bytes
        {
            return Err(storage_err(
                "private ordinal artifact identity or length changed",
            ));
        }
        Ok((checksum.finish(), bytes))
    })();
    let cleanup = reader.finish().map_err(storage_err);
    match (checked, cleanup) {
        (Ok(value), Ok(_)) => Ok(value),
        (Err(primary), Ok(_)) | (Ok(_), Err(primary)) => Err(primary),
        (Err(primary), Err(cleanup)) => Err(storage_err(format!(
            "{primary}; private ordinal cache cleanup also failed: {cleanup}"
        ))),
    }
}

pub(super) fn combine_cache_cleanup<T>(
    primary: Result<T, GfError>,
    cleanup: Result<(), GfError>,
    source: &str,
) -> Result<T, GfError> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(primary), Ok(())) => Err(primary),
        (Err(primary), Err(cleanup)) => Err(storage_err(format!(
            "{primary}; {source} cache release also failed: {cleanup}"
        ))),
    }
}

pub(super) fn combine_v4_cleanup<T>(
    primary: Result<T, GfError>,
    cleanup: Result<(), GfError>,
    context: &str,
) -> Result<T, GfError> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(primary), Ok(())) => Err(primary),
        (Err(primary), Err(cleanup)) => Err(storage_err(format!(
            "{primary}; {context} also failed: {cleanup}"
        ))),
    }
}

pub(super) fn install_construction_bytes(
    output: &graphforge_filesystem::StableDirectory,
    name: &str,
    bytes: &[u8],
    work: &mut ConstructionIndexWork,
    allocation: Option<&crate::StorageAllocationOperation>,
) -> Result<(ConstructionIndexOutput, V4PublicationGuard), GfError> {
    let temporary = format!(".{name}-{}.tmp", Uuid::new_v4().simple());
    let mut publication =
        V4PublicationGuard::create(output, &temporary, allocation).map_err(storage_err)?;
    let mut file = publication.take_file().map_err(storage_err)?;
    let written = file.write_all(bytes).map_err(storage_err);
    let observed = publication.observe(&file).map_err(storage_err);
    written?;
    observed?;
    work.write_bytes = work.write_bytes.saturating_add(bytes.len() as u64);
    work.write_operations = work.write_operations.saturating_add(1);
    let seal = crate::durable_commit::seal_file_witness(&file).map_err(storage_err)?;
    publication.record_seal(seal);
    publication.observe(&file).map_err(storage_err)?;
    work.fsync_operations = work.fsync_operations.saturating_add(1);
    let failpoint = match name {
        V4_ORDINAL_RECEIPT => Some("v4_publish.after_receipt_temp_fsync"),
        V4_ORDINAL_MANIFEST => Some("v4_publish.after_manifest_temp_fsync"),
        "ordinal-v4.lock" => Some("v4_publish.after_lock_temp_fsync"),
        _ => None,
    };
    if let Some(failpoint) = failpoint {
        crate::graph_construction::construction_failpoint(failpoint);
    }
    drop(file);
    let installed = publication
        .install_child(std::ffi::OsStr::new(name))
        .map_err(storage_err);
    if let Err(primary) = installed {
        let cleanup = cleanup_v4_publication(&mut publication);
        return combine_v4_cleanup(Err(primary), cleanup, "v4 construction control cleanup");
    }
    let authority_point = match name {
        V4_ORDINAL_RECEIPT => Some("receipt_install"),
        V4_ORDINAL_MANIFEST => Some("manifest_install"),
        "ordinal-v4.lock" => Some("lock_install"),
        _ => None,
    };
    if let Some(point) = authority_point
        && let Err(primary) = v4_authority_failure(point)
    {
        let cleanup = cleanup_v4_publication(&mut publication);
        return combine_v4_cleanup(Err(primary), cleanup, "v4 construction control cleanup");
    }
    work.fsync_operations = work.fsync_operations.saturating_add(1);
    Ok((
        ConstructionIndexOutput {
            name: name.to_owned(),
            bytes: bytes.len() as u64,
            xxh64: crate::corruption_checksum::checksum(bytes),
            sha256: if matches!(name, V4_ORDINAL_RECEIPT | V4_ORDINAL_MANIFEST) {
                hex_sha256(bytes)
            } else {
                hex_bytes(&graphforge_core::hash_observation::ControlSha256::digest(
                    bytes,
                ))
            },
        },
        publication,
    ))
}

/// Remove the private ordinal-facet residue a crashed encoding attempt left in
/// the encoded tree, so a rerun starts from an empty directory. Only exact
/// private v4 names (and the retired membership names an older binary may have
/// left) are removed; anything else is refused.
pub(crate) fn clear_private_ordinal_residue(
    encoded: &crate::construction_directory::ConstructionDirectory,
) -> Result<(), GfError> {
    let allocation = encoded.allocation();
    let graph = match encoded.open_child_directory(std::ffi::OsStr::new("graph")) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage_err(error)),
    };
    let topology = match graph.open_child_directory(std::ffi::OsStr::new("topology")) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage_err(error)),
    };
    let index = match topology.open_child_directory(std::ffi::OsStr::new("uuid-membership")) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(storage_err(error)),
    };
    let names = if allocation.is_some() {
        index.child_names_bounded(1_000_000)
    } else {
        index.child_names()
    }
    .map_err(storage_err)?;
    let v4_allowed = authenticate_private_v4_residue(index.physical(), &names)?;
    if allocation.is_some() {
        for name in &names {
            let file = index.open_child_file(name).map_err(storage_err)?;
            index.observe_file(name, &file).map_err(storage_err)?;
        }
    }
    for name in names {
        let name_text = name
            .to_str()
            .ok_or_else(|| storage_err("construction recovery inventory name is not UTF-8"))?;
        if name_text != "manifest.json"
            && name_text != ".construction-intent.json"
            && !name_text.starts_with(".construction-")
            && !name_text.starts_with(".manifest.json-")
            && !name_text.starts_with("identities-v5")
            && !name_text.starts_with("node-surrogates-v5")
            && !v4_allowed.contains(name_text)
        {
            return Err(storage_err(
                "construction recovery inventory contains an unauthorised object",
            ));
        }
        let file = index.open_child_file(&name).map_err(storage_err)?;
        if graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1 {
            return Err(storage_err(
                "private construction index artifact has extra links",
            ));
        }
        let identity = graphforge_filesystem::file_identity(&file).map_err(storage_err)?;
        index
            .unlink_child_if_identity(&name, identity)
            .map_err(storage_err)?;
    }
    index.acknowledge().map_err(storage_err)?;
    topology.acknowledge().map_err(storage_err)?;
    graph.acknowledge().map_err(storage_err)?;
    encoded.acknowledge().map_err(storage_err)
}

fn authenticate_private_v4_residue(
    index: &graphforge_filesystem::StableDirectory,
    names: &[std::ffi::OsString],
) -> Result<BTreeSet<String>, GfError> {
    let text = names
        .iter()
        .map(|name| {
            name.to_str()
                .map(str::to_owned)
                .ok_or_else(|| storage_err("construction recovery inventory name is not UTF-8"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let receipt_present = text.contains(V4_ORDINAL_RECEIPT);
    let manifest_present = text.contains(V4_ORDINAL_MANIFEST);
    let lock_present = text.contains("ordinal-v4.lock");
    let mut allowed = BTreeSet::new();

    if manifest_present && !receipt_present || lock_present && !manifest_present {
        return Err(storage_err(
            "private v4 construction controls are not an install-order prefix",
        ));
    }
    authenticate_private_v4_control_temp(
        index,
        &text,
        receipt_present,
        manifest_present,
        lock_present,
        &mut allowed,
    )?;
    if receipt_present {
        let receipt_body =
            read_private_construction_child(index, V4_ORDINAL_RECEIPT, MAX_MANIFEST_BYTES)?;
        let receipt: TopologyIndexReceipt =
            serde_json::from_slice(&receipt_body).map_err(storage_err)?;
        if !canonical_lower_hex(&receipt.nonce, 32)
            || !canonical_lower_hex(&receipt.topology_delta_sha256, 64)
            || !canonical_lower_hex(&receipt.manifest_sha256, 64)
        {
            return Err(storage_err(
                "private v4 construction receipt is noncanonical",
            ));
        }
        allowed.insert(V4_ORDINAL_RECEIPT.to_owned());
        if !manifest_present {
            return authenticate_private_v4_artifact_residue(index, &text, allowed);
        }
        let manifest_body = read_private_construction_child(
            index,
            V4_ORDINAL_MANIFEST,
            crate::ordinal_identity_v4::MAX_MANIFEST_BYTES,
        )?;
        if hex_sha256(&manifest_body) != receipt.manifest_sha256 {
            return Err(storage_err(
                "private v4 construction manifest does not match its receipt",
            ));
        }
        let manifest = crate::ordinal_identity_v4::decode_ordinal_manifest(&manifest_body)
            .map_err(storage_err)?;
        if manifest.topology_generation != receipt.expected_generation {
            return Err(storage_err(
                "private v4 construction generation does not match its receipt",
            ));
        }
        admit_v4_construction_manifest(&manifest)?;
        allowed.insert(V4_ORDINAL_MANIFEST.to_owned());
        if lock_present {
            let lock = index
                .open_child_file(std::ffi::OsStr::new("ordinal-v4.lock"))
                .map_err(storage_err)?;
            if lock.metadata().map_err(storage_err)?.len() != 0 {
                return Err(storage_err("private v4 construction lock is nonempty"));
            }
            allowed.insert("ordinal-v4.lock".to_owned());
        }
        for artifact in manifest
            .forward_identities
            .iter()
            .chain(manifest.ordinal_ranges.iter().map(|range| &range.artifact))
            .chain(manifest.tombstones.iter().map(|run| &run.artifact))
        {
            authenticate_private_v4_artifact(index, artifact)?;
            if !allowed.insert(artifact.name.clone()) {
                return Err(storage_err(
                    "private v4 construction manifest repeats an artifact",
                ));
            }
        }
    }

    authenticate_private_v4_artifact_residue(index, &text, allowed)
}

fn authenticate_private_v4_control_temp(
    index: &graphforge_filesystem::StableDirectory,
    text: &BTreeSet<String>,
    receipt_present: bool,
    manifest_present: bool,
    lock_present: bool,
    allowed: &mut BTreeSet<String>,
) -> Result<(), GfError> {
    let temporaries = text
        .iter()
        .filter_map(|name| {
            [V4_ORDINAL_RECEIPT, V4_ORDINAL_MANIFEST, "ordinal-v4.lock"]
                .into_iter()
                .find_map(|control| {
                    name.strip_prefix(&format!(".{control}-"))
                        .and_then(|suffix| suffix.strip_suffix(".tmp"))
                        .filter(|nonce| canonical_lower_hex(nonce, 32))
                        .map(|_| (name, control))
                })
        })
        .collect::<Vec<_>>();
    if temporaries.len() > 1 {
        return Err(storage_err(
            "private v4 construction has multiple control temporaries",
        ));
    }
    let Some((name, control)) = temporaries.first().copied() else {
        return Ok(());
    };
    match control {
        V4_ORDINAL_RECEIPT if !receipt_present && !manifest_present && !lock_present => {
            let body = read_private_construction_child(index, name, MAX_MANIFEST_BYTES)?;
            let receipt: TopologyIndexReceipt =
                serde_json::from_slice(&body).map_err(storage_err)?;
            if !canonical_lower_hex(&receipt.nonce, 32)
                || !canonical_lower_hex(&receipt.topology_delta_sha256, 64)
                || !canonical_lower_hex(&receipt.manifest_sha256, 64)
            {
                return Err(storage_err("private v4 receipt temporary is noncanonical"));
            }
        }
        V4_ORDINAL_MANIFEST if receipt_present && !manifest_present && !lock_present => {
            let receipt_body =
                read_private_construction_child(index, V4_ORDINAL_RECEIPT, MAX_MANIFEST_BYTES)?;
            let receipt: TopologyIndexReceipt =
                serde_json::from_slice(&receipt_body).map_err(storage_err)?;
            let body = read_private_construction_child(
                index,
                name,
                crate::ordinal_identity_v4::MAX_MANIFEST_BYTES,
            )?;
            let manifest =
                crate::ordinal_identity_v4::decode_ordinal_manifest(&body).map_err(storage_err)?;
            if hex_sha256(&body) != receipt.manifest_sha256
                || manifest.topology_generation != receipt.expected_generation
            {
                return Err(storage_err(
                    "private v4 manifest temporary is not receipt-bound",
                ));
            }
            admit_v4_construction_manifest(&manifest)?;
        }
        "ordinal-v4.lock" if receipt_present && manifest_present && !lock_present => {
            let body = read_private_construction_child(index, name, 0)?;
            if !body.is_empty() {
                return Err(storage_err("private v4 lock temporary is nonempty"));
            }
        }
        _ => {
            return Err(storage_err(
                "private v4 control temporary is outside its install boundary",
            ));
        }
    }
    allowed.insert(name.clone());
    Ok(())
}

fn authenticate_private_v4_artifact_residue(
    index: &graphforge_filesystem::StableDirectory,
    text: &BTreeSet<String>,
    mut allowed: BTreeSet<String>,
) -> Result<BTreeSet<String>, GfError> {
    for name in text {
        if allowed.contains(name) || !is_exact_private_v4_name(name) {
            continue;
        }
        if name.starts_with(".v4-") {
            allowed.insert(name.clone());
            continue;
        }
        let file = index
            .open_child_file(std::ffi::OsStr::new(name))
            .map_err(storage_err)?;
        let length = file.metadata().map_err(storage_err)?.len();
        let digest = sha256_reader_streaming(file)?;
        let suffix = name
            .strip_suffix(".uuidx")
            .and_then(|name| name.rsplit_once('-'))
            .map(|(_, digest)| digest)
            .ok_or_else(|| storage_err("private v4 artifact name is malformed"))?;
        if length == 0 && !name.starts_with("tombstones-v4-") || !digest.starts_with(suffix) {
            return Err(storage_err(
                "private v4 construction artifact fails content authentication",
            ));
        }
        allowed.insert(name.clone());
    }
    Ok(allowed)
}

fn read_private_construction_child(
    index: &graphforge_filesystem::StableDirectory,
    name: &str,
    maximum: u64,
) -> Result<Vec<u8>, GfError> {
    let mut file = index
        .open_child_file(std::ffi::OsStr::new(name))
        .map_err(storage_err)?;
    if graphforge_filesystem::file_link_count(&file).map_err(storage_err)? != 1 {
        return Err(storage_err(
            "private v4 construction control has extra links",
        ));
    }
    read_bounded(&mut file, maximum)
}

fn authenticate_private_v4_artifact(
    index: &graphforge_filesystem::StableDirectory,
    artifact: &crate::V4OrdinalArtifact,
) -> Result<(), GfError> {
    if !is_exact_private_v4_name(&artifact.name) || artifact.name.starts_with(".v4-") {
        return Err(storage_err("private v4 artifact name is noncanonical"));
    }
    let file = index
        .open_child_file(std::ffi::OsStr::new(&artifact.name))
        .map_err(storage_err)?;
    authenticate_private_v4_artifact_file(file, artifact)
}

fn sha256_reader_streaming(file: File) -> Result<String, GfError> {
    let mut reader =
        graphforge_filesystem::FileCacheReleasingReader::new(file).map_err(storage_err)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    let hashed = (|| -> Result<String, GfError> {
        loop {
            let read = reader.read(&mut buffer).map_err(storage_err)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        Ok(hex_bytes(&digest.finalize()))
    })();
    let released = reader.finish().map_err(storage_err);
    match (hashed, released) {
        (Ok(digest), Ok(_)) => Ok(digest),
        (Ok(_), Err(error)) => Err(error),
        (Err(primary), Ok(_)) => Err(primary),
        (Err(primary), Err(release)) => Err(storage_err(format!(
            "{primary}; private v4 cache release also failed: {release}"
        ))),
    }
}
