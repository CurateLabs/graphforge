//! Private authority for files emitted by import's adjacency reconstruction.
use super::{
    check_cancel, AtomicBool, BTreeMap, File, MaterializedCapture, Path, PortableV2Error,
    PortableV2ErrorCode, Read,
};
use graphforge_core::hash_observation::ArtifactSha256;
use graphforge_filesystem::StableDirectory;
use sha2::Digest;
use std::path::PathBuf;

/// Invoke the actual writer before admitting any of its outputs. Callers cannot
/// turn digest/checksum metadata into a source capability through this boundary.
pub(crate) fn capture_import_adjacency(
    stage: &Path,
    graph_tree: &Path,
    participants: &[crate::project_publication::ProjectFileParticipant],
    cancelled: Option<&AtomicBool>,
    allocation: Option<&crate::StorageAllocationOperation>,
    max_entry_bytes: u64,
) -> Result<(usize, BTreeMap<PathBuf, MaterializedCapture>), PortableV2Error> {
    let (added, records) =
        crate::project_portable_v2_import::adjacency::persist_import_adjacency_with_captures(
            stage,
            graph_tree,
            participants,
            cancelled,
            allocation,
        )?;
    if added == 0 {
        return Ok((0, BTreeMap::new()));
    }
    let mut written = BTreeMap::new();
    for record in records {
        let path = record.path.clone();
        if written.insert(path, record).is_some() {
            return Err(changed("duplicate written adjacency capture"));
        }
    }
    let root = crate::adjacency::adjacency_dir(graph_tree);
    let directory = StableDirectory::open(&root).map_err(|_| changed("adjacency root changed"))?;
    let mut pending = vec![(root, directory)];
    let mut remaining = added.saturating_mul(2).saturating_add(1024);
    let mut captures = BTreeMap::new();
    while let Some((path, directory)) = pending.pop() {
        check_cancel(cancelled)?;
        let names = directory.child_names_bounded(remaining).map_err(|_| {
            PortableV2Error::new(
                PortableV2ErrorCode::LimitExceeded,
                "derived adjacency inventory bound",
            )
        })?;
        remaining = remaining.saturating_sub(names.len());
        for name in names {
            let child_path = path.join(&name);
            if let Ok(child) = directory.open_child_directory(&name) {
                pending.push((child_path, child));
                continue;
            }
            let file = directory
                .open_child_file(&name)
                .map_err(|_| changed("derived adjacency file changed"))?;
            let capture = capture_file(
                &file,
                &child_path,
                written.remove(&child_path),
                max_entry_bytes,
                cancelled,
            )?;
            // Bind the fresh FD, namespace, allocation and single-link identity
            // before releasing authority; installation checks the copied CRC.
            capture.open_source(&child_path)?;
            captures.insert(child_path, capture);
            if captures.len() > added {
                return Err(changed("derived adjacency gained files"));
            }
        }
    }
    if captures.len() != added || !written.is_empty() {
        return Err(changed("written adjacency inventory changed"));
    }
    Ok((added, captures))
}

fn capture_file(
    file: &File,
    path: &Path,
    written: Option<crate::adjacency::CapturedAdjacencyArtifact>,
    max_entry_bytes: u64,
    cancelled: Option<&AtomicBool>,
) -> Result<MaterializedCapture, PortableV2Error> {
    let metadata = file
        .metadata()
        .map_err(|_| changed("derived metadata changed"))?;
    let length = metadata.len();
    if !metadata.is_file() || length > max_entry_bytes {
        return Err(PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
            "derived artifact size bound",
        ));
    }
    let identity = graphforge_filesystem::file_identity(file)
        .map_err(|_| changed("derived identity changed"))?;
    let allocated_bytes = graphforge_filesystem::file_space_usage(file)
        .map_err(|_| changed("derived allocation changed"))?
        .allocated_bytes;
    let (digest, checksum) = if let Some(written) = written {
        if length != written.bytes {
            return Err(changed("written adjacency length changed"));
        }
        (decode_digest(&written.sha256)?, written.xxh64)
    } else {
        // Manifests and other outputs without a writer capture are genuinely
        // authenticated once. The sealed proof carries both consumed-byte
        // identities into installation, which only checks its actual copy CRC.
        hash_uncaptured(file, length, cancelled)?
    };
    let capture = MaterializedCapture {
        identity,
        length,
        digest,
        checksum,
        allocated_bytes,
    };
    capture.open_source(path)?;
    Ok(capture)
}

fn hash_uncaptured(
    file: &File,
    length: u64,
    cancelled: Option<&AtomicBool>,
) -> Result<([u8; 32], u64), PortableV2Error> {
    let bound = length.checked_add(1).ok_or_else(|| {
        PortableV2Error::new(
            PortableV2ErrorCode::LimitExceeded,
            "derived artifact length overflow",
        )
    })?;
    let mut input = file.take(bound);
    let mut hash = ArtifactSha256::new();
    let mut checksum = crate::corruption_checksum::Checksum::new();
    let mut consumed = 0_u64;
    let mut buffer = vec![0; 64 * 1024];
    loop {
        check_cancel(cancelled)?;
        let count = input
            .read(&mut buffer)
            .map_err(|_| changed("derived artifact read failed"))?;
        if count == 0 {
            break;
        }
        consumed += count as u64;
        hash.update(&buffer[..count]);
        checksum.update(&buffer[..count]);
    }
    if consumed != length {
        return Err(changed("derived artifact length changed"));
    }
    Ok((hash.finalize().into(), checksum.finish()))
}

fn decode_digest(text: &str) -> Result<[u8; 32], PortableV2Error> {
    if text.len() != 64 {
        return Err(changed("written adjacency digest invalid"));
    }
    let mut digest = [0; 32];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let pair =
            std::str::from_utf8(pair).map_err(|_| changed("written adjacency digest invalid"))?;
        digest[index] = u8::from_str_radix(pair, 16)
            .map_err(|_| changed("written adjacency digest invalid"))?;
    }
    Ok(digest)
}

fn changed(message: &'static str) -> PortableV2Error {
    PortableV2Error::new(PortableV2ErrorCode::ConcurrentMutation, message)
}
