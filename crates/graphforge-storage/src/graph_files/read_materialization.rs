//! Retained checksum admission for private graph read copies.

use graphforge_core::GfError;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::Path;

use super::{
    GraphFileEntry, HASH_BUFFER_BYTES, RetainedV1InventoryEntry, checksum_reader, corrupt, storage,
    validation,
};

pub(super) struct ReadCopyIoEvidence {
    pub(super) read_bytes: u64,
    pub(super) read_calls: u64,
    pub(super) write_bytes: u64,
    pub(super) write_calls: u64,
    pub(super) fsync_calls: u64,
}

pub(super) fn copy_read_inventory_file(
    mut source: RetainedV1InventoryEntry,
    destination: &Path,
    entry: &GraphFileEntry,
) -> Result<ReadCopyIoEvidence, GfError> {
    source
        .file
        .rewind()
        .map_err(|error| storage("rewind graph copy source", &source.path, error))?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .map_err(|error| storage("create private graph copy", destination, error))?;
    let read_bound = entry
        .byte_length
        .checked_add(1)
        .ok_or_else(|| validation("copy length overflow"))?;
    let mut bounded = (&mut source.file).take(read_bound);
    let mut checksum = crate::corruption_checksum::Checksum::new();
    let mut bytes = 0_u64;
    let mut calls = 0_u64;
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        let read = bounded
            .read(&mut buffer)
            .map_err(|error| storage("read retained graph copy source", &source.path, error))?;
        if read == 0 {
            break;
        }
        checksum.update(&buffer[..read]);
        output
            .write_all(&buffer[..read])
            .map_err(|error| storage("write private graph copy", destination, error))?;
        bytes = bytes
            .checked_add(read as u64)
            .ok_or_else(|| validation("graph copy byte count overflow"))?;
        calls += 1;
    }
    if bytes != entry.byte_length
        || checksum.finish() != entry.content_xxh64
        || source
            .file
            .metadata()
            .map_err(|error| storage("inspect graph copy source", &source.path, error))?
            .len()
            != entry.byte_length
        || graphforge_filesystem::file_identity(&source.file)
            .map_err(|error| storage("identify graph copy source", &source.path, error))?
            != source.identity
        || graphforge_filesystem::path_identity(&source.path).ok() != Some(source.identity)
    {
        return Err(corrupt("retained graph copy source changed"));
    }
    crate::durable_commit::seal_file(&output)
        .map_err(|error| storage("sync private graph copy", destination, error))?;
    let mut reopened = File::open(destination)
        .map_err(|error| storage("open private graph copy", destination, error))?;
    let (output_checksum, output_calls) =
        checksum_reader(&mut (&mut reopened).take(read_bound), destination)?;
    let output_bytes = reopened
        .stream_position()
        .map_err(|error| storage("inspect copied graph length", destination, error))?;
    if output_checksum != entry.content_xxh64 || output_bytes != entry.byte_length {
        return Err(corrupt("private graph copy checksum or length changed"));
    }
    crate::graph_construction::diagnostics::sealed_payload(bytes, 1);
    crate::graph_construction::diagnostics::hashed_bytes(output_bytes, 1);
    crate::lifecycle_io::record_read(
        crate::StorageIoPhase::HydrationVerification,
        bytes + output_bytes,
        calls + output_calls,
    );
    crate::lifecycle_io::record_write(crate::StorageIoPhase::HydrationVerification, bytes, calls);
    crate::lifecycle_io::record_objects(crate::StorageIoPhase::HydrationVerification, 1);
    crate::lifecycle_io::record_fsync(crate::StorageIoPhase::HydrationVerification, 1);
    Ok(ReadCopyIoEvidence {
        read_bytes: bytes + output_bytes,
        read_calls: calls + output_calls,
        write_bytes: bytes,
        write_calls: calls,
        fsync_calls: 1,
    })
}
