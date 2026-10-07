//! The size bound on the encoded inventory, `encoded-v1/inventory.json`, and
//! on the encoding intent beside it. The encoder enforces it where it writes
//! them and every reader where it loads them (#900).
//!
//! The inventory has one JSON row per encoded artifact and per retained parent
//! object, and each row names a graph file of the generation under
//! construction. A generation lists at most [`MAX_GRAPH_FILES_PER_GENERATION`]
//! graph files, so the bound is that cap times [`INVENTORY_ROW_BYTES`]:
//! 100,000 × 512 = 51,200,000 bytes. Raising the cap raises the bound, and
//! `inventory_bound_tracks_the_graph_file_cap` fails until both are reviewed.
//!
//! The 512-byte row allowance covers the longest encoded-artifact row with
//! room to spare. Its fixed fields take at most 144 bytes: a 20-digit length,
//! a 64-digit SHA-256, a 16-digit xxh64, and the keys and punctuation. The
//! longest path the encoder writes is an adjacency CSR shard, 151 bytes
//! (`indexes/adjacency/r-<64 hex>.out.csr.shards-<24 hex>.d/<20 digits>.csr`),
//! so the row takes at most 295 bytes, 1.74× under the allowance. The Graph500 S26 inventory averaged 262 bytes per
//! row: 5,650,146 bytes for 21,525 artifacts. At that average, the projected
//! S28 inventory (about 86,000 artifacts, 22.6 MB) uses 44% of the bound, and
//! a full 100,000-file generation uses 51%. The header and evidence take a few
//! kilobytes. Retained-parent rows are longer, because they carry the parent
//! root's path, but there is one per retained membership-index run.
//!
//! A decoding reader holds the file bytes and the decoded inventory at once.
//! The read buffer is sized from the file's length. At the bound, decoding
//! peaks at 106 MB of heap with 262-byte rows (194,670 rows: the 51.2 MB file
//! plus 54.7 MB of decoded rows) and at 119 MB with the shortest possible rows
//! (406,335 rows of 126 bytes, 60.0 MB decoded). The format check probes the
//! version without first building a JSON tree of the whole inventory.
//!
//! No caller decodes a second copy while it holds one. Supersession after
//! encoding and the publication check bind the inventory the caller holds to
//! the durable file through [`durable_inventory_authority_sha256`], which
//! streams the file's bytes into the digest in 64 KiB chunks, and
//! [`inventory_authority_sha256`] streams the held inventory's serialization
//! the same way. These checks add about 1.1 MiB of buffers (a 1 MiB read
//! buffer and two 64 KiB chunks) to the inventory the caller already holds.
//!
//! The writer counts bytes as it serializes and refuses before the temporary
//! file is installed. A refused inventory is never installed or pinned by a
//! checkpoint, so the session reopens and resumes from its encoding intent
//! once the bound admits the inventory.

use std::io::{BufWriter, Read, Write};

use graphforge_core::ProjectErrorCode;
use graphforge_core::hash_observation::ControlSha256;
use serde::{Deserialize, Serialize};
use sha2::Digest;

use super::{ENCODING_FORMAT_VERSION, GfError, GraphConstructionEncoding, hex, storage};
use crate::graph_manifest::MAX_GRAPH_FILES_PER_GENERATION;

/// Allowance per inventory row; see the module documentation.
pub(super) const INVENTORY_ROW_BYTES: u64 = 512;

/// Largest encoded inventory or encoding intent the encoder writes or loads.
pub(super) const MAX_INVENTORY_BYTES: u64 =
    MAX_GRAPH_FILES_PER_GENERATION as u64 * INVENTORY_ROW_BYTES;

#[cfg(test)]
thread_local! {
    static BOUND_OVERRIDE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Replaces the bound for the calling thread, for both writing and reading,
/// until dropped.
#[cfg(test)]
pub(crate) struct InventoryBoundOverride {
    previous: Option<u64>,
}

#[cfg(test)]
impl InventoryBoundOverride {
    pub(crate) fn set(bound: u64) -> Self {
        Self {
            previous: BOUND_OVERRIDE.with(|slot| slot.replace(Some(bound))),
        }
    }
}

#[cfg(test)]
impl Drop for InventoryBoundOverride {
    fn drop(&mut self) {
        BOUND_OVERRIDE.with(|slot| slot.set(self.previous));
    }
}

/// The bound in force: [`MAX_INVENTORY_BYTES`], unless a test replaced it.
pub(super) fn inventory_bound() -> u64 {
    #[cfg(test)]
    if let Some(bound) = BOUND_OVERRIDE.with(std::cell::Cell::get) {
        return bound;
    }
    MAX_INVENTORY_BYTES
}

/// Serialize `value` into `output` as JSON, refusing once it would exceed the
/// bound. The caller has not installed `output` yet and discards it on error.
pub(super) fn write_bounded_json<T: Serialize>(
    output: impl Write,
    name: &str,
    value: &T,
) -> Result<(), GfError> {
    let limit = inventory_bound();
    let mut writer = BoundedWriter {
        inner: output,
        written: 0,
        limit,
        exceeded: false,
    };
    let written = serde_json::to_writer(&mut writer, value)
        .and_then(|()| writer.flush().map_err(serde_json::Error::io));
    match written {
        Ok(()) => Ok(()),
        Err(_) if writer.exceeded => Err(GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            message: format!(
                "encoded {name} exceeds the {limit}-byte encoded inventory bound; it was not \
                 installed, and the construction session resumes once the bound admits it"
            ),
        }),
        Err(error) => Err(storage(error)),
    }
}

struct BoundedWriter<W> {
    inner: W,
    written: u64,
    limit: u64,
    exceeded: bool,
}

impl<W: Write> Write for BoundedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .written
            .checked_add(bytes.len() as u64)
            .is_none_or(|total| total > self.limit)
        {
            self.exceeded = true;
            return Err(std::io::Error::other("encoded control exceeds bound"));
        }
        let accepted = self.inner.write(bytes)?;
        self.written += accepted as u64;
        Ok(accepted)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Decode an encoded inventory of at most the bound, checking its format
/// version before its schema. `length_hint` sizes the read buffer; a file
/// reader passes the file's length so the buffer does not double past it.
pub(crate) fn decode_encoding_inventory(
    reader: impl Read,
    length_hint: u64,
) -> Result<GraphConstructionEncoding, GfError> {
    /// Reads only the version and skips every other field without building
    /// a JSON tree of the inventory.
    #[derive(Deserialize)]
    struct FormatProbe {
        format_version: Option<serde_json::Value>,
    }

    let bound = inventory_bound();
    let mut bytes = Vec::with_capacity(usize::try_from(length_hint.min(bound)).map_err(storage)?);
    reader
        .take(bound + 1)
        .read_to_end(&mut bytes)
        .map_err(storage)?;
    if bytes.len() as u64 > bound {
        return Err(storage("canonical inventory exceeds bound"));
    }
    let probe: FormatProbe = serde_json::from_slice(&bytes).map_err(storage)?;
    if probe
        .format_version
        .as_ref()
        .and_then(serde_json::Value::as_u64)
        != Some(u64::from(ENCODING_FORMAT_VERSION))
    {
        return Err(storage(
            "unsupported encoded inventory format; recreate the project",
        ));
    }
    serde_json::from_slice(&bytes).map_err(storage)
}

const INVENTORY_AUTHORITY_DOMAIN: &[u8] = b"graphforge-construction-encoding-inventory/v2\0";
const DIGEST_CHUNK_BYTES: usize = 64 * 1024;

/// Authority over an inventory: the control digest of its serialization,
/// streamed so that no serialized copy is held.
pub(crate) fn inventory_authority_sha256(
    inventory: &GraphConstructionEncoding,
) -> Result<String, GfError> {
    struct DigestWriter<'a>(&'a mut ControlSha256);
    impl Write for DigestWriter<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut digest = ControlSha256::default();
    digest.update(INVENTORY_AUTHORITY_DOMAIN);
    let mut writer = BufWriter::with_capacity(DIGEST_CHUNK_BYTES, DigestWriter(&mut digest));
    serde_json::to_writer(&mut writer, inventory).map_err(storage)?;
    writer.flush().map_err(storage)?;
    drop(writer);
    Ok(hex(&digest.finalize()))
}

/// Authority over a durable inventory file, from its bytes within the bound
/// and without decoding them. The encoder installs exactly the serialization
/// that [`inventory_authority_sha256`] digests, so the two agree for an intact
/// file, and a caller binds an inventory it already holds to the durable
/// record without decoding a second copy (#900).
pub(super) fn durable_inventory_authority_sha256(reader: impl Read) -> Result<String, GfError> {
    let bound = inventory_bound();
    let mut reader = reader.take(bound + 1);
    let mut digest = ControlSha256::default();
    digest.update(INVENTORY_AUTHORITY_DOMAIN);
    let mut chunk = vec![0_u8; DIGEST_CHUNK_BYTES];
    let mut total = 0_u64;
    loop {
        let read = match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(storage(error)),
        };
        total += read as u64;
        if total > bound {
            return Err(storage("canonical inventory exceeds bound"));
        }
        digest.update(&chunk[..read]);
    }
    Ok(hex(&digest.finalize()))
}
