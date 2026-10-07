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
//! longest path the encoder formats takes 132 bytes
//! (`edge_properties/r-<64 hex>/<20 digits>-<20 digits>.parquet`), so the row
//! takes at most 276 bytes. The Graph500 S26 inventory averaged 262 bytes per
//! row: 5,650,146 bytes for 21,525 artifacts. At that average, the projected
//! S28 inventory (about 86,000 artifacts, 22.6 MB) uses 44% of the bound, and
//! a full 100,000-file generation uses 51%. The header and evidence take a few
//! kilobytes.
//!
//! A reader holds the file bytes and the decoded inventory at once. At the
//! bound, with S26-shaped rows, that is about 121 MB, of which the file bytes
//! take 51.2 MB. The format check probes the version without first building a
//! JSON tree of the whole inventory, which would add about 270 MB more.
//!
//! The writer counts bytes as it serializes and refuses before the temporary
//! file is installed. A refused inventory is never installed or pinned by a
//! checkpoint, so the session reopens and resumes from its encoding intent
//! once the bound admits the inventory.

use std::io::{Read, Write};

use graphforge_core::ProjectErrorCode;
use serde::{Deserialize, Serialize};

use super::{ENCODING_FORMAT_VERSION, GfError, GraphConstructionEncoding, storage};
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
/// version before its schema.
pub(crate) fn decode_encoding_inventory(
    reader: impl Read,
) -> Result<GraphConstructionEncoding, GfError> {
    /// Reads only the version and skips every other field without building
    /// a JSON tree of the inventory.
    #[derive(Deserialize)]
    struct FormatProbe {
        format_version: Option<serde_json::Value>,
    }

    let bound = inventory_bound();
    let mut bytes = Vec::new();
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
