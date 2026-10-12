//! Dense node-identity read for analyst exports.
//!
//! Canonical node tables assign `node_id` densely and monotonically (see
//! `docs/book/architecture/storage.md`, "Filtered node lookup"), so the UUID of
//! `node_id = first_node_id + n` is the `n`-th stored row. This read returns just
//! those UUIDs, in file order, from a two-column projection: no other column is
//! decoded and no per-node hash map is built. The layout is proved from the
//! values read, not assumed, and a table that is not dense is refused so the
//! caller can take a path that handles gaps.

use super::{admitted_parquet, parquet_err};
use arrow::array::{Array, FixedSizeBinaryArray, UInt64Array};
use graphforge_core::GfError;
use parquet::arrow::ProjectionMask;

/// Rows decoded per batch while projecting `(node_id, node_uuid)`.
const IDENTITY_BATCH_ROWS: usize = 64 * 1024;

/// UUIDs of a dense node table, indexed by ordinal.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DenseNodeIdentities {
    /// `node_id` of ordinal 0; `0` for a table with no nodes.
    pub first_node_id: u64,
    /// `uuids[n]` belongs to `node_id = first_node_id + n`.
    pub uuids: Vec<[u8; 16]>,
}

impl DenseNodeIdentities {
    /// UUID of `node_id`, if the table holds it.
    #[must_use]
    pub fn uuid_of(&self, node_id: u64) -> Option<&[u8; 16]> {
        let ordinal = usize::try_from(node_id.checked_sub(self.first_node_id)?).ok()?;
        self.uuids.get(ordinal)
    }
}

/// Why [`read_dense_node_identities`] returned no identities.
#[derive(Debug)]
pub enum DenseNodeIdentityError {
    /// The stored `node_id` sequence is not `first, first + 1, ...` in file
    /// order: ids were deleted or the files are not in id order.
    NotDense {
        /// Row, counted across the files in order, where the sequence breaks.
        ordinal: u64,
        /// `node_id` a dense table holds at that row.
        expected_node_id: u64,
        /// `node_id` actually stored there.
        found_node_id: u64,
    },
    /// A node file could not be read or holds an invalid value.
    Read(GfError),
}

impl std::fmt::Display for DenseNodeIdentityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotDense {
                ordinal,
                expected_node_id,
                found_node_id,
            } => write!(
                formatter,
                "node table is not dense: row {ordinal} holds node_id {found_node_id}, \
                 expected {expected_node_id}"
            ),
            Self::Read(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for DenseNodeIdentityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotDense { .. } => None,
            Self::Read(error) => Some(error),
        }
    }
}

impl From<GfError> for DenseNodeIdentityError {
    fn from(error: GfError) -> Self {
        Self::Read(error)
    }
}

fn read_error(error: impl std::fmt::Display) -> DenseNodeIdentityError {
    DenseNodeIdentityError::Read(GfError::Storage(error.to_string()))
}

/// Read `(node_id, node_uuid)` from every node file of `files`, in file order,
/// and return the UUIDs by ordinal.
///
/// Every row is checked against the dense sequence while it is read, so a
/// returned value is a proof of the layout: [`DenseNodeIdentityError::NotDense`]
/// names the first row that breaks it. Dense `node_id`s are unique by
/// construction; this read does not separately dedupe UUIDs the way
/// [`read_nodes_from_files`](super::read_nodes_from_files) does. The output is
/// allocated once, sized from the file footers.
pub fn read_dense_node_identities(
    files: &crate::TopologyFiles,
) -> Result<DenseNodeIdentities, DenseNodeIdentityError> {
    let mut total_rows = 0_usize;
    for (path, _) in &files.nodes {
        let rows = admitted_parquet(path)
            .map_err(read_error)?
            .metadata()
            .file_metadata()
            .num_rows();
        total_rows = usize::try_from(rows)
            .ok()
            .and_then(|rows| total_rows.checked_add(rows))
            .ok_or_else(|| read_error("node table row count overflows usize"))?;
    }
    let mut identities = DenseNodeIdentities {
        first_node_id: 0,
        uuids: Vec::with_capacity(total_rows),
    };
    for (path, _) in &files.nodes {
        let builder = admitted_parquet(path).map_err(read_error)?;
        let mask = ProjectionMask::columns(builder.parquet_schema(), ["node_id", "node_uuid"]);
        let reader = builder
            .with_projection(mask)
            .with_batch_size(IDENTITY_BATCH_ROWS)
            .build()
            .map_err(parquet_err)
            .map_err(read_error)?;
        for batch in reader {
            let batch = batch.map_err(read_error)?;
            let ids = batch
                .column_by_name("node_id")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| read_error("node_id is not UInt64"))?;
            let uuids = batch
                .column_by_name("node_uuid")
                .and_then(|column| column.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .filter(|column| column.value_length() == 16)
                .ok_or_else(|| read_error("node_uuid is not FixedSizeBinary(16)"))?;
            if ids.null_count() != 0 || uuids.null_count() != 0 {
                return Err(read_error("node table holds a null node_id or node_uuid"));
            }
            for row in 0..batch.num_rows() {
                let found = ids.value(row);
                let ordinal = identities.uuids.len() as u64;
                if ordinal == 0 {
                    identities.first_node_id = found;
                }
                let expected = identities.first_node_id.checked_add(ordinal);
                if expected != Some(found) {
                    return Err(DenseNodeIdentityError::NotDense {
                        ordinal,
                        expected_node_id: expected.unwrap_or(u64::MAX),
                        found_node_id: found,
                    });
                }
                identities
                    .uuids
                    .push(uuids.value(row).try_into().map_err(read_error)?);
            }
        }
    }
    if identities.uuids.len() != total_rows {
        return Err(read_error("node files changed while being read"));
    }
    Ok(identities)
}
