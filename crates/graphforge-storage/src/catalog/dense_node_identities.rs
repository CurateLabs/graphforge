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
use datafusion::error::DataFusionError;
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

/// An admission or open failure keeps the typed refusal it was raised as.
/// Admission of a hydrated payload returns a `GfError` (`GF_VALIDATION` for
/// corruption) wrapped in `DataFusionError::External`; flattening it to a
/// storage error would let a caller read corruption as a transient I/O fault.
fn open_error(error: DataFusionError) -> DenseNodeIdentityError {
    if let DataFusionError::External(inner) = error.find_root()
        && let Some(refusal) = inner.downcast_ref::<GfError>()
    {
        return DenseNodeIdentityError::Read(refusal.clone());
    }
    read_error(error)
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
            .map_err(open_error)?
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
        let builder = admitted_parquet(path).map_err(open_error)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_admission::AdmissionBatch;

    /// A node file hydrated the way a compact generation links it: one object
    /// and a workspace hard link that share an inode, registered for first-touch
    /// admission against the object's true checksum.
    #[test]
    fn corrupt_hydrated_node_payload_keeps_its_validation_refusal() {
        let source = tempfile::tempdir().unwrap();
        let mut writer = crate::GraphWriter::open_at(
            source.path(),
            graphforge_core::OntologyMode::Exploratory,
            1_700_000_000_000_000,
        )
        .unwrap();
        for _ in 0..3 {
            writer
                .create_node(
                    graphforge_core::uuid::new_v7(),
                    graphforge_value::EntityTypeId::decode(0).unwrap(),
                )
                .unwrap();
        }
        writer.flush().unwrap();
        let original = crate::TopologyFiles::discover_legacy(source.path()).unwrap();
        let bytes = std::fs::read(&original.nodes[0].0).unwrap();

        let hydrated = tempfile::tempdir().unwrap();
        let object = hydrated.path().join("object");
        let workspace = hydrated.path().join("workspace");
        let link = workspace.join("nodes.parquet");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(&object, &bytes).unwrap();
        std::fs::hard_link(&object, &link).unwrap();
        let entry = crate::GraphFileEntry {
            relative_path: "nodes.parquet".into(),
            byte_length: bytes.len() as u64,
            content_sha256: "0".repeat(64),
            content_xxh64: crate::corruption_checksum::checksum(&bytes),
            role: crate::GraphFileRole::Topology,
        };
        AdmissionBatch::begin().register(
            graphforge_filesystem::path_identity(&object).unwrap(),
            &entry,
            object.clone(),
            &workspace,
            link.clone(),
        );
        // Same-inode, same-length corruption away from the Parquet magic, so only
        // first-touch admission can notice it.
        let mut corrupt = bytes.clone();
        corrupt[bytes.len() / 2] ^= 0xff;
        std::fs::write(&object, corrupt).unwrap();
        let files = crate::TopologyFiles {
            nodes: vec![(link, "nodes.parquet".into())],
            edges: Vec::new(),
        };

        let error = read_dense_node_identities(&files).unwrap_err();
        let DenseNodeIdentityError::Read(refusal) = error else {
            panic!("corruption must be a read refusal");
        };
        assert_eq!(refusal.code(), "GF_VALIDATION", "{refusal}");
    }
}
