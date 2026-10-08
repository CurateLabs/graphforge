//! Generation-owned counts of nodes carrying each runtime label token.
//!
//! The summary lets statement setup answer whether a label is present without
//! decoding every node topology row. It lives in the graph tree, so the graph
//! files inventory authenticates it and generation publication commits it with
//! the topology it describes.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use arrow::array::{Array, ListArray, UInt32Array};
use graphforge_core::GfError;
use graphforge_value::EntityTypeId;
use serde::{Deserialize, Serialize};

const SUMMARY_PATH: &str = "topology/label_membership_counts.json";
const SUMMARY_FORMAT: &str = "graphforge-label-membership-counts";
const SUMMARY_VERSION: u32 = 1;

/// Per-label counts of nodes carrying each runtime label token.
pub type LabelMembershipCounts = HashMap<EntityTypeId, u64>;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireSummary {
    format: String,
    version: u32,
    counts: BTreeMap<u32, u64>,
}

/// Read and validate the generation-owned membership counts.
pub fn read_label_membership_counts(
    dir: &Path,
) -> Result<Option<(LabelMembershipCounts, u64)>, GfError> {
    let path = summary_path(dir);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(GfError::Storage(
                "label membership summary is not a regular file".into(),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(GfError::Storage(format!(
                "inspect label membership summary: {error}"
            )));
        }
    }
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(GfError::Storage(format!(
                "read label membership summary: {error}"
            )));
        }
    };
    let wire: WireSummary = serde_json::from_slice(&bytes)
        .map_err(|error| GfError::Storage(format!("invalid label membership summary: {error}")))?;
    if wire.format != SUMMARY_FORMAT || wire.version != SUMMARY_VERSION {
        return Err(GfError::Storage(
            "unsupported label membership summary format or version".into(),
        ));
    }
    let mut counts = HashMap::new();
    for (encoded, count) in wire.counts {
        let label = EntityTypeId::decode(encoded).map_err(|error| {
            GfError::Storage(format!("invalid label membership token: {error}"))
        })?;
        if count == 0 || counts.insert(label, count).is_some() {
            return Err(GfError::Storage(
                "label membership summary has a zero count or duplicate token".into(),
            ));
        }
    }
    Ok(Some((counts, bytes.len() as u64)))
}

/// Establish a missing summary once from the selected authenticated topology.
/// Callers persist the result in the same generation as their next write.
pub fn establish_label_membership_counts(
    files: &crate::TopologyFiles,
) -> Result<(LabelMembershipCounts, u64, u64), GfError> {
    let batches =
        crate::read_nodes_from_files(files).map_err(|error| GfError::Storage(error.to_string()))?;
    let mut counts = HashMap::new();
    let mut rows = 0_u64;
    for batch in batches {
        rows = rows
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(|| GfError::Storage("label summary row counter overflow".into()))?;
        let labels = batch
            .column_by_name("type_ids")
            .and_then(|array| array.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| GfError::Storage("node topology missing type_ids".into()))?;
        for row in 0..batch.num_rows() {
            if labels.is_null(row) {
                return Err(GfError::Storage("node type_ids contains null list".into()));
            }
            let label_array = labels.value(row);
            let values = label_array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| GfError::Storage("node type_ids are not UInt32".into()))?;
            let mut memberships = std::collections::HashSet::new();
            for value in values {
                let encoded = value
                    .ok_or_else(|| GfError::Storage("node type_ids contains null item".into()))?;
                let label = EntityTypeId::decode(encoded)
                    .map_err(|error| GfError::Storage(error.to_string()))?;
                if !memberships.insert(label) {
                    return Err(GfError::Storage(
                        "node type_ids contains duplicate membership".into(),
                    ));
                }
            }
            for label in memberships {
                let count = counts.entry(label).or_insert(0_u64);
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| GfError::Storage("label membership count overflow".into()))?;
            }
        }
    }
    Ok((
        counts,
        rows,
        u64::try_from(files.node_fragments().len()).unwrap_or(u64::MAX),
    ))
}

/// Read labels for named nodes through the topology Parquet's UUID-to-surrogate
/// pairs, decoding only rows selected for those node IDs.
pub fn read_node_labels_for_uuids(
    dir: &Path,
    files: &crate::TopologyFiles,
    requested: &[[u8; 16]],
) -> Result<BTreeMap<[u8; 16], std::collections::HashSet<EntityTypeId>>, GfError> {
    if requested.is_empty() {
        return Ok(BTreeMap::new());
    }
    let uuids = requested
        .iter()
        .map(|bytes| graphforge_core::uuid::Uuid::from_bytes(*bytes))
        .collect::<Vec<_>>();
    let mut index =
        crate::TopologyIdentityProbe::open(dir, files, crate::read_topology_generation(dir)?)?;
    let (surrogates, _) = index.lookup_node_surrogates(&uuids)?;
    let mut node_ids = std::collections::HashSet::new();
    for (uuid, surrogate) in uuids.iter().zip(surrogates) {
        let Some(surrogate) = surrogate else {
            return Err(GfError::Storage(format!(
                "node {uuid} is absent from the authenticated node index"
            )));
        };
        node_ids.insert(surrogate);
    }
    let schema = &crate::TOPOLOGY_NODES_SCHEMA;
    let uuid_idx = schema
        .index_of("node_uuid")
        .map_err(|error| GfError::Storage(error.to_string()))?;
    let labels_idx = schema
        .index_of("type_ids")
        .map_err(|error| GfError::Storage(error.to_string()))?;
    let batches = crate::read_nodes_filtered_projected_observed_from_files(
        files,
        &node_ids,
        &[uuid_idx, labels_idx],
        None,
    )
    .map_err(|error| GfError::Storage(error.to_string()))?;
    let mut found = BTreeMap::new();
    for batch in batches {
        let uuids = batch
            .column_by_name("node_uuid")
            .and_then(|array| {
                array
                    .as_any()
                    .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            })
            .ok_or_else(|| GfError::Storage("node topology missing node_uuid".into()))?;
        let labels = batch
            .column_by_name("type_ids")
            .and_then(|array| array.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| GfError::Storage("node topology missing type_ids".into()))?;
        for row in 0..batch.num_rows() {
            if uuids.is_null(row) || labels.is_null(row) {
                return Err(GfError::Storage(
                    "node topology contains null identity data".into(),
                ));
            }
            let mut uuid = [0; 16];
            uuid.copy_from_slice(uuids.value(row));
            let label_array = labels.value(row);
            let values = label_array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| GfError::Storage("node type_ids are not UInt32".into()))?;
            let mut memberships = std::collections::HashSet::new();
            for value in values {
                let encoded = value
                    .ok_or_else(|| GfError::Storage("node type_ids contains null item".into()))?;
                memberships.insert(
                    EntityTypeId::decode(encoded)
                        .map_err(|error| GfError::Storage(error.to_string()))?,
                );
            }
            found.insert(uuid, memberships);
        }
    }
    if found.len() != node_ids.len() {
        return Err(GfError::Storage(
            "authenticated node index and topology disagree for requested labels".into(),
        ));
    }
    Ok(found)
}

/// Stage a complete replacement of the generation-owned membership summary.
pub fn stage_label_membership_counts<S: std::hash::BuildHasher>(
    staged: &mut crate::RewriteBatch,
    dir: &Path,
    counts: &HashMap<EntityTypeId, u64, S>,
) -> Result<(), GfError> {
    let mut encoded = BTreeMap::new();
    for (label, count) in counts {
        if *count == 0 {
            continue;
        }
        encoded.insert(label.encode(), *count);
    }
    let wire = WireSummary {
        format: SUMMARY_FORMAT.into(),
        version: SUMMARY_VERSION,
        counts: encoded,
    };
    let mut bytes = serde_json::to_vec(&wire)
        .map_err(|error| GfError::Storage(format!("encode label membership summary: {error}")))?;
    bytes.push(b'\n');
    staged.stage_bytes(&summary_path(dir), &bytes)
}

fn summary_path(dir: &Path) -> PathBuf {
    dir.join(SUMMARY_PATH)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_round_trips_and_rejects_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let label = EntityTypeId::decode(9).unwrap();
        let counts = HashMap::from([(label, 3)]);
        let mut staged = crate::RewriteBatch::new();
        stage_label_membership_counts(&mut staged, directory.path(), &counts).unwrap();
        staged.commit_at(directory.path()).unwrap();
        let (actual, bytes_read) = read_label_membership_counts(directory.path())
            .unwrap()
            .expect("staged label summary");
        assert_eq!(actual, counts);
        assert!(bytes_read > 0);

        fs::write(summary_path(directory.path()), b"not json").unwrap();
        assert!(read_label_membership_counts(directory.path()).is_err());
    }
}
