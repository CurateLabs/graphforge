//! Publish the derived adjacency CSR with the canonical inventory (#1388).

use std::ffi::OsStr;
use std::path::{Component, Path};

use graphforge_core::GfError;
use serde::{Deserialize, Serialize};

use super::{
    ConstructionEncodedArtifact, GraphConstructionEncodingEvidence, StableDirectory,
    account_cache_release, add_evidence_counter, authenticate_file_cancellable, directory_for,
    storage,
};

/// Measured work of the adjacency build inside canonical encoding.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdjacencyEncodingEvidence {
    /// Exact bytes of the published CSR artifacts. Also counted in the
    /// encoding's `output_write_bytes`. The bounded builder's individual write
    /// and fsync calls are not attributed to this evidence (the artifacts are
    /// accounted at file granularity here), but they do reach the lifecycle
    /// ledger, scoped to the encoding row (#1449).
    #[serde(default)]
    pub write_bytes: u64,
    /// Projected edge rows streamed from the encoded edge tables.
    #[serde(default)]
    pub source_rows: u64,
    /// Sorted spill runs written and merged.
    #[serde(default)]
    pub spill_runs: u64,
    /// Peak bytes charged to the spill session.
    #[serde(default)]
    pub spill_peak_bytes: u64,
    /// CSR shards published across every relation/direction pair.
    #[serde(default)]
    pub csr_shards: u64,
}

pub(super) fn register_adjacency_artifacts(
    output: &StableDirectory,
    graph_root: &Path,
    adjacency: &Path,
    shard_outputs: Vec<crate::adjacency::CapturedAdjacencyArtifact>,
    cancelled: &mut impl FnMut() -> bool,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    let mut relative_paths = Vec::new();
    collect_relative_files(graph_root, adjacency, &mut relative_paths)?;
    relative_paths.sort();
    let mut captured = shard_outputs
        .into_iter()
        .map(|artifact| {
            let path = artifact
                .path
                .strip_prefix(graph_root)
                .map_err(storage)?
                .to_str()
                .ok_or_else(|| storage("captured adjacency path is not UTF-8"))?
                .replace('\\', "/");
            Ok((path, artifact))
        })
        .collect::<Result<std::collections::BTreeMap<_, _>, GfError>>()?;
    for relative in relative_paths {
        let (directory, name) = directory_for(output, &relative)?;
        let file = directory
            .open_child_file(OsStr::new(&name))
            .map_err(storage)?;
        // The CSR is a published artifact like any other, so the allocation
        // tracker has to own it. Registering here rather than inside the
        // builder keeps the bounded builder's own writes unattributed, which
        // is deliberate, while still accounting for the files it leaves
        // behind -- `matches_file_inventory` admits no untracked file.
        if let Some(allocation) = output.allocation() {
            allocation.replace_file_at(&graph_root.join(&relative), &file)?;
        }
        let artifact = if let Some(captured) = captured.remove(&relative) {
            if file.metadata().map_err(storage)?.len() != captured.bytes
                || graphforge_filesystem::file_link_count(&file).map_err(storage)? != 1
            {
                return Err(storage("captured adjacency artifact identity changed"));
            }
            ConstructionEncodedArtifact {
                path: relative,
                bytes: captured.bytes,
                sha256: captured.sha256,
                xxh64: captured.xxh64,
            }
        } else {
            let (artifact, released, _) =
                authenticate_file_cancellable(&relative, file, cancelled)?;
            account_cache_release(released, evidence)?;
            artifact
        };
        add_evidence_counter(
            &mut evidence.adjacency.write_bytes,
            artifact.bytes,
            "adjacency write bytes",
        )?;
        add_evidence_counter(
            &mut evidence.output_write_bytes,
            artifact.bytes,
            "output write bytes",
        )?;
        artifacts.push(artifact);
    }
    if !captured.is_empty() {
        return Err(storage(
            "captured adjacency inventory includes absent files",
        ));
    }
    Ok(())
}

/// Every regular file below `directory`, as normalized paths relative to
/// `root`, refusing links and other non-regular entries.
fn collect_relative_files(
    root: &Path,
    directory: &Path,
    output: &mut Vec<String>,
) -> Result<(), GfError> {
    for entry in std::fs::read_dir(directory).map_err(storage)? {
        let entry = entry.map_err(storage)?;
        let path = entry.path();
        let kind = entry.file_type().map_err(storage)?;
        if kind.is_dir() {
            collect_relative_files(root, &path, output)?;
            continue;
        }
        if !kind.is_file() {
            return Err(storage(
                "adjacency artifact tree contains a non-regular file",
            ));
        }
        let relative = path
            .strip_prefix(root)
            .map_err(|_| storage("adjacency artifact escaped the encoded graph tree"))?;
        let mut text = String::new();
        for component in relative.components() {
            let Component::Normal(name) = component else {
                return Err(storage("adjacency artifact path is not normalized"));
            };
            if !text.is_empty() {
                text.push('/');
            }
            text.push_str(
                name.to_str()
                    .ok_or_else(|| storage("adjacency artifact name is not UTF-8"))?,
            );
        }
        output.push(text);
    }
    Ok(())
}
