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
use crate::graph_construction::ConstructionShape;

/// Measured work of the adjacency build inside canonical encoding.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdjacencyEncodingEvidence {
    /// Exact bytes of the published CSR artifacts. Also counted in the
    /// encoding's `output_write_bytes`; the bounded builder's individual write
    /// and fsync calls are not attributed.
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

/// Private spill root for the adjacency build, beside (never inside) the
/// encoded graph tree so a crash cannot leave spill runs among artifacts.
const ADJACENCY_SPILL_ROOT: &str = ".adjacency-spill";

/// Publish the derived adjacency CSR with the canonical inventory (#1388).
///
/// Every query process used to rebuild the whole CSR into a private temporary
/// directory and delete it at exit. Building it once here, from the exact edge
/// tables just encoded, makes it an ordinary SHA-256-declared artifact under
/// `indexes/adjacency/` that the publisher installs into the CAS like every
/// other file: hydration verifies its digest at open, and the persistent
/// adjacency provider then opens it presence-only instead of rebuilding.
///
/// Only an initial construction (`parent_topology_generation == 0`) is
/// covered: the encoder holds every edge table of that generation. An append
/// carries the parent's files forward structurally without re-reading them,
/// so its index would need the parent's edge tables too; until that lands an
/// append keeps today's lazy rebuild (the provider reads the carried-forward
/// manifest as stale and never serves it).
///
/// Deterministic by construction: CSR bytes derive from `topology/` alone
/// (R-ADJ-2), the shard directory takes its content digest as its name, and
/// the manifest's build time is the session's recorded clock, so the encoded
/// inventory authority stays reproducible across sessions and resume.
pub(super) fn encode_adjacency(
    output: &StableDirectory,
    shape: &ConstructionShape,
    generation: u64,
    routes: &crate::route_component::RouteTable,
    cancelled: &mut impl FnMut() -> bool,
    artifacts: &mut Vec<ConstructionEncodedArtifact>,
    evidence: &mut GraphConstructionEncodingEvidence,
) -> Result<(), GfError> {
    if shape.parent_topology_generation != 0 {
        return Ok(());
    }
    let graph_root = output.path().join("graph");
    let mut edge_files = Vec::new();
    for artifact in artifacts.iter() {
        if !artifact.path.starts_with("topology/edges/") {
            continue;
        }
        let semantic = routes.semantic_relative_path(&artifact.path)?;
        let relation = crate::route_component::route_position(&semantic)?
            .ok_or_else(|| storage("encoded edge table lacks a relation route"))?;
        edge_files.push((relation.to_owned(), graph_root.join(&artifact.path)));
    }
    edge_files.sort();

    // A crashed earlier attempt may have left a torn index or spill behind;
    // the artifact set must be exactly this build's.
    let adjacency = crate::adjacency::adjacency_dir(&graph_root);
    if adjacency.exists() {
        std::fs::remove_dir_all(&adjacency).map_err(storage)?;
    }
    let spill_root = output.path().join(ADJACENCY_SPILL_ROOT);
    if spill_root.exists() {
        std::fs::remove_dir_all(&spill_root).map_err(storage)?;
    }
    let options = crate::adjacency::AdjacencyBuildOptions {
        spill_dir: Some(spill_root.clone()),
        ..crate::adjacency::AdjacencyBuildOptions::default()
    };
    let (rows, metrics) = crate::adjacency::build_adjacency_index_for_edge_files(
        &graph_root,
        &edge_files,
        generation,
        shape.runtime_catalog_now_micros,
        &options,
        || crate::graph_construction::reject_cancelled(cancelled),
    )?;
    let _ = std::fs::remove_dir_all(&spill_root);
    if rows.is_empty() {
        return Err(storage("adjacency build published no manifest rows"));
    }

    let mut relative_paths = Vec::new();
    collect_relative_files(&graph_root, &adjacency, &mut relative_paths)?;
    relative_paths.sort();
    for relative in relative_paths {
        let (directory, name) = directory_for(output, &relative)?;
        let file = directory
            .open_child_file(OsStr::new(&name))
            .map_err(storage)?;
        let (artifact, released, _) = authenticate_file_cancellable(&relative, file, cancelled)?;
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
        account_cache_release(released, evidence)?;
        artifacts.push(artifact);
    }
    evidence.adjacency.source_rows = metrics.source_rows;
    evidence.adjacency.spill_runs = metrics.spill_runs;
    evidence.adjacency.spill_peak_bytes = metrics.spill_bytes;
    evidence.adjacency.csr_shards = metrics.csr_shards;
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
