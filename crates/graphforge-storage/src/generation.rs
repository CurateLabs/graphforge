//! Project mutation generations — staleness signals for derived indexes.
//!
//! Every committed batch that rewrites topology (`topology/nodes.parquet` or
//! any file under `topology/edges/`) bumps a monotonically increasing counter
//! persisted at `topology/generation.json`. Derived indexes record the counter
//! they were built from (see the [`adjacency`](crate::adjacency) manifest); a
//! mismatch against the current value marks the index **stale**, and a stale
//! or missing index falls back to scan-and-build — identical results, only
//! slower. Property-only writes (`properties/`, `edge_properties/`) never bump
//! the topology counter because properties cannot change adjacency.
//!
//! Search artifacts use the sibling `search_generation`. It advances for node
//! topology and node-property commits, but not for edge-only, provenance, or
//! knowledge-layer writes. Older projects without this key inherit the current
//! topology generation until their first search-relevant mutation.
//!
//! # Crash-safety invariant
//!
//! [`commit_topology_aware`] bumps the counter **strictly before** the first
//! rename of the staged batch. A crash after the bump but before (or during)
//! the commit leaves the counter advanced over an unchanged or partially
//! renamed topology — any existing index now merely *looks* stale and is
//! rebuilt, costing one spurious rebuild. The reverse order would be unsound:
//! a crash between commit and bump would leave new topology under the old
//! counter, making a stale index look **fresh** and silently serving wrong
//! traversals. Spurious bumps are safe; missed bumps are not.
//!
//! Multi-process writers can lose a bump (read-increment-rename is not
//! cross-process atomic); this matches the consistency envelope of every
//! Parquet rewrite in this embedded engine (see [`crate::staging`]).

use std::io::Write;
use std::path::{Path, PathBuf};

use graphforge_core::GfError;

use crate::staging::RewriteBatch;

/// JSON key holding the counter inside `topology/generation.json`.
const GENERATION_KEY: &str = "topology_generation";
/// JSON key holding the graph-native search source counter.
const SEARCH_GENERATION_KEY: &str = "search_generation";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct GenerationState {
    topology: u64,
    search: u64,
}

fn storage_err(e: impl std::fmt::Display) -> GfError {
    GfError::Storage(e.to_string())
}

/// Path of the generation counter file within `project_dir`:
/// `topology/generation.json`.
#[must_use]
pub fn generation_path(project_dir: &Path) -> PathBuf {
    project_dir.join("topology").join("generation.json")
}

/// The project's current topology generation.
///
/// A missing file is generation **0** (a project that has never written
/// topology with a counter-aware binary), mirroring the absent-file semantics
/// of the catalog readers. This is sound because no index manifest can
/// predate the counter: every binary that writes `indexes/` also bumps.
///
/// # Errors
/// Returns [`GfError::Storage`] if the file exists but cannot be read or is
/// not of the form `{"topology_generation": N}` — callers treating the index
/// as a capability must then consider it always-stale, never fresh.
pub fn read_topology_generation(project_dir: &Path) -> Result<u64, GfError> {
    Ok(read_generation_state(project_dir)?.topology)
}

/// The generation of the committed node topology and node properties consumed
/// by graph-native search.
///
/// A legacy counter without `search_generation` inherits
/// `topology_generation`, preserving reopen compatibility. A missing counter
/// file is generation zero.
///
/// # Errors
/// Returns [`GfError::Storage`] when the counter exists but is corrupt or
/// unreadable.
pub fn read_search_generation(project_dir: &Path) -> Result<u64, GfError> {
    Ok(read_generation_state(project_dir)?.search)
}

fn read_generation_state(project_dir: &Path) -> Result<GenerationState, GfError> {
    let path = generation_path(project_dir);
    let contents = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(GenerationState::default());
        }
        Err(e) => {
            return Err(GfError::Storage(format!(
                "cannot read {}: {e}",
                path.display()
            )));
        }
    };
    let value: serde_json::Value = serde_json::from_str(&contents)
        .map_err(|e| GfError::Storage(format!("corrupt {}: {e}", path.display())))?;
    let topology = value
        .get(GENERATION_KEY)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            GfError::Storage(format!(
                "corrupt {}: expected {{\"{GENERATION_KEY}\": <u64>}}",
                path.display()
            ))
        })?;
    let search = match value.get(SEARCH_GENERATION_KEY) {
        Some(value) => value.as_u64().ok_or_else(|| {
            GfError::Storage(format!(
                "corrupt {}: expected \"{SEARCH_GENERATION_KEY}\" to be a u64",
                path.display()
            ))
        })?,
        None => topology,
    };
    Ok(GenerationState { topology, search })
}

/// Atomically persist `current + 1` (sibling temp + rename) and return the
/// new value. Creates `topology/` if needed.
///
/// # Errors
/// Returns [`GfError::Storage`] if the current value cannot be read (corrupt
/// file) or on I/O failure; on failure the prior file is untouched.
pub fn bump_topology_generation(project_dir: &Path) -> Result<u64, GfError> {
    Ok(bump_generations(project_dir, true, false)?.topology)
}

/// Atomically advance and persist the graph-native search generation.
///
/// # Errors
/// Returns [`GfError::Storage`] if the existing generation is corrupt or the
/// replacement cannot be persisted.
pub fn bump_search_generation(project_dir: &Path) -> Result<u64, GfError> {
    Ok(bump_generations(project_dir, false, true)?.search)
}

fn bump_generations(
    project_dir: &Path,
    bump_topology: bool,
    bump_search: bool,
) -> Result<GenerationState, GfError> {
    let mut next = read_generation_state(project_dir)?;
    if bump_topology {
        next.topology = next
            .topology
            .checked_add(1)
            .ok_or_else(|| GfError::Storage("topology generation counter overflow".to_owned()))?;
    }
    if bump_search {
        next.search = next
            .search
            .checked_add(1)
            .ok_or_else(|| GfError::Storage("search generation counter overflow".to_owned()))?;
    }
    let path = generation_path(project_dir);
    let parent = path.parent().expect("generation path always has a parent");
    std::fs::create_dir_all(parent).map_err(storage_err)?;
    let mut tmp = tempfile::Builder::new()
        .prefix("generation.json.")
        .suffix(".tmp")
        .tempfile_in(parent)
        .map_err(storage_err)?;
    let body = serde_json::json!({
        GENERATION_KEY: next.topology,
        SEARCH_GENERATION_KEY: next.search,
    })
    .to_string();
    tmp.write_all(body.as_bytes()).map_err(storage_err)?;
    tmp.as_file().sync_all().map_err(storage_err)?;
    tmp.persist(&path).map_err(|e| storage_err(e.error))?;
    sync_directory(parent)?;
    Ok(next)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), GfError> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(storage_err)
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> Result<(), GfError> {
    Ok(())
}

/// Whether any staged destination in `staged` rewrites topology:
/// `topology/nodes.parquet` or any file under `topology/edges/` (including
/// `_exploratory.parquet`). Paths elsewhere (`properties/`,
/// `edge_properties/`, `provenance/`, …) do not count.
#[must_use]
pub fn touches_topology(staged: &RewriteBatch, project_dir: &Path) -> bool {
    let topology = project_dir.join("topology");
    let nodes = topology.join("nodes.parquet");
    let edges = topology.join("edges");
    staged
        .staged_paths()
        .any(|path| path == nodes || path.starts_with(&edges))
}

/// Whether a staged batch changes graph-native search inputs: node identity or
/// label membership (`topology/nodes.parquet`) or node properties
/// (`properties/`). Edge-only and knowledge-layer writes are intentionally
/// excluded.
#[must_use]
pub fn touches_search_source(staged: &RewriteBatch, project_dir: &Path) -> bool {
    let nodes = project_dir.join("topology").join("nodes.parquet");
    let properties = project_dir.join("properties");
    staged
        .staged_paths()
        .any(|path| path == nodes || path.starts_with(&properties))
}

/// Commit `staged`, bumping each affected generation **first** (see the module
/// docs for why bump-before-commit is the only sound order). Edge topology
/// advances only topology; node topology advances topology and search; node
/// properties advance only search.
///
/// Returns `Some(new_generation)` when the batch bumped, `None` otherwise — the
/// caller tags an adjacency delta segment (#765) with the returned value rather
/// than re-reading the counter (which a concurrent bump could have advanced).
///
/// # Errors
/// Returns [`GfError::Storage`] on bump or rename failure. A bump followed by
/// a failed commit leaves the counter advanced — safe (the index reads as
/// stale), see the crash-safety invariant.
pub fn commit_topology_aware(
    staged: RewriteBatch,
    project_dir: &Path,
) -> Result<Option<u64>, GfError> {
    let topology = touches_topology(&staged, project_dir);
    let search = touches_search_source(&staged, project_dir);
    let bumped = if topology || search {
        let generations = bump_generations(project_dir, topology, search)?;
        topology.then_some(generations.topology)
    } else {
        None
    };
    staged.commit()?;
    Ok(bumped)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use tempfile::TempDir;

    use super::*;

    fn int_batch() -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        (schema, batch)
    }

    fn staged_for(dir: &Path, rel_paths: &[&str]) -> RewriteBatch {
        let mut staged = RewriteBatch::new();
        for rel in rel_paths {
            let (schema, batch) = int_batch();
            staged.stage(&dir.join(rel), schema, &batch).unwrap();
        }
        staged
    }

    #[test]
    fn missing_file_reads_as_generation_zero() {
        let dir = TempDir::new().unwrap();
        assert_eq!(read_topology_generation(dir.path()).unwrap(), 0);
        assert_eq!(read_search_generation(dir.path()).unwrap(), 0);
    }

    #[test]
    fn bump_increments_and_persists() {
        let dir = TempDir::new().unwrap();
        assert_eq!(bump_topology_generation(dir.path()).unwrap(), 1);
        assert_eq!(bump_topology_generation(dir.path()).unwrap(), 2);
        assert_eq!(bump_topology_generation(dir.path()).unwrap(), 3);
        assert_eq!(read_topology_generation(dir.path()).unwrap(), 3);
        assert_eq!(read_search_generation(dir.path()).unwrap(), 0);
        // No temp residue next to the counter.
        let temps = std::fs::read_dir(dir.path().join("topology"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().is_some_and(|x| x == "tmp"))
            .count();
        assert_eq!(temps, 0);
    }

    #[test]
    fn corrupt_file_is_an_error_not_zero() {
        let dir = TempDir::new().unwrap();
        let path = generation_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();

        for bad in ["not json", "{}", "{\"topology_generation\": -1}", "[3]"] {
            std::fs::write(&path, bad).unwrap();
            assert!(
                matches!(
                    read_topology_generation(dir.path()),
                    Err(GfError::Storage(_))
                ),
                "{bad:?} must not parse"
            );
            // A corrupt counter must also fail the bump, not silently reset.
            assert!(matches!(
                bump_topology_generation(dir.path()),
                Err(GfError::Storage(_))
            ));
        }
    }

    #[test]
    fn touches_topology_matrix() {
        let dir = TempDir::new().unwrap();
        for (rel, topology, search) in [
            ("topology/nodes.parquet", true, true),
            ("topology/edges/KNOWS.parquet", true, false),
            ("topology/edges/_exploratory.parquet", true, false),
            ("topology/runtime_catalog.parquet", false, false),
            ("properties/Person.parquet", false, true),
            ("edge_properties/KNOWS.parquet", false, false),
            ("auxiliary/records.parquet", false, false),
        ] {
            let staged = staged_for(dir.path(), &[rel]);
            assert_eq!(
                touches_topology(&staged, dir.path()),
                topology,
                "{rel} should {}count as topology",
                if topology { "" } else { "not " }
            );
            assert_eq!(
                touches_search_source(&staged, dir.path()),
                search,
                "{rel} should {}count as a search source",
                if search { "" } else { "not " }
            );
        }
    }

    #[test]
    fn commit_topology_aware_bumps_only_for_topology() {
        let dir = TempDir::new().unwrap();

        // Property-only batch: commit, no bump.
        let staged = staged_for(dir.path(), &["properties/Person.parquet"]);
        commit_topology_aware(staged, dir.path()).unwrap();
        assert_eq!(read_topology_generation(dir.path()).unwrap(), 0);
        assert_eq!(read_search_generation(dir.path()).unwrap(), 1);

        // Mixed batch staging topology: exactly one bump.
        let staged = staged_for(
            dir.path(),
            &["topology/nodes.parquet", "properties/Person.parquet"],
        );
        commit_topology_aware(staged, dir.path()).unwrap();
        assert_eq!(read_topology_generation(dir.path()).unwrap(), 1);
        assert_eq!(read_search_generation(dir.path()).unwrap(), 2);
        assert!(dir.path().join("topology/nodes.parquet").exists());

        // Edge-only topology advances adjacency without invalidating search.
        let staged = staged_for(dir.path(), &["topology/edges/KNOWS.parquet"]);
        commit_topology_aware(staged, dir.path()).unwrap();
        assert_eq!(read_topology_generation(dir.path()).unwrap(), 2);
        assert_eq!(read_search_generation(dir.path()).unwrap(), 2);
    }

    #[test]
    fn legacy_counter_seeds_search_generation() {
        let dir = TempDir::new().unwrap();
        let path = generation_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, r#"{"topology_generation":7}"#).unwrap();

        assert_eq!(read_search_generation(dir.path()).unwrap(), 7);
        assert_eq!(bump_search_generation(dir.path()).unwrap(), 8);
        assert_eq!(read_topology_generation(dir.path()).unwrap(), 7);
    }
}
