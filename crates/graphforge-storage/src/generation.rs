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
//! [`commit_topology_aware`] serializes writers, durably authenticates every
//! retained replacement in a bounded intent journal, rolls data files forward,
//! and publishes this counter last as the explicit authority switch. A crash at
//! any barrier is replayed idempotently before the generation is read. Thus a
//! generation can never describe a prefix of the intended topology and a
//! completed topology can never remain authoritative under its prior counter.

use std::path::{Path, PathBuf};

use graphforge_core::GfError;

use crate::staging::RewriteBatch;

/// JSON key holding the counter inside `topology/generation.json`.
const GENERATION_KEY: &str = "topology_generation";
/// JSON key holding the graph-native search source counter.
const SEARCH_GENERATION_KEY: &str = "search_generation";
const PROPERTY_GENERATION_KEY: &str = "property_generation";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct GenerationState {
    pub(crate) topology: u64,
    pub(crate) search: u64,
    pub(crate) property: u64,
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

/// Return the latest committed property-fragment generation.
///
/// Legacy authorities without an explicit property counter migrate logically
/// from the greater of the topology and search counters.
pub fn read_property_generation(project_dir: &Path) -> Result<u64, GfError> {
    Ok(read_generation_state(project_dir)?.property)
}

pub(crate) fn read_generation_state_raw(project_dir: &Path) -> Result<GenerationState, GfError> {
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
    let property = match value.get(PROPERTY_GENERATION_KEY) {
        Some(value) => value.as_u64().ok_or_else(|| {
            GfError::Storage(format!(
                "corrupt {}: expected \"{PROPERTY_GENERATION_KEY}\" to be a u64",
                path.display()
            ))
        })?,
        None => topology.max(search),
    };
    Ok(GenerationState {
        topology,
        search,
        property,
    })
}

pub(crate) fn encode_generation_state(
    topology: u64,
    search: u64,
    property: u64,
) -> Result<Vec<u8>, GfError> {
    serde_json::to_vec(&serde_json::json!({
        GENERATION_KEY: topology,
        SEARCH_GENERATION_KEY: search,
        PROPERTY_GENERATION_KEY: property,
    }))
    .map_err(storage_err)
}

fn read_generation_state(project_dir: &Path) -> Result<GenerationState, GfError> {
    if crate::durable_rewrite::recovery_required(project_dir)? {
        crate::durable_rewrite::recover(project_dir)?;
    }
    read_generation_state_raw(project_dir)
}

/// Atomically persist `current + 1` (sibling temp + rename) and return the
/// new value. Creates `topology/` if needed.
///
/// # Errors
/// Returns [`GfError::Storage`] if the current value cannot be read (corrupt
/// file) or on I/O failure; on failure the prior file is untouched.
pub fn bump_topology_generation(project_dir: &Path) -> Result<u64, GfError> {
    Ok(
        crate::durable_rewrite::commit(RewriteBatch::new(), project_dir, true, false, false, None)?
            .topology,
    )
}

/// Atomically advance and persist the graph-native search generation.
///
/// # Errors
/// Returns [`GfError::Storage`] if the existing generation is corrupt or the
/// replacement cannot be persisted.
pub fn bump_search_generation(project_dir: &Path) -> Result<u64, GfError> {
    Ok(
        crate::durable_rewrite::commit(RewriteBatch::new(), project_dir, false, true, false, None)?
            .search,
    )
}

/// Whether any staged destination in `staged` rewrites topology:
/// `topology/nodes.parquet`, any immutable node shard under `topology/nodes/`,
/// or any file under `topology/edges/` (including
/// `_exploratory.parquet`). Paths elsewhere (`properties/`,
/// `edge_properties/`, `provenance/`, …) do not count.
#[must_use]
pub fn touches_topology(staged: &RewriteBatch, project_dir: &Path) -> bool {
    let topology = project_dir.join("topology");
    let nodes = topology.join("nodes.parquet");
    let node_shards = topology.join("nodes");
    let edges = topology.join("edges");
    staged
        .staged_paths()
        .any(|path| path == nodes || path.starts_with(&node_shards) || path.starts_with(&edges))
}

/// Whether a staged batch changes graph-native search inputs: node identity or
/// label membership (`topology/nodes.parquet` or an immutable node shard) or node properties
/// (`properties/`). Edge-only and knowledge-layer writes are intentionally
/// excluded.
#[must_use]
pub fn touches_search_source(staged: &RewriteBatch, project_dir: &Path) -> bool {
    let nodes = project_dir.join("topology").join("nodes.parquet");
    let node_shards = project_dir.join("topology").join("nodes");
    let properties = project_dir.join("properties");
    staged.has_node_property_windows()
        || staged.staged_paths().any(|path| {
            path == nodes || path.starts_with(&node_shards) || path.starts_with(&properties)
        })
}

/// Durably commit `staged`, publishing each affected generation **last** (see
/// the module-level crash invariant). Edge topology
/// advances only topology; node topology advances topology and search; node
/// properties advance only search.
///
/// Returns `Some(new_generation)` when the batch bumped, `None` otherwise — the
/// caller tags an adjacency delta segment (#765) with the returned value rather
/// than re-reading the counter (which a concurrent bump could have advanced).
///
/// # Errors
/// Returns [`GfError::Storage`] on admission, journal, authentication, replay,
/// or namespace-durability failure. A durable intent is always rolled forward
/// on retry/reopen; failures before intent preserve the prior authority.
pub fn commit_topology_aware(
    staged: RewriteBatch,
    project_dir: &Path,
) -> Result<Option<u64>, GfError> {
    commit_topology_aware_with_auxiliary(staged, project_dir, None)
}

/// Commit a rewrite with an authenticated typed auxiliary receipt bound to a
/// staged destination in the same generation-last transaction.
pub fn commit_topology_aware_with_auxiliary(
    staged: RewriteBatch,
    project_dir: &Path,
    auxiliary: Option<crate::AuxiliaryReceipt>,
) -> Result<Option<u64>, GfError> {
    let topology = touches_topology(&staged, project_dir);
    let search = touches_search_source(&staged, project_dir);
    let property = staged.has_property_windows();
    let generations =
        crate::durable_rewrite::commit(staged, project_dir, topology, search, property, auxiliary)?;
    Ok(topology.then_some(generations.topology))
}

/// Commit a rewrite whose auxiliary participant is prepared only after
/// recovery and authoritative next-generation derivation, while the retained
/// project rewrite lock remains held.
pub fn commit_topology_aware_with_participant(
    staged: RewriteBatch,
    project_dir: &Path,
    participant: crate::RewriteParticipantPreparer<'_>,
) -> Result<Option<u64>, GfError> {
    let topology = touches_topology(&staged, project_dir);
    let search = touches_search_source(&staged, project_dir);
    let property = touches_property_data(&staged, project_dir);
    let generations = crate::durable_rewrite::commit_with_participant(
        staged,
        project_dir,
        topology,
        search,
        property,
        None,
        Some(participant),
    )?;
    Ok(topology.then_some(generations.topology))
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
        assert_eq!(read_property_generation(dir.path()).unwrap(), 0);
        assert!(!dir.path().join(".graphforge-rewrite.lock").exists());

        let missing = dir.path().join("missing-project");
        assert_eq!(read_topology_generation(&missing).unwrap(), 0);
        assert_eq!(read_search_generation(&missing).unwrap(), 0);
        assert_eq!(read_property_generation(&missing).unwrap(), 0);
        assert!(!missing.exists());
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
    fn corrupt_rewrite_journal_fails_closed_without_changing_authority() {
        let dir = TempDir::new().unwrap();
        assert_eq!(bump_topology_generation(dir.path()).unwrap(), 1);
        std::fs::write(
            dir.path().join(".graphforge-rewrite-v1.json"),
            br#"{"version":1,"checksum":"forged"}"#,
        )
        .unwrap();
        let before = std::fs::read(generation_path(dir.path())).unwrap();
        assert!(read_topology_generation(dir.path()).is_err());
        assert_eq!(std::fs::read(generation_path(dir.path())).unwrap(), before);
    }

    #[test]
    fn touches_topology_matrix() {
        let dir = TempDir::new().unwrap();
        for (rel, topology, search) in [
            ("topology/nodes.parquet", true, true),
            ("topology/nodes/0001-0002.parquet", true, true),
            ("topology/nodes2/0001-0002.parquet", false, false),
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
        // Repeated reopen/recovery is idempotent and consumes the intent.
        assert_eq!(read_topology_generation(dir.path()).unwrap(), 0);
        assert_eq!(read_search_generation(dir.path()).unwrap(), 1);
        assert!(!dir.path().join(".graphforge-rewrite-v1.json").exists());

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
        assert_eq!(read_property_generation(dir.path()).unwrap(), 7);
    }

    #[test]
    fn legacy_counter_seeds_property_generation_from_highest_authority() {
        let dir = TempDir::new().unwrap();
        let path = generation_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, r#"{"topology_generation":7,"search_generation":11}"#).unwrap();

        assert_eq!(read_property_generation(dir.path()).unwrap(), 11);
        bump_topology_generation(dir.path()).unwrap();
        let state = read_generation_state_raw(dir.path()).unwrap();
        assert_eq!(state.property, 11);
        let persisted: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(persisted[PROPERTY_GENERATION_KEY], 11);
    }
}
