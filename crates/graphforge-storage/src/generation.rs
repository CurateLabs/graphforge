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

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use graphforge_core::GfError;

use crate::staging::RewriteBatch;

/// JSON key holding the counter inside `topology/generation.json`.
const GENERATION_KEY: &str = "topology_generation";
const SEALED_GRAPH_DELTA_FILE: &str = ".graphforge-sealed-graph-delta.json";

/// A pending logical graph-file change awaiting durable generation publication.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "operation")]
pub enum GraphFileDeltaDescriptor {
    /// Install or replace this exact logical file from the private workspace.
    Sealed {
        /// Canonical graph-relative path.
        relative_path: PathBuf,
        /// Unique identity of this exact workspace mutation.
        revision_uuid: uuid::Uuid,
    },
    /// Remove this exact logical file from the durable logical manifest.
    Tombstone {
        /// Canonical graph-relative path.
        relative_path: PathBuf,
        /// Unique identity of this exact workspace mutation.
        revision_uuid: uuid::Uuid,
    },
}

/// Exact descriptor snapshot captured for one optimistic publication attempt.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PendingGraphFileDelta {
    /// Existing files to seal into CAS.
    pub sealed_paths: Vec<PathBuf>,
    /// Logical files to remove from the manifest.
    pub tombstones: Vec<String>,
    /// Exact operations used to avoid acknowledging a newer same-path change.
    pub descriptors: BTreeMap<PathBuf, GraphFileDeltaDescriptor>,
}

/// Read the pending sealed and tombstone descriptor snapshot.
pub fn pending_graph_file_delta(project_dir: &Path) -> Result<PendingGraphFileDelta, GfError> {
    let descriptors = read_graph_file_delta(project_dir)?;
    let mut pending = PendingGraphFileDelta::default();
    for descriptor in descriptors.values() {
        match descriptor {
            GraphFileDeltaDescriptor::Sealed { relative_path, .. } => {
                pending.sealed_paths.push(relative_path.clone());
            }
            GraphFileDeltaDescriptor::Tombstone { relative_path, .. } => {
                pending.tombstones.push(path_string(relative_path)?);
            }
        }
    }
    pending.descriptors = descriptors;
    Ok(pending)
}

/// Exact logical graph paths committed in the private workspace but not yet
/// acknowledged by a durable project-generation publication.
pub fn sealed_graph_delta(project_dir: &Path) -> Result<Vec<PathBuf>, GfError> {
    Ok(pending_graph_file_delta(project_dir)?.sealed_paths)
}

fn read_graph_file_delta(
    project_dir: &Path,
) -> Result<BTreeMap<PathBuf, GraphFileDeltaDescriptor>, GfError> {
    let path = project_dir.join(SEALED_GRAPH_DELTA_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(storage_err(error)),
    };
    let descriptors: Vec<GraphFileDeltaDescriptor> = serde_json::from_slice(&bytes)
        .or_else(|_| {
            serde_json::from_slice::<Vec<String>>(&bytes).map(|legacy| {
                legacy
                    .into_iter()
                    .map(|relative_path| GraphFileDeltaDescriptor::Sealed {
                        relative_path: PathBuf::from(relative_path),
                        revision_uuid: uuid::Uuid::nil(),
                    })
                    .collect()
            })
        })
        .map_err(storage_err)?;
    let mut by_path = BTreeMap::new();
    for descriptor in descriptors {
        let path = match &descriptor {
            GraphFileDeltaDescriptor::Sealed { relative_path, .. }
            | GraphFileDeltaDescriptor::Tombstone { relative_path, .. } => relative_path,
        };
        validate_delta_path(path)?;
        by_path.insert(path.clone(), descriptor);
    }
    Ok(by_path)
}

/// Clear the exact descriptor snapshot after its generation becomes CURRENT.
/// A newer commit added concurrently is retained rather than accidentally
/// acknowledged with the older publication.
pub fn acknowledge_sealed_graph_delta(
    project_dir: &Path,
    published: &PendingGraphFileDelta,
) -> Result<(), GfError> {
    let mut current = read_graph_file_delta(project_dir)?;
    for (path, published_descriptor) in &published.descriptors {
        if current.get(path) == Some(published_descriptor) {
            current.remove(path);
        }
    }
    write_graph_file_delta(project_dir, &current)
}

fn record_sealed_graph_delta(staged: &RewriteBatch, project_dir: &Path) -> Result<(), GfError> {
    let mut descriptors = read_graph_file_delta(project_dir)?;
    for path in staged.staged_paths() {
        let relative = path.strip_prefix(project_dir).map_err(|_| {
            GfError::Validation("staged graph path escapes the private workspace".into())
        })?;
        let relative_path = relative.to_path_buf();
        descriptors.insert(
            relative_path.clone(),
            GraphFileDeltaDescriptor::Sealed {
                relative_path,
                revision_uuid: uuid::Uuid::new_v4(),
            },
        );
    }
    write_graph_file_delta(project_dir, &descriptors)
}

/// Merge authoritative sealed/tombstone descriptors into the pending journal.
///
/// Callers must record descriptors before making the corresponding workspace
/// mutation visible. A later descriptor for the same path supersedes the
/// earlier operation.
pub fn record_graph_file_descriptors(
    project_dir: &Path,
    descriptors: impl IntoIterator<Item = GraphFileDeltaDescriptor>,
) -> Result<(), GfError> {
    let mut current = read_graph_file_delta(project_dir)?;
    for descriptor in descriptors {
        let path = match &descriptor {
            GraphFileDeltaDescriptor::Sealed { relative_path, .. }
            | GraphFileDeltaDescriptor::Tombstone { relative_path, .. } => relative_path,
        };
        validate_delta_path(path)?;
        current.insert(path.clone(), descriptor);
    }
    write_graph_file_delta(project_dir, &current)
}

fn write_graph_file_delta(
    project_dir: &Path,
    descriptors: &BTreeMap<PathBuf, GraphFileDeltaDescriptor>,
) -> Result<(), GfError> {
    let destination = project_dir.join(SEALED_GRAPH_DELTA_FILE);
    if descriptors.is_empty() {
        match std::fs::remove_file(&destination) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(storage_err(error)),
        }
    }
    let encoded = descriptors.values().collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&encoded).map_err(storage_err)?;
    let temporary = project_dir.join(format!("{SEALED_GRAPH_DELTA_FILE}.tmp"));
    std::fs::write(&temporary, bytes).map_err(storage_err)?;
    std::fs::rename(&temporary, &destination).map_err(storage_err)?;
    Ok(())
}

fn validate_delta_path(path: &Path) -> Result<(), GfError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(GfError::Validation(
            "graph delta contains an unsafe path".into(),
        ));
    }
    if path
        .components()
        .next()
        .is_some_and(|component| component.as_os_str() == std::ffi::OsStr::new(".graphforge-cache"))
    {
        return Err(GfError::Validation(
            "derived cache path cannot be graph authority".into(),
        ));
    }
    Ok(())
}

fn path_string(path: &Path) -> Result<String, GfError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| GfError::Validation("graph delta path is not UTF-8".into()))
}
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
    record_graph_file_descriptors(
        project_dir,
        [GraphFileDeltaDescriptor::Sealed {
            relative_path: PathBuf::from("topology/generation.json"),
            revision_uuid: uuid::Uuid::new_v4(),
        }],
    )?;
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
    let node_shards = topology.join("nodes");
    let edges = topology.join("edges");
    staged
        .staged_paths()
        .any(|path| path == nodes || path.starts_with(&node_shards) || path.starts_with(&edges))
}

/// Whether a staged batch changes graph-native search inputs: node identity or
/// label membership (`topology/nodes.parquet`) or node properties
/// (`properties/`). Edge-only and knowledge-layer writes are intentionally
/// excluded.
#[must_use]
pub fn touches_search_source(staged: &RewriteBatch, project_dir: &Path) -> bool {
    let nodes = project_dir.join("topology").join("nodes.parquet");
    let node_shards = project_dir.join("topology").join("nodes");
    let properties = project_dir.join("properties");
    staged.staged_paths().any(|path| {
        path == nodes || path.starts_with(&node_shards) || path.starts_with(&properties)
    })
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
    record_sealed_graph_delta(&staged, project_dir)?;
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
    use std::collections::BTreeSet;
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
    fn sealed_delta_accumulates_until_exact_paths_are_acknowledged() {
        let dir = TempDir::new().unwrap();
        commit_topology_aware(
            staged_for(dir.path(), &["properties/A.parquet"]),
            dir.path(),
        )
        .unwrap();
        commit_topology_aware(
            staged_for(dir.path(), &["topology/nodes/2-2.parquet"]),
            dir.path(),
        )
        .unwrap();
        let sealed = sealed_graph_delta(dir.path()).unwrap();
        assert_eq!(
            sealed,
            vec![
                PathBuf::from("properties/A.parquet"),
                PathBuf::from("topology/generation.json"),
                PathBuf::from("topology/nodes/2-2.parquet")
            ]
        );
        let captured = pending_graph_file_delta(dir.path()).unwrap();

        acknowledge_sealed_graph_delta(
            dir.path(),
            &PendingGraphFileDelta {
                sealed_paths: vec![sealed[0].clone()],
                tombstones: vec![],
                descriptors: BTreeMap::from([(
                    sealed[0].clone(),
                    captured.descriptors[&sealed[0]].clone(),
                )]),
            },
        )
        .unwrap();
        assert_eq!(
            sealed_graph_delta(dir.path()).unwrap(),
            vec![sealed[1].clone(), sealed[2].clone()]
        );
        acknowledge_sealed_graph_delta(
            dir.path(),
            &PendingGraphFileDelta {
                sealed_paths: vec![sealed[1].clone(), sealed[2].clone()],
                tombstones: vec![],
                descriptors: BTreeMap::from([
                    (sealed[1].clone(), captured.descriptors[&sealed[1]].clone()),
                    (sealed[2].clone(), captured.descriptors[&sealed[2]].clone()),
                ]),
            },
        )
        .unwrap();
        assert!(sealed_graph_delta(dir.path()).unwrap().is_empty());
    }

    #[test]
    fn authoritative_write_family_census_emits_transactional_descriptors() {
        // This is the executable census for graph-workspace authority.  Every
        // Parquet mutation site funnels through RewriteBatch and
        // commit_topology_aware; the representatives cover every routed file
        // family plus caller-defined authoritative extensions.  The three
        // non-Parquet control writers are asserted below through their shared
        // descriptor journal.
        let dir = TempDir::new().unwrap();
        let rewrite_families = [
            "topology/nodes/00000000000000000001-00000000000000000001.parquet",
            "topology/edges/KNOWS/00000000000000000001-00000000000000000001.parquet",
            "properties/Person.parquet",
            "edge_properties/KNOWS.parquet",
            "deltas/00000000000000000001.parquet",
            "catalog/state.parquet",
            "extensions/vendor-authority.parquet",
        ];
        commit_topology_aware(staged_for(dir.path(), &rewrite_families), dir.path()).unwrap();

        let direct_controls = [
            "topology/runtime_catalog.parquet",
            "topology/runtime_entity_label_encoding.json",
        ];
        for relative in direct_controls {
            record_graph_file_descriptors(
                dir.path(),
                [GraphFileDeltaDescriptor::Sealed {
                    relative_path: PathBuf::from(relative),
                    revision_uuid: uuid::Uuid::new_v4(),
                }],
            )
            .unwrap();
            let path = dir.path().join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, b"control").unwrap();
        }

        let pending = pending_graph_file_delta(dir.path()).unwrap();
        let mut expected: BTreeSet<_> = rewrite_families.into_iter().map(PathBuf::from).collect();
        expected.extend(direct_controls.into_iter().map(PathBuf::from));
        // Topology mutation writes generation.json through its own
        // descriptor-before-persist control path.
        expected.insert(PathBuf::from("topology/generation.json"));
        assert_eq!(
            pending.descriptors.keys().cloned().collect::<BTreeSet<_>>(),
            expected
        );

        let (inventory, _) = crate::capture_graph_files(dir.path()).unwrap();
        let authoritative: BTreeSet<_> = inventory
            .files
            .into_iter()
            .map(|entry| PathBuf::from(entry.relative_path))
            .collect();
        assert_eq!(authoritative, expected);

        // Rebuildable indexes are cache-owned and cannot enter either census.
        let cache = dir.path().join(".graphforge-cache/adjacency/index.parquet");
        std::fs::create_dir_all(cache.parent().unwrap()).unwrap();
        std::fs::write(cache, b"derived").unwrap();
        let (inventory, _) = crate::capture_graph_files(dir.path()).unwrap();
        assert!(
            !inventory
                .files
                .iter()
                .any(|entry| { entry.relative_path.starts_with(".graphforge-cache/") })
        );
        assert!(
            record_graph_file_descriptors(
                dir.path(),
                [GraphFileDeltaDescriptor::Sealed {
                    relative_path: PathBuf::from(".graphforge-cache/adjacency/index.parquet"),
                    revision_uuid: uuid::Uuid::new_v4(),
                }],
            )
            .is_err()
        );
    }

    #[test]
    fn tombstone_supersedes_sealed_descriptor_and_acknowledges_exact_snapshot() {
        let dir = TempDir::new().unwrap();
        let path = PathBuf::from("topology/nodes/1-1.parquet");
        record_graph_file_descriptors(
            dir.path(),
            [GraphFileDeltaDescriptor::Sealed {
                relative_path: path.clone(),
                revision_uuid: uuid::Uuid::new_v4(),
            }],
        )
        .unwrap();
        record_graph_file_descriptors(
            dir.path(),
            [GraphFileDeltaDescriptor::Tombstone {
                relative_path: path.clone(),
                revision_uuid: uuid::Uuid::new_v4(),
            }],
        )
        .unwrap();
        let pending = pending_graph_file_delta(dir.path()).unwrap();
        assert!(pending.sealed_paths.is_empty());
        assert_eq!(pending.tombstones, vec![path_string(&path).unwrap()]);
        acknowledge_sealed_graph_delta(dir.path(), &pending).unwrap();
        assert_eq!(
            pending_graph_file_delta(dir.path()).unwrap(),
            PendingGraphFileDelta::default()
        );
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
