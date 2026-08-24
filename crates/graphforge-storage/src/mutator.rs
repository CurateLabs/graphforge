//! In-place mutation primitives for `DELETE` / `DETACH DELETE` (#740).
//!
//! Unlike [`GraphWriter`](crate::GraphWriter), which buffers and **appends**,
//! these functions **rewrite** committed Parquet files: they read the current
//! on-disk rows, drop the targeted ones (by `node_uuid` / `edge_uuid`), and
//! write the survivors back. They operate directly on a project directory and
//! take effect immediately — there is no buffering.
//!
//! Each file is read, filtered against a keep-mask, and its replacement staged
//! into a [`RewriteBatch`]; a delete's rewrites then commit **all-or-nothing**
//! (#790), with `topology/nodes.parquet` renamed last so even a (rare)
//! rename-phase failure leaves at worst orphaned-but-unreferenced rows, never
//! a deleted node with surviving edges. A failure while building any
//! replacement file leaves the prior state fully intact. Files not touched by
//! a given delete are never opened.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::hash::BuildHasher;
use std::path::Path;

use arrow::array::{Array, FixedSizeBinaryArray, ListArray, RecordBatch, UInt32Array, UInt64Array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;

use graphforge_core::GfError;
use uuid::Uuid;

use crate::catalog::{
    discover_parquet_schema, discover_parquet_schema_detailed, normalize_topology_nodes,
    read_nodes, read_parquet_or_empty,
};
use crate::schemas::TOPOLOGY_NODES_SCHEMA;
use crate::staging::RewriteBatch;

/// Stage label additions for persisted nodes into the statement rewrite batch.
/// Existing labels and the immutable primary `type_id` are preserved.
pub fn stage_add_node_labels<S1: BuildHasher, S2: BuildHasher>(
    staged: &mut RewriteBatch,
    dir: &Path,
    additions: &HashMap<[u8; 16], HashSet<u32, S2>, S1>,
) -> Result<u64, GfError> {
    let removals: HashMap<[u8; 16], HashSet<u32>> = HashMap::new();
    stage_mutate_node_labels(staged, dir, additions, &removals).map(|(added, _)| added)
}

/// Stage label additions and removals for persisted nodes in one topology
/// rewrite. The scalar primary `type_id` remains unchanged as a routing key;
/// `type_ids` is the authoritative label-membership set.
pub fn stage_mutate_node_labels<AS1, AS2, RS1, RS2>(
    staged: &mut RewriteBatch,
    dir: &Path,
    additions: &HashMap<[u8; 16], HashSet<u32, AS2>, AS1>,
    removals: &HashMap<[u8; 16], HashSet<u32, RS2>, RS1>,
) -> Result<(u64, u64), GfError>
where
    AS1: BuildHasher,
    AS2: BuildHasher,
    RS1: BuildHasher,
    RS2: BuildHasher,
{
    if additions.is_empty() && removals.is_empty() {
        return Ok((0, 0));
    }
    let path = dir.join("topology").join("nodes.parquet");
    let read_path = staged
        .staged_temp(&path)
        .map_or_else(|| path.clone(), Path::to_path_buf);
    if !read_path.exists() {
        return Ok((0, 0));
    }
    let batches = normalize_topology_nodes(
        read_parquet_or_empty(&read_path, TOPOLOGY_NODES_SCHEMA.clone()).map_err(pq_err)?,
    )
    .map_err(pq_err)?;
    let mut changed = 0u64;
    let mut removed = 0u64;
    let mut rebuilt = Vec::with_capacity(batches.len());
    for batch in batches {
        let uuids = batch
            .column_by_name("node_uuid")
            .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| GfError::Storage("node topology missing node_uuid".into()))?;
        let labels = batch
            .column_by_name("type_ids")
            .and_then(|a| a.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| GfError::Storage("node topology missing type_ids".into()))?;
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let values = labels.value(row);
            let values = values
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| GfError::Storage("node type_ids are not UInt32".into()))?;
            let mut merged = values.values().to_vec();
            if let Some(drop) = removals.get(&uuid_at(uuids, row)) {
                let before = merged.len();
                merged.retain(|label| !drop.contains(label));
                removed += before.saturating_sub(merged.len()) as u64;
            }
            if let Some(extra) = additions.get(&uuid_at(uuids, row)) {
                let before = merged.len();
                merged.extend(extra.iter().copied());
                merged.sort_unstable();
                merged.dedup();
                changed += merged.len().saturating_sub(before) as u64;
            }
            rows.push(Some(merged.into_iter().map(Some).collect::<Vec<_>>()));
        }
        let nullable = ListArray::from_iter_primitive::<arrow::datatypes::UInt32Type, _, _>(rows);
        let new_labels = ListArray::new(
            std::sync::Arc::new(arrow::datatypes::Field::new(
                "item",
                arrow::datatypes::DataType::UInt32,
                false,
            )),
            nullable.offsets().clone(),
            nullable.values().clone(),
            nullable.nulls().cloned(),
        );
        let index = batch.schema().index_of("type_ids").map_err(pq_err)?;
        let mut columns = batch.columns().to_vec();
        columns[index] = std::sync::Arc::new(new_labels);
        rebuilt.push(RecordBatch::try_new(batch.schema(), columns).map_err(pq_err)?);
    }
    if changed > 0 || removed > 0 {
        let merged =
            arrow::compute::concat_batches(&TOPOLOGY_NODES_SCHEMA, &rebuilt).map_err(pq_err)?;
        staged.restage(&path, TOPOLOGY_NODES_SCHEMA.clone(), &merged)?;
    }
    Ok((changed, removed))
}

fn pq_err(e: impl std::fmt::Display) -> GfError {
    GfError::Storage(e.to_string())
}

fn io_err(e: &std::io::Error) -> GfError {
    GfError::Storage(e.to_string())
}

/// Read a row's `FixedSizeBinary(16)` cell into a `[u8; 16]`.
fn uuid_at(col: &FixedSizeBinaryArray, row: usize) -> [u8; 16] {
    let mut out = [0u8; 16];
    out.copy_from_slice(col.value(row));
    out
}

/// Build a keep-mask: `true` for rows whose `key_col` UUID is **not** in
/// `targets`. Returns `None` (caller skips the rewrite) when no row matches a
/// target, so an untouched file is never rewritten.
fn keep_mask<S: BuildHasher>(
    batch: &RecordBatch,
    key_col: &str,
    targets: &HashSet<[u8; 16], S>,
) -> Result<Option<arrow::array::BooleanArray>, GfError> {
    let col = batch
        .column_by_name(key_col)
        .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
        .ok_or_else(|| GfError::Storage(format!("file missing {key_col} column")))?;
    let mut any_dropped = false;
    let mask: arrow::array::BooleanArray = (0..col.len())
        .map(|r| {
            let keep = !targets.contains(&uuid_at(col, r));
            if !keep {
                any_dropped = true;
            }
            Some(keep)
        })
        .collect();
    Ok(any_dropped.then_some(mask))
}

/// Stage a rewrite of one Parquet file into `staged`, dropping rows whose
/// `key_col` UUID is in `targets`. Returns the number of rows that will be
/// removed once the batch commits. A missing file or no-match stages nothing.
///
/// Reads **through** `staged`: content already staged for this file in the
/// same statement (an earlier SET/REMOVE or append) is the base, so the
/// restaged result is the net of all the statement's effects (#792).
fn stage_rewrite_dropping<S: BuildHasher>(
    staged: &mut RewriteBatch,
    path: &Path,
    schema: SchemaRef,
    key_col: &str,
    targets: &HashSet<[u8; 16], S>,
) -> Result<u64, GfError> {
    let read_path = match staged.staged_temp(path) {
        Some(tmp) => tmp.to_path_buf(),
        None if !path.exists() => return Ok(0),
        None => path.to_path_buf(),
    };
    let batches = read_parquet_or_empty(&read_path, schema.clone()).map_err(pq_err)?;
    let mut removed = 0u64;
    let mut kept: Vec<RecordBatch> = Vec::with_capacity(batches.len());
    for batch in &batches {
        match keep_mask(batch, key_col, targets)? {
            Some(mask) => {
                let before = batch.num_rows() as u64;
                let filtered = filter_record_batch(batch, &mask).map_err(pq_err)?;
                removed += before - filtered.num_rows() as u64;
                kept.push(filtered);
            }
            None => kept.push(batch.clone()),
        }
    }
    if removed == 0 {
        return Ok(0); // nothing in this file matched — leave it untouched
    }
    let merged = arrow::compute::concat_batches(&schema, &kept).map_err(pq_err)?;
    staged.restage(path, schema, &merged)?;
    Ok(removed)
}

fn stage_rewrite_nodes_dropping<S: BuildHasher>(
    staged: &mut RewriteBatch,
    path: &Path,
    targets: &HashSet<[u8; 16], S>,
) -> Result<u64, GfError> {
    let read_path = match staged.staged_temp(path) {
        Some(tmp) => tmp.to_path_buf(),
        None if !path.exists() => return Ok(0),
        None => path.to_path_buf(),
    };
    let Some(stored_schema) = discover_parquet_schema(&read_path) else {
        return Ok(0);
    };
    let batches = read_parquet_or_empty(&read_path, stored_schema).map_err(pq_err)?;
    let batches = normalize_topology_nodes(batches).map_err(pq_err)?;
    let mut removed = 0u64;
    let mut kept = Vec::with_capacity(batches.len());
    for batch in &batches {
        match keep_mask(batch, "node_uuid", targets)? {
            Some(mask) => {
                let before = batch.num_rows() as u64;
                let filtered = filter_record_batch(batch, &mask).map_err(pq_err)?;
                removed += before - filtered.num_rows() as u64;
                kept.push(filtered);
            }
            None => kept.push(batch.clone()),
        }
    }
    if removed == 0 {
        return Ok(0);
    }
    let merged = arrow::compute::concat_batches(&TOPOLOGY_NODES_SCHEMA, &kept).map_err(pq_err)?;
    staged.restage(path, TOPOLOGY_NODES_SCHEMA.clone(), &merged)?;
    Ok(removed)
}

/// Every `*.parquet` path directly under `dir/<subdir>`, or an empty list when
/// the directory is absent. Enumeration order is filesystem-dependent; callers
/// needing determinism must sort or group the results themselves.
pub(crate) fn parquet_files_in(
    dir: &Path,
    subdir: &str,
) -> Result<Vec<std::path::PathBuf>, GfError> {
    let d = dir.join(subdir);
    let entries = match fs::read_dir(&d) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(io_err(&e)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let path = entry.map_err(|e| io_err(&e))?.path();
        if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
    Ok(out)
}

/// Enumerate canonical edge Parquet fragments, including the legacy flat
/// `<relation>.parquet` layout and append-only `<relation>/<range>.parquet`
/// shards. The returned relation is authoritative for typed fragments.
pub(crate) fn edge_parquet_files(
    dir: &Path,
    relation: Option<&str>,
) -> Result<Vec<(String, std::path::PathBuf)>, GfError> {
    let root = dir.join("topology/edges");
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_err(&error)),
    };
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| io_err(&error))?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| io_err(&error))?;
        if file_type.is_symlink() {
            return Err(GfError::Storage(
                "edge topology contains a symbolic link".into(),
            ));
        }
        if file_type.is_file()
            && path.extension().and_then(|value| value.to_str()) == Some("parquet")
        {
            let stem = path
                .file_stem()
                .and_then(|value| value.to_str())
                .ok_or_else(|| GfError::Storage("edge topology file stem is not UTF-8".into()))?;
            if relation.is_none_or(|expected| expected == stem) {
                out.push((stem.to_owned(), path));
            }
            continue;
        }
        if !file_type.is_dir() {
            continue;
        }
        let stem = entry.file_name().into_string().map_err(|_| {
            GfError::Storage("edge topology relation directory is not UTF-8".into())
        })?;
        if relation.is_some_and(|expected| expected != stem) {
            continue;
        }
        for shard in fs::read_dir(&path).map_err(|error| io_err(&error))? {
            let shard = shard.map_err(|error| io_err(&error))?;
            let shard_type = shard.file_type().map_err(|error| io_err(&error))?;
            if shard_type.is_symlink() || shard_type.is_dir() {
                return Err(GfError::Storage(
                    "edge shard directory contains a linked or nested entry".into(),
                ));
            }
            let shard_path = shard.path();
            if shard_type.is_file()
                && shard_path.extension().and_then(|value| value.to_str()) == Some("parquet")
            {
                out.push((stem.clone(), shard_path));
            }
        }
    }
    out.sort_by(|left, right| left.1.cmp(&right.1));
    Ok(out)
}

/// Enumerate the legacy flat node file plus immutable range shards.
pub(crate) fn node_parquet_files(dir: &Path) -> Result<Vec<std::path::PathBuf>, GfError> {
    let topology = dir.join("topology");
    let mut out = Vec::new();
    let legacy = topology.join("nodes.parquet");
    if legacy.exists() {
        out.push(legacy);
    }
    let shards = topology.join("nodes");
    let entries = match fs::read_dir(&shards) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(error) => return Err(io_err(&error)),
    };
    let mut shard_paths = Vec::new();
    let mut prior_end = None;
    for entry in entries {
        let entry = entry.map_err(|error| io_err(&error))?;
        let file_type = entry.file_type().map_err(|error| io_err(&error))?;
        if file_type.is_symlink() || file_type.is_dir() {
            return Err(GfError::Storage(
                "node shard directory contains a linked or nested entry".into(),
            ));
        }
        let path = entry.path();
        if file_type.is_file()
            && path.extension().and_then(|value| value.to_str()) == Some("parquet")
        {
            let stem = path
                .file_stem()
                .and_then(|value| value.to_str())
                .ok_or_else(|| GfError::Storage("node shard name is not canonical UTF-8".into()))?;
            let (first, last) = stem.split_once('-').ok_or_else(|| {
                GfError::Storage("node shard name lacks a surrogate range".into())
            })?;
            if first.len() != 20
                || last.len() != 20
                || !first.bytes().all(|byte| byte.is_ascii_digit())
                || !last.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(GfError::Storage(
                    "node shard name is not a canonical padded range".into(),
                ));
            }
            let first = first
                .parse::<u64>()
                .map_err(|error| GfError::Storage(error.to_string()))?;
            let last = last
                .parse::<u64>()
                .map_err(|error| GfError::Storage(error.to_string()))?;
            if first == 0 || first > last {
                return Err(GfError::Storage("node shard range is invalid".into()));
            }
            shard_paths.push((first, last, path));
        }
    }
    shard_paths.sort_by_key(|(first, _, _)| *first);
    for (first, last, path) in shard_paths {
        if prior_end.is_some_and(|end| first <= end) {
            return Err(GfError::Storage("node shard ranges overlap".into()));
        }
        prior_end = Some(last);
        out.push(path);
    }
    Ok(out)
}

/// Enumerate a legacy flat property file plus immutable construction shards.
pub(crate) fn property_parquet_files(
    dir: &Path,
    subdir: &str,
    stem: &str,
) -> Result<Vec<std::path::PathBuf>, GfError> {
    let root = dir.join(subdir);
    let mut out = Vec::new();
    let legacy = root.join(format!("{stem}.parquet"));
    if legacy.exists() {
        out.push(legacy);
    }
    let shards = root.join(stem);
    let entries = match fs::read_dir(&shards) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(error) => return Err(io_err(&error)),
    };
    for entry in entries {
        let entry = entry.map_err(|error| io_err(&error))?;
        let file_type = entry.file_type().map_err(|error| io_err(&error))?;
        if file_type.is_symlink() || file_type.is_dir() {
            return Err(GfError::Storage(
                "property shard directory contains a linked or nested entry".into(),
            ));
        }
        let path = entry.path();
        if file_type.is_file()
            && path.extension().and_then(|value| value.to_str()) == Some("parquet")
        {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

/// Stage the deletion of the given nodes into `staged`: every
/// `properties/*.parquet` first, then `topology/nodes.parquet` **last** — the
/// authoritative existence record commits only after everything that refers
/// to those nodes (#790). Returns the node rows that will be removed.
///
/// # Errors
/// Returns [`GfError::Storage`] on any I/O, Arrow, or Parquet failure.
#[allow(clippy::implicit_hasher)]
pub fn stage_delete_nodes<S: BuildHasher>(
    staged: &mut RewriteBatch,
    dir: &Path,
    node_uuids: &HashSet<[u8; 16], S>,
) -> Result<u64, GfError> {
    if node_uuids.is_empty() {
        return Ok(0);
    }
    // Drop the deleted nodes' property rows so they don't dangle.
    for stem in crate::catalog::list_property_stems(dir) {
        for path in property_parquet_files(dir, "properties", &stem)? {
            if let Some(schema) = discover_parquet_schema(&path) {
                stage_rewrite_dropping(staged, &path, schema, "node_uuid", node_uuids)?;
            }
        }
    }
    let mut removed = 0_u64;
    for path in node_parquet_files(dir)? {
        removed = removed.saturating_add(stage_rewrite_nodes_dropping(staged, &path, node_uuids)?);
    }
    Ok(removed)
}

/// Stage the deletion of the given edges into `staged`: every
/// `topology/edges/*.parquet`, then any `edge_properties/*.parquet`. Returns
/// the edge rows that will be removed.
///
/// # Errors
/// Returns [`GfError::Storage`] on any I/O, Arrow, or Parquet failure.
#[allow(clippy::implicit_hasher)]
pub fn stage_delete_edges<S: BuildHasher>(
    staged: &mut RewriteBatch,
    dir: &Path,
    edge_uuids: &HashSet<[u8; 16], S>,
) -> Result<u64, GfError> {
    if edge_uuids.is_empty() {
        return Ok(0);
    }
    let mut removed = 0u64;
    for path in parquet_files_in(dir, "topology/edges")? {
        if let Some(schema) = discover_parquet_schema(&path) {
            removed += stage_rewrite_dropping(staged, &path, schema, "edge_uuid", edge_uuids)?;
        }
    }
    // Drop edge-property rows (the `edge_properties/` dir exists once edge
    // properties have been written, #784).
    for stem in crate::catalog::list_edge_property_stems(dir) {
        for path in property_parquet_files(dir, "edge_properties", &stem)? {
            if let Some(schema) = discover_parquet_schema(&path) {
                stage_rewrite_dropping(staged, &path, schema, "edge_uuid", edge_uuids)?;
            }
        }
    }
    Ok(removed)
}

/// Delete the given nodes by `node_uuid`, rewriting `topology/nodes.parquet` and
/// dropping the same nodes' rows from every `properties/*.parquet` file.
///
/// All rewrites stage and commit as one batch, `topology/nodes.parquet` last
/// (#790): a failure while building the replacement files leaves the prior
/// state fully intact.
///
/// Returns the number of node rows removed. Does **not** touch edges — callers
/// enforce openCypher's "no relationships without DETACH" rule (see
/// [`incident_edge_uuids`]) and delete incident edges via [`delete_edges`].
///
/// # Errors
/// Returns [`GfError::Storage`] on any I/O, Arrow, or Parquet failure.
pub fn delete_nodes<S: BuildHasher>(
    dir: &Path,
    node_uuids: &HashSet<[u8; 16], S>,
) -> Result<u64, GfError> {
    let mut staged = RewriteBatch::new();
    let removed = stage_delete_nodes(&mut staged, dir, node_uuids)?;
    let mut snapshot = None;
    if let Some(g) = crate::uuid_membership::commit_uuid_topology_rewrite(
        dir,
        staged,
        crate::uuid_membership::UuidTopologyDelta {
            nodes: Vec::new(),
            edges: Vec::new(),
            deleted_nodes: if removed == 0 {
                Vec::new()
            } else {
                node_uuids.iter().copied().map(Uuid::from_bytes).collect()
            },
            deleted_edges: Vec::new(),
        },
        &mut snapshot,
    )? {
        crate::adjacency_delta::discard_segment(dir, g); // delete writes no segment
    }
    Ok(removed)
}

/// Delete the given edges by `edge_uuid`, rewriting every `topology/edges/*.parquet`
/// file and dropping the same edges' rows from any `edge_properties/*.parquet`.
///
/// All rewrites stage and commit as one batch (#790).
///
/// Returns the number of edge rows removed.
///
/// # Errors
/// Returns [`GfError::Storage`] on any I/O, Arrow, or Parquet failure.
pub fn delete_edges<S: BuildHasher>(
    dir: &Path,
    edge_uuids: &HashSet<[u8; 16], S>,
) -> Result<u64, GfError> {
    let mut staged = RewriteBatch::new();
    let removed = stage_delete_edges(&mut staged, dir, edge_uuids)?;
    let mut snapshot = None;
    if let Some(g) = crate::uuid_membership::commit_uuid_topology_rewrite(
        dir,
        staged,
        crate::uuid_membership::UuidTopologyDelta {
            nodes: Vec::new(),
            edges: Vec::new(),
            deleted_nodes: Vec::new(),
            deleted_edges: if removed == 0 {
                Vec::new()
            } else {
                edge_uuids.iter().copied().map(Uuid::from_bytes).collect()
            },
        },
        &mut snapshot,
    )? {
        crate::adjacency_delta::discard_segment(dir, g); // delete writes no segment
    }
    Ok(removed)
}

/// Delete nodes and edges as **one all-or-nothing statement** (#790): stages
/// every rewrite — edge files, edge properties, node properties, and
/// `topology/nodes.parquet` strictly last — then commits once. A failure while
/// building any replacement file leaves the prior on-disk state fully intact;
/// a (rare) rename-phase failure is bounded by the ordering to consistent
/// states — at worst orphaned-but-unreferenced rows, never a deleted node with
/// surviving edges.
///
/// Returns `(node_rows_removed, edge_rows_removed)`.
///
/// # Errors
/// Returns [`GfError::Storage`] on any I/O, Arrow, or Parquet failure.
#[allow(clippy::implicit_hasher)]
pub fn delete_nodes_and_edges<S: BuildHasher>(
    dir: &Path,
    node_uuids: &HashSet<[u8; 16], S>,
    edge_uuids: &HashSet<[u8; 16], S>,
) -> Result<(u64, u64), GfError> {
    let mut staged = RewriteBatch::new();
    let edges_removed = stage_delete_edges(&mut staged, dir, edge_uuids)?;
    let nodes_removed = stage_delete_nodes(&mut staged, dir, node_uuids)?;
    let mut snapshot = None;
    if let Some(g) = crate::uuid_membership::commit_uuid_topology_rewrite(
        dir,
        staged,
        crate::uuid_membership::UuidTopologyDelta {
            nodes: Vec::new(),
            edges: Vec::new(),
            deleted_nodes: if nodes_removed == 0 {
                Vec::new()
            } else {
                node_uuids.iter().copied().map(Uuid::from_bytes).collect()
            },
            deleted_edges: if edges_removed == 0 {
                Vec::new()
            } else {
                edge_uuids.iter().copied().map(Uuid::from_bytes).collect()
            },
        },
        &mut snapshot,
    )? {
        crate::adjacency_delta::discard_segment(dir, g); // delete writes no segment
    }
    Ok((nodes_removed, edges_removed))
}

/// Return the `edge_uuid`s of every edge incident to any of `node_uuids`
/// (as `src` or `dst`), across all edge files.
///
/// Used to enforce openCypher's `DELETE` semantics: deleting a node that still
/// has relationships **without** `DETACH` is an error; `DETACH DELETE` deletes
/// these incident edges alongside the node.
///
/// Endpoints in the edge files are keyed by the surrogate `src_id`/`dst_id`, so
/// this first maps the target `node_uuid`s to their `node_id`s via
/// `topology/nodes.parquet`, then scans the edge files for those ids.
///
/// # Errors
/// Returns [`GfError::Storage`] on any I/O, Arrow, or Parquet failure.
pub fn incident_edge_uuids<S: BuildHasher>(
    dir: &Path,
    node_uuids: &HashSet<[u8; 16], S>,
) -> Result<Vec<[u8; 16]>, GfError> {
    if node_uuids.is_empty() {
        return Ok(Vec::new());
    }
    // node_uuid → node_id for the targets.
    let target_ids = node_ids_for(dir, node_uuids)?;
    if target_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    for (_, path) in edge_parquet_files(dir, None)? {
        let schema = discover_parquet_schema_detailed(&path).map_err(pq_err)?;
        for batch in read_parquet_or_empty(&path, schema).map_err(pq_err)? {
            let edge_uuid = batch
                .column_by_name("edge_uuid")
                .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(|| GfError::Storage("edge file missing edge_uuid".to_owned()))?;
            let src_id = u64_col(&batch, "src_id")?;
            let dst_id = u64_col(&batch, "dst_id")?;
            for r in 0..batch.num_rows() {
                if target_ids.contains(&src_id.value(r)) || target_ids.contains(&dst_id.value(r)) {
                    out.push(uuid_at(edge_uuid, r));
                }
            }
        }
    }
    Ok(out)
}

/// Resolve the `node_id` surrogates of the given `node_uuid`s from
/// `topology/nodes.parquet`.
fn node_ids_for<S: BuildHasher>(
    dir: &Path,
    node_uuids: &HashSet<[u8; 16], S>,
) -> Result<HashSet<u64>, GfError> {
    let mut ids = HashSet::new();
    for batch in read_nodes(dir).map_err(pq_err)? {
        let uuid = batch
            .column_by_name("node_uuid")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
            .ok_or_else(|| GfError::Storage("nodes file missing node_uuid".to_owned()))?;
        let id = u64_col(&batch, "node_id")?;
        for r in 0..batch.num_rows() {
            if node_uuids.contains(&uuid_at(uuid, r)) {
                ids.insert(id.value(r));
            }
        }
    }
    Ok(ids)
}

/// Borrow a `UInt64` column by name, erroring if absent or mistyped.
fn u64_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a UInt64Array, GfError> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| GfError::Storage(format!("column {name} missing or not UInt64")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use graphforge_core::OntologyMode;
    use graphforge_core::TypeId;
    use graphforge_core::uuid::{Uuid, new_v7, to_bytes};
    use graphforge_ir::IrLiteral;
    use tempfile::TempDir;

    use super::*;
    use crate::GraphWriter;

    const TS: i64 = 1_700_000_000_000_000;

    /// Total rows across the batches of a (possibly absent) parquet file.
    fn row_count(dir: &Path, rel: &str) -> usize {
        let path = dir.join(rel);
        let Some(schema) = discover_parquet_schema(&path) else {
            return 0;
        };
        read_parquet_or_empty(&path, schema)
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum()
    }

    fn set(uuids: &[Uuid]) -> HashSet<[u8; 16]> {
        uuids.iter().map(to_bytes).collect()
    }

    /// Build A--KNOWS-->B--KNOWS-->C in Strict mode; return (dir, a, b, c).
    fn chain() -> (TempDir, Uuid, Uuid, Uuid) {
        let dir = TempDir::new().unwrap();
        let (a, b, c) = (new_v7(), new_v7(), new_v7());
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        w.create_node(a, TypeId(0)).unwrap();
        w.create_node(b, TypeId(0)).unwrap();
        w.create_node(c, TypeId(0)).unwrap();
        w.create_edge(new_v7(), "KNOWS", &a, &b).unwrap();
        w.create_edge(new_v7(), "KNOWS", &b, &c).unwrap();
        w.flush().unwrap();
        (dir, a, b, c)
    }

    #[test]
    fn delete_edges_removes_only_targeted_rows() {
        let dir = TempDir::new().unwrap();
        let (a, b) = (new_v7(), new_v7());
        let (e1, e2) = (new_v7(), new_v7());
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Strict, TS).unwrap();
        w.create_node(a, TypeId(0)).unwrap();
        w.create_node(b, TypeId(0)).unwrap();
        w.create_edge(e1, "KNOWS", &a, &b).unwrap();
        w.create_edge(e2, "KNOWS", &b, &a).unwrap();
        w.flush().unwrap();
        assert_eq!(row_count(dir.path(), "topology/edges/KNOWS.parquet"), 2);

        let removed = delete_edges(dir.path(), &set(&[e1])).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(row_count(dir.path(), "topology/edges/KNOWS.parquet"), 1);
    }

    #[test]
    fn delete_nodes_removes_node_rows_and_leaves_edges() {
        let (dir, a, _b, _c) = chain();
        assert_eq!(row_count(dir.path(), "topology/nodes.parquet"), 3);

        let removed = delete_nodes(dir.path(), &set(&[a])).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(row_count(dir.path(), "topology/nodes.parquet"), 2);
        // delete_nodes does not touch edges — that's the caller's job (DETACH).
        assert_eq!(row_count(dir.path(), "topology/edges/KNOWS.parquet"), 2);
    }

    #[test]
    fn delete_nodes_rewrites_shard_only_matches() {
        let dir = TempDir::new().unwrap();
        let (legacy, sharded) = (new_v7(), new_v7());
        let mut first = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        first.create_node(legacy, TypeId(0)).unwrap();
        first.flush().unwrap();
        let mut second =
            GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS + 1).unwrap();
        second.create_node(sharded, TypeId(0)).unwrap();
        second.flush().unwrap();
        assert_eq!(
            delete_nodes(dir.path(), &HashSet::from([to_bytes(&sharded)])).unwrap(),
            1
        );
        let nodes = crate::read_nodes(dir.path()).unwrap();
        assert_eq!(nodes.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }

    #[test]
    fn delete_nodes_drops_their_property_rows() {
        let dir = TempDir::new().unwrap();
        let (a, b) = (new_v7(), new_v7());
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        w.create_node(a, TypeId(0)).unwrap();
        w.create_node(b, TypeId(0)).unwrap();
        w.set_properties(
            &a,
            None,
            HashMap::from([("name".to_owned(), IrLiteral::Str("A".into()))]),
        )
        .unwrap();
        w.set_properties(
            &b,
            None,
            HashMap::from([("name".to_owned(), IrLiteral::Str("B".into()))]),
        )
        .unwrap();
        w.flush().unwrap();
        assert_eq!(row_count(dir.path(), "properties/_untyped.parquet"), 2);

        delete_nodes(dir.path(), &set(&[a])).unwrap();
        assert_eq!(
            row_count(dir.path(), "properties/_untyped.parquet"),
            1,
            "the deleted node's property row is dropped too"
        );
    }

    #[test]
    fn incident_edge_uuids_finds_edges_on_either_endpoint() {
        // B is the middle of A->B->C, so both edges are incident to B.
        let (dir, _a, b, _c) = chain();
        let incident = incident_edge_uuids(dir.path(), &set(&[b])).unwrap();
        assert_eq!(incident.len(), 2, "both chain edges touch B");
    }

    #[test]
    fn incident_edge_safety_rejects_a_corrupt_canonical_shard() {
        let (dir, a, _b, _c) = chain();
        fs::write(
            dir.path().join("topology/edges/corrupt.parquet"),
            b"not parquet",
        )
        .unwrap();

        assert!(incident_edge_uuids(dir.path(), &set(&[a])).is_err());
    }

    #[test]
    fn incident_edge_uuids_for_leaf_finds_one_edge() {
        // A is only the source of A->B.
        let (dir, a, _b, _c) = chain();
        let incident = incident_edge_uuids(dir.path(), &set(&[a])).unwrap();
        assert_eq!(incident.len(), 1);
    }

    #[test]
    fn detach_delete_flow_removes_node_and_incident_edges() {
        // Emulate DETACH DELETE B: collect incident edges, delete them, delete B.
        let (dir, _a, b, _c) = chain();
        let incident: HashSet<[u8; 16]> = incident_edge_uuids(dir.path(), &set(&[b]))
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(delete_edges(dir.path(), &incident).unwrap(), 2);
        assert_eq!(delete_nodes(dir.path(), &set(&[b])).unwrap(), 1);
        assert_eq!(row_count(dir.path(), "topology/edges/KNOWS.parquet"), 0);
        assert_eq!(row_count(dir.path(), "topology/nodes.parquet"), 2);
    }

    #[test]
    fn empty_target_sets_are_noops() {
        let (dir, _a, _b, _c) = chain();
        assert_eq!(delete_nodes(dir.path(), &HashSet::new()).unwrap(), 0);
        assert_eq!(delete_edges(dir.path(), &HashSet::new()).unwrap(), 0);
        assert!(
            incident_edge_uuids(dir.path(), &HashSet::new())
                .unwrap()
                .is_empty()
        );
        // Nothing changed.
        assert_eq!(row_count(dir.path(), "topology/nodes.parquet"), 3);
        assert_eq!(row_count(dir.path(), "topology/edges/KNOWS.parquet"), 2);
    }

    #[test]
    fn deletes_bump_topology_generation() {
        use crate::generation::read_topology_generation;

        let (dir, _a, b, _c) = chain(); // the fixture's flush is bump #1
        assert_eq!(read_topology_generation(dir.path()).unwrap(), 1);

        let incident: HashSet<[u8; 16]> = incident_edge_uuids(dir.path(), &set(&[b]))
            .unwrap()
            .into_iter()
            .collect();
        assert_eq!(delete_edges(dir.path(), &incident).unwrap(), 2);
        assert_eq!(read_topology_generation(dir.path()).unwrap(), 2);

        assert_eq!(delete_nodes(dir.path(), &set(&[b])).unwrap(), 1);
        assert_eq!(read_topology_generation(dir.path()).unwrap(), 3);
    }

    #[test]
    fn zero_match_delete_does_not_bump_topology_generation() {
        use crate::generation::read_topology_generation;

        let (dir, _a, _b, _c) = chain();
        assert_eq!(read_topology_generation(dir.path()).unwrap(), 1);

        // No matching rows ⇒ nothing staged under topology/ ⇒ no bump.
        assert_eq!(delete_edges(dir.path(), &set(&[new_v7()])).unwrap(), 0);
        assert_eq!(delete_nodes(dir.path(), &set(&[new_v7()])).unwrap(), 0);
        assert_eq!(read_topology_generation(dir.path()).unwrap(), 1);
    }

    // -----------------------------------------------------------------------
    // Staged all-or-nothing commit (#790)
    // -----------------------------------------------------------------------

    /// Chain fixture plus a node property on `a` and an edge property on the
    /// A→B edge, so a full delete touches all four file groups.
    fn chain_with_properties() -> (TempDir, Uuid, Uuid, Uuid, Uuid) {
        let dir = TempDir::new().unwrap();
        let (a, b, c) = (new_v7(), new_v7(), new_v7());
        let e_ab = new_v7();
        let mut w = GraphWriter::open_at(dir.path(), OntologyMode::Exploratory, TS).unwrap();
        w.create_node(a, TypeId(0)).unwrap();
        w.create_node(b, TypeId(0)).unwrap();
        w.create_node(c, TypeId(0)).unwrap();
        w.create_edge(e_ab, "KNOWS", &a, &b).unwrap();
        w.create_edge(new_v7(), "KNOWS", &b, &c).unwrap();
        w.set_properties(
            &a,
            None,
            HashMap::from([("name".to_owned(), IrLiteral::Str("A".into()))]),
        )
        .unwrap();
        w.set_edge_properties(
            &e_ab,
            Some("KNOWS"),
            HashMap::from([("since".to_owned(), IrLiteral::Int(2020))]),
        )
        .unwrap();
        w.flush().unwrap();
        (dir, a, b, c, e_ab)
    }

    #[test]
    fn delete_staging_orders_nodes_parquet_last() {
        // The one hard ordering invariant: the authoritative existence record
        // commits only after every file that refers to the deleted entities.
        let (dir, a, _b, _c, e_ab) = chain_with_properties();

        let mut staged = RewriteBatch::new();
        stage_delete_edges(&mut staged, dir.path(), &set(&[e_ab])).unwrap();
        stage_delete_nodes(&mut staged, dir.path(), &set(&[a])).unwrap();

        let order: Vec<_> = staged.staged_paths().collect();
        assert!(
            order
                .last()
                .is_some_and(|p| p.ends_with("topology/nodes.parquet")),
            "nodes.parquet must commit last, got {order:?}"
        );
        let pos = |suffix: &str| {
            order
                .iter()
                .position(|p| p.to_string_lossy().contains(suffix))
                .unwrap_or_else(|| panic!("{suffix} not staged: {order:?}"))
        };
        assert!(
            pos("topology/edges/") < pos("edge_properties/"),
            "edge files before edge properties: {order:?}"
        );
        // Originals untouched while staged.
        assert_eq!(row_count(dir.path(), "topology/nodes.parquet"), 3);
        assert_eq!(
            row_count(dir.path(), "topology/edges/_exploratory.parquet"),
            2
        );
    }

    #[test]
    fn delete_nodes_and_edges_applies_all_and_reports_counts() {
        let (dir, a, _b, _c, e_ab) = chain_with_properties();

        let (nodes_removed, edges_removed) =
            delete_nodes_and_edges(dir.path(), &set(&[a]), &set(&[e_ab])).unwrap();
        assert_eq!((nodes_removed, edges_removed), (1, 1));
        assert_eq!(row_count(dir.path(), "topology/nodes.parquet"), 2);
        assert_eq!(
            row_count(dir.path(), "topology/edges/_exploratory.parquet"),
            1
        );
        assert_eq!(row_count(dir.path(), "properties/_untyped.parquet"), 0);
        assert_eq!(row_count(dir.path(), "edge_properties/KNOWS.parquet"), 0);

        // No temp residue anywhere the delete touched.
        for sub in [
            "topology",
            "topology/edges",
            "properties",
            "edge_properties",
        ] {
            let residue = fs::read_dir(dir.path().join(sub))
                .unwrap()
                .filter_map(Result::ok)
                .filter(|e| e.path().extension().is_some_and(|x| x == "tmp"))
                .count();
            assert_eq!(residue, 0, "temp residue under {sub}");
        }
    }

    #[test]
    fn dropped_staged_delete_changes_nothing() {
        // The abort path: stage a full delete, drop without commit — the
        // on-disk state stays byte-identical and the temps are gone.
        let (dir, a, _b, _c, e_ab) = chain_with_properties();
        {
            let mut staged = RewriteBatch::new();
            stage_delete_edges(&mut staged, dir.path(), &set(&[e_ab])).unwrap();
            stage_delete_nodes(&mut staged, dir.path(), &set(&[a])).unwrap();
            // Dropped here.
        }
        assert_eq!(row_count(dir.path(), "topology/nodes.parquet"), 3);
        assert_eq!(
            row_count(dir.path(), "topology/edges/_exploratory.parquet"),
            2
        );
        assert_eq!(row_count(dir.path(), "properties/_untyped.parquet"), 1);
        assert_eq!(row_count(dir.path(), "edge_properties/KNOWS.parquet"), 1);
    }
}
