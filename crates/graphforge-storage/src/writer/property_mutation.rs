//! Authenticated property mutation staging and completion.

use super::Arc;
use super::Array;
use super::ArrayRef;
use super::BTreeMap;
use super::BTreeSet;
use super::BooleanArray;
use super::CompletedPropertyWindow;
use super::DataType;
use super::EDGE_PROPERTY_UUID_FIELD;
use super::EdgePropRow;
use super::Field;
use super::FixedSizeBinaryArray;
use super::GfError;
use super::HashMap;
use super::HashSet;
use super::IrLiteral;
use super::NODE_PROPERTY_UUID_FIELD;
use super::Path;
use super::PathBuf;
use super::ProjectErrorCode;
use super::PropRow;
use super::PropRowLike;
use super::PropertyGenerationAuthority;
use super::RecordBatch;
use super::RewriteBatch;
use super::Schema;
use super::SchemaRef;
use super::build_property_columns;
use super::build_property_columns_keyed;
use super::pq_err;
use super::property_rows_batch_with_schema;

// ---------------------------------------------------------------------------
// SET / REMOVE property rewrite primitives (#791)
// ---------------------------------------------------------------------------
//
// These rewrite committed `properties/<stem>.parquet` / `edge_properties/
// <stem>.parquet` files, mirroring the writer's decode → mutate → re-infer →
// write cycle (see [`GraphWriter::flush_properties`]). The `stage_*` forms
// stage into a caller-owned [`RewriteBatch`] so one statement's rewrites
// across stems commit all-or-nothing (#790); the original four functions are
// stage-and-commit wrappers for single-stem callers.
//
// The execution layer accumulates per-uuid updates/removals from the matched
// rows, then calls these once per file stem. SET **merges** into a uuid's
// existing property map (overwriting same-named keys) and **inserts** a fresh
// row for a uuid that had no property row yet; REMOVE drops keys (a column that
// becomes all-absent disappears on re-inference). Both return the number of
// distinct entities whose file row was written.

/// Apply per-uuid property `updates` (SET) to the rows decoded from `existing`,
/// merging into each uuid's map and inserting a row for any uuid not present.
/// Returns the rebuilt row set and the number of distinct uuids touched.
fn apply_property_updates<R: PropRowLike>(
    mut rows: Vec<R>,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> (Vec<R>, u64) {
    // uuid → index into `rows` (first occurrence wins; the decode never emits
    // duplicate uuids, but a defensive first-wins keeps this total).
    let mut index: HashMap<[u8; 16], usize> = HashMap::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        index.entry(*row.uuid_bytes()).or_insert(i);
    }
    let mut touched = 0u64;
    for (uuid, new_props) in updates {
        if new_props.is_empty() {
            continue;
        }
        touched += 1;
        if let Some(&i) = index.get(uuid) {
            rows[i].props_mut().extend(new_props.clone());
        } else {
            index.insert(*uuid, rows.len());
            rows.push(R::from_parts(*uuid, new_props.clone()));
        }
    }
    (rows, touched)
}

/// Apply per-uuid property `removals` (REMOVE) to the rows decoded from
/// `existing`. Removing an absent key or an absent uuid is a no-op (openCypher).
/// Returns the rebuilt row set and the number of distinct uuids touched (a uuid
/// is counted even if every named key was already absent — the entity was
/// targeted).
fn apply_property_removals<R: PropRowLike>(
    mut rows: Vec<R>,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> (Vec<R>, u64) {
    let mut index: HashMap<[u8; 16], usize> = HashMap::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        index.entry(*row.uuid_bytes()).or_insert(i);
    }
    let mut touched = 0u64;
    for (uuid, keys) in removals {
        if keys.is_empty() {
            continue;
        }
        touched += 1;
        if let Some(&i) = index.get(uuid) {
            let props = rows[i].props_mut();
            for k in keys {
                props.remove(k);
            }
        }
    }
    (rows, touched)
}

/// Stage a rewrite of `properties/<stem>.parquet` applying per-`node_uuid` SET
/// `updates` into `staged` (committed by the caller, #790).
///
/// Reads the current file (decode → re-infer over the merged set), inserts a
/// row for any node that had no property row yet, and stages the rebuilt file.
/// A write is skipped only when the merged set is empty. Returns the number of
/// distinct nodes whose row was set.
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or staging the file.
// The execution layer always accumulates updates with the default hasher;
// generalizing the nested maps over `BuildHasher` would add two type params per
// fn for no caller benefit.
#[allow(clippy::implicit_hasher)]
pub fn stage_set_node_properties(
    staged: &mut RewriteBatch,
    dir: &Path,
    stem: &str,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> Result<u64, GfError> {
    stage_set_node_properties_from_inventory(staged, dir, None, stem, updates)
        .map(|counts| counts.entities_touched)
}

#[allow(clippy::implicit_hasher)]
/// Stage node SET rows against a caller-pinned authenticated generation.
pub fn stage_set_node_properties_authenticated(
    staged: &mut RewriteBatch,
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    stem: &str,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> Result<u64, GfError> {
    stage_set_node_properties_from_inventory(staged, dir, Some(inventory), stem, updates)
        .map(|counts| counts.entities_touched)
}

/// Data-level facts from a staged node property update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodePropertySetCounts {
    /// Distinct node rows touched by the update.
    pub entities_touched: u64,
    /// Non-null existing property values replaced by supplied keys.
    pub properties_replaced: u64,
}

/// Stage authenticated node properties and report existing-value replacements.
/// Uses the same before-map required by the rewrite, without another read pass.
#[allow(clippy::implicit_hasher)] // same execution accumulator contract as the SET wrappers above
pub fn stage_set_node_properties_authenticated_with_counts(
    staged: &mut RewriteBatch,
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    stem: &str,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> Result<NodePropertySetCounts, GfError> {
    stage_set_node_properties_from_inventory(staged, dir, Some(inventory), stem, updates)
}

fn stage_set_node_properties_from_inventory(
    staged: &mut RewriteBatch,
    dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
    stem: &str,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> Result<NodePropertySetCounts, GfError> {
    let targets = updates.keys().copied().collect();
    let owned_inventory;
    let inventory = if let Some(inventory) = inventory {
        inventory
    } else {
        owned_inventory = crate::property_overlay::authenticated_property_inventory_for_route(
            dir,
            crate::PropertyRouteKind::Node,
            stem,
        )?;
        &owned_inventory
    };
    let (mut existing, _) =
        crate::property_overlay::read_authenticated_property_snapshots_for_inventory(
            inventory,
            crate::PropertyRouteKind::Node,
            stem,
            &targets,
        )?;
    existing.extend(pending_property_snapshots(
        staged,
        dir,
        crate::property_overlay::PropertyRouteKind::Node,
        stem,
    )?);
    existing.retain(|uuid, _| targets.contains(uuid));
    let properties_replaced = existing
        .iter()
        .map(|(uuid, row)| {
            updates.get(uuid).map_or(0, |properties| {
                properties
                    .keys()
                    .filter(|name| {
                        row.values
                            .get(*name)
                            .is_some_and(|value| !matches!(value, IrLiteral::Null))
                    })
                    .count() as u64
            })
        })
        .sum();
    let before = existing.clone();
    let rows = existing
        .into_values()
        .map(|row| PropRow {
            node_uuid: row.uuid,
            props: row.values.into_iter().collect(),
        })
        .collect();
    let (rows, touched) = apply_property_updates(rows, updates);
    let rows = rows
        .into_iter()
        .filter(|row| updates.contains_key(&row.node_uuid))
        .collect::<Vec<_>>();
    let route_schema = staged
        .property_window_schema(crate::PropertyRouteKind::Node, stem)
        .or_else(|| inventory.route_schema(crate::PropertyRouteKind::Node, stem));
    stage_node_property_file(
        staged,
        dir,
        stem,
        &rows,
        route_schema,
        inventory.generation_authority(),
        Some(&before),
    )?;
    Ok(NodePropertySetCounts {
        entities_touched: touched,
        properties_replaced,
    })
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "keeps staging lookup contract aligned with authenticated lookup"
)]
fn pending_property_snapshots(
    staged: &RewriteBatch,
    _dir: &Path,
    kind: crate::property_overlay::PropertyRouteKind,
    route: &str,
) -> Result<BTreeMap<[u8; 16], crate::property_overlay::PropertySnapshotRow>, GfError> {
    Ok(staged
        .property_window_rows(kind, route)
        .map(|rows| {
            rows.iter()
                .map(|(uuid, row)| (*uuid, row.clone()))
                .collect()
        })
        .unwrap_or_default())
}

/// Stage a rewrite of `properties/<stem>.parquet` applying per-`node_uuid`
/// REMOVE `removals`. Returns the number of distinct nodes targeted.
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or staging the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn stage_remove_node_properties(
    staged: &mut RewriteBatch,
    dir: &Path,
    stem: &str,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> Result<u64, GfError> {
    stage_remove_node_properties_from_inventory(staged, dir, None, stem, removals)
}

#[allow(clippy::implicit_hasher)]
/// Stage node REMOVE rows against a caller-pinned authenticated generation.
pub fn stage_remove_node_properties_authenticated(
    staged: &mut RewriteBatch,
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    stem: &str,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> Result<u64, GfError> {
    stage_remove_node_properties_from_inventory(staged, dir, Some(inventory), stem, removals)
}

fn stage_remove_node_properties_from_inventory(
    staged: &mut RewriteBatch,
    dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
    stem: &str,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> Result<u64, GfError> {
    let targets = removals.keys().copied().collect();
    let owned_inventory;
    let inventory = if let Some(inventory) = inventory {
        inventory
    } else {
        owned_inventory = crate::property_overlay::authenticated_property_inventory_for_route(
            dir,
            crate::PropertyRouteKind::Node,
            stem,
        )?;
        &owned_inventory
    };
    let (mut existing, _) =
        crate::property_overlay::read_authenticated_property_snapshots_for_inventory(
            inventory,
            crate::PropertyRouteKind::Node,
            stem,
            &targets,
        )?;
    existing.extend(pending_property_snapshots(
        staged,
        dir,
        crate::property_overlay::PropertyRouteKind::Node,
        stem,
    )?);
    existing.retain(|uuid, _| targets.contains(uuid));
    let before = existing.clone();
    // Preserve a pending entity-delete tombstone across a later REMOVE.
    let rows = existing
        .into_values()
        .filter(|row| !row.tombstone)
        .map(|row| PropRow {
            node_uuid: row.uuid,
            props: row.values.into_iter().collect(),
        })
        .collect();
    let (rows, touched) = apply_property_removals(rows, removals);
    let rows = rows
        .into_iter()
        .filter(|row| removals.contains_key(&row.node_uuid))
        .collect::<Vec<_>>();
    let route_schema = staged
        .property_window_schema(crate::PropertyRouteKind::Node, stem)
        .or_else(|| inventory.route_schema(crate::PropertyRouteKind::Node, stem));
    stage_node_property_file(
        staged,
        dir,
        stem,
        &rows,
        route_schema,
        inventory.generation_authority(),
        Some(&before),
    )?;
    Ok(touched)
}

/// Stage a rewrite of `edge_properties/<rel_stem>.parquet` applying
/// per-`edge_uuid` SET `updates`. Edge analogue of
/// [`stage_set_node_properties`]; the join key is `edge_uuid` and the file is
/// routed by relation name.
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or staging the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn stage_set_edge_properties(
    staged: &mut RewriteBatch,
    dir: &Path,
    rel_stem: &str,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> Result<u64, GfError> {
    stage_set_edge_properties_from_inventory(staged, dir, None, rel_stem, updates)
}

#[allow(clippy::implicit_hasher)]
/// Stage edge SET rows against a caller-pinned authenticated generation.
pub fn stage_set_edge_properties_authenticated(
    staged: &mut RewriteBatch,
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    rel_stem: &str,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> Result<u64, GfError> {
    stage_set_edge_properties_from_inventory(staged, dir, Some(inventory), rel_stem, updates)
}

fn stage_set_edge_properties_from_inventory(
    staged: &mut RewriteBatch,
    dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
    rel_stem: &str,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> Result<u64, GfError> {
    let targets = updates.keys().copied().collect();
    let owned_inventory;
    let inventory = if let Some(inventory) = inventory {
        inventory
    } else {
        owned_inventory = crate::property_overlay::authenticated_property_inventory_for_route(
            dir,
            crate::PropertyRouteKind::Edge,
            rel_stem,
        )?;
        &owned_inventory
    };
    let (mut existing, _) =
        crate::property_overlay::read_authenticated_property_snapshots_for_inventory(
            inventory,
            crate::PropertyRouteKind::Edge,
            rel_stem,
            &targets,
        )?;
    existing.extend(pending_property_snapshots(
        staged,
        dir,
        crate::property_overlay::PropertyRouteKind::Edge,
        rel_stem,
    )?);
    existing.retain(|uuid, _| targets.contains(uuid));
    let before = existing.clone();
    let rows = existing
        .into_values()
        .map(|row| EdgePropRow {
            edge_uuid: row.uuid,
            props: row.values.into_iter().collect(),
        })
        .collect();
    let (rows, touched) = apply_property_updates(rows, updates);
    let rows = rows
        .into_iter()
        .filter(|row| updates.contains_key(&row.edge_uuid))
        .collect::<Vec<_>>();
    let route_schema = staged
        .property_window_schema(crate::PropertyRouteKind::Edge, rel_stem)
        .or_else(|| inventory.route_schema(crate::PropertyRouteKind::Edge, rel_stem));
    stage_edge_property_file(
        staged,
        dir,
        rel_stem,
        &rows,
        route_schema,
        inventory.generation_authority(),
        Some(&before),
    )?;
    Ok(touched)
}

/// Stage a rewrite of `edge_properties/<rel_stem>.parquet` applying
/// per-`edge_uuid` REMOVE `removals`. Edge analogue of
/// [`stage_remove_node_properties`].
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or staging the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn stage_remove_edge_properties(
    staged: &mut RewriteBatch,
    dir: &Path,
    rel_stem: &str,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> Result<u64, GfError> {
    stage_remove_edge_properties_from_inventory(staged, dir, None, rel_stem, removals)
}

#[allow(clippy::implicit_hasher)]
/// Stage edge REMOVE rows against a caller-pinned authenticated generation.
pub fn stage_remove_edge_properties_authenticated(
    staged: &mut RewriteBatch,
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    rel_stem: &str,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> Result<u64, GfError> {
    stage_remove_edge_properties_from_inventory(staged, dir, Some(inventory), rel_stem, removals)
}

fn stage_remove_edge_properties_from_inventory(
    staged: &mut RewriteBatch,
    dir: &Path,
    inventory: Option<&crate::AuthenticatedPropertyInventory>,
    rel_stem: &str,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> Result<u64, GfError> {
    let targets = removals.keys().copied().collect();
    let owned_inventory;
    let inventory = if let Some(inventory) = inventory {
        inventory
    } else {
        owned_inventory = crate::property_overlay::authenticated_property_inventory_for_route(
            dir,
            crate::PropertyRouteKind::Edge,
            rel_stem,
        )?;
        &owned_inventory
    };
    let (mut existing, _) =
        crate::property_overlay::read_authenticated_property_snapshots_for_inventory(
            inventory,
            crate::PropertyRouteKind::Edge,
            rel_stem,
            &targets,
        )?;
    existing.extend(pending_property_snapshots(
        staged,
        dir,
        crate::property_overlay::PropertyRouteKind::Edge,
        rel_stem,
    )?);
    existing.retain(|uuid, _| targets.contains(uuid));
    let before = existing.clone();
    // REMOVE cannot resurrect a row deleted earlier in this transaction.  The
    // already-staged tombstone remains authoritative; only an explicit SET may
    // turn that UUID live again.
    let rows = existing
        .into_values()
        .filter(|row| !row.tombstone)
        .map(|row| EdgePropRow {
            edge_uuid: row.uuid,
            props: row.values.into_iter().collect(),
        })
        .collect();
    let (rows, touched) = apply_property_removals(rows, removals);
    let rows = rows
        .into_iter()
        .filter(|row| removals.contains_key(&row.edge_uuid))
        .collect::<Vec<_>>();
    let route_schema = staged
        .property_window_schema(crate::PropertyRouteKind::Edge, rel_stem)
        .or_else(|| inventory.route_schema(crate::PropertyRouteKind::Edge, rel_stem));
    stage_edge_property_file(
        staged,
        dir,
        rel_stem,
        &rows,
        route_schema,
        inventory.generation_authority(),
        Some(&before),
    )?;
    Ok(touched)
}

/// Rewrite `properties/<stem>.parquet` applying per-`node_uuid` SET `updates`,
/// staged and committed as one batch (#790).
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or rewriting the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn set_node_properties(
    dir: &Path,
    stem: &str,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> Result<u64, GfError> {
    let mut staged = RewriteBatch::new();
    let touched = stage_set_node_properties(&mut staged, dir, stem, updates)?;
    crate::generation::commit_topology_aware(staged, dir)?;
    Ok(touched)
}

/// Rewrite `properties/<stem>.parquet` applying per-`node_uuid` REMOVE
/// `removals`, staged and committed as one batch (#790).
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or rewriting the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn remove_node_properties(
    dir: &Path,
    stem: &str,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> Result<u64, GfError> {
    let mut staged = RewriteBatch::new();
    let touched = stage_remove_node_properties(&mut staged, dir, stem, removals)?;
    crate::generation::commit_topology_aware(staged, dir)?;
    Ok(touched)
}

/// Rewrite `edge_properties/<rel_stem>.parquet` applying per-`edge_uuid` SET
/// `updates`, staged and committed as one batch (#790).
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or rewriting the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn set_edge_properties_rewrite(
    dir: &Path,
    rel_stem: &str,
    updates: &HashMap<[u8; 16], HashMap<String, IrLiteral>>,
) -> Result<u64, GfError> {
    let mut staged = RewriteBatch::new();
    let touched = stage_set_edge_properties(&mut staged, dir, rel_stem, updates)?;
    crate::generation::commit_topology_aware(staged, dir)?;
    Ok(touched)
}

/// Rewrite `edge_properties/<rel_stem>.parquet` applying per-`edge_uuid`
/// REMOVE `removals`, staged and committed as one batch (#790).
///
/// # Errors
/// Propagates Parquet / Arrow / IO errors from reading or rewriting the file.
#[allow(clippy::implicit_hasher)] // see `stage_set_node_properties`
pub fn remove_edge_properties(
    dir: &Path,
    rel_stem: &str,
    removals: &HashMap<[u8; 16], HashSet<String>>,
) -> Result<u64, GfError> {
    let mut staged = RewriteBatch::new();
    let touched = stage_remove_edge_properties(&mut staged, dir, rel_stem, removals)?;
    crate::generation::commit_topology_aware(staged, dir)?;
    Ok(touched)
}

/// Move one bounded set of exploratory snapshots into its promoted owner.
pub(crate) fn stage_promoted_properties(
    staged: &mut RewriteBatch,
    dir: &Path,
    stem: &str,
    kind: crate::PropertyRouteKind,
    source_schema: &Schema,
    source: &BTreeMap<[u8; 16], crate::PropertySnapshotRow>,
) -> Result<(), GfError> {
    let inventory =
        crate::property_overlay::authenticated_property_inventory_for_route(dir, kind, stem)?;
    let targets = source.keys().copied().collect();
    let (before, _) = crate::property_overlay::read_authenticated_property_snapshots_for_inventory(
        &inventory, kind, stem, &targets,
    )?;
    if kind == crate::PropertyRouteKind::Edge {
        let (present, _) =
            crate::property_overlay::read_authenticated_property_presence_for_inventory(
                &inventory, kind, stem, &targets,
            )?;
        if !present.is_empty() {
            return Err(GfError::Validation(
                "ontology promotion has overlapping edge property owners".into(),
            ));
        }
    }
    let destination = inventory.route_schema(kind, stem);
    let mut source_metadata = source_schema.metadata().clone();
    source_metadata.insert(
        match kind {
            crate::PropertyRouteKind::Node => "graphforge.entity_type",
            crate::PropertyRouteKind::Edge => "graphforge.rel_type",
        }
        .into(),
        stem.into(),
    );
    let source_schema = source_schema.clone().with_metadata(source_metadata);
    let mut schemas = vec![&source_schema];
    if let Some(schema) = &destination {
        schemas.push(schema.as_ref());
    }
    let authority = crate::property_overlay::merge_property_route_schemas(kind, stem, schemas)?;
    let mut metadata = authority.metadata().clone();
    if let Some(summary) = destination.as_ref().and_then(|schema| {
        schema
            .metadata()
            .get(crate::property_overlay::PROPERTY_LIVE_SCHEMA_KEY)
    }) {
        metadata.insert(
            crate::property_overlay::PROPERTY_LIVE_SCHEMA_KEY.into(),
            summary.clone(),
        );
    }
    let authority = Arc::new(authority.as_ref().clone().with_metadata(metadata));
    let rows = source
        .iter()
        .filter(|(_, row)| !row.tombstone)
        .map(|(uuid, row)| {
            let mut values = before
                .get(uuid)
                .map_or_else(BTreeMap::new, |row| row.values.clone());
            values.extend(row.values.clone());
            PropRow {
                node_uuid: *uuid,
                props: values.into_iter().collect(),
            }
        })
        .collect::<Vec<_>>();
    match kind {
        crate::PropertyRouteKind::Node => stage_node_property_file(
            staged,
            dir,
            stem,
            &rows,
            Some(authority),
            inventory.generation_authority(),
            Some(&before),
        ),
        crate::PropertyRouteKind::Edge => {
            let rows = rows
                .into_iter()
                .map(|row| EdgePropRow {
                    edge_uuid: row.node_uuid,
                    props: row.props,
                })
                .collect::<Vec<_>>();
            stage_edge_property_file(
                staged,
                dir,
                stem,
                &rows,
                Some(authority),
                inventory.generation_authority(),
                Some(&before),
            )?;
            let tombstones = source
                .iter()
                .filter_map(|(uuid, row)| row.tombstone.then_some(*uuid))
                .collect::<HashSet<_>>();
            stage_property_tombstones_authenticated(
                staged,
                dir,
                &inventory,
                kind,
                stem,
                &tombstones,
            )
        }
    }
}

fn stage_node_property_file(
    staged: &mut RewriteBatch,
    dir: &Path,
    stem: &str,
    rows: &[PropRow],
    authority: Option<SchemaRef>,
    authority_generation_uuid: Option<(uuid::Uuid, PathBuf)>,
    before: Option<&BTreeMap<[u8; 16], crate::PropertySnapshotRow>>,
) -> Result<(), GfError> {
    if rows.is_empty() {
        return Ok(());
    }
    let (schema, _) = build_property_columns(stem, rows)?;
    let inferred = Arc::new(schema);
    let schema = if let Some(before) = before {
        let after = rows
            .iter()
            .map(|row| crate::PropertySnapshotRow {
                uuid: row.node_uuid,
                tombstone: false,
                values: row.props.clone().into_iter().collect(),
            })
            .collect::<Vec<_>>();
        crate::property_overlay::update_live_route_schema(
            crate::PropertyRouteKind::Node,
            stem,
            authority.as_ref(),
            inferred,
            before,
            &after,
        )?
    } else {
        merge_property_write_schema(crate::PropertyRouteKind::Node, stem, inferred, authority)?
    };
    let cols = property_rows_batch_with_schema(schema.as_ref(), NODE_PROPERTY_UUID_FIELD, rows)?
        .columns()
        .to_vec();
    stage_property_fragment(
        staged,
        dir,
        PropertyFragmentInput {
            kind: crate::property_overlay::PropertyRouteKind::Node,
            route: stem,
            schema: &schema,
            input_schema: schema.as_ref(),
            columns: cols,
            tombstone: false,
            authority: authority_generation_uuid,
        },
    )
}

fn stage_edge_property_file(
    staged: &mut RewriteBatch,
    dir: &Path,
    stem: &str,
    rows: &[EdgePropRow],
    authority: Option<SchemaRef>,
    authority_generation_uuid: Option<(uuid::Uuid, PathBuf)>,
    before: Option<&BTreeMap<[u8; 16], crate::PropertySnapshotRow>>,
) -> Result<(), GfError> {
    if rows.is_empty() {
        return Ok(());
    }
    let (schema, _) =
        build_property_columns_keyed(EDGE_PROPERTY_UUID_FIELD, "graphforge.rel_type", stem, rows)?;
    let inferred = Arc::new(schema);
    let schema = if let Some(before) = before {
        let after = rows
            .iter()
            .map(|row| crate::PropertySnapshotRow {
                uuid: row.edge_uuid,
                tombstone: false,
                values: row.props.clone().into_iter().collect(),
            })
            .collect::<Vec<_>>();
        crate::property_overlay::update_live_route_schema(
            crate::PropertyRouteKind::Edge,
            stem,
            authority.as_ref(),
            inferred,
            before,
            &after,
        )?
    } else {
        merge_property_write_schema(crate::PropertyRouteKind::Edge, stem, inferred, authority)?
    };
    let cols = property_rows_batch_with_schema(schema.as_ref(), EDGE_PROPERTY_UUID_FIELD, rows)?
        .columns()
        .to_vec();
    stage_property_fragment(
        staged,
        dir,
        PropertyFragmentInput {
            kind: crate::property_overlay::PropertyRouteKind::Edge,
            route: stem,
            schema: &schema,
            input_schema: schema.as_ref(),
            columns: cols,
            tombstone: false,
            authority: authority_generation_uuid,
        },
    )
}

/// Stage a rebuilt property file under `<dir>/<subdir>/<stem>.parquet` (the
/// staging core creates the subdirectory), replacing any content this
/// statement already staged for it. Callers guard the empty-row case before
/// reaching here.
fn merge_node_property_window(rows: Vec<PropRow>) -> Vec<PropRow> {
    let mut merged = BTreeMap::<[u8; 16], HashMap<String, IrLiteral>>::new();
    for row in rows {
        merged.entry(row.node_uuid).or_default().extend(row.props);
    }
    merged
        .into_iter()
        .map(|(node_uuid, props)| PropRow { node_uuid, props })
        .collect()
}

fn merge_edge_property_window(rows: Vec<EdgePropRow>) -> Vec<EdgePropRow> {
    let mut merged = BTreeMap::<[u8; 16], HashMap<String, IrLiteral>>::new();
    for row in rows {
        merged.entry(row.edge_uuid).or_default().extend(row.props);
    }
    merged
        .into_iter()
        .map(|(edge_uuid, props)| EdgePropRow { edge_uuid, props })
        .collect()
}

pub(super) fn merge_property_write_schema(
    kind: crate::PropertyRouteKind,
    route: &str,
    inferred: SchemaRef,
    authority: Option<SchemaRef>,
) -> Result<SchemaRef, GfError> {
    let live_summary = authority.as_ref().and_then(|schema| {
        schema
            .metadata()
            .get(crate::property_overlay::PROPERTY_LIVE_SCHEMA_KEY)
            .cloned()
    });
    let mut schemas = authority.into_iter().collect::<Vec<_>>();
    schemas.push(inferred);
    let mut merged = crate::property_overlay::merge_property_route_schemas(
        kind,
        route,
        schemas.iter().map(AsRef::as_ref),
    )?;
    if let Some(summary) = live_summary {
        let mut metadata = merged.metadata().clone();
        metadata.insert(
            crate::property_overlay::PROPERTY_LIVE_SCHEMA_KEY.to_owned(),
            summary,
        );
        merged = Arc::new(merged.as_ref().clone().with_metadata(metadata));
    }
    Ok(merged)
}

pub(super) fn complete_node_property_window(
    staged: &RewriteBatch,
    dir: &Path,
    route: &str,
    rows: Vec<PropRow>,
) -> Result<CompletedPropertyWindow<PropRow>, GfError> {
    let rows = merge_node_property_window(rows);
    let targets = rows.iter().map(|row| row.node_uuid).collect();
    let inventory = crate::property_overlay::authenticated_property_inventory_for_rewrite_route(
        dir,
        crate::PropertyRouteKind::Node,
        route,
        staged,
    )?;
    let (mut complete, _) = crate::read_authenticated_property_snapshots_for_inventory(
        &inventory,
        crate::PropertyRouteKind::Node,
        route,
        &targets,
    )?;
    complete.extend(pending_property_snapshots(
        staged,
        dir,
        crate::PropertyRouteKind::Node,
        route,
    )?);
    complete.retain(|uuid, _| targets.contains(uuid));
    let before = complete.clone();
    for row in rows {
        let complete =
            complete
                .entry(row.node_uuid)
                .or_insert_with(|| crate::PropertySnapshotRow {
                    uuid: row.node_uuid,
                    tombstone: false,
                    values: BTreeMap::new(),
                });
        complete.tombstone = false;
        complete.values.extend(row.props);
    }
    let rows = targets
        .into_iter()
        .filter_map(|uuid| complete.remove(&uuid))
        .map(|row| PropRow {
            node_uuid: row.uuid,
            props: row.values.into_iter().collect(),
        })
        .collect::<Vec<_>>();
    let (inferred, _) = build_property_columns(route, &rows)?;
    let after = rows
        .iter()
        .map(|row| crate::PropertySnapshotRow {
            uuid: row.node_uuid,
            tombstone: false,
            values: row.props.clone().into_iter().collect(),
        })
        .collect::<Vec<_>>();
    let authority = staged
        .property_window_schema(crate::PropertyRouteKind::Node, route)
        .or_else(|| inventory.route_schema(crate::PropertyRouteKind::Node, route));
    let schema = crate::property_overlay::update_live_route_schema(
        crate::PropertyRouteKind::Node,
        route,
        authority.as_ref(),
        Arc::new(inferred),
        &before,
        &after,
    )?;
    Ok((rows, Some(schema), inventory.generation_authority()))
}

pub(super) fn complete_edge_property_window(
    staged: &RewriteBatch,
    dir: &Path,
    route: &str,
    rows: Vec<EdgePropRow>,
) -> Result<CompletedPropertyWindow<EdgePropRow>, GfError> {
    let rows = merge_edge_property_window(rows);
    let targets = rows.iter().map(|row| row.edge_uuid).collect();
    let inventory = crate::property_overlay::authenticated_property_inventory_for_rewrite_route(
        dir,
        crate::PropertyRouteKind::Edge,
        route,
        staged,
    )?;
    let (mut complete, _) = crate::read_authenticated_property_snapshots_for_inventory(
        &inventory,
        crate::PropertyRouteKind::Edge,
        route,
        &targets,
    )?;
    complete.extend(pending_property_snapshots(
        staged,
        dir,
        crate::PropertyRouteKind::Edge,
        route,
    )?);
    complete.retain(|uuid, _| targets.contains(uuid));
    let before = complete.clone();
    for row in rows {
        let complete =
            complete
                .entry(row.edge_uuid)
                .or_insert_with(|| crate::PropertySnapshotRow {
                    uuid: row.edge_uuid,
                    tombstone: false,
                    values: BTreeMap::new(),
                });
        complete.tombstone = false;
        complete.values.extend(row.props);
    }
    let rows = targets
        .into_iter()
        .filter_map(|uuid| complete.remove(&uuid))
        .map(|row| EdgePropRow {
            edge_uuid: row.uuid,
            props: row.values.into_iter().collect(),
        })
        .collect::<Vec<_>>();
    let (inferred, _) = build_property_columns_keyed(
        EDGE_PROPERTY_UUID_FIELD,
        "graphforge.rel_type",
        route,
        &rows,
    )?;
    let after = rows
        .iter()
        .map(|row| crate::PropertySnapshotRow {
            uuid: row.edge_uuid,
            tombstone: false,
            values: row.props.clone().into_iter().collect(),
        })
        .collect::<Vec<_>>();
    let authority = staged
        .property_window_schema(crate::PropertyRouteKind::Edge, route)
        .or_else(|| inventory.route_schema(crate::PropertyRouteKind::Edge, route));
    let schema = crate::property_overlay::update_live_route_schema(
        crate::PropertyRouteKind::Edge,
        route,
        authority.as_ref(),
        Arc::new(inferred),
        &before,
        &after,
    )?;
    Ok((rows, Some(schema), inventory.generation_authority()))
}

/// Stage whole-row tombstones using an already authenticated generation inventory.
///
/// This is the publication-safe analogue of the workspace convenience path:
/// the caller pins one project generation for every property operation in the
/// batch, so sealing can reject a stale window if CURRENT changes.
pub fn stage_property_tombstones_authenticated<S: std::hash::BuildHasher>(
    staged: &mut RewriteBatch,
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    kind: crate::property_overlay::PropertyRouteKind,
    route: &str,
    uuids: &HashSet<[u8; 16], S>,
) -> Result<(), GfError> {
    if uuids.is_empty() {
        return Ok(());
    }
    stage_property_tombstones_from_inventory(staged, dir, inventory, kind, route, uuids)
}

fn stage_property_tombstones_from_inventory<S: std::hash::BuildHasher>(
    staged: &mut RewriteBatch,
    dir: &Path,
    inventory: &crate::AuthenticatedPropertyInventory,
    kind: crate::property_overlay::PropertyRouteKind,
    route: &str,
    uuids: &HashSet<[u8; 16], S>,
) -> Result<(), GfError> {
    let targets = uuids.iter().copied().collect::<BTreeSet<_>>();
    let (mut before, _) =
        crate::property_overlay::read_authenticated_property_snapshots_for_inventory(
            inventory, kind, route, &targets,
        )?;
    before.extend(pending_property_snapshots(staged, dir, kind, route)?);
    before.retain(|uuid, _| targets.contains(uuid));
    let after = targets
        .iter()
        .copied()
        .map(|uuid| crate::PropertySnapshotRow {
            uuid,
            tombstone: true,
            values: BTreeMap::new(),
        })
        .collect::<Vec<_>>();

    let mut uuids = targets.into_iter().collect::<Vec<_>>();
    uuids.sort_unstable();
    let uuid_field = match kind {
        crate::property_overlay::PropertyRouteKind::Node => NODE_PROPERTY_UUID_FIELD,
        crate::property_overlay::PropertyRouteKind::Edge => EDGE_PROPERTY_UUID_FIELD,
    };
    let route_key = match kind {
        crate::property_overlay::PropertyRouteKind::Node => "graphforge.entity_type",
        crate::property_overlay::PropertyRouteKind::Edge => "graphforge.rel_type",
    };
    let inferred = Arc::new(Schema::new_with_metadata(
        vec![Field::new(uuid_field, DataType::FixedSizeBinary(16), false)],
        HashMap::from([(route_key.to_owned(), route.to_owned())]),
    ));
    let authority = staged
        .property_window_schema(kind, route)
        .or_else(|| inventory.route_schema(kind, route));
    let schema = crate::property_overlay::update_live_route_schema(
        kind,
        route,
        authority.as_ref(),
        inferred,
        &before,
        &after,
    )?;
    let column = FixedSizeBinaryArray::try_from_iter(uuids.into_iter().map(|uuid| uuid.to_vec()))
        .map_err(pq_err)?;
    let mut columns = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(column) as ArrayRef);
    let rows = columns[0].len();
    columns.extend(
        schema.fields()[1..]
            .iter()
            .map(|field| arrow::array::new_null_array(field.data_type(), rows)),
    );
    stage_property_fragment(
        staged,
        dir,
        PropertyFragmentInput {
            kind,
            route,
            schema: &schema,
            input_schema: schema.as_ref(),
            columns,
            tombstone: true,
            authority: inventory.generation_authority(),
        },
    )
}

pub(super) struct PropertyFragmentInput<'a> {
    pub(super) kind: crate::property_overlay::PropertyRouteKind,
    pub(super) route: &'a str,
    pub(super) schema: &'a SchemaRef,
    pub(super) input_schema: &'a Schema,
    pub(super) columns: Vec<ArrayRef>,
    pub(super) tombstone: bool,
    pub(super) authority: PropertyGenerationAuthority,
}

pub(super) fn stage_property_fragment(
    staged: &mut RewriteBatch,
    dir: &Path,
    input: PropertyFragmentInput<'_>,
) -> Result<(), GfError> {
    use crate::property_overlay::PROPERTY_TOMBSTONE_FIELD;
    let PropertyFragmentInput {
        kind,
        route,
        schema,
        input_schema,
        columns,
        tombstone,
        authority,
    } = input;
    let cols = columns;
    if input_schema.fields().len() != cols.len() {
        return Err(pq_err("property fragment schema/column count mismatch"));
    }
    let mut by_name = HashMap::with_capacity(cols.len());
    for (field, column) in input_schema.fields().iter().zip(cols) {
        if column.data_type() != field.data_type() {
            return Err(pq_err(
                "property fragment column type conflicts with its field",
            ));
        }
        if by_name.insert(field.name().as_str(), column).is_some() {
            return Err(pq_err("property fragment contains duplicate fields"));
        }
    }
    let mut cols = schema
        .fields()
        .iter()
        .map(|field| {
            by_name.remove(field.name().as_str()).ok_or_else(|| {
                pq_err(format!(
                    "property fragment is missing field {}",
                    field.name()
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !by_name.is_empty() {
        return Err(pq_err(
            "property fragment contains fields outside authority schema",
        ));
    }
    let mut fields = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.insert(
        1,
        Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
    );
    let rows = cols.first().map_or(0, arrow::array::Array::len);
    cols.insert(1, Arc::new(BooleanArray::from(vec![tombstone; rows])));
    let logical_schema = Arc::clone(schema);
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        logical_schema.metadata().clone(),
    ));
    let batch = RecordBatch::try_new(schema, cols).map_err(pq_err)?;
    let uuid_field = match kind {
        crate::property_overlay::PropertyRouteKind::Node => NODE_PROPERTY_UUID_FIELD,
        crate::property_overlay::PropertyRouteKind::Edge => EDGE_PROPERTY_UUID_FIELD,
    };
    let rows = crate::property_overlay::decode_snapshot_batch(&batch, uuid_field)?;
    staged.accumulate_property_window(dir, kind, route, rows, &logical_schema, authority.as_ref())
}

fn property_snapshot_fragment_schema(
    base: &Schema,
    kind: crate::PropertyRouteKind,
    route: &str,
    generation: u64,
) -> SchemaRef {
    use crate::property_overlay::{
        PROPERTY_GENERATION_KEY, PROPERTY_KIND_KEY, PROPERTY_ORDINAL_KEY, PROPERTY_OVERLAY_FORMAT,
        PROPERTY_OVERLAY_FORMAT_KEY, PROPERTY_ROUTE_KEY, PROPERTY_TOMBSTONE_FIELD,
    };
    let mut fields = base
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.insert(
        1,
        Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
    );
    let mut metadata = base.metadata().clone();
    metadata.insert(
        PROPERTY_OVERLAY_FORMAT_KEY.into(),
        PROPERTY_OVERLAY_FORMAT.into(),
    );
    metadata.insert(PROPERTY_ROUTE_KEY.into(), route.to_owned());
    metadata.insert(PROPERTY_KIND_KEY.into(), kind.metadata_value().into());
    metadata.insert(PROPERTY_GENERATION_KEY.into(), generation.to_string());
    metadata.insert(PROPERTY_ORDINAL_KEY.into(), "0".into());
    Arc::new(Schema::new_with_metadata(fields, metadata))
}

pub(crate) fn edge_property_snapshots_batch(
    schema: &Schema,
    snapshots: &BTreeMap<[u8; 16], crate::PropertySnapshotRow>,
) -> Result<RecordBatch, GfError> {
    if snapshots.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
    }
    let rows = snapshots
        .values()
        .map(|row| EdgePropRow {
            edge_uuid: row.uuid,
            props: row
                .values
                .iter()
                .filter(|(name, _)| schema.column_with_name(name).is_some())
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        })
        .collect::<Vec<_>>();
    let indices = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| field.data_type() != &DataType::Null)
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    let concrete = schema.project(&indices).map_err(pq_err)?;
    let batch = property_rows_batch_with_schema(&concrete, EDGE_PROPERTY_UUID_FIELD, &rows)?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            if field.data_type() == &DataType::Null {
                arrow::array::new_null_array(&DataType::Null, rows.len())
            } else {
                Arc::clone(
                    batch
                        .column_by_name(field.name())
                        .expect("projected property field"),
                )
            }
        })
        .collect();
    RecordBatch::try_new(Arc::new(schema.clone()), columns).map_err(pq_err)
}

fn property_snapshot_chunk_with_schema(
    base: &Schema,
    fragment_schema: SchemaRef,
    kind: crate::PropertyRouteKind,
    rows: &[crate::PropertySnapshotRow],
) -> Result<RecordBatch, GfError> {
    let tombstones = rows.iter().map(|row| row.tombstone).collect::<Vec<_>>();
    let mut batch = match kind {
        crate::PropertyRouteKind::Node => {
            let rows = rows
                .iter()
                .map(|row| PropRow {
                    node_uuid: row.uuid,
                    props: row.values.clone().into_iter().collect(),
                })
                .collect::<Vec<_>>();
            property_rows_batch_with_schema(base, NODE_PROPERTY_UUID_FIELD, &rows)?
        }
        crate::PropertyRouteKind::Edge => {
            let rows = rows
                .iter()
                .map(|row| EdgePropRow {
                    edge_uuid: row.uuid,
                    props: row.values.clone().into_iter().collect(),
                })
                .collect::<Vec<_>>();
            property_rows_batch_with_schema(base, EDGE_PROPERTY_UUID_FIELD, &rows)?
        }
    };
    let mut columns = batch.columns().to_vec();
    columns.insert(1, Arc::new(BooleanArray::from(tombstones)));
    batch = RecordBatch::try_new(fragment_schema, columns).map_err(pq_err)?;
    Ok(batch)
}

pub(crate) fn seal_property_windows(
    staged: &mut RewriteBatch,
    dir: &Path,
    generation: u64,
) -> Result<(), GfError> {
    use crate::property_overlay::{PropertyFragmentId, enumerate_property_fragments};
    let windows = staged.take_property_windows();
    for (key, window) in windows {
        if window.project_root != dir {
            return Err(GfError::Storage(
                "property window project root changed".into(),
            ));
        }
        if let Some((expected, container_root)) = &window.authority_generation_uuid {
            let current = crate::resolve_project_generation(container_root)?;
            if current.generation_uuid() != *expected {
                return Err(GfError::Project {
                    code: ProjectErrorCode::WriteConflict,
                    message: format!(
                        "property mutation authority changed: expected={} current={}",
                        expected,
                        current.generation_uuid()
                    ),
                });
            }
        }
        let component = staged.route_component(dir, &key.route)?;
        if enumerate_property_fragments(dir, key.kind, &component)?
            .iter()
            .any(|fragment| fragment.id.generation >= generation)
        {
            return Err(GfError::Storage(
                "property fragment generation is not strictly monotonic".into(),
            ));
        }
        let rows = window.rows.into_values().collect::<Vec<_>>();
        if rows.is_empty() {
            return Err(GfError::Storage(
                "property window cannot seal empty fragment".into(),
            ));
        }
        let base_schema = window.schema;
        let fragment_schema = property_snapshot_fragment_schema(
            base_schema.as_ref(),
            key.kind,
            &key.route,
            generation,
        );
        let subdir = match key.kind {
            crate::property_overlay::PropertyRouteKind::Node => "properties",
            crate::property_overlay::PropertyRouteKind::Edge => "edge_properties",
        };
        let destination = dir.join(subdir).join(component).join(
            PropertyFragmentId {
                generation,
                ordinal: 0,
            }
            .file_name(),
        );
        staged.stage_batches(
            &destination,
            Arc::clone(&fragment_schema),
            rows.chunks(4096).map(|chunk| {
                property_snapshot_chunk_with_schema(
                    base_schema.as_ref(),
                    Arc::clone(&fragment_schema),
                    key.kind,
                    chunk,
                )
            }),
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
