//! Runtime entity label identity reconciliation (#702).
//!
//! Bound/persisted node label TypeIds for runtime-catalog entities are tagged
//! with [`1_073_741_824`]. Legacy projects may still
//! store untagged catalog IDs that collide with ontology entity type IDs.
//!
//! When an ontology is present, tagged runtime labels whose catalog **name**
//! matches an ontology entity type are remapped to that ontology [`TypeId`] so
//! progressive adoption keeps one logical population visible under the adopted
//! label. True unknowns stay tagged.
//!
//! Projects that write or successfully migrate under the tagged encoding record
//! [`RUNTIME_ENTITY_LABEL_ENCODING_VERSION`] in
//! `topology/runtime_entity_label_encoding.json`. Unmarked projects that still
//! contain untagged IDs claimed by both ontology and runtime domains fail
//! closed rather than silently cross-classifying labels.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ListArray, UInt32Array, UInt32Builder};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use graphforge_core::{GfError, TypeId};
use graphforge_ir::RuntimeCatalog;
use graphforge_ontology::OntologyHandle;
use graphforge_value::{EntityTypeId, PrimaryEntityTypeId, RuntimeEntityId};
use serde::{Deserialize, Serialize};

use crate::catalog::normalize_topology_nodes;
#[cfg(test)]
use crate::catalog::read_nodes;
use crate::schemas::TOPOLOGY_NODES_SCHEMA;
use crate::staging::RewriteBatch;

/// On-disk encoding contract for runtime entity labels in node topology.
pub const RUNTIME_ENTITY_LABEL_ENCODING_VERSION: u32 = 1;

const ENCODING_FILE: &str = "runtime_entity_label_encoding.json";

/// Outcome of reconciling persisted node label IDs with the runtime-entity
/// plan encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuntimeEntityLabelReconcile {
    /// Node label membership values rewritten (untagged→tagged or tagged→ontology).
    pub remapped_label_values: u64,
    /// Distinct colliding raw IDs present in the runtime catalog ∩ ontology.
    pub colliding_raw_ids: usize,
    /// Whether the tagged-encoding marker was present or written.
    pub encoding_marked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct EncodingMarker {
    format: String,
    version: u32,
}

fn storage_err(message: impl Into<String>) -> GfError {
    GfError::Storage(message.into())
}

fn pq_err(error: impl std::fmt::Display) -> GfError {
    GfError::Storage(error.to_string())
}

fn encoding_path(dir: &Path) -> std::path::PathBuf {
    dir.join("topology").join(ENCODING_FILE)
}

/// Returns true when the project has recorded the tagged runtime-entity encoding.
#[must_use]
pub fn has_runtime_entity_label_encoding_marker(dir: &Path) -> bool {
    read_encoding_version(dir)
        .is_some_and(|version| version >= RUNTIME_ENTITY_LABEL_ENCODING_VERSION)
}

fn read_encoding_version(dir: &Path) -> Option<u32> {
    let bytes = std::fs::read(encoding_path(dir)).ok()?;
    let marker: EncodingMarker = serde_json::from_slice(&bytes).ok()?;
    if marker.format != "graphforge-runtime-entity-label-encoding" {
        return None;
    }
    Some(marker.version)
}

/// Persist the tagged runtime-entity label encoding marker.
///
/// # Errors
/// Returns [`GfError::Storage`] on I/O failure.
pub fn write_runtime_entity_label_encoding_marker(dir: &Path) -> Result<(), GfError> {
    let topology = dir.join("topology");
    std::fs::create_dir_all(&topology).map_err(|e| storage_err(e.to_string()))?;
    let bytes = runtime_entity_label_encoding_bytes()?;
    let path = encoding_path(dir);
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, bytes).map_err(|e| storage_err(e.to_string()))?;
    std::fs::rename(&tmp, &path).map_err(|e| storage_err(e.to_string()))?;
    Ok(())
}

/// Persist observed runtime names by replacing the private workspace alias.
/// A catalog hydrated from CAS must never be truncated in place.
///
/// # Errors
/// Propagates Parquet encoding, authenticated replacement, and durability errors.
pub fn persist_runtime_catalog(dir: &Path, catalog: &RuntimeCatalog) -> Result<(), GfError> {
    let topology = dir.join("topology");
    std::fs::create_dir_all(&topology).map_err(|error| storage_err(error.to_string()))?;
    let batch = catalog.to_record_batch();
    let temporary = tempfile::NamedTempFile::new_in(&topology)
        .map_err(|error| storage_err(error.to_string()))?;
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        temporary.as_file(),
        batch.schema(),
        Some(crate::permanent_parquet::writer_properties().build()),
    )
    .map_err(|error| storage_err(error.to_string()))?;
    writer
        .write(&batch)
        .map_err(|error| storage_err(error.to_string()))?;
    writer
        .close()
        .map_err(|error| storage_err(error.to_string()))?;
    let mut staged = crate::RewriteBatch::new();
    staged.stage_file(&topology.join("runtime_catalog.parquet"), temporary.path())?;
    staged.stage_bytes(&encoding_path(dir), &runtime_entity_label_encoding_bytes()?)?;
    crate::generation::commit_topology_aware(staged, dir)?;
    Ok(())
}

/// Shared marker bytes for reconciliation and authenticated construction artifacts.
pub(crate) fn runtime_entity_label_encoding_bytes() -> Result<Vec<u8>, GfError> {
    let marker = EncodingMarker {
        format: "graphforge-runtime-entity-label-encoding".into(),
        version: RUNTIME_ENTITY_LABEL_ENCODING_VERSION,
    };
    serde_json::to_vec_pretty(&marker).map_err(|e| storage_err(e.to_string()))
}

fn ontology_entity_ids(ontology: Option<&OntologyHandle>) -> HashSet<u32> {
    let mut ids = HashSet::new();
    let Some(handle) = ontology else {
        return ids;
    };
    for name in handle.entity_type_names() {
        if let Some(TypeId(id)) = handle.entity_type_id(name) {
            ids.insert(id);
        }
    }
    ids
}

fn colliding_raw_ids(
    runtime_catalog: &RuntimeCatalog,
    ontology_ids: &HashSet<u32>,
) -> HashSet<u32> {
    runtime_catalog
        .entity_type_names_with_ids()
        .map(|(id, _)| id.get())
        .filter(|id| ontology_ids.contains(id))
        .collect()
}

fn migratable_raw_ids(
    runtime_catalog: &RuntimeCatalog,
    ontology_ids: &HashSet<u32>,
) -> HashMap<EntityTypeId, EntityTypeId> {
    runtime_catalog
        .entity_type_names_with_ids()
        .map(|(id, _)| id)
        .filter(|id| !ontology_ids.contains(&id.get()))
        // Only this legacy-admission boundary interprets a catalog-local integer
        // as the old untagged topology representation, after collision checks.
        .map(|id| {
            (
                EntityTypeId::ontology(TypeId(id.get())).expect("checked local range"),
                EntityTypeId::runtime(id),
            )
        })
        .collect()
}

/// Remap tagged runtime entity plan IDs → ontology TypeIds when catalog names match.
///
/// Fail closed when two distinct runtime names would claim the same ontology id
/// (ambiguous identity).
fn adoption_name_remaps(
    ontology: Option<&OntologyHandle>,
    runtime_catalog: &RuntimeCatalog,
) -> Result<HashMap<EntityTypeId, EntityTypeId>, GfError> {
    let Some(handle) = ontology else {
        return Ok(HashMap::new());
    };
    let mut remap = HashMap::new();
    let mut ontology_targets: HashMap<EntityTypeId, String> = HashMap::new();
    for (runtime_id, name) in runtime_catalog.entity_type_names_with_ids() {
        let Some(TypeId(ontology_id)) = handle.entity_type_id(name) else {
            continue;
        };
        let tagged = EntityTypeId::runtime(runtime_id);
        let target =
            EntityTypeId::ontology(TypeId(ontology_id)).map_err(|e| storage_err(e.to_string()))?;
        if let Some(prior) = ontology_targets.get(&target)
            && prior != name
        {
            return Err(storage_err(format!(
                "ambiguous runtime→ontology entity label remapping: \
                 {prior:?} and {name:?} both claim ontology type id {ontology_id}"
            )));
        }
        ontology_targets.insert(target, name.to_owned());
        if tagged != target {
            remap.insert(tagged, target);
        }
    }
    Ok(remap)
}

fn collect_label_hits(
    batches: &[RecordBatch],
    candidates: &HashSet<EntityTypeId>,
) -> Result<HashSet<EntityTypeId>, GfError> {
    let mut hits = HashSet::new();
    if candidates.is_empty() {
        return Ok(hits);
    }
    for batch in batches {
        let type_ids = batch
            .column_by_name("type_ids")
            .and_then(|column| column.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| storage_err("node topology missing type_ids"))?;
        let primary = batch
            .column_by_name("type_id")
            .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| storage_err("node topology missing type_id"))?;
        for row in 0..batch.num_rows() {
            if !primary.is_null(row)
                && let Some(value) = PrimaryEntityTypeId::decode(primary.value(row))
                    .map_err(|e| storage_err(e.to_string()))?
                    .label()
                && candidates.contains(&value)
            {
                hits.insert(value);
            }
            if type_ids.is_null(row) {
                continue;
            }
            let values_array = type_ids.value(row);
            let values = values_array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| storage_err("node type_ids are not UInt32"))?;
            for index in 0..values.len() {
                if values.is_null(index) {
                    return Err(storage_err("null node membership"));
                }
                let value = EntityTypeId::decode(values.value(index))
                    .map_err(|e| storage_err(e.to_string()))?;
                if candidates.contains(&value) {
                    hits.insert(value);
                }
            }
        }
    }
    Ok(hits)
}

fn collect_untagged_label_hits(
    batches: &[RecordBatch],
    candidates: &HashSet<u32>,
) -> Result<HashSet<u32>, GfError> {
    let mut hits = HashSet::new();
    if candidates.is_empty() {
        return Ok(hits);
    }
    for batch in batches {
        let type_ids = batch
            .column_by_name("type_ids")
            .and_then(|column| column.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| storage_err("node topology missing type_ids"))?;
        let primary = batch
            .column_by_name("type_id")
            .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| storage_err("node topology missing type_id"))?;
        for row in 0..batch.num_rows() {
            if !primary.is_null(row) {
                let value = primary.value(row);
                if candidates.contains(&value)
                    && EntityTypeId::decode(value)
                        .map_err(|e| storage_err(e.to_string()))?
                        .tagged()
                        .ontology_id()
                        .is_some()
                {
                    hits.insert(value);
                }
            }
            if type_ids.is_null(row) {
                continue;
            }
            let values_array = type_ids.value(row);
            let values = values_array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| storage_err("node type_ids are not UInt32"))?;
            for index in 0..values.len() {
                let value = values.value(index);
                if candidates.contains(&value)
                    && EntityTypeId::decode(value)
                        .map_err(|e| storage_err(e.to_string()))?
                        .tagged()
                        .ontology_id()
                        .is_some()
                {
                    hits.insert(value);
                }
            }
        }
    }
    Ok(hits)
}

fn remap_label_value(
    value: EntityTypeId,
    remap: &HashMap<EntityTypeId, EntityTypeId>,
) -> (EntityTypeId, bool) {
    match remap.get(&value) {
        Some(&mapped) if mapped != value => (mapped, true),
        _ => (value, false),
    }
}

fn remap_batches(
    batches: Vec<RecordBatch>,
    remap: &HashMap<EntityTypeId, EntityTypeId>,
) -> Result<(Vec<RecordBatch>, u64), GfError> {
    let mut remapped = 0u64;
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        let type_ids = batch
            .column_by_name("type_ids")
            .and_then(|column| column.as_any().downcast_ref::<ListArray>())
            .ok_or_else(|| storage_err("node topology missing type_ids"))?;
        let primary = batch
            .column_by_name("type_id")
            .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
            .ok_or_else(|| storage_err("node topology missing type_id"))?;

        let mut primary_builder = UInt32Builder::with_capacity(batch.num_rows());
        let mut list_builder =
            arrow::array::ListBuilder::with_capacity(UInt32Builder::new(), batch.num_rows());

        for row in 0..batch.num_rows() {
            let original_primary = PrimaryEntityTypeId::decode(primary.value(row))
                .map_err(|e| storage_err(e.to_string()))?;
            let (primary_value, changed_primary) = match original_primary.label() {
                Some(id) => {
                    let (id, changed) = remap_label_value(id, remap);
                    (PrimaryEntityTypeId::known(id), changed)
                }
                None => (original_primary, false),
            };
            if changed_primary {
                remapped += 1;
            }
            primary_builder.append_value(primary_value.encode());

            if type_ids.is_null(row) {
                list_builder.append(false);
                continue;
            }
            let values_array = type_ids.value(row);
            let values = values_array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| storage_err("node type_ids are not UInt32"))?;
            for index in 0..values.len() {
                if values.is_null(index) {
                    return Err(storage_err("null node membership"));
                }
                let id = EntityTypeId::decode(values.value(index))
                    .map_err(|e| storage_err(e.to_string()))?;
                let (value, changed) = remap_label_value(id, remap);
                if changed {
                    remapped += 1;
                }
                list_builder.values().append_value(value.encode());
            }
            list_builder.append(true);
        }

        let primary_array: Arc<dyn Array> = Arc::new(primary_builder.finish());
        let raw_lists = list_builder.finish();
        let list_array: Arc<dyn Array> = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::UInt32, false)),
            raw_lists.offsets().clone(),
            raw_lists.values().clone(),
            None,
        ));

        let primary_column_idx = batch
            .schema()
            .index_of("type_id")
            .map_err(|e| storage_err(format!("node topology missing type_id column: {e}")))?;
        let membership_column_idx = batch
            .schema()
            .index_of("type_ids")
            .map_err(|e| storage_err(format!("node topology missing type_ids column: {e}")))?;
        let mut columns = batch.columns().to_vec();
        columns[primary_column_idx] = primary_array;
        columns[membership_column_idx] = list_array;
        out.push(
            RecordBatch::try_new(TOPOLOGY_NODES_SCHEMA.clone(), columns)
                .map_err(|e| storage_err(e.to_string()))?,
        );
    }
    Ok((out, remapped))
}

fn merge_remaps(
    legacy: HashMap<EntityTypeId, EntityTypeId>,
    adoption: HashMap<EntityTypeId, EntityTypeId>,
) -> Result<HashMap<EntityTypeId, EntityTypeId>, GfError> {
    let mut remap = legacy;
    for (from, to) in adoption {
        if let Some(existing) = remap.get(&from)
            && *existing != to
        {
            return Err(storage_err(format!(
                "ambiguous runtime entity label remap for id {from:?}: {existing:?} vs {to:?}"
            )));
        }
        remap.insert(from, to);
    }
    Ok(remap)
}

// Process one topology decoder batch at a time. Targeted authenticated property
// reads and private fragment commits prevent property windows growing with the graph.
fn promote_node_properties(
    dir: &Path,
    ontology: &OntologyHandle,
    runtime_catalog: &RuntimeCatalog,
) -> Result<(), GfError> {
    use arrow::array::FixedSizeBinaryArray;
    use std::collections::{BTreeMap, BTreeSet};
    let remaps = adoption_name_remaps(Some(ontology), runtime_catalog)?;
    if remaps.is_empty() {
        return Ok(());
    }
    let source = crate::property_overlay::authenticated_property_inventory_for_route(
        dir,
        crate::PropertyRouteKind::Node,
        "_untyped",
    )?;
    let Some(schema) = source.route_schema(crate::PropertyRouteKind::Node, "_untyped") else {
        return Ok(());
    };
    let names = runtime_catalog
        .entity_type_names_with_ids()
        .filter_map(|(id, name)| {
            remaps
                .contains_key(&EntityTypeId::runtime(id))
                .then_some((EntityTypeId::runtime(id).encode(), name.to_owned()))
        })
        .collect::<HashMap<_, _>>();
    for path in crate::mutator::node_parquet_files(dir)? {
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(path).map_err(pq_err)?,
        )
        .map_err(pq_err)?
        .with_batch_size(4096)
        .build()
        .map_err(pq_err)?;
        for batch in reader {
            let batch = batch.map_err(pq_err)?;
            let primary = batch
                .column_by_name("type_id")
                .and_then(|a| a.as_any().downcast_ref::<UInt32Array>())
                .ok_or_else(|| storage_err("missing node primary label"))?;
            let uuids = batch
                .column_by_name("node_uuid")
                .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(|| storage_err("missing node UUID"))?;
            let mut targets = BTreeMap::<String, BTreeSet<[u8; 16]>>::new();
            for row in 0..batch.num_rows() {
                if let Some(name) = names.get(&primary.value(row)) {
                    targets
                        .entry(name.clone())
                        .or_default()
                        .insert(uuids.value(row).try_into().map_err(pq_err)?);
                }
            }
            for (name, targets) in targets {
                let (rows, _) =
                    crate::property_overlay::read_authenticated_property_snapshots_for_inventory(
                        &source,
                        crate::PropertyRouteKind::Node,
                        "_untyped",
                        &targets,
                    )?;
                if rows.is_empty() {
                    continue;
                }
                let mut staged = RewriteBatch::new();
                crate::writer::stage_promoted_properties(
                    &mut staged,
                    dir,
                    &name,
                    crate::PropertyRouteKind::Node,
                    schema.as_ref(),
                    &rows,
                )?;
                let current_source =
                    crate::property_overlay::authenticated_property_inventory_for_route(
                        dir,
                        crate::PropertyRouteKind::Node,
                        "_untyped",
                    )?;
                crate::writer::stage_property_tombstones_authenticated(
                    &mut staged,
                    dir,
                    &current_source,
                    crate::PropertyRouteKind::Node,
                    "_untyped",
                    &rows.keys().copied().collect::<HashSet<_>>(),
                )?;
                staged.commit_at(dir)?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)] // bounded ownership transfer and source tombstones form one operation
fn promote_edge_properties(dir: &Path) -> Result<HashSet<std::path::PathBuf>, GfError> {
    use arrow::array::{FixedSizeBinaryArray, StringArray};
    use std::collections::{BTreeMap, BTreeSet};
    let inventory = crate::property_overlay::authenticated_property_inventory(dir)?;
    let kind = crate::PropertyRouteKind::Edge;
    let mut transferred = HashSet::new();
    let Some(schema) = inventory.route_schema(kind, "_exploratory") else {
        return Ok(transferred);
    };
    for (_, path) in inventory.edge_files(Some("_exploratory")) {
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(path).map_err(pq_err)?,
        )
        .map_err(pq_err)?
        .with_batch_size(4096)
        .build()
        .map_err(pq_err)?;
        for batch in reader {
            let batch = batch.map_err(pq_err)?;
            let names = batch
                .column_by_name("rel_type_name")
                .and_then(|a| a.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| storage_err("missing exploratory relation name"))?;
            let uuids = batch
                .column_by_name("edge_uuid")
                .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(|| storage_err("missing edge UUID"))?;
            let mut targets = BTreeMap::<String, BTreeSet<[u8; 16]>>::new();
            for row in 0..batch.num_rows() {
                let name = names.value(row);
                targets
                    .entry(name.to_owned())
                    .or_default()
                    .insert(uuids.value(row).try_into().map_err(pq_err)?);
            }
            for (name, targets) in targets {
                let (mut rows, _) =
                    crate::property_overlay::read_authenticated_property_snapshots_for_inventory(
                        &inventory,
                        kind,
                        "_exploratory",
                        &targets,
                    )?;
                let (present, _) =
                    crate::property_overlay::read_authenticated_property_presence_for_inventory(
                        &inventory,
                        kind,
                        "_exploratory",
                        &targets,
                    )?;
                for uuid in present {
                    rows.entry(uuid)
                        .or_insert_with(|| crate::PropertySnapshotRow {
                            uuid,
                            tombstone: true,
                            values: BTreeMap::new(),
                        });
                }
                if rows.is_empty() {
                    continue;
                }
                let prior_destination =
                    crate::property_overlay::authenticated_property_inventory_for_route(
                        dir, kind, &name,
                    )?;
                let prior_paths = prior_destination
                    .property_fragments(kind, &name)
                    .into_iter()
                    .map(|fragment| fragment.path)
                    .collect::<HashSet<_>>();
                let mut staged = RewriteBatch::new();
                crate::writer::stage_promoted_properties(
                    &mut staged,
                    dir,
                    &name,
                    kind,
                    schema.as_ref(),
                    &rows,
                )?;
                let current_source =
                    crate::property_overlay::authenticated_property_inventory_for_route(
                        dir,
                        kind,
                        "_exploratory",
                    )?;
                crate::writer::stage_property_tombstones_authenticated(
                    &mut staged,
                    dir,
                    &current_source,
                    kind,
                    "_exploratory",
                    &rows.keys().copied().collect::<HashSet<_>>(),
                )?;
                staged.commit_at(dir)?;
                let current_destination =
                    crate::property_overlay::authenticated_property_inventory_for_route(
                        dir, kind, &name,
                    )?;
                transferred.extend(
                    current_destination
                        .property_fragments(kind, &name)
                        .into_iter()
                        .map(|fragment| fragment.path)
                        .filter(|path| !prior_paths.contains(path)),
                );
            }
        }
    }
    Ok(transferred)
}

// Tombstones remain property ownership evidence. Remove every physical source
// occurrence after transferring that ownership, including old snapshots.
fn stage_retired_edge_property_owners(
    dir: &Path,
    transferred: &HashSet<std::path::PathBuf>,
    staged: &mut RewriteBatch,
) -> Result<(), GfError> {
    use arrow::array::{BooleanArray, FixedSizeBinaryArray};
    use std::collections::BTreeSet;
    if transferred.is_empty() {
        return Ok(());
    }
    let inventory =
        crate::property_overlay::authenticated_property_inventory_for_rewrite(dir, staged)?;
    let mut transferred_inventory =
        crate::property_overlay::authenticated_property_inventory_for_rewrite(dir, staged)?;
    transferred_inventory.retain_property_fragment_paths(transferred);
    let kind = crate::PropertyRouteKind::Edge;
    let routes = transferred_inventory.routes(kind).collect::<Vec<_>>();
    let final_summary = inventory
        .route_schema(kind, "_exploratory")
        .and_then(|schema| {
            schema
                .metadata()
                .get(crate::property_overlay::PROPERTY_LIVE_SCHEMA_KEY)
                .cloned()
        });
    let fragments = inventory.property_fragments(kind, "_exploratory");
    let last_fragment = fragments.last().map(|fragment| fragment.path.clone());
    if routes.is_empty() {
        return Ok(());
    }
    for fragment in fragments {
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(&fragment.path).map_err(pq_err)?,
        )
        .map_err(pq_err)?
        .with_batch_size(4096)
        .build()
        .map_err(pq_err)?;
        // Historical summaries counted the removed ownership. Preserve the
        // current cumulative summary only on the newest source fragment.
        let mut metadata = reader.schema().metadata().clone();
        metadata.remove(crate::property_overlay::PROPERTY_LIVE_SCHEMA_KEY);
        if Some(&fragment.path) == last_fragment.as_ref()
            && let Some(summary) = &final_summary
        {
            metadata.insert(
                crate::property_overlay::PROPERTY_LIVE_SCHEMA_KEY.into(),
                summary.clone(),
            );
        }
        metadata.insert(
            crate::property_overlay::PROPERTY_OVERLAY_FORMAT_KEY.into(),
            crate::property_overlay::PROPERTY_OVERLAY_FORMAT.into(),
        );
        metadata.insert(
            crate::property_overlay::PROPERTY_ROUTE_KEY.into(),
            "_exploratory".into(),
        );
        metadata.insert(
            crate::property_overlay::PROPERTY_KIND_KEY.into(),
            "edge".into(),
        );
        metadata.insert(
            crate::property_overlay::PROPERTY_GENERATION_KEY.into(),
            fragment.id.generation.to_string(),
        );
        metadata.insert(
            crate::property_overlay::PROPERTY_ORDINAL_KEY.into(),
            fragment.id.ordinal.to_string(),
        );
        let schema = Arc::new(reader.schema().as_ref().clone().with_metadata(metadata));
        let batches = reader.map(|batch| {
            let batch = batch.map_err(pq_err)?;
            let uuids = batch
                .column_by_name("edge_uuid")
                .and_then(|a| a.as_any().downcast_ref::<FixedSizeBinaryArray>())
                .ok_or_else(|| storage_err("edge property UUID missing"))?;
            let targets = (0..batch.num_rows())
                .map(|row| uuids.value(row).try_into().map_err(pq_err))
                .collect::<Result<BTreeSet<[u8; 16]>, _>>()?;
            let mut retired = BTreeSet::new();
            for route in &routes {
                let (present, _) =
                    crate::property_overlay::read_authenticated_property_presence_for_inventory(
                        &transferred_inventory,
                        kind,
                        route,
                        &targets,
                    )?;
                retired.extend(present);
            }
            let keep = BooleanArray::from(
                (0..batch.num_rows())
                    .map(|row| {
                        !retired.contains(
                            &<[u8; 16]>::try_from(uuids.value(row)).expect("validated UUID width"),
                        )
                    })
                    .collect::<Vec<_>>(),
            );
            let filtered = arrow::compute::filter_record_batch(&batch, &keep).map_err(pq_err)?;
            RecordBatch::try_new(schema.clone(), filtered.columns().to_vec()).map_err(pq_err)
        });
        staged.stage_batches(&fragment.path, schema.clone(), batches)?;
    }
    Ok(())
}

fn stage_promoted_edges(dir: &Path, staged: &mut RewriteBatch) -> Result<bool, GfError> {
    use arrow::array::{BooleanArray, StringArray, UInt64Array};
    let inventory =
        crate::property_overlay::authenticated_property_inventory_for_rewrite(dir, staged)?;
    let mut changed = false;
    for (_, path) in inventory.edge_files(Some("_exploratory")) {
        let reader = || -> Result<_, GfError> {
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                std::fs::File::open(&path).map_err(pq_err)?,
            )
            .map_err(pq_err)?
            .with_batch_size(4096)
            .build()
            .map_err(pq_err)
        };
        let mut file_changed = false;
        for batch in reader()? {
            let batch = batch.map_err(pq_err)?;
            let names = batch
                .column_by_name("rel_type_name")
                .and_then(|a| a.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| storage_err("exploratory edge relation name missing"))?;
            let routes = (0..batch.num_rows())
                .map(|row| names.value(row))
                .collect::<std::collections::BTreeSet<_>>();
            for route in routes {
                let selected = arrow::compute::filter_record_batch(
                    &batch,
                    &BooleanArray::from(
                        (0..batch.num_rows())
                            .map(|row| names.value(row) == route)
                            .collect::<Vec<_>>(),
                    ),
                )
                .map_err(pq_err)?;
                let schema = crate::TYPED_EDGE_SCHEMA.clone();
                let columns = schema
                    .fields()
                    .iter()
                    .map(|field| {
                        selected
                            .column_by_name(field.name())
                            .cloned()
                            .ok_or_else(|| storage_err("promoted edge field missing"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let selected = RecordBatch::try_new(schema.clone(), columns).map_err(pq_err)?;
                let ids = selected
                    .column_by_name("edge_id")
                    .and_then(|a| a.as_any().downcast_ref::<UInt64Array>())
                    .ok_or_else(|| storage_err("promoted edge ID missing"))?;
                let first = ids.value(0);
                let last = ids.value(ids.len() - 1);
                let component = staged.route_component(dir, route)?;
                let target = dir
                    .join("topology/edges")
                    .join(component)
                    .join(format!("{first:020}-{last:020}.parquet"));
                if target.exists() || staged.staged_temp(&target).is_some() {
                    return Err(storage_err("promoted edge surrogate range already exists"));
                }
                staged.stage(&target, schema, &selected)?;
                file_changed = true;
            }
        }
        if file_changed {
            let schema = reader()?.schema();
            // Ontology modes read every named relationship through its own
            // route, including Advisory names not declared by the ontology.
            staged.stage_batches(&path, schema, std::iter::empty())?;
            changed = true;
        }
    }
    Ok(changed)
}

fn reconcile_inner(
    dir: &Path,
    ontology: Option<&OntologyHandle>,
    runtime_catalog: &RuntimeCatalog,
    rewrite: bool,
    promotion: bool,
) -> Result<RuntimeEntityLabelReconcile, GfError> {
    let marked = has_runtime_entity_label_encoding_marker(dir);
    let ontology_ids = ontology_entity_ids(ontology);
    let collisions = colliding_raw_ids(runtime_catalog, &ontology_ids);
    let adoption = adoption_name_remaps(ontology, runtime_catalog)?;
    let legacy = if marked && ontology.is_none() {
        // Session ontologies are not durable. A marked project may still store
        // untagged ontology TypeIds (e.g. Person = 0) beside tagged runtime
        // labels; remapping those untagged values without the ontology handle
        // would silently reclassify them as runtime entities.
        HashMap::new()
    } else {
        migratable_raw_ids(runtime_catalog, &ontology_ids)
    };
    let remap = merge_remaps(legacy, adoption)?;

    // Nothing to validate or rewrite: skip topology I/O entirely. This keeps
    // non-parquet legacy snapshot placeholders out of the migration path and
    // avoids a full nodes.parquet scan on already-reconciled projects.
    let promotes_relations = promotion
        && ontology.is_some_and(|ontology| {
            runtime_catalog
                .relation_type_names_with_ids()
                .any(|(_, name)| ontology.relation_type_id(name).is_some())
        });
    if collisions.is_empty() && remap.is_empty() && !promotes_relations {
        if rewrite && !marked {
            write_runtime_entity_label_encoding_marker(dir)?;
        }
        return Ok(RuntimeEntityLabelReconcile {
            remapped_label_values: 0,
            colliding_raw_ids: 0,
            encoding_marked: marked || rewrite,
        });
    }

    let candidate_keys = remap.keys().copied().collect::<HashSet<_>>();
    let mut remapped_label_values = 0u64;
    let mut staged = RewriteBatch::new();
    for path in crate::mutator::node_parquet_files(dir)? {
        let reader = || -> Result<_, GfError> {
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                std::fs::File::open(&path).map_err(pq_err)?,
            )
            .map_err(pq_err)?
            .with_batch_size(4096)
            .build()
            .map_err(pq_err)
        };
        let mut needs_rewrite = false;
        for batch in reader()? {
            let batches = normalize_topology_nodes(vec![batch.map_err(pq_err)?]).map_err(pq_err)?;
            if !marked && !collect_untagged_label_hits(&batches, &collisions)?.is_empty() {
                return Err(storage_err(
                    "runtime entity label ID collision with ontology type IDs",
                ));
            }
            needs_rewrite |= !collect_label_hits(&batches, &candidate_keys)?.is_empty();
        }
        if !needs_rewrite {
            continue;
        }
        if !rewrite {
            return Err(storage_err(
                "runtime entity label IDs require writable reconciliation; read-only open cannot rewrite topology",
            ));
        }
        let batches = reader()?.map(|batch| {
            let source = normalize_topology_nodes(vec![batch.map_err(pq_err)?]).map_err(pq_err)?;
            let (mut rewritten, count) = remap_batches(source, &remap)?;
            remapped_label_values = remapped_label_values.saturating_add(count);
            Ok(rewritten.remove(0))
        });
        staged.stage_batches(&path, TOPOLOGY_NODES_SCHEMA.clone(), batches)?;
    }
    if promotion
        && remapped_label_values > 0
        && let Some(ontology) = ontology
    {
        promote_node_properties(dir, ontology, runtime_catalog)?;
    }
    let edges_changed = if promotion
        && rewrite
        && ontology.is_some()
        && (remapped_label_values > 0 || promotes_relations)
    {
        let transferred = promote_edge_properties(dir)?;
        stage_retired_edge_property_owners(dir, &transferred, &mut staged)?;
        stage_promoted_edges(dir, &mut staged)?
    } else {
        false
    };
    if remapped_label_values > 0 || edges_changed {
        crate::uuid_membership::commit_uuid_neutral_topology_rewrite(dir, staged)?;
    }

    if rewrite && !marked {
        write_runtime_entity_label_encoding_marker(dir)?;
    }

    Ok(RuntimeEntityLabelReconcile {
        remapped_label_values,
        colliding_raw_ids: collisions.len(),
        encoding_marked: marked || rewrite,
    })
}

/// Detect colliding legacy entity IDs and migrate unambiguous untagged runtime
/// entity label values to the tagged plan encoding. When an ontology is present,
/// also promote same-named tagged runtime labels onto ontology TypeIds.
///
/// # Errors
/// Returns [`GfError::Storage`] when an unmarked project still stores untagged
/// node labels whose raw IDs are claimed by both ontology and runtime domains,
/// when adoption remapping is ambiguous, or on I/O / Arrow failures while rewriting.
pub fn reconcile_runtime_entity_label_ids(
    dir: &Path,
    ontology: Option<&OntologyHandle>,
    runtime_catalog: &RuntimeCatalog,
) -> Result<RuntimeEntityLabelReconcile, GfError> {
    reconcile_inner(dir, ontology, runtime_catalog, true, false)
}

/// Promote labels and property/relationship routes in a private adoption candidate.
/// The caller must publish this complete tree with its ontology before installing readers.
///
/// # Errors
/// Refuses ambiguous identities or property owners and propagates authenticated I/O failures.
pub fn promote_runtime_graph_for_ontology(
    dir: &Path,
    ontology: &OntologyHandle,
    runtime_catalog: &RuntimeCatalog,
) -> Result<RuntimeEntityLabelReconcile, GfError> {
    reconcile_inner(dir, Some(ontology), runtime_catalog, true, true)
}

/// Validate runtime entity label encoding without rewriting topology.
///
/// Used for read-only opens: fail closed on unmarked collisions or pending
/// remaps that would require a writable migration.
///
/// # Errors
/// Same collision / ambiguity failures as
/// [`reconcile_runtime_entity_label_ids`], plus rejection when a rewrite is
/// required.
pub fn validate_runtime_entity_label_ids(
    dir: &Path,
    ontology: Option<&OntologyHandle>,
    runtime_catalog: &RuntimeCatalog,
) -> Result<RuntimeEntityLabelReconcile, GfError> {
    reconcile_inner(dir, ontology, runtime_catalog, false, false)
}

/// Pure helper: tagged runtime entity plan IDs stay disjoint from ontology IDs.
#[must_use]
pub fn runtime_entity_plan_id_is_disjoint_from_ontology(
    runtime_id: RuntimeEntityId,
    ontology_id: TypeId,
) -> bool {
    EntityTypeId::ontology(ontology_id)
        .is_ok_and(|ontology| EntityTypeId::runtime(runtime_id) != ontology)
}

#[cfg(test)]
mod tests {

    #[test]
    fn catalog_persistence_replaces_read_only_cas_aliases() {
        use std::fs::{self, File};
        let source = tempfile::tempdir().unwrap();
        let mut writer = crate::GraphWriter::open_at(
            source.path(),
            graphforge_core::OntologyMode::Exploratory,
            1,
        )
        .unwrap();
        writer.flush().unwrap();
        drop(writer);
        let mut catalog = graphforge_ir::RuntimeCatalog::new();
        catalog.intern_property("before", None).unwrap();
        super::persist_runtime_catalog(source.path(), &catalog).unwrap();
        let (inventory, _) = crate::capture_graph_files(source.path()).unwrap();
        let objects = tempfile::tempdir().unwrap();
        for entry in &inventory.files {
            let bytes = fs::read(source.path().join(&entry.relative_path)).unwrap();
            let (digest, _) = crate::install_graph_object_bytes(objects.path(), &bytes).unwrap();
            assert_eq!(digest, entry.content_sha256);
        }
        let workspace = tempfile::tempdir().unwrap();
        crate::materialize_graph_objects(objects.path(), &inventory, workspace.path()).unwrap();
        let controls = [
            "topology/runtime_catalog.parquet",
            "topology/runtime_entity_label_encoding.json",
        ];
        for path in controls {
            let file = File::open(workspace.path().join(path)).unwrap();
            assert!(file.metadata().unwrap().permissions().readonly());
            assert_eq!(graphforge_filesystem::file_link_count(&file).unwrap(), 2);
        }
        catalog.intern_property("after", None).unwrap();
        super::persist_runtime_catalog(workspace.path(), &catalog).unwrap();
        for path in controls {
            let entry = inventory
                .files
                .iter()
                .find(|entry| entry.relative_path == path)
                .unwrap();
            let object = crate::graph_object_path(objects.path(), &entry.content_sha256).unwrap();
            assert_eq!(
                fs::read(&object).unwrap(),
                fs::read(source.path().join(path)).unwrap()
            );
            assert!(fs::metadata(&object).unwrap().permissions().readonly());
            let private = File::open(workspace.path().join(path)).unwrap();
            assert_eq!(graphforge_filesystem::file_link_count(&private).unwrap(), 1);
            assert_ne!(
                graphforge_filesystem::file_identity(&private).unwrap(),
                graphforge_filesystem::file_identity(&File::open(object).unwrap()).unwrap()
            );
        }
        let batches = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            File::open(workspace.path().join(controls[0])).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert_eq!(batches, vec![catalog.to_record_batch()]);
        assert_eq!(
            fs::read(workspace.path().join(controls[1])).unwrap(),
            super::runtime_entity_label_encoding_bytes().unwrap()
        );
    }
    use super::*;
    use crate::schemas::TOPOLOGY_NODES_SCHEMA;
    use arrow::array::{
        FixedSizeBinaryArray, ListArray, TimestampMicrosecondArray, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::UInt32Type;
    use graphforge_ontology::{OntologyCompiler, OntologyHandle, OntologyLoader};
    use parquet::arrow::ArrowWriter;
    use std::io::Cursor;
    use tempfile::TempDir;

    fn write_nodes(dir: &Path, type_ids: &[&[u32]]) {
        let topology = dir.join("topology");
        std::fs::create_dir_all(&topology).unwrap();
        let n = type_ids.len();
        let mut uuids = Vec::with_capacity(n);
        for i in 0..n {
            let mut bytes = [0u8; 16];
            bytes[15] = (i + 1) as u8;
            uuids.push(Some(bytes.to_vec()));
        }
        let uuid_array =
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(uuids.into_iter(), 16).unwrap();
        let node_ids = UInt64Array::from((1..=n as u64).collect::<Vec<_>>());
        let primary = UInt32Array::from(
            type_ids
                .iter()
                .map(|labels| labels.first().copied().unwrap_or(u32::MAX))
                .collect::<Vec<_>>(),
        );
        let lists = ListArray::from_iter_primitive::<UInt32Type, _, _>(
            type_ids
                .iter()
                .map(|labels| Some(labels.iter().copied().map(Some).collect::<Vec<_>>())),
        );
        let lists = ListArray::new(
            Arc::new(Field::new("item", DataType::UInt32, false)),
            lists.offsets().clone(),
            lists.values().clone(),
            None,
        );
        let recorded = TimestampMicrosecondArray::from(vec![1i64; n])
            .with_timezone_opt(Some(Arc::from("UTC")));
        let batch = RecordBatch::try_new(
            TOPOLOGY_NODES_SCHEMA.clone(),
            vec![
                Arc::new(uuid_array),
                Arc::new(node_ids),
                Arc::new(primary),
                Arc::new(lists),
                Arc::new(recorded.clone()),
                Arc::new(recorded),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(topology.join("nodes.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, TOPOLOGY_NODES_SCHEMA.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn membership_remap_preserves_absent_original_primary() {
        let directory = TempDir::new().unwrap();
        write_nodes(directory.path(), &[&[0]]);
        let batch = read_nodes(directory.path()).unwrap().remove(0);
        let mut columns = batch.columns().to_vec();
        columns[batch.schema().index_of("type_id").unwrap()] =
            Arc::new(UInt32Array::from(vec![u32::MAX]));
        let batch = RecordBatch::try_new(batch.schema(), columns).unwrap();
        let legacy = EntityTypeId::ontology(TypeId(0)).unwrap();
        let runtime = EntityTypeId::runtime(RuntimeEntityId::new(0).unwrap());
        let (batches, count) =
            remap_batches(vec![batch], &HashMap::from([(legacy, runtime)])).unwrap();
        assert_eq!(count, 1);
        let primary = batches[0]
            .column_by_name("type_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(primary.value(0), u32::MAX);
        let membership = batches[0]
            .column_by_name("type_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value(0);
        assert_eq!(
            membership
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(0),
            runtime.encode()
        );
    }

    fn person_ontology() -> OntologyHandle {
        let yaml = r#"
ontology_id: collision
version: "v1"
entity_types:
  - name: Person
    abstract: false
relation_types: []
properties: []
constraints: []
migrations: []
"#;
        let doc = OntologyLoader::load_yaml(Cursor::new(yaml.as_bytes())).unwrap();
        OntologyHandle::new(OntologyCompiler::compile(&doc).unwrap())
    }

    #[test]
    fn tagged_runtime_entity_id_is_disjoint_from_ontology_zero() {
        assert!(runtime_entity_plan_id_is_disjoint_from_ontology(
            RuntimeEntityId::new(0).unwrap(),
            TypeId(0)
        ));
        assert_eq!(
            EntityTypeId::runtime(RuntimeEntityId::new(0).unwrap()).encode(),
            1_073_741_824
        );
    }

    #[test]
    fn exploratory_legacy_ids_migrate_to_tagged_form() {
        const CHILD: &str = "GRAPHFORGE_RUNTIME_LABEL_MIGRATION_IO_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("runtime_entity_labels::tests::exploratory_legacy_ids_migrate_to_tagged_form")
                .arg("--nocapture")
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success(), "isolated runtime-label I/O proof failed");
            return;
        }

        let dir = TempDir::new().unwrap();
        let endpoints = [uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2)];
        let mut seed =
            crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Exploratory, 1)
                .unwrap();
        seed.create_node(
            endpoints[0],
            graphforge_value::EntityTypeId::ontology(TypeId(0)).unwrap(),
        )
        .unwrap();
        seed.create_node(
            endpoints[1],
            graphforge_value::EntityTypeId::ontology(TypeId(1)).unwrap(),
        )
        .unwrap();
        seed.flush().unwrap();
        let mut catalog = RuntimeCatalog::new();
        assert_eq!(catalog.intern_label("Ghost").unwrap().get(), 0);
        assert_eq!(catalog.intern_label("Spectre").unwrap().get(), 1);

        let outcome =
            reconcile_runtime_entity_label_ids(dir.path(), None, &catalog).expect("migrate");
        assert_eq!(outcome.remapped_label_values, 4);
        assert!(outcome.encoding_marked);
        assert!(has_runtime_entity_label_encoding_marker(dir.path()));
        assert!(crate::uuid_membership_index_is_fresh(dir.path()).unwrap());

        // Reopening and probing the generation-carried UUID authority must not
        // decode the topology that reconciliation just rewrote.
        crate::io_stats::reset();
        let mut writer =
            crate::GraphWriter::open_at(dir.path(), graphforge_core::OntologyMode::Exploratory, 2)
                .unwrap();
        let work = writer.register_existing_endpoints(&endpoints).unwrap();
        assert_eq!(work.found, 2);
        let io = crate::io_stats::snapshot();
        assert_eq!(io.node_full_reads, 0);
        assert_eq!(io.node_filtered_reads, 0);

        let batches = read_nodes(dir.path()).unwrap();
        let type_ids = batches[0]
            .column_by_name("type_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let row0 = type_ids.value(0);
        let row0 = row0.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(
            row0.value(0),
            EntityTypeId::runtime(RuntimeEntityId::new(0).unwrap()).encode()
        );
    }

    #[test]
    fn uuid_neutral_label_rewrite_recovers_after_committed_refresh_failure() {
        let dir = TempDir::new().unwrap();
        write_nodes(dir.path(), &[&[0], &[1]]);
        let mut catalog = RuntimeCatalog::new();
        catalog.intern_label("Ghost").unwrap();
        catalog.intern_label("Spectre").unwrap();
        reconcile_runtime_entity_label_ids(dir.path(), None, &catalog).unwrap();

        let stage_current = || {
            let batches = read_nodes(dir.path()).unwrap();
            let batch = arrow::compute::concat_batches(&TOPOLOGY_NODES_SCHEMA, &batches).unwrap();
            let mut staged = RewriteBatch::new();
            staged
                .restage(
                    &dir.path().join("topology/nodes.parquet"),
                    TOPOLOGY_NODES_SCHEMA.clone(),
                    &batch,
                )
                .unwrap();
            staged
        };

        let before = crate::read_topology_generation(dir.path()).unwrap();
        crate::uuid_membership::fail_next_snapshot_refresh_for_test();
        let error = crate::uuid_membership::commit_uuid_neutral_topology_rewrite(
            dir.path(),
            stage_current(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("committed but UUID index snapshot refresh failed")
        );
        assert_eq!(
            crate::read_topology_generation(dir.path()).unwrap(),
            before + 1
        );
        assert!(crate::uuid_membership_index_is_fresh(dir.path()).unwrap());

        assert_eq!(
            crate::uuid_membership::commit_uuid_neutral_topology_rewrite(
                dir.path(),
                stage_current(),
            )
            .unwrap(),
            Some(before + 2)
        );
        assert!(crate::uuid_membership_index_is_fresh(dir.path()).unwrap());
        assert_eq!(
            crate::UuidMembershipIndex::open(dir.path())
                .unwrap()
                .count(crate::UuidIndexKind::Node),
            2
        );
    }

    #[test]
    fn unmarked_colliding_legacy_ids_fail_closed() {
        let dir = TempDir::new().unwrap();
        write_nodes(dir.path(), &[&[0]]);
        let mut catalog = RuntimeCatalog::new();
        catalog.intern_label("Ghost").unwrap();
        let handle = person_ontology();
        assert_eq!(handle.entity_type_id("Person"), Some(TypeId(0)));

        let err = reconcile_runtime_entity_label_ids(dir.path(), Some(&handle), &catalog)
            .expect_err("collision must fail");
        let message = err.to_string();
        assert!(
            message.contains("runtime entity label ID collision"),
            "{message}"
        );
    }

    #[test]
    fn marked_project_keeps_ontology_zero_beside_runtime_catalog_zero() {
        let dir = TempDir::new().unwrap();
        let ghost = EntityTypeId::runtime(RuntimeEntityId::new(0).unwrap()).encode();
        write_nodes(dir.path(), &[&[0], &[ghost]]);
        write_runtime_entity_label_encoding_marker(dir.path()).unwrap();
        let mut catalog = RuntimeCatalog::new();
        catalog.intern_label("Ghost").unwrap();
        let handle = person_ontology();

        let outcome = reconcile_runtime_entity_label_ids(dir.path(), Some(&handle), &catalog)
            .expect("marked project must open");
        assert_eq!(outcome.remapped_label_values, 0);
        assert_eq!(outcome.colliding_raw_ids, 1);
    }

    #[test]
    fn marked_project_without_ontology_does_not_remap_untagged_zero() {
        let dir = TempDir::new().unwrap();
        let ghost = EntityTypeId::runtime(RuntimeEntityId::new(0).unwrap()).encode();
        write_nodes(dir.path(), &[&[0], &[ghost]]);
        write_runtime_entity_label_encoding_marker(dir.path()).unwrap();
        let mut catalog = RuntimeCatalog::new();
        catalog.intern_label("Ghost").unwrap();

        let outcome = reconcile_runtime_entity_label_ids(dir.path(), None, &catalog)
            .expect("marked project without ontology must not rewrite ontology zeros");
        assert_eq!(outcome.remapped_label_values, 0);

        let batches = read_nodes(dir.path()).unwrap();
        let type_ids = batches[0]
            .column_by_name("type_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let row0 = type_ids.value(0);
        let row0 = row0.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(row0.value(0), 0, "Person-shaped untagged zero must survive");
    }

    #[test]
    fn adoption_promotes_same_named_tagged_person_keeps_ghost_tagged() {
        let dir = TempDir::new().unwrap();
        let mut catalog = RuntimeCatalog::new();
        let person_runtime = catalog.intern_label("Person").unwrap();
        let ghost_runtime = catalog.intern_label("Ghost").unwrap();
        let person_tagged = EntityTypeId::runtime(person_runtime).encode();
        let ghost_tagged = EntityTypeId::runtime(ghost_runtime).encode();
        write_nodes(dir.path(), &[&[person_tagged], &[ghost_tagged]]);
        write_runtime_entity_label_encoding_marker(dir.path()).unwrap();
        let handle = person_ontology();
        assert_eq!(handle.entity_type_id("Person"), Some(TypeId(0)));

        let outcome = reconcile_runtime_entity_label_ids(dir.path(), Some(&handle), &catalog)
            .expect("adoption remap");
        assert!(outcome.remapped_label_values >= 2);

        let batches = read_nodes(dir.path()).unwrap();
        let type_ids = batches[0]
            .column_by_name("type_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let row0_array = type_ids.value(0);
        let row0 = row0_array.as_any().downcast_ref::<UInt32Array>().unwrap();
        let row1_array = type_ids.value(1);
        let row1 = row1_array.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(row0.value(0), 0, "Person must promote to ontology TypeId");
        assert_eq!(
            row1.value(0),
            ghost_tagged,
            "Ghost must remain tagged runtime"
        );
    }

    #[test]
    fn marked_empty_remap_skips_non_parquet_placeholder() {
        let dir = TempDir::new().unwrap();
        let topology = dir.path().join("topology");
        std::fs::create_dir_all(&topology).unwrap();
        std::fs::write(topology.join("nodes.parquet"), b"legacy").unwrap();
        write_runtime_entity_label_encoding_marker(dir.path()).unwrap();
        let catalog = RuntimeCatalog::new();

        let outcome = reconcile_runtime_entity_label_ids(dir.path(), None, &catalog)
            .expect("legacy placeholder must not be read as parquet");
        assert_eq!(outcome.remapped_label_values, 0);
        assert!(outcome.encoding_marked);
    }

    #[test]
    fn validate_read_only_rejects_pending_adoption_remap() {
        let dir = TempDir::new().unwrap();
        let mut catalog = RuntimeCatalog::new();
        let person_runtime = catalog.intern_label("Person").unwrap();
        let person_tagged = EntityTypeId::runtime(person_runtime).encode();
        write_nodes(dir.path(), &[&[person_tagged]]);
        write_runtime_entity_label_encoding_marker(dir.path()).unwrap();
        let handle = person_ontology();

        let err = validate_runtime_entity_label_ids(dir.path(), Some(&handle), &catalog)
            .expect_err("read-only must reject pending rewrite");
        assert!(
            err.to_string().contains("read-only open cannot rewrite"),
            "{err}"
        );
    }
}
