//! Runtime entity label identity reconciliation (#702).
//!
//! Bound/persisted node label TypeIds for runtime-catalog entities are tagged
//! with [`graphforge_ir::RUNTIME_ENTITY_TYPE_TAG`]. Legacy projects may still
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
use arrow::record_batch::RecordBatch;
use graphforge_core::{GfError, TypeId};
use graphforge_ir::{
    RuntimeCatalog, RuntimeTypeId, is_runtime_entity_type_id, runtime_entity_type_id,
};
use graphforge_ontology::OntologyHandle;
use serde::{Deserialize, Serialize};

use crate::catalog::{normalize_topology_nodes, read_nodes};
use crate::generation::commit_topology_aware;
use crate::mutator::node_parquet_files;
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
    let marker = EncodingMarker {
        format: "graphforge-runtime-entity-label-encoding".into(),
        version: RUNTIME_ENTITY_LABEL_ENCODING_VERSION,
    };
    let bytes = serde_json::to_vec_pretty(&marker).map_err(|e| storage_err(e.to_string()))?;
    let path = encoding_path(dir);
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, bytes).map_err(|e| storage_err(e.to_string()))?;
    crate::record_graph_file_descriptors(
        dir,
        [crate::GraphFileDeltaDescriptor::Sealed {
            relative_path: std::path::PathBuf::from("topology/runtime_entity_label_encoding.json"),
            revision_uuid: uuid::Uuid::new_v4(),
        }],
    )?;
    std::fs::rename(&tmp, &path).map_err(|e| storage_err(e.to_string()))?;
    Ok(())
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
        .map(|(id, _)| id.0)
        .filter(|id| ontology_ids.contains(id))
        .collect()
}

fn migratable_raw_ids(
    runtime_catalog: &RuntimeCatalog,
    ontology_ids: &HashSet<u32>,
) -> HashMap<u32, u32> {
    runtime_catalog
        .entity_type_names_with_ids()
        .map(|(id, _)| id)
        .filter(|id| !ontology_ids.contains(&id.0))
        .map(|id| (id.0, runtime_entity_type_id(id).0))
        .collect()
}

/// Remap tagged runtime entity plan IDs → ontology TypeIds when catalog names match.
///
/// Fail closed when two distinct runtime names would claim the same ontology id
/// (ambiguous identity).
fn adoption_name_remaps(
    ontology: Option<&OntologyHandle>,
    runtime_catalog: &RuntimeCatalog,
) -> Result<HashMap<u32, u32>, GfError> {
    let Some(handle) = ontology else {
        return Ok(HashMap::new());
    };
    let mut remap = HashMap::new();
    let mut ontology_targets: HashMap<u32, String> = HashMap::new();
    for (runtime_id, name) in runtime_catalog.entity_type_names_with_ids() {
        let Some(TypeId(ontology_id)) = handle.entity_type_id(name) else {
            continue;
        };
        let tagged = runtime_entity_type_id(runtime_id).0;
        if let Some(prior) = ontology_targets.get(&ontology_id)
            && prior != name
        {
            return Err(storage_err(format!(
                "ambiguous runtime→ontology entity label remapping: \
                 {prior:?} and {name:?} both claim ontology type id {ontology_id}"
            )));
        }
        ontology_targets.insert(ontology_id, name.to_owned());
        if tagged != ontology_id {
            remap.insert(tagged, ontology_id);
        }
    }
    Ok(remap)
}

fn collect_label_hits(
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
                if candidates.contains(&value) {
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
                if candidates.contains(&value) && !is_runtime_entity_type_id(TypeId(value)) {
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
                if candidates.contains(&value) && !is_runtime_entity_type_id(TypeId(value)) {
                    hits.insert(value);
                }
            }
        }
    }
    Ok(hits)
}

fn remap_label_value(value: u32, remap: &HashMap<u32, u32>) -> (u32, bool) {
    match remap.get(&value) {
        Some(&mapped) if mapped != value => (mapped, true),
        _ => (value, false),
    }
}

fn remap_batches(
    batches: Vec<RecordBatch>,
    remap: &HashMap<u32, u32>,
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
            let (primary_value, changed_primary) = remap_label_value(primary.value(row), remap);
            if changed_primary {
                remapped += 1;
            }
            primary_builder.append_value(primary_value);

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
                let (value, changed) = remap_label_value(values.value(index), remap);
                if changed {
                    remapped += 1;
                }
                list_builder.values().append_value(value);
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
    legacy: HashMap<u32, u32>,
    adoption: HashMap<u32, u32>,
) -> Result<HashMap<u32, u32>, GfError> {
    let mut remap = legacy;
    for (from, to) in adoption {
        if let Some(existing) = remap.get(&from)
            && *existing != to
        {
            return Err(storage_err(format!(
                "ambiguous runtime entity label remap for id {from}: {existing} vs {to}"
            )));
        }
        remap.insert(from, to);
    }
    Ok(remap)
}

fn read_topology_batches(dir: &Path) -> Result<Vec<RecordBatch>, GfError> {
    normalize_topology_nodes(read_nodes(dir).map_err(pq_err)?).map_err(pq_err)
}

fn reconcile_inner(
    dir: &Path,
    ontology: Option<&OntologyHandle>,
    runtime_catalog: &RuntimeCatalog,
    rewrite: bool,
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
    if collisions.is_empty() && remap.is_empty() {
        if rewrite && !marked {
            write_runtime_entity_label_encoding_marker(dir)?;
        }
        return Ok(RuntimeEntityLabelReconcile {
            remapped_label_values: 0,
            colliding_raw_ids: 0,
            encoding_marked: marked || rewrite,
        });
    }

    let batches = read_topology_batches(dir)?;

    if !marked {
        let collision_hits = collect_untagged_label_hits(&batches, &collisions)?;
        if !collision_hits.is_empty() {
            let mut ids = collision_hits.into_iter().collect::<Vec<_>>();
            ids.sort_unstable();
            return Err(storage_err(format!(
                "runtime entity label ID collision with ontology type IDs; \
                 cannot safely migrate untagged node labels for raw ids {ids:?} \
                 (identity domains must remain disjoint)"
            )));
        }
    }

    let mut remapped_label_values = 0u64;
    if !remap.is_empty() {
        let candidate_keys = remap.keys().copied().collect::<HashSet<_>>();
        let needs_migration = !collect_label_hits(&batches, &candidate_keys)?.is_empty();
        if needs_migration {
            if !rewrite {
                return Err(storage_err(
                    "runtime entity label IDs require writable reconciliation; \
                     read-only open cannot rewrite topology",
                ));
            }
            let mut staged = RewriteBatch::new();
            for path in node_parquet_files(dir)? {
                let source =
                    crate::catalog::read_parquet_or_empty(&path, TOPOLOGY_NODES_SCHEMA.clone())
                        .map_err(pq_err)?;
                let source = normalize_topology_nodes(source).map_err(pq_err)?;
                let (rewritten, remapped) = remap_batches(source, &remap)?;
                if remapped > 0 {
                    let merged = arrow::compute::concat_batches(&TOPOLOGY_NODES_SCHEMA, &rewritten)
                        .map_err(|e| storage_err(e.to_string()))?;
                    staged.restage(&path, TOPOLOGY_NODES_SCHEMA.clone(), &merged)?;
                    remapped_label_values += remapped;
                }
            }
            if remapped_label_values > 0 {
                commit_topology_aware(staged, dir)?;
            }
        }
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
    reconcile_inner(dir, ontology, runtime_catalog, true)
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
    reconcile_inner(dir, ontology, runtime_catalog, false)
}

/// Pure helper: tagged runtime entity plan IDs stay disjoint from ontology IDs.
#[must_use]
pub fn runtime_entity_plan_id_is_disjoint_from_ontology(
    runtime_id: RuntimeTypeId,
    ontology_id: TypeId,
) -> bool {
    runtime_entity_type_id(runtime_id) != ontology_id
}

#[cfg(test)]
mod tests {
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
        let node_ids = UInt64Array::from((0..n as u64).collect::<Vec<_>>());
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
            RuntimeTypeId(0),
            TypeId(0)
        ));
        assert_eq!(
            runtime_entity_type_id(RuntimeTypeId(0)).0,
            graphforge_ir::RUNTIME_ENTITY_TYPE_TAG
        );
    }

    #[test]
    fn exploratory_legacy_ids_migrate_to_tagged_form() {
        let dir = TempDir::new().unwrap();
        write_nodes(dir.path(), &[&[0], &[1]]);
        let mut catalog = RuntimeCatalog::new();
        assert_eq!(catalog.intern_label("Ghost").0, 0);
        assert_eq!(catalog.intern_label("Spectre").0, 1);

        let outcome =
            reconcile_runtime_entity_label_ids(dir.path(), None, &catalog).expect("migrate");
        assert_eq!(outcome.remapped_label_values, 4);
        assert!(outcome.encoding_marked);
        assert!(has_runtime_entity_label_encoding_marker(dir.path()));

        let batches = read_nodes(dir.path()).unwrap();
        let type_ids = batches[0]
            .column_by_name("type_ids")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let row0 = type_ids.value(0);
        let row0 = row0.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(row0.value(0), runtime_entity_type_id(RuntimeTypeId(0)).0);
    }

    #[test]
    fn unmarked_colliding_legacy_ids_fail_closed() {
        let dir = TempDir::new().unwrap();
        write_nodes(dir.path(), &[&[0]]);
        let mut catalog = RuntimeCatalog::new();
        catalog.intern_label("Ghost");
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
        let ghost = runtime_entity_type_id(RuntimeTypeId(0)).0;
        write_nodes(dir.path(), &[&[0], &[ghost]]);
        write_runtime_entity_label_encoding_marker(dir.path()).unwrap();
        let mut catalog = RuntimeCatalog::new();
        catalog.intern_label("Ghost");
        let handle = person_ontology();

        let outcome = reconcile_runtime_entity_label_ids(dir.path(), Some(&handle), &catalog)
            .expect("marked project must open");
        assert_eq!(outcome.remapped_label_values, 0);
        assert_eq!(outcome.colliding_raw_ids, 1);
    }

    #[test]
    fn marked_project_without_ontology_does_not_remap_untagged_zero() {
        let dir = TempDir::new().unwrap();
        let ghost = runtime_entity_type_id(RuntimeTypeId(0)).0;
        write_nodes(dir.path(), &[&[0], &[ghost]]);
        write_runtime_entity_label_encoding_marker(dir.path()).unwrap();
        let mut catalog = RuntimeCatalog::new();
        catalog.intern_label("Ghost");

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
        let person_runtime = catalog.intern_label("Person");
        let ghost_runtime = catalog.intern_label("Ghost");
        let person_tagged = runtime_entity_type_id(person_runtime).0;
        let ghost_tagged = runtime_entity_type_id(ghost_runtime).0;
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
        let person_runtime = catalog.intern_label("Person");
        let person_tagged = runtime_entity_type_id(person_runtime).0;
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
