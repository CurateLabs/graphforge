//! Parquet persistence for [`OntologyRuntime`].
//!
//! Persisting the compiled Arrow tables avoids re-parsing and re-validating
//! the ontology YAML/JSON on every process restart.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray, UInt32Array};
use arrow::compute::concat_batches;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;

use crate::compiler::{OntologyRuntime, PropertyOwnerKind};
use crate::error::OntologyError;

// ---------------------------------------------------------------------------
// Table manifest
// ---------------------------------------------------------------------------

/// The 8 table filenames written/read in a Parquet snapshot directory.
const TABLE_NAMES: &[&str] = &[
    "ontology_meta",
    "entity_types",
    "relation_types",
    "property_types",
    "type_constraints",
    "semantic_flags",
    "cardinality_rules",
    "aliases",
];

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

fn parquet_err(e: impl std::fmt::Display) -> OntologyError {
    OntologyError::Parquet(e.to_string())
}

fn io_err(e: &std::io::Error) -> OntologyError {
    OntologyError::Parquet(e.to_string())
}

// ---------------------------------------------------------------------------
// save_parquet
// ---------------------------------------------------------------------------

/// Write all eight ontology runtime tables to `dir` as Parquet files.
///
/// Creates `dir` if it does not exist.  Each file carries GraphForge metadata
/// in its Arrow schema key-value metadata.
///
/// # Errors
/// Returns [`OntologyError::Parquet`] on any I/O or Parquet write failure.
pub fn save_parquet(runtime: &OntologyRuntime, dir: &Path) -> Result<(), OntologyError> {
    fs::create_dir_all(dir).map_err(|e| io_err(&e))?;

    // Read identifiers from the ontology_meta table.
    let meta_id = try_string_col(&runtime.ontology_meta, 0, 0, "ontology_meta")?;
    let meta_ver = try_string_col(&runtime.ontology_meta, 1, 0, "ontology_meta")?;
    let meta_checksum = try_string_col(&runtime.ontology_meta, 3, 0, "ontology_meta")?;

    let gf_meta: Vec<(String, String)> = vec![
        ("graphforge.ontology_id".into(), meta_id),
        ("graphforge.ontology_version".into(), meta_ver),
        ("graphforge.ontology_checksum".into(), meta_checksum),
        ("graphforge.writer_version".into(), "0.5.0".into()),
    ];

    let batches = [
        &runtime.ontology_meta,
        &runtime.entity_types,
        &runtime.relation_types,
        &runtime.property_types,
        &runtime.type_constraints,
        &runtime.semantic_flags,
        &runtime.cardinality_rules,
        &runtime.aliases,
    ];

    for (&name, &batch) in TABLE_NAMES.iter().zip(batches.iter()) {
        let path = dir.join(format!("{name}.parquet"));
        write_batch_parquet(&path, batch, &gf_meta)?;
    }

    Ok(())
}

fn write_batch_parquet(
    path: &Path,
    batch: &RecordBatch,
    gf_meta: &[(String, String)],
) -> Result<(), OntologyError> {
    // Embed GraphForge metadata into the Arrow schema.
    let meta_map: HashMap<String, String> = gf_meta.iter().cloned().collect();
    let schema_with_meta = batch.schema().as_ref().clone().with_metadata(meta_map);

    let file = File::create(path).map_err(|e| io_err(&e))?;
    let props = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(file, Arc::new(schema_with_meta), Some(props)).map_err(parquet_err)?;
    writer.write(batch).map_err(parquet_err)?;
    writer.close().map_err(parquet_err)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// load_parquet
// ---------------------------------------------------------------------------

/// Load an [`OntologyRuntime`] from a Parquet snapshot directory.
///
/// All lookup maps and the inheritance closure are reconstructed from the
/// loaded Arrow tables — no YAML/JSON parsing or validation is performed.
///
/// If `expected_checksum` is `Some`, the stored `graphforge.ontology_checksum`
/// metadata value from `ontology_meta.parquet` is compared against it.  A
/// mismatch returns [`OntologyError::ChecksumMismatch`], signalling that the
/// snapshot was compiled from a different version of the ontology source and
/// should be discarded so the caller can recompile from YAML/JSON.
///
/// # Errors
/// - [`OntologyError::Parquet`] if any file is missing or malformed.
/// - [`OntologyError::ChecksumMismatch`] if `expected_checksum` is `Some` and
///   the stored checksum differs.
pub fn load_parquet(
    dir: &Path,
    expected_checksum: Option<&str>,
) -> Result<OntologyRuntime, OntologyError> {
    let [
        ontology_meta,
        entity_types,
        relation_types,
        property_types,
        type_constraints,
        semantic_flags,
        cardinality_rules,
        aliases,
    ] = TABLE_NAMES
        .iter()
        .map(|&name| read_batch_parquet(&dir.join(format!("{name}.parquet"))))
        .collect::<Result<Vec<_>, _>>()?
        .try_into()
        .map_err(|_| OntologyError::Parquet("unexpected table count".into()))?;

    // Verify checksum if the caller supplied one.
    if let Some(expected) = expected_checksum {
        let schema_meta = ontology_meta.schema().metadata().clone();
        let stored = schema_meta
            .get("graphforge.ontology_checksum")
            .map_or("", String::as_str);
        if stored != expected {
            return Err(OntologyError::ChecksumMismatch {
                cached: stored.to_owned(),
                computed: expected.to_owned(),
            });
        }
    }

    let (entity_name_to_id, entity_id_to_name) = rebuild_entity_maps(&entity_types)?;
    let (relation_name_to_id, relation_id_to_name) = rebuild_relation_maps(&relation_types)?;
    let property_name_to_id = rebuild_property_map(&property_types)?;
    let (ancestors, descendants) = rebuild_inheritance_closure(&entity_types, &entity_name_to_id)?;

    Ok(OntologyRuntime {
        ontology_meta,
        entity_types,
        relation_types,
        property_types,
        type_constraints,
        semantic_flags,
        cardinality_rules,
        aliases,
        entity_name_to_id,
        entity_id_to_name,
        relation_name_to_id,
        relation_id_to_name,
        ancestors,
        descendants,
        property_name_to_id,
    })
}

// ---------------------------------------------------------------------------
// Lookup-map reconstruction helpers
// ---------------------------------------------------------------------------

type NameIdMaps = (HashMap<String, u32>, HashMap<u32, String>);
type ClosureMaps = (HashMap<u32, HashSet<u32>>, HashMap<u32, HashSet<u32>>);
type PropertyMap = HashMap<(PropertyOwnerKind, u32, String), u32>;

/// Rebuild entity name ↔ id maps from the `entity_types` batch (cols 0=id, 1=name).
fn rebuild_entity_maps(batch: &RecordBatch) -> Result<NameIdMaps, OntologyError> {
    let mut n2i = HashMap::new();
    let mut i2n = HashMap::new();
    if batch.num_rows() > 0 {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| OntologyError::Parquet("entity_types col 0 is not UInt32".into()))?;
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| OntologyError::Parquet("entity_types col 1 is not Utf8".into()))?;
        for r in 0..batch.num_rows() {
            let id = ids.value(r);
            let name = names.value(r).to_owned();
            n2i.insert(name.clone(), id);
            i2n.insert(id, name);
        }
    }
    Ok((n2i, i2n))
}

/// Rebuild relation name ↔ id maps from the `relation_types` batch (cols 0=id, 1=name).
fn rebuild_relation_maps(batch: &RecordBatch) -> Result<NameIdMaps, OntologyError> {
    let mut n2i = HashMap::new();
    let mut i2n = HashMap::new();
    if batch.num_rows() > 0 {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| OntologyError::Parquet("relation_types col 0 is not UInt32".into()))?;
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| OntologyError::Parquet("relation_types col 1 is not Utf8".into()))?;
        for r in 0..batch.num_rows() {
            let id = ids.value(r);
            let name = names.value(r).to_owned();
            n2i.insert(name.clone(), id);
            i2n.insert(id, name);
        }
    }
    Ok((n2i, i2n))
}

/// Rebuild property lookup map from the `property_types` batch
/// (cols 0=property_type_id, 1=owner_kind, 2=owner_type_id, 3=name).
fn rebuild_property_map(batch: &RecordBatch) -> Result<PropertyMap, OntologyError> {
    let mut map = HashMap::new();
    if batch.num_rows() > 0 {
        let prop_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| OntologyError::Parquet("property_types col 0 not UInt32".into()))?;
        let owner_ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| OntologyError::Parquet("property_types col 2 not UInt32".into()))?;
        let owner_kinds = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| OntologyError::Parquet("property_types col 1 not Utf8".into()))?;
        let names = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| OntologyError::Parquet("property_types col 3 not Utf8".into()))?;
        for r in 0..batch.num_rows() {
            let owner_kind = match owner_kinds.value(r) {
                "entity" => PropertyOwnerKind::Entity,
                "relation" => PropertyOwnerKind::Relation,
                "unknown" => continue,
                other => {
                    return Err(OntologyError::Parquet(format!(
                        "property_types col 1 has unknown owner kind {other:?}"
                    )));
                }
            };
            map.insert(
                (owner_kind, owner_ids.value(r), names.value(r).to_owned()),
                prop_ids.value(r),
            );
        }
    }
    Ok(map)
}

/// Rebuild the inheritance ancestor/descendant closure from the `entity_types` batch.
fn rebuild_inheritance_closure(
    batch: &RecordBatch,
    entity_name_to_id: &HashMap<String, u32>,
) -> Result<ClosureMaps, OntologyError> {
    let mut anc: HashMap<u32, HashSet<u32>> = HashMap::new();
    let mut desc: HashMap<u32, HashSet<u32>> = HashMap::new();
    for &id in entity_name_to_id.values() {
        anc.insert(id, HashSet::new());
        desc.insert(id, HashSet::new());
    }
    if batch.num_rows() > 0 {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| OntologyError::Parquet("entity_types col 0 not UInt32".into()))?;
        let parent_ids = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| OntologyError::Parquet("entity_types col 3 not UInt32".into()))?;
        let parent_of: HashMap<u32, u32> = (0..batch.num_rows())
            .filter(|&r| !parent_ids.is_null(r))
            .map(|r| (ids.value(r), parent_ids.value(r)))
            .collect();
        let all_ids: Vec<u32> = entity_name_to_id.values().copied().collect();
        for id in &all_ids {
            let mut current = *id;
            while let Some(&parent) = parent_of.get(&current) {
                if anc[id].contains(&parent) {
                    break;
                }
                anc.get_mut(id).unwrap().insert(parent);
                desc.get_mut(&parent).unwrap().insert(*id);
                current = parent;
            }
        }
    }
    Ok((anc, desc))
}

fn read_batch_parquet(path: &Path) -> Result<RecordBatch, OntologyError> {
    let file = File::open(path).map_err(|e| io_err(&e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(parquet_err)?;
    let schema = builder.schema().clone();
    let reader = builder.build().map_err(parquet_err)?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().map_err(parquet_err)?;

    if batches.is_empty() {
        // Zero-row table — return an empty RecordBatch preserving the schema.
        return Ok(RecordBatch::new_empty(schema));
    }

    // Concatenate using the builder schema so Arrow key-value metadata is preserved.
    // (Individual batch schemas may lose the metadata after Parquet round-trip.)
    concat_batches(&schema, &batches).map_err(|e| OntologyError::Arrow(e.to_string()))
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn try_string_col(
    batch: &RecordBatch,
    col: usize,
    row: usize,
    label: &str,
) -> Result<String, OntologyError> {
    let arr = batch
        .column(col)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OntologyError::Parquet(format!("{label} col {col} is not Utf8")))?;
    if row >= batch.num_rows() {
        return Err(OntologyError::Parquet(format!(
            "{label}: row {row} out of range (have {} rows)",
            batch.num_rows()
        )));
    }
    Ok(arr.value(row).to_owned())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiler::OntologyCompiler;
    use crate::ontology::{
        EntityTypeDef, OntologyDoc, PropertyDef, PropertyValueType, RelationTypeDef, SemanticFlags,
    };

    fn sample_doc() -> OntologyDoc {
        OntologyDoc {
            ontology_id: "test".to_string(),
            version: "1.0".to_string(),
            entity_types: vec![
                EntityTypeDef {
                    name: "Person".to_string(),
                    r#abstract: false,
                    parent: None,
                },
                EntityTypeDef {
                    name: "Employee".to_string(),
                    r#abstract: false,
                    parent: Some("Person".to_string()),
                },
            ],
            relation_types: vec![
                RelationTypeDef {
                    name: "MANAGES".to_string(),
                    src: "Employee".to_string(),
                    dst: "Employee".to_string(),
                    inverse: Some("MANAGED_BY".to_string()),
                    semantic: SemanticFlags {
                        transitive: true,
                        ..Default::default()
                    },
                },
                RelationTypeDef {
                    name: "MANAGED_BY".to_string(),
                    src: "Employee".to_string(),
                    dst: "Employee".to_string(),
                    inverse: Some("MANAGES".to_string()),
                    semantic: SemanticFlags::default(),
                },
            ],
            properties: vec![PropertyDef {
                owner: "Person".to_string(),
                name: "name".to_string(),
                value_type: PropertyValueType::Utf8,
                nullable: false,
                multivalued: false,
                default_json: None,
            }],
            constraints: vec![],
            migrations: vec![],
        }
    }

    #[test]
    fn save_parquet_creates_files() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        save_parquet(&rt, dir.path()).unwrap();
        for name in TABLE_NAMES {
            assert!(
                dir.path().join(format!("{name}.parquet")).exists(),
                "{name}.parquet should exist"
            );
        }
    }

    #[test]
    fn load_parquet_roundtrip_row_counts() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        save_parquet(&rt, dir.path()).unwrap();
        let loaded = load_parquet(dir.path(), None).unwrap();

        assert_eq!(loaded.entity_types.num_rows(), rt.entity_types.num_rows());
        assert_eq!(
            loaded.relation_types.num_rows(),
            rt.relation_types.num_rows()
        );
        assert_eq!(
            loaded.property_types.num_rows(),
            rt.property_types.num_rows()
        );
        assert_eq!(
            loaded.semantic_flags.num_rows(),
            rt.semantic_flags.num_rows()
        );
        assert_eq!(loaded.ontology_meta.num_rows(), 1);
    }

    #[test]
    fn load_parquet_lookup_maps_reconstructed() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        save_parquet(&rt, dir.path()).unwrap();
        let loaded = load_parquet(dir.path(), None).unwrap();

        assert_eq!(
            loaded.entity_name_to_id.get("Person"),
            rt.entity_name_to_id.get("Person")
        );
        assert_eq!(
            loaded.entity_name_to_id.get("Employee"),
            rt.entity_name_to_id.get("Employee")
        );
        assert_eq!(
            loaded.relation_name_to_id.get("MANAGES"),
            rt.relation_name_to_id.get("MANAGES")
        );
    }

    #[test]
    fn load_parquet_ancestors_reconstructed() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        save_parquet(&rt, dir.path()).unwrap();
        let loaded = load_parquet(dir.path(), None).unwrap();

        let emp_id = rt.entity_name_to_id["Employee"];
        let per_id = rt.entity_name_to_id["Person"];
        assert!(
            loaded.ancestors[&emp_id].contains(&per_id),
            "Employee's ancestors should include Person after round-trip"
        );
    }

    #[test]
    fn save_load_full_example() {
        let yaml = r#"
ontology_id: core
version: "2026.05"
entity_types:
  - name: Person
    abstract: false
  - name: Employee
    parent: Person
relation_types:
  - name: MANAGES
    src: Employee
    dst: Employee
    inverse: MANAGED_BY
  - name: MANAGED_BY
    src: Employee
    dst: Employee
    inverse: MANAGES
properties:
  - name: name
    owner: Person
    type: utf8
    nullable: false
constraints:
  - owner: Employee
    kind: unique_property
"#;
        let doc: OntologyDoc = serde_yaml::from_str(yaml).unwrap();
        let rt = OntologyCompiler::compile(&doc).unwrap();
        let dir = tempfile::tempdir().unwrap();
        save_parquet(&rt, dir.path()).unwrap();
        let loaded = load_parquet(dir.path(), None).unwrap();

        assert_eq!(loaded.entity_types.num_rows(), 2);
        assert_eq!(loaded.relation_types.num_rows(), 2);
        assert_eq!(loaded.property_types.num_rows(), 1);
        assert_eq!(loaded.type_constraints.num_rows(), 1);
    }

    #[test]
    fn load_parquet_correct_checksum_passes() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        save_parquet(&rt, dir.path()).unwrap();
        // Use the checksum stored in the runtime (from ontology_meta col 3).
        use arrow::array::StringArray;
        let stored_checksum = rt
            .ontology_meta
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_owned();
        // Loading with the matching checksum should succeed.
        let loaded = load_parquet(dir.path(), Some(&stored_checksum)).unwrap();
        assert_eq!(loaded.entity_types.num_rows(), 2);
    }

    #[test]
    fn load_parquet_wrong_checksum_returns_error() {
        let rt = OntologyCompiler::compile(&sample_doc()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        save_parquet(&rt, dir.path()).unwrap();
        let result = load_parquet(dir.path(), Some("wrong_checksum_value"));
        assert!(
            matches!(
                result,
                Err(crate::error::OntologyError::ChecksumMismatch { .. })
            ),
            "expected ChecksumMismatch error"
        );
    }
}
