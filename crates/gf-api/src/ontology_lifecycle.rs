//! Deterministic, non-mutating ontology inspection, suggestion, validation, and export.

use std::path::Path;
use std::{fmt::Write as _, io::Write as _};

use arrow::array::{Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use gf_core::{ApiErrorCode, GfError};
use gf_ontology::{
    ConstraintKind, EntityTypeDef, OntologyDoc, OntologyValidationError, OntologyValidator,
    PropertyDef, PropertyValueType,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::GraphForge;

/// Stable kind of one observed runtime-catalog entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogEntryKind {
    /// Observed node label.
    EntityType,
    /// Observed edge type. Endpoints are not inferred by the runtime catalog.
    RelationType,
    /// Observed property name and optional owning label.
    Property,
}

/// One portable structural observation. Runtime IDs and timestamps are deliberately absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCatalogEntry {
    /// Observation category.
    pub kind: CatalogEntryKind,
    /// Observed label, edge type, or property name.
    pub name: String,
    /// Owning label for a property, otherwise `None`.
    pub owner: Option<String>,
    /// Number of observations recorded by this catalog.
    pub observation_count: u64,
}

/// Immutable, deterministically ordered public runtime-catalog view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCatalogSnapshot {
    /// Contract version for forward-compatible consumers.
    pub contract_version: u32,
    /// Entries ordered by kind, owner, and name.
    pub entries: Vec<RuntimeCatalogEntry>,
}

/// Caller-owned stable identity for a generated draft.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OntologySuggestionOptions {
    /// Stable ontology identifier to place in the draft.
    pub ontology_id: String,
    /// Version to place in the draft.
    pub version: String,
}

/// Explicitly non-authoritative ontology draft.
#[derive(Debug, Clone, PartialEq)]
pub struct OntologySuggestion {
    /// Always true; callers must explicitly load or adopt the document.
    pub draft: bool,
    /// Canonically ordered ontology document.
    pub document: OntologyDoc,
    /// SHA-256 of canonical JSON bytes.
    pub fingerprint_sha256: String,
    /// Observed relation types omitted because the catalog has no endpoint evidence.
    pub omitted_relation_types: Vec<String>,
}

/// Structured result of non-mutating semantic validation.
#[derive(Debug, Clone, PartialEq)]
pub struct OntologyValidationReport {
    /// Whether the document is valid.
    pub valid: bool,
    /// Every semantic diagnostic, in validator order.
    pub diagnostics: Vec<OntologyValidationError>,
}

/// Serialization format for an ontology-only export.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OntologyExportFormat {
    /// YAML document.
    Yaml,
    /// Pretty JSON document.
    Json,
}

/// Explicit authority source for ontology export.
#[derive(Debug, Clone, PartialEq)]
pub enum OntologyExportSource {
    /// A caller-reviewed or freshly generated draft. Its declaration vectors
    /// are canonicalized before validation, fingerprinting, and serialization.
    Suggested(OntologyDoc),
    /// The ontology currently loaded in this live facade.
    Loaded,
    /// The ontology adopted in the committed workspace generation.
    Adopted,
}

impl GraphForge {
    /// Return an immutable, portable snapshot of structural observations.
    ///
    /// Runtime IDs and first/last-seen timestamps are implementation state and
    /// intentionally excluded from this stable product contract.
    pub fn inspect_runtime_catalog(&self) -> Result<RuntimeCatalogSnapshot, GfError> {
        let batch = self
            .runtime_catalog
            .lock()
            .map_err(|_| GfError::Storage("runtime catalog lock poisoned".into()))?
            .to_record_batch();
        snapshot_from_batch(&batch)
    }

    /// Suggest a conservative ontology draft from structural observations.
    ///
    /// Properties are nullable UTF-8 because the runtime catalog records no
    /// values or value types. Relations are reported as omitted because it
    /// records no source/destination evidence. No constraints or semantics are guessed.
    pub fn suggest_ontology(
        &self,
        options: OntologySuggestionOptions,
    ) -> Result<OntologySuggestion, GfError> {
        if options.ontology_id.trim().is_empty() || options.version.trim().is_empty() {
            return Err(GfError::Validation(
                "ontology_id and version must be non-empty".into(),
            ));
        }
        let snapshot = self.inspect_runtime_catalog()?;
        let entity_names = snapshot
            .entries
            .iter()
            .filter(|entry| entry.kind == CatalogEntryKind::EntityType)
            .map(|entry| entry.name.clone())
            .collect::<std::collections::BTreeSet<_>>();
        let entity_types = entity_names
            .iter()
            .map(|name| EntityTypeDef {
                name: name.clone(),
                r#abstract: false,
                parent: None,
            })
            .collect();
        let properties = snapshot
            .entries
            .iter()
            .filter_map(|entry| {
                let owner = entry.owner.as_ref()?;
                (entry.kind == CatalogEntryKind::Property && entity_names.contains(owner)).then(
                    || PropertyDef {
                        owner: owner.clone(),
                        name: entry.name.clone(),
                        value_type: PropertyValueType::Utf8,
                        nullable: true,
                        multivalued: false,
                        default_json: None,
                    },
                )
            })
            .collect();
        let omitted_relation_types = snapshot
            .entries
            .iter()
            .filter(|entry| entry.kind == CatalogEntryKind::RelationType)
            .map(|entry| entry.name.clone())
            .collect();
        let document = canonicalize_document(OntologyDoc {
            ontology_id: options.ontology_id,
            version: options.version,
            entity_types,
            relation_types: Vec::new(),
            properties,
            constraints: Vec::new(),
            migrations: Vec::new(),
        });
        let report = self.validate_ontology(&document);
        if !report.valid {
            return Err(GfError::Ontology(format!(
                "suggested ontology failed validation with {} diagnostic(s)",
                report.diagnostics.len()
            )));
        }
        let canonical = serde_json::to_vec(&document)
            .map_err(|error| GfError::Ontology(format!("failed to encode suggestion: {error}")))?;
        Ok(OntologySuggestion {
            draft: true,
            document,
            fingerprint_sha256: hex_sha256(&canonical),
            omitted_relation_types,
        })
    }

    /// Validate an ontology document without changing live or durable state.
    #[must_use]
    pub fn validate_ontology(&self, document: &OntologyDoc) -> OntologyValidationReport {
        match OntologyValidator::validate(document) {
            Ok(()) => OntologyValidationReport {
                valid: true,
                diagnostics: Vec::new(),
            },
            Err(diagnostics) => OntologyValidationReport {
                valid: false,
                diagnostics,
            },
        }
    }

    /// Atomically export one explicit ontology source as YAML or JSON.
    ///
    /// All declaration vectors are canonicalized first. Validation and
    /// serialization complete before the destination is replaced.
    pub fn export_ontology(
        &self,
        source: OntologyExportSource,
        destination: &Path,
        format: OntologyExportFormat,
    ) -> Result<(), GfError> {
        let document = match source {
            OntologyExportSource::Suggested(document) => document,
            OntologyExportSource::Loaded => self
                .ontology_document
                .clone()
                .ok_or_else(|| GfError::Ontology("no ontology is loaded in this facade".into()))?,
            OntologyExportSource::Adopted => {
                let record = self.workspace_ontology()?;
                let value = record.canonical_ontology.ok_or_else(|| {
                    GfError::Ontology("the current workspace has no adopted ontology".into())
                })?;
                serde_json::from_value(value).map_err(|error| {
                    GfError::Ontology(format!("invalid adopted ontology: {error}"))
                })?
            }
        };
        let document = canonicalize_document(document);
        let report = self.validate_ontology(&document);
        if !report.valid {
            return Err(GfError::Ontology(format!(
                "ontology export rejected {} validation diagnostic(s)",
                report.diagnostics.len()
            )));
        }
        let bytes = match format {
            OntologyExportFormat::Yaml => serde_yaml::to_string(&document)
                .map(String::into_bytes)
                .map_err(|error| GfError::Ontology(format!("failed to encode YAML: {error}")))?,
            OntologyExportFormat::Json => serde_json::to_vec_pretty(&document)
                .map_err(|error| GfError::Ontology(format!("failed to encode JSON: {error}")))?,
        };
        atomic_replace(destination, &bytes)
    }
}

fn snapshot_from_batch(batch: &RecordBatch) -> Result<RuntimeCatalogSnapshot, GfError> {
    let schema_error = |message: String| GfError::Api {
        code: ApiErrorCode::SchemaMismatch,
        message,
    };
    let column = |index: usize, name: &str| {
        batch
            .columns()
            .get(index)
            .ok_or_else(|| schema_error(format!("runtime catalog is missing column '{name}'")))
    };
    let kinds = column(0, "entry_kind")?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| schema_error("runtime catalog entry_kind is not Utf8".into()))?;
    let names = column(1, "name")?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| schema_error("runtime catalog name is not Utf8".into()))?;
    let counts = column(3, "observation_count")?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| schema_error("runtime catalog observation_count is not UInt64".into()))?;
    let owners = column(6, "owner_label")?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| schema_error("runtime catalog owner_label is not Utf8".into()))?;
    let mut entries = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if kinds.is_null(row) || names.is_null(row) || counts.is_null(row) {
            return Err(schema_error(format!(
                "runtime catalog row {row} has a null required field"
            )));
        }
        entries.push(RuntimeCatalogEntry {
            kind: match kinds.value(row) {
                "entity_type" => CatalogEntryKind::EntityType,
                "relation_type" => CatalogEntryKind::RelationType,
                "property" => CatalogEntryKind::Property,
                other => {
                    return Err(schema_error(format!(
                        "runtime catalog row {row} has unknown entry kind '{other}'"
                    )));
                }
            },
            name: names.value(row).to_owned(),
            owner: (!owners.is_null(row)).then(|| owners.value(row).to_owned()),
            observation_count: counts.value(row),
        });
    }
    entries.sort_by(|left, right| {
        (&left.kind, &left.owner, &left.name).cmp(&(&right.kind, &right.owner, &right.name))
    });
    Ok(RuntimeCatalogSnapshot {
        contract_version: 1,
        entries,
    })
}

fn canonicalize_document(mut document: OntologyDoc) -> OntologyDoc {
    document
        .entity_types
        .sort_by(|left, right| left.name.cmp(&right.name));
    document.relation_types.sort_by(|left, right| {
        (&left.name, &left.src, &left.dst).cmp(&(&right.name, &right.src, &right.dst))
    });
    document
        .properties
        .sort_by(|left, right| (&left.owner, &left.name).cmp(&(&right.owner, &right.name)));
    document.constraints.sort_by(|left, right| {
        (&left.owner, constraint_rank(&left.kind), &left.expr_json).cmp(&(
            &right.owner,
            constraint_rank(&right.kind),
            &right.expr_json,
        ))
    });
    document.migrations.sort_by(|left, right| {
        (
            &left.from_version,
            &left.to_version,
            &left.transform_kind,
            &left.script_ref,
            &left.checksum,
        )
            .cmp(&(
                &right.from_version,
                &right.to_version,
                &right.transform_kind,
                &right.script_ref,
                &right.checksum,
            ))
    });
    document
}

const fn constraint_rank(kind: &ConstraintKind) -> u8 {
    match kind {
        ConstraintKind::UniqueProperty => 0,
        ConstraintKind::RequiredProperty => 1,
        ConstraintKind::RangeCheck => 2,
        ConstraintKind::CustomExpr => 3,
    }
}

fn atomic_replace(destination: &Path, bytes: &[u8]) -> Result<(), GfError> {
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
        GfError::Storage(format!("create ontology export temporary file: {error}"))
    })?;
    temporary
        .write_all(bytes)
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|error| GfError::Storage(format!("write ontology export: {error}")))?;
    temporary.persist(destination).map_err(|error| {
        GfError::Storage(format!(
            "replace ontology export {}: {error}",
            destination.display()
        ))
    })?;
    Ok(())
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AdoptOntologyRequest, OperationId, WriteContext};
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use gf_core::OntologyMode;
    use std::sync::Arc;

    fn observations(first_order: bool) -> GraphForge {
        let graph = GraphForge::new(None).unwrap();
        let catalog = graph.runtime_catalog();
        let mut catalog = catalog.lock().unwrap();
        if first_order {
            catalog.intern_label("Person");
            catalog.intern_label("Organization");
        } else {
            catalog.intern_label("Organization");
            catalog.intern_label("Person");
        }
        catalog.intern_property("name", Some("Person"));
        catalog.intern_relation_type("WORKS_AT");
        drop(catalog);
        graph
    }

    #[test]
    fn equivalent_observation_order_produces_identical_suggestion() {
        let options = || OntologySuggestionOptions {
            ontology_id: "draft".into(),
            version: "0.1.0".into(),
        };
        let left_graph = observations(true);
        let right_graph = observations(false);
        let left = left_graph.suggest_ontology(options()).unwrap();
        let right = right_graph.suggest_ontology(options()).unwrap();
        assert_eq!(left, right);
        assert_eq!(left.omitted_relation_types, vec!["WORKS_AT"]);
        assert!(left.document.relation_types.is_empty());
        assert!(observations(true).validate_ontology(&left.document).valid);

        let directory = tempfile::tempdir().unwrap();
        for (format, extension) in [
            (OntologyExportFormat::Yaml, "yaml"),
            (OntologyExportFormat::Json, "json"),
        ] {
            let left_path = directory.path().join(format!("left.{extension}"));
            let right_path = directory.path().join(format!("right.{extension}"));
            left_graph
                .export_ontology(
                    OntologyExportSource::Suggested(left.document.clone()),
                    &left_path,
                    format,
                )
                .unwrap();
            right_graph
                .export_ontology(
                    OntologyExportSource::Suggested(right.document.clone()),
                    &right_path,
                    format,
                )
                .unwrap();
            assert_eq!(
                std::fs::read(left_path).unwrap(),
                std::fs::read(right_path).unwrap()
            );
        }
    }

    #[test]
    fn snapshot_hides_runtime_identity_and_orders_entries() {
        let snapshot = observations(false).inspect_runtime_catalog().unwrap();
        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(!json.contains("runtime_id"));
        assert!(!json.contains("first_seen"));
        assert_eq!(snapshot.entries[0].name, "Organization");
        assert_eq!(snapshot.entries[1].name, "Person");
    }

    #[test]
    fn catalog_inspection_reports_poisoned_lock_and_schema_mismatch() {
        let graph = GraphForge::new(None).unwrap();
        let catalog = graph.runtime_catalog();
        let poison = std::thread::spawn(move || {
            let _guard = catalog.lock().unwrap();
            panic!("poison runtime catalog for error-path test");
        });
        assert!(poison.join().is_err());
        assert!(matches!(
            graph.inspect_runtime_catalog(),
            Err(GfError::Storage(message)) if message == "runtime catalog lock poisoned"
        ));

        let schema = Arc::new(Schema::new(vec![
            Field::new("entry_kind", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("runtime_id", DataType::UInt32, false),
            Field::new("observation_count", DataType::UInt64, false),
            Field::new("first_seen", DataType::Int64, false),
            Field::new("last_seen", DataType::Int64, false),
            Field::new("owner_label", DataType::Utf8, true),
        ]));
        let empty_strings = || Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
        let malformed = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                empty_strings(),
                Arc::new(arrow::array::UInt32Array::from(Vec::<u32>::new())),
                Arc::new(UInt64Array::from(Vec::<u64>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                empty_strings(),
            ],
        )
        .unwrap();
        assert!(matches!(
            snapshot_from_batch(&malformed),
            Err(GfError::Api {
                code: ApiErrorCode::SchemaMismatch,
                ..
            })
        ));
    }

    #[test]
    fn yaml_and_json_exports_are_atomic_valid_documents() {
        let graph = observations(true);
        let suggestion = graph
            .suggest_ontology(OntologySuggestionOptions {
                ontology_id: "draft".into(),
                version: "0.1.0".into(),
            })
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        for (format, name) in [
            (OntologyExportFormat::Yaml, "ontology.yaml"),
            (OntologyExportFormat::Json, "ontology.json"),
        ] {
            let path = dir.path().join(name);
            graph
                .export_ontology(
                    OntologyExportSource::Suggested(suggestion.document.clone()),
                    &path,
                    format,
                )
                .unwrap();
            assert_eq!(
                gf_ontology::OntologyLoader::load_file(&path).unwrap(),
                suggestion.document
            );
        }
    }

    #[test]
    fn suggested_source_is_canonicalized_before_serialization() {
        let graph = GraphForge::new(None).unwrap();
        let ordered = OntologyDoc {
            ontology_id: "draft".into(),
            version: "1".into(),
            entity_types: vec![
                EntityTypeDef {
                    name: "Organization".into(),
                    r#abstract: false,
                    parent: None,
                },
                EntityTypeDef {
                    name: "Person".into(),
                    r#abstract: false,
                    parent: None,
                },
            ],
            relation_types: vec![],
            properties: vec![],
            constraints: vec![],
            migrations: vec![],
        };
        let mut reversed = ordered.clone();
        reversed.entity_types.reverse();
        let directory = tempfile::tempdir().unwrap();
        for (format, extension) in [
            (OntologyExportFormat::Yaml, "yaml"),
            (OntologyExportFormat::Json, "json"),
        ] {
            let ordered_path = directory.path().join(format!("ordered.{extension}"));
            let reversed_path = directory.path().join(format!("reversed.{extension}"));
            graph
                .export_ontology(
                    OntologyExportSource::Suggested(ordered.clone()),
                    &ordered_path,
                    format,
                )
                .unwrap();
            graph
                .export_ontology(
                    OntologyExportSource::Suggested(reversed.clone()),
                    &reversed_path,
                    format,
                )
                .unwrap();
            assert_eq!(
                std::fs::read(ordered_path).unwrap(),
                std::fs::read(reversed_path).unwrap()
            );
        }
    }

    #[test]
    fn validation_and_failed_export_do_not_replace_destination() {
        let graph = GraphForge::new(None).unwrap();
        let invalid = OntologyDoc {
            ontology_id: "bad".into(),
            version: "1".into(),
            entity_types: vec![
                EntityTypeDef {
                    name: "Duplicate".into(),
                    r#abstract: false,
                    parent: None,
                },
                EntityTypeDef {
                    name: "Duplicate".into(),
                    r#abstract: false,
                    parent: None,
                },
            ],
            relation_types: vec![],
            properties: vec![],
            constraints: vec![],
            migrations: vec![],
        };
        assert!(!graph.validate_ontology(&invalid).valid);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ontology.json");
        std::fs::write(&path, b"existing").unwrap();
        assert!(
            graph
                .export_ontology(
                    OntologyExportSource::Suggested(invalid),
                    &path,
                    OntologyExportFormat::Json,
                )
                .is_err()
        );
        assert_eq!(std::fs::read(path).unwrap(), b"existing");
    }

    #[test]
    fn loaded_and_adopted_sources_are_explicit_and_leave_authority_unchanged() {
        let project = tempfile::tempdir().unwrap();
        let files = tempfile::tempdir().unwrap();
        let adopted_input = files.path().join("adopted-input.yaml");
        let loaded_input = files.path().join("loaded-input.yaml");
        std::fs::write(
            &adopted_input,
            "ontology_id: durable\nversion: \"1\"\nentity_types:\n  - name: Person\n",
        )
        .unwrap();
        std::fs::write(
            &loaded_input,
            "ontology_id: session\nversion: \"2\"\nentity_types:\n  - name: Organization\n",
        )
        .unwrap();
        let mut graph = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();

        graph
            .adopt_ontology(AdoptOntologyRequest {
                context: WriteContext {
                    operation_uuid: OperationId(uuid::Uuid::from_u128(236)),
                    actor_uuid: None,
                },
                path: adopted_input,
                mode: OntologyMode::Strict,
            })
            .unwrap();
        let generation = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        graph.load_ontology(loaded_input.to_str().unwrap()).unwrap();
        let loaded = files.path().join("loaded.json");
        graph
            .export_ontology(
                OntologyExportSource::Loaded,
                &loaded,
                OntologyExportFormat::Json,
            )
            .unwrap();
        let adopted = files.path().join("adopted.yaml");
        graph
            .export_ontology(
                OntologyExportSource::Adopted,
                &adopted,
                OntologyExportFormat::Yaml,
            )
            .unwrap();
        assert_eq!(graph.ontology_mode(), OntologyMode::Strict);
        let loaded_doc = gf_ontology::OntologyLoader::load_file(&loaded).unwrap();
        let adopted_doc = gf_ontology::OntologyLoader::load_file(&adopted).unwrap();
        assert_eq!(loaded_doc.ontology_id, "session");
        assert_eq!(adopted_doc.ontology_id, "durable");
        assert_ne!(loaded_doc, adopted_doc);
        assert_eq!(
            graph
                .workspace_ontology()
                .unwrap()
                .canonical_ontology
                .unwrap()["ontology_id"],
            "durable"
        );
        assert_eq!(
            *graph
                .current_generation_uuid
                .lock()
                .expect("generation UUID lock poisoned"),
            generation
        );

        drop(graph);
        let reopened = GraphForge::new(Some(project.path().to_str().unwrap())).unwrap();
        assert_eq!(reopened.ontology_mode(), OntologyMode::Strict);
        let reopened_path = files.path().join("reopened.json");
        reopened
            .export_ontology(
                OntologyExportSource::Loaded,
                &reopened_path,
                OntologyExportFormat::Json,
            )
            .unwrap();
        assert_eq!(
            gf_ontology::OntologyLoader::load_file(&reopened_path).unwrap(),
            adopted_doc
        );
    }

    #[test]
    fn filesystem_failure_preserves_destination_and_cleans_temporary_file() {
        let graph = observations(true);
        let suggestion = graph
            .suggest_ontology(OntologySuggestionOptions {
                ontology_id: "draft".into(),
                version: "1".into(),
            })
            .unwrap();
        let parent = tempfile::tempdir().unwrap();
        let destination = parent.path().join("destination-is-a-directory");
        std::fs::create_dir(&destination).unwrap();
        let before = std::fs::read_dir(parent.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();

        assert!(matches!(
            graph.export_ontology(
                OntologyExportSource::Suggested(suggestion.document),
                &destination,
                OntologyExportFormat::Json,
            ),
            Err(GfError::Storage(_))
        ));
        assert!(destination.is_dir());
        let after = std::fs::read_dir(parent.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(
            before, after,
            "failed persist must clean its sibling temp file"
        );

        let missing_parent = parent.path().join("missing").join("ontology.json");
        assert!(matches!(
            graph.export_ontology(
                OntologyExportSource::Suggested(
                    graph
                        .suggest_ontology(OntologySuggestionOptions {
                            ontology_id: "draft".into(),
                            version: "1".into(),
                        })
                        .unwrap()
                        .document,
                ),
                &missing_parent,
                OntologyExportFormat::Json,
            ),
            Err(GfError::Storage(_))
        ));
        assert!(!missing_parent.exists());
    }
}
