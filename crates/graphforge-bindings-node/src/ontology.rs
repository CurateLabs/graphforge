//! Ontology bindings and native task ownership.

use crate::BigInt;
use crate::GfError;
use crate::GraphForge;
use crate::Result;
use crate::WriteContext;
use crate::canonical_operation_id;
use crate::napi;
use crate::optional_uuid;
use crate::to_napi_err;

#[napi(object)]
/// One portable runtime-catalog observation.
pub struct RuntimeCatalogEntryOutput {
    /// Stable observation category.
    pub kind: String,
    /// Observed label, relation type, or property name.
    pub name: String,
    /// Owning entity label for a property.
    pub owner: Option<String>,
    /// Number of observations recorded by the catalog.
    pub observation_count: BigInt,
}

#[napi(object)]
/// Frozen, deterministically ordered runtime-catalog contract.
pub struct RuntimeCatalogSnapshotOutput {
    /// Frozen contract version.
    pub contract_version: u32,
    /// Entries in deterministic kind, owner, and name order.
    pub entries: Vec<RuntimeCatalogEntryOutput>,
}

#[napi(object)]
/// Conservative non-authoritative ontology suggestion.
pub struct OntologySuggestionOutput {
    /// Always true: the result is not authoritative until explicitly adopted.
    pub draft: bool,
    /// Canonically ordered Rust-owned ontology document.
    pub document: serde_json::Value,
    /// SHA-256 of the canonical JSON document bytes.
    pub fingerprint_sha256: String,
    /// Relations omitted because the catalog lacks endpoint evidence.
    pub omitted_relation_types: Vec<String>,
}

#[napi(object)]
/// One semantic ontology validation diagnostic.
pub struct OntologyValidationDiagnosticOutput {
    /// Stable semantic diagnostic category.
    pub kind: String,
    /// Human-readable ontology field location.
    pub location: String,
    /// Human-readable diagnostic detail.
    pub message: String,
}

#[napi(object)]
/// Complete non-mutating ontology validation result.
pub struct OntologyValidationReportOutput {
    /// Whether the document passed semantic validation.
    pub valid: bool,
    /// Complete diagnostics in Rust validator order.
    pub diagnostics: Vec<OntologyValidationDiagnosticOutput>,
}

#[napi(object)]
/// Generation-managed authoritative ontology record.
pub struct WorkspaceOntologyOutput {
    /// Frozen workspace-record contract version.
    pub contract_version: u32,
    /// Explicit persisted mode: none, advisory, or strict.
    pub mode: String,
    /// Original adopted source syntax.
    pub source_format: Option<String>,
    /// SHA-256 of the canonical adopted document.
    pub canonical_ontology_sha256: Option<String>,
    /// Canonical adopted document, absent in none mode.
    pub canonical_ontology: Option<serde_json::Value>,
}

impl From<graphforge_api::WorkspaceOntology> for WorkspaceOntologyOutput {
    fn from(record: graphforge_api::WorkspaceOntology) -> Self {
        Self {
            contract_version: record.contract_version,
            mode: match record.mode {
                graphforge_api::WorkspaceOntologyMode::None => "none",
                graphforge_api::WorkspaceOntologyMode::Advisory => "advisory",
                graphforge_api::WorkspaceOntologyMode::Strict => "strict",
            }
            .into(),
            source_format: record.source_format.map(|format| {
                match format {
                    graphforge_api::WorkspaceOntologySourceFormat::Yaml => "yaml",
                    graphforge_api::WorkspaceOntologySourceFormat::Json => "json",
                }
                .into()
            }),
            canonical_ontology_sha256: record.canonical_ontology_sha256,
            canonical_ontology: record.canonical_ontology,
        }
    }
}

pub(super) fn ontology_mode(value: &str) -> Result<graphforge_api::OntologyMode> {
    match value {
        "advisory" => Ok(graphforge_api::OntologyMode::Advisory),
        "strict" => Ok(graphforge_api::OntologyMode::Strict),
        _ => Err(to_napi_err(&GfError::Validation(
            "ontology mode must be advisory or strict".into(),
        ))),
    }
}

fn ontology_export_format(value: &str) -> Result<graphforge_api::OntologyExportFormat> {
    match value {
        "yaml" | "yml" => Ok(graphforge_api::OntologyExportFormat::Yaml),
        "json" => Ok(graphforge_api::OntologyExportFormat::Json),
        _ => Err(to_napi_err(&GfError::Validation(
            "ontology export format must be yaml or json".into(),
        ))),
    }
}

#[napi]
impl GraphForge {
    /// Load and apply an ontology from `path` (YAML/JSON by extension).
    #[napi]
    pub fn load_ontology(&self, path: String) -> Result<()> {
        let mut g = self.open_write_guard()?;
        g.load_ontology(&path).map_err(|e| to_napi_err(&e))
    }

    /// Return the stable, deterministically ordered runtime-catalog contract.
    #[napi]
    pub fn inspect_runtime_catalog(&self) -> Result<RuntimeCatalogSnapshotOutput> {
        let graph = self.open_guard()?;
        let snapshot = graph
            .inspect_runtime_catalog()
            .map_err(|error| to_napi_err(&error))?;
        Ok(RuntimeCatalogSnapshotOutput {
            contract_version: snapshot.contract_version,
            entries: snapshot
                .entries
                .into_iter()
                .map(|entry| RuntimeCatalogEntryOutput {
                    kind: match entry.kind {
                        graphforge_api::CatalogEntryKind::EntityType => "entity_type",
                        graphforge_api::CatalogEntryKind::RelationType => "relation_type",
                        graphforge_api::CatalogEntryKind::Property => "property",
                    }
                    .into(),
                    name: entry.name,
                    owner: entry.owner,
                    observation_count: entry.observation_count.into(),
                })
                .collect(),
        })
    }

    /// Suggest a conservative, explicitly non-authoritative ontology draft.
    #[napi]
    pub fn suggest_ontology(
        &self,
        ontology_id: String,
        version: String,
    ) -> Result<OntologySuggestionOutput> {
        let graph = self.open_guard()?;
        let suggestion = graph
            .suggest_ontology(graphforge_api::OntologySuggestionOptions {
                ontology_id,
                version,
            })
            .map_err(|error| to_napi_err(&error))?;
        Ok(OntologySuggestionOutput {
            draft: suggestion.draft,
            document: serde_json::to_value(suggestion.document).map_err(|error| {
                to_napi_err(&GfError::Validation(format!(
                    "encode ontology suggestion: {error}"
                )))
            })?,
            fingerprint_sha256: suggestion.fingerprint_sha256,
            omitted_relation_types: suggestion.omitted_relation_types,
        })
    }

    /// Validate an ontology document without changing live or durable state.
    #[napi]
    pub fn validate_ontology(
        &self,
        document: serde_json::Value,
    ) -> Result<OntologyValidationReportOutput> {
        let document: graphforge_api::OntologyDoc =
            serde_json::from_value(document).map_err(|error| {
                to_napi_err(&GfError::Validation(format!(
                    "invalid ontology document: {error}"
                )))
            })?;
        let graph = self.open_guard()?;
        let report = graph.validate_ontology(&document);
        let diagnostics = report
            .diagnostics
            .into_iter()
            .map(|diagnostic| OntologyValidationDiagnosticOutput {
                kind: diagnostic.kind.to_string(),
                location: diagnostic.location,
                message: diagnostic.message,
            })
            .collect::<Vec<_>>();
        Ok(OntologyValidationReportOutput {
            valid: report.valid,
            diagnostics,
        })
    }

    /// Atomically export an explicit ontology source as YAML or JSON.
    #[napi]
    pub fn export_ontology(
        &self,
        source: String,
        destination: String,
        format: String,
        document: Option<serde_json::Value>,
    ) -> Result<()> {
        let source = match source.as_str() {
            "loaded" => graphforge_api::OntologyExportSource::Loaded,
            "adopted" => graphforge_api::OntologyExportSource::Adopted,
            "suggested" => {
                let document = document.ok_or_else(|| {
                    to_napi_err(&GfError::Validation(
                        "document is required for suggested ontology export".into(),
                    ))
                })?;
                graphforge_api::OntologyExportSource::Suggested(
                    serde_json::from_value(document).map_err(|error| {
                        to_napi_err(&GfError::Validation(format!(
                            "invalid ontology document: {error}"
                        )))
                    })?,
                )
            }
            _ => {
                return Err(to_napi_err(&GfError::Validation(
                    "ontology export source must be suggested, loaded, or adopted".into(),
                )));
            }
        };
        let format = ontology_export_format(&format)?;
        let graph = self.open_guard()?;
        graph
            .export_ontology(source, std::path::Path::new(&destination), format)
            .map_err(|error| to_napi_err(&error))
    }

    /// Inspect the generation-managed authoritative ontology record.
    #[napi]
    pub fn workspace_ontology(&self) -> Result<WorkspaceOntologyOutput> {
        let graph = self.open_guard()?;
        let record = graph
            .workspace_ontology()
            .map_err(|error| to_napi_err(&error))?;
        Ok(record.into())
    }

    /// Adopt an ontology as durable project authority.
    #[napi]
    pub fn adopt_ontology(
        &self,
        path: String,
        mode: String,
        operation_uuid: String,
        actor_uuid: Option<String>,
    ) -> Result<()> {
        let request = graphforge_api::AdoptOntologyRequest {
            context: WriteContext {
                operation_uuid: canonical_operation_id(&operation_uuid)?,
                actor_uuid: optional_uuid(actor_uuid.as_deref())?,
            },
            path: path.into(),
            mode: ontology_mode(&mode)?,
        };
        let mut graph = self.open_write_guard()?;
        graph
            .adopt_ontology(request)
            .map_err(|error| to_napi_err(&error))
    }

    /// Publish explicit durable ontology absence.
    #[napi]
    pub fn clear_ontology(&self, operation_uuid: String, actor_uuid: Option<String>) -> Result<()> {
        let request = graphforge_api::ClearOntologyRequest {
            context: WriteContext {
                operation_uuid: canonical_operation_id(&operation_uuid)?,
                actor_uuid: optional_uuid(actor_uuid.as_deref())?,
            },
        };
        let mut graph = self.open_write_guard()?;
        graph
            .clear_ontology(request)
            .map_err(|error| to_napi_err(&error))
    }
}
