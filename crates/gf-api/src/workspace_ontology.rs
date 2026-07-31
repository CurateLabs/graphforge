//! Persistent generation-managed ontology adoption.

use std::path::PathBuf;

use gf_core::{GfError, OntologyMode};
use gf_ontology::{OntologyCompiler, OntologyHandle, OntologyLoader};
use gf_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
    ProjectStageOutcome, WorkspaceConfiguration, WorkspaceOntology, WorkspaceOntologyMode,
    WorkspaceOntologySourceFormat,
};
use sha2::{Digest, Sha256};

use crate::{GraphForge, WriteContext};

/// Persistent ontology-adoption request.
#[derive(Debug, Clone)]
pub struct AdoptOntologyRequest {
    /// Idempotency and optional actor identity.
    pub context: WriteContext,
    /// YAML or JSON import path. Its bytes are input, never project authority.
    pub path: PathBuf,
    /// Persistent enforcement mode; exploratory is invalid for adoption.
    pub mode: OntologyMode,
}

/// Persistent ontology-clear request.
#[derive(Debug, Clone)]
pub struct ClearOntologyRequest {
    /// Idempotency and optional actor identity.
    pub context: WriteContext,
}

impl GraphForge {
    /// Inspect the authoritative ontology record from the current generation.
    ///
    /// # Errors
    /// Returns a structured project error for a missing or invalid participant.
    pub fn workspace_ontology(&self) -> Result<WorkspaceOntology, GfError> {
        let current = self.generation_for_read()?;
        let snapshot = current
            .participant_snapshot(
                gf_storage::WORKSPACE_CAPABILITY_ID,
                gf_storage::WORKSPACE_ONTOLOGY_FAMILY,
            )?
            .ok_or_else(|| GfError::Validation("workspace ontology is missing".into()))?;
        WorkspaceOntology::from_canonical_json(&snapshot.bytes)
    }

    /// Inspect the authoritative project configuration from the current generation.
    ///
    /// # Errors
    /// Returns a structured project error for a missing or invalid participant.
    pub fn workspace_configuration(&self) -> Result<WorkspaceConfiguration, GfError> {
        current_configuration(self)
    }

    /// Adopt a validated YAML/JSON ontology into one complete generation.
    ///
    /// # Errors
    /// Returns a structured ontology, validation, publication, or idempotency error.
    pub fn adopt_ontology(&mut self, request: AdoptOntologyRequest) -> Result<(), GfError> {
        let AdoptOntologyRequest {
            context,
            path,
            mode: requested_mode,
        } = request;
        let source_format = source_format(&path)?;
        let mode = match requested_mode {
            OntologyMode::Advisory => WorkspaceOntologyMode::Advisory,
            OntologyMode::Strict => WorkspaceOntologyMode::Strict,
            OntologyMode::Exploratory => {
                return Err(GfError::Validation(
                    "ontology adoption mode must be advisory or strict".into(),
                ));
            }
        };
        let document = OntologyLoader::load_file(&path)
            .map_err(|error| GfError::Ontology(format!("failed to load ontology: {error}")))?;
        let runtime = OntologyCompiler::compile(&document)
            .map_err(|error| GfError::Ontology(format!("failed to compile ontology: {error}")))?;
        let canonical_ontology = serde_json::to_value(&document)
            .map_err(|error| GfError::Ontology(format!("failed to encode ontology: {error}")))?;
        let canonical_bytes = serde_json::to_vec(&canonical_ontology)
            .map_err(|error| GfError::Ontology(format!("failed to encode ontology: {error}")))?;
        let record = WorkspaceOntology {
            contract_version: 1,
            mode,
            source_format: Some(source_format),
            canonical_ontology_sha256: Some(encode_hex(&Sha256::digest(canonical_bytes))),
            canonical_ontology: Some(canonical_ontology),
        };
        let mut configuration = current_configuration(self)?;
        configuration.ontology_mode = mode;
        publish_workspace_records(
            self,
            context.operation_uuid.0,
            context.actor_uuid,
            &record,
            &configuration,
        )?;
        self.ontology = Some(OntologyHandle::new(runtime));
        self.ontology_document = Some(document);
        self.ontology_mode = requested_mode;
        self.adjacency_provider = std::sync::Arc::new(gf_exec::PersistentAdjacencyProvider::new(
            self.dir.clone(),
            self.ontology_mode,
        ));
        Ok(())
    }

    /// Publish explicit ontology absence and return the project to exploratory mode.
    ///
    /// # Errors
    /// Returns a structured publication or idempotency error.
    pub fn clear_ontology(&mut self, request: ClearOntologyRequest) -> Result<(), GfError> {
        let ClearOntologyRequest { context } = request;
        let mut configuration = current_configuration(self)?;
        configuration.ontology_mode = WorkspaceOntologyMode::None;
        publish_workspace_records(
            self,
            context.operation_uuid.0,
            context.actor_uuid,
            &WorkspaceOntology::none(),
            &configuration,
        )?;
        self.ontology = None;
        self.ontology_document = None;
        self.ontology_mode = OntologyMode::Exploratory;
        self.adjacency_provider = std::sync::Arc::new(gf_exec::PersistentAdjacencyProvider::new(
            self.dir.clone(),
            self.ontology_mode,
        ));
        Ok(())
    }
}

fn current_configuration(graph: &GraphForge) -> Result<WorkspaceConfiguration, GfError> {
    let current = graph.generation_for_read()?;
    let snapshot = current
        .participant_snapshot(
            gf_storage::WORKSPACE_CAPABILITY_ID,
            gf_storage::WORKSPACE_CONFIGURATION_FAMILY,
        )?
        .ok_or_else(|| GfError::Validation("workspace configuration is missing".into()))?;
    WorkspaceConfiguration::from_canonical_json(&snapshot.bytes)
}

fn publish_workspace_records(
    graph: &mut GraphForge,
    operation_uuid: uuid::Uuid,
    actor_uuid: Option<uuid::Uuid>,
    ontology: &WorkspaceOntology,
    configuration: &WorkspaceConfiguration,
) -> Result<(), GfError> {
    let root = graph.resolved_generation.container_root().to_path_buf();
    let parent = gf_storage::resolve_project_generation(&root)?;
    parent.validate_complete_participant_inventory()?;
    let expected_parent = *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned");
    if parent.generation_uuid() != expected_parent {
        return Err(GfError::Validation(
            "project generation changed before ontology publication".into(),
        ));
    }
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == gf_storage::WORKSPACE_CAPABILITY_ID
                && matches!(
                    snapshot.record_family_id.as_str(),
                    gf_storage::WORKSPACE_ONTOLOGY_FAMILY
                        | gf_storage::WORKSPACE_CONFIGURATION_FAMILY
                ))
        })
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()?;
    participants.push(workspace_participant(
        gf_storage::WORKSPACE_ONTOLOGY_FAMILY,
        ontology.to_canonical_json()?,
    ));
    participants.push(workspace_participant(
        gf_storage::WORKSPACE_CONFIGURATION_FAMILY,
        configuration.to_canonical_json()?,
    ));
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    let generation_uuid = workspace_generation_uuid(operation_uuid, actor_uuid, &participants);
    let request = ProjectGenerationRequest {
        transaction_uuid: operation_uuid,
        generation_uuid,
        capabilities: parent
            .capabilities()
            .into_iter()
            .map(|entry| ProjectCapability {
                capability_id: entry.capability_id,
                capability_version: entry.capability_version,
            })
            .collect(),
        participants,
    };
    let receipt = match gf_storage::stage_project_generation(&root, &request)? {
        ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
        ProjectStageOutcome::Staged(staged) => staged
            .validate(validate_workspace_record_inventory, |actual_parent, _| {
                if actual_parent.generation_uuid() != expected_parent {
                    return Err(GfError::Validation(
                        "project generation changed before ontology publication".into(),
                    ));
                }
                Ok(())
            })?
            .publish()?,
    };
    *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned") = receipt.generation_uuid;
    graph.resolved_generation = gf_storage::resolve_project_generation(&root)?;
    Ok(())
}

fn validate_workspace_record_inventory(
    metadata: &[gf_storage::StagedParticipant],
) -> Result<(), GfError> {
    let mut ontology_count = 0;
    let mut configuration_count = 0;
    for entry in metadata
        .iter()
        .filter(|entry| entry.capability_id == gf_storage::WORKSPACE_CAPABILITY_ID)
    {
        match entry.record_family_id.as_str() {
            gf_storage::WORKSPACE_ONTOLOGY_FAMILY => ontology_count += 1,
            gf_storage::WORKSPACE_CONFIGURATION_FAMILY => configuration_count += 1,
            _ => {}
        }
    }
    if ontology_count != 1 || configuration_count != 1 {
        return Err(GfError::Validation(
            "workspace generation must contain exactly one ontology and one configuration".into(),
        ));
    }
    Ok(())
}

fn workspace_generation_uuid(
    operation_uuid: uuid::Uuid,
    actor_uuid: Option<uuid::Uuid>,
    participants: &[ProjectParticipant],
) -> uuid::Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-workspace-ontology-generation/1");
    hasher.update(operation_uuid.as_bytes());
    if let Some(actor_uuid) = actor_uuid {
        hasher.update([1]);
        hasher.update(actor_uuid.as_bytes());
    } else {
        hasher.update([0]);
    }
    for participant in participants {
        hasher.update(participant.capability_id.as_bytes());
        hasher.update([0]);
        hasher.update(participant.record_family_id.as_bytes());
        hasher.update([0]);
        hasher.update(Sha256::digest(&participant.bytes));
    }
    gf_core::canonical::uuid_v8(hasher.finalize().into())
}

fn workspace_participant(family: &str, bytes: Vec<u8>) -> ProjectParticipant {
    ProjectParticipant {
        capability_id: gf_storage::WORKSPACE_CAPABILITY_ID.into(),
        capability_version: gf_storage::WORKSPACE_CAPABILITY_VERSION,
        record_family_id: family.into(),
        record_version: 1,
        encoding: ProjectParticipantEncoding::Json,
        schema_fingerprint: Sha256::digest(format!("workspace/{family}@1")).into(),
        row_count: 1,
        bytes,
    }
}

fn snapshot_to_participant(
    snapshot: gf_storage::ProjectParticipantSnapshot,
) -> Result<ProjectParticipant, GfError> {
    let encoding = match snapshot.encoding.as_str() {
        "parquet" => ProjectParticipantEncoding::Parquet,
        "arrow" => ProjectParticipantEncoding::Arrow,
        "json" => ProjectParticipantEncoding::Json,
        _ => {
            return Err(GfError::Validation(
                "committed participant has unsupported encoding".into(),
            ));
        }
    };
    Ok(ProjectParticipant {
        capability_id: snapshot.capability_id,
        capability_version: snapshot.capability_version,
        record_family_id: snapshot.record_family_id,
        record_version: snapshot.record_version,
        encoding,
        schema_fingerprint: snapshot.schema_fingerprint,
        row_count: snapshot.row_count,
        bytes: snapshot.bytes,
    })
}

fn source_format(path: &std::path::Path) -> Result<WorkspaceOntologySourceFormat, GfError> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("yaml" | "yml") => Ok(WorkspaceOntologySourceFormat::Yaml),
        Some("json") => Ok(WorkspaceOntologySourceFormat::Json),
        _ => Err(GfError::Validation(
            "ontology input must use .yaml, .yml, or .json".into(),
        )),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OperationId;
    use arrow::array::{FixedSizeBinaryArray, Float64Array};
    use gf_core::{RankOptions, algorithms::RankAlgorithm};

    fn ontology_yaml() -> &'static str {
        "ontology_id: test\nversion: \"1\"\nentity_types:\n  - name: Person\n    abstract: false\nrelation_types:\n  - name: KNOWS\n    src: Person\n    dst: Person\n"
    }

    fn context(seed: u128) -> WriteContext {
        WriteContext {
            operation_uuid: OperationId(uuid::Uuid::from_u128(seed)),
            actor_uuid: None,
        }
    }

    fn staged_workspace(family: &str) -> gf_storage::StagedParticipant {
        gf_storage::StagedParticipant {
            capability_id: gf_storage::WORKSPACE_CAPABILITY_ID.into(),
            capability_version: 1,
            record_family_id: family.into(),
            record_version: 1,
            relative_path: format!("workspace/{family}.json"),
            encoding: "json".into(),
            byte_length: 1,
            row_count: 1,
            schema_fingerprint: "schema".into(),
            content_sha256: "content".into(),
        }
    }

    fn rank_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<(Vec<u8>, u64)> {
        let uuids = batch
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let scores = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|row| (uuids.value(row).to_vec(), scores.value(row).to_bits()))
            .collect()
    }

    #[test]
    fn adopted_ontology_and_mode_reopen_from_one_generation() {
        let root = tempfile::tempdir().unwrap();
        let imports = tempfile::tempdir().unwrap();
        let input = imports.path().join("input.yaml");
        std::fs::write(&input, ontology_yaml()).unwrap();
        let mut graph = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();

        graph
            .adopt_ontology(AdoptOntologyRequest {
                context: context(1),
                path: input,
                mode: OntologyMode::Strict,
            })
            .unwrap();
        let generation_uuid = *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        drop(graph);

        let reopened = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();
        assert_eq!(reopened.ontology_mode(), OntologyMode::Strict);
        assert!(reopened.ontology.is_some());
        assert_eq!(
            reopened.resolved_generation.generation_uuid(),
            generation_uuid
        );
        assert_eq!(
            reopened
                .resolved_generation
                .participant_snapshot("workspace", "ontology")
                .unwrap()
                .unwrap()
                .record_version,
            1
        );
    }

    #[test]
    fn ontology_promotion_after_checkpoint_revert_preserves_restoration_record() {
        let root = tempfile::tempdir().unwrap();
        let imports = tempfile::tempdir().unwrap();
        let input = imports.path().join("input.yaml");
        std::fs::write(&input, ontology_yaml()).unwrap();
        let mut graph = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();
        graph
            .adopt_ontology(AdoptOntologyRequest {
                context: context(20),
                path: input.clone(),
                mode: OntologyMode::Advisory,
            })
            .unwrap();
        graph
            .checkpoint(crate::CheckpointRequest {
                name: "Before promotion".into(),
                description: None,
                idempotency_key: crate::OperationId(uuid::Uuid::from_u128(21)),
                actor_uuid: None,
            })
            .unwrap();
        graph.execute("CREATE (:Person)").unwrap();
        graph
            .revert_to_checkpoint(crate::RevertCheckpointRequest {
                name: "Before promotion".into(),
                reason: "restore advisory generation".into(),
                idempotency_key: crate::OperationId(uuid::Uuid::from_u128(22)),
                actor_uuid: None,
            })
            .unwrap();

        graph
            .adopt_ontology(AdoptOntologyRequest {
                context: context(23),
                path: input,
                mode: OntologyMode::Strict,
            })
            .unwrap();
        assert_eq!(graph.ontology_mode(), OntologyMode::Strict);
        assert!(
            graph
                .resolved_generation
                .participant_snapshot("workspace", "restoration_transition")
                .unwrap()
                .is_some()
        );
        drop(graph);

        let reopened = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();
        assert_eq!(reopened.ontology_mode(), OntologyMode::Strict);
        assert!(
            reopened
                .resolved_generation
                .participant_snapshot("workspace", "restoration_transition")
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn workspace_inventory_requires_exactly_one_ontology_and_configuration() {
        let ontology = staged_workspace(gf_storage::WORKSPACE_ONTOLOGY_FAMILY);
        let configuration = staged_workspace(gf_storage::WORKSPACE_CONFIGURATION_FAMILY);
        let restoration = staged_workspace("restoration_transition");
        assert!(
            validate_workspace_record_inventory(&[
                ontology.clone(),
                configuration.clone(),
                restoration,
            ])
            .is_ok()
        );
        assert!(validate_workspace_record_inventory(std::slice::from_ref(&ontology)).is_err());
        assert!(
            validate_workspace_record_inventory(&[
                ontology.clone(),
                configuration.clone(),
                configuration.clone(),
            ])
            .is_err()
        );
        assert!(
            validate_workspace_record_inventory(&[
                ontology.clone(),
                ontology,
                configuration.clone(),
            ])
            .is_err()
        );
        assert!(
            validate_workspace_record_inventory(&[configuration.clone(), configuration]).is_err()
        );
    }

    #[test]
    fn advisory_adoption_preserves_exploratory_edge_uuid_for_algorithms_and_reopen() {
        let root = tempfile::tempdir().unwrap();
        let imports = tempfile::tempdir().unwrap();
        let input = imports.path().join("input.yaml");
        std::fs::write(&input, ontology_yaml()).unwrap();
        let mut graph = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();
        graph
            .execute("CREATE (:Person {name: 'Ada'})-[:KNOWS]->(:Person {name: 'Cy'})")
            .unwrap();
        let options = RankOptions {
            by: RankAlgorithm::Degree,
            via: Some("KNOWS".into()),
            directed: false,
            write_property: None,
        };
        let exploratory = graph.rank("Person", options.clone()).unwrap();
        let exploratory_descriptor = graph
            .prepare_rank_invocation("Person", &options)
            .unwrap()
            .canonical_bytes()
            .to_vec();

        graph
            .adopt_ontology(AdoptOntologyRequest {
                context: context(10),
                path: input,
                mode: OntologyMode::Advisory,
            })
            .unwrap();
        graph
            .execute("CREATE (:Person)-[:KNOWS]->(:Person)")
            .unwrap();
        let advisory = graph.rank("Person", options.clone()).unwrap();
        let exploratory_nodes = rank_rows(&exploratory)
            .into_iter()
            .map(|(uuid, _)| uuid)
            .collect::<std::collections::BTreeSet<_>>();
        let advisory_rows = rank_rows(&advisory);
        let advisory_nodes = advisory_rows
            .iter()
            .map(|(uuid, _)| uuid.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(exploratory_nodes.len(), 2);
        assert_eq!(advisory_nodes.len(), 4);
        assert!(exploratory_nodes.is_subset(&advisory_nodes));
        let advisory_descriptor = graph
            .prepare_rank_invocation("Person", &options)
            .unwrap()
            .canonical_bytes()
            .to_vec();
        assert_ne!(advisory_descriptor, exploratory_descriptor);
        drop(graph);

        let reopened = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();
        assert_eq!(
            rank_rows(&reopened.rank("Person", options.clone()).unwrap()),
            advisory_rows
        );
        assert_eq!(
            reopened
                .prepare_rank_invocation("Person", &options)
                .unwrap()
                .canonical_bytes(),
            advisory_descriptor
        );
    }

    #[test]
    fn root_ontology_file_is_not_authoritative_after_initialization() {
        let root = tempfile::tempdir().unwrap();
        drop(GraphForge::new(Some(root.path().to_str().unwrap())).unwrap());
        std::fs::write(root.path().join("ontology.yaml"), ontology_yaml()).unwrap();

        let reopened = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();

        assert_eq!(reopened.ontology_mode(), OntologyMode::Exploratory);
        assert!(reopened.ontology.is_none());
    }

    #[test]
    fn clear_ontology_publishes_explicit_absence() {
        let root = tempfile::tempdir().unwrap();
        let imports = tempfile::tempdir().unwrap();
        let input = imports.path().join("input.json");
        let document = OntologyLoader::load_yaml(ontology_yaml().as_bytes()).unwrap();
        std::fs::write(&input, serde_json::to_vec(&document).unwrap()).unwrap();
        let mut graph = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();
        graph
            .adopt_ontology(AdoptOntologyRequest {
                context: context(2),
                path: input,
                mode: OntologyMode::Advisory,
            })
            .unwrap();
        graph
            .clear_ontology(ClearOntologyRequest {
                context: context(3),
            })
            .unwrap();
        drop(graph);

        let reopened = GraphForge::new(Some(root.path().to_str().unwrap())).unwrap();
        assert_eq!(reopened.ontology_mode(), OntologyMode::Exploratory);
        assert!(reopened.ontology.is_none());
    }
}
