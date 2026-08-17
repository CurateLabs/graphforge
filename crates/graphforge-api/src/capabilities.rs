//! Versioned project-capability inspection.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::{GfError, ProjectErrorCode};
use graphforge_exec::{ExecutionResult, ExecutionStats};
use graphforge_knowledge::EPISTEMIC_CAPABILITY_VERSION;
use graphforge_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
    ProjectStageOutcome,
};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::GraphForge;

/// Frozen knowledge public API contract version.
pub const KNOWLEDGE_API_VERSION: u32 = 1;

/// Stable idempotency identity for a Bazel-migration0 write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationId(pub Uuid);

/// Shared context for a Bazel-migration0 write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteContext {
    /// Required operation/idempotency UUID.
    pub operation_uuid: OperationId,
    /// Optional analyst or agent identity.
    pub actor_uuid: Option<Uuid>,
}

/// Closed project-capability registry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityId {
    /// Graph storage and execution.
    Graph,
    /// Mutation provenance and lineage.
    Provenance,
    /// Immutable knowledge ledger.
    Knowledge,
    /// epistemic extension.
    Epistemic,
    /// Optional epistemic valid-time interpretation.
    ValidTime,
}

impl CapabilityId {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Graph => "graph",
            Self::Provenance => "provenance",
            Self::Knowledge => "knowledge",
            Self::Epistemic => "epistemic",
            Self::ValidTime => "valid_time",
        }
    }
}

/// Exact request shape for atomic capability initialization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnableCapabilityRequest {
    /// Write identity and optional actor.
    pub context: WriteContext,
    /// Registered capability.
    pub capability_id: CapabilityId,
    /// Requested capability contract version.
    pub capability_version: u32,
}

impl GraphForge {
    /// Inspect only the current committed capability manifest.
    ///
    /// Participant files are not opened. Unknown capability IDs and future
    /// versions remain visible as `unsupported_future`.
    ///
    /// # Errors
    /// Returns a structured project error when the current generation cannot
    /// be resolved or its manifest is invalid.
    pub fn project_capabilities(&self) -> Result<ExecutionResult, GfError> {
        let resolved = self.generation_for_read()?;
        let capabilities = resolved.capabilities();
        let row_count = capabilities.len();
        let ids = StringArray::from_iter_values(
            capabilities
                .iter()
                .map(|capability| capability.capability_id.as_str()),
        );
        let versions = UInt32Array::from_iter_values(
            capabilities
                .iter()
                .map(|capability| capability.capability_version),
        );
        let support = StringArray::from_iter_values(capabilities.iter().map(|capability| {
            if supported_capability(&capability.capability_id, capability.capability_version) {
                "supported"
            } else {
                "unsupported_future"
            }
        }));
        let mut inventories = FixedSizeBinaryBuilder::with_capacity(row_count, 32);
        for _ in 0..row_count {
            inventories.append_null();
        }
        let mut generations = FixedSizeBinaryBuilder::with_capacity(row_count, 16);
        for _ in 0..row_count {
            generations
                .append_value(resolved.generation_uuid().as_bytes())
                .expect("UUID is exactly 16 bytes");
        }
        let schema = capability_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(ids) as ArrayRef,
                Arc::new(versions),
                Arc::new(support),
                Arc::new(inventories.finish()),
                Arc::new(generations.finish()),
            ],
        )
        .map_err(|error| GfError::Execution(error.to_string()))?;
        Ok(ExecutionResult {
            schema,
            batches: vec![batch],
            stats: ExecutionStats {
                rows_produced: u64::try_from(row_count).unwrap_or(u64::MAX),
                execution_time_ms: 0,
            },
            side_effects: None,
            mutation_receipt: None,
        })
    }

    /// Atomically add one supported capability to the committed manifest.
    ///
    /// Existing participant bytes are verified and copied into the complete
    /// replacement generation. Capability-specific slices register their
    /// required initial empty tables through the participant registry.
    ///
    /// # Errors
    /// Returns a structured validation/project error for unsupported versions,
    /// conflicting declarations, participant corruption, or publication
    /// failure.
    #[allow(
        clippy::needless_pass_by_value,
        reason = "graphforge-knowledge-api/1 freezes owned request structs"
    )]
    pub fn enable_capability(
        &self,
        request: EnableCapabilityRequest,
    ) -> Result<ExecutionResult, GfError> {
        let capability_id = request.capability_id.as_str();
        if request.capability_id == CapabilityId::Graph && request.capability_version == 1 {
            return self.project_capabilities();
        }
        if !supported_capability(capability_id, request.capability_version) {
            return Err(unsupported_capability(
                capability_id,
                request.capability_version,
            ));
        }
        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        if request.capability_id == CapabilityId::ValidTime {
            parent.require_capability("epistemic", EPISTEMIC_CAPABILITY_VERSION)?;
        }
        let existing = parent.capability(capability_id)?;
        if let Some(existing) = &existing
            && existing.capability_version != request.capability_version
        {
            return Err(unsupported_capability(
                capability_id,
                request.capability_version,
            ));
        }

        let mut capabilities = parent
            .capabilities()
            .into_iter()
            .map(|entry| ProjectCapability {
                capability_id: entry.capability_id,
                capability_version: entry.capability_version,
            })
            .collect::<Vec<_>>();
        if existing.is_none() {
            capabilities.push(ProjectCapability {
                capability_id: capability_id.into(),
                capability_version: request.capability_version,
            });
        }
        capabilities.sort_by(|left, right| left.capability_id.cmp(&right.capability_id));

        let mut participants = parent
            .participant_snapshots()?
            .into_iter()
            .map(|snapshot| {
                Ok(ProjectParticipant {
                    capability_id: snapshot.capability_id,
                    capability_version: snapshot.capability_version,
                    record_family_id: snapshot.record_family_id,
                    record_version: snapshot.record_version,
                    encoding: parse_encoding(&snapshot.encoding)?,
                    schema_fingerprint: snapshot.schema_fingerprint,
                    row_count: snapshot.row_count,
                    bytes: snapshot.bytes,
                })
            })
            .collect::<Result<Vec<_>, GfError>>()?;
        if existing.is_none() {
            participants.extend(initial_capability_participants(
                request.capability_id,
                request.capability_version,
            )?);
        }
        let publication = ProjectGenerationRequest {
            transaction_uuid: request.context.operation_uuid.0,
            generation_uuid: capability_generation_uuid(
                &request.context,
                request.capability_id,
                request.capability_version,
            ),
            capabilities,
            participants,
        };
        let generation_uuid = match self.stage_project_generation(&publication)? {
            ProjectStageOutcome::AlreadyPublished(receipt) => receipt.generation_uuid,
            ProjectStageOutcome::Staged(staged) => {
                let expected_parent = parent.generation_uuid();
                staged
                    .validate(
                        |_| Ok(()),
                        |actual_parent, _| {
                            if actual_parent.generation_uuid() != expected_parent {
                                return Err(GfError::Validation(
                                    "project generation changed before capability publication"
                                        .into(),
                                ));
                            }
                            Ok(())
                        },
                    )?
                    .publish()?
                    .generation_uuid
            }
        };
        *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned") = generation_uuid;
        self.project_capabilities()
    }
}

fn supported_capability(capability_id: &str, version: u32) -> bool {
    matches!(
        (capability_id, version),
        (
            "graph" | "workspace" | "provenance" | "knowledge" | "valid_time",
            1
        ) | ("epistemic", EPISTEMIC_CAPABILITY_VERSION)
    )
}

fn unsupported_capability(capability_id: &str, version: u32) -> GfError {
    GfError::Project {
        code: ProjectErrorCode::UnsupportedCapabilityVersion,
        message: format!("unsupported capability {capability_id}@{version}"),
    }
}

fn initial_capability_participants(
    capability_id: CapabilityId,
    version: u32,
) -> Result<Vec<ProjectParticipant>, GfError> {
    match (capability_id, version) {
        (CapabilityId::Provenance, 1) => crate::provenance::empty_participants(),
        (CapabilityId::Knowledge, 1) => crate::knowledge::empty_participants(),
        (CapabilityId::Epistemic, 1) => crate::knowledge::empty_epistemic_participants(),
        (CapabilityId::ValidTime, 1) => crate::valid_time::empty_participants(),
        _ => Ok(Vec::new()),
    }
}

fn parse_encoding(value: &str) -> Result<ProjectParticipantEncoding, GfError> {
    match value {
        "parquet" => Ok(ProjectParticipantEncoding::Parquet),
        "arrow" => Ok(ProjectParticipantEncoding::Arrow),
        "json" => Ok(ProjectParticipantEncoding::Json),
        _ => Err(GfError::Validation(
            "committed participant has unsupported encoding".into(),
        )),
    }
}

fn capability_generation_uuid(
    context: &WriteContext,
    capability_id: CapabilityId,
    version: u32,
) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-enable-capability/1");
    hasher.update(context.operation_uuid.0.as_bytes());
    match context.actor_uuid {
        Some(actor_uuid) => {
            hasher.update([1]);
            hasher.update(actor_uuid.as_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.update(capability_id.as_str().as_bytes());
    hasher.update(version.to_be_bytes());
    let digest: [u8; 32] = hasher.finalize().into();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn capability_schema() -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("capability_id", DataType::Utf8, false),
            Field::new("capability_version", DataType::UInt32, false),
            Field::new("support", DataType::Utf8, false),
            Field::new(
                "schema_inventory_sha256",
                DataType::FixedSizeBinary(32),
                true,
            ),
            Field::new("generation_uuid", DataType::FixedSizeBinary(16), false),
        ],
        HashMap::from([
            ("graphforge.contract.id".into(), "project_capability".into()),
            ("graphforge.contract.version".into(), "1".into()),
        ]),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn initial_project_reports_mandatory_graph_and_workspace_capabilities() {
        let graph = GraphForge::new(None).unwrap();

        let result = graph.project_capabilities().unwrap();

        assert_eq!(result.stats.rows_produced, 2);
        assert_eq!(
            result.schema.metadata().get("graphforge.contract.id"),
            Some(&"project_capability".to_owned())
        );
        let batch = &result.batches[0];
        assert_eq!(
            batch
                .column_by_name("capability_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "graph"
        );
        assert_eq!(
            batch
                .column_by_name("capability_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "workspace"
        );
        assert_eq!(
            batch
                .column_by_name("support")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "supported"
        );
    }

    #[test]
    fn capability_enable_is_atomic_and_idempotent() {
        let graph = GraphForge::new(None).unwrap();
        assert_eq!(
            graph.lifecycle_mode,
            graphforge_storage::filesystem_admission::ProjectLifecycleMode::Ephemeral
        );
        let request = EnableCapabilityRequest {
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            capability_id: CapabilityId::Knowledge,
            capability_version: 1,
        };

        let first = graph.enable_capability(request.clone()).unwrap();
        let second = graph.enable_capability(request).unwrap();

        assert_eq!(first.stats.rows_produced, 3);
        assert_eq!(second.stats.rows_produced, 3);
        let ids = first.batches[0]
            .column_by_name("capability_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.value(0), "graph");
        assert_eq!(ids.value(1), "knowledge");
        assert_eq!(ids.value(2), "workspace");
        assert_eq!(
            first.batches[0]
                .column_by_name("generation_uuid")
                .unwrap()
                .to_data(),
            second.batches[0]
                .column_by_name("generation_uuid")
                .unwrap()
                .to_data()
        );
    }

    #[test]
    fn enabled_capability_is_selected_after_persistent_reopen() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                capability_id: CapabilityId::Knowledge,
                capability_version: 1,
            })
            .unwrap();
        drop(graph);

        let reopened = GraphForge::new(Some(path)).unwrap();
        let result = reopened.project_capabilities().unwrap();
        let ids = result.batches[0]
            .column_by_name("capability_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            ids.iter().collect::<Vec<_>>(),
            vec![Some("graph"), Some("knowledge"), Some("workspace")]
        );
    }

    #[test]
    fn operation_uuid_reuse_with_changed_context_conflicts() {
        let graph = GraphForge::new(None).unwrap();
        let operation_uuid = OperationId(Uuid::now_v7());
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid: None,
                },
                capability_id: CapabilityId::Knowledge,
                capability_version: 1,
            })
            .unwrap();

        let error = graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid,
                    actor_uuid: Some(Uuid::now_v7()),
                },
                capability_id: CapabilityId::Knowledge,
                capability_version: 1,
            })
            .unwrap_err();

        assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
    }

    #[test]
    fn capability_transition_never_drops_untracked_graph_files() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().to_str().unwrap();
        let graph = GraphForge::new(Some(path)).unwrap();
        graph.execute("CREATE (:Person {name: 'Ada'})").unwrap();

        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                capability_id: CapabilityId::Knowledge,
                capability_version: 1,
            })
            .unwrap();
        drop(graph);
        let reopened = GraphForge::new(Some(path)).unwrap();
        let result = reopened
            .execute("MATCH (n:Person) RETURN n.name AS name")
            .unwrap();
        assert_eq!(result.stats.rows_produced, 1);
    }

    #[test]
    fn future_capability_is_reported_without_opening_participants() {
        let root = tempfile::tempdir().unwrap();
        let initial = graphforge_storage::open_or_initialize_project(root.path()).unwrap();
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: vec![
                ProjectCapability {
                    capability_id: "graph".into(),
                    capability_version: 1,
                },
                ProjectCapability {
                    capability_id: "epistemic".into(),
                    capability_version: 99,
                },
                ProjectCapability {
                    capability_id: "workspace".into(),
                    capability_version: 1,
                },
            ],
            participants: initial
                .participant_snapshots()
                .unwrap()
                .into_iter()
                .map(|snapshot| {
                    Ok(ProjectParticipant {
                        capability_id: snapshot.capability_id,
                        capability_version: snapshot.capability_version,
                        record_family_id: snapshot.record_family_id,
                        record_version: snapshot.record_version,
                        encoding: parse_encoding(&snapshot.encoding)?,
                        schema_fingerprint: snapshot.schema_fingerprint,
                        row_count: snapshot.row_count,
                        bytes: snapshot.bytes,
                    })
                })
                .collect::<Result<Vec<_>, GfError>>()
                .unwrap(),
        };
        let ProjectStageOutcome::Staged(staged) =
            graphforge_storage::stage_project_generation(root.path(), &request).unwrap()
        else {
            panic!("new request replayed");
        };
        staged
            .validate(
                |_| Ok(()),
                |parent, _| {
                    assert_eq!(parent.generation_uuid(), initial.generation_uuid());
                    Ok(())
                },
            )
            .unwrap()
            .publish()
            .unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();

        let result = graph.project_capabilities().unwrap();
        let support = result.batches[0]
            .column_by_name("support")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(support.value(0), "unsupported_future");
        assert_eq!(support.value(1), "supported");
        assert_eq!(support.value(2), "supported");
    }

    #[test]
    fn epistemic_v1_is_supported_but_future_versions_are_rejected() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                capability_id: CapabilityId::Epistemic,
                capability_version: 1,
            })
            .unwrap();
        for (capability_id, capability_version) in
            [(CapabilityId::Epistemic, 2), (CapabilityId::Knowledge, 2)]
        {
            let error = graph
                .enable_capability(EnableCapabilityRequest {
                    context: WriteContext {
                        operation_uuid: OperationId(Uuid::now_v7()),
                        actor_uuid: None,
                    },
                    capability_id,
                    capability_version,
                })
                .unwrap_err();
            assert_eq!(error.code(), "GF_UNSUPPORTED_CAPABILITY_VERSION");
        }
    }
}
