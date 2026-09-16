//! Graph mutation publication, reconciliation, and in-memory reset.

use super::{CompositionBindingContext, GfError, GraphForge, RuntimeCatalog, provenance};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) type BoundGenerationStorage = (
    Arc<CompositionBindingContext>,
    graphforge_storage::SemanticStorageBindings,
    Vec<(std::path::PathBuf, std::path::PathBuf)>,
);

impl GraphForge {
    pub(crate) fn stage_project_generation(
        &self,
        request: &graphforge_storage::ProjectGenerationRequest,
    ) -> Result<graphforge_storage::ProjectStageOutcome, GfError> {
        graphforge_storage::stage_project_generation_with_graph_tree_mode(
            self.resolved_generation.container_root(),
            request,
            None,
            self.lifecycle_mode,
        )
    }

    pub(super) fn bind_generation_storage(
        &self,
        context: &Arc<CompositionBindingContext>,
    ) -> Result<BoundGenerationStorage, GfError> {
        let current = self
            .semantic_storage_bindings
            .lock()
            .expect("semantic storage binding lock poisoned");
        let (projected, route_moves) =
            if current.is_none() && context.composition().modules.len() == 1 {
                let projection =
                    graphforge_storage::SemanticStorageBindings::project_legacy_unambiguous(
                        context.composition(),
                        &self.dir(),
                    )?;
                (projection.bindings, projection.route_moves)
            } else {
                if current.is_none() {
                    graphforge_storage::require_atomic_legacy_migration(&self.dir())?;
                }
                (
                    graphforge_storage::SemanticStorageBindings::project(
                        context.composition(),
                        current.as_ref(),
                    )?,
                    Vec::new(),
                )
            };
        projected.validate_against(context.composition())?;
        let context = context.with_generation_storage_ids(
            projected
                .bindings
                .iter()
                .map(|binding| (binding.symbol.clone(), binding.storage_id)),
        )?;
        drop(current);
        Ok((Arc::new(context), projected, route_moves))
    }

    pub(super) fn publish_graph_mutation(
        &self,
        receipt: &graphforge_exec::MutationReceipt,
    ) -> Result<(), GfError> {
        self.publish_graph_mutation_with_bindings(receipt, None)
    }

    pub(super) fn publish_graph_mutation_with_bindings(
        &self,
        receipt: &graphforge_exec::MutationReceipt,
        candidate_bindings: Option<&graphforge_storage::SemanticStorageBindings>,
    ) -> Result<(), GfError> {
        let operation_uuid = uuid::Uuid::now_v7();
        let recorded_at_micros = (self.clock.lock().expect("clock lock poisoned"))()?;
        self.publish_graph_mutation_with_context_and_bindings(
            receipt,
            operation_uuid,
            None,
            recorded_at_micros,
            candidate_bindings,
        )
    }

    pub(crate) fn publish_graph_mutation_with_context(
        &self,
        receipt: &graphforge_exec::MutationReceipt,
        operation_uuid: uuid::Uuid,
        actor_uuid: Option<uuid::Uuid>,
        recorded_at_micros: i64,
    ) -> Result<(), GfError> {
        self.publish_graph_mutation_with_context_and_bindings(
            receipt,
            operation_uuid,
            actor_uuid,
            recorded_at_micros,
            None,
        )
    }

    fn publish_graph_mutation_with_context_and_bindings(
        &self,
        receipt: &graphforge_exec::MutationReceipt,
        operation_uuid: uuid::Uuid,
        actor_uuid: Option<uuid::Uuid>,
        recorded_at_micros: i64,
        candidate_bindings: Option<&graphforge_storage::SemanticStorageBindings>,
    ) -> Result<(), GfError> {
        use graphforge_storage::{
            ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome,
        };

        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(GfError::Validation(
                "project generation changed before graph publication".into(),
            ));
        }

        if !graphforge_storage::uuid_membership_index_is_fresh(&self.dir())? {
            graphforge_storage::rebuild_uuid_membership_indexes(
                &self.dir(),
                graphforge_storage::UuidIndexBuildLimits::default(),
            )?;
        }
        let graph = graphforge_storage::capture_graph_files(&self.dir())?.1;
        let provenance_enabled = parent.capability("provenance")?.is_some();
        let installed_bindings = self
            .semantic_storage_bindings
            .lock()
            .expect("semantic storage binding lock poisoned");
        let participants = graph_publication_participants(
            &parent,
            graph,
            candidate_bindings.or(installed_bindings.as_ref()),
            provenance_enabled,
            receipt,
            operation_uuid,
            actor_uuid,
            recorded_at_micros,
        )?;
        drop(installed_bindings);
        let capabilities = parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect::<Vec<_>>();
        let generation_uuid = mutation_generation_uuid(operation_uuid, &participants);
        let request = ProjectGenerationRequest {
            transaction_uuid: operation_uuid,
            generation_uuid,
            capabilities,
            participants,
        };
        let publication = match graphforge_storage::stage_project_generation_with_graph_tree_mode(
            root,
            &request,
            Some(self.dir().path()),
            self.lifecycle_mode,
        )? {
            ProjectStageOutcome::AlreadyPublished(receipt) => Ok(receipt),
            ProjectStageOutcome::Staged(staged) => staged
                .validate(
                    |_| Ok(()),
                    |actual_parent, _| {
                        if actual_parent.generation_uuid() != expected_parent {
                            return Err(GfError::Validation(
                                "project generation changed before graph publication".into(),
                            ));
                        }
                        Ok(())
                    },
                )?
                .publish(),
        };
        let published = match publication {
            Ok(receipt) => receipt,
            Err(error) => {
                if let Ok(current) = graphforge_storage::resolve_project_generation(root)
                    && current.generation_uuid() == generation_uuid
                {
                    self.install_property_generation(&current)?;
                }
                return Err(error);
            }
        };
        let committed = graphforge_storage::resolve_project_generation(root)?;
        if committed.generation_uuid() != published.generation_uuid {
            return Err(GfError::Storage(
                "published property authority did not resolve exact generation".into(),
            ));
        }
        self.install_property_generation(&committed)?;
        Ok(())
    }

    pub(super) fn publish_graph_mutation_with_generation(
        &self,
        receipt: &graphforge_exec::MutationReceipt,
        operation_uuid: uuid::Uuid,
        generation_uuid: uuid::Uuid,
        expected_parent: uuid::Uuid,
        recorded_at_micros: i64,
    ) -> Result<(), GfError> {
        use graphforge_storage::{
            ProjectCapability, ProjectGenerationRequest, ProjectStageOutcome,
        };

        let root = self.resolved_generation.container_root();
        let parent = graphforge_storage::resolve_project_generation(root)?;
        parent.validate_complete_participant_inventory()?;
        if parent.generation_uuid() != expected_parent {
            return Err(GfError::Validation(
                "project generation changed before graph publication".into(),
            ));
        }
        if !graphforge_storage::uuid_membership_index_is_fresh(&self.dir())? {
            graphforge_storage::rebuild_uuid_membership_indexes(
                &self.dir(),
                graphforge_storage::UuidIndexBuildLimits::default(),
            )?;
        }
        let graph = graphforge_storage::capture_graph_files(&self.dir())?.1;
        let provenance_enabled = parent.capability("provenance")?.is_some();
        let participants = graph_publication_participants(
            &parent,
            graph,
            self.semantic_storage_bindings
                .lock()
                .expect("semantic storage binding lock poisoned")
                .as_ref(),
            provenance_enabled,
            receipt,
            operation_uuid,
            None,
            recorded_at_micros,
        )?;
        let capabilities = parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect();
        let request = ProjectGenerationRequest {
            transaction_uuid: operation_uuid,
            generation_uuid,
            capabilities,
            participants,
        };
        let publication = match graphforge_storage::stage_project_generation_with_graph_tree_mode(
            root,
            &request,
            Some(self.dir().path()),
            self.lifecycle_mode,
        )? {
            ProjectStageOutcome::AlreadyPublished(receipt) => Ok(receipt),
            ProjectStageOutcome::Staged(staged) => staged
                .validate(
                    |_| Ok(()),
                    |actual_parent, _| {
                        if actual_parent.generation_uuid() != expected_parent {
                            return Err(GfError::Validation(
                                "project generation changed before graph publication".into(),
                            ));
                        }
                        Ok(())
                    },
                )?
                .publish(),
        };
        let published = match publication {
            Ok(receipt) => receipt,
            Err(error) => {
                if let Ok(current) = graphforge_storage::resolve_project_generation(root)
                    && current.generation_uuid() == generation_uuid
                {
                    self.install_property_generation(&current)?;
                }
                return Err(error);
            }
        };
        let committed = graphforge_storage::resolve_project_generation(root)?;
        if committed.generation_uuid() != published.generation_uuid {
            return Err(GfError::Storage(
                "published property authority did not resolve exact generation".into(),
            ));
        }
        self.install_property_generation(&committed)?;
        Ok(())
    }

    pub(super) fn publish_workspace_update(&self) -> Result<(), GfError> {
        self.publish_graph_mutation(&graphforge_exec::MutationReceipt::default())
    }

    /// Remove all nodes and edges (in-memory instances only).
    ///
    /// # Errors
    /// Returns [`GfError::Storage`] for persistent projects or if the in-memory
    /// project cannot be reset.
    pub fn clear(&self) -> Result<(), GfError> {
        let _graph_visibility = self.graph_visibility.lock()?;
        if self.path.is_some() {
            return Err(GfError::Storage(
                "clear is supported only for in-memory GraphForge instances".to_owned(),
            ));
        }

        let cleanup_result =
            self.adjacency_provider_for_session()
                .reset_graph(|| -> Result<(), GfError> {
                    let entries = std::fs::read_dir(self.dir()).map_err(|e| {
                        GfError::Storage(format!("failed to read in-memory project: {e}"))
                    })?;
                    let mut first_error = None;

                    for entry in entries {
                        let entry = match entry {
                            Ok(entry) => entry,
                            Err(error) => {
                                first_error.get_or_insert_with(|| {
                                    GfError::Storage(format!(
                                        "failed to inspect in-memory project entry: {error}"
                                    ))
                                });
                                continue;
                            }
                        };
                        let path = entry.path();
                        let file_type = match entry.file_type() {
                            Ok(file_type) => file_type,
                            Err(error) => {
                                first_error.get_or_insert_with(|| {
                                    GfError::Storage(format!(
                                        "failed to inspect in-memory project entry {}: {error}",
                                        path.display()
                                    ))
                                });
                                continue;
                            }
                        };
                        let result = if file_type.is_dir() && !file_type.is_symlink() {
                            std::fs::remove_dir_all(&path)
                        } else {
                            std::fs::remove_file(&path)
                        };
                        if let Err(error) = result {
                            first_error.get_or_insert_with(|| {
                                GfError::Storage(format!(
                                    "failed to remove in-memory project entry {}: {error}",
                                    path.display()
                                ))
                            });
                        }
                    }

                    first_error.map_or(Ok(()), Err)
                });

        // These registries describe the fixture, not only its remaining files.
        // Reset them even when filesystem cleanup is partial so callers never
        // observe a stale catalog or procedure registry after `clear()` returns.
        *self.runtime_catalog.lock().expect("runtime catalog lock") = RuntimeCatalog::new();
        self.procedures
            .lock()
            .expect("procedure registry lock")
            .clear();
        self.adjacency_provider_for_session().invalidate();
        cleanup_result
    }
}

pub(super) fn participant_encoding(
    value: &str,
) -> Result<graphforge_storage::ProjectParticipantEncoding, GfError> {
    match value {
        "parquet" => Ok(graphforge_storage::ProjectParticipantEncoding::Parquet),
        "arrow" => Ok(graphforge_storage::ProjectParticipantEncoding::Arrow),
        "json" => Ok(graphforge_storage::ProjectParticipantEncoding::Json),
        _ => Err(GfError::Validation(
            "committed participant has unsupported encoding".into(),
        )),
    }
}

#[allow(clippy::too_many_arguments)] // participant assembly carries authenticated audit context
fn graph_publication_participants(
    parent: &graphforge_storage::ResolvedProjectGeneration,
    graph: graphforge_storage::ProjectParticipant,
    semantic_bindings: Option<&graphforge_storage::SemanticStorageBindings>,
    provenance_enabled: bool,
    receipt: &graphforge_exec::MutationReceipt,
    operation_uuid: uuid::Uuid,
    actor_uuid: Option<uuid::Uuid>,
    recorded_at_micros: i64,
) -> Result<Vec<graphforge_storage::ProjectParticipant>, GfError> {
    let mut participants = parent
        .participant_snapshots()?
        .into_iter()
        .filter(|snapshot| {
            !(snapshot.capability_id == "graph"
                && matches!(
                    snapshot.record_family_id.as_str(),
                    "snapshot" | "files" | graphforge_storage::GRAPH_SEMANTIC_BINDINGS_FAMILY
                )
                || provenance_enabled
                    && snapshot.capability_id == "provenance"
                    && matches!(snapshot.record_family_id.as_str(), "events" | "lineage"))
        })
        .map(|snapshot| {
            Ok(graphforge_storage::ProjectParticipant {
                capability_id: snapshot.capability_id,
                capability_version: snapshot.capability_version,
                record_family_id: snapshot.record_family_id,
                record_version: snapshot.record_version,
                encoding: participant_encoding(&snapshot.encoding)?,
                schema_fingerprint: snapshot.schema_fingerprint,
                row_count: snapshot.row_count,
                bytes: snapshot.bytes,
            })
        })
        .collect::<Result<Vec<_>, GfError>>()?;
    participants.push(graph);
    if let Some(bindings) = semantic_bindings {
        participants.push(bindings.to_project_participant()?);
        let composition = participants
            .iter()
            .find(|participant| {
                participant.capability_id == "workspace"
                    && participant.record_family_id == "ontology_composition"
            })
            .ok_or_else(|| {
                GfError::Validation(
                    "semantic graph publication requires persisted composition authority".into(),
                )
            })?;
        let value: serde_json::Value = serde_json::from_slice(&composition.bytes)
            .map_err(|_| GfError::Validation("persisted composition is malformed".into()))?;
        if value
            .get("composition_fingerprint")
            .and_then(serde_json::Value::as_str)
            != Some(bindings.composition_fingerprint.as_str())
        {
            return Err(GfError::Validation(
                "semantic bindings and composition must publish at one fingerprint".into(),
            ));
        }
    }
    if provenance_enabled {
        participants.extend(provenance::merged_participants(
            parent,
            receipt,
            operation_uuid,
            actor_uuid,
            recorded_at_micros,
        )?);
    }
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    Ok(participants)
}

fn mutation_generation_uuid(
    operation_uuid: uuid::Uuid,
    participants: &[graphforge_storage::ProjectParticipant],
) -> uuid::Uuid {
    let mut hasher = Sha256::new();
    hasher.update(b"graphforge-graph-mutation-generation/1");
    hasher.update(operation_uuid.as_bytes());
    for participant in participants {
        hasher.update(participant.capability_id.as_bytes());
        hasher.update([0]);
        hasher.update(participant.record_family_id.as_bytes());
        hasher.update([0]);
        hasher.update(Sha256::digest(&participant.bytes));
    }
    let digest: [u8; 32] = hasher.finalize().into();
    graphforge_core::canonical::uuid_v8(digest)
}

pub(super) fn system_time_micros() -> Result<i64, GfError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| GfError::Execution("system clock is before Unix epoch".into()))?;
    i64::try_from(duration.as_micros())
        .map_err(|_| GfError::Execution("system clock exceeds microsecond range".into()))
}

/// Write the runtime catalog to `topology/runtime_catalog.parquet` so a later
/// `GraphForge::new(path)` reloads the types/properties observed this session
/// (#725). Best-effort directory creation; surfaces I/O / Parquet errors.
pub(super) fn persist_runtime_catalog(
    dir: &std::path::Path,
    rc: &RuntimeCatalog,
) -> Result<(), GfError> {
    graphforge_storage::runtime_entity_labels::persist_runtime_catalog(dir, rc)
}

#[cfg(test)]
mod tests;
