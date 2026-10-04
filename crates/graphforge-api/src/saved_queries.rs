//! Project-owned reusable read-only Cypher definitions and explicit execution.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use futures::StreamExt;
use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode};
use graphforge_storage::{
    ProjectCapability, ProjectGenerationRequest, ProjectParticipant, ProjectParticipantEncoding,
    ProjectParticipantSnapshot, ProjectStageOutcome, WORKSPACE_CAPABILITY_ID,
    WORKSPACE_SAVED_QUERIES_FAMILY, WorkspaceSavedQueries, read_workspace_saved_queries,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{CancellationToken, ExecutionResult, ExecutionStats, GraphForge, IrLiteral};
pub use graphforge_storage::{SavedQuery, SavedQueryParameterType};

/// Maximum collected Arrow array bytes returned by one saved query.
pub const MAX_SAVED_QUERY_RESULT_BYTES: usize = 64 * 1024 * 1024;
/// Maximum rows returned by one saved query; exceeding it fails without partial output.
pub const MAX_SAVED_QUERY_RESULT_ROWS: u64 = 1_000_000;

/// Choose both a definition revision and its graph execution context explicitly.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SavedQuerySource {
    /// Pin the current committed Project generation.
    #[default]
    Current,
    /// Read the definition and graph from one exact retained research Version.
    Version {
        /// Immutable research Version identity.
        version_uuid: Uuid,
    },
}

enum ParameterInput<'a> {
    Typed(&'a HashMap<String, IrLiteral>),
    Json(&'a HashMap<String, serde_json::Value>),
}

impl GraphForge {
    /// List current saved definitions in stable UUID order without executing them.
    pub fn saved_queries(&self) -> Result<Vec<SavedQuery>, GfError> {
        Ok(read_workspace_saved_queries(&self.generation_for_read()?)?
            .queries
            .into_values()
            .collect())
    }

    /// Inspect a current definition without executing it.
    pub fn saved_query(&self, query_uuid: Uuid) -> Result<SavedQuery, GfError> {
        read_workspace_saved_queries(&self.generation_for_read()?)?
            .queries
            .remove(&query_uuid)
            .ok_or_else(missing_query)
    }

    /// List exact current or historical definitions without executing them.
    pub fn saved_queries_at(&self, source: &SavedQuerySource) -> Result<Vec<SavedQuery>, GfError> {
        match source {
            SavedQuerySource::Current => self.saved_queries(),
            SavedQuerySource::Version { version_uuid } => {
                #[cfg(feature = "research")]
                {
                    self.open_research_version(*version_uuid)?.saved_queries()
                }
                #[cfg(not(feature = "research"))]
                {
                    let _ = version_uuid;
                    Err(research_unavailable())
                }
            }
        }
    }

    /// Inspect a definition from an explicitly chosen revision.
    pub fn saved_query_at(
        &self,
        query_uuid: Uuid,
        source: &SavedQuerySource,
    ) -> Result<SavedQuery, GfError> {
        match source {
            SavedQuerySource::Current => self.saved_query(query_uuid),
            SavedQuerySource::Version { version_uuid } => {
                #[cfg(feature = "research")]
                {
                    self.open_research_version(*version_uuid)?
                        .saved_query(query_uuid)
                }
                #[cfg(not(feature = "research"))]
                {
                    let _ = version_uuid;
                    Err(research_unavailable())
                }
            }
        }
    }

    /// Atomically create a definition; duplicate identities or names are refused.
    pub fn create_saved_query(&mut self, definition: SavedQuery) -> Result<SavedQuery, GfError> {
        self.change_saved_queries(|registry| {
            if registry.queries.contains_key(&definition.query_uuid) {
                return Err(invalid("saved query identity already exists"));
            }
            registry
                .queries
                .insert(definition.query_uuid, definition.clone());
            Ok(definition)
        })
    }

    /// Replace an existing definition atomically, preserving its identity.
    pub fn update_saved_query(&mut self, definition: SavedQuery) -> Result<SavedQuery, GfError> {
        self.change_saved_queries(|registry| {
            if !registry.queries.contains_key(&definition.query_uuid) {
                return Err(missing_query());
            }
            registry
                .queries
                .insert(definition.query_uuid, definition.clone());
            Ok(definition)
        })
    }

    /// Delete a current definition; retained Versions keep their own revision.
    pub fn delete_saved_query(&mut self, query_uuid: Uuid) -> Result<(), GfError> {
        self.change_saved_queries(|registry| {
            registry
                .queries
                .remove(&query_uuid)
                .map(|_| ())
                .ok_or_else(missing_query)
        })
    }

    /// Execute a saved definition against one pinned context with caller parameters.
    ///
    /// Definitions are never executed implicitly. Parameters must match the declared
    /// names and scalar types exactly. Native session resource policy applies, and
    /// collected results have finite row/byte ceilings. Cancellation or a bound
    /// failure returns an error rather than a partial result.
    pub fn execute_saved_query(
        &self,
        query_uuid: Uuid,
        params: &HashMap<String, IrLiteral>,
        source: &SavedQuerySource,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ExecutionResult, GfError> {
        self.run_saved_query(
            query_uuid,
            ParameterInput::Typed(params),
            source,
            cancellation,
        )
    }

    /// Execute plain JSON parameters using the saved declaration's numeric types.
    ///
    /// This is the shared native parameter boundary for Node and CLI. Whole JSON
    /// numbers can supply a declared float; floating representations can supply
    /// integers only when integral and within the exact JSON/JavaScript safe range.
    /// UUID values use a single `{"$uuid":"canonical UUID"}` tag.
    pub fn execute_saved_query_json(
        &self,
        query_uuid: Uuid,
        params: &HashMap<String, serde_json::Value>,
        source: &SavedQuerySource,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ExecutionResult, GfError> {
        self.run_saved_query(
            query_uuid,
            ParameterInput::Json(params),
            source,
            cancellation,
        )
    }

    fn run_saved_query(
        &self,
        query_uuid: Uuid,
        input: ParameterInput<'_>,
        source: &SavedQuerySource,
        cancellation: Option<&CancellationToken>,
    ) -> Result<ExecutionResult, GfError> {
        checkpoint(cancellation)?;
        let started = Instant::now();
        let mut view = match source {
            SavedQuerySource::Current => {
                let generation = self.generation_for_read()?;
                let mut view = Self::open_resolved_with_lifecycle_mode(
                    generation.container_root().to_path_buf(),
                    generation,
                    true,
                    self.lifecycle_mode,
                )?;
                // In-memory catalogs carry names that are intentionally not
                // persisted. Durable views hydrate the pinned generation's own
                // catalog, which can be newer than this caller's cached names.
                if self.path.is_none() {
                    view.runtime_catalog = Arc::new(Mutex::new(
                        self.runtime_catalog
                            .lock()
                            .map_err(|_| invalid("runtime catalog is unavailable"))?
                            .clone(),
                    ));
                }
                view.tempdir.clone_from(&self.tempdir);
                view.research_materialization
                    .clone_from(&self.research_materialization);
                view
            }
            SavedQuerySource::Version { version_uuid } => {
                #[cfg(feature = "research")]
                {
                    self.open_research_version(*version_uuid)?
                        .into_saved_query_graph()?
                }
                #[cfg(not(feature = "research"))]
                {
                    let _ = version_uuid;
                    return Err(research_unavailable());
                }
            }
        };
        // A private pinned facade must share the owner's resource policy and
        // admission, rather than granting a fresh default budget for each run.
        view.resource_policy.clone_from(&self.resource_policy);
        view.runtime = Arc::clone(&self.runtime);
        view.compute_pool = Arc::clone(&self.compute_pool);
        view.heavy_query_admission = Arc::clone(&self.heavy_query_admission);
        let definition = view.saved_query(query_uuid)?;
        definition.validate()?;
        let params = match input {
            ParameterInput::Typed(params) => Cow::Borrowed(params),
            ParameterInput::Json(params) => Cow::Owned(json_parameters(&definition, params)?),
        };
        validate_parameters(&definition, &params)?;
        checkpoint(cancellation)?;
        let mut stream = view.execute_stream_with_params(&definition.query, &params)?;
        let schema = stream.schema();
        let mut batches = Vec::new();
        let mut bytes = 0_usize;
        let mut rows = 0_u64;
        loop {
            checkpoint(cancellation)?;
            let next = view.block_on(async {
                loop {
                    tokio::select! {
                        batch = stream.next() => return Ok::<_, GfError>(batch),
                        () = tokio::time::sleep(std::time::Duration::from_millis(10)), if cancellation.is_some() => checkpoint(cancellation)?,
                    }
                }
            })?;
            let Some(batch) = next else { break };
            let batch = batch.map_err(GfError::from_execution_error)?;
            rows = rows
                .checked_add(batch.num_rows() as u64)
                .ok_or_else(result_limit)?;
            bytes = bytes
                .checked_add(batch.get_array_memory_size())
                .ok_or_else(result_limit)?;
            if rows > MAX_SAVED_QUERY_RESULT_ROWS || bytes > MAX_SAVED_QUERY_RESULT_BYTES {
                return Err(result_limit());
            }
            batches.push(batch);
        }
        checkpoint(cancellation)?;
        Ok(ExecutionResult {
            schema,
            batches,
            stats: ExecutionStats {
                rows_produced: rows,
                execution_time_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            },
            side_effects: None,
            mutation_receipt: None,
        })
    }

    fn change_saved_queries<T>(
        &mut self,
        change: impl FnOnce(&mut WorkspaceSavedQueries) -> Result<T, GfError>,
    ) -> Result<T, GfError> {
        if self.read_only {
            return Err(GfError::Project {
                code: ProjectErrorCode::ReadOnlyView,
                message: "saved-query changes require a writable Project".into(),
            });
        }
        self.require_private_workspace()?;
        let root = self.resolved_generation.container_root().to_path_buf();
        let parent = graphforge_storage::resolve_project_generation(&root)?;
        let expected_parent = *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned");
        if parent.generation_uuid() != expected_parent {
            return Err(conflict());
        }
        let mut registry = read_workspace_saved_queries(&parent)?;
        let result = change(&mut registry)?;
        registry.validate()?;
        let mut participants = parent
            .participant_snapshots()?
            .into_iter()
            .filter(|snapshot| {
                snapshot.capability_id != WORKSPACE_CAPABILITY_ID
                    || snapshot.record_family_id != WORKSPACE_SAVED_QUERIES_FAMILY
            })
            .map(snapshot_to_participant)
            .collect::<Result<Vec<_>, _>>()?;
        participants.push(registry.to_project_participant()?);
        let request = ProjectGenerationRequest {
            transaction_uuid: Uuid::now_v7(),
            generation_uuid: Uuid::now_v7(),
            capabilities: parent
                .capabilities()
                .into_iter()
                .map(|capability| ProjectCapability {
                    capability_id: capability.capability_id,
                    capability_version: capability.capability_version,
                })
                .collect(),
            participants,
        };
        let graph_objects = graphforge_storage::begin_graph_object_publication(&root)?;
        let receipt = match graphforge_storage::stage_project_generation(&root, &request)? {
            ProjectStageOutcome::AlreadyPublished(receipt) => receipt,
            ProjectStageOutcome::Staged(staged) => staged
                .validate(
                    |_| Ok(()),
                    |actual_parent, _| {
                        if actual_parent.generation_uuid() != expected_parent {
                            return Err(conflict());
                        }
                        Ok(())
                    },
                )?
                .publish_with_graph_objects(&graph_objects)?,
        };
        *self
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned") = receipt.generation_uuid;
        self.resolved_generation = graphforge_storage::resolve_project_generation(&root)?;
        Ok(result)
    }
}

fn validate_parameters(
    definition: &SavedQuery,
    params: &HashMap<String, IrLiteral>,
) -> Result<(), GfError> {
    if params.len() != definition.parameters.len() {
        return Err(invalid(
            "saved query requires exactly its declared parameters",
        ));
    }
    for (name, kind) in &definition.parameters {
        let valid = matches!(
            (kind, params.get(name)),
            (SavedQueryParameterType::Boolean, Some(IrLiteral::Bool(_)))
                | (SavedQueryParameterType::Integer, Some(IrLiteral::Int(_)))
                | (SavedQueryParameterType::String, Some(IrLiteral::Str(_)))
                | (SavedQueryParameterType::Uuid, Some(IrLiteral::Uuid(_)))
        ) || matches!((kind, params.get(name)), (SavedQueryParameterType::Float, Some(IrLiteral::Float(value))) if value.is_finite());
        if !valid {
            return Err(invalid(
                "saved query parameter is missing or has the wrong type",
            ));
        }
    }
    Ok(())
}

fn checkpoint(token: Option<&CancellationToken>) -> Result<(), GfError> {
    token.map_or(Ok(()), CancellationToken::checkpoint)
}

fn invalid(message: &str) -> GfError {
    GfError::Validation(message.into())
}

fn missing_query() -> GfError {
    GfError::Api {
        code: ApiErrorCode::NotFound,
        message: "saved query identity does not exist in the selected Project context".into(),
    }
}

fn conflict() -> GfError {
    GfError::Project {
        code: ProjectErrorCode::WriteConflict,
        message: "project generation changed before saved-query publication".into(),
    }
}

#[cfg(not(feature = "research"))]
fn research_unavailable() -> GfError {
    GfError::Project {
        code: ProjectErrorCode::CapabilityDisabled,
        message: "historical saved queries require the research feature".into(),
    }
}

fn json_parameters(
    definition: &SavedQuery,
    values: &HashMap<String, serde_json::Value>,
) -> Result<HashMap<String, IrLiteral>, GfError> {
    if values.len() != definition.parameters.len() {
        return Err(invalid(
            "saved query requires exactly its declared parameters",
        ));
    }
    definition
        .parameters
        .iter()
        .map(|(name, kind)| {
            let value = values
                .get(name)
                .ok_or_else(|| invalid("saved query parameter is missing"))?;
            let literal = match kind {
                SavedQueryParameterType::Boolean => value.as_bool().map(IrLiteral::Bool),
                SavedQueryParameterType::String => {
                    value.as_str().map(|v| IrLiteral::Str(v.to_owned()))
                }
                SavedQueryParameterType::Integer => {
                    value.as_i64().map(IrLiteral::Int).or_else(|| {
                        let number = value.as_f64()?;
                        if number.is_finite()
                            && number.fract() == 0.0
                            && number.abs() <= 9_007_199_254_740_991.0
                        {
                            #[allow(
                                clippy::cast_possible_truncation,
                                reason = "finite integral value is within the exact safe range"
                            )]
                            Some(IrLiteral::Int(number as i64))
                        } else {
                            None
                        }
                    })
                }
                SavedQueryParameterType::Float => value
                    .as_f64()
                    .filter(|v| v.is_finite())
                    .map(IrLiteral::Float),
                SavedQueryParameterType::Uuid => {
                    let tag = value.as_object().filter(|tag| tag.len() == 1);
                    tag.and_then(|tag| tag.get("$uuid"))
                        .and_then(serde_json::Value::as_str)
                        .and_then(|text| {
                            Uuid::parse_str(text)
                                .ok()
                                .filter(|id| id.hyphenated().to_string() == text)
                        })
                        .map(|id| IrLiteral::Uuid(*id.as_bytes()))
                }
            }
            .ok_or_else(|| invalid("saved query parameter has the wrong type or numeric range"))?;
            Ok((name.clone(), literal))
        })
        .collect()
}

fn result_limit() -> GfError {
    GfError::Project {
        code: ProjectErrorCode::ResourceLimit,
        message: "saved query result exceeds its row or byte ceiling".into(),
    }
}

fn snapshot_to_participant(
    snapshot: ProjectParticipantSnapshot,
) -> Result<ProjectParticipant, GfError> {
    let encoding = match snapshot.encoding.as_str() {
        "parquet" => ProjectParticipantEncoding::Parquet,
        "arrow" => ProjectParticipantEncoding::Arrow,
        "json" => ProjectParticipantEncoding::Json,
        _ => return Err(invalid("committed participant has unsupported encoding")),
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
