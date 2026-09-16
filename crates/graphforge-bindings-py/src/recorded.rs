//! Recorded Python binding methods and conversions.

use super::{
    GraphForge, PyCancellationToken, PyInvocationDescriptor, canonical_operation_id, hex_bytes,
    parse_algorithm_id, parse_terminal_uuids, py_to_node_selector, result_to_pyarrow,
    to_py_invocation_error, to_pyerr,
};
use graphforge_api::GfError;
use graphforge_api::WriteContext;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::Arc;

/// Result of one first-time recorded algorithm dispatch.
#[pyclass(name = "RecordedAlgorithmResult", module = "graphforge")]
pub struct PyRecordedAlgorithmResult {
    run_uuid: String,
    result: Py<PyAny>,
}

/// Opaque Rust-owned graph-only resolved-belief projection.
#[pyclass(name = "ResolvedBeliefProjection", module = "graphforge")]
pub struct PyResolvedBeliefProjection {
    inner: Arc<graphforge_api::ResolvedBeliefProjection>,
}

/// Result of a resolved recorded dispatch and its separate attachment outcome.
#[pyclass(name = "ResolvedRecordedAlgorithmResult", module = "graphforge")]
pub struct PyResolvedRecordedAlgorithmResult {
    run_uuid: String,
    result: Py<PyAny>,
    attachment_state: &'static str,
    attachment: Option<Py<PyAny>>,
    attachment_uuid: Option<String>,
    attachment_error_code: Option<String>,
}

#[pymethods]
impl PyRecordedAlgorithmResult {
    /// Durable run UUID.
    #[getter]
    fn run_uuid(&self) -> &str {
        &self.run_uuid
    }

    /// Canonical Arrow result table.
    #[getter]
    fn result(&self, py: Python<'_>) -> Py<PyAny> {
        self.result.clone_ref(py)
    }
}

#[pymethods]
impl PyResolvedRecordedAlgorithmResult {
    #[getter]
    fn run_uuid(&self) -> &str {
        &self.run_uuid
    }

    #[getter]
    fn result(&self, py: Python<'_>) -> Py<PyAny> {
        self.result.clone_ref(py)
    }

    #[getter]
    fn attachment_state(&self) -> &'static str {
        self.attachment_state
    }

    #[getter]
    fn attachment(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.attachment.as_ref().map(|value| value.clone_ref(py))
    }

    #[getter]
    fn attachment_uuid(&self) -> Option<&str> {
        self.attachment_uuid.as_deref()
    }

    #[getter]
    fn attachment_error_code(&self) -> Option<&str> {
        self.attachment_error_code.as_deref()
    }
}

#[pymethods]
impl PyResolvedBeliefProjection {
    #[getter]
    fn source_generation_uuid(&self) -> String {
        self.inner.source_generation_uuid().to_string()
    }

    #[getter]
    fn graph_content_fingerprint(&self) -> String {
        hex_bytes(&self.inner.graph_content_fingerprint())
    }

    #[getter]
    fn policy_fingerprint(&self) -> String {
        hex_bytes(&self.inner.policy_fingerprint())
    }

    #[getter]
    fn policy_bytes(&self, py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, self.inner.policy_bytes()).unbind()
    }

    #[getter]
    fn snapshot_fingerprint(&self) -> String {
        hex_bytes(&self.inner.snapshot_fingerprint())
    }

    #[getter]
    fn valid_time_fingerprint(&self) -> Option<String> {
        self.inner
            .valid_time_fingerprint()
            .map(|value| hex_bytes(&value))
    }

    #[getter]
    fn source_record_uuids(&self) -> Vec<String> {
        self.inner
            .source_record_uuids()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[getter]
    fn transaction_cutoff(&self) -> i64 {
        self.inner.transaction_cutoff_micros()
    }

    #[getter]
    fn valid_time(&self) -> Option<i64> {
        self.inner.valid_time_micros()
    }

    #[pyo3(signature = (label, *, by, via=None, directed=true))]
    fn prepare_rank_invocation(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        via: Option<&str>,
        directed: bool,
    ) -> PyResult<PyInvocationDescriptor> {
        let options = graphforge_api::RankOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            via: via.map(str::to_owned),
            directed,
            write_property: None,
        };
        let label = label.to_owned();
        py.detach(|| self.inner.prepare_rank_invocation(&label, &options))
            .map(|inner| PyInvocationDescriptor { inner })
            .map_err(|error| to_py_invocation_error(py, &error))
    }

    #[pyo3(signature = (label, *, by, vector_property=None, via=None, directed=false))]
    fn prepare_cluster_invocation(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        vector_property: Option<&str>,
        via: Option<&str>,
        directed: bool,
    ) -> PyResult<PyInvocationDescriptor> {
        let options = graphforge_api::ClusterOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            vector_property: vector_property.map(str::to_owned),
            via: via.map(str::to_owned),
            directed,
            write_property: None,
        };
        let label = label.to_owned();
        py.detach(|| self.inner.prepare_cluster_invocation(&label, &options))
            .map(|inner| PyInvocationDescriptor { inner })
            .map_err(|error| to_py_invocation_error(py, &error))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (source=None, target=None, *, by, via=None, directed=true, k=1, weight=None, capacity_property=None, cost_property=None, heuristic=None, walk_length=None, seed=None, terminal_uuids=None, prize_property=None))]
    fn prepare_paths_invocation(
        &self,
        py: Python<'_>,
        source: Option<&Bound<'_, PyAny>>,
        target: Option<&Bound<'_, PyAny>>,
        by: &str,
        via: Option<&str>,
        directed: bool,
        k: usize,
        weight: Option<&str>,
        capacity_property: Option<&str>,
        cost_property: Option<&str>,
        heuristic: Option<&str>,
        walk_length: Option<usize>,
        seed: Option<u64>,
        terminal_uuids: Option<Vec<String>>,
        prize_property: Option<&str>,
    ) -> PyResult<PyInvocationDescriptor> {
        let terminal_uuids = parse_terminal_uuids(&terminal_uuids.unwrap_or_default())
            .map_err(|error| to_pyerr(py, &error))?;
        let options = graphforge_api::PathsOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            via: via.map(str::to_owned),
            directed,
            k,
            weight: weight.map(str::to_owned),
            capacity_property: capacity_property.map(str::to_owned),
            cost_property: cost_property.map(str::to_owned),
            heuristic: heuristic.map(str::to_owned),
            walk_length,
            seed,
            terminal_uuids,
            prize_property: prize_property.map(str::to_owned),
        };
        let source = source
            .map(|value| py_to_node_selector(py, value))
            .transpose()?;
        let target = target
            .map(|value| py_to_node_selector(py, value))
            .transpose()?;
        py.detach(|| {
            self.inner
                .prepare_paths_invocation(source.as_ref(), target.as_ref(), &options)
        })
        .map(|inner| PyInvocationDescriptor { inner })
        .map_err(|error| to_py_invocation_error(py, &error))
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (label=None, *, by, via=None, directed=true, weight=None, partition_property=None, k=None))]
    fn prepare_analyze_invocation(
        &self,
        py: Python<'_>,
        label: Option<&str>,
        by: &str,
        via: Option<&str>,
        directed: bool,
        weight: Option<&str>,
        partition_property: Option<&str>,
        k: Option<usize>,
    ) -> PyResult<PyInvocationDescriptor> {
        let options = graphforge_api::AnalyzeOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            via: via.map(str::to_owned),
            directed,
            weight: weight.map(str::to_owned),
            k,
            partition_property: partition_property.map(str::to_owned),
        };
        let label = label.map(str::to_owned);
        py.detach(|| {
            self.inner
                .prepare_analyze_invocation(label.as_deref(), &options)
        })
        .map(|inner| PyInvocationDescriptor { inner })
        .map_err(|error| to_py_invocation_error(py, &error))
    }

    #[pyo3(signature = (label, *, by, k=10, vector_property=None, via=None))]
    fn prepare_similar_invocation(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        k: usize,
        vector_property: Option<&str>,
        via: Option<&str>,
    ) -> PyResult<PyInvocationDescriptor> {
        let options = graphforge_api::SimilarOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            k,
            vector_property: vector_property.map(str::to_owned),
            via: via.map(str::to_owned),
        };
        let label = label.to_owned();
        py.detach(|| self.inner.prepare_similar_invocation(&label, &options))
            .map(|inner| PyInvocationDescriptor { inner })
            .map_err(|error| to_py_invocation_error(py, &error))
    }
}

#[pymethods]
impl GraphForge {
    /// Durably record a run lifecycle around the unchanged descriptor dispatch.
    #[pyo3(signature = (*, operation_uuid, run_uuid, descriptor, actor_uuid=None, cancellation=None))]
    fn invoke_recorded(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        run_uuid: &str,
        descriptor: &PyInvocationDescriptor,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<PyRecordedAlgorithmResult> {
        let native = self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let run_uuid = canonical_operation_id(run_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|value| value.0);
        let cancellation = cancellation.map(|token| token.inner.clone());
        let descriptor = descriptor.inner.clone();
        let recorded = py
            .detach(|| {
                native.invoke_recorded(graphforge_api::RecordedAlgorithmRequest {
                    context: WriteContext {
                        operation_uuid,
                        actor_uuid,
                    },
                    run_uuid,
                    descriptor,
                    cancellation: cancellation.clone(),
                })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        Ok(PyRecordedAlgorithmResult {
            run_uuid: recorded.run_uuid.to_string(),
            result: result_to_pyarrow(py, &recorded.result)?,
        })
    }

    /// Resolve an explicit epistemic policy into an opaque graph-only projection.
    #[pyo3(signature = (*, transaction_cutoff, included_statuses, statusless, supersession_branches, hypotheses, valid_time=None))]
    #[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
    fn resolve_belief_projection(
        &self,
        py: Python<'_>,
        transaction_cutoff: i64,
        included_statuses: Vec<String>,
        statusless: &str,
        supersession_branches: &str,
        hypotheses: &str,
        valid_time: Option<i64>,
    ) -> PyResult<PyResolvedBeliefProjection> {
        let native = self.ensure_open()?;
        let policy = parse_belief_projection_policy(
            &included_statuses,
            statusless,
            supersession_branches,
            hypotheses,
        )
        .map_err(|error| to_pyerr(py, &error))?;
        py.detach(|| {
            native.resolve_belief_projection(graphforge_api::ResolveBeliefProjectionRequest {
                transaction_cutoff_micros: transaction_cutoff,
                valid_time_micros: valid_time,
                policy,
            })
        })
        .map(|inner| PyResolvedBeliefProjection {
            inner: Arc::new(inner),
        })
        .map_err(|error| to_pyerr(py, &error))
    }

    /// Execute one neutral descriptor on a resolved projection and record its attachment.
    #[pyo3(signature = (*, projection, operation_uuid, run_uuid, attachment_uuid, descriptor, actor_uuid=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn invoke_resolved_recorded(
        &self,
        py: Python<'_>,
        projection: &PyResolvedBeliefProjection,
        operation_uuid: &str,
        run_uuid: &str,
        attachment_uuid: &str,
        descriptor: &PyInvocationDescriptor,
        actor_uuid: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<PyResolvedRecordedAlgorithmResult> {
        let native = self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let attachment_uuid = parse(attachment_uuid)?.0;
        let requested_attachment_uuid = attachment_uuid.to_string();
        let cancellation = cancellation.map(|token| token.inner.clone());
        let projection = Arc::clone(&projection.inner);
        let descriptor = descriptor.inner.clone();
        let operation_uuid = parse(operation_uuid)?;
        let actor_uuid = actor_uuid.map(parse).transpose()?.map(|id| id.0);
        let run_uuid = parse(run_uuid)?.0;
        let result = py
            .detach(|| {
                native.invoke_resolved_recorded(
                    &projection,
                    graphforge_api::ResolvedRecordedAlgorithmRequest {
                        recorded: graphforge_api::RecordedAlgorithmRequest {
                            context: WriteContext {
                                operation_uuid,
                                actor_uuid,
                            },
                            run_uuid,
                            descriptor,
                            cancellation: cancellation.clone(),
                        },
                        attachment_uuid,
                    },
                )
            })
            .map_err(|error| to_pyerr(py, &error))?;
        resolved_recorded_result_to_python(py, result, requested_attachment_uuid)
    }

    /// Retry only the epistemic attachment for an already-completed knowledge run.
    #[pyo3(signature = (*, projection, operation_uuid, attachment_uuid, run_uuid, descriptor, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn attach_resolved_run(
        &self,
        py: Python<'_>,
        projection: &PyResolvedBeliefProjection,
        operation_uuid: &str,
        attachment_uuid: &str,
        run_uuid: &str,
        descriptor: &PyInvocationDescriptor,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let projection = Arc::clone(&projection.inner);
        let request = graphforge_api::AttachResolvedRunRequest {
            context: WriteContext {
                operation_uuid: parse(operation_uuid)?,
                actor_uuid: actor_uuid.map(parse).transpose()?.map(|id| id.0),
            },
            attachment_uuid: parse(attachment_uuid)?.0,
            run_uuid: parse(run_uuid)?.0,
            descriptor: descriptor.inner.clone(),
        };
        let result = py
            .detach(|| native.attach_resolved_run(&projection, request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one immutable algorithm-run identity.
    #[pyo3(signature = (run_uuid, *, cancellation=None))]
    fn algorithm_run(
        &self,
        py: Python<'_>,
        run_uuid: &str,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let run_uuid = canonical_operation_id(run_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| native.algorithm_run(run_uuid, cancellation.clone()))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic generation-bound run page.
    #[pyo3(signature = (*, algorithm=None, limit=100, after=None, cancellation=None))]
    fn list_algorithm_runs(
        &self,
        py: Python<'_>,
        algorithm: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let algorithm = algorithm
            .map(parse_algorithm_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                native.list_algorithm_runs(graphforge_api::ListAlgorithmRunsRequest {
                    algorithm,
                    page: graphforge_api::PageRequest {
                        limit,
                        after,
                        cancellation: cancellation.clone(),
                    },
                })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic generation-bound lifecycle page.
    #[pyo3(signature = (run_uuid, *, limit=100, after=None, cancellation=None))]
    fn algorithm_run_events(
        &self,
        py: Python<'_>,
        run_uuid: &str,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let run_uuid = canonical_operation_id(run_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                native.algorithm_run_events(
                    run_uuid,
                    graphforge_api::PageRequest {
                        limit,
                        after,
                        cancellation: cancellation.clone(),
                    },
                )
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }
}

fn parse_belief_projection_policy(
    included_statuses: &[String],
    statusless: &str,
    supersession_branches: &str,
    hypotheses: &str,
) -> Result<graphforge_api::BeliefProjectionPolicyV1, GfError> {
    let included_statuses = included_statuses
        .iter()
        .map(|status| match status.as_str() {
            "hypothesis" => Ok(graphforge_api::AssertionStatus::Hypothesis),
            "supported" => Ok(graphforge_api::AssertionStatus::Supported),
            "refuted" => Ok(graphforge_api::AssertionStatus::Refuted),
            "disputed" => Ok(graphforge_api::AssertionStatus::Disputed),
            "retracted" => Ok(graphforge_api::AssertionStatus::Retracted),
            "superseded" => Ok(graphforge_api::AssertionStatus::Superseded),
            _ => Err(GfError::Validation(
                "included_statuses contains an unknown status".into(),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let statusless = match statusless {
        "reject" => graphforge_api::StatuslessPolicyV1::Reject,
        "exclude" => graphforge_api::StatuslessPolicyV1::Exclude,
        "include" => graphforge_api::StatuslessPolicyV1::Include,
        _ => {
            return Err(GfError::Validation(
                "statusless must be reject, exclude, or include".into(),
            ));
        }
    };
    let supersession_branches = match supersession_branches {
        "reject" => graphforge_api::SupersessionBranchPolicyV1::Reject,
        "include_all_leaves" => graphforge_api::SupersessionBranchPolicyV1::IncludeAllLeaves,
        _ => {
            return Err(GfError::Validation(
                "supersession_branches must be reject or include_all_leaves".into(),
            ));
        }
    };
    let hypotheses = match hypotheses {
        "require_selected" => graphforge_api::HypothesisSelectionPolicyV1::RequireSelected,
        "exclude_unselected_group" => {
            graphforge_api::HypothesisSelectionPolicyV1::ExcludeUnselectedGroup
        }
        "include_all_current_members" => {
            graphforge_api::HypothesisSelectionPolicyV1::IncludeAllCurrentMembers
        }
        _ => {
            return Err(GfError::Validation(
                "hypotheses must be require_selected, exclude_unselected_group, or include_all_current_members".into(),
            ));
        }
    };
    Ok(graphforge_api::BeliefProjectionPolicyV1 {
        included_statuses,
        statusless,
        supersession_branches,
        hypotheses,
    })
}

fn resolved_recorded_result_to_python(
    py: Python<'_>,
    value: graphforge_api::ResolvedRecordedAlgorithmResult,
    requested_attachment_uuid: String,
) -> PyResult<PyResolvedRecordedAlgorithmResult> {
    let run_uuid = value.recorded.run_uuid.to_string();
    let result = result_to_pyarrow(py, &value.recorded.result)?;
    match value.attachment {
        graphforge_api::ResolvedAttachmentOutcome::Attached(attachment) => {
            Ok(PyResolvedRecordedAlgorithmResult {
                run_uuid,
                result,
                attachment_state: "attached",
                attachment: Some(result_to_pyarrow(py, &attachment)?),
                attachment_uuid: Some(requested_attachment_uuid),
                attachment_error_code: None,
            })
        }
        graphforge_api::ResolvedAttachmentOutcome::Failed {
            attachment_uuid,
            run_uuid: _,
            error_code,
        } => Ok(PyResolvedRecordedAlgorithmResult {
            run_uuid,
            result,
            attachment_state: "attachment_failed",
            attachment: None,
            attachment_uuid: Some(attachment_uuid.to_string()),
            attachment_error_code: Some(error_code),
        }),
    }
}
