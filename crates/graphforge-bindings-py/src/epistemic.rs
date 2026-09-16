//! Epistemic Python binding methods and conversions.

use super::{
    GraphForge, PyCancellationToken, assertion_status, canonical_operation_id, result_to_pyarrow,
    to_pyerr,
};
use graphforge_api::GfError;
use graphforge_api::WriteContext;
use pyo3::prelude::*;

#[pymethods]
impl GraphForge {
    /// Atomically append one immutable epistemic reasoning record.
    #[pyo3(signature = (*, operation_uuid, reasoning_uuid, assertion_uuid, kind, content_format, content, provenance_uuid, supersedes_reasoning_uuid=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn record_reasoning(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        reasoning_uuid: &str,
        assertion_uuid: &str,
        kind: &str,
        content_format: &str,
        content: Vec<u8>,
        provenance_uuid: &str,
        supersedes_reasoning_uuid: Option<&str>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let reasoning_uuid = canonical_operation_id(reasoning_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let provenance_uuid = canonical_operation_id(provenance_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let supersedes_reasoning_uuid = supersedes_reasoning_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|value| value.0);
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|value| value.0);
        let kind = match kind {
            "evidence_interpretation" => graphforge_api::ReasoningKind::EvidenceInterpretation,
            "logical_inference" => graphforge_api::ReasoningKind::LogicalInference,
            "methodological_note" => graphforge_api::ReasoningKind::MethodologicalNote,
            "decision_rationale" => graphforge_api::ReasoningKind::DecisionRationale,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("unknown reasoning kind".into()),
                ));
            }
        };
        let content_format = match content_format {
            "text/plain" => graphforge_api::ReasoningContentFormat::TextPlain,
            "text/markdown" => graphforge_api::ReasoningContentFormat::TextMarkdown,
            "application/json" => graphforge_api::ReasoningContentFormat::ApplicationJson,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("unknown reasoning content format".into()),
                ));
            }
        };
        let result = py
            .detach(|| {
                self.inner
                    .record_reasoning(graphforge_api::RecordReasoningRequest {
                        context: WriteContext {
                            operation_uuid,
                            actor_uuid,
                        },
                        reasoning_uuid,
                        assertion_uuid,
                        kind,
                        content_format,
                        content,
                        supersedes_reasoning_uuid,
                        provenance_uuid,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one exact immutable reasoning record.
    #[pyo3(signature = (reasoning_uuid, *, cancellation=None))]
    fn reasoning(
        &self,
        py: Python<'_>,
        reasoning_uuid: &str,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let reasoning_uuid = canonical_operation_id(reasoning_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| self.inner.reasoning(reasoning_uuid, cancellation.clone()))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic immutable reasoning history.
    #[pyo3(signature = (*, assertion_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_reasoning(
        &self,
        py: Python<'_>,
        assertion_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = assertion_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|value| value.0);
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .list_reasoning(graphforge_api::ListReasoningRequest {
                        assertion_uuid,
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

    /// Append one explicit assertion-status event.
    #[pyo3(signature = (*, operation_uuid, status_event_uuid, assertion_uuid, status, provenance_uuid, confidence_uuid=None, reasoning_uuid=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn record_assertion_status(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        status_event_uuid: &str,
        assertion_uuid: &str,
        status: &str,
        provenance_uuid: &str,
        confidence_uuid: Option<&str>,
        reasoning_uuid: Option<&str>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse = |value: &str| {
            canonical_operation_id(value)
                .map(|id| id.0)
                .map_err(|error| to_pyerr(py, &error))
        };
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let request = graphforge_api::RecordAssertionStatusRequest {
            context: WriteContext {
                operation_uuid,
                actor_uuid,
            },
            status_event_uuid: parse(status_event_uuid)?,
            assertion_uuid: parse(assertion_uuid)?,
            status: assertion_status(status).map_err(|error| to_pyerr(py, &error))?,
            confidence_uuid: confidence_uuid.map(parse).transpose()?,
            reasoning_uuid: reasoning_uuid.map(parse).transpose()?,
            provenance_uuid: parse(provenance_uuid)?,
        };
        let result = py
            .detach(|| self.inner.record_assertion_status(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return the current explicit status or an empty Arrow table when statusless.
    fn assertion_status(&self, py: Python<'_>, assertion_uuid: &str) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let result = py
            .detach(|| self.inner.assertion_status(assertion_uuid))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic append-only assertion-status history.
    #[pyo3(signature = (*, assertion_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_assertion_status(
        &self,
        py: Python<'_>,
        assertion_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = assertion_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .list_assertion_status(graphforge_api::ListAssertionStatusRequest {
                        assertion_uuid,
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

    /// Atomically append one assertion supersession and paired terminal status.
    #[pyo3(signature = (*, operation_uuid, supersession_uuid, prior_assertion_uuid, replacement_assertion_uuid, status_event_uuid, reasoning_uuid, provenance_uuid, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn supersede_assertion(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        supersession_uuid: &str,
        prior_assertion_uuid: &str,
        replacement_assertion_uuid: &str,
        status_event_uuid: &str,
        reasoning_uuid: &str,
        provenance_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse = |value: &str| {
            canonical_operation_id(value)
                .map(|id| id.0)
                .map_err(|error| to_pyerr(py, &error))
        };
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let request = graphforge_api::SupersedeAssertionRequest {
            context: WriteContext {
                operation_uuid,
                actor_uuid,
            },
            supersession_uuid: parse(supersession_uuid)?,
            prior_assertion_uuid: parse(prior_assertion_uuid)?,
            replacement_assertion_uuid: parse(replacement_assertion_uuid)?,
            status_event_uuid: parse(status_event_uuid)?,
            reasoning_uuid: parse(reasoning_uuid)?,
            provenance_uuid: parse(provenance_uuid)?,
        };
        let result = py
            .detach(|| self.inner.supersede_assertion(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic branch-preserving assertion-supersession history.
    #[pyo3(signature = (*, prior_assertion_uuid=None, replacement_assertion_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_assertion_supersessions(
        &self,
        py: Python<'_>,
        prior_assertion_uuid: Option<&str>,
        replacement_assertion_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse_optional = |value: Option<&str>| {
            value
                .map(canonical_operation_id)
                .transpose()
                .map(|value| value.map(|id| id.0))
                .map_err(|error| to_pyerr(py, &error))
        };
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let request = graphforge_api::ListAssertionSupersessionsRequest {
            prior_assertion_uuid: parse_optional(prior_assertion_uuid)?,
            replacement_assertion_uuid: parse_optional(replacement_assertion_uuid)?,
            page: graphforge_api::PageRequest {
                limit,
                after,
                cancellation,
            },
        };
        let result = py
            .detach(|| self.inner.list_assertion_supersessions(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Create one immutable hypothesis group.
    #[pyo3(signature = (*, operation_uuid, group_uuid, question_key, provenance_uuid, actor_uuid=None))]
    fn create_hypothesis_group(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        group_uuid: &str,
        question_key: String,
        provenance_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let request = graphforge_api::CreateHypothesisGroupRequest {
            context: WriteContext {
                operation_uuid: parse(operation_uuid)?,
                actor_uuid: actor_uuid.map(parse).transpose()?.map(|id| id.0),
            },
            group_uuid: parse(group_uuid)?.0,
            question_key,
            provenance_uuid: parse(provenance_uuid)?.0,
        };
        let result = py
            .detach(|| self.inner.create_hypothesis_group(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Append one explicit hypothesis-membership event.
    #[pyo3(signature = (*, operation_uuid, membership_event_uuid, group_uuid, assertion_uuid, action, reasoning_uuid, provenance_uuid, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn record_hypothesis_membership(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        membership_event_uuid: &str,
        group_uuid: &str,
        assertion_uuid: &str,
        action: &str,
        reasoning_uuid: &str,
        provenance_uuid: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let action = match action {
            "added" => graphforge_api::HypothesisMembershipAction::Added,
            "removed" => graphforge_api::HypothesisMembershipAction::Removed,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("action must be 'added' or 'removed'".into()),
                ));
            }
        };
        let request = graphforge_api::RecordHypothesisMembershipRequest {
            context: WriteContext {
                operation_uuid: parse(operation_uuid)?,
                actor_uuid: actor_uuid.map(parse).transpose()?.map(|id| id.0),
            },
            membership_event_uuid: parse(membership_event_uuid)?.0,
            group_uuid: parse(group_uuid)?.0,
            assertion_uuid: parse(assertion_uuid)?.0,
            action,
            reasoning_uuid: parse(reasoning_uuid)?.0,
            provenance_uuid: parse(provenance_uuid)?.0,
        };
        let result = py
            .detach(|| self.inner.record_hypothesis_membership(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Append one explicit hypothesis selection or clear event.
    #[pyo3(signature = (*, operation_uuid, selection_event_uuid, group_uuid, reasoning_uuid, provenance_uuid, selected_assertion_uuid=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn record_hypothesis_selection(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        selection_event_uuid: &str,
        group_uuid: &str,
        reasoning_uuid: &str,
        provenance_uuid: &str,
        selected_assertion_uuid: Option<&str>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let request = graphforge_api::RecordHypothesisSelectionRequest {
            context: WriteContext {
                operation_uuid: parse(operation_uuid)?,
                actor_uuid: actor_uuid.map(parse).transpose()?.map(|id| id.0),
            },
            selection_event_uuid: parse(selection_event_uuid)?.0,
            group_uuid: parse(group_uuid)?.0,
            selected_assertion_uuid: selected_assertion_uuid
                .map(parse)
                .transpose()?
                .map(|id| id.0),
            reasoning_uuid: parse(reasoning_uuid)?.0,
            provenance_uuid: parse(provenance_uuid)?.0,
        };
        let result = py
            .detach(|| self.inner.record_hypothesis_selection(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Atomically remove one member and explicitly change or clear selection.
    #[pyo3(signature = (*, operation_uuid, membership_event_uuid, selection_event_uuid, group_uuid, assertion_uuid, reasoning_uuid, provenance_uuid, selected_assertion_uuid=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn remove_hypothesis_member(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        membership_event_uuid: &str,
        selection_event_uuid: &str,
        group_uuid: &str,
        assertion_uuid: &str,
        reasoning_uuid: &str,
        provenance_uuid: &str,
        selected_assertion_uuid: Option<&str>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let request = graphforge_api::RemoveHypothesisMemberRequest {
            context: WriteContext {
                operation_uuid: parse(operation_uuid)?,
                actor_uuid: actor_uuid.map(parse).transpose()?.map(|id| id.0),
            },
            membership_event_uuid: parse(membership_event_uuid)?.0,
            selection_event_uuid: parse(selection_event_uuid)?.0,
            group_uuid: parse(group_uuid)?.0,
            assertion_uuid: parse(assertion_uuid)?.0,
            selected_assertion_uuid: selected_assertion_uuid
                .map(parse)
                .transpose()?
                .map(|id| id.0),
            reasoning_uuid: parse(reasoning_uuid)?.0,
            provenance_uuid: parse(provenance_uuid)?.0,
        };
        let result = py
            .detach(|| self.inner.remove_hypothesis_member(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic hypothesis-group history.
    #[pyo3(signature = (*, question_key=None, limit=100, after=None, cancellation=None))]
    fn list_hypothesis_groups(
        &self,
        py: Python<'_>,
        question_key: Option<String>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let request = graphforge_api::ListHypothesisGroupsRequest {
            question_key,
            page: graphforge_api::PageRequest {
                limit,
                after: after
                    .map(graphforge_api::PageToken::parse)
                    .transpose()
                    .map_err(|error| to_pyerr(py, &error))?,
                cancellation: cancellation.map(|token| token.inner.clone()),
            },
        };
        let result = py
            .detach(|| self.inner.list_hypothesis_groups(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic hypothesis-membership history.
    #[pyo3(signature = (*, group_uuid=None, assertion_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_hypothesis_membership(
        &self,
        py: Python<'_>,
        group_uuid: Option<&str>,
        assertion_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse = |value: &str| {
            canonical_operation_id(value)
                .map(|id| id.0)
                .map_err(|error| to_pyerr(py, &error))
        };
        let request = graphforge_api::ListHypothesisMembershipRequest {
            group_uuid: group_uuid.map(parse).transpose()?,
            assertion_uuid: assertion_uuid.map(parse).transpose()?,
            page: graphforge_api::PageRequest {
                limit,
                after: after
                    .map(graphforge_api::PageToken::parse)
                    .transpose()
                    .map_err(|error| to_pyerr(py, &error))?,
                cancellation: cancellation.map(|token| token.inner.clone()),
            },
        };
        let result = py
            .detach(|| self.inner.list_hypothesis_membership(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic hypothesis-selection history.
    #[pyo3(signature = (*, group_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_hypothesis_selection(
        &self,
        py: Python<'_>,
        group_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let group_uuid = group_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let request = graphforge_api::ListHypothesisSelectionRequest {
            group_uuid,
            page: graphforge_api::PageRequest {
                limit,
                after: after
                    .map(graphforge_api::PageToken::parse)
                    .transpose()
                    .map_err(|error| to_pyerr(py, &error))?,
                cancellation: cancellation.map(|token| token.inner.clone()),
            },
        };
        let result = py
            .detach(|| self.inner.list_hypothesis_selection(&request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return current hypothesis members.
    fn hypothesis_members(&self, py: Python<'_>, group_uuid: &str) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let group_uuid = canonical_operation_id(group_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let result = py
            .detach(|| self.inner.hypothesis_members(group_uuid))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return the current explicit hypothesis selection.
    fn hypothesis_selection(&self, py: Python<'_>, group_uuid: &str) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let group_uuid = canonical_operation_id(group_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let result = py
            .detach(|| self.inner.hypothesis_selection(group_uuid))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Reconstruct one deterministic epistemic transaction-time snapshot.
    #[pyo3(signature = (*, transaction_cutoff))]
    fn epistemic_snapshot(&self, py: Python<'_>, transaction_cutoff: i64) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let result = py
            .detach(|| self.inner.epistemic_snapshot(transaction_cutoff))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Append one immutable assertion valid-time event.
    #[pyo3(signature = (*, operation_uuid, validity_event_uuid, assertion_uuid, provenance_uuid, valid_from=None, valid_to=None, reasoning_uuid=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn record_assertion_validity(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        validity_event_uuid: &str,
        assertion_uuid: &str,
        provenance_uuid: &str,
        valid_from: Option<i64>,
        valid_to: Option<i64>,
        reasoning_uuid: Option<&str>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let parse =
            |value: &str| canonical_operation_id(value).map_err(|error| to_pyerr(py, &error));
        let request = graphforge_api::RecordAssertionValidityRequest {
            context: WriteContext {
                operation_uuid: parse(operation_uuid)?,
                actor_uuid: actor_uuid.map(parse).transpose()?.map(|id| id.0),
            },
            validity_event_uuid: parse(validity_event_uuid)?.0,
            assertion_uuid: parse(assertion_uuid)?.0,
            valid_from_micros: valid_from,
            valid_to_micros: valid_to,
            reasoning_uuid: reasoning_uuid.map(parse).transpose()?.map(|id| id.0),
            provenance_uuid: parse(provenance_uuid)?.0,
        };
        let result = py
            .detach(|| self.inner.record_assertion_validity(request))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return deterministic append-only assertion validity history.
    #[pyo3(signature = (*, assertion_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_assertion_validity(
        &self,
        py: Python<'_>,
        assertion_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = assertion_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .list_assertion_validity(graphforge_api::ListAssertionValidityRequest {
                        assertion_uuid,
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

    /// Apply valid time after resolving the mandatory transaction-time cutoff.
    #[pyo3(signature = (*, transaction_cutoff, valid_time))]
    fn apply_valid_time(
        &self,
        py: Python<'_>,
        transaction_cutoff: i64,
        valid_time: i64,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let result = py
            .detach(|| {
                self.inner
                    .apply_valid_time(graphforge_api::ApplyValidTimeRequest {
                        transaction_cutoff_micros: transaction_cutoff,
                        valid_time_micros: valid_time,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }
}
