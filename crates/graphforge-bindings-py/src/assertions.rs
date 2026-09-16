//! Assertions Python binding methods and conversions.

use super::{GraphForge, PyCancellationToken, canonical_operation_id, result_to_pyarrow, to_pyerr};
use graphforge_api::CapabilityId;
use graphforge_api::GfError;
use graphforge_api::OperationId;
use graphforge_api::WriteContext;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::types::PyList;

pub(super) fn py_operation_id(value: &Bound<'_, PyAny>) -> Result<OperationId, GfError> {
    let value = value
        .str()
        .map_err(|error| GfError::Validation(error.to_string()))?;
    canonical_operation_id(
        value
            .to_str()
            .map_err(|error| GfError::Validation(error.to_string()))?,
    )
}

pub(super) fn assertion_status(value: &str) -> Result<graphforge_api::AssertionStatus, GfError> {
    match value {
        "hypothesis" => Ok(graphforge_api::AssertionStatus::Hypothesis),
        "supported" => Ok(graphforge_api::AssertionStatus::Supported),
        "refuted" => Ok(graphforge_api::AssertionStatus::Refuted),
        "disputed" => Ok(graphforge_api::AssertionStatus::Disputed),
        "retracted" => Ok(graphforge_api::AssertionStatus::Retracted),
        "superseded" => Ok(graphforge_api::AssertionStatus::Superseded),
        _ => Err(GfError::Validation("unknown assertion status".into())),
    }
}

fn parse_capability_id(value: &str) -> Result<CapabilityId, GfError> {
    match value {
        "graph" => Ok(CapabilityId::Graph),
        "provenance" => Ok(CapabilityId::Provenance),
        "knowledge" => Ok(CapabilityId::Knowledge),
        "epistemic" => Ok(CapabilityId::Epistemic),
        "valid_time" => Ok(CapabilityId::ValidTime),
        _ => Err(GfError::Validation(format!("unknown capability {value:?}"))),
    }
}

fn py_assertion_graph_ref(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<graphforge_api::AssertionGraphRefInput> {
    let value = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("graph_refs entries must be dictionaries"))?;
    let field = |name: &str| {
        value
            .get_item(name)?
            .ok_or_else(|| PyTypeError::new_err(format!("graph_refs entry requires {name}")))
    };
    let graph_uuid = canonical_operation_id(&field("graph_uuid")?.extract::<String>()?)
        .map_err(|error| to_pyerr(py, &error))?
        .0;
    let graph_kind = match field("graph_kind")?.extract::<String>()?.as_str() {
        "node" => graphforge_api::GraphObjectKind::Node,
        "edge" => graphforge_api::GraphObjectKind::Edge,
        _ => {
            return Err(PyTypeError::new_err("graph_kind must be 'node' or 'edge'"));
        }
    };
    let role = match field("role")?.extract::<String>()?.as_str() {
        "subject" => graphforge_api::AssertionGraphRole::Subject,
        "object" => graphforge_api::AssertionGraphRole::Object,
        "context" => graphforge_api::AssertionGraphRole::Context,
        _ => {
            return Err(PyTypeError::new_err(
                "role must be 'subject', 'object', or 'context'",
            ));
        }
    };
    let ordinal = field("ordinal")?.extract::<u32>()?;
    Ok(graphforge_api::AssertionGraphRefInput {
        graph_uuid,
        graph_kind,
        role,
        ordinal,
    })
}

fn py_evidence_input(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<graphforge_api::EvidenceInput> {
    let value = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("evidence entries must be dictionaries"))?;
    let field = |name: &str| {
        value
            .get_item(name)?
            .ok_or_else(|| PyTypeError::new_err(format!("evidence entry requires {name}")))
    };
    let evidence_uuid = canonical_operation_id(&field("evidence_uuid")?.extract::<String>()?)
        .map_err(|error| to_pyerr(py, &error))?
        .0;
    let source_uuid = canonical_operation_id(&field("source_uuid")?.extract::<String>()?)
        .map_err(|error| to_pyerr(py, &error))?
        .0;
    let source_kind = match field("source_kind")?.extract::<String>()?.as_str() {
        "document" => graphforge_api::EvidenceSourceKind::Document,
        "observation" => graphforge_api::EvidenceSourceKind::Observation,
        "graph_node" => graphforge_api::EvidenceSourceKind::GraphNode,
        "graph_edge" => graphforge_api::EvidenceSourceKind::GraphEdge,
        _ => return Err(PyTypeError::new_err("unknown evidence source kind")),
    };
    let role = match field("role")?.extract::<String>()?.as_str() {
        "supports" => graphforge_api::EvidenceRole::Supports,
        "contradicts" => graphforge_api::EvidenceRole::Contradicts,
        "context" => graphforge_api::EvidenceRole::Context,
        _ => return Err(PyTypeError::new_err("unknown evidence role")),
    };
    let weight = value
        .get_item("weight")?
        .filter(|value| !value.is_none())
        .map(|value| value.extract::<f64>())
        .transpose()?;
    Ok(graphforge_api::EvidenceInput {
        evidence_uuid,
        source_uuid,
        source_kind,
        role,
        weight,
    })
}

#[pymethods]
impl GraphForge {
    /// Atomically enable one registered project capability.
    #[pyo3(signature = (*, operation_uuid, capability_id, capability_version, actor_uuid=None))]
    fn enable_capability(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        capability_id: &str,
        capability_version: u32,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let capability_id =
            parse_capability_id(capability_id).map_err(|error| to_pyerr(py, &error))?;
        let result = py
            .detach(|| {
                self.inner
                    .enable_capability(graphforge_api::EnableCapabilityRequest {
                        context: WriteContext {
                            operation_uuid,
                            actor_uuid,
                        },
                        capability_id,
                        capability_version,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one exact provenance event as a `pyarrow.Table`.
    #[pyo3(signature = (provenance_uuid, *, cancellation=None))]
    fn provenance_event(
        &self,
        py: Python<'_>,
        provenance_uuid: &str,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let provenance_uuid = canonical_operation_id(provenance_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .provenance_event(provenance_uuid, cancellation.clone())
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic generation-bound provenance history page.
    #[pyo3(signature = (*, subject_uuid=None, operation_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_provenance_history(
        &self,
        py: Python<'_>,
        subject_uuid: Option<&str>,
        operation_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let subject_uuid = subject_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let operation_uuid = operation_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .list_provenance_history(graphforge_api::ProvenanceHistoryRequest {
                        subject_uuid,
                        operation_uuid,
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

    /// Atomically create one immutable analytical assertion.
    #[pyo3(signature = (*, operation_uuid, assertion_uuid, claim, graph_refs, actor_uuid=None))]
    fn create_assertion(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        assertion_uuid: &str,
        claim: String,
        graph_refs: &Bound<'_, PyList>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let graph_refs = graph_refs
            .iter()
            .map(|value| py_assertion_graph_ref(py, &value))
            .collect::<PyResult<Vec<_>>>()?;
        let result = py
            .detach(|| {
                self.inner
                    .create_assertion(graphforge_api::CreateAssertionRequest {
                        context: WriteContext {
                            operation_uuid,
                            actor_uuid,
                        },
                        assertion_uuid,
                        claim,
                        graph_refs,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Atomically create one assertion and a non-empty evidence bundle.
    #[pyo3(signature = (*, operation_uuid, assertion_uuid, claim, graph_refs, evidence, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn create_assertion_with_evidence(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        assertion_uuid: &str,
        claim: String,
        graph_refs: &Bound<'_, PyList>,
        evidence: &Bound<'_, PyList>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let graph_refs = graph_refs
            .iter()
            .map(|value| py_assertion_graph_ref(py, &value))
            .collect::<PyResult<Vec<_>>>()?;
        let evidence = evidence
            .iter()
            .map(|value| py_evidence_input(py, &value))
            .collect::<PyResult<Vec<_>>>()?;
        let result = py
            .detach(|| {
                self.inner.create_assertion_with_evidence(
                    graphforge_api::CreateAssertionWithEvidenceRequest {
                        assertion: graphforge_api::CreateAssertionRequest {
                            context: WriteContext {
                                operation_uuid,
                                actor_uuid,
                            },
                            assertion_uuid,
                            claim,
                            graph_refs,
                        },
                        evidence,
                    },
                )
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Atomically create one assertion and its first explicit status.
    #[pyo3(signature = (*, operation_uuid, assertion_uuid, claim, graph_refs, status_event_uuid, status, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn create_assertion_with_status(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        assertion_uuid: &str,
        claim: String,
        graph_refs: &Bound<'_, PyList>,
        status_event_uuid: &str,
        status: &str,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let status_event_uuid = canonical_operation_id(status_event_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let status = assertion_status(status).map_err(|error| to_pyerr(py, &error))?;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let graph_refs = graph_refs
            .iter()
            .map(|value| py_assertion_graph_ref(py, &value))
            .collect::<PyResult<Vec<_>>>()?;
        let result = py
            .detach(|| {
                self.inner.create_assertion_with_status(
                    graphforge_api::CreateAssertionWithStatusRequest {
                        assertion: graphforge_api::CreateAssertionRequest {
                            context: WriteContext {
                                operation_uuid,
                                actor_uuid,
                            },
                            assertion_uuid,
                            claim,
                            graph_refs,
                        },
                        first_status: graphforge_api::FirstAssertionStatusInput {
                            status_event_uuid,
                            status,
                        },
                    },
                )
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one exact immutable assertion.
    #[pyo3(signature = (assertion_uuid, *, cancellation=None))]
    fn assertion(
        &self,
        py: Python<'_>,
        assertion_uuid: &str,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| self.inner.assertion(assertion_uuid, cancellation.clone()))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic generation-bound assertion page.
    #[pyo3(signature = (*, graph_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_assertions(
        &self,
        py: Python<'_>,
        graph_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let graph_uuid = graph_uuid
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
                    .list_assertions(graphforge_api::ListAssertionsRequest {
                        graph_uuid,
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

    /// Return one assertion's graph references in canonical order.
    #[pyo3(signature = (assertion_uuid, *, limit=100, after=None, cancellation=None))]
    fn assertion_graph_refs(
        &self,
        py: Python<'_>,
        assertion_uuid: &str,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner.assertion_graph_refs(
                    assertion_uuid,
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

    /// Atomically record one immutable confidence assessment.
    #[pyo3(signature = (*, operation_uuid, confidence_uuid, assertion_uuid, policy, value=None, input_confidence_uuids=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn assess_confidence(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        confidence_uuid: &str,
        assertion_uuid: &str,
        policy: &str,
        value: Option<f64>,
        input_confidence_uuids: Option<Vec<String>>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let confidence_uuid = canonical_operation_id(confidence_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let policy = match policy {
            "explicit" => graphforge_api::ConfidencePolicyRequest::Explicit {
                value: value.ok_or_else(|| {
                    to_pyerr(py, &GfError::Validation("explicit requires value".into()))
                })?,
            },
            "conservative_min" => {
                if value.is_some() {
                    return Err(to_pyerr(
                        py,
                        &GfError::Validation(
                            "conservative_min does not accept explicit value".into(),
                        ),
                    ));
                }
                let input_confidence_uuids = input_confidence_uuids
                    .unwrap_or_default()
                    .iter()
                    .map(|value| {
                        canonical_operation_id(value)
                            .map(|id| id.0)
                            .map_err(|error| to_pyerr(py, &error))
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                graphforge_api::ConfidencePolicyRequest::ConservativeMin {
                    input_confidence_uuids,
                }
            }
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("unknown confidence policy".into()),
                ));
            }
        };
        let result = py
            .detach(|| {
                self.inner
                    .assess_confidence(graphforge_api::AssessConfidenceRequest {
                        context: WriteContext {
                            operation_uuid,
                            actor_uuid,
                        },
                        confidence_uuid,
                        assertion_uuid,
                        policy,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one exact immutable confidence assessment.
    #[pyo3(signature = (confidence_uuid, *, cancellation=None))]
    fn confidence_assessment(
        &self,
        py: Python<'_>,
        confidence_uuid: &str,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let confidence_uuid = canonical_operation_id(confidence_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .confidence_assessment(confidence_uuid, cancellation.clone())
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic generation-bound confidence page.
    #[pyo3(signature = (*, assertion_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_confidence_assessments(
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
                self.inner.list_confidence_assessments(
                    graphforge_api::ListConfidenceAssessmentsRequest {
                        assertion_uuid,
                        page: graphforge_api::PageRequest {
                            limit,
                            after,
                            cancellation: cancellation.clone(),
                        },
                    },
                )
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one assessment's immutable normalized input snapshot.
    #[pyo3(signature = (confidence_uuid, *, limit=100, after=None, cancellation=None))]
    fn confidence_inputs(
        &self,
        py: Python<'_>,
        confidence_uuid: &str,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let confidence_uuid = canonical_operation_id(confidence_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner.confidence_inputs(
                    confidence_uuid,
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

    /// Atomically attach one immutable evidence link.
    #[pyo3(signature = (*, operation_uuid, evidence_uuid, assertion_uuid, source_uuid, source_kind, role, weight=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn attach_evidence(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        evidence_uuid: &str,
        assertion_uuid: &str,
        source_uuid: &str,
        source_kind: &str,
        role: &str,
        weight: Option<f64>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let operation_uuid =
            canonical_operation_id(operation_uuid).map_err(|error| to_pyerr(py, &error))?;
        let evidence_uuid = canonical_operation_id(evidence_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let assertion_uuid = canonical_operation_id(assertion_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let source_uuid = canonical_operation_id(source_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let actor_uuid = actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let source_kind = match source_kind {
            "document" => graphforge_api::EvidenceSourceKind::Document,
            "observation" => graphforge_api::EvidenceSourceKind::Observation,
            "graph_node" => graphforge_api::EvidenceSourceKind::GraphNode,
            "graph_edge" => graphforge_api::EvidenceSourceKind::GraphEdge,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("unknown evidence source kind".into()),
                ));
            }
        };
        let role = match role {
            "supports" => graphforge_api::EvidenceRole::Supports,
            "contradicts" => graphforge_api::EvidenceRole::Contradicts,
            "context" => graphforge_api::EvidenceRole::Context,
            _ => {
                return Err(to_pyerr(
                    py,
                    &GfError::Validation("unknown evidence role".into()),
                ));
            }
        };
        let result = py
            .detach(|| {
                self.inner
                    .attach_evidence(graphforge_api::AttachEvidenceRequest {
                        context: WriteContext {
                            operation_uuid,
                            actor_uuid,
                        },
                        evidence_uuid,
                        assertion_uuid,
                        source_uuid,
                        source_kind,
                        role,
                        weight,
                    })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one exact immutable evidence link.
    #[pyo3(signature = (evidence_uuid, *, cancellation=None))]
    fn evidence_link(
        &self,
        py: Python<'_>,
        evidence_uuid: &str,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let evidence_uuid = canonical_operation_id(evidence_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                self.inner
                    .evidence_link(evidence_uuid, cancellation.clone())
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic generation-bound evidence page.
    #[pyo3(signature = (*, assertion_uuid=None, source_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_evidence_links(
        &self,
        py: Python<'_>,
        assertion_uuid: Option<&str>,
        source_uuid: Option<&str>,
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
        let source_uuid = source_uuid
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
                    .list_evidence_links(graphforge_api::ListEvidenceLinksRequest {
                        assertion_uuid,
                        source_uuid,
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
}
