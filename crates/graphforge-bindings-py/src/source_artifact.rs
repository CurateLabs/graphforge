//! Source and Artifact lifecycle Python bindings (#1349).

use super::{GraphForge, PyCancellationToken, canonical_operation_id, result_to_pyarrow, to_pyerr};
use graphforge_api::{
    ArtifactKind, ArtifactPayloadRequest, DerivationInput, DerivationSubjectKind, LineageDirection,
    ListArtifactsRequest, ListSourcesRequest, RegisterArtifactRequest, RegisterSourceRequest,
    ReplacementImpactRequest, ResearchLineageRequest, RetentionDependencyClosureRequest,
    SetPreferredArtifactRequest, SourceKind, WriteContext,
};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

fn parse_source_kind(value: &str) -> Result<SourceKind, graphforge_api::GfError> {
    match value {
        "manuscript" => Ok(SourceKind::Manuscript),
        "edition" => Ok(SourceKind::Edition),
        "pdf" => Ok(SourceKind::Pdf),
        "epub" => Ok(SourceKind::Epub),
        "web" => Ok(SourceKind::Web),
        "photograph" => Ok(SourceKind::Photograph),
        "recording" => Ok(SourceKind::Recording),
        "database_export" => Ok(SourceKind::DatabaseExport),
        "other" => Ok(SourceKind::Other),
        _ => Err(graphforge_api::GfError::Validation(
            "unknown source kind".into(),
        )),
    }
}

fn parse_artifact_kind(value: &str) -> Result<ArtifactKind, graphforge_api::GfError> {
    match value {
        "raw_scan" => Ok(ArtifactKind::RawScan),
        "processed_scan" => Ok(ArtifactKind::ProcessedScan),
        "ocr_text" => Ok(ArtifactKind::OcrText),
        "normalized_text" => Ok(ArtifactKind::NormalizedText),
        "passage_extract" => Ok(ArtifactKind::PassageExtract),
        "other" => Ok(ArtifactKind::Other),
        _ => Err(graphforge_api::GfError::Validation(
            "unknown artifact kind".into(),
        )),
    }
}

fn derivation_subject_kind(value: &str) -> Result<DerivationSubjectKind, graphforge_api::GfError> {
    match value {
        "source" => Ok(DerivationSubjectKind::Source),
        "artifact" => Ok(DerivationSubjectKind::Artifact),
        "node" => Ok(DerivationSubjectKind::Node),
        "edge" => Ok(DerivationSubjectKind::Edge),
        "assertion" => Ok(DerivationSubjectKind::Assertion),
        "evidence_link" => Ok(DerivationSubjectKind::EvidenceLink),
        "algorithm_run" => Ok(DerivationSubjectKind::AlgorithmRun),
        _ => Err(graphforge_api::GfError::Validation(
            "unknown derivation subject kind".into(),
        )),
    }
}

fn lineage_direction(value: &str) -> Result<LineageDirection, graphforge_api::GfError> {
    match value {
        "backward" => Ok(LineageDirection::Backward),
        "forward" => Ok(LineageDirection::Forward),
        _ => Err(graphforge_api::GfError::Validation(
            "unknown lineage direction".into(),
        )),
    }
}

fn py_derivation_input(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<DerivationInput> {
    let value = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("derivation_inputs entries must be dictionaries"))?;
    let input_uuid = canonical_operation_id(
        &value
            .get_item("input_uuid")?
            .ok_or_else(|| PyTypeError::new_err("derivation_inputs entry requires input_uuid"))?
            .extract::<String>()?,
    )
    .map_err(|error| to_pyerr(py, &error))?
    .0;
    let input_kind = derivation_subject_kind(
        &value
            .get_item("input_kind")?
            .ok_or_else(|| PyTypeError::new_err("derivation_inputs entry requires input_kind"))?
            .extract::<String>()?,
    )
    .map_err(|error| to_pyerr(py, &error))?;
    Ok(DerivationInput {
        input_uuid,
        input_kind,
    })
}

fn parse_fingerprint(value: &Bound<'_, PyAny>) -> PyResult<[u8; 32]> {
    let value = value.extract::<&[u8]>()?;
    if value.len() != 32 {
        return Err(PyTypeError::new_err("fingerprint must be exactly 32 bytes"));
    }
    let mut fingerprint = [0_u8; 32];
    fingerprint.copy_from_slice(value);
    Ok(fingerprint)
}

fn py_artifact_payload(value: &Bound<'_, PyAny>) -> PyResult<ArtifactPayloadRequest> {
    let value = value
        .cast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("payload must be a dictionary"))?;
    let kind = value
        .get_item("kind")?
        .ok_or_else(|| PyTypeError::new_err("payload requires kind"))?
        .extract::<String>()?;
    match kind.as_str() {
        "local_bytes" => {
            let bytes = value
                .get_item("bytes")?
                .ok_or_else(|| PyTypeError::new_err("local_bytes payload requires bytes"))?
                .extract::<Vec<u8>>()?;
            Ok(ArtifactPayloadRequest::LocalBytes(bytes))
        }
        "external_reference" => {
            let uri = value
                .get_item("uri")?
                .ok_or_else(|| PyTypeError::new_err("external_reference payload requires uri"))?
                .extract::<String>()?;
            let fingerprint = match value.get_item("fingerprint")? {
                None => None,
                Some(value) if value.is_none() => None,
                Some(value) => Some(parse_fingerprint(&value)?),
            };
            Ok(ArtifactPayloadRequest::ExternalReference { uri, fingerprint })
        }
        "absent" => Ok(ArtifactPayloadRequest::Absent),
        _ => Err(PyTypeError::new_err("unknown artifact payload kind")),
    }
}

fn write_context(
    py: Python<'_>,
    operation_uuid: &str,
    actor_uuid: Option<&str>,
) -> PyResult<WriteContext> {
    Ok(WriteContext {
        operation_uuid: canonical_operation_id(operation_uuid)
            .map_err(|error| to_pyerr(py, &error))?,
        actor_uuid: actor_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0),
    })
}

#[pymethods]
impl GraphForge {
    /// Atomically register one immutable research Source.
    #[pyo3(signature = (*, operation_uuid, source_uuid, label, source_kind, identity_uri=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn register_source(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        source_uuid: &str,
        label: String,
        source_kind: &str,
        identity_uri: Option<String>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let source_uuid = canonical_operation_id(source_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let source_kind = parse_source_kind(source_kind).map_err(|error| to_pyerr(py, &error))?;
        let context = write_context(py, operation_uuid, actor_uuid)?;
        let result = py
            .detach(|| {
                native.register_source(RegisterSourceRequest {
                    context,
                    source_uuid,
                    label,
                    source_kind,
                    identity_uri,
                })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Atomically register one immutable research Artifact.
    #[pyo3(signature = (*, operation_uuid, artifact_uuid, source_uuid, artifact_kind, media_type, payload, derivation_inputs=None, run_uuid=None, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn register_artifact(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        artifact_uuid: &str,
        source_uuid: &str,
        artifact_kind: &str,
        media_type: String,
        payload: &Bound<'_, PyAny>,
        derivation_inputs: Option<&Bound<'_, PyList>>,
        run_uuid: Option<&str>,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let artifact_uuid = canonical_operation_id(artifact_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let source_uuid = canonical_operation_id(source_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let artifact_kind =
            parse_artifact_kind(artifact_kind).map_err(|error| to_pyerr(py, &error))?;
        let payload = py_artifact_payload(payload)?;
        let derivation_inputs = derivation_inputs
            .map(|inputs| {
                inputs
                    .iter()
                    .map(|value| py_derivation_input(py, &value))
                    .collect::<PyResult<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        let run_uuid = run_uuid
            .map(canonical_operation_id)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?
            .map(|id| id.0);
        let context = write_context(py, operation_uuid, actor_uuid)?;
        let result = py
            .detach(|| {
                native.register_artifact(RegisterArtifactRequest {
                    context,
                    artifact_uuid,
                    source_uuid,
                    artifact_kind,
                    media_type,
                    payload,
                    derivation_inputs,
                    run_uuid,
                })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one exact immutable Source as a `pyarrow.Table`.
    #[pyo3(signature = (source_uuid))]
    fn source(&self, py: Python<'_>, source_uuid: &str) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let source_uuid = canonical_operation_id(source_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let result = py
            .detach(|| native.source(source_uuid))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one exact immutable Artifact as a `pyarrow.Table`.
    #[pyo3(signature = (artifact_uuid))]
    fn artifact(&self, py: Python<'_>, artifact_uuid: &str) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let artifact_uuid = canonical_operation_id(artifact_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let result = py
            .detach(|| native.artifact(artifact_uuid))
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic generation-bound Source page.
    #[pyo3(signature = (*, limit=100, after=None, cancellation=None))]
    fn list_sources(
        &self,
        py: Python<'_>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                native.list_sources(ListSourcesRequest {
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

    /// Return one deterministic generation-bound Artifact page.
    #[pyo3(signature = (*, source_uuid=None, limit=100, after=None, cancellation=None))]
    fn list_artifacts(
        &self,
        py: Python<'_>,
        source_uuid: Option<&str>,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
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
                native.list_artifacts(ListArtifactsRequest {
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

    /// Set the preferred Artifact for one Source.
    #[pyo3(signature = (*, operation_uuid, preference_event_uuid, source_uuid, artifact_uuid, reason, actor_uuid=None))]
    #[allow(clippy::too_many_arguments)]
    fn set_preferred_artifact(
        &self,
        py: Python<'_>,
        operation_uuid: &str,
        preference_event_uuid: &str,
        source_uuid: &str,
        artifact_uuid: &str,
        reason: String,
        actor_uuid: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let preference_event_uuid = canonical_operation_id(preference_event_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let source_uuid = canonical_operation_id(source_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let artifact_uuid = canonical_operation_id(artifact_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let context = write_context(py, operation_uuid, actor_uuid)?;
        let result = py
            .detach(|| {
                native.set_preferred_artifact(SetPreferredArtifactRequest {
                    context,
                    preference_event_uuid,
                    source_uuid,
                    artifact_uuid,
                    reason,
                })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return read-only replacement impact for one Source preference change.
    #[pyo3(signature = (source_uuid, artifact_uuid))]
    fn replacement_impact(
        &self,
        py: Python<'_>,
        source_uuid: &str,
        artifact_uuid: &str,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let source_uuid = canonical_operation_id(source_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let artifact_uuid = canonical_operation_id(artifact_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let result = py
            .detach(|| {
                native.replacement_impact(ReplacementImpactRequest {
                    source_uuid,
                    artifact_uuid,
                })
            })
            .map_err(|error| to_pyerr(py, &error))?;
        result_to_pyarrow(py, &result)
    }

    /// Return one deterministic page of derivation edges for a research subject.
    #[pyo3(signature = (subject_uuid, *, subject_kind, direction, max_depth, limit=100, after=None, cancellation=None))]
    #[allow(clippy::too_many_arguments)]
    fn research_lineage(
        &self,
        py: Python<'_>,
        subject_uuid: &str,
        subject_kind: &str,
        direction: &str,
        max_depth: u32,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let subject_uuid = canonical_operation_id(subject_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let subject_kind =
            derivation_subject_kind(subject_kind).map_err(|error| to_pyerr(py, &error))?;
        let direction = lineage_direction(direction).map_err(|error| to_pyerr(py, &error))?;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                native.research_lineage(ResearchLineageRequest {
                    subject_uuid,
                    subject_kind,
                    direction,
                    max_depth,
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

    /// Return read-only retention dependency closure for one scope.
    #[pyo3(signature = (scope_uuid, *, limit=100, after=None, cancellation=None))]
    fn retention_dependency_closure(
        &self,
        py: Python<'_>,
        scope_uuid: &str,
        limit: u32,
        after: Option<&str>,
        cancellation: Option<&PyCancellationToken>,
    ) -> PyResult<Py<PyAny>> {
        let native = self.ensure_open()?;
        let scope_uuid = canonical_operation_id(scope_uuid)
            .map_err(|error| to_pyerr(py, &error))?
            .0;
        let after = after
            .map(graphforge_api::PageToken::parse)
            .transpose()
            .map_err(|error| to_pyerr(py, &error))?;
        let cancellation = cancellation.map(|token| token.inner.clone());
        let result = py
            .detach(|| {
                native.retention_dependency_closure(RetentionDependencyClosureRequest {
                    scope_uuid,
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
