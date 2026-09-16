//! Analyst Python binding methods and conversions.

use super::{
    GraphForge, algorithm_result, embedding_options_from_kwargs, embedding_validation, hex_bytes,
    py_to_node_selector, to_py_invocation_error, to_pyerr,
};
use graphforge_api::GfError;
use graphforge_api::GraphScaleIndexProfile;
use graphforge_api::InvocationDescriptor;
use graphforge_api::NodeSelector;
use graphforge_api::validate_embedding_options;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::types::PyDict;

pub(super) fn parse_terminal_uuids(values: &[String]) -> Result<Vec<[u8; 16]>, GfError> {
    let mut terminals = Vec::new();
    terminals.try_reserve_exact(values.len()).map_err(|_| {
        GfError::Execution("Steiner terminal allocation exceeds available memory".into())
    })?;
    for value in values {
        if value.len() != 36 {
            return Err(GfError::Validation(format!(
                "invalid Steiner terminal UUID {value:?}"
            )));
        }
        let NodeSelector::Uuid(uuid) = NodeSelector::uuid(value)? else {
            unreachable!("UUID parser always constructs a UUID selector")
        };
        if uuid.hyphenated().to_string() != *value {
            return Err(GfError::Validation(format!(
                "invalid Steiner terminal UUID {value:?}"
            )));
        }
        terminals.push(*uuid.as_bytes());
    }
    Ok(terminals)
}

/// Opaque Rust-owned neutral algorithm invocation descriptor.
#[pyclass(name = "InvocationDescriptor", module = "graphforge")]
pub struct PyInvocationDescriptor {
    pub(super) inner: InvocationDescriptor,
}

/// Structured Graph Scale Index grade for one opened workspace.
#[pyclass(name = "GraphScaleIndexProfile", module = "graphforge")]
pub struct PyGraphScaleIndexProfile {
    pub(super) inner: GraphScaleIndexProfile,
}

#[pymethods]
impl PyGraphScaleIndexProfile {
    #[getter]
    fn gsi(&self) -> &str {
        &self.inner.gsi
    }

    #[getter]
    fn directedness(&self) -> &'static str {
        self.inner.directedness.as_str()
    }

    #[getter]
    fn node_count(&self) -> u64 {
        self.inner.node_count
    }

    #[getter]
    fn edge_count(&self) -> u64 {
        self.inner.edge_count
    }

    #[getter]
    fn density(&self) -> f64 {
        self.inner.density
    }

    #[getter]
    fn scale_code(&self) -> &str {
        &self.inner.scale_code
    }

    #[getter]
    fn size_tag(&self) -> &str {
        &self.inner.size_tag
    }

    #[getter]
    fn density_integer(&self) -> u32 {
        self.inner.density_integer
    }

    fn __repr__(&self) -> String {
        format!("GraphScaleIndexProfile(gsi={:?})", self.inner.gsi)
    }
}

#[pymethods]
impl PyInvocationDescriptor {
    /// Canonical language-neutral descriptor bytes.
    #[getter]
    fn canonical_bytes(&self, py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, self.inner.canonical_bytes()).unbind()
    }

    /// Full descriptor fingerprint as lowercase hex.
    #[getter]
    fn fingerprint(&self) -> String {
        hex_bytes(self.inner.fingerprint())
    }

    /// Exact logical projection fingerprint as lowercase hex.
    #[getter]
    fn projection_fingerprint(&self) -> String {
        hex_bytes(self.inner.projection_fingerprint())
    }

    /// Owning analyst verb.
    #[getter]
    fn verb(&self) -> &'static str {
        self.inner.algorithm().verb().as_str()
    }

    /// Canonical algorithm catalog value.
    #[getter]
    fn algorithm(&self) -> &'static str {
        self.inner.algorithm().as_str()
    }
}

pub(super) fn parse_algorithm_id(value: &str) -> Result<graphforge_api::Algorithm, GfError> {
    let (verb, name) = value
        .split_once('.')
        .ok_or_else(|| GfError::Validation("algorithm must be verb.name".into()))?;
    let verb = match verb {
        "rank" => graphforge_api::AlgorithmVerb::Rank,
        "cluster" => graphforge_api::AlgorithmVerb::Cluster,
        "paths" => graphforge_api::AlgorithmVerb::Paths,
        "analyze" => graphforge_api::AlgorithmVerb::Analyze,
        "similar" => graphforge_api::AlgorithmVerb::Similar,
        _ => return Err(GfError::Validation("unknown algorithm verb".into())),
    };
    graphforge_api::Algorithm::parse(verb, name)
        .map_err(|_| GfError::Validation("unknown algorithm ID".into()))
}

#[pymethods]
impl GraphForge {
    /// Rank nodes by a centrality/structural algorithm (`by=`). Returns a
    /// `pyarrow.Table`.
    #[pyo3(signature = (label, *, by, via=None, directed=true, write_property=None))]
    fn rank(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        via: Option<&str>,
        directed: bool,
        write_property: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let opts = graphforge_api::RankOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        };
        let label = label.to_owned();
        algorithm_result(py, py.detach(|| self.inner.rank(&label, opts)))
    }

    /// Prepare a Rust-owned neutral rank invocation without executing it.
    #[pyo3(signature = (label, *, by, via=None, directed=true))]
    fn prepare_rank_invocation(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        via: Option<&str>,
        directed: bool,
    ) -> PyResult<PyInvocationDescriptor> {
        self.ensure_open()?;
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

    /// Dispatch an opaque descriptor through its Rust-owned analyst verb.
    fn invoke_descriptor(
        &self,
        py: Python<'_>,
        descriptor: &PyInvocationDescriptor,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let descriptor = descriptor.inner.clone();
        let batch = py
            .detach(|| self.inner.invoke_descriptor(&descriptor))
            .map_err(|error| to_py_invocation_error(py, &error))?;
        algorithm_result(py, Ok(batch))
    }

    /// Decode canonical descriptor bytes in Rust and dispatch them.
    fn invoke_descriptor_bytes(&self, py: Python<'_>, descriptor: &[u8]) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let descriptor = descriptor.to_owned();
        let batch = py
            .detach(|| self.inner.invoke_descriptor_bytes(&descriptor))
            .map_err(|error| to_py_invocation_error(py, &error))?;
        algorithm_result(py, Ok(batch))
    }

    /// Detect communities/components (`by=`). Returns a `pyarrow.Table`.
    #[allow(clippy::too_many_arguments)] // kwarg-rich v0.5 cluster() signature
    #[pyo3(signature = (label, *, by, vector_property=None, via=None, directed=false, write_property=None))]
    fn cluster(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        vector_property: Option<&str>,
        via: Option<&str>,
        directed: bool,
        write_property: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let opts = graphforge_api::ClusterOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            vector_property: vector_property.map(str::to_owned),
            via: via.map(str::to_owned),
            directed,
            write_property: write_property.map(str::to_owned),
        };
        let label = label.to_owned();
        algorithm_result(py, py.detach(|| self.inner.cluster(&label, opts)))
    }

    /// Path-finding / flow between nodes (`by=`). Returns a `pyarrow.Table`.
    #[allow(clippy::too_many_arguments)] // kwarg-rich v0.5 paths() signature
    #[pyo3(signature = (source=None, target=None, *, by, via=None, directed=true, k=1, weight=None, capacity_property=None, cost_property=None, heuristic=None, walk_length=None, seed=None, terminal_uuids=None, prize_property=None))]
    fn paths(
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
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let terminal_uuids = terminal_uuids.unwrap_or_default();
        let terminal_uuids =
            parse_terminal_uuids(&terminal_uuids).map_err(|error| to_pyerr(py, &error))?;
        let opts = graphforge_api::PathsOptions {
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
        algorithm_result(
            py,
            py.detach(|| self.inner.paths(source.as_ref(), target.as_ref(), opts)),
        )
    }

    /// Graph-level structural metric (`by=`). Returns a `pyarrow.Table`.
    #[allow(clippy::too_many_arguments)] // kwarg-rich v0.5 analyze() signature
    #[pyo3(signature = (label=None, *, by, via=None, directed=None, weight=None, partition_property=None, k=None, embedding_options=None))]
    fn analyze(
        &self,
        py: Python<'_>,
        label: Option<&str>,
        by: &str,
        via: Option<&str>,
        directed: Option<bool>,
        weight: Option<&str>,
        partition_property: Option<&str>,
        k: Option<usize>,
        embedding_options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let algorithm = by.parse().map_err(|error| to_pyerr(py, &error))?;
        let label = label.map(str::to_owned);
        if matches!(
            algorithm,
            graphforge_api::AnalyzeAlgorithm::Node2Vec
                | graphforge_api::AnalyzeAlgorithm::GraphSage
                | graphforge_api::AnalyzeAlgorithm::FastRandomProjection
                | graphforge_api::AnalyzeAlgorithm::HashGnn
        ) {
            if partition_property.is_some() || k.is_some() {
                return Err(embedding_validation(
                    py,
                    "embedding algorithms do not accept partition_property or k",
                ));
            }
            let directed = directed.unwrap_or(!matches!(
                algorithm,
                graphforge_api::AnalyzeAlgorithm::GraphSage
            ));
            let options = embedding_options_from_kwargs(
                py,
                algorithm,
                via,
                directed,
                weight,
                embedding_options,
            )?;
            validate_embedding_options(&options).map_err(|error| to_pyerr(py, &error))?;
            return algorithm_result(
                py,
                py.detach(|| self.inner.analyze_embedding(label.as_deref(), &options)),
            );
        }
        if embedding_options.is_some() {
            return Err(embedding_validation(
                py,
                format!("{by} does not accept embedding_options"),
            ));
        }
        // Keep binding construction extension-safe as AnalyzeOptions gains fields.
        #[allow(clippy::needless_update)]
        let opts = graphforge_api::AnalyzeOptions {
            by: algorithm,
            via: via.map(str::to_owned),
            directed: directed.unwrap_or(true),
            weight: weight.map(str::to_owned),
            k,
            partition_property: partition_property.map(str::to_owned),
            ..graphforge_api::AnalyzeOptions::default()
        };
        algorithm_result(py, py.detach(|| self.inner.analyze(label.as_deref(), opts)))
    }

    /// Pairwise node similarity (`by=`). Returns a `pyarrow.Table`.
    #[pyo3(signature = (label, *, by, k=10, vector_property=None, via=None))]
    fn similar(
        &self,
        py: Python<'_>,
        label: &str,
        by: &str,
        k: usize,
        vector_property: Option<&str>,
        via: Option<&str>,
    ) -> PyResult<Py<PyAny>> {
        self.ensure_open()?;
        let opts = graphforge_api::SimilarOptions {
            by: by.parse().map_err(|error| to_pyerr(py, &error))?,
            k,
            vector_property: vector_property.map(str::to_owned),
            via: via.map(str::to_owned),
        };
        let label = label.to_owned();
        algorithm_result(py, py.detach(|| self.inner.similar(&label, opts)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn steiner_terminals_are_checked_and_preserve_input_order() {
        let first = "018f0f4e-7b8c-7000-8000-000000000002".to_owned();
        let second = "018f0f4e-7b8c-7000-8000-000000000001".to_owned();
        let terminals = parse_terminal_uuids(&[first.clone(), second.clone()]).unwrap();

        let NodeSelector::Uuid(first_uuid) = NodeSelector::uuid(&first).unwrap() else {
            unreachable!()
        };
        let NodeSelector::Uuid(second_uuid) = NodeSelector::uuid(&second).unwrap() else {
            unreachable!()
        };
        assert_eq!(
            terminals,
            vec![*first_uuid.as_bytes(), *second_uuid.as_bytes()]
        );

        assert!(matches!(
            parse_terminal_uuids(&["not-a-uuid".to_owned()]),
            Err(GfError::Validation(_))
        ));
        assert!(matches!(
            parse_terminal_uuids(&[first.to_uppercase()]),
            Err(GfError::Validation(_))
        ));
    }
}
