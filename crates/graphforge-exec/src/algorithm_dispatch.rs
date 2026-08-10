//! Typed Rust-only dispatch and cooperative execution controls for M18.
#![allow(
    dead_code,
    reason = "M18 foundation consumed incrementally by algorithm leaf issues"
)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;
use graphforge_core::GfError;
use graphforge_core::algorithms::{Algorithm, AlgorithmResultSchema};

use crate::algorithm_arrow_sink::{AlgorithmArrowSink, decode_logical_rows};
use crate::algorithm_graph::AdjacencyGraph;

/// Structured failures produced by Rust algorithm dispatch.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum AlgorithmError {
    /// No Rust handler has registered this typed algorithm yet.
    #[error("Rust algorithm capability is unavailable: {algorithm}")]
    Unavailable {
        /// Canonical `verb.by` identity.
        algorithm: String,
    },
    /// Two handlers attempted to own the same canonical algorithm.
    #[error("duplicate Rust algorithm capability: {algorithm}")]
    DuplicateCapability {
        /// Canonical `verb.by` identity.
        algorithm: String,
    },
    /// Cooperative cancellation was requested.
    #[error("algorithm execution cancelled")]
    Cancelled,
    /// Selected graph exceeded the node budget.
    #[error("algorithm node limit exceeded: observed {observed}, limit {limit}")]
    NodeLimit {
        /// Selected node count.
        observed: u64,
        /// Configured maximum.
        limit: u64,
    },
    /// Selected graph exceeded the adjacency-entry budget.
    #[error("algorithm edge limit exceeded: observed {observed}, limit {limit}")]
    EdgeLimit {
        /// Selected adjacency-entry count.
        observed: u64,
        /// Configured maximum.
        limit: u64,
    },
    /// Handler produced more rows than permitted.
    #[error("algorithm output row limit exceeded: observed {observed}, limit {limit}")]
    OutputLimit {
        /// Produced row count.
        observed: u64,
        /// Configured maximum.
        limit: u64,
    },
    /// Cooperative iteration budget was exhausted.
    #[error("algorithm iteration limit exceeded: observed {observed}, limit {limit}")]
    IterationLimit {
        /// Attempted iteration number.
        observed: u64,
        /// Configured maximum.
        limit: u64,
    },
    /// An exact solver exceeded its aggregate search-state budget.
    #[error("algorithm state-space limit exceeded: observed {observed}, limit {limit}")]
    StateLimit {
        /// Attempted cumulative state count.
        observed: u64,
        /// Configured maximum cumulative state count.
        limit: u64,
    },
    /// An exact solver's aggregate state counter exceeded `UInt64`.
    #[error("algorithm state-space counter exceeds UInt64 range")]
    StateOverflow,
    /// A Steiner invocation supplied an option outside its closed contract.
    #[error("{algorithm} invalid option {option}: {reason}")]
    SteinerOption {
        /// Canonical path catalog value.
        algorithm: &'static str,
        /// Canonical option name.
        option: &'static str,
        /// Stable rejection reason.
        reason: &'static str,
    },
    /// A Steiner invocation has too few distinct mandatory terminals.
    #[error("{algorithm} requires at least {required} distinct terminals; observed {observed}")]
    SteinerTerminalCardinality {
        /// Canonical path catalog value.
        algorithm: &'static str,
        /// Distinct terminal count after normalization.
        observed: usize,
        /// Minimum distinct terminal count.
        required: usize,
    },
    /// A mandatory Steiner terminal is outside the selected projection.
    #[error("Steiner terminal {uuid:?} is outside the selected graph")]
    SteinerTerminalOutsideProjection {
        /// Canonical graph-native terminal UUID.
        uuid: [u8; 16],
    },
    /// An iterative algorithm stopped without satisfying its convergence rule.
    #[error("algorithm did not converge after {iterations} iterations")]
    NonConvergence {
        /// Completed iterations.
        iterations: u64,
    },
    /// Handler-specific execution failure without graph data in the message.
    #[error("Rust algorithm execution failed: {message}")]
    Execution {
        /// Sanitized diagnostic.
        message: String,
    },
    /// Conductance is undefined for a zero-volume partition or complement.
    #[error("conductance is undefined for partition {partition}: denominator volume is zero")]
    UndefinedConductance {
        /// Normalized partition identifier.
        partition: String,
    },
    /// Modularity has no denominator because the selected graph has zero total edge weight.
    #[error("modularity is undefined: total edge weight is zero")]
    UndefinedModularity,
    /// Exact automorphism counting exceeded the canonical unsigned result range.
    #[error("automorphism count exceeds UInt64 range")]
    AutomorphismCountOverflow,
    /// Automorphism counting exceeded its deterministic search-state budget.
    #[error(
        "automorphism count search-state limit exceeded: observed {observed} entries, limit {limit}"
    )]
    AutomorphismCountStateLimit {
        /// Attempted cumulative retained/generated search-state entries.
        observed: u64,
        /// Maximum cumulative retained/generated search-state entries.
        limit: u64,
    },
    /// No Euler circuit exists for the selected canonical projection.
    #[error("Euler circuit is undefined for the selected graph")]
    UndefinedEulerCircuit,
    /// No Euler path exists for the selected canonical projection.
    #[error("Euler path is undefined for the selected graph")]
    UndefinedEulerPath,
}

impl From<AlgorithmError> for GfError {
    fn from(error: AlgorithmError) -> Self {
        match error {
            AlgorithmError::Unavailable { .. } | AlgorithmError::DuplicateCapability { .. } => {
                Self::Validation(error.to_string())
            }
            _ => Self::Execution(error.to_string()),
        }
    }
}

/// Hard limits shared by every Rust algorithm handler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AlgorithmLimits {
    /// Maximum selected nodes.
    pub nodes: u64,
    /// Maximum adjacency entries after direction and filtering.
    pub edges: u64,
    /// Maximum public output rows.
    pub output_rows: u64,
    /// Maximum cooperative iteration checkpoints.
    pub iterations: u64,
    /// Maximum aggregate exact-solver states retained or generated.
    pub states: u64,
    /// Internal Arrow shaping batch size (from #337 resource policy).
    pub batch_size: usize,
}

impl Default for AlgorithmLimits {
    fn default() -> Self {
        Self {
            nodes: 10_000_000,
            edges: 100_000_000,
            output_rows: 10_000_000,
            iterations: 10_000,
            states: 10_000_000,
            batch_size: 8_192,
        }
    }
}

impl AlgorithmLimits {
    /// Override the internal Arrow shaping batch size (#337 / #341).
    #[must_use]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }
}

/// Cloneable cancellation signal shared with a running handler.
#[derive(Clone, Debug, Default)]
pub(crate) struct AlgorithmCancellation(Arc<AtomicBool>);

impl AlgorithmCancellation {
    /// Request cooperative cancellation.
    pub(crate) fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Per-invocation controls passed to a Rust handler.
#[derive(Debug)]
pub(crate) struct AlgorithmControl {
    limits: AlgorithmLimits,
    cancellation: AlgorithmCancellation,
    iterations: AtomicU64,
    states: AtomicU64,
}

impl AlgorithmControl {
    pub(crate) fn new(limits: AlgorithmLimits, cancellation: AlgorithmCancellation) -> Self {
        Self {
            limits,
            cancellation,
            iterations: AtomicU64::new(0),
            states: AtomicU64::new(0),
        }
    }

    pub(crate) fn configured_limits(&self) -> AlgorithmLimits {
        self.limits
    }

    /// Internal Arrow shaping batch size for bounded columnar sinks (#341).
    pub(crate) fn batch_size(&self) -> usize {
        self.limits.batch_size.max(1)
    }

    /// Open a columnar output sink for `algorithm`.
    pub(crate) fn output_sink(
        &self,
        algorithm: Algorithm,
    ) -> Result<AlgorithmArrowSink, AlgorithmError> {
        AlgorithmArrowSink::new(algorithm, self)
    }

    /// Check cancellation and consume one cooperative iteration.
    pub(crate) fn checkpoint(&self) -> Result<u64, AlgorithmError> {
        self.check_cancelled()?;
        let observed = self.iterations.fetch_add(1, Ordering::AcqRel) + 1;
        if observed > self.limits.iterations {
            return Err(AlgorithmError::IterationLimit {
                observed,
                limit: self.limits.iterations,
            });
        }
        Ok(observed)
    }

    /// Return a typed non-convergence failure with the consumed iteration count.
    pub(crate) fn non_convergence(&self) -> AlgorithmError {
        AlgorithmError::NonConvergence {
            iterations: self.iterations.load(Ordering::Acquire),
        }
    }

    pub(crate) fn check_cancelled(&self) -> Result<(), AlgorithmError> {
        if self.cancellation.is_cancelled() {
            Err(AlgorithmError::Cancelled)
        } else {
            Ok(())
        }
    }

    pub(crate) fn check_output_rows(&self, observed: usize) -> Result<(), AlgorithmError> {
        let observed = u64::try_from(observed).unwrap_or(u64::MAX);
        if observed > self.limits.output_rows {
            Err(AlgorithmError::OutputLimit {
                observed,
                limit: self.limits.output_rows,
            })
        } else {
            Ok(())
        }
    }

    pub(crate) fn check_graph_size(&self, nodes: usize, edges: u64) -> Result<(), AlgorithmError> {
        let nodes = u64::try_from(nodes).unwrap_or(u64::MAX);
        if nodes > self.limits.nodes {
            return Err(AlgorithmError::NodeLimit {
                observed: nodes,
                limit: self.limits.nodes,
            });
        }
        if edges > self.limits.edges {
            return Err(AlgorithmError::EdgeLimit {
                observed: edges,
                limit: self.limits.edges,
            });
        }
        Ok(())
    }

    /// Validate a requested iteration budget without consuming it.
    pub(crate) fn check_iterations(&self, observed: usize) -> Result<(), AlgorithmError> {
        let observed = u64::try_from(observed).unwrap_or(u64::MAX);
        if observed > self.limits.iterations {
            Err(AlgorithmError::IterationLimit {
                observed,
                limit: self.limits.iterations,
            })
        } else {
            Ok(())
        }
    }

    /// Validate a deterministic exact-solver state estimate without consuming it.
    pub(crate) fn check_states(&self, observed: u64) -> Result<(), AlgorithmError> {
        self.check_cancelled()?;
        if observed > self.limits.states {
            Err(AlgorithmError::StateLimit {
                observed,
                limit: self.limits.states,
            })
        } else {
            Ok(())
        }
    }

    /// Atomically consume aggregate exact-solver states for this invocation.
    pub(crate) fn consume_states(&self, additional: u64) -> Result<u64, AlgorithmError> {
        self.check_cancelled()?;
        let mut current = self.states.load(Ordering::Acquire);
        loop {
            let observed = current
                .checked_add(additional)
                .ok_or(AlgorithmError::StateOverflow)?;
            if observed > self.limits.states {
                return Err(AlgorithmError::StateLimit {
                    observed,
                    limit: self.limits.states,
                });
            }
            match self.states.compare_exchange_weak(
                current,
                observed,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(observed),
                Err(updated) => current = updated,
            }
        }
    }
}

/// Internal typed value returned by Rust handlers before Arrow shaping (#1147).
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum AlgorithmValue {
    Null,
    Uuid([u8; 16]),
    UuidList(Vec<[u8; 16]>),
    Float32List(Vec<f32>),
    Utf8(String),
    Boolean(bool),
    UInt64(u64),
    Int64(i64),
    Float64(f64),
}

/// Columnar Arrow result produced by one Rust handler via [`AlgorithmArrowSink`].
///
/// Handlers append directly into the sink; this type retains the finished public
/// `RecordBatch` only — never a second complete `Vec<Vec<AlgorithmValue>>`.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AlgorithmOutput {
    pub schema: AlgorithmResultSchema,
    pub(crate) batch: RecordBatch,
    /// Number of internal batches before public coalesce.
    pub(crate) internal_batch_count: usize,
    /// Peak rows retained in the active builder window (≤ batch_size).
    pub(crate) peak_builder_rows: usize,
}

impl AlgorithmOutput {
    /// Number of public result rows.
    pub(crate) fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    /// Borrow the canonical public Arrow batch.
    pub(crate) fn record_batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Decode logical rows for assertions. Does not store a retained row graph.
    pub(crate) fn rows(&self) -> Vec<Vec<AlgorithmValue>> {
        decode_logical_rows(&self.schema, &self.batch).unwrap_or_default()
    }

    /// Shape one or more logical rows through the shared columnar sink.
    pub(crate) fn from_rows(
        algorithm: Algorithm,
        control: &AlgorithmControl,
        rows: impl IntoIterator<Item = Vec<AlgorithmValue>>,
    ) -> Result<Self, AlgorithmError> {
        let mut sink = control.output_sink(algorithm)?;
        for row in rows {
            sink.append_row(&row)?;
        }
        sink.finish()
    }

    /// Empty canonical result for `algorithm`.
    pub(crate) fn empty(
        algorithm: Algorithm,
        control: &AlgorithmControl,
    ) -> Result<Self, AlgorithmError> {
        control.output_sink(algorithm)?.finish()
    }
}

/// Required dependency review attached to each registered capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DependencyReview {
    pub implementation: &'static str,
    pub license: &'static str,
    pub maintenance: &'static str,
    pub security: &'static str,
    pub binary_size: &'static str,
    pub determinism: &'static str,
    pub platforms: &'static str,
}

/// Non-sensitive capability metadata exposed for diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AlgorithmCapability {
    pub algorithm: Algorithm,
    pub backend: &'static str,
    pub dependency: DependencyReview,
}

/// One Rust implementation registered under exactly one typed algorithm.
pub(crate) trait RustAlgorithm: Send + Sync {
    fn capability(&self) -> AlgorithmCapability;

    fn execute(
        &self,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError>;
}

/// Deterministic typed registry for Rust algorithm handlers.
#[derive(Default)]
pub(crate) struct AlgorithmRegistry {
    handlers: HashMap<Algorithm, Arc<dyn RustAlgorithm>>,
}

impl AlgorithmRegistry {
    pub(crate) fn register(
        &mut self,
        handler: Arc<dyn RustAlgorithm>,
    ) -> Result<(), AlgorithmError> {
        let capability = handler.capability();
        if self.handlers.contains_key(&capability.algorithm) {
            return Err(AlgorithmError::DuplicateCapability {
                algorithm: algorithm_name(capability.algorithm),
            });
        }
        self.handlers.insert(capability.algorithm, handler);
        Ok(())
    }

    pub(crate) fn capabilities(&self) -> Vec<AlgorithmCapability> {
        let mut capabilities: Vec<_> = self
            .handlers
            .values()
            .map(|handler| handler.capability())
            .collect();
        capabilities.sort_by_key(|capability| algorithm_name(capability.algorithm));
        capabilities
    }

    pub(crate) fn execute(
        &self,
        algorithm: Algorithm,
        graph: &AdjacencyGraph,
        control: &AlgorithmControl,
    ) -> Result<AlgorithmOutput, AlgorithmError> {
        control.check_cancelled()?;
        let node_count = u64::try_from(graph.node_ids().len()).unwrap_or(u64::MAX);
        if node_count > control.limits.nodes {
            return Err(AlgorithmError::NodeLimit {
                observed: node_count,
                limit: control.limits.nodes,
            });
        }
        let edge_count = graph.edge_entry_count();
        if edge_count > control.limits.edges {
            return Err(AlgorithmError::EdgeLimit {
                observed: edge_count,
                limit: control.limits.edges,
            });
        }
        let handler = self
            .handlers
            .get(&algorithm)
            .ok_or_else(|| AlgorithmError::Unavailable {
                algorithm: algorithm_name(algorithm),
            })?;
        let output = handler.execute(graph, control)?;
        control.check_output_rows(output.num_rows())?;
        control.check_cancelled()?;
        Ok(output)
    }
}

fn algorithm_name(algorithm: Algorithm) -> String {
    format!("{}.{}", algorithm.verb().as_str(), algorithm.as_str())
}

#[cfg(test)]
mod tests {
    use graphforge_core::algorithms::{AnalyzeAlgorithm, RankAlgorithm};

    use super::*;

    const REVIEW: DependencyReview = DependencyReview {
        implementation: "graphforge-exec built-in",
        license: "Apache-2.0",
        maintenance: "GraphForge workspace",
        security: "workspace cargo-deny and CodeQL",
        binary_size: "no additional dependency",
        determinism: "fixture-verified deterministic ordering",
        platforms: "Rust workspace targets",
    };

    #[derive(Clone, Copy)]
    enum Behavior {
        Rows(usize),
        Checkpoints(u64),
        NonConverge(u64),
    }

    struct TestHandler {
        algorithm: Algorithm,
        behavior: Behavior,
    }

    impl RustAlgorithm for TestHandler {
        fn capability(&self) -> AlgorithmCapability {
            AlgorithmCapability {
                algorithm: self.algorithm,
                backend: "rust",
                dependency: REVIEW,
            }
        }

        fn execute(
            &self,
            _graph: &AdjacencyGraph,
            control: &AlgorithmControl,
        ) -> Result<AlgorithmOutput, AlgorithmError> {
            match self.behavior {
                Behavior::Rows(count) => {
                    let mut sink = control.output_sink(self.algorithm)?;
                    let placeholders = placeholder_row(self.algorithm);
                    for _ in 0..count {
                        sink.append_row(&placeholders)?;
                    }
                    sink.finish()
                }
                Behavior::Checkpoints(count) => {
                    for _ in 0..count {
                        control.checkpoint()?;
                    }
                    AlgorithmOutput::empty(self.algorithm, control)
                }
                Behavior::NonConverge(count) => {
                    for _ in 0..count {
                        control.checkpoint()?;
                    }
                    Err(control.non_convergence())
                }
            }
        }
    }

    fn placeholder_row(algorithm: Algorithm) -> Vec<AlgorithmValue> {
        use graphforge_core::algorithms::AlgorithmFieldType;
        algorithm
            .result_schema()
            .fields
            .iter()
            .map(|field| match field.data_type {
                AlgorithmFieldType::Uuid => AlgorithmValue::Uuid([0; 16]),
                AlgorithmFieldType::UuidList => AlgorithmValue::UuidList(Vec::new()),
                AlgorithmFieldType::Float32List => AlgorithmValue::Float32List(Vec::new()),
                AlgorithmFieldType::Utf8 => AlgorithmValue::Utf8(String::new()),
                AlgorithmFieldType::Boolean => AlgorithmValue::Boolean(false),
                AlgorithmFieldType::UInt64 => AlgorithmValue::UInt64(0),
                AlgorithmFieldType::Int64 => AlgorithmValue::Int64(0),
                AlgorithmFieldType::Float64 => AlgorithmValue::Float64(0.0),
            })
            .collect()
    }

    fn degree() -> Algorithm {
        Algorithm::Rank(RankAlgorithm::Degree)
    }

    fn handler(algorithm: Algorithm, behavior: Behavior) -> Arc<dyn RustAlgorithm> {
        Arc::new(TestHandler {
            algorithm,
            behavior,
        })
    }

    fn control(limits: AlgorithmLimits) -> AlgorithmControl {
        AlgorithmControl::new(limits, AlgorithmCancellation::default())
    }

    #[test]
    fn registered_handler_dispatches_and_reports_rust_metadata() {
        let mut registry = AlgorithmRegistry::default();
        registry
            .register(handler(degree(), Behavior::Rows(1)))
            .unwrap();
        let output = registry
            .execute(
                degree(),
                &AdjacencyGraph::default(),
                &control(AlgorithmLimits::default()),
            )
            .unwrap();
        assert_eq!(output.schema, degree().result_schema());
        assert_eq!(output.num_rows(), 1);
        assert_eq!(registry.capabilities()[0].backend, "rust");
        assert_eq!(registry.capabilities()[0].dependency, REVIEW);
    }

    #[test]
    fn unavailable_and_duplicate_capabilities_are_typed() {
        let mut registry = AlgorithmRegistry::default();
        let unavailable = registry
            .execute(
                degree(),
                &AdjacencyGraph::default(),
                &control(AlgorithmLimits::default()),
            )
            .unwrap_err();
        assert_eq!(
            unavailable,
            AlgorithmError::Unavailable {
                algorithm: "rank.degree".into()
            }
        );

        registry
            .register(handler(degree(), Behavior::Rows(0)))
            .unwrap();
        assert_eq!(
            registry
                .register(handler(degree(), Behavior::Rows(0)))
                .unwrap_err(),
            AlgorithmError::DuplicateCapability {
                algorithm: "rank.degree".into()
            }
        );
    }

    #[test]
    fn capabilities_sort_by_canonical_identity() {
        let mut registry = AlgorithmRegistry::default();
        let is_dag = Algorithm::Analyze(AnalyzeAlgorithm::IsDag);
        registry
            .register(handler(degree(), Behavior::Rows(0)))
            .unwrap();
        registry
            .register(handler(is_dag, Behavior::Rows(0)))
            .unwrap();
        assert_eq!(
            registry
                .capabilities()
                .into_iter()
                .map(|capability| algorithm_name(capability.algorithm))
                .collect::<Vec<_>>(),
            ["analyze.is_dag", "rank.degree"]
        );
    }

    #[test]
    fn cancellation_is_observed_before_dispatch() {
        let mut registry = AlgorithmRegistry::default();
        registry
            .register(handler(degree(), Behavior::Rows(0)))
            .unwrap();
        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let error = registry
            .execute(
                degree(),
                &AdjacencyGraph::default(),
                &AlgorithmControl::new(AlgorithmLimits::default(), cancellation),
            )
            .unwrap_err();
        assert_eq!(error, AlgorithmError::Cancelled);
    }

    #[test]
    fn graph_and_output_limits_are_enforced() {
        let mut registry = AlgorithmRegistry::default();
        registry
            .register(handler(degree(), Behavior::Rows(3)))
            .unwrap();
        let limits = AlgorithmLimits {
            nodes: 1,
            edges: 1,
            output_rows: 1,
            iterations: 10,
            states: 10,
            batch_size: AlgorithmLimits::default().batch_size,
        };
        assert_eq!(
            registry
                .execute(
                    degree(),
                    &AdjacencyGraph::with_test_counts(2, 0),
                    &control(limits)
                )
                .unwrap_err(),
            AlgorithmError::NodeLimit {
                observed: 2,
                limit: 1
            }
        );
        assert_eq!(
            registry
                .execute(
                    degree(),
                    &AdjacencyGraph::with_test_counts(1, 2),
                    &control(limits)
                )
                .unwrap_err(),
            AlgorithmError::EdgeLimit {
                observed: 2,
                limit: 1
            }
        );
        assert_eq!(
            registry
                .execute(
                    degree(),
                    &AdjacencyGraph::with_test_counts(1, 1),
                    &control(limits)
                )
                .unwrap_err(),
            AlgorithmError::OutputLimit {
                observed: 3,
                limit: 1
            }
        );
    }

    #[test]
    fn iteration_limit_and_non_convergence_are_distinct() {
        let limits = AlgorithmLimits {
            iterations: 2,
            ..AlgorithmLimits::default()
        };
        let mut limited = AlgorithmRegistry::default();
        limited
            .register(handler(degree(), Behavior::Checkpoints(3)))
            .unwrap();
        assert_eq!(
            limited
                .execute(degree(), &AdjacencyGraph::default(), &control(limits))
                .unwrap_err(),
            AlgorithmError::IterationLimit {
                observed: 3,
                limit: 2
            }
        );

        let mut non_convergent = AlgorithmRegistry::default();
        non_convergent
            .register(handler(degree(), Behavior::NonConverge(2)))
            .unwrap();
        assert_eq!(
            non_convergent
                .execute(degree(), &AdjacencyGraph::default(), &control(limits))
                .unwrap_err(),
            AlgorithmError::NonConvergence { iterations: 2 }
        );
    }

    #[test]
    fn exact_solver_state_limit_has_named_default() {
        assert_eq!(AlgorithmLimits::default().states, 10_000_000);
    }

    #[test]
    fn state_preflight_is_deterministic_cancellable_and_non_consuming() {
        let limits = AlgorithmLimits {
            states: 3,
            ..AlgorithmLimits::default()
        };
        let control = control(limits);
        assert_eq!(control.check_states(3), Ok(()));
        assert_eq!(
            control.check_states(4),
            Err(AlgorithmError::StateLimit {
                observed: 4,
                limit: 3,
            })
        );
        assert_eq!(control.consume_states(3), Ok(3));

        let cancellation = AlgorithmCancellation::default();
        cancellation.cancel();
        let cancelled = AlgorithmControl::new(limits, cancellation);
        assert_eq!(cancelled.check_states(0), Err(AlgorithmError::Cancelled));
    }

    #[test]
    fn state_consumption_is_cumulative_and_failed_consumption_is_atomic() {
        let control = control(AlgorithmLimits {
            states: 3,
            ..AlgorithmLimits::default()
        });
        assert_eq!(control.consume_states(0), Ok(0));
        assert_eq!(control.consume_states(2), Ok(2));
        assert_eq!(control.consume_states(1), Ok(3));
        assert_eq!(
            control.consume_states(1),
            Err(AlgorithmError::StateLimit {
                observed: 4,
                limit: 3,
            })
        );
        assert_eq!(control.consume_states(0), Ok(3));
    }

    #[test]
    fn state_consumption_reports_overflow_and_cancellation_without_mutation() {
        let overflow = control(AlgorithmLimits {
            states: u64::MAX,
            ..AlgorithmLimits::default()
        });
        assert_eq!(overflow.consume_states(u64::MAX), Ok(u64::MAX));
        assert_eq!(
            overflow.consume_states(1),
            Err(AlgorithmError::StateOverflow)
        );
        assert_eq!(overflow.consume_states(0), Ok(u64::MAX));

        let cancellation = AlgorithmCancellation::default();
        let cancelled = AlgorithmControl::new(AlgorithmLimits::default(), cancellation.clone());
        assert_eq!(cancelled.consume_states(2), Ok(2));
        cancellation.cancel();
        assert_eq!(cancelled.consume_states(1), Err(AlgorithmError::Cancelled));
        assert_eq!(cancelled.states.load(Ordering::Acquire), 2);
    }

    #[test]
    fn public_error_domain_is_stable() {
        let unavailable: GfError = AlgorithmError::Unavailable {
            algorithm: "rank.degree".into(),
        }
        .into();
        assert!(matches!(unavailable, GfError::Validation(_)));
        let cancelled: GfError = AlgorithmError::Cancelled.into();
        assert!(matches!(cancelled, GfError::Execution(_)));
    }
}
