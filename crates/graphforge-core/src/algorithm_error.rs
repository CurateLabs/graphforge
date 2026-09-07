//! Typed algorithm failures shared with the facade.
/// Structured failures produced by Rust algorithm dispatch.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AlgorithmError {
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

impl AlgorithmError {
    /// Established public fault domain for this algorithm failure.
    #[must_use]
    pub const fn fault_domain(&self) -> &'static str {
        match self {
            Self::Unavailable { .. } | Self::DuplicateCapability { .. } => "validation",
            _ => "execution",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AlgorithmError as E;
    use crate::GfError;

    #[test]
    fn every_algorithm_payload_survives_public_conversion() {
        let cases = [
            E::Unavailable {
                algorithm: "rank.future".into(),
            },
            E::DuplicateCapability {
                algorithm: "rank.degree".into(),
            },
            E::Cancelled,
            E::NodeLimit {
                observed: 3,
                limit: 2,
            },
            E::EdgeLimit {
                observed: 5,
                limit: 4,
            },
            E::OutputLimit {
                observed: 7,
                limit: 6,
            },
            E::IterationLimit {
                observed: 9,
                limit: 8,
            },
            E::StateLimit {
                observed: 11,
                limit: 10,
            },
            E::StateOverflow,
            E::SteinerOption {
                algorithm: "steiner",
                option: "terminals",
                reason: "invalid",
            },
            E::SteinerTerminalCardinality {
                algorithm: "steiner",
                observed: 1,
                required: 2,
            },
            E::SteinerTerminalOutsideProjection { uuid: [42; 16] },
            E::NonConvergence { iterations: 17 },
            E::Execution {
                message: "safe detail".into(),
            },
            E::UndefinedConductance {
                partition: "p".into(),
            },
            E::UndefinedModularity,
            E::AutomorphismCountOverflow,
            E::AutomorphismCountStateLimit {
                observed: 19,
                limit: 18,
            },
            E::UndefinedEulerCircuit,
            E::UndefinedEulerPath,
        ];
        for original in cases {
            let expected = if matches!(
                original,
                E::Unavailable { .. } | E::DuplicateCapability { .. }
            ) {
                "GF_VALIDATION"
            } else {
                "GF_EXECUTION"
            };
            let error = GfError::from(original.clone());
            assert_eq!(error.code(), expected);
            assert_eq!(
                error.to_string(),
                format!("{} error: {original}", original.fault_domain())
            );
            let GfError::Algorithm(recovered) = error else {
                panic!("algorithm kind lost")
            };
            assert_eq!(recovered, original);
        }
    }
}
