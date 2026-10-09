//! Recognized Cypher constructs whose execution is not implemented.

use serde::{Deserialize, Serialize};

/// A specific unsupported Cypher construct, shared by compiler diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum UnsupportedCypherFeature {
    /// A CALL body, including correlated and UNION subqueries.
    CallSubquery,
    /// A COUNT block expression.
    CountSubquery,
    /// An accumulator-style reduce expression.
    Reduce,
    /// A shortestPath/allShortestPaths pattern.
    ShortestPath,
    /// A duration map whose values are not literal constants.
    DynamicDuration,
    /// Relationship identity through id().
    IdentityFunction,
    /// Endpoint lookup on a relationship without a bound fixed-hop endpoint.
    UnboundRelationshipEndpoint,
    /// An ALL predicate reading indexed variable-hop relationship properties.
    IndexedPathRelationshipPredicate,
    /// A variable-hop relationship with alternative relationship types.
    VariableLengthRelationshipAlternation,
    /// Parameter rows driving property matches across different node labels.
    ParameterRowsAcrossLabels,
}

impl UnsupportedCypherFeature {
    /// Stable explanation used by the public typed not-supported error.
    #[must_use]
    pub const fn description(self) -> &'static str {
        match self {
            Self::CallSubquery => "Cypher CALL subqueries",
            Self::CountSubquery => "Cypher COUNT subqueries",
            Self::Reduce => "Cypher reduce accumulator expressions",
            Self::ShortestPath => "Cypher shortest-path patterns",
            Self::DynamicDuration => "Cypher duration maps with nonliteral values",
            Self::IdentityFunction => "Cypher id identity function",
            Self::UnboundRelationshipEndpoint => {
                "Cypher endpoint functions on unbound relationship values"
            }
            Self::IndexedPathRelationshipPredicate => {
                "Cypher ALL predicates over indexed variable-length relationship properties"
            }
            Self::VariableLengthRelationshipAlternation => {
                "Cypher variable-length relationship type alternation"
            }
            Self::ParameterRowsAcrossLabels => {
                "Cypher parameter rows matching properties across different node labels"
            }
        }
    }
}
