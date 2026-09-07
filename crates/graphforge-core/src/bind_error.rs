//! Binder diagnostics shared with the facade.
use crate::Span;

/// The kind of a binder error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindErrorKind {
    /// A label name was not found in the ontology (strict mode).
    UnknownLabel,
    /// A relation type name was not found in the ontology (strict mode).
    UnknownRelationType,
    /// A property name was not found in the ontology (strict mode).
    UnknownProperty,
    /// A variable was referenced before it was introduced by a MATCH pattern.
    UndeclaredVariable,
    /// The same variable was introduced more than once in a conflicting way.
    DuplicateVariable,
    /// A property name is ambiguous across multiple owner labels.
    AmbiguousProperty,
    /// A clause is recognized by the parser but not yet implemented by the
    /// binder/executor (SET/REMOVE/CALL). Surfaced as an error instead
    /// of a silent no-op (#724).
    UnsupportedClause,
    /// A `DELETE` target was not a plain bound variable (e.g. `DELETE n.prop`
    /// or `DELETE n + 1`). Only `DELETE <var>` is supported (#740).
    InvalidDeleteTarget,
    /// A variable is used with two incompatible kinds — e.g. a relationship
    /// variable reused as a node pattern (`MATCH ()-[r]-() MATCH (r)`).
    /// openCypher rejects this as a compile-time `VariableTypeConflict` (#956).
    VariableKindConflict,
    /// An already-bound variable is re-declared by a CREATE/MERGE pattern
    /// (`MATCH (a) CREATE (a)`, a reused relationship variable, a bound node
    /// given new labels/properties). openCypher `VariableAlreadyBound` (#956).
    VariableAlreadyBound,
    /// A clause or function argument is semantically invalid — a non-integer
    /// `range()`/`SKIP`/`LIMIT` argument, an aggregate in `WHERE`, a CREATE
    /// relationship with no/var-length/undirected type, a UNION column
    /// mismatch, etc. Covers openCypher's `ArgumentError` / `InvalidAggregation`
    /// / `NoSingleRelationshipType` / … — the harness checks the phase, not the
    /// sub-code, so one kind with a descriptive message suffices (#956).
    InvalidArgument,
    /// A composed ontology symbol has multiple valid candidates.
    AmbiguousComposedSymbol,
    /// A composed ontology qualifier or bridge path is invalid/conflicting.
    CompositionConflict,
}

/// A semantic error or warning produced by the binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindError {
    /// The kind of error.
    pub kind: BindErrorKind,
    /// Source location of the offending token.
    pub span: Span,
    /// Human-readable message.
    pub message: String,
}

impl BindError {
    /// Construct a typed binder diagnostic.
    #[must_use]
    pub fn new(kind: BindErrorKind, span: Span, message: impl Into<String>) -> Self {
        Self {
            kind,
            span,
            message: message.into(),
        }
    }
}
