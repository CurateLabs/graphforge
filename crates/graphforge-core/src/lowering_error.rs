//! Lowering diagnostics shared with the facade.
/// Errors that can occur when lowering an IR expression to DataFusion.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LoweringError {
    /// A function call name has no
    /// DataFusion built-in equivalent.
    #[error("unknown built-in function: {0}")]
    UnknownFunction(String),

    /// An IR expression variant cannot be lowered yet (e.g. `MapLiteral`).
    #[error("unsupported expression: {0}")]
    UnsupportedExpr(String),

    /// A variable ID referenced by a `VarRef` or `PropertyAccess` is not in the
    /// variable map.
    #[error("unbound variable: VarId({0})")]
    UnboundVar(u32),

    /// A genuine Cypher type error caught at planning — e.g. a quantifier
    /// predicate that cannot apply to the list's element type (`x % 2` over a
    /// string list). A deliberate validation rejection (openCypher
    /// `InvalidArgumentType`), distinct from a capability gap. (#955)
    #[error("invalid argument type: {0}")]
    InvalidType(String),
}

impl LoweringError {
    /// Existing public fault domain, with InvalidType corrected for v0.6.0.
    #[must_use]
    pub const fn fault_domain(&self) -> &'static str {
        match self {
            Self::InvalidType(_) => "validation",
            _ => "plan",
        }
    }
}
