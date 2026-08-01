//! Procedure signatures and deterministic procedure fixtures.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{IrLiteral, VarId};

/// A named value in a procedure signature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcedureField {
    /// Field name used for implicit arguments or yielded values.
    pub name: String,
    /// openCypher type name, retained for compile-time validation.
    pub type_name: String,
    /// Whether the field accepts null.
    pub nullable: bool,
}

/// A deterministic procedure definition used by query planning and the TCK.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureDefinition {
    /// Fully-qualified procedure name.
    pub name: String,
    /// Ordered input signature.
    pub inputs: Vec<ProcedureField>,
    /// Ordered output signature.
    pub outputs: Vec<ProcedureField>,
    /// Fixture rows containing all inputs followed by all outputs.
    pub rows: Vec<Vec<IrLiteral>>,
}

/// Procedure definitions keyed by fully-qualified name.
pub type ProcedureRegistry = HashMap<String, ProcedureDefinition>;

/// One output selected by a `YIELD` list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcedureYield {
    /// Name in the registered procedure output signature.
    pub field: String,
    /// Query-visible name after an optional `AS` alias.
    pub alias: String,
    /// Variable introduced into the downstream query scope.
    pub var: VarId,
}
