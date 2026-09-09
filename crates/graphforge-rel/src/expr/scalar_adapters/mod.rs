//! IR/DataFusion literal representations and Cypher conversion adapters.

mod conversions;
mod literals;

#[cfg(test)]
mod tests;

pub(in crate::expr) use conversions::{
    CYPHER_TO_BOOLEAN, CYPHER_TO_FLOAT, CYPHER_TO_INTEGER, CYPHER_TO_STRING,
};
#[cfg(test)]
pub(in crate::expr) use conversions::{
    CypherConversion, CypherConversionKind, CypherToString, cypher_float_string, to_cypher_boolean,
    to_cypher_float, to_cypher_integer, to_cypher_string, trunc_float_to_i64,
};
pub use literals::{ir_literal_to_scalar, scalar_to_ir_literal};
pub(in crate::expr) use literals::{render_temporal, spatial_scalar};
