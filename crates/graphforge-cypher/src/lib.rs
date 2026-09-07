//! GraphForge Cypher parser — recursive descent + Pratt expression parser.
//!
//! Parsing accepts query text and returns a syntax-faithful AST or parse error.
//! Compiler-stage explanations belong to `graphforge_api::GraphForge`.
#![forbid(unsafe_code)]
#![allow(missing_docs)]

pub mod lexer;
pub mod parser;

pub use graphforge_ast::{AstQuery, ParseError, ParseErrorKind, Token};
pub use graphforge_core::Span;

/// Parse a Cypher query string into an [`AstQuery`].
///
/// # Errors
/// Returns [`ParseError`] on any lexer or syntax error.
pub fn parse(input: &str) -> Result<AstQuery, ParseError> {
    parser::parse(input)
}
