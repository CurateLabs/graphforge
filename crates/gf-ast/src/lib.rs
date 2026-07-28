//! GraphForge AST — syntax-faithful, span-rich parse tree.
//!
//! The AST is internal to the compiler pipeline. `gf-cypher` produces it;
//! `gf-ir` consumes it. No other crate should depend on this crate directly.
//!
//! # Module layout
//!
//! - [`token`] — `Token` enum and `Keyword` enum (frozen lexer ABI)
//! - [`parse_error`] — `ParseError` and `ParseErrorKind`
//! - [`ast`] — all AST node types
#![forbid(unsafe_code)]

pub mod ast;
pub mod parse_error;
pub mod token;

// Flat re-exports for convenience
pub use ast::{
    AstClause, AstQuery, BinaryOp, BinaryOpKind, CallClause, CaseExpr, CreateClause, DeleteClause,
    DialectVersion, Direction, ExistentialSubquery, ExistentialSubqueryBody, Expr, FunctionCall,
    LabelPredicate, ListComprehension, ListLiteral, Literal, MapLiteral, MatchClause, MergeClause,
    NodePattern, OrderByClause, ParamRef, PathElement, PathPattern, PatternComprehension,
    PatternPredicate, PropertyAccess, Quantifier, QuantifierKind, RelPattern, RemoveClause,
    RemoveItem, ReturnClause, ReturnItem, SetClause, SetItem, SortItem, SortOrder, StringOpKind,
    UnaryOp, UnaryOpKind, UnionClause, UnwindClause, VarRef, WhenClause, WhereClause, WithClause,
};
pub use gf_core::Span;
pub use parse_error::{ParseError, ParseErrorKind};
pub use token::{Keyword, Token};

#[cfg(test)]
mod tests;
