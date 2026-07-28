//! Structured parse error type for the GraphForge Cypher parser.

use gf_core::Span;
use serde::{Deserialize, Serialize};

/// The kind of parse error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ParseErrorKind {
    /// The lexer encountered a byte sequence it could not tokenize.
    UnexpectedChar,
    /// The parser encountered a token that does not fit the grammar at this
    /// position.
    UnexpectedToken {
        /// What was found.
        found: String,
        /// What the parser expected (human-readable).
        expected: Vec<String>,
    },
    /// A string literal was opened but never closed.
    UnterminatedString,
    /// A block comment was opened with `/*` but never closed with `*/`.
    UnterminatedBlockComment,
    /// An integer or float literal could not be parsed.
    InvalidNumericLiteral,
    /// A `$` parameter prefix was not followed by a valid name or index.
    InvalidParameter,
    /// A query ended before it was syntactically complete.
    UnexpectedEof {
        /// What the parser expected at end of input.
        expected: Vec<String>,
    },
}

/// A structured parse error produced by `gf-cypher`.
///
/// `ParseError` is the error type in the `Result` returned by
/// `gf_cypher::parse`. The differential test harness uses the `span` and
/// `kind` fields to assert that both parsers flag the same source location.
/// A structured parse error produced by `gf-cypher`.
///
/// `ParseError` is the error type in the `Result` returned by
/// `gf_cypher::parse`. The differential test harness uses the `span` and
/// `kind` fields to assert that both parsers flag the same source location.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParseError {
    /// Structured classification of the failure.
    pub kind: ParseErrorKind,
    /// Source location of the offending token or character.
    pub span: Span,
    /// Human-readable explanation (may include context beyond `kind`).
    pub message: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind_str = match &self.kind {
            ParseErrorKind::UnexpectedChar => "unexpected character".to_owned(),
            ParseErrorKind::UnexpectedToken { found, .. } => {
                format!("unexpected token '{found}'")
            }
            ParseErrorKind::UnterminatedString => "unterminated string".to_owned(),
            ParseErrorKind::UnterminatedBlockComment => "unterminated block comment".to_owned(),
            ParseErrorKind::InvalidNumericLiteral => "invalid numeric literal".to_owned(),
            ParseErrorKind::InvalidParameter => "invalid parameter".to_owned(),
            ParseErrorKind::UnexpectedEof { .. } => "unexpected end of input".to_owned(),
        };
        write!(f, "{kind_str} at {}", self.span)
    }
}

impl std::error::Error for ParseError {}

impl ParseError {
    /// Create a new `ParseError`.
    #[must_use]
    pub fn new(kind: ParseErrorKind, span: Span, message: impl Into<String>) -> Self {
        Self {
            kind,
            span,
            message: message.into(),
        }
    }
}
