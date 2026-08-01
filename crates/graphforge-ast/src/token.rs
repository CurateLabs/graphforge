//! Token contract for the GraphForge Cypher lexer.
#![allow(missing_docs)]
//!
//! This enum is the **frozen ABI** between the lexer (`graphforge-cypher`) and the
//! differential test harness (`graphforge-cypher/tests/`). Removing or renaming a
//! variant is a breaking change. Adding new variants is allowed only in
//! minor releases and must be accompanied by a `#[non_exhaustive]` guard.

use graphforge_core::Span;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Keyword list
// ---------------------------------------------------------------------------

/// Every reserved keyword in the openCypher grammar.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Keyword {
    // Query structure
    Match,
    Optional,
    Where,
    Return,
    With,
    As,
    Distinct,
    Union,
    All,
    // Write clauses
    Create,
    Merge,
    On,
    Set,
    Remove,
    Delete,
    Detach,
    // Sub-queries / procedures
    Call,
    Yield,
    // Iteration
    Unwind,
    // Ordering / pagination
    Order,
    By,
    Skip,
    Limit,
    // Logical operators
    Not,
    And,
    Or,
    Xor,
    // Predicates
    In,
    Is,
    Null,
    Starts,
    Ends,
    Contains,
    // Conditional
    Case,
    When,
    Then,
    Else,
    End,
    // Boolean literals
    True,
    False,
    // List predicates
    Any,
    None,
    Single,
    Exists,
    // Path functions
    ShortestPath,
    AllShortestPaths,
    // Aggregation
    Count,
    // Reduce / comprehension
    Reduce,
    Filter,
    Extract,
}

// ---------------------------------------------------------------------------
// Token
// ---------------------------------------------------------------------------

/// A lexed token emitted by the GraphForge Cypher lexer.
///
/// Every variant carries a [`Span`] so that downstream consumers can report
/// precise error locations. The lexer never omits the span — not even for
/// whitespace tokens that the parser would normally discard.
///
/// # Stability
///
/// This enum is `#[non_exhaustive]`. Parsers and test harnesses must match
/// only the variants they care about and use a wildcard arm.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Token {
    // -----------------------------------------------------------------------
    // Keywords
    // -----------------------------------------------------------------------
    /// A reserved keyword (case-insensitive in openCypher).
    Keyword(Keyword, Span),

    // -----------------------------------------------------------------------
    // Literals
    // -----------------------------------------------------------------------
    /// A decimal or hexadecimal integer literal.
    IntLit(i64, Span),
    /// A floating-point literal.
    FloatLit(f64, Span),
    /// A single- or double-quoted string literal (escape sequences resolved).
    StrLit(String, Span),
    /// `true` or `false` (also surfaced as `Keyword::True/False` for parity).
    BoolLit(bool, Span),
    /// The `null` literal.
    NullLit(Span),

    // -----------------------------------------------------------------------
    // Identifiers and parameters
    // -----------------------------------------------------------------------
    /// An unquoted or backtick-quoted identifier.
    Ident(String, Span),
    /// A Cypher parameter: `$name` or `$0`.
    Param(String, Span),

    // -----------------------------------------------------------------------
    // Punctuation
    // -----------------------------------------------------------------------
    /// `(`
    LParen(Span),
    /// `)`
    RParen(Span),
    /// `[`
    LBracket(Span),
    /// `]`
    RBracket(Span),
    /// `{`
    LBrace(Span),
    /// `}`
    RBrace(Span),
    /// `.`
    Dot(Span),
    /// `,`
    Comma(Span),
    /// `:`
    Colon(Span),
    /// `;`
    Semi(Span),
    /// `|`
    Pipe(Span),
    /// `..` (variable-length relationship range separator)
    DotDot(Span),

    // -----------------------------------------------------------------------
    // Operators
    // -----------------------------------------------------------------------
    /// `=`
    Eq(Span),
    /// `<>` or `!=`
    Neq(Span),
    /// `<`
    Lt(Span),
    /// `<=`
    Lte(Span),
    /// `>`
    Gt(Span),
    /// `>=`
    Gte(Span),
    /// `+`
    Plus(Span),
    /// `-`
    Minus(Span),
    /// `*`
    Star(Span),
    /// `/`
    Slash(Span),
    /// `%`
    Percent(Span),
    /// `^`
    Caret(Span),
    /// `=~` (regular expression match)
    RegexMatch(Span),

    // -----------------------------------------------------------------------
    // Trivia (retained for source-accurate round-tripping)
    // -----------------------------------------------------------------------
    /// Whitespace (space, tab, newline).
    Whitespace(Span),
    /// A single- or multi-line comment.
    Comment(String, Span),

    // -----------------------------------------------------------------------
    // Sentinel
    // -----------------------------------------------------------------------
    /// End of input.
    Eof(Span),
}

impl Token {
    /// Return the [`Span`] of this token.
    #[must_use]
    pub fn span(&self) -> Span {
        match self {
            Self::Keyword(_, s)
            | Self::IntLit(_, s)
            | Self::FloatLit(_, s)
            | Self::StrLit(_, s)
            | Self::BoolLit(_, s)
            | Self::Param(_, s)
            | Self::Ident(_, s)
            | Self::Comment(_, s)
            | Self::NullLit(s)
            | Self::LParen(s)
            | Self::RParen(s)
            | Self::LBracket(s)
            | Self::RBracket(s)
            | Self::LBrace(s)
            | Self::RBrace(s)
            | Self::Dot(s)
            | Self::Comma(s)
            | Self::Colon(s)
            | Self::Semi(s)
            | Self::Pipe(s)
            | Self::DotDot(s)
            | Self::Eq(s)
            | Self::Neq(s)
            | Self::Lt(s)
            | Self::Lte(s)
            | Self::Gt(s)
            | Self::Gte(s)
            | Self::Plus(s)
            | Self::Minus(s)
            | Self::Star(s)
            | Self::Slash(s)
            | Self::Percent(s)
            | Self::Caret(s)
            | Self::RegexMatch(s)
            | Self::Whitespace(s)
            | Self::Eof(s) => *s,
        }
    }

    /// Return `true` if this token is trivia (whitespace or comment) that
    /// the parser should skip.
    #[must_use]
    pub fn is_trivia(&self) -> bool {
        matches!(self, Self::Whitespace(_) | Self::Comment(_, _))
    }
}
