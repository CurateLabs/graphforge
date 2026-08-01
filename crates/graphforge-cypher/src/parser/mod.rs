pub mod clauses;
pub mod expr;
pub mod patterns;

pub use clauses::parse_query;
pub use expr::parse_expr;
pub use patterns::{parse_node_pattern, parse_pattern, parse_pattern_list};

use crate::lexer::{Lexer, Tok};
use graphforge_ast::{AstQuery, ParseError, ParseErrorKind};
use graphforge_core::Span;

/// Parse a Cypher query string into an [`AstQuery`].
pub fn parse(input: &str) -> Result<AstQuery, ParseError> {
    let mut ts = TokenStream::new(input)?;
    clauses::parse_query(&mut ts)
}

/// Token stream backed by an eagerly-collected `Vec`.
///
/// Lexing is separated from parsing: `new()` collects all tokens upfront and
/// fails immediately on the first lex error.  The parser then walks a clean
/// `Vec<(usize, Tok, usize)>` with no further fallibility from the lexer.
#[derive(Clone)]
pub struct TokenStream<'input> {
    tokens: Vec<(usize, Tok, usize)>,
    pos: usize,
    input: &'input str,
}

impl<'input> TokenStream<'input> {
    /// Lex the entire input upfront.  Returns the first `ParseError` encountered.
    pub fn new(input: &'input str) -> Result<Self, ParseError> {
        let tokens = Lexer::new(input).collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            tokens,
            pos: 0,
            input,
        })
    }

    // -----------------------------------------------------------------------
    // Lookahead
    // -----------------------------------------------------------------------

    /// Peek at the current token without consuming it.
    pub fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.pos).map(|(_, tok, _)| tok)
    }

    /// Peek `n` positions ahead (0 == current).
    pub fn peek_n(&self, n: usize) -> Option<&Tok> {
        self.tokens.get(self.pos + n).map(|(_, tok, _)| tok)
    }

    /// Return `true` if the current token is `expected`.
    pub fn at(&self, expected: &Tok) -> bool {
        self.peek() == Some(expected)
    }

    /// Return `true` if there are no more tokens.
    pub fn is_empty(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    // -----------------------------------------------------------------------
    // Advancing
    // -----------------------------------------------------------------------

    /// Consume and return the current token triple, or `None` at EOF.
    pub fn advance(&mut self) -> Option<(usize, Tok, usize)> {
        if self.pos < self.tokens.len() {
            let item = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(item)
        } else {
            None
        }
    }

    /// Consume the current token if it equals `expected`, returning its span.
    /// Produces `UnexpectedToken` or `UnexpectedEof` on mismatch.
    pub fn eat(&mut self, expected: &Tok) -> Result<(usize, usize), ParseError> {
        match self.tokens.get(self.pos) {
            Some((l, tok, r)) if tok == expected => {
                let span = (*l, *r);
                self.pos += 1;
                Ok(span)
            }
            Some((l, tok, r)) => Err(ParseError::new(
                ParseErrorKind::UnexpectedToken {
                    found: format!("{tok:?}"),
                    expected: vec![format!("{expected:?}")],
                },
                Span::new(*l, *r),
                format!("expected {expected:?}, found {tok:?}"),
            )),
            None => Err(ParseError::new(
                ParseErrorKind::UnexpectedEof {
                    expected: vec![format!("{expected:?}")],
                },
                Span::new(self.current_pos(), self.current_pos()),
                format!("expected {expected:?}, found end of input"),
            )),
        }
    }

    /// Consume the current token if it equals `expected`.
    /// Returns `true` if consumed, `false` if not present (no error).
    pub fn eat_if(&mut self, expected: &Tok) -> bool {
        if self.at(expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    // -----------------------------------------------------------------------
    // Position helpers
    // -----------------------------------------------------------------------

    /// Byte offset of the start of the current token (or end-of-input).
    pub fn current_pos(&self) -> usize {
        self.tokens
            .get(self.pos)
            .map(|(l, _, _)| *l)
            .unwrap_or(self.input.len())
    }

    /// Span covering a single point at `current_pos`.
    pub fn current_span(&self) -> Span {
        let p = self.current_pos();
        Span::new(p, p)
    }

    /// Span from `start` byte offset to `current_pos`.
    pub fn span_from(&self, start: usize) -> Span {
        Span::new(start, self.current_pos())
    }

    /// The source text covered by `span` (byte offsets into the original input).
    /// Used to capture an un-aliased projection item's verbatim text for its
    /// default column name (openCypher names columns by the expression as written).
    pub fn text(&self, span: Span) -> &'input str {
        self.input.get(span.start..span.end).unwrap_or("")
    }

    // -----------------------------------------------------------------------
    // Error helpers
    // -----------------------------------------------------------------------

    /// Build an `UnexpectedToken` (or `UnexpectedEof`) error at the current position.
    pub fn err(&self, msg: impl Into<String>) -> ParseError {
        match self.tokens.get(self.pos) {
            Some((l, tok, r)) => ParseError::new(
                ParseErrorKind::UnexpectedToken {
                    found: format!("{tok:?}"),
                    expected: vec![],
                },
                Span::new(*l, *r),
                msg,
            ),
            None => ParseError::new(
                ParseErrorKind::UnexpectedEof { expected: vec![] },
                Span::new(self.input.len(), self.input.len()),
                msg,
            ),
        }
    }

    /// Build an error at an explicit span.
    pub fn err_at(&self, span: Span, kind: ParseErrorKind, msg: impl Into<String>) -> ParseError {
        ParseError::new(kind, span, msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Tok;

    fn ts(input: &str) -> TokenStream<'_> {
        TokenStream::new(input).expect("lex failed")
    }

    #[test]
    fn new_collects_all_tokens() {
        let s = ts("MATCH (n)");
        assert_eq!(s.tokens.len(), 4); // MATCH ( Ident("n") )
    }

    #[test]
    fn peek_does_not_consume() {
        let s = ts("RETURN 1");
        assert_eq!(s.peek(), Some(&Tok::Return));
        assert_eq!(s.peek(), Some(&Tok::Return));
        assert_eq!(s.pos, 0);
    }

    #[test]
    fn peek_n() {
        let s = ts("RETURN 1");
        assert_eq!(s.peek_n(0), Some(&Tok::Return));
        assert!(matches!(s.peek_n(1), Some(Tok::IntLit(1))));
        assert_eq!(s.peek_n(2), None);
    }

    #[test]
    fn advance_consumes() {
        let mut s = ts("RETURN 1");
        let (_, tok, _) = s.advance().unwrap();
        assert_eq!(tok, Tok::Return);
        assert_eq!(s.pos, 1);
    }

    #[test]
    fn eat_success() {
        let mut s = ts("RETURN 1");
        s.eat(&Tok::Return).expect("should succeed");
        assert_eq!(s.pos, 1);
    }

    #[test]
    fn eat_mismatch_errors() {
        let mut s = ts("RETURN 1");
        let err = s.eat(&Tok::Match).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("Return") || msg.contains("expected"), "{msg}");
    }

    #[test]
    fn eat_eof_errors() {
        let mut s = ts("");
        let err = s.eat(&Tok::Match).unwrap_err();
        // Should be UnexpectedEof
        assert!(matches!(err.kind, ParseErrorKind::UnexpectedEof { .. }));
    }

    #[test]
    fn eat_if() {
        let mut s = ts("MATCH WHERE");
        assert!(s.eat_if(&Tok::Match));
        assert!(!s.eat_if(&Tok::Return));
        assert_eq!(s.pos, 1);
    }

    #[test]
    fn is_empty() {
        let mut s = ts("RETURN");
        assert!(!s.is_empty());
        s.advance();
        assert!(s.is_empty());
    }

    #[test]
    fn lex_error_propagates() {
        // `@` is not a valid Cypher character — lexer should error
        let result = TokenStream::new("MATCH @");
        assert!(result.is_err());
    }

    #[test]
    fn span_from() {
        let mut s = ts("RETURN 1");
        let start = s.current_pos();
        s.advance(); // consume RETURN
        let span = s.span_from(start);
        assert!(span.start < span.end);
    }
}
