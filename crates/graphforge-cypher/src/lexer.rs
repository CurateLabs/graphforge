//! Hand-written lexer for the GraphForge Cypher parser.
//!
//! Returns `(usize, Tok, usize)` triples (start, token, end) as LALRPOP
//! expects. All keywords are case-insensitive; identifiers may be
//! backtick-quoted.
#![allow(missing_docs)]

use graphforge_ast::{ParseError, ParseErrorKind, Span};

// ---------------------------------------------------------------------------
// Token enum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    // Keywords
    Match,
    Optional,
    Where,
    Return,
    With,
    As,
    Distinct,
    Union,
    All,
    Create,
    Merge,
    On,
    Set,
    Remove,
    Delete,
    Detach,
    Call,
    Yield,
    Unwind,
    Order,
    By,
    Skip,
    Limit,
    Not,
    And,
    Or,
    Xor,
    In,
    Is,
    Null,
    Starts,
    Ends,
    Contains,
    // Compound two-word tokens — emitted by the lexer to avoid multi-token
    // predicate ambiguity in the LALR(1) parser.
    NotIn,      // NOT IN
    IsNull,     // IS NULL
    IsNotNull,  // IS NOT NULL
    StartsWith, // STARTS WITH
    EndsWith,   // ENDS WITH
    Case,
    When,
    Then,
    Else,
    End,
    True,
    False,
    ShortestPath,
    AllShortestPaths,
    Count,
    Exists,
    Reduce,
    Filter,
    Extract,
    Any,
    None,
    Single,
    Asc,
    Desc,
    Ascending,
    Descending,

    // Literals
    IntLit(i128),
    FloatLit(f64),
    StrLit(String),

    // Identifiers and parameters
    Ident(String),
    Param(String),

    // Punctuation
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Dot,
    Comma,
    Colon,
    Semi,
    Pipe,
    DotDot,

    // Operators
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Caret,
    RegexMatch,
    PlusEq,

    // Composite arrow tokens
    RelOpen,    // -[
    LeftArrow,  // <-
    RightArrow, // ->
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

pub struct Lexer<'input> {
    input: &'input str,
    pos: usize,
}

impl<'input> Lexer<'input> {
    pub fn new(input: &'input str) -> Self {
        Self { input, pos: 0 }
    }

    fn rest(&self) -> &str {
        &self.input[self.pos..]
    }

    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn advance(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn skip_whitespace_and_comments(&mut self) -> Result<(), ParseError> {
        loop {
            // whitespace
            while self.peek().map_or(false, |c| c.is_ascii_whitespace()) {
                self.advance();
            }
            // line comment
            if self.rest().starts_with("//") {
                while self.peek().map_or(false, |c| c != '\n') {
                    self.advance();
                }
                continue;
            }
            // block comment
            if self.rest().starts_with("/*") {
                let comment_start = self.pos;
                self.pos += 2;
                loop {
                    if self.rest().starts_with("*/") {
                        self.pos += 2;
                        break;
                    }
                    if self.advance().is_none() {
                        return Err(ParseError::new(
                            ParseErrorKind::UnterminatedBlockComment,
                            Span::new(comment_start, self.pos),
                            "unterminated block comment",
                        ));
                    }
                }
                continue;
            }
            break;
        }
        Ok(())
    }

    // Peek past whitespace to see if the next word forms a compound keyword.
    // Does NOT advance `pos` if the compound is not matched.
    fn peek_keyword(&self) -> Option<&str> {
        let rest = &self.input[self.pos..];
        let trimmed = rest.trim_start_matches(|c: char| c.is_ascii_whitespace());
        let word_end = trimmed
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(trimmed.len());
        if word_end == 0 {
            None
        } else {
            Some(&trimmed[..word_end])
        }
    }

    // Consume whitespace + exactly `n` bytes from the current position.
    fn skip_ws_and_advance_n(&mut self, n: usize) {
        while self.peek().map_or(false, |c| c.is_ascii_whitespace()) {
            self.advance();
        }
        self.pos += n;
    }

    // After lexing a keyword token, check whether the following word(s) form a
    // compound token. Advances past the extra word(s) if matched.
    fn maybe_compound(&mut self, tok: Tok) -> Tok {
        match tok {
            Tok::Not => {
                if self.peek_keyword().map(|w| w.eq_ignore_ascii_case("IN")) == Some(true) {
                    self.skip_ws_and_advance_n(2); // "IN"
                    return Tok::NotIn;
                }
                tok
            }
            Tok::Is => {
                match self.peek_keyword() {
                    Some(w) if w.eq_ignore_ascii_case("NULL") => {
                        self.skip_ws_and_advance_n(4);
                        return Tok::IsNull;
                    }
                    Some(w) if w.eq_ignore_ascii_case("NOT") => {
                        // Save position in case "NULL" does not follow
                        let saved = self.pos;
                        self.skip_ws_and_advance_n(3); // "NOT"
                        if self.peek_keyword().map(|w| w.eq_ignore_ascii_case("NULL")) == Some(true)
                        {
                            self.skip_ws_and_advance_n(4); // "NULL"
                            return Tok::IsNotNull;
                        }
                        // No NULL after NOT — restore and emit plain IS
                        self.pos = saved;
                        tok
                    }
                    _ => tok,
                }
            }
            Tok::Starts => {
                if self.peek_keyword().map(|w| w.eq_ignore_ascii_case("WITH")) == Some(true) {
                    self.skip_ws_and_advance_n(4);
                    return Tok::StartsWith;
                }
                tok
            }
            Tok::Ends => {
                if self.peek_keyword().map(|w| w.eq_ignore_ascii_case("WITH")) == Some(true) {
                    self.skip_ws_and_advance_n(4);
                    return Tok::EndsWith;
                }
                tok
            }
            other => other,
        }
    }

    fn read_string(&mut self, quote: char) -> Result<String, ParseError> {
        let start = self.pos - 1; // already consumed opening quote
        let mut s = String::new();
        loop {
            match self.advance() {
                None => {
                    return Err(ParseError::new(
                        ParseErrorKind::UnterminatedString,
                        Span::new(start, self.pos),
                        "unterminated string literal",
                    ));
                }
                Some(c) if c == quote => break,
                Some('\\') => {
                    match self.advance() {
                        Some('n') => s.push('\n'),
                        Some('t') => s.push('\t'),
                        Some('r') => s.push('\r'),
                        Some('\\') => s.push('\\'),
                        Some('\'') => s.push('\''),
                        Some('"') => s.push('"'),
                        Some('u') => {
                            // \uXXXX — exactly 4 hex digits required
                            let escape_start = self.pos - 2; // back past \u
                            let mut hex = String::new();
                            for _ in 0..4 {
                                match self.advance() {
                                    Some(h) => hex.push(h),
                                    None => break,
                                }
                            }
                            let n = u32::from_str_radix(&hex, 16).ok();
                            let ch = n.and_then(char::from_u32);
                            match ch {
                                Some(c) => s.push(c),
                                None => {
                                    return Err(ParseError::new(
                                        ParseErrorKind::InvalidNumericLiteral,
                                        Span::new(escape_start, self.pos),
                                        format!("invalid unicode escape: \\u{hex}"),
                                    ));
                                }
                            }
                        }
                        Some(other) => {
                            s.push('\\');
                            s.push(other);
                        }
                        None => break,
                    }
                }
                Some(c) => s.push(c),
            }
        }
        Ok(s)
    }

    fn read_backtick_ident(&mut self) -> Result<String, ParseError> {
        let start = self.pos - 1;
        let mut s = String::new();
        loop {
            match self.advance() {
                None | Some('\n') => {
                    return Err(ParseError::new(
                        ParseErrorKind::UnterminatedString,
                        Span::new(start, self.pos),
                        "unterminated backtick identifier",
                    ));
                }
                Some('`') => break,
                Some(c) => s.push(c),
            }
        }
        Ok(s)
    }

    fn read_number(&mut self, first: char) -> Result<Tok, ParseError> {
        let start = self.pos - first.len_utf8();
        let mut s = String::from(first);

        // Hex / octal
        if first == '0' {
            match self.peek() {
                Some('x') | Some('X') => {
                    s.push(self.advance().unwrap());
                    while self.peek().map_or(false, |c| c.is_ascii_hexdigit()) {
                        s.push(self.advance().unwrap());
                    }
                    return i128::from_str_radix(&s[2..], 16)
                        .map(Tok::IntLit)
                        .map_err(|_| {
                            ParseError::new(
                                ParseErrorKind::InvalidNumericLiteral,
                                Span::new(start, self.pos),
                                format!("invalid hex literal: {s}"),
                            )
                        });
                }
                Some('o') | Some('O') => {
                    s.push(self.advance().unwrap());
                    while self.peek().map_or(false, |c| matches!(c, '0'..='7')) {
                        s.push(self.advance().unwrap());
                    }
                    return i128::from_str_radix(&s[2..], 8)
                        .map(Tok::IntLit)
                        .map_err(|_| {
                            ParseError::new(
                                ParseErrorKind::InvalidNumericLiteral,
                                Span::new(start, self.pos),
                                format!("invalid octal literal: {s}"),
                            )
                        });
                }
                _ => {}
            }
        }

        while self.peek().map_or(false, |c| c.is_ascii_digit()) {
            s.push(self.advance().unwrap());
        }

        let mut is_float = false;
        if self.peek() == Some('.') {
            // look ahead one more: if next after '.' is a digit, it's a float
            let rest = &self.input[self.pos + 1..];
            if rest.chars().next().map_or(false, |c| c.is_ascii_digit()) {
                is_float = true;
                s.push(self.advance().unwrap()); // '.'
                while self.peek().map_or(false, |c| c.is_ascii_digit()) {
                    s.push(self.advance().unwrap());
                }
            }
        }
        if self.peek().map_or(false, |c| c == 'e' || c == 'E') {
            is_float = true;
            s.push(self.advance().unwrap());
            if self.peek().map_or(false, |c| c == '+' || c == '-') {
                s.push(self.advance().unwrap());
            }
            while self.peek().map_or(false, |c| c.is_ascii_digit()) {
                s.push(self.advance().unwrap());
            }
        }

        if is_float {
            s.parse::<f64>().map(Tok::FloatLit).map_err(|_| {
                ParseError::new(
                    ParseErrorKind::InvalidNumericLiteral,
                    Span::new(start, self.pos),
                    format!("invalid float literal: {s}"),
                )
            })
        } else {
            s.parse::<i128>().map(Tok::IntLit).map_err(|_| {
                ParseError::new(
                    ParseErrorKind::InvalidNumericLiteral,
                    Span::new(start, self.pos),
                    format!("invalid integer literal: {s}"),
                )
            })
        }
    }

    fn read_leading_dot_float(&mut self, start: usize) -> Result<Tok, ParseError> {
        let mut s = String::from(".");
        while self.peek().map_or(false, |c| c.is_ascii_digit()) {
            s.push(self.advance().unwrap());
        }
        if self.peek().map_or(false, |c| c == 'e' || c == 'E') {
            s.push(self.advance().unwrap());
            if self.peek().map_or(false, |c| c == '+' || c == '-') {
                s.push(self.advance().unwrap());
            }
            while self.peek().map_or(false, |c| c.is_ascii_digit()) {
                s.push(self.advance().unwrap());
            }
        }
        s.parse::<f64>().map(Tok::FloatLit).map_err(|_| {
            ParseError::new(
                ParseErrorKind::InvalidNumericLiteral,
                Span::new(start, self.pos),
                format!("invalid float literal: {s}"),
            )
        })
    }
}

fn keyword(s: &str) -> Option<Tok> {
    match s.to_uppercase().as_str() {
        "MATCH" => Some(Tok::Match),
        "OPTIONAL" => Some(Tok::Optional),
        "WHERE" => Some(Tok::Where),
        "RETURN" => Some(Tok::Return),
        "WITH" => Some(Tok::With),
        "AS" => Some(Tok::As),
        "DISTINCT" => Some(Tok::Distinct),
        "UNION" => Some(Tok::Union),
        "ALL" => Some(Tok::All),
        "CREATE" => Some(Tok::Create),
        "MERGE" => Some(Tok::Merge),
        "ON" => Some(Tok::On),
        "SET" => Some(Tok::Set),
        "REMOVE" => Some(Tok::Remove),
        "DELETE" => Some(Tok::Delete),
        "DETACH" => Some(Tok::Detach),
        "CALL" => Some(Tok::Call),
        "YIELD" => Some(Tok::Yield),
        "UNWIND" => Some(Tok::Unwind),
        "ORDER" => Some(Tok::Order),
        "BY" => Some(Tok::By),
        "SKIP" => Some(Tok::Skip),
        "LIMIT" => Some(Tok::Limit),
        "NOT" => Some(Tok::Not),
        "AND" => Some(Tok::And),
        "OR" => Some(Tok::Or),
        "XOR" => Some(Tok::Xor),
        "IN" => Some(Tok::In),
        "IS" => Some(Tok::Is),
        "NULL" => Some(Tok::Null),
        "STARTS" => Some(Tok::Starts),
        "ENDS" => Some(Tok::Ends),
        "CONTAINS" => Some(Tok::Contains),
        "CASE" => Some(Tok::Case),
        "WHEN" => Some(Tok::When),
        "THEN" => Some(Tok::Then),
        "ELSE" => Some(Tok::Else),
        "END" => Some(Tok::End),
        "TRUE" => Some(Tok::True),
        "FALSE" => Some(Tok::False),
        "SHORTESTPATH" => Some(Tok::ShortestPath),
        "ALLSHORTESTPATHS" => Some(Tok::AllShortestPaths),
        "COUNT" => Some(Tok::Count),
        "EXISTS" => Some(Tok::Exists),
        "REDUCE" => Some(Tok::Reduce),
        "FILTER" => Some(Tok::Filter),
        "EXTRACT" => Some(Tok::Extract),
        "ANY" => Some(Tok::Any),
        "NONE" => Some(Tok::None),
        "SINGLE" => Some(Tok::Single),
        "ASC" => Some(Tok::Asc),
        "DESC" => Some(Tok::Desc),
        "ASCENDING" => Some(Tok::Ascending),
        "DESCENDING" => Some(Tok::Descending),
        _ => None,
    }
}

pub type Spanned<T> = Result<(usize, T, usize), ParseError>;

impl<'input> Iterator for Lexer<'input> {
    type Item = Spanned<Tok>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Err(e) = self.skip_whitespace_and_comments() {
            return Some(Err(e));
        }
        let start = self.pos;
        let c = self.advance()?;

        let tok = match c {
            '(' => Tok::LParen,
            ')' => Tok::RParen,
            '[' => Tok::LBracket,
            ']' => Tok::RBracket,
            '{' => Tok::LBrace,
            '}' => Tok::RBrace,
            ',' => Tok::Comma,
            ':' => Tok::Colon,
            ';' => Tok::Semi,
            '|' => Tok::Pipe,
            '^' => Tok::Caret,
            '%' => Tok::Percent,
            '/' => Tok::Slash,
            '*' => Tok::Star,
            '+' => {
                if self.peek() == Some('=') {
                    self.advance();
                    Tok::PlusEq
                } else {
                    Tok::Plus
                }
            }
            '.' => {
                if self.peek() == Some('.') {
                    self.advance();
                    Tok::DotDot
                } else if self.peek().map_or(false, |c| c.is_ascii_digit()) {
                    match self.read_leading_dot_float(start) {
                        Ok(t) => t,
                        Err(e) => return Some(Err(e)),
                    }
                } else {
                    Tok::Dot
                }
            }
            '=' => {
                if self.peek() == Some('~') {
                    self.advance();
                    Tok::RegexMatch
                } else {
                    Tok::Eq
                }
            }
            '!' => {
                if self.peek() == Some('=') {
                    self.advance();
                    Tok::Neq
                } else {
                    return Some(Err(ParseError::new(
                        ParseErrorKind::UnexpectedChar,
                        Span::new(start, self.pos),
                        "unexpected '!'",
                    )));
                }
            }
            '<' => match self.peek() {
                Some('=') => {
                    self.advance();
                    Tok::Lte
                }
                Some('>') => {
                    self.advance();
                    Tok::Neq
                }
                Some('-') => {
                    self.advance();
                    Tok::LeftArrow
                }
                _ => Tok::Lt,
            },
            '>' => {
                if self.peek() == Some('=') {
                    self.advance();
                    Tok::Gte
                } else {
                    Tok::Gt
                }
            }
            '-' => match self.peek() {
                Some('[') => {
                    self.advance();
                    Tok::RelOpen
                }
                Some('>') => {
                    self.advance();
                    Tok::RightArrow
                }
                _ => Tok::Minus,
            },
            '\'' | '"' => match self.read_string(c) {
                Ok(s) => Tok::StrLit(s),
                Err(e) => return Some(Err(e)),
            },
            '`' => match self.read_backtick_ident() {
                Ok(s) => Tok::Ident(s),
                Err(e) => return Some(Err(e)),
            },
            '$' => {
                // parameter: $name (identifier) or $0, $1, ... (decimal index)
                let mut name = String::new();
                while self
                    .peek()
                    .map_or(false, |c| c.is_alphanumeric() || c == '_')
                {
                    name.push(self.advance().unwrap());
                }
                if name.is_empty() {
                    return Some(Err(ParseError::new(
                        ParseErrorKind::InvalidParameter,
                        Span::new(start, self.pos),
                        "empty parameter name after '$'",
                    )));
                }
                // Reject mixed names like $123abc — must be all-digits or a valid identifier
                let all_digits = name.chars().all(|c| c.is_ascii_digit());
                let valid_ident = name
                    .chars()
                    .next()
                    .map_or(false, |c| c.is_alphabetic() || c == '_');
                if !all_digits && !valid_ident {
                    return Some(Err(ParseError::new(
                        ParseErrorKind::InvalidParameter,
                        Span::new(start, self.pos),
                        format!(
                            "parameter name must be an identifier or decimal integer, got '{name}'"
                        ),
                    )));
                }
                Tok::Param(name)
            }
            c if c.is_ascii_digit() => match self.read_number(c) {
                Ok(t) => t,
                Err(e) => return Some(Err(e)),
            },
            c if c.is_alphabetic() || c == '_' => {
                let mut word = String::from(c);
                while self
                    .peek()
                    .map_or(false, |c| c.is_alphanumeric() || c == '_')
                {
                    word.push(self.advance().unwrap());
                }
                match keyword(&word) {
                    Some(kw) => self.maybe_compound(kw),
                    None => Tok::Ident(word),
                }
            }
            other => {
                return Some(Err(ParseError::new(
                    ParseErrorKind::UnexpectedChar,
                    Span::new(start, self.pos),
                    format!("unexpected character: {other:?}"),
                )));
            }
        };

        Some(Ok((start, tok, self.pos)))
    }
}
