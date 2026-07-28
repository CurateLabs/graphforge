# ADR 0002: Recursive Descent + Pratt Parser for gf-cypher

**Date:** 2026-05-28
**Status:** Accepted
**Supersedes:** Partial update to ADR 0001 (parser algorithm choice)

---

## Context

The Cypher parser in `gf-cypher` was evaluated first as a LALRPOP grammar. Two approaches were attempted in sequence; both were abandoned in favor of the hand-written parser below.

### Attempt 1: LALR(1) with `#[precedence]`

LALRPOP's `#[LALR]` mode with `#[precedence]` and `#[assoc]` annotations was the first approach — chosen because it mirrors a Lark-style declarative grammar and LALRPOP presents it as idiomatic. It produced **5 unresolvable shift/reduce conflicts**:

1. **`WithBody` / `ReturnBody` context conflicts** — expressions end these rules; LALR(1) state merging conflates "still inside an expression" with the post-expression FOLLOW set of the enclosing rule. On tokens `"."` (property access) and `"WHERE"`, the LALR parser cannot decide whether to extend the expression or reduce the enclosing rule. `#[precedence]` only resolves conflicts *within* the `Expr` rule; it cannot reach cross-rule conflicts.

2. **List comprehension `[Ident IN...]`** — LALR(1) merges the `Ident` variable-reference state with the start of a comprehension. On lookahead `IN` it cannot decide which to use.

### Attempt 2: LR(1) with layered nonterminals

Removing `#[LALR]` and rewriting `Expr` as a hierarchy of named nonterminals (one per precedence level) is the classical LR(1) approach — used by SQL, the openCypher ANTLR grammar, C, and Java parsers. After full implementation of the openCypher operator and predicate set, **50 structural shift/reduce conflicts remained**. All 50 are architectural:

- LALRPOP's state-merging is more aggressive than canonical LR(1): it merges states with identical cores across unrelated grammar contexts, contaminating FOLLOW sets globally.
- Optional suffixes on `ReturnBody` and `WithBody` (`WHERE`, `ORDER BY`, `SKIP`, `LIMIT`) create conflicts because the same tokens appear in expression positions inside those rules.
- The problem is not resolvable by grammar refactoring, disambiguation rules, or any mechanism within LALRPOP's model.

---

## Decision

`gf-cypher` uses a **hand-written recursive-descent clause parser** with a **Pratt expression parser** subroutine.

This is the architecture used by Neo4j's production Cypher parser, PostgreSQL, SQLite, GQL, and virtually every other production SQL/query-language parser. It is conflict-free by construction: there is no parser table, no state merging, and no FOLLOW set contamination.

### Architecture

```
TokenStream (Vec<(usize, Tok, usize)> + cursor)
      ↓
parse_query()              ← dispatch on peek()
  parse_match_clause()     ← recursive descent, one function per clause
  parse_return_clause()
  parse_with_clause()
  ...
      ↓ (expressions anywhere)
parse_expr(ts, min_bp)     ← Pratt parser, binding-power table
```

### TokenStream

The `Lexer` iterator is consumed upfront into a `Vec`. The parser then walks a clean `Vec<(usize, Tok, usize)>` with one token of lookahead via `peek()`. Lexing failures surface immediately at `TokenStream::new()`, before any parsing begins.

### Pratt binding powers

| Operators | Left BP | Right BP |
|---|---|---|
| OR | 10 | 11 |
| XOR | 20 | 21 |
| AND | 30 | 31 |
| NOT (prefix) | — | 35 |
| `=` `<>` `<` `<=` `>` `>=` `=~` `IS NULL` `IS NOT NULL` `IN` `NOT IN` `STARTS WITH` `ENDS WITH` `CONTAINS` | 40 | 40 |
| `+` `-` | 50 | 51 |
| `*` `/` `%` | 60 | 61 |
| `^` (right-assoc) | 70 | 70 |
| `.` `[` (postfix) | 80 | 81 |

Compound multi-word tokens (`NOT IN`, `IS NULL`, `IS NOT NULL`, `STARTS WITH`, `ENDS WITH`) are emitted by the hand-written lexer as single `Tok` variants, so the Pratt parser treats them as single-token operators.

### Clause dispatch

One token of lookahead is sufficient to dispatch every Cypher clause:

| Peek token | Clause |
|---|---|
| `MATCH` | `parse_match_clause` |
| `OPTIONAL` | `parse_optional_match_clause` |
| `RETURN` | `parse_return_clause` |
| `WITH` | `parse_with_clause` |
| `CREATE` | `parse_create_clause` |
| `MERGE` | `parse_merge_clause` |
| `SET` | `parse_set_clause` |
| `REMOVE` | `parse_remove_clause` |
| `DELETE` / `DETACH` | `parse_delete_clause` |
| `UNWIND` | `parse_unwind_clause` |
| `CALL` | `parse_call_clause` |
| `UNION` | `parse_union_marker` |

Optional suffixes (`WHERE`, `ORDER BY`, `SKIP`, `LIMIT`) are handled by explicit `if ts.at(Tok::Where)` peek checks — no ambiguity, no conflicts.

---

## Rationale

**Conflict-free by construction** — recursive descent has no parser table and no state merging. Every dispatch point is an explicit `match ts.peek()`. There is no mechanism by which FOLLOW set contamination can occur.

**One token of lookahead is sufficient** — verified by inspecting every clause boundary and every optional suffix in the full openCypher grammar. The only case requiring two tokens is `OPTIONAL MATCH` (peek 0 = `OPTIONAL`, peek 1 = `MATCH`), handled with `peek_n(1)`.

**Pratt is the natural fit for expression precedence** — binding powers encode precedence and associativity directly; right-associativity is `right_bp = left_bp` (no increment); prefix operators have no left BP; postfix operators have no right BP. Adding a new operator is one entry in the binding-power match.

**The lexer is unchanged** — compound tokens (`Tok::NotIn`, `Tok::IsNull`, etc.) and the full `Tok` enum were already correct for LALRPOP and remain correct for the Pratt parser.

**Aligned with production practice** — Neo4j's Cypher parser, PostgreSQL, SQLite, GQL, and the openCypher reference implementation all use recursive descent + Pratt (or equivalent). This is the proven architecture for query-language parsers.

---

## Consequences

### Positive

- Zero shift/reduce conflicts — by construction, not by workaround
- No build-time grammar generation step (`build.rs` deleted, `grammar.lalrpop` deleted)
- No `lalrpop` or `lalrpop-util` dependency
- Error messages are produced at the exact point of failure with precise spans
- Adding a new clause is one new function; adding a new operator is one new binding-power entry

### Negative / Trade-offs

- More Rust code than a grammar file (~600 lines vs. ~300 lines of LALRPOP grammar)
- Grammar is no longer declarative — correctness depends on the implementation matching the openCypher BNF

### Implementation shape

- `lib.rs` — delegates directly to `parser::parse(input)`
- `parser/mod.rs` — `TokenStream` scaffold
- `parser/expr.rs` — Pratt expression parser
- `parser/patterns.rs` — node/rel/path patterns
- `parser/clauses.rs` — all 13 clause types
- No `grammar.lalrpop` / `lalrpop` build step — the grammar lives in Rust functions

---

## Alternatives Considered

| Alternative | Rejected because |
|---|---|
| LALR(1) + `#[precedence]` | 5 unresolvable cross-rule conflicts in LALR state-merging model |
| LR(1) + layered nonterminals | 50 structural conflicts remain; LALRPOP state-merging is more aggressive than canonical LR(1) |
| PEG parser (pest) | No external lexer support; cannot reuse the hand-written `Tok` lexer |
| Parser combinators (nom) | Not a natural fit for a large precedence grammar |
| ANTLR4 | Java toolchain; no native Rust target |

---

## References

- [ADR 0001: Rust Core](0001-rust-core.md) — Rust-core architecture that hosts `gf-cypher`
- [AST & Planning](../book/architecture/ast-and-planning.md) — parser pipeline and file layout
- openCypher grammar specification — ISO WG3 BNF / ANTLR grammar
