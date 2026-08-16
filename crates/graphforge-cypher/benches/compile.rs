//! Benchmarks for the Cypher front end: lex → parse → bind.
//!
//! Every query that reaches the engine walks this path before any relational
//! lowering happens, so its cost is paid on every statement — including the
//! short ones a client sends in a loop. The queries below cover the shapes the
//! parser sees in practice (read, filter, aggregate, variable-length traversal,
//! write) plus a corpus-wide pass over the frozen openCypher regression corpus.

use std::sync::{Arc, LazyLock, Mutex};

use divan::Bencher;
use graphforge_cypher::lexer::Lexer;
use graphforge_cypher::{AstQuery, Binder, OntologyMode, RuntimeCatalog, parse};

fn main() {
    divan::main();
}

/// Representative query shapes, smallest first.
mod queries {
    /// Single label scan with a projection.
    pub const SIMPLE_MATCH: &str = "MATCH (n:Person) RETURN n.name AS name";

    /// Two-hop pattern with a compound predicate and an ordered, bounded result.
    pub const FILTERED_TRAVERSAL: &str = "\
MATCH (person:Person)-[:WORKS_AT]->(company:Company)<-[:WORKS_AT]-(peer:Person)
WHERE person.name STARTS WITH 'A'
  AND company.founded > 1990
  AND peer.name <> person.name
RETURN person.name AS person, company.name AS company, peer.name AS peer
ORDER BY company, person
SKIP 10
LIMIT 25";

    /// Grouped aggregation with a post-aggregation filter.
    pub const AGGREGATION: &str = "\
MATCH (author:Person)-[:WROTE]->(paper:Paper)-[:CITES]->(cited:Paper)
WITH author, count(DISTINCT cited) AS citations, avg(paper.year) AS mean_year
WHERE citations > 5
RETURN author.name AS author, citations, mean_year
ORDER BY citations DESC
LIMIT 100";

    /// Variable-length expansion with an optional match and a CASE expression.
    pub const VARIABLE_LENGTH: &str = "\
MATCH path = (start:City {name: $origin})-[:ROAD*1..4]->(destination:City)
OPTIONAL MATCH (destination)-[:HAS_AIRPORT]->(airport:Airport)
RETURN destination.name AS city,
       length(path) AS hops,
       CASE WHEN airport IS NULL THEN 'road' ELSE 'multimodal' END AS mode
ORDER BY hops, city";

    /// Write pipeline: UNWIND driven MERGE with both ON clauses, then SET.
    pub const WRITE_PIPELINE: &str = "\
UNWIND $rows AS row
MERGE (person:Person {id: row.id})
  ON CREATE SET person.created_at = row.now, person.name = row.name
  ON MATCH SET person.updated_at = row.now
MERGE (org:Company {id: row.org_id})
CREATE (person)-[link:WORKS_AT {since: row.since}]->(org)
SET link.role = row.role
RETURN count(person) AS written";
}

/// A wide query built from repeated UNION ALL segments — the stress shape for
/// the clause parser and the binder's scope handling.
static WIDE_UNION: LazyLock<String> = LazyLock::new(|| {
    (0..64)
        .map(|index| {
            format!(
                "MATCH (n{index}:Label{index}) WHERE n{index}.value > {index} \
                 RETURN n{index}.value AS value"
            )
        })
        .collect::<Vec<_>>()
        .join("\nUNION ALL\n")
});

/// The frozen parser regression corpus (`tests/corpus/valid.json`, 1.4k queries).
static CORPUS: LazyLock<Vec<String>> = LazyLock::new(|| {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/valid.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("missing parser corpus at {}: {error}", path.display()));
    serde_json::from_str(&raw).expect("parser corpus is a JSON array of query strings")
});

fn bind(ast: &AstQuery) -> usize {
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let binder = Binder::new(None, catalog, OntologyMode::Exploratory);
    binder.bind(ast).map_or(0, |plan| plan.ops.len())
}

/// How many lexer passes run inside one measured sample.
///
/// `SimpleMatch` is a few dozen tokens; a single pass is short enough that
/// CodSpeed CPU-simulation overhead can dominate and produce intermittent
/// sole `lex[simple_match]` false regressions (~−19% to −31%) on PRs that
/// never touch the Cypher front end. Batching raises SNR without changing
/// lexer semantics. Longer shapes already have enough work per sample.
const fn lex_iters(shape: Shape) -> usize {
    match shape {
        Shape::SimpleMatch => 256,
        _ => 1,
    }
}

/// Tokenization only — the first pass over the query text.
#[divan::bench(args = SHAPES)]
fn lex(bencher: Bencher, shape: Shape) {
    let query = shape.query();
    let iters = lex_iters(shape);
    bencher.bench(|| {
        let mut tokens = 0_usize;
        for _ in 0..iters {
            tokens = tokens.wrapping_add(
                Lexer::new(divan::black_box(query))
                    .collect::<Result<Vec<_>, _>>()
                    .expect("benchmark query lexes")
                    .len(),
            );
        }
        divan::black_box(tokens)
    });
}

/// Lexing plus recursive-descent/Pratt parsing into the AST.
#[divan::bench(args = SHAPES)]
fn parse_ast(bencher: Bencher, shape: Shape) {
    let query = shape.query();
    bencher.bench(|| divan::black_box(parse(divan::black_box(query)).expect("query parses")));
}

/// Binding the AST into the Graph IR in exploratory mode (no ontology).
#[divan::bench(args = SHAPES)]
fn bind_ir(bencher: Bencher, shape: Shape) {
    let ast = parse(shape.query()).expect("query parses");
    bencher.bench(|| divan::black_box(bind(divan::black_box(&ast))));
}

/// The whole front end for one query: text in, Graph IR out.
#[divan::bench(args = SHAPES)]
fn parse_and_bind(bencher: Bencher, shape: Shape) {
    let query = shape.query();
    bencher.bench(|| {
        let ast = parse(divan::black_box(query)).expect("query parses");
        divan::black_box(bind(&ast))
    });
}

/// Parse every query in the frozen regression corpus.
#[divan::bench]
fn parse_corpus(bencher: Bencher) {
    let corpus: &[String] = &CORPUS;
    bencher.bench(|| {
        let mut parsed = 0_usize;
        for query in divan::black_box(corpus) {
            if parse(query).is_ok() {
                parsed += 1;
            }
        }
        divan::black_box(parsed)
    });
}

/// The query shapes exercised by every front-end benchmark.
#[derive(Clone, Copy)]
enum Shape {
    SimpleMatch,
    FilteredTraversal,
    Aggregation,
    VariableLength,
    WritePipeline,
    WideUnion,
}

const SHAPES: [Shape; 6] = [
    Shape::SimpleMatch,
    Shape::FilteredTraversal,
    Shape::Aggregation,
    Shape::VariableLength,
    Shape::WritePipeline,
    Shape::WideUnion,
];

impl Shape {
    fn query(self) -> &'static str {
        match self {
            Self::SimpleMatch => queries::SIMPLE_MATCH,
            Self::FilteredTraversal => queries::FILTERED_TRAVERSAL,
            Self::Aggregation => queries::AGGREGATION,
            Self::VariableLength => queries::VARIABLE_LENGTH,
            Self::WritePipeline => queries::WRITE_PIPELINE,
            Self::WideUnion => &WIDE_UNION,
        }
    }
}

impl std::fmt::Display for Shape {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::SimpleMatch => "simple_match",
            Self::FilteredTraversal => "filtered_traversal",
            Self::Aggregation => "aggregation",
            Self::VariableLength => "variable_length",
            Self::WritePipeline => "write_pipeline",
            Self::WideUnion => "wide_union",
        };
        formatter.write_str(name)
    }
}
