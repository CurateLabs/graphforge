//! GraphForge Cypher parser — recursive descent + Pratt expression parser.
//!
//! Public surface: [`parse`], [`explain`], [`explain_stage`], [`lexer`], [`parser`].
#![forbid(unsafe_code)]
#![allow(missing_docs)]
#![allow(clippy::all)]
#![allow(clippy::pedantic)]

pub mod lexer;
pub mod parser;

use std::sync::{Arc, Mutex};

pub use graphforge_ast::{AstQuery, ParseError, ParseErrorKind, Token};
pub use graphforge_core::{ExplainStage, GfError, Span};
pub use graphforge_ir::{Binder, OntologyMode, RuntimeCatalog};

/// Parse a Cypher query string into an [`AstQuery`].
///
/// # Errors
/// Returns [`ParseError`] on any lexer or syntax error.
pub fn parse(input: &str) -> Result<AstQuery, ParseError> {
    parser::parse(input)
}

/// Return a human-readable compiler pipeline explanation covering AST and GraphIR stages.
///
/// The output contains two sections separated by a blank line:
/// - `AST\n---\n<JSON>` — pretty-printed parse tree
/// - `GraphIR\n-------\n<JSON>` — serialised [`graphforge_ir::GraphPlan`] produced by the
///   binder running in [`OntologyMode::Exploratory`] (no ontology required)
///
/// # Errors
/// Returns [`ParseError`] if `cypher` is syntactically invalid.
pub fn explain(cypher: &str) -> Result<String, ParseError> {
    let ast = parse(cypher)?;
    let ast_json =
        serde_json::to_string_pretty(&ast).unwrap_or_else(|e| format!("{{\"error\": \"{e}\"}}"));

    let ir_json = bind_and_serialise(&ast);

    Ok(format!(
        "AST\n---\n{ast_json}\n\nGraphIR\n-------\n{ir_json}"
    ))
}

/// Return the compiler pipeline output for a single named stage.
///
/// # Stages
///
/// - [`ExplainStage::Ast`] — pretty-printed JSON of the parsed [`AstQuery`]
/// - [`ExplainStage::GraphIr`] — serialised [`graphforge_ir::GraphPlan`] (binder runs in
///   [`OntologyMode::Exploratory`]; all unknown labels/types are auto-interned)
/// - [`ExplainStage::BoundAst`] — deferred; returns [`GfError::NotImplemented`]
/// - [`ExplainStage::LogicalPlan`] / [`ExplainStage::PhysicalPlan`] — not yet implemented
///
/// # Errors
/// Returns [`GfError::Parse`] if `cypher` is syntactically invalid, or
/// [`GfError::NotImplemented`] for stages that are not yet wired up.
pub fn explain_stage(cypher: &str, stage: ExplainStage) -> Result<String, GfError> {
    let ast = parse(cypher).map_err(|e| GfError::Parse {
        msg: e.to_string(),
        span: e.span,
    })?;

    match stage {
        ExplainStage::Ast => {
            serde_json::to_string_pretty(&ast).map_err(|e| GfError::Plan(e.to_string()))
        }

        ExplainStage::GraphIr => Ok(bind_and_serialise(&ast)),

        ExplainStage::BoundAst => Err(GfError::NotImplemented(
            "ExplainStage::BoundAst is not yet implemented",
        )),

        ExplainStage::LogicalPlan => {
            let plan = bind_exploratory(&ast)?;
            graphforge_rel::explain_logical(&plan)
        }

        ExplainStage::PhysicalPlan => Err(GfError::NotImplemented(
            "ExplainStage::PhysicalPlan is not yet implemented",
        )),
    }
}

/// Bind `ast` in [`OntologyMode::Exploratory`] and return the [`GraphPlan`].
///
/// Exploratory mode requires no formal ontology: unknown labels and relation
/// types are auto-interned by a fresh [`RuntimeCatalog`].  Bind errors (e.g.
/// undeclared variables) are collapsed into a single [`GfError::Plan`].
///
/// # Errors
/// Returns [`GfError::Plan`] if the binder rejects the query.
fn bind_exploratory(ast: &AstQuery) -> Result<graphforge_ir::GraphPlan, GfError> {
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let binder = Binder::new(None, catalog, OntologyMode::Exploratory);
    binder.bind(ast).map_err(|errors| {
        let msgs: Vec<&str> = errors.iter().map(|e| e.message.as_str()).collect();
        GfError::Plan(format!("bind errors: {}", msgs.join("; ")))
    })
}

/// Run the binder in exploratory mode and serialise the resulting [`GraphPlan`] to JSON.
///
/// Errors from the binder (e.g. undeclared variables) are surfaced as a JSON
/// object with a `"bind_errors"` key rather than causing a panic.
fn bind_and_serialise(ast: &AstQuery) -> String {
    let catalog = Arc::new(Mutex::new(RuntimeCatalog::new()));
    let binder = Binder::new(None, catalog, OntologyMode::Exploratory);
    match binder.bind(ast) {
        Ok(plan) => serde_json::to_string_pretty(&plan)
            .unwrap_or_else(|e| format!("{{\"error\": \"{e}\"}}")),
        Err(errors) => {
            let msgs: Vec<&str> = errors.iter().map(|e| e.message.as_str()).collect();
            serde_json::json!({ "bind_errors": msgs }).to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------------------
    // explain() — existing tests
    // ---------------------------------------------------------------------------

    #[test]
    fn explain_ast_returns_nonempty_string() {
        let result = explain("MATCH (n:Person) RETURN n.name AS name");
        assert!(result.is_ok());
        assert!(!result.unwrap().is_empty());
    }

    #[test]
    fn explain_output_starts_with_ast_header() {
        let s = explain("RETURN 1").unwrap();
        assert!(s.starts_with("AST"), "output was:\n{s}");
    }

    #[test]
    fn explain_output_is_valid_json_after_header() {
        let s = explain("MATCH (n) RETURN n").unwrap();
        let json_part = s
            .trim_start_matches("AST\n---\n")
            .split("\n\n")
            .next()
            .unwrap_or("");
        let parsed: serde_json::Result<serde_json::Value> = serde_json::from_str(json_part);
        assert!(parsed.is_ok(), "JSON parse failed: {:?}", parsed.err());
    }

    #[test]
    fn explain_json_contains_clauses_key() {
        let s = explain("MATCH (n) RETURN n").unwrap();
        assert!(s.contains("clauses"), "output was:\n{s}");
    }

    #[test]
    fn explain_parse_error_propagates() {
        let result = explain("NOT VALID @@@@");
        assert!(result.is_err());
    }

    #[test]
    fn explain_write_query_does_not_execute() {
        let result = explain("CREATE (:Ghost {name: 'nobody'})");
        assert!(result.is_ok());
    }

    // ---------------------------------------------------------------------------
    // explain() — GraphIR section now populated
    // ---------------------------------------------------------------------------

    #[test]
    fn explain_graphir_section_present() {
        let s = explain("MATCH (n:Person) RETURN n.name").unwrap();
        assert!(s.contains("GraphIR"), "expected GraphIR section:\n{s}");
        assert!(
            !s.contains("not yet implemented"),
            "GraphIR section still a stub:\n{s}"
        );
    }

    #[test]
    fn explain_graphir_section_is_valid_json() {
        let s = explain("MATCH (n:Person) RETURN n.name").unwrap();
        let ir_part = s.split("GraphIR\n-------\n").nth(1).unwrap_or("");
        let parsed: serde_json::Result<serde_json::Value> = serde_json::from_str(ir_part.trim());
        assert!(
            parsed.is_ok(),
            "GraphIR JSON parse failed: {:?}",
            parsed.err()
        );
    }

    // ---------------------------------------------------------------------------
    // explain_stage() — new function
    // ---------------------------------------------------------------------------

    #[test]
    fn explain_stage_ast_returns_json() {
        let result = explain_stage("MATCH (n:Person) RETURN n.name", ExplainStage::Ast);
        assert!(result.is_ok());
        let json: serde_json::Result<serde_json::Value> = serde_json::from_str(&result.unwrap());
        assert!(json.is_ok());
    }

    #[test]
    fn explain_stage_graph_ir_contains_node_scan_and_project() {
        let result =
            explain_stage("MATCH (n:Person) RETURN n.name", ExplainStage::GraphIr).unwrap();
        assert!(
            result.contains("NodeScan"),
            "expected NodeScan in GraphIR:\n{result}"
        );
        assert!(
            result.contains("Project"),
            "expected Project in GraphIR:\n{result}"
        );
    }

    #[test]
    fn explain_stage_graph_ir_filter_present_when_where_clause() {
        let result = explain_stage(
            "MATCH (n:Person) WHERE n.age > 30 RETURN n.name",
            ExplainStage::GraphIr,
        )
        .unwrap();
        assert!(result.contains("Filter"), "expected Filter op:\n{result}");
    }

    #[test]
    fn explain_stage_bound_ast_returns_not_implemented() {
        let result = explain_stage("RETURN 1", ExplainStage::BoundAst);
        assert!(matches!(result, Err(GfError::NotImplemented(_))));
    }

    #[test]
    fn explain_stage_logical_plan_contains_table_scan() {
        // Property/variable projection cannot lower in M12 (property tables are
        // a later milestone), so project a literal — the node scan still
        // produces a TableScan, which is what the explain stage exposes.
        let result = explain_stage(
            "MATCH (n:Person) RETURN 1 AS one",
            ExplainStage::LogicalPlan,
        )
        .unwrap();
        assert!(
            result.contains("TableScan"),
            "expected TableScan in logical plan:\n{result}"
        );
    }

    #[test]
    fn explain_stage_logical_plan_bind_error_surfaces_as_plan_error() {
        // `RETURN n` references an undeclared variable → binder rejects it.
        let result = explain_stage("RETURN n", ExplainStage::LogicalPlan);
        assert!(matches!(result, Err(GfError::Plan(_))), "got {result:?}");
    }

    #[test]
    fn explain_stage_physical_plan_returns_not_implemented() {
        let result = explain_stage("RETURN 1", ExplainStage::PhysicalPlan);
        assert!(matches!(result, Err(GfError::NotImplemented(_))));
    }

    #[test]
    fn explain_stage_parse_error_surfaces_as_gf_error() {
        let result = explain_stage("NOT VALID @@@@", ExplainStage::GraphIr);
        assert!(matches!(result, Err(GfError::Parse { .. })));
    }
}
