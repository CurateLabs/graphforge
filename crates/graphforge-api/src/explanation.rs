//! Side-effect-free compiler inspection through the engine facade.

use std::sync::{Arc, Mutex};

use graphforge_ir::{Binder, GraphOp, GraphPlan, OntologyMode};
use graphforge_storage::GraphCatalog;

use crate::{ExplainStage, GfError, GraphForge};

impl GraphForge {
    /// Explain every compiler stage: `AST`, `GraphIR`, `LogicalPlan`, and `PhysicalPlan`.
    ///
    /// Binding uses this instance's ontology, mode, and procedures with a private
    /// runtime-catalog snapshot. Explanation never executes the query or publishes
    /// the labels, relations, or properties that its binding would intern.
    ///
    /// # Errors
    /// Returns the same structured parse, binder, and lowering diagnostics as the
    /// corresponding facade stages, or a storage/execution error while planning.
    pub fn explain(&self, cypher: &str) -> Result<String, GfError> {
        self.explain_selected(cypher, None)
    }

    /// Explain one compiler stage using this instance's binding context.
    ///
    /// AST inspection only parses. Later stages share the same private catalog
    /// snapshot and binder conversion as [`Self::explain`]. A physical explanation
    /// renders the existing execution plan without executing it.
    ///
    /// # Errors
    /// Returns a structured stage error, or [`GfError::NotImplemented`] for
    /// [`ExplainStage::BoundAst`], since the binder produces GraphIR directly.
    pub fn explain_stage(&self, cypher: &str, stage: ExplainStage) -> Result<String, GfError> {
        self.explain_selected(cypher, Some(stage))
    }

    fn explain_selected(
        &self,
        cypher: &str,
        stage: Option<ExplainStage>,
    ) -> Result<String, GfError> {
        let ast = graphforge_cypher::parse(cypher).map_err(GfError::from_parse_display)?;
        if stage == Some(ExplainStage::BoundAst) {
            return Err(GfError::NotImplemented(
                "ExplainStage::BoundAst is not yet implemented",
            ));
        }
        let ast_json =
            serde_json::to_string_pretty(&ast).map_err(|error| GfError::Plan(error.to_string()))?;
        if stage == Some(ExplainStage::Ast) {
            return Ok(ast_json);
        }

        // Pin the composition projection and all generation-coupled planning
        // authorities together. Projection does not publish its candidate.
        let _read_visibility = self.graph_visibility.read()?;
        let composition = self
            .default_composition_snapshot()
            .map(|context| self.bind_generation_storage(&context))
            .transpose()?;
        let execution_mode = composition
            .as_ref()
            .map_or(self.ontology_mode, |(context, _, _)| {
                Self::composition_execution_mode(context)
            });

        // Binding interns only into this private snapshot, which also backs
        // property-name resolution during logical and physical planning.
        let snapshot = Arc::new(Mutex::new(
            self.runtime_catalog
                .lock()
                .expect("runtime catalog poisoned")
                .clone(),
        ));
        let mut binder = Binder::new(
            self.ontology.clone(),
            Arc::clone(&snapshot),
            self.ontology_mode,
        )
        .with_procedures(self.procedure_snapshot());
        if let Some((context, _, _)) = &composition {
            binder = binder.with_composition(Arc::clone(context));
        }
        let plan = binder
            .bind(&ast)
            .map_err(|errors| GfError::from_bind_errors(&errors))?;
        let graph_ir = serde_json::to_string_pretty(&plan)
            .map_err(|error| GfError::Plan(error.to_string()))?;
        if stage == Some(ExplainStage::GraphIr) {
            return Ok(graph_ir);
        }

        if composition
            .as_ref()
            .is_some_and(|(_, _, moves)| !moves.is_empty())
        {
            return Err(GfError::Validation(
                "GF_SEMANTIC_LEGACY_MIGRATION_REQUIRED: run a publishing write to migrate the unambiguous legacy generation".into(),
            ));
        }
        let catalog = self.open_query_catalog(
            &snapshot,
            composition.as_ref().map(|(_, candidate, _)| candidate),
        )?;
        if stage == Some(ExplainStage::LogicalPlan) {
            return self.explain_logical_stage(&plan, &catalog, execution_mode);
        }
        // Preserve the all-stage presentation: some operators only lower in
        // the directory-backed physical stage, so that section remains useful
        // when the directory-less logical renderer cannot represent them.
        let logical = if stage.is_none() {
            self.explain_logical_stage(&plan, &catalog, execution_mode)
                .unwrap_or_else(|error| format!("(logical plan unavailable: {error})"))
        } else {
            String::new()
        };
        let adjacency_provider = if execution_mode == self.ontology_mode {
            self.adjacency_provider_for_session()
        } else {
            Arc::new(crate::adjacency_provider_for_graph(
                &self.dir,
                execution_mode,
                self.property_inventory_for_session(),
            )?)
        };
        let session =
            graphforge_exec::ExecutionSession::new_with_target_provider_resources_and_identity(
                catalog,
                self.ontology.clone(),
                self.dir.clone(),
                execution_mode,
                adjacency_provider,
                Some(Arc::clone(&self.ordinal_identities)),
                &self.session_resource_config(),
            )?;
        // No executable plan or write capability escapes this rendering boundary.
        let physical = self.block_on(async move { session.explain_physical(&plan).await })?;
        if stage == Some(ExplainStage::PhysicalPlan) {
            return Ok(physical);
        }
        Ok(format!(
            "AST\n---\n{ast_json}\n\n\
             GraphIR\n-------\n{graph_ir}\n\n\
             LogicalPlan\n-----------\n{logical}\n\n\
             PhysicalPlan\n------------\n{physical}"
        ))
    }

    fn explain_logical_stage(
        &self,
        plan: &GraphPlan,
        catalog: &GraphCatalog,
        execution_mode: OntologyMode,
    ) -> Result<String, GfError> {
        let needs_writes = plan.ops.iter().any(|op| {
            matches!(
                op,
                GraphOp::Create { .. }
                    | GraphOp::Merge { .. }
                    | GraphOp::Delete { .. }
                    | GraphOp::Set { .. }
                    | GraphOp::Remove { .. }
            )
        });
        if needs_writes {
            graphforge_rel::explain_logical_for_writes(
                plan,
                &catalog.lowering_snapshot(Some(&self.dir))?,
                self.ontology.as_ref(),
                execution_mode,
            )
        } else {
            graphforge_rel::explain_logical_with_catalog(
                plan,
                Some(&catalog.lowering_snapshot(None)?),
                self.ontology.as_ref(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_stages_match_full_explanation_and_real_query_results() {
        let graph = GraphForge::new(None).unwrap();
        graph
            .execute("CREATE (:Person {name:'Ada', age:40}), (:Person {name:'Bob', age:20})")
            .unwrap();
        let query = "MATCH (n:Person) WHERE n.age > 30 RETURN n.name AS name";
        let full = graph.explain(query).unwrap();
        let (ast, rest) = full
            .strip_prefix("AST\n---\n")
            .unwrap()
            .split_once("\n\nGraphIR\n-------\n")
            .unwrap();
        let (ir, rest) = rest.split_once("\n\nLogicalPlan\n-----------\n").unwrap();
        let (logical, physical) = rest.split_once("\n\nPhysicalPlan\n------------\n").unwrap();
        for (stage, expected) in [
            (ExplainStage::Ast, ast),
            (ExplainStage::GraphIr, ir),
            (ExplainStage::LogicalPlan, logical),
            (ExplainStage::PhysicalPlan, physical),
        ] {
            assert_eq!(graph.explain_stage(query, stage).unwrap(), expected);
        }
        let ast: serde_json::Value = serde_json::from_str(ast).unwrap();
        assert!(ast.get("clauses").is_some());
        let ir: serde_json::Value = serde_json::from_str(ir).unwrap();
        assert!(ir.to_string().contains("NodeScan"));
        assert!(ir.to_string().contains("Filter"));
        assert!(ir.to_string().contains("Project"));
        let result = graph.execute(query).unwrap();
        assert_eq!(result.stats.rows_produced, 1);
        let names = result.batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Ada");
    }

    #[test]
    fn every_explanation_stage_preserves_the_parser_diagnostic() {
        let graph = GraphForge::new(None).unwrap();
        let query = "RETURN (";
        let expected = graphforge_cypher::parse(query).unwrap_err();
        for stage in [
            ExplainStage::Ast,
            ExplainStage::BoundAst,
            ExplainStage::GraphIr,
            ExplainStage::LogicalPlan,
            ExplainStage::PhysicalPlan,
        ] {
            let error = graph.explain_stage(query, stage).unwrap_err();
            assert_eq!(error.code(), "GF_PARSE");
            let GfError::Parse {
                diagnostic,
                msg,
                span,
            } = error
            else {
                panic!("parser diagnostic lost")
            };
            assert_eq!(diagnostic.as_deref(), Some(&expected));
            assert_eq!(span, expected.span);
            assert_eq!(msg, expected.to_string());
        }
    }
}
