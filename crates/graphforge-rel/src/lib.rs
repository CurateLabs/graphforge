//! GraphForge Graph IR → DataFusion relational lowering.
//!
//! # Milestone status
//!
//! - logical-plan lowering #574 — IR expression lowering to DataFusion `Expr`
//! - logical-plan lowering #573 — Wire SessionContext
//! - logical-plan lowering #575 — Lower simple GraphOps (Filter, Project, …)
//! - logical-plan lowering #576 — Lower NodeScan and Expand
//! - logical-plan lowering #578 — Graph-native Extension stubs + `explain_logical` ← **this issue**
#![forbid(unsafe_code)]

pub mod expr;
pub use expr::{ExprLowerer, LoweringError, VarMap, ir_literal_to_scalar, scalar_to_ir_literal};

pub mod lowerer;
pub use lowerer::GraphPlanLowerer;

pub mod calendar;

pub mod temporal;

pub use graphforge_core::GfError;
pub use graphforge_ir::GraphPlan;
use graphforge_ontology::OntologyHandle;

/// A DataFusion [`datafusion::logical_expr::LogicalPlan`] produced by the
/// relational lowering pass.
///
/// Using a type alias means all downstream code (including `graphforge-exec`) works
/// directly with the native DataFusion type without any wrapping overhead.
pub type LogicalPlan = datafusion::logical_expr::LogicalPlan;

/// Lower a [`GraphPlan`] to a DataFusion [`LogicalPlan`].
///
/// Delegates to [`GraphPlanLowerer`] with `catalog = None` and
/// `ontology = None` (exploratory mode).  Scan operators (`NodeScan`,
/// `Expand`) return [`GfError::Plan`] until #576 is complete.
///
/// # Errors
///
/// Returns [`GfError::Plan`] if any operator in the pipeline cannot be
/// lowered (e.g. graph-native scan operators not yet implemented in #576).
pub fn lower(plan: &GraphPlan) -> Result<LogicalPlan, GfError> {
    GraphPlanLowerer::new(None, None).lower_plan(plan)
}

/// Lower a [`GraphPlan`] and run DataFusion's analyzer + optimizer over it.
///
/// Uses a transient [`SessionContext`](datafusion::prelude::SessionContext)
/// with no registered catalog — the lowered plan is self-contained (scans use
/// [`LogicalTableSource`](datafusion::logical_expr::logical_plan::LogicalTableSource)
/// with static schemas), so optimisation needs no external state.  Lowering
/// runs in exploratory mode (`catalog = None`, `ontology = None`).
///
/// # Errors
///
/// Returns [`GfError::Plan`] if lowering fails or the analyzer/optimizer
/// rejects the plan (e.g. unresolved `$param` placeholders).
pub fn lower_and_optimize(plan: &GraphPlan) -> Result<LogicalPlan, GfError> {
    let lowered = lower(plan)?;
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.state()
        .optimize(&lowered)
        .map_err(|e| GfError::Plan(e.to_string()))
}

/// Render a [`GraphPlan`]'s optimised DataFusion [`LogicalPlan`] as indented
/// text (with per-node schemas) for the `explain` LogicalPlan stage.
///
/// Lowers in exploratory mode (no ontology).  Falls back to the
/// pre-optimisation plan when the analyzer/optimizer rejects it — graph-native
/// [`Extension`](datafusion::logical_expr::Extension) stub nodes carry no
/// optimiser semantics yet, and `$param` placeholders cannot be type-coerced
/// until execution time.  In both cases the un-optimised plan is still valid,
/// inspectable output.
///
/// # Errors
///
/// Returns [`GfError`] if the plan cannot be lowered at all.
pub fn explain_logical(plan: &GraphPlan) -> Result<String, GfError> {
    explain_logical_with(plan, None)
}

/// Like [`explain_logical`] but lowers with an optional [`OntologyHandle`] so
/// that typed edge scans and variable-length expands can resolve relation-type
/// names.  Used by the logical-plan golden suite (which binds against a formal
/// ontology for deterministic type IDs).
///
/// # Errors
///
/// Returns [`GfError`] if the plan cannot be lowered.
pub fn explain_logical_with(
    plan: &GraphPlan,
    ontology: Option<&OntologyHandle>,
) -> Result<String, GfError> {
    explain_logical_with_catalog(plan, None, ontology)
}

/// Like [`explain_logical_with`] but also threads a
/// [`GraphCatalog`](graphforge_storage::GraphCatalog) so property accesses (e.g.
/// `n.name`) resolve to their column names via the catalog's `PropId → name`
/// map instead of falling back to `prop_<id>` placeholders. Used by the engine
/// facade's `explain`, which lowers against the instance's runtime catalog.
///
/// # Errors
///
/// Returns [`GfError`] if the plan cannot be lowered.
pub fn explain_logical_with_catalog(
    plan: &GraphPlan,
    catalog: Option<&graphforge_storage::GraphCatalog>,
    ontology: Option<&OntologyHandle>,
) -> Result<String, GfError> {
    let lowered = GraphPlanLowerer::new(catalog, ontology).lower_plan(plan)?;
    let ctx = datafusion::prelude::SessionContext::new();
    let final_plan = ctx.state().optimize(&lowered).unwrap_or(lowered);
    Ok(final_plan.display_indent_schema().to_string())
}

/// Like [`explain_logical_with_catalog`] but lowers write terminals through
/// [`GraphPlanLowerer::new_for_writes`] so EXPLAIN can render `CREATE` / `MERGE`
/// / `DELETE` / `SET` / `REMOVE` without executing them.
///
/// # Errors
///
/// Returns [`GfError`] if the plan cannot be lowered.
pub fn explain_logical_for_writes(
    plan: &GraphPlan,
    catalog: Option<&graphforge_storage::GraphCatalog>,
    ontology: Option<&OntologyHandle>,
    dir: &std::path::Path,
    mode: graphforge_core::OntologyMode,
) -> Result<String, GfError> {
    let lowered =
        GraphPlanLowerer::new_for_writes(catalog, ontology, dir, mode).lower_plan(plan)?;
    let ctx = datafusion::prelude::SessionContext::new();
    let final_plan = ctx.state().optimize(&lowered).unwrap_or(lowered);
    Ok(final_plan.display_indent_schema().to_string())
}
