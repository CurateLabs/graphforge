//! Typed errors for panics inside query execution (#1986).
//!
//! A panic in an operator, whether on the polling thread or in a task DataFusion
//! spawned, must reach the caller as `GF_EXECUTION` naming the operators of the
//! running plan, never as a process abort or a bare join error.

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use datafusion::common::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use graphforge_core::GfError;

/// Operator names of `plan` in pre-order, comma separated.
fn operator_names(plan: &Arc<dyn ExecutionPlan>) -> String {
    fn walk(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<String>) {
        out.push(plan.name().to_string());
        for child in plan.children() {
            walk(child, out);
        }
    }
    let mut names = Vec::new();
    walk(plan, &mut names);
    names.join(", ")
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string())
}

fn typed(detail: &str, plan: &Arc<dyn ExecutionPlan>) -> DataFusionError {
    DataFusionError::External(Box::new(GfError::Execution(format!(
        "query execution panicked: {detail} (plan operators: {})",
        operator_names(plan)
    ))))
}

/// Convert a panic caught on the polling thread into a typed execution error.
pub(crate) fn from_unwind(
    payload: &(dyn Any + Send),
    plan: &Arc<dyn ExecutionPlan>,
) -> DataFusionError {
    typed(&panic_message(payload), plan)
}

/// Rewrite an error that carries a panicked DataFusion task into a typed
/// execution error; every other error is returned unchanged.
pub(crate) fn from_task_error(
    error: DataFusionError,
    plan: &Arc<dyn ExecutionPlan>,
) -> DataFusionError {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    while let Some(current) = source {
        if let Some(join) = current.downcast_ref::<tokio::task::JoinError>()
            && join.is_panic()
        {
            return typed(&join.to_string(), plan);
        }
        source = current.source();
    }
    error
}

/// Run `poll` and turn a panic it raises into a typed execution error.
pub(crate) fn guard_poll<T>(
    plan: &Arc<dyn ExecutionPlan>,
    poll: impl FnOnce() -> T,
) -> Result<T, DataFusionError> {
    catch_unwind(AssertUnwindSafe(poll)).map_err(|payload| from_unwind(payload.as_ref(), plan))
}
