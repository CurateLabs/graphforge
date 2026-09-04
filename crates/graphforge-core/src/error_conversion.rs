//! Recover GraphForge's structured errors at foreign error boundaries.

use std::error::Error;

use crate::GfError;

impl GfError {
    /// Preserve a GraphForge source when a planner wraps it in another error.
    ///
    /// Foreign failures without a GraphForge source retain the existing
    /// `GF_PLAN` classification and diagnostic. Source traversal uses Rust's
    /// error identity, never diagnostic text.
    #[must_use]
    pub fn from_plan_error(error: impl Error + 'static) -> Self {
        Self::recover_source(&error).unwrap_or_else(|| Self::Plan(error.to_string()))
    }

    /// Preserve a GraphForge source when execution wraps it in another error.
    ///
    /// This also handles shared foreign errors: cloning the GraphForge value
    /// retains its typed code, variant, message, and source span without needing
    /// exclusive ownership of an upstream error. Foreign failures without a
    /// GraphForge source retain `GF_EXECUTION` and their existing diagnostic.
    #[must_use]
    pub fn from_execution_error(error: impl Error + 'static) -> Self {
        Self::recover_source(&error).unwrap_or_else(|| Self::Execution(error.to_string()))
    }

    fn recover_source(error: &(dyn Error + 'static)) -> Option<Self> {
        let mut source = Some(error);
        while let Some(error) = source {
            if let Some(original) = error.downcast_ref::<Self>() {
                return Some(original.clone());
            }
            source = error.source();
        }
        None
    }
}
