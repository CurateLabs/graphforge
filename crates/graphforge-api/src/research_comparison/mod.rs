//! Native semantic comparison over existing research fields and incorporated baselines.
mod accepted;
mod accepted_history;
mod delta;
mod model;
mod output;
pub(crate) mod scope;
mod state;
#[cfg(test)]
mod tests;
use crate::{CancellationToken, ExecutionResult, GfError, GraphForge, branches::fields::Objects};
pub use model::*;
impl GraphForge {
    /// Compare exact research endpoints, per-field incorporated baselines and accepted subsets.
    /// Returns native Arrow changes or semantic indicators; never advances any context.
    pub fn compare_research(
        &self,
        request: &ResearchComparisonRequest,
        cancellation: &CancellationToken,
    ) -> Result<ExecutionResult, GfError> {
        validate(request)?;
        cancellation.checkpoint()?;
        if self.read_only && self.research_materialization.is_some() {
            return Err(invalid(
                "research comparison requires the owning Project facade",
            ));
        }
        let current = self.generation_for_read()?;
        let registry = graphforge_storage::research_versions::read_research_registry(&current)?;
        let mut left = state::load(self, &current, &registry, &request.left, None, cancellation)
            .map_err(|e| retention(e, request))?;
        bounds(request, &left, None)?;
        let right_branch = match request.right {
            ResearchComparisonEndpoint::Branch { branch_uuid } => Some(branch_uuid),
            ResearchComparisonEndpoint::Version { version_uuid } => registry
                .versions
                .get(&version_uuid)
                .and_then(|v| registry.branches.get(&v.context_uuid))
                .map(|b| b.branch_uuid),
            ResearchComparisonEndpoint::Project => None,
        };
        let selected = if left.branch.is_some() && left.parent_branch == right_branch {
            Some(
                left.fields
                    .keys()
                    .chain(left.baseline.keys())
                    .map(|k| (k.0.clone(), k.1))
                    .collect::<Objects>(),
            )
        } else {
            None
        };
        let mut right = state::load(
            self,
            &current,
            &registry,
            &request.right,
            selected.as_ref(),
            cancellation,
        )
        .map_err(|e| retention(e, request))?;
        state::canonical(&mut left, &current, request.left_authority.as_ref(), None)?;
        state::canonical(
            &mut right,
            &current,
            request.right_authority.as_ref(),
            selected.as_ref(),
        )?;
        bounds(request, &left, Some(&right))?;
        let accepted = accepted::verify(
            self,
            &current,
            &registry,
            request,
            &left,
            &right,
            cancellation,
        )
        .map_err(|e| retention(e, request))?;
        let rows = delta::compare(&left, &right, &accepted, cancellation)?;
        cancellation.checkpoint()?;
        output::render(request, &left, &right, current.generation_uuid(), &rows)
    }
}
fn validate(r: &ResearchComparisonRequest) -> Result<(), GfError> {
    if r.max_fields == 0
        || r.max_fields > 40_000
        || r.max_bytes < 1024
        || r.max_bytes > 64 * 1024 * 1024
        || r.page_size == 0
        || r.page_size > 1000
        || r.accepted.len() > 256
        || r.after.as_ref().is_some_and(|s| s.len() > 256)
    {
        return Err(invalid(
            "comparison requires 1..40000 fields, 1024..67108864 bytes, 1..1000 page size and at most 256 accepted units",
        ));
    }
    if r.detail == ResearchComparisonDetail::Summary && r.after.is_some() {
        return Err(invalid("summary comparison does not accept a continuation"));
    }
    let bytes = serde_json::to_vec(r).map_err(|_| invalid("invalid comparison request"))?;
    if bytes.len() > 1024 * 1024 {
        return Err(limit());
    }
    Ok(())
}
fn bounds(
    request: &ResearchComparisonRequest,
    left: &state::State,
    right: Option<&state::State>,
) -> Result<(), GfError> {
    let states = [Some(left), right];
    let mut count = 0usize;
    let mut bytes = 0usize;
    for state in states.into_iter().flatten() {
        count = count
            .saturating_add(state.fields.len())
            .saturating_add(state.baseline.len());
        for key in state.fields.keys().chain(state.baseline.keys()) {
            bytes = bytes
                .saturating_add(1536)
                .saturating_add(key.0.len().saturating_mul(3))
                .saturating_add(key.2.len().saturating_mul(3));
        }
    }
    if count > request.max_fields || bytes > request.max_bytes {
        return Err(limit());
    }
    Ok(())
}
fn retention(error: GfError, request: &ResearchComparisonRequest) -> GfError {
    if request.after.is_some() && error.code() == "GF_RESULT_NOT_RETAINED" {
        output::stale()
    } else {
        error
    }
}
fn invalid(message: &str) -> GfError {
    GfError::Validation(message.into())
}
fn limit() -> GfError {
    GfError::Api{code:graphforge_core::ApiErrorCode::ResourceLimit,message:"semantic comparison exceeds its field or byte bound; narrow the research scope or raise the admitted limit".into()}
}
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut s, b| {
        write!(s, "{b:02x}").expect("string");
        s
    })
}
