//! Explicit immutable owner histories remain inspectable after scoped suppression.
use super::{ResearchClaimHistoryKind as Kind, ResearchClaimHistoryRequest, authority, ledger};
use crate::{
    ExecutionResult, GfError, GraphForge,
    knowledge::{assertion_result, knowledge_error, ledger as k},
};
use arrow::array::{Array, BooleanArray, FixedSizeBinaryArray};
impl GraphForge {
    /// Inspect native classifications, relations, suppression, status, reasoning or evidence.
    pub fn research_claim_history(
        &self,
        request: &ResearchClaimHistoryRequest,
    ) -> Result<ExecutionResult, GfError> {
        self.graph_visibility.health.check()?;
        authority::require_owner(self)?;
        let generation = self.generation_for_read()?;
        authority::resolve(&generation, &request.context, None)?;
        authority::with_context(self, &generation, &request.context, |_, g| {
            crate::branches::domain_bounds::preflight(g)?;
            let batch = match request.family {
                Kind::Classification => ledger::read_claims(g)?.claim_batch(),
                Kind::Relations => ledger::read_claims(g)?.relation_batch(),
                Kind::Suppressions => ledger::read_suppressions(g)?.batch(),
                Kind::Status => k::read_status_ledger(g)?.batch(),
                Kind::Reasoning => k::read_reasoning_ledger(g)?.batch(),
                Kind::Evidence => k::read_evidence_ledger(g)?.batch(),
            }
            .map_err(knowledge_error)?;
            let Some(id) = request.assertion_uuid else {
                return Ok(assertion_result(batch));
            };
            let names: &[&str] = if request.family == Kind::Relations {
                &["source_assertion_uuid", "target_assertion_uuid"]
            } else {
                &["assertion_uuid"]
            };
            let mut keep = vec![false; batch.num_rows()];
            for name in names {
                let ids = batch
                    .column_by_name(name)
                    .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>())
                    .ok_or_else(|| {
                        GfError::Validation("invalid assertion history schema".into())
                    })?;
                for (row, keep) in keep.iter_mut().enumerate() {
                    *keep |= !ids.is_null(row) && ids.value(row) == id.as_bytes();
                }
            }
            Ok(assertion_result(
                arrow::compute::filter_record_batch(&batch, &BooleanArray::from(keep))
                    .map_err(|e| GfError::Execution(e.to_string()))?,
            ))
        })
    }
}
