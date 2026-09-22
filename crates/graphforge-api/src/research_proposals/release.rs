//! Explicit obsolete-payload lifecycle; receipts and deduplication never expire here.
use super::ReleaseResearchProposalRequest;
use crate::{CancellationToken, GfError, GraphForge, branches::publication};
use graphforge_storage::research_versions::{ResearchMutation, ResearchOperationReceipt};
impl GraphForge {
    /// Withdraw the frozen root; preserve all accepted proof and permanent history.
    /// Unreferenced immutable payloads can then be deleted through Version retention.
    pub fn release_research_proposal(
        &mut self,
        request: &ReleaseResearchProposalRequest,
        cancellation: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        let command = publication::begin(
            self,
            request.operation_uuid,
            request.expected_generation_uuid,
            request,
            cancellation,
        )?;
        if let Some(receipt) = command.replay(self)? {
            return Ok(receipt);
        }
        let intent_sha256 = command.intent;
        command.publish(
            self,
            ResearchMutation::ReleaseProposal {
                intent_sha256,
                proposal_uuid: request.proposal_uuid,
            },
            cancellation,
        )
    }
}
