//! Accepted ontology revisions use the shared native compiler and publication owner.
use super::ReviewResearchProposalRequest;
use crate::{
    CancellationToken, GfError, GraphForge,
    branches::{field_application::MutationContext, field_ontology},
};
use graphforge_storage::research_versions::ResearchProposalItem;
pub(super) fn apply(
    destination: &mut GraphForge,
    source: &GraphForge,
    request: &ReviewResearchProposalRequest,
    items: &[ResearchProposalItem],
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    field_ontology::apply(
        destination,
        source,
        &MutationContext {
            operation_uuid: request.operation_uuid,
            actor_uuid: request.actor_uuid,
        },
        &super::apply_graph::changes(items),
        cancellation,
    )
}
