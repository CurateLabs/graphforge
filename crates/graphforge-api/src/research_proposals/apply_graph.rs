//! Apply accepted proposals through the shared native typed field owner.
use crate::{CancellationToken, GfError, GraphForge, branches::field_application};
use graphforge_storage::research_versions::ResearchProposalItem;
pub(super) fn changes(items: &[ResearchProposalItem]) -> Vec<field_application::FieldChange> {
    items
        .iter()
        .map(|item| field_application::FieldChange {
            unit: crate::ResearchFieldIdentity {
                object_kind: item.unit.object_kind.clone(),
                object_uuid: item.unit.object_uuid,
                field: item.unit.field.clone(),
            },
            value_sha256: item.value_sha256,
        })
        .collect()
}
pub(super) fn apply(
    destination: &GraphForge,
    source: &GraphForge,
    items: &[ResearchProposalItem],
    cancellation: &CancellationToken,
) -> Result<(), GfError> {
    field_application::apply(destination, source, &changes(items), cancellation)
}
