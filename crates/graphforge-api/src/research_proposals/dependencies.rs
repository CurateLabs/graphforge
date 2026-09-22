//! Native evidence closure and atomic-record dependencies are explicit review rows.
use super::preview::Row;
use crate::{CancellationToken, GfError, GraphForge, branches::fields};
use graphforge_storage::research_versions::ResearchEvidenceReference;
use graphforge_storage::research_versions::{ResearchProposalItem, ResearchProposalRecord};

pub(super) struct Context<'a> {
    pub proposal: &'a ResearchProposalRecord,
    pub source: &'a GraphForge,
    pub proposed: &'a fields::Fields,
    pub before: &'a fields::Fields,
    pub cancellation: &'a CancellationToken,
    pub evidence: &'a [graphforge_storage::research_versions::ResearchEvidenceReference],
}

impl Context<'_> {
    pub(super) fn resolve(
        &self,
        item: &ResearchProposalItem,
        row: &mut Row,
        remaining: &mut usize,
    ) -> Result<(), GfError> {
        if item.value_sha256.is_none() || row.already_accepted {
            return Ok(());
        }
        let object = (item.unit.object_kind.clone(), item.unit.object_uuid);
        let closure = crate::slices::branch::dependency_objects(
            self.source,
            &[object.clone()].into(),
            self.cancellation,
        )?;
        for key in self.proposed.keys() {
            self.cancellation.checkpoint()?;
            if !closure.contains(&(key.0.clone(), key.1)) {
                continue;
            }
            let own = (key.0.clone(), key.1) == object;
            if matches!(key.0.as_str(), "node" | "edge") {
                // Existing graph objects do not require unrelated property changes.
                // Newly required identities need only their structural context.
                if self
                    .before
                    .contains_key(&(key.0.clone(), key.1, "$object".into()))
                    || !key.2.starts_with('$')
                {
                    continue;
                }
            } else if own && key.2 == item.unit.field {
                continue;
            }
            self.require(key, item, row, remaining)?;
        }
        if matches!(
            item.unit.object_kind.as_str(),
            "node" | "edge" | "assertion"
        ) {
            for key in self
                .proposed
                .keys()
                .filter(|key| key.0.starts_with("ontology"))
            {
                self.cancellation.checkpoint()?;
                self.require(key, item, row, remaining)?;
            }
        }
        for evidence in self.evidence {
            let (artifact, state) = match evidence {
                ResearchEvidenceReference::Local { .. } => continue,
                ResearchEvidenceReference::ExternalOnly { artifact_uuid, .. } => {
                    (*artifact_uuid, "external_only")
                }
                ResearchEvidenceReference::Unverifiable { artifact_uuid } => {
                    (*artifact_uuid, "unverifiable")
                }
            };
            if closure.contains(&("artifact".into(), artifact)) {
                charge(remaining, 128)?;
                row.evidence_gaps.push((artifact, state.into()));
            }
        }
        row.unavailable.sort();
        row.unavailable.dedup();
        Ok(())
    }

    fn require(
        &self,
        key: &fields::Key,
        item: &ResearchProposalItem,
        row: &mut Row,
        remaining: &mut usize,
    ) -> Result<(), GfError> {
        if self.proposed.get(key) == self.before.get(key) {
            return Ok(());
        }
        if let Some(dependency) = self.proposal.items.iter().find(|candidate| {
            candidate.unit.object_kind == key.0
                && candidate.unit.object_uuid == key.1
                && candidate.unit.field == key.2
        }) {
            if dependency.item_uuid != item.item_uuid {
                charge(remaining, 96)?;
                row.required_items.insert(dependency.item_uuid);
            }
        } else {
            charge(
                remaining,
                key.0.len().saturating_add(key.2.len()).saturating_add(64),
            )?;
            row.unavailable
                .push(format!("{}:{}:{}", key.0, key.1, key.2));
        }
        Ok(())
    }
}

fn charge(remaining: &mut usize, bytes: usize) -> Result<(), GfError> {
    *remaining = remaining
        .checked_sub(bytes)
        .ok_or_else(|| GfError::Project {
            code: graphforge_core::ProjectErrorCode::ResourceLimit,
            message: "Proposal dependency preview exceeds its 8 MiB working limit".into(),
        })?;
    Ok(())
}
