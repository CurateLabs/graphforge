//! Private native preparation; one shared publication owns content and history.
use super::{ResearchUpstreamResolution, UpdateResearchBranchRequest, invalid, preview, selection};
use crate::{
    CancellationToken, GfError, GraphForge,
    branches::{baseline, fields, publication},
};
use graphforge_storage::research_versions::{
    RegisterResearchVersion, ResearchMutation, ResearchOperationReceipt, ResearchUpstreamField,
    ResearchUpstreamFieldReview, ResearchUpstreamResolutionRecord, ResearchUpstreamReview,
    prepare_branch_content,
};
use std::collections::BTreeSet;

impl GraphForge {
    /// Publish only explicitly reviewed upstream changes and their incorporated baselines.
    pub fn update_research_branch(
        &mut self,
        request: &UpdateResearchBranchRequest,
        cancel: &CancellationToken,
    ) -> Result<ResearchOperationReceipt, GfError> {
        let command = publication::begin(
            self,
            request.operation_uuid,
            request.expected_generation_uuid,
            request,
            cancel,
        )?;
        if let Some(receipt) = command.replay(self)? {
            return Ok(receipt);
        }
        let preview = preview::load(self, &request.preview, cancel)?;
        let selected = selection::validate(&preview, request)?;
        let source_capture = if preview.branch.parent_branch_uuid.is_none() {
            Some(Box::new(RegisterResearchVersion {
                version_uuid: preview::identity(request.operation_uuid, "upstream_origin"),
                context_uuid: preview.branch.project_uuid,
                source_generation_uuid: preview.current.generation_uuid(),
                source_version: None,
                selection: None,
                required_versions: BTreeSet::new(),
                label: None,
                description: None,
                created_at: request.created_at,
                evidence: crate::research_versions::complete_evidence(&preview.current)?,
            }))
        } else {
            None
        };
        let (mut graph, mut version) = super::adoption::apply(
            self,
            &command,
            &preview,
            request,
            &selected,
            source_capture.as_deref(),
            cancel,
        )?;
        for resolution in selected.values() {
            cancel.checkpoint()?;
            match resolution {
                ResearchUpstreamResolution::KeepLocal => {}
                ResearchUpstreamResolution::Explain { claim } => {
                    let expected_generation_uuid = graph.generation_for_read()?.generation_uuid();
                    crate::research_claims::branch::apply(
                        &mut graph,
                        &crate::ChangeResearchBranchClaimRequest {
                            operation_uuid: preview::identity(
                                request.operation_uuid,
                                &claim.assertion_uuid.to_string(),
                            ),
                            expected_generation_uuid,
                            branch_uuid: request.preview.branch_uuid,
                            version_uuid: request.version_uuid,
                            creator_uuid: request.actor_uuid,
                            created_at: request.created_at,
                            change: crate::ResearchClaimChange::Create {
                                claim: claim.clone(),
                            },
                        },
                        cancel,
                    )?;
                }
                ResearchUpstreamResolution::AdoptUpstream => {}
                ResearchUpstreamResolution::RetainBoth => {
                    return Err(invalid(
                        "retain-both native list application is not yet connected",
                    ));
                }
            }
        }
        version.version_uuid = request.version_uuid;
        version.created_at = request.created_at;
        let generation = graph.generation_for_read()?;
        version.content.evidence = crate::research_versions::complete_evidence(&generation)?;
        let mut prepared =
            prepare_branch_content(&command.root, &generation, version, cancel.flag())?;
        let upstream_version = source_capture
            .as_ref()
            .map(|source| source.version_uuid)
            .or(preview.upstream.version)
            .ok_or_else(|| invalid("exact upstream Version is unavailable"))?;
        // Register any newly created local explanatory fields, then advance only reviewed keys.
        baseline::update(self, &graph, &mut prepared, request.operation_uuid, cancel)?;
        let prepared_graph = crate::branches::private_view::open(self, &prepared)?;
        let current = fields::read(&prepared_graph, cancel)?;
        let mut baselines = baseline::read(&prepared_graph)?;
        super::baselines::seed_incoming(
            &preview.local.baseline,
            &preview.upstream.baseline,
            &selected.keys().cloned().collect(),
            &preview.upstream.fields,
            upstream_version,
            &mut baselines,
        );
        super::baselines::incorporate(
            request.preview.branch_uuid,
            request.operation_uuid,
            upstream_version,
            &selected.keys().cloned().collect(),
            &preview.upstream.fields,
            &current,
            &mut baselines,
        )?;
        baseline::install(&command.root, &mut prepared, &baselines, cancel)?;
        let mut fields = Vec::new();
        for (key, resolution) in &selected {
            let row = preview
                .rows
                .iter()
                .find(|row| &row.key == key)
                .expect("validated selection");
            fields.push(ResearchUpstreamFieldReview {
                unit: ResearchUpstreamField {
                    object_kind: key.0.clone(),
                    object_uuid: key.1,
                    field: key.2.clone(),
                },
                baseline_sha256: row.baseline,
                local_sha256: row.left,
                upstream_sha256: row.right,
                result_sha256: current.get(key).copied(),
                resolution: match resolution {
                    ResearchUpstreamResolution::AdoptUpstream => {
                        ResearchUpstreamResolutionRecord::AdoptUpstream
                    }
                    ResearchUpstreamResolution::KeepLocal => {
                        ResearchUpstreamResolutionRecord::KeepLocal
                    }
                    ResearchUpstreamResolution::RetainBoth => {
                        ResearchUpstreamResolutionRecord::RetainBoth
                    }
                    ResearchUpstreamResolution::Explain { claim } => {
                        ResearchUpstreamResolutionRecord::Explain {
                            assertion_uuid: claim.assertion_uuid,
                        }
                    }
                },
            });
        }
        let review = ResearchUpstreamReview {
            sequence: preview.registry.upstream.reviews.len() as u64 + 1,
            operation_uuid: request.operation_uuid,
            branch_uuid: request.preview.branch_uuid,
            original_base_version_uuid: preview.branch.base_version_uuid,
            prior_version_uuid: preview
                .local
                .version
                .ok_or_else(|| invalid("local Branch Version is unavailable"))?,
            upstream_version_uuid: upstream_version,
            version_uuid: request.version_uuid,
            preview_generation_uuid: preview.current.generation_uuid(),
            preview_sha256: preview.digest,
            fields,
            acknowledged_evidence: request.acknowledge_evidence.clone(),
            actor_uuid: request.actor_uuid,
            created_at: request.created_at,
            explanation: request.explanation.clone(),
        };
        let intent_sha256 = command.intent;
        let outcome = command.publish(
            self,
            ResearchMutation::UpdateBranch {
                intent_sha256,
                source_capture,
                review: Box::new(review),
                version: Box::new(prepared.version.clone()),
            },
            cancel,
        );
        drop(prepared);
        outcome
    }
}
