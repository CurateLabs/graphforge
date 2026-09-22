//! Native contextual authority must survive reopen without filtering the raw graph.
use arrow::array::{FixedSizeBinaryArray, Int64Array};
use graphforge_api::*;
use graphforge_knowledge::research::{ResearchDecisionKind, ResearchSubjectKind};
use uuid::Uuid;
fn current(g: &GraphForge) -> Uuid {
    g.research_project_summary()
        .unwrap()
        .identity
        .generation_uuid
}
fn subject(g: &GraphForge) -> Uuid {
    let rows = g
        .execute("MATCH (n:ClaimSubject) RETURN n.node_uuid AS id")
        .unwrap();
    Uuid::from_slice(
        rows.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap()
}
fn count(g: &GraphForge) -> i64 {
    g.execute("MATCH (n) RETURN count(n)").unwrap().batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}
fn request(
    g: &GraphForge,
    context: ResearchContext,
    kind: ResearchDecisionKind,
) -> RecordResearchDecisionsRequest {
    RecordResearchDecisionsRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(g),
        context,
        community_uuid: None,
        creator_uuid: Uuid::now_v7(),
        recorded_at: 10,
        decisions: vec![ResearchDecisionInput {
            decision_uuid: Uuid::now_v7(),
            subject_kind: ResearchSubjectKind::Node,
            subject_uuid: subject(g),
            kind,
            source_version_uuid: None,
        }],
    }
}
#[test]
fn canonical_parent_and_branch_decisions_are_explicit_independent_and_durable() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut g = GraphForge::new(root.to_str()).unwrap();
    g.execute("CREATE (:ClaimSubject {name:'shared evidence'})")
        .unwrap();
    let promote = request(&g, ResearchContext::Project, ResearchDecisionKind::Promote);
    g.record_research_decisions(&promote, &CancellationToken::new())
        .unwrap();
    let published = current(&g);
    assert_eq!(
        g.research_canonical_choices(&ResearchContext::Project, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        g.record_research_decisions(&promote, &CancellationToken::new())
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(current(&g), published);
    assert_eq!(count(&g), 1);
    let branch = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 11,
        label: "alternative interpretation".into(),
    };
    g.create_research_branch(&branch, &CancellationToken::new())
        .unwrap();
    let context = ResearchContext::Branch {
        branch_uuid: branch.branch_uuid,
    };
    let integrate = request(&g, context.clone(), ResearchDecisionKind::Integrate);
    g.record_research_decisions(&integrate, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        g.research_canonical_choices(&context, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        0
    );
    let accepted = request(&g, context.clone(), ResearchDecisionKind::Promote);
    g.record_research_decisions(&accepted, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        g.research_canonical_choices(&context, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        g.research_canonical_choices(&context, Some(Uuid::now_v7()))
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        0
    );
    assert_eq!(
        g.research_canonical_choices(&ResearchContext::Project, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        count(g.open_research_branch(branch.branch_uuid).unwrap().graph()),
        1
    );
    drop(g);
    let mut g = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        g.research_decision_history(&context, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        2
    );
    let revoke = request(&g, context.clone(), ResearchDecisionKind::Revoke);
    g.record_research_decisions(&revoke, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        g.research_canonical_choices(&context, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        0
    );
    assert_eq!(
        g.research_decision_history(&context, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        3
    );
    assert_eq!(
        g.record_research_decisions(&integrate, &CancellationToken::new())
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        g.research_canonical_choices(&ResearchContext::Project, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(count(&g), 1);
}
#[test]
fn cancellation_missing_subject_and_changed_retry_leave_current_unchanged() {
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:ClaimSubject)").unwrap();
    let mut write = request(&g, ResearchContext::Project, ResearchDecisionKind::Promote);
    let before = current(&g);
    let token = CancellationToken::new();
    token.cancel();
    assert!(g.record_research_decisions(&write, &token).is_err());
    assert_eq!(current(&g), before);
    let subject = write.decisions[0].subject_uuid;
    write.decisions[0].subject_uuid = Uuid::now_v7();
    assert!(
        g.record_research_decisions(&write, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(current(&g), before);
    write.decisions[0].subject_uuid = subject;
    g.record_research_decisions(&write, &CancellationToken::new())
        .unwrap();
    let published = current(&g);
    write.decisions[0].kind = ResearchDecisionKind::Revoke;
    assert!(
        g.record_research_decisions(&write, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(current(&g), published);
}

fn enable_knowledge(g: &mut GraphForge) {
    for capability_id in [
        CapabilityId::Provenance,
        CapabilityId::Knowledge,
        CapabilityId::Epistemic,
    ] {
        g.enable_capability(EnableCapabilityRequest {
            context: WriteContext {
                operation_uuid: OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            capability_id,
            capability_version: 1,
        })
        .unwrap();
    }
}
fn claim_request(g: &GraphForge, text: &str) -> CreateResearchClaimRequest {
    CreateResearchClaimRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(g),
        assertion_uuid: Uuid::now_v7(),
        claim: text.into(),
        graph_refs: vec![AssertionGraphRefInput {
            graph_uuid: subject(g),
            graph_kind: GraphObjectKind::Node,
            role: AssertionGraphRole::Subject,
            ordinal: 0,
        }],
        category: graphforge_knowledge::research::ResearchCategory::Interpretation,
        creator_uuid: Uuid::now_v7(),
        run_uuid: None,
        created_at: 10,
    }
}
fn claim_provenance(g: &GraphForge, id: Uuid) -> Uuid {
    let result = g.assertion(id, None).unwrap();
    Uuid::from_slice(
        result.batches[0]
            .column_by_name("provenance_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap()
}
fn create_branch(g: &mut GraphForge) -> Uuid {
    let request = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(g),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 20,
        label: "claims".into(),
    };
    g.create_research_branch(&request, &CancellationToken::new())
        .unwrap();
    request.branch_uuid
}
fn change(
    g: &GraphForge,
    branch_uuid: Uuid,
    action: ResearchClaimChange,
) -> ChangeResearchBranchClaimRequest {
    ChangeResearchBranchClaimRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(g),
        branch_uuid,
        version_uuid: Uuid::now_v7(),
        creator_uuid: Uuid::now_v7(),
        created_at: 30,
        change: action,
    }
}
fn rows(result: ExecutionResult) -> usize {
    result.batches.iter().map(|b| b.num_rows()).sum()
}
#[test]
fn branch_challenge_revision_and_suppression_preserve_parent_and_shared_graph() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("claims");
    let mut g = GraphForge::new(root.to_str()).unwrap();
    g.execute("CREATE (:ClaimSubject)").unwrap();
    enable_knowledge(&mut g);
    let first = claim_request(&g, "Parent interpretation");
    g.create_research_claim(&first, &CancellationToken::new())
        .unwrap();
    let mut accept = request(&g, ResearchContext::Project, ResearchDecisionKind::Promote);
    accept.decisions[0].subject_kind = ResearchSubjectKind::Assertion;
    accept.decisions[0].subject_uuid = first.assertion_uuid;
    g.record_research_decisions(&accept, &CancellationToken::new())
        .unwrap();
    let original = g.assertion(first.assertion_uuid, None).unwrap().batches;
    let raw_rank = g
        .rank(
            "ClaimSubject",
            RankOptions {
                by: graphforge_core::algorithms::RankAlgorithm::Degree,
                via: None,
                directed: true,
                write_property: None,
            },
        )
        .unwrap();
    let provenance = claim_provenance(&g, first.assertion_uuid);
    let branch = create_branch(&mut g);
    let context = ResearchContext::Branch {
        branch_uuid: branch,
    };
    let challenge = change(
        &g,
        branch,
        ResearchClaimChange::Challenge {
            assertion_uuid: first.assertion_uuid,
            status_event_uuid: Uuid::now_v7(),
            reasoning_uuid: Uuid::now_v7(),
            rationale: "An alternative reading remains possible".into(),
            provenance_uuid: provenance,
        },
    );
    g.change_research_branch_claim(&challenge, &CancellationToken::new())
        .unwrap();
    let view = InspectResearchClaimsRequest {
        context: context.clone(),
        community_uuid: None,
        include_suppressed: false,
    };
    let result = g.inspect_research_claims(&view).unwrap();
    let status = result.batches[0]
        .column_by_name("status")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(status.value(0), "disputed");
    let state = result.batches[0]
        .column_by_name("local_state")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(state.value(0), "modified");
    let next = Uuid::now_v7();
    let revision = change(
        &g,
        branch,
        ResearchClaimChange::Revise {
            prior_assertion_uuid: first.assertion_uuid,
            claim: ResearchClaimDraft {
                assertion_uuid: next,
                claim: "Revised local interpretation".into(),
                graph_refs: first.graph_refs.clone(),
                category: graphforge_knowledge::research::ResearchCategory::Hypothesis,
                run_uuid: None,
            },
            supersession_uuid: Uuid::now_v7(),
            status_event_uuid: Uuid::now_v7(),
            reasoning_uuid: Uuid::now_v7(),
            rationale: "Revision retains its conceptual origin".into(),
            provenance_uuid: provenance,
            relation_uuid: Uuid::now_v7(),
        },
    );
    g.change_research_branch_claim(&revision, &CancellationToken::new())
        .unwrap();
    assert_eq!(rows(g.inspect_research_claims(&view).unwrap()), 2);
    // Reclassification creates an immutable successor within this Branch only.
    let assert_classification = |g: &GraphForge| {
        let result = g
            .inspect_research_claims(&InspectResearchClaimsRequest {
                include_suppressed: true,
                ..view.clone()
            })
            .unwrap();
        assert_eq!(
            result
                .batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            2
        );
        let mut seen = std::collections::BTreeSet::new();
        for batch in &result.batches {
            let uuid = |name: &str, row: usize| {
                Uuid::from_slice(
                    batch
                        .column_by_name(name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap()
                        .value(row),
                )
                .unwrap()
            };
            let categories = batch
                .column_by_name("category")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let id = uuid("assertion_uuid", row);
                assert!(seen.insert(id));
                assert_eq!(uuid("conceptual_uuid", row), first.assertion_uuid);
                if id == first.assertion_uuid {
                    assert_eq!(categories.value(row), "interpretation");
                } else {
                    assert_eq!(id, next);
                    assert_eq!(categories.value(row), "hypothesis");
                    assert_eq!(uuid("origin_branch_uuid", row), branch);
                    assert_eq!(uuid("origin_version_uuid", row), revision.version_uuid);
                }
            }
        }
        assert_eq!(
            g.assertion(first.assertion_uuid, None).unwrap().batches,
            original
        );
        assert!(g.assertion(next, None).is_err());
        assert_eq!(
            g.open_research_branch(branch)
                .unwrap()
                .graph()
                .assertion(first.assertion_uuid, None)
                .unwrap()
                .batches,
            original
        );
        let parent = g
            .inspect_research_claims(&InspectResearchClaimsRequest {
                context: ResearchContext::Project,
                community_uuid: None,
                include_suppressed: true,
            })
            .unwrap();
        assert_eq!(
            parent
                .batches
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );
        assert_eq!(
            parent.batches[0]
                .column_by_name("category")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0),
            "interpretation"
        );
    };
    assert_classification(&g);
    let suppress = change(
        &g,
        branch,
        ResearchClaimChange::Suppress {
            suppression_uuid: Uuid::now_v7(),
            assertion_uuid: first.assertion_uuid,
            provenance_uuid: provenance,
        },
    );
    g.change_research_branch_claim(&suppress, &CancellationToken::new())
        .unwrap();
    assert_eq!(rows(g.inspect_research_claims(&view).unwrap()), 1);
    let hidden = InspectResearchClaimsRequest {
        include_suppressed: true,
        ..view.clone()
    };
    assert_eq!(rows(g.inspect_research_claims(&hidden).unwrap()), 2);
    let branch_view = g.open_research_branch(branch).unwrap();
    assert_eq!(
        branch_view
            .graph()
            .assertion(first.assertion_uuid, None)
            .unwrap()
            .batches,
        original
    );
    assert_eq!(
        g.assertion(first.assertion_uuid, None).unwrap().batches,
        original
    );
    assert!(g.assertion(next, None).is_err());
    assert_eq!(count(branch_view.graph()), 1);
    assert_eq!(
        branch_view
            .graph()
            .rank(
                "ClaimSubject",
                RankOptions {
                    by: graphforge_core::algorithms::RankAlgorithm::Degree,
                    via: None,
                    directed: true,
                    write_property: None
                }
            )
            .unwrap(),
        raw_rank
    );
    assert!(
        branch_view
            .graph()
            .research_canonical_choices(&ResearchContext::Project, None)
            .is_err()
    );
    assert_eq!(count(&g), 1);
    assert_eq!(
        rows(
            g.research_canonical_choices(&ResearchContext::Project, None)
                .unwrap()
        ),
        1
    );
    assert_eq!(
        rows(
            g.research_claim_history(&ResearchClaimHistoryRequest {
                context: context.clone(),
                family: ResearchClaimHistoryKind::Suppressions,
                assertion_uuid: Some(first.assertion_uuid)
            })
            .unwrap()
        ),
        1
    );
    drop(branch_view);
    drop(g);
    let mut g = GraphForge::new(root.to_str()).unwrap();
    assert_classification(&g);
    assert_eq!(rows(g.inspect_research_claims(&view).unwrap()), 1);
    let before = current(&g);
    g.change_research_branch_claim(&suppress, &CancellationToken::new())
        .unwrap();
    assert_eq!(current(&g), before);
    assert_eq!(
        rows(
            g.research_claim_history(&ResearchClaimHistoryRequest {
                context,
                family: ResearchClaimHistoryKind::Status,
                assertion_uuid: Some(first.assertion_uuid)
            })
            .unwrap()
        ),
        2
    );
}

#[test]
fn operation_identity_cannot_replay_a_different_existing_relation() {
    use graphforge_knowledge::research::{ClaimRelationKind, ClaimRelationRecord};
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:ClaimSubject)").unwrap();
    enable_knowledge(&mut g);
    let a = claim_request(&g, "A");
    g.create_research_claim(&a, &CancellationToken::new())
        .unwrap();
    let b = claim_request(&g, "B");
    g.create_research_claim(&b, &CancellationToken::new())
        .unwrap();
    let first = RelateResearchClaimsRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        relation: ClaimRelationRecord {
            relation_uuid: Uuid::now_v7(),
            source_assertion_uuid: a.assertion_uuid,
            target_assertion_uuid: b.assertion_uuid,
            kind: ClaimRelationKind::Supports,
            creator_uuid: a.creator_uuid,
            provenance_uuid: claim_provenance(&g, a.assertion_uuid),
            recorded_at: 30,
        },
    };
    g.relate_research_claims(&first, &CancellationToken::new())
        .unwrap();
    let mut second = first.clone();
    second.operation_uuid = Uuid::now_v7();
    second.expected_generation_uuid = current(&g);
    second.relation.relation_uuid = Uuid::now_v7();
    second.relation.kind = ClaimRelationKind::Contradicts;
    g.relate_research_claims(&second, &CancellationToken::new())
        .unwrap();
    let before = current(&g);
    second.operation_uuid = first.operation_uuid;
    assert!(
        g.relate_research_claims(&second, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(current(&g), before);
    let mut changed = a.clone();
    changed.expected_generation_uuid = current(&g);
    assert!(
        g.create_research_claim(&changed, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(current(&g), before);
    assert_eq!(
        rows(
            g.create_research_claim(&a, &CancellationToken::new())
                .unwrap()
        ),
        1
    );
    assert_eq!(
        rows(
            g.relate_research_claims(&first, &CancellationToken::new())
                .unwrap()
        ),
        1
    );
    assert_eq!(current(&g), before);
}
#[test]
fn restoration_keeps_history_and_allows_revoking_an_absent_subject() {
    let mut g = GraphForge::new(None).unwrap();
    let context = Uuid::now_v7();
    let version = Uuid::now_v7();
    let capture = g
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: version,
            context_uuid: context,
            label: None,
            description: None,
            created_at: 1,
            required_versions: Default::default(),
        })
        .unwrap();
    g.commit_research_version_operation(capture, &CancellationToken::new())
        .unwrap();
    g.execute("CREATE (:ClaimSubject)").unwrap();
    let promoted = request(&g, ResearchContext::Project, ResearchDecisionKind::Promote);
    g.record_research_decisions(&promoted, &CancellationToken::new())
        .unwrap();
    g.commit_research_version_operation(
        ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(&g),
            mutation: ResearchMutation::RestoreProject {
                context_uuid: context,
                source_version: version,
                version_uuid: Uuid::now_v7(),
                created_at: 2,
            },
        },
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(count(&g), 0);
    assert_eq!(
        rows(
            g.research_decision_history(&ResearchContext::Project, None)
                .unwrap()
        ),
        1
    );
    let mut revoke = promoted.clone();
    revoke.operation_uuid = Uuid::now_v7();
    revoke.expected_generation_uuid = current(&g);
    revoke.decisions[0].decision_uuid = Uuid::now_v7();
    revoke.decisions[0].kind = ResearchDecisionKind::Revoke;
    g.record_research_decisions(&revoke, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        rows(
            g.research_canonical_choices(&ResearchContext::Project, None)
                .unwrap()
        ),
        0
    );
    assert_eq!(
        rows(
            g.research_decision_history(&ResearchContext::Project, None)
                .unwrap()
        ),
        2
    );
    assert_eq!(count(&g), 0);
}
#[test]
fn failed_private_revision_leaves_no_new_authoritative_claim_or_version() {
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:ClaimSubject)").unwrap();
    enable_knowledge(&mut g);
    let a = claim_request(&g, "Original");
    g.create_research_claim(&a, &CancellationToken::new())
        .unwrap();
    let branch = create_branch(&mut g);
    let before = current(&g);
    let head = g.open_research_branch(branch).unwrap().version_uuid();
    let successor = Uuid::now_v7();
    let bad = change(
        &g,
        branch,
        ResearchClaimChange::Revise {
            prior_assertion_uuid: a.assertion_uuid,
            claim: ResearchClaimDraft {
                assertion_uuid: successor,
                claim: "Private intermediate".into(),
                graph_refs: a.graph_refs,
                category: a.category,
                run_uuid: None,
            },
            supersession_uuid: Uuid::now_v7(),
            status_event_uuid: Uuid::now_v7(),
            reasoning_uuid: Uuid::now_v7(),
            rationale: "Requires missing provenance".into(),
            provenance_uuid: Uuid::now_v7(),
            relation_uuid: Uuid::now_v7(),
        },
    );
    assert!(
        g.change_research_branch_claim(&bad, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(current(&g), before);
    let view = g.open_research_branch(branch).unwrap();
    assert_eq!(view.version_uuid(), head);
    assert!(view.graph().assertion(successor, None).is_err());
    assert!(g.research_version(bad.version_uuid).is_err());
}

#[test]
fn child_inherits_frozen_suppression_while_sibling_and_parent_remain_visible() {
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:ClaimSubject)").unwrap();
    enable_knowledge(&mut g);
    let claim = claim_request(&g, "Shared interpretation");
    g.create_research_claim(&claim, &CancellationToken::new())
        .unwrap();
    let provenance = claim_provenance(&g, claim.assertion_uuid);
    let parent = create_branch(&mut g);
    let sibling = create_branch(&mut g);
    let suppress = change(
        &g,
        parent,
        ResearchClaimChange::Suppress {
            suppression_uuid: Uuid::now_v7(),
            assertion_uuid: claim.assertion_uuid,
            provenance_uuid: provenance,
        },
    );
    g.change_research_branch_claim(&suppress, &CancellationToken::new())
        .unwrap();
    let child = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Branch {
            branch_uuid: parent,
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 40,
        label: "Inherited suppression".into(),
    };
    g.create_research_branch(&child, &CancellationToken::new())
        .unwrap();
    for (context, expected) in [
        (ResearchContext::Project, 1),
        (
            ResearchContext::Branch {
                branch_uuid: sibling,
            },
            1,
        ),
        (
            ResearchContext::Branch {
                branch_uuid: child.branch_uuid,
            },
            0,
        ),
    ] {
        assert_eq!(
            rows(
                g.inspect_research_claims(&InspectResearchClaimsRequest {
                    context,
                    community_uuid: None,
                    include_suppressed: false
                })
                .unwrap()
            ),
            expected
        );
    }
    assert_eq!(
        rows(
            g.research_claim_history(&ResearchClaimHistoryRequest {
                context: ResearchContext::Branch {
                    branch_uuid: child.branch_uuid
                },
                family: ResearchClaimHistoryKind::Suppressions,
                assertion_uuid: Some(claim.assertion_uuid)
            })
            .unwrap()
        ),
        1
    );
    assert_eq!(
        count(g.open_research_branch(child.branch_uuid).unwrap().graph()),
        1
    );
}

#[test]
fn supported_and_statusless_claims_require_separate_canonical_promotion() {
    use arrow::array::{Array, BooleanArray, StringArray};
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:ClaimSubject)").unwrap();
    enable_knowledge(&mut g);
    let claim = claim_request(&g, "Status and authority are independent");
    g.create_research_claim(&claim, &CancellationToken::new())
        .unwrap();
    let inspect = InspectResearchClaimsRequest {
        context: ResearchContext::Project,
        community_uuid: None,
        include_suppressed: false,
    };
    let result = g.inspect_research_claims(&inspect).unwrap();
    assert_eq!(
        result.batches[0]
            .column_by_name("status")
            .unwrap()
            .null_count(),
        1
    );
    assert!(
        !result.batches[0]
            .column_by_name("canonical")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    g.record_assertion_status(RecordAssertionStatusRequest {
        context: WriteContext {
            operation_uuid: OperationId(Uuid::now_v7()),
            actor_uuid: None,
        },
        status_event_uuid: Uuid::now_v7(),
        assertion_uuid: claim.assertion_uuid,
        status: AssertionStatus::Supported,
        confidence_uuid: None,
        reasoning_uuid: None,
        provenance_uuid: claim_provenance(&g, claim.assertion_uuid),
    })
    .unwrap();
    let result = g.inspect_research_claims(&inspect).unwrap();
    assert_eq!(
        result.batches[0]
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "supported"
    );
    assert!(
        !result.batches[0]
            .column_by_name("canonical")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    let mut promote = request(&g, ResearchContext::Project, ResearchDecisionKind::Promote);
    promote.decisions[0].subject_kind = ResearchSubjectKind::Assertion;
    promote.decisions[0].subject_uuid = claim.assertion_uuid;
    g.record_research_decisions(&promote, &CancellationToken::new())
        .unwrap();
    let result = g.inspect_research_claims(&inspect).unwrap();
    assert!(
        result.batches[0]
            .column_by_name("canonical")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0)
    );
    assert_eq!(
        result.batches[0]
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "supported"
    );
}

#[test]
fn contextual_claim_contract_pins_closed_native_request_values() {
    use graphforge_knowledge::research::{ClaimRelationKind, ResearchCategory};
    let contract: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/contracts/research-claims-api-v1.json"
    ))
    .unwrap();
    for value in contract["categories"].as_array().unwrap() {
        let category: ResearchCategory = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(category).unwrap(), *value);
    }
    for value in contract["relation_kinds"].as_array().unwrap() {
        let kind: ClaimRelationKind = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(kind).unwrap(), *value);
    }
    assert_eq!(
        contract["family_rows"],
        graphforge_knowledge::research::MAX_RESEARCH_ROWS
    );
    assert!(serde_json::from_value::<ResearchCategory>(serde_json::json!("evidence")).is_err());
    assert!(
        serde_json::from_value::<ResearchAuthorityQuery>(
            serde_json::json!({"context":{"kind":"project"},"community_uuid":null,"canonical":true})
        )
        .is_err()
    );
}

#[test]
fn bring_selected_claim_keeps_classification_without_importing_source_authority() {
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:ClaimSubject)").unwrap();
    enable_knowledge(&mut g);
    let destination = create_branch(&mut g);
    let claim = claim_request(&g, "Source interpretation");
    g.create_research_claim(&claim, &CancellationToken::new())
        .unwrap();
    let mut promote = request(&g, ResearchContext::Project, ResearchDecisionKind::Promote);
    promote.decisions[0].subject_kind = ResearchSubjectKind::Assertion;
    promote.decisions[0].subject_uuid = claim.assertion_uuid;
    g.record_research_decisions(&promote, &CancellationToken::new())
        .unwrap();
    let version = Uuid::now_v7();
    let capture = g
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: version,
            context_uuid: Uuid::now_v7(),
            label: None,
            description: None,
            created_at: 40,
            required_versions: Default::default(),
        })
        .unwrap();
    g.commit_research_version_operation(capture, &CancellationToken::new())
        .unwrap();
    let frozen = g
        .freeze_slice(
            &SliceRequest {
                request_uuid: Uuid::now_v7(),
                source: SliceSource::Version {
                    version_uuid: version,
                },
                selector: SliceSelector::Direct {
                    members: SliceMembers {
                        assertions: std::collections::BTreeSet::from([claim.assertion_uuid]),
                        ..Default::default()
                    },
                },
                include: Default::default(),
                exclude: Default::default(),
                limits: Default::default(),
            },
            &CancellationToken::new(),
        )
        .unwrap();
    let mut frozen_ipc = Vec::new();
    {
        let mut writer =
            arrow::ipc::writer::StreamWriter::try_new(&mut frozen_ipc, frozen.schema.as_ref())
                .unwrap();
        for batch in frozen.batches {
            writer.write(&batch).unwrap();
        }
        writer.finish().unwrap();
    }
    g.bring_research_branch(
        &BringResearchBranchRequest {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(&g),
            branch_uuid: destination,
            version_uuid: Uuid::now_v7(),
            frozen_ipc,
            created_at: 50,
        },
        &CancellationToken::new(),
    )
    .unwrap();
    let context = ResearchContext::Branch {
        branch_uuid: destination,
    };
    let inspected = g
        .inspect_research_claims(&InspectResearchClaimsRequest {
            context: context.clone(),
            community_uuid: None,
            include_suppressed: false,
        })
        .unwrap();
    assert_eq!(
        inspected
            .batches
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        inspected.batches[0]
            .column_by_name("category")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0),
        "interpretation"
    );
    assert_eq!(
        rows(g.research_canonical_choices(&context, None).unwrap()),
        0
    );
    assert_eq!(
        rows(
            g.research_canonical_choices(&ResearchContext::Project, None)
                .unwrap()
        ),
        1
    );
    let mut integrate = promote;
    integrate.operation_uuid = Uuid::now_v7();
    integrate.expected_generation_uuid = current(&g);
    integrate.context = context.clone();
    integrate.decisions[0].decision_uuid = Uuid::now_v7();
    integrate.decisions[0].kind = ResearchDecisionKind::Integrate;
    integrate.decisions[0].source_version_uuid = Some(version);
    g.record_research_decisions(&integrate, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        rows(g.research_canonical_choices(&context, None).unwrap()),
        0
    );
    assert_eq!(
        rows(g.research_decision_history(&context, None).unwrap()),
        1
    );
}
