//! Preview reads real native Branch state without advancing either authority.
mod assertions;
mod history;
mod ontology;
mod recovery;
mod repeated;
use super::*;
use crate::*;
use uuid::Uuid;

#[test]
fn preview_is_pinned_read_only_and_requires_explicit_conflict_selection() {
    let mut graph = GraphForge::new(None).unwrap();
    graph.execute("CREATE (:Item {x:0,y:0})").unwrap();
    let current = |g: &GraphForge| g.generation_for_read().unwrap().generation_uuid();
    let cancel = CancellationToken::new();
    let branch = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "Selective study".into(),
    };
    graph.create_research_branch(&branch, &cancel).unwrap();
    graph
        .execute("MATCH (n:Item) SET n.x = 1, n.y = 2")
        .unwrap();
    let request = PreviewResearchUpstreamRequest {
        branch_uuid: branch.branch_uuid,
        scope: ResearchUpstreamScope::Branch,
    };
    let before = current(&graph);
    let snapshot = preview::load(&graph, &request, &cancel).unwrap();
    let properties: Vec<_> = snapshot
        .rows
        .iter()
        .filter(|row| row.key.2.starts_with("property:"))
        .collect();
    assert_eq!(properties.len(), 2);
    assert!(properties.iter().all(|row| row.change == "upstream"));
    graph.preview_research_upstream(&request, &cancel).unwrap();
    assert_eq!(current(&graph), before);
    assert_eq!(snapshot.branch.base_version_uuid, branch.version_uuid);
    let digest = snapshot.digest;
    assert_eq!(
        preview::load(&graph, &request, &cancel).unwrap().digest,
        digest
    );
    let mut update = UpdateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: before,
        version_uuid: Uuid::now_v7(),
        preview: request.clone(),
        preview_sha256: digest,
        selection: ResearchUpstreamSelection::AllCompatible,
        acknowledge_evidence: Default::default(),
        actor_uuid: Uuid::now_v7(),
        created_at: 2,
        explanation: String::new(),
    };
    let selected = selection::validate(&snapshot, &update).unwrap();
    assert!(properties.iter().all(|row| selected.contains_key(&row.key)));
    graph
        .execute_research_branch(
            &ExecuteResearchBranchRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: before,
                branch_uuid: branch.branch_uuid,
                version_uuid: Uuid::now_v7(),
                created_at: 3,
                query: "MATCH (n:Item) SET n.x = 3".into(),
            },
            &cancel,
        )
        .unwrap();
    let changed = preview::load(&graph, &request, &cancel).unwrap();
    assert!(selection::validate(&changed, &update).is_err());
    update.expected_generation_uuid = current(&graph);
    update.preview_sha256 = changed.digest;
    let selected = selection::validate(&changed, &update).unwrap();
    let conflict = changed
        .rows
        .iter()
        .find(|row| row.key.2 == "property:x")
        .unwrap();
    assert_eq!(conflict.change, "conflict");
    assert!(!selected.contains_key(&conflict.key));
    assert!(selected.keys().any(|key| key.2 == "property:y"));
    update.selection = ResearchUpstreamSelection::Selected {
        decisions: vec![ResearchUpstreamDecision {
            unit: ResearchFieldIdentity {
                object_kind: conflict.key.0.clone(),
                object_uuid: conflict.key.1,
                field: conflict.key.2.clone(),
            },
            resolution: ResearchUpstreamResolution::KeepLocal,
        }],
    };
    assert_eq!(selection::validate(&changed, &update).unwrap().len(), 1);
    let receipt = graph.update_research_branch(&update, &cancel).unwrap();
    let incorporated = preview::load(&graph, &request, &cancel).unwrap();
    let x = incorporated
        .rows
        .iter()
        .find(|row| row.key.2 == "property:x")
        .unwrap();
    assert_eq!(x.change, "local");
    assert_eq!(x.baseline, conflict.right);
    assert_eq!(x.left, conflict.left);
    let y = incorporated
        .rows
        .iter()
        .find(|row| row.key.2 == "property:y")
        .unwrap();
    assert_eq!(y.change, "upstream");
    assert_eq!(
        y.incorporated,
        snapshot
            .rows
            .iter()
            .find(|row| row.key.2 == "property:y")
            .unwrap()
            .incorporated
    );
    assert_eq!(
        graph.update_research_branch(&update, &cancel).unwrap(),
        receipt
    );
    let history = graph
        .research_upstream_history(
            &ResearchUpstreamHistoryRequest {
                branch_uuid: branch.branch_uuid,
                page_size: 10,
                after: None,
            },
            &cancel,
        )
        .unwrap();
    assert_eq!(history.stats.rows_produced, 1);
    let mut conflicting = update.clone();
    conflicting.explanation = "different request".into();
    assert_eq!(
        graph
            .update_research_branch(&conflicting, &cancel)
            .unwrap_err()
            .code(),
        "GF_IDEMPOTENCY_CONFLICT"
    );
}

#[test]
fn preference_only_source_update_is_visible_without_changing_immutable_source() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    let context = || WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    };
    for capability_id in [CapabilityId::Provenance, CapabilityId::Knowledge] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    let source_uuid = Uuid::now_v7();
    graph
        .register_source(RegisterSourceRequest {
            context: context(),
            source_uuid,
            label: "Manuscript".into(),
            source_kind: SourceKind::Manuscript,
            identity_uri: None,
        })
        .unwrap();
    let first = Uuid::now_v7();
    let second = Uuid::now_v7();
    for artifact_uuid in [first] {
        graph
            .register_artifact(RegisterArtifactRequest {
                context: context(),
                artifact_uuid,
                source_uuid,
                artifact_kind: ArtifactKind::RawScan,
                media_type: "image/png".into(),
                payload: ArtifactPayloadRequest::LocalBytes(vec![1, 2, 3]),
                derivation_inputs: vec![],
                run_uuid: None,
            })
            .unwrap();
    }
    let historical = Uuid::now_v7();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: context(),
            artifact_uuid: historical,
            source_uuid,
            artifact_kind: ArtifactKind::NormalizedText,
            media_type: "text/plain".into(),
            payload: ArtifactPayloadRequest::LocalBytes(b"Historical normalized reading".to_vec()),
            derivation_inputs: vec![DerivationInput {
                input_uuid: first,
                input_kind: DerivationSubjectKind::Artifact,
            }],
            run_uuid: None,
        })
        .unwrap();
    let historical_derivations =
        crate::knowledge::ledger::read_derivation_ledger(&graph.generation_for_read().unwrap())
            .unwrap();
    let set_preference = |graph: &GraphForge, artifact_uuid| {
        graph
            .set_preferred_artifact(SetPreferredArtifactRequest {
                context: context(),
                preference_event_uuid: Uuid::now_v7(),
                source_uuid,
                artifact_uuid,
                reason: "Reviewed representation".into(),
            })
            .unwrap();
    };
    graph.set_clock_for_test(|| Ok(9000));
    set_preference(&graph, first);
    let current = |g: &GraphForge| g.generation_for_read().unwrap().generation_uuid();
    let cancel = CancellationToken::new();
    let branch = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "Source review".into(),
    };
    graph.create_research_branch(&branch, &cancel).unwrap();
    let child = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Branch {
            branch_uuid: branch.branch_uuid,
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 2,
        label: "Nested source review".into(),
    };
    graph.create_research_branch(&child, &cancel).unwrap();
    let unselected_artifact = Uuid::now_v7();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: context(),
            artifact_uuid: unselected_artifact,
            source_uuid,
            artifact_kind: ArtifactKind::Other,
            media_type: "application/octet-stream".into(),
            payload: ArtifactPayloadRequest::ExternalReference {
                uri: "https://private.invalid/unselected".into(),
                fingerprint: Some([19; 32]),
            },
            derivation_inputs: vec![],
            run_uuid: None,
        })
        .unwrap();
    graph.set_clock_for_test(|| Ok(1000));
    graph
        .register_artifact(RegisterArtifactRequest {
            context: context(),
            artifact_uuid: second,
            source_uuid,
            artifact_kind: ArtifactKind::OcrText,
            media_type: "text/plain".into(),
            payload: ArtifactPayloadRequest::LocalBytes(b"Reviewed OCR reading".to_vec()),
            derivation_inputs: vec![DerivationInput {
                input_uuid: first,
                input_kind: DerivationSubjectKind::Artifact,
            }],
            run_uuid: None,
        })
        .unwrap();
    set_preference(&graph, second);
    assert_eq!(
        crate::knowledge::ledger::read_preference_ledger(&graph.generation_for_read().unwrap())
            .unwrap()
            .current_preferred_artifact(source_uuid),
        Some(second)
    );
    let request = PreviewResearchUpstreamRequest {
        branch_uuid: branch.branch_uuid,
        scope: ResearchUpstreamScope::Sources,
    };
    let review = preview::load(&graph, &request, &cancel).unwrap();
    let changes: Vec<_> = review
        .rows
        .iter()
        .filter(|row| row.key.0 == "source")
        .collect();
    assert_eq!(changes.len(), 1);
    assert_eq!(
        changes[0].key,
        ("source".into(), source_uuid, "$preferred_artifact".into())
    );
    assert_eq!(changes[0].change, "upstream");
    assert_eq!(changes[0].baseline, changes[0].left);
    assert_ne!(changes[0].left, changes[0].right);
    let old = graph.open_research_branch(branch.branch_uuid).unwrap();
    let preferences = crate::knowledge::ledger::read_preference_ledger(
        &old.graph().generation_for_read().unwrap(),
    )
    .unwrap();
    assert_eq!(preferences.events.len(), 1);
    assert_eq!(preferences.events[0].artifact_uuid, first);
    drop(old);
    let mut update = UpdateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        version_uuid: Uuid::now_v7(),
        preview: request,
        preview_sha256: review.digest,
        selection: ResearchUpstreamSelection::Selected {
            decisions: vec![ResearchUpstreamDecision {
                unit: ResearchFieldIdentity {
                    object_kind: "source".into(),
                    object_uuid: source_uuid,
                    field: "$preferred_artifact".into(),
                },
                resolution: ResearchUpstreamResolution::AdoptUpstream,
            }],
        },
        acknowledge_evidence: Default::default(),
        actor_uuid: Uuid::now_v7(),
        created_at: 4,
        explanation: "Adopt the reviewed representation".into(),
    };
    let before = current(&graph);
    assert!(
        graph.update_research_branch(&update, &cancel).is_err(),
        "new preferred Artifact requires explicit dependency adoption"
    );
    assert_eq!(current(&graph), before);
    let ResearchUpstreamSelection::Selected { decisions } = &mut update.selection else {
        unreachable!()
    };
    for key in
        &review.requirements[&("source".into(), source_uuid, "$preferred_artifact".into())].fields
    {
        decisions.push(ResearchUpstreamDecision {
            unit: ResearchFieldIdentity {
                object_kind: key.0.clone(),
                object_uuid: key.1,
                field: key.2.clone(),
            },
            resolution: ResearchUpstreamResolution::AdoptUpstream,
        });
    }
    let receipt = graph.update_research_branch(&update, &cancel).unwrap();
    let updated = graph.open_research_branch(branch.branch_uuid).unwrap();
    let registry = graph.research_version_retention().unwrap();
    let upstream_version = registry.upstream.reviews[&update.operation_uuid].upstream_version_uuid;
    assert!(
        !serde_json::to_string(&registry.upstream.reviews[&update.operation_uuid])
            .unwrap()
            .contains(&unselected_artifact.to_string())
    );
    assert!(
        !crate::knowledge::ledger::read_artifact_ledger(
            &updated.graph().generation_for_read().unwrap()
        )
        .unwrap()
        .artifacts
        .iter()
        .any(|artifact| artifact.artifact_uuid == unselected_artifact)
    );

    let baseline = crate::branches::baseline::read(updated.graph()).unwrap();
    for (key, row) in &baseline {
        if key.0 == "artifact" && key.1 == second {
            assert_eq!(row.origin, upstream_version);
            assert_eq!(row.incorporated, Some(upstream_version));
            assert_eq!(row.original, row.current);
        }
    }

    let adopted = crate::knowledge::ledger::read_preference_ledger(
        &updated.graph().generation_for_read().unwrap(),
    )
    .unwrap();
    assert_eq!(
        adopted.current_preferred_artifact(source_uuid),
        Some(second)
    );
    assert_eq!(adopted.events.len(), 2);
    assert!(
        adopted
            .events
            .iter()
            .any(|event| event == &preferences.events[0])
    );
    assert_eq!(
        graph.update_research_branch(&update, &cancel).unwrap(),
        receipt
    );
    let child_preview = PreviewResearchUpstreamRequest {
        branch_uuid: child.branch_uuid,
        scope: ResearchUpstreamScope::Sources,
    };
    let child_view = preview::load(&graph, &child_preview, &cancel).unwrap();
    assert_eq!(child_view.upstream.version, Some(update.version_uuid));
    let child_decisions = child_view
        .rows
        .iter()
        .map(|row| ResearchUpstreamDecision {
            unit: ResearchFieldIdentity {
                object_kind: row.key.0.clone(),
                object_uuid: row.key.1,
                field: row.key.2.clone(),
            },
            resolution: if row.key.2 == "$preferred_artifact" {
                ResearchUpstreamResolution::KeepLocal
            } else {
                ResearchUpstreamResolution::AdoptUpstream
            },
        })
        .collect();
    let child_update = UpdateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        version_uuid: Uuid::now_v7(),
        preview: child_preview,
        preview_sha256: child_view.digest,
        selection: ResearchUpstreamSelection::Selected {
            decisions: child_decisions,
        },
        acknowledge_evidence: Default::default(),
        actor_uuid: Uuid::now_v7(),
        created_at: 5,
        explanation: "Adopt immediate parent".into(),
    };
    graph
        .update_research_branch(&child_update, &cancel)
        .unwrap();
    let child_branch = graph.open_research_branch(child.branch_uuid).unwrap();
    let child_baseline = crate::branches::baseline::read(child_branch.graph()).unwrap();
    assert_eq!(
        crate::knowledge::ledger::read_preference_ledger(
            &child_branch.graph().generation_for_read().unwrap()
        )
        .unwrap()
        .current_preferred_artifact(source_uuid),
        Some(first)
    );
    assert_eq!(
        crate::knowledge::ledger::read_preference_ledger(
            &updated.graph().generation_for_read().unwrap()
        )
        .unwrap()
        .current_preferred_artifact(source_uuid),
        Some(second)
    );

    for (key, row) in &baseline {
        if key.0 == "artifact" && key.1 == second {
            let inherited = &child_baseline[key];
            assert_eq!(inherited.origin, row.origin);
            assert_eq!(inherited.contribution, row.contribution);
            assert_eq!(inherited.original, row.original);
            assert_eq!(inherited.incorporated, Some(update.version_uuid));
        }
    }
    drop(child_branch);
    drop(updated);
    drop(graph);
    graphforge_storage::execute_project_cleanup(
        &root,
        graphforge_storage::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        Default::default(),
    )
    .unwrap();
    let mut reopened = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        reopened.update_research_branch(&update, &cancel).unwrap(),
        receipt
    );
    let historical_view = reopened.open_research_version(branch.version_uuid).unwrap();
    let payload = historical_view.artifact_payload(historical).unwrap();
    assert_eq!(payload.batches[0].num_rows(), 1);
    assert_eq!(
        payload.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap()
            .value(0),
        b"Historical normalized reading"
    );
    let child_branch = reopened.open_research_branch(child.branch_uuid).unwrap();
    let recovered_baseline = crate::branches::baseline::read(child_branch.graph()).unwrap();
    for (key, row) in &baseline {
        if key.0 == "artifact" && key.1 == second {
            assert_eq!(recovered_baseline[key].origin, row.origin);
            assert_eq!(recovered_baseline[key].contribution, row.contribution);
        }
    }
    let recovered_derivations = crate::knowledge::ledger::read_derivation_ledger(
        &child_branch.graph().generation_for_read().unwrap(),
    )
    .unwrap();
    for original in historical_derivations.derivations {
        assert!(recovered_derivations.derivations.contains(&original));
    }
}

#[test]
fn explanatory_claim_and_review_publish_with_the_same_branch_version() {
    let mut graph = GraphForge::new(None).unwrap();
    for capability_id in [
        CapabilityId::Provenance,
        CapabilityId::Knowledge,
        CapabilityId::Epistemic,
    ] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: WriteContext {
                    operation_uuid: OperationId(Uuid::now_v7()),
                    actor_uuid: None,
                },
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    let mut request = recovery::prepare(&mut graph);
    let assertion_uuid = Uuid::now_v7();
    let ResearchUpstreamSelection::Selected { decisions } = &mut request.selection else {
        unreachable!()
    };
    decisions[0].resolution = ResearchUpstreamResolution::Explain {
        claim: ResearchClaimDraft {
            assertion_uuid,
            claim: "The local classification follows the reviewed manuscript.".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: decisions[0].unit.object_uuid,
                graph_kind: graphforge_knowledge::GraphObjectKind::Node,
                role: graphforge_knowledge::AssertionGraphRole::Subject,
                ordinal: 0,
            }],
            category: graphforge_knowledge::research::ResearchCategory::Interpretation,
            run_uuid: None,
        },
    };
    let receipt = graph
        .update_research_branch(&request, &CancellationToken::new())
        .unwrap();
    let branch = graph
        .open_research_branch(request.preview.branch_uuid)
        .unwrap();
    let ledger =
        crate::research_claims::ledger::read_claims(&branch.graph().generation_for_read().unwrap())
            .unwrap();
    assert!(
        ledger
            .claims()
            .iter()
            .any(|claim| claim.assertion_uuid == assertion_uuid)
    );
    assert!(
        !crate::research_claims::ledger::read_claims(&graph.generation_for_read().unwrap())
            .unwrap()
            .claims()
            .iter()
            .any(|claim| claim.assertion_uuid == assertion_uuid)
    );
    let registry = graph.research_version_retention().unwrap();
    let review = &registry.upstream.reviews[&request.operation_uuid];
    assert_eq!(review.version_uuid, receipt.version_uuid.unwrap());
    assert_eq!(
        review.fields[0].resolution,
        graphforge_storage::research_versions::ResearchUpstreamResolutionRecord::Explain {
            assertion_uuid
        }
    );
    assert_eq!(
        graph
            .update_research_branch(&request, &CancellationToken::new())
            .unwrap(),
        receipt
    );
}
