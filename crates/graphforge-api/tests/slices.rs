//! Native Slice semantics over a shared character and explicit source contexts.
use arrow::array::{FixedSizeBinaryArray, StringArray};
use graphforge_api::*;
use std::collections::BTreeSet;
use std::str::FromStr;
use uuid::Uuid;
fn request(selector: SliceSelector) -> SliceRequest {
    SliceRequest {
        request_uuid: Uuid::now_v7(),
        source: SliceSource::Current,
        selector,
        include: SliceMembers::default(),
        exclude: SliceMembers::default(),
        limits: SliceLimits::default(),
    }
}
fn id(graph: &GraphForge, name: &str) -> Uuid {
    let result = graph
        .execute(&format!(
            "MATCH (n {{name: '{name}'}}) RETURN n.node_uuid AS node_uuid"
        ))
        .unwrap();
    Uuid::from_slice(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap()
}
fn rows(graph: &GraphForge, request: &SliceRequest, kind: SlicePageKind) -> Vec<(String, String)> {
    let result = graph
        .preview_slice(request, kind, PageRequest::default())
        .unwrap();
    let batch = &result.batches[0];
    let kind = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..batch.num_rows())
        .map(|i| (kind.value(i).into(), ids.value(i).into()))
        .collect()
}
fn fixture() -> GraphForge {
    let graph = GraphForge::new(None).unwrap();
    graph.execute("CREATE (a:Story {name: 'First'}), (b:Story {name: 'Second'}), (c:Character {name: 'Shared'}), (a)-[:FEATURES]->(c), (b)-[:FEATURES]->(c)").unwrap();
    graph
}
#[test]
fn shared_character_has_separate_boundary_and_deterministic_explanations() {
    let graph = fixture();
    let first = id(&graph, "First");
    let shared = id(&graph, "Shared");
    let second = id(&graph, "Second");
    let mut request = request(SliceSelector::Traverse {
        seeds: BTreeSet::from([first]),
        direction: SliceDirection::Both,
        max_depth: 1,
        relationship_types: BTreeSet::new(),
    });
    let included = rows(&graph, &request, SlicePageKind::Included);
    assert_eq!(included.len(), 3);
    assert!(included.contains(&("node".into(), shared.to_string())));
    assert!(!included.contains(&("node".into(), second.to_string())));
    let boundary = rows(&graph, &request, SlicePageKind::Boundary);
    assert!(boundary.contains(&("node".into(), second.to_string())));
    assert_eq!(
        rows(&graph, &request, SlicePageKind::Explanations),
        included
    );
    request.include.nodes.insert(second);
    assert_eq!(rows(&graph, &request, SlicePageKind::Included).len(), 4);
    request.exclude.nodes.insert(shared);
    assert!(
        !rows(&graph, &request, SlicePageKind::Included)
            .contains(&("node".into(), shared.to_string()))
    );
    assert!(
        rows(&graph, &request, SlicePageKind::Dependencies)
            .contains(&("node".into(), shared.to_string()))
    );
}
#[test]
fn selectors_page_bounds_and_cancellation_use_real_execution() {
    let graph = fixture();
    let shared = id(&graph, "Shared");
    let selectors = [
        SliceSelector::Direct {
            members: SliceMembers {
                nodes: BTreeSet::from([shared]),
                ..Default::default()
            },
        },
        SliceSelector::Filter {
            label: "Character".into(),
            property: "name".into(),
            equals: serde_json::json!("Shared"),
        },
        SliceSelector::Query {
            query: "MATCH (n:Character) RETURN n.node_uuid AS node_uuid".into(),
        },
        SliceSelector::Search {
            label: "Character".into(),
            text: "Shared".into(),
            limit: 10,
        },
    ];
    for selector in selectors {
        assert_eq!(
            rows(&graph, &request(selector), SlicePageKind::Included),
            vec![("node".into(), shared.to_string())]
        );
    }
    let mut request = request(SliceSelector::Query {
        query: "MATCH (n) RETURN n.node_uuid AS node_uuid".into(),
    });
    let first = graph
        .preview_slice(
            &request,
            SlicePageKind::Included,
            PageRequest {
                limit: 1,
                ..Default::default()
            },
        )
        .unwrap();
    let after = PageToken::parse(&first.schema.metadata()["graphforge.next_page_token"]).unwrap();
    let next = PageRequest {
        limit: 1,
        after: Some(after),
        ..Default::default()
    };
    assert_eq!(
        graph
            .preview_slice(&request, SlicePageKind::Included, next.clone())
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );
    graph.execute("CREATE (:Story {name: 'Third'})").unwrap();
    assert!(matches!(
        graph.preview_slice(&request, SlicePageKind::Included, next),
        Err(GfError::Api {
            code: ApiErrorCode::PageSnapshotGone,
            ..
        })
    ));
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(matches!(
        graph.preview_slice(
            &request,
            SlicePageKind::Included,
            PageRequest {
                cancellation: Some(cancellation),
                ..Default::default()
            }
        ),
        Err(GfError::Api {
            code: ApiErrorCode::Cancelled,
            ..
        })
    ));
    request.limits.scanned_rows = 1;
    assert!(matches!(
        graph.preview_slice(&request, SlicePageKind::Included, PageRequest::default()),
        Err(GfError::Api {
            code: ApiErrorCode::ResourceLimit,
            ..
        })
    ));
}

fn capture(graph: &mut GraphForge) -> Uuid {
    let version_uuid = Uuid::now_v7();
    let prepared = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid,
            context_uuid: graph
                .research_version_retention()
                .unwrap()
                .heads
                .keys()
                .next()
                .copied()
                .unwrap_or_else(Uuid::now_v7),
            label: None,
            description: None,
            created_at: 1,
            required_versions: BTreeSet::new(),
        })
        .unwrap();
    graph
        .commit_research_version_operation(prepared, &CancellationToken::new())
        .unwrap();
    version_uuid
}
fn ipc(result: &ExecutionResult) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut writer =
        arrow::ipc::writer::StreamWriter::try_new(&mut bytes, result.schema.as_ref()).unwrap();
    for batch in &result.batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    bytes
}
#[test]
fn frozen_membership_ignores_parent_edits_and_rejects_forged_capsules() {
    let mut graph = fixture();
    let shared = id(&graph, "Shared");
    let version = capture(&mut graph);
    let mut request = request(SliceSelector::Query {
        query: "MATCH (n:Character) RETURN n.node_uuid AS node_uuid".into(),
    });
    request.source = SliceSource::Version {
        version_uuid: version,
    };
    let frozen = graph
        .freeze_slice(&request, &CancellationToken::new())
        .unwrap();
    let bytes = ipc(&frozen);
    assert_eq!(
        ipc(&graph
            .freeze_slice(&request, &CancellationToken::new())
            .unwrap()),
        bytes
    );
    graph
        .execute("CREATE (:Character {name: 'Later'})")
        .unwrap();
    let inspected = graph
        .inspect_frozen_slice(&bytes, SlicePageKind::Included, PageRequest::default())
        .unwrap();
    assert_eq!(inspected.batches[0].num_rows(), 1);
    assert_eq!(
        inspected.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        shared.to_string()
    );
    request.source = SliceSource::Current;
    assert_eq!(rows(&graph, &request, SlicePageKind::Included).len(), 2);
    let later = id(&graph, "Later");
    let revision = SliceRevisionRequest {
        request_uuid: Uuid::now_v7(),
        include: SliceMembers {
            nodes: BTreeSet::from([later]),
            ..Default::default()
        },
        exclude: Default::default(),
        source_version: None,
    };
    assert!(matches!(
        graph.revise_frozen_slice(&bytes, &revision, &CancellationToken::new()),
        Err(GfError::Api {
            code: ApiErrorCode::ResultNotRetained,
            ..
        })
    ));
    let newer = capture(&mut graph);
    let revised = graph
        .revise_frozen_slice(
            &bytes,
            &SliceRevisionRequest {
                source_version: Some(newer),
                ..revision
            },
            &CancellationToken::new(),
        )
        .unwrap();
    assert_eq!(
        graph
            .inspect_frozen_slice(
                &ipc(&revised),
                SlicePageKind::Included,
                PageRequest::default()
            )
            .unwrap()
            .batches[0]
            .num_rows(),
        2
    );
    let deletion = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: graph
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid,
        mutation: ResearchMutation::DeleteVersion {
            version_uuid: version,
        },
    };
    graph
        .commit_research_version_operation(deletion, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        graph
            .inspect_frozen_slice(&bytes, SlicePageKind::Included, PageRequest::default())
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );
    assert!(matches!(
        graph.revise_frozen_slice(
            &bytes,
            &SliceRevisionRequest {
                request_uuid: Uuid::now_v7(),
                include: Default::default(),
                exclude: Default::default(),
                source_version: None
            },
            &CancellationToken::new()
        ),
        Err(GfError::Api {
            code: ApiErrorCode::ResultNotRetained,
            ..
        })
    ));
    let mut corrupted = bytes.clone();
    let at = corrupted
        .windows(6)
        .position(|window| window == b"active")
        .unwrap();
    corrupted[at] = b'X';
    assert!(
        graph
            .inspect_frozen_slice(&corrupted, SlicePageKind::Included, PageRequest::default())
            .is_err()
    );
    assert!(
        graph
            .inspect_frozen_slice(
                &bytes[..bytes.len() - 1],
                SlicePageKind::Included,
                PageRequest::default()
            )
            .is_err()
    );
}

#[test]
fn selected_history_never_falls_back_to_current_or_retains_its_ancestor() {
    use graphforge_storage::research_versions::{
        RegisterResearchVersion, ResearchGraphSelection, publish_research_operation,
    };
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph.execute("CREATE (a:Story {name: 'First'}), (b:Story {name: 'Second'}), (c:Character {name: 'Shared'}), (a)-[:FEATURES]->(c), (b)-[:FEATURES]->(c)").unwrap();
    let first = id(&graph, "First");
    let second = id(&graph, "Second");
    let shared = id(&graph, "Shared");
    let original = capture(&mut graph);
    let version = graph.research_version(original).unwrap();
    let projected = Uuid::now_v7();
    let operation = ResearchOperation {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: graph
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid,
        mutation: ResearchMutation::RegisterGraphProjection {
            spec: RegisterResearchVersion {
                version_uuid: projected,
                context_uuid: version.context_uuid,
                source_generation_uuid: version.content.generation_uuid,
                selection: Some(
                    version
                        .content
                        .participants
                        .iter()
                        .map(|p| p.key.clone())
                        .collect(),
                ),
                source_version: Some(original),
                required_versions: BTreeSet::new(),
                label: None,
                description: None,
                created_at: 2,
                evidence: vec![],
            },
            selection: ResearchGraphSelection {
                nodes: BTreeSet::from([first, shared]),
                edges: BTreeSet::new(),
                induced_edges: true,
                exclude_properties: BTreeSet::new(),
            },
        },
    };
    // Scoped storage fixture; actual Branch creation is owned by #1352.
    publish_research_operation(
        &root,
        &operation,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .unwrap();
    drop(graph);
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    let mut request = request(SliceSelector::Direct {
        members: SliceMembers {
            nodes: BTreeSet::from([first, shared]),
            ..Default::default()
        },
    });
    request.source = SliceSource::Version {
        version_uuid: projected,
    };
    let bytes = ipc(&graph
        .freeze_slice(&request, &CancellationToken::new())
        .unwrap());
    assert!(
        rows(&graph, &request, SlicePageKind::Boundary)
            .contains(&("source_version".into(), original.to_string()))
    );
    let revision = SliceRevisionRequest {
        request_uuid: Uuid::now_v7(),
        include: SliceMembers {
            nodes: BTreeSet::from([second]),
            ..Default::default()
        },
        exclude: Default::default(),
        source_version: None,
    };
    assert!(matches!(
        graph.revise_frozen_slice(&bytes, &revision, &CancellationToken::new()),
        Err(GfError::Api {
            code: ApiErrorCode::ResultNotRetained,
            ..
        })
    ));
    assert!(
        graph
            .revise_frozen_slice(
                &bytes,
                &SliceRevisionRequest {
                    source_version: Some(original),
                    ..revision.clone()
                },
                &CancellationToken::new()
            )
            .is_ok()
    );
    for mutation in [
        ResearchMutation::Compact {
            versions: BTreeSet::from([projected]),
        },
        ResearchMutation::DeleteVersion {
            version_uuid: original,
        },
    ] {
        let operation = ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: graph
                .research_project_summary()
                .unwrap()
                .identity
                .generation_uuid,
            mutation,
        };
        graph
            .commit_research_version_operation(operation, &CancellationToken::new())
            .unwrap();
    }
    drop(graph);
    graphforge_storage::execute_project_cleanup(
        &root,
        graphforge_storage::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        graphforge_storage::ProjectRetentionLimits::default(),
    )
    .unwrap();
    let graph = GraphForge::new(root.to_str()).unwrap();
    assert_eq!(
        graph
            .inspect_frozen_slice(&bytes, SlicePageKind::Included, PageRequest::default())
            .unwrap()
            .batches[0]
            .num_rows(),
        2
    );
    assert_eq!(rows(&graph, &request, SlicePageKind::Included).len(), 2);
    assert!(matches!(
        graph.revise_frozen_slice(
            &bytes,
            &SliceRevisionRequest {
                source_version: Some(original),
                ..revision
            },
            &CancellationToken::new()
        ),
        Err(GfError::Api {
            code: ApiErrorCode::ResultNotRetained,
            ..
        })
    ));
    assert_eq!(id(&graph, "Second"), second); // live bytes exist but are never a fallback
}

#[test]
fn source_artifact_evidence_closure_stays_separate_from_membership() {
    let mut graph = fixture();
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
    let source = Uuid::now_v7();
    let artifact = Uuid::now_v7();
    let assertion = Uuid::now_v7();
    let shared = id(&graph, "Shared");
    graph
        .register_source(RegisterSourceRequest {
            context: context(),
            source_uuid: source,
            label: "Source".into(),
            source_kind: SourceKind::Manuscript,
            identity_uri: None,
        })
        .unwrap();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: context(),
            source_uuid: source,
            artifact_uuid: artifact,
            artifact_kind: ArtifactKind::RawScan,
            media_type: "application/octet-stream".into(),
            payload: ArtifactPayloadRequest::LocalBytes(b"selected evidence".to_vec()),
            derivation_inputs: vec![],
            run_uuid: None,
        })
        .unwrap();
    graph
        .create_assertion(CreateAssertionRequest {
            context: context(),
            assertion_uuid: assertion,
            claim: "Shared appears".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: shared,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    graph
        .attach_evidence(AttachEvidenceRequest {
            context: context(),
            evidence_uuid: Uuid::now_v7(),
            assertion_uuid: assertion,
            source_uuid: artifact,
            source_kind: EvidenceSourceKind::Artifact,
            role: EvidenceRole::Supports,
            weight: None,
        })
        .unwrap();
    let version = capture(&mut graph);
    let mut request = request(SliceSelector::Direct {
        members: SliceMembers {
            assertions: BTreeSet::from([assertion]),
            ..Default::default()
        },
    });
    request.source = SliceSource::Version {
        version_uuid: version,
    };
    assert_eq!(
        rows(&graph, &request, SlicePageKind::Included),
        vec![("assertion".into(), assertion.to_string())]
    );
    let dependencies = rows(&graph, &request, SlicePageKind::Dependencies);
    for (kind, id) in [("node", shared), ("artifact", artifact), ("source", source)] {
        assert!(dependencies.contains(&(kind.into(), id.to_string())));
    }
    let frozen = graph
        .freeze_slice(&request, &CancellationToken::new())
        .unwrap();
    let context: serde_json::Value =
        serde_json::from_str(&frozen.schema.metadata()["graphforge.slice.frozen_context"]).unwrap();
    assert_eq!(context["evidence"].as_array().unwrap().len(), 1);
    assert_eq!(
        context["evidence"][0]["artifact_uuid"],
        artifact.to_string()
    );
    assert_eq!(context["evidence"][0]["availability"], "local");
}

#[test]
fn frozen_cursors_bind_selector_and_final_ipc_limits_cover_metadata() {
    let mut graph = fixture();
    graph
        .execute("CREATE (:Character {name:'Another'})")
        .unwrap();
    let version = capture(&mut graph);
    let mut request = request(SliceSelector::Query {
        query: "MATCH (n:Character) RETURN n.node_uuid AS node_uuid".into(),
    });
    request.source = SliceSource::Version {
        version_uuid: version,
    };
    let original = ipc(&graph
        .freeze_slice(&request, &CancellationToken::new())
        .unwrap());
    let first = graph
        .inspect_frozen_slice(
            &original,
            SlicePageKind::Included,
            PageRequest {
                limit: 1,
                ..Default::default()
            },
        )
        .unwrap();
    let after = PageToken::parse(&first.schema.metadata()["graphforge.next_page_token"]).unwrap();
    request.selector = SliceSelector::Query {
        query: "MATCH (n:Character) RETURN n.node_uuid AS node_uuid ORDER BY node_uuid".into(),
    };
    let different = ipc(&graph
        .freeze_slice(&request, &CancellationToken::new())
        .unwrap());
    assert!(matches!(
        graph.inspect_frozen_slice(
            &different,
            SlicePageKind::Included,
            PageRequest {
                limit: 1,
                after: Some(after),
                ..Default::default()
            }
        ),
        Err(GfError::Api {
            code: ApiErrorCode::PageInvalid,
            ..
        })
    ));
    request.source = SliceSource::Current;
    request.limits.response_bytes = 1024;
    assert!(matches!(
        graph.preview_slice(
            &request,
            SlicePageKind::Included,
            PageRequest {
                limit: 1,
                ..Default::default()
            }
        ),
        Err(GfError::Api {
            code: ApiErrorCode::ResourceLimit,
            ..
        })
    ));
}

#[test]
fn in_memory_decision_input_composes_bounded_slice_and_explicit_query_without_retention() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute(
            "CREATE (m:Story {name:'Mystery', summary:'A locked room', private_note:'not selected'}), \
             (v:Story {name:'Voyage', summary:'A sea crossing', private_note:'not selected'}), \
             (c:Character {name:'Ada'}), (m)-[:FEATURES]->(c), (v)-[:FEATURES]->(c)",
        )
        .unwrap();

    let generation = graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid;
    let selection_request = request(SliceSelector::Query {
        query: "MATCH (s:Story {name:'Mystery'}) RETURN s.node_uuid AS node_uuid".into(),
    });
    let selected = graph
        .preview_slice(
            &selection_request,
            SlicePageKind::Included,
            PageRequest {
                limit: 1,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        selected.schema.metadata()["graphforge.slice.snapshot_uuid"],
        generation.to_string()
    );
    assert_eq!(selected.batches[0].num_rows(), 1);
    let selected_id = selected.batches[0]
        .column_by_name("object_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_owned();

    // The caller chooses the fields and reads only the selected candidate.
    // The query result retains Arrow UUID fidelity and carries no unrequested
    // private field into the external decision payload.
    let input = graph
        .execute(
            "MATCH (s:Story {name:'Mystery'}) \
             RETURN s.node_uuid AS item_uuid, s.name AS title, s.summary AS summary \
             ORDER BY s.node_uuid LIMIT 1",
        )
        .unwrap();
    assert_eq!(input.batches[0].num_rows(), 1);
    assert_eq!(
        input
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["item_uuid", "title", "summary"]
    );
    let item_uuid = input.batches[0]
        .column_by_name("item_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(
        Uuid::from_slice(item_uuid.value(0)).unwrap().to_string(),
        selected_id
    );
    let titles = input.batches[0]
        .column_by_name("title")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let summaries = input.batches[0]
        .column_by_name("summary")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(titles.value(0), "Mystery");
    assert_eq!(summaries.value(0), "A locked room");
    assert_eq!(
        graph
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid,
        generation
    );
    assert_eq!(
        graph.list_research_versions().unwrap().batches[0].num_rows(),
        0
    );
    assert!(input.side_effects.is_none());
    assert!(input.mutation_receipt.is_none());

    let boundary_request = request(SliceSelector::Traverse {
        seeds: BTreeSet::from([Uuid::from_str(&selected_id).unwrap()]),
        direction: SliceDirection::Both,
        max_depth: 1,
        relationship_types: BTreeSet::new(),
    });
    let outside = rows(&graph, &boundary_request, SlicePageKind::Boundary);
    assert!(outside.iter().any(|(kind, _)| kind == "node"));
    assert_eq!(
        graph
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid,
        generation
    );
}

#[test]
fn selected_branch_decision_context_reads_exact_version_after_parent_change_reopen_and_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute(
            "CREATE (m:Story {name:'Mystery', summary:'A locked room', private_note:'not selected'}), \
             (v:Story {name:'Voyage', summary:'A sea crossing', private_note:'not selected'}), \
             (c:Character {name:'Ada'}), (m)-[:FEATURES]->(c), (v)-[:FEATURES]->(c)",
        )
        .unwrap();
    let mystery = id(&graph, "Mystery");
    let source_version = capture(&mut graph);
    let mut selection = request(SliceSelector::Traverse {
        seeds: BTreeSet::from([mystery]),
        direction: SliceDirection::Both,
        max_depth: 1,
        relationship_types: BTreeSet::from(["FEATURES".into()]),
    });
    selection.source = SliceSource::Version {
        version_uuid: source_version,
    };
    let included = rows(&graph, &selection, SlicePageKind::Included);
    let boundary = rows(&graph, &selection, SlicePageKind::Boundary);
    let voyages = graph
        .execute("MATCH (s:Story {name:'Voyage'}) RETURN s.node_uuid AS node_uuid")
        .unwrap();
    let voyage = Uuid::from_slice(
        voyages.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    assert!(included.contains(&("node".into(), mystery.to_string())));
    assert!(!included.contains(&("node".into(), voyage.to_string())));
    assert!(boundary.contains(&("node".into(), voyage.to_string())));

    let frozen = graph
        .freeze_slice(&selection, &CancellationToken::new())
        .unwrap();
    let branch = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: graph
            .research_project_summary()
            .unwrap()
            .identity
            .generation_uuid,
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Slice {
            frozen_ipc: ipc(&frozen),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "Mystery decision context".into(),
    };
    graph
        .create_research_branch(&branch, &CancellationToken::new())
        .unwrap();
    let branch_version = graph
        .open_research_branch(branch.branch_uuid)
        .unwrap()
        .version_uuid();
    let mut decision_slice = request(SliceSelector::Query {
        query: "MATCH (s:Story) RETURN s.node_uuid AS node_uuid".into(),
    });
    decision_slice.source = SliceSource::Version {
        version_uuid: branch_version,
    };
    let before_parent_change = graph
        .preview_slice(
            &decision_slice,
            SlicePageKind::Included,
            PageRequest::default(),
        )
        .unwrap();
    assert_eq!(before_parent_change.batches[0].num_rows(), 1);
    assert_eq!(
        serde_json::from_str::<SliceSource>(
            &before_parent_change.schema.metadata()["graphforge.slice.source"]
        )
        .unwrap(),
        decision_slice.source
    );
    let expected_rows = rows(&graph, &decision_slice, SlicePageKind::Included);
    let historical = graph.open_research_version(branch_version).unwrap();
    let data = historical
        .execute(
            "MATCH (s:Story) RETURN s.node_uuid AS item_uuid, s.name AS title, s.summary AS summary \
             ORDER BY s.node_uuid LIMIT 10",
        )
        .unwrap();
    assert_eq!(data.batches[0].num_rows(), 1);
    assert_eq!(
        data.schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["item_uuid", "title", "summary"]
    );
    assert_eq!(
        data.batches[0]
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "Mystery"
    );
    drop(historical);

    // The exact Branch Version remains the source after unrelated parent edits.
    graph
        .execute("CREATE (:Story {name:'Parent only', summary:'outside selection'})")
        .unwrap();
    let after_parent_change = graph
        .preview_slice(
            &decision_slice,
            SlicePageKind::Included,
            PageRequest::default(),
        )
        .unwrap();
    assert_eq!(after_parent_change.batches[0].num_rows(), 1);
    assert_eq!(
        after_parent_change.schema.metadata()["graphforge.slice.snapshot_uuid"],
        before_parent_change.schema.metadata()["graphforge.slice.snapshot_uuid"]
    );
    assert_eq!(
        graph
            .open_research_version(branch_version)
            .unwrap()
            .execute("MATCH (s:Story) RETURN s.name ORDER BY s.name")
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );
    drop(graph);
    graphforge_storage::execute_project_cleanup(
        &root,
        graphforge_storage::ProjectRetentionPolicy {
            retained_ancestors: 0,
        },
        Default::default(),
    )
    .unwrap();
    let reopened = GraphForge::new(root.to_str()).unwrap();
    let retained = reopened.open_research_version(branch_version).unwrap();
    assert_eq!(
        rows(&reopened, &decision_slice, SlicePageKind::Included),
        expected_rows
    );
    assert_eq!(
        retained
            .execute("MATCH (s:Story) RETURN s.name ORDER BY s.name")
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );
}

#[test]
fn large_selection_has_bounded_pages_and_registered_native_contract() {
    let graph = GraphForge::new(None).unwrap();
    graph
        .execute("UNWIND range(0, 2047) AS i CREATE (:Character {number: i})")
        .unwrap();
    let mut request = request(SliceSelector::Query {
        query: "MATCH (n:Character) RETURN n.node_uuid AS node_uuid".into(),
    });
    request.limits.selected_objects = 1000;
    assert!(matches!(
        graph.preview_slice(&request, SlicePageKind::Included, PageRequest::default()),
        Err(GfError::Api {
            code: ApiErrorCode::ResourceLimit,
            ..
        })
    ));
    request.limits.selected_objects = 3000;
    request.limits.response_bytes = 4096;
    let page = graph
        .preview_slice(
            &request,
            SlicePageKind::Included,
            PageRequest {
                limit: 1,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(page.batches[0].num_rows(), 1);
    assert!(ipc(&page).len() <= 4096);
    let contract: serde_json::Value =
        serde_json::from_str(include_str!("../../../tests/contracts/slice-api-v1.json")).unwrap();
    let fields: Vec<_> = page
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        serde_json::to_value(fields).unwrap(),
        contract["row_fields"]
    );
    assert_eq!(
        serde_json::to_value(SliceLimits::default()).unwrap(),
        contract["limits"]
    );
    assert_eq!(
        page.schema.metadata()["graphforge.slice.membership_is_retention"],
        "false"
    );
    request.selector = SliceSelector::Query {
        query: "MATCH (n) RETURN private_sentinel".into(),
    };
    let error = graph
        .preview_slice(&request, SlicePageKind::Included, PageRequest::default())
        .unwrap_err()
        .to_string();
    assert!(!error.contains("private_sentinel"));
}
