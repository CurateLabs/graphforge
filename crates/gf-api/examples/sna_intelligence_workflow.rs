//! Executable release workflow for issue #2465.
//!
//! This is intentionally an opt-in example target.  The bundle runner in
//! `tests/release_workflows/sna-intelligence/` invokes it on a release-candidate
//! machine; it is not wired into the required PR test matrix.

use std::collections::{BTreeSet, HashMap};
use std::fs;

use arrow::array::{Array, FixedSizeBinaryArray, StringArray};
use gf_api::{
    AdoptOntologyRequest, AnalyzeOptions, AssertionGraphRefInput, AssertionGraphRole,
    AssessConfidenceRequest, AttachEvidenceRequest, CapabilityId, CheckpointRequest,
    ClusterOptions, ConfidencePolicyRequest, CreateAssertionRequest, CreateHypothesisGroupRequest,
    EnableCapabilityRequest, EvidenceRole, EvidenceSourceKind, FindOptions, GraphForge,
    GraphObjectKind, HypothesisMembershipAction, ListAlgorithmRunsRequest,
    ListAssertionSupersessionsRequest, ListAssertionsRequest, ListHypothesisMembershipRequest,
    ListReasoningRequest, NodeSelector, OntologyMode, OperationId, PageRequest, PathAlgorithm,
    PathsOptions, PropValue, RankAlgorithm, RankOptions, ReasoningContentFormat, ReasoningKind,
    RecordHypothesisMembershipRequest, RecordHypothesisSelectionRequest, RecordReasoningRequest,
    RecordedAlgorithmRequest, SearchIndexOptions, SupersedeAssertionRequest, WriteContext,
};
use serde_json::json;
use tempfile::TempDir;
use uuid::Uuid;

fn uuid7(seed: u8) -> Uuid {
    let mut bytes = [seed; 16];
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn context(seed: u8) -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(uuid7(seed)),
        actor_uuid: Some(uuid7(250)),
    }
}

fn enable(graph: &GraphForge, capability_id: CapabilityId, seed: u8) {
    graph
        .enable_capability(EnableCapabilityRequest {
            context: context(seed),
            capability_id,
            capability_version: 1,
        })
        .unwrap();
}

fn props(name: &str, summary: &str) -> HashMap<String, PropValue> {
    HashMap::from([
        ("name".into(), PropValue::Str(name.into())),
        ("summary".into(), PropValue::Str(summary.into())),
    ])
}

fn uuids(batch: &arrow::record_batch::RecordBatch, column: &str) -> BTreeSet<Uuid> {
    let values = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("missing UUID column {column}"))
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap_or_else(|| panic!("{column} is not fixed-size binary"));
    (0..values.len())
        .filter(|row| !values.is_null(*row))
        .map(|row| Uuid::from_slice(values.value(row)).unwrap())
        .collect()
}

fn provenance_uuid(result: &gf_api::ExecutionResult) -> Uuid {
    let values = result.batches[0]
        .column_by_name("provenance_uuid")
        .expect("assertion provenance")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("provenance UUID array");
    Uuid::from_slice(values.value(0)).unwrap()
}

fn assert_batch_data_equal(
    left: &arrow::record_batch::RecordBatch,
    right: &arrow::record_batch::RecordBatch,
) {
    assert_eq!(left.schema().fields(), right.schema().fields());
    assert_eq!(left.num_rows(), right.num_rows());
    assert_eq!(left.num_columns(), right.num_columns());
    for (left_column, right_column) in left.columns().iter().zip(right.columns()) {
        assert_eq!(left_column.to_data(), right_column.to_data());
    }
}

fn assertion(
    graph: &GraphForge,
    seed: u8,
    claim: &str,
    graph_uuid: Uuid,
    graph_kind: GraphObjectKind,
) -> (Uuid, Uuid) {
    let assertion_uuid = uuid7(seed);
    let result = graph
        .create_assertion(CreateAssertionRequest {
            context: context(seed + 80),
            assertion_uuid,
            claim: claim.into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid,
                graph_kind,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    (assertion_uuid, provenance_uuid(&result))
}

fn reasoning(
    graph: &GraphForge,
    seed: u8,
    assertion_uuid: Uuid,
    provenance_uuid: Uuid,
    content: &str,
) -> Uuid {
    let reasoning_uuid = uuid7(seed);
    graph
        .record_reasoning(RecordReasoningRequest {
            context: context(seed + 80),
            reasoning_uuid,
            assertion_uuid,
            kind: ReasoningKind::EvidenceInterpretation,
            content_format: ReasoningContentFormat::TextPlain,
            content: content.as_bytes().to_vec(),
            supersedes_reasoning_uuid: None,
            provenance_uuid,
        })
        .unwrap();
    reasoning_uuid
}

#[allow(
    clippy::too_many_lines,
    reason = "one release workflow is one auditable story"
)]
fn main() {
    let project = TempDir::new().unwrap();
    let imports = TempDir::new().unwrap();
    let ontology_path = imports.path().join("partial-advisory.yaml");
    fs::write(
        &ontology_path,
        include_str!(
            "../../../tests/release_workflows/sna-intelligence/ontologies/phase-2-advisory.yaml"
        ),
    )
    .unwrap();

    let mut graph = GraphForge::new(project.path().to_str()).unwrap();
    enable(&graph, CapabilityId::Provenance, 1);
    enable(&graph, CapabilityId::Knowledge, 2);
    enable(&graph, CapabilityId::Epistemic, 3);

    // Phase 1: two sparse components arrive in separate public construction batches.
    let ada = graph
        .add_node("Actor", &props("Ada", "regional organizer"))
        .unwrap();
    let ben = graph
        .add_node("Actor", &props("Ben", "operations liaison"))
        .unwrap();
    let cy = graph
        .add_node("Actor", &props("Cy", "event coordinator"))
        .unwrap();
    let ada_ben = graph
        .add_edge(&ada, "COMMUNICATED", &ben, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&ben, "COMMUNICATED", &cy, &HashMap::new())
        .unwrap();

    let dana = graph
        .add_node("Actor", &props("Dana", "external contact"))
        .unwrap();
    let eli = graph
        .add_node("Actor", &props("Eli", "independent observer"))
        .unwrap();
    graph
        .add_edge(&dana, "COMMUNICATED", &eli, &HashMap::new())
        .unwrap();
    let event = graph
        .add_node("UnclassifiedEvent", &props("E-17", "shared venue"))
        .unwrap();
    graph
        .add_edge(&cy, "ATTENDED", &event, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&dana, "ATTENDED", &event, &HashMap::new())
        .unwrap();
    // Construction writes relationship storage directly; querying each observed
    // type records it in the exploratory RuntimeCatalog presented to analysts.
    assert_eq!(
        graph
            .execute("MATCH ()-[r:COMMUNICATED]->() RETURN r.edge_uuid AS edge_uuid")
            .unwrap()
            .stats
            .rows_produced,
        3
    );
    assert_eq!(
        graph
            .execute("MATCH ()-[r:ATTENDED]->() RETURN r.edge_uuid AS edge_uuid")
            .unwrap()
            .stats
            .rows_produced,
        2
    );

    let phase_one_catalog = graph.runtime_catalog();
    let phase_one_guard = phase_one_catalog.lock().unwrap();
    let phase_one_types = phase_one_guard.entity_types().len();
    let phase_one_relations = phase_one_guard.relation_types().len();
    let phase_one_properties = phase_one_guard.property_names().count();
    assert!(phase_one_guard.contains_entity_type("UnclassifiedEvent"));
    drop(phase_one_guard);

    // Query + text search bound the investigation before structural analysis.
    let scope = graph
        .execute("MATCH (a:Actor) RETURN a.node_uuid AS node_uuid, a.name AS name ORDER BY name")
        .unwrap();
    assert_eq!(scope.stats.rows_produced, 5);
    graph
        .index_search(
            "Actor",
            SearchIndexOptions::Text {
                properties: Some(vec!["name".into(), "summary".into()]),
                rebuild: false,
            },
        )
        .unwrap();
    let search = graph
        .find(FindOptions {
            query: Some("organizer".into()),
            label: Some("Actor".into()),
            limit: 5,
            ..FindOptions::default()
        })
        .unwrap();
    assert_eq!(uuids(&search, "node_uuid"), BTreeSet::from([ada.uuid]));

    let rank_options = RankOptions {
        by: RankAlgorithm::Degree,
        via: Some("COMMUNICATED".into()),
        directed: false,
        write_property: None,
    };
    let cluster_options = ClusterOptions {
        via: Some("COMMUNICATED".into()),
        directed: false,
        ..ClusterOptions::default()
    };
    let rank_before = graph.rank("Actor", rank_options.clone()).unwrap();
    let run_uuid = uuid7(9);
    let recorded = graph
        .invoke_recorded(RecordedAlgorithmRequest {
            context: context(89),
            run_uuid,
            descriptor: graph
                .prepare_rank_invocation("Actor", &rank_options)
                .unwrap(),
            cancellation: None,
        })
        .unwrap();
    assert_eq!(recorded.run_uuid, run_uuid);
    assert_eq!(
        graph
            .algorithm_run_events(run_uuid, PageRequest::default())
            .unwrap()
            .batches[0]
            .num_rows(),
        2
    );
    let clusters_before = graph.cluster("Actor", cluster_options.clone()).unwrap();
    assert_eq!(
        uuids(&rank_before, "node_uuid"),
        uuids(&clusters_before, "node_uuid")
    );
    assert_eq!(uuids(&rank_before, "node_uuid").len(), 5);
    let path_before = graph
        .paths(
            Some(&NodeSelector::Handle(ada.clone())),
            Some(&NodeSelector::Handle(cy.clone())),
            PathsOptions {
                by: PathAlgorithm::Bfs,
                via: Some("COMMUNICATED".into()),
                directed: false,
                ..PathsOptions::default()
            },
        )
        .unwrap();
    assert_eq!(path_before.num_rows(), 1);
    let dag_before = graph
        .analyze(Some("Actor"), AnalyzeOptions::default())
        .unwrap();
    assert_eq!(dag_before.num_rows(), 1);

    // Phase 2: a mistaken cross-component classification grows the catalog.
    let mistaken = graph
        .add_edge(&cy, "DIRECTS", &dana, &HashMap::new())
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH ()-[r:DIRECTS]->() RETURN r.edge_uuid AS edge_uuid")
            .unwrap()
            .stats
            .rows_produced,
        1
    );
    let phase_two_catalog = graph.runtime_catalog();
    let phase_two_guard = phase_two_catalog.lock().unwrap();
    assert_eq!(phase_two_guard.entity_types().len(), phase_one_types);
    assert_eq!(
        phase_two_guard.relation_types().len(),
        phase_one_relations + 1
    );
    let phase_two_properties = phase_two_guard.property_names().count();
    assert!(
        phase_two_properties > phase_one_properties,
        "observing the new relationship and its UUID must expose catalog drift"
    );
    drop(phase_two_guard);
    // A later source batch supplies a neutral association.  The epistemic
    // correction below determines which interpretation is current without
    // rewriting either imported graph record.
    let corrected = graph
        .add_edge(&cy, "ASSOCIATED_WITH", &dana, &HashMap::new())
        .unwrap();

    // The partial ontology is advisory: declared vocabulary is recognized while
    // deliberate unknown Event/ATTENDED/DIRECTS vocabulary remains observable.
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: context(4),
            path: ontology_path,
            mode: OntologyMode::Advisory,
        })
        .unwrap();
    assert_eq!(graph.ontology_mode(), OntologyMode::Advisory);
    let catalog = graph.runtime_catalog();
    let guard = catalog.lock().unwrap();
    assert!(guard.contains_entity_type("UnclassifiedEvent"));
    assert!(guard.relation_types().contains(&"DIRECTS"));
    assert!(guard.relation_types().contains(&"ATTENDED"));
    drop(guard);

    // Three explanations are recorded without treating structural signal as attribution.
    let (influence, influence_provenance) = assertion(
        &graph,
        10,
        "DIRECTS may indicate influence, but is not attribution",
        mistaken.uuid,
        GraphObjectKind::Edge,
    );
    let influence_reasoning = reasoning(
        &graph,
        20,
        influence,
        influence_provenance,
        "Brokerage and cross-component reach support investigation, not a conclusion.",
    );
    graph
        .attach_evidence(AttachEvidenceRequest {
            context: context(105),
            evidence_uuid: uuid7(25),
            assertion_uuid: influence,
            source_uuid: ada_ben.uuid,
            source_kind: EvidenceSourceKind::GraphEdge,
            role: EvidenceRole::Supports,
            weight: Some(0.65),
        })
        .unwrap();
    graph
        .assess_confidence(AssessConfidenceRequest {
            context: context(106),
            confidence_uuid: uuid7(26),
            assertion_uuid: influence,
            policy: ConfidencePolicyRequest::Explicit { value: 0.8 },
        })
        .unwrap();

    let (coordination, coordination_provenance) = assertion(
        &graph,
        11,
        "Shared timing may indicate coordination",
        event.uuid,
        GraphObjectKind::Node,
    );
    let coordination_reasoning = reasoning(
        &graph,
        21,
        coordination,
        coordination_provenance,
        "Co-attendance and communication timing are compatible with coordination.",
    );
    let (coincidence, coincidence_provenance) = assertion(
        &graph,
        12,
        "Shared venue may be coincidental",
        event.uuid,
        GraphObjectKind::Node,
    );
    let coincidence_reasoning = reasoning(
        &graph,
        22,
        coincidence,
        coincidence_provenance,
        "A public venue provides a non-coordination explanation.",
    );

    let group_uuid = uuid7(30);
    graph
        .create_hypothesis_group(CreateHypothesisGroupRequest {
            context: context(110),
            group_uuid,
            question_key: "sna.e17.relationship-explanation.v1".into(),
            provenance_uuid: influence_provenance,
        })
        .unwrap();
    for (seed, assertion_uuid, reasoning_uuid, provenance_uuid) in [
        (31, influence, influence_reasoning, influence_provenance),
        (
            32,
            coordination,
            coordination_reasoning,
            coordination_provenance,
        ),
        (
            33,
            coincidence,
            coincidence_reasoning,
            coincidence_provenance,
        ),
    ] {
        graph
            .record_hypothesis_membership(&RecordHypothesisMembershipRequest {
                context: context(seed + 80),
                membership_event_uuid: uuid7(seed),
                group_uuid,
                assertion_uuid,
                action: HypothesisMembershipAction::Added,
                reasoning_uuid,
                provenance_uuid,
            })
            .unwrap();
    }
    assert_eq!(
        graph.hypothesis_members(group_uuid).unwrap().batches[0].num_rows(),
        3
    );
    assert_eq!(
        graph.hypothesis_selection(group_uuid).unwrap().batches[0].num_rows(),
        0,
        "confidence must not implicitly select a hypothesis"
    );

    graph
        .checkpoint(CheckpointRequest {
            name: "before-correction".into(),
            description: Some(
                "mistaken DIRECTS interpretation retained for transaction-time reconstruction"
                    .into(),
            ),
            idempotency_key: OperationId(uuid7(40)),
            actor_uuid: Some(uuid7(250)),
        })
        .unwrap();

    // Phase 3: public compensating graph and epistemic operations preserve history.
    let (corrected_assertion, corrected_provenance) = assertion(
        &graph,
        13,
        "Cy and Dana are associated; the relationship direction is unresolved",
        corrected.uuid,
        GraphObjectKind::Edge,
    );
    let corrected_reasoning = reasoning(
        &graph,
        23,
        corrected_assertion,
        corrected_provenance,
        "Source correction removes the unsupported directionality claim.",
    );
    graph
        .supersede_assertion(SupersedeAssertionRequest {
            context: context(120),
            supersession_uuid: uuid7(41),
            prior_assertion_uuid: influence,
            replacement_assertion_uuid: corrected_assertion,
            status_event_uuid: uuid7(42),
            reasoning_uuid: influence_reasoning,
            provenance_uuid: influence_provenance,
        })
        .unwrap();
    graph
        .record_hypothesis_membership(&RecordHypothesisMembershipRequest {
            context: context(121),
            membership_event_uuid: uuid7(43),
            group_uuid,
            assertion_uuid: corrected_assertion,
            action: HypothesisMembershipAction::Added,
            reasoning_uuid: corrected_reasoning,
            provenance_uuid: corrected_provenance,
        })
        .unwrap();
    graph
        .record_hypothesis_selection(&RecordHypothesisSelectionRequest {
            context: context(122),
            selection_event_uuid: uuid7(44),
            group_uuid,
            selected_assertion_uuid: Some(coordination),
            reasoning_uuid: coordination_reasoning,
            provenance_uuid: coordination_provenance,
        })
        .unwrap();

    let before = graph.open_checkpoint("before-correction").unwrap();
    assert_eq!(
        before.hypothesis_members(group_uuid).unwrap().batches[0].num_rows(),
        3
    );
    assert_eq!(
        before.hypothesis_selection(group_uuid).unwrap().batches[0].num_rows(),
        0
    );
    assert_eq!(
        before
            .list_assertion_supersessions(ListAssertionSupersessionsRequest::default())
            .unwrap()
            .batches[0]
            .num_rows(),
        0
    );
    assert_eq!(
        before
            .list_algorithm_runs(ListAlgorithmRunsRequest::default())
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );
    assert_eq!(
        before
            .algorithm_run_events(run_uuid, PageRequest::default())
            .unwrap()
            .batches[0]
            .num_rows(),
        2
    );
    assert_eq!(
        graph
            .list_assertion_supersessions(ListAssertionSupersessionsRequest::default())
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );
    assert_eq!(
        graph
            .list_assertions(ListAssertionsRequest::default())
            .unwrap()
            .batches[0]
            .num_rows(),
        4
    );
    assert_eq!(
        graph
            .list_reasoning(ListReasoningRequest::default())
            .unwrap()
            .batches[0]
            .num_rows(),
        4
    );
    assert_eq!(
        graph
            .list_hypothesis_membership(&ListHypothesisMembershipRequest {
                group_uuid: Some(group_uuid),
                assertion_uuid: None,
                page: PageRequest::default()
            })
            .unwrap()
            .batches[0]
            .num_rows(),
        4
    );

    let final_scope = graph
        .execute("MATCH (a:Actor) RETURN a.node_uuid AS node_uuid ORDER BY node_uuid")
        .unwrap();
    let final_search = graph
        .find(FindOptions {
            query: Some("organizer".into()),
            label: Some("Actor".into()),
            limit: 5,
            ..FindOptions::default()
        })
        .unwrap();
    let final_selection = graph.hypothesis_selection(group_uuid).unwrap();
    let selected = final_selection.batches[0]
        .column_by_name("selected_assertion_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(Uuid::from_slice(selected.value(0)).unwrap(), coordination);
    assert_eq!(
        uuids(&rank_before, "node_uuid"),
        uuids(&final_scope.batches[0], "node_uuid"),
        "pre-ontology algorithm UUIDs must compose with the corrected advisory view"
    );
    assert_eq!(
        uuids(&clusters_before, "node_uuid"),
        uuids(&final_scope.batches[0], "node_uuid")
    );

    drop(before);
    drop(graph);
    let reopened = GraphForge::new(project.path().to_str()).unwrap();
    assert_eq!(reopened.ontology_mode(), OntologyMode::Advisory);
    let reopened_scope = reopened
        .execute("MATCH (a:Actor) RETURN a.node_uuid AS node_uuid ORDER BY node_uuid")
        .unwrap();
    assert_batch_data_equal(&reopened_scope.batches[0], &final_scope.batches[0]);
    assert_eq!(
        reopened
            .find(FindOptions {
                query: Some("organizer".into()),
                label: Some("Actor".into()),
                limit: 5,
                ..FindOptions::default()
            })
            .unwrap(),
        final_search
    );
    assert_eq!(
        reopened.hypothesis_selection(group_uuid).unwrap().batches,
        final_selection.batches
    );
    assert_eq!(
        reopened
            .open_checkpoint("before-correction")
            .unwrap()
            .hypothesis_selection(group_uuid)
            .unwrap()
            .batches[0]
            .num_rows(),
        0
    );
    assert_eq!(
        reopened
            .list_algorithm_runs(ListAlgorithmRunsRequest::default())
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );

    if let Ok(path) = std::env::var("GRAPHFORGE_WORKFLOW_EVIDENCE") {
        let commit_sha = std::env::var("GRAPHFORGE_WORKFLOW_SHA").expect("runner supplies SHA");
        let names = scope.batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ordered_names = (0..names.len())
            .map(|row| names.value(row))
            .collect::<Vec<_>>();
        let evidence = json!({
            "contract_version": 1,
            "scenario_id": "sna-intelligence",
            "commit_sha": commit_sha,
            "seed": 2465,
            "outcome": "association corrected; coordination remains an explicit working interpretation, not objective attribution",
            "ordered_scope": ordered_names,
            "stable_node_uuids": uuids(&rank_before, "node_uuid").iter().map(Uuid::to_string).collect::<Vec<_>>(),
            "correction": {"preserved_source_edge_uuid": mistaken.uuid, "compensating_edge_uuid": corrected.uuid, "supersession_uuid": uuid7(41)},
            "hypotheses": {"visible": 4, "selected_assertion_uuid": coordination, "confidence_selected_implicitly": false},
            "history": {"before_selection_rows": 0, "after_selection_rows": 1, "algorithm_runs": 1, "algorithm_run_events": 2, "assertions": 4, "reasoning_records": 4, "membership_events": 4, "supersessions": 1},
            "ontology": {"mode": "advisory", "known_entity_types": 1, "known_relation_types": 1, "deliberate_unknown_entity_types": 1, "deliberate_unknown_relation_types": 3},
            "reopen_equal": true
        });
        fs::write(path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    }
}
