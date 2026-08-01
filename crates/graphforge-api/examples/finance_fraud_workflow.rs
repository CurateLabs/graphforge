//! Opt-in executable release evidence for issue #2468.
//!
//! This example is invoked only by its bundle-local runner. It is not an
//! ordinary integration test or part of the required aggregate CI gate.

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray};
use graphforge_api::{
    AdoptOntologyRequest, AssertionGraphRefInput, AssertionGraphRole, AssessConfidenceRequest,
    AttachEvidenceRequest, CapabilityId, CheckpointRequest, ClusterAlgorithm, ClusterOptions,
    ConfidencePolicyRequest, CreateAssertionRequest, CreateHypothesisGroupRequest,
    EnableCapabilityRequest, EvidenceRole, EvidenceSourceKind, FindOptions, GraphForge,
    GraphObjectKind, HypothesisMembershipAction, IrLiteral, ListAlgorithmRunsRequest,
    ListHypothesisSelectionRequest, NodeHandle, NodeSelector, OntologyMode, OperationId,
    PageRequest, PathAlgorithm, PathsOptions, PropValue, RankAlgorithm, RankOptions,
    ReasoningContentFormat, ReasoningKind, RecordHypothesisMembershipRequest,
    RecordHypothesisSelectionRequest, RecordReasoningRequest, RecordedAlgorithmRequest,
    RevertCheckpointRequest, SearchIndexOptions, SimilarAlgorithm, SimilarOptions, WriteContext,
};
use graphforge_ontology::{OntologyCompiler, OntologyLoader};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

const BUNDLE: &str = "../../tests/release_workflows/finance-fraud";

fn fixture(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(BUNDLE)
        .join(relative)
}

fn id(seed: u16) -> Uuid {
    Uuid::parse_str(&format!("018f0f4e-7b8c-7000-8000-00000004{seed:04x}")).unwrap()
}

fn context(seed: u16) -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(id(seed)),
        actor_uuid: Some(id(0xff00)),
    }
}

fn props(values: &[(&str, PropValue)]) -> HashMap<String, PropValue> {
    values
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect()
}

fn add_edge_by_key(
    graph: &GraphForge,
    src_label: &str,
    src_key: &str,
    src_value: &str,
    rel_type: &str,
    dst_label: &str,
    dst_key: &str,
    dst_value: &str,
) {
    let params = HashMap::from([
        ("src_value".to_owned(), IrLiteral::Str(src_value.to_owned())),
        ("dst_value".to_owned(), IrLiteral::Str(dst_value.to_owned())),
    ]);
    graph
        .execute_with_params(
            &format!(
                "MATCH (src:`{src_label}` {{`{src_key}`: $src_value}}), \
                 (dst:`{dst_label}` {{`{dst_key}`: $dst_value}}) \
                 CREATE (src)-[:`{rel_type}`]->(dst)"
            ),
            &params,
        )
        .unwrap();
}

fn enable(graph: &GraphForge, capability_id: CapabilityId, seed: u16) {
    graph
        .enable_capability(EnableCapabilityRequest {
            context: context(seed),
            capability_id,
            capability_version: 1,
        })
        .unwrap();
}

fn provenance(result: &graphforge_api::ExecutionResult) -> Uuid {
    let values = result.batches[0]
        .column_by_name("provenance_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    Uuid::from_slice(values.value(0)).unwrap()
}

fn assertion(
    graph: &GraphForge,
    seed: u16,
    claim: &str,
    subject: &NodeHandle,
) -> (Uuid, Uuid, Uuid) {
    let assertion_uuid = id(seed);
    let result = graph
        .create_assertion(CreateAssertionRequest {
            context: context(seed + 1),
            assertion_uuid,
            claim: claim.to_owned(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: subject.uuid,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    let provenance_uuid = provenance(&result);
    graph
        .attach_evidence(AttachEvidenceRequest {
            context: context(seed + 2),
            evidence_uuid: id(seed + 3),
            assertion_uuid,
            source_uuid: subject.uuid,
            source_kind: EvidenceSourceKind::GraphNode,
            role: EvidenceRole::Supports,
            weight: Some(0.6),
        })
        .unwrap();
    graph
        .assess_confidence(AssessConfidenceRequest {
            context: context(seed + 4),
            confidence_uuid: id(seed + 5),
            assertion_uuid,
            policy: ConfidencePolicyRequest::Explicit { value: 0.6 },
        })
        .unwrap();
    let reasoning_uuid = id(seed + 6);
    graph
        .record_reasoning(RecordReasoningRequest {
            context: context(seed + 7),
            reasoning_uuid,
            assertion_uuid,
            kind: ReasoningKind::EvidenceInterpretation,
            content_format: ReasoningContentFormat::TextPlain,
            content: claim.as_bytes().to_vec(),
            supersedes_reasoning_uuid: None,
            provenance_uuid,
        })
        .unwrap();
    (assertion_uuid, reasoning_uuid, provenance_uuid)
}

fn rank_options() -> RankOptions {
    RankOptions {
        by: RankAlgorithm::Degree,
        via: Some("TRANSFERRED_TO".into()),
        directed: true,
        write_property: None,
    }
}

fn fingerprint(batch: &arrow::record_batch::RecordBatch) -> String {
    let value = format!("{:?}|{batch:?}", batch.schema());
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn uuid_set(batch: &arrow::record_batch::RecordBatch, column: &str) -> BTreeSet<Uuid> {
    let values = batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    (0..values.len())
        .filter(|row| !values.is_null(*row))
        .map(|row| Uuid::from_slice(values.value(row)).unwrap())
        .collect()
}

#[allow(clippy::too_many_lines, reason = "one auditable release workflow")]
fn main() {
    let project = TempDir::new().unwrap();
    let advisory_path = fixture("ontologies/advisory-v1.yaml");
    let strict_path = fixture("ontologies/strict-v2.yaml");
    let strict_doc = OntologyLoader::load_file(&strict_path).unwrap();
    let strict_runtime = OntologyCompiler::compile(&strict_doc).unwrap();
    assert_eq!(strict_runtime.entity_types.num_rows(), 9);
    assert_eq!(strict_runtime.relation_types.num_rows(), 9);
    assert_eq!(strict_runtime.property_types.num_rows(), 18);
    assert_eq!(strict_runtime.type_constraints.num_rows(), 3);

    let mut graph = GraphForge::new(project.path().to_str()).unwrap();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: context(1),
            path: advisory_path,
            mode: OntologyMode::Advisory,
        })
        .unwrap();
    enable(&graph, CapabilityId::Provenance, 2);
    enable(&graph, CapabilityId::Knowledge, 3);
    enable(&graph, CapabilityId::Epistemic, 4);

    let parties = (0..4)
        .map(|index| {
            graph
                .add_node(
                    "Party",
                    &props(&[
                        ("name", PropValue::Str(format!("Synthetic Party {index}"))),
                        ("party_key", PropValue::Str(format!("P-{index:02}"))),
                        (
                            "risk_note",
                            PropValue::Str(if index == 0 {
                                "review repeated transfers".into()
                            } else {
                                "no determination".into()
                            }),
                        ),
                    ]),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let accounts = (0..5)
        .map(|index| {
            graph
                .add_node(
                    "Account",
                    &props(&[
                        ("name", PropValue::Str(format!("Review Account {index}"))),
                        ("account_key", PropValue::Str(format!("A-{index:02}"))),
                        ("currency", PropValue::Str("USD".into())),
                        ("balance", PropValue::Float(10_000.0 + index as f64)),
                    ]),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    for (party, account) in parties.iter().zip(accounts.iter()) {
        graph
            .add_edge(party, "OWNS", account, &HashMap::new())
            .unwrap();
    }
    let device = graph
        .add_node(
            "Device",
            &props(&[
                ("name", PropValue::Str("Shared Device".into())),
                ("device_key", PropValue::Str("D-01".into())),
                (
                    "fingerprint",
                    PropValue::Str("synthetic-fingerprint".into()),
                ),
            ]),
        )
        .unwrap();
    graph
        .add_edge(&parties[0], "USED_DEVICE", &device, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&parties[1], "USED_DEVICE", &device, &HashMap::new())
        .unwrap();
    let merchant = graph
        .add_node(
            "Merchant",
            &props(&[
                ("name", PropValue::Str("Synthetic Market".into())),
                ("merchant_key", PropValue::Str("M-01".into())),
                ("category", PropValue::Str("retail".into())),
            ]),
        )
        .unwrap();
    graph
        .add_edge(&accounts[4], "PAID_MERCHANT", &merchant, &HashMap::new())
        .unwrap();

    let transfer_pairs = [
        (0, 1),
        (0, 1),
        (1, 2),
        (2, 0),
        (2, 3),
        (3, 4),
        (4, 2),
        (1, 4),
    ];
    let mut transactions = Vec::new();
    for (index, (source, target)) in transfer_pairs.into_iter().enumerate() {
        let key = format!("T-{index:02}");
        let amount = 100.0 + index as f64 * 25.0;
        let transaction = graph
            .add_node(
                "Transaction",
                &props(&[
                    ("name", PropValue::Str(format!("Transfer {key}"))),
                    ("transaction_key", PropValue::Str(key.clone())),
                    ("amount", PropValue::Float(amount)),
                    ("reviewed", PropValue::Bool(false)),
                ]),
            )
            .unwrap();
        graph
            .add_edge(
                &transaction,
                "FROM_ACCOUNT",
                &accounts[source],
                &HashMap::new(),
            )
            .unwrap();
        graph
            .add_edge(
                &transaction,
                "TO_ACCOUNT",
                &accounts[target],
                &HashMap::new(),
            )
            .unwrap();
        graph
            .add_edge(
                &accounts[source],
                "TRANSFERRED_TO",
                &accounts[target],
                &props(&[
                    ("amount", PropValue::Float(amount)),
                    ("transaction_key", PropValue::Str(key)),
                ]),
            )
            .unwrap();
        transactions.push(transaction);
    }

    graph
        .index_search(
            "Account",
            SearchIndexOptions::Text {
                properties: Some(vec!["name".into(), "account_key".into()]),
                rebuild: false,
            },
        )
        .unwrap();
    let search = graph
        .find(FindOptions {
            query: Some("Review Account".into()),
            label: Some("Account".into()),
            limit: 5,
            ..FindOptions::default()
        })
        .unwrap();
    let invalid_scope = graph.find(FindOptions::default()).unwrap_err();
    assert_eq!(invalid_scope.code(), "GF_VALIDATION");
    assert_eq!(search.num_rows(), 5);
    let rank = graph.rank("Account", rank_options()).unwrap();
    let cluster = graph
        .cluster(
            "Account",
            ClusterOptions {
                by: ClusterAlgorithm::Components,
                via: Some("TRANSFERRED_TO".into()),
                directed: false,
                ..ClusterOptions::default()
            },
        )
        .unwrap();
    let path = graph
        .paths(
            Some(&NodeSelector::Handle(accounts[0].clone())),
            Some(&NodeSelector::Handle(accounts[4].clone())),
            PathsOptions {
                by: PathAlgorithm::Bfs,
                via: Some("TRANSFERRED_TO".into()),
                directed: true,
                ..PathsOptions::default()
            },
        )
        .unwrap();
    let similar = graph
        .similar(
            "Account",
            SimilarOptions {
                by: SimilarAlgorithm::NodeSimilarity,
                k: 4,
                vector_property: None,
                via: Some("TRANSFERRED_TO".into()),
            },
        )
        .unwrap();
    assert_eq!(rank.num_rows(), 5);
    assert_eq!(cluster.num_rows(), 5);
    assert_eq!(path.num_rows(), 1);
    assert!(similar.num_rows() > 0);
    let run_uuid = id(0x80);
    graph
        .invoke_recorded(RecordedAlgorithmRequest {
            context: context(0x81),
            run_uuid,
            descriptor: graph
                .prepare_rank_invocation("Account", &rank_options())
                .unwrap(),
            cancellation: None,
        })
        .unwrap();
    assert_eq!(
        graph
            .algorithm_run_events(run_uuid, PageRequest::default())
            .unwrap()
            .batches[0]
            .num_rows(),
        2
    );

    let fraud = assertion(
        &graph,
        0x100,
        "The cyclic transfers are compatible with coordinated fraudulent activity",
        &transactions[0],
    );
    let legitimate = assertion(
        &graph,
        0x120,
        "The transfers may reflect legitimate treasury and merchant activity",
        &transactions[1],
    );
    let group_uuid = id(0x180);
    graph
        .create_hypothesis_group(CreateHypothesisGroupRequest {
            context: context(0x181),
            group_uuid,
            question_key: "finance.synthetic.transfer-interpretation.v1".into(),
            provenance_uuid: fraud.2,
        })
        .unwrap();
    for (seed, item) in [(0x190, fraud), (0x1a0, legitimate)] {
        graph
            .record_hypothesis_membership(&RecordHypothesisMembershipRequest {
                context: context(seed),
                membership_event_uuid: id(seed + 1),
                group_uuid,
                assertion_uuid: item.0,
                action: HypothesisMembershipAction::Added,
                reasoning_uuid: item.1,
                provenance_uuid: item.2,
            })
            .unwrap();
    }
    assert_eq!(
        graph.hypothesis_selection(group_uuid).unwrap().batches[0].num_rows(),
        0
    );
    graph
        .checkpoint(CheckpointRequest {
            name: "initial-analysis".into(),
            description: Some("initial graph, run, and unresolved hypotheses".into()),
            idempotency_key: OperationId(id(0x200)),
            actor_uuid: Some(id(0xff00)),
        })
        .unwrap();

    graph
        .add_edge(&parties[0], "SAME_AS", &parties[1], &HashMap::new())
        .unwrap();
    graph
        .add_edge(
            &parties[0],
            "SHARED_BROWSER_SESSION",
            &parties[1],
            &HashMap::new(),
        )
        .unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "mistaken-entity-merge".into(),
            description: Some("preserved merge and advisory drift".into()),
            idempotency_key: OperationId(id(0x201)),
            actor_uuid: Some(id(0xff00)),
        })
        .unwrap();
    assert_eq!(
        graph
            .execute("MATCH ()-[r:SHARED_BROWSER_SESSION]->() RETURN r.edge_uuid AS edge_uuid")
            .unwrap()
            .stats
            .rows_produced,
        1
    );
    assert!(
        graph
            .runtime_catalog()
            .lock()
            .unwrap()
            .relation_types()
            .contains(&"SHARED_BROWSER_SESSION")
    );
    graph
        .revert_to_checkpoint(RevertCheckpointRequest {
            name: "initial-analysis".into(),
            reason: "separate parties and remove unsupported drift".into(),
            idempotency_key: OperationId(id(0x202)),
            actor_uuid: Some(id(0xff00)),
        })
        .unwrap();
    add_edge_by_key(
        &graph,
        "Party",
        "party_key",
        "P-00",
        "DISTINCT_FROM",
        "Party",
        "party_key",
        "P-01",
    );
    graph
        .checkpoint(CheckpointRequest {
            name: "entity-corrected".into(),
            description: Some("parties explicitly remain distinct".into()),
            idempotency_key: OperationId(id(0x203)),
            actor_uuid: Some(id(0xff00)),
        })
        .unwrap();

    add_edge_by_key(
        &graph,
        "Transaction",
        "transaction_key",
        "T-00",
        "TO_ACCOUNT",
        "Account",
        "account_key",
        "A-04",
    );
    graph
        .checkpoint(CheckpointRequest {
            name: "mistaken-transaction-association".into(),
            description: Some("preserved incorrect beneficiary association".into()),
            idempotency_key: OperationId(id(0x204)),
            actor_uuid: Some(id(0xff00)),
        })
        .unwrap();
    graph
        .revert_to_checkpoint(RevertCheckpointRequest {
            name: "entity-corrected".into(),
            reason: "restore original transaction beneficiary".into(),
            idempotency_key: OperationId(id(0x205)),
            actor_uuid: Some(id(0xff00)),
        })
        .unwrap();
    graph
        .checkpoint(CheckpointRequest {
            name: "transaction-corrected".into(),
            description: Some("correct beneficiary restored".into()),
            idempotency_key: OperationId(id(0x206)),
            actor_uuid: Some(id(0xff00)),
        })
        .unwrap();

    let _over_broad_review = graph
        .add_node(
            "Investigation",
            &props(&[
                ("name", PropValue::Str("Synthetic transfer review".into())),
                ("case_key", PropValue::Str("CASE-2468".into())),
            ]),
        )
        .unwrap();
    for index in 0..4 {
        add_edge_by_key(
            &graph,
            "Investigation",
            "case_key",
            "CASE-2468",
            "REVIEWS",
            "Transaction",
            "transaction_key",
            &format!("T-{index:02}"),
        );
    }
    graph
        .checkpoint(CheckpointRequest {
            name: "over-broad-analytical-scope".into(),
            description: Some("preserved review scope before evidence-based narrowing".into()),
            idempotency_key: OperationId(id(0x207)),
            actor_uuid: Some(id(0xff00)),
        })
        .unwrap();
    graph
        .revert_to_checkpoint(RevertCheckpointRequest {
            name: "transaction-corrected".into(),
            reason: "remove accounts outside the supported review scope".into(),
            idempotency_key: OperationId(id(0x208)),
            actor_uuid: Some(id(0xff00)),
        })
        .unwrap();
    let _corrected_review = graph
        .add_node(
            "Investigation",
            &props(&[
                ("name", PropValue::Str("Synthetic transfer review".into())),
                ("case_key", PropValue::Str("CASE-2468".into())),
            ]),
        )
        .unwrap();
    for index in 0..2 {
        add_edge_by_key(
            &graph,
            "Investigation",
            "case_key",
            "CASE-2468",
            "REVIEWS",
            "Transaction",
            "transaction_key",
            &format!("T-{index:02}"),
        );
    }
    graph
        .checkpoint(CheckpointRequest {
            name: "scope-corrected".into(),
            description: Some("review scope narrowed without erasing the prior generation".into()),
            idempotency_key: OperationId(id(0x209)),
            actor_uuid: Some(id(0xff00)),
        })
        .unwrap();

    for (seed, selected) in [
        (0x210, Some(fraud)),
        (0x220, Some(legitimate)),
        (0x230, None),
    ] {
        let item = selected.unwrap_or(legitimate);
        graph
            .record_hypothesis_selection(&RecordHypothesisSelectionRequest {
                context: context(seed),
                selection_event_uuid: id(seed + 1),
                group_uuid,
                selected_assertion_uuid: selected.map(|value| value.0),
                reasoning_uuid: item.1,
                provenance_uuid: item.2,
            })
            .unwrap();
    }
    let current_selection = graph.hypothesis_selection(group_uuid).unwrap();
    assert_eq!(current_selection.batches[0].num_rows(), 1);
    assert!(
        current_selection.batches[0]
            .column_by_name("selected_assertion_uuid")
            .unwrap()
            .is_null(0)
    );
    assert_eq!(
        graph
            .list_hypothesis_selection(&ListHypothesisSelectionRequest {
                group_uuid: Some(group_uuid),
                page: PageRequest::default(),
            })
            .unwrap()
            .batches[0]
            .num_rows(),
        3
    );
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: context(0x240),
            path: strict_path,
            mode: OntologyMode::Strict,
        })
        .unwrap();
    assert_eq!(graph.ontology_mode(), OntologyMode::Strict);
    graph
        .index_search(
            "Account",
            SearchIndexOptions::Text {
                properties: Some(vec!["name".into(), "account_key".into()]),
                rebuild: true,
            },
        )
        .unwrap();
    let corrected_search = graph
        .find(FindOptions {
            query: Some("Review Account".into()),
            label: Some("Account".into()),
            limit: 5,
            ..FindOptions::default()
        })
        .unwrap();
    let corrected_rank = graph.rank("Account", rank_options()).unwrap();
    assert_eq!(
        uuid_set(&corrected_rank, "node_uuid"),
        uuid_set(&rank, "node_uuid")
    );
    assert_eq!(
        graph
            .list_algorithm_runs(ListAlgorithmRunsRequest::default())
            .unwrap()
            .batches[0]
            .num_rows(),
        1
    );
    assert_eq!(
        graph
            .open_checkpoint("mistaken-entity-merge")
            .unwrap()
            .execute("MATCH ()-[r:SAME_AS]->() RETURN r.edge_uuid AS edge_uuid")
            .unwrap()
            .stats
            .rows_produced,
        1
    );
    assert_eq!(
        graph
            .open_checkpoint("mistaken-transaction-association")
            .unwrap()
            .execute("MATCH (t:Transaction {transaction_key:'T-00'})-[:TO_ACCOUNT]->(a:Account) RETURN a.account_key AS account_key ORDER BY account_key")
            .unwrap()
            .stats
            .rows_produced,
        2
    );
    assert_eq!(
        graph
            .open_checkpoint("over-broad-analytical-scope")
            .unwrap()
            .execute("MATCH (:Investigation)-[:REVIEWS]->(t:Transaction) RETURN t.transaction_key AS transaction_key ORDER BY transaction_key")
            .unwrap()
            .stats
            .rows_produced,
        4
    );
    assert_eq!(
        graph
            .execute("MATCH (:Investigation)-[:REVIEWS]->(t:Transaction) RETURN t.transaction_key AS transaction_key ORDER BY transaction_key")
            .unwrap()
            .stats
            .rows_produced,
        2
    );

    let account_uuids = accounts
        .iter()
        .map(|account| account.uuid)
        .collect::<Vec<_>>();
    let search_fingerprint = fingerprint(&corrected_search);
    let rank_fingerprint = fingerprint(&corrected_rank);
    drop(graph);
    let reopened = GraphForge::new(project.path().to_str()).unwrap();
    assert_eq!(reopened.ontology_mode(), OntologyMode::Strict);
    assert_eq!(
        reopened.rank("Account", rank_options()).unwrap(),
        corrected_rank
    );
    assert_eq!(
        reopened
            .find(FindOptions {
                query: Some("Review Account".into()),
                label: Some("Account".into()),
                limit: 5,
                ..FindOptions::default()
            })
            .unwrap(),
        corrected_search
    );
    assert_eq!(
        reopened.hypothesis_members(group_uuid).unwrap().batches[0].num_rows(),
        2
    );
    assert_eq!(
        reopened.hypothesis_selection(group_uuid).unwrap().batches,
        current_selection.batches
    );
    assert_eq!(
        reopened
            .open_checkpoint("over-broad-analytical-scope")
            .unwrap()
            .execute("MATCH (:Investigation)-[:REVIEWS]->(t:Transaction) RETURN t.transaction_key AS transaction_key")
            .unwrap()
            .stats
            .rows_produced,
        4
    );

    let evidence = json!({
        "schema_version": 1,
        "scenario_id": "finance-fraud",
        "commit_sha": std::env::var("GF_RELEASE_WORKFLOW_SHA").unwrap(),
        "ontology": {"mode": "strict", "entities": 9, "relations": 9, "properties": 18, "constraints": 3, "advisory_drift_removed": true},
        "operation_rows": {"search": search.num_rows(), "cluster": cluster.num_rows(), "paths": path.num_rows(), "rank": rank.num_rows(), "similar": similar.num_rows(), "recorded_runs": 1},
        "invalid_scope_error": invalid_scope.code(),
        "corrections": {"entity_merge": true, "transaction_association": true, "analytical_scope": true, "prior_checkpoints_readable": true},
        "hypotheses": {"members": 2, "selection_events": 3, "current_selection": null, "fraud_determination": false},
        "account_uuids": account_uuids.iter().map(Uuid::to_string).collect::<Vec<_>>(),
        "arrow_fingerprints": {"search": search_fingerprint, "rank": rank_fingerprint},
        "reopen_equal": true
    });
    fs::write(
        std::env::var("GF_FINANCE_EVIDENCE_PATH").unwrap(),
        serde_json::to_vec_pretty(&evidence).unwrap(),
    )
    .unwrap();
}
