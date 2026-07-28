//! Direct public contract proof for pinned belief-subject evidence.

use std::collections::{BTreeSet, HashMap};

use arrow::array::{
    Array, FixedSizeBinaryArray, ListArray, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use gf_api::{
    AssertionGraphRefInput, AssertionGraphRole, AssertionStatus, AssessConfidenceRequest,
    BeliefProjectionPolicyV1, BeliefSubjectV1, CapabilityId, ConfidencePolicyRequest,
    CreateAssertionRequest, CreateHypothesisGroupRequest, EnableCapabilityRequest, GraphForge,
    GraphObjectKind, HypothesisMembershipAction, HypothesisSelectionPolicyV1, NodeSelector,
    OperationId, PathsOptions, ReasoningContentFormat, ReasoningKind, RecordAssertionStatusRequest,
    RecordAssertionValidityRequest, RecordHypothesisMembershipRequest,
    RecordHypothesisSelectionRequest, RecordReasoningRequest, ResolveBeliefProjectionRequest,
    ResolveBeliefSubjectRequest, StatuslessPolicyV1, SupersedeAssertionRequest,
    SupersessionBranchPolicyV1, WriteContext,
};
use tempfile::TempDir;
use uuid::Uuid;

const QUESTION_KEY: &str = "belief-subject.primary.v1";
const CUTOFF: i64 = i64::MAX;
const VALID_AT: i64 = 150;

#[derive(Clone, Copy)]
struct Ids {
    prior: Uuid,
    selected: Uuid,
    alternative: Uuid,
    primary_group: Uuid,
    unselected_group: Uuid,
    prior_reasoning: Uuid,
    selected_reasoning: Uuid,
    alternative_reasoning: Uuid,
    supersession: Uuid,
    superseded_status: Uuid,
    selected_status: Uuid,
    primary_selected_membership: Uuid,
    primary_alternative_membership: Uuid,
    unselected_membership: Uuid,
    selection: Uuid,
    validity: Uuid,
}

impl Ids {
    fn new() -> Self {
        Self {
            prior: uuid7(10),
            selected: uuid7(11),
            alternative: uuid7(12),
            primary_group: uuid7(40),
            unselected_group: uuid7(41),
            prior_reasoning: uuid7(30),
            selected_reasoning: uuid7(31),
            alternative_reasoning: uuid7(32),
            supersession: uuid7(50),
            superseded_status: uuid7(51),
            selected_status: uuid7(52),
            primary_selected_membership: uuid7(60),
            primary_alternative_membership: uuid7(61),
            unselected_membership: uuid7(62),
            selection: uuid7(63),
            validity: uuid7(70),
        }
    }
}

struct Project {
    root: TempDir,
    graph: GraphForge,
    ids: Ids,
    nodes: [Uuid; 3],
}

fn uuid7(seed: u8) -> Uuid {
    let mut bytes = [seed; 16];
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn context(seed: u8) -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(uuid7(seed)),
        actor_uuid: None,
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

fn fixed<'a>(batch: &'a RecordBatch, name: &str) -> &'a FixedSizeBinaryArray {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap()
}

fn uuid_at(batch: &RecordBatch, name: &str, row: usize) -> Option<Uuid> {
    let values = fixed(batch, name);
    (!values.is_null(row)).then(|| Uuid::from_slice(values.value(row)).unwrap())
}

fn uuid_list_at(batch: &RecordBatch, name: &str, row: usize) -> Vec<Uuid> {
    let lists = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = lists.value(row);
    let values = values
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    (0..values.len())
        .map(|index| Uuid::from_slice(values.value(index)).unwrap())
        .collect()
}

fn strings<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
}

fn provenance(result: &gf_api::ExecutionResult) -> Uuid {
    Uuid::from_slice(fixed(&result.batches[0], "provenance_uuid").value(0)).unwrap()
}

fn create_assertion(graph: &GraphForge, assertion_uuid: Uuid, seed: u8) -> (Uuid, Uuid) {
    let node = graph.add_node("BeliefSubject", &HashMap::new()).unwrap();
    let provenance_uuid = provenance(
        &graph
            .create_assertion(CreateAssertionRequest {
                context: context(seed),
                assertion_uuid,
                claim: format!("claim {assertion_uuid}"),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid: node.uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            })
            .unwrap(),
    );
    (provenance_uuid, node.uuid)
}

fn record_reasoning(
    graph: &GraphForge,
    assertion_uuid: Uuid,
    reasoning_uuid: Uuid,
    provenance_uuid: Uuid,
    seed: u8,
) {
    graph
        .record_reasoning(RecordReasoningRequest {
            context: context(seed),
            reasoning_uuid,
            assertion_uuid,
            kind: ReasoningKind::DecisionRationale,
            content_format: ReasoningContentFormat::TextPlain,
            content: format!("reason {assertion_uuid}").into_bytes(),
            supersedes_reasoning_uuid: None,
            provenance_uuid,
        })
        .unwrap();
}

fn membership(
    graph: &GraphForge,
    event_uuid: Uuid,
    operation_seed: u8,
    group_uuid: Uuid,
    assertion_uuid: Uuid,
    reasoning_uuid: Uuid,
    provenance_uuid: Uuid,
) {
    graph
        .record_hypothesis_membership(&RecordHypothesisMembershipRequest {
            context: context(operation_seed),
            membership_event_uuid: event_uuid,
            group_uuid,
            assertion_uuid,
            action: HypothesisMembershipAction::Added,
            reasoning_uuid,
            provenance_uuid,
        })
        .unwrap();
}

#[allow(
    clippy::too_many_lines,
    reason = "the fixture freezes one complete public belief-subject event graph"
)]
fn project(reverse_independent_events: bool) -> Project {
    let root = TempDir::new().unwrap();
    let graph = GraphForge::new(root.path().to_str()).unwrap();
    enable(&graph, CapabilityId::Provenance, 1);
    enable(&graph, CapabilityId::Knowledge, 2);
    enable(&graph, CapabilityId::Epistemic, 3);
    enable(&graph, CapabilityId::ValidTime, 4);
    let ids = Ids::new();
    let assertions = [
        create_assertion(&graph, ids.prior, 20),
        create_assertion(&graph, ids.selected, 21),
        create_assertion(&graph, ids.alternative, 22),
    ];
    let provenances = assertions.map(|(provenance_uuid, _)| provenance_uuid);
    let nodes = assertions.map(|(_, node_uuid)| node_uuid);
    for (assertion, reasoning, provenance, seed) in [
        (ids.prior, ids.prior_reasoning, provenances[0], 33),
        (ids.selected, ids.selected_reasoning, provenances[1], 34),
        (
            ids.alternative,
            ids.alternative_reasoning,
            provenances[2],
            35,
        ),
    ] {
        record_reasoning(&graph, assertion, reasoning, provenance, seed);
    }
    graph
        .record_assertion_status(RecordAssertionStatusRequest {
            context: context(53),
            status_event_uuid: ids.selected_status,
            assertion_uuid: ids.selected,
            status: AssertionStatus::Supported,
            confidence_uuid: None,
            reasoning_uuid: Some(ids.selected_reasoning),
            provenance_uuid: provenances[1],
        })
        .unwrap();
    graph
        .assess_confidence(AssessConfidenceRequest {
            context: context(54),
            confidence_uuid: uuid7(55),
            assertion_uuid: ids.alternative,
            policy: ConfidencePolicyRequest::Explicit { value: 1.0 },
        })
        .unwrap();
    graph
        .create_hypothesis_group(CreateHypothesisGroupRequest {
            context: context(42),
            group_uuid: ids.primary_group,
            question_key: QUESTION_KEY.into(),
            provenance_uuid: provenances[1],
        })
        .unwrap();
    graph
        .create_hypothesis_group(CreateHypothesisGroupRequest {
            context: context(43),
            group_uuid: ids.unselected_group,
            question_key: "belief-subject.unselected.v1".into(),
            provenance_uuid: provenances[2],
        })
        .unwrap();
    let selected_membership = || {
        membership(
            &graph,
            ids.primary_selected_membership,
            64,
            ids.primary_group,
            ids.selected,
            ids.selected_reasoning,
            provenances[1],
        );
    };
    let alternative_memberships = || {
        membership(
            &graph,
            ids.primary_alternative_membership,
            65,
            ids.primary_group,
            ids.alternative,
            ids.alternative_reasoning,
            provenances[2],
        );
        membership(
            &graph,
            ids.unselected_membership,
            66,
            ids.unselected_group,
            ids.alternative,
            ids.alternative_reasoning,
            provenances[2],
        );
    };
    if reverse_independent_events {
        alternative_memberships();
        selected_membership();
    } else {
        selected_membership();
        alternative_memberships();
    }
    graph
        .record_hypothesis_selection(&RecordHypothesisSelectionRequest {
            context: context(67),
            selection_event_uuid: ids.selection,
            group_uuid: ids.primary_group,
            selected_assertion_uuid: Some(ids.selected),
            reasoning_uuid: ids.selected_reasoning,
            provenance_uuid: provenances[1],
        })
        .unwrap();
    graph
        .supersede_assertion(SupersedeAssertionRequest {
            context: context(56),
            supersession_uuid: ids.supersession,
            prior_assertion_uuid: ids.prior,
            replacement_assertion_uuid: ids.selected,
            status_event_uuid: ids.superseded_status,
            reasoning_uuid: ids.prior_reasoning,
            provenance_uuid: provenances[0],
        })
        .unwrap();
    graph
        .record_assertion_validity(RecordAssertionValidityRequest {
            context: context(71),
            validity_event_uuid: ids.validity,
            assertion_uuid: ids.selected,
            valid_from_micros: Some(100),
            valid_to_micros: Some(200),
            reasoning_uuid: Some(ids.selected_reasoning),
            provenance_uuid: provenances[1],
        })
        .unwrap();
    Project {
        root,
        graph,
        ids,
        nodes,
    }
}

fn policy() -> BeliefProjectionPolicyV1 {
    BeliefProjectionPolicyV1 {
        included_statuses: vec![AssertionStatus::Supported],
        statusless: StatuslessPolicyV1::Include,
        supersession_branches: SupersessionBranchPolicyV1::IncludeAllLeaves,
        hypotheses: HypothesisSelectionPolicyV1::ExcludeUnselectedGroup,
    }
}

fn request(
    subject: BeliefSubjectV1,
    valid_time_micros: Option<i64>,
) -> ResolveBeliefSubjectRequest {
    ResolveBeliefSubjectRequest {
        subject,
        projection: ResolveBeliefProjectionRequest {
            transaction_cutoff_micros: CUTOFF,
            valid_time_micros,
            policy: policy(),
        },
    }
}

fn assert_exact_schema(batch: &RecordBatch) {
    let schema = batch.schema();
    let fields = schema.fields().clone();
    let names = fields
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            "entity_kind",
            "assertion_uuid",
            "group_uuid",
            "question_key",
            "status",
            "status_event_uuid",
            "reasoning_history_uuids",
            "reasoning_leaf_uuids",
            "superseded_by_assertion_uuids",
            "current_member_assertion_uuids",
            "selected_assertion_uuid",
            "source_record_uuids",
            "transaction_cutoff",
            "resolution_policy",
            "snapshot_fingerprint",
            "source_generation_uuid",
            "transaction_cutoff_micros",
            "valid_time_micros",
            "policy_fingerprint",
            "valid_time_fingerprint",
            "graph_content_fingerprint",
        ]
    );
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    for (name, width, nullable) in [
        ("source_generation_uuid", 16, false),
        ("policy_fingerprint", 32, false),
        ("valid_time_fingerprint", 32, true),
        ("graph_content_fingerprint", 32, false),
    ] {
        let field = schema.field_with_name(name).unwrap();
        assert_eq!(field.data_type(), &DataType::FixedSizeBinary(width));
        assert_eq!(field.is_nullable(), nullable);
    }
    for (name, nullable) in [
        ("transaction_cutoff_micros", false),
        ("valid_time_micros", true),
    ] {
        let field = schema.field_with_name(name).unwrap();
        assert_eq!(field.data_type(), &timestamp);
        assert_eq!(field.is_nullable(), nullable);
    }
}

fn assert_envelope(result: &gf_api::ResolvedBeliefSubject, valid_time: Option<i64>) {
    let batch = &result.evidence.batches[0];
    assert_exact_schema(batch);
    let cutoff = batch
        .column_by_name("transaction_cutoff_micros")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let valid = batch
        .column_by_name("valid_time_micros")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    for row in 0..batch.num_rows() {
        assert_eq!(
            uuid_at(batch, "source_generation_uuid", row),
            Some(result.projection.source_generation_uuid())
        );
        assert_eq!(cutoff.value(row), CUTOFF);
        assert_eq!((!valid.is_null(row)).then(|| valid.value(row)), valid_time);
        assert_eq!(
            fixed(batch, "policy_fingerprint").value(row),
            result.projection.policy_fingerprint()
        );
        assert_eq!(
            fixed(batch, "snapshot_fingerprint").value(row),
            result.projection.snapshot_fingerprint()
        );
        assert_eq!(
            (!fixed(batch, "valid_time_fingerprint").is_null(row)).then(|| fixed(
                batch,
                "valid_time_fingerprint"
            )
            .value(row)),
            result
                .projection
                .valid_time_fingerprint()
                .as_ref()
                .map(<[u8; 32]>::as_slice)
        );
        assert_eq!(
            fixed(batch, "graph_content_fingerprint").value(row),
            result.projection.graph_content_fingerprint()
        );
    }
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one public matrix keeps identities, temporal axes, and reopen evidence auditable"
)]
fn public_belief_subject_contract_is_exact_temporal_and_reopenable() {
    let project = project(false);
    let ids = project.ids;
    let by_question = project
        .graph
        .resolve_belief_subject(&request(
            BeliefSubjectV1::HypothesisQuestionKey(QUESTION_KEY.into()),
            Some(VALID_AT),
        ))
        .unwrap();
    let by_assertion = project
        .graph
        .resolve_belief_subject(&request(
            BeliefSubjectV1::Assertion(ids.prior),
            Some(VALID_AT),
        ))
        .unwrap();
    assert_eq!(by_question.evidence.batches, by_assertion.evidence.batches);
    assert_envelope(&by_question, Some(VALID_AT));
    let batch = &by_question.evidence.batches[0];
    assert_eq!(batch.num_rows(), 5);
    assert_eq!(
        (0..3)
            .map(|row| uuid_at(batch, "assertion_uuid", row).unwrap())
            .collect::<Vec<_>>(),
        [ids.prior, ids.selected, ids.alternative]
    );
    assert_eq!(
        (3..5)
            .map(|row| uuid_at(batch, "group_uuid", row).unwrap())
            .collect::<Vec<_>>(),
        [ids.primary_group, ids.unselected_group]
    );
    assert_eq!(strings(batch, "status").value(0), "superseded");
    assert_eq!(strings(batch, "status").value(1), "supported");
    assert!(strings(batch, "status").is_null(2));
    assert_eq!(
        uuid_list_at(batch, "superseded_by_assertion_uuids", 0),
        [ids.selected]
    );
    assert_eq!(
        uuid_list_at(batch, "current_member_assertion_uuids", 3),
        [ids.selected, ids.alternative]
    );
    assert_eq!(
        uuid_at(batch, "selected_assertion_uuid", 3),
        Some(ids.selected)
    );
    assert_eq!(
        uuid_list_at(batch, "current_member_assertion_uuids", 4),
        [ids.alternative]
    );
    assert_eq!(uuid_at(batch, "selected_assertion_uuid", 4), None);
    assert_eq!(strings(batch, "question_key").value(3), QUESTION_KEY);
    let expected_sources = [
        BTreeSet::from([
            ids.prior,
            ids.prior_reasoning,
            ids.supersession,
            ids.superseded_status,
        ]),
        BTreeSet::from([
            ids.selected,
            ids.selected_reasoning,
            ids.supersession,
            ids.selected_status,
        ]),
        BTreeSet::from([ids.alternative, ids.alternative_reasoning]),
        BTreeSet::from([
            ids.primary_group,
            ids.primary_selected_membership,
            ids.primary_alternative_membership,
            ids.selection,
        ]),
        BTreeSet::from([ids.unselected_group, ids.unselected_membership]),
    ];
    for (row, expected) in expected_sources.iter().enumerate() {
        assert_eq!(
            uuid_list_at(batch, "source_record_uuids", row)
                .into_iter()
                .collect::<BTreeSet<_>>(),
            *expected
        );
    }
    assert_eq!(
        by_question
            .projection
            .source_record_uuids()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>(),
        expected_sources.into_iter().flatten().collect()
    );

    let no_valid_time = project
        .graph
        .resolve_belief_subject(&request(BeliefSubjectV1::Assertion(ids.prior), None))
        .unwrap();
    assert_envelope(&no_valid_time, None);
    assert_eq!(
        no_valid_time.projection.snapshot_fingerprint(),
        by_question.projection.snapshot_fingerprint()
    );
    assert_ne!(
        no_valid_time.projection.valid_time_fingerprint(),
        by_question.projection.valid_time_fingerprint()
    );
    let outside_validity = project
        .graph
        .resolve_belief_subject(&request(BeliefSubjectV1::Assertion(ids.prior), Some(200)))
        .unwrap();
    assert_ne!(
        outside_validity.projection.graph_content_fingerprint(),
        by_question.projection.graph_content_fingerprint()
    );
    let too_early = ResolveBeliefSubjectRequest {
        subject: BeliefSubjectV1::Assertion(ids.prior),
        projection: ResolveBeliefProjectionRequest {
            transaction_cutoff_micros: 0,
            valid_time_micros: Some(VALID_AT),
            policy: policy(),
        },
    };
    assert_eq!(
        project
            .graph
            .resolve_belief_subject(&too_early)
            .unwrap_err()
            .code(),
        "GF_NOT_FOUND"
    );

    let path = project.root.path().to_str().unwrap();
    let reopened = GraphForge::new(Some(path)).unwrap();
    let reopened_result = reopened
        .resolve_belief_subject(&request(
            BeliefSubjectV1::Assertion(ids.prior),
            Some(VALID_AT),
        ))
        .unwrap();
    let selected_node = NodeSelector::Uuid(project.nodes[1]);
    by_question
        .projection
        .prepare_paths_invocation(
            Some(&selected_node),
            Some(&selected_node),
            &PathsOptions::default(),
        )
        .expect("selected assertion node must exist before valid_to");
    let expired_node = outside_validity
        .projection
        .prepare_paths_invocation(
            Some(&selected_node),
            Some(&selected_node),
            &PathsOptions::default(),
        )
        .expect_err("selected assertion node must be absent at exclusive valid_to");
    assert_eq!(
        expired_node.code(),
        "GF_VALIDATION",
        "the exact selected assertion node must not resolve in the expired projection"
    );
    assert_eq!(
        reopened_result.evidence.batches,
        by_question.evidence.batches
    );
    assert_eq!(
        reopened_result.projection.source_generation_uuid(),
        by_question.projection.source_generation_uuid()
    );
    assert_eq!(
        reopened_result.projection.graph_content_fingerprint(),
        by_question.projection.graph_content_fingerprint()
    );
}

#[test]
fn public_belief_subject_is_stable_across_independent_event_order() {
    let first = project(false);
    let second = project(true);
    let first_result = first
        .graph
        .resolve_belief_subject(&request(
            BeliefSubjectV1::HypothesisQuestionKey(QUESTION_KEY.into()),
            Some(VALID_AT),
        ))
        .unwrap();
    let second_result = second
        .graph
        .resolve_belief_subject(&request(
            BeliefSubjectV1::HypothesisQuestionKey(QUESTION_KEY.into()),
            Some(VALID_AT),
        ))
        .unwrap();
    assert_eq!(
        first_result.projection.snapshot_fingerprint(),
        second_result.projection.snapshot_fingerprint()
    );
    assert_eq!(
        first_result.projection.policy_fingerprint(),
        second_result.projection.policy_fingerprint()
    );
    assert_eq!(
        first_result.projection.valid_time_fingerprint(),
        second_result.projection.valid_time_fingerprint()
    );
    // Generation UUIDs and graph-content fingerprints are intentionally excluded:
    // each fixture is an independent project with distinct generated node identities.
    for name in [
        "entity_kind",
        "assertion_uuid",
        "group_uuid",
        "question_key",
        "status",
        "status_event_uuid",
        "reasoning_history_uuids",
        "reasoning_leaf_uuids",
        "superseded_by_assertion_uuids",
        "current_member_assertion_uuids",
        "selected_assertion_uuid",
        "source_record_uuids",
        "transaction_cutoff",
        "resolution_policy",
        "snapshot_fingerprint",
        "transaction_cutoff_micros",
        "valid_time_micros",
        "policy_fingerprint",
        "valid_time_fingerprint",
    ] {
        let first_column = first_result.evidence.batches[0]
            .column_by_name(name)
            .unwrap_or_else(|| panic!("first evidence batch is missing {name}"));
        let second_column = second_result.evidence.batches[0]
            .column_by_name(name)
            .unwrap_or_else(|| panic!("second evidence batch is missing {name}"));
        assert_eq!(first_column, second_column, "event-order drift in {name}");
    }
}
