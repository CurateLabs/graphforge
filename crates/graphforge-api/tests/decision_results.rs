//! Direct contract tests for provider-neutral decision result validation.
use arrow::array::{Array, FixedSizeBinaryArray, Float64Array, StringArray};
use arrow::ipc::writer::StreamWriter;
use graphforge_api::*;
use std::collections::BTreeSet;
use uuid::Uuid;

fn input(items: &[Uuid]) -> DecisionInputIdentityV1 {
    DecisionInputIdentityV1 {
        generation_uuid: Uuid::now_v7(),
        version_uuid: Some(Uuid::now_v7()),
        projection_sha256: [7; 32],
        selection_sha256: [9; 32],
        selected_item_uuids: items.iter().copied().collect(),
    }
}

fn producer() -> DecisionProducerV1 {
    DecisionProducerV1 {
        name: "offline fixture producer".into(),
        model: Some("triage-fixture".into()),
        revision: None,
    }
}

fn decision_batch() -> (DecisionBatchV1, Uuid, Uuid, Uuid, Uuid) {
    let mystery = Uuid::now_v7();
    let voyage = Uuid::now_v7();
    let queue_question = Uuid::now_v7();
    let rubric_question = Uuid::now_v7();
    let review_question = Uuid::now_v7();
    let batch = DecisionBatchV1 {
        input: input(&[mystery, voyage]),
        producer: producer(),
        questions: vec![
            DecisionQuestionV1 {
                question_uuid: queue_question,
                text: "Where should this item go?".into(),
                item_uuids: vec![mystery, voyage],
                kind: DecisionQuestionKindV1::Choice {
                    allowed_choices: vec!["research".into(), "human_review".into()],
                },
            },
            DecisionQuestionV1 {
                question_uuid: rubric_question,
                text: "How relevant is this evidence?".into(),
                item_uuids: vec![mystery, voyage],
                kind: DecisionQuestionKindV1::RubricScore {
                    ordered_levels: vec!["low".into(), "medium".into(), "high".into()],
                },
            },
            DecisionQuestionV1 {
                question_uuid: review_question,
                text: "Does this evidence need human review?".into(),
                item_uuids: vec![],
                kind: DecisionQuestionKindV1::YesNoProbability,
            },
        ],
        // Deliberately shuffled. The missing Voyage rubric row is made explicit
        // by validation, rather than being filled from the row position.
        results: vec![
            DecisionResultV1 {
                question_uuid: queue_question,
                item_uuid: Some(voyage),
                status: DecisionResultStatusV1::Uncertain,
                value: Some(DecisionValueV1::Choice("human_review".into())),
                confidence: None,
            },
            DecisionResultV1 {
                question_uuid: review_question,
                item_uuid: None,
                status: DecisionResultStatusV1::Answered,
                value: Some(DecisionValueV1::YesNoProbability {
                    yes_probability: 0.82,
                    no_probability: Some(0.18),
                }),
                confidence: None,
            },
            DecisionResultV1 {
                question_uuid: rubric_question,
                item_uuid: Some(mystery),
                status: DecisionResultStatusV1::Answered,
                value: Some(DecisionValueV1::RubricScore("high".into())),
                confidence: Some(DecisionConfidenceV1 {
                    value: 0.91,
                    minimum: 0.0,
                    maximum: 1.0,
                    domain: "unit_interval".into(),
                    meaning: "producer-estimated correctness".into(),
                }),
            },
            DecisionResultV1 {
                question_uuid: queue_question,
                item_uuid: Some(mystery),
                status: DecisionResultStatusV1::Answered,
                value: Some(DecisionValueV1::Choice("research".into())),
                confidence: None,
            },
        ],
    };
    (batch, mystery, voyage, queue_question, rubric_question)
}

fn strings<'a>(batch: &'a arrow::record_batch::RecordBatch, column: &str) -> &'a StringArray {
    batch
        .column_by_name(column)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
}

fn find_row(
    batch: &arrow::record_batch::RecordBatch,
    question_uuid: Uuid,
    item_uuid: Option<Uuid>,
) -> usize {
    let questions = batch
        .column_by_name("question_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let items = batch
        .column_by_name("item_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    (0..batch.num_rows())
        .find(|row| {
            questions.value(*row) == question_uuid.as_bytes()
                && match item_uuid {
                    Some(item) => items.value(*row) == item.as_bytes(),
                    None => items.is_null(*row),
                }
        })
        .unwrap()
}

fn context() -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(Uuid::now_v7()),
        actor_uuid: None,
    }
}

fn capture(graph: &mut GraphForge) -> Uuid {
    let operation = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
            label: Some("Decision evidence".into()),
            description: Some("Explicitly retained decision result".into()),
            created_at: 1,
            required_versions: BTreeSet::new(),
        })
        .unwrap();
    let version_uuid = match &operation.mutation {
        ResearchMutation::Register(spec) => spec.version_uuid,
        _ => unreachable!("prepare_research_version creates a Version"),
    };
    graph
        .commit_research_version_operation(operation, &CancellationToken::new())
        .unwrap();
    version_uuid
}

fn ipc_bytes(batch: &arrow::record_batch::RecordBatch) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, batch.schema().as_ref()).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    bytes
}

#[test]
fn validates_shuffled_choices_rubrics_probability_confidence_and_explicit_missing_rows() {
    let (request, mystery, voyage, queue, rubric) = decision_batch();
    let arrow = request.validate().unwrap();
    assert_eq!(arrow.num_rows(), 5);
    assert_eq!(
        arrow.schema().metadata()["graphforge.contract"],
        "decision_result/1"
    );
    assert_eq!(
        strings(&arrow, "choice_value").value(find_row(&arrow, queue, Some(voyage))),
        "human_review"
    );
    assert_eq!(
        strings(&arrow, "status").value(find_row(&arrow, queue, Some(voyage))),
        "uncertain"
    );
    assert_eq!(
        strings(&arrow, "choice_value").value(find_row(&arrow, queue, Some(mystery))),
        "research"
    );
    let missing = find_row(&arrow, rubric, Some(voyage));
    assert_eq!(strings(&arrow, "status").value(missing), "missing");
    assert!(
        arrow
            .column_by_name("rubric_score")
            .unwrap()
            .is_null(missing)
    );

    let review = request
        .questions
        .iter()
        .find(|question| question.kind == DecisionQuestionKindV1::YesNoProbability)
        .unwrap()
        .question_uuid;
    let probability_row = find_row(&arrow, review, None);
    let yes = arrow
        .column_by_name("yes_probability")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let no = arrow
        .column_by_name("no_probability")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(yes.value(probability_row), 0.82);
    assert_eq!(no.value(probability_row), 0.18);

    let confidence_row = find_row(&arrow, rubric, Some(mystery));
    assert_eq!(
        strings(&arrow, "confidence_domain").value(confidence_row),
        "unit_interval"
    );
    assert_eq!(
        strings(&arrow, "confidence_meaning").value(confidence_row),
        "producer-estimated correctness"
    );
    assert!(request.producer.revision.is_none());
}

#[test]
fn rejects_duplicate_unknown_incompatible_and_malformed_result_rows() {
    let (request, mystery, voyage, queue, _) = decision_batch();

    let mut duplicate = request.clone();
    duplicate.results.push(duplicate.results[0].clone());
    assert!(duplicate.validate().is_err());

    let mut unknown_item = request.clone();
    unknown_item.results[0].item_uuid = Some(Uuid::now_v7());
    assert!(unknown_item.validate().is_err());

    let mut unknown_question = request.clone();
    unknown_question.results[0].question_uuid = Uuid::now_v7();
    assert!(unknown_question.validate().is_err());

    let mut incompatible_choice = request.clone();
    let choice = incompatible_choice
        .results
        .iter_mut()
        .find(|row| row.question_uuid == queue && row.item_uuid == Some(mystery))
        .unwrap();
    choice.value = Some(DecisionValueV1::Choice("canonical".into()));
    assert!(incompatible_choice.validate().is_err());

    let mut unavailable_negative = request.clone();
    let unavailable = &mut unavailable_negative.results[0];
    unavailable.status = DecisionResultStatusV1::Unavailable;
    unavailable.value = Some(DecisionValueV1::Choice("human_review".into()));
    assert!(unavailable_negative.validate().is_err());

    let mut bad_probability = request.clone();
    let probability = bad_probability
        .results
        .iter_mut()
        .find(|row| {
            row.value
                .as_ref()
                .is_some_and(|value| matches!(value, DecisionValueV1::YesNoProbability { .. }))
        })
        .unwrap();
    probability.value = Some(DecisionValueV1::YesNoProbability {
        yes_probability: 0.8,
        no_probability: Some(0.3),
    });
    assert!(bad_probability.validate().is_err());

    let mut non_finite = request;
    let probability = non_finite
        .results
        .iter_mut()
        .find(|row| {
            row.value
                .as_ref()
                .is_some_and(|value| matches!(value, DecisionValueV1::YesNoProbability { .. }))
        })
        .unwrap();
    probability.value = Some(DecisionValueV1::YesNoProbability {
        yes_probability: f64::NAN,
        no_probability: None,
    });
    assert!(non_finite.validate().is_err());

    let mut invalid_confidence = decision_batch().0;
    let answer = invalid_confidence
        .results
        .iter_mut()
        .find(|row| row.confidence.is_some())
        .unwrap();
    answer.confidence.as_mut().unwrap().value = f64::INFINITY;
    assert!(invalid_confidence.validate().is_err());
    assert_ne!(mystery, voyage);
}

#[test]
fn unavailable_results_remain_explicit_and_never_encode_a_negative_answer() {
    let (mut request, _, voyage, question_uuid, _) = decision_batch();
    let unavailable = request
        .results
        .iter_mut()
        .find(|row| row.question_uuid == question_uuid && row.item_uuid == Some(voyage))
        .unwrap();
    unavailable.status = DecisionResultStatusV1::Unavailable;
    unavailable.value = None;
    let arrow = request.validate().unwrap();
    let row = find_row(&arrow, question_uuid, Some(voyage));
    assert_eq!(strings(&arrow, "status").value(row), "unavailable");
    assert!(arrow.column_by_name("choice_value").unwrap().is_null(row));
}

#[test]
fn enforces_item_membership_and_the_256_row_decision_batch_bound() {
    let (mut request, _, _, _, _) = decision_batch();
    request.questions[0].item_uuids.push(Uuid::now_v7());
    assert!(request.validate().is_err());

    let mut oversized = decision_batch().0;
    let question_uuid = Uuid::now_v7();
    oversized.questions = vec![DecisionQuestionV1 {
        question_uuid,
        text: "Choose a route".into(),
        item_uuids: (0..=DECISION_BATCH_MAX_ROWS)
            .map(|_| Uuid::now_v7())
            .collect(),
        kind: DecisionQuestionKindV1::Choice {
            allowed_choices: vec!["research".into()],
        },
    }];
    oversized.input.selected_item_uuids = oversized.questions[0]
        .item_uuids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    assert!(oversized.validate().is_err());
}

#[test]
fn validation_is_read_only_and_explicit_artifact_recording_survives_reopen() {
    let memory_graph = GraphForge::new(None).unwrap();
    let (mut ephemeral, _, _, _, _) = decision_batch();
    let identity_before = memory_graph.research_project_summary().unwrap().identity;
    ephemeral.input.generation_uuid = identity_before.generation_uuid;
    ephemeral.input.version_uuid = None;
    ephemeral.validate().unwrap();
    let identity_after = memory_graph.research_project_summary().unwrap().identity;
    assert_eq!(
        identity_after.generation_uuid,
        identity_before.generation_uuid
    );
    assert_eq!(
        memory_graph.list_research_versions().unwrap().batches[0].num_rows(),
        0
    );
    drop(memory_graph);

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Story {name:'Mystery'}), (:Story {name:'Voyage'})")
        .unwrap();
    let mystery = graph
        .execute("MATCH (s:Story {name:'Mystery'}) RETURN s.node_uuid")
        .unwrap()
        .batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap()
        .value(0)
        .to_vec();
    let voyage = graph
        .execute("MATCH (s:Story {name:'Voyage'}) RETURN s.node_uuid")
        .unwrap()
        .batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap()
        .value(0)
        .to_vec();
    let mystery = Uuid::from_slice(&mystery).unwrap();
    let voyage = Uuid::from_slice(&voyage).unwrap();
    for capability_id in [CapabilityId::Provenance, CapabilityId::Knowledge] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
    let source_version = capture(&mut graph);
    let source_generation = graph
        .research_version(source_version)
        .unwrap()
        .content
        .generation_uuid;
    let (mut retained, old_mystery, old_voyage, _, _) = decision_batch();
    retained.input.generation_uuid = source_generation;
    retained.input.version_uuid = Some(source_version);
    retained.input.selected_item_uuids = BTreeSet::from([mystery, voyage]);
    for question in &mut retained.questions {
        for item_uuid in &mut question.item_uuids {
            *item_uuid = if *item_uuid == old_mystery {
                mystery
            } else {
                voyage
            };
        }
    }
    for result in &mut retained.results {
        if let Some(item_uuid) = &mut result.item_uuid {
            *item_uuid = if *item_uuid == old_mystery {
                mystery
            } else {
                voyage
            };
        }
    }
    assert_ne!(old_mystery, old_voyage);
    let arrow = retained.validate().unwrap();
    let payload = ipc_bytes(&arrow);
    let source_uuid = Uuid::now_v7();
    let artifact_uuid = Uuid::now_v7();
    graph
        .register_source(RegisterSourceRequest {
            context: context(),
            source_uuid,
            label: "Decision result record".into(),
            source_kind: SourceKind::Other,
            identity_uri: None,
        })
        .unwrap();
    graph
        .register_artifact(RegisterArtifactRequest {
            context: context(),
            artifact_uuid,
            source_uuid,
            artifact_kind: ArtifactKind::Other,
            media_type: "application/vnd.apache.arrow.stream".into(),
            payload: ArtifactPayloadRequest::LocalBytes(payload.clone()),
            derivation_inputs: vec![
                DerivationInput {
                    input_uuid: mystery,
                    input_kind: DerivationSubjectKind::Node,
                },
                DerivationInput {
                    input_uuid: voyage,
                    input_kind: DerivationSubjectKind::Node,
                },
            ],
            run_uuid: None,
        })
        .unwrap();
    let retained_version = capture(&mut graph);
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
    let view = reopened.open_research_version(retained_version).unwrap();
    let payload_result = view.artifact_payload(artifact_uuid).unwrap();
    let bytes = payload_result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .unwrap()
        .value(0);
    assert_eq!(bytes, payload);
}
