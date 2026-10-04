//! Direct contract tests for provider-neutral decision result validation.
use arrow::array::{Array, BinaryArray, FixedSizeBinaryArray, Float64Array, StringArray};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use graphforge_api::*;
use graphforge_knowledge::research::ResearchCategory;
use graphforge_knowledge::{AssertionGraphRole, GraphObjectKind};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io::Cursor;
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
            author: None,
            committer: None,
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

fn current_generation(graph: &GraphForge) -> Uuid {
    graph
        .research_project_summary()
        .unwrap()
        .identity
        .generation_uuid
}

fn selected_candidates(graph: &GraphForge) -> (Vec<Uuid>, Vec<String>) {
    let rows = graph
        .execute("MATCH (c:Candidate) RETURN c.node_uuid AS item_uuid, c.title AS title ORDER BY c.node_uuid LIMIT 20")
        .unwrap();
    let batch = &rows.batches[0];
    let ids = batch
        .column_by_name("item_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let titles = batch
        .column_by_name("title")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (
        (0..batch.num_rows())
            .map(|row| Uuid::from_slice(ids.value(row)).unwrap())
            .collect(),
        (0..batch.num_rows())
            .map(|row| titles.value(row).to_owned())
            .collect(),
    )
}

fn journey_batch(graph: &GraphForge) -> DecisionBatchV1 {
    let (items, titles) = selected_candidates(graph);
    let choices = Uuid::now_v7();
    let rubric = Uuid::now_v7();
    let review = Uuid::now_v7();
    let projection = serde_json::to_vec(&items).unwrap();
    let selection = serde_json::to_vec(&titles).unwrap();
    let mut projection_hash = Sha256::new();
    projection_hash.update(&projection);
    let mut selection_hash = Sha256::new();
    selection_hash.update(&selection);
    DecisionBatchV1 {
        input: DecisionInputIdentityV1 {
            generation_uuid: current_generation(graph),
            version_uuid: None,
            projection_sha256: projection_hash.finalize().into(),
            selection_sha256: selection_hash.finalize().into(),
            selected_item_uuids: items.iter().copied().collect(),
        },
        producer: producer(),
        questions: vec![
            DecisionQuestionV1 {
                question_uuid: choices,
                text: "Which queue should receive this candidate?".into(),
                item_uuids: items.clone(),
                kind: DecisionQuestionKindV1::Choice {
                    allowed_choices: vec!["research".into(), "human_review".into()],
                },
            },
            DecisionQuestionV1 {
                question_uuid: rubric,
                text: "How relevant is this evidence?".into(),
                item_uuids: items.clone(),
                kind: DecisionQuestionKindV1::RubricScore {
                    ordered_levels: vec!["low".into(), "medium".into(), "high".into()],
                },
            },
            DecisionQuestionV1 {
                question_uuid: review,
                text: "Does this work need human review?".into(),
                item_uuids: vec![],
                kind: DecisionQuestionKindV1::YesNoProbability,
            },
        ],
        results: vec![],
    }
}

fn callable_results(batch: &DecisionBatchV1) -> Vec<DecisionResultV1> {
    let choice = batch.questions[0].question_uuid;
    let rubric = batch.questions[1].question_uuid;
    let review = batch.questions[2].question_uuid;
    let items = batch
        .input
        .selected_item_uuids
        .iter()
        .copied()
        .collect::<Vec<_>>();
    let [mystery, voyage] = items.as_slice() else {
        unreachable!("fixture has two candidates")
    };
    vec![
        DecisionResultV1 {
            question_uuid: rubric,
            item_uuid: Some(*voyage),
            status: DecisionResultStatusV1::Answered,
            value: Some(DecisionValueV1::RubricScore("medium".into())),
            confidence: None,
        },
        DecisionResultV1 {
            question_uuid: choice,
            item_uuid: Some(*voyage),
            status: DecisionResultStatusV1::Uncertain,
            value: Some(DecisionValueV1::Choice("human_review".into())),
            confidence: None,
        },
        DecisionResultV1 {
            question_uuid: review,
            item_uuid: None,
            status: DecisionResultStatusV1::Answered,
            value: Some(DecisionValueV1::YesNoProbability {
                yes_probability: 0.25,
                no_probability: Some(0.75),
            }),
            confidence: None,
        },
        DecisionResultV1 {
            question_uuid: rubric,
            item_uuid: Some(*mystery),
            status: DecisionResultStatusV1::Answered,
            value: Some(DecisionValueV1::RubricScore("high".into())),
            confidence: None,
        },
        DecisionResultV1 {
            question_uuid: choice,
            item_uuid: Some(*mystery),
            status: DecisionResultStatusV1::Answered,
            value: Some(DecisionValueV1::Choice("research".into())),
            confidence: None,
        },
    ]
}

fn loaded_result_artifact(batch: &DecisionBatchV1) -> Vec<DecisionResultV1> {
    let artifact_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/decision-workflow/fixtures/independent-producer-results.json");
    let artifact: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(artifact_path).unwrap()).unwrap();
    let items = batch
        .input
        .selected_item_uuids
        .iter()
        .copied()
        .collect::<Vec<_>>();
    artifact
        .into_iter()
        .map(|row| {
            let question_uuid = match row["question"].as_str().unwrap() {
                "route" => batch.questions[0].question_uuid,
                "rubric" => batch.questions[1].question_uuid,
                "review" => batch.questions[2].question_uuid,
                other => panic!("unknown fixture question key {other}"),
            };
            let item_uuid = row["item_index"]
                .as_u64()
                .map(|index| items[index as usize]);
            DecisionResultV1 {
                question_uuid,
                item_uuid,
                status: serde_json::from_value(row["status"].clone()).unwrap(),
                value: Some(serde_json::from_value(row["value"].clone()).unwrap()),
                confidence: None,
            }
        })
        .collect()
}

fn enable_decision_action(graph: &mut GraphForge) {
    for capability_id in [
        CapabilityId::Provenance,
        CapabilityId::Knowledge,
        CapabilityId::Epistemic,
    ] {
        graph
            .enable_capability(EnableCapabilityRequest {
                context: context(),
                capability_id,
                capability_version: 1,
            })
            .unwrap();
    }
}

#[test]
fn composed_workflow_supports_independent_producers_explicit_action_and_replay() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("project");
    let mut graph = GraphForge::new(root.to_str()).unwrap();
    graph
        .execute("CREATE (:Candidate {title:'Mystery', evidence:'locked room', private_note:'omit'}), (:Candidate {title:'Voyage', evidence:'sea crossing', private_note:'omit'})")
        .unwrap();
    enable_decision_action(&mut graph);
    let batch = journey_batch(&graph);
    let observed_generation = batch.input.generation_uuid;
    assert!(
        !serde_json::to_string(&batch)
            .unwrap()
            .contains("private_note")
    );
    assert!(!serde_json::to_string(&batch).unwrap().contains("omit"));
    let selected = batch
        .input
        .selected_item_uuids
        .iter()
        .next()
        .copied()
        .unwrap();

    // Producer A is a caller-owned callable returning typed records.
    let mut from_callable = batch.clone();
    from_callable.producer.name = "caller callable fixture".into();
    from_callable.results = callable_results(&batch);
    let callable_result = from_callable.validate().unwrap();

    // Producer B is an independently loaded result artifact with different
    // answers. It shares only the public question/item identities.
    let artifact = loaded_result_artifact(&batch);
    let mut from_artifact = batch.clone();
    from_artifact.producer.name = "independent result artifact".into();
    from_artifact.results = artifact;
    let artifact_result = from_artifact.validate().unwrap();
    assert_eq!(callable_result.num_rows(), 5);
    assert_eq!(artifact_result.num_rows(), 5);
    assert_ne!(ipc_bytes(&callable_result), ipc_bytes(&artifact_result));

    let row = find_row(
        &callable_result,
        from_callable.questions[0].question_uuid,
        Some(selected),
    );
    assert_eq!(
        strings(&callable_result, "choice_value").value(row),
        "research"
    );
    let other_row = find_row(
        &artifact_result,
        from_artifact.questions[0].question_uuid,
        Some(selected),
    );
    assert_eq!(
        strings(&artifact_result, "choice_value").value(other_row),
        "human_review"
    );
    let artifact_review_row = find_row(
        &artifact_result,
        from_artifact.questions[2].question_uuid,
        None,
    );
    assert_eq!(
        artifact_result
            .column_by_name("yes_probability")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(artifact_review_row),
        0.85
    );
    let review = from_callable.questions[2].question_uuid;
    let review_row = find_row(&callable_result, review, None);
    assert_eq!(
        strings(&callable_result, "status").value(review_row),
        "answered"
    );
    assert_eq!(
        callable_result
            .column_by_name("yes_probability")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(review_row),
        0.25
    );

    // Uncertainty and malformed output are explicit. Neither path may change
    // the graph or grant canonical/Proposal authority.
    let uncertain = find_row(
        &callable_result,
        from_callable.questions[0].question_uuid,
        Some(*batch.input.selected_item_uuids.iter().nth(1).unwrap()),
    );
    assert_eq!(
        strings(&callable_result, "status").value(uncertain),
        "uncertain"
    );
    let mut partial = from_artifact.clone();
    let voyage = *batch.input.selected_item_uuids.iter().nth(1).unwrap();
    partial.results.retain(|result| {
        !(result.question_uuid == partial.questions[0].question_uuid
            && result.item_uuid == Some(voyage))
    });
    let unavailable = partial
        .results
        .iter_mut()
        .find(|result| {
            result.question_uuid == partial.questions[0].question_uuid
                && result.item_uuid == Some(selected)
        })
        .unwrap();
    unavailable.status = DecisionResultStatusV1::Unavailable;
    unavailable.value = None;
    let partial_arrow = partial.validate().unwrap();
    assert_eq!(partial_arrow.num_rows(), 5);
    assert_eq!(
        strings(&partial_arrow, "status").value(find_row(
            &partial_arrow,
            partial.questions[0].question_uuid,
            Some(selected),
        )),
        "unavailable"
    );
    assert_eq!(
        strings(&partial_arrow, "status").value(find_row(
            &partial_arrow,
            partial.questions[0].question_uuid,
            Some(voyage),
        )),
        "missing"
    );
    let mut malformed = from_artifact.clone();
    malformed.results.push(malformed.results[0].clone());
    assert!(malformed.validate().is_err());
    let mut oversized = batch.clone();
    let extra_items = (0..=DECISION_BATCH_MAX_ROWS)
        .map(|_| Uuid::now_v7())
        .collect::<Vec<_>>();
    oversized
        .input
        .selected_item_uuids
        .extend(extra_items.iter().copied());
    oversized.questions[0].item_uuids.extend(extra_items);
    assert!(oversized.validate().is_err());
    assert_eq!(current_generation(&graph), observed_generation);
    assert_eq!(
        graph
            .research_canonical_choices(&ResearchContext::Project, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        0
    );
    assert_eq!(
        graph
            .inspect_research_claims(&InspectResearchClaimsRequest {
                context: ResearchContext::Project,
                community_uuid: None,
                include_suppressed: false,
            })
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        0
    );

    // A changed live generation makes the old external answer stale.
    graph
        .execute("MATCH (c:Candidate) SET c.reviewed = true")
        .unwrap();
    let changed_generation = current_generation(&graph);
    assert_ne!(changed_generation, observed_generation);
    let stale_action = CreateResearchClaimRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: observed_generation,
        assertion_uuid: Uuid::now_v7(),
        claim: "route Mystery to research".into(),
        graph_refs: vec![AssertionGraphRefInput {
            graph_uuid: selected,
            graph_kind: GraphObjectKind::Node,
            role: AssertionGraphRole::Subject,
            ordinal: 0,
        }],
        category: ResearchCategory::Interpretation,
        creator_uuid: Uuid::now_v7(),
        run_uuid: None,
        created_at: 10,
    };
    assert!(
        graph
            .create_research_claim(&stale_action, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(current_generation(&graph), changed_generation);

    // Explicitly prepare against current state. Cancellation publishes
    // nothing; the same request then commits and its exact retry is durable.
    let fresh = journey_batch(&graph);
    let mut fresh_results = fresh.clone();
    fresh_results.producer.name = "caller callable fixture".into();
    fresh_results.results = callable_results(&fresh);
    let fresh_arrow = fresh_results.validate().unwrap();
    let reviewed_item = voyage;
    let route_row = find_row(
        &fresh_arrow,
        fresh.questions[0].question_uuid,
        Some(reviewed_item),
    );
    assert_eq!(
        strings(&fresh_arrow, "choice_value").value(route_row),
        "human_review"
    );
    assert_eq!(
        strings(&fresh_arrow, "status").value(route_row),
        "uncertain"
    );
    let action = CreateResearchClaimRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current_generation(&graph),
        assertion_uuid: Uuid::now_v7(),
        claim: "Human review explicitly overrides Voyage's uncertain route to research.".into(),
        graph_refs: vec![AssertionGraphRefInput {
            graph_uuid: reviewed_item,
            graph_kind: GraphObjectKind::Node,
            role: AssertionGraphRole::Subject,
            ordinal: 0,
        }],
        category: ResearchCategory::Interpretation,
        creator_uuid: Uuid::now_v7(),
        run_uuid: None,
        created_at: 11,
    };
    let before_cancel = current_generation(&graph);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert!(graph.create_research_claim(&action, &cancellation).is_err());
    assert_eq!(current_generation(&graph), before_cancel);
    let receipt = graph
        .create_research_claim(&action, &CancellationToken::new())
        .unwrap();
    assert_eq!(
        receipt
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    let receipt_bytes = ipc_bytes(&receipt.batches[0]);

    // Retain the validated Arrow result explicitly with the selected node as
    // derivation context and pin the resulting Project Version.
    let result_bytes = ipc_bytes(&fresh_arrow);
    let source_uuid = Uuid::now_v7();
    let artifact_uuid = Uuid::now_v7();
    graph
        .register_source(RegisterSourceRequest {
            context: context(),
            source_uuid,
            label: "Decision result fixture".into(),
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
            payload: ArtifactPayloadRequest::LocalBytes(result_bytes.clone()),
            derivation_inputs: vec![DerivationInput {
                input_uuid: selected,
                input_kind: DerivationSubjectKind::Node,
            }],
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

    let mut reopened = GraphForge::new(root.to_str()).unwrap();
    let view = reopened.open_research_version(retained_version).unwrap();
    let stored_payload = view.artifact_payload(artifact_uuid).unwrap();
    let stored_bytes = stored_payload.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0);
    assert_eq!(stored_bytes, result_bytes);
    let mut reader = StreamReader::try_new(Cursor::new(stored_bytes.to_vec()), None).unwrap();
    let retained_result = reader.next().unwrap().unwrap();
    assert_eq!(retained_result.num_rows(), 5);
    assert_eq!(
        retained_result
            .column_by_name("generation_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        fresh.input.generation_uuid.as_bytes()
    );
    assert_eq!(
        retained_result
            .column_by_name("projection_sha256")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        fresh.input.projection_sha256
    );
    assert_eq!(
        retained_result
            .column_by_name("selection_sha256")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        fresh.input.selection_sha256
    );
    assert_eq!(
        find_row(
            &retained_result,
            fresh.questions[0].question_uuid,
            Some(selected),
        ),
        find_row(
            &fresh_arrow,
            fresh.questions[0].question_uuid,
            Some(selected),
        )
    );

    let replay = reopened
        .create_research_claim(&action, &CancellationToken::new())
        .unwrap();
    assert_eq!(ipc_bytes(&replay.batches[0]), receipt_bytes);
    let mut changed_request = action.clone();
    changed_request.claim.push_str(" changed");
    assert!(
        reopened
            .create_research_claim(&changed_request, &CancellationToken::new())
            .is_err()
    );
    assert_eq!(
        reopened
            .inspect_research_claims(&InspectResearchClaimsRequest {
                context: ResearchContext::Project,
                community_uuid: None,
                include_suppressed: false,
            })
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        reopened
            .research_canonical_choices(&ResearchContext::Project, None)
            .unwrap()
            .batches
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        0
    );
}
