//! Executable release-workflow evidence for issue #2466.
//!
//! This is deliberately an opt-in example target. The bundle-local runner
//! invokes it on a developer or release-candidate machine; ordinary workspace
//! tests and the aggregate PR gate never execute the full workflow.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use arrow::array::{Array, FixedSizeBinaryArray, StringArray, TimestampMicrosecondArray};
use graphforge_api::{
    AdoptOntologyRequest, ApplyValidTimeRequest, AssertionGraphRefInput, AssertionGraphRole,
    AssertionStatus, CapabilityId, CreateAssertionRequest, CreateAssertionWithEvidenceRequest,
    CreateHypothesisGroupRequest, EnableCapabilityRequest, EvidenceInput, EvidenceRole,
    EvidenceSourceKind, FindOptions, GraphForge, GraphObjectKind, HypothesisMembershipAction,
    ListAssertionStatusRequest, ListAssertionSupersessionsRequest, ListHypothesisSelectionRequest,
    ListReasoningRequest, NodeSelector, OperationId, PageRequest, PathAlgorithm, PathsOptions,
    PropValue, RankAlgorithm, RankOptions, ReasoningContentFormat, ReasoningKind,
    RecordAssertionStatusRequest, RecordAssertionValidityRequest,
    RecordHypothesisMembershipRequest, RecordHypothesisSelectionRequest, RecordReasoningRequest,
    SearchIndexOptions, SupersedeAssertionRequest, WriteContext,
};
use graphforge_ontology::OntologyLoader;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

const BUNDLE: &str = "../../tests/release_workflows/probate-genealogy";

fn fixture(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(BUNDLE)
        .join(relative)
}

fn id(suffix: u16) -> Uuid {
    Uuid::parse_str(&format!("018f0f4e-7b8c-7000-8000-00000001{suffix:04x}")).unwrap()
}

fn context(suffix: u16) -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(id(suffix)),
        actor_uuid: Some(id(0xfffe)),
    }
}

fn enable(graph: &GraphForge, capability_id: CapabilityId, suffix: u16) {
    graph
        .enable_capability(EnableCapabilityRequest {
            context: context(suffix),
            capability_id,
            capability_version: 1,
        })
        .unwrap();
}

fn string_prop(value: &str) -> PropValue {
    PropValue::Str(value.to_owned())
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

fn recorded_at(result: &graphforge_api::ExecutionResult) -> i64 {
    result.batches[0]
        .column_by_name("recorded_at")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap()
        .value(0)
}

fn create_evidenced_assertion(
    graph: &GraphForge,
    assertion_suffix: u16,
    operation_suffix: u16,
    evidence_suffix: u16,
    source_suffix: u16,
    claim: &str,
    graph_uuid: Uuid,
) -> graphforge_api::ExecutionResult {
    graph
        .create_assertion_with_evidence(CreateAssertionWithEvidenceRequest {
            assertion: CreateAssertionRequest {
                context: context(operation_suffix),
                assertion_uuid: id(assertion_suffix),
                claim: claim.to_owned(),
                graph_refs: vec![AssertionGraphRefInput {
                    graph_uuid,
                    graph_kind: GraphObjectKind::Node,
                    role: AssertionGraphRole::Subject,
                    ordinal: 0,
                }],
            },
            evidence: vec![EvidenceInput {
                evidence_uuid: id(evidence_suffix),
                source_uuid: id(source_suffix),
                source_kind: EvidenceSourceKind::Document,
                role: EvidenceRole::Supports,
                weight: Some(0.75),
            }],
        })
        .unwrap()
}

fn reason(
    graph: &GraphForge,
    assertion_uuid: Uuid,
    provenance_uuid: Uuid,
    reasoning_suffix: u16,
    operation_suffix: u16,
    content: &str,
    supersedes: Option<Uuid>,
) {
    graph
        .record_reasoning(RecordReasoningRequest {
            context: context(operation_suffix),
            reasoning_uuid: id(reasoning_suffix),
            assertion_uuid,
            kind: ReasoningKind::EvidenceInterpretation,
            content_format: ReasoningContentFormat::TextPlain,
            content: content.as_bytes().to_vec(),
            supersedes_reasoning_uuid: supersedes,
            provenance_uuid,
        })
        .unwrap();
}

fn schema_signature(batch: &arrow::record_batch::RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| format!("{}:{:?}", field.name(), field.data_type()))
        .collect()
}

fn read_manifest() -> Value {
    serde_json::from_slice(&fs::read(fixture("scenario.yaml")).unwrap()).unwrap()
}

fn ontology_depth(name: &str, parents: &HashMap<&str, Option<&str>>) -> usize {
    parents
        .get(name)
        .and_then(|parent| *parent)
        .map_or(1, |parent| 1 + ontology_depth(parent, parents))
}


fn hex_sha256(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes.as_ref())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn main() {
    let manifest = read_manifest();
    assert_eq!(manifest["id"], "probate-genealogy");
    assert_eq!(manifest["seed"], 2_466_001);
    assert_eq!(manifest["steps"].as_array().unwrap().len(), 11);

    let ontology_path = fixture("ontologies/advisory-v1.yaml");
    let ontology = OntologyLoader::load_file(&ontology_path).unwrap();
    let metrics = &manifest["ontology_metrics"];
    assert_eq!(
        ontology.entity_types.len() as u64,
        metrics["entity_type_count"]
    );
    assert_eq!(
        ontology.relation_types.len() as u64,
        metrics["relation_type_count"]
    );
    assert_eq!(
        ontology.properties.len() as u64,
        metrics["property_definition_count"]
    );
    assert_eq!(
        ontology.constraints.len() as u64,
        metrics["constraint_count"]
    );
    assert_eq!(
        ontology
            .entity_types
            .iter()
            .filter(|item| item.r#abstract)
            .count() as u64,
        metrics["abstract_type_count"]
    );
    let parents = ontology
        .entity_types
        .iter()
        .map(|item| (item.name.as_str(), item.parent.as_deref()))
        .collect::<HashMap<_, _>>();
    let depths = ontology
        .entity_types
        .iter()
        .map(|item| ontology_depth(&item.name, &parents))
        .collect::<Vec<_>>();
    assert_eq!(
        *depths.iter().max().unwrap() as u64,
        metrics["inheritance_max_depth"]
    );
    let widest_level = depths
        .iter()
        .copied()
        .map(|depth| depths.iter().filter(|value| **value == depth).count())
        .max()
        .unwrap();
    assert_eq!(widest_level as u64, metrics["inheritance_breadth"]);
    let parent_names = ontology
        .entity_types
        .iter()
        .filter_map(|item| item.parent.as_deref())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        ontology
            .entity_types
            .iter()
            .filter(|item| !parent_names.contains(item.name.as_str()))
            .count() as u64,
        metrics["leaf_count"]
    );
    assert_eq!(
        ontology
            .properties
            .iter()
            .filter(|item| item.nullable)
            .count() as u64,
        metrics["nullable_count"]
    );
    assert_eq!(
        ontology
            .properties
            .iter()
            .filter(|item| item.multivalued)
            .count() as u64,
        metrics["multivalue_count"]
    );
    assert_eq!(
        ontology
            .properties
            .iter()
            .filter(|item| item.default_json.is_some())
            .count() as u64,
        metrics["default_count"]
    );
    let ontology_json = serde_json::to_value(&ontology).unwrap();
    let value_types = ontology_json["properties"]
        .as_array()
        .unwrap()
        .iter()
        .map(|property| property["type"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        value_types,
        metrics["value_type_mix"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect()
    );
    let constraint_kinds = ontology_json["constraints"]
        .as_array()
        .unwrap()
        .iter()
        .map(|constraint| constraint["kind"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        constraint_kinds,
        metrics["constraint_kinds"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect()
    );
    let semantic_flag_count = ontology
        .relation_types
        .iter()
        .map(|relation| {
            let flags = &relation.semantic;
            [
                flags.transitive,
                flags.symmetric,
                flags.reflexive,
                flags.functional,
                flags.inverse_functional,
                flags.acyclic,
            ]
            .into_iter()
            .filter(|enabled| *enabled)
            .count()
        })
        .sum::<usize>();
    assert_eq!(semantic_flag_count as u64, metrics["semantic_flag_count"]);
    let compiled = graphforge_ontology::OntologyCompiler::compile(&ontology).unwrap();
    let ontology_fingerprint = graphforge_ontology::OntologyHandle::new(compiled)
        .checksum()
        .to_owned();

    let project = TempDir::new().unwrap();
    let project_path = project.path().to_str().unwrap();
    let mut graph = GraphForge::new(Some(project_path)).unwrap();
    graph
        .adopt_ontology(AdoptOntologyRequest {
            context: context(0x1000),
            path: ontology_path.clone(),
            mode: graphforge_api::OntologyMode::Advisory,
        })
        .unwrap();
    assert_eq!(format!("{:?}", graph.ontology_mode()), "Advisory");
    drop(graph);
    let graph = GraphForge::new(Some(project_path)).unwrap();
    assert_eq!(format!("{:?}", graph.ontology_mode()), "Advisory");

    // PG-01: deterministic synthetic graph. The incomplete birth record is
    // intentionally retained in advisory mode and represented in expected evidence.
    let ada = graph
        .add_node(
            "Person",
            &HashMap::from([
                ("name".into(), string_prop("Ada North")),
                ("birth_year".into(), PropValue::Int(1912)),
                (
                    "aliases".into(),
                    PropValue::List(vec![string_prop("Ada N.")]),
                ),
            ]),
        )
        .unwrap();
    let bea = graph
        .add_node(
            "Person",
            &HashMap::from([
                ("name".into(), string_prop("Bea North")),
                ("birth_year".into(), PropValue::Int(1888)),
            ]),
        )
        .unwrap();
    let cora = graph
        .add_node(
            "Person",
            &HashMap::from([
                ("name".into(), string_prop("Cora Vale")),
                ("birth_year".into(), PropValue::Int(1889)),
            ]),
        )
        .unwrap();
    let drew = graph
        .add_node(
            "Person",
            &HashMap::from([
                ("name".into(), string_prop("Drew Vale")),
                ("birth_year".into(), PropValue::Int(1910)),
            ]),
        )
        .unwrap();
    let birth_record = graph
        .add_node(
            "BirthRecord",
            &HashMap::from([("title".into(), string_prop("County birth register 1912"))]),
        )
        .unwrap();
    let late_record = graph
        .add_node(
            "BirthRecord",
            &HashMap::from([
                ("title".into(), string_prop("Late parish register 1911")),
                ("recorded_on".into(), string_prop("1979-01-01T00:00:00Z")),
                ("verified".into(), PropValue::Bool(true)),
            ]),
        )
        .unwrap();
    let will = graph
        .add_node(
            "Will",
            &HashMap::from([("title".into(), string_prop("Estate will 1978"))]),
        )
        .unwrap();
    let household = graph
        .add_node(
            "Household",
            &HashMap::from([("name".into(), string_prop("North household"))]),
        )
        .unwrap();
    let estate = graph
        .add_node(
            "Estate",
            &HashMap::from([
                ("name".into(), string_prop("Ada North estate")),
                ("value".into(), PropValue::Float(125_000.0)),
            ]),
        )
        .unwrap();
    graph
        .execute(
            "MATCH (a:Person {name:'Ada North'}), (b:Person {name:'Bea North'}), \
             (c:Person {name:'Cora Vale'}) \
             CREATE (b)-[:PARENT_OF]->(a), (c)-[:PARENT_OF]->(a)",
        )
        .unwrap();
    graph
        .add_edge(&ada, "MEMBER_OF", &household, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&birth_record, "MENTIONS", &ada, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&late_record, "MENTIONS", &cora, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&ada, "BENEFICIARY_OF", &estate, &HashMap::new())
        .unwrap();
    let _ = (drew, will);

    // PG-02: query -> record search -> rank -> kinship path.
    let people = graph
        .execute("MATCH (p:Person) RETURN p.name AS name, p.birth_year AS birth_year ORDER BY name")
        .unwrap();
    assert_eq!(people.stats.rows_produced, 4);
    assert_eq!(
        people.batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| value.unwrap().to_owned())
            .collect::<Vec<_>>(),
        ["Ada North", "Bea North", "Cora Vale", "Drew Vale"]
    );
    assert_eq!(
        people.schema.metadata()["graphforge.ontology_mode"],
        "advisory"
    );
    assert!(
        people.schema.metadata()["graphforge.ontology_version"].contains(&ontology_fingerprint)
    );
    graph
        .index_search(
            "BirthRecord",
            SearchIndexOptions::Text {
                properties: Some(vec!["title".into()]),
                rebuild: false,
            },
        )
        .unwrap();
    let found = graph
        .find(FindOptions {
            query: Some("parish".into()),
            label: Some("BirthRecord".into()),
            limit: 5,
            ..FindOptions::default()
        })
        .unwrap();
    assert_eq!(found.num_rows(), 1);
    assert_eq!(
        found
            .column_by_name("title")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "Late parish register 1911"
    );
    let ranked = graph
        .rank(
            "Person",
            RankOptions {
                by: RankAlgorithm::Degree,
                via: Some("PARENT_OF".into()),
                directed: false,
                write_property: None,
            },
        )
        .unwrap();
    assert_eq!(ranked.num_rows(), 4);
    let path = graph
        .paths(
            Some(&NodeSelector::Handle(bea.clone())),
            Some(&NodeSelector::Handle(ada.clone())),
            PathsOptions {
                by: PathAlgorithm::Bfs,
                via: Some("PARENT_OF".into()),
                directed: false,
                ..PathsOptions::default()
            },
        )
        .unwrap();
    assert_eq!(path.num_rows(), 1);

    for (capability, suffix) in [
        (CapabilityId::Provenance, 0x1010),
        (CapabilityId::Knowledge, 0x1011),
        (CapabilityId::Epistemic, 0x1012),
        (CapabilityId::ValidTime, 0x1013),
    ] {
        enable(&graph, capability, suffix);
    }

    // PG-03: alternatives are evidence-backed and explicitly statused hypotheses.
    let first = create_evidenced_assertion(
        &graph,
        0x1100,
        0x1101,
        0x1102,
        0x1103,
        "Bea North is Ada North's recorded parent",
        ada.uuid,
    );
    let second = create_evidenced_assertion(
        &graph,
        0x1110,
        0x1111,
        0x1112,
        0x1113,
        "Cora Vale is Ada North's recorded parent",
        ada.uuid,
    );
    let first_provenance = provenance(&first);
    let second_provenance = provenance(&second);
    for (assertion, provenance_uuid, event, operation) in [
        (id(0x1100), first_provenance, 0x1120, 0x1121),
        (id(0x1110), second_provenance, 0x1122, 0x1123),
    ] {
        graph
            .record_assertion_status(RecordAssertionStatusRequest {
                context: context(operation),
                status_event_uuid: id(event),
                assertion_uuid: assertion,
                status: AssertionStatus::Hypothesis,
                confidence_uuid: None,
                reasoning_uuid: None,
                provenance_uuid,
            })
            .unwrap();
    }
    reason(
        &graph,
        id(0x1100),
        first_provenance,
        0x1130,
        0x1131,
        "County register initially favors Bea; this is a working interpretation.",
        None,
    );
    reason(
        &graph,
        id(0x1110),
        second_provenance,
        0x1132,
        0x1133,
        "Late parish register makes Cora independently plausible.",
        None,
    );
    let group = id(0x1140);
    graph
        .create_hypothesis_group(CreateHypothesisGroupRequest {
            context: context(0x1141),
            group_uuid: group,
            question_key: "probate.ada-parentage.v1".into(),
            provenance_uuid: first_provenance,
        })
        .unwrap();
    for (assertion, reasoning, provenance_uuid, event, operation) in [
        (id(0x1100), id(0x1130), first_provenance, 0x1150, 0x1151),
        (id(0x1110), id(0x1132), second_provenance, 0x1152, 0x1153),
    ] {
        graph
            .record_hypothesis_membership(&RecordHypothesisMembershipRequest {
                context: context(operation),
                membership_event_uuid: id(event),
                group_uuid: group,
                assertion_uuid: assertion,
                action: HypothesisMembershipAction::Added,
                reasoning_uuid: reasoning,
                provenance_uuid,
            })
            .unwrap();
    }
    assert_eq!(
        graph.hypothesis_selection(group).unwrap().batches[0].num_rows(),
        0
    );

    // PG-04 through PG-06: select, amend/change, and clear are three immutable events.
    graph
        .record_hypothesis_selection(&RecordHypothesisSelectionRequest {
            context: context(0x1160),
            selection_event_uuid: id(0x1161),
            group_uuid: group,
            selected_assertion_uuid: Some(id(0x1100)),
            reasoning_uuid: id(0x1130),
            provenance_uuid: first_provenance,
        })
        .unwrap();
    reason(
        &graph,
        id(0x1110),
        second_provenance,
        0x1170,
        0x1171,
        "Amended after inspecting the backdated parish record; still not objective truth.",
        Some(id(0x1132)),
    );
    graph
        .record_hypothesis_selection(&RecordHypothesisSelectionRequest {
            context: context(0x1172),
            selection_event_uuid: id(0x1173),
            group_uuid: group,
            selected_assertion_uuid: Some(id(0x1110)),
            reasoning_uuid: id(0x1170),
            provenance_uuid: second_provenance,
        })
        .unwrap();
    graph
        .record_hypothesis_selection(&RecordHypothesisSelectionRequest {
            context: context(0x1180),
            selection_event_uuid: id(0x1181),
            group_uuid: group,
            selected_assertion_uuid: None,
            reasoning_uuid: id(0x1170),
            provenance_uuid: second_provenance,
        })
        .unwrap();
    assert_eq!(
        graph.hypothesis_members(group).unwrap().batches[0].num_rows(),
        2
    );
    assert_eq!(
        graph.hypothesis_selection(group).unwrap().batches[0].num_rows(),
        1
    );
    let selections = graph
        .list_hypothesis_selection(&ListHypothesisSelectionRequest {
            group_uuid: Some(group),
            page: PageRequest::default(),
        })
        .unwrap();
    assert_eq!(selections.batches[0].num_rows(), 3);
    let current_selection = graph.hypothesis_selection(group).unwrap();
    assert!(
        current_selection.batches[0]
            .column_by_name("selected_assertion_uuid")
            .unwrap()
            .is_null(0)
    );
    for assertion_uuid in [id(0x1100), id(0x1110)] {
        let statuses = graph
            .list_assertion_status(ListAssertionStatusRequest {
                assertion_uuid: Some(assertion_uuid),
                page: PageRequest::default(),
            })
            .unwrap();
        assert_eq!(statuses.batches[0].num_rows(), 1);
        assert_eq!(
            statuses.batches[0]
                .column_by_name("status")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "hypothesis",
            "unselected alternatives must not be coerced to false"
        );
    }
    assert_eq!(
        graph
            .list_reasoning(ListReasoningRequest {
                assertion_uuid: Some(id(0x1110)),
                page: PageRequest::default(),
            })
            .unwrap()
            .batches[0]
            .num_rows(),
        2
    );

    // Capture the immutable transaction-time view before the correction.
    let cutoff = selections.batches[0]
        .column_by_name("recorded_at")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap()
        .iter()
        .flatten()
        .max()
        .unwrap();
    let before_correction = graph.epistemic_snapshot(cutoff).unwrap();
    let valid_before = graph
        .apply_valid_time(ApplyValidTimeRequest {
            transaction_cutoff_micros: cutoff,
            valid_time_micros: -1_861_920_000_000_000,
        })
        .unwrap();

    // PG-07: correct a misattributed document association through immutable supersession.
    let misattributed = create_evidenced_assertion(
        &graph,
        0x1200,
        0x1201,
        0x1202,
        0x1203,
        "County birth register 1912 is associated with Bea North",
        birth_record.uuid,
    );
    let misattributed_provenance = provenance(&misattributed);
    let corrected = create_evidenced_assertion(
        &graph,
        0x1210,
        0x1211,
        0x1212,
        0x1213,
        "County birth register 1912 is associated with Cora Vale",
        birth_record.uuid,
    );
    let corrected_provenance = provenance(&corrected);
    reason(
        &graph,
        id(0x1200),
        misattributed_provenance,
        0x1220,
        0x1221,
        "The original association used the wrong handwritten surname.",
        None,
    );
    graph
        .supersede_assertion(SupersedeAssertionRequest {
            context: context(0x1222),
            supersession_uuid: id(0x1223),
            prior_assertion_uuid: id(0x1200),
            replacement_assertion_uuid: id(0x1210),
            status_event_uuid: id(0x1224),
            reasoning_uuid: id(0x1220),
            provenance_uuid: misattributed_provenance,
        })
        .unwrap();

    // PG-08: a late/backdated interpretation is an append-only valid-time event.
    let validity = graph
        .record_assertion_validity(RecordAssertionValidityRequest {
            context: context(0x1230),
            validity_event_uuid: id(0x1231),
            assertion_uuid: id(0x1210),
            valid_from_micros: Some(-1_861_920_000_000_000),
            valid_to_micros: Some(-1_830_384_000_000_000),
            reasoning_uuid: None,
            provenance_uuid: corrected_provenance,
        })
        .unwrap();
    assert!(recorded_at(&validity) > cutoff);

    // PG-09: the prior cutoff is byte-for-byte stable after later writes.
    assert_eq!(
        graph.epistemic_snapshot(cutoff).unwrap().batches,
        before_correction.batches
    );
    assert_eq!(
        graph
            .apply_valid_time(ApplyValidTimeRequest {
                transaction_cutoff_micros: cutoff,
                valid_time_micros: -1_861_920_000_000_000,
            })
            .unwrap()
            .batches,
        valid_before.batches
    );
    let current_valid = graph
        .apply_valid_time(ApplyValidTimeRequest {
            transaction_cutoff_micros: i64::MAX,
            valid_time_micros: -1_861_920_000_000_000,
        })
        .unwrap();
    let valid_rows = current_valid.batches[0]
        .column_by_name("assertion_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let validity_flags = current_valid.batches[0]
        .column_by_name("is_valid")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .unwrap();
    let corrected_row = (0..valid_rows.len())
        .find(|row| valid_rows.value(*row) == id(0x1210).as_bytes())
        .unwrap();
    assert!(validity_flags.value(corrected_row));

    let final_summary = json!({
        "people": 4,
        "search_title": "Late parish register 1911",
        "path_rows": path.num_rows(),
        "hypothesis_members": 2,
        "selection_events": 3,
        "current_selection": Value::Null,
        "supersessions": graph.list_assertion_supersessions(ListAssertionSupersessionsRequest {
            prior_assertion_uuid: Some(id(0x1200)),
            replacement_assertion_uuid: None,
            page: PageRequest::default(),
        }).unwrap().batches[0].num_rows(),
        "ontology_fingerprint": ontology_fingerprint,
        "person_schema": schema_signature(&people.batches[0]),
        "find_schema": schema_signature(&found),
        "selection_schema": schema_signature(&selections.batches[0]),
        "stable_assertion_uuids": [id(0x1100), id(0x1110), id(0x1200), id(0x1210)],
    });
    let canonical = serde_json::to_vec(&final_summary).unwrap();
    let final_fingerprint = hex_sha256(&canonical);
    let arrow_expectations: Value =
        serde_json::from_slice(&fs::read(fixture("expected/arrow-fingerprints.json")).unwrap())
            .unwrap();
    assert_eq!(
        final_summary["person_schema"],
        arrow_expectations["schemas"]["person_query"]
    );
    assert_eq!(
        final_summary["find_schema"],
        arrow_expectations["schemas"]["find"]
    );
    assert_eq!(
        final_summary["selection_schema"],
        arrow_expectations["schemas"]["hypothesis_selection"]
    );
    assert_eq!(
        final_fingerprint,
        arrow_expectations["normalized_final_fingerprint"]
    );

    // PG-10: close/reopen preserves public UUIDs, histories, and projections.
    let person_ids = [
        ada.uuid,
        bea.uuid,
        cora.uuid,
        birth_record.uuid,
        late_record.uuid,
    ];
    drop(graph);
    let reopened = GraphForge::new(Some(project_path)).unwrap();
    assert_eq!(
        reopened.epistemic_snapshot(cutoff).unwrap().batches,
        before_correction.batches
    );
    assert_eq!(
        reopened.hypothesis_members(group).unwrap().batches[0].num_rows(),
        2
    );
    assert!(
        reopened.hypothesis_selection(group).unwrap().batches[0]
            .column_by_name("selected_assertion_uuid")
            .unwrap()
            .is_null(0)
    );
    assert_eq!(
        reopened
            .execute("MATCH (p:Person) RETURN p.node_uuid AS id ORDER BY id")
            .unwrap()
            .stats
            .rows_produced,
        4
    );
    for uuid in person_ids {
        let rows = reopened.list_assertions(graphforge_api::ListAssertionsRequest {
            graph_uuid: Some(uuid),
            page: PageRequest::default(),
        });
        assert!(
            rows.is_ok(),
            "persisted graph UUID must remain a valid public selector"
        );
    }
    assert_eq!(
        reopened
            .apply_valid_time(ApplyValidTimeRequest {
                transaction_cutoff_micros: i64::MAX,
                valid_time_micros: -1_861_920_000_000_000,
            })
            .unwrap()
            .batches,
        current_valid.batches
    );
    assert_eq!(
        reopened
            .find(FindOptions {
                query: Some("parish".into()),
                label: Some("BirthRecord".into()),
                limit: 5,
                ..FindOptions::default()
            })
            .unwrap(),
        found
    );

    if let Ok(evidence_path) = std::env::var("GF_PROBATE_EVIDENCE_PATH") {
        let evidence = json!({
            "schema_version": 1,
            "scenario_id": "probate-genealogy",
            "commit_sha": std::env::var("GF_RELEASE_WORKFLOW_SHA").unwrap_or_else(|_| "unknown".into()),
            "seed": 2_466_001,
            "rust_authoritative": true,
            "step_ids": manifest["steps"],
            "ontology_fingerprint": final_summary["ontology_fingerprint"],
            "fixture_fingerprint": hex_sha256(&fs::read(fixture("generator.yaml")).unwrap()),
            "normalized_final_fingerprint": final_fingerprint,
            "source_uuids": [id(0x1103), id(0x1113), id(0x1203), id(0x1213)],
            "assertion_uuids": final_summary["stable_assertion_uuids"],
            "transaction_cutoff": cutoff,
            "valid_time": -1_861_920_000_000_000i64,
            "prior_transaction_view_unchanged": true,
            "reopen_identical": true,
            "outcome": final_summary,
        });
        fs::write(evidence_path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
    }
}
