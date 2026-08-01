//! Opt-in executable evidence for issue #2469.
//!
//! Proves ontology-free exploration, session-scoped advisory formalization, and
//! curated handoff into a separate strict target through public bulk and
//! incremental construction APIs.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Float64Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use graphforge_api::{
    AdoptOntologyRequest, AnalyzeOptions, AssertionGraphRefInput, AssertionGraphRole,
    AttachEvidenceRequest, CapabilityId, ClusterOptions, CreateAssertionRequest,
    CreateHypothesisGroupRequest, EnableCapabilityRequest, EvidenceRole, EvidenceSourceKind,
    FindOptions, GraphForge, GraphObjectKind, HypothesisMembershipAction, OperationId, PropValue,
    RankAlgorithm, RankOptions, ReasoningContentFormat, ReasoningKind,
    RecordHypothesisMembershipRequest, RecordHypothesisSelectionRequest, RecordReasoningRequest,
    SearchIndexOptions, WriteContext, bulk_edge_input_schema, bulk_node_input_schema,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use uuid::Uuid;

const BUNDLE: &str = "../../tests/release_workflows/ontology-emergence-strict-handoff";
const APPROVAL: &str = "018f0f4e-7b8c-7000-8000-00000000a001";

fn fixture(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(BUNDLE)
        .join(relative)
}

fn id(suffix: u16) -> Uuid {
    Uuid::parse_str(&format!("018f0f4e-7b8c-7000-8000-00000002{suffix:04x}")).unwrap()
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

fn uuid_column(values: &[Uuid]) -> FixedSizeBinaryArray {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        builder.append_value(value.as_bytes()).unwrap();
    }
    builder.finish()
}

fn count_label(graph: &GraphForge, label: &str) -> usize {
    graph
        .execute(&format!(
            "MATCH (n:{label}) RETURN n.node_uuid AS node_uuid"
        ))
        .unwrap()
        .stats
        .rows_produced as usize
}

fn count_rel(graph: &GraphForge, rel: &str) -> usize {
    graph
        .execute(&format!("MATCH ()-[r:{rel}]->() RETURN r"))
        .unwrap()
        .stats
        .rows_produced as usize
}

fn total_nodes(graph: &GraphForge) -> usize {
    graph
        .execute("MATCH (n) RETURN n.node_uuid AS node_uuid")
        .unwrap()
        .stats
        .rows_produced as usize
}

fn total_edges(graph: &GraphForge) -> usize {
    graph
        .execute("MATCH ()-[r]->() RETURN r")
        .unwrap()
        .stats
        .rows_produced as usize
}

fn catalog_snapshot(graph: &GraphForge) -> (Vec<String>, Vec<String>, usize) {
    let catalog = graph.runtime_catalog();
    let guard = catalog.lock().unwrap();
    let mut entities = guard
        .entity_types()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    entities.sort();
    let mut relations = guard
        .relation_types()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    relations.sort();
    let properties = guard.property_names().count();
    (entities, relations, properties)
}

fn density(nodes: usize, edges: usize) -> f64 {
    if nodes < 2 {
        return 0.0;
    }
    let possible = nodes * (nodes - 1);
    (edges as f64) / (possible as f64)
}

fn fingerprint_json(value: &serde_json::Value) -> String {
    let digest = Sha256::digest(serde_json::to_vec(value).unwrap());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn publish_bulk_source(graph: &GraphForge) -> Vec<Uuid> {
    let node_ids = (0..12_u16)
        .map(|index| id(0x0100 + index))
        .collect::<Vec<_>>();
    let labels = [
        "Host",
        "Host",
        "Host",
        "Host",
        "Address",
        "Address",
        "Address",
        "Address",
        "Service",
        "Service",
        "Observation",
        "Observation",
    ];
    let names = [
        Some("edge-gw-01"),
        Some("edge-gw-02"),
        Some("lab-host-01"),
        Some("lab-host-02"),
        None,
        None,
        None,
        None,
        Some("ssh"),
        Some("https"),
        Some("scan-batch-a"),
        Some("scan-batch-b"),
    ];
    let values = [
        None,
        None,
        None,
        None,
        Some("10.0.0.10"),
        Some("10.0.0.11"),
        Some("10.0.1.20"),
        Some("10.0.1.21"),
        None,
        None,
        None,
        None,
    ];
    let ports = [
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(22_i64),
        Some(443),
        None,
        None,
    ];
    let risk = [
        Some(0.4_f64),
        Some(0.55),
        Some(0.2),
        Some(0.25),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ];
    let confidence = [
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(0.7_f64),
        Some(0.65),
    ];

    let schema = bulk_node_input_schema(vec![
        Field::new("confidence", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("port", DataType::Int64, true),
        Field::new("risk_score", DataType::Float64, true),
        Field::new("value", DataType::Utf8, true),
    ])
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(uuid_column(&node_ids)),
            Arc::new(StringArray::from(labels.to_vec())),
            Arc::new(Float64Array::from(confidence.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Int64Array::from(ports.to_vec())),
            Arc::new(Float64Array::from(risk.to_vec())),
            Arc::new(StringArray::from(values.to_vec())),
        ],
    )
    .unwrap();
    let receipt = graph
        .publish_bulk_nodes(OperationId(id(0x0200)), &[batch])
        .unwrap();
    assert_eq!(receipt.num_rows(), 12);

    let edges = [
        (0_usize, 4_usize, "RESOLVES_TO"),
        (1, 5, "RESOLVES_TO"),
        (2, 6, "RESOLVES_TO"),
        (0, 8, "EXPOSES"),
        (1, 9, "EXPOSES"),
        (10, 0, "OBSERVED_ON"),
    ];
    let edge_schema = bulk_edge_input_schema(Vec::new()).unwrap();
    let mut edge_ids = FixedSizeBinaryBuilder::with_capacity(edges.len(), 16);
    let mut sources = FixedSizeBinaryBuilder::with_capacity(edges.len(), 16);
    let mut targets = FixedSizeBinaryBuilder::with_capacity(edges.len(), 16);
    let mut rels = Vec::with_capacity(edges.len());
    for (index, (src, dst, rel)) in edges.iter().enumerate() {
        edge_ids
            .append_value(id(0x0300 + index as u16).as_bytes())
            .unwrap();
        sources.append_value(node_ids[*src].as_bytes()).unwrap();
        targets.append_value(node_ids[*dst].as_bytes()).unwrap();
        rels.push(*rel);
    }
    let edge_batch = RecordBatch::try_new(
        edge_schema,
        vec![
            Arc::new(edge_ids.finish()),
            Arc::new(StringArray::from(rels)),
            Arc::new(sources.finish()),
            Arc::new(targets.finish()),
        ],
    )
    .unwrap();
    graph
        .publish_bulk_edges(OperationId(id(0x0201)), &[edge_batch])
        .unwrap();
    node_ids
}

fn enrich_source(graph: &GraphForge, _hosts: &[Uuid]) {
    let note_a = graph
        .add_node(
            "AnalystNote",
            &HashMap::from([
                ("body".into(), PropValue::Str("bridge lab and edge".into())),
                ("severity".into(), PropValue::Int(2)),
            ]),
        )
        .unwrap();
    let note_b = graph
        .add_node(
            "AnalystNote",
            &HashMap::from([
                ("body".into(), PropValue::Str("https exposure".into())),
                ("severity".into(), PropValue::Int(3)),
            ]),
        )
        .unwrap();
    let orphan = graph
        .add_node(
            "Host",
            &HashMap::from([
                ("name".into(), PropValue::Str("orphan-host".into())),
                ("risk_score".into(), PropValue::Float(0.1)),
            ]),
        )
        .unwrap();
    let addr = graph
        .add_node(
            "Address",
            &HashMap::from([("value".into(), PropValue::Str("10.0.2.50".into()))]),
        )
        .unwrap();
    let svc = graph
        .add_node(
            "Service",
            &HashMap::from([
                ("name".into(), PropValue::Str("dns".into())),
                ("port".into(), PropValue::Int(53)),
            ]),
        )
        .unwrap();
    let obs = graph
        .add_node(
            "Observation",
            &HashMap::from([
                ("name".into(), PropValue::Str("scan-batch-c".into())),
                ("confidence".into(), PropValue::Float(0.8)),
            ]),
        )
        .unwrap();

    graph
        .add_edge(&note_a, "MENTIONS", &orphan, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&note_b, "MENTIONS", &orphan, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&orphan, "RESOLVES_TO", &addr, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&orphan, "EXPOSES", &svc, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&obs, "OBSERVED_ON", &orphan, &HashMap::new())
        .unwrap();
    graph
        .add_edge(&note_a, "CORRELATES_WITH", &note_b, &HashMap::new())
        .unwrap();

    for (left, right) in [
        ("edge-gw-01", "lab-host-01"),
        ("edge-gw-02", "lab-host-02"),
        ("edge-gw-01", "edge-gw-02"),
        ("lab-host-01", "lab-host-02"),
    ] {
        graph
            .execute(&format!(
                "MATCH (a:Host {{name:'{left}'}}), (b:Host {{name:'{right}'}}) \
                 CREATE (a)-[:CORRELATES_WITH]->(b)"
            ))
            .unwrap();
    }

    for host in [
        "edge-gw-01",
        "edge-gw-02",
        "lab-host-01",
        "lab-host-02",
        "orphan-host",
    ] {
        graph
            .execute(&format!(
                "MATCH (o:Observation {{name:'scan-batch-c'}}), (h:Host {{name:'{host}'}}) \
                 CREATE (o)-[:OBSERVED_ON]->(h)"
            ))
            .unwrap();
    }
    graph
        .execute(
            "MATCH (n:AnalystNote {body:'bridge lab and edge'}), (h:Host {name:'edge-gw-01'}) \
             CREATE (n)-[:MENTIONS]->(h)",
        )
        .unwrap();
}

fn named_host_uuid(graph: &GraphForge, name: &str) -> Uuid {
    let result = graph
        .execute(&format!(
            "MATCH (h:Host {{name:'{name}'}}) RETURN h.node_uuid AS node_uuid"
        ))
        .unwrap();
    let values = result.batches[0]
        .column_by_name("node_uuid")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    Uuid::from_slice(values.value(0)).unwrap()
}

fn provenance_uuid(result: &graphforge_api::ExecutionResult) -> Uuid {
    let values = result.batches[0]
        .column_by_name("provenance_uuid")
        .expect("assertion provenance")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("provenance UUID array");
    Uuid::from_slice(values.value(0)).unwrap()
}

#[allow(
    clippy::too_many_lines,
    reason = "one release workflow is one auditable story"
)]
fn main() {
    let sha = std::env::var("GRAPHFORGE_WORKFLOW_SHA").expect("workflow SHA is required");
    let evidence_path = PathBuf::from(
        std::env::var("GRAPHFORGE_WORKFLOW_EVIDENCE").expect("evidence path is required"),
    );

    let _temporary = TempDir::new().unwrap();
    let root = std::env::var("GRAPHFORGE_WORKFLOW_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| _temporary.path().to_path_buf());
    let source_path = root.join("source");
    let target_path = root.join("target");
    fs::create_dir_all(&source_path).unwrap();
    fs::create_dir_all(&target_path).unwrap();

    // OEH-01
    let source = GraphForge::new(Some(source_path.to_str().unwrap())).unwrap();
    assert_eq!(
        source.ontology_mode(),
        graphforge_api::OntologyMode::Exploratory
    );
    assert_eq!(total_nodes(&source), 0);
    assert_eq!(total_edges(&source), 0);
    let empty_catalog = catalog_snapshot(&source);

    // OEH-02
    let bulk_ids = publish_bulk_source(&source);
    assert_eq!(total_nodes(&source), 12);
    assert_eq!(total_edges(&source), 6);
    let after_bulk_catalog = catalog_snapshot(&source);
    assert!(after_bulk_catalog.0.len() > empty_catalog.0.len());

    // OEH-03
    enrich_source(&source, &bulk_ids);
    let enriched_nodes = total_nodes(&source);
    let enriched_edges = total_edges(&source);
    assert!(
        enriched_nodes >= 18,
        "expected XS->S growth, got {enriched_nodes}"
    );
    assert!(
        enriched_edges >= 22,
        "expected connected enrichment, got {enriched_edges}"
    );
    let after_enrich_catalog = catalog_snapshot(&source);
    assert!(after_enrich_catalog.0.contains(&"AnalystNote".to_owned()));
    assert!(
        after_enrich_catalog
            .1
            .contains(&"CORRELATES_WITH".to_owned())
    );
    assert!(after_enrich_catalog.1.contains(&"MENTIONS".to_owned()));

    // OEH-04: reopen preserves catalog growth without ontology.
    drop(source);
    let mut source = GraphForge::new(Some(source_path.to_str().unwrap())).unwrap();
    assert_eq!(
        source.ontology_mode(),
        graphforge_api::OntologyMode::Exploratory
    );
    assert_eq!(total_nodes(&source), enriched_nodes);
    assert_eq!(total_edges(&source), enriched_edges);
    let reopened_catalog = catalog_snapshot(&source);
    assert_eq!(reopened_catalog.0, after_enrich_catalog.0);
    assert_eq!(reopened_catalog.1, after_enrich_catalog.1);

    // OEH-05
    source
        .index_search(
            "Host",
            SearchIndexOptions::Text {
                properties: Some(vec!["name".into()]),
                rebuild: true,
            },
        )
        .unwrap();
    let scope = source
        .execute("MATCH (h:Host) RETURN h.name AS name, h.node_uuid AS node_uuid ORDER BY name")
        .unwrap();
    let search = source
        .find(FindOptions {
            query: Some("edge".into()),
            label: Some("Host".into()),
            limit: 10,
            ..FindOptions::default()
        })
        .unwrap();
    assert!(search.num_rows() >= 1);
    let rank = source
        .rank(
            "Host",
            RankOptions {
                by: RankAlgorithm::Degree,
                via: None,
                directed: false,
                write_property: None,
            },
        )
        .unwrap();
    assert!(rank.num_rows() >= 1);
    let clusters = source
        .cluster(
            "Host",
            ClusterOptions {
                via: Some("CORRELATES_WITH".into()),
                directed: false,
                ..ClusterOptions::default()
            },
        )
        .unwrap();
    assert!(clusters.num_rows() >= 1);
    let dag = source
        .analyze(Some("Host"), AnalyzeOptions::default())
        .unwrap();
    assert_eq!(dag.num_rows(), 1);

    // OEH-06: explicit analyst-approved session ontology (not automatic truth).
    let approval_uuid = Uuid::parse_str(APPROVAL).unwrap();
    for (capability, suffix) in [
        (CapabilityId::Provenance, 0x0401),
        (CapabilityId::Knowledge, 0x0402),
        (CapabilityId::Epistemic, 0x0403),
    ] {
        enable(&source, capability, suffix);
    }
    let host_uuid = named_host_uuid(&source, "edge-gw-01");
    let assertion_uuid = id(0x0411);
    let assertion_result = source
        .create_assertion(CreateAssertionRequest {
            context: context(0x0410),
            assertion_uuid,
            claim: "Partial infrastructure vocabulary is analyst-approved for advisory use".into(),
            graph_refs: vec![AssertionGraphRefInput {
                graph_uuid: host_uuid,
                graph_kind: GraphObjectKind::Node,
                role: AssertionGraphRole::Subject,
                ordinal: 0,
            }],
        })
        .unwrap();
    let assertion_provenance = provenance_uuid(&assertion_result);
    source
        .attach_evidence(AttachEvidenceRequest {
            context: context(0x0412),
            evidence_uuid: id(0x0413),
            assertion_uuid,
            source_uuid: host_uuid,
            source_kind: EvidenceSourceKind::GraphNode,
            role: EvidenceRole::Supports,
            weight: Some(0.9),
        })
        .unwrap();
    let reasoning_uuid = id(0x041a);
    source
        .record_reasoning(RecordReasoningRequest {
            context: context(0x041b),
            reasoning_uuid,
            assertion_uuid,
            kind: ReasoningKind::EvidenceInterpretation,
            content_format: ReasoningContentFormat::TextPlain,
            content: b"Analyst explicitly approved a partial vocabulary; unknowns remain visible."
                .to_vec(),
            supersedes_reasoning_uuid: None,
            provenance_uuid: assertion_provenance,
        })
        .unwrap();
    let group_uuid = id(0x0415);
    source
        .create_hypothesis_group(CreateHypothesisGroupRequest {
            context: context(0x0414),
            group_uuid,
            question_key: "oeh.handoff-candidates.v1".into(),
            provenance_uuid: assertion_provenance,
        })
        .unwrap();
    source
        .record_hypothesis_membership(&RecordHypothesisMembershipRequest {
            context: context(0x0416),
            membership_event_uuid: id(0x0417),
            group_uuid,
            assertion_uuid,
            action: HypothesisMembershipAction::Added,
            reasoning_uuid,
            provenance_uuid: assertion_provenance,
        })
        .unwrap();
    source
        .record_hypothesis_selection(&RecordHypothesisSelectionRequest {
            context: context(0x0418),
            selection_event_uuid: id(0x0419),
            group_uuid,
            selected_assertion_uuid: Some(assertion_uuid),
            reasoning_uuid,
            provenance_uuid: assertion_provenance,
        })
        .unwrap();

    source
        .load_ontology(
            fixture("ontologies/emergent-advisory-v1.yaml")
                .to_str()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        source.ontology_mode(),
        graphforge_api::OntologyMode::Advisory
    );

    // OEH-07: unknown concepts remain observable after advisory load.
    source
        .add_node(
            "UnmappedSensor",
            &HashMap::from([("name".into(), PropValue::Str("sensor-x".into()))]),
        )
        .unwrap();
    source
        .execute(
            "MATCH (s:UnmappedSensor {name:'sensor-x'}), (h:Host {name:'edge-gw-01'}) \
             CREATE (s)-[:RAW_SIGNAL]->(h)",
        )
        .unwrap();
    let advisory_catalog = catalog_snapshot(&source);
    assert!(advisory_catalog.0.contains(&"UnmappedSensor".to_owned()));
    assert!(advisory_catalog.1.contains(&"RAW_SIGNAL".to_owned()));
    let advisory_nodes = total_nodes(&source);
    let advisory_edges = total_edges(&source);

    // OEH-08: session load_ontology must not persist as project migration.
    drop(source);
    let source = GraphForge::new(Some(source_path.to_str().unwrap())).unwrap();
    assert_eq!(
        source.ontology_mode(),
        graphforge_api::OntologyMode::Exploratory,
        "load_ontology must remain session-scoped"
    );
    assert_eq!(total_nodes(&source), advisory_nodes);
    assert_eq!(total_edges(&source), advisory_edges);
    let source_scope_names = source
        .execute("MATCH (h:Host) RETURN h.name AS name ORDER BY name")
        .unwrap();
    let source_names = source_scope_names.batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap().to_owned())
        .collect::<Vec<_>>();

    // OEH-09: separate strict target.
    let mut target = GraphForge::new(Some(target_path.to_str().unwrap())).unwrap();
    target
        .adopt_ontology(AdoptOntologyRequest {
            context: context(0x0500),
            path: fixture("ontologies/strict-target-v1.yaml"),
            mode: graphforge_api::OntologyMode::Strict,
        })
        .unwrap();
    assert_eq!(target.ontology_mode(), graphforge_api::OntologyMode::Strict);
    assert_eq!(
        target.workspace_ontology().unwrap().mode,
        graphforge_storage::WorkspaceOntologyMode::Strict
    );

    // OEH-10/11: curated mapped subset with explicit lineage properties.
    let curated_hosts = ["edge-gw-01", "edge-gw-02"];
    let curated_source_uuids = curated_hosts
        .iter()
        .map(|name| named_host_uuid(&source, name))
        .collect::<Vec<_>>();
    let curated_target_ids = (0..2_u16)
        .map(|index| id(0x0600 + index))
        .collect::<Vec<_>>();
    let addr_ids = (0..2_u16)
        .map(|index| id(0x0610 + index))
        .collect::<Vec<_>>();
    let svc_id = id(0x0620);
    let approval_id = id(0x0630);

    let host_schema = bulk_node_input_schema(vec![
        Field::new("approval_record_uuid", DataType::Utf8, false),
        Field::new("environment", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("risk_score", DataType::Float64, true),
        Field::new("source_graph_uuid", DataType::Utf8, false),
    ])
    .unwrap();
    let host_source_refs = [
        curated_source_uuids[0].to_string(),
        curated_source_uuids[1].to_string(),
    ];
    let host_batch = RecordBatch::try_new(
        host_schema,
        vec![
            Arc::new(uuid_column(&curated_target_ids)),
            Arc::new(StringArray::from(vec!["HostAsset", "HostAsset"])),
            Arc::new(StringArray::from(vec![APPROVAL, APPROVAL])),
            Arc::new(StringArray::from(vec!["edge", "edge"])),
            Arc::new(StringArray::from(vec!["edge-gw-01", "edge-gw-02"])),
            Arc::new(Float64Array::from(vec![Some(0.4_f64), Some(0.55)])),
            Arc::new(StringArray::from(
                host_source_refs
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    target
        .publish_bulk_nodes(OperationId(id(0x0700)), &[host_batch])
        .unwrap();

    let addr_schema = bulk_node_input_schema(vec![
        Field::new("approval_record_uuid", DataType::Utf8, false),
        Field::new("family", DataType::Utf8, false),
        Field::new("source_graph_uuid", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ])
    .unwrap();
    let addr_source_refs = [
        curated_source_uuids[0].to_string(),
        curated_source_uuids[1].to_string(),
    ];
    let addr_batch = RecordBatch::try_new(
        addr_schema,
        vec![
            Arc::new(uuid_column(&addr_ids)),
            Arc::new(StringArray::from(vec![
                "NetworkIdentity",
                "NetworkIdentity",
            ])),
            Arc::new(StringArray::from(vec![APPROVAL, APPROVAL])),
            Arc::new(StringArray::from(vec!["ipv4", "ipv4"])),
            Arc::new(StringArray::from(
                addr_source_refs
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec!["10.0.0.10", "10.0.0.11"])),
        ],
    )
    .unwrap();
    target
        .publish_bulk_nodes(OperationId(id(0x0702)), &[addr_batch])
        .unwrap();

    let svc_schema = bulk_node_input_schema(vec![
        Field::new("approval_record_uuid", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("port", DataType::Int64, false),
        Field::new("protocol", DataType::Utf8, false),
        Field::new("source_graph_uuid", DataType::Utf8, false),
    ])
    .unwrap();
    let svc_source = curated_source_uuids[0].to_string();
    let svc_batch = RecordBatch::try_new(
        svc_schema,
        vec![
            Arc::new(uuid_column(&[svc_id])),
            Arc::new(StringArray::from(vec!["ObservedService"])),
            Arc::new(StringArray::from(vec![APPROVAL])),
            Arc::new(StringArray::from(vec!["https"])),
            Arc::new(Int64Array::from(vec![443_i64])),
            Arc::new(StringArray::from(vec!["tcp"])),
            Arc::new(StringArray::from(vec![svc_source.as_str()])),
        ],
    )
    .unwrap();
    target
        .publish_bulk_nodes(OperationId(id(0x0703)), &[svc_batch])
        .unwrap();

    let approval_schema = bulk_node_input_schema(vec![
        Field::new("analyst_id", DataType::Utf8, false),
        Field::new("approval_record_uuid", DataType::Utf8, false),
        Field::new("decision", DataType::Utf8, false),
        Field::new("source_graph_uuid", DataType::Utf8, false),
    ])
    .unwrap();
    let approval_source = approval_uuid.to_string();
    let approval_batch = RecordBatch::try_new(
        approval_schema,
        vec![
            Arc::new(uuid_column(&[approval_id])),
            Arc::new(StringArray::from(vec!["ApprovalRecord"])),
            Arc::new(StringArray::from(vec!["synthetic-analyst-01"])),
            Arc::new(StringArray::from(vec![APPROVAL])),
            Arc::new(StringArray::from(vec!["approve curated edge findings"])),
            Arc::new(StringArray::from(vec![approval_source.as_str()])),
        ],
    )
    .unwrap();
    target
        .publish_bulk_nodes(OperationId(id(0x0704)), &[approval_batch])
        .unwrap();

    let edge_schema = bulk_edge_input_schema(Vec::new()).unwrap();
    let edge_specs = [
        (curated_target_ids[0], addr_ids[0], "HAS_IDENTITY"),
        (curated_target_ids[1], addr_ids[1], "HAS_IDENTITY"),
        (curated_target_ids[0], svc_id, "EXPOSES_APPROVED"),
        (curated_target_ids[0], approval_id, "APPROVED_BY"),
    ];
    let mut edge_ids = FixedSizeBinaryBuilder::with_capacity(edge_specs.len(), 16);
    let mut sources = FixedSizeBinaryBuilder::with_capacity(edge_specs.len(), 16);
    let mut targets_col = FixedSizeBinaryBuilder::with_capacity(edge_specs.len(), 16);
    let mut rels = Vec::new();
    for (index, (src, dst, rel)) in edge_specs.iter().enumerate() {
        edge_ids
            .append_value(id(0x0710 + index as u16).as_bytes())
            .unwrap();
        sources.append_value(src.as_bytes()).unwrap();
        targets_col.append_value(dst.as_bytes()).unwrap();
        rels.push(*rel);
    }
    let edge_batch = RecordBatch::try_new(
        edge_schema,
        vec![
            Arc::new(edge_ids.finish()),
            Arc::new(StringArray::from(rels)),
            Arc::new(sources.finish()),
            Arc::new(targets_col.finish()),
        ],
    )
    .unwrap();
    target
        .publish_bulk_edges(OperationId(id(0x0701)), &[edge_batch])
        .unwrap();

    let target_nodes_before = total_nodes(&target);
    let target_edges_before = total_edges(&target);
    assert_eq!(target_nodes_before, 6);
    assert_eq!(target_edges_before, 4);

    // OEH-12
    let target_scope = target
        .execute(
            "MATCH (h:HostAsset) RETURN h.name AS name, h.source_graph_uuid AS source_graph_uuid \
             ORDER BY name",
        )
        .unwrap();
    let target_names = target_scope.batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|value| value.unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        target_names,
        vec!["edge-gw-01".to_owned(), "edge-gw-02".to_owned()]
    );
    for name in &curated_hosts {
        assert!(source_names.iter().any(|value| value == name));
    }

    // OEH-13/14: invalid handoff and malformed batch must not mutate either project.
    let source_nodes_before_fail = total_nodes(&source);
    let source_edges_before_fail = total_edges(&source);
    let invalid_schema = bulk_node_input_schema(vec![
        Field::new("approval_record_uuid", DataType::Utf8, false),
        Field::new("environment", DataType::Utf8, false),
        Field::new("mystery", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("source_graph_uuid", DataType::Utf8, false),
    ])
    .unwrap();
    let invalid_nil = Uuid::nil().to_string();
    let invalid_batch = RecordBatch::try_new(
        invalid_schema,
        vec![
            Arc::new(uuid_column(&[id(0x0800)])),
            Arc::new(StringArray::from(vec!["HostAsset"])),
            Arc::new(StringArray::from(vec![APPROVAL])),
            Arc::new(StringArray::from(vec!["edge"])),
            Arc::new(StringArray::from(vec!["unexpected"])),
            Arc::new(StringArray::from(vec!["bad-host"])),
            Arc::new(StringArray::from(vec![invalid_nil.as_str()])),
        ],
    )
    .unwrap();
    let invalid_error = target
        .publish_bulk_nodes(OperationId(id(0x0801)), &[invalid_batch])
        .unwrap_err();
    let invalid_code = format!("{invalid_error:?}");

    let unmapped_schema = bulk_node_input_schema(vec![
        Field::new("approval_record_uuid", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("source_graph_uuid", DataType::Utf8, false),
    ])
    .unwrap();
    let unmapped_nil = Uuid::nil().to_string();
    let unmapped_batch = RecordBatch::try_new(
        unmapped_schema,
        vec![
            Arc::new(uuid_column(&[id(0x0810)])),
            Arc::new(StringArray::from(vec!["UnmappedLabel"])),
            Arc::new(StringArray::from(vec![APPROVAL])),
            Arc::new(StringArray::from(vec!["ghost"])),
            Arc::new(StringArray::from(vec![unmapped_nil.as_str()])),
        ],
    )
    .unwrap();
    let unmapped_error = target
        .publish_bulk_nodes(OperationId(id(0x0811)), &[unmapped_batch])
        .unwrap_err();
    let unmapped_code = format!("{unmapped_error:?}");

    let missing_endpoint = {
        let edge_schema = bulk_edge_input_schema(Vec::new()).unwrap();
        let mut edge_ids = FixedSizeBinaryBuilder::with_capacity(1, 16);
        let mut sources = FixedSizeBinaryBuilder::with_capacity(1, 16);
        let mut targets_col = FixedSizeBinaryBuilder::with_capacity(1, 16);
        edge_ids.append_null();
        sources.append_value(Uuid::nil().as_bytes()).unwrap();
        targets_col
            .append_value(curated_target_ids[0].as_bytes())
            .unwrap();
        RecordBatch::try_new(
            edge_schema,
            vec![
                Arc::new(edge_ids.finish()),
                Arc::new(StringArray::from(vec!["HAS_IDENTITY"])),
                Arc::new(sources.finish()),
                Arc::new(targets_col.finish()),
            ],
        )
        .unwrap()
    };
    let missing_error = target
        .publish_bulk_edges(OperationId(id(0x0812)), &[missing_endpoint])
        .unwrap_err();
    let missing_code = format!("{missing_error:?}");

    assert_eq!(total_nodes(&target), target_nodes_before);
    assert_eq!(total_edges(&target), target_edges_before);
    assert_eq!(total_nodes(&source), source_nodes_before_fail);
    assert_eq!(total_edges(&source), source_edges_before_fail);

    let source_fingerprint = fingerprint_json(&json!({
        "nodes": total_nodes(&source),
        "edges": total_edges(&source),
        "names": source_names,
        "catalog": catalog_snapshot(&source),
    }));
    let target_fingerprint = fingerprint_json(&json!({
        "nodes": total_nodes(&target),
        "edges": total_edges(&target),
        "names": target_names,
        "mode": format!("{:?}", target.ontology_mode()),
    }));

    // OEH-15
    drop(source);
    drop(target);
    let source = GraphForge::new(Some(source_path.to_str().unwrap())).unwrap();
    let target = GraphForge::new(Some(target_path.to_str().unwrap())).unwrap();
    assert_eq!(
        source.ontology_mode(),
        graphforge_api::OntologyMode::Exploratory
    );
    assert_eq!(target.ontology_mode(), graphforge_api::OntologyMode::Strict);
    assert_eq!(total_nodes(&source), advisory_nodes);
    assert_eq!(total_edges(&source), advisory_edges);
    assert_eq!(total_nodes(&target), target_nodes_before);
    assert_eq!(total_edges(&target), target_edges_before);
    let source_fingerprint_after = fingerprint_json(&json!({
        "nodes": total_nodes(&source),
        "edges": total_edges(&source),
        "names": source
            .execute("MATCH (h:Host) RETURN h.name AS name ORDER BY name")
            .unwrap()
            .batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| value.unwrap().to_owned())
            .collect::<Vec<_>>(),
        "catalog": catalog_snapshot(&source),
    }));
    let target_fingerprint_after = fingerprint_json(&json!({
        "nodes": total_nodes(&target),
        "edges": total_edges(&target),
        "names": target
            .execute("MATCH (h:HostAsset) RETURN h.name AS name ORDER BY name")
            .unwrap()
            .batches[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|value| value.unwrap().to_owned())
            .collect::<Vec<_>>(),
        "mode": format!("{:?}", target.ontology_mode()),
    }));
    assert_eq!(source_fingerprint, source_fingerprint_after);
    assert_eq!(target_fingerprint, target_fingerprint_after);

    let phase_metrics = vec![
        json!({"phase_id":"01-empty","project":"source","ontology_mode":"exploratory","node_count":0,"edge_count":0,"density":0.0}),
        json!({"phase_id":"02-bulk","project":"source","ontology_mode":"exploratory","node_count":12,"edge_count":6,"density":density(12,6)}),
        json!({"phase_id":"03-enriched","project":"source","ontology_mode":"exploratory","node_count":enriched_nodes,"edge_count":enriched_edges,"density":density(enriched_nodes,enriched_edges)}),
        json!({"phase_id":"04-advisory","project":"source","ontology_mode":"advisory-session","node_count":advisory_nodes,"edge_count":advisory_edges,"density":density(advisory_nodes,advisory_edges)}),
        json!({"phase_id":"05-source-reopened","project":"source","ontology_mode":"exploratory","node_count":advisory_nodes,"edge_count":advisory_edges,"density":density(advisory_nodes,advisory_edges)}),
        json!({"phase_id":"06-strict-target","project":"target","ontology_mode":"strict","node_count":target_nodes_before,"edge_count":target_edges_before,"density":density(target_nodes_before,target_edges_before)}),
        json!({"phase_id":"07-rejected","project":"target","ontology_mode":"strict","node_count":target_nodes_before,"edge_count":target_edges_before,"density":density(target_nodes_before,target_edges_before)}),
        json!({"phase_id":"08-reopened","project":"target","ontology_mode":"strict","node_count":target_nodes_before,"edge_count":target_edges_before,"density":density(target_nodes_before,target_edges_before)}),
    ];

    let evidence = json!({
        "contract_version": 1,
        "scenario_id": "ontology-emergence-strict-handoff",
        "commit_sha": sha,
        "seed": 2469,
        "outcome": "curated findings remain traceable from exploratory source into a separate strict target; session advisory load did not migrate project truth",
        "phases": phase_metrics,
        "ontology_states": [
            {"project":"source","mode":"exploratory","persistence":"authoritative"},
            {"project":"source","mode":"advisory","persistence":"session-scoped-only"},
            {"project":"target","mode":"strict","persistence":"authoritative"}
        ],
        "load_path_classification": {
            "rust_bulk": "supported-publish_bulk_nodes-publish_bulk_edges",
            "python_bulk": "supported-publish_bulk_nodes-publish_bulk_edges",
            "node_bulk": "supported-publishBulkNodes-publishBulkEdges",
            "legacy_add_nodes_add_edges": "placeholder-not-used-by-this-scenario"
        },
        "analyst_approval": {
            "automatic_truth": false,
            "approval_record_uuid": APPROVAL,
            "assertion_uuid": assertion_uuid.to_string(),
            "selected": true
        },
        "catalog": {
            "after_enrichment": after_enrich_catalog,
            "after_advisory_unknowns": advisory_catalog,
            "host_count": count_label(&source, "Host"),
            "resolves_to": count_rel(&source, "RESOLVES_TO")
        },
        "handoff": {
            "curated_source_uuids": curated_source_uuids.iter().map(Uuid::to_string).collect::<Vec<_>>(),
            "target_names": target_names,
            "mapping_properties": ["source_graph_uuid", "approval_record_uuid"]
        },
        "failures": {
            "undeclared_property": invalid_code,
            "unmapped_label": unmapped_code,
            "missing_endpoint": missing_code,
            "partial_mutation": false
        },
        "algorithms": {
            "search_rows": search.num_rows(),
            "rank_rows": rank.num_rows(),
            "cluster_rows": clusters.num_rows(),
            "scope_rows": scope.stats.rows_produced
        },
        "fingerprints": {
            "source": source_fingerprint,
            "target": target_fingerprint
        },
        "reopen_equal": true
    });
    fs::write(evidence_path, serde_json::to_vec_pretty(&evidence).unwrap()).unwrap();
}
