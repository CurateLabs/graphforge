use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::{Array, FixedSizeBinaryArray};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use gf_core::PropValue;
use gf_knowledge::{
    Assertion, AssertionGraphRef, AssertionGraphRole, AssertionStatus, AssertionStatusEvent,
    AssertionSupersession, AssertionValidityEvent, ConfidenceAssessment, ConfidencePolicy,
    EvidenceLink, EvidenceRole, EvidenceSourceKind, GraphObjectKind, HypothesisGroup,
    HypothesisMembershipAction, HypothesisMembershipEvent, HypothesisSelectionEvent,
    ReasoningContentFormat, ReasoningKind, ReasoningRecord,
};
use gf_provenance::{EventKind, LineageRecord, LineageRole, ProvenanceEvent, SubjectKind};
use serde::Deserialize;
use uuid::Uuid;

use crate::composite_transaction::{
    COMPOSITE_TRANSACTION_CONTRACT_VERSION, CompositeGraphMutation, CompositeKnowledgeParticipants,
    CompositeTransactionRequest, MAX_COMPOSITE_TRANSACTION_ENTRIES,
};
use crate::{
    CapabilityId, EnableCapabilityRequest, GraphForge, ListAssertionStatusRequest,
    ListAssertionSupersessionsRequest, ListAssertionValidityRequest, ListAssertionsRequest,
    ListConfidenceAssessmentsRequest, ListEvidenceLinksRequest, ListHypothesisGroupsRequest,
    ListHypothesisMembershipRequest, ListHypothesisSelectionRequest, ListReasoningRequest,
    OperationId, PageRequest, ProvenanceHistoryRequest, WriteContext,
};

const DEADLINE: Duration = Duration::from_secs(20);
const HELPER: &str = "composite_recovery_tests::composite_publication_helper";
const ROOT_ENV: &str = "GF_TEST_COMPOSITE_ROOT";
const FAILPOINT_ENABLE_ENV: &str = "GRAPHFORGE_PROJECT_FAILPOINTS";
const FAILPOINT_ACTIVE_ENV: &str = "GRAPHFORGE_PROJECT_FAILPOINT";
const FAILPOINT_COOKIE: &str = "graphforge-internal-subprocess-v1";
const FAILPOINT_EXIT: i32 = 86;
const GRAPH_QUERY: &str = "MATCH (a:RecoverySubject)-[r:RECOVERY_LINK]->(b:RecoveryPeer) \
    RETURN a.node_uuid AS subject_uuid, a.name AS subject_name, \
           r.edge_uuid AS edge_uuid, r.weight AS weight, \
           b.node_uuid AS peer_uuid, b.name AS peer_name \
    ORDER BY subject_uuid, edge_uuid, peer_uuid";

const PRE_CURRENT_FAILPOINTS: &[&str] = &[
    "project.after_writer_lock",
    "project.after_journal_preparing",
    "project.after_participant_write",
    "project.after_participant_fsync",
    "project.after_participant_dir_fsync",
    "project.after_journal_staged",
    "project.after_domain_validation",
    "project.after_composite_validation",
    "project.after_journal_validated",
    "project.after_manifest_write",
    "project.after_manifest_fsync",
    "project.after_generation_dir_fsync",
    "project.after_journal_durable",
    "project.after_current_temp_write",
    "project.after_current_temp_fsync",
    "project.before_current_replace",
];
const POST_CURRENT_FAILPOINTS: &[&str] = &[
    "project.after_current_replace",
    "project.after_root_fsync",
    "project.after_journal_published",
];

fn uuid7(seed: u8) -> Uuid {
    let mut bytes = [seed; 16];
    bytes[..6].copy_from_slice(&[1, 2, 3, 4, 5, seed]);
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
        .unwrap_or_else(|error| panic!("phase=fixture capability={capability_id:?} error={error}"));
}

fn composite_request() -> CompositeTransactionRequest {
    let operation = uuid7(10);
    let subject = uuid7(20);
    let peer = uuid7(21);
    let edge = uuid7(22);
    let provenance =
        ProvenanceEvent::new(operation, EventKind::CreateAssertion, None, 1_000_000).unwrap();
    let assertion = Assertion::new(
        uuid7(30),
        "composite recovery remains generation-atomic".into(),
        provenance.provenance_uuid,
        1_000_000,
    )
    .unwrap();
    let replacement = Assertion::new(
        uuid7(31),
        "composite recovery replacement claim".into(),
        provenance.provenance_uuid,
        1_000_001,
    )
    .unwrap();
    let confidence = ConfidenceAssessment::new(
        uuid7(40),
        assertion.assertion_uuid,
        ConfidencePolicy::Explicit,
        Some(0.91),
        provenance.provenance_uuid,
        1_000_000,
    )
    .unwrap();
    let reasoning = ReasoningRecord::new(
        uuid7(50),
        assertion.assertion_uuid,
        ReasoningKind::LogicalInference,
        ReasoningContentFormat::TextPlain,
        b"recovery-proof".to_vec(),
        None,
        provenance.provenance_uuid,
        1_000_000,
    )
    .unwrap();
    let evidence = EvidenceLink::new(
        uuid7(60),
        assertion.assertion_uuid,
        uuid7(61),
        EvidenceSourceKind::Observation,
        EvidenceRole::Supports,
        Some(0.75),
        provenance.provenance_uuid,
        1_000_000,
    )
    .unwrap();
    let status = AssertionStatusEvent::new(
        uuid7(70),
        assertion.assertion_uuid,
        AssertionStatus::Supported,
        Some(confidence.confidence_uuid),
        Some(reasoning.reasoning_uuid),
        provenance.provenance_uuid,
        1_000_000,
    )
    .unwrap();
    let supersession = AssertionSupersession::new(
        uuid7(80),
        assertion.assertion_uuid,
        replacement.assertion_uuid,
        status.status_event_uuid,
        reasoning.reasoning_uuid,
        provenance.provenance_uuid,
        1_000_002,
    )
    .unwrap();
    let group = HypothesisGroup::new(
        uuid7(90),
        "recovery.question".into(),
        provenance.provenance_uuid,
        1_000_000,
    )
    .unwrap();
    let membership = HypothesisMembershipEvent::new(
        uuid7(100),
        operation,
        group.group_uuid,
        assertion.assertion_uuid,
        HypothesisMembershipAction::Added,
        reasoning.reasoning_uuid,
        provenance.provenance_uuid,
        1_000_000,
    )
    .unwrap();
    let selection = HypothesisSelectionEvent::new(
        uuid7(110),
        operation,
        group.group_uuid,
        Some(assertion.assertion_uuid),
        reasoning.reasoning_uuid,
        provenance.provenance_uuid,
        1_000_001,
    )
    .unwrap();
    let validity = AssertionValidityEvent::new(
        uuid7(120),
        assertion.assertion_uuid,
        Some(0),
        Some(2_000_000),
        Some(reasoning.reasoning_uuid),
        provenance.provenance_uuid,
        1_000_000,
    )
    .unwrap();

    CompositeTransactionRequest {
        contract_version: COMPOSITE_TRANSACTION_CONTRACT_VERSION,
        context: WriteContext {
            operation_uuid: OperationId(operation),
            actor_uuid: None,
        },
        graph_mutations: vec![
            CompositeGraphMutation::CreateNode {
                node_uuid: subject,
                label: "RecoverySubject".into(),
                properties: HashMap::from([("name".into(), PropValue::Str("initial".into()))]),
            },
            CompositeGraphMutation::CreateNode {
                node_uuid: peer,
                label: "RecoveryPeer".into(),
                properties: HashMap::from([("name".into(), PropValue::Str("peer".into()))]),
            },
            CompositeGraphMutation::CreateEdge {
                edge_uuid: edge,
                rel_type: "RECOVERY_LINK".into(),
                source_uuid: subject,
                target_uuid: peer,
                properties: HashMap::from([("weight".into(), PropValue::Int(1))]),
            },
            CompositeGraphMutation::SetNodeProperty {
                node_uuid: subject,
                property: "name".into(),
                value: PropValue::Str("stable".into()),
            },
            CompositeGraphMutation::SetEdgeProperty {
                edge_uuid: edge,
                property: "weight".into(),
                value: PropValue::Int(2),
            },
        ],
        knowledge: CompositeKnowledgeParticipants {
            provenance_events: vec![provenance.clone()],
            lineage: vec![
                LineageRecord::new(
                    provenance.provenance_uuid,
                    subject,
                    SubjectKind::Node,
                    LineageRole::Output,
                    0,
                )
                .unwrap(),
                LineageRecord::new(
                    provenance.provenance_uuid,
                    peer,
                    SubjectKind::Node,
                    LineageRole::Output,
                    1,
                )
                .unwrap(),
                LineageRecord::new(
                    provenance.provenance_uuid,
                    edge,
                    SubjectKind::Edge,
                    LineageRole::Output,
                    2,
                )
                .unwrap(),
                LineageRecord::new(
                    provenance.provenance_uuid,
                    assertion.assertion_uuid,
                    SubjectKind::Assertion,
                    LineageRole::Output,
                    3,
                )
                .unwrap(),
            ],
            assertions: vec![assertion.clone(), replacement.clone()],
            assertion_graph_refs: vec![
                AssertionGraphRef::new(
                    assertion.assertion_uuid,
                    subject,
                    GraphObjectKind::Node,
                    AssertionGraphRole::Subject,
                    0,
                )
                .unwrap(),
                AssertionGraphRef::new(
                    assertion.assertion_uuid,
                    edge,
                    GraphObjectKind::Edge,
                    AssertionGraphRole::Object,
                    0,
                )
                .unwrap(),
                AssertionGraphRef::new(
                    replacement.assertion_uuid,
                    peer,
                    GraphObjectKind::Node,
                    AssertionGraphRole::Subject,
                    0,
                )
                .unwrap(),
            ],
            confidence_assessments: vec![confidence],
            confidence_inputs: Vec::new(),
            evidence: vec![evidence],
            reasoning: vec![reasoning],
            assertion_status: vec![status],
            assertion_supersessions: vec![supersession],
            hypothesis_groups: vec![group],
            hypothesis_membership: vec![membership],
            hypothesis_selection: vec![selection],
            assertion_validity: vec![validity],
        },
    }
}

fn fixture(root: &Path) {
    let graph = GraphForge::new(root.to_str()).expect("phase=fixture open");
    enable(&graph, CapabilityId::Provenance, 1);
    enable(&graph, CapabilityId::Knowledge, 2);
    enable(&graph, CapabilityId::Epistemic, 3);
    enable(&graph, CapabilityId::ValidTime, 4);
}

fn alphanumeric_token(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn tokenized_temp(name: &str, destination: &str) -> bool {
    name.strip_prefix(destination)
        .and_then(|name| name.strip_prefix('.'))
        .and_then(|name| name.strip_suffix(".tmp"))
        .is_some_and(alphanumeric_token)
}

fn is_under(relative_parent: &Path, prefix: &Path) -> bool {
    relative_parent == prefix || relative_parent.starts_with(prefix)
}

fn is_embedding_space_root(relative_parent: &Path) -> bool {
    let components = relative_parent
        .iter()
        .filter_map(std::ffi::OsStr::to_str)
        .collect::<Vec<_>>();
    components.len() == 3
        && components[0] == "embeddings"
        && components[1] == "spaces"
        && components[2].len() == 64
        && components[2]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validated_atomicwrite_shape(path: &Path) -> bool {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return false;
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return false;
    }
    let Ok(mut entries) = std::fs::read_dir(path) else {
        return false;
    };
    let Some(Ok(entry)) = entries.next() else {
        return true;
    };
    if entries.next().is_some() || entry.file_name() != "tmpfile.tmp" {
        return false;
    }
    let Ok(metadata) = std::fs::symlink_metadata(entry.path()) else {
        return false;
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return false;
        }
    }
    true
}

fn is_storage_temporary_entry(
    relative_parent: &Path,
    entry: &std::fs::DirEntry,
    file_type: std::fs::FileType,
) -> bool {
    let name_os = entry.file_name();
    let Some(name) = name_os.to_str() else {
        return false;
    };
    let atomicwrite_parent =
        relative_parent.as_os_str().is_empty() || relative_parent == Path::new("transactions");
    if atomicwrite_parent
        && file_type.is_dir()
        && name
            .strip_prefix(".atomicwrite")
            .is_some_and(|suffix| suffix.len() == 6 && alphanumeric_token(suffix))
    {
        return validated_atomicwrite_shape(&entry.path());
    }

    let search_root = Path::new("indexes/search");
    let embeddings_root = Path::new("embeddings");
    let in_search_publication_tree =
        is_under(relative_parent, search_root) || is_under(relative_parent, embeddings_root);
    if in_search_publication_tree {
        if file_type.is_dir() && name.strip_prefix(".build-").is_some_and(alphanumeric_token) {
            return true;
        }
        if file_type.is_file() && tokenized_temp(name, "current.json") {
            return true;
        }
    }
    if relative_parent == embeddings_root
        && file_type.is_file()
        && tokenized_temp(name, ".refresh.json")
    {
        return true;
    }
    if relative_parent == embeddings_root
        && file_type.is_file()
        && tokenized_temp(name, ".catalog.json")
    {
        return true;
    }
    if is_embedding_space_root(relative_parent)
        && file_type.is_file()
        && [".mutations.json", ".space.json", ".active.json"]
            .iter()
            .any(|destination| tokenized_temp(name, destination))
    {
        return true;
    }

    let in_graph_staging_tree = ["topology", "properties", "edge_properties"]
        .iter()
        .any(|root| is_under(relative_parent, Path::new(root)));
    if in_graph_staging_tree && file_type.is_file() {
        let staged = name
            .strip_suffix(".tmp")
            .and_then(|name| name.rsplit_once('.'))
            .is_some_and(|(destination, token)| {
                alphanumeric_token(token)
                    && (destination.ends_with(".parquet") || destination == "generation.json")
            });
        if staged {
            return true;
        }
    }
    if is_under(relative_parent, Path::new("indexes/adjacency"))
        && file_type.is_file()
        && name
            .strip_suffix(".tmp")
            .and_then(|name| name.rsplit_once('.'))
            .is_some_and(|(destination, token)| {
                destination.ends_with(".csr") && alphanumeric_token(token)
            })
    {
        return true;
    }

    if relative_parent != Path::new("checkpoints") || !file_type.is_file() {
        return false;
    }
    let Some(private) = name.strip_prefix(".registry.") else {
        return false;
    };
    let Some((uuid, suffix)) = private.split_once('.') else {
        return false;
    };
    Uuid::parse_str(uuid).is_ok() && matches!(suffix, "json.next" | "sha256.next" | "txn.next")
}

fn assert_clone_source_hygiene(source: &Path, relative_parent: &Path) {
    for entry in std::fs::read_dir(source).expect("phase=oracle read fixture") {
        let entry = entry.expect("phase=oracle fixture entry");
        if relative_parent.as_os_str().is_empty() && entry.file_name() == "locks" {
            continue;
        }
        let file_type = entry
            .file_type()
            .expect("phase=oracle source-hygiene entry type");
        assert!(
            !is_storage_temporary_entry(relative_parent, &entry, file_type),
            "phase=oracle source-hygiene temporary-artifact path={}",
            entry.path().display()
        );
        assert!(
            file_type.is_dir() || file_type.is_file(),
            "phase=oracle source-hygiene unsupported-entry path={}",
            entry.path().display()
        );
        if file_type.is_dir() {
            assert_clone_source_hygiene(&entry.path(), &relative_parent.join(entry.file_name()));
        }
    }
}

fn copy_project(source: &Path, destination: &Path, relative_parent: &Path) {
    for entry in std::fs::read_dir(source).expect("phase=oracle clone read fixture") {
        let entry = entry.expect("phase=oracle clone fixture entry");
        if relative_parent.as_os_str().is_empty() && entry.file_name() == "locks" {
            continue;
        }
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry
            .file_type()
            .expect("phase=oracle clone fixture entry type");
        assert!(
            !is_storage_temporary_entry(relative_parent, &entry, file_type),
            "phase=oracle clone temporary-artifact path={}",
            entry.path().display()
        );
        if file_type.is_dir() {
            std::fs::create_dir(&destination_path).expect("phase=oracle create directory");
            copy_project(
                &source_path,
                &destination_path,
                &relative_parent.join(entry.file_name()),
            );
        } else if file_type.is_file() {
            std::fs::copy(&source_path, &destination_path).expect("phase=oracle copy fixture file");
        } else {
            panic!(
                "phase=oracle clone unsupported-entry path={}",
                source_path.display()
            );
        }
    }
}

fn fingerprint(result: &crate::ExecutionResult) -> [u8; 32] {
    let logical = result
        .batches
        .iter()
        .map(|batch| {
            let mut metadata = batch.schema().metadata().clone();
            metadata.remove("graphforge.ir_version");
            RecordBatch::try_new(
                Arc::new(Schema::new_with_metadata(
                    batch.schema().fields().clone(),
                    metadata,
                )),
                batch.columns().to_vec(),
            )
            .expect("phase=state logical Arrow normalization")
        })
        .collect::<Vec<_>>();
    crate::canonical_arrow::result_fingerprint(&logical)
        .expect("phase=state canonical Arrow fingerprint")
}

fn receipts_equal(left: &RecordBatch, right: &RecordBatch) -> bool {
    if left.num_columns() != right.num_columns() || left.num_rows() != right.num_rows() {
        return false;
    }
    if left.schema().fields() != right.schema().fields() {
        return false;
    }
    (0..left.num_columns()).all(|index| left.column(index) == right.column(index))
}

fn uuid_values(result: &crate::ExecutionResult, column: &str) -> Vec<Uuid> {
    let mut values = result
        .batches
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("phase=state missing UUID column={column}"))
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap_or_else(|| panic!("phase=state malformed UUID column={column}"));
            (0..values.len())
                .map(|row| Uuid::from_slice(values.value(row)).expect("canonical UUID bytes"))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    values.sort_unstable();
    values.dedup();
    values
}

#[derive(Debug, Eq, PartialEq)]
struct CompositeState {
    graph: [u8; 32],
    provenance: [u8; 32],
    assertions: [u8; 32],
    confidence: [u8; 32],
    evidence: [u8; 32],
    reasoning: [u8; 32],
    statuses: [u8; 32],
    supersessions: [u8; 32],
    refs: [u8; 32],
    hypothesis_groups: [u8; 32],
    hypothesis_membership: [u8; 32],
    hypothesis_selection: [u8; 32],
    validity: [u8; 32],
    provenance_uuids: Vec<Uuid>,
    assertion_uuids: Vec<Uuid>,
    evidence_uuids: Vec<Uuid>,
    reasoning_uuids: Vec<Uuid>,
    status_event_uuids: Vec<Uuid>,
    group_uuids: Vec<Uuid>,
    validity_uuids: Vec<Uuid>,
    referenced_graph_uuids: Vec<Uuid>,
}

fn state(graph: &GraphForge) -> CompositeState {
    let graph_rows = graph.execute(GRAPH_QUERY).expect("phase=state graph");
    let provenance = graph
        .list_provenance_history(ProvenanceHistoryRequest::default())
        .expect("phase=state provenance");
    let assertions = graph
        .list_assertions(ListAssertionsRequest::default())
        .expect("phase=state assertions");
    let confidence = graph
        .list_confidence_assessments(ListConfidenceAssessmentsRequest::default())
        .expect("phase=state confidence");
    let evidence = graph
        .list_evidence_links(ListEvidenceLinksRequest::default())
        .expect("phase=state evidence");
    let reasoning = graph
        .list_reasoning(ListReasoningRequest::default())
        .expect("phase=state reasoning");
    let statuses = graph
        .list_assertion_status(ListAssertionStatusRequest::default())
        .expect("phase=state statuses");
    let supersessions = graph
        .list_assertion_supersessions(ListAssertionSupersessionsRequest::default())
        .expect("phase=state supersessions");
    let groups = graph
        .list_hypothesis_groups(&ListHypothesisGroupsRequest::default())
        .expect("phase=state hypothesis groups");
    let membership = graph
        .list_hypothesis_membership(&ListHypothesisMembershipRequest::default())
        .expect("phase=state hypothesis membership");
    let selection = graph
        .list_hypothesis_selection(&ListHypothesisSelectionRequest::default())
        .expect("phase=state hypothesis selection");
    let validity = graph
        .list_assertion_validity(ListAssertionValidityRequest::default())
        .expect("phase=state validity");
    let refs = (assertions.stats.rows_produced > 0).then(|| {
        graph
            .assertion_graph_refs(uuid7(30), PageRequest::default())
            .expect("phase=state assertion refs")
    });
    CompositeState {
        graph: fingerprint(&graph_rows),
        provenance: fingerprint(&provenance),
        assertions: fingerprint(&assertions),
        confidence: fingerprint(&confidence),
        evidence: fingerprint(&evidence),
        reasoning: fingerprint(&reasoning),
        statuses: fingerprint(&statuses),
        supersessions: fingerprint(&supersessions),
        refs: refs.as_ref().map_or([0; 32], fingerprint),
        hypothesis_groups: fingerprint(&groups),
        hypothesis_membership: fingerprint(&membership),
        hypothesis_selection: fingerprint(&selection),
        validity: fingerprint(&validity),
        provenance_uuids: uuid_values(&provenance, "provenance_uuid"),
        assertion_uuids: uuid_values(&assertions, "assertion_uuid"),
        evidence_uuids: uuid_values(&evidence, "evidence_uuid"),
        reasoning_uuids: uuid_values(&reasoning, "reasoning_uuid"),
        status_event_uuids: uuid_values(&statuses, "status_event_uuid"),
        group_uuids: uuid_values(&groups, "group_uuid"),
        validity_uuids: uuid_values(&validity, "validity_event_uuid"),
        referenced_graph_uuids: refs
            .as_ref()
            .map_or_else(Vec::new, |result| uuid_values(result, "graph_uuid")),
    }
}

fn publish_clean(graph: &GraphForge) -> RecordBatch {
    graph.set_clock_for_test(|| Ok(1_000_000));
    graph
        .publish_composite_transaction(composite_request())
        .expect("phase=oracle clean composite publication")
}

fn clean_publication_oracle(source: &Path) -> (CompositeState, RecordBatch) {
    let oracle_root = tempfile::tempdir().expect("phase=oracle tempdir");
    assert_clone_source_hygiene(source, Path::new(""));
    copy_project(source, oracle_root.path(), Path::new(""));
    let graph = GraphForge::new(oracle_root.path().to_str()).expect("phase=oracle open");
    let receipt = publish_clean(&graph);
    (state(&graph), receipt)
}

fn pre_current_retention_removals(source: &Path) -> BTreeSet<String> {
    let oracle_root = tempfile::tempdir().expect("phase=retention-oracle tempdir");
    assert_clone_source_hygiene(source, Path::new(""));
    copy_project(source, oracle_root.path(), Path::new(""));
    let before = inventory(oracle_root.path(), "generations");
    let recovery = gf_storage::recover_project_transactions(oracle_root.path())
        .expect("phase=retention-oracle recover clean fixture");
    let after = inventory(oracle_root.path(), "generations");
    assert!(
        after.difference(&before).next().is_none(),
        "phase=retention-oracle clean recovery added a generation"
    );
    let removed = before.difference(&after).cloned().collect::<BTreeSet<_>>();
    assert_eq!(
        recovery.removed_generations,
        u64::try_from(removed.len()).expect("retention removal count fits u64"),
        "phase=retention-oracle report disagrees with removed generation inventory"
    );
    removed
}

fn post_current_retention_removals(source: &Path) -> BTreeSet<String> {
    let oracle_root = tempfile::tempdir().expect("phase=post-retention-oracle tempdir");
    assert_clone_source_hygiene(source, Path::new(""));
    copy_project(source, oracle_root.path(), Path::new(""));
    let fixture_generations = inventory(oracle_root.path(), "generations");
    let graph =
        GraphForge::new(oracle_root.path().to_str()).expect("phase=post-retention-oracle open");
    let _ = publish_clean(&graph);
    drop(graph);
    let selected = gf_storage::resolve_project_generation(oracle_root.path())
        .expect("phase=post-retention-oracle resolve publication")
        .generation_uuid()
        .hyphenated()
        .to_string();
    assert!(
        !fixture_generations.contains(&selected),
        "phase=post-retention-oracle publication reused a fixture generation"
    );
    let recovery = gf_storage::recover_project_transactions(oracle_root.path())
        .expect("phase=post-retention-oracle recover clean publication");
    let after = inventory(oracle_root.path(), "generations");
    assert!(
        after.contains(&selected),
        "phase=post-retention-oracle recovery removed selected CURRENT"
    );
    let removed = fixture_generations
        .difference(&after)
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        recovery.removed_generations,
        u64::try_from(removed.len()).expect("post retention removal count fits u64"),
        "phase=post-retention-oracle report disagrees with removed fixture generations"
    );
    removed
}

struct ChildGuard {
    child: Child,
    reaped: bool,
}

impl ChildGuard {
    fn spawn(root: &Path, failpoint: &str) -> Self {
        let child = Command::new(std::env::current_exe().expect("phase=child current exe"))
            .args(["--exact", HELPER, "--nocapture"])
            .env(ROOT_ENV, root)
            .env(FAILPOINT_ENABLE_ENV, FAILPOINT_COOKIE)
            .env(FAILPOINT_ACTIVE_ENV, failpoint)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|error| {
                panic!("phase=child failpoint={failpoint} spawn error={error}")
            });
        Self {
            child,
            reaped: false,
        }
    }

    fn wait(mut self, failpoint: &str) -> ExitStatus {
        let deadline = Instant::now() + DEADLINE;
        loop {
            if let Some(status) = self.child.try_wait().unwrap_or_else(|error| {
                panic!("phase=child failpoint={failpoint} try_wait error={error}")
            }) {
                self.reaped = true;
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "phase=child failpoint={failpoint} timeout={DEADLINE:?}"
            );
            thread::yield_now();
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if !self.reaped {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn inventory(root: &Path, directory: &str) -> BTreeSet<String> {
    std::fs::read_dir(root.join(directory))
        .unwrap_or_else(|error| panic!("phase=inventory directory={directory} error={error}"))
        .map(|entry| {
            entry
                .expect("phase=inventory entry")
                .file_name()
                .into_string()
                .expect("phase=inventory UTF-8 machine name")
        })
        .collect()
}

fn assert_no_partial_generation(root: &Path, failpoint: &str) {
    for generation in inventory(root, "generations") {
        let path = root.join("generations").join(&generation);
        assert!(path.join("manifest.json").is_file(), "{failpoint}");
        assert!(path.join("participants").is_dir(), "{failpoint}");
    }
    assert!(
        inventory(root, "transactions")
            .iter()
            .all(|entry| !entry.starts_with(".atomicwrite")),
        "failpoint={failpoint} left an atomic journal temporary"
    );
}

#[derive(Deserialize)]
struct JournalIdentity {
    format: String,
    format_version: u32,
    transaction_uuid: String,
    generation_uuid: String,
    phase: String,
}

fn journal_path(root: &Path) -> std::path::PathBuf {
    root.join("transactions")
        .join(format!("{}.json", uuid7(10).hyphenated()))
}

fn journal_bytes_phase_and_generation(root: &Path, failpoint: &str) -> (Vec<u8>, String, Uuid) {
    let bytes = std::fs::read(journal_path(root))
        .unwrap_or_else(|error| panic!("phase=journal failpoint={failpoint} read error={error}"));
    let journal: JournalIdentity = serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("phase=journal failpoint={failpoint} parse error={error}"));
    assert_eq!(journal.format, "graphforge-transaction", "{failpoint}");
    assert_eq!(journal.format_version, 1, "{failpoint}");
    assert_eq!(
        journal.transaction_uuid,
        uuid7(10).hyphenated().to_string(),
        "{failpoint}"
    );
    let generation_uuid = Uuid::parse_str(&journal.generation_uuid).unwrap_or_else(|error| {
        panic!("phase=journal failpoint={failpoint} generation error={error}")
    });
    assert_eq!(
        generation_uuid.hyphenated().to_string(),
        journal.generation_uuid,
        "failpoint={failpoint} noncanonical generation UUID"
    );
    (bytes, journal.phase, generation_uuid)
}

fn recovery_expectation(failpoint: &str) -> (u64, u64, Option<&'static str>) {
    match failpoint {
        "project.after_writer_lock" => (0, 0, None),
        "project.after_current_replace" | "project.after_root_fsync" => (0, 1, Some("PUBLISHED")),
        "project.after_journal_published" => (0, 0, Some("PUBLISHED")),
        _ => (1, 0, Some("ABORTED")),
    }
}

fn assert_committed_identities(after: &CompositeState, failpoint: &str) {
    assert_eq!(after.assertion_uuids, [uuid7(30), uuid7(31)], "{failpoint}");
    assert_eq!(after.evidence_uuids, [uuid7(60)], "{failpoint}");
    assert_eq!(after.reasoning_uuids, [uuid7(50)], "{failpoint}");
    assert_eq!(after.status_event_uuids, [uuid7(70)], "{failpoint}");
    assert_eq!(after.group_uuids, [uuid7(90)], "{failpoint}");
    assert_eq!(after.validity_uuids, [uuid7(120)], "{failpoint}");
    assert_eq!(
        after.referenced_graph_uuids,
        [uuid7(20), uuid7(22)],
        "{failpoint}"
    );
    assert_eq!(after.provenance_uuids.len(), 1, "{failpoint}");
}

fn verify_case(failpoint: &str, committed: bool) {
    let root = tempfile::tempdir().expect("phase=case tempdir");
    fixture(root.path());
    let before_graph = GraphForge::new(root.path().to_str()).expect("phase=case before reopen");
    let before = state(&before_graph);
    let before_generation = gf_storage::resolve_project_generation(root.path())
        .expect("phase=case before generation")
        .generation_uuid();
    let before_generations = inventory(root.path(), "generations");
    let before_transactions = inventory(root.path(), "transactions");
    drop(before_graph);
    let allowed_retention_removals = if committed {
        post_current_retention_removals(root.path())
    } else {
        pre_current_retention_removals(root.path())
    };
    let (oracle, oracle_receipt) = clean_publication_oracle(root.path());
    assert_ne!(
        oracle, before,
        "phase=oracle clean publication did not change full public state"
    );

    let status = ChildGuard::spawn(root.path(), failpoint).wait(failpoint);
    assert_eq!(
        status.code(),
        Some(FAILPOINT_EXIT),
        "failpoint={failpoint} did not terminate at the accepted boundary"
    );

    let recovery = gf_storage::recover_project_transactions(root.path())
        .unwrap_or_else(|error| panic!("phase=recovery failpoint={failpoint} error={error}"));
    let (expected_aborted, expected_repaired, expected_phase) = recovery_expectation(failpoint);
    assert_eq!(
        recovery.aborted_journals, expected_aborted,
        "failpoint={failpoint} aborted journal count"
    );
    assert_eq!(
        recovery.repaired_journals, expected_repaired,
        "failpoint={failpoint} repaired journal count"
    );
    let resolved = gf_storage::resolve_project_generation(root.path())
        .unwrap_or_else(|error| panic!("phase=resolve failpoint={failpoint} error={error}"));
    resolved
        .validate_complete_participant_inventory()
        .unwrap_or_else(|error| panic!("phase=validate failpoint={failpoint} error={error}"));
    assert_eq!(
        recovery.selected_generation_uuid,
        resolved.generation_uuid()
    );
    assert_eq!(resolved.generation_uuid() != before_generation, committed);
    assert_no_partial_generation(root.path(), failpoint);
    let after_generations = inventory(root.path(), "generations");
    let after_transactions = inventory(root.path(), "transactions");
    let added_generations = after_generations
        .difference(&before_generations)
        .cloned()
        .collect::<BTreeSet<_>>();
    let removed_preexisting_generations = before_generations
        .difference(&after_generations)
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        removed_preexisting_generations, allowed_retention_removals,
        "failpoint={failpoint} removed a generation outside the clean retention oracle"
    );
    let aborted_candidate_removals = u64::from(!committed && expected_phase.is_some());
    assert_eq!(
        recovery.removed_generations,
        u64::try_from(allowed_retention_removals.len()).expect("retention removal count fits u64")
            + aborted_candidate_removals,
        "failpoint={failpoint} removed-generation report"
    );
    if failpoint == "project.after_writer_lock" {
        assert!(
            added_generations.is_empty(),
            "failpoint={failpoint} created a generation before staging"
        );
        assert_eq!(
            after_transactions, before_transactions,
            "failpoint={failpoint} unexpectedly created a journal"
        );
        assert!(!journal_path(root.path()).exists(), "{failpoint}");
    } else {
        assert!(
            before_transactions.is_subset(&after_transactions),
            "failpoint={failpoint} removed an existing transaction journal"
        );
        assert_eq!(
            after_transactions.len(),
            before_transactions.len() + 1,
            "failpoint={failpoint} did not retain exactly one resolved journal"
        );
        assert!(
            after_transactions.contains(&format!("{}.json", uuid7(10).hyphenated())),
            "failpoint={failpoint} lost the composite transaction journal"
        );
    }
    let journal_after_recovery = expected_phase.map(|expected_phase| {
        let (bytes, phase, generation_uuid) =
            journal_bytes_phase_and_generation(root.path(), failpoint);
        assert_eq!(phase, expected_phase, "failpoint={failpoint} journal phase");
        let generation_name = generation_uuid.hyphenated().to_string();
        if committed {
            assert_eq!(
                generation_uuid,
                resolved.generation_uuid(),
                "failpoint={failpoint} journal generation is not selected CURRENT"
            );
            assert_eq!(
                added_generations,
                BTreeSet::from([generation_name]),
                "failpoint={failpoint} retained a generation other than selected CURRENT"
            );
        } else {
            assert!(
                added_generations.is_empty(),
                "failpoint={failpoint} retained an added private generation"
            );
            assert!(
                !after_generations.contains(&generation_name),
                "failpoint={failpoint} retained the aborted transaction generation"
            );
        }
        (bytes, generation_uuid)
    });

    let reopened = GraphForge::new(root.path().to_str()).expect("phase=case reopened");
    let after = state(&reopened);
    if committed {
        assert_eq!(
            after, oracle,
            "failpoint={failpoint} differs from clean publication oracle"
        );
        assert_committed_identities(&after, failpoint);
        let identical = reopened
            .publish_composite_transaction(composite_request())
            .unwrap_or_else(|error| {
                panic!("phase=idempotent-repeat failpoint={failpoint} error={error}")
            });
        assert!(
            receipts_equal(&identical, &oracle_receipt),
            "failpoint={failpoint} identical request receipt drifted"
        );
        assert_eq!(
            state(&reopened),
            after,
            "failpoint={failpoint} identical request mutated reopened state"
        );
    } else {
        assert_eq!(after, before, "failpoint={failpoint} exposed mixed state");
    }
    drop(reopened);

    let second_recovery =
        gf_storage::recover_project_transactions(root.path()).unwrap_or_else(|error| {
            panic!("phase=second-recovery failpoint={failpoint} error={error}")
        });
    assert_eq!(
        second_recovery.selected_generation_uuid,
        resolved.generation_uuid()
    );
    assert_eq!(second_recovery.aborted_journals, 0, "{failpoint}");
    assert_eq!(second_recovery.repaired_journals, 0, "{failpoint}");
    assert_eq!(second_recovery.removed_generations, 0, "{failpoint}");
    let journal_after_second_recovery = expected_phase.map(|expected_phase| {
        let (bytes, phase, generation_uuid) =
            journal_bytes_phase_and_generation(root.path(), failpoint);
        assert_eq!(phase, expected_phase, "failpoint={failpoint} journal phase");
        (bytes, generation_uuid)
    });
    assert_eq!(
        journal_after_second_recovery, journal_after_recovery,
        "failpoint={failpoint} second recovery rewrote journal bytes"
    );
    let reopened_again = GraphForge::new(root.path().to_str()).expect("phase=case second reopen");
    assert_eq!(state(&reopened_again), after, "failpoint={failpoint}");
    assert_no_partial_generation(root.path(), failpoint);
    assert_eq!(
        inventory(root.path(), "generations"),
        after_generations,
        "failpoint={failpoint} second recovery changed generation inventory"
    );
    assert_eq!(
        inventory(root.path(), "transactions"),
        after_transactions,
        "failpoint={failpoint} second recovery changed journal inventory"
    );
}

#[test]
fn composite_publication_helper() {
    let Ok(root) = std::env::var(ROOT_ENV) else {
        return;
    };
    let graph = GraphForge::new(Some(&root)).expect("phase=helper open project");
    graph.set_clock_for_test(|| Ok(1_000_000));
    graph
        .publish_composite_transaction(composite_request())
        .expect("configured composite failpoint did not terminate");
}

#[test]
fn composite_kill_reopen_matrix_never_exposes_mixed_state() {
    assert_eq!(PRE_CURRENT_FAILPOINTS.len(), 16);
    assert_eq!(POST_CURRENT_FAILPOINTS.len(), 3);
    for failpoint in PRE_CURRENT_FAILPOINTS {
        verify_case(failpoint, false);
    }
    for failpoint in POST_CURRENT_FAILPOINTS {
        verify_case(failpoint, true);
    }
}

#[test]
fn composite_exact_replay_and_conflict_are_byte_identical_without_mutation() {
    let root = tempfile::tempdir().expect("phase=idempotency tempdir");
    fixture(root.path());
    let graph = GraphForge::new(root.path().to_str()).expect("phase=idempotency open");
    let before = state(&graph);
    let first = publish_clean(&graph);
    let published = state(&graph);
    assert_ne!(published, before);
    let exact_replay = graph
        .publish_composite_transaction(composite_request())
        .expect("phase=idempotency exact replay");
    assert!(
        receipts_equal(&exact_replay, &first),
        "phase=idempotency exact replay receipt drifted"
    );
    assert_eq!(state(&graph), published);

    let mut conflicting = composite_request();
    conflicting.knowledge.assertions[0] = Assertion::new(
        uuid7(30),
        "conflicting composite payload".into(),
        conflicting.knowledge.provenance_events[0].provenance_uuid,
        1_000_000,
    )
    .unwrap();
    let error = graph
        .publish_composite_transaction(conflicting)
        .expect_err("phase=idempotency conflict");
    assert_eq!(error.code(), "GF_IDEMPOTENCY_CONFLICT");
    assert_eq!(state(&graph), published);
    assert_no_partial_generation(root.path(), "idempotency");
}

#[test]
fn invalid_composite_requests_stage_nothing() {
    let root = tempfile::tempdir().expect("phase=validation tempdir");
    fixture(root.path());
    let graph = GraphForge::new(root.path().to_str()).expect("phase=validation open");
    let before = state(&graph);
    let before_generation = *graph
        .current_generation_uuid
        .lock()
        .expect("generation UUID lock poisoned");
    let before_generations = inventory(root.path(), "generations");
    let before_transactions = inventory(root.path(), "transactions");

    let mut bad_endpoint = composite_request();
    if let CompositeGraphMutation::CreateEdge {
        source_uuid,
        target_uuid,
        ..
    } = &mut bad_endpoint.graph_mutations[2]
    {
        *source_uuid = uuid7(200);
        *target_uuid = uuid7(201);
    } else {
        panic!("expected CreateEdge mutation");
    }
    let endpoint_error = graph
        .publish_composite_transaction(bad_endpoint)
        .expect_err("phase=validation endpoint");
    assert_eq!(endpoint_error.code(), "GF_NOT_FOUND");

    let mut bad_kind = composite_request();
    bad_kind.knowledge.assertion_graph_refs[0].graph_kind = GraphObjectKind::Edge;
    let kind_error = graph
        .publish_composite_transaction(bad_kind)
        .expect_err("phase=validation graph-kind");
    assert_eq!(kind_error.code(), "GF_NOT_FOUND");

    let mut overflow = composite_request();
    overflow.graph_mutations = vec![
        CompositeGraphMutation::DeleteNode {
            node_uuid: uuid7(1),
        };
        MAX_COMPOSITE_TRANSACTION_ENTRIES + 1
    ];
    let overflow_error = graph
        .publish_composite_transaction(overflow)
        .expect_err("phase=validation aggregate-cap");
    assert_eq!(overflow_error.code(), "GF_VALIDATION");

    let mut bad_identity = composite_request();
    bad_identity.context.operation_uuid = OperationId(Uuid::nil());
    let identity_error = graph
        .publish_composite_transaction(bad_identity)
        .expect_err("phase=validation identity");
    assert_eq!(identity_error.code(), "GF_VALIDATION");

    assert_eq!(state(&graph), before);
    assert_eq!(
        *graph
            .current_generation_uuid
            .lock()
            .expect("generation UUID lock poisoned"),
        before_generation
    );
    assert_eq!(inventory(root.path(), "generations"), before_generations);
    assert_eq!(inventory(root.path(), "transactions"), before_transactions);
    assert_no_partial_generation(root.path(), "validation");
}
