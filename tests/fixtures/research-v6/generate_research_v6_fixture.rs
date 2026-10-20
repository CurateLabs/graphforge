//! Generator for the research revision 6 fixture. It is not built here: it must
//! run against graphforge 0.5.2 at commit b2b703732, the last research/6 writer
//! (see README.md). Output: `project/` (a durable Project), `package.gfpb` (a
//! research interchange export), `imported/` (that package imported by the same
//! code) and `ids.json` (identities the tests assert).
use graphforge_api::*;
use graphforge_knowledge::research::{ResearchDecisionKind, ResearchSubjectKind};
use std::path::PathBuf;
use uuid::Uuid;

fn current(graph: &GraphForge) -> Uuid {
    graph.research_project_summary().unwrap().identity.generation_uuid
}

fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn main() {
    let out = PathBuf::from(std::env::args().nth(1).expect("output directory"));
    assert!(!out.exists(), "output directory must not exist");
    std::fs::create_dir_all(&out).unwrap();
    let cancel = CancellationToken::new();
    let mut graph = GraphForge::new(out.join("project").to_str()).unwrap();
    graph
        .execute("CREATE (:ClaimSubject {name: 'shared evidence', score: 0})")
        .unwrap();
    let subject = graph
        .execute("MATCH (n:ClaimSubject) RETURN n.node_uuid AS id")
        .unwrap();
    let subject = Uuid::from_slice(
        subject.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
    )
    .unwrap();
    // A canonical decision: the research/6 `canonical_decisions` participant.
    graph
        .record_research_decisions(
            &RecordResearchDecisionsRequest {
                operation_uuid: Uuid::now_v7(),
                expected_generation_uuid: current(&graph),
                context: ResearchContext::Project,
                community_uuid: None,
                creator_uuid: Uuid::now_v7(),
                recorded_at: 1,
                decisions: vec![ResearchDecisionInput {
                    decision_uuid: Uuid::now_v7(),
                    subject_kind: ResearchSubjectKind::Node,
                    subject_uuid: subject,
                    kind: ResearchDecisionKind::Promote,
                    source_version_uuid: None,
                }],
            },
            &cancel,
        )
        .unwrap();
    // A Project capture: the Project context head.
    let project_context = Uuid::now_v7();
    let project_version = Uuid::now_v7();
    let operation = graph
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: project_version,
            context_uuid: project_context,
            label: Some("research/6 capture".into()),
            description: None,
            created_at: 2,
            required_versions: Default::default(),
        })
        .unwrap();
    graph
        .commit_research_version_operation(operation, &cancel)
        .unwrap();
    // A Branch created from current research, then one edit: the v6 Branch head.
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
        created_at: 3,
        label: "main".into(),
    };
    graph.create_research_branch(&branch, &cancel).unwrap();
    let edit = ExecuteResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&graph),
        branch_uuid: branch.branch_uuid,
        version_uuid: Uuid::now_v7(),
        query: "MATCH (n:ClaimSubject) SET n.score = 1".into(),
        created_at: 4,
    };
    graph.execute_research_branch(&edit, &cancel).unwrap();
    graph
        .export_research(
            &ExportResearchRequest {
                version_uuid: edit.version_uuid,
                output: out.join("package.gfpb"),
                bundled: true,
                projection: None,
            },
            &cancel,
        )
        .unwrap();
    GraphForge::import_portable_v2(
        &out.join("imported"),
        &PortableV2ImportRequest {
            input: out.join("package.gfpb"),
            operation_id: OperationId(Uuid::now_v7()),
            limits: Default::default(),
        },
        None,
    )
    .unwrap();
    let registry = graph.research_version_retention().unwrap();
    let identities: serde_json::Map<_, _> = registry
        .identities
        .iter()
        .map(|(id, digest)| (id.to_string(), hex(digest).into()))
        .collect();
    let ids = serde_json::json!({
        "producer": registry.versions[&edit.version_uuid].content.producer,
        "project_context_uuid": project_context,
        "project_version_uuid": project_version,
        "branch_uuid": branch.branch_uuid,
        "branch_base_version_uuid": branch.version_uuid,
        "branch_head_version_uuid": edit.version_uuid,
        "origin_version_uuid": match branch.source {
            BranchSource::Current { origin_version_uuid, .. } => origin_version_uuid,
            _ => unreachable!(),
        },
        "identities": identities,
    });
    drop(graph);
    std::fs::write(
        out.join("ids.json"),
        serde_json::to_vec_pretty(&ids).unwrap(),
    )
    .unwrap();
}
