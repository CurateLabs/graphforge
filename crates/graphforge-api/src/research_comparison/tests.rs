//! Per-field incorporated history is distinct from immutable origin.
use super::{delta, state::State};
use crate::{
    CancellationToken,
    branches::{
        baseline::Row,
        fields::{Fields, Objects},
    },
};
use std::collections::BTreeMap;
use uuid::Uuid;
fn state(
    branch: Uuid,
    fields: Fields,
    baseline: BTreeMap<crate::branches::fields::Key, Row>,
) -> State {
    State {
        fields,
        baseline,
        version: Some(Uuid::now_v7()),
        generation: Uuid::now_v7(),
        context: branch,
        authority_context: (1, branch),
        branch: Some(branch),
        parent_branch: None,
        missing: vec![],
        suppressed: Objects::new(),
    }
}
#[test]
fn repeated_partial_incorporation_keeps_independent_field_baselines() {
    let object = Uuid::now_v7();
    let origin = Uuid::now_v7();
    let incorporated = Uuid::now_v7();
    let x = ("node".into(), object, "property:x".into());
    let y = ("node".into(), object, "property:y".into());
    let row = |key, value, version| Row {
        key,
        origin,
        incorporated: Some(version),
        original: super::hex(&[0; 32]),
        baseline: super::hex(&[value; 32]),
        current: super::hex(&[value; 32]),
        contribution: Uuid::now_v7(),
        role: "active".into(),
    };
    let baseline = BTreeMap::from([
        (x.clone(), row(x.clone(), 1, incorporated)),
        (y.clone(), row(y.clone(), 0, origin)),
    ]);
    let mut left = state(
        Uuid::now_v7(),
        Fields::from([(x.clone(), [1; 32]), (y.clone(), [0; 32])]),
        baseline,
    );
    let right = state(
        Uuid::now_v7(),
        Fields::from([(x.clone(), [2; 32]), (y.clone(), [2; 32])]),
        BTreeMap::new(),
    );
    let rows = delta::compare(&left, &right, &BTreeMap::new(), &CancellationToken::new()).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|r| r.change == "upstream" && r.disposition == "inherited")
    );
    assert_eq!(rows[0].origin, Some(origin));
    assert_eq!(rows[0].incorporated, Some(incorporated));
    assert_eq!(rows[0].baseline, Some([1; 32]));
    assert_eq!(rows[1].baseline, Some([0; 32]));
    left.fields.insert(x.clone(), [3; 32]);
    let rows = delta::compare(&left, &right, &BTreeMap::new(), &CancellationToken::new()).unwrap();
    assert_eq!(rows[0].change, "conflict");
    assert_eq!(rows[1].change, "upstream");
    left.fields.insert(x, [1; 32]);
    left.suppressed.insert(("node".into(), object));
    let rows = delta::compare(&left, &right, &BTreeMap::new(), &CancellationToken::new()).unwrap();
    assert!(rows.iter().all(|r| r.change == "conflict"));
}
#[test]
fn unregistered_projection_cannot_certify_project_acceptance_even_with_same_context_uuid() {
    use crate::*;
    use graphforge_storage::research_versions::{
        RegisterResearchVersion, ResearchMutation, ResearchOperation,
    };
    let mut g = GraphForge::new(None).unwrap();
    g.execute("CREATE (:Item {x:0})").unwrap();
    let current = |g: &GraphForge| g.generation_for_read().unwrap().generation_uuid();
    let token = CancellationToken::new();
    let b = CreateResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        branch_uuid: Uuid::now_v7(),
        version_uuid: Uuid::now_v7(),
        source: BranchSource::Current {
            origin_version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
        },
        creator_uuid: Uuid::now_v7(),
        created_at: 1,
        label: "Study".into(),
    };
    g.create_research_branch(&b, &token).unwrap();
    let edit = ExecuteResearchBranchRequest {
        operation_uuid: Uuid::now_v7(),
        expected_generation_uuid: current(&g),
        branch_uuid: b.branch_uuid,
        version_uuid: Uuid::now_v7(),
        created_at: 2,
        query: "MATCH (n:Item) SET n.x = 1".into(),
    };
    g.execute_research_branch(&edit, &token).unwrap();
    g.execute("MATCH (n:Item) SET n.x = 1").unwrap();
    let op = g
        .prepare_research_version(PrepareResearchVersionRequest {
            operation_uuid: Uuid::now_v7(),
            version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
            label: None,
            description: None,
            created_at: 3,
            required_versions: Default::default(),
        })
        .unwrap();
    let destination = g
        .commit_research_version_operation(op, &token)
        .unwrap()
        .version_uuid
        .unwrap();
    let native = g.research_version(destination).unwrap();
    let projection = Uuid::now_v7();
    let projected = RegisterResearchVersion {
        version_uuid: projection,
        context_uuid: crate::research_claims::authority::project_uuid(
            &g.generation_for_read().unwrap(),
        )
        .unwrap(),
        source_generation_uuid: native.content.generation_uuid,
        source_version: Some(destination),
        selection: Some(
            native
                .content
                .participants
                .iter()
                .map(|p| p.key.clone())
                .collect(),
        ),
        required_versions: Default::default(),
        label: None,
        description: None,
        created_at: 4,
        evidence: native.content.evidence.clone(),
    };
    let root = g.resolved_generation.container_root().to_path_buf();
    graphforge_storage::research_versions::publish_research_operation(
        &root,
        &ResearchOperation {
            operation_uuid: Uuid::now_v7(),
            expected_generation_uuid: current(&g),
            mutation: ResearchMutation::Register(projected),
        },
        token.flag(),
    )
    .unwrap();
    let g = GraphForge::new(root.to_str()).unwrap();
    let view = g.open_research_branch(b.branch_uuid).unwrap();
    let rows = crate::branches::baseline::read(view.graph()).unwrap();
    let row = rows.values().find(|r| r.key.2 == "property:x").unwrap();
    let mut q = super::ResearchComparisonRequest {
        left: super::ResearchComparisonEndpoint::Branch {
            branch_uuid: b.branch_uuid,
        },
        right: super::ResearchComparisonEndpoint::Project,
        left_authority: None,
        right_authority: None,
        detail: super::ResearchComparisonDetail::Changes,
        accepted: vec![super::ResearchAcceptedContribution {
            source_version_uuid: edit.version_uuid,
            destination_version_uuid: destination,
            contribution_uuid: row.contribution,
            unit: super::ResearchFieldIdentity {
                object_kind: row.key.0.clone(),
                object_uuid: row.key.1,
                field: row.key.2.clone(),
            },
        }],
        max_fields: 40000,
        max_bytes: 64 * 1024 * 1024,
        page_size: 100,
        after: None,
    };
    assert!(g.compare_research(&q, &token).is_ok());
    // Same content and deliberately colliding context UUID still belongs to a
    // projection, not the complete Project authority.
    q.accepted[0].destination_version_uuid = projection;
    let error = g.compare_research(&q, &token).unwrap_err();
    assert!(error.to_string().contains("destination context"), "{error}");
}
