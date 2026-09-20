//! Research Project metadata and bounded local discovery (#1348).

use graphforge_api::{
    DiscoverResearchProjectsRequest, GraphForge, ResearchProjectDiscoveryLimits,
    ResearchProjectDiscoveryQuery, ResearchTemporalCoverage, UpdateResearchMetadataRequest,
    WorkspaceResearchMetadata, WriteContext,
};
use graphforge_core::{GfError, ProjectErrorCode, uuid::Uuid};
use tempfile::TempDir;

fn open_admitted_graph(path: &str) -> Option<GraphForge> {
    match GraphForge::new(Some(path)) {
        Ok(graph) => Some(graph),
        Err(GfError::Project {
            code: ProjectErrorCode::UnsupportedFilesystem,
            ..
        }) => None,
        Err(error) => panic!("{error}"),
    }
}

#[test]
fn metadata_survives_reopen_and_discovery_filters_without_graph_open() {
    let first_root = TempDir::new().unwrap();
    let second_root = TempDir::new().unwrap();
    let first_path = first_root.path().to_str().unwrap();
    let second_path = second_root.path().to_str().unwrap();
    if open_admitted_graph(first_path).is_none() || open_admitted_graph(second_path).is_none() {
        return;
    }
    let mut first = GraphForge::new(Some(first_path)).unwrap();
    let _second = GraphForge::new(Some(second_path)).unwrap();

    let mut metadata = WorkspaceResearchMetadata::empty();
    metadata.title = Some("Arabian Nights".into());
    metadata.languages = vec!["ar".into()];
    metadata.subjects = vec!["literature".into()];
    metadata.ontologies = vec!["narrative-events".into()];
    metadata.temporal_coverage = Some(ResearchTemporalCoverage {
        start: Some("800".into()),
        end: Some("1500".into()),
        label: Some("800-1500 CE".into()),
    });
    first
        .update_research_metadata(UpdateResearchMetadataRequest {
            context: WriteContext {
                operation_uuid: graphforge_api::OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            metadata,
        })
        .unwrap();

    let reopened = GraphForge::new(Some(first_path)).unwrap();
    let metadata = reopened.research_project_metadata().unwrap();
    assert_eq!(metadata.title.as_deref(), Some("Arabian Nights"));
    let summary = reopened.research_project_summary().unwrap();
    assert_eq!(summary.metadata.title.as_deref(), Some("Arabian Nights"));
    assert_eq!(summary.metadata.languages, vec!["ar".to_string()]);

    let mut second_metadata = WorkspaceResearchMetadata::empty();
    second_metadata.title = Some("Medieval Latin Corpus".into());
    second_metadata.languages = vec!["la".into()];
    second_metadata.subjects = vec!["history".into()];
    GraphForge::new(Some(second_path))
        .unwrap()
        .update_research_metadata(UpdateResearchMetadataRequest {
            context: WriteContext {
                operation_uuid: graphforge_api::OperationId(Uuid::now_v7()),
                actor_uuid: None,
            },
            metadata: second_metadata,
        })
        .unwrap();

    let request = DiscoverResearchProjectsRequest {
        project_roots: vec![
            first_root.path().to_path_buf(),
            second_root.path().to_path_buf(),
        ],
        query: ResearchProjectDiscoveryQuery {
            languages: vec!["ar".into()],
            subjects: vec!["literature".into()],
            ontologies: vec!["narrative-events".into()],
            temporal_label: Some("800-1500".into()),
            ..ResearchProjectDiscoveryQuery::default()
        },
        limits: ResearchProjectDiscoveryLimits::default(),
    };
    let discovered = GraphForge::discover_research_projects(&request).unwrap();
    assert_eq!(discovered.batches.len(), 1);
    assert_eq!(discovered.batches[0].num_rows(), 1);
    let titles = discovered.batches[0]
        .column_by_name("title")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(titles.value(0), "Arabian Nights");
}
