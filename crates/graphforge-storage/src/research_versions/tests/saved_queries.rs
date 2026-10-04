use super::*;

#[test]
fn signed_historical_saved_query_definitions_are_decoded_before_materialization() {
    let source = tempfile::tempdir().unwrap();
    fixture(source.path(), "current has no saved queries");
    let current = crate::resolve_project_generation(source.path()).unwrap();
    let query = crate::SavedQuery {
        query_uuid: Uuid::now_v7(),
        name: "Historical analysis".into(),
        description: None,
        query: "RETURN $value".into(),
        parameters: BTreeMap::from([("value".into(), crate::SavedQueryParameterType::Integer)]),
    };
    let original = crate::WorkspaceSavedQueries {
        queries: BTreeMap::from([(query.query_uuid, query)]),
        ..crate::WorkspaceSavedQueries::default()
    };
    for case in ["valid", "mutation", "declarations", "header"] {
        let mut definitions = original.clone();
        match case {
            "mutation" => {
                let query = definitions.queries.values_mut().next().unwrap();
                query.query = "CREATE (n)".into();
                query.parameters.clear();
            }
            "declarations" => definitions
                .queries
                .values_mut()
                .next()
                .unwrap()
                .parameters
                .clear(),
            _ => {}
        }
        // Encode and authenticate invalid native content deliberately; a generic
        // hash/registry check cannot distinguish it from a valid saved definition.
        let mut bytes = serde_json::to_vec(&definitions).unwrap();
        bytes.push(b'\n');
        let (digest, _) =
            crate::graph_object_store::install_graph_object_bytes(source.path(), &bytes).unwrap();
        let participant = original.to_project_participant().unwrap();
        let version = ResearchVersionRecord {
            parents: Vec::new(),
            author: None,
            committer: None,
            provenance: None,
            version_uuid: Uuid::now_v7(),
            context_uuid: Uuid::now_v7(),
            label: None,
            description: None,
            created_at: 1,
            content: ResearchVersionContent {
                generation_uuid: current.generation_uuid(),
                manifest_sha256: current.manifest_sha256(),
                source_version: None,
                graph_projection: None,
                participants: vec![ResearchParticipantCommitment {
                    key: ResearchParticipantKey {
                        capability: participant.capability_id,
                        family: participant.record_family_id,
                    },
                    capability_version: participant.capability_version,
                    record_version: if case == "header" {
                        2
                    } else {
                        participant.record_version
                    },
                    encoding: "json".into(),
                    schema_sha256: participant.schema_fingerprint,
                    row_count: participant.row_count,
                    content_sha256: Sha256::digest(&bytes).into(),
                }],
                required_versions: BTreeSet::new(),
                producer: PRODUCER.into(),
                evidence: Vec::new(),
            },
        };
        assert_eq!(digest, hex(&version.content.participants[0].content_sha256));
        let registry = ResearchRegistry {
            versions: BTreeMap::from([(version.version_uuid, version.clone())]),
            identities: BTreeMap::from([(
                version.version_uuid,
                identity_digest(&version).unwrap(),
            )]),
            materialized: BTreeSet::from([version.version_uuid]),
            ..ResearchRegistry::default()
        };
        registry.validate().unwrap();
        let inspection = inspect_with_registry(source.path(), &version, &registry);
        let destination = tempfile::tempdir().unwrap();
        let target = destination.path().join("historical");
        std::fs::create_dir(&target).unwrap();
        let materialized = materialize_prepared_research_version(source.path(), &version, &target);
        if case == "valid" {
            assert_eq!(inspection.unwrap()[0].bytes, bytes);
            assert_eq!(
                crate::read_workspace_saved_queries(&materialized.unwrap()).unwrap(),
                original
            );
        } else {
            assert_eq!(
                inspection.unwrap_err().code(),
                "GF_PROJECT_CORRUPT",
                "{case}"
            );
            assert_eq!(
                materialized.unwrap_err().code(),
                "GF_PROJECT_CORRUPT",
                "{case}"
            );
            assert!(
                std::fs::read_dir(&target).unwrap().next().is_none(),
                "{case} must leave the private target empty before publication"
            );
        }
    }
    assert_eq!(
        crate::resolve_project_generation(source.path())
            .unwrap()
            .generation_uuid(),
        current.generation_uuid()
    );
    assert!(
        crate::read_workspace_saved_queries(&current)
            .unwrap()
            .queries
            .is_empty()
    );
}
