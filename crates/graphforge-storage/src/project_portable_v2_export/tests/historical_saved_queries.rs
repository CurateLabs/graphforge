use super::*;
use crate::research_versions::{
    RESEARCH_VERSION, ResearchInterchangeManifest, ResearchInterchangeSelection,
    ResearchParticipantCommitment, ResearchParticipantKey, ResearchVersionContent,
    ResearchVersionRecord,
};
use serde_json::Value;

fn identity(version: &ResearchVersionRecord) -> [u8; 32] {
    // Re-sign the documented logical Version commitment, excluding physical locators.
    let mut logical = version.clone();
    logical.content.generation_uuid = Uuid::nil();
    logical.content.manifest_sha256 = [0; 32];
    Sha256::digest(serde_json::to_vec(&logical).unwrap()).into()
}

fn version(participants: Vec<ResearchParticipantCommitment>) -> ResearchVersionRecord {
    ResearchVersionRecord {
        version_uuid: Uuid::new_v4(),
        context_uuid: Uuid::new_v4(),
        label: None,
        description: None,
        created_at: 1,
        content: ResearchVersionContent {
            generation_uuid: Uuid::new_v4(),
            manifest_sha256: [7; 32],
            source_version: None,
            graph_projection: None,
            participants,
            required_versions: BTreeSet::new(),
            producer: concat!(
                "graphforge-storage/",
                env!("CARGO_PKG_VERSION"),
                ";research/6"
            )
            .into(),
            evidence: Vec::new(),
        },
    }
}

fn archived_queries() -> (
    tempfile::TempDir,
    ResolvedProjectGeneration,
    ResearchInterchangeManifest,
    crate::WorkspaceSavedQueries,
) {
    let query = crate::SavedQuery {
        query_uuid: Uuid::new_v4(),
        name: "Historical analysis".into(),
        description: None,
        query: "MATCH (n:NotPublishedYet) RETURN n.name".into(),
        parameters: BTreeMap::new(),
    };
    let definitions = crate::WorkspaceSavedQueries {
        queries: BTreeMap::from([(query.query_uuid, query)]),
        ..crate::WorkspaceSavedQueries::default()
    };
    let participant = definitions.to_project_participant().unwrap();
    let historical = version(vec![ResearchParticipantCommitment {
        key: ResearchParticipantKey {
            capability: participant.capability_id,
            family: participant.record_family_id,
        },
        capability_version: participant.capability_version,
        record_version: participant.record_version,
        encoding: "json".into(),
        schema_sha256: participant.schema_fingerprint,
        row_count: participant.row_count,
        content_sha256: Sha256::digest(&participant.bytes).into(),
    }]);
    let mut selected = version(Vec::new());
    selected
        .content
        .required_versions
        .insert(historical.version_uuid);
    let project_uuid = Uuid::new_v4();
    let archive = ResearchInterchangeManifest {
        contract_version: 1,
        research_capability_version: RESEARCH_VERSION,
        producer: concat!(
            "graphforge-storage/",
            env!("CARGO_PKG_VERSION"),
            ";research-interchange/1"
        )
        .into(),
        source_project_uuid: project_uuid,
        fork_project_uuid: None,
        fork: None,
        selected_version_uuid: selected.version_uuid,
        selection: ResearchInterchangeSelection::Complete {
            source_version_uuid: selected.version_uuid,
        },
        versions: BTreeMap::from([
            (selected.version_uuid, selected.clone()),
            (historical.version_uuid, historical.clone()),
        ]),
        version_projects: BTreeMap::from([
            (selected.version_uuid, project_uuid),
            (historical.version_uuid, project_uuid),
        ]),
        identities: BTreeMap::from([
            (selected.version_uuid, identity(&selected)),
            (historical.version_uuid, identity(&historical)),
        ]),
        genealogy: BTreeMap::new(),
        accepted: BTreeMap::new(),
        proof_exports: BTreeMap::new(),
    };
    let registry = archive.registry().unwrap();
    let (root, parent) = graph_generation();
    crate::graph_object_store::install_graph_object_bytes(root.path(), &participant.bytes).unwrap();
    let mut participants = parent
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .map(|snapshot| crate::ProjectParticipant {
            capability_id: snapshot.capability_id,
            capability_version: snapshot.capability_version,
            record_family_id: snapshot.record_family_id,
            record_version: snapshot.record_version,
            encoding: crate::ProjectParticipantEncoding::Json,
            schema_fingerprint: snapshot.schema_fingerprint,
            row_count: snapshot.row_count,
            bytes: snapshot.bytes,
        })
        .collect::<Vec<_>>();
    participants.push(registry.participant().unwrap());
    participants.sort_by(|left, right| {
        (&left.capability_id, &left.record_family_id)
            .cmp(&(&right.capability_id, &right.record_family_id))
    });
    let request = crate::ProjectGenerationRequest {
        transaction_uuid: Uuid::new_v4(),
        generation_uuid: Uuid::new_v4(),
        capabilities: vec![
            crate::ProjectCapability {
                capability_id: "graph".into(),
                capability_version: 1,
            },
            crate::ProjectCapability {
                capability_id: "research".into(),
                capability_version: RESEARCH_VERSION,
            },
            crate::ProjectCapability {
                capability_id: "workspace".into(),
                capability_version: 1,
            },
        ],
        participants,
    };
    let crate::ProjectStageOutcome::Staged(staged) =
        crate::stage_project_generation_with_graph_tree(
            root.path(),
            &request,
            Some(&parent.graph_tree_root()),
        )
        .unwrap()
    else {
        panic!("fresh archive publication");
    };
    staged
        .validate(|_| Ok(()), |_, _| Ok(()))
        .unwrap()
        .publish()
        .unwrap();
    let generation = crate::resolve_project_generation(root.path()).unwrap();
    assert!(
        crate::read_workspace_saved_queries(&generation)
            .unwrap()
            .queries
            .is_empty()
    );
    (root, generation, archive, definitions)
}

#[test]
fn full_package_verification_and_import_decode_required_historical_saved_queries() {
    let (_root, generation, mut archive, mut definitions) = archived_queries();
    let limits = PortableV2Limits::default();
    let mut plan = plan_complete_portable_v2(&generation, limits).unwrap();
    let valid_outputs = tempfile::tempdir().unwrap();
    let (expanded, bundle) = write_test_representations(&plan, valid_outputs.path());
    for source in [&expanded, &bundle] {
        verify_portable_v2(source, PortableV2Mode::Full, limits, None).unwrap();
    }
    // Current and selected content have no saved-query participant. Only a
    // required historical Version receives authenticated but invalid Cypher.
    definitions.queries.values_mut().next().unwrap().query = "CREATE (n)".into();
    let mut malformed = serde_json::to_vec(&definitions).unwrap();
    malformed.push(b'\n');
    let historical = archive
        .versions
        .values_mut()
        .find(|version| !version.content.participants.is_empty())
        .unwrap();
    let old_digest = hex(historical.content.participants[0].content_sha256);
    let new_digest: [u8; 32] = Sha256::digest(&malformed).into();
    historical.content.participants[0].content_sha256 = new_digest;
    archive
        .identities
        .insert(historical.version_uuid, identity(historical));
    let registry = archive.registry().unwrap();
    let registry_id = planning::portable_participant_id("research", "registry");
    let registry_path = format!("data/components/research/{registry_id}/participant.json");
    replace_test_control(
        &mut plan,
        &registry_path,
        registry.participant().unwrap().bytes,
    );
    let old_path = format!("data/components/research/research-content/{old_digest}");
    let new_path = format!(
        "data/components/research/research-content/{}",
        hex(new_digest)
    );
    replace_test_control(&mut plan, &old_path, malformed);
    plan.files
        .iter_mut()
        .find(|file| file.path == old_path)
        .unwrap()
        .path
        .clone_from(&new_path);
    let mut manifest: Value = serde_json::from_slice(&plan.manifest).unwrap();
    let file = manifest["components"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .flat_map(|component| component["files"].as_array_mut().unwrap())
        .find(|file| file["path"] == old_path)
        .unwrap();
    file["path"] = new_path.into();
    plan.selection_fingerprint = format!(
        "sha256:{}",
        hex(Sha256::digest(serde_json::to_vec(&registry.interchange).unwrap()).into())
    );
    plan.manifest = canonical_json(&manifest).unwrap();
    resign_test_manifest(&mut plan);
    let outputs = tempfile::tempdir().unwrap();
    let (expanded, bundle) = write_test_representations(&plan, outputs.path());
    for source in [&expanded, &bundle] {
        verify_portable_v2(source, PortableV2Mode::StructureOnly, limits, None).unwrap();
        let error = verify_portable_v2(source, PortableV2Mode::Full, limits, None).unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
        assert!(
            error.to_string().contains(
                "research component identity, schema, provenance or selected closure is invalid"
            ),
            "{error}"
        );
        let destination = tempfile::tempdir().unwrap();
        let target = destination.path().join("refused");
        let mut native_calls = 0;
        let error =
            crate::project_portable_v2_import::import_complete_portable_v2_with_native_validation(
                source,
                &target,
                Uuid::new_v4(),
                Uuid::new_v4(),
                &[
                    crate::ProjectCapability {
                        capability_id: "graph".into(),
                        capability_version: 1,
                    },
                    crate::ProjectCapability {
                        capability_id: "research".into(),
                        capability_version: RESEARCH_VERSION,
                    },
                    crate::ProjectCapability {
                        capability_id: "workspace".into(),
                        capability_version: 1,
                    },
                ],
                limits,
                None,
                |_| {},
                None,
                &mut |_, _, _| {
                    native_calls += 1;
                    Ok(())
                },
            )
            .unwrap_err();
        assert_eq!(error.code, PortableV2ErrorCode::Incompatible);
        assert_eq!(
            native_calls, 0,
            "storage must refuse before the native callback"
        );
        assert!(
            !target.exists(),
            "historical native refusal must precede destination publication"
        );
    }
}
