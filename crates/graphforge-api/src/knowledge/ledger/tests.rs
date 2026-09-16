use super::super::tests::enable;
use super::super::tests::uuid7;
use super::super::*;
use super::*;
use crate::CapabilityId;

#[test]
fn empty_ledger_codecs_and_participant_contracts_are_exact() {
    let assertion = AssertionLedger::default();
    let confidence = ConfidenceLedger::default();
    let evidence = EvidenceLedger::default();
    let reasoning = ReasoningLedger::default();
    let status = AssertionStatusLedger::default();
    let supersession = AssertionSupersessionLedger::default();
    for participants in [
        encode_ledger(&assertion).unwrap(),
        encode_confidence_ledger(&confidence).unwrap(),
        encode_evidence_ledger(&evidence).unwrap(),
        encode_reasoning_ledger(&reasoning).unwrap(),
        encode_status_ledger(&status).unwrap(),
        encode_supersession_ledger(&supersession).unwrap(),
    ] {
        assert!(!participants.is_empty());
        assert!(participants.iter().all(|participant| {
            participant.encoding == ProjectParticipantEncoding::Parquet
                && participant.row_count == 0
                && !participant.bytes.is_empty()
        }));
        for participant in participants {
            assert!(read_parquet(&participant.bytes).unwrap().is_empty());
        }
    }

    let registry = schema_registry();
    let entry = registry
        .iter()
        .find(|entry| entry.record_family == "assertions")
        .unwrap();
    let snapshot = graphforge_storage::ProjectParticipantSnapshot {
        capability_id: entry.capability_id.into(),
        capability_version: entry.capability_version,
        record_family_id: entry.record_family.into(),
        record_version: entry.record_version,
        encoding: "parquet".into(),
        schema_fingerprint: entry.schema_fingerprint,
        row_count: 0,
        bytes: Vec::new(),
    };
    require_participant_contract(&snapshot, "assertions").unwrap();
    assert_eq!(read_or_empty(&snapshot, true).unwrap()[0].num_rows(), 0);
    assert_eq!(read_or_empty(&snapshot, false).unwrap()[0].num_rows(), 0);
    let mut incompatible = snapshot.clone();
    incompatible.encoding = "json".into();
    assert_eq!(
        require_participant_contract(&incompatible, "assertions")
            .unwrap_err()
            .code(),
        "GF_SCHEMA_MISMATCH"
    );
    assert_eq!(
        snapshot_to_participant(incompatible).unwrap().encoding,
        ProjectParticipantEncoding::Json
    );
    let mut unsupported = snapshot;
    unsupported.encoding = "sqlite".into();
    assert_eq!(
        snapshot_to_participant(unsupported).unwrap_err().code(),
        "GF_VALIDATION"
    );
}

fn generation_without_family(
    graph: &GraphForge,
    omitted_family: &str,
    transaction_uuid: Uuid,
    generation_uuid: Uuid,
) -> graphforge_storage::ResolvedProjectGeneration {
    let parent =
        graphforge_storage::resolve_project_generation(graph.resolved_generation.container_root())
            .unwrap();
    let participants = parent
        .participant_snapshots()
        .unwrap()
        .into_iter()
        .filter(|snapshot| snapshot.record_family_id != omitted_family)
        .map(snapshot_to_participant)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let request = ProjectGenerationRequest {
        transaction_uuid,
        generation_uuid,
        capabilities: parent
            .capabilities()
            .into_iter()
            .map(|capability| ProjectCapability {
                capability_id: capability.capability_id,
                capability_version: capability.capability_version,
            })
            .collect(),
        participants,
    };
    match graphforge_storage::stage_project_generation(
        graph.resolved_generation.container_root(),
        &request,
    )
    .unwrap()
    {
        ProjectStageOutcome::Staged(staged) => staged
            .validate(|_| Ok(()), |_, _| Ok(()))
            .unwrap()
            .publish()
            .unwrap(),
        ProjectStageOutcome::AlreadyPublished(_) => panic!("fresh generation expected"),
    };
    graphforge_storage::resolve_project_generation(graph.resolved_generation.container_root())
        .unwrap()
}

#[test]
fn incomplete_paired_knowledge_participants_fail_with_schema_mismatch() {
    for (omitted, confidence, seed) in [
        ("assertion_graph_refs", false, 120_u8),
        ("confidence_inputs", true, 130_u8),
    ] {
        let root = tempfile::tempdir().unwrap();
        let graph = GraphForge::new(root.path().to_str()).unwrap();
        enable(&graph, CapabilityId::Knowledge, seed);
        let generation =
            generation_without_family(&graph, omitted, uuid7(seed + 1), uuid7(seed + 2));
        let error = if confidence {
            read_confidence_ledger(&generation).unwrap_err()
        } else {
            read_ledger(&generation).unwrap_err()
        };
        assert_eq!(error.code(), "GF_SCHEMA_MISMATCH");
    }
}
