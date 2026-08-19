//! Integrated durability/isolation certification surface (#756).
//!
//! Required CI exercises the bounded seeded state machine. Native POSIX and
//! Windows subprocess failpoint matrices remain authoritative for real
//! API/handle behavior and are cross-checked against the shared oracle
//! boundaries here.

#![cfg(test)]

use std::collections::HashMap;

use arrow::array::{Array, StringArray};
use graphforge_storage::project_certification::{
    CERT_CONTRACT, CERT_SEED, WRITE_SKEW_CLASSIFICATION, evidence_summary, run_certification_suite,
};
use graphforge_storage::{ProjectRetentionLimits, ProjectRetentionPolicy};
use uuid::Uuid;

use crate::{
    CancellationToken, CheckpointRequest, GraphForge, GraphForgeOptions, OperationId,
    ProjectWriteMode, PropValue, WriteContext,
};

const READ_NAMES: &str = "MATCH (n:CertPerson) RETURN n.name AS name ORDER BY name";

fn uuid7(seed: u8) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = seed;
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    Uuid::from_bytes(bytes)
}

fn context(seed: u8) -> WriteContext {
    WriteContext {
        operation_uuid: OperationId(uuid7(seed)),
        actor_uuid: None,
    }
}

fn names(graph: &GraphForge) -> Vec<String> {
    let result = graph.execute(READ_NAMES).expect("certification query");
    let mut names = Vec::new();
    for batch in result.batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let values = batch
            .column_by_name("name")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .expect("certification name column");
        names.extend((0..values.len()).map(|row| values.value(row).to_owned()));
    }
    names
}

#[test]
fn seeded_certification_suite_is_clean_at_required_budget() {
    let evidence = run_certification_suite();
    assert_eq!(evidence.contract, CERT_CONTRACT);
    assert_eq!(evidence.seed, CERT_SEED);
    assert_eq!(evidence.issue, 756);
    assert_eq!(
        evidence.untriaged_failures,
        0,
        "{}",
        evidence_summary(&evidence)
    );
    assert!(!evidence.artifact_digest.is_empty());
    assert!(!evidence.commands.is_empty());
    let rendered = serde_json::to_string(&evidence)
        .unwrap()
        .to_ascii_lowercase();
    assert!(!rendered.contains("provides ssi"));
    assert!(!rendered.contains("serializable isolation"));
    assert!(!rendered.contains("distributed durability"));
    assert!(!rendered.contains("universal filesystem"));
}

#[test]
fn write_skew_classification_remains_honest() {
    assert_eq!(WRITE_SKEW_CLASSIFICATION, "allowed_documented_not_ssi");
    assert!(!WRITE_SKEW_CLASSIFICATION.contains("serializable"));
}

#[test]
fn production_histories_observe_pinned_and_reopened_state_in_every_write_mode() {
    for (mode_index, mode) in [
        ProjectWriteMode::SingleWriter,
        ProjectWriteMode::QueuedWriter,
        ProjectWriteMode::OptimisticMultiWriter,
    ]
    .into_iter()
    .enumerate()
    {
        let project = tempfile::tempdir().expect("certification project");
        let path = project.path().to_str().expect("utf-8 project path");
        let options = GraphForgeOptions {
            write_mode: mode,
            ..GraphForgeOptions::default()
        };
        let writer =
            GraphForge::new_with_options(Some(path), options.clone()).expect("writer open");
        let pinned = GraphForge::new_with_options(Some(path), options).expect("pinned open");
        assert!(names(&pinned).is_empty());

        let seed = u8::try_from(mode_index + 1).expect("bounded mode index");
        let tx = writer.begin_transaction(context(seed)).expect("begin");
        tx.stage_add_node(
            uuid7(seed + 16),
            "CertPerson",
            HashMap::from([("name".into(), PropValue::Str(format!("mode-{mode_index}")))]),
        )
        .expect("stage production node");
        tx.validate(&writer)
            .expect("validate production transaction");
        let receipt = tx.commit(&writer).expect("commit production transaction");

        // A facade is a pinned reader. It must not drift after a later commit.
        assert!(names(&pinned).is_empty());
        let reopened = GraphForge::new(Some(path)).expect("fresh reopen");
        assert_eq!(
            reopened.project_open_recovery().selected_generation_uuid,
            receipt.generation_uuid
        );
        assert_eq!(names(&reopened), [format!("mode-{mode_index}")]);

        reopened
            .checkpoint(CheckpointRequest {
                name: format!("mode-{mode_index}"),
                description: Some("production certification root".into()),
                idempotency_key: OperationId(uuid7(seed + 32)),
                actor_uuid: None,
            })
            .expect("checkpoint production generation");
        let reachability = reopened
            .inspect_project_reachability(
                ProjectRetentionPolicy::default(),
                ProjectRetentionLimits::default(),
            )
            .expect("inspect production reachability");
        assert!(
            reachability
                .checkpoint_roots
                .contains(&receipt.generation_uuid)
        );
        let preview = reopened
            .preview_project_cleanup(
                ProjectRetentionPolicy::default(),
                ProjectRetentionLimits::default(),
            )
            .expect("preview production cleanup");
        assert_eq!(preview.selected_generation_uuid, receipt.generation_uuid);
    }
}

#[test]
fn production_history_certifies_cancel_drop_and_idempotent_retry() {
    let project = tempfile::tempdir().expect("certification project");
    let path = project.path().to_str().expect("utf-8 project path");
    let graph = GraphForge::new_with_options(
        Some(path),
        GraphForgeOptions {
            write_mode: ProjectWriteMode::QueuedWriter,
            ..GraphForgeOptions::default()
        },
    )
    .expect("queued writer open");

    {
        let dropped = graph.begin_transaction(context(80)).expect("begin dropped");
        dropped
            .stage_add_node(
                uuid7(81),
                "CertPerson",
                HashMap::from([("name".into(), PropValue::Str("dropped".into()))]),
            )
            .expect("stage dropped");
    }
    assert!(names(&GraphForge::new(Some(path)).expect("reopen after drop")).is_empty());

    let cancelled = graph
        .begin_transaction(context(82))
        .expect("begin cancelled");
    cancelled
        .stage_add_node(
            uuid7(83),
            "CertPerson",
            HashMap::from([("name".into(), PropValue::Str("cancelled".into()))]),
        )
        .expect("stage cancelled");
    let token = CancellationToken::new();
    token.cancel();
    let error = cancelled
        .commit_with_cancellation(&graph, Some(token))
        .expect_err("pre-admission cancellation must fail");
    assert_eq!(error.code(), "GF_CANCELLED");
    assert!(names(&GraphForge::new(Some(path)).expect("reopen after cancel")).is_empty());

    let retry_context = context(84);
    let node = uuid7(85);
    let properties = HashMap::from([("name".into(), PropValue::Str("durable".into()))]);
    let first = graph
        .begin_transaction(retry_context.clone())
        .expect("begin first");
    first
        .stage_add_node(node, "CertPerson", properties.clone())
        .expect("stage first");
    let first_receipt = first.commit(&graph).expect("commit first");
    let retry = graph.begin_transaction(retry_context).expect("begin retry");
    retry
        .stage_add_node(node, "CertPerson", properties)
        .expect("stage retry");
    let retry_receipt = retry.commit(&graph).expect("commit retry");
    assert_eq!(first_receipt.generation_uuid, retry_receipt.generation_uuid);
    let reopened = GraphForge::new(Some(path)).expect("reopen durable retry");
    assert_eq!(names(&reopened), ["durable"]);
}
