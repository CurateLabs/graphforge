use super::super::intake::write_fixed_run;
use super::super::tests::{node_batch, open, tree_has_no_temps};
use super::super::*;
use super::*;

use tempfile::TempDir;

#[test]
fn fixed_run_receipts_count_cache_rollovers_and_preserve_reopened_bytes() {
    let window = graphforge_filesystem::cache_release_window_for_streams(4)
        .unwrap()
        .get();
    assert_eq!(window, 16 * 1024 * 1024);
    let supported = cfg!(target_os = "linux");
    for bytes in [window - 1, window, window + 1] {
        let temporary = TempDir::new().unwrap();
        let root = StableDirectory::open(temporary.path()).unwrap();
        let records = vec![[0x5a_u8]; usize::try_from(bytes).unwrap()];
        let mut evidence = GraphConstructionEvidence::default();
        let receipt = write_fixed_run(&root, "window.run", &records, &mut evidence).unwrap();
        // The existing protocol has one final file barrier and two
        // namespace barriers. Only a supported full-window rollover adds
        // another file barrier; an exact final window is not charged twice.
        let rollovers = u64::from(supported && bytes > window);
        assert_eq!(receipt.fsync_operations, 3 + rollovers);
        assert_eq!(receipt.bytes, bytes);
        assert_eq!(
            evidence.cache_release_operations,
            u64::from(supported) * (1 + rollovers)
        );
        assert_eq!(
            evidence.cache_release_unsupported_operations,
            u64::from(!supported)
        );
        assert_eq!(evidence.cache_released_bytes, u64::from(supported) * bytes);
        assert_eq!(
            evidence.peak_cache_release_window_bytes,
            if supported { bytes.min(window) } else { bytes }
        );
        drop(root);

        let reopened = StableDirectory::open(temporary.path()).unwrap();
        let mut file = reopened.open_child_file(OsStr::new("window.run")).unwrap();
        let mut actual = Vec::new();
        file.read_to_end(&mut actual).unwrap();
        assert_eq!(u64::try_from(actual.len()).unwrap(), bytes);
        assert!(actual.iter().all(|byte| *byte == 0x5a));
        assert_eq!(hex(&Sha256::digest(&actual)), receipt.sha256);
        assert!(tree_has_no_temps(temporary.path()));
    }
}

#[test]
fn checkpoint_compacts_live_transition_history_without_losing_peak_or_union() {
    let root = TempDir::new().unwrap();
    let operation = 9_901_u128;
    let mut session = open(&root, operation);
    session
        .append(ConstructionChunkKind::Node, "n", &node_batch(1, 32))
        .unwrap();
    let active = session
        .checkpoint
        .evidence
        .storage_active_identity_allocated_bytes
        .clone();
    let peak = session
        .checkpoint
        .evidence
        .storage_transient_peak_total_allocated_bytes;
    let transition = session
        .checkpoint
        .evidence
        .storage_allocation_transitions
        .last()
        .unwrap()
        .clone();
    session.checkpoint.evidence.storage_allocation_transitions = vec![transition; 20_000];
    replace_checkpoint_control(&session.root, &session.checkpoint).unwrap();
    assert!(
        session
            .root
            .open_child_file(OsStr::new(CHECKPOINT))
            .unwrap()
            .metadata()
            .unwrap()
            .len()
            < MAX_CONTROL_BYTES
    );
    drop(session);
    let reopened = GraphConstructionSession::resume_with_mode_and_lifecycle(
        root.path(),
        Uuid::from_u128(operation),
        graphforge_core::OntologyMode::Exploratory,
        GraphConstructionBudgets::default(),
        crate::filesystem_admission::ProjectLifecycleMode::Durable,
    )
    .unwrap();
    assert_eq!(
        reopened.evidence().storage_active_identity_allocated_bytes,
        active
    );
    assert_eq!(
        reopened
            .evidence()
            .storage_transient_peak_total_allocated_bytes,
        peak
    );
    assert_eq!(reopened.evidence().storage_allocation_transitions.len(), 1);
}

#[test]
fn counting_reader_uses_authenticated_cas_length() {
    let root = TempDir::new().unwrap();
    let payload = b"authenticated parquet-sized payload";
    let (digest, _) = crate::install_graph_object_bytes(root.path(), payload).unwrap();
    let file = crate::graph_object_store::open_graph_object_by_digest(
        root.path(),
        &digest,
        payload.len() as u64,
    )
    .unwrap();
    let reader = CountingChunkReader::new(file, IoCounter::default());
    assert_eq!(Length::len(&reader), payload.len() as u64);
}
