use super::*;

/// Three accepted chunks: a node chunk of three artifacts, then two edge
/// chunks of four, each artifact allocating 4096 bytes.
fn index() -> StagedIdentityIndex {
    let mut index = StagedIdentityIndex::default();
    for (sequence, count) in [(0_u64, 3_u8), (1, 4), (2, 4)] {
        for artifact in 0..count {
            index
                .entries
                .insert(staged_key(sequence, artifact), (sequence, 4096));
        }
        index.artifacts.push(count);
    }
    index
}

fn staged_key(sequence: u64, artifact: u8) -> String {
    format!("{:016x}:{sequence:02}{artifact:02}", 3)
}

fn staged_ledger(
    index: &StagedIdentityIndex,
    sequences: std::ops::Range<u64>,
) -> BTreeMap<String, u64> {
    index
        .entries
        .iter()
        .filter(|(_, (sequence, _))| sequences.contains(sequence))
        .map(|(key, (_, allocated))| (key.clone(), *allocated))
        .collect()
}

/// Restore what `restore_staged_ledger` restores from `[from, 3)`.
fn restored(
    index: &StagedIdentityIndex,
    persisted: &BTreeMap<String, u64>,
    from: u64,
) -> BTreeMap<String, u64> {
    let mut ledger = persisted.clone();
    for (key, allocated) in staged_ledger(index, from..3) {
        assert!(ledger.insert(key, allocated).is_none());
    }
    ledger
}

#[test]
fn staging_omits_every_staged_entry() {
    let index = index();
    let ledger = staged_ledger(&index, 0..3);
    let (persisted, from) = index.elide(&ledger, 3).unwrap().unwrap();
    assert!(persisted.is_empty());
    assert_eq!(from, 0);
    assert_eq!(restored(&index, &persisted, from), ledger);
}

#[test]
fn a_shape_end_keeps_its_outputs_and_omits_the_unretired_inputs() {
    let index = index();
    // The first chunk retired behind a sealing boundary, and one shaped
    // output reuses the inode its node parquet held, at another size.
    let mut ledger = staged_ledger(&index, 1..3);
    ledger.insert(staged_key(0, 0), 8192);
    ledger.insert(format!("{:016x}:ff00", 3), 12_288);
    let (persisted, from) = index.elide(&ledger, 3).unwrap().unwrap();
    assert_eq!(from, 1);
    assert_eq!(
        persisted,
        BTreeMap::from([
            (staged_key(0, 0), 8192),
            (format!("{:016x}:ff00", 3), 12_288),
        ])
    );
    assert_eq!(restored(&index, &persisted, from), ledger);
}

#[test]
fn only_a_suffix_of_whole_matching_chunks_is_omitted() {
    let index = index();
    // A partially retired middle chunk ends the suffix: the complete chunk
    // below it stays in the persisted ledger rather than being omitted.
    let mut ledger = staged_ledger(&index, 0..3);
    ledger.remove(&staged_key(1, 2));
    let (persisted, from) = index.elide(&ledger, 3).unwrap().unwrap();
    assert_eq!(from, 2);
    assert_eq!(
        persisted,
        staged_ledger(&index, 0..2)
            .into_iter()
            .filter(|(key, _)| *key != staged_key(1, 2))
            .collect()
    );
    assert_eq!(restored(&index, &persisted, from), ledger);

    // An entry whose allocation differs from its receipt is not reproducible
    // from the receipt, so neither it nor anything below it is omitted.
    let mut ledger = staged_ledger(&index, 0..3);
    ledger.insert(staged_key(2, 1), 8192);
    assert_eq!(index.elide(&ledger, 3).unwrap(), None);
}

#[test]
fn retired_inputs_omit_nothing() {
    let index = index();
    let ledger = BTreeMap::from([(format!("{:016x}:ff00", 3), 12_288)]);
    assert_eq!(index.elide(&ledger, 3).unwrap(), None);
    assert_eq!(index.elide(&BTreeMap::new(), 3).unwrap(), None);
}

#[test]
fn an_index_that_differs_from_the_journal_refuses_to_write() {
    let index = index();
    let ledger = staged_ledger(&index, 0..3);
    assert!(index.elide(&ledger, 4).is_err());
    assert!(index.elide(&ledger, 2).is_err());
    // A decoded checkpoint whose index was never rebuilt cannot be written.
    assert!(StagedIdentityIndex::default()
        .elide(&BTreeMap::new(), 1)
        .is_err());
}

/// A closed session with three accepted node chunks: its directory and the
/// ledger it held.
fn staged_session(
    root: &tempfile::TempDir,
    operation: u128,
) -> (std::path::PathBuf, BTreeMap<String, u64>) {
    let mut session = super::super::tests::open(root, operation);
    for chunk in 0..3_u128 {
        session
            .append(
                super::super::ConstructionChunkKind::Node,
                &format!("nodes-{chunk}"),
                &super::super::tests::node_batch(1 + chunk * 4, 4),
            )
            .unwrap();
    }
    let ledger = session
        .evidence()
        .storage_active_identity_allocated_bytes
        .clone();
    let path = session.root.path().to_path_buf();
    drop(session);
    (path, ledger)
}

fn edit_json(path: &std::path::Path, edit: impl FnOnce(&mut serde_json::Value)) {
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    edit(&mut value);
    std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
}

/// Reopen and require the restore to refuse with `expected`, leaving the
/// checkpoint exactly as it was.
fn assert_restore_refuses(
    root: &tempfile::TempDir,
    session: &std::path::Path,
    operation: u128,
    expected: &str,
) {
    let checkpoint = std::fs::read(session.join(super::super::CHECKPOINT)).unwrap();
    let error = super::super::GraphConstructionSession::open(
        root.path(),
        uuid::Uuid::from_u128(operation),
        0,
        super::super::GraphConstructionBudgets::default(),
    )
    .err()
    .unwrap_or_else(|| panic!("reopen must refuse: {expected}"))
    .to_string();
    assert!(error.contains(expected), "{error}");
    assert_eq!(
        std::fs::read(session.join(super::super::CHECKPOINT)).unwrap(),
        checkpoint
    );
}

#[test]
fn restore_refuses_a_broken_receipt_chain_link() {
    let root = tempfile::TempDir::new().unwrap();
    let (session, _) = staged_session(&root, 9_370);
    // The middle receipt names another predecessor. The tail receipt is
    // untouched, so only the per-link check can see it.
    edit_json(
        &session.join(super::super::intake::receipt_name(1)),
        |receipt| {
            receipt["prior_receipt_sha256"] = serde_json::json!("0".repeat(64));
        },
    );
    assert_restore_refuses(&root, &session, 9_370, "staged receipt chain changed");
}

#[test]
fn restore_refuses_a_journal_tail_that_differs_from_the_checkpoint() {
    let root = tempfile::TempDir::new().unwrap();
    let (session, _) = staged_session(&root, 9_371);
    // The last receipt changes: every link still holds, and only its digest
    // differs from the checkpoint's recorded tail.
    edit_json(
        &session.join(super::super::intake::receipt_name(2)),
        |receipt| {
            receipt["chunk_id"] = serde_json::json!("nodes-x");
        },
    );
    assert_restore_refuses(
        &root,
        &session,
        9_371,
        "staged receipt journal tail differs from checkpoint",
    );
}

#[test]
fn restore_refuses_an_omitted_suffix_beyond_the_journal() {
    let root = tempfile::TempDir::new().unwrap();
    let (session, _) = staged_session(&root, 9_372);
    edit_json(&session.join(super::super::CHECKPOINT), |checkpoint| {
        assert_eq!(checkpoint["staged_ledger_from_sequence"], 0);
        checkpoint["staged_ledger_from_sequence"] = serde_json::json!(3);
    });
    assert_restore_refuses(
        &root,
        &session,
        9_372,
        "checkpoint staged ledger suffix is out of range",
    );
}

#[test]
fn restore_refuses_an_entry_both_persisted_and_omitted() {
    let root = tempfile::TempDir::new().unwrap();
    let (session, ledger) = staged_session(&root, 9_373);
    let (key, allocated) = ledger.iter().next().unwrap();
    edit_json(&session.join(super::super::CHECKPOINT), |checkpoint| {
        assert_eq!(checkpoint["staged_ledger_from_sequence"], 0);
        checkpoint["evidence"]["storage_active_identity_allocated_bytes"] =
            serde_json::json!({ key.clone(): allocated });
    });
    assert_restore_refuses(
        &root,
        &session,
        9_373,
        "checkpoint persisted a staged ledger entry it also omitted",
    );
}
