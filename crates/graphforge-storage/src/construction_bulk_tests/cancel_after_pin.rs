// A cancel that lands after the inventory is pinned.
//
// Included at the end of `bulk_builder`, whose helpers it shares. The encoding
// is complete once its inventory is pinned in the checkpoint; a cancel after
// that only interrupts the reclaim of superseded payloads, and the next call
// reuses the pinned inventory (#1881).

/// Cancel a build at the first poll after its inventory file is installed.
/// The pin follows the install without a poll, so the cancel always lands in
/// the reclaim of superseded payloads, after the build is complete.
fn cancel_after_inventory_install(
    session: &mut GraphConstructionSession,
    plan: &BulkBuildPlan<'_>,
) -> GfError {
    let installed = session
        .root
        .path()
        .join("encoded-v1")
        .join("inventory.json");
    session
        .prepare_bulk_encoding(1, plan, || installed.exists())
        .unwrap_err()
}

/// The call after a cancelled attempt, with the budget cut below the fixed
/// floor. What the cancelled attempt left is session state, not timing:
/// - not pinned: nothing was built, so the cut budget is refused before any
///   decoding;
/// - pinned: the build is complete (the cancel only interrupted the reclaim
///   of superseded payloads), so the call reuses the pinned inventory and
///   consults no budget.
fn reduced_budget_after_cancel(
    session: &mut GraphConstructionSession,
    mut reduced: BulkBuildPlan<'_>,
    expected: &Inventory,
    context: &str,
) {
    reduced.memory_budget = Some(1);
    let pinned_before = session.checkpoint.encoding_inventory_sha256.is_some();
    let result = session.prepare_bulk_encoding(1, &reduced, || false);
    if pinned_before {
        let reused = result.unwrap_or_else(|error| {
            panic!("{context}: the pinned inventory was not reused: {error}")
        });
        assert_eq!(expected, &inventory(&reused), "{context}");
    } else {
        assert!(
            matches!(
                result,
                Err(GfError::Project {
                    code: graphforge_core::ProjectErrorCode::ResourceLimit,
                    ..
                })
            ),
            "{context}: {result:?}"
        );
    }
    assert!(!scratch_dir(session).exists(), "{context}");
}

#[test]
fn a_cancel_after_the_inventory_is_pinned_leaves_a_complete_scratch_build_the_next_call_reuses() {
    let _gate = crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::with_gate(
        32 << 10,
    );
    let (nodes, edges) = graph(1_021, 3_001, 700, scattered);
    let expected = bulk_with(&nodes, &edges, 2, 4).unwrap();
    let _forced =
        crate::graph_construction_encoding::bulk_test_support::ForcedPartitions::set(6, 4);
    let root = TempDir::new().unwrap();
    let mut session = pinned(&root);
    let error = cancel_after_inventory_install(&mut session, &scratch_plan(&nodes, &edges, 2));
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert!(!scratch_dir(&session).exists());
    assert!(
        session.checkpoint.encoding_inventory_sha256.is_some(),
        "the cancel landed before the inventory was pinned"
    );
    reduced_budget_after_cancel(
        &mut session,
        scratch_plan(&nodes, &edges, 2),
        &expected,
        "cancelled after the pin",
    );
    let rerun = session
        .prepare_bulk_encoding(1, &scratch_plan(&nodes, &edges, 2), || false)
        .unwrap();
    assert_eq!(expected, inventory(&rerun));
    assert!(!scratch_dir(&session).exists());
}
