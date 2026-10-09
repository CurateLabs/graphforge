use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode};

use crate::CancellationToken;
use crate::import_session::inventory_budget::InventoryBudget;

use super::{LeafProgress, LeafProgressInventory, WindowLedger};

fn ledger(
    group_start: u64,
    rows: u64,
    batch_rows: u64,
    window_limit: u64,
    budget: &mut InventoryBudget,
) -> WindowLedger {
    WindowLedger::new(group_start, rows, batch_rows, window_limit, budget, None).unwrap()
}

fn progress(group_start: u64, rows: u64, batch_rows: u64, events: u64) -> LeafProgress {
    LeafProgress::new(group_start, rows, batch_rows, events, None).unwrap()
}

fn is_resource_limit(error: &GfError) -> bool {
    matches!(
        error,
        GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        }
    )
}

fn is_storage(error: &GfError) -> bool {
    matches!(error, GfError::Storage(_))
}

#[test]
fn a_repeated_row_remains_in_one_window_across_pages_and_refuses_before_excess() {
    let mut budget = InventoryBudget::new(4096);
    let mut ledger = ledger(0, 1, 8, 10, &mut budget);
    let mut leaf = progress(0, 1, 8, 5);

    assert_eq!(
        leaf.process_block(&mut ledger, &[0, 1], &[2, 2], None)
            .unwrap(),
        2
    );
    assert_eq!(
        leaf.process_block(&mut ledger, &[1, 1], &[2, 2], None)
            .unwrap(),
        2
    );
    assert_eq!(ledger.batch_bytes(0).unwrap(), 8);

    let error = leaf
        .process_block(&mut ledger, &[1], &[3], None)
        .unwrap_err();
    assert!(is_resource_limit(&error), "{error}");
    assert_eq!(ledger.batch_bytes(0).unwrap(), 8);
    assert_eq!(leaf.events_seen(), 4);
    assert_eq!(leaf.rows_started(), 1);
}

#[test]
fn oversized_caller_blocks_are_processed_in_capped_resumable_chunks() {
    let mut budget = InventoryBudget::new(32_768);
    let mut ledger = ledger(0, 1, 2_000, 2_000, &mut budget);
    let mut leaf = progress(0, 1, 2_000, 1_025);
    let repetitions = std::iter::once(0_i16)
        .chain(std::iter::repeat_n(1_i16, 1_024))
        .collect::<Vec<_>>();
    let increments = vec![1_u64; repetitions.len()];

    let consumed = leaf
        .process_block(&mut ledger, &repetitions, &increments, None)
        .unwrap();
    assert_eq!(consumed, 1_024);
    assert_eq!(leaf.events_seen(), 1_024);
    assert_eq!(ledger.batch_bytes(0).unwrap(), 1_024);
    let consumed = leaf
        .process_block(
            &mut ledger,
            &repetitions[consumed..],
            &increments[consumed..],
            None,
        )
        .unwrap();
    assert_eq!(consumed, 1);
    leaf.finish(None).unwrap();
    assert_eq!(ledger.batch_bytes(0).unwrap(), 1_025);
}

#[test]
fn projected_leaves_share_the_same_window_limit() {
    let mut budget = InventoryBudget::new(4096);
    let mut ledger = ledger(0, 1, 4, 10, &mut budget);
    let mut first = progress(0, 1, 4, 1);
    let mut second = progress(0, 1, 4, 1);
    first.process_block(&mut ledger, &[0], &[6], None).unwrap();
    assert_eq!(ledger.batch_bytes(0).unwrap(), 6);

    let error = second
        .process_block(&mut ledger, &[0], &[6], None)
        .unwrap_err();
    assert!(is_resource_limit(&error), "{error}");
    assert_eq!(ledger.batch_bytes(0).unwrap(), 6);
    assert_eq!(second.events_seen(), 0);
}

#[test]
fn row_groups_reuse_the_task_window_at_a_batch_boundary() {
    let mut budget = InventoryBudget::new(4096);
    let mut ledger = ledger(0, 4, 4, 10, &mut budget);
    let mut first_group_leaf = progress(0, 2, 4, 2);
    let mut second_group_leaf = progress(2, 2, 4, 2);

    first_group_leaf
        .process_block(&mut ledger, &[0, 0], &[3, 3], None)
        .unwrap();
    first_group_leaf.finish(None).unwrap();
    assert_eq!(ledger.batch_bytes(0).unwrap(), 6);

    let error = second_group_leaf
        .process_block(&mut ledger, &[0, 0], &[3, 3], None)
        .unwrap_err();
    assert!(is_resource_limit(&error), "{error}");
    assert_eq!(ledger.batch_bytes(0).unwrap(), 9);
    assert_eq!(second_group_leaf.events_seen(), 1);
}

#[test]
fn group_rows_cross_global_batch_boundaries() {
    let mut budget = InventoryBudget::new(4096);
    let mut ledger = ledger(3, 4, 4, 12, &mut budget);
    let mut leaf = progress(3, 4, 4, 4);
    leaf.process_block(&mut ledger, &[0, 0, 0, 0], &[2, 3, 4, 5], None)
        .unwrap();
    leaf.finish(None).unwrap();

    assert_eq!(ledger.batch_totals().collect::<Vec<_>>(), [(0, 2), (1, 12)]);
    assert_eq!(ledger.current_bytes().unwrap(), 14);
}

#[test]
fn cross_global_batch_charge_refuses_one_byte_below_the_actual_total() {
    let mut budget = InventoryBudget::new(4096);
    let mut ledger = ledger(3, 4, 4, 11, &mut budget);
    let mut leaf = progress(3, 4, 4, 4);
    let error = leaf
        .process_block(&mut ledger, &[0, 0, 0, 0], &[2, 3, 4, 5], None)
        .unwrap_err();
    assert!(is_resource_limit(&error), "{error}");
    assert_eq!(ledger.batch_bytes(0).unwrap(), 2);
    assert_eq!(ledger.batch_bytes(1).unwrap(), 7);
    assert_eq!(leaf.events_seen(), 3);
}

#[test]
fn a_continuation_without_an_open_row_is_rejected() {
    let mut budget = InventoryBudget::new(4096);
    let mut ledger = ledger(0, 1, 4, 10, &mut budget);
    let mut leaf = progress(0, 1, 4, 1);
    let error = leaf
        .process_block(&mut ledger, &[1], &[1], None)
        .unwrap_err();
    assert!(is_storage(&error), "{error}");
    assert_eq!(ledger.current_bytes().unwrap(), 0);
    assert_eq!(leaf.events_seen(), 0);
}

#[test]
fn finish_requires_exact_footer_rows_and_events() {
    let mut budget = InventoryBudget::new(4096);
    let mut ledger = ledger(0, 1, 4, 10, &mut budget);
    let mut leaf = progress(0, 1, 4, 2);
    leaf.process_block(&mut ledger, &[0], &[1], None).unwrap();
    let error = leaf.finish(None).unwrap_err();
    assert!(is_storage(&error), "{error}");
}

#[test]
fn a_combined_window_floor_is_charged_once() {
    let mut budget = InventoryBudget::new(4096);
    let mut ledger = ledger(0, 1, 4, 10, &mut budget);
    ledger.apply_window_floor(0, 4, None).unwrap();
    assert_eq!(ledger.batch_bytes(0).unwrap(), 4);
    let error = ledger.apply_window_floor(0, 4, None).unwrap_err();
    assert!(is_storage(&error), "{error}");

    let mut leaf = progress(0, 1, 4, 1);
    leaf.process_block(&mut ledger, &[0], &[3], None).unwrap();
    assert_eq!(ledger.batch_bytes(0).unwrap(), 7);
}

#[test]
fn empty_progress_and_ledger_paths_observe_cancellation() {
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let mut budget = InventoryBudget::new(4096);
    let error = WindowLedger::new(0, 0, 4, 10, &mut budget, Some(&cancelled)).unwrap_err();
    assert!(matches!(
        error,
        GfError::Api {
            code: ApiErrorCode::Cancelled,
            ..
        }
    ));

    let mut budget = InventoryBudget::new(4096);
    let mut ledger = ledger(0, 0, 4, 10, &mut budget);
    let mut leaf = progress(0, 0, 4, 0);
    let error = leaf
        .process_block(&mut ledger, &[], &[], Some(&cancelled))
        .unwrap_err();
    assert!(matches!(
        error,
        GfError::Api {
            code: ApiErrorCode::Cancelled,
            ..
        }
    ));
    let error = leaf.finish(Some(&cancelled)).unwrap_err();
    assert!(matches!(
        error,
        GfError::Api {
            code: ApiErrorCode::Cancelled,
            ..
        }
    ));
    let error = ledger
        .apply_window_floor(0, 0, Some(&cancelled))
        .unwrap_err();
    assert!(matches!(
        error,
        GfError::Api {
            code: ApiErrorCode::Cancelled,
            ..
        }
    ));
}

#[test]
fn an_empty_unaligned_task_range_has_no_window_entries() {
    let mut budget = InventoryBudget::new(4096);
    let ledger = WindowLedger::new(1, 0, 4, 10, &mut budget, None).unwrap();
    assert_eq!(ledger.batch_totals().count(), 0);
    assert_eq!(ledger.current_bytes().unwrap(), 0);
    assert_eq!(ledger.inventory_bytes().unwrap(), 0);
}

#[test]
fn window_and_leaf_inventories_reserve_before_growing() {
    let mut budget = InventoryBudget::new(64);
    let mut inventory = LeafProgressInventory::new();
    inventory.push(progress(0, 1, 4, 1), &mut budget).unwrap();
    assert_eq!(inventory.len(), 1);
    assert!(!inventory.is_empty());
    assert!(inventory.inventory_bytes().unwrap() <= budget.live_bytes());

    let mut denied_budget = InventoryBudget::new(0);
    let mut denied_inventory = LeafProgressInventory::new();
    let error = denied_inventory
        .push(progress(0, 1, 4, 1), &mut denied_budget)
        .unwrap_err();
    assert!(is_resource_limit(&error), "{error}");
    assert!(denied_inventory.is_empty());
    assert_eq!(denied_budget.live_bytes(), 0);

    let error = WindowLedger::new(0, 32, 1, 10, &mut InventoryBudget::new(1), None).unwrap_err();
    assert!(is_resource_limit(&error), "{error}");
}
