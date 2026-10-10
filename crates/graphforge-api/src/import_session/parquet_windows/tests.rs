use graphforge_core::GfError;

use super::PhysicalBatchMap;

fn is_storage(error: &GfError) -> bool {
    matches!(error, GfError::Storage(_))
}

#[test]
fn physical_parquet_pieces_keep_logical_batch_and_row_identity() {
    let map = PhysicalBatchMap::new(10, 4, 2).unwrap();
    assert_eq!(map.physical_rows(), 2);
    assert_eq!(map.physical_batches(), 5);
    assert_eq!(map.identity(0).unwrap(), (0, 0, 2));
    assert_eq!(map.identity(1).unwrap(), (0, 2, 2));
    assert_eq!(map.identity(2).unwrap(), (1, 0, 2));
    assert_eq!(map.identity(3).unwrap(), (1, 2, 2));
    assert_eq!(map.identity(4).unwrap(), (2, 0, 2));
}

#[test]
fn physical_parquet_pieces_must_not_cross_logical_identity_boundaries() {
    assert!(is_storage(&PhysicalBatchMap::new(10, 4, 3).unwrap_err()));
    assert!(is_storage(&PhysicalBatchMap::new(10, 4, 0).unwrap_err()));
    assert!(is_storage(&PhysicalBatchMap::new(10, 4, 8).unwrap_err()));
}

#[test]
fn physical_piece_range_handles_the_u64_row_limit_without_wrapping() {
    let rows = u64::MAX;
    let map = PhysicalBatchMap::new(rows, 8, 4).unwrap();
    let final_index = map.physical_batches() - 1;
    let (start, end) = map.physical_range(final_index).unwrap();
    assert_eq!(start, rows - 3);
    assert_eq!(end, rows);
}
