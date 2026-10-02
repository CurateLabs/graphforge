use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, FixedSizeBinaryArray, Int64Array, ListBuilder, RecordBatch,
    StringArray, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};

use super::*;
use crate::property_overlay::PROPERTY_ORDINAL_KEY;

/// What a published fragment actually holds, read back from its file.
#[derive(Debug)]
pub(crate) struct FragmentStats {
    pub(crate) rows: usize,
    pub(crate) logical_bytes: u64,
    pub(crate) file_bytes: u64,
    pub(crate) first_uuid: [u8; 16],
    pub(crate) last_uuid: [u8; 16],
    pub(crate) ordinal_metadata: String,
}

pub(crate) fn fragment_stats(path: &std::path::Path) -> FragmentStats {
    let file_bytes = std::fs::metadata(path).unwrap().len();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(path).unwrap(),
    )
    .unwrap();
    let ordinal_metadata = reader.schema().metadata()[PROPERTY_ORDINAL_KEY].clone();
    let mut stats = FragmentStats {
        rows: 0,
        logical_bytes: 0,
        file_bytes,
        first_uuid: [0xff; 16],
        last_uuid: [0; 16],
        ordinal_metadata,
    };
    for batch in reader.build().unwrap() {
        let batch = batch.unwrap();
        let uuids = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let uuid: [u8; 16] = uuids.value(row).try_into().unwrap();
            if stats.rows == 0 {
                stats.first_uuid = uuid;
            }
            stats.last_uuid = uuid;
            stats.rows += 1;
        }
        stats.logical_bytes += row_charges(&batch).iter().sum::<u64>();
    }
    stats
}

/// A deterministic, poorly compressible `bytes`-long hex string.
pub(crate) fn wide_value(seed: u64, bytes: usize) -> String {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = String::with_capacity(bytes + 16);
    while out.len() < bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push_str(&format!("{state:016x}"));
    }
    out.truncate(bytes);
    out
}

/// Assert the cap invariants of one route's fragments, in `(generation,
/// ordinal)` order, and return what each holds: every fragment is within both
/// caps (a lone oversize row excepted), ordinals are dense per generation, the
/// UUID ranges are disjoint and ascending within a generation, and `rows`
/// rows are present in total.
pub(crate) fn assert_capped_fragments(
    fragments: &[crate::property_overlay::PropertyFragment],
    rows: usize,
) -> Vec<FragmentStats> {
    let stats = fragments
        .iter()
        .map(|fragment| fragment_stats(&fragment.path))
        .collect::<Vec<_>>();
    for stat in &stats {
        eprintln!("fragment {stat:?}");
    }
    assert_eq!(stats.iter().map(|stat| stat.rows).sum::<usize>(), rows);
    for (index, (fragment, stat)) in fragments.iter().zip(&stats).enumerate() {
        assert_eq!(
            stat.ordinal_metadata,
            fragment.id.ordinal.to_string(),
            "{}",
            fragment.path.display()
        );
        assert!(stat.rows <= MAX_PROPERTY_FRAGMENT_ROWS, "{stat:?}");
        assert!(
            stat.logical_bytes <= MAX_PROPERTY_FRAGMENT_BYTES || stat.rows == 1,
            "{stat:?}"
        );
        assert!(stat.file_bytes <= MAX_PROPERTY_FRAGMENT_BYTES, "{stat:?}");
        if let Some(previous) = index.checked_sub(1) {
            let before = &fragments[previous];
            if before.id.generation == fragment.id.generation {
                assert_eq!(before.id.ordinal + 1, fragment.id.ordinal);
                assert!(stats[previous].last_uuid < stat.first_uuid);
            } else {
                assert_eq!(fragment.id.ordinal, 0);
            }
        }
    }
    stats
}

fn fragments(pieces: &[FragmentPiece]) -> Vec<Range<usize>> {
    pieces.iter().map(|piece| piece.rows.clone()).collect()
}

#[test]
fn rows_cap_cuts_a_narrow_stream_into_full_fragments() {
    let rows = 2 * MAX_PROPERTY_FRAGMENT_ROWS + 5;
    let pieces = FragmentSplitter::default().push(&vec![1; rows]);
    assert_eq!(
        fragments(&pieces),
        vec![
            0..MAX_PROPERTY_FRAGMENT_ROWS,
            MAX_PROPERTY_FRAGMENT_ROWS..2 * MAX_PROPERTY_FRAGMENT_ROWS,
            2 * MAX_PROPERTY_FRAGMENT_ROWS..rows,
        ]
    );
    assert!(pieces.iter().all(|piece| piece.opens_fragment));
}

#[test]
fn bytes_cap_cuts_a_wide_stream_before_the_row_that_would_overflow() {
    let quarter = MAX_PROPERTY_FRAGMENT_BYTES / 4;
    let pieces = FragmentSplitter::default().push(&[quarter; 10]);
    assert_eq!(fragments(&pieces), vec![0..4, 4..8, 8..10]);
    // The fragment is full to the byte, never over.
    let pieces = FragmentSplitter::default().push(&[quarter + 1; 3]);
    assert_eq!(fragments(&pieces), vec![0..3]);
    let pieces = FragmentSplitter::default().push(&[quarter + 1; 4]);
    assert_eq!(fragments(&pieces), vec![0..3, 3..4]);
}

#[test]
fn a_row_larger_than_the_cap_is_a_fragment_of_its_own() {
    let pieces = FragmentSplitter::default().push(&[10, 3 * MAX_PROPERTY_FRAGMENT_BYTES, 10, 10]);
    assert_eq!(fragments(&pieces), vec![0..1, 1..2, 2..4]);
}

#[test]
fn cuts_do_not_depend_on_how_the_rows_arrive() {
    let charges = (0..300_000_u64)
        .map(|row| 1 + (row * 2_654_435_761) % 300)
        .collect::<Vec<_>>();
    let boundaries = |pieces: Vec<FragmentPiece>| {
        pieces
            .into_iter()
            .filter(|piece| piece.opens_fragment)
            .map(|piece| piece.rows.start)
            .collect::<Vec<_>>()
    };
    let at_once = boundaries(FragmentSplitter::default().push(&charges));
    assert!(at_once.len() > 3, "fixture must span several fragments");
    for chunk in [1, 7, 4096, 65_537] {
        let mut splitter = FragmentSplitter::default();
        let mut offset = 0;
        let mut chunked = Vec::new();
        for window in charges.chunks(chunk) {
            for piece in splitter.push(window) {
                if piece.opens_fragment {
                    chunked.push(offset + piece.rows.start);
                }
            }
            offset += window.len();
        }
        assert_eq!(chunked, at_once, "chunk size {chunk}");
    }
}

#[test]
fn row_charge_counts_value_offset_and_presence_bytes() {
    let mut list = ListBuilder::new(StringBuilder::new());
    list.values().append_value("abc");
    list.values().append_value("de");
    list.append(true);
    list.append(true);
    let schema = Schema::new(vec![
        Field::new("uuid", DataType::FixedSizeBinary(16), false),
        Field::new("tombstone", DataType::Boolean, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int64, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([[0_u8; 16], [1_u8; 16]].into_iter()).unwrap(),
            ) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false, false])),
            Arc::new(StringArray::from(vec![Some("hello"), Some("")])),
            Arc::new(Int64Array::from(vec![Some(7), None])),
            Arc::new(list.finish()),
        ],
    )
    .unwrap();
    let charges = row_charges(&batch);
    // uuid 16+1, tombstone 1+1, name payload+4+1, age 8+1, tags 4+1 plus its
    // two elements (payload + 4 + 1 each).
    assert_eq!(charges[0], 17 + 2 + (5 + 5) + 9 + (5 + (3 + 5) + (2 + 5)));
    assert_eq!(charges[1], 17 + 2 + 5 + 9 + 5);
    // Additive: a slice charges the sum of its rows.
    assert_eq!(
        row_charges(&batch.slice(0, 1))[0] + row_charges(&batch.slice(1, 1))[0],
        charges[0] + charges[1]
    );
}

#[test]
fn split_stamps_dense_ordinals_and_preserves_row_order() {
    let rows = MAX_PROPERTY_FRAGMENT_ROWS + MAX_PROPERTY_FRAGMENT_ROWS / 2;
    let uuids = (0..rows as u128).map(u128::to_be_bytes).collect::<Vec<_>>();
    let schema = Schema::new(vec![Field::new(
        "uuid",
        DataType::FixedSizeBinary(16),
        false,
    )])
    .with_metadata(
        [(PROPERTY_ORDINAL_KEY.to_owned(), "9".to_owned())]
            .into_iter()
            .collect(),
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(FixedSizeBinaryArray::try_from_iter(uuids.iter()).unwrap()) as ArrayRef],
    )
    .unwrap();
    let parts = split_into_fragments(&batch, 3).unwrap();
    let ordinals = parts
        .iter()
        .map(|part| part.schema().metadata()[PROPERTY_ORDINAL_KEY].clone())
        .collect::<Vec<_>>();
    assert_eq!(ordinals, ["3", "4"]);
    assert_eq!(parts[0].num_rows(), MAX_PROPERTY_FRAGMENT_ROWS);
    assert_eq!(parts[1].num_rows(), rows - MAX_PROPERTY_FRAGMENT_ROWS);
}
