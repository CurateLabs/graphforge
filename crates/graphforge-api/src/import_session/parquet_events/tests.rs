use bytes::Bytes;
use graphforge_core::{ApiErrorCode, GfError};
use parquet::basic::Encoding;
use parquet::column::page::Page;
use parquet::encodings::levels::LevelEncoder;
use parquet::util::bit_util::BitWriter;

use crate::CancellationToken;
use crate::import_session::parquet_events::PageEvents;
use crate::import_session::parquet_levels::MAX_BLOCK_EVENTS;

fn v1_encoded(maximum: i16, levels: &[i16]) -> Vec<u8> {
    let mut encoder = LevelEncoder::v1_streaming(maximum);
    encoder.put_with_observer(levels, |_, _| {});
    encoder.consume()
}

fn v2_encoded(maximum: i16, levels: &[i16]) -> Vec<u8> {
    let mut encoder = LevelEncoder::v2_streaming(maximum);
    encoder.put_with_observer(levels, |_, _| {});
    encoder.consume()
}

fn append_v1_rle_tail(section: &mut Vec<u8>, tail: &[u8]) {
    let declared = i32::from_le_bytes(section[..4].try_into().unwrap());
    let declared = declared
        .checked_add(i32::try_from(tail.len()).unwrap())
        .unwrap();
    section[..4].copy_from_slice(&declared.to_le_bytes());
    section.extend_from_slice(tail);
}

fn v1_page(rep: &[i16], def: &[i16], max_rep: i16, max_def: i16, suffix: &[u8]) -> Page {
    assert_eq!(rep.len(), def.len());
    let rep_section = if max_rep == 0 {
        Vec::new()
    } else {
        v1_encoded(max_rep, rep)
    };
    let def_section = if max_def == 0 {
        Vec::new()
    } else {
        v1_encoded(max_def, def)
    };
    let mut body = rep_section;
    body.extend_from_slice(&def_section);
    body.extend_from_slice(suffix);
    Page::DataPage {
        buf: Bytes::from(body),
        num_values: u32::try_from(rep.len()).unwrap(),
        encoding: Encoding::PLAIN,
        def_level_encoding: Encoding::RLE,
        rep_level_encoding: Encoding::RLE,
        statistics: None,
    }
}

fn v2_page(
    rep: &[i16],
    def: &[i16],
    max_rep: i16,
    max_def: i16,
    declared_nulls: u32,
    declared_rows: u32,
    suffix: &[u8],
) -> Page {
    assert_eq!(rep.len(), def.len());
    let rep_section = if max_rep == 0 {
        Vec::new()
    } else {
        v2_encoded(max_rep, rep)
    };
    let def_section = if max_def == 0 {
        Vec::new()
    } else {
        v2_encoded(max_def, def)
    };
    let mut body = rep_section.clone();
    body.extend_from_slice(&def_section);
    body.extend_from_slice(suffix);
    Page::DataPageV2 {
        buf: Bytes::from(body),
        num_values: u32::try_from(rep.len()).unwrap(),
        encoding: Encoding::PLAIN,
        num_nulls: declared_nulls,
        num_rows: declared_rows,
        def_levels_byte_len: u32::try_from(def_section.len()).unwrap(),
        rep_levels_byte_len: u32::try_from(rep_section.len()).unwrap(),
        is_compressed: false,
        statistics: None,
    }
}

fn drain(page: &Page, max_rep: i16, max_def: i16) -> Vec<(i16, i16)> {
    let mut cursor = PageEvents::new(page, max_rep, max_def).unwrap();
    let mut rep = [0_i16; MAX_BLOCK_EVENTS];
    let mut def = [0_i16; MAX_BLOCK_EVENTS];
    let mut values = Vec::new();
    loop {
        let count = cursor.next_block(&mut rep, &mut def, None).unwrap();
        values.extend((0..count).map(|index| (rep[index], def[index])));
        if count == 0 {
            break;
        }
    }
    values
}

fn is_storage(error: &GfError) -> bool {
    matches!(error, GfError::Storage(_))
}

#[test]
fn v1_rle_summary_counts_actual_nonnull_rows_and_preserves_value_suffix() {
    let rep = [0, 1, 0, 1, 1, 0];
    let def = [2, 2, 0, 1, 2, 2];
    let page = v1_page(&rep, &def, 1, 2, b"value bytes");
    let cursor = PageEvents::new(&page, 1, 2).unwrap();
    assert_eq!(cursor.value_suffix(), b"value bytes");
    assert_eq!(
        cursor.validated_summary(None).unwrap(),
        super::EventSummary {
            events: 6,
            nonnull: 3,
            row_starts: 3,
            first_repetition: Some(0),
        }
    );
    assert_eq!(
        drain(&page, 1, 2),
        rep.into_iter().zip(def).collect::<Vec<_>>()
    );
}

#[test]
fn v1_bit_packed_repetition_and_rle_definition_use_their_own_grammars() {
    let rep = [0_i16, 1, 1, 0, 1, 0, 1, 1, 0];
    let def = [1_i16, 0, 1, 1, 0, 1, 1, 1, 0];
    let mut writer = BitWriter::new(8);
    for level in rep {
        writer.put_value(i32::from(level), 1);
    }
    let mut body = writer.consume();
    // Only bit zero of the final byte is a logical level; the pinned reader
    // ignores the remaining packed padding bits.
    *body.last_mut().unwrap() |= 0xfe;
    body.extend_from_slice(&v1_encoded(1, &def));
    body.extend_from_slice(b"suffix");
    let page = Page::DataPage {
        buf: Bytes::from(body),
        num_values: rep.len() as u32,
        encoding: Encoding::PLAIN,
        def_level_encoding: Encoding::RLE,
        rep_level_encoding: Encoding::BIT_PACKED,
        statistics: None,
    };
    assert_eq!(
        drain(&page, 1, 1),
        rep.into_iter().zip(def).collect::<Vec<_>>()
    );
    assert_eq!(
        PageEvents::new(&page, 1, 1).unwrap().value_suffix(),
        b"suffix"
    );
}

#[test]
fn v1_hybrid_sections_keep_legal_unused_tails_and_zero_width_is_implicit() {
    let rep = [0, 1, 0, 1];
    let def = [1, 0, 1, 1];
    let mut rep_section = v1_encoded(1, &rep);
    append_v1_rle_tail(&mut rep_section, &[0xff, 0xfe]);
    let def_section = v1_encoded(1, &def);
    let mut body = rep_section;
    body.extend_from_slice(&def_section);
    body.extend_from_slice(b"suffix");
    let page = Page::DataPage {
        buf: Bytes::from(body),
        num_values: rep.len() as u32,
        encoding: Encoding::PLAIN,
        def_level_encoding: Encoding::RLE,
        rep_level_encoding: Encoding::RLE,
        statistics: None,
    };
    assert_eq!(
        drain(&page, 1, 1),
        rep.into_iter().zip(def).collect::<Vec<_>>()
    );
    assert_eq!(
        PageEvents::new(&page, 1, 1).unwrap().value_suffix(),
        b"suffix"
    );

    let flat = v1_page(&[0, 0, 0], &[0, 0, 0], 0, 0, b"flat values");
    let cursor = PageEvents::new(&flat, 0, 0).unwrap();
    assert_eq!(cursor.value_suffix(), b"flat values");
    assert_eq!(
        cursor.validated_summary(None).unwrap(),
        super::EventSummary {
            events: 3,
            nonnull: 3,
            row_starts: 3,
            first_repetition: Some(0),
        }
    );
}

#[test]
fn v1_page_may_begin_with_a_repetition_continuation() {
    let page = v1_page(&[1, 1, 0], &[1, 1, 1], 1, 1, b"");
    let summary = PageEvents::new(&page, 1, 1)
        .unwrap()
        .validated_summary(None)
        .unwrap();
    assert_eq!(summary.first_repetition, Some(1));
    assert_eq!(summary.row_starts, 1);
}

#[test]
fn v2_summary_checks_actual_nonnull_rows_and_separate_sections() {
    let rep = [0, 1, 0, 1, 1, 0];
    let def = [2, 2, 0, 1, 2, 2];
    let page = v2_page(&rep, &def, 1, 2, 1, 3, b"value bytes");
    let cursor = PageEvents::new(&page, 1, 2).unwrap();
    assert_eq!(cursor.value_suffix(), b"value bytes");
    assert_eq!(
        cursor.validated_summary(None).unwrap(),
        super::EventSummary {
            events: 6,
            nonnull: 3,
            row_starts: 3,
            first_repetition: Some(0),
        }
    );
}

#[test]
fn v2_rejects_declared_null_row_and_start_mismatches() {
    let rep = [0, 1, 0];
    let def = [1, 0, 1];
    let null_mismatch = v2_page(&rep, &def, 1, 1, 0, 2, b"");
    let error = PageEvents::new(&null_mismatch, 1, 1)
        .unwrap()
        .validated_summary(None)
        .err()
        .expect("declared null mismatch must fail");
    assert!(is_storage(&error));

    let row_mismatch = v2_page(&rep, &def, 1, 1, 1, 1, b"");
    let error = PageEvents::new(&row_mismatch, 1, 1)
        .unwrap()
        .validated_summary(None)
        .err()
        .expect("declared row mismatch must fail");
    assert!(is_storage(&error));

    let continuation = v2_page(&[1, 0], &[1, 1], 1, 1, 0, 1, b"");
    let error = PageEvents::new(&continuation, 1, 1)
        .unwrap()
        .validated_summary(None)
        .err()
        .expect("V2 page continuation must fail");
    assert!(is_storage(&error));
}

#[test]
fn v2_zero_maximum_descriptors_skip_declared_sections_for_value_offset() {
    let body = [0x9a, 0xbc, 0xde, 0xf0, 0x12, b'v', b'a', b'l'];
    let page = Page::DataPageV2 {
        buf: Bytes::copy_from_slice(&body),
        num_values: 3,
        encoding: Encoding::PLAIN,
        num_nulls: 0,
        num_rows: 3,
        def_levels_byte_len: 3,
        rep_levels_byte_len: 2,
        is_compressed: false,
        statistics: None,
    };
    let cursor = PageEvents::new(&page, 0, 0).unwrap();
    assert_eq!(cursor.value_suffix(), b"val");
    assert_eq!(
        cursor.validated_summary(None).unwrap(),
        super::EventSummary {
            events: 3,
            nonnull: 3,
            row_starts: 3,
            first_repetition: Some(0),
        }
    );
}

#[test]
fn large_pages_are_drained_in_caller_owned_blocks_capped_at_1024() {
    let count = 3073;
    let rep = vec![0_i16; count];
    let def = (0..count)
        .map(|index| if index % 3 != 0 { 1_i16 } else { 0_i16 })
        .collect::<Vec<_>>();
    let page = v1_page(&rep, &def, 1, 1, b"");
    let mut cursor = PageEvents::new(&page, 1, 1).unwrap();

    let mut oversized_rep = [0x5555_i16; 2048];
    let mut oversized_def = [0x6666_i16; 2048];
    let first = cursor
        .next_block(&mut oversized_rep, &mut oversized_def, None)
        .unwrap();
    assert_eq!(first, MAX_BLOCK_EVENTS);
    assert!(
        oversized_rep[..MAX_BLOCK_EVENTS]
            .iter()
            .all(|value| *value == 0)
    );
    for (index, value) in oversized_def[..MAX_BLOCK_EVENTS].iter().enumerate() {
        assert_eq!(*value, if index % 3 != 0 { 1 } else { 0 });
    }
    assert!(
        oversized_rep[MAX_BLOCK_EVENTS..]
            .iter()
            .all(|value| *value == 0x5555)
    );
    assert!(
        oversized_def[MAX_BLOCK_EVENTS..]
            .iter()
            .all(|value| *value == 0x6666)
    );

    let mut rep_block = [0_i16; MAX_BLOCK_EVENTS];
    let mut def_block = [0_i16; MAX_BLOCK_EVENTS];
    let mut remaining_counts = Vec::new();
    loop {
        let count = cursor
            .next_block(&mut rep_block, &mut def_block, None)
            .unwrap();
        assert!(count <= MAX_BLOCK_EVENTS);
        if count == 0 {
            break;
        }
        remaining_counts.push(count);
    }
    assert_eq!(remaining_counts, [1024, 1024, 1]);
    assert_eq!(first + remaining_counts.iter().sum::<usize>(), 3073);
    assert_eq!(cursor.validated_summary(None).unwrap().nonnull, 3073 - 1025);
}

#[test]
fn mismatched_output_blocks_fail_before_writing_either_buffer() {
    let page = v1_page(&[0, 1], &[1, 0], 1, 1, b"");
    let mut cursor = PageEvents::new(&page, 1, 1).unwrap();
    let mut rep = [0x1111_i16; 4];
    let mut def = [0x2222_i16; 3];
    let error = cursor
        .next_block(&mut rep, &mut def, None)
        .err()
        .expect("different output lengths must fail");
    assert!(is_storage(&error));
    assert!(rep.iter().all(|value| *value == 0x1111));
    assert!(def.iter().all(|value| *value == 0x2222));
}

#[test]
fn cancellation_is_typed_and_precedes_any_event_write() {
    let page = v1_page(&[0, 1, 0], &[1, 0, 1], 1, 1, b"");
    let mut cursor = PageEvents::new(&page, 1, 1).unwrap();
    let token = CancellationToken::new();
    token.cancel();
    let mut rep = [0x1111_i16; 3];
    let mut def = [0x2222_i16; 3];
    let error = cursor
        .next_block(&mut rep, &mut def, Some(&token))
        .err()
        .expect("cancelled page must stop before writing events");
    assert!(matches!(
        error,
        GfError::Api {
            code: ApiErrorCode::Cancelled,
            ..
        }
    ));
    assert!(rep.iter().all(|value| *value == 0x1111));
    assert!(def.iter().all(|value| *value == 0x2222));
}

#[test]
fn cancellation_is_checked_for_empty_v1_and_v2_summaries() {
    let empty_v1 = v1_page(&[], &[], 0, 0, b"");
    let empty_v2 = v2_page(&[], &[], 0, 0, 0, 0, b"");
    let token = CancellationToken::new();
    token.cancel();
    for page in [&empty_v1, &empty_v2] {
        let error = PageEvents::new(page, 0, 0)
            .unwrap()
            .validated_summary(Some(&token))
            .err()
            .expect("empty summary must honor cancellation before scanning");
        assert!(matches!(
            error,
            GfError::Api {
                code: ApiErrorCode::Cancelled,
                ..
            }
        ));
    }
}

#[test]
fn descriptor_and_truncated_v2_sections_are_refused() {
    let page = v1_page(&[0], &[0], 1, 1, b"");
    let error = PageEvents::new(&page, -1, 1)
        .err()
        .expect("negative descriptor maximum must fail");
    assert!(is_storage(&error));

    let short = Page::DataPageV2 {
        buf: Bytes::from_static(b"short"),
        num_values: 1,
        encoding: Encoding::PLAIN,
        num_nulls: 0,
        num_rows: 1,
        def_levels_byte_len: u32::MAX,
        rep_levels_byte_len: 1,
        is_compressed: false,
        statistics: None,
    };
    let error = PageEvents::new(&short, 0, 0)
        .err()
        .expect("out-of-body V2 prefix must fail before slicing");
    assert!(is_storage(&error));
}

#[test]
fn dictionary_page_is_not_a_data_event_cursor() {
    let page = Page::DictionaryPage {
        buf: Bytes::new(),
        num_values: 1,
        encoding: Encoding::PLAIN,
        is_sorted: false,
    };
    let error = PageEvents::new(&page, 0, 0)
        .err()
        .expect("dictionary pages have no level-event cursor");
    assert!(is_storage(&error));
}
