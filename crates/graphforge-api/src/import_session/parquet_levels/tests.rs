use super::*;
use parquet::encodings::levels::LevelEncoder;
use parquet::util::bit_util::{BitReader, BitWriter};

fn vlq(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
    out
}

fn rle_run(count: u64, value: u64, width: u8) -> Vec<u8> {
    let mut out = vlq(count << 1);
    let value_bytes = value.to_le_bytes();
    out.extend_from_slice(&value_bytes[..usize::from(width).div_ceil(8)]);
    out
}

fn pack_values(values: &[u64], width: u8) -> Vec<u8> {
    let mut writer = BitWriter::new(16);
    for &value in values {
        writer.put_value(value, usize::from(width));
    }
    writer.consume()
}

fn packed_run(groups: u64, values: &[u64], width: u8) -> Vec<u8> {
    let mut out = vlq((groups << 1) | 1);
    out.extend(pack_values(values, width));
    out
}

fn encode_v1_levels(max: i16, levels: &[i16]) -> Vec<u8> {
    let mut encoder = LevelEncoder::v1_streaming(max);
    encoder.put_with_observer(levels, |_, _| {});
    encoder.consume()
}

fn encode_v2_levels(max: i16, levels: &[i16]) -> Vec<u8> {
    let mut encoder = LevelEncoder::v2_streaming(max);
    encoder.put_with_observer(levels, |_, _| {});
    encoder.consume()
}

fn drain_levels(source: &mut impl LevelSource) -> Vec<i16> {
    let mut out = Vec::new();
    while let Some(level) = source.next_level().expect("level decode") {
        out.push(level);
    }
    out
}

fn drain_indices(source: &mut impl IndexSource) -> Vec<u32> {
    let mut out = Vec::new();
    while let Some(index) = source.next_index().expect("index decode") {
        out.push(index);
    }
    out
}

fn as_levels(values: &[u64]) -> Vec<i16> {
    values
        .iter()
        .map(|&value| i16::try_from(value).unwrap())
        .collect()
}

/// Levels mixing RLE-friendly runs with bit-packed variety.
fn mixed_levels(count: usize, max: i16) -> Vec<i16> {
    (0..count)
        .map(|index| {
            let phase = index % 100;
            if phase < 40 {
                i16::try_from(phase).unwrap() % (max + 1)
            } else if phase < 70 {
                max
            } else {
                0
            }
        })
        .collect()
}

#[test]
fn v1_rle_sections_round_trip_pinned_encoder_bytes() {
    for count in [
        1_usize, 2, 7, 8, 9, 15, 16, 17, 31, 32, 1023, 1024, 1025, 2500,
    ] {
        let max = 5_i16;
        let levels = mixed_levels(count, max);
        let encoded = encode_v1_levels(max, &levels);
        let sections = split_v1(
            &encoded,
            0,
            max,
            u32::try_from(count).unwrap(),
            Encoding::RLE,
            Encoding::RLE,
        )
        .unwrap();
        assert!(sections.repetition.is_none());
        let section = sections.definition.unwrap();
        assert_eq!(section, &encoded[4..]);
        assert!(sections.values.is_empty());
        let mut cursor = HybridLevels::new(section, max, count).unwrap();
        let decoded = drain_levels(&mut cursor);
        assert_eq!(decoded, levels);
        assert_eq!(cursor.emitted(), count);
        assert_eq!(cursor.next_level().unwrap(), None);
        assert!(cursor.consumed_bytes() <= section.len());
    }
}

#[test]
fn v1_page_with_both_level_sections_and_values_splits_in_pinned_order() {
    let max_rep = 1_i16;
    let max_def = 2_i16;
    let rep = vec![0_i16, 1, 1, 0, 1, 1];
    let def = vec![2_i16, 2, 1, 2, 2, 1];
    let rep_encoded = encode_v1_levels(max_rep, &rep);
    let def_encoded = encode_v1_levels(max_def, &def);
    let mut body = rep_encoded.clone();
    body.extend_from_slice(&def_encoded);
    body.extend_from_slice(b"VALUES");
    let sections = split_v1(&body, max_rep, max_def, 6, Encoding::RLE, Encoding::RLE).unwrap();
    let mut rep_cursor =
        HybridLevels::new(sections.repetition.unwrap(), max_rep, rep.len()).unwrap();
    assert_eq!(drain_levels(&mut rep_cursor), rep);
    let mut def_cursor =
        HybridLevels::new(sections.definition.unwrap(), max_def, def.len()).unwrap();
    assert_eq!(drain_levels(&mut def_cursor), def);
    assert_eq!(sections.values, &b"VALUES"[..]);
}

#[test]
#[allow(deprecated)]
fn v1_page_with_bit_packed_repetition_and_rle_definition_splits_in_pinned_order() {
    let max_rep = 1_i16;
    let max_def = 1_i16;
    let rep = vec![0_i16, 1, 1, 0, 1, 0];
    let def = vec![1_i16, 0, 1, 1, 1, 1];
    let mut body = pack_values(
        &rep.iter()
            .map(|&level| u64::try_from(level).unwrap())
            .collect::<Vec<_>>(),
        1,
    );
    body.extend_from_slice(&encode_v1_levels(max_def, &def));
    body.extend_from_slice(b"V");
    let sections = split_v1(
        &body,
        max_rep,
        max_def,
        6,
        Encoding::BIT_PACKED,
        Encoding::RLE,
    )
    .unwrap();
    let mut rep_cursor = PackedLevels::new(sections.repetition.unwrap(), max_rep, 6).unwrap();
    assert_eq!(drain_levels(&mut rep_cursor), rep);
    let mut def_cursor =
        HybridLevels::new(sections.definition.unwrap(), max_def, def.len()).unwrap();
    assert_eq!(drain_levels(&mut def_cursor), def);
    assert_eq!(sections.values, &b"V"[..]);
}

#[test]
fn v2_hybrid_levels_round_trip_pinned_encoder_bytes() {
    for count in [1_usize, 8, 9, 64, 1024, 5000] {
        let max = 9_i16;
        let levels = mixed_levels(count, max);
        let encoded = encode_v2_levels(max, &levels);
        let mut cursor = HybridLevels::new(&encoded, max, count).unwrap();
        assert_eq!(drain_levels(&mut cursor), levels);
        assert!(cursor.consumed_bytes() <= encoded.len());
        assert_eq!(cursor.next_level().unwrap(), None);
    }
}

#[test]
fn packed_levels_match_pinned_bit_reader_ordering() {
    for (max, values) in [
        (1_i16, vec![0_u64, 1, 1, 0, 1, 0, 0, 1, 1]),
        (5_i16, vec![0, 5, 3, 1, 2, 4, 1, 4, 5, 5, 0, 3, 2, 0, 1, 4]),
        (
            127_i16,
            (0_u64..33).map(|index| (index * 11) % 128).collect(),
        ),
    ] {
        let width = num_required_bits(u64::try_from(max).unwrap());
        let packed = pack_values(&values, width);
        let mut reader = BitReader::from(packed.clone());
        for &value in &values {
            let expected = i16::try_from(value).unwrap();
            assert_eq!(reader.get_value::<i16>(usize::from(width)), Some(expected));
        }
        let mut cursor = PackedLevels::new(&packed, max, values.len()).unwrap();
        assert_eq!(drain_levels(&mut cursor), as_levels(&values));
        assert_eq!(cursor.consumed_bytes(), packed.len());
        assert_eq!(cursor.next_level().unwrap(), None);
    }
}

#[test]
fn dictionary_indices_decode_pinned_encoder_streams() {
    let dictionary_count = 5_usize;
    let width = num_required_bits(u64::try_from(dictionary_count - 1).unwrap());
    let max_index = i16::try_from(dictionary_count - 1).unwrap();
    let mut indices: Vec<u32> = vec![3; 3000];
    indices.extend((0_u32..250).map(|index| index % 5));
    indices.push(0);
    let levels: Vec<i16> = indices
        .iter()
        .map(|&index| i16::try_from(index).unwrap())
        .collect();
    let encoded = encode_v2_levels(max_index, &levels);
    let mut stream = vec![width];
    stream.extend_from_slice(&encoded);
    let mut cursor = DictionaryIndices::new(&stream, dictionary_count, indices.len()).unwrap();
    assert_eq!(drain_indices(&mut cursor), indices);
    assert_eq!(cursor.next_index().unwrap(), None);
    assert!(cursor.consumed_bytes() <= stream.len());

    let v1_encoded = encode_v1_levels(max_index, &levels);
    let sections = split_v1(
        &v1_encoded,
        0,
        max_index,
        u32::try_from(indices.len()).unwrap(),
        Encoding::RLE,
        Encoding::RLE,
    )
    .unwrap();
    let mut stream = vec![width];
    stream.extend_from_slice(sections.definition.unwrap());
    let mut cursor = DictionaryIndices::new(&stream, dictionary_count, indices.len()).unwrap();
    assert_eq!(drain_indices(&mut cursor), indices);
}

#[test]
fn width_zero_streams_decode_zero_events_without_sections() {
    let zeros = vec![0_i16; 37];
    let encoded = encode_v2_levels(0, &zeros);
    let mut cursor = HybridLevels::new(&encoded, 0, zeros.len()).unwrap();
    assert_eq!(drain_levels(&mut cursor), zeros);
    assert_eq!(cursor.next_level().unwrap(), None);

    let mut cursor = PackedLevels::new(&[], 0, 40).unwrap();
    assert_eq!(drain_levels(&mut cursor), vec![0_i16; 40]);
    assert_eq!(cursor.next_level().unwrap(), None);

    let mut implicit = ImplicitZero::new(40);
    assert_eq!(drain_levels(&mut implicit), vec![0_i16; 40]);
    assert_eq!(implicit.next_level().unwrap(), None);

    let mut stream = vec![0_u8];
    stream.extend_from_slice(&encode_v2_levels(0, &zeros));
    let mut cursor = DictionaryIndices::new(&stream, 1, zeros.len()).unwrap();
    assert_eq!(drain_indices(&mut cursor), vec![0_u32; 37]);
    assert_eq!(cursor.next_index().unwrap(), None);

    let mut stream = vec![0_u8];
    stream.extend_from_slice(&vlq(37 << 1));
    let mut cursor = DictionaryIndices::new(&stream, 1, 37).unwrap();
    assert_eq!(drain_indices(&mut cursor), vec![0_u32; 37]);
}

#[test]
fn partial_last_group_padding_beyond_expected_prefix_is_accepted() {
    let max = 5_i16;
    let width = num_required_bits(u64::try_from(max).unwrap());
    let values: Vec<u64> = (0_u64..8).map(|index| index % 6).collect();
    let packed = pack_values(&values, width);

    let mut cursor = PackedLevels::new(&packed, max, 5).unwrap();
    assert_eq!(drain_levels(&mut cursor), as_levels(&values[..5]));
    assert_eq!(cursor.consumed_bytes(), 2);

    let mut cursor = PackedLevels::new(&packed[..2], max, 5).unwrap();
    assert_eq!(drain_levels(&mut cursor), as_levels(&values[..5]));

    let mut cursor = PackedLevels::new(&packed[..1], max, 5).unwrap();
    assert_eq!(cursor.next_level().unwrap(), Some(0));
    assert_eq!(cursor.next_level().unwrap(), Some(1));
    assert!(cursor.next_level().is_err());

    let full: Vec<u64> = (0_u64..16).map(|index| index % 6).collect();
    let claimed = packed_run(2, &full, width);
    let mut cursor = HybridLevels::new(&claimed, max, 10).unwrap();
    assert_eq!(drain_levels(&mut cursor), as_levels(&full[..10]));
    assert_eq!(cursor.next_level().unwrap(), None);

    let partial_payload = packed_run(2, &values, width);
    let mut cursor = HybridLevels::new(&partial_payload, max, 8).unwrap();
    assert_eq!(drain_levels(&mut cursor), as_levels(&values));

    let mut cursor = HybridLevels::new(&partial_payload, max, 9).unwrap();
    assert!(cursor.next_level().is_err());
}

#[test]
fn fastparquet_zero_terminator_is_permitted_only_after_required_events() {
    let max = 1_i16;
    let levels = vec![1_i16; 10];
    let mut encoded = encode_v2_levels(max, &levels);
    encoded.push(0);
    let mut cursor = HybridLevels::new(&encoded, max, levels.len()).unwrap();
    assert_eq!(drain_levels(&mut cursor), levels);
    assert_eq!(cursor.next_level().unwrap(), None);

    let mut stream = rle_run(3, 1, 1);
    stream.push(0);
    let mut cursor = HybridLevels::new(&stream, max, 10).unwrap();
    for _ in 0..3 {
        assert_eq!(cursor.next_level().unwrap(), Some(1));
    }
    assert!(cursor.next_level().is_err());

    let mut cursor = HybridLevels::new(&[0_u8], max, 0).unwrap();
    assert_eq!(cursor.next_level().unwrap(), None);
}

#[test]
fn overlong_and_overflowing_vlq_indicators_are_refused() {
    let max = 1_i16;
    let ten_bytes = vec![0xFF_u8; 10];
    let mut cursor = HybridLevels::new(&ten_bytes, max, 1).unwrap();
    assert!(cursor.next_level().is_err());

    let mut overflow_byte = vec![0xFF_u8; 9];
    overflow_byte.push(2);
    let mut cursor = HybridLevels::new(&overflow_byte, max, 1).unwrap();
    assert!(cursor.next_level().is_err());

    let eleven_bytes = vec![0xFF_u8; 11];
    let mut cursor = HybridLevels::new(&eleven_bytes, max, 1).unwrap();
    assert!(cursor.next_level().is_err());

    let mut cursor = HybridLevels::new(&[0x80, 0x80], max, 1).unwrap();
    assert!(cursor.next_level().is_err());
}

#[test]
fn bit_packed_group_counts_and_rle_counts_refuse_u32_overflow() {
    let max = 1_i16;
    let groups = u64::from(u32::MAX) / 8 + 1;
    let stream = vlq((groups << 1) | 1);
    let mut cursor = HybridLevels::new(&stream, max, 1).unwrap();
    assert!(cursor.next_level().is_err());

    let stream = vlq((1_u64 << 62) | 1);
    let mut cursor = HybridLevels::new(&stream, max, 1).unwrap();
    assert!(cursor.next_level().is_err());

    let stream = vlq((u64::from(u32::MAX) + 1) << 1);
    let mut cursor = HybridLevels::new(&stream, max, 1).unwrap();
    assert!(cursor.next_level().is_err());

    let mut stream = vlq(u64::from(u32::MAX) << 1);
    stream.push(1);
    let mut cursor = HybridLevels::new(&stream, max, 3).unwrap();
    for _ in 0..3 {
        assert_eq!(cursor.next_level().unwrap(), Some(1));
    }
    assert_eq!(cursor.next_level().unwrap(), None);
}

#[test]
fn truncated_run_payloads_are_refused() {
    let max = 5_i16;
    let stream = vlq(5 << 1);
    let mut cursor = HybridLevels::new(&stream, max, 1).unwrap();
    assert!(cursor.next_level().is_err());

    let values: Vec<u64> = (0_u64..8).map(|index| index % 6).collect();
    let partial_payload = packed_run(2, &values, 3);
    let mut cursor = HybridLevels::new(&partial_payload, max, 10).unwrap();
    assert!(cursor.next_level().is_err());

    let mut cursor = PackedLevels::new(&[0x2A_u8], max, 3).unwrap();
    assert_eq!(cursor.next_level().unwrap(), Some(2));
    assert_eq!(cursor.next_level().unwrap(), Some(5));
    assert!(cursor.next_level().is_err());
}

/// Mutation sentinel: reverting any production allowed-maximum check makes
/// this test fail, because the refused stream then decodes to an out-of-range
/// value instead of an error.
#[test]
fn allowed_maximum_check_blocks_out_of_range_levels_and_indices() {
    let max = 5_i16;
    let encoded = rle_run(4, 6, 3);
    let mut cursor = HybridLevels::new(&encoded, max, 4).unwrap();
    let error = cursor.next_level().unwrap_err();
    assert!(error.to_string().contains("allowed maximum"), "{error}");

    let values = [5_u64, 5, 6, 5, 5, 5, 5, 5];
    let encoded = packed_run(1, &values, 3);
    let mut cursor = HybridLevels::new(&encoded, max, 8).unwrap();
    assert_eq!(cursor.next_level().unwrap(), Some(5));
    assert_eq!(cursor.next_level().unwrap(), Some(5));
    let error = cursor.next_level().unwrap_err();
    assert!(error.to_string().contains("allowed maximum"), "{error}");

    let overflow_first = [6_u64, 5, 5, 5, 5, 5, 5, 5];
    let encoded = pack_values(&overflow_first, 3);
    let mut cursor = PackedLevels::new(&encoded, max, 8).unwrap();
    let error = cursor.next_level().unwrap_err();
    assert!(error.to_string().contains("allowed maximum"), "{error}");

    let rep = [0_u64, 2, 3];
    let encoded = pack_values(&rep, 2);
    let mut cursor = PackedLevels::new(&encoded, 2, 3).unwrap();
    assert_eq!(cursor.next_level().unwrap(), Some(0));
    assert_eq!(cursor.next_level().unwrap(), Some(2));
    let error = cursor.next_level().unwrap_err();
    assert!(error.to_string().contains("allowed maximum"), "{error}");

    let mut stream = vec![3_u8];
    stream.extend_from_slice(&rle_run(2, 7, 3));
    let mut cursor = DictionaryIndices::new(&stream, 3, 2).unwrap();
    let error = cursor.next_index().unwrap_err();
    assert!(error.to_string().contains("allowed maximum"), "{error}");

    let indices = [0_u64, 1, 2, 3, 0, 0, 0, 0];
    let mut stream = vec![2_u8];
    stream.extend_from_slice(&packed_run(1, &indices, 2));
    let mut cursor = DictionaryIndices::new(&stream, 3, 8).unwrap();
    for expected in 0..3_u32 {
        assert_eq!(cursor.next_index().unwrap(), Some(expected));
    }
    let error = cursor.next_index().unwrap_err();
    assert!(error.to_string().contains("allowed maximum"), "{error}");
}

#[test]
#[allow(deprecated)]
fn insufficient_streams_are_refused() {
    let mut cursor = HybridLevels::new(&[], 1, 1).unwrap();
    assert!(cursor.next_level().is_err());

    let stream = rle_run(3, 1, 1);
    let mut cursor = HybridLevels::new(&stream, 1, 10).unwrap();
    for _ in 0..3 {
        assert_eq!(cursor.next_level().unwrap(), Some(1));
    }
    assert!(cursor.next_level().is_err());

    let mut cursor = PackedLevels::new(&[], 5, 1).unwrap();
    assert!(cursor.next_level().is_err());

    assert!(DictionaryIndices::new(&[], 3, 1).is_err());

    let mut cursor = DictionaryIndices::new(&[3_u8], 3, 1).unwrap();
    assert!(cursor.next_index().is_err());

    assert!(DictionaryIndices::new(&[1_u8, 1], 0, 1).is_err());

    let mut cursor = DictionaryIndices::new(&[1_u8], 0, 0).unwrap();
    assert_eq!(cursor.next_index().unwrap(), None);
}

#[test]
#[allow(deprecated)]
fn v1_rle_section_lengths_are_validated_before_borrowing() {
    let max = 1_i16;
    let body = [0xFF_u8, 0xFF, 0xFF, 0xFF, 1];
    let sections = split_v1(&body, 0, max, 4, Encoding::RLE, Encoding::RLE).unwrap_err();
    assert!(sections.to_string().contains("negative"), "{sections}");

    let body = [0xFF_u8, 0xFF, 0xFF, 0x7F, 1];
    let sections = split_v1(&body, 0, max, 4, Encoding::RLE, Encoding::RLE).unwrap_err();
    assert!(
        sections.to_string().contains("exceeds the page body"),
        "{sections}"
    );

    let body = [0x01_u8, 0x00, 0x00];
    let sections = split_v1(&body, 0, max, 4, Encoding::RLE, Encoding::RLE).unwrap_err();
    assert!(sections.to_string().contains("length prefix"), "{sections}");

    let levels = vec![1_i16, 0, 1, 1];
    let encoded = encode_v1_levels(max, &levels);
    let sections = split_v1(
        &encoded,
        0,
        max,
        u32::try_from(levels.len()).unwrap(),
        Encoding::RLE,
        Encoding::RLE,
    )
    .unwrap();
    assert_eq!(sections.definition.unwrap(), &encoded[4..]);
    assert!(sections.values.is_empty());

    let body = [0xFF_u8, 0xFF, 0xFF, 0xFF];
    let sections = split_v1(&body, 0, max, 0, Encoding::RLE, Encoding::RLE).unwrap_err();
    assert!(sections.to_string().contains("negative"), "{sections}");
}

#[test]
#[allow(deprecated)]
fn v1_bit_packed_section_length_is_checked_before_borrowing() {
    let max = 3_i16;
    let rep = vec![0_i16, 1, 2, 3, 0, 2];
    let mut packed = pack_values(
        &rep.iter()
            .map(|&level| u64::try_from(level).unwrap())
            .collect::<Vec<_>>(),
        2,
    );
    packed.extend_from_slice(b"VALUES");
    let sections = split_v1(
        &packed,
        max,
        0,
        u32::try_from(rep.len()).unwrap(),
        Encoding::BIT_PACKED,
        Encoding::RLE,
    )
    .unwrap();
    assert_eq!(sections.repetition.unwrap(), &packed[..2]);
    assert!(sections.definition.is_none());
    assert_eq!(sections.values, &b"VALUES"[..]);
    let mut cursor = PackedLevels::new(sections.repetition.unwrap(), max, rep.len()).unwrap();
    assert_eq!(drain_levels(&mut cursor), rep);

    let body = [0xFF_u8];
    let sections = split_v1(&body, max, 0, 5, Encoding::BIT_PACKED, Encoding::RLE).unwrap_err();
    assert!(
        sections.to_string().contains("exceeds the page body"),
        "{sections}"
    );

    let body = [0xAB_u8];
    let sections = split_v1(&body, max, 0, 0, Encoding::BIT_PACKED, Encoding::RLE).unwrap();
    assert!(sections.repetition.unwrap().is_empty());
    assert_eq!(sections.values, &body[..]);

    let sections = split_v1(&[0_u8; 4], 0, max, 4, Encoding::RLE, Encoding::PLAIN).unwrap_err();
    assert!(sections.to_string().contains("unsupported"), "{sections}");

    let sections = split_v1(&[0_u8; 8], 0, -1, 4, Encoding::RLE, Encoding::RLE).unwrap_err();
    assert!(
        sections.to_string().contains("negative maximum"),
        "{sections}"
    );
}

#[test]
fn fixed_blocks_fill_at_most_1024_events() {
    let max = 9_i16;
    let levels = mixed_levels(2500, max);
    let encoded = encode_v2_levels(max, &levels);
    let mut cursor = HybridLevels::new(&encoded, max, levels.len()).unwrap();
    let mut block = [0_i16; 1024];
    assert_eq!(cursor.next_block(&mut block).unwrap(), 1024);
    assert_eq!(&levels[..1024], &block[..]);
    assert_eq!(cursor.next_block(&mut block).unwrap(), 1024);
    assert_eq!(&levels[1024..2048], &block[..]);
    assert_eq!(cursor.next_block(&mut block).unwrap(), 452);
    assert_eq!(&levels[2048..], &block[..452]);
    assert_eq!(cursor.next_block(&mut block).unwrap(), 0);
    assert_eq!(cursor.next_level().unwrap(), None);

    let mut oversized = [0_i16; 2048];
    assert!(cursor.next_block(&mut oversized).is_err());

    let dictionary_count = 5_usize;
    let indices: Vec<u32> = (0_u32..1500).map(|index| index % 5).collect();
    let levels: Vec<i16> = indices
        .iter()
        .map(|&index| i16::try_from(index).unwrap())
        .collect();
    let encoded = encode_v2_levels(4, &levels);
    let mut stream = vec![num_required_bits(4)];
    stream.extend_from_slice(&encoded);
    let mut cursor = DictionaryIndices::new(&stream, dictionary_count, indices.len()).unwrap();
    let mut block = [0_u32; 1024];
    assert_eq!(cursor.next_block(&mut block).unwrap(), 1024);
    assert_eq!(&indices[..1024], &block[..]);
    assert_eq!(cursor.next_block(&mut block).unwrap(), 476);
    assert_eq!(&indices[1024..], &block[..476]);
    assert_eq!(cursor.next_block(&mut block).unwrap(), 0);
}

#[test]
fn hybrid_consumption_tracks_logical_prefix_not_final_padding() {
    // The first two values are legal; unused packed values deliberately are
    // not. They remain charged body bytes, but are not logical events.
    let encoded = packed_run(1, &[0, 1, 7, 7, 7, 7, 7, 7], 3);
    let mut cursor = HybridLevels::new(&encoded, 5, 2).unwrap();
    assert_eq!(drain_levels(&mut cursor), [0, 1]);
    assert_eq!(cursor.consumed_bytes(), 2);
    assert_eq!(encoded.len(), 4);
    assert_eq!(cursor.next_level().unwrap(), None);
    let mut cursor = HybridLevels::new(&encoded, 5, 3).unwrap();
    assert_eq!(cursor.next_level().unwrap(), Some(0));
    assert_eq!(cursor.next_level().unwrap(), Some(1));
    assert!(cursor.next_level().is_err());

    let mut stream = vec![3];
    stream.extend_from_slice(&encoded);
    let mut cursor = DictionaryIndices::new(&stream, 6, 2).unwrap();
    assert_eq!(drain_indices(&mut cursor), [0, 1]);
    assert_eq!(cursor.consumed_bytes(), 3);
    assert_eq!(stream.len(), 5);
    assert_eq!(cursor.next_index().unwrap(), None);
}
