use std::sync::Arc;

use super::*;
use parquet::basic::Type as PhysicalType;
use parquet::data_type::{ByteArray, ByteArrayType};
use parquet::encodings::encoding::{
    DeltaByteArrayEncoder, DeltaLengthByteArrayEncoder, DictEncoder, Encoder, PlainEncoder,
};
use parquet::encodings::levels::LevelEncoder;
use parquet::schema::types::Type as SchemaType;
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath};
use parquet::util::bit_util::{BitWriter, num_required_bits};

fn byte_array_desc() -> ColumnDescPtr {
    let ty = SchemaType::primitive_type_builder("t", PhysicalType::BYTE_ARRAY)
        .build()
        .unwrap();
    Arc::new(ColumnDescriptor::new(
        Arc::new(ty),
        0,
        0,
        ColumnPath::new(vec![]),
    ))
}

fn varied_values(count: usize) -> Vec<ByteArray> {
    (0..count)
        .map(|index| {
            ByteArray::from(format!("value-{}-{}", index % 17, "x".repeat(index % 43)).as_bytes())
        })
        .collect()
}

fn expected_lengths(values: &[ByteArray]) -> Vec<u64> {
    values
        .iter()
        .map(|value| u64::try_from(value.len()).unwrap())
        .collect()
}

fn plain_page(values: &[ByteArray]) -> Vec<u8> {
    let mut encoder = PlainEncoder::<ByteArrayType>::new();
    encoder.put(values).unwrap();
    encoder.flush_buffer().unwrap().to_vec()
}

fn delta_page(encoding: Encoding, values: &[ByteArray]) -> Vec<u8> {
    match encoding {
        Encoding::DELTA_LENGTH_BYTE_ARRAY => {
            let mut encoder = DeltaLengthByteArrayEncoder::<ByteArrayType>::new();
            encoder.put(values).unwrap();
            encoder.flush_buffer().unwrap().to_vec()
        }
        Encoding::DELTA_BYTE_ARRAY => {
            let mut encoder = DeltaByteArrayEncoder::<ByteArrayType>::new();
            encoder.put(values).unwrap();
            encoder.flush_buffer().unwrap().to_vec()
        }
        _ => unreachable!("delta page helper supports only delta encodings"),
    }
}

/// One real dictionary page body and its entry count, in the pinned interning
/// order (first occurrence of each distinct value).
fn dictionary_page(values: &[ByteArray]) -> (Vec<u8>, usize) {
    let mut encoder = DictEncoder::<ByteArrayType>::new(byte_array_desc());
    encoder.put(values).unwrap();
    let dict = encoder.write_dict().unwrap().to_vec();
    (dict, encoder.num_entries())
}

fn encode_hybrid(max: i16, levels: &[i16]) -> Vec<u8> {
    let mut encoder = LevelEncoder::v2_streaming(max);
    encoder.put_with_observer(levels, |_, _| {});
    encoder.consume()
}

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

/// Drains every cursor under the block contract: each call fills at most 1024
/// lengths of the 2048-slot sentinel buffer, the second half stays untouched,
/// and the emitted prefix must equal the expected lengths exactly.
fn drain(cursor: &mut impl LengthSource, expected: &[u64]) {
    let mut block = [u64::MAX; 2048];
    let mut at = 0;
    loop {
        let count = cursor.next_block(&mut block).expect("length decode");
        assert!(count <= 1024, "block bound");
        assert!(
            block[1024..].iter().all(|&value| value == u64::MAX),
            "sentinel half untouched",
        );
        for length in &block[..count] {
            assert_eq!(*length, expected[at], "length at {at}");
            at += 1;
        }
        if count == 0 {
            break;
        }
    }
    assert_eq!(at, expected.len());
}

fn dictionary_facts(dict: &[u8], entries: usize) -> DictionaryByteFacts {
    DictionaryByteFacts::new(dict, entries, entries * size_of::<u64>(), None).unwrap()
}

#[test]
fn plain_lengths_track_real_encoder_values_and_preserve_prefix_tail() {
    let values = varied_values(600);
    let expected = expected_lengths(&values);
    let encoded = plain_page(&values);

    // Mutation sentinel: the whole stream stays valid, so a production change
    // that requires whole-body exhaustion instead of the exact expected
    // prefix makes this admitted-prefix run fail.
    let mut cursor = ValueLengths::new(Encoding::PLAIN, &encoded, 3, None).unwrap();
    drain(&mut cursor, &expected[..3]);
    assert_eq!(cursor.emitted(), 3);
    assert_eq!(cursor.next_length().unwrap(), None);

    let mut tailed = encoded.clone();
    tailed.extend_from_slice(b"admitted unused tail");
    let mut cursor = ValueLengths::new(Encoding::PLAIN, &tailed, values.len(), None).unwrap();
    drain(&mut cursor, &expected);
    assert_eq!(cursor.emitted(), values.len());
    assert_eq!(cursor.next_length().unwrap(), None);

    // Zero required values stay valid with or without any body.
    let mut cursor = ValueLengths::new(Encoding::PLAIN, &encoded, 0, None).unwrap();
    assert_eq!(cursor.next_length().unwrap(), None);
    assert_eq!(cursor.next_block(&mut [0_u64; 4]).unwrap(), 0);
    let mut cursor = ValueLengths::new(Encoding::PLAIN, &[], 0, None).unwrap();
    assert_eq!(cursor.next_length().unwrap(), None);
}

#[test]
fn plain_facts_aggregate_real_payload_totals() {
    let values = varied_values(517);
    let expected = expected_lengths(&values);
    let encoded = plain_page(&values);
    let facts = plain_value_facts(&encoded, values.len(), None).unwrap();
    assert_eq!(facts.payload_bytes, expected.iter().sum::<u64>());
    assert_eq!(
        facts.largest_length,
        expected.iter().max().copied().unwrap()
    );

    let facts = plain_value_facts(&[], 0, None).unwrap();
    assert_eq!(facts.payload_bytes, 0);
    assert_eq!(facts.largest_length, 0);
}

#[test]
fn plain_blocks_stay_bounded_and_honor_cancellation() {
    let values = varied_values(3073);
    let encoded = plain_page(&values);
    let mut cursor = ValueLengths::new(Encoding::PLAIN, &encoded, values.len(), None).unwrap();
    drain(&mut cursor, &expected_lengths(&values));
    assert_eq!(cursor.emitted(), values.len());

    let cancellation = CancellationToken::new();
    let mut cursor =
        ValueLengths::new(Encoding::PLAIN, &encoded, values.len(), Some(&cancellation)).unwrap();
    cancellation.cancel();
    assert!(cursor.next_block(&mut [0_u64; 1024]).is_err());
    assert!(plain_value_facts(&encoded, values.len(), Some(&cancellation)).is_err());
    assert!(
        ValueLengths::new(Encoding::PLAIN, &encoded, values.len(), Some(&cancellation)).is_err()
    );
}

#[test]
fn plain_refuses_tiny_bodies_with_huge_negative_or_truncated_headers() {
    // A structurally valid four-byte header claiming the full i32 range:
    // refused without any length-sized allocation.
    let mut cursor = ValueLengths::new(Encoding::PLAIN, &i32::MAX.to_le_bytes(), 1, None).unwrap();
    assert!(cursor.next_length().is_err());

    let mut negative = Vec::new();
    negative.extend_from_slice(&(-2_i32).to_le_bytes());
    negative.extend_from_slice(b"ab");
    let mut cursor = ValueLengths::new(Encoding::PLAIN, &negative, 1, None).unwrap();
    assert!(cursor.next_length().is_err());

    let mut cursor = ValueLengths::new(Encoding::PLAIN, &[1_u8, 0], 1, None).unwrap();
    assert!(cursor.next_length().is_err());

    let mut truncated = Vec::new();
    truncated.extend_from_slice(&4_i32.to_le_bytes());
    truncated.extend_from_slice(b"a");
    let mut cursor = ValueLengths::new(Encoding::PLAIN, &truncated, 1, None).unwrap();
    assert!(cursor.next_length().is_err());
}

#[test]
fn delta_length_sources_match_real_encoders_and_enforce_count_agreement() {
    let values = varied_values(1025);
    let expected = expected_lengths(&values);
    for encoding in [
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
        Encoding::DELTA_BYTE_ARRAY,
    ] {
        let encoded = delta_page(encoding, &values);

        let mut cursor = ValueLengths::new(encoding, &encoded, values.len(), None).unwrap();
        drain(&mut cursor, &expected);
        assert_eq!(cursor.emitted(), values.len());
        assert_eq!(cursor.next_length().unwrap(), None);

        // Unused tails after the required values stay admitted.
        let mut tailed = encoded.clone();
        tailed.extend_from_slice(b"admitted tail");
        let mut cursor = ValueLengths::new(encoding, &tailed, values.len(), None).unwrap();
        drain(&mut cursor, &expected);

        // A complete valid stream admitted as one value fewer is refused by
        // the delta body-count maximum.
        assert!(ValueLengths::new(encoding, &encoded, values.len() - 1, None).is_err());
        // Mutation sentinel: the stream remains fully valid for its own
        // count, so reverting the exact nonnull agreement guard makes this
        // admission succeed.
        let error = ValueLengths::new(encoding, &encoded, values.len() + 1, None).unwrap_err();
        assert!(error.to_string().contains("nonnull"), "{error}");
    }

    // A real encoder stream for zero values satisfies a zero requirement.
    let mut encoder = DeltaLengthByteArrayEncoder::<ByteArrayType>::new();
    let empty = encoder.flush_buffer().unwrap().to_vec();
    let mut cursor = ValueLengths::new(Encoding::DELTA_LENGTH_BYTE_ARRAY, &empty, 0, None).unwrap();
    assert_eq!(cursor.next_length().unwrap(), None);
}

#[test]
fn delta_length_sources_honor_cancellation() {
    let values = varied_values(3);
    let encoded = delta_page(Encoding::DELTA_BYTE_ARRAY, &values);
    let cancelled_token = CancellationToken::new();
    cancelled_token.cancel();
    assert!(
        ValueLengths::new(
            Encoding::DELTA_BYTE_ARRAY,
            &encoded,
            values.len(),
            Some(&cancelled_token)
        )
        .is_err()
    );

    let fresh = CancellationToken::new();
    let mut cursor = ValueLengths::new(
        Encoding::DELTA_BYTE_ARRAY,
        &encoded,
        values.len(),
        Some(&fresh),
    )
    .unwrap();
    fresh.cancel();
    assert!(cursor.next_block(&mut [0_u64; 1024]).is_err());
    assert!(cursor.next_length().is_err());
}

#[test]
fn dictionary_facts_admit_exact_budget_and_refuse_one_byte_less() {
    let entries = varied_values(100);
    let (dict, count) = dictionary_page(&entries);
    assert_eq!(count, entries.len());
    let facts = dictionary_facts(&dict, count);
    assert_eq!(facts.entries(), count);
    for (index, value) in entries.iter().enumerate() {
        assert_eq!(
            facts.length(index),
            Some(u64::try_from(value.len()).unwrap())
        );
    }
    let expected = expected_lengths(&entries);
    assert_eq!(facts.payload_bytes(), expected.iter().sum::<u64>());
    assert_eq!(
        facts.largest_length(),
        expected.iter().max().copied().unwrap()
    );

    // Mutation sentinel: reverting either the pre-reserve capacity charge or
    // the actual-capacity check makes this one-byte-short admission succeed.
    let error =
        DictionaryByteFacts::new(&dict, count, count * size_of::<u64>() - 1, None).unwrap_err();
    assert!(error.to_string().contains("capacity"), "{error}");
}

#[test]
fn dictionary_facts_refuse_huge_counts_and_malformed_geometry() {
    let entries = varied_values(2);
    let (dict, count) = dictionary_page(&entries);

    // The capacity charge fires before any allocation request; reverting both
    // allocation guards makes this admission attempt the huge resize instead.
    assert!(DictionaryByteFacts::new(&dict, 1_usize << 40, 1024, None).is_err());
    assert!(DictionaryByteFacts::new(&dict, usize::MAX, 1024, None).is_err());

    // Honest count, truncated final payload.
    let mut truncated = dict.clone();
    truncated.pop();
    assert!(DictionaryByteFacts::new(&truncated, count, count * size_of::<u64>(), None).is_err());

    // Truncated entry header.
    let header_only = vec![3_u8, 0];
    assert!(DictionaryByteFacts::new(&header_only, 1, size_of::<u64>(), None).is_err());

    // Negative length inside the body.
    let mut negative = Vec::new();
    negative.extend_from_slice(&4_i32.to_le_bytes());
    negative.extend_from_slice(b"abcd");
    negative.extend_from_slice(&(-1_i32).to_le_bytes());
    assert!(DictionaryByteFacts::new(&negative, 2, 2 * size_of::<u64>(), None).is_err());

    // Unused tails after the entries stay admitted.
    let mut tailed = dict.clone();
    tailed.extend_from_slice(b"admitted tail");
    let facts = dictionary_facts(&tailed, count);
    assert_eq!(facts.entries(), count);

    // A zero-entry dictionary is valid; its vector needs no capacity.
    let facts = dictionary_facts(&[], 0);
    assert_eq!(facts.entries(), 0);
    assert_eq!(facts.payload_bytes(), 0);
    assert_eq!(facts.largest_length(), 0);
    let facts = dictionary_facts(&dict, 0);
    assert_eq!(facts.entries(), 0);
}

#[test]
fn dictionary_expanded_lengths_expand_repeated_real_indices_in_blocks() {
    let dict_values: Vec<ByteArray> = (0..5)
        .map(|index| {
            ByteArray::from(format!("dict-{}-{}", index, "y".repeat(index * 7)).as_bytes())
        })
        .collect();
    let pattern: [usize; 10] = [0, 1, 1, 4, 4, 4, 2, 3, 0, 4];
    let mut data = dict_values.clone();
    data.extend(
        (0..3073 - dict_values.len())
            .map(|index| dict_values[pattern[index % pattern.len()]].clone()),
    );

    // One real encoder builds both sides, so the admitted facts and the index
    // stream share the pinned interning order: the seed values make entry
    // numbers equal the positions in dict_values.
    let mut encoder = DictEncoder::<ByteArrayType>::new(byte_array_desc());
    encoder.put(&data).unwrap();
    let dict = encoder.write_dict().unwrap().to_vec();
    let entries = encoder.num_entries();
    assert_eq!(entries, dict_values.len());
    let facts = dictionary_facts(&dict, entries);
    let stream = encoder.write_indices().unwrap().to_vec();

    let mut expected = expected_lengths(&dict_values);
    expected
        .extend((0..3073 - dict_values.len()).map(|index| {
            u64::try_from(dict_values[pattern[index % pattern.len()]].len()).unwrap()
        }));

    // Mutation sentinel: repeated references with distinct entry lengths fail
    // any production change that reports a dictionary maximum, the compressed
    // length, or a single entry instead of each referenced length.
    let mut cursor = DictionaryExpanded::new(Some(&facts), &stream, data.len(), None).unwrap();
    drain(&mut cursor, &expected);
    assert_eq!(cursor.emitted(), data.len());
    assert_eq!(cursor.next_length().unwrap(), None);

    let cancellation = CancellationToken::new();
    let mut cursor =
        DictionaryExpanded::new(Some(&facts), &stream, data.len(), Some(&cancellation)).unwrap();
    cancellation.cancel();
    assert!(cursor.next_block(&mut [0_u64; 1024]).is_err());
}

#[test]
fn dictionary_expanded_refuses_missing_dictionary_and_out_of_range_indices() {
    let entries = varied_values(3);
    let (dict, count) = dictionary_page(&entries);
    let facts = dictionary_facts(&dict, count);

    // A missing dictionary with required nonnull values is refused.
    assert!(DictionaryExpanded::new(None, &[2_u8, 0], 1, None).is_err());
    // A zero-entry dictionary with required nonnull values is refused.
    let empty = dictionary_facts(&[], 0);
    assert!(DictionaryExpanded::new(Some(&empty), &[0_u8], 1, None).is_err());

    // Out-of-range indices are refused by the existing index cursor. The run
    // grammar is complete and valid; only the referenced entry is missing.
    let max = i16::try_from(count - 1).unwrap();
    let width = num_required_bits(u64::try_from(count - 1).unwrap());
    let mut stream = vec![width];
    stream.extend_from_slice(&rle_run(2, u64::from(u16::try_from(count).unwrap()), width));
    let mut cursor = DictionaryExpanded::new(Some(&facts), &stream, 2, None).unwrap();
    let error = cursor.next_length().unwrap_err();
    assert!(error.to_string().contains("allowed maximum"), "{error}");

    // A fully valid stream cannot satisfy a larger required count.
    let indices: Vec<u32> = (0..4)
        .map(|index| index % u32::try_from(count).unwrap())
        .collect();
    let levels: Vec<i16> = indices
        .iter()
        .map(|&index| i16::try_from(index).unwrap())
        .collect();
    let mut stream = vec![width];
    stream.extend_from_slice(&encode_hybrid(max, &levels));
    let mut cursor =
        DictionaryExpanded::new(Some(&facts), &stream, indices.len() + 1, None).unwrap();
    for &index in &indices {
        let expected_length =
            u64::try_from(entries[usize::try_from(index).unwrap()].len()).unwrap();
        assert_eq!(cursor.next_length().unwrap(), Some(expected_length));
    }
    assert!(cursor.next_length().is_err());
}

#[test]
fn dictionary_expanded_accepts_width0_padding_and_zero_required_values() {
    let single = varied_values(1);
    let (dict, count) = dictionary_page(&single);
    let facts = dictionary_facts(&dict, count);
    let only = u64::try_from(single[0].len()).unwrap();

    // One-entry dictionary: the legal width-zero stream repeats index zero.
    let mut stream = vec![0_u8];
    stream.extend_from_slice(&encode_hybrid(0, &[0_i16; 10]));
    let mut cursor = DictionaryExpanded::new(Some(&facts), &stream, 10, None).unwrap();
    drain(&mut cursor, &[only; 10]);
    assert_eq!(cursor.next_length().unwrap(), None);

    let entries = varied_values(3);
    let (dict, count) = dictionary_page(&entries);
    let facts = dictionary_facts(&dict, count);
    let width = num_required_bits(u64::try_from(count - 1).unwrap());

    // Legal final packed padding is neither validated nor consumed: the tail
    // holds out-of-range indices that must never decode.
    let mut stream = vec![width];
    stream.extend_from_slice(&packed_run(1, &[0, 1, 2, 1, 0, 9, 9, 9], width));
    let expected: Vec<u64> = [0_usize, 1, 2, 1, 0]
        .iter()
        .map(|&index| u64::try_from(entries[index].len()).unwrap())
        .collect();
    let mut cursor = DictionaryExpanded::new(Some(&facts), &stream, 5, None).unwrap();
    drain(&mut cursor, &expected);
    assert_eq!(cursor.next_length().unwrap(), None);

    // Zero required values end the cursor before any stream byte is read.
    let mut cursor = DictionaryExpanded::new(Some(&facts), &[], 0, None).unwrap();
    assert_eq!(cursor.next_length().unwrap(), None);
}

#[test]
fn length_sources_reject_unsupported_encodings() {
    let values = varied_values(2);
    let encoded = plain_page(&values);
    assert!(ValueLengths::new(Encoding::RLE_DICTIONARY, &encoded, 2, None).is_err());
    assert!(ValueLengths::new(Encoding::PLAIN_DICTIONARY, &encoded, 2, None).is_err());
}
