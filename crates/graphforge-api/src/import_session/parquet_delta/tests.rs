use super::*;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type};
use parquet::encodings::encoding::{
    DeltaBitPackEncoder, DeltaByteArrayEncoder, DeltaLengthByteArrayEncoder, Encoder,
};

#[test]
fn delta_integer_validation_tracks_real_miniblocks_and_padding() {
    for count in [1, 2, 31, 32, 33, 127, 128, 129, 255, 256, 257, 1024] {
        let values = (0..count)
            .map(|index| ((index * 17) % 131) - 65)
            .collect::<Vec<i32>>();
        let mut encoder = DeltaBitPackEncoder::<Int32Type>::new();
        encoder.put(&values).unwrap();
        let encoded = encoder.flush_buffer().unwrap();
        let mut decoder = Integers::new(&encoded, values.len(), 32).unwrap();
        for expected in values {
            assert_eq!(decoder.next().unwrap(), Some(i64::from(expected)));
        }
        assert_eq!(decoder.next().unwrap(), None);
        assert_eq!(decoder.finish().unwrap(), encoded.len());
    }
}

#[test]
fn real_delta_length_and_prefix_pages_validate_across_blocks() {
    let values = (0..1025)
        .map(|index| {
            ByteArray::from(
                format!("a shared prefix with variable suffix {}", index % 47).as_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let mut lengths = DeltaLengthByteArrayEncoder::<ByteArrayType>::new();
    lengths.put(&values).unwrap();
    let encoded = lengths.flush_buffer().unwrap();
    let facts = validate(
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
        &encoded,
        values.len(),
        32,
    )
    .unwrap()
    .unwrap();
    assert_eq!(facts.values, values.len());
    assert_eq!(
        facts.largest_value,
        values.iter().map(|value| value.len() as u64).max().unwrap()
    );
    assert!(facts.auxiliary_bytes >= values.len() as u64 * 4);

    let mut prefixes = DeltaByteArrayEncoder::<ByteArrayType>::new();
    prefixes.put(&values).unwrap();
    let encoded = prefixes.flush_buffer().unwrap();
    let facts = validate(Encoding::DELTA_BYTE_ARRAY, &encoded, values.len(), 32)
        .unwrap()
        .unwrap();
    assert_eq!(facts.values, values.len());
    assert_eq!(
        facts.largest_value,
        values.iter().map(|value| value.len() as u64).max().unwrap()
    );
    assert!(facts.auxiliary_bytes >= values.len() as u64 * 8);
}

#[test]
fn tiny_delta_body_cannot_supply_a_count_beyond_the_admitted_header() {
    // block=128, miniblocks=4, count=2^36. The body ends immediately, before
    // any miniblock. Third-party length decoders resize from this count first.
    let body = [128, 1, 4, 128, 128, 128, 128, 128, 2, 0];
    for encoding in [
        Encoding::DELTA_BINARY_PACKED,
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
        Encoding::DELTA_BYTE_ARRAY,
    ] {
        assert!(validate(encoding, &body, 1, 32).is_err());
    }
}

#[test]
fn delta_lengths_reject_negative_values_and_mismatched_suffix_storage() {
    let mut encoder = DeltaBitPackEncoder::<Int32Type>::new();
    encoder.put(&[-1]).unwrap();
    let encoded = encoder.flush_buffer().unwrap();
    assert!(validate(Encoding::DELTA_LENGTH_BYTE_ARRAY, &encoded, 1, 32).is_err());
    let mut encoder = DeltaBitPackEncoder::<Int32Type>::new();
    encoder.put(&[2]).unwrap();
    let mut encoded = encoder.flush_buffer().unwrap().to_vec();
    encoded.extend_from_slice(b"a");
    assert!(validate(Encoding::DELTA_LENGTH_BYTE_ARRAY, &encoded, 1, 32).is_err());
}

#[test]
fn borrowing_length_blocks_match_real_values_without_row_sized_output() {
    let values = (0..3073)
        .map(|index| {
            ByteArray::from(format!("prefix-{}-{}", index % 3, "x".repeat(index % 91)).as_bytes())
        })
        .collect::<Vec<_>>();
    let mut length_encoder = DeltaLengthByteArrayEncoder::<ByteArrayType>::new();
    length_encoder.put(&values).unwrap();
    let mut prefix_encoder = DeltaByteArrayEncoder::<ByteArrayType>::new();
    prefix_encoder.put(&values).unwrap();
    for (encoding, encoded) in [
        (
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            length_encoder.flush_buffer().unwrap(),
        ),
        (
            Encoding::DELTA_BYTE_ARRAY,
            prefix_encoder.flush_buffer().unwrap(),
        ),
    ] {
        let mut cursor = Lengths::new(encoding, &encoded, values.len(), None).unwrap();
        assert_eq!(cursor.count(), values.len());
        let mut block = [u64::MAX; 2048];
        let mut at = 0;
        loop {
            let count = cursor.next_block(&mut block).unwrap();
            assert!(count <= 1024);
            assert!(block[1024..].iter().all(|value| *value == u64::MAX));
            for length in &block[..count] {
                assert_eq!(*length, values[at].len() as u64);
                at += 1;
            }
            if count == 0 {
                break;
            }
        }
        assert_eq!(at, values.len());
        let cancellation = CancellationToken::new();
        let mut cursor =
            Lengths::new(encoding, &encoded, values.len(), Some(&cancellation)).unwrap();
        cancellation.cancel();
        assert!(cursor.next_block(&mut block).is_err());
        assert!(Lengths::new(encoding, &encoded, values.len(), Some(&cancellation)).is_err());
    }
}

#[test]
fn borrowing_length_blocks_reject_invalid_prefixes_and_payload_counts() {
    let mut prefixes = DeltaBitPackEncoder::<Int32Type>::new();
    prefixes.put(&[1]).unwrap(); // first value has no previous prefix to reuse
    let mut suffixes = DeltaBitPackEncoder::<Int32Type>::new();
    suffixes.put(&[0]).unwrap();
    let mut encoded = prefixes.flush_buffer().unwrap().to_vec();
    encoded.extend_from_slice(&suffixes.flush_buffer().unwrap());
    let mut cursor = Lengths::new(Encoding::DELTA_BYTE_ARRAY, &encoded, 1, None).unwrap();
    assert!(cursor.next_block(&mut [0; 1]).is_err());

    let mut lengths = DeltaBitPackEncoder::<Int32Type>::new();
    lengths.put(&[2]).unwrap();
    let mut encoded = lengths.flush_buffer().unwrap().to_vec();
    encoded.extend_from_slice(b"a");
    let mut cursor = Lengths::new(Encoding::DELTA_LENGTH_BYTE_ARRAY, &encoded, 1, None).unwrap();
    assert!(cursor.next_block(&mut [0; 1]).is_err());

    let mut lengths = DeltaBitPackEncoder::<Int32Type>::new();
    lengths.put(&[0]).unwrap();
    let mut encoded = lengths.flush_buffer().unwrap().to_vec();
    encoded.extend_from_slice(b"unused");
    let mut cursor = Lengths::new(Encoding::DELTA_LENGTH_BYTE_ARRAY, &encoded, 1, None).unwrap();
    assert_eq!(cursor.next_block(&mut [0; 1]).unwrap(), 1);
    assert_eq!(cursor.next_block(&mut [0; 1]).unwrap(), 0);
}

#[test]
fn complete_delta_stream_cannot_exceed_its_admitted_value_count() {
    let mut encoder = DeltaBitPackEncoder::<Int32Type>::new();
    encoder.put(&[0, 0]).unwrap();
    let encoded = encoder.flush_buffer().unwrap();
    // Unlike a truncated malicious header, this stream remains fully valid if
    // the admission guard is reverted: that mutation must fail this assertion.
    for encoding in [
        Encoding::DELTA_BINARY_PACKED,
        Encoding::DELTA_LENGTH_BYTE_ARRAY,
    ] {
        assert!(validate(encoding, &encoded, 1, 32).is_err());
    }
    assert!(Lengths::new(Encoding::DELTA_LENGTH_BYTE_ARRAY, &encoded, 1, None).is_err());
}

#[test]
fn admitted_delta_pages_preserve_the_consumers_unused_tail() {
    let values = [
        ByteArray::from(""),
        ByteArray::from("prefix"),
        ByteArray::from("prefix suffix"),
    ];
    let mut lengths = DeltaLengthByteArrayEncoder::<ByteArrayType>::new();
    lengths.put(&values).unwrap();
    let mut prefixes = DeltaByteArrayEncoder::<ByteArrayType>::new();
    prefixes.put(&values).unwrap();
    for (encoding, bytes) in [
        (
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            lengths.flush_buffer().unwrap(),
        ),
        (Encoding::DELTA_BYTE_ARRAY, prefixes.flush_buffer().unwrap()),
    ] {
        let mut bytes = bytes.to_vec();
        bytes.extend_from_slice(b"unused admitted tail");
        assert_eq!(
            validate(encoding, &bytes, values.len(), 32)
                .unwrap()
                .unwrap()
                .values,
            values.len()
        );
        let mut cursor = Lengths::new(encoding, &bytes, values.len(), None).unwrap();
        let mut block = [0; 3];
        assert_eq!(cursor.next_block(&mut block).unwrap(), 3);
        assert_eq!(block, [0, 6, 13]);
        assert_eq!(cursor.next_block(&mut block).unwrap(), 0);
    }
}
