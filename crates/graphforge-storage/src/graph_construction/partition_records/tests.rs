use super::*;

fn record<const N: usize>(key: u128, name: &str) -> [u8; N] {
    let mut record = [0; N];
    record[..16].copy_from_slice(&key.to_be_bytes());
    // Edge endpoints are part of the ordering too, not just the UUID/name.
    if N == 304 {
        record[16..48].fill((key % 251) as u8);
    }
    let prefix = N - 256;
    record[prefix] = u8::try_from(name.len()).unwrap();
    record[prefix + 1..prefix + 1 + name.len()].copy_from_slice(name.as_bytes());
    record
}

fn compact_matches_padded<const N: usize>() {
    let codec = DetailCodec::Compact;
    let names = [
        "a".to_owned(),
        "\0".to_owned(),
        "é".to_owned(),
        "z".repeat(255),
    ];
    let mut padded = (0..257)
        .flat_map(|key| names.iter().map(move |name| record::<N>(key, name)))
        .collect::<Vec<_>>();
    // Equal UUIDs with different wire lengths and values exercise the full
    // comparator; exact duplicates exercise ties without relying on stability.
    padded.push(padded[11]);
    if N == 304 {
        for position in [16, 31, 32, 47] {
            let mut edge = padded[11];
            edge[position] ^= 0xff;
            padded.push(edge);
        }
    }
    padded.reverse();
    let wire_bytes = padded
        .iter()
        .map(|record| codec.bytes(record).unwrap().len())
        .sum::<usize>();
    let mut compact =
        PartitionRecords::new(Some(codec), Some(padded.len() as u64), wire_bytes as u64).unwrap();
    for record in &padded {
        compact.push(*record);
    }
    compact.sort();
    padded.sort_unstable();
    let expected = padded
        .iter()
        .flat_map(|record| codec.bytes(record).unwrap().iter().copied())
        .collect::<Vec<_>>();
    let actual = compact.iter().flatten().copied().collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert_eq!(compact.len(), padded.len());
}

#[test]
fn compact_details_preserve_padded_sort_order_and_exact_wire_bytes() {
    compact_matches_padded::<272>();
    compact_matches_padded::<304>();
}

#[test]
fn short_detail_names_reserve_wire_bytes_plus_one_offset_per_record() {
    const COUNT: usize = 16_384;
    let wire_width = 48 + 1 + "LINK".len();
    let mut records = PartitionRecords::<304>::new(
        Some(DetailCodec::Compact),
        Some(COUNT as u64),
        (COUNT * wire_width) as u64,
    )
    .unwrap();
    for key in 0..COUNT {
        records.push(record(key as u128, "LINK"));
    }
    let PartitionRecords::Details { bytes, offsets } = records else {
        panic!("detail records must use compact storage");
    };
    assert_eq!(bytes.len(), COUNT * wire_width);
    assert_eq!(bytes.capacity(), bytes.len());
    assert_eq!(offsets.len(), COUNT);
    assert_eq!(offsets.capacity(), COUNT);
    let resident = bytes.capacity() + offsets.capacity() * std::mem::size_of::<usize>();
    assert!(resident * 4 < COUNT * 304, "{resident}");
}

#[test]
fn fixed_records_and_empty_partitions_keep_their_representation() {
    let mut fixed = PartitionRecords::<16>::new(None, Some(3), 48).unwrap();
    for key in [3_u128, 1, 2] {
        fixed.push(key.to_be_bytes());
    }
    fixed.sort();
    assert_eq!(
        fixed.iter().flatten().copied().collect::<Vec<_>>(),
        [
            1_u128.to_be_bytes(),
            2_u128.to_be_bytes(),
            3_u128.to_be_bytes()
        ]
        .concat()
    );
    let mut empty = PartitionRecords::<272>::new(Some(DetailCodec::Compact), Some(0), 0).unwrap();
    empty.sort();
    assert_eq!(empty.iter().count(), 0);
    assert!(PartitionRecords::<16>::new(Some(DetailCodec::Compact), None, 0).is_err());
}
