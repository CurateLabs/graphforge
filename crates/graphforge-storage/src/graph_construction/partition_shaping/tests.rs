use super::*;

// BenchExec can run either fixture in a fresh test process with
// GF_DETAIL_PARTITION_ROWS set to a fixed larger scale. No timing or resource
// measurements originate in this fixture: it only proves bytes and counts.
fn detail_partition_load<const N: usize>(family: PartitionFamily) {
    let count = std::env::var("GF_DETAIL_PARTITION_ROWS")
        .map(|value| value.parse::<u64>().expect("positive record count"))
        .unwrap_or(16_384);
    assert!(count > 0);
    let root = tempfile::TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let mut session = super::super::tests::open(&root, 0x1441);
    let codec = DetailCodec::Compact;
    let mut partitioner =
        FixedRangePartitioner::<N>::new(&session.root, family, 1, Some(codec), true).unwrap();
    let mut block = Vec::new();
    let mut block_records = 0;
    for key in (1..=count).rev() {
        let record = detail_record::<N>(key);
        block.extend_from_slice(codec.bytes(&record).unwrap());
        block_records += 1;
        if block_records == 4096 || key == 1 {
            partitioner
                .route_slice(0, &block, block_records, &mut session.checkpoint.evidence)
                .unwrap();
            block.clear();
            block_records = 0;
        }
    }
    partitioner.seal(&mut session.checkpoint.evidence).unwrap();
    drop(block);
    drop(partitioner);
    let (records, counters) = load_fixed_partition::<N>(
        &session.root,
        &fixed_spill_name(family, 0),
        Some(count),
        Some(codec),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(counters.records, count);
    assert_eq!(records.len() as u64, count);
    let mut digest = sha2::Sha256::new();
    for (index, actual) in records.iter().enumerate() {
        let expected = detail_record::<N>(index as u64 + 1);
        let wire = codec.bytes(&expected).unwrap();
        // Works for both the old padded Vec and compact wire storage, letting
        // this exact fixture run unchanged against the comparison baseline.
        assert_eq!(&actual[..wire.len()], wire);
        digest.update(wire);
    }
    println!(
        "detail_width={N} records={count} sha256={}",
        hex(&digest.finalize())
    );
}

fn detail_record<const N: usize>(key: u64) -> [u8; N] {
    let mut record = [0; N];
    record[..16].copy_from_slice(&u128::from(key).to_be_bytes());
    if N == 304 {
        record[16..32].copy_from_slice(&u128::from(key / 2).to_be_bytes());
        record[32..48].copy_from_slice(&u128::from(key / 3).to_be_bytes());
    }
    let prefix = N - 256;
    record[prefix] = 4;
    record[prefix + 1..prefix + 5].copy_from_slice(b"LINK");
    record
}

#[test]
fn node_detail_partition_load_preserves_sorted_wire_bytes() {
    detail_partition_load::<272>(PartitionFamily::NodeDetails);
}

#[test]
fn edge_detail_partition_load_preserves_sorted_wire_bytes() {
    detail_partition_load::<304>(PartitionFamily::EdgeDetails);
}
