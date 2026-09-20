use super::*;
use std::sync::Arc;

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
        super::super::partition::default_materialization_bytes(),
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

#[test]
fn concentrated_fixed_partition_is_refused_before_record_allocation() {
    let root = tempfile::TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let mut session = super::super::tests::open(&root, 0x1439);
    let mut partitioner =
        FixedRangePartitioner::<33>::new(&session.root, PartitionFamily::Endpoints, 1, None, false)
            .unwrap();
    // Every endpoint has the same hub key. More UUID splitters cannot separate it.
    let mut wire = Vec::new();
    for i in 0_u64..1024 {
        let mut row = [0; 33];
        row[24..32].copy_from_slice(&i.to_be_bytes());
        wire.extend_from_slice(&row);
    }
    partitioner
        .route_slice(0, &wire, 1024, &mut session.checkpoint.evidence)
        .unwrap();
    partitioner.seal(&mut session.checkpoint.evidence).unwrap();
    let name = fixed_spill_name(PartitionFamily::Endpoints, 0);
    let error = load_fixed_partition::<33>(
        &session.root,
        &name,
        Some(1024),
        None,
        1024 * 33 - 1,
        &AtomicBool::new(false),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("exceeds recorded budget"));
    let (rows, _) = load_fixed_partition::<33>(
        &session.root,
        &name,
        Some(1024),
        None,
        1024 * 33,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(rows.len(), 1024);
    // A corrupt routed count cannot make the Vec grow beyond its reservation.
    assert!(
        load_fixed_partition::<33>(
            &session.root,
            &name,
            Some(1),
            None,
            33,
            &AtomicBool::new(false)
        )
        .err()
        .unwrap()
        .to_string()
        .contains("admitted record count")
    );
}

#[test]
fn row_partition_admits_before_decode_and_authenticates_before_ipc_allocation() {
    use arrow::{
        array::{FixedSizeBinaryArray, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    let root = tempfile::TempDir::new().unwrap();
    crate::open_or_initialize_project(root.path()).unwrap();
    let mut session = super::super::tests::open(&root, 0x143a);
    let schema = Arc::new(Schema::new(vec![
        Field::new("uuid", DataType::FixedSizeBinary(16), false),
        Field::new("text", DataType::Utf8, false),
    ]));
    let ids = [[2_u8; 16], [1_u8; 16]];
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(ids.iter()).unwrap()),
            Arc::new(StringArray::from(vec![
                "long property".repeat(1000),
                "other".to_owned(),
            ])),
        ],
    )
    .unwrap();
    let mut partitioner = RowRangePartitioner::new(&session.root, "node-budget-test", 1).unwrap();
    partitioner
        .route_slice(&schema, &batch, 0, 2, 0, &mut session.checkpoint.evidence)
        .unwrap();
    partitioner.seal(&mut session.checkpoint.evidence).unwrap();
    let name = partitioner.sealed[0].as_ref().unwrap().name.clone();
    let sorted = partitioner
        .load_partition(0, &name, &schema, &mut session.checkpoint.evidence)
        .unwrap();
    assert_eq!(uuid_value(key_column(&sorted).unwrap(), 0).unwrap(), ids[1]);
    partitioner.max_partition_bytes = 1;
    let error = partitioner
        .load_partition(0, &name, &schema, &mut session.checkpoint.evidence)
        .unwrap_err();
    assert!(error.to_string().contains("exceeds recorded budget"));
    partitioner.max_partition_bytes = super::super::partition::default_materialization_bytes();
    let path = root
        .path()
        .join(".graphforge-construction")
        .join(format!("{:032x}", 0x143a))
        .join(&name);
    // Corrupt the IPC metadata length. Arrow must never get to allocate it.
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[4..8].copy_from_slice(&i32::MAX.to_le_bytes());
    std::fs::write(&path, bytes).unwrap();
    let error = partitioner
        .load_partition(0, &name, &schema, &mut session.checkpoint.evidence)
        .unwrap_err();
    assert!(error.to_string().contains("digest"), "{error}");
}
