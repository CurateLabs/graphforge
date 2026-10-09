use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::Int64Array;

use super::*;

fn payload(length: usize) -> Vec<u8> {
    let mut state = 0x91ec_37f2_81da_324d_u64;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

fn parts(logical: &[u8]) -> Vec<Bytes> {
    let mut result = Vec::new();
    let count = encode_parts(
        &mut Cursor::new(logical),
        logical.len() as u64,
        |index, bytes| {
            assert_eq!(index, result.len() as u64);
            result.push(bytes);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(count, result.len() as u64);
    result
}

fn source(parts: Vec<Bytes>) -> SegmentedSource {
    let (layout, index) = inspect_envelope(parts[0].clone()).unwrap().unwrap();
    assert_eq!(index, 0);
    validate_parts(layout, &(0..parts.len() as u64).collect::<Vec<_>>()).unwrap();
    SegmentedSource::new(
        layout,
        Arc::new(move |index| {
            parts
                .get(usize::try_from(index).unwrap())
                .cloned()
                .ok_or_else(|| invalid("missing test part"))
        }),
    )
    .unwrap()
}

#[test]
fn incompressible_payloads_have_a_complete_physical_bound_and_round_trip() {
    let logical = payload(2 * PROPERTY_OBJECT_PAYLOAD_BYTES + 19);
    let encoded = parts(&logical);
    assert_eq!(encoded.len(), 3);
    for (index, bytes) in encoded.iter().enumerate() {
        assert!(bytes.len() <= MAX_PROPERTY_OBJECT_BYTES);
        assert_eq!(&bytes[..4], b"PAR1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
        let (layout, found) = inspect_envelope(bytes.clone()).unwrap().unwrap();
        assert_eq!(found, index as u64);
        assert_eq!(layout.logical_length, logical.len() as u64);
        assert_eq!(layout.part_count, 3);
    }
    let source = source(encoded);
    assert_eq!(
        source.get_bytes(0, logical.len()).unwrap().as_ref(),
        logical
    );
}

#[test]
fn segmentation_is_deterministic() {
    let logical = payload(PROPERTY_OBJECT_PAYLOAD_BYTES + 29);
    assert_eq!(parts(&logical), parts(&logical));
}

#[test]
fn opening_is_lazy_and_reads_load_only_the_touched_part() {
    let logical = payload(2 * PROPERTY_OBJECT_PAYLOAD_BYTES + 23);
    let encoded = parts(&logical);
    let (layout, _) = inspect_envelope(encoded[0].clone()).unwrap().unwrap();
    let loads = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&loads);
    let source = SegmentedSource::new(
        layout,
        Arc::new(move |index| {
            observed.lock().unwrap().push(index);
            Ok(encoded[usize::try_from(index).unwrap()].clone())
        }),
    )
    .unwrap();
    assert!(loads.lock().unwrap().is_empty());
    let last = logical.len() - 8;
    assert_eq!(
        source.get_bytes(last as u64, 8).unwrap().as_ref(),
        &logical[last..]
    );
    assert_eq!(*loads.lock().unwrap(), [2]);
    source.get_bytes(last as u64, 4).unwrap();
    assert_eq!(
        *loads.lock().unwrap(),
        [2],
        "same part uses its retained payload"
    );
    let boundary = PROPERTY_OBJECT_PAYLOAD_BYTES - 7;
    assert_eq!(
        source.get_bytes(boundary as u64, 17).unwrap().as_ref(),
        &logical[boundary..boundary + 17]
    );
    assert_eq!(*loads.lock().unwrap(), [2, 0, 1]);
    source.get_bytes(last as u64, 1).unwrap();
    assert_eq!(
        *loads.lock().unwrap(),
        [2, 0, 1, 2],
        "only one part is retained"
    );
}

#[derive(Clone)]
struct ReadProbe {
    bytes: Bytes,
    read_bytes: Arc<AtomicUsize>,
}

impl Length for ReadProbe {
    fn len(&self) -> u64 {
        self.bytes.len() as u64
    }
}

impl ChunkReader for ReadProbe {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let remaining = self.bytes.slice(usize::try_from(start).unwrap()..);
        self.read_bytes
            .fetch_add(remaining.len(), Ordering::Relaxed);
        Ok(Cursor::new(remaining))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        self.read_bytes.fetch_add(length, Ordering::Relaxed);
        let start = usize::try_from(start).unwrap();
        Ok(self.bytes.slice(start..start + length))
    }
}

#[test]
fn envelope_recognition_reads_only_the_small_footer() {
    let encoded = parts(&payload(PROPERTY_OBJECT_PAYLOAD_BYTES));
    let read_bytes = Arc::new(AtomicUsize::new(0));
    let info = inspect_envelope(ReadProbe {
        bytes: encoded[0].clone(),
        read_bytes: Arc::clone(&read_bytes),
    })
    .unwrap()
    .unwrap();
    assert_eq!(info.0.part_count, 1);
    assert!(read_bytes.load(Ordering::Relaxed) < 16 << 10);
}

fn logical_parquet(batch: &RecordBatch) -> Bytes {
    let mut output = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(
            &mut output,
            batch.schema(),
            Some(
                WriterProperties::builder()
                    .set_compression(Compression::UNCOMPRESSED)
                    .set_dictionary_enabled(false)
                    .build(),
            ),
        )
        .unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }
    output.into()
}

#[test]
fn plain_parquet_remains_recognizable_without_a_wrapper() {
    let batch = RecordBatch::try_from_iter(vec![(
        "value",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
    )])
    .unwrap();
    assert!(inspect_envelope(logical_parquet(&batch)).unwrap().is_none());
}

#[test]
fn oversized_schema_footer_is_segmented_without_changing_the_logical_schema() {
    let field = Field::new("value", DataType::Int64, false).with_metadata(
        [(
            "description".to_owned(),
            "s".repeat(MAX_PROPERTY_OBJECT_BYTES + 1),
        )]
        .into_iter()
        .collect(),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(Int64Array::from(vec![42])) as ArrayRef],
    )
    .unwrap();
    let logical = logical_parquet(&batch);
    assert!(logical.len() > MAX_PROPERTY_OBJECT_BYTES);
    let encoded = parts(&logical);
    assert!(encoded
        .iter()
        .all(|part| part.len() <= MAX_PROPERTY_OBJECT_BYTES));
    let expected = ParquetRecordBatchReaderBuilder::try_new(logical).unwrap();
    let actual = ParquetRecordBatchReaderBuilder::try_new(source(encoded)).unwrap();
    assert!(
        actual.schema() == expected.schema(),
        "large schema metadata changed"
    );
    let actual = actual
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        actual == [batch],
        "small value or its field metadata changed"
    );
}

#[test]
fn ordinary_parquet_reader_recovers_large_values_and_schema_from_segmented_source() {
    let value = payload(2 * PROPERTY_OBJECT_PAYLOAD_BYTES + 57);
    let field = Field::new("large_value", DataType::Binary, false).with_metadata(
        [("unit".to_owned(), "opaque".to_owned())]
            .into_iter()
            .collect(),
    );
    let schema = Arc::new(
        Schema::new(vec![field]).with_metadata(
            [("custom".to_owned(), "preserved".to_owned())]
                .into_iter()
                .collect(),
        ),
    );
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(BinaryArray::from_vec(vec![value.as_slice()])) as ArrayRef],
    )
    .unwrap();
    let original = logical_parquet(&batch);
    let plain = ParquetRecordBatchReaderBuilder::try_new(original.clone()).unwrap();
    let expected_schema = Arc::clone(plain.schema());
    let expected = plain
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let segmented = ParquetRecordBatchReaderBuilder::try_new(source(parts(&original))).unwrap();
    assert_eq!(segmented.schema(), &expected_schema);
    let recovered = segmented
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    // Compare with ordinary Parquet decoding, including its batch metadata
    // convention, and keep failure output bounded for the multi-megabyte value.
    assert!(
        recovered == expected,
        "segmentation changed decoded batches"
    );
    assert_eq!(recovered.len(), 1);
    let actual = recovered[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert!(actual.value(0) == value, "large property bytes changed");
}

#[test]
fn inventory_completeness_and_canonical_names_are_required() {
    let layout = EnvelopeLayout {
        logical_length: 2 * PROPERTY_OBJECT_PAYLOAD_BYTES as u64 + 1,
        part_count: 3,
    };
    validate_parts(layout, &[0, 1, 2]).unwrap();
    for indexes in [&[0, 1][..], &[0, 1, 1], &[0, 2, 1], &[0, 1, 2, 3]] {
        assert!(validate_parts(layout, indexes).is_err());
    }
    let anchor = Path::new("properties/route/00000000000000000001-00000000000000000000.parquet");
    assert_eq!(part_path(anchor, 0), anchor);
    assert_eq!(part_index(anchor, anchor), Some(0));
    for index in [1, 12, u64::MAX] {
        let path = part_path(anchor, index);
        assert_eq!(split_part_path(&path), Some((anchor.to_owned(), index)));
        assert_eq!(part_index(anchor, &path), Some(index));
    }
    for name in [
        "x.parquet.part-1.parquet",
        "x.parquet.part-00000000000000000000.parquet",
    ] {
        assert!(split_part_path(Path::new(name)).is_none());
    }
}

#[test]
fn truncated_or_extra_input_is_refused() {
    assert!(encode_parts(&mut Cursor::new([1, 2]), 3, |_, _| Ok(())).is_err());
    assert!(encode_parts(&mut Cursor::new([1, 2]), 1, |_, _| Ok(())).is_err());
    assert!(encode_parts(&mut Cursor::new([]), 0, |_, _| Ok(())).is_err());
}

#[test]
fn swapped_missing_and_foreign_parts_are_refused_on_touch() {
    let logical = payload(PROPERTY_OBJECT_PAYLOAD_BYTES + 1);
    let encoded = parts(&logical);
    let (layout, _) = inspect_envelope(encoded[0].clone()).unwrap().unwrap();
    let swapped = encoded[1].clone();
    let source = SegmentedSource::new(layout, Arc::new(move |_| Ok(swapped.clone()))).unwrap();
    assert!(source.get_bytes(0, 1).is_err());
    let source = SegmentedSource::new(layout, Arc::new(|_| Err(invalid("missing part")))).unwrap();
    assert!(source.get_bytes(0, 1).is_err());
    let foreign = parts(&[7; 23])[0].clone();
    let source = SegmentedSource::new(layout, Arc::new(move |_| Ok(foreign.clone()))).unwrap();
    assert!(source.get_bytes(0, 1).is_err());
}

#[test]
fn invalid_versions_layouts_and_overflowing_ranges_are_refused() {
    let schema = Schema::empty().with_metadata(
        [(FORMAT_KEY.to_owned(), "2".to_owned())]
            .into_iter()
            .collect(),
    );
    assert!(inspect_schema(&schema).is_err());
    let bad = EnvelopeLayout {
        logical_length: u64::MAX,
        part_count: 1,
    };
    assert!(bad.validate().is_err());
    let source = source(parts(&[1, 2, 3]));
    assert!(source.get_bytes(u64::MAX, 1).is_err());
    assert!(source.get_bytes(2, 2).is_err());
    assert!(source.get_read(4).is_err());
    assert_eq!(source.read_at(&mut [0; 1], u64::MAX).unwrap(), 0);
    assert_eq!(source.get_bytes(3, 0).unwrap().len(), 0);
}
