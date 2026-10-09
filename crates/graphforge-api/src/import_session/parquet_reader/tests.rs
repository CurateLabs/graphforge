use std::error::Error;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::{ApiErrorCode, GfError, ProjectErrorCode};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::column::page::{Page, PageReader};
use parquet::file::properties::{WriterProperties, WriterVersion};

use crate::CancellationToken;

use super::{OwnedPageReader, PageFailures, PagePreflight};
use crate::import_session::parquet_page_decode::DecodedPage;

struct Preflight {
    workspace: usize,
    validated_pages: usize,
    page_bodies: Vec<Vec<u8>>,
    data_events: u64,
    dictionaries: usize,
    finishes: usize,
    refuse: bool,
}

impl Preflight {
    fn new(workspace: usize) -> Self {
        Self {
            workspace,
            validated_pages: 0,
            page_bodies: Vec::new(),
            data_events: 0,
            dictionaries: 0,
            finishes: 0,
            refuse: false,
        }
    }
}

impl PagePreflight for Preflight {
    fn remaining_workspace(&self) -> Result<usize, GfError> {
        Ok(self.workspace)
    }

    fn validate(&mut self, decoded: &DecodedPage) -> Result<(), GfError> {
        self.validated_pages += 1;
        self.page_bodies.push(decoded.page.buffer().to_vec());
        match &decoded.page {
            Page::DictionaryPage { .. } => self.dictionaries += 1,
            Page::DataPage { num_values, .. } | Page::DataPageV2 { num_values, .. } => {
                self.data_events += u64::from(*num_values);
            }
        }
        if self.refuse {
            return Err(GfError::Storage("preflight refused page".into()));
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), GfError> {
        self.finishes += 1;
        Ok(())
    }
}

struct BoxedProbe(Arc<AtomicUsize>);

impl PagePreflight for BoxedProbe {
    fn remaining_workspace(&self) -> Result<usize, GfError> {
        Ok(64 << 20)
    }

    fn validate(&mut self, _: &DecodedPage) -> Result<(), GfError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn finish(&mut self) -> Result<(), GfError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct Fixture {
    bytes: Vec<u8>,
    start: usize,
    length: usize,
    compression: Compression,
    events: u64,
}

impl Fixture {
    fn numeric(compression: Compression) -> Self {
        Self::numeric_with_version(compression, WriterVersion::PARQUET_1_0)
    }

    fn numeric_with_version(compression: Compression, version: WriterVersion) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let values = Int32Array::from_iter_values(0..4096);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)]).unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        let properties = WriterProperties::builder()
            .set_compression(compression)
            .set_writer_version(version)
            .build();
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), schema, Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let bytes = std::fs::read(file.path()).unwrap();
        let metadata = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
            .unwrap()
            .metadata()
            .clone();
        let column = metadata.row_group(0).column(0);
        let (start, length) = column.byte_range();
        Self {
            bytes,
            start: usize::try_from(start).unwrap(),
            length: usize::try_from(length).unwrap(),
            compression: column.compression(),
            events: u64::try_from(column.num_values()).unwrap(),
        }
    }

    fn reader(&self, preflight: Preflight) -> OwnedPageReader<Cursor<Vec<u8>>, Preflight> {
        let end = self.start + self.length;
        OwnedPageReader::new(
            Cursor::new(self.bytes[self.start..end].to_vec()),
            u64::try_from(self.length).unwrap(),
            self.compression,
            self.events,
            None,
            PageFailures::new(),
            preflight,
        )
    }
}

#[test]
fn peek_preserves_v1_unknown_rows_and_v2_declared_rows() {
    let v1 = Fixture::numeric(Compression::UNCOMPRESSED);
    let mut v1_reader = v1.reader(Preflight::new(64 << 20));
    let first_v1 = v1_reader.peek_next_page().unwrap().unwrap();
    assert!(first_v1.is_dict || first_v1.num_rows.is_none());
    while let Some(page) = v1_reader.next() {
        page.unwrap();
        if let Some(metadata) = v1_reader.peek_next_page().unwrap() {
            if !metadata.is_dict {
                assert_eq!(metadata.num_rows, None);
                break;
            }
        }
    }

    let v2 = Fixture::numeric_with_version(Compression::UNCOMPRESSED, WriterVersion::PARQUET_2_0);
    let mut v2_reader = v2.reader(Preflight::new(64 << 20));
    while let Some(page) = v2_reader.next() {
        page.unwrap();
        if let Some(metadata) = v2_reader.peek_next_page().unwrap() {
            if !metadata.is_dict {
                assert_eq!(metadata.num_rows, Some(usize::try_from(v2.events).unwrap()));
                break;
            }
        }
    }
}

fn external_gf_error(error: &parquet::errors::ParquetError) -> &GfError {
    error
        .source()
        .and_then(|source| source.downcast_ref::<GfError>())
        .expect("owned reader preserves the original typed GraphForge error")
}

#[test]
fn real_arrow_written_column_chunks_pass_through_owned_preflight_for_each_codec() {
    for compression in [
        Compression::UNCOMPRESSED,
        Compression::SNAPPY,
        Compression::GZIP(Default::default()),
        Compression::BROTLI(Default::default()),
        Compression::ZSTD(Default::default()),
        Compression::LZ4,
        Compression::LZ4_RAW,
    ] {
        let fixture = Fixture::numeric(compression);
        let mut reader = fixture.reader(Preflight::new(64 << 20));
        let mut returned_events = 0_u64;
        let mut page_index = 0;
        while let Some(page) = reader.next() {
            let page = page.unwrap();
            assert_eq!(
                &page.buffer()[..],
                &reader.preflight.page_bodies[page_index][..],
                "returned page must be the same owned body validated by preflight"
            );
            page_index += 1;
            if page.is_data_page() {
                returned_events += u64::from(page.num_values());
            }
        }
        assert_eq!(returned_events, fixture.events, "{compression:?}");
        assert_eq!(
            reader.preflight.data_events, fixture.events,
            "{compression:?}"
        );
        assert_eq!(
            reader.preflight.validated_pages,
            reader.preflight.dictionaries + 1
        );
        assert_eq!(reader.preflight.finishes, 1);
    }
}

#[test]
fn adapter_accepts_a_boxed_shared_preflight_callback() {
    let fixture = Fixture::numeric(Compression::SNAPPY);
    let end = fixture.start + fixture.length;
    let calls = Arc::new(AtomicUsize::new(0));
    let preflight: Box<dyn PagePreflight> = Box::new(BoxedProbe(Arc::clone(&calls)));
    let mut reader: OwnedPageReader<Cursor<Vec<u8>>, Box<dyn PagePreflight>> = OwnedPageReader::new(
        Cursor::new(fixture.bytes[fixture.start..end].to_vec()),
        u64::try_from(fixture.length).unwrap(),
        fixture.compression,
        fixture.events,
        None,
        PageFailures::new(),
        preflight,
    );
    let mut events = 0_u64;
    for page in reader.by_ref() {
        let page = page.unwrap();
        if page.is_data_page() {
            events += u64::from(page.num_values());
        }
    }
    assert_eq!(events, fixture.events);
    assert!(calls.load(Ordering::Relaxed) >= 2);
}

#[test]
fn peek_caches_only_the_header_until_get_reads_and_validates_the_page() {
    let fixture = Fixture::numeric(Compression::SNAPPY);
    let mut reader = fixture.reader(Preflight::new(64 << 20));
    let metadata = reader.peek_next_page().unwrap().unwrap();
    let position = reader.reader.position();
    assert!(position > 0);
    assert!(
        position < u64::try_from(fixture.length).unwrap(),
        "peek read a page body"
    );
    assert_eq!(
        reader.peek_next_page().unwrap().unwrap().is_dict,
        metadata.is_dict
    );
    assert_eq!(
        reader.reader.position(),
        position,
        "cached peek reread bytes"
    );
    assert_eq!(reader.preflight.validated_pages, 0);

    let page = reader.get_next_page().unwrap().unwrap();
    assert_eq!(page.is_dictionary_page(), metadata.is_dict);
    assert_eq!(reader.preflight.validated_pages, 1);
}

#[test]
fn skip_reads_and_validates_instead_of_bypassing_preflight() {
    let fixture = Fixture::numeric(Compression::SNAPPY);
    let mut reader = fixture.reader(Preflight::new(64 << 20));
    reader.skip_next_page().unwrap();
    assert_eq!(reader.preflight.validated_pages, 1);
    for page in reader.by_ref() {
        page.unwrap();
    }
    assert_eq!(reader.preflight.data_events, fixture.events);
    assert_eq!(reader.preflight.finishes, 1);
}

#[test]
fn callback_refusal_is_typed_terminal_and_never_retried_or_dispatched() {
    let fixture = Fixture::numeric(Compression::SNAPPY);
    let mut preflight = Preflight::new(64 << 20);
    preflight.refuse = true;
    let mut reader = fixture.reader(preflight);
    let error = reader.next().unwrap().unwrap_err();
    assert!(matches!(external_gf_error(&error), GfError::Storage(_)));
    assert_eq!(reader.preflight.validated_pages, 1);
    assert!(reader.next().is_none());
    assert_eq!(reader.preflight.validated_pages, 1);
    assert_eq!(reader.preflight.finishes, 0);
}

#[test]
fn output_limit_is_checked_after_peek_but_before_body_read() {
    let fixture = Fixture::numeric(Compression::SNAPPY);
    let mut reader = fixture.reader(Preflight::new(0));
    reader.peek_next_page().unwrap().unwrap();
    let header_end = reader.reader.position();
    let error = reader.get_next_page().unwrap_err();
    assert!(matches!(
        external_gf_error(&error),
        GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        }
    ));
    assert_eq!(reader.reader.position(), header_end);
    assert_eq!(reader.preflight.validated_pages, 0);
    assert!(reader.next().is_none());
}

#[test]
fn wrong_codec_and_cancelled_reads_fail_before_callback_or_retry() {
    let fixture = Fixture::numeric(Compression::SNAPPY);
    let end = fixture.start + fixture.length;
    let wrong_codec = if fixture.compression == Compression::GZIP(Default::default()) {
        Compression::SNAPPY
    } else {
        Compression::GZIP(Default::default())
    };
    let mut reader = OwnedPageReader::new(
        Cursor::new(fixture.bytes[fixture.start..end].to_vec()),
        u64::try_from(fixture.length).unwrap(),
        wrong_codec,
        fixture.events,
        None,
        PageFailures::new(),
        Preflight::new(64 << 20),
    );
    let error = reader.next().unwrap().unwrap_err();
    assert!(matches!(external_gf_error(&error), GfError::Storage(_)));
    assert_eq!(reader.preflight.validated_pages, 0);
    assert!(reader.next().is_none());

    let cancelled = CancellationToken::new();
    let mut reader = OwnedPageReader::new(
        Cursor::new(fixture.bytes[fixture.start..end].to_vec()),
        u64::try_from(fixture.length).unwrap(),
        fixture.compression,
        fixture.events,
        Some(cancelled),
        PageFailures::new(),
        Preflight::new(64 << 20),
    );
    reader.peek_next_page().unwrap().unwrap();
    let header_end = reader.reader.position();
    // Cancel after the header is cached but before any body allocation/read.
    reader.cancellation.as_ref().unwrap().cancel();
    let error = reader.next().unwrap().unwrap_err();
    assert!(matches!(
        external_gf_error(&error),
        GfError::Api {
            code: ApiErrorCode::Cancelled,
            ..
        }
    ));
    assert_eq!(reader.reader.position(), header_end);
    assert_eq!(reader.preflight.validated_pages, 0);
    assert!(reader.next().is_none());
}

#[test]
fn footer_event_mismatch_is_reported_once_at_end_of_chunk() {
    let fixture = Fixture::numeric(Compression::SNAPPY);
    let mut reader = fixture.reader(Preflight::new(64 << 20));
    reader.expected_data_events += 1;
    let mut error = None;
    while let Some(page) = reader.next() {
        if let Err(found) = page {
            error = Some(found);
            break;
        }
    }
    let error = error.expect("footer mismatch must be emitted at chunk EOF");
    assert!(matches!(external_gf_error(&error), GfError::Storage(_)));
    assert_eq!(reader.preflight.finishes, 0);
    assert!(reader.next().is_none());
}

#[test]
fn data_pages_that_exceed_footer_count_are_refused_before_callback() {
    let fixture = Fixture::numeric(Compression::SNAPPY);
    let mut reader = fixture.reader(Preflight::new(64 << 20));
    reader.expected_data_events -= 1;
    let mut error = None;
    while let Some(page) = reader.next() {
        if let Err(found) = page {
            error = Some(found);
            break;
        }
    }
    let error = error.expect("footer overrun must be rejected before page dispatch");
    assert!(matches!(external_gf_error(&error), GfError::Storage(_)));
    assert_eq!(reader.preflight.data_events, 0);
    assert!(reader.next().is_none());
}

fn varint(mut value: u64, output: &mut Vec<u8>) {
    loop {
        let byte = u8::try_from(value & 0x7f).unwrap();
        value >>= 7;
        output.push(byte | if value == 0 { 0 } else { 0x80 });
        if value == 0 {
            break;
        }
    }
}

#[allow(clippy::cast_possible_wrap)] // Zigzag encoding uses the signed sign mask.
fn compact_int(value: i64, output: &mut Vec<u8>) {
    let encoded = ((value as u64) << 1) ^ ((value >> 63) as u64);
    varint(encoded, output);
}

fn field(output: &mut Vec<u8>, previous: &mut i16, id: i16, kind: u8) {
    let delta = id - *previous;
    if (1..=15).contains(&delta) {
        output.push((u8::try_from(delta).unwrap() << 4) | kind);
    } else {
        output.push(kind);
        compact_int(i64::from(id), output);
    }
    *previous = id;
}

fn page_with_crc(body: &[u8], expected_crc: u32) -> Vec<u8> {
    const I32: u8 = 5;
    const STRUCT: u8 = 12;
    let mut output = Vec::new();
    let mut previous = 0;
    field(&mut output, &mut previous, 1, I32);
    compact_int(0, &mut output); // DATA_PAGE
    field(&mut output, &mut previous, 2, I32);
    compact_int(i64::try_from(body.len()).unwrap(), &mut output);
    field(&mut output, &mut previous, 3, I32);
    compact_int(i64::try_from(body.len()).unwrap(), &mut output);
    field(&mut output, &mut previous, 4, I32);
    compact_int(
        i64::from(i32::from_ne_bytes(expected_crc.to_ne_bytes())),
        &mut output,
    );
    field(&mut output, &mut previous, 5, STRUCT);
    let mut data_previous = 0;
    field(&mut output, &mut data_previous, 1, I32);
    compact_int(1, &mut output);
    field(&mut output, &mut data_previous, 2, I32);
    compact_int(0, &mut output); // PLAIN
    field(&mut output, &mut data_previous, 3, I32);
    compact_int(3, &mut output); // RLE definition
    field(&mut output, &mut data_previous, 4, I32);
    compact_int(3, &mut output); // RLE repetition
    output.push(0); // end DataPageHeader
    output.push(0); // end PageHeader
    output.extend_from_slice(body);
    output
}

fn index_page(body: &[u8]) -> Vec<u8> {
    const I32: u8 = 5;
    let mut output = Vec::new();
    let mut previous = 0;
    field(&mut output, &mut previous, 1, I32);
    compact_int(1, &mut output); // INDEX_PAGE
    field(&mut output, &mut previous, 2, I32);
    compact_int(i64::try_from(body.len()).unwrap(), &mut output);
    field(&mut output, &mut previous, 3, I32);
    compact_int(i64::try_from(body.len()).unwrap(), &mut output);
    field(&mut output, &mut previous, 4, I32);
    compact_int(
        i64::from(i32::from_ne_bytes(crc32fast::hash(body).to_ne_bytes())),
        &mut output,
    );
    output.push(0);
    output.extend_from_slice(body);
    output
}

#[test]
fn index_pages_are_crc_checked_in_bounded_chunks_and_not_dispatched() {
    let index_body = vec![0x5a; 20_000];
    let mut bytes = index_page(&index_body);
    let data_body = [1_u8, 2, 3, 4];
    bytes.extend_from_slice(&page_with_crc(&data_body, crc32fast::hash(&data_body)));
    let mut reader = OwnedPageReader::new(
        Cursor::new(bytes.clone()),
        u64::try_from(bytes.len()).unwrap(),
        Compression::UNCOMPRESSED,
        1,
        None,
        PageFailures::new(),
        Preflight::new(1024),
    );
    let page = reader.next().unwrap().unwrap();
    assert!(page.is_data_page());
    assert_eq!(reader.preflight.validated_pages, 1);
    assert_eq!(reader.preflight.data_events, 1);
    assert!(reader.next().is_none());
    assert_eq!(reader.preflight.finishes, 1);
}

#[test]
fn checksum_failure_on_an_owned_body_never_reaches_preflight() {
    let body = [1_u8, 2, 3, 4];
    let bytes = page_with_crc(&body, crc32fast::hash(&body) ^ 1);
    let mut reader = OwnedPageReader::new(
        Cursor::new(bytes.clone()),
        u64::try_from(bytes.len()).unwrap(),
        Compression::UNCOMPRESSED,
        1,
        None,
        PageFailures::new(),
        Preflight::new(1024),
    );
    let error = reader.next().unwrap().unwrap_err();
    assert!(matches!(external_gf_error(&error), GfError::Storage(_)));
    assert_eq!(reader.preflight.validated_pages, 0);
    assert!(reader.next().is_none());
}
