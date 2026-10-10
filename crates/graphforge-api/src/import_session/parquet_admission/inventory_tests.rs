//! The page inventory a source scan builds is bounded by the caller's
//! capacity before it is allocated (#1918).
//!
//! A column chunk of many tiny pages — a hostile file of zero-value page
//! headers, or an ordinary writer's one-row pages — must be refused, as a typed
//! resource limit, when the scan's workspace cannot hold the facts those pages
//! produce, and a page that overruns the footer's value count must be refused
//! the moment it arrives rather than after the whole chunk is inventoried.
//! Columns and row groups share one budget, so a file cannot multiply its way
//! past it, and an ordinary file with many small pages is admitted whole when
//! the budget is adequate.
//!
//! The admission itself is proved deterministic by the helper test: a vector
//! grown through the budget can never hold more of the inventory than the
//! budget covers, so a scan's allocations for it are bounded by the workspace
//! however many pages the file claims.

use std::fs::File;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use graphforge_core::{GfError, ProjectErrorCode};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::basic::{Encoding, Type as PhysicalType};
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData,
};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::{
    ColumnDescPtr, ColumnDescriptor, ColumnPath, SchemaDescPtr, SchemaDescriptor, Type,
};

use super::SourceScan;
use crate::import_session::inventory_budget::{InventoryBudget, reserve};
use crate::import_session::parquet_scan::{PageFact, PageKind, scan_chunk};

const KIB: u64 = 1 << 10;
const MIB: u64 = 1 << 20;

fn is_resource_limit(error: &GfError) -> bool {
    matches!(
        error,
        GfError::Project {
            code: ProjectErrorCode::ResourceLimit,
            ..
        }
    )
}

/// Unwraps a scan that must have been refused by the inventory budget.
fn expect_limit(result: Result<SourceScan, GfError>, context: &str) -> GfError {
    match result {
        Ok(_) => panic!("{context}: expected a resource limit"),
        Err(error) if is_resource_limit(&error) => error,
        Err(error) => panic!("{context}: expected a resource limit, got {error}"),
    }
}

/// Unwraps a scan that must have been refused, for whatever the checks say.
fn expect_refused(result: Result<SourceScan, GfError>, context: &str) -> GfError {
    match result {
        Ok(_) => panic!("{context}"),
        Err(error) => error,
    }
}

/// One thrift compact-protocol integer, high bit continuing the value.
fn varint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let byte = u8::try_from(value & 0x7f).expect("seven bits fit a byte");
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// A signed thrift compact-protocol integer, zigzag-coded.
fn zig(value: i64, out: &mut Vec<u8>) {
    let encoded = value.wrapping_shl(1) ^ value.wrapping_shr(63);
    varint(u64::try_from(encoded).unwrap_or(0), out);
}

fn field(id: i16, previous: &mut i16, kind: u8, out: &mut Vec<u8>) {
    let delta = id - *previous;
    *previous = id;
    match u8::try_from(delta) {
        Ok(delta) if delta <= 15 => out.push((delta << 4) | kind),
        _ => {
            out.push(kind);
            zig(i64::from(id), out);
        }
    }
}

fn integer(id: i16, previous: &mut i16, value: i64, out: &mut Vec<u8>) {
    field(id, previous, 5, out);
    zig(value, out);
}

/// A `DATA_PAGE_V2` header whose body is empty: `values` rows, none of them
/// holding a byte, self-consistent with the chunk that states them.
fn zero_body_v2_header(values: u32, out: &mut Vec<u8>) {
    let mut previous = 0_i16;
    integer(1, &mut previous, 3, out); // type: DATA_PAGE_V2
    integer(2, &mut previous, 0, out); // uncompressed_page_size
    integer(3, &mut previous, 0, out); // compressed_page_size
    field(8, &mut previous, 12, out); // data_page_header_v2
    let mut page = 0_i16;
    integer(1, &mut page, i64::from(values), out); // num_values
    integer(2, &mut page, 0, out); // num_nulls
    integer(3, &mut page, i64::from(values), out); // num_rows
    integer(4, &mut page, 0, out); // encoding: PLAIN
    integer(5, &mut page, 0, out); // definition_levels_byte_length
    integer(6, &mut page, 0, out); // repetition_levels_byte_length
    field(7, &mut page, 1, out); // is_compressed: true
    out.push(0x00); // end of the page header struct
    out.push(0x00); // end of the PageHeader struct
}

fn leaf(name: &str, physical: PhysicalType) -> ColumnDescPtr {
    Arc::new(ColumnDescriptor::new(
        Arc::new(
            Type::primitive_type_builder(name, physical)
                .build()
                .expect("a primitive type"),
        ),
        0,
        0,
        ColumnPath::new(vec![name.to_owned()]),
    ))
}

fn schema(leaves: &[ColumnDescPtr]) -> SchemaDescPtr {
    let fields = leaves
        .iter()
        .map(|leaf| {
            Arc::new(
                Type::primitive_type_builder("c", leaf.physical_type())
                    .build()
                    .expect("a primitive type"),
            ) as parquet::schema::types::TypePtr
        })
        .collect::<Vec<_>>();
    let group = Type::group_type_builder("schema")
        .with_fields(fields)
        .build()
        .expect("a schema");
    Arc::new(SchemaDescriptor::new(Arc::new(group)))
}

/// A chunk of `length` bytes of empty pages at offset 0, saying what the
/// pages' headers say about their value count.
fn chunk(descr: ColumnDescPtr, length: u64, num_values: i64) -> ColumnChunkMetaData {
    ColumnChunkMetaData::builder(descr)
        .set_encodings(vec![Encoding::PLAIN])
        .set_num_values(num_values)
        .set_data_page_offset(0)
        .set_total_compressed_size(i64::try_from(length).expect("a file length"))
        .set_total_uncompressed_size(0)
        .build()
        .expect("a column chunk")
}

fn row_group(
    schema_descr: &SchemaDescPtr,
    columns: Vec<ColumnChunkMetaData>,
    rows: i64,
) -> RowGroupMetaData {
    let mut builder = RowGroupMetaData::builder(Arc::clone(schema_descr))
        .set_num_rows(rows)
        .set_total_byte_size(0);
    for column in columns {
        builder = builder.add_column_metadata(column);
    }
    builder.build().expect("a row group")
}

fn file_metadata(
    schema_descr: &SchemaDescPtr,
    groups: Vec<RowGroupMetaData>,
    rows: i64,
) -> ParquetMetaData {
    ParquetMetaData::new(
        FileMetaData::new(1, rows, None, None, Arc::clone(schema_descr), None),
        groups,
    )
}

fn write_pages(bytes: &[u8]) -> tempfile::NamedTempFile {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), bytes).unwrap();
    file
}

fn build(
    file: &tempfile::NamedTempFile,
    metadata: &ParquetMetaData,
    capacity: u64,
) -> Result<SourceScan, GfError> {
    let metadata =
        ArrowReaderMetadata::try_new(Arc::new(metadata.clone()), ArrowReaderOptions::new())
            .unwrap();
    SourceScan::build(
        &File::open(file.path()).unwrap(),
        &metadata,
        1,
        capacity,
        u64::MAX,
        None,
    )
}

/// A flood of zero-value page headers, each self-consistent with a footer that
/// states no values: nothing but the page facts themselves is large.
fn zero_value_pages(count: usize) -> (tempfile::NamedTempFile, ParquetMetaData) {
    let mut bytes = Vec::new();
    for _ in 0..count {
        zero_body_v2_header(0, &mut bytes);
    }
    let descr = leaf("c", PhysicalType::BYTE_ARRAY);
    let schema_descr = schema(std::slice::from_ref(&descr));
    let chunk = chunk(Arc::clone(&descr), u64::try_from(bytes.len()).unwrap(), 0);
    let metadata = file_metadata(
        &schema_descr,
        vec![row_group(&schema_descr, vec![chunk], 0)],
        0,
    );
    (write_pages(&bytes), metadata)
}

#[test]
fn many_zero_value_pages_are_refused_before_their_inventory_is_allocated() {
    const PAGES: usize = 200_000;
    let (file, metadata) = zero_value_pages(PAGES);
    let fact_bytes = u64::try_from(std::mem::size_of::<PageFact>()).unwrap();

    // An adequate budget admits the whole inventory, unfabricated: every page
    // is there, and the plan keeps what it read. The budget must also cover the
    // last reallocation's old and new buffer, held together, so it is twice the
    // inventory plus working room.
    let scan = build(&file, &metadata, 16 * MIB).expect("an adequate budget admits the flood");
    assert!(scan.resident_bytes() >= u64::try_from(PAGES).unwrap() * fact_bytes);
    let mut budget = InventoryBudget::new(u64::MAX);
    let pages = scan_chunk(
        &mut File::open(file.path()).unwrap(),
        metadata.row_group(0).column(0),
        &mut budget,
    )
    .unwrap();
    assert_eq!(pages.len(), PAGES);
    assert!(
        pages
            .iter()
            .all(|page| page.kind == PageKind::Data && page.values == 0)
    );

    // A workspace far smaller than the inventory refuses the scan. Every byte
    // of it is admitted before it is reserved, so no vector of 200,000 page
    // facts — over 5 MB against a 64 KiB workspace — ever comes to exist; the
    // helper test below proves that bound is deterministic.
    expect_limit(
        build(&file, &metadata, 64 * KIB),
        "a tiny workspace must refuse the flood",
    );
}

#[test]
fn the_inventory_helper_admits_only_what_the_budget_covers() {
    // A vector of page facts grown through a 4 KiB budget stops at exactly the
    // capacity that fits it: the next doubling would be refused before the
    // allocator is asked, so the allocated bytes never exceed the budget.
    const BUDGET: u64 = 4 * KIB;
    let element = u64::try_from(std::mem::size_of::<PageFact>()).unwrap();
    let mut budget = InventoryBudget::new(BUDGET);
    let mut pages: Vec<PageFact> = Vec::new();
    loop {
        if reserve(&mut pages, 1, &mut budget, "test facts").is_err() {
            break;
        }
        pages.push(PageFact {
            kind: PageKind::Data,
            compressed: 0,
            uncompressed: 0,
            values: 0,
            rows: Some(0),
            encoding: 0,
        });
    }
    let capacity_bytes = u64::try_from(pages.capacity()).unwrap() * element;
    assert!(
        capacity_bytes <= BUDGET,
        "the vector grew to {capacity_bytes} bytes inside a {BUDGET}-byte budget"
    );
    let held = u64::try_from(pages.len()).unwrap();
    assert_eq!(
        u64::try_from(pages.capacity()).unwrap(),
        held,
        "capacity is exactly what the budget admitted"
    );
    // One more fact is refused because doubling it would hold the old and the
    // new buffer together: that transient is what the budget declined.
    assert!(
        held * 3 * element > BUDGET,
        "another doubling, {} bytes live at once, should not fit the budget",
        held * 3 * element
    );
}

#[test]
fn pages_past_the_footer_value_count_are_refused_when_they_arrive() {
    // One page of 100 values against a footer that states 10, then a header
    // that stops mid-integer: the value count is refused before that header is
    // read, so the error says the counts disagree and not that it is truncated.
    let mut bytes = Vec::new();
    zero_body_v2_header(100, &mut bytes);
    bytes.push(0x15);
    let descr = leaf("c", PhysicalType::INT32);
    let schema_descr = schema(std::slice::from_ref(&descr));
    let chunk = chunk(Arc::clone(&descr), u64::try_from(bytes.len()).unwrap(), 10);
    let metadata = file_metadata(
        &schema_descr,
        vec![row_group(&schema_descr, vec![chunk], 0)],
        0,
    );
    let file = write_pages(&bytes);
    let error = expect_refused(
        build(&file, &metadata, 8 * MIB),
        "pages overrun the footer's value count",
    )
    .to_string();
    assert!(
        error.contains("disagree with the footer's value count"),
        "{error}"
    );
    assert!(
        !error.contains("truncated"),
        "the overrun page was not refused when it arrived: {error}"
    );
}

#[test]
fn columns_and_row_groups_share_one_inventory_budget() {
    // Four chunks of 256 zero-value byte-array pages each: one of them fits in
    // a 24 KiB workspace, so all four do only if they share the budget instead
    // of each drawing it in full.
    const PAGES: usize = 256;
    let mut bytes = Vec::new();
    for _ in 0..PAGES {
        zero_body_v2_header(0, &mut bytes);
    }
    let chunk_bytes = u64::try_from(bytes.len()).unwrap();
    let tiny: u64 = 24 * KIB;
    let leaves = [
        leaf("a", PhysicalType::BYTE_ARRAY),
        leaf("b", PhysicalType::BYTE_ARRAY),
    ];

    let one = {
        let schema_descr = schema(&leaves[..1]);
        let chunk = chunk(Arc::clone(&leaves[0]), chunk_bytes, 0);
        file_metadata(
            &schema_descr,
            vec![row_group(&schema_descr, vec![chunk], 0)],
            0,
        )
    };
    let one_file = write_pages(&bytes);
    build_from_path(one_file.path(), &one, tiny)
        .expect("one chunk's inventory fits the tiny workspace");

    let four = {
        let schema_descr = schema(&leaves);
        let groups = [0, 1]
            .iter()
            .map(|_| {
                let columns = leaves
                    .iter()
                    .map(|leaf| chunk(Arc::clone(leaf), chunk_bytes, 0))
                    .collect::<Vec<_>>();
                row_group(&schema_descr, columns, 0)
            })
            .collect::<Vec<_>>();
        file_metadata(&schema_descr, groups, 0)
    };
    let four_file = write_pages(&bytes);
    expect_limit(
        build_from_path(four_file.path(), &four, tiny),
        "four chunks cannot each draw the full workspace",
    );

    // The same file on an adequate budget keeps all four inventories whole.
    let scan =
        build_from_path(four_file.path(), &four, MIB).expect("an adequate budget admits four");
    assert_eq!(scan.groups.len(), 2);
    for group in &scan.groups {
        assert_eq!(group.leaves.len(), 2);
        for leaf_scan in &group.leaves {
            let pages = leaf_scan.pages.as_ref().expect("byte arrays keep pages");
            assert_eq!(pages.len(), PAGES);
        }
    }
}

fn build_from_path(
    path: &std::path::Path,
    metadata: &ParquetMetaData,
    capacity: u64,
) -> Result<SourceScan, GfError> {
    let metadata =
        ArrowReaderMetadata::try_new(Arc::new(metadata.clone()), ArrowReaderOptions::new())
            .unwrap();
    SourceScan::build(
        &File::open(path).unwrap(),
        &metadata,
        1,
        capacity,
        u64::MAX,
        None,
    )
}

#[test]
fn short_cached_row_group_columns_return_a_typed_error() {
    let leaves = [
        leaf("before", PhysicalType::INT32),
        leaf("middle", PhysicalType::INT32),
        leaf("after", PhysicalType::INT32),
    ];
    let file_schema = schema(&leaves);
    // A cached public row group can be valid against its own descriptor while
    // disagreeing with the enclosing file's descriptor. The native footer
    // parser rejects this mismatch; the public cached-metadata entry also
    // needs to refuse it without an indexing panic.
    let group_schema = schema(&leaves[..2]);
    let columns = leaves[..2]
        .iter()
        .map(|leaf| chunk(Arc::clone(leaf), 0, 0))
        .collect();
    let metadata = file_metadata(&file_schema, vec![row_group(&group_schema, columns, 0)], 0);
    let file = write_pages(&[]);
    let error = expect_refused(
        build(&file, &metadata, MIB),
        "short cached column inventory",
    );
    assert!(matches!(error, GfError::Storage(_)), "{error}");
}

#[test]
fn writer_generated_small_pages_pass_with_exact_counts() {
    // An ordinary writer told to flush a page per row: 3,000 one-value pages
    // across three row groups. A scan with an adequate budget reads them all and
    // states exactly what they held.
    let rows = 3_000_usize;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(
                (0..i64::try_from(rows).unwrap()).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                (0..rows).map(|row| format!("v{row}")).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1_000))
        .set_data_page_row_count_limit(1)
        .set_write_batch_size(1)
        .set_dictionary_enabled(false)
        .build();
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let metadata = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
        .unwrap()
        .metadata()
        .clone();
    let metadata = ArrowReaderMetadata::try_new(
        Arc::new(metadata.as_ref().clone()),
        ArrowReaderOptions::new(),
    )
    .unwrap();

    let scan = SourceScan::build(
        &File::open(file.path()).unwrap(),
        &metadata,
        500,
        4 * MIB,
        u64::MAX,
        None,
    )
    .expect("an ordinary many-page file is admitted");
    assert_eq!(scan.groups.len(), 3);
    let retained = scan.groups.capacity()
        * std::mem::size_of::<super::super::parquet_scan::GroupScan>()
        + scan.group_start.capacity() * std::mem::size_of::<u64>()
        + scan.value_bytes.capacity() * std::mem::size_of::<u64>()
        + scan.batch_max_row_value_bytes.capacity() * std::mem::size_of::<u64>()
        + scan.batch_growing_bytes.capacity() * std::mem::size_of::<u64>()
        + scan.leaf_page_max.capacity() * std::mem::size_of::<u64>()
        + scan.first_oversized_row.capacity() * std::mem::size_of::<u64>()
        + scan
            .groups
            .iter()
            .map(|group| {
                group.leaves.capacity()
                    * std::mem::size_of::<super::super::parquet_scan::LeafScan>()
                    + group
                        .leaves
                        .iter()
                        .filter_map(|leaf| leaf.pages.as_ref())
                        .map(|pages| pages.capacity() * std::mem::size_of::<PageFact>())
                        .sum::<usize>()
            })
            .sum::<usize>();
    assert!(
        scan.groups.capacity() > scan.groups.len(),
        "exercise geometric slack"
    );
    assert_eq!(
        scan.resident_bytes(),
        retained as u64 + scan.shape.inventory_bytes().unwrap(),
        "charge allocated slots, not just occupied slots"
    );

    for group in &scan.groups {
        let pages = group.leaves[1].pages.as_ref().expect("strings keep pages");
        assert_eq!(pages.len(), 1_000, "one page per row");
        assert!(
            pages
                .iter()
                .all(|page| page.kind == PageKind::Data && page.values == 1)
        );
    }
    let mut budget = InventoryBudget::new(u64::MAX);
    let integers = scan_chunk(
        &mut File::open(file.path()).unwrap(),
        metadata.metadata().row_group(0).column(0),
        &mut budget,
    )
    .unwrap();
    assert_eq!(integers.len(), 1_000);
    assert_eq!(
        integers
            .iter()
            .map(|page| u64::from(page.values))
            .sum::<u64>(),
        1_000
    );
}

#[test]
fn inventory_admission_never_saturates_an_overflow_into_success() {
    let mut budget = InventoryBudget::new(u64::MAX);
    budget.admit(u64::MAX, "already charged").unwrap();
    let error = budget.admit(1, "one more byte").unwrap_err();
    assert!(is_resource_limit(&error), "{error}");
    assert_eq!(budget.live_bytes(), u64::MAX);
}
