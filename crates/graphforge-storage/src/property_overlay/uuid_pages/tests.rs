use super::super::*;
use crate::io_stats::{CaptureScope, IoSnapshot};
use crate::permanent_parquet::{
    POINT_ACCESS_PAGE_ROWS, point_access_writer_properties, writer_properties,
};
use crate::uuid_set::UuidMembership;
use arrow::array::{FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use std::collections::{BTreeSet, HashMap};
use tempfile::TempDir;

/// The page row limit as a `u64`, for arithmetic over row indices.
const PAGE: u64 = POINT_ACCESS_PAGE_ROWS as u64;

/// The UUID of row `index`: strictly ascending in `index` and spread over the
/// whole 128-bit space, so the leading byte crosses `0x80` where a signed
/// comparison would misorder. Valid for `index < 100_000`.
fn uuid_of(index: u64) -> [u8; 16] {
    (u128::from(index) * (u128::MAX / 100_000)).to_be_bytes()
}

/// A UUID no row holds, immediately above row `index`.
fn absent_after(index: u64) -> [u8; 16] {
    (u128::from_be_bytes(uuid_of(index)) + 1).to_be_bytes()
}

/// Rows whose index divides 997 are tombstones.
fn is_deleted(index: u64) -> bool {
    index % 997 == 0
}

/// The physical layout a fragment is written in.
#[derive(Debug, Clone, Copy)]
enum Layout {
    /// Pages of 20,000 rows, as projects built before point-access pages.
    Legacy,
    /// Pages of `POINT_ACCESS_PAGE_ROWS` rows.
    PointAccess,
    /// Point-access pages with only row-group statistics: no column index.
    NoColumnIndex,
    /// Point-access pages with neither index. (The writer cannot produce a
    /// column index without an offset index: page statistics override
    /// `set_offset_index_disabled`.)
    NoPageIndex,
}

impl Layout {
    fn properties(self, row_group_rows: usize) -> WriterProperties {
        match self {
            Self::Legacy => writer_properties(),
            Self::PointAccess => point_access_writer_properties(),
            Self::NoColumnIndex => {
                point_access_writer_properties().set_statistics_enabled(EnabledStatistics::Chunk)
            }
            Self::NoPageIndex => point_access_writer_properties()
                .set_statistics_enabled(EnabledStatistics::Chunk)
                .set_offset_index_disabled(true),
        }
        .set_max_row_group_row_count(Some(row_group_rows))
        .build()
    }
}

struct Fixture {
    dir: TempDir,
    entries: Vec<crate::GraphFileEntry>,
    // The project's parent is the property scratch parent, so each fixture
    // gets its own. Dropped after `dir`, as fields drop in declaration order.
    _parent: TempDir,
}

impl Fixture {
    fn new() -> Self {
        let parent = TempDir::new().unwrap();
        let dir = TempDir::new_in(parent.path()).unwrap();
        fs::create_dir_all(dir.path().join("properties/Person")).unwrap();
        Self {
            dir,
            entries: Vec::new(),
            _parent: parent,
        }
    }

    /// Write one fragment holding the rows of `indices` in the given physical
    /// order. A tombstone carries no values; a live row carries
    /// `ident = index` and `name = "n<index>"`.
    fn fragment(&mut self, ordinal: u64, indices: &[u64], layout: Layout, row_group_rows: usize) {
        let id = PropertyFragmentId {
            generation: 0,
            ordinal,
        };
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("node_uuid", DataType::FixedSizeBinary(16), false),
                Field::new(PROPERTY_TOMBSTONE_FIELD, DataType::Boolean, false),
                Field::new("ident", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
            ],
            HashMap::from([
                (
                    PROPERTY_OVERLAY_FORMAT_KEY.into(),
                    PROPERTY_OVERLAY_FORMAT.into(),
                ),
                (PROPERTY_ROUTE_KEY.into(), "Person".into()),
                (PROPERTY_KIND_KEY.into(), "node".into()),
                (PROPERTY_GENERATION_KEY.into(), "0".into()),
                (PROPERTY_ORDINAL_KEY.into(), ordinal.to_string()),
            ]),
        ));
        let uuids = indices
            .iter()
            .map(|index| uuid_of(*index))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(FixedSizeBinaryArray::try_from_iter(uuids.iter()).unwrap()),
                Arc::new(BooleanArray::from(
                    indices
                        .iter()
                        .map(|index| is_deleted(*index))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    indices
                        .iter()
                        .map(|index| (!is_deleted(*index)).then(|| i64::try_from(*index).unwrap()))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    indices
                        .iter()
                        .map(|index| (!is_deleted(*index)).then(|| format!("n{index}")))
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let path = self
            .dir
            .path()
            .join("properties/Person")
            .join(id.file_name());
        let mut writer = ArrowWriter::try_new(
            File::create(&path).unwrap(),
            schema,
            Some(layout.properties(row_group_rows)),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let bytes = fs::read(&path).unwrap();
        self.entries.push(crate::GraphFileEntry {
            content_xxh64: crate::corruption_checksum::checksum(&bytes),
            relative_path: format!("properties/Person/{}", id.file_name()),
            byte_length: u64::try_from(bytes.len()).unwrap(),
            content_sha256: digest_hex(&Sha256::digest(&bytes)),
            role: crate::GraphFileRole::Properties,
        });
    }

    fn inventory(&self) -> AuthenticatedPropertyInventory {
        AuthenticatedPropertyInventory::from_entries_at_root(self.dir.path(), self.entries.clone())
            .unwrap()
    }
}

/// A route of `rows` rows split over `partitions` fragments, each holding the
/// rows whose index is congruent to its partition: every fragment spans the
/// whole UUID range, as the fragments of a route written by parallel
/// partitions do.
fn route(partitions: u64, rows: u64, layout: Layout, row_group_rows: usize) -> Fixture {
    let mut fixture = Fixture::new();
    for partition in 0..partitions {
        let indices = (0..rows)
            .filter(|index| index % partitions == partition)
            .collect::<Vec<_>>();
        fixture.fragment(partition, &indices, layout, row_group_rows);
    }
    fixture
}

fn uuids_of(indices: &[u64]) -> BTreeSet<[u8; 16]> {
    indices.iter().map(|index| uuid_of(*index)).collect()
}

/// One streaming read, restricted to `wanted` when given, with the work its
/// capture saw. The read's own error is returned, not unwrapped.
fn try_read(
    inventory: &AuthenticatedPropertyInventory,
    wanted: Option<&BTreeSet<[u8; 16]>>,
) -> (Result<Vec<PropertySnapshotRow>, GfError>, IoSnapshot) {
    let _capture = CaptureScope::install();
    crate::io_stats::reset();
    let mut rows = Vec::new();
    let result = inventory.visit_route_streaming(
        &RouteRead {
            kind: PropertyRouteKind::Node,
            route: "Person",
            selected_properties: None,
            uuids: wanted.map(|set| set as &dyn UuidMembership),
            limits: PropertyOverlayLimits::default(),
            collect: true,
        },
        |row| {
            rows.push(row);
            Ok(true)
        },
    );
    let io = crate::io_stats::snapshot().expect("requested I/O statistics");
    (result.map(|_| rows), io)
}

fn read(
    inventory: &AuthenticatedPropertyInventory,
    wanted: Option<&BTreeSet<[u8; 16]>>,
) -> (Vec<PropertySnapshotRow>, IoSnapshot) {
    let (rows, io) = try_read(inventory, wanted);
    (rows.unwrap(), io)
}

/// What a read of `wanted` must return: the unfiltered read, narrowed.
fn oracle(
    inventory: &AuthenticatedPropertyInventory,
    wanted: &BTreeSet<[u8; 16]>,
) -> Vec<PropertySnapshotRow> {
    read(inventory, None)
        .0
        .into_iter()
        .filter(|row| wanted.contains(&row.uuid))
        .collect()
}

fn assert_corrupt_unsorted(result: Result<Vec<PropertySnapshotRow>, GfError>) {
    match result {
        Err(GfError::Project {
            code: graphforge_core::ProjectErrorCode::ProjectCorrupt,
            message,
        }) => assert!(
            message.contains("property fragment UUIDs are duplicate or unsorted"),
            "unexpected corruption message: {message}"
        ),
        other => panic!("expected the unsorted-fragment corruption error, got {other:?}"),
    }
}

/// Present rows whose pages differ, absent UUIDs in and between pages, and
/// rows on both sides of the `0x80` leading byte (indices 49,999 to 50,001).
const SCATTERED: [u64; 7] = [5, 2_050, 2_051, 49_999, 50_000, 50_001, 61_999];
const ROUTE_ROWS: u64 = 62_000;

fn scattered_with_absent() -> BTreeSet<[u8; 16]> {
    let mut wanted = uuids_of(&SCATTERED);
    wanted.insert(absent_after(100));
    wanted.insert(absent_after(30_000));
    wanted.insert([0xFF; 16]);
    wanted
}

#[test]
fn wanted_rows_come_back_the_same_from_either_page_layout() {
    for row_group_rows in [1_048_576, 25_000] {
        let wanted = scattered_with_absent();
        let legacy = route(1, ROUTE_ROWS, Layout::Legacy, row_group_rows);
        let point = route(1, ROUTE_ROWS, Layout::PointAccess, row_group_rows);
        let legacy_inventory = legacy.inventory();
        let point_inventory = point.inventory();

        let expected = oracle(&point_inventory, &wanted);
        assert_eq!(
            expected.len(),
            SCATTERED.len(),
            "every present wanted row is live: none of them is a multiple of 997"
        );
        assert_eq!(oracle(&legacy_inventory, &wanted), expected);

        let (legacy_rows, legacy_io) = read(&legacy_inventory, Some(&wanted));
        let (point_rows, point_io) = read(&point_inventory, Some(&wanted));
        assert_eq!(legacy_rows, expected, "{row_group_rows}-row groups, legacy");
        assert_eq!(
            point_rows, expected,
            "{row_group_rows}-row groups, point access"
        );

        for (layout, io) in [("legacy", legacy_io), ("point access", point_io)] {
            assert_eq!(
                io.property_uuid_index_fallbacks, 0,
                "{layout} files carry a page index: no fallback"
            );
            assert_eq!(io.property_uuid_selections, 1, "{layout}");
            assert!(
                io.property_uuid_pages_selected <= io.property_uuid_pages_considered,
                "{layout}: {} of {} pages",
                io.property_uuid_pages_selected,
                io.property_uuid_pages_considered
            );
            assert!(
                io.property_uuid_rows_decoded <= ROUTE_ROWS,
                "{layout}: {} rows decoded of {ROUTE_ROWS}",
                io.property_uuid_rows_decoded
            );
        }
        // The scattered rows touch nearly every one of a legacy file's few
        // 20,480-row pages, so a legacy file narrows almost nothing; a
        // point-access file has many pages and narrows to the few that bracket
        // the wanted rows. In 25,000-row groups only the 4,520-row tail page of
        // group 0 holds no wanted row.
        let legacy_rows = if row_group_rows > 62_000 {
            ROUTE_ROWS
        } else {
            ROUTE_ROWS - 4_520
        };
        assert_eq!(
            legacy_io.property_uuid_rows_decoded, legacy_rows,
            "{row_group_rows}-row groups: legacy pages decode nearly everything"
        );
        assert!(
            point_io.property_uuid_pages_selected < point_io.property_uuid_pages_considered,
            "point access: {} of {} pages",
            point_io.property_uuid_pages_selected,
            point_io.property_uuid_pages_considered
        );
        // Point-access pages narrow the decode to the candidate pages: a
        // 2,048-row page per cluster of wanted rows, where a legacy page is
        // ten times larger.
        let (pages_considered, pages_selected) = if row_group_rows > 62_000 {
            // One row group of 31 pages. Page 0 holds row 5 and the absent
            // UUID above row 100, page 1 rows 2,050 and 2,051, page 14 the
            // absent UUID above row 30,000, page 24 rows 49,999..=50,001 and
            // page 30 row 61,999.
            (31, 5)
        } else {
            // Groups 0..25,000, 25,000..50,000 and 50,000..62,000 have 13, 13
            // and 6 pages. Group 0 pages 0 (row 5, the first absent UUID) and
            // 1 (rows 2,050, 2,051); group 1 pages 2 (the absent UUID above row
            // 30,000) and 12 (row 49,999); group 2 pages 0 (rows 50,000,
            // 50,001) and 5 (row 61,999).
            (32, 6)
        };
        assert_eq!(
            point_io.property_uuid_pages_considered, pages_considered,
            "{row_group_rows}-row groups"
        );
        assert_eq!(
            point_io.property_uuid_pages_selected, pages_selected,
            "{row_group_rows}-row groups"
        );
        assert!(
            point_io.property_uuid_rows_decoded <= pages_selected * PAGE,
            "{} rows decoded for {pages_selected} candidate pages",
            point_io.property_uuid_rows_decoded
        );
        assert!(
            point_io.property_uuid_rows_decoded < legacy_io.property_uuid_rows_decoded,
            "point-access pages decode {} rows, legacy pages {}",
            point_io.property_uuid_rows_decoded,
            legacy_io.property_uuid_rows_decoded
        );
    }
}

#[test]
fn a_small_selection_decodes_its_uuid_column_without_the_page_index() {
    let mut fixture = Fixture::new();
    let indices = (0..3_000).collect::<Vec<u64>>();
    fixture.fragment(0, &indices, Layout::PointAccess, 1_048_576);
    let inventory = fixture.inventory();
    let wanted = uuids_of(&[7, 2_999]);
    let (rows, io) = read(&inventory, Some(&wanted));
    assert_eq!(rows, oracle(&inventory, &wanted));
    assert_eq!(io.property_uuid_selections, 1);
    assert_eq!(io.property_uuid_rows_decoded, 3_000);
    assert_eq!(io.property_uuid_pages_considered, 0);
    assert_eq!(
        io.property_uuid_index_fallbacks, 0,
        "a column below the paging threshold is not a missing index"
    );
}

#[test]
fn a_fragment_without_a_usable_page_index_takes_the_counted_full_decode() {
    let wanted = scattered_with_absent();
    for layout in [Layout::NoColumnIndex, Layout::NoPageIndex] {
        let fixture = route(1, ROUTE_ROWS, layout, 1_048_576);
        let inventory = fixture.inventory();
        let (rows, io) = read(&inventory, Some(&wanted));
        assert_eq!(rows, oracle(&inventory, &wanted), "{layout:?}");
        assert_eq!(io.property_uuid_index_fallbacks, 1, "{layout:?}");
        assert_eq!(io.property_uuid_pages_considered, 0, "{layout:?}");
        assert_eq!(io.property_uuid_pages_selected, 0, "{layout:?}");
        assert_eq!(
            io.property_uuid_rows_decoded, ROUTE_ROWS,
            "{layout:?}: the fallback decodes the whole UUID column"
        );
    }
}

#[test]
fn unsorted_rows_in_a_candidate_page_fall_back_and_are_refused() {
    // Rows 3,000 and 3,001 sit inside the second page (rows 2,048..4,096),
    // swapped, so the page's bounds are intact but its rows descend.
    let mut indices = (0..ROUTE_ROWS).collect::<Vec<_>>();
    indices.swap(3_000, 3_001);
    let mut fixture = Fixture::new();
    fixture.fragment(0, &indices, Layout::PointAccess, 1_048_576);
    let inventory = fixture.inventory();

    let wanted = uuids_of(&[2_050]);
    let (result, io) = try_read(&inventory, Some(&wanted));
    assert_eq!(
        io.property_uuid_index_fallbacks, 1,
        "the page decode finds the descent and hands over to the full decode"
    );
    assert_corrupt_unsorted(result);

    // The unfiltered read refuses the same fragment: the filter does not hide it.
    let (result, _) = try_read(&inventory, None);
    assert_corrupt_unsorted(result);
}

#[test]
fn pages_whose_bounds_overlap_fall_back_and_are_refused() {
    // Rows 2,047 and 2,048 straddle the first page boundary, swapped: the
    // first page's maximum now exceeds the second page's minimum.
    let mut indices = (0..ROUTE_ROWS).collect::<Vec<_>>();
    indices.swap(2_047, 2_048);
    let mut fixture = Fixture::new();
    fixture.fragment(0, &indices, Layout::PointAccess, 1_048_576);
    let inventory = fixture.inventory();

    let wanted = uuids_of(&[5, 40_000]);
    let (result, io) = try_read(&inventory, Some(&wanted));
    assert_eq!(io.property_uuid_index_fallbacks, 1);
    assert_eq!(io.property_uuid_pages_selected, 0);
    assert_corrupt_unsorted(result);
}

#[test]
fn a_repeated_frontier_read_pages_its_uuid_column_and_returns_the_same_rows() {
    let wanted = scattered_with_absent();
    for layout in [Layout::Legacy, Layout::PointAccess] {
        let fixture = route(1, ROUTE_ROWS, layout, 1_048_576);
        let inventory = fixture.inventory();
        let expected = oracle(&inventory, &wanted);

        let targeted = |inventory: &AuthenticatedPropertyInventory| {
            let _capture = CaptureScope::install();
            crate::io_stats::reset();
            let data = read_authenticated_property_target_data_for_inventory(
                inventory,
                PropertyRouteKind::Node,
                "Person",
                &wanted,
            )
            .unwrap();
            (data, crate::io_stats::snapshot().unwrap())
        };
        // The first read of a fragment walks its keys to validate them; the
        // second, on the same inventory, selects rows through the page index.
        let (first, _) = targeted(&inventory);
        let (second, io) = targeted(&inventory);
        for (call, data) in [("first", &first), ("second", &second)] {
            assert_eq!(
                data.rows.values().cloned().collect::<Vec<_>>(),
                expected,
                "{layout:?}, {call} read"
            );
            assert_eq!(
                data.present,
                uuids_of(&SCATTERED),
                "{layout:?}, {call} read: the absent UUIDs have no owner"
            );
        }
        assert_eq!(io.property_uuid_index_fallbacks, 0, "{layout:?}");
        assert_eq!(io.property_uuid_selections, 1, "{layout:?}");
        assert!(io.property_uuid_pages_selected > 0, "{layout:?}");
        assert!(
            io.property_uuid_pages_selected <= io.property_uuid_pages_considered,
            "{layout:?}: {} of {} pages",
            io.property_uuid_pages_selected,
            io.property_uuid_pages_considered
        );
        if matches!(layout, Layout::PointAccess) {
            // Only the point-access pages are small enough to narrow the
            // scattered rows: the four legacy pages are all candidates.
            assert!(
                io.property_uuid_pages_selected < io.property_uuid_pages_considered
                    && io.property_uuid_rows_decoded <= io.property_uuid_pages_selected * PAGE,
                "{layout:?}: {} of {} pages, {} rows decoded",
                io.property_uuid_pages_selected,
                io.property_uuid_pages_considered,
                io.property_uuid_rows_decoded
            );
        }
    }
}

#[test]
fn a_tombstone_among_the_wanted_rows_is_owned_but_not_returned() {
    // Index 997 is a tombstone, and sits on a candidate page with live rows.
    let fixture = route(1, ROUTE_ROWS, Layout::PointAccess, 1_048_576);
    let inventory = fixture.inventory();
    let wanted = uuids_of(&[996, 997, 998, 61_999]);
    let (rows, io) = read(&inventory, Some(&wanted));
    assert_eq!(rows, oracle(&inventory, &wanted));
    assert_eq!(
        rows.iter().map(|row| row.uuid).collect::<Vec<_>>(),
        vec![uuid_of(996), uuid_of(998), uuid_of(61_999)],
        "the tombstone resolves to no row"
    );
    assert_eq!(io.property_uuid_index_fallbacks, 0);
    assert_eq!(io.property_uuid_pages_selected, 2, "pages 0 and 30");
}

#[test]
fn nominated_page_reads_stay_bounded_as_the_route_doubles() {
    // The same four rows, in four distinct pages of both routes.
    let wanted_rows = [10, 20_000, 40_000, 49_000];
    let wanted = uuids_of(&wanted_rows);
    let mut considered = Vec::new();
    for rows in [50_000, 100_000] {
        let fixture = route(1, rows, Layout::PointAccess, 1_048_576);
        let inventory = fixture.inventory();
        let (got, io) = read(&inventory, Some(&wanted));
        assert_eq!(got, oracle(&inventory, &wanted), "{rows}-row route");
        assert_eq!(io.property_uuid_index_fallbacks, 0, "{rows}-row route");
        assert_eq!(
            io.property_uuid_pages_selected,
            wanted_rows.len() as u64,
            "{rows}-row route: one candidate page per wanted row"
        );
        assert!(
            io.property_uuid_rows_decoded <= wanted_rows.len() as u64 * PAGE,
            "{rows}-row route decoded {} UUID rows for {} wanted",
            io.property_uuid_rows_decoded,
            wanted_rows.len()
        );
        considered.push(io.property_uuid_pages_considered);
    }
    assert_eq!(
        considered,
        [50_000_u64.div_ceil(PAGE), 100_000_u64.div_ceil(PAGE)],
        "the route doubled while the pages decoded stayed fixed"
    );
}

#[test]
fn nominated_page_reads_grow_with_partitions_not_with_the_route() {
    // One wanted row per residue modulo four, so each partition count still
    // finds every row, and each fragment holds a page bracketing each of them.
    let wanted_rows = [1_000, 30_001, 60_002, 90_003];
    let wanted = uuids_of(&wanted_rows);
    let rows = 100_000;
    for partitions in [1_u64, 2, 4] {
        let point = route(partitions, rows, Layout::PointAccess, 1_048_576);
        let legacy = route(partitions, rows, Layout::Legacy, 1_048_576);
        let point_inventory = point.inventory();
        let legacy_inventory = legacy.inventory();
        let expected = oracle(&point_inventory, &wanted);
        assert_eq!(expected.len(), wanted_rows.len());

        let (point_rows, point_io) = read(&point_inventory, Some(&wanted));
        let (legacy_rows, legacy_io) = read(&legacy_inventory, Some(&wanted));
        assert_eq!(point_rows, expected, "{partitions} partitions");
        assert_eq!(legacy_rows, expected, "{partitions} partitions, legacy");
        assert_eq!(point_io.property_uuid_index_fallbacks, 0);
        assert_eq!(point_io.property_uuid_selections, partitions);
        assert_eq!(
            point_io.property_uuid_pages_selected,
            wanted_rows.len() as u64 * partitions,
            "{partitions} partitions: a bracketing page per wanted row per fragment"
        );
        assert!(
            point_io.property_uuid_rows_decoded <= wanted_rows.len() as u64 * partitions * PAGE,
            "{partitions} partitions decoded {} UUID rows of a {rows}-row route",
            point_io.property_uuid_rows_decoded
        );
        assert!(
            point_io.property_uuid_rows_decoded < legacy_io.property_uuid_rows_decoded,
            "{partitions} partitions: point-access pages decode {}, legacy pages {}",
            point_io.property_uuid_rows_decoded,
            legacy_io.property_uuid_rows_decoded
        );
    }
}
