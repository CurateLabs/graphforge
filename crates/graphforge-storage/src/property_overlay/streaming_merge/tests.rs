use super::super::selective_reads::{EqualityValue, PropertyEquality};
use super::super::*;
use super::*;
use arrow::array::{FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::{BTreeSet, HashMap};
use tempfile::TempDir;

/// One physical row of a fragment: its UUID, whether it deletes the UUID, and
/// its `ident` and `name` properties.
type Row = (u8, bool, Option<i64>, Option<&'static str>);

fn uuid(index: u8) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    bytes[15] = index;
    bytes
}

struct Fixture {
    dir: TempDir,
    entries: Vec<crate::GraphFileEntry>,
}

impl Fixture {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("properties/Person")).unwrap();
        Self {
            dir,
            entries: Vec::new(),
        }
    }

    /// Write one fragment. `row_group_rows` splits it into row groups.
    fn fragment(&mut self, generation: u64, ordinal: u64, rows: &[Row], row_group_rows: usize) {
        let id = PropertyFragmentId {
            generation,
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
                (PROPERTY_GENERATION_KEY.into(), generation.to_string()),
                (PROPERTY_ORDINAL_KEY.into(), ordinal.to_string()),
            ]),
        ));
        let uuids = rows.iter().map(|row| uuid(row.0)).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(FixedSizeBinaryArray::try_from_iter(uuids.iter()).unwrap()),
                Arc::new(BooleanArray::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.3).collect::<Vec<_>>(),
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
            Some(
                WriterProperties::builder()
                    .set_max_row_group_row_count(Some(row_group_rows))
                    .build(),
            ),
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

fn live(index: u8, ident: i64, name: &'static str) -> Row {
    (index, false, Some(ident), Some(name))
}

fn dead(index: u8) -> Row {
    (index, true, None, None)
}

/// Two disjoint base fragments, then three mutation fragments that overwrite,
/// delete and resurrect rows across both.
fn layered() -> Fixture {
    let mut fixture = Fixture::new();
    fixture.fragment(
        0,
        0,
        &(0..40)
            .map(|index| live(index, i64::from(index), "base"))
            .collect::<Vec<_>>(),
        8,
    );
    fixture.fragment(
        0,
        1,
        &(40..80)
            .map(|index| live(index, i64::from(index), "base"))
            .collect::<Vec<_>>(),
        8,
    );
    // Overwrite across both base fragments, with a value that moves 45 to 5.
    fixture.fragment(
        1,
        0,
        &[
            live(5, 500, "rewritten"),
            live(17, 17, "rewritten"),
            live(45, 5, "rewritten"),
            live(79, 790, "rewritten"),
        ],
        2,
    );
    fixture.fragment(2, 0, &[dead(5), dead(60)], 2);
    fixture.fragment(3, 0, &[live(5, 5, "resurrected")], 2);
    fixture
}

fn spooled(inventory: &AuthenticatedPropertyInventory) -> Vec<PropertySnapshotRow> {
    let scratch = TempDir::new().unwrap();
    let mut rows = Vec::new();
    inventory
        .visit_route(
            PropertyRouteKind::Node,
            "Person",
            scratch.path(),
            PropertyOverlayLimits::default(),
            |row| {
                rows.push(row);
                Ok(())
            },
        )
        .unwrap();
    rows
}

fn streamed(
    inventory: &AuthenticatedPropertyInventory,
    uuids: Option<&BTreeSet<[u8; 16]>>,
) -> (Vec<PropertySnapshotRow>, PropertyOverlayMetrics) {
    let mut rows = Vec::new();
    let metrics = inventory
        .visit_route_streaming(
            &RouteRead {
                kind: PropertyRouteKind::Node,
                route: "Person",
                selected_properties: None,
                uuids,
                limits: PropertyOverlayLimits::default(),
                collect: true,
            },
            |row| {
                rows.push(row);
                Ok(true)
            },
        )
        .unwrap();
    (rows, metrics)
}

#[test]
fn streaming_merge_returns_the_rows_the_spooled_merge_returns() {
    let fixture = layered();
    let inventory = fixture.inventory();
    let expected = spooled(&inventory);
    assert_eq!(
        expected.len(),
        79,
        "80 UUIDs, 60 deleted for good, 5 resurrected"
    );
    assert!(expected.iter().any(|row| row.uuid == uuid(5)));
    assert!(expected.iter().all(|row| row.uuid != uuid(60)));
    let (rows, metrics) = streamed(&inventory, None);
    assert_eq!(rows, expected);
    assert_eq!(metrics.spill_bytes, 0);
    assert_eq!(metrics.spill_runs, 0);
    assert_eq!(metrics.merge_passes, 0);
    assert_eq!(metrics.logical_rows, 79);
    assert_eq!(metrics.tombstones, 1, "only 60 stays deleted");
    assert_eq!(metrics.fragments_considered, 5);
}

#[test]
fn streaming_merge_creates_no_scratch_and_writes_nothing() {
    let fixture = layered();
    let inventory = fixture.inventory();
    let parent = fixture.dir.path().parent().unwrap().to_path_buf();
    let scratch_directories = || {
        fs::read_dir(&parent)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".gf-property-scratch-")
            })
            .count()
    };
    let _capture = crate::lifecycle_io::CaptureScope::install();
    let before_directories = scratch_directories();
    let before = crate::lifecycle_io::snapshot().unwrap();
    let (rows, _) = streamed(&inventory, None);
    assert_eq!(rows.len(), 79);
    let work = crate::lifecycle_io::snapshot()
        .unwrap()
        .since(&before)
        .unwrap();
    assert_eq!(work.totals.write_bytes, 0, "{work:#?}");
    assert_eq!(work.totals.write_calls, 0, "{work:#?}");
    assert!(
        work.phases[&crate::StorageIoPhase::ReadPathScan].read_bytes > 0,
        "the read is observed: {work:#?}"
    );
    assert_eq!(scratch_directories(), before_directories);
}

#[test]
fn a_uuid_restricted_read_resolves_each_target_against_newer_fragments() {
    let fixture = layered();
    let inventory = fixture.inventory();
    let all = spooled(&inventory);
    // 5 is resurrected, 60 is deleted, 17 and 45 are rewritten, 70 is untouched
    // and 99 does not exist.
    let targets = [5, 17, 45, 60, 70, 99].map(uuid).into_iter().collect();
    let (rows, metrics) = streamed(&inventory, Some(&targets));
    let expected = all
        .into_iter()
        .filter(|row| targets.contains(&row.uuid))
        .collect::<Vec<_>>();
    assert_eq!(rows, expected);
    assert_eq!(rows.len(), 4);
    assert!(
        metrics.physical_rows < 80,
        "row groups outside the targets are not decoded: {}",
        metrics.physical_rows
    );
}

fn people_named(
    inventory: &AuthenticatedPropertyInventory,
    equality: &PropertyEquality,
) -> (Vec<[u8; 16]>, PropertyOverlayMetrics) {
    let limits = PropertyOverlayLimits::default();
    let (candidates, mut metrics) = inventory
        .equality_candidates(
            PropertyRouteKind::Node,
            "Person",
            equality,
            limits,
            1 << 10,
            true,
        )
        .unwrap()
        .expect("the equality is answerable");
    let (rows, resolved) = streamed(inventory, Some(&candidates));
    metrics.absorb(&resolved);
    (
        rows.into_iter()
            .filter(|row| equality.holds(&row.values))
            .map(|row| row.uuid)
            .collect(),
        metrics,
    )
}

fn ident_equals(value: i64) -> PropertyEquality {
    PropertyEquality {
        column: "ident".into(),
        value: EqualityValue::Int(value),
    }
}

#[test]
fn an_equality_answers_from_statistics_and_agrees_with_a_full_scan() {
    let fixture = layered();
    let inventory = fixture.inventory();
    let all = spooled(&inventory);
    for value in [0, 5, 17, 33, 45, 60, 79, 500, 790, 12_345] {
        let equality = ident_equals(value);
        let expected = all
            .iter()
            .filter(|row| equality.holds(&row.values))
            .map(|row| row.uuid)
            .collect::<Vec<_>>();
        let (found, _) = people_named(&inventory, &equality);
        assert_eq!(found, expected, "ident = {value}");
    }
}

#[test]
fn a_candidate_shadowed_by_a_newer_snapshot_is_not_returned() {
    let fixture = layered();
    let inventory = fixture.inventory();
    // 45 held ident 45 in the base and now holds 5; 5 held 5 in the base, then
    // 500, then was deleted, then returned with 5.
    let (found, _) = people_named(&inventory, &ident_equals(45));
    assert!(found.is_empty(), "no snapshot holds 45 any more");
    let (found, _) = people_named(&inventory, &ident_equals(5));
    assert_eq!(found, vec![uuid(5), uuid(45)]);
    // 60's only snapshot holding 60 is deleted by a newer tombstone.
    let (found, _) = people_named(&inventory, &ident_equals(60));
    assert!(found.is_empty());
}

#[test]
fn an_equality_reads_only_the_fragments_and_row_groups_that_can_hold_the_value() {
    let fixture = layered();
    let inventory = fixture.inventory();
    let (found, metrics) = people_named(&inventory, &ident_equals(33));
    assert_eq!(found, vec![uuid(33)]);
    // The candidate scan opens the base fragment whose range holds 33 and the
    // mutation fragments whose statistics admit it; the resolution reads the
    // fragments whose UUID range holds the candidate.
    let (all, full) = streamed(&inventory, None);
    assert_eq!(all.len(), 79);
    assert!(
        metrics.physical_rows * 2 < full.physical_rows,
        "pruned read decodes {} rows, a full scan {}",
        metrics.physical_rows,
        full.physical_rows
    );
    assert!(metrics.row_groups_selected <= metrics.row_groups_considered);
}

#[test]
fn a_column_stored_with_another_type_is_not_answered_from_statistics() {
    let fixture = layered();
    let inventory = fixture.inventory();
    let equality = PropertyEquality {
        column: "ident".into(),
        value: EqualityValue::Str("5".into()),
    };
    let candidates = inventory
        .equality_candidates(
            PropertyRouteKind::Node,
            "Person",
            &equality,
            PropertyOverlayLimits::default(),
            1 << 10,
            true,
        )
        .unwrap();
    assert!(candidates.is_none());
}

#[test]
fn too_many_candidates_decline_the_pushdown() {
    let fixture = layered();
    let inventory = fixture.inventory();
    let equality = PropertyEquality {
        column: "name".into(),
        value: EqualityValue::Str("base".into()),
    };
    let candidates = inventory
        .equality_candidates(
            PropertyRouteKind::Node,
            "Person",
            &equality,
            PropertyOverlayLimits::default(),
            10,
            true,
        )
        .unwrap();
    assert!(candidates.is_none(), "76 rows hold `base`; the cap is 10");
}

#[test]
fn an_overlapping_fragment_beyond_the_live_byte_budget_is_refused_not_spilled() {
    let fixture = layered();
    let inventory = fixture.inventory();
    let _capture = crate::lifecycle_io::CaptureScope::install();
    let before = crate::lifecycle_io::snapshot().unwrap();
    let error = inventory
        .visit_route_streaming(
            &RouteRead {
                kind: PropertyRouteKind::Node,
                route: "Person",
                selected_properties: None,
                uuids: None,
                limits: PropertyOverlayLimits {
                    max_buffered_bytes: 20 * 1024,
                    ..PropertyOverlayLimits::default()
                },
                collect: true,
            },
            |_| Ok(true),
        )
        .unwrap_err();
    assert!(error.to_string().contains("budget"), "{error}");
    let work = crate::lifecycle_io::snapshot()
        .unwrap()
        .since(&before)
        .unwrap();
    assert_eq!(work.totals.write_bytes, 0, "{work:#?}");
}

#[test]
fn a_stop_request_ends_the_merge_early() {
    let fixture = layered();
    let inventory = fixture.inventory();
    let mut seen = 0;
    inventory
        .visit_route_streaming(
            &RouteRead {
                kind: PropertyRouteKind::Node,
                route: "Person",
                selected_properties: None,
                uuids: None,
                limits: PropertyOverlayLimits::default(),
                collect: true,
            },
            |_| {
                seen += 1;
                Ok(seen < 3)
            },
        )
        .unwrap();
    assert_eq!(seen, 3);
}
