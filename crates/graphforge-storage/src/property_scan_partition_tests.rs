//! Output partitioning of the property scan: each partition reads one UUID
//! interval of the route, and together they return what one partition returns.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use sha2::Digest;
use tempfile::TempDir;

use super::{
    MIN_ROWS_PER_PARTITION, PropertyOverlayExec, PropertyScanOptions, partitions_for_route,
};
use crate::property_join_nomination::{UuidBuildKeyNomination, UuidBuildKeyTapExec};
use crate::property_overlay::{
    PROPERTY_GENERATION_KEY, PROPERTY_KIND_KEY, PROPERTY_ORDINAL_KEY, PROPERTY_OVERLAY_FORMAT,
    PROPERTY_OVERLAY_FORMAT_KEY, PROPERTY_ROUTE_KEY, PROPERTY_TOMBSTONE_FIELD, partition_interval,
};
use crate::{AuthenticatedPropertyInventory, PropertyFragmentId, PropertyRouteKind};

/// One physical row: key, deletes the key, `ident`, `name`.
type Row = (u16, bool, Option<i64>, Option<&'static str>);

/// What a key resolves to after the merge: `(ident, name)`.
type Resolved = (Option<i64>, Option<&'static str>);

fn key(id: u16) -> [u8; 16] {
    u128::from(id).to_be_bytes()
}

pub(crate) struct Route {
    dir: TempDir,
    entries: Vec<crate::GraphFileEntry>,
    sizes: Vec<u64>,
}

impl Route {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("properties/Person")).unwrap();
        Self {
            dir,
            entries: Vec::new(),
            sizes: Vec::new(),
        }
    }

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
        let keys = rows.iter().map(|row| key(row.0)).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(FixedSizeBinaryArray::try_from_iter(keys.iter()).unwrap()),
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
        let mut sha = String::with_capacity(64);
        for byte in sha2::Sha256::digest(&bytes) {
            use std::fmt::Write as _;
            write!(&mut sha, "{byte:02x}").unwrap();
        }
        self.sizes.push(u64::try_from(bytes.len()).unwrap());
        self.entries.push(crate::GraphFileEntry {
            content_xxh64: crate::corruption_checksum::checksum(&bytes),
            relative_path: format!("properties/Person/{}", id.file_name()),
            byte_length: u64::try_from(bytes.len()).unwrap(),
            content_sha256: sha,
            role: crate::GraphFileRole::Properties,
        });
    }

    pub(crate) fn inventory(&self) -> Arc<AuthenticatedPropertyInventory> {
        Arc::new(
            AuthenticatedPropertyInventory::from_entries_at_root(
                self.dir.path(),
                self.entries.clone(),
            )
            .unwrap(),
        )
    }

    pub(crate) fn scan(
        &self,
        inventory: &Arc<AuthenticatedPropertyInventory>,
        partitions: usize,
    ) -> Arc<dyn ExecutionPlan> {
        self.scan_with(inventory, partitions, None)
    }

    fn scan_with(
        &self,
        inventory: &Arc<AuthenticatedPropertyInventory>,
        partitions: usize,
        limit: Option<usize>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(self.scan_exec(inventory, partitions, limit))
    }

    fn scan_exec(
        &self,
        inventory: &Arc<AuthenticatedPropertyInventory>,
        partitions: usize,
        limit: Option<usize>,
    ) -> PropertyOverlayExec {
        let schema = inventory
            .route_schema(PropertyRouteKind::Node, "Person")
            .unwrap()
            .unwrap();
        PropertyOverlayExec::try_new(
            self.dir.path().to_path_buf(),
            Some(Arc::clone(inventory)),
            "Person".into(),
            false,
            schema,
            PropertyScanOptions {
                projection: None,
                limit,
                batch_size: 7,
                footer_statistics: true,
                equality: None,
                partitions,
            },
        )
        .unwrap()
    }
}

/// Four disjoint base fragments over keys 0..256, then mutation fragments that
/// reach across the whole key space: rewrites, deletions, and a resurrection.
pub(crate) fn layered() -> (Route, BTreeMap<u16, Resolved>) {
    let mut route = Route::new();
    let mut expected = BTreeMap::new();
    for slice in 0_u16..4 {
        let rows = (slice * 64..slice * 64 + 64)
            .map(|id| (id, false, Some(i64::from(id)), Some("base")))
            .collect::<Vec<Row>>();
        route.fragment(0, u64::from(slice), &rows, 16);
        for &(id, _, ident, name) in &rows {
            expected.insert(id, (ident, name));
        }
    }
    let rewritten = [3_u16, 70, 130, 200, 255]
        .map(|id| (id, false, Some(i64::from(id) * 10), Some("rewritten")));
    route.fragment(1, 0, &rewritten, 2);
    for &(id, _, ident, name) in &rewritten {
        expected.insert(id, (ident, name));
    }
    let deleted = [3_u16, 64, 130, 255].map(|id| (id, true, None, None));
    route.fragment(2, 0, &deleted, 2);
    for &(id, ..) in &deleted {
        expected.remove(&id);
    }
    let resurrected = [(130_u16, false, Some(1300), Some("resurrected"))];
    route.fragment(3, 0, &resurrected, 2);
    expected.insert(130, (Some(1300), Some("resurrected")));
    (route, expected)
}

/// The rows of one partition, in the order it emitted them.
async fn partition_rows(plan: &Arc<dyn ExecutionPlan>, partition: usize) -> Vec<(u16, Resolved)> {
    let mut stream = plan
        .execute(partition, Arc::new(TaskContext::default()))
        .unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        let keys = batch
            .column_by_name("node_uuid")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let idents = batch
            .column_by_name("ident")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let bytes: [u8; 16] = keys.value(row).try_into().unwrap();
            let id = u16::try_from(u128::from_be_bytes(bytes)).unwrap();
            let ident = (!idents.is_null(row)).then(|| idents.value(row));
            let name = (!names.is_null(row)).then(|| names.value(row).to_owned());
            rows.push((id, (ident, name)));
        }
    }
    rows.into_iter()
        .map(|(id, (ident, name))| {
            // The fixture's names are static; map the decoded text back.
            let name = name.map(|name| match name.as_str() {
                "base" => "base",
                "rewritten" => "rewritten",
                "resurrected" => "resurrected",
                other => panic!("unexpected name {other}"),
            });
            (id, (ident, name))
        })
        .collect()
}

async fn all_rows(plan: &Arc<dyn ExecutionPlan>) -> Vec<Vec<(u16, Resolved)>> {
    let partitions = plan.properties().output_partitioning().partition_count();
    let mut out = Vec::new();
    for partition in 0..partitions {
        out.push(partition_rows(plan, partition).await);
    }
    out
}

#[tokio::test]
async fn partitions_return_the_rows_of_the_single_partition_scan() {
    let (route, expected) = layered();
    let inventory = route.inventory();
    let single = route.scan(&inventory, 1);
    let split = route.scan(&inventory, 4);
    assert_eq!(
        single.properties().output_partitioning().partition_count(),
        1
    );
    assert_eq!(
        split.properties().output_partitioning().partition_count(),
        4
    );

    let single_rows = all_rows(&single).await;
    let split_rows = all_rows(&split).await;
    let model = expected.into_iter().collect::<Vec<_>>();
    let mut one = single_rows.into_iter().flatten().collect::<Vec<_>>();
    one.sort();
    assert_eq!(one, model, "the single partition matches the merge model");

    // Every output row appears exactly once across the four partitions, and
    // the rows are identical to the single partition's.
    let mut many = split_rows.iter().flatten().cloned().collect::<Vec<_>>();
    many.sort();
    assert_eq!(many, model, "four partitions return the same rows");

    // Each partition holds a disjoint interval of keys, so no key is read twice.
    let keys = split_rows
        .iter()
        .map(|rows| rows.iter().map(|row| row.0).collect::<BTreeSet<_>>())
        .collect::<Vec<_>>();
    for (left, first) in keys.iter().enumerate() {
        for second in &keys[left + 1..] {
            assert!(first.is_disjoint(second), "partitions share a key");
        }
    }
    assert!(
        split_rows.iter().all(|rows| !rows.is_empty()),
        "the row-balanced cuts leave no partition empty: {:?}",
        split_rows.iter().map(Vec::len).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn mutations_and_tombstones_resolve_in_the_partition_that_holds_their_key() {
    let (route, expected) = layered();
    let inventory = route.inventory();
    let cuts = inventory
        .route_partition_cuts(PropertyRouteKind::Node, "Person", 4)
        .unwrap();
    assert_eq!(cuts.len(), 3, "four partitions need three cuts");
    let owner = |id: u16| -> usize {
        (0..4)
            .find(|&partition| {
                let (low, high) = partition_interval(&cuts, partition);
                key(id) >= low && high.is_none_or(|high| key(id) < high)
            })
            .unwrap()
    };
    // The rewritten and deleted keys straddle the cuts: at least three
    // partitions resolve a mutation or a tombstone.
    let touched = [3_u16, 64, 70, 130, 200, 255];
    let spanned = touched.iter().map(|&id| owner(id)).collect::<BTreeSet<_>>();
    assert!(
        spanned.len() >= 3,
        "mutations must span cuts, got partitions {spanned:?}"
    );

    let split = route.scan(&inventory, 4);
    let rows = all_rows(&split).await;
    for (partition, rows) in rows.iter().enumerate() {
        for (id, resolved) in rows {
            assert_eq!(owner(*id), partition, "key {id} read outside its interval");
            assert_eq!(
                expected.get(id),
                Some(resolved),
                "key {id} resolved wrongly"
            );
        }
    }
    // Deleted keys are absent and the resurrected key shows its last value.
    let present = rows
        .iter()
        .flatten()
        .map(|row| row.0)
        .collect::<BTreeSet<_>>();
    for deleted in [3_u16, 64, 255] {
        assert!(
            !present.contains(&deleted),
            "tombstoned key {deleted} returned"
        );
    }
    let resurrected = rows.iter().flatten().find(|row| row.0 == 130).unwrap();
    assert_eq!(resurrected.1, (Some(1300), Some("resurrected")));
    // A rewrite appears once, with the newest value.
    let rewritten = rows.iter().flatten().filter(|row| row.0 == 70).count();
    assert_eq!(rewritten, 1);
}

#[tokio::test]
async fn each_partition_started_is_counted_and_a_missing_partition_is_refused() {
    let (route, _) = layered();
    let inventory = route.inventory();
    let _capture = crate::lifecycle_io::CaptureScope::install();
    let single = route.scan(&inventory, 1);
    let split = route.scan(&inventory, 4);
    let executed = |plan: &Arc<dyn ExecutionPlan>| {
        plan.metrics()
            .unwrap()
            .sum_by_name("property_partitions_executed")
            .unwrap()
            .as_usize()
    };
    assert_eq!(executed(&single), 0);
    assert_eq!(executed(&split), 0);

    let _ = all_rows(&single).await;
    assert_eq!(executed(&single), 1);
    let _ = partition_rows(&split, 2).await;
    assert_eq!(executed(&split), 1);
    for partition in [0, 1, 3] {
        let _ = partition_rows(&split, partition).await;
    }
    assert_eq!(executed(&split), 4, "every output partition ran");

    let refused = split.execute(4, Arc::new(TaskContext::default()));
    assert!(refused.is_err(), "a fifth partition does not exist");
    let refused = single.execute(1, Arc::new(TaskContext::default()));
    assert!(
        refused.is_err(),
        "a single-partition scan has partition 0 only"
    );
    assert_eq!(executed(&split), 4, "a refused partition is not counted");
}

/// A nomination completed by the build keys `keys`, as a join's build side
/// publishes it.
async fn completed_nomination(keys: &[u16]) -> Arc<UuidBuildKeyNomination> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "node_uuid",
        DataType::FixedSizeBinary(16),
        false,
    )]));
    let ids = FixedSizeBinaryArray::try_from_iter(keys.iter().map(|id| key(*id))).unwrap();
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids)]).unwrap();
    let input = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
    let nomination = UuidBuildKeyNomination::new();
    let tap = Arc::new(UuidBuildKeyTapExec::new(input, 0, Arc::clone(&nomination)));
    let mut stream = tap.execute(0, Arc::new(TaskContext::default())).unwrap();
    while let Some(batch) = stream.next().await {
        batch.unwrap();
    }
    nomination
}

#[tokio::test]
async fn a_nominated_read_requests_its_keys_once_however_many_partitions_read_them() {
    let (route, expected) = layered();
    let inventory = route.inventory();
    // Keys spread over every quarter of the key space, so each partition of a
    // four-way scan holds some of them.
    let wanted = [3_u16, 40, 70, 100, 130, 160, 200, 230, 250];
    let nomination = completed_nomination(&wanted).await;
    let model = wanted
        .iter()
        .filter_map(|id| expected.get(id).map(|resolved| (*id, *resolved)))
        .collect::<Vec<_>>();

    for partitions in [1, 4] {
        let _capture = crate::lifecycle_io::CaptureScope::install();
        crate::io_stats::reset();
        let plan = Arc::new(
            route
                .scan_exec(&inventory, partitions, None)
                .with_uuid_nomination(Arc::clone(&nomination)),
        ) as Arc<dyn ExecutionPlan>;
        assert_eq!(
            plan.properties().output_partitioning().partition_count(),
            partitions
        );
        let mut rows = all_rows(&plan)
            .await
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        rows.sort();
        assert_eq!(rows, model, "{partitions} partitions: the nominated rows");
        let requested = crate::io_stats::snapshot()
            .expect("the I/O capture is installed")
            .property_rows_requested;
        assert_eq!(
            requested,
            wanted.len() as u64,
            "{partitions} partitions: one request for the nominated keys, not one per partition"
        );
    }
}

#[tokio::test]
async fn the_plan_names_its_partition_count_only_when_there_are_several() {
    let (route, _) = layered();
    let inventory = route.inventory();
    let display = |plan: &Arc<dyn ExecutionPlan>| {
        datafusion::physical_plan::displayable(plan.as_ref())
            .one_line()
            .to_string()
            .trim_end()
            .to_owned()
    };
    assert_eq!(
        display(&route.scan(&inventory, 1)),
        "PropertyOverlayExec: route=Person"
    );
    assert_eq!(
        display(&route.scan(&inventory, 4)),
        "PropertyOverlayExec: route=Person, partitions=4"
    );
}

#[tokio::test]
async fn a_limit_or_a_missing_inventory_keeps_one_partition() {
    let (route, _) = layered();
    let inventory = route.inventory();
    let limited = route.scan_with(&inventory, 4, Some(5));
    assert_eq!(
        limited.properties().output_partitioning().partition_count(),
        1
    );
    let rows = partition_rows(&limited, 0).await;
    assert_eq!(rows.len(), 5);

    let schema = inventory
        .route_schema(PropertyRouteKind::Node, "Person")
        .unwrap()
        .unwrap();
    let unpinned = PropertyOverlayExec::try_new(
        route.dir.path().to_path_buf(),
        None,
        "Person".into(),
        false,
        schema,
        PropertyScanOptions {
            projection: None,
            limit: None,
            batch_size: 7,
            footer_statistics: true,
            equality: None,
            partitions: 4,
        },
    )
    .unwrap();
    assert_eq!(
        unpinned
            .properties()
            .output_partitioning()
            .partition_count(),
        1
    );
}

#[tokio::test]
async fn partition_statistics_split_the_planned_rows() {
    let (route, _) = layered();
    let inventory = route.inventory();
    let split = route.scan(&inventory, 4);
    let total = split.partition_statistics(None).unwrap().num_rows;
    let share = split.partition_statistics(Some(1)).unwrap().num_rows;
    let beyond = split.partition_statistics(Some(4)).unwrap().num_rows;
    let total = *total.get_value().unwrap();
    let share = *share.get_value().unwrap();
    assert_eq!(share, total.div_ceil(4));
    assert!(
        beyond.get_value().is_none(),
        "no such partition has no estimate"
    );
}

#[tokio::test]
async fn a_fragment_is_authenticated_once_for_each_partition_that_opens_it() {
    let (route, _) = layered();
    let inventory = route.inventory();
    let single = {
        let _capture = crate::lifecycle_io::CaptureScope::install();
        let plan = route.scan(&inventory, 1);
        let _ = all_rows(&plan).await;
        plan.metrics()
            .unwrap()
            .sum_by_name("property_authentication_bytes")
            .unwrap()
            .as_usize()
    };

    let cuts = inventory
        .route_partition_cuts(PropertyRouteKind::Node, "Person", 4)
        .unwrap();
    // Legacy (non-content-addressed) files authenticate in full on every open,
    // so a fragment whose keys straddle a cut is authenticated by each
    // partition that opens it, never skipped and never charged to one that
    // prunes it.
    let ranges = [
        (0_u16, 63_u16),
        (64, 127),
        (128, 191),
        (192, 255),
        (3, 255),
        (3, 255),
        (130, 130),
    ];
    let mut expected = 0_u64;
    for (fragment, (low, high)) in ranges.iter().enumerate() {
        let opened = (0..4)
            .filter(|&partition| {
                let (from, to) = partition_interval(&cuts, partition);
                key(*high) >= from && to.is_none_or(|to| key(*low) < to)
            })
            .count() as u64;
        expected += opened * route.sizes[fragment];
    }

    let split = {
        let _capture = crate::lifecycle_io::CaptureScope::install();
        let plan = route.scan(&inventory, 4);
        let _ = all_rows(&plan).await;
        plan.metrics()
            .unwrap()
            .sum_by_name("property_authentication_bytes")
            .unwrap()
            .as_usize()
    };
    assert_eq!(single as u64, route.sizes.iter().sum::<u64>());
    assert_eq!(split as u64, expected);
    assert!(
        split >= single,
        "partitioning never reads less than one pass"
    );
}

#[test]
fn a_small_route_keeps_one_partition_and_a_large_one_takes_the_session_target() {
    let (route, _) = layered();
    let inventory = route.inventory();
    assert_eq!(
        partitions_for_route(16, Some(&inventory), false, "Person"),
        1
    );
    assert_eq!(partitions_for_route(16, None, false, "Person"), 1);
    assert_eq!(
        partitions_for_route(16, Some(&inventory), false, "Absent"),
        1
    );

    // The estimate is the manifest's byte length over sixteen, so a route of
    // many rows takes one partition per MIN_ROWS_PER_PARTITION, capped by the
    // session target.
    let mut large = Route::new();
    let rows = (0_u16..40_000)
        .map(|id| (id, false, Some(i64::from(id) * 7_919), None))
        .collect::<Vec<Row>>();
    large.fragment(0, 0, &rows, 4_096);
    let by_size = usize::try_from(large.sizes[0] / 16).unwrap() / MIN_ROWS_PER_PARTITION;
    assert!(by_size >= 4, "the fixture must be large enough: {by_size}");
    let manifest = large.inventory();
    assert_eq!(partitions_for_route(4, Some(&manifest), false, "Person"), 4);
    assert_eq!(
        partitions_for_route(by_size + 10, Some(&manifest), false, "Person"),
        by_size
    );
    assert_eq!(partitions_for_route(1, Some(&manifest), false, "Person"), 1);
}

#[test]
fn partitions_started_together_derive_the_route_cuts_once() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let shared = super::SharedCuts::default();
    let computed = AtomicUsize::new(0);
    let answers = std::thread::scope(|scope| {
        let workers = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    shared
                        .get_or_compute(|| {
                            computed.fetch_add(1, Ordering::SeqCst);
                            // Hold the computation long enough that every
                            // other worker reaches the cache while it runs.
                            std::thread::sleep(std::time::Duration::from_millis(50));
                            Ok(vec![key(10), key(20)])
                        })
                        .expect("cuts")
                })
            })
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .map(|worker| worker.join().expect("worker"))
            .collect::<Vec<_>>()
    });
    assert_eq!(computed.load(Ordering::SeqCst), 1, "one partition derives");
    assert!(answers.iter().all(|cuts| **cuts == vec![key(10), key(20)]));
}

#[test]
fn a_failed_cut_derivation_is_not_remembered() {
    let shared = super::SharedCuts::default();
    let failed = shared.get_or_compute(|| Err(crate::GfError::Validation("footer refused".into())));
    assert!(failed.is_err(), "the failure reaches the caller");
    let recovered = shared
        .get_or_compute(|| Ok(vec![key(5)]))
        .expect("a later partition derives the cuts");
    assert_eq!(*recovered, vec![key(5)]);
}
