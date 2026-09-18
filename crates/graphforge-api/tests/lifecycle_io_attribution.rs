//! Per-phase application I/O attribution for the phases outside construction
//! (#1389): project open, reopen, recount and query execution.
//!
//! The CI test asserts the attribution exists, reconciles and separates open
//! cost from execution cost. The ignored report prints the per-phase table this
//! issue was opened to produce, plus the instrumentation's own overhead.

use std::sync::Mutex;
use std::time::Instant;

use arrow::array::Int64Array;
use graphforge_api::{GraphForge, LifecyclePhaseAttribution, lifecycle_io_snapshot};
use graphforge_storage::StorageIoPhase;

/// Serializes the process-global lifecycle counters used by the assertions.
static LIFECYCLE_IO_LOCK: Mutex<()> = Mutex::new(());

const ORDERED_ONE_HOP: &str = "MATCH (a)-[r]->(b) RETURN b.value AS id ORDER BY id LIMIT 10";
const EDGE_RECOUNT: &str = "MATCH ()-[r]->() RETURN count(*)";

fn seed_query(edges: usize) -> String {
    let mut query = "CREATE (root:Root {value: 0})".to_owned();
    for index in 1..=edges {
        query.push_str(&format!(
            " CREATE (root)-[:HAS]->(n{index}:Leaf {{value: {index}}})"
        ));
    }
    query
}

fn seed_project(path: &std::path::Path, edges: usize) {
    let forge = GraphForge::new(Some(path.to_str().expect("utf-8 project path")))
        .expect("project opens for seeding");
    forge.execute(&seed_query(edges)).expect("seed CREATE runs");
    drop(forge);
}

fn scalar_count(forge: &GraphForge, query: &str) -> i64 {
    let result = forge.execute(query).expect("count query executes");
    result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is Int64")
        .value(0)
}

/// Application I/O attributed to one measured region, with its wall time.
struct Region {
    attribution: LifecyclePhaseAttribution,
    elapsed_micros: u128,
}

fn measure<T>(work: impl FnOnce() -> T) -> (T, Region) {
    let before = lifecycle_io_snapshot();
    let started = Instant::now();
    let value = work();
    let elapsed_micros = started.elapsed().as_micros();
    let attribution = lifecycle_io_snapshot()
        .since(&before)
        .expect("region attribution");
    attribution
        .validate_for_qualification()
        .expect("region attribution reconciles");
    (
        value,
        Region {
            attribution,
            elapsed_micros,
        },
    )
}

fn read_bytes(attribution: &LifecyclePhaseAttribution, phase: StorageIoPhase) -> u64 {
    attribution.phases[&phase].read_bytes
}

/// One project lifecycle measured phase by phase.
struct Lifecycle {
    edges: usize,
    open: Region,
    recount: Region,
    first_query: Region,
    second_query: Region,
}

fn measure_lifecycle(edges: usize) -> Lifecycle {
    let project = tempfile::tempdir().expect("project directory");
    let path = project.path().join("state");
    seed_project(&path, edges);

    let (forge, open) = measure(|| {
        GraphForge::new(Some(path.to_str().expect("utf-8 project path"))).expect("project reopens")
    });
    // The facade reports the same region for itself, without a caller snapshot.
    assert_eq!(forge.open_io_attribution(), &open.attribution);

    let (counted, recount) = measure(|| scalar_count(&forge, EDGE_RECOUNT));
    assert_eq!(counted, i64::try_from(edges).expect("edge count fits i64"));

    let (_, first_query) = measure(|| {
        forge
            .execute(ORDERED_ONE_HOP)
            .expect("bounded query executes")
    });
    let (_, second_query) = measure(|| {
        forge
            .execute(ORDERED_ONE_HOP)
            .expect("bounded query repeats")
    });

    Lifecycle {
        edges,
        open,
        recount,
        first_query,
        second_query,
    }
}

#[test]
fn every_lifecycle_phase_reports_reconciled_per_phase_application_io() {
    let _guard = LIFECYCLE_IO_LOCK.lock().expect("lifecycle I/O lock");
    let lifecycle = measure_lifecycle(64);

    // 1. Opening a project reads and authenticates committed data, and the
    //    hydration/verification row is where that work lands.
    assert!(
        read_bytes(
            &lifecycle.open.attribution,
            StorageIoPhase::HydrationVerification
        ) > 0,
        "open performs no attributed hydration work: {:#?}",
        lifecycle.open.attribution
    );
    // 2. Publication control preauthentication is part of every open: CURRENT,
    //    FORMAT, the generation manifest and its participants.
    assert!(
        read_bytes(
            &lifecycle.open.attribution,
            StorageIoPhase::PublicationPreauthentication
        ) > 0,
        "open authenticates no publication control: {:#?}",
        lifecycle.open.attribution
    );
    // 3. The read path is attributed separately from the open path, so query
    //    cost can be told apart from open cost.
    assert!(
        read_bytes(&lifecycle.recount.attribution, StorageIoPhase::ReadPathScan)
            + read_bytes(
                &lifecycle.first_query.attribution,
                StorageIoPhase::ReadPathScan
            )
            > 0,
        "no read-path scan attributed to recount or query"
    );
    // 4. A second query in the same session re-pays no open cost: the handle is
    //    the session, and hydration happened once.
    assert_eq!(
        read_bytes(
            &lifecycle.second_query.attribution,
            StorageIoPhase::HydrationVerification
        ),
        0,
        "a repeated query re-paid hydration: {:#?}",
        lifecycle.second_query.attribution
    );
    assert_eq!(
        read_bytes(
            &lifecycle.second_query.attribution,
            StorageIoPhase::PublicationPreauthentication
        ),
        0,
        "a repeated query re-paid publication preauthentication"
    );

    // Construction phases never appear on a read-only lifecycle region.
    for phase in [
        StorageIoPhase::AppendMerge,
        StorageIoPhase::SealAuthentication,
        StorageIoPhase::ShapeConsumeReauthentication,
    ] {
        assert_eq!(
            lifecycle.second_query.attribution.phases[&phase],
            graphforge_storage::PhaseIoTotals::default(),
            "{phase:?} reported work during a query"
        );
    }
}

#[test]
fn the_lifecycle_inventory_extends_the_construction_inventory_without_changing_it() {
    // The construction inventory is closed at nine rows, so construction
    // attribution documents are byte-identical to the ones recorded before
    // this instrumentation existed.
    assert_eq!(StorageIoPhase::ALL.len(), 9);
    assert!(!StorageIoPhase::ALL.contains(&StorageIoPhase::ReadPathScan));
    assert_eq!(StorageIoPhase::LIFECYCLE.len(), 10);

    let encoded = serde_json::to_value(lifecycle_io_snapshot()).expect("attribution serializes");
    let phases = encoded["phases"].as_object().expect("phase map");
    assert_eq!(phases.len(), 10);
    assert!(phases.contains_key("read_path_scan"));
    // The document is the shape construction already emits.
    assert_eq!(encoded.as_object().expect("document").len(), 2);
    assert!(encoded.get("totals").is_some());
}

/// Print the per-phase lifecycle attribution this issue was opened to produce.
///
/// `cargo test -p graphforge-api --test lifecycle_io_attribution -- --ignored
/// --nocapture report_lifecycle_io_attribution`
#[test]
#[ignore = "reporting run; prints the per-phase table rather than asserting"]
fn report_lifecycle_io_attribution() {
    let _guard = LIFECYCLE_IO_LOCK.lock().expect("lifecycle I/O lock");
    for edges in [1_024_usize, 4_096, 16_384] {
        let lifecycle = measure_lifecycle(edges);
        print_lifecycle(&lifecycle);
    }
    report_instrumentation_overhead();
}

fn print_lifecycle(lifecycle: &Lifecycle) {
    println!("\n=== edges={} ===", lifecycle.edges);
    for (name, region) in [
        ("open/reopen", &lifecycle.open),
        ("recount", &lifecycle.recount),
        ("query #1", &lifecycle.first_query),
        ("query #2 (same session)", &lifecycle.second_query),
    ] {
        let edges = u128::try_from(lifecycle.edges).expect("edge count fits u128");
        println!(
            "\n-- {name}: {} us total, {:.3} us/edge",
            region.elapsed_micros,
            region.elapsed_micros as f64 / edges as f64
        );
        println!(
            "{:<42} {:>12} {:>9} {:>12} {:>9} {:>7}",
            "phase", "read_bytes", "reads", "write_bytes", "writes", "fsyncs"
        );
        for phase in StorageIoPhase::LIFECYCLE {
            let totals = &region.attribution.phases[&phase];
            if totals == &graphforge_storage::PhaseIoTotals::default() {
                continue;
            }
            println!(
                "{:<42} {:>12} {:>9} {:>12} {:>9} {:>7}",
                format!("{phase:?}"),
                totals.read_bytes,
                totals.read_calls,
                totals.write_bytes,
                totals.write_calls,
                totals.fsync_calls
            );
        }
        let totals = &region.attribution.totals;
        println!(
            "{:<42} {:>12} {:>9} {:>12} {:>9} {:>7}",
            "TOTAL",
            totals.read_bytes,
            totals.read_calls,
            totals.write_bytes,
            totals.write_calls,
            totals.fsync_calls
        );
        println!(
            "   read bytes per edge: {:.2}",
            totals.read_bytes as f64 / edges as f64
        );
    }
}

/// Cost of the recording itself, measured against the number of record calls a
/// lifecycle actually makes.
fn report_instrumentation_overhead() {
    const SAMPLES: u64 = 10_000_000;
    let started = Instant::now();
    for _ in 0..SAMPLES {
        graphforge_storage::lifecycle_io::record_read(StorageIoPhase::ReadPathScan, 4_096, 1);
    }
    let elapsed = started.elapsed();
    println!(
        "\n=== instrumentation overhead ===\n{SAMPLES} record_read calls in {:?} \
         ({:.2} ns per recorded operation)",
        elapsed,
        elapsed.as_nanos() as f64 / SAMPLES as f64
    );
}
