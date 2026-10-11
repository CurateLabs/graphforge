//! Every published object has exactly one file barrier (ADR 0058 decision 3;
//! #1881 criterion 5), inventoried on the public route.
//!
//! `BarrierAudit` records the inode each `fsync` of a process reached. The
//! child process audits a whole validate and commit, then finds each published
//! object by inode and counts the file barriers it received. The producer's
//! barrier is the object's only one: installing it into the object store links
//! it without a second.

use std::collections::BTreeMap;

use serde_json::Value;

use super::child::{ChildSpec, Conditions, run, spec_for};
use super::routes::{Route, budget_for, floor_of};
use super::support::*;

/// Property fragments wider than one physical object, so multi-part objects are
/// among those inventoried.
pub fn wide_properties() -> Spec {
    Spec {
        nodes: 3_000,
        edges: 5_000,
        node_blobs: 3,
        edge_blobs: 2,
        blob_bytes: 6 * MIB,
        scattered_edges: true,
    }
}

/// Objects by the number of file barriers they received, with the paths of
/// those that did not receive exactly one.
pub fn offenders(report: &Value) -> (BTreeMap<u64, u64>, Vec<(String, u64)>) {
    let mut histogram = BTreeMap::new();
    let mut offenders = Vec::new();
    for (path, count) in report["objects"].as_object().unwrap() {
        let count = count.as_u64().unwrap();
        *histogram.entry(count).or_insert(0) += 1;
        if count != 1 {
            offenders.push((path.clone(), count));
        }
    }
    (histogram, offenders)
}

#[test]
fn every_published_object_has_exactly_one_file_barrier_on_every_route() {
    let directory = tempfile::tempdir().unwrap();
    let sources = Sources::write(&directory.path().join("input"), wide_properties());
    let floor = floor_of(directory.path(), &sources);
    for route in [Route::Resident, Route::Scratch] {
        let case = directory.path().join(format!("{route:?}"));
        std::fs::create_dir(&case).unwrap();
        let project = empty_project(&case);
        let outcome = run(
            &ChildSpec {
                commit: true,
                audit: true,
                ..spec_for(&project, &sources)
            },
            &Conditions {
                budget: budget_for(route, floor),
                ..Conditions::default()
            },
        );
        assert!(outcome.succeeded(), "{route:?}: {outcome:?}");
        let scratch = outcome.report()["scratch_write_bytes"].as_u64().unwrap();
        assert_eq!(
            scratch > 0,
            route == Route::Scratch,
            "{route:?} took another route"
        );

        let report = outcome.barriers();
        let (histogram, offenders) = offenders(report);
        println!("{route:?}: objects by file barriers {histogram:?}");
        assert!(histogram.values().sum::<u64>() > 40, "{histogram:?}");
        assert!(
            offenders.is_empty(),
            "{route:?}: objects by file barriers {histogram:?}; not exactly one: {offenders:#?}"
        );
        // ADR 0013: each address is acknowledged after its payload is durable,
        // and `CURRENT` is swapped after every object, with one barrier of its
        // own and the project root synced after it.
        assert_eq!(report["buckets_acknowledged"], true, "{route:?}");
        assert_eq!(report["current_file_barriers"], 1, "{route:?}");
        assert_eq!(report["objects_before_current"], true, "{route:?}");
        assert_eq!(report["root_synced_after_current"], true, "{route:?}");
    }
}
