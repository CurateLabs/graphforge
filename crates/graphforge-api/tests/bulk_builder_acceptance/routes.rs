//! The resident, scratch and node-scratch routes each commit, reopen and
//! return the same exact counts and query answers, over the same bytes
//! (ADR 0058; #1881 criteria "reopened project" and "forced tiny budget").

use super::child::{ChildSpec, Conditions, Outcome, run, spec_for};
use super::support::*;

/// Property-bearing input, so the property scratch path runs on the way.
pub fn spec() -> Spec {
    Spec {
        nodes: 6_000,
        edges: 12_000,
        node_blobs: 20,
        edge_blobs: 20,
        blob_bytes: 24 << 10,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Resident,
    Scratch,
    NodeScratch,
}

/// A refused tiny budget names the resident bytes the scratch route needs
/// before it decodes anything: the least budget at which it runs.
pub fn scratch_floor(outcome: &Outcome) -> u64 {
    let refused = &outcome.lines["REFUSED"];
    assert_eq!(refused["resource_limit"], true, "{outcome:?}");
    let message = refused["message"].as_str().unwrap();
    message
        .split_once("scratch requires ")
        .and_then(|(_, rest)| rest.split_once(" resident bytes before decoding"))
        .map(|(bytes, _)| bytes.parse().unwrap())
        .unwrap_or_else(|| panic!("no scratch requirement in {message}"))
}

/// The least budget at which a build runs, found through the public refusal
/// of a budget too small for any route. A refusal names what the route needs
/// at the budget it was refused under; the requirement settles in a few steps.
pub fn floor_of(root: &std::path::Path, sources: &Sources) -> u64 {
    let mut budget = 256 << 20;
    for attempt in 0..8 {
        let probe = root.join(format!("probe-{attempt}"));
        std::fs::create_dir(&probe).unwrap();
        let outcome = run(
            &spec_for(&empty_project(&probe), sources),
            &Conditions {
                budget: Some(budget),
                ..Conditions::default()
            },
        );
        if outcome.lines.contains_key("VALIDATED") {
            return budget;
        }
        let needed = scratch_floor(&outcome);
        assert!(needed > budget, "{needed} is not above the budget {budget}");
        budget = needed;
    }
    panic!("the scratch requirement did not settle: {budget}");
}

/// The memory budget under which a build takes `route`.
pub fn budget_for(route: Route, floor: u64) -> Option<u64> {
    match route {
        Route::Resident => None,
        // At the floor nothing is left for node tables, which go to scratch.
        Route::NodeScratch => Some(floor),
        // Room for the node tables, none for a resident build.
        Route::Scratch => Some(floor + (64 << 20)),
    }
}

#[test]
fn every_route_reopens_to_the_same_counts_answers_and_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let spec = spec();
    let sources = Sources::write(&directory.path().join("input"), spec);

    let floor = floor_of(directory.path(), &sources);
    let mut results = Vec::new();
    for route in [Route::Resident, Route::Scratch, Route::NodeScratch] {
        let budget = budget_for(route, floor);
        let case = directory.path().join(format!("{route:?}"));
        std::fs::create_dir(&case).unwrap();
        let project = empty_project(&case);
        let outcome = run(
            &ChildSpec {
                commit: true,
                ..spec_for(&project, &sources)
            },
            &Conditions {
                budget,
                ..Conditions::default()
            },
        );
        assert!(outcome.succeeded(), "{route:?}: {outcome:?}");
        let report = outcome.report();
        let scratch = report["scratch_write_bytes"].as_u64().unwrap();
        let node_partitions = report["node_partitions"].as_u64().unwrap();
        match route {
            Route::Resident => assert_eq!((scratch, node_partitions), (0, 0), "{report}"),
            Route::Scratch => assert!(scratch > 0 && node_partitions == 0, "{report}"),
            Route::NodeScratch => assert!(scratch > 0 && node_partitions > 0, "{report}"),
        }
        if let Some(budget) = budget {
            assert!(
                outcome.peak_rss_bytes() <= budget,
                "{route:?}: peak {} against budget {budget}",
                outcome.peak_rss_bytes()
            );
        }

        let graph = graphforge_api::GraphForge::new(project.to_str()).unwrap();
        assert_eq!(
            counts(&graph),
            (spec.nodes as i64, spec.edges as i64),
            "{route:?}"
        );
        results.push((route, outcome.inventory(), answers(&graph)));
    }
    let (_, inventory, answers) = &results[0];
    assert!(inventory.len() > 40);
    for (route, other_inventory, other_answers) in &results[1..] {
        assert_eq!(
            inventory_differences(inventory, other_inventory),
            Vec::<String>::new(),
            "{route:?} published different bytes than the resident build"
        );
        assert_eq!(other_answers, answers, "{route:?}");
    }
}
