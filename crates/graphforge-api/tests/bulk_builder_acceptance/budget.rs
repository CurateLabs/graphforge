//! What an over-budget build writes to scratch (ADR 0058, "Scratch route" and
//! "Bounded property scratch"; #1881 criterion "Over budget, they add exactly
//! one scratch pass").
//!
//! The report of a public build names the bytes its scratch files took. The
//! topology bytes are the build's scratch total less the property bytes, which
//! the report counts separately and which are also in that total.

use serde_json::Value;

use super::child::{ChildSpec, Conditions, run, spec_for};
use super::routes::{Route, budget_for, floor_of};
use super::support::*;

fn field(report: &Value, name: &str) -> u64 {
    report[name]
        .as_u64()
        .unwrap_or_else(|| panic!("{name} in {report}"))
}

struct Measured {
    edges: u64,
    topology_write: u64,
    topology_read: u64,
    property_write: u64,
    property_read: u64,
    property_source: u64,
    runs: u64,
    fan_in: u64,
}

fn measure(report: &Value) -> Measured {
    let write = field(report, "scratch_write_bytes");
    let read = field(report, "scratch_read_bytes");
    let property_write = field(report, "property_scratch_write_bytes");
    let property_read = field(report, "property_scratch_read_bytes");
    // Nothing else moves through scratch: a balanced single-relation graph needs
    // no refinement and no relation spool, and its node tables are resident.
    for name in [
        "edge_refinement_write_bytes",
        "edge_refinement_read_bytes",
        "csr_spool_write_bytes",
        "csr_spool_read_bytes",
        "node_scratch_write_bytes",
        "endpoint_scratch_write_bytes",
    ] {
        assert_eq!(field(report, name), 0, "{name} in {report}");
    }
    Measured {
        edges: field(report, "edges"),
        topology_write: write - property_write,
        topology_read: read - property_read,
        property_write,
        property_read,
        property_source: field(report, "property_source_bytes"),
        runs: field(report, "property_runs"),
        fan_in: field(report, "property_merge_fan_in"),
    }
}

#[test]
fn over_budget_scratch_is_one_topology_pass_and_bounded_property_runs() {
    let directory = tempfile::tempdir().unwrap();
    let mut sizes = Vec::new();
    let mut budget = None;
    for (index, (nodes, edges)) in [(5_000, 10_000), (20_000, 80_000)].into_iter().enumerate() {
        let sources = Sources::write(
            &directory.path().join(format!("input-{index}")),
            Spec {
                nodes,
                edges,
                node_blobs: 20,
                edge_blobs: 20,
                blob_bytes: 24 << 10,
            },
        );
        let budget = *budget.get_or_insert_with(|| {
            budget_for(Route::Scratch, floor_of(directory.path(), &sources)).unwrap()
        });
        let project = directory.path().join(format!("project-{index}"));
        std::fs::create_dir(&project).unwrap();
        let outcome = run(
            &ChildSpec {
                commit: true,
                ..spec_for(&project, &sources)
            },
            &Conditions {
                budget: Some(budget),
                ..Conditions::default()
            },
        );
        assert!(outcome.succeeded(), "{outcome:?}");
        sizes.push(measure(outcome.report()));
    }

    for measured in &sizes {
        // The topology goes through scratch once: every byte written is read
        // back once, and no byte is written twice.
        assert!(measured.topology_write > 0);
        assert_eq!(
            measured.topology_write, measured.topology_read,
            "the topology scratch is not read exactly as written"
        );
        // 28 bytes per edge record, and at most 32 more per edge for the
        // adjacency entries of both directions, plus block headers (ADR 0058).
        let per_edge_ceiling = 28 + 32;
        assert!(
            measured.topology_write <= measured.edges * per_edge_ceiling + (64 << 10),
            "{} topology scratch bytes for {} edges",
            measured.topology_write,
            measured.edges
        );
        // Property runs are written once and merged once. Fewer runs than the
        // merge fan-in need no reduction level that would rewrite them.
        assert!(
            measured.runs > 0 && measured.runs <= measured.fan_in,
            "{} runs against a fan-in of {}",
            measured.runs,
            measured.fan_in
        );
    }
    // The property scratch is proportional to its input: a larger build does
    // not pay more scratch per source byte.
    let ratio = |bytes: u64, measured: &Measured| bytes as f64 / measured.property_source as f64;
    let (small, large) = (&sizes[0], &sizes[1]);
    assert!(
        ratio(large.property_write, large) <= ratio(small.property_write, small) * 1.1,
        "property scratch writes per source byte grew: {} vs {}",
        ratio(small.property_write, small),
        ratio(large.property_write, large)
    );
    assert!(
        ratio(large.property_read, large) <= ratio(small.property_read, small) * 1.1,
        "property scratch reads per source byte grew: {} vs {}",
        ratio(small.property_read, small),
        ratio(large.property_read, large)
    );
    for measured in &sizes {
        println!(
            "SCRATCH edges={} topology={} B/edge property_write/source={:.2} property_read/source={:.2}",
            measured.edges,
            measured.topology_write / measured.edges,
            ratio(measured.property_write, measured),
            ratio(measured.property_read, measured),
        );
    }
}
