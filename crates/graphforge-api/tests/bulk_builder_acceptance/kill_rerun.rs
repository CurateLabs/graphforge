//! A build killed in any pass reruns to the same published artifacts, and the
//! prior `CURRENT` survives until the swap (ADR 0058 decision 1; #1881).
//!
//! Each case runs the whole public route (`begin`, `register_parquet`,
//! `validate`, `commit`) in a child process that exits at one named failpoint.
//! A second child then resumes the same session. The storage layer proves the
//! same property below the facade; this proves it through it.

use std::path::Path;

use graphforge_api::{GraphForge, ImportPhase};

use super::child::{ChildSpec, Conditions, KILLED, run, spec_for};
use super::support::*;

/// Competing decode tasks are covered by the forced-worker test; this input is
/// small so every pass is killed and rerun quickly. It still carries property
/// fragments, one of them wider than a physical object.
fn spec() -> Spec {
    Spec {
        nodes: 2_500,
        edges: 4_000,
        node_blobs: 1,
        edge_blobs: 1,
        blob_bytes: 5 * MIB,
    }
}

/// Where a build is killed, and whether `CURRENT` has been swapped by then.
struct Kill {
    pass: &'static str,
    conditions: Conditions,
    swapped: bool,
}

fn construction(pass: &'static str, name: &str, swapped: bool) -> Kill {
    Kill {
        pass,
        conditions: Conditions {
            construction_failpoint: Some(name.to_owned()),
            ..Conditions::default()
        },
        swapped,
    }
}

fn project_point(pass: &'static str, name: &str, swapped: bool) -> Kill {
    Kill {
        pass,
        conditions: Conditions {
            project_failpoint: Some(name.to_owned()),
            ..Conditions::default()
        },
        swapped,
    }
}

fn current(project: &Path) -> Vec<u8> {
    std::fs::read(project.join("CURRENT")).unwrap()
}

/// Node and edge counts and the recorded query answers of a reopened project.
fn reopened(project: &Path) -> (u64, String) {
    let graph = GraphForge::new(project.to_str()).unwrap();
    (graph.node_count("Person").unwrap(), answers(&graph))
}

#[test]
fn a_process_killed_in_any_pass_reruns_to_identical_artifacts() {
    let directory = tempfile::tempdir().unwrap();
    let spec = spec();
    let sources = Sources::write(&directory.path().join("input"), spec);

    let reference_project = empty_project(directory.path());
    let reference = run(
        &ChildSpec {
            commit: true,
            ..spec_for(&reference_project, &sources)
        },
        &Conditions::default(),
    );
    assert!(reference.succeeded(), "{reference:?}");
    let expected = reference.inventory();
    assert!(expected.len() > 20, "{}", expected.len());
    let (expected_nodes, expected_answers) = reopened(&reference_project);
    assert_eq!(expected_nodes, spec.nodes as u64);

    let kills = [
        construction("plan", "bulk.after_plan", false),
        construction("nodes", "bulk.after_nodes", false),
        construction("edges", "bulk.after_edges", false),
        construction("edges+tables", "bulk.after_tables", false),
        construction("edges+ordinal", "bulk.after_ordinal", false),
        construction("edges+csr", "bulk.after_adjacency", false),
        construction("inventory", "bulk.before_inventory", false),
        construction(
            "inventory-pending-intent",
            "bulk.after_inventory_before_intent_removal",
            false,
        ),
        construction("inventory-pinned", "encode.after_inventory_pinned", false),
        construction(
            "publish-install",
            "cas.install.after_object_sync.topology/generation.json",
            false,
        ),
        construction(
            "publish-link",
            "cas.install.after_link.topology/generation.json",
            false,
        ),
        project_point(
            "publish-before-current",
            "project.before_current_replace",
            false,
        ),
        project_point(
            "publish-after-current",
            "project.after_current_replace",
            true,
        ),
        construction(
            "publish-after-current-before-receipt",
            "publication.after_current_before_receipt",
            true,
        ),
    ];
    for kill in kills {
        let case = directory.path().join(kill.pass);
        std::fs::create_dir(&case).unwrap();
        let project = empty_project(&case);
        // Opening creates the project's first generation: the prior CURRENT.
        drop(GraphForge::new(project.to_str()).unwrap());
        let prior = current(&project);

        let killed = run(
            &ChildSpec {
                commit: true,
                ..spec_for(&project, &sources)
            },
            &kill.conditions,
        );
        assert_eq!(killed.code, Some(KILLED), "{}: {killed:?}", kill.pass);
        assert!(!killed.succeeded(), "{}", kill.pass);
        let session = killed.session();

        // The swap is the only publication: before it the project is as it was,
        // after it the whole graph is there.
        let after_kill = current(&project);
        if kill.swapped {
            assert_ne!(after_kill, prior, "{}: CURRENT was not swapped", kill.pass);
        } else {
            assert_eq!(after_kill, prior, "{}: CURRENT changed", kill.pass);
            assert_eq!(reopened(&project).0, 0, "{}", kill.pass);
        }

        let rerun = run(
            &ChildSpec {
                project: project.clone(),
                session: Some(session),
                commit: true,
                ..ChildSpec::default()
            },
            &Conditions::default(),
        );
        assert!(rerun.succeeded(), "{}: {rerun:?}", kill.pass);
        assert_eq!(
            inventory_differences(&expected, &rerun.inventory()),
            Vec::<String>::new(),
            "{}: rerun published different bytes",
            kill.pass
        );
        assert_ne!(current(&project), prior, "{}", kill.pass);
        let graph = GraphForge::new(project.to_str()).unwrap();
        assert_eq!(
            graph.import_session_status(session).unwrap().0,
            ImportPhase::Committed,
            "{}: the resumed session did not finish",
            kill.pass
        );
        let (nodes, answers) = reopened(&project);
        assert_eq!(nodes, expected_nodes, "{}", kill.pass);
        assert_eq!(answers, expected_answers, "{}", kill.pass);
    }
}
