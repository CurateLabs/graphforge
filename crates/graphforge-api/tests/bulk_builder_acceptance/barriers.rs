//! Every published object has exactly one file barrier (ADR 0058 decision 3;
//! #1881 criterion 5), inventoried on the public route.
//!
//! `BarrierAudit` records the inode each `fsync` of a process reached, with its
//! creation time, so a deleted file's inode number reused by a later file is not
//! mistaken for it. The
//! child process audits a whole validate and commit, then finds each published
//! object by inode and counts the file barriers it received. The producer's
//! barrier is the object's only one: installing it into the object store links
//! it without a second.

use std::collections::BTreeMap;

use serde_json::Value;

use super::child::{ChildSpec, Conditions, KILLED, run, spec_for};
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

/// An import into a project that already has a generation encodes through the
/// staged path (appends keep it), whose encoders are the same sealing writers.
/// Each object it newly publishes has exactly one file barrier too. An object
/// already in the store (identical content) is linked, not installed, and
/// received none in this process.
#[test]
fn every_object_an_appended_import_publishes_has_exactly_one_file_barrier() {
    let directory = tempfile::tempdir().unwrap();
    let project = empty_project(directory.path());
    let first = Sources::write(&directory.path().join("first"), wide_properties());
    let built = run(
        &ChildSpec {
            commit: true,
            ..spec_for(&project, &first)
        },
        &Conditions::default(),
    );
    assert!(built.succeeded(), "{built:?}");
    let before = stored_objects(&project);

    // Different identities, the same shapes: new nodes and edges to add.
    let second = Sources::write_at(
        &directory.path().join("second"),
        wide_properties(),
        10_000_000,
    );
    let appended = run(
        &ChildSpec {
            commit: true,
            audit: true,
            ..spec_for(&project, &second)
        },
        &Conditions::default(),
    );
    assert!(appended.succeeded(), "{appended:?}");
    assert!(
        appended.report().is_null(),
        "the append must take the staged path, not the bulk builder"
    );
    let published = published_digests(&project);
    let mut wrong = Vec::new();
    for (path, count) in appended.barriers()["objects"].as_object().unwrap() {
        let count = count.as_u64().unwrap();
        let expected = u64::from(!before.contains(&published[path]));
        if count != expected {
            wrong.push((path.clone(), count, expected));
        }
    }
    assert!(published.len() > 20, "{}", published.len());
    assert!(
        wrong.is_empty(),
        "objects (path, file barriers, expected) of an append: {wrong:#?}"
    );
}

/// The SHA-256 of every object in the store.
fn stored_objects(project: &std::path::Path) -> std::collections::BTreeSet<String> {
    let mut digests = std::collections::BTreeSet::new();
    let root = project.join("graph-objects/sha256");
    for bucket in std::fs::read_dir(root).unwrap() {
        let bucket = bucket.unwrap().path();
        for object in std::fs::read_dir(&bucket).unwrap() {
            digests.insert(format!(
                "{}{}",
                bucket.file_name().unwrap().to_str().unwrap(),
                object.unwrap().file_name().to_str().unwrap()
            ));
        }
    }
    digests
}

/// The digest of each artifact the latest session published, by path.
fn published_digests(project: &std::path::Path) -> BTreeMap<String, String> {
    inventory(project)
        .into_iter()
        .map(|artifact| (artifact.path, artifact.sha256))
        .collect()
}

/// A validated session is durable across processes: validation seals every
/// object before it pins the inventory, so a process that did not produce them
/// commits them without any barrier of their own.
///
/// Process A validates and is lost before `commit` (it exits abruptly, running
/// no destructor). Process B resumes the session and commits it.
#[test]
fn a_session_validated_in_one_process_commits_in_another_without_install_barriers() {
    let directory = tempfile::tempdir().unwrap();
    let sources = Sources::write(&directory.path().join("input"), wide_properties());

    let reference_project = empty_project(directory.path());
    let reference = run(
        &ChildSpec {
            commit: true,
            ..spec_for(&reference_project, &sources)
        },
        &Conditions::default(),
    );
    assert!(reference.succeeded(), "{reference:?}");

    let case = directory.path().join("case");
    std::fs::create_dir(&case).unwrap();
    let project = empty_project(&case);
    let validated = run(
        &ChildSpec {
            audit: true,
            abandon_after_validate: true,
            ..spec_for(&project, &sources)
        },
        &Conditions::default(),
    );
    assert_eq!(validated.code, Some(KILLED), "{validated:?}");
    // Validation alone gave every object its one barrier.
    let (histogram, offenders) = offenders(validated.barriers());
    assert!(
        offenders.is_empty(),
        "validate: objects by file barriers {histogram:?}; not exactly one: {offenders:#?}"
    );
    let session = validated.session();

    let committed = run(
        &ChildSpec {
            project: project.clone(),
            session: Some(session),
            commit: true,
            audit: true,
            ..ChildSpec::default()
        },
        &Conditions::default(),
    );
    assert!(committed.succeeded(), "{committed:?}");
    let report = committed.barriers();
    // Committing ran none: every object is linked at its address as it was.
    let installed = report["objects"]
        .as_object()
        .unwrap()
        .iter()
        .filter(|(_, count)| count.as_u64() != Some(0))
        .map(|(path, count)| (path.clone(), count.clone()))
        .collect::<Vec<_>>();
    assert!(
        installed.is_empty(),
        "commit made objects durable again: {installed:#?}"
    );
    assert!(report["objects"].as_object().unwrap().len() > 40);
    assert_eq!(report["current_file_barriers"], 1);
    assert_eq!(report["root_synced_after_current"], true);

    // And the bytes are those of a build that was never interrupted.
    assert_eq!(
        inventory_differences(&reference.inventory(), &committed.inventory()),
        Vec::<String>::new()
    );
}

/// The durable record that the producers synced the objects is part of the
/// digest that pins the inventory. An inventory an earlier binary wrote has no
/// such record, and its objects may be unsynced: committing it seals each at
/// install, with one file barrier of its own, and the bytes are the same.
///
/// The record is removed from a validated session's inventory, and the digest
/// that pins it re-derived in the checkpoint, exactly as the encoder derives it
/// (`inventory_authority_sha256`): the session is then authentic, with an
/// inventory that carries no record.
#[test]
fn a_session_pinned_without_the_seal_record_is_sealed_when_committed() {
    use sha2::{Digest, Sha256};

    let directory = tempfile::tempdir().unwrap();
    let sources = Sources::write(&directory.path().join("input"), wide_properties());
    let reference_project = empty_project(directory.path());
    let reference = run(
        &ChildSpec {
            commit: true,
            ..spec_for(&reference_project, &sources)
        },
        &Conditions::default(),
    );
    assert!(reference.succeeded(), "{reference:?}");

    let case = directory.path().join("case");
    std::fs::create_dir(&case).unwrap();
    let project = empty_project(&case);
    let validated = run(
        &ChildSpec {
            abandon_after_validate: true,
            ..spec_for(&project, &sources)
        },
        &Conditions::default(),
    );
    assert_eq!(validated.code, Some(KILLED), "{validated:?}");

    let root = construction_root(&project);
    let digest = |bytes: &[u8]| {
        let mut digest = Sha256::new();
        digest.update(b"graphforge-construction-encoding-inventory/v2\0");
        digest.update(bytes);
        digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let inventory_path = root.join("encoded-v1/inventory.json");
    let checkpoint_path = root.join("checkpoint.json");
    let inventory = std::fs::read_to_string(&inventory_path).unwrap();
    let checkpoint = std::fs::read_to_string(&checkpoint_path).unwrap();
    // The checkpoint pins exactly this serialization.
    let pinned = digest(inventory.as_bytes());
    assert!(checkpoint.contains(&pinned), "the pin is not this digest");
    let record = ",\"seal_protocol\":1";
    assert!(
        inventory.contains(record),
        "the inventory carries no record"
    );
    let stripped = inventory.replacen(record, "", 1);
    assert!(!stripped.contains("seal_protocol"));
    std::fs::write(&inventory_path, &stripped).unwrap();
    std::fs::write(
        &checkpoint_path,
        checkpoint.replacen(&pinned, &digest(stripped.as_bytes()), 1),
    )
    .unwrap();

    let committed = run(
        &ChildSpec {
            project: project.clone(),
            session: Some(validated.session()),
            commit: true,
            audit: true,
            ..ChildSpec::default()
        },
        &Conditions::default(),
    );
    assert!(committed.succeeded(), "{committed:?}");
    // Each object got exactly one file barrier, made when it was installed.
    let (histogram, offenders) = offenders(committed.barriers());
    assert!(
        offenders.is_empty(),
        "commit: objects by file barriers {histogram:?}; not exactly one: {offenders:#?}"
    );
    assert!(histogram.values().sum::<u64>() > 40, "{histogram:?}");
    assert_eq!(
        inventory_differences(&reference.inventory(), &committed.inventory()),
        Vec::<String>::new()
    );
}
