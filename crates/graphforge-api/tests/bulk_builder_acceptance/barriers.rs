//! One durability barrier per published object (ADR 0058 decision 3; #1881
//! criterion 5), inventoried on the public route.
//!
//! `BarrierAudit` records the inode every `fsync` of the process reached while
//! a build validates and commits. The published objects are then looked up by
//! inode in the object store, so the inventory is exact rather than a count.

use std::collections::BTreeMap;

use graphforge_api::GraphForge;
use graphforge_filesystem::observation::BarrierAudit;

use super::support::*;

type Inode = (u64, [u8; 16]);

fn inode(path: &std::path::Path) -> Inode {
    let identity = graphforge_filesystem::path_identity(path).unwrap();
    (identity.volume_serial, identity.file_id)
}

#[test]
fn every_published_object_has_exactly_one_file_barrier() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();
    let sources = Sources::write(
        &directory.path().join("input"),
        Spec {
            nodes: 3_000,
            edges: 5_000,
            node_blobs: 3,
            edge_blobs: 2,
            blob_bytes: 6 * MIB,
        },
    );
    let project = empty_project(directory.path());
    let graph = GraphForge::new(project.to_str()).unwrap();
    let mut session = register(&graph, &sources);

    let audit = BarrierAudit::start();
    validate(&graph, &mut session);
    session.commit(&graph, None).unwrap();
    let barriers = audit.finish();

    // Barrier positions per inode: `(sequence, is_directory)`.
    let mut per_inode: BTreeMap<Inode, Vec<(usize, bool)>> = BTreeMap::new();
    for (sequence, (identity, directory)) in barriers.iter().enumerate() {
        per_inode
            .entry((identity.volume_serial, identity.file_id))
            .or_default()
            .push((sequence, *directory));
    }
    let file_barriers = |inode: Inode| -> Vec<usize> {
        per_inode.get(&inode).map_or_else(Vec::new, |barriers| {
            barriers
                .iter()
                .filter(|(_, directory)| !directory)
                .map(|(sequence, _)| *sequence)
                .collect()
        })
    };
    let directory_barriers = |inode: Inode| -> Vec<usize> {
        per_inode.get(&inode).map_or_else(Vec::new, |barriers| {
            barriers
                .iter()
                .filter(|(_, directory)| *directory)
                .map(|(sequence, _)| *sequence)
                .collect()
        })
    };

    // Identical objects share one address, so count each address once.
    let mut published = inventory(&project);
    published.sort_by(|a, b| a.sha256.cmp(&b.sha256));
    published.dedup_by(|a, b| a.sha256 == b.sha256);
    assert!(published.len() > 40, "{}", published.len());
    let mut offenders = Vec::new();
    let mut unsynced_names = Vec::new();
    let mut last_object_barrier = 0;
    for artifact in &published {
        let object = object_path(&project, &artifact.sha256);
        let files = file_barriers(inode(&object));
        if files.len() != 1 {
            offenders.push((artifact.path.clone(), files.len()));
            continue;
        }
        last_object_barrier = last_object_barrier.max(files[0]);
        // ADR 0013: the address is acknowledged after the payload is durable.
        let bucket = inode(object.parent().unwrap());
        if !directory_barriers(bucket)
            .iter()
            .any(|sequence| *sequence > files[0])
        {
            unsynced_names.push(artifact.path.clone());
        }
    }
    assert!(
        offenders.is_empty(),
        "{} of {} published objects do not have exactly one file barrier: {offenders:#?}",
        offenders.len(),
        published.len()
    );
    assert!(
        unsynced_names.is_empty(),
        "objects whose bucket directory was not acknowledged after their file barrier: \
         {unsynced_names:#?}"
    );

    // The CURRENT swap: one barrier on the file that becomes CURRENT, then a
    // barrier on the project root that holds its new name.
    let current = file_barriers(inode(&project.join("CURRENT")));
    assert_eq!(current.len(), 1, "CURRENT file barriers: {current:?}");
    assert!(
        last_object_barrier < current[0],
        "an object was made durable after CURRENT: {last_object_barrier} vs {}",
        current[0]
    );
    let root = directory_barriers(inode(&project));
    assert!(
        root.iter().any(|sequence| *sequence > current[0]),
        "the project root was not synced after CURRENT: {root:?} vs {current:?}"
    );
}
