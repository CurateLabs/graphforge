//! Each published byte is written once (ADR 0058 decision 3; #1881 criterion 4).

use graphforge_api::GraphForge;
use graphforge_storage::concurrency_attribution::RegionCapture;
use graphforge_storage::property_overlay::MAX_PROPERTY_OBJECT_BYTES;

use super::support::*;

/// A within-budget build whose property fragments are wider than one physical
/// object: three nodes and two edges each carry a 6 MiB value that does not
/// compress, so each fragment's Parquet stream spans two bounded objects.
fn wide_properties() -> Spec {
    Spec {
        nodes: 3_000,
        edges: 5_000,
        node_blobs: 3,
        edge_blobs: 2,
        blob_bytes: 6 * MIB,
    }
}

#[test]
fn construction_writes_equal_published_bytes_plus_control_files() {
    let _serial = serial();
    let directory = tempfile::tempdir().unwrap();
    let sources = Sources::write(&directory.path().join("input"), wide_properties());
    let project = empty_project(directory.path());
    let graph = GraphForge::new(project.to_str()).unwrap();
    let mut session = register(&graph, &sources);

    let capture = RegionCapture::start("import");
    let progress = validate(&graph, &mut session);
    let regions = capture.finish();

    let published = inventory(&project);
    let bulk = progress
        .construction
        .as_ref()
        .and_then(|construction| construction.bulk_build.as_ref())
        .expect("the import ran on the bulk builder");
    assert_eq!(
        (bulk.scratch_write_bytes, bulk.property_scratch_write_bytes),
        (0, 0),
        "the build must fit its budget: no scratch"
    );
    let objects = published
        .iter()
        .filter(|artifact| artifact.path.contains("properties/"))
        .collect::<Vec<_>>();
    assert!(
        objects
            .iter()
            .any(|artifact| artifact.path.contains(".part-")),
        "the fixture must span several physical objects: {objects:?}"
    );
    assert!(
        objects
            .iter()
            .all(|artifact| artifact.bytes <= MAX_PROPERTY_OBJECT_BYTES as u64),
        "{objects:?}"
    );

    let published_bytes = published.iter().map(|artifact| artifact.bytes).sum::<u64>();
    let encoding = regions
        .regions
        .iter()
        .find(|(path, _)| path.ends_with("/canonical_encoding"))
        .expect("the encoding region")
        .1;
    let written = encoding.inclusive.written_bytes.expect("write counter");
    // While the objects are encoded the only other writes are the two control
    // files: the inventory, which stays, and the encoding intent (a few hundred
    // bytes), which is removed when the inventory is complete.
    let root = construction_root(&project);
    let inventory_bytes = std::fs::metadata(root.join("encoded-v1/inventory.json"))
        .unwrap()
        .len();
    let intent_bytes = written
        .checked_sub(published_bytes + inventory_bytes)
        .unwrap_or_else(|| {
            panic!(
                "{written} bytes written for {published_bytes} published and \
                 {inventory_bytes} inventory bytes"
            )
        });
    assert!(
        intent_bytes < 1 << 10,
        "encoding wrote {written} bytes for {published_bytes} published bytes and a \
         {inventory_bytes}-byte inventory: {intent_bytes} bytes are neither"
    );

    // Everything validate wrote, session bookkeeping included, adds control
    // files only: far less than one extra copy of any object.
    let total = regions
        .regions
        .iter()
        .find(|(path, _)| path.ends_with("/stage+seal"))
        .expect("the validate region")
        .1
        .inclusive
        .written_bytes
        .expect("write counter");
    let control = total
        .checked_sub(published_bytes)
        .expect("validate wrote less than it published");
    assert!(
        control < published_bytes / 50,
        "validate wrote {total} bytes for {published_bytes} published bytes: \
         {control} bytes of control files"
    );
}
