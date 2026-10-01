//! Opening a compact (V2) project reads metadata, not nodes or edges (#1388).
//!
//! Nodes and edges scale 16x together. Everything an open reads must then stay
//! bounded by descriptors: the manifest, route table and sidecars grow by a few
//! hundred bytes, while the identity runs alone grow by 40 bytes per node.
//!
//! This file holds exactly one test on purpose. `/proc/self/io` `rchar` counts
//! every read of the whole process, so a second test running on another thread
//! would be charged to this one.

use std::path::{Path, PathBuf};

use graphforge_api::{GraphForge, LifecycleIoCapture, lifecycle_io_snapshot};

#[path = "support/bulk_fixture.rs"]
mod bulk_fixture;

const SMALL_NODES: usize = 1_024;
const FAN_OUT: usize = 4;
const SCALE: usize = 16;

/// Bytes the whole process has asked the kernel to read.
fn rchar() -> u64 {
    std::fs::read_to_string("/proc/self/io")
        .expect("/proc/self/io")
        .lines()
        .find_map(|line| line.strip_prefix("rchar: "))
        .expect("rchar line")
        .trim()
        .parse()
        .expect("rchar value")
}

struct Open {
    nodes: usize,
    declared_payload: u64,
    rchar: u64,
    attributed: u64,
    checksummed: u64,
    copied: u64,
    identity_bytes: u64,
}

fn workspace(project: &Path) -> PathBuf {
    std::fs::read_dir(project)
        .expect("project directory")
        .map(|entry| entry.expect("entry").path())
        .find(|path| {
            path.file_name().is_some_and(|name| {
                name.to_string_lossy()
                    .starts_with("graphforge-graph-workspace-")
            })
        })
        .expect("hydrated workspace")
}

fn identity_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    for entry in std::fs::read_dir(dir).expect("directory") {
        let entry = entry.expect("entry");
        let metadata = entry.metadata().expect("metadata");
        if metadata.is_dir() {
            total += identity_bytes(&entry.path());
        } else if entry.path().extension().is_some_and(|ext| ext == "uuidx") {
            total += metadata.len();
        }
    }
    total
}

fn measure(nodes: usize) -> Open {
    let root = tempfile::tempdir().expect("project directory");
    let path = root.path().join("state");
    bulk_fixture::generate_bulk_graph_with_index(&path, nodes, FAN_OUT, false);
    let _capture = LifecycleIoCapture::install();
    let before = lifecycle_io_snapshot().expect("requested observation");
    let started = rchar();
    let forge = GraphForge::new(Some(path.to_str().expect("utf-8 path"))).expect("project opens");
    let opened = rchar() - started;
    let attributed = lifecycle_io_snapshot()
        .expect("requested observation")
        .since(&before)
        .expect("region")
        .totals
        .read_bytes;
    let evidence = forge.graph_open_evidence();
    // Not asserted: what the first queries cost after the open. A one-hop that
    // resolves a few identities reads their blocks; the first *ordered*
    // one-hop must also prove UUID order over every ordinal block (16 B/node).
    for query in [
        "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id LIMIT 3",
        "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 3",
        "MATCH (a)-[r]->(b) RETURN b.node_uuid AS id ORDER BY id LIMIT 3",
    ] {
        let started = rchar();
        forge.execute(query).expect("query");
        eprintln!(
            "  nodes={nodes} first-touch query read {}: {query}",
            rchar() - started
        );
    }
    let open = Open {
        nodes,
        declared_payload: evidence.bytes_validated,
        rchar: opened,
        attributed,
        checksummed: evidence.bytes_checksummed,
        copied: evidence.bytes_copied,
        identity_bytes: identity_bytes(&workspace(&path.parent().unwrap().join("state"))),
    };
    eprintln!(
        "nodes={} declared_payload={} identity_bytes={} rchar={} attributed={} checksummed={} copied={}",
        open.nodes,
        open.declared_payload,
        open.identity_bytes,
        open.rchar,
        open.attributed,
        open.checksummed,
        open.copied
    );
    open
}

#[test]
fn open_reads_metadata_when_nodes_and_edges_scale_sixteenfold() {
    let small = measure(SMALL_NODES);
    let large = measure(SMALL_NODES * SCALE);
    let added_nodes = (large.nodes - small.nodes) as u64;

    // The comparison means something only if the data really grew.
    assert!(
        large.declared_payload >= 10 * small.declared_payload,
        "payload {} -> {}",
        small.declared_payload,
        large.declared_payload
    );
    assert!(
        large.identity_bytes >= 10 * small.identity_bytes,
        "identity runs {} -> {}",
        small.identity_bytes,
        large.identity_bytes
    );

    // Growth under one byte per added node. Descriptors grow by about 0.2 B per
    // node (one block fence per 4,096 nodes plus the larger manifest). The
    // cheapest node-linear pass over identity runs costs at least 16 B per
    // node, a copy 40, the old open-time sweep 64.
    for (what, small, large) in [
        ("rchar", small.rchar, large.rchar),
        ("attributed reads", small.attributed, large.attributed),
        ("checksummed bytes", small.checksummed, large.checksummed),
        ("copied bytes", small.copied, large.copied),
    ] {
        let growth = large.saturating_sub(small);
        assert!(
            growth < added_nodes,
            "{what} grew {growth} bytes ({small} -> {large}) for {added_nodes} added nodes"
        );
    }
    // Attribution never claims more than the process actually read.
    assert!(small.attributed <= small.rchar && large.attributed <= large.rchar);
    // And what an open reads is a sliver of what it declares.
    assert!(
        large.rchar * 16 < large.declared_payload,
        "open read {} of {} declared bytes",
        large.rchar,
        large.declared_payload
    );
}
