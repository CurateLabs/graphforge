//! A path operator hydrates each distinct node once, however many partitions
//! and batches its paths arrive in (#1984).
//!
//! `[n IN nodes(p) | n.ident]` resolves the node of every position of every
//! path. The paths of an unanchored variable-length expansion revisit the same
//! few nodes in every batch, and each partition used to resolve the nodes of
//! its own batches from storage again. The operator now keeps what it has
//! resolved for the length of one execution and shares it between its
//! partitions, so the nodes it asks storage for are the distinct nodes of its
//! paths and nothing is kept for the next query.
//!
//! The graph is a ring: node `i` links forward to `i + 1` and `i + 2`, so an
//! expansion of one to three hops yields 14 paths from every node, and every
//! node sits at 48 positions of those paths. A small batch size splits those paths into
//! many batches, which is where a per-batch resolution repeats itself. Each
//! case checks the values against the paths computed from the ring, so the
//! results are what resolving every batch separately gives, and checks the
//! `hydration_requested` counter against the distinct nodes of those paths.

use arrow::array::{Array, Int64Array, ListArray};
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use graphforge_api::{ExecutionResourcePolicy, GraphForge, GraphForgeOptions};
use graphforge_exec::demand;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

const NODES: i64 = 60;
const SMALL_BATCH: usize = 16;
const DEFAULT_BATCH: usize = 8192;

const IDENTS: &str = "MATCH p = (a:Entity)-[r:LINK*1..3]->(b:Entity) \
                      RETURN [n IN nodes(p) | n.ident] AS ids";
const TAGS: &str = "MATCH p = (a:Entity)-[r:LINK*1..3]->(b:Entity) \
                    RETURN [n IN nodes(p) | n.tag] AS tags";
const WHOLE: &str = "MATCH p = (a:Entity)-[r:LINK*1..3]->(b:Entity) \
                     RETURN a.ident AS start, nodes(p) AS ns";

type Counters = BTreeMap<String, u64>;

fn open(project: &Path, target_partitions: usize, batch_size: usize) -> GraphForge {
    GraphForge::new_with_options(
        Some(project.to_str().expect("utf-8 path")),
        GraphForgeOptions {
            resource: ExecutionResourcePolicy {
                target_partitions: Some(target_partitions),
                batch_size: Some(batch_size),
                ..ExecutionResourcePolicy::default()
            },
            ..GraphForgeOptions::default()
        },
    )
    .expect("open project")
}

fn tag(ident: i64) -> String {
    format!("node-{ident}")
}

/// A durable ring of [`NODES`] nodes, closed before it returns so that every
/// case opens the project afresh.
fn ring() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let project = dir.path().join("project");
    let forge = open(&project, 1, DEFAULT_BATCH);
    for statement in [
        format!(
            "UNWIND range(0, {}) AS i CREATE (:Entity {{ident: i, tag: 'node-' + toString(i)}})",
            NODES - 1
        ),
        format!(
            "MATCH (a:Entity), (b:Entity) \
             WHERE b.ident = (a.ident + 1) % {NODES} OR b.ident = (a.ident + 2) % {NODES} \
             CREATE (a)-[:LINK]->(b)"
        ),
    ] {
        forge
            .execute(&statement)
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    drop(forge);
    (dir, project)
}

/// Every path of one to three forward hops over the nodes that still exist, as
/// the identifiers of its nodes.
fn paths(deleted: &BTreeSet<i64>) -> Vec<Vec<i64>> {
    let mut found = Vec::new();
    let mut frontier = (0..NODES)
        .filter(|node| !deleted.contains(node))
        .map(|node| vec![node])
        .collect::<Vec<_>>();
    for _ in 0..3 {
        frontier = frontier
            .iter()
            .flat_map(|path| {
                let last = *path.last().expect("a path has a node");
                [1, 2]
                    .map(|step| (last + step) % NODES)
                    .into_iter()
                    .filter(|next| !deleted.contains(next))
                    .map(|next| {
                        let mut longer = path.clone();
                        longer.push(next);
                        longer
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        found.extend(frontier.iter().cloned());
    }
    found
}

fn distinct(paths: &[Vec<i64>]) -> u64 {
    paths.iter().flatten().collect::<BTreeSet<_>>().len() as u64
}

fn count(counters: &Counters, name: &str) -> u64 {
    counters
        .get(&format!("hydration_{name}"))
        .copied()
        .unwrap_or(0)
}

fn run(forge: &GraphForge, query: &str) -> (Vec<RecordBatch>, Counters) {
    let (result, snapshot) = demand::capture(|| forge.execute(query));
    let result = result.unwrap_or_else(|e| panic!("{query}: {e}"));
    (result.batches, snapshot.hydration)
}

/// The rows of a result, rendered and sorted: partitions return rows in no
/// particular order.
fn rendered_rows(batches: &[RecordBatch]) -> Vec<String> {
    let options = FormatOptions::default().with_null("NULL");
    let mut rows = Vec::new();
    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|column| ArrayFormatter::try_new(column.as_ref(), &options).expect("formatter"))
            .collect::<Vec<_>>();
        for row in 0..batch.num_rows() {
            rows.push(
                formatters
                    .iter()
                    .map(|formatter| formatter.value(row).to_string())
                    .collect::<Vec<_>>()
                    .join(" | "),
            );
        }
    }
    rows.sort();
    rows
}

fn id_lists(batches: &[RecordBatch]) -> Vec<Vec<Option<i64>>> {
    let mut lists = Vec::new();
    for batch in batches {
        let ids = batch
            .column_by_name("ids")
            .expect("ids column")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("ids is a list");
        for row in 0..batch.num_rows() {
            let values = ids.value(row);
            let values = values
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ident is Int64");
            lists.push(
                (0..values.len())
                    .map(|index| (!values.is_null(index)).then(|| values.value(index)))
                    .collect(),
            );
        }
    }
    lists.sort();
    lists
}

fn expected_id_lists(
    paths: &[Vec<i64>],
    ident_of: impl Fn(i64) -> Option<i64>,
) -> Vec<Vec<Option<i64>>> {
    let mut lists = paths
        .iter()
        .map(|path| path.iter().map(|node| ident_of(*node)).collect())
        .collect::<Vec<Vec<_>>>();
    lists.sort();
    lists
}

/// The partition counts and batch sizes every case runs at: one partition and
/// four, with batches far smaller than the paths and one batch for all of them.
const SHAPES: [(usize, usize); 4] = [
    (1, SMALL_BATCH),
    (4, SMALL_BATCH),
    (4, DEFAULT_BATCH),
    (1, DEFAULT_BATCH),
];

/// The nodes the operator asked storage for are the distinct nodes of its
/// paths, whatever the partition count or batch size, and the repeats were
/// answered from what it had already resolved.
#[test]
fn every_distinct_path_node_is_resolved_once_per_operator() {
    let (_dir, project) = ring();
    let all = paths(&BTreeSet::new());
    assert_eq!(distinct(&all), NODES as u64);
    let positions = all.iter().map(Vec::len).sum::<usize>() as u64;
    for (partitions, batch_size) in SHAPES {
        let forge = open(&project, partitions, batch_size);
        let (batches, counters) = run(&forge, IDENTS);
        let context = format!("{partitions} partitions, batches of {batch_size}: {counters:?}");
        assert_eq!(
            id_lists(&batches),
            expected_id_lists(&all, Some),
            "{context}"
        );
        assert_eq!(count(&counters, "requested"), NODES as u64, "{context}");
        assert_eq!(count(&counters, "shared_bypass"), 0, "{context}");
        if batch_size == SMALL_BATCH {
            assert!(
                count(&counters, "shared_hits") > 0,
                "{positions} positions in many batches must reuse nodes: {context}"
            );
        }
    }
}

/// `[n IN nodes(p) | n.tag]` over a string property.
#[test]
fn a_property_projection_is_the_same_at_every_partition_count() {
    let (_dir, project) = ring();
    let all = paths(&BTreeSet::new());
    let mut expected = all
        .iter()
        .map(|path| {
            format!(
                "[{}]",
                path.iter().map(|node| tag(*node)).collect::<Vec<_>>().join(", ")
            )
        })
        .collect::<Vec<_>>();
    expected.sort();
    for (partitions, batch_size) in SHAPES {
        let forge = open(&project, partitions, batch_size);
        let (batches, counters) = run(&forge, TAGS);
        let context = format!("{partitions} partitions, batches of {batch_size}: {counters:?}");
        assert_eq!(rendered_rows(&batches), expected, "{context}");
        assert_eq!(count(&counters, "requested"), NODES as u64, "{context}");
    }
}

/// `nodes(p)` taken whole hydrates the labels and every property, and the
/// rows are the same whichever way the paths are split into partitions and
/// batches.
#[test]
fn whole_nodes_are_the_same_at_every_partition_count() {
    let (_dir, project) = ring();
    let reference = {
        let forge = open(&project, 1, DEFAULT_BATCH);
        let (batches, _) = run(&forge, WHOLE);
        let rows = rendered_rows(&batches);
        assert_eq!(rows.len(), paths(&BTreeSet::new()).len());
        assert!(
            rows.iter().all(|row| row.contains("Entity")),
            "labels are hydrated: {rows:?}"
        );
        rows
    };
    for (partitions, batch_size) in SHAPES {
        let forge = open(&project, partitions, batch_size);
        let (batches, counters) = run(&forge, WHOLE);
        let context = format!("{partitions} partitions, batches of {batch_size}: {counters:?}");
        assert_eq!(rendered_rows(&batches), reference, "{context}");
        assert_eq!(count(&counters, "requested"), NODES as u64, "{context}");
    }
}

/// A node that was deleted is neither a node of any path nor resolved for one,
/// and the nodes next to it are still resolved once each.
#[test]
fn deleted_nodes_are_not_resolved_and_the_rest_still_are_once() {
    let (_dir, project) = ring();
    let deleted = BTreeSet::from([7_i64, 8, 30]);
    let forge = open(&project, 1, DEFAULT_BATCH);
    forge
        .execute("MATCH (n:Entity) WHERE n.ident IN [7, 8, 30] DETACH DELETE n")
        .expect("deletion");
    drop(forge);
    let survivors = paths(&deleted);
    let expected = expected_id_lists(&survivors, Some);
    let nodes = distinct(&survivors);
    assert_eq!(nodes, NODES as u64 - deleted.len() as u64);
    for (partitions, batch_size) in SHAPES {
        let forge = open(&project, partitions, batch_size);
        let (batches, counters) = run(&forge, IDENTS);
        let context = format!("{partitions} partitions, batches of {batch_size}: {counters:?}");
        assert_eq!(id_lists(&batches), expected, "{context}");
        assert_eq!(count(&counters, "requested"), nodes, "{context}");
        let (whole, counters) = run(&forge, WHOLE);
        let rows = rendered_rows(&whole);
        assert_eq!(rows.len(), survivors.len(), "{context}");
        assert_eq!(count(&counters, "requested"), nodes, "{context}");
    }
}

/// What an operator resolved ends with its execution: an update between two
/// runs of the same statement shows in the second, which resolves every node
/// again.
#[test]
fn nothing_resolved_for_one_query_is_kept_for_the_next() {
    let (_dir, project) = ring();
    let all = paths(&BTreeSet::new());
    let forge = open(&project, 4, SMALL_BATCH);
    let (batches, counters) = run(&forge, IDENTS);
    assert_eq!(id_lists(&batches), expected_id_lists(&all, Some));
    assert_eq!(count(&counters, "requested"), NODES as u64, "{counters:?}");
    forge
        .execute("MATCH (a:Entity {ident: 7}) SET a.ident = 7000")
        .expect("update");
    forge
        .execute("MATCH (a:Entity {ident: 8}) REMOVE a.ident")
        .expect("removal");
    let ident_of = |node: i64| match node {
        7 => Some(7000),
        8 => None,
        other => Some(other),
    };
    for _ in 0..2 {
        let (batches, counters) = run(&forge, IDENTS);
        assert_eq!(
            id_lists(&batches),
            expected_id_lists(&all, ident_of),
            "{counters:?}"
        );
        assert_eq!(count(&counters, "requested"), NODES as u64, "{counters:?}");
    }
}
