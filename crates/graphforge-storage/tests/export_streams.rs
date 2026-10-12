//! The projected streaming readers behind analyst exports return exactly what
//! the table readers do, and nothing else is decoded.

use std::collections::HashMap;

use arrow::array::{Array, FixedSizeBinaryArray, UInt64Array};
use graphforge_core::OntologyMode;
use graphforge_core::uuid::{Uuid, to_bytes};
use graphforge_ir::IrLiteral;
use graphforge_storage::{
    GraphWriter, TopologyFiles, visit_edge_identities, visit_edge_property_batches,
    visit_node_identities,
};
use graphforge_value::EntityTypeId;
use tempfile::TempDir;

const TS: i64 = 1_700_000_000_000_000;

fn label(id: u32) -> EntityTypeId {
    EntityTypeId::decode(id).unwrap()
}

fn uuid(base: u128, index: usize) -> Uuid {
    Uuid::from_u128(base | (index as u128 + 1))
}

struct Fixture {
    dir: TempDir,
    nodes: Vec<(u64, [u8; 16], Vec<u32>)>,
    edges: Vec<(u64, [u8; 16], &'static str)>,
}

/// Six nodes over two labels (node 2 holds both) and edges of two relations.
fn fixture(mode: OntologyMode) -> Fixture {
    let dir = TempDir::new().unwrap();
    let mut writer = GraphWriter::open_at(dir.path(), mode, TS).unwrap();
    let mut nodes = Vec::new();
    for index in 0..6 {
        let labels = match index % 3 {
            0 => vec![label(1)],
            1 => vec![label(2)],
            _ => vec![label(1), label(2)],
        };
        let id = writer
            .create_node_with_labels(uuid(0x40 << 120, index), &labels)
            .unwrap();
        nodes.push((
            id,
            to_bytes(&uuid(0x40 << 120, index)),
            labels.iter().map(|label| label.encode()).collect(),
        ));
    }
    let mut edges = Vec::new();
    for index in 0..9 {
        let relation = if index % 2 == 0 { "KNOWS" } else { "LIKES" };
        let edge = uuid(0x41 << 120, index);
        let id = writer
            .create_edge(
                edge,
                relation,
                &uuid(0x40 << 120, index % 6),
                &uuid(0x40 << 120, (index + 1) % 6),
            )
            .unwrap();
        writer
            .set_edge_properties(
                &edge,
                Some(relation),
                HashMap::from([("cost".to_owned(), IrLiteral::Float(index as f64 + 0.5))]),
            )
            .unwrap();
        edges.push((id, to_bytes(&edge), relation));
    }
    writer.flush().unwrap();
    Fixture { dir, nodes, edges }
}

fn files(dir: &std::path::Path) -> TopologyFiles {
    TopologyFiles::discover_legacy(dir).unwrap()
}

fn selected(files: &TopologyFiles, label: Option<EntityTypeId>) -> Vec<(u64, [u8; 16])> {
    let mut rows = Vec::new();
    visit_node_identities(files, label, |id, uuid| {
        rows.push((id, *uuid));
        Ok(())
    })
    .unwrap();
    rows
}

#[test]
fn node_identities_filter_by_label_membership_in_file_order() {
    let fixture = fixture(OntologyMode::Strict);
    let files = files(fixture.dir.path());
    let expect = |wanted: Option<u32>| {
        fixture
            .nodes
            .iter()
            .filter(|(_, _, labels)| wanted.is_none_or(|wanted| labels.contains(&wanted)))
            .map(|(id, uuid, _)| (*id, *uuid))
            .collect::<Vec<_>>()
    };
    assert_eq!(selected(&files, None), expect(None));
    assert_eq!(
        selected(&files, Some(label(1))),
        expect(Some(label(1).encode()))
    );
    assert_eq!(
        selected(&files, Some(label(2))),
        expect(Some(label(2).encode()))
    );
    assert_eq!(selected(&files, Some(label(1))).len(), 4);
    assert!(selected(&files, Some(label(3))).is_empty());
}

#[test]
fn node_identities_stop_on_a_visitor_error() {
    let fixture = fixture(OntologyMode::Strict);
    let files = files(fixture.dir.path());
    let mut seen = 0;
    let error = visit_node_identities(&files, None, |_, _| {
        seen += 1;
        if seen == 2 {
            Err(graphforge_core::GfError::Validation("stop".into()))
        } else {
            Ok(())
        }
    })
    .unwrap_err();
    assert_eq!(seen, 2);
    assert!(matches!(error, graphforge_core::GfError::Validation(_)));
}

fn identities(files: &TopologyFiles, relation: &str, mode: OntologyMode) -> Vec<(u64, [u8; 16])> {
    let mut rows = Vec::new();
    visit_edge_identities(
        files,
        relation,
        mode,
        |ids: &UInt64Array, uuids: &FixedSizeBinaryArray| {
            for row in 0..ids.len() {
                rows.push((ids.value(row), uuids.value(row).try_into().unwrap()));
            }
            Ok(())
        },
    )
    .unwrap();
    rows.sort_unstable();
    rows
}

#[test]
fn edge_identities_cover_the_files_a_relation_is_built_from() {
    let fixture = fixture(OntologyMode::Strict);
    let files = files(fixture.dir.path());
    let only = |relation: &str| {
        let mut rows: Vec<_> = fixture
            .edges
            .iter()
            .filter(|(_, _, name)| *name == relation)
            .map(|(id, uuid, _)| (*id, *uuid))
            .collect();
        rows.sort_unstable();
        rows
    };
    assert_eq!(
        identities(&files, "KNOWS", OntologyMode::Strict),
        only("KNOWS")
    );
    assert_eq!(
        identities(&files, "LIKES", OntologyMode::Strict),
        only("LIKES")
    );
    let mut all: Vec<_> = fixture
        .edges
        .iter()
        .map(|(id, uuid, _)| (*id, *uuid))
        .collect();
    all.sort_unstable();
    assert_eq!(identities(&files, "*", OntologyMode::Strict), all);
    assert!(identities(&files, "ABSENT", OntologyMode::Strict).is_empty());
}

#[test]
fn exploratory_edge_identities_read_the_shared_file() {
    let fixture = fixture(OntologyMode::Exploratory);
    let files = files(fixture.dir.path());
    let mut all: Vec<_> = fixture
        .edges
        .iter()
        .map(|(id, uuid, _)| (*id, *uuid))
        .collect();
    all.sort_unstable();
    // The shared file holds every relation; the caller matches by edge id.
    assert_eq!(identities(&files, "KNOWS", OntologyMode::Exploratory), all);
}

#[test]
fn edge_identity_reads_are_counted_as_edge_table_reads() {
    let fixture = fixture(OntologyMode::Strict);
    let files = files(fixture.dir.path());
    let _capture = graphforge_storage::io_stats::CaptureScope::install();
    let before = graphforge_storage::io_stats::snapshot().unwrap();
    let rows = identities(&files, "*", OntologyMode::Strict);
    let after = graphforge_storage::io_stats::snapshot().unwrap();
    assert_eq!(
        after.edge_full_rows - before.edge_full_rows,
        rows.len() as u64
    );
    assert!(after.edge_full_reads > before.edge_full_reads);
}

#[test]
fn edge_property_batches_match_the_projected_table_read() {
    let fixture = fixture(OntologyMode::Strict);
    let requested = ["cost".to_owned()];
    let mut streamed = Vec::new();
    visit_edge_property_batches(fixture.dir.path(), None, "KNOWS", &requested, |batch| {
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            ["edge_uuid", "cost"],
            "only the key and the requested property are decoded"
        );
        streamed.push(batch.clone());
        Ok(())
    })
    .unwrap();
    let table =
        graphforge_storage::read_edge_properties_projected(fixture.dir.path(), "KNOWS", &requested)
            .unwrap();
    let flatten = |batches: &[arrow::record_batch::RecordBatch]| {
        let mut rows = Vec::new();
        for batch in batches {
            let uuids = batch
                .column_by_name("edge_uuid")
                .unwrap()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            let costs = batch
                .column_by_name("cost")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((uuids.value(row).to_vec(), costs.value(row).to_bits()));
            }
        }
        rows.sort_unstable();
        rows
    };
    assert_eq!(flatten(&streamed), flatten(&table));
    assert_eq!(flatten(&streamed).len(), 5);
}

#[test]
fn edge_property_visitor_errors_are_returned_unchanged() {
    let fixture = fixture(OntologyMode::Strict);
    let error = visit_edge_property_batches(
        fixture.dir.path(),
        None,
        "KNOWS",
        &["cost".to_owned()],
        |_| Err(graphforge_core::GfError::Validation("refused".into())),
    )
    .unwrap_err();
    assert!(matches!(error, graphforge_core::GfError::Validation(_)));
}
