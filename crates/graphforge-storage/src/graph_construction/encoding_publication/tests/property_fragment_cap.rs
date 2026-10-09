//! Construction cuts property fragments at the fixed cap (#1388).

use super::*;
use crate::property_overlay::fragment_cap::tests::{assert_capped_fragments, wide_value};
use crate::property_overlay::{PropertyFragment, PropertyFragmentId, MAX_PROPERTY_FRAGMENT_BYTES};
use std::collections::BTreeMap;

const PAYLOAD_BYTES: usize = 4096;

fn wide_node_batch(first: u128, rows: usize) -> RecordBatch {
    let uuids = (first..first + rows as u128)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let mut fields = CONSTRUCTION_NODE_SCHEMA
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("payload", DataType::Utf8, true));
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(fixed(&uuids)),
            Arc::new(StringArray::from(vec!["Person"; rows])),
            Arc::new(StringArray::from_iter_values(
                (0..rows as u64).map(|row| wide_value(first as u64 + row, PAYLOAD_BYTES)),
            )),
        ],
    )
    .unwrap()
}

fn wide_edge_batch(first: u128, rows: usize) -> RecordBatch {
    let edges = (first..first + rows as u128)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let src = (1..=rows as u128)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let dst = (2..=rows as u128 + 1)
        .map(u128::to_be_bytes)
        .collect::<Vec<_>>();
    let mut fields = CONSTRUCTION_EDGE_SCHEMA
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("payload", DataType::Utf8, true));
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(fixed(&edges)),
            Arc::new(StringArray::from(vec!["R"; rows])),
            Arc::new(fixed(&src)),
            Arc::new(fixed(&dst)),
            Arc::new(StringArray::from_iter_values(
                (0..rows as u64).map(|row| wide_value(first as u64 + row, PAYLOAD_BYTES)),
            )),
        ],
    )
    .unwrap()
}

struct Encoded {
    graph: std::path::PathBuf,
    artifacts: Vec<crate::graph_construction_encoding::ConstructionEncodedArtifact>,
}

/// Encode three node batches and three edge batches, each about 1.2 times the
/// byte cap, into a fresh root.
fn encode_wide(operation: u128, batch_rows: usize) -> (TempDir, Encoded) {
    let root = TempDir::new().unwrap();
    let mut session = open(&root, operation);
    for batch in 0..3 {
        let first = 1 + (batch * batch_rows) as u128;
        session
            .append(
                ConstructionChunkKind::Node,
                &format!("nodes-{batch}"),
                &wide_node_batch(first, batch_rows),
            )
            .unwrap();
    }
    for batch in 0..3 {
        let first = 1 + (batch * batch_rows) as u128;
        session
            .append(
                ConstructionChunkKind::Edge,
                &format!("edges-{batch}"),
                &wide_edge_batch(100_000 + first, batch_rows),
            )
            .unwrap();
    }
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let encoded = session.encode_canonical(&shape, 1).unwrap();
    let graph = construction_session_root(&root, Uuid::from_u128(operation))
        .join(&encoded.root)
        .join("graph");
    (
        root,
        Encoded {
            graph,
            artifacts: encoded.artifacts,
        },
    )
}

fn fragments_under(encoded: &Encoded, subdir: &str) -> Vec<PropertyFragment> {
    let mut fragments = encoded
        .artifacts
        .iter()
        .filter(|artifact| artifact.path.starts_with(subdir))
        .map(|artifact| {
            let path = encoded.graph.join(&artifact.path);
            let id =
                PropertyFragmentId::parse(path.file_name().and_then(|name| name.to_str()).unwrap())
                    .unwrap();
            assert_eq!(artifact.bytes, std::fs::metadata(&path).unwrap().len());
            PropertyFragment { id, path }
        })
        .collect::<Vec<_>>();
    fragments.sort_by_key(|fragment| fragment.id);
    fragments
}

#[test]
fn construction_cuts_node_and_edge_property_fragments_at_the_cap() {
    // 1,200 rows x 4 KiB is about 4.9 MiB per batch, over the 4 MiB cap.
    let rows = 1_200;
    let (_root, encoded) = encode_wide(9_901, rows);
    for subdir in ["properties/", "edge_properties/"] {
        let fragments = fragments_under(&encoded, subdir);
        let stats = assert_capped_fragments(&fragments, 3 * rows);
        let logical = stats.iter().map(|stat| stat.logical_bytes).sum::<u64>();
        assert!(
            logical > 3 * MAX_PROPERTY_FRAGMENT_BYTES,
            "{subdir}: the input must exceed the cap or the test proves nothing: {logical}"
        );
        // The route cannot fit in fewer fragments than its bytes divided by the cap.
        assert!(
            fragments.len() as u64 >= logical.div_ceil(MAX_PROPERTY_FRAGMENT_BYTES),
            "{subdir}: {fragments:?}"
        );
        assert!(fragments.len() >= 4, "{subdir}: {fragments:?}");
        let generations = fragments
            .iter()
            .map(|fragment| fragment.id.generation)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(generations.len(), 1);
    }
    // The split changes no answer: every value reads back.
    let nodes = crate::read_properties(&encoded.graph, "_untyped").unwrap();
    let values = nodes
        .iter()
        .map(|batch| {
            batch
                .column_by_name("payload")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|value| value.unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .concat();
    assert_eq!(values.len(), 3 * rows);
    let mut expected = (0..3 * rows as u64)
        .map(|row| wide_value(row + 1, PAYLOAD_BYTES))
        .collect::<Vec<_>>();
    expected.sort();
    let mut values = values;
    values.sort();
    assert_eq!(values, expected);
}

#[test]
fn identical_constructions_publish_identical_property_fragments() {
    let digests = |operation| {
        let (_root, encoded) = encode_wide(operation, 1_200);
        let digests = encoded
            .artifacts
            .iter()
            .filter(|artifact| {
                artifact.path.starts_with("properties/")
                    || artifact.path.starts_with("edge_properties/")
            })
            .map(|artifact| (artifact.path.clone(), artifact.sha256.clone()))
            .collect::<BTreeMap<_, _>>();
        assert!(digests.len() >= 8, "{digests:?}");
        digests
    };
    assert_eq!(digests(9_902), digests(9_903));
}
