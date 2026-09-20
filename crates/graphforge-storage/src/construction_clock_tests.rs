#[test]
fn cross_process_shape_digest_helper() {
    let Some(output) = std::env::var_os("GF_TEST_SHAPE_DIGEST_OUTPUT") else {
        return;
    };
    let root = TempDir::new().unwrap();
    let mut session =
        GraphConstructionSession::open(root.path(), Uuid::from_u128(OPERATION), 0, budgets(64))
            .unwrap();
    let nodes = node_ids(1_024);
    let edges = edge_ids(1_024);
    if std::env::var("GF_TEST_SHAPE_PROPERTIES").as_deref() == Ok("1") {
        for (index, window) in nodes.chunks(128).enumerate() {
            let bare = node_rows(window);
            let mut fields = bare.schema().fields().to_vec();
            fields.push(Arc::new(arrow::datatypes::Field::new(
                "score",
                arrow::datatypes::DataType::Int64,
                false,
            )));
            let mut columns = bare.columns().to_vec();
            columns.push(Arc::new(arrow::array::Int64Array::from(vec![
                7;
                window.len()
            ])));
            let rows =
                RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), columns)
                    .unwrap();
            session
                .append(
                    ConstructionChunkKind::Node,
                    &format!("nodes-{index}"),
                    &rows,
                )
                .unwrap();
        }
        for (index, window) in edges.chunks(128).enumerate() {
            session
                .append(
                    ConstructionChunkKind::Edge,
                    &format!("edges-{index}"),
                    &edge_rows(index * 128, window, &nodes),
                )
                .unwrap();
        }
    } else {
        append_all(&mut session, &nodes, &edges, 128);
    }
    session.seal().unwrap();
    let shape = session.shape_canonical_with_cancellation(|| false).unwrap();
    let mut digests = std::collections::BTreeMap::new();
    for name in std::iter::once(&shape.identities)
        .chain(shape.node_details.iter())
        .chain(shape.edge_details.iter())
        .chain(shape.node_rows.iter())
        .chain(shape.edge_rows.iter())
        .chain(shape.edge_endpoints.iter())
        .chain(std::iter::once(&shape.runtime_catalog))
    {
        let receipt = receipt_for_existing(&session.root, name).unwrap();
        digests.insert(name, receipt.sha256);
    }
    std::fs::write(
        output,
        serde_json::to_vec(&serde_json::json!({
            "session_now_micros": session.session_now_micros(), "digests": digests
        }))
        .unwrap(),
    )
    .unwrap();
}

#[test]
fn identical_input_shaped_outputs_match_across_processes() {
    let outputs = TempDir::new().unwrap();
    for properties in ["0", "1"] {
        let mut maps = Vec::new();
        let mut clocks = Vec::new();
        for index in 0..2 {
            let output = outputs
                .path()
                .join(format!("shape-{properties}-{index}.json"));
            let child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "graph_construction::tests::determinism::cross_process_shape_digest_helper",
                    "--nocapture",
                ])
                .env("GF_TEST_SHAPE_DIGEST_OUTPUT", &output)
                .env("GF_TEST_SHAPE_PROPERTIES", properties)
                .output()
                .unwrap();
            assert!(
                child.status.success(),
                "child failed: {}\n{}",
                String::from_utf8_lossy(&child.stdout),
                String::from_utf8_lossy(&child.stderr)
            );
            let evidence: serde_json::Value =
                serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
            let map: std::collections::BTreeMap<String, String> =
                serde_json::from_value(evidence["digests"].clone()).unwrap();
            assert!(map.contains_key(SHAPED_RUNTIME_CATALOG));
            assert!(map.len() >= 5);
            assert_eq!(
                map.keys().any(|name| name.starts_with("shaped-rows-")),
                properties == "1"
            );
            maps.push(map);
            clocks.push(evidence["session_now_micros"].as_i64().unwrap());
        }
        // This is a coverage precondition, not a uniqueness claim about SystemTime.
        // Equal clocks would let the original wall-clock leak pass unnoticed.
        assert_ne!(
            clocks[0], clocks[1],
            "independent processes must exercise different session clocks"
        );
        println!(
            "CROSS_PROCESS_SHAPE_DIGESTS={}",
            serde_json::json!({"properties":properties,"clocks":clocks,"digests":maps})
        );
        assert_eq!(
            maps[0], maps[1],
            "identical input must produce identical shaped bytes across processes"
        );
    }
}
