#[test]
fn cross_process_shape_digest_helper() {
    let Some(output) = std::env::var_os("GF_TEST_SHAPE_DIGEST_OUTPUT") else {
        return;
    };
    let root = TempDir::new().unwrap();
    let mut session = GraphConstructionSession::open(
        root.path(),
        Uuid::from_u128(OPERATION),
        0,
        budgets(64),
    )
    .unwrap();
    append_all(&mut session, &node_ids(1_024), &edge_ids(1_024), 128);
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
    std::fs::write(output, serde_json::to_vec(&digests).unwrap()).unwrap();
}

#[test]
fn identical_input_shaped_outputs_match_across_processes() {
    let outputs = TempDir::new().unwrap();
    let mut maps = Vec::new();
    for index in 0..2 {
        let output = outputs.path().join(format!("shape-{index}.json"));
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "graph_construction::tests::determinism::cross_process_shape_digest_helper",
                "--nocapture",
            ])
            .env("GF_TEST_SHAPE_DIGEST_OUTPUT", &output)
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "child failed: {}\n{}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr),
        );
        let map: std::collections::BTreeMap<String, String> =
            serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert!(map.contains_key(SHAPED_RUNTIME_CATALOG));
        assert!(map.len() >= 5);
        maps.push(map);
    }
    println!("CROSS_PROCESS_SHAPE_DIGESTS={}", serde_json::to_string(&maps).unwrap());
    assert_eq!(maps[0], maps[1], "identical input must produce identical shaped bytes across processes");
}
