use super::*;

fn chain(count: usize) -> Vec<LineageVersion> {
    (0..count)
        .map(|index| LineageVersion {
            version_uuid: format!("00000000-0000-0000-0000-{index:012x}"),
            parents: if index + 1 < count {
                vec![format!("00000000-0000-0000-0000-{:012x}", index + 1)]
            } else {
                Vec::new()
            },
            identity_digest: Sha256Digest(format!("sha256:{}", "1".repeat(64))),
            kind: "complete".into(),
            branch_uuid: "00000000-0000-0000-0001-000000000000".into(),
            source_version_uuid: None,
            package: None,
        })
        .collect()
}

#[test]
fn deep_parent_chain_expands_each_record_once_and_detects_back_edge() {
    let mut records = chain(DiscoveryLimits::default().max_lineage_entries);
    assert_eq!(
        validate_parent_graph(&references(&records)).unwrap(),
        records.len()
    );
    // A shared, already completed ancestor is not a cycle.
    let shared = records[2].version_uuid.clone();
    records[0].parents.push(shared);
    assert_eq!(
        validate_parent_graph(&references(&records)).unwrap(),
        records.len()
    );
    let first = records[0].version_uuid.clone();
    records.last_mut().unwrap().parents.push(first);
    let error = validate_parent_graph(&references(&records)).unwrap_err();
    assert_eq!(error.code, DiscoveryErrorCode::IntegrityFailure);
}

fn references(records: &[LineageVersion]) -> BTreeMap<&str, &LineageVersion> {
    records
        .iter()
        .map(|record| (record.version_uuid.as_str(), record))
        .collect()
}
