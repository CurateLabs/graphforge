use super::*;

fn definition() -> SavedQuery {
    SavedQuery {
        query_uuid: Uuid::new_v4(),
        name: "Find people".into(),
        description: Some("Parameterized analysis".into()),
        query: "MATCH (n:UnpublishedLabel) WHERE n.name = $name RETURN n.name".into(),
        parameters: BTreeMap::from([("name".into(), SavedQueryParameterType::String)]),
    }
}

fn collection() -> WorkspaceSavedQueries {
    let query = definition();
    WorkspaceSavedQueries {
        queries: BTreeMap::from([(query.query_uuid, query)]),
        ..WorkspaceSavedQueries::default()
    }
}

#[test]
fn definitions_round_trip_without_schema_binding_or_parameter_values() {
    let record = collection();
    let bytes = record.to_canonical_json().unwrap();
    assert_eq!(
        WorkspaceSavedQueries::from_canonical_json(&bytes).unwrap(),
        record
    );
    let participant = record.to_project_participant().unwrap();
    assert_eq!(participant.record_family_id, WORKSPACE_SAVED_QUERIES_FAMILY);
    assert_eq!(participant.bytes, bytes);
    assert_eq!(participant.schema_fingerprint, schema_fingerprint());
    assert_eq!(
        serde_json::to_value(record.queries.values().next().unwrap()).unwrap()["parameters"],
        serde_json::json!({"name": "string"})
    );
}

#[test]
fn identity_name_syntax_parameter_and_bounds_contracts_fail_closed() {
    let original = definition();
    for name in ["", " ", " leading", "trailing ", "line\nfeed"] {
        let mut query = original.clone();
        query.name = name.into();
        assert!(query.validate().is_err());
    }
    let mut query = original.clone();
    query.query_uuid = Uuid::nil();
    assert!(query.validate().is_err());
    for text in [
        "",
        "RETURN",
        "CREATE (n)",
        "CALL unsafe.procedure() RETURN 1",
        "RETURN exists { CREATE (n) RETURN n }",
    ] {
        let mut query = original.clone();
        query.query = text.into();
        assert!(query.validate().is_err(), "{text}");
    }
    let mut query = original.clone();
    query.parameters.clear();
    assert!(query.validate().is_err());
    let mut query = original.clone();
    query
        .parameters
        .insert("unused".into(), SavedQueryParameterType::Integer);
    assert!(query.validate().is_err());
    let mut query = original.clone();
    query.parameters = BTreeMap::from([("Name".into(), SavedQueryParameterType::String)]);
    assert!(query.validate().is_err());
    let mut query = original.clone();
    query.query = " ".repeat(MAX_SAVED_QUERY_BYTES + 1);
    assert!(query.validate().is_err());
    let mut record = collection();
    let first = record.queries.values().next().unwrap().clone();
    let mut second = first.clone();
    second.query_uuid = Uuid::new_v4();
    record.queries.insert(second.query_uuid, second.clone());
    assert!(record.validate().is_err());
    second.name = "find people".into();
    record.queries.insert(second.query_uuid, second);
    assert!(record.validate().is_ok(), "names are case-sensitive");
    record.queries.insert(Uuid::new_v4(), first);
    assert!(record.validate().is_err());
    let mut oversized = WorkspaceSavedQueries::default();
    for index in 0..MAX_SAVED_QUERIES + 1 {
        let mut query = original.clone();
        query.query_uuid = Uuid::new_v4();
        query.name = index.to_string();
        oversized.queries.insert(query.query_uuid, query);
    }
    assert!(oversized.validate().is_err());
    let mut oversized = WorkspaceSavedQueries::default();
    for index in 0..20 {
        let mut query = original.clone();
        query.query_uuid = Uuid::new_v4();
        query.name = index.to_string();
        query.query = format!("RETURN '{}'", "x".repeat(60 * 1024));
        query.parameters.clear();
        oversized.queries.insert(query.query_uuid, query);
    }
    assert!(oversized.validate().is_err(), "aggregate byte bound");
}

#[test]
fn persisted_contract_rejects_future_noncanonical_unknown_and_duplicate_fields() {
    let bytes = collection().to_canonical_json().unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["contract_version"] = 2.into();
    assert!(
        WorkspaceSavedQueries::from_canonical_json(&serde_json::to_vec(&value).unwrap()).is_err()
    );
    assert!(WorkspaceSavedQueries::from_canonical_json(&bytes[..bytes.len() - 1]).is_err());
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let query = value["queries"]
        .as_object_mut()
        .unwrap()
        .values_mut()
        .next()
        .unwrap();
    query["parameter_values"] = serde_json::json!({"name": "secret"});
    assert!(
        WorkspaceSavedQueries::from_canonical_json(&serde_json::to_vec(&value).unwrap()).is_err()
    );
    assert!(WorkspaceSavedQueries::from_canonical_json(
        b"{\"contract_version\":1,\"queries\":{},\"queries\":{}}\n"
    )
    .is_err());
    let id = Uuid::new_v4();
    let query = SavedQuery {
        query_uuid: id,
        name: "q".into(),
        description: None,
        query: "RETURN 1".into(),
        parameters: BTreeMap::new(),
    };
    let query_json = serde_json::to_string(&query).unwrap();
    let duplicates = format!(
        "{{\"contract_version\":1,\"queries\":{{\"{id}\":{query_json},\"{id}\":{query_json}}}}}\n"
    );
    assert!(WorkspaceSavedQueries::from_canonical_json(duplicates.as_bytes()).is_err());
    let oversized = vec![b' '; MAX_WORKSPACE_SAVED_QUERIES_BYTES + 1];
    assert!(WorkspaceSavedQueries::from_canonical_json(&oversized).is_err());
}

#[test]
fn absent_saved_query_participant_reads_empty_without_initial_layout_change() {
    let root = tempfile::tempdir().unwrap();
    let generation = crate::open_or_initialize_project(root.path()).unwrap();
    assert!(!generation
        .participant_descriptors()
        .unwrap()
        .iter()
        .any(|descriptor| descriptor.record_family_id == WORKSPACE_SAVED_QUERIES_FAMILY));
    assert_eq!(
        read_workspace_saved_queries(&generation).unwrap(),
        WorkspaceSavedQueries::default()
    );
}
