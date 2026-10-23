use graphforge_discovery::ItemKind;
use graphforge_hub_publish::{
    HubErrorBody, HubPublishError, HubPublishErrorCode, ID_TAKEN_MESSAGE, IdTakenItem,
    MAX_ID_TAKEN_ITEMS,
};
use serde_json::{Value, json};

fn item() -> Value {
    json!({"kind": "ontology_module", "id": "ont_aaaaaaaaaaaaaaaa"})
}
fn body(items: Value) -> Value {
    json!({"code": "id_taken", "message": ID_TAKEN_MESSAGE, "items": items})
}

#[test]
fn typed_refusal_has_fixed_message_canonical_ids_and_true_only_optional_hints() {
    let item = IdTakenItem {
        kind: ItemKind::OntologyModule,
        id: "ont_aaaaaaaaaaaaaaaa".into(),
        owner: Some("organization".into()),
        publish_as_owner: Some(true),
    };
    let refusal = HubPublishError::id_taken(vec![item.clone()]).unwrap();
    assert_eq!(refusal.code.http_status(), 409);
    assert_eq!(refusal.wire_code(), "hub.publish.id_taken");
    assert_eq!(refusal.gf_code(), "hub.publish.id_taken");
    let encoded = serde_json::to_vec(&refusal.body()).unwrap();
    assert_eq!(
        serde_json::from_slice::<HubErrorBody>(&encoded)
            .unwrap()
            .items,
        Some(vec![item])
    );
    assert_eq!(
        HubPublishError::new(HubPublishErrorCode::IdTaken, "untrusted detail")
            .body()
            .message,
        ID_TAKEN_MESSAGE
    );
    assert!(
        serde_json::to_value(HubPublishError::new(HubPublishErrorCode::Internal, "fixed").body())
            .unwrap()
            .get("items")
            .is_none()
    );
    assert!(
        serde_json::from_value::<HubErrorBody>(
            json!({"code": "id_taken", "message": "ignored by clients"})
        )
        .is_ok()
    );
}

#[test]
fn malformed_or_misclassified_refusal_items_are_rejected() {
    let mut invalid = vec![body(json!([])), body(Value::Null)];
    for (field, value) in [
        ("kind", json!("bridge_set")),
        ("kind", json!("unknown")),
        ("id", json!("ONT_AAAAAAAAAAAAAAAA")),
        ("id", json!("https://graphforge.sh/ont_aaaaaaaaaaaaaaaa")),
        ("owner", json!("private/owner")),
        ("owner", Value::Null),
        ("publish_as_owner", json!(false)),
        ("publish_as_owner", Value::Null),
        ("extra", json!(true)),
    ] {
        let mut changed = item();
        changed[field] = value;
        invalid.push(body(json!([changed])));
    }
    invalid.push(body(json!([item(), item()])));
    invalid.push(body(
        json!([item(), {"kind": "bridge_set", "id": "brg_aaaaaaaaaaaaaaaa"}]),
    ));
    let mut wrong_code = body(json!([item()]));
    wrong_code["code"] = json!("invalid_input");
    invalid.push(wrong_code);
    let mut wrong_null = body(Value::Null);
    wrong_null["code"] = json!("invalid_input");
    invalid.push(wrong_null);
    invalid.push(body(Value::Array(vec![item(); MAX_ID_TAKEN_ITEMS + 1])));
    for document in invalid {
        assert!(
            serde_json::from_value::<HubErrorBody>(document.clone()).is_err(),
            "accepted malformed refusal: {document}"
        );
    }
}
