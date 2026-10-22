//! Permanent IDs use the same native engine that the WASM exports call.

use graphforge_discovery::ItemId;
use graphforge_protocol_wasm::engine::{self, ItemIdKind};
use serde_json::json;

#[test]
fn item_references_normalize_to_typed_canonical_data() {
    for (prefix, kind, wire_kind) in [
        ("ont", ItemIdKind::OntologyModule, "ontology_module"),
        ("brg", ItemIdKind::BridgeSet, "bridge_set"),
    ] {
        let id = format!("{prefix}_7k2m9x4q8r3t5v1w");
        let uri = format!("https://graphforge.sh/{id}");
        for reference in [
            &id,
            &uri,
            &id.to_ascii_uppercase(),
            &uri.to_ascii_uppercase(),
        ] {
            let parsed = engine::parse_item_id(reference).unwrap();
            assert_eq!(parsed.id, id);
            assert_eq!(parsed.uri, uri);
            assert_eq!(parsed.kind, kind);
            assert_eq!(
                serde_json::to_value(parsed).unwrap(),
                json!({"id": id, "uri": uri, "kind": wire_kind})
            );
        }
    }
}

#[test]
fn malformed_references_return_the_shared_grammar_error_without_panicking() {
    for input in [
        "",
        "ont_7k2m9x4q8r3t5v1i",
        "ont_7k2m9x4q8r3t5v1l",
        "ont_7k2m9x4q8r3t5v1o",
        "ont_7k2m9x4q8r3t5v1u",
        "ont_7k2m9x4q8r3t5v1I",
        "abc_0000000000000000",
        "ont_000000000000000",
        "ont_00000000000000000",
        "ont-0000000000000000",
        "https://example.org/ont_7k2m9x4q8r3t5v1w",
        "http://graphforge.sh/ont_7k2m9x4q8r3t5v1w",
        "https://graphforge.sh:443/ont_7k2m9x4q8r3t5v1w",
        "https://graphforge.sh/ont_7k2m9x4q8r3t5v1w/",
        "https://graphforge.sh/owner/ont_7k2m9x4q8r3t5v1w",
        "https://graphforge.sh/ont_7k2m9x4q8r3t5v1w?x=1",
        "https://graphforge.sh/ont_7k2m9x4q8r3t5v1w#fragment",
        "https:",
        "https://graphforge.sé",
        "https://graphforge.sh/ont_7k2m9x4q8r3t5vé",
        "éx_0000000000000000",
        "😀_000000000000000",
        "ont_7k2m9x4q8r3t5vé",
        "ont_7k2m9x4q8r3t5v1é",
        "ont_000000000000000\0",
        "ont_00000000000000K",
        " ont_7k2m9x4q8r3t5v1w",
    ] {
        let shared = ItemId::parse_reference(input).unwrap_err();
        let error = engine::parse_item_id(input).unwrap_err();
        assert_eq!(error.code, "invalid_argument", "{input:?}");
        assert_eq!(error.message, shared.to_string(), "{input:?}");
        assert!(error.path.is_none() && error.version.is_none());
    }
}

#[test]
fn name_reservation_does_not_require_a_known_kind_or_normalize_case() {
    for name in [
        "ont_7k2m9x4q8r3t5v1w",
        "brg_zzzzzzzzzzzzzzzz",
        "abc_0000000000000000",
    ] {
        assert!(engine::has_item_id_shape(name), "{name}");
    }
    assert!(engine::parse_item_id("abc_0000000000000000").is_err());
    for name in [
        "",
        "ONT_7K2M9X4Q8R3T5V1W",
        "ont_7K2m9x4q8r3t5v1w",
        "abc_000000000000000",
        "abc_00000000000000000",
        "abc_000000000000000o",
        "https://graphforge.sh/ont_7k2m9x4q8r3t5v1w",
        "éx_0000000000000000",
        "😀_000000000000000",
    ] {
        assert!(!engine::has_item_id_shape(name), "{name}");
    }
}
