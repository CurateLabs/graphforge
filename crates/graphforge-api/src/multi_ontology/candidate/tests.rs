use super::super::{MultiOntologyValidationReceipt, tests::parity_document};
use crate::GraphForge;

#[test]
fn validation_receipts_are_rust_owned_and_structured() {
    let graph = GraphForge::new(None).unwrap();
    let valid = graph
        .validate_ontology_module(&parity_document("base"))
        .unwrap();
    assert!(valid.valid);
    assert!(valid.diagnostics.is_empty());
    let mut invalid = parity_document("base");
    invalid.entity_types.push(invalid.entity_types[0].clone());
    let receipt = graph.validate_ontology_module(&invalid).unwrap();
    assert!(!receipt.valid);
    assert_eq!(receipt.diagnostics[0].code, "inventory.malformed");
    assert_eq!(
        serde_json::from_str::<MultiOntologyValidationReceipt>(
            &serde_json::to_string(&receipt).unwrap()
        )
        .unwrap(),
        receipt
    );
}
