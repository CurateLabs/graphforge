use super::*;

#[test]
fn assertion_validation_applies_row_limits_before_row_checks() {
    let ledger = fixture();
    let mut assertions = ledger.assertions().to_vec();
    assertions[0].contract_version = 0;

    assert!(matches!(
        validate_rows_with_limit(&assertions, ledger.graph_refs(), 0),
        Err(KnowledgeError::Limit {
            participant: "assertions",
            observed: 1,
            limit: 0,
        })
    ));
}

#[test]
fn assertion_merge_conflicts_when_only_graph_references_differ() {
    let base = fixture();
    let mut references = base.graph_refs().to_vec();
    references[0].graph_uuid = uuid7(30);
    let conflicting = AssertionLedger::new(base.assertions().to_vec(), references).unwrap();

    assert!(matches!(
        base.merge(&conflicting),
        Err(KnowledgeError::Conflict("assertion_uuid"))
    ));
}
