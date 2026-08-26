//! External-crate proof that construction contracts are owned by `graphforge-api`.

use graphforge_api::{
    CONSTRUCTION_EDGE_SCHEMA, CONSTRUCTION_NODE_SCHEMA, ConstructionChunkReceipt,
    GraphConstructionBudgets, GraphConstructionEvidence, GraphConstructionProgress,
    GraphConstructionState,
};

#[test]
fn construction_contract_is_nameable_from_graphforge_api_alone() {
    let budgets = GraphConstructionBudgets::default();
    let evidence = GraphConstructionEvidence::default();
    let state = GraphConstructionState::Staging;
    let node_schema = CONSTRUCTION_NODE_SCHEMA.clone();
    let edge_schema = CONSTRUCTION_EDGE_SCHEMA.clone();

    fn accepts_receipt(_: Option<ConstructionChunkReceipt>) {}
    fn accepts_progress(_: Option<GraphConstructionProgress>) {}

    accepts_receipt(None);
    accepts_progress(None);
    assert!(budgets.max_batch_rows > 0);
    assert_eq!(evidence.input_rows, 0);
    assert_eq!(state, GraphConstructionState::Staging);
    assert_eq!(node_schema.fields().len(), 2);
    assert_eq!(edge_schema.fields().len(), 4);
}
