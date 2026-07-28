Feature: Deterministic cyber intrusion investigation

  Scenario: Correct a false association without silently trusting stale derived state
    Given [CY-01] a deep and wide strict security ontology
    And [CY-02] deterministic heterogeneous security telemetry
    When [CY-03] unknown and invalid strict inputs are rejected atomically
    And [CY-04] text, vector, and hybrid retrieval establish the investigative scope
    And [CY-05] traversal, paths, similarity, rank, and clustering are chained
    And [CY-06] evidence and competing intrusion hypotheses are recorded
    And [CY-07] confidence leaves both hypotheses unresolved
    And [CY-08] a false host association is corrected through a checkpoint revert
    And [CY-09] stale search state is rejected and explicitly refreshed
    And [CY-10] a cancelled operation fails without poisoning subsequent work
    Then [CY-11] transaction history and corrected results remain inspectable
    And [CY-12] close and reopen reproduce stable UUIDs, Arrow order, and fingerprints
