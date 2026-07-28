Feature: Recover atomic graph and knowledge cycles after interruption

  Scenario: Composite publication recovers one complete generation
    # AR-01 implementation=rust:seed-strict-s-graph
    Given [AR-01] a deterministic strict S-graph project under a deep-wide ontology
    # AR-02 implementation=rust:graph-m20-composite-success
    When [AR-02] a graph-plus-M20 composite analytical step publishes successfully
    # AR-03 implementation=rust:graph-m20-m21-composite-success
    And [AR-03] a graph-plus-M20-plus-M21 composite analytical step publishes successfully
    # AR-04 implementation=rust:reject-invalid-before-publish
    Then [AR-04] invalid ontology or cross-reference input rejects before any participant publication
    # AR-05 implementation=rust:failpoint-pre-current-recover-previous
    When [AR-05] process termination at each pre-CURRENT boundary recovers exactly the previous complete generation
    # AR-06 implementation=rust:failpoint-post-current-recover-new
    When [AR-06] process termination at each post-CURRENT boundary recovers exactly the new complete generation
    # AR-07 implementation=rust:no-mixed-or-orphan-state
    Then [AR-07] no mixed participants, orphan evidence, reasoning, hypothesis, provenance, staging file, or live lock remains
    # AR-08 implementation=rust:idempotent-exact-retry
    When [AR-08] the recovered request identity is retried exactly without duplication
    # AR-09 implementation=rust:conflicting-reuse-structured-error
    And [AR-09] conflicting identity reuse returns a structured idempotency conflict without mutation
    # AR-10 implementation=rust:query-history-transaction-time
    And [AR-10] ordered query results, hypothesis state, and transaction-time history agree
    # AR-11 implementation=rust:close-and-reopen
    And [AR-11] close and reopen reproduce the authoritative complete generation
    # AR-12 implementation=rust-and-python:same-commit-parity
    And [AR-12] Rust and representative Python bindings agree on ordered results at one tested commit
