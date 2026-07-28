Feature: Preserve derived-state freshness through mutation and reanalysis

  Scenario: Public freshness contracts never serve stale derived state as current
    # DSF-01 implementation=rust:seed-strict-project
    Given [DSF-01] a deterministic strict project with sparse global and dense local topology
    # DSF-02 implementation=rust:publish-baseline-derived-state
    And [DSF-02] text, adjacency, caller-vector, analytical, and hypothesis state is current
    # DSF-03 implementation=rust:topology-staleness
    When [DSF-03] an edge-only generation makes adjacency deterministically stale
    # DSF-04 implementation=rust:barrier-cancel-then-public-rebuild
    Then [DSF-04] barrier-timed cancellation preserves prior authority before an atomic public adjacency rebuild
    # DSF-05 implementation=rust:query-inspect-rebuild-query-text
    When [DSF-05] indexed text changes and public query, inspection, rebuild, and subsequent query publish only current text results
    # DSF-06 implementation=rust:property-correction-and-reanalysis
    When [DSF-06] a non-indexed property correction is reanalyzed without ontology or text-index drift
    # DSF-07 implementation=rust:replace-caller-vectors
    When [DSF-07] supplied vectors are atomically replaced and freshness, similarity, and replay identify one current generation
    # DSF-08 implementation=rust:reject-incompatible-vectors
    And [DSF-08] an incompatible vector publication fails without replacing current authority
    # DSF-09 implementation=rust:preserve-hypothesis-state
    And [DSF-09] the recorded hypothesis state remains unchanged across freshness generations
    # DSF-10 implementation=rust:close-and-reopen
    And [DSF-10] close and reopen reproduce current inspections, results, and identifiers
    # DSF-11 implementation=rust-python-node:same-sha-parity
    And [DSF-11] Rust, Python, and Node report the same public freshness contract at one SHA
