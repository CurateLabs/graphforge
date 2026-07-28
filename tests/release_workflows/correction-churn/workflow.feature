Feature: Preserve history through repeated correction churn

  Scenario: Public compensations preserve every prior analytical view
    # CC-01 implementation=rust:seed-advisory-project
    Given [CC-01] a persistent advisory curation project with deterministic synthetic records
    # CC-02 implementation=rust:analyze-and-checkpoint-duplicate
    When [CC-02] a duplicate entity is analyzed and checkpointed
    # CC-03 implementation=rust:graph-compensation
    And [CC-03] the duplicate is compensated without erasing its prior generation
    # CC-04 implementation=rust:strict-validation-rejection
    And [CC-04] invalid ontology data is rejected before publication
    # CC-05 implementation=rust:corrected-data-publication
    And [CC-05] corrected data and ontology definitions publish in a later generation
    # CC-06 implementation=rust:assertion-supersession
    And [CC-06] misattached evidence is replaced through assertion supersession
    # CC-07 implementation=rust:reasoning-amendment
    And [CC-07] reasoning is amended by an append-only successor record
    # CC-08 implementation=rust:hypothesis-membership-removal
    And [CC-08] status and hypothesis membership are corrected by new events
    # CC-09 implementation=rust:idempotency-conflict
    Then [CC-09] repeated compensation is idempotent or a structured conflict
    # CC-10 implementation=rust:pinned-history-and-runs
    And [CC-10] pinned transaction-time views and algorithm runs remain reproducible
    # CC-11 implementation=rust:current-view
    And [CC-11] current graph, ontology, knowledge, and epistemic results are deterministic
    # CC-12 implementation=rust-and-python:reopen
    And [CC-12] representative binding results and UUIDs agree after reopen
