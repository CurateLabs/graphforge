Feature: Deterministic probate and genealogy interpretation

  Scenario: Late evidence changes the working view without rewriting history
    Given [PG-01] a persistent advisory probate project with synthetic records
    When [PG-02] the researcher searches records and traverses candidate kinship
    And [PG-03] records two evidence-backed parentage hypotheses
    And [PG-04] selects the first working hypothesis
    And [PG-05] amends reasoning and changes the selection after late evidence
    And [PG-06] clears the working selection without rejecting either alternative
    And [PG-07] supersedes a misattributed document association
    And [PG-08] records a backdated valid-time interpretation
    Then [PG-09] the prior transaction-time view is unchanged
    And [PG-10] UUIDs, rows, history, and current interpretation survive reopen
    And [PG-11] the Python binding repeats selection, clearing, and reopen behavior
