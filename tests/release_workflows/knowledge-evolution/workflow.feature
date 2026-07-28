Feature: Evolve knowledge over a byte-stable graph and ontology

  Scenario: Explicit belief views evolve while neutral graph analysis does not
    Given [KE-01] a strict project with one stable synthetic graph and ontology
    And [KE-02] baseline query search rank and path results are recorded
    When [KE-03] two evidence-backed unresolved assertions are appended
    And [KE-04] confidence assessments are recorded without selecting either assertion
    And [KE-05] reasoning is amended and status and valid-time events are appended
    And [KE-06] both assertions join one competing-hypothesis group
    And [KE-07] the first working hypothesis is explicitly selected
    And [KE-08] the selection explicitly changes to the second hypothesis
    And [KE-09] the working selection is explicitly cleared
    Then [KE-10] neutral graph search and algorithms equal the baseline byte for byte
    And [KE-11] explicit belief projections and temporal cutoffs reproduce each interpretation
    And [KE-12] graph ontology UUIDs cutoffs and binding results agree after reopen
