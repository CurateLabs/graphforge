Feature: Mature exploratory findings into a separate strict target

  Scenario: Ontology emerges without becoming automatic truth
    Given [OEH-01] an empty persistent source project has no ontology
    When [OEH-02] a supported public bulk load creates disconnected heterogeneous components
    And [OEH-03] public incremental construction connects and enriches those components
    Then [OEH-04] the RuntimeCatalog grows deterministically and survives source reopen
    When [OEH-05] query, text search, and representative algorithms bound the findings
    And [OEH-06] the analyst explicitly approves and loads a partial ontology
    Then [OEH-07] that live source session is advisory and unknown concepts remain observable
    And [OEH-08] reopening the source remains exploratory because session loading is not migration

  Scenario: Curated findings enter a separate governed target
    Given [OEH-09] an independently created target has a formal strict ontology
    And [OEH-10] curated rows carry an explicit source UUID mapping and approval record
    When [OEH-11] conforming mapped findings are loaded through public construction APIs
    Then [OEH-12] source and target analytical outcomes agree for the curated subset
    When [OEH-13] an unmapped property and malformed batch are submitted
    Then [OEH-14] structured failures occur before either committed project changes
    And [OEH-15] source and target reopen with identical authoritative rows and fingerprints
