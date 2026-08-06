@api @recipes
Feature: Recipes API

  Scenario: neighbourhood returns an Arrow Table
    Given a graph with Person nodes connected by KNOWS edges up to 3 hops deep
    When I call neighbourhood for "Alice" with hops 2 in label "Person" using canonical property "name"
    Then the result is an Arrow Table

  Scenario: neighbourhood with hops 1 returns only direct neighbours
    Given a graph where Alice knows Bob and Bob knows Charlie but Alice does not know Charlie
    When I call neighbourhood for "Alice" with hops 1 in label "Person" using canonical property "name"
    Then the result contains a row for "Bob"
    And the result does not contain a row for "Charlie"

  Scenario: neighbourhood with hops 2 reaches two-hop neighbours
    Given a graph where Alice knows Bob and Bob knows Charlie but Alice does not know Charlie
    When I call neighbourhood for "Alice" with hops 2 in label "Person" using canonical property "name"
    Then the result contains a row for "Bob"
    And the result contains a row for "Charlie"

  Scenario: neighbourhood on a node with no neighbours returns an empty Arrow Table
    Given a graph with a single Person node named "Lone"
    When I call neighbourhood for "Lone" with hops 1 in label "Person" using canonical property "name"
    Then the result is an Arrow Table
    And the table has 0 rows

  Scenario: neighbourhood with non-existent canonical value returns empty Arrow Table
    Given a graph with Person nodes connected by KNOWS edges
    When I call neighbourhood for "NonExistent" with hops 2 in label "Person" using canonical property "name"
    Then the result is an Arrow Table
    And the table has 0 rows

  Scenario: neighbourhood with hops 0 returns empty Arrow Table
    Given a graph where Alice knows Bob
    When I call neighbourhood for "Alice" with hops 0 in label "Person" using canonical property "name"
    Then the result is an Arrow Table
    And the table has 0 rows
