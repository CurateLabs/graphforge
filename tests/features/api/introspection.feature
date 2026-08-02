@api @introspection
Feature: Introspection API

  Scenario: schema returns Arrow Table describing present node labels
    Given a graph with a Person node named "Alice" and a Paper node titled "GNN"
    When I call schema
    Then the result is an Arrow Table
    And the table contains an entry for label "Person"
    And the table contains an entry for label "Paper"

  Scenario: labels returns list of node label strings
    Given a graph with a Person node named "Alice" and a Paper node titled "GNN"
    When I call labels
    Then the result contains "Person"
    And the result contains "Paper"

  Scenario: relationship_types returns list of relationship type strings
    Given a graph with a KNOWS relationship and an AUTHORED relationship
    When I call relationship_types
    Then the result contains "KNOWS"
    And the result contains "AUTHORED"

  Scenario: node_count returns integer count for a label
    Given a graph with 3 Person nodes and 1 Paper node
    When I call node_count for label "Person"
    Then the result is 3

  Scenario: node_count returns 0 for a label with no nodes
    Given an empty graph
    When I call node_count for label "Person"
    Then the result is 0
