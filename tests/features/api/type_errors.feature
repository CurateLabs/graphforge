@api @types
Feature: Type Errors

  Background:
    Given an empty graph

  @binding-only
  Scenario: TypeError when add_edge source is not a NodeHandle
    Given a Person node named "Alice"
    When I add a "KNOWS" edge from a raw integer to the node for "Alice"
    Then a TypeError is raised

  @binding-only
  Scenario: TypeError when add_edge destination is not a NodeHandle
    Given a Person node named "Alice"
    When I add a "KNOWS" edge from the node for "Alice" to a raw integer
    Then a TypeError is raised

  @binding-only
  Scenario: TypeError on unsupported property value type
    When I add a node with label "Person" with an unsupported property value
    Then a TypeError is raised
