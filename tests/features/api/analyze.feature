@api
Feature: Analyze API

  Scenario: is_dag returns one true Boolean for an empty graph
    Given an empty graph
    When I analyze by "is_dag"
    Then the result is an Arrow Table
    And the table has column "is_dag"
    And the table has 1 row
    And the "is_dag" value is true

  Scenario: is_dag returns false for a directed cycle
    Given a graph with a directed cycle
    When I analyze by "is_dag"
    Then the "is_dag" value is false
