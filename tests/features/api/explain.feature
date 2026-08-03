@api @explain
Feature: Explain API

  Scenario: explain returns a non-empty string
    Given an empty graph
    When I call explain on "MATCH (n:Person) RETURN n.name AS name"
    Then the result is a non-empty string

  Scenario: explain output contains the query stages
    Given an empty graph
    When I call explain on "MATCH (n:Person) RETURN n.name AS name"
    Then the result contains "AST"
    And the result contains "GraphIR"

  @excluded-api-bdd @issue-354
  Scenario: explain does not execute the query
    Given an empty graph
    When I call explain on "CREATE (:Person {name: 'Ghost'})"
    Then the result is a non-empty string
    And execute "MATCH (n:Person) RETURN n" returns 0 rows
