@api @execute
Feature: Cypher Execute API

  Scenario: simple MATCH returns Arrow Table
    Given a graph with a Person node named "Alice"
    When I execute "MATCH (p:Person) RETURN p.name AS name"
    Then the result is an Arrow Table
    And the table has column "name"
    And the table has 1 row

  Scenario: RETURN columns match alias names in schema
    Given a graph with a Person node named "Alice" with age 30
    When I execute "MATCH (p:Person) RETURN p.name AS person_name, p.age AS person_age"
    Then the result schema contains column "person_name"
    And the result schema contains column "person_age"

  @skip-node
  Scenario: empty result returns Arrow Table with correct schema and zero rows
    Given an empty graph
    When I execute "MATCH (p:Person) RETURN p.name AS name"
    Then the result is an Arrow Table
    And the table has 0 rows
    And the table has column "name"

  Scenario: query with named parameters
    Given a graph with a Person node named "Alice"
    When I execute "MATCH (p:Person) WHERE p.name = $name RETURN p.name AS name" with parameter name "Alice"
    Then the table has 1 row
    And the first row value for "name" is "Alice"

  Scenario: multi-row result has correct row count
    Given a graph with 3 Person nodes
    When I execute "MATCH (p:Person) RETURN p.name AS name"
    Then the table has 3 rows

  Scenario: execute raises ParseError on invalid Cypher
    Given an empty graph
    When I execute "NOT VALID CYPHER !!!"
    Then a ParseError is raised
