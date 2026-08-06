@api @edge_cases
Feature: Edge Cases and Graceful Empty Results

  Scenario Outline: rank on <condition> returns empty Arrow Table
    Given <precondition>
    When I rank "<label>" by "pagerank"
    Then the result is an Arrow Table
    And the table has 0 rows

    Examples:
      | condition              | precondition                                  | label       |
      | a label with no nodes  | a graph with Person nodes but no Paper nodes  | Paper       |
      | a non-existent label   | an empty graph                                | NonExistent |

  Scenario Outline: cluster on <condition> returns empty Arrow Table
    Given <precondition>
    When I cluster "<label>" by "louvain"
    Then the result is an Arrow Table
    And the table has 0 rows

    Examples:
      | condition              | precondition                                  | label       |
      | a label with no nodes  | a graph with Person nodes but no Paper nodes  | Paper       |
      | a non-existent label   | an empty graph                                | NonExistent |

  Scenario: find on a label with no indexed content returns empty Arrow Table
    Given an empty graph
    When I find "anything" in label "Paper"
    Then the result is an Arrow Table
    And the table has 0 rows

  Scenario: find with vector dimension mismatch returns empty Arrow Table
    Given a graph with Paper nodes indexed with 128-dimensional vectors
    When I find by a 64-dimensional vector in label "Paper"
    Then the result is an Arrow Table
    And the table has 0 rows

  Scenario: schema on empty graph returns empty Arrow Table
    Given an empty graph
    When I call schema
    Then the result is an Arrow Table
    And the table has 0 rows

  Scenario: labels and relationship_types on empty graph return empty results
    Given an empty graph
    When I call labels
    Then the result is an empty list
    And calling relationship_types also returns an empty list

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
