@api @validation @skip-node
Feature: Input Validation Errors

  Background:
    Given an empty graph

  Scenario: ValidationError on empty string query to execute
    When I execute ""
    Then a ValidationError is raised

  Scenario: ValidationError on whitespace-only query to execute
    When I execute "   "
    Then a ValidationError is raised

  Scenario Outline: ValidationError on invalid label name for add_node
    When I add a node with label "<invalid_label>"
    Then a ValidationError is raised

    Examples:
      | invalid_label |
      |               |
      | 9Label        |
      | My-Label      |
      | My Label      |

  Scenario Outline: ValidationError on invalid relationship type name for add_edge
    Given a Person node named "Alice"
    And a Person node named "Bob"
    When I add a "<invalid_type>" edge from "Alice" to "Bob"
    Then a ValidationError is raised

    Examples:
      | invalid_type |
      |              |
      | 9KNOWS       |
      | HAS-EDGE     |

  Scenario: ValidationError on invalid algorithm name for rank
    Given a graph with 3 Person nodes connected by KNOWS edges
    When I rank "Person" by "nonexistent_algorithm"
    Then a ValidationError is raised

  Scenario: ValidationError on invalid algorithm name for cluster
    Given a graph with 3 Person nodes connected by KNOWS edges
    When I cluster "Person" by "nonexistent_algorithm"
    Then a ValidationError is raised

  Scenario: ValidationError on empty vector to find
    Given a graph with a Paper node titled "Graph Neural Networks"
    When I find by an empty vector in label "Paper"
    Then a ValidationError is raised

  Scenario: ValidationError on vector containing NaN
    Given a graph with a Paper node titled "Graph Neural Networks"
    When I find by a vector containing NaN in label "Paper"
    Then a ValidationError is raised

  Scenario: ValidationError on vector containing infinity
    Given a graph with a Paper node titled "Graph Neural Networks"
    When I find by a vector containing infinity in label "Paper"
    Then a ValidationError is raised

  Scenario: ValidationError when find is called with neither text nor vector
    Given a graph with a Paper node titled "Graph Neural Networks"
    When I find with no query and no vector in label "Paper"
    Then a ValidationError is raised

  Scenario: ValidationError on empty properties list to index
    Given a graph with a Paper node titled "Graph Neural Networks"
    When I index label "Paper" on an empty properties list
    Then a ValidationError is raised
