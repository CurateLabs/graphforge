@api
Feature: Graph Construction API

  Background:
    Given an empty graph

  Scenario: add a single node with label and properties
    When I add a node with label "Person" named "Alice" aged 30
    Then the result is a NodeHandle with label "Person"
    And the NodeHandle exposes UUID identity with no numeric surrogate
    And execute readback returns the NodeHandle UUID and name "Alice"

  @construction @skip-node
  Scenario: add a relationship between two nodes
    Given a Person node named "Alice"
    And a Person node named "Bob"
    When I add a "KNOWS" edge from "Alice" to "Bob" with since 2020
    Then the result is an EdgeHandle with UUID identity and no numeric surrogate
    And execute "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name" returns 1 row

  Scenario: NodeHandle UUID round-trips through execute
    When I add a node with label "Person" named "Alice"
    Then execute readback returns the NodeHandle UUID and name "Alice"

  @construction
  Scenario: bulk add_nodes with a list of records
    When I bulk add nodes with label "Person" and 2 records
    Then execute "MATCH (p:Person) RETURN p.name" returns 2 rows

  @construction
  Scenario: bulk add_nodes with Arrow Table
    When I bulk add nodes with label "Person" from an Arrow Table of 5 rows
    Then execute "MATCH (p:Person) RETURN p.name" returns 5 rows

  @construction
  Scenario: bulk add_edges with src and dst column names
    Given a graph with 2 Person nodes with ids in columns "src_id" and "dst_id"
    When I bulk add edges with type "KNOWS" using source column "src_id" and destination column "dst_id"
    Then execute "MATCH ()-[:KNOWS]->() RETURN count(*)" returns 1 row with value 1

  Scenario: NodeHandle string representation contains UUID without cached properties
    When I add a node with label "Person" named "Alice" aged 30
    Then the string representation contains the NodeHandle UUID
    And the string representation does not contain cached property "Alice"

  Scenario Outline: UUID, handle, and property path selectors reach Rust dispatch
    Given Person nodes named "Alice" and "Bob"
    When I request "bfs" paths using "<selector>" selectors
    Then the path request reaches Rust dispatch

    Examples:
      | selector |
      | UUID     |
      | handle   |
      | property |

  Scenario Outline: Invalid path selectors preserve structured validation errors
    Given Person nodes named "Alice" and "Bob"
    When I request "bfs" paths with a "<case>" source selector
    Then a structured selector error is raised

    Examples:
      | case        |
      | malformed   |
      | missing     |
      | ambiguous   |
      | cross-graph |

  Scenario: Closed instances reject path selectors before coercion
    Given Person nodes named "Alice" and "Bob"
    And the forge instance is closed
    When I request "bfs" paths using "UUID" selectors
    Then a LifecycleError is raised
