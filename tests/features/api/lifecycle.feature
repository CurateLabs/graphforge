@api @lifecycle
Feature: Lifecycle State

  @binding-only
  Scenario Outline: LifecycleError on <method> after close
    Given a graph with a Person node named "Alice"
    And the forge instance is closed
    When I attempt to call <method>
    Then a LifecycleError is raised

    Examples:
      | method                                    |
      | execute with query "MATCH (n) RETURN n"   |
      | rank with label "Person" by "pagerank"    |
      | find with text "Alice" in label "Person"  |
      | add_node with label "Person" named "Bob"  |

  Scenario: StorageError when clear is called on a persistent instance
    Given a persistent graph backed by Parquet
    When I call clear
    Then a StorageError is raised

  @persistence
  Scenario: persistent forge survives close and reopen cycle
    Given a persistent graph at a temporary path
    And I add a node with label "Person" named "Alice"
    And the forge instance is closed
    When I reopen the forge at the same path
    Then execute "MATCH (p:Person) RETURN p.name AS name" returns 1 row
    And the first row value for "name" is "Alice"
