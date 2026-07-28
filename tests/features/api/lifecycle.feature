@api @lifecycle @skip-node
Feature: Lifecycle and Transaction State

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

  Scenario: LifecycleError when begin is called during an active transaction
    Given an empty graph
    And a transaction has been started
    When I call begin
    Then a LifecycleError is raised

  Scenario: LifecycleError when commit is called without begin
    Given an empty graph
    When I call commit
    Then a LifecycleError is raised

  Scenario: LifecycleError when rollback is called without begin
    Given an empty graph
    When I call rollback
    Then a LifecycleError is raised

  Scenario: StorageError when clear is called on a persistent instance
    Given a persistent graph backed by Parquet
    When I call clear
    Then a StorageError is raised

  @transactions
  Scenario: transaction rollback restores prior state completely
    Given an empty graph
    And I add a node with label "Person" named "Alice"
    When I call begin
    And I add a node with label "Person" named "Bob"
    And I call rollback
    Then execute "MATCH (p:Person) RETURN p.name AS name" returns 1 row
    And the first row value for "name" is "Alice"

  @transactions
  Scenario: transaction commit persists changes
    Given an empty graph
    When I call begin
    And I add a node with label "Person" named "Alice"
    And I call commit
    Then execute "MATCH (p:Person) RETURN p.name AS name" returns 1 row
    And the first row value for "name" is "Alice"

  @persistence
  Scenario: persistent forge survives close and reopen cycle
    Given a persistent graph at a temporary path
    And I add a node with label "Person" named "Alice"
    And the forge instance is closed
    When I reopen the forge at the same path
    Then execute "MATCH (p:Person) RETURN p.name AS name" returns 1 row
    And the first row value for "name" is "Alice"
