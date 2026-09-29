@api @lifecycle
Feature: Lifecycle State

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
