@api @errors
Feature: Error Handling

  Scenario: ParseError is raised on invalid Cypher syntax
    Given an empty graph
    When I execute "METCH (n) RETRUN n"
    Then a ParseError is raised
    And the error includes a source span

  Scenario: ParseError message identifies the invalid token
    Given an empty graph
    When I execute "MATCH (n) RETURN n BLARG"
    Then a ParseError is raised


  Scenario: ExecutionError is raised on type mismatch
    Given a graph with a Person node with age stored as a string "not-a-number"
    When I execute "MATCH (p:Person) RETURN p.age + 1 AS result"
    Then an ExecutionError is raised

  Scenario: StorageError is raised on unreadable Parquet path
    Given a path that does not exist on disk
    When I open a graph at that path
    And I execute "MATCH (n) RETURN n"
    Then a StorageError is raised


  Scenario: ParseError on undefined variable in RETURN
    Given an empty graph
    When I execute "MATCH (n:Person) RETURN x.name AS name"
    Then a ParseError is raised


  Scenario: ParseError on aggregation used in WHERE clause
    Given a graph with 3 Person nodes
    When I execute "MATCH (n:Person) WHERE count(n) > 1 RETURN n"
    Then a ParseError is raised


  Scenario: ParseError on CREATE with undirected relationship
    Given an empty graph
    When I execute "CREATE (a:Person)-[:KNOWS]-(b:Person)"
    Then a ParseError is raised

  Scenario: ParseError on CREATE with multiple relationship types
    Given an empty graph
    When I execute "CREATE (a:Person)-[:KNOWS|LIKES]->(b:Person)"
    Then a ParseError is raised


  Scenario: ParseError on duplicate alias in WITH clause
    Given a graph with a Person node named "Alice"
    When I execute "MATCH (n:Person) WITH n.name AS x, n.name AS x RETURN x"
    Then a ParseError is raised


  Scenario: ExecutionError when deleting a node that has relationships without DETACH
    Given a graph with a Person node named "Alice" connected by a KNOWS edge to a Person node named "Bob"
    When I execute "MATCH (p:Person {name: 'Alice'}) DELETE p"
    Then an ExecutionError is raised


  Scenario: ExecutionError when a query references an unbound parameter
    Given an empty graph
    When I execute "MATCH (p:Person) WHERE p.name = $name RETURN p" without parameters
    Then an ExecutionError is raised


  Scenario: property access on NULL returns NULL without error
    Given a graph with a Person node named "Alice" without an age property
    When I execute "MATCH (p:Person) RETURN p.age AS age"
    Then the result is an Arrow Table
    And the first row value for "age" is null
