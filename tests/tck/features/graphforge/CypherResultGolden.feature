Feature: GraphForge Cypher result goldens

  Scenario: ordered property rows match the expected Arrow values
    Given any graph
    And having executed:
      """
      CREATE (a:Person {name: 'Alice', score: 3}), (b:Person {name: 'Bob', score: 2})
      """
    When executing query:
      """
      MATCH (p:Person)
      RETURN p.name AS name, p.score AS score
      ORDER BY p.score DESC
      """
    Then the result should be, in order:
      | name  | score |
      | 'Alice' | 3     |
      | 'Bob'   | 2     |
