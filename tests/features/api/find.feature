@api @find
Feature: Find API

  Scenario: find by text builds index lazily on first call
    Given a graph with a Paper node titled "Graph Neural Networks"
    When I find "graph neural networks" in label "Paper"
    Then the result is an Arrow Table
    And no index call was made before find

  Scenario: find by text returns score and matched_on columns
    Given a graph with a Paper node titled "Graph Neural Networks"
    When I find "graph neural networks" in label "Paper"
    Then the table has column "score"
    And the table has column "matched_on"
    And the first row value for "matched_on" is "text"

  @excluded-api-bdd @issue-352
  Scenario: find by vector returns score and matched_on set to vector
    Given a graph with a Paper node that has a stored vector embedding
    When I find by the stored vector in label "Paper"
    Then the table has column "score"
    And the first row value for "matched_on" is "vector"

  @excluded-api-bdd @issue-352
  Scenario: find with text and vector returns matched_on set to text+vector for fused results
    Given a graph with a Paper node titled "Graph Neural Networks" and a stored vector embedding
    When I find "graph neural networks" with the stored vector in label "Paper"
    Then the first row value for "matched_on" is "text+vector"

  Scenario: find result node ids are addressable in execute
    Given a graph with a Paper node titled "Graph Neural Networks"
    When I find "graph neural networks" in label "Paper"
    Then for each result row the id is valid in execute "MATCH (n) WHERE n.node_uuid = $id RETURN n"

  Scenario: find with label filters results to one node label
    Given a graph with a Paper node titled "test" and a Person node named "test"
    When I find "test" in label "Paper"
    Then all result rows have label "Paper"

  Scenario: find with limit caps result count
    Given a graph with 10 Paper nodes with similar titles
    When I find "graph" in label "Paper" with limit 3
    Then the table has at most 3 rows
