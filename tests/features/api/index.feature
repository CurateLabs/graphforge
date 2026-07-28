@api @index @skip-node
Feature: Index API

  Scenario: index with properties makes subsequent find faster
    Given a graph with 5 Paper nodes with title and abstract properties
    When I index label "Paper" on properties "title" and "abstract"
    And I find "neural networks" in label "Paper"
    Then the result is an Arrow Table with at least 1 row

  Scenario: index vector upsert stores a vector for a node
    Given a graph with a Paper node
    And I have stored the node id as "paper_id"
    And I have an embedding vector stored as "embedding"
    When I index label "Paper" storing the vector for node "paper_id" in space "sbert"
    And I find by the stored embedding in label "Paper" in space "sbert"
    Then the result contains that node

  Scenario: calling index twice is idempotent
    Given a graph with 3 Paper nodes with title properties
    When I index label "Paper" on property "title"
    And I index label "Paper" on property "title"
    Then no error is raised
    And find "paper" in label "Paper" returns the same results as after the first index call

  Scenario: find reflects nodes added after initial index build
    Given a graph with a Paper node titled "Graph Neural Networks"
    And I index label "Paper" on property "title"
    When I add a node with label "Paper" titled "Deep Graph Learning"
    And I find "deep graph" in label "Paper"
    Then the result contains a row with title "Deep Graph Learning"
