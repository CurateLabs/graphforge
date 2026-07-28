@api @cluster @skip-node
Feature: Cluster API

  Background:
    Given a graph with 4 Person nodes in two connected groups

  Scenario: cluster by louvain returns Arrow Table with community_id column
    When I cluster "Person" by "louvain"
    Then the result is an Arrow Table
    And the table has column "community_id"
    And the table has 4 rows

  Scenario: cluster by components returns community_id column
    When I cluster "Person" by "components"
    Then the table has column "community_id"

  Scenario: nodes in the same connected component get the same community_id
    Given a graph with 2 Person nodes connected by a KNOWS edge
    And 2 other Person nodes connected by a KNOWS edge but isolated from the first pair
    When I cluster "Person" by "components"
    Then the 2 connected nodes share the same community_id
    And the 2 isolated nodes share a different community_id

  Scenario: cluster with write property stores community_id on nodes
    When I cluster "Person" by "louvain" writing result to property "community"
    Then execute "MATCH (p:Person) WHERE p.community IS NOT NULL RETURN p.name" returns 4 rows

  Scenario: cluster without write property does not mutate the graph
    When I cluster "Person" by "louvain"
    Then execute "MATCH (p:Person) WHERE p.community IS NOT NULL RETURN p.name" returns 0 rows
