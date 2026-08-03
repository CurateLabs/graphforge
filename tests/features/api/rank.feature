@api @rank
Feature: Rank API

  Background:
    Given a graph with 3 Person nodes connected by KNOWS edges

  Scenario: rank by pagerank returns Arrow Table with score column
    When I rank "Person" by "pagerank"
    Then the result is an Arrow Table
    And the table has column "score"
    And the table has 3 rows

  Scenario Outline: rank by <algorithm> returns a score column
    When I rank "Person" by "<algorithm>"
    Then the table has column "score"

    Examples:
      | algorithm                    |
      | degree                       |
      | betweenness                  |
      | closeness                    |
      | harmonic_closeness           |
      | eigenvector                  |
      | article_rank                 |
      | hits_hub                     |
      | hits_authority               |
      | celf                         |
      | clustering_coefficient       |
      | local_clustering_coefficient |
      | triangles                    |
      | k_core                       |
      | preferential_attachment     |
      | adamic_adar                 |
      | common_neighbors            |
      | resource_allocation         |
      | total_neighbors             |

  Scenario: rank with write property stores score on nodes
    When I rank "Person" by "pagerank" writing result to property "rank"
    Then execute "MATCH (p:Person) WHERE p.rank IS NOT NULL RETURN p.name" returns 3 rows

  Scenario: rank without write property does not mutate the graph
    When I rank "Person" by "pagerank"
    Then execute "MATCH (p:Person) WHERE p.rank IS NOT NULL RETURN p.name" returns 0 rows

  Scenario: rank via a relationship type filters edge traversal
    Given a graph with Person nodes connected by both KNOWS and FOLLOWS edges
    When I rank "Person" by "pagerank" via relationship type "KNOWS"
    Then the result is an Arrow Table
    And the table has column "score"

  Scenario: rank scores differ when treating edges as directed versus undirected
    Given a graph with Person nodes connected by directed KNOWS edges
    When I rank "Person" by "pagerank" treating edges as directed
    And I rank "Person" by "pagerank" treating edges as undirected
    Then the two score results are not identical
