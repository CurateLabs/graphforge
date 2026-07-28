# TCK Test Scenario Inventory
Comprehensive inventory of all OpenCypher Technology Compatibility Kit (TCK) test scenarios in GraphForge.
**Generated:** 2026-02-17 21:56:38 UTC
**Total feature files:** 222
**Total scenarios:** 1626

## Table of Contents
- [clauses](#clauses) (93 files, 827 scenarios)
- [expressions](#expressions) (125 files, 758 scenarios)
- [legacy](#legacy) (2 files, 11 scenarios)
- [useCases](#usecases) (2 files, 30 scenarios)

## Summary Statistics
| Category | Feature Files | Scenarios | Avg Scenarios/File |
|----------|---------------|-----------|--------------------|
| clauses | 93 | 827 | 8.9 |
| expressions | 125 | 758 | 6.1 |
| legacy | 2 | 11 | 5.5 |
| useCases | 2 | 30 | 15.0 |
| **TOTAL** | **222** | **1626** | **7.3** |

## Detailed Inventory by Category

### clauses

**93 feature files, 827 scenarios**

#### `official/clauses/call/Call1.feature`

**Feature:** Call1 - Basic procedure calling

**Scenarios:** 16

1. [1] Standalone call to procedure that takes no arguments and yields no results (Scenario, line 33)
2. [2] Standalone call to procedure that takes no arguments and yields no results, called with implicit arguments (Scenario, line 44)
3. [3] In-query call to procedure that takes no arguments and yields no results (Scenario, line 55)
4. [4] In-query call to procedure that takes no arguments and yields no results and consumes no rows (Scenario, line 69)
5. [5] Standalone call to STRING procedure that takes no arguments (Scenario, line 92)
6. [6] In-query call to STRING procedure that takes no arguments (Scenario, line 110)
7. [7] Standalone call to procedure should fail if explicit argument is missing (Scenario, line 129)
8. [8] In-query call to procedure should fail if explicit argument is missing (Scenario, line 139)
9. [9] Standalone call to procedure should fail if too many explicit argument are given (Scenario, line 150)
10. [10] In-query call to procedure should fail if too many explicit argument are given (Scenario, line 160)
11. [11] Standalone call to procedure should fail if implicit argument is missing (Scenario, line 171)
12. [12] In-query call to procedure that has outputs fails if no outputs are yielded (Scenario, line 183)
13. [13] Standalone call to unknown procedure should fail (Scenario, line 194)
14. [14] In-query call to unknown procedure should fail (Scenario, line 202)
15. [15] In-query procedure call should fail if shadowing an already bound variable (Scenario, line 211)
16. [16] In-query procedure call should fail if one of the argument expressions uses an aggregation function (Scenario, line 226)

#### `official/clauses/call/Call2.feature`

**Feature:** Call2 - Procedure arguments

**Scenarios:** 6

1. [1] In-query call to procedure with explicit arguments (Scenario, line 33)
2. [2] Standalone call to procedure with explicit arguments (Scenario, line 53)
3. [3] Standalone call to procedure with implicit arguments (Scenario, line 72)
4. [4] In-query call to procedure that takes arguments fails when trying to pass them implicitly (Scenario, line 95)
5. [5] Standalone call to procedure should fail if input type is wrong (Scenario, line 106)
6. [6] In-query call to procedure should fail if input type is wrong (Scenario, line 116)

#### `official/clauses/call/Call3.feature`

**Feature:** Call3 - Assignable-type arguments

**Scenarios:** 6

1. [1] Standalone call to procedure with argument of type NUMBER accepts value of type INTEGER (Scenario, line 33)
2. [2] In-query call to procedure with argument of type NUMBER accepts value of type INTEGER (Scenario, line 48)
3. [3] Standalone call to procedure with argument of type NUMBER accepts value of type FLOAT (Scenario, line 64)
4. [4] In-query call to procedure with argument of type NUMBER accepts value of type FLOAT (Scenario, line 79)
5. [5] Standalone call to procedure with argument of type FLOAT accepts value of type INTEGER (Scenario, line 95)
6. [6] In-query call to procedure with argument of type FLOAT accepts value of type INTEGER (Scenario, line 109)

#### `official/clauses/call/Call4.feature`

**Feature:** Call4 - Null Arguments

**Scenarios:** 2

1. [1] Standalone call to procedure with null argument (Scenario, line 33)
2. [2] In-query call to procedure with null argument (Scenario, line 47)

#### `official/clauses/call/Call5.feature`

**Feature:** Call5 - Results projection

**Scenarios:** 8

1. [1] Explicit procedure result projection (Scenario, line 33)
2. [2] Explicit procedure result projection with RETURN * (Scenario, line 48)
3. [3] The order of yield items is irrelevant (Scenario Outline, line 63)
4. [4] Rename outputs to unbound variable names (Scenario Outline, line 83)
5. [5] Fail on renaming to an already bound variable name (Scenario, line 113)
6. [6] Fail on renaming all outputs to the same variable name (Scenario, line 125)
7. [7] Fail on in-query call to procedure with YIELD * (Scenario, line 138)
8. [8] Allow standalone call to procedure with YIELD * (Scenario, line 155)

#### `official/clauses/call/Call6.feature`

**Feature:** Call6 - Call clause interoperation with other clauses

**Scenarios:** 3

1. [1] Calling the same STRING procedure twice using the same outputs in each call (Scenario, line 33)
2. [2] Project procedure results between query scopes with WITH clause (Scenario, line 54)
3. [3] Project procedure results between query scopes with WITH clause and rename the projection (Scenario, line 69)

#### `official/clauses/create/Create1.feature`

**Feature:** Create1 - Creating nodes

**Scenarios:** 20

1. [1] Create a single node (Scenario, line 33)
2. [2] Create two nodes (Scenario, line 43)
3. [3] Create a single node with a label (Scenario, line 53)
4. [4] Create two nodes with same label (Scenario, line 64)
5. [5] Create a single node with multiple labels (Scenario, line 75)
6. [6] Create three nodes with multiple labels (Scenario, line 86)
7. [7] Create a single node with a property (Scenario, line 97)
8. [8] Create a single node with a property and return it (Scenario, line 108)
9. [9] Create a single node with two properties (Scenario, line 122)
10. [10] Create a single node with two properties and return them (Scenario, line 133)
11. [11] Create a single node with null properties should not return those properties (Scenario, line 147)
12. [12] CREATE does not lose precision on large integers (Scenario, line 161)
13. [13] Fail when creating a node that is already bound (Scenario, line 176)
14. [14] Fail when creating a node with properties that is already bound (Scenario, line 185)
15. [15] Fail when adding a new label predicate on a node that is already bound 1 (Scenario, line 195)
16. [16] Fail when adding new label predicate on a node that is already bound 2 (Scenario, line 205)
17. [17] Fail when adding new label predicate on a node that is already bound 3 (Scenario, line 214)
18. [18] Fail when adding new label predicate on a node that is already bound 4 (Scenario, line 223)
19. [19] Fail when adding new label predicate on a node that is already bound 5 (Scenario, line 232)
20. [20] Fail when creating a node using undefined variable in pattern (Scenario, line 241)

#### `official/clauses/create/Create2.feature`

**Feature:** Create2 - Creating relationships

**Scenarios:** 24

1. [1] Create two nodes and a single relationship in a single pattern (Scenario, line 33)
2. [2] Create two nodes and a single relationship in separate patterns (Scenario, line 44)
3. [3] Create two nodes and a single relationship in separate clauses (Scenario, line 56)
4. [4] Create two nodes and a single relationship in the reverse direction (Scenario, line 69)
5. [5] Create a single relationship between two existing nodes (Scenario, line 89)
6. [6] Create a single relationship between two existing nodes in the reverse direction (Scenario, line 105)
7. [7] Create a single node and a single self loop in a single pattern (Scenario, line 129)
8. [8] Create a single node and a single self loop in separate patterns (Scenario, line 140)
9. [9] Create a single node and a single self loop in separate clauses (Scenario, line 152)
10. [10] Create a single self loop on an existing node (Scenario, line 164)
11. [11] Create a single relationship and an end node on an existing starting node (Scenario, line 179)
12. [12] Create a single relationship and a starting node on an existing end node (Scenario, line 204)
13. [13] Create a single relationship with a property (Scenario, line 229)
14. [14] Create a single relationship with a property and return it (Scenario, line 241)
15. [15] Create a single relationship with two properties (Scenario, line 256)
16. [16] Create a single relationship with two properties and return them (Scenario, line 268)
17. [17] Create a single relationship with null properties should not return those properties (Scenario, line 283)
18. [18] Fail when creating a relationship without a type (Scenario, line 298)
19. [19] Fail when creating a relationship without a direction (Scenario, line 306)
20. [20] Fail when creating a relationship with two directions (Scenario, line 314)
21. [21] Fail when creating a relationship with more than one type (Scenario, line 322)
22. [22] Fail when creating a variable-length relationship (Scenario, line 330)
23. [23] Fail when creating a relationship that is already bound (Scenario, line 338)
24. [24] Fail when creating a relationship using undefined variable in pattern (Scenario, line 347)

#### `official/clauses/create/Create3.feature`

**Feature:** Create3 - Interoperation with other clauses

**Scenarios:** 13

1. [1] MATCH-CREATE (Scenario, line 33)
2. [2] WITH-CREATE (Scenario, line 48)
3. [3] MATCH-CREATE-WITH-CREATE (Scenario, line 65)
4. [4] MATCH-CREATE: Newly-created nodes not visible to preceding MATCH (Scenario, line 83)
5. [5] WITH-CREATE: Nodes are not created when aliases are applied to variable names (Scenario, line 98)
6. [6] WITH-CREATE: Only a single node is created when an alias is applied to a variable name (Scenario, line 118)
7. [7] WITH-CREATE: Nodes are not created when aliases are applied to variable names multiple times (Scenario, line 138)
8. [8] WITH-CREATE: Only a single node is created when an alias is applied to a variable name multiple times (Scenario, line 160)
9. [9] WITH-CREATE: A bound node should be recognized after projection with WITH + WITH (Scenario, line 182)
10. [10] WITH-UNWIND-CREATE: A bound node should be recognized after projection with WITH + UNWIND (Scenario, line 197)
11. [11] WITH-MERGE-CREATE: A bound node should be recognized after projection with WITH + MERGE node (Scenario, line 212)
12. [12] WITH-MERGE-CREATE: A bound node should be recognized after projection with WITH + MERGE pattern (Scenario, line 227)
13. [13] Merge followed by multiple creates (Scenario, line 244)

#### `official/clauses/create/Create4.feature`

**Feature:** Create4 - Large Create Query

**Scenarios:** 2

1. [1] Generate the movie graph (Scenario, line 33)
2. [2] Many CREATE clauses (Scenario, line 579)

#### `official/clauses/create/Create5.feature`

**Feature:** Create5 - Multiple hops create patterns

**Scenarios:** 5

1. [1] Create a pattern with multiple hops (Scenario, line 33)
2. [2] Create a pattern with multiple hops in the reverse direction (Scenario, line 53)
3. [3] Create a pattern with multiple hops in varying directions (Scenario, line 73)
4. [4] Create a pattern with multiple hops with multiple types and varying directions (Scenario, line 93)
5. [5] Create a pattern with multiple hops and varying directions (Scenario, line 112)

#### `official/clauses/create/Create6.feature`

**Feature:** Create6 - Persistence of create clause side effects

**Scenarios:** 14

1. [1] Limiting to zero results after creating nodes affects the result set but not the side effects (Scenario, line 33)
2. [2] Skipping all results after creating nodes affects the result set but not the side effects (Scenario, line 48)
3. [3] Skipping and limiting to a few results after creating nodes does not affect the result set nor the side effects (Scenario, line 63)
4. [4] Skipping zero result and limiting to all results after creating nodes does not affect the result set nor the side effects (Scenario, line 81)
5. [5] Filtering after creating nodes affects the result set but not the side effects (Scenario, line 102)
6. [6] Aggregating in `RETURN` after creating nodes affects the result set but not the side effects (Scenario, line 121)
7. [7] Aggregating in `WITH` after creating nodes affects the result set but not the side effects (Scenario, line 137)
8. [8] Limiting to zero results after creating relationships affects the result set but not the side effects (Scenario, line 154)
9. [9] Skipping all results after creating relationships affects the result set but not the side effects (Scenario, line 169)
10. [10] Skipping and limiting to a few results after creating relationships does not affect the result set nor the side effects (Scenario, line 184)
11. [11] Skipping zero result and limiting to all results after creating relationships does not affect the result set nor the side effects (Scenario, line 202)
12. [12] Filtering after creating relationships affects the result set but not the side effects (Scenario, line 223)
13. [13] Aggregating in `RETURN` after creating relationships affects the result set but not the side effects (Scenario, line 242)
14. [14] Aggregating in `WITH` after creating relationships affects the result set but not the side effects (Scenario, line 258)

#### `official/clauses/delete/Delete1.feature`

**Feature:** Delete1 - Deleting nodes

**Scenarios:** 8

1. [1] Delete nodes (Scenario, line 33)
2. [2] Detach delete node (Scenario, line 48)
3. [3] Detach deleting connected nodes and relationships (Scenario, line 63)
4. [4] Delete on null node (Scenario, line 83)
5. [5] Ignore null when deleting node (Scenario, line 93)
6. [6] Detach delete on null node (Scenario, line 106)
7. [7] Failing when deleting connected nodes (Scenario, line 116)
8. [8] Failing when deleting a label (Scenario, line 132)

#### `official/clauses/delete/Delete2.feature`

**Feature:** Delete2 - Deleting relationships

**Scenarios:** 5

1. [1] Delete relationships (Scenario, line 33)
2. [2] Delete optionally matched relationship (Scenario, line 49)
3. [3] Delete relationship with bidirectional matching (Scenario, line 65)
4. [4] Ignore null when deleting relationship (Scenario, line 83)
5. [5] Failing when deleting a relationship type (Scenario, line 96)

#### `official/clauses/delete/Delete3.feature`

**Feature:** Delete3 - Deleting named paths

**Scenarios:** 2

1. [1] Detach deleting paths (Scenario, line 33)
2. [2] Delete on null path (Scenario, line 53)

#### `official/clauses/delete/Delete4.feature`

**Feature:** Delete4 - Delete clause interoperation with other clauses

**Scenarios:** 3

1. [1] Undirected expand followed by delete and count (Scenario, line 33)
2. [2] Undirected variable length expand followed by delete and count (Scenario, line 52)
3. [3] Create and delete in same query (Scenario, line 73)

#### `official/clauses/delete/Delete5.feature`

**Feature:** Delete5 - Delete clause interoperation with built-in data types

**Scenarios:** 9

1. [1] Delete node from a list (Scenario, line 33)
2. [2] Delete relationship from a list (Scenario, line 56)
3. [3] Delete nodes from a map (Scenario, line 78)
4. [4] Delete relationships from a map (Scenario, line 95)
5. [5] Detach delete nodes from nested map/list (Scenario, line 113)
6. [6] Delete relationships from nested map/list (Scenario, line 132)
7. [7] Delete paths from nested map/list (Scenario, line 150)
8. [8] Failing when using undefined variable in DELETE (Scenario, line 170)
9. [9] Failing when deleting an integer expression (Scenario, line 179)

#### `official/clauses/delete/Delete6.feature`

**Feature:** Delete6 - Persistence of delete clause side effects

**Scenarios:** 14

1. [1] Limiting to zero results after deleting nodes affects the result set but not the side effects (Scenario, line 33)
2. [2] Skipping all results after deleting nodes affects the result set but not the side effects (Scenario, line 53)
3. [3] Skipping and limiting to a few results after deleting nodes affects the result set but not the side effects (Scenario, line 73)
4. [4] Skipping zero results and limiting to all results after deleting nodes does not affect the result set nor the side effects (Scenario, line 99)
5. [5] Filtering after deleting nodes affects the result set but not the side effects (Scenario, line 128)
6. [6] Aggregating in `RETURN` after deleting nodes affects the result set but not the side effects (Scenario, line 156)
7. [7] Aggregating in `WITH` after deleting nodes affects the result set but not the side effects (Scenario, line 181)
8. [8] Limiting to zero results after deleting relationships affects the result set but not the side effects (Scenario, line 207)
9. [9] Skipping all results after deleting relationships affects the result set but not the side effects (Scenario, line 226)
10. [10] Skipping and limiting to a few results after deleting relationships affects the result set but not the side effects (Scenario, line 245)
11. [11] Skipping zero result and limiting to all results after deleting relationships does not affect the result set nor the side effects (Scenario, line 270)
12. [12] Filtering after deleting relationships affects the result set but not the side effects (Scenario, line 298)
13. [13] Aggregating in `RETURN` after deleting relationships affects the result set but not the side effects (Scenario, line 325)
14. [14] Aggregating in `WITH` after deleting relationships affects the result set but not the side effects (Scenario, line 349)

#### `official/clauses/match-where/MatchWhere1.feature`

**Feature:** MatchWhere1 - Filter single variable

**Scenarios:** 15

1. [1] Filter node with node label predicate on multi variables with multiple bindings (Scenario, line 33)
2. [2] Filter node with node label predicate on multi variables without any bindings (Scenario, line 50)
3. [3] Filter node with property predicate on a single variable with multiple bindings (Scenario, line 66)
4. [4] Filter start node of relationship with property predicate on multi variables with multiple bindings (Scenario, line 83)
5. [5] Filter end node of relationship with property predicate on multi variables with multiple bindings (Scenario, line 103)
6. [6] Filter node with a parameter in a property predicate on multi variables with one binding (Scenario, line 120)
7. [7] Filter relationship with relationship type predicate on multi variables with multiple bindings (Scenario, line 139)
8. [8] Filter relationship with property predicate on multi variables with multiple bindings (Scenario, line 160)
9. [9] Filter relationship with a parameter in a property predicate on multi variables with one binding (Scenario, line 177)
10. [10] Filter node with disjunctive property predicate on single variables with multiple bindings (Scenario, line 196)
11. [11] Filter relationship with disjunctive relationship type predicate on multi variables with multiple bindings (Scenario, line 216)
12. [12] Filter path with path length predicate on multi variables with one binding (Scenario, line 239)
13. [13] Filter path with false path length predicate on multi variables with one binding (Scenario, line 256)
14. [14] Fail when filtering path with property predicate (Scenario, line 272)
15. [15] Fail on aggregation in WHERE (Scenario, line 283)

#### `official/clauses/match-where/MatchWhere2.feature`

**Feature:** MatchWhere2 - Filter multiple variables

**Scenarios:** 2

1. [1] Filter nodes with conjunctive two-part property predicate on multi variables with multiple bindings (Scenario, line 33)
2. [2] Filter node with conjunctive multi-part property predicates on multi variables with multiple bindings (Scenario, line 58)

#### `official/clauses/match-where/MatchWhere3.feature`

**Feature:** MatchWhere3 - Equi-Joins on variables

**Scenarios:** 3

1. [1] Join between node identities (Scenario, line 33)
2. [2] Join between node properties of disconnected nodes (Scenario, line 51)
3. [3] Join between node properties of adjacent nodes (Scenario, line 71)

#### `official/clauses/match-where/MatchWhere4.feature`

**Feature:** MatchWhere4 - Non-Equi-Joins on variables

**Scenarios:** 2

1. [1] Join nodes on inequality (Scenario, line 33)
2. [2] Join with disjunctive multi-part predicates including patterns (Scenario, line 51)

#### `official/clauses/match-where/MatchWhere5.feature`

**Feature:** MatchWhere5 - Filter on predicate resulting in null

**Scenarios:** 4

1. [1] Filter out on null (Scenario, line 33)
2. [2] Filter out on null if the AND'd predicate evaluates to false (Scenario, line 54)
3. [3] Filter out on null if the AND'd predicate evaluates to true (Scenario, line 75)
4. [4] Do not filter out on null if the OR'd predicate evaluates to true (Scenario, line 96)

#### `official/clauses/match-where/MatchWhere6.feature`

**Feature:** MatchWhere6 - Filter optional matches

**Scenarios:** 8

1. [1] Filter node with node label predicate on multi variables with multiple bindings after MATCH and OPTIONAL MATCH (Scenario, line 33)
2. [2] Filter node with false node label predicate after OPTIONAL MATCH (Scenario, line 55)
3. [3] Filter node with property predicate on multi variables with multiple bindings after OPTIONAL MATCH (Scenario, line 78)
4. [4] Do not fail when predicates on optionally matched and missed nodes are invalid (Scenario, line 101)
5. [5] Matching and optionally matching with unbound nodes and equality predicate in reverse direction (Scenario, line 120)
6. [6] Join nodes on non-equality of properties – OPTIONAL MATCH and WHERE (Scenario, line 140)
7. [7] Join nodes on non-equality of properties – OPTIONAL MATCH on two relationships and WHERE (Scenario, line 163)
8. [8] Join nodes on non-equality of properties – Two OPTIONAL MATCH clauses and WHERE (Scenario, line 186)

#### `official/clauses/match/Match1.feature`

**Feature:** Match1 - Match nodes

**Scenarios:** 11

1. [1] Match non-existent nodes returns empty (Scenario, line 33)
2. [2] Matching all nodes (Scenario, line 44)
3. [3] Matching nodes using multiple labels (Scenario, line 62)
4. [4] Simple node inline property predicate (Scenario, line 81)
5. [5] Use multiple MATCH clauses to do a Cartesian product (Scenario, line 97)
6. [6] Fail when using parameter as node predicate in MATCH (Scenario, line 123)
7. [7] Fail when a relationship has the same variable in a preceding MATCH (Scenario Outline, line 132)
8. [8] Fail when a path has the same variable in a preceding MATCH (Scenario Outline, line 156)
9. [9] Fail when a relationship has the same variable in the same pattern (Scenario Outline, line 185)
10. [10] Fail when a path has the same variable in the same pattern (Scenario Outline, line 222)
11. [11] Fail when matching a node variable bound to a value (Scenario Outline, line 254)

#### `official/clauses/match/Match2.feature`

**Feature:** Match2 - Match relationships

**Scenarios:** 13

1. [1] Match non-existent relationships returns empty (Scenario, line 33)
2. [2] Matching a relationship pattern using a label predicate on both sides (Scenario, line 44)
3. [3] Matching a self-loop with an undirected relationship pattern (Scenario, line 63)
4. [4] Matching a self-loop with a directed relationship pattern (Scenario, line 80)
5. [5] Match relationship with inline property value (Scenario, line 97)
6. [6] Match relationships with multiple types (Scenario, line 113)
7. [7] Matching twice with conflicting relationship types on same relationship (Scenario, line 135)
8. [8] Fail when using parameter as relationship predicate in MATCH (Scenario, line 152)
9. [9] Fail when a node has the same variable in a preceding MATCH (Scenario Outline, line 161)
10. [10] Fail when a path has the same variable in a preceding MATCH (Scenario Outline, line 200)
11. [11] Fail when a node has the same variable in the same pattern (Scenario Outline, line 232)
12. [12] Fail when a path has the same variable in the same pattern (Scenario Outline, line 261)
13. [13] Fail when matching a relationship variable bound to a value (Scenario Outline, line 281)

#### `official/clauses/match/Match3.feature`

**Feature:** Match3 - Match fixed length patterns

**Scenarios:** 30

1. [1] Get neighbours (Scenario, line 33)
2. [2] Directed match of a simple relationship (Scenario, line 49)
3. [3] Undirected match on simple relationship graph (Scenario, line 65)
4. [4] Get two related nodes (Scenario, line 82)
5. [5] Return two subgraphs with bound undirected relationship (Scenario, line 101)
6. [6] Matching a relationship pattern using a label predicate (Scenario, line 118)
7. [7] Matching nodes with many labels (Scenario, line 136)
8. [8] Matching using relationship predicate with multiples of the same type (Scenario, line 154)
9. [9] Get related to related to (Scenario, line 171)
10. [10] Matching using self-referencing pattern returns no result (Scenario, line 187)
11. [11] Undirected match in self-relationship graph (Scenario, line 204)
12. [12] Undirected match of self-relationship in self-relationship graph (Scenario, line 220)
13. [13] Directed match on self-relationship graph (Scenario, line 236)
14. [14] Directed match of self-relationship on self-relationship graph (Scenario, line 252)
15. [15] Mixing directed and undirected pattern parts with self-relationship, simple (Scenario, line 268)
16. [16] Mixing directed and undirected pattern parts with self-relationship, undirected (Scenario, line 287)
17. [17] Handling cyclic patterns (Scenario, line 310)
18. [18] Handling cyclic patterns when separated into two parts (Scenario, line 329)
19. [19] Two bound nodes pointing to the same node (Scenario, line 348)
20. [20] Three bound nodes pointing to the same node (Scenario, line 371)
21. [21] Three bound nodes pointing to the same node with extra connections (Scenario, line 396)
22. [22] Returning bound nodes that are not part of the pattern (Scenario, line 432)
23. [23] Matching disconnected patterns (Scenario, line 451)
24. [24] Matching twice with duplicate relationship types on same relationship (Scenario, line 473)
25. [25] Matching twice with an additional node label (Scenario, line 491)
26. [26] Matching twice with a duplicate predicate (Scenario, line 508)
27. [27] Matching from null nodes should return no results owing to finding no matches (Scenario, line 526)
28. [28] Matching from null nodes should return no results owing to matches being filtered out (Scenario, line 539)
29. [29] Fail when re-using a relationship in the same pattern (Scenario, line 556)
30. [30] Fail when using a list or nodes as a node (Scenario, line 565)

#### `official/clauses/match/Match4.feature`

**Feature:** Match4 - Match variable length patterns scenarios

**Scenarios:** 10

1. [1] Handling fixed-length variable length pattern (Scenario, line 33)
2. [2] Simple variable length pattern (Scenario, line 49)
3. [3] Zero-length variable length pattern in the middle of the pattern (Scenario, line 71)
4. [4] Matching longer variable length paths (Scenario, line 93)
5. [5] Matching variable length pattern with property predicate (Scenario, line 116)
6. [6] Matching variable length patterns from a bound node (Scenario, line 134)
7. [7] Matching variable length patterns including a bound relationship (Scenario, line 153)
8. [8] Matching relationships into a list and matching variable length using the list (Scenario, line 176)
9. [9] Fail when asterisk operator is missing (Scenario, line 198)
10. [10] Fail on negative bound (Scenario, line 241)

#### `official/clauses/match/Match5.feature`

**Feature:** Match5 - Match variable length patterns over given graphs scenarios

**Scenarios:** 29

1. [1] Handling unbounded variable length match (Scenario, line 69)
2. [2] Handling explicitly unbounded variable length match (Scenario, line 94)
3. [3] Handling single bounded variable length match 1 (Scenario, line 119)
4. [4] Handling single bounded variable length match 2 (Scenario, line 131)
5. [5] Handling single bounded variable length match 3 (Scenario, line 144)
6. [6] Handling upper and lower bounded variable length match 1 (Scenario, line 159)
7. [7] Handling upper and lower bounded variable length match 2 (Scenario, line 177)
8. [8] Handling symmetrically bounded variable length match, bounds are zero (Scenario, line 194)
9. [9] Handling symmetrically bounded variable length match, bounds are one (Scenario, line 206)
10. [10] Handling symmetrically bounded variable length match, bounds are two (Scenario, line 219)
11. [11] Handling upper and lower bounded variable length match, empty interval 1 (Scenario, line 234)
12. [12] Handling upper and lower bounded variable length match, empty interval 2 (Scenario, line 245)
13. [13] Handling upper bounded variable length match, empty interval (Scenario, line 256)
14. [14] Handling upper bounded variable length match 1 (Scenario, line 267)
15. [15] Handling upper bounded variable length match 2 (Scenario, line 280)
16. [16] Handling lower bounded variable length match 1 (Scenario, line 297)
17. [17] Handling lower bounded variable length match 2 (Scenario, line 323)
18. [18] Handling lower bounded variable length match 3 (Scenario, line 348)
19. [19] Handling a variable length relationship and a standard relationship in chain, zero length 1 (Scenario, line 371)
20. [20] Handling a variable length relationship and a standard relationship in chain, zero length 2 (Scenario, line 384)
21. [21] Handling a variable length relationship and a standard relationship in chain, single length 1 (Scenario, line 397)
22. [22] Handling a variable length relationship and a standard relationship in chain, single length 2 (Scenario, line 412)
23. [23] Handling a variable length relationship and a standard relationship in chain, longer 1 (Scenario, line 427)
24. [24] Handling a variable length relationship and a standard relationship in chain, longer 2 (Scenario, line 446)
25. [25] Handling a variable length relationship and a standard relationship in chain, longer 3 (Scenario, line 465)
26. [26] Handling mixed relationship patterns and directions 1 (Scenario, line 500)
27. [27] Handling mixed relationship patterns and directions 2 (Scenario, line 541)
28. [28] Handling mixed relationship patterns 1 (Scenario, line 584)
29. [29] Handling mixed relationship patterns 2 (Scenario, line 619)

#### `official/clauses/match/Match6.feature`

**Feature:** Match6 - Match named paths scenarios

**Scenarios:** 25

1. [1] Zero-length named path (Scenario, line 33)
2. [2] Return a simple path (Scenario, line 49)
3. [3] Return a three node path (Scenario, line 66)
4. [4] Respecting direction when matching non-existent path (Scenario, line 82)
5. [5] Path query should return results in written order (Scenario, line 98)
6. [6] Handling direction of named paths (Scenario, line 114)
7. [7] Respecting direction when matching existing path (Scenario, line 130)
8. [8] Respecting direction when matching non-existent path with multiple directions (Scenario, line 147)
9. [9] Longer path query should return results in written order (Scenario, line 164)
10. [10] Named path with alternating directed/undirected relationships (Scenario, line 180)
11. [11] Named path with multiple alternating directed/undirected relationships (Scenario, line 198)
12. [12] Matching path with multiple bidirectional relationships (Scenario, line 217)
13. [13] Matching path with both directions should respect other directions (Scenario, line 238)
14. [14] Named path with undirected fixed variable length pattern (Scenario, line 257)
15. [15] Variable-length named path (Scenario, line 281)
16. [16] Return a var length path (Scenario, line 297)
17. [17] Return a named var length path of length zero (Scenario, line 314)
18. [18] Undirected named path (Scenario, line 332)
19. [19] Variable length relationship without lower bound (Scenario, line 350)
20. [20] Variable length relationship without bounds (Scenario, line 370)
21. [21] Fail when a node has the same variable in a preceding MATCH (Scenario Outline, line 390)
22. [22] Fail when a relationship has the same variable in a preceding MATCH (Scenario Outline, line 417)
23. [23] Fail when a node has the same variable in the same pattern (Scenario Outline, line 445)
24. [24] Fail when a relationship has the same variable in the same pattern (Scenario Outline, line 477)
25. [25] Fail when matching a path variable bound to a value (Scenario Outline, line 509)

#### `official/clauses/match/Match7.feature`

**Feature:** Match7 - Optional match

**Scenarios:** 31

1. [1] Simple OPTIONAL MATCH on empty graph (Scenario, line 34)
2. [2] OPTIONAL MATCH with previously bound nodes (Scenario, line 46)
3. [3] OPTIONAL MATCH and bound nodes (Scenario, line 63)
4. [4] Optionally matching relationship with bound nodes in reverse direction (Scenario, line 85)
5. [5] Optionally matching relationship with a relationship that is already bound (Scenario, line 104)
6. [6] Optionally matching relationship with a relationship and node that are both already bound (Scenario, line 123)
7. [7] MATCH with OPTIONAL MATCH in longer pattern (Scenario, line 142)
8. [8] Longer pattern with bound nodes without matches (Scenario, line 161)
9. [9] Longer pattern with bound nodes (Scenario, line 183)
10. [10] Optionally matching from null nodes should return null (Scenario, line 205)
11. [11] Return two subgraphs with bound undirected relationship and optional relationship (Scenario, line 219)
12. [12] Variable length optional relationships (Scenario, line 238)
13. [13] Variable length optional relationships with bound nodes (Scenario, line 263)
14. [14] Variable length optional relationships with length predicates (Scenario, line 285)
15. [15] Variable length patterns and nulls (Scenario, line 307)
16. [16] Optionally matching named paths - null result (Scenario, line 325)
17. [17] Optionally matching named paths - existing result (Scenario, line 347)
18. [18] Named paths inside optional matches with node predicates (Scenario, line 367)
19. [19] Optionally matching named paths with single and variable length patterns (Scenario, line 389)
20. [20] Variable length optional relationships with bound nodes, no matches (Scenario, line 407)
21. [21] Handling optional matches between nulls (Scenario, line 429)
22. [22] MATCH after OPTIONAL MATCH (Scenario, line 453)
23. [23] OPTIONAL MATCH with labels on the optional end node (Scenario, line 477)
24. [24] Optionally matching self-loops (Scenario, line 498)
25. [25] Optionally matching self-loops without matches (Scenario, line 520)
26. [26] Handling correlated optional matches; first does not match implies second does not match (Scenario, line 545)
27. [27] Handling optional matches between optionally matched entities (Scenario, line 568)
28. [28] Handling optional matches with inline label predicate (Scenario, line 593)
29. [29] Satisfies the open world assumption, relationships between same nodes (Scenario, line 615)
30. [30] Satisfies the open world assumption, single relationship (Scenario, line 634)
31. [31] Satisfies the open world assumption, relationships between different nodes (Scenario, line 652)

#### `official/clauses/match/Match8.feature`

**Feature:** Match8 - Match clause interoperation with other clauses

**Scenarios:** 3

1. [1] Pattern independent of bound variables results in cross product (Scenario, line 33)
2. [2] Counting rows after MATCH, MERGE, OPTIONAL MATCH (Scenario, line 54)
3. [3] Matching and disregarding output, then matching again (Scenario, line 75)

#### `official/clauses/match/Match9.feature`

**Feature:** Match9 - Match deprecated scenarios

**Scenarios:** 9

1. [1] Variable length relationship variables are lists of relationships (Scenario, line 33)
2. [2] Return relationships by collecting them as a list - directed, one way (Scenario, line 54)
3. [3] Return relationships by collecting them as a list - undirected, starting from two extremes (Scenario, line 70)
4. [4] Return relationships by collecting them as a list - undirected, starting from one extreme (Scenario, line 87)
5. [5] Variable length pattern with label predicate on both sides (Scenario, line 103)
6. [6] Matching relationships into a list and matching variable length using the list, with bound nodes (Scenario, line 122)
7. [7] Matching relationships into a list and matching variable length using the list, with bound nodes, wrong direction (Scenario, line 143)
8. [8] Variable length relationship in OPTIONAL MATCH (Scenario, line 163)
9. [9] Optionally matching named paths with variable length patterns (Scenario, line 182)

#### `official/clauses/merge/Merge1.feature`

**Feature:** Merge1 - Merge node

**Scenarios:** 17

1. [1] Merge node when no nodes exist (Scenario, line 33)
2. [2] Merge node with label (Scenario, line 46)
3. [3] Merge node with label when it exists (Scenario, line 60)
4. [4] Merge node should create when it doesn't match, properties (Scenario, line 76)
5. [5] Merge node should create when it doesn't match, properties and label (Scenario, line 94)
6. [6] Merge node with prop and label (Scenario, line 112)
7. [7] Merge should work when finding multiple elements (Scenario, line 128)
8. [8] Merge should handle argument properly (Scenario, line 141)
9. [9] Merge should support updates while merging (Scenario, line 159)
10. [10] Merge must properly handle multiple labels (Scenario, line 192)
11. [11] Merge should be able to merge using property of bound node (Scenario, line 211)
12. [12] Merge should be able to merge using property of freshly created node (Scenario, line 233)
13. [13] Merge should bind a path (Scenario, line 245)
14. [14] Merges should not be able to match on deleted nodes (Scenario, line 259)
15. [15] Fail when merge a node that is already bound (Scenario, line 282)
16. [16] Fail when using parameter as node predicate in MERGE (Scenario, line 291)
17. [17] Fail on merging node with null property (Scenario, line 300)

#### `official/clauses/merge/Merge2.feature`

**Feature:** Merge2 - Merge node - on create

**Scenarios:** 6

1. [1] Merge node with label add label on create (Scenario, line 33)
2. [2] ON CREATE on created nodes (Scenario, line 48)
3. [3] Merge node with label add property on create (Scenario, line 60)
4. [4] Merge node with label add property on update when it exists (Scenario, line 76)
5. [5] Merge should be able to use properties of bound node in ON CREATE (Scenario, line 93)
6. [6] Fail when using undefined variable in ON CREATE (Scenario, line 116)

#### `official/clauses/merge/Merge3.feature`

**Feature:** Merge3 - Merge node - on match

**Scenarios:** 5

1. [1] Merge should be able to set labels on match (Scenario, line 33)
2. [2] Merge node with label add label on match when it exists (Scenario, line 48)
3. [3] Merge node and set property on match (Scenario, line 66)
4. [4] Merge should be able to use properties of bound node in ON MATCH (Scenario, line 84)
5. [5] Fail when using undefined variable in ON MATCH (Scenario, line 107)

#### `official/clauses/merge/Merge4.feature`

**Feature:** Merge4 - Merge node - on match and on create

**Scenarios:** 2

1. [1] Merge should be able to set labels on match and on create (Scenario, line 33)
2. [2] Merge should be able to use properties of bound node in ON MATCH and ON CREATE (Scenario, line 51)

#### `official/clauses/merge/Merge5.feature`

**Feature:** Merge5 - Merge relationships

**Scenarios:** 29

1. [1] Creating a relationship (Scenario, line 33)
2. [2] Matching a relationship (Scenario, line 51)
3. [3] Matching two relationships (Scenario, line 69)
4. [4] Using bound variables from other updating clause (Scenario, line 88)
5. [5] Filtering relationships (Scenario, line 103)
6. [6] Creating relationship when all matches filtered out (Scenario, line 122)
7. [7] Matching incoming relationship (Scenario, line 142)
8. [8] Creating relationship with property (Scenario, line 161)
9. [9] Creating relationship using merged nodes (Scenario, line 180)
10. [10] Merge should bind a path (Scenario, line 196)
11. [11] Use outgoing direction when unspecified (Scenario, line 213)
12. [12] Match outgoing relationship when direction unspecified (Scenario, line 229)
13. [13] Match both incoming and outgoing relationships when direction unspecified (Scenario, line 247)
14. [14] Using list properties via variable (Scenario, line 267)
15. [15] Matching using list property (Scenario, line 287)
16. [16] Aliasing of existing nodes 1 (Scenario, line 305)
17. [17] Aliasing of existing nodes 2 (Scenario, line 325)
18. [18] Double aliasing of existing nodes 1 (Scenario, line 344)
19. [19] Double aliasing of existing nodes 2 (Scenario, line 368)
20. [20] Do not match on deleted entities (Scenario, line 391)
21. [21] Do not match on deleted relationships (Scenario, line 421)
22. [22] Fail when imposing new predicates on a variable that is already bound (Scenario, line 446)
23. [23] Fail when merging relationship without type (Scenario, line 455)
24. [24] Fail when merging relationship without type, no colon (Scenario, line 464)
25. [25] Fail when merging relationship with more than one type (Scenario, line 473)
26. [26] Fail when merging relationship that is already bound (Scenario, line 482)
27. [27] Fail when using parameter as relationship predicate in MERGE (Scenario, line 491)
28. [28] Fail when using variable length relationship in MERGE (Scenario, line 502)
29. [29] Fail on merging relationship with null property (Scenario, line 512)

#### `official/clauses/merge/Merge6.feature`

**Feature:** Merge6 - Merge relationships - on create

**Scenarios:** 6

1. [1] Using ON CREATE on a node (Scenario, line 33)
2. [2] Using ON CREATE on a relationship (Scenario, line 50)
3. [3] Updating one property with ON CREATE (Scenario, line 70)
4. [4] Null-setting one property with ON CREATE (Scenario, line 95)
5. [6] Copying properties from node with ON CREATE (Scenario, line 119)
6. [7] Copying properties from literal map with ON CREATE (Scenario, line 144)

#### `official/clauses/merge/Merge7.feature`

**Feature:** Merge7 - Merge relationships - on match

**Scenarios:** 5

1. [1] Using ON MATCH on created node (Scenario, line 33)
2. [2] Using ON MATCH on created relationship (Scenario, line 49)
3. [3] Using ON MATCH on a relationship (Scenario, line 65)
4. [4] Copying properties from node with ON MATCH (Scenario, line 85)
5. [5] Copying properties from literal map with ON MATCH (Scenario, line 115)

#### `official/clauses/merge/Merge8.feature`

**Feature:** Merge8 - Merge relationships - on match and on create

**Scenarios:** 1

1. [1] Using ON CREATE and ON MATCH (Scenario, line 33)

#### `official/clauses/merge/Merge9.feature`

**Feature:** Merge9 - Merge clause interoperation with other clauses

**Scenarios:** 4

1. [1] UNWIND with one MERGE (Scenario, line 33)
2. [2] UNWIND with multiple MERGE (Scenario, line 48)
3. [3] Mixing MERGE with CREATE (Scenario, line 64)
4. [4] MERGE after WITH with predicate and WITH with aggregation (Scenario, line 81)

#### `official/clauses/remove/Remove1.feature`

**Feature:** Remove1 - Remove a Property

**Scenarios:** 7

1. [1] Remove a single node property (Scenario, line 33)
2. [2] Remove multiple node properties (Scenario, line 51)
3. [3] Remove a single relationship property (Scenario, line 69)
4. [4] Remove multiple relationship properties (Scenario, line 87)
5. [5] Ignore null when removing property from a node (Scenario, line 105)
6. [6] Ignore null when removing property from a relationship (Scenario, line 118)
7. [7] Remove a missing node property (Scenario, line 136)

#### `official/clauses/remove/Remove2.feature`

**Feature:** Remove2 - Remove a Label

**Scenarios:** 5

1. [1] Remove a single label from a node with a single label (Scenario, line 33)
2. [2] Remove a single label from a node with two labels (Scenario, line 51)
3. [3] Remove two labels from a node with three labels (Scenario, line 69)
4. [4] Remove a non-existent node label (Scenario, line 87)
5. [5] Ignore null when removing a node label (Scenario, line 104)

#### `official/clauses/remove/Remove3.feature`

**Feature:** Remove3 - Persistence of remove clause side effects

**Scenarios:** 21

1. [1] Limiting to zero results after removing a property from nodes affects the result set but not the side effects (Scenario, line 33)
2. [2] Skipping all results after removing a property from nodes affects the result set but not the side effects (Scenario, line 51)
3. [3] Skipping and limiting to a few results after removing a property from nodes affects the result set but not the side effects (Scenario, line 69)
4. [4] Skipping zero results and limiting to all results after removing a property from nodes does not affect the result set nor the side effects (Scenario, line 93)
5. [5] Filtering after removing a property from nodes affects the result set but not the side effects (Scenario, line 120)
6. [6] Aggregating in `RETURN` after removing a property from nodes affects the result set but not the side effects (Scenario, line 145)
7. [7] Aggregating in `WITH` after removing a property from nodes affects the result set but not the side effects (Scenario, line 167)
8. [8] Limiting to zero results after removing a label from nodes affects the result set but not the side effects (Scenario, line 190)
9. [9] Skipping all results after removing a label from nodes affects the result set but not the side effects (Scenario, line 208)
10. [10] Skipping and limiting to a few results after removing a label from nodes affects the result set but not the side effects (Scenario, line 226)
11. [11] Skipping zero result and limiting to all results after removing a label from nodes does not affect the result set nor the side effects (Scenario, line 250)
12. [12] Filtering after removing a label from nodes affects the result set but not the side effects (Scenario, line 277)
13. [13] Aggregating in `RETURN` after removing a label from nodes affects the result set but not the side effects (Scenario, line 302)
14. [14] Aggregating in `WITH` after removing a label from nodes affects the result set but not the side effects (Scenario, line 324)
15. [15] Limiting to zero results after removing a property from relationships affects the result set but not the side effects (Scenario, line 347)
16. [16] Skipping all results after removing a property from relationships affects the result set but not the side effects (Scenario, line 365)
17. [17] Skipping and limiting to a few results after removing a property from relationships affects the result set but not the side effects (Scenario, line 383)
18. [18] Skipping zero result and limiting to all results after removing a property from relationships does not affect the result set nor the side effects (Scenario, line 407)
19. [19] Filtering after removing a property from relationships affects the result set but not the side effects (Scenario, line 434)
20. [20] Aggregating in `RETURN` after removing a property from relationships affects the result set but not the side effects (Scenario, line 459)
21. [21] Aggregating in `WITH` after removing a property from relationships affects the result set but not the side effects (Scenario, line 481)

#### `official/clauses/return-orderby/ReturnOrderBy1.feature`

**Feature:** ReturnOrderBy1 - Order by a single variable (correct order of values according to their type)

**Scenarios:** 12

1. [1] ORDER BY should order booleans in the expected order (Scenario, line 33)
2. [2] ORDER BY DESC should order booleans in the expected order (Scenario, line 47)
3. [3] ORDER BY should order strings in the expected order (Scenario, line 61)
4. [4] ORDER BY DESC should order strings in the expected order (Scenario, line 77)
5. [5] ORDER BY should order ints in the expected order (Scenario, line 93)
6. [6] ORDER BY DESC should order ints in the expected order (Scenario, line 108)
7. [7] ORDER BY should order floats in the expected order (Scenario, line 123)
8. [8] ORDER BY DESC should order floats in the expected order (Scenario, line 138)
9. [9] ORDER BY should order lists in the expected order (Scenario, line 153)
10. [10] ORDER BY DESC should order lists in the expected order (Scenario, line 173)
11. [11] ORDER BY should order distinct types in the expected order (Scenario, line 193)
12. [12] ORDER BY DESC should order distinct types in the expected order (Scenario, line 220)

#### `official/clauses/return-orderby/ReturnOrderBy2.feature`

**Feature:** ReturnOrderBy2 - Order by a single expression (order of projection)

**Scenarios:** 14

1. [1] ORDER BY should return results in ascending order (Scenario, line 33)
2. [2] ORDER BY DESC should return results in descending order (Scenario, line 54)
3. [3] Sort on aggregated function (Scenario, line 75)
4. [4] Support sort and distinct (Scenario, line 98)
5. [5] Support ordering by a property after being distinct-ified (Scenario, line 119)
6. [6] Count star should count everything in scope (Scenario, line 136)
7. [7] Ordering with aggregation (Scenario, line 155)
8. [8] Returning all variables with ordering (Scenario, line 172)
9. [9] Using aliased DISTINCT expression in ORDER BY (Scenario, line 190)
10. [10] Returned columns do not change from using ORDER BY (Scenario, line 208)
11. [11] Aggregates ordered by arithmetics (Scenario, line 226)
12. [12] Aggregation of named paths (Scenario, line 243)
13. [13] Fail when sorting on variable removed by DISTINCT (Scenario, line 266)
14. [14] Fail on aggregation in ORDER BY after RETURN (Scenario, line 280)

#### `official/clauses/return-orderby/ReturnOrderBy3.feature`

**Feature:** ReturnOrderBy3 - Order by multiple expressions (order obey priority of expressions)

**Scenarios:** 1

1. [1] Sort on aggregate function and normal property (Scenario, line 33)

#### `official/clauses/return-orderby/ReturnOrderBy4.feature`

**Feature:** ReturnOrderBy4 - Order by in combination with projection

**Scenarios:** 2

1. [1] ORDER BY of a column introduced in RETURN should return salient results in ascending order (Scenario, line 33)
2. [2] Handle projections with ORDER BY (Scenario, line 50)

#### `official/clauses/return-orderby/ReturnOrderBy5.feature`

**Feature:** ReturnOrderBy5 - Order by in combination with column renaming

**Scenarios:** 1

1. [1] Renaming columns before ORDER BY should return results in ascending order (Scenario, line 33)

#### `official/clauses/return-orderby/ReturnOrderBy6.feature`

**Feature:** ReturnOrderBy6 - Aggregation expressions in order by

**Scenarios:** 5

1. [1] Handle constants and parameters inside an order by item which contains an aggregation expression (Scenario, line 33)
2. [2] Handle returned aliases inside an order by item which contains an aggregation expression (Scenario, line 48)
3. [3] Handle returned property accesses inside an order by item which contains an aggregation expression (Scenario, line 60)
4. [4] Fail if not returned variables are used inside an order by item which contains an aggregation expression (Scenario, line 72)
5. [5] Fail if more complex expressions, even if returned, are used inside an order by item which contains an aggregation expression (Scenario, line 82)

#### `official/clauses/return-skip-limit/ReturnSkipLimit1.feature`

**Feature:** ReturnSkipLimit1 - Skip

**Scenarios:** 11

1. [1] Start the result from the second row (Scenario, line 33)
2. [2] Start the result from the second row by param (Scenario, line 57)
3. [3] SKIP with an expression that does not depend on variables (Scenario, line 83)
4. [4] Accept skip zero (Scenario, line 102)
5. [5] SKIP with an expression that depends on variables should fail (Scenario, line 114)
6. [6] Negative parameter for SKIP should fail (Scenario, line 122)
7. [7] Negative SKIP should fail (Scenario, line 139)
8. [8] Floating point parameter for SKIP should fail (Scenario, line 154)
9. [9] Floating point SKIP should fail (Scenario, line 171)
10. [10] Fail when using non-constants in SKIP (Scenario, line 186)
11. [11] Fail when using negative value in SKIP (Scenario, line 196)

#### `official/clauses/return-skip-limit/ReturnSkipLimit2.feature`

**Feature:** ReturnSkipLimit2 - Limit

**Scenarios:** 17

1. [1] Limit to two hits (Scenario, line 33)
2. [2] Limit to two hits with explicit order (Scenario, line 47)
3. [3] LIMIT 0 should return an empty result (Scenario, line 70)
4. [4] Handle ORDER BY with LIMIT 1 (Scenario, line 86)
5. [5] ORDER BY with LIMIT 0 should not generate errors (Scenario, line 105)
6. [6] LIMIT with an expression that does not depend on variables (Scenario, line 118)
7. [7] Limit to more rows than actual results 1 (Scenario, line 136)
8. [8] Limit to more rows than actual results 2 (Scenario, line 156)
9. [9] Fail when using non-constants in LIMIT (Scenario, line 180)
10. [10] Negative parameter for LIMIT should fail (Scenario, line 188)
11. [11] Negative parameter for LIMIT with ORDER BY should fail (Scenario, line 205)
12. [12] Fail when using negative value in LIMIT 1 (Scenario, line 222)
13. [13] Fail when using negative value in LIMIT 2 (Scenario, line 232)
14. [14] Floating point parameter for LIMIT should fail (Scenario, line 247)
15. [15] Floating point parameter for LIMIT with ORDER BY should fail (Scenario, line 264)
16. [16] Fail when using floating point in LIMIT 1 (Scenario, line 281)
17. [17] Fail when using floating point in LIMIT 2 (Scenario, line 291)

#### `official/clauses/return-skip-limit/ReturnSkipLimit3.feature`

**Feature:** ReturnSkipLimit3 - Skip and limit

**Scenarios:** 3

1. [1] Get rows in the middle (Scenario, line 33)
2. [2] Get rows in the middle by param (Scenario, line 57)
3. [3] Limiting amount of rows when there are fewer left than the LIMIT argument (Scenario, line 84)

#### `official/clauses/return/Return1.feature`

**Feature:** Return1 - Return single variable (correct return of values according to their type)

**Scenarios:** 2

1. [1] Returning a list property (Scenario, line 33)
2. [2] Fail when returning an undefined variable (Scenario, line 49)

#### `official/clauses/return/Return2.feature`

**Feature:** Return2 - Return single expression (correctly projecting an expression)

**Scenarios:** 18

1. [1] Arithmetic expressions should propagate null values (Scenario, line 33)
2. [2] Returning a node property value (Scenario, line 44)
3. [3] Missing node property should become null (Scenario, line 60)
4. [4] Returning a relationship property value (Scenario, line 76)
5. [5] Missing relationship property should become null (Scenario, line 92)
6. [6] Adding a property and a literal in projection (Scenario, line 108)
7. [7] Adding list properties in projection (Scenario, line 124)
8. [8] Returning label predicate expression (Scenario, line 140)
9. [9] Returning a projected map (Scenario, line 157)
10. [10] Return count aggregation over an empty graph (Scenario, line 172)
11. [11] RETURN does not lose precision on large integers (Scenario, line 184)
12. [12] Projecting a list of nodes and relationships (Scenario, line 200)
13. [13] Projecting a map of nodes and relationships (Scenario, line 217)
14. [14] Do not fail when returning type of deleted relationships (Scenario, line 234)
15. [15] Fail when returning properties of deleted nodes (Scenario, line 252)
16. [16] Fail when returning labels of deleted nodes (Scenario, line 266)
17. [17] Fail when returning properties of deleted relationships (Scenario, line 280)
18. [18] Fail on projecting a non-existent function (Scenario, line 294)

#### `official/clauses/return/Return3.feature`

**Feature:** Return3 - Return multiple expressions (if column order correct)

**Scenarios:** 3

1. [1] Returning multiple expressions (Scenario, line 33)
2. [2] Returning multiple node property values (Scenario, line 49)
3. [3] Projecting nodes and relationships (Scenario, line 65)

#### `official/clauses/return/Return4.feature`

**Feature:** Return4 - Column renaming

**Scenarios:** 11

1. [1] Honour the column name for RETURN items (Scenario, line 33)
2. [2] Support column renaming (Scenario, line 50)
3. [3] Aliasing expressions (Scenario, line 66)
4. [4] Keeping used expression 1 (Scenario, line 82)
5. [5] Keeping used expression 2 (Scenario, line 98)
6. [6] Keeping used expression 3 (Scenario, line 114)
7. [7] Keeping used expression 4 (Scenario, line 130)
8. [8] Support column renaming for aggregations (Scenario, line 146)
9. [9] Handle subexpression in aggregation also occurring as standalone expression with nested aggregation in a literal map (Scenario, line 163)
10. [10] Fail when returning multiple columns with same name (Scenario, line 181)
11. [11] Reusing variable names in RETURN (Scenario, line 189)

#### `official/clauses/return/Return5.feature`

**Feature:** Return5 - Implicit grouping with distinct

**Scenarios:** 5

1. [1] DISTINCT inside aggregation should work with lists in maps (Scenario, line 33)
2. [2] DISTINCT on nullable values (Scenario, line 49)
3. [3] DISTINCT inside aggregation should work with nested lists in maps (Scenario, line 66)
4. [4] DISTINCT inside aggregation should work with nested lists of maps in maps (Scenario, line 82)
5. [5] Aggregate on list values (Scenario, line 98)

#### `official/clauses/return/Return6.feature`

**Feature:** Return6 - Implicit grouping with aggregates

**Scenarios:** 21

1. [1] Return count aggregation over nodes (Scenario, line 33)
2. [2] Projecting an arithmetic expression with aggregation (Scenario, line 49)
3. [3] Aggregating by a list property has a correct definition of equality (Scenario, line 65)
4. [4] Support multiple divisions in aggregate function (Scenario, line 82)
5. [5] Aggregates inside normal functions (Scenario, line 99)
6. [6] Handle aggregates inside non-aggregate expressions (Scenario, line 116)
7. [7] Aggregate on property (Scenario, line 127)
8. [8] Handle aggregation on functions (Scenario, line 146)
9. [9] Aggregates with arithmetics (Scenario, line 164)
10. [10] Multiple aggregates on same variable (Scenario, line 180)
11. [11] Counting matches (Scenario, line 196)
12. [12] Counting matches per group (Scenario, line 213)
13. [13] Returning the minimum length of paths (Scenario, line 230)
14. [14] Aggregates in aggregates (Scenario, line 251)
15. [15] Using `rand()` in aggregations (Scenario, line 259)
16. [16] Aggregation on complex expressions (Scenario, line 267)
17. [17] Handle constants and parameters inside an expression which contains an aggregation expression (Scenario, line 300)
18. [18] Handle returned variables inside an expression which contains an aggregation expression (Scenario, line 314)
19. [19] Handle returned property accesses inside an expression which contains an aggregation expression (Scenario, line 326)
20. [20] Fail if not returned variables are used inside an expression which contains an aggregation expression (Scenario, line 337)
21. [21] Fail if more complex expressions, even if returned, are used inside expression which contains an aggregation expression (Scenario, line 346)

#### `official/clauses/return/Return7.feature`

**Feature:** Return7 - Return all variables

**Scenarios:** 2

1. [1] Return all variables (Scenario, line 33)
2. [2] Fail when using RETURN * without variables in scope (Scenario, line 49)

#### `official/clauses/return/Return8.feature`

**Feature:** Return8 - Return clause interoperation with other clauses

**Scenarios:** 1

1. [1] Return aggregation after With filtering (Scenario, line 33)

#### `official/clauses/set/Set1.feature`

**Feature:** Set1 - Set a Property

**Scenarios:** 11

1. [1] Set a property (Scenario, line 33)
2. [2] Set a property to an expression (Scenario, line 53)
3. [3] Set a property by selecting the node using a simple expression (Scenario, line 73)
4. [4] Set a property by selecting the relationship using a simple expression (Scenario, line 91)
5. [5] Adding a list property (Scenario, line 109)
6. [6] Concatenate elements onto a list property (Scenario, line 127)
7. [7] Concatenate elements in reverse onto a list property (Scenario, line 142)
8. [8] Ignore null when setting property (Scenario, line 157)
9. [9] Failing when using undefined variable in SET (Scenario, line 170)
10. [10] Failing when setting a list of maps as a property (Scenario, line 180)
11. [11] Set multiple node properties (Scenario, line 189)

#### `official/clauses/set/Set2.feature`

**Feature:** Set2 - Set a Property to Null

**Scenarios:** 3

1. [1] Setting a node property to null removes the existing property (Scenario, line 33)
2. [2] Setting a node property to null removes the existing property, but not before SET (Scenario, line 51)
3. [3] Setting a relationship property to null removes the existing property (Scenario, line 70)

#### `official/clauses/set/Set3.feature`

**Feature:** Set3 - Set a Label

**Scenarios:** 8

1. [1] Add a single label to a node with no label (Scenario, line 33)
2. [2] Adding multiple labels to a node with no label (Scenario, line 51)
3. [3] Add a single label to a node with an existing label (Scenario, line 69)
4. [4] Adding multiple labels to a node with an existing label (Scenario, line 87)
5. [5] Ignore whitespace before colon 1 (Scenario, line 105)
6. [6] Ignore whitespace before colon 2 (Scenario, line 123)
7. [7] Ignore whitespace before colon 3 (Scenario, line 141)
8. [8] Ignore null when setting label (Scenario, line 159)

#### `official/clauses/set/Set4.feature`

**Feature:** Set4 - Set all properties with a map

**Scenarios:** 5

1. [1] Set multiple properties with a property map (Scenario, line 33)
2. [2] Non-existent values in a property map are removed with SET (Scenario, line 51)
3. [3] Null values in a property map are removed with SET (Scenario, line 70)
4. [4] All properties are removed if node is set to empty property map (Scenario, line 89)
5. [5] Ignore null when setting properties using an overriding map (Scenario, line 107)

#### `official/clauses/set/Set5.feature`

**Feature:** Set5 - Set multiple properties with a map

**Scenarios:** 5

1. [1] Ignore null when setting properties using an appending map (Scenario, line 33)
2. [2] Overwrite values when using += (Scenario, line 46)
3. [3] Retain old values when using += (Scenario, line 65)
4. [4] Explicit null values in a map remove old values (Scenario, line 83)
5. [5] Set an empty map when using += has no effect (Scenario, line 101)

#### `official/clauses/set/Set6.feature`

**Feature:** Set6 - Persistence of set clause side effects

**Scenarios:** 21

1. [1] Limiting to zero results after setting a property on nodes affects the result set but not the side effects (Scenario, line 33)
2. [2] Skipping all results after setting a property on nodes affects the result set but not the side effects (Scenario, line 52)
3. [3] Skipping and limiting to a few results after setting a property on nodes affects the result set but not the side effects (Scenario, line 71)
4. [4] Skipping zero results and limiting to all results after setting a property on nodes does not affect the result set nor the side effects (Scenario, line 96)
5. [5] Filtering after setting a property on nodes affects the result set but not the side effects (Scenario, line 124)
6. [6] Aggregating in `RETURN` after setting a property on nodes affects the result set but not the side effects (Scenario, line 151)
7. [7] Aggregating in `WITH` after setting a property on nodes affects the result set but not the side effects (Scenario, line 174)
8. [8] Limiting to zero results after adding a label on nodes affects the result set but not the side effects (Scenario, line 198)
9. [9] Skipping all results after adding a label on nodes affects the result set but not the side effects (Scenario, line 216)
10. [10] Skipping and limiting to a few results after adding a label on nodes affects the result set but not the side effects (Scenario, line 234)
11. [11] Skipping zero result and limiting to all results after adding a label on nodes does not affect the result set nor the side effects (Scenario, line 258)
12. [12] Filtering after adding a label on nodes affects the result set but not the side effects (Scenario, line 285)
13. [13] Aggregating in `RETURN` after adding a label on nodes affects the result set but not the side effects (Scenario, line 310)
14. [14] Aggregating in `WITH` after adding a label on nodes affects the result set but not the side effects (Scenario, line 332)
15. [15] Limiting to zero results after setting a property on relationships affects the result set but not the side effects (Scenario, line 355)
16. [16] Skipping all results after setting a property on relationships affects the result set but not the side effects (Scenario, line 374)
17. [17] Skipping and limiting to a few results after setting a property on relationships affects the result set but not the side effects (Scenario, line 393)
18. [18] Skipping zero result and limiting to all results after setting a property on relationships does not affect the result set nor the side effects (Scenario, line 418)
19. [19] Filtering after setting a property on relationships affects the result set but not the side effects (Scenario, line 446)
20. [20] Aggregating in `RETURN` after setting a property on relationships affects the result set but not the side effects (Scenario, line 473)
21. [21] Aggregating in `WITH` after setting a property on relationships affects the result set but not the side effects (Scenario, line 496)

#### `official/clauses/union/Union1.feature`

**Feature:** Union1 - Union

**Scenarios:** 5

1. [1] Two elements, both unique, distinct (Scenario, line 33)
2. [2] Three elements, two unique, distinct (Scenario, line 47)
3. [3] Two single-column inputs, one with duplicates, distinct (Scenario, line 63)
4. [4] Should be able to create text output from union queries (Scenario, line 81)
5. [5] Failing when UNION has different columns (Scenario, line 101)

#### `official/clauses/union/Union2.feature`

**Feature:** Union2 - Union All

**Scenarios:** 5

1. [1] Two elements, both unique, not distinct (Scenario, line 33)
2. [2] Three elements, two unique, not distinct (Scenario, line 47)
3. [3] Two single-column inputs, one with duplicates, not distinct (Scenario, line 64)
4. [4] Should be able to create text output from union all queries (Scenario, line 84)
5. [5] Failing when UNION ALL has different columns (Scenario, line 104)

#### `official/clauses/union/Union3.feature`

**Feature:** Union3 - Union in combination with Union All

**Scenarios:** 2

1. [1] Failing when mixing UNION and UNION ALL (Scenario, line 33)
2. [2] Failing when mixing UNION ALL and UNION (Scenario, line 45)

#### `official/clauses/unwind/Unwind1.feature`

**Feature:** Unwind1

**Scenarios:** 14

1. [1] Unwinding a list (Scenario, line 33)
2. [2] Unwinding a range (Scenario, line 47)
3. [3] Unwinding a concatenation of lists (Scenario, line 61)
4. [4] Unwinding a collected unwound expression (Scenario, line 79)
5. [5] Unwinding a collected expression (Scenario, line 94)
6. [6] Creating nodes from an unwound parameter list (Scenario, line 113)
7. [7] Double unwinding a list of lists (Scenario, line 140)
8. [8] Unwinding the empty list (Scenario, line 159)
9. [9] Unwinding null (Scenario, line 170)
10. [10] Unwinding list with duplicates (Scenario, line 181)
11. [11] Unwind does not prune context (Scenario, line 202)
12. [12] Unwind does not remove variables from scope (Scenario, line 217)
13. [13] Multiple unwinds after each other (Scenario, line 241)
14. [14] Unwind with merge (Scenario, line 263)

#### `official/clauses/with-orderBy/WithOrderBy1.feature`

**Feature:** WithOrderBy1 - Order by a single variable

**Scenarios:** 46

1. [1] Sort booleans in ascending order (Scenario, line 35)
2. [2] Sort booleans in descending order (Scenario, line 50)
3. [3] Sort integers in ascending order (Scenario, line 65)
4. [4] Sort integers in descending order (Scenario, line 81)
5. [5] Sort floats in ascending order (Scenario, line 97)
6. [6] Sort floats in descending order (Scenario, line 113)
7. [7] Sort strings in ascending order (Scenario, line 129)
8. [8] Sort strings in descending order (Scenario, line 145)
9. [9] Sort lists in ascending order (Scenario, line 161)
10. [10] Sort lists in descending order (Scenario, line 179)
11. [11] Sort dates in ascending order (Scenario, line 197)
12. [12] Sort dates in descending order (Scenario, line 218)
13. [13] Sort local times in ascending order (Scenario, line 239)
14. [14] Sort local times in descending order (Scenario, line 260)
15. [15] Sort times in ascending order (Scenario, line 281)
16. [16] Sort times in descending order (Scenario, line 302)
17. [17] Sort local date times in ascending order (Scenario, line 323)
18. [18] Sort local date times in descending order (Scenario, line 344)
19. [19] Sort date times in ascending order (Scenario, line 365)
20. [20] Sort date times in descending order (Scenario, line 386)
21. [21] Sort distinct types in ascending order (Scenario, line 407)
22. [22] Sort distinct types in descending order (Scenario, line 431)
23. [23] Sort by a boolean variable projected from a node property in ascending order (Scenario Outline, line 455)
24. [24] Sort by a boolean variable projected from a node property in descending order (Scenario Outline, line 487)
25. [25] Sort by an integer variable projected from a node property in ascending order (Scenario Outline, line 517)
26. [26] Sort by an integer variable projected from a node property in descending order (Scenario Outline, line 549)
27. [27] Sort by a float variable projected from a node property in ascending order (Scenario Outline, line 580)
28. [28] Sort by a float variable projected from a node property in descending order (Scenario Outline, line 612)
29. [29] Sort by a string variable projected from a node property in ascending order (Scenario Outline, line 643)
30. [30] Sort by a string variable projected from a node property in descending order (Scenario Outline, line 675)
31. [31] Sort by a list variable projected from a node property in ascending order (Scenario Outline, line 706)
32. [32] Sort by a list variable projected from a node property in descending order (Scenario Outline, line 738)
33. [33] Sort by a date variable projected from a node property in ascending order (Scenario Outline, line 769)
34. [34] Sort by a date variable projected from a node property in descending order (Scenario Outline, line 801)
35. [35] Sort by a local time variable projected from a node property in ascending order (Scenario Outline, line 832)
36. [36] Sort by a local time variable projected from a node property in descending order (Scenario Outline, line 864)
37. [37] Sort by a time variable projected from a node property in ascending order (Scenario Outline, line 895)
38. [38] Sort by a time variable projected from a node property in descending order (Scenario Outline, line 927)
39. [39] Sort by a local date time variable projected from a node property in ascending order (Scenario Outline, line 958)
40. [40] Sort by a local date time variable projected from a node property in descending order (Scenario Outline, line 990)
41. [41] Sort by a date time variable projected from a node property in ascending order (Scenario Outline, line 1021)
42. [42] Sort by a date time variable projected from a node property in descending order (Scenario Outline, line 1053)
43. [43] Sort by a variable that is only partially orderable on a non-distinct binding table (Scenario Outline, line 1084)
44. [44] Sort by a variable that is only partially orderable on a non-distinct binding table, but made distinct (Scenario Outline, line 1105)
45. [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> (Scenario Outline, line 1125)
46. [46] Fail on sorting by an undefined variable #Example: <exampleName> (Scenario Outline, line 1155)

#### `official/clauses/with-orderBy/WithOrderBy2.feature`

**Feature:** WithOrderBy2 - Order by a single expression

**Scenarios:** 25

1. [1] Sort by a boolean expression in ascending order (Scenario Outline, line 35)
2. [2] Sort by a boolean expression in descending order (Scenario Outline, line 65)
3. [3] Sort by an integer expression in ascending order (Scenario Outline, line 95)
4. [4] Sort by an integer expression in descending order (Scenario Outline, line 126)
5. [5] Sort by a float expression in ascending order (Scenario Outline, line 156)
6. [6] Sort by a float expression in descending order (Scenario Outline, line 187)
7. [7] Sort by a string expression in ascending order (Scenario Outline, line 217)
8. [8] Sort by a string expression in descending order (Scenario Outline, line 248)
9. [9] Sort by a list expression in ascending order (Scenario Outline, line 278)
10. [10] Sort by a list expression in descending order (Scenario Outline, line 309)
11. [11] Sort by a date expression in ascending order (Scenario Outline, line 339)
12. [12] Sort by a date expression in descending order (Scenario Outline, line 370)
13. [13] Sort by a local time expression in ascending order (Scenario Outline, line 400)
14. [14] Sort by a local time expression in descending order (Scenario Outline, line 431)
15. [15] Sort by a time expression in ascending order (Scenario Outline, line 461)
16. [16] Sort by a time expression in descending order (Scenario Outline, line 492)
17. [17] Sort by a local date time expression in ascending order (Scenario Outline, line 522)
18. [18] Sort by a local date time expression in descending order (Scenario Outline, line 553)
19. [19] Sort by a date time expression in ascending order (Scenario Outline, line 583)
20. [20] Sort by a date time expression in descending order (Scenario Outline, line 614)
21. [21] Sort by an expression that is only partially orderable on a non-distinct binding table (Scenario Outline, line 644)
22. [22] Sort by an expression that is only partially orderable on a non-distinct binding table, but used as a grouping key (Scenario Outline, line 673)
23. [23] Sort by an expression that is only partially orderable on a non-distinct binding table, but used in parts as a grouping key (Scenario Outline, line 701)
24. [24] Sort by an expression that is only partially orderable on a non-distinct binding table, but made distinct (Scenario Outline, line 729)
25. [25] Fail on sorting by an aggregation (Scenario Outline, line 757)

#### `official/clauses/with-orderBy/WithOrderBy3.feature`

**Feature:** WithOrderBy3 - Order by multiple expressions

**Scenarios:** 8

1. [1] Sort by two expressions, both in ascending order (Scenario Outline, line 35)
2. [2] Sort by two expressions, first in ascending order, second in descending order (Scenario Outline, line 73)
3. [3] Sort by two expressions, first in descending order, second in ascending order (Scenario Outline, line 108)
4. [4] Sort by two expressions, both in descending order (Scenario Outline, line 143)
5. [5] An expression without explicit sort direction is sorted in ascending order (Scenario Outline, line 176)
6. [6] An constant expression does not influence the order determined by other expression before and after the constant expression (Scenario Outline, line 209)
7. [7] The order direction cannot be overwritten (Scenario Outline, line 262)
8. [8] Fail on sorting by any number of undefined variables in any position #Example: <exampleName> (Scenario Outline, line 290)

#### `official/clauses/with-orderBy/WithOrderBy4.feature`

**Feature:** WithOrderBy4 - Order by in combination with projection and aliasing

**Scenarios:** 20

1. [1] Sort by a projected expression (Scenario, line 35)
2. [2] Sort by an alias of a projected expression (Scenario, line 60)
3. [3] Sort by two projected expressions with order priority being different than projection order (Scenario, line 85)
4. [4] Sort by one projected expression and one alias of a projected expression with order priority being different than projection order (Scenario, line 110)
5. [5] Sort by one alias of a projected expression and one projected expression with order priority being different than projection order (Scenario, line 135)
6. [6] Sort by aliases of two projected expressions with order priority being different than projection order (Scenario, line 160)
7. [7] Sort by an alias of a projected expression where the alias shadows an existing variable (Scenario, line 185)
8. [8] Sort by non-projected existing variable (Scenario, line 211)
9. [9] Sort by an alias of a projected expression containing the variable shadowed by the alias (Scenario, line 237)
10. [10] Sort by a non-projected expression containing an alias of a projected expression containing the variable shadowed by the alias (Scenario, line 263)
11. [11] Sort by an aggregate projection (Scenario, line 289)
12. [12] Sort by an aliased aggregate projection (Scenario, line 313)
13. [13] Fail on sorting by a non-projected aggregation on a variable (Scenario, line 337)
14. [14] Fail on sorting by a non-projected aggregation on an expression (Scenario, line 358)
15. [15] Sort by an aliased aggregate projection does allow subsequent matching (Scenario, line 378)
16. [16] Handle constants and parameters inside an order by item which contains an aggregation expression (Scenario, line 401)
17. [17] Handle projected variables inside an order by item which contains an aggregation expression (Scenario, line 417)
18. [18] Handle projected property accesses inside an order by item which contains an aggregation expression (Scenario, line 430)
19. [19] Fail if not projected variables are used inside an order by item which contains an aggregation expression (Scenario, line 443)
20. [20] Fail if more complex expressions, even if projected, are used inside an order by item which contains an aggregation expression (Scenario, line 454)

#### `official/clauses/with-skip-limit/WithSkipLimit1.feature`

**Feature:** WithSkipLimit1 - Skip

**Scenarios:** 2

1. [1] Handle dependencies across WITH with SKIP (Scenario, line 62)
2. [2] Ordering and skipping on aggregate (Scenario, line 85)

#### `official/clauses/with-skip-limit/WithSkipLimit2.feature`

**Feature:** WithSkipLimit2 - Limit

**Scenarios:** 4

1. [1] ORDER BY and LIMIT can be used (Scenario, line 62)
2. [2] Handle dependencies across WITH with LIMIT (Scenario, line 84)
3. [3] Connected components succeeding WITH with LIMIT (Scenario, line 106)
4. [4] Ordering and limiting on aggregate (Scenario, line 126)

#### `official/clauses/with-skip-limit/WithSkipLimit3.feature`

**Feature:** WithSkipLimit3 - Skip and limit

**Scenarios:** 3

1. [1] Get rows in the middle (Scenario, line 62)
2. [2] Get rows in the middle by param (Scenario, line 87)
3. [3] Limiting amount of rows when there are fewer left than the LIMIT argument (Scenario, line 115)

#### `official/clauses/with-where/WithWhere1.feature`

**Feature:** WithWhere1 - Filter single variable

**Scenarios:** 4

1. [1] Filter node with property predicate on a single variable with multiple bindings (Scenario, line 33)
2. [2] Filter node with property predicate on a single variable with multiple distinct bindings (Scenario, line 53)
3. [3] Filter for an unbound relationship variable (Scenario, line 73)
4. [4] Filter for an unbound node variable (Scenario, line 92)

#### `official/clauses/with-where/WithWhere2.feature`

**Feature:** WithWhere2 - Filter multiple variables

**Scenarios:** 2

1. [1] Filter nodes with conjunctive two-part property predicate on multi variables with multiple bindings (Scenario, line 33)
2. [2] Filter node with conjunctive multi-part property predicates on multi variables with multiple bindings (Scenario, line 59)

#### `official/clauses/with-where/WithWhere3.feature`

**Feature:** WithWhere3 - Equi-Joins on variables

**Scenarios:** 3

1. [1] Join between node identities (Scenario, line 33)
2. [2] Join between node properties of disconnected nodes (Scenario, line 52)
3. [3] Join between node properties of adjacent nodes (Scenario, line 73)

#### `official/clauses/with-where/WithWhere4.feature`

**Feature:** WithWhere4 - Non-Equi-Joins on variables

**Scenarios:** 2

1. [1] Join nodes on inequality (Scenario, line 33)
2. [2] Join with disjunctive multi-part predicates including patterns (Scenario, line 52)

#### `official/clauses/with-where/WithWhere5.feature`

**Feature:** WithWhere5 - Filter on predicate resulting in null

**Scenarios:** 4

1. [1] Filter out on null (Scenario, line 33)
2. [2] Filter out on null if the AND'd predicate evaluates to false (Scenario, line 55)
3. [3] Filter out on null if the AND'd predicate evaluates to true (Scenario, line 77)
4. [4] Do not filter out on null if the OR'd predicate evaluates to true (Scenario, line 99)

#### `official/clauses/with-where/WithWhere6.feature`

**Feature:** WithWhere6 - Filter on aggregates

**Scenarios:** 1

1. [1] Filter a single aggregate (Scenario, line 33)

#### `official/clauses/with-where/WithWhere7.feature`

**Feature:** WithWhere7 - Variable visibility under aliasing

**Scenarios:** 3

1. [1] WHERE sees a variable bound before but not after WITH (Scenario, line 33)
2. [2] WHERE sees a variable bound after but not before WITH (Scenario, line 53)
3. [3] WHERE sees both, variable bound before but not after WITH and variable bound after but not before WITH (Scenario, line 73)

#### `official/clauses/with/With1.feature`

**Feature:** With1 - Forward single variable

**Scenarios:** 6

1. [1] Forwarind a node variable 1 (Scenario, line 34)
2. [2] Forwarind a node variable 2 (Scenario, line 52)
3. [3] Forwarding a relationship variable (Scenario, line 71)
4. [4] Forwarding a path variable (Scenario, line 92)
5. [5] Forwarding null (Scenario, line 109)
6. [6] Forwarding a node variable possibly null (Scenario, line 122)

#### `official/clauses/with/With2.feature`

**Feature:** With2 - Forward single expression

**Scenarios:** 2

1. [1] Forwarding a property to express a join (Scenario, line 34)
2. [2] Forwarding a nested map literal (Scenario, line 55)

#### `official/clauses/with/With3.feature`

**Feature:** With3 - Forward multiple expressions

**Scenarios:** 1

1. [1] Forwarding multiple node and relationship variables (Scenario, line 33)

#### `official/clauses/with/With4.feature`

**Feature:** With4 - Variable aliasing

**Scenarios:** 7

1. [1] Aliasing relationship variable (Scenario, line 34)
2. [2] Aliasing expression to new variable name (Scenario, line 53)
3. [3] Aliasing expression to existing variable name (Scenario, line 74)
4. [4] Fail when forwarding multiple aliases with the same name (Scenario, line 93)
5. [5] Fail when not aliasing expressions in WITH (Scenario, line 102)
6. [6] Reusing variable names in WITH (Scenario, line 112)
7. [7] Multiple aliasing and backreferencing (Scenario, line 134)

#### `official/clauses/with/With5.feature`

**Feature:** With5 - Implicit grouping with DISTINCT

**Scenarios:** 2

1. [1] DISTINCT on an expression (Scenario, line 33)
2. [2] Handling DISTINCT with lists in maps (Scenario, line 53)

#### `official/clauses/with/With6.feature`

**Feature:** With6 - Implicit grouping with aggregates

**Scenarios:** 9

1. [1] Implicit grouping with single expression as grouping key and single aggregation (Scenario, line 33)
2. [2] Implicit grouping with single relationship variable as grouping key and single aggregation (Scenario, line 53)
3. [3] Implicit grouping with multiple node and relationship variables as grouping key and single aggregation (Scenario, line 74)
4. [4] Implicit grouping with single path variable as grouping key and single aggregation (Scenario, line 95)
5. [5] Handle constants and parameters inside an expression which contains an aggregation expression (Scenario, line 116)
6. [6] Handle projected variables inside an expression which contains an aggregation expression (Scenario, line 131)
7. [7] Handle projected property accesses inside an expression which contains an aggregation expression (Scenario, line 144)
8. [8] Fail if not projected variables are used inside an expression which contains an aggregation expression (Scenario, line 156)
9. [9] Fail if more complex expression, even if projected, are used inside expression which contains an aggregation expression (Scenario, line 166)

#### `official/clauses/with/With7.feature`

**Feature:** With7 - WITH on WITH

**Scenarios:** 2

1. [1] A simple pattern with one bound endpoint (Scenario, line 33)
2. [2] Multiple WITHs using a predicate and aggregation (Scenario, line 53)

### expressions

**125 feature files, 758 scenarios**

#### `official/expressions/aggregation/Aggregation1.feature`

**Feature:** Aggregation1 - Count

**Scenarios:** 2

1. [1] Count only non-null values (Scenario, line 33)
2. [2] Counting loop relationships (Scenario, line 52)

#### `official/expressions/aggregation/Aggregation2.feature`

**Feature:** Aggregation2 - Min and Max

**Scenarios:** 12

1. [1] `max()` over integers (Scenario, line 33)
2. [2] `min()` over integers (Scenario, line 45)
3. [3] `max()` over floats (Scenario, line 57)
4. [4] `min()` over floats (Scenario, line 69)
5. [5] `max()` over mixed numeric values (Scenario, line 81)
6. [6] `min()` over mixed numeric values (Scenario, line 93)
7. [7] `max()` over strings (Scenario, line 105)
8. [8] `min()` over strings (Scenario, line 117)
9. [9] `max()` over list values (Scenario, line 129)
10. [10] `min()` over list values (Scenario, line 141)
11. [11] `max()` over mixed values (Scenario, line 153)
12. [12] `min()` over mixed values (Scenario, line 165)

#### `official/expressions/aggregation/Aggregation3.feature`

**Feature:** Aggregation3 - Sum

**Scenarios:** 2

1. [1] Sum only non-null values (Scenario, line 33)
2. [2] No overflow during summation (Scenario, line 51)

#### `official/expressions/aggregation/Aggregation4.feature`

**Feature:** Aggregation4 - Avg

**Scenarios:** 0

#### `official/expressions/aggregation/Aggregation5.feature`

**Feature:** Aggregation5 - Collect

**Scenarios:** 2

1. [1] `collect()` filtering nulls (Scenario, line 33)
2. [2] OPTIONAL MATCH and `collect()` on node property (Scenario, line 50)

#### `official/expressions/aggregation/Aggregation6.feature`

**Feature:** Aggregation6 - Percentiles

**Scenarios:** 5

1. [1] `percentileDisc()` (Scenario Outline, line 33)
2. [2] `percentileCont()` (Scenario Outline, line 59)
3. [3] `percentileCont()` failing on bad arguments (Scenario Outline, line 85)
4. [4] `percentileDisc()` failing on bad arguments (Scenario Outline, line 106)
5. [5] `percentileDisc()` failing in more involved query (Scenario, line 127)

#### `official/expressions/aggregation/Aggregation7.feature`

**Feature:** Aggregation7 - Standard deviation

**Scenarios:** 0

#### `official/expressions/aggregation/Aggregation8.feature`

**Feature:** Aggregation8 - DISTINCT

**Scenarios:** 4

1. [1] Distinct on unbound node (Scenario, line 33)
2. [2] Distinct on null (Scenario, line 45)
3. [3] Collect distinct nulls (Scenario, line 61)
4. [4] Collect distinct values mixed with nulls (Scenario, line 73)

#### `official/expressions/boolean/Boolean1.feature`

**Feature:** Boolean1 - And logical operations

**Scenarios:** 8

1. [1] Conjunction of two truth values (Scenario, line 33)
2. [2] Conjunction of three truth values (Scenario, line 52)
3. [3] Conjunction of many truth values (Scenario, line 89)
4. [4] Conjunction is commutative on non-null (Scenario, line 111)
5. [5] Conjunction is commutative on null (Scenario, line 127)
6. [6] Conjunction is associative on non-null (Scenario, line 145)
7. [7] Conjunction is associative on null (Scenario, line 166)
8. [8] Fail on conjunction of at least one non-booleans (Scenario Outline, line 199)

#### `official/expressions/boolean/Boolean2.feature`

**Feature:** Boolean2 - OR logical operations

**Scenarios:** 8

1. [1] Disjunction of two truth values (Scenario, line 33)
2. [2] Disjunction of three truth values (Scenario, line 52)
3. [3] Disjunction of many truth values (Scenario, line 89)
4. [4] Disjunction is commutative on non-null (Scenario, line 111)
5. [5] Disjunction is commutative on null (Scenario, line 127)
6. [6] Disjunction is associative on non-null (Scenario, line 145)
7. [7] Disjunction is associative on null (Scenario, line 166)
8. [8] Fail on disjunction of at least one non-booleans (Scenario Outline, line 199)

#### `official/expressions/boolean/Boolean3.feature`

**Feature:** Boolean3 - XOR logical operations

**Scenarios:** 8

1. [1] Exclusive disjunction of two truth values (Scenario, line 33)
2. [2] Exclusive disjunction of three truth values (Scenario, line 52)
3. [3] Exclusive disjunction of many truth values (Scenario, line 89)
4. [4] Exclusive disjunction is commutative on non-null (Scenario, line 111)
5. [5] Exclusive disjunction is commutative on null (Scenario, line 127)
6. [6] Exclusive disjunction is associative on non-null (Scenario, line 145)
7. [7] Exclusive disjunction is associative on null (Scenario, line 166)
8. [8] Fail on exclusive disjunction of at least one non-booleans (Scenario Outline, line 199)

#### `official/expressions/boolean/Boolean4.feature`

**Feature:** Boolean4 - NOT logical operations

**Scenarios:** 4

1. [1] Logical negation of truth values (Scenario, line 33)
2. [2] Double logical negation of truth values (Scenario, line 44)
3. [3] NOT and false (Scenario, line 55)
4. [4] Fail when using NOT on a non-boolean literal (Scenario Outline, line 72)

#### `official/expressions/boolean/Boolean5.feature`

**Feature:** Boolean5 - Interop of logical operations

**Scenarios:** 8

1. [1] Disjunction is distributive over conjunction on non-null (Scenario, line 33)
2. [2] Disjunction is distributive over conjunction on null (Scenario, line 54)
3. [3] Conjunction is distributive over disjunction on non-null (Scenario, line 87)
4. [4] Conjunction is distributive over disjunction on null (Scenario, line 108)
5. [5] Conjunction is distributive over exclusive disjunction on non-null (Scenario, line 141)
6. [6] Conjunction is not distributive over exclusive disjunction on null (Scenario, line 162)
7. [7] De Morgan's law on non-null: the negation of a disjunction is the conjunction of the negations (Scenario, line 195)
8. [8] De Morgan's law on non-null: the negation of a conjunction is the disjunction of the negations (Scenario, line 211)

#### `official/expressions/comparison/Comparison1.feature`

**Feature:** Comparison1 - Equality

**Scenarios:** 17

1. [1] Number-typed integer comparison (Scenario, line 33)
2. [2] Number-typed float comparison (Scenario, line 52)
3. [3] Any-typed string comparison (Scenario, line 70)
4. [4] Comparing nodes to nodes (Scenario, line 88)
5. [5] Comparing relationships to relationships (Scenario, line 107)
6. [6] Comparing lists to lists (Scenario Outline, line 126)
7. [7] Comparing maps to maps (Scenario Outline, line 146)
8. [8] Equality and inequality of NaN (Scenario Outline, line 176)
9. [9] Equality between strings and numbers (Scenario Outline, line 194)
10. [10] Handling inlined equality of large integer (Scenario, line 212)
11. [11] Handling explicit equality of large integer (Scenario, line 228)
12. [12] Handling inlined equality of large integer, non-equal values (Scenario, line 245)
13. [13] Handling explicit equality of large integer, non-equal values (Scenario, line 260)
14. [14] Direction of traversed relationship is not significant for path equality, simple (Scenario, line 276)
15. [15] It is unknown - i.e. null - if a null is equal to a null (Scenario, line 293)
16. [16] It is unknown - i.e. null - if a null is not equal to a null (Scenario, line 304)
17. [17] Failing when comparing to an undefined variable (Scenario, line 315)

#### `official/expressions/comparison/Comparison2.feature`

**Feature:** Comparison2 - Half-bounded Range

**Scenarios:** 6

1. [1] Comparing strings and integers using > in an AND'd predicate (Scenario, line 33)
2. [2] Comparing strings and integers using > in a OR'd predicate (Scenario, line 52)
3. [3] Comparing across types yields null, except numbers (Scenario Outline, line 72)
4. [4] Comparing lists (Scenario Outline, line 102)
5. [5] Comparing NaN (Scenario Outline, line 121)
6. [6] Comparability between numbers and strings (Scenario Outline, line 139)

#### `official/expressions/comparison/Comparison3.feature`

**Feature:** Comparison3 - Full-Bound Range

**Scenarios:** 9

1. [1] Handling numerical ranges 1 (Scenario, line 33)
2. [2] Handling numerical ranges 2 (Scenario, line 51)
3. [3] Handling numerical ranges 3 (Scenario, line 70)
4. [4] Handling numerical ranges 4 (Scenario, line 89)
5. [5] Handling string ranges 1 (Scenario, line 109)
6. [6] Handling string ranges 2 (Scenario, line 127)
7. [7] Handling string ranges 3 (Scenario, line 146)
8. [8] Handling string ranges 4 (Scenario, line 165)
9. [9] Handling empty range (Scenario, line 185)

#### `official/expressions/comparison/Comparison4.feature`

**Feature:** Comparison4 - Combination of Comparisons

**Scenarios:** 1

1. [1] Handling long chains of operators (Scenario, line 33)

#### `official/expressions/conditional/Conditional1.feature`

**Feature:** Conditional1 - Coalesce expression

**Scenarios:** 1

1. [1] Run coalesce (Scenario, line 33)

#### `official/expressions/conditional/Conditional2.feature`

**Feature:** Conditional2 - Case Expression

**Scenarios:** 1

1. [1] Simple cases over integers (Scenario Outline, line 33)

#### `official/expressions/existentialSubqueries/ExistentialSubquery1.feature`

**Feature:** ExistentialSubquery1 - Simple existential subquery

**Scenarios:** 4

1. [1] Simple subquery without WHERE clause (Scenario, line 33)
2. [2] Simple subquery with WHERE clause (Scenario, line 53)
3. [3] Simple subquery without WHERE clause, not existing pattern (Scenario, line 74)
4. [4] Simple subquery with WHERE clause, not existing pattern (Scenario, line 93)

#### `official/expressions/existentialSubqueries/ExistentialSubquery2.feature`

**Feature:** ExistentialSubquery2 - Full existential subquery

**Scenarios:** 3

1. [1] Full existential subquery (Scenario, line 33)
2. [2] Full existential subquery with aggregation (Scenario, line 54)
3. [3] Full existential subquery with update clause should fail (Scenario, line 78)

#### `official/expressions/existentialSubqueries/ExistentialSubquery3.feature`

**Feature:** ExistentialSubquery3 - Nested existential subquery

**Scenarios:** 3

1. [1] Nested simple existential subquery (Scenario, line 33)
2. [2] Nested full existential subquery (Scenario, line 56)
3. [3] Nested full existential subquery with pattern predicate (Scenario, line 79)

#### `official/expressions/graph/Graph1.feature`

**Feature:** Graph1 - Node and edge identifier - ID function

**Scenarios:** 0

#### `official/expressions/graph/Graph2.feature`

**Feature:** Graph2 - Edge source and destination

**Scenarios:** 0

#### `official/expressions/graph/Graph3.feature`

**Feature:** Graph3 - Node labels

**Scenarios:** 9

1. [1] Creating node without label (Scenario, line 34)
2. [2] Creating node with two labels (Scenario, line 48)
3. [3] Ignore space when creating node with labels (Scenario, line 64)
4. [4] Create node with label in pattern (Scenario, line 79)
5. [5] Using `labels()` in return clauses (Scenario, line 94)
6. [6] `labels()` should accept type Any (Scenario, line 110)
7. [7] `labels()` on null node (Scenario, line 128)
8. [8] `labels()` failing on a path (Scenario, line 140)
9. [9] `labels()` failing on invalid arguments (Scenario, line 153)

#### `official/expressions/graph/Graph4.feature`

**Feature:** Graph4 - Edge relationship type

**Scenarios:** 7

1. [1] `type()` (Scenario, line 33)
2. [2] `type()` on two relationships (Scenario, line 49)
3. [3] `type()` on null relationship (Scenario, line 65)
4. [4] `type()` on mixed null and non-null relationships (Scenario, line 82)
5. [5] `type()` handling Any type (Scenario, line 100)
6. [6] `type()` failing on invalid arguments (Scenario Outline, line 117)
7. [7] Failing when using `type()` on a node (Scenario, line 138)

#### `official/expressions/graph/Graph5.feature`

**Feature:** Graph5 - Node and edge label expressions

**Scenarios:** 5

1. [1] Single-labels expression on nodes (Scenario, line 33)
2. [2] Single-labels expression on relationships (Scenario, line 59)
3. [3] Conjunctive labels expression on nodes (Scenario, line 83)
4. [4] Conjunctive labels expression on nodes with varying order and repeating labels (Scenario Outline, line 107)
5. [5] Label expression on null (Scenario, line 133)

#### `official/expressions/graph/Graph6.feature`

**Feature:** Graph6 - Static property access

**Scenarios:** 9

1. [1] Statically access a property of a non-null node (Scenario, line 34)
2. [2] Statically access a property of a optional non-null node (Scenario, line 50)
3. [3] Statically access a property of a null node (Scenario, line 66)
4. [4] Statically access a property of a node resulting from an expression (Scenario, line 78)
5. [5] Statically access a property of a non-null relationship (Scenario, line 95)
6. [6] Statically access a property of a optional non-null relationship (Scenario, line 111)
7. [7] Statically access a property of a null relationship (Scenario, line 127)
8. [8] Statically access a property of a relationship resulting from an expression (Scenario, line 139)
9. [9] Fail when performing property access on a non-graph element (Scenario Outline, line 156)

#### `official/expressions/graph/Graph7.feature`

**Feature:** Graph7 - Dynamic property access

**Scenarios:** 3

1. [1] Execute n['name'] in read queries (Scenario, line 34)
2. [2] Execute n['name'] in update queries (Scenario, line 50)
3. [3] Use dynamic property lookup based on parameters when there is lhs type information (Scenario, line 64)

#### `official/expressions/graph/Graph8.feature`

**Feature:** Graph8 - Property keys function

**Scenarios:** 8

1. [1] Using `keys()` on a single node, non-empty result (Scenario, line 33)
2. [2] Using `keys()` on multiple nodes, non-empty result (Scenario, line 51)
3. [3] Using `keys()` on a single node, empty result (Scenario, line 72)
4. [4] Using `keys()` on an optionally matched node (Scenario, line 88)
5. [5] Using `keys()` on a relationship, non-empty result (Scenario, line 104)
6. [6] Using `keys()` on a relationship, empty result (Scenario, line 122)
7. [7] Using `keys()` on an optionally matched relationship (Scenario, line 138)
8. [8] Using `keys()` and `IN` to check property existence (Scenario, line 154)

#### `official/expressions/graph/Graph9.feature`

**Feature:** Graph9 - Retrieve all properties as a property map

**Scenarios:** 7

1. [1] `properties()` on a node (Scenario, line 33)
2. [2] `properties()` on a relationship (Scenario, line 49)
3. [3] `properties()` on null (Scenario, line 65)
4. [4] `properties()` on a map (Scenario, line 78)
5. [5] `properties()` failing on an integer literal (Scenario, line 89)
6. [6] `properties()` failing on a string literal (Scenario, line 97)
7. [7] `properties()` failing on a list of booleans (Scenario, line 105)

#### `official/expressions/list/List1.feature`

**Feature:** List1 - Dynamic Element Access

**Scenarios:** 9

1. [1] Indexing into literal list (Scenario, line 34)
2. [2] Indexing into nested literal lists (Scenario, line 45)
3. [3] Use list lookup based on parameters when there is no type information (Scenario, line 56)
4. [4] Use list lookup based on parameters when there is lhs type information (Scenario, line 71)
5. [5] Use list lookup based on parameters when there is rhs type information (Scenario, line 85)
6. [6] Fail when indexing a non-list #Example: <exampleName> (Scenario Outline, line 100)
7. [7] Fail when indexing a non-list given by a parameter #Example: <exampleName> (Scenario Outline, line 116)
8. [8] Fail when indexing with a non-integer #Example: <exampleName> (Scenario Outline, line 135)
9. [9] Fail when indexing with a non-integer given by a parameter #Example: <exampleName> (Scenario Outline, line 152)

#### `official/expressions/list/List10.feature`

**Feature:** List10 - Reverse List

**Scenarios:** 0

#### `official/expressions/list/List11.feature`

**Feature:** List11 - Create a list from a range

**Scenarios:** 5

1. [1] Create list from `range()` with default step (Scenario Outline, line 33)
2. [2] Create list from `range()` with explicitly given step (Scenario Outline, line 61)
3. [3] Create an empty list if range direction and step direction are inconsistent (Scenario, line 101)
4. [4] Fail on invalid arguments for `range()` (Scenario Outline, line 117)
5. [5] Fail on invalid argument types for `range()` (Scenario Outline, line 132)

#### `official/expressions/list/List12.feature`

**Feature:** List12 - List Comprehension

**Scenarios:** 7

1. [1] Collect and extract using a list comprehension (Scenario, line 33)
2. [2] Collect and filter using a list comprehension (Scenario, line 55)
3. [3] Size of list comprehension (Scenario, line 77)
4. [4] Returning a list comprehension (Scenario, line 90)
5. [5] Using a list comprehension in a WITH (Scenario, line 108)
6. [6] Using a list comprehension in a WHERE (Scenario, line 127)
7. [7] Fail when using aggregation in list comprehension (Scenario, line 146)

#### `official/expressions/list/List2.feature`

**Feature:** List2 - List Slicing

**Scenarios:** 11

1. [1] List slice (Scenario, line 33)
2. [2] List slice with implicit end (Scenario, line 45)
3. [3] List slice with implicit start (Scenario, line 57)
4. [4] List slice with singleton range (Scenario, line 69)
5. [5] List slice with empty range (Scenario, line 81)
6. [6] List slice with negative range (Scenario, line 93)
7. [7] List slice with invalid range (Scenario, line 105)
8. [8] List slice with exceeding range (Scenario, line 117)
9. [9] List slice with null range (Scenario Outline, line 129)
10. [10] List slice with parameterised range (Scenario, line 149)
11. [11] List slice with parameterised invalid range (Scenario, line 164)

#### `official/expressions/list/List3.feature`

**Feature:** List3 - List Equality

**Scenarios:** 7

1. [1] Equality between list and literal should return false (Scenario, line 33)
2. [2] Equality of lists of different length should return false despite nulls (Scenario, line 44)
3. [3] Equality between different lists with null should return false (Scenario, line 55)
4. [4] Equality between almost equal lists with null should return null (Scenario, line 66)
5. [5] Equality of nested lists of different length should return false despite nulls (Scenario, line 77)
6. [6] Equality between different nested lists with null should return false (Scenario, line 88)
7. [7] Equality between almost equal nested lists with null should return null (Scenario, line 99)

#### `official/expressions/list/List4.feature`

**Feature:** List4 - List Concatenation

**Scenarios:** 2

1. [1] Concatenating lists of same type (Scenario, line 33)
2. [2] Concatenating a list with a scalar of same type (Scenario, line 44)

#### `official/expressions/list/List5.feature`

**Feature:** List5 - List Membership Validation - IN Operator

**Scenarios:** 42

1. [1] IN should work with nested list subscripting (Scenario, line 33)
2. [2] IN should work with nested literal list subscripting (Scenario, line 45)
3. [3] IN should work with list slices (Scenario, line 56)
4. [4] IN should work with literal list slices (Scenario, line 68)
5. [5] IN should return false when matching a number with a string (Scenario, line 79)
6. [6] IN should return false when matching a number with a string - list version (Scenario, line 90)
7. [7] IN should return false when types of LHS and RHS don't match - singleton list (Scenario, line 101)
8. [8] IN should return false when types of LHS and RHS don't match - list (Scenario, line 112)
9. [9] IN should return true when types of LHS and RHS match - singleton list (Scenario, line 123)
10. [10] IN should return true when types of LHS and RHS match - list (Scenario, line 134)
11. [11] IN should return false when order of elements in LHS list and RHS list don't match (Scenario, line 145)
12. [12] IN with different length lists should return false (Scenario, line 156)
13. [13] IN should return false when matching a list with a nested list with same elements (Scenario, line 167)
14. [14] IN should return true when both LHS and RHS contain nested lists (Scenario, line 178)
15. [15] IN should return true when both LHS and RHS contain a nested list alongside a scalar element (Scenario, line 189)
16. [16] IN should return true when LHS and RHS contain a nested list - singleton version (Scenario, line 200)
17. [17] IN should return true when LHS and RHS contain a nested list (Scenario, line 211)
18. [18] IN should return false when LHS contains a nested list and type mismatch on RHS - singleton version (Scenario, line 222)
19. [19] IN should return false when LHS contains a nested list and type mismatch on RHS (Scenario, line 233)
20. [20] IN should return null if LHS and RHS are null (Scenario, line 246)
21. [21] IN should return null if LHS and RHS are null - list version (Scenario, line 257)
22. [22] IN should return null when LHS and RHS both ultimately contain null, even if LHS and RHS are of different types (nested list and flat list) (Scenario, line 268)
23. [23] IN with different length lists should return false despite nulls (Scenario, line 279)
24. [24] IN should return true if match despite nulls (Scenario, line 290)
25. [25] IN should return null if comparison with null is required (Scenario, line 301)
26. [26] IN should return true if correct list found despite other lists having nulls (Scenario, line 312)
27. [27] IN should return true if correct list found despite null being another element within containing list (Scenario, line 323)
28. [28] IN should return false if no match can be found, despite nulls (Scenario, line 334)
29. [29] IN should return null if comparison with null is required, list version (Scenario, line 345)
30. [30] IN should return false if different length lists compared, even if the extra element is null (Scenario, line 356)
31. [31] IN should return null when comparing two so-called identical lists where one element is null (Scenario, line 367)
32. [32] IN should return true with previous null match, list version (Scenario, line 378)
33. [33] IN should return false if different length lists with nested elements compared, even if the extra element is null (Scenario, line 389)
34. [34] IN should return null if comparison with null is required, list version 2 (Scenario, line 400)
35. [35] IN should work with an empty list (Scenario, line 413)
36. [36] IN should return false for the empty list if the LHS and RHS types differ (Scenario, line 424)
37. [37] IN should work with an empty list in the presence of other list elements: matching (Scenario, line 435)
38. [38] IN should work with an empty list in the presence of other list elements: not matching (Scenario, line 446)
39. [39] IN should work with an empty list when comparing nested lists (Scenario, line 457)
40. [40] IN should return null if comparison with null is required for empty list (Scenario, line 468)
41. [41] IN should return true when LHS and RHS contain nested list with multiple empty lists (Scenario, line 479)
42. [42] Failing when using IN on a non-list literal (Scenario Outline, line 490)

#### `official/expressions/list/List6.feature`

**Feature:** List6 - List size

**Scenarios:** 10

1. [1] Return list size (Scenario, line 33)
2. [2] Setting and returning the size of a list property (Scenario, line 44)
3. [3] Concatenating and returning the size of literal lists (Scenario, line 62)
4. [4] `size()` on null list (Scenario, line 73)
5. [5] Fail for `size()` on paths (Scenario, line 85)
6. [6] Fail for `size()` on pattern predicates (Scenario Outline, line 94)
7. [7] Using size of pattern comprehension to test existence (Scenario, line 114)
8. [8] Get node degree via size of pattern comprehension (Scenario, line 132)
9. [9] Get node degree via size of pattern comprehension that specifies a relationship type (Scenario, line 151)
10. [10] Get node degree via size of pattern comprehension that specifies multiple relationship types (Scenario, line 171)

#### `official/expressions/list/List7.feature`

**Feature:** List7 - List Head

**Scenarios:** 0

#### `official/expressions/list/List8.feature`

**Feature:** List8 - List Last

**Scenarios:** 0

#### `official/expressions/list/List9.feature`

**Feature:** List9 - List Tail

**Scenarios:** 1

1. [1] Returning nested expressions based on list property (Scenario, line 33)

#### `official/expressions/literals/Literals1.feature`

**Feature:** Literals1 - Boolean and Null

**Scenarios:** 6

1. [1] Return a boolean true lower case (Scenario, line 33)
2. [2] Return a boolean true upper case (Scenario, line 45)
3. [3] Return a boolean false lower case (Scenario, line 56)
4. [4] Return a boolean false upper case (Scenario, line 68)
5. [5] Return null lower case (Scenario, line 79)
6. [6] Return null upper case (Scenario, line 91)

#### `official/expressions/literals/Literals2.feature`

**Feature:** Literals2 - Decimal integer

**Scenarios:** 12

1. [1] Return a short positive integer (Scenario, line 33)
2. [2] Return a long positive integer (Scenario, line 44)
3. [3] Return the largest integer (Scenario, line 55)
4. [4] Return a positive zero (Scenario, line 66)
5. [5] Return a negative zero (Scenario, line 77)
6. [6] Return a short negative integer (Scenario, line 88)
7. [7] Return a long negative integer (Scenario, line 99)
8. [8] Return the smallest integer (Scenario, line 110)
9. [9] Fail on a too large integer (Scenario, line 121)
10. [10] Fail on a too small integer (Scenario, line 129)
11. [11] Fail on an integer containing a alphabetic character (Scenario, line 138)
12. [12] Fail on an integer containing a invalid symbol character (Scenario, line 147)

#### `official/expressions/literals/Literals3.feature`

**Feature:** Literals3 - Hexadecimal integer

**Scenarios:** 16

1. [1] Return a short positive hexadecimal integer (Scenario, line 33)
2. [2] Return a long positive hexadecimal integer (Scenario, line 44)
3. [3] Return the largest hexadecimal integer (Scenario, line 55)
4. [4] Return a positive hexadecimal zero (Scenario, line 66)
5. [5] Return a negative hexadecimal zero (Scenario, line 77)
6. [6] Return a short negative hexadecimal integer (Scenario, line 88)
7. [7] Return a long negative hexadecimal integer (Scenario, line 99)
8. [8] Return the smallest hexadecimal integer (Scenario, line 110)
9. [9] Return a lower case hexadecimal integer (Scenario, line 121)
10. [10] Return a upper case hexadecimal integer (Scenario, line 132)
11. [11] Return a mixed case hexadecimal integer (Scenario, line 143)
12. [12] Fail on an incomplete hexadecimal integer (Scenario, line 155)
13. [13] Fail on an hexadecimal literal containing a lower case invalid alphanumeric character (Scenario, line 164)
14. [14] Fail on an hexadecimal literal containing a upper case invalid alphanumeric character (Scenario, line 173)
15. [16] Fail on a too large hexadecimal integer (Scenario, line 191)
16. [17] Fail on a too small hexadecimal integer (Scenario, line 199)

#### `official/expressions/literals/Literals4.feature`

**Feature:** Literals4 - Octal integer

**Scenarios:** 10

1. [1] Return a short positive octal integer (Scenario, line 33)
2. [2] Return a long positive octal integer (Scenario, line 44)
3. [3] Return the largest octal integer (Scenario, line 55)
4. [4] Return a positive octal zero (Scenario, line 66)
5. [5] Return a negative octal zero (Scenario, line 77)
6. [6] Return a short negative octal integer (Scenario, line 88)
7. [7] Return a long negative octal integer (Scenario, line 99)
8. [8] Return the smallest octal integer (Scenario, line 110)
9. [9] Fail on a too large octal integer (Scenario, line 121)
10. [10] Fail on a too small octal integer (Scenario, line 129)

#### `official/expressions/literals/Literals5.feature`

**Feature:** Literals5 - Float

**Scenarios:** 27

1. [1] Return a short positive float (Scenario, line 33)
2. [2] Return a short positive float without integer digits (Scenario, line 44)
3. [3] Return a long positive float (Scenario, line 55)
4. [4] Return a long positive float without integer digits (Scenario, line 66)
5. [5] Return a very long positive float (Scenario, line 77)
6. [6] Return a very long positive float without integer digits (Scenario, line 88)
7. [7] Return a positive zero float (Scenario, line 99)
8. [8] Return a positive zero float without integer digits (Scenario, line 110)
9. [9] Return a negative zero float (Scenario, line 121)
10. [10] Return a negative zero float without integer digits (Scenario, line 132)
11. [11] Return a very long negative float (Scenario, line 143)
12. [12] Return a very long negative float without integer digits (Scenario, line 154)
13. [13] Return a positive float with positive lower case exponent (Scenario, line 165)
14. [14] Return a positive float with positive upper case exponent (Scenario, line 176)
15. [15] Return a positive float with positive lower case exponent without integer digits (Scenario, line 187)
16. [16] Return a positive float with negative lower case exponent (Scenario, line 198)
17. [17] Return a positive float with negative lower case exponent without integer digits (Scenario, line 209)
18. [18] Return a positive float with negative upper case exponent without integer digits (Scenario, line 220)
19. [19] Return a negative float in with positive lower case exponent (Scenario, line 231)
20. [20] Return a negative float in with positive upper case exponent (Scenario, line 242)
21. [21] Return a negative float with positive lower case exponent without integer digits (Scenario, line 253)
22. [22] Return a negative float with negative lower case exponent (Scenario, line 264)
23. [23] Return a negative float with negative lower case exponent without integer digits (Scenario, line 275)
24. [24] Return a negative float with negative upper case exponent without integer digits (Scenario, line 286)
25. [25] Return a positive float with one integer digit and maximum positive exponent (Scenario, line 297)
26. [26] Return a positive float with nine integer digit and maximum positive exponent (Scenario, line 308)
27. [27] Fail when float value is too large (Scenario, line 319)

#### `official/expressions/literals/Literals6.feature`

**Feature:** Literals6 - String

**Scenarios:** 13

1. [1] Return a single-quoted empty string (Scenario, line 33)
2. [2] Return a single-quoted string with one character (Scenario, line 44)
3. [3] Return a single-quoted string with uft-8 characters (Scenario, line 55)
4. [4] Return a single-quoted string with escaped single-quoted (Scenario, line 66)
5. [5] Return a single-quoted string with escaped characters (Scenario, line 78)
6. [6] Return a single-quoted string with 100 characters (Scenario, line 90)
7. [7] Return a single-quoted string with 1000 characters (Scenario, line 101)
8. [8] Return a single-quoted string with 10000 characters (Scenario, line 112)
9. [9] Return a double-quoted empty string (Scenario, line 123)
10. [10] Accept valid Unicode literal (Scenario, line 134)
11. [11] Return a double-quoted string with one character (Scenario, line 146)
12. [12] Return a double-quoted string with uft-8 characters (Scenario, line 158)
13. [13] Failing on incorrect unicode literal (Scenario, line 170)

#### `official/expressions/literals/Literals7.feature`

**Feature:** Literals7 - List

**Scenarios:** 20

1. [1] Return an empty list (Scenario, line 33)
2. [2] Return a list containing a boolean (Scenario, line 44)
3. [3] Return a list containing a null (Scenario, line 55)
4. [4] Return a list containing a integer (Scenario, line 66)
5. [5] Return a list containing a hexadecimal integer (Scenario, line 77)
6. [6] Return a list containing a octal integer (Scenario, line 88)
7. [7] Return a list containing a float (Scenario, line 99)
8. [8] Return a list containing a string (Scenario, line 110)
9. [9] Return a list containing an empty lists (Scenario, line 121)
10. [10] Return seven-deep nested empty lists (Scenario, line 132)
11. [11] Return 20-deep nested empty lists (Scenario, line 143)
12. [12] Return 40-deep nested empty lists (Scenario, line 154)
13. [13] Return a list containing an empty map (Scenario, line 165)
14. [14] Return a list containing multiple integer (Scenario, line 176)
15. [16] Return a list containing multiple mixed values (Scenario, line 199)
16. [17] Return a list containing real and fake nested lists (Scenario, line 210)
17. [18] Return a complex list containing multiple mixed and nested values (Scenario, line 222)
18. [19] Fail on a list containing only a comma (Scenario, line 301)
19. [20] Fail on a nested list with non-matching brackets (Scenario, line 310)
20. [21] Fail on a nested list with missing commas (Scenario, line 319)

#### `official/expressions/literals/Literals8.feature`

**Feature:** Literals8 - Maps

**Scenarios:** 27

1. [1] Return an empty map (Scenario, line 33)
2. [2] Return a map containing one value with alphabetic lower case key (Scenario, line 44)
3. [3] Return a map containing one value with alphabetic upper case key (Scenario, line 55)
4. [4] Return a map containing one value with alphabetic mixed case key (Scenario, line 66)
5. [5] Return a map containing one value with alphanumeric mixed case key (Scenario, line 77)
6. [6] Return a map containing a boolean (Scenario, line 88)
7. [7] Return a map containing a null (Scenario, line 99)
8. [8] Return a map containing a integer (Scenario, line 110)
9. [9] Return a map containing a hexadecimal integer (Scenario, line 121)
10. [10] Return a map containing a octal integer (Scenario, line 132)
11. [11] Return a map containing a float (Scenario, line 143)
12. [12] Return a map containing a string (Scenario, line 154)
13. [13] Return a map containing an empty map (Scenario, line 165)
14. [14] Return seven-deep nested maps (Scenario, line 176)
15. [15] Return 20-deep nested maps (Scenario, line 187)
16. [16] Return 40-deep nested maps (Scenario, line 198)
17. [17] Return a map containing real and fake nested maps (Scenario, line 209)
18. [18] Return a complex map containing multiple mixed and nested values (Scenario, line 221)
19. [19] Fail on a map containing key starting with a number (Scenario, line 300)
20. [20] Fail on a map containing key with symbol (Scenario, line 309)
21. [21] Fail on a map containing key with dot (Scenario, line 318)
22. [22] Fail on a map containing unquoted string (Scenario, line 326)
23. [23] Fail on a map containing only a comma (Scenario, line 335)
24. [24] Fail on a map containing a value without key (Scenario, line 344)
25. [25] Fail on a map containing a list without key (Scenario, line 353)
26. [26] Fail on a map containing a map without key (Scenario, line 362)
27. [27] Fail on a nested map with non-matching braces (Scenario, line 371)

#### `official/expressions/map/Map1.feature`

**Feature:** Map1 - Static value access

**Scenarios:** 6

1. [1] Statically access a field of a non-null map (Scenario, line 34)
2. [2] Statically access a field of a null map (Scenario, line 46)
3. [3] Statically access a field of a map resulting from an expression (Scenario, line 58)
4. [4] Statically access a field is case-sensitive (Scenario Outline, line 70)
5. [5] Statically access a field with a delimited identifier (Scenario Outline, line 89)
6. [6] Fail when performing property access on a non-map (Scenario Outline, line 110)

#### `official/expressions/map/Map2.feature`

**Feature:** Map2 - Dynamic Value Access

**Scenarios:** 8

1. [1] Dynamically access a field based on parameters when there is no type information (Scenario, line 34)
2. [2] Dynamically access a field based on parameters when there is rhs type information (Scenario, line 49)
3. [3] Dynamically access a field on null results in null (Scenario, line 64)
4. [4] Dynamically access a field with null results in null (Scenario, line 76)
5. [5] Dynamically access a field is case-sensitive (Scenario Outline, line 88)
6. [6] Fail at runtime when attempting to index with an Int into a Map (Scenario, line 110)
7. [7] Fail at runtime when trying to index into a map with a non-string (Scenario, line 122)
8. [8] Fail at runtime when trying to index something which is not a map (Scenario, line 134)

#### `official/expressions/map/Map3.feature`

**Feature:** Map3 - Keys function

**Scenarios:** 5

1. [1] Using `keys()` on a literal map (Scenario, line 33)
2. [2] Using `keys()` on a parameter map (Scenario, line 44)
3. [3] Using `keys()` on null map (Scenario, line 57)
4. [4] Using `keys()` on map with null values (Scenario Outline, line 69)
5. [5] Using `keys()` and `IN` to check field existence (Scenario, line 90)

#### `official/expressions/mathematical/Mathematical1.feature`

**Feature:** Mathematical1 - Unary delimiter

**Scenarios:** 0

#### `official/expressions/mathematical/Mathematical10.feature`

**Feature:** Mathematical10 - Random numbers

**Scenarios:** 0

#### `official/expressions/mathematical/Mathematical11.feature`

**Feature:** Mathematical11 - Signed numbers functions

**Scenarios:** 1

1. [1] Absolute function (Scenario, line 33)

#### `official/expressions/mathematical/Mathematical12.feature`

**Feature:** Mathematical12 - Rounding numbers

**Scenarios:** 0

#### `official/expressions/mathematical/Mathematical13.feature`

**Feature:** Mathematical13 - Square root

**Scenarios:** 1

1. [1] `sqrt()` returning float values (Scenario, line 33)

#### `official/expressions/mathematical/Mathematical14.feature`

**Feature:** Mathematical14 - Logarithm

**Scenarios:** 0

#### `official/expressions/mathematical/Mathematical15.feature`

**Feature:** Mathematical15 - Degrees and radians

**Scenarios:** 0

#### `official/expressions/mathematical/Mathematical16.feature`

**Feature:** Mathematical16 - Trigonometric functions

**Scenarios:** 0

#### `official/expressions/mathematical/Mathematical17.feature`

**Feature:** Mathematical17 - Inverse trigonometric functions

**Scenarios:** 0

#### `official/expressions/mathematical/Mathematical2.feature`

**Feature:** Mathematical2 - Addition

**Scenarios:** 1

1. [1] Allow addition (Scenario, line 33)

#### `official/expressions/mathematical/Mathematical3.feature`

**Feature:** Mathematical3 - Subtraction

**Scenarios:** 1

1. [1] Fail for invalid Unicode hyphen in subtraction (Scenario, line 34)

#### `official/expressions/mathematical/Mathematical4.feature`

**Feature:** Mathematical4 - Multiplication

**Scenarios:** 0

#### `official/expressions/mathematical/Mathematical5.feature`

**Feature:** Mathematical5 - Division

**Scenarios:** 0

#### `official/expressions/mathematical/Mathematical6.feature`

**Feature:** Mathematical6 - Modulo division

**Scenarios:** 0

#### `official/expressions/mathematical/Mathematical7.feature`

**Feature:** Mathematical7 - Power

**Scenarios:** 0

#### `official/expressions/mathematical/Mathematical8.feature`

**Feature:** Mathematical8 - Arithmetic precedence

**Scenarios:** 2

1. [1] Arithmetic precedence test (Scenario, line 33)
2. [2] Arithmetic precedence with parenthesis test (Scenario, line 44)

#### `official/expressions/mathematical/Mathematical9.feature`

**Feature:** Mathematical9 - Mathematical constants

**Scenarios:** 0

#### `official/expressions/null/Null1.feature`

**Feature:** Null1 - IS NULL validation

**Scenarios:** 6

1. [1] Property null check on non-null node (Scenario, line 33)
2. [2] Property null check on optional non-null node (Scenario, line 50)
3. [3] Property null check on null node (Scenario, line 67)
4. [4] A literal null IS null (Scenario, line 79)
5. [5] IS NULL on a map (Scenario Outline, line 90)
6. [6] IS NULL is case insensitive (Scenario, line 118)

#### `official/expressions/null/Null2.feature`

**Feature:** Null2 - IS NOT NULL validation

**Scenarios:** 6

1. [1] Property not null check on non-null node (Scenario, line 33)
2. [2] Property not null check on optional non-null node (Scenario, line 50)
3. [3] Property not null check on null node (Scenario, line 67)
4. [4] A literal null is not IS NOT null (Scenario, line 79)
5. [5] IS NOT NULL on a map (Scenario Outline, line 90)
6. [6] IS NOT NULL is case insensitive (Scenario, line 118)

#### `official/expressions/null/Null3.feature`

**Feature:** Null3 - Null evaluation

**Scenarios:** 4

1. [1] The inverse of a null is a null (Scenario, line 34)
2. [2] It is unknown - i.e. null - if a null is equal to a null (Scenario, line 45)
3. [3] It is unknown - i.e. null - if a null is not equal to a null (Scenario, line 56)
4. [4] Using null in IN (Scenario Outline, line 67)

#### `official/expressions/path/Path1.feature`

**Feature:** Path1 - Nodes of a path

**Scenarios:** 1

1. [1] `nodes()` on null path (Scenario, line 33)

#### `official/expressions/path/Path2.feature`

**Feature:** Path2 - Relationships of a path

**Scenarios:** 3

1. [1] Return relationships by fetching them from the path (Scenario, line 33)
2. [2] Return relationships by fetching them from the path - starting from the end (Scenario, line 50)
3. [3] `relationships()` on null path (Scenario, line 66)

#### `official/expressions/path/Path3.feature`

**Feature:** Path3 - Length of a path

**Scenarios:** 3

1. [1] Return a var length path of length zero (Scenario, line 33)
2. [2] Failing when using `length()` on a node (Scenario, line 51)
3. [3] Failing when using `length()` on a relationship (Scenario, line 60)

#### `official/expressions/pattern/Pattern1.feature`

**Feature:** Pattern1 - Pattern predicate

**Scenarios:** 25

1. [1] Matching on any single outgoing directed connection (Scenario, line 33)
2. [2] Matching on a single undirected connection (Scenario, line 49)
3. [3] Matching on any single incoming directed connection (Scenario, line 67)
4. [4] Matching on a specific type of single outgoing directed connection (Scenario, line 85)
5. [5] Matching on a specific type of single undirected connection (Scenario, line 100)
6. [6] Matching on a specific type of single incoming directed connection (Scenario, line 117)
7. [7] Matching on a specific type of a variable length outgoing directed connection (Scenario, line 133)
8. [8] Matching on a specific type of variable length undirected connection (Scenario, line 148)
9. [9] Matching on a specific type of variable length incoming directed connection (Scenario, line 165)
10. [10] Matching on a specific type of undirected connection with length 2 (Scenario, line 181)
11. [10] Fail on introducing unbounded variables in pattern (Scenario Outline, line 197)
12. [11] Fail on checking self pattern (Scenario, line 223)
13. [12] Matching two nodes on a single directed connection between them (Scenario, line 231)
14. [13] Fail on matching two nodes on a single undirected connection between them (Scenario, line 249)
15. [14] Matching two nodes on a specific type of single outgoing directed connection (Scenario, line 269)
16. [15] Matching two nodes on a specific type of single undirected connection (Scenario, line 285)
17. [16] Matching two nodes on a specific type of a variable length outgoing directed connection (Scenario, line 303)
18. [17] Matching two nodes on a specific type of variable length undirected connection (Scenario, line 319)
19. [18] Matching two nodes on a specific type of undirected connection with length 2 (Scenario, line 339)
20. [19] Using a negated existential pattern predicate (Scenario, line 356)
21. [20] Using two existential pattern predicates in a conjunction (Scenario, line 372)
22. [21] Using two existential pattern predicates in a disjunction (Scenario, line 387)
23. [22] Fail on using pattern in RETURN projection (Scenario, line 404)
24. [23] Fail on using pattern in WITH projection (Scenario, line 412)
25. [24] Fail on using pattern in right-hand side of SET (Scenario, line 420)

#### `official/expressions/pattern/Pattern2.feature`

**Feature:** Pattern2 - Pattern Comprehension

**Scenarios:** 11

1. [1] Return a pattern comprehension (Scenario, line 33)
2. [2] Return a pattern comprehension with label predicate (Scenario, line 53)
3. [3] Return a pattern comprehension with bound nodes (Scenario, line 72)
4. [4] Introduce a new node variable in pattern comprehension (Scenario, line 89)
5. [5] Introduce a new relationship variable in pattern comprehension (Scenario, line 109)
6. [6] Aggregate on a pattern comprehension (Scenario, line 129)
7. [7] Use a pattern comprehension inside a list comprehension (Scenario, line 146)
8. [8] Use a pattern comprehension in WITH (Scenario, line 170)
9. [9] Use a variable-length pattern comprehension in WITH (Scenario, line 190)
10. [10] Use a pattern comprehension in RETURN (Scenario, line 207)
11. [11] Use a pattern comprehension and ORDER BY (Scenario, line 226)

#### `official/expressions/precedence/Precedence1.feature`

**Feature:** Precedence1 - On boolean values

**Scenarios:** 28

1. [1] Exclusive disjunction takes precedence over inclusive disjunction (Scenario, line 33)
2. [2] Conjunction disjunction takes precedence over exclusive disjunction (Scenario, line 46)
3. [3] Conjunction disjunction takes precedence over inclusive disjunction (Scenario, line 59)
4. [4] Negation takes precedence over conjunction (Scenario, line 72)
5. [5] Negation takes precedence over inclusive disjunction (Scenario, line 87)
6. [6] Comparison operator takes precedence over boolean negation (Scenario, line 100)
7. [7] Comparison operator takes precedence over binary boolean operator (Scenario, line 113)
8. [8] Null predicate takes precedence over comparison operator (Scenario, line 126)
9. [9] Null predicate takes precedence over negation (Scenario, line 139)
10. [10] Null predicate takes precedence over boolean operator (Scenario, line 152)
11. [11] List predicate takes precedence over comparison operator (Scenario, line 165)
12. [12] List predicate takes precedence over negation (Scenario, line 178)
13. [13] List predicate takes precedence over boolean operator (Scenario, line 191)
14. [14] Exclusive disjunction takes precedence over inclusive disjunction in every combination of truth values (Scenario, line 204)
15. [15] Conjunction takes precedence over exclusive disjunction in every combination of truth values (Scenario, line 220)
16. [16] Conjunction takes precedence over inclusive disjunction in every combination of truth values (Scenario, line 236)
17. [17] Negation takes precedence over conjunction in every combination of truth values (Scenario, line 252)
18. [18] Negation takes precedence over inclusive disjunction in every combination of truth values (Scenario, line 269)
19. [19] Comparison operators takes precedence over boolean negation in every combination of truth values (Scenario Outline, line 284)
20. [20] Pairs of comparison operators and boolean negation that are associative in every combination of truth values (Scenario Outline, line 308)
21. [21] Comparison operators take precedence over binary boolean operators in every combination of truth values (Scenario Outline, line 327)
22. [22] Pairs of comparison operators and binary boolean operators that are associative in every combination of truth values (Scenario Outline, line 364)
23. [23] Null predicates take precedence over comparison operators in every combination of truth values (Scenario Outline, line 386)
24. [24] Null predicates take precedence over boolean negation on every truth values (Scenario Outline, line 416)
25. [25] Null predicates take precedence over binary boolean operators in every combination of truth values (Scenario Outline, line 436)
26. [26] List predicate takes precedence over comparison operators in every combination of truth values (Scenario Outline, line 460)
27. [27] List predicate takes precedence over negation in every combination of truth values (Scenario, line 485)
28. [28] List predicate takes precedence over binary boolean operators in every combination of truth values (Scenario Outline, line 500)

#### `official/expressions/precedence/Precedence2.feature`

**Feature:** Precedence2 - On numeric values

**Scenarios:** 5

1. [1] Numeric multiplicative operations takes precedence over numeric additive operations (Scenario Outline, line 33)
2. [2] Exponentiation takes precedence over numeric multiplicative operations (Scenario Outline, line 67)
3. [3] Exponentiation takes precedence over numeric additive operations (Scenario Outline, line 86)
4. [4] Numeric unary negative takes precedence over exponentiation (Scenario, line 104)
5. [5] Numeric unary negative takes precedence over numeric additive operations (Scenario Outline, line 119)

#### `official/expressions/precedence/Precedence3.feature`

**Feature:** Precedence3 - On list values

**Scenarios:** 6

1. [1] List element access takes precedence over list appending (Scenario, line 33)
2. [2] List element access takes precedence over list concatenation (Scenario, line 46)
3. [3] List slicing takes precedence over list concatenation (Scenario, line 59)
4. [4] List appending takes precedence over list element containment (Scenario, line 72)
5. [5] List concatenation takes precedence over list element containment (Scenario, line 85)
6. [6] List element containment takes precedence over comparison operator (Scenario Outline, line 99)

#### `official/expressions/precedence/Precedence4.feature`

**Feature:** Precedence4 - On null value

**Scenarios:** 4

1. [1] Null predicate takes precedence over comparison operator (Scenario Outline, line 34)
2. [2] Null predicate takes precedence over boolean negation (Scenario, line 53)
3. [3] Null predicate takes precedence over binary boolean operator (Scenario Outline, line 66)
4. [4] String predicate takes precedence over binary boolean operator (Scenario, line 89)

#### `official/expressions/quantifier/Quantifier1.feature`

**Feature:** Quantifier1 - None quantifier

**Scenarios:** 15

1. [1] None quantifier is always true on empty list (Scenario, line 33)
2. [2] None quantifier on list literal containing booleans (Scenario Outline, line 44)
3. [3] None quantifier on list literal containing integers (Scenario Outline, line 67)
4. [4] None quantifier on list literal containing floats (Scenario Outline, line 98)
5. [5] None quantifier on list literal containing strings (Scenario Outline, line 124)
6. [6] None quantifier on list literal containing lists (Scenario Outline, line 147)
7. [7] None quantifier on list literal containing maps (Scenario Outline, line 170)
8. [8] None quantifier on list containing nodes (Scenario, line 193)
9. [9] None quantifier on list containing relationships (Scenario, line 239)
10. [10] None quantifier on lists containing nulls (Scenario Outline, line 285)
11. [11] None quantifier with IS NULL predicate (Scenario Outline, line 307)
12. [12] None quantifier with IS NOT NULL predicate (Scenario Outline, line 333)
13. [13] None quantifier is true if the predicate is statically false and the list is not empty (Scenario, line 359)
14. [14] None quantifier is false if the predicate is statically true and the list is not empty (Scenario, line 370)
15. [15] Fail none quantifier on type mismatch between list elements and predicate (Scenario Outline, line 381)

#### `official/expressions/quantifier/Quantifier10.feature`

**Feature:** Quantifier10 - Single quantifier invariants

**Scenarios:** 4

1. [1] Single quantifier is always false if the predicate is statically false and the list is not empty (Scenario, line 33)
2. [2] Single quantifier is always false if the predicate is statically true and the list has more than one element (Scenario, line 56)
3. [3] Single quantifier is always true if the predicate is statically true and the list has exactly one non-null element (Scenario, line 79)
4. [4] Single quantifier is always equal whether the size of the list filtered with same the predicate is one (Scenario Outline, line 93)

#### `official/expressions/quantifier/Quantifier11.feature`

**Feature:** Quantifier11 - Any quantifier invariants

**Scenarios:** 6

1. [1] Any quantifier is always false if the predicate is statically false and the list is not empty (Scenario, line 33)
2. [2] Any quantifier is always true if the predicate is statically true and the list is not empty (Scenario, line 56)
3. [3] Any quantifier is always true if the single or the all quantifier is true (Scenario Outline, line 79)
4. [4] Any quantifier is always equal the boolean negative of the none quantifier (Scenario Outline, line 115)
5. [5] Any quantifier is always equal the boolean negative of the all quantifier on the boolean negative of the predicate (Scenario Outline, line 145)
6. [6] Any quantifier is always equal whether the size of the list filtered with same the predicate is grater zero (Scenario Outline, line 175)

#### `official/expressions/quantifier/Quantifier12.feature`

**Feature:** Quantifier12 - All quantifier invariants

**Scenarios:** 5

1. [1] All quantifier is always false if the predicate is statically false and the list is not empty (Scenario, line 33)
2. [2] All quantifier is always true if the predicate is statically true and the list is not empty (Scenario, line 56)
3. [3] All quantifier is always equal the none quantifier on the boolean negative of the predicate (Scenario Outline, line 79)
4. [4] All quantifier is always equal the boolean negative of the any quantifier on the boolean negative of the predicate (Scenario Outline, line 109)
5. [5] All quantifier is always equal whether the size of the list filtered with same the predicate is equal the size of the unfiltered list (Scenario Outline, line 139)

#### `official/expressions/quantifier/Quantifier2.feature`

**Feature:** Quantifier2 - Single quantifier

**Scenarios:** 16

1. [1] Single quantifier is always false on empty list (Scenario, line 33)
2. [2] Single quantifier on list literal (Scenario Outline, line 44)
3. [3] Single quantifier on list literal containing integers (Scenario Outline, line 67)
4. [4] Single quantifier on list literal containing floats (Scenario Outline, line 98)
5. [5] Single quantifier on list literal containing strings (Scenario Outline, line 124)
6. [6] Single quantifier on list literal containing lists (Scenario Outline, line 147)
7. [7] Single quantifier on list literal containing maps (Scenario Outline, line 170)
8. [8] Single quantifier on list containing nodes (Scenario, line 193)
9. [9] Single quantifier on list containing relationships (Scenario, line 239)
10. [10] Single quantifier on lists containing nulls (Scenario Outline, line 285)
11. [11] Single quantifier with IS NULL predicate (Scenario Outline, line 307)
12. [12] Single quantifier with IS NOT NULL predicate (Scenario Outline, line 333)
13. [13] Single quantifier is false if the predicate is statically false and the list is not empty (Scenario, line 359)
14. [14] Single quantifier is false if the predicate is statically true and the list has more than one element (Scenario, line 370)
15. [15] Single quantifier is true if the predicate is statically true and the list has exactly one non-null element (Scenario, line 381)
16. [16] Fail single quantifier on type mismatch between list elements and predicate (Scenario Outline, line 392)

#### `official/expressions/quantifier/Quantifier3.feature`

**Feature:** Quantifier3 - Any quantifier

**Scenarios:** 15

1. [1] Any quantifier is always false on empty list (Scenario, line 33)
2. [2] Any quantifier on list literal (Scenario Outline, line 44)
3. [3] Any quantifier on list literal containing integers (Scenario Outline, line 67)
4. [4] Any quantifier on list literal containing floats (Scenario Outline, line 98)
5. [5] Any quantifier on list literal containing strings (Scenario Outline, line 124)
6. [6] Any quantifier on list literal containing lists (Scenario Outline, line 147)
7. [7] Any quantifier on list literal containing maps (Scenario Outline, line 170)
8. [8] Any quantifier on list containing nodes (Scenario, line 193)
9. [9] Any quantifier on list containing relationships (Scenario, line 239)
10. [10] Any quantifier on lists containing nulls (Scenario Outline, line 285)
11. [11] Any quantifier with IS NULL predicate (Scenario Outline, line 307)
12. [12] Any quantifier with IS NOT NULL predicate (Scenario Outline, line 333)
13. [13] Any quantifier is false if the predicate is statically false and the list is not empty (Scenario, line 359)
14. [14] Any quantifier is true if the predicate is statically true and the list is not empty (Scenario, line 370)
15. [15] Fail any quantifier on type mismatch between list elements and predicate (Scenario Outline, line 381)

#### `official/expressions/quantifier/Quantifier4.feature`

**Feature:** Quantifier4 - All quantifier

**Scenarios:** 15

1. [1] All quantifier is always true on empty list (Scenario, line 33)
2. [2] All quantifier on list literal (Scenario Outline, line 44)
3. [3] All quantifier on list literal containing integers (Scenario Outline, line 67)
4. [4] All quantifier on list literal containing floats (Scenario Outline, line 98)
5. [5] All quantifier on list literal containing strings (Scenario Outline, line 124)
6. [6] All quantifier on list literal containing lists (Scenario Outline, line 147)
7. [7] All quantifier on list literal containing maps (Scenario Outline, line 170)
8. [8] All quantifier on list containing nodes (Scenario, line 193)
9. [9] All quantifier on list containing relationships (Scenario, line 239)
10. [10] All quantifier on lists containing nulls (Scenario Outline, line 285)
11. [11] All quantifier with IS NULL predicate (Scenario Outline, line 307)
12. [12] All quantifier with IS NOT NULL predicate (Scenario Outline, line 333)
13. [13] All quantifier is false if the predicate is statically false and the list is not empty (Scenario, line 359)
14. [14] All quantifier is true if the predicate is statically true and the list is not empty (Scenario, line 370)
15. [15] Fail all quantifier on type mismatch between list elements and predicate (Scenario Outline, line 381)

#### `official/expressions/quantifier/Quantifier5.feature`

**Feature:** Quantifier5 - None quantifier interop

**Scenarios:** 5

1. [1] None quantifier can nest itself and other quantifiers on nested lists (Scenario Outline, line 33)
2. [2] None quantifier can nest itself and other quantifiers on the same list (Scenario Outline, line 55)
3. [3] None quantifier is equal the boolean negative of the any quantifier (Scenario Outline, line 78)
4. [4] None quantifier is equal the all quantifier on the boolean negative of the predicate (Scenario Outline, line 97)
5. [5] None quantifier is equal whether the size of the list filtered with same the predicate is zero (Scenario Outline, line 116)

#### `official/expressions/quantifier/Quantifier6.feature`

**Feature:** Quantifier6 - Single quantifier interop

**Scenarios:** 3

1. [1] Single quantifier can nest itself and other quantifiers on nested lists (Scenario Outline, line 33)
2. [2] Single quantifier can nest itself and other quantifiers on the same list (Scenario Outline, line 55)
3. [3] Single quantifier is equal whether the size of the list filtered with same the predicate is one (Scenario Outline, line 78)

#### `official/expressions/quantifier/Quantifier7.feature`

**Feature:** Quantifier7 - Any quantifier interop

**Scenarios:** 6

1. [1] Any quantifier can nest itself and other quantifiers on nested lists (Scenario Outline, line 33)
2. [2] Any quantifier can nest itself and other quantifiers on the same list (Scenario Outline, line 55)
3. [3] Any quantifier is true if the single or the all quantifier is true (Scenario Outline, line 78)
4. [4] Any quantifier is equal the boolean negative of the none quantifier (Scenario Outline, line 98)
5. [5] Any quantifier is equal the boolean negative of the all quantifier on the boolean negative of the predicate (Scenario Outline, line 117)
6. [6] Any quantifier is equal whether the size of the list filtered with same the predicate is grater zero (Scenario Outline, line 136)

#### `official/expressions/quantifier/Quantifier8.feature`

**Feature:** Quantifier8 - All quantifier interop

**Scenarios:** 5

1. [1] All quantifier can nest itself and other quantifiers on nested lists (Scenario Outline, line 33)
2. [2] All quantifier can nest itself and other quantifiers on the same list (Scenario Outline, line 55)
3. [3] All quantifier is equal the none quantifier on the boolean negative of the predicate (Scenario Outline, line 78)
4. [4] All quantifier is equal the boolean negative of the any quantifier on the boolean negative of the predicate (Scenario Outline, line 97)
5. [5] All quantifier is equal whether the size of the list filtered with same the predicate is equal the size of the unfiltered list (Scenario Outline, line 116)

#### `official/expressions/quantifier/Quantifier9.feature`

**Feature:** Quantifier9 - None quantifier invariants

**Scenarios:** 5

1. [1] None quantifier is always true if the predicate is statically false and the list is not empty (Scenario, line 33)
2. [2] None quantifier is always false if the predicate is statically true and the list is not empty (Scenario, line 56)
3. [3] None quantifier is always equal the boolean negative of the any quantifier (Scenario Outline, line 79)
4. [4] None quantifier is always equal the all quantifier on the boolean negative of the predicate (Scenario Outline, line 109)
5. [5] None quantifier is always equal whether the size of the list filtered with same the predicate is zero (Scenario Outline, line 139)

#### `official/expressions/string/String1.feature`

**Feature:** String1 - Substring extraction

**Scenarios:** 1

1. [1] `substring()` with default second argument (Scenario, line 33)

#### `official/expressions/string/String10.feature`

**Feature:** String10 - Exact Substring Search

**Scenarios:** 9

1. [1] Finding exact matches with non-proper substring (Scenario, line 33)
2. [2] Finding substring of string (Scenario, line 52)
3. [3] Finding the empty substring (Scenario, line 71)
4. [4] Finding strings containing whitespace (Scenario, line 94)
5. [5] Finding strings containing newline (Scenario, line 116)
6. [6] No string contains null (Scenario, line 138)
7. [7] No string does not contain null (Scenario, line 156)
8. [8] Handling non-string operands for CONTAINS (Scenario, line 174)
9. [9] NOT with CONTAINS (Scenario, line 195)

#### `official/expressions/string/String11.feature`

**Feature:** String11 - Combining Exact String Search

**Scenarios:** 2

1. [1] Combining prefix and suffix search (Scenario, line 33)
2. [2] Combining prefix, suffix, and substring search (Scenario, line 53)

#### `official/expressions/string/String12.feature`

**Feature:** String12 - Exact Substring Replacement

**Scenarios:** 0

#### `official/expressions/string/String13.feature`

**Feature:** String13 - Regex Search

**Scenarios:** 0

#### `official/expressions/string/String14.feature`

**Feature:** String14 - Regex Replacing

**Scenarios:** 0

#### `official/expressions/string/String2.feature`

**Feature:** String2 - Whitespace Trimming

**Scenarios:** 0

#### `official/expressions/string/String3.feature`

**Feature:** String3 - String Reversal

**Scenarios:** 1

1. [1] `reverse()` (Scenario, line 33)

#### `official/expressions/string/String4.feature`

**Feature:** String4 - String Splitting

**Scenarios:** 1

1. [1] `split()` (Scenario, line 33)

#### `official/expressions/string/String5.feature`

**Feature:** String5 - Manipulate Letter Casing

**Scenarios:** 0

#### `official/expressions/string/String6.feature`

**Feature:** String6 - String Concatenation

**Scenarios:** 0

#### `official/expressions/string/String7.feature`

**Feature:** String7 - String Length

**Scenarios:** 0

#### `official/expressions/string/String8.feature`

**Feature:** String8 - Exact String Prefix Search

**Scenarios:** 9

1. [1] Finding exact matches with non-proper prefix (Scenario, line 33)
2. [2] Finding beginning of string (Scenario, line 52)
3. [3] Finding the empty prefix (Scenario, line 71)
4. [4] Finding strings starting with whitespace (Scenario, line 94)
5. [5] Finding strings starting with newline (Scenario, line 116)
6. [6] No string starts with null (Scenario, line 138)
7. [7] No string does not start with null (Scenario, line 156)
8. [8] Handling non-string operands for STARTS WITH (Scenario, line 174)
9. [9] NOT with STARTS WITH (Scenario, line 195)

#### `official/expressions/string/String9.feature`

**Feature:** String9 - Exact String Suffix Search

**Scenarios:** 9

1. [1] Finding exact matches with non-proper suffix (Scenario, line 33)
2. [2] Finding end of string (Scenario, line 52)
3. [3] Finding the empty suffix (Scenario, line 71)
4. [4] Finding strings ending with whitespace (Scenario, line 94)
5. [5] Finding strings ending with newline (Scenario, line 116)
6. [6] No string ends with null (Scenario, line 138)
7. [7] No string does not end with null (Scenario, line 156)
8. [8] Handling non-string operands for ENDS WITH (Scenario, line 174)
9. [9] NOT with ENDS WITH (Scenario, line 195)

#### `official/expressions/temporal/Temporal1.feature`

**Feature:** Temporal1 - Create Temporal Values from a Map

**Scenarios:** 13

1. [1] Should construct week date (Scenario Outline, line 33)
2. [2] Should construct week localdatetime (Scenario Outline, line 62)
3. [3] Should construct week datetime (Scenario Outline, line 91)
4. [4] Should construct date (Scenario Outline, line 120)
5. [5] Should construct local time (Scenario Outline, line 142)
6. [6] Should construct time (Scenario Outline, line 163)
7. [7] Should construct local date time (Scenario Outline, line 191)
8. [8] Should construct date time with default time zone (Scenario Outline, line 236)
9. [9] Should construct date time with offset time zone (Scenario Outline, line 280)
10. [10] Should construct date time with named time zone (Scenario Outline, line 323)
11. [11] Should construct date time from epoch (Scenario, line 366)
12. [12] Should construct duration (Scenario Outline, line 378)
13. [13] Should construct temporal with time offset with second precision (Scenario Outline, line 401)

#### `official/expressions/temporal/Temporal10.feature`

**Feature:** Temporal10 - Compute Durations Between two Temporal Values

**Scenarios:** 13

1. [1] Should split between boundaries correctly (Scenario Outline, line 33)
2. [2] Should compute duration between two temporals (Scenario Outline, line 54)
3. [3] Should compute duration between two temporals in months (Scenario Outline, line 93)
4. [4] Should compute duration between two temporals in days (Scenario Outline, line 128)
5. [5] Should compute duration between two temporals in seconds (Scenario Outline, line 163)
6. [6] Should compute duration between if they differ only by a fraction of a second and the first comes after the second. (Scenario, line 202)
7. [7] Should compute negative duration between in big units (Scenario Outline, line 213)
8. [8] Should handle durations at daylight saving time day (Scenario Outline, line 232)
9. [9] Should handle large durations (Scenario, line 252)
10. [10] Should handle large durations in seconds (Scenario, line 263)
11. [11] Should handle when seconds and subseconds have different signs (Scenario Outline, line 274)
12. [12] Should compute durations with no difference (Scenario Outline, line 298)
13. [13] Should propagate null (Scenario Outline, line 317)

#### `official/expressions/temporal/Temporal2.feature`

**Feature:** Temporal2 - Create Temporal Values from a String

**Scenarios:** 7

1. [1] Should parse date from string (Scenario Outline, line 33)
2. [2] Should parse local time from string (Scenario Outline, line 58)
3. [3] Should parse time from string (Scenario Outline, line 79)
4. [4] Should parse local date time from string (Scenario Outline, line 101)
5. [5] Should parse date time from string (Scenario Outline, line 122)
6. [6] Should parse date time with named time zone from string (Scenario Outline, line 144)
7. [7] Should parse duration from string (Scenario Outline, line 163)

#### `official/expressions/temporal/Temporal3.feature`

**Feature:** Temporal3 - Project Temporal Values from other Temporal Values

**Scenarios:** 11

1. [1] Should select date (Scenario Outline, line 32)
2. [2] Should select local time (Scenario Outline, line 68)
3. [3] Should select time (Scenario Outline, line 95)
4. [4] Should select date into local date time (Scenario Outline, line 130)
5. [5] Should select time into local date time (Scenario Outline, line 151)
6. [6] Should select date and time into local date time (Scenario Outline, line 174)
7. [7] Should select datetime into local date time (Scenario Outline, line 213)
8. [8] Should select date into date time (Scenario Outline, line 234)
9. [9] Should select time into date time (Scenario Outline, line 261)
10. [10] Should select date and time into date time (Scenario Outline, line 292)
11. [11] Should datetime into date time (Scenario Outline, line 355)

#### `official/expressions/temporal/Temporal4.feature`

**Feature:** Temporal4 - Store Temporal Values

**Scenarios:** 13

1. [1] Should store date (Scenario Outline, line 34)
2. [2] Should store date array (Scenario Outline, line 57)
3. [3] Should store local time (Scenario Outline, line 81)
4. [4] Should store local time array (Scenario Outline, line 104)
5. [5] Should store time (Scenario Outline, line 128)
6. [6] Should store time array (Scenario Outline, line 151)
7. [7] Should store local date time (Scenario Outline, line 175)
8. [8] Should store local date time array (Scenario Outline, line 198)
9. [9] Should store date time (Scenario Outline, line 222)
10. [10] Should store date time array (Scenario Outline, line 245)
11. [11] Should store duration (Scenario Outline, line 269)
12. [12] Should store duration array (Scenario Outline, line 292)
13. [13] Should propagate null (Scenario Outline, line 316)

#### `official/expressions/temporal/Temporal5.feature`

**Feature:** Temporal5 - Access Components of Temporal Values

**Scenarios:** 7

1. [1] Should provide accessors for date (Scenario, line 33)
2. [2] Should provide accessors for date in last weekYear (Scenario, line 50)
3. [3] Should provide accessors for local time (Scenario, line 67)
4. [4] Should provide accessors for time (Scenario, line 84)
5. [5] Should provide accessors for local date time (Scenario, line 101)
6. [6] Should provide accessors for date time (Scenario, line 119)
7. [7] Should provide accessors for duration (Scenario, line 138)

#### `official/expressions/temporal/Temporal6.feature`

**Feature:** Temporal6 - Render Temporal Values as a String

**Scenarios:** 7

1. [1] Should serialize date (Scenario, line 33)
2. [2] Should serialize local time (Scenario, line 45)
3. [3] Should serialize time (Scenario, line 57)
4. [4] Should serialize local date time (Scenario, line 69)
5. [5] Should serialize date time (Scenario, line 81)
6. [6] Should serialize duration (Scenario Outline, line 93)
7. [7] Should serialize timezones correctly (Scenario, line 119)

#### `official/expressions/temporal/Temporal7.feature`

**Feature:** Temporal7 - Compare Temporal Values

**Scenarios:** 6

1. [1] Should compare dates (Scenario Outline, line 33)
2. [2] Should compare local times (Scenario Outline, line 50)
3. [3] Should compare times (Scenario Outline, line 67)
4. [4] Should compare local date times (Scenario Outline, line 84)
5. [5] Should compare date times (Scenario Outline, line 101)
6. [6] Should compare durations for equality (Scenario Outline, line 118)

#### `official/expressions/temporal/Temporal8.feature`

**Feature:** Temporal8 - Compute Arithmetic Operations on Temporal Values

**Scenarios:** 7

1. [1] Should add or subtract duration to or from date (Scenario Outline, line 33)
2. [2] Should add or subtract duration to or from local time (Scenario Outline, line 56)
3. [3] Should add or subtract duration to or from time (Scenario Outline, line 79)
4. [4] Should add or subtract duration to or from local date time (Scenario Outline, line 102)
5. [5] Should add or subtract duration to or from date time (Scenario Outline, line 125)
6. [6] Should add or subtract durations (Scenario Outline, line 148)
7. [7] Should multiply or divide durations by numbers (Scenario Outline, line 177)

#### `official/expressions/temporal/Temporal9.feature`

**Feature:** Temporal9 - Truncate Temporal Values

**Scenarios:** 5

1. [1] Should truncate date (Scenario Outline, line 33)
2. [2] Should truncate datetime (Scenario Outline, line 98)
3. [3] Should truncate localdatetime (Scenario Outline, line 217)
4. [4] Should truncate localtime (Scenario Outline, line 305)
5. [5] Should truncate time (Scenario Outline, line 363)

#### `official/expressions/typeConversion/TypeConversion1.feature`

**Feature:** TypeConversion1 - To Boolean

**Scenarios:** 5

1. [1] `toBoolean()` on booleans (Scenario, line 33)
2. [2] `toBoolean()` on valid literal string (Scenario, line 46)
3. [3] `toBoolean()` on variables with valid string values (Scenario, line 57)
4. [4] `toBoolean()` on invalid strings (Scenario, line 70)
5. [5] Fail `toBoolean()` on invalid types #Example: <exampleName> (Scenario Outline, line 85)

#### `official/expressions/typeConversion/TypeConversion2.feature`

**Feature:** TypeConversion2 - To Integer

**Scenarios:** 8

1. [1] `toInteger()` on float (Scenario, line 33)
2. [2] `toInteger()` returning null on non-numerical string (Scenario, line 45)
3. [3] `toInteger()` handling mixed number types (Scenario, line 57)
4. [4] `toInteger()` handling Any type (Scenario, line 69)
5. [5] `toInteger()` on a list of strings (Scenario, line 81)
6. [6] `toInteger()` on a complex-typed expression (Scenario, line 93)
7. [7] `toInteger()` on node property (Scenario, line 106)
8. [8] Fail `toInteger()` on invalid types #Example: <exampleName> (Scenario Outline, line 124)

#### `official/expressions/typeConversion/TypeConversion3.feature`

**Feature:** TypeConversion3 - To Float

**Scenarios:** 6

1. [1] `toFloat()` on mixed number types (Scenario, line 33)
2. [2] `toFloat()` returning null on non-numerical string (Scenario, line 45)
3. [3] `toFloat()` handling Any type (Scenario, line 57)
4. [4] `toFloat()` on a list of strings (Scenario, line 69)
5. [5] `toFloat()` on node property (Scenario, line 81)
6. [6] Fail `toFloat()` on invalid types #Example: <exampleName> (Scenario Outline, line 99)

#### `official/expressions/typeConversion/TypeConversion4.feature`

**Feature:** TypeConversion4 - To String

**Scenarios:** 10

1. [1] `toString()` handling integer literal (Scenario, line 33)
2. [2] `toString()` handling boolean literal (Scenario, line 44)
3. [3] `toString()` handling inlined boolean (Scenario, line 55)
4. [4] `toString()` handling boolean properties (Scenario, line 66)
5. [5] `toString()` should work on Any type (Scenario, line 82)
6. [6] `toString()` on a list of integers (Scenario, line 93)
7. [7] `toString()` on node property (Scenario, line 105)
8. [8] `toString()` should accept potentially correct types 1 (Scenario, line 123)
9. [9] `toString()` should accept potentially correct types 2 (Scenario, line 137)
10. [10] Fail `toString()` on invalid types #Example: <exampleName> (Scenario Outline, line 151)

#### `official/expressions/typeConversion/TypeConversion5.feature`

**Feature:** TypeConversion5 - To List

**Scenarios:** 0

#### `official/expressions/typeConversion/TypeConversion6.feature`

**Feature:** TypeConversion6 - To Map

**Scenarios:** 0

### legacy

**2 feature files, 11 scenarios**

#### `Aggregation1_BasicAggregation.feature`

**Feature:** Aggregation1 - Basic aggregation functions

**Scenarios:** 6

1. Count all rows with COUNT(*) (Scenario, line 5)
2. Count non-null values with COUNT(expr) (Scenario, line 24)
3. Sum numeric values with SUM (Scenario, line 43)
4. Average numeric values with AVG (Scenario, line 62)
5. Find minimum and maximum with MIN and MAX (Scenario, line 81)
6. Group by property and count (Scenario, line 100)

#### `Match1_SimpleMatches.feature`

**Feature:** Match1 - Basic MATCH patterns

**Scenarios:** 5

1. Match all nodes (Scenario, line 5)
2. Match nodes with a specific label (Scenario, line 26)
3. Match nodes with WHERE clause on property (Scenario, line 46)
4. Match with LIMIT clause (Scenario, line 66)
5. Match with SKIP clause (Scenario, line 84)

### useCases

**2 feature files, 30 scenarios**

#### `official/useCases/countingSubgraphMatches/CountingSubgraphMatches1.feature`

**Feature:** CountingSubgraphMatches1 - Matching subgraph patterns and count the number of matches

**Scenarios:** 11

1. [1] Undirected match in self-relationship graph, count (Scenario, line 33)
2. [2] Undirected match of self-relationship in self-relationship graph, count (Scenario, line 49)
3. [3] Undirected match on simple relationship graph, count (Scenario, line 65)
4. [4] Directed match on self-relationship graph, count (Scenario, line 81)
5. [5] Directed match of self-relationship on self-relationship graph, count (Scenario, line 97)
6. [6] Counting undirected self-relationships in self-relationship graph (Scenario, line 113)
7. [7] Counting distinct undirected self-relationships in self-relationship graph (Scenario, line 129)
8. [8] Directed match of a simple relationship, count (Scenario, line 145)
9. [9] Counting directed self-relationships (Scenario, line 161)
10. [10] Mixing directed and undirected pattern parts with self-relationship, count (Scenario, line 178)
11. [11] Mixing directed and undirected pattern parts with self-relationship, undirected count (Scenario, line 196)

#### `official/useCases/triadicSelection/TriadicSelection1.feature`

**Feature:** TriadicSelection1 - Query three related nodes on binary-tree graphs

**Scenarios:** 19

1. [1] Handling triadic friend of a friend (Scenario, line 33)
2. [2] Handling triadic friend of a friend that is not a friend (Scenario, line 50)
3. [3] Handling triadic friend of a friend that is not a friend with different relationship type (Scenario, line 68)
4. [4] Handling triadic friend of a friend that is not a friend with superset of relationship type (Scenario, line 86)
5. [5] Handling triadic friend of a friend that is not a friend with implicit subset of relationship type (Scenario, line 103)
6. [6] Handling triadic friend of a friend that is not a friend with explicit subset of relationship type (Scenario, line 126)
7. [7] Handling triadic friend of a friend that is not a friend with same labels (Scenario, line 149)
8. [8] Handling triadic friend of a friend that is not a friend with different labels (Scenario, line 165)
9. [9] Handling triadic friend of a friend that is not a friend with implicit subset of labels (Scenario, line 180)
10. [10] Handling triadic friend of a friend that is not a friend with implicit superset of labels (Scenario, line 196)
11. [11] Handling triadic friend of a friend that is a friend (Scenario, line 214)
12. [12] Handling triadic friend of a friend that is a friend with different relationship type (Scenario, line 228)
13. [13] Handling triadic friend of a friend that is a friend with superset of relationship type (Scenario, line 242)
14. [14] Handling triadic friend of a friend that is a friend with implicit subset of relationship type (Scenario, line 257)
15. [15] Handling triadic friend of a friend that is a friend with explicit subset of relationship type (Scenario, line 272)
16. [16] Handling triadic friend of a friend that is a friend with same labels (Scenario, line 287)
17. [17] Handling triadic friend of a friend that is a friend with different labels (Scenario, line 301)
18. [18] Handling triadic friend of a friend that is a friend with implicit subset of labels (Scenario, line 314)
19. [19] Handling triadic friend of a friend that is a friend with implicit superset of labels (Scenario, line 328)
