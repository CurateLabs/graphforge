# OpenCypher Operators and Expressions

This document provides a comprehensive reference for all operators and expressions in the OpenCypher specification. Operators are the building blocks for creating predicates, calculations, and transformations in Cypher queries.

**OpenCypher Reference**: [https://opencypher.org/](https://opencypher.org/)

**Neo4j Cypher Manual**: [https://neo4j.com/docs/cypher-manual/current/syntax/operators/](https://neo4j.com/docs/cypher-manual/current/syntax/operators/)

---

## Table of Contents

- [Operator Overview](#operator-overview)
- [Operator Precedence](#operator-precedence)
- [Comparison Operators](#comparison-operators)
- [Logical Operators](#logical-operators)
- [Arithmetic Operators](#arithmetic-operators)
- [String Operators](#string-operators)
- [List Operators](#list-operators)
- [Property Access Operator](#property-access-operator)
- [NULL Handling](#null-handling)
- [Type Coercion](#type-coercion)

---

## Operator Overview

OpenCypher supports multiple categories of operators for different data types and operations:

| Category | Operators | Purpose |
|----------|-----------|---------|
| **Comparison** | `=`, `<>`, `<`, `>`, `<=`, `>=` | Compare values for equality and ordering |
| **NULL Testing** | `IS NULL`, `IS NOT NULL` | Test for NULL values |
| **Logical** | `AND`, `OR`, `NOT`, `XOR` | Combine boolean expressions |
| **Arithmetic** | `+`, `-`, `*`, `/`, `%`, `^` | Perform numeric calculations |
| **String** | `+`, `STARTS WITH`, `ENDS WITH`, `CONTAINS`, `=~` | String operations and pattern matching |
| **List** | `IN`, `[]`, `+`, `[..]` | List membership, indexing, concatenation, slicing |
| **Property** | `.` | Access properties on nodes, relationships, and maps |
| **Aliasing** | `AS` | Rename columns in RETURN and WITH clauses |
| **Deduplication** | `DISTINCT` | Remove duplicate values |

---

## Operator Precedence

Operators are evaluated in the following order (highest to lowest precedence):

| Precedence | Operator(s) | Description | Associativity |
|------------|-------------|-------------|---------------|
| 1 | `.` | Property access | Left to right |
| 2 | `[]` | Subscript (list/map access) | Left to right |
| 3 | `-` (unary) | Unary minus (negation) | Right to left |
| 4 | `^` | Exponentiation (power) | Right to left |
| 5 | `*`, `/`, `%` | Multiplication, division, modulo | Left to right |
| 6 | `+`, `-` | Addition, subtraction | Left to right |
| 7 | `=`, `<>`, `<`, `>`, `<=`, `>=` | Comparison operators | Left to right |
| 8 | `IS NULL`, `IS NOT NULL` | NULL testing | N/A |
| 9 | `STARTS WITH`, `ENDS WITH`, `CONTAINS`, `=~` | String matching | Left to right |
| 10 | `IN` | List membership | N/A |
| 11 | `NOT` | Logical negation | Right to left |
| 12 | `AND` | Logical conjunction | Left to right |
| 13 | `OR`, `XOR` | Logical disjunction | Left to right |

**Examples**:

```cypher
// 2 + 3 * 4 = 2 + 12 = 14 (multiplication before addition)
RETURN 2 + 3 * 4 AS result

// NOT x.active AND x.age > 30 = (NOT x.active) AND (x.age > 30)
MATCH (x:Person)
WHERE NOT x.active AND x.age > 30
RETURN x

// Use parentheses to override precedence
RETURN 2 * (3 + 4) AS result  // = 2 * 7 = 14
```

---

## Comparison Operators

Comparison operators compare two values and return a boolean result (true, false, or NULL).

### Equality: `=`

**Purpose**: Test if two values are equal.

**Syntax**: `expression1 = expression2`

**Returns**: `true` if equal, `false` if not equal, `NULL` if either operand is NULL

**Examples**:

```cypher
// Simple equality
MATCH (p:Person)
WHERE p.age = 30
RETURN p.name

// Comparing properties
MATCH (p1:Person), (p2:Person)
WHERE p1.age = p2.age AND p1.name <> p2.name
RETURN p1.name, p2.name

// In RETURN clause
MATCH (p:Person)
RETURN p.name, p.age = 30 AS is_thirty
```

**NULL Handling**:
```cypher
RETURN 5 = 5        // true
RETURN 5 = 10       // false
RETURN 5 = NULL     // NULL
RETURN NULL = NULL  // NULL (not true!)
```

**Type Compatibility**: Values must be comparable types. Comparing incompatible types (e.g., string to number) returns `false`.

---

### Inequality: `<>`

**Purpose**: Test if two values are not equal.

**Syntax**: `expression1 <> expression2`

**Returns**: `true` if not equal, `false` if equal, `NULL` if either operand is NULL

**Examples**:

```cypher
// Filter by inequality
MATCH (p:Person)
WHERE p.status <> 'inactive'
RETURN p.name

// Multiple inequalities
MATCH (p:Person)
WHERE p.name <> 'Alice' AND p.name <> 'Bob'
RETURN p.name
```

**NULL Handling**:
```cypher
RETURN 5 <> 10       // true
RETURN 5 <> 5        // false
RETURN 5 <> NULL     // NULL
RETURN NULL <> NULL  // NULL (not true!)
```

---

### Less Than: `<`

**Purpose**: Test if left value is less than right value.

**Syntax**: `expression1 < expression2`

**Returns**: `true` if less than, `false` otherwise, `NULL` if either operand is NULL

**Examples**:

```cypher
// Numeric comparison
MATCH (p:Person)
WHERE p.age < 30
RETURN p.name, p.age

// String comparison (lexicographic)
MATCH (p:Person)
WHERE p.name < 'M'
RETURN p.name
ORDER BY p.name

// Date comparison
MATCH (e:Event)
WHERE e.date < date('2024-01-01')
RETURN e.name, e.date
```

**Type-Specific Ordering**:
- Numbers: Natural numeric ordering
- Strings: Lexicographic (dictionary) ordering
- Dates/Times: Chronological ordering
- Booleans: `false < true`

---

### Greater Than: `>`

**Purpose**: Test if left value is greater than right value.

**Syntax**: `expression1 > expression2`

**Returns**: `true` if greater than, `false` otherwise, `NULL` if either operand is NULL

**Examples**:

```cypher
// Find senior employees
MATCH (p:Person)
WHERE p.age > 50
RETURN p.name, p.age
ORDER BY p.age DESC

// Compare aggregations
MATCH (p:Person)-[:PURCHASED]->(product)
WITH p, count(product) AS purchase_count
WHERE purchase_count > 10
RETURN p.name, purchase_count
```

---

### Less Than or Equal: `<=`

**Purpose**: Test if left value is less than or equal to right value.

**Syntax**: `expression1 <= expression2`

**Returns**: `true` if less than or equal, `false` otherwise, `NULL` if either operand is NULL

**Examples**:

```cypher
// Include boundary
MATCH (p:Person)
WHERE p.age <= 30
RETURN p.name

// Range query (both boundaries)
MATCH (e:Event)
WHERE e.year >= 2020 AND e.year <= 2023
RETURN e.name, e.year
```

---

### Greater Than or Equal: `>=`

**Purpose**: Test if left value is greater than or equal to right value.

**Syntax**: `expression1 >= expression2`

**Returns**: `true` if greater than or equal, `false` otherwise, `NULL` if either operand is NULL

**Examples**:

```cypher
// Minimum threshold
MATCH (product:Product)
WHERE product.rating >= 4.0
RETURN product.name, product.rating

// Age range
MATCH (p:Person)
WHERE p.age >= 18 AND p.age < 65
RETURN p.name AS working_age
```

---

### NULL Testing: `IS NULL`, `IS NOT NULL`

**Purpose**: Test whether a value is NULL or not NULL. These are the ONLY operators that can reliably test for NULL.

**Syntax**:
- `expression IS NULL`
- `expression IS NOT NULL`

**Returns**: Always returns `true` or `false` (never NULL)

**Examples**:

```cypher
// Find nodes with missing properties
MATCH (p:Person)
WHERE p.email IS NULL
RETURN p.name

// Find nodes with required properties
MATCH (p:Person)
WHERE p.email IS NOT NULL
RETURN p.name, p.email

// Check optional match results
MATCH (p:Person)
OPTIONAL MATCH (p)-[:WORKS_AT]->(company:Company)
WHERE company IS NOT NULL
RETURN p.name, company.name

// Complex NULL checks
MATCH (p:Person)
WHERE p.age IS NOT NULL AND p.age > 30
RETURN p.name, p.age
```

**Important**: Use `IS NULL` / `IS NOT NULL`, not `= NULL` or `<> NULL`:

```cypher
// WRONG: Always returns NULL, filters out everything
WHERE p.email = NULL     // Never matches
WHERE p.email <> NULL    // Never matches

// CORRECT: Returns true/false
WHERE p.email IS NULL
WHERE p.email IS NOT NULL
```

---

## Logical Operators

Logical operators combine boolean expressions using three-valued logic (true, false, NULL).

### AND

**Purpose**: Logical conjunction - true only if both operands are true.

**Syntax**: `expression1 AND expression2`

**Returns**: Three-valued logic result

**Truth Table**:

| Left | Right | Result |
|------|-------|--------|
| true | true | true |
| true | false | false |
| true | NULL | NULL |
| false | true | false |
| false | false | false |
| false | NULL | **false** (short-circuit) |
| NULL | true | NULL |
| NULL | false | **false** |
| NULL | NULL | NULL |

**Examples**:

```cypher
// Multiple conditions
MATCH (p:Person)
WHERE p.age > 18 AND p.age < 65
RETURN p.name

// Chaining AND operators
MATCH (p:Person)
WHERE p.country = 'USA'
  AND p.age >= 18
  AND p.active = true
RETURN p.name

// With NULL handling
MATCH (p:Person)
WHERE p.verified = true AND p.email IS NOT NULL
RETURN p.name, p.email
```

**Short-Circuit Evaluation**: If the left operand is `false`, the result is always `false` without evaluating the right operand.

---

### OR

**Purpose**: Logical disjunction - true if either operand is true.

**Syntax**: `expression1 OR expression2`

**Returns**: Three-valued logic result

**Truth Table**:

| Left | Right | Result |
|------|-------|--------|
| true | true | true |
| true | false | true |
| true | NULL | **true** (short-circuit) |
| false | true | true |
| false | false | false |
| false | NULL | NULL |
| NULL | true | **true** |
| NULL | false | NULL |
| NULL | NULL | NULL |

**Examples**:

```cypher
// Alternative conditions
MATCH (p:Person)
WHERE p.status = 'active' OR p.status = 'pending'
RETURN p.name

// Multiple alternatives
MATCH (p:Person)
WHERE p.country = 'USA'
   OR p.country = 'Canada'
   OR p.country = 'Mexico'
RETURN p.name, p.country

// Combine with AND (AND has higher precedence)
MATCH (p:Person)
WHERE (p.age < 18 OR p.age > 65) AND p.active = true
RETURN p.name
```

**Short-Circuit Evaluation**: If the left operand is `true`, the result is always `true` without evaluating the right operand.

---

### NOT

**Purpose**: Logical negation - inverts a boolean value.

**Syntax**: `NOT expression`

**Returns**: Three-valued logic result

**Truth Table**:

| Operand | Result |
|---------|--------|
| true | false |
| false | true |
| NULL | NULL |

**Examples**:

```cypher
// Negate condition
MATCH (p:Person)
WHERE NOT p.active
RETURN p.name

// Negate comparison
MATCH (p:Person)
WHERE NOT p.age > 30
RETURN p.name

// Double negation
RETURN NOT NOT true   // true

// With NULL
MATCH (p:Person)
WHERE NOT p.verified  // NULL values filtered out
RETURN p.name
```

**Common Pattern**: Combine NOT with parentheses for complex logic:

```cypher
// NOT with AND/OR
MATCH (p:Person)
WHERE NOT (p.age < 18 OR p.age > 65)
RETURN p.name  // Ages 18-65

// Equivalent to:
MATCH (p:Person)
WHERE p.age >= 18 AND p.age <= 65
RETURN p.name
```

---

### XOR

**Purpose**: Exclusive OR - true if exactly one operand is true (not both).

**Syntax**: `expression1 XOR expression2`

**Returns**: Three-valued logic result

**Truth Table**:

| Left | Right | Result |
|------|-------|--------|
| true | true | false |
| true | false | true |
| true | NULL | NULL |
| false | true | true |
| false | false | false |
| false | NULL | NULL |
| NULL | true | NULL |
| NULL | false | NULL |
| NULL | NULL | NULL |

**Examples**:

```cypher
// Exactly one condition must be true
MATCH (p:Person)
WHERE p.has_passport XOR p.has_drivers_license
RETURN p.name

// Use in validation
MATCH (account:Account)
WHERE account.email_verified XOR account.phone_verified
RETURN account.id AS needs_second_verification
```

**Note**: XOR is less commonly used than AND/OR but useful for exclusivity checks.

---

## Arithmetic Operators

Arithmetic operators perform mathematical calculations on numeric values.

### Addition: `+`

**Purpose**: Add two numbers or concatenate strings/lists.

**Syntax**: `expression1 + expression2`

**Returns**:
- Numbers: Sum (int + int = int, any float = float)
- Strings: Concatenated string
- Lists: Concatenated list

**Examples**:

```cypher
// Numeric addition
RETURN 5 + 3 AS result           // 8

// Float coercion
RETURN 5 + 3.5 AS result         // 8.5 (float)

// String concatenation
RETURN 'Hello' + ' ' + 'World'   // "Hello World"

// Mixed string and number
RETURN 'Age: ' + 30              // "Age: 30"

// In WHERE clause
MATCH (p:Person)
WHERE p.age + 5 > 40
RETURN p.name, p.age

// With properties
MATCH (order:Order)
RETURN order.id,
       order.subtotal + order.tax AS total
```

**NULL Handling**: Any NULL operand returns NULL.

**Type Coercion**: If either operand is float, result is float. If either is string, result is string (concatenation).

---

### Subtraction: `-`

**Purpose**: Subtract two numbers or durations.

**Syntax**: `expression1 - expression2`

**Returns**: Difference (int or float)

**Examples**:

```cypher
// Basic subtraction
RETURN 10 - 3 AS result          // 7

// With properties
MATCH (p:Person)
RETURN p.name,
       2024 - p.birth_year AS age

// Negative result
RETURN 5 - 10 AS result          // -5

// Date arithmetic
MATCH (event:Event)
RETURN event.name,
       date() - event.date AS days_since
```

**NULL Handling**: Any NULL operand returns NULL.

---

### Unary Minus: `-` (Negation)

**Purpose**: Negate a numeric value.

**Syntax**: `-expression`

**Returns**: Negated value (int or float)

**Examples**:

```cypher
// Simple negation
RETURN -5 AS result              // -5

// Negate property
MATCH (p:Person)
RETURN p.name, -p.debt AS credit

// In calculations
RETURN -2 * 3 AS result          // -6

// Double negation
RETURN --5 AS result             // 5
```

**NULL Handling**: `-NULL` returns `NULL`.

---

### Multiplication: `*`

**Purpose**: Multiply two numbers.

**Syntax**: `expression1 * expression2`

**Returns**: Product (int or float)

**Examples**:

```cypher
// Basic multiplication
RETURN 4 * 5 AS result           // 20

// Calculate area
MATCH (rect:Rectangle)
RETURN rect.id,
       rect.width * rect.height AS area

// With float
RETURN 2.5 * 4 AS result         // 10.0 (float)

// Operator precedence
RETURN 2 + 3 * 4 AS result       // 14 (not 20)
```

**NULL Handling**: Any NULL operand returns NULL.

**Type Coercion**: If either operand is float, result is float.

---

### Division: `/`

**Purpose**: Divide two numbers.

**Syntax**: `expression1 / expression2`

**Returns**: Quotient (always returns float)

**Examples**:

```cypher
// Division (always returns float)
RETURN 10 / 2 AS result          // 5.0 (float)

// Non-integer result
RETURN 7 / 2 AS result           // 3.5

// Division by zero returns NULL
RETURN 10 / 0 AS result          // NULL

// Calculate rate
MATCH (employee:Employee)
RETURN employee.name,
       employee.salary / 12 AS monthly_salary
```

**NULL Handling**: Any NULL operand or division by zero returns NULL.

**Important**: Division ALWAYS returns float, even for integer operands.

---

### Modulo: `%`

**Purpose**: Calculate remainder after division.

**Syntax**: `expression1 % expression2`

**Returns**: Remainder (int or float)

**Examples**:

```cypher
// Basic modulo
RETURN 10 % 3 AS result          // 1

// Check even/odd
MATCH (n:Number)
WHERE n.value % 2 = 0
RETURN n.value AS even_numbers

// Modulo by zero returns NULL
RETURN 10 % 0 AS result          // NULL

// With floats
RETURN 10.5 % 3.0 AS result      // 1.5
```

**NULL Handling**: Any NULL operand or modulo by zero returns NULL.

---

### Exponentiation: `^`

**Purpose**: Raise a number to a power.

**Syntax**: `expression1 ^ expression2`

**Returns**: Result of exponentiation (float)

**Examples**:

```cypher
// Basic exponentiation
RETURN 2 ^ 3 AS result           // 8.0

// Square
RETURN 5 ^ 2 AS result           // 25.0

// Square root (fractional exponent)
RETURN 16 ^ 0.5 AS result        // 4.0

// Negative exponent (reciprocal)
RETURN 2 ^ -1 AS result          // 0.5

// Calculate compound interest
MATCH (account:Account)
RETURN account.id,
       account.principal * (1 + account.rate) ^ account.years AS balance
```

**NULL Handling**: Any NULL operand returns NULL.

**Note**: Right-associative: `2 ^ 3 ^ 2` = `2 ^ (3 ^ 2)` = `2 ^ 9` = 512

---

## String Operators

String operators perform text manipulation and pattern matching.

### String Concatenation: `+`

**Purpose**: Concatenate two or more strings.

**Syntax**: `string1 + string2`

**Returns**: Combined string

**Examples**:

```cypher
// Basic concatenation
RETURN 'Hello' + ' ' + 'World' AS greeting

// Concatenate properties
MATCH (p:Person)
RETURN p.first_name + ' ' + p.last_name AS full_name

// Mix strings and numbers (converts to string)
MATCH (p:Person)
RETURN p.name + ' is ' + p.age + ' years old' AS description

// Build URLs
MATCH (user:User)
RETURN 'https://example.com/users/' + user.id AS profile_url
```

**NULL Handling**: Any NULL operand returns NULL.

**Type Conversion**: Non-string values are automatically converted to strings.

---

### STARTS WITH

**Purpose**: Test if a string starts with a specific prefix.

**Syntax**: `string STARTS WITH prefix`

**Returns**: `true` if string starts with prefix, `false` otherwise, `NULL` if either operand is NULL

**Examples**:

```cypher
// Simple prefix match
MATCH (p:Person)
WHERE p.name STARTS WITH 'A'
RETURN p.name

// Case-sensitive matching
MATCH (p:Person)
WHERE p.email STARTS WITH 'admin'
RETURN p.name, p.email

// With property
MATCH (product:Product)
WHERE product.sku STARTS WITH 'ELEC'
RETURN product.name

// Empty string always matches
RETURN 'Hello' STARTS WITH ''    // true
```

**NULL Handling**:
```cypher
RETURN 'Hello' STARTS WITH NULL  // NULL
RETURN NULL STARTS WITH 'He'     // NULL
```

**Case Sensitivity**: STARTS WITH is case-sensitive. Use `toLower()` or `toUpper()` for case-insensitive matching:

```cypher
MATCH (p:Person)
WHERE toLower(p.name) STARTS WITH 'a'
RETURN p.name
```

---

### ENDS WITH

**Purpose**: Test if a string ends with a specific suffix.

**Syntax**: `string ENDS WITH suffix`

**Returns**: `true` if string ends with suffix, `false` otherwise, `NULL` if either operand is NULL

**Examples**:

```cypher
// Email domain filtering
MATCH (p:Person)
WHERE p.email ENDS WITH '@company.com'
RETURN p.name, p.email

// File extension
MATCH (file:File)
WHERE file.name ENDS WITH '.pdf'
RETURN file.name

// Pattern matching
MATCH (product:Product)
WHERE product.sku ENDS WITH '-XL'
RETURN product.name AS extra_large_products

// Empty string always matches
RETURN 'Hello' ENDS WITH ''      // true
```

**NULL Handling**:
```cypher
RETURN 'Hello' ENDS WITH NULL    // NULL
RETURN NULL ENDS WITH 'lo'       // NULL
```

**Case Sensitivity**: Case-sensitive (use `toLower()` or `toUpper()` for case-insensitive matching).

---

### CONTAINS

**Purpose**: Test if a string contains a substring.

**Syntax**: `string CONTAINS substring`

**Returns**: `true` if string contains substring, `false` otherwise, `NULL` if either operand is NULL

**Examples**:

```cypher
// Substring search
MATCH (p:Person)
WHERE p.name CONTAINS 'son'
RETURN p.name  // Johnson, Anderson, etc.

// Search in descriptions
MATCH (product:Product)
WHERE product.description CONTAINS 'waterproof'
RETURN product.name, product.description

// Multi-word search
MATCH (article:Article)
WHERE article.text CONTAINS 'machine learning'
RETURN article.title

// Empty string always matches
RETURN 'Hello' CONTAINS ''       // true
```

**NULL Handling**:
```cypher
RETURN 'Hello' CONTAINS NULL     // NULL
RETURN NULL CONTAINS 'He'        // NULL
```

**Case Sensitivity**: Case-sensitive (use `toLower()` or `toUpper()` for case-insensitive matching).

**Performance**: For large text searches, consider full-text indexes instead of CONTAINS.

---

### Regular Expression Match: `=~`

**Purpose**: Test if a string matches a regular expression pattern.

**Syntax**: `string =~ regex_pattern`

**Returns**: `true` if string matches pattern, `false` otherwise, `NULL` if either operand is NULL

**Examples**:

```cypher
// Email validation
MATCH (p:Person)
WHERE p.email =~ '.*@.*\\.com'
RETURN p.name, p.email

// Phone number format
MATCH (contact:Contact)
WHERE contact.phone =~ '\\d{3}-\\d{3}-\\d{4}'
RETURN contact.name, contact.phone

// Case-insensitive with (?i)
MATCH (p:Person)
WHERE p.name =~ '(?i)john.*'
RETURN p.name  // John, johnny, JOHNSON, etc.

// Extract patterns
MATCH (product:Product)
WHERE product.sku =~ 'PROD-[0-9]{6}'
RETURN product.sku
```

**Regex Syntax**: Uses Java regular expression syntax (java.util.regex.Pattern).

**Common Patterns**:
- `.` - Any character
- `*` - Zero or more
- `+` - One or more
- `?` - Zero or one
- `\d` - Digit
- `\w` - Word character
- `[abc]` - Character class
- `^` - Start of string
- `$` - End of string
- `(?i)` - Case-insensitive flag

**NULL Handling**:
```cypher
RETURN 'test' =~ NULL            // NULL
RETURN NULL =~ '.*'              // NULL
```

**Escaping**: Use double backslashes in Cypher strings: `\\d` for digit, `\\.` for literal dot.

---

## List Operators

List operators work with collections of values.

### List Membership: `IN`

**Purpose**: Test if a value is in a list.

**Syntax**: `value IN list`

**Returns**: `true` if value is in list, `false` if not, `NULL` in certain cases

**Examples**:

```cypher
// Simple membership
MATCH (p:Person)
WHERE p.country IN ['USA', 'Canada', 'Mexico']
RETURN p.name

// With property
MATCH (product:Product)
WHERE product.category IN ['Electronics', 'Computers']
RETURN product.name

// Check against dynamic list
MATCH (p:Person)-[:LIKES]->(hobby:Hobby)
WITH p, collect(hobby.name) AS hobbies
WHERE 'Reading' IN hobbies
RETURN p.name

// Numeric values
MATCH (n:Number)
WHERE n.value IN [1, 2, 3, 5, 8, 13]
RETURN n.value AS fibonacci_numbers
```

**NULL Handling**:
```cypher
RETURN 5 IN [1, 2, 3]            // false
RETURN 2 IN [1, 2, 3]            // true
RETURN NULL IN [1, 2, 3]         // NULL
RETURN 5 IN []                   // false
RETURN NULL IN []                // false (special case)
RETURN 5 IN NULL                 // NULL
RETURN 5 IN [1, NULL, 3]         // NULL (if not found)
```

**Three-Valued Logic**: If the value is not found but the list contains NULL, returns NULL (might be equal to NULL, unknown).

---

### List Indexing: `[]`

**Purpose**: Access a single element from a list by index.

**Syntax**: `list[index]`

**Returns**: Element at index, or NULL if out of bounds

**Examples**:

```cypher
// Positive indexing (0-based)
RETURN [1, 2, 3, 4, 5][0] AS first        // 1
RETURN [1, 2, 3, 4, 5][2] AS third        // 3

// Negative indexing (from end)
RETURN [1, 2, 3, 4, 5][-1] AS last        // 5
RETURN [1, 2, 3, 4, 5][-2] AS second_last // 4

// Out of bounds returns NULL
RETURN [1, 2, 3][10] AS result            // NULL
RETURN [1, 2, 3][-10] AS result           // NULL

// Access property list
MATCH (p:Person)
RETURN p.name, p.hobbies[0] AS first_hobby

// With path functions
MATCH path = (a:Person)-[:KNOWS*]->(b:Person)
RETURN nodes(path)[0] AS start_node,
       relationships(path)[-1] AS last_relationship
```

**NULL Handling**:
```cypher
RETURN NULL[0]                            // NULL
RETURN [1, 2, 3][NULL]                    // NULL
RETURN [][0]                              // NULL
```

**Indexing Rules**:
- Positive indices: 0 = first element, 1 = second, etc.
- Negative indices: -1 = last element, -2 = second-to-last, etc.
- Out of bounds: Returns NULL (no error)

---

### List Slicing: `[start..end]`

**Purpose**: Extract a sub-list from a list.

**Syntax**:
- `list[start..end]` - From start (inclusive) to end (exclusive)
- `list[start..]` - From start to end of list
- `list[..end]` - From beginning to end
- `list[..]` - Entire list (copy)

**Returns**: Sub-list (CypherList)

**Examples**:

```cypher
// Range slice
RETURN [1, 2, 3, 4, 5][1..3] AS result    // [2, 3]

// From start
RETURN [1, 2, 3, 4, 5][2..] AS result     // [3, 4, 5]

// To end
RETURN [1, 2, 3, 4, 5][..3] AS result     // [1, 2, 3]

// Full slice (copy)
RETURN [1, 2, 3][..] AS result            // [1, 2, 3]

// Negative indices
RETURN [1, 2, 3, 4, 5][-3..] AS result    // [3, 4, 5]
RETURN [1, 2, 3, 4, 5][..-2] AS result    // [1, 2, 3]

// With path functions
MATCH path = (a)-[:KNOWS*]-(b)
RETURN nodes(path)[1..-1] AS middle_nodes
```

**NULL Handling**:
```cypher
RETURN NULL[1..3]                         // NULL
RETURN [1, 2, 3][NULL..2]                 // NULL
RETURN [][1..3]                           // []
```

**Slicing Rules**:
- Start is inclusive, end is exclusive: `[1..3]` means indices 1 and 2
- Negative indices count from end: -1 is last element
- Out-of-bounds indices are clamped to valid range
- Empty list if start >= end

---

### List Concatenation: `+`

**Purpose**: Concatenate two lists.

**Syntax**: `list1 + list2`

**Returns**: Combined list

**Examples**:

```cypher
// Basic concatenation
RETURN [1, 2, 3] + [4, 5, 6] AS result    // [1, 2, 3, 4, 5, 6]

// Combine collections
MATCH (p:Person)
WITH collect(p.hobby) AS hobbies
RETURN hobbies + ['Reading', 'Writing'] AS all_hobbies

// Add single element (wrap in list)
RETURN [1, 2, 3] + [4] AS result          // [1, 2, 3, 4]

// Flatten nested lists
RETURN [1, 2] + [3, 4] + [5, 6] AS result // [1, 2, 3, 4, 5, 6]
```

**NULL Handling**:
```cypher
RETURN [1, 2] + NULL                      // NULL
RETURN NULL + [3, 4]                      // NULL
```

**Type Compatibility**: Both operands must be lists. To add a single element, wrap it in a list: `list + [element]`.

---

## Property Access Operator

### Dot Notation: `.`

**Purpose**: Access properties on nodes, relationships, maps, and dates.

**Syntax**: `object.property_name`

**Returns**: Property value, or NULL if property doesn't exist

**Examples**:

```cypher
// Node property access
MATCH (p:Person)
RETURN p.name, p.age, p.email

// Relationship property access
MATCH (p1:Person)-[r:KNOWS]->(p2:Person)
RETURN p1.name, r.since, p2.name

// Map property access
WITH {name: 'Alice', age: 30} AS person
RETURN person.name, person.age

// Nested property access
MATCH (p:Person)
RETURN p.address.city, p.address.zip

// Date/time component access
MATCH (e:Event)
RETURN e.timestamp.year,
       e.timestamp.month,
       e.timestamp.day

// In WHERE clause
MATCH (p:Person)
WHERE p.age > 30 AND p.country = 'USA'
RETURN p.name
```

**NULL Handling**:
```cypher
// Missing property returns NULL
MATCH (p:Person)
RETURN p.middle_name                      // NULL if not set

// Property access on NULL returns NULL
WITH NULL AS obj
RETURN obj.property                       // NULL
```

**Dynamic Property Access**: Use bracket notation for computed property names:

```cypher
MATCH (p:Person)
WITH p, 'name' AS prop
RETURN p[prop] AS value                   // Same as p.name
```

---

## NULL Handling

OpenCypher uses three-valued logic: `true`, `false`, and `NULL`. Understanding NULL propagation is critical for correct query behavior.

### General NULL Rules

1. **Comparison with NULL returns NULL** (not false)
2. **Arithmetic with NULL returns NULL**
3. **Most functions with NULL input return NULL**
4. **WHERE filters out NULL** (only true passes)
5. **Use IS NULL / IS NOT NULL to test for NULL**

### NULL Propagation by Operator

| Operation | NULL Behavior | Example |
|-----------|---------------|---------|
| `x = NULL` | NULL (not false!) | `5 = NULL` → NULL |
| `x <> NULL` | NULL (not true!) | `5 <> NULL` → NULL |
| `x > NULL` | NULL | `5 > NULL` → NULL |
| `x + NULL` | NULL | `5 + NULL` → NULL |
| `x - NULL` | NULL | `5 - NULL` → NULL |
| `x * NULL` | NULL | `5 * NULL` → NULL |
| `x / NULL` | NULL | `5 / NULL` → NULL |
| `x % NULL` | NULL | `5 % NULL` → NULL |
| `NOT NULL` | NULL | `NOT NULL` → NULL |
| `true AND NULL` | NULL | - |
| `false AND NULL` | **false** (short-circuit) | - |
| `true OR NULL` | **true** (short-circuit) | - |
| `false OR NULL` | NULL | - |
| `x IN NULL` | NULL | `5 IN NULL` → NULL |
| `NULL IN [...]` | NULL | `NULL IN [1,2,3]` → NULL |
| `x IS NULL` | true/false (never NULL) | - |
| `x IS NOT NULL` | true/false (never NULL) | - |

### NULL in WHERE Clauses

WHERE clauses filter out both `false` AND `NULL`:

```cypher
// Only rows where condition is TRUE pass through
MATCH (p:Person)
WHERE p.age > 30        // false and NULL both filtered out
RETURN p.name

// Explicit NULL handling
MATCH (p:Person)
WHERE p.age > 30 OR p.age IS NULL
RETURN p.name
```

### NULL in Aggregations

Aggregation functions typically ignore NULL values:

```cypher
// count() ignores NULLs
MATCH (p:Person)
RETURN count(p.email) AS has_email        // Only non-NULL emails

// avg() ignores NULLs
MATCH (p:Person)
RETURN avg(p.age) AS avg_age              // Average of non-NULL ages

// collect() includes NULLs
MATCH (p:Person)
RETURN collect(p.email) AS emails         // May include NULLs
```

### COALESCE for NULL Handling

Use `coalesce()` to provide default values for NULL:

```cypher
MATCH (p:Person)
RETURN p.name,
       coalesce(p.email, 'no-email@example.com') AS email,
       coalesce(p.age, 0) AS age
```

---

## Type Coercion

OpenCypher performs automatic type coercion in certain contexts.

### Numeric Type Coercion

**Rule**: If either operand is float, result is float.

```cypher
RETURN 5 + 3              // 8 (int)
RETURN 5 + 3.0            // 8.0 (float)
RETURN 5.0 + 3.0          // 8.0 (float)
RETURN 5 * 2              // 10 (int)
RETURN 5.0 * 2            // 10.0 (float)
```

**Exception**: Division always returns float.

```cypher
RETURN 10 / 2             // 5.0 (always float)
RETURN 7 / 2              // 3.5 (float)
```

### String Coercion

The `+` operator converts non-string operands to strings:

```cypher
RETURN 'Age: ' + 30                       // "Age: 30"
RETURN 'Price: $' + 19.99                 // "Price: $19.99"
RETURN 'Active: ' + true                  // "Active: true"
```

### Boolean Coercion

Comparison operators return booleans:

```cypher
RETURN 5 > 3              // true
RETURN 5 = 10             // false
```

### Explicit Type Conversion

Use conversion functions for explicit type conversion:

```cypher
// String to number
RETURN toInteger('42') AS int             // 42
RETURN toFloat('3.14') AS float           // 3.14

// Number to string
RETURN toString(42) AS str                // "42"

// String to boolean
RETURN toBoolean('true') AS bool          // true

// Any to boolean (for predicates)
RETURN toBoolean(5) AS bool               // true (non-zero)
RETURN toBoolean(0) AS bool               // false
RETURN toBoolean('') AS bool              // false (empty string)
```

---

## Special Operators

### AS (Aliasing)

**Purpose**: Rename columns in RETURN and WITH clauses.

**Syntax**: `expression AS alias`

**Examples**:

```cypher
// Simple alias
MATCH (p:Person)
RETURN p.name AS person_name

// Calculated columns
MATCH (p:Person)
RETURN p.first_name + ' ' + p.last_name AS full_name

// Required for aggregations without property
MATCH (p:Person)
RETURN count(p) AS person_count

// In WITH clause
MATCH (p:Person)-[:LIKES]->(hobby:Hobby)
WITH p, count(hobby) AS hobby_count
WHERE hobby_count > 3
RETURN p.name, hobby_count
```

**Rules**:
- Alias names follow identifier rules (letters, digits, underscores)
- Required for expressions without a natural column name
- Optional for simple property access

---

### DISTINCT (Deduplication)

**Purpose**: Remove duplicate values from results.

**Syntax**:
- `RETURN DISTINCT ...`
- `WITH DISTINCT ...`
- `count(DISTINCT ...)`
- `collect(DISTINCT ...)`

**Examples**:

```cypher
// Distinct rows
MATCH (p:Person)
RETURN DISTINCT p.country

// Distinct in WITH
MATCH (p:Person)-[:LIKES]->(hobby:Hobby)
WITH DISTINCT hobby
RETURN hobby.name

// Distinct count
MATCH (p:Person)-[:LIKES]->(hobby:Hobby)
RETURN count(DISTINCT hobby) AS unique_hobbies

// Distinct collect
MATCH (p:Person)-[:LIKES]->(hobby:Hobby)
RETURN collect(DISTINCT hobby.name) AS unique_hobby_names

// Multiple columns (distinct on combination)
MATCH (p:Person)
RETURN DISTINCT p.country, p.city
```

**Performance**: DISTINCT requires sorting/hashing, which can be expensive on large result sets.

---

## Operator Combinations and Examples

### Complex Filtering

```cypher
// Multiple operators in WHERE
MATCH (p:Person)
WHERE p.age >= 18
  AND p.age <= 65
  AND p.country IN ['USA', 'Canada']
  AND p.email IS NOT NULL
  AND p.name STARTS WITH 'A'
RETURN p.name, p.age, p.country
```

### Calculated Properties

```cypher
// Combine arithmetic and string operators
MATCH (product:Product)
RETURN product.name,
       '$' + toString(product.price) AS price_display,
       product.price * 0.9 AS sale_price,
       product.price * 1.08 AS price_with_tax
```

### Conditional Logic with CASE

```cypher
// CASE expression with operators
MATCH (p:Person)
RETURN p.name,
       p.age,
       CASE
         WHEN p.age < 18 THEN 'Minor'
         WHEN p.age >= 18 AND p.age < 65 THEN 'Adult'
         WHEN p.age >= 65 THEN 'Senior'
         ELSE 'Unknown'
       END AS age_group
```

### List Operations

```cypher
// Complex list operations
MATCH (p:Person)
WITH p, p.hobbies + ['Reading'] AS extended_hobbies
WHERE 'Sports' IN extended_hobbies
RETURN p.name,
       extended_hobbies[0] AS first_hobby,
       extended_hobbies[1..3] AS next_hobbies,
       size(extended_hobbies) AS hobby_count
```

---

## Summary

OpenCypher provides a rich set of operators for:

- **Comparison**: `=`, `<>`, `<`, `>`, `<=`, `>=`, `IS NULL`, `IS NOT NULL`
- **Logical**: `AND`, `OR`, `NOT`, `XOR`
- **Arithmetic**: `+`, `-`, `*`, `/`, `%`, `^`, unary `-`
- **String**: `+` (concat), `STARTS WITH`, `ENDS WITH`, `CONTAINS`, `=~` (regex)
- **List**: `IN`, `[]` (index), `[..]` (slice), `+` (concat)
- **Property**: `.` (dot notation)
- **Special**: `AS` (alias), `DISTINCT` (deduplication)

**Key Concepts**:
1. **Operator precedence** determines evaluation order
2. **Three-valued logic** (true/false/NULL) applies to comparisons and logic
3. **NULL propagation** affects most operators (except IS NULL/IS NOT NULL)
4. **Type coercion** handles mixed numeric types and string concatenation
5. **Short-circuit evaluation** optimizes AND/OR operators

---

## References

- **OpenCypher Official Site**: [https://opencypher.org/](https://opencypher.org/)
- **OpenCypher Specification**: [https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf](https://s3.amazonaws.com/artifacts.opencypher.org/openCypher9.pdf)
- **Neo4j Cypher Manual - Operators**: [https://neo4j.com/docs/cypher-manual/current/syntax/operators/](https://neo4j.com/docs/cypher-manual/current/syntax/operators/)
- **Neo4j Cypher Manual - Expressions**: [https://neo4j.com/docs/cypher-manual/current/syntax/expressions/](https://neo4j.com/docs/cypher-manual/current/syntax/expressions/)

---

**Document Version**: 1.0
**Last Updated**: 2026-02-16
**OpenCypher Version**: Based on OpenCypher 9 and Neo4j Cypher Manual (Current)
