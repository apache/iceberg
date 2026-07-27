---
title: "Expressions Spec"
---
<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->

# Iceberg Expressions

This document defines the structure and behavior of expressions for use in Iceberg specifications. The purpose is to define a common structure that enables simple expressions to be stored and exchanged.

Stored expressions are needed for use cases like data validations (`CHECK` constraints) and default values (for instance, `current_timestamp()`). Expressions are exchanged in use cases like server-side scan planning in the catalog protocol.


## Overview

The goal of this specification is to define a simple expression structure and avoid complexity.

To remain simple, the expressions that can be represented are deliberately constrained to value expressions (constants, references, and function calls) and predicates (comparisons that produce true or false).

This approach is intended to keep focus on the logical structure of expressions. Complexity is pushed to the functions that are called, which are a limited set of well-defined and portable functions (like Iceberg partition transforms) or [user-defined functions][udf-spec] that can use the full range of SQL capabilities. Multi-dialect UDFs are responsible for any SQL constructs that are specific to an engine, rather than importing and duplicating dialects in Iceberg expressions.

This is consistent with Iceberg's conservative approach in other specs. Expressions and predicates are an important part of Iceberg implementation APIs, but have been deliberately limited in specifications. For example, sort orders and partition fields are strictly limited to a small set of transforms over well-defined inputs (source field IDs). This spec is widening what can be expressed, but depends on function calls for complex tasks.

This specification covers the structure of Iceberg expressions and includes appendices that specify serialization as JSON and a set of portable functions defined by Iceberg specifications.

[udf-spec]: https://iceberg.apache.org/udf-spec


## Structure

Iceberg expressions have two types:

* **Value expressions** represent data values and transformations of values (function calls) that produce any Iceberg type
* **Predicates** represent comparisons of value expressions as well as combinations of predicates with boolean logic (and, or, not)


### Value expressions

A value expression is an expression that produces a typed value.

Value expressions can be one of three types: a constant value, a field reference, or a function applied to zero or more value expressions.


#### Constant values

A constant or literal is the simplest type of value expression that represents a specific typed value.


#### Field reference

A field reference represents the value of a specific field in a row. When an expression is evaluated on a row, it returns the value of the field.

Field references may be named references (unbound) or ID references (bound). ID references identify a field by field ID from a schema. Named references identify a field by name that must be resolved to an ID (bound to a schema) to access the field.

ID references are used for stored expressions, where the identity of the column is determined when the stored expression is created. For example, column constraints are tied to field IDs so that renaming a column does not invalidate the reference in its stored constraint.

Named references are used when the identity of the column is determined when the expression is evaluated. For example, query filters are resolved each time a query runs so server-side planning uses unbound named references.

The context in which an expression is used determines the type of references that are valid. Iceberg specifications should document whether ID references, named references, or both are allowed.


#### Apply function

An apply expression represents the result of a function applied to (or called on) zero or more values produced by child value expressions or predicates.

Functions are referenced using a catalog and a function identifier.

* The function identifier consists of 0 or more namespace names followed by the function name. At least one part, the function name, is required.
* Catalog is optional and is assumed to be the catalog in which the referencing object is stored if it is not present or is null

The catalog name identifies the catalog where the function definition can be loaded or is a reserved name that identifies a set of functions. As in the view and UDF specs, catalog names represent connection configurations that may differ across environments. Omitting catalog names is recommended to avoid depending on consistent environments. For example, if a table has a CHECK constraint that references a UDF without a catalog name (missing or null), the UDF should be loaded from the table's catalog.

The reserved names used to identify sets are:

* `sql_functions` is used for functions defined by the SQL standard
* `iceberg_functions` is used for functions defined in this specification

Engines may document and use a catalog name to identify their built-in functions that are not part of the SQL spec, like `spark_builtin_functions.to_utc_timestamp`.

Function references are unambiguous and are not interpreted using session context. Producers are responsible for resolving catalog, namespace, and name if the session is relevant. For example, if a SQL engine uses its current catalog and namespace to find a function, the resolved catalog and namespace must be used to produce an unambiguous function reference.


#### Value expression types

The type produced by a value expression may change. For example, an ID reference may produce a widened type after the underlying column's type is promoted.

A value expression's result type is determined when it is bound to a specific input schema.

Function calls may produce different types when function definitions change, and type changes may change the definition that is resolved for a function name. For example, if the input field passed to `identity(int) -> int` is promoted from `int` to `long`, the resolved `identity` function can change to `identity(long) -> long` if it is defined.

If types are incompatible at runtime, implementations binding or evaluating expressions may apply type promotion to align types for predicates and to resolve functions. Implementations may choose when to promote values to accommodate engines that differ in casting behavior. However, implementations must fail rather than insert unsafe casts.


### Predicates

A predicate is a boolean expression that produces true or false.

Predicates can be constants (true or false), tests of a value expression, comparisons of value expressions, or logical combinations of predicates (AND, OR, NOT).

Value expressions are not valid predicates, even when the expression is expected to return a boolean value. Value expressions must be compared or tested to produce a predicate. For example, `is_empty(str_col)` is not a valid predicate because it may produce `null`, but `is_empty(str_col) = true` is a valid predicate.


#### Tests

Tests are predicates that test a single value expression, optionally using a constant or set of constants. Constants must all have the same type and must be non-null and non-NaN. Tests are:

| Test                    | Allowed types | Constant type | Description |
|-------------------------|---------------|---------------|-------------|
| `IS NULL`               | any           |               | true iff the value is null |
| `IS NOT NULL`           | any           |               | true iff the value is not null |
| `IS NaN`                | float, double |               | true iff the value is an IEEE 754 NaN |
| `IS NOT NaN`            | float, double |               | true iff the value is not an IEEE 754 NaN |
| `STARTS WITH const`     | string        | string        | true iff the constant is a prefix of the value |
| `NOT STARTS WITH const` | string        | string        | true iff the constant is not a prefix of the value |
| `IN (constant set)`     | any primitive | same as value | true iff the value is equal to any constant |
| `NOT IN (constant set)` | any primitive | same as value | true iff the value is not equal to any constant |


#### Comparisons

Comparisons are predicates that compare two value expressions with the same primitive type.

If value expression types in a comparison are incompatible, implementations should align types using type promotion. For instance, `int_col > 5.0` should promote int values to float. If the types cannot be aligned according to type promotion rules (for instance, `"goats" > -Infinity`), the predicate cannot be evaluated and implementations must fail.

Comparisons are:

| Comparison  | Description |
|-------------|-------------|
| `=`         | Is equal (is not distinct from) |
| `!=`        | Is not equal |
| `<`         | Less than |
| `<=`        | Less than or equal |
| `>`         | Greater than |
| `>=`        | Greater than or equal |

Comparisons must be null-safe. For any two operands a and b:

* `a = b` is true if both are null, or both are non-null and equal; otherwise false
* `a != b` is the boolean negation of `a = b`
* `a < b` and `a > b` are false when either operand is null; otherwise they use the order defined above
* `a <= b` is `(a = b) OR (a < b)`; `a >= b` is `(a = b) OR (a > b)`; both are true when both operands are null and false when only one operand is null

This table shows examples of these rules after evaluating value expressions to constants:

| Comparison     | Result  |
|----------------|---------|
| `null = null`  | `true`  |
| `34 = null`    | `false` |
| `null != null` | `false` |
| `34 != null`   | `true`  |
| `null < null`  | `false` |
| `null <= null` | `true`  |
| `34 < null`    | `false` |

Value expressions that are the direct child of a comparison must not be either a null or NaN constant. However, comparisons must handle null and NaN values that are the result of evaluating a value expression. For example, `x = get_item(map, "key")` is valid although `get_item` may return a null value, but `x = null` must be rejected because `x IS NULL` is the correct unambiguous predicate. Similarly, `multiply(a, b)` may produce NaN for `a=0.0` and `b=Infinity` and is valid, but `x = NaN` must be rejected because `x IS NaN` is the correct test.

Primitive types are compared using signed comparison, except for the following types:

* `false` is less than `true` for `boolean`
* `fixed` and `binary` use unsigned byte-wise comparison
* `string` uses unsigned byte-wise comparison of the UTF-8 representation; it is not the Unicode Collation Algorithm
* `uuid` uses unsigned byte-wise comparison of the UUID bytes
* `decimal` uses signed comparison independent of scale; this is equivalent to comparison of unscaled values because type alignment produces values with the same scale
* `float` and `double` use IEEE 754 order for all non-NaN values; see below for NaN comparison rules

For floating point values, comparison with NaN behaves similarly to comparison of values with null. NaN should be specifically handled using `IS NaN` and `IS NOT NaN` tests. However, when value expressions produce a NaN value, the following rules must be applied:

* `a = b` is true if both are NaN, or both are non-NaN and equal; false otherwise
* `a != b` is the boolean negation of `a = b`
* `a < b` and `a > b` are false when either operand is NaN; otherwise the IEEE 754 order is used
* `a <= b` is `(a = b) OR (a < b)`; `a >= b` is `(a = b) OR (a > b)`; both are true when both operands are NaN and false when only one operand is NaN


#### Boolean logic

Predicates must use 2-valued boolean logic. Evaluation of all predicates must produce `true` or `false`.

Engines that implement SQL 3-valued boolean logic must add `IS NULL` and `IS NOT NULL` to produce the 2-valued equivalent. This avoids bugs in engines and languages that do not natively implement 3-valued logic. For example, the SQL predicate `x < 10` should be passed as `x < 10 AND x IS NOT NULL` for a SQL `WHERE` condition (or `x < 10`; see null-safe comparisons below). For a `CHECK` constraint, the expression is passed as `x < 10 OR x IS NULL`. This ensures that implementations will make the correct determination, rather than depending on context to interpret a null result (`WHERE` vs `CHECK`).

Logical combinations are boolean operators applied to predicates. `AND` and `OR` are binary operations and `NOT` is a unary operation. `AND`, `OR`, and `NOT` do not accept null values because predicates cannot produce them.


### Compatibility with REST catalog expressions

Prior to this spec, REST APIs used a more restrictive, term-based form of predicates and references. Those forms are now deprecated, but should be supported for backward compatibility to allow older clients to interact with newer REST catalog services.

The deprecated expressions were passed in 3 places:

* As `filter` passed to server-side scan planning
* As `filter` passed to the service in `ScanReport`
* As `residual` passed to the client with a scan task

Both server-side scan planning and the report endpoint should continue to accept filters from older clients by parsing term-based expressions (see [Appendix B: JSON serialization](#backward-compatibility)).

Residuals passed from services back to clients that do not use the new syntax would cause clients to fail. Services are allowed to omit the residual so that it is calculated on the client side (intended to avoid duplicating large IN filters). For compatibility, REST services should omit residuals from tasks, but may include them if the service detects support for newer predicates (for example, via client version).


## Appendix A: Iceberg functions

This section defines the functions in the `iceberg_functions` reserved catalog name.

* `if_else(condition: predicate, when_true: T, when_false: T) -> T`: returns the value of `when_true` when `condition` is true and `when_false` otherwise

### Partition transforms

Iceberg partition transforms are also defined as functions (other than `void`).

All partition transforms produce `null` for a `null` input value.

| Function name     | Description                                                  | Source types                                                         | Result type |
|-------------------|--------------------------------------------------------------|----------------------------------------------------------------------|-------------|
| `identity(value)` | Source value, unmodified                                     | Any primitive except for `geometry`, `geography`, and `variant`      | Source type |
| `year(value)`     | Extract a date or timestamp year, as years from 1970         | `date`, `timestamp`, `timestamptz`, `timestamp_ns`, `timestamptz_ns` | `int`       |
| `month(value)`    | Extract a date or timestamp month, as months from 1970-01-01 | `date`, `timestamp`, `timestamptz`, `timestamp_ns`, `timestamptz_ns` | `int`       |
| `day(value)`      | Extract a date or timestamp day, as days from 1970-01-01     | `date`, `timestamp`, `timestamptz`, `timestamp_ns`, `timestamptz_ns` | `date`      |
| `hour(value)`     | Extract a timestamp hour, as hours from 1970-01-01 00:00:00  | `timestamp`, `timestamptz`, `timestamp_ns`, `timestamptz_ns`         | `int`       |

Note that `year`, `month`, and `hour` transforms produce ordinal values and not human-readable values. For example, `year(2018-05-13)` produces `48`, not `2018`.

`bucket` and `truncate` are called as 2-argument functions. The first argument is an `int` parameter (`N` or `W` from the table spec) and the second argument is the value to transform. For example, `bucket(256, id)` calls `bucket[256]`.

| Parameterized function name | Description                                                           | Source types                                                                                 | Result type |
|-----------------------------|-----------------------------------------------------------------------|----------------------------------------------------------------------------------------------|-------------|
| `bucket(N, value)`          | Hash of value, mod `N` (see [table spec details][bucket-ref])         | Any primitive except for `geometry`, `geography`, `variant`, `boolean`, `float`, or `double` | `int`       |
| `truncate(W, value)`        | Value truncated to width `W` (see [table spec details][truncate-ref]) | `int`, `long`, `decimal`, `string`, `binary`                                                 | Source type |

[bucket-ref]: spec/#bucket-transform-details
[truncate-ref]: spec/#truncate-transform-details


## Appendix B: JSON serialization

Iceberg expressions are serialized as JSON objects in table, view, and UDF metadata, and in the REST protocol for catalogs.

### Value expressions

```
EXPR: LITERAL | REFERENCE | APPLY

LITERAL: VALUE
    | { "type": "literal", "value": VALUE }
    | { "type": "literal", "value": VALUE, "data-type": DATA_TYPE }
LITERALS: [ LITERAL* ]
    | { "type": "literals", "values": [ VALUE* ], "data-type": DATA_TYPE }

REFERENCE: BOUND_REF | UNBOUND_REF
BOUND_REF: { "type": "reference", "id": ID }
UNBOUND_REF: { "type": "reference", "name": NAME }

APPLY: { "type": "apply", "function": FUNC_REF, "arguments": [ FUNC_ARG* ] }
FUNC_ARG: EXPR | PREDICATE
FUNC_REF: NAME
    | [ NAME* ]
    | { "identifier": [ NAME* ] }
    | { "catalog": NAME, "identifier": [ NAME* ] }

ID: integer
NAME: string

VALUE: single value JSON from the table spec
DATA_TYPE: Iceberg type from the spec
```

If a function reference is a string, that string is the one-part identifier and the catalog is missing/null.
If a function reference is a list of strings, it is the function identifier and the catalog is missing/null.

### Predicates

```
PREDICATE: true | false
    | { "type": "not", "child": PREDICATE }
    | { "type": BINARY_OP, "left": PREDICATE, "right": PREDICATE }
    | { "type": UNARY_OP, "child": EXPR }
    | { "type": CMP_OP, "left": EXPR, "right": EXPR }
    | { "type": SET_OP, "child": EXPR, "values": LITERALS }
    | DEPRECATED_PREDICATE

BINARY_OP: "and" | "or"
UNARY_OP: "is-null" | "not-null" | "is-nan" | "not-nan"
CMP_OP: "lt" | "lt-eq" | "gt" | "gt-eq" | "eq" | "not-eq"
      | "starts-with" | "not-starts-with"
SET_OP: "in" | "not-in"
```

### Backward compatibility

```
DEPRECATED_PREDICATE:
    | { "type": UNARY_OP, "term": TERM }
    | { "type": CMP_OP, "term": TERM, "value": LITERAL }
    | { "type": SET_OP, "term": TERM, "values": LITERALS }

DEPRECATED_REF: { "type": "reference", "term": NAME }

TERM: NAME | DEPRECATED_REF | TRANSFORM
TRANSFORM: { "type": "transform", "transform": NAME, "term": TERM }
```
