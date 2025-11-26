---
title: "SQL UDF Spec"
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

# Iceberg UDF Spec

## Background and Motivation

A SQL user-defined function (UDF or UDTF) is a callable routine that accepts input parameters and executes a function body.
Depending on the function type, the result can be:

- **Scalar function (UDF)** – returns a scalar value, which may be a primitive type (e.g., `int`, `string`) or a non-primitive type (e.g., `struct`, `list`).
- **Table function (UDTF)** – returns a table with zero or more rows of columns with a uniform schema.

Many compute engines (e.g., Spark, Trino) already support UDFs, but in different and incompatible ways. Without a common
standard, UDFs cannot be reliably shared across engines or reused in multi-engine environments.

This specification introduces a standardized metadata format for UDFs in Iceberg.

## Goals

* Define a portable metadata format for both scalar and table SQL UDFs. The metadata is self-contained and can be moved across catalogs.
* Support function evolution through versioning and rollback.
* Provide consistent semantics for representing UDFs across engines.

## Overview

UDF metadata follows the same design principles as Iceberg table and view metadata: each function is represented by a
**self-contained metadata file**. Metadata captures definitions, parameters, return types, documentation, security,
properties, and engine-specific representations.

* Any modification (new definition, updated representation, changed properties, etc.) creates a new metadata file, and atomically swaps in the new file as the current metadata.
* Each metadata file includes recent definition versions, enabling rollbacks without external state.

## Specification

### UDF Metadata
The UDF metadata file has the following fields:

| Requirement | Field name        | Type                   | Description                                                                                                     |
|-------------|-------------------|------------------------|-----------------------------------------------------------------------------------------------------------------|
| *required*  | `function-uuid`   | `string`               | A UUID that identifies the function, generated once at creation.                                                |
| *required*  | `format-version`  | `int`                  | Metadata format version (must be `1`).                                                                          |
| *required*  | `definitions`     | `list<definition>`     | List of function [definition](#definition) entities.                                                            |
| *required*  | `definition-log`  | `list<definition-log>` | History of [definitions snapshots](#definitions-log).                                                           |
| *required*  | `parameter-names` | `list<parameter-name>` | Global ordered parameter names shared across all overloads. Overloads must use a prefix of this list, in order. |
| *optional*  | `location`        | `string`               | Storage location of metadata files.                                                                             |
| *optional*  | `properties`      | `map`                  | A string to string map of properties.                                                                           |
| *optional*  | `secure`          | `boolean`              | Whether it is a secure function. Default: `false`.                                                              |
| *optional*  | `doc`             | `string`               | Documentation string.                                                                                           |

### Parameter-Name
| Requirement | Field  | Type     | Description              |
|-------------|--------|----------|--------------------------|
| *required*  | `name` | `string` | Parameter name.          |
| *optional*  | `doc`  | `string` | Parameter documentation. |

Notes:
1. When `secure` is `true`:
   - Engines MUST NOT expose the function definition or its body through any form of metadata inspection (e.g., `SHOW FUNCTIONS`).
   - Engines MUST prevent leakage of sensitive information during execution via error messages, logs, query plans, or intermediate results.
   - Engines MUST NOT perform predicate reordering, short-circuiting, or other optimizations that could change the order or scope of data access.
2. Entries in `properties` are treated as hints, not strict rules. Engines MAY choose to honor them or ignore them.
3. `parameter-names` is the single global source of truth for parameter naming across all overloads:
   - Each overload MUST use a prefix of this list, in order. 
   - Names and relative ordering are immutable. 
   - Only appending new names is allowed.
   - Only the doc field may be updated in place.
   - Type-only overloads (e.g., `foo(int x)` and `foo(float x)`) are valid as long as the names match the prefix.

### Definition

Each `definition` represents one function signature (e.g., `add_one(int)` vs `add_one(float)`).

| Requirement | Field name           | Type                                                                                                                                                                                                                                                                    | Description                                                                                                                                                                                         |
|-------------|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| *required*  | `definition-id`      | `string`                                                                                                                                                                                                                                                                | An identifier derived from canonical parameter-type tuple (lowercase, no spaces; e.g., `"(int,int,string)"`). If longer than 128 chars, use hashed form `"sig1-<base32(SHA-256(signature))[:26]>"`. |
| *required*  | `parameters`         | `list<parameter>`                                                                                                                                                                                                                                                       | Ordered list of [function parameters](#parameter). Invocation order **must** match this list.                                                                                                       |
| *required*  | `return-type`        | [JSON representation](https://iceberg.apache.org/spec/#appendix-c-json-serialization) of an Iceberg type where primitive types are encoded as JSON strings (e.g., `"int"`, `"string"`), and complex types are encoded as JSON objects (e.g., `{"type": "struct", ...}`) | Type of value returned                                                                                                                                                                              |
| *optional*  | `nullable-return`    | `boolean`                                                                                                                                                                                                                                                               | A hint to indicate whether the return value is nullable or not. Default: `true`.                                                                                                                    |
| *required*  | `versions`           | `list<definition-version>`                                                                                                                                                                                                                                              | [Versioned implementations](#definition-version) of this definition.                                                                                                                                |
| *required*  | `current-version-id` | `int`                                                                                                                                                                                                                                                                   | Identifier of the current version for this definition.                                                                                                                                              |
| *optional*  | `function-type`      | `string` (`"udf"` or `"udtf"`, default `"udf"`)                                                                                                                                                                                                                         | If `"udtf"`, `return-type` must be an Iceberg type `struct` describing the output schema.                                                                                                           |
| *optional*  | `doc`                | `string`                                                                                                                                                                                                                                                                | Documentation string.                                                                                                                                                                               |

Notes:
1. `sig1-<base32(SHA-256(signature))[:26]>` is a fixed-length hash used when the canonical signature is too long.
It’s generated by taking the SHA-256 of the normalized signature, encoding it in Base32, keeping the first 26 characters,
and prefixing with `sig1-`. This yields a 31-character deterministic ID, easy to verify and future-proof via the `sig1-`
version prefix.

### Parameter
| Requirement | Field           | Type                                                                                                                                                                                                                                                                    | Description         |
|-------------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------|
| *required*  | `type`          | [JSON representation](https://iceberg.apache.org/spec/#appendix-c-json-serialization) of an Iceberg type where primitive types are encoded as JSON strings (e.g., `"int"`, `"string"`), and complex types are encoded as JSON objects (e.g., `{"type": "struct", ...}`) | Parameter data type. |
| *optional*  | `default-value` | `string`                                                                                                                                                                                                                                                                | Default value.      |

Notes:
1. Function definitions are identified by the tuple of `type`s and there can be only one definition for a given tuple.
   The type tuple is immutable across versions.
2. Variadic (vararg) parameters are not supported. Each definition must declare a fixed number of parameters.
3. Each parameter input MUST be assignable to its declared Iceberg type. For complex types, the value’s
   structure must match (correct field names, element/key/value types, and nesting). If a parameter—or any nested
   field/element—is marked required, engines MUST reject null at that position (including inside structs, lists, and maps).
4. The `return-type` is immutable across versions. To change it, users must create a new definition and remove the old one.
5. The function MUST return a value assignable to the declared `return-type`, meaning the returned value’s type and
   structure must match the declared Iceberg type (including field names, element types, and nesting for complex types),
   and any field or element marked as required MUST NOT be null. Engines MUST reject results that violate these rules.

### Definition-Version

Each definition can evolve over time by introducing new versions.  
A `definition version` represents a specific implementation of that definition at a given point in time.

| Requirement | Field name        | Type                                                                                                    | Description                                                    |
|-------------|-------------------|---------------------------------------------------------------------------------------------------------|----------------------------------------------------------------|
| *required*  | `version-id`      | `int`                                                                                                   | Monotonically increasing identifier of the definition version. |
| *required*  | `representations` | `list<representation>`                                                                                  | [Dialect-specific implementations](#representation).           |
| *optional*  | `deterministic`   | `boolean` (default `false`)                                                                             | Whether the function is deterministic.                         |
| *optional*  | `on-null-input`   | `string` (`"returns_null_on_null_input"` or `"called_on_null_input"`, default `"called_on_null_input"`) | Defines how the UDF behaves when any input parameter is NULL.  |
| *required*  | `timestamp-ms`    | `long` (epoch millis)                                                                                   | Creation timestamp of this version.                            |

Note:

`on-null-input` provides an optimization hint for query engines, its value can be either `"returns_null_on_null_input"` or `"called_on_null_input"`:
1. If set to `returns_null_on_null_input`, the function always returns `NULL` if any input argument is `NULL`. This allows engines to apply predicate pushdown or skip function evaluation for rows with `NULL` inputs. For a function `f(x, y) = x + y`,
the engine can safely rewrite `WHERE f(a,b) > 0` as `WHERE a IS NOT NULL AND b IS NOT NULL AND f(a,b) > 0`.
2. If set to `called_on_null_input`, the function may handle `NULL`s internally (e.g., `COALESCE`, `NVL`, `IFNULL`), so the engine must execute the function even if some inputs are `NULL`.

### Representation
A representation encodes how the definition version is expressed in a specific SQL dialect.

| Requirement | Field name   | Type              | Description                                                                                 |
|-------------|--------------|-------------------|---------------------------------------------------------------------------------------------|
| *required*  | `type`       | `string`          | Must be `"sql"`                                                                             |
| *required*  | `dialect`    | `string`          | SQL dialect identifier (e.g., `"spark"`, `"trino"`).                                        |
| *required*  | `body`       | `string`          | SQL expression text.                                                                        |

Note: The `body` must be valid SQL in the specified dialect; validation is the responsibility of the consuming engine.

### Definition log
| Requirement | Field name            | Type                                               | Description                                                      |
|-------------|-----------------------|----------------------------------------------------|------------------------------------------------------------------|
| *required*  | `timestamp-ms`        | `long` (epoch millis)                              | When the definition snapshot was created or updated.             |
| *required*  | `definition-versions` | `list<{ definition-id: string, version-id: int }>` | Mapping of each definition to its selected version at this time. |

## Function Resolution in Engines
Resolution rule is decided by engines, but engines SHOULD:
1. Prefer exact signature matches over casting or subtyping.
2. Allow numeric widening (e.g., `INT` --> `BIGINT/FLOAT`) when needed and where it does not conflict with the rule above.
3. Require explicit casts for unsafe conversions (e.g., `STRING` --> `DATE`).

## Appendix A: Example – Overloaded Scalar Function

SQL statement:
```sql
CREATE FUNCTION add_one(x INT COMMENT 'Input integer')
COMMENT 'Add one to the input value'
RETURNS INT
RETURN x + 1;

CREATE FUNCTION add_one(x FLOAT COMMENT 'Input float')
COMMENT 'Add one to the input value'
RETURNS FLOAT
RETURN x + 1.0;
```

```json
{
   "function-uuid": "42fd3f91-bc10-41c1-8a52-92b57dd0a9b2",
   "format-version": 1,
   "definitions": [
      {
         "definition-id": "(int)",
         "parameters": [
            { "name": "x", "type": "int", "doc": "Input integer" }
         ],
         "return-type": "int",
         "doc": "Add one to the input integer",
         "versions": [
            {
               "version-id": 1,
               "deterministic": true,
               "representations": [
                  { "dialect": "trino", "body": "x + 2" }
               ],
               "timestamp-ms": 1734507000123
            },
            {
               "version-id": 2,
               "deterministic": true,
               "representations": [
                  { "dialect": "trino", "body": "x + 1" },
                  { "dialect": "spark", "body": "x + 1" }
               ],
               "timestamp-ms": 1735507000124
            }
         ],
         "current-version-id": 2
      },
      {
         "definition-id": "(float)",
         "parameters": [
            { "name": "x", "type": "float", "doc": "Input float" }
         ],
         "return-type": "float",
         "doc": "Add one to the input float",
         "versions": [
            {
               "version-id": 1,
               "deterministic": true,
               "representations": [
                  { "dialect": "trino", "body": "x + 1.0" }
               ],
               "timestamp-ms": 1734507001123
            }
         ],
         "current-version-id": 1
      }
   ],
   "definition-log": [
      {
         "timestamp-ms": 1734507001123,
         "definition-versions": [
            { "definition-id": "(int)", "version-id": 1 },
            { "definition-id": "(float)", "version-id": 1 }
         ]
      },
      {
         "timestamp-ms": 1735507000124,
         "definition-versions": [
            { "definition-id": "(int)", "version-id": 2 },
            { "definition-id": "(float)", "version-id": 1 }
         ]
      }
   ],
   "doc": "Overloaded scalar UDF for integer and float inputs",
   "secure": false
}
```

## Appendix B: UDTF Example
SQL statement:

```sql
CREATE FUNCTION fruits_by_color(c VARCHAR COMMENT 'Color of fruits')
    COMMENT 'Return fruits of specific color from fruits table'
RETURNS TABLE (name VARCHAR, color VARCHAR)
RETURN SELECT name, color FROM fruits WHERE color = c;
```

```json
{
   "function-uuid": "8a7fa39a-6d8f-4a2f-9d8d-3f3a8f3c2a10",
   "format-version": 1,
   "definitions": [
      {
         "definition-id": "(string)",
         "parameters": [
            { "name": "c", "type": "string", "doc": "Color of fruits" }
         ],
         "return-type": {
            "type": "struct",
            "fields": [
               { "id": 1, "name": "name", "type": "string" },
               { "id": 2, "name": "color", "type": "string" }
            ]
         },
         "function-type": "udtf",
         "doc": "Return fruits of a specific color from the fruits table",
         "versions": [
            {
               "version-id": 1,
               "deterministic": true,
               "representations": [
                  { "dialect": "trino", "body": "SELECT name, color FROM fruits WHERE color = c" },
                  { "dialect": "spark", "body": "SELECT name, color FROM fruits WHERE color = c" }
               ],
               "timestamp-ms": 1734508000123
            }
         ],
         "current-version-id": 1
      }
   ],
   "definition-log": [
      {
         "timestamp-ms": 1734508000123,
         "definition-versions": [
            { "definition-id": "(string)", "version-id": 1 }
         ]
      }
   ],
   "doc": "UDTF returning (name, color) rows filtered by the given color",
   "secure": false
}
```
