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

# Iceberg SQL UDF Spec

## Background and Motivation

A SQL user-defined function (UDF or UDTF) is a callable routine that accepts input parameters, executes a function body.
Depending on the function type, the result can be:

- **UDFs** – return a scalar value, which may be a primitive type (e.g., `int`, `string`) or a non-primitive type (e.g., `struct`, `list`).
- **Table functions (UDTFs)** – return a table, i.e., a table with zero or more rows and columns with a uniform schema.

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

* Any modification (new overload, updated representation, changed properties, etc.) creates a new metadata file, and atomically swaps in the new file as the current metadata.
* Each metadata file includes recent definition versions, enabling rollbacks without external state.

## Specification

### UDF Metadata
The UDF metadata file has the following fields:

| Requirement | Field name                   | Description                                                      |
|-------------|------------------------------|------------------------------------------------------------------|
| *required*  | `function-uuid`              | A UUID that identifies the function, generated once at creation. |
| *required*  | `format-version`             | Metadata format version (must be `1`).                           |
| *required*  | `definitions`                | List of function overloads.                                      |
| *required*  | `definition-versions`        | List of versioned function definitions.                          |
| *required*  | `current-definition-version` | Identifier of the current definition version.                    |
| *optional*  | `location`                   | The storage location of metadata files.                          | 
| *optional*  | `properties`                 | Arbitrary key-value pairs.                                       | 
| *optional*  | `secure`                     | Whether it is a secure function. Default: `false`.               |
| *optional*  | `doc`                        | Documentation string.                                            |

Notes:
1. When `secure` is `true`,
    - Engines **SHOULD NOT** expose the function definition through any inspection (e.g., `SHOW FUNCTIONS`).
    - Engines **SHOULD** ensure that execution does not leak sensitive information through any channels, such as error messages, logs, or query plans.
   
### Overload

Function overloads allow multiple implementations of the same function name with different signatures. Each overload has
the following fields:

| Requirement | Field name                 | Description                                                                                                                                                                                        |
|-------------|----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| *required*  | `overload-uuid`            | A UUID that identifies this function overload.                                                                                                                                                     |
| *required*  | `parameters`               | Ordered list of function parameter definitions (name, type, optional doc). The order of parameters in this list **must exactly match** the order of arguments provided when invoking the function. |
| *required*  | `return-type`              | Return type (any primitive type or non-primitive type supported by Iceberg). Example: `"string"` or `"struct<...>"`                                                                                |
| *required*  | `versions`                 | List of overload versions.                                                                                                                                                                         |
| *required*  | `current-overload-version` | Identifier of the current overload version.                                                                                                                                                        |
| *optional*  | `function-type`            | `udf` or `udtf`, default to `udf`. If `udtf`, the `return-type` must be a `struct` describing the output schema.                                                                                   | 
| *optional*  | `doc`                      | Documentation string.                                                                                                                                                                              |

Notes:
1. The `name` and `type` of `parameters` are immutable. To change them, a new overload must be created. Only the optional documentation field (`doc`) can be updated in-place.
2. The `return type` is immutable. To change it, users must create a new overload and deprecate or remove the old one.

### Overload-Version
Each overload can evolve over time by introducing new versions. An overload version represents a specific implementation
of the overload at a given point in time.

| Requirement | Field name            | Description                                                                                  |
|-------------|-----------------------|----------------------------------------------------------------------------------------------|
| *required*  | `overload-version-id` | Monotonically increasing identifier (long) for this overload’s version history. Example: `1` |
| *required*  | `representations`     | Dialect-specific implementations of this overload.                                           |
| *optional*  | `deterministic`       | Whether the function is deterministic. Defaults to `false`.                                  |
| *required*  | `timestamp-ms`        | Creation timestamp of this version, in epoch milliseconds.                                   |

### Representation
A representation encodes how the overload version is expressed in a specific SQL dialect.

| Requirement | Field name | Description                                            |
|-------------|------------|--------------------------------------------------------|
| *required*  | `dialect`  | SQL dialect identifier. Example: `"spark"`, `"trino"`. |
| *required*  | `body`     | SQL expression text.                                   |

Note: The `body` must be valid SQL in the specified dialect; validation is the responsibility of the consuming engine.

### Definition-Version

| Requirement | Field name              | Description                                                                                                                                                      |
|-------------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| *required*  | `definition-version-id` | Unique identifier of the definition version, a monotonically increasing long. Example: `2`                                                                       |
| *required*  | `timestamp-ms`          | Timestamp when the definition was created or updated. Example: `1734506000456`                                                                                   |
| *required*  | `overload-versions`     | List of mapping of overload uuids to their current version ids. Example: `[{"overload-uuid": "1f2c9b5b-1b7c-4a36-a9b0-6d3a0f4b7c21", "overload-version-id": 1}]` |

## Function Resolution in Engines
Resolution rule is decided by engines, but engines SHOULD:
1. Prefer exact signature matches over casting or subtyping.
2. Allow numeric widening (e.g., `INT` --> `BIGINT/FLOAT`) by default.
3. Require explicit casts for risky conversions (e.g., `STRING` --> `DATE`).

## Appendix A: UDF Example

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
         "overload-uuid": "d2c7dfe0-54a3-4d5f-a34d-2e8cfbc34111",
         "parameters": [
            { "name": "x", "type": "int", "doc": "Input integer" }
         ],
         "return-type": "int",
         "doc": "Add one to the input integer",
         "versions": [
            {
               "overload-version-id": 1,
               "deterministic": true,
               "representations": [
                  {
                     "dialect": "trino",
                     "body": "x + 2"
                  }
               ],
               "timestamp-ms": 1734507000123
            },
            {
               "overload-version-id": 2,
               "deterministic": true,
               "representations": [
                  {
                     "dialect": "trino",
                     "body": "x + 1"
                  }
               ],
               "timestamp-ms": 1735507000124
            }
         ],
         "current-overload-version": 2
      },
      {
         "overload-uuid": "7c9f93b1-28b4-4ef5-90f5-70c73cda2222",
         "parameters": [
            { "name": "x", "type": "float", "doc": "Input float" }
         ],
         "return-type": "float",
         "doc": "Add one to the input float",
         "versions": [
            {
               "overload-version-id": 1,
               "deterministic": true,
               "representations": [
                  {
                     "dialect": "trino",
                     "body": "x + 1.0"
                  }
               ],
               "timestamp-ms": 1734507001123
            }
         ],
         "current-overload-version": 1
      }
   ],
   "definition-versions": [
      {
         "definition-version-id": 2,
         "timestamp-ms": 1735507000124,
         "overload-versions": [
            {"overload-uuid": "d2c7dfe0-54a3-4d5f-a34d-2e8cfbc34111", "overload-version-id": 2},
            {"overload-uuid": "7c9f93b1-28b4-4ef5-90f5-70c73cda2222", "overload-version-id": 1}
         ]
      },
      {
         "definition-version-id": 1,
         "timestamp-ms": 1734507001123,
         "overload-versions": [
            {"overload-uuid": "d2c7dfe0-54a3-4d5f-a34d-2e8cfbc34111", "overload-version-id": 1},
            {"overload-uuid": "7c9f93b1-28b4-4ef5-90f5-70c73cda2222", "overload-version-id": 1}
         ]
      }
   ],
   "current-definition-version": 2,
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
      "overload-uuid": "1f2c9b5b-1b7c-4a36-a9b0-6d3a0f4b7c21",
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
          "overload-version-id": 1,
          "deterministic": true,
          "representations": [
            {
              "dialect": "trino",
              "body": "SELECT name, color FROM fruits WHERE color = c"
            },
            {
              "dialect": "spark",
              "body": "SELECT name, color FROM fruits WHERE color = c"
            }
          ],
          "timestamp-ms": 1734508000123
        }
      ],
      "current-overload-version": 1
    }
  ],
  "definition-versions": [
    {
      "definition-version-id": 1,
      "timestamp-ms": 1734508000123,
      "overload-versions": [
        {"overload-uuid": "1f2c9b5b-1b7c-4a36-a9b0-6d3a0f4b7c21", "overload-version-id": 1}
      ]
    }
  ],
  "current-definition-version": 1,
  "doc": "UDTF returning (name, color) rows filtered by the given color",
  "secure": false
}
```
