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

- **Scalar function (UDF)** – returns a single value, which may be a primitive type (e.g., `int`, `string`) or a non-primitive type (e.g., `struct`, `list`).
- **Table function (UDTF)** – returns a table with zero or more rows of columns with a uniform schema.

This specification introduces a standardized metadata format for UDFs in Iceberg.

## Goals

* Define a portable metadata format for both scalar and table SQL UDFs. The metadata is self-contained and can be moved across catalogs.
* Support function evolution through versioning and rollback.
* Provide consistent semantics for representing UDFs across engines.

## Overview

UDF metadata follows the same design principles as Iceberg table and view metadata: each function is represented by a
**self-contained metadata file**. Metadata captures definitions, parameters, return types, documentation, security,
properties, and engine-specific representations.

* UDF metadata files are immutable. Any modification (new definition, updated representation, changed properties, etc.)
  creates a new metadata file. Catalogs use an atomic swap to update the metadata file linked to a catalog identifier.
* Each metadata file includes recent definition versions, enabling rollbacks without external state.

## Specification

### UDF Metadata
The UDF metadata file has the following fields:

| Requirement | Field name       | Type                   | Description                                                           |
|-------------|------------------|------------------------|-----------------------------------------------------------------------|
| *required*  | `function-uuid`  | `string`               | A UUID that identifies this UDF, generated once at creation.          |
| *required*  | `format-version` | `int`                  | UDF specification format version (must be `1`).                       |
| *required*  | `definitions`    | `list<definition>`     | List of function [definition](#definition) entities.                  |
| *required*  | `definition-log` | `list<definition-log>` | History of [definition version history](#definition-log).             |
| *optional*  | `location`       | `string`               | The function's base location; used to create metadata file locations. |
| *optional*  | `properties`     | `map<string,string>`   | A string-to-string map of properties.                                 |
| *optional*  | `secure`         | `boolean`              | Whether it is a secure function. Default: `false`.                    |
| *optional*  | `doc`            | `string`               | Documentation string.                                                 |

Notes:
1. When `secure` is set to `true`, engines should prevent leakage of sensitive information to end users. Each engine
   may have its own security definition and mechanisms. It is the administrator's responsibility to ensure that secure
   UDFs are properly configured and protected in their environment.
2. Entries in `properties` are treated as hints, not strict rules.
3. UDF names are not stored in metadata. It's the catalog's responsibility to map UDF names to metadata file locations.

### Definition

Each `definition` represents one function signature (e.g., `add_one(int)` vs `add_one(float)`). A definition is uniquely
identified by its signature (the ordered list of parameter types). There can be only one definition for a given signature.
All versions within a definition must accept the same signature as specified in the definition's `parameters` field and
must produce values of the declared `return-type`.

| Requirement | Field name           | Type                           | Description                                                                                       |
|-------------|----------------------|--------------------------------|---------------------------------------------------------------------------------------------------|
| *required*  | `definition-id`      | `string`                       | An identifier derived from canonical parameter-type tuple (see [Definition ID](#definition-id)).  |
| *required*  | `parameters`         | `list<parameter>`              | Ordered list of [function parameters](#parameter). Invocation order **must** match this list.     |
| *required*  | `return-type`        | `string` or `object`           | Declared return type using [Types](#types).                                                       |
| *optional*  | `return-nullable`    | `boolean`                      | A hint to indicate whether the return value is nullable or not. Default: `true`.                  |
| *required*  | `versions`           | `list<definition-version>`     | [Versioned implementations](#definition-version) of this definition.                              |
| *required*  | `current-version-id` | `int`                          | Identifier of the current version for this definition.                                            |
| *required*  | `function-type`      | `string` (`"udf"` or `"udtf"`) | If `"udtf"`, `return-type` must be a `struct` (see [Types](#types)) describing the output schema. |
| *optional*  | `doc`                | `string`                       | Documentation string.                                                                             |

### Parameter
| Requirement | Field  | Type     | Description                                |
|-------------|--------|----------|--------------------------------------------|
| *required*  | `type` | `string` | Parameter data type (see [Types](#types)). |
| *required*  | `name` | `string` | Parameter name.                            |
| *optional*  | `doc`  | `string` | Parameter documentation.                   |

Notes:
1. Variadic (vararg) parameters are not supported. Each definition must declare a fixed number of parameters.

#### Types
[iceberg-type-json]: https://iceberg.apache.org/spec/#appendix-c-json-serialization
Types are based on the [Iceberg Type](https://iceberg.apache.org/spec/#schemas-and-data-types).

Primitive and semi-structured type strings are encoded based on [Iceberg Type JSON Representation][iceberg-type-json]
(e.g., `int`, `string`, `timestamp`, `decimal(9,2)`, `variant`). Type strings must contain no spaces or quote characters.

Nested types (`struct`, `list`, `map`) use the [Iceberg Type JSON Representation][iceberg-type-json] with the
following fields required. Any other fields must be ignored.
* `list` requires `type` and `element`, e.g., `{ "type": "list", "element": "string" }`
* `map` requires `type`, `key`, and `value`, e.g., `{ "type": "map", "key": "string", "value": "int" }`
* `struct` requires `type` and `fields`, where each field requires `name` and `type`,
  e.g., `{ "type": "struct", "fields": [ { "name": "id", "type": "int" }, { "name": "name", "type": "string" } ] }`

#### Definition ID
The `definition-id` is a canonical string derived from the parameter types, formatted as a comma-separated list with no
spaces. Each type uses the string representation described in [Types](#types):

* Primitives: the type name (e.g., `int`)
* List: `list<element-type>` (e.g., `list<int>`)
* Map: `map<key-type,value-type>` (e.g., `map<string,int>`)
* Struct: `struct<name1:type1,name2:type2,...>` with field names and types (e.g., `struct<id:int,name:string>`)

Examples of complete definition-id signatures:
* `int` – single int parameter
* `int,string` – two parameters: int and string
* `int,list<int>,struct<id:int,name:string>` – three parameters: an int, a list and a struct

### Definition Version

Each definition can evolve over time by introducing new versions.  
A `definition version` represents a specific implementation of that definition at a given point in time. A definition version can have multiple SQL representations of different dialects, but only one SQL representation per dialect.

| Requirement | Field name        | Type                                                     | Description                                                    |
|-------------|-------------------|----------------------------------------------------------|----------------------------------------------------------------|
| *required*  | `version-id`      | `int`                                                    | Monotonically increasing identifier of the definition version. |
| *required*  | `representations` | `list<representation>`                                   | [Dialect-specific implementations](#representation).           |
| *optional*  | `deterministic`   | `boolean` (default `false`)                              | Whether the function is deterministic.                         |
| *optional*  | `on-null-input`   | `string` (`"return-null"` or `"call"`, default `"call"`) | Defines how the UDF behaves when any input parameter is NULL.  |
| *required*  | `timestamp-ms`    | `long` (unix epoch millis)                               | Creation timestamp of this version.                            |


#### Null Input Handling
`on-null-input` provides an optimization hint for query engines:
1. If set to `return-null`, the function always returns `NULL` if any input argument is `NULL`. This allows engines to
   apply predicate pushdown or skip function evaluation for rows with `NULL` inputs. For a function `f(x, y) = x + y`,
   the engine can safely rewrite `WHERE f(a,b) > 0` as `WHERE a IS NOT NULL AND b IS NOT NULL AND f(a,b) > 0`.
2. If set to `call`, the function may handle `NULL`s internally (e.g., `COALESCE`, `NVL`, `IFNULL`), so the engine must
   execute the function even if some inputs are `NULL`.

### Representation
Each representation is an object with at least one common field, `type`, that is one of the following:
* `sql`: a SQL expression that defines the function body

Representations further define metadata for each type.

#### SQL Representation

The SQL representation stores the function body as a SQL expression, with metadata such as the SQL dialect.

| Requirement | Field name | Type     | Description                                          |
|-------------|------------|----------|------------------------------------------------------|
| *required*  | `type`     | `string` | Must be `"sql"`                                      |
| *required*  | `dialect`  | `string` | SQL dialect identifier (e.g., `"spark"`, `"trino"`). |
| *required*  | `sql`      | `string` | SQL expression text.                                 |

Notes:
1. The `sql` must reference parameters using the names declared in the definition's `parameters` field.

### Definition Log
| Requirement | Field name            | Type                                                | Description                                                      |
|-------------|-----------------------|-----------------------------------------------------|------------------------------------------------------------------|
| *required*  | `timestamp-ms`        | `long` (unix epoch millis)                          | When the definition snapshot was created or updated.             |
| *required*  | `definition-versions` | `list<struct<definition-id:string,version-id:int>>` | Mapping of each definition to its selected version at this time. |

## Function Call Convention and Resolution in Engines
Selecting the definition of a function to use is delegated to engines, which may apply their own casting rules. However, engines should:

1. Prefer exact parameter matches over safe (widening) or unsafe casts.
2. Safely widen types as needed to avoid failing to find a matching definition.
3. Require explicit casts for unsafe or non-obvious conversions.
4. Use definitions with the same number of arguments as the input.
5. Pass positional arguments in the same position as the input.
6. Use definitions with the same set of field names as named input arguments.

## Appendix A: Example – Overloaded Scalar Function

SQL statement:
```sql
-- Trino SQL
CREATE FUNCTION add_one(x INT COMMENT 'Input integer')
COMMENT 'Add one to the input integer'
RETURNS INT
RETURN x + 1;

-- Trino SQL
CREATE FUNCTION add_one(x FLOAT COMMENT 'Input float')
COMMENT 'Add one to the input float'
RETURNS FLOAT
RETURN x + 1.0;

-- Spark SQL
CREATE OR REPLACE FUNCTION add_one(x FLOAT)
RETURNS FLOAT
RETURN x + 1.0;
```

```json
{
  "function-uuid": "42fd3f91-bc10-41c1-8a52-92b57dd0a9b2",
  "format-version": 1,
  "definitions": [
    {
      "definition-id": "int",
      "parameters": [
        {
          "name": "x", "type": "int", "doc": "Input integer"
        }
      ],
      "return-type": "int",
      "function-type": "udf",
      "doc": "Add one to the input integer",
      "versions": [
        {
          "version-id": 1,
          "deterministic": true,
          "representations": [
            { "type": "sql", "dialect": "trino", "sql": "x + 2" }
          ],
          "timestamp-ms": 1734507000123
        },
        {
          "version-id": 2,
          "deterministic": true,
          "representations": [
            { "type": "sql", "dialect": "trino", "sql": "x + 1" },
            { "type": "sql", "dialect": "spark", "sql": "x + 1" }
          ],
          "timestamp-ms": 1735507000124
        }
      ],
      "current-version-id": 2
    },
    {
      "definition-id": "float",
      "parameters": [
        {
          "name": "x", "type": "float", "doc": "Input float"
        }
      ],
      "return-type": "float",
      "function-type": "udf",
      "doc": "Add one to the input float",
      "versions": [
        {
          "version-id": 1,
          "deterministic": true,
          "representations": [
            { "type": "sql", "dialect": "trino", "sql": "x + 1.0" }
          ],
          "timestamp-ms": 1734507001123
        }
      ],
      "current-version-id": 1
    }
  ],
  "definition-log": [
    {
      "timestamp-ms": 1734507000123,
      "definition-versions": [
        { "definition-id": "int", "version-id": 1 }
      ]
    },
    {
      "timestamp-ms": 1734507001123,
      "definition-versions": [
        { "definition-id": "int", "version-id": 1 },
        { "definition-id": "float", "version-id": 1 }
      ]
    },
    {
      "timestamp-ms": 1735507000124,
      "definition-versions": [
        { "definition-id": "int", "version-id": 2 },
        { "definition-id": "float", "version-id": 1 }
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
      "definition-id": "string",
      "parameters": [
        {
          "name": "c", "type": "string", "doc": "Color of fruits"
        }
      ],
      "return-type": {
        "type": "struct",
        "fields": [
          { "name": "name", "type": "string" },
          { "name": "color", "type": "string" }
        ]
      },
      "function-type": "udtf",
      "doc": "Return fruits of a specific color from the fruits table",
      "versions": [
        {
          "version-id": 1,
          "deterministic": true,
          "representations": [
            { "type": "sql", "dialect": "trino", "sql": "SELECT name, color FROM fruits WHERE color = c" },
            { "type": "sql", "dialect": "spark", "sql": "SELECT name, color FROM fruits WHERE color = c" }
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
        { "definition-id": "string", "version-id": 1 }
      ]
    }
  ],
  "doc": "UDTF returning (name, color) rows filtered by the given color",
  "secure": false
}
```
