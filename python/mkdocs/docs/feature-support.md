---
hide:
  - navigation
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

# Feature Support

The goal is that the python library will provide a functional, performant subset of the Java library. The initial focus has been on reading table metadata and provide a convenient CLI to go through the catalog.

## Metadata

| Operation                | Java | Python |
| :----------------------- | :--: | :----: |
| Get Schema               |  X   |   X    |
| Get Snapshots            |  X   |   X    |
| Plan Scan                |  X   |   X    |
| Plan Scan for Snapshot   |  X   |   X    |
| Update Current Snapshot  |  X   |        |
| Create Table             |  X   |   X    |
| Rename Table             |  X   |   X    |
| Drop Table               |  X   |   X    |
| Alter Table              |  X   |        |
| Set Table Properties     |  X   |        |
| Create Namespace         |  X   |   X    |
| Drop Namespace           |  X   |   X    |
| Set Namespace Properties |  X   |   X    |

## Types

The types are kept in `pyiceberg.types`.

Primitive types:

- `BooleanType`
- `StringType`
- `IntegerType`
- `LongType`
- `FloatType`
- `DoubleType`
- `DateType`
- `TimeType`
- `TimestampType`
- `TimestamptzType`
- `BinaryType`
- `UUIDType`

Complex types:

- `StructType`
- `ListType`
- `MapType`
- `FixedType(16)`
- `DecimalType(8, 3)`

## Expressions

The expressions are kept in `pyiceberg.expressions`.

- `IsNull`
- `NotNull`
- `IsNaN`
- `NotNaN`
- `In`
- `NotIn`
- `EqualTo`
- `NotEqualTo`
- `GreaterThanOrEqual`
- `GreaterThan`
- `LessThanOrEqual`
- `LessThan`
- `And`
- `Or`
- `Not`
