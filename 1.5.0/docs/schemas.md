---
title: Schemas
search:
  exclude: true
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

# Schemas

Iceberg tables support the following types:

| Type               | Description                                                              | Notes                                            |
|--------------------|--------------------------------------------------------------------------|--------------------------------------------------|
| **`boolean`**      | True or false                                                            |                                                  |
| **`int`**          | 32-bit signed integers                                                   | Can promote to `long`                            |
| **`long`**         | 64-bit signed integers                                                   |                                                  |
| **`float`**        | [32-bit IEEE 754](https://en.wikipedia.org/wiki/IEEE_754) floating point | Can promote to `double`                          |
| **`double`**       | [64-bit IEEE 754](https://en.wikipedia.org/wiki/IEEE_754) floating point |                                                  |
| **`decimal(P,S)`** | Fixed-point decimal; precision P, scale S                                | Scale is fixed and precision must be 38 or less  |
| **`date`**         | Calendar date without timezone or time                                   |                                                  |
| **`time`**         | Time of day without date, timezone                                       | Stored as microseconds                           |
| **`timestamp`**    | Timestamp without timezone                                               | Stored as microseconds                           |
| **`timestamptz`**  | Timestamp with timezone                                                  | Stored as microseconds                           |
| **`string`**       | Arbitrary-length character sequences                                     | Encoded with UTF-8                               |
| **`fixed(L)`**     | Fixed-length byte array of length L                                      |                                                  |
| **`binary`**       | Arbitrary-length byte array                                              |                                                  |
| **`struct<...>`**  | A record with named fields of any data type                              |                                                  |
| **`list<E>`**      | A list with elements of any data type                                    |                                                  |
| **`map<K, V>`**    | A map with keys and values of any data type                              |                                                  |

Iceberg tracks each field in a table schema using an ID that is never reused in a table. See [correctness guarantees](evolution.md#correctness) for more information.
