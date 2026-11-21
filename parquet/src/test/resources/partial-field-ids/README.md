<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Partial field IDs fixture

`a.parquet` is copied unchanged from Apache Iceberg verification PR #27,
commit `9542d5291c100cb036648beb11920feb1b5f69c1`:
https://github.com/apache/iceberg-verification/blob/9542d5291c100cb036648beb11920feb1b5f69c1/reference-tables/v2/partial-field-ids-parquet-data/data/a.parquet

The fixture was contributed by Sylwester Lachiewicz. Its source README describes
the schema and the three rows. Top-level and struct fields carry IDs; the list
element and map key/value do not. Physical field names and order differ from the
table schema. These tests explore the proposed behavior in Iceberg PR #19037;
they do not claim that its still-proposed semantics are already normative.
