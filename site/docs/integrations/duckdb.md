---
title: "DuckDB"
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

# Getting started with DuckDB

[DuckDB](https://duckdb.org/) is an in-process analytical SQL database. Its
[`iceberg` extension](https://duckdb.org/docs/stable/core_extensions/iceberg/overview)
can read Iceberg tables directly from storage or connect to an Iceberg REST catalog
to read and write tables by name.

## Install and load the extension

Install the [DuckDB CLI](https://duckdb.org/docs/stable/clients/cli/overview), start
`duckdb`, and run:

```sql
INSTALL iceberg;
LOAD iceberg;
```

The extension is maintained by the DuckDB project and has its own release cycle.
To get the latest extension available for your DuckDB version, run
`UPDATE EXTENSIONS;`. See DuckDB's documentation for the supported operations and
limitations of your version.

## Read a table without a catalog

Download and extract DuckDB's [sample Iceberg tables](https://duckdb.org/data/iceberg_data.zip):

```sh
curl -LO https://duckdb.org/data/iceberg_data.zip
unzip iceberg_data.zip
duckdb
```

From the directory where you extracted the archive, run:

```sql
SELECT count(*)
FROM iceberg_scan('data/iceberg/lineitem_iceberg', allow_moved_paths = true);
```

This returns 51,793 rows. The sample table has been moved from its original storage
location, so `allow_moved_paths` lets DuckDB resolve its relocated files.

For your own table, use the metadata file location supplied by its catalog:

```sql
SELECT *
FROM iceberg_scan('/path/to/table/metadata/current.metadata.json');
```

Replace the path with an actual metadata file. A direct read uses that metadata
version and does not track later catalog commits. Direct reads are read-only.
For tables in object storage, configure storage access as described in DuckDB's
[Iceberg documentation](https://duckdb.org/docs/stable/core_extensions/iceberg/overview).

## Connect to a REST catalog

To read and write tables by name, attach an existing Iceberg REST catalog. The
following example connects to a local catalog that does not require authentication:

```sql
ATTACH 'warehouse' AS lake (
    TYPE iceberg,
    ENDPOINT 'http://localhost:8181',
    AUTHORIZATION_TYPE 'none'
);
```

Replace the endpoint and warehouse with your catalog's values. Use
`AUTHORIZATION_TYPE 'none'` only for a catalog configured without authentication.
For authenticated catalogs and object storage credentials, follow DuckDB's
[catalog-specific setup instructions](https://duckdb.org/docs/stable/core_extensions/iceberg/catalogs).
DuckDB must also be able to access the catalog's table storage.

With permission to create namespaces and tables, run:

```sql
CREATE SCHEMA lake.demo;
CREATE TABLE lake.demo.events (id BIGINT, data VARCHAR);
INSERT INTO lake.demo.events VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT * FROM lake.demo.events ORDER BY id;
```

Tables in an attached catalog use the name `catalog.schema.table`. The query
returns the three inserted rows. See
[writing to Iceberg](https://duckdb.org/docs/stable/core_extensions/iceberg/writing_to_iceberg)
for supported write operations and their limitations.

## Inspect table metadata

DuckDB provides table functions to inspect snapshots and manifest entries:

```sql
SELECT * FROM iceberg_snapshots('data/iceberg/lineitem_iceberg');

SELECT *
FROM iceberg_metadata(
    'data/iceberg/lineitem_iceberg',
    allow_moved_paths = true
);
```

These functions also accept a fully qualified table name from an attached catalog:

```sql
SELECT * FROM iceberg_snapshots(lake.demo.events);
SELECT * FROM iceberg_metadata(lake.demo.events);
```

The function names and output columns differ from Iceberg's metadata tables in
other engines. In particular, `iceberg_metadata` returns manifest entries,
including deleted entries, rather than only the table's current data files.
See DuckDB's [Iceberg functions reference](https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_functions)
for the available functions and their parameters.
