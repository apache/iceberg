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

# DuckDB

[DuckDB](https://duckdb.org/) is an in-process analytical SQL database. Its
[`iceberg` extension](https://duckdb.org/docs/current/core_extensions/iceberg/overview.html)
can read Iceberg tables directly from storage or connect to an Iceberg REST catalog
to read and write tables by name.

## Supported features

| Operation | Direct table access | Attached REST catalog |
| --- | --- | --- |
| Query table data and historical snapshots | Yes | Yes |
| Inspect snapshots, manifest entries, and statistics | Yes | Yes |
| Create and drop namespaces and tables | No | Yes |
| Insert, update, delete, and merge rows | No | Yes |
| Evolve schemas and configure table properties | No | Yes |

Support depends on the DuckDB version, extension version, catalog, and table
features. See [writing to Iceberg](https://duckdb.org/docs/current/core_extensions/iceberg/writing_to_iceberg.html)
for supported operations and [limitations](#limitations).

## Table formats and storage

DuckDB reads Iceberg metadata and follows its manifests to the table's data and
delete files. Reading the underlying Parquet files directly does not apply
Iceberg's snapshot selection or deletes.

The extension supports Iceberg v3 features, including deletion vectors and row
lineage, in addition to v2 tables. Check DuckDB's
[format support](https://duckdb.org/docs/current/core_extensions/iceberg/writing_to_iceberg.html#iceberg-v3-support)
before using newer types or upgrading an existing table's format version.

Direct reads can use local files or object storage through DuckDB's
[`httpfs`](https://duckdb.org/docs/stable/core_extensions/httpfs/overview) and
[`azure`](https://duckdb.org/docs/stable/core_extensions/azure) extensions.
Catalog and storage access are separate: a successful catalog connection does not
necessarily grant access to its data files.

## Enabling Iceberg support in DuckDB

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

## Catalogs

DuckDB attaches catalogs through the Iceberg REST protocol. Catalog-specific
configuration is available for Apache Polaris, Lakekeeper, AWS Glue, Amazon S3
Tables, and other services in DuckDB's
[catalog documentation](https://duckdb.org/docs/current/core_extensions/iceberg/catalogs.html).
An existing Hive or JDBC catalog needs a REST service in front of it; its native
connection settings cannot be used as a REST endpoint.

### Local development catalog

For a local example, install Docker and start Iceberg's REST catalog test fixture
from a terminal on the same machine as the DuckDB CLI:

```sh
mkdir -p warehouse
docker run --rm --name iceberg-duckdb-catalog \
    -p 127.0.0.1:8181:8181 \
    -v "$PWD/warehouse:$PWD/warehouse" \
    -e CATALOG_WAREHOUSE="$PWD/warehouse" \
    apache/iceberg-rest-fixture
```

Keep that terminal running. The identical host and container paths let DuckDB read
the files referenced by the catalog. The container user must be able to write to
the mounted directory. This fixture is for testing, not production; removing the
container also removes its catalog database, even though the warehouse files remain.

In another terminal, start `duckdb`, load the extension, and attach the catalog:

```sql
ATTACH 'warehouse' AS lake (
    TYPE iceberg,
    ENDPOINT 'http://localhost:8181',
    AUTHORIZATION_TYPE 'none'
);
```

Use `AUTHORIZATION_TYPE 'none'` only for a catalog configured without authentication.
For an existing catalog, replace the endpoint and warehouse with its values.

### Authenticated catalogs

For an OAuth2 catalog, store the credentials in a DuckDB secret and reference it
when attaching. This is an alternative to the local development connection:

```sql
CREATE SECRET catalog_credentials (
    TYPE iceberg,
    CLIENT_ID 'your-client-id',
    CLIENT_SECRET 'your-client-secret',
    OAUTH2_SERVER_URI 'https://catalog.example.com/v1/oauth/tokens'
);

ATTACH 'your-warehouse' AS lake (
    TYPE iceberg,
    SECRET catalog_credentials,
    ENDPOINT 'https://catalog.example.com'
);
```

Replace the example values with your catalog's settings. The token endpoint and
required OAuth2 scope depend on the provider. A catalog-issued bearer token can
instead be supplied with the secret's `TOKEN` option. AWS catalogs can use SigV4
authentication. Follow the provider-specific instructions linked above rather than
reusing the OAuth2 example for every catalog.

### Storage credentials

If the catalog vends storage credentials, DuckDB can use those credentials to
access the table files. Otherwise, configure storage access separately. For S3,
DuckDB can use your configured AWS credential chain:

```sql
CREATE SECRET storage_credentials (
    TYPE s3,
    PROVIDER credential_chain
);
```

Configure the region and, for S3-compatible stores, the endpoint, URL style, and TLS
settings as required by your provider. See DuckDB's
[S3 authentication documentation](https://duckdb.org/docs/stable/core_extensions/httpfs/s3api#configuration-and-authentication).
Do not store production credentials in scripts or version control.

## Querying Iceberg using DuckDB

### Reading a table without a catalog

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
For tables in object storage, configure [storage credentials](#storage-credentials)
and pass the metadata file's URI instead of a local path.

### Creating and reading catalog tables

With the `lake` catalog attached and permission to create namespaces and tables, run:

```sql
CREATE SCHEMA lake.demo;
CREATE TABLE lake.demo.events (id BIGINT, data VARCHAR);
INSERT INTO lake.demo.events VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT * FROM lake.demo.events ORDER BY id;
```

Tables in an attached catalog use the name `catalog.schema.table`. The query
returns `(1, 'a')`, `(2, 'b')`, and `(3, 'c')`. To discover existing tables, use
`SHOW ALL TABLES;`.

### Time travel

List the sample table's snapshots with `iceberg_snapshots`, then select a snapshot
ID or timestamp. These two queries read its first snapshot, containing 60,175 rows:

```sql
SELECT count(*)
FROM iceberg_scan(
    'data/iceberg/lineitem_iceberg',
    snapshot_from_id = 3776207205136740581,
    allow_moved_paths = true
);

SELECT count(*)
FROM iceberg_scan(
    'data/iceberg/lineitem_iceberg',
    snapshot_from_timestamp = TIMESTAMP '2023-02-15 15:08:00',
    allow_moved_paths = true
);
```

The snapshot ID is specific to the downloaded sample. For an attached table, use
DuckDB's `AT` clause. Capture a snapshot before changing the example table:

```sql
SET VARIABLE initial_snapshot = (
    SELECT snapshot_id FROM iceberg_snapshots(lake.demo.events)
    ORDER BY sequence_number LIMIT 1
);

SELECT * FROM lake.demo.events
AT (VERSION => getvariable('initial_snapshot'))
ORDER BY id;

SET VARIABLE initial_timestamp = (
    SELECT timestamp_ms FROM iceberg_snapshots(lake.demo.events)
    ORDER BY sequence_number LIMIT 1
);

SELECT * FROM lake.demo.events
AT (TIMESTAMP => getvariable('initial_timestamp'))
ORDER BY id;
```

The same snapshot can be queried by ID or timestamp after the updates below.
Historical queries require the snapshot and its files to remain available;
expired snapshots cannot be queried.

### Updating, deleting, and merging rows

These statements modify the catalog table created above:

```sql
UPDATE lake.demo.events SET data = 'updated' WHERE id = 1;
DELETE FROM lake.demo.events WHERE id = 2;

MERGE INTO lake.demo.events AS target
USING (VALUES (3, 'merged'), (4, 'new')) AS source(id, data)
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET data = source.data
WHEN NOT MATCHED THEN INSERT (id, data) VALUES (source.id, source.data);

SELECT * FROM lake.demo.events ORDER BY id;
```

The current table contains `(1, 'updated')`, `(3, 'merged')`, and `(4, 'new')`.
Querying `initial_snapshot` still returns the three original rows. These operations
use merge-on-read semantics, subject to the [limitations](#limitations) below.

### Schema evolution and table properties

Use `ALTER TABLE` to add, rename, or drop columns without rewriting existing data:

```sql
ALTER TABLE lake.demo.events ADD COLUMN source VARCHAR;
ALTER TABLE lake.demo.events RENAME COLUMN source TO origin;
ALTER TABLE lake.demo.events DROP COLUMN origin;

ALTER TABLE lake.demo.events SET ('owner' = 'analytics');
SELECT * FROM iceberg_table_properties(lake.demo.events);
ALTER TABLE lake.demo.events RESET ('owner');
```

Table properties can also be supplied in a `WITH` clause at creation time. For
example, `WITH ('format-version' = '3')` selects the v3 table format. Confirm that
all engines using a table support its format and features before upgrading it.

### Partitioning

DuckDB supports creating tables with Iceberg partition transforms. For example:

```sql
CREATE TABLE lake.demo.partitioned_events (
    id BIGINT,
    country VARCHAR,
    data VARCHAR
)
PARTITIONED BY (bucket(16, id), truncate(2, country));

INSERT INTO lake.demo.partitioned_events
VALUES (1, 'NL', 'a'), (2, 'US', 'b');

SELECT * FROM lake.demo.partitioned_events WHERE id = 1;
```

Query the source columns normally; callers do not need to supply bucket values.
DuckDB uses Iceberg metadata to prune files based on query filters. See DuckDB's
[partitioning documentation](https://duckdb.org/docs/current/core_extensions/iceberg/writing_to_iceberg.html#partition-transforms)
for supported transforms and restrictions.

### Inspecting table metadata

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

Additional inspection functions include:

| Function | Information |
| --- | --- |
| `iceberg_column_stats` | Per-file, per-column statistics, including lower and upper bounds |
| `iceberg_partition_stats` | Partition-field summaries from manifests, not table-wide partition totals |
| `iceberg_table_properties` | Table properties from an attached catalog |
| `iceberg_schema_properties` | Namespace properties from an attached catalog |

For example:

```sql
SELECT * FROM iceberg_column_stats(lake.demo.events);
SELECT * FROM iceberg_partition_stats(lake.demo.partitioned_events);
SELECT * FROM iceberg_schema_properties(lake.demo);
```

For an attached table, `iceberg_load_table_response` returns the REST catalog's
table response, including the metadata location.

See DuckDB's [functions reference](https://duckdb.org/docs/current/core_extensions/iceberg/reference.html)
for available functions and parameters. Work toward Iceberg-compatible metadata
tables is tracked in [duckdb/duckdb-iceberg#1587](https://github.com/duckdb/duckdb-iceberg/issues/1587);
do not assume every metadata table available in Spark or PyIceberg has a DuckDB
equivalent.

### Python

The same SQL can be run through DuckDB's Python client. Install it with
`pip install duckdb`, then query the downloaded sample from the extraction directory:

```python
import duckdb

with duckdb.connect() as connection:
    connection.execute("INSTALL iceberg; LOAD iceberg;")
    result = connection.execute("""
        SELECT count(*)
        FROM iceberg_scan(
            'data/iceberg/lineitem_iceberg', allow_moved_paths = true
        )
    """).fetchone()
    print(result[0])
```

This prints `51793`. To query a catalog from Python, configure the catalog
attachment and credentials in the Python session as well. See the
[Python client documentation](https://duckdb.org/docs/stable/clients/python/overview)
for retrieving results as Arrow tables or DataFrames.

### Copying data and DuckLake interoperability

For a data copy between attached DuckDB and Iceberg databases, DuckDB supports
`COPY FROM DATABASE source_database TO destination_database`. This reads and
writes data; it is not a metadata-only conversion.

For DuckLake, `iceberg_to_ducklake` provides a metadata-only copy into an attached
DuckLake catalog. This is a separate operation, not a migration of the original
Iceberg catalog. See DuckDB's
[DuckLake interoperability documentation](https://duckdb.org/docs/current/core_extensions/iceberg/overview.html#interoperability-with-ducklake)
for setup and examples.

## Limitations

- Direct reads cannot commit writes. Prefer catalog access to follow the current
  committed table state. Guessing the latest metadata filename can read an
  uncommitted version and should not replace catalog resolution.
- `UPDATE` and `DELETE` do not support sorted tables. Row mutations use
  merge-on-read, not copy-on-write. Tables configured with incompatible
  `write.update.mode` or `write.delete.mode` values must be configured appropriately
  before those operations can succeed.
- V2 mutations write positional deletes; v3 mutations use deletion vectors.
  Do not infer support for every Iceberg feature from support for a format version.
- DuckDB's current write documentation lists `GEOGRAPHY` and `UNKNOWN` as
  unsupported. Check the
  [write reference](https://duckdb.org/docs/current/core_extensions/iceberg/writing_to_iceberg.html)
  for type support and restrictions in your version.
- Catalog implementations differ in support for staged creates, multi-table
  commits, nested namespaces, and credential vending. Use DuckDB's
  [catalog compatibility options](https://duckdb.org/docs/current/core_extensions/iceberg/catalogs.html#catalogs-with-limited-rest-spec-support)
  when required by your provider.

## Troubleshooting and further reading

For authentication failures, check both catalog and storage credentials, their
scopes, regions, and expiration. For missing files during a direct read, check the
metadata location and storage access before enabling path relocation or version
guessing. Keep the extension up to date when diagnosing compatibility problems.

- [DuckDB Iceberg overview](https://duckdb.org/docs/current/core_extensions/iceberg/overview.html)
- [Catalog configuration](https://duckdb.org/docs/current/core_extensions/iceberg/catalogs.html)
- [Writing to Iceberg](https://duckdb.org/docs/current/core_extensions/iceberg/writing_to_iceberg.html)
- [Functions and settings](https://duckdb.org/docs/current/core_extensions/iceberg/reference.html)
- [Extension options](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_options.html)
- [Troubleshooting](https://duckdb.org/docs/current/core_extensions/iceberg/troubleshooting.html)
- [DuckDB Iceberg extension repository](https://github.com/duckdb/duckdb-iceberg)
