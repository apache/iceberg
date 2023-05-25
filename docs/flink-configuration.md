---
title: "Flink Configuration"
url: flink-configuration
aliases:
    - "flink/flink-configuration"
menu:
    main:
        parent: Flink
        identifier: flink_configuration
        weight: 600
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

# Flink Configuration

## Catalog Configuration

A catalog is created and named by executing the following query (replace `<catalog_name>` with your catalog name and
`<config_key>`=`<config_value>` with catalog implementation config):

```sql
CREATE CATALOG <catalog_name> WITH (
  'type'='iceberg',
  `<config_key>`=`<config_value>`
); 
```

The following properties can be set globally and are not limited to a specific catalog implementation:

| Property                     | Required | Values                     | Description                                                                                                                                                                      |
| ---------------------------- |----------| -------------------------- |----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type                         | ✔️       | iceberg                    | Must be `iceberg`.                                                                                                                                                               |
| catalog-type                 |          | `hive`, `hadoop` or `rest` | `hive`, `hadoop` or `rest` for built-in catalogs, or left unset for custom catalog implementations using catalog-impl.                                                           |
| catalog-impl                 |          |                            | The fully-qualified class name of a custom catalog implementation. Must be set if `catalog-type` is unset.                                                                       |
| property-version             |          |                            | Version number to describe the property version. This property can be used for backwards compatibility in case the property format changes. The current property version is `1`. |
| cache-enabled                |          | `true` or `false`          | Whether to enable catalog cache, default value is `true`.                                                                                                                        |
| cache.expiration-interval-ms |          |                            | How long catalog entries are locally cached, in milliseconds; negative values like `-1` will disable expiration, value 0 is not allowed to set. default value is `-1`.           |

The following properties can be set if using the Hive catalog:

| Property        | Required | Values | Description                                                                                                                                                                                                                                                                                                                                                                                |
| --------------- |----------| ------ |--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| uri             | ✔️       |        | The Hive metastore's thrift URI.                                                                                                                                                                                                                                                                                                                                                           |
| clients         |          |        | The Hive metastore client pool size, default value is 2.                                                                                                                                                                                                                                                                                                                                   |
| warehouse       |          |        | The Hive warehouse location, users should specify this path if neither set the `hive-conf-dir` to specify a location containing a `hive-site.xml` configuration file nor add a correct `hive-site.xml` to classpath.                                                                                                                                                                       |
| hive-conf-dir   |          |        | Path to a directory containing a `hive-site.xml` configuration file which will be used to provide custom Hive configuration values. The value of `hive.metastore.warehouse.dir` from `<hive-conf-dir>/hive-site.xml` (or hive configure file from classpath) will be overwritten with the `warehouse` value if setting both `hive-conf-dir` and `warehouse` when creating iceberg catalog. |
| hadoop-conf-dir |          |        | Path to a directory containing `core-site.xml` and `hdfs-site.xml` configuration files which will be used to provide custom Hadoop configuration values.                                                                                                                                                                                                                                   |

The following properties can be set if using the Hadoop catalog:

| Property  | Required    | Values | Description                                                |
| --------- |-------------| ------ | ---------------------------------------------------------- |
| warehouse | ✔️          |        | The HDFS directory to store metadata files and data files. |

The following properties can be set if using the REST catalog:

| Property   | Required | Values | Description                                                                 |
| ---------- |----------| ------ |-----------------------------------------------------------------------------|
| uri        | ✔️       |        | The URL to the REST Catalog.                                                |
| credential |          |        | A credential to exchange for a token in the OAuth2 client credentials flow. |
| token      |          |        | A token which will be used to interact with the server.                     |


## Runtime configuration

### Read options

Flink read options are passed when configuring the Flink IcebergSource:

```
IcebergSource.forRowData()
    .tableLoader(TableLoader.fromCatalog(...))
    .assignerFactory(new SimpleSplitAssignerFactory())
    .streaming(true)
    .streamingStartingStrategy(StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT)
    .startSnapshotId(3821550127947089987L)
    .monitorInterval(Duration.ofMillis(10L)) // or .set("monitor-interval", "10s") \ set(FlinkReadOptions.MONITOR_INTERVAL, "10s")
    .build()
```

For Flink SQL, read options can be passed in via SQL hints like this:

```
SELECT * FROM tableName /*+ OPTIONS('monitor-interval'='10s') */
...
```

Options can be passed in via Flink configuration, which will be applied to current session. Note that not all options support this mode.

```
env.getConfig()
    .getConfiguration()
    .set(FlinkReadOptions.SPLIT_FILE_OPEN_COST_OPTION, 1000L);
...
```

`Read option` has the highest priority, followed by `Flink configuration` and then `Table property`.

| Read option                   | Flink configuration                             | Table property               | Default                          | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|-------------------------------|-------------------------------------------------|------------------------------|----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| snapshot-id                   | N/A                                             | N/A                          | null                             | For time travel in batch mode. Read data from the specified snapshot-id.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| case-sensitive                | connector.iceberg.case-sensitive                | N/A                          | false                            | If true, match column name in a case sensitive way.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| as-of-timestamp               | N/A                                             | N/A                          | null                             | For time travel in batch mode. Read data from the most recent snapshot as of the given time in milliseconds.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| starting-strategy             | connector.iceberg.starting-strategy             | N/A                          | INCREMENTAL_FROM_LATEST_SNAPSHOT | Starting strategy for streaming execution. TABLE_SCAN_THEN_INCREMENTAL: Do a regular table scan then switch to the incremental mode. The incremental mode starts from the current snapshot exclusive. INCREMENTAL_FROM_LATEST_SNAPSHOT: Start incremental mode from the latest snapshot inclusive. If it is an empty map, all future append snapshots should be discovered. INCREMENTAL_FROM_EARLIEST_SNAPSHOT: Start incremental mode from the earliest snapshot inclusive. If it is an empty map, all future append snapshots should be discovered. INCREMENTAL_FROM_SNAPSHOT_ID: Start incremental mode from a snapshot with a specific id inclusive. INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP: Start incremental mode from a snapshot with a specific timestamp inclusive. If the timestamp is between two snapshots, it should start from the snapshot after the timestamp. Just for FIP27 Source. |
| start-snapshot-timestamp      | N/A                                             | N/A                          | null                             | Start to read data from the most recent snapshot as of the given time in milliseconds.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| start-snapshot-id             | N/A                                             | N/A                          | null                             | Start to read data from the specified snapshot-id.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| end-snapshot-id               | N/A                                             | N/A                          | The latest snapshot id           | Specifies the end snapshot.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| branch                        | N/A                                             | N/A                          | main                             | Specifies the branch to read from in batch mode                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| tag                           | N/A                                             | N/A                          | null                             | Specifies the tag to read from in batch mode                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| start-tag                     | N/A                                             | N/A                          | null                             | Specifies the starting tag to read from for incremental reads                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| end-tag                       | N/A                                             | N/A                          | null                             | Specifies the ending tag to to read from for incremental reads                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| split-size                    | connector.iceberg.split-size                    | read.split.target-size       | 128 MB                           | Target size when combining input splits.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| split-lookback                | connector.iceberg.split-file-open-cost          | read.split.planning-lookback | 10                               | Number of bins to consider when combining input splits.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| split-file-open-cost          | connector.iceberg.split-file-open-cost          | read.split.open-file-cost    | 4MB                              | The estimated cost to open a file, used as a minimum weight when combining splits.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| streaming                     | connector.iceberg.streaming                     | N/A                          | false                            | Sets whether the current task runs in streaming or batch mode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| monitor-interval              | connector.iceberg.monitor-interval              | N/A                          | 60s                              | Monitor interval to discover splits from new snapshots. Applicable only for streaming read.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| include-column-stats          | connector.iceberg.include-column-stats          | N/A                          | false                            | Create a new scan from this that loads the column stats with each data file. Column stats include: value count, null value count, lower bounds, and upper bounds.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| max-planning-snapshot-count   | connector.iceberg.max-planning-snapshot-count   | N/A                          | Integer.MAX_VALUE                | Max number of snapshots limited per split enumeration. Applicable only to streaming read.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| limit                         | connector.iceberg.limit                         | N/A                          | -1                               | Limited output number of rows.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| max-allowed-planning-failures | connector.iceberg.max-allowed-planning-failures | N/A                          | 3                                | Max allowed consecutive failures for scan planning before failing the job. Set to -1 for never failing the job for scan planing failure.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |


### Write options

Flink write options are passed when configuring the FlinkSink, like this:

```
FlinkSink.Builder builder = FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
    .table(table)
    .tableLoader(tableLoader)
    .set("write-format", "orc")
    .set(FlinkWriteOptions.OVERWRITE_MODE, "true");
```

For Flink SQL, write options can be passed in via SQL hints like this:

```
INSERT INTO tableName /*+ OPTIONS('upsert-enabled'='true') */
...
```

| Flink option           | Default                                    | Description                                                  |
| ---------------------- | ------------------------------------------ | ------------------------------------------------------------ |
| write-format           | Table write.format.default                 | File format to use for this write operation; parquet, avro, or orc |
| target-file-size-bytes | As per table property                      | Overrides this table's write.target-file-size-bytes          |
| upsert-enabled         | Table write.upsert.enabled                 | Overrides this table's write.upsert.enabled                  |
| overwrite-enabled      | false                                      | Overwrite the table's data, overwrite mode shouldn't be enable when configuring to use UPSERT data stream. |
| distribution-mode      | Table write.distribution-mode              | Overrides this table's write.distribution-mode               |
| compression-codec      | Table write.(fileformat).compression-codec | Overrides this table's compression codec for this write      |
| compression-level      | Table write.(fileformat).compression-level | Overrides this table's compression level for Parquet and Avro tables for this write |
| compression-strategy   | Table write.orc.compression-strategy       | Overrides this table's compression strategy for ORC tables for this write |
| write-parallelism      | Upstream operator parallelism              | Overrides the writer parallelism                             |
