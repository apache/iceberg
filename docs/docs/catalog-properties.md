---
title: "Catalog properties"
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

# Catalog properties

## Common properties

Iceberg catalogs support using catalog properties to configure catalog behaviors. Here is a list of commonly used catalog properties:

| Property                          | Default            | Description                                            |
| --------------------------------- | ------------------ | ------------------------------------------------------ |
| catalog-impl                      | null               | a custom `Catalog` implementation to use by an engine  |
| io-impl                           | null               | a custom `FileIO` implementation to use in a catalog   |
| warehouse                         | null               | the root path of the data warehouse                    |
| uri                               | null               | a URI string, such as Hive metastore URI               |
| clients                           | 2                  | client pool size                                       |
| cache-enabled                     | true               | Whether to cache catalog entries |
| cache.expiration-interval-ms      | 30000              | How long catalog entries are locally cached, in milliseconds; 0 disables caching, negative values disable expiration |
| metrics-reporter-impl | org.apache.iceberg.metrics.LoggingMetricsReporter | Custom `MetricsReporter` implementation to use in a catalog. See the [Metrics reporting](metrics-reporting.md) section for additional details |
| unique-table-location             | false              | Whether to use a unique location for new tables |
| encryption.kms-impl               | null               | a custom `KeyManagementClient` implementation to use in a catalog for interactions with KMS (key management service). See the [Encryption](encryption.md) document for additional details |

`HadoopCatalog` and `HiveCatalog` can access the properties in their constructors.
Any other custom catalog can access the properties by implementing `Catalog.initialize(catalogName, catalogProperties)`.
The properties can be manually constructed or passed in from a compute engine like Spark or Flink.
Spark uses its session properties as catalog properties, see more details in the [Spark configuration](spark-configuration.md#catalog-configuration) section.
Flink passes in catalog properties through `CREATE CATALOG` statement, see more details in the [Flink](flink.md#adding-catalogs) section.

## REST catalog properties

Configuration for connecting to a REST catalog, including authentication, is
documented on the [REST catalog page](rest-catalog.md#configuration).

## Lock catalog properties

Here are the catalog properties related to locking. They are used by some catalog implementations to control the locking behavior during commits.

| Property                          | Default            | Description                                            |
| --------------------------------- | ------------------ | ------------------------------------------------------ |
| lock-impl                         | null               | a custom implementation of the lock manager, the actual interface depends on the catalog used  |
| lock.table                        | null               | an auxiliary table for locking, such as in [AWS DynamoDB lock manager](aws.md#dynamodb-lock-manager)  |
| lock.acquire-interval-ms          | 5000 (5 s)         | the interval to wait between each attempt to acquire a lock  |
| lock.acquire-timeout-ms           | 180000 (3 min)     | the maximum time to try acquiring a lock               |
| lock.heartbeat-interval-ms        | 3000 (3 s)         | the interval to wait between each heartbeat after acquiring a lock  |
| lock.heartbeat-timeout-ms         | 15000 (15 s)       | the maximum time without a heartbeat to consider a lock expired  |

## Hadoop configuration

### HadoopTables Lock Configuration

When using `HadoopTables` (tables without a catalog), lock properties from the [Lock catalog properties](#lock-catalog-properties) section can be configured by prefixing them with `iceberg.tables.hadoop.`. This ensures atomic commits on file systems like S3 that lack native write mutual exclusion.

!!! info
    To use DynamoDB as a lock manager with `HadoopTables`, set `iceberg.tables.hadoop.lock-impl` to `org.apache.iceberg.aws.dynamodb.DynamoDbLockManager` and `iceberg.tables.hadoop.lock.table` to your DynamoDB table name. See [DynamoDB Lock Manager](aws.md#dynamodb-lock-manager) for more details.

### Hive Metastore Configuration

The following properties from the Hadoop configuration are used by the Hive Metastore connector.
The HMS table locking is a 2-step process:

1. Lock Creation: Create lock in HMS and queue for acquisition
2. Lock Check: Check if lock successfully acquired

| Property                                  | Default         | Description                                                                  |
|-------------------------------------------|-----------------|------------------------------------------------------------------------------|
| iceberg.hive.client-pool-size             | 5               | The size of the Hive client pool when tracking tables in HMS                 |
| iceberg.hive.lock-creation-timeout-ms     | 180000 (3 min)  | Maximum time in milliseconds to create a lock in the HMS                     |
| iceberg.hive.lock-creation-min-wait-ms    | 50              | Minimum time in milliseconds between retries of creating the lock in the HMS |
| iceberg.hive.lock-creation-max-wait-ms    | 5000            | Maximum time in milliseconds between retries of creating the lock in the HMS |
| iceberg.hive.lock-timeout-ms              | 180000 (3 min)  | Maximum time in milliseconds to acquire a lock                               |
| iceberg.hive.lock-check-min-wait-ms       | 50              | Minimum time in milliseconds between checking the acquisition of the lock    |
| iceberg.hive.lock-check-max-wait-ms       | 5000            | Maximum time in milliseconds between checking the acquisition of the lock    |
| iceberg.hive.lock-heartbeat-interval-ms   | 240000 (4 min)  | The heartbeat interval for the HMS locks.                                    |
| iceberg.hive.metadata-refresh-max-retries | 2               | Maximum number of retries when the metadata file is missing                  |
| iceberg.hive.table-level-lock-evict-ms    | 600000 (10 min) | The timeout for the JVM table lock is                                        |
| iceberg.engine.hive.lock-enabled          | true            | Use HMS locks to ensure atomicity of commits                                 |

Note: `iceberg.hive.lock-check-max-wait-ms` and `iceberg.hive.lock-heartbeat-interval-ms` should be less than the [transaction timeout](https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-hive.txn.timeout)
of the Hive Metastore (`hive.txn.timeout` or `metastore.txn.timeout` in the newer versions). Otherwise, the heartbeats on the lock (which happens during the lock checks) would end up expiring in the
Hive Metastore before the lock is retried from Iceberg.

Warn: Setting `iceberg.engine.hive.lock-enabled`=`false` will cause HiveCatalog to commit to tables without using Hive locks.
This should only be set to `false` if all following conditions are met:

- [HIVE-26882](https://issues.apache.org/jira/browse/HIVE-26882)
is available on the Hive Metastore server
- [HIVE-28121](https://issues.apache.org/jira/browse/HIVE-28121)
is available on the Hive Metastore server, if it is backed by MySQL or MariaDB
- All other HiveCatalogs committing to tables that this HiveCatalog commits to are also on Iceberg 1.3 or later
- All other HiveCatalogs committing to tables that this HiveCatalog commits to have also disabled Hive locks on commit.

**Failing to ensure these conditions risks corrupting the table.**

Even with `iceberg.engine.hive.lock-enabled` set to `false`, a HiveCatalog can still use locks for individual tables by setting the table property `engine.hive.lock-enabled`=`true`.
This is useful in the case where other HiveCatalogs cannot be upgraded and set to commit without using Hive locks.
