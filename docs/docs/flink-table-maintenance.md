---
title: "Flink Table Maintenance "
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

# Flink Table Maintenance 

## Overview

In **Apache Iceberg** deployments within **Flink streaming environments**, implementing automated table maintenance operations—including `snapshot expiration`, `small file compaction`, and `orphan file cleanup`—is critical for optimal query performance and storage efficiency.

Traditionally, these maintenance operations were exclusively accessible through **Iceberg Spark Actions**, necessitating the deployment and management of dedicated Spark clusters. This dependency on **Spark infrastructure** solely for table optimization introduces significant **architectural complexity** and **operational overhead**.

The `TableMaintenance` API in **Apache Iceberg** empowers **Flink jobs** to execute maintenance tasks **natively**, either embedded within existing streaming pipelines or deployed as standalone Flink jobs. This eliminates dependencies on external systems, thereby **streamlining architecture**, **reducing operational costs**, and **enhancing automation capabilities**.

## Supported Features (Flink)

### ExpireSnapshots
Removes old snapshots and their files. Internally uses `cleanExpiredFiles(true)` when committing, so expired metadata/files are cleaned up automatically.

```java
.add(ExpireSnapshots.builder()
    .maxSnapshotAge(Duration.ofDays(7))
    .retainLast(10)
    .deleteBatchSize(1000))
```

### RewriteDataFiles
Compacts small files to optimize file sizes. Supports partial progress commits and limiting maximum rewritten bytes per run.

```java
.add(RewriteDataFiles.builder()
    .targetFileSizeBytes(256 * 1024 * 1024)
    .minFileSizeBytes(32 * 1024 * 1024)
    .partialProgressEnabled(true)
    .partialProgressMaxCommits(5))
```

## Lock Management

The `TriggerLockFactory` is essential for coordinating maintenance tasks across multiple Flink jobs or instances. It prevents concurrent maintenance operations on the same table, which could lead to conflicts or data corruption.

### Why Locks Are Needed
- **Concurrent Access**: Multiple Flink jobs may attempt maintenance simultaneously
- **Data Consistency**: Ensures only one maintenance operation runs per table at a time
- **Resource Management**: Prevents resource conflicts and scheduling issues
- **Avoid Duplicate Work**: Even when only a single compaction job is scheduled, multiple instances could attempt the same operation, leading to redundant work and wasted resources.

### Supported Lock Types

#### JDBC Lock Factory
Uses a database table to manage distributed locks:

```java
Map<String, String> jdbcProps = new HashMap<>();
jdbcProps.put("jdbc.user", "flink");
jdbcProps.put("jdbc.password", "flinkpw");

TriggerLockFactory lockFactory = new JdbcLockFactory(
    "jdbc:postgresql://localhost:5432/iceberg", // JDBC URL
    "catalog.db.table",                         // Lock ID (unique identifier)
    jdbcProps                                   // JDBC connection properties
);
```

#### ZooKeeper Lock Factory
Uses Apache ZooKeeper for distributed coordination:

```java
TriggerLockFactory lockFactory = new ZkLockFactory(
    "localhost:2181",        // ZooKeeper connection string
    "catalog.db.table"       // Lock ID (unique identifier)
);
```

## Quick Start

The following example demonstrates the implementation of automated maintenance for an Iceberg table within a Flink environment.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
TableLoader tableLoader = TableLoader.fromHadoopTable("path/to/table");

Map<String, String> jdbcProps = new HashMap<>();
jdbcProps.put("jdbc.user", "flink");
jdbcProps.put("jdbc.password", "flinkpw");

// JdbcLockFactory Example
TriggerLockFactory lockFactory = new JdbcLockFactory(
    "jdbc:postgresql://localhost:5432/iceberg", // JDBC URL
    "catalog.db.table",                         // Lock ID (unique identifier)
    jdbcProps                                   // JDBC connection properties
);

TableMaintenance.forTable(env, tableLoader, lockFactory)
    .uidSuffix("my-maintenance-job")
    .rateLimit(Duration.ofMinutes(10))
    .lockCheckDelay(Duration.ofSeconds(10))
    .add(ExpireSnapshots.builder()
        .scheduleOnCommitCount(10)
        .maxSnapshotAge(Duration.ofMinutes(10))
        .retainLast(5)
        .deleteBatchSize(5)
        .parallelism(8))
    .add(RewriteDataFiles.builder()
        .scheduleOnDataFileCount(10)
        .targetFileSizeBytes(128 * 1024 * 1024)
        .partialProgressEnabled(true)
        .partialProgressMaxCommits(10))
    .append();

env.execute("Table Maintenance Job");
```

## Configuration Options

### TableMaintenance Builder

| Method | Description | Default |
|--------|-------------|---------|
| `uidSuffix(String)` | Unique identifier suffix for the job | Random UUID |
| `rateLimit(Duration)` | Minimum interval between task executions | 60 seconds |
| `lockCheckDelay(Duration)` | Delay for checking lock availability | 30 seconds |
| `parallelism(int)` | Default parallelism for maintenance tasks | System default |
| `maxReadBack(int)` | Max snapshots to check during initialization | 100 |

### Maintenance Task Common Options

| Method | Description | Default Value | Type |
|--------|-------------|---------------|------|
| `scheduleOnCommitCount(int)` | Trigger after N commits | No automatic scheduling | int |
| `scheduleOnDataFileCount(int)` | Trigger after N data files | No automatic scheduling | int |
| `scheduleOnDataFileSize(long)` | Trigger after total data file size (bytes) | No automatic scheduling | long |
| `scheduleOnPosDeleteFileCount(int)` | Trigger after N positional delete files | No automatic scheduling | int |
| `scheduleOnPosDeleteRecordCount(long)` | Trigger after N positional delete records | No automatic scheduling | long |
| `scheduleOnEqDeleteFileCount(int)` | Trigger after N equality delete files | No automatic scheduling | int |
| `scheduleOnEqDeleteRecordCount(long)` | Trigger after N equality delete records | No automatic scheduling | long |
| `scheduleOnInterval(Duration)` | Trigger after time interval | No automatic scheduling | Duration |

### ExpireSnapshots Configuration

| Method | Description | Default Value | Type |
|--------|-------------|---------------|------|
| `maxSnapshotAge(Duration)` | Maximum age of snapshots to retain | None | Duration |
| `retainLast(int)` | Minimum number of snapshots to retain | None | int |
| `deleteBatchSize(int)` | Number of files to delete in each batch | 1000 | int |
| `planningWorkerPoolSize(int)` | Number of worker threads for planning snapshot expiration | Shared worker pool | int |
| `cleanExpiredMetadata(boolean)` | Remove expired metadata files when expiring snapshots | true | boolean |

### RewriteDataFiles Configuration

| Method | Description | Default Value | Type |
|--------|-------------|---------------|------|
| `targetFileSizeBytes(long)` | Target size for rewritten files | Table property or 512MB | long |
| `minFileSizeBytes(long)` | Minimum size of files eligible for compaction | 0 | long |
| `maxFileSizeBytes(long)` | Maximum size of files eligible for compaction | Long.MAX_VALUE | long |
| `minInputFiles(int)` | Minimum number of files to trigger rewrite | Integer.MAX_VALUE | int |
| `deleteFileThreshold(int)` | Minimum delete-file count per data file to force rewrite | Integer.MAX_VALUE | int |
| `rewriteAll(boolean)` | Rewrite all data files regardless of thresholds | false | boolean |
| `maxFileGroupSizeBytes(long)` | Maximum total size of a file group | 107374182400 (100GB) | long |
| `maxFilesToRewrite(int)` | Maximum number of files to rewrite per task | Integer.MAX_VALUE | int |
| `partialProgressEnabled(boolean)` | Enable partial progress commits | false | boolean |
| `partialProgressMaxCommits(int)` | Maximum commits for partial progress | 10 | int |
| `maxRewriteBytes(long)` | Maximum bytes to rewrite per execution | Long.MAX_VALUE | long |

## Complete Example

```java
public class TableMaintenanceJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000); // Enable checkpointing
        
        // Configure table loader
        TableLoader tableLoader = TableLoader.fromCatalog(
            CatalogLoader.hive("my_catalog", configuration),
            TableIdentifier.of("database", "table")
        );
        
        // Set up maintenance with comprehensive configuration
        TableMaintenance.forTable(env, tableLoader, TriggerLockFactory.defaultLockFactory())
            .uidSuffix("production-maintenance")
            .rateLimit(Duration.ofMinutes(15))
            .lockCheckDelay(Duration.ofSeconds(30))
            .parallelism(4)
            
            // Daily snapshot cleanup
            .add(ExpireSnapshots.builder()
                .maxSnapshotAge(Duration.ofDays(7))
                .retainLast(10))
            
            // Continuous file optimization
            .add(RewriteDataFiles.builder()
                .targetFileSizeBytes(256 * 1024 * 1024)
                .minFileSizeBytes(32 * 1024 * 1024)
                .scheduleOnDataFileCount(20)
                .partialProgressEnabled(true)
                .partialProgressMaxCommits(5)
                .maxRewriteBytes(2L * 1024 * 1024 * 1024)
                .parallelism(6))
                
            .append();
        
        env.execute("Iceberg Table Maintenance");
    }
}
```

## IcebergSink with Post-Commit Integration

Apache Iceberg Sink V2 for Flink allows automatic execution of maintenance tasks after data is committed to the table, using the addPostCommitTopology(...) method.

```java
Map<String, String> flinkConf = new HashMap<>();

// Enable compaction and maintenance features
flinkConf.put(FlinkWriteOptions.COMPACTION_ENABLE.key(), "true");

// Configure JDBC lock settings
flinkConf.put(LockConfig.LOCK_TYPE_OPTION.key(), LockConfig.JdbcLockConfig.JDBC);
flinkConf.put(LockConfig.JdbcLockConfig.JDBC_URI_OPTION.key(), "jdbc:postgresql://localhost:5432/iceberg");
flinkConf.put(LockConfig.LOCK_ID_OPTION.key(), "catalog.db.table");

// Add any other maintenance-related options here as needed
// ...

IcebergSink.forRowData(dataStream)
    .table(table)
    .tableLoader(tableLoader)
    .setAll(flinkConf)
    .append();
```

### SQL Examples

You can enable maintenance and configure locks using SQL before executing writes:

```sql
-- Enable Iceberg V2 Sink and compaction (maintenance)
SET 'table.exec.iceberg.use.v2.sink' = 'true';
SET 'compaction-enabled' = 'true';

-- Configure maintenance lock (JDBC)
SET 'flink-maintenance.lock.type' = 'jdbc';
SET 'flink-maintenance.lock.lock-id' = 'catalog.db.table';
SET 'flink-maintenance.lock.jdbc.uri' = 'jdbc:postgresql://localhost:5432/iceberg';
SET 'flink-maintenance.lock.jdbc.init-lock-table' = 'true';

-- Now run writes; maintenance will be scheduled post-commit
INSERT INTO db.tbl SELECT ...;
```

Or specify options in table DDL:

```sql
CREATE TABLE db.tbl (
  ...
) WITH (
  'connector' = 'iceberg',
  'catalog-name' = 'my_catalog',
  'namespace' = 'db',
  'table' = 'tbl',
  'compaction-enabled' = 'true',

  'flink-maintenance.lock.type' = 'jdbc',
  'flink-maintenance.lock.lock-id' = 'catalog.db.table',
  'flink-maintenance.lock.jdbc.uri' = 'jdbc:postgresql://localhost:5432/iceberg',
  'flink-maintenance.lock.jdbc.init-lock-table' = 'true'
);
```

## Lock Configuration (SQL)

These keys are used in SQL (SET or table WITH options) and are applicable when writing with compaction enabled.

- JDBC

| Key | Description | Default |
|-----|-------------|---------|
| `flink-maintenance.lock.type` | Set to `jdbc` |  |
| `flink-maintenance.lock.lock-id` | Unique lock ID per table |  |
| `flink-maintenance.lock.jdbc.uri` | JDBC URI |  |
| `flink-maintenance.lock.jdbc.init-lock-table` | Auto-create lock table | `false` |

- ZooKeeper

| Key | Description | Default |
|-----|-------------|---------|
| `flink-maintenance.lock.type` | Set to `zookeeper` |  |
| `flink-maintenance.lock.lock-id` | Unique lock ID per table |  |
| `flink-maintenance.lock.zookeeper.uri` | ZK connection URI |  |
| `flink-maintenance.lock.zookeeper.session-timeout-ms` | Session timeout (ms) | `60000` |
| `flink-maintenance.lock.zookeeper.connection-timeout-ms` | Connection timeout (ms) | `15000` |
| `flink-maintenance.lock.zookeeper.max-retries` | Max retries | `3` |
| `flink-maintenance.lock.zookeeper.base-sleep-ms` | Base sleep between retries (ms) | `3000` |

## Best Practices

### Resource Management
- Use dedicated slot sharing groups for maintenance tasks
- Set appropriate parallelism based on cluster resources
- Enable checkpointing for fault tolerance

### Scheduling Strategy
- Avoid too frequent executions with `rateLimit`
- Use `scheduleOnCommitCount` for write-heavy tables
- Use `scheduleOnDataFileCount` for fine-grained control

### Performance Tuning
- Adjust `deleteBatchSize` based on storage performance
- Enable `partialProgressEnabled` for large rewrite operations
- Set reasonable `maxRewriteBytes` limits

## Troubleshooting

### OutOfMemoryError during file deletion
**Scenario:** This can occur when the maintenance task attempts to delete a very large number of files in a single batch, especially in tables with long retention histories or after bulk deletions.
**Cause:** Each file deletion involves metadata and object store operations, which together can consume significant memory. Large batches magnify this effect and may exhaust the JVM heap.
**Recommendation:** Reduce the batch size to limit memory usage during deletion.
```java
.deleteBatchSize(500) // Example: 500 files per batch
```

### Lock conflicts
**Scenario:** In multi-job or high-availability environments, two or more Flink jobs may attempt maintenance on the same table simultaneously.
**Cause:** Concurrent jobs compete for the same distributed lock, causing retries and possible delays.
**Recommendation:** Increase lock check delay and rate limit so that failed attempts back off and reduce contention.
```java
.lockCheckDelay(Duration.ofMinutes(1)) // Wait longer before re-checking lock
.rateLimit(Duration.ofMinutes(10))     // Reduce frequency of task execution
```

### Slow rewrite operations
**Scenario:** Large tables with many small files can require rewriting terabytes of data in a single run, which may overwhelm available resources.
**Cause:** Without limits, rewrite tasks attempt to process all eligible files at once, leading to long execution times and possible job failures.
**Recommendation:** Enable partial progress so that rewritten files can be committed in smaller batches, and cap the maximum data rewritten in each execution.
```java
.partialProgressEnabled(true) // Commit progress incrementally
.partialProgressMaxCommits(3) // Allow up to 3 commits per run
.maxRewriteBytes(1L * 1024 * 1024 * 1024) // Limit to ~1GB per run
```
```
