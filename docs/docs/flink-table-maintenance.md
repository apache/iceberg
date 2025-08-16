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

In **Apache Iceberg** deployments within **Flink streaming environments**, implementing automated table maintenance operations—including `snapshot expiration`, `small file compaction`, and `orphan file cleanup`—is critical for optimal performance and storage efficiency.

Traditionally, these maintenance operations were exclusively accessible through **Iceberg Spark Actions**, necessitating the deployment and management of dedicated Spark clusters. This dependency on **Spark infrastructure** solely for table optimization introduces significant **architectural complexity** and **operational overhead**.

The `TableMaintenance` API in **Apache Iceberg** empowers **Flink jobs** to execute maintenance tasks **natively**, either embedded within existing streaming pipelines or deployed as standalone Flink jobs. This eliminates dependencies on external systems, thereby **streamlining architecture**, **reducing operational costs**, and **enhancing automation capabilities**.

## Quick Start

The following example demonstrates the implementation of automated maintenance for an Iceberg table within a Flink environment.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
TableLoader tableLoader = TableLoader.fromHadoopTable("path/to/table");

Map<String, String> jdbcProps = new HashMap<>();
jdbcProps.put("user", "flink");
jdbcProps.put("password", "flinkpw");

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
| `scheduleOnIntervalSecond(long)` | Trigger after time interval (seconds) | No automatic scheduling | long |
| `parallelism(int)` | Parallelism for this specific task | Inherits from TableMaintenance | int |

### ExpireSnapshots Configuration

| Method | Description | Default Value | Type |
|--------|-------------|---------------|------|
| `maxSnapshotAge(Duration)` | Maximum age of snapshots to retain | 5 days | Duration |
| `retainLast(int)` | Minimum number of snapshots to retain | 1 | int |
| `deleteBatchSize(int)` | Number of files to delete in each batch | 1000 | int |
| `planningWorkerPoolSize(int)` | Number of worker threads for planning snapshot expiration | System default | int |
| `cleanExpiredMetadata(boolean)` | Remove expired metadata files when expiring snapshots | true | boolean |

### RewriteDataFiles Configuration

| Method | Description | Default Value | Type |
|--------|-------------|---------------|------|
| `targetFileSizeBytes(long)` | Target size for rewritten files | Table property or 512MB | long |
| `minFileSizeBytes(long)` | Minimum size of files eligible for compaction | 0 | long |
| `maxFileSizeBytes(long)` | Maximum size of files eligible for compaction | Long.MAX_VALUE | long |
| `minInputFiles(int)` | Minimum number of files to trigger rewrite | 1 | int |
| `deleteFileThreshold(double)` | Fraction of delete files to trigger rewrite | 0.0 | double |
| `rewriteAll(boolean)` | Rewrite all data files regardless of thresholds | false | boolean |
| `maxFileGroupSizeBytes(long)` | Maximum total size of a file group | Long.MAX_VALUE | long |
| `maxFilesToRewrite(int)` | Maximum number of files to rewrite per task | Integer.MAX_VALUE | int |
| `filter(Expression)` | Filter expression for selecting files to rewrite | None | Expression |
| `partialProgressEnabled(boolean)` | Enable partial progress commits, allowing compacted data files to be committed in batches. | false | boolean |
| `partialProgressMaxCommits(int)` | Maximum commits for partial progress | 10 | int |
| `maxRewriteBytes(long)` | Maximum bytes to rewrite per execution | Long.MAX_VALUE | long |


### Scheduling Examples

You can configure scheduling triggers as shown below:

- Time-based scheduling
```java
RewriteDataFiles.builder()
    .scheduleOnIntervalSecond(600)
```

- Commit count based scheduling
```java
RewriteDataFiles.builder()
    .scheduleOnCommitCount(50)
```

- Data volume based scheduling
```java
RewriteDataFiles.builder()
    .scheduleOnDataFileCount(500)
    .scheduleOnDataFileSize(50L * 1024 * 1024 * 1024)
```


## Flink Configuration Options

The following configuration keys can be used to control Iceberg table maintenance behavior when running in Flink.  
They are read from Flink's `Configuration` and applied during the initialization of `TableMaintenance` tasks.

You can set these options in:

- **Flink cluster configuration** (`flink-conf.yaml`)
- **Programmatically** via `StreamExecutionEnvironment` / `Configuration`
- **SQL Client** using `SET` statements before executing maintenance commands

| Configuration Key | Description | Default Value | Type |
|-------------------|-------------|---------------|------|
| `iceberg.maintenance.rate-limit-seconds` | Minimum interval (seconds) between maintenance task executions | 60 | long |
| `iceberg.maintenance.lock-check-delay-seconds` | Delay (seconds) before re-checking lock availability | 30 | long |
| `iceberg.maintenance.rewrite.max-bytes` | Maximum bytes to rewrite per maintenance execution | Long.MAX_VALUE | long |
| `iceberg.maintenance.rewrite.schedule.commit-count` | Trigger rewrite after N commits | 10 | int |
| `iceberg.maintenance.rewrite.schedule.data-file-count` | Trigger rewrite after N data files | 1000 | int |
| `iceberg.maintenance.rewrite.schedule.data-file-size` | Trigger rewrite after total data file size (bytes) | 100GB | long |
| `iceberg.maintenance.rewrite.schedule.interval-second` | Trigger rewrite after fixed interval (seconds) | 600 | long |

### Example: Programmatic Configuration
```java
Configuration conf = new Configuration();
conf.setLong("iceberg.maintenance.rate-limit-seconds", 120);
conf.setLong("iceberg.maintenance.lock-check-delay-seconds", 60);

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
```
```sql
SET 'iceberg.maintenance.rate-limit-seconds' = '120';
SET 'iceberg.maintenance.lock-check-delay-seconds' = '60';
```


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
                .retainLast(10)
                .deleteBatchSize(1000)
                .scheduleOnCommitCount(50)
                .parallelism(2))
            
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
flinkConf.put(LockConfig.JdbcLockConfig.JDBC_USER_OPTION.key(), "flink");
flinkConf.put(LockConfig.JdbcLockConfig.JDBC_PASSWORD_OPTION.key(), "flinkpw");
flinkConf.put(LockConfig.LOCK_ID_OPTION.key(), "catalog.db.table");

// Add any other maintenance-related options here as needed
// ...

IcebergSink.forRowData(dataStream)
    .table(table)
    .tableLoader(tableLoader)
    .setAll(flinkConf)
    .append();
```

This approach executes maintenance tasks in the same job as the sink, enabling real-time optimization without running a separate job.

## Lock Configuration Example

Iceberg uses a locking mechanism to prevent multiple Flink jobs from performing maintenance on the same table simultaneously. Locks are provided via the TriggerLockFactory and support either JDBC or ZooKeeper backends.

### JDBC Lock Example
```properties
flink-maintenance.lock.type=jdbc
flink-maintenance.lock.lock-id=catalog.db.table
flink-maintenance.lock.jdbc.uri=jdbc:postgresql://localhost:5432/iceberg
flink-maintenance.lock.jdbc.user=flink
flink-maintenance.lock.jdbc.password=flinkpw
flink-maintenance.lock.jdbc.init-lock-tables=true
```
JDBC-based locking is recommended for most production environments.


### ZooKeeper Lock Example
```properties
flink-maintenance.lock.type=zookeeper
flink-maintenance.lock.zookeeper.uri=zk://zk1:2181,zk2:2181
flink-maintenance.lock.zookeeper.session-timeout-ms=60000
flink-maintenance.lock.zookeeper.connection-timeout-ms=15000
flink-maintenance.lock.zookeeper.max-retries=3
flink-maintenance.lock.zookeeper.base-sleep-ms=3000
```
Use ZooKeeper-based locks only in high-availability or multi-process coordination environments.


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
