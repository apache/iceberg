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
TriggerLockFactory lockFactory = TriggerLockFactory.defaultLockFactory();

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

### ExpireSnapshots Configuration

| Method | Description | Default Value | Type |
|--------|-------------|---------------|------|
| `maxSnapshotAge(Duration)` | Maximum age of snapshots to retain | No limit | Duration |
| `retainLast(int)` | Minimum number of snapshots to retain | 1 | int |
| `deleteBatchSize(int)` | Number of files to delete in each batch | 1000 | int |
| `scheduleOnCommitCount(int)` | Trigger after N commits | No automatic scheduling | int |
| `scheduleOnDataFileCount(int)` | Trigger after N data files | No automatic scheduling | int |
| `scheduleOnDataFileSize(long)` | Trigger after total data file size (bytes) | No automatic scheduling | long |
| `scheduleOnIntervalSecond(long)` | Trigger after time interval (seconds) | No automatic scheduling | long |
| `parallelism(int)` | Parallelism for this specific task | Inherits from TableMaintenance | int |



### RewriteDataFiles Configuration

| Method | Description | Default Value | Type |
|--------|-------------|---------------|------|
| `targetFileSizeBytes(long)` | Target size for rewritten files | Table property or 512MB | long |
| `partialProgressEnabled(boolean)` | Enable partial progress commits | false | boolean |
| `partialProgressMaxCommits(int)` | Maximum commits for partial progress | 10 | int |
| `scheduleOnCommitCount(int)` | Trigger after N commits | 10 | int |
| `scheduleOnDataFileCount(int)` | Trigger after N data files | 1000 | int |
| `scheduleOnDataFileSize(long)` | Trigger after total data file size (bytes) | 100GB | long |
| `scheduleOnIntervalSecond(long)` | Trigger after time interval (seconds) | 600 (10 minutes) | long |
| `maxRewriteBytes(long)` | Maximum bytes to rewrite per execution | Long.MAX_VALUE | long |
| `parallelism(int)` | Parallelism for this specific task | Inherits from TableMaintenance | int |

## Flink Configuration Options

You can also configure maintenance behavior through Flink configuration:

| Configuration Key | Description | Default Value | Type |
|-------------------|-------------|---------------|------|
| `iceberg.maintenance.rate-limit-seconds` | Rate limit in seconds | 60 | long |
| `iceberg.maintenance.lock-check-delay-seconds` | Lock check delay in seconds | 30 | long |
| `iceberg.maintenance.rewrite.max-bytes` | Maximum rewrite bytes | Long.MAX_VALUE | long |
| `iceberg.maintenance.rewrite.schedule.commit-count` | Schedule on commit count | 10 | int |
| `iceberg.maintenance.rewrite.schedule.data-file-count` | Schedule on data file count | 1000 | int |
| `iceberg.maintenance.rewrite.schedule.data-file-size` | Schedule on data file size | 100GB | long |
| `iceberg.maintenance.rewrite.schedule.interval-second` | Schedule interval in seconds | 600 | long |


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

## Scheduling Options

Maintenance tasks can be triggered based on various conditions:

### Time-based Scheduling
```java
ExpireSnapshots.builder()
    .scheduleOnIntervalSecond(3600)
```

### Commit-based Scheduling
```java
RewriteDataFiles.builder()
    .scheduleOnCommitCount(50)
```

### Data Volume-based Scheduling
```java
RewriteDataFiles.builder()
    .scheduleOnDataFileCount(500)
    .scheduleOnDataFileSize(50L * 1024 * 1024 * 1024)
```

## IcebergSink with Post-Commit Integration

Apache Iceberg Sink V2 for Flink allows automatic execution of maintenance tasks after data is committed to the table, using the addPostCommitTopology(...) method.

```java
IcebergSink.forRowData(dataStream)
    .table(table)
    .tableLoader(tableLoader)
    .setAll(properties)
    .addPostCommitTopology(
        TableMaintenance.forTable(env, tableLoader, TriggerLockFactory.defaultLockFactory())
            .rateLimit(Duration.ofMinutes(10))
            .add(ExpireSnapshots.builder().scheduleOnCommitCount(10))
            .add(RewriteDataFiles.builder().scheduleOnDataFileCount(50))
    )
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

### Common Issues

**OutOfMemoryError during file deletion:**
```java
.deleteBatchSize(500)
```

**Lock conflicts:**
```java
.lockCheckDelay(Duration.ofMinutes(1))
.rateLimit(Duration.ofMinutes(10))
```

**Slow rewrite operations:**
```java
.partialProgressEnabled(true)
.partialProgressMaxCommits(3)
.maxRewriteBytes(1024 * 1024 * 1024)
```