---
title: "Flink TableMaintenance"
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

## Flink Table Maintenance BatchMode

### Rewrite files action

Iceberg provides API to rewrite small files into large files by submitting Flink batch jobs. The behavior of this Flink action is the same as Spark's [rewriteDataFiles](maintenance.md#compact-data-files).

```java
import org.apache.iceberg.flink.actions.Actions;

TableLoader tableLoader = TableLoader.fromCatalog(
    CatalogLoader.hive("my_catalog", configuration, properties),
    TableIdentifier.of("database", "table")
);

Table table = tableLoader.loadTable();
RewriteDataFilesActionResult result = Actions.forTable(table)
        .rewriteDataFiles()
        .execute();
```

For more details of the rewrite files action, please refer to [RewriteDataFilesAction](../../javadoc/{{ icebergVersion }}/org/apache/iceberg/flink/actions/RewriteDataFilesAction.html)

## Flink Table Maintenance StreamingMode

### Overview

In **Apache Iceberg** deployments within **Flink streaming environments**, implementing automated table maintenance operations—including `snapshot expiration`, `small file compaction`, and `orphan file cleanup`—is critical for optimal query performance and storage efficiency.

Traditionally, these maintenance operations were exclusively accessible through **Iceberg Spark Actions**, necessitating the deployment and management of dedicated Spark clusters. This dependency on **Spark infrastructure** solely for table optimization introduces significant **architectural complexity** and **operational overhead**.

The `TableMaintenance` API in **Apache Iceberg** empowers **Flink jobs** to execute maintenance tasks **natively**, either embedded within existing streaming pipelines or deployed as standalone Flink jobs. This eliminates dependencies on external systems, thereby **streamlining architecture**, **reducing operational costs**, and **enhancing automation capabilities**.

### Supported Features (Flink)

#### ExpireSnapshots
Removes old snapshots and their files. Internally uses `cleanExpiredFiles(true)` when committing, so expired metadata/files are cleaned up automatically.

```java
.add(ExpireSnapshots.builder()
    .maxSnapshotAge(Duration.ofDays(7))
    .retainLast(10)
    .deleteBatchSize(1000))
```

#### RewriteDataFiles
Compacts small files to optimize file sizes. Supports partial progress commits and limiting maximum rewritten bytes per run.

```java
.add(RewriteDataFiles.builder()
    .targetFileSizeBytes(256 * 1024 * 1024)
    .minFileSizeBytes(32 * 1024 * 1024)
    .partialProgressEnabled(true)
    .partialProgressMaxCommits(5))
```


#### DeleteOrphanFiles
Used to remove files which are not referenced in any metadata files of an Iceberg table and can thus be considered "orphaned".The table location is checked for such files.

```java
.add(DeleteOrphanFiles.builder()
    .minAge(Duration.ofDays(3))
    .deleteBatchSize(1000))
```

### Lock Management

The `TriggerLockFactory` is essential for coordinating maintenance tasks. It prevents concurrent maintenance operations on the same table, which could lead to conflicts or data corruption. This locking mechanism is necessary even for a single job, as multiple instances of the same task could otherwise conflict.

#### Why Locks Are Needed
- **Concurrent Access**: Multiple Flink jobs may attempt maintenance simultaneously
- **Data Consistency**: Ensures only one maintenance operation runs per table at a time
- **Resource Management**: Prevents resource conflicts and scheduling issues
- **Avoid Duplicate Work**: Even when only a single compaction job is scheduled, multiple instances could attempt the same operation, leading to redundant work and wasted resources.

#### Supported Lock Types

##### JDBC Lock Factory
Uses a database table to manage distributed locks:

```java
Map<String, String> jdbcProps = new HashMap<>();
jdbcProps.put("jdbc.user", "flink");
jdbcProps.put("jdbc.password", "flinkpw");
jdbcProps.put("flink-maintenance.lock.jdbc.init-lock-tables", "true"); // Auto-create lock table if it doesn't exist

TriggerLockFactory lockFactory = new JdbcLockFactory(
    "jdbc:postgresql://localhost:5432/iceberg", // JDBC URL
    "catalog.db.table",                         // Lock ID (unique identifier)
    jdbcProps                                   // JDBC connection properties
);
```

##### ZooKeeper Lock Factory
Uses Apache ZooKeeper for distributed locks:

```java
TriggerLockFactory lockFactory = new ZkLockFactory(
    "localhost:2181",       // ZooKeeper connection string
    "catalog.db.table",     // Lock ID (unique identifier)
    60000,                  // sessionTimeoutMs
    15000,                  // connectionTimeoutMs
    3000,                   // baseSleepTimeMs
    3                       // maxRetries
);
```

### Quick Start

The following example demonstrates the implementation of automated maintenance for an Iceberg table within a Flink environment.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

TableLoader tableLoader = TableLoader.fromCatalog(
    CatalogLoader.hive("my_catalog", configuration, properties),  
    TableIdentifier.of("database", "table")
);

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

### Configuration Options

#### TableMaintenance Builder

| Method | Description | Default |
|--------|-------------|---------|
| `uidSuffix(String)` | Unique identifier suffix for the job | Random UUID |
| `rateLimit(Duration)` | Minimum interval between task executions | 60 seconds |
| `lockCheckDelay(Duration)` | Delay for checking lock availability | 30 seconds |
| `parallelism(int)` | Default parallelism for maintenance tasks | System default |
| `maxReadBack(int)` | Max snapshots to check during initialization | 100 |

#### Maintenance Task Common Options

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

#### ExpireSnapshots Configuration

| Method | Description | Default Value | Type |
|--------|-------------|---------------|------|
| `maxSnapshotAge(Duration)` | Maximum age of snapshots to retain | 5 days | Duration |
| `retainLast(int)` | Minimum number of snapshots to retain | 1 | int |
| `deleteBatchSize(int)` | Number of files to delete in each batch | 1000 | int |
| `planningWorkerPoolSize(int)` | Number of worker threads for planning snapshot expiration | Shared worker pool | int |
| `cleanExpiredMetadata(boolean)` | Remove expired metadata files when expiring snapshots | false | boolean |

#### RewriteDataFiles Configuration

| Method | Description | Default Value | Type |
|--------|-------------|---------------|------|
| `targetFileSizeBytes(long)` | Target size for rewritten files | Table property or 512MB | long |
| `minFileSizeBytes(long)` | Minimum size of files eligible for compaction | 75% of target file size | long |
| `maxFileSizeBytes(long)` | Maximum size of files eligible for compaction | 180% of target file size | long |
| `minInputFiles(int)` | Minimum number of files to trigger rewrite | 5 | int |
| `deleteFileThreshold(int)` | Minimum delete-file count per data file to force rewrite | Integer.MAX_VALUE | int |
| `rewriteAll(boolean)` | Rewrite all data files regardless of thresholds | false | boolean |
| `maxFileGroupSizeBytes(long)` | Maximum total size of a file group | 107374182400 (100GB) | long |
| `maxFilesToRewrite(int)` | If this option is not specified, all eligible files will be rewritten | null | int |
| `partialProgressEnabled(boolean)` | Enable partial progress commits | false | boolean |
| `partialProgressMaxCommits(int)` | Maximum commits allowed for partial progress when partialProgressEnabled is true | 10 | int |
| `maxRewriteBytes(long)` | Maximum bytes to rewrite per execution | Long.MAX_VALUE | long |
| `filter(Expression)` | Filter expression for selecting files to rewrite | Expressions.alwaysTrue() | Expression |

#### DeleteOrphanFiles Configuration

| Method                                   | Description                                                                                                                                                                                                                                                                                                                                                             | Default Value           | Type               |
|------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|--------------------|
| `location(string)`                       | The location to start the recursive listing the candidate files for removal.                                                                                                                                                                                                                                                                                            | Table's location        | String             |
| `usePrefixListing(boolean)`              | When true, use prefix-based file listing via the SupportsPrefixOperations interface. The Table FileIO implementation must support SupportsPrefixOperations when this flag is enabled.(Note: Setting it to False will use a recursive method to obtain file information. If the underlying storage is object storage, it will repeatedly call the API to get the path.)  | False                   | boolean            |
| `prefixMismatchMode(PrefixMismatchMode)` | Action behavior when location prefixes (schemes/authorities) mismatch: <ul><li>ERROR - throw an exception. </li><li>IGNORE - no action.</li><li>DELETE - delete files.</li></ul>                                                                                                                                                                                        | ERROR                   | PrefixMismatchMode |
| `equalSchemes(Map<String, String>)`      | Mapping of file system schemes to be considered equal. Key is a comma-separated list of schemes and value is a scheme                                                                                                                                                                                                                                                   | "s3n"=>"s3","s3a"=>"s3" | Map<String,String> |                     
| `equalAuthorities(Map<String, String>)`  | Mapping of file system authorities to be considered equal. Key is a comma-separated list of authorities and value is an authority.                                                                                                                                                                                                                                      | Empty map               | Map<String,String> |       
| `minAge(Duration)`                       | Remove orphan files created before this timestamp                                                                                                                                                                                                                                                                                                                       | 3 days ago              | Duration           |
| `planningWorkerPoolSize(int)`            | Number of worker threads for planning snapshot expiration                                                                                                                                                                                                                                                                                                               | Shared worker pool      | int                |

### Complete Example

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
		
		// Set up JDBC lock factory
        Map<String, String> jdbcProps = new HashMap<>();
        jdbcProps.put("jdbc.user", "flink");
        jdbcProps.put("jdbc.password", "flinkpw");
        jdbcProps.put("flink-maintenance.lock.jdbc.init-lock-tables", "true");
        
        TriggerLockFactory lockFactory = new JdbcLockFactory(
            "jdbc:postgresql://localhost:5432/iceberg",
            "catalog.db.table",
            jdbcProps
        );
        
        // Set up maintenance with comprehensive configuration
        TableMaintenance.forTable(env, tableLoader, lockFactory)
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

            // Delete orphans files created more than five days ago
            .add(DeleteOrphanFiles.builder()
                        .minAge(Duration.ofDays(5)))    
                
            .append();
        
        env.execute("Iceberg Table Maintenance");
    }
}
```

### IcebergSink with Post-Commit Integration

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

#### SQL Examples

You can enable maintenance and configure locks using SQL before executing writes:

```sql
-- Enable Iceberg V2 Sink and compaction (maintenance)
SET 'table.exec.iceberg.use.v2.sink' = 'true';
SET 'compaction-enabled' = 'true';

-- Configure maintenance lock (JDBC)
SET 'flink-maintenance.lock.type' = 'jdbc';
SET 'flink-maintenance.lock.lock-id' = 'catalog.db.table';
SET 'flink-maintenance.lock.jdbc.uri' = 'jdbc:postgresql://localhost:5432/iceberg';
SET 'flink-maintenance.lock.jdbc.init-lock-tables' = 'true';

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
  'catalog-database' = 'db',
  'catalog-table' = 'tbl',
  'compaction-enabled' = 'true',

  'flink-maintenance.lock.type' = 'jdbc',
  'flink-maintenance.lock.lock-id' = 'catalog.db.table',
  'flink-maintenance.lock.jdbc.uri' = 'jdbc:postgresql://localhost:5432/iceberg',
  'flink-maintenance.lock.jdbc.init-lock-tables' = 'true'
);
```

### Lock Configuration (SQL)

These keys are used in SQL (SET or table WITH options) and are applicable when writing with compaction enabled.

- JDBC

| Key | Description | Default |
|-----|-------------|---------|
| `flink-maintenance.lock.type` | Set to `jdbc` |  |
| `flink-maintenance.lock.lock-id` | Unique lock ID per table |  |
| `flink-maintenance.lock.jdbc.uri` | JDBC URI |  |
| `flink-maintenance.lock.jdbc.init-lock-tables` | Auto-create lock table | `false` |

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

### Best Practices

#### Resource Management
- Use dedicated slot sharing groups for maintenance tasks
- Set appropriate parallelism based on cluster resources
- Enable checkpointing for fault tolerance

#### Scheduling Strategy
- Avoid too frequent executions with `rateLimit`
- Use `scheduleOnCommitCount` for write-heavy tables
- Use `scheduleOnDataFileCount` for fine-grained control

#### Performance Tuning
- Adjust `deleteBatchSize` based on storage performance
- Enable `partialProgressEnabled` for large rewrite operations
- Set reasonable `maxRewriteBytes` limits
- Setting an appropriate `maxFileGroupSizeBytes` can break down large FileGroups into smaller ones, thereby increasing the speed of parallel processing

### Troubleshooting

#### OutOfMemoryError during file deletion
**Scenario:** This can occur when the maintenance task attempts to delete a very large number of files in a single batch, especially in tables with long retention histories or after bulk deletions.
**Cause:** Each file deletion involves metadata and object store operations, which together can consume significant memory. Large batches magnify this effect and may exhaust the JVM heap.
**Recommendation:** Reduce the batch size to limit memory usage during deletion.
```java
.deleteBatchSize(500) // Example: 500 files per batch
```

#### Lock conflicts
**Scenario:** In multi-job or high-availability environments, two or more Flink jobs may attempt maintenance on the same table simultaneously.
**Cause:** Concurrent jobs compete for the same distributed lock, causing retries and possible delays.
**Recommendation:** Increase lock check delay and rate limit so that failed attempts back off and reduce contention.
```java
.lockCheckDelay(Duration.ofMinutes(1)) // Wait longer before re-checking lock
.rateLimit(Duration.ofMinutes(10))     // Reduce frequency of task execution
```

#### Slow rewrite operations
**Scenario:** Large tables with many small files can require rewriting terabytes of data in a single run, which may overwhelm available resources.
**Cause:** Without limits, rewrite tasks attempt to process all eligible files at once, leading to long execution times and possible job failures.
**Recommendation:** Enable partial progress so that rewritten files can be committed in smaller batches, and cap the maximum data rewritten in each execution.
```java
.partialProgressEnabled(true) // Commit progress incrementally
.partialProgressMaxCommits(3) // Allow up to 3 commits per run
.maxRewriteBytes(1L * 1024 * 1024 * 1024) // Limit to ~1GB per run
```
