/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import java.util.List;
import java.util.Objects;

import org.apache.iceberg.*;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.source.SparkScan.ReaderFactory;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

class SparkBatch implements Batch {

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final SparkReadConf readConf;
  private final List<? extends ScanTaskGroup<?>> taskGroups;
  private final Schema expectedSchema;
  private final boolean caseSensitive;
  private final boolean localityEnabled;
  private final int scanHashCode;

  SparkBatch(
      JavaSparkContext sparkContext,
      Table table,
      SparkReadConf readConf,
      List<? extends ScanTaskGroup<?>> taskGroups,
      Schema expectedSchema,
      int scanHashCode) {
    this.sparkContext = sparkContext;
    this.table = table;
    this.readConf = readConf;
    this.taskGroups = taskGroups;
    this.expectedSchema = expectedSchema;
    this.caseSensitive = readConf.caseSensitive();
    this.localityEnabled = readConf.localityEnabled();
    this.scanHashCode = scanHashCode;
  }

  @Override
  public InputPartition[] planInputPartitions() {
    // broadcast the table metadata as input partitions will be sent to executors
    Broadcast<Table> tableBroadcast =
        sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
    String expectedSchemaString = SchemaParser.toJson(expectedSchema);

    InputPartition[] partitions = new InputPartition[taskGroups.size()];

    Tasks.range(partitions.length)
        .stopOnFailure()
        .executeWith(localityEnabled ? ThreadPools.getWorkerPool() : null)
        .run(
            index ->
                partitions[index] =
                    new SparkInputPartition(
                        taskGroups.get(index),
                        tableBroadcast,
                        expectedSchemaString,
                        caseSensitive,
                        localityEnabled));

    return partitions;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new ReaderFactory(batchSize());
  }

  private int batchSize() {
    if (useParquetBatchReads()) {
      return readConf.parquetBatchSize();
    } else if (useOrcBatchReads()) {
      return readConf.orcBatchSize();
    } else {
      return 0;
    }
  }

  // conditions for using Parquet batch reads:
  // - Parquet vectorization is enabled
  // - at least one column is projected
  // - only primitives are projected
  // - all tasks are of FileScanTask type and read only Parquet files
  private boolean useParquetBatchReads() {
    return readConf.parquetVectorizationEnabled()
            && expectedSchema.columns().size() > 0
            && expectedSchema.columns().stream().allMatch(c -> c.type().isPrimitiveType())
            && taskGroups.stream().allMatch(this::supportsParquetBatchReads);
  }

  private boolean supportsParquetBatchReads(ScanTask task) {
    if (task instanceof ScanTaskGroup) {
      ScanTaskGroup<?> taskGroup = (ScanTaskGroup<?>) task;
      return taskGroup.tasks().stream().allMatch(this::supportsParquetBatchReads);

    } else if (task.isFileScanTask() && !task.isDataTask()) {
      FileScanTask fileScanTask = task.asFileScanTask();
      return fileScanTask.file().format() == FileFormat.PARQUET;

    } else {
      return false;
    }
  }

  // conditions for using ORC batch reads:
  // - ORC vectorization is enabled
  // - all tasks are of type FileScanTask and read only ORC files with no delete files
  private boolean useOrcBatchReads() {
    return readConf.orcVectorizationEnabled()
            && taskGroups.stream().allMatch(this::supportsOrcBatchReads);
  }

  private boolean supportsOrcBatchReads(ScanTask task) {
    if (task instanceof ScanTaskGroup) {
      ScanTaskGroup<?> taskGroup = (ScanTaskGroup<?>) task;
      return taskGroup.tasks().stream().allMatch(this::supportsOrcBatchReads);

    } else if (task.isFileScanTask() && !task.isDataTask()) {
      FileScanTask fileScanTask = task.asFileScanTask();
      return fileScanTask.file().format() == FileFormat.ORC && fileScanTask.deletes().isEmpty();

    } else {
      return false;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkBatch that = (SparkBatch) o;
    return table.name().equals(that.table.name()) && scanHashCode == that.scanHashCode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(table.name(), scanHashCode);
  }
}
