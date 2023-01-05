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
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
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
  private final List<CombinedScanTask> taskGroups;
  private final Schema expectedSchema;
  private final boolean caseSensitive;
  private final boolean localityEnabled;
  private final int scanHashCode;

  SparkBatch(
      JavaSparkContext sparkContext,
      Table table,
      SparkReadConf readConf,
      List<CombinedScanTask> taskGroups,
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
    if (parquetOnly() && parquetBatchReadsEnabled()) {
      return readConf.parquetBatchSize();
    } else if (orcOnly() && orcBatchReadsEnabled()) {
      return readConf.orcBatchSize();
    } else {
      return 0;
    }
  }

  private boolean parquetOnly() {
    return taskGroups.stream()
        .allMatch(task -> !task.isDataTask() && onlyFileFormat(task, FileFormat.PARQUET));
  }

  private boolean parquetBatchReadsEnabled() {
    return readConf.parquetVectorizationEnabled()
        && // vectorization enabled
        expectedSchema.columns().size() > 0
        && // at least one column is projected
        expectedSchema.columns().stream()
            .allMatch(c -> c.type().isPrimitiveType()); // only primitives
  }

  private boolean orcOnly() {
    return taskGroups.stream()
        .allMatch(task -> !task.isDataTask() && onlyFileFormat(task, FileFormat.ORC));
  }

  private boolean orcBatchReadsEnabled() {
    return readConf.orcVectorizationEnabled()
        && // vectorization enabled
        taskGroups.stream().noneMatch(TableScanUtil::hasDeletes); // no delete files
  }

  private boolean onlyFileFormat(CombinedScanTask task, FileFormat fileFormat) {
    return task.files().stream()
        .allMatch(fileScanTask -> fileScanTask.file().format().equals(fileFormat));
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
