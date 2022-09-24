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
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.source.SparkScan.ReadTask;
import org.apache.iceberg.spark.source.SparkScan.ReaderFactory;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

abstract class SparkBatch implements Batch {

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final SparkReadConf readConf;
  private final Schema expectedSchema;
  private final boolean caseSensitive;
  private final boolean localityEnabled;

  SparkBatch(SparkSession spark, Table table, SparkReadConf readConf, Schema expectedSchema) {
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.readConf = readConf;
    this.expectedSchema = expectedSchema;
    this.caseSensitive = readConf.caseSensitive();
    this.localityEnabled = readConf.localityEnabled();
  }

  @Override
  public InputPartition[] planInputPartitions() {
    // broadcast the table metadata as input partitions will be sent to executors
    Broadcast<Table> tableBroadcast =
        sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
    String expectedSchemaString = SchemaParser.toJson(expectedSchema);

    InputPartition[] readTasks = new InputPartition[tasks().size()];

    Tasks.range(readTasks.length)
        .stopOnFailure()
        .executeWith(localityEnabled ? ThreadPools.getWorkerPool() : null)
        .run(
            index ->
                readTasks[index] =
                    new ReadTask(
                        tasks().get(index),
                        tableBroadcast,
                        expectedSchemaString,
                        caseSensitive,
                        localityEnabled));

    return readTasks;
  }

  protected abstract List<CombinedScanTask> tasks();

  protected JavaSparkContext sparkContext() {
    return sparkContext;
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
    return tasks().stream()
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
    return tasks().stream()
        .allMatch(task -> !task.isDataTask() && onlyFileFormat(task, FileFormat.ORC));
  }

  private boolean orcBatchReadsEnabled() {
    return readConf.orcVectorizationEnabled()
        && // vectorization enabled
        tasks().stream().noneMatch(TableScanUtil::hasDeletes); // no delete files
  }

  private boolean onlyFileFormat(CombinedScanTask task, FileFormat fileFormat) {
    return task.files().stream()
        .allMatch(fileScanTask -> fileScanTask.file().format().equals(fileFormat));
  }
}
