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

import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.source.metrics.TaskNumDeletes;
import org.apache.iceberg.spark.source.metrics.TaskNumSplits;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchDataReader extends BaseBatchReader<FileScanTask>
    implements PartitionReader<ColumnarBatch> {

  private static final Logger LOG = LoggerFactory.getLogger(BatchDataReader.class);

  private final long numSplits;

  public BatchDataReader(SparkInputPartition partition, int batchSize) {
    this(
        partition.table(),
        partition.taskGroup(),
        SnapshotUtil.schemaFor(partition.table(), partition.branch()),
        partition.expectedSchema(),
        partition.isCaseSensitive(),
        batchSize);
  }

  BatchDataReader(
      Table table,
      ScanTaskGroup<FileScanTask> taskGroup,
      Schema tableSchema,
      Schema expectedSchema,
      boolean caseSensitive,
      int size) {
    super(table, taskGroup, tableSchema, expectedSchema, caseSensitive, size);

    numSplits = taskGroup.tasks().size();
    LOG.debug("Reading {} file split(s) for table {}", numSplits, table.name());
  }

  @Override
  public CustomTaskMetric[] currentMetricsValues() {
    return new CustomTaskMetric[] {
      new TaskNumSplits(numSplits), new TaskNumDeletes(counter().get())
    };
  }

  @Override
  protected Stream<ContentFile<?>> referencedFiles(FileScanTask task) {
    return Stream.concat(Stream.of(task.file()), task.deletes().stream());
  }

  @Override
  protected CloseableIterator<ColumnarBatch> open(FileScanTask task) {
    String filePath = task.file().path().toString();
    LOG.debug("Opening data file {}", filePath);

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(filePath, task.start(), task.length());

    Map<Integer, ?> idToConstant = constantsMap(task, expectedSchema());

    InputFile inputFile = getInputFile(filePath);
    Preconditions.checkNotNull(inputFile, "Could not find InputFile associated with FileScanTask");

    SparkDeleteFilter deleteFilter =
        task.deletes().isEmpty()
            ? null
            : new SparkDeleteFilter(filePath, task.deletes(), counter());

    return newBatchIterable(
            inputFile,
            task.file().format(),
            task.start(),
            task.length(),
            task.residual(),
            idToConstant,
            deleteFilter)
        .iterator();
  }
}
