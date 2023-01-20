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
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PositionDeletesRowReader extends BaseRowReader<PositionDeletesScanTask>
    implements PartitionReader<InternalRow> {

  private static final Logger LOG = LoggerFactory.getLogger(PositionDeletesRowReader.class);

  PositionDeletesRowReader(SparkInputPartition partition) {
    this(
        partition.table(),
        partition.taskGroup(),
        partition.expectedSchema(),
        partition.isCaseSensitive());
  }

  PositionDeletesRowReader(
      Table table,
      ScanTaskGroup<PositionDeletesScanTask> taskGroup,
      Schema expectedSchema,
      boolean caseSensitive) {

    super(table, taskGroup, expectedSchema, caseSensitive);

    int numSplits = taskGroup.tasks().size();
    LOG.debug("Reading {} position delete file split(s) for table {}", numSplits, table.name());
  }

  @Override
  protected Stream<ContentFile<?>> referencedFiles(PositionDeletesScanTask task) {
    return Stream.of(task.file());
  }

  @Override
  protected CloseableIterator<InternalRow> open(PositionDeletesScanTask task) {
    Map<Integer, ?> idToConstant = constantsMap(task, expectedSchema());
    String filePath = task.file().path().toString();
    LOG.debug("Opening position delete file {}", filePath);

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(filePath, task.start(), task.length());

    InputFile inputFile = getInputFile(task.file().path().toString());
    Preconditions.checkNotNull(
        inputFile, "Could not find InputFile associated with PositionDeleteScanTask");

    return newIterable(
            inputFile,
            task.file().format(),
            task.start(),
            task.length(),
            task.residual(),
            expectedSchema(),
            idToConstant)
        .iterator();
  }
}
