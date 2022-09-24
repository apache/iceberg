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
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BatchDataReader extends BaseBatchReader<FileScanTask> {
  private static final Logger LOG = LoggerFactory.getLogger(BatchDataReader.class);

  BatchDataReader(
      ScanTaskGroup<FileScanTask> task,
      Table table,
      Schema expectedSchema,
      boolean caseSensitive,
      int size) {
    super(table, task, expectedSchema, caseSensitive, size);
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
