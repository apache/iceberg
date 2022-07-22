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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.vectorized.ColumnarBatch;

class BatchDataReader extends BaseBatchReader<FileScanTask> {
  BatchDataReader(ScanTaskGroup<FileScanTask> task, Table table, Schema expectedSchema, boolean caseSensitive,
                  int size) {
    super(table, task, expectedSchema, caseSensitive, size);
  }

  @Override
  protected Stream<ContentFile<?>> referencedFiles(FileScanTask task) {
    return Stream.concat(Stream.of(task.file()), task.deletes().stream());
  }

  @Override
  CloseableIterator<ColumnarBatch> open(FileScanTask task) {
    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(task.file().path().toString(), task.start(), task.length());

    Map<Integer, ?> idToConstant = constantsMap(task, expectedSchema());

    InputFile location = getInputFile(task.file().path().toString());
    Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");

    CloseableIterable<ColumnarBatch> iter = newBatchIterable(location, task.file().format(), task.start(),
        task.length(), task.residual(), idToConstant, deleteFilter(task));
    return iter.iterator();
  }

  private SparkDeleteFilter deleteFilter(FileScanTask task) {
    return task.deletes().isEmpty() ? null : new SparkDeleteFilter(task, table().schema(), expectedSchema(), this);
  }
}
