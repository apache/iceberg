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
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.DeletedRowsScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.unsafe.types.UTF8String;

class ChangelogRowReader extends BaseRowReader<ChangelogScanTask> {
  ChangelogRowReader(
      Table table,
      ScanTaskGroup<ChangelogScanTask> taskGroup,
      Schema expectedSchema,
      boolean caseSensitive) {
    super(table, taskGroup, expectedSchema, caseSensitive);
  }

  @Override
  protected CloseableIterator<InternalRow> open(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return openAddedRowsScanTask((AddedRowsScanTask) task);
    } else if (task instanceof DeletedRowsScanTask) {
      throw new UnsupportedOperationException("Deleted rows scan task is not supported yet");
    } else if (task instanceof DeletedDataFileScanTask) {
      return openDeletedDataFileScanTask((DeletedDataFileScanTask) task);
    } else {
      throw new IllegalArgumentException(
          "Unsupported changelog scan task type: " + task.getClass().getName());
    }
  }

  @Override
  protected Stream<ContentFile<?>> referencedFiles(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return Stream.concat(
          Stream.of(((AddedRowsScanTask) task).file()),
          ((AddedRowsScanTask) task).deletes().stream());
    } else if (task instanceof DeletedRowsScanTask) {
      throw new UnsupportedOperationException("Deleted rows scan task is not supported yet");
    } else if (task instanceof DeletedDataFileScanTask) {
      return Stream.concat(
          Stream.of(((DeletedDataFileScanTask) task).file()),
          ((DeletedDataFileScanTask) task).existingDeletes().stream());
    } else {
      throw new IllegalArgumentException(
          "Unsupported changelog scan task type: " + task.getClass().getName());
    }
  }

  CloseableIterator<InternalRow> openAddedRowsScanTask(AddedRowsScanTask task) {
    SparkDeleteFilter deletes =
        new SparkDeleteFilter(task.file().path().toString(), task.deletes());
    return deletes.filter(internalRowIterable(task, deletes.requiredSchema())).iterator();
  }

  private CloseableIterator<InternalRow> openDeletedDataFileScanTask(DeletedDataFileScanTask task) {
    SparkDeleteFilter deletes =
        new SparkDeleteFilter(task.file().path().toString(), task.existingDeletes());
    return deletes.filter(internalRowIterable(task, deletes.requiredSchema())).iterator();
  }

  private CloseableIterable<InternalRow> internalRowIterable(
      ContentScanTask<DataFile> task, Schema readSchema) {
    // schema or rows returned by readers
    Map<Integer, ?> idToConstant = constantsMap(task, readSchema);

    String filePath = task.file().path().toString();

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(filePath, task.start(), task.length());

    InputFile location = getInputFile(filePath);
    Preconditions.checkNotNull(location, "Could not find InputFile");
    return newIterable(
        location,
        task.file().format(),
        task.start(),
        task.length(),
        task.residual(),
        readSchema,
        idToConstant);
  }

  @Override
  public InternalRow get() {
    JoinedRow cdcRow = new JoinedRow();

    InternalRow metadataRow = new GenericInternalRow(3);
    metadataRow.update(0, UTF8String.fromString(currentTask().operation().name()));
    metadataRow.update(1, currentTask().changeOrdinal());
    metadataRow.update(2, currentTask().commitSnapshotId());

    return cdcRow.apply(super.get(), metadataRow);
  }
}
