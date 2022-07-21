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

import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.DeletedRowsScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.spark.sql.catalyst.InternalRow;

public class ChangelogRowReader extends BaseRowReader<ChangelogScanTask> {
  public ChangelogRowReader(Table table, ScanTaskGroup<ChangelogScanTask> taskGroup, Schema expectedSchema,
                            boolean caseSensitive) {
    super(table, taskGroup, expectedSchema, caseSensitive);
  }

  @Override
  CloseableIterator<InternalRow> open(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return openAddedRowsScanTask((AddedRowsScanTask) task);
    } else if (task instanceof DeletedRowsScanTask) {
      return openDeletedRowsScanTask((DeletedRowsScanTask) task);
    } else if (task instanceof DeletedDataFileScanTask) {
      return openDeletedDataFileScanTask((DeletedDataFileScanTask) task);
    } else {
      throw new IllegalArgumentException("Unsupported task type: " + task.getClass().getName());
    }
  }

  CloseableIterator<InternalRow> openAddedRowsScanTask(AddedRowsScanTask task) {
    SparkDeleteFilter deletes = new SparkDeleteFilter(task.file().path().toString(), task.deletes(), tableSchema(),
        expectedSchema(), this);
    return null;
  }

  private CloseableIterator<InternalRow> openDeletedDataFileScanTask(DeletedDataFileScanTask task) {
    SparkDeleteFilter deletes = new SparkDeleteFilter(task.file().path().toString(), task.existingDeletes(),
        tableSchema(), expectedSchema(), this);
    return null;
  }

  CloseableIterator<InternalRow> openDeletedRowsScanTask(DeletedRowsScanTask task) {
    SparkDeleteFilter deletes = new SparkDeleteFilter(task.file().path().toString(), task.existingDeletes(),
        tableSchema(), expectedSchema(), this);

//    // schema or rows returned by readers
//    Schema requiredSchema = deletes.requiredSchema();
//    Map<Integer, ?> idToConstant = constantsMap(task, requiredSchema);
//    DataFile file = task.file();
//
//    // update the current file for Spark's filename() function
//    InputFileBlockHolder.set(file.path().toString(), task.start(), task.length());
//
//    CloseableIterable<InternalRow> iterable = deletes.filter(open(task, requiredSchema, idToConstant));
//
//    // create a new iterator that will filter out the deleted rows
//    SparkDeleteFilter currentDeleteFilter = new SparkDeleteFilter(
//        task.file().path().toString(), task.addedDeletes(), tableSchema(), expectedSchema(), this);
//
//    return currentDeleteFilter.filter(iterable).iterator();
    return null;
  }
}
