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
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RowDataReader extends BaseRowReader<FileScanTask> {
  private static final Logger LOG = LoggerFactory.getLogger(RowDataReader.class);

  RowDataReader(ScanTaskGroup<FileScanTask> task, Table table, Schema expectedSchema, boolean caseSensitive) {
    super(table, task, expectedSchema, caseSensitive);
  }

  @Override
  CloseableIterator<InternalRow> open(FileScanTask task) {
    SparkDeleteFilter deletes = new SparkDeleteFilter(task, tableSchema(), expectedSchema(), this);

    // schema or rows returned by readers
    Schema requiredSchema = deletes.requiredSchema();
    Map<Integer, ?> idToConstant = constantsMap(task, requiredSchema);

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(task.file().path().toString(), task.start(), task.length());

    return deletes.filter(open(task, requiredSchema, idToConstant)).iterator();
  }

  protected Map<Integer, ?> constantsMap(FileScanTask task, Schema readSchema) {
    if (readSchema.findField(MetadataColumns.PARTITION_COLUMN_ID) != null) {
      Types.StructType partitionType = Partitioning.partitionType(table());
      return PartitionUtil.constantsMap(task, partitionType, BaseReader::convertConstant);
    } else {
      return PartitionUtil.constantsMap(task, BaseReader::convertConstant);
    }
  }

  protected CloseableIterable<InternalRow> open(FileScanTask task, Schema readSchema, Map<Integer, ?> idToConstant) {
    CloseableIterable<InternalRow> iter;
    if (task.isDataTask()) {
      iter = newDataIterable(task.asDataTask(), readSchema);
    } else {
      InputFile location = getInputFile(task.file().path().toString());
      Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");
      iter = newIterable(location, task.file().format(), task.start(), task.length(), task.residual(), readSchema,
          idToConstant);
    }

    return iter;
  }

  private CloseableIterable<InternalRow> newDataIterable(DataTask task, Schema readSchema) {
    StructInternalRow row = new StructInternalRow(readSchema.asStruct());
    CloseableIterable<InternalRow> asSparkRows = CloseableIterable.transform(
        task.asDataTask().rows(), row::setStruct);
    return asSparkRows;
  }

  @Override
  protected void printError(Exception e, FileScanTask task) {
    if (task != null && !task.isDataTask()) {
      LOG.error("Error reading file: {}", task.file().path(), e);
    }
  }
}
