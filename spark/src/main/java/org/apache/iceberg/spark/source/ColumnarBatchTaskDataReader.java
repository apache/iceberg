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

import com.google.common.base.Preconditions;
import java.util.Iterator;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

class ColumnarBatchTaskDataReader extends BaseTaskDataReader<ColumnarBatch>
    implements InputPartitionReader<ColumnarBatch> {

  ColumnarBatchTaskDataReader(
      CombinedScanTask task, Schema tableSchema, Schema expectedSchema, FileIO fileIo,
      EncryptionManager encryptionManager, boolean caseSensitive, int bSize) {
    super(task, tableSchema, expectedSchema, fileIo, encryptionManager, caseSensitive, bSize);
  }

  @Override
  public ColumnarBatch get() {
    return current;
  }

  @Override
  Iterator<ColumnarBatch> open(FileScanTask task) {
    // schema or rows returned by readers
    Schema finalSchema = expectedSchema;
    // schema needed for the projection and filtering
    StructType sparkType = SparkSchemaUtil.convert(finalSchema);
    Schema requiredSchema = SparkSchemaUtil.prune(tableSchema, sparkType, task.residual(), caseSensitive);
    boolean hasExtraFilterColumns = requiredSchema.columns().size() != finalSchema.columns().size();
    Iterator<ColumnarBatch> iter;
    if (hasExtraFilterColumns) {
      iter = open(task, requiredSchema);
    } else {
      iter = open(task, finalSchema);
    }
    return iter;
  }

  private Iterator<ColumnarBatch> open(FileScanTask task, Schema readSchema) {
    CloseableIterable<ColumnarBatch> iter;
    InputFile location = inputFiles.get(task.file().path().toString());
    Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");
    if (task.file().format() == FileFormat.PARQUET) {
      iter = Parquet.read(location)
          .project(readSchema)
          .split(task.start(), task.length())
          .createBatchedReaderFunc(fileSchema -> VectorizedSparkParquetReaders.buildReader(tableSchema, readSchema,
              fileSchema, batchSize))
          .filter(task.residual())
          .caseSensitive(caseSensitive)
          .recordsPerBatch(batchSize)
          // Spark eagerly consumes the batches so the underlying memory allocated could be reused
          // without worrying about subsequent reads clobbering over each other. This improves
          // read performance as every batch read doesn't have to pay the cost of allocating memory.
          .reuseContainers()
          .build();
    } else {
      throw new UnsupportedOperationException(
          "Format: " + task.file().format() + " not supported for batched reads");
    }
    this.currentCloseable = iter;
    return iter.iterator();
  }

}
