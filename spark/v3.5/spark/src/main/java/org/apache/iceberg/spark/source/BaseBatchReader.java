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
import org.apache.iceberg.DataFileFormats;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;

abstract class BaseBatchReader<T extends ScanTask> extends BaseReader<ColumnarBatch, T> {

  static {
    DataFileFormats.register(
        FileFormat.PARQUET,
        ColumnarBatch.class,
        (inputFile, task, readSchema, table, deleteFilter) -> {
          // get required schema if there are deletes
          Schema requiredSchema = deleteFilter != null ? deleteFilter.requiredSchema() : readSchema;
          return Parquet.read(inputFile)
              .project(requiredSchema)
              .createBatchedReaderFunc(
                  fileSchema ->
                      VectorizedSparkParquetReaders.buildReader(
                          requiredSchema,
                          fileSchema,
                          constantsMap(task, readSchema, table),
                          (DeleteFilter<InternalRow>) deleteFilter));
        });

    DataFileFormats.register(
        FileFormat.ORC,
        ColumnarBatch.class,
        (inputFile, task, readSchema, table, deleteFilter) -> {
          Map<Integer, ?> idToConstant = constantsMap(task, readSchema, table);
          return ORC.read(inputFile)
              .project(ORC.schemaWithoutConstantAndMetadataFields(readSchema, idToConstant))
              .createBatchedReaderFunc(
                  fileSchema ->
                      VectorizedSparkOrcReaders.buildReader(readSchema, fileSchema, idToConstant));
        });
  }

  private final int batchSize;

  BaseBatchReader(
      Table table,
      ScanTaskGroup<T> taskGroup,
      Schema tableSchema,
      Schema expectedSchema,
      boolean caseSensitive,
      int batchSize) {
    super(table, taskGroup, tableSchema, expectedSchema, caseSensitive);
    this.batchSize = batchSize;
  }

  protected CloseableIterable<ColumnarBatch> newBatchIterable(
      InputFile inputFile, FileScanTask task, SparkDeleteFilter deleteFilter) {
    return DataFileFormats.read(
            task.file().format(),
            ColumnarBatch.class,
            inputFile,
            task,
            expectedSchema(),
            table(),
            deleteFilter)
        .split(task.start(), task.length())
        .recordsPerBatch(batchSize)
        .filter(task.residual())
        .caseSensitive(caseSensitive())
        // Spark eagerly consumes the batches. So the underlying memory allocated could be reused
        // without worrying about subsequent reads clobbering over each other. This improves
        // read performance as every batch read doesn't have to pay the cost of allocating memory.
        .reuseContainers()
        .withNameMapping(nameMapping())
        .build();
  }
}
