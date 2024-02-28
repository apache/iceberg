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
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.vectorized.ColumnarBatch;

abstract class BaseBatchReader<T extends ScanTask> extends BaseReader<ColumnarBatch, T> {
  protected final int batchSize;

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
      InputFile inputFile,
      FileFormat format,
      long start,
      long length,
      Expression residual,
      Map<Integer, ?> idToConstant,
      SparkDeleteFilter deleteFilter) {
    switch (format) {
      case PARQUET:
        return newParquetIterable(inputFile, start, length, residual, idToConstant, deleteFilter);

      case ORC:
        return newOrcIterable(inputFile, start, length, residual, idToConstant);

      default:
        throw new UnsupportedOperationException(
            "Format: " + format + " not supported for batched reads");
    }
  }

  private CloseableIterable<ColumnarBatch> newParquetIterable(
      InputFile inputFile,
      long start,
      long length,
      Expression residual,
      Map<Integer, ?> idToConstant,
      SparkDeleteFilter deleteFilter) {
    // get required schema if there are deletes
    Schema requiredSchema = deleteFilter != null ? deleteFilter.requiredSchema() : expectedSchema();

    return Parquet.read(inputFile)
        .project(requiredSchema)
        .split(start, length)
        .createBatchedReaderFunc(
            fileSchema ->
                VectorizedSparkParquetReaders.buildReader(
                    requiredSchema, fileSchema, idToConstant, deleteFilter))
        .recordsPerBatch(batchSize)
        .filter(residual)
        .caseSensitive(caseSensitive())
        // Spark eagerly consumes the batches. So the underlying memory allocated could be reused
        // without worrying about subsequent reads clobbering over each other. This improves
        // read performance as every batch read doesn't have to pay the cost of allocating memory.
        .reuseContainers()
        .withNameMapping(nameMapping())
        .build();
  }

  private CloseableIterable<ColumnarBatch> newOrcIterable(
      InputFile inputFile,
      long start,
      long length,
      Expression residual,
      Map<Integer, ?> idToConstant) {
    Set<Integer> constantFieldIds = idToConstant.keySet();
    Set<Integer> metadataFieldIds = MetadataColumns.metadataFieldIds();
    Sets.SetView<Integer> constantAndMetadataFieldIds =
        Sets.union(constantFieldIds, metadataFieldIds);
    Schema schemaWithoutConstantAndMetadataFields =
        TypeUtil.selectNot(expectedSchema(), constantAndMetadataFieldIds);

    return ORC.read(inputFile)
        .project(schemaWithoutConstantAndMetadataFields)
        .split(start, length)
        .createBatchedReaderFunc(
            fileSchema ->
                VectorizedSparkOrcReaders.buildReader(expectedSchema(), fileSchema, idToConstant))
        .recordsPerBatch(batchSize)
        .filter(residual)
        .caseSensitive(caseSensitive())
        .withNameMapping(nameMapping())
        .build();
  }
}
