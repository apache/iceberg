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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.FileAccessFactoryRegistry;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.ReadBuilder;
import org.apache.iceberg.parquet.ParquetFileAccessFactory;
import org.apache.iceberg.spark.OrcBatchReadConf;
import org.apache.iceberg.spark.ParquetBatchReadConf;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.spark.sql.vectorized.ColumnarBatch;

abstract class BaseBatchReader<T extends ScanTask> extends BaseReader<ColumnarBatch, T> {
  private final ParquetBatchReadConf parquetConf;
  private final OrcBatchReadConf orcConf;

  BaseBatchReader(
      Table table,
      ScanTaskGroup<T> taskGroup,
      Schema tableSchema,
      Schema expectedSchema,
      boolean caseSensitive,
      ParquetBatchReadConf parquetConf,
      OrcBatchReadConf orcConf) {
    super(table, taskGroup, tableSchema, expectedSchema, caseSensitive);
    this.parquetConf = parquetConf;
    this.orcConf = orcConf;
  }

  @SuppressWarnings("unchecked")
  protected CloseableIterable<ColumnarBatch> newBatchIterable(
      InputFile inputFile,
      FileFormat format,
      long start,
      long length,
      Expression residual,
      Map<Integer, ?> idToConstant,
      SparkDeleteFilter deleteFilter) {
    Schema requiredSchema = deleteFilter != null ? deleteFilter.requiredSchema() : expectedSchema();
    ReadBuilder<?, ColumnarBatch> readBuilder =
        FileAccessFactoryRegistry.readBuilder(
            format, SparkObjectModels.SPARK_VECTORIZED_OBJECT_MODEL, inputFile);
    if (parquetConf != null) {
      readBuilder =
          readBuilder
              .set(ReadBuilder.RECORDS_PER_BATCH_KEY, String.valueOf(parquetConf.batchSize()))
              .set(
                  VectorizedSparkParquetReaders.PARQUET_READER_TYPE,
                  parquetConf.readerType().name());
    } else if (orcConf != null) {
      readBuilder =
          readBuilder.set(ReadBuilder.RECORDS_PER_BATCH_KEY, String.valueOf(orcConf.batchSize()));
    }

    if (readBuilder instanceof ParquetFileAccessFactory.SupportsDeleteFilter<?>) {
      ((ParquetFileAccessFactory.SupportsDeleteFilter<SparkDeleteFilter>) readBuilder)
          .deleteFilter(deleteFilter);
    }

    return readBuilder
        .project(requiredSchema)
        .constantFieldAccessors(idToConstant)
        .split(start, length)
        .filter(residual, caseSensitive())
        // Spark eagerly consumes the batches. So the underlying memory allocated could be
        // reused without worrying about subsequent reads clobbering over each other. This
        // improves read performance as every batch read doesn't have to pay the cost of
        // allocating memory.
        .reuseContainers()
        .nameMapping(nameMapping())
        .build();
  }
}
