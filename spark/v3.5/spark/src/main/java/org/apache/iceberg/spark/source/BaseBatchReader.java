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
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.datafile.DataFileServiceRegistry;
import org.apache.iceberg.io.datafile.DeleteFilter;
import org.apache.iceberg.io.datafile.ReaderBuilder;
import org.apache.iceberg.io.datafile.ReaderService;
import org.apache.iceberg.io.datafile.ServiceBase;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.OrcBatchReadConf;
import org.apache.iceberg.spark.ParquetBatchReadConf;
import org.apache.iceberg.spark.ParquetReaderType;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.spark.sql.catalyst.InternalRow;
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

  protected CloseableIterable<ColumnarBatch> newBatchIterable(
      InputFile inputFile, ContentScanTask<?> task, Table table, SparkDeleteFilter deleteFilter) {
    ReaderBuilder<?> readerBuilder =
        DataFileServiceRegistry.read(
                task.file().format(),
                InternalRow.class.getName(),
                parquetConf != null ? parquetConf.readerType().name() : null,
                inputFile)
            .forTask(task)
            .forTable(table)
            .project(expectedSchema())
            .deleteFilter(deleteFilter)
            .caseSensitive(caseSensitive())
            // Spark eagerly consumes the batches. So the underlying memory allocated could be
            // reused
            // without worrying about subsequent reads clobbering over each other. This improves
            // read performance as every batch read doesn't have to pay the cost of allocating
            // memory.
            .reuseContainers()
            .withNameMapping(nameMapping());
    if (parquetConf != null) {
      readerBuilder = readerBuilder.recordsPerBatch(parquetConf.batchSize());
    } else if (orcConf != null) {
      readerBuilder = readerBuilder.recordsPerBatch(orcConf.batchSize());
    }

    return readerBuilder.build();
  }

  public static class IcebergParquetReaderService extends ServiceBase implements ReaderService {
    @SuppressWarnings("checkstyle:RedundantModifier")
    public IcebergParquetReaderService() {
      super(FileFormat.PARQUET, InternalRow.class.getName(), ParquetReaderType.ICEBERG.name());
    }

    @Override
    public ReaderBuilder<?> builder(InputFile inputFile) {
      return Parquet.read(inputFile)
          .initMethod(
              b -> {
                // get required schema if there are deletes
                Schema requiredSchema =
                    b.deleteFilter() != null ? b.deleteFilter().requiredSchema() : b.schema();
                b.project(requiredSchema)
                    .createBatchedReaderFunc(
                        fileSchema ->
                            VectorizedSparkParquetReaders.buildReader(
                                requiredSchema,
                                fileSchema,
                                constantsMap(b.task(), b.schema(), b.table()),
                                (DeleteFilter<InternalRow>) b.deleteFilter()));
              });
    }
  }

  public static class CometParquetReaderService extends ServiceBase implements ReaderService {
    @SuppressWarnings("checkstyle:RedundantModifier")
    public CometParquetReaderService() {
      super(FileFormat.PARQUET, InternalRow.class.getName(), ParquetReaderType.COMET.name());
    }

    @Override
    public ReaderBuilder<?> builder(InputFile inputFile) {
      return Parquet.read(inputFile)
          .initMethod(
              b -> {
                // get required schema if there are deletes
                Schema requiredSchema =
                    b.deleteFilter() != null ? b.deleteFilter().requiredSchema() : b.schema();
                Schema projectedSchema = b.schema();

                b.project(requiredSchema)
                    .createBatchedReaderFunc(
                        fileSchema ->
                            VectorizedSparkParquetReaders.buildCometReader(
                                requiredSchema,
                                fileSchema,
                                constantsMap(b.task(), projectedSchema, b.table()),
                                (DeleteFilter<InternalRow>) b.deleteFilter()));
              });
    }
  }

  public static class ORCReaderService extends ServiceBase implements ReaderService {
    @SuppressWarnings("checkstyle:RedundantModifier")
    public ORCReaderService() {
      super(FileFormat.ORC, InternalRow.class.getName());
    }

    @Override
    public ReaderBuilder<?> builder(InputFile inputFile) {
      return ORC.read(inputFile)
          .initMethod(
              b -> {
                Map<Integer, ?> idToConstant = constantsMap(b.task(), b.schema(), b.table());
                Schema projectedSchema = b.schema();
                b.project(ORC.schemaWithoutConstantAndMetadataFields(projectedSchema, idToConstant))
                    .createBatchedReaderFunc(
                        fileSchema ->
                            VectorizedSparkOrcReaders.buildReader(
                                projectedSchema, fileSchema, idToConstant));
              });
    }
  }
}
