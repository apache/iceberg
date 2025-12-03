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
import javax.annotation.Nonnull;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.OrcBatchReadConf;
import org.apache.iceberg.spark.ParquetBatchReadConf;
import org.apache.iceberg.spark.ParquetReaderType;
import org.apache.iceberg.spark.data.vectorized.ColumnVectorWithFilter;
import org.apache.iceberg.spark.data.vectorized.ColumnarBatchUtil;
import org.apache.iceberg.spark.data.vectorized.UpdatableDeletedColumnVector;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
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
      OrcBatchReadConf orcConf,
      boolean cacheDeleteFilesOnExecutors) {
    super(
        table, taskGroup, tableSchema, expectedSchema, caseSensitive, cacheDeleteFilesOnExecutors);
    this.parquetConf = parquetConf;
    this.orcConf = orcConf;
  }

  protected CloseableIterable<ColumnarBatch> newBatchIterable(
      InputFile inputFile,
      FileFormat format,
      long start,
      long length,
      Expression residual,
      Map<Integer, ?> idToConstant,
      @Nonnull SparkDeleteFilter deleteFilter) {
    CloseableIterable<ColumnarBatch> iterable;
    switch (format) {
      case PARQUET:
        iterable =
            newParquetIterable(
                inputFile, start, length, residual, idToConstant, deleteFilter.requiredSchema());
        break;
      case ORC:
        iterable = newOrcIterable(inputFile, start, length, residual, idToConstant);
        break;
      default:
        throw new UnsupportedOperationException(
            "Format: " + format + " not supported for batched reads");
    }

    return CloseableIterable.transform(iterable, new BatchDeleteFilter(deleteFilter)::filterBatch);
  }

  private CloseableIterable<ColumnarBatch> newParquetIterable(
      InputFile inputFile,
      long start,
      long length,
      Expression residual,
      Map<Integer, ?> idToConstant,
      Schema requiredSchema) {
    return Parquet.read(inputFile)
        .project(requiredSchema)
        .split(start, length)
        .createBatchedReaderFunc(
            fileSchema -> {
              if (parquetConf.readerType() == ParquetReaderType.COMET) {
                return VectorizedSparkParquetReaders.buildCometReader(
                    requiredSchema, fileSchema, idToConstant);
              } else {
                return VectorizedSparkParquetReaders.buildReader(
                    requiredSchema, fileSchema, idToConstant);
              }
            })
        .recordsPerBatch(parquetConf.batchSize())
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
        .recordsPerBatch(orcConf.batchSize())
        .filter(residual)
        .caseSensitive(caseSensitive())
        .withNameMapping(nameMapping())
        .build();
  }

  @VisibleForTesting
  static class BatchDeleteFilter {
    private final DeleteFilter<InternalRow> deletes;
    private boolean hasIsDeletedColumn;
    private int rowPositionColumnIndex = -1;

    BatchDeleteFilter(DeleteFilter<InternalRow> deletes) {
      this.deletes = deletes;

      Schema schema = deletes.requiredSchema();
      for (int i = 0; i < schema.columns().size(); i++) {
        if (schema.columns().get(i).fieldId() == MetadataColumns.ROW_POSITION.fieldId()) {
          this.rowPositionColumnIndex = i;
        } else if (schema.columns().get(i).fieldId() == MetadataColumns.IS_DELETED.fieldId()) {
          this.hasIsDeletedColumn = true;
        }
      }
    }

    ColumnarBatch filterBatch(ColumnarBatch batch) {
      if (!needDeletes()) {
        return batch;
      }

      ColumnVector[] vectors = new ColumnVector[batch.numCols()];
      for (int i = 0; i < batch.numCols(); i++) {
        vectors[i] = batch.column(i);
      }

      int numLiveRows = batch.numRows();
      long rowStartPosInBatch =
          rowPositionColumnIndex == -1 ? -1 : vectors[rowPositionColumnIndex].getLong(0);

      if (hasIsDeletedColumn) {
        boolean[] isDeleted =
            ColumnarBatchUtil.buildIsDeleted(vectors, deletes, rowStartPosInBatch, numLiveRows);
        for (ColumnVector vector : vectors) {
          if (vector instanceof UpdatableDeletedColumnVector) {
            ((UpdatableDeletedColumnVector) vector).setValue(isDeleted);
          }
        }
      } else {
        Pair<int[], Integer> pair =
            ColumnarBatchUtil.buildRowIdMapping(vectors, deletes, rowStartPosInBatch, numLiveRows);
        if (pair != null) {
          int[] rowIdMapping = pair.first();
          numLiveRows = pair.second();
          for (int i = 0; i < vectors.length; i++) {
            vectors[i] = new ColumnVectorWithFilter(vectors[i], rowIdMapping);
          }
        }
      }

      if (deletes != null && deletes.hasEqDeletes()) {
        vectors = ColumnarBatchUtil.removeExtraColumns(deletes, vectors);
      }

      ColumnarBatch output = new ColumnarBatch(vectors);
      output.setNumRows(numLiveRows);
      return output;
    }

    private boolean needDeletes() {
      return hasIsDeletedColumn
          || (deletes != null && (deletes.hasEqDeletes() || deletes.hasPosDeletes()));
    }
  }
}
