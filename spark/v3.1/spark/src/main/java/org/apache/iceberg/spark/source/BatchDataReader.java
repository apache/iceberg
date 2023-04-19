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
import org.apache.arrow.vector.NullCheckingForGet;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;

class BatchDataReader extends BaseDataReader<ColumnarBatch> {
  private final Schema expectedSchema;
  private final String nameMapping;
  private final boolean caseSensitive;
  private final int batchSize;

  BatchDataReader(
      CombinedScanTask task, Table table, Schema expectedSchema, boolean caseSensitive, int size) {
    super(table, task);
    this.expectedSchema = expectedSchema;
    this.nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    this.caseSensitive = caseSensitive;
    this.batchSize = size;
  }

  @Override
  CloseableIterator<ColumnarBatch> open(FileScanTask task) {
    DataFile file = task.file();

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(file.path().toString(), task.start(), task.length());

    Map<Integer, ?> idToConstant = constantsMap(task, expectedSchema);

    CloseableIterable<ColumnarBatch> iter;
    InputFile location = getInputFile(task);
    Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");
    if (task.file().format() == FileFormat.PARQUET) {
      SparkDeleteFilter deleteFilter = deleteFilter(task);

      // get required schema if there are deletes
      Schema requiredSchema = deleteFilter != null ? deleteFilter.requiredSchema() : expectedSchema;

      Parquet.ReadBuilder builder =
          Parquet.read(location)
              .project(requiredSchema)
              .split(task.start(), task.length())
              .createBatchedReaderFunc(
                  fileSchema ->
                      VectorizedSparkParquetReaders.buildReader(
                          requiredSchema,
                          fileSchema, /* setArrowValidityVector */
                          NullCheckingForGet.NULL_CHECKING_ENABLED,
                          idToConstant,
                          deleteFilter))
              .recordsPerBatch(batchSize)
              .filter(task.residual())
              .caseSensitive(caseSensitive)
              // Spark eagerly consumes the batches. So the underlying memory allocated could be
              // reused
              // without worrying about subsequent reads clobbering over each other. This improves
              // read performance as every batch read doesn't have to pay the cost of allocating
              // memory.
              .reuseContainers();

      if (nameMapping != null) {
        builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
      }

      iter = builder.build();
    } else if (task.file().format() == FileFormat.ORC) {
      Set<Integer> constantFieldIds = idToConstant.keySet();
      Set<Integer> metadataFieldIds = MetadataColumns.metadataFieldIds();
      Sets.SetView<Integer> constantAndMetadataFieldIds =
          Sets.union(constantFieldIds, metadataFieldIds);
      Schema schemaWithoutConstantAndMetadataFields =
          TypeUtil.selectNot(expectedSchema, constantAndMetadataFieldIds);
      ORC.ReadBuilder builder =
          ORC.read(location)
              .project(schemaWithoutConstantAndMetadataFields)
              .split(task.start(), task.length())
              .createBatchedReaderFunc(
                  fileSchema ->
                      VectorizedSparkOrcReaders.buildReader(
                          expectedSchema, fileSchema, idToConstant))
              .recordsPerBatch(batchSize)
              .filter(task.residual())
              .caseSensitive(caseSensitive);

      if (nameMapping != null) {
        builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
      }

      iter = builder.build();
    } else {
      throw new UnsupportedOperationException(
          "Format: " + task.file().format() + " not supported for batched reads");
    }
    return iter.iterator();
  }

  private SparkDeleteFilter deleteFilter(FileScanTask task) {
    return task.deletes().isEmpty()
        ? null
        : new SparkDeleteFilter(task, table().schema(), expectedSchema);
  }

  private class SparkDeleteFilter extends DeleteFilter<InternalRow> {
    private final InternalRowWrapper asStructLike;

    SparkDeleteFilter(FileScanTask task, Schema tableSchema, Schema requestedSchema) {
      super(task.file().path().toString(), task.deletes(), tableSchema, requestedSchema);
      this.asStructLike = new InternalRowWrapper(SparkSchemaUtil.convert(requiredSchema()));
    }

    @Override
    protected StructLike asStructLike(InternalRow row) {
      return asStructLike.wrap(row);
    }

    @Override
    protected InputFile getInputFile(String location) {
      return BatchDataReader.this.getInputFile(location);
    }
  }
}
