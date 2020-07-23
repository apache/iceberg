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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkOrcReaders;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.vectorized.ColumnarBatch;

class BatchDataReader extends BaseDataReader<ColumnarBatch> {
  private final Schema expectedSchema;
  private final String nameMapping;
  private final boolean caseSensitive;
  private final int batchSize;

  BatchDataReader(
      CombinedScanTask task, Schema expectedSchema, String nameMapping, FileIO fileIo,
      EncryptionManager encryptionManager, boolean caseSensitive, int size) {
    super(task, fileIo, encryptionManager);
    this.expectedSchema = expectedSchema;
    this.nameMapping = nameMapping;
    this.caseSensitive = caseSensitive;
    this.batchSize = size;
  }

  @Override
  CloseableIterator<ColumnarBatch> open(FileScanTask task) {
    DataFile file = task.file();

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(file.path().toString(), task.start(), task.length());

    // schema or rows returned by readers
    PartitionSpec spec = task.spec();
    Set<Integer> idColumns = spec.identitySourceIds();
    Schema partitionSchema = TypeUtil.select(expectedSchema, idColumns);
    boolean projectsIdentityPartitionColumns = !partitionSchema.columns().isEmpty();

    Map<Integer, ?> idToConstant;
    if (projectsIdentityPartitionColumns) {
      idToConstant = PartitionUtil.constantsMap(task, BatchDataReader::convertConstant);
    } else {
      idToConstant = ImmutableMap.of();
    }

    CloseableIterable<ColumnarBatch> iter;
    InputFile location = getInputFile(task);
    Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");
    if (task.file().format() == FileFormat.PARQUET) {
      Parquet.ReadBuilder builder = Parquet.read(location)
          .project(expectedSchema)
          .split(task.start(), task.length())
          .createBatchedReaderFunc(fileSchema -> VectorizedSparkParquetReaders.buildReader(expectedSchema,
              fileSchema, /* setArrowValidityVector */ NullCheckingForGet.NULL_CHECKING_ENABLED))
          .recordsPerBatch(batchSize)
          .filter(task.residual())
          .caseSensitive(caseSensitive)
          // Spark eagerly consumes the batches. So the underlying memory allocated could be reused
          // without worrying about subsequent reads clobbering over each other. This improves
          // read performance as every batch read doesn't have to pay the cost of allocating memory.
          .reuseContainers();

      if (nameMapping != null) {
        builder.withNameMapping(NameMappingParser.fromJson(nameMapping));
      }

      iter = builder.build();
    } else if (task.file().format() == FileFormat.ORC) {
      Schema schemaWithoutConstants = TypeUtil.selectNot(expectedSchema, idToConstant.keySet());
      iter = ORC.read(location)
          .project(schemaWithoutConstants)
          .split(task.start(), task.length())
          .createBatchedReaderFunc(fileSchema -> VectorizedSparkOrcReaders.buildReader(expectedSchema, fileSchema,
              idToConstant))
          .recordsPerBatch(batchSize)
          .filter(task.residual())
          .caseSensitive(caseSensitive)
          .build();
    } else {
      throw new UnsupportedOperationException(
          "Format: " + task.file().format() + " not supported for batched reads");
    }
    return iter.iterator();
  }
}
