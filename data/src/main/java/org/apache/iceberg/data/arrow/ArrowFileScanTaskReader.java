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

package org.apache.iceberg.data.arrow;

import org.apache.arrow.vector.NullCheckingForGet;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Reads the data file and returns an iterator of {@link VectorSchemaRoot}. Only Parquet data file format is supported.
 */
public class ArrowFileScanTaskReader extends BaseArrowFileScanTaskReader<VectorSchemaRoot> {

  private final Schema expectedSchema;
  private final String nameMapping;
  private final boolean caseSensitive;
  private final int batchSize;
  private final boolean reuseContainers;

  /**
   * Create a new instance.
   *
   * @param task              Combined file scan task.
   * @param expectedSchema    Read schema. The returned data will have this schema.
   * @param nameMapping       Mapping from external schema names to Iceberg type IDs.
   * @param fileIo            File I/O.
   * @param encryptionManager Encryption manager.
   * @param caseSensitive     Indicates whether column names are case sensitive.
   * @param batchSize         Batch size in number of rows. Each Arrow batch ({@link VectorSchemaRoot}) contains a
   *                          maximum of {@code batchSize} rows.
   * @param reuseContainers   Reuse Arrow vectors from the previous {@link VectorSchemaRoot} in the next {@link
   *                          VectorSchemaRoot}.
   */
  ArrowFileScanTaskReader(
      CombinedScanTask task, Schema expectedSchema, String nameMapping, FileIO fileIo,
      EncryptionManager encryptionManager, boolean caseSensitive, int batchSize, boolean reuseContainers) {
    super(task, fileIo, encryptionManager);
    this.expectedSchema = expectedSchema;
    this.nameMapping = nameMapping;
    this.caseSensitive = caseSensitive;
    this.batchSize = batchSize;
    this.reuseContainers = reuseContainers;
  }

  @Override
  CloseableIterator<VectorSchemaRoot> open(FileScanTask task) {
    CloseableIterable<VectorSchemaRoot> iter;
    InputFile location = getInputFile(task);
    Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");
    if (task.file().format() == FileFormat.PARQUET) {
      Parquet.ReadBuilder builder = Parquet.read(location)
          .project(expectedSchema)
          .split(task.start(), task.length())
          .createBatchedReaderFunc(fileSchema -> VectorizedParquetReaders.buildReader(expectedSchema,
              fileSchema, /* setArrowValidityVector */ NullCheckingForGet.NULL_CHECKING_ENABLED))
          .recordsPerBatch(batchSize)
          .filter(task.residual())
          .caseSensitive(caseSensitive);

      if (reuseContainers) {
        builder.reuseContainers();
      }
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
}
