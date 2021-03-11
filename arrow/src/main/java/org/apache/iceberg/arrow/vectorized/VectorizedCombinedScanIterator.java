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

package org.apache.iceberg.arrow.vectorized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.vector.NullCheckingForGet;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the data file and returns an iterator of {@link VectorSchemaRoot}.
 * Only Parquet data file format is supported.
 */
class VectorizedCombinedScanIterator implements CloseableIterator<ArrowBatch> {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedCombinedScanIterator.class);

  private final Iterator<FileScanTask> tasks;
  private final Map<String, InputFile> inputFiles;
  private final Schema expectedSchema;
  private final String nameMapping;
  private final boolean caseSensitive;
  private final int batchSize;
  private final boolean reuseContainers;
  private CloseableIterator<ArrowBatch> currentIterator;
  private ArrowBatch current;
  private FileScanTask currentTask;

  /**
   * Create a new instance.
   *
   * @param task              Combined file scan task.
   * @param expectedSchema    Read schema. The returned data will have this schema.
   * @param nameMapping       Mapping from external schema names to Iceberg type IDs.
   * @param io                File I/O.
   * @param encryptionManager Encryption manager.
   * @param caseSensitive     If {@code true}, column names are case sensitive.
   *                          If {@code false}, column names are not case sensitive.
   * @param batchSize         Batch size in number of rows. Each Arrow batch contains
   *                          a maximum of {@code batchSize} rows.
   * @param reuseContainers   If set to {@code false}, every {@link Iterator#next()} call creates
   *                          new instances of Arrow vectors.
   *                          If set to {@code true}, the Arrow vectors in the previous
   *                          {@link Iterator#next()} may be reused for the data returned
   *                          in the current {@link Iterator#next()}.
   *                          This option avoids allocating memory again and again.
   *                          Irrespective of the value of {@code reuseContainers}, the Arrow vectors
   *                          in the previous {@link Iterator#next()} call are closed before creating
   *                          new instances if the current {@link Iterator#next()}.
   */
  VectorizedCombinedScanIterator(
          CombinedScanTask task,
          Schema expectedSchema,
          String nameMapping,
          FileIO io,
          EncryptionManager encryptionManager,
          boolean caseSensitive,
          int batchSize,
          boolean reuseContainers) {
    this.tasks = task.files().iterator();
    Map<String, ByteBuffer> keyMetadata = Maps.newHashMap();
    task.files().stream()
        .flatMap(fileScanTask -> Stream.concat(Stream.of(fileScanTask.file()), fileScanTask.deletes().stream()))
        .forEach(file -> keyMetadata.put(file.path().toString(), file.keyMetadata()));
    Stream<EncryptedInputFile> encrypted = keyMetadata.entrySet().stream()
        .map(entry -> EncryptedFiles.encryptedInput(io.newInputFile(entry.getKey()), entry.getValue()));

    // decrypt with the batch call to avoid multiple RPCs to a key server, if possible
    Iterable<InputFile> decryptedFiles = encryptionManager.decrypt(encrypted::iterator);

    Map<String, InputFile> files = Maps.newHashMapWithExpectedSize(task.files().size());
    decryptedFiles.forEach(decrypted -> files.putIfAbsent(decrypted.location(), decrypted));
    this.inputFiles = Collections.unmodifiableMap(files);

    this.currentIterator = CloseableIterator.empty();
    this.expectedSchema = expectedSchema;
    this.nameMapping = nameMapping;
    this.caseSensitive = caseSensitive;
    this.batchSize = batchSize;
    this.reuseContainers = reuseContainers;
  }

  @Override
  public boolean hasNext() {
    try {
      while (true) {
        if (currentIterator.hasNext()) {
          this.current = currentIterator.next();
          return true;
        } else if (tasks.hasNext()) {
          this.currentIterator.close();
          this.currentTask = tasks.next();
          this.currentIterator = open(currentTask);
        } else {
          this.currentIterator.close();
          return false;
        }
      }
    } catch (IOException | RuntimeException e) {
      if (currentTask != null && !currentTask.isDataTask()) {
        LOG.error("Error reading file: {}", getInputFile(currentTask).location(), e);
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public ArrowBatch next() {
    return current;
  }

  CloseableIterator<ArrowBatch> open(FileScanTask task) {
    CloseableIterable<ArrowBatch> iter;
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

  @Override
  public void close() throws IOException {
    // close the current iterator
    this.currentIterator.close();

    // exhaust the task iterator
    while (tasks.hasNext()) {
      tasks.next();
    }
  }

  private InputFile getInputFile(FileScanTask task) {
    Preconditions.checkArgument(!task.isDataTask(), "Invalid task type");
    return inputFiles.get(task.file().path().toString());
  }
}
