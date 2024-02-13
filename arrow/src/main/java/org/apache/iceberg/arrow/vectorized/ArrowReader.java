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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.arrow.vector.NullCheckingForGet;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ExceptionUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vectorized reader that returns an iterator of {@link ColumnarBatch}. See {@link
 * #open(CloseableIterable)} ()} to learn about the behavior of the iterator.
 *
 * <p>The following Iceberg data types are supported and have been tested:
 *
 * <ul>
 *   <li>Iceberg: {@link Types.BooleanType}, Arrow: {@link MinorType#BIT}
 *   <li>Iceberg: {@link Types.IntegerType}, Arrow: {@link MinorType#INT}
 *   <li>Iceberg: {@link Types.LongType}, Arrow: {@link MinorType#BIGINT}
 *   <li>Iceberg: {@link Types.FloatType}, Arrow: {@link MinorType#FLOAT4}
 *   <li>Iceberg: {@link Types.DoubleType}, Arrow: {@link MinorType#FLOAT8}
 *   <li>Iceberg: {@link Types.StringType}, Arrow: {@link MinorType#VARCHAR}
 *   <li>Iceberg: {@link Types.TimestampType} (both with and without timezone), Arrow: {@link
 *       MinorType#TIMEMICRO}
 *   <li>Iceberg: {@link Types.BinaryType}, Arrow: {@link MinorType#VARBINARY}
 *   <li>Iceberg: {@link Types.DateType}, Arrow: {@link MinorType#DATEDAY}
 *   <li>Iceberg: {@link Types.TimeType}, Arrow: {@link MinorType#TIMEMICRO}
 *   <li>Iceberg: {@link Types.UUIDType}, Arrow: {@link MinorType#FIXEDSIZEBINARY}(16)
 * </ul>
 *
 * <p>Features that don't work in this implementation:
 *
 * <ul>
 *   <li>Type promotion: In case of type promotion, the Arrow vector corresponding to the data type
 *       in the parquet file is returned instead of the data type in the latest schema. See
 *       https://github.com/apache/iceberg/issues/2483.
 *   <li>Columns with constant values are physically encoded as a dictionary. The Arrow vector type
 *       is int32 instead of the type as per the schema. See
 *       https://github.com/apache/iceberg/issues/2484.
 *   <li>Data types: {@link Types.ListType}, {@link Types.MapType}, {@link Types.StructType}, {@link
 *       Types.FixedType} and {@link Types.DecimalType} See
 *       https://github.com/apache/iceberg/issues/2485 and
 *       https://github.com/apache/iceberg/issues/2486.
 *   <li>Delete files are not supported. See https://github.com/apache/iceberg/issues/2487.
 * </ul>
 */
public class ArrowReader extends CloseableGroup {
  private static final Logger LOG = LoggerFactory.getLogger(ArrowReader.class);

  private static final Set<TypeID> SUPPORTED_TYPES =
      ImmutableSet.of(
          TypeID.BOOLEAN,
          TypeID.INTEGER,
          TypeID.LONG,
          TypeID.FLOAT,
          TypeID.DOUBLE,
          TypeID.STRING,
          TypeID.TIMESTAMP,
          TypeID.BINARY,
          TypeID.DATE,
          TypeID.UUID,
          TypeID.TIME,
          TypeID.DECIMAL);

  private final Schema schema;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final int batchSize;
  private final boolean reuseContainers;

  /**
   * Create a new instance of the reader.
   *
   * @param scan the table scan object.
   * @param batchSize the maximum number of rows per Arrow batch.
   * @param reuseContainers whether to reuse Arrow vectors when iterating through the data. If set
   *     to {@code false}, every {@link Iterator#next()} call creates new instances of Arrow
   *     vectors. If set to {@code true}, the Arrow vectors in the previous {@link Iterator#next()}
   *     may be reused for the data returned in the current {@link Iterator#next()}. This option
   *     avoids allocating memory again and again. Irrespective of the value of {@code
   *     reuseContainers}, the Arrow vectors in the previous {@link Iterator#next()} call are closed
   *     before creating new instances if the current {@link Iterator#next()}.
   */
  public ArrowReader(TableScan scan, int batchSize, boolean reuseContainers) {
    this.schema = scan.schema();
    this.io = scan.table().io();
    this.encryption = scan.table().encryption();
    this.batchSize = batchSize;
    // start planning tasks in the background
    this.reuseContainers = reuseContainers;
  }

  /**
   * Returns a new iterator of {@link ColumnarBatch} objects.
   *
   * <p>Note that the reader owns the {@link ColumnarBatch} objects and takes care of closing them.
   * The caller should not hold onto a {@link ColumnarBatch} or try to close them.
   *
   * <p>If {@code reuseContainers} is {@code false}, the Arrow vectors in the previous {@link
   * ColumnarBatch} are closed before returning the next {@link ColumnarBatch} object. This implies
   * that the caller should either use the {@link ColumnarBatch} or transfer the ownership of {@link
   * ColumnarBatch} before getting the next {@link ColumnarBatch}.
   *
   * <p>If {@code reuseContainers} is {@code true}, the Arrow vectors in the previous {@link
   * ColumnarBatch} may be reused for the next {@link ColumnarBatch}. This implies that the caller
   * should either use the {@link ColumnarBatch} or deep copy the {@link ColumnarBatch} before
   * getting the next {@link ColumnarBatch}.
   *
   * <p>This method works for only when the following conditions are true:
   *
   * <ol>
   *   <li>At least one column is queried,
   *   <li>There are no delete files, and
   *   <li>Supported data types are queried (see {@link #SUPPORTED_TYPES}).
   * </ol>
   *
   * When any of these conditions fail, an {@link UnsupportedOperationException} is thrown.
   */
  public CloseableIterator<ColumnarBatch> open(CloseableIterable<CombinedScanTask> tasks) {
    CloseableIterator<ColumnarBatch> itr =
        new VectorizedCombinedScanIterator(
            tasks, schema, null, io, encryption, true, batchSize, reuseContainers);
    addCloseable(itr);
    return itr;
  }

  @Override
  public void close() throws IOException {
    super.close(); // close data files
  }

  /**
   * Reads the data file and returns an iterator of {@link VectorSchemaRoot}. Only Parquet data file
   * format is supported.
   */
  private static final class VectorizedCombinedScanIterator
      implements CloseableIterator<ColumnarBatch> {

    private final Iterator<FileScanTask> fileItr;
    private final Map<String, InputFile> inputFiles;
    private final Schema expectedSchema;
    private final String nameMapping;
    private final boolean caseSensitive;
    private final int batchSize;
    private final boolean reuseContainers;
    private CloseableIterator<ColumnarBatch> currentIterator;
    private FileScanTask currentTask;

    /**
     * Create a new instance.
     *
     * @param tasks Combined file scan tasks.
     * @param expectedSchema Read schema. The returned data will have this schema.
     * @param nameMapping Mapping from external schema names to Iceberg type IDs.
     * @param io File I/O.
     * @param encryptionManager Encryption manager.
     * @param caseSensitive If {@code true}, column names are case sensitive. If {@code false},
     *     column names are not case sensitive.
     * @param batchSize Batch size in number of rows. Each Arrow batch contains a maximum of {@code
     *     batchSize} rows.
     * @param reuseContainers If set to {@code false}, every {@link Iterator#next()} call creates
     *     new instances of Arrow vectors. If set to {@code true}, the Arrow vectors in the previous
     *     {@link Iterator#next()} may be reused for the data returned in the current {@link
     *     Iterator#next()}. This option avoids allocating memory again and again. Irrespective of
     *     the value of {@code reuseContainers}, the Arrow vectors in the previous {@link
     *     Iterator#next()} call are closed before creating new instances if the current {@link
     *     Iterator#next()}.
     */
    VectorizedCombinedScanIterator(
        CloseableIterable<CombinedScanTask> tasks,
        Schema expectedSchema,
        String nameMapping,
        FileIO io,
        EncryptionManager encryptionManager,
        boolean caseSensitive,
        int batchSize,
        boolean reuseContainers) {
      List<FileScanTask> fileTasks =
          StreamSupport.stream(tasks.spliterator(), false)
              .map(CombinedScanTask::files)
              .flatMap(Collection::stream)
              .collect(Collectors.toList());
      this.fileItr = fileTasks.iterator();

      if (fileTasks.stream().anyMatch(TableScanUtil::hasDeletes)) {
        throw new UnsupportedOperationException(
            "Cannot read files that require applying delete files");
      }

      if (expectedSchema.columns().isEmpty()) {
        throw new UnsupportedOperationException(
            "Cannot read without at least one projected column");
      }

      Set<TypeID> unsupportedTypes =
          Sets.difference(
              expectedSchema.columns().stream()
                  .map(c -> c.type().typeId())
                  .collect(Collectors.toSet()),
              SUPPORTED_TYPES);
      if (!unsupportedTypes.isEmpty()) {
        throw new UnsupportedOperationException(
            "Cannot read unsupported column types: " + unsupportedTypes);
      }

      Map<String, ByteBuffer> keyMetadata = Maps.newHashMap();
      fileTasks.stream()
          .map(FileScanTask::file)
          .forEach(file -> keyMetadata.put(file.path().toString(), file.keyMetadata()));

      Stream<EncryptedInputFile> encrypted =
          keyMetadata.entrySet().stream()
              .map(
                  entry ->
                      EncryptedFiles.encryptedInput(
                          io.newInputFile(entry.getKey()), entry.getValue()));

      // decrypt with the batch call to avoid multiple RPCs to a key server, if possible
      @SuppressWarnings("StreamToIterable")
      Iterable<InputFile> decryptedFiles = encryptionManager.decrypt(encrypted::iterator);

      Map<String, InputFile> files = Maps.newHashMapWithExpectedSize(fileTasks.size());
      decryptedFiles.forEach(decrypted -> files.putIfAbsent(decrypted.location(), decrypted));
      this.inputFiles = ImmutableMap.copyOf(files);
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
            return true;
          } else if (fileItr.hasNext()) {
            this.currentIterator.close();
            this.currentTask = fileItr.next();
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
        ExceptionUtil.castAndThrow(e, RuntimeException.class);
        return false;
      }
    }

    @Override
    public ColumnarBatch next() {
      if (hasNext()) {
        return currentIterator.next();
      } else {
        throw new NoSuchElementException();
      }
    }

    CloseableIterator<ColumnarBatch> open(FileScanTask task) {
      CloseableIterable<ColumnarBatch> iter;
      InputFile location = getInputFile(task);
      Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");
      if (task.file().format() == FileFormat.PARQUET) {
        Parquet.ReadBuilder builder =
            Parquet.read(location)
                .project(expectedSchema)
                .split(task.start(), task.length())
                .createBatchedReaderFunc(
                    fileSchema ->
                        buildReader(
                            expectedSchema,
                            fileSchema, /* setArrowValidityVector */
                            NullCheckingForGet.NULL_CHECKING_ENABLED))
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
      while (fileItr.hasNext()) {
        fileItr.next();
      }
    }

    private InputFile getInputFile(FileScanTask task) {
      Preconditions.checkArgument(!task.isDataTask(), "Invalid task type");
      return inputFiles.get(task.file().path().toString());
    }

    /**
     * Build the {@link ArrowBatchReader} for the expected schema and file schema.
     *
     * @param expectedSchema Expected schema of the data returned.
     * @param fileSchema Schema of the data file.
     * @param setArrowValidityVector Indicates whether to set the validity vector in Arrow vectors.
     */
    private static ArrowBatchReader buildReader(
        Schema expectedSchema, MessageType fileSchema, boolean setArrowValidityVector) {
      VectorizedReaderBuilder vectorizedReaderBuilder = new VectorizedReaderBuilder();
      vectorizedReaderBuilder.initialize(
          expectedSchema,
          fileSchema,
          setArrowValidityVector,
          ImmutableMap.of(),
          readers -> {
            ArrowBatchReader batchReader = new ArrowBatchReader();
            batchReader.initialize(readers);
            return batchReader;
          });
      return (ArrowBatchReader)
          TypeWithSchemaVisitor.visit(
              expectedSchema.asStruct(), fileSchema, vectorizedReaderBuilder);
    }
  }
}
