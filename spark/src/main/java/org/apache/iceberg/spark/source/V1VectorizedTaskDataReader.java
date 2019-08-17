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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.vectorized.MutableColumnarRow;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

class V1VectorizedTaskDataReader implements InputPartitionReader<ColumnarBatch> {

  private static final Logger LOG = LoggerFactory.getLogger(V1VectorizedTaskDataReader.class);

  private final Iterator<FileScanTask> tasks;
  private final Schema tableSchema;
  private final Schema expectedSchema;
  private final FileIO fileIo;
  private final Map<String, InputFile> inputFiles;
  private final boolean caseSensitive;
  private final scala.Function1<PartitionedFile, scala.collection.Iterator<InternalRow>> buildReaderFunc;

  private Iterator<ColumnarBatch> currentIterator = null;
  private Closeable currentCloseable = null;
  private ColumnarBatch current = null;

  V1VectorizedTaskDataReader(
      CombinedScanTask task, Schema tableSchema, Schema expectedSchema, FileIO fileIo,
      EncryptionManager encryptionManager, boolean caseSensitive,
      scala.Function1<PartitionedFile,
      scala.collection.Iterator<InternalRow>> buildReaderFunc) {

    this.fileIo = fileIo;
    this.tasks = task.files().iterator();
    this.tableSchema = tableSchema;
    this.expectedSchema = expectedSchema;
    Iterable<InputFile> decryptedFiles = encryptionManager.decrypt(Iterables.transform(task.files(),
        fileScanTask ->
            EncryptedFiles.encryptedInput(
                this.fileIo.newInputFile(fileScanTask.file().path().toString()),
                fileScanTask.file().keyMetadata())));
    ImmutableMap.Builder<String, InputFile> inputFileBuilder = ImmutableMap.builder();
    decryptedFiles.forEach(decrypted -> inputFileBuilder.put(decrypted.location(), decrypted));
    this.inputFiles = inputFileBuilder.build();
    // open last because the schemas and fileIo must be set
    this.caseSensitive = caseSensitive;
    this.buildReaderFunc = buildReaderFunc;

    // open after initializing everything
    this.currentIterator = open(tasks.next());
    LOG.warn("V1VectorizedTaskDataReader initialized.");
  }

  @Override
  public boolean next() throws IOException {
    while (true) {
      if (currentIterator.hasNext()) {
        this.current = currentIterator.next();
        return true;

      } else if (tasks.hasNext()) {
        this.currentCloseable.close();
        this.currentIterator = open(tasks.next());

      } else {
        return false;
      }
    }
  }

  @Override
  public ColumnarBatch get() {
    return current;
  }

  @Override
  public void close() throws IOException {
    // close the current iterator
    this.currentCloseable.close();

    // exhaust the task iterator
    while (tasks.hasNext()) {
      tasks.next();
    }
  }

  private Iterator<ColumnarBatch> open(FileScanTask task) {
    DataFile file = task.file();

    // schema or rows returned by readers
    Schema finalSchema = expectedSchema;
    PartitionSpec spec = task.spec();
    Set<Integer> idColumns = spec.identitySourceIds();

    // schema needed for the projection and filtering
    StructType sparkType = SparkSchemaUtil.convert(finalSchema);
    Schema requiredSchema = SparkSchemaUtil.prune(tableSchema, sparkType, task.residual(), caseSensitive);
    boolean hasExtraFilterColumns = requiredSchema.columns().size() != finalSchema.columns().size();

    Iterator<ColumnarBatch> iter;

    if (hasExtraFilterColumns) {
      // add projection to the final schema
      iter = open(task, requiredSchema);

    } else {
      // return the base iterator
      iter = open(task, finalSchema);
    }

    return iter;
  }

  private Iterator<ColumnarBatch> open(FileScanTask task, Schema readSchema) {
    // CloseableIterable<InternalRow> iter;
    BatchOverRowIterator batchOverRowIter = null;

    InputFile location = inputFiles.get(task.file().path().toString());
    Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");


    switch (task.file().format()) {
      case PARQUET:
        // V1 Vectorized reading
        scala.collection.Iterator<InternalRow> internalRowIter =
            buildV1VectorizedBatchReader(task);

        batchOverRowIter = new BatchOverRowIterator(JavaConverters.asJavaIteratorConverter(internalRowIter).asJava());
        break;

      default:
        throw new UnsupportedOperationException(
            "Cannot read unknown format: " + task.file().format());
    }

    this.currentCloseable = batchOverRowIter;

    return batchOverRowIter;
  }


  private scala.collection.Iterator<InternalRow> buildV1VectorizedBatchReader(FileScanTask task) {

    Class<?> clazz = PartitionedFile.class;
    Constructor<?> ctor = clazz.getConstructors()[0]; // we know PartitionedFile has only one constructor
    PartitionedFile partitionedFile;
    try {
      // Pick partition fields
      partitionedFile = (PartitionedFile)
          ctor.newInstance(InternalRow.empty(),   task.file().path().toString(),
              task.start(), task.length(), null);
    } catch (Throwable t) {
      throw new RuntimeException("Could not instantiate PartitionedFile", t);
    }

    return buildReaderFunc.apply(partitionedFile);
  }

  private static class BatchOverRowIterator implements Iterator<ColumnarBatch>, Closeable {

    private Iterator<InternalRow> rowIterator;

    BatchOverRowIterator(Iterator<InternalRow> rowIterator) {

      this.rowIterator = rowIterator;
    }

    @Override
    public boolean hasNext() {
      return rowIterator.hasNext();
    }

    @Override
    public ColumnarBatch next() {

      Object object = rowIterator.next();

      if (object instanceof ColumnarBatch) {

        ColumnarBatch batch = (ColumnarBatch) object;
        return batch;
      } else {

        MutableColumnarRow columnarRow = (MutableColumnarRow) object;
      }

      throw new RuntimeException("Object not of [ColumnarBatch] type, " +
          "found [" + object.getClass().getCanonicalName() + "]");
    }

    @Override
    public void close() throws IOException {

    }
  }
}
