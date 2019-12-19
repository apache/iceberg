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
import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.vectorized.VectorizedSparkParquetReaders;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * Spark Task reading with Vectorization support
 */
public class VectorizedReading {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedReading.class);

  /**
   * Organizes input data into [InputPartition]s for Vectorized [ColumnarBatch] reads
   */
  public static class ReadTask implements InputPartition<ColumnarBatch>, Serializable {

    private final CombinedScanTask task;
    private final String tableSchemaString;
    private final String expectedSchemaString;
    private final FileIO fileIo;
    private final EncryptionManager encryptionManager;
    private final boolean caseSensitive;
    private final int numRecordsPerBatch;

    private transient Schema tableSchema = null;
    private transient Schema expectedSchema = null;

    ReadTask(
        CombinedScanTask task, String tableSchemaString, String expectedSchemaString, FileIO fileIo,
        EncryptionManager encryptionManager, boolean caseSensitive, int numRecordsPerBatch) {
      this.task = task;
      this.tableSchemaString = tableSchemaString;
      this.expectedSchemaString = expectedSchemaString;
      this.fileIo = fileIo;
      this.encryptionManager = encryptionManager;
      this.caseSensitive = caseSensitive;
      this.numRecordsPerBatch = numRecordsPerBatch;
      LOG.info("=> [BatchedReadTask] numRecordsPerBatch = {}", numRecordsPerBatch);
    }

    @Override
    public InputPartitionReader<ColumnarBatch> createPartitionReader() {
      return new VectorizedReading.TaskDataReader(task, lazyTableSchema(), lazyExpectedSchema(), fileIo,
          encryptionManager, caseSensitive, numRecordsPerBatch);
    }

    private Schema lazyTableSchema() {
      if (tableSchema == null) {
        this.tableSchema = SchemaParser.fromJson(tableSchemaString);
      }
      return tableSchema;
    }

    private Schema lazyExpectedSchema() {
      if (expectedSchema == null) {
        this.expectedSchema = SchemaParser.fromJson(expectedSchemaString);
      }
      return expectedSchema;
    }
  }

  public static final class TaskDataReader implements InputPartitionReader<ColumnarBatch> {

    private static final Logger LOG = LoggerFactory.getLogger(TaskDataReader.class);

    // for some reason, the apply method can't be called from Java without reflection
    private static final DynMethods.UnboundMethod APPLY_PROJECTION = DynMethods.builder("apply")
        .impl(UnsafeProjection.class, InternalRow.class)
        .build();

    private final Iterator<FileScanTask> tasks;
    private final Schema tableSchema;
    private final Schema expectedSchema;
    private final FileIO fileIo;
    private final Map<String, InputFile> inputFiles;
    private final boolean caseSensitive;
    private final Integer numRecordsPerBatch;

    private Iterator<ColumnarBatch> currentIterator;
    private Closeable currentCloseable = null;
    private ColumnarBatch current = null;

    TaskDataReader(
        CombinedScanTask task, Schema tableSchema, Schema expectedSchema, FileIO fileIo,
        EncryptionManager encryptionManager, boolean caseSensitive, int numRecordsPerBatch) {
      this.fileIo = fileIo;
      this.tasks = task.files().iterator();
      this.tableSchema = tableSchema;
      this.expectedSchema = expectedSchema;
      Iterable<InputFile> decryptedFiles = encryptionManager.decrypt(Iterables.transform(
          task.files(),
          fileScanTask ->
              EncryptedFiles.encryptedInput(
                  this.fileIo.newInputFile(fileScanTask.file().path().toString()),
                  fileScanTask.file().keyMetadata())));
      ImmutableMap.Builder<String, InputFile> inputFileBuilder = ImmutableMap.builder();
      decryptedFiles.forEach(decrypted -> inputFileBuilder.put(decrypted.location(), decrypted));
      this.inputFiles = inputFileBuilder.build();
      // open last because the schemas and fileIo must be set
      this.numRecordsPerBatch = numRecordsPerBatch;
      this.currentIterator = open(tasks.next());
      this.caseSensitive = caseSensitive;
      LOG.info("=> [TaskDataReader] numRecordsPerBatch = {}", numRecordsPerBatch);
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
        iter = open(task, requiredSchema);
      } else {
        iter = open(task, finalSchema);
      }
      return iter;
    }

    private Iterator<ColumnarBatch> open(FileScanTask task, Schema readSchema) {
      CloseableIterable<ColumnarBatch> iter;
      InputFile location = inputFiles.get(task.file().path().toString());
      Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");

      switch (task.file().format()) {
        case PARQUET:
          iter = newParquetIterable(location, task, readSchema);
          break;
        default:
          throw new UnsupportedOperationException(
              "Cannot read unknown format: " + task.file().format());
      }
      this.currentCloseable = iter;
      return iter.iterator();
    }

    private static UnsafeProjection projection(Schema finalSchema, Schema readSchema) {
      StructType struct = SparkSchemaUtil.convert(readSchema);

      List<AttributeReference> refs = JavaConverters.seqAsJavaListConverter(struct.toAttributes()).asJava();
      List<Attribute> attrs = Lists.newArrayListWithExpectedSize(struct.fields().length);
      List<org.apache.spark.sql.catalyst.expressions.Expression> exprs =
          Lists.newArrayListWithExpectedSize(struct.fields().length);

      for (AttributeReference ref : refs) {
        attrs.add(ref.toAttribute());
      }

      for (Types.NestedField field : finalSchema.columns()) {
        int indexInReadSchema = struct.fieldIndex(field.name());
        exprs.add(refs.get(indexInReadSchema));
      }

      return UnsafeProjection.create(
          JavaConverters.asScalaBufferConverter(exprs).asScala().toSeq(),
          JavaConverters.asScalaBufferConverter(attrs).asScala().toSeq());
    }

    private CloseableIterable<ColumnarBatch> newParquetIterable(
        InputFile location,
        FileScanTask task,
        Schema readSchema) {
      return Parquet.read(location)
          .project(readSchema)
          .split(task.start(), task.length())
          .enableBatchedRead()
          .createBatchedReaderFunc(fileSchema -> VectorizedSparkParquetReaders.buildReader(tableSchema, readSchema,
              fileSchema, numRecordsPerBatch))
          .filter(task.residual())
          .caseSensitive(caseSensitive)
          .recordsPerBatch(numRecordsPerBatch)
          .build();
    }
  }
}
