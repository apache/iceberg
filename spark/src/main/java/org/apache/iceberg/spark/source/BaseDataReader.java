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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

/**
 * Base class of readers of type {@link InputPartitionReader} to read data as objects of type @param &lt;T&gt;
 *
 * @param <T> is the Java class returned by this reader whose objects contain one or more rows.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
abstract class BaseDataReader<T> implements InputPartitionReader<T> {
  // for some reason, the apply method can't be called from Java without reflection
  static final DynMethods.UnboundMethod APPLY_PROJECTION = DynMethods.builder("apply")
      .impl(UnsafeProjection.class, InternalRow.class)
      .build();

  final Schema tableSchema;
  private final Schema expectedSchema;
  final boolean caseSensitive;

  private final Iterator<FileScanTask> tasks;
  private final FileIO fileIo;
  private final Map<String, InputFile> inputFiles;

  private Iterator<T> currentIterator;
  Closeable currentCloseable;
  private T current = null;

  BaseDataReader(CombinedScanTask task, FileIO fileIo, EncryptionManager encryptionManager, Schema tableSchema,
      Schema expectedSchema, boolean caseSensitive) {
    this.tableSchema = tableSchema;
    this.expectedSchema = expectedSchema;
    this.caseSensitive = caseSensitive;
    this.fileIo = fileIo;
    this.tasks = task.files().iterator();
    Iterable<InputFile> decryptedFiles = encryptionManager.decrypt(Iterables.transform(
        task.files(),
        fileScanTask ->
            EncryptedFiles.encryptedInput(
                this.fileIo.newInputFile(fileScanTask.file().path().toString()),
                fileScanTask.file().keyMetadata())));
    ImmutableMap.Builder<String, InputFile> inputFileBuilder = ImmutableMap.builder();
    decryptedFiles.forEach(decrypted -> inputFileBuilder.put(decrypted.location(), decrypted));
    this.inputFiles = inputFileBuilder.build();
    this.currentCloseable = CloseableIterable.empty();
    this.currentIterator = Collections.emptyIterator();
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
  public T get() {
    return current;
  }

  /**
   * Return a {@link Pair} of {@link Schema} and {@link Iterator} over records of type T that include the identity
   * partition columns being projected.
   */
  abstract Pair<Schema, Iterator<T>> getJoinedSchemaAndIteratorWithIdentityPartition(
      DataFile file, FileScanTask task,
      Schema requiredSchema, Set<Integer> idColumns, PartitionSpec spec);

  abstract Iterator<T> open(FileScanTask task, Schema readSchema, Map<Integer, ?> idToConstant);

  private Iterator<T> open(FileScanTask task) {
    DataFile file = task.file();

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(file.path().toString(), task.start(), task.length());

    // schema or rows returned by readers
    Schema finalSchema = expectedSchema;
    PartitionSpec spec = task.spec();
    Set<Integer> idColumns = spec.identitySourceIds();

    // schema needed for the projection and filtering
    StructType sparkType = SparkSchemaUtil.convert(finalSchema);
    Schema requiredSchema = SparkSchemaUtil.prune(tableSchema, sparkType, task.residual(), caseSensitive);
    boolean hasJoinedPartitionColumns = !idColumns.isEmpty();
    boolean hasExtraFilterColumns = requiredSchema.columns().size() != finalSchema.columns().size();

    Schema iterSchema;
    Iterator<T> iter;

    if (hasJoinedPartitionColumns) {
      Pair<Schema, Iterator<T>> pair = getJoinedSchemaAndIteratorWithIdentityPartition(file, task, requiredSchema,
          idColumns, spec);
      iterSchema = pair.first();
      iter = pair.second();
    } else if (hasExtraFilterColumns) {
      // add projection to the final schema
      iterSchema = requiredSchema;
      iter = open(task, requiredSchema, ImmutableMap.of());
    } else {
      // return the base iterator
      iterSchema = finalSchema;
      iter = open(task, finalSchema, ImmutableMap.of());
    }

    // TODO: remove the projection by reporting the iterator's schema back to Spark
    return Iterators.transform(
        iter,
        APPLY_PROJECTION.bind(projection(finalSchema, iterSchema))::invoke);
  }

  static UnsafeProjection projection(Schema finalSchema, Schema readSchema) {
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

  @Override
  public void close() throws IOException {
    InputFileBlockHolder.unset();

    // close the current iterator
    this.currentCloseable.close();

    // exhaust the task iterator
    while (tasks.hasNext()) {
      tasks.next();
    }
  }

  InputFile getInputFile(FileScanTask task) {
    Preconditions.checkArgument(!task.isDataTask(), "Invalid task type");
    return inputFiles.get(task.file().path().toString());
  }
}
