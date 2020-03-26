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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.SparkAvroReader;
import org.apache.iceberg.spark.data.SparkOrcReader;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

class RowDataReader extends BaseDataReader<InternalRow> {
  // for some reason, the apply method can't be called from Java without reflection
  private static final DynMethods.UnboundMethod APPLY_PROJECTION = DynMethods.builder("apply")
      .impl(UnsafeProjection.class, InternalRow.class)
      .build();

  private final Schema tableSchema;
  private final Schema expectedSchema;
  private final boolean caseSensitive;

  RowDataReader(
      CombinedScanTask task, Schema tableSchema, Schema expectedSchema, FileIO fileIo,
      EncryptionManager encryptionManager, boolean caseSensitive) {
    super(task, fileIo, encryptionManager);
    this.tableSchema = tableSchema;
    this.expectedSchema = expectedSchema;
    this.caseSensitive = caseSensitive;
  }

  @Override
  Iterator<InternalRow> open(FileScanTask task) {
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
    Iterator<InternalRow> iter;

    if (hasJoinedPartitionColumns) {
      // schema used to read data files
      Schema readSchema = TypeUtil.selectNot(requiredSchema, idColumns);
      Schema partitionSchema = TypeUtil.select(requiredSchema, idColumns);
      PartitionRowConverter convertToRow = new PartitionRowConverter(partitionSchema, spec);
      JoinedRow joined = new JoinedRow();

      InternalRow partition = convertToRow.apply(file.partition());
      joined.withRight(partition);

      // create joined rows and project from the joined schema to the final schema
      iterSchema = TypeUtil.join(readSchema, partitionSchema);
      iter = Iterators.transform(open(task, readSchema), joined::withLeft);
    } else if (hasExtraFilterColumns) {
      // add projection to the final schema
      iterSchema = requiredSchema;
      iter = open(task, requiredSchema);
    } else {
      // return the base iterator
      iterSchema = finalSchema;
      iter = open(task, finalSchema);
    }

    // TODO: remove the projection by reporting the iterator's schema back to Spark
    return Iterators.transform(
        iter,
        APPLY_PROJECTION.bind(projection(finalSchema, iterSchema))::invoke);
  }

  private Iterator<InternalRow> open(FileScanTask task, Schema readSchema) {
    CloseableIterable<InternalRow> iter;
    if (task.isDataTask()) {
      iter = newDataIterable(task.asDataTask(), readSchema);
    } else {
      InputFile location = getInputFile(task);
      Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");

      switch (task.file().format()) {
        case PARQUET:
          iter = newParquetIterable(location, task, readSchema);
          break;

        case AVRO:
          iter = newAvroIterable(location, task, readSchema);
          break;

        case ORC:
          iter = newOrcIterable(location, task, readSchema);
          break;

        default:
          throw new UnsupportedOperationException(
              "Cannot read unknown format: " + task.file().format());
      }
    }

    this.currentCloseable = iter;

    return iter.iterator();
  }

  private CloseableIterable<InternalRow> newAvroIterable(
      InputFile location,
      FileScanTask task,
      Schema readSchema) {
    return Avro.read(location)
        .reuseContainers()
        .project(readSchema)
        .split(task.start(), task.length())
        .createReaderFunc(SparkAvroReader::new)
        .build();
  }

  private CloseableIterable<InternalRow> newParquetIterable(
      InputFile location,
      FileScanTask task,
      Schema readSchema) {
    return Parquet.read(location)
        .project(readSchema)
        .split(task.start(), task.length())
        .createReaderFunc(fileSchema -> SparkParquetReaders.buildReader(readSchema, fileSchema))
        .filter(task.residual())
        .caseSensitive(caseSensitive)
        .build();
  }

  private CloseableIterable<InternalRow> newOrcIterable(
      InputFile location,
      FileScanTask task,
      Schema readSchema) {
    return ORC.read(location)
        .project(readSchema)
        .split(task.start(), task.length())
        .createReaderFunc(SparkOrcReader::new)
        .caseSensitive(caseSensitive)
        .build();
  }

  private CloseableIterable<InternalRow> newDataIterable(DataTask task, Schema readSchema) {
    StructInternalRow row = new StructInternalRow(tableSchema.asStruct());
    CloseableIterable<InternalRow> asSparkRows = CloseableIterable.transform(
        task.asDataTask().rows(), row::setStruct);
    return CloseableIterable.transform(
        asSparkRows, APPLY_PROJECTION.bind(projection(readSchema, tableSchema))::invoke);
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
}
