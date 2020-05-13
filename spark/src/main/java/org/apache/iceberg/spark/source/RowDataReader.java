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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.SparkAvroReader;
import org.apache.iceberg.spark.data.SparkOrcReader;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConverters;

class RowDataReader extends BaseDataReader<InternalRow> {
  private static final Set<FileFormat> SUPPORTS_CONSTANTS = Sets.newHashSet(FileFormat.AVRO, FileFormat.PARQUET);
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
  CloseableIterator<InternalRow> open(FileScanTask task) {
    DataFile file = task.file();

    // update the current file for Spark's filename() function
    InputFileBlockHolder.set(file.path().toString(), task.start(), task.length());

    // schema or rows returned by readers
    PartitionSpec spec = task.spec();
    Set<Integer> idColumns = spec.identitySourceIds();
    Schema partitionSchema = TypeUtil.select(expectedSchema, idColumns);
    boolean projectsIdentityPartitionColumns = !partitionSchema.columns().isEmpty();

    if (projectsIdentityPartitionColumns) {
      if (SUPPORTS_CONSTANTS.contains(file.format())) {
        return open(task, expectedSchema, PartitionUtil.constantsMap(task, RowDataReader::convertConstant))
            .iterator();
      }

      // schema used to read data files
      Schema readSchema = TypeUtil.selectNot(expectedSchema, idColumns);
      PartitionRowConverter convertToRow = new PartitionRowConverter(partitionSchema, spec);
      JoinedRow joined = new JoinedRow();

      // create joined rows and project from the joined schema to the final schema
      Schema joinedSchema = TypeUtil.join(readSchema, partitionSchema);
      InternalRow partition = convertToRow.apply(file.partition());
      joined.withRight(partition);

      CloseableIterable<InternalRow> transformedIterable = CloseableIterable.transform(
          CloseableIterable.transform(open(task, readSchema, ImmutableMap.of()), joined::withLeft),
          APPLY_PROJECTION.bind(projection(expectedSchema, joinedSchema))::invoke);
      return transformedIterable.iterator();
    }

    // return the base iterator
    return open(task, expectedSchema, ImmutableMap.of()).iterator();
  }

  private CloseableIterable<InternalRow> open(FileScanTask task, Schema readSchema, Map<Integer, ?> idToConstant) {
    CloseableIterable<InternalRow> iter;
    if (task.isDataTask()) {
      iter = newDataIterable(task.asDataTask(), readSchema);
    } else {
      InputFile location = getInputFile(task);
      Preconditions.checkNotNull(location, "Could not find InputFile associated with FileScanTask");

      switch (task.file().format()) {
        case PARQUET:
          iter = newParquetIterable(location, task, readSchema, idToConstant);
          break;

        case AVRO:
          iter = newAvroIterable(location, task, readSchema, idToConstant);
          break;

        case ORC:
          iter = newOrcIterable(location, task, readSchema);
          break;

        default:
          throw new UnsupportedOperationException(
              "Cannot read unknown format: " + task.file().format());
      }
    }

    return iter;
  }

  private CloseableIterable<InternalRow> newAvroIterable(
      InputFile location,
      FileScanTask task,
      Schema projection,
      Map<Integer, ?> idToConstant) {
    return Avro.read(location)
        .reuseContainers()
        .project(projection)
        .split(task.start(), task.length())
        .createReaderFunc(readSchema -> new SparkAvroReader(projection, readSchema, idToConstant))
        .build();
  }

  private CloseableIterable<InternalRow> newParquetIterable(
      InputFile location,
      FileScanTask task,
      Schema readSchema,
      Map<Integer, ?> idToConstant) {
    return Parquet.read(location)
        .project(readSchema)
        .split(task.start(), task.length())
        .createReaderFunc(fileSchema -> SparkParquetReaders.buildReader(readSchema, fileSchema, idToConstant))
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

  private static Object convertConstant(Type type, Object value) {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
      case DECIMAL:
        return Decimal.apply((BigDecimal) value);
      case STRING:
        if (value instanceof Utf8) {
          Utf8 utf8 = (Utf8) value;
          return UTF8String.fromBytes(utf8.getBytes(), 0, utf8.getByteLength());
        }
        return UTF8String.fromString(value.toString());
      case FIXED:
        if (value instanceof byte[]) {
          return value;
        } else if (value instanceof GenericData.Fixed) {
          return ((GenericData.Fixed) value).bytes();
        }
        return ByteBuffers.toByteArray((ByteBuffer) value);
      case BINARY:
        return ByteBuffers.toByteArray((ByteBuffer) value);
      default:
    }
    return value;
  }
}
