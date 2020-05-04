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
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Iterator;
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
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.SparkAvroReader;
import org.apache.iceberg.spark.data.SparkOrcReader;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.JoinedRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

class RowDataReader extends BaseDataReader<InternalRow> {
  private static final Set<FileFormat> SUPPORTS_CONSTANTS = Sets.newHashSet(FileFormat.AVRO, FileFormat.PARQUET);

  RowDataReader(
      CombinedScanTask task, Schema tableSchema, Schema expectedSchema, FileIO fileIo,
      EncryptionManager encryptionManager, boolean caseSensitive) {
    super(task, fileIo, encryptionManager, tableSchema, expectedSchema, caseSensitive);
  }

  @Override
  Pair<Schema, Iterator<InternalRow>> getJoinedSchemaAndIteratorWithIdentityPartition(DataFile file, FileScanTask task,
      Schema requiredSchema, Set<Integer> idColumns, PartitionSpec spec) {
    Schema iterSchema;
    Iterator<InternalRow> iter;
    if (SUPPORTS_CONSTANTS.contains(file.format())) {
      iterSchema = requiredSchema;
      iter = open(task, requiredSchema, PartitionUtil.constantsMap(task, RowDataReader::convertConstant));
    } else {
      // schema used to read data files
      Schema readSchema = TypeUtil.selectNot(requiredSchema, idColumns);
      Schema partitionSchema = TypeUtil.select(requiredSchema, idColumns);
      PartitionRowConverter convertToRow = new PartitionRowConverter(partitionSchema, spec);
      JoinedRow joined = new JoinedRow();

      InternalRow partition = convertToRow.apply(file.partition());
      joined.withRight(partition);

      // create joined rows and project from the joined schema to the final schema
      iterSchema = TypeUtil.join(readSchema, partitionSchema);
      iter = Iterators.transform(open(task, readSchema, ImmutableMap.of()), joined::withLeft);
    }
    return Pair.of(iterSchema, iter);
  }

  @Override
  Iterator<InternalRow> open(FileScanTask task, Schema readSchema, Map<Integer, ?> idToConstant) {
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

    this.currentCloseable = iter;

    return iter.iterator();
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
