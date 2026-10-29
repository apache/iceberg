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
package org.apache.iceberg.flink.maintenance.operator;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Reads the serialized equality field values of the rows of a file, which is what a primary key
 * index is built from and resolved against. Only the equality fields, and for data files the row
 * position, are read.
 *
 * <p>Not thread-safe: the keys are serialized through a shared buffer.
 */
@Internal
public class EqualityKeyReader {

  private final FileIO io;
  private final Schema keySchema;
  private final Schema keyWithPositionSchema;
  private final StructLikeSerializer serializer = new StructLikeSerializer();

  public EqualityKeyReader(FileIO io, Schema tableSchema, Set<Integer> equalityFieldIds) {
    Preconditions.checkArgument(
        equalityFieldIds != null && !equalityFieldIds.isEmpty(),
        "Equality field IDs must not be null or empty");
    Set<Integer> fieldIds = ImmutableSet.copyOf(equalityFieldIds);
    this.io = io;
    // Fail fast on a missing id rather than silently narrowing the key.
    this.keySchema = TypeUtil.select(tableSchema, fieldIds);
    Preconditions.checkArgument(
        TypeUtil.getProjectedIds(keySchema).containsAll(fieldIds),
        "Equality field IDs %s not present in table schema",
        fieldIds);
    this.keyWithPositionSchema = appendRowPosition(keySchema);
  }

  /** Receives the key and position of one row. */
  @FunctionalInterface
  public interface RowConsumer {
    void accept(SerializedEqualityValues key, long position);
  }

  /**
   * Reports the key and position of every row of a data file that is not deleted.
   *
   * @param file the data file
   * @param deleted positions already deleted from the file, or null when there are none
   * @param out receives one call per live row
   * @return the number of rows reported
   */
  public long readLiveRows(ContentFile<?> file, PositionDeleteIndex deleted, RowConsumer out)
      throws IOException {
    long rows = 0;
    try (CloseableIterable<Record> records = open(file, keyWithPositionSchema)) {
      Accessor<StructLike> positionAccessor =
          keyWithPositionSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
      for (Record record : records) {
        long position = (long) positionAccessor.get(record);
        if (deleted == null || !deleted.isDeleted(position)) {
          out.accept(serializer.serializeKey(record, keySchema.asStruct()), position);
          rows++;
        }
      }
    }

    return rows;
  }

  /** Reports the key of every row of an equality delete file. */
  public void readKeys(ContentFile<?> file, Consumer<SerializedEqualityValues> out)
      throws IOException {
    try (CloseableIterable<Record> records = open(file, keySchema)) {
      for (Record record : records) {
        out.accept(serializer.serializeKey(record, keySchema.asStruct()));
      }
    }
  }

  /** Serializes partition values in the same format the keys are serialized in. */
  public byte[] encodePartition(StructLike partition, Types.StructType partitionType) {
    return serializer.encodePartition(partition, partitionType);
  }

  private CloseableIterable<Record> open(ContentFile<?> file, Schema projection) {
    InputFile input = io.newInputFile(file.location());
    ReadBuilder<Record, Schema> builder =
        FormatModelRegistry.readBuilder(file.format(), Record.class, input);
    return builder.project(projection).reuseContainers().build();
  }

  private static Schema appendRowPosition(Schema schema) {
    List<Types.NestedField> columns = Lists.newArrayList(schema.columns());
    columns.add(MetadataColumns.ROW_POSITION);
    return new Schema(columns);
  }
}
