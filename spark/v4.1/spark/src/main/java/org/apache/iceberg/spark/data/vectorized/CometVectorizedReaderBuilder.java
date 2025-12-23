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
package org.apache.iceberg.spark.data.vectorized;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

class CometVectorizedReaderBuilder extends TypeWithSchemaVisitor<VectorizedReader<?>> {

  private final MessageType parquetSchema;
  private final Schema icebergSchema;
  private final Map<Integer, ?> idToConstant;
  private final Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory;

  CometVectorizedReaderBuilder(
      Schema expectedSchema,
      MessageType parquetSchema,
      Map<Integer, ?> idToConstant,
      Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory) {
    this.parquetSchema = parquetSchema;
    this.icebergSchema = expectedSchema;
    this.idToConstant = idToConstant;
    this.readerFactory = readerFactory;
  }

  @Override
  public VectorizedReader<?> message(
      Types.StructType expected, MessageType message, List<VectorizedReader<?>> fieldReaders) {
    GroupType groupType = message.asGroupType();
    Map<Integer, VectorizedReader<?>> readersById = Maps.newHashMap();
    List<Type> fields = groupType.getFields();

    IntStream.range(0, fields.size())
        .filter(pos -> fields.get(pos).getId() != null)
        .forEach(pos -> readersById.put(fields.get(pos).getId().intValue(), fieldReaders.get(pos)));

    List<Types.NestedField> icebergFields =
        expected != null ? expected.fields() : ImmutableList.of();

    List<VectorizedReader<?>> reorderedFields =
        Lists.newArrayListWithExpectedSize(icebergFields.size());

    for (Types.NestedField field : icebergFields) {
      int id = field.fieldId();
      VectorizedReader<?> reader = readersById.get(id);
      if (idToConstant.containsKey(id)) {
        CometConstantColumnReader constantReader =
            new CometConstantColumnReader<>(idToConstant.get(id), field);
        reorderedFields.add(constantReader);
      } else if (id == MetadataColumns.ROW_POSITION.fieldId()) {
        reorderedFields.add(new CometPositionColumnReader(field));
      } else if (id == MetadataColumns.IS_DELETED.fieldId()) {
        CometColumnReader deleteReader = new CometDeleteColumnReader<>(field);
        reorderedFields.add(deleteReader);
      } else if (reader != null) {
        reorderedFields.add(reader);
      } else if (field.initialDefault() != null) {
        CometColumnReader constantReader =
            new CometConstantColumnReader<>(field.initialDefault(), field);
        reorderedFields.add(constantReader);
      } else if (field.isOptional()) {
        CometColumnReader constantReader = new CometConstantColumnReader<>(null, field);
        reorderedFields.add(constantReader);
      } else {
        throw new IllegalArgumentException(
            String.format("Missing required field: %s", field.name()));
      }
    }
    return vectorizedReader(reorderedFields);
  }

  protected VectorizedReader<?> vectorizedReader(List<VectorizedReader<?>> reorderedFields) {
    return readerFactory.apply(reorderedFields);
  }

  @Override
  public VectorizedReader<?> struct(
      Types.StructType expected, GroupType groupType, List<VectorizedReader<?>> fieldReaders) {
    if (expected != null) {
      throw new UnsupportedOperationException(
          "Vectorized reads are not supported yet for struct fields");
    }
    return null;
  }

  @Override
  public VectorizedReader<?> primitive(
      org.apache.iceberg.types.Type.PrimitiveType expected, PrimitiveType primitive) {

    if (primitive.getId() == null) {
      return null;
    }
    int parquetFieldId = primitive.getId().intValue();
    ColumnDescriptor desc = parquetSchema.getColumnDescription(currentPath());
    // Nested types not yet supported for vectorized reads
    if (desc.getMaxRepetitionLevel() > 0) {
      return null;
    }
    Types.NestedField icebergField = icebergSchema.findField(parquetFieldId);
    if (icebergField == null) {
      return null;
    }

    return new CometColumnReader(SparkSchemaUtil.convert(icebergField.type()), desc);
  }
}
