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

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.arrow.ArrowAllocation;
import org.apache.iceberg.arrow.vectorized.VectorizedArrowReader.ConstantVectorReader;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class VectorizedReaderBuilder extends TypeWithSchemaVisitor<VectorizedReader<?>> {
  private final MessageType parquetSchema;
  private final Schema icebergSchema;
  private final BufferAllocator rootAllocator;
  private final Map<Integer, ?> idToConstant;
  private final boolean setArrowValidityVector;
  private final Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory;
  private final BiFunction<org.apache.iceberg.types.Type, Object, Object> convert;

  public VectorizedReaderBuilder(
      Schema expectedSchema,
      MessageType parquetSchema,
      boolean setArrowValidityVector,
      Map<Integer, ?> idToConstant,
      Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory) {
    this(
        expectedSchema,
        parquetSchema,
        setArrowValidityVector,
        idToConstant,
        readerFactory,
        (type, value) -> value);
  }

  protected VectorizedReaderBuilder(
      Schema expectedSchema,
      MessageType parquetSchema,
      boolean setArrowValidityVector,
      Map<Integer, ?> idToConstant,
      Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory,
      BiFunction<org.apache.iceberg.types.Type, Object, Object> convert) {
    this(
        expectedSchema,
        parquetSchema,
        setArrowValidityVector,
        idToConstant,
        readerFactory,
        convert,
        ArrowAllocation.rootAllocator());
  }

  protected VectorizedReaderBuilder(
      Schema expectedSchema,
      MessageType parquetSchema,
      boolean setArrowValidityVector,
      Map<Integer, ?> idToConstant,
      Function<List<VectorizedReader<?>>, VectorizedReader<?>> readerFactory,
      BiFunction<org.apache.iceberg.types.Type, Object, Object> convert,
      BufferAllocator bufferAllocator) {
    this.parquetSchema = parquetSchema;
    this.icebergSchema = expectedSchema;
    this.rootAllocator =
        bufferAllocator.newChildAllocator("VectorizedReadBuilder", 0, Long.MAX_VALUE);
    this.setArrowValidityVector = setArrowValidityVector;
    this.idToConstant = idToConstant;
    this.readerFactory = readerFactory;
    this.convert = convert;
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
      VectorizedReader<?> reader =
          VectorizedArrowReader.replaceWithMetadataReader(
              field, readersById.get(field.fieldId()), idToConstant, setArrowValidityVector);
      reorderedFields.add(defaultReader(field, reader));
    }
    return vectorizedReader(reorderedFields);
  }

  private VectorizedReader<?> defaultReader(Types.NestedField field, VectorizedReader<?> reader) {
    if (reader != null) {
      return reader;
    } else if (field.initialDefault() != null) {
      return constantReader(field, convert.apply(field.type(), field.initialDefault()));
    } else if (field.isOptional()) {
      return VectorizedArrowReader.nulls();
    }

    throw new IllegalArgumentException(String.format("Missing required field: %s", field.name()));
  }

  private <T> ConstantVectorReader<T> constantReader(Types.NestedField field, T constant) {
    return new ConstantVectorReader<>(field, constant);
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

    // Create arrow vector for this field
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
    // Set the validity buffer if null checking is enabled in arrow
    return new VectorizedArrowReader(desc, icebergField, rootAllocator, setArrowValidityVector);
  }
}
