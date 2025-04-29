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
package org.apache.iceberg.parquet;

import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.VariantValue;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class VariantWriterBuilder extends ParquetVariantVisitor<ParquetValueWriter<?>> {
  private final MessageType schema;
  private final Iterable<String> basePath;
  private final Deque<String> fieldNames = Lists.newLinkedList();

  public VariantWriterBuilder(MessageType schema, Iterable<String> basePath) {
    this.schema = schema;
    this.basePath = basePath;
  }

  @Override
  public void beforeField(Type type) {
    fieldNames.addLast(type.getName());
  }

  @Override
  public void afterField(Type type) {
    fieldNames.removeLast();
  }

  private String[] currentPath() {
    return Streams.concat(Streams.stream(basePath), fieldNames.stream()).toArray(String[]::new);
  }

  private String[] path(String... names) {
    return Streams.concat(Streams.stream(basePath), fieldNames.stream(), Stream.of(names))
        .toArray(String[]::new);
  }

  @Override
  public ParquetValueWriter<?> variant(
      GroupType variant, ParquetValueWriter<?> metadataWriter, ParquetValueWriter<?> valueWriter) {
    return ParquetVariantWriters.variant(metadataWriter, valueWriter);
  }

  @Override
  public ParquetValueWriter<?> metadata(PrimitiveType metadata) {
    ColumnDescriptor desc = schema.getColumnDescription(currentPath());
    return ParquetVariantWriters.metadata(ParquetValueWriters.byteBuffers(desc));
  }

  @Override
  public ParquetValueWriter<?> serialized(PrimitiveType value) {
    ColumnDescriptor desc = schema.getColumnDescription(currentPath());
    return ParquetVariantWriters.value(ParquetValueWriters.byteBuffers(desc));
  }

  @Override
  public ParquetValueWriter<?> primitive(PrimitiveType primitive) {
    ColumnDescriptor desc = schema.getColumnDescription(currentPath());
    LogicalTypeAnnotation annotation = primitive.getLogicalTypeAnnotation();
    if (annotation != null) {
      Optional<ParquetValueWriter<?>> writer =
          annotation.accept(new LogicalTypeToVariantWriter(desc));
      if (writer.isPresent()) {
        return writer.get();
      }

    } else {
      switch (primitive.getPrimitiveTypeName()) {
        case BINARY:
          return ParquetVariantWriters.primitive(
              ParquetValueWriters.byteBuffers(desc), PhysicalType.BINARY);
        case BOOLEAN:
          return ParquetVariantWriters.primitive(
              ParquetValueWriters.booleans(desc),
              PhysicalType.BOOLEAN_TRUE,
              PhysicalType.BOOLEAN_FALSE);
        case INT32:
          return ParquetVariantWriters.primitive(
              ParquetValueWriters.ints(desc), PhysicalType.INT32);
        case INT64:
          return ParquetVariantWriters.primitive(
              ParquetValueWriters.longs(desc), PhysicalType.INT64);
        case FLOAT:
          // use an unboxed writer to skip metrics collection that requires an ID
          return ParquetVariantWriters.primitive(
              ParquetValueWriters.unboxed(desc), PhysicalType.FLOAT);
        case DOUBLE:
          // use an unboxed writer to skip metrics collection that requires an ID
          return ParquetVariantWriters.primitive(
              ParquetValueWriters.unboxed(desc), PhysicalType.DOUBLE);
      }
    }

    throw new UnsupportedOperationException("Unsupported shredded value type: " + primitive);
  }

  @Override
  public ParquetValueWriter<?> value(
      GroupType value, ParquetValueWriter<?> valueWriter, ParquetValueWriter<?> typedWriter) {
    int valueDL = schema.getMaxDefinitionLevel(path(VALUE));
    if (typedWriter != null) {
      int typedValueDL = schema.getMaxDefinitionLevel(path(TYPED_VALUE));
      return ParquetVariantWriters.shredded(valueDL, valueWriter, typedValueDL, typedWriter);
    } else if (value.getType(VALUE).isRepetition(Type.Repetition.OPTIONAL)) {
      return ParquetValueWriters.option(value.getType(VALUE), valueDL, valueWriter);
    } else {
      return valueWriter;
    }
  }

  @Override
  public ParquetValueWriter<?> object(
      GroupType object,
      ParquetValueWriter<?> valueWriter,
      List<ParquetValueWriter<?>> fieldWriters) {
    int valueDL = schema.getMaxDefinitionLevel(path(VALUE));
    int typedDL = schema.getMaxDefinitionLevel(path(TYPED_VALUE));
    GroupType firstField = object.getType(TYPED_VALUE).asGroupType().getType(0).asGroupType();
    int fieldDL =
        schema.getMaxDefinitionLevel(
            path(TYPED_VALUE, firstField.getName(), firstField.getType(0).getName()));

    List<String> names =
        object.getType(TYPED_VALUE).asGroupType().getFields().stream()
            .map(Type::getName)
            .collect(Collectors.toList());

    return ParquetVariantWriters.objects(
        valueDL, valueWriter, typedDL, fieldDL, names, fieldWriters);
  }

  @Override
  public ParquetValueWriter<?> array(
      GroupType array, ParquetValueWriter<?> valueWriter, ParquetValueWriter<?> elementWriter) {
    int valueDL = schema.getMaxDefinitionLevel(path(VALUE));
    int typedDL = schema.getMaxDefinitionLevel(path(TYPED_VALUE));
    int repeatedDL = schema.getMaxDefinitionLevel(path(TYPED_VALUE, LIST));
    int repeatedRL = schema.getMaxRepetitionLevel(path(TYPED_VALUE, LIST));

    ParquetValueWriter<VariantValue> typedWriter =
        ParquetVariantWriters.array(repeatedDL, repeatedRL, elementWriter);

    return ParquetVariantWriters.shredded(valueDL, valueWriter, typedDL, typedWriter);
  }

  private static class LogicalTypeToVariantWriter
      implements LogicalTypeAnnotationVisitor<ParquetValueWriter<?>> {
    private final ColumnDescriptor desc;

    private LogicalTypeToVariantWriter(ColumnDescriptor desc) {
      this.desc = desc;
    }

    @Override
    public Optional<ParquetValueWriter<?>> visit(StringLogicalTypeAnnotation ignored) {
      ParquetValueWriter<VariantValue> writer =
          ParquetVariantWriters.primitive(ParquetValueWriters.strings(desc), PhysicalType.STRING);
      return Optional.of(writer);
    }

    @Override
    public Optional<ParquetValueWriter<?>> visit(DecimalLogicalTypeAnnotation decimal) {
      ParquetValueWriter<VariantValue> writer;
      switch (desc.getPrimitiveType().getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
          writer =
              ParquetVariantWriters.primitive(
                  ParquetValueWriters.decimalAsFixed(
                      desc, decimal.getPrecision(), decimal.getScale()),
                  PhysicalType.DECIMAL16);
          return Optional.of(writer);
        case INT64:
          writer =
              ParquetVariantWriters.primitive(
                  ParquetValueWriters.decimalAsLong(
                      desc, decimal.getPrecision(), decimal.getScale()),
                  PhysicalType.DECIMAL8);
          return Optional.of(writer);
        case INT32:
          writer =
              ParquetVariantWriters.primitive(
                  ParquetValueWriters.decimalAsInteger(
                      desc, decimal.getPrecision(), decimal.getScale()),
                  PhysicalType.DECIMAL4);
          return Optional.of(writer);
      }

      throw new IllegalArgumentException(
          "Invalid primitive type for decimal: " + desc.getPrimitiveType());
    }

    @Override
    public Optional<ParquetValueWriter<?>> visit(DateLogicalTypeAnnotation ignored) {
      ParquetValueWriter<VariantValue> writer =
          ParquetVariantWriters.primitive(ParquetValueWriters.ints(desc), PhysicalType.DATE);
      return Optional.of(writer);
    }

    @Override
    public Optional<ParquetValueWriter<?>> visit(TimeLogicalTypeAnnotation time) {
      ParquetValueWriter<VariantValue> writer =
          ParquetVariantWriters.primitive(ParquetValueWriters.longs(desc), PhysicalType.TIME);
      return Optional.of(writer);
    }

    @Override
    public Optional<ParquetValueWriter<?>> visit(TimestampLogicalTypeAnnotation timestamp) {
      return Optional.of(
          ParquetVariantWriters.primitive(
              ParquetValueWriters.longs(desc), ParquetVariantUtil.convert(timestamp)));
    }

    @Override
    public Optional<ParquetValueWriter<?>> visit(IntLogicalTypeAnnotation logical) {
      Preconditions.checkArgument(
          logical.isSigned(), "Invalid logical type for variant, unsigned: %s", logical);
      ParquetValueWriter<?> writer;
      switch (logical.getBitWidth()) {
        case 8:
          writer =
              ParquetVariantWriters.primitive(
                  ParquetValueWriters.tinyints(desc), PhysicalType.INT8);
          return Optional.of(writer);
        case 16:
          writer =
              ParquetVariantWriters.primitive(ParquetValueWriters.shorts(desc), PhysicalType.INT16);
          return Optional.of(writer);
        case 32:
          writer =
              ParquetVariantWriters.primitive(ParquetValueWriters.ints(desc), PhysicalType.INT32);
          return Optional.of(writer);
        case 64:
          writer =
              ParquetVariantWriters.primitive(ParquetValueWriters.longs(desc), PhysicalType.INT64);
          return Optional.of(writer);
      }

      throw new IllegalArgumentException("Invalid bit width for int: " + logical.getBitWidth());
    }

    @Override
    public Optional<ParquetValueWriter<?>> visit(UUIDLogicalTypeAnnotation uuidLogicalType) {
      ParquetValueWriter<VariantValue> writer =
          ParquetVariantWriters.primitive(ParquetValueWriters.uuids(desc), PhysicalType.UUID);
      return Optional.of(writer);
    }
  }
}
