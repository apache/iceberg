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
package org.apache.iceberg.data.parquet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.parquet.ParquetVariantVisitor;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.parquet.VariantReaderBuilder;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.JsonLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

abstract class BaseParquetReaders<T> {
  // Root ID is used for the top-level struct in the Parquet Schema
  protected static final int ROOT_ID = -1;

  protected BaseParquetReaders() {}

  protected ParquetValueReader<T> createReader(Schema expectedSchema, MessageType fileSchema) {
    return createReader(expectedSchema, fileSchema, ImmutableMap.of());
  }

  @SuppressWarnings("unchecked")
  protected ParquetValueReader<T> createReader(
      Schema expectedSchema, MessageType fileSchema, Map<Integer, ?> idToConstant) {
    if (ParquetSchemaUtil.hasIds(fileSchema)) {
      return (ParquetValueReader<T>)
          TypeWithSchemaVisitor.visit(
              expectedSchema.asStruct(), fileSchema, new ReadBuilder(fileSchema, idToConstant));
    } else {
      return (ParquetValueReader<T>)
          TypeWithSchemaVisitor.visit(
              expectedSchema.asStruct(),
              fileSchema,
              new FallbackReadBuilder(fileSchema, idToConstant));
    }
  }

  /**
   * @deprecated will be removed in 1.12.0. Subclasses should override {@link
   *     #createStructReader(List, Types.StructType, Integer)} instead
   */
  @Deprecated
  protected ParquetValueReader<T> createStructReader(
      List<ParquetValueReader<?>> fieldReaders, Types.StructType structType) {
    throw new UnsupportedOperationException(
        "Deprecated method is not used in this implementation, only createStructReader(list, Types.Struct, Integer) should be used");
  }

  /**
   * This method can be overridden to provide a custom implementation which also uses the fieldId of
   * the Schema when creating the struct reader
   */
  protected ParquetValueReader<T> createStructReader(
      List<ParquetValueReader<?>> fieldReaders, Types.StructType structType, Integer fieldId) {
    // Fallback to the signature without fieldId if not overridden
    return createStructReader(fieldReaders, structType);
  }

  protected abstract ParquetValueReader<?> fixedReader(ColumnDescriptor desc);

  protected abstract ParquetValueReader<?> dateReader(ColumnDescriptor desc);

  protected abstract ParquetValueReader<?> timeReader(ColumnDescriptor desc);

  protected abstract ParquetValueReader<?> timestampReader(
      ColumnDescriptor desc, boolean isAdjustedToUTC);

  protected Object convertConstant(org.apache.iceberg.types.Type type, Object value) {
    return value;
  }

  private class FallbackReadBuilder extends ReadBuilder {
    private FallbackReadBuilder(MessageType type, Map<Integer, ?> idToConstant) {
      super(type, idToConstant);
    }

    @Override
    public ParquetValueReader<?> message(
        Types.StructType expected, MessageType message, List<ParquetValueReader<?>> fieldReaders) {
      // The top level matches by ID, but the remaining IDs are missing
      // Mark the top-level struct with the ROOT_ID
      return super.struct(expected, message.withId(ROOT_ID), fieldReaders);
    }

    @Override
    public ParquetValueReader<?> struct(
        Types.StructType expected, GroupType struct, List<ParquetValueReader<?>> fieldReaders) {
      // the expected struct is ignored because nested fields are never found when the
      List<ParquetValueReader<?>> newFields =
          Lists.newArrayListWithExpectedSize(fieldReaders.size());
      List<Type> fields = struct.getFields();
      for (int i = 0; i < fields.size(); i += 1) {
        ParquetValueReader<?> fieldReader = fieldReaders.get(i);
        if (fieldReader != null) {
          Type fieldType = fields.get(i);
          int fieldD = type().getMaxDefinitionLevel(path(fieldType.getName())) - 1;
          newFields.add(ParquetValueReaders.option(fieldType, fieldD, fieldReader));
        }
      }

      return createStructReader(newFields, expected, fieldId(struct));
    }
  }

  /** Returns the field ID from a Parquet GroupType, returning null if the ID is not set */
  private static Integer fieldId(GroupType struct) {
    return struct.getId() != null ? struct.getId().intValue() : null;
  }

  private class LogicalTypeReadBuilder
      implements LogicalTypeAnnotationVisitor<ParquetValueReader<?>> {

    private final ColumnDescriptor desc;
    private final org.apache.iceberg.types.Type.PrimitiveType expected;

    LogicalTypeReadBuilder(
        ColumnDescriptor desc, org.apache.iceberg.types.Type.PrimitiveType expected) {
      this.desc = desc;
      this.expected = expected;
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(StringLogicalTypeAnnotation stringLogicalType) {
      return Optional.of(ParquetValueReaders.strings(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(EnumLogicalTypeAnnotation enumLogicalType) {
      return Optional.of(ParquetValueReaders.strings(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(DecimalLogicalTypeAnnotation decimalLogicalType) {
      return Optional.of(ParquetValueReaders.bigDecimals(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(DateLogicalTypeAnnotation dateLogicalType) {
      return Optional.of(dateReader(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(TimeLogicalTypeAnnotation timeLogicalType) {
      return Optional.of(timeReader(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        TimestampLogicalTypeAnnotation timestampLogicalType) {
      return Optional.of(timestampReader(desc, timestampLogicalType.isAdjustedToUTC()));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(IntLogicalTypeAnnotation intLogicalType) {
      if (intLogicalType.getBitWidth() == 64) {
        Preconditions.checkArgument(
            intLogicalType.isSigned(), "Cannot read UINT64 as a long value");

        return Optional.of(new ParquetValueReaders.UnboxedReader<>(desc));
      }

      if (expected.typeId() == TypeID.LONG) {
        return Optional.of(new ParquetValueReaders.IntAsLongReader(desc));
      }

      Preconditions.checkArgument(
          intLogicalType.isSigned() || intLogicalType.getBitWidth() < 32,
          "Cannot read UINT32 as an int value");

      return Optional.of(new ParquetValueReaders.UnboxedReader<>(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(JsonLogicalTypeAnnotation jsonLogicalType) {
      return Optional.of(ParquetValueReaders.strings(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
      return Optional.of(ParquetValueReaders.byteBuffers(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.UUIDLogicalTypeAnnotation uuidLogicalType) {
      return Optional.of(ParquetValueReaders.uuids(desc));
    }
  }

  private class ReadBuilder extends TypeWithSchemaVisitor<ParquetValueReader<?>> {
    private final MessageType type;
    private final Map<Integer, ?> idToConstant;

    private ReadBuilder(MessageType type, Map<Integer, ?> idToConstant) {
      this.type = type;
      this.idToConstant = idToConstant;
    }

    @Override
    public ParquetValueReader<?> message(
        Types.StructType expected, MessageType message, List<ParquetValueReader<?>> fieldReaders) {
      return struct(expected, message.asGroupType().withId(ROOT_ID), fieldReaders);
    }

    @Override
    public ParquetValueReader<?> struct(
        Types.StructType expected, GroupType struct, List<ParquetValueReader<?>> fieldReaders) {
      if (null == expected) {
        return createStructReader(ImmutableList.of(), null, fieldId(struct));
      }

      // match the expected struct's order
      Map<Integer, ParquetValueReader<?>> readersById = Maps.newHashMap();
      List<Type> fields = struct.getFields();
      for (int i = 0; i < fields.size(); i += 1) {
        ParquetValueReader<?> fieldReader = fieldReaders.get(i);
        if (fieldReader != null) {
          Type fieldType = fields.get(i);
          int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName())) - 1;
          int id = fieldType.getId().intValue();
          readersById.put(id, ParquetValueReaders.option(fieldType, fieldD, fieldReader));
        }
      }

      int constantDefinitionLevel = type.getMaxDefinitionLevel(currentPath());
      List<Types.NestedField> expectedFields = expected.fields();
      List<ParquetValueReader<?>> reorderedFields =
          Lists.newArrayListWithExpectedSize(expectedFields.size());

      for (Types.NestedField field : expectedFields) {
        int id = field.fieldId();
        ParquetValueReader<?> reader =
            ParquetValueReaders.replaceWithMetadataReader(
                id, readersById.get(id), idToConstant, constantDefinitionLevel);
        reorderedFields.add(defaultReader(field, reader, constantDefinitionLevel));
      }

      return createStructReader(reorderedFields, expected, fieldId(struct));
    }

    private ParquetValueReader<?> defaultReader(
        Types.NestedField field, ParquetValueReader<?> reader, int constantDL) {
      if (reader != null) {
        return reader;
      } else if (field.initialDefault() != null) {
        return ParquetValueReaders.constant(
            convertConstant(field.type(), field.initialDefault()), constantDL);
      } else if (field.isOptional()) {
        return ParquetValueReaders.nulls();
      }

      throw new IllegalArgumentException(String.format("Missing required field: %s", field.name()));
    }

    @Override
    public ParquetValueReader<?> list(
        Types.ListType expectedList, GroupType array, ParquetValueReader<?> elementReader) {
      if (expectedList == null) {
        return null;
      }

      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath) - 1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath) - 1;

      Type elementType = ParquetSchemaUtil.determineListElementType(array);
      int elementD = type.getMaxDefinitionLevel(path(elementType.getName())) - 1;

      return new ParquetValueReaders.ListReader<>(
          repeatedD, repeatedR, ParquetValueReaders.option(elementType, elementD, elementReader));
    }

    @Override
    public ParquetValueReader<?> map(
        Types.MapType expectedMap,
        GroupType map,
        ParquetValueReader<?> keyReader,
        ParquetValueReader<?> valueReader) {
      if (expectedMap == null) {
        return null;
      }

      GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath) - 1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath) - 1;

      Type keyType = repeatedKeyValue.getType(0);
      int keyD = type.getMaxDefinitionLevel(path(keyType.getName())) - 1;
      Type valueType = repeatedKeyValue.getType(1);
      int valueD = type.getMaxDefinitionLevel(path(valueType.getName())) - 1;

      return new ParquetValueReaders.MapReader<>(
          repeatedD,
          repeatedR,
          ParquetValueReaders.option(keyType, keyD, keyReader),
          ParquetValueReaders.option(valueType, valueD, valueReader));
    }

    @Override
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public ParquetValueReader<?> primitive(
        org.apache.iceberg.types.Type.PrimitiveType expected, PrimitiveType primitive) {
      if (expected == null) {
        return null;
      }

      ColumnDescriptor desc = type.getColumnDescription(currentPath());

      if (primitive.getLogicalTypeAnnotation() != null) {
        return primitive
            .getLogicalTypeAnnotation()
            .accept(new LogicalTypeReadBuilder(desc, expected))
            .orElseThrow(
                () ->
                    new UnsupportedOperationException(
                        "Unsupported logical type: " + primitive.getLogicalTypeAnnotation()));
      }

      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
          return fixedReader(desc);
        case BINARY:
          if (expected.typeId() == TypeID.STRING) {
            return ParquetValueReaders.strings(desc);
          } else {
            return ParquetValueReaders.byteBuffers(desc);
          }
        case INT32:
          if (expected.typeId() == TypeID.LONG) {
            return ParquetValueReaders.intsAsLongs(desc);
          } else {
            return ParquetValueReaders.unboxed(desc);
          }
        case FLOAT:
          if (expected.typeId() == TypeID.DOUBLE) {
            return ParquetValueReaders.floatsAsDoubles(desc);
          } else {
            return ParquetValueReaders.unboxed(desc);
          }
        case BOOLEAN:
        case INT64:
        case DOUBLE:
          return ParquetValueReaders.unboxed(desc);
        case INT96:
          // Impala & Spark used to write timestamps as INT96 without a logical type. For backwards
          // compatibility we try to read INT96 as timestamps.
          return timestampReader(desc, true);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }

    @Override
    public ParquetValueReader<?> variant(
        Types.VariantType iVariant, GroupType variant, ParquetValueReader<?> reader) {
      return reader;
    }

    @Override
    public ParquetVariantVisitor<ParquetValueReader<?>> variantVisitor() {
      return new VariantReaderBuilder(type, Arrays.asList(currentPath()));
    }

    MessageType type() {
      return type;
    }
  }
}
