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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type.Repetition;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

class MessageTypeToType extends ParquetTypeVisitor<Type> {
  private static final Joiner DOT = Joiner.on(".");

  private final Map<String, Integer> aliasToId = Maps.newHashMap();
  private final GroupType root;
  private int nextId = 1;

  MessageTypeToType(GroupType root) {
    this.root = root;
    this.nextId = 1_000; // use ids that won't match other than for root
  }

  public Map<String, Integer> getAliases() {
    return aliasToId;
  }

  @Override
  public Type message(MessageType message, List<Type> fields) {
    return struct(message, fields);
  }

  @Override
  public Type struct(GroupType struct, List<Type> fieldTypes) {
    if (struct == root) {
      nextId = 1; // use the reserved IDs for the root struct
    }

    List<org.apache.parquet.schema.Type> parquetFields = struct.getFields();
    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(fieldTypes.size());
    for (int i = 0; i < parquetFields.size(); i += 1) {
      org.apache.parquet.schema.Type field = parquetFields.get(i);

      Preconditions.checkArgument(
          !field.isRepetition(Repetition.REPEATED),
          "Fields cannot have repetition REPEATED: %s", field);

      int fieldId = getId(field);

      addAlias(field.getName(), fieldId);

      if (parquetFields.get(i).isRepetition(Repetition.OPTIONAL)) {
        fields.add(optional(fieldId, field.getName(), fieldTypes.get(i)));
      } else {
        fields.add(required(fieldId, field.getName(), fieldTypes.get(i)));
      }
    }

    return Types.StructType.of(fields);
  }

  @Override
  public Type list(GroupType array, Type elementType) {
    GroupType repeated = array.getType(0).asGroupType();
    org.apache.parquet.schema.Type element = repeated.getType(0);

    Preconditions.checkArgument(
        !element.isRepetition(Repetition.REPEATED),
        "Elements cannot have repetition REPEATED: %s", element);

    int elementFieldId = getId(element);

    addAlias(element.getName(), elementFieldId);

    if (element.isRepetition(Repetition.OPTIONAL)) {
      return Types.ListType.ofOptional(elementFieldId, elementType);
    } else {
      return Types.ListType.ofRequired(elementFieldId, elementType);
    }
  }

  @Override
  public Type map(GroupType map, Type keyType, Type valueType) {
    GroupType keyValue = map.getType(0).asGroupType();
    org.apache.parquet.schema.Type key = keyValue.getType(0);
    org.apache.parquet.schema.Type value = keyValue.getType(1);

    Preconditions.checkArgument(
        !value.isRepetition(Repetition.REPEATED),
        "Values cannot have repetition REPEATED: %s", value);

    int keyFieldId = getId(key);
    int valueFieldId = getId(value);

    addAlias(key.getName(), keyFieldId);
    addAlias(value.getName(), valueFieldId);

    if (value.isRepetition(Repetition.OPTIONAL)) {
      return Types.MapType.ofOptional(keyFieldId, valueFieldId, keyType, valueType);
    } else {
      return Types.MapType.ofRequired(keyFieldId, valueFieldId, keyType, valueType);
    }
  }

  @Override
  public Type primitive(PrimitiveType primitive) {
    // first, use the logical type annotation, if present
    LogicalTypeAnnotation logicalType = primitive.getLogicalTypeAnnotation();
    if (logicalType != null) {
      Optional<Type> converted = logicalType.accept(ParquetLogicalTypeVisitor.get());
      if (converted.isPresent()) {
        return converted.get();
      }
    }

    // last, use the primitive type
    switch (primitive.getPrimitiveTypeName()) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case INT32:
        return Types.IntegerType.get();
      case INT64:
        return Types.LongType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case FIXED_LEN_BYTE_ARRAY:
        return Types.FixedType.ofLength(primitive.getTypeLength());
      case BINARY:
        return Types.BinaryType.get();
    }

    throw new UnsupportedOperationException(
        "Cannot convert unknown primitive type: " + primitive);
  }

  private static class ParquetLogicalTypeVisitor implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Type> {
    private static final ParquetLogicalTypeVisitor INSTANCE = new ParquetLogicalTypeVisitor();

    private static ParquetLogicalTypeVisitor get() {
      return INSTANCE;
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringType) {
      return Optional.of(Types.StringType.get());
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumType) {
      return Optional.of(Types.StringType.get());
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType) {
      return Optional.of(Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale()));
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateType) {
      return Optional.of(Types.DateType.get());
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeType) {
      return Optional.of(Types.TimeType.get());
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampType) {
      return Optional.of(timestampType.isAdjustedToUTC() ? TimestampType.withZone() : TimestampType.withoutZone());
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intType) {
      Preconditions.checkArgument(intType.isSigned() || intType.getBitWidth() < 64,
          "Cannot use uint64: not a supported Java type");
      if (intType.getBitWidth() < 32) {
        return Optional.of(Types.IntegerType.get());
      } else if (intType.getBitWidth() == 32 && intType.isSigned()) {
        return Optional.of(Types.IntegerType.get());
      } else {
        return Optional.of(Types.LongType.get());
      }
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonType) {
      return Optional.of(Types.StringType.get());
    }

    @Override
    public Optional<Type> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonType) {
      return Optional.of(Types.BinaryType.get());
    }
  }

  private void addAlias(String name, int fieldId) {
    aliasToId.put(DOT.join(path(name)), fieldId);
  }

  protected int nextId() {
    int current = nextId;
    nextId += 1;
    return current;
  }

  private int getId(org.apache.parquet.schema.Type type) {
    org.apache.parquet.schema.Type.ID id = type.getId();
    if (id != null) {
      return id.intValue();
    } else {
      return nextId();
    }
  }
}
