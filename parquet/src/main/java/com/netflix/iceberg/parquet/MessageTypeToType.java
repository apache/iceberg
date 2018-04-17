/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.parquet;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import org.apache.parquet.Preconditions;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type.Repetition;
import java.util.List;
import java.util.Map;

import static com.netflix.iceberg.types.Types.NestedField.optional;
import static com.netflix.iceberg.types.Types.NestedField.required;

class MessageTypeToType extends ParquetTypeVisitor<Type> {
  private static final Joiner DOT = Joiner.on(".");

  private final Map<String, Integer> aliasToId = Maps.newHashMap();
  private final GroupType root;
  private int nextId = 1;

  public MessageTypeToType(GroupType root) {
    this.root = root;
    this.nextId = root.getFieldCount() + 1; // reserve ids for the root struct
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
          "Fields cannot have repetition REPEATED: {}", field);

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
        "Elements cannot have repetition REPEATED: {}", element);

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

    Preconditions.checkArgument(Types.StringType.get() == keyType,
        "Non-string map keys are not supported: " + key);
    Preconditions.checkArgument(
        !value.isRepetition(Repetition.REPEATED),
        "Values cannot have repetition REPEATED: {}", value);

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
    OriginalType annotation = primitive.getOriginalType();
    if (annotation != null) {
      switch (annotation) {
        case DATE:
          return Types.DateType.get();
        case TIME_MICROS:
          return Types.TimeType.get();
        case TIMESTAMP_MICROS:
          return Types.TimestampType.withZone();
        case UTF8:
          return Types.StringType.get();
        case DECIMAL:
          DecimalMetadata decimal = primitive.getDecimalMetadata();
          return Types.DecimalType.of(
              decimal.getPrecision(), decimal.getScale());
        default:
          throw new UnsupportedOperationException("Unsupported logical type: " + annotation);
      }
    }

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

  private void addAlias(int fieldId) {
    if (!fieldNames.isEmpty()) {
      String fullName = DOT.join(fieldNames.descendingIterator());
      aliasToId.put(fullName, fieldId);
    }
  }

  private void addAlias(String name, int fieldId) {
    String fullName = name;
    if (!fieldNames.isEmpty()) {
      fullName = DOT.join(DOT.join(fieldNames.descendingIterator()), name);
    }
    aliasToId.put(fullName, fieldId);
  }

  private int nextId() {
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
