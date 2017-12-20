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

package com.netflix.iceberg.avro;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import java.util.List;

import static com.netflix.iceberg.avro.AvroSchemaUtil.ADJUST_TO_UTC_PROP;
import static com.netflix.iceberg.avro.AvroSchemaUtil.isOptionSchema;

class SchemaToType extends AvroSchemaVisitor<Type> {
  private final Schema root;

  SchemaToType(Schema root) {
    this.root = root;
    if (root.getType() == Schema.Type.RECORD) {
      this.nextId = root.getFields().size();
    }
  }

  private int nextId = 0;

  private int getElementId(Schema schema) {
    if (schema.getObjectProp(AvroSchemaUtil.ELEMENT_ID_PROP) != null) {
      return AvroSchemaUtil.getElementId(schema);
    } else {
      return allocateId();
    }
  }

  private int getKeyId(Schema schema) {
    if (schema.getObjectProp(AvroSchemaUtil.KEY_ID_PROP) != null) {
      return AvroSchemaUtil.getKeyId(schema);
    } else {
      return allocateId();
    }
  }

  private int getValueId(Schema schema) {
    if (schema.getObjectProp(AvroSchemaUtil.VALUE_ID_PROP) != null) {
      return AvroSchemaUtil.getValueId(schema);
    } else {
      return allocateId();
    }
  }

  private int getId(Schema.Field field) {
    if (field.getObjectProp(AvroSchemaUtil.FIELD_ID_PROP) != null) {
      return AvroSchemaUtil.getId(field);
    } else {
      return allocateId();
    }
  }

  private int allocateId() {
    int current = nextId;
    nextId += 1;
    return current;
  }

  @Override
  public Type record(Schema record, List<String> names, List<Type> fieldTypes) {
    List<Schema.Field> fields = record.getFields();
    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(fields.size());

    if (root == record) {
      this.nextId = 0;
    }

    for (int i = 0; i < fields.size(); i += 1) {
      Schema.Field field = fields.get(i);
      Type fieldType = fieldTypes.get(i);
      int fieldId = getId(field);

      if (isOptionSchema(field.schema())) {
        newFields.add(Types.NestedField.optional(fieldId, field.name(), fieldType));
      } else {
        newFields.add(Types.NestedField.required(fieldId, field.name(), fieldType));
      }
    }

    return Types.StructType.of(newFields);
  }

  @Override
  public Type union(Schema union, List<Type> options) {
    Preconditions.checkArgument(isOptionSchema(union),
        "Unsupported type: non-option union: {}", union);
    // records, arrays, and maps will check nullability later
    if (options.get(0) == null) {
      return options.get(1);
    } else {
      return options.get(0);
    }
  }

  @Override
  public Type array(Schema array, Type elementType) {
    Schema elementSchema = array.getElementType();
    int id = getElementId(array);
    if (isOptionSchema(elementSchema)) {
      return Types.ListType.ofOptional(id, elementType);
    } else {
      return Types.ListType.ofRequired(id, elementType);
    }
  }

  @Override
  public Type map(Schema map, Type valueType) {
    Schema valueSchema = map.getValueType();
    int keyId = getKeyId(map);
    int valueId = getValueId(map);

    if (isOptionSchema(valueSchema)) {
      return Types.MapType.ofOptional(keyId, valueId, valueType);
    } else {
      return Types.MapType.ofRequired(keyId, valueId, valueType);
    }
  }

  @Override
  public Type primitive(Schema primitive) {
    // first check supported logical types
    LogicalType logical = primitive.getLogicalType();
    if (logical != null) {
      String name = logical.getName();
      if (logical instanceof LogicalTypes.Decimal) {
        return Types.DecimalType.of(
            ((LogicalTypes.Decimal) logical).getPrecision(),
            ((LogicalTypes.Decimal) logical).getScale());

      } else if (logical instanceof LogicalTypes.Date) {
        return Types.DateType.get();

      } else if (
          logical instanceof LogicalTypes.TimeMillis ||
          logical instanceof LogicalTypes.TimeMicros) {
        Object adjustToUTC = primitive.getObjectProp(ADJUST_TO_UTC_PROP);
        Preconditions.checkArgument(adjustToUTC instanceof Boolean,
            "Invalid value for adjust-to-utc: %s", adjustToUTC);
        if ((Boolean) adjustToUTC) {
          return Types.TimeType.withZone();
        } else {
          return Types.TimeType.withoutZone();
        }

      } else if (
          logical instanceof LogicalTypes.TimestampMillis ||
          logical instanceof LogicalTypes.TimestampMicros) {
        Object adjustToUTC = primitive.getObjectProp(ADJUST_TO_UTC_PROP);
        Preconditions.checkArgument(adjustToUTC instanceof Boolean,
            "Invalid value for adjust-to-utc: %s", adjustToUTC);
        if ((Boolean) adjustToUTC) {
          return Types.TimestampType.withZone();
        } else {
          return Types.TimestampType.withoutZone();
        }

      } else if (LogicalTypes.uuid().getName().equals(name)) {
        return Types.UUIDType.get();
      }
    }

    switch (primitive.getType()) {
      case BOOLEAN:
        return Types.BooleanType.get();
      case INT:
        return Types.IntegerType.get();
      case LONG:
        return Types.LongType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case STRING:
      case ENUM:
        return Types.StringType.get();
      case FIXED:
        return Types.FixedType.ofLength(primitive.getFixedSize());
      case BYTES:
        return Types.BinaryType.get();
      case NULL:
        return null;
    }

    throw new UnsupportedOperationException(
        "Unsupported primitive type: " + primitive);
  }
}
