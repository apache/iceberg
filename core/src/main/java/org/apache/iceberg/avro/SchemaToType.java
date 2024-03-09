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
package org.apache.iceberg.avro;

import java.util.List;
import java.util.Objects;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

class SchemaToType extends AvroSchemaVisitor<Type> {
  private final Schema root;

  SchemaToType(Schema root) {
    this.root = root;
    if (root.getType() == Schema.Type.RECORD) {
      this.nextId = root.getFields().size();
    }
  }

  private int nextId = 1;

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
      return AvroSchemaUtil.getFieldId(field);
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

    if (Objects.equals(root, record)) {
      this.nextId = 0;
    }

    for (int i = 0; i < fields.size(); i += 1) {
      Schema.Field field = fields.get(i);
      Type fieldType = fieldTypes.get(i);
      int fieldId = getId(field);

      if (AvroSchemaUtil.isOptionSchema(field.schema())) {
        newFields.add(Types.NestedField.optional(fieldId, field.name(), fieldType, field.doc()));
      } else {
        newFields.add(Types.NestedField.required(fieldId, field.name(), fieldType, field.doc()));
      }
    }

    return Types.StructType.of(newFields);
  }

  @Override
  public Type union(Schema union, List<Type> options) {
    if (AvroSchemaUtil.isOptionSchema(union)) {
      if (options.get(0) == null) {
        return options.get(1);
      } else {
        return options.get(0);
      }
    } else {
      // Create list of Iceberg schema fields
      List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(options.size());
      int tagIndex = 0;
      fields.add(Types.NestedField.required(allocateId(), "tag", Types.IntegerType.get()));
      for (Type option : options) {
        if (option != null) {
          fields.add(Types.NestedField.optional(allocateId(), "field" + tagIndex, option));
          tagIndex++;
        }
      }
      return Types.StructType.of(fields);
    }
  }

  @Override
  public Type array(Schema array, Type elementType) {
    if (array.getLogicalType() instanceof LogicalMap) {
      // map stored as an array
      Schema keyValueSchema = array.getElementType();
      Preconditions.checkArgument(
          AvroSchemaUtil.isKeyValueSchema(keyValueSchema),
          "Invalid key-value pair schema: %s",
          keyValueSchema);

      Types.StructType keyValueType = elementType.asStructType();
      Types.NestedField keyField = keyValueType.field("key");
      Types.NestedField valueField = keyValueType.field("value");

      if (keyValueType.field("value").isOptional()) {
        return Types.MapType.ofOptional(
            keyField.fieldId(), valueField.fieldId(), keyField.type(), valueField.type());
      } else {
        return Types.MapType.ofRequired(
            keyField.fieldId(), valueField.fieldId(), keyField.type(), valueField.type());
      }

    } else {
      // normal array
      Schema elementSchema = array.getElementType();
      int id = getElementId(array);
      if (AvroSchemaUtil.isOptionSchema(elementSchema)) {
        return Types.ListType.ofOptional(id, elementType);
      } else {
        return Types.ListType.ofRequired(id, elementType);
      }
    }
  }

  @Override
  public Type map(Schema map, Type valueType) {
    Schema valueSchema = map.getValueType();
    int keyId = getKeyId(map);
    int valueId = getValueId(map);

    if (AvroSchemaUtil.isOptionSchema(valueSchema)) {
      return Types.MapType.ofOptional(keyId, valueId, Types.StringType.get(), valueType);
    } else {
      return Types.MapType.ofRequired(keyId, valueId, Types.StringType.get(), valueType);
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

      } else if (logical instanceof LogicalTypes.TimeMillis
          || logical instanceof LogicalTypes.TimeMicros) {
        return Types.TimeType.get();

      } else if (logical instanceof LogicalTypes.TimestampMillis
          || logical instanceof LogicalTypes.TimestampMicros) {
        if (AvroSchemaUtil.isTimestamptz(primitive)) {
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

    throw new UnsupportedOperationException("Unsupported primitive type: " + primitive);
  }
}
