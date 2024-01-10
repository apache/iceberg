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

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

abstract class TypeToSchema extends TypeUtil.SchemaVisitor<Schema> {
  private static final Schema BOOLEAN_SCHEMA = Schema.create(Schema.Type.BOOLEAN);
  private static final Schema INTEGER_SCHEMA = Schema.create(Schema.Type.INT);
  private static final Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);
  private static final Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);
  private static final Schema DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE);
  private static final Schema DATE_SCHEMA =
      LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
  private static final Schema TIME_SCHEMA =
      LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema TIMESTAMP_SCHEMA =
      LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema TIMESTAMPTZ_SCHEMA =
      LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  private static final Schema UUID_SCHEMA =
      LogicalTypes.uuid().addToSchema(Schema.createFixed("uuid_fixed", null, null, 16));
  private static final Schema BINARY_SCHEMA = Schema.create(Schema.Type.BYTES);

  static {
    TIMESTAMP_SCHEMA.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, false);
    TIMESTAMPTZ_SCHEMA.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, true);
  }

  private final Deque<Integer> fieldIds = Lists.newLinkedList();
  private final BiFunction<Integer, Types.StructType, String> namesFunction;

  TypeToSchema(BiFunction<Integer, Types.StructType, String> namesFunction) {
    this.namesFunction = namesFunction;
  }

  @Override
  public Schema schema(org.apache.iceberg.Schema schema, Schema structSchema) {
    return structSchema;
  }

  @Override
  public void beforeField(Types.NestedField field) {
    fieldIds.push(field.fieldId());
  }

  @Override
  public void afterField(Types.NestedField field) {
    fieldIds.pop();
  }

  Schema lookupSchema(Type type) {
    return lookupSchema(type, null);
  }

  abstract Schema lookupSchema(Type type, String recordName);

  void cacheSchema(Type struct, Schema schema) {
    cacheSchema(struct, null, schema);
  }

  abstract void cacheSchema(Type struct, String recordName, Schema schema);

  @Override
  public Schema struct(Types.StructType struct, List<Schema> fieldSchemas) {
    Integer fieldId = fieldIds.peek();
    String recordName = namesFunction.apply(fieldId, struct);
    if (recordName == null) {
      recordName = "r" + fieldId;
    }

    Schema recordSchema = lookupSchema(struct, recordName);
    if (recordSchema != null) {
      return recordSchema;
    }

    List<Types.NestedField> structFields = struct.fields();
    List<Schema.Field> fields = Lists.newArrayListWithExpectedSize(fieldSchemas.size());
    for (int i = 0; i < structFields.size(); i += 1) {
      Types.NestedField structField = structFields.get(i);
      String origFieldName = structField.name();
      boolean isValidFieldName = AvroSchemaUtil.validAvroName(origFieldName);
      String fieldName = isValidFieldName ? origFieldName : AvroSchemaUtil.sanitize(origFieldName);
      Schema.Field field =
          new Schema.Field(
              fieldName,
              fieldSchemas.get(i),
              structField.doc(),
              structField.isOptional() ? JsonProperties.NULL_VALUE : null);
      if (!isValidFieldName) {
        field.addProp(AvroSchemaUtil.ICEBERG_FIELD_NAME_PROP, origFieldName);
      }
      field.addProp(AvroSchemaUtil.FIELD_ID_PROP, structField.fieldId());
      fields.add(field);
    }

    recordSchema = Schema.createRecord(recordName, null, null, false, fields);

    cacheSchema(struct, recordName, recordSchema);

    return recordSchema;
  }

  @Override
  public Schema field(Types.NestedField field, Schema fieldSchema) {
    if (field.isOptional()) {
      return AvroSchemaUtil.toOption(fieldSchema);
    } else {
      return fieldSchema;
    }
  }

  @Override
  public Schema list(Types.ListType list, Schema elementSchema) {
    Schema listSchema = lookupSchema(list);
    if (listSchema != null) {
      return listSchema;
    }

    if (list.isElementOptional()) {
      listSchema = Schema.createArray(AvroSchemaUtil.toOption(elementSchema));
    } else {
      listSchema = Schema.createArray(elementSchema);
    }

    listSchema.addProp(AvroSchemaUtil.ELEMENT_ID_PROP, list.elementId());

    cacheSchema(list, listSchema);

    return listSchema;
  }

  @Override
  public Schema map(Types.MapType map, Schema keySchema, Schema valueSchema) {
    Schema mapSchema = lookupSchema(map);
    if (mapSchema != null) {
      return mapSchema;
    }

    if (keySchema.getType() == Schema.Type.STRING) {
      // if the map has string keys, use Avro's map type
      mapSchema =
          Schema.createMap(
              map.isValueOptional() ? AvroSchemaUtil.toOption(valueSchema) : valueSchema);
      mapSchema.addProp(AvroSchemaUtil.KEY_ID_PROP, map.keyId());
      mapSchema.addProp(AvroSchemaUtil.VALUE_ID_PROP, map.valueId());

    } else {
      mapSchema =
          AvroSchemaUtil.createMap(
              map.keyId(),
              keySchema,
              map.valueId(),
              map.isValueOptional() ? AvroSchemaUtil.toOption(valueSchema) : valueSchema);
    }

    cacheSchema(map, mapSchema);

    return mapSchema;
  }

  @Override
  public Schema primitive(Type.PrimitiveType primitive) {
    Schema primitiveSchema;
    switch (primitive.typeId()) {
      case BOOLEAN:
        primitiveSchema = BOOLEAN_SCHEMA;
        break;
      case INTEGER:
        primitiveSchema = INTEGER_SCHEMA;
        break;
      case LONG:
        primitiveSchema = LONG_SCHEMA;
        break;
      case FLOAT:
        primitiveSchema = FLOAT_SCHEMA;
        break;
      case DOUBLE:
        primitiveSchema = DOUBLE_SCHEMA;
        break;
      case DATE:
        primitiveSchema = DATE_SCHEMA;
        break;
      case TIME:
        primitiveSchema = TIME_SCHEMA;
        break;
      case TIMESTAMP:
        if (((Types.TimestampType) primitive).shouldAdjustToUTC()) {
          primitiveSchema = TIMESTAMPTZ_SCHEMA;
        } else {
          primitiveSchema = TIMESTAMP_SCHEMA;
        }
        break;
      case STRING:
        primitiveSchema = STRING_SCHEMA;
        break;
      case UUID:
        primitiveSchema = UUID_SCHEMA;
        break;
      case FIXED:
        Types.FixedType fixed = (Types.FixedType) primitive;
        primitiveSchema = Schema.createFixed("fixed_" + fixed.length(), null, null, fixed.length());
        break;
      case BINARY:
        primitiveSchema = BINARY_SCHEMA;
        break;
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) primitive;
        primitiveSchema =
            LogicalTypes.decimal(decimal.precision(), decimal.scale())
                .addToSchema(
                    Schema.createFixed(
                        "decimal_" + decimal.precision() + "_" + decimal.scale(),
                        null,
                        null,
                        TypeUtil.decimalRequiredBytes(decimal.precision())));
        break;
      default:
        throw new UnsupportedOperationException("Unsupported type ID: " + primitive.typeId());
    }

    cacheSchema(primitive, primitiveSchema);

    return primitiveSchema;
  }

  static class WithTypeToName extends TypeToSchema {

    private final Map<Type, Schema> results = Maps.newHashMap();

    WithTypeToName(Map<Types.StructType, String> names) {
      super((id, struct) -> names.get(struct));
    }

    Map<Type, Schema> getConversionMap() {
      return results;
    }

    @Override
    void cacheSchema(Type type, String recordName, Schema schema) {
      results.put(type, schema);
    }

    @Override
    Schema lookupSchema(Type type, String recordName) {
      return results.get(type);
    }
  }

  static class WithNamesFunction extends TypeToSchema {
    private final Map<String, Schema> schemaCache = Maps.newHashMap();

    WithNamesFunction(BiFunction<Integer, Types.StructType, String> namesFunction) {
      super(namesFunction);
    }

    @Override
    void cacheSchema(Type type, String recordName, Schema schema) {
      if (recordName != null) {
        schemaCache.put(recordName, schema);
      }
    }

    @Override
    Schema lookupSchema(Type type, String recordName) {
      return recordName == null ? null : schemaCache.get(recordName);
    }
  }
}
