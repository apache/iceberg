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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import static org.apache.avro.JsonProperties.NULL_VALUE;
import static org.apache.iceberg.avro.AvroSchemaUtil.toOption;

class TypeToSchema extends TypeUtil.SchemaVisitor<Schema> {
  private static final Schema BOOLEAN_SCHEMA = Schema.create(Schema.Type.BOOLEAN);
  private static final Schema INTEGER_SCHEMA = Schema.create(Schema.Type.INT);
  private static final Schema LONG_SCHEMA = Schema.create(Schema.Type.LONG);
  private static final Schema FLOAT_SCHEMA = Schema.create(Schema.Type.FLOAT);
  private static final Schema DOUBLE_SCHEMA = Schema.create(Schema.Type.DOUBLE);
  private static final Schema DATE_SCHEMA = LogicalTypes.date()
      .addToSchema(Schema.create(Schema.Type.INT));
  private static final Schema TIME_SCHEMA = LogicalTypes.timeMicros()
      .addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema TIMESTAMP_SCHEMA = LogicalTypes.timestampMicros()
      .addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema TIMESTAMPTZ_SCHEMA = LogicalTypes.timestampMicros()
      .addToSchema(Schema.create(Schema.Type.LONG));
  private static final Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);
  private static final Schema UUID_SCHEMA = LogicalTypes.uuid()
      .addToSchema(Schema.createFixed("uuid_fixed", null, null, 16));
  private static final Schema BINARY_SCHEMA = Schema.create(Schema.Type.BYTES);

  static {
    TIMESTAMP_SCHEMA.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, false);
    TIMESTAMPTZ_SCHEMA.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, true);
  }

  private final Map<Type, Schema> results = Maps.newHashMap();
  private final Map<Types.StructType, String> names;

  TypeToSchema(Map<Types.StructType, String> names) {
    this.names = names;
  }

  Map<Type, Schema> getConversionMap() {
    return results;
  }

  @Override
  public Schema schema(org.apache.iceberg.Schema schema, Schema structSchema) {
    return structSchema;
  }

  @Override
  public Schema struct(Types.StructType struct, List<Schema> fieldSchemas) {
    Schema recordSchema = results.get(struct);
    if (recordSchema != null) {
      return recordSchema;
    }

    String recordName = names.get(struct);
    if (recordName == null) {
      recordName = "r" + fieldIds().peek();
    }

    List<Types.NestedField> structFields = struct.fields();
    List<Schema.Field> fields = Lists.newArrayListWithExpectedSize(fieldSchemas.size());
    for (int i = 0; i < structFields.size(); i += 1) {
      Types.NestedField structField = structFields.get(i);
      Schema.Field field = new Schema.Field(
          structField.name(), fieldSchemas.get(i), null,
          structField.isOptional() ? NULL_VALUE : null);
      field.addProp(AvroSchemaUtil.FIELD_ID_PROP, structField.fieldId());
      fields.add(field);
    }

    recordSchema = Schema.createRecord(recordName, null, null, false, fields);

    results.put(struct, recordSchema);

    return recordSchema;
  }

  @Override
  public Schema field(Types.NestedField field, Schema fieldSchema) {
    if (field.isOptional()) {
      return toOption(fieldSchema);
    } else {
      return fieldSchema;
    }
  }

  @Override
  public Schema list(Types.ListType list, Schema elementSchema) {
    Schema listSchema = results.get(list);
    if (listSchema != null) {
      return listSchema;
    }

    if (list.isElementOptional()) {
      listSchema = Schema.createArray(toOption(elementSchema));
    } else {
      listSchema = Schema.createArray(elementSchema);
    }

    listSchema.addProp(AvroSchemaUtil.ELEMENT_ID_PROP, list.elementId());

    results.put(list, listSchema);

    return listSchema;
  }

  @Override
  public Schema map(Types.MapType map, Schema keySchema, Schema valueSchema) {
    Schema mapSchema = results.get(map);
    if (mapSchema != null) {
      return mapSchema;
    }

    if (keySchema.getType() == Schema.Type.STRING) {
      // if the map has string keys, use Avro's map type
      mapSchema = Schema.createMap(
          map.isValueOptional() ? toOption(valueSchema) : valueSchema);
      mapSchema.addProp(AvroSchemaUtil.KEY_ID_PROP, map.keyId());
      mapSchema.addProp(AvroSchemaUtil.VALUE_ID_PROP, map.valueId());

    } else {
      mapSchema = AvroSchemaUtil.createMap(map.keyId(), keySchema,
          map.valueId(), map.isValueOptional() ? toOption(valueSchema) : valueSchema);
    }

    results.put(map, mapSchema);

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
        primitiveSchema = LogicalTypes.decimal(decimal.precision(), decimal.scale())
            .addToSchema(Schema.createFixed(
                "decimal_" + decimal.precision() + "_" + decimal.scale(),
                null, null, TypeUtil.decimalRequriedBytes(decimal.precision())));
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported type ID: " + primitive.typeId());
    }

    results.put(primitive, primitiveSchema);

    return primitiveSchema;
  }
}
