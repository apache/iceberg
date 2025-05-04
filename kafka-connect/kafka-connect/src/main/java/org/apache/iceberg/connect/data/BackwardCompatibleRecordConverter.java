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
package org.apache.iceberg.connect.data;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.kafka.connect.data.Struct;

class BackwardCompatibleRecordConverter extends Converter {
  private final Map<Integer, Map<String, NestedField>> structFieldMaps = Maps.newHashMap();
  private final Map<String, NestedField> rootFieldMap;

  BackwardCompatibleRecordConverter(Table table, IcebergSinkConfig config) {
    super(table, config);
    this.rootFieldMap = createFieldMap(tableSchema().asStruct());
  }

  @Override
  public Record convert(Object data) {
    return convert(data, null);
  }

  @Override
  public Record convert(Object data, SchemaUpdate.Consumer schemaUpdateConsumer) {
    if (data instanceof Struct || data instanceof Map) {
      return convertStructValue(data, tableSchema().asStruct(), -1, schemaUpdateConsumer);
    }
    throw new UnsupportedOperationException("Cannot convert type: " + data.getClass().getName());
  }

  @Override
  protected GenericRecord convertToStruct(
      Map<?, ?> map,
      StructType schema,
      int structFieldId,
      SchemaUpdate.Consumer schemaUpdateConsumer) {
    GenericRecord result = GenericRecord.create(schema);
    Map<String, NestedField> fieldMap = getOrCreateFieldMap(schema, structFieldId);

    map.forEach(
        (key, value) -> {
          String fieldName = key.toString();
          NestedField tableField = fieldMap.get(fieldName);

          if (tableField == null) {
            if (schemaUpdateConsumer != null) {
              Type type = SchemaUtils.inferIcebergType(value, config());
              if (type != null) {
                String parentFieldName =
                    structFieldId < 0 ? null : tableSchema().findColumnName(structFieldId);
                schemaUpdateConsumer.addColumn(parentFieldName, fieldName, type);
              }
            }
          } else {
            result.setField(
                tableField.name(),
                convertValue(value, tableField.type(), tableField.fieldId(), schemaUpdateConsumer));
          }
        });

    return result;
  }

  @Override
  protected GenericRecord convertToStruct(
      Struct struct,
      StructType schema,
      int structFieldId,
      SchemaUpdate.Consumer schemaUpdateConsumer) {
    GenericRecord result = GenericRecord.create(schema);
    Map<String, NestedField> fieldMap = getOrCreateFieldMap(schema, structFieldId);
    org.apache.kafka.connect.data.Schema structSchema = struct.schema();

    for (org.apache.kafka.connect.data.Field recordField : structSchema.fields()) {
      String fieldName = recordField.name();
      NestedField tableField = fieldMap.get(fieldName);

      if (tableField == null) {
        if (schemaUpdateConsumer != null) {
          String parentFieldName =
              structFieldId < 0 ? null : tableSchema().findColumnName(structFieldId);
          Type type = SchemaUtils.toIcebergType(recordField.schema(), config());
          schemaUpdateConsumer.addColumn(parentFieldName, fieldName, type);
        }
      } else {
        handleFieldChanges(tableField, recordField, struct, result, schemaUpdateConsumer);
      }
    }

    if (schemaUpdateConsumer != null) {
      checkMissingFields(fieldMap, structSchema, schemaUpdateConsumer);
    }

    return result;
  }

  private Map<String, NestedField> getOrCreateFieldMap(StructType schema, int structFieldId) {
    if (structFieldId < 0) {
      return rootFieldMap;
    }
    return structFieldMaps.computeIfAbsent(structFieldId, id -> createFieldMap(schema));
  }

  private Map<String, NestedField> createFieldMap(StructType schema) {
    Map<String, NestedField> fieldMap = Maps.newHashMapWithExpectedSize(schema.fields().size());

    for (NestedField field : schema.fields()) {
      fieldMap.put(field.name(), field);

      if (nameMapping() != null) {
        MappedField mappedField = nameMapping().find(field.fieldId());
        if (mappedField != null) {
          for (String name : mappedField.names()) {
            fieldMap.put(name, field);
          }
        }
      }
    }

    return fieldMap;
  }

  private void checkMissingFields(
      Map<String, NestedField> fieldMap,
      org.apache.kafka.connect.data.Schema structSchema,
      SchemaUpdate.Consumer schemaUpdateConsumer) {
    for (NestedField field : fieldMap.values()) {
      if (structSchema.field(field.name()) == null && field.isRequired()) {
        String fullFieldName = tableSchema().findColumnName(field.fieldId());
        schemaUpdateConsumer.deleteColumn(fullFieldName);
        if (field.type().isNestedType()) {
          handleNestedFieldRemoval(field, schemaUpdateConsumer);
        }
      }
    }
  }

  private void handleNestedFieldRemoval(
      NestedField field, SchemaUpdate.Consumer schemaUpdateConsumer) {
    if (schemaUpdateConsumer == null) {
      return;
    }

    Type fieldType = field.type();

    if (fieldType.isStructType()) {
      for (NestedField nestedField : fieldType.asStructType().fields()) {
        if (nestedField.isRequired()) {
          String fullPath = tableSchema().findColumnName(nestedField.fieldId());
          schemaUpdateConsumer.deleteColumn(fullPath);
        }
        if (nestedField.type().isNestedType()) {
          handleNestedFieldRemoval(nestedField, schemaUpdateConsumer);
        }
      }
    } else if (fieldType.isListType()) {
      NestedField elementField = fieldType.asListType().fields().get(0);
      if (elementField.type().isNestedType()) {
        handleNestedFieldRemoval(elementField, schemaUpdateConsumer);
      }
    } else if (fieldType.isMapType()) {
      List<NestedField> mapFields = fieldType.asMapType().fields();
      NestedField valueField = mapFields.get(1);
      if (valueField.type().isNestedType()) {
        handleNestedFieldRemoval(valueField, schemaUpdateConsumer);
      }
    }
  }

  private void handleFieldChanges(
      NestedField tableField,
      org.apache.kafka.connect.data.Field recordField,
      Struct struct,
      GenericRecord result,
      SchemaUpdate.Consumer schemaUpdateConsumer) {

    boolean hasSchemaUpdates = false;
    Object fieldValue = struct.get(recordField);

    if (schemaUpdateConsumer != null) {
      PrimitiveType evolveDataType =
          SchemaUtils.needsDataTypeUpdate(tableField.type(), recordField.schema());
      if (evolveDataType != null) {
        String fieldName = tableSchema().findColumnName(tableField.fieldId());
        schemaUpdateConsumer.updateType(fieldName, evolveDataType);
        hasSchemaUpdates = true;
      }

      if (tableField.isRequired() && recordField.schema().isOptional()) {
        String fieldName = tableSchema().findColumnName(tableField.fieldId());
        schemaUpdateConsumer.makeOptional(fieldName);
        hasSchemaUpdates = true;
      }

      if (tableField.type().isStructType()
          && recordField.schema().type() == org.apache.kafka.connect.data.Schema.Type.STRUCT
          && fieldValue instanceof Struct) {
        convertToStruct(
            (Struct) fieldValue,
            tableField.type().asStructType(),
            tableField.fieldId(),
            schemaUpdateConsumer);
      }
    }

    if (!hasSchemaUpdates) {
      result.setField(
          tableField.name(),
          convertValue(fieldValue, tableField.type(), tableField.fieldId(), schemaUpdateConsumer));
    }
  }
}
