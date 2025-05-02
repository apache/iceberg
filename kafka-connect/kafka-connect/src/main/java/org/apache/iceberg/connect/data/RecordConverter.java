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

class RecordConverter extends Converter {
  private final Map<Integer, Map<String, NestedField>> structNameMap = Maps.newHashMap();

  RecordConverter(Table table, IcebergSinkConfig config) {
    super(table, config);
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
    map.forEach(
        (recordFieldNameObj, recordFieldValue) -> {
          String recordFieldName = recordFieldNameObj.toString();
          NestedField tableField = lookupStructField(recordFieldName, schema, structFieldId);
          if (tableField == null) {
            if (schemaUpdateConsumer != null) {
              Type type = SchemaUtils.inferIcebergType(recordFieldValue, config());
              if (type != null) {
                String parentFieldName =
                    structFieldId < 0 ? null : tableSchema().findColumnName(structFieldId);
                schemaUpdateConsumer.addColumn(parentFieldName, recordFieldName, type);
              }
            }
          } else {
            result.setField(
                tableField.name(),
                convertValue(
                    recordFieldValue,
                    tableField.type(),
                    tableField.fieldId(),
                    schemaUpdateConsumer));
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
    struct
        .schema()
        .fields()
        .forEach(
            recordField -> {
              NestedField tableField = lookupStructField(recordField.name(), schema, structFieldId);
              if (tableField == null) {
                if (schemaUpdateConsumer != null) {
                  String parentFieldName =
                      structFieldId < 0 ? null : tableSchema().findColumnName(structFieldId);
                  Type type = SchemaUtils.toIcebergType(recordField.schema(), config());
                  schemaUpdateConsumer.addColumn(parentFieldName, recordField.name(), type);
                }
              } else {
                boolean hasSchemaUpdates = false;
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
                }
                if (!hasSchemaUpdates) {
                  result.setField(
                      tableField.name(),
                      convertValue(
                          struct.get(recordField),
                          tableField.type(),
                          tableField.fieldId(),
                          schemaUpdateConsumer));
                }
              }
            });
    return result;
  }

  private NestedField lookupStructField(String fieldName, StructType schema, int structFieldId) {
    if (nameMapping() == null) {
      return config().schemaCaseInsensitive()
          ? schema.caseInsensitiveField(fieldName)
          : schema.field(fieldName);
    }

    return structNameMap
        .computeIfAbsent(structFieldId, notUsed -> createStructNameMap(schema))
        .get(fieldName);
  }

  private Map<String, NestedField> createStructNameMap(StructType schema) {
    Map<String, NestedField> map = Maps.newHashMap();
    schema
        .fields()
        .forEach(
            col -> {
              MappedField mappedField = nameMapping().find(col.fieldId());
              if (mappedField != null && !mappedField.names().isEmpty()) {
                mappedField.names().forEach(name -> map.put(name, col));
              } else {
                map.put(col.name(), col);
              }
            });
    return map;
  }
}
