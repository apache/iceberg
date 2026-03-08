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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RecordConverter {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(RecordConverter.class);

  private static final DateTimeFormatter OFFSET_TIMESTAMP_FORMAT =
      new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          .appendOffset("+HHmm", "Z")
          .toFormatter(Locale.ROOT);

  private final Schema tableSchema;
  private final NameMapping nameMapping;
  private final IcebergSinkConfig config;
  private final Map<Integer, Map<String, NestedField>> structNameMap = Maps.newHashMap();

  RecordConverter(Table table, IcebergSinkConfig config) {
    this.tableSchema = table.schema();
    this.nameMapping = createNameMapping(table);
    this.config = config;
  }

  Record convert(SinkRecord record) {
    Object data = record.value();
    return convertStructValue(record, data, tableSchema.asStruct(), -1);
  }

  void evolveSchema(SinkRecord record, SchemaUpdate.Consumer schemaUpdateConsumer) {
    Object data = record.value();
    if (!(data instanceof Struct || data instanceof Map)) {
      throw new UnsupportedOperationException("Cannot convert type: " + data.getClass().getName());
    }

    // Schema evolution - detect all schema changes without converting data
    if (schemaUpdateConsumer != null) {
      evolveSchema(data, tableSchema.asStruct(), -1, schemaUpdateConsumer);
    }
  }

  /**
   * Schema Evolution Traverses the entire record schema to detect new fields, type changes, and
   * optionality changes. Does NOT convert any data - only collects schema updates.
   */
  private void evolveSchema(
      Object data,
      StructType tableStructType,
      int parentFieldId,
      SchemaUpdate.Consumer schemaUpdateConsumer) {
    if (data instanceof Map) {
      evolveSchemaFromMap((Map<?, ?>) data, tableStructType, parentFieldId, schemaUpdateConsumer);
    } else if (data instanceof Struct) {
      evolveSchemaFromStruct((Struct) data, tableStructType, parentFieldId, schemaUpdateConsumer);
    }
  }

  /** Evolves schema from a Struct record by traversing all fields recursively. */
  private void evolveSchemaFromStruct(
      Struct struct,
      StructType tableStructType,
      int parentFieldId,
      SchemaUpdate.Consumer schemaUpdateConsumer) {

    struct
        .schema()
        .fields()
        .forEach(
            recordField -> {
              NestedField tableField =
                  lookupStructField(recordField.name(), tableStructType, parentFieldId);
              Type recordFieldType = SchemaUtils.toIcebergType(recordField.schema(), config);

              evolveFieldSchema(
                  recordField, tableField, recordFieldType, parentFieldId, schemaUpdateConsumer);
            });
  }

  /**
   * Evolves schema for a single field - handles new fields, type updates, optionality changes, and
   * recursion into nested types. Used by both top-level and nested field processing.
   */
  private void evolveFieldSchema(
      Field recordField,
      NestedField tableField,
      Type recordFieldType,
      int parentFieldId,
      SchemaUpdate.Consumer schemaUpdateConsumer) {

    if (tableField == null) {
      // Field doesn't exist in table - add it
      String parentFieldName = parentFieldId < 0 ? null : tableSchema.findColumnName(parentFieldId);
      schemaUpdateConsumer.addColumn(parentFieldName, recordField.name(), recordFieldType);
      LOG.debug("Added new column: {} to parent: {}", recordField.name(), parentFieldName);
    } else if (recordFieldType.isNestedType()) {
      // Field exists and is nested - recurse into nested fields
      evolveNestedSchemaFromStruct(recordField, tableField, recordFieldType, schemaUpdateConsumer);
    } else {
      // Field exists and is primitive - check for type updates and optionality changes
      updateTypeIfNeeded(schemaUpdateConsumer, tableField, recordField);
      makeOptionalIfNeeded(schemaUpdateConsumer, tableField, recordField);
    }
  }

  /**
   * Recursively evolves schema for nested fields in Struct records. Handles struct,
   * list&lt;struct&gt;, and map&lt;string, struct&gt; types.
   */
  private void evolveNestedSchemaFromStruct(
      Field recordField,
      NestedField tableField,
      Type recordFieldType,
      SchemaUpdate.Consumer schemaUpdateConsumer) {

    List<Field> nestedRecordFields =
        extractNestedFieldsFromRecordSchema(recordField, recordFieldType);
    int nestedParentFieldId =
        getNestedParentFieldId(tableField, tableField.type(), recordFieldType);

    for (Field nestedRecordField : nestedRecordFields) {
      NestedField nestedTableField =
          lookupNestedFieldInTable(recordFieldType, tableField, nestedRecordField.name());
      Type nestedFieldType = SchemaUtils.toIcebergType(nestedRecordField.schema(), config);

      // Reuse the same logic for nested fields
      evolveFieldSchema(
          nestedRecordField,
          nestedTableField,
          nestedFieldType,
          nestedParentFieldId,
          schemaUpdateConsumer);
    }
  }

  /**
   * Gets the correct parent field ID for nested fields based on the container type. For
   * list&lt;struct&gt;, returns the element field ID. For map&lt;k, struct&gt;, returns the value
   * field ID. For struct, returns the struct field ID itself.
   */
  private int getNestedParentFieldId(NestedField tableField, Type tableType, Type recordFieldType) {
    if (recordFieldType.isListType() && tableType.isListType()) {
      // For list<struct>, parent is the element field (first field in list)
      return tableType.asListType().fields().get(0).fieldId();
    } else if (recordFieldType.isMapType() && tableType.isMapType()) {
      // For map<k, struct>, parent is the value field (second field in map)
      return tableType.asMapType().fields().get(1).fieldId();
    } else if (recordFieldType.isStructType() && tableType.isStructType()) {
      // For struct<struct>, parent is the struct field itself
      return tableField.fieldId();
    }
    return -1;
  }

  /** Extracts nested fields from a Kafka Connect Field schema based on the Iceberg type. */
  private List<Field> extractNestedFieldsFromRecordSchema(Field recordField, Type recordFieldType) {
    try {
      if (recordFieldType.isListType()) {
        org.apache.kafka.connect.data.Schema valueSchema = recordField.schema().valueSchema();
        return valueSchema != null && valueSchema.fields() != null
            ? valueSchema.fields()
            : java.util.Collections.emptyList();
      } else if (recordFieldType.isMapType()) {
        org.apache.kafka.connect.data.Schema valueSchema = recordField.schema().valueSchema();
        return valueSchema != null && valueSchema.fields() != null
            ? valueSchema.fields()
            : java.util.Collections.emptyList();
      } else {
        return recordField.schema().fields() != null
            ? recordField.schema().fields()
            : java.util.Collections.emptyList();
      }
    } catch (Exception e) {
      return java.util.Collections.emptyList();
    }
  }

  /** Looks up a nested field in the table schema based on the record field type. */
  private NestedField lookupNestedFieldInTable(
      Type recordFieldType, NestedField tableField, String fieldName) {
    Type tableType = tableField.type();

    if (recordFieldType.isListType() && tableType.isListType()) {
      ListType listType = tableType.asListType();
      if (listType.elementType().isStructType()) {
        return listType.elementType().asStructType().field(fieldName);
      }
    } else if (recordFieldType.isMapType() && tableType.isMapType()) {
      MapType mapType = tableType.asMapType();
      if (mapType.valueType().isStructType()) {
        return mapType.valueType().asStructType().field(fieldName);
      }
    } else if (recordFieldType.isStructType() && tableType.isStructType()) {
      return tableType.asStructType().field(fieldName);
    }
    return null;
  }

  /** Evolves schema from a Map record by traversing all entries recursively. */
  private void evolveSchemaFromMap(
      Map<?, ?> map,
      StructType tableStructType,
      int parentFieldId,
      SchemaUpdate.Consumer schemaUpdateConsumer) {

    map.forEach(
        (recordFieldNameObj, recordFieldValue) -> {
          String recordFieldName = recordFieldNameObj.toString();
          NestedField tableField =
              lookupStructField(recordFieldName, tableStructType, parentFieldId);

          if (tableField == null) {
            // Field doesn't exist in table - add it
            Type inferredType = SchemaUtils.inferIcebergType(recordFieldValue, config);
            if (inferredType != null) {
              String parentFieldName =
                  parentFieldId < 0 ? null : tableSchema.findColumnName(parentFieldId);
              schemaUpdateConsumer.addColumn(parentFieldName, recordFieldName, inferredType);
              LOG.debug(
                  "Added new column from map: {} to parent: {}", recordFieldName, parentFieldName);
            }
          } else if (recordFieldValue != null) {
            // Field exists - recursively check nested fields
            evolveNestedSchemaFromMapValue(recordFieldValue, tableField, schemaUpdateConsumer);
          }
        });
  }

  /**
   * Recursively evolves schema for nested fields discovered in Map values. Handles struct,
   * list&lt;struct&gt;, and map&lt;string, struct&gt; types.
   */
  private void evolveNestedSchemaFromMapValue(
      Object value, NestedField tableField, SchemaUpdate.Consumer schemaUpdateConsumer) {

    if (value == null) {
      return;
    }

    Type tableType = tableField.type();

    if (value instanceof Map) {
      if (tableType.isStructType()) {
        // Nested struct - process each field in the map
        evolveMapAsStruct(
            (Map<?, ?>) value, tableField, tableType.asStructType(), schemaUpdateConsumer);
      } else if (tableType.isMapType() && tableType.asMapType().valueType().isStructType()) {
        // Map with struct values - check first non-null value
        evolveFirstStructValueInMap((Map<?, ?>) value, tableType.asMapType(), schemaUpdateConsumer);
      }
    } else if (value instanceof List
        && tableType.isListType()
        && tableType.asListType().elementType().isStructType()) {
      // List of structs - check first non-null element
      evolveFirstStructElementInList((List<?>) value, tableType.asListType(), schemaUpdateConsumer);
    }
  }

  /** Evolves schema treating a Map value as a struct. */
  private void evolveMapAsStruct(
      Map<?, ?> map,
      NestedField parentField,
      StructType structType,
      SchemaUpdate.Consumer schemaUpdateConsumer) {

    map.forEach(
        (fieldName, fieldValue) -> {
          String name = fieldName.toString();
          NestedField nestedTableField = lookupStructField(name, structType, parentField.fieldId());

          if (nestedTableField == null) {
            // Field doesn't exist - add it
            Type inferredType = SchemaUtils.inferIcebergType(fieldValue, config);
            if (inferredType != null) {
              String parentFieldName = tableSchema.findColumnName(parentField.fieldId());
              schemaUpdateConsumer.addColumn(parentFieldName, name, inferredType);
              LOG.debug(
                  "Added new nested column from map: {} to parent: {}", name, parentFieldName);
            }
          } else if (fieldValue != null) {
            // Field exists - recurse to check for deeper nested fields
            evolveNestedSchemaFromMapValue(fieldValue, nestedTableField, schemaUpdateConsumer);
          }
        });
  }

  /** Evolves schema from the first struct element in a list. */
  private void evolveFirstStructElementInList(
      List<?> list, ListType listType, SchemaUpdate.Consumer schemaUpdateConsumer) {

    for (Object element : list) {
      if (element instanceof Map) {
        NestedField elementField = listType.fields().get(0);
        evolveNestedSchemaFromMapValue(element, elementField, schemaUpdateConsumer);
        return; // Only need to check schema from first element
      }
    }
  }

  /** Evolves schema from the first struct value in a map. */
  private void evolveFirstStructValueInMap(
      Map<?, ?> map, MapType mapType, SchemaUpdate.Consumer schemaUpdateConsumer) {

    for (Object entryValue : map.values()) {
      if (entryValue instanceof Map) {
        NestedField valueField = mapType.fields().get(1);
        evolveNestedSchemaFromMapValue(entryValue, valueField, schemaUpdateConsumer);
        return; // Only need to check schema from first value
      }
    }
  }

  /** Updates field type if the record schema requires a wider type. */
  private void updateTypeIfNeeded(
      SchemaUpdate.Consumer schemaUpdateConsumer, NestedField tableField, Field recordField) {
    PrimitiveType evolvedType =
        SchemaUtils.needsDataTypeUpdate(tableField.type(), recordField.schema());
    if (evolvedType != null) {
      String fieldName = tableSchema.findColumnName(tableField.fieldId());
      schemaUpdateConsumer.updateType(fieldName, evolvedType);
      LOG.debug("Updated type for field: {} to: {}", fieldName, evolvedType);
    }
  }

  /** Makes a field optional if it's required in the table but optional in the record. */
  private void makeOptionalIfNeeded(
      SchemaUpdate.Consumer schemaUpdateConsumer, NestedField tableField, Field recordField) {
    if (tableField.isRequired() && recordField.schema().isOptional()) {
      String fieldName = tableSchema.findColumnName(tableField.fieldId());
      schemaUpdateConsumer.makeOptional(fieldName);
      LOG.debug("Made field optional: {}", fieldName);
    }
  }

  private NestedField lookupStructField(String fieldName, StructType schema, int structFieldId) {
    if (nameMapping == null) {
      return config.schemaCaseInsensitive()
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
              MappedField mappedField = nameMapping.find(col.fieldId());
              if (mappedField != null && !mappedField.names().isEmpty()) {
                mappedField.names().forEach(name -> map.put(name, col));
              } else {
                map.put(col.name(), col);
              }
            });
    return map;
  }

  private NameMapping createNameMapping(Table table) {
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    return nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
  }

  /**
   * Converts a value based on its Iceberg type. This is called after schema evolution is complete.
   */
  private Object convertValue(SinkRecord sinkRecord, Object value, Type type, int fieldId) {
    if (value == null) {
      return null;
    }
    switch (type.typeId()) {
      case STRUCT:
        return convertStructValue(sinkRecord, value, type.asStructType(), fieldId);
      case LIST:
        return convertListValue(sinkRecord, value, type.asListType());
      case MAP:
        return convertMapValue(sinkRecord, value, type.asMapType());
      case INTEGER:
        return convertInt(value);
      case LONG:
        return convertLong(value);
      case FLOAT:
        return convertFloat(value);
      case DOUBLE:
        return convertDouble(value);
      case DECIMAL:
        return convertDecimal(value, (DecimalType) type);
      case BOOLEAN:
        return convertBoolean(value);
      case STRING:
        return convertString(value);
      case UUID:
        return convertUUID(value);
      case BINARY:
        return convertBase64Binary(value);
      case FIXED:
        return ByteBuffers.toByteArray(convertBase64Binary(value));
      case DATE:
        return convertDateValue(value);
      case TIME:
        return convertTimeValue(value);
      case TIMESTAMP:
        return convertTimestampValue(value, (TimestampType) type);
    }
    throw new UnsupportedOperationException("Unsupported type: " + type.typeId());
  }

  protected GenericRecord convertStructValue(
      SinkRecord record, Object value, StructType schema, int parentFieldId) {
    if (value instanceof Map) {
      return convertMapToStruct(record, (Map<?, ?>) value, schema, parentFieldId);
    } else if (value instanceof Struct) {
      return convertStructToStruct(record, (Struct) value, schema, parentFieldId);
    }
    throw new IllegalArgumentException("Cannot convert to struct: " + value.getClass().getName());
  }

  /** Converts a schemaless Map record to an Iceberg GenericRecord. */
  private GenericRecord convertMapToStruct(
      SinkRecord record, Map<?, ?> map, StructType schema, int structFieldId) {
    GenericRecord result = GenericRecord.create(schema);
    map.forEach(
        (recordFieldNameObj, recordFieldValue) -> {
          String recordFieldName = recordFieldNameObj.toString();
          NestedField tableField = lookupStructField(recordFieldName, schema, structFieldId);

          if (tableField != null) {
            result.setField(
                tableField.name(),
                convertValue(record, recordFieldValue, tableField.type(), tableField.fieldId()));
          }
          // If tableField is null, the field doesn't exist in table schema - skip it
          // (schema evolution should have been done in Phase 1)
        });
    return result;
  }

  /** Converts a Kafka Connect Struct record to an Iceberg GenericRecord. */
  private GenericRecord convertStructToStruct(
      SinkRecord sinkRecord, Struct struct, StructType schema, int parentFieldId) {
    GenericRecord result = GenericRecord.create(schema);

    struct
        .schema()
        .fields()
        .forEach(
            recordField -> {
              NestedField tableField = lookupStructField(recordField.name(), schema, parentFieldId);

              if (tableField != null) {
                result.setField(
                    tableField.name(),
                    convertValue(
                        sinkRecord,
                        struct.get(recordField),
                        tableField.type(),
                        tableField.fieldId()));
              }
              // If tableField is null, the field doesn't exist in table schema - skip it
              // (schema evolution should have been done in Phase 1)
            });

    return result;
  }

  protected List<Object> convertListValue(SinkRecord record, Object value, ListType type) {
    Preconditions.checkArgument(value instanceof List);
    List<?> list = (List<?>) value;
    return list.stream()
        .map(
            element -> {
              int fieldId = type.fields().get(0).fieldId();
              return convertValue(record, element, type.elementType(), fieldId);
            })
        .collect(Collectors.toList());
  }

  protected Map<Object, Object> convertMapValue(SinkRecord record, Object value, MapType type) {
    Preconditions.checkArgument(value instanceof Map);
    Map<?, ?> map = (Map<?, ?>) value;
    Map<Object, Object> result = Maps.newHashMap();
    map.forEach(
        (k, v) -> {
          int keyFieldId = type.fields().get(0).fieldId();
          int valueFieldId = type.fields().get(1).fieldId();
          result.put(
              convertValue(record, k, type.keyType(), keyFieldId),
              convertValue(record, v, type.valueType(), valueFieldId));
        });
    return result;
  }

  protected int convertInt(Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof String) {
      return Integer.parseInt((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to int: " + value.getClass().getName());
  }

  protected long convertLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      return Long.parseLong((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to long: " + value.getClass().getName());
  }

  protected float convertFloat(Object value) {
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      return Float.parseFloat((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to float: " + value.getClass().getName());
  }

  protected double convertDouble(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else if (value instanceof String) {
      return Double.parseDouble((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to double: " + value.getClass().getName());
  }

  protected BigDecimal convertDecimal(Object value, DecimalType type) {
    BigDecimal bigDecimal;
    if (value instanceof BigDecimal) {
      bigDecimal = (BigDecimal) value;
    } else if (value instanceof Number) {
      Number num = (Number) value;
      Double dbl = num.doubleValue();
      if (dbl.equals(Math.floor(dbl))) {
        bigDecimal = BigDecimal.valueOf(num.longValue());
      } else {
        bigDecimal = BigDecimal.valueOf(dbl);
      }
    } else if (value instanceof String) {
      bigDecimal = new BigDecimal((String) value);
    } else {
      throw new IllegalArgumentException(
          "Cannot convert to BigDecimal: " + value.getClass().getName());
    }
    return bigDecimal.setScale(type.scale(), RoundingMode.HALF_UP);
  }

  protected boolean convertBoolean(Object value) {
    if (value instanceof Boolean) {
      return (boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to boolean: " + value.getClass().getName());
  }

  protected String convertString(Object value) {
    try {
      if (value instanceof String) {
        return (String) value;
      } else if (value instanceof Number || value instanceof Boolean) {
        return value.toString();
      } else if (value instanceof Map || value instanceof List) {
        return MAPPER.writeValueAsString(value);
      } else if (value instanceof Struct) {
        Struct struct = (Struct) value;
        byte[] data = config.jsonConverter().fromConnectData(null, struct.schema(), struct);
        return new String(data, StandardCharsets.UTF_8);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    throw new IllegalArgumentException("Cannot convert to string: " + value.getClass().getName());
  }

  protected Object convertUUID(Object value) {
    UUID uuid;
    if (value instanceof String) {
      uuid = UUID.fromString((String) value);
    } else if (value instanceof UUID) {
      uuid = (UUID) value;
    } else {
      throw new IllegalArgumentException("Cannot convert to UUID: " + value.getClass().getName());
    }

    if (FileFormat.PARQUET
        .name()
        .toLowerCase(Locale.ROOT)
        .equals(config.writeProps().get(TableProperties.DEFAULT_FILE_FORMAT))) {
      return UUIDUtil.convert(uuid);
    } else {
      return uuid;
    }
  }

  protected ByteBuffer convertBase64Binary(Object value) {
    if (value instanceof String) {
      return ByteBuffer.wrap(Base64.getDecoder().decode((String) value));
    } else if (value instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) value);
    } else if (value instanceof ByteBuffer) {
      return (ByteBuffer) value;
    }
    throw new IllegalArgumentException("Cannot convert to binary: " + value.getClass().getName());
  }

  @SuppressWarnings("JavaUtilDate")
  protected LocalDate convertDateValue(Object value) {
    if (value instanceof Number) {
      int days = ((Number) value).intValue();
      return DateTimeUtil.dateFromDays(days);
    } else if (value instanceof String) {
      return LocalDate.parse((String) value);
    } else if (value instanceof LocalDate) {
      return (LocalDate) value;
    } else if (value instanceof Date) {
      int days = (int) (((Date) value).getTime() / 1000 / 60 / 60 / 24);
      return DateTimeUtil.dateFromDays(days);
    }
    throw new ConnectException("Cannot convert date: " + value);
  }

  @SuppressWarnings("JavaUtilDate")
  protected LocalTime convertTimeValue(Object value) {
    if (value instanceof Number) {
      long millis = ((Number) value).longValue();
      return DateTimeUtil.timeFromMicros(millis * 1000);
    } else if (value instanceof String) {
      return LocalTime.parse((String) value);
    } else if (value instanceof LocalTime) {
      return (LocalTime) value;
    } else if (value instanceof Date) {
      long millis = ((Date) value).getTime();
      return DateTimeUtil.timeFromMicros(millis * 1000);
    }
    throw new ConnectException("Cannot convert time: " + value);
  }

  protected Temporal convertTimestampValue(Object value, TimestampType type) {
    if (type.shouldAdjustToUTC()) {
      return convertOffsetDateTime(value);
    }
    return convertLocalDateTime(value);
  }

  @SuppressWarnings("JavaUtilDate")
  private OffsetDateTime convertOffsetDateTime(Object value) {
    if (value instanceof Number) {
      long millis = ((Number) value).longValue();
      return DateTimeUtil.timestamptzFromMicros(millis * 1000);
    } else if (value instanceof String) {
      return parseOffsetDateTime((String) value);
    } else if (value instanceof OffsetDateTime) {
      return (OffsetDateTime) value;
    } else if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).atOffset(ZoneOffset.UTC);
    } else if (value instanceof Date) {
      return DateTimeUtil.timestamptzFromMicros(((Date) value).getTime() * 1000);
    }
    throw new ConnectException(
        "Cannot convert timestamptz: " + value + ", type: " + value.getClass());
  }

  private OffsetDateTime parseOffsetDateTime(String str) {
    String tsStr = ensureTimestampFormat(str);
    try {
      return OFFSET_TIMESTAMP_FORMAT.parse(tsStr, OffsetDateTime::from);
    } catch (DateTimeParseException e) {
      return LocalDateTime.parse(tsStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          .atOffset(ZoneOffset.UTC);
    }
  }

  @SuppressWarnings("JavaUtilDate")
  private LocalDateTime convertLocalDateTime(Object value) {
    if (value instanceof Number) {
      long millis = ((Number) value).longValue();
      return DateTimeUtil.timestampFromMicros(millis * 1000);
    } else if (value instanceof String) {
      return parseLocalDateTime((String) value);
    } else if (value instanceof LocalDateTime) {
      return (LocalDateTime) value;
    } else if (value instanceof OffsetDateTime) {
      return ((OffsetDateTime) value).toLocalDateTime();
    } else if (value instanceof Date) {
      return DateTimeUtil.timestampFromMicros(((Date) value).getTime() * 1000);
    }
    throw new ConnectException(
        "Cannot convert timestamp: " + value + ", type: " + value.getClass());
  }

  private LocalDateTime parseLocalDateTime(String str) {
    String tsStr = ensureTimestampFormat(str);
    try {
      return LocalDateTime.parse(tsStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    } catch (DateTimeParseException e) {
      return OFFSET_TIMESTAMP_FORMAT.parse(tsStr, OffsetDateTime::from).toLocalDateTime();
    }
  }

  private String ensureTimestampFormat(String str) {
    String result = str;
    if (result.charAt(10) == ' ') {
      result = result.substring(0, 10) + 'T' + result.substring(11);
    }
    if (result.length() > 22
        && (result.charAt(19) == '+' || result.charAt(19) == '-')
        && result.charAt(22) == ':') {
      result = result.substring(0, 19) + result.substring(19).replace(":", "");
    }
    return result;
  }
}
