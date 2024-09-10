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
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

class RecordConverter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final DateTimeFormatter OFFSET_TIMESTAMP_FORMAT =
      new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          .appendOffset("+HHmm", "Z")
          .toFormatter();

  private final Schema tableSchema;
  private final NameMapping nameMapping;
  private final IcebergSinkConfig config;
  private final Map<Integer, Map<String, NestedField>> structNameMap = Maps.newHashMap();

  RecordConverter(Table table, IcebergSinkConfig config) {
    this.tableSchema = table.schema();
    this.nameMapping = createNameMapping(table);
    this.config = config;
  }

  Record convert(Object data) {
    return convert(data, null);
  }

  Record convert(Object data, SchemaUpdate.Consumer schemaUpdateConsumer) {
    if (data instanceof Struct || data instanceof Map) {
      return convertStructValue(data, tableSchema.asStruct(), -1, schemaUpdateConsumer);
    }
    throw new UnsupportedOperationException("Cannot convert type: " + data.getClass().getName());
  }

  private NameMapping createNameMapping(Table table) {
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    return nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
  }

  private Object convertValue(
      Object value, Type type, int fieldId, SchemaUpdate.Consumer schemaUpdateConsumer) {
    if (value == null) {
      return null;
    }
    switch (type.typeId()) {
      case STRUCT:
        return convertStructValue(value, type.asStructType(), fieldId, schemaUpdateConsumer);
      case LIST:
        return convertListValue(value, type.asListType(), schemaUpdateConsumer);
      case MAP:
        return convertMapValue(value, type.asMapType(), schemaUpdateConsumer);
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
      case FIXED:
        return convertBase64Binary(value);
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
      Object value,
      StructType schema,
      int parentFieldId,
      SchemaUpdate.Consumer schemaUpdateConsumer) {
    if (value instanceof Map) {
      return convertToStruct((Map<?, ?>) value, schema, parentFieldId, schemaUpdateConsumer);
    } else if (value instanceof Struct) {
      return convertToStruct((Struct) value, schema, parentFieldId, schemaUpdateConsumer);
    }
    throw new IllegalArgumentException("Cannot convert to struct: " + value.getClass().getName());
  }

  /**
   * This method will be called for records when there is no record schema. Also, when there is no
   * schema, we infer that map values are struct types. This method might also be called if the
   * field value is a map but the Iceberg type is a struct. This can happen if the Iceberg table
   * schema is not managed by the sink, i.e. created manually.
   */
  private GenericRecord convertToStruct(
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
            // add the column if schema evolution is on, otherwise skip the value,
            // skip the add column if we can't infer the type
            if (schemaUpdateConsumer != null) {
              Type type = SchemaUtils.inferIcebergType(recordFieldValue, config);
              if (type != null) {
                String parentFieldName =
                    structFieldId < 0 ? null : tableSchema.findColumnName(structFieldId);
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

  /** This method will be called for records and struct values when there is a record schema. */
  private GenericRecord convertToStruct(
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
                // add the column if schema evolution is on, otherwise skip the value
                if (schemaUpdateConsumer != null) {
                  String parentFieldName =
                      structFieldId < 0 ? null : tableSchema.findColumnName(structFieldId);
                  Type type = SchemaUtils.toIcebergType(recordField.schema(), config);
                  schemaUpdateConsumer.addColumn(parentFieldName, recordField.name(), type);
                }
              } else {
                boolean hasSchemaUpdates = false;
                if (schemaUpdateConsumer != null) {
                  // update the type if needed and schema evolution is on
                  PrimitiveType evolveDataType =
                      SchemaUtils.needsDataTypeUpdate(tableField.type(), recordField.schema());
                  if (evolveDataType != null) {
                    String fieldName = tableSchema.findColumnName(tableField.fieldId());
                    schemaUpdateConsumer.updateType(fieldName, evolveDataType);
                    hasSchemaUpdates = true;
                  }
                  // make optional if needed and schema evolution is on
                  if (tableField.isRequired() && recordField.schema().isOptional()) {
                    String fieldName = tableSchema.findColumnName(tableField.fieldId());
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

  protected List<Object> convertListValue(
      Object value, ListType type, SchemaUpdate.Consumer schemaUpdateConsumer) {
    Preconditions.checkArgument(value instanceof List);
    List<?> list = (List<?>) value;
    return list.stream()
        .map(
            element -> {
              int fieldId = type.fields().get(0).fieldId();
              return convertValue(element, type.elementType(), fieldId, schemaUpdateConsumer);
            })
        .collect(Collectors.toList());
  }

  protected Map<Object, Object> convertMapValue(
      Object value, MapType type, SchemaUpdate.Consumer schemaUpdateConsumer) {
    Preconditions.checkArgument(value instanceof Map);
    Map<?, ?> map = (Map<?, ?>) value;
    Map<Object, Object> result = Maps.newHashMap();
    map.forEach(
        (k, v) -> {
          int keyFieldId = type.fields().get(0).fieldId();
          int valueFieldId = type.fields().get(1).fieldId();
          result.put(
              convertValue(k, type.keyType(), keyFieldId, schemaUpdateConsumer),
              convertValue(v, type.valueType(), valueFieldId, schemaUpdateConsumer));
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

  protected UUID convertUUID(Object value) {
    if (value instanceof String) {
      return UUID.fromString((String) value);
    } else if (value instanceof UUID) {
      return (UUID) value;
    }
    throw new IllegalArgumentException("Cannot convert to UUID: " + value.getClass().getName());
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
