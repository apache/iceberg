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
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.ValueArray;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

class RecordConverter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

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
        return convertBase64Binary(value);
      case FIXED:
        return ByteBuffers.toByteArray(convertBase64Binary(value));
      case DATE:
        return convertDateValue(value);
      case TIME:
        return convertTimeValue(value);
      case TIMESTAMP:
        return convertTimestampValue(value, (TimestampType) type);
      case VARIANT:
        return convertVariantValue(value);
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

  protected Variant convertVariantValue(Object value) {
    if (value instanceof Variant variant) {
      return variant;
    }

    List<String> sortedFieldNames =
        collectFieldNames(value).stream().sorted().collect(Collectors.toList());
    VariantMetadata metadata = Variants.metadata(sortedFieldNames);
    return Variant.of(metadata, objectToVariantValue(value, metadata, null));
  }

  /**
   * Recursively collects field names from collections, maps, and structs. Returns an empty set for
   * null, scalar values, and empty maps, lists, or structs. Map keys must be strings; non-string
   * keys cause IllegalArgumentException.
   */
  private static Set<String> collectFieldNames(Object value) {
    if (value == null) {
      return Collections.emptySet();
    }
    if (value instanceof Collection<?> collection) {
      if (collection.isEmpty()) {
        return Collections.emptySet();
      }
      Set<String> names = Sets.newHashSet();
      collection.forEach(element -> names.addAll(collectFieldNames(element)));
      return names;
    } else if (value instanceof Map<?, ?> map) {
      if (map.isEmpty()) {
        return Collections.emptySet();
      }
      Set<String> names = Sets.newHashSet();
      map.forEach(
          (key, val) -> {
            if (key instanceof String keyStr) {
              names.add(keyStr);
              names.addAll(collectFieldNames(val));
            } else {
              throw new IllegalArgumentException(
                  "Cannot convert map to variant: keys must be non-null strings, was: "
                      + (key == null ? "null" : key.getClass().getName()));
            }
          });
      return names;
    } else if (value instanceof Struct struct) {
      List<Field> fields = struct.schema().fields();
      if (fields.isEmpty()) {
        return Collections.emptySet();
      }
      Set<String> names = Sets.newHashSet();
      fields.forEach(
          field -> {
            names.add(field.name());
            names.addAll(collectFieldNames(struct.get(field)));
          });
      return names;
    }
    return Collections.emptySet();
  }

  /**
   * Recursively converts a Java object to a VariantValue using the given shared metadata for all
   * nested maps. Handles primitives, List (array), and Map (object); map keys become field names.
   */
  private static VariantValue objectToVariantValue(
      Object value, VariantMetadata metadata, org.apache.kafka.connect.data.Schema schema) {
    if (value == null) {
      return Variants.ofNull();
    }
    VariantValue primitive = primitiveToVariantValue(value, schema);
    if (primitive != null) {
      return primitive;
    }
    if (value instanceof Collection<?> collection) {
      ValueArray array = Variants.array();
      org.apache.kafka.connect.data.Schema elementSchema =
          schema != null ? schema.valueSchema() : null;
      for (Object element : collection) {
        array.add(objectToVariantValue(element, metadata, elementSchema));
      }
      return array;
    }
    if (value instanceof Map<?, ?> map) {
      return mapToVariantValue(map, metadata, schema);
    }
    if (value instanceof Struct struct) {
      ShreddedObject object = Variants.object(metadata);
      for (Field field : struct.schema().fields()) {
        object.put(field.name(), objectToVariantValue(struct.get(field), metadata, field.schema()));
      }
      return object;
    }
    throw new IllegalArgumentException("Cannot convert to variant: " + value.getClass().getName());
  }

  /** Converts a Map to VariantValue; throw IllegalArgumentException if the key is not a string. */
  private static VariantValue mapToVariantValue(
      Map<?, ?> map, VariantMetadata metadata, org.apache.kafka.connect.data.Schema schema) {
    ShreddedObject object = Variants.object(metadata);
    org.apache.kafka.connect.data.Schema mapValueSchema =
        schema != null ? schema.valueSchema() : null;
    map.forEach(
        (key, val) -> {
          if (key instanceof String keyStr) {
            object.put(keyStr, objectToVariantValue(val, metadata, mapValueSchema));
          } else {
            throw new IllegalArgumentException(
                "Cannot convert map to variant: keys must be non-null strings, was: "
                    + (key == null ? "null" : key.getClass().getName()));
          }
        });
    return object;
  }

  /**
   * Converts a primitive or primitive-like value to VariantValue; returns null if not supported.
   * The optional schema is used to disambiguate java.util.Date which Kafka Connect uses for Date,
   * Time, and Timestamp logical types.
   */
  private static VariantValue primitiveToVariantValue(
      Object value, org.apache.kafka.connect.data.Schema schema) {
    if (value instanceof Boolean booleanValue) {
      return Variants.of(booleanValue);
    }
    VariantValue temporal = temporalObjectToVariantValue(value, schema);
    if (temporal != null) {
      return temporal;
    }
    if (value instanceof Number number) {
      return numberToVariantValue(number);
    }
    if (value instanceof String stringValue) {
      return Variants.of(stringValue);
    }
    if (value instanceof ByteBuffer byteBuffer) {
      return Variants.of(byteBuffer);
    }
    if (value instanceof byte[] byteArray) {
      return Variants.of(ByteBuffer.wrap(byteArray));
    }
    if (value instanceof UUID uuid) {
      return Variants.ofUUID(uuid);
    }
    return null;
  }

  /**
   * Converts java.time values and java.util.Date (with Connect logical type from the optional
   * schema) to VariantValue; returns null if the value is not a supported temporal representation.
   */
  private static VariantValue temporalObjectToVariantValue(
      Object value, org.apache.kafka.connect.data.Schema schema) {
    if (value instanceof Instant instant) {
      return Variants.ofTimestamptz(DateTimeUtil.microsFromInstant(instant));
    }
    if (value instanceof OffsetDateTime offsetDateTime) {
      return Variants.ofTimestamptz(DateTimeUtil.microsFromTimestamptz(offsetDateTime));
    }
    if (value instanceof ZonedDateTime zonedDateTime) {
      return Variants.ofTimestamptz(
          DateTimeUtil.microsFromTimestamptz(zonedDateTime.toOffsetDateTime()));
    }
    if (value instanceof LocalDateTime localDateTime) {
      return Variants.ofTimestampntz(DateTimeUtil.microsFromTimestamp(localDateTime));
    }
    if (value instanceof LocalDate localDate) {
      return Variants.ofDate(DateTimeUtil.daysFromDate(localDate));
    }
    if (value instanceof LocalTime localTime) {
      return Variants.ofTime(DateTimeUtil.microsFromTime(localTime));
    }
    if (value instanceof Date date) {
      String logicalName = schema != null ? schema.name() : null;
      // Connect represents Timestamp, Time, and Date logical types as java.util.Date at runtime;
      // normalize to Instant once, then interpret using the schema logical type name.
      Instant connectInstant = date.toInstant();
      if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(logicalName)) {
        return Variants.ofTimestamptz(DateTimeUtil.microsFromInstant(connectInstant));
      }
      if (org.apache.kafka.connect.data.Time.LOGICAL_NAME.equals(logicalName)) {
        LocalTime utcTime = connectInstant.atZone(ZoneOffset.UTC).toLocalTime();
        return Variants.ofTime(DateTimeUtil.microsFromTime(utcTime));
      }
      if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(logicalName)) {
        return Variants.ofDate(DateTimeUtil.daysFromInstant(connectInstant));
      }
      throw new IllegalArgumentException(
          "Cannot convert java.util.Date to variant without a recognized logical type schema"
              + " (expected Timestamp, Time, or Date but got: "
              + logicalName
              + ")");
    }
    return null;
  }

  /**
   * Converts a Number to VariantValue; throw IllegalArgumentException if the value is not a
   * supported number representation.
   */
  private static VariantValue numberToVariantValue(Number number) {
    if (number instanceof BigDecimal bigDecimal) {
      return Variants.of(bigDecimal);
    }
    if (number instanceof BigInteger bigInteger) {
      return Variants.of(new BigDecimal(bigInteger));
    }
    if (number instanceof Integer integer) {
      return Variants.of(integer);
    }
    if (number instanceof Long longValue) {
      return Variants.of(longValue);
    }
    if (number instanceof Float floatValue) {
      return Variants.of(floatValue);
    }
    if (number instanceof Double doubleValue) {
      return Variants.of(doubleValue);
    }
    if (number instanceof Byte byteValue) {
      return Variants.of(byteValue);
    }
    if (number instanceof Short shortValue) {
      return Variants.of(shortValue);
    }
    throw new IllegalArgumentException(
        "Cannot convert Number to variant (unknown type): " + number.getClass().getName());
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
    // Search for the timezone offset sign starting after the seconds portion (index 19+).
    // With fractional seconds (e.g. "...T03:17:37.260514+00:00") the sign appears later
    // than index 19, so we must locate it dynamically rather than assuming a fixed position.
    int signIdx = -1;
    for (int i = 19; i < result.length(); i++) {
      char ch = result.charAt(i);
      if (ch == '+' || ch == '-') {
        signIdx = i;
        break;
      }
    }
    if (signIdx != -1 && signIdx + 3 < result.length() && result.charAt(signIdx + 3) == ':') {
      result = result.substring(0, signIdx + 3) + result.substring(signIdx + 4);
    }
    return result;
  }
}
