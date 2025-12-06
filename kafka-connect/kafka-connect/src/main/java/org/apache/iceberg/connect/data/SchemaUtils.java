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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SchemaUpdate.AddColumn;
import org.apache.iceberg.connect.data.SchemaUpdate.MakeOptional;
import org.apache.iceberg.connect.data.SchemaUpdate.UpdateType;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SchemaUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);

  private static final Pattern TRANSFORM_REGEX = Pattern.compile("(\\w+)\\((.+)\\)");

  static PrimitiveType needsDataTypeUpdate(Type currentIcebergType, Schema valueSchema) {
    if (currentIcebergType.typeId() == TypeID.FLOAT && valueSchema.type() == Schema.Type.FLOAT64) {
      return DoubleType.get();
    }
    if (currentIcebergType.typeId() == TypeID.INTEGER && valueSchema.type() == Schema.Type.INT64) {
      return LongType.get();
    }
    return null;
  }

  static void applySchemaUpdates(Table table, SchemaUpdate.Consumer updates) {
    if (updates == null || updates.empty()) {
      // no updates to apply
      return;
    }

    Tasks.range(1)
        .retry(IcebergSinkConfig.SCHEMA_UPDATE_RETRIES)
        .run(notUsed -> commitSchemaUpdates(table, updates));
  }

  private static void commitSchemaUpdates(Table table, SchemaUpdate.Consumer updates) {
    // get the latest schema in case another process updated it
    table.refresh();

    // filter out columns that have already been added
    List<AddColumn> addColumns =
        updates.addColumns().stream()
            .filter(addCol -> !columnExists(table.schema(), addCol))
            .collect(Collectors.toList());

    // filter out columns that have the updated type
    List<UpdateType> updateTypes =
        updates.updateTypes().stream()
            .filter(updateType -> !typeMatches(table.schema(), updateType))
            .collect(Collectors.toList());

    // filter out columns that have already been made optional
    List<MakeOptional> makeOptionals =
        updates.makeOptionals().stream()
            .filter(makeOptional -> !isOptional(table.schema(), makeOptional))
            .collect(Collectors.toList());

    if (addColumns.isEmpty() && updateTypes.isEmpty() && makeOptionals.isEmpty()) {
      // no updates to apply
      LOG.info("Schema for table {} already up-to-date", table.name());
      return;
    }

    // apply the updates
    UpdateSchema updateSchema = table.updateSchema();
    addColumns.forEach(
        update ->
            updateSchema.addColumn(
                update.parentName(), update.name(), update.type(), null, update.defaultValue()));
    updateTypes.forEach(update -> updateSchema.updateColumn(update.name(), update.type()));
    makeOptionals.forEach(update -> updateSchema.makeColumnOptional(update.name()));
    updateSchema.commit();
    LOG.info("Schema for table {} updated with new columns", table.name());
  }

  private static boolean columnExists(org.apache.iceberg.Schema schema, AddColumn update) {
    return schema.findType(update.key()) != null;
  }

  private static boolean typeMatches(org.apache.iceberg.Schema schema, UpdateType update) {
    Type type = schema.findType(update.name());
    if (type == null) {
      throw new IllegalArgumentException("Invalid column: " + update.name());
    }
    return type.typeId() == update.type().typeId();
  }

  private static boolean isOptional(org.apache.iceberg.Schema schema, MakeOptional update) {
    NestedField field = schema.findField(update.name());
    if (field == null) {
      throw new IllegalArgumentException("Invalid column: " + update.name());
    }
    return field.isOptional();
  }

  static PartitionSpec createPartitionSpec(
      org.apache.iceberg.Schema schema, List<String> partitionBy) {
    if (partitionBy.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
    partitionBy.forEach(
        partitionField -> {
          Matcher matcher = TRANSFORM_REGEX.matcher(partitionField);
          if (matcher.matches()) {
            String transform = matcher.group(1);
            switch (transform) {
              case "year":
              case "years":
                specBuilder.year(matcher.group(2));
                break;
              case "month":
              case "months":
                specBuilder.month(matcher.group(2));
                break;
              case "day":
              case "days":
                specBuilder.day(matcher.group(2));
                break;
              case "hour":
              case "hours":
                specBuilder.hour(matcher.group(2));
                break;
              case "bucket":
                {
                  Pair<String, Integer> args = transformArgPair(matcher.group(2));
                  specBuilder.bucket(args.first(), args.second());
                  break;
                }
              case "truncate":
                {
                  Pair<String, Integer> args = transformArgPair(matcher.group(2));
                  specBuilder.truncate(args.first(), args.second());
                  break;
                }
              default:
                throw new UnsupportedOperationException("Unsupported transform: " + transform);
            }
          } else {
            specBuilder.identity(partitionField);
          }
        });
    return specBuilder.build();
  }

  private static Pair<String, Integer> transformArgPair(String argsStr) {
    List<String> parts = Splitter.on(',').splitToList(argsStr);
    if (parts.size() != 2) {
      throw new IllegalArgumentException("Invalid argument " + argsStr + ", should have 2 parts");
    }
    return Pair.of(parts.get(0).trim(), Integer.parseInt(parts.get(1).trim()));
  }

  static Type toIcebergType(Schema valueSchema, IcebergSinkConfig config, int formatVersion) {
    return new SchemaGenerator(config, formatVersion).toIcebergType(valueSchema);
  }

  static Type inferIcebergType(Object value, IcebergSinkConfig config) {
    return new SchemaGenerator(config, IcebergSinkConfig.DEFAULT_VALUE_MIN_FORMAT_VERSION)
        .inferIcebergType(value);
  }

  /**
   * Converts a Kafka Connect default value to an Iceberg Literal based on the provided Iceberg
   * type.
   *
   * @param defaultValue the default value from Kafka Connect schema
   * @param icebergType the target Iceberg type
   * @return an Iceberg Literal representing the default value, or null if conversion is not
   *     possible
   */
  static Literal<?> convertDefaultValue(Object defaultValue, Type icebergType) {
    if (defaultValue == null) {
      return null;
    }

    try {
      TypeID typeId = icebergType.typeId();
      switch (typeId) {
        case BOOLEAN:
          return Expressions.lit((Boolean) defaultValue);
        case INTEGER:
          return convertIntegerDefault(defaultValue);
        case LONG:
          return convertLongDefault(defaultValue);
        case FLOAT:
          return convertFloatDefault(defaultValue);
        case DOUBLE:
          return convertDoubleDefault(defaultValue);
        case STRING:
          return Expressions.lit(defaultValue.toString());
        case DECIMAL:
          return convertDecimalDefault(defaultValue, (DecimalType) icebergType);
        case DATE:
          return convertDateDefault(defaultValue, icebergType);
        case TIME:
          return convertTimeDefault(defaultValue, icebergType);
        case TIMESTAMP:
          return convertTimestampDefault(defaultValue);
        case BINARY:
        case FIXED:
          return convertBinaryDefault(defaultValue);
        case UUID:
          return convertUuidDefault(defaultValue);
        default:
          // Nested types (LIST, MAP, STRUCT) cannot have non-null defaults in Iceberg
          LOG.warn(
              "Default value conversion not supported for type: {}. Nested types can only have null defaults.",
              typeId);
          return null;
      }
    } catch (Exception e) {
      LOG.warn(
          "Failed to convert default value {} to Iceberg type {}", defaultValue, icebergType, e);
      return null;
    }
  }

  private static Literal<?> convertIntegerDefault(Object defaultValue) {
    if (defaultValue instanceof Number) {
      return Expressions.lit(((Number) defaultValue).intValue());
    }
    return null;
  }

  private static Literal<?> convertLongDefault(Object defaultValue) {
    if (defaultValue instanceof Number) {
      return Expressions.lit(((Number) defaultValue).longValue());
    }
    return null;
  }

  private static Literal<?> convertFloatDefault(Object defaultValue) {
    if (defaultValue instanceof Number) {
      return Expressions.lit(((Number) defaultValue).floatValue());
    }
    return null;
  }

  private static Literal<?> convertDoubleDefault(Object defaultValue) {
    if (defaultValue instanceof Number) {
      return Expressions.lit(((Number) defaultValue).doubleValue());
    }
    return null;
  }

  private static Literal<?> convertDecimalDefault(Object defaultValue, DecimalType decimalType) {
    if (defaultValue instanceof BigDecimal) {
      return Expressions.lit((BigDecimal) defaultValue);
    } else if (defaultValue instanceof byte[]) {
      // Kafka Connect Decimal can be byte array
      // BigDecimal constructor takes (unscaledValue, scale)
      // Precision is determined by the number of digits in unscaledValue
      BigDecimal decimal =
          new BigDecimal(new java.math.BigInteger((byte[]) defaultValue), decimalType.scale());
      return Expressions.lit(decimal);
    }
    return null;
  }

  private static Literal<?> convertDateDefault(Object defaultValue, Type icebergType) {
    // Iceberg Date is stored as days from epoch (int)
    if (defaultValue instanceof java.util.Date) {
      // Convert java.util.Date to days from epoch
      long epochDay =
          ((java.util.Date) defaultValue)
              .toInstant()
              .atZone(java.time.ZoneOffset.UTC)
              .toLocalDate()
              .toEpochDay();
      // Create an IntegerLiteral and convert to DateLiteral using .to(Type)
      return Expressions.lit((int) epochDay).to(icebergType);
    } else if (defaultValue instanceof Number) {
      // Already in days from epoch
      return Expressions.lit(((Number) defaultValue).intValue()).to(icebergType);
    } else if (defaultValue instanceof LocalDate) {
      int days = (int) ((LocalDate) defaultValue).toEpochDay();
      return Expressions.lit(days).to(icebergType);
    }
    return null;
  }

  private static Literal<?> convertTimeDefault(Object defaultValue, Type icebergType) {
    // Iceberg Time is stored as microseconds from midnight (long)
    if (defaultValue instanceof java.util.Date) {
      // Kafka Connect Time is milliseconds since midnight
      long millis = ((java.util.Date) defaultValue).getTime();
      // Create a LongLiteral and convert to TimeLiteral using .to(Type)
      return Expressions.lit(millis * 1000).to(icebergType);
    } else if (defaultValue instanceof Number) {
      // Assume microseconds from midnight
      return Expressions.lit(((Number) defaultValue).longValue()).to(icebergType);
    } else if (defaultValue instanceof LocalTime) {
      long micros = ((LocalTime) defaultValue).toNanoOfDay() / 1000;
      return Expressions.lit(micros).to(icebergType);
    }
    return null;
  }

  private static Literal<?> convertTimestampDefault(Object defaultValue) {
    // Iceberg Timestamp is stored as microseconds from epoch (long)
    if (defaultValue instanceof java.util.Date) {
      // Kafka Connect Timestamp is milliseconds from epoch
      long micros = ((java.util.Date) defaultValue).getTime() * 1000;
      // Create TimestampLiteral directly using micros() which returns the correct type
      return Expressions.micros(micros);
    } else if (defaultValue instanceof Number) {
      // Assume microseconds from epoch
      return Expressions.micros(((Number) defaultValue).longValue());
    } else if (defaultValue instanceof LocalDateTime) {
      long micros =
          ((LocalDateTime) defaultValue).atZone(java.time.ZoneOffset.UTC).toInstant().toEpochMilli()
              * 1000;
      return Expressions.micros(micros);
    } else if (defaultValue instanceof OffsetDateTime) {
      long micros = ((OffsetDateTime) defaultValue).toInstant().toEpochMilli() * 1000;
      return Expressions.micros(micros);
    }
    return null;
  }

  private static Literal<?> convertBinaryDefault(Object defaultValue) {
    if (defaultValue instanceof byte[]) {
      return Expressions.lit(ByteBuffer.wrap((byte[]) defaultValue));
    } else if (defaultValue instanceof ByteBuffer) {
      return Expressions.lit((ByteBuffer) defaultValue);
    }
    return null;
  }

  private static Literal<?> convertUuidDefault(Object defaultValue) {
    if (defaultValue instanceof java.util.UUID) {
      return Expressions.lit((java.util.UUID) defaultValue);
    } else if (defaultValue instanceof String) {
      return Expressions.lit(java.util.UUID.fromString((String) defaultValue));
    }
    return null;
  }

  static class SchemaGenerator {

    private int fieldId = 1;
    private final IcebergSinkConfig config;
    private final int formatVersion;

    SchemaGenerator(IcebergSinkConfig config, int formatVersion) {
      this.config = config;
      this.formatVersion = formatVersion;
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    Type toIcebergType(Schema valueSchema) {
      switch (valueSchema.type()) {
        case BOOLEAN:
          return BooleanType.get();
        case BYTES:
          if (Decimal.LOGICAL_NAME.equals(valueSchema.name())) {
            int scale = Integer.parseInt(valueSchema.parameters().get(Decimal.SCALE_FIELD));
            return DecimalType.of(38, scale);
          }
          return BinaryType.get();
        case INT8:
        case INT16:
          return IntegerType.get();
        case INT32:
          if (Date.LOGICAL_NAME.equals(valueSchema.name())) {
            return DateType.get();
          } else if (Time.LOGICAL_NAME.equals(valueSchema.name())) {
            return TimeType.get();
          }
          return IntegerType.get();
        case INT64:
          if (Timestamp.LOGICAL_NAME.equals(valueSchema.name())) {
            return TimestampType.withZone();
          }
          return LongType.get();
        case FLOAT32:
          return FloatType.get();
        case FLOAT64:
          return DoubleType.get();
        case ARRAY:
          Type elementType = toIcebergType(valueSchema.valueSchema());
          if (config.schemaForceOptional() || valueSchema.valueSchema().isOptional()) {
            return ListType.ofOptional(nextId(), elementType);
          } else {
            return ListType.ofRequired(nextId(), elementType);
          }
        case MAP:
          Type keyType = toIcebergType(valueSchema.keySchema());
          Type valueType = toIcebergType(valueSchema.valueSchema());
          if (config.schemaForceOptional() || valueSchema.valueSchema().isOptional()) {
            return MapType.ofOptional(nextId(), nextId(), keyType, valueType);
          } else {
            return MapType.ofRequired(nextId(), nextId(), keyType, valueType);
          }
        case STRUCT:
          List<NestedField> structFields =
              valueSchema.fields().stream()
                  .map(
                      field -> {
                        Type fieldType = toIcebergType(field.schema());
                        NestedField.Builder builder =
                            NestedField.builder()
                                .isOptional(
                                    config.schemaForceOptional() || field.schema().isOptional())
                                .withId(nextId())
                                .ofType(fieldType)
                                .withName(field.name());

                        // Apply default only if the table format version is greater or equal to the
                        // minimum format version needed to support default which is 3
                        if (formatVersion >= IcebergSinkConfig.DEFAULT_VALUE_MIN_FORMAT_VERSION) {
                          // Extract default value from Kafka Connect schema if present
                          Object defaultValue = field.schema().defaultValue();
                          if (defaultValue != null) {
                            Literal<?> defaultLiteral =
                                convertDefaultValue(defaultValue, fieldType);
                            if (defaultLiteral != null) {
                              builder.withInitialDefault(defaultLiteral);
                              builder.withWriteDefault(defaultLiteral);
                            }
                          }
                        }

                        return builder.build();
                      })
                  .collect(Collectors.toList());
          return StructType.of(structFields);
        case STRING:
        default:
          return StringType.get();
      }
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    Type inferIcebergType(Object value) {
      if (value == null) {
        return null;
      } else if (value instanceof String) {
        return StringType.get();
      } else if (value instanceof Boolean) {
        return BooleanType.get();
      } else if (value instanceof BigDecimal) {
        BigDecimal bigDecimal = (BigDecimal) value;
        return DecimalType.of(bigDecimal.precision(), bigDecimal.scale());
      } else if (value instanceof Integer || value instanceof Long) {
        return LongType.get();
      } else if (value instanceof Float || value instanceof Double) {
        return DoubleType.get();
      } else if (value instanceof LocalDate) {
        return DateType.get();
      } else if (value instanceof LocalTime) {
        return TimeType.get();
      } else if (value instanceof java.util.Date || value instanceof OffsetDateTime) {
        return TimestampType.withZone();
      } else if (value instanceof LocalDateTime) {
        return TimestampType.withoutZone();
      } else if (value instanceof List) {
        List<?> list = (List<?>) value;
        if (list.isEmpty()) {
          return null;
        }
        Type elementType = inferIcebergType(list.get(0));
        return elementType == null ? null : ListType.ofOptional(nextId(), elementType);
      } else if (value instanceof Map) {
        Map<?, ?> map = (Map<?, ?>) value;
        List<NestedField> structFields =
            map.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                .map(
                    entry -> {
                      Type valueType = inferIcebergType(entry.getValue());
                      return valueType == null
                          ? null
                          : NestedField.optional(nextId(), entry.getKey().toString(), valueType);
                    })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (structFields.isEmpty()) {
          return null;
        }
        return StructType.of(structFields);
      } else {
        return null;
      }
    }

    private int nextId() {
      return fieldId++;
    }
  }

  private SchemaUtils() {}
}
