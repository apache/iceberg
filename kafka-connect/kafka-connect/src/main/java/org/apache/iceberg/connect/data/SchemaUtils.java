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

import static java.util.stream.Collectors.toList;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.connect.data.SchemaUpdate.AddColumn;
import org.apache.iceberg.connect.data.SchemaUpdate.MakeOptional;
import org.apache.iceberg.connect.data.SchemaUpdate.UpdateType;
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

public class SchemaUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);

  private static final Pattern TRANSFORM_REGEX = Pattern.compile("(\\w+)\\((.+)\\)");

  public static PrimitiveType needsDataTypeUpdate(Type currentIcebergType, Schema valueSchema) {
    if (currentIcebergType.typeId() == TypeID.FLOAT && valueSchema.type() == Schema.Type.FLOAT64) {
      return DoubleType.get();
    }
    if (currentIcebergType.typeId() == TypeID.INTEGER && valueSchema.type() == Schema.Type.INT64) {
      return LongType.get();
    }
    return null;
  }

  public static void applySchemaUpdates(Table table, SchemaUpdate.Consumer updates) {
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
            .collect(toList());

    // filter out columns that have the updated type
    List<UpdateType> updateTypes =
        updates.updateTypes().stream()
            .filter(updateType -> !typeMatches(table.schema(), updateType))
            .collect(toList());

    // filter out columns that have already been made optional
    List<MakeOptional> makeOptionals =
        updates.makeOptionals().stream()
            .filter(makeOptional -> !isOptional(table.schema(), makeOptional))
            .collect(toList());

    if (addColumns.isEmpty() && updateTypes.isEmpty() && makeOptionals.isEmpty()) {
      // no updates to apply
      LOG.info("Schema for table {} already up-to-date", table.name());
      return;
    }

    // apply the updates
    UpdateSchema updateSchema = table.updateSchema();
    addColumns.forEach(
        update -> updateSchema.addColumn(update.parentName(), update.name(), update.type()));
    updateTypes.forEach(update -> updateSchema.updateColumn(update.name(), update.type()));
    makeOptionals.forEach(update -> updateSchema.makeColumnOptional(update.name()));
    updateSchema.commit();
    LOG.info("Schema for table {} updated with new columns", table.name());
  }

  private static boolean columnExists(org.apache.iceberg.Schema schema, AddColumn update) {
    StructType struct;
    if (update.parentName() == null) {
      struct = schema.asStruct();
    } else {
      Type type = schema.findType(update.parentName()).asStructType();
      if (type == null) {
        return false;
      }
      struct = type.asStructType();
    }
    return struct.field(update.name()) != null;
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

  public static PartitionSpec createPartitionSpec(
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

  public static Type toIcebergType(Schema valueSchema, IcebergSinkConfig config) {
    return new SchemaGenerator(config).toIcebergType(valueSchema);
  }

  public static Optional<Type> inferIcebergType(Object value, IcebergSinkConfig config) {
    return new SchemaGenerator(config).inferIcebergType(value);
  }

  static class SchemaGenerator {

    private int fieldId = 1;
    private final IcebergSinkConfig config;

    SchemaGenerator(IcebergSinkConfig config) {
      this.config = config;
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    Type toIcebergType(Schema valueSchema) {
      switch (valueSchema.type()) {
        case BOOLEAN:
          return BooleanType.get();
        case BYTES:
          if (valueSchema.name() != null && valueSchema.name().equals(Decimal.LOGICAL_NAME)) {
            int scale = Integer.parseInt(valueSchema.parameters().get(Decimal.SCALE_FIELD));
            return DecimalType.of(38, scale);
          }
          return BinaryType.get();
        case INT8:
        case INT16:
          return IntegerType.get();
        case INT32:
          if (valueSchema.name() != null) {
            if (valueSchema.name().equals(Date.LOGICAL_NAME)) {
              return DateType.get();
            } else if (valueSchema.name().equals(Time.LOGICAL_NAME)) {
              return TimeType.get();
            }
          }
          return IntegerType.get();
        case INT64:
          if (valueSchema.name() != null && valueSchema.name().equals(Timestamp.LOGICAL_NAME)) {
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
                      field ->
                          NestedField.of(
                              nextId(),
                              config.schemaForceOptional() || field.schema().isOptional(),
                              field.name(),
                              toIcebergType(field.schema())))
                  .collect(toList());
          return StructType.of(structFields);
        case STRING:
        default:
          return StringType.get();
      }
    }

    Optional<Type> inferIcebergType(Object value) {
      return Optional.ofNullable(doInferIcebergType(value));
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private Type doInferIcebergType(Object value) {
      if (value == null) {
        return null;
      } else if (value instanceof String) {
        return StringType.get();
      } else if (value instanceof Boolean) {
        return BooleanType.get();
      } else if (value instanceof BigDecimal) {
        BigDecimal bigDecimal = (BigDecimal) value;
        return DecimalType.of(bigDecimal.precision(), bigDecimal.scale());
      } else if (value instanceof Number) {
        Number num = (Number) value;
        Double dbl = num.doubleValue();
        if (dbl.equals(Math.floor(dbl))) {
          return LongType.get();
        } else {
          return DoubleType.get();
        }
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
        Optional<Type> elementType = inferIcebergType(list.get(0));
        return elementType.map(type -> ListType.ofOptional(nextId(), type)).orElse(null);
      } else if (value instanceof Map) {
        Map<?, ?> map = (Map<?, ?>) value;
        List<NestedField> structFields =
            map.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                .map(
                    entry -> {
                      Optional<Type> valueType = inferIcebergType(entry.getValue());
                      return valueType
                          .map(
                              type ->
                                  NestedField.optional(nextId(), entry.getKey().toString(), type))
                          .orElse(null);
                    })
                .filter(Objects::nonNull)
                .collect(toList());
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
