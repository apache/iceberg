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
package io.tabular.iceberg.connect.data;

import static java.util.stream.Collectors.toList;

import io.tabular.iceberg.connect.data.SchemaUpdate.AddColumn;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.types.Type;
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
  private static final int COMMIT_RETRY_ATTEMPTS = 2; // 3 total attempts

  public static void applySchemaUpdates(Table table, List<AddColumn> updates) {
    if (updates == null || updates.isEmpty()) {
      // no updates to apply
      return;
    }

    Tasks.foreach(Collections.singleton(updates))
        .retry(COMMIT_RETRY_ATTEMPTS)
        .run(notUsed -> commitSchemaUpdates(table, updates));
  }

  private static void commitSchemaUpdates(Table table, List<AddColumn> updates) {
    // get the latest schema in case another process updated it
    table.refresh();

    // filter out columns that have already been added
    List<AddColumn> filteredUpdates =
        updates.stream().filter(update -> !columnExists(table.schema(), update)).collect(toList());

    if (filteredUpdates.isEmpty()) {
      // no updates to apply
      LOG.info("Schema for table {} already up-to-date", table.name());
      return;
    }

    // apply the updates
    UpdateSchema updateSchema = table.updateSchema();
    filteredUpdates.forEach(
        update -> updateSchema.addColumn(update.parentName(), update.name(), update.type()));
    updateSchema.commit();
    LOG.info("Schema for table {} updated with new columns", table.name());
  }

  private static boolean columnExists(org.apache.iceberg.Schema schema, AddColumn update) {
    StructType struct =
        update.parentName() == null
            ? schema.asStruct()
            : schema.findType(update.parentName()).asStructType();
    return struct.field(update.name()) != null;
  }

  public static Type toIcebergType(Schema valueSchema) {
    return new SchemaGenerator().toIcebergType(valueSchema);
  }

  public static Type inferIcebergType(Object value) {
    return new SchemaGenerator().inferIcebergType(value);
  }

  static class SchemaGenerator {

    private int fieldId = 1;

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
          return ListType.ofOptional(nextId(), elementType);
        case MAP:
          Type keyType = toIcebergType(valueSchema.keySchema());
          Type valueType = toIcebergType(valueSchema.valueSchema());
          return MapType.ofOptional(nextId(), nextId(), keyType, valueType);
        case STRUCT:
          List<NestedField> structFields =
              valueSchema.fields().stream()
                  .map(
                      field ->
                          NestedField.optional(
                              nextId(), field.name(), toIcebergType(field.schema())))
                  .collect(toList());
          return StructType.of(structFields);
        case STRING:
        default:
          return StringType.get();
      }
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    Type inferIcebergType(Object value) {
      if (value == null) {
        throw new UnsupportedOperationException("Cannot infer type from null value");
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
        if (!list.isEmpty()) {
          Type elementType = inferIcebergType(list.get(0));
          return ListType.ofOptional(nextId(), elementType);
        } else {
          return ListType.ofOptional(nextId(), StringType.get());
        }
      } else if (value instanceof Map) {
        Map<?, ?> map = (Map<?, ?>) value;
        List<NestedField> structFields =
            map.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                .map(
                    entry ->
                        NestedField.optional(
                            nextId(),
                            entry.getKey().toString(),
                            inferIcebergType(entry.getValue())))
                .collect(toList());
        return StructType.of(structFields);
      } else {
        return StringType.get();
      }
    }

    private int nextId() {
      return fieldId++;
    }
  }

  private SchemaUtils() {}
}
