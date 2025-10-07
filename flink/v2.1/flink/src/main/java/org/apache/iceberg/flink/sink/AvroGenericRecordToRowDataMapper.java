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
package org.apache.iceberg.flink.sink;

import java.util.function.Function;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.iceberg.avro.AvroSchemaUtil;

/**
 * This util class converts Avro GenericRecord to Flink RowData. <br>
 * <br>
 * Internally it uses Flink {@link AvroToRowDataConverters}. Because of the precision difference
 * between how Iceberg schema (micro) and Flink {@link AvroToRowDataConverters} (milli) deal with
 * time type, we can't directly use the Avro Schema converted from Iceberg schema via {@link
 * AvroSchemaUtil#convert(org.apache.iceberg.Schema, String)}.
 */
public class AvroGenericRecordToRowDataMapper implements MapFunction<GenericRecord, RowData> {

  private final AvroToRowDataConverters.AvroToRowDataConverter converter;
  private final Schema avroSchema;
  private final Function<GenericRecord, RowData> customConverter;

  AvroGenericRecordToRowDataMapper(RowType rowType, Schema avroSchema) {
    this.avroSchema = avroSchema;
    this.converter = AvroToRowDataConverters.createRowConverter(rowType);
    // Check if we need custom conversion for timestamp-nanos
    this.customConverter = needsCustomConversion(avroSchema) ? this::convertWithNanos : null;
  }

  private boolean needsCustomConversion(Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      if (field.schema().getLogicalType() instanceof LogicalTypes.TimestampNanos) {
        return true;
      }
    }
    return false;
  }

  private RowData convertWithNanos(GenericRecord genericRecord) {
    RowData baseRowData = (RowData) converter.convert(genericRecord);
    GenericRowData result = new GenericRowData(baseRowData.getArity());
    result.setRowKind(baseRowData.getRowKind());

    for (int i = 0; i < avroSchema.getFields().size(); i++) {
      Schema.Field field = avroSchema.getFields().get(i);
      if (field.schema().getLogicalType() instanceof LogicalTypes.TimestampNanos) {
        // Handle timestamp-nanos by converting from long nanos to TimestampData
        Long nanos = (Long) genericRecord.get(i);
        if (nanos == null) {
          result.setField(i, null);
        } else {
          long millis = Math.floorDiv(nanos, 1_000_000L);
          int nanoOfMillis = (int) Math.floorMod(nanos, 1_000_000L);
          result.setField(i, TimestampData.fromEpochMillis(millis, nanoOfMillis));
        }
      } else {
        // Copy from base conversion
        if (baseRowData.isNullAt(i)) {
          result.setField(i, null);
        } else {
          // Get the value from baseRowData using the appropriate getter
          result.setField(i, getFieldValue(baseRowData, i, field));
        }
      }
    }
    return result;
  }

  private Object getFieldValue(RowData rowData, int pos, Schema.Field field) {
    // This is a simplified version - you may need to handle more types
    Schema fieldSchema = field.schema();
    if (fieldSchema.getType() == Schema.Type.UNION) {
      // Handle nullable fields
      for (Schema unionSchema : fieldSchema.getTypes()) {
        if (unionSchema.getType() != Schema.Type.NULL) {
          fieldSchema = unionSchema;
          break;
        }
      }
    }

    // Check for logical types first
    if (fieldSchema.getLogicalType() != null) {
      if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis
          || fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
        return rowData.getTimestamp(pos, 6);
      } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
        LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) fieldSchema.getLogicalType();
        return rowData.getDecimal(pos, decimal.getPrecision(), decimal.getScale());
      }
      // timestamp-nanos is handled separately in convertWithNanos
    }

    switch (fieldSchema.getType()) {
      case BOOLEAN:
        return rowData.getBoolean(pos);
      case INT:
        return rowData.getInt(pos);
      case LONG:
        return rowData.getLong(pos);
      case FLOAT:
        return rowData.getFloat(pos);
      case DOUBLE:
        return rowData.getDouble(pos);
      case STRING:
        return rowData.getString(pos);
      case BYTES:
        return rowData.getBinary(pos);
      case FIXED:
        return rowData.getBinary(pos);
      case RECORD:
        return rowData.getRow(pos, fieldSchema.getFields().size());
      case ARRAY:
        return rowData.getArray(pos);
      case MAP:
        return rowData.getMap(pos);
      default:
        // For complex types, just return as-is
        return ((GenericRowData) rowData).getField(pos);
    }
  }

  @Override
  public RowData map(GenericRecord genericRecord) throws Exception {
    if (customConverter != null) {
      return customConverter.apply(genericRecord);
    }
    return (RowData) converter.convert(genericRecord);
  }

  /** Create a mapper based on Avro schema. */
  public static AvroGenericRecordToRowDataMapper forAvroSchema(Schema avroSchema) {
    DataType dataType = AvroSchemaConverter.convertToDataType(avroSchema.toString());
    LogicalType logicalType = TypeConversions.fromDataToLogicalType(dataType);
    RowType rowType = RowType.of(logicalType.getChildren().toArray(new LogicalType[0]));
    return new AvroGenericRecordToRowDataMapper(rowType, avroSchema);
  }
}
