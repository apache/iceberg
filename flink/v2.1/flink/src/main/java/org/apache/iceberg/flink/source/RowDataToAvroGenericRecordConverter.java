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
package org.apache.iceberg.flink.source;

import java.io.Serializable;
import java.util.function.Function;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.annotation.Internal;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.flink.FlinkSchemaUtil;

/**
 * This is not serializable because Avro {@link Schema} is not actually serializable, even though it
 * implements {@link Serializable} interface.
 */
@Internal
public class RowDataToAvroGenericRecordConverter implements Function<RowData, GenericRecord> {
  private final RowDataToAvroConverters.RowDataToAvroConverter converter;
  private final Schema avroSchema;
  private final RowType rowType;

  private RowDataToAvroGenericRecordConverter(RowType rowType, Schema avroSchema) {
    this.converter = RowDataToAvroConverters.createConverter(rowType);
    this.avroSchema = avroSchema;
    this.rowType = rowType;
  }

  @Override
  public GenericRecord apply(RowData rowData) {
    // Check if schema has timestamp-nanos fields
    if (needsCustomConversion(avroSchema)) {
      return convertWithNanos(rowData);
    }
    return (GenericRecord) converter.convert(avroSchema, rowData);
  }

  private boolean needsCustomConversion(Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      if (field.schema().getLogicalType() instanceof LogicalTypes.TimestampNanos) {
        return true;
      }
    }
    return false;
  }

  private GenericRecord convertWithNanos(RowData rowData) {
    GenericRecord result = new GenericData.Record(avroSchema);

    for (int i = 0; i < avroSchema.getFields().size(); i++) {
      Schema.Field field = avroSchema.getFields().get(i);
      if (rowData.isNullAt(i)) {
        result.put(i, null);
      } else if (field.schema().getLogicalType() instanceof LogicalTypes.TimestampNanos) {
        // Handle timestamp-nanos by converting TimestampData to long nanos
        // Get precision from the rowType
        int precision = 6; // default
        if (rowType.getTypeAt(i).asSummaryString().contains("(9)")) {
          precision = 9;
        }
        TimestampData timestampData = rowData.getTimestamp(i, precision);
        long nanos =
            timestampData.getMillisecond() * 1_000_000L + timestampData.getNanoOfMillisecond();
        result.put(i, nanos);
      } else {
        // For non-timestamp-nanos fields, copy value directly from rowData
        result.put(i, getFieldValue(rowData, i, field));
      }
    }
    return result;
  }

  private Object getFieldValue(RowData rowData, int pos, Schema.Field field) {
    Schema fieldSchema = field.schema();

    // Handle unions (nullable fields)
    if (fieldSchema.getType() == Schema.Type.UNION) {
      for (Schema unionSchema : fieldSchema.getTypes()) {
        if (unionSchema.getType() != Schema.Type.NULL) {
          fieldSchema = unionSchema;
          break;
        }
      }
    }

    // Check for logical types
    if (fieldSchema.getLogicalType() != null) {
      if (fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis
          || fieldSchema.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
        // Note: Although the Avro logical type is timestamp-micros, Flink's AvroToRowDataConverters
        // expects long values in milliseconds, not microseconds. This is a quirk of Flink's
        // implementation.
        TimestampData ts = rowData.getTimestamp(pos, 6);
        return ts.getMillisecond();
      } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
        LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) fieldSchema.getLogicalType();
        org.apache.flink.table.data.DecimalData decimalData =
            rowData.getDecimal(pos, decimal.getPrecision(), decimal.getScale());
        return java.nio.ByteBuffer.wrap(decimalData.toBigDecimal().unscaledValue().toByteArray());
      }
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
        return rowData.getString(pos).toString();
      case BYTES:
        return java.nio.ByteBuffer.wrap(rowData.getBinary(pos));
      case FIXED:
        return java.nio.ByteBuffer.wrap(rowData.getBinary(pos));
      default:
        // For other types, try to get as generic field
        if (rowData instanceof org.apache.flink.table.data.GenericRowData) {
          return ((org.apache.flink.table.data.GenericRowData) rowData).getField(pos);
        }
        return null;
    }
  }

  /** Create a converter based on Iceberg schema */
  public static RowDataToAvroGenericRecordConverter fromIcebergSchema(
      String tableName, org.apache.iceberg.Schema icebergSchema) {
    RowType rowType = FlinkSchemaUtil.convert(icebergSchema);
    Schema avroSchema = AvroSchemaUtil.convert(icebergSchema, tableName);
    return new RowDataToAvroGenericRecordConverter(rowType, avroSchema);
  }

  /** Create a mapper based on Avro schema */
  public static RowDataToAvroGenericRecordConverter fromAvroSchema(Schema avroSchema) {
    DataType dataType = AvroSchemaConverter.convertToDataType(avroSchema.toString());
    LogicalType logicalType = TypeConversions.fromDataToLogicalType(dataType);
    RowType rowType = RowType.of(logicalType.getChildren().toArray(new LogicalType[0]));
    return new RowDataToAvroGenericRecordConverter(rowType, avroSchema);
  }
}
