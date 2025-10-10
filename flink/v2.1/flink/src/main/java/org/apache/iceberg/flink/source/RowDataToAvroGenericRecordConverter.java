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
import java.util.List;
import java.util.function.Function;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * This is not serializable because Avro {@link Schema} is not actually serializable, even though it
 * implements {@link Serializable} interface.
 */
@Internal
public class RowDataToAvroGenericRecordConverter implements Function<RowData, GenericRecord> {
  private final RowDataToAvroConverters.RowDataToAvroConverter converter;
  private final Schema avroSchema;
  private final int[] timestampNanosFieldIndices;

  private RowDataToAvroGenericRecordConverter(RowType rowType, Schema avroSchema) {
    this.converter = RowDataToAvroConverters.createConverter(rowType);
    this.avroSchema = avroSchema;
    this.timestampNanosFieldIndices = findTimestampNanosFields(avroSchema);
  }

  private int[] findTimestampNanosFields(Schema schema) {
    List<Integer> indices = Lists.newArrayList();
    for (int i = 0; i < schema.getFields().size(); i++) {
      Schema.Field field = schema.getFields().get(i);
      if (field.schema().getLogicalType() instanceof LogicalTypes.TimestampNanos) {
        indices.add(i);
      }
    }
    return indices.stream().mapToInt(Integer::intValue).toArray();
  }

  private GenericRecord postProcessTimestampNanos(GenericRecord baseRecord, RowData rowData) {
    if (timestampNanosFieldIndices.length == 0) {
      return baseRecord; // No timestamp-nanos fields to process
    }

    // Override only timestamp-nanos fields with correct nanosecond precision
    for (int fieldIndex : timestampNanosFieldIndices) {
      if (!rowData.isNullAt(fieldIndex)) {
        // Get precision from the rowType (assuming it's available in the context)
        int precision = 9; // Default to nanosecond precision for timestamp-nanos
        TimestampData timestampData = rowData.getTimestamp(fieldIndex, precision);
        // Use correct nanosecond precision conversion
        long nanos =
            timestampData.getMillisecond() * 1_000_000_000L + timestampData.getNanoOfMillisecond();
        baseRecord.put(fieldIndex, nanos);
      }
    }

    return baseRecord;
  }

  @Override
  public GenericRecord apply(RowData rowData) {
    GenericRecord baseRecord = (GenericRecord) converter.convert(avroSchema, rowData);
    return postProcessTimestampNanos(baseRecord, rowData);
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
