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

import java.util.List;
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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

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
  private final int[] timestampNanosFieldIndices;

  AvroGenericRecordToRowDataMapper(RowType rowType, Schema avroSchema) {
    this.converter = AvroToRowDataConverters.createRowConverter(rowType);
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

  private RowData postProcessTimestampNanos(RowData baseRowData, GenericRecord genericRecord) {
    if (timestampNanosFieldIndices.length == 0) {
      return baseRowData; // No timestamp-nanos fields to process
    }

    GenericRowData result = new GenericRowData(baseRowData.getArity());
    result.setRowKind(baseRowData.getRowKind());

    // Copy all fields from base conversion
    for (int i = 0; i < baseRowData.getArity(); i++) {
      if (baseRowData.isNullAt(i)) {
        result.setField(i, null);
      } else {
        result.setField(i, ((GenericRowData) baseRowData).getField(i));
      }
    }

    // Override only timestamp-nanos fields with correct nanosecond precision
    for (int fieldIndex : timestampNanosFieldIndices) {
      Long nanos = (Long) genericRecord.get(fieldIndex);
      if (nanos == null) {
        result.setField(fieldIndex, null);
      } else {
        long mills = Math.floorDiv(nanos, 1_000_000L);
        int leftoverNanos = (int) Math.floorMod(nanos, 1_000_000L);
        result.setField(fieldIndex, TimestampData.fromEpochMillis(mills, leftoverNanos));
      }
    }

    return result;
  }

  @Override
  public RowData map(GenericRecord genericRecord) throws Exception {
    RowData baseRowData = (RowData) converter.convert(genericRecord);
    return postProcessTimestampNanos(baseRowData, genericRecord);
  }

  /** Create a mapper based on Avro schema. */
  public static AvroGenericRecordToRowDataMapper forAvroSchema(Schema avroSchema) {
    DataType dataType = AvroSchemaConverter.convertToDataType(avroSchema.toString());
    LogicalType logicalType = TypeConversions.fromDataToLogicalType(dataType);
    RowType rowType = RowType.of(logicalType.getChildren().toArray(new LogicalType[0]));
    return new AvroGenericRecordToRowDataMapper(rowType, avroSchema);
  }
}
