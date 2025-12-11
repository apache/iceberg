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
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * This util class converts Avro GenericRecord to Flink RowData. <br>
 * <br>
 * Internally it uses Flink {@link AvroToRowDataConverters}. Because of the precision difference
 * between how Iceberg schema (micro) and Flink {@link AvroToRowDataConverters} (milli) deal with
 * time type, we can't directly use the Avro Schema converted from Iceberg schema. Additionally,
 * Flink's converter doesn't handle timestamp-nanos logical type properly, so we manually fix those
 * fields after conversion.
 */
public class AvroGenericRecordToRowDataMapper implements MapFunction<GenericRecord, RowData> {

  private final AvroToRowDataConverters.AvroToRowDataConverter converter;
  private final List<Integer> timestampNanosFieldIndices;
  private final RowData.FieldGetter[] fieldGetters;

  AvroGenericRecordToRowDataMapper(RowType rowType, List<Integer> timestampNanosFieldIndices) {
    this.converter = AvroToRowDataConverters.createRowConverter(rowType);
    this.timestampNanosFieldIndices = timestampNanosFieldIndices;

    // Pre-create field getters only if there are timestamp-nanos fields
    // (otherwise we take the early return path and never use them)
    if (!timestampNanosFieldIndices.isEmpty()) {
      this.fieldGetters = new RowData.FieldGetter[rowType.getFieldCount()];
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        LogicalType fieldType = rowType.getTypeAt(i);
        this.fieldGetters[i] = RowData.createFieldGetter(fieldType, i);
      }
    } else {
      this.fieldGetters = null;
    }
  }

  @Override
  public RowData map(GenericRecord genericRecord) throws Exception {
    RowData rowData = (RowData) converter.convert(genericRecord);

    // Post-process: Flink's AvroToRowDataConverters doesn't properly handle timestamp-nanos,
    // so we need to manually fix the timestamp fields after conversion
    if (timestampNanosFieldIndices.isEmpty()) {
      return rowData;
    }

    // Create a new GenericRowData with corrected timestamp-nanos fields
    GenericRowData correctedRowData = new GenericRowData(rowData.getArity());
    correctedRowData.setRowKind(rowData.getRowKind());

    for (int i = 0; i < rowData.getArity(); i++) {
      if (timestampNanosFieldIndices.contains(i)) {
        // Manually convert timestamp-nanos field from original Avro record
        Object avroValue = genericRecord.get(i);
        if (avroValue instanceof Long) {
          long nanos = (Long) avroValue;
          TimestampData timestampData =
              TimestampData.fromEpochMillis(nanos / 1_000_000L, (int) (nanos % 1_000_000L));
          correctedRowData.setField(i, timestampData);
        } else {
          // If not a Long, copy as-is
          correctedRowData.setField(i, fieldGetters[i].getFieldOrNull(rowData));
        }
      } else {
        // Copy other fields as-is
        correctedRowData.setField(i, fieldGetters[i].getFieldOrNull(rowData));
      }
    }

    return correctedRowData;
  }

  /** Create a mapper based on Avro schema. */
  public static AvroGenericRecordToRowDataMapper forAvroSchema(Schema avroSchema) {
    DataType dataType = AvroSchemaConverter.convertToDataType(avroSchema.toString());
    LogicalType logicalType = TypeConversions.fromDataToLogicalType(dataType);

    // Fix up timestamp-nanos fields: Flink's AvroSchemaConverter doesn't properly handle
    // timestamp-nanos logical type and converts it to BIGINT instead of TIMESTAMP(9).
    List<LogicalType> fixedFields = Lists.newArrayList();
    List<Integer> timestampNanosFieldIndices = Lists.newArrayList();
    List<Schema.Field> avroFields = avroSchema.getFields();
    LogicalType[] originalFields = logicalType.getChildren().toArray(new LogicalType[0]);

    for (int i = 0; i < avroFields.size(); i++) {
      Schema.Field avroField = avroFields.get(i);
      LogicalType fieldType = originalFields[i];

      if (avroField.schema().getLogicalType() instanceof LogicalTypes.TimestampNanos) {
        // Replace BIGINT with TIMESTAMP(9) for nanosecond precision
        fixedFields.add(new TimestampType(9));
        timestampNanosFieldIndices.add(i);
      } else {
        fixedFields.add(fieldType);
      }
    }

    RowType rowType = RowType.of(fixedFields.toArray(new LogicalType[0]));
    return new AvroGenericRecordToRowDataMapper(rowType, timestampNanosFieldIndices);
  }
}
