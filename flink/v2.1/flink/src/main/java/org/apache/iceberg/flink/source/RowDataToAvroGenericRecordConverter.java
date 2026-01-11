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
import java.util.Set;
import java.util.function.Function;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.annotation.Internal;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
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
  private final Set<Integer> timestampNanosFieldIndices;
  private final RowData.FieldGetter[] fieldGetters;

  private RowDataToAvroGenericRecordConverter(
      RowType converterRowType, Schema avroSchema, Set<Integer> timestampNanosFieldIndices) {
    this.converter = RowDataToAvroConverters.createConverter(converterRowType);
    this.avroSchema = avroSchema;
    this.timestampNanosFieldIndices = timestampNanosFieldIndices;

    // Pre-create field getters only if there are timestamp-nanos fields
    // (otherwise we take the early return path and never use them)
    if (!timestampNanosFieldIndices.isEmpty()) {
      this.fieldGetters = new RowData.FieldGetter[converterRowType.getFieldCount()];
      for (int i = 0; i < converterRowType.getFieldCount(); i++) {
        LogicalType fieldType = converterRowType.getTypeAt(i);
        this.fieldGetters[i] = RowData.createFieldGetter(fieldType, i);
      }
    } else {
      this.fieldGetters = null;
    }
  }

  @Override
  public GenericRecord apply(RowData rowData) {
    // Pre-process: Flink's RowDataToAvroConverters expects Long (nanoseconds) for timestamp-nanos,
    // but our RowData has TimestampData. Convert TimestampData to Long nanoseconds.
    if (timestampNanosFieldIndices.isEmpty()) {
      return (GenericRecord) converter.convert(avroSchema, rowData);
    }

    // Create a new GenericRowData with Long values for timestamp-nanos fields
    GenericRowData processedRowData = new GenericRowData(rowData.getArity());
    processedRowData.setRowKind(rowData.getRowKind());

    for (int i = 0; i < rowData.getArity(); i++) {
      if (timestampNanosFieldIndices.contains(i)) {
        // Convert TimestampData to Long nanoseconds
        if (!rowData.isNullAt(i)) {
          TimestampData timestampData = rowData.getTimestamp(i, 9);
          long nanos =
              timestampData.getMillisecond() * 1_000_000L + timestampData.getNanoOfMillisecond();
          processedRowData.setField(i, nanos);
        } else {
          processedRowData.setField(i, null);
        }
      } else {
        // Copy other fields as-is using pre-created field getter
        processedRowData.setField(i, fieldGetters[i].getFieldOrNull(rowData));
      }
    }

    return (GenericRecord) converter.convert(avroSchema, processedRowData);
  }

  /** Create a converter based on Iceberg schema */
  public static RowDataToAvroGenericRecordConverter fromIcebergSchema(
      String tableName, org.apache.iceberg.Schema icebergSchema) {
    RowType originalRowType = FlinkSchemaUtil.convert(icebergSchema);
    Schema avroSchema = AvroSchemaUtil.convert(icebergSchema, tableName);

    // Detect timestamp-nanos fields and create converter RowType with BIGINT for those fields
    List<LogicalType> converterFields = Lists.newArrayList();
    Set<Integer> timestampNanosFieldIndices = new java.util.HashSet<>();
    List<Schema.Field> avroFields = avroSchema.getFields();

    for (int i = 0; i < avroFields.size(); i++) {
      if (avroFields.get(i).schema().getLogicalType() instanceof LogicalTypes.TimestampNanos) {
        // Use BIGINT for Flink's converter (it expects Long for timestamp-nanos)
        converterFields.add(new BigIntType());
        timestampNanosFieldIndices.add(i);
      } else {
        converterFields.add(originalRowType.getTypeAt(i));
      }
    }

    RowType converterRowType = RowType.of(converterFields.toArray(new LogicalType[0]));
    return new RowDataToAvroGenericRecordConverter(
        converterRowType, avroSchema, timestampNanosFieldIndices);
  }

  /** Create a mapper based on Avro schema */
  public static RowDataToAvroGenericRecordConverter fromAvroSchema(Schema avroSchema) {
    DataType dataType = AvroSchemaConverter.convertToDataType(avroSchema.toString());
    LogicalType logicalType = TypeConversions.fromDataToLogicalType(dataType);

    // Fix up timestamp-nanos fields: Flink's RowDataToAvroConverters expects Long (nanoseconds)
    // for timestamp-nanos logical type, but our RowData has TimESTAMP(9) with TimestampData.
    // We need to use BIGINT in the RowType for Flink's converter, but detect these fields
    // to convert TimestampData to Long before conversion.
    List<LogicalType> converterFields = Lists.newArrayList();
    Set<Integer> timestampNanosFieldIndices = new java.util.HashSet<>();
    List<Schema.Field> avroFields = avroSchema.getFields();
    LogicalType[] originalFields = logicalType.getChildren().toArray(new LogicalType[0]);

    for (int i = 0; i < avroFields.size(); i++) {
      Schema.Field avroField = avroFields.get(i);
      LogicalType fieldType = originalFields[i];

      // Check if this field has timestamp-nanos logical type
      if (avroField.schema().getLogicalType() instanceof LogicalTypes.TimestampNanos) {
        // Use BIGINT for Flink's converter (it expects Long for timestamp-nanos)
        converterFields.add(new BigIntType());
        timestampNanosFieldIndices.add(i);
      } else {
        converterFields.add(fieldType);
      }
    }

    RowType converterRowType = RowType.of(converterFields.toArray(new LogicalType[0]));
    return new RowDataToAvroGenericRecordConverter(
        converterRowType, avroSchema, timestampNanosFieldIndices);
  }
}
