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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.annotation.Internal;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.RowData;
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

  private RowDataToAvroGenericRecordConverter(RowType rowType, Schema avroSchema) {
    this.converter = RowDataToAvroConverters.createConverter(rowType);
    this.avroSchema = avroSchema;
  }

  @Override
  public GenericRecord apply(RowData rowData) {
    return (GenericRecord) converter.convert(avroSchema, rowData);
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
    RowType rowType = RowType.of(logicalType.getChildren().stream().toArray(LogicalType[]::new));
    return new RowDataToAvroGenericRecordConverter(rowType, avroSchema);
  }
}
