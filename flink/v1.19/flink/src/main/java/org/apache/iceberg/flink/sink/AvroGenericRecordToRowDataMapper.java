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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.RowData;
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

  AvroGenericRecordToRowDataMapper(RowType rowType) {
    this.converter = AvroToRowDataConverters.createRowConverter(rowType);
  }

  @Override
  public RowData map(GenericRecord genericRecord) throws Exception {
    return (RowData) converter.convert(genericRecord);
  }

  /** Create a mapper based on Avro schema. */
  public static AvroGenericRecordToRowDataMapper forAvroSchema(Schema avroSchema) {
    DataType dataType = AvroSchemaConverter.convertToDataType(avroSchema.toString());
    LogicalType logicalType = TypeConversions.fromDataToLogicalType(dataType);
    RowType rowType = RowType.of(logicalType.getChildren().toArray(new LogicalType[0]));
    return new AvroGenericRecordToRowDataMapper(rowType);
  }
}
