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
package org.apache.iceberg.flink.source.reader;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.flink.FlinkSchemaUtil;

public class AvroGenericRecordConverter implements RowDataConverter<GenericRecord> {
  private final Schema avroSchema;
  private final RowDataToAvroConverters.RowDataToAvroConverter flinkConverter;
  private final TypeInformation<GenericRecord> outputTypeInfo;

  private AvroGenericRecordConverter(Schema avroSchema, RowType rowType) {
    this.avroSchema = avroSchema;
    this.flinkConverter = RowDataToAvroConverters.createConverter(rowType);
    this.outputTypeInfo = new GenericRecordAvroTypeInfo(avroSchema);
  }

  public static AvroGenericRecordConverter fromIcebergSchema(
      org.apache.iceberg.Schema icebergSchema, String tableName) {
    RowType rowType = FlinkSchemaUtil.convert(icebergSchema);
    Schema avroSchema = AvroSchemaUtil.convert(icebergSchema, tableName);
    return new AvroGenericRecordConverter(avroSchema, rowType);
  }

  public static AvroGenericRecordConverter fromAvroSchema(Schema avroSchema, String tableName) {
    DataType dataType = AvroSchemaConverter.convertToDataType(avroSchema.toString());
    LogicalType logicalType = TypeConversions.fromDataToLogicalType(dataType);
    RowType rowType = RowType.of(logicalType.getChildren().toArray(new LogicalType[0]));
    return new AvroGenericRecordConverter(avroSchema, rowType);
  }

  @Override
  public GenericRecord apply(RowData rowData) {
    return (GenericRecord) flinkConverter.convert(avroSchema, rowData);
  }

  @Override
  public TypeInformation<GenericRecord> getProducedType() {
    return outputTypeInfo;
  }
}
