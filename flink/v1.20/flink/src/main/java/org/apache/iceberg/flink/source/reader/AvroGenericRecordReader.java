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

import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.source.AvroGenericRecordFileScanTaskReader;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.flink.source.RowDataToAvroGenericRecordConverter;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class AvroGenericRecordReader extends DataIteratorReader<GenericRecord> {
  private final String tableName;
  private final Schema readSchema;
  private final FileIO io;
  private final EncryptionManager encryption;
  private final RowDataFileScanTaskReader rowDataReader;
  private final TypeInformation<GenericRecord> outputTypeInfo;

  private transient RowDataToAvroGenericRecordConverter converter;

  public AvroGenericRecordReader(
      String tableName,
      ReadableConfig config,
      Schema tableSchema,
      Schema projectedSchema,
      String nameMapping,
      boolean caseSensitive,
      FileIO io,
      EncryptionManager encryption,
      List<Expression> filters) {
    super(new ListDataIteratorBatcher<>(config));
    this.tableName = tableName;
    this.readSchema = readSchema(tableSchema, projectedSchema);
    this.io = io;
    this.encryption = encryption;
    this.rowDataReader =
        new RowDataFileScanTaskReader(tableSchema, readSchema, nameMapping, caseSensitive, filters);

    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(readSchema, tableName);
    this.outputTypeInfo = new GenericRecordAvroTypeInfo(avroSchema);
  }

  @Override
  public DataIterator<GenericRecord> createDataIterator(IcebergSourceSplit split) {
    return new DataIterator<>(
        new AvroGenericRecordFileScanTaskReader(rowDataReader, lazyConverter()),
        split.task(),
        io,
        encryption);
  }

  @Override
  public TypeInformation<GenericRecord> outputTypeInfo() {
    return outputTypeInfo;
  }

  private RowDataToAvroGenericRecordConverter lazyConverter() {
    if (converter == null) {
      this.converter = RowDataToAvroGenericRecordConverter.fromIcebergSchema(tableName, readSchema);
    }
    return converter;
  }

  private static Schema readSchema(Schema tableSchema, Schema projectedSchema) {
    Preconditions.checkNotNull(tableSchema, "Table schema can't be null");
    return projectedSchema == null ? tableSchema : projectedSchema;
  }
}
