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
package org.apache.iceberg.data;

import java.util.function.Function;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroFileAccessFactory;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.orc.ORCFileAccessFactory;
import org.apache.iceberg.parquet.ParquetFileAccessFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class GenericFileAccessor {
  public static final FileAccessor<Schema, Record, Object> INSTANCE =
      new FileAccessor<>(
          ImmutableMap.of(
              FileFormat.PARQUET,
              new ParquetFileAccessFactory<>(
                  GenericParquetReaders::buildReader,
                  (nativeSchema, icebergSchema, messageType) ->
                      GenericParquetWriter.create(icebergSchema, messageType),
                  Function.identity()),
              FileFormat.AVRO,
              new AvroFileAccessFactory<>(
                  PlannedDataReader::create, (avroSchema, unused) -> DataWriter.create(avroSchema)),
              FileFormat.ORC,
              new ORCFileAccessFactory<>(
                  GenericOrcReader::buildReader,
                  (schema, messageType, nativeSchema) ->
                      GenericOrcWriter.buildWriter(schema, messageType),
                  Function.identity())));

  private GenericFileAccessor() {}
}
