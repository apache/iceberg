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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.spark.SparkSQLProperties.PARQUET_READER_TYPE;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.ParquetReaderType;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;

public class TestParquetScan extends ScanTestBase {
  protected boolean vectorized() {
    return false;
  }

  @BeforeAll
  public static void startSpark() {
    ScanTestBase.spark = SparkSession.builder().master("local[2]").getOrCreate();
    ScanTestBase.spark.conf().set(PARQUET_READER_TYPE, ParquetReaderType.ICEBERG.toString());
    ScanTestBase.sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @Override
  protected void configureTable(Table table) {
    table
        .updateProperties()
        .set(TableProperties.PARQUET_VECTORIZATION_ENABLED, String.valueOf(vectorized()))
        .commit();
  }

  @Override
  protected void writeRecords(Table table, List<GenericData.Record> records) throws IOException {
    File dataFolder = new File(table.location(), "data");

    File parquetFile =
        new File(dataFolder, FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()));

    try (FileAppender<GenericData.Record> writer =
        Parquet.write(localOutput(parquetFile)).schema(table.schema()).build()) {
      writer.addAll(records);
    }

    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withFileSizeInBytes(parquetFile.length())
            .withPath(parquetFile.toString())
            .withRecordCount(records.size())
            .build();

    table.newAppend().appendFile(file).commit();
  }

  @Override
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    assumeThat(
            TypeUtil.find(
                writeSchema,
                type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()))
        .as("Cannot handle non-string map keys in parquet-avro")
        .isNull();

    super.writeAndValidate(writeSchema, expectedSchema);
  }
}
