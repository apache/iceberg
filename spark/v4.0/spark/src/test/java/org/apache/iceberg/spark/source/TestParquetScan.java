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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestParquetScan extends ScanTestBase {
  protected boolean vectorized() {
    return false;
  }

  @Override
  protected boolean supportsVariant() {
    return true;
  }

  @Override
  protected void configureTable(Table table) {
    table
        .updateProperties()
        .set(TableProperties.PARQUET_VECTORIZATION_ENABLED, String.valueOf(vectorized()))
        .commit();
  }

  @Override
  protected void writeRecords(Table table, List<Record> records) throws IOException {
    File dataFolder = new File(table.location(), "data");
    File parquetFile =
        new File(dataFolder, FileFormat.PARQUET.addExtension(UUID.randomUUID().toString()));

    try (FileAppender<Record> writer =
        Parquet.write(localOutput(parquetFile))
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
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

  @Test
  @Override
  public void testUnknownListType() {
    assertThatThrownBy(super::testUnknownListType)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot convert element Parquet: unknown");
  }

  @Test
  @Override
  public void testUnknownMapType() {
    assertThatThrownBy(super::testUnknownMapType)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot convert value Parquet: unknown");
  }
}
