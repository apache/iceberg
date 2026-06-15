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
package org.apache.iceberg.data.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.AvroTestHelpers;
import org.apache.iceberg.data.FileFormatTestSupport;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.ParquetFileTestUtils;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;

public class ParquetFormat implements FileFormatTestSupport {
  @Override
  public FileFormat format() {
    return FileFormat.PARQUET;
  }

  @Override
  public void writeRecordsWithoutFieldIds(
      OutputFile outputFile, Schema schema, List<Record> records) throws IOException {
    org.apache.avro.Schema avroSchemaWithoutIds = AvroTestHelpers.removeIds(schema);

    try (ParquetWriter<GenericData.Record> writer =
        AvroParquetWriter.<GenericData.Record>builder(ParquetFileTestUtils.file(outputFile))
            .withDataModel(GenericData.get())
            .withSchema(avroSchemaWithoutIds)
            .withConf(new Configuration())
            .build()) {
      for (Record record : records) {
        GenericData.Record avroRecord = new GenericData.Record(avroSchemaWithoutIds);
        for (Types.NestedField field : schema.columns()) {
          avroRecord.put(field.name(), record.getField(field.name()));
        }

        writer.write(avroRecord);
      }
    }

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetFileTestUtils.file(outputFile.toInputFile()))) {
      assertThat(ParquetSchemaUtil.hasIds(reader.getFooter().getFileMetaData().getSchema()))
          .isFalse();
    }
  }

  @Override
  public Map<String, String> testPropertiesToSet() {
    return Map.of(
        TableProperties.PARQUET_COMPRESSION,
        "gzip",
        TableProperties.PARQUET_PAGE_ROW_LIMIT,
        "1",
        TableProperties.PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT,
        "1");
  }

  @Override
  public boolean checkTestProperties(InputFile inputFile) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetFileTestUtils.file(inputFile))) {
      boolean compressionMatches =
          "GZIP"
              .equals(reader.getFooter().getBlocks().get(0).getColumns().get(0).getCodec().name());

      return compressionMatches && hasExpectedRowsPerPage(reader, 1);
    }
  }

  private boolean hasExpectedRowsPerPage(ParquetFileReader reader, int expectedRowsPerPage)
      throws IOException {
    PageReadStore rowGroup;

    while ((rowGroup = reader.readNextRowGroup()) != null) {
      PageReader pageReader =
          rowGroup.getPageReader(reader.getFileMetaData().getSchema().getColumns().get(0));
      DataPage page = pageReader.readPage();
      if (page != null) {
        return page.getValueCount() == expectedRowsPerPage;
      }
    }

    return false;
  }

  @Override
  public String metadataValue(InputFile inputFile, String key) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(ParquetFileTestUtils.file(inputFile))) {
      return reader.getFooter().getFileMetaData().getKeyValueMetaData().get(key);
    }
  }

  @Override
  public String splitSizeProperty() {
    return TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
  }
}
