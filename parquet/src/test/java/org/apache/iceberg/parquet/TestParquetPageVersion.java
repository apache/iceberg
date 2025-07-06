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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.parquet.ParquetWritingTestUtils.createTempFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestParquetPageVersion {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private List<Record> records;

  @TempDir private Path temp;

  @BeforeEach
  void createRecords() {
    GenericRecord record = GenericRecord.create(SCHEMA);

    this.records =
        ImmutableList.of(
            record.copy(ImmutableMap.of("id", 1L, "data", "a")),
            record.copy(ImmutableMap.of("id", 2L, "data", "b")),
            record.copy(ImmutableMap.of("id", 3L, "data", "c")),
            record.copy(ImmutableMap.of("id", 4L, "data", "d")),
            record.copy(ImmutableMap.of("id", 5L, "data", "e")));
  }

  @Test
  void testWriterDefaultsToPageVersion1() throws IOException {
    OutputFile outputFile = newOutputFile();

    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      writer.addAll(records);
    }

    assertThat(firstDataPage(outputFile)).isInstanceOf(DataPageV1.class);
  }

  @Test
  void testWriterUsesConfiguredPageVersion() throws IOException {
    OutputFile outputFile = newOutputFile();

    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(SCHEMA)
            .set(TableProperties.PARQUET_PAGE_VERSION, "v2")
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      writer.addAll(records);
    }

    assertThat(firstDataPage(outputFile)).isInstanceOf(DataPageV2.class);
  }

  @Test
  void testDeleteWriterUsesConfiguredPageVersion() throws IOException {
    OutputFile outputFile = newOutputFile();

    EqualityDeleteWriter<Record> deleteWriter =
        Parquet.writeDeletes(outputFile)
            .createWriterFunc(GenericParquetWriter::create)
            .set(TableProperties.PARQUET_PAGE_VERSION, "v2")
            .overwrite()
            .rowSchema(SCHEMA)
            .withSpec(PartitionSpec.unpartitioned())
            .equalityFieldIds(1)
            .buildEqualityWriter();

    try (EqualityDeleteWriter<Record> writer = deleteWriter) {
      writer.write(records);
    }

    assertThat(firstDataPage(outputFile)).isInstanceOf(DataPageV2.class);
  }

  @Test
  void testDeleteWriterUsesDeleteSpecificPageVersion() throws IOException {
    OutputFile outputFile = newOutputFile();

    EqualityDeleteWriter<Record> deleteWriter =
        Parquet.writeDeletes(outputFile)
            .createWriterFunc(GenericParquetWriter::create)
            .set(TableProperties.PARQUET_PAGE_VERSION, "v1")
            .set(TableProperties.DELETE_PARQUET_PAGE_VERSION, "v2")
            .overwrite()
            .rowSchema(SCHEMA)
            .withSpec(PartitionSpec.unpartitioned())
            .equalityFieldIds(1)
            .buildEqualityWriter();

    try (EqualityDeleteWriter<Record> writer = deleteWriter) {
      writer.write(records);
    }

    assertThat(firstDataPage(outputFile)).isInstanceOf(DataPageV2.class);
  }

  @Test
  void testExplicitWriterVersion2OverridesPageVersionProperty() throws IOException {
    OutputFile outputFile = newOutputFile();

    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(SCHEMA)
            .set(TableProperties.PARQUET_PAGE_VERSION, "v1")
            .writerVersion(WriterVersion.PARQUET_2_0)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      writer.addAll(records);
    }

    assertThat(firstDataPage(outputFile)).isInstanceOf(DataPageV2.class);
  }

  @Test
  void testExplicitWriterVersion1OverridesPageVersionProperty() throws IOException {
    OutputFile outputFile = newOutputFile();

    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(SCHEMA)
            .set(TableProperties.PARQUET_PAGE_VERSION, "v2")
            .writerVersion(WriterVersion.PARQUET_1_0)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      writer.addAll(records);
    }

    assertThat(firstDataPage(outputFile)).isInstanceOf(DataPageV1.class);
  }

  @Test
  void testPageVersionPropertyAfterWriterVersionSetsVersion() throws IOException {
    OutputFile outputFile = newOutputFile();

    try (FileAppender<Record> writer =
        Parquet.write(outputFile)
            .schema(SCHEMA)
            .writerVersion(WriterVersion.PARQUET_1_0)
            .set(TableProperties.PARQUET_PAGE_VERSION, "v2")
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      writer.addAll(records);
    }

    assertThat(firstDataPage(outputFile)).isInstanceOf(DataPageV2.class);
  }

  @Test
  void testInvalidPageVersionFails() throws IOException {
    OutputFile outputFile = newOutputFile();

    assertThatThrownBy(
            () ->
                Parquet.write(outputFile)
                    .schema(SCHEMA)
                    .set(TableProperties.PARQUET_PAGE_VERSION, "3")
                    .createWriterFunc(GenericParquetWriter::create)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported Parquet page version: 3 (must be v1 or v2)");
  }

  @Test
  void testInvalidDeletePageVersionFails() throws IOException {
    OutputFile outputFile = newOutputFile();

    assertThatThrownBy(
            () ->
                Parquet.writeDeletes(outputFile)
                    .createWriterFunc(GenericParquetWriter::create)
                    .set(TableProperties.DELETE_PARQUET_PAGE_VERSION, "3")
                    .overwrite()
                    .rowSchema(SCHEMA)
                    .withSpec(PartitionSpec.unpartitioned())
                    .equalityFieldIds(1)
                    .buildEqualityWriter())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported Parquet page version: 3 (must be v1 or v2)");
  }

  private OutputFile newOutputFile() throws IOException {
    return Files.localOutput(createTempFile(temp));
  }

  private DataPage firstDataPage(OutputFile outputFile) throws IOException {
    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(outputFile.toInputFile()))) {
      PageReadStore rowGroup = reader.readNextRowGroup();
      assertThat(rowGroup).isNotNull();

      DataPage dataPage =
          rowGroup
              .getPageReader(
                  reader.getFileMetaData().getSchema().getColumnDescription(new String[] {"id"}))
              .readPage();
      assertThat(dataPage).isNotNull();
      return dataPage;
    }
  }
}
