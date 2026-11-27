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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.metadata.BlockMetadata;
import org.apache.iceberg.parquet.metadata.ColumnChunkMetadata;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestNativeParquetFileWriter {

  private static final MessageType SCHEMA =
      Types.buildMessage()
          .required(INT32)
          .named("ID")
          .optional(INT32)
          .as(LogicalTypeAnnotation.dateType())
          .named("Day")
          .named("test_schema");

  @TempDir private File tempDir;

  @Test
  void appendedFileIsReadableByHadoopReader() throws IOException {
    File source = writeSource("source.parquet", 100);
    File target = new File(tempDir, "target.parquet");
    OutputFile output = Files.localOutput(target);

    try (NativeParquetFileWriter writer =
        NativeParquetFileWriter.create(output, SCHEMA, CompressionCodecName.SNAPPY)) {
      writer.appendFile(Files.localInput(source));
      writer.appendFile(Files.localInput(source));
      writer.end(java.util.Map.of("key", "value"));
    }

    int rows = 0;
    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(target.toURI()))
            .build()) {
      Group group;
      while ((group = reader.read()) != null) {
        assertThat(group.getInteger("ID", 0)).isEqualTo(rows % 100);
        rows++;
      }
    }

    assertThat(rows).isEqualTo(200);
  }

  @Test
  void appendKeepsStatisticsAndColumnCase() throws IOException {
    File source = writeSource("source.parquet", 10);
    File target = new File(tempDir, "target.parquet");

    try (NativeParquetFileWriter writer =
        NativeParquetFileWriter.create(
            Files.localOutput(target), SCHEMA, CompressionCodecName.SNAPPY)) {
      writer.appendFile(Files.localInput(source));
      writer.end(java.util.Map.of());
    }

    try (NativeParquetFileReader reader = NativeParquetFileReader.open(Files.localInput(target))) {
      assertThat(reader.fileSchema().getFieldName(0)).isEqualTo("ID");
      assertThat(reader.fileSchema().getType("Day").getLogicalTypeAnnotation())
          .isEqualTo(LogicalTypeAnnotation.dateType());

      ColumnChunkMetadata column = reader.rowGroups().get(0).columns().get(0);
      assertThat(column.statistics().isNumNullsSet()).isTrue();
      assertThat(column.statistics().genericGetMin()).isEqualTo(0);
      assertThat(column.statistics().genericGetMax()).isEqualTo(9);
    }
  }

  @Test
  void appendRejectsSchemaMismatch() throws IOException {
    File source = writeSource("source.parquet", 10);
    MessageType other = Types.buildMessage().required(INT32).named("other").named("test_schema");

    try (NativeParquetFileWriter writer =
        NativeParquetFileWriter.create(
            Files.localOutput(new File(tempDir, "target.parquet")),
            other,
            CompressionCodecName.SNAPPY)) {
      assertThatThrownBy(() -> writer.appendFile(Files.localInput(source)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("schema does not match");
    }
  }

  @Test
  void readRowGroupReturnsAllPages() throws IOException {
    File source = writeSource("source.parquet", 100);

    try (NativeParquetFileReader reader = NativeParquetFileReader.open(Files.localInput(source))) {
      BlockMetadata block = reader.rowGroups().get(0);
      PageReadStore store = reader.readNextRowGroup();
      assertThat(store.getRowCount()).isEqualTo(block.rowCount());
      assertThat(store.getPageReader(reader.fileSchema().getColumns().get(0)).readPage())
          .isNotNull();
      assertThat(reader.readNextRowGroup()).isNull();
    }
  }

  @Test
  void rowGroupRangeUsesStartingPosition() throws IOException {
    File source = writeSource("source.parquet", 100);

    try (NativeParquetFileReader reader = NativeParquetFileReader.open(Files.localInput(source))) {
      assertThat(reader.rowGroupsInRange(0, Long.MAX_VALUE)).hasSize(reader.rowGroups().size());
      assertThat(reader.rowGroupsInRange(0, 1)).isEmpty();
    }
  }

  @Test
  void footerMatchesHadoopReader() throws IOException {
    File source = writeSource("source.parquet", 100);

    try (NativeParquetFileReader nativeReader =
            NativeParquetFileReader.open(Files.localInput(source));
        ParquetFileReader hadoopReader =
            ParquetFileReader.open(ParquetIO.file(Files.localInput(source)))) {
      List<org.apache.parquet.hadoop.metadata.BlockMetaData> expected = hadoopReader.getRowGroups();
      assertThat(nativeReader.rowGroups()).hasSize(expected.size());
      assertThat(nativeReader.rowGroups().get(0).totalByteSize())
          .isEqualTo(expected.get(0).getTotalByteSize());
      assertThat(nativeReader.fileSchema()).isEqualTo(hadoopReader.getFileMetaData().getSchema());
    }
  }

  private File writeSource(String name, int rows) throws IOException {
    File file = new File(tempDir, name);
    try (org.apache.parquet.hadoop.ParquetWriter<Group> writer =
        org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(
                new org.apache.hadoop.fs.Path(file.toURI()))
            .withType(SCHEMA)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()) {
      SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
      for (int i = 0; i < rows; i++) {
        Group group = factory.newGroup();
        group.append("ID", i);
        if (i % 2 == 0) {
          group.append("Day", i);
        }
        writer.write(group);
      }
    }

    return file;
  }
}
