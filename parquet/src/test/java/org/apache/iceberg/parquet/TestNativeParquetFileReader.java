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

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.metadata.BlockMetadata;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestNativeParquetFileReader {

  @TempDir private File tempDir;

  @Test
  public void testReadFooter() throws IOException {
    // Create test file using Hadoop writer for compatibility
    File parquetFile = new File(tempDir, "test.parquet");
    writeTestParquetFile(parquetFile);

    // Read with native reader
    InputFile inputFile = Files.localInput(parquetFile);
    try (NativeParquetFileReader reader = NativeParquetFileReader.open(inputFile)) {
      org.apache.iceberg.parquet.metadata.ParquetMetadata metadata = reader.footer();
      assertThat(metadata).isNotNull();
      assertThat(metadata.blocks()).isNotEmpty();

      MessageType schema = reader.fileSchema();
      assertThat(schema).isNotNull();
      assertThat(schema.getColumns()).isNotEmpty();
    }
  }

  @Test
  public void testReadRowGroups() throws IOException {
    // Create test file
    File parquetFile = new File(tempDir, "test_rowgroups.parquet");
    writeTestParquetFile(parquetFile);

    // Read with native reader
    InputFile inputFile = Files.localInput(parquetFile);
    try (NativeParquetFileReader reader = NativeParquetFileReader.open(inputFile)) {
      List<BlockMetadata> rowGroups = reader.rowGroups();
      assertThat(rowGroups).isNotEmpty();

      for (BlockMetadata block : rowGroups) {
        assertThat(block.rowCount()).isGreaterThan(0);
        assertThat(block.columns()).isNotEmpty();
      }
    }
  }

  private static void writeTestParquetFile(File file) throws IOException {
    MessageType schema =
        Types.buildMessage()
            .required(INT32)
            .named("id")
            .required(INT32)
            .named("value")
            .named("test_schema");

    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toURI());

    try (ParquetWriter<Group> writer =
        org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(hadoopPath)
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .build()) {

      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
      for (int i = 0; i < 100; i++) {
        Group group = groupFactory.newGroup();
        group.append("id", i);
        group.append("value", i * 10);
        writer.write(group);
      }
    }
  }
}
