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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestParquetFileMerger {

  @TempDir private File tempDir;

  @Test
  public void testCanMergeReturnsFalseForEmptyList() {
    boolean result = ParquetFileMerger.canMerge(Collections.emptyList());
    assertThat(result).isFalse();
  }

  @Test
  public void testCanMergeReturnsFalseForNullInput() {
    boolean result = ParquetFileMerger.canMerge(null);
    assertThat(result).isFalse();
  }

  @Test
  public void testCanMergeReturnsFalseForNonParquetFile() throws IOException {
    // Create a non-Parquet file (just a text file)
    File textFile = new File(tempDir, "not-parquet.txt");
    java.nio.file.Files.write(textFile.toPath(), "This is not a Parquet file".getBytes());

    InputFile inputFile = Files.localInput(textFile);
    List<InputFile> inputFiles = Lists.newArrayList(inputFile);

    // Should return false because file is not valid Parquet
    boolean result = ParquetFileMerger.canMerge(inputFiles);
    assertThat(result).isFalse();
  }

  @Test
  public void testCanMergeReturnsFalseForDifferentSchemas() throws IOException {
    // Create first Parquet file with schema1
    Schema icebergSchema1 =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    File parquetFile1 = new File(tempDir, "file1.parquet");
    OutputFile outputFile1 = Files.localOutput(parquetFile1);
    createParquetFileWithSchema(outputFile1, icebergSchema1);

    // Create second Parquet file with different schema
    Schema icebergSchema2 =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(3, "other", Types.LongType.get())); // Different field

    File parquetFile2 = new File(tempDir, "file2.parquet");
    OutputFile outputFile2 = Files.localOutput(parquetFile2);
    createParquetFileWithSchema(outputFile2, icebergSchema2);

    // Try to validate - should return false due to different schemas
    InputFile inputFile1 = Files.localInput(parquetFile1);
    InputFile inputFile2 = Files.localInput(parquetFile2);
    List<InputFile> inputFiles = Arrays.asList(inputFile1, inputFile2);

    boolean result = ParquetFileMerger.canMerge(inputFiles);
    assertThat(result).isFalse();
  }

  @Test
  public void testCanMergeReturnsTrueForIdenticalSchemas() throws IOException {
    // Create two Parquet files with the same schema
    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    File parquetFile1 = new File(tempDir, "file1.parquet");
    OutputFile outputFile1 = Files.localOutput(parquetFile1);
    createParquetFileWithSchema(outputFile1, icebergSchema);

    File parquetFile2 = new File(tempDir, "file2.parquet");
    OutputFile outputFile2 = Files.localOutput(parquetFile2);
    createParquetFileWithSchema(outputFile2, icebergSchema);

    // Should return true for identical schemas
    InputFile inputFile1 = Files.localInput(parquetFile1);
    InputFile inputFile2 = Files.localInput(parquetFile2);
    List<InputFile> inputFiles = Arrays.asList(inputFile1, inputFile2);

    boolean result = ParquetFileMerger.canMerge(inputFiles);
    assertThat(result).isTrue();
  }

  @Test
  public void testCanMergeReturnsTrueForSingleFile() throws IOException {
    // Create a single Parquet file
    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    File parquetFile = new File(tempDir, "file.parquet");
    OutputFile outputFile = Files.localOutput(parquetFile);
    createParquetFileWithSchema(outputFile, icebergSchema);

    // Should return true for single file
    InputFile inputFile = Files.localInput(parquetFile);
    List<InputFile> inputFiles = Lists.newArrayList(inputFile);

    boolean result = ParquetFileMerger.canMerge(inputFiles);
    assertThat(result).isTrue();
  }

  /** Helper method to create a Parquet file with a given schema (empty file, just for testing) */
  private void createParquetFileWithSchema(OutputFile outputFile, Schema schema)
      throws IOException {
    org.apache.iceberg.parquet.Parquet.writeData(outputFile)
        .schema(schema)
        .withSpec(PartitionSpec.unpartitioned())
        .createWriterFunc(GenericParquetWriter::create)
        .overwrite()
        .build()
        .close();
  }
}
